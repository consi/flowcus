//! IPFIX data record decoder.
//!
//! Uses cached templates to decode raw data set bytes into typed field values.
//! Unknown IEs (not in the registry) are preserved as OctetArray per RFC 5153
//! and logged once per unique (element_id, enterprise_id) pair.

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};

use tracing::{debug, trace, warn};

/// Tracks which unknown IEs have been logged, to avoid log flooding.
/// Per RFC 5153: "The Collecting Process MUST note the Information Element
/// identifier of any Information Element that it does not understand."
/// We satisfy this with a one-time debug log per unique (element_id, enterprise_id).
static UNKNOWN_IES_LOGGED: LazyLock<Mutex<HashSet<(u16, u32)>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

use crate::ie;
use crate::protocol::{
    DataField, DataRecord, IpfixMessage, ParseError, SetContents, decode_field_value,
    read_variable_length,
};
use crate::session::{CachedTemplate, SessionStore};

/// Decode all data records in an IPFIX message using the session's template cache.
///
/// Templates sets are processed first to ensure they're available for data decoding.
/// Returns the message with data records populated.
pub fn decode_message(msg: &mut IpfixMessage, raw: &[u8], session: &mut SessionStore) {
    let exporter = msg.exporter;
    let domain = msg.header.observation_domain_id;

    let proto_ver = msg.header.protocol_version;

    // First pass: register all templates from this message
    for set in &msg.sets {
        match &set.contents {
            SetContents::Template(templates) => {
                for tmpl in templates {
                    session.update_template(exporter, domain, tmpl, proto_ver);
                }
            }
            SetContents::OptionsTemplate(templates) => {
                for tmpl in templates {
                    session.update_options_template(exporter, domain, tmpl, proto_ver);
                }
            }
            SetContents::Data(_) => {}
        }
    }

    // Second pass: decode data sets
    // We need to re-parse set boundaries from raw bytes to get the data payload
    let mut offset = 16; // skip message header
    let msg_len = msg.header.length as usize;

    for set in &mut msg.sets {
        if offset + 4 > msg_len {
            break;
        }

        let set_id = u16::from_be_bytes([raw[offset], raw[offset + 1]]);
        let set_length = u16::from_be_bytes([raw[offset + 2], raw[offset + 3]]) as usize;

        if let SetContents::Data(ref mut data_set) = set.contents {
            debug_assert_eq!(set_id, data_set.template_id);

            let data_start = offset + 4;
            let data_end = offset + set_length;

            if data_end <= raw.len() {
                let data_bytes = &raw[data_start..data_end];
                // Clone template to release the immutable borrow on session,
                // allowing mutable access for option metadata extraction.
                let tmpl_owned = session
                    .get_template(exporter, domain, data_set.template_id)
                    .cloned();
                if let Some(tmpl) = tmpl_owned {
                    if tmpl.scope_field_count > 0 {
                        // Option data record — extract metadata, skip normal ingestion
                        let records = decode_data_records(data_bytes, &tmpl, data_set.template_id);
                        extract_option_metadata(&records, &tmpl, exporter, domain, session);
                        // Leave data_set.records empty so the listener does NOT
                        // forward these to the storage writer.
                    } else {
                        data_set.records =
                            decode_data_records(data_bytes, &tmpl, data_set.template_id);
                    }
                } else {
                    warn!(
                        template_id = data_set.template_id,
                        %exporter,
                        domain,
                        "No template found for data set, skipping"
                    );
                }
            }
        }

        offset += set_length;
    }
}

/// Decode data records from a data set body using a template.
fn decode_data_records(buf: &[u8], template: &CachedTemplate, template_id: u16) -> Vec<DataRecord> {
    let mut records = Vec::new();
    let mut offset = 0;

    while offset + template.min_record_length <= buf.len() {
        match decode_single_record(buf, offset, template) {
            Ok((record, consumed)) => {
                offset += consumed;
                records.push(record);
            }
            Err(e) => {
                // Remaining bytes may be padding (RFC 7011 Section 3.3.1)
                trace!(
                    template_id,
                    offset,
                    remaining = buf.len() - offset,
                    error = %e,
                    "Stopped decoding data records (likely padding)"
                );
                break;
            }
        }
    }

    records
}

/// IANA IE IDs for interface metadata in option templates.
const IE_INGRESS_INTERFACE: u16 = 10;
const IE_EGRESS_INTERFACE: u16 = 14;
const IE_INTERFACE_NAME: u16 = 82;
const IE_INTERFACE_DESCRIPTION: u16 = 83;

/// Extract interface metadata from option data records and store in the session.
///
/// Looks for scope fields IE 10 (ingressInterface) or IE 14 (egressInterface)
/// paired with IE 82 (interfaceName) or IE 83 (interfaceDescription).
fn extract_option_metadata(
    records: &[DataRecord],
    template: &CachedTemplate,
    exporter: std::net::SocketAddr,
    domain: u32,
    session: &mut SessionStore,
) {
    use crate::protocol::FieldValue;

    let exporter_ip = exporter.ip();

    // Check if this option template contains interface-related fields.
    // Scope fields are the first `scope_field_count` specifiers.
    let scope_count = template.scope_field_count as usize;

    let has_if_scope = template.field_specifiers.iter().take(scope_count).any(|f| {
        f.enterprise_id == 0
            && (f.element_id == IE_INGRESS_INTERFACE || f.element_id == IE_EGRESS_INTERFACE)
    });

    let has_if_name = template.field_specifiers.iter().skip(scope_count).any(|f| {
        f.enterprise_id == 0
            && (f.element_id == IE_INTERFACE_NAME || f.element_id == IE_INTERFACE_DESCRIPTION)
    });

    if !has_if_scope || !has_if_name {
        debug!(
            template_id = template.template_id,
            scope_count, "Option data record does not contain interface metadata, skipping"
        );
        return;
    }

    for record in records {
        // Extract interface index from scope fields
        let mut if_index: Option<u32> = None;
        for field in &record.fields {
            if field.spec.enterprise_id == 0
                && (field.spec.element_id == IE_INGRESS_INTERFACE
                    || field.spec.element_id == IE_EGRESS_INTERFACE)
            {
                if_index = match &field.value {
                    FieldValue::Unsigned32(v) => Some(*v),
                    FieldValue::Unsigned16(v) => Some(u32::from(*v)),
                    FieldValue::Unsigned8(v) => Some(u32::from(*v)),
                    _ => None,
                };
                break;
            }
        }

        let Some(idx) = if_index else {
            continue;
        };

        // Extract interface name (prefer IE 82 over IE 83)
        let mut if_name: Option<String> = None;
        for field in &record.fields {
            if field.spec.enterprise_id == 0 && field.spec.element_id == IE_INTERFACE_NAME {
                if let FieldValue::String(s) = &field.value {
                    if_name = Some(s.clone());
                    break;
                }
            }
        }
        if if_name.is_none() {
            for field in &record.fields {
                if field.spec.enterprise_id == 0
                    && field.spec.element_id == IE_INTERFACE_DESCRIPTION
                {
                    if let FieldValue::String(s) = &field.value {
                        if_name = Some(s.clone());
                        break;
                    }
                }
            }
        }

        if let Some(name) = if_name {
            if !name.is_empty() {
                session.set_interface_name(exporter_ip, domain, idx, name);
            }
        }
    }
}

/// Decode a single data record at the given offset.
/// Returns the record and number of bytes consumed.
fn decode_single_record(
    buf: &[u8],
    mut offset: usize,
    template: &CachedTemplate,
) -> Result<(DataRecord, usize), ParseError> {
    let start = offset;
    let mut fields = Vec::with_capacity(template.field_specifiers.len());

    for spec in &template.field_specifiers {
        let (field_bytes, consumed) = if spec.is_variable_length() {
            let (data, consumed) = read_variable_length(buf, offset)?;
            (data, consumed)
        } else {
            let len = spec.field_length as usize;
            if offset + len > buf.len() {
                return Err(ParseError::BufferTooShort {
                    need: offset + len,
                    have: buf.len(),
                });
            }
            (&buf[offset..offset + len], len)
        };

        let dt = ie::data_type(spec.element_id, spec.enterprise_id);
        let value = decode_field_value(field_bytes, dt);
        let name = ie::name(spec.element_id, spec.enterprise_id);

        // RFC 5153 compliance: note unknown IEs (once per unique id pair)
        if !ie::is_known(spec.element_id, spec.enterprise_id) {
            let key = (spec.element_id, spec.enterprise_id);
            if let Ok(mut set) = UNKNOWN_IES_LOGGED.lock() {
                if set.insert(key) {
                    debug!(
                        element_id = spec.element_id,
                        enterprise_id = spec.enterprise_id,
                        field_length = spec.field_length,
                        generated_name = %name,
                        "Unknown IPFIX IE encountered, storing as OctetArray"
                    );
                }
            }
        }

        fields.push(DataField {
            spec: *spec,
            name,
            value,
        });

        offset += consumed;
    }

    Ok((DataRecord { fields }, offset - start))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{self, IPFIX_VERSION};
    use std::net::SocketAddr;

    /// Build a complete IPFIX message with one template and one data record.
    fn build_test_message() -> Vec<u8> {
        let mut msg = Vec::new();

        // We'll build template set + data set, then fix the header length

        // --- Template Set ---
        // Set header: id=2, length=20
        let mut tmpl_set = Vec::new();
        tmpl_set.extend_from_slice(&2u16.to_be_bytes());
        tmpl_set.extend_from_slice(&16u16.to_be_bytes());
        // Template: id=256, field_count=2
        tmpl_set.extend_from_slice(&256u16.to_be_bytes());
        tmpl_set.extend_from_slice(&2u16.to_be_bytes());
        // IE 8 (sourceIPv4Address), 4 bytes
        tmpl_set.extend_from_slice(&8u16.to_be_bytes());
        tmpl_set.extend_from_slice(&4u16.to_be_bytes());
        // IE 12 (destinationIPv4Address), 4 bytes
        tmpl_set.extend_from_slice(&12u16.to_be_bytes());
        tmpl_set.extend_from_slice(&4u16.to_be_bytes());

        // --- Data Set ---
        // Set header: id=256, length=12 (4 header + 8 data)
        let mut data_set = Vec::new();
        data_set.extend_from_slice(&256u16.to_be_bytes());
        data_set.extend_from_slice(&12u16.to_be_bytes());
        // Record: 192.168.1.1 -> 10.0.0.1
        data_set.extend_from_slice(&[192, 168, 1, 1]);
        data_set.extend_from_slice(&[10, 0, 0, 1]);

        // --- Message Header ---
        let total_len = 16 + tmpl_set.len() + data_set.len();
        msg.extend_from_slice(&IPFIX_VERSION.to_be_bytes());
        msg.extend_from_slice(&(total_len as u16).to_be_bytes());
        msg.extend_from_slice(&1_700_000_000u32.to_be_bytes());
        msg.extend_from_slice(&1u32.to_be_bytes()); // seq
        msg.extend_from_slice(&1u32.to_be_bytes()); // domain
        msg.extend_from_slice(&tmpl_set);
        msg.extend_from_slice(&data_set);

        msg
    }

    #[test]
    fn decode_full_message() {
        let raw = build_test_message();
        let addr: SocketAddr = "10.0.0.1:4739".parse().unwrap();

        let mut msg = protocol::parse_message(&raw, addr).unwrap();
        let mut session = SessionStore::new(1800);

        decode_message(&mut msg, &raw, &mut session);

        // Template should be registered
        assert_eq!(session.template_count(), 1);

        // Find the data set and check it was decoded
        let data = msg.sets.iter().find_map(|s| {
            if let SetContents::Data(ds) = &s.contents {
                Some(ds)
            } else {
                None
            }
        });
        let data = data.expect("should have a data set");
        assert_eq!(data.records.len(), 1);

        let record = &data.records[0];
        assert_eq!(record.fields.len(), 2);
        assert_eq!(record.fields[0].name, "sourceIPv4Address");
        assert_eq!(record.fields[0].value.to_string(), "192.168.1.1");
        assert_eq!(record.fields[1].name, "destinationIPv4Address");
        assert_eq!(record.fields[1].value.to_string(), "10.0.0.1");
    }

    /// Build an IPFIX message with an option template (Set ID 3) containing
    /// interface metadata: scope=ingressInterface(IE 10), value=interfaceName(IE 82).
    /// Simulates a Cisco-style interface name option data export.
    fn build_option_template_message() -> Vec<u8> {
        let mut msg = Vec::new();

        // --- Options Template Set (Set ID = 3) ---
        // Option template: id=257, total_field_count=2, scope_field_count=1
        //   Scope: IE 10 (ingressInterface), 4 bytes
        //   Value: IE 82 (interfaceName), variable length (0xFFFF)
        let mut opt_tmpl_set = Vec::new();
        opt_tmpl_set.extend_from_slice(&3u16.to_be_bytes()); // Set ID
        // length placeholder — filled after
        opt_tmpl_set.extend_from_slice(&0u16.to_be_bytes());
        // Template header: id=257, total_field_count=2, scope_field_count=1
        opt_tmpl_set.extend_from_slice(&257u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&2u16.to_be_bytes()); // total fields
        opt_tmpl_set.extend_from_slice(&1u16.to_be_bytes()); // scope fields
        // IE 10 (ingressInterface), 4 bytes, enterprise_id=0
        opt_tmpl_set.extend_from_slice(&10u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&4u16.to_be_bytes());
        // IE 82 (interfaceName), variable length
        opt_tmpl_set.extend_from_slice(&82u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&0xFFFFu16.to_be_bytes());
        // Fix set length (includes 4-byte set header)
        let set_len = opt_tmpl_set.len() as u16;
        opt_tmpl_set[2..4].copy_from_slice(&set_len.to_be_bytes());

        // --- Option Data Set (Set ID = 257) ---
        // Two records: ifIndex=1 → "GigabitEthernet0/0", ifIndex=2 → "Loopback0"
        let if_name_1 = b"GigabitEthernet0/0";
        let if_name_2 = b"Loopback0";
        let mut data_set = Vec::new();
        data_set.extend_from_slice(&257u16.to_be_bytes()); // Set ID
        data_set.extend_from_slice(&0u16.to_be_bytes()); // length placeholder
        // Record 1: ifIndex=1, interfaceName="GigabitEthernet0/0"
        data_set.extend_from_slice(&1u32.to_be_bytes());
        data_set.push(if_name_1.len() as u8); // variable-length: short encoding
        data_set.extend_from_slice(if_name_1);
        // Record 2: ifIndex=2, interfaceName="Loopback0"
        data_set.extend_from_slice(&2u32.to_be_bytes());
        data_set.push(if_name_2.len() as u8);
        data_set.extend_from_slice(if_name_2);
        // Fix set length
        let data_len = data_set.len() as u16;
        data_set[2..4].copy_from_slice(&data_len.to_be_bytes());

        // --- Message Header ---
        let total_len = 16 + opt_tmpl_set.len() + data_set.len();
        msg.extend_from_slice(&IPFIX_VERSION.to_be_bytes());
        msg.extend_from_slice(&(total_len as u16).to_be_bytes());
        msg.extend_from_slice(&1_700_000_000u32.to_be_bytes()); // export time
        msg.extend_from_slice(&1u32.to_be_bytes()); // seq
        msg.extend_from_slice(&42u32.to_be_bytes()); // observation domain
        msg.extend_from_slice(&opt_tmpl_set);
        msg.extend_from_slice(&data_set);

        msg
    }

    #[test]
    fn decode_option_data_extracts_interface_names() {
        let raw = build_option_template_message();
        let addr: SocketAddr = "10.0.0.1:4739".parse().unwrap();

        let mut msg = protocol::parse_message(&raw, addr).unwrap();
        let mut session = SessionStore::new(1800);

        decode_message(&mut msg, &raw, &mut session);

        // Option template should be registered
        assert_eq!(session.template_count(), 1);
        let tmpl = session.get_template(addr, 42, 257).unwrap();
        assert_eq!(tmpl.scope_field_count, 1);

        // Data set records should be empty (option data is NOT forwarded to storage)
        let data = msg.sets.iter().find_map(|s| {
            if let SetContents::Data(ds) = &s.contents {
                Some(ds)
            } else {
                None
            }
        });
        let data = data.expect("should have a data set");
        assert!(
            data.records.is_empty(),
            "option data records must not be forwarded"
        );

        // Interface names should be stored in session metadata
        let names = session.get_interface_names();
        assert_eq!(names.len(), 2);
        let exporter_ip = addr.ip();
        assert_eq!(names[&(exporter_ip, 42, 1)], "GigabitEthernet0/0");
        assert_eq!(names[&(exporter_ip, 42, 2)], "Loopback0");
    }

    /// Test with egressInterface (IE 14) as scope field instead of ingressInterface.
    /// Simulates Juniper-style option data where egress interface is the key.
    #[test]
    fn decode_option_data_egress_interface() {
        let mut msg_bytes = Vec::new();

        // Options Template Set: scope=IE 14 (egressInterface, 4 bytes), value=IE 82 (interfaceName, varlen)
        let mut opt_tmpl_set = Vec::new();
        opt_tmpl_set.extend_from_slice(&3u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&0u16.to_be_bytes()); // length placeholder
        opt_tmpl_set.extend_from_slice(&258u16.to_be_bytes()); // template id
        opt_tmpl_set.extend_from_slice(&2u16.to_be_bytes()); // total fields
        opt_tmpl_set.extend_from_slice(&1u16.to_be_bytes()); // scope fields
        opt_tmpl_set.extend_from_slice(&14u16.to_be_bytes()); // IE 14 (egressInterface)
        opt_tmpl_set.extend_from_slice(&4u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&82u16.to_be_bytes()); // IE 82 (interfaceName)
        opt_tmpl_set.extend_from_slice(&0xFFFFu16.to_be_bytes());
        let set_len = opt_tmpl_set.len() as u16;
        opt_tmpl_set[2..4].copy_from_slice(&set_len.to_be_bytes());

        // Data Set
        let if_name = b"xe-0/0/0";
        let mut data_set = Vec::new();
        data_set.extend_from_slice(&258u16.to_be_bytes());
        data_set.extend_from_slice(&0u16.to_be_bytes());
        data_set.extend_from_slice(&100u32.to_be_bytes()); // ifIndex=100
        data_set.push(if_name.len() as u8);
        data_set.extend_from_slice(if_name);
        let data_len = data_set.len() as u16;
        data_set[2..4].copy_from_slice(&data_len.to_be_bytes());

        // Message header
        let total_len = 16 + opt_tmpl_set.len() + data_set.len();
        msg_bytes.extend_from_slice(&IPFIX_VERSION.to_be_bytes());
        msg_bytes.extend_from_slice(&(total_len as u16).to_be_bytes());
        msg_bytes.extend_from_slice(&1_700_000_000u32.to_be_bytes());
        msg_bytes.extend_from_slice(&1u32.to_be_bytes());
        msg_bytes.extend_from_slice(&7u32.to_be_bytes()); // domain=7
        msg_bytes.extend_from_slice(&opt_tmpl_set);
        msg_bytes.extend_from_slice(&data_set);

        let addr: SocketAddr = "192.168.1.1:4739".parse().unwrap();
        let mut msg = protocol::parse_message(&msg_bytes, addr).unwrap();
        let mut session = SessionStore::new(1800);

        decode_message(&mut msg, &msg_bytes, &mut session);

        let names = session.get_interface_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[&(addr.ip(), 7, 100)], "xe-0/0/0");
    }

    /// Option data that does NOT contain interface metadata (e.g. sampling rate options)
    /// should be silently skipped without storing any interface names.
    #[test]
    fn decode_non_interface_option_data_skipped() {
        let mut msg_bytes = Vec::new();

        // Options Template: scope=IE 10 (ingressInterface), value=IE 34 (samplingInterval)
        // This has IF scope but no IF name → should not store anything
        let mut opt_tmpl_set = Vec::new();
        opt_tmpl_set.extend_from_slice(&3u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&0u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&259u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&2u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&1u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&10u16.to_be_bytes()); // IE 10 scope
        opt_tmpl_set.extend_from_slice(&4u16.to_be_bytes());
        opt_tmpl_set.extend_from_slice(&34u16.to_be_bytes()); // IE 34 (samplingInterval)
        opt_tmpl_set.extend_from_slice(&4u16.to_be_bytes());
        let set_len = opt_tmpl_set.len() as u16;
        opt_tmpl_set[2..4].copy_from_slice(&set_len.to_be_bytes());

        // Data Set
        let mut data_set = Vec::new();
        data_set.extend_from_slice(&259u16.to_be_bytes());
        data_set.extend_from_slice(&0u16.to_be_bytes());
        data_set.extend_from_slice(&1u32.to_be_bytes()); // ifIndex=1
        data_set.extend_from_slice(&100u32.to_be_bytes()); // samplingInterval=100
        let data_len = data_set.len() as u16;
        data_set[2..4].copy_from_slice(&data_len.to_be_bytes());

        let total_len = 16 + opt_tmpl_set.len() + data_set.len();
        msg_bytes.extend_from_slice(&IPFIX_VERSION.to_be_bytes());
        msg_bytes.extend_from_slice(&(total_len as u16).to_be_bytes());
        msg_bytes.extend_from_slice(&1_700_000_000u32.to_be_bytes());
        msg_bytes.extend_from_slice(&1u32.to_be_bytes());
        msg_bytes.extend_from_slice(&0u32.to_be_bytes());
        msg_bytes.extend_from_slice(&opt_tmpl_set);
        msg_bytes.extend_from_slice(&data_set);

        let addr: SocketAddr = "10.0.0.1:4739".parse().unwrap();
        let mut msg = protocol::parse_message(&msg_bytes, addr).unwrap();
        let mut session = SessionStore::new(1800);

        decode_message(&mut msg, &msg_bytes, &mut session);

        // No interface names should be stored
        assert!(session.get_interface_names().is_empty());
        // Data records should still be empty (option data is never forwarded)
        let data = msg.sets.iter().find_map(|s| {
            if let SetContents::Data(ds) = &s.contents {
                Some(ds)
            } else {
                None
            }
        });
        let data = data.expect("should have a data set");
        assert!(data.records.is_empty());
    }
}
