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

    // First pass: register all templates from this message
    for set in &msg.sets {
        match &set.contents {
            SetContents::Template(templates) => {
                for tmpl in templates {
                    session.update_template(exporter, domain, tmpl);
                }
            }
            SetContents::OptionsTemplate(templates) => {
                for tmpl in templates {
                    session.update_options_template(exporter, domain, tmpl);
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
                if let Some(tmpl) = session.get_template(exporter, domain, data_set.template_id) {
                    data_set.records = decode_data_records(data_bytes, tmpl, data_set.template_id);
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
}
