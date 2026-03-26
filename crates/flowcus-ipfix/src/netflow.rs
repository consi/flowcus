//! NetFlow v9 (RFC 3954) and v5 translation to IPFIX wire format.
//!
//! Both protocols are translated into IPFIX `IpfixMessage` structures at the
//! protocol boundary so the entire downstream pipeline (decoder, session,
//! storage, query) operates unchanged.

use std::net::SocketAddr;
use std::sync::LazyLock;

use crate::protocol::{
    self, DataSet, FieldSpecifier, IpfixMessage, MessageHeader, OptionsTemplateRecord, ParseError,
    Set, SetContents, TemplateRecord,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// NetFlow v9 protocol version (RFC 3954).
pub const NETFLOW_V9_VERSION: u16 = 0x0009;
/// NetFlow v5 protocol version.
pub const NETFLOW_V5_VERSION: u16 = 0x0005;

/// NetFlow v9 header length in bytes.
const V9_HEADER_LEN: usize = 20;
/// NetFlow v5 header length in bytes.
const V5_HEADER_LEN: usize = 24;
/// NetFlow v5 flow record length in bytes.
const V5_RECORD_LEN: usize = 48;

/// v9 Template FlowSet ID.
const V9_TEMPLATE_SET_ID: u16 = 0;
/// v9 Options Template FlowSet ID.
const V9_OPTIONS_TEMPLATE_SET_ID: u16 = 1;

/// Synthetic template ID used for NetFlow v5 flows.
const V5_SYNTHETIC_TEMPLATE_ID: u16 = 300;

// ---------------------------------------------------------------------------
// Cisco v9 field ID remapping
// ---------------------------------------------------------------------------

/// Remap well-known Cisco proprietary NetFlow v9 field IDs to their IANA IPFIX
/// equivalents. Returns the original ID unchanged if no mapping exists.
///
/// Mappings follow libfixbuf conventions:
/// - 40001 → 225 `postNATSourceIPv4Address`
/// - 40002 → 226 `postNATDestinationIPv4Address`
/// - 40003 → 227 `postNAPTSourceTransportPort`
/// - 40004 → 228 `postNAPTDestinationTransportPort`
const fn remap_v9_field_id(id: u16) -> u16 {
    match id {
        40001 => 225,
        40002 => 226,
        40003 => 227,
        40004 => 228,
        other => other,
    }
}

// ---------------------------------------------------------------------------
// NetFlow v5 synthetic template
// ---------------------------------------------------------------------------

/// Static template for NetFlow v5 flows. Maps the 18 non-padding fields of the
/// 48-byte v5 record to their IANA IPFIX IE equivalents with exact wire lengths.
static V5_TEMPLATE: LazyLock<Vec<FieldSpecifier>> = LazyLock::new(|| {
    vec![
        // srcaddr (4 bytes) → IE 8 sourceIPv4Address
        FieldSpecifier {
            element_id: 8,
            field_length: 4,
            enterprise_id: 0,
        },
        // dstaddr (4 bytes) → IE 12 destinationIPv4Address
        FieldSpecifier {
            element_id: 12,
            field_length: 4,
            enterprise_id: 0,
        },
        // nexthop (4 bytes) → IE 15 ipNextHopIPv4Address
        FieldSpecifier {
            element_id: 15,
            field_length: 4,
            enterprise_id: 0,
        },
        // input (2 bytes) → IE 10 ingressInterface (Unsigned32, reduced-size)
        FieldSpecifier {
            element_id: 10,
            field_length: 2,
            enterprise_id: 0,
        },
        // output (2 bytes) → IE 14 egressInterface (Unsigned32, reduced-size)
        FieldSpecifier {
            element_id: 14,
            field_length: 2,
            enterprise_id: 0,
        },
        // dPkts (4 bytes) → IE 2 packetDeltaCount (Unsigned64, reduced-size)
        FieldSpecifier {
            element_id: 2,
            field_length: 4,
            enterprise_id: 0,
        },
        // dOctets (4 bytes) → IE 1 octetDeltaCount (Unsigned64, reduced-size)
        FieldSpecifier {
            element_id: 1,
            field_length: 4,
            enterprise_id: 0,
        },
        // first (4 bytes) → IE 22 flowStartSysUpTime
        FieldSpecifier {
            element_id: 22,
            field_length: 4,
            enterprise_id: 0,
        },
        // last (4 bytes) → IE 21 flowEndSysUpTime
        FieldSpecifier {
            element_id: 21,
            field_length: 4,
            enterprise_id: 0,
        },
        // srcport (2 bytes) → IE 7 sourceTransportPort
        FieldSpecifier {
            element_id: 7,
            field_length: 2,
            enterprise_id: 0,
        },
        // dstport (2 bytes) → IE 11 destinationTransportPort
        FieldSpecifier {
            element_id: 11,
            field_length: 2,
            enterprise_id: 0,
        },
        // [pad1 at offset 36, 1 byte — skipped]
        // tcp_flags (1 byte) → IE 6 tcpControlBits (Unsigned16, reduced-size)
        FieldSpecifier {
            element_id: 6,
            field_length: 1,
            enterprise_id: 0,
        },
        // prot (1 byte) → IE 4 protocolIdentifier
        FieldSpecifier {
            element_id: 4,
            field_length: 1,
            enterprise_id: 0,
        },
        // tos (1 byte) → IE 5 ipClassOfService
        FieldSpecifier {
            element_id: 5,
            field_length: 1,
            enterprise_id: 0,
        },
        // src_as (2 bytes) → IE 16 bgpSourceAsNumber (Unsigned32, reduced-size)
        FieldSpecifier {
            element_id: 16,
            field_length: 2,
            enterprise_id: 0,
        },
        // dst_as (2 bytes) → IE 17 bgpDestinationAsNumber (Unsigned32, reduced-size)
        FieldSpecifier {
            element_id: 17,
            field_length: 2,
            enterprise_id: 0,
        },
        // src_mask (1 byte) → IE 9 sourceIPv4PrefixLength
        FieldSpecifier {
            element_id: 9,
            field_length: 1,
            enterprise_id: 0,
        },
        // dst_mask (1 byte) → IE 13 destinationIPv4PrefixLength
        FieldSpecifier {
            element_id: 13,
            field_length: 1,
            enterprise_id: 0,
        },
        // [pad2 at offset 46, 2 bytes — skipped]
    ]
});

/// Size of one v5 record after removing padding bytes (pad1=1, pad2=2).
/// Sum of all field_length values in V5_TEMPLATE.
const V5_REPACKED_RECORD_LEN: usize = 45;

// ---------------------------------------------------------------------------
// NetFlow v9 translation
// ---------------------------------------------------------------------------

/// Translate a NetFlow v9 packet into an IPFIX-compatible message and raw buffer.
///
/// The returned raw buffer has a 16-byte IPFIX header followed by the original
/// FlowSet data (with set IDs remapped to IPFIX equivalents). This allows
/// `decode_message()` to re-walk set boundaries at offset 16 unchanged.
pub fn translate_v9(
    buf: &[u8],
    exporter: SocketAddr,
) -> Result<(IpfixMessage, Vec<u8>), ParseError> {
    if buf.len() < V9_HEADER_LEN {
        return Err(ParseError::BufferTooShort {
            need: V9_HEADER_LEN,
            have: buf.len(),
        });
    }

    // Parse v9 header (20 bytes)
    let version = protocol::read_u16(buf, 0)?;
    if version != NETFLOW_V9_VERSION {
        return Err(ParseError::InvalidVersion(version));
    }
    // count field at offset 2 is unreliable — we walk FlowSet lengths instead
    let _sys_uptime = protocol::read_u32(buf, 4)?;
    let unix_secs = protocol::read_u32(buf, 8)?;
    let sequence_number = protocol::read_u32(buf, 12)?;
    let source_id = protocol::read_u32(buf, 16)?;

    // Build synthetic IPFIX raw buffer: [16-byte IPFIX header][set data from offset 20..]
    let set_data = &buf[V9_HEADER_LEN..];
    let ipfix_length = (protocol::HEADER_LEN + set_data.len()) as u16;
    let mut ipfix_buf = Vec::with_capacity(protocol::HEADER_LEN + set_data.len());

    // Write IPFIX header
    ipfix_buf.extend_from_slice(&protocol::IPFIX_VERSION.to_be_bytes());
    ipfix_buf.extend_from_slice(&ipfix_length.to_be_bytes());
    ipfix_buf.extend_from_slice(&unix_secs.to_be_bytes());
    ipfix_buf.extend_from_slice(&sequence_number.to_be_bytes());
    ipfix_buf.extend_from_slice(&source_id.to_be_bytes());
    ipfix_buf.extend_from_slice(set_data);

    // Walk FlowSets and build IPFIX sets
    let mut sets = Vec::new();
    let mut offset = V9_HEADER_LEN;
    let msg_end = buf.len();

    while offset + protocol::SET_HEADER_LEN <= msg_end {
        let v9_set_id = protocol::read_u16(buf, offset)?;
        let set_length = protocol::read_u16(buf, offset + 2)? as usize;

        if set_length < protocol::SET_HEADER_LEN {
            return Err(ParseError::SetTooSmall(set_length));
        }
        if offset + set_length > msg_end {
            return Err(ParseError::SetOverflow {
                set_len: set_length,
                remaining: msg_end - offset,
            });
        }

        let set_body = &buf[offset + protocol::SET_HEADER_LEN..offset + set_length];

        // Remap v9 set IDs to IPFIX equivalents
        let ipfix_set_id = match v9_set_id {
            V9_TEMPLATE_SET_ID => protocol::TEMPLATE_SET_ID,
            V9_OPTIONS_TEMPLATE_SET_ID => protocol::OPTIONS_TEMPLATE_SET_ID,
            id => id,
        };

        // Rewrite set_id in the IPFIX buffer
        let buf_offset = offset - V9_HEADER_LEN + protocol::HEADER_LEN;
        let id_bytes = ipfix_set_id.to_be_bytes();
        ipfix_buf[buf_offset] = id_bytes[0];
        ipfix_buf[buf_offset + 1] = id_bytes[1];

        let contents = match v9_set_id {
            V9_TEMPLATE_SET_ID => {
                // v9 field specifiers are always 4 bytes (no enterprise bit).
                // We use a dedicated parser because field IDs >= 32768 would
                // be misinterpreted as enterprise fields by the IPFIX parser.
                let templates = parse_v9_template_records(set_body)?;
                // Rewrite remapped field IDs in the raw buffer for the decoder's second pass
                rewrite_template_field_ids(
                    &templates,
                    &mut ipfix_buf,
                    buf_offset + protocol::SET_HEADER_LEN,
                );
                SetContents::Template(templates)
            }
            V9_OPTIONS_TEMPLATE_SET_ID => {
                let templates = parse_v9_options_template(set_body)?;
                SetContents::OptionsTemplate(templates)
            }
            id if id >= protocol::MIN_DATA_SET_ID => SetContents::Data(DataSet {
                template_id: id,
                records: Vec::new(),
            }),
            _ => {
                // Reserved (2-255 in v9): skip
                offset += set_length;
                continue;
            }
        };

        sets.push(Set {
            set_id: ipfix_set_id,
            contents,
        });
        offset += set_length;
    }

    let header = MessageHeader {
        version: protocol::IPFIX_VERSION,
        length: ipfix_length,
        export_time: unix_secs,
        sequence_number,
        observation_domain_id: source_id,
        protocol_version: NETFLOW_V9_VERSION,
    };

    Ok((
        IpfixMessage {
            header,
            exporter,
            sets,
        },
        ipfix_buf,
    ))
}

/// Rewrite remapped field IDs in the raw IPFIX buffer so the decoder's second
/// pass sees correct field IDs when it re-reads template bytes. Only touches
/// field IDs that were actually remapped (Cisco proprietary → IANA).
fn rewrite_template_field_ids(
    templates: &[TemplateRecord],
    ipfix_buf: &mut [u8],
    buf_set_body_start: usize,
) {
    let buf_len = ipfix_buf.len();
    let mut offset = buf_set_body_start;
    for tmpl in templates {
        // Skip template header (template_id + field_count = 4 bytes)
        offset += protocol::TEMPLATE_RECORD_HEADER_LEN;
        for spec in &tmpl.field_specifiers {
            if offset + 1 >= buf_len {
                return;
            }
            let id_bytes = spec.element_id.to_be_bytes();
            ipfix_buf[offset] = id_bytes[0];
            ipfix_buf[offset + 1] = id_bytes[1];
            // Field specs in v9 are always 4 bytes (no enterprise bit)
            offset += protocol::FIELD_SPEC_LEN;
        }
    }
}

// ---------------------------------------------------------------------------
// NetFlow v9 Template parsing (no enterprise bit)
// ---------------------------------------------------------------------------

/// Parse v9 template records. Unlike IPFIX, v9 field specifiers are always
/// 4 bytes — there is no enterprise bit or enterprise number extension.
/// This is critical: v9 field IDs >= 32768 (e.g., Cisco 40001 = 0x9C41)
/// have the high bit set, which IPFIX would misinterpret as the enterprise bit.
fn parse_v9_template_records(buf: &[u8]) -> Result<Vec<TemplateRecord>, ParseError> {
    let mut records = Vec::new();
    let mut offset = 0;

    while offset + protocol::TEMPLATE_RECORD_HEADER_LEN <= buf.len() {
        let template_id = protocol::read_u16(buf, offset)?;
        let field_count = protocol::read_u16(buf, offset + 2)? as usize;
        offset += protocol::TEMPLATE_RECORD_HEADER_LEN;

        // Template withdrawal: field_count == 0
        if field_count == 0 {
            records.push(TemplateRecord {
                template_id,
                field_specifiers: Vec::new(),
            });
            continue;
        }

        let mut field_specifiers = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            if offset + protocol::FIELD_SPEC_LEN > buf.len() {
                return Err(ParseError::BufferTooShort {
                    need: offset + protocol::FIELD_SPEC_LEN,
                    have: buf.len(),
                });
            }
            let element_id = protocol::read_u16(buf, offset)?;
            let field_length = protocol::read_u16(buf, offset + 2)?;
            offset += protocol::FIELD_SPEC_LEN;

            field_specifiers.push(FieldSpecifier {
                element_id: remap_v9_field_id(element_id),
                field_length,
                enterprise_id: 0,
            });
        }

        records.push(TemplateRecord {
            template_id,
            field_specifiers,
        });
    }

    Ok(records)
}

// ---------------------------------------------------------------------------
// NetFlow v9 Options Template parsing
// ---------------------------------------------------------------------------

/// Parse v9 Options Template FlowSet body (RFC 3954 §6.1).
///
/// v9 format differs from IPFIX:
/// - `template_id(u16) + option_scope_length(u16) + option_length(u16)`
/// - `option_scope_length` and `option_length` are in bytes (not field counts)
/// - Field specifiers are always 4 bytes (no enterprise bit)
fn parse_v9_options_template(buf: &[u8]) -> Result<Vec<OptionsTemplateRecord>, ParseError> {
    let mut records = Vec::new();
    let mut offset = 0;

    // v9 options template header: template_id(2) + scope_length(2) + option_length(2) = 6
    while offset + 6 <= buf.len() {
        let template_id = protocol::read_u16(buf, offset)?;
        let scope_length = protocol::read_u16(buf, offset + 2)? as usize;
        let option_length = protocol::read_u16(buf, offset + 4)? as usize;
        offset += 6;

        let scope_field_count = scope_length / protocol::FIELD_SPEC_LEN;
        let option_field_count = option_length / protocol::FIELD_SPEC_LEN;
        let total_field_count = scope_field_count + option_field_count;

        let mut field_specifiers = Vec::with_capacity(total_field_count);

        // Parse scope fields — v9 scope types (1-5) are NOT IANA IEs.
        // Map type 2 (Interface) → IE 10 (ingressInterface) for interface
        // name extraction. Others pass through as-is (harmless — non-interface
        // option data is silently discarded by the decoder).
        for _ in 0..scope_field_count {
            if offset + protocol::FIELD_SPEC_LEN > buf.len() {
                break;
            }
            let field_type = protocol::read_u16(buf, offset)?;
            let field_length = protocol::read_u16(buf, offset + 2)?;
            offset += protocol::FIELD_SPEC_LEN;

            // Map v9 scope type 2 (Interface) → IANA IE 10 (ingressInterface)
            let element_id = if field_type == 2 { 10 } else { field_type };
            field_specifiers.push(FieldSpecifier {
                element_id,
                field_length,
                enterprise_id: 0,
            });
        }

        // Parse option fields (standard v9 field specifiers)
        for _ in 0..option_field_count {
            if offset + protocol::FIELD_SPEC_LEN > buf.len() {
                break;
            }
            let field_type = protocol::read_u16(buf, offset)?;
            let field_length = protocol::read_u16(buf, offset + 2)?;
            offset += protocol::FIELD_SPEC_LEN;

            field_specifiers.push(FieldSpecifier {
                element_id: remap_v9_field_id(field_type),
                field_length,
                enterprise_id: 0,
            });
        }

        // Skip any padding bytes within the options template
        let expected_end = offset; // already advanced past all fields
        let actual_data = scope_length + option_length;
        if actual_data > (total_field_count * protocol::FIELD_SPEC_LEN) {
            offset = expected_end + (actual_data - total_field_count * protocol::FIELD_SPEC_LEN);
        }

        records.push(OptionsTemplateRecord {
            template_id,
            scope_field_count: scope_field_count as u16,
            field_specifiers,
        });
    }

    Ok(records)
}

// ---------------------------------------------------------------------------
// NetFlow v5 translation
// ---------------------------------------------------------------------------

/// Translate a NetFlow v5 packet into an IPFIX-compatible message and raw buffer.
///
/// v5 has no templates — we synthesize one from the fixed 48-byte record format
/// and repack records (removing padding bytes) into an IPFIX-format data set.
pub fn translate_v5(
    buf: &[u8],
    exporter: SocketAddr,
) -> Result<(IpfixMessage, Vec<u8>), ParseError> {
    if buf.len() < V5_HEADER_LEN {
        return Err(ParseError::BufferTooShort {
            need: V5_HEADER_LEN,
            have: buf.len(),
        });
    }

    // Parse v5 header (24 bytes)
    let version = protocol::read_u16(buf, 0)?;
    if version != NETFLOW_V5_VERSION {
        return Err(ParseError::InvalidVersion(version));
    }
    let count = protocol::read_u16(buf, 2)? as usize;
    let _sys_uptime = protocol::read_u32(buf, 4)?;
    let unix_secs = protocol::read_u32(buf, 8)?;
    // unix_nsecs at offset 12 — sub-second precision, not used for export_time
    let flow_sequence = protocol::read_u32(buf, 16)?;
    let engine_type = buf[20];
    let engine_id = buf[21];
    // sampling_interval at offset 22 — ignored

    // Clamp record count to what the buffer actually contains
    let max_records = (buf.len() - V5_HEADER_LEN) / V5_RECORD_LEN;
    let record_count = count.min(max_records);

    let observation_domain_id = u32::from(engine_type) << 8 | u32::from(engine_id);

    // Build the synthetic template set
    let template = TemplateRecord {
        template_id: V5_SYNTHETIC_TEMPLATE_ID,
        field_specifiers: V5_TEMPLATE.clone(),
    };
    let template_field_bytes: usize = V5_TEMPLATE.len() * protocol::FIELD_SPEC_LEN;
    let template_set_len =
        protocol::SET_HEADER_LEN + protocol::TEMPLATE_RECORD_HEADER_LEN + template_field_bytes;

    // Repack v5 records: remove pad1 (offset 36, 1 byte) and pad2 (offset 46, 2 bytes)
    let data_payload_len = record_count * V5_REPACKED_RECORD_LEN;
    let data_set_len = protocol::SET_HEADER_LEN + data_payload_len;
    let ipfix_length = (protocol::HEADER_LEN + template_set_len + data_set_len) as u16;

    let mut ipfix_buf = Vec::with_capacity(ipfix_length as usize);

    // IPFIX header (16 bytes)
    ipfix_buf.extend_from_slice(&protocol::IPFIX_VERSION.to_be_bytes());
    ipfix_buf.extend_from_slice(&ipfix_length.to_be_bytes());
    ipfix_buf.extend_from_slice(&unix_secs.to_be_bytes());
    ipfix_buf.extend_from_slice(&flow_sequence.to_be_bytes());
    ipfix_buf.extend_from_slice(&observation_domain_id.to_be_bytes());

    // Template set header
    ipfix_buf.extend_from_slice(&protocol::TEMPLATE_SET_ID.to_be_bytes());
    ipfix_buf.extend_from_slice(&(template_set_len as u16).to_be_bytes());
    // Template record header
    ipfix_buf.extend_from_slice(&V5_SYNTHETIC_TEMPLATE_ID.to_be_bytes());
    ipfix_buf.extend_from_slice(&(V5_TEMPLATE.len() as u16).to_be_bytes());
    // Template field specifiers
    for spec in V5_TEMPLATE.iter() {
        ipfix_buf.extend_from_slice(&spec.element_id.to_be_bytes());
        ipfix_buf.extend_from_slice(&spec.field_length.to_be_bytes());
    }

    // Data set header
    ipfix_buf.extend_from_slice(&V5_SYNTHETIC_TEMPLATE_ID.to_be_bytes());
    ipfix_buf.extend_from_slice(&(data_set_len as u16).to_be_bytes());

    // Repacked v5 records (remove padding)
    for i in 0..record_count {
        let rec_start = V5_HEADER_LEN + i * V5_RECORD_LEN;
        let rec = &buf[rec_start..rec_start + V5_RECORD_LEN];
        // Fields at offsets 0-35 (36 bytes: srcaddr through dstport)
        ipfix_buf.extend_from_slice(&rec[0..36]);
        // Skip pad1 at offset 36 (1 byte)
        // Fields at offsets 37-45 (9 bytes: tcp_flags through dst_mask)
        ipfix_buf.extend_from_slice(&rec[37..46]);
        // Skip pad2 at offset 46 (2 bytes)
    }

    // Build sets
    let sets = vec![
        Set {
            set_id: protocol::TEMPLATE_SET_ID,
            contents: SetContents::Template(vec![template]),
        },
        Set {
            set_id: V5_SYNTHETIC_TEMPLATE_ID,
            contents: SetContents::Data(DataSet {
                template_id: V5_SYNTHETIC_TEMPLATE_ID,
                records: Vec::new(),
            }),
        },
    ];

    let header = MessageHeader {
        version: protocol::IPFIX_VERSION,
        length: ipfix_length,
        export_time: unix_secs,
        sequence_number: flow_sequence,
        observation_domain_id,
        protocol_version: NETFLOW_V5_VERSION,
    };

    Ok((
        IpfixMessage {
            header,
            exporter,
            sets,
        },
        ipfix_buf,
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_exporter() -> SocketAddr {
        "10.0.0.1:4739".parse().unwrap()
    }

    /// Build a minimal v9 packet with one template FlowSet and one data FlowSet.
    fn build_v9_packet(
        template_id: u16,
        fields: &[(u16, u16)], // (field_type, field_length)
        data: &[u8],
    ) -> Vec<u8> {
        // Template FlowSet
        let field_bytes = fields.len() * 4;
        let template_set_len = 4 + 4 + field_bytes; // set header + tmpl header + fields
        let mut template_set = Vec::new();
        template_set.extend_from_slice(&V9_TEMPLATE_SET_ID.to_be_bytes()); // set_id = 0
        template_set.extend_from_slice(&(template_set_len as u16).to_be_bytes());
        template_set.extend_from_slice(&template_id.to_be_bytes());
        template_set.extend_from_slice(&(fields.len() as u16).to_be_bytes());
        for &(ft, fl) in fields {
            template_set.extend_from_slice(&ft.to_be_bytes());
            template_set.extend_from_slice(&fl.to_be_bytes());
        }

        // Data FlowSet
        let data_set_len = 4 + data.len();
        let mut data_set = Vec::new();
        data_set.extend_from_slice(&template_id.to_be_bytes()); // set_id = template_id
        data_set.extend_from_slice(&(data_set_len as u16).to_be_bytes());
        data_set.extend_from_slice(data);

        // v9 header (20 bytes)
        let mut pkt = Vec::new();
        pkt.extend_from_slice(&NETFLOW_V9_VERSION.to_be_bytes()); // version
        pkt.extend_from_slice(&1u16.to_be_bytes()); // count (unreliable)
        pkt.extend_from_slice(&1000u32.to_be_bytes()); // sysUptime
        pkt.extend_from_slice(&1_711_468_800_u32.to_be_bytes()); // unix_secs (2024-03-26)
        pkt.extend_from_slice(&42u32.to_be_bytes()); // sequence
        pkt.extend_from_slice(&100u32.to_be_bytes()); // source_id
        pkt.extend_from_slice(&template_set);
        pkt.extend_from_slice(&data_set);
        pkt
    }

    #[test]
    fn translate_v9_template_and_data() {
        let fields = [(8u16, 4u16), (12, 4)]; // srcIPv4, dstIPv4
        let data = [10, 0, 0, 1, 192, 168, 1, 1]; // one record: 10.0.0.1 → 192.168.1.1
        let pkt = build_v9_packet(256, &fields, &data);

        let (msg, ipfix_buf) = translate_v9(&pkt, test_exporter()).unwrap();

        assert_eq!(msg.header.protocol_version, NETFLOW_V9_VERSION);
        assert_eq!(msg.header.observation_domain_id, 100);
        assert_eq!(msg.header.export_time, 1_711_468_800);
        assert_eq!(msg.sets.len(), 2);

        // First set: template
        match &msg.sets[0].contents {
            SetContents::Template(templates) => {
                assert_eq!(templates.len(), 1);
                assert_eq!(templates[0].template_id, 256);
                assert_eq!(templates[0].field_specifiers.len(), 2);
                assert_eq!(templates[0].field_specifiers[0].element_id, 8);
                assert_eq!(templates[0].field_specifiers[1].element_id, 12);
            }
            _ => panic!("expected template set"),
        }

        // Second set: data
        match &msg.sets[1].contents {
            SetContents::Data(ds) => {
                assert_eq!(ds.template_id, 256);
                assert!(ds.records.is_empty()); // decoded later by decoder
            }
            _ => panic!("expected data set"),
        }

        // IPFIX buffer starts with valid header
        assert_eq!(ipfix_buf[0], 0x00);
        assert_eq!(ipfix_buf[1], 0x0a); // version 10
        assert_eq!(msg.header.length as usize, ipfix_buf.len());

        // Set IDs are remapped in the buffer
        let buf_set_id = u16::from_be_bytes([ipfix_buf[16], ipfix_buf[17]]);
        assert_eq!(buf_set_id, protocol::TEMPLATE_SET_ID); // 0 → 2
    }

    #[test]
    fn translate_v9_cisco_field_remap() {
        // Cisco ASA NAT fields: 40001-40004 → 225-228
        let fields = [(40001u16, 4u16), (40003, 2)];
        let data = [10, 0, 0, 1, 0, 80]; // one record
        let pkt = build_v9_packet(256, &fields, &data);

        let (msg, _) = translate_v9(&pkt, test_exporter()).unwrap();

        match &msg.sets[0].contents {
            SetContents::Template(templates) => {
                assert_eq!(templates[0].field_specifiers[0].element_id, 225);
                assert_eq!(templates[0].field_specifiers[1].element_id, 227);
            }
            _ => panic!("expected template set"),
        }
    }

    #[test]
    fn translate_v9_options_template() {
        // Build a v9 options template FlowSet
        // Scope: type=2 (Interface), length=4
        // Option: type=82 (interfaceName), length=32
        let mut pkt = Vec::new();
        // v9 header
        pkt.extend_from_slice(&NETFLOW_V9_VERSION.to_be_bytes());
        pkt.extend_from_slice(&0u16.to_be_bytes()); // count
        pkt.extend_from_slice(&1000u32.to_be_bytes()); // sysUptime
        pkt.extend_from_slice(&1_711_468_800_u32.to_be_bytes()); // unix_secs
        pkt.extend_from_slice(&1u32.to_be_bytes()); // sequence
        pkt.extend_from_slice(&1u32.to_be_bytes()); // source_id

        // Options Template FlowSet (set_id=1)
        let opts_set_len: u16 = 4 + 6 + 4 + 4; // set hdr + opts tmpl hdr + scope spec + option spec
        pkt.extend_from_slice(&V9_OPTIONS_TEMPLATE_SET_ID.to_be_bytes());
        pkt.extend_from_slice(&opts_set_len.to_be_bytes());
        // Options template record
        pkt.extend_from_slice(&257u16.to_be_bytes()); // template_id
        pkt.extend_from_slice(&4u16.to_be_bytes()); // option_scope_length (1 field × 4 bytes)
        pkt.extend_from_slice(&4u16.to_be_bytes()); // option_length (1 field × 4 bytes)
        // Scope field: type=2 (Interface), length=4
        pkt.extend_from_slice(&2u16.to_be_bytes());
        pkt.extend_from_slice(&4u16.to_be_bytes());
        // Option field: type=82 (interfaceName), length=32
        pkt.extend_from_slice(&82u16.to_be_bytes());
        pkt.extend_from_slice(&32u16.to_be_bytes());

        let (msg, _) = translate_v9(&pkt, test_exporter()).unwrap();

        match &msg.sets[0].contents {
            SetContents::OptionsTemplate(templates) => {
                assert_eq!(templates.len(), 1);
                assert_eq!(templates[0].template_id, 257);
                assert_eq!(templates[0].scope_field_count, 1);
                assert_eq!(templates[0].field_specifiers.len(), 2);
                // Scope type 2 → remapped to IE 10 (ingressInterface)
                assert_eq!(templates[0].field_specifiers[0].element_id, 10);
                // Option field 82 → interfaceName (unchanged)
                assert_eq!(templates[0].field_specifiers[1].element_id, 82);
            }
            _ => panic!("expected options template set"),
        }
    }

    /// Build a minimal v5 packet with the given number of records.
    fn build_v5_packet(record_count: u16) -> Vec<u8> {
        let mut pkt = Vec::new();
        // v5 header (24 bytes)
        pkt.extend_from_slice(&NETFLOW_V5_VERSION.to_be_bytes());
        pkt.extend_from_slice(&record_count.to_be_bytes());
        pkt.extend_from_slice(&5000u32.to_be_bytes()); // sysUptime
        pkt.extend_from_slice(&1_711_468_800_u32.to_be_bytes()); // unix_secs
        pkt.extend_from_slice(&0u32.to_be_bytes()); // unix_nsecs
        pkt.extend_from_slice(&100u32.to_be_bytes()); // flow_sequence
        pkt.push(1); // engine_type
        pkt.push(2); // engine_id
        pkt.extend_from_slice(&0u16.to_be_bytes()); // sampling_interval

        // Records (48 bytes each)
        for i in 0..record_count {
            let mut rec = [0u8; V5_RECORD_LEN];
            // srcaddr = 10.0.0.<i+1>
            rec[0..4].copy_from_slice(&[10, 0, 0, (i + 1) as u8]);
            // dstaddr = 192.168.1.<i+1>
            rec[4..8].copy_from_slice(&[192, 168, 1, (i + 1) as u8]);
            // nexthop = 10.0.0.254
            rec[8..12].copy_from_slice(&[10, 0, 0, 254]);
            // input = 1
            rec[12..14].copy_from_slice(&1u16.to_be_bytes());
            // output = 2
            rec[14..16].copy_from_slice(&2u16.to_be_bytes());
            // dPkts = 100
            rec[16..20].copy_from_slice(&100u32.to_be_bytes());
            // dOctets = 15000
            rec[20..24].copy_from_slice(&15000u32.to_be_bytes());
            // first = 1000 (sysUptime ms)
            rec[24..28].copy_from_slice(&1000u32.to_be_bytes());
            // last = 4000 (sysUptime ms)
            rec[28..32].copy_from_slice(&4000u32.to_be_bytes());
            // srcport = 12345
            rec[32..34].copy_from_slice(&12345u16.to_be_bytes());
            // dstport = 80
            rec[34..36].copy_from_slice(&80u16.to_be_bytes());
            // pad1 = 0
            rec[36] = 0;
            // tcp_flags = SYN|ACK (0x12)
            rec[37] = 0x12;
            // prot = TCP (6)
            rec[38] = 6;
            // tos = 0
            rec[39] = 0;
            // src_as = 64512
            rec[40..42].copy_from_slice(&64512u16.to_be_bytes());
            // dst_as = 65001
            rec[42..44].copy_from_slice(&65001u16.to_be_bytes());
            // src_mask = 24
            rec[44] = 24;
            // dst_mask = 16
            rec[45] = 16;
            // pad2 = 0
            rec[46..48].copy_from_slice(&[0, 0]);
            pkt.extend_from_slice(&rec);
        }

        pkt
    }

    #[test]
    fn translate_v5_packet() {
        let pkt = build_v5_packet(2);
        let (msg, ipfix_buf) = translate_v5(&pkt, test_exporter()).unwrap();

        assert_eq!(msg.header.protocol_version, NETFLOW_V5_VERSION);
        assert_eq!(msg.header.export_time, 1_711_468_800);
        // observation_domain_id = (engine_type << 8) | engine_id = (1 << 8) | 2 = 258
        assert_eq!(msg.header.observation_domain_id, 258);
        assert_eq!(msg.sets.len(), 2);

        // Template set
        match &msg.sets[0].contents {
            SetContents::Template(templates) => {
                assert_eq!(templates.len(), 1);
                assert_eq!(templates[0].template_id, V5_SYNTHETIC_TEMPLATE_ID);
                assert_eq!(templates[0].field_specifiers.len(), 18);
            }
            _ => panic!("expected template set"),
        }

        // Data set
        match &msg.sets[1].contents {
            SetContents::Data(ds) => {
                assert_eq!(ds.template_id, V5_SYNTHETIC_TEMPLATE_ID);
            }
            _ => panic!("expected data set"),
        }

        // Buffer is well-formed
        assert_eq!(msg.header.length as usize, ipfix_buf.len());
    }

    #[test]
    fn v5_field_mapping() {
        let specs = &*V5_TEMPLATE;
        assert_eq!(specs.len(), 18);

        // Verify IE IDs and wire lengths for all fields
        let expected: &[(u16, u16)] = &[
            (8, 4),  // sourceIPv4Address
            (12, 4), // destinationIPv4Address
            (15, 4), // ipNextHopIPv4Address
            (10, 2), // ingressInterface (reduced-size)
            (14, 2), // egressInterface (reduced-size)
            (2, 4),  // packetDeltaCount (reduced-size)
            (1, 4),  // octetDeltaCount (reduced-size)
            (22, 4), // flowStartSysUpTime
            (21, 4), // flowEndSysUpTime
            (7, 2),  // sourceTransportPort
            (11, 2), // destinationTransportPort
            (6, 1),  // tcpControlBits (reduced-size)
            (4, 1),  // protocolIdentifier
            (5, 1),  // ipClassOfService
            (16, 2), // bgpSourceAsNumber (reduced-size)
            (17, 2), // bgpDestinationAsNumber (reduced-size)
            (9, 1),  // sourceIPv4PrefixLength
            (13, 1), // destinationIPv4PrefixLength
        ];

        for (i, &(ie, len)) in expected.iter().enumerate() {
            assert_eq!(specs[i].element_id, ie, "field {i} element_id");
            assert_eq!(specs[i].field_length, len, "field {i} field_length");
            assert_eq!(specs[i].enterprise_id, 0, "field {i} enterprise_id");
        }
    }

    #[test]
    fn v5_repacked_record_length() {
        // Verify constant matches actual sum of field lengths
        let total: usize = V5_TEMPLATE.iter().map(|s| s.field_length as usize).sum();
        assert_eq!(total, V5_REPACKED_RECORD_LEN);
    }

    #[test]
    fn v5_template_has_duration_fields() {
        // V5_TEMPLATE must contain IE 22 (flowStartSysUpTime) and IE 21
        // (flowEndSysUpTime) so DurationSource::SysUpTime is detected by
        // the storage layer's detect_duration_source().
        let has_start = V5_TEMPLATE.iter().any(|s| s.element_id == 22);
        let has_end = V5_TEMPLATE.iter().any(|s| s.element_id == 21);
        assert!(
            has_start,
            "v5 template should have IE 22 (flowStartSysUpTime)"
        );
        assert!(has_end, "v5 template should have IE 21 (flowEndSysUpTime)");
    }

    #[test]
    fn translate_v9_flowset_padding() {
        // Build a v9 packet where the template FlowSet has 2 bytes of padding
        // to align to 4-byte boundary. Template has 1 field (4 bytes) + header (4 bytes)
        // = 8 bytes body, already aligned, so let's use 3 fields (12 bytes body) + 4 header
        // = 16 total, already aligned. Instead, test with data set padding.
        let fields = [(4u16, 1u16)]; // protocolIdentifier, 1 byte
        // One 1-byte record + 3 bytes padding to align to 4 bytes
        let data = [6, 0, 0, 0]; // TCP + 3 padding bytes
        let pkt = build_v9_packet(256, &fields, &data);

        let (msg, _) = translate_v9(&pkt, test_exporter()).unwrap();

        // Should parse successfully without error
        assert_eq!(msg.sets.len(), 2);
        match &msg.sets[1].contents {
            SetContents::Data(ds) => {
                assert_eq!(ds.template_id, 256);
            }
            _ => panic!("expected data set"),
        }
    }

    #[test]
    fn v9_rejects_wrong_version() {
        let mut pkt = build_v9_packet(256, &[(8, 4)], &[0; 4]);
        pkt[0] = 0x00;
        pkt[1] = 0x0a; // change to IPFIX version
        assert!(translate_v9(&pkt, test_exporter()).is_err());
    }

    #[test]
    fn v5_rejects_wrong_version() {
        let mut pkt = build_v5_packet(1);
        pkt[0] = 0x00;
        pkt[1] = 0x09; // change to v9
        assert!(translate_v5(&pkt, test_exporter()).is_err());
    }

    #[test]
    fn v5_empty_packet() {
        let pkt = build_v5_packet(0);
        let (msg, ipfix_buf) = translate_v5(&pkt, test_exporter()).unwrap();
        assert_eq!(msg.sets.len(), 2);
        assert_eq!(msg.header.length as usize, ipfix_buf.len());
    }
}
