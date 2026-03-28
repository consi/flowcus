//! RFC compliance tests for the IPFIX implementation.
//!
//! Each test references the specific RFC section it validates.
//! Tests use raw byte arrays to construct IPFIX messages on the wire.

#![allow(
    clippy::cast_possible_truncation,
    clippy::redundant_clone,
    clippy::doc_markdown,
    clippy::unusual_byte_groupings,
    clippy::clone_on_ref_ptr,
    clippy::format_push_string,
    clippy::assigning_clones,
    clippy::decimal_literal_representation,
    clippy::clone_on_copy
)]

use std::net::SocketAddr;

use flowcus_ipfix::decoder::decode_message;
use flowcus_ipfix::ie;
use flowcus_ipfix::protocol::{
    self, ENTERPRISE_BIT, FieldSpecifier, FieldValue, IPFIX_VERSION, MessageHeader, SET_HEADER_LEN,
    SetContents, read_variable_length,
};
use flowcus_ipfix::session::SessionStore;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn addr() -> SocketAddr {
    "10.0.0.1:4739".parse().unwrap()
}

fn build_header(length: u16, seq: u32, domain: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&IPFIX_VERSION.to_be_bytes()); // version 0x000a
    buf.extend_from_slice(&length.to_be_bytes());
    buf.extend_from_slice(&1_700_000_000u32.to_be_bytes()); // export time
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(&domain.to_be_bytes());
    buf
}

fn build_msg_with_sets(sets: &[Vec<u8>], seq: u32, domain: u32) -> Vec<u8> {
    let mut msg = build_header(0, seq, domain);
    for set in sets {
        msg.extend_from_slice(set);
    }
    let len = msg.len() as u16;
    msg[2..4].copy_from_slice(&len.to_be_bytes());
    msg
}

/// Build a Template Set (set_id=2) containing one or more templates.
/// Each template is (template_id, &[(element_id, field_length)]).
fn build_template_set(templates: &[(u16, &[(u16, u16)])]) -> Vec<u8> {
    let mut body = Vec::new();
    for (template_id, fields) in templates {
        body.extend_from_slice(&template_id.to_be_bytes());
        body.extend_from_slice(&(fields.len() as u16).to_be_bytes());
        for (ie_id, ie_len) in *fields {
            body.extend_from_slice(&ie_id.to_be_bytes());
            body.extend_from_slice(&ie_len.to_be_bytes());
        }
    }
    let set_length = (SET_HEADER_LEN + body.len()) as u16;
    let mut set = Vec::new();
    set.extend_from_slice(&2u16.to_be_bytes()); // set_id = TEMPLATE_SET_ID
    set.extend_from_slice(&set_length.to_be_bytes());
    set.extend_from_slice(&body);
    set
}

/// Build a Data Set with the given template_id and raw record bytes.
fn build_data_set(template_id: u16, records: &[&[u8]]) -> Vec<u8> {
    let mut body = Vec::new();
    for rec in records {
        body.extend_from_slice(rec);
    }
    let set_length = (SET_HEADER_LEN + body.len()) as u16;
    let mut set = Vec::new();
    set.extend_from_slice(&template_id.to_be_bytes());
    set.extend_from_slice(&set_length.to_be_bytes());
    set.extend_from_slice(&body);
    set
}

/// Build a template withdrawal record (field_count=0) inside a Template Set.
fn build_withdrawal_set(template_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&template_id.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes()); // field_count = 0
    let set_length = (SET_HEADER_LEN + body.len()) as u16;
    let mut set = Vec::new();
    set.extend_from_slice(&2u16.to_be_bytes()); // set_id = TEMPLATE_SET_ID
    set.extend_from_slice(&set_length.to_be_bytes());
    set.extend_from_slice(&body);
    set
}

/// Build a set with an arbitrary set_id and raw body.
fn build_raw_set(set_id: u16, body: &[u8]) -> Vec<u8> {
    let set_length = (SET_HEADER_LEN + body.len()) as u16;
    let mut set = Vec::new();
    set.extend_from_slice(&set_id.to_be_bytes());
    set.extend_from_slice(&set_length.to_be_bytes());
    set.extend_from_slice(body);
    set
}

// ===========================================================================
// RFC 7011 Section 3.1 — Message Header
// ===========================================================================

#[test]
fn test_rfc7011_3_1_reject_version_zero() {
    // RFC 7011 Section 3.1: "Version ... MUST be the value 10."
    // Version 0x0000 must be rejected.
    let mut buf = build_header(16, 0, 0);
    buf[0] = 0x00;
    buf[1] = 0x00; // version = 0
    assert!(MessageHeader::parse(&buf).is_err());
}

#[test]
fn test_rfc7011_3_1_reject_netflow_v5() {
    // RFC 7011 Section 3.1: "Version ... MUST be the value 10."
    // NetFlow v5 (0x0005) must be rejected.
    let mut buf = build_header(16, 0, 0);
    buf[0] = 0x00;
    buf[1] = 0x05; // version = 5
    assert!(MessageHeader::parse(&buf).is_err());
}

#[test]
fn test_rfc7011_3_1_accept_version_10() {
    // RFC 7011 Section 3.1: "Version ... MUST be the value 10."
    let buf = build_header(16, 42, 1);
    let hdr = MessageHeader::parse(&buf).unwrap();
    assert_eq!(hdr.version, 0x000a);
    assert_eq!(hdr.length, 16);
    assert_eq!(hdr.sequence_number, 42);
    assert_eq!(hdr.observation_domain_id, 1);
}

#[test]
fn test_rfc7011_3_1_length_exceeds_buffer() {
    // RFC 7011 Section 3.1: "Length ... Total length of the IPFIX Message,
    // measured in octets." If header.length > buffer length, this is an error.
    let buf = build_header(200, 0, 0); // claims 200 bytes but buffer is only 16
    let result = protocol::parse_message(&buf, addr());
    assert!(result.is_err());
}

#[test]
fn test_rfc7011_3_1_trailing_bytes_ignored() {
    // RFC 7011 Section 3.1: The message length field defines the boundary.
    // Bytes beyond header.length must not be parsed.
    let mut buf = build_header(16, 0, 0); // length = 16 (header only)
    buf.extend_from_slice(&[0xFF; 32]); // trailing garbage
    let msg = protocol::parse_message(&buf, addr()).unwrap();
    assert!(msg.sets.is_empty());
}

// ===========================================================================
// RFC 7011 Section 3.3 — Set Format
// ===========================================================================

#[test]
fn test_rfc7011_3_3_set_length_zero() {
    // RFC 7011 Section 3.3: "Every Set Header field contains the length of the
    // set." A length of zero is invalid (minimum is 4 for the header itself).
    let mut msg = build_header(0, 0, 0);
    // Manually craft a set with length=0
    msg.extend_from_slice(&256u16.to_be_bytes()); // set_id
    msg.extend_from_slice(&0u16.to_be_bytes()); // set_length = 0
    let len = msg.len() as u16;
    msg[2..4].copy_from_slice(&len.to_be_bytes());
    assert!(protocol::parse_message(&msg, addr()).is_err());
}

#[test]
fn test_rfc7011_3_3_set_length_below_minimum() {
    // RFC 7011 Section 3.3: Set length must be >= 4 (the set header size).
    // A length of 2 is invalid.
    let mut msg = build_header(0, 0, 0);
    msg.extend_from_slice(&256u16.to_be_bytes()); // set_id
    msg.extend_from_slice(&2u16.to_be_bytes()); // set_length = 2 (too small)
    let len = msg.len() as u16;
    msg[2..4].copy_from_slice(&len.to_be_bytes());
    assert!(protocol::parse_message(&msg, addr()).is_err());
}

#[test]
fn test_rfc7011_3_3_two_sets_parsed() {
    // RFC 7011 Section 3.3: "An IPFIX Message ... consists of a Message Header,
    // followed by zero or more Sets." Multiple sets must all be parsed.
    let tmpl_set = build_template_set(&[(256, &[(8, 4), (12, 4)])]);
    let data_set = build_data_set(256, &[&[192, 168, 1, 1, 10, 0, 0, 1]]);
    let msg = build_msg_with_sets(&[tmpl_set, data_set], 1, 1);
    let parsed = protocol::parse_message(&msg, addr()).unwrap();
    assert_eq!(parsed.sets.len(), 2);
}

#[test]
fn test_rfc7011_3_3_reserved_set_id_4_skipped() {
    // RFC 7011 Section 3.3: "Set IDs 4-255 are reserved for future use.
    // The Collecting Process MUST ignore Sets with reserved Set IDs."
    let reserved_set = build_raw_set(4, &[0xDE, 0xAD, 0xBE, 0xEF]);
    let msg = build_msg_with_sets(&[reserved_set], 0, 0);
    let parsed = protocol::parse_message(&msg, addr()).unwrap();
    // Reserved set should be silently skipped — no sets in the result
    assert_eq!(parsed.sets.len(), 0);
}

#[test]
fn test_rfc7011_3_3_reserved_set_id_100_skipped() {
    // RFC 7011 Section 3.3: "Set IDs 4-255 are reserved for future use."
    let reserved_set = build_raw_set(100, &[0x00; 8]);
    let msg = build_msg_with_sets(&[reserved_set], 0, 0);
    let parsed = protocol::parse_message(&msg, addr()).unwrap();
    assert_eq!(parsed.sets.len(), 0);
}

#[test]
fn test_rfc7011_3_3_reserved_set_id_255_skipped() {
    // RFC 7011 Section 3.3: "Set IDs 4-255 are reserved for future use."
    let reserved_set = build_raw_set(255, &[0x00; 4]);
    let msg = build_msg_with_sets(&[reserved_set], 0, 0);
    let parsed = protocol::parse_message(&msg, addr()).unwrap();
    assert_eq!(parsed.sets.len(), 0);
}

#[test]
fn test_rfc7011_3_3_reserved_set_id_between_valid_sets() {
    // RFC 7011 Section 3.3: Reserved sets between valid sets must be skipped,
    // but the valid sets on either side must still be parsed correctly.
    let tmpl_set1 = build_template_set(&[(256, &[(8, 4)])]);
    let reserved = build_raw_set(100, &[0x00; 4]);
    let tmpl_set2 = build_template_set(&[(257, &[(12, 4)])]);
    let msg = build_msg_with_sets(&[tmpl_set1, reserved, tmpl_set2], 0, 0);
    let parsed = protocol::parse_message(&msg, addr()).unwrap();
    // Only the two template sets should appear (reserved is skipped)
    assert_eq!(parsed.sets.len(), 2);
    if let SetContents::Template(ref t) = parsed.sets[0].contents {
        assert_eq!(t[0].template_id, 256);
    } else {
        panic!("expected template set at index 0");
    }
    if let SetContents::Template(ref t) = parsed.sets[1].contents {
        assert_eq!(t[0].template_id, 257);
    } else {
        panic!("expected template set at index 1");
    }
}

// ===========================================================================
// RFC 7011 Section 3.2 — Field Specifier
// ===========================================================================

#[test]
fn test_rfc7011_3_2_standard_field_4_bytes() {
    // RFC 7011 Section 3.2: "If the Information Element is an IETF IE, then
    // the Enterprise bit is 0 and the field specifier is 4 octets."
    let mut buf = Vec::new();
    buf.extend_from_slice(&8u16.to_be_bytes()); // IE 8
    buf.extend_from_slice(&4u16.to_be_bytes()); // length 4
    let (spec, consumed) = FieldSpecifier::parse(&buf, 0).unwrap();
    assert_eq!(consumed, 4);
    assert_eq!(spec.element_id, 8);
    assert_eq!(spec.field_length, 4);
    assert_eq!(spec.enterprise_id, 0);
    assert!(!spec.is_enterprise());
}

#[test]
fn test_rfc7011_3_2_enterprise_field_8_bytes() {
    // RFC 7011 Section 3.2: "If the Enterprise bit is set ... the Field
    // Specifier is 8 octets, including the Enterprise Number."
    let mut buf = Vec::new();
    let raw_id = 0x002Au16 | 0x8000;
    buf.extend_from_slice(&raw_id.to_be_bytes());
    buf.extend_from_slice(&8u16.to_be_bytes()); // length 8
    buf.extend_from_slice(&12345u32.to_be_bytes()); // enterprise ID
    let (spec, consumed) = FieldSpecifier::parse(&buf, 0).unwrap();
    assert_eq!(consumed, 8);
    assert_eq!(spec.element_id, 42);
    assert_eq!(spec.field_length, 8);
    assert_eq!(spec.enterprise_id, 12345);
    assert!(spec.is_enterprise());
}

#[test]
fn test_rfc7011_3_2_enterprise_bit_stripped() {
    // RFC 7011 Section 3.2: "Information Element identifier ... Bit 14 to Bit 0."
    // The enterprise bit (bit 15) must be stripped from the element_id.
    let mut buf = Vec::new();
    let raw_id: u16 = 0x8001; // enterprise bit set, element_id = 1
    buf.extend_from_slice(&raw_id.to_be_bytes());
    buf.extend_from_slice(&4u16.to_be_bytes());
    buf.extend_from_slice(&9u32.to_be_bytes()); // Cisco PEN
    let (spec, _) = FieldSpecifier::parse(&buf, 0).unwrap();
    assert_eq!(spec.element_id, 1);
}

// ===========================================================================
// RFC 7011 Section 3.4 — Templates
// ===========================================================================

#[test]
fn test_rfc7011_3_4_template_id_256_accepted() {
    // RFC 7011 Section 3.4.1: "Template IDs 0-255 are reserved ... Each
    // Template Record is given a unique Template ID in the range 256 to 65535."
    let tmpl_set = build_template_set(&[(256, &[(8, 4)])]);
    let msg = build_msg_with_sets(&[tmpl_set], 0, 0);
    let parsed = protocol::parse_message(&msg, addr()).unwrap();
    if let SetContents::Template(ref t) = parsed.sets[0].contents {
        assert_eq!(t[0].template_id, 256);
    } else {
        panic!("expected template set");
    }
}

#[test]
fn test_rfc7011_3_4_template_id_65535_accepted() {
    // RFC 7011 Section 3.4.1: "Each Template Record is given a unique Template
    // ID in the range 256 to 65535."
    let tmpl_set = build_template_set(&[(65535, &[(8, 4)])]);
    let msg = build_msg_with_sets(&[tmpl_set], 0, 0);
    let parsed = protocol::parse_message(&msg, addr()).unwrap();
    if let SetContents::Template(ref t) = parsed.sets[0].contents {
        assert_eq!(t[0].template_id, 65535);
    } else {
        panic!("expected template set");
    }
}

#[test]
fn test_rfc7011_3_4_nonsequential_template_ids() {
    // RFC 7011 Section 3.4.1: Template IDs need not be sequential.
    // IDs 300, 256, 500 must all be stored successfully.
    let mut session = SessionStore::new(1800);
    let ex = addr();
    let domain = 1;

    for &id in &[300u16, 256, 500] {
        let tmpl_set = build_template_set(&[(id, &[(8, 4), (12, 4)])]);
        let raw = build_msg_with_sets(&[tmpl_set], 0, domain);
        let mut msg = protocol::parse_message(&raw, ex).unwrap();
        decode_message(&mut msg, &raw, &mut session);
    }

    assert!(session.get_template(ex, domain, 300).is_some());
    assert!(session.get_template(ex, domain, 256).is_some());
    assert!(session.get_template(ex, domain, 500).is_some());
    assert_eq!(session.template_count(), 3);
}

// ===========================================================================
// RFC 7011 Section 7 — Variable-Length Information Elements
// ===========================================================================

#[test]
fn test_rfc7011_7_variable_length_zero() {
    // RFC 7011 Section 7: "If the length ... is less than 255 octets, the
    // length is encoded in a single octet." Length 0 → empty bytes.
    let buf = vec![0u8]; // length = 0
    let (data, consumed) = read_variable_length(&buf, 0).unwrap();
    assert_eq!(data.len(), 0);
    assert_eq!(consumed, 1); // only the length byte consumed
}

#[test]
fn test_rfc7011_7_variable_length_254_boundary() {
    // RFC 7011 Section 7: "If the length ... is less than 255 octets, the
    // length is encoded in a single octet." 254 is the maximum short form.
    let mut buf = vec![254u8]; // length = 254
    buf.extend_from_slice(&vec![0xAB; 254]);
    let (data, consumed) = read_variable_length(&buf, 0).unwrap();
    assert_eq!(data.len(), 254);
    assert_eq!(consumed, 255); // 1 byte length + 254 bytes data
    assert!(data.iter().all(|&b| b == 0xAB));
}

#[test]
fn test_rfc7011_7_variable_length_255_long_form() {
    // RFC 7011 Section 7: "If the length is 255 or greater ... the first
    // octet of the Length field MUST be 255, and the length is carried in
    // the second and third octets."
    let payload_len: u16 = 300;
    let mut buf = vec![255u8]; // marker for long form
    buf.extend_from_slice(&payload_len.to_be_bytes());
    buf.extend_from_slice(&vec![0xCD; 300]);
    let (data, consumed) = read_variable_length(&buf, 0).unwrap();
    assert_eq!(data.len(), 300);
    assert_eq!(consumed, 303); // 1 marker + 2 length + 300 data
}

#[test]
fn test_rfc7011_7_variable_length_truncated() {
    // RFC 7011 Section 7: If the buffer is too short for the declared length,
    // parsing must fail.
    let buf = vec![10u8, 0x01, 0x02]; // claims 10 bytes but only 2 available
    let result = read_variable_length(&buf, 0);
    assert!(result.is_err());
}

// ===========================================================================
// RFC 7011 Section 8 — Template Management
// ===========================================================================

#[test]
fn test_rfc7011_8_template_persists() {
    // RFC 7011 Section 8: "Templates ... are kept and used by the Collecting
    // Process until they are ... withdrawn." Templates must persist across messages.
    let mut session = SessionStore::new(1800);
    let ex = addr();
    let domain = 1;

    // Message 1: send template
    let tmpl_set = build_template_set(&[(256, &[(8, 4), (12, 4)])]);
    let raw1 = build_msg_with_sets(&[tmpl_set], 1, domain);
    let mut msg1 = protocol::parse_message(&raw1, ex).unwrap();
    decode_message(&mut msg1, &raw1, &mut session);

    // Template should still be available
    assert!(session.get_template(ex, domain, 256).is_some());

    // Message 2: no template, just data — template should still be there
    let data_set = build_data_set(256, &[&[10, 0, 0, 1, 172, 16, 0, 1]]);
    let raw2 = build_msg_with_sets(&[data_set], 2, domain);
    let mut msg2 = protocol::parse_message(&raw2, ex).unwrap();
    decode_message(&mut msg2, &raw2, &mut session);

    // Template still present
    assert!(session.get_template(ex, domain, 256).is_some());
}

#[test]
fn test_rfc7011_8_1_withdrawal_removes_template() {
    // RFC 7011 Section 8.1: "Template Withdrawal ... field count of 0 ...
    // requests the withdrawal of the identified Template."
    let mut session = SessionStore::new(1800);
    let ex = addr();
    let domain = 1;

    // Register template
    let tmpl_set = build_template_set(&[(256, &[(8, 4)])]);
    let raw = build_msg_with_sets(&[tmpl_set], 1, domain);
    let mut msg = protocol::parse_message(&raw, ex).unwrap();
    decode_message(&mut msg, &raw, &mut session);
    assert_eq!(session.template_count(), 1);

    // Withdraw it
    let withdrawal = build_withdrawal_set(256);
    let raw2 = build_msg_with_sets(&[withdrawal], 2, domain);
    let mut msg2 = protocol::parse_message(&raw2, ex).unwrap();
    decode_message(&mut msg2, &raw2, &mut session);
    assert_eq!(session.template_count(), 0);
    assert!(session.get_template(ex, domain, 256).is_none());
}

#[test]
fn test_rfc7011_8_1_withdrawal_unknown_ignored() {
    // RFC 7011 Section 8.1: Withdrawing a template that does not exist
    // must not cause a panic or error.
    let mut session = SessionStore::new(1800);
    let ex = addr();

    let withdrawal = build_withdrawal_set(999);
    let raw = build_msg_with_sets(&[withdrawal], 1, 1);
    let mut msg = protocol::parse_message(&raw, ex).unwrap();
    decode_message(&mut msg, &raw, &mut session);
    // Should succeed without panic
    assert_eq!(session.template_count(), 0);
}

#[test]
fn test_rfc7011_8_1_template_scoped_to_domain() {
    // RFC 7011 Section 8: "Templates ... are scoped to the Transport Session
    // and the Observation Domain." A template registered in domain 1 must not
    // be visible in domain 2.
    let mut session = SessionStore::new(1800);
    let ex = addr();

    let tmpl_set = build_template_set(&[(256, &[(8, 4)])]);
    let raw = build_msg_with_sets(&[tmpl_set], 1, 1); // domain = 1
    let mut msg = protocol::parse_message(&raw, ex).unwrap();
    decode_message(&mut msg, &raw, &mut session);

    assert!(session.get_template(ex, 1, 256).is_some());
    assert!(session.get_template(ex, 2, 256).is_none()); // different domain
}

#[test]
fn test_rfc7011_8_1_template_scoped_to_exporter() {
    // RFC 7011 Section 8: Templates are scoped to the Transport Session.
    // Different exporters have separate template spaces.
    let mut session = SessionStore::new(1800);
    let ex1: SocketAddr = "10.0.0.1:4739".parse().unwrap();
    let ex2: SocketAddr = "10.0.0.2:4739".parse().unwrap();

    let tmpl_set = build_template_set(&[(256, &[(8, 4)])]);

    let tmpl_set_copy = tmpl_set.clone();
    let raw1 = build_msg_with_sets(&[tmpl_set_copy], 1, 1);
    let mut msg1 = protocol::parse_message(&raw1, ex1).unwrap();
    decode_message(&mut msg1, &raw1, &mut session);

    assert!(session.get_template(ex1, 1, 256).is_some());
    assert!(session.get_template(ex2, 1, 256).is_none()); // different exporter
}

#[test]
fn test_rfc7011_8_2_template_before_data_same_msg() {
    // RFC 7011 Section 8: "The Exporting Process SHOULD export the Template
    // Set ... before the first Data Set that uses that Template ID."
    // Template and data in the same message must be decoded correctly.
    let tmpl_set = build_template_set(&[(256, &[(8, 4), (12, 4)])]);
    let data_set = build_data_set(256, &[&[192, 168, 1, 1, 10, 0, 0, 1]]);
    let raw = build_msg_with_sets(&[tmpl_set, data_set], 1, 1);

    let mut msg = protocol::parse_message(&raw, addr()).unwrap();
    let mut session = SessionStore::new(1800);
    decode_message(&mut msg, &raw, &mut session);

    // Find the data set
    let data = msg.sets.iter().find_map(|s| {
        if let SetContents::Data(ds) = &s.contents {
            Some(ds)
        } else {
            None
        }
    });
    let data = data.expect("should have a data set");
    assert_eq!(data.records.len(), 1);
    assert_eq!(data.records[0].fields.len(), 2);
    assert_eq!(data.records[0].fields[0].value.to_string(), "192.168.1.1");
    assert_eq!(data.records[0].fields[1].value.to_string(), "10.0.0.1");
}

// ===========================================================================
// RFC 7011 Section 3.3.1 — Padding
// ===========================================================================

#[test]
fn test_rfc7011_3_3_1_padding_shorter_than_record() {
    // RFC 7011 Section 3.3.1: "The Exporting Process MAY insert some octets
    // of padding ... The padding length MUST be shorter than any allowable
    // record in this Set." Trailing zeros that are shorter than a record must
    // be handled (ignored, not decoded as a record).
    let tmpl_set = build_template_set(&[(256, &[(8, 4), (12, 4)])]);
    // One valid record (8 bytes) + 3 bytes of padding (shorter than min record = 8)
    let mut data_body: Vec<u8> = Vec::new();
    data_body.extend_from_slice(&[192, 168, 1, 1, 10, 0, 0, 1]); // valid record
    data_body.extend_from_slice(&[0, 0, 0]); // 3 bytes padding
    let data_set = build_data_set(256, &[&data_body]);
    let raw = build_msg_with_sets(&[tmpl_set, data_set], 1, 1);

    let mut msg = protocol::parse_message(&raw, addr()).unwrap();
    let mut session = SessionStore::new(1800);
    decode_message(&mut msg, &raw, &mut session);

    let data = msg.sets.iter().find_map(|s| {
        if let SetContents::Data(ds) = &s.contents {
            Some(ds)
        } else {
            None
        }
    });
    let data = data.expect("should have data set");
    // Only 1 record decoded; padding bytes are ignored
    assert_eq!(data.records.len(), 1);
}

// ===========================================================================
// RFC 5153 — Implementation Guidelines
// ===========================================================================

#[test]
fn test_rfc5153_unknown_ie_decoded_as_bytes() {
    // RFC 5153 Section 6.1: "If the Collecting Process receives a Data Set
    // for which it does not have a Template ... skip the Data Set."
    // Also: unknown enterprise IEs should be decoded as OctetArray / bytes.
    let enterprise_id: u32 = 99999;
    let element_id: u16 = 42;

    // Build a template with an enterprise field
    let mut tmpl_body = Vec::new();
    tmpl_body.extend_from_slice(&256u16.to_be_bytes()); // template_id
    tmpl_body.extend_from_slice(&1u16.to_be_bytes()); // field_count
    // Enterprise field specifier: enterprise bit set
    let raw_ie = element_id | ENTERPRISE_BIT;
    tmpl_body.extend_from_slice(&raw_ie.to_be_bytes());
    tmpl_body.extend_from_slice(&3u16.to_be_bytes()); // field_length = 3
    tmpl_body.extend_from_slice(&enterprise_id.to_be_bytes());

    let set_len = (SET_HEADER_LEN + tmpl_body.len()) as u16;
    let mut tmpl_set = Vec::new();
    tmpl_set.extend_from_slice(&2u16.to_be_bytes());
    tmpl_set.extend_from_slice(&set_len.to_be_bytes());
    tmpl_set.extend_from_slice(&tmpl_body);

    let data_set = build_data_set(256, &[&[0xDE, 0xAD, 0xBE]]);
    let raw = build_msg_with_sets(&[tmpl_set, data_set], 1, 1);

    let mut msg = protocol::parse_message(&raw, addr()).unwrap();
    let mut session = SessionStore::new(1800);
    decode_message(&mut msg, &raw, &mut session);

    let data = msg.sets.iter().find_map(|s| {
        if let SetContents::Data(ds) = &s.contents {
            Some(ds)
        } else {
            None
        }
    });
    let data = data.expect("should have data set");
    assert_eq!(data.records.len(), 1);
    // Unknown enterprise IE → OctetArray → FieldValue::Bytes
    match &data.records[0].fields[0].value {
        FieldValue::Bytes(b) => assert_eq!(b, &[0xDE, 0xAD, 0xBE]),
        other => panic!("expected Bytes, got {other:?}"),
    }
}

#[test]
fn test_rfc5153_unknown_ie_has_generated_name() {
    // RFC 5153: Unknown IEs should have a generated name following the
    // pen{N}.ie{M} naming convention for enterprise IEs.
    let name = ie::name(42, 99999);
    assert_eq!(name, "pen99999.ie42");

    // IANA unknown uses ie{N}
    let name_iana = ie::name(65534, 0);
    assert_eq!(name_iana, "ie65534");
}

#[test]
fn test_rfc5153_data_without_template_skipped() {
    // RFC 5153 Section 6.1: "If the Collecting Process receives a Data Set
    // for which no corresponding Template has been received, it SHOULD store
    // the Data Set temporarily and try to decode it after the next Template
    // ... An implementation MAY choose to discard the Data Set."
    // Our implementation skips data sets without templates.
    let data_set = build_data_set(256, &[&[10, 0, 0, 1, 172, 16, 0, 1]]);
    let raw = build_msg_with_sets(&[data_set], 1, 1);

    let mut msg = protocol::parse_message(&raw, addr()).unwrap();
    let mut session = SessionStore::new(1800);
    decode_message(&mut msg, &raw, &mut session);

    // Data set is present but records should be empty (no template to decode)
    let data = msg.sets.iter().find_map(|s| {
        if let SetContents::Data(ds) = &s.contents {
            Some(ds)
        } else {
            None
        }
    });
    let data = data.expect("data set should exist structurally");
    assert_eq!(data.records.len(), 0);
}

// ===========================================================================
// Decoder integration
// ===========================================================================

#[test]
fn test_decoder_multiple_records_in_data_set() {
    // RFC 7011 Section 3.4.1: "Data Records ... multiple records within a
    // single Data Set." Three records must all be decoded.
    let tmpl_set = build_template_set(&[(256, &[(8, 4), (12, 4)])]);
    let rec1: &[u8] = &[10, 0, 0, 1, 172, 16, 0, 1];
    let rec2: &[u8] = &[10, 0, 0, 2, 172, 16, 0, 2];
    let rec3: &[u8] = &[10, 0, 0, 3, 172, 16, 0, 3];
    let data_set = build_data_set(256, &[rec1, rec2, rec3]);
    let raw = build_msg_with_sets(&[tmpl_set, data_set], 1, 1);

    let mut msg = protocol::parse_message(&raw, addr()).unwrap();
    let mut session = SessionStore::new(1800);
    decode_message(&mut msg, &raw, &mut session);

    let data = msg.sets.iter().find_map(|s| {
        if let SetContents::Data(ds) = &s.contents {
            Some(ds)
        } else {
            None
        }
    });
    let data = data.expect("should have data set");
    assert_eq!(data.records.len(), 3);
    assert_eq!(data.records[0].fields[0].value.to_string(), "10.0.0.1");
    assert_eq!(data.records[1].fields[0].value.to_string(), "10.0.0.2");
    assert_eq!(data.records[2].fields[0].value.to_string(), "10.0.0.3");
}

#[test]
fn test_decoder_template_and_data_different_messages() {
    // RFC 7011 Section 8: Template sent in message 1, data in message 2.
    // The session store must persist the template across messages.
    let mut session = SessionStore::new(1800);
    let ex = addr();
    let domain = 1;

    // Message 1: template only
    let tmpl_set = build_template_set(&[(256, &[(8, 4), (12, 4)])]);
    let raw1 = build_msg_with_sets(&[tmpl_set], 1, domain);
    let mut msg1 = protocol::parse_message(&raw1, ex).unwrap();
    decode_message(&mut msg1, &raw1, &mut session);

    // Message 2: data only
    let data_set = build_data_set(256, &[&[192, 168, 0, 1, 10, 10, 10, 1]]);
    let raw2 = build_msg_with_sets(&[data_set], 2, domain);
    let mut msg2 = protocol::parse_message(&raw2, ex).unwrap();
    decode_message(&mut msg2, &raw2, &mut session);

    let data = msg2.sets.iter().find_map(|s| {
        if let SetContents::Data(ds) = &s.contents {
            Some(ds)
        } else {
            None
        }
    });
    let data = data.expect("should have data set");
    assert_eq!(data.records.len(), 1);
    assert_eq!(data.records[0].fields[0].name, "sourceIPv4Address");
    assert_eq!(data.records[0].fields[0].value.to_string(), "192.168.0.1");
    assert_eq!(data.records[0].fields[1].name, "destinationIPv4Address");
    assert_eq!(data.records[0].fields[1].value.to_string(), "10.10.10.1");
}
