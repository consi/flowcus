//! IPFIX wire protocol types and deserialization (RFC 7011).
//!
//! Message layout:
//! ```text
//! +--------+--------+--------+--------+
//! | Version (0x000a)| Length           |
//! +--------+--------+--------+--------+
//! | Export Time                        |
//! +--------+--------+--------+--------+
//! | Sequence Number                    |
//! +--------+--------+--------+--------+
//! | Observation Domain ID              |
//! +--------+--------+--------+--------+
//! | Set 1 ...                          |
//! | Set 2 ...                          |
//! +------------------------------------+
//! ```

use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// IPFIX version number (always 10 / 0x000a).
pub const IPFIX_VERSION: u16 = 0x000a;
/// IPFIX message header size in bytes.
pub const HEADER_LEN: usize = 16;
/// Set header size in bytes.
pub const SET_HEADER_LEN: usize = 4;
/// Minimum template record header size (template ID + field count).
pub const TEMPLATE_RECORD_HEADER_LEN: usize = 4;
/// Field specifier size without enterprise number.
pub const FIELD_SPEC_LEN: usize = 4;
/// Field specifier size with enterprise number.
pub const FIELD_SPEC_ENTERPRISE_LEN: usize = 8;
/// Template Set ID (RFC 7011 Section 3.3.1).
pub const TEMPLATE_SET_ID: u16 = 2;
/// Options Template Set ID (RFC 7011 Section 3.3.2).
pub const OPTIONS_TEMPLATE_SET_ID: u16 = 3;
/// Minimum data set ID (RFC 7011 Section 3.3.3).
pub const MIN_DATA_SET_ID: u16 = 256;
/// Enterprise bit mask for Information Element ID.
pub const ENTERPRISE_BIT: u16 = 0x8000;
/// Variable-length marker (RFC 7011 Section 7).
pub const VARIABLE_LENGTH: u16 = 65535;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("buffer too short: need {need} bytes, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("invalid IPFIX version: {0:#06x} (expected 0x000a)")]
    InvalidVersion(u16),
    #[error("invalid message length: header says {header_len}, buffer has {buf_len}")]
    LengthMismatch { header_len: usize, buf_len: usize },
    #[error("set length {set_len} exceeds remaining message bytes {remaining}")]
    SetOverflow { set_len: usize, remaining: usize },
    #[error("set length {0} too small (minimum is {SET_HEADER_LEN})")]
    SetTooSmall(usize),
    #[error("variable-length field encoding error at offset {0}")]
    VariableLengthError(usize),
}

/// A parsed IPFIX message header.
#[derive(Debug, Clone, Serialize)]
pub struct MessageHeader {
    pub version: u16,
    pub length: u16,
    pub export_time: u32,
    pub sequence_number: u32,
    pub observation_domain_id: u32,
}

/// A fully parsed IPFIX message.
#[derive(Debug, Clone, Serialize)]
pub struct IpfixMessage {
    pub header: MessageHeader,
    pub exporter: SocketAddr,
    pub sets: Vec<Set>,
}

/// A parsed IPFIX set.
#[derive(Debug, Clone, Serialize)]
pub struct Set {
    pub set_id: u16,
    pub contents: SetContents,
}

/// The contents of a set, depending on its ID.
#[derive(Debug, Clone, Serialize)]
pub enum SetContents {
    Template(Vec<TemplateRecord>),
    OptionsTemplate(Vec<OptionsTemplateRecord>),
    Data(DataSet),
}

/// A template record (Set ID = 2).
#[derive(Debug, Clone, Serialize)]
pub struct TemplateRecord {
    pub template_id: u16,
    pub field_specifiers: Vec<FieldSpecifier>,
}

/// An options template record (Set ID = 3).
#[derive(Debug, Clone, Serialize)]
pub struct OptionsTemplateRecord {
    pub template_id: u16,
    pub scope_field_count: u16,
    pub field_specifiers: Vec<FieldSpecifier>,
}

/// A field specifier within a template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct FieldSpecifier {
    /// Information Element ID (without enterprise bit).
    pub element_id: u16,
    /// Field length in bytes (65535 = variable length).
    pub field_length: u16,
    /// Enterprise number (0 = IANA standard).
    pub enterprise_id: u32,
}

impl FieldSpecifier {
    pub const fn is_enterprise(&self) -> bool {
        self.enterprise_id != 0
    }

    pub const fn is_variable_length(&self) -> bool {
        self.field_length == VARIABLE_LENGTH
    }
}

/// Raw data set (decoded later using template cache).
#[derive(Debug, Clone, Serialize)]
pub struct DataSet {
    pub template_id: u16,
    pub records: Vec<DataRecord>,
}

/// A decoded data record with named field values.
#[derive(Debug, Clone, Serialize)]
pub struct DataRecord {
    pub fields: Vec<DataField>,
}

/// A single field in a data record.
#[derive(Debug, Clone, Serialize)]
pub struct DataField {
    pub spec: FieldSpecifier,
    pub name: String,
    pub value: FieldValue,
}

/// Typed field value decoded from raw bytes.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum FieldValue {
    Unsigned8(u8),
    Unsigned16(u16),
    Unsigned32(u32),
    Unsigned64(u64),
    Signed8(i8),
    Signed16(i16),
    Signed32(i32),
    Signed64(i64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
    Mac([u8; 6]),
    String(String),
    Bytes(Vec<u8>),
    DateTimeSeconds(u32),
    DateTimeMilliseconds(u64),
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsigned8(v) => write!(f, "{v}"),
            Self::Unsigned16(v) => write!(f, "{v}"),
            Self::Unsigned32(v) => write!(f, "{v}"),
            Self::Unsigned64(v) => write!(f, "{v}"),
            Self::Signed8(v) => write!(f, "{v}"),
            Self::Signed16(v) => write!(f, "{v}"),
            Self::Signed32(v) => write!(f, "{v}"),
            Self::Signed64(v) => write!(f, "{v}"),
            Self::Float32(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Bool(v) => write!(f, "{v}"),
            Self::Ipv4(v) => write!(f, "{v}"),
            Self::Ipv6(v) => write!(f, "{v}"),
            Self::Mac(m) => write!(
                f,
                "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                m[0], m[1], m[2], m[3], m[4], m[5]
            ),
            Self::String(v) => write!(f, "{v}"),
            Self::Bytes(v) => {
                write!(f, "0x")?;
                for b in v {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
            Self::DateTimeSeconds(v) => write!(f, "{v}"),
            Self::DateTimeMilliseconds(v) => write!(f, "{v}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire parsing
// ---------------------------------------------------------------------------

/// Read a `u16` from a byte slice at the given offset.
fn read_u16(buf: &[u8], offset: usize) -> Result<u16, ParseError> {
    if offset + 2 > buf.len() {
        return Err(ParseError::BufferTooShort {
            need: offset + 2,
            have: buf.len(),
        });
    }
    Ok(u16::from_be_bytes([buf[offset], buf[offset + 1]]))
}

/// Read a `u32` from a byte slice at the given offset.
fn read_u32(buf: &[u8], offset: usize) -> Result<u32, ParseError> {
    if offset + 4 > buf.len() {
        return Err(ParseError::BufferTooShort {
            need: offset + 4,
            have: buf.len(),
        });
    }
    Ok(u32::from_be_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]))
}

impl MessageHeader {
    /// Parse an IPFIX message header from the first 16 bytes.
    ///
    /// # Errors
    /// Returns `ParseError` if the buffer is too short or the version is wrong.
    pub fn parse(buf: &[u8]) -> Result<Self, ParseError> {
        if buf.len() < HEADER_LEN {
            return Err(ParseError::BufferTooShort {
                need: HEADER_LEN,
                have: buf.len(),
            });
        }

        let version = read_u16(buf, 0)?;
        if version != IPFIX_VERSION {
            return Err(ParseError::InvalidVersion(version));
        }

        Ok(Self {
            version,
            length: read_u16(buf, 2)?,
            export_time: read_u32(buf, 4)?,
            sequence_number: read_u32(buf, 8)?,
            observation_domain_id: read_u32(buf, 12)?,
        })
    }
}

impl FieldSpecifier {
    /// Parse a field specifier at the given offset.
    /// Returns the specifier and the number of bytes consumed.
    pub fn parse(buf: &[u8], offset: usize) -> Result<(Self, usize), ParseError> {
        let raw_id = read_u16(buf, offset)?;
        let field_length = read_u16(buf, offset + 2)?;
        let enterprise_bit = raw_id & ENTERPRISE_BIT != 0;
        let element_id = raw_id & !ENTERPRISE_BIT;

        if enterprise_bit {
            let enterprise_id = read_u32(buf, offset + 4)?;
            Ok((
                Self {
                    element_id,
                    field_length,
                    enterprise_id,
                },
                FIELD_SPEC_ENTERPRISE_LEN,
            ))
        } else {
            Ok((
                Self {
                    element_id,
                    field_length,
                    enterprise_id: 0,
                },
                FIELD_SPEC_LEN,
            ))
        }
    }
}

/// Parse all template records from a Template Set body (after the set header).
pub fn parse_template_records(buf: &[u8]) -> Result<Vec<TemplateRecord>, ParseError> {
    let mut records = Vec::new();
    let mut offset = 0;

    while offset + TEMPLATE_RECORD_HEADER_LEN <= buf.len() {
        let template_id = read_u16(buf, offset)?;
        let field_count = read_u16(buf, offset + 2)? as usize;
        offset += TEMPLATE_RECORD_HEADER_LEN;

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
            let (spec, consumed) = FieldSpecifier::parse(buf, offset)?;
            field_specifiers.push(spec);
            offset += consumed;
        }

        records.push(TemplateRecord {
            template_id,
            field_specifiers,
        });
    }

    Ok(records)
}

/// Parse all options template records from an Options Template Set body.
pub fn parse_options_template_records(
    buf: &[u8],
) -> Result<Vec<OptionsTemplateRecord>, ParseError> {
    let mut records = Vec::new();
    let mut offset = 0;

    while offset + 6 <= buf.len() {
        let template_id = read_u16(buf, offset)?;
        let total_field_count = read_u16(buf, offset + 2)? as usize;
        let scope_field_count = read_u16(buf, offset + 4)?;
        offset += 6;

        let mut field_specifiers = Vec::with_capacity(total_field_count);
        for _ in 0..total_field_count {
            let (spec, consumed) = FieldSpecifier::parse(buf, offset)?;
            field_specifiers.push(spec);
            offset += consumed;
        }

        records.push(OptionsTemplateRecord {
            template_id,
            scope_field_count,
            field_specifiers,
        });
    }

    Ok(records)
}

/// Parse a complete IPFIX message from a byte buffer.
///
/// This only parses the structural layout (header, sets, templates).
/// Data records are left as raw bytes until decoded with templates.
///
/// # Errors
/// Returns `ParseError` on any wire format violation.
pub fn parse_message(buf: &[u8], exporter: SocketAddr) -> Result<IpfixMessage, ParseError> {
    let header = MessageHeader::parse(buf)?;
    let msg_len = header.length as usize;

    if msg_len > buf.len() {
        return Err(ParseError::LengthMismatch {
            header_len: msg_len,
            buf_len: buf.len(),
        });
    }

    let mut sets = Vec::new();
    let mut offset = HEADER_LEN;

    while offset + SET_HEADER_LEN <= msg_len {
        let set_id = read_u16(buf, offset)?;
        let set_length = read_u16(buf, offset + 2)? as usize;

        if set_length < SET_HEADER_LEN {
            return Err(ParseError::SetTooSmall(set_length));
        }
        if offset + set_length > msg_len {
            return Err(ParseError::SetOverflow {
                set_len: set_length,
                remaining: msg_len - offset,
            });
        }

        let set_body = &buf[offset + SET_HEADER_LEN..offset + set_length];

        let contents = match set_id {
            TEMPLATE_SET_ID => SetContents::Template(parse_template_records(set_body)?),
            OPTIONS_TEMPLATE_SET_ID => {
                SetContents::OptionsTemplate(parse_options_template_records(set_body)?)
            }
            id if id >= MIN_DATA_SET_ID => SetContents::Data(DataSet {
                template_id: id,
                records: Vec::new(), // decoded later by session decoder
            }),
            _ => {
                // Reserved set IDs (4-255): skip per RFC 7011
                offset += set_length;
                continue;
            }
        };

        sets.push(Set { set_id, contents });
        offset += set_length;
    }

    Ok(IpfixMessage {
        header,
        exporter,
        sets,
    })
}

/// Read a variable-length encoded value from the buffer (RFC 7011 Section 7).
/// Returns the value bytes and the total bytes consumed (including length prefix).
pub fn read_variable_length(buf: &[u8], offset: usize) -> Result<(&[u8], usize), ParseError> {
    if offset >= buf.len() {
        return Err(ParseError::VariableLengthError(offset));
    }

    let first = buf[offset] as usize;
    if first < 255 {
        let end = offset + 1 + first;
        if end > buf.len() {
            return Err(ParseError::BufferTooShort {
                need: end,
                have: buf.len(),
            });
        }
        Ok((&buf[offset + 1..end], 1 + first))
    } else {
        // 255 prefix: next 2 bytes are the actual length
        if offset + 3 > buf.len() {
            return Err(ParseError::VariableLengthError(offset));
        }
        let len = u16::from_be_bytes([buf[offset + 1], buf[offset + 2]]) as usize;
        let end = offset + 3 + len;
        if end > buf.len() {
            return Err(ParseError::BufferTooShort {
                need: end,
                have: buf.len(),
            });
        }
        Ok((&buf[offset + 3..end], 3 + len))
    }
}

/// Zero-extend unsigned bytes to a `u16`.
fn zero_extend_u16(bytes: &[u8]) -> u16 {
    match bytes.len() {
        1 => u16::from(bytes[0]),
        2 => u16::from_be_bytes([bytes[0], bytes[1]]),
        _ => 0,
    }
}

/// Zero-extend unsigned bytes to a `u32`.
fn zero_extend_u32(bytes: &[u8]) -> u32 {
    match bytes.len() {
        1 => u32::from(bytes[0]),
        2 => u32::from(u16::from_be_bytes([bytes[0], bytes[1]])),
        3 => u32::from(bytes[0]) << 16 | u32::from(bytes[1]) << 8 | u32::from(bytes[2]),
        4 => u32::from_be_bytes(bytes[..4].try_into().expect("checked len")),
        _ => 0,
    }
}

/// Zero-extend unsigned bytes to a `u64`.
fn zero_extend_u64(bytes: &[u8]) -> u64 {
    match bytes.len() {
        1 => u64::from(bytes[0]),
        2 => u64::from(u16::from_be_bytes([bytes[0], bytes[1]])),
        4 => u64::from(u32::from_be_bytes(
            bytes[..4].try_into().expect("checked len"),
        )),
        8 => u64::from_be_bytes(bytes[..8].try_into().expect("checked len")),
        n if n <= 8 => {
            let mut padded = [0u8; 8];
            padded[8 - n..].copy_from_slice(bytes);
            u64::from_be_bytes(padded)
        }
        _ => 0,
    }
}

/// Sign-extend signed bytes to an `i16`.
fn sign_extend_i16(bytes: &[u8]) -> i16 {
    match bytes.len() {
        1 => i16::from(bytes[0] as i8),
        2 => i16::from_be_bytes([bytes[0], bytes[1]]),
        _ => 0,
    }
}

/// Sign-extend signed bytes to an `i32`.
fn sign_extend_i32(bytes: &[u8]) -> i32 {
    match bytes.len() {
        1 => i32::from(bytes[0] as i8),
        2 => i32::from(i16::from_be_bytes([bytes[0], bytes[1]])),
        4 => i32::from_be_bytes(bytes[..4].try_into().expect("checked len")),
        _ => 0,
    }
}

/// Sign-extend signed bytes to an `i64`.
fn sign_extend_i64(bytes: &[u8]) -> i64 {
    match bytes.len() {
        1 => i64::from(bytes[0] as i8),
        2 => i64::from(i16::from_be_bytes([bytes[0], bytes[1]])),
        4 => i64::from(i32::from_be_bytes(
            bytes[..4].try_into().expect("checked len"),
        )),
        8 => i64::from_be_bytes(bytes[..8].try_into().expect("checked len")),
        _ => 0,
    }
}

/// Decode a field value from raw bytes based on the data type and length.
///
/// Supports reduced-size encoding (RFC 7011 Section 6.2): when the encoded
/// length is shorter than the native size of the Information Element, the
/// value is zero-extended (unsigned) or sign-extended (signed) to the
/// native type.
pub fn decode_field_value(bytes: &[u8], data_type: DataType) -> FieldValue {
    match data_type {
        DataType::Unsigned8 if bytes.len() == 1 => FieldValue::Unsigned8(bytes[0]),
        DataType::Unsigned16 => match bytes.len() {
            1 | 2 => FieldValue::Unsigned16(zero_extend_u16(bytes)),
            _ => FieldValue::Bytes(bytes.to_vec()),
        },
        DataType::Unsigned32 => match bytes.len() {
            1..=4 => FieldValue::Unsigned32(zero_extend_u32(bytes)),
            _ => FieldValue::Bytes(bytes.to_vec()),
        },
        DataType::Unsigned64 => match bytes.len() {
            1..=8 => FieldValue::Unsigned64(zero_extend_u64(bytes)),
            _ => FieldValue::Bytes(bytes.to_vec()),
        },
        DataType::Signed8 if bytes.len() == 1 => FieldValue::Signed8(bytes[0] as i8),
        DataType::Signed16 => match bytes.len() {
            1 | 2 => FieldValue::Signed16(sign_extend_i16(bytes)),
            _ => FieldValue::Bytes(bytes.to_vec()),
        },
        DataType::Signed32 => match bytes.len() {
            1 | 2 | 4 => FieldValue::Signed32(sign_extend_i32(bytes)),
            _ => FieldValue::Bytes(bytes.to_vec()),
        },
        DataType::Signed64 => match bytes.len() {
            1 | 2 | 4 | 8 => FieldValue::Signed64(sign_extend_i64(bytes)),
            _ => FieldValue::Bytes(bytes.to_vec()),
        },
        DataType::Float32 if bytes.len() == 4 => FieldValue::Float32(f32::from_be_bytes(
            bytes[..4].try_into().expect("checked len"),
        )),
        DataType::Float64 if bytes.len() == 8 => FieldValue::Float64(f64::from_be_bytes(
            bytes[..8].try_into().expect("checked len"),
        )),
        DataType::Boolean if bytes.len() == 1 => FieldValue::Bool(bytes[0] != 0),
        DataType::Ipv4Address if bytes.len() == 4 => {
            FieldValue::Ipv4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]))
        }
        DataType::Ipv6Address if bytes.len() == 16 => {
            let arr: [u8; 16] = bytes[..16].try_into().expect("checked len");
            FieldValue::Ipv6(Ipv6Addr::from(arr))
        }
        DataType::MacAddress if bytes.len() == 6 => {
            let arr: [u8; 6] = bytes[..6].try_into().expect("checked len");
            FieldValue::Mac(arr)
        }
        DataType::String => FieldValue::String(String::from_utf8_lossy(bytes).into_owned()),
        DataType::DateTimeSeconds => match bytes.len() {
            1..=4 => FieldValue::DateTimeSeconds(zero_extend_u32(bytes)),
            _ => FieldValue::Bytes(bytes.to_vec()),
        },
        DataType::DateTimeMilliseconds => match bytes.len() {
            1..=8 => FieldValue::DateTimeMilliseconds(zero_extend_u64(bytes)),
            _ => FieldValue::Bytes(bytes.to_vec()),
        },
        // Fallback for types without reduced-size support or unknown types
        _ => match bytes.len() {
            1 => FieldValue::Unsigned8(bytes[0]),
            2 => FieldValue::Unsigned16(u16::from_be_bytes([bytes[0], bytes[1]])),
            4 => FieldValue::Unsigned32(u32::from_be_bytes(
                bytes[..4].try_into().expect("checked len"),
            )),
            8 => FieldValue::Unsigned64(u64::from_be_bytes(
                bytes[..8].try_into().expect("checked len"),
            )),
            _ => FieldValue::Bytes(bytes.to_vec()),
        },
    }
}

/// IPFIX abstract data types (RFC 7012 Section 3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    OctetArray,
    Unsigned8,
    Unsigned16,
    Unsigned32,
    Unsigned64,
    Signed8,
    Signed16,
    Signed32,
    Signed64,
    Float32,
    Float64,
    Boolean,
    MacAddress,
    String,
    DateTimeSeconds,
    DateTimeMilliseconds,
    DateTimeMicroseconds,
    DateTimeNanoseconds,
    Ipv4Address,
    Ipv6Address,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_header(length: u16, seq: u32, domain: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&IPFIX_VERSION.to_be_bytes());
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(&1_700_000_000u32.to_be_bytes()); // export time
        buf.extend_from_slice(&seq.to_be_bytes());
        buf.extend_from_slice(&domain.to_be_bytes());
        buf
    }

    #[test]
    fn parse_valid_header() {
        let buf = build_header(16, 42, 1);
        let hdr = MessageHeader::parse(&buf).unwrap();
        assert_eq!(hdr.version, IPFIX_VERSION);
        assert_eq!(hdr.sequence_number, 42);
        assert_eq!(hdr.observation_domain_id, 1);
    }

    #[test]
    fn reject_invalid_version() {
        let mut buf = build_header(16, 0, 0);
        buf[0] = 0x00;
        buf[1] = 0x09; // NetFlow v9
        assert!(MessageHeader::parse(&buf).is_err());
    }

    #[test]
    fn parse_template_set() {
        // Build a message with one template set containing one template
        let mut msg = build_header(0, 1, 1); // length placeholder

        // Template Set: set_id=2
        // Template: id=256, field_count=2
        // Field 1: IE 8 (sourceIPv4Address), length=4
        // Field 2: IE 12 (destinationIPv4Address), length=4
        // set_length = 4 (set header) + 4 (template header) + 8 (2 fields) = 16
        let mut set = Vec::new();
        set.extend_from_slice(&2u16.to_be_bytes()); // set_id = TEMPLATE
        set.extend_from_slice(&16u16.to_be_bytes()); // set_length
        set.extend_from_slice(&256u16.to_be_bytes()); // template_id
        set.extend_from_slice(&2u16.to_be_bytes()); // field_count
        set.extend_from_slice(&8u16.to_be_bytes()); // IE 8
        set.extend_from_slice(&4u16.to_be_bytes()); // length 4
        set.extend_from_slice(&12u16.to_be_bytes()); // IE 12
        set.extend_from_slice(&4u16.to_be_bytes()); // length 4

        msg.extend_from_slice(&set);

        // Fix message length
        let len = msg.len() as u16;
        msg[2..4].copy_from_slice(&len.to_be_bytes());

        let addr: SocketAddr = "10.0.0.1:4739".parse().unwrap();
        let parsed = parse_message(&msg, addr).unwrap();

        assert_eq!(parsed.sets.len(), 1);
        if let SetContents::Template(templates) = &parsed.sets[0].contents {
            assert_eq!(templates.len(), 1);
            assert_eq!(templates[0].template_id, 256);
            assert_eq!(templates[0].field_specifiers.len(), 2);
            assert_eq!(templates[0].field_specifiers[0].element_id, 8);
            assert_eq!(templates[0].field_specifiers[1].element_id, 12);
        } else {
            panic!("expected template set");
        }
    }

    #[test]
    fn parse_enterprise_field_specifier() {
        let mut buf = vec![0u8; 8];
        // IE 1 with enterprise bit set, length 4, enterprise ID 9 (Cisco)
        let id_with_enterprise = 1u16 | ENTERPRISE_BIT;
        buf[0..2].copy_from_slice(&id_with_enterprise.to_be_bytes());
        buf[2..4].copy_from_slice(&4u16.to_be_bytes());
        buf[4..8].copy_from_slice(&9u32.to_be_bytes());

        let (spec, consumed) = FieldSpecifier::parse(&buf, 0).unwrap();
        assert_eq!(consumed, 8);
        assert_eq!(spec.element_id, 1);
        assert_eq!(spec.enterprise_id, 9);
        assert!(spec.is_enterprise());
    }

    #[test]
    fn decode_ipv4_field() {
        let bytes = [192, 168, 1, 1];
        let val = decode_field_value(&bytes, DataType::Ipv4Address);
        assert_eq!(val.to_string(), "192.168.1.1");
    }

    #[test]
    fn decode_mac_field() {
        let bytes = [0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff];
        let val = decode_field_value(&bytes, DataType::MacAddress);
        assert_eq!(val.to_string(), "aa:bb:cc:dd:ee:ff");
    }

    #[test]
    fn variable_length_short() {
        let mut buf = vec![5u8]; // length = 5
        buf.extend_from_slice(b"hello");
        let (data, consumed) = read_variable_length(&buf, 0).unwrap();
        assert_eq!(data, b"hello");
        assert_eq!(consumed, 6);
    }

    #[test]
    fn variable_length_long() {
        let mut buf = vec![255u8]; // marker
        buf.extend_from_slice(&300u16.to_be_bytes());
        buf.extend_from_slice(&vec![0x42; 300]);
        let (data, consumed) = read_variable_length(&buf, 0).unwrap();
        assert_eq!(data.len(), 300);
        assert_eq!(consumed, 303);
    }
}
