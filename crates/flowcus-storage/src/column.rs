//! Columnar in-memory buffers.
//!
//! Data is stored in typed vectors for natural alignment and auto-vectorization.
//! Fixed-size columns use packed arrays. Variable-length columns use offset + data.
//! All buffers are designed for sequential append and bulk flush.

use flowcus_ipfix::protocol::FieldValue;

use crate::schema::StorageType;

/// A columnar buffer holding values for a single column.
/// Typed for natural alignment: the compiler and SIMD instructions
/// can operate directly on the underlying `Vec<T>` data.
#[derive(Debug, Clone)]
pub enum ColumnBuffer {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    /// IPv6 stored as two u64s (high, low) interleaved for alignment.
    U128(Vec<[u64; 2]>),
    /// MAC addresses: packed 6-byte values.
    Mac(Vec<[u8; 6]>),
    /// Variable-length: offsets into data buffer. offset[i]..offset[i+1] is element i.
    VarLen {
        offsets: Vec<u32>,
        data: Vec<u8>,
    },
}

impl ColumnBuffer {
    /// Create a new empty buffer for the given storage type.
    pub fn new(st: StorageType) -> Self {
        match st {
            StorageType::U8 => Self::U8(Vec::new()),
            StorageType::U16 => Self::U16(Vec::new()),
            StorageType::U32 => Self::U32(Vec::new()),
            StorageType::U64 => Self::U64(Vec::new()),
            StorageType::U128 => Self::U128(Vec::new()),
            StorageType::Mac => Self::Mac(Vec::new()),
            StorageType::VarLen => Self::VarLen {
                offsets: vec![0],
                data: Vec::new(),
            },
        }
    }

    /// Create a new buffer with pre-allocated capacity.
    pub fn with_capacity(st: StorageType, rows: usize) -> Self {
        match st {
            StorageType::U8 => Self::U8(Vec::with_capacity(rows)),
            StorageType::U16 => Self::U16(Vec::with_capacity(rows)),
            StorageType::U32 => Self::U32(Vec::with_capacity(rows)),
            StorageType::U64 => Self::U64(Vec::with_capacity(rows)),
            StorageType::U128 => Self::U128(Vec::with_capacity(rows)),
            StorageType::Mac => Self::Mac(Vec::with_capacity(rows)),
            StorageType::VarLen => {
                let mut offsets = Vec::with_capacity(rows + 1);
                offsets.push(0);
                Self::VarLen {
                    offsets,
                    data: Vec::with_capacity(rows * 32), // estimate 32 bytes avg
                }
            }
        }
    }

    /// Append a FieldValue to this buffer.
    /// The caller must ensure the value type matches the buffer type.
    pub fn push(&mut self, value: &FieldValue) {
        match self {
            Self::U8(v) => v.push(extract_u8(value)),
            Self::U16(v) => v.push(extract_u16(value)),
            Self::U32(v) => v.push(extract_u32(value)),
            Self::U64(v) => v.push(extract_u64(value)),
            Self::U128(v) => v.push(extract_u128(value)),
            Self::Mac(v) => v.push(extract_mac(value)),
            Self::VarLen { offsets, data } => {
                let bytes = extract_varlen(value);
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
        }
    }

    /// Push a raw `u32` value into a U32 buffer.
    /// Panics if the buffer is not U32.
    pub fn push_u32(&mut self, val: u32) {
        match self {
            Self::U32(v) => v.push(val),
            _ => panic!("push_u32 called on non-U32 buffer"),
        }
    }

    /// Push a raw `u16` value into a U16 buffer.
    /// Panics if the buffer is not U16.
    pub fn push_u16(&mut self, val: u16) {
        match self {
            Self::U16(v) => v.push(val),
            _ => panic!("push_u16 called on non-U16 buffer"),
        }
    }

    /// Push a raw `u64` value into a U64 buffer.
    /// Panics if the buffer is not U64.
    pub fn push_u64(&mut self, val: u64) {
        match self {
            Self::U64(v) => v.push(val),
            _ => panic!("push_u64 called on non-U64 buffer"),
        }
    }

    /// Push a raw `[u64; 2]` value into a U128 buffer.
    /// Panics if the buffer is not U128.
    pub fn push_u128(&mut self, val: [u64; 2]) {
        match self {
            Self::U128(v) => v.push(val),
            _ => panic!("push_u128 called on non-U128 buffer"),
        }
    }

    /// Number of rows in this buffer.
    pub fn row_count(&self) -> usize {
        match self {
            Self::U8(v) => v.len(),
            Self::U16(v) => v.len(),
            Self::U32(v) => v.len(),
            Self::U64(v) => v.len(),
            Self::U128(v) => v.len(),
            Self::Mac(v) => v.len(),
            Self::VarLen { offsets, .. } => offsets.len().saturating_sub(1),
        }
    }

    /// Approximate memory usage in bytes.
    pub fn mem_size(&self) -> usize {
        match self {
            Self::U8(v) => v.len(),
            Self::U16(v) => v.len() * 2,
            Self::U32(v) => v.len() * 4,
            Self::U64(v) => v.len() * 8,
            Self::U128(v) => v.len() * 16,
            Self::Mac(v) => v.len() * 6,
            Self::VarLen { offsets, data } => offsets.len() * 4 + data.len(),
        }
    }

    /// Clear the buffer, keeping allocated memory.
    pub fn clear(&mut self) {
        match self {
            Self::U8(v) => v.clear(),
            Self::U16(v) => v.clear(),
            Self::U32(v) => v.clear(),
            Self::U64(v) => v.clear(),
            Self::U128(v) => v.clear(),
            Self::Mac(v) => v.clear(),
            Self::VarLen { offsets, data } => {
                offsets.clear();
                offsets.push(0);
                data.clear();
            }
        }
    }

    /// Get the raw bytes slice for fixed-size columns. Used by codecs.
    pub fn as_raw_bytes(&self) -> &[u8] {
        match self {
            Self::U8(v) => v.as_slice(),
            Self::U16(v) => bytemuck_slice(v),
            Self::U32(v) => bytemuck_slice(v),
            Self::U64(v) => bytemuck_slice(v),
            Self::U128(v) => bytemuck_slice(v),
            Self::Mac(v) => bytemuck_slice(v),
            Self::VarLen { .. } => &[], // use varlen_parts() instead
        }
    }

    /// Get offset and data arrays for variable-length columns.
    pub fn varlen_parts(&self) -> Option<(&[u32], &[u8])> {
        if let Self::VarLen { offsets, data } = self {
            Some((offsets.as_slice(), data.as_slice()))
        } else {
            None
        }
    }
}

/// Reinterpret a slice of T as a slice of bytes (safe for native-endian packed data).
fn bytemuck_slice<T: Copy>(slice: &[T]) -> &[u8] {
    let ptr = slice.as_ptr().cast::<u8>();
    let len = slice.len() * std::mem::size_of::<T>();
    // SAFETY: T is Copy (no drop), and we're just reinterpreting the bytes.
    // The lifetime is tied to the original slice.
    // This is equivalent to bytemuck::cast_slice but without the dependency.
    #[allow(unsafe_code)]
    unsafe {
        std::slice::from_raw_parts(ptr, len)
    }
}

// --- Value extractors ---
// These convert FieldValue variants to the appropriate native type.
// IPFIX reduced-size encoding means a u32 field might arrive as Unsigned16, etc.

fn extract_u8(v: &FieldValue) -> u8 {
    match v {
        FieldValue::Unsigned8(x) => *x,
        FieldValue::Bool(b) => u8::from(*b),
        FieldValue::Signed8(x) => *x as u8,
        _ => 0,
    }
}

fn extract_u16(v: &FieldValue) -> u16 {
    match v {
        FieldValue::Unsigned16(x) => *x,
        FieldValue::Unsigned8(x) => u16::from(*x),
        FieldValue::Signed16(x) => *x as u16,
        _ => 0,
    }
}

fn extract_u32(v: &FieldValue) -> u32 {
    match v {
        FieldValue::Unsigned32(x) => *x,
        FieldValue::Unsigned16(x) => u32::from(*x),
        FieldValue::Unsigned8(x) => u32::from(*x),
        FieldValue::Signed32(x) => *x as u32,
        FieldValue::Float32(x) => x.to_bits(),
        FieldValue::Ipv4(addr) => u32::from(*addr),
        FieldValue::DateTimeSeconds(x) => *x,
        _ => 0,
    }
}

fn extract_u64(v: &FieldValue) -> u64 {
    match v {
        FieldValue::Unsigned64(x) => *x,
        FieldValue::Unsigned32(x) => u64::from(*x),
        FieldValue::Unsigned16(x) => u64::from(*x),
        FieldValue::Unsigned8(x) => u64::from(*x),
        FieldValue::Signed64(x) => *x as u64,
        FieldValue::Float64(x) => x.to_bits(),
        FieldValue::DateTimeMilliseconds(x) => *x,
        FieldValue::DateTimeSeconds(x) => u64::from(*x),
        _ => 0,
    }
}

fn extract_u128(v: &FieldValue) -> [u64; 2] {
    match v {
        FieldValue::Ipv6(addr) => {
            let octets = addr.octets();
            let high = u64::from_be_bytes(octets[..8].try_into().unwrap());
            let low = u64::from_be_bytes(octets[8..].try_into().unwrap());
            [high, low]
        }
        _ => [0, 0],
    }
}

fn extract_mac(v: &FieldValue) -> [u8; 6] {
    match v {
        FieldValue::Mac(m) => *m,
        _ => [0; 6],
    }
}

fn extract_varlen<'a>(v: &'a FieldValue) -> &'a [u8] {
    match v {
        FieldValue::String(s) => s.as_bytes(),
        FieldValue::Bytes(b) => b.as_slice(),
        _ => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn u32_column_push_and_raw() {
        let mut col = ColumnBuffer::new(StorageType::U32);
        col.push(&FieldValue::Ipv4(Ipv4Addr::new(10, 0, 0, 1)));
        col.push(&FieldValue::Ipv4(Ipv4Addr::new(192, 168, 1, 1)));
        assert_eq!(col.row_count(), 2);
        assert_eq!(col.mem_size(), 8);

        let raw = col.as_raw_bytes();
        assert_eq!(raw.len(), 8);
    }

    #[test]
    fn varlen_column_push() {
        let mut col = ColumnBuffer::new(StorageType::VarLen);
        col.push(&FieldValue::String("hello".into()));
        col.push(&FieldValue::String("world".into()));
        assert_eq!(col.row_count(), 2);

        let (offsets, data) = col.varlen_parts().unwrap();
        assert_eq!(offsets, &[0, 5, 10]);
        assert_eq!(data, b"helloworld");
    }

    #[test]
    fn clear_preserves_capacity() {
        let mut col = ColumnBuffer::with_capacity(StorageType::U64, 1024);
        for i in 0..100u64 {
            col.push(&FieldValue::Unsigned64(i));
        }
        assert_eq!(col.row_count(), 100);
        col.clear();
        assert_eq!(col.row_count(), 0);
    }
}
