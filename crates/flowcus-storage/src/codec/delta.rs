//! Delta and double-delta encoding for numeric columns.
//!
//! **Delta**: stores first_value + (value[i] - value[i-1]) for each subsequent value.
//! Optimal for monotonically increasing data like timestamps.
//!
//! **DeltaDelta**: stores first_value + first_delta + (delta[i] - delta[i-1]).
//! Optimal for near-constant rate data (e.g., 1-second interval timestamps).
//!
//! Layout (Delta):
//!   [first_value: element_size bytes] [deltas: packed, element_size bytes each]
//!
//! Layout (DeltaDelta):
//!   [first_value: element_size bytes] [first_delta: element_size bytes] [dd: packed]

use crate::column::ColumnBuffer;

/// Delta-encode a fixed-size column buffer. Returns encoded bytes.
pub fn encode_delta(buffer: &ColumnBuffer, element_size: usize) -> Vec<u8> {
    match element_size {
        4 => encode_delta_typed::<u32>(buffer),
        8 => encode_delta_typed::<u64>(buffer),
        2 => encode_delta_typed::<u16>(buffer),
        1 => encode_delta_typed::<u8>(buffer),
        _ => buffer.as_raw_bytes().to_vec(), // fallback to plain
    }
}

/// Double-delta encode a fixed-size column buffer.
pub fn encode_delta_delta(buffer: &ColumnBuffer, element_size: usize) -> Vec<u8> {
    match element_size {
        4 => encode_dd_typed::<u32>(buffer),
        8 => encode_dd_typed::<u64>(buffer),
        2 => encode_dd_typed::<u16>(buffer),
        1 => encode_dd_typed::<u8>(buffer),
        _ => buffer.as_raw_bytes().to_vec(),
    }
}

trait DeltaOps: Copy + Default {
    fn wrapping_sub(self, other: Self) -> Self;
    fn to_le_bytes_vec(self) -> Vec<u8>;
    const SIZE: usize;
}

macro_rules! impl_delta_ops {
    ($t:ty, $sz:expr) => {
        impl DeltaOps for $t {
            fn wrapping_sub(self, other: Self) -> Self {
                <$t>::wrapping_sub(self, other)
            }
            fn to_le_bytes_vec(self) -> Vec<u8> {
                self.to_le_bytes().to_vec()
            }
            const SIZE: usize = $sz;
        }
    };
}

impl_delta_ops!(u8, 1);
impl_delta_ops!(u16, 2);
impl_delta_ops!(u32, 4);
impl_delta_ops!(u64, 8);

fn extract_values<T: DeltaOps>(buffer: &ColumnBuffer) -> Vec<T> {
    let raw = buffer.as_raw_bytes();
    let count = raw.len() / T::SIZE;
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let start = i * T::SIZE;
        let mut bytes = [0u8; 8];
        bytes[..T::SIZE].copy_from_slice(&raw[start..start + T::SIZE]);
        // Read native-endian (data is already in native endian in the Vec)
        let val = match T::SIZE {
            1 => {
                let v: u8 = bytes[0];
                // SAFETY: T is u8 here, reinterpreting is fine
                let ptr = &v as *const u8 as *const T;
                #[allow(unsafe_code)]
                unsafe {
                    *ptr
                }
            }
            2 => {
                let v = u16::from_ne_bytes([bytes[0], bytes[1]]);
                let ptr = &v as *const u16 as *const T;
                #[allow(unsafe_code)]
                unsafe {
                    *ptr
                }
            }
            4 => {
                let v = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                let ptr = &v as *const u32 as *const T;
                #[allow(unsafe_code)]
                unsafe {
                    *ptr
                }
            }
            8 => {
                let v = u64::from_ne_bytes(bytes);
                let ptr = &v as *const u64 as *const T;
                #[allow(unsafe_code)]
                unsafe {
                    *ptr
                }
            }
            _ => T::default(),
        };
        values.push(val);
    }
    values
}

fn encode_delta_typed<T: DeltaOps>(buffer: &ColumnBuffer) -> Vec<u8> {
    let values = extract_values::<T>(buffer);
    if values.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(values.len() * T::SIZE);

    // Write first value
    out.extend_from_slice(&values[0].to_le_bytes_vec());

    // Write deltas
    let mut prev = values[0];
    for &val in &values[1..] {
        let delta = val.wrapping_sub(prev);
        out.extend_from_slice(&delta.to_le_bytes_vec());
        prev = val;
    }

    out
}

fn encode_dd_typed<T: DeltaOps>(buffer: &ColumnBuffer) -> Vec<u8> {
    let values = extract_values::<T>(buffer);
    if values.len() < 2 {
        return encode_delta_typed::<T>(buffer);
    }

    let mut out = Vec::with_capacity(values.len() * T::SIZE);

    // Write first value
    out.extend_from_slice(&values[0].to_le_bytes_vec());

    // Write first delta
    let first_delta = values[1].wrapping_sub(values[0]);
    out.extend_from_slice(&first_delta.to_le_bytes_vec());

    // Write delta-of-deltas
    let mut prev_delta = first_delta;
    for i in 2..values.len() {
        let delta = values[i].wrapping_sub(values[i - 1]);
        let dd = delta.wrapping_sub(prev_delta);
        out.extend_from_slice(&dd.to_le_bytes_vec());
        prev_delta = delta;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnBuffer;
    use crate::schema::StorageType;
    use flowcus_ipfix::protocol::FieldValue;

    #[test]
    fn delta_monotonic_u32() {
        let mut col = ColumnBuffer::new(StorageType::U32);
        for i in 0..10u32 {
            col.push(&FieldValue::Unsigned32(1000 + i * 5));
        }
        let encoded = encode_delta(&col, 4);
        // First value (4 bytes) + 9 deltas (36 bytes) = 40 bytes = same as raw
        // But delta values are small (5), so downstream compression benefits
        assert_eq!(encoded.len(), 40);
    }

    #[test]
    fn delta_delta_constant_rate() {
        let mut col = ColumnBuffer::new(StorageType::U64);
        for i in 0..10u64 {
            col.push(&FieldValue::Unsigned64(1_700_000_000 + i));
        }
        let encoded = encode_delta_delta(&col, 8);
        // First value (8) + first delta (8) + 8 dd values (64) = 80 bytes
        // DD values should all be 0
        assert_eq!(encoded.len(), 80);
        // Verify dd values are all zero (bytes 16..80)
        assert!(encoded[16..].iter().all(|&b| b == 0));
    }
}
