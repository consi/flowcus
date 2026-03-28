//! GCD (Greatest Common Divisor) encoding for numeric columns.
//!
//! If all values in a column share a common divisor > 1, we store:
//!   [gcd: element_size bytes] [values / gcd: element_size bytes each]
//!
//! The divided values use fewer bits, benefiting downstream compression.
//! Common for port numbers, byte counts, packet sizes that are multiples of MTU, etc.

use crate::column::ColumnBuffer;

/// GCD-encode a fixed-size column buffer.
pub fn encode_gcd(buffer: &ColumnBuffer, element_size: usize) -> Vec<u8> {
    match element_size {
        1 => encode_gcd_typed::<u8>(buffer),
        2 => encode_gcd_typed::<u16>(buffer),
        4 => encode_gcd_typed::<u32>(buffer),
        8 => encode_gcd_typed::<u64>(buffer),
        _ => buffer.as_raw_bytes().to_vec(),
    }
}

trait GcdOps: Copy + Default + PartialEq {
    fn as_u64(self) -> u64;
    fn from_u64(v: u64) -> Self;
    fn to_le_bytes_vec(self) -> Vec<u8>;
    const SIZE: usize;
}

macro_rules! impl_gcd_ops {
    ($t:ty, $sz:expr) => {
        impl GcdOps for $t {
            fn as_u64(self) -> u64 {
                self as u64
            }
            fn from_u64(v: u64) -> Self {
                v as $t
            }
            fn to_le_bytes_vec(self) -> Vec<u8> {
                self.to_le_bytes().to_vec()
            }
            const SIZE: usize = $sz;
        }
    };
}

impl_gcd_ops!(u8, 1);
impl_gcd_ops!(u16, 2);
impl_gcd_ops!(u32, 4);
impl_gcd_ops!(u64, 8);

fn extract_values_native<T: GcdOps>(buffer: &ColumnBuffer) -> Vec<T> {
    let raw = buffer.as_raw_bytes();
    let count = raw.len() / T::SIZE;
    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let start = i * T::SIZE;
        let mut bytes = [0u8; 8];
        bytes[..T::SIZE].copy_from_slice(&raw[start..start + T::SIZE]);
        let val = match T::SIZE {
            1 => T::from_u64(u64::from(bytes[0])),
            2 => T::from_u64(u64::from(u16::from_ne_bytes([bytes[0], bytes[1]]))),
            4 => T::from_u64(u64::from(u32::from_ne_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ]))),
            8 => T::from_u64(u64::from_ne_bytes(bytes)),
            _ => T::default(),
        };
        values.push(val);
    }
    values
}

fn gcd<T: GcdOps>(a: T, b: T) -> T {
    let (mut a, mut b) = (a.as_u64(), b.as_u64());
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    T::from_u64(a)
}

fn encode_gcd_typed<T: GcdOps>(buffer: &ColumnBuffer) -> Vec<u8> {
    let values = extract_values_native::<T>(buffer);
    if values.is_empty() {
        return Vec::new();
    }

    // Compute GCD of all values
    let g = values.iter().copied().fold(T::default(), |acc, v| {
        if acc == T::default() { v } else { gcd(acc, v) }
    });

    // If GCD is 0 or 1, no benefit
    if g.as_u64() <= 1 {
        return buffer.as_raw_bytes().to_vec();
    }

    let mut out = Vec::with_capacity(T::SIZE + values.len() * T::SIZE);

    // Write GCD
    out.extend_from_slice(&g.to_le_bytes_vec());

    // Write divided values
    for &val in &values {
        let divided = T::from_u64(val.as_u64() / g.as_u64());
        out.extend_from_slice(&divided.to_le_bytes_vec());
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
    fn gcd_multiples_of_100() {
        let mut col = ColumnBuffer::new(StorageType::U16);
        for i in 1..=10u16 {
            col.push(&FieldValue::Unsigned16(i * 100));
        }
        let encoded = encode_gcd(&col, 2);
        // 2 bytes GCD (100) + 10 * 2 bytes divided values = 22 bytes
        assert_eq!(encoded.len(), 22);
        // First 2 bytes should be GCD = 100 in LE
        assert_eq!(u16::from_le_bytes([encoded[0], encoded[1]]), 100);
        // First value should be 1 (100/100)
        assert_eq!(u16::from_le_bytes([encoded[2], encoded[3]]), 1);
    }

    #[test]
    fn gcd_no_common_divisor() {
        let mut col = ColumnBuffer::new(StorageType::U32);
        col.push(&FieldValue::Unsigned32(7));
        col.push(&FieldValue::Unsigned32(11));
        col.push(&FieldValue::Unsigned32(13));
        let raw_len = col.as_raw_bytes().len();
        let encoded = encode_gcd(&col, 4);
        // GCD=1, falls back to raw
        assert_eq!(encoded.len(), raw_len);
    }
}
