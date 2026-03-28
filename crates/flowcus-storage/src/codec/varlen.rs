//! Variable-length column codec.
//!
//! Layout on disk:
//!   [offset_count: u32]          — number of offset entries (row_count + 1)
//!   [offsets: u32 * offset_count] — byte offset for each element boundary
//!   [data: raw bytes]            — concatenated variable-length values

use super::CodecType;
use super::EncodedColumn;

/// Encode variable-length column data (offsets + raw data).
pub fn encode(offsets: &[u32], data: &[u8]) -> EncodedColumn {
    let offset_count = offsets.len() as u32;
    let mut out = Vec::with_capacity(4 + offsets.len() * 4 + data.len());

    // Write offset count
    out.extend_from_slice(&offset_count.to_le_bytes());

    // Write offsets as packed u32 LE
    for &off in offsets {
        out.extend_from_slice(&off.to_le_bytes());
    }

    // Write raw data
    out.extend_from_slice(data);

    EncodedColumn::new(CodecType::VarLen, out, [0; 16], [0; 16])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_two_strings() {
        let offsets = &[0u32, 5, 10];
        let data = b"helloworld";
        let encoded = encode(offsets, data);
        assert_eq!(encoded.codec, CodecType::VarLen);
        // 4 (count) + 3*4 (offsets) + 10 (data) = 26
        assert_eq!(encoded.data.len(), 26);
    }
}
