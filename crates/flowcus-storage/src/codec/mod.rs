//! Column codecs: encode/decode typed column data for disk storage.
//!
//! Two-layer encoding pipeline:
//! 1. **Transform codec**: reshapes data for better compressibility
//!    - Plain, Delta, DeltaDelta, GCD — selected automatically by data analysis
//! 2. **Compression codec**: reduces physical size on disk
//!    - None (raw), LZ4 — applied on top of the transform output
//!
//! Codec selection is automatic: the writer samples the data in a column buffer,
//! picks the best transform, then applies LZ4 if it shrinks the result.

mod delta;
mod fixed;
mod gcd;
mod varlen;

use serde::{Deserialize, Serialize};

use crate::column::ColumnBuffer;

/// Transform codec identifier (pre-compression data reshaping).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CodecType {
    /// Raw native-endian bytes, no transformation.
    Plain = 0,
    /// Delta encoding: first value + deltas.
    Delta = 1,
    /// Double-delta encoding: first value + first delta + delta-of-deltas.
    DeltaDelta = 2,
    /// GCD encoding: all values divided by their GCD.
    Gcd = 3,
    /// Variable-length: u32 offsets + byte data.
    VarLen = 10,
}

/// Compression codec applied after the transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionType {
    /// No compression — raw bytes.
    None = 0,
    /// LZ4 block compression — fast, good ratio for columnar data.
    Lz4 = 1,
}

/// Result of encoding a column buffer to bytes.
#[derive(Clone)]
pub struct EncodedColumn {
    pub codec: CodecType,
    pub compression: CompressionType,
    /// The final encoded data bytes (after transform + compression).
    pub data: Vec<u8>,
    /// Uncompressed size (before compression, after transform).
    pub uncompressed_size: usize,
    /// Min value as bytes (up to 16 bytes, for future granule marks / bloom filters).
    pub min_value: [u8; 16],
    /// Max value as bytes.
    pub max_value: [u8; 16],
}

impl EncodedColumn {
    fn new(codec: CodecType, data: Vec<u8>, min_value: [u8; 16], max_value: [u8; 16]) -> Self {
        let uncompressed_size = data.len();
        Self {
            codec,
            compression: CompressionType::None,
            data,
            uncompressed_size,
            min_value,
            max_value,
        }
    }
}

/// Encode a column buffer, automatically selecting the best transform + compression.
///
/// Pipeline: analyze data → pick transform → apply transform → try LZ4 → keep smaller.
pub fn encode(buffer: &ColumnBuffer) -> EncodedColumn {
    let _t = flowcus_core::profiling::span_timer("storage;codec;encode");
    let mut encoded = match buffer {
        ColumnBuffer::VarLen { offsets, data } => varlen::encode(offsets, data),
        _ => encode_fixed(buffer),
    };

    let _t2 = flowcus_core::profiling::span_timer("storage;codec;lz4_compress");
    encoded = try_compress_lz4(encoded);
    encoded
}

/// Apply LZ4 compression if it actually reduces size.
fn try_compress_lz4(mut encoded: EncodedColumn) -> EncodedColumn {
    if encoded.data.len() < 64 {
        // Too small for compression overhead to pay off
        return encoded;
    }

    let compressed = lz4_flex::compress_prepend_size(&encoded.data);
    if compressed.len() < encoded.data.len() {
        encoded.uncompressed_size = encoded.data.len();
        encoded.data = compressed;
        encoded.compression = CompressionType::Lz4;
    }
    encoded
}

/// For fixed-size columns, analyze and pick the best numeric codec.
fn encode_fixed(buffer: &ColumnBuffer) -> EncodedColumn {
    let raw = buffer.as_raw_bytes();
    if raw.is_empty() {
        return EncodedColumn::new(CodecType::Plain, Vec::new(), [0; 16], [0; 16]);
    }

    let element_size = match buffer {
        ColumnBuffer::U8(_) => 1,
        ColumnBuffer::U16(_) => 2,
        ColumnBuffer::U32(_) => 4,
        ColumnBuffer::U64(_) => 8,
        ColumnBuffer::U128(_) => 16,
        ColumnBuffer::Mac(_) => 6,
        ColumnBuffer::VarLen { .. } => unreachable!(),
    };

    let row_count = buffer.row_count();
    let (min_value, max_value) = compute_min_max(buffer);

    // For MAC (6 bytes) or U128 (16 bytes), just use Plain - delta doesn't help much
    if element_size == 6 || element_size == 16 {
        return EncodedColumn::new(CodecType::Plain, raw.to_vec(), min_value, max_value);
    }

    // Analyze data to pick best codec.
    // Score each codec by estimated encoded size.
    let plain_size = raw.len();

    let delta_score = analyze_delta(buffer, element_size, row_count);
    let gcd_score = analyze_gcd(buffer, element_size, row_count);

    // Pick smallest encoding
    let best = if delta_score.dd_saves > delta_score.d_saves && delta_score.dd_saves > gcd_score {
        CodecChoice::DeltaDelta
    } else if delta_score.d_saves > gcd_score && delta_score.d_saves > 0 {
        CodecChoice::Delta
    } else if gcd_score > 0 {
        CodecChoice::Gcd
    } else {
        CodecChoice::Plain
    };

    let data = match best {
        CodecChoice::Plain => raw.to_vec(),
        CodecChoice::Delta => delta::encode_delta(buffer, element_size),
        CodecChoice::DeltaDelta => delta::encode_delta_delta(buffer, element_size),
        CodecChoice::Gcd => gcd::encode_gcd(buffer, element_size),
    };

    let codec = match best {
        CodecChoice::Plain => CodecType::Plain,
        CodecChoice::Delta => CodecType::Delta,
        CodecChoice::DeltaDelta => CodecType::DeltaDelta,
        CodecChoice::Gcd => CodecType::Gcd,
    };

    tracing::trace!(
        codec = ?codec,
        raw_size = plain_size,
        encoded_size = data.len(),
        row_count,
        element_size,
        "Column encoded"
    );

    EncodedColumn::new(codec, data, min_value, max_value)
}

enum CodecChoice {
    Plain,
    Delta,
    DeltaDelta,
    Gcd,
}

struct DeltaScore {
    /// Estimated bytes saved by delta encoding over plain.
    d_saves: usize,
    /// Estimated bytes saved by double-delta over plain.
    dd_saves: usize,
}

/// Analyze how well delta/double-delta would compress this column.
fn analyze_delta(buffer: &ColumnBuffer, element_size: usize, row_count: usize) -> DeltaScore {
    if row_count < 4 {
        return DeltaScore {
            d_saves: 0,
            dd_saves: 0,
        };
    }

    // Sample the column to estimate delta magnitude
    let values = extract_u64_samples(buffer, 128);
    if values.len() < 2 {
        return DeltaScore {
            d_saves: 0,
            dd_saves: 0,
        };
    }

    // Compute deltas and delta-of-deltas
    let deltas: Vec<u64> = values.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();

    // Estimate bits needed for deltas vs original values
    let orig_bits = max_bits(&values);
    let delta_bits = max_bits(&deltas);

    let d_saves = if delta_bits < orig_bits {
        (row_count * element_size) * (orig_bits - delta_bits) / (orig_bits.max(1) * 8)
    } else {
        0
    };

    let dd_saves = if deltas.len() >= 2 {
        let dd: Vec<u64> = deltas.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect();
        let dd_bits = max_bits(&dd);
        if dd_bits < delta_bits {
            (row_count * element_size) * (orig_bits - dd_bits) / (orig_bits.max(1) * 8)
        } else {
            0
        }
    } else {
        0
    };

    DeltaScore { d_saves, dd_saves }
}

/// Analyze how well GCD encoding would compress.
fn analyze_gcd(buffer: &ColumnBuffer, element_size: usize, row_count: usize) -> usize {
    if row_count < 4 {
        return 0;
    }

    let values = extract_u64_samples(buffer, 128);
    if values.is_empty() {
        return 0;
    }

    let g = values.iter().copied().fold(0u64, gcd_u64);
    if g <= 1 {
        return 0;
    }

    // After dividing by GCD, values are smaller → fewer bits needed
    let orig_bits = max_bits(&values);
    let divided: Vec<u64> = values.iter().map(|v| v / g).collect();
    let new_bits = max_bits(&divided);

    if new_bits < orig_bits {
        (row_count * element_size) * (orig_bits - new_bits) / (orig_bits.max(1) * 8)
    } else {
        0
    }
}

/// Extract up to `limit` values from a column as u64 for analysis.
fn extract_u64_samples(buffer: &ColumnBuffer, limit: usize) -> Vec<u64> {
    let count = buffer.row_count().min(limit);
    match buffer {
        ColumnBuffer::U8(v) => v.iter().take(count).map(|x| u64::from(*x)).collect(),
        ColumnBuffer::U16(v) => v.iter().take(count).map(|x| u64::from(*x)).collect(),
        ColumnBuffer::U32(v) => v.iter().take(count).map(|x| u64::from(*x)).collect(),
        ColumnBuffer::U64(v) => v.iter().take(count).copied().collect(),
        _ => Vec::new(),
    }
}

/// Maximum number of significant bits across a set of values.
fn max_bits(values: &[u64]) -> usize {
    values
        .iter()
        .map(|v| 64 - v.leading_zeros() as usize)
        .max()
        .unwrap_or(0)
}

/// GCD of two u64 values.
fn gcd_u64(a: u64, b: u64) -> u64 {
    let (mut a, mut b) = (a, b);
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

/// Compute min/max values for a column (up to 16 bytes each).
fn compute_min_max(buffer: &ColumnBuffer) -> ([u8; 16], [u8; 16]) {
    let mut min_val = [0xFFu8; 16];
    let mut max_val = [0u8; 16];

    match buffer {
        ColumnBuffer::U32(v) => {
            if let (Some(&min), Some(&max)) = (v.iter().min(), v.iter().max()) {
                min_val[..4].copy_from_slice(&min.to_le_bytes());
                max_val[..4].copy_from_slice(&max.to_le_bytes());
            }
        }
        ColumnBuffer::U64(v) => {
            if let (Some(&min), Some(&max)) = (v.iter().min(), v.iter().max()) {
                min_val[..8].copy_from_slice(&min.to_le_bytes());
                max_val[..8].copy_from_slice(&max.to_le_bytes());
            }
        }
        ColumnBuffer::U16(v) => {
            if let (Some(&min), Some(&max)) = (v.iter().min(), v.iter().max()) {
                min_val[..2].copy_from_slice(&min.to_le_bytes());
                max_val[..2].copy_from_slice(&max.to_le_bytes());
            }
        }
        ColumnBuffer::U8(v) => {
            if let (Some(&min), Some(&max)) = (v.iter().min(), v.iter().max()) {
                min_val[0] = min;
                max_val[0] = max;
            }
        }
        _ => {
            // For U128, Mac, VarLen: zeroed min/max for now
            min_val = [0; 16];
        }
    }

    (min_val, max_val)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::StorageType;
    use flowcus_ipfix::protocol::FieldValue;
    use std::net::Ipv4Addr;

    #[test]
    fn encode_monotonic_timestamps_picks_delta() {
        let mut col = ColumnBuffer::new(StorageType::U32);
        for i in 0..100u32 {
            col.push(&FieldValue::DateTimeSeconds(1_700_000_000 + i));
        }
        let encoded = encode(&col);
        // Delta or DeltaDelta should be chosen for monotonic sequence
        assert!(
            encoded.codec == CodecType::Delta || encoded.codec == CodecType::DeltaDelta,
            "expected delta codec for monotonic data, got {:?}",
            encoded.codec
        );
        assert!(encoded.data.len() < col.as_raw_bytes().len());
    }

    #[test]
    fn encode_random_ipv4_uses_plain() {
        let mut col = ColumnBuffer::new(StorageType::U32);
        let addrs = [
            Ipv4Addr::new(192, 168, 1, 1),
            Ipv4Addr::new(10, 0, 0, 5),
            Ipv4Addr::new(172, 16, 0, 100),
            Ipv4Addr::new(8, 8, 8, 8),
        ];
        for addr in &addrs {
            col.push(&FieldValue::Ipv4(*addr));
        }
        let encoded = encode(&col);
        // With only 4 random values, plain is expected
        assert_eq!(encoded.codec, CodecType::Plain);
    }

    #[test]
    fn encode_varlen_strings() {
        let mut col = ColumnBuffer::new(StorageType::VarLen);
        col.push(&FieldValue::String("https".into()));
        col.push(&FieldValue::String("dns".into()));
        col.push(&FieldValue::String("ssh".into()));
        let encoded = encode(&col);
        assert_eq!(encoded.codec, CodecType::VarLen);
    }

    #[test]
    fn gcd_detection() {
        // Non-monotonic values that are all multiples of 100
        let mut col = ColumnBuffer::new(StorageType::U16);
        let vals = [
            500u16, 100, 300, 200, 400, 100, 500, 300, 200, 400, 100, 300, 500, 200, 400, 100, 300,
            200, 500, 400,
        ];
        for &v in &vals {
            col.push(&FieldValue::Unsigned16(v));
        }
        let encoded = encode(&col);
        assert_eq!(
            encoded.codec,
            CodecType::Gcd,
            "should detect GCD=100 for shuffled data"
        );
    }
}
