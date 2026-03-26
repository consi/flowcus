//! Column codecs: encode/decode typed column data for disk storage.
//!
//! Two-layer encoding pipeline:
//! 1. **Transform codec**: reshapes data for better compressibility
//!    - Plain, Delta, DeltaDelta, GCD — selected automatically by data analysis
//! 2. **Compression codec**: reduces physical size on disk
//!    - None (raw), LZ4 — applied on top of the transform output
//!
//! Codec selection uses a two-tier strategy:
//! - **Known IPFIX fields**: static codec hints based on RFC-defined data semantics
//!   (timestamps → DeltaDelta, counters → Delta, low-cardinality → Plain, etc.)
//! - **Unknown fields**: heuristic analysis (sample data, score codecs, pick best)
//!
//! LZ4 compression is applied aggressively — columns are rarely read due to bloom
//! filters and mark-based seeking, so smaller disk footprint > decode speed.

mod delta;
mod fixed;
mod gcd;
mod varlen;

use serde::{Deserialize, Serialize};

use crate::column::ColumnBuffer;
use crate::schema::{ColumnDef, SYSTEM_ENTERPRISE_ID};
use flowcus_ipfix::protocol::DataType;

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
    /// Zstd compression — better ratio than LZ4, used in v2+ parts.
    Zstd = 2,
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

/// Static codec recommendation for a known column, based on IPFIX field semantics.
///
/// When present, the encoder skips heuristic analysis and uses this directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecHint {
    pub transform: CodecType,
    pub compression: CompressionType,
}

impl CodecHint {
    const fn new(transform: CodecType, compression: CompressionType) -> Self {
        Self {
            transform,
            compression,
        }
    }

    const fn dd_lz4() -> Self {
        Self::new(CodecType::DeltaDelta, CompressionType::Lz4)
    }
    const fn delta_lz4() -> Self {
        Self::new(CodecType::Delta, CompressionType::Lz4)
    }
    const fn plain_lz4() -> Self {
        Self::new(CodecType::Plain, CompressionType::Lz4)
    }
    const fn plain_zstd() -> Self {
        Self::new(CodecType::Plain, CompressionType::Zstd)
    }
}

/// Look up the optimal codec for a known column based on IPFIX field semantics.
///
/// Returns `None` for unknown/vendor IEs — these fall back to heuristic selection.
pub fn hint_for_column(col: &ColumnDef) -> Option<CodecHint> {
    // System columns (flowcus-generated metadata).
    if col.enterprise_id == SYSTEM_ENTERPRISE_ID {
        return hint_for_system_column(&col.name);
    }
    // IANA standard IEs — we know the data semantics from the RFC.
    if col.enterprise_id == 0 {
        return hint_for_iana_ie(col.element_id, col.data_type);
    }
    // Vendor IEs: no static hint, use heuristic.
    None
}

fn hint_for_system_column(name: &str) -> Option<CodecHint> {
    Some(match name {
        // Monotonic export timestamp — near-constant rate within a part.
        "flowcusExportTime" => CodecHint::dd_lz4(),
        // Exporter IP is usually constant or near-constant per part — LZ4 crushes it.
        "flowcusExporterIPv4" => CodecHint::plain_lz4(),
        // Exporter port — typically constant per session.
        "flowcusExporterPort" => CodecHint::plain_lz4(),
        // Observation domain — usually constant per exporter.
        "flowcusObservationDomainId" => CodecHint::plain_lz4(),
        // Computed flow duration — variable but benefits from LZ4.
        "flowcusFlowDuration" => CodecHint::plain_lz4(),
        // UUIDv7 row identifier — monotonic U128, zstd handles the timestamp prefix well.
        "flowcusRowId" => CodecHint::plain_zstd(),
        _ => return None,
    })
}

fn hint_for_iana_ie(id: u16, data_type: DataType) -> Option<CodecHint> {
    // First: specific overrides for IEs where we know exact behavior.
    if let Some(hint) = hint_for_iana_ie_by_id(id) {
        return Some(hint);
    }
    // Second: fall back to data-type-based defaults.
    hint_for_data_type(data_type)
}

/// Specific codec hints for well-known IANA IEs by element ID.
#[allow(clippy::too_many_lines)]
fn hint_for_iana_ie_by_id(id: u16) -> Option<CodecHint> {
    Some(match id {
        // ── Timestamps: monotonically increasing within a part ──────────
        // flowEndSysUpTime, flowStartSysUpTime
        21 | 22 => CodecHint::dd_lz4(),
        // flowStartSeconds, flowEndSeconds
        150 | 151 => CodecHint::dd_lz4(),
        // flowStartMilliseconds, flowEndMilliseconds
        152 | 153 => CodecHint::dd_lz4(),
        // flowStartMicroseconds, flowEndMicroseconds
        154 | 155 => CodecHint::dd_lz4(),
        // flowStartNanoseconds, flowEndNanoseconds
        156 | 157 => CodecHint::dd_lz4(),
        // flowStartDeltaMicroseconds, flowEndDeltaMicroseconds
        158 | 159 => CodecHint::delta_lz4(),
        // systemInitTimeMilliseconds
        160 => CodecHint::dd_lz4(),

        // ── Counters: generally increasing, large absolute values ───────
        // octetDeltaCount, packetDeltaCount, deltaFlowCount
        1 | 2 | 3 => CodecHint::delta_lz4(),
        // postMCastPacketDeltaCount, postMCastOctetDeltaCount
        19 | 20 => CodecHint::delta_lz4(),
        // postOctetDeltaCount, postPacketDeltaCount
        23 | 24 => CodecHint::delta_lz4(),
        // minimumIpTotalLength, maximumIpTotalLength
        25 | 26 => CodecHint::plain_lz4(),
        // exportedOctetTotalCount
        40 => CodecHint::delta_lz4(),
        // exportedMessageTotalCount, exportedFlowRecordTotalCount
        41 | 42 => CodecHint::delta_lz4(),
        // octetTotalCount, packetTotalCount
        85 | 86 => CodecHint::delta_lz4(),
        // observedFlowTotalCount..notSentOctetTotalCount
        163..=168 => CodecHint::delta_lz4(),
        // layer2OctetDeltaCount, layer2OctetTotalCount
        298 | 299 | 300 => CodecHint::delta_lz4(),

        // ── Low-cardinality u8 fields: few distinct values ──────────────
        // protocolIdentifier (~10 distinct in practice)
        4 => CodecHint::plain_lz4(),
        // ipClassOfService / ToS
        5 => CodecHint::plain_lz4(),
        // sourceIPv4PrefixLength, destinationIPv4PrefixLength
        9 | 13 => CodecHint::plain_lz4(),
        // igmpType, samplingAlgorithm
        33 | 35 => CodecHint::plain_lz4(),
        // engineType, engineId
        38 | 39 => CodecHint::plain_lz4(),
        // minimumTTL, maximumTTL
        52 | 53 => CodecHint::plain_lz4(),
        // postIpClassOfService
        55 => CodecHint::plain_lz4(),
        // sourceIPv6PrefixLength, destinationIPv6PrefixLength
        29 | 30 => CodecHint::plain_lz4(),
        // ipVersion (4 or 6)
        60 => CodecHint::plain_lz4(),
        // flowDirection (0 or 1)
        61 => CodecHint::plain_lz4(),
        // forwardingStatus
        89 => CodecHint::plain_lz4(),
        // flowEndReason
        136 => CodecHint::plain_lz4(),
        // icmpTypeIPv4, icmpCodeIPv4, icmpTypeIPv6, icmpCodeIPv6
        176 | 177 | 178 | 179 => CodecHint::plain_lz4(),
        // mplsTopLabelType
        46 => CodecHint::plain_lz4(),

        // ── Low-cardinality u16 fields ──────────────────────────────────
        // tcpControlBits (bitmask, few distinct combos)
        6 => CodecHint::plain_lz4(),
        // icmpTypeCodeIPv4, icmpTypeCodeIPv6
        32 | 139 => CodecHint::plain_lz4(),
        // vlanId, postVlanId
        58 | 59 => CodecHint::plain_lz4(),
        // samplerId, samplerMode
        48 | 49 => CodecHint::plain_lz4(),
        // mplsTopLabelPrefixLength
        91 => CodecHint::plain_lz4(),

        // ── Port numbers: high cardinality but LZ4 helps with locality ──
        // sourceTransportPort, destinationTransportPort
        7 | 11 => CodecHint::plain_lz4(),
        // UDP/TCP source/dest ports (duplicate IEs in some templates)
        180 | 181 | 182 | 183 => CodecHint::plain_lz4(),

        // ── Interface indices: low cardinality u32 ──────────────────────
        // ingressInterface, egressInterface
        10 | 14 => CodecHint::plain_lz4(),

        // ── AS numbers: moderate cardinality ────────────────────────────
        // bgpSourceAsNumber, bgpDestinationAsNumber
        16 | 17 => CodecHint::plain_lz4(),

        // ── IP addresses: no numeric pattern, but LZ4 helps with subnet locality ──
        // sourceIPv4Address, destinationIPv4Address
        8 | 12 => CodecHint::plain_lz4(),
        // ipNextHopIPv4Address, bgpNextHopIPv4Address
        15 | 18 => CodecHint::plain_lz4(),
        // sourceIPv4Prefix, destinationIPv4Prefix
        44 | 45 => CodecHint::plain_lz4(),
        // exporterIPv4Address, exporterIPv6Address
        130 | 131 => CodecHint::plain_lz4(),

        // ── Durations / timeouts ────────────────────────────────────────
        // flowActiveTimeout, flowIdleTimeout (usually constant per exporter)
        36 | 37 => CodecHint::plain_lz4(),
        // flowDurationMilliseconds, flowDurationMicroseconds
        161 | 162 => CodecHint::plain_lz4(),
        // samplingInterval
        34 => CodecHint::plain_lz4(),
        // samplerRandomInterval
        50 => CodecHint::plain_lz4(),

        // ── Identifiers: various u32/u64 ────────────────────────────────
        // fragmentIdentification
        54 => CodecHint::plain_lz4(),
        // flowLabelIPv6
        31 => CodecHint::plain_lz4(),
        // fragmentOffset
        87 => CodecHint::plain_lz4(),
        // commonPropertiesId, observationPointId
        137 | 138 => CodecHint::plain_lz4(),
        // lineCardId, portId, meteringProcessId, exportingProcessId
        141 | 142 | 143 | 144 => CodecHint::plain_lz4(),
        // templateId
        145 => CodecHint::plain_lz4(),
        // flowId
        148 => CodecHint::delta_lz4(),
        // observationDomainId
        149 => CodecHint::plain_lz4(),
        // httpStatusCode, dnsQueryType, dnsResponseCode
        459 | 469 | 470 => CodecHint::plain_lz4(),

        // ── NAT fields (RFC 8158) ─────────────────────────────────────
        // postNATSourceIPv4Address, postNATDestinationIPv4Address
        225 | 226 => CodecHint::plain_lz4(),
        // postNAPTSourceTransportPort, postNAPTDestinationTransportPort
        227 | 228 => CodecHint::plain_lz4(),

        _ => return None,
    })
}

/// Default codec hints based on IPFIX abstract data type.
/// Used for IANA IEs not covered by `hint_for_iana_ie_by_id`.
fn hint_for_data_type(dt: DataType) -> Option<CodecHint> {
    Some(match dt {
        // All timestamp types are monotonic within a part.
        DataType::DateTimeSeconds
        | DataType::DateTimeMilliseconds
        | DataType::DateTimeMicroseconds
        | DataType::DateTimeNanoseconds => CodecHint::dd_lz4(),
        // IP addresses: no transform helps, but LZ4 benefits from subnet locality.
        DataType::Ipv4Address | DataType::Ipv6Address => CodecHint::plain_lz4(),
        // MAC addresses: similar to IPs.
        DataType::MacAddress => CodecHint::plain_lz4(),
        // Strings/octet arrays: always compress.
        DataType::String | DataType::OctetArray => {
            CodecHint::new(CodecType::VarLen, CompressionType::Lz4)
        }
        // Numeric types without specific hints: no static recommendation,
        // let the heuristic decide the transform but always compress.
        _ => return None,
    })
}

/// Minimum data size for compression to be worthwhile. Aggressive: columns are
/// rarely read (bloom/mark skipping), so we favour smaller disk footprint.
const COMPRESS_MIN_SIZE: usize = 32;

/// Zstd compression level — level 3 provides good ratio with fast decode.
const ZSTD_LEVEL: i32 = 3;

/// Encode a column buffer for v1 parts (LZ4 compression).
pub fn encode(buffer: &ColumnBuffer, col: &ColumnDef) -> EncodedColumn {
    encode_with_version(buffer, col, 1)
}

/// Encode a column buffer for v2 parts (zstd compression).
pub fn encode_v2(buffer: &ColumnBuffer, col: &ColumnDef) -> EncodedColumn {
    encode_with_version(buffer, col, 2)
}

/// Encode a column buffer, selecting codec via static hint or heuristic fallback.
///
/// When `col` provides a known IPFIX field, the optimal codec is selected
/// statically (no data sampling). Unknown fields fall back to heuristic analysis.
/// `part_version` determines whether LZ4 (v1) or zstd (v2+) compression is used.
fn encode_with_version(buffer: &ColumnBuffer, col: &ColumnDef, part_version: u32) -> EncodedColumn {
    let hint = hint_for_column(col);

    let mut encoded = match buffer {
        ColumnBuffer::VarLen { offsets, data } => varlen::encode(offsets, data),
        _ => {
            if let Some(h) = hint {
                encode_fixed_with_hint(buffer, h.transform)
            } else {
                encode_fixed(buffer)
            }
        }
    };

    // v2+ parts use zstd; v1 parts use lz4.
    if part_version >= 2 {
        let force = hint.is_some_and(|h| {
            h.compression == CompressionType::Lz4 || h.compression == CompressionType::Zstd
        });
        encoded = try_compress_zstd(encoded, force);
    } else {
        let force_lz4 = hint.is_some_and(|h| h.compression == CompressionType::Lz4);
        encoded = try_compress_lz4(encoded, force_lz4);
    }
    encoded
}

/// Encode using a specific transform codec (from a hint), skipping heuristic analysis.
fn encode_fixed_with_hint(buffer: &ColumnBuffer, transform: CodecType) -> EncodedColumn {
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

    let (min_value, max_value) = compute_min_max(buffer);

    // MAC/U128 cannot use delta/gcd — fall back to Plain.
    let effective_transform = if (element_size == 6 || element_size == 16)
        && !matches!(transform, CodecType::Plain | CodecType::VarLen)
    {
        CodecType::Plain
    } else {
        transform
    };

    let data = match effective_transform {
        CodecType::Plain => raw.to_vec(),
        CodecType::Delta => delta::encode_delta(buffer, element_size),
        CodecType::DeltaDelta => delta::encode_delta_delta(buffer, element_size),
        CodecType::Gcd => gcd::encode_gcd(buffer, element_size),
        CodecType::VarLen => unreachable!("VarLen handled in caller"),
    };

    tracing::trace!(
        codec = ?effective_transform,
        raw_size = raw.len(),
        encoded_size = data.len(),
        row_count = buffer.row_count(),
        element_size,
        hint = true,
        "Column encoded (hint)"
    );

    EncodedColumn::new(effective_transform, data, min_value, max_value)
}

/// Apply LZ4 compression if it actually reduces size.
///
/// When `force` is true (hint-driven), we always attempt compression regardless
/// of data size — the hint already determined this column benefits from LZ4.
fn try_compress_lz4(mut encoded: EncodedColumn, force: bool) -> EncodedColumn {
    if !force && encoded.data.len() < COMPRESS_MIN_SIZE {
        return encoded;
    }

    if encoded.data.is_empty() {
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

/// Apply zstd compression if it actually reduces size.
fn try_compress_zstd(mut encoded: EncodedColumn, force: bool) -> EncodedColumn {
    if !force && encoded.data.len() < COMPRESS_MIN_SIZE {
        return encoded;
    }

    if encoded.data.is_empty() {
        return encoded;
    }

    match zstd::bulk::compress(&encoded.data, ZSTD_LEVEL) {
        Ok(compressed) if compressed.len() < encoded.data.len() => {
            encoded.uncompressed_size = encoded.data.len();
            encoded.data = compressed;
            encoded.compression = CompressionType::Zstd;
        }
        _ => {}
    }
    encoded
}

/// Decompress data based on the compression type stored in the column header.
///
/// For zstd, we use streaming decompression instead of `bulk::decompress` to
/// avoid "Destination buffer is too small" errors when the stored `raw_size`
/// doesn't match the actual decompressed size (e.g. GCD codec adds a header
/// value, making the encoded output larger than `row_count * element_size`).
pub fn decompress(
    data: &[u8],
    compression: CompressionType,
    uncompressed_size: usize,
) -> std::io::Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Lz4 => lz4_flex::decompress_size_prepended(data).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("LZ4 error: {e}"))
        }),
        CompressionType::Zstd => {
            // Pre-allocate with the hint but let the decoder grow if needed.
            let mut out = Vec::with_capacity(uncompressed_size);
            let mut decoder = zstd::Decoder::new(std::io::Cursor::new(data))?;
            std::io::Read::read_to_end(&mut decoder, &mut out)?;
            Ok(out)
        }
    }
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
        ColumnBuffer::U128(v) => {
            if !v.is_empty() {
                let min = v
                    .iter()
                    .min_by(|a, b| a[0].cmp(&b[0]).then(a[1].cmp(&b[1])))
                    .unwrap();
                let max = v
                    .iter()
                    .max_by(|a, b| a[0].cmp(&b[0]).then(a[1].cmp(&b[1])))
                    .unwrap();
                min_val[..8].copy_from_slice(&min[0].to_be_bytes());
                min_val[8..].copy_from_slice(&min[1].to_be_bytes());
                max_val[..8].copy_from_slice(&max[0].to_be_bytes());
                max_val[8..].copy_from_slice(&max[1].to_be_bytes());
            }
        }
        _ => {
            // For Mac, VarLen: zeroed min/max for now
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

    /// Helper: build a ColumnDef for an IANA IE.
    fn iana_col(name: &str, id: u16, dt: DataType, st: StorageType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            element_id: id,
            enterprise_id: 0,
            data_type: dt,
            storage_type: st,
            wire_length: st.element_size().unwrap_or(65535) as u16,
        }
    }

    /// Helper: build a ColumnDef for a system column.
    fn sys_col(name: &str, dt: DataType, st: StorageType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            element_id: 0,
            enterprise_id: SYSTEM_ENTERPRISE_ID,
            data_type: dt,
            storage_type: st,
            wire_length: st.element_size().unwrap_or(65535) as u16,
        }
    }

    /// Helper: build a ColumnDef for an unknown vendor IE.
    fn vendor_col(name: &str, pen: u32, id: u16, dt: DataType, st: StorageType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            element_id: id,
            enterprise_id: pen,
            data_type: dt,
            storage_type: st,
            wire_length: st.element_size().unwrap_or(65535) as u16,
        }
    }

    // ── Hint lookup tests ──────────────────────────────────────────────

    #[test]
    fn hint_timestamp_fields_pick_delta_delta() {
        for id in [21, 22, 150, 151, 152, 153, 154, 155, 156, 157, 160] {
            let h = hint_for_iana_ie_by_id(id);
            assert_eq!(
                h.map(|h| h.transform),
                Some(CodecType::DeltaDelta),
                "IE {id} should hint DeltaDelta"
            );
            assert_eq!(
                h.map(|h| h.compression),
                Some(CompressionType::Lz4),
                "IE {id} should hint LZ4"
            );
        }
    }

    #[test]
    fn hint_counter_fields_pick_delta() {
        for id in [
            1, 2, 3, 19, 20, 23, 24, 40, 41, 42, 85, 86, 163, 164, 165, 298,
        ] {
            let h = hint_for_iana_ie_by_id(id);
            assert_eq!(
                h.map(|h| h.transform),
                Some(CodecType::Delta),
                "IE {id} should hint Delta"
            );
        }
    }

    #[test]
    fn hint_low_cardinality_fields_pick_plain_lz4() {
        for id in [4, 5, 6, 60, 61, 136, 58, 59, 7, 11] {
            let h = hint_for_iana_ie_by_id(id);
            assert_eq!(
                h.map(|h| h.transform),
                Some(CodecType::Plain),
                "IE {id} should hint Plain"
            );
            assert_eq!(
                h.map(|h| h.compression),
                Some(CompressionType::Lz4),
                "IE {id} should hint LZ4"
            );
        }
    }

    #[test]
    fn hint_system_columns() {
        let h = hint_for_system_column("flowcusExportTime");
        assert_eq!(h.unwrap().transform, CodecType::DeltaDelta);

        let h = hint_for_system_column("flowcusExporterIPv4");
        assert_eq!(h.unwrap().transform, CodecType::Plain);

        let h = hint_for_system_column("flowcusObservationDomainId");
        assert_eq!(h.unwrap().transform, CodecType::Plain);
    }

    #[test]
    fn hint_vendor_ie_returns_none() {
        let col = vendor_col("ciscoApp", 9, 100, DataType::Unsigned32, StorageType::U32);
        assert!(hint_for_column(&col).is_none());
    }

    #[test]
    fn hint_unknown_iana_ie_falls_through_to_data_type() {
        // IE 999 is not in our map, but DateTimeSeconds → DeltaDelta via data type.
        let col = iana_col(
            "unknown_ts",
            999,
            DataType::DateTimeSeconds,
            StorageType::U32,
        );
        let h = hint_for_column(&col).unwrap();
        assert_eq!(h.transform, CodecType::DeltaDelta);
        assert_eq!(h.compression, CompressionType::Lz4);
    }

    #[test]
    fn hint_unknown_iana_numeric_returns_none() {
        // IE 999 with Unsigned32 is not covered by ID or data type → None → heuristic.
        let col = iana_col("unknown_u32", 999, DataType::Unsigned32, StorageType::U32);
        assert!(hint_for_column(&col).is_none());
    }

    // ── Encode integration tests ───────────────────────────────────────

    #[test]
    fn encode_monotonic_timestamps_uses_delta_delta() {
        let col_def = iana_col(
            "flowStartSeconds",
            150,
            DataType::DateTimeSeconds,
            StorageType::U32,
        );
        let mut col = ColumnBuffer::new(StorageType::U32);
        for i in 0..100u32 {
            col.push(&FieldValue::DateTimeSeconds(1_700_000_000 + i));
        }
        let encoded = encode(&col, &col_def);
        assert_eq!(
            encoded.codec,
            CodecType::DeltaDelta,
            "hint should force DeltaDelta for flowStartSeconds"
        );
        assert_eq!(encoded.compression, CompressionType::Lz4);
        assert!(encoded.data.len() < col.as_raw_bytes().len());
    }

    #[test]
    fn encode_counter_uses_delta() {
        let col_def = iana_col("octetDeltaCount", 1, DataType::Unsigned64, StorageType::U64);
        let mut col = ColumnBuffer::new(StorageType::U64);
        for i in 0..100u64 {
            col.push(&FieldValue::Unsigned64(1000 + i * 50));
        }
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::Delta);
        assert_eq!(encoded.compression, CompressionType::Lz4);
    }

    #[test]
    fn encode_protocol_id_uses_plain_lz4() {
        let col_def = iana_col(
            "protocolIdentifier",
            4,
            DataType::Unsigned8,
            StorageType::U8,
        );
        let mut col = ColumnBuffer::new(StorageType::U8);
        // Typical: mostly TCP (6) and UDP (17)
        for _ in 0..50 {
            col.push(&FieldValue::Unsigned8(6));
        }
        for _ in 0..50 {
            col.push(&FieldValue::Unsigned8(17));
        }
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::Plain);
        assert_eq!(encoded.compression, CompressionType::Lz4);
    }

    #[test]
    fn encode_export_time_system_column() {
        let col_def = sys_col(
            "flowcusExportTime",
            DataType::DateTimeSeconds,
            StorageType::U32,
        );
        let mut col = ColumnBuffer::new(StorageType::U32);
        for i in 0..200u32 {
            col.push(&FieldValue::DateTimeSeconds(1_700_000_000 + i));
        }
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::DeltaDelta);
        assert_eq!(encoded.compression, CompressionType::Lz4);
    }

    #[test]
    fn encode_exporter_ipv4_constant_compresses_well() {
        let col_def = sys_col(
            "flowcusExporterIPv4",
            DataType::Ipv4Address,
            StorageType::U32,
        );
        let mut col = ColumnBuffer::new(StorageType::U32);
        // All same IP — LZ4 should crush this.
        for _ in 0..1000 {
            col.push(&FieldValue::Ipv4(Ipv4Addr::new(10, 0, 0, 1)));
        }
        let raw_size = col.as_raw_bytes().len();
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::Plain);
        assert_eq!(encoded.compression, CompressionType::Lz4);
        // 1000 identical 4-byte values → LZ4 should achieve >90% compression.
        assert!(
            encoded.data.len() < raw_size / 5,
            "expected >80% compression for constant IP, got {} -> {} bytes",
            raw_size,
            encoded.data.len()
        );
    }

    #[test]
    fn encode_random_ipv4_uses_plain_with_lz4() {
        let col_def = iana_col(
            "sourceIPv4Address",
            8,
            DataType::Ipv4Address,
            StorageType::U32,
        );
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
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::Plain);
    }

    #[test]
    fn encode_varlen_strings() {
        let col_def = iana_col("interfaceName", 82, DataType::String, StorageType::VarLen);
        let mut col = ColumnBuffer::new(StorageType::VarLen);
        col.push(&FieldValue::String("https".into()));
        col.push(&FieldValue::String("dns".into()));
        col.push(&FieldValue::String("ssh".into()));
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::VarLen);
    }

    #[test]
    fn encode_vendor_ie_uses_heuristic() {
        let col_def = vendor_col(
            "ciscoCounter",
            9,
            100,
            DataType::Unsigned32,
            StorageType::U32,
        );
        let mut col = ColumnBuffer::new(StorageType::U32);
        // Monotonic data — heuristic should still detect delta.
        for i in 0..100u32 {
            col.push(&FieldValue::Unsigned32(1000 + i));
        }
        let encoded = encode(&col, &col_def);
        // Heuristic should pick Delta or DeltaDelta.
        assert!(
            encoded.codec == CodecType::Delta || encoded.codec == CodecType::DeltaDelta,
            "heuristic should detect monotonic pattern, got {:?}",
            encoded.codec
        );
    }

    #[test]
    fn gcd_detection_heuristic_for_unknown() {
        let col_def = vendor_col(
            "vendorField",
            9,
            200,
            DataType::Unsigned16,
            StorageType::U16,
        );
        let mut col = ColumnBuffer::new(StorageType::U16);
        let vals = [
            500u16, 100, 300, 200, 400, 100, 500, 300, 200, 400, 100, 300, 500, 200, 400, 100, 300,
            200, 500, 400,
        ];
        for &v in &vals {
            col.push(&FieldValue::Unsigned16(v));
        }
        let encoded = encode(&col, &col_def);
        assert_eq!(
            encoded.codec,
            CodecType::Gcd,
            "heuristic should still detect GCD=100"
        );
    }

    #[test]
    fn encode_observation_domain_constant() {
        let col_def = sys_col(
            "flowcusObservationDomainId",
            DataType::Unsigned32,
            StorageType::U32,
        );
        let mut col = ColumnBuffer::new(StorageType::U32);
        for _ in 0..500 {
            col.push(&FieldValue::Unsigned32(42));
        }
        let raw_size = col.as_raw_bytes().len();
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::Plain);
        assert_eq!(encoded.compression, CompressionType::Lz4);
        assert!(
            encoded.data.len() < raw_size / 2,
            "constant u32 column should compress well"
        );
    }

    #[test]
    fn encode_millisecond_timestamps_delta_delta() {
        let col_def = iana_col(
            "flowStartMilliseconds",
            152,
            DataType::DateTimeMilliseconds,
            StorageType::U64,
        );
        let mut col = ColumnBuffer::new(StorageType::U64);
        let base: u64 = 1_700_000_000_000;
        for i in 0..200u64 {
            // Near-constant 1000ms intervals.
            col.push(&FieldValue::Unsigned64(base + i * 1000));
        }
        let raw_size = col.as_raw_bytes().len();
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::DeltaDelta);
        assert_eq!(encoded.compression, CompressionType::Lz4);
        // DeltaDelta on constant-rate + LZ4 should compress extremely well.
        assert!(
            encoded.data.len() < raw_size / 4,
            "constant-rate ms timestamps should compress >75%, got {} -> {}",
            raw_size,
            encoded.data.len()
        );
    }

    #[test]
    fn encode_ports_plain_lz4() {
        let col_def = iana_col(
            "sourceTransportPort",
            7,
            DataType::Unsigned16,
            StorageType::U16,
        );
        let mut col = ColumnBuffer::new(StorageType::U16);
        // Mix of common ports — LZ4 helps with repeated values.
        for _ in 0..50 {
            col.push(&FieldValue::Unsigned16(443));
            col.push(&FieldValue::Unsigned16(80));
            col.push(&FieldValue::Unsigned16(53));
            col.push(&FieldValue::Unsigned16(8080));
        }
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::Plain);
        assert_eq!(encoded.compression, CompressionType::Lz4);
    }

    #[test]
    fn encode_mac_address_plain() {
        let col_def = iana_col(
            "sourceMacAddress",
            56,
            DataType::MacAddress,
            StorageType::Mac,
        );
        let mut col = ColumnBuffer::new(StorageType::Mac);
        for i in 0..100u8 {
            col.push(&FieldValue::Mac([0xAA, 0xBB, 0xCC, 0x00, 0x00, i]));
        }
        let encoded = encode(&col, &col_def);
        // MAC columns use Plain (delta not supported for 6-byte).
        assert_eq!(encoded.codec, CodecType::Plain);
    }

    #[test]
    fn encode_ipv6_plain() {
        let col_def = iana_col(
            "sourceIPv6Address",
            27,
            DataType::Ipv6Address,
            StorageType::U128,
        );
        let mut col = ColumnBuffer::new(StorageType::U128);
        for i in 0..50u128 {
            col.push(&FieldValue::Ipv6(std::net::Ipv6Addr::from(
                0xfe80_0000_0000_0000_0000_0000_0000_0001u128 + i,
            )));
        }
        let encoded = encode(&col, &col_def);
        // U128 cannot use delta — must fall back to Plain even though hint says Plain.
        assert_eq!(encoded.codec, CodecType::Plain);
    }

    #[test]
    fn encode_flow_direction_compresses_well() {
        let col_def = iana_col("flowDirection", 61, DataType::Unsigned8, StorageType::U8);
        let mut col = ColumnBuffer::new(StorageType::U8);
        // Only 0 and 1 — extremely low cardinality.
        for i in 0..1000u32 {
            col.push(&FieldValue::Unsigned8((i % 2) as u8));
        }
        let raw_size = col.as_raw_bytes().len();
        let encoded = encode(&col, &col_def);
        assert_eq!(encoded.codec, CodecType::Plain);
        assert_eq!(encoded.compression, CompressionType::Lz4);
        assert!(
            encoded.data.len() < raw_size / 2,
            "binary column should compress >50%"
        );
    }
}
