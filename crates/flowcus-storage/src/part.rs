//! Part: an immutable unit of columnar data on disk.
//!
//! # Directory layout
//!
//! Parts are stored in a time-partitioned directory tree to enable fast
//! partition pruning at the filesystem level (no readdir needed to skip years):
//!
//! ```text
//! storage/flows/{YYYY}/{MM}/{DD}/{HH}/{part_dir}/
//!   meta.bin             — binary part metadata (fixed 256-byte header)
//!   column_index.bin     — per-column codec/type/min/max index (64 bytes per column)
//!   schema.bin           — column definitions: names, IE IDs, types (variable length)
//!   columns/
//!     {column_name}.col  — encoded column data with 64-byte binary header
//! ```
//!
//! # Part directory naming
//!
//! `{gen:05}_{min_ts}_{max_ts}_{seq:06}`
//!
//! - `gen`: merge generation (00000 = raw ingested, increments on each merge, max 65535)
//! - `min_ts`: minimum export_time as unix seconds (u32) — enables time range skip
//! - `max_ts`: maximum export_time as unix seconds (u32) — enables time range skip
//! - `seq`: monotonic sequence within the process for uniqueness
//!
//! Examples:
//!   `00000_1700000000_1700003599_000001`  — raw part, first hour
//!   `00001_1700000000_1700007199_000042`  — merged part spanning 2 hours
//!
//! By encoding min/max timestamps in the directory name, a reader scanning
//! for a time range can skip parts without opening any file (readdir only).
//! Generation in the name groups merge candidates and enables level-based
//! compaction strategies.
//!
//! # Binary metadata (meta.bin)
//!
//! Fixed 256-byte header (4 cache lines), no JSON ser/de overhead:
//!
//! ```text
//! Offset  Size  Field
//! 0       4     magic: "FMTA"
//! 4       1     version: 1
//! 5       3     reserved
//! 8       8     row_count: u64 LE
//! 16      4     generation: u32 LE
//! 20      8     time_min: u64 LE (unix milliseconds)
//! 28      8     time_max: u64 LE (unix milliseconds)
//! 36      4     observation_domain_id: u32 LE
//! 40      8     created_at_ms: u64 LE
//! 48      8     disk_bytes: u64 LE
//! 56      4     column_count: u32 LE
//! 60      8     schema_fingerprint: u64 LE
//! 68      2     exporter_port: u16 LE
//! 70      1     exporter_family: u8 (4=IPv4, 6=IPv6)
//! 71      1     reserved
//! 72      16    exporter_addr: IPv4 in first 4 bytes or full IPv6
//! 88      4     CRC32-C over bytes 0..88
//! 92      164   reserved (for future: column index, bloom filter offsets, etc.)
//! ```
//!
//! # Column file layout (.col)
//!
//! 64-byte header (1 cache line) + encoded data:
//!
//! ```text
//! Offset  Size  Field
//! 0       4     magic: "FCOL"
//! 4       1     version: 1
//! 5       1     codec: CodecType
//! 6       1     storage_type: StorageType
//! 7       1     compression: CompressionType
//! 8       8     row_count: u64 LE
//! 16      8     raw_size: u64 LE
//! 24      8     encoded_size: u64 LE
//! 32      16    min_value
//! 48      16    max_value
//! 64      ...   encoded column data
//! ```

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use tracing::debug;

use crate::codec::EncodedColumn;
use crate::granule::{self, GranuleBloom, GranuleMark};
use crate::schema::{ColumnDef, Schema, StorageType};

// ---- Constants ----

pub const COLUMN_MAGIC: &[u8; 4] = b"FCOL";
pub const META_MAGIC: &[u8; 4] = b"FMTA";
pub const COLUMN_VERSION: u8 = 1;
pub const META_VERSION: u8 = 1;
pub const COLUMN_HEADER_SIZE: usize = 64;
pub const META_HEADER_SIZE: usize = 256;
/// Per-column index entry size (1 cache line per column).
pub const COLUMN_INDEX_ENTRY_SIZE: usize = 64;
pub const COLUMN_INDEX_MAGIC: &[u8; 4] = b"FCIX";
pub const SCHEMA_MAGIC: &[u8; 4] = b"FSCH";

/// Current part format version. Encoded in directory name as `v{N}_...`.
/// - v0 (implicit, no prefix): original format (4-segment dir name).
/// - v1 (current, 5-segment dir name): adds version prefix to directory name.
pub const PART_FORMAT_VERSION: u32 = 1;

// ---- Part metadata ----

/// Binary part metadata. All fields are needed for scan-limiting decisions
/// without opening column files.
#[derive(Debug, Clone)]
pub struct PartMetadata {
    pub row_count: u64,
    pub generation: u32,
    pub time_min: u64,
    pub time_max: u64,
    pub observation_domain_id: u32,
    pub created_at_ms: u64,
    pub disk_bytes: u64,
    pub column_count: u32,
    pub schema_fingerprint: u64,
    pub exporter: SocketAddr,
    pub schema: Schema,
}

// ---- Path construction ----

/// Build the time-partitioned directory path: `{base}/{YYYY}/{MM}/{DD}/{HH}/`
///
/// `unix_ms` is a unix timestamp in milliseconds. Internally converted to
/// seconds for YMD/hour computation.
pub fn hour_partition_dir(base: &Path, unix_ms: u64) -> PathBuf {
    let ts = unix_ms / 1000;
    let (year, month, day) = days_to_ymd(ts / 86400);
    let hour = (ts % 86400) / 3600;
    base.join(format!("{year:04}"))
        .join(format!("{month:02}"))
        .join(format!("{day:02}"))
        .join(format!("{hour:02}"))
}

/// Build the part directory name: `{ver}_{gen:05}_{min}_{max}_{seq:06}`
///
/// Encoding format version + generation + min/max timestamps enables:
/// - Format version detection by readdir (migration decisions without file open)
/// - Time range skip by readdir (no file open needed)
/// - Merge candidate grouping by generation
/// - Lexicographic sort within a generation = chronological order
pub fn part_dir_name(generation: u32, time_min: u64, time_max: u64, seq: u32) -> String {
    format!("{PART_FORMAT_VERSION}_{generation:05}_{time_min}_{time_max}_{seq:06}")
}

/// Resolve full path for a part: `{base}/{YYYY}/{MM}/{DD}/{HH}/{part_name}/`
pub fn part_path(base: &Path, time_min: u64, generation: u32, time_max: u64, seq: u32) -> PathBuf {
    let hour_dir = hour_partition_dir(base, time_min);
    let name = part_dir_name(generation, time_min, time_max, seq);
    hour_dir.join(name)
}

/// Parse with format version stripped. Returns `(generation, min_ts, max_ts, seq)`.
/// Only used in tests — production code uses `parse_part_dir_name_versioned`.
#[cfg(test)]
pub fn parse_part_dir_name(name: &str) -> Option<(u32, u64, u64, u32)> {
    let (_, generation, min, max, seq) = parse_part_dir_name_versioned(name)?;
    Some((generation, min, max, seq))
}

/// Parse with format version. Returns `(format_version, generation, min_ts, max_ts, seq)`.
pub fn parse_part_dir_name_versioned(name: &str) -> Option<(u32, u32, u64, u64, u32)> {
    let parts: Vec<&str> = name.split('_').collect();

    // Versioned format: {ver}_{gen}_{min}_{max}_{seq} — 5 segments
    if parts.len() == 5 {
        let version: u32 = parts[0].parse().ok()?;
        let generation = parts[1].parse().ok()?;
        let min_ts: u64 = parts[2].parse().ok()?;
        let max_ts: u64 = parts[3].parse().ok()?;
        let seq = parts[4].parse().ok()?;
        return Some((version, generation, min_ts, max_ts, seq));
    }

    // Legacy format: {gen}_{min}_{max}_{seq} — 4 segments, version 0
    if parts.len() == 4 {
        let generation = parts[0].parse().ok()?;
        let min_ts: u64 = parts[1].parse().ok()?;
        let max_ts: u64 = parts[2].parse().ok()?;
        let seq = parts[3].parse().ok()?;
        return Some((0, generation, min_ts, max_ts, seq));
    }

    None
}

// ---- Writing ----

/// All data for a single column ready to be written to a part.
pub struct ColumnWriteData {
    pub def: ColumnDef,
    pub encoded: EncodedColumn,
    pub marks: Vec<GranuleMark>,
    pub blooms: Vec<GranuleBloom>,
}

/// Write a complete part to disk including column data, marks, bloom filters.
/// No fsync — relies on kernel page cache.
pub fn write_part(
    base_dir: &Path,
    metadata: &PartMetadata,
    columns: &[ColumnWriteData],
    granule_size: usize,
    bloom_bits: usize,
    seq: u32,
) -> std::io::Result<PathBuf> {
    let part_dir = part_path(
        base_dir,
        metadata.time_min,
        metadata.generation,
        metadata.time_max,
        seq,
    );

    // Write to a staging directory first, then atomic-rename to the final
    // path. This prevents queries and merges from reading a half-written
    // part. The `.staging` suffix doesn't match the part name parser, so
    // list_parts / walk_parts_recursive will never discover it.
    let staging_dir = part_dir.with_extension("staging");
    // Clean up any leftover staging dir from a previous crash
    if staging_dir.exists() {
        let _ = std::fs::remove_dir_all(&staging_dir);
    }
    let col_dir = staging_dir.join("columns");
    std::fs::create_dir_all(&col_dir)?;

    // Write column files + marks + bloom filters
    for cwd in columns {
        let name = &cwd.def.name;
        write_column_file(
            &col_dir.join(format!("{name}.col")),
            &cwd.def,
            &cwd.encoded,
            metadata.row_count,
        )?;
        granule::write_marks(
            &col_dir.join(format!("{name}.mrk")),
            &cwd.marks,
            granule_size,
        )?;
        granule::write_blooms(
            &col_dir.join(format!("{name}.bloom")),
            &cwd.blooms,
            bloom_bits,
        )?;
    }

    // Write binary metadata header
    write_meta_bin(&staging_dir.join("meta.bin"), metadata)?;

    // Column index for query planning (never open .col during planning)
    let index_data: Vec<_> = columns
        .iter()
        .map(|c| (c.def.clone(), c.encoded.clone()))
        .collect();
    write_column_index(&staging_dir.join("column_index.bin"), &index_data)?;

    // Schema for merge recovery
    write_schema_bin(&staging_dir.join("schema.bin"), &metadata.schema)?;

    // Atomic rename: part becomes visible to readers only after all files
    // are written. Directory rename is atomic on the same filesystem.
    std::fs::rename(&staging_dir, &part_dir).inspect_err(|_| {
        // Clean up staging on rename failure
        let _ = std::fs::remove_dir_all(&staging_dir);
    })?;

    debug!(
        path = %part_dir.display(),
        rows = metadata.row_count,
        generation = metadata.generation,
        time_min = metadata.time_min,
        time_max = metadata.time_max,
        columns = columns.len(),
        disk_bytes = metadata.disk_bytes,
        "Part written"
    );

    Ok(part_dir)
}

/// Write binary metadata to meta.bin (256 bytes, fixed layout).
fn write_meta_bin(path: &Path, meta: &PartMetadata) -> std::io::Result<()> {
    let mut buf = [0u8; META_HEADER_SIZE];

    // Magic (0..4)
    buf[..4].copy_from_slice(META_MAGIC);
    // Version (4)
    buf[4] = META_VERSION;
    // reserved (5..8)
    // Row count (8..16)
    buf[8..16].copy_from_slice(&meta.row_count.to_le_bytes());
    // Generation (16..20)
    buf[16..20].copy_from_slice(&meta.generation.to_le_bytes());
    // time_min (20..28) — unix milliseconds
    buf[20..28].copy_from_slice(&meta.time_min.to_le_bytes());
    // time_max (28..36) — unix milliseconds
    buf[28..36].copy_from_slice(&meta.time_max.to_le_bytes());
    // observation_domain_id (36..40)
    buf[36..40].copy_from_slice(&meta.observation_domain_id.to_le_bytes());
    // created_at_ms (40..48)
    buf[40..48].copy_from_slice(&meta.created_at_ms.to_le_bytes());
    // disk_bytes (48..56)
    buf[48..56].copy_from_slice(&meta.disk_bytes.to_le_bytes());
    // column_count (56..60)
    buf[56..60].copy_from_slice(&meta.column_count.to_le_bytes());
    // schema_fingerprint (60..68)
    buf[60..68].copy_from_slice(&meta.schema_fingerprint.to_le_bytes());
    // exporter_port (68..70)
    buf[68..70].copy_from_slice(&meta.exporter.port().to_le_bytes());
    // exporter_family (70)
    match meta.exporter.ip() {
        std::net::IpAddr::V4(_) => buf[70] = 4,
        std::net::IpAddr::V6(_) => buf[70] = 6,
    }
    // reserved (71)
    // exporter_addr (72..88)
    match meta.exporter.ip() {
        std::net::IpAddr::V4(v4) => buf[72..76].copy_from_slice(&v4.octets()),
        std::net::IpAddr::V6(v6) => buf[72..88].copy_from_slice(&v6.octets()),
    }

    // CRC32-C over bytes 0..88, written at 88..92
    let crc = crate::crc::crc32c(&buf[..88]);
    buf[88..92].copy_from_slice(&crc.to_le_bytes());

    std::fs::write(path, buf)
}

/// Column index entry: one per column, 64 bytes (1 cache line).
///
/// ```text
/// Offset  Size  Field
/// 0       1     codec: CodecType
/// 1       1     compression: CompressionType
/// 2       1     storage_type: StorageType
/// 3       1     reserved
/// 4       4     name_hash: u32 LE (FNV-1a of column name for fast lookup)
/// 8       8     row_count: u64 LE
/// 16      8     encoded_size: u64 LE (bytes on disk after header)
/// 24      8     raw_size: u64 LE (uncompressed logical size)
/// 32      16    min_value: column minimum
/// 48      16    max_value: column maximum
/// ```
///
/// The query engine reads column_index.bin as a single sequential read,
/// then uses it to decide:
/// - Which columns exist in this part
/// - Whether column value ranges overlap the query predicates (skip part)
/// - Which codec/compression to expect when actually reading .col files
/// - Estimated I/O cost for each column
#[derive(Debug, Clone)]
pub struct ColumnIndexEntry {
    pub codec: u8,
    pub compression: u8,
    pub storage_type: u8,
    pub name_hash: u32,
    pub row_count: u64,
    pub encoded_size: u64,
    pub raw_size: u64,
    pub min_value: [u8; 16],
    pub max_value: [u8; 16],
}

/// Write column_index.bin: header (8 bytes) + N * 64-byte entries.
fn write_column_index(path: &Path, columns: &[(ColumnDef, EncodedColumn)]) -> std::io::Result<()> {
    let count = columns.len();
    // Header: magic (4) + count (4) = 8 bytes, then entries
    let mut buf = Vec::with_capacity(8 + count * COLUMN_INDEX_ENTRY_SIZE);

    buf.extend_from_slice(COLUMN_INDEX_MAGIC);
    buf.extend_from_slice(&(count as u32).to_le_bytes());

    for (def, encoded) in columns {
        let mut entry = [0u8; COLUMN_INDEX_ENTRY_SIZE];
        entry[0] = encoded.codec as u8;
        entry[1] = encoded.compression as u8;
        entry[2] = storage_type_to_u8(def.storage_type);
        // entry[3] reserved
        entry[4..8].copy_from_slice(&fnv1a_hash(def.name.as_bytes()).to_le_bytes());
        entry[8..16].copy_from_slice(&(0u64).to_le_bytes()); // row_count filled from meta
        entry[16..24].copy_from_slice(&(encoded.data.len() as u64).to_le_bytes());
        entry[24..32].copy_from_slice(&(encoded.uncompressed_size as u64).to_le_bytes());
        entry[32..48].copy_from_slice(&encoded.min_value);
        entry[48..64].copy_from_slice(&encoded.max_value);
        buf.extend_from_slice(&entry);
    }

    // Append CRC32-C of entire buffer
    let crc = crate::crc::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    std::fs::write(path, buf)
}

/// Read column_index.bin. Returns entries in column order.
pub fn read_column_index(path: &Path) -> std::io::Result<Vec<ColumnIndexEntry>> {
    let buf = std::fs::read(path)?;
    if buf.len() < 12 || &buf[..4] != COLUMN_INDEX_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid column_index.bin",
        ));
    }

    // Verify trailing CRC32-C (last 4 bytes)
    let (data, crc_bytes) = buf.split_at(buf.len() - 4);
    let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
    if !crate::crc::verify_crc32c(data, stored_crc) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("CRC mismatch in {}", path.display()),
        ));
    }

    let count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(count);

    for i in 0..count {
        let off = 8 + i * COLUMN_INDEX_ENTRY_SIZE;
        if off + COLUMN_INDEX_ENTRY_SIZE > data.len() {
            break;
        }
        let e = &data[off..off + COLUMN_INDEX_ENTRY_SIZE];
        entries.push(ColumnIndexEntry {
            codec: e[0],
            compression: e[1],
            storage_type: e[2],
            name_hash: u32::from_le_bytes(e[4..8].try_into().unwrap()),
            row_count: u64::from_le_bytes(e[8..16].try_into().unwrap()),
            encoded_size: u64::from_le_bytes(e[16..24].try_into().unwrap()),
            raw_size: u64::from_le_bytes(e[24..32].try_into().unwrap()),
            min_value: e[32..48].try_into().unwrap(),
            max_value: e[48..64].try_into().unwrap(),
        });
    }

    Ok(entries)
}

/// Write schema.bin: column definitions for schema recovery.
///
/// Layout: magic(4) + count(4) + entries
/// Each entry: name_len(2) + name(N) + element_id(2) + enterprise_id(4) + data_type(1) + storage_type(1)
fn write_schema_bin(path: &Path, schema: &Schema) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(8 + schema.columns.len() * 40);

    buf.extend_from_slice(SCHEMA_MAGIC);
    buf.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());

    for col in &schema.columns {
        let name_bytes = col.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&col.element_id.to_le_bytes());
        buf.extend_from_slice(&col.enterprise_id.to_le_bytes());
        buf.push(data_type_to_u8(col.data_type));
        buf.push(storage_type_to_u8(col.storage_type));
    }

    // Append CRC32-C of entire buffer
    let crc = crate::crc::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    std::fs::write(path, buf)
}

/// Read schema.bin back into a Schema.
pub fn read_schema_bin(path: &Path) -> std::io::Result<Schema> {
    use crate::schema::ColumnDef;

    let raw = std::fs::read(path)?;
    if raw.len() < 12 || &raw[..4] != SCHEMA_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid schema.bin",
        ));
    }

    // Verify trailing CRC32-C (last 4 bytes)
    let (buf, crc_bytes) = raw.split_at(raw.len() - 4);
    let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
    if !crate::crc::verify_crc32c(buf, stored_crc) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("CRC mismatch in {}", path.display()),
        ));
    }

    let count = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
    let mut columns = Vec::with_capacity(count);
    let mut off = 8;

    for _ in 0..count {
        if off + 2 > buf.len() {
            break;
        }
        let name_len = u16::from_le_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        off += 2;
        if off + name_len + 8 > buf.len() {
            break;
        }
        let name = String::from_utf8_lossy(&buf[off..off + name_len]).into_owned();
        off += name_len;
        let element_id = u16::from_le_bytes(buf[off..off + 2].try_into().unwrap());
        off += 2;
        let enterprise_id = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
        off += 4;
        let data_type = u8_to_data_type(buf[off]);
        off += 1;
        let storage_type = u8_to_storage_type(buf[off]);
        off += 1;

        columns.push(ColumnDef {
            name,
            element_id,
            enterprise_id,
            data_type,
            storage_type,
            wire_length: storage_type
                .element_size()
                .map(|s| s as u16)
                .unwrap_or(65535),
        });
    }

    Ok(Schema {
        columns,
        duration_source: None,
    })
}

/// FNV-1a 32-bit hash for column name lookup.
fn fnv1a_hash(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in data {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// Public: compute name hash for query engine column lookup.
pub fn column_name_hash(name: &str) -> u32 {
    fnv1a_hash(name.as_bytes())
}

fn data_type_to_u8(dt: flowcus_ipfix::protocol::DataType) -> u8 {
    use flowcus_ipfix::protocol::DataType;
    match dt {
        DataType::OctetArray => 0,
        DataType::Unsigned8 => 1,
        DataType::Unsigned16 => 2,
        DataType::Unsigned32 => 3,
        DataType::Unsigned64 => 4,
        DataType::Signed8 => 5,
        DataType::Signed16 => 6,
        DataType::Signed32 => 7,
        DataType::Signed64 => 8,
        DataType::Float32 => 9,
        DataType::Float64 => 10,
        DataType::Boolean => 11,
        DataType::MacAddress => 12,
        DataType::String => 13,
        DataType::DateTimeSeconds => 14,
        DataType::DateTimeMilliseconds => 15,
        DataType::DateTimeMicroseconds => 16,
        DataType::DateTimeNanoseconds => 17,
        DataType::Ipv4Address => 18,
        DataType::Ipv6Address => 19,
    }
}

/// Read binary metadata from meta.bin. Fast: just 256 bytes, no parsing.
pub fn read_meta_bin(path: &Path) -> std::io::Result<PartMetadataHeader> {
    let buf = std::fs::read(path)?;
    if buf.len() < META_HEADER_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "meta.bin too short",
        ));
    }
    if &buf[..4] != META_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid meta.bin magic",
        ));
    }

    // Verify CRC32-C at bytes 88..92 over bytes 0..88
    let stored_crc = u32::from_le_bytes(buf[88..92].try_into().unwrap());
    if !crate::crc::verify_crc32c(&buf[..88], stored_crc) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("CRC mismatch in {}", path.display()),
        ));
    }

    Ok(PartMetadataHeader {
        row_count: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        generation: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
        time_min: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        time_max: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
        observation_domain_id: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
        created_at_ms: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
        disk_bytes: u64::from_le_bytes(buf[48..56].try_into().unwrap()),
        column_count: u32::from_le_bytes(buf[56..60].try_into().unwrap()),
        schema_fingerprint: u64::from_le_bytes(buf[60..68].try_into().unwrap()),
    })
}

/// Lightweight metadata read from binary header (no schema, no exporter details).
/// Enough for scan-limiting and merge decisions.
#[derive(Debug, Clone)]
pub struct PartMetadataHeader {
    pub row_count: u64,
    pub generation: u32,
    pub time_min: u64,
    pub time_max: u64,
    pub observation_domain_id: u32,
    pub created_at_ms: u64,
    pub disk_bytes: u64,
    pub column_count: u32,
    pub schema_fingerprint: u64,
}

// ---- Column file writing ----

fn write_column_file(
    path: &Path,
    def: &ColumnDef,
    encoded: &EncodedColumn,
    row_count: u64,
) -> std::io::Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::with_capacity(64 * 1024, file);

    let mut header = [0u8; COLUMN_HEADER_SIZE];
    header[..4].copy_from_slice(COLUMN_MAGIC);
    header[4] = COLUMN_VERSION;
    header[5] = encoded.codec as u8;
    header[6] = storage_type_to_u8(def.storage_type);
    header[7] = encoded.compression as u8;
    header[8..16].copy_from_slice(&row_count.to_le_bytes());
    let raw_size = def
        .storage_type
        .element_size()
        .map(|es| row_count * es as u64)
        .unwrap_or(encoded.uncompressed_size as u64);
    header[16..24].copy_from_slice(&raw_size.to_le_bytes());
    header[24..32].copy_from_slice(&(encoded.data.len() as u64).to_le_bytes());
    header[32..48].copy_from_slice(&encoded.min_value);
    header[48..60].copy_from_slice(&encoded.max_value[..12]);

    // CRC32-C over header bytes 0..60, written at 60..64
    let header_crc = crate::crc::crc32c(&header[..60]);
    header[60..64].copy_from_slice(&header_crc.to_le_bytes());

    writer.write_all(&header)?;
    writer.write_all(&encoded.data)?;

    // Append CRC32-C of the data section
    let data_crc = crate::crc::crc32c(&encoded.data);
    writer.write_all(&data_crc.to_le_bytes())?;

    Ok(())
}

/// Maximum merge generation. Stop merging if this is reached.
/// Maximum merge generation (u16 range). Stop merging beyond this.
pub const MAX_GENERATION: u32 = 65535;

/// Read raw encoded data from a column file (skips the 64-byte header).
/// Returns the raw bytes after the header. For merge: we concatenate
/// decoded column data from multiple source parts.
pub fn read_column_raw(path: &Path) -> std::io::Result<Vec<u8>> {
    let data = std::fs::read(path)?;
    if data.len() < COLUMN_HEADER_SIZE + 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "column file too short",
        ));
    }
    if &data[..4] != COLUMN_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid column magic",
        ));
    }

    // Verify header CRC32-C at bytes 60..64 over bytes 0..60
    let header_crc = u32::from_le_bytes(data[60..64].try_into().unwrap());
    if !crate::crc::verify_crc32c(&data[..60], header_crc) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("CRC mismatch in column header of {}", path.display()),
        ));
    }

    // Verify trailing data CRC32-C (last 4 bytes after header)
    let payload = &data[COLUMN_HEADER_SIZE..data.len() - 4];
    let data_crc = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
    if !crate::crc::verify_crc32c(payload, data_crc) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("CRC mismatch in column data of {}", path.display()),
        ));
    }

    Ok(payload.to_vec())
}

/// Read column file header only (64 bytes). Returns codec, compression, row count.
pub fn read_column_header(path: &Path) -> std::io::Result<ColumnFileHeader> {
    let mut file = std::fs::File::open(path)?;
    let mut buf = [0u8; COLUMN_HEADER_SIZE];
    std::io::Read::read_exact(&mut file, &mut buf)?;
    if &buf[..4] != COLUMN_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid column magic",
        ));
    }

    // Verify header CRC32-C at bytes 60..64 over bytes 0..60
    let header_crc = u32::from_le_bytes(buf[60..64].try_into().unwrap());
    if !crate::crc::verify_crc32c(&buf[..60], header_crc) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("CRC mismatch in column header of {}", path.display()),
        ));
    }

    Ok(ColumnFileHeader {
        codec: buf[5],
        storage_type: buf[6],
        compression: buf[7],
        row_count: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        raw_size: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        encoded_size: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
    })
}

#[derive(Debug, Clone)]
pub struct ColumnFileHeader {
    pub codec: u8,
    pub storage_type: u8,
    pub compression: u8,
    pub row_count: u64,
    pub raw_size: u64,
    pub encoded_size: u64,
}

/// List column names in a part directory.
pub fn list_columns(part_dir: &Path) -> std::io::Result<Vec<String>> {
    let col_dir = part_dir.join("columns");
    let mut names = Vec::new();
    if col_dir.exists() {
        for entry in std::fs::read_dir(&col_dir)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                if let Some(stem) = name.strip_suffix(".col") {
                    names.push(stem.to_string());
                }
            }
        }
    }
    names.sort();
    Ok(names)
}

/// Remove a part directory entirely. Used after successful merge.
pub fn remove_part(part_dir: &Path) -> std::io::Result<()> {
    tracing::debug!(path = %part_dir.display(), "Removing merged source part");
    std::fs::remove_dir_all(part_dir)
}

const fn storage_type_to_u8(st: StorageType) -> u8 {
    match st {
        StorageType::U8 => 0,
        StorageType::U16 => 1,
        StorageType::U32 => 2,
        StorageType::U64 => 3,
        StorageType::U128 => 4,
        StorageType::Mac => 5,
        StorageType::VarLen => 10,
    }
}

pub fn u8_to_storage_type(v: u8) -> StorageType {
    match v {
        0 => StorageType::U8,
        1 => StorageType::U16,
        2 => StorageType::U32,
        3 => StorageType::U64,
        4 => StorageType::U128,
        5 => StorageType::Mac,
        _ => StorageType::VarLen,
    }
}

pub fn u8_to_data_type(v: u8) -> flowcus_ipfix::protocol::DataType {
    use flowcus_ipfix::protocol::DataType;
    match v {
        1 => DataType::Unsigned8,
        2 => DataType::Unsigned16,
        3 => DataType::Unsigned32,
        4 => DataType::Unsigned64,
        5 => DataType::Signed8,
        6 => DataType::Signed16,
        7 => DataType::Signed32,
        8 => DataType::Signed64,
        9 => DataType::Float32,
        10 => DataType::Float64,
        11 => DataType::Boolean,
        12 => DataType::MacAddress,
        13 => DataType::String,
        14 => DataType::DateTimeSeconds,
        15 => DataType::DateTimeMilliseconds,
        16 => DataType::DateTimeMicroseconds,
        17 => DataType::DateTimeNanoseconds,
        18 => DataType::Ipv4Address,
        19 => DataType::Ipv6Address,
        _ => DataType::OctetArray,
    }
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_dir_name_format() {
        let name = part_dir_name(0, 1_700_000_000_000, 1_700_003_599_000, 1);
        // Format: {version}_{gen:05}_{min}_{max}_{seq:06}
        assert_eq!(name, "1_00000_1700000000000_1700003599000_000001");
    }

    #[test]
    fn part_dir_names_sort_by_time_within_generation() {
        let a = part_dir_name(0, 1_700_000_000_000, 1_700_003_599_000, 1);
        let b = part_dir_name(0, 1_700_003_600_000, 1_700_007_199_000, 2);
        assert!(a < b);
    }

    #[test]
    fn merged_parts_sort_after_raw_parts() {
        let raw = part_dir_name(0, 1_700_000_000_000, 1_700_003_599_000, 1);
        let merged = part_dir_name(1, 1_700_000_000_000, 1_700_007_199_000, 42);
        assert!(raw < merged);
    }

    #[test]
    fn parse_roundtrip() {
        let name = part_dir_name(2, 1_700_000_000_000, 1_700_003_599_000, 99);
        let (generation, min, max, seq) = parse_part_dir_name(&name).unwrap();
        assert_eq!(generation, 2);
        assert_eq!(min, 1_700_000_000_000);
        assert_eq!(max, 1_700_003_599_000);
        assert_eq!(seq, 99);
    }

    #[test]
    fn parse_versioned_roundtrip() {
        let name = part_dir_name(2, 1_700_000_000_000, 1_700_003_599_000, 99);
        let (ver, generation, min, max, seq) = parse_part_dir_name_versioned(&name).unwrap();
        assert_eq!(ver, PART_FORMAT_VERSION);
        assert_eq!(generation, 2);
        assert_eq!(min, 1_700_000_000_000);
        assert_eq!(max, 1_700_003_599_000);
        assert_eq!(seq, 99);
    }

    #[test]
    fn parse_legacy_format() {
        // Legacy v0 format: no version prefix, 4 segments
        let name = "00002_1700000000000_1700003599000_000099";
        let (ver, generation, min, max, seq) = parse_part_dir_name_versioned(name).unwrap();
        assert_eq!(ver, 0);
        assert_eq!(generation, 2);
        assert_eq!(min, 1_700_000_000_000);
        assert_eq!(max, 1_700_003_599_000);
        assert_eq!(seq, 99);
    }

    #[test]
    fn hour_partition_path() {
        let base = Path::new("/data/flows");
        // 2023-11-14T22:xx:xxZ — value is now in milliseconds
        let dir = hour_partition_dir(base, 1_700_000_000_000);
        assert_eq!(dir.to_str().unwrap(), "/data/flows/2023/11/14/22");
    }

    #[test]
    fn meta_header_sizes() {
        assert_eq!(COLUMN_HEADER_SIZE, 64);
        assert_eq!(META_HEADER_SIZE, 256);
    }

    #[test]
    fn meta_bin_roundtrip() {
        let dir = std::env::temp_dir().join("flowcus_test_meta_bin");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let meta = PartMetadata {
            row_count: 5000,
            generation: 0,
            time_min: 1_700_000_000_000,
            time_max: 1_700_003_599_000,
            observation_domain_id: 42,
            created_at_ms: 1_700_000_000_000,
            disk_bytes: 123_456,
            column_count: 8,
            schema_fingerprint: 0xDEAD_BEEF_CAFE_1234,
            exporter: "10.0.0.1:4739".parse().unwrap(),
            schema: Schema {
                columns: vec![],
                duration_source: None,
            },
        };

        write_meta_bin(&dir.join("meta.bin"), &meta).unwrap();
        let header = read_meta_bin(&dir.join("meta.bin")).unwrap();

        assert_eq!(header.row_count, 5000);
        assert_eq!(header.generation, 0);
        assert_eq!(header.time_min, 1_700_000_000_000);
        assert_eq!(header.time_max, 1_700_003_599_000);
        assert_eq!(header.observation_domain_id, 42);
        assert_eq!(header.disk_bytes, 123_456);
        assert_eq!(header.column_count, 8);
        assert_eq!(header.schema_fingerprint, 0xDEAD_BEEF_CAFE_1234);

        std::fs::remove_dir_all(&dir).ok();
    }
}
