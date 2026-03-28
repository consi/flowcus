//! Granule-level indexes: marks and bloom filters.
//!
//! A **granule** is a fixed-size group of rows (default 8192). For each granule
//! in a column, we store:
//!
//! - **Mark**: byte offset into the .col data section + min/max values.
//!   Enables seeking directly to the relevant data without scanning.
//!
//! - **Bloom filter**: probabilistic membership test for point queries.
//!   Enables skipping entire granules that definitely don't contain a value.
//!
//! # File formats
//!
//! ## Marks (`{column}.mrk`)
//! ```text
//! Header (16 bytes):
//!   magic: "FMRK" (4)
//!   granule_size: u32 LE (4)
//!   num_granules: u32 LE (4)
//!   reserved: u32 (4)
//!
//! Per granule (48 bytes, 3/4 cache line):
//!   data_offset: u64 LE   — byte position in .col data (after 64-byte header)
//!   row_start: u32 LE     — first row index
//!   row_count: u32 LE     — rows in this granule
//!   min_value: [u8; 16]   — granule minimum (for range pruning)
//!   max_value: [u8; 16]   — granule maximum (for range pruning)
//! ```
//!
//! ## Bloom filters (`{column}.bloom`)
//! ```text
//! Header (16 bytes):
//!   magic: "FBLM" (4)
//!   num_granules: u32 LE (4)
//!   bits_per_granule: u32 LE (4)
//!   num_hashes: u32 LE (4)
//!
//! Per granule: bits_per_granule / 8 bytes of filter bits
//! ```

use std::io::Write;
use std::path::Path;

use crate::column::ColumnBuffer;
use crate::schema::StorageType;

pub const MARK_MAGIC: &[u8; 4] = b"FMRK";
pub const BLOOM_MAGIC: &[u8; 4] = b"FBLM";
pub const MARK_HEADER_SIZE: usize = 16;
pub const MARK_ENTRY_SIZE: usize = 48;
/// Number of hash functions for bloom filter.
pub const BLOOM_NUM_HASHES: u32 = 3;

/// A single granule mark entry.
#[derive(Debug, Clone)]
pub struct GranuleMark {
    /// Byte offset into .col data section (past the 64-byte column header).
    pub data_offset: u64,
    /// First row index in this granule.
    pub row_start: u32,
    /// Number of rows in this granule (last may be partial).
    pub row_count: u32,
    /// Minimum value in this granule (up to 16 bytes).
    pub min_value: [u8; 16],
    /// Maximum value in this granule.
    pub max_value: [u8; 16],
}

/// Bloom filter for a single granule.
#[derive(Debug, Clone)]
pub struct GranuleBloom {
    pub bits: Vec<u64>,
}

impl GranuleBloom {
    /// Create a new bloom filter with the given bit count.
    pub fn new(num_bits: usize) -> Self {
        let words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; words],
        }
    }

    /// Insert a value into the bloom filter.
    pub fn insert(&mut self, value: &[u8]) {
        let num_bits = self.bits.len() * 64;
        if num_bits == 0 {
            return;
        }
        for i in 0..BLOOM_NUM_HASHES {
            let h = bloom_hash(value, i) % num_bits as u64;
            let word = (h / 64) as usize;
            let bit = h % 64;
            self.bits[word] |= 1u64 << bit;
        }
    }

    /// Check if a value might be in the filter.
    pub fn may_contain(&self, value: &[u8]) -> bool {
        let num_bits = self.bits.len() * 64;
        if num_bits == 0 {
            return true; // empty filter: assume possible
        }
        for i in 0..BLOOM_NUM_HASHES {
            let h = bloom_hash(value, i) % num_bits as u64;
            let word = (h / 64) as usize;
            let bit = h % 64;
            if self.bits[word] & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }
}

/// Compute marks and bloom filters for a column buffer.
///
/// Called during both ingestion flush and merge. Returns mark entries
/// and bloom filter data ready for writing to disk.
pub fn compute_granules(
    buffer: &ColumnBuffer,
    _encoded_data: &[u8],
    granule_size: usize,
    bloom_bits: usize,
    storage_type: StorageType,
) -> (Vec<GranuleMark>, Vec<GranuleBloom>) {
    let row_count = buffer.row_count();
    if row_count == 0 {
        return (Vec::new(), Vec::new());
    }

    let num_granules = row_count.div_ceil(granule_size);
    let elem_size = storage_type.element_size();

    let mut marks = Vec::with_capacity(num_granules);
    let mut blooms = Vec::with_capacity(num_granules);

    for g in 0..num_granules {
        let row_start = g * granule_size;
        let row_end = ((g + 1) * granule_size).min(row_count);
        let rows_in_granule = row_end - row_start;

        // Compute data offset for this granule within the encoded column data.
        // For plain codec on fixed-size columns: offset = row_start * elem_size.
        // For compressed/transformed codecs the offset is approximate since
        // we can't seek into compressed streams. We store the offset into
        // the *uncompressed* logical layout; the query engine will decompress
        // and skip to the right position.
        let data_offset = if let Some(es) = elem_size {
            (row_start * es) as u64
        } else {
            // Variable-length: need offset from the offset array
            compute_varlen_offset(buffer, row_start) as u64
        };

        // Compute min/max for this granule
        let (min_val, max_val) = compute_granule_min_max(buffer, row_start, row_end);

        marks.push(GranuleMark {
            data_offset,
            row_start: row_start as u32,
            row_count: rows_in_granule as u32,
            min_value: min_val,
            max_value: max_val,
        });

        // Compute bloom filter for this granule
        let mut bloom = GranuleBloom::new(bloom_bits);
        insert_granule_into_bloom(buffer, row_start, row_end, &mut bloom);
        blooms.push(bloom);
    }

    (marks, blooms)
}

/// Write marks to a .mrk file.
pub fn write_marks(path: &Path, marks: &[GranuleMark], granule_size: usize) -> std::io::Result<()> {
    let file = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::with_capacity(32 * 1024, file);

    // Build content into a buffer so we can compute CRC
    let mut content = Vec::with_capacity(MARK_HEADER_SIZE + marks.len() * MARK_ENTRY_SIZE);
    content.extend_from_slice(MARK_MAGIC);
    content.extend_from_slice(&(granule_size as u32).to_le_bytes());
    content.extend_from_slice(&(marks.len() as u32).to_le_bytes());
    content.extend_from_slice(&0u32.to_le_bytes()); // reserved

    for mark in marks {
        content.extend_from_slice(&mark.data_offset.to_le_bytes());
        content.extend_from_slice(&mark.row_start.to_le_bytes());
        content.extend_from_slice(&mark.row_count.to_le_bytes());
        content.extend_from_slice(&mark.min_value);
        content.extend_from_slice(&mark.max_value);
    }

    // Append CRC32-C of entire content
    let crc = crate::crc::crc32c(&content);
    content.extend_from_slice(&crc.to_le_bytes());

    w.write_all(&content)?;

    Ok(())
}

/// Read marks from a .mrk file.
pub fn read_marks(path: &Path) -> std::io::Result<(u32, Vec<GranuleMark>)> {
    let raw = std::fs::read(path)?;
    if raw.len() < MARK_HEADER_SIZE + 4 || &raw[..4] != MARK_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid .mrk file",
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

    let granule_size = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let num_granules = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;

    let mut marks = Vec::with_capacity(num_granules);
    for i in 0..num_granules {
        let off = MARK_HEADER_SIZE + i * MARK_ENTRY_SIZE;
        if off + MARK_ENTRY_SIZE > buf.len() {
            break;
        }
        marks.push(GranuleMark {
            data_offset: u64::from_le_bytes(buf[off..off + 8].try_into().unwrap()),
            row_start: u32::from_le_bytes(buf[off + 8..off + 12].try_into().unwrap()),
            row_count: u32::from_le_bytes(buf[off + 12..off + 16].try_into().unwrap()),
            min_value: buf[off + 16..off + 32].try_into().unwrap(),
            max_value: buf[off + 32..off + 48].try_into().unwrap(),
        });
    }

    Ok((granule_size, marks))
}

/// Write bloom filters to a .bloom file.
pub fn write_blooms(
    path: &Path,
    blooms: &[GranuleBloom],
    bits_per_granule: usize,
) -> std::io::Result<()> {
    let file = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::with_capacity(32 * 1024, file);

    let bytes_per_bloom = bits_per_granule.div_ceil(64) * 8;

    // Build content into buffer for CRC computation
    let mut content = Vec::with_capacity(16 + blooms.len() * bytes_per_bloom);
    content.extend_from_slice(BLOOM_MAGIC);
    content.extend_from_slice(&(blooms.len() as u32).to_le_bytes());
    content.extend_from_slice(&(bits_per_granule as u32).to_le_bytes());
    content.extend_from_slice(&BLOOM_NUM_HASHES.to_le_bytes());

    for bloom in blooms {
        for &word in &bloom.bits {
            content.extend_from_slice(&word.to_le_bytes());
        }
        // Pad if needed
        let written = bloom.bits.len() * 8;
        if written < bytes_per_bloom {
            content.resize(content.len() + bytes_per_bloom - written, 0);
        }
    }

    // Append CRC32-C of entire content
    let crc = crate::crc::crc32c(&content);
    content.extend_from_slice(&crc.to_le_bytes());

    w.write_all(&content)?;

    Ok(())
}

/// Read bloom filters from a .bloom file.
pub fn read_blooms(path: &Path) -> std::io::Result<(u32, Vec<GranuleBloom>)> {
    let raw = std::fs::read(path)?;
    if raw.len() < 20 || &raw[..4] != BLOOM_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid .bloom file",
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

    let num_granules = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
    let bits_per_granule = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    let words_per_bloom = bits_per_granule.div_ceil(64);
    let bytes_per_bloom = words_per_bloom * 8;

    let mut blooms = Vec::with_capacity(num_granules);
    for i in 0..num_granules {
        let off = 16 + i * bytes_per_bloom;
        if off + bytes_per_bloom > buf.len() {
            break;
        }
        let mut bits = Vec::with_capacity(words_per_bloom);
        for w in 0..words_per_bloom {
            let woff = off + w * 8;
            bits.push(u64::from_le_bytes(buf[woff..woff + 8].try_into().unwrap()));
        }
        blooms.push(GranuleBloom { bits });
    }

    Ok((bits_per_granule as u32, blooms))
}

// ---- Internal helpers ----

/// Bloom hash function: FNV-1a with seed mixing for multiple hashes.
fn bloom_hash(value: &[u8], seed: u32) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325 ^ (u64::from(seed).wrapping_mul(0x517c_c1b7_2722_0a95));
    for &b in value {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Compute min/max for a range of rows in a column buffer.
fn compute_granule_min_max(
    buffer: &ColumnBuffer,
    row_start: usize,
    row_end: usize,
) -> ([u8; 16], [u8; 16]) {
    let mut min_val = [0xFFu8; 16];
    let mut max_val = [0u8; 16];

    match buffer {
        ColumnBuffer::U32(v) => {
            let slice = &v[row_start..row_end];
            if let (Some(&mn), Some(&mx)) = (slice.iter().min(), slice.iter().max()) {
                min_val = [0; 16];
                max_val = [0; 16];
                min_val[..4].copy_from_slice(&mn.to_le_bytes());
                max_val[..4].copy_from_slice(&mx.to_le_bytes());
            }
        }
        ColumnBuffer::U64(v) => {
            let slice = &v[row_start..row_end];
            if let (Some(&mn), Some(&mx)) = (slice.iter().min(), slice.iter().max()) {
                min_val = [0; 16];
                max_val = [0; 16];
                min_val[..8].copy_from_slice(&mn.to_le_bytes());
                max_val[..8].copy_from_slice(&mx.to_le_bytes());
            }
        }
        ColumnBuffer::U16(v) => {
            let slice = &v[row_start..row_end];
            if let (Some(&mn), Some(&mx)) = (slice.iter().min(), slice.iter().max()) {
                min_val = [0; 16];
                max_val = [0; 16];
                min_val[..2].copy_from_slice(&mn.to_le_bytes());
                max_val[..2].copy_from_slice(&mx.to_le_bytes());
            }
        }
        ColumnBuffer::U8(v) => {
            let slice = &v[row_start..row_end];
            if let (Some(&mn), Some(&mx)) = (slice.iter().min(), slice.iter().max()) {
                min_val = [0; 16];
                max_val = [0; 16];
                min_val[0] = mn;
                max_val[0] = mx;
            }
        }
        _ => {
            min_val = [0; 16];
            max_val = [0; 16];
        }
    }

    (min_val, max_val)
}

/// Insert all values in a granule row range into a bloom filter.
fn insert_granule_into_bloom(
    buffer: &ColumnBuffer,
    row_start: usize,
    row_end: usize,
    bloom: &mut GranuleBloom,
) {
    match buffer {
        ColumnBuffer::U8(v) => {
            for &val in &v[row_start..row_end] {
                bloom.insert(&[val]);
            }
        }
        ColumnBuffer::U16(v) => {
            for &val in &v[row_start..row_end] {
                bloom.insert(&val.to_le_bytes());
            }
        }
        ColumnBuffer::U32(v) => {
            for &val in &v[row_start..row_end] {
                bloom.insert(&val.to_le_bytes());
            }
        }
        ColumnBuffer::U64(v) => {
            for &val in &v[row_start..row_end] {
                bloom.insert(&val.to_le_bytes());
            }
        }
        ColumnBuffer::U128(v) => {
            for val in &v[row_start..row_end] {
                let mut bytes = [0u8; 16];
                bytes[..8].copy_from_slice(&val[0].to_le_bytes());
                bytes[8..].copy_from_slice(&val[1].to_le_bytes());
                bloom.insert(&bytes);
            }
        }
        ColumnBuffer::Mac(v) => {
            for val in &v[row_start..row_end] {
                bloom.insert(val);
            }
        }
        ColumnBuffer::VarLen { offsets, data } => {
            for i in row_start..row_end {
                if i + 1 < offsets.len() {
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    if end <= data.len() {
                        bloom.insert(&data[start..end]);
                    }
                }
            }
        }
    }
}

/// Get byte offset for a variable-length row.
fn compute_varlen_offset(buffer: &ColumnBuffer, row: usize) -> usize {
    if let ColumnBuffer::VarLen { offsets, .. } = buffer {
        offsets.get(row).copied().unwrap_or(0) as usize
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnBuffer;
    use crate::schema::StorageType;
    use flowcus_ipfix::protocol::FieldValue;
    use std::net::Ipv4Addr;

    #[test]
    fn bloom_filter_basic() {
        let mut bloom = GranuleBloom::new(1024);
        bloom.insert(&42u32.to_le_bytes());
        bloom.insert(&100u32.to_le_bytes());

        assert!(bloom.may_contain(&42u32.to_le_bytes()));
        assert!(bloom.may_contain(&100u32.to_le_bytes()));
        // This may have false positives but shouldn't for such a sparse filter
        assert!(!bloom.may_contain(&999u32.to_le_bytes()));
    }

    #[test]
    fn compute_marks_for_ipv4_column() {
        let mut col = ColumnBuffer::new(StorageType::U32);
        for i in 0..20_000u32 {
            col.push(&FieldValue::Ipv4(Ipv4Addr::from(i)));
        }
        let encoded = col.as_raw_bytes();
        let (marks, blooms) = compute_granules(&col, encoded, 8192, 1024, StorageType::U32);

        // 20000 rows / 8192 granule = 3 granules (8192, 8192, 3616)
        assert_eq!(marks.len(), 3);
        assert_eq!(blooms.len(), 3);
        assert_eq!(marks[0].row_start, 0);
        assert_eq!(marks[0].row_count, 8192);
        assert_eq!(marks[1].row_start, 8192);
        assert_eq!(marks[1].row_count, 8192);
        assert_eq!(marks[2].row_start, 16384);
        assert_eq!(marks[2].row_count, 3616);
        // Data offsets should be at 4-byte boundaries
        assert_eq!(marks[0].data_offset, 0);
        assert_eq!(marks[1].data_offset, 8192 * 4);
    }

    #[test]
    fn bloom_actually_filters() {
        let mut col = ColumnBuffer::new(StorageType::U32);
        // Only even numbers in first granule
        for i in 0..100u32 {
            col.push(&FieldValue::Unsigned32(i * 2));
        }
        let encoded = col.as_raw_bytes();
        let (_, blooms) = compute_granules(&col, encoded, 8192, 8192, StorageType::U32);

        assert_eq!(blooms.len(), 1);
        // Even numbers should be found
        assert!(blooms[0].may_contain(&10u32.to_le_bytes()));
        assert!(blooms[0].may_contain(&50u32.to_le_bytes()));
        // Odd numbers should (mostly) not be found
        let mut false_positives = 0;
        for i in 0..100u32 {
            if blooms[0].may_contain(&(i * 2 + 1).to_le_bytes()) {
                false_positives += 1;
            }
        }
        // With 8192 bits and 100 values, FPR should be very low
        assert!(
            false_positives < 5,
            "too many false positives: {false_positives}"
        );
    }

    #[test]
    fn mark_file_roundtrip() {
        let marks = vec![
            GranuleMark {
                data_offset: 0,
                row_start: 0,
                row_count: 8192,
                min_value: [0; 16],
                max_value: [0xFF; 16],
            },
            GranuleMark {
                data_offset: 32768,
                row_start: 8192,
                row_count: 4000,
                min_value: [1; 16],
                max_value: [0xFE; 16],
            },
        ];

        let dir = std::env::temp_dir().join("flowcus_test_marks");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("test.mrk");
        write_marks(&path, &marks, 8192).unwrap();

        let (gs, read_marks) = read_marks(&path).unwrap();
        assert_eq!(gs, 8192);
        assert_eq!(read_marks.len(), 2);
        assert_eq!(read_marks[0].data_offset, 0);
        assert_eq!(read_marks[0].row_count, 8192);
        assert_eq!(read_marks[1].data_offset, 32768);
        assert_eq!(read_marks[1].row_start, 8192);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn bloom_file_roundtrip() {
        let mut b1 = GranuleBloom::new(512);
        b1.insert(&42u32.to_le_bytes());
        let mut b2 = GranuleBloom::new(512);
        b2.insert(&99u32.to_le_bytes());

        let dir = std::env::temp_dir().join("flowcus_test_blooms");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("test.bloom");
        write_blooms(&path, &[b1, b2], 512).unwrap();

        let (bits, read_blooms) = read_blooms(&path).unwrap();
        assert_eq!(bits, 512);
        assert_eq!(read_blooms.len(), 2);
        assert!(read_blooms[0].may_contain(&42u32.to_le_bytes()));
        assert!(read_blooms[1].may_contain(&99u32.to_le_bytes()));
        assert!(!read_blooms[0].may_contain(&99u32.to_le_bytes()));

        std::fs::remove_dir_all(&dir).ok();
    }
}
