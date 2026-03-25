//! Data integrity tests for the ingestion and merge pipelines.
//!
//! These integration tests verify that data written through the storage engine
//! survives flush, has valid binary structure, and maintains consistency across
//! all metadata and column files. Designed to catch regressions in data loss
//! scenarios.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::case_sensitive_file_extension_comparisons
)]

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use flowcus_ipfix::protocol::*;
use flowcus_query::ast::{
    AggExpr, Query, SelectExpr, SelectField, SelectFieldExpr, Stage, TimeRange,
};
use flowcus_storage::cache::StorageCache;
use flowcus_storage::executor::QueryExecutor;
use flowcus_storage::granule;
use flowcus_storage::part;
use flowcus_storage::schema::StorageType;
use flowcus_storage::table::Table;
use flowcus_storage::writer::{StorageWriter, WriterConfig};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a unique temp directory for each test to ensure isolation.
fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("flowcus_integrity_tests")
        .join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Clean up a test directory (best-effort).
fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn make_record(src: Ipv4Addr, dst: Ipv4Addr, bytes: u64) -> DataRecord {
    DataRecord {
        fields: vec![
            DataField {
                spec: FieldSpecifier {
                    element_id: 8,
                    field_length: 4,
                    enterprise_id: 0,
                },
                name: "sourceIPv4Address".into(),
                value: FieldValue::Ipv4(src),
            },
            DataField {
                spec: FieldSpecifier {
                    element_id: 12,
                    field_length: 4,
                    enterprise_id: 0,
                },
                name: "destinationIPv4Address".into(),
                value: FieldValue::Ipv4(dst),
            },
            DataField {
                spec: FieldSpecifier {
                    element_id: 1,
                    field_length: 8,
                    enterprise_id: 0,
                },
                name: "octetDeltaCount".into(),
                value: FieldValue::Unsigned64(bytes),
            },
        ],
    }
}

fn make_message(records: Vec<DataRecord>) -> IpfixMessage {
    make_message_with_time(records, 1_700_000_000)
}

fn make_message_with_time(records: Vec<DataRecord>, export_time: u32) -> IpfixMessage {
    IpfixMessage {
        header: MessageHeader {
            version: 0x000a,
            length: 0,
            export_time,
            sequence_number: 1,
            observation_domain_id: 1,
        },
        exporter: "10.0.0.1:4739".parse().unwrap(),
        sets: vec![Set {
            set_id: 256,
            contents: SetContents::Data(DataSet {
                template_id: 256,
                records,
            }),
        }],
    }
}

/// Create a writer that flushes on every ingest (1-byte threshold).
fn tiny_flush_writer(dir: &Path) -> StorageWriter {
    let table = Table::open(dir, "flows").unwrap();
    let config = WriterConfig {
        flush_bytes: 1,
        flush_interval_secs: 0,
        initial_row_capacity: 64,
        ..WriterConfig::default()
    };
    StorageWriter::new(table, config, None)
}

/// Find the first (and usually only) part directory under the table.
fn find_first_part(dir: &Path) -> PathBuf {
    let table = Table::open(dir, "flows").unwrap();
    let parts = table.list_all_parts().unwrap();
    assert!(!parts.is_empty(), "expected at least one part on disk");
    parts[0].path.clone()
}

/// Ingest records and force-flush, returning the part directory path.
fn ingest_and_flush(dir: &Path, records: Vec<DataRecord>) -> PathBuf {
    let mut writer = tiny_flush_writer(dir);
    let msg = make_message(records);
    writer.ingest(&msg);
    writer.flush_all();
    find_first_part(dir)
}

// ---------------------------------------------------------------------------
// 1. test_ingested_data_survives_flush
// ---------------------------------------------------------------------------

#[test]
fn test_ingested_data_survives_flush() {
    let dir = test_dir("survives_flush");

    let records = vec![
        make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            1500,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            2500,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 3),
            Ipv4Addr::new(192, 168, 1, 3),
            3500,
        ),
    ];

    let mut writer = tiny_flush_writer(&dir);
    let msg = make_message(records);
    let ingested = writer.ingest(&msg);
    assert_eq!(ingested, 3);
    let (flushed, _) = writer.flush_ready();
    assert_eq!(flushed, 1);

    let part_dir = find_first_part(&dir);

    // Part directory exists
    assert!(part_dir.exists(), "part directory must exist after flush");

    // meta.bin row_count matches
    let meta = part::read_meta_bin(&part_dir.join("meta.bin")).unwrap();
    assert_eq!(
        meta.row_count, 3,
        "meta.bin row_count must match ingested records"
    );
    // 4 system columns + 3 template columns = 7
    assert_eq!(
        meta.column_count, 7,
        "meta.bin column_count must match schema (4 system + 3 template)"
    );

    // Each .col file header row_count matches
    let col_dir = part_dir.join("columns");
    let expected_columns = [
        "flowcusExporterIPv4",
        "flowcusExporterPort",
        "flowcusExportTime",
        "flowcusObservationDomainId",
        "sourceIPv4Address",
        "destinationIPv4Address",
        "octetDeltaCount",
    ];
    for col_name in &expected_columns {
        let col_path = col_dir.join(format!("{col_name}.col"));
        assert!(col_path.exists(), "column file {col_name}.col must exist");
        let header = part::read_column_header(&col_path).unwrap();
        assert_eq!(
            header.row_count, 3,
            "column {col_name} row_count must match ingested records"
        );
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 2. test_part_files_have_valid_structure
// ---------------------------------------------------------------------------

#[test]
fn test_part_files_have_valid_structure() {
    let dir = test_dir("valid_structure");

    let records = vec![
        make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            100,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            200,
        ),
    ];
    let part_dir = ingest_and_flush(&dir, records);

    // meta.bin starts with "FMTA"
    let meta_bytes = std::fs::read(part_dir.join("meta.bin")).unwrap();
    assert_eq!(
        &meta_bytes[..4],
        b"FMTA",
        "meta.bin must start with FMTA magic"
    );
    assert_eq!(meta_bytes.len(), 256, "meta.bin must be exactly 256 bytes");

    // column_index.bin starts with "FCIX"
    let cix_bytes = std::fs::read(part_dir.join("column_index.bin")).unwrap();
    assert_eq!(
        &cix_bytes[..4],
        b"FCIX",
        "column_index.bin must start with FCIX magic"
    );

    // schema.bin starts with "FSCH"
    let schema_bytes = std::fs::read(part_dir.join("schema.bin")).unwrap();
    assert_eq!(
        &schema_bytes[..4],
        b"FSCH",
        "schema.bin must start with FSCH magic"
    );

    // Each .col file starts with "FCOL"
    let col_dir = part_dir.join("columns");
    for entry in std::fs::read_dir(&col_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".col") {
            let bytes = std::fs::read(entry.path()).unwrap();
            assert!(bytes.len() >= 64, "{name} must be at least 64 bytes");
            assert_eq!(&bytes[..4], b"FCOL", "{name} must start with FCOL magic");
        }
    }

    // Each .mrk file starts with "FMRK"
    for entry in std::fs::read_dir(&col_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".mrk") {
            let bytes = std::fs::read(entry.path()).unwrap();
            assert!(bytes.len() >= 16, "{name} must be at least 16 bytes");
            assert_eq!(&bytes[..4], b"FMRK", "{name} must start with FMRK magic");
        }
    }

    // Each .bloom file starts with "FBLM"
    for entry in std::fs::read_dir(&col_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".bloom") {
            let bytes = std::fs::read(entry.path()).unwrap();
            assert!(bytes.len() >= 16, "{name} must be at least 16 bytes");
            assert_eq!(&bytes[..4], b"FBLM", "{name} must start with FBLM magic");
        }
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 3. test_merge_preserves_row_count (structural: multiple parts, total rows)
// ---------------------------------------------------------------------------

#[test]
fn test_merge_preserves_row_count() {
    let dir = test_dir("merge_row_count");

    // Write 3 separate parts with known row counts by using 3 different export times
    // in the same hour so they land in the same hour directory.
    let counts: [usize; 3] = [2, 3, 5];
    let base_time: u32 = 1_700_000_000;

    for (i, &count) in counts.iter().enumerate() {
        let mut writer = tiny_flush_writer(&dir);
        let records: Vec<DataRecord> = (0..count)
            .map(|j| {
                make_record(
                    Ipv4Addr::new(10, 0, i as u8, j as u8),
                    Ipv4Addr::new(192, 168, i as u8, j as u8),
                    (i * 1000 + j) as u64,
                )
            })
            .collect();
        // Use slightly different export times within the same hour partition
        let msg = make_message_with_time(records, base_time + i as u32);
        writer.ingest(&msg);
        writer.flush_all();
    }

    let table = Table::open(&dir, "flows").unwrap();
    let parts = table.list_all_parts().unwrap();
    assert_eq!(parts.len(), 3, "should have 3 parts on disk");

    let total_rows: u64 = parts
        .iter()
        .map(|p| {
            let meta = part::read_meta_bin(&p.path.join("meta.bin")).unwrap();
            meta.row_count
        })
        .sum();

    let expected: u64 = counts.iter().map(|c| *c as u64).sum();
    assert_eq!(
        total_rows, expected,
        "total row count across all parts must equal sum of ingested records"
    );

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 4. test_column_file_not_truncated
// ---------------------------------------------------------------------------

#[test]
fn test_column_file_not_truncated() {
    let dir = test_dir("not_truncated");

    let records = vec![
        make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            1500,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            2500,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 3),
            Ipv4Addr::new(192, 168, 1, 3),
            3500,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 4),
            Ipv4Addr::new(192, 168, 1, 4),
            4500,
        ),
    ];
    let part_dir = ingest_and_flush(&dir, records);

    let col_dir = part_dir.join("columns");
    for entry in std::fs::read_dir(&col_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".col") {
            continue;
        }

        let header = part::read_column_header(&entry.path()).unwrap();
        let file_len = std::fs::metadata(entry.path()).unwrap().len();
        // File = 64-byte header + encoded data + 4-byte CRC32 trailing checksum
        let expected_len = 64 + header.encoded_size + 4;

        assert_eq!(
            file_len, expected_len,
            "column file {name}: actual size ({file_len}) must equal header (64) + encoded_size ({}) + CRC (4)",
            header.encoded_size
        );
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 5. test_meta_row_count_matches_column_row_count
// ---------------------------------------------------------------------------

#[test]
fn test_meta_row_count_matches_column_row_count() {
    let dir = test_dir("meta_col_match");

    let records = vec![
        make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            100,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            200,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 3),
            Ipv4Addr::new(192, 168, 1, 3),
            300,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 4),
            Ipv4Addr::new(192, 168, 1, 4),
            400,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 5),
            Ipv4Addr::new(192, 168, 1, 5),
            500,
        ),
    ];
    let part_dir = ingest_and_flush(&dir, records);

    let meta = part::read_meta_bin(&part_dir.join("meta.bin")).unwrap();
    let meta_rows = meta.row_count;

    let col_dir = part_dir.join("columns");
    for entry in std::fs::read_dir(&col_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".col") {
            continue;
        }

        let header = part::read_column_header(&entry.path()).unwrap();
        assert_eq!(
            header.row_count, meta_rows,
            "column {name} row_count ({}) must match meta.bin row_count ({meta_rows})",
            header.row_count
        );
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 6. test_schema_bin_roundtrip
// ---------------------------------------------------------------------------

#[test]
fn test_schema_bin_roundtrip() {
    let dir = test_dir("schema_roundtrip");

    // Write a part so schema.bin is produced
    let records = vec![make_record(
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 168, 1, 1),
        100,
    )];
    let part_dir = ingest_and_flush(&dir, records);

    let schema = part::read_schema_bin(&part_dir.join("schema.bin")).unwrap();

    // 4 system columns + 3 template columns = 7
    assert_eq!(schema.columns.len(), 7);

    // System columns come first
    assert_eq!(schema.columns[0].name, "flowcusExporterIPv4");
    assert_eq!(schema.columns[1].name, "flowcusExporterPort");
    assert_eq!(schema.columns[2].name, "flowcusExportTime");
    assert_eq!(schema.columns[3].name, "flowcusObservationDomainId");

    // Template columns follow
    let src_col = &schema.columns[4];
    assert_eq!(src_col.name, "sourceIPv4Address");
    assert_eq!(src_col.element_id, 8);
    assert_eq!(src_col.enterprise_id, 0);
    assert_eq!(src_col.data_type, DataType::Ipv4Address);
    assert_eq!(src_col.storage_type, StorageType::U32);

    let dst_col = &schema.columns[5];
    assert_eq!(dst_col.name, "destinationIPv4Address");
    assert_eq!(dst_col.element_id, 12);

    let bytes_col = &schema.columns[6];
    assert_eq!(bytes_col.name, "octetDeltaCount");
    assert_eq!(bytes_col.data_type, DataType::Unsigned64);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 7. test_column_index_matches_actual_columns
// ---------------------------------------------------------------------------

#[test]
fn test_column_index_matches_actual_columns() {
    let dir = test_dir("index_matches_columns");

    let records = vec![
        make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            100,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            200,
        ),
    ];
    let part_dir = ingest_and_flush(&dir, records);

    // Read the column index
    let index_entries = part::read_column_index(&part_dir.join("column_index.bin")).unwrap();

    // List actual .col files on disk
    let col_names = part::list_columns(&part_dir).unwrap();

    assert_eq!(
        index_entries.len(),
        col_names.len(),
        "column_index.bin entry count ({}) must match number of .col files ({})",
        index_entries.len(),
        col_names.len()
    );

    // Verify each index entry has a matching .col file by name_hash
    let col_dir = part_dir.join("columns");
    for col_name in &col_names {
        let col_path = col_dir.join(format!("{col_name}.col"));
        assert!(
            col_path.exists(),
            "column file {col_name}.col must exist on disk"
        );

        // Verify the name_hash for this column appears in the index
        let expected_hash = part::column_name_hash(col_name);
        let found = index_entries.iter().any(|e| e.name_hash == expected_hash);
        assert!(
            found,
            "column_index.bin must contain an entry with name_hash for {col_name}"
        );
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 8. test_marks_granule_coverage
// ---------------------------------------------------------------------------

#[test]
fn test_marks_granule_coverage() {
    let dir = test_dir("marks_granule_coverage");

    // Generate enough records to span multiple granules (default granule_size = 8192).
    // We need > 8192 rows. Let's do 20000.
    let total_rows = 20_000usize;
    let records: Vec<DataRecord> = (0..total_rows)
        .map(|i| {
            let b3 = ((i >> 8) & 0xFF) as u8;
            let b4 = (i & 0xFF) as u8;
            make_record(
                Ipv4Addr::new(10, 0, b3, b4),
                Ipv4Addr::new(192, 168, b3, b4),
                i as u64,
            )
        })
        .collect();

    let part_dir = ingest_and_flush(&dir, records);

    // Pick any column's .mrk file
    let mrk_path = part_dir.join("columns").join("sourceIPv4Address.mrk");
    assert!(mrk_path.exists(), ".mrk file must exist");

    let (granule_size, marks) = granule::read_marks(&mrk_path).unwrap();
    assert_eq!(
        granule_size, 8192,
        "granule_size must be the ingestion default"
    );

    // Expected: ceil(20000 / 8192) = 3 granules
    let expected_granules = total_rows.div_ceil(8192);
    assert_eq!(
        marks.len(),
        expected_granules,
        "number of granule marks must equal ceil(total_rows / granule_size)"
    );

    // Verify marks cover all rows
    let mark_total_rows: u64 = marks.iter().map(|m| m.row_count as u64).sum();
    assert_eq!(
        mark_total_rows, total_rows as u64,
        "sum of mark row_counts must equal total rows"
    );

    // Verify data_offsets are monotonically increasing
    for i in 1..marks.len() {
        assert!(
            marks[i].data_offset >= marks[i - 1].data_offset,
            "data_offsets must be monotonically increasing (mark[{}]={} < mark[{}]={})",
            i - 1,
            marks[i - 1].data_offset,
            i,
            marks[i].data_offset
        );
    }

    // Verify no gaps: mark[i+1].row_start == mark[i].row_start + mark[i].row_count
    for i in 0..marks.len() - 1 {
        let expected_next_start = marks[i].row_start + marks[i].row_count;
        assert_eq!(
            marks[i + 1].row_start,
            expected_next_start,
            "granule marks must be contiguous: mark[{}].row_start ({}) != mark[{}].row_start + mark[{}].row_count ({})",
            i + 1,
            marks[i + 1].row_start,
            i,
            i,
            expected_next_start
        );
    }

    // First mark starts at row 0
    assert_eq!(marks[0].row_start, 0, "first granule must start at row 0");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 9. test_cache_marks_hit_returns_same_data
// ---------------------------------------------------------------------------

#[test]
fn test_cache_marks_hit_returns_same_data() {
    let dir = test_dir("cache_marks_hit");
    let records = vec![
        make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            1500,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            2500,
        ),
    ];
    let part_dir = ingest_and_flush(&dir, records);
    let cache = StorageCache::default();
    let mrk_path = part_dir.join("columns").join("sourceIPv4Address.mrk");

    let (gs_miss, marks_miss, was_cached) = cache.get_marks(&mrk_path).unwrap();
    assert!(!was_cached, "first read must be a miss");

    let (gs_hit, marks_hit, was_cached) = cache.get_marks(&mrk_path).unwrap();
    assert!(was_cached, "second read must be a hit");

    assert_eq!(gs_miss, gs_hit, "granule_size must match");
    assert_eq!(marks_miss.len(), marks_hit.len(), "mark count must match");
    for (a, b) in marks_miss.iter().zip(marks_hit.iter()) {
        assert_eq!(a.row_start, b.row_start);
        assert_eq!(a.row_count, b.row_count);
        assert_eq!(a.min_value, b.min_value);
        assert_eq!(a.max_value, b.max_value);
    }

    let (hits, misses) = cache.stats();
    assert_eq!(hits, 1);
    assert_eq!(misses, 1);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 10. test_cache_bloom_hit_returns_same_data
// ---------------------------------------------------------------------------

#[test]
fn test_cache_bloom_hit_returns_same_data() {
    let dir = test_dir("cache_bloom_hit");
    let records = vec![
        make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            100,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            200,
        ),
    ];
    let part_dir = ingest_and_flush(&dir, records);
    let cache = StorageCache::default();
    let bloom_path = part_dir.join("columns").join("sourceIPv4Address.bloom");

    let (bits_miss, blooms_miss, was_cached) = cache.get_blooms(&bloom_path).unwrap();
    assert!(!was_cached, "first read must be a miss");

    let (bits_hit, blooms_hit, was_cached) = cache.get_blooms(&bloom_path).unwrap();
    assert!(was_cached, "second read must be a hit");

    assert_eq!(bits_miss, bits_hit);
    assert_eq!(blooms_miss.len(), blooms_hit.len());
    for (a, b) in blooms_miss.iter().zip(blooms_hit.iter()) {
        assert_eq!(a.bits, b.bits);
    }

    let (hits, misses) = cache.stats();
    assert_eq!(hits, 1);
    assert_eq!(misses, 1);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 11. test_cache_meta_hit_returns_same_data
// ---------------------------------------------------------------------------

#[test]
fn test_cache_meta_hit_returns_same_data() {
    let dir = test_dir("cache_meta_hit");
    let records = vec![make_record(
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 168, 1, 1),
        100,
    )];
    let part_dir = ingest_and_flush(&dir, records);
    let cache = StorageCache::default();
    let meta_path = part_dir.join("meta.bin");

    let (meta_miss, was_cached) = cache.get_meta(&meta_path).unwrap();
    assert!(!was_cached, "first meta read must be a miss");

    let (meta_hit, was_cached) = cache.get_meta(&meta_path).unwrap();
    assert!(was_cached, "second meta read must be a hit");

    assert_eq!(meta_miss.row_count, meta_hit.row_count);
    assert_eq!(meta_miss.column_count, meta_hit.column_count);
    assert_eq!(meta_miss.time_min, meta_hit.time_min);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 12. test_cache_invalidate_after_part_delete
// ---------------------------------------------------------------------------

#[test]
fn test_cache_invalidate_after_part_delete() {
    let dir = test_dir("cache_invalidate_delete");
    let records = vec![make_record(
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 168, 1, 1),
        1000,
    )];
    let part_dir = ingest_and_flush(&dir, records);
    let cache = StorageCache::default();

    let bloom_path = part_dir.join("columns").join("sourceIPv4Address.bloom");
    let _ = cache.get_blooms(&bloom_path).unwrap();
    let (_, _, was_cached) = cache.get_blooms(&bloom_path).unwrap();
    assert!(was_cached, "must be cached");

    std::fs::remove_dir_all(&part_dir).unwrap();
    cache.invalidate_part(&part_dir);

    let result = cache.get_blooms(&bloom_path);
    assert!(result.is_err(), "must fail after delete + invalidation");
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 13. test_cache_invalidate_part_clears_all_types
// ---------------------------------------------------------------------------

#[test]
fn test_cache_invalidate_part_clears_all_types() {
    let dir = test_dir("cache_invalidate_all_types");
    let records = vec![make_record(
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 168, 1, 1),
        500,
    )];
    let part_dir = ingest_and_flush(&dir, records);
    let cache = StorageCache::default();

    let bloom_path = part_dir.join("columns").join("sourceIPv4Address.bloom");
    let mrk_path = part_dir.join("columns").join("sourceIPv4Address.mrk");
    let meta_path = part_dir.join("meta.bin");

    let _ = cache.get_blooms(&bloom_path).unwrap();
    let _ = cache.get_marks(&mrk_path).unwrap();
    let _ = cache.get_meta(&meta_path).unwrap();

    let (hits_before, _) = cache.stats();
    cache.invalidate_part(&part_dir);

    // All re-reads must be misses
    let (_, _, c1) = cache.get_blooms(&bloom_path).unwrap();
    let (_, _, c2) = cache.get_marks(&mrk_path).unwrap();
    let (_, c3) = cache.get_meta(&meta_path).unwrap();
    assert!(
        !c1 && !c2 && !c3,
        "all reads after invalidation must be misses"
    );

    let (hits_after, _) = cache.stats();
    assert_eq!(
        hits_after, hits_before,
        "invalidation must not produce hits"
    );
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 14. test_cache_partitions_are_independent
// ---------------------------------------------------------------------------

#[test]
fn test_cache_partitions_are_independent() {
    let dir = test_dir("cache_partitions_independent");
    let records = vec![make_record(
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 168, 1, 1),
        100,
    )];
    let part_dir = ingest_and_flush(&dir, records);

    // Tiny cache: marks/blooms each get ~50 bytes. Should evict within their pool
    // but not affect each other.
    let cache = StorageCache::new(200);
    let bloom_path = part_dir.join("columns").join("sourceIPv4Address.bloom");
    let mrk_path = part_dir.join("columns").join("sourceIPv4Address.mrk");
    let meta_path = part_dir.join("meta.bin");

    // All reads must succeed despite tiny budget — each type has its own pool
    assert!(cache.get_blooms(&bloom_path).is_ok());
    assert!(cache.get_marks(&mrk_path).is_ok());
    assert!(cache.get_meta(&meta_path).is_ok());
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 15. test_cache_survives_concurrent_reads
// ---------------------------------------------------------------------------

#[test]
fn test_cache_survives_concurrent_reads() {
    let dir = test_dir("cache_concurrent_reads");
    let records = vec![
        make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            1500,
        ),
        make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            2500,
        ),
    ];
    let part_dir = ingest_and_flush(&dir, records);
    let cache = StorageCache::default();
    let mrk_path = part_dir.join("columns").join("sourceIPv4Address.mrk");

    std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let cache_ref = &cache;
                let path_ref = &mrk_path;
                s.spawn(move || cache_ref.get_marks(path_ref).unwrap())
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let ref_len = results[0].1.len();
        for (i, (_, marks, _)) in results.iter().enumerate() {
            assert_eq!(
                marks.len(),
                ref_len,
                "thread {i} returned different mark count"
            );
        }
    });

    let (hits, misses) = cache.stats();
    assert_eq!(hits + misses, 8);
    assert!(misses >= 1);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Query consistency helpers
// ---------------------------------------------------------------------------

fn make_executor(dir: &Path) -> QueryExecutor {
    QueryExecutor::new(dir, 8192)
}

/// Create a record with only sourceIPv4Address and octetDeltaCount (no destination).
fn make_record_no_dst(src: Ipv4Addr, bytes: u64) -> DataRecord {
    DataRecord {
        fields: vec![
            DataField {
                spec: FieldSpecifier {
                    element_id: 8,
                    field_length: 4,
                    enterprise_id: 0,
                },
                name: "sourceIPv4Address".into(),
                value: FieldValue::Ipv4(src),
            },
            DataField {
                spec: FieldSpecifier {
                    element_id: 1,
                    field_length: 8,
                    enterprise_id: 0,
                },
                name: "octetDeltaCount".into(),
                value: FieldValue::Unsigned64(bytes),
            },
        ],
    }
}

/// Build a query covering a wide absolute time range with the given stages.
fn make_wide_query(stages: Vec<Stage>) -> Query {
    Query {
        time_range: TimeRange::Absolute {
            start: "2023-01-01".into(),
            end: "2025-01-01".into(),
        },
        stages,
    }
}

// ---------------------------------------------------------------------------
// 16. test_query_column_mapping_across_heterogeneous_parts
// ---------------------------------------------------------------------------

#[test]
fn test_query_column_mapping_across_heterogeneous_parts() {
    let dir = test_dir("query_heterogeneous_columns");

    // Part A: sourceIPv4Address + octetDeltaCount (no destination), export_time T1
    let mut writer = tiny_flush_writer(&dir);
    let msg_a = make_message_with_time(
        vec![make_record_no_dst(Ipv4Addr::new(10, 0, 0, 1), 1000)],
        1_700_000_000,
    );
    writer.ingest(&msg_a);
    writer.flush_all();

    // Part B: sourceIPv4Address + destinationIPv4Address + octetDeltaCount, export_time T2
    let msg_b = make_message_with_time(
        vec![make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            2000,
        )],
        1_700_000_100,
    );
    writer.ingest(&msg_b);
    writer.flush_all();

    let executor = make_executor(&dir);
    let query = make_wide_query(vec![]);
    let result = executor.execute(&query, 0, 100, None, None).unwrap();

    // Both parts should be returned
    assert_eq!(result.rows.len(), 2, "expected 2 rows from 2 parts");

    // Find column indices in result
    let src_idx = result
        .columns
        .iter()
        .position(|c| c == "sourceIPv4Address")
        .expect("sourceIPv4Address must be in result columns");
    let dst_idx = result
        .columns
        .iter()
        .position(|c| c == "destinationIPv4Address");
    let bytes_idx = result
        .columns
        .iter()
        .position(|c| c == "octetDeltaCount")
        .expect("octetDeltaCount must be in result columns");

    // Rows are sorted newest-first, so Part B (T2) comes first
    let row_b = &result.rows[0]; // newer part (has destination)
    let row_a = &result.rows[1]; // older part (no destination)

    // Verify Part B row has correct values
    assert_eq!(
        row_b[src_idx].as_str().unwrap(),
        "10.0.0.2",
        "Part B source IP must be correct"
    );
    assert_eq!(
        row_b[bytes_idx].as_u64().unwrap(),
        2000,
        "Part B bytes must be correct"
    );
    if let Some(di) = dst_idx {
        assert_eq!(
            row_b[di].as_str().unwrap(),
            "192.168.1.2",
            "Part B destination IP must be correct"
        );
    }

    // Verify Part A row has correct values and null for missing destination
    assert_eq!(
        row_a[src_idx].as_str().unwrap(),
        "10.0.0.1",
        "Part A source IP must be correct"
    );
    assert_eq!(
        row_a[bytes_idx].as_u64().unwrap(),
        1000,
        "Part A bytes must be correct"
    );
    if let Some(di) = dst_idx {
        assert!(
            row_a[di].is_null(),
            "Part A destination must be null (column not in part)"
        );
    }

    // Critical check: bytes column must NEVER contain an IP address value.
    // This is the exact bug scenario — column misalignment would cause this.
    for (i, row) in result.rows.iter().enumerate() {
        let bytes_val = &row[bytes_idx];
        assert!(
            bytes_val.is_u64() || bytes_val.is_number(),
            "row {i}: octetDeltaCount must be numeric, got {bytes_val}"
        );
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 17. test_query_column_mapping_with_explicit_select
// ---------------------------------------------------------------------------

#[test]
fn test_query_column_mapping_with_explicit_select() {
    let dir = test_dir("query_explicit_select_columns");

    // Part A: no destination column
    let mut writer = tiny_flush_writer(&dir);
    let msg_a = make_message_with_time(
        vec![make_record_no_dst(Ipv4Addr::new(10, 0, 0, 1), 500)],
        1_700_000_000,
    );
    writer.ingest(&msg_a);
    writer.flush_all();

    // Part B: has destination column
    let msg_b = make_message_with_time(
        vec![make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            600,
        )],
        1_700_000_100,
    );
    writer.ingest(&msg_b);
    writer.flush_all();

    let executor = make_executor(&dir);
    let query = make_wide_query(vec![Stage::Select(SelectExpr::Fields(vec![
        SelectField {
            expr: SelectFieldExpr::Field("sourceIPv4Address".into()),
            alias: None,
        },
        SelectField {
            expr: SelectFieldExpr::Field("destinationIPv4Address".into()),
            alias: None,
        },
    ]))]);

    let result = executor.execute(&query, 0, 100, None, None).unwrap();

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0], "sourceIPv4Address");
    assert_eq!(result.columns[1], "destinationIPv4Address");

    // Row from Part B (newer): both columns populated
    let row_b = &result.rows[0];
    assert_eq!(row_b[0].as_str().unwrap(), "10.0.0.2");
    assert_eq!(row_b[1].as_str().unwrap(), "192.168.1.2");

    // Row from Part A (older): destination is null
    let row_a = &result.rows[1];
    assert_eq!(row_a[0].as_str().unwrap(), "10.0.0.1");
    assert!(row_a[1].is_null(), "missing column must be null");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 18. test_query_early_termination_within_part
// ---------------------------------------------------------------------------

#[test]
fn test_query_early_termination_within_part() {
    let dir = test_dir("query_early_termination");

    let mut writer = tiny_flush_writer(&dir);
    // Ingest many records in a single message so they land in one part
    let records: Vec<DataRecord> = (0..50)
        .map(|i| {
            make_record(
                Ipv4Addr::new(10, 0, 0, (i % 254 + 1) as u8),
                Ipv4Addr::new(192, 168, 1, (i % 254 + 1) as u8),
                (i + 1) * 100,
            )
        })
        .collect();
    let msg = make_message_with_time(records, 1_700_000_000);
    writer.ingest(&msg);
    writer.flush_all();

    let executor = make_executor(&dir);
    let query = make_wide_query(vec![Stage::Aggregate(AggExpr::Limit(5))]);

    let result = executor.execute(&query, 0, 5, None, None).unwrap();
    assert_eq!(result.rows.len(), 5, "LIMIT 5 must return exactly 5 rows");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 19. test_query_parts_read_newest_first
// ---------------------------------------------------------------------------

#[test]
fn test_query_parts_read_newest_first() {
    let dir = test_dir("query_newest_first");

    let mut writer = tiny_flush_writer(&dir);

    // Older part: export_time = T1, bytes = 1111
    let msg_old = make_message_with_time(
        vec![make_record(
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            1111,
        )],
        1_700_000_000,
    );
    writer.ingest(&msg_old);
    writer.flush_all();

    // Newer part: export_time = T2, bytes = 2222
    let msg_new = make_message_with_time(
        vec![make_record(
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::new(192, 168, 1, 2),
            2222,
        )],
        1_700_000_100,
    );
    writer.ingest(&msg_new);
    writer.flush_all();

    let executor = make_executor(&dir);
    // Request only 1 row — should come from the newer part
    let query = make_wide_query(vec![Stage::Aggregate(AggExpr::Limit(1))]);
    let result = executor.execute(&query, 0, 1, None, None).unwrap();

    assert_eq!(result.rows.len(), 1);
    let bytes_idx = result
        .columns
        .iter()
        .position(|c| c == "octetDeltaCount")
        .expect("octetDeltaCount column");
    assert_eq!(
        result.rows[0][bytes_idx].as_u64().unwrap(),
        2222,
        "LIMIT 1 with newest-first must return the newer row"
    );

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 20. test_query_pagination_total_matching_not_capped_by_limit
// ---------------------------------------------------------------------------

#[test]
fn test_query_pagination_total_matching_not_capped_by_limit() {
    let dir = test_dir("query_pagination_total");

    let mut writer = tiny_flush_writer(&dir);
    // Ingest 30 records so total_matching > any reasonable page size
    let records: Vec<DataRecord> = (0..30)
        .map(|i| {
            make_record(
                Ipv4Addr::new(10, 0, 0, (i % 254 + 1) as u8),
                Ipv4Addr::new(192, 168, 1, (i % 254 + 1) as u8),
                (i + 1) * 100,
            )
        })
        .collect();
    let msg = make_message_with_time(records, 1_700_000_000);
    writer.ingest(&msg);
    writer.flush_all();

    let executor = make_executor(&dir);
    let query = make_wide_query(vec![]);

    // Page 1: offset=0, limit=5
    let page1 = executor.execute(&query, 0, 5, None, None).unwrap();
    assert_eq!(page1.rows.len(), 5, "page 1 must return 5 rows");
    assert!(
        page1.total_matching_rows >= 30,
        "total_matching_rows ({}) must reflect all matches, not be capped by limit",
        page1.total_matching_rows
    );

    // Page 2: offset=5, limit=5 (simulating infinite scroll)
    let page2 = executor
        .execute(&query, 5, 5, Some((page1.time_start, page1.time_end)), None)
        .unwrap();
    assert_eq!(page2.rows.len(), 5, "page 2 must return 5 rows");
    assert!(
        page2.total_matching_rows >= 30,
        "total_matching_rows ({}) must still reflect all matches on page 2",
        page2.total_matching_rows
    );

    // Verify page 2 rows are different from page 1
    let bytes_idx = page1
        .columns
        .iter()
        .position(|c| c == "octetDeltaCount")
        .expect("octetDeltaCount column");
    let page1_bytes: Vec<u64> = page1
        .rows
        .iter()
        .map(|r| r[bytes_idx].as_u64().unwrap())
        .collect();
    let page2_bytes: Vec<u64> = page2
        .rows
        .iter()
        .map(|r| r[bytes_idx].as_u64().unwrap())
        .collect();
    assert_ne!(
        page1_bytes, page2_bytes,
        "page 2 must have different rows than page 1"
    );

    cleanup(&dir);
}
