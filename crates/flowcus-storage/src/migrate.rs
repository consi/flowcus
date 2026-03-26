//! Part format migration: v1 → v2.
//!
//! Runs on startup to detect and migrate old-format parts to the current format.
//! Migration is non-blocking — runs in a background thread, parts remain queryable
//! during migration (the executor handles both old and new formats).
//!
//! # Crash safety
//!
//! Migration is a two-phase process to protect against crashes:
//!
//! 1. **Migrate phase**: For each v1 part, write a new v2 part alongside it, then
//!    rename the v1 part to `{name}.broken`. Both the v2 part and `.broken` marker
//!    coexist until all parts are migrated. If the process crashes here, both the
//!    original data (in `.broken`) and the new v2 part exist on disk.
//!
//! 2. **Cleanup phase**: After ALL parts are migrated successfully, remove all
//!    `.broken` directories. Only now is v1 data deleted.
//!
//! On startup, if `.broken` parts are detected, the migration was interrupted.
//! The user is given two recovery options via log messages.
//!
//! # Version history
//!
//! - **v1** (`1_{gen}_{min}_{max}_{seq}`): LZ4 compression, 4 system columns
//! - **v2** (`2_{gen}_{min}_{max}_{seq}`): zstd compression, 5 system columns (+flowcusRowId)

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tracing::{debug, error, info, warn};

use crate::cache::StorageCache;
use crate::codec;
use crate::column::ColumnBuffer;
use crate::part::{self, ColumnWriteData, PartMetadata};
use crate::schema::{ColumnDef, SYSTEM_ENTERPRISE_ID, Schema, StorageType};
use crate::uuid7::Uuid7Generator;

/// Check for interrupted migrations and exit if manual recovery is needed.
///
/// Must be called **before** spawning the background migration. If `.broken`
/// parts exist from a previous crash, logs recovery instructions and returns
/// an error — the caller should exit the process.
pub fn check_interrupted_migration(table_base: &Path) -> std::io::Result<()> {
    let broken_parts = find_broken_parts(table_base)?;
    if broken_parts.is_empty() {
        return Ok(());
    }
    log_interrupted_migration_recovery(&broken_parts, table_base);
    Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        format!(
            "Interrupted migration: {} .broken part(s) require manual recovery before Flowcus can start",
            broken_parts.len()
        ),
    ))
}

/// Start background migration on a tokio blocking thread.
///
/// Scans for v1 parts and upgrades them to v2 (adds `flowcusRowId`, re-encodes
/// with zstd). Crash-safe: v1 parts are renamed to `.broken` after their v2
/// replacement is written, and `.broken` directories are only removed after
/// all migrations complete.
///
/// Call `check_interrupted_migration` first to handle crash recovery.
pub fn start_background_migration(
    table_base: PathBuf,
    cache: Arc<StorageCache>,
    metrics: Arc<flowcus_core::observability::Metrics>,
) {
    tokio::task::spawn_blocking(move || {
        if let Err(e) = run_migration(&table_base, &cache, &metrics) {
            warn!(error = %e, "Background migration failed");
        }
    });
}

fn run_migration(
    table_base: &Path,
    cache: &StorageCache,
    metrics: &flowcus_core::observability::Metrics,
) -> std::io::Result<()> {
    let v1_parts = find_v1_parts(table_base)?;
    if v1_parts.is_empty() {
        return Ok(());
    }

    info!(count = v1_parts.len(), "Found v1 parts to migrate");
    let seq = AtomicU32::new(0);

    // Phase 1: Migrate each v1 part → v2 part, rename v1 to .broken.
    let mut migrated: Vec<PathBuf> = Vec::new();
    let mut failed = false;

    for (i, part_dir) in v1_parts.iter().enumerate() {
        info!(
            part = %part_dir.display(),
            progress = format!("{}/{}", i + 1, v1_parts.len()),
            "Migrating v1 part to v2"
        );
        match migrate_part(part_dir, table_base, &seq) {
            Ok(new_dir) => {
                // Invalidate old cache entries.
                cache.invalidate_part(part_dir);
                // Bump merge_completed to invalidate query cache.
                metrics.merge_completed.fetch_add(1, Ordering::Relaxed);

                // Rename v1 part to .broken (not delete — deferred to phase 2).
                let broken_path = part_dir.with_extension("broken");
                if let Err(e) = std::fs::rename(part_dir, &broken_path) {
                    warn!(
                        error = %e,
                        old = %part_dir.display(),
                        broken = %broken_path.display(),
                        "Failed to rename v1 part to .broken"
                    );
                    // If we can't rename, don't track it for cleanup — the v1
                    // part still exists and will be retried on next startup.
                    continue;
                }

                migrated.push(broken_path);

                debug!(
                    old = %part_dir.display(),
                    new = %new_dir.display(),
                    "Migrated v1 part to v2"
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Part was removed by a concurrent merge — not an error.
                debug!(
                    error = %e,
                    path = %part_dir.display(),
                    "v1 part no longer exists (likely merged), skipping"
                );
            }
            Err(e) => {
                warn!(
                    error = %e,
                    path = %part_dir.display(),
                    "Failed to migrate v1 part, aborting migration"
                );
                failed = true;
                break;
            }
        }

        // Yield to avoid starving other tasks.
        std::thread::yield_now();
    }

    if failed {
        // Some migrations succeeded, some didn't. The .broken parts are safe
        // (v2 replacements exist). Log recovery instructions for next startup.
        if !migrated.is_empty() {
            warn!(
                migrated = migrated.len(),
                total = v1_parts.len(),
                "Migration partially completed — .broken parts left on disk, will be cleaned up on next successful run"
            );
        }
        return Ok(());
    }

    // Phase 2: All migrations succeeded — remove .broken directories.
    for broken_path in &migrated {
        if let Err(e) = std::fs::remove_dir_all(broken_path) {
            warn!(
                error = %e,
                path = %broken_path.display(),
                "Failed to remove .broken part after successful migration"
            );
        }
    }

    info!(migrated = migrated.len(), "Migration complete");
    Ok(())
}

/// Detect .broken parts left by a crashed migration and log recovery options.
fn log_interrupted_migration_recovery(broken_parts: &[PathBuf], table_base: &Path) {
    let count = broken_parts.len();
    let table_str = table_base.display();

    error!("================================================================================");
    error!("FATAL: Interrupted migration detected — manual recovery required");
    error!("================================================================================");
    error!(
        count,
        "{count} .broken part(s) found from a previous crash. Flowcus cannot start until this is resolved."
    );
    error!("");
    error!(
        "Option 1 (recommended, no data loss): Revert migration — remove v2 parts and restore .broken originals to v1:"
    );
    error!(
        "  find {table_str} -name '2_*' -type d -exec rm -rf {{}} + && find {table_str} -name '*.broken' -type d | while read d; do mv \"$d\" \"${{d%.broken}}\"; done"
    );
    error!("");
    error!(
        "Option 2 (possible data loss): Keep migrated v2 parts and delete .broken originals to unblock Flowcus:"
    );
    error!("  find {table_str} -name '*.broken' -type d -exec rm -rf {{}} +");
    error!("");
    error!("After running either option, restart Flowcus.");
    error!("================================================================================");
}

/// Find `.broken` part directories left by an interrupted migration.
fn find_broken_parts(table_base: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut broken = Vec::new();
    walk_for_broken(table_base, 0, &mut broken)?;
    Ok(broken)
}

fn walk_for_broken(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if depth < 4 {
            walk_for_broken(&path, depth + 1, out)?;
        } else if name_str.ends_with(".broken") {
            out.push(path);
        }
    }
    Ok(())
}

/// Find all v1 parts by walking the storage directory tree.
fn find_v1_parts(table_base: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut v1_parts = Vec::new();
    walk_for_v1(table_base, 0, &mut v1_parts)?;
    Ok(v1_parts)
}

fn walk_for_v1(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if depth < 4 {
            // YYYY/MM/DD/HH tree
            walk_for_v1(&path, depth + 1, out)?;
        } else if let Some((format_version, ..)) = part::parse_part_dir_name_versioned(&name) {
            if format_version == 1 && path.join("meta.bin").exists() {
                out.push(path);
            }
        }
    }
    Ok(())
}

/// Migrate a single v1 part to v2.
fn migrate_part(part_dir: &Path, table_base: &Path, seq: &AtomicU32) -> std::io::Result<PathBuf> {
    // Read metadata.
    let meta = part::read_meta_bin(&part_dir.join("meta.bin"))?;
    let schema = part::read_schema_bin(&part_dir.join("schema.bin"))?;

    // Read the export_time column for UUIDv7 derivation.
    let export_time_col = part_dir.join("columns").join("flowcusExportTime.col");
    let export_times = if export_time_col.exists() {
        let buf = crate::decode::decode_column(&export_time_col, StorageType::U64)?;
        match buf {
            ColumnBuffer::U64(v) => v,
            _ => Vec::new(),
        }
    } else {
        // Fallback: generate timestamps from part time range.
        vec![meta.time_min; meta.row_count as usize]
    };

    // Generate UUIDv7 row IDs spread within each second.
    let mut uuid_gen = Uuid7Generator::new();
    let rowids = uuid_gen.generate_batch(&export_times, true);

    // Build new schema with flowcusRowId inserted at index 4.
    let rowid_col_def = ColumnDef {
        name: "flowcusRowId".into(),
        element_id: 0,
        enterprise_id: SYSTEM_ENTERPRISE_ID,
        data_type: flowcus_ipfix::protocol::DataType::Unsigned64,
        storage_type: StorageType::U128,
        wire_length: 16,
    };

    let mut new_columns_defs = schema.columns.clone();
    let insert_idx = 4.min(new_columns_defs.len());
    new_columns_defs.insert(insert_idx, rowid_col_def.clone());

    let new_schema = Schema {
        columns: new_columns_defs,
        duration_source: schema.duration_source,
    };

    // Default granule config.
    let granule_size = 8192;
    let bloom_bits = 1024;

    // Re-encode all columns with zstd + add flowcusRowId.
    let mut encoded_columns = Vec::with_capacity(new_schema.columns.len());
    let mut disk_bytes: u64 = 0;

    for col_def in &new_schema.columns {
        let buffer = if col_def.name == "flowcusRowId" {
            // New column: use generated UUIDv7s.
            ColumnBuffer::U128(rowids.clone())
        } else {
            // Existing column: decode from v1 part.
            let col_path = part_dir
                .join("columns")
                .join(format!("{}.col", col_def.name));
            if col_path.exists() {
                crate::decode::decode_column(&col_path, col_def.storage_type)?
            } else {
                // Column missing in source — create zero-filled buffer.
                warn!(column = %col_def.name, "Column missing in v1 part, padding with zeros");
                pad_zero_buffer(col_def.storage_type, meta.row_count as usize)
            }
        };

        let encoded = codec::encode_v2(&buffer, col_def);
        disk_bytes += (part::COLUMN_HEADER_SIZE + encoded.data.len()) as u64;

        let (marks, blooms) = crate::granule::compute_granules(
            &buffer,
            &encoded.data,
            granule_size,
            bloom_bits,
            col_def.storage_type,
        );

        encoded_columns.push(ColumnWriteData {
            def: col_def.clone(),
            encoded,
            marks,
            blooms,
        });
    }

    // Read exporter info from old meta for the new part.
    let exporter = read_exporter_from_v1_meta(part_dir)?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let new_metadata = PartMetadata {
        row_count: meta.row_count,
        generation: meta.generation,
        time_min: meta.time_min,
        time_max: meta.time_max,
        observation_domain_id: meta.observation_domain_id,
        created_at_ms: now,
        disk_bytes,
        column_count: new_schema.columns.len() as u32,
        schema_fingerprint: new_schema.fingerprint(),
        exporter,
        schema: new_schema,
    };

    let migrate_seq = seq.fetch_add(1, Ordering::Relaxed);

    part::write_part(
        table_base,
        &new_metadata,
        &encoded_columns,
        granule_size,
        bloom_bits,
        migrate_seq,
    )
}

/// Create a zero-filled `ColumnBuffer` with `rows` entries.
fn pad_zero_buffer(st: StorageType, rows: usize) -> ColumnBuffer {
    match st {
        StorageType::U8 => ColumnBuffer::U8(vec![0; rows]),
        StorageType::U16 => ColumnBuffer::U16(vec![0; rows]),
        StorageType::U32 => ColumnBuffer::U32(vec![0; rows]),
        StorageType::U64 => ColumnBuffer::U64(vec![0; rows]),
        StorageType::U128 => ColumnBuffer::U128(vec![[0, 0]; rows]),
        StorageType::Mac => ColumnBuffer::Mac(vec![[0; 6]; rows]),
        StorageType::VarLen => {
            // Empty strings: offsets all point to 0.
            ColumnBuffer::VarLen {
                offsets: vec![0; rows],
                data: Vec::new(),
            }
        }
    }
}

fn read_exporter_from_v1_meta(part_dir: &Path) -> std::io::Result<std::net::SocketAddr> {
    let meta_path = part_dir.join("meta.bin");
    let buf = std::fs::read(&meta_path)?;
    if buf.len() < 92 {
        return Ok(std::net::SocketAddr::from(([0, 0, 0, 0], 0)));
    }

    let port = u16::from_le_bytes(buf[68..70].try_into().unwrap());
    let family = buf[70];

    let addr = if family == 6 {
        let mut octets = [0u8; 16];
        octets.copy_from_slice(&buf[72..88]);
        std::net::SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)), port)
    } else {
        let mut octets = [0u8; 4];
        octets.copy_from_slice(&buf[72..76]);
        std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::from(octets)), port)
    };

    Ok(addr)
}
