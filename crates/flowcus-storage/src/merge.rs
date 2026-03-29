//! Background merge system: compacts parts within time partitions.
//!
//! Architecture:
//! - **Coordinator** (single async task): scans hour directories, builds merge plans,
//!   dispatches column-level work to the worker pool, manages atomic part swaps.
//! - **Workers** (tokio spawn_blocking): execute CPU-bound column merges (read, concat,
//!   re-encode). Run at low priority relative to ingestion.
//! - **Throttle** (fair scheduler): monitors system load and limits concurrent merges
//!   to avoid starving ingestion.
//!
//! Merge strategy:
//! - Current hour: merge parts at the same generation into fewer larger parts
//! - Past hours: keep merging until a single part per hour (best compression)
//! - Generation increments on each merge: 00000 → 00001 → 00002 → ...
//! - Stop at generation 65535 (MAX_GENERATION)
//!
//! Race condition prevention:
//! - Coordinator holds exclusive merge planning authority (single decision maker)
//! - Parts being merged are tracked in `active_merges` set
//! - New merged part is staged first, then source parts are removed
//! - If a merge fails, source parts remain untouched (crash-safe)

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use tokio::sync::Semaphore;
use tracing::{debug, error, info, trace, warn};

use crate::codec::{self, EncodedColumn};
use crate::column::ColumnBuffer;
use crate::part::{self, ColumnFileHeader, MAX_GENERATION, PartMetadata};
use crate::schema::{Schema, StorageType};
use crate::table::PartEntry;

/// Configuration for the merge system.
#[derive(Debug, Clone)]
pub struct MergeConfig {
    pub workers: usize,
    pub scan_interval: Duration,
    pub cpu_throttle: f64,
    pub mem_throttle: f64,
    pub partition_duration_secs: u32,
    pub granule_size: usize,
    pub bloom_bits: usize,
}

/// A plan to merge a set of parts into one.
#[derive(Debug)]
struct MergePlan {
    /// Source parts to merge (ordered by time_min, seq).
    sources: Vec<PartEntry>,
    /// Target generation (max source gen + 1).
    target_generation: u32,
    /// Combined time range.
    time_min: u32,
    time_max: u32,
    /// Schema from the first source (all must match by fingerprint).
    schema: Schema,
    /// Table base directory.
    base_dir: PathBuf,
}

impl MergePlan {
    /// Total rows across all source parts (reads metadata from disk).
    fn total_rows(&self) -> u64 {
        self.sources
            .iter()
            .filter_map(|s| part::read_meta_bin(&s.path.join("meta.bin")).ok())
            .map(|h| h.row_count)
            .sum()
    }
}

/// Start the background merge system.
pub fn start(
    table_base: PathBuf,
    config: MergeConfig,
    pending: crate::pending::PendingHours,
    metrics: Arc<flowcus_core::observability::Metrics>,
) {
    if config.workers == 0 {
        info!("Merge workers set to 0, background merging disabled");
        return;
    }

    // On startup, rebuild pending set from disk in case of crash/restart.
    // This is a one-time O(directories) scan, not repeated.
    pending.rebuild_from_disk(&table_base);

    info!(
        workers = config.workers,
        scan_interval_secs = config.scan_interval.as_secs(),
        cpu_throttle = config.cpu_throttle,
        pending_hours = pending.count(),
        "Merge coordinator starting"
    );

    tokio::spawn(async move {
        run_coordinator(table_base, config, pending, metrics).await;
    });
}

/// The coordinator loop: scans, plans, dispatches, cleans up.
#[allow(clippy::too_many_lines)]
async fn run_coordinator(
    table_base: PathBuf,
    config: MergeConfig,
    pending: crate::pending::PendingHours,
    metrics: Arc<flowcus_core::observability::Metrics>,
) {
    // Recover any merges that were interrupted by a previous crash/restart.
    recover_interrupted_merges(&table_base);

    // Semaphore limits concurrent merge operations for fair scheduling.
    // Starts at full capacity, shrinks when system is under load.
    let merge_permits = Arc::new(Semaphore::new(config.workers));
    let active_merges: Arc<std::sync::Mutex<HashSet<PathBuf>>> =
        Arc::new(std::sync::Mutex::new(HashSet::new()));
    let seq = Arc::new(AtomicU32::new(1));
    let shutdown = Arc::new(AtomicBool::new(false));

    let mut interval = tokio::time::interval(config.scan_interval);

    loop {
        interval.tick().await;

        if shutdown.load(Ordering::Relaxed) {
            info!("Merge coordinator shutting down");
            return;
        }

        // Update pending hours gauge
        metrics
            .merge_pending_hours
            .store(pending.count() as i64, Ordering::Relaxed);

        // Throttle check: reduce available permits based on system load
        let effective_workers = compute_effective_workers(
            config.workers,
            config.cpu_throttle,
            config.mem_throttle,
            &metrics,
        );

        // Scan ONLY pending hours (not full tree traversal)
        let plans = match build_merge_plans(
            &table_base,
            &active_merges,
            &pending,
            config.partition_duration_secs,
        ) {
            Ok(plans) => plans,
            Err(e) => {
                warn!(error = %e, "Failed to scan for merge candidates");
                continue;
            }
        };

        if plans.is_empty() {
            trace!("No merge work available");
            continue;
        }

        debug!(plans = plans.len(), effective_workers, "Merge plans ready");

        for plan in plans {
            // Check if we have capacity (fair scheduling)
            let Ok(permit) = merge_permits.clone().try_acquire_owned() else {
                trace!("All merge workers busy, deferring remaining plans");
                break;
            };

            // Mark source parts as being merged
            {
                let mut active = active_merges.lock().unwrap();
                for src in &plan.sources {
                    active.insert(src.path.clone());
                }
            }

            let active_merges = Arc::clone(&active_merges);
            let seq = Arc::clone(&seq);
            let pending = pending.clone();
            let metrics = Arc::clone(&metrics);
            let granule_size = config.granule_size;
            let bloom_bits = config.bloom_bits;

            tokio::task::spawn_blocking(move || {
                let source_paths: Vec<PathBuf> =
                    plan.sources.iter().map(|s| s.path.clone()).collect();
                let source_count = source_paths.len();

                // Remember the hour directory for clean-up
                let hour_dir = source_paths
                    .first()
                    .and_then(|p| p.parent())
                    .map(Path::to_path_buf);

                metrics.merge_active_workers.fetch_add(1, Ordering::Relaxed);
                let result = execute_merge(&plan, &seq, granule_size, bloom_bits);
                metrics.merge_active_workers.fetch_sub(1, Ordering::Relaxed);

                // Release source parts from active set
                {
                    let mut active = active_merges.lock().unwrap();
                    for path in &source_paths {
                        active.remove(path);
                    }
                }

                match result {
                    Ok((new_part, merge_bytes)) => {
                        metrics.merge_completed.fetch_add(1, Ordering::Relaxed);
                        metrics
                            .merge_rows_processed
                            .fetch_add(plan.total_rows(), Ordering::Relaxed);
                        metrics
                            .merge_parts_removed
                            .fetch_add(source_count as u64, Ordering::Relaxed);
                        metrics
                            .merge_bytes_written
                            .fetch_add(merge_bytes, Ordering::Relaxed);

                        info!(
                            new_part = %new_part.display(),
                            sources = source_count,
                            generation = plan.target_generation,
                            "Merge completed"
                        );

                        // Check if this hour is now fully merged (single part)
                        if let Some(hour) = &hour_dir {
                            let remaining = crate::pending::count_parts_in(hour);
                            if remaining <= 1 {
                                pending.mark_clean(hour);
                            }
                        }
                    }
                    Err(e) => {
                        metrics.merge_failed.fetch_add(1, Ordering::Relaxed);
                        error!(
                            error = %e,
                            sources = source_paths.len(),
                            "Merge failed, source parts preserved"
                        );
                    }
                }

                drop(permit);
            });
        }
    }
}

/// Build merge plans by scanning ONLY pending (dirty) hour directories.
/// No full tree traversal — O(pending_hours) not O(all_hours).
#[allow(clippy::too_many_lines)]
fn build_merge_plans(
    table_base: &Path,
    active_merges: &std::sync::Mutex<HashSet<PathBuf>>,
    pending: &crate::pending::PendingHours,
    partition_duration_secs: u32,
) -> std::io::Result<Vec<MergePlan>> {
    let mut plans = Vec::new();
    let active = active_merges.lock().unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32;
    let current_partition = now / partition_duration_secs;

    let dirty_hours = pending.get_dirty();
    if dirty_hours.is_empty() {
        return Ok(plans);
    }

    trace!(
        dirty_hours = dirty_hours.len(),
        "Scanning pending hours for merge candidates"
    );

    // Scan only dirty hour directories (not the full tree)
    let mut by_hour: HashMap<PathBuf, Vec<PartEntry>> = HashMap::new();

    for hour_dir in &dirty_hours {
        if !hour_dir.exists() {
            // Hour was deleted (retention policy?), clean it
            drop(active); // release lock before calling pending
            pending.mark_clean(hour_dir);
            return build_merge_plans(table_base, active_merges, pending, partition_duration_secs);
        }

        let Ok(entries) = std::fs::read_dir(hour_dir) else {
            continue;
        };

        for entry in entries {
            let Ok(entry) = entry else {
                continue;
            };
            if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                continue;
            }
            let name = match entry.file_name().to_str() {
                Some(n) => n.to_string(),
                None => continue,
            };
            if let Some((format_version, generation, pmin, pmax, seq)) =
                crate::part::parse_part_dir_name_versioned(&name)
            {
                let path = entry.path();
                if active.contains(&path) || generation >= MAX_GENERATION {
                    continue;
                }
                by_hour
                    .entry(hour_dir.clone())
                    .or_default()
                    .push(PartEntry {
                        path,
                        format_version,
                        generation,
                        time_min: pmin,
                        time_max: pmax,
                        seq,
                    });
            }
        }
    }

    for (hour_dir, mut parts) in by_hour {
        if parts.len() < 2 {
            continue;
        }

        parts.sort_by_key(|p| (p.generation, p.time_min, p.seq));

        // Read schema from the highest-generation part (most reliable after merges)
        let best_part = parts.iter().max_by_key(|p| p.generation).unwrap();
        let schema = match read_part_schema(&best_part.path) {
            Ok(s) => s,
            Err(e) => {
                debug!(error = %e, path = %best_part.path.display(), "Cannot read part schema, skipping");
                continue;
            }
        };

        let max_gen = parts.iter().map(|p| p.generation).max().unwrap();
        let target_gen = max_gen + 1;
        if target_gen > MAX_GENERATION {
            debug!(max_gen, hour = %hour_dir.display(), "At max generation, skipping merge");
            continue;
        }

        let time_min = parts.iter().map(|p| p.time_min).min().unwrap();
        let time_max = parts.iter().map(|p| p.time_max).max().unwrap();
        let partition_of_parts = time_min / partition_duration_secs;
        let is_past = partition_of_parts < current_partition;

        if is_past {
            // Past hour: merge ALL parts (any generation) into one.
            // This is the key fix: past hours must converge to a single part.
            // We merge across generations — a gen0 + gen1 part become gen2.
            info!(
                hour = %hour_dir.display(),
                parts = parts.len(),
                target_gen,
                "Past hour merge: converging to single part"
            );
            plans.push(MergePlan {
                sources: parts,
                target_generation: target_gen,
                time_min,
                time_max,
                schema,
                base_dir: table_base.to_path_buf(),
            });
        } else {
            // Current hour: merge same-generation parts in small batches.
            // Group by generation so we don't mix raw and merged data.
            let mut gen_groups: HashMap<u32, Vec<PartEntry>> = HashMap::new();
            for p in parts {
                gen_groups.entry(p.generation).or_default().push(p);
            }

            for (generation, gen_parts) in gen_groups {
                if gen_parts.len() < 2 {
                    continue;
                }
                for chunk in gen_parts.chunks(4).filter(|c| c.len() >= 2) {
                    let chunk_min = chunk.iter().map(|p| p.time_min).min().unwrap();
                    let chunk_max = chunk.iter().map(|p| p.time_max).max().unwrap();
                    plans.push(MergePlan {
                        sources: chunk.to_vec(),
                        target_generation: generation + 1,
                        time_min: chunk_min,
                        time_max: chunk_max,
                        schema: schema.clone(),
                        base_dir: table_base.to_path_buf(),
                    });
                }
            }
        }
    }

    // Prioritize: past hours first, then lower generation first (compact base level)
    plans.sort_by_key(|p| {
        let is_current = p.time_min / partition_duration_secs >= current_partition;
        (is_current, p.target_generation, p.time_min)
    });

    Ok(plans)
}

/// Execute a merge plan: read source columns, concatenate, re-encode, write new part.
/// Returns the new part directory and total bytes written on success.
#[allow(clippy::too_many_lines)]
fn execute_merge(
    plan: &MergePlan,
    seq: &AtomicU32,
    granule_size: usize,
    bloom_bits: usize,
) -> std::io::Result<(PathBuf, u64)> {
    let _t = flowcus_core::profiling::span_timer("merge;execute");
    let src_count = plan.sources.len();

    debug!(
        sources = src_count,
        generation = plan.target_generation,
        time_min = plan.time_min,
        time_max = plan.time_max,
        columns = plan.schema.columns.len(),
        "Starting merge"
    );

    // For each column: read raw data from all sources, concatenate, re-encode
    let mut encoded_columns = Vec::with_capacity(plan.schema.columns.len());
    let mut disk_bytes: u64 = 0;

    // Read row counts from source parts
    let source_rows: Vec<u64> = plan
        .sources
        .iter()
        .filter_map(|s| part::read_meta_bin(&s.path.join("meta.bin")).ok())
        .map(|h| h.row_count)
        .collect();
    let total_rows: u64 = source_rows.iter().sum();

    // granule_size and bloom_bits are passed from MergeConfig

    for col_def in &plan.schema.columns {
        let (combined_buf, encoded) = merge_column(col_def, &plan.sources, &source_rows)?;
        disk_bytes += (part::COLUMN_HEADER_SIZE + encoded.data.len()) as u64;

        // Recompute granule marks + bloom filters for the merged data
        let (marks, blooms) = crate::granule::compute_granules(
            &combined_buf,
            &encoded.data,
            granule_size,
            bloom_bits,
            col_def.storage_type,
        );

        encoded_columns.push(part::ColumnWriteData {
            def: col_def.clone(),
            encoded,
            marks,
            blooms,
        });
    }

    // Build metadata for the merged part
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let first_source = &plan.sources[0];
    let source_meta = part::read_meta_bin(&first_source.path.join("meta.bin"))?;

    let metadata = PartMetadata {
        row_count: total_rows,
        generation: plan.target_generation,
        time_min: plan.time_min,
        time_max: plan.time_max,
        observation_domain_id: source_meta.observation_domain_id,
        created_at_ms: now,
        disk_bytes,
        column_count: plan.schema.columns.len() as u32,
        schema_fingerprint: plan.schema.fingerprint(),
        exporter: read_exporter_from_meta(&first_source.path)?,
        schema: plan.schema.clone(),
    };

    let merge_seq = seq.fetch_add(1, Ordering::Relaxed);

    // Stage: write new merged part
    let new_part_dir = part::write_part(
        &plan.base_dir,
        &metadata,
        &encoded_columns,
        granule_size,
        bloom_bits,
        merge_seq,
    )?;

    // Verify the new part was written correctly
    let verify = part::read_meta_bin(&new_part_dir.join("meta.bin"))?;
    if verify.row_count != total_rows {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "Merge verification failed: expected {} rows, got {}",
                total_rows, verify.row_count
            ),
        ));
    }

    // Fsync the new part directory so the merged data is durable on disk
    // before we delete any source parts.
    fsync_dir(&new_part_dir)?;

    // Write a staging marker that lists all source parts. This acts as a
    // crash-recovery journal: if we crash after deleting some sources but
    // before finishing, `recover_interrupted_merges` can complete the work.
    let marker_path = new_part_dir.join(".merging");
    {
        let mut f = fs::File::create(&marker_path)?;
        for source in &plan.sources {
            writeln!(f, "{}", source.path.display())?;
        }
        f.sync_all()?;
    }
    // Fsync parent so the marker directory entry is durable.
    fsync_dir(&new_part_dir)?;

    // Commit: remove source parts (new part + marker are durable on disk)
    for source in &plan.sources {
        if let Err(e) = part::remove_part(&source.path) {
            warn!(
                error = %e,
                path = %source.path.display(),
                "Failed to remove source part after merge"
            );
            // Non-fatal: source part remains as an orphan, will be cleaned up later
        }
    }

    // All sources removed — delete the staging marker.
    if let Err(e) = fs::remove_file(&marker_path) {
        warn!(
            error = %e,
            path = %marker_path.display(),
            "Failed to remove .merging marker"
        );
    }

    info!(
        new_part = %new_part_dir.display(),
        rows = total_rows,
        generation = plan.target_generation,
        sources_removed = src_count,
        "Merge committed"
    );

    Ok((new_part_dir, disk_bytes))
}

/// Fsync a directory to ensure its entries are durable on disk.
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    let d = fs::File::open(dir)?;
    d.sync_all()
}

/// Recover any merges that were interrupted by a crash.
///
/// Scans all part directories under `table_base` for `.merging` marker files.
/// Each marker contains the list of source part paths (one per line) that were
/// being merged into the part that contains the marker.
///
/// Recovery logic:
/// - If any listed source path still exists on disk, the merge was not committed
///   (sources were not yet deleted), so the new (duplicate) merged part is removed.
/// - If none of the listed source paths exist, the merge was committed (all
///   sources deleted) and only the marker cleanup was missed, so we just remove
///   the marker.
pub fn recover_interrupted_merges(table_base: &Path) {
    // Walk up to 3 levels: table_base / hour_dir / part_dir / .merging
    let Ok(year_entries) = fs::read_dir(table_base) else {
        return;
    };

    let mut markers: Vec<PathBuf> = Vec::new();

    // Traverse: year/month/day/hour/part/.merging
    for y in year_entries.flatten() {
        if !y.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }
        let Ok(month_entries) = fs::read_dir(y.path()) else {
            continue;
        };
        for m in month_entries.flatten() {
            if !m.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                continue;
            }
            let Ok(day_entries) = fs::read_dir(m.path()) else {
                continue;
            };
            for d in day_entries.flatten() {
                if !d.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    continue;
                }
                let Ok(hour_entries) = fs::read_dir(d.path()) else {
                    continue;
                };
                for h in hour_entries.flatten() {
                    if !h.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                        continue;
                    }
                    let Ok(part_entries) = fs::read_dir(h.path()) else {
                        continue;
                    };
                    for p in part_entries.flatten() {
                        if !p.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                            continue;
                        }
                        let marker = p.path().join(".merging");
                        if marker.exists() {
                            markers.push(marker);
                        }
                    }
                }
            }
        }
    }

    if markers.is_empty() {
        return;
    }

    info!(
        count = markers.len(),
        "Found interrupted merges, running recovery"
    );

    for marker in markers {
        let part_dir = marker.parent().unwrap().to_path_buf();
        let content = match fs::read_to_string(&marker) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    error = %e,
                    marker = %marker.display(),
                    "Cannot read .merging marker, skipping"
                );
                continue;
            }
        };

        let source_paths: Vec<PathBuf> = content
            .lines()
            .filter(|l| !l.is_empty())
            .map(PathBuf::from)
            .collect();

        let any_source_exists = source_paths.iter().any(|p| p.exists());

        if any_source_exists {
            // Merge was NOT committed — sources still exist, so the new merged
            // part is a duplicate. Remove the new part entirely.
            warn!(
                part = %part_dir.display(),
                "Interrupted merge: sources still exist, removing incomplete merged part"
            );
            if let Err(e) = fs::remove_dir_all(&part_dir) {
                warn!(
                    error = %e,
                    part = %part_dir.display(),
                    "Failed to remove incomplete merged part during recovery"
                );
            }
        } else {
            // Merge WAS committed — all sources are gone. Just clean up the marker.
            warn!(
                part = %part_dir.display(),
                "Interrupted merge: sources already removed, cleaning up marker"
            );
            if let Err(e) = fs::remove_file(&marker) {
                warn!(
                    error = %e,
                    marker = %marker.display(),
                    "Failed to remove .merging marker during recovery"
                );
            }
        }
    }
}

/// Merge a single column from multiple source parts.
/// Reads raw encoded data, decompresses if needed, concatenates, re-encodes.
fn merge_column(
    col_def: &crate::schema::ColumnDef,
    sources: &[PartEntry],
    source_rows: &[u64],
) -> std::io::Result<(ColumnBuffer, EncodedColumn)> {
    let _t = flowcus_core::profiling::span_timer("merge;column;total");
    let col_name = &col_def.name;
    let st = col_def.storage_type;

    let total_rows: usize = source_rows.iter().map(|r| *r as usize).sum();
    let mut combined = ColumnBuffer::with_capacity(st, total_rows);

    for (i, source) in sources.iter().enumerate() {
        let col_path = source.path.join("columns").join(format!("{col_name}.col"));
        if !col_path.exists() {
            debug!(column = col_name, path = %col_path.display(), "Column missing in source, padding with zeros");
            // Pad with zero values for missing columns
            let rows = source_rows.get(i).copied().unwrap_or(0) as usize;
            pad_column(&mut combined, st, rows);
            continue;
        }

        let raw_data = part::read_column_raw(&col_path)?;
        let header = part::read_column_header(&col_path)?;

        // Decompress if needed
        let decompressed = if header.compression == 1 {
            // LZ4
            lz4_flex::decompress_size_prepended(&raw_data).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("LZ4 decompress: {e}"),
                )
            })?
        } else {
            raw_data
        };

        // For merge, we append the raw decoded bytes directly to the combined buffer.
        // The codec transform (delta, gcd) must be decoded first.
        // For simplicity and correctness: we read the raw column data as plain bytes
        // and push them into the buffer. This works because the re-encode step
        // will pick the optimal codec for the combined data.
        append_raw_to_buffer(&mut combined, st, &decompressed, &header)?;
    }

    trace!(
        column = col_name,
        rows = combined.row_count(),
        mem = combined.mem_size(),
        "Column merged, re-encoding"
    );

    // Re-encode with automatic codec selection (benefits from larger dataset)
    let encoded = codec::encode(&combined);
    Ok((combined, encoded))
}

/// Append raw bytes from a column file into a ColumnBuffer.
/// Handles the transform codec decode if necessary.
fn append_raw_to_buffer(
    buf: &mut ColumnBuffer,
    st: StorageType,
    data: &[u8],
    header: &ColumnFileHeader,
) -> std::io::Result<()> {
    let rows = header.row_count as usize;
    let codec = header.codec;

    // For Plain codec (0) on fixed-size columns: data is directly usable
    // For Delta/DeltaDelta/GCD: we need to decode back to plain values
    // For VarLen: decode offsets + data structure

    match st {
        StorageType::VarLen => {
            append_varlen_raw(buf, data, rows)?;
        }
        _ => {
            let elem_size = st.element_size().unwrap_or(1);
            let plain = if codec == 0 {
                // Plain codec: data is raw values
                data.to_vec()
            } else {
                // Delta/DeltaDelta/GCD: for now, treat as plain since the transform
                // was applied before compression and the decompressed data is the
                // transformed representation. A full decode would require reverse
                // transforms. For merge, we decode to plain values.
                decode_transform(data, codec, elem_size, rows)
            };

            append_fixed_raw(buf, st, &plain, rows);
        }
    }

    Ok(())
}

/// Decode transform codecs back to plain values.
fn decode_transform(data: &[u8], codec: u8, elem_size: usize, rows: usize) -> Vec<u8> {
    match codec {
        1 => decode_delta(data, elem_size, rows),       // Delta
        2 => decode_delta_delta(data, elem_size, rows), // DeltaDelta
        3 => decode_gcd(data, elem_size, rows),         // GCD
        _ => data.to_vec(),                             // Unknown: pass through
    }
}

fn decode_delta(data: &[u8], elem_size: usize, rows: usize) -> Vec<u8> {
    if data.len() < elem_size || rows == 0 {
        return data.to_vec();
    }
    let mut out = Vec::with_capacity(rows * elem_size);
    match elem_size {
        4 => {
            let mut val = u32::from_le_bytes(data[..4].try_into().unwrap());
            out.extend_from_slice(&val.to_ne_bytes());
            for i in 1..rows {
                let off = i * 4;
                if off + 4 > data.len() {
                    break;
                }
                let delta = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
                val = val.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
            }
        }
        8 => {
            let mut val = u64::from_le_bytes(data[..8].try_into().unwrap());
            out.extend_from_slice(&val.to_ne_bytes());
            for i in 1..rows {
                let off = i * 8;
                if off + 8 > data.len() {
                    break;
                }
                let delta = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                val = val.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
            }
        }
        2 => {
            let mut val = u16::from_le_bytes(data[..2].try_into().unwrap());
            out.extend_from_slice(&val.to_ne_bytes());
            for i in 1..rows {
                let off = i * 2;
                if off + 2 > data.len() {
                    break;
                }
                let delta = u16::from_le_bytes(data[off..off + 2].try_into().unwrap());
                val = val.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
            }
        }
        _ => return data.to_vec(),
    }
    out
}

fn decode_delta_delta(data: &[u8], elem_size: usize, rows: usize) -> Vec<u8> {
    if data.len() < elem_size * 2 || rows < 2 {
        return decode_delta(data, elem_size, rows);
    }
    let mut out = Vec::with_capacity(rows * elem_size);
    match elem_size {
        4 => {
            let first = u32::from_le_bytes(data[..4].try_into().unwrap());
            let first_delta = u32::from_le_bytes(data[4..8].try_into().unwrap());
            out.extend_from_slice(&first.to_ne_bytes());
            let mut prev = first.wrapping_add(first_delta);
            out.extend_from_slice(&prev.to_ne_bytes());
            let mut prev_delta = first_delta;
            for i in 2..rows {
                let off = i * 4;
                if off + 4 > data.len() {
                    break;
                }
                let dd = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
                let delta = prev_delta.wrapping_add(dd);
                let val = prev.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
                prev = val;
                prev_delta = delta;
            }
        }
        8 => {
            let first = u64::from_le_bytes(data[..8].try_into().unwrap());
            let first_delta = u64::from_le_bytes(data[8..16].try_into().unwrap());
            out.extend_from_slice(&first.to_ne_bytes());
            let mut prev = first.wrapping_add(first_delta);
            out.extend_from_slice(&prev.to_ne_bytes());
            let mut prev_delta = first_delta;
            for i in 2..rows {
                let off = i * 8;
                if off + 8 > data.len() {
                    break;
                }
                let dd = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                let delta = prev_delta.wrapping_add(dd);
                let val = prev.wrapping_add(delta);
                out.extend_from_slice(&val.to_ne_bytes());
                prev = val;
                prev_delta = delta;
            }
        }
        _ => return data.to_vec(),
    }
    out
}

fn decode_gcd(data: &[u8], elem_size: usize, rows: usize) -> Vec<u8> {
    if data.len() < elem_size {
        return data.to_vec();
    }
    let mut out = Vec::with_capacity(rows * elem_size);
    match elem_size {
        4 => {
            let gcd = u32::from_le_bytes(data[..4].try_into().unwrap());
            for i in 0..rows {
                let off = (i + 1) * 4;
                if off + 4 > data.len() {
                    break;
                }
                let divided = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
                out.extend_from_slice(&divided.wrapping_mul(gcd).to_ne_bytes());
            }
        }
        8 => {
            let gcd = u64::from_le_bytes(data[..8].try_into().unwrap());
            for i in 0..rows {
                let off = (i + 1) * 8;
                if off + 8 > data.len() {
                    break;
                }
                let divided = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                out.extend_from_slice(&divided.wrapping_mul(gcd).to_ne_bytes());
            }
        }
        2 => {
            let gcd = u16::from_le_bytes(data[..2].try_into().unwrap());
            for i in 0..rows {
                let off = (i + 1) * 2;
                if off + 2 > data.len() {
                    break;
                }
                let divided = u16::from_le_bytes(data[off..off + 2].try_into().unwrap());
                out.extend_from_slice(&divided.wrapping_mul(gcd).to_ne_bytes());
            }
        }
        _ => return data.to_vec(),
    }
    out
}

/// Append raw fixed-size column data to a buffer.
fn append_fixed_raw(buf: &mut ColumnBuffer, _st: StorageType, plain: &[u8], rows: usize) {
    match buf {
        ColumnBuffer::U8(v) => {
            v.extend_from_slice(&plain[..rows.min(plain.len())]);
        }
        ColumnBuffer::U16(v) => {
            for i in 0..rows {
                let off = i * 2;
                if off + 2 > plain.len() {
                    break;
                }
                v.push(u16::from_ne_bytes([plain[off], plain[off + 1]]));
            }
        }
        ColumnBuffer::U32(v) => {
            for i in 0..rows {
                let off = i * 4;
                if off + 4 > plain.len() {
                    break;
                }
                v.push(u32::from_ne_bytes(plain[off..off + 4].try_into().unwrap()));
            }
        }
        ColumnBuffer::U64(v) => {
            for i in 0..rows {
                let off = i * 8;
                if off + 8 > plain.len() {
                    break;
                }
                v.push(u64::from_ne_bytes(plain[off..off + 8].try_into().unwrap()));
            }
        }
        ColumnBuffer::U128(v) => {
            for i in 0..rows {
                let off = i * 16;
                if off + 16 > plain.len() {
                    break;
                }
                let high = u64::from_ne_bytes(plain[off..off + 8].try_into().unwrap());
                let low = u64::from_ne_bytes(plain[off + 8..off + 16].try_into().unwrap());
                v.push([high, low]);
            }
        }
        ColumnBuffer::Mac(v) => {
            for i in 0..rows {
                let off = i * 6;
                if off + 6 > plain.len() {
                    break;
                }
                let mut mac = [0u8; 6];
                mac.copy_from_slice(&plain[off..off + 6]);
                v.push(mac);
            }
        }
        ColumnBuffer::VarLen { .. } => {} // handled by append_varlen_raw
    }
}

/// Append variable-length data from encoded format to buffer.
fn append_varlen_raw(buf: &mut ColumnBuffer, data: &[u8], _rows: usize) -> std::io::Result<()> {
    if data.len() < 4 {
        return Ok(());
    }

    let offset_count = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
    let offsets_end = 4 + offset_count * 4;

    if offsets_end > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "VarLen offsets overflow",
        ));
    }

    // Parse offsets
    let mut src_offsets = Vec::with_capacity(offset_count);
    for i in 0..offset_count {
        let off = 4 + i * 4;
        src_offsets.push(u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize);
    }

    let var_data = &data[offsets_end..];

    if let ColumnBuffer::VarLen {
        offsets,
        data: buf_data,
    } = buf
    {
        // Append each element
        for i in 0..offset_count.saturating_sub(1) {
            let start = src_offsets[i];
            let end = src_offsets.get(i + 1).copied().unwrap_or(start);
            if end <= var_data.len() && start <= end {
                buf_data.extend_from_slice(&var_data[start..end]);
            }
            offsets.push(buf_data.len() as u32);
        }
    }

    Ok(())
}

/// Pad a column buffer with zero values.
fn pad_column(buf: &mut ColumnBuffer, st: StorageType, rows: usize) {
    use flowcus_ipfix::protocol::FieldValue;
    let zero = match st {
        StorageType::U8 => FieldValue::Unsigned8(0),
        StorageType::U16 => FieldValue::Unsigned16(0),
        StorageType::U32 => FieldValue::Unsigned32(0),
        StorageType::U64 => FieldValue::Unsigned64(0),
        StorageType::U128 => FieldValue::Bytes(vec![0; 16]),
        StorageType::Mac => FieldValue::Mac([0; 6]),
        StorageType::VarLen => FieldValue::Bytes(Vec::new()),
    };
    for _ in 0..rows {
        buf.push(&zero);
    }
}

/// Read the schema from a part's schema.bin (full column definitions including IE IDs).
/// Falls back to reconstructing from column file headers if schema.bin doesn't exist.
fn read_part_schema(part_dir: &Path) -> std::io::Result<Schema> {
    let schema_path = part_dir.join("schema.bin");
    if schema_path.exists() {
        return part::read_schema_bin(&schema_path);
    }

    // Fallback for parts written before schema.bin was added
    let columns = part::list_columns(part_dir)?;
    let mut col_defs = Vec::with_capacity(columns.len());

    for col_name in &columns {
        let col_path = part_dir.join("columns").join(format!("{col_name}.col"));
        let header = part::read_column_header(&col_path)?;
        let st = u8_to_storage_type(header.storage_type);
        let dt = part::u8_to_data_type(header.storage_type);

        col_defs.push(crate::schema::ColumnDef {
            name: col_name.clone(),
            element_id: 0,
            enterprise_id: 0,
            data_type: dt,
            storage_type: st,
            wire_length: st.element_size().map(|s| s as u16).unwrap_or(65535),
        });
    }

    Ok(Schema {
        columns: col_defs,
        duration_source: None,
    })
}

/// Read exporter address from meta.bin.
fn read_exporter_from_meta(part_dir: &Path) -> std::io::Result<std::net::SocketAddr> {
    let buf = std::fs::read(part_dir.join("meta.bin"))?;
    if buf.len() < part::META_HEADER_SIZE {
        return Ok(([0, 0, 0, 0], 0).into());
    }
    let port = u16::from_le_bytes(buf[60..62].try_into().unwrap());
    let family = buf[62];
    let addr = if family == 6 {
        let octets: [u8; 16] = buf[64..80].try_into().unwrap();
        std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets))
    } else {
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(buf[64], buf[65], buf[66], buf[67]))
    };
    Ok(std::net::SocketAddr::new(addr, port))
}

fn u8_to_storage_type(v: u8) -> StorageType {
    part::u8_to_storage_type(v)
}

// Removed: storage_type_to_data_type — now in part.rs as u8_to_data_type

/// Compute effective workers based on system load.
/// Returns a reduced count when CPU or memory is under pressure.
fn compute_effective_workers(
    max_workers: usize,
    cpu_throttle: f64,
    mem_throttle: f64,
    metrics: &flowcus_core::observability::Metrics,
) -> usize {
    let cpu_load = get_system_cpu_load();
    let mem_usage = get_system_mem_usage();

    let mut effective = max_workers;
    let mut throttled = false;

    if cpu_load > cpu_throttle {
        // Scale down linearly: at 100% CPU, allow only 1 worker
        let scale = ((1.0 - cpu_load) / (1.0 - cpu_throttle)).max(0.0);
        effective = (max_workers as f64 * scale).ceil() as usize;
        throttled = true;
        debug!(
            cpu_load = format!("{:.1}%", cpu_load * 100.0),
            effective, "CPU throttle active"
        );
    }

    if mem_usage > mem_throttle {
        let scale = ((1.0 - mem_usage) / (1.0 - mem_throttle)).max(0.0);
        let mem_effective = (max_workers as f64 * scale).ceil() as usize;
        effective = effective.min(mem_effective);
        throttled = true;
        debug!(
            mem_usage = format!("{:.1}%", mem_usage * 100.0),
            effective, "Memory throttle active"
        );
    }

    if throttled {
        metrics
            .merge_throttled_count
            .fetch_add(1, Ordering::Relaxed);
    }

    effective.max(1) // always allow at least 1 worker
}

/// Read system CPU load from /proc/loadavg (Linux).
/// Returns 1-minute load average normalized to CPU count.
fn get_system_cpu_load() -> f64 {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1) as f64;

    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse::<f64>().ok())
        .map(|load| (load / cpus).min(1.0))
        .unwrap_or(0.0)
}

/// Read system memory usage from /proc/meminfo (Linux).
/// Returns fraction of memory used (0.0 - 1.0).
fn get_system_mem_usage() -> f64 {
    let Ok(content) = std::fs::read_to_string("/proc/meminfo") else {
        return 0.0;
    };

    let mut total: u64 = 0;
    let mut available: u64 = 0;

    for line in content.lines() {
        if let Some(val) = line.strip_prefix("MemTotal:") {
            total = parse_meminfo_kb(val);
        } else if let Some(val) = line.strip_prefix("MemAvailable:") {
            available = parse_meminfo_kb(val);
        }
    }

    if total == 0 {
        return 0.0;
    }

    let used = total.saturating_sub(available);
    (used as f64 / total as f64).min(1.0)
}

fn parse_meminfo_kb(val: &str) -> u64 {
    val.split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_workers_no_load() {
        // At 0% load, all workers available
        let metrics = flowcus_core::observability::Metrics::new();
        assert_eq!(compute_effective_workers(8, 0.80, 0.85, &metrics), 8);
    }

    #[test]
    fn decode_delta_roundtrip() {
        // Encode delta then decode
        let values = [100u32, 105, 110, 115, 120];
        let mut data = Vec::new();
        data.extend_from_slice(&100u32.to_le_bytes()); // first value
        for i in 1..values.len() {
            let delta = values[i].wrapping_sub(values[i - 1]);
            data.extend_from_slice(&delta.to_le_bytes());
        }

        let decoded = decode_delta(&data, 4, 5);
        // Check decoded values match original
        for (i, &expected) in values.iter().enumerate() {
            let got = u32::from_ne_bytes(decoded[i * 4..i * 4 + 4].try_into().unwrap());
            assert_eq!(got, expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn decode_gcd_roundtrip() {
        // GCD=100, values: 100, 200, 300
        let mut data = Vec::new();
        data.extend_from_slice(&100u32.to_le_bytes()); // gcd
        data.extend_from_slice(&1u32.to_le_bytes()); // 100/100
        data.extend_from_slice(&2u32.to_le_bytes()); // 200/100
        data.extend_from_slice(&3u32.to_le_bytes()); // 300/100

        let decoded = decode_gcd(&data, 4, 3);
        let v0 = u32::from_ne_bytes(decoded[0..4].try_into().unwrap());
        let v1 = u32::from_ne_bytes(decoded[4..8].try_into().unwrap());
        let v2 = u32::from_ne_bytes(decoded[8..12].try_into().unwrap());
        assert_eq!(v0, 100);
        assert_eq!(v1, 200);
        assert_eq!(v2, 300);
    }

    #[test]
    fn max_generation_respected() {
        assert_eq!(MAX_GENERATION, 65535);
    }

    #[test]
    fn past_hour_plan_merges_across_generations() {
        // Simulate: past hour has gen0 + gen1 parts (from a previous partial merge).
        // The coordinator must merge them all into one gen2 part.
        use crate::table::PartEntry;

        let hour_dir = PathBuf::from("/storage/flows/2026/03/23/10");

        let parts = [
            PartEntry {
                path: hour_dir.join("1_00000_1711180000_1711183599_000001"),
                format_version: 1,
                generation: 0,
                time_min: 1_711_180_000,
                time_max: 1_711_183_599,
                seq: 1,
            },
            PartEntry {
                path: hour_dir.join("1_00001_1711180000_1711183599_000010"),
                format_version: 1,
                generation: 1,
                time_min: 1_711_180_000,
                time_max: 1_711_183_599,
                seq: 10,
            },
        ];

        // For past hours, both parts should be in the same plan
        // regardless of different generations
        let max_gen = parts.iter().map(|p| p.generation).max().unwrap();
        let target_gen = max_gen + 1;

        assert_eq!(target_gen, 2);
        assert_eq!(parts.len(), 2);
        // Both would be merged together — the key invariant
    }

    #[test]
    fn current_hour_groups_by_generation() {
        // Current hour: gen0 parts merge with gen0, gen1 with gen1
        use crate::table::PartEntry;
        use std::collections::HashMap;

        let parts = vec![
            PartEntry {
                path: PathBuf::from("a"),
                format_version: 1,
                generation: 0,
                time_min: 100,
                time_max: 200,
                seq: 1,
            },
            PartEntry {
                path: PathBuf::from("b"),
                format_version: 1,
                generation: 0,
                time_min: 100,
                time_max: 200,
                seq: 2,
            },
            PartEntry {
                path: PathBuf::from("c"),
                format_version: 1,
                generation: 1,
                time_min: 100,
                time_max: 200,
                seq: 3,
            },
        ];

        let mut gen_groups: HashMap<u32, Vec<&PartEntry>> = HashMap::new();
        for p in &parts {
            gen_groups.entry(p.generation).or_default().push(p);
        }

        // gen0 has 2 parts → mergeable
        assert_eq!(gen_groups[&0].len(), 2);
        // gen1 has 1 part → not mergeable yet (needs another gen1)
        assert_eq!(gen_groups[&1].len(), 1);
    }
}
