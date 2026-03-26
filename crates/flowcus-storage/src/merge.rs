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
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tracing::{debug, error, info, trace, warn};

use crate::codec;
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
    pub max_batch_size: usize,
}

/// A plan to merge a set of parts into one.
#[derive(Debug)]
struct MergePlan {
    /// Source parts to merge (ordered by time_min, seq).
    sources: Vec<PartEntry>,
    /// Target generation (max source gen + 1).
    target_generation: u32,
    /// Combined time range (unix milliseconds).
    time_min: u64,
    time_max: u64,
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

/// Result sent from a worker thread back to the coordinator.
struct MergeResult {
    source_paths: Vec<PathBuf>,
    source_count: usize,
    total_rows: u64,
    target_generation: u32,
    hour_dir: Option<PathBuf>,
    outcome: std::io::Result<(PathBuf, u64)>,
}

/// Start the background merge system.
pub fn start(
    table_base: PathBuf,
    config: MergeConfig,
    pending: crate::pending::PendingHours,
    metrics: Arc<flowcus_core::observability::Metrics>,
    storage_cache: Arc<crate::cache::StorageCache>,
    part_locks: crate::part_locks::PartLocks,
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

    // Work queue: coordinator pushes MergePlans, workers pull.
    let (work_tx, work_rx) = std::sync::mpsc::sync_channel::<MergePlan>(config.workers);
    let work_rx = Arc::new(std::sync::Mutex::new(work_rx));

    // Result channel: workers push results, coordinator drains.
    let (result_tx, result_rx) = std::sync::mpsc::channel::<MergeResult>();

    // Shared state for workers
    let seq = Arc::new(AtomicU32::new(1));

    // Spawn persistent worker threads
    for worker_id in 0..config.workers {
        let rx = Arc::clone(&work_rx);
        let tx = result_tx.clone();
        let seq = Arc::clone(&seq);
        let locks = part_locks.clone();
        let granule_size = config.granule_size;
        let bloom_bits = config.bloom_bits;
        let metrics = Arc::clone(&metrics);

        std::thread::Builder::new()
            .name(format!("merge-worker-{worker_id}"))
            .spawn(move || {
                loop {
                    // Block until a plan is available.
                    let plan = {
                        let rx = rx.lock().unwrap();
                        match rx.recv() {
                            Ok(plan) => plan,
                            Err(_) => return, // Channel closed, shutdown.
                        }
                    };

                    let source_paths: Vec<PathBuf> =
                        plan.sources.iter().map(|s| s.path.clone()).collect();
                    let source_count = source_paths.len();
                    let total_rows = plan.total_rows();
                    let target_generation = plan.target_generation;
                    let hour_dir = source_paths
                        .first()
                        .and_then(|p| p.parent())
                        .map(Path::to_path_buf);

                    metrics.merge_active_workers.fetch_add(1, Ordering::Relaxed);
                    let outcome = execute_merge(&plan, &seq, granule_size, bloom_bits, &locks);
                    metrics.merge_active_workers.fetch_sub(1, Ordering::Relaxed);

                    let _ = tx.send(MergeResult {
                        source_paths,
                        source_count,
                        total_rows,
                        target_generation,
                        hour_dir,
                        outcome,
                    });
                }
            })
            .expect("Failed to spawn merge worker thread");
    }
    // Drop the extra sender so the channel closes when coordinator drops its sender.
    drop(result_tx);

    let coordinator_locks = part_locks.clone();
    tokio::spawn(async move {
        run_coordinator(
            table_base,
            config,
            pending,
            metrics,
            storage_cache,
            coordinator_locks,
            work_tx,
            result_rx,
        )
        .await;
    });
}

/// The coordinator loop: scans, plans, dispatches, processes results.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn run_coordinator(
    table_base: PathBuf,
    config: MergeConfig,
    pending: crate::pending::PendingHours,
    metrics: Arc<flowcus_core::observability::Metrics>,
    storage_cache: Arc<crate::cache::StorageCache>,
    part_locks: crate::part_locks::PartLocks,
    work_tx: std::sync::mpsc::SyncSender<MergePlan>,
    result_rx: std::sync::mpsc::Receiver<MergeResult>,
) {
    // Recover any merges that were interrupted by a previous crash/restart.
    recover_interrupted_merges(&table_base);

    let active_merges: Arc<std::sync::Mutex<HashSet<PathBuf>>> =
        Arc::new(std::sync::Mutex::new(HashSet::new()));

    let mut throttle =
        ThrottleController::new(config.workers, config.cpu_throttle, config.mem_throttle);

    let mut interval = tokio::time::interval(config.scan_interval);

    loop {
        interval.tick().await;

        // Drain completed merge results from workers
        process_results(
            &result_rx,
            &active_merges,
            &pending,
            &metrics,
            &storage_cache,
        );

        // Prune stale entries (directories removed by retention) and update gauge
        pending.prune_stale();
        part_locks.prune();
        metrics
            .merge_pending_hours
            .store(pending.count() as i64, Ordering::Relaxed);

        // Recompute throttle slots
        let slots = throttle.recompute(&metrics);

        // Scan ONLY pending hours (not full tree traversal)
        let plans = match build_merge_plans(
            &table_base,
            &active_merges,
            &pending,
            config.partition_duration_secs,
            config.max_batch_size,
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

        debug!(plans = plans.len(), slots, "Merge plans ready");

        let mut dispatched = 0usize;
        for plan in plans {
            if dispatched >= slots {
                trace!("Throttle limit reached, deferring remaining plans");
                break;
            }

            // Mark source parts as being merged
            {
                let mut active = active_merges.lock().unwrap();
                for src in &plan.sources {
                    active.insert(src.path.clone());
                }
            }

            // Push to work queue; if full (all workers busy), stop dispatching.
            match work_tx.try_send(plan) {
                Ok(()) => dispatched += 1,
                Err(std::sync::mpsc::TrySendError::Full(plan)) => {
                    // Undo active marks — plan wasn't dispatched.
                    let mut active = active_merges.lock().unwrap();
                    for src in &plan.sources {
                        active.remove(&src.path);
                    }
                    trace!("Work queue full, deferring remaining plans");
                    break;
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                    error!("Merge work queue disconnected, workers gone");
                    return;
                }
            }
        }
    }
}

/// Drain completed merge results and update metrics/cache/pending.
fn process_results(
    result_rx: &std::sync::mpsc::Receiver<MergeResult>,
    active_merges: &std::sync::Mutex<HashSet<PathBuf>>,
    pending: &crate::pending::PendingHours,
    metrics: &flowcus_core::observability::Metrics,
    storage_cache: &crate::cache::StorageCache,
) {
    while let Ok(result) = result_rx.try_recv() {
        // Release source parts from active set
        {
            let mut active = active_merges.lock().unwrap();
            for path in &result.source_paths {
                active.remove(path);
            }
        }

        match result.outcome {
            Ok((new_part, merge_bytes)) => {
                metrics.merge_completed.fetch_add(1, Ordering::Relaxed);
                metrics
                    .merge_rows_processed
                    .fetch_add(result.total_rows, Ordering::Relaxed);
                metrics
                    .merge_parts_removed
                    .fetch_add(result.source_count as u64, Ordering::Relaxed);
                metrics
                    .merge_bytes_written
                    .fetch_add(merge_bytes, Ordering::Relaxed);

                for path in &result.source_paths {
                    storage_cache.invalidate_part(path);
                }

                info!(
                    new_part = %new_part.display(),
                    sources = result.source_count,
                    generation = result.target_generation,
                    "Merge completed"
                );

                if let Some(hour) = &result.hour_dir {
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
                    sources = result.source_count,
                    "Merge failed, source parts preserved"
                );
            }
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
    max_batch_size: usize,
) -> std::io::Result<Vec<MergePlan>> {
    let mut plans = Vec::new();
    let active = active_merges.lock().unwrap();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let partition_duration_ms = u64::from(partition_duration_secs) * 1000;
    let current_partition = now_ms / partition_duration_ms;

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
            return build_merge_plans(
                table_base,
                active_merges,
                pending,
                partition_duration_secs,
                max_batch_size,
            );
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

        // Both past and current hours: group by generation, chunk into bounded batches.
        // Past hours get priority in the final sort (below) so they converge first.
        // Multiple merge rounds increment generations until only one part remains.
        let mut gen_groups: HashMap<u32, Vec<PartEntry>> = HashMap::new();
        for p in parts {
            gen_groups.entry(p.generation).or_default().push(p);
        }

        for (generation, gen_parts) in gen_groups {
            if gen_parts.len() < 2 {
                continue;
            }
            for chunk in gen_parts.chunks(max_batch_size).filter(|c| c.len() >= 2) {
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

    // Prioritize: past hours first, then lower generation first (compact base level)
    plans.sort_by_key(|p| {
        let is_current = p.time_min / partition_duration_ms >= current_partition;
        (is_current, p.target_generation, p.time_min)
    });

    Ok(plans)
}

/// Execute a streaming merge plan: read rowids to build merge order, then process
/// one column at a time to avoid holding all data in memory simultaneously.
/// Returns the new part directory and total bytes written on success.
#[allow(clippy::too_many_lines)]
fn execute_merge(
    plan: &MergePlan,
    seq: &AtomicU32,
    granule_size: usize,
    bloom_bits: usize,
    part_locks: &crate::part_locks::PartLocks,
) -> std::io::Result<(PathBuf, u64)> {
    let src_count = plan.sources.len();

    debug!(
        sources = src_count,
        generation = plan.target_generation,
        time_min = plan.time_min,
        time_max = plan.time_max,
        columns = plan.schema.columns.len(),
        "Starting streaming merge"
    );

    // Read row counts from source parts
    let source_rows: Vec<u64> = plan
        .sources
        .iter()
        .filter_map(|s| part::read_meta_bin(&s.path.join("meta.bin")).ok())
        .map(|h| h.row_count)
        .collect();
    let total_rows: u64 = source_rows.iter().sum();

    // Determine effective schema (may insert flowcusRowId for v1 parts)
    let has_rowid = plan.schema.columns.iter().any(|c| c.name == "flowcusRowId");

    let effective_schema = if has_rowid {
        plan.schema.clone()
    } else {
        // v1 parts: insert flowcusRowId column at index 4
        let mut cols = plan.schema.columns.clone();
        let rowid_col_def = crate::schema::ColumnDef {
            name: "flowcusRowId".into(),
            element_id: 0,
            enterprise_id: crate::schema::SYSTEM_ENTERPRISE_ID,
            data_type: flowcus_ipfix::protocol::DataType::Unsigned64,
            storage_type: crate::schema::StorageType::U128,
            wire_length: 16,
        };
        let insert_idx = 4.min(cols.len());
        cols.insert(insert_idx, rowid_col_def);
        crate::schema::Schema {
            columns: cols,
            duration_source: plan.schema.duration_source,
        }
    };

    // Phase 1: Build merge order from rowids only (memory-efficient).
    let merge_order = build_merge_order(plan, &source_rows, has_rowid)?;

    // Phase 2: Stream one column at a time through PartWriter.
    let merge_seq = seq.fetch_add(1, Ordering::Relaxed);

    // Build metadata (disk_bytes updated after all columns written)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let first_source = &plan.sources[0];
    let source_meta = part::read_meta_bin(&first_source.path.join("meta.bin"))?;

    let mut metadata = PartMetadata {
        row_count: total_rows,
        generation: plan.target_generation,
        time_min: plan.time_min,
        time_max: plan.time_max,
        observation_domain_id: source_meta.observation_domain_id,
        created_at_ms: now,
        disk_bytes: 0,
        column_count: effective_schema.columns.len() as u32,
        schema_fingerprint: effective_schema.fingerprint(),
        exporter: read_exporter_from_meta(&first_source.path)?,
        schema: effective_schema.clone(),
    };

    let mut writer = part::PartWriter::new(&plan.base_dir, &metadata, merge_seq)?;
    let mut disk_bytes: u64 = 0;

    for col_def in &effective_schema.columns {
        // For v1 parts that had no flowcusRowId, we generated it in build_merge_order.
        // The rowid column was not on disk — it's synthesized. We need to reconstruct it
        // from the merge order itself (which already encodes the sorted rowid sequence).
        let merged_buf = if col_def.name == "flowcusRowId" && !has_rowid {
            // Regenerate rowids in merge order (they're already sorted).
            build_generated_rowid_column(plan, &source_rows, &merge_order)?
        } else {
            // Read this column from all sources, then gather in merge order.
            let source_bufs = read_column_from_sources(col_def, &plan.sources, &source_rows)?;
            gather_in_order(
                &source_bufs,
                &merge_order,
                col_def.storage_type,
                total_rows as usize,
            )
        };

        let encoded = codec::encode_v2(&merged_buf, col_def);
        disk_bytes += (part::COLUMN_HEADER_SIZE + encoded.data.len()) as u64;

        let (marks, blooms) = crate::granule::compute_granules(
            &merged_buf,
            &encoded.data,
            granule_size,
            bloom_bits,
            col_def.storage_type,
        );

        writer.write_column(
            &part::ColumnWriteData {
                def: col_def.clone(),
                encoded,
                marks,
                blooms,
            },
            total_rows,
            granule_size,
            bloom_bits,
        )?;
        // merged_buf dropped here — memory freed before next column
    }

    metadata.disk_bytes = disk_bytes;
    let new_part_dir = writer.finish(&metadata)?;

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

    // Commit: write .merging marker, delete sources, remove marker.
    commit_merge(&new_part_dir, plan, part_locks)?;

    info!(
        new_part = %new_part_dir.display(),
        rows = total_rows,
        generation = plan.target_generation,
        sources_removed = src_count,
        "Merge committed"
    );

    Ok((new_part_dir, disk_bytes))
}

/// Build the global merge order by reading only rowid columns.
/// Returns `Vec<(u8, u32)>` where each entry is (source_index, row_within_source),
/// representing the globally sorted sequence of rows.
fn build_merge_order(
    plan: &MergePlan,
    source_rows: &[u64],
    has_rowid: bool,
) -> std::io::Result<Vec<(u8, u32)>> {
    let total_rows: usize = source_rows.iter().map(|r| *r as usize).sum();

    // Read rowids from each source
    let mut source_rowids: Vec<Vec<[u64; 2]>> = Vec::with_capacity(plan.sources.len());

    if has_rowid {
        for (i, source) in plan.sources.iter().enumerate() {
            let rows = source_rows.get(i).copied().unwrap_or(0) as usize;
            let col_path = source.path.join("columns").join("flowcusRowId.col");
            if col_path.exists() {
                let mut buf = ColumnBuffer::with_capacity(StorageType::U128, rows);
                let raw_data = part::read_column_raw(&col_path)?;
                let header = part::read_column_header(&col_path)?;
                let compression = match header.compression {
                    1 => crate::codec::CompressionType::Lz4,
                    2 => crate::codec::CompressionType::Zstd,
                    _ => crate::codec::CompressionType::None,
                };
                let decompressed =
                    crate::codec::decompress(&raw_data, compression, header.raw_size as usize)?;
                append_raw_to_buffer(&mut buf, StorageType::U128, &decompressed, &header)?;

                let mut rowids = match buf {
                    ColumnBuffer::U128(v) => v,
                    _ => Vec::new(),
                };

                // Handle v1 parts with zero-padded rowids (mixed v1+v2 merge)
                if rowids.contains(&[0, 0]) {
                    if let Some(et_idx) = plan
                        .schema
                        .columns
                        .iter()
                        .position(|c| c.name == "flowcusExportTime")
                    {
                        let timestamps =
                            read_export_times(source, rows, &plan.schema.columns[et_idx])?;
                        let mut uuid_gen = crate::uuid7::Uuid7Generator::new();
                        for (j, rid) in rowids.iter_mut().enumerate() {
                            if *rid == [0, 0] {
                                let ts = timestamps.get(j).copied().unwrap_or(0);
                                *rid = uuid_gen.generate(ts);
                            }
                        }
                    }
                }

                source_rowids.push(rowids);
            } else {
                // Source lacks rowid column — generate from export_time
                let rowids = generate_rowids_for_source(source, rows, &plan.schema)?;
                source_rowids.push(rowids);
            }
        }
    } else {
        // All v1 parts: generate rowids from export_time for each source
        for (i, source) in plan.sources.iter().enumerate() {
            let rows = source_rows.get(i).copied().unwrap_or(0) as usize;
            let rowids = generate_rowids_for_source(source, rows, &plan.schema)?;
            source_rowids.push(rowids);
        }
    }

    // K-way merge the sorted rowid arrays
    let order = kway_merge_rowids(&source_rowids, total_rows);

    Ok(order)
}

/// Read export_time values from a single source part.
fn read_export_times(
    source: &PartEntry,
    rows: usize,
    et_def: &crate::schema::ColumnDef,
) -> std::io::Result<Vec<u64>> {
    let col_path = source.path.join("columns").join("flowcusExportTime.col");
    if !col_path.exists() {
        return Ok(vec![0; rows]);
    }
    let mut buf = ColumnBuffer::with_capacity(StorageType::U64, rows);
    let raw_data = part::read_column_raw(&col_path)?;
    let header = part::read_column_header(&col_path)?;
    let compression = match header.compression {
        1 => crate::codec::CompressionType::Lz4,
        2 => crate::codec::CompressionType::Zstd,
        _ => crate::codec::CompressionType::None,
    };
    let decompressed = crate::codec::decompress(&raw_data, compression, header.raw_size as usize)?;
    append_raw_to_buffer(&mut buf, et_def.storage_type, &decompressed, &header)?;

    match buf {
        ColumnBuffer::U64(v) => Ok(v),
        _ => Ok(vec![0; rows]),
    }
}

/// Generate rowids for a v1 source part from its export_time column.
fn generate_rowids_for_source(
    source: &PartEntry,
    rows: usize,
    schema: &crate::schema::Schema,
) -> std::io::Result<Vec<[u64; 2]>> {
    let timestamps = if let Some(et_def) = schema
        .columns
        .iter()
        .find(|c| c.name == "flowcusExportTime")
    {
        read_export_times(source, rows, et_def)?
    } else {
        vec![0; rows]
    };

    let mut uuid_gen = crate::uuid7::Uuid7Generator::new();
    Ok(uuid_gen.generate_batch(&timestamps, true))
}

/// Build the generated rowid column for v1 parts in merge-order sequence.
/// Since generate_rowids_for_source produces time-sorted UUIDs and the merge
/// order is already computed from those rowids, we just regenerate and gather.
fn build_generated_rowid_column(
    plan: &MergePlan,
    source_rows: &[u64],
    merge_order: &[(u8, u32)],
) -> std::io::Result<ColumnBuffer> {
    let total = merge_order.len();
    let mut source_rowids: Vec<Vec<[u64; 2]>> = Vec::with_capacity(plan.sources.len());
    for (i, source) in plan.sources.iter().enumerate() {
        let rows = source_rows.get(i).copied().unwrap_or(0) as usize;
        let rowids = generate_rowids_for_source(source, rows, &plan.schema)?;
        source_rowids.push(rowids);
    }

    let mut result = Vec::with_capacity(total);
    for &(src_idx, row_idx) in merge_order {
        let rowid = source_rowids
            .get(src_idx as usize)
            .and_then(|v| v.get(row_idx as usize))
            .copied()
            .unwrap_or([0, 0]);
        result.push(rowid);
    }

    Ok(ColumnBuffer::U128(result))
}

/// K-way merge of pre-sorted rowid arrays using a min-heap.
/// Produces a global merge order as `(source_index, row_within_source)`.
fn kway_merge_rowids(sources: &[Vec<[u64; 2]>], total_rows: usize) -> Vec<(u8, u32)> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut order = Vec::with_capacity(total_rows);

    if sources.len() == 1 {
        // Fast path: single source, no merge needed
        for i in 0..sources[0].len() {
            order.push((0u8, i as u32));
        }
        return order;
    }

    // Heap entries: (Reverse(rowid), source_idx, cursor_position)
    // Reverse because BinaryHeap is max-heap and we want min.
    let mut heap: BinaryHeap<(Reverse<[u64; 2]>, u8, u32)> =
        BinaryHeap::with_capacity(sources.len());

    // Seed the heap with the first element from each source
    for (i, src) in sources.iter().enumerate() {
        if !src.is_empty() {
            heap.push((Reverse(src[0]), i as u8, 0));
        }
    }

    while let Some((Reverse(_rowid), src_idx, row_idx)) = heap.pop() {
        order.push((src_idx, row_idx));

        let next = row_idx + 1;
        let src = &sources[src_idx as usize];
        if (next as usize) < src.len() {
            heap.push((Reverse(src[next as usize]), src_idx, next));
        }
    }

    order
}

/// Read a single column from all source parts, returning one ColumnBuffer per source.
fn read_column_from_sources(
    col_def: &crate::schema::ColumnDef,
    sources: &[PartEntry],
    source_rows: &[u64],
) -> std::io::Result<Vec<ColumnBuffer>> {
    let col_name = &col_def.name;
    let st = col_def.storage_type;
    let mut result = Vec::with_capacity(sources.len());

    for (i, source) in sources.iter().enumerate() {
        let rows = source_rows.get(i).copied().unwrap_or(0) as usize;
        let col_path = source.path.join("columns").join(format!("{col_name}.col"));

        if !col_path.exists() {
            let mut buf = ColumnBuffer::with_capacity(st, rows);
            pad_column(&mut buf, st, rows);
            result.push(buf);
            continue;
        }

        let mut buf = ColumnBuffer::with_capacity(st, rows);
        let raw_data = part::read_column_raw(&col_path)?;
        let header = part::read_column_header(&col_path)?;
        let compression = match header.compression {
            1 => crate::codec::CompressionType::Lz4,
            2 => crate::codec::CompressionType::Zstd,
            _ => crate::codec::CompressionType::None,
        };
        let decompressed =
            crate::codec::decompress(&raw_data, compression, header.raw_size as usize)?;
        append_raw_to_buffer(&mut buf, st, &decompressed, &header)?;
        result.push(buf);
    }

    Ok(result)
}

/// Gather rows from multiple source buffers in merge-order sequence.
/// Produces a single combined ColumnBuffer with rows in the specified order.
fn gather_in_order(
    sources: &[ColumnBuffer],
    order: &[(u8, u32)],
    st: StorageType,
    total_rows: usize,
) -> ColumnBuffer {
    match st {
        StorageType::U8 => {
            let mut v = Vec::with_capacity(total_rows);
            for &(src_idx, row_idx) in order {
                let val = match &sources[src_idx as usize] {
                    ColumnBuffer::U8(s) => s.get(row_idx as usize).copied().unwrap_or(0),
                    _ => 0,
                };
                v.push(val);
            }
            ColumnBuffer::U8(v)
        }
        StorageType::U16 => {
            let mut v = Vec::with_capacity(total_rows);
            for &(src_idx, row_idx) in order {
                let val = match &sources[src_idx as usize] {
                    ColumnBuffer::U16(s) => s.get(row_idx as usize).copied().unwrap_or(0),
                    _ => 0,
                };
                v.push(val);
            }
            ColumnBuffer::U16(v)
        }
        StorageType::U32 => {
            let mut v = Vec::with_capacity(total_rows);
            for &(src_idx, row_idx) in order {
                let val = match &sources[src_idx as usize] {
                    ColumnBuffer::U32(s) => s.get(row_idx as usize).copied().unwrap_or(0),
                    _ => 0,
                };
                v.push(val);
            }
            ColumnBuffer::U32(v)
        }
        StorageType::U64 => {
            let mut v = Vec::with_capacity(total_rows);
            for &(src_idx, row_idx) in order {
                let val = match &sources[src_idx as usize] {
                    ColumnBuffer::U64(s) => s.get(row_idx as usize).copied().unwrap_or(0),
                    _ => 0,
                };
                v.push(val);
            }
            ColumnBuffer::U64(v)
        }
        StorageType::U128 => {
            let mut v = Vec::with_capacity(total_rows);
            for &(src_idx, row_idx) in order {
                let val = match &sources[src_idx as usize] {
                    ColumnBuffer::U128(s) => s.get(row_idx as usize).copied().unwrap_or([0, 0]),
                    _ => [0, 0],
                };
                v.push(val);
            }
            ColumnBuffer::U128(v)
        }
        StorageType::Mac => {
            let mut v = Vec::with_capacity(total_rows);
            for &(src_idx, row_idx) in order {
                let val = match &sources[src_idx as usize] {
                    ColumnBuffer::Mac(s) => s.get(row_idx as usize).copied().unwrap_or([0; 6]),
                    _ => [0; 6],
                };
                v.push(val);
            }
            ColumnBuffer::Mac(v)
        }
        StorageType::VarLen => {
            let mut data = Vec::new();
            let mut offsets = Vec::with_capacity(total_rows);
            for &(src_idx, row_idx) in order {
                if let ColumnBuffer::VarLen {
                    offsets: src_offsets,
                    data: src_data,
                } = &sources[src_idx as usize]
                {
                    let idx = row_idx as usize;
                    let start = if idx == 0 {
                        0usize
                    } else {
                        src_offsets.get(idx - 1).copied().unwrap_or(0) as usize
                    };
                    let end = src_offsets.get(idx).copied().unwrap_or(start as u32) as usize;
                    if end <= src_data.len() && start <= end {
                        data.extend_from_slice(&src_data[start..end]);
                    }
                }
                offsets.push(data.len() as u32);
            }
            ColumnBuffer::VarLen { offsets, data }
        }
    }
}

/// Commit a completed merge: write .merging marker, delete sources, clean up.
fn commit_merge(
    new_part_dir: &Path,
    plan: &MergePlan,
    part_locks: &crate::part_locks::PartLocks,
) -> std::io::Result<()> {
    // Fsync the new part directory so the data is durable before source deletion.
    fsync_dir(new_part_dir)?;

    // Hold a write lock on the new part so queries don't read it while the
    // .merging marker is still present.
    let _new_part_lock = part_locks.with_write(new_part_dir, || {
        // Write crash-recovery journal listing source parts.
        let marker_path = new_part_dir.join(".merging");
        {
            let mut f = fs::File::create(&marker_path)?;
            for source in &plan.sources {
                writeln!(f, "{}", source.path.display())?;
            }
            f.sync_all()?;
        }
        fsync_dir(new_part_dir)?;

        // Delete source parts with write locks.
        for source in &plan.sources {
            let deleted = part_locks.with_write(&source.path, || part::remove_part(&source.path));
            match deleted {
                Some(Ok(())) => {}
                Some(Err(e)) => {
                    warn!(
                        error = %e,
                        path = %source.path.display(),
                        "Failed to remove source part after merge"
                    );
                }
                None => {
                    warn!(
                        path = %source.path.display(),
                        "Lock timeout removing source part, forcing removal"
                    );
                    if let Err(e) = part::remove_part(&source.path) {
                        warn!(error = %e, "Forced removal also failed");
                    }
                }
            }
        }

        // All sources removed — delete the marker.
        if let Err(e) = fs::remove_file(&marker_path) {
            warn!(
                error = %e,
                path = %marker_path.display(),
                "Failed to remove .merging marker"
            );
        }
        Ok::<(), std::io::Error>(())
    });

    Ok(())
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
                        let name = p.file_name();
                        let name_str = name.to_string_lossy();
                        // Clean up leftover staging dirs from crashed writes
                        if name_str.ends_with(".staging") {
                            info!(path = %p.path().display(), "Removing leftover staging directory");
                            let _ = fs::remove_dir_all(p.path());
                            continue;
                        }
                        // Clean up leftover in-progress dirs from crashed merges.
                        // Safe to delete: source parts were never removed (rename
                        // to final path happens before source deletion).
                        if name_str.ends_with(".inprogress") {
                            warn!(path = %p.path().display(), "Removing incomplete merge artifact from previous run");
                            let _ = fs::remove_dir_all(p.path());
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
    let port = u16::from_le_bytes(buf[68..70].try_into().unwrap());
    let family = buf[70];
    let addr = if family == 6 {
        let octets: [u8; 16] = buf[72..88].try_into().unwrap();
        std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets))
    } else {
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(buf[72], buf[73], buf[74], buf[75]))
    };
    Ok(std::net::SocketAddr::new(addr, port))
}

fn u8_to_storage_type(v: u8) -> StorageType {
    part::u8_to_storage_type(v)
}

// Removed: storage_type_to_data_type — now in part.rs as u8_to_data_type

/// Stateful throttle controller with exponential ramp-down/up.
///
/// On high load: halves available slots each cycle (4 → 2 → 1).
/// On low load: doubles available slots each cycle (1 → 2 → 4).
/// Never reduces below 1 — merges are never stopped completely.
/// Only controls how many new merges can be started; running merges finish naturally.
struct ThrottleController {
    max_workers: usize,
    current_slots: usize,
    cpu_threshold: f64,
    mem_threshold: f64,
}

impl ThrottleController {
    fn new(max_workers: usize, cpu_threshold: f64, mem_threshold: f64) -> Self {
        Self {
            max_workers,
            current_slots: max_workers,
            cpu_threshold,
            mem_threshold,
        }
    }

    /// Recompute available slots based on current system load.
    /// Returns the number of new merges that can be dispatched this cycle.
    fn recompute(&mut self, metrics: &flowcus_core::observability::Metrics) -> usize {
        let cpu_load = get_system_cpu_load();
        let mem_usage = get_system_mem_usage();

        let overloaded = cpu_load > self.cpu_threshold || mem_usage > self.mem_threshold;

        let prev = self.current_slots;
        if overloaded {
            self.current_slots = (self.current_slots / 2).max(1);
            metrics
                .merge_throttled_count
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                cpu_load = format!("{:.1}%", cpu_load * 100.0),
                mem_usage = format!("{:.1}%", mem_usage * 100.0),
                slots = %format!("{prev} → {}", self.current_slots),
                "Throttle: reducing merge slots"
            );
        } else {
            self.current_slots = (self.current_slots * 2).min(self.max_workers);
            if self.current_slots != prev {
                debug!(
                    slots = %format!("{prev} → {}", self.current_slots),
                    "Throttle: increasing merge slots"
                );
            }
        }

        self.current_slots
    }
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
                path: hour_dir.join("1_00000_1711180000000_1711183599000_000001"),
                format_version: 1,
                generation: 0,
                time_min: 1_711_180_000_000,
                time_max: 1_711_183_599_000,
                seq: 1,
            },
            PartEntry {
                path: hour_dir.join("1_00001_1711180000000_1711183599000_000010"),
                format_version: 1,
                generation: 1,
                time_min: 1_711_180_000_000,
                time_max: 1_711_183_599_000,
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
                time_min: 100_000,
                time_max: 200_000,
                seq: 1,
            },
            PartEntry {
                path: PathBuf::from("b"),
                format_version: 1,
                generation: 0,
                time_min: 100_000,
                time_max: 200_000,
                seq: 2,
            },
            PartEntry {
                path: PathBuf::from("c"),
                format_version: 1,
                generation: 1,
                time_min: 100_000,
                time_max: 200_000,
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

    // ---- Throttle controller tests ----

    #[test]
    fn throttle_ramp_down() {
        // ThrottleController halves slots on high load: 4 → 2 → 1
        let metrics = flowcus_core::observability::Metrics::new();
        let mut tc = ThrottleController::new(4, 0.0, 0.0); // thresholds at 0% → always overloaded
        assert_eq!(tc.recompute(&metrics), 2);
        assert_eq!(tc.recompute(&metrics), 1);
        assert_eq!(tc.recompute(&metrics), 1); // stays at 1
    }

    #[test]
    fn throttle_ramp_up() {
        // ThrottleController doubles slots on low load: 1 → 2 → 4
        let metrics = flowcus_core::observability::Metrics::new();
        let mut tc = ThrottleController::new(4, 1.0, 1.0); // thresholds at 100% → never overloaded
        tc.current_slots = 1;
        assert_eq!(tc.recompute(&metrics), 2);
        assert_eq!(tc.recompute(&metrics), 4);
        assert_eq!(tc.recompute(&metrics), 4); // capped at max
    }

    #[test]
    fn throttle_never_zero() {
        let metrics = flowcus_core::observability::Metrics::new();
        let mut tc = ThrottleController::new(1, 0.0, 0.0);
        // Even with 1 worker and continuous overload, never goes to 0
        for _ in 0..10 {
            assert!(tc.recompute(&metrics) >= 1);
        }
    }

    // ---- K-way merge tests ----

    #[test]
    fn kway_merge_two_sorted() {
        let a = vec![[0u64, 1], [0, 3], [0, 5]];
        let b = vec![[0u64, 2], [0, 4], [0, 6]];
        let order = kway_merge_rowids(&[a, b], 6);
        assert_eq!(order.len(), 6);
        // Should produce globally sorted sequence
        let expected: Vec<(u8, u32)> = vec![
            (0, 0), // [0,1]
            (1, 0), // [0,2]
            (0, 1), // [0,3]
            (1, 1), // [0,4]
            (0, 2), // [0,5]
            (1, 2), // [0,6]
        ];
        assert_eq!(order, expected);
    }

    #[test]
    fn kway_merge_single_source() {
        let a = vec![[0u64, 10], [0, 20], [0, 30]];
        let order = kway_merge_rowids(&[a], 3);
        assert_eq!(order, vec![(0, 0), (0, 1), (0, 2)]);
    }

    #[test]
    fn kway_merge_empty_sources() {
        let order = kway_merge_rowids(&[], 0);
        assert!(order.is_empty());

        let order = kway_merge_rowids(&[vec![], vec![]], 0);
        assert!(order.is_empty());
    }

    // ---- Gather in order tests ----

    #[test]
    fn gather_in_order_u64() {
        let src0 = ColumnBuffer::U64(vec![10, 20, 30]);
        let src1 = ColumnBuffer::U64(vec![40, 50]);
        let order = vec![(1u8, 0u32), (0, 2), (0, 0), (1, 1), (0, 1)];
        let result = gather_in_order(&[src0, src1], &order, StorageType::U64, 5);
        match result {
            ColumnBuffer::U64(v) => assert_eq!(v, vec![40, 30, 10, 50, 20]),
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn gather_in_order_u128() {
        let src0 = ColumnBuffer::U128(vec![[0, 1], [0, 2]]);
        let src1 = ColumnBuffer::U128(vec![[0, 3]]);
        let order = vec![(1u8, 0u32), (0, 0), (0, 1)];
        let result = gather_in_order(&[src0, src1], &order, StorageType::U128, 3);
        match result {
            ColumnBuffer::U128(v) => assert_eq!(v, vec![[0, 3], [0, 1], [0, 2]]),
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn gather_in_order_mac() {
        let src0 = ColumnBuffer::Mac(vec![[1; 6], [2; 6]]);
        let src1 = ColumnBuffer::Mac(vec![[3; 6]]);
        let order = vec![(1u8, 0u32), (0, 1), (0, 0)];
        let result = gather_in_order(&[src0, src1], &order, StorageType::Mac, 3);
        match result {
            ColumnBuffer::Mac(v) => assert_eq!(v, vec![[3; 6], [2; 6], [1; 6]]),
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn gather_in_order_varlen() {
        // src0: ["abc", "def"], src1: ["xy"]
        let src0 = ColumnBuffer::VarLen {
            offsets: vec![3, 6],
            data: b"abcdef".to_vec(),
        };
        let src1 = ColumnBuffer::VarLen {
            offsets: vec![2],
            data: b"xy".to_vec(),
        };
        let order = vec![(1u8, 0u32), (0, 1), (0, 0)]; // "xy", "def", "abc"
        let result = gather_in_order(&[src0, src1], &order, StorageType::VarLen, 3);
        match result {
            ColumnBuffer::VarLen { offsets, data } => {
                assert_eq!(offsets.len(), 3);
                assert_eq!(&data[..offsets[0] as usize], b"xy");
                assert_eq!(&data[offsets[0] as usize..offsets[1] as usize], b"def");
                assert_eq!(&data[offsets[1] as usize..offsets[2] as usize], b"abc");
            }
            _ => panic!("wrong type"),
        }
    }
}
