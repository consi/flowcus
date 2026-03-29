//! Background merge system: compacts parts within time partitions.
//!
//! Architecture:
//! - **Coordinator** (single async task): scans hour directories via an in-memory
//!   priority queue, dispatches merge jobs to `spawn_blocking` tasks.
//! - **Workers** (tokio spawn_blocking): execute CPU-bound merges with per-column
//!   parallelism via rayon. Each column is read, gathered, encoded, and streamed
//!   directly to disk — no cross-column memory accumulation.
//! - **Throttle** (fair scheduler): monitors system load and limits concurrent merges
//!   to avoid starving ingestion.
//!
//! Merge strategy:
//! - One merge job per hour: all active parts in an hour form a single batch.
//! - Priority by backlog: hours with the most unmerged parts are processed first.
//! - Sealed hours: past hours with only 1 part are sealed and never re-scanned.
//! - Generation increments on each merge: 00000 → 00001 → 00002 → ...
//! - Stop at generation 65535 (MAX_GENERATION)
//!
//! Race condition prevention:
//! - Coordinator holds exclusive merge planning authority (single decision maker)
//! - Parts being merged are tracked in the queue's `active` map
//! - New merged part is staged first, then source parts are removed
//! - If a merge fails, source parts remain untouched (crash-safe)

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use rayon::prelude::*;
use tracing::{debug, error, info, trace, warn};

use crate::codec;
use crate::column::ColumnBuffer;
use crate::part::{self, ColumnFileHeader, MAX_GENERATION, PartMetadata};
use crate::schema::{Schema, StorageType};
use crate::table::PartEntry;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

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
    pub queue_length: usize,
    /// Zstd compression level for merged parts.
    pub compression_level: i32,
    /// Minimum parts in an hour before triggering a merge.
    pub min_parts: usize,
    /// Proactively warm cache when an hour is sealed.
    pub preheat_on_seal: bool,
}

// ---------------------------------------------------------------------------
// Merge queue
// ---------------------------------------------------------------------------

/// A queued merge job (not yet started).
struct QueuedJob {
    hour_dir: PathBuf,
    sources: Vec<PartEntry>,
    target_generation: u32,
    time_min: u64,
    time_max: u64,
    schema: Schema,
    base_dir: PathBuf,
    /// Priority = number of source parts. Higher → merge first.
    priority: usize,
}

/// A merge job currently being executed by a worker.
struct ActiveJob {
    source_paths: HashSet<PathBuf>,
    #[allow(dead_code)]
    started_at: Instant,
}

/// In-memory merge queue with priority ordering, refresh of pending items,
/// and sealed-hour tracking.
struct MergeQueue {
    /// Pending jobs keyed by hour directory. NOT yet being processed.
    pending: HashMap<PathBuf, QueuedJob>,
    /// Currently executing jobs. Locked — cannot be refreshed or re-queued.
    active: HashMap<PathBuf, ActiveJob>,
    /// Hours converged to a single part. Never re-scanned until restart.
    sealed: HashSet<PathBuf>,
}

impl MergeQueue {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
            active: HashMap::new(),
            sealed: HashSet::new(),
        }
    }

    /// Returns true if the given source path is part of any active merge job.
    fn is_source_active(&self, path: &Path) -> bool {
        self.active.values().any(|j| j.source_paths.contains(path))
    }

    /// Seal an hour directory so it is never re-scanned.
    fn seal(&mut self, hour_dir: &Path) {
        self.sealed.insert(hour_dir.to_path_buf());
        self.pending.remove(hour_dir);
    }

    /// Enqueue or refresh a merge job for the given hour.
    /// If the hour is already pending, the job is replaced (refreshed).
    fn enqueue_or_refresh(&mut self, job: QueuedJob) {
        self.pending.insert(job.hour_dir.clone(), job);
    }

    /// Dequeue the highest-priority pending job and move it to active.
    /// Returns None if the pending queue is empty.
    fn dequeue_highest_priority(&mut self) -> Option<QueuedJob> {
        let best_hour = self
            .pending
            .iter()
            .max_by_key(|(_, j)| j.priority)
            .map(|(k, _)| k.clone())?;

        let job = self.pending.remove(&best_hour)?;

        let source_paths: HashSet<PathBuf> = job.sources.iter().map(|s| s.path.clone()).collect();
        self.active.insert(
            job.hour_dir.clone(),
            ActiveJob {
                source_paths,
                started_at: Instant::now(),
            },
        );

        Some(job)
    }

    /// Mark a job as completed and remove it from active.
    fn complete(&mut self, hour_dir: &Path) {
        self.active.remove(hour_dir);
    }
}

// ---------------------------------------------------------------------------
// Coordinator entry point
// ---------------------------------------------------------------------------

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
    pending.rebuild_from_disk(&table_base);

    info!(
        workers = config.workers,
        scan_interval_secs = config.scan_interval.as_secs(),
        cpu_throttle = config.cpu_throttle,
        queue_length = config.queue_length,
        pending_hours = pending.count(),
        "Merge coordinator starting"
    );

    tokio::spawn(async move {
        run_coordinator(
            table_base,
            config,
            pending,
            metrics,
            storage_cache,
            part_locks,
        )
        .await;
    });
}

// ---------------------------------------------------------------------------
// Coordinator loop
// ---------------------------------------------------------------------------

/// The coordinator loop: scans, enqueues, dispatches, processes results.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn run_coordinator(
    table_base: PathBuf,
    config: MergeConfig,
    pending: crate::pending::PendingHours,
    metrics: Arc<flowcus_core::observability::Metrics>,
    storage_cache: Arc<crate::cache::StorageCache>,
    part_locks: crate::part_locks::PartLocks,
) {
    // Recover any merges that were interrupted by a previous crash/restart.
    recover_interrupted_merges(&table_base);

    let queue = Arc::new(std::sync::Mutex::new(MergeQueue::new()));
    let seq = Arc::new(AtomicU32::new(1));

    let mut throttle =
        ThrottleController::new(config.workers, config.cpu_throttle, config.mem_throttle);

    let mut interval = tokio::time::interval(config.scan_interval);

    // Track running merge tasks
    let mut handles: Vec<(PathBuf, tokio::task::JoinHandle<Option<MergeJobResult>>)> = Vec::new();

    loop {
        interval.tick().await;

        // ── 1. Drain completed merge tasks ──
        let mut still_running = Vec::new();
        for (hour_dir, handle) in std::mem::take(&mut handles) {
            if handle.is_finished() {
                match handle.await {
                    Ok(Some(result)) => {
                        process_merge_result(
                            &result,
                            &queue,
                            &pending,
                            &metrics,
                            &storage_cache,
                            config.preheat_on_seal,
                        );
                    }
                    Ok(None) => {
                        // Task was cancelled or returned None
                        queue.lock().unwrap().complete(&hour_dir);
                    }
                    Err(e) => {
                        error!(error = %e, hour = %hour_dir.display(), "Merge task panicked");
                        metrics.merge_failed.fetch_add(1, Ordering::Relaxed);
                        queue.lock().unwrap().complete(&hour_dir);
                    }
                }
            } else {
                still_running.push((hour_dir, handle));
            }
        }
        handles = still_running;

        // ── 2. Prune stale entries and scan dirty hours into queue ──
        pending.prune_stale();
        part_locks.prune();

        scan_and_enqueue(
            &table_base,
            &queue,
            &pending,
            &metrics,
            config.partition_duration_secs,
            config.queue_length,
            config.min_parts,
            &storage_cache,
            config.preheat_on_seal,
        );

        // Update metrics
        {
            let q = queue.lock().unwrap();
            metrics
                .merge_queue_pending
                .store(q.pending.len() as i64, Ordering::Relaxed);
            metrics
                .merge_queue_active
                .store(q.active.len() as i64, Ordering::Relaxed);
            metrics
                .merge_pending_hours
                .store(pending.count() as i64, Ordering::Relaxed);
        }

        // ── 3. Recompute throttle and dispatch new jobs ──
        let slots = throttle.recompute(&metrics);
        let active_count = queue.lock().unwrap().active.len();
        let available = slots.saturating_sub(active_count);

        for _ in 0..available {
            let job = {
                let mut q = queue.lock().unwrap();
                q.dequeue_highest_priority()
            };

            let Some(job) = job else {
                break;
            };

            let hour_dir = job.hour_dir.clone();
            let seq = Arc::clone(&seq);
            let locks = part_locks.clone();
            let m = Arc::clone(&metrics);
            let granule_size = config.granule_size;
            let bloom_bits = config.bloom_bits;
            let compression_level = config.compression_level;

            let handle = tokio::task::spawn_blocking(move || {
                m.merge_active_workers.fetch_add(1, Ordering::Relaxed);
                let started = Instant::now();

                let source_paths: Vec<PathBuf> =
                    job.sources.iter().map(|s| s.path.clone()).collect();
                let source_count = source_paths.len();
                let total_rows = job_total_rows(&job);
                let target_generation = job.target_generation;
                let hour = job.hour_dir.clone();

                let outcome = execute_merge(
                    &job,
                    &seq,
                    granule_size,
                    bloom_bits,
                    compression_level,
                    &locks,
                    &m,
                );

                let duration_ms = started.elapsed().as_millis() as u64;
                m.merge_active_workers.fetch_sub(1, Ordering::Relaxed);
                m.merge_job_duration_ms
                    .fetch_add(duration_ms, Ordering::Relaxed);

                Some(MergeJobResult {
                    hour_dir: hour,
                    source_paths,
                    source_count,
                    total_rows,
                    target_generation,
                    outcome,
                })
            });

            handles.push((hour_dir, handle));
        }
    }
}

/// Result from a completed merge job.
struct MergeJobResult {
    hour_dir: PathBuf,
    source_paths: Vec<PathBuf>,
    source_count: usize,
    total_rows: u64,
    target_generation: u32,
    outcome: std::io::Result<(PathBuf, u64)>,
}

/// Total rows across all source parts in a job.
fn job_total_rows(job: &QueuedJob) -> u64 {
    job.sources
        .iter()
        .filter_map(|s| part::read_meta_bin(&s.path.join("meta.bin")).ok())
        .map(|h| h.row_count)
        .sum()
}

/// Process a completed merge result: update metrics, cache, pending, queue.
fn process_merge_result(
    result: &MergeJobResult,
    queue: &std::sync::Mutex<MergeQueue>,
    pending: &crate::pending::PendingHours,
    metrics: &flowcus_core::observability::Metrics,
    storage_cache: &crate::cache::StorageCache,
    preheat_on_seal: bool,
) {
    // Release from active
    let mut q = queue.lock().unwrap();
    q.complete(&result.hour_dir);

    match &result.outcome {
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
                .fetch_add(*merge_bytes, Ordering::Relaxed);

            for path in &result.source_paths {
                storage_cache.invalidate_part(path);
            }

            info!(
                new_part = %new_part.display(),
                sources = result.source_count,
                generation = result.target_generation,
                "Merge completed"
            );

            // Check if hour is now fully merged
            let remaining = crate::pending::count_parts_in(&result.hour_dir);
            if remaining <= 1 {
                q.seal(&result.hour_dir);
                pending.mark_clean(&result.hour_dir);
                metrics.merge_queue_sealed.fetch_add(1, Ordering::Relaxed);

                if preheat_on_seal {
                    let loaded = storage_cache.preheat_part(new_part);
                    if loaded > 0 {
                        debug!(part = %new_part.display(), files = loaded, "Pre-heated cache for sealed part");
                    }
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

// ---------------------------------------------------------------------------
// Queue population: scan dirty hours
// ---------------------------------------------------------------------------

/// Scan pending (dirty) hours and populate/refresh the merge queue.
#[allow(clippy::too_many_arguments)]
fn scan_and_enqueue(
    table_base: &Path,
    queue: &std::sync::Mutex<MergeQueue>,
    pending: &crate::pending::PendingHours,
    metrics: &flowcus_core::observability::Metrics,
    _partition_duration_secs: u32,
    queue_length: usize,
    min_parts: usize,
    storage_cache: &crate::cache::StorageCache,
    preheat_on_seal: bool,
) {
    let dirty_hours = pending.get_dirty();
    if dirty_hours.is_empty() {
        return;
    }

    trace!(
        dirty_hours = dirty_hours.len(),
        "Scanning pending hours for merge candidates"
    );

    let mut q = queue.lock().unwrap();

    for hour_dir in &dirty_hours {
        // Skip sealed or currently-active hours
        if q.sealed.contains(hour_dir) {
            pending.mark_clean(hour_dir);
            continue;
        }
        if q.active.contains_key(hour_dir) {
            continue;
        }

        if !hour_dir.exists() {
            pending.mark_clean(hour_dir);
            continue;
        }

        // Discover valid parts in this hour
        let parts = match discover_parts(hour_dir, &q) {
            Ok(p) => p,
            Err(e) => {
                debug!(error = %e, hour = %hour_dir.display(), "Failed to scan hour directory");
                continue;
            }
        };

        // Fewer than min_parts → seal this hour (nothing to merge yet)
        if parts.len() < min_parts {
            q.seal(hour_dir);
            pending.mark_clean(hour_dir);
            metrics.merge_queue_sealed.fetch_add(1, Ordering::Relaxed);

            if preheat_on_seal {
                if let Some(part) = parts.first() {
                    let loaded = storage_cache.preheat_part(&part.path);
                    if loaded > 0 {
                        debug!(part = %part.path.display(), files = loaded, "Pre-heated cache for sealed part");
                    }
                }
            }
            continue;
        }

        // Read schema from the highest-generation part
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
        let priority = parts.len();

        // Enqueue or refresh
        q.enqueue_or_refresh(QueuedJob {
            hour_dir: hour_dir.clone(),
            sources: parts,
            target_generation: target_gen,
            time_min,
            time_max,
            schema,
            base_dir: table_base.to_path_buf(),
            priority,
        });

        // Cap queue length
        while q.pending.len() > queue_length {
            // Drop the lowest-priority entry
            if let Some(lowest) = q
                .pending
                .iter()
                .min_by_key(|(_, j)| j.priority)
                .map(|(k, _)| k.clone())
            {
                q.pending.remove(&lowest);
            }
        }
    }
}

/// Discover valid, non-active parts in an hour directory.
/// Verifies meta.bin exists and CRC checks before including.
fn discover_parts(hour_dir: &Path, queue: &MergeQueue) -> std::io::Result<Vec<PartEntry>> {
    let entries = std::fs::read_dir(hour_dir)?;
    let mut parts = Vec::new();

    for entry in entries {
        let Ok(entry) = entry else { continue };
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

            // Skip parts in active merge jobs
            if queue.is_source_active(&path) || generation >= MAX_GENERATION {
                continue;
            }

            // Verify meta.bin exists and is readable (catches incomplete parts)
            let meta_path = path.join("meta.bin");
            if part::read_meta_bin(&meta_path).is_err() {
                debug!(path = %path.display(), "Skipping part with unreadable meta.bin");
                continue;
            }

            parts.push(PartEntry {
                path,
                format_version,
                generation,
                time_min: pmin,
                time_max: pmax,
                seq,
            });
        }
    }

    parts.sort_by_key(|p| (p.generation, p.time_min, p.seq));
    Ok(parts)
}

// ---------------------------------------------------------------------------
// Merge execution (per-column parallelism, streaming to disk)
// ---------------------------------------------------------------------------

/// Execute a streaming merge: read rowids to build merge order, then process
/// columns in parallel via rayon — each column streams directly to disk.
/// Returns the new part directory and total bytes written on success.
#[allow(clippy::too_many_lines)]
fn execute_merge(
    job: &QueuedJob,
    seq: &AtomicU32,
    granule_size: usize,
    bloom_bits: usize,
    compression_level: i32,
    part_locks: &crate::part_locks::PartLocks,
    metrics: &flowcus_core::observability::Metrics,
) -> std::io::Result<(PathBuf, u64)> {
    let src_count = job.sources.len();

    debug!(
        sources = src_count,
        generation = job.target_generation,
        time_min = job.time_min,
        time_max = job.time_max,
        columns = job.schema.columns.len(),
        "Starting streaming merge"
    );

    // Read row counts from source parts
    let source_rows: Vec<u64> = job
        .sources
        .iter()
        .filter_map(|s| part::read_meta_bin(&s.path.join("meta.bin")).ok())
        .map(|h| h.row_count)
        .collect();
    let total_rows: u64 = source_rows.iter().sum();

    // Determine effective schema (may insert flowcusRowId for v1 parts)
    let has_rowid = job.schema.columns.iter().any(|c| c.name == "flowcusRowId");

    let effective_schema = if has_rowid {
        job.schema.clone()
    } else {
        // v1 parts: insert flowcusRowId column at index 4
        let mut cols = job.schema.columns.clone();
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
            duration_source: job.schema.duration_source,
        }
    };

    // Phase 1: Build merge order from rowids only (memory-efficient).
    let merge_order = build_merge_order(job, &source_rows, has_rowid)?;

    // Phase 2: Stream columns in parallel to disk.
    let merge_seq = seq.fetch_add(1, Ordering::Relaxed);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let first_source = &job.sources[0];
    let source_meta = part::read_meta_bin(&first_source.path.join("meta.bin"))?;

    let metadata = PartMetadata {
        row_count: total_rows,
        generation: job.target_generation,
        time_min: job.time_min,
        time_max: job.time_max,
        observation_domain_id: source_meta.observation_domain_id,
        created_at_ms: now,
        disk_bytes: 0,
        column_count: effective_schema.columns.len() as u32,
        schema_fingerprint: effective_schema.fingerprint(),
        exporter: read_exporter_from_meta(&first_source.path)?,
        schema: effective_schema.clone(),
    };

    let mut writer = part::PartWriter::new(&job.base_dir, &metadata, merge_seq)?;
    let col_dir = writer.col_dir().to_path_buf();

    // Per-column parallel processing — each thread reads, gathers, encodes,
    // and writes directly to disk. merge_order is shared read-only.
    let merge_order_ref = &merge_order;
    let sources_ref = &job.sources;
    let source_rows_ref = &source_rows;
    let schema_ref = &job.schema;

    let column_results: Vec<
        std::io::Result<(crate::schema::ColumnDef, crate::codec::EncodedColumn, u64)>,
    > = effective_schema
        .columns
        .par_iter()
        .map(|col_def| {
            // 1. Read + gather
            let merged_buf = if col_def.name == "flowcusRowId" && !has_rowid {
                build_generated_rowid_column_from_job(
                    sources_ref,
                    source_rows_ref,
                    schema_ref,
                    merge_order_ref,
                )?
            } else {
                let source_bufs = read_column_from_sources(col_def, sources_ref, source_rows_ref)?;
                gather_in_order(
                    &source_bufs,
                    merge_order_ref,
                    col_def.storage_type,
                    total_rows as usize,
                )
            };

            // 2. Encode + compute granules
            let encoded = codec::encode_v2_level(&merged_buf, col_def, compression_level);
            let (marks, blooms) = crate::granule::compute_granules(
                &merged_buf,
                &encoded.data,
                granule_size,
                bloom_bits,
                col_def.storage_type,
            );
            let col_bytes = (part::COLUMN_HEADER_SIZE + encoded.data.len()) as u64;

            // 3. Stream to disk (merged_buf dropped after encode)
            let name = &col_def.name;
            part::write_column_file(
                &col_dir.join(format!("{name}.col")),
                col_def,
                &encoded,
                total_rows,
            )?;
            crate::granule::write_marks(
                &col_dir.join(format!("{name}.mrk")),
                &marks,
                granule_size,
            )?;
            crate::granule::write_blooms(
                &col_dir.join(format!("{name}.bloom")),
                &blooms,
                bloom_bits,
            )?;

            Ok((col_def.clone(), encoded, col_bytes))
        })
        .collect();

    // Collect index entries sequentially (cheap metadata)
    let mut disk_bytes: u64 = 0;
    let mut columns_processed: u64 = 0;
    for result in column_results {
        let (def, encoded, bytes) = result?;
        disk_bytes += bytes;
        columns_processed += 1;
        writer.add_index_entry(def, encoded);
    }

    metrics
        .merge_columns_processed
        .fetch_add(columns_processed, Ordering::Relaxed);

    // Phase 3: Finalize — write metadata, fsync, atomic rename.
    let mut final_metadata = metadata;
    final_metadata.disk_bytes = disk_bytes;
    let new_part_dir = writer.finish(&final_metadata)?;

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
    commit_merge(&new_part_dir, &job.sources, part_locks)?;

    info!(
        new_part = %new_part_dir.display(),
        rows = total_rows,
        generation = job.target_generation,
        sources_removed = src_count,
        "Merge committed"
    );

    Ok((new_part_dir, disk_bytes))
}

// ---------------------------------------------------------------------------
// Merge order (Phase 1) — rowid-only k-way merge
// ---------------------------------------------------------------------------

/// Build the global merge order by reading only rowid columns.
/// Returns `Vec<(u8, u32)>` where each entry is (source_index, row_within_source),
/// representing the globally sorted sequence of rows.
fn build_merge_order(
    job: &QueuedJob,
    source_rows: &[u64],
    has_rowid: bool,
) -> std::io::Result<Vec<(u8, u32)>> {
    let total_rows: usize = source_rows.iter().map(|r| *r as usize).sum();

    // Read rowids from each source
    let mut source_rowids: Vec<Vec<[u64; 2]>> = Vec::with_capacity(job.sources.len());

    if has_rowid {
        for (i, source) in job.sources.iter().enumerate() {
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
                    if let Some(et_idx) = job
                        .schema
                        .columns
                        .iter()
                        .position(|c| c.name == "flowcusExportTime")
                    {
                        let timestamps =
                            read_export_times(source, rows, &job.schema.columns[et_idx])?;
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
                let rowids = generate_rowids_for_source(source, rows, &job.schema)?;
                source_rowids.push(rowids);
            }
        }
    } else {
        // All v1 parts: generate rowids from export_time for each source
        for (i, source) in job.sources.iter().enumerate() {
            let rows = source_rows.get(i).copied().unwrap_or(0) as usize;
            let rowids = generate_rowids_for_source(source, rows, &job.schema)?;
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
fn build_generated_rowid_column_from_job(
    sources: &[PartEntry],
    source_rows: &[u64],
    schema: &Schema,
    merge_order: &[(u8, u32)],
) -> std::io::Result<ColumnBuffer> {
    let total = merge_order.len();
    let mut source_rowids: Vec<Vec<[u64; 2]>> = Vec::with_capacity(sources.len());
    for (i, source) in sources.iter().enumerate() {
        let rows = source_rows.get(i).copied().unwrap_or(0) as usize;
        let rowids = generate_rowids_for_source(source, rows, schema)?;
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

// ---------------------------------------------------------------------------
// Column I/O helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Commit (Phase 3) — atomic source deletion
// ---------------------------------------------------------------------------

/// Commit a completed merge: write .merging marker, delete sources, clean up.
fn commit_merge(
    new_part_dir: &Path,
    sources: &[PartEntry],
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
            for source in sources {
                writeln!(f, "{}", source.path.display())?;
            }
            f.sync_all()?;
        }
        fsync_dir(new_part_dir)?;

        // Delete source parts with write locks.
        for source in sources {
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

// ---------------------------------------------------------------------------
// Crash recovery
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Column decode helpers
// ---------------------------------------------------------------------------

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

    match st {
        StorageType::VarLen => {
            append_varlen_raw(buf, data, rows)?;
        }
        _ => {
            let elem_size = st.element_size().unwrap_or(1);
            let plain = if codec == 0 {
                data.to_vec()
            } else {
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

// ---------------------------------------------------------------------------
// Schema / metadata helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Throttle controller
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Test helpers (integration test API)
// ---------------------------------------------------------------------------

/// Merge all parts in a given hour directory into a single part.
/// Exposed for integration tests only.
#[doc(hidden)]
pub fn merge_hour_for_test(
    table_base: &Path,
    hour_dir: &Path,
    granule_size: usize,
    bloom_bits: usize,
) -> std::io::Result<PathBuf> {
    let seq = AtomicU32::new(1);
    let locks = crate::part_locks::PartLocks::new();
    let metrics = flowcus_core::observability::Metrics::new();

    let queue = MergeQueue::new();
    let parts = discover_parts(hour_dir, &queue)?;
    if parts.len() < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Need at least 2 parts to merge",
        ));
    }

    let best = parts.iter().max_by_key(|p| p.generation).unwrap();
    let schema = read_part_schema(&best.path)?;
    let max_gen = parts.iter().map(|p| p.generation).max().unwrap();
    let time_min = parts.iter().map(|p| p.time_min).min().unwrap();
    let time_max = parts.iter().map(|p| p.time_max).max().unwrap();

    let job = QueuedJob {
        hour_dir: hour_dir.to_path_buf(),
        sources: parts,
        target_generation: max_gen + 1,
        time_min,
        time_max,
        schema,
        base_dir: table_base.to_path_buf(),
        priority: 0,
    };

    let (new_part, _bytes) = execute_merge(
        &job,
        &seq,
        granule_size,
        bloom_bits,
        crate::codec::DEFAULT_ZSTD_LEVEL,
        &locks,
        &metrics,
    )?;
    Ok(new_part)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_delta_roundtrip() {
        let values = [100u32, 105, 110, 115, 120];
        let mut data = Vec::new();
        data.extend_from_slice(&100u32.to_le_bytes());
        for i in 1..values.len() {
            let delta = values[i].wrapping_sub(values[i - 1]);
            data.extend_from_slice(&delta.to_le_bytes());
        }

        let decoded = decode_delta(&data, 4, 5);
        for (i, &expected) in values.iter().enumerate() {
            let got = u32::from_ne_bytes(decoded[i * 4..i * 4 + 4].try_into().unwrap());
            assert_eq!(got, expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn decode_gcd_roundtrip() {
        let mut data = Vec::new();
        data.extend_from_slice(&100u32.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&3u32.to_le_bytes());

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

    // ---- Throttle controller tests ----

    #[test]
    fn throttle_ramp_down() {
        let metrics = flowcus_core::observability::Metrics::new();
        let mut tc = ThrottleController::new(4, 0.0, 0.0);
        assert_eq!(tc.recompute(&metrics), 2);
        assert_eq!(tc.recompute(&metrics), 1);
        assert_eq!(tc.recompute(&metrics), 1);
    }

    #[test]
    fn throttle_ramp_up() {
        let metrics = flowcus_core::observability::Metrics::new();
        let mut tc = ThrottleController::new(4, 1.0, 1.0);
        tc.current_slots = 1;
        assert_eq!(tc.recompute(&metrics), 2);
        assert_eq!(tc.recompute(&metrics), 4);
        assert_eq!(tc.recompute(&metrics), 4);
    }

    #[test]
    fn throttle_never_zero() {
        let metrics = flowcus_core::observability::Metrics::new();
        let mut tc = ThrottleController::new(1, 0.0, 0.0);
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
        let expected: Vec<(u8, u32)> = vec![(0, 0), (1, 0), (0, 1), (1, 1), (0, 2), (1, 2)];
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
        let src0 = ColumnBuffer::VarLen {
            offsets: vec![3, 6],
            data: b"abcdef".to_vec(),
        };
        let src1 = ColumnBuffer::VarLen {
            offsets: vec![2],
            data: b"xy".to_vec(),
        };
        let order = vec![(1u8, 0u32), (0, 1), (0, 0)];
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

    // ---- MergeQueue tests ----

    #[test]
    fn queue_dequeue_highest_priority() {
        let mut q = MergeQueue::new();

        q.enqueue_or_refresh(QueuedJob {
            hour_dir: PathBuf::from("/hours/01"),
            sources: vec![make_entry("/hours/01/a"), make_entry("/hours/01/b")],
            target_generation: 1,
            time_min: 100,
            time_max: 200,
            schema: Schema {
                columns: vec![],
                duration_source: None,
            },
            base_dir: PathBuf::from("/base"),
            priority: 2,
        });

        q.enqueue_or_refresh(QueuedJob {
            hour_dir: PathBuf::from("/hours/02"),
            sources: vec![
                make_entry("/hours/02/a"),
                make_entry("/hours/02/b"),
                make_entry("/hours/02/c"),
                make_entry("/hours/02/d"),
            ],
            target_generation: 1,
            time_min: 100,
            time_max: 200,
            schema: Schema {
                columns: vec![],
                duration_source: None,
            },
            base_dir: PathBuf::from("/base"),
            priority: 4,
        });

        // Should dequeue hour/02 first (priority 4 > 2)
        let job = q.dequeue_highest_priority().unwrap();
        assert_eq!(job.hour_dir, PathBuf::from("/hours/02"));
        assert_eq!(job.priority, 4);
        assert!(q.active.contains_key(&PathBuf::from("/hours/02")));

        // Next should be hour/01
        let job = q.dequeue_highest_priority().unwrap();
        assert_eq!(job.hour_dir, PathBuf::from("/hours/01"));

        // Queue empty
        assert!(q.dequeue_highest_priority().is_none());
    }

    #[test]
    fn queue_refresh_updates_pending() {
        let mut q = MergeQueue::new();

        q.enqueue_or_refresh(QueuedJob {
            hour_dir: PathBuf::from("/hours/01"),
            sources: vec![make_entry("/hours/01/a"), make_entry("/hours/01/b")],
            target_generation: 1,
            time_min: 100,
            time_max: 200,
            schema: Schema {
                columns: vec![],
                duration_source: None,
            },
            base_dir: PathBuf::from("/base"),
            priority: 2,
        });

        // Refresh with more parts
        q.enqueue_or_refresh(QueuedJob {
            hour_dir: PathBuf::from("/hours/01"),
            sources: vec![
                make_entry("/hours/01/a"),
                make_entry("/hours/01/b"),
                make_entry("/hours/01/c"),
            ],
            target_generation: 1,
            time_min: 100,
            time_max: 200,
            schema: Schema {
                columns: vec![],
                duration_source: None,
            },
            base_dir: PathBuf::from("/base"),
            priority: 3,
        });

        let job = q.dequeue_highest_priority().unwrap();
        assert_eq!(job.sources.len(), 3);
        assert_eq!(job.priority, 3);
    }

    #[test]
    fn queue_sealed_hour_skipped() {
        let mut q = MergeQueue::new();

        q.seal(Path::new("/hours/01"));

        // Attempting to enqueue for a sealed hour still puts it in pending
        // (the scan_and_enqueue function checks sealed before enqueuing)
        assert!(q.sealed.contains(Path::new("/hours/01")));
    }

    #[test]
    fn queue_active_job_blocks_source() {
        let mut q = MergeQueue::new();

        q.enqueue_or_refresh(QueuedJob {
            hour_dir: PathBuf::from("/hours/01"),
            sources: vec![make_entry("/hours/01/a"), make_entry("/hours/01/b")],
            target_generation: 1,
            time_min: 100,
            time_max: 200,
            schema: Schema {
                columns: vec![],
                duration_source: None,
            },
            base_dir: PathBuf::from("/base"),
            priority: 2,
        });

        let _job = q.dequeue_highest_priority().unwrap();

        // Source paths from active job should be detected
        assert!(q.is_source_active(Path::new("/hours/01/a")));
        assert!(q.is_source_active(Path::new("/hours/01/b")));
        assert!(!q.is_source_active(Path::new("/hours/01/c")));
    }

    #[test]
    fn queue_complete_removes_active() {
        let mut q = MergeQueue::new();

        q.enqueue_or_refresh(QueuedJob {
            hour_dir: PathBuf::from("/hours/01"),
            sources: vec![make_entry("/hours/01/a")],
            target_generation: 1,
            time_min: 100,
            time_max: 200,
            schema: Schema {
                columns: vec![],
                duration_source: None,
            },
            base_dir: PathBuf::from("/base"),
            priority: 1,
        });

        let _job = q.dequeue_highest_priority().unwrap();
        assert!(q.active.contains_key(&PathBuf::from("/hours/01")));

        q.complete(Path::new("/hours/01"));
        assert!(!q.active.contains_key(&PathBuf::from("/hours/01")));
    }

    #[test]
    fn queue_empty_returns_none() {
        let mut q = MergeQueue::new();
        assert!(q.dequeue_highest_priority().is_none());
    }

    fn make_entry(path: &str) -> PartEntry {
        PartEntry {
            path: PathBuf::from(path),
            format_version: 2,
            generation: 0,
            time_min: 100,
            time_max: 200,
            seq: 1,
        }
    }
}
