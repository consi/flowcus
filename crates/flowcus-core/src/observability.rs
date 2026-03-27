//! Production observability: always-on health metrics for Prometheus scraping.
//!
//! These metrics run at zero cost
//! (atomic increments only) and are always available at `/observability/metrics`.
//!
//! Designed for bottleneck detection:
//! - Queue depths reveal backpressure
//! - Error rates reveal failing components
//! - Latency histograms reveal slow paths
//! - Resource gauges reveal capacity limits

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// All production metrics. Single global instance shared across components.
/// Every field is atomic — no locks, no contention.
#[derive(Debug)]
pub struct Metrics {
    // ---- IPFIX listener ----
    pub ipfix_packets_received: AtomicU64,
    pub ipfix_packets_parsed: AtomicU64,
    pub ipfix_packets_errors: AtomicU64,
    pub ipfix_records_decoded: AtomicU64,
    pub ipfix_bytes_received: AtomicU64,
    pub ipfix_templates_active: AtomicI64,
    pub ipfix_tcp_connections: AtomicI64,
    /// Unique unknown IEs encountered (not in registry). Per RFC 5153.
    pub ipfix_unknown_ies: AtomicU64,
    /// Packets saved to .unproc files (no template available).
    pub ipfix_unprocessed_saved: AtomicU64,
    /// Packets successfully reprocessed from .unproc files.
    pub ipfix_unprocessed_reprocessed: AtomicU64,
    /// Packets expired (TTL exceeded, deleted).
    pub ipfix_unprocessed_expired: AtomicU64,
    /// Current count of pending .unproc files.
    pub ipfix_unprocessed_pending: AtomicI64,

    // ---- NetFlow ----
    pub netflow_v9_packets_received: AtomicU64,
    pub netflow_v5_packets_received: AtomicU64,

    // ---- Storage writer (ingestion) ----
    pub writer_records_ingested: AtomicU64,
    pub writer_parts_flushed: AtomicU64,
    pub writer_bytes_flushed: AtomicU64,
    pub writer_flush_errors: AtomicU64,
    pub writer_active_buffers: AtomicI64,
    pub writer_buffer_bytes: AtomicI64,
    pub writer_channel_depth: AtomicI64,
    pub writer_channel_drops: AtomicU64,

    // ---- Merge workers ----
    pub merge_completed: AtomicU64,
    pub merge_failed: AtomicU64,
    pub merge_rows_processed: AtomicU64,
    pub merge_bytes_written: AtomicU64,
    pub merge_parts_removed: AtomicU64,
    pub merge_active_workers: AtomicI64,
    pub merge_pending_hours: AtomicI64,
    pub merge_throttled_count: AtomicU64,
    /// Merge jobs queued but not yet started.
    pub merge_queue_pending: AtomicI64,
    /// Merge jobs currently executing.
    pub merge_queue_active: AtomicI64,
    /// Cumulative hours sealed (converged to 1 part).
    pub merge_queue_sealed: AtomicU64,
    /// Total unmerged parts across all dirty hours.
    pub merge_unmerged_parts: AtomicI64,
    /// Total columns processed across all merges.
    pub merge_columns_processed: AtomicU64,
    /// Cumulative merge job duration in milliseconds.
    pub merge_job_duration_ms: AtomicU64,

    // ---- Storage state ----
    pub storage_parts_total: AtomicI64,
    pub storage_parts_gen0: AtomicI64,
    pub storage_parts_gen1_plus: AtomicI64,
    pub storage_disk_bytes: AtomicI64,

    // ---- Query ----
    pub query_requests: AtomicU64,
    pub query_errors: AtomicU64,
    pub query_cache_hits: AtomicU64,
    pub query_cache_misses: AtomicU64,
    pub query_rows_scanned: AtomicU64,
    pub query_rows_returned: AtomicU64,
    pub query_parts_scanned: AtomicU64,
    pub query_parts_skipped: AtomicU64,
    /// Cumulative query execution time in microseconds (divide by requests for avg).
    pub query_duration_us: AtomicU64,

    // ---- Retention ----
    pub retention_parts_removed: AtomicU64,
    pub retention_passes: AtomicU64,

    // ---- IPFIX exporters ----
    pub ipfix_exporters_active: AtomicI64,

    // ---- Process ----
    pub process_start_time_secs: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Arc::new(Self {
            ipfix_packets_received: AtomicU64::new(0),
            ipfix_packets_parsed: AtomicU64::new(0),
            ipfix_packets_errors: AtomicU64::new(0),
            ipfix_records_decoded: AtomicU64::new(0),
            ipfix_bytes_received: AtomicU64::new(0),
            ipfix_templates_active: AtomicI64::new(0),
            ipfix_tcp_connections: AtomicI64::new(0),
            ipfix_unknown_ies: AtomicU64::new(0),
            ipfix_unprocessed_saved: AtomicU64::new(0),
            ipfix_unprocessed_reprocessed: AtomicU64::new(0),
            ipfix_unprocessed_expired: AtomicU64::new(0),
            ipfix_unprocessed_pending: AtomicI64::new(0),

            netflow_v9_packets_received: AtomicU64::new(0),
            netflow_v5_packets_received: AtomicU64::new(0),

            writer_records_ingested: AtomicU64::new(0),
            writer_parts_flushed: AtomicU64::new(0),
            writer_bytes_flushed: AtomicU64::new(0),
            writer_flush_errors: AtomicU64::new(0),
            writer_active_buffers: AtomicI64::new(0),
            writer_buffer_bytes: AtomicI64::new(0),
            writer_channel_depth: AtomicI64::new(0),
            writer_channel_drops: AtomicU64::new(0),

            merge_completed: AtomicU64::new(0),
            merge_failed: AtomicU64::new(0),
            merge_rows_processed: AtomicU64::new(0),
            merge_bytes_written: AtomicU64::new(0),
            merge_parts_removed: AtomicU64::new(0),
            merge_active_workers: AtomicI64::new(0),
            merge_pending_hours: AtomicI64::new(0),
            merge_throttled_count: AtomicU64::new(0),
            merge_queue_pending: AtomicI64::new(0),
            merge_queue_active: AtomicI64::new(0),
            merge_queue_sealed: AtomicU64::new(0),
            merge_unmerged_parts: AtomicI64::new(0),
            merge_columns_processed: AtomicU64::new(0),
            merge_job_duration_ms: AtomicU64::new(0),

            storage_parts_total: AtomicI64::new(0),
            storage_parts_gen0: AtomicI64::new(0),
            storage_parts_gen1_plus: AtomicI64::new(0),
            storage_disk_bytes: AtomicI64::new(0),

            query_requests: AtomicU64::new(0),
            query_errors: AtomicU64::new(0),
            query_cache_hits: AtomicU64::new(0),
            query_cache_misses: AtomicU64::new(0),
            query_rows_scanned: AtomicU64::new(0),
            query_rows_returned: AtomicU64::new(0),
            query_parts_scanned: AtomicU64::new(0),
            query_parts_skipped: AtomicU64::new(0),
            query_duration_us: AtomicU64::new(0),

            retention_parts_removed: AtomicU64::new(0),
            retention_passes: AtomicU64::new(0),

            ipfix_exporters_active: AtomicI64::new(0),

            process_start_time_secs: AtomicU64::new(now),
        })
    }

    /// Read all metrics as structured key-value pairs.
    pub fn to_json_values(&self) -> Vec<(&'static str, i64)> {
        vec![
            (
                "ipfix_packets_received",
                self.ipfix_packets_received.load(Ordering::Relaxed) as i64,
            ),
            (
                "ipfix_packets_parsed",
                self.ipfix_packets_parsed.load(Ordering::Relaxed) as i64,
            ),
            (
                "ipfix_packets_errors",
                self.ipfix_packets_errors.load(Ordering::Relaxed) as i64,
            ),
            (
                "ipfix_records_decoded",
                self.ipfix_records_decoded.load(Ordering::Relaxed) as i64,
            ),
            (
                "ipfix_bytes_received",
                self.ipfix_bytes_received.load(Ordering::Relaxed) as i64,
            ),
            (
                "ipfix_templates_active",
                self.ipfix_templates_active.load(Ordering::Relaxed),
            ),
            (
                "ipfix_tcp_connections",
                self.ipfix_tcp_connections.load(Ordering::Relaxed),
            ),
            (
                "ipfix_unknown_ies",
                self.ipfix_unknown_ies.load(Ordering::Relaxed) as i64,
            ),
            (
                "ipfix_exporters_active",
                self.ipfix_exporters_active.load(Ordering::Relaxed),
            ),
            (
                "netflow_v9_packets_received",
                self.netflow_v9_packets_received.load(Ordering::Relaxed) as i64,
            ),
            (
                "netflow_v5_packets_received",
                self.netflow_v5_packets_received.load(Ordering::Relaxed) as i64,
            ),
            (
                "writer_records_ingested",
                self.writer_records_ingested.load(Ordering::Relaxed) as i64,
            ),
            (
                "writer_parts_flushed",
                self.writer_parts_flushed.load(Ordering::Relaxed) as i64,
            ),
            (
                "writer_bytes_flushed",
                self.writer_bytes_flushed.load(Ordering::Relaxed) as i64,
            ),
            (
                "writer_flush_errors",
                self.writer_flush_errors.load(Ordering::Relaxed) as i64,
            ),
            (
                "writer_active_buffers",
                self.writer_active_buffers.load(Ordering::Relaxed),
            ),
            (
                "writer_buffer_bytes",
                self.writer_buffer_bytes.load(Ordering::Relaxed),
            ),
            (
                "writer_channel_depth",
                self.writer_channel_depth.load(Ordering::Relaxed),
            ),
            (
                "writer_channel_drops",
                self.writer_channel_drops.load(Ordering::Relaxed) as i64,
            ),
            (
                "merge_completed",
                self.merge_completed.load(Ordering::Relaxed) as i64,
            ),
            (
                "merge_failed",
                self.merge_failed.load(Ordering::Relaxed) as i64,
            ),
            (
                "merge_rows_processed",
                self.merge_rows_processed.load(Ordering::Relaxed) as i64,
            ),
            (
                "merge_bytes_written",
                self.merge_bytes_written.load(Ordering::Relaxed) as i64,
            ),
            (
                "merge_parts_removed",
                self.merge_parts_removed.load(Ordering::Relaxed) as i64,
            ),
            (
                "merge_active_workers",
                self.merge_active_workers.load(Ordering::Relaxed),
            ),
            (
                "merge_pending_hours",
                self.merge_pending_hours.load(Ordering::Relaxed),
            ),
            (
                "merge_throttled",
                self.merge_throttled_count.load(Ordering::Relaxed) as i64,
            ),
            (
                "merge_queue_pending",
                self.merge_queue_pending.load(Ordering::Relaxed),
            ),
            (
                "merge_queue_active",
                self.merge_queue_active.load(Ordering::Relaxed),
            ),
            (
                "merge_queue_sealed",
                self.merge_queue_sealed.load(Ordering::Relaxed) as i64,
            ),
            (
                "merge_unmerged_parts",
                self.merge_unmerged_parts.load(Ordering::Relaxed),
            ),
            (
                "merge_columns_processed",
                self.merge_columns_processed.load(Ordering::Relaxed) as i64,
            ),
            (
                "merge_job_duration_ms",
                self.merge_job_duration_ms.load(Ordering::Relaxed) as i64,
            ),
            (
                "storage_parts_total",
                self.storage_parts_total.load(Ordering::Relaxed),
            ),
            (
                "storage_parts_gen0",
                self.storage_parts_gen0.load(Ordering::Relaxed),
            ),
            (
                "storage_parts_gen1_plus",
                self.storage_parts_gen1_plus.load(Ordering::Relaxed),
            ),
            (
                "storage_disk_bytes",
                self.storage_disk_bytes.load(Ordering::Relaxed),
            ),
            (
                "query_requests",
                self.query_requests.load(Ordering::Relaxed) as i64,
            ),
            (
                "query_errors",
                self.query_errors.load(Ordering::Relaxed) as i64,
            ),
            (
                "query_cache_hits",
                self.query_cache_hits.load(Ordering::Relaxed) as i64,
            ),
            (
                "query_cache_misses",
                self.query_cache_misses.load(Ordering::Relaxed) as i64,
            ),
            (
                "query_rows_scanned",
                self.query_rows_scanned.load(Ordering::Relaxed) as i64,
            ),
            (
                "query_rows_returned",
                self.query_rows_returned.load(Ordering::Relaxed) as i64,
            ),
            (
                "query_parts_scanned",
                self.query_parts_scanned.load(Ordering::Relaxed) as i64,
            ),
            (
                "query_parts_skipped",
                self.query_parts_skipped.load(Ordering::Relaxed) as i64,
            ),
            (
                "query_duration_us",
                self.query_duration_us.load(Ordering::Relaxed) as i64,
            ),
            (
                "retention_parts_removed",
                self.retention_parts_removed.load(Ordering::Relaxed) as i64,
            ),
            (
                "retention_passes",
                self.retention_passes.load(Ordering::Relaxed) as i64,
            ),
            (
                "process_start_time_secs",
                self.process_start_time_secs.load(Ordering::Relaxed) as i64,
            ),
        ]
    }

    /// Format all metrics as Prometheus text exposition.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(4096);

        // Helper closures
        let counter = |out: &mut String, name: &str, help: &str, val: &AtomicU64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {}\n",
                val.load(Ordering::Relaxed)
            ));
        };
        let gauge_u = |out: &mut String, name: &str, help: &str, val: &AtomicU64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {}\n",
                val.load(Ordering::Relaxed)
            ));
        };
        let gauge_i = |out: &mut String, name: &str, help: &str, val: &AtomicI64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {}\n",
                val.load(Ordering::Relaxed)
            ));
        };

        // IPFIX
        counter(
            &mut out,
            "flowcus_ipfix_packets_received_total",
            "Total IPFIX packets received",
            &self.ipfix_packets_received,
        );
        counter(
            &mut out,
            "flowcus_ipfix_packets_parsed_total",
            "Total IPFIX packets successfully parsed",
            &self.ipfix_packets_parsed,
        );
        counter(
            &mut out,
            "flowcus_ipfix_packets_errors_total",
            "Total IPFIX packet parse errors",
            &self.ipfix_packets_errors,
        );
        counter(
            &mut out,
            "flowcus_ipfix_records_decoded_total",
            "Total IPFIX data records decoded",
            &self.ipfix_records_decoded,
        );
        counter(
            &mut out,
            "flowcus_ipfix_bytes_received_total",
            "Total bytes received on IPFIX listeners",
            &self.ipfix_bytes_received,
        );
        gauge_i(
            &mut out,
            "flowcus_ipfix_templates_active",
            "Currently cached IPFIX templates",
            &self.ipfix_templates_active,
        );
        gauge_i(
            &mut out,
            "flowcus_ipfix_tcp_connections",
            "Active IPFIX TCP connections",
            &self.ipfix_tcp_connections,
        );
        counter(
            &mut out,
            "flowcus_ipfix_unknown_ies_total",
            "Unique unknown IEs encountered (not in registry, stored as OctetArray per RFC 5153)",
            &self.ipfix_unknown_ies,
        );
        counter(
            &mut out,
            "flowcus_ipfix_unprocessed_saved_total",
            "Packets saved to .unproc files (no template available)",
            &self.ipfix_unprocessed_saved,
        );
        counter(
            &mut out,
            "flowcus_ipfix_unprocessed_reprocessed_total",
            "Packets successfully reprocessed from .unproc files",
            &self.ipfix_unprocessed_reprocessed,
        );
        counter(
            &mut out,
            "flowcus_ipfix_unprocessed_expired_total",
            "Packets expired (TTL exceeded, deleted)",
            &self.ipfix_unprocessed_expired,
        );
        gauge_i(
            &mut out,
            "flowcus_ipfix_unprocessed_pending",
            "Current count of pending .unproc files",
            &self.ipfix_unprocessed_pending,
        );

        // Writer
        counter(
            &mut out,
            "flowcus_writer_records_ingested_total",
            "Total records ingested into write buffers",
            &self.writer_records_ingested,
        );
        counter(
            &mut out,
            "flowcus_writer_parts_flushed_total",
            "Total parts flushed to disk",
            &self.writer_parts_flushed,
        );
        counter(
            &mut out,
            "flowcus_writer_bytes_flushed_total",
            "Total bytes flushed to disk",
            &self.writer_bytes_flushed,
        );
        counter(
            &mut out,
            "flowcus_writer_flush_errors_total",
            "Total part flush errors",
            &self.writer_flush_errors,
        );
        gauge_i(
            &mut out,
            "flowcus_writer_active_buffers",
            "Number of active write buffers (schema x partition)",
            &self.writer_active_buffers,
        );
        gauge_i(
            &mut out,
            "flowcus_writer_buffer_bytes",
            "Current total bytes in write buffers",
            &self.writer_buffer_bytes,
        );
        gauge_i(
            &mut out,
            "flowcus_writer_channel_depth",
            "Current ingestion channel queue depth (backpressure indicator)",
            &self.writer_channel_depth,
        );
        counter(
            &mut out,
            "flowcus_writer_channel_drops_total",
            "Messages dropped due to full ingestion channel",
            &self.writer_channel_drops,
        );

        // Merge
        counter(
            &mut out,
            "flowcus_merge_completed_total",
            "Total merge operations completed",
            &self.merge_completed,
        );
        counter(
            &mut out,
            "flowcus_merge_failed_total",
            "Total merge operations failed",
            &self.merge_failed,
        );
        counter(
            &mut out,
            "flowcus_merge_rows_processed_total",
            "Total rows processed by merge operations",
            &self.merge_rows_processed,
        );
        counter(
            &mut out,
            "flowcus_merge_bytes_written_total",
            "Total bytes written by merge operations",
            &self.merge_bytes_written,
        );
        counter(
            &mut out,
            "flowcus_merge_parts_removed_total",
            "Total source parts removed after merge",
            &self.merge_parts_removed,
        );
        gauge_i(
            &mut out,
            "flowcus_merge_active_workers",
            "Currently active merge workers",
            &self.merge_active_workers,
        );
        gauge_i(
            &mut out,
            "flowcus_merge_pending_hours",
            "Hour directories waiting for merge",
            &self.merge_pending_hours,
        );
        counter(
            &mut out,
            "flowcus_merge_throttled_total",
            "Times merge was throttled by CPU/memory pressure",
            &self.merge_throttled_count,
        );
        gauge_i(
            &mut out,
            "flowcus_merge_queue_pending",
            "Merge jobs queued but not yet started",
            &self.merge_queue_pending,
        );
        gauge_i(
            &mut out,
            "flowcus_merge_queue_active",
            "Merge jobs currently executing",
            &self.merge_queue_active,
        );
        counter(
            &mut out,
            "flowcus_merge_queue_sealed_total",
            "Hours sealed (converged to single part)",
            &self.merge_queue_sealed,
        );
        gauge_i(
            &mut out,
            "flowcus_merge_unmerged_parts",
            "Total unmerged parts across all dirty hours",
            &self.merge_unmerged_parts,
        );
        counter(
            &mut out,
            "flowcus_merge_columns_processed_total",
            "Total columns processed by merge operations",
            &self.merge_columns_processed,
        );
        counter(
            &mut out,
            "flowcus_merge_job_duration_ms_total",
            "Cumulative merge job duration in milliseconds",
            &self.merge_job_duration_ms,
        );

        // Storage
        gauge_i(
            &mut out,
            "flowcus_storage_parts_total",
            "Total parts on disk",
            &self.storage_parts_total,
        );
        gauge_i(
            &mut out,
            "flowcus_storage_parts_gen0",
            "Raw (generation 0) parts on disk",
            &self.storage_parts_gen0,
        );
        gauge_i(
            &mut out,
            "flowcus_storage_parts_gen1_plus",
            "Merged (generation >= 1) parts on disk",
            &self.storage_parts_gen1_plus,
        );
        gauge_i(
            &mut out,
            "flowcus_storage_disk_bytes",
            "Estimated total storage disk usage",
            &self.storage_disk_bytes,
        );

        // Query
        counter(
            &mut out,
            "flowcus_query_requests_total",
            "Total query requests received",
            &self.query_requests,
        );
        counter(
            &mut out,
            "flowcus_query_errors_total",
            "Total query errors (parse + execution)",
            &self.query_errors,
        );
        counter(
            &mut out,
            "flowcus_query_cache_hits_total",
            "Total query cache hits",
            &self.query_cache_hits,
        );
        counter(
            &mut out,
            "flowcus_query_cache_misses_total",
            "Total query cache misses",
            &self.query_cache_misses,
        );
        counter(
            &mut out,
            "flowcus_query_rows_scanned_total",
            "Total rows scanned across all queries",
            &self.query_rows_scanned,
        );
        counter(
            &mut out,
            "flowcus_query_rows_returned_total",
            "Total rows returned across all queries",
            &self.query_rows_returned,
        );
        counter(
            &mut out,
            "flowcus_query_parts_scanned_total",
            "Total storage parts scanned across all queries",
            &self.query_parts_scanned,
        );
        counter(
            &mut out,
            "flowcus_query_parts_skipped_total",
            "Total storage parts skipped (time pruning, bloom filter)",
            &self.query_parts_skipped,
        );
        counter(
            &mut out,
            "flowcus_query_duration_microseconds_total",
            "Cumulative query execution time in microseconds",
            &self.query_duration_us,
        );

        // Retention
        counter(
            &mut out,
            "flowcus_retention_parts_removed_total",
            "Total parts removed by data retention",
            &self.retention_parts_removed,
        );
        counter(
            &mut out,
            "flowcus_retention_passes_total",
            "Total retention scan passes completed",
            &self.retention_passes,
        );

        // IPFIX exporters
        gauge_i(
            &mut out,
            "flowcus_ipfix_exporters_active",
            "Unique IPFIX exporters with active templates",
            &self.ipfix_exporters_active,
        );

        // Process
        gauge_u(
            &mut out,
            "flowcus_process_start_time_seconds",
            "Unix timestamp when the process started",
            &self.process_start_time_secs,
        );

        // System metrics (read live)
        append_system_metrics(&mut out);

        out
    }
}

fn append_system_metrics(out: &mut String) {
    // Process RSS
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(v) = line.strip_prefix("VmRSS:") {
                let kb = v
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                out.push_str(&format!(
                    "# HELP flowcus_process_rss_bytes Resident set size in bytes\n\
                     # TYPE flowcus_process_rss_bytes gauge\nflowcus_process_rss_bytes {}\n",
                    kb * 1024
                ));
            }
            if let Some(v) = line.strip_prefix("Threads:") {
                if let Ok(n) = v.trim().parse::<i64>() {
                    out.push_str(&format!(
                        "# HELP flowcus_process_threads Number of threads\n\
                         # TYPE flowcus_process_threads gauge\nflowcus_process_threads {n}\n"
                    ));
                }
            }
        }
    }

    // Open FDs
    if let Ok(fds) = std::fs::read_dir("/proc/self/fd") {
        let count = fds.count();
        out.push_str(&format!(
            "# HELP flowcus_process_open_fds Number of open file descriptors\n\
             # TYPE flowcus_process_open_fds gauge\nflowcus_process_open_fds {count}\n"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_format_valid() {
        let m = Metrics::new();
        m.ipfix_packets_received.fetch_add(100, Ordering::Relaxed);
        m.writer_records_ingested.fetch_add(5000, Ordering::Relaxed);
        m.merge_active_workers.store(3, Ordering::Relaxed);

        let output = m.to_prometheus();

        // Verify Prometheus text format structure
        assert!(output.contains("# HELP flowcus_ipfix_packets_received_total"));
        assert!(output.contains("# TYPE flowcus_ipfix_packets_received_total counter"));
        assert!(output.contains("flowcus_ipfix_packets_received_total 100"));
        assert!(output.contains("flowcus_writer_records_ingested_total 5000"));
        assert!(output.contains("flowcus_merge_active_workers 3"));
        assert!(output.contains("flowcus_process_start_time_seconds"));

        // Verify new metric categories are present
        assert!(output.contains("flowcus_query_requests_total 0"));
        assert!(output.contains("flowcus_query_cache_hits_total 0"));
        assert!(output.contains("flowcus_retention_parts_removed_total 0"));
        assert!(output.contains("flowcus_ipfix_exporters_active 0"));
    }

    #[test]
    fn metrics_are_zero_initialized() {
        let m = Metrics::new();
        assert_eq!(m.ipfix_packets_received.load(Ordering::Relaxed), 0);
        assert_eq!(m.merge_active_workers.load(Ordering::Relaxed), 0);
    }
}
