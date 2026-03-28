//! Production observability: always-on health metrics for Prometheus scraping.
//!
//! Completely separate from dev profiling. These metrics run at zero cost
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

    // ---- Storage state ----
    pub storage_parts_total: AtomicI64,
    pub storage_parts_gen0: AtomicI64,
    pub storage_parts_gen1_plus: AtomicI64,
    pub storage_disk_bytes: AtomicI64,

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

            storage_parts_total: AtomicI64::new(0),
            storage_parts_gen0: AtomicI64::new(0),
            storage_parts_gen1_plus: AtomicI64::new(0),
            storage_disk_bytes: AtomicI64::new(0),

            process_start_time_secs: AtomicU64::new(now),
        })
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
    }

    #[test]
    fn metrics_are_zero_initialized() {
        let m = Metrics::new();
        assert_eq!(m.ipfix_packets_received.load(Ordering::Relaxed), 0);
        assert_eq!(m.merge_active_workers.load(Ordering::Relaxed), 0);
    }
}
