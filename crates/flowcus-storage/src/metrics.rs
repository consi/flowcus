//! Storage engine metrics for profiling.

use std::sync::Arc;

use flowcus_core::profiling::{Counter, Gauge, Metric, MetricReporter};

/// Metrics for the storage writer (ingestion path).
pub struct WriterMetrics {
    pub records_ingested: Counter,
    pub parts_flushed: Counter,
    pub flush_bytes: Counter,
    pub buffer_count: Gauge,
    pub buffer_total_bytes: Gauge,
}

impl WriterMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            records_ingested: Counter::new(),
            parts_flushed: Counter::new(),
            flush_bytes: Counter::new(),
            buffer_count: Gauge::new(),
            buffer_total_bytes: Gauge::new(),
        })
    }
}

impl MetricReporter for WriterMetrics {
    fn component_name(&self) -> &'static str {
        "writer"
    }

    fn collect(&self) -> Vec<Metric> {
        vec![
            Metric {
                name: "records_ingested",
                value: self.records_ingested.take() as f64,
                unit: "rec/window",
            },
            Metric {
                name: "parts_flushed",
                value: self.parts_flushed.take() as f64,
                unit: "parts/window",
            },
            Metric {
                name: "flush_bytes",
                value: self.flush_bytes.take() as f64,
                unit: "bytes/window",
            },
            Metric {
                name: "active_buffers",
                value: self.buffer_count.get() as f64,
                unit: "",
            },
            Metric {
                name: "buffer_total_bytes",
                value: self.buffer_total_bytes.get() as f64,
                unit: "bytes",
            },
        ]
    }
}

/// Metrics for the merge system.
pub struct MergeMetrics {
    pub merges_completed: Counter,
    pub merges_failed: Counter,
    pub rows_merged: Counter,
    pub parts_removed: Counter,
    pub pending_hours: Gauge,
}

impl MergeMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            merges_completed: Counter::new(),
            merges_failed: Counter::new(),
            rows_merged: Counter::new(),
            parts_removed: Counter::new(),
            pending_hours: Gauge::new(),
        })
    }
}

impl MetricReporter for MergeMetrics {
    fn component_name(&self) -> &'static str {
        "merge"
    }

    fn collect(&self) -> Vec<Metric> {
        vec![
            Metric {
                name: "merges_completed",
                value: self.merges_completed.take() as f64,
                unit: "/window",
            },
            Metric {
                name: "merges_failed",
                value: self.merges_failed.take() as f64,
                unit: "/window",
            },
            Metric {
                name: "rows_merged",
                value: self.rows_merged.take() as f64,
                unit: "rows/window",
            },
            Metric {
                name: "parts_removed",
                value: self.parts_removed.take() as f64,
                unit: "/window",
            },
            Metric {
                name: "pending_hours",
                value: self.pending_hours.get() as f64,
                unit: "",
            },
        ]
    }
}

/// Metrics for the IPFIX listener.
pub struct IpfixMetrics {
    pub packets_received: Counter,
    pub packets_parsed: Counter,
    pub packets_failed: Counter,
    pub records_decoded: Counter,
    pub templates_active: Gauge,
}

impl IpfixMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            packets_received: Counter::new(),
            packets_parsed: Counter::new(),
            packets_failed: Counter::new(),
            records_decoded: Counter::new(),
            templates_active: Gauge::new(),
        })
    }
}

impl MetricReporter for IpfixMetrics {
    fn component_name(&self) -> &'static str {
        "ipfix"
    }

    fn collect(&self) -> Vec<Metric> {
        vec![
            Metric {
                name: "packets_received",
                value: self.packets_received.take() as f64,
                unit: "pkt/window",
            },
            Metric {
                name: "packets_parsed",
                value: self.packets_parsed.take() as f64,
                unit: "pkt/window",
            },
            Metric {
                name: "packets_failed",
                value: self.packets_failed.take() as f64,
                unit: "pkt/window",
            },
            Metric {
                name: "records_decoded",
                value: self.records_decoded.take() as f64,
                unit: "rec/window",
            },
            Metric {
                name: "templates_active",
                value: self.templates_active.get() as f64,
                unit: "",
            },
        ]
    }
}
