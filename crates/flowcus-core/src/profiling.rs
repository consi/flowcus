//! Dev-mode performance profiling.
//!
//! When dev mode is enabled, captures 10-second window performance snapshots
//! from every component and writes them to `profiling/` as compact structured
//! text files designed for LLM analysis.
//!
//! # File naming convention
//!
//! `{component}_{unix_ts}.prof`
//!
//! Components: `ipfix`, `storage`, `merge`, `writer`, `server`, `system`
//!
//! Files are intentionally small and self-contained so an LLM can read a few
//! of them without blowing up its context window. Each file is a single
//! snapshot — not appended to.
//!
//! # Usage
//!
//! Components register metric reporters via `Profiler::register()`. The profiler
//! collects from all reporters every 10 seconds and writes the dumps.
//! Analysis is NOT automatic — only done on explicit user request to Claude.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tracing::{debug, info, warn};

/// A single numeric metric with a name.
#[derive(Debug, Clone)]
pub struct Metric {
    pub name: &'static str,
    pub value: f64,
    pub unit: &'static str,
}

/// Snapshot from one component for one profiling window.
#[derive(Debug, Clone)]
pub struct ComponentSnapshot {
    pub component: &'static str,
    pub metrics: Vec<Metric>,
}

/// Trait for components to report their current metrics.
pub trait MetricReporter: Send + Sync {
    fn component_name(&self) -> &'static str;
    fn collect(&self) -> Vec<Metric>;
}

/// Atomic counter that components increment. The profiler reads and resets.
#[derive(Debug)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub const fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Read and reset to zero. Returns the value before reset.
    pub fn take(&self) -> u64 {
        self.value.swap(0, Ordering::Relaxed)
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Atomic gauge (current value, not accumulated).
#[derive(Debug)]
pub struct Gauge {
    value: AtomicU64,
}

impl Gauge {
    pub const fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    pub fn set(&self, v: u64) {
        self.value.store(v, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// The profiler. Collects metrics and writes dumps in dev mode.
pub struct Profiler {
    dir: PathBuf,
    reporters: Vec<Arc<dyn MetricReporter>>,
    window_secs: u64,
    enabled: AtomicBool,
}

impl Profiler {
    /// Create a new profiler writing to the given directory.
    pub fn new(dir: &Path, window_secs: u64) -> Self {
        Self {
            dir: dir.to_path_buf(),
            reporters: Vec::new(),
            window_secs,
            enabled: AtomicBool::new(false),
        }
    }

    /// Register a metric reporter.
    pub fn register(&mut self, reporter: Arc<dyn MetricReporter>) {
        self.reporters.push(reporter);
    }

    /// Enable profiling. Creates the output directory.
    pub fn enable(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        self.enabled.store(true, Ordering::Relaxed);
        info!(dir = %self.dir.display(), window_secs = self.window_secs, "Profiling enabled");
        Ok(())
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
}

/// Start the profiling background task. Only runs if dev_mode is true.
pub fn start(profiler: Arc<std::sync::Mutex<Profiler>>) {
    let p = profiler.lock().unwrap();
    if !p.is_enabled() {
        return;
    }
    let window = Duration::from_secs(p.window_secs);
    let dir = p.dir.clone();
    let reporters: Vec<Arc<dyn MetricReporter>> = p.reporters.clone();
    drop(p);

    tokio::spawn(async move {
        run_profiling_loop(dir, reporters, window).await;
    });
}

async fn run_profiling_loop(
    dir: PathBuf,
    reporters: Vec<Arc<dyn MetricReporter>>,
    window: Duration,
) {
    let mut interval = tokio::time::interval(window);

    loop {
        interval.tick().await;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Collect system-level metrics
        let sys = collect_system_metrics();
        write_snapshot(&dir, "system", ts, &sys);

        // Flush accumulated stack traces for this window
        flush_stacks(&dir, ts);

        // Collect from each registered component
        for reporter in &reporters {
            let metrics = reporter.collect();
            if !metrics.is_empty() {
                write_snapshot(&dir, reporter.component_name(), ts, &metrics);
            }
        }

        debug!(reporters = reporters.len(), "Profiling snapshot written");

        // Garbage collect old profiles (keep last 1 hour = 360 files per component)
        gc_old_profiles(&dir, 360 * (reporters.len() + 1));
    }
}

fn write_snapshot(dir: &Path, component: &str, ts: u64, metrics: &[Metric]) {
    let filename = format!("{component}_{ts}.prof");
    let path = dir.join(&filename);

    // Compact format: one line per metric, easy for LLM to parse
    // Component: name
    // ---
    // metric_name: value unit
    let mut content = String::with_capacity(256);
    content.push_str(&format!("component: {component}\n"));
    content.push_str(&format!("timestamp: {ts}\n"));
    content.push_str("---\n");

    for m in metrics {
        if m.value == m.value.floor() && m.value.abs() < 1e15 {
            content.push_str(&format!("{}: {} {}\n", m.name, m.value as i64, m.unit));
        } else {
            content.push_str(&format!("{}: {:.2} {}\n", m.name, m.value, m.unit));
        }
    }

    if let Err(e) = std::fs::write(&path, content.as_bytes()) {
        warn!(error = %e, file = %filename, "Failed to write profiling snapshot");
    }
}

fn collect_system_metrics() -> Vec<Metric> {
    let mut metrics = Vec::with_capacity(8);

    // CPU load
    if let Ok(loadavg) = std::fs::read_to_string("/proc/loadavg") {
        if let Some(load) = loadavg.split_whitespace().next() {
            if let Ok(v) = load.parse::<f64>() {
                metrics.push(Metric {
                    name: "cpu_load_1m",
                    value: v,
                    unit: "",
                });
            }
        }
    }

    // Memory
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        let mut total = 0u64;
        let mut avail = 0u64;
        for line in meminfo.lines() {
            if let Some(v) = line.strip_prefix("MemTotal:") {
                total = parse_kb(v);
            } else if let Some(v) = line.strip_prefix("MemAvailable:") {
                avail = parse_kb(v);
            }
        }
        if total > 0 {
            let used_mb = (total - avail) / 1024;
            let total_mb = total / 1024;
            metrics.push(Metric {
                name: "mem_used_mb",
                value: used_mb as f64,
                unit: "MB",
            });
            metrics.push(Metric {
                name: "mem_total_mb",
                value: total_mb as f64,
                unit: "MB",
            });
            metrics.push(Metric {
                name: "mem_usage_pct",
                value: ((total - avail) as f64 / total as f64) * 100.0,
                unit: "%",
            });
        }
    }

    // Process RSS
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(v) = line.strip_prefix("VmRSS:") {
                let kb = parse_kb(v);
                metrics.push(Metric {
                    name: "process_rss_mb",
                    value: (kb / 1024) as f64,
                    unit: "MB",
                });
            }
            if let Some(v) = line.strip_prefix("Threads:") {
                if let Ok(n) = v.trim().parse::<u64>() {
                    metrics.push(Metric {
                        name: "threads",
                        value: n as f64,
                        unit: "",
                    });
                }
            }
        }
    }

    // Open file descriptors
    if let Ok(fds) = std::fs::read_dir("/proc/self/fd") {
        let count = fds.count();
        metrics.push(Metric {
            name: "open_fds",
            value: count as f64,
            unit: "",
        });
    }

    metrics
}

fn parse_kb(val: &str) -> u64 {
    val.split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Remove oldest profiles if we have more than `max_files`.
fn gc_old_profiles(dir: &Path, max_files: usize) {
    let mut files: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str());
            if ext == Some("prof") || ext == Some("stacks") {
                if let Ok(meta) = entry.metadata() {
                    if let Ok(modified) = meta.modified() {
                        files.push((modified, path));
                    }
                }
            }
        }
    }

    if files.len() <= max_files {
        return;
    }

    files.sort_by_key(|(t, _)| *t);
    let to_remove = files.len() - max_files;
    for (_, path) in files.iter().take(to_remove) {
        let _ = std::fs::remove_file(path);
    }
}

// ---------------------------------------------------------------------------
// Stack trace profiling: sampled wall-clock time per code path
// ---------------------------------------------------------------------------

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// Global stack collector. Accumulates (stack_path, duration_us) samples.
static STACK_COLLECTOR: std::sync::LazyLock<Mutex<StackCollector>> =
    std::sync::LazyLock::new(|| Mutex::new(StackCollector::new()));

struct StackEntry {
    total_us: u64,
    calls: u64,
    max_us: u64,
}

struct StackCollector {
    /// Aggregated: stack_path → (total_us, call_count, max_single_us).
    stacks: HashMap<String, StackEntry>,
    /// Whether collection is active.
    enabled: bool,
}

impl StackCollector {
    fn new() -> Self {
        Self {
            stacks: HashMap::new(),
            enabled: false,
        }
    }

    fn record(&mut self, path: &str, duration_us: u64) {
        if !self.enabled {
            return;
        }
        let entry = self.stacks.entry(path.to_string()).or_insert(StackEntry {
            total_us: 0,
            calls: 0,
            max_us: 0,
        });
        entry.total_us += duration_us;
        entry.calls += 1;
        if duration_us > entry.max_us {
            entry.max_us = duration_us;
        }
    }

    fn drain(&mut self) -> Vec<(String, StackEntry)> {
        let mut result: Vec<(String, StackEntry)> = self.stacks.drain().collect();
        result.sort_by(|a, b| b.1.total_us.cmp(&a.1.total_us));
        result
    }
}

/// Enable stack collection. Call once at startup in dev mode.
pub fn enable_stack_profiling() {
    if let Ok(mut c) = STACK_COLLECTOR.lock() {
        c.enabled = true;
    }
}

/// Drain and write the current window's stack data to a .stacks file.
/// Called by the profiling loop every window.
pub fn flush_stacks(dir: &Path, ts: u64) {
    let stacks = {
        let Ok(mut c) = STACK_COLLECTOR.lock() else {
            return;
        };
        c.drain()
    };

    if stacks.is_empty() {
        return;
    }

    // Format: folded stacks with total, calls, avg, max per line.
    // Sorted by total time descending — top entries are the bottlenecks.
    //
    // Header explains the format so an LLM can parse it without docs:
    // stack_path total_us calls avg_us max_us
    //
    // For optimization: look at high total_us (where time goes overall),
    // high avg_us (slow individual calls), high max_us (outlier latency).
    let mut content = String::with_capacity(stacks.len() * 100);
    content.push_str("# stack_path total_us calls avg_us max_us\n");
    for (path, entry) in &stacks {
        let avg = if entry.calls > 0 {
            entry.total_us / entry.calls
        } else {
            0
        };
        content.push_str(&format!(
            "{path} {total} {calls} {avg} {max}\n",
            total = entry.total_us,
            calls = entry.calls,
            max = entry.max_us,
        ));
    }

    let filename = format!("stacks_{ts}.stacks");
    if let Err(e) = std::fs::write(dir.join(&filename), content.as_bytes()) {
        warn!(error = %e, "Failed to write stack profile");
    }
}

/// RAII guard for timing a code section. Records wall-clock time on drop.
///
/// Usage:
/// ```ignore
/// let _t = span_timer("ipfix;parse_message");
/// // ... code being timed ...
/// // timer records duration on drop
/// ```
///
/// Stack paths use semicolons as separators (folded stack convention):
/// - "ipfix;udp_recv" — IPFIX UDP receive path
/// - "storage;writer;flush;encode_column" — deep in the flush pipeline
/// - "merge;column_read;lz4_decompress" — merge reading compressed data
pub struct SpanTimer {
    path: &'static str,
    start: Instant,
}

impl Drop for SpanTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        let us = elapsed.as_micros() as u64;
        // Only record if >0 microseconds (skip trivial spans)
        if us > 0 {
            if let Ok(mut c) = STACK_COLLECTOR.lock() {
                c.record(self.path, us);
            }
        }
    }
}

/// Create a span timer for the given stack path.
/// Returns a guard that records wall-clock duration on drop.
/// Zero cost when profiling is disabled (the lock check is the only overhead).
#[inline]
pub fn span_timer(path: &'static str) -> SpanTimer {
    SpanTimer {
        path,
        start: Instant::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_take_resets() {
        let c = Counter::new();
        c.add(42);
        assert_eq!(c.take(), 42);
        assert_eq!(c.get(), 0);
    }

    #[test]
    fn gauge_set_get() {
        let g = Gauge::new();
        g.set(100);
        assert_eq!(g.get(), 100);
        g.set(0);
        assert_eq!(g.get(), 0);
    }

    #[test]
    fn snapshot_format_is_compact() {
        let metrics = vec![
            Metric {
                name: "records_per_sec",
                value: 50000.0,
                unit: "rec/s",
            },
            Metric {
                name: "buffer_bytes",
                value: 1048576.0,
                unit: "bytes",
            },
            Metric {
                name: "avg_latency_us",
                value: 42.5,
                unit: "us",
            },
        ];

        let dir = std::env::temp_dir().join("flowcus_test_prof");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        write_snapshot(&dir, "test", 1700000000, &metrics);

        let content = std::fs::read_to_string(dir.join("test_1700000000.prof")).unwrap();
        assert!(content.contains("component: test"));
        assert!(content.contains("records_per_sec: 50000 rec/s"));
        assert!(content.contains("avg_latency_us: 42.50 us"));
        // Verify it's small
        assert!(
            content.len() < 300,
            "snapshot should be compact: {} bytes",
            content.len()
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn span_timer_records_time() {
        // Enable collection for this test
        {
            let mut c = STACK_COLLECTOR.lock().unwrap();
            c.enabled = true;
            c.stacks.clear();
        }

        {
            let _t = span_timer("test;slow_op");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        {
            let _t = span_timer("test;slow_op");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        {
            let _t = span_timer("test;fast_op");
            // no sleep — should still record >0us
        }

        let stacks = STACK_COLLECTOR.lock().unwrap().drain();
        assert!(!stacks.is_empty());

        // Find the slow_op entry
        let slow = stacks.iter().find(|(p, _)| p == "test;slow_op");
        assert!(slow.is_some(), "slow_op should be recorded");
        let (_, entry) = slow.unwrap();
        assert_eq!(entry.calls, 2);
        assert!(
            entry.total_us >= 8000,
            "should be at least 8ms total: got {}us",
            entry.total_us
        );
        assert!(
            entry.max_us >= 4000,
            "max should be at least 4ms: got {}us",
            entry.max_us
        );
    }

    #[test]
    fn stacks_file_format() {
        {
            let mut c = STACK_COLLECTOR.lock().unwrap();
            c.enabled = true;
            c.stacks.clear();
            c.record("a;b;c", 1000);
            c.record("a;b;c", 2000);
            c.record("a;b", 500);
        }

        let dir = std::env::temp_dir().join("flowcus_test_stacks");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        flush_stacks(&dir, 1700000000);

        let content = std::fs::read_to_string(dir.join("stacks_1700000000.stacks")).unwrap();
        assert!(content.contains("# stack_path total_us calls avg_us max_us"));
        assert!(content.contains("a;b;c 3000 2 1500 2000"));
        assert!(content.contains("a;b 500 1 500 500"));

        std::fs::remove_dir_all(&dir).ok();
    }
}
