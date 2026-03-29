use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tracing::{error, info};

use flowcus_core::{AppConfig, LogFormat, observability, profiling, telemetry};
use flowcus_ipfix::IpfixListener;
use flowcus_server::state::AppState;
use flowcus_storage::{MergeConfig, WriterConfig};

#[derive(Parser, Debug)]
#[command(
    name = "flowcus",
    version,
    about = "Flowcus - IPFIX collector, storage and query system"
)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "flowcus.toml")]
    config: PathBuf,

    /// Enable development mode (human logs, frontend proxy, profiling)
    #[arg(short, long, env = "FLOWCUS_DEV")]
    dev: bool,

    /// Log format: human or json
    #[arg(long, env = "FLOWCUS_LOG_FORMAT")]
    log_format: Option<LogFormat>,

    /// Override server port
    #[arg(short, long, env = "FLOWCUS_PORT")]
    port: Option<u16>,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut config = AppConfig::load(&cli.config)?;

    if cli.dev {
        config.server.dev_mode = true;
        config.logging.format = LogFormat::Human;
    }
    if let Some(fmt) = cli.log_format {
        config.logging.format = fmt;
    }
    if let Some(port) = cli.port {
        config.server.port = port;
    }

    telemetry::init(config.logging.format, &config.logging.filter);

    // Production metrics — created FIRST, passed to all components
    let metrics = observability::Metrics::new();

    if cli.config.exists() {
        info!(config_file = %cli.config.display(), "Configuration loaded from file");
    } else {
        info!(config_file = %cli.config.display(), "Configuration file not found, using defaults");
    }

    info!(
        version = env!("CARGO_PKG_VERSION"),
        log_format = %config.logging.format,
        dev_mode = config.server.dev_mode,
        server_port = config.server.port,
        ipfix_port = config.ipfix.port,
        storage_dir = %config.storage.dir,
        merge_workers = config.storage.merge_workers,
        "Starting Flowcus"
    );

    // Dev-mode profiling
    let mut profiler = profiling::Profiler::new(Path::new("profiling"), 10);
    let ipfix_prof_metrics = flowcus_storage::metrics::IpfixMetrics::new();
    let writer_prof_metrics = flowcus_storage::metrics::WriterMetrics::new();
    let merge_prof_metrics = flowcus_storage::metrics::MergeMetrics::new();
    profiler.register(Arc::clone(&ipfix_prof_metrics) as Arc<dyn profiling::MetricReporter>);
    profiler.register(Arc::clone(&writer_prof_metrics) as Arc<dyn profiling::MetricReporter>);
    profiler.register(Arc::clone(&merge_prof_metrics) as Arc<dyn profiling::MetricReporter>);
    if config.server.dev_mode {
        profiler.enable()?;
        profiling::enable_stack_profiling();
    }
    let profiler = Arc::new(std::sync::Mutex::new(profiler));
    profiling::start(Arc::clone(&profiler));

    // Storage
    let writer_config = WriterConfig {
        flush_bytes: config.storage.flush_bytes,
        flush_interval_secs: config.storage.flush_interval_secs,
        initial_row_capacity: config.storage.initial_row_capacity,
        partition_duration_secs: config.storage.partition_duration_secs,
        channel_capacity: config.storage.channel_capacity,
    };

    let storage_dir = Path::new(&config.storage.dir);
    let table_base = storage_dir.join("flows");
    let pending = flowcus_storage::pending::PendingHours::open(&table_base);

    let sink = flowcus_storage::ingest::start(
        storage_dir,
        "flows",
        writer_config,
        pending.clone(),
        Arc::clone(&metrics),
    )?;

    info!(
        storage_dir = %config.storage.dir,
        flush_bytes = config.storage.flush_bytes,
        partition_duration_secs = config.storage.partition_duration_secs,
        "Storage engine ready"
    );

    // Migrate old-format parts in background (v0 → v1: directory rename)
    flowcus_storage::migrate::start_background_migration(table_base.clone());

    // Background merge
    let merge_config = MergeConfig {
        workers: config.storage.merge_workers,
        scan_interval: Duration::from_secs(config.storage.merge_scan_interval_secs),
        cpu_throttle: config.storage.merge_cpu_throttle,
        mem_throttle: config.storage.merge_mem_throttle,
        partition_duration_secs: config.storage.partition_duration_secs,
        granule_size: config.storage.granule_size,
        bloom_bits: config.storage.bloom_bits_per_granule,
    };
    flowcus_storage::merge::start(
        table_base.clone(),
        merge_config,
        pending,
        Arc::clone(&metrics),
    );

    // Data retention
    flowcus_storage::retention::start(
        table_base,
        config.storage.retention_hours,
        config.storage.retention_scan_interval_secs,
        Arc::clone(&metrics),
    );

    // IPFIX collector — pass metrics for live counter updates
    let ipfix = IpfixListener::new(
        &config.ipfix,
        sink,
        Arc::clone(&metrics),
        &config.storage.dir,
    );
    let session_store = ipfix.session_store();
    tokio::spawn(async move {
        if let Err(e) = ipfix.run().await {
            error!(error = %e, "IPFIX listener failed");
        }
    });

    info!(
        ipfix_host = %config.ipfix.host,
        ipfix_port = config.ipfix.port,
        "IPFIX collector running"
    );

    // HTTP server with observability endpoint (shares IPFIX session for metadata API)
    let state = AppState::with_session_store(config.clone(), metrics, session_store);
    flowcus_server::serve(&config.server, state).await?;

    info!("Flowcus shutting down");

    Ok(())
}
