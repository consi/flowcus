use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tracing::{error, info};

use flowcus_core::{LogFormat, observability, telemetry};
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
    /// Path to settings file (default: `{storage_dir}/flowcus.settings`)
    #[arg(short, long)]
    settings: Option<PathBuf>,

    /// Override storage directory
    #[arg(long, env = "FLOWCUS_STORAGE")]
    storage: Option<String>,

    /// Enable development mode (human logs, frontend proxy)
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

    // Resolve settings path: explicit CLI flag, or {storage_dir}/flowcus.settings.
    // We need to load config to know storage_dir, but storage_dir may be in the
    // settings file. Break the cycle: try the explicit path first, then fall back
    // to the default location inside the storage directory.
    let settings_path = if let Some(ref p) = cli.settings {
        p.clone()
    } else {
        // Determine storage dir: CLI override > env > default "storage"
        let storage_dir = cli.storage.as_deref().unwrap_or("storage");
        PathBuf::from(storage_dir).join("flowcus.settings")
    };

    let mut config = flowcus_core::settings::load_or_create(&settings_path)?;

    if let Some(dir) = cli.storage.as_deref() {
        config.storage.dir = dir.to_string();
    }

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
    let state = AppState::with_session_store(config.clone(), metrics, session_store, settings_path);
    flowcus_server::serve(&config.server, state.clone()).await?;

    // Distinguish restart request from normal shutdown (ctrl+c / SIGTERM)
    if *state.shutdown_rx().borrow() {
        info!("Restart requested, re-executing...");
        restart_process();
    }

    info!("Flowcus shutting down");

    Ok(())
}

fn restart_process() -> ! {
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe().expect("failed to get current executable path");
    let args: Vec<String> = std::env::args().collect();
    let err = std::process::Command::new(&exe).args(&args[1..]).exec();
    panic!("Failed to restart process: {err}");
}
