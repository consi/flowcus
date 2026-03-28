//! Async ingestion pipeline: bridges the IPFIX listener to the storage writer.
//!
//! Implements the MessageSink trait from flowcus-ipfix so decoded messages
//! flow from the listener through a bounded channel to the writer task.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, info};

use flowcus_ipfix::listener::MessageSink;
use flowcus_ipfix::protocol::IpfixMessage;

use crate::table::Table;
use crate::writer::{StorageWriter, WriterConfig};

/// Ingestion handle that implements MessageSink for the IPFIX listener.
/// Sends decoded messages to the storage writer via a bounded channel.
pub struct IngestionHandle {
    tx: mpsc::Sender<IpfixMessage>,
}

impl MessageSink for IngestionHandle {
    fn on_message(&self, msg: IpfixMessage) {
        if self.tx.try_send(msg).is_err() {
            debug!("Storage ingestion channel full, message dropped");
        }
    }
}

/// Start the ingestion pipeline. Returns a handle that can be passed to the IPFIX listener.
///
/// Spawns a background task that receives messages, buffers them columnar, and flushes parts.
///
/// # Errors
/// Returns an error if the storage directory cannot be created.
pub fn start(
    storage_dir: &Path,
    table_name: &str,
    writer_config: WriterConfig,
    pending: crate::pending::PendingHours,
    metrics: std::sync::Arc<flowcus_core::observability::Metrics>,
) -> flowcus_core::Result<Arc<IngestionHandle>> {
    let table = Table::open(storage_dir, table_name)
        .map_err(|e| flowcus_core::Error::Internal(format!("open table: {e}")))?;

    let (tx, rx) = mpsc::channel(writer_config.channel_capacity);
    let flush_interval = writer_config.flush_interval_secs;

    tokio::spawn(async move {
        run_writer(table, writer_config, rx, flush_interval, pending, metrics).await;
    });

    info!(
        table = table_name,
        storage_dir = %storage_dir.display(),
        "Ingestion pipeline started"
    );

    Ok(Arc::new(IngestionHandle { tx }))
}

async fn run_writer(
    table: Table,
    config: WriterConfig,
    mut rx: mpsc::Receiver<IpfixMessage>,
    flush_interval_secs: u64,
    pending: crate::pending::PendingHours,
    metrics: std::sync::Arc<flowcus_core::observability::Metrics>,
) {
    use std::sync::atomic::Ordering::Relaxed;
    let mut writer = StorageWriter::new(table, config, Some(pending));
    let mut flush_timer =
        tokio::time::interval(std::time::Duration::from_secs(flush_interval_secs.max(1)));
    let mut total_ingested: u64 = 0;
    let mut total_flushed: u64 = 0;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(ipfix_msg) => {
                        let n = writer.ingest(&ipfix_msg);
                        total_ingested += n as u64;
                        metrics.writer_records_ingested.fetch_add(n as u64, Relaxed);

                        let flushed = writer.flush_ready();
                        total_flushed += flushed as u64;
                        if flushed > 0 {
                            metrics.writer_parts_flushed.fetch_add(flushed as u64, Relaxed);
                            debug!(
                                parts_flushed = flushed,
                                total_ingested,
                                total_flushed,
                                "Size-triggered flush"
                            );
                        }

                        // Update buffer gauges
                        metrics.writer_active_buffers.store(writer.buffer_count() as i64, Relaxed);
                        metrics.writer_buffer_bytes.store(writer.buffer_bytes() as i64, Relaxed);
                    }
                    None => {
                        info!(
                            total_ingested,
                            total_flushed,
                            "Ingestion channel closed, flushing remaining data"
                        );
                        let remaining = writer.flush_all();
                        if remaining > 0 {
                            metrics.writer_parts_flushed.fetch_add(remaining as u64, Relaxed);
                            info!(parts = remaining, "Final flush completed");
                        }
                        return;
                    }
                }
            }
            _ = flush_timer.tick() => {
                let flushed = writer.flush_ready();
                if flushed > 0 {
                    total_flushed += flushed as u64;
                    debug!(
                        parts_flushed = flushed,
                        total_ingested,
                        total_flushed,
                        "Timer-triggered flush"
                    );
                }
            }
        }
    }
}
