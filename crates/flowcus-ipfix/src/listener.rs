//! IPFIX network listeners (UDP and TCP).
//!
//! Listens for incoming IPFIX messages, parses them, decodes using templates,
//! and forwards decoded messages to a consumer via callback.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace, warn};

use flowcus_core::config::IpfixConfig;
use flowcus_core::observability::Metrics;

use crate::decoder;
use crate::display::DisplayMessage;
use crate::netflow;
use crate::protocol::{self, IpfixMessage, SetContents};
use crate::session::SessionStore;
use crate::unprocessed;

/// Callback for decoded IPFIX messages. Implementations must be Send + Sync.
pub trait MessageSink: Send + Sync + 'static {
    fn on_message(&self, msg: IpfixMessage);
}

/// No-op sink that only logs (for use when no storage is configured).
pub struct LogOnlySink;
impl MessageSink for LogOnlySink {
    fn on_message(&self, _msg: IpfixMessage) {}
}

/// IPFIX collector listener managing UDP and/or TCP endpoints.
pub struct IpfixListener {
    config: IpfixConfig,
    session: Arc<Mutex<SessionStore>>,
    sink: Arc<dyn MessageSink>,
    metrics: Arc<Metrics>,
    unprocessed_dir: std::path::PathBuf,
}

impl IpfixListener {
    pub fn new(
        config: &IpfixConfig,
        sink: Arc<dyn MessageSink>,
        metrics: Arc<Metrics>,
        storage_dir: &str,
    ) -> Self {
        let unprocessed_dir = std::path::PathBuf::from(storage_dir).join(&config.unprocessed_dir);
        Self {
            config: config.clone(),
            session: Arc::new(Mutex::new(SessionStore::new(config.template_expiry_secs))),
            sink,
            metrics,
            unprocessed_dir,
        }
    }

    /// Start all configured listeners. Runs until cancelled.
    ///
    /// # Errors
    /// Returns an error if binding to the configured address fails.
    pub async fn run(&self) -> flowcus_core::Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);

        let mut handles = Vec::new();

        if self.config.udp {
            let socket = UdpSocket::bind(&addr)
                .await
                .map_err(|e| flowcus_core::Error::server(format!("IPFIX UDP bind {addr}: {e}")))?;

            info!(addr = %addr, protocol = "UDP", "IPFIX listener started");

            let session = Arc::clone(&self.session);
            let sink = Arc::clone(&self.sink);
            let m = Arc::clone(&self.metrics);
            let buf_size = self.config.udp_recv_buffer;
            let udir = self.unprocessed_dir.clone();
            handles.push(tokio::spawn(async move {
                run_udp(socket, session, sink, m, buf_size, udir).await;
            }));
        }

        if self.config.tcp {
            let listener = TcpListener::bind(&addr)
                .await
                .map_err(|e| flowcus_core::Error::server(format!("IPFIX TCP bind {addr}: {e}")))?;

            info!(addr = %addr, protocol = "TCP", "IPFIX listener started");

            let session = Arc::clone(&self.session);
            let sink = Arc::clone(&self.sink);
            let m = Arc::clone(&self.metrics);
            let udir = self.unprocessed_dir.clone();
            handles.push(tokio::spawn(async move {
                run_tcp(listener, session, sink, m, udir).await;
            }));
        }

        // Template expiry task
        let session = Arc::clone(&self.session);
        let expiry_interval = self.config.template_expiry_secs / 3;
        handles.push(tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(expiry_interval.max(60)));
            loop {
                interval.tick().await;
                let count = {
                    let mut s = session.lock().await;
                    let c = s.template_count();
                    s.expire_templates();
                    c
                };
                if count > 0 {
                    trace!(templates = count, "Template expiry check");
                }
            }
        }));

        // Unprocessed packet reprocessing worker
        unprocessed::start_worker(
            self.unprocessed_dir.clone(),
            Arc::clone(&self.session),
            Arc::clone(&self.sink),
            Arc::clone(&self.metrics),
            std::time::Duration::from_secs(self.config.unprocessed_ttl_secs),
            std::time::Duration::from_secs(self.config.unprocessed_scan_interval_secs),
        );

        if handles.is_empty() {
            warn!("No IPFIX listeners configured (both UDP and TCP disabled)");
            return Ok(());
        }

        for handle in handles {
            if let Err(e) = handle.await {
                error!(error = %e, "IPFIX listener task failed");
            }
        }

        Ok(())
    }

    /// Current cached template count.
    pub async fn template_count(&self) -> usize {
        self.session.lock().await.template_count()
    }

    /// Get a handle to the shared session store.
    /// Used by the API server to read metadata (e.g. interface names).
    pub fn session_store(&self) -> Arc<Mutex<SessionStore>> {
        Arc::clone(&self.session)
    }
}

async fn run_udp(
    socket: UdpSocket,
    session: Arc<Mutex<SessionStore>>,
    sink: Arc<dyn MessageSink>,
    metrics: Arc<Metrics>,
    buf_size: usize,
    unprocessed_dir: std::path::PathBuf,
) {
    let mut buf = vec![0u8; buf_size];

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((len, src)) => {
                use std::sync::atomic::Ordering::Relaxed;

                metrics.ipfix_packets_received.fetch_add(1, Relaxed);
                metrics.ipfix_bytes_received.fetch_add(len as u64, Relaxed);

                let msg_buf = &buf[..len];
                if len < 2 {
                    debug!(len, %src, "UDP datagram too short, discarding");
                    continue;
                }

                // Detect protocol version from first 2 bytes
                let version = u16::from_be_bytes([msg_buf[0], msg_buf[1]]);
                match version {
                    protocol::IPFIX_VERSION => {
                        if len < protocol::HEADER_LEN {
                            debug!(len, %src, "UDP datagram too short for IPFIX header");
                            continue;
                        }
                        process_ipfix_packet(
                            msg_buf,
                            src,
                            &session,
                            &sink,
                            &metrics,
                            &unprocessed_dir,
                        )
                        .await;
                    }
                    netflow::NETFLOW_V9_VERSION => {
                        metrics.netflow_v9_packets_received.fetch_add(1, Relaxed);
                        match netflow::translate_v9(msg_buf, src) {
                            Ok((msg, ipfix_buf)) => {
                                process_parsed_packet(
                                    msg,
                                    &ipfix_buf,
                                    src,
                                    &session,
                                    &sink,
                                    &metrics,
                                    &unprocessed_dir,
                                )
                                .await;
                            }
                            Err(e) => {
                                metrics.ipfix_packets_errors.fetch_add(1, Relaxed);
                                warn!(exporter = %src, error = %e, "Failed to translate NetFlow v9 message");
                            }
                        }
                    }
                    netflow::NETFLOW_V5_VERSION => {
                        metrics.netflow_v5_packets_received.fetch_add(1, Relaxed);
                        match netflow::translate_v5(msg_buf, src) {
                            Ok((msg, ipfix_buf)) => {
                                process_parsed_packet(
                                    msg,
                                    &ipfix_buf,
                                    src,
                                    &session,
                                    &sink,
                                    &metrics,
                                    &unprocessed_dir,
                                )
                                .await;
                            }
                            Err(e) => {
                                metrics.ipfix_packets_errors.fetch_add(1, Relaxed);
                                warn!(exporter = %src, error = %e, "Failed to translate NetFlow v5 message");
                            }
                        }
                    }
                    _ => {
                        metrics.ipfix_packets_errors.fetch_add(1, Relaxed);
                        warn!(
                            exporter = %src,
                            version = format_args!("{version:#06x}"),
                            "Unknown flow protocol version"
                        );
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "UDP recv error");
            }
        }
    }
}

async fn run_tcp(
    listener: TcpListener,
    session: Arc<Mutex<SessionStore>>,
    sink: Arc<dyn MessageSink>,
    metrics: Arc<Metrics>,
    unprocessed_dir: std::path::PathBuf,
) {
    loop {
        match listener.accept().await {
            Ok((stream, src)) => {
                info!(%src, "IPFIX TCP connection accepted");
                metrics
                    .ipfix_tcp_connections
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let session = Arc::clone(&session);
                let sink = Arc::clone(&sink);
                let m = Arc::clone(&metrics);
                let udir = unprocessed_dir.clone();
                tokio::spawn(async move {
                    handle_tcp_connection(stream, src, session, sink, m, udir).await;
                });
            }
            Err(e) => {
                error!(error = %e, "TCP accept error");
            }
        }
    }
}

async fn handle_tcp_connection(
    stream: tokio::net::TcpStream,
    src: SocketAddr,
    session: Arc<Mutex<SessionStore>>,
    sink: Arc<dyn MessageSink>,
    metrics: Arc<Metrics>,
    unprocessed_dir: std::path::PathBuf,
) {
    use tokio::io::AsyncReadExt;

    let mut stream = stream;
    let mut header_buf = [0u8; 16];

    loop {
        match stream.read_exact(&mut header_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!(%src, "IPFIX TCP connection closed");
                return;
            }
            Err(e) => {
                warn!(%src, error = %e, "IPFIX TCP read error");
                return;
            }
        }

        let version = u16::from_be_bytes([header_buf[0], header_buf[1]]);
        if version != protocol::IPFIX_VERSION {
            warn!(%src, version, "Invalid IPFIX version on TCP stream, closing");
            return;
        }

        let msg_len = u16::from_be_bytes([header_buf[2], header_buf[3]]) as usize;
        if msg_len < 16 || msg_len > 65535 {
            warn!(%src, msg_len, "Invalid IPFIX message length on TCP stream");
            return;
        }

        let mut msg_buf = vec![0u8; msg_len];
        msg_buf[..16].copy_from_slice(&header_buf);

        if msg_len > 16 {
            match stream.read_exact(&mut msg_buf[16..]).await {
                Ok(_) => {}
                Err(e) => {
                    warn!(%src, error = %e, "IPFIX TCP read error during message body");
                    return;
                }
            }
        }

        metrics
            .ipfix_packets_received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics
            .ipfix_bytes_received
            .fetch_add(msg_buf.len() as u64, std::sync::atomic::Ordering::Relaxed);
        process_ipfix_packet(&msg_buf, src, &session, &sink, &metrics, &unprocessed_dir).await;
    }
}

/// Parse and process a native IPFIX (v10) packet.
async fn process_ipfix_packet(
    buf: &[u8],
    src: SocketAddr,
    session: &Arc<Mutex<SessionStore>>,
    sink: &Arc<dyn MessageSink>,
    metrics: &Arc<Metrics>,
    unprocessed_dir: &std::path::Path,
) {
    use std::sync::atomic::Ordering::Relaxed;

    match protocol::parse_message(buf, src) {
        Ok(msg) => {
            metrics.ipfix_packets_parsed.fetch_add(1, Relaxed);
            process_parsed_packet(msg, buf, src, session, sink, metrics, unprocessed_dir).await;
        }
        Err(e) => {
            metrics.ipfix_packets_errors.fetch_add(1, Relaxed);
            warn!(exporter = %src, error = %e, "Failed to parse IPFIX message");
        }
    }
}

/// Process an already-parsed message (IPFIX or translated NetFlow v9/v5).
/// Decodes data records using templates, saves unprocessed packets, and
/// forwards to the message sink.
async fn process_parsed_packet(
    mut msg: IpfixMessage,
    raw: &[u8],
    src: SocketAddr,
    session: &Arc<Mutex<SessionStore>>,
    sink: &Arc<dyn MessageSink>,
    metrics: &Arc<Metrics>,
    unprocessed_dir: &std::path::Path,
) {
    use std::sync::atomic::Ordering::Relaxed;

    {
        let mut session = session.lock().await;
        decoder::decode_message(&mut msg, raw, &mut session);
        metrics
            .ipfix_templates_active
            .store(session.template_count() as i64, Relaxed);
        metrics
            .ipfix_exporters_active
            .store(session.exporter_count() as i64, Relaxed);
    }

    // Check for data sets that could not be decoded (missing template).
    let missing_tids: Vec<u16> = msg
        .sets
        .iter()
        .filter_map(|s| {
            if let SetContents::Data(d) = &s.contents {
                if d.records.is_empty() && d.template_id >= protocol::MIN_DATA_SET_ID {
                    return Some(d.template_id);
                }
            }
            None
        })
        .collect();

    if !missing_tids.is_empty() {
        if let Err(e) = unprocessed::save_unprocessed(unprocessed_dir, raw, src, &missing_tids) {
            warn!(error = %e, "Failed to save unprocessed packet");
        } else {
            metrics.ipfix_unprocessed_saved.fetch_add(1, Relaxed);
        }
    }

    let record_count: usize = msg
        .sets
        .iter()
        .filter_map(|s| {
            if let SetContents::Data(d) = &s.contents {
                Some(d.records.len())
            } else {
                None
            }
        })
        .sum();
    metrics
        .ipfix_records_decoded
        .fetch_add(record_count as u64, Relaxed);

    let proto_label = match msg.header.protocol_version {
        5 => "NetFlow v5",
        9 => "NetFlow v9",
        _ => "IPFIX",
    };

    debug!(
        exporter = %src,
        seq = msg.header.sequence_number,
        domain = msg.header.observation_domain_id,
        records = record_count,
        protocol = proto_label,
        "Message decoded"
    );

    trace!("\n{}", DisplayMessage(&msg));

    // Forward to storage (or other consumers)
    sink.on_message(msg);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn listener_creation() {
        let config = IpfixConfig::default();
        let metrics = Metrics::new();
        let listener = IpfixListener::new(&config, Arc::new(LogOnlySink), metrics, "storage");
        assert_eq!(listener.template_count().await, 0);
    }
}
