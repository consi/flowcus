use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::telemetry::LogFormat;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub ipfix: IpfixConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default)]
    pub format: LogFormat,
    #[serde(default = "default_log_filter")]
    pub filter: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_server_port")]
    pub port: u16,
    #[serde(default)]
    pub dev_mode: bool,
    #[serde(default = "default_frontend_proxy")]
    pub frontend_proxy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpfixConfig {
    #[serde(default = "default_ipfix_host")]
    pub host: String,
    #[serde(default = "default_ipfix_port")]
    pub port: u16,
    #[serde(default = "default_true")]
    pub udp: bool,
    #[serde(default)]
    pub tcp: bool,
    /// Maximum UDP datagram size to accept (bytes).
    #[serde(default = "default_udp_recv_buffer")]
    pub udp_recv_buffer: usize,
    /// Template expiry in seconds (RFC 7011 recommends at least 3x export interval).
    #[serde(default = "default_template_expiry_secs")]
    pub template_expiry_secs: u64,
    /// Directory for unprocessed data files (relative to storage dir).
    #[serde(default = "default_unprocessed_dir")]
    pub unprocessed_dir: String,
    /// Time in seconds before unprocessed files are deleted.
    #[serde(default = "default_unprocessed_ttl_secs")]
    pub unprocessed_ttl_secs: u64,
    /// How often to check for reprocessable/expired files (seconds).
    #[serde(default = "default_unprocessed_scan_interval_secs")]
    pub unprocessed_scan_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base directory for storage data.
    #[serde(default = "default_storage_dir")]
    pub dir: String,
    /// Flush write buffer when it exceeds this many bytes.
    #[serde(default = "default_flush_bytes")]
    pub flush_bytes: usize,
    /// Flush write buffer after this many seconds.
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
    /// Time partition duration in seconds. Parts won't span across boundaries.
    #[serde(default = "default_partition_duration_secs")]
    pub partition_duration_secs: u32,
    /// Ingestion channel capacity (backpressure threshold).
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    /// Pre-allocated row capacity for column buffers.
    #[serde(default = "default_initial_row_capacity")]
    pub initial_row_capacity: usize,
    /// Number of merge worker threads.
    #[serde(default = "default_merge_workers")]
    pub merge_workers: usize,
    /// How often the merge coordinator scans for work (seconds).
    #[serde(default = "default_merge_scan_interval_secs")]
    pub merge_scan_interval_secs: u64,
    /// Maximum system CPU load (0.0-1.0) before throttling merge workers.
    #[serde(default = "default_merge_cpu_throttle")]
    pub merge_cpu_throttle: f64,
    /// Maximum system memory usage fraction (0.0-1.0) before throttling merges.
    #[serde(default = "default_merge_mem_throttle")]
    pub merge_mem_throttle: f64,
    /// Granule size in rows. Marks and bloom filters are computed per granule.
    /// Smaller = more precise seeking but more index overhead.
    #[serde(default = "default_granule_size")]
    pub granule_size: usize,
    /// Bloom filter bits per granule (must be multiple of 64).
    #[serde(default = "default_bloom_bits_per_granule")]
    pub bloom_bits_per_granule: usize,
    /// LRU cache for granule marks (.mrk files). Marks are small and always
    /// needed for seeks — keep hot. Default 1 GB.
    #[serde(default = "default_mark_cache_bytes")]
    pub mark_cache_bytes: usize,
    /// LRU cache for bloom filters (.bloom files). Blooms are larger and only
    /// needed for point/equality queries. Default 1 GB.
    #[serde(default = "default_bloom_cache_bytes")]
    pub bloom_cache_bytes: usize,
}

fn default_storage_dir() -> String {
    "storage".to_string()
}

const fn default_flush_bytes() -> usize {
    1024 * 1024 // 1 MB
}

const fn default_flush_interval_secs() -> u64 {
    1
}

const fn default_partition_duration_secs() -> u32 {
    3600 // 1 hour
}

const fn default_channel_capacity() -> usize {
    4096
}

const fn default_initial_row_capacity() -> usize {
    8192
}

const fn default_merge_workers() -> usize {
    8
}

const fn default_merge_scan_interval_secs() -> u64 {
    5
}

const fn default_merge_cpu_throttle() -> f64 {
    0.80
}

const fn default_merge_mem_throttle() -> f64 {
    0.85
}

const fn default_granule_size() -> usize {
    8192
}

const fn default_bloom_bits_per_granule() -> usize {
    8192 // 1 KB per granule, ~1% FPR for up to ~800 distinct values
}

const fn default_mark_cache_bytes() -> usize {
    1024 * 1024 * 1024 // 1 GB
}

const fn default_bloom_cache_bytes() -> usize {
    1024 * 1024 * 1024 // 1 GB
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            dir: default_storage_dir(),
            flush_bytes: default_flush_bytes(),
            flush_interval_secs: default_flush_interval_secs(),
            partition_duration_secs: default_partition_duration_secs(),
            channel_capacity: default_channel_capacity(),
            initial_row_capacity: default_initial_row_capacity(),
            merge_workers: default_merge_workers(),
            merge_scan_interval_secs: default_merge_scan_interval_secs(),
            merge_cpu_throttle: default_merge_cpu_throttle(),
            merge_mem_throttle: default_merge_mem_throttle(),
            granule_size: default_granule_size(),
            bloom_bits_per_granule: default_bloom_bits_per_granule(),
            mark_cache_bytes: default_mark_cache_bytes(),
            bloom_cache_bytes: default_bloom_cache_bytes(),
        }
    }
}

fn default_log_filter() -> String {
    "info,tower_http=debug".to_string()
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

const fn default_server_port() -> u16 {
    2137
}

fn default_frontend_proxy() -> String {
    "http://localhost:5173".to_string()
}

fn default_ipfix_host() -> String {
    "0.0.0.0".to_string()
}

const fn default_ipfix_port() -> u16 {
    4739
}

const fn default_true() -> bool {
    true
}

const fn default_udp_recv_buffer() -> usize {
    65535
}

const fn default_template_expiry_secs() -> u64 {
    1800
}

fn default_unprocessed_dir() -> String {
    "unprocessed".to_string()
}

const fn default_unprocessed_ttl_secs() -> u64 {
    300
}

const fn default_unprocessed_scan_interval_secs() -> u64 {
    10
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            format: LogFormat::default(),
            filter: default_log_filter(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_server_port(),
            dev_mode: false,
            frontend_proxy: default_frontend_proxy(),
        }
    }
}

impl Default for IpfixConfig {
    fn default() -> Self {
        Self {
            host: default_ipfix_host(),
            port: default_ipfix_port(),
            udp: default_true(),
            tcp: false,
            udp_recv_buffer: default_udp_recv_buffer(),
            template_expiry_secs: default_template_expiry_secs(),
            unprocessed_dir: default_unprocessed_dir(),
            unprocessed_ttl_secs: default_unprocessed_ttl_secs(),
            unprocessed_scan_interval_secs: default_unprocessed_scan_interval_secs(),
        }
    }
}

impl AppConfig {
    /// Load config from a TOML file, falling back to defaults.
    ///
    /// # Errors
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn load(path: &Path) -> crate::Result<Self> {
        if path.exists() {
            let content = std::fs::read_to_string(path)?;
            let config: Self = toml::from_str(&content)?;
            Ok(config)
        } else {
            Ok(Self::default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_correct_ports() {
        let config = AppConfig::default();
        assert_eq!(config.server.port, 2137);
        assert_eq!(config.ipfix.port, 4739);
        assert!(config.ipfix.udp);
        assert!(!config.ipfix.tcp);
    }

    #[test]
    fn parse_config_with_logging_and_ipfix() {
        let toml_str = r#"
            [logging]
            format = "json"
            filter = "debug"

            [server]
            port = 3000

            [ipfix]
            port = 9995
            tcp = true
            template_expiry_secs = 600
        "#;
        let config: AppConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.logging.format, LogFormat::Json);
        assert_eq!(config.logging.filter, "debug");
        assert_eq!(config.server.port, 3000);
        assert_eq!(config.ipfix.port, 9995);
        assert!(config.ipfix.tcp);
        assert_eq!(config.ipfix.template_expiry_secs, 600);
    }
}
