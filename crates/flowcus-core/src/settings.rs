use std::net::IpAddr;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::AppConfig;

// ---------------------------------------------------------------------------
// Schema descriptors — drive the frontend settings UI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct SettingDescriptor {
    pub section: &'static str,
    pub key: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub guidance: &'static str,
    pub control: ControlType,
    pub default_value: serde_json::Value,
    pub restart_required: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ControlType {
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "number")]
    Number {
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        step: Option<f64>,
        unit: &'static str,
    },
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "select")]
    Select { options: Vec<SelectOption> },
    #[serde(rename = "slider")]
    Slider {
        min: f64,
        max: f64,
        step: f64,
        unit: &'static str,
    },
    #[serde(rename = "bytes")]
    Bytes { min: usize, max: usize },
    #[serde(rename = "duration")]
    Duration { min_secs: u64, max_secs: u64 },
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectOption {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SectionDescriptor {
    pub key: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub fields: Vec<SettingDescriptor>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SettingsSchema {
    pub sections: Vec<SectionDescriptor>,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldError {
    pub section: String,
    pub field: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationResult {
    pub errors: Vec<FieldError>,
    pub warnings: Vec<FieldError>,
}

impl ValidationResult {
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

fn err(section: &str, field: &str, msg: impl Into<String>) -> FieldError {
    FieldError {
        section: section.into(),
        field: field.into(),
        message: msg.into(),
    }
}

pub fn validate(config: &AppConfig) -> ValidationResult {
    let mut r = ValidationResult::default();

    // --- server ---
    if config.server.host.parse::<IpAddr>().is_err() {
        r.errors
            .push(err("server", "host", "Must be a valid IP address"));
    }
    if config.server.port == 0 {
        r.errors
            .push(err("server", "port", "Port must be between 1 and 65535"));
    }
    if config.server.query_cache_entries > 100_000 {
        r.warnings.push(err(
            "server",
            "query_cache_entries",
            "Very large cache may use excessive memory",
        ));
    }

    // --- ipfix ---
    if config.ipfix.host.parse::<IpAddr>().is_err() {
        r.errors
            .push(err("ipfix", "host", "Must be a valid IP address"));
    }
    if config.ipfix.port == 0 {
        r.errors
            .push(err("ipfix", "port", "Port must be between 1 and 65535"));
    }
    if !config.ipfix.udp && !config.ipfix.tcp {
        r.errors.push(err(
            "ipfix",
            "udp",
            "At least one of UDP or TCP must be enabled",
        ));
    }
    if !(1500..=1_048_576).contains(&config.ipfix.udp_recv_buffer) {
        r.errors.push(err(
            "ipfix",
            "udp_recv_buffer",
            "Must be between 1500 and 1048576 bytes",
        ));
    }
    if !(60..=86400).contains(&config.ipfix.template_expiry_secs) {
        r.errors.push(err(
            "ipfix",
            "template_expiry_secs",
            "Must be between 60 and 86400 seconds",
        ));
    } else if config.ipfix.template_expiry_secs < 300 {
        r.warnings.push(err(
            "ipfix",
            "template_expiry_secs",
            "Templates may expire too quickly; RFC 7011 recommends at least 3x export interval",
        ));
    }
    if !(60..=86400).contains(&config.ipfix.unprocessed_ttl_secs) {
        r.errors.push(err(
            "ipfix",
            "unprocessed_ttl_secs",
            "Must be between 60 and 86400 seconds",
        ));
    }
    if !(1..=3600).contains(&config.ipfix.unprocessed_scan_interval_secs) {
        r.errors.push(err(
            "ipfix",
            "unprocessed_scan_interval_secs",
            "Must be between 1 and 3600 seconds",
        ));
    }

    // --- storage ---
    let mb = 1024 * 1024;
    if !(mb..=1024 * mb).contains(&config.storage.flush_bytes) {
        r.errors.push(err(
            "storage",
            "flush_bytes",
            "Must be between 1 MB and 1 GB",
        ));
    } else if config.storage.flush_bytes < 4 * mb {
        r.warnings.push(err(
            "storage",
            "flush_bytes",
            "Very small flush threshold will create many small parts",
        ));
    }
    if !(1..=3600).contains(&config.storage.flush_interval_secs) {
        r.errors.push(err(
            "storage",
            "flush_interval_secs",
            "Must be between 1 and 3600 seconds",
        ));
    }
    if !(64..=1_048_576).contains(&config.storage.channel_capacity) {
        r.errors.push(err(
            "storage",
            "channel_capacity",
            "Must be between 64 and 1048576",
        ));
    }
    if !(1024..=1_048_576).contains(&config.storage.initial_row_capacity) {
        r.errors.push(err(
            "storage",
            "initial_row_capacity",
            "Must be between 1024 and 1048576",
        ));
    }
    if config.storage.merge_workers > 64 {
        r.errors
            .push(err("storage", "merge_workers", "Must be between 1 and 64"));
    }
    if !(1..=3600).contains(&config.storage.merge_scan_interval_secs) {
        r.errors.push(err(
            "storage",
            "merge_scan_interval_secs",
            "Must be between 1 and 3600 seconds",
        ));
    }
    if !(0.1..=1.0).contains(&config.storage.merge_cpu_throttle) {
        r.errors.push(err(
            "storage",
            "merge_cpu_throttle",
            "Must be between 0.1 and 1.0",
        ));
    }
    if !(0.1..=1.0).contains(&config.storage.merge_mem_throttle) {
        r.errors.push(err(
            "storage",
            "merge_mem_throttle",
            "Must be between 0.1 and 1.0",
        ));
    }
    let cache_min = 16 * mb;
    let cache_max = 128usize.saturating_mul(1024 * mb);
    if !(cache_min..=cache_max).contains(&config.storage.storage_cache_bytes) {
        r.errors.push(err(
            "storage",
            "storage_cache_bytes",
            "Must be between 16 MB and 128 GB",
        ));
    }
    if config.storage.retention_hours != 0 && !(1..=87600).contains(&config.storage.retention_hours)
    {
        r.errors.push(err(
            "storage",
            "retention_hours",
            "Must be 0 (disabled) or between 1 and 87600 hours (10 years)",
        ));
    } else if config.storage.retention_hours > 0 && config.storage.retention_hours < 24 {
        r.warnings.push(err(
            "storage",
            "retention_hours",
            "Very short retention period; data will be deleted quickly",
        ));
    }
    if !(60..=86400).contains(&config.storage.retention_scan_interval_secs) {
        r.errors.push(err(
            "storage",
            "retention_scan_interval_secs",
            "Must be between 60 and 86400 seconds",
        ));
    }
    if config.storage.compression_level < 1 {
        r.errors.push(err(
            "storage",
            "compression_level",
            "Must be at least 1 (zstd minimum)",
        ));
    } else if config.storage.compression_level > 6 {
        r.warnings.push(err(
            "storage",
            "compression_level",
            "High compression levels may slow ingestion under heavy load",
        ));
    }
    if config.storage.merge_min_parts < 2 {
        r.errors.push(err(
            "storage",
            "merge_min_parts",
            "Must be at least 2; merging requires at least two source parts",
        ));
    } else if config.storage.merge_min_parts > 8 {
        r.warnings.push(err(
            "storage",
            "merge_min_parts",
            "Many small parts will accumulate between merges, which may degrade query performance",
        ));
    }
    if config.storage.max_aggregate_rows == 0 {
        r.errors.push(err(
            "storage",
            "max_aggregate_rows",
            "Must be greater than 0",
        ));
    } else if config.storage.max_aggregate_rows > 50_000_000 {
        r.warnings.push(err(
            "storage",
            "max_aggregate_rows",
            "Very high limit; aggregation queries may use multiple GB of memory",
        ));
    }

    // --- logging ---
    if config.logging.filter.trim().is_empty() {
        r.errors
            .push(err("logging", "filter", "Log filter must not be empty"));
    }

    r
}

// ---------------------------------------------------------------------------
// Schema generation
// ---------------------------------------------------------------------------

/// Build the full schema for the frontend settings UI.
pub fn schema() -> SettingsSchema {
    let defaults = AppConfig::default();
    let dj = serde_json::to_value(&defaults).unwrap_or_default();

    SettingsSchema {
        sections: vec![
            SectionDescriptor {
                key: "server",
                label: "Server",
                description: "HTTP server and API configuration",
                fields: vec![
                    SettingDescriptor {
                        section: "server",
                        key: "host",
                        label: "Listen Address",
                        description: "IP address to bind the HTTP server to.",
                        guidance: "Use 0.0.0.0 for all interfaces, 127.0.0.1 for localhost only.",
                        control: ControlType::Text,
                        default_value: dj["server"]["host"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "server",
                        key: "port",
                        label: "HTTP Port",
                        description: "Port for the web UI and API.",
                        guidance: "Avoid ports below 1024 unless running as root. Default is 2137.",
                        control: ControlType::Number {
                            min: Some(1.0),
                            max: Some(65535.0),
                            step: Some(1.0),
                            unit: "",
                        },
                        default_value: dj["server"]["port"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "server",
                        key: "query_cache_entries",
                        label: "Query Cache Size",
                        description: "Maximum number of cached query results.",
                        guidance: "Higher = more memory but fewer re-executions. 0 disables caching.",
                        control: ControlType::Number {
                            min: Some(0.0),
                            max: Some(100_000.0),
                            step: Some(1.0),
                            unit: "entries",
                        },
                        default_value: dj["server"]["query_cache_entries"].clone(),
                        restart_required: false,
                    },
                ],
            },
            SectionDescriptor {
                key: "ipfix",
                label: "IPFIX Collector",
                description: "IPFIX/NetFlow collection endpoint configuration",
                fields: vec![
                    SettingDescriptor {
                        section: "ipfix",
                        key: "host",
                        label: "Collector Address",
                        description: "IP address for the IPFIX listener.",
                        guidance: "0.0.0.0 accepts from all sources.",
                        control: ControlType::Text,
                        default_value: dj["ipfix"]["host"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "ipfix",
                        key: "port",
                        label: "Collector Port",
                        description: "Port for the IPFIX/NetFlow collector.",
                        guidance: "Standard IPFIX port is 4739. NetFlow v9 commonly uses 2055 or 9995.",
                        control: ControlType::Number {
                            min: Some(1.0),
                            max: Some(65535.0),
                            step: Some(1.0),
                            unit: "",
                        },
                        default_value: dj["ipfix"]["port"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "ipfix",
                        key: "udp",
                        label: "Enable UDP",
                        description: "Listen for IPFIX data over UDP.",
                        guidance: "Most exporters use UDP. Disable only if using TCP exclusively.",
                        control: ControlType::Bool,
                        default_value: dj["ipfix"]["udp"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "ipfix",
                        key: "tcp",
                        label: "Enable TCP",
                        description: "Listen for IPFIX data over TCP.",
                        guidance: "TCP provides reliable delivery. Enable alongside UDP for exporters that support it.",
                        control: ControlType::Bool,
                        default_value: dj["ipfix"]["tcp"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "ipfix",
                        key: "udp_recv_buffer",
                        label: "UDP Receive Buffer",
                        description: "OS socket receive buffer size.",
                        guidance: "Increase if you see packet drops under high volume.",
                        control: ControlType::Bytes {
                            min: 1500,
                            max: 1_048_576,
                        },
                        default_value: dj["ipfix"]["udp_recv_buffer"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "ipfix",
                        key: "template_expiry_secs",
                        label: "Template Expiry",
                        description: "How long to keep IPFIX templates before requiring re-announcement.",
                        guidance: "RFC 7011 recommends at least 3x the export interval. Default is 30 minutes.",
                        control: ControlType::Duration {
                            min_secs: 60,
                            max_secs: 86400,
                        },
                        default_value: dj["ipfix"]["template_expiry_secs"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "ipfix",
                        key: "unprocessed_dir",
                        label: "Unprocessed Directory",
                        description: "Directory for storing raw IPFIX data files that failed processing.",
                        guidance: "Relative to storage directory.",
                        control: ControlType::Text,
                        default_value: dj["ipfix"]["unprocessed_dir"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "ipfix",
                        key: "unprocessed_ttl_secs",
                        label: "Unprocessed File TTL",
                        description: "How long to keep unprocessed files before deletion.",
                        guidance: "Set higher if you want more time to investigate failed processing.",
                        control: ControlType::Duration {
                            min_secs: 60,
                            max_secs: 86400,
                        },
                        default_value: dj["ipfix"]["unprocessed_ttl_secs"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "ipfix",
                        key: "unprocessed_scan_interval_secs",
                        label: "Unprocessed Scan Interval",
                        description: "How often to check for reprocessable or expired unprocessed files.",
                        guidance: "Lower values detect failed files faster but add minor I/O overhead.",
                        control: ControlType::Duration {
                            min_secs: 1,
                            max_secs: 3600,
                        },
                        default_value: dj["ipfix"]["unprocessed_scan_interval_secs"].clone(),
                        restart_required: true,
                    },
                ],
            },
            SectionDescriptor {
                key: "storage",
                label: "Storage",
                description: "Columnar storage engine tuning",
                fields: vec![
                    SettingDescriptor {
                        section: "storage",
                        key: "flush_bytes",
                        label: "Flush Threshold",
                        description: "Write buffer is flushed to disk when it exceeds this size.",
                        guidance: "Larger = fewer but bigger disk writes. 16 MB is a good default for most workloads.",
                        control: ControlType::Bytes {
                            min: 1_048_576,
                            max: 1_073_741_824,
                        },
                        default_value: dj["storage"]["flush_bytes"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "flush_interval_secs",
                        label: "Flush Interval",
                        description: "Maximum seconds before the write buffer is flushed regardless of size.",
                        guidance: "Lower = fresher data for queries. Higher = fewer disk writes.",
                        control: ControlType::Duration {
                            min_secs: 1,
                            max_secs: 3600,
                        },
                        default_value: dj["storage"]["flush_interval_secs"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "channel_capacity",
                        label: "Ingestion Channel Size",
                        description: "Backpressure threshold between IPFIX decoder and storage writer.",
                        guidance: "Increase if you see ingestion stalls under high flow volume.",
                        control: ControlType::Number {
                            min: Some(64.0),
                            max: Some(1_048_576.0),
                            step: Some(1.0),
                            unit: "rows",
                        },
                        default_value: dj["storage"]["channel_capacity"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "initial_row_capacity",
                        label: "Row Buffer Pre-allocation",
                        description: "Pre-allocated rows per column buffer.",
                        guidance: "Higher = fewer reallocations but more initial memory usage.",
                        control: ControlType::Number {
                            min: Some(1024.0),
                            max: Some(1_048_576.0),
                            step: Some(1024.0),
                            unit: "rows",
                        },
                        default_value: dj["storage"]["initial_row_capacity"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "storage_cache_bytes",
                        label: "Storage Cache Size",
                        description: "LRU cache for decoded columns, bloom filters, marks, and metadata.",
                        guidance: "Larger cache = faster repeated queries. 1 GB is a good starting point.",
                        control: ControlType::Bytes {
                            min: 16_777_216,
                            max: 137_438_953_472,
                        },
                        default_value: dj["storage"]["storage_cache_bytes"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "compression_level",
                        label: "Compression Level",
                        description: "Zstd compression level for new parts.",
                        guidance: "Lower levels write faster at the cost of larger files. Higher levels compress better but slow down ingestion flushes. Level 3 is recommended for most workloads.",
                        control: ControlType::Number {
                            min: Some(1.0),
                            max: None,
                            step: Some(1.0),
                            unit: "",
                        },
                        default_value: dj["storage"]["compression_level"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "max_aggregate_rows",
                        label: "Max Aggregate Rows",
                        description: "Maximum rows matched during an aggregation query before it is rejected.",
                        guidance: "Higher values allow larger aggregations but risk high memory use. 10M rows ≈ 120 MB.",
                        control: ControlType::Number {
                            min: Some(1.0),
                            max: None,
                            step: Some(100_000.0),
                            unit: "rows",
                        },
                        default_value: dj["storage"]["max_aggregate_rows"].clone(),
                        restart_required: false,
                    },
                ],
            },
            SectionDescriptor {
                key: "merge",
                label: "Merge & Compaction",
                description: "Background merge workers and data lifecycle",
                fields: vec![
                    SettingDescriptor {
                        section: "storage",
                        key: "merge_workers",
                        label: "Merge Workers",
                        description: "Number of parallel merge threads for background compaction.",
                        guidance: "Set to number of CPU cores or less. 0 disables background merge entirely.",
                        control: ControlType::Number {
                            min: Some(0.0),
                            max: Some(64.0),
                            step: Some(1.0),
                            unit: "threads",
                        },
                        default_value: dj["storage"]["merge_workers"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "merge_scan_interval_secs",
                        label: "Merge Scan Interval",
                        description: "How often the merge coordinator checks for parts to compact.",
                        guidance: "Lower values detect new parts faster but add minor overhead.",
                        control: ControlType::Duration {
                            min_secs: 1,
                            max_secs: 3600,
                        },
                        default_value: dj["storage"]["merge_scan_interval_secs"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "merge_cpu_throttle",
                        label: "CPU Throttle",
                        description: "Pause merges when system CPU usage exceeds this fraction.",
                        guidance: "Protects query latency under load. 0.8 means pause when CPU is above 80%.",
                        control: ControlType::Slider {
                            min: 0.1,
                            max: 1.0,
                            step: 0.05,
                            unit: "",
                        },
                        default_value: dj["storage"]["merge_cpu_throttle"].clone(),
                        restart_required: false,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "merge_mem_throttle",
                        label: "Memory Throttle",
                        description: "Pause merges when system memory usage exceeds this fraction.",
                        guidance: "Prevents OOM under heavy load. 0.85 means pause when memory is above 85%.",
                        control: ControlType::Slider {
                            min: 0.1,
                            max: 1.0,
                            step: 0.05,
                            unit: "",
                        },
                        default_value: dj["storage"]["merge_mem_throttle"].clone(),
                        restart_required: false,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "merge_queue_length",
                        label: "Merge Queue Length",
                        description: "Maximum number of merge jobs queued at once.",
                        guidance: "Controls how many hours can be queued for merge simultaneously. Higher values allow more batching but use more memory for tracking.",
                        control: ControlType::Number {
                            min: Some(1.0),
                            max: Some(64.0),
                            step: Some(1.0),
                            unit: "jobs",
                        },
                        default_value: dj["storage"]["merge_queue_length"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "merge_min_parts",
                        label: "Min Parts to Merge",
                        description: "Minimum number of parts in an hour before a merge is triggered.",
                        guidance: "Higher values batch more parts per merge, reducing merge overhead but leaving data fragmented longer.",
                        control: ControlType::Number {
                            min: Some(2.0),
                            max: None,
                            step: Some(1.0),
                            unit: "parts",
                        },
                        default_value: dj["storage"]["merge_min_parts"].clone(),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "retention_hours",
                        label: "Data Retention",
                        description: "Hours to keep flow data before automatic deletion.",
                        guidance: "744 = 31 days. Set to 0 to disable automatic deletion entirely.",
                        control: ControlType::Number {
                            min: Some(0.0),
                            max: Some(87600.0),
                            step: Some(1.0),
                            unit: "hours",
                        },
                        default_value: dj["storage"]["retention_hours"].clone(),
                        restart_required: false,
                    },
                    SettingDescriptor {
                        section: "storage",
                        key: "retention_scan_interval_secs",
                        label: "Retention Scan Interval",
                        description: "How often to scan for and delete expired data partitions.",
                        guidance: "Lower values enforce retention more promptly.",
                        control: ControlType::Duration {
                            min_secs: 60,
                            max_secs: 86400,
                        },
                        default_value: dj["storage"]["retention_scan_interval_secs"].clone(),
                        restart_required: false,
                    },
                ],
            },
            SectionDescriptor {
                key: "logging",
                label: "Logging",
                description: "Log output format and verbosity",
                fields: vec![
                    SettingDescriptor {
                        section: "logging",
                        key: "format",
                        label: "Log Format",
                        description: "Output format for log messages.",
                        guidance: "Human: colored structured output for development. JSON: machine-parseable for log aggregators.",
                        control: ControlType::Select {
                            options: vec![
                                SelectOption {
                                    value: "human".into(),
                                    label: "Human-readable".into(),
                                },
                                SelectOption {
                                    value: "json".into(),
                                    label: "JSON".into(),
                                },
                            ],
                        },
                        default_value: serde_json::Value::String("human".into()),
                        restart_required: true,
                    },
                    SettingDescriptor {
                        section: "logging",
                        key: "filter",
                        label: "Log Filter",
                        description: "tracing-subscriber filter directive controlling log verbosity.",
                        guidance: "Examples: 'debug', 'info,flowcus_storage=trace', 'warn'. See tracing-subscriber docs for syntax.",
                        control: ControlType::Text,
                        default_value: dj["logging"]["filter"].clone(),
                        restart_required: true,
                    },
                ],
            },
        ],
    }
}

// ---------------------------------------------------------------------------
// Internal fields — excluded from API/UI
// ---------------------------------------------------------------------------

/// Fields that are internal and should not be exposed to the settings UI.
const INTERNAL_STORAGE_FIELDS: &[&str] = &[
    "granule_size",
    "partition_duration_secs",
    "bloom_bits_per_granule",
    "dir",
];

const INTERNAL_SERVER_FIELDS: &[&str] = &["dev_mode", "frontend_proxy"];

// ---------------------------------------------------------------------------
// JSON conversion (strips internal fields)
// ---------------------------------------------------------------------------

/// Convert an `AppConfig` to a JSON value, stripping internal fields.
pub fn config_to_json(config: &AppConfig) -> serde_json::Value {
    let mut v = serde_json::to_value(config).unwrap_or_default();
    strip_internal_fields(&mut v);
    v
}

/// Return the default `AppConfig` as JSON, stripping internal fields.
pub fn defaults_json() -> serde_json::Value {
    config_to_json(&AppConfig::default())
}

fn strip_internal_fields(v: &mut serde_json::Value) {
    if let Some(storage) = v.get_mut("storage").and_then(|s| s.as_object_mut()) {
        for key in INTERNAL_STORAGE_FIELDS {
            storage.remove(*key);
        }
    }
    if let Some(server) = v.get_mut("server").and_then(|s| s.as_object_mut()) {
        for key in INTERNAL_SERVER_FIELDS {
            server.remove(*key);
        }
    }
}

// ---------------------------------------------------------------------------
// Partial update — merge JSON patch into existing config
// ---------------------------------------------------------------------------

/// Apply a partial JSON update to a base config.
///
/// Only updates fields present in the patch; missing fields keep their
/// current values. Internal fields in the patch are ignored.
pub fn apply_partial(base: &AppConfig, patch: &serde_json::Value) -> crate::Result<AppConfig> {
    let mut base_json =
        serde_json::to_value(base).map_err(|e| crate::Error::Config(e.to_string()))?;

    if let (Some(base_obj), Some(patch_obj)) = (base_json.as_object_mut(), patch.as_object()) {
        for (section_key, section_patch) in patch_obj {
            if let (Some(base_section), Some(patch_section)) = (
                base_obj
                    .get_mut(section_key)
                    .and_then(|s| s.as_object_mut()),
                section_patch.as_object(),
            ) {
                for (field_key, field_value) in patch_section {
                    // Skip internal fields
                    if section_key == "storage"
                        && INTERNAL_STORAGE_FIELDS.contains(&field_key.as_str())
                    {
                        continue;
                    }
                    if section_key == "server"
                        && INTERNAL_SERVER_FIELDS.contains(&field_key.as_str())
                    {
                        continue;
                    }
                    base_section.insert(field_key.clone(), field_value.clone());
                }
            }
        }
    }

    serde_json::from_value(base_json).map_err(|e| crate::Error::Config(e.to_string()))
}

// ---------------------------------------------------------------------------
// Detect which changed fields require a restart
// ---------------------------------------------------------------------------

/// Compare two configs and return a list of "section.field" keys that changed
/// and require a server restart.
pub fn changed_restart_fields(old: &AppConfig, new: &AppConfig) -> Vec<String> {
    let schema = schema();
    let old_json = serde_json::to_value(old).unwrap_or_default();
    let new_json = serde_json::to_value(new).unwrap_or_default();

    let mut changed = Vec::new();
    for section in &schema.sections {
        for field in &section.fields {
            if !field.restart_required {
                continue;
            }
            let old_val = &old_json[field.section][field.key];
            let new_val = &new_json[field.section][field.key];
            if old_val != new_val {
                changed.push(format!("{}.{}", field.section, field.key));
            }
        }
    }
    changed
}

// ---------------------------------------------------------------------------
// File I/O
// ---------------------------------------------------------------------------

/// Section comments injected into the generated settings file.
const SECTION_COMMENTS: &[(&str, &str)] = &[
    (
        "[logging]",
        "# Logging\n# Log output format and verbosity.\n",
    ),
    (
        "[server]",
        "# Server\n# HTTP server and API configuration.\n",
    ),
    (
        "[ipfix]",
        "# IPFIX Collector\n# IPFIX/NetFlow collection endpoint configuration.\n",
    ),
    (
        "[storage]",
        "# Storage & Merge\n# Columnar storage engine tuning and background compaction.\n",
    ),
];

/// Save an `AppConfig` to a TOML file with section comments.
pub fn save(path: &Path, config: &AppConfig) -> crate::Result<()> {
    let raw = toml::to_string_pretty(config).map_err(|e| crate::Error::Config(e.to_string()))?;

    let mut out = String::with_capacity(raw.len() + 256);
    out.push_str("# Flowcus Settings\n");
    out.push_str("# Edit via the Settings panel in the web UI, or modify this file directly.\n");
    out.push_str("# Changes take effect after a server restart.\n\n");

    for line in raw.lines() {
        // Inject section comments before TOML section headers
        for &(header, comment) in SECTION_COMMENTS {
            if line == header {
                out.push_str(comment);
            }
        }
        out.push_str(line);
        out.push('\n');
    }

    // Atomic write: write to temp file, then rename
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &out)?;
    std::fs::rename(&tmp, path)?;

    info!(path = %path.display(), "Settings saved");
    Ok(())
}

/// Load settings from file, or create file with defaults if missing.
///
/// If `flowcus.toml` exists in the same directory and `path` does not,
/// the TOML file is migrated.
pub fn load_or_create(path: &Path) -> crate::Result<AppConfig> {
    if path.exists() {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;
        info!(path = %path.display(), "Settings loaded");
        return Ok(config);
    }

    // Migration: check for flowcus.toml in the same directory
    let legacy = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("flowcus.toml");

    if legacy.exists() {
        let content = std::fs::read_to_string(&legacy)?;
        let config: AppConfig = toml::from_str(&content)?;
        save(path, &config)?;
        warn!(
            legacy = %legacy.display(),
            new = %path.display(),
            "Migrated configuration from flowcus.toml to flowcus.settings"
        );
        return Ok(config);
    }

    // First run — create with defaults
    let config = AppConfig::default();
    save(path, &config)?;
    info!(path = %path.display(), "Created default settings file");
    Ok(config)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_five_sections() {
        let s = schema();
        assert_eq!(s.sections.len(), 5);
        assert_eq!(s.sections[0].key, "server");
        assert_eq!(s.sections[1].key, "ipfix");
        assert_eq!(s.sections[2].key, "storage");
        assert_eq!(s.sections[3].key, "merge");
        assert_eq!(s.sections[4].key, "logging");
    }

    #[test]
    fn defaults_pass_validation() {
        let config = AppConfig::default();
        let result = validate(&config);
        assert!(
            result.errors.is_empty(),
            "default config has errors: {:?}",
            result.errors
        );
    }

    #[test]
    fn invalid_port_is_caught() {
        let mut config = AppConfig::default();
        config.server.port = 0;
        let result = validate(&config);
        assert!(result.errors.iter().any(|e| e.field == "port"));
    }

    #[test]
    fn invalid_throttle_is_caught() {
        let mut config = AppConfig::default();
        config.storage.merge_cpu_throttle = 1.5;
        let result = validate(&config);
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.field == "merge_cpu_throttle")
        );
    }

    #[test]
    fn internal_fields_stripped_from_json() {
        let config = AppConfig::default();
        let json = config_to_json(&config);
        let storage = json["storage"].as_object().unwrap();
        assert!(!storage.contains_key("granule_size"));
        assert!(!storage.contains_key("partition_duration_secs"));
        assert!(!storage.contains_key("bloom_bits_per_granule"));
        assert!(!storage.contains_key("dir"));
        let server = json["server"].as_object().unwrap();
        assert!(!server.contains_key("dev_mode"));
        assert!(!server.contains_key("frontend_proxy"));
    }

    #[test]
    fn apply_partial_preserves_unset_fields() {
        let base = AppConfig::default();
        let patch = serde_json::json!({
            "server": { "port": 9999 }
        });
        let new = apply_partial(&base, &patch).unwrap();
        assert_eq!(new.server.port, 9999);
        assert_eq!(new.server.host, base.server.host);
        assert_eq!(new.ipfix.port, base.ipfix.port);
    }

    #[test]
    fn apply_partial_ignores_internal_fields() {
        let base = AppConfig::default();
        let patch = serde_json::json!({
            "storage": { "granule_size": 999 }
        });
        let new = apply_partial(&base, &patch).unwrap();
        assert_eq!(new.storage.granule_size, base.storage.granule_size);
    }

    #[test]
    fn changed_restart_fields_detects_port_change() {
        let old = AppConfig::default();
        let mut new = old.clone();
        new.server.port = 9999;
        let fields = changed_restart_fields(&old, &new);
        assert!(fields.contains(&"server.port".to_string()));
    }

    #[test]
    fn changed_restart_fields_ignores_non_restart_changes() {
        let old = AppConfig::default();
        let mut new = old.clone();
        new.storage.merge_cpu_throttle = 0.5;
        let fields = changed_restart_fields(&old, &new);
        assert!(fields.is_empty());
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("flowcus_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.settings");

        let mut config = AppConfig::default();
        config.server.port = 8080;
        config.storage.retention_hours = 168;

        save(&path, &config).unwrap();
        let loaded = load_or_create(&path).unwrap();

        assert_eq!(loaded.server.port, 8080);
        assert_eq!(loaded.storage.retention_hours, 168);
        // Internal fields are preserved in file (just not exposed in UI)
        assert_eq!(loaded.storage.granule_size, config.storage.granule_size);

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
