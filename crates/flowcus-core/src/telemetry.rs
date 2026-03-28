use std::fmt;

use serde::{Deserialize, Serialize};
use tracing_subscriber::{
    EnvFilter, fmt as tracing_fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

/// Log output format.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Colored, structured output optimized for human reading.
    /// Highlights important fields, uses indentation and formatting.
    #[default]
    Human,
    /// Structured JSON lines with all fields serialized.
    /// One JSON object per log event, suitable for log aggregation.
    Json,
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Human => write!(f, "human"),
            Self::Json => write!(f, "json"),
        }
    }
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "human" => Ok(Self::Human),
            "json" => Ok(Self::Json),
            other => Err(format!(
                "unknown log format '{other}', expected 'human' or 'json'"
            )),
        }
    }
}

/// Initialize the tracing/logging subsystem.
///
/// Respects `RUST_LOG` env var for filtering. Defaults to `info` level.
/// Format is selected explicitly via [`LogFormat`].
pub fn init(format: LogFormat, default_filter: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    let registry = tracing_subscriber::registry().with(filter);

    match format {
        LogFormat::Human => {
            registry
                .with(
                    tracing_fmt::layer()
                        .with_target(true)
                        .with_thread_names(true)
                        .with_level(true)
                        .with_ansi(true),
                )
                .init();
        }
        LogFormat::Json => {
            registry
                .with(
                    tracing_fmt::layer()
                        .json()
                        .with_current_span(true)
                        .with_span_list(true)
                        .with_thread_names(true),
                )
                .init();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_format_roundtrip() {
        assert_eq!("human".parse::<LogFormat>().unwrap(), LogFormat::Human);
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("JSON".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert!("xml".parse::<LogFormat>().is_err());
    }

    #[test]
    fn log_format_serde() {
        let json = serde_json::to_string(&LogFormat::Human).unwrap();
        assert_eq!(json, r#""human""#);
        let parsed: LogFormat = serde_json::from_str(r#""json""#).unwrap();
        assert_eq!(parsed, LogFormat::Json);
    }
}
