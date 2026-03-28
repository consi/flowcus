pub mod config;
pub mod error;
#[allow(
    clippy::format_push_string,
    clippy::too_many_lines,
    clippy::cast_sign_loss
)]
pub mod observability;
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::format_push_string,
    clippy::doc_markdown,
    clippy::float_cmp,
    clippy::needless_pass_by_value,
    clippy::new_without_default,
    clippy::unreadable_literal
)]
pub mod profiling;
pub mod telemetry;

pub use config::AppConfig;
pub use error::{Error, Result};
pub use telemetry::LogFormat;
