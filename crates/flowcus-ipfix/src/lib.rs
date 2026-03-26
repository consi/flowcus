// IPFIX protocol parsing inherently requires numeric casts for wire format decoding.
// IE registries are large by nature (hundreds of entries).
// Doc lints relaxed: internal protocol crate, not a public API.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::too_many_lines,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown,
    clippy::option_if_let_else,
    clippy::manual_range_contains,
    clippy::match_same_arms,
    clippy::redundant_closure_for_method_calls,
    clippy::manual_let_else,
    clippy::map_unwrap_or,
    clippy::match_wildcard_for_single_variants,
    clippy::single_match,
    clippy::bool_to_int_with_if
)]

pub mod decoder;
pub mod display;
pub mod ie;
pub mod listener;
pub mod netflow;
pub mod protocol;
pub mod session;
pub mod unprocessed;

pub use listener::IpfixListener;
pub use protocol::{DataRecord, FieldValue, IpfixMessage, Set, TemplateRecord};
pub use session::SessionStore;
