// Storage engine: columnar layout with per-type codecs, immutable parts, buffered writes.
// No fsync - rely on kernel page cache. Optimized for sequential writes and future SIMD scans.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::ptr_as_ptr,
    clippy::borrow_as_ptr,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::module_name_repetitions,
    clippy::option_if_let_else,
    clippy::redundant_clone,
    clippy::manual_slice_size_calculation,
    clippy::match_same_arms,
    clippy::single_match_else,
    clippy::needless_lifetimes,
    clippy::missing_const_for_fn,
    clippy::ref_as_ptr,
    clippy::map_unwrap_or,
    clippy::io_other_error,
    clippy::cast_precision_loss,
    clippy::or_fun_call,
    clippy::redundant_closure_for_method_calls,
    clippy::significant_drop_in_scrutinee,
    clippy::needless_pass_by_value,
    clippy::significant_drop_tightening,
    clippy::string_lit_as_bytes,
    clippy::manual_string_new,
    clippy::assigning_clones,
    clippy::manual_range_contains,
    clippy::manual_let_else,
    clippy::if_not_else,
    clippy::match_wildcard_for_single_variants,
    clippy::too_many_lines,
    clippy::unused_self,
    clippy::collapsible_if,
    clippy::single_match,
    clippy::unnested_or_patterns,
    clippy::manual_range_patterns
)]

pub mod cache;
pub mod codec;
pub mod column;
pub mod crc;
pub mod decode;
pub mod executor;
pub mod granule;
pub mod ingest;
pub mod merge;
pub mod migrate;
pub mod part;
pub mod part_locks;
pub mod pending;
pub mod retention;
pub mod schema;
pub mod table;
pub mod uuid7;
pub mod writer;

pub use ingest::IngestionHandle;
pub use merge::MergeConfig;
pub use writer::WriterConfig;
