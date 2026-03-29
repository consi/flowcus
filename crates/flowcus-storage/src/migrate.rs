//! Part format migration.
//!
//! Runs on startup to detect and migrate old-format parts to the current format.
//! Migration is non-blocking — runs in a background thread, parts remain queryable
//! during migration (the executor handles both old and new formats).
//!
//! # Version history
//!
//! - **v1** (current, 5-segment dir name): `1_{gen}_{min}_{max}_{seq}`
//!
//! No migrations are needed at this time. This module exists as scaffolding for
//! future data format migrations.

use std::path::PathBuf;

/// Start background migration on a tokio blocking thread.
///
/// Currently a no-op. When a new format version is introduced, add migration
/// logic here that scans for old parts and upgrades them in place.
pub fn start_background_migration(_table_base: PathBuf) {
    // No migrations needed — v1 is the only released format.
}
