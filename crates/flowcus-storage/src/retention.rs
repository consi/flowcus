//! Background worker that enforces data retention by removing expired parts.
//!
//! Parts whose `time_max` (encoded in the directory name) is older than
//! `now - retention_hours` are deleted, provided they are not currently being
//! merged (indicated by a `.merging` sentinel file).
//!
//! After removing parts, empty parent directories (hour, day, month, year) are
//! cleaned up to keep the tree tidy.
//!
//! **Crash safety**: every removal targets a single directory via
//! `std::fs::remove_dir_all`. If the process crashes mid-walk, the worst case
//! is that some expired parts survive until the next scan — no data is lost or
//! corrupted. Attempting to remove a directory that no longer exists (e.g. from
//! a concurrent merge) is silently ignored.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{debug, info, warn};

/// Spawn the retention worker as a background tokio task.
///
/// `retention_hours == 0` disables retention entirely (the task is not spawned).
pub fn start(
    table_base: PathBuf,
    retention_hours: u64,
    scan_interval_secs: u64,
    metrics: Arc<flowcus_core::observability::Metrics>,
) {
    if retention_hours == 0 {
        info!("Data retention disabled (retention_hours = 0)");
        return;
    }

    info!(
        retention_hours,
        scan_interval_secs, "Starting data retention worker"
    );

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(scan_interval_secs));
        // First tick fires immediately — skip it to give the system time to start up.
        interval.tick().await;

        loop {
            interval.tick().await;

            let base = table_base.clone();
            let hours = retention_hours;
            let m = Arc::clone(&metrics);

            // Filesystem walk is blocking I/O — run off the async runtime.
            if let Err(e) =
                tokio::task::spawn_blocking(move || enforce_retention(&base, hours, Some(&m))).await
            {
                warn!(error = %e, "Retention worker task panicked");
            }
        }
    });
}

/// Single retention pass: scan the directory tree and remove expired parts.
///
/// `metrics` is optional so unit tests can call this without constructing a
/// full `Metrics` instance.
fn enforce_retention(
    table_base: &Path,
    retention_hours: u64,
    metrics: Option<&Arc<flowcus_core::observability::Metrics>>,
) {
    let now_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let cutoff = now_epoch.saturating_sub(retention_hours * 3600);
    let cutoff_u32 = u32::try_from(cutoff).unwrap_or(u32::MAX);

    let mut removed_count: u64 = 0;
    let mut skipped_merging: u64 = 0;

    let Ok(year_entries) = fs::read_dir(table_base) else {
        return;
    };

    for y in year_entries.flatten() {
        if !is_dir(&y) {
            continue;
        }
        let Ok(month_entries) = fs::read_dir(y.path()) else {
            continue;
        };
        for m in month_entries.flatten() {
            if !is_dir(&m) {
                continue;
            }
            let Ok(day_entries) = fs::read_dir(m.path()) else {
                continue;
            };
            for d in day_entries.flatten() {
                if !is_dir(&d) {
                    continue;
                }
                let Ok(hour_entries) = fs::read_dir(d.path()) else {
                    continue;
                };
                for h in hour_entries.flatten() {
                    if !is_dir(&h) {
                        continue;
                    }
                    let Ok(part_entries) = fs::read_dir(h.path()) else {
                        continue;
                    };
                    for p in part_entries.flatten() {
                        if !is_dir(&p) {
                            continue;
                        }
                        let name = p.file_name();
                        let name_str = name.to_string_lossy();

                        let Some((_, _, _, time_max, _)) =
                            crate::part::parse_part_dir_name_versioned(&name_str)
                        else {
                            continue;
                        };

                        if time_max >= cutoff_u32 {
                            continue;
                        }

                        // Do not remove parts that are currently being merged.
                        if p.path().join(".merging").exists() {
                            debug!(
                                part = %p.path().display(),
                                "Skipping expired part (merge in progress)"
                            );
                            skipped_merging += 1;
                            continue;
                        }

                        match fs::remove_dir_all(p.path()) {
                            Ok(()) => {
                                debug!(part = %p.path().display(), "Removed expired part");
                                removed_count += 1;
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                // Already removed (race with merge or concurrent retention).
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    part = %p.path().display(),
                                    "Failed to remove expired part"
                                );
                            }
                        }
                    }

                    // Clean up empty hour directory.
                    remove_if_empty(&h.path());
                }

                // Clean up empty day directory.
                remove_if_empty(&d.path());
            }

            // Clean up empty month directory.
            remove_if_empty(&m.path());
        }

        // Clean up empty year directory.
        remove_if_empty(&y.path());
    }

    if let Some(m) = metrics {
        m.retention_parts_removed
            .fetch_add(removed_count, Ordering::Relaxed);
        m.retention_passes.fetch_add(1, Ordering::Relaxed);
    }

    if removed_count > 0 || skipped_merging > 0 {
        info!(
            removed_count,
            skipped_merging, retention_hours, "Retention pass complete"
        );
    }
}

fn is_dir(entry: &fs::DirEntry) -> bool {
    entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false)
}

/// Remove a directory only if it is empty. Silently ignores errors (the
/// directory may have been repopulated or removed by another task).
fn remove_if_empty(path: &Path) {
    let Ok(mut entries) = fs::read_dir(path) else {
        return;
    };
    if entries.next().is_none() {
        let _ = fs::remove_dir(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_epoch() -> u32 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32
    }

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("flowcus_test_retention_{name}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Create a part directory structure: table_base/YYYY/MM/DD/HH/part_name/meta.bin
    fn create_part(table_base: &Path, part_name: &str) -> PathBuf {
        let part_dir = table_base
            .join("2024")
            .join("01")
            .join("15")
            .join("10")
            .join(part_name);
        fs::create_dir_all(&part_dir).unwrap();
        // Write a dummy file so the dir is non-empty (like a real part).
        fs::write(part_dir.join("meta.bin"), b"dummy").unwrap();
        part_dir
    }

    #[test]
    fn removes_expired_parts_and_keeps_recent() {
        let base = test_dir("expired");

        let old_ts = now_epoch() - 3600 * 24 * 60; // 60 days ago
        let recent_ts = now_epoch() - 3600; // 1 hour ago

        let old_name = format!("1_00000_1000000_{old_ts}_000001");
        let recent_name = format!("1_00000_1000000_{recent_ts}_000002");

        let old_part = create_part(&base, &old_name);
        let recent_part = create_part(&base, &recent_name);

        enforce_retention(&base, 744, None);

        assert!(!old_part.exists(), "expired part should be removed");
        assert!(recent_part.exists(), "recent part should survive");

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn skips_parts_with_merging_sentinel() {
        let base = test_dir("merging");

        let old_ts = now_epoch() - 3600 * 24 * 60;
        let name = format!("1_00000_1000000_{old_ts}_000001");
        let part = create_part(&base, &name);

        // Place a .merging marker.
        fs::write(part.join(".merging"), b"source1\nsource2\n").unwrap();

        enforce_retention(&base, 744, None);

        assert!(part.exists(), "merging part should not be removed");

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn handles_missing_table_base_gracefully() {
        let nonexistent = std::env::temp_dir().join("flowcus_test_retention_missing");
        let _ = fs::remove_dir_all(&nonexistent);
        // Should not panic.
        enforce_retention(&nonexistent, 744, None);
    }

    #[test]
    fn cleans_up_empty_ancestor_dirs() {
        let base = test_dir("cleanup");

        let old_ts = now_epoch() - 3600 * 24 * 60;
        let name = format!("1_00000_1000000_{old_ts}_000001");
        create_part(&base, &name);

        enforce_retention(&base, 744, None);

        assert!(
            !base.join("2024").exists(),
            "empty year dir should be removed"
        );

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn legacy_part_name_format() {
        let base = test_dir("legacy");

        let old_ts = now_epoch() - 3600 * 24 * 60;
        // Legacy 4-segment format (no version prefix)
        let name = format!("00000_1000000_{old_ts}_000001");
        let part = create_part(&base, &name);

        enforce_retention(&base, 744, None);

        assert!(
            !part.exists(),
            "legacy-format expired part should be removed"
        );

        let _ = fs::remove_dir_all(&base);
    }
}
