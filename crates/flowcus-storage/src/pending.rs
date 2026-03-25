//! Pending merge tracker: knows which hour directories need merge attention.
//!
//! Avoids full tree traversal on every merge scan. Instead of walking
//! thousands of hour directories, the coordinator only checks the ones
//! registered here.
//!
//! Write path: ingestion marks an hour dirty after flushing a part.
//! Merge path: coordinator reads dirty set, scans only those hours,
//! removes hours that have converged to a single part.
//!
//! Persisted to `pending_merges.bin` so it survives restarts.
//! On startup, if the file is missing or corrupt, a one-time full scan
//! rebuilds it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tracing::{debug, info, trace, warn};

const PENDING_FILE: &str = "pending_merges.bin";
const PENDING_MAGIC: &[u8; 4] = b"FPND";

/// Shared pending-hours tracker. Thread-safe, used by both writer and merger.
#[derive(Clone)]
pub struct PendingHours {
    inner: Arc<Mutex<PendingInner>>,
}

struct PendingInner {
    /// Hour directories that have >1 part and need merge attention.
    dirty: BTreeSet<PathBuf>,
    /// Path to the persistence file.
    persist_path: PathBuf,
    /// Counter: how many marks since last persist.
    marks_since_persist: u32,
}

impl PendingHours {
    /// Load or create the pending tracker for a table.
    pub fn open(table_base: &Path) -> Self {
        let persist_path = table_base.join(PENDING_FILE);
        let dirty = load_pending(&persist_path).unwrap_or_default();
        let count = dirty.len();

        if count > 0 {
            info!(pending_hours = count, "Loaded pending merge set");
        }

        Self {
            inner: Arc::new(Mutex::new(PendingInner {
                dirty,
                persist_path,
                marks_since_persist: 0,
            })),
        }
    }

    /// Mark an hour directory as needing merge attention.
    /// Called by the writer after flushing a new part.
    pub fn mark_dirty(&self, hour_dir: PathBuf) {
        let mut inner = self.inner.lock().unwrap();
        let was_new = inner.dirty.insert(hour_dir.clone());
        if was_new {
            trace!(hour = %hour_dir.display(), "Hour marked dirty for merge");
        }
        inner.marks_since_persist += 1;
        // Persist periodically (every 16 marks) to limit I/O
        if inner.marks_since_persist >= 16 {
            persist_pending(&inner.persist_path, &inner.dirty);
            inner.marks_since_persist = 0;
        }
    }

    /// Mark an hour directory as fully merged (single part). Removes from set.
    pub fn mark_clean(&self, hour_dir: &Path) {
        let mut inner = self.inner.lock().unwrap();
        if inner.dirty.remove(hour_dir) {
            debug!(hour = %hour_dir.display(), "Hour fully merged, removed from pending");
            persist_pending(&inner.persist_path, &inner.dirty);
            inner.marks_since_persist = 0;
        }
    }

    /// Get all dirty hour directories. The coordinator scans only these.
    pub fn get_dirty(&self) -> Vec<PathBuf> {
        let inner = self.inner.lock().unwrap();
        inner.dirty.iter().cloned().collect()
    }

    /// Remove entries whose directories no longer exist on disk.
    /// Called periodically by the merge coordinator to prevent stale accumulation.
    pub fn prune_stale(&self) {
        let mut inner = self.inner.lock().unwrap();
        let before = inner.dirty.len();
        inner.dirty.retain(|p| p.exists());
        let pruned = before - inner.dirty.len();
        if pruned > 0 {
            debug!(pruned, "Pruned stale pending entries");
            persist_pending(&inner.persist_path, &inner.dirty);
        }
    }

    /// Number of hours pending merge.
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().dirty.len()
    }

    /// Force persist to disk (e.g., on shutdown).
    pub fn flush(&self) {
        let inner = self.inner.lock().unwrap();
        persist_pending(&inner.persist_path, &inner.dirty);
    }

    /// One-time full scan to rebuild the pending set.
    /// Called on startup if pending file was missing or corrupt.
    pub fn rebuild_from_disk(&self, table_base: &Path) {
        info!("Rebuilding pending merge set from disk");

        let mut dirty = BTreeSet::new();
        if let Ok(entries) = walk_hour_dirs(table_base) {
            for hour_dir in entries {
                let part_count = count_parts_in(&hour_dir);
                if part_count > 1 {
                    dirty.insert(hour_dir);
                }
            }
        }

        let count = dirty.len();
        let mut inner = self.inner.lock().unwrap();
        inner.dirty = dirty;
        persist_pending(&inner.persist_path, &inner.dirty);
        info!(pending_hours = count, "Pending set rebuilt");
    }
}

fn persist_pending(path: &Path, dirty: &BTreeSet<PathBuf>) {
    let mut buf = Vec::with_capacity(8 + dirty.len() * 64);
    buf.extend_from_slice(PENDING_MAGIC);
    buf.extend_from_slice(&(dirty.len() as u32).to_le_bytes());

    for path_entry in dirty {
        let bytes = path_entry.to_string_lossy();
        let bytes = bytes.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(bytes);
    }

    // Append CRC32-C of entire buffer
    let crc = crate::crc::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    if let Err(e) = std::fs::write(path, &buf) {
        warn!(error = %e, "Failed to persist pending merges");
    }
}

fn load_pending(path: &Path) -> Option<BTreeSet<PathBuf>> {
    let raw = std::fs::read(path).ok()?;
    if raw.len() < 12 || &raw[..4] != PENDING_MAGIC {
        return None;
    }

    // Verify trailing CRC32-C (last 4 bytes)
    let (buf, crc_bytes) = raw.split_at(raw.len() - 4);
    let stored_crc = u32::from_le_bytes(crc_bytes.try_into().ok()?);
    if !crate::crc::verify_crc32c(buf, stored_crc) {
        warn!("CRC mismatch in {}", path.display());
        return None;
    }

    let count = u32::from_le_bytes(buf[4..8].try_into().ok()?) as usize;
    let mut set = BTreeSet::new();
    let mut off = 8;

    for _ in 0..count {
        if off + 2 > buf.len() {
            break;
        }
        let len = u16::from_le_bytes(buf[off..off + 2].try_into().ok()?) as usize;
        off += 2;
        if off + len > buf.len() {
            break;
        }
        let s = std::str::from_utf8(&buf[off..off + len]).ok()?;
        set.insert(PathBuf::from(s));
        off += len;
    }

    Some(set)
}

/// Walk the time directory tree and return all hour-level directories.
fn walk_hour_dirs(base: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut hours = Vec::new();
    walk_dirs_recursive(base, 0, &mut hours)?;
    Ok(hours)
}

fn walk_dirs_recursive(dir: &Path, depth: u8, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if depth < 3 {
            walk_dirs_recursive(&entry.path(), depth + 1, out)?;
        } else {
            // depth 3 = hour level
            out.push(entry.path());
        }
    }
    Ok(())
}

/// Count part directories in an hour directory (public for merge cleanup).
pub fn count_parts_in(hour_dir: &Path) -> usize {
    std::fs::read_dir(hour_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
                .count()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_and_get_dirty() {
        let dir = std::env::temp_dir().join("flowcus_test_pending");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let tracker = PendingHours::open(&dir);
        assert_eq!(tracker.count(), 0);

        tracker.mark_dirty(PathBuf::from("/data/2026/03/23/10"));
        tracker.mark_dirty(PathBuf::from("/data/2026/03/23/11"));
        assert_eq!(tracker.count(), 2);

        let dirty = tracker.get_dirty();
        assert!(dirty.contains(&PathBuf::from("/data/2026/03/23/10")));
        assert!(dirty.contains(&PathBuf::from("/data/2026/03/23/11")));

        tracker.mark_clean(Path::new("/data/2026/03/23/10"));
        assert_eq!(tracker.count(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn persist_and_reload() {
        let dir = std::env::temp_dir().join("flowcus_test_pending2");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        {
            let tracker = PendingHours::open(&dir);
            tracker.mark_dirty(PathBuf::from("/a/b/c"));
            tracker.mark_dirty(PathBuf::from("/d/e/f"));
            tracker.flush();
        }

        // Reload from disk
        let tracker = PendingHours::open(&dir);
        assert_eq!(tracker.count(), 2);
        let dirty = tracker.get_dirty();
        assert!(dirty.contains(&PathBuf::from("/a/b/c")));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn idempotent_mark() {
        let dir = std::env::temp_dir().join("flowcus_test_pending3");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let tracker = PendingHours::open(&dir);
        tracker.mark_dirty(PathBuf::from("/x"));
        tracker.mark_dirty(PathBuf::from("/x"));
        tracker.mark_dirty(PathBuf::from("/x"));
        assert_eq!(tracker.count(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }
}
