//! Per-part read/write lock registry for query-merge coordination.
//!
//! Prevents race conditions where a merge deletes source parts while a query
//! is mid-read, or a query reads a partially-written merged part.
//!
//! - **Queries** acquire a read lock before scanning a part. Multiple queries
//!   can read the same part concurrently.
//! - **Merges** acquire a write lock before deleting source parts. This blocks
//!   until all active readers finish. The write lock also covers the window
//!   between "new part fully written" and "marker removed" so queries don't
//!   see a half-ready merged part.
//! - **Timeout**: all lock attempts have a 100 ms timeout. Queries skip a part
//!   if they can't acquire the lock (merge in progress). Merges defer deletion
//!   to the next cycle if they can't acquire the lock.
//!
//! Power-failure safety: locks are in-memory only. On crash, all locks vanish.
//! The existing `.merging` marker + `recover_interrupted_merges` handles crash
//! recovery — it either completes the merge (removes remaining source parts)
//! or rolls it back (removes the incomplete merged part).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Lock acquisition timeout.
const LOCK_TIMEOUT: Duration = Duration::from_millis(100);

/// Shared lock registry. Clone is cheap (Arc interior).
#[derive(Clone)]
pub struct PartLocks {
    inner: Arc<Mutex<HashMap<PathBuf, Arc<RwLock<()>>>>>,
}

impl Default for PartLocks {
    fn default() -> Self {
        Self::new()
    }
}

impl PartLocks {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get or create a lock for the given part directory.
    fn get_lock(&self, part_dir: &Path) -> Arc<RwLock<()>> {
        self.inner
            .lock()
            .unwrap()
            .entry(part_dir.to_path_buf())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    /// Execute `f` while holding a read lock on `part_dir`.
    /// Returns `None` if the lock couldn't be acquired within 100 ms
    /// (merge deleting this part).
    pub fn with_read<F, R>(&self, part_dir: &Path, f: F) -> Option<R>
    where
        F: FnOnce() -> R,
    {
        let lock = self.get_lock(part_dir);
        let guard = try_read_timeout(&lock)?;
        let result = f();
        drop(guard);
        Some(result)
    }

    /// Execute `f` while holding a write lock on `part_dir`.
    /// Returns `None` if the lock couldn't be acquired within 100 ms
    /// (queries still reading this part).
    pub fn with_write<F, R>(&self, part_dir: &Path, f: F) -> Option<R>
    where
        F: FnOnce() -> R,
    {
        let lock = self.get_lock(part_dir);
        let guard = try_write_timeout(&lock)?;
        let result = f();
        drop(guard);
        Some(result)
    }

    /// Remove stale entries from the lock map. Call periodically to avoid
    /// unbounded growth from deleted parts.
    pub fn prune(&self) {
        let mut map = self.inner.lock().unwrap();
        // An entry is stale if nobody else holds a reference (strong_count == 1
        // means only the map holds it — no active lock).
        map.retain(|_, lock| Arc::strong_count(lock) > 1);
    }
}

/// Try to acquire a read lock within the timeout.
fn try_read_timeout(lock: &RwLock<()>) -> Option<std::sync::RwLockReadGuard<'_, ()>> {
    let deadline = Instant::now() + LOCK_TIMEOUT;
    loop {
        match lock.try_read() {
            Ok(guard) => return Some(guard),
            Err(std::sync::TryLockError::WouldBlock) => {
                if Instant::now() >= deadline {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(std::sync::TryLockError::Poisoned(_)) => return None,
        }
    }
}

/// Try to acquire a write lock within the timeout.
fn try_write_timeout(lock: &RwLock<()>) -> Option<std::sync::RwLockWriteGuard<'_, ()>> {
    let deadline = Instant::now() + LOCK_TIMEOUT;
    loop {
        match lock.try_write() {
            Ok(guard) => return Some(guard),
            Err(std::sync::TryLockError::WouldBlock) => {
                if Instant::now() >= deadline {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(std::sync::TryLockError::Poisoned(_)) => return None,
        }
    }
}
