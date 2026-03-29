//! Partitioned LRU cache for storage index structures.
//!
//! Caches marks, blooms, column indexes, and part metadata — the index
//! structures used for seeking and filtering. Column data (.col files)
//! is NOT cached here; it's read from disk and decoded per-query.
//!
//! Each data type has its own memory budget so marks can't evict blooms, etc.
//! Thread-safe via per-partition `Mutex`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::granule::{GranuleBloom, GranuleMark};
use crate::part::{ColumnIndexEntry, PartMetadataHeader};

// ---------------------------------------------------------------------------
// Generic LRU pool
// ---------------------------------------------------------------------------

struct LruPool<V: Clone> {
    entries: HashMap<PathBuf, (V, usize, u64)>, // (value, mem_size, access_order)
    max_bytes: usize,
    current_bytes: usize,
    counter: u64,
    hits: u64,
    misses: u64,
}

impl<V: Clone> LruPool<V> {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_bytes,
            current_bytes: 0,
            counter: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, path: &Path) -> Option<V> {
        self.counter += 1;
        let counter = self.counter;
        if let Some(entry) = self.entries.get_mut(path) {
            entry.2 = counter;
            self.hits += 1;
            Some(entry.0.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    fn insert(&mut self, path: PathBuf, value: V, mem_size: usize) {
        self.counter += 1;
        let counter = self.counter;

        if let Some(old) = self.entries.remove(&path) {
            self.current_bytes -= old.1;
        }

        while self.current_bytes + mem_size > self.max_bytes && !self.entries.is_empty() {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.2)
                .map(|(k, _)| k.clone());
            if let Some(key) = oldest {
                if let Some(evicted) = self.entries.remove(&key) {
                    self.current_bytes -= evicted.1;
                }
            }
        }

        self.current_bytes += mem_size;
        self.entries.insert(path, (value, mem_size, counter));
    }

    fn invalidate_prefix(&mut self, prefix: &Path) {
        let keys: Vec<PathBuf> = self
            .entries
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        for key in keys {
            if let Some(evicted) = self.entries.remove(&key) {
                self.current_bytes -= evicted.1;
            }
        }
    }

    fn stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }

    fn saturation(&self) -> (usize, usize) {
        (self.current_bytes, self.max_bytes)
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Thread-safe partitioned LRU cache for storage index structures.
///
/// Budget split: 50% marks, 44% blooms, 5% column index, 1% metadata.
pub struct StorageCache {
    blooms: Mutex<LruPool<(u32, Vec<GranuleBloom>)>>,
    marks: Mutex<LruPool<(u32, Vec<GranuleMark>)>>,
    column_index: Mutex<LruPool<Vec<ColumnIndexEntry>>>,
    meta: Mutex<LruPool<PartMetadataHeader>>,
}

impl StorageCache {
    /// Create a new partitioned cache with the given total memory budget.
    pub fn new(total_bytes: usize) -> Self {
        Self {
            marks: Mutex::new(LruPool::new(total_bytes / 2)), // 50%
            blooms: Mutex::new(LruPool::new(total_bytes * 44 / 100)), // 44%
            column_index: Mutex::new(LruPool::new(total_bytes * 5 / 100)), // 5%
            meta: Mutex::new(LruPool::new(total_bytes / 100)), // 1%
        }
    }

    pub fn get_blooms(&self, path: &Path) -> std::io::Result<(u32, Vec<GranuleBloom>, bool)> {
        if let Some((bits, blooms)) = self.blooms.lock().unwrap().get(path) {
            return Ok((bits, blooms, true));
        }
        let (bits, blooms) = crate::granule::read_blooms(path)?;
        let size = blooms.iter().map(|b| b.bits.len() * 8).sum::<usize>() + 64;
        self.blooms
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), (bits, blooms.clone()), size);
        Ok((bits, blooms, false))
    }

    pub fn get_marks(&self, path: &Path) -> std::io::Result<(u32, Vec<GranuleMark>, bool)> {
        if let Some((gs, marks)) = self.marks.lock().unwrap().get(path) {
            return Ok((gs, marks, true));
        }
        let (gs, marks) = crate::granule::read_marks(path)?;
        let size = marks.len() * std::mem::size_of::<GranuleMark>() + 64;
        self.marks
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), (gs, marks.clone()), size);
        Ok((gs, marks, false))
    }

    pub fn get_meta(&self, path: &Path) -> std::io::Result<(PartMetadataHeader, bool)> {
        if let Some(meta) = self.meta.lock().unwrap().get(path) {
            return Ok((meta, true));
        }
        let meta = crate::part::read_meta_bin(path)?;
        let size = std::mem::size_of::<PartMetadataHeader>() + 64;
        self.meta
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), meta.clone(), size);
        Ok((meta, false))
    }

    pub fn get_column_index(&self, path: &Path) -> std::io::Result<(Vec<ColumnIndexEntry>, bool)> {
        if let Some(entries) = self.column_index.lock().unwrap().get(path) {
            return Ok((entries, true));
        }
        let entries = crate::part::read_column_index(path)?;
        let size = entries.len() * std::mem::size_of::<ColumnIndexEntry>() + 64;
        self.column_index
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), entries.clone(), size);
        Ok((entries, false))
    }

    /// Invalidate all cached entries for a part directory.
    pub fn invalidate_part(&self, part_dir: &Path) {
        self.blooms.lock().unwrap().invalidate_prefix(part_dir);
        self.marks.lock().unwrap().invalidate_prefix(part_dir);
        self.column_index
            .lock()
            .unwrap()
            .invalidate_prefix(part_dir);
        self.meta.lock().unwrap().invalidate_prefix(part_dir);
    }

    /// Get aggregate hit/miss stats across all partitions.
    pub fn stats(&self) -> (u64, u64) {
        let (bh, bm) = self.blooms.lock().unwrap().stats();
        let (mh, mm) = self.marks.lock().unwrap().stats();
        let (ih, im) = self.column_index.lock().unwrap().stats();
        let (eh, em) = self.meta.lock().unwrap().stats();
        (bh + mh + ih + eh, bm + mm + im + em)
    }

    /// Get aggregate cache saturation (used_bytes, max_bytes).
    pub fn saturation(&self) -> (usize, usize) {
        let (bu, bm) = self.blooms.lock().unwrap().saturation();
        let (mu, mm) = self.marks.lock().unwrap().saturation();
        let (iu, im) = self.column_index.lock().unwrap().saturation();
        let (eu, em) = self.meta.lock().unwrap().saturation();
        (bu + mu + iu + eu, bm + mm + im + em)
    }

    /// Per-partition cache breakdown: `[(name, used_bytes, max_bytes)]`.
    pub fn partition_stats(&self) -> [(&'static str, usize, usize); 4] {
        let (mu, mm) = self.marks.lock().unwrap().saturation();
        let (bu, bm) = self.blooms.lock().unwrap().saturation();
        let (iu, im) = self.column_index.lock().unwrap().saturation();
        let (eu, em) = self.meta.lock().unwrap().saturation();
        [
            ("marks", mu, mm),
            ("blooms", bu, bm),
            ("column_index", iu, im),
            ("metadata", eu, em),
        ]
    }
}

impl Default for StorageCache {
    fn default() -> Self {
        Self::new(1024 * 1024 * 1024) // 1 GB
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_hit_miss_tracking() {
        let cache = StorageCache::new(1024 * 1024);
        let (hits, misses) = cache.stats();
        assert_eq!(hits, 0);
        assert_eq!(misses, 0);
    }
}
