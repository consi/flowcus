use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use flowcus_core::AppConfig;
use flowcus_core::observability::Metrics;
use flowcus_ipfix::SessionStore;
use flowcus_storage::cache::StorageCache;

/// Shared application state available to all handlers.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    config: AppConfig,
    metrics: Arc<Metrics>,
    query_cache: QueryCache,
    storage_cache: Arc<StorageCache>,
    /// Shared IPFIX session store for reading metadata (interface names, etc.).
    /// `None` when no IPFIX listener is configured (e.g. in tests).
    session_store: Option<Arc<tokio::sync::Mutex<SessionStore>>>,
    /// Path to the settings file on disk.
    settings_path: PathBuf,
    /// Mutex to serialize settings writes (load-modify-save must be atomic).
    settings_lock: tokio::sync::Mutex<()>,
    /// Sender half of the shutdown/restart signal.
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// Receiver half of the shutdown/restart signal.
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl AppState {
    pub fn new(config: AppConfig, metrics: Arc<Metrics>, settings_path: PathBuf) -> Self {
        let query_entries = config.server.query_cache_entries;
        let cache_bytes = config.storage.storage_cache_bytes;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            inner: Arc::new(AppStateInner {
                config,
                metrics,
                query_cache: QueryCache::new(query_entries),
                storage_cache: Arc::new(StorageCache::new(cache_bytes)),
                session_store: None,
                settings_path,
                settings_lock: tokio::sync::Mutex::new(()),
                shutdown_tx,
                shutdown_rx,
            }),
        }
    }

    /// Create state with an IPFIX session store for metadata access.
    pub fn with_session_store(
        config: AppConfig,
        metrics: Arc<Metrics>,
        session_store: Arc<tokio::sync::Mutex<SessionStore>>,
        settings_path: PathBuf,
    ) -> Self {
        let query_entries = config.server.query_cache_entries;
        let cache_bytes = config.storage.storage_cache_bytes;
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            inner: Arc::new(AppStateInner {
                config,
                metrics,
                query_cache: QueryCache::new(query_entries),
                storage_cache: Arc::new(StorageCache::new(cache_bytes)),
                session_store: Some(session_store),
                settings_path,
                settings_lock: tokio::sync::Mutex::new(()),
                shutdown_tx,
                shutdown_rx,
            }),
        }
    }

    pub fn config(&self) -> &AppConfig {
        &self.inner.config
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
    }

    pub fn storage_dir(&self) -> &str {
        &self.inner.config.storage.dir
    }

    pub fn granule_size(&self) -> usize {
        self.inner.config.storage.granule_size
    }

    /// Access the query result cache.
    pub fn query_cache(&self) -> &QueryCache {
        &self.inner.query_cache
    }

    /// Access the shared storage LRU cache.
    pub fn storage_cache(&self) -> &Arc<StorageCache> {
        &self.inner.storage_cache
    }

    /// Access the IPFIX session store (if available).
    pub fn session_store(&self) -> Option<&Arc<tokio::sync::Mutex<SessionStore>>> {
        self.inner.session_store.as_ref()
    }

    /// Path to the settings file on disk.
    pub fn settings_path(&self) -> &Path {
        &self.inner.settings_path
    }

    /// Mutex protecting load-modify-save cycles on the settings file.
    pub fn settings_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.inner.settings_lock
    }

    /// Clone a receiver for the shutdown/restart signal.
    pub fn shutdown_rx(&self) -> tokio::sync::watch::Receiver<bool> {
        self.inner.shutdown_rx.clone()
    }

    /// Signal that the server should shut down and restart.
    pub fn trigger_restart(&self) {
        let _ = self.inner.shutdown_tx.send(true);
    }
}

// ---------------------------------------------------------------------------
// Query Result Cache
// ---------------------------------------------------------------------------

/// LRU-like cache for query results. Avoids re-executing identical queries
/// when the underlying data has not changed.
///
/// Invalidation strategy: each cached entry records the part count at the
/// time it was created. When parts are flushed or merged the count changes,
/// which causes cache misses without any explicit invalidation signal.
pub struct QueryCache {
    entries: Mutex<CacheInner>,
}

struct CacheInner {
    map: HashMap<u64, CachedResult>,
    /// Insertion-order keys for LRU eviction.
    order: Vec<u64>,
    max_entries: usize,
}

/// A cached query result.
#[derive(Clone)]
pub struct CachedResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub stats: CachedQueryStats,
    pub total: u64,
    pub explain: Vec<serde_json::Value>,
    /// Flush counter at cache time — monotonic, invalidates when new data arrives.
    pub flush_count: u64,
    pub created_at: Instant,
    /// Resolved time range bounds for infinite scroll pinning.
    pub time_range: crate::query::TimeRangeBounds,
}

/// Cached subset of query stats (without timing info, which is per-request).
#[derive(Clone)]
pub struct CachedQueryStats {
    pub rows_scanned: u64,
    pub rows_returned: u64,
    pub total_rows: u64,
    pub parts_scanned: u64,
    pub parts_skipped: u64,
    pub bytes_read: u64,
}

impl QueryCache {
    /// Create a new cache with the given maximum entry count.
    fn new(max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(CacheInner {
                map: HashMap::new(),
                order: Vec::new(),
                max_entries,
            }),
        }
    }

    /// Compute a cache key from the query string and pagination params.
    pub fn cache_key(query: &str, offset: u64, limit: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        offset.hash(&mut hasher);
        limit.hash(&mut hasher);
        hasher.finish()
    }

    /// Look up a cached result. Returns `None` if not found, if new
    /// data has been flushed/merged, or if the entry is older than 2 seconds.
    #[allow(clippy::significant_drop_tightening)]
    pub fn get(&self, key: u64, current_flush_count: u64) -> Option<CachedResult> {
        let mut inner = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let entry = inner.map.get(&key)?;
        let stale =
            entry.flush_count != current_flush_count || entry.created_at.elapsed().as_secs() >= 2;

        if stale {
            inner.map.remove(&key);
            inner.order.retain(|k| *k != key);
            return None;
        }

        let result = entry.clone();
        inner.order.retain(|k| *k != key);
        inner.order.push(key);
        Some(result)
    }

    /// Store a query result in the cache.
    pub fn put(&self, key: u64, result: CachedResult) {
        let mut inner = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Evict oldest if at capacity
        while inner.map.len() >= inner.max_entries && !inner.order.is_empty() {
            let oldest_key = inner.order.remove(0);
            inner.map.remove(&oldest_key);
        }
        inner.order.retain(|k| *k != key);
        inner.order.push(key);
        inner.map.insert(key, result);
    }
}
