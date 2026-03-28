use std::sync::Arc;

use flowcus_core::AppConfig;
use flowcus_core::observability::Metrics;

/// Shared application state available to all handlers.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    config: AppConfig,
    metrics: Arc<Metrics>,
}

impl AppState {
    pub fn new(config: AppConfig, metrics: Arc<Metrics>) -> Self {
        Self {
            inner: Arc::new(AppStateInner { config, metrics }),
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
}
