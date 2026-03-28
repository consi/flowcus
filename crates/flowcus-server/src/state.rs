use std::sync::Arc;

use flowcus_core::AppConfig;
use flowcus_core::observability::Metrics;
use flowcus_worker::WorkerPoolHandle;

/// Shared application state available to all handlers.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    config: AppConfig,
    worker: WorkerPoolHandle,
    metrics: Arc<Metrics>,
}

impl AppState {
    pub fn new(config: AppConfig, worker: WorkerPoolHandle, metrics: Arc<Metrics>) -> Self {
        Self {
            inner: Arc::new(AppStateInner {
                config,
                worker,
                metrics,
            }),
        }
    }

    pub fn config(&self) -> &AppConfig {
        &self.inner.config
    }

    pub fn worker(&self) -> &WorkerPoolHandle {
        &self.inner.worker
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
    }
}
