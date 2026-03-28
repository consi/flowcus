use std::sync::Arc;

use crossbeam::channel;
use rayon::ThreadPoolBuilder;
use tokio::sync::{Semaphore, oneshot};
use tracing::{info, warn};

use flowcus_core::config::WorkerConfig;

/// Handle to submit work to the worker pool.
#[derive(Clone)]
pub struct WorkerPoolHandle {
    cpu_tx: channel::Sender<BoxedCpuWork>,
    async_semaphore: Arc<Semaphore>,
}

type BoxedCpuWork = Box<dyn FnOnce() + Send + 'static>;

/// Manages both async (tokio) and CPU-bound (rayon) worker pools.
pub struct WorkerPool {
    config: WorkerConfig,
    rayon_pool: rayon::ThreadPool,
    cpu_tx: channel::Sender<BoxedCpuWork>,
    cpu_rx: channel::Receiver<BoxedCpuWork>,
    async_semaphore: Arc<Semaphore>,
}

impl WorkerPool {
    /// Create a new worker pool from configuration.
    ///
    /// # Errors
    /// Returns an error if the rayon thread pool cannot be built.
    pub fn new(config: &WorkerConfig) -> flowcus_core::Result<Self> {
        let rayon_pool = ThreadPoolBuilder::new()
            .num_threads(config.cpu_workers)
            .thread_name(|i| format!("cpu-worker-{i}"))
            .build()
            .map_err(|e| flowcus_core::Error::worker(e.to_string()))?;

        let (cpu_tx, cpu_rx) = channel::bounded(config.queue_capacity);
        let async_semaphore = Arc::new(Semaphore::new(config.async_workers));

        info!(
            cpu_workers = config.cpu_workers,
            async_workers = config.async_workers,
            queue_capacity = config.queue_capacity,
            "Worker pool initialized"
        );

        Ok(Self {
            config: config.clone(),
            rayon_pool,
            cpu_tx,
            cpu_rx,
            async_semaphore,
        })
    }

    /// Get a cloneable handle for submitting work.
    pub fn handle(&self) -> WorkerPoolHandle {
        WorkerPoolHandle {
            cpu_tx: self.cpu_tx.clone(),
            async_semaphore: Arc::clone(&self.async_semaphore),
        }
    }

    /// Run the CPU worker dispatch loop. Call this once from your runtime.
    /// Consumes the pool. Blocks until all senders (handles) are dropped.
    pub fn run_cpu_dispatch(self) {
        let Self {
            config,
            rayon_pool,
            cpu_tx: sender,
            cpu_rx,
            async_semaphore: _,
        } = self;
        // Drop our copy of the sender so the channel closes when all handles drop
        drop(sender);

        info!(workers = config.cpu_workers, "CPU dispatch loop started");
        while let Ok(work) = cpu_rx.recv() {
            rayon_pool.spawn(work);
        }
        info!("CPU dispatch loop exited");
    }
}

impl WorkerPoolHandle {
    /// Submit a CPU-bound task and get the result back via a oneshot channel.
    ///
    /// # Errors
    /// Returns an error if the worker pool has shut down.
    pub fn spawn_cpu<F, R>(&self, f: F) -> oneshot::Receiver<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let work: BoxedCpuWork = Box::new(move || {
            let result = f();
            let _ = tx.send(result);
        });

        if self.cpu_tx.send(work).is_err() {
            warn!("CPU worker pool is shut down, task dropped");
        }

        rx
    }

    /// Run an async task with concurrency limiting.
    /// Returns the result of the future.
    ///
    /// # Panics
    /// Panics if the internal semaphore is closed (should not happen during normal operation).
    pub async fn spawn_async<F, R>(&self, f: F) -> R
    where
        F: std::future::Future<Output = R>,
    {
        let _permit = self
            .async_semaphore
            .acquire()
            .await
            .expect("semaphore closed");
        f.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> WorkerConfig {
        WorkerConfig {
            async_workers: 4,
            cpu_workers: 2,
            queue_capacity: 64,
        }
    }

    #[test]
    fn pool_creation() {
        let pool = WorkerPool::new(&test_config()).unwrap();
        let _handle = pool.handle();
    }

    #[tokio::test]
    async fn cpu_task_roundtrip() {
        let config = test_config();
        let pool = WorkerPool::new(&config).unwrap();
        let handle = pool.handle();

        // Spawn dispatch in background
        let dispatch = std::thread::spawn(move || pool.run_cpu_dispatch());

        let rx = handle.spawn_cpu(|| 42);
        let result = rx.await.unwrap();
        assert_eq!(result, 42);

        // Drop sender to stop dispatch loop
        drop(handle);
        dispatch.join().unwrap();
    }

    #[tokio::test]
    async fn async_concurrency_limit() {
        let config = WorkerConfig {
            async_workers: 2,
            cpu_workers: 1,
            queue_capacity: 8,
        };
        let pool = WorkerPool::new(&config).unwrap();
        let handle = pool.handle();

        let result = handle.spawn_async(async { "hello from async" }).await;
        assert_eq!(result, "hello from async");
    }

    #[tokio::test]
    async fn multiple_cpu_tasks() {
        let config = test_config();
        let pool = WorkerPool::new(&config).unwrap();
        let handle = pool.handle();

        let dispatch = std::thread::spawn(move || pool.run_cpu_dispatch());

        let mut receivers = Vec::new();
        for i in 0..10 {
            receivers.push(handle.spawn_cpu(move || i * i));
        }

        let mut results = Vec::new();
        for rx in receivers {
            results.push(rx.await.unwrap());
        }

        let expected: Vec<i32> = (0..10).map(|i| i * i).collect();
        assert_eq!(results, expected);

        drop(handle);
        dispatch.join().unwrap();
    }
}
