use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use flowcus_core::config::WorkerConfig;
use flowcus_worker::WorkerPool;

const fn test_config() -> WorkerConfig {
    WorkerConfig {
        async_workers: 2,
        cpu_workers: 2,
        queue_capacity: 64,
    }
}

#[tokio::test]
async fn cpu_tasks_return_correct_results() {
    let pool = WorkerPool::new(&test_config()).unwrap();
    let handle = pool.handle();
    let dispatch = std::thread::spawn(move || pool.run_cpu_dispatch());

    let receivers: Vec<_> = (0i32..20)
        .map(|i| handle.spawn_cpu(move || i * i))
        .collect();
    let expected: Vec<i32> = (0..20).map(|i: i32| i * i).collect();

    for (exp, rx) in expected.into_iter().zip(receivers) {
        assert_eq!(rx.await.unwrap(), exp);
    }

    drop(handle);
    dispatch.join().unwrap();
}

#[tokio::test]
async fn async_semaphore_bounds_concurrency() {
    let pool = WorkerPool::new(&test_config()).unwrap();
    let handle = pool.handle();

    let peak = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));

    let mut joins = Vec::new();
    for _ in 0..6 {
        let h = handle.clone();
        let p = Arc::clone(&peak);
        let a = Arc::clone(&active);
        joins.push(tokio::spawn(async move {
            h.spawn_async(async {
                let val = a.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(val, Ordering::SeqCst);
                tokio::task::yield_now().await;
                a.fetch_sub(1, Ordering::SeqCst);
            })
            .await;
        }));
    }

    for j in joins {
        j.await.unwrap();
    }

    assert!(peak.load(Ordering::SeqCst) <= 2);
}
