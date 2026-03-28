use criterion::{Criterion, black_box, criterion_group, criterion_main};
use flowcus_core::config::WorkerConfig;
use flowcus_worker::WorkerPool;

fn bench_cpu_task_throughput(c: &mut Criterion) {
    let config = WorkerConfig {
        async_workers: 4,
        cpu_workers: 4,
        queue_capacity: 4096,
    };

    let pool = WorkerPool::new(&config).unwrap();
    let handle = pool.handle();
    let dispatch = std::thread::spawn(move || pool.run_cpu_dispatch());

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("cpu_task_submit_and_recv", |b| {
        b.iter(|| {
            let rx = handle.spawn_cpu(|| black_box(42 * 42));
            rt.block_on(rx).unwrap();
        });
    });

    c.bench_function("cpu_task_batch_100", |b| {
        b.iter(|| {
            let receivers: Vec<_> = (0..100)
                .map(|i| handle.spawn_cpu(move || black_box(i * i)))
                .collect();
            for rx in receivers {
                rt.block_on(rx).unwrap();
            }
        });
    });

    drop(handle);
    dispatch.join().unwrap();
}

fn bench_async_semaphore(c: &mut Criterion) {
    let config = WorkerConfig {
        async_workers: 8,
        cpu_workers: 1,
        queue_capacity: 64,
    };

    let pool = WorkerPool::new(&config).unwrap();
    let handle = pool.handle();
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("async_semaphore_acquire_release", |b| {
        b.iter(|| {
            rt.block_on(handle.spawn_async(async { black_box(1 + 1) }));
        });
    });
}

criterion_group!(benches, bench_cpu_task_throughput, bench_async_semaphore);
criterion_main!(benches);
