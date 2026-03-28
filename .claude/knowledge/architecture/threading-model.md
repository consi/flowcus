# Threading Model
**Context:** How concurrency is structured in Flowcus
**Decision/Pattern:** Tokio async runtime for all I/O-bound work (HTTP, IPFIX listeners, merge coordination). CPU-bound work (column encoding, merge operations) runs via `tokio::task::spawn_blocking`. Concurrent merge tasks are bounded by a tokio Semaphore.
**Rationale:** Keeps the architecture simple with a single runtime. `spawn_blocking` moves CPU-heavy work off the async executor threads without needing a separate thread pool.
**Last verified:** 2026-03-23
