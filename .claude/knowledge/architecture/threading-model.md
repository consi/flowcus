# Threading Model
**Context:** How concurrency is structured in Flowcus
**Decision/Pattern:** Two-tier worker pool: Tokio async runtime for I/O-bound work (HTTP, network), Rayon thread pool for CPU-bound work. A dedicated dispatch thread bridges crossbeam channels to rayon. Async concurrency is bounded by a tokio Semaphore.
**Rationale:** Prevents CPU-heavy tasks from blocking the async runtime. Rayon provides work-stealing for CPU tasks. Semaphore prevents async task explosion.
**Last verified:** 2026-03-23
