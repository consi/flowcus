# CPU Dispatch Loop Must Consume WorkerPool
**Context:** The `run_cpu_dispatch` method on `WorkerPool`
**Decision/Pattern:** `run_cpu_dispatch` takes `self` (not `&self`) so it can drop its copy of the crossbeam sender. Otherwise the channel never closes and the loop hangs forever.
**Rationale:** If the pool holds a sender clone, dropping all `WorkerPoolHandle`s won't close the channel. The dispatch thread blocks on `recv()` indefinitely.
**Last verified:** 2026-03-23
