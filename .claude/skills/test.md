---
name: test
description: Run the Flowcus test suite
---

# Test Skill

Run tests at the appropriate level.

## Test Levels

### Unit Tests
```bash
cargo test --workspace --lib
```
Tests within each crate's source files. Fast, no I/O.

### Integration Tests
```bash
cargo test --test worker_integration
```
Tests in `tests/integration/`. Test cross-crate behavior.

### E2E Tests
```bash
cargo test --test server_test
```
Tests in `tests/e2e/`. Start a real HTTP server and make requests.

### All Tests
```bash
just test
# or
cargo test --workspace
```

### Benchmarks
```bash
just bench
# or
cargo bench -p flowcus-worker
```

## When to run what
- Changed core types/config -> unit tests
- Changed worker pool -> unit + integration + benchmarks
- Changed server/API -> unit + e2e
- Changed frontend -> `just frontend-typecheck` + e2e
- Before commit -> `just check` (fmt + lint + all tests)
