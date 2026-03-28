# Testing Patterns
**Context:** How to write tests in this project
**Decision/Pattern:**
- Unit tests: inline `#[cfg(test)] mod tests` in each source file
- Integration tests: `tests/integration/*.rs` for cross-crate behavior
- E2E tests: `tests/e2e/*.rs` spawn a real server on a random free port
- Benchmarks: `criterion` in `crates/*/benches/`
- E2E pattern: use `free_port()` helper to bind to port 0, then connect
**Rationale:** Inline unit tests stay close to code. Separate integration/e2e test binaries have proper access to all crates. Random ports prevent test interference.
**Last verified:** 2026-03-23
