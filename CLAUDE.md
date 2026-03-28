# Flowcus - Claude Code Configuration

## Project Overview
IPFIX collector, columnar storage, and query system for network flow analysis. Rust backend with embedded React frontend, single-binary deployment.

## Crate Structure
- `flowcus-core` - Config (`AppConfig` with 5 sections), error types, telemetry (human/json), observability (Prometheus metrics), profiling (dev-mode snapshots + stack traces)
- `flowcus-worker` - Thread pool: rayon CPU-bound + tokio semaphore async concurrency
- `flowcus-ipfix` - IPFIX protocol: wire parsing, IE registry (IANA + 9 vendors), session/template management, decoder, UDP/TCP listener, trace-level pretty printer
- `flowcus-storage` - Columnar storage engine (see below)
- `flowcus-server` - Axum on :2137, API routes (`/api/health`, `/api/info`), observability routes (`/observability/metrics`), embedded frontend
- `flowcus-app` - Binary entrypoint, CLI parsing, runtime orchestration

## Storage Crate Modules
- `codec/` - Transform codecs (Plain, Delta, DeltaDelta, GCD) + LZ4 compression. Auto-selected per column.
- `column.rs` - In-memory column buffers with typed storage (U8/U16/U32/U64/U128/Mac/VarLen)
- `schema.rs` - Schema from IPFIX templates. System columns prepended: `flowcusExporterIPv4`, `flowcusExporterPort`, `flowcusExportTime`, `flowcusObservationDomainId`
- `writer.rs` - Buffered writer. Flushes to immutable parts on size/time threshold.
- `part.rs` - On-disk format: `meta.bin` (256-byte header, magic "FMTA"), `column_index.bin`, `schema.bin`, `columns/{name}.col` with 64-byte headers
- `granule.rs` - Marks (`.mrk`, magic "FMRK") for byte-offset seeking + bloom filters (`.bloom`, magic "FBLM") for point queries. Default granule = 8192 rows.
- `merge.rs` - Background merge: coordinator (async) + workers (rayon). Generation-based compaction. Throttled by CPU/memory. Crash-safe (staged writes, source parts untouched on failure).
- `pending.rs` - Tracks hour directories needing merge. Rebuilt from disk on restart.
- `table.rs` - Table-level part registry
- `ingest.rs` - Channel from IPFIX decoder to storage writer (backpressure via bounded channel)
- `crc.rs` - CRC32-C (Castagnoli) checksums on all binary formats
- `metrics.rs` - Storage metric reporting for profiler

## Observability vs Profiling
- **Observability** (`observability.rs`): Always-on Prometheus metrics at `/observability/metrics`. Lock-free atomics. For production monitoring.
- **Profiling** (`profiling.rs`): Dev-mode only. 10-second snapshots to `profiling/` directory. Stack trace collection with folded-stack format. For development performance analysis.

## CRC32 Integrity
All binary storage formats include CRC32-C checksums: `meta.bin`, `column_index.bin`, `.col` headers, `.mrk`, `.bloom`. Verified on read. Implementation in `crc.rs` (lookup table, no external dep).

## Directory Layout
Time-partitioned: `storage/flows/{YYYY}/{MM}/{DD}/{HH}/{gen}_{min_ts}_{max_ts}_{seq}/`
Part names encode generation, time range, and sequence for filesystem-level pruning.

## Key Ports
- HTTP Server: 2137 (configurable)
- IPFIX Collector: 4739 (configurable, UDP default, TCP optional)
- Vite dev: 5173

## Development Commands
```
just dev          # Full stack dev (Vite + Rust with hot reload)
just dev-backend  # Backend only
just test         # All tests
just check        # Format + lint + test
just bench        # Benchmarks
just ci           # Full local CI pipeline
just build        # Production build (single binary)
```

## Code Standards

### Rust
- Edition 2024, MSRV 1.85
- `unsafe` denied workspace-wide
- Clippy pedantic + nursery enabled
- `flowcus-ipfix` and `flowcus-storage` have targeted allows for cast/doc lints
- Use `thiserror` for library errors, `anyhow` only in the binary crate
- Prefer `tracing` over `println!`

### Testing
- Unit tests: `#[cfg(test)] mod tests` in source files
- Integration tests: `cargo test -p flowcus-storage --test integrity_tests` (storage integrity)
- E2E tests: `cargo test -p flowcus-app --test server_test`
- IPFIX tests: raw byte arrays for wire format, set lengths include 4-byte header
- Use `free_port()` pattern for server tests

### Git
- Conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `ci:`, `perf:`
- PR branches: `feat/description`, `fix/description`

## Knowledge System
Claude maintains structured knowledge in `.claude/knowledge/`:
- `architecture/` - System design decisions
- `decisions/` - ADRs
- `patterns/` - Recurring code patterns
- `issues/` - Known issues and resolutions
