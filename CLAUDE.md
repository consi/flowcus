# Flowcus - Claude Code Configuration

## Project Overview
IPFIX collector, columnar storage, and query system for network flow analysis. Rust backend with embedded React frontend, single-binary deployment.

## Crate Structure
- `flowcus-core` - Config (`AppConfig` with 5 sections), error types, telemetry (human/json), observability (Prometheus metrics)
- `flowcus-ipfix` - IPFIX protocol: wire parsing, IE registry (IANA + 9 vendors), session/template management, decoder, UDP/TCP listener, trace-level pretty printer
- `flowcus-storage` - Columnar storage engine (see below)
- `flowcus-query` - FQL query language: hand-written lexer/parser, typed AST, semantic validation
- `flowcus-server` - Axum on :2137, API routes (`/api/health`, `/api/info`, `/api/query`), observability routes (`/observability/metrics`), embedded frontend
- `flowcus-app` - Binary entrypoint, CLI parsing, runtime orchestration

## Storage Crate Modules
- `codec/` - Transform codecs (Plain, Delta, DeltaDelta, GCD) + LZ4 compression. Auto-selected per column.
- `column.rs` - In-memory column buffers with typed storage (U8/U16/U32/U64/U128/Mac/VarLen)
- `schema.rs` - Schema from IPFIX templates. System columns prepended: `flowcusExporterIPv4`, `flowcusExporterPort`, `flowcusExportTime`, `flowcusObservationDomainId`
- `writer.rs` - Buffered writer. Flushes to immutable parts on size/time threshold.
- `part.rs` - On-disk format: `meta.bin` (256-byte header, magic "FMTA"), `column_index.bin`, `schema.bin`, `columns/{name}.col` with 64-byte headers
- `granule.rs` - Marks (`.mrk`, magic "FMRK") for byte-offset seeking + bloom filters (`.bloom`, magic "FBLM") for point queries. Default granule = 8192 rows.
- `merge.rs` - Background merge: coordinator (async) + workers (tokio spawn_blocking). Generation-based compaction. Throttled by CPU/memory. Crash-safe (staged writes, source parts untouched on failure).
- `pending.rs` - Tracks hour directories needing merge. Rebuilt from disk on restart.
- `table.rs` - Table-level part registry
- `ingest.rs` - Channel from IPFIX decoder to storage writer (backpressure via bounded channel)
- `crc.rs` - CRC32-C (Castagnoli) checksums on all binary formats

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

## Agent Coordination & Parallelization

### Agent Roster
| Agent | Crate Scope | Color | Role |
|-------|------------|-------|------|
| `storage-integrity-engineer` | flowcus-storage | red | Schema, merge, writer, part format, crash safety |
| `ipfix-ingestion-engine` | flowcus-ipfix + ingest path | green | Wire parsing, IE registry, templates, decoder |
| `query-engine-architect` | flowcus-query + executor | purple | FQL parser, AST, planner, vectorized execution |
| `frontend-engineer` | frontend/ | orange | React components, Vite, UX, visualization |
| `test-engineer` | all crates | orange | Test design, execution, coverage, regression |
| `claude-config-optimizer` | .claude/ | cyan | Meta: agents, memory, skills, config |

### Parallelization Rules
- **Always launch independent agents in a single message** (multiple Agent tool calls). Never serialize what can run concurrently.
- **Crate boundaries are parallelism boundaries.** Changes to `flowcus-ipfix` and `flowcus-query` can always run in parallel. Changes to `flowcus-storage` may conflict with both.
- **Code + test = two agents.** After a code agent writes, launch `test-engineer` immediately. Don't wait for user confirmation.
- **Multi-crate tasks:** Split into per-crate agents. Example: "add a new IE type" → `ipfix-ingestion-engine` (wire parsing) + `storage-integrity-engineer` (column type) + `test-engineer` (both crates) — all launched in one message.

### Output Contract (all agents)
Every agent must return results in this structure so the orchestrator can reliably parse and relay them:
1. **Status**: `DONE` | `BLOCKED` | `NEEDS_REVIEW` — first word of the response
2. **Summary**: 1-3 sentence description of what was accomplished or what's blocking
3. **Files changed**: List of modified/created files with one-line description each
4. **Tests**: Which tests were run and their pass/fail status (if applicable)
5. **Follow-up**: Any remaining work or recommendations for other agents

Agents should NOT repeat source code in their summary — the diff is visible. Keep it terse.

### When NOT to Use Agents
- Reading a single known file → `Read` tool directly
- Searching for a specific symbol → `Grep` directly
- Simple one-file edits → edit directly, no agent overhead

## Knowledge System
Claude maintains structured knowledge in `.claude/knowledge/`:
- `architecture/` - System design decisions
- `decisions/` - ADRs
- `patterns/` - Recurring code patterns
- `issues/` - Known issues and resolutions
