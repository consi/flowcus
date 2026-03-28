# Flowcus

IPFIX collector with columnar storage and a query system for network flow analysis. Collects IPFIX (RFC 7011) data over UDP/TCP, decodes it using a comprehensive Information Element registry, stores it in a compressed columnar format with time-partitioned directory layout, and exposes Prometheus metrics for production observability.

## Architecture

Six Rust crates, single-binary deployment:

```
flowcus/
├── crates/
│   ├── flowcus-core/       # Config, error types, telemetry, observability, profiling
│   ├── flowcus-worker/     # CPU (rayon) + async (tokio) worker pools
│   ├── flowcus-ipfix/      # IPFIX protocol: wire parsing, IE registry, templates, listener
│   ├── flowcus-storage/    # Columnar storage engine: codecs, parts, merge, granule indexes
│   ├── flowcus-server/     # Axum web server, API routes, embedded frontend assets
│   └── flowcus-app/        # Binary entrypoint, CLI parsing, runtime orchestration
├── frontend/               # React 19 + TypeScript + Vite (embedded via rust-embed)
└── flowcus.toml            # Configuration
```

### Threading Model

- **Tokio runtime** handles all async I/O (HTTP, IPFIX listeners, merge coordination)
- **Rayon thread pool** handles CPU-bound work (column encoding, merge operations)
- **Crossbeam channels** bridge async-to-sync boundaries
- **Tokio semaphore** bounds async task concurrency

## Storage Engine

Columnar format inspired by ClickHouse MergeTree. Data flows: IPFIX records -> write buffers -> immutable parts on disk -> background merge.

### Columnar Format

Each column is stored in its own `.col` file with a 64-byte binary header containing codec metadata. Storage types: `U8`, `U16`, `U32`, `U64`, `U128`, `Mac`, `VarLen`.

### Codecs

Two-layer encoding pipeline:
1. **Transform codec** (data reshaping): `Plain`, `Delta`, `DeltaDelta`, `GCD` -- selected automatically by data analysis
2. **Compression codec**: `None` or `LZ4` -- applied if it shrinks the result

### Directory Layout

Time-partitioned tree enables filesystem-level partition pruning:

```
storage/flows/{YYYY}/{MM}/{DD}/{HH}/{gen}_{min_ts}_{max_ts}_{seq}/
  meta.bin            # 256-byte binary header (magic "FMTA", row count, time range, etc.)
  column_index.bin    # per-column codec/type/min/max (64 bytes per column)
  schema.bin          # column definitions (names, IE IDs, types)
  columns/
    {column}.col      # encoded column data with 64-byte binary header
    {column}.mrk      # granule marks (offsets + min/max per granule)
    {column}.bloom    # bloom filter (per-granule membership test)
```

### Parts and Merge

Parts are immutable once written. The background merge system compacts parts within time partitions:
- Coordinator (async task) scans hour directories, builds merge plans, dispatches work
- Workers (rayon pool) execute CPU-bound column merges
- Throttle monitors system CPU/memory load to avoid starving ingestion
- Generation increments on each merge: `00000` -> `00001` -> ... (max 65535)
- Current hour: merge same-generation parts into fewer larger parts
- Past hours: merge until a single part per hour

### Granule Indexes

A granule is a fixed-size group of rows (default 8192). Per granule:
- **Marks** (`.mrk`): byte offset + min/max values for range pruning
- **Bloom filters** (`.bloom`): probabilistic membership test for point queries

### Integrity

CRC32-C (Castagnoli) checksums on all binary formats: `meta.bin`, `column_index.bin`, `.col` headers, `.mrk`, `.bloom`.

### System Columns

Every schema is prepended with 4 system columns capturing IPFIX message metadata:
- `flowcusExporterIPv4` -- exporter source IP
- `flowcusExporterPort` -- exporter source port
- `flowcusExportTime` -- IPFIX message export timestamp
- `flowcusObservationDomainId` -- observation domain ID

## IPFIX

### Listeners

UDP (default) and TCP listeners on port 4739. UDP receive buffer is configurable. TCP supports multiple concurrent connections.

### Information Element Registry

~170 IANA standard IEs plus vendor IEs from 9 vendors: Cisco, Juniper, VMware, Palo Alto, Barracuda, ntop, Fortinet, Nokia, Huawei.

### Template Management

Per-exporter template cache scoped by `(exporter_addr, observation_domain, template_id)`. Templates expire after a configurable interval (default 1800s). Decoded records are converted to typed `FieldValue`s.

Trace-level pretty-print of decoded IPFIX messages: `RUST_LOG=flowcus_ipfix=trace`

## Observability

### Production Metrics

Always-on Prometheus endpoint at `/observability/metrics`. Lock-free atomic counters/gauges, zero-cost in the hot path. Metrics cover:
- IPFIX: packets received/parsed/errors, records decoded, bytes, active templates, TCP connections
- Writer: records ingested, parts flushed, bytes flushed, buffer depth, channel drops
- Merge: completed/failed, rows processed, bytes written, active workers, throttle count
- Storage: total parts, gen0/gen1+ parts, disk bytes
- Process: RSS, threads, open FDs, start time

### Dev-Mode Profiling

When `dev_mode = true`, captures 10-second performance snapshots to `profiling/` as compact text files. Includes system metrics and stack trace profiling with folded-stack format. Components register metric reporters; analysis is on-demand.

## Configuration

`flowcus.toml` with all sections:

```toml
[logging]
# format = "human"                   # "human" or "json"
# filter = "info,tower_http=debug"

[server]
# host = "0.0.0.0"
# port = 2137
# dev_mode = false

[worker]
# async_workers = <num_cpus>
# cpu_workers = <num_cpus>
# queue_capacity = 1024

[ipfix]
# host = "0.0.0.0"
# port = 4739
# udp = true
# tcp = false
# udp_recv_buffer = 65535
# template_expiry_secs = 1800

[storage]
# dir = "storage"
# flush_bytes = 1048576              # 1 MB
# flush_interval_secs = 1
# partition_duration_secs = 3600     # 1 hour
# channel_capacity = 4096
# initial_row_capacity = 8192
# merge_workers = 8
# merge_scan_interval_secs = 5
# merge_cpu_throttle = 0.80
# merge_mem_throttle = 0.85
# granule_size = 8192
# bloom_bits_per_granule = 8192      # 1 KB per granule
# mark_cache_bytes = 1073741824      # 1 GB
# bloom_cache_bytes = 1073741824     # 1 GB
```

CLI overrides:
```bash
flowcus --dev                # Enable dev mode
flowcus --port 8080          # Override port
flowcus --config custom.toml # Custom config file
FLOWCUS_DEV=1 flowcus       # Env var for dev mode
```

## Quick Start

```bash
cargo build
./target/debug/flowcus --dev
# HTTP server on :2137, IPFIX collector on :4739 (UDP)
# Prometheus metrics at http://localhost:2137/observability/metrics
```

## Development

| Command | Description |
|---------|-------------|
| `just dev` | Full stack dev (Vite HMR + Rust backend) |
| `just dev-backend` | Rust server only (dev mode) |
| `just build` | Production build (single binary) |
| `just test` | All tests (unit + integration + e2e) |
| `just check` | Format + lint + test |
| `just bench` | Benchmarks |
| `just ci` | Full local CI pipeline |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/info` | GET | Server info (version, config, workers) |
| `/observability/metrics` | GET | Prometheus metrics |
| `/*` | GET | Frontend SPA (embedded assets) |

## Directory Structure

```
crates/flowcus-core/src/
  config.rs          # AppConfig with all sections
  telemetry.rs       # Tracing setup (human/json formats)
  observability.rs   # Prometheus metrics (always-on)
  profiling.rs       # Dev-mode performance snapshots + stack traces
  error.rs           # Error types

crates/flowcus-ipfix/src/
  protocol.rs        # Wire format parsing (headers, sets, templates)
  ie/                # IE registry: iana.rs, vendor.rs (9 vendors)
  session.rs         # Per-exporter template cache
  decoder.rs         # Template-based record decoding
  listener.rs        # UDP + TCP async listeners
  display.rs         # Trace-level pretty-print

crates/flowcus-storage/src/
  codec/             # Plain, Delta, DeltaDelta, GCD transforms + LZ4
  column.rs          # In-memory column buffers
  schema.rs          # Schema from IPFIX templates, system columns
  writer.rs          # Buffered writer -> immutable parts
  part.rs            # On-disk part format (binary metadata, column files)
  granule.rs         # Marks (.mrk) + bloom filters (.bloom)
  merge.rs           # Background merge coordinator + workers
  pending.rs         # Pending hour tracking for merge
  table.rs           # Table-level part registry
  ingest.rs          # Ingestion channel (IPFIX -> writer)
  crc.rs             # CRC32-C integrity checksums
  metrics.rs         # Storage-specific metric reporting
```
