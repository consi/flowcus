# Granule Index System (Marks + Bloom Filters)
**Context:** How query engines avoid full column scans
**Decision/Pattern:** Every 8192 rows (configurable granule_size), two index files are computed per column:
- `{col}.mrk` (marks): 48-byte entries with data_offset (byte position in .col), row range, and per-granule min/max values. Enables direct seeking to relevant data.
- `{col}.bloom`: per-granule bloom filters (default 8192 bits, 3 FNV-1a hashes) for probabilistic point-query filtering. ~1% FPR for up to ~800 distinct values per granule.

Both are computed during ingestion flush AND rebuilt during merge (merged data gets new optimal granules). Written alongside every part. Query path:
1. column_index.bin → part-level min/max (skip entire part)
2. .bloom → per-granule bloom check (skip granules)
3. .mrk → byte offset for matching granules (seek directly)
4. .col → read only matching bytes

LRU mark cache (default 2GB, configurable mark_cache_bytes) keeps frequently accessed marks + blooms in memory.
**Rationale:** Granule indexing is the core scan-limiting mechanism. Without it, every query reads entire columns. With it, point queries touch only the relevant 8K-row blocks. Bloom filters add a probabilistic fast-path that eliminates most non-matching granules without reading any column data.
**Last verified:** 2026-03-23
