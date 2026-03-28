# Codec Pipeline: Transform + Compression
**Context:** How column data is encoded for disk storage
**Decision/Pattern:** Two-layer pipeline: (1) Transform codec selected by analyzing data — Delta for monotonic timestamps, DeltaDelta for constant-rate, GCD for quantized values, Plain otherwise. (2) LZ4 compression applied if it actually shrinks the result (skip for small columns <64 bytes). Codecs can combine: e.g., Delta+LZ4 for timestamps gives excellent compression.
**Rationale:** Transform codecs reshape data to increase compression ratio. LZ4 is CPU-cheap and benefits from low-entropy transformed data. Auto-selection means the user doesn't need to configure per-column codecs.
**Last verified:** 2026-03-23
