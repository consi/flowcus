# IPFIX Crate Clippy Allows
**Context:** Why flowcus-ipfix has more clippy allows than other crates
**Decision/Pattern:** The crate-level `#![allow(...)]` in lib.rs permits cast lints (truncation, wrap, sign loss, precision loss) and doc lints. This is intentional because wire protocol parsing inherently requires u8/u16/u32/u64 casts from byte buffers, and the IE registry functions are necessarily large.
**Rationale:** Annotating every individual cast in protocol.rs would add noise without safety benefit - the casts match the IPFIX wire format spec exactly. Other crates maintain strict clippy.
**Last verified:** 2026-03-23
