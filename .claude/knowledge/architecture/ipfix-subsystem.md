# IPFIX Subsystem Architecture
**Context:** How IPFIX collection works in Flowcus
**Decision/Pattern:** The IPFIX subsystem is in `crates/flowcus-ipfix` with clear layering: wire parsing (protocol.rs) -> IE lookup (ie/) -> template cache (session.rs) -> record decoding (decoder.rs) -> network I/O (listener.rs). Messages are parsed structurally first, then templates are registered, then data records are decoded using those templates. All this happens in a single `process_ipfix_packet` call per message.
**Rationale:** IPFIX requires templates before data can be decoded, but templates and data can arrive in the same message. Two-pass decode (register templates first, then decode data) handles this. Per-exporter session scoping matches RFC 7011 semantics.
**Last verified:** 2026-03-23
