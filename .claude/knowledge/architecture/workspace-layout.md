# Workspace Layout
**Context:** Understanding the overall project structure
**Decision/Pattern:** Cargo workspace with focused crates: core (types/config), worker (thread pools), server (HTTP), app (binary). Frontend is a separate npm project embedded at build time.
**Rationale:** Clear separation of concerns, independent compilation, explicit dependency graph. Each crate can be tested in isolation.
**Last verified:** 2026-03-23
