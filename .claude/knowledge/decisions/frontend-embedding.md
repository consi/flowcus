# Frontend Embedding Strategy
**Context:** How the frontend is bundled with the Rust binary
**Decision/Pattern:** Use `rust-embed` to embed `frontend/dist/` at compile time. In dev mode, the Rust server can be run alongside Vite's dev server (port 5173) with proxy config. In production, all assets are inside the single binary.
**Rationale:** Single binary deployment simplifies operations. Vite dev server provides HMR during development. The proxy configuration in `vite.config.ts` routes `/api` calls to the Rust backend.
**Last verified:** 2026-03-23
