---
name: build
description: Build the Flowcus project for development or production
---

# Build Skill

Build the project based on the requested target.

## Steps

1. Ensure frontend dependencies are installed: `cd frontend && npm ci`
2. Build the frontend: `cd frontend && npm run build`
3. Build the Rust project:
   - Development: `cargo build`
   - Production: `cargo build --release`
   - Specific target: `cargo build --release --target <target>`

## Available targets
- `x86_64-unknown-linux-gnu` (default on x86_64)
- `aarch64-unknown-linux-gnu` (requires cross-compilation toolchain or `cross`)

## Quick commands
- `just build` - Full production build
- `just build-x86` - x86_64 Linux
- `just build-arm64` - aarch64 Linux
- `just cross-build <target>` - Cross-compile via Docker
