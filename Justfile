# Flowcus task runner
# Install: cargo install just

set dotenv-load := true

# Default: show available recipes
default:
    @just --list

# --- Development ---

# Run in development mode with hot reload
dev: frontend-install
    #!/usr/bin/env bash
    set -euo pipefail
    # Start Vite dev server in background
    cd frontend && npm run dev &
    VITE_PID=$!
    trap "kill $VITE_PID 2>/dev/null" EXIT
    # Run Rust server in dev mode
    RUST_LOG=debug cargo run -p flowcus-app -- --dev

# Run only the backend in dev mode
dev-backend:
    RUST_LOG=debug cargo run -p flowcus-app -- --dev

# Run only the frontend dev server
dev-frontend: frontend-install
    cd frontend && npm run dev

# --- Build ---

# Build everything for production
build: frontend-build
    cargo build --release

# Build for specific target
build-target target: frontend-build
    cargo build --release --target {{target}}

# Build for x86_64 Linux
build-x86:
    just build-target x86_64-unknown-linux-gnu

# Build for aarch64 Linux
build-arm64:
    just build-target aarch64-unknown-linux-gnu

# Build for all supported targets
build-all: frontend-build
    just build-x86
    just build-arm64

# Cross-compile using cross (docker-based)
cross-build target:
    cross build --release --target {{target}}

# --- Frontend ---

# Install frontend dependencies
frontend-install:
    cd frontend && npm ci --prefer-offline 2>/dev/null || cd frontend && npm install

# Build frontend for production
frontend-build: frontend-install
    cd frontend && npm run build

# Typecheck frontend
frontend-typecheck:
    cd frontend && npm run typecheck

# Lint frontend
frontend-lint:
    cd frontend && npm run lint

# --- Testing ---

# Run all tests
test: test-unit test-integration test-e2e

# Run unit tests
test-unit:
    cargo test --workspace --lib

# Run integration tests
test-integration:
    cargo test -p flowcus-worker --test worker_integration

# Run e2e tests
test-e2e:
    cargo test -p flowcus-app --test server_test

# Run tests with coverage (requires cargo-llvm-cov)
test-coverage:
    cargo llvm-cov --workspace --html

# --- Benchmarks ---

# Run all benchmarks
bench:
    cargo bench --workspace

# Run worker pool benchmarks
bench-worker:
    cargo bench -p flowcus-worker

# --- Quality ---

# Run all checks (lint, format, test)
check: fmt-check lint test

# Format code
fmt:
    cargo fmt --all
    cd frontend && npx eslint --fix . 2>/dev/null || true

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Run clippy
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Full lint (Rust + frontend)
lint-all: lint frontend-lint frontend-typecheck

# --- CI (local mirror of GitHub Actions) ---

# Run the full CI pipeline locally
ci: ci-check ci-test ci-build
    @echo "CI pipeline passed"

# CI: quality checks
ci-check:
    just fmt-check
    just lint-all

# CI: test suite
ci-test:
    just test

# CI: production build
ci-build:
    just build

# --- Utilities ---

# Clean all build artifacts
clean:
    cargo clean
    rm -rf frontend/dist frontend/node_modules/.vite

# Show dependency tree
deps:
    cargo tree --workspace

# Audit dependencies for vulnerabilities
audit:
    cargo audit

# Update dependencies
update:
    cargo update
    cd frontend && npm update
