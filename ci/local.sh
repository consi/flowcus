#!/usr/bin/env bash
# Local CI pipeline - mirrors GitHub Actions workflow
# Usage: ./ci/local.sh [stage]
# Stages: check, test, build, all (default)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

step() {
    echo -e "${YELLOW}>>> $1${NC}"
}

pass() {
    echo -e "${GREEN}  PASS: $1${NC}"
}

fail() {
    echo -e "${RED}  FAIL: $1${NC}"
    exit 1
}

stage_check() {
    step "Formatting check"
    cargo fmt --all -- --check && pass "cargo fmt" || fail "cargo fmt"

    step "Clippy"
    cargo clippy --workspace --all-targets -- -D warnings && pass "clippy" || fail "clippy"

    step "Frontend install"
    cd frontend && npm ci --prefer-offline 2>/dev/null || npm install
    cd "$ROOT_DIR"

    step "Frontend typecheck"
    cd frontend && npm run typecheck && pass "typecheck" || fail "typecheck"
    cd "$ROOT_DIR"

    step "Frontend build"
    cd frontend && npm run build && pass "frontend build" || fail "frontend build"
    cd "$ROOT_DIR"
}

stage_test() {
    step "Unit tests"
    cargo test --workspace --lib && pass "unit tests" || fail "unit tests"

    step "Integration tests"
    cargo test --test worker_integration && pass "integration tests" || fail "integration tests"

    step "E2E tests"
    cargo test --test server_test && pass "e2e tests" || fail "e2e tests"
}

stage_build() {
    step "Release build"
    cargo build --release && pass "release build" || fail "release build"

    local size
    size=$(du -h target/release/flowcus 2>/dev/null | cut -f1)
    echo -e "  Binary size: ${size:-N/A}"
}

STAGE="${1:-all}"

echo "=== Flowcus Local CI ==="
echo "Stage: $STAGE"
echo ""

case "$STAGE" in
    check)  stage_check ;;
    test)   stage_check; stage_test ;;
    build)  stage_check; stage_test; stage_build ;;
    all)    stage_check; stage_test; stage_build ;;
    *)      echo "Unknown stage: $STAGE"; echo "Usage: $0 [check|test|build|all]"; exit 1 ;;
esac

echo ""
echo -e "${GREEN}=== CI pipeline passed ===${NC}"
