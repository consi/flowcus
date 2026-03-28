---
name: ci
description: Run CI pipeline locally or prepare for GitHub Actions
---

# CI Skill

## Local CI
Run the full CI pipeline locally, mirroring GitHub Actions:

```bash
# Full pipeline
just ci

# Or use the script directly
./ci/local.sh all

# Individual stages
./ci/local.sh check   # format + lint
./ci/local.sh test    # check + tests
./ci/local.sh build   # check + test + release build
```

## GitHub Actions
The CI workflow is in `.github/workflows/ci.yml`. It runs:

1. **check** - formatting, clippy, frontend typecheck
2. **test** - unit, integration, e2e tests
3. **build** - release builds for x86_64 and aarch64

## Keeping local and CI in sync
- `Justfile` recipes mirror CI steps exactly
- `ci/local.sh` runs the same commands as GitHub Actions
- When adding a new CI step, add it to both `ci.yml` and the Justfile
