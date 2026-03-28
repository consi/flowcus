---
name: add-component
description: Add a new system component (crate) to the Flowcus workspace
---

# Add Component Skill

When adding a new component to Flowcus:

## Steps

1. Create the crate directory:
   ```bash
   mkdir -p crates/flowcus-<name>/src
   ```

2. Create `crates/flowcus-<name>/Cargo.toml`:
   - Use `version.workspace = true`, `edition.workspace = true`, etc.
   - Add `[lints] workspace = true`
   - Reference workspace dependencies

3. Add to workspace `Cargo.toml`:
   - Add `"crates/flowcus-<name>"` to `[workspace] members`
   - Add `flowcus-<name> = { path = "crates/flowcus-<name>" }` to `[workspace.dependencies]`

4. Create `crates/flowcus-<name>/src/lib.rs` with module structure

5. Wire it into dependent crates (usually `flowcus-app` and/or `flowcus-server`)

6. Add relevant tests:
   - Unit tests in source files
   - Integration test in `tests/integration/` if cross-crate behavior
   - E2E test cases if it affects HTTP API

7. Update CLAUDE.md architecture section if the component is significant

## Naming Convention
- Crate: `flowcus-<name>` (kebab-case)
- Module: `flowcus_<name>` (snake_case in Rust)
