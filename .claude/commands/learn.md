---
name: learn
description: Analyze recent changes and update the knowledge base
---

Review the recent git diff and conversation context. Identify any:

1. **Architectural decisions** - New components, dependency choices, structural changes
2. **Code patterns** - Recurring idioms, error handling approaches, test patterns
3. **Issues/gotchas** - Problems encountered and their solutions
4. **Decision rationale** - Why something was done a specific way

For each finding, check if it already exists in `.claude/knowledge/`. If not, create a new file in the appropriate subdirectory:
- `architecture/` for structural decisions
- `decisions/` for ADR-style records
- `patterns/` for code patterns
- `issues/` for gotchas and fixes

Use this format:
```markdown
# Title
**Context:** When this applies
**Decision/Pattern:** What to do
**Rationale:** Why
**Last verified:** <today's date>
```

If an existing knowledge file is outdated, update it with current information and the new verification date.

After updating, list what was added or changed.
