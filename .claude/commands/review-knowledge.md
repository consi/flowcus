---
name: review-knowledge
description: Audit and clean up the knowledge base
---

Scan all files in `.claude/knowledge/` and for each one:

1. **Verify accuracy** - Read the referenced code/files to confirm the knowledge is still correct
2. **Check staleness** - Flag entries with "Last verified" older than 30 days
3. **Remove duplicates** - Merge entries that cover the same topic
4. **Identify gaps** - Look at the current codebase and note important patterns or decisions not yet documented

Report:
- Files updated (with what changed)
- Files removed (with reason)
- Suggested new entries (topic + which directory)
