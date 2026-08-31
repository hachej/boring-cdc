# Beads workspace

This repository uses **br** (`beads_rust`) for issue tracking.

- Local command database: `.beads/beads.db` (SQLite, ignored by Git)
- Tracked collaboration snapshot: `.beads/issues.jsonl`
- No Dolt backend or remote issue database is used.

## Common commands

```bash
br ready
br list
br show <issue-id>
br update <issue-id> --claim
br close <issue-id>
br lint
br doctor
```

## Synchronization

Import the checked-in snapshot after checkout, pull, or rebase:

```bash
br sync --import-only
```

Export local issue changes before committing:

```bash
br sync --flush-only
git add .beads/issues.jsonl
```

Collaboration and history use normal Git commits and pushes. Do not run `bd` or any Dolt command.
