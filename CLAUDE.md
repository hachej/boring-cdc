# Project instructions for AI agents

Follow [`AGENTS.md`](AGENTS.md).

This project uses **br** (`beads_rust`) with a local SQLite database and Git-tracked `.beads/issues.jsonl`. Do not use `bd` or Dolt.

Before work:

```bash
br sync --import-only
br ready
br show <id>
br update <id> --claim
```

Before committing issue changes:

```bash
br lint
br sync --flush-only
git add .beads/issues.jsonl
git commit -m "..."
git pull --rebase
git push
git status
```

Work is incomplete until the Git push succeeds and the working tree is clean.
