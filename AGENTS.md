# Agent Instructions

This project uses **br** (`beads_rust`) for all issue tracking. The local database is SQLite and the Git-tracked interchange/source snapshot is `.beads/issues.jsonl`. **Do not use `bd`, Dolt, or `bd dolt push`.**

## Quick reference

```bash
br ready                         # Find unblocked work
br show <id>                     # Read a Bead
br update <id> --claim           # Claim work atomically
br close <id>                    # Close completed work
br blocked                       # Inspect blocked work
br lint                          # Validate Bead templates
br doctor                        # Diagnose workspace state
br sync --status                 # Compare SQLite and JSONL
br sync --flush-only             # Export SQLite changes to tracked JSONL
br sync --import-only            # Import tracked JSONL into SQLite
```

Use non-interactive commands. In particular, use `cp -f`, `mv -f`, `rm -f`, and `rm -rf`; never invoke an editor-based command.

## Rules

- Use `br` for all task tracking; do not use markdown TODO lists or another issue tracker.
- Read the complete Bead before implementation and claim it with `br update <id> --claim`.
- Treat `.beads/issues.jsonl` as the Git-pinned graph snapshot for the checked-out commit.
- Use the local `.beads/beads.db` SQLite database for commands; it is ignored by Git.
- After JSONL changes from Git, run `br sync --import-only` before relying on readiness/status.
- Before committing Bead changes, run `br sync --flush-only`, then stage `.beads/issues.jsonl`.
- Never use Dolt or any remote database push. Beads synchronization between collaborators happens through Git-tracked JSONL.

## Session completion

Work is not complete until Git is pushed successfully:

```bash
git status
br lint
br sync --flush-only
git add <changed-files> .beads/issues.jsonl
git commit -m "..."
git pull --rebase
git push
git status  # must be clean and synchronized with origin
```

If a rebase changes `.beads/issues.jsonl`, run `br sync --import-only` and re-check `br doctor`, `br lint`, and `br ready` before continuing.
