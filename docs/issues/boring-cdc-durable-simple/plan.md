# [Boring CDC Durable Stream Simple Case] Execution plan

## Objective
Deliver only the priority-1 simple case: one PostgreSQL 17.6 database, one configured publication, one configured pgoutput slot, one Rust binary, and one SQLite journal. From an empty operator database, `init → run --bootstrap → run` must survive `kill -9`, retain writes made while down, catch up without loss or duplication, and never advance PostgreSQL `confirmed_flush_lsn` beyond the durably journalled boundary. Backfill, chunking, anchors, ClickHouse, Parquet, and other destination work are excluded.

## Reproduction and decisions
`.handoff/supervisor-repro.sh` reproduced the reported chain on the starting SHA `b1eeb226cce8f6763be3ae48901db50bc5484ae7`: `init --confirm` failed with `M2_INIT_PUBLICATION_DRIFT`; `run --bootstrap` then failed with `M3_SOURCE_STATE_UNAVAILABLE`; no slot existed; both stream attempts failed and the journal remained empty. Inspection also confirms the production runtime re-exports article-1 publication/slot constants behind `ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol` markers and rejects different configured names.

All three blockers can be resolved without another owner decision. The owner already selected configured publication/slot names, allowed either exact prerequisite SQL or provisioning for init, and required the command order to be documented. Workers must stop only if repository contracts conflict on the exact protocol or privilege model; no such conflict is known at Gate 1.

## Dependency-correct Bead graph

1. `boring-cdc-ckoi.1` — configure publication and slot protocol. Finish the unresolved `boring-cdc-4py0.15` / `boring-cdc-d-pg-protocol` decision, remove article-1-only runtime coupling, and retain the demo fixture.
2. `boring-cdc-ckoi.2` — make init reachable and diagnosable. Depends on `.1`; supplies exercised prerequisites, specific fail-closed drift diagnostics, and the operator command sequence.
3. `boring-cdc-ckoi.3` — prove crash-safe durable stream acceptance. Depends on `.1` and `.2`; adds `scripts/acceptance/durable_simple_case.sh` and fixes only product defects exposed by the real path. It must not modify SQLite directly.
4. `boring-cdc-ckoi.4` — terminal certification. Depends on `.3`; merges `origin/master` forward, runs the complete required matrix and real PostgreSQL 17.6 acceptance, performs the sole repo-wide reseal, pushes, and opens/updates the PR to `master` with `factory: MERGE-READY <full SHA>`.

The parent epic is `boring-cdc-ckoi`; every node carries `epic:boring-cdc-durable-simple`. The chain is intentionally mostly serial because the slices share runtime assumptions and one shared worktree.

## Proof and operating contract
Every Worker verifies its exact ready/unassigned Bead, claims it with its session actor, changes only its declared scope, and records a complete handoff with SHA, commands, artifacts, and adversarial review provenance. Before every push it merges `origin/master` forward and runs: `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets -- -D warnings`; `cargo test --locked --workspace --all-targets`; all four scaffold validators; and `python3 -m unittest discover -s tests`. `TMPDIR=/var/tmp` is mandatory. Evidence is generated only by real runs against pinned PostgreSQL 17.6; tests are never removed or weakened, and evidence is resealed only once in the terminal slice.

The acceptance script must assert all committed transactions appear exactly once, journal sequence has no gaps, transaction IDs are unique, and observed `confirmed_flush_lsn` never exceeds the journal's durable LSN. It must drive the product's actual init/bootstrap/run interfaces and never insert or update SQLite itself.

## Risk and rollback
Primary risks are unsafe PostgreSQL identifier interpolation, privilege setup that is documented but not executable, bootstrap state that still relies on test-only journal mutation, and feedback racing ahead of SQLite durability. Each boundary remains fail-closed and receives focused negative tests. A slice is rolled back by reverting its commit before owner approval; no worker merges. Unexpected M3/M4 defects are reported, not fixed here.

## Review record
The mandatory adversarial plan-review mechanism is not exposed to this Orchestrator. Gate 1 therefore discloses that limitation rather than self-certifying. The owner decides whether the reproduced evidence, bounded graph, and explicit proof path are sufficient to dispatch.
