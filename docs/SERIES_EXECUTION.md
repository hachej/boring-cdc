# Series execution and evidence handoff

This schedule connects the five Boring CDC articles to real implementation evidence. It does not change product scope, approve a technical decision, promise publication dates, or replace Beads readiness. Product obligations remain in [REQUIREMENTS.md](REQUIREMENTS.md), architecture and milestone exits in [PLAN.md](PLAN.md), and live ownership/dependencies in `br`.

The source editorial context is `boring-content/research/clients/estuary/boring-cdc-series.md`. Its older Debezium-only/optional-extra-vendor notes conflict with the current CDC requirements. The policy for this experiment is **Estuary for externally visible comparisons; Debezium for capture/offset explanation only; no additional benchmark vendors**. The preparation owner must reconcile the source note before comparison commissioning or publication. No sponsor/client approval is inferred from this engineering schedule.

## Owners and preparation

- **`boring-cdc-v01.5` — series preparation:** accountable editorial/access owner Julien (repository owner). Owns source-note reconciliation, article evidence mapping, access/permissions/budget provenance, and editorial handoff. This non-product planning task can run before M0; it does not block product milestones M0–M6.
- **Existing milestone proof owners:** retain their code/scenario/command evidence and the redacted implementation story. They do not write or publish articles or inherit later runtime requirements.
- **`boring-cdc-m7-estuary`:** owns final measured external scenarios and their reproducibility/capability limits, consuming preparation. Early access preparation is not a comparative result.
- **`boring-cdc-m7-docs`:** owns public, source-bound reproduction and limitations documentation, consuming preparation. Article drafting/publication remains in the content workflow, not a duplicate product issue tracker.

Preparation establishes tenant/region and observable version/configuration, secure credential handoff (never credential values in artifacts), source/destination connectivity, budget, allowed outage/recovery operations, event-level observation capability, and the person authorizing any cost-bearing experiment. Access may be explicitly unavailable; local evidence remains publishable with that limitation. An unresolved comparison-policy conflict must keep preparation open. Source-note edits must not stage or push unrelated content work.

## Sequencing policy

**Build and preserve evidence progressively; publish only after the evidence promised by an installment exists.** Milestone numbers are not article-ready tags. Final comparative results currently belong to M7; that ordering remains unchanged. Until then, articles may be drafted with explicit evidence placeholders, not invented measurements. Julien controls publication dates after reviewing the evidence and disclosure. Publishing earlier comparisons would require an explicit ownership/dependency revision, not borrowing future evidence.

In particular, M1 does not provide a production destination, M2 JSONL candidate segments are not live output, and M3 anchor proof is not final ClickHouse/archive acceptance. Do not bypass anchors or promote a diagnostic WAL suffix merely to produce an earlier demo.

| Article | Build/story sources | Required reader-facing evidence before publication | Final comparison/handoff |
|---|---|---|---|
| 1. How Postgres CDC Works | `boring-cdc-m1-raw-demo`: protocol, workload, decode and fail-closed fixtures | M1 raw-event command plus `boring-cdc-m4-bench`/`boring-cdc-m4-toast` evidence connecting an event to a canonical destination row; explain the tested topology, keys, source-risk and oracle limits | Relevant M7 external decode/observation evidence where available; otherwise clearly label unavailable visibility. Debezium explanation only |
| 2. I Vibe-Coded Durable CDC | `boring-cdc-m2-fault-status`: spool, atomic commit, feedback and crash evidence | M2 crash boundaries plus completed M3 anchor and M4 live ClickHouse write-before-checkpoint/replay/convergence evidence. M2 JSONL internals may be inspected only as candidate/diagnostic artifacts, never advertised as live complete state | `boring-cdc-m7-estuary` outage/recovery scenarios; distinguish local internal crash hooks from managed external outages |
| 3. I Added Backfills | `boring-cdc-m3-faults` and `boring-cdc-m3-oracle`: initial/restarted snapshots, concurrent mutation, naive control and source impact | Bounded snapshot/stitching and independent event-correlation evidence; actual destination comparisons from M4/M5 for each destination claimed. Snapshot baselines do not prove historical business-event delivery | M7 matching-profile backfill/control results or explicit unavailable outcome |
| 4. I Sent CDC into ClickHouse | `boring-cdc-m4-bench`: current-state/TOAST/tombstone/merge/FINAL/query cost and durability/audit evidence | Canonical view correctness before/after merges; update/delete/DDL cases and capacity cost. Full table-add acceptance also consumes `boring-cdc-m5-table-add`, not an M4-only claim about both destinations | M7 ClickHouse external scenarios with common observable guarantees and stated version/durability differences |
| 5. I Added a Second Destination | `boring-cdc-m5-faults`: JSONL/Parquet, schedules, independent failure/replay, anchors/retention and full table-add | Actual archive reconstruct/verify commands, retained-anchor late add, one-destination outage, expiry rejection, selector visibility, independent checkpoint/catch-up and source-load measurements | M7 fan-out/recovery comparison; do not equate local changelog files with remote managed current-state or object-store guarantees |

For every row the owner supplies the **actually executed** command/argv, code commit and binary/image/configuration/profile/seed digests, scenario IDs, raw redacted results, correctness verdicts and operator steps. This planning document deliberately does not claim that future CLI or scripts already exist. Reproduction commands come from their canonical command owners and measured evidence, never guessed article snippets.

## Preserve the implementation story

At each capability boundary retain redacted originating prompts, the first relevant implementation output/commit, failed attempts and fault evidence, subsequent corrections, and explicit human interventions. Bind them to the Bead/code/scenario snapshot under the existing evidence root. Never rewrite an earlier failure into success, silently reconstruct prompts after the fact, or publish private agent context, credentials, production payloads or unrelated client material. Public examples use synthetic workload data only.

This is evidence handoff within existing work—not a new connector feature or requirement to run a release matrix on every leaf.

## Correctness and comparison rules

1. Report **ledger delivery**, **independently observed business-event delivery**, and **final-state convergence** separately. Test business-only and ledger-only loss. A managed system without sufficient independent event/delete visibility has an unavailable event-level measurement, even when ledger and final-state results pass.
2. Use equivalent externally visible scenarios, not fabricated managed journal/ack crash hooks. Label unsupported and manual operations.
3. Freeze workload/profile/reset and measurement intervals. Do not contaminate source-load comparisons with simultaneous competing captures unless that competition is the declared scenario. Account for workload/ledger/harness overhead.
4. Record tenant/region/configuration/time provenance and matching local baseline inputs. Compare only common observable guarantees; document local-filesystem/single-node versus managed-service differences.
5. Report resource consumption, managed charges, credits, hardware assumptions and operator effort separately. Zero marginal local invoice is not a free-CDC claim.
6. Disclose sponsorship in each article. Preserve unfavorable results, limitations and failed tests; there is no predetermined winner.

## Kickoff and approvals

The first implementation slice is `boring-cdc-m0.1` (bootstrap validators). `boring-cdc-m0.2` adds assignments and read-only context; individual decision work may then proceed. `boring-cdc-m0.3` and `boring-cdc-m0-validation-tooling` remain mandatory before M0 exits. Only repository-local tooling and preparation are authorized before the decision gate; no runtime shortcut is implied.

All 25 decision records still require their own explicit approval evidence. Repository-owner approvals include license, archive scope, history/quota/retirement, finite WAL policy and scale profiles; exposure/security requires the security or repository owner; the named implementation lead approves the other technical contracts. An issue's owner field or this repair's approval is not approval of those selected values. Keep the initial version/type/filesystem/profile matrix as small as the approved workload permits; Parquet, audits, safe recovery and repository-quality obligations remain in completed v0.1.

Beads, not this document, record completion, blockers and assignments. No publication or M0-completion date is promised by an engineering kickoff.
