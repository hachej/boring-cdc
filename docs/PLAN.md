# Boring CDC v0.1 architecture and implementation roadmap

This document is the canonical architectural synthesis and implementation roadmap for Boring CDC v0.1. It was derived from the article-series brief and `docs/REQUIREMENTS.md`, then strengthened through adversarial architecture review. Implementation has not started.

Authority is deliberately layered. `docs/REQUIREMENTS.md` owns product obligations; this plan owns architectural rationale, invariants, boundaries, and sequencing; approved versioned artifacts under `contracts/` own exact literals and machine contracts; Beads own executable task state and acceptance for a pinned projection; evidence manifests own test results. A generated view, context pack, or handoff never overrides those sources. The freeze, drift, and projection rules are defined in `docs/AGENT_SYSTEM.md` and section 2.3 below.

The plan deliberately stays narrow: one PostgreSQL source through `pgoutput`, one publication and one logical replication slot, one Rust binary plus Docker Compose, a local SQLite journal/checkpoint store, ClickHouse, and one scheduled JSONL/Parquet archive materializer. Estuary is the managed reference for externally visible experiments. Debezium is a capture-and-offset design reference only.

## 1. Objective and non-goals

Build the smallest inspectable system that proves this architecture:

```text
PostgreSQL
  -> one pgoutput capture stream
    -> durable local SQLite journal
      -> independent ClickHouse materialization
      -> independent scheduled JSONL/Parquet archive
```

The system must:

- capture once and durably journal before acknowledging PostgreSQL;
- backfill existing rows while concurrent writes continue;
- replay destinations independently;
- converge after expected crashes, retries, and bounded outages;
- expose source-WAL, snapshot, local-disk, schema, and destination risk;
- fail explicitly when an operator must re-seed instead of claiming false health; and
- publish reproducible correctness and source-impact evidence.

The delivery promise is **at-least-once capture with idempotent destination convergence**. A crash may cause duplicate attempts. Boring CDC does not promise global exactly-once delivery, destination atomicity across tables, or atomicity across destinations.

This is an educational and benchmark-driven public project, not a production SLA, distributed platform, or generalized connector framework.

## 2. Fixed scope and design principles

### 2.1 Fixed topology

1. One PostgreSQL database.
2. One publication and one logical replication slot using `pgoutput`.
3. One Tokio-based Rust binary containing CLI, capture, backfill, journal, materializers, safety monitoring, and benchmark helpers.
4. Docker Compose for the reproducible local environment.
5. One SQLite database: `journal_events` is the logical append-only payload journal, and adjacent SQLite tables hold metadata/checkpoints. There is no separate payload-log file.
6. Two independently checkpointed destinations:
   - ClickHouse;
   - one archive materializer capable of scheduled JSONL and Parquet output.
7. No required Kafka, Kafka Connect, schema registry, Kubernetes, control plane, or plugin API.

### 2.2 Safety and correctness principles

1. **Durable before feedback.** PostgreSQL `flush_lsn` and `apply_lsn` feedback never exceed the end LSN durably committed with the source transaction in SQLite.
2. **Capture is destination-independent.** Destination success or failure never controls source acknowledgement.
3. **Journal order is not source order.** `journal_seq` controls local consumption. A total `source_version` controls winner selection for one source key.
4. **One capture epoch is one continuity claim.** Source replacement, unsafe slot movement, slot invalidation, or full re-seed creates a new epoch; versions are never compared across epochs.
5. **A retained suffix is not automatically a baseline.** Every destination candidate that may be promoted—new, replayed, rebuilt, or re-seeded—starts from a completed, retained reconstruction anchor.
6. **Re-seed means fresh destination generations.** A new snapshot cannot remove rows that were deleted before it if it is merged into stale destination state.
7. **Bounded means bounded.** Transaction spool, SQLite files, temporary files, archives on the same filesystem, replay history, snapshot lifetime, and retained PostgreSQL WAL all have declared limits.
8. **Unsupported means blocked.** No state-changing protocol message, value, schema change, or unavailable history may be silently skipped.
9. **One active runtime.** The local runtime and asynchronous completions are fenced so a second process or stale worker cannot corrupt checkpoints.
10. **Idle progress is published, not inferred.** Informational keepalive `wal_end` is never acknowledged. A connector-owned heartbeat row advances through the same published, journaled, durable-before-feedback path as source transactions.
11. **Claims follow measurements.** Table count, throughput, event size, backfill size, outage duration, and cost claims are limited to published profiles that were actually run.

### 2.3 Agent-native implementation and control model

The implementation system mirrors the product's safety model: inspect one authoritative snapshot, derive legal actions, bind mutations to preconditions, verify postconditions, and retain immutable evidence. The full contributor architecture is in `docs/AGENT_SYSTEM.md`; this section fixes its architectural obligations.

#### Tower and authority

The repository forms one linked tower:

```text
charter -> product requirements -> architecture -> approved contracts
        -> Beads ownership/dependencies -> generated bounded views
        -> immutable evidence/claims -> deterministic handoff
```

Stable semantic IDs connect the tower: `REQ-*`, `INV-*`, `DEC-*`, `CMD-*`, `COND-*`, `TRANS-*`, `SCN-*`, `REL-*`, `RISK-*`, `RUNBOOK-*`, `CLAIM-*`, and `FINDING-*`. Identity never depends on wording, line numbers, or tracker order. Every ID has exactly one canonical owner; consumers cite the owner and digest instead of duplicating semantics. Coverage validation rejects missing, duplicate, dangling, stale, or future-evidence ownership.

The plan is architectural source; a Bead is a frozen executable projection for a specific graph and contract digest set. Its effective contract contains the task-specific delta plus versioned shared policies. A canonical change emits an impact set. Affected open/in-progress Beads become stale until regenerated or explicitly re-approved, and prior evidence remains historical rather than proving changed semantics. Repeated Bead boilerplate may be compacted only after clean-checkout effective-contract materialization can prove that the complete standalone contract is present and digest-bound.

#### World state and progressive disclosure

Every plan, mutation, verification, and handoff binds to one `world_state` containing Git commit/dirty-tree digest, Beads snapshot digest, selected Bead, dependency-closure digest, applicable contract digests, generated-view versions, and evidence-index digest. A changed precondition requires re-planning.

Repository-local tooling produces bounded `orient`, `implement`, `review`, `handoff`, and explicit `expand` context packs. Packs declare source IDs/digests, included and omitted sections, and byte/token estimate. The default target is 16 KiB. Normative content is never silently truncated; an oversized pack returns an explicit expansion plan. Routine work begins from the full claimed Bead and relevant dependency/contract fragments, not from mandatory ingestion of the complete plan and tracker snapshot.

The transparent `scripts/agent/` interface will expose `doctor`, `next`, `context`, `impact`, `verify`, `handoff`, `recover`, and `finish`. It derives views from Git, `br`, contracts, and evidence; explains rankings and invalidation; and never automatically claims, mutates the product, commits, pushes, or hides underlying changes. This is repository tooling, not a product daemon, scheduler, or distributed control plane.

#### Evidence economy and accretion

Evidence claims bind owner/scenario IDs to Git, graph, effective-contract, code, fixture, image, configuration, profile, seed, environment, result, artifact, and redaction digests. Reuse requires exact declared compatibility or an explicitly owned compatibility range; a mismatch reports the invalidating input and rerun owner.

Verification tiers keep resource cost proportional without weakening gates: leaf work runs targeted static/unit/contract checks; component owners run boundary e2e/fault suites; milestone gates run workspace and clean-environment integration/reproducibility; M6/M7 own endurance and release matrices. Findings and failed approaches retain stable IDs, provenance, environment, resolution, and supersession. Handoff observations remain non-normative until promoted through the canonical owner.

#### Control symmetry

The product exposes one canonical runtime `SystemSnapshot` and every mutating dry-run returns an `ActionPlan` bound to its `snapshot_id` and `state_revision`. The plan states preconditions, transitions, external effects, resource/continuity consequences, rollback boundary, expected postconditions, and confirmation policy. Confirmation and terminal evidence cite the action-plan digest; changed state or fingerprints force a new plan. Domain Beads continue to own their transitions—the shared envelopes are not a second state machine.

## 3. Runtime architecture and ownership

```text
cli/config
runtime/ownership
postgres/preflight
postgres/bootstrap
postgres/capture
postgres/backfill
journal/sqlite
journal/retention
materialize/clickhouse
materialize/archive
safety/state-machine
status/metrics
benchmark/workload
benchmark/oracle
benchmark/faults
```

Long-running components inside the one binary:

1. PostgreSQL capture loop.
2. Backfill coordinator and bounded workers.
3. Capture-priority SQLite writer.
4. ClickHouse materializer loop.
5. Archive scheduler/materializer loop.
6. Retention and safety monitor.
7. Read-only status and metrics server.
8. Local Unix-domain operator-command endpoint whose handlers execute inside the lock-owning runtime.

`boring-cdc run` acquires both (1) an exclusive lock tied to the configured SQLite store and (2) a fixed PostgreSQL advisory lock derived canonically from the source system identifier, database identity, publication, and slot. The advisory-lock key algorithm is frozen in M0 and is not user-configurable. After the minimum read-only identity query needed to derive the key, ordinary and maintenance runtimes acquire the same source lock before reconciliation, slot/publication use, or mutation, so two configurations pointing different SQLite paths at the same source/slot cannot run concurrently. Either lock conflict fails closed. PostgreSQL also fences the logical slot, but these locks are required to prevent competing bootstrap/recovery workflows, duplicate materializers, archive publishers, and checkpoint writers.

The advisory lock lives on one dedicated, non-pooled SQL session whose backend PID and random connection nonce are persisted for the active `run_id`. Successful probes refresh a monotonic local ownership deadline. Before dispatching any **source-mutating** operation—feedback packet, control-row update, slot operation, or snapshot/export action—the runtime proves that the operation's bounded completion deadline fits inside the remaining ownership interval. Destination writes and promotions do not borrow the source-lock deadline: their immutable intents, generation tokens/namespaces, and restart reconciliation fence stale effects. EOF, backend-PID/nonce change, a failed or late probe, PostgreSQL restart, or any uncertain lock state is ownership loss: stop dispatch, cancel/close source work, fence component generations, persist `ownership_lost` when SQLite remains writable, and exit non-zero. The process never reacquires the lock in place.

Loss of the source/lock connection is therefore ownership loss: “reconnect” in this plan means supervised process restart followed by takeover reconciliation, not in-process advisory-lock reacquisition. In v0.1, loss or uncertainty of the CopyBoth replication connection alone is also ownership loss even when the advisory-lock session still answers; the process fences all dispatch and exits rather than reopening replication in place. The canonical Compose deployment uses an unlimited restart policy—not a finite attempt budget—and pins PostgreSQL `tcp_keepalives_idle`, `tcp_keepalives_interval`, `tcp_keepalives_count`, and `client_connection_check_interval` so an unreachable zombie lock backend is reaped within the declared takeover bound. A successor uses a new `run_id`, acquires both locks, waits longer than the frozen maximum stale-ownership interval, and completes source plus external-intent reconciliation before dispatching work. The wait may be skipped only when `runtime_ownership` contains a persisted, read-back clean-release proof for the immediately previous `run_id`; every crash, timeout, uncertain release, or missing proof requires the full wait.

The source privilege contract is explicit:

| Role | Allowed | Forbidden |
|---|---|---|
| Application | Normal application DML on selected tables | Publication/slot/control-schema administration |
| Capture/bootstrap | PostgreSQL `REPLICATION`, required catalog reads, read-only snapshot queries, advisory-lock acquisition; connector code issues `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` only for the configured permanent slot while a matching persisted bootstrap/re-seed intent is active under both ownership locks | `ALTER`/drop publication is PostgreSQL-enforced by ownership; wrong-name/unbound slot creation and every slot-drop path are connector-enforced because PostgreSQL cannot scope `REPLICATION` by slot name or operation; arbitrary application DML |
| Heartbeat/fence writer | Column-limited `UPDATE` of bounded value columns plus `SELECT` of the immutable key only on the one seeded row in each connector-owned control relation | `INSERT`, `DELETE`, key changes, excess column reads/writes, selected-table DML, or publication/slot administration |
| Administration/maintenance | Exact documented publication/control-schema lifecycle operations, dropping the configured old slot during confirmed re-seed/recovery, and seeding/verifying the two fixed control rows | Creating the exported-snapshot slot, long-lived use by ordinary `run` |

The publication is owned by the dedicated administration role whose credentials are unavailable to application and ordinary capture roles. Normal `boring-cdc run` never loads or retains the administration DSN. ClickHouse likewise uses separate maintenance and runtime materializer roles. Maintenance creates/migrates the shared generation-keyed history and selector objects during `init`, retires old generation partitions after grace, and owns all shared-object DDL. Ordinary `run` never loads that role: its materializer can only insert generation-tagged rows into the pre-created history objects, insert idempotent rows into the append-only promotion selector, and read bounded verification metadata. Publishing a live selector is data insertion, not DDL; no runtime command replaces a view or creates/drops shared objects.

There is exactly one state-store and source owner path at a time. Ordinary operation is owned by `run`. Routine state-changing CLI commands never open SQLite writable beside it: the CLI submits a bounded idempotent request to `run` over the local Unix-domain command endpoint, and the owner revalidates and executes the transition through the capture-priority SQLite writer. The owner issues a fresh unpredictable nonce for each dry-run. The request ID is `hash(nonce || canonical_request_payload)`, with the displayed fingerprints and expiry inside that canonical payload, so retries of one displayed dry-run reuse the ID while a later legitimately repeated command necessarily gets a new ID. A request also carries expected `run_id`, capture epoch, configuration, table-set, anchor, and generation fingerprints as applicable; expiry; and the exact transition. Retrying the same ID and payload returns the persisted result; reusing an ID with a different payload is corruption. Lost responses are therefore safe to retry. Startup never re-executes an incomplete request: any nonterminal `operator_command_requests` row from a prior `run_id` becomes `aborted_by_restart`, after remote-intent reconciliation where applicable, and the operator must obtain a new dry-run.

The command socket is local-only, lives below a `0700` directory, has mode `0600`, enforces bounded request/response sizes and timeouts, and verifies Linux peer credentials match the connector account. The status/Prometheus TCP listener is read-only and never routes mutation. When ordinary `run` is absent, an eligible offline dry-run is itself a short-lived owner transaction: it acquires both ownership locks in canonical order, reconciles, proves no live runtime owns the source, and atomically persists the nonce/request snapshot with JSON `expected_run_id = null` before cleanly releasing both locks and displaying the confirmation. The confirming offline owner reacquires both locks, reconciles again, proves no live `run` started after issuance, revalidates every bound fingerprint and expiry, consumes the nonce exactly once, and only then persists the intent. It never performs an unowned write, bypasses reconciliation, or mutates from a second owner.

A workflow that needs the administration role (`init`, confirmed full re-seed, or explicit source recovery) is a **maintenance runtime**: the operator first stops ordinary `run`; the maintenance command then acquires the same exclusive SQLite and source-derived PostgreSQL advisory locks, performs integrity/source reconciliation, and loads the administration DSN only for the minimum owner-only operation. It drops that credential immediately after readback/verification and keeps both locks until its persisted workflow reaches a safe terminal or resumable phase. A maintenance command attempted while either lock is owned fails closed with handoff instructions.

A selected-table-set change is deliberately disruptive in v0.1. It is never applied online to the current capture epoch. The operator confirms a full re-seed: stop the old runtime, create a new capture epoch, recreate the publication and slot for the complete expanded table set, snapshot every selected table, and build fresh full ClickHouse and archive generations. The old destination generations remain readable until the new full generations pass verification and are promoted. This keeps table addition within the requirements' tested re-backfill path instead of introducing an online schema-control plane.

Each process start receives a `run_id`. Every backfill and destination has a persisted generation token. State and checkpoint updates use compare-and-swap against that token. A completion from a paused, detached, replaced, or re-seeded component is stale and cannot advance state.

Each destination worker additionally holds a persisted, expiring generation lease containing its destination ID, `capture_epoch`, anchor identity (when bootstrapping), generation, configuration fingerprint, and `run_id`. It validates the generation token and lease immediately before every external side effect and writes only into the namespace named by that epoch/generation. A SQLite compare-and-swap does not undo an already completed ClickHouse insert, archive publication, or selector switch; the lease and the promotion protocol in section 9 fence which namespace can be made live.

A component or ownership failure must become persisted visible state when SQLite remains writable. If SQLite cannot persist the failure, health fails, the process exits non-zero, and the operator must not infer health from stale metadata. No action may continue merely because an old lease or stale status row still appears valid after source-lock uncertainty.

## 4. PostgreSQL protocol contract

### 4.1 Supported protocol surface

Before M1 begins, the project selects and pins a Rust PostgreSQL logical-replication transport that explicitly supports replication-mode `CopyBoth`; ordinary query-only `tokio-postgres` usage must not be assumed to provide this protocol. The project freezes supported PostgreSQL major versions, transport/crate versions, wire framing limits, and exact `START_REPLICATION SLOT ... LOGICAL` options. The options explicitly request `origin='any'` where supported by the pinned PostgreSQL version; if a pinned version cannot express that policy, M0 must prove the equivalent accepted-origin behavior or exclude the version. `Origin` messages are parsed and classified as transaction metadata but never increment row ordinals. The origin policy is part of the supported-protocol fingerprint. Protocol fixtures exercise bidirectional CopyData/keepalive/feedback behavior on every admitted PostgreSQL version. The v0.1 publication is created or verified with:

```text
publish = 'insert,update,delete,truncate'
```

`TRUNCATE` is published for **detection only**. It is not materialized in v0.1: receiving a real `Truncate` message blocks capture before acknowledgement, marks the affected continuity as requiring re-seed, and gives an explicit recovery path. Excluding it from the publication would be unsafe because PostgreSQL would then emit no message and stale destination rows would remain undetectable. Any other unsupported state-changing message likewise blocks before acknowledgement.

Unless separately implemented and covered by the full fault matrix, v0.1 disables or rejects:

- streamed in-progress transactions;
- two-phase transaction decoding;
- binary tuple mode;
- publication row filters;
- publication column lists;
- unsupported partition-root/leaf publication configurations;
- unsupported replica identities; and
- source types outside the frozen v0.1 type matrix.

Relation, `Origin`, and other metadata messages may be emitted again after reconnect. They never affect row-change ordinal assignment or event identity. Every changed `Relation` contract triggers synchronous catalog validation before any subsequent row event for that relation is decoded or its transaction acknowledged; the periodic catalog poll is defense in depth, not the only DDL guard.

At startup and on a documented polling interval while running, the connector recomputes the publication-definition fingerprint. The expected fingerprint includes owner, published operation set, table membership (including connector control relations), row-filter/column-list absence, and partition settings. An unexpected change is detected no earlier than that interval; once detected, feedback stops before any newly decoded transaction is acknowledged and state transitions to `publication_drift_requires_reseed`. Because changes may have been omitted during the detection interval, continuity is not assumed even when detection is prompt. Planned table-set changes use the full-reseed workflow in section 12.3; direct live `ALTER PUBLICATION` is unsupported.

### 4.2 Preflight

`boring-cdc check` verifies without mutation:

- `wal_level=logical`, required privileges, and supported server version;
- publication definition, table membership, and connector-owned published `boring_cdc_control.capture_fences` and `boring_cdc_control.heartbeat` relations;
- slot name, plugin, activity, `restart_lsn`, `confirmed_flush_lsn`, `wal_status`, `safe_wal_size`, `logical_decoding_work_mem`, and version-available `pg_stat_replication_slots` spill/stream counters;
- finite `max_slot_wal_keep_size`, unless a clearly labelled local-experiment override is set;
- selected tables, primary/unique keys, replica identity, partition shape, and supported key forms;
- source types, row/event-size limits, and ClickHouse/archive mappings;
- incompatible row filters, column lists, streamed/two-phase settings, publication ownership/drift, the required detection-only `TRUNCATE` operation, and the exact frozen `START_REPLICATION` options;
- selected CopyBoth-capable replication transport, exact origin policy, the source/ClickHouse privilege matrices, deterministic source advisory-lock derivation, dedicated lock-session probe/deadline/takeover policy, control-writer privileges limited to bounded value-column updates plus immutable-key-only reads of exactly one seeded row in each control relation, and role/database values for `idle_in_transaction_session_timeout`, `statement_timeout`, `lock_timeout`, TCP keepalives, and `client_connection_check_interval` compatible with bounded guard/importer/lock lifetimes;
- local SQLite/journal path, named supported Linux filesystem semantics, per-filesystem disk budgets, emergency reserves, configured process/cgroup memory limit against the aggregate receive/decoder/staging/worker equation, replay policy, and nonterminal command/re-seed/promotion intents;
- Unix-domain command endpoint path/modes/peer-credential policy and proof that status/Prometheus listeners are read-only; and
- availability of required operational metrics.

A missing source free-disk metric is not reported as healthy. Docker Compose must expose it. For a remote source where it cannot be collected, status reports an explicit unknown condition. New bootstrap/backfill is blocked unless an operator enables a prominently unsafe local-experiment override; healthy capture continues because stopping it can worsen retained WAL.

### 4.3 Source identity and capture epochs

Persist in `source_state`:

- `capture_epoch`;
- PostgreSQL system identifier and timeline;
- database identity;
- slot name and plugin;
- publication-definition fingerprint;
- supported-protocol configuration fingerprint;
- last received LSN;
- nullable durable transaction end LSN derived only from a decoded, durably published source transaction;
- nullable server-established slot-creation floor LSN, stored separately from transaction progress and valid only for its matching nonterminal bootstrap intent;
- last feedback LSN, including whether a requested bootstrap reply merely repeated the verified creation floor; and
- observed server slot positions and status.

Include `capture_epoch` in transaction and event identities. Never compare `source_version` values across capture epochs.

On every startup, reconcile the live source with local state before streaming. Rows are evaluated top-to-bottom and the first match wins:

| Condition | Required behavior |
|---|---|
| Source, timeline, database, slot/plugin, publication, or protocol fingerprint mismatch | Block startup and require inspection/re-seed as appropriate |
| Bootstrap intent is `prepared`, the slot exists, and no creation floor/snapshot token was persisted | Transition to `bootstrap_ambiguous_requires_restart`; section 7.1 provenance and WAL-continuity recovery takes precedence over generic ahead-of-local handling |
| Matching nonterminal bootstrap has a persisted creation floor, no local source transaction exists, and server `confirmed_flush_lsn` is null or equals that floor | Treat it only as the server-established creation floor; request the pinned protocol zero sentinel or repeat that persisted floor only, and never populate durable transaction progress from it |
| Server `confirmed_flush_lsn` ahead of both the verified creation floor and SQLite durable transaction end, outside the preceding ambiguous-bootstrap case | `requires_reseed`; local restored state may have lost acknowledged events |
| Required local resume LSN no longer available or slot invalid | `requires_reseed` |
| Server slot behind local durable transaction end LSN and resume WAL remains available | Request the local durable position; tolerate deterministic duplicate delivery |
| Server and local positions compatible | Request SQLite's durable transaction end LSN; test PostgreSQL's effective restart rule `max(requested_lsn, confirmed_flush_lsn)` |

Server-confirmed progress may advance only through a durably committed published transaction end LSN. The slot-creation `consistent_point` is not a decoded transaction, is never copied into `durable_transaction_end_lsn`, and is never proactively acknowledged. Before the first journaled source transaction, a requested status reply uses the pinned protocol's zero sentinel or repeats only the verified server-established creation floor; it cannot manufacture progress. After durable transaction progress exists, a standby-status update sets `write_lsn`, `flush_lsn`, and `apply_lsn` to that same persisted safe boundary. It includes the current client timestamp and requested-reply flag required by the pinned protocol. A keepalive with `replyRequested=1` triggers an immediate status packet even when the safe boundary has not advanced, but never copies informational keepalive `wal_end`, the merely received WAL end, a timer-derived position, or a destination checkpoint. Startup fixtures cover null/equal creation-floor representations, requested positions on both sides of `confirmed_flush_lsn`, requested keepalive replies, and PostgreSQL's effective `max(requested_lsn, confirmed_flush_lsn)` behavior.

### 4.4 Published heartbeat progress

A quiet selected-table workload can still coexist with unrelated or unpublished WAL. Informational keepalives cannot safely prove that WAL is represented in the journal, so v0.1 advances an idle slot only through a real published transaction:

1. `boring_cdc_control.heartbeat` contains exactly one administration-seeded connector-owned row with an immutable key, monotonic nonce, and update time; it is in the existing single publication.
2. On the configured interval, the one runtime uses a narrowly scoped SQL credential that has column-level `UPDATE` only for the nonce/time fields of that fixed row—no `INSERT`, `DELETE`, or key update. M0 also grants `SELECT` on the immutable key column only so the bounded keyed `UPDATE ... WHERE key = ...` is executable; unqualified updates are forbidden. The command requires exactly one affected row; zero or multiple rows blocks control progress. The heartbeat has fixed maximum payload size and cannot be user-configured into arbitrary SQL.
3. `pgoutput` captures the heartbeat transaction. The transaction and an internal `control_kind=heartbeat` event are committed to SQLite under the normal durable-before-feedback invariant.
4. Only then may feedback advance through that heartbeat transaction's end LSN. Keepalive `wal_end` remains informational.
5. ClickHouse and archive materializers route heartbeat/control events as journal-only no-ops: they validate the control envelope and advance their complete-transaction checkpoint without writing a user table row.
6. Heartbeat failure is visible as degraded idle-progress health with WAL-headroom impact; it never causes synthetic feedback.

The capture-fence relation follows the same internal-control routing, except its durable event also completes the matching anchor proof. Heartbeat cadence, role grants, retry policy, and maximum source write rate are frozen in M0 and exercised during idle-source WAL-retention tests.

## 5. Capture, transaction durability, and ordering

### 5.1 Row decoding

Decode `Begin`, `Relation`, `Insert`, `Update`, `Delete`, and `Commit` semantics needed by v0.1, and recognize real `Truncate` messages for fail-closed detection. Preserve source transaction identity and the order of row-change messages. For either connector-owned fixed control relation, only an `Update` of the seeded immutable-key row with the admitted column shape is valid; any observed `Insert`, `Delete`, key change, unexpected column change, or row cardinality identity blocks capture before feedback even if issued administratively.

The WAL transaction ordinal is the zero-based index of **row-changing messages only**. Relation, begin, commit, parsed `Origin`, cache refresh, keepalive, and reconnect-dependent metadata do not change it.

If capture cannot losslessly encode any event in a transaction, it stops before acknowledging that transaction. It must not skip an event and continue.

Every selected table has one frozen **canonical source row identity**. It is the admitted primary/unique key configured as the table's effective PostgreSQL replica identity, and `pgoutput` must supply every component for every update and delete. Null, absent, partial, or `unchanged_toast` key components block capture before feedback. Snapshot keyset pagination, event keys, checksums, and destination grouping all use this same identity; v0.1 does not translate between a separate backfill key and replica identity.

A canonical-key-changing update with complete old and new identities and a complete new tuple expands deterministically into:

1. an old-key tombstone; and
2. a new-key upsert.

A `mutation_ordinal` orders those expanded mutations. Repeated changes to the same key in one transaction remain deterministically ordered. If **any** column is `unchanged_toast` during a key change, lossless new-key reconstruction is impossible under the v0.1 model, so capture blocks before feedback.

### 5.2 Bounded transaction spool

A complete PostgreSQL transaction is the atomic visibility unit in the local journal, but it is not buffered without bounds in memory. One `TxnBuffer` abstraction receives bounded CopyBoth frames incrementally, reserves every byte through one `MemoryBudget`, and exposes one iterator to the commit path; it may retain a bounded in-memory prefix and overflows to its one file, so commit logic does not branch on storage location. The transport propagates backpressure to the socket; adapters that collect a complete transaction or unbounded sequence of CopyData frames are rejected. M0 freezes named receive-buffer, decoder-queue, event-staging, and file-spool reservations within that one budget. Pre-allocation checks apply before every reservation, and no layer may hold an unaccounted second copy.

- Incoming row events spool to a bounded temporary file while the transaction is in progress.
- Spool bytes count against the same local disk budget and emergency reserve as SQLite.
- `max_transaction_bytes` and `max_transaction_events` are hard limits.
- Temporary data remains invisible to journal readers.
- At commit, validated events, immutable relation schemas, transaction metadata, checksums, and durable source LSN are published in one bounded SQLite transaction.
- The configured transaction limits make that SQLite writer hold time finite and testable.
- Only after this commit succeeds may the capture loop send PostgreSQL feedback.
- After publish or rollback, the spool is removed and its directory durability policy is applied.
- Spool filenames and headers contain capture epoch, transaction identity, creation run, length, and checksum; no spool is treated as committed state.
- After acquiring the exclusive runtime lock, startup scans and accounts for every spool before accepting source work. A spool whose transaction is already committed in SQLite is removed and the directory is synced. A well-formed uncommitted spool from a dead run is discarded because PostgreSQL will replay it. A malformed, unowned, or contradictory spool is quarantined and startup blocks for inspection rather than guessing.
- If a limit, checksum error, I/O error, or `ENOSPC` occurs, capture disconnects or safe-stops without acknowledgement.

One M0-owned, versioned typed `FailurePolicy` is shared by capture and both destinations and persisted in `processing_failures`; domains supply typed recovery hooks but cannot redefine its classes, schedule, fingerprint, persistence, or re-arm transitions: `Transient { schedule }`, `Deterministic { fingerprint }`, or `Exhausted { fingerprint }`. Network loss and other demonstrably environmental failures persist capped exponential backoff with jitter; before any reconnect, a restarted process waits until the persisted `next_retry_at`, so Compose may restart indefinitely without becoming the retry scheduler. A deterministic protocol, encoding, checksum, or transaction-limit failure records capture epoch, XID/final LSN when known, failure class, validated configuration/limit fingerprint, and observed bytes/events. The process remains alive in persisted `capture_safe_stopped`, keeps status and safe destination draining available, and does not reconnect replication for the same fingerprint. The checkpoint never advances. An admitted-limit failure re-arms only when a later startup validates a **different** relevant limit/configuration fingerprint and proves the required WAL remains; no resume command exists. Other deterministic capture failures, unavailable WAL, or unchanged configuration require confirmed re-seed. `logical_decoding_work_mem` and version-available `pg_stat_replication_slots` spill/stream counters are preflighted and monitored because v0.1 rejects streamed in-progress transactions and can otherwise receive a large commit-time burst after PostgreSQL has spilled decoding state.

A single capture-priority SQLite writer serializes source commits, bounded backfill-chunk commits, destination checkpoint changes, archive intents, and metadata. Backfill chunks have byte and writer-hold-time limits so they cannot starve capture.

### 5.3 Deterministic identities and total source order

PostgreSQL `commit_lsn` and `end_lsn` have distinct fixed roles:

- `commit_lsn` orders mutations from committed WAL transactions and populates `source_version.lsn_u64`;
- `end_lsn` is the complete transaction resume/feedback boundary and the transaction uniqueness position.

`connector_event_id` identifies a stable **position**, not a decoded payload. For WAL events, it is derived only from:

```text
capture_epoch
source/slot identity
transaction end_lsn
row-change transaction_ordinal
mutation_ordinal
```

For snapshot rows, it is derived only from:

```text
capture_epoch
backfill generation
logical_table_id
chunk identity
canonical key (effective replica identity)
```

`journal_seq`, timestamps, run-local fields, relation-message timing, and every payload byte are excluded from the identity hash. The canonical payload hash is stored separately. Re-inserting an existing positional identity is an idempotent duplicate only when its stored payload hash and immutable position metadata match; a mismatch is a decoder/storage conflict and blocks capture or materialization. This makes a conflicting decode observable instead of assigning it a new identity.

Within one `(capture_epoch, logical_table_id, canonical_key)`, compare this source version lexicographically:

```text
source_version = (
  lsn_u64,              // snapshot boundary or WAL commit_lsn
  origin_rank,          // snapshot = 0, WAL = 1
  transaction_ordinal,  // snapshot = 0
  mutation_ordinal,     // snapshot = 0
  connector_event_id    // deterministic final tie-breaker
)
```

Physical duplicate rows with the same `connector_event_id` are one logical event. A conflicting payload for an existing ID is corruption and blocks. `journal_seq` is a monotonically increasing local consumption position and is never used to decide the newest source row. This is essential because a WAL event may be appended before an older snapshot row.

## 6. Event envelope and value model

The exact v0.1 physical envelope is frozen in a separate `docs/EVENT_FORMAT.md` design artifact during M0, before journal code begins. That artifact defines binary/text encodings, canonicalization, hashes, and compatibility rules. The logical envelope is:

```text
Event {
  envelope_version
  connector_event_id
  journal_seq
  capture_epoch

  source {
    system_identifier
    timeline
    database_identity
    schema
    table
    logical_table_id
    relation_id
    slot
  }

  transaction {
    xid
    commit_lsn_u64
    end_lsn_u64
    transaction_ordinal
    mutation_ordinal
  }

  operation              // snapshot | insert | update | delete | control
  control_kind           // none | heartbeat | capture_fence
  key
  before_key
  columns                 // map of column -> tagged ColumnState

  source_version {
    lsn_u64              // snapshot boundary or WAL commit_lsn
    origin_rank           // snapshot=0, WAL=1
    transaction_ordinal   // snapshot=0
    mutation_ordinal      // snapshot=0
    connector_event_id    // deterministic tie-breaker
  }

  schema {
    fingerprint
    immutable_relation_contract
  }

  snapshot {
    run_id
    generation
    chunk_id
    boundary_lsn_u64
  }

  captured_at
}
```

Each column is a tagged state, never an ambiguous nullable value:

```text
explicit_value(value)
explicit_null
unchanged_toast
absent_for_schema
```

The event-format specification defines canonical encodings for keys, booleans, signed values, arbitrary-precision numerics, floating values, timestamps and infinity, dates, UUIDs, text, `bytea`, arrays, and every other admitted v0.1 type. Unsupported values fail preflight or block capture; they are never silently coerced.

`logical_table_id` is the stable identity of one selected source table within a capture lineage. M0 freezes its derivation from immutable admitted table identity/provenance; it does not change for a compatible additive schema revision and is never derived from a relation/schema fingerprint. Correctness state is grouped by `(capture_epoch, logical_table_id, canonical_key)`. PostgreSQL relation OIDs and relation/schema fingerprints remain versioned decoding metadata, while `key_hash` is only a physical sorting/sharding aid and never a correctness identity.

`docs/EVENT_FORMAT.md` separately freezes the inputs to positional event identity, canonical payload hash, and provider-neutral benchmark hash. Stable source facts and canonical typed values are included. `captured_at`, `journal_seq`, snapshot `run_id`, process/generation timestamps, retry counts, local paths, writer metadata, and every other local or volatile field are excluded from event identity and canonical payload hashes. The specification includes golden vectors proving that retrying under a different process/run/time yields the same identity and payload hash. Internal `heartbeat` and `capture_fence` control events have explicit fixed schemas and routing; they remain in journal transaction order but never enter user-table state or provider-neutral workload hashes.

`unchanged_toast` means “reuse the newest older explicit value or explicit null for this key and column,” not null and not missing. No destination may query PostgreSQL to repair it. A canonical-key-changing update that also contains any `unchanged_toast` column is explicitly unsupported in v0.1: capture blocks before acknowledging that transaction and requires re-seed/recovery. This is narrower and safer than inventing cross-key predecessor lookup semantics. A key change with a complete new tuple still expands to old-key tombstone plus new-key upsert.

The supported type list, maximum key/row/event size, maximum transaction size, and TOAST reconstruction semantics must be approved before M1 begins.

### 6.1 Relation-schema fingerprint contract

The physical envelope stores a frozen relation-contract fingerprint rather than only column names and types. Before M1, `docs/EVENT_FORMAT.md` must define its canonical inputs and ordering:

- stable `logical_table_id` plus versioned relation identity, column identity (`attnum`), logical and physical order, name, and dropped-column status;
- type OID, typmod, and collation;
- nullability;
- canonical hashes of default, generated, and identity expressions, including an explicit no-expression value;
- primary/unique-key definition and replica-identity mode/index;
- partition-root/leaf routing, partition key/bounds where relevant; and
- publication projection/membership for the relation.

`relation_schemas` retains each immutable full contract by fingerprint. Every schema referenced by a retained event remains available. At startup and on a bounded catalog-poll interval, the connector recomputes the selected relations' contract even if no row change emits a new `Relation` message. In the streaming path, a changed `Relation` message pauses decoding synchronously and re-reads the contract before any following DML can be decoded or acknowledged. A changed contract is evaluated against the frozen additive-column policy: unsupported capture encoding blocks before feedback; destination-only incompatibility blocks that whole destination; an active snapshot generation is fenced and invalidated. Idle/no-row DDL therefore cannot remain invisible indefinitely, while immediate DDL-to-DML cannot race ahead of validation.

The canonical key is exactly the admitted effective replica identity. `before_key` and update/delete identity must contain every non-null canonical component; null, absent, partial, or `unchanged_toast` key state is unsupported and blocks before feedback.

## 7. Snapshot, backfill, and WAL stitching

Initial bootstrap and later/resumed backfills use related but distinct protocols.

### 7.1 Initial bootstrap protocol

The permanent slot and its one-use creation snapshot are created only in the M3 bootstrap vertical slice, after M2 can durably store bootstrap state.

1. Initialize SQLite and validate/create the publication and connector-owned control relations; do not independently create and abandon the permanent slot snapshot.
2. Persist a `bootstrap_intent` in state `prepared` with a proposed capture epoch, slot/publication identity, table-set fingerprint, configuration fingerprint, and `exporter_liveness=not_started` **before** issuing slot creation.
3. On a dedicated read-only SQL connection that is forbidden from writing, creating temporary objects, or otherwise acquiring an XID, begin the DDL-guard transaction, acquire `ACCESS SHARE` locks on every selected relation in frozen canonical order, and verify the finite admitted relation contracts **before slot creation or snapshot export**. Hold this guard through durable observation of the post-copy capture fence. Every contract-changing DDL operation admitted by M0 must conflict with this guard; polling/fingerprint checks alone never admit DDL.
4. With the guard already held, `boring-cdc run --bootstrap` uses the dedicated capture/bootstrap role and connector guard to create only the configured permanent logical slot with `EXPORT_SNAPSHOT` on one dedicated replication exporter connection. This is permitted only while both ownership locks and the matching persisted intent from step 2 remain valid. The runtime then keeps that exact connection command-idle; it does not start `START_REPLICATION` on the exporter.
5. While that exact exporter connection is alive, atomically persist the returned slot `consistent_point` as both the snapshot boundary and a separate server-established creation-floor field, plus the exported snapshot identifier, source identity, and `start_seq`; then transition through `snapshot_exported` to `imports_pending`. `start_seq` is the current durable complete journal transaction boundary immediately before capture starts (the explicit zero-boundary sentinel in a fresh store). The pair `(consistent_point, start_seq)` is the initial anchor's lower stitch proof, but `consistent_point` is never represented as a decoded/durable source transaction. Persist all fields before any worker copies rows.
6. Start `START_REPLICATION` at the consistent point on a **separate** CopyBoth-capable replication connection only after that SQLite transaction commits. While that snapshot generation remains eligible, later journaled transactions are not feedback-eligible until every intended importer has durably acknowledged a successful snapshot import; atomic generation invalidation releases this gate as described below. A requested status reply may use the protocol zero sentinel or repeat the verified server creation floor, but the connector never proactively acknowledges that floor or advances through later WAL before the importer gate opens.
7. Each bounded importer begins a read-only `REPEATABLE READ` transaction and executes `SET TRANSACTION SNAPSHOT` as its first statement before any catalog or data query. It verifies the already guarded relation contracts through the imported snapshot. Worker import acknowledgements include assigned ranges and become durable only after import and contract binding succeed. The exporter, importer, and guard sessions use explicit admitted session timeouts; the guard issues a bounded read-only keepalive statement often enough to remain within those limits without hiding a failed probe.
8. Once every worker import acknowledgement is durable, transition to `imports_complete` and `exporter_release_permitted`; closing the command-idle exporter is then legitimate, not generation invalidation. Worker transactions remain alive until assigned reads finish; the independently acquired DDL guard remains alive through fence observation. Every chunk binds to the immutable `snapshot_schema_fingerprint` imported by its worker.
9. Snapshot rows use the persisted slot consistent point, `origin_rank=0`, and zero transaction/mutation ordinals. WAL uses its commit LSN and `origin_rank=1`, so WAL at an equal/later source position wins.
10. After all chunks are durably complete, create the published transactional fence described in section 7.3. Its observed and journaled source-transaction boundary is the sole `post_copy_fence_lsn` proof. Release the DDL guard only after the matching fence record is durably visible in SQLite.
11. An anchor becomes complete only after all snapshot chunks, all importer/schema acknowledgements, `(consistent_point, start_seq)`, and the matching durable fence record exist. Close exporter/importer/guard transactions at their explicit lifecycle points and report snapshot/XID/lock age throughout.

`bootstrap_intents` records monotonic states including `prepared`, `snapshot_exported`, `imports_pending`, `imports_complete`, `exporter_release_permitted`, `exporter_released`, `snapshot_unusable`, `bootstrap_ambiguous_requires_restart`, `existing_slot_generation_required`, and terminal anchor/re-seed states. It separately records exporter liveness and each worker import acknowledgement; a persisted token is not treated as a reusable snapshot after its exporter has died.

There is an unavoidable response/persistence crash window around remote slot creation. On restart, an intent with a now-existing slot but no persisted snapshot token transitions to `bootstrap_ambiguous_requires_restart`; the lost creation snapshot cannot be reconstructed. If the exporter is lost before every worker import acknowledgement, atomically invalidate that snapshot generation, make all of its snapshot events permanently non-promotable, and release its importer feedback gate. Once no usable snapshot basis is pending, journal durability is again the only feedback precondition, so capture can drain retained WAL rather than wait forever for acknowledgements that can no longer arrive. Do **not** drop an unused-but-WAL-retaining slot merely to obtain a later snapshot: transient insert/delete activity between snapshots would disappear from both baselines. Recovery first proves source identity, deterministic slot provenance, publication/protocol fingerprint, and uninterrupted WAL availability. When all are provable, start capture from the existing slot, durably drain its retained WAL into the journal, establish a locally durable lower stitch boundary, and run section 7.2's existing-slot snapshot protocol in the same capture epoch. If provenance or WAL continuity is ambiguous, create a new capture epoch and perform a confirmed full re-seed with an explicit continuity break. Deterministic crash hooks cover before slot creation, after server response, token persistence, every importer acknowledgement, exporter release, first feedback, and an insert-then-delete committed between the lost and replacement snapshots.

### 7.2 Existing-slot and restart-generation protocol

A used slot cannot reproduce its creation snapshot. A resumed or explicit re-backfill therefore creates a fenced generation while capture is already active:

1. In one SQLite transaction, read the last complete locally durable source transaction from `source_state` and persist it as `(lower_stitch_lsn, lower_stitch_seq)` in the new generation. `lower_stitch_lsn` is that transaction's durable end LSN and `lower_stitch_seq` is its matching complete journal boundary; retention pins it. It is never an LSN sampled after the snapshot begins.
2. Before opening/exporting the snapshot, begin a dedicated DDL-guard transaction, acquire canonically ordered `ACCESS SHARE` locks on every selected relation, and verify the admitted contracts. Hold the locks until the matching post-copy fence is durably observed.
3. With the guard already held, open a read-only `REPEATABLE READ` exporter transaction on a dedicated SQL connection, export its snapshot, persist token/liveness/import state, and keep the exporter command-idle until every importer durably acknowledges importing that exact snapshot. Capture continues independently on the existing CopyBoth replication connection.
4. Every importer executes `SET TRANSACTION SNAPSHOT` as its first statement, verifies the guarded contract through the imported MVCC snapshot, and binds every chunk to its `snapshot_schema_fingerprint`.
5. Assign every snapshot row the already persisted `lower_stitch_lsn`, `origin_rank=0`, and zero snapshot ordinals. The exported snapshot itself does not create an LSN; it must never be stamped with an LSN sampled after export. All retained WAL events after the lower boundary win by total source version.
6. Persist half-open key ranges and completion in generation-fenced SQLite transactions while capture continues from the same slot.
7. After all chunks finish, create the published transactional fence described in section 7.3. The matching observed source transaction proves `post_copy_fence_lsn` and its complete journal boundary proves `post_copy_fence_seq`.
8. Publish a completed reconstruction anchor only when the snapshot is complete and the durable capture-fence record exists for the same capture epoch, generation, table-set fingerprint, and lower stitch pair. Release the DDL guard only after that fence record is durable.

Exporter, importer, and guard lifetimes are bounded by configured wall-clock and source-impact limits and admitted role/database timeout settings; the guard executes bounded read-only keepalives without masking lock-session probe failure. Their backend PIDs are persisted while live; after network loss/restart, reconciliation verifies those sessions are gone or explicitly terminates the exact stale owned backends before a replacement generation starts. Unexpected exporter loss **before** all import acknowledgements, any importer death before its assigned reads finish, DDL-guard loss before durable fence observation, generation cancellation, or a source-impact limit ending the generation marks **all** of that generation's chunks and snapshot events `invalid`. Normal exporter release after every import acknowledgement is durable does not invalidate the generation. The append-only journal retains invalid events only as non-replayable audit records until GC; they are excluded from anchors and live destination state. A materializer may write pending snapshot events only to an isolated candidate namespace, never to a live namespace, and cannot promote it before anchor completion. Reuse is allowed only while the lower boundary, capture epoch, schema contracts, importer assignments, DDL guard, and generation remain valid; after pause, cancellation, worker/process death, or source-impact termination, resume starts a full re-read under a new generation. Capture WAL may continue for an existing slot, but an invalid generation cannot contribute a baseline. Invalidating a generation atomically releases any importer feedback gate because no one-use snapshot basis remains eligible; subsequent feedback again depends only on durable complete source transactions. This is eventual convergence, not one globally atomic point-in-time snapshot across generations.

### 7.3 Transactional capture-fence proof

The connector-owned `boring_cdc_control.capture_fences` relation is included in the same one publication and contains exactly one administration-seeded row with an immutable key. After the last snapshot chunk commits, the coordinator first persists the intended unique nonce in `backfill_generations`, then uses the least-privileged control credential to update only that row's `(capture_epoch, generation, table_set_fingerprint, unique_nonce)` columns and requires exactly one affected row. It has no `INSERT`, `DELETE`, or key-update privilege and only key-column `SELECT`. The `pgoutput` capture loop recognizes the update and, in the **same SQLite transaction** that publishes its complete source transaction and durable source state, inserts `durable_capture_fences(capture_epoch, generation, nonce, post_copy_fence_lsn, post_copy_fence_seq)`. Anchor completion requires a durable observed nonce that matches a nonce persisted for that generation before dispatch; a crash/retry may reuse an intended nonce but cannot substitute an unrecorded one. The first durable observation of an intended nonce is the immutable anchor proof; a later source transaction repeating the same nonce is retained as audit evidence but cannot replace or create a second proof. `post_copy_fence_lsn` is the first proof transaction's source end LSN; `post_copy_fence_seq` is that transaction's complete journal sequence. Repeated generations reuse the fixed source row but have unique nonces and immutable historical proofs in SQLite.

The coordinator never treats `pg_current_wal_lsn()`, a later sampled replication `wal_end`, or a timer as a fence proof. The only accepted proof is the explicitly published fixed-row update observed through `pgoutput` and atomically paired with its journal sequence. No anchor may be complete without its initial/lower stitch pair and this matching fence nonce/LSN/sequence pair. Tests cover source transactions committed before snapshot creation, while workers hold the imported snapshot, after the snapshot read but before the fence, after the fence, repeated fence reuse, unexpected row cardinality, and unauthorized insert/delete/key-change attempts.

The DDL guard holds canonically ordered `ACCESS SHARE` locks from relation-contract verification through durable capture-fence observation. M0 must enumerate every admitted contract-changing DDL operation for every supported PostgreSQL version and prove that it conflicts with this lock; any operation that does not conflict is unsupported until a stronger guard is designed and tested. The M3 schema-guard coordinator solely polls `pg_locks` for conflicting waiters on every guarded relation. A conflicting waiter older than the bound configured/exposed by backfill controls is a source-impact limit: the schema-guard coordinator atomically invalidates the generation and snapshot eligibility, releases the importer feedback gate, cancels/closes importer/exporter/guard sessions, verifies lock release, and persists the fact projected later as `backfill_ddl_waiter`. Status exposes `backfill_ddl_waiter` with waiting PID/relation/age and the runbook states plainly that DDL attempted during backfill is blocked until the generation ends. Final fingerprint verification and bounded polling remain defense in depth. Outside backfill, any changed `Relation` message synchronously pauses decoding for contract validation before following DML can be acknowledged.

If relation metadata nevertheless changes while a generation is active, fence and end that generation; do not combine chunks read under different schemas. The only in-place additive change supported by v0.1 is a nullable, non-generated column with no default, outside an active backfill generation. For rows written under older schemas, the canonical projection maps `absent_for_schema` for such a column to SQL null. Columns with defaults, generated/identity expressions, or `NOT NULL` constraints require a fresh destination generation and re-backfill. Tests place DDL immediately before/after chunk-copy and capture-fence boundaries and DDL immediately before DML.

### 7.4 Chunk planning

Before M1, freeze supported canonical effective-replica-identity scalar types and composite-key arity. Backfill uses that same identity for persisted keyset-pagination ranges with deterministic canonical ordering.

- Ranges are half-open.
- `OFFSET` and `ctid` chunking are prohibited.
- Keyset pagination requires a frozen supported, non-null, canonically comparable identity in the imported snapshot.
- A canonical-key change is allowed only when `pgoutput` supplies complete old and new effective-replica identities so it can become an old-key tombstone plus new-key upsert; a key change with **any** unchanged TOAST remains blocked.
- Chunk row, byte, duration, and writer-hold-time limits are configurable.
- Chunk completion and snapshot-event insertion are atomic.
- Every worker completion compares its generation token before commit.
- An old worker cannot commit after pause, resume, restart, or re-seed.

### 7.5 Source-impact controls

Monitor and limit:

- snapshot/exporter age;
- backend `xmin` and dead-tuple growth where measurable;
- source CPU, I/O, connections, read latency, and application p50/p95/p99 latency;
- WAL production and retained-WAL growth;
- chunk throughput, remaining ranges, progress, and ETA; and
- conflicting `pg_locks` waiters on guarded relations, including waiter PID, lock mode, relation, and wait age.

At configured source-impact limits, throttle workers and then end the generation rather than leave an exported snapshot paused indefinitely. A conflicting DDL waiter beyond its shorter configured bound immediately invalidates the generation and releases the DDL guard so PostgreSQL lock-queue fairness cannot turn a long backfill into an application outage.

### 7.6 Re-backfill and re-seed generations

A snapshot contains present rows but cannot emit tombstones for rows deleted before its boundary. Therefore an incompatible-schema re-backfill, slot-loss recovery, or full re-seed never merges an incomplete baseline into stale destination state.

- A re-seed creates a new `capture_epoch`.
- ClickHouse writes generation-tagged rows into its pre-created append-only history objects, verifies the complete candidate generation, then publishes a greater fence in the append-only selector. Canonical queries resolve the live generation from selector data at query time; no DDL participates in promotion. Old generation partitions retire only after the selector publication and grace.
- Archive re-seed writes a new dataset root/generation and records an explicit continuity break.
- Because one destination has one checkpoint, any incompatible-schema rebuild creates one complete fresh destination/table-set candidate from a compatible anchor; table-only candidate promotion is forbidden.
- Adding a selected table is a full re-seed: create a new capture epoch, recreate the publication and slot for the complete expanded set, snapshot **all** selected tables, and build fresh full ClickHouse and archive generations. The old complete generations stay readable until the new full generations are verified and promoted; a candidate containing only the new table is forbidden because it would drop existing-table state at switch time.

Recovery tests include a row deleted before re-seed and prove it is absent after the destination-generation switch. The table-add test also proves existing tables remain complete across the full-generation promotion.

## 8. SQLite journal and metadata model

The requirement for an “append-only event journal for payloads and SQLite for metadata/checkpoints” is implemented as one physical SQLite database. `journal_events` is logically append-only: normal operation only inserts, and retention removes only whole eligible transaction ranges through the audited GC path. There is no separate append-only payload file. This single-store choice makes event publication and source-checkpoint advancement one SQLite transaction while preserving the requirement's logical separation.

Minimum logical tables:

- `journal_events`
- `source_transactions`
- `source_state`
- `runtime_ownership`
- `operator_command_requests`
- `relation_schemas`
- `destinations`
- `destination_checkpoints`
- `backfill_runs`
- `backfill_generations`
- `backfill_chunks`
- `bootstrap_intents`
- `bootstrap_imports`
- `durable_capture_fences`
- `bootstrap_anchors`
- `reseed_intents`
- `destination_generation_leases`
- `destination_promotion_intents`
- `clickhouse_batch_intents`
- `archive_generations`
- `archive_segment_intents`
- `archive_segments`
- `archive_generation_markers`
- `processing_failures`
- `destination_audits`
- `condition_hysteresis`
- `alerts`
- `schema_migrations`

`bootstrap_anchors` contains at least:

```text
anchor_id
capture_epoch
generation
lower_stitch_lsn
start_seq
snapshot_boundary_lsn
snapshot_complete_seq
post_copy_fence_nonce
post_copy_fence_lsn
post_copy_fence_seq
table_set_fingerprint
snapshot_schema_fingerprints
state
expires_at
```

A usable anchor represents a complete baseline plus every required concurrent journal event from its lower complete boundary through a post-copy durable capture fence. For initial bootstrap, `(snapshot_boundary_lsn, start_seq)` is the slot `(consistent_point, current complete journal boundary)` persisted before copying; for an existing slot, `(lower_stitch_lsn, start_seq)` is the last locally durable complete source boundary persisted before exporting a snapshot. `post_copy_fence_seq` is the complete journal position atomically paired with the published control-row fence's `post_copy_fence_lsn` and nonce. Anchor state can transition to `complete` only when all valid snapshot chunks, schema bindings/import acknowledgements, the lower proof, and that matching durable capture-fence record exist. Retention pins `start_seq`. An arbitrary retained sequence, an invalidated generation, or snapshot completion without the fence is never advertised as a valid empty-destination bootstrap point.

Important constraints:

- unique `(capture_epoch, source/slot identity, transaction end LSN)`;
- unique positional connector event ID; an existing identity with a different stored payload hash is corruption, not a second event;
- monotonic journal sequence;
- source durable LSN changes in the same transaction as its event publication;
- bootstrap intent/import transitions are monotonic and make exporter loss or ambiguous remote slot creation visible;
- durable capture fences and heartbeat control events reference a capture epoch, source transaction, and complete journal sequence; materializers route them as internal transaction-preserving no-ops;
- destination checkpoint references a complete committed journal transaction boundary plus `capture_epoch`, anchor identity where applicable, configuration fingerprint, generation, and nullable `current_failure_id`; retry schedules/reasons exist only in `processing_failures`;
- runtime ownership records the `run_id`, dedicated advisory-lock backend PID/nonce, monotonic ownership deadline, loss state, and takeover/reconciliation evidence;
- operator command request IDs are unique per server-issued dry-run nonce; the persisted nonce, canonical payload/digest, run ID, state, and result make one dry-run's lost-response retries idempotent, while same-ID/different-payload reuse blocks and every nonterminal prior-run request becomes `aborted_by_restart` after remote-intent reconciliation;
- destination lease and promotion intent transitions are compare-and-swap fenced and externally reconcilable; each destination persists its locally allocated and maintenance-adopted `highest_external_fence`, allocates only a strictly greater fence, and cannot reuse a fence for different state;
- backfill chunk completion and snapshot-event insertion are atomic and generation-fenced; invalid generations cannot become anchors or live state;
- archive segment intent is immutable once selected;
- immutable transaction and event payload checksums;
- immutable relation schemas by fingerprint; and
- versioned migrations with clean-install and upgrade tests.

Represent LSNs as ordered unsigned eight-byte values or canonical hexadecimal strings. Do not rely on signed SQLite arithmetic for every PostgreSQL LSN.

### 8.1 Integrity and startup validation

At startup and at documented maintenance intervals:

- run appropriate SQLite `quick_check`, with full `integrity_check` available for recovery/maintenance;
- validate journal sequence continuity at committed transaction boundaries;
- validate event counts and transaction/event checksums;
- validate source and destination checkpoints against retained complete boundaries;
- reconcile nonterminal operator requests to `aborted_by_restart` without re-execution and reconcile every external selector before allocating a promotion fence;
- validate every retained event's schema fingerprint;
- validate archive intent/manifest/`SEGMENT_READY`/generation-live-marker/checkpoint relationships;
- reconcile persisted processing-failure classification/backoff state before any component retries; and
- block on missing, corrupt, or contradictory state.

Destination integrity is checked independently of checkpoint advancement through incremental audits. At startup, periodically, and on `destination verify`, each audit resumes a persisted cursor toward the current complete checkpoint and stops at frozen per-pass byte, event, and wall-time budgets. ClickHouse compares retained journal summaries with batch markers/event identities/payload hashes and fingerprints correctness-critical tables, columns, engines, canonical queries/views, selector schema, and settings. It persists `journal_verified_range`, cursor, freshness-window coverage, evidence digest, and first mismatch. Archive persists two independent coverages: `journal_verified_range` for retained-journal-to-manifest comparison and `self_consistent_range` for selector/manifest/`SEGMENT_READY`/file-hash consistency, each with cursor, budget accounting, and freshness. Coverage is the union of completed passes still inside the frozen freshness window; gaps or older history are explicitly `unverifiable_before_seq`, never silently verified. Audits never read PostgreSQL or mutate a live checkpoint. A mismatch creates the affected destination's `processing_failures` record with reason `integrity_mismatch`; only that destination blocks and recovery requires a fresh verified generation from a compatible anchor, or re-seed when history is unavailable.

Journal corruption or a missing required replay sequence transitions to `journal_corrupt_requires_reseed`. If state cannot be persisted, fail health and exit non-zero.

### 8.2 Admission control and physical storage management

All limits use explicit byte units:

- `wire_frame_bytes` is the length announced by the replication protocol before a frame is allocated;
- `decoded_row_bytes` is the canonical decoded row/event representation before compression or SQLite-page overhead;
- `max_event_bytes` bounds one canonical event, and `max_transaction_bytes` bounds the sum of canonical event bytes plus fixed transaction/envelope headers;
- `on_disk_reservation_bytes` is a conservative allocation reservation, never a compressed-file observation after the fact.

Frame length and field lengths are checked before memory or spool allocation. `max_transaction_bytes` and `max_transaction_events` apply to the canonical bounded transaction spool; a separate frozen `max_wire_frame_bytes` prevents an oversized wire frame from bypassing that bound. Capture memory admission additionally enforces:

```text
required_runtime_memory =
  fixed_runtime_overhead_budget
  + max_copyboth_receive_buffer_bytes
  + max_decoder_queue_bytes
  + max_in_memory_event_staging_bytes
  + max_sqlite_writer_staging_bytes
  + backfill_worker_count * max_backfill_worker_memory_bytes
  + sum(max_destination_worker_memory_bytes)
  + telemetry_and_command_headroom_bytes
```

`boring-cdc check` compares this conservative total with the configured process/cgroup memory limit, and `run` samples peak RSS against the profile without treating RSS as admission authority. A commit-time burst is consumed frame-by-frame under backpressure into the disk spool; neither the transport, decoder, channel, nor retry wrapper may aggregate the whole transaction in memory.

Every physical filesystem used by state, SQLite temporary files, spool files, archive staging, or archive output has its own configured budget and emergency reserve. The state and archive roots may share a filesystem only when their combined reservation is validated. `boring-cdc check` rejects a configuration when the worst-case reservation cannot fit on **each** filesystem:

```text
required_free(fs) =
  reserved_free_bytes(fs)
  + max_capture_spool_reservation(fs)
  + max_sqlite_main_and_wal_growth(fs)
  + max_sqlite_temp_reservation(fs)
  + backfill_worker_count * max_backfill_chunk_reservation(fs)
  + max_concurrent_archive_segment_reservation(fs)
  + archive_publish_and_directory_overhead(fs)
  + fixed_metadata_and_rounding_headroom(fs)
```

The calculation includes SQLite main, `-wal`, `-shm`, configured SQLite temporary files, temporary source-transaction spools (including orphans), temporary archive directories, final archive output, and metadata/rounding overhead. A separately mounted archive root must pass its own calculation; it is not excused because it is outside the SQLite filesystem. At startup and before each bounded allocation, the runtime reserves its declared share and refuses work before the emergency reserve is crossed. It releases a reservation only after the related cleanup is durably complete.

Thresholds are monotonic for free space: `warning_free_bytes > action_free_bytes > critical_free_bytes > hard_free_bytes >= reserved_free_bytes`; entering a lower state never enables work that a higher state prohibited. Near-limit and separate-filesystem tests prove that no spool, SQLite growth, backfill chunk, or archive publication can consume the reserve.

Before creating any table, set `auto_vacuum=INCREMENTAL` and read it back after database initialization. Configure bounded WAL checkpoints, page reuse, and `incremental_vacuum` work limits so maintenance cannot starve capture. Automatic full `VACUUM` is prohibited in v0.1; any offline full-vacuum operator procedure is outside normal runtime and requires a stopped, backed-up store. GC removes only whole contiguous ranges ending at committed source-transaction boundaries; it never splits a transaction. Repeated crashes before source-transaction publication must not leak spool space: exclusive-lock startup reconciliation removes safely classified orphans, syncs the spool directory, and blocks on anything ambiguous.

### 8.3 SQLite durability boundary

The durable-before-feedback claim is valid only under a frozen storage contract:

- SQLite `journal_mode=WAL`;
- `synchronous=FULL` applied and read back on **every** connection that can write or read durability-bearing source/journal/checkpoint state, including pooled/reopened maintenance connections;
- `auto_vacuum=INCREMENTAL` set before schema creation, bounded checkpoints/incremental vacuum, and no automatic full `VACUUM`;
- controlled checkpoints that never weaken committed WAL durability;
- a named, tested Linux filesystem/storage stack (initial candidates: ext4 and XFS, each accepted only after crash evidence) with working advisory locks, atomic sector writes as required by SQLite, and honest `fsync`/barrier semantics;
- directory sync for initial database creation, migrations, and durable file lifecycle where required; and
- no NFS, SMB, FUSE/object-store mounts, multi-host access, or `:memory:` database.

`boring-cdc init` applies persistent SQLite settings before schema creation and reads them back on every writable connection class. `boring-cdc check` opens the real store read-only and validates persisted PRAGMAs without applying or changing them. Any filesystem durability/rename/fsync experiment uses a disposable probe under the configured filesystem—not the real state/archive store—and cleans it only after sync/reconciliation. It identifies the mounted filesystem against the M0 allowlist and rejects unknown/unsupported classes. Filesystem API compatibility alone is not a support claim. Weaker settings are available only behind an explicit `unsafe_local_experiment` switch; status is permanently `unsafe_durability`, PostgreSQL feedback remains opt-in, and no durability benchmark/result may be presented as passing.

The v0.1 crash guarantee covers process/container crashes and abrupt host restart when the tested storage stack honors successful sync calls. It does not claim survival of media/controller failure, filesystem corruption, or hardware that lies about flushes. Release evidence includes clean recovery after abrupt VM/host termination on each documented supported filesystem; fault injection alone is not labelled a power-loss test.

## 9. Destination semantics

### 9.1 Shared checkpoint rules

Each destination stores independent:

- configuration fingerprint;
- `capture_epoch`, compatible complete anchor identity for every promotable generation, and generation token;
- checkpoint at a complete journal transaction boundary;
- nullable `current_failure_id` referencing the sole retry/block record in `processing_failures`;
- lag and retained bytes pinned; and
- last successful side-effect identity and its externally discoverable batch/segment intent.

Capture and the other destination may continue while one destination is blocked, within configured local and source bounds.

Destination attempts project the shared `FailurePolicy` from their referenced `processing_failures` row. `Transient` persists attempt count, first/last failure, capped backoff/jitter, `next_retry_at`, and one immutable batch/segment intent; restart preserves that schedule. Deterministic event/schema/configuration failures, payload conflicts, integrity mismatches, permission/contract drift, and `Exhausted` enter `destination_blocked` without hot-looping. Resume is explicit after the operator fixes the fingerprinted cause; the owner revalidates configuration, destination contract, anchor/generation, audit evidence, and the unchanged failed journal boundary before clearing the block. No class may skip the failed transaction or advance its checkpoint, and one destination's poison event cannot stop the other destination or capture while retention/headroom remain safe.

If a destination cannot apply an otherwise losslessly captured schema/event, it blocks **the whole destination immediately before that journal transaction**. Its checkpoint cannot skip the event. Per-table quarantine with independent progress would require separate durable checkpoints and is out of scope for v0.1.

### 9.2 External side effects, leases, and promotion

Every ClickHouse batch and archive segment targets an immutable namespace named by `(destination, capture_epoch, generation)`. A worker holds a `destination_generation_lease` for that exact tuple and rechecks it immediately before the side effect. A stale worker may leave an idempotently discoverable artifact only in its fenced old namespace; it cannot write into a later candidate or live namespace.

A promotion is a durable state machine, not merely a checkpoint CAS. Each destination has a strictly increasing `promotion_fence` allocated durably before external switching. `destination_promotion_intents` records the old live tuple, candidate tuple, capture epoch, compatible anchor, configuration fingerprint, promotion fence, expected external selector state, and one of:

```text
prepared -> old_leases_fenced -> candidate_verified -> switch_pending
  -> switched -> verified -> retirement_eligible -> retired
```

1. Persist `prepared` with the next destination-scoped fence, acquire the candidate lease, and fence/drain old leases before a pause, detach, re-seed, or promotion can proceed.
2. Materialize and verify the candidate generation against its anchor/fence and correctness watermark.
3. Persist `switch_pending`, then append `(destination_id, promotion_fence, candidate_tuple, intent_id)` to the destination selector and read it back. Duplicate identical rows for the same tuple are harmless under retry. Canonical resolution chooses the greatest fence and requires exactly one distinct `(candidate_tuple, intent_id)` at that fence; same-fence/different-state is selector corruption and blocks. A delayed lower fence remains stored but can never become live. Per-segment durability markers are never promotion markers.
4. Persist `switched` only when query-time selector resolution exactly matches the intent, then mark `verified` and `retirement_eligible` after the configured grace. The only `retired` transition and physical deletion are executed later by the lock-owning maintenance command with the ClickHouse maintenance role; ordinary `run` cannot retire data.
5. On restart, reconcile every nonterminal intent and the externally highest valid fence before any worker starts. If external state is ahead of restored SQLite state, transition to `promotion_recovery_required`; never allocate a lower fence or overwrite/delete an external selector row. A state-store checkpoint alone never decides which external generation is live.

ClickHouse promotion is explicitly data-driven. A pre-created append-only selector table stores immutable fence rows, and every canonical current-state query resolves the live generation at query time from the greatest valid fence, failing when that fence names more than one distinct candidate/intent. No `CREATE OR REPLACE VIEW`, `EXCHANGE TABLES`, read-then-DDL sequence, or other DDL participates in switching. Candidate isolation is the `generation` key/partition within pre-created append-only history objects, not a runtime-created table. Archive promotion publishes immutable fence-named selector markers, and readers select the valid marker with the greatest fence, so a delayed lower-fence publication cannot move visibility backwards. A capture-epoch change invalidates all old destination generations for continuation: they require re-seed into the new epoch and cannot advance an old checkpoint. Archive and ClickHouse generation namespaces make a crash after a side effect but before SQLite checkpoint/promotion recovery explicit and replayable. Promotion tests cover every transition, a delayed stale switch completing after a newer promotion, same-fence conflict, external-ahead-of-SQLite restore, and stale writes.

### 9.3 ClickHouse materializer

The ClickHouse destination stores append-only event history containing explicit key, total source version, connector event ID, payload hash, schema fingerprint, tagged column states, and tombstone operation. M0 freezes a concrete `docs/CLICKHOUSE_MODEL.md` contract with executable event-history DDL, canonical queries, pinned server/client image versions and digests, exact storage engine/table settings, and an executable insert-recovery protocol before M4 implementation starts.

The ClickHouse contract must state the acknowledgement boundary precisely. v0.1 disables `async_insert` for materializer batches and rejects any server/client setting that acknowledges only an asynchronous insert queue. It freezes the supported synchronous insert and metadata-`fsync`/write-concern equivalents (including `fsync_after_insert` and directory/part durability settings when supported by the pinned version), volume/filesystem assumptions, batch marker schema, and one-node Compose limitation. It also pins the Rust client/crate version and requires its documented insert-finalization call (for example, completing/finalizing the insert stream and awaiting the server result); dropping a request/stream without successful finalization can never advance a checkpoint. There is no replication quorum or HA guarantee in the single-node Compose environment. A response is called durable only under that frozen contract; a crash-after-ack integration test kills and restarts ClickHouse, then verifies every batch marker/event identity before its checkpoint can be trusted. If recovery cannot verify an accepted batch, it replays it while retained or blocks rather than claiming durability.

The raw history partition/sort contract includes `generation` before `(capture_epoch, logical_table_id, key_hash)` and then the full source version and `connector_event_id`; the exact typed canonical-key columns are specified per source table. `key_hash` is only a physical sort/sharding aid; canonical correctness groups by `(capture_epoch, logical_table_id, canonical_key)`. Each batch has a persisted `clickhouse_batch_intent` with epoch, generation, journal range, event-ID/payload hash summary, and target namespace before insertion. A write-before-checkpoint retry may create physical duplicate rows. Canonical queries first collapse rows by the positional `connector_event_id` and reject any same-ID payload-hash conflict, then compare the full source-version tuple including the event-ID tie-breaker. Correctness never depends on background merge timing.

A naive latest-row `ReplacingMergeTree` is insufficient because an `unchanged_toast` patch may need an older explicit value that a merge discarded. The composite `source_version` must not be narrowed/coerced into a native `ReplacingMergeTree` version column, and no TTL, mutation, merge, or cleanup may physically remove protective tombstones or predecessor values required by reconstruction. The canonical current-state layer therefore:

1. selects row liveness from the latest operation by total source version;
2. for each live column, selects the latest `explicit_value` or `explicit_null` at or before that row version;
3. treats `unchanged_toast` as a patch that preserves the prior explicit state;
4. treats `absent_for_schema` as not-yet-defined, mapping it to null only for the explicitly supported nullable/no-default additive-column case; and
5. applies complete canonical-key changes as old-key tombstone plus new-key upsert, while blocking key changes containing any `unchanged_toast` before source acknowledgement.

A table reports `backfill_incomplete` until its baseline anchor is complete. The default documented current-state query/view must be correct without asking users to guess merge timing. Raw table behavior, background merges, canonical view cost, and `FINAL` behavior are documented and benchmarked before any optional compaction design.

ClickHouse requirements:

- deterministic retries and replay;
- explicit ordering/sort key and source version;
- versioned tombstones for hard deletes;
- only the frozen nullable/no-default additive-column projection;
- destination-wide block before incompatible schema transactions;
- one complete fresh destination/table-set generation from a compatible anchor and query-time resolution through an append-only greatest-fence selector for incompatible rebuild/re-seed; no table-only promotion, runtime DDL, or unconditional view replacement;
- checkpoint advancement only after the configured ClickHouse durable-acceptance contract and batch intent are satisfied; and
- tests before, during, and after merges under update/delete-heavy load.

ClickHouse raw history is deliberately append-only in v0.1 because it preserves the predecessor values required for TOAST patches. v0.1 does not claim unlimited sink history or automatic safe compaction. `docs/CLICKHOUSE_MODEL.md` sets a tested history quota, alert/action/block behavior, and a retired-generation grace period. Reaching the quota blocks the whole ClickHouse destination before its next journal transaction, pins local retention, and therefore surfaces normal source-safety pressure; it never silently drops raw history. Old generation partitions/data may be retired only by the short-lived ClickHouse maintenance role after selector verification and the configured grace; ordinary `run` marks them retirement-eligible but cannot execute DROP, mutation, or shared-object DDL. Published capacity results include raw-history growth, quota behavior, and retained-generation cost.

No destination may query PostgreSQL to reconstruct TOAST values.

### 9.4 JSONL/Parquet archive materializer

The archive is a changelog with documented reconstruction by capture epoch, source version, tagged column state, and tombstone. It is not silently presented as current-state snapshots. Snapshot events from an incomplete or invalid generation are never visible through the live archive generation. A fenced candidate generation may contain individually durable segments, but only generation-level promotion can make them reader-visible.

v0.1 durable publication is scoped to named, tested local Linux filesystems with same-filesystem atomic **directory** rename and file/directory `fsync`. M0 starts with ext4 and XFS candidates and admits each only after the crash matrix passes on the pinned Linux/storage configuration. `boring-cdc check` identifies the filesystem and fails on anything outside that allowlist. Object stores, network mounts, FUSE, and support inferred from API compatibility alone are out of scope.

Archive path components are derived only from frozen ASCII encodings of opaque logical table IDs, schema/configuration hashes, capture epochs, generation/fence values, and journal ranges. Raw or sanitized database, schema, table, and column names are forbidden in paths; canonical display-name-to-opaque-ID mappings live in hash-covered manifests. All filesystem operations are descriptor-relative beneath an already-opened archive root and use no-follow/exclusive creation semantics. Preflight and recovery reject symlinks, non-directory intermediates, path/type swaps, unexpected hard-link counts, and owner/mode mismatches. Hostile quoted identifiers, Unicode normalization collisions, slash/dot segments, and concurrent symlink/type-swap attempts are required fixtures.

For every scheduled range:

1. Select a range ending on a complete committed journal transaction and persist an immutable segment intent containing capture epoch, archive generation, compatible anchor where applicable, `start_seq`, `end_seq`, enabled format set, writer configuration hash, and state.
2. Retry exactly that intent range after failure; never silently widen it because newer events arrived.
3. Create one deterministic, same-parent temporary directory for the segment. It contains every JSONL/Parquet part and a pending manifest; no reader trusts a temporary directory.
4. Serialize events in canonical `(journal_seq, transaction_ordinal, mutation_ordinal, connector_event_id)` order. JSONL field order, Parquet column order, compression, row-group limits, `created_by` value, and all metadata are frozen so a retry produces the same hashes without volatile timestamps/UUIDs. Parquet remains grouped by source table and schema fingerprint; heterogeneous schemas never share a file.
5. Write every part and pending manifest, `fsync` each file, then `fsync` the temporary directory. The manifest records part paths/hashes/sizes, journal range, event count, schema fingerprints, format versions, and the intent/configuration identity.
6. Atomically rename the **single fully staged directory** to its deterministic final directory and `fsync` its parent. The protocol does not claim atomic publication of unrelated files; directory rename on a named tested filesystem is the only set-publication primitive used here.
7. Write a final internal `SEGMENT_READY` marker containing the intent ID and manifest hash, `fsync` it and the final directory, then `fsync` the parent again. This proves segment durability only; it does **not** make a candidate generation live.
8. Persist the durable manifest/segment record and advance the candidate generation's shared archive checkpoint only after every enabled artifact and `SEGMENT_READY` publication is durable.

A crash after any file, manifest, directory-rename, parent-sync, or `SEGMENT_READY` phase is reconciled against the immutable intent. A deterministic final directory that exists without `SEGMENT_READY` is **not** ignored: recovery validates every file and the pending manifest against the intent, re-syncs files/directory as required, writes and syncs the marker, and adopts it; any mismatch is quarantined and blocks. Temporary directories are similarly validated or quarantined. Existing output is never overwritten blindly.

Archive visibility is generation-level. The promotion state machine verifies the complete candidate generation against its anchor/watermark, persists `switch_pending` with a destination-scoped promotion fence, publishes an immutable fence-named selector marker using the tested rename-and-fsync contract, reads it back, and only then records `switched`. Readers choose the valid selector marker with the greatest fence and then accept only matching `SEGMENT_READY` segments/manifests in that generation. A delayed lower-fence marker is harmless; same-fence/different-state is corruption. Candidate generations can have segment-ready data but are invisible before promotion. With roots/directories at mode `0700`, v0.1 archive readers run as the same connector Unix account; broader user/group sharing and ACL design are out of scope.

If JSONL and Parquet are both enabled, they share one archive-generation checkpoint and it advances only after both artifact sets are segment-ready. Changing enabled formats or writer configuration creates a new archive generation. To preserve continuity, that generation must replay from a compatible retained anchor/range and be verified/promoted under section 9.2. If no compatible history exists, it cannot enter the existing destination's selector lineage or promotion path. An explicit confirmed continuity break instead creates a **new archive destination/root identity** at a complete journal-transaction boundary, writes a durable discontinuity record declaring that pre-boundary history is absent, and starts a separate selector lineage; it never promotes a suffix-only candidate as though it were a complete replacement. Archive output has a per-filesystem quota/budget; no automatic deletion is allowed to make a failed archive look caught up.

M0 pins the Parquet Rust writer/crate and version, format version, schema mapping, compression codec/level/library, row-group and page sizing, dictionary/statistics behavior, field/metadata order, `created_by`, and all optional metadata. Byte-identical retry is scoped to the same pinned release container/toolchain, architecture, and writer configuration. Golden fixtures run under a fresh process/run/time inside that scope and require identical Parquet parts, JSONL parts, manifests, and hashes for the same immutable segment intent.

## 10. Reconstruction anchors, replay, and retention

### 10.1 Bootstrap rules

Every generation that may become live in an existing destination lineage—including a new attached destination intended to claim complete reconstruction, replay/rebuild candidate, or incompatible-schema/re-seed candidate—must bootstrap from a retained, complete, compatible reconstruction anchor whose capture epoch and full table-set/schema contracts cover that destination. Suffix-only replay is permitted only as non-promotable diagnostics. The sole exception is section 9.4's explicitly confirmed continuity break: it creates a new archive destination/root identity and selector lineage with a durable first-boundary discontinuity record and makes no completeness claim for earlier history; it is never promoted into or substituted for the old lineage.

A completed anchor pins its `start_seq` while configured as usable. The destination consumes from that lower complete boundary through `snapshot_complete_seq`, `post_copy_fence_seq`, and onward, using total source version to resolve WAL events that were journaled before delayed snapshot rows. An anchor without the persisted initial/last-durable lower stitch proof, worker/schema acknowledgements, and matching published-fence nonce/source-LSN/journal-sequence pair is incomplete and cannot bootstrap anything.

Snapshot events carry their generation eligibility. Until the matching anchor is complete, a live destination ignores them; a candidate destination may consume them only in its isolated epoch/generation namespace. Normal exporter release after all import acknowledgements are durable is valid. Unexpected pre-import exporter loss, an importer dying before assigned reads finish, DDL-guard loss before durable fence observation, cancellation, or any other generation invalidation makes every snapshot event permanently ineligible for anchor/live-state use. This prevents completed chunks from an abandoned generation from leaking a stale baseline through later replay or promotion.

If no complete compatible anchor remains, destination addition fails with explicit guidance to run a shared journaled re-backfill or full re-seed. It never starts from the oldest arbitrary retained event.

### 10.2 Replay rules

A replay start resolves to a retained complete source-transaction boundary. Every replay candidate intended for promotion must additionally resolve to a compatible complete reconstruction anchor in the same capture epoch and reconstruct the full destination table set from that anchor. Suffix-only replay may populate a clearly diagnostic, non-promotable namespace. Replaying an established destination never rewrites its live checkpoint. The dry run selects an explicit new destination generation and reports its epoch/anchor/range/storage consequences; execution requires that same generation plus a confirmation bound to the source/epoch/table-set/generation fingerprints. The new generation is verified and promoted through section 9.2. No command mutates the currently live checkpoint in place.

### 10.3 Retention and pressure transitions

Retention preserves all events required by:

- each attached destination checkpoint;
- each active backfill generation stitch boundary;
- every retained usable reconstruction anchor; and
- the configured post-consumption replay window while disk permits.

A paused destination remains attached and pins retention. Only explicit detach, with displayed consequences and confirmation, releases its pin.

Pressure states:

| State | Required behavior |
|---|---|
| Warning | Surface forecast/headroom; throttle backfill |
| Action | End snapshot generations safely; continue capture, materializers, and automatic audited pin-safe eligible GC |
| Critical | Reserve space for metadata/source commits and draining consumers; stop new archive/backfill work |
| Hard | Safe-stop capture before `ENOSPC`; continue consumers that can drain without violating reserve |

If a pinned destination prevents reclamation, status identifies it and forecasts source-WAL consequences. Runtime GC automatically removes only audited, pin-safe, complete transaction ranges in bounded batches; `journal gc --dry-run` is inspection-only and cannot be the mechanism required for normal safety. Alert/audit rows, completed intents, invalid snapshot generations, retired-generation metadata, and orphan diagnostics each have explicit retention bounds and bounded GC batch-duration limits, without deleting evidence needed by a nonterminal workflow. The system escalates retained-WAL risk honestly; a finite PostgreSQL WAL cap may ultimately invalidate the slot and require re-seed. It never detaches a destination, deletes unconsumed events, drops a slot, or sends fake feedback automatically.

## 11. Source-safety state machine and observability

Monitor:

- retained WAL bytes and slot lag;
- `wal_status`, `safe_wal_size`, slot activity, invalidation, server WAL end, `logical_decoding_work_mem`, and version-available logical-decoding spill/stream transaction/count/byte counters;
- source free disk or explicit `unknown` capability;
- source CPU, I/O, connections, application latency, and WAL rate in Compose/benchmarks;
- per-filesystem reservation, free space, and actual state/archive usage including all files listed in section 8.2;
- ClickHouse raw-history quota/usage, filesystem free space, temporary parts, merge backlog/amplification, insert quota, archive quota/usage, retired-generation grace, and destination-side pressure;
- oldest/newest retained sequence, usable anchors, and replay duration;
- last received, durable, and feedback LSN plus last heartbeat attempt, published heartbeat end LSN, success/failure, and idle-WAL headroom;
- capture and per-destination lag/retries/pinned bytes, generation lease, highest local/external promotion fence, and promotion-intent state;
- processing-failure class/fingerprint, first/last failure, attempts, next retry, failed XID/final LSN or journal range, observed transaction/spool bytes/events versus limit, automatic-retry armed state, and resulting WAL headroom;
- per-destination audit status, last success/attempt, incremental cursor, per-pass byte/event/time consumption, freshness-window coverage, ClickHouse `journal_verified_range`, archive `journal_verified_range` plus `self_consistent_range`, contract fingerprint, mismatch evidence, and explicitly unverifiable gaps/older history;
- dedicated source-lock backend PID/nonce, last successful probe, monotonic ownership deadline, takeover interval, and ownership-loss/reconciliation state;
- operator-command endpoint availability plus request/retry/result counts without command payloads or secrets;
- snapshot age, backend `xmin`, backfill rate/progress/ETA; and
- integrity-check status and last successful time.

Required states:

```text
healthy
degraded
backfill_throttled
backfill_ddl_waiter
backfill_incomplete
destination_blocked
schema_blocked
capture_safe_stopped
slot_invalid_requires_reseed
journal_corrupt_requires_reseed
publication_drift_requires_reseed
bootstrap_ambiguous_requires_restart
bootstrap_snapshot_unusable
reseed_incomplete
heartbeat_degraded
promotion_recovery_required
clickhouse_durability_unverified
source_identity_blocked
ownership_lost
unsafe_durability
```

Status derives `overall_health` plus a concurrent `conditions[]` set from domain facts in `processing_failures`, `destination_audits`, source/bootstrap/promotion state, and pressure gauges rather than driving an independent safety transition machine. `condition_hysteresis` persists only enter/exit timestamps needed for stable projection. External condition names remain frozen. Every condition also carries a stable versioned `runbook_id` resolved through the checked-in runbook registry, plus a safe copy-pasteable next command when one exists. Conditions have severity precedence, observed-at/fresh-until timestamps, evidence, automated action, allowed remaining actions, and exact recovery path. `overall_health` is the maximum active fresh severity; stale required evidence contributes an explicit unknown/degraded condition rather than disappearing.

`boring-cdc status --json` renders these facts as one versioned `SystemSnapshot` rather than forcing an agent to join unrelated command outputs. It contains `snapshot_id`, monotonically changing `state_revision`, `observed_at`, `fresh_until`, source/capture/run/ownership identity, configuration/table-set fingerprints, durable/feedback boundaries, journal/filesystem budgets, destinations/checkpoints/generations/audit coverage, active conditions/evidence/runbooks, `blocked_by`, `allowed_actions`, safe next-command templates, and an evidence digest. Every field identifies its domain owner and freshness. The snapshot is read-only derived state, not another transition machine, and stale or unavailable required facts remain explicit.

A persisted runtime action result links the before-snapshot, action-plan digest, immutable intent/request IDs, external-effect evidence, after-snapshot or terminal state revision, asserted postconditions, and continuity consequence. This gives operators and agents one causal chain from observation through action to proof without trusting log chronology or prose summaries.

Source-WAL headroom combines the configured slot-retention cap, current source free space where measurable, retained bytes, observed WAL production rate, monitoring delay, and a reaction reserve. M0 freezes the conservative equation and warning/action/critical horizons. Missing required metrics block new bootstrap/backfill unless explicitly overridden; they do not automatically stop otherwise healthy capture, because disconnecting can increase retained WAL.

`promotion_recovery_required` has one bounded exit after local state loss: the lock-owning maintenance command adopts the externally greatest valid fence only after verifying its selector uniqueness and target generation, then persists that fence before any new allocation. Missing, conflicting, or unverifiable external state remains blocked and requires operator repair or a new explicit destination identity.

“Safe stop” means the connector stops accepting source work before violating its local durability contract. It cannot preserve PostgreSQL WAL indefinitely. The finite source WAL cap remains the final source-safety boundary and may force an explicit re-seed.

## 12. CLI and operator workflows

### 12.1 Commands

```text
boring-cdc check
boring-cdc init
boring-cdc run
boring-cdc run --bootstrap
boring-cdc status [--json]

boring-cdc backfill start|pause|resume|status
boring-cdc backfill restart --confirm

boring-cdc destination list|add|pause|resume
boring-cdc destination add NEW_DESTINATION --archive-root PATH --continuity-break --from-seq SEQ --dry-run
boring-cdc destination add NEW_DESTINATION --archive-root PATH --continuity-break --from-seq SEQ --confirm-data-gap
boring-cdc destination detach --confirm
boring-cdc destination promote DESTINATION --generation ID --confirm
boring-cdc destination retire DESTINATION --generation ID --confirm
boring-cdc destination verify DESTINATION [--from-seq SEQ] [--json]
boring-cdc archive reconstruct DESTINATION --selector-fence FENCE --output PATH
boring-cdc archive verify DESTINATION --selector-fence FENCE --oracle-manifest PATH [--json]

boring-cdc replay DESTINATION --from-anchor/--from-seq/--since --new-generation ID --dry-run
boring-cdc replay DESTINATION --from-anchor/--from-seq/--since --new-generation ID --confirm-replay
boring-cdc journal inspect
boring-cdc journal verify
boring-cdc journal gc --dry-run

boring-cdc recover inspect
boring-cdc recover promotion DESTINATION --adopt-external-fence --confirm
boring-cdc recover reseed --recreate-publication --recreate-slot --confirm-data-gap
boring-cdc recover reseed --add-table SCHEMA.TABLE --recreate-publication --recreate-slot --confirm-data-gap
boring-cdc recover reseed --resume RESEED_ID --confirm
```

The shared CLI registry assigns every command a stable `CMD-*` ID, canonical handler-owning Bead, mutation/read-only class, ownership requirement, confirmation policy, result schema, exit codes, condition/runbook links, and redaction policy. Help, command inventory, and compatibility fixtures are generated from that registry; command lists in human documents are checked projections rather than independently maintained grammar.

Every mutating dry-run returns a versioned `ActionPlan` with `plan_id`/digest, command ID, bound `SystemSnapshot.snapshot_id` and `state_revision`, asserted preconditions, intended domain transitions, immutable intents and external effects, resource and continuity consequences, affected checkpoints/generations, allowed remaining actions, rollback/recovery boundary, expected postconditions, evidence requirements, expiry, and canonical confirmation argv/token. The lock-owning runtime revalidates the same fields immediately before dispatch. Any state revision or bound fingerprint change invalidates the plan instead of attempting a best-effort mutation. The terminal `CliResult` references the plan digest and persisted postcondition evidence.

`init` initializes local state and publication/control relations; it does not create and abandon the permanent slot's one-time exported snapshot. `run --bootstrap` owns initial slot/snapshot lifecycle after persisting its intent. Direct live `ALTER PUBLICATION` is unsupported and continuity-breaking. Selecting a new table is expressed only as a confirmed full re-seed/new epoch.

Mutating commands first provide a dry-run showing the current source identity, `run_id`, capture epoch, configuration/table-set fingerprint, anchor/generation, intended transitions, retained-history requirements, source/destination consequences, expected interruption, and rollback/recovery path. Text output includes one shell-escaped, copy-pasteable confirmation command; JSON output includes the same canonical arguments plus an opaque short-lived `confirm_token`. The operator never computes a hash or reconstructs hidden arguments. The token binds the nonce, request ID, canonical payload digest, and expiry, is accepted only for that exact displayed transition, and is redacted from logs/artifacts. The lock-owning runtime issues a fresh unpredictable per-dry-run nonce; destructive confirmation uses `hash(nonce || canonical_request_payload)`, whose payload includes the displayed fingerprints and short expiry, and cannot authorize a changed source/epoch/configuration/table set/generation. A new dry-run always receives a new nonce, while retrying the exact displayed dry-run reuses its request ID. Replay of an established destination always targets a named new generation, requires `--confirm-replay` after dry-run, and needs a separate verified promotion; it cannot rewrite the live checkpoint.

While `run` is active, every routine mutating command is an authenticated local request to its Unix-domain owner endpoint; the CLI never opens SQLite writable. The request ID is `hash(nonce || canonical_request_payload)` and is persisted with that payload hash before the transition; retrying that exact dry-run after a lost reply reuses the ID, while a new issuance cannot collide. The terminal result is persisted before reply, and a lost reply is recovered by resubmitting that same request. Same-ID/different-payload, expired digest, peer mismatch, stale `run_id`, or changed fingerprint fails closed. Startup marks every nonterminal prior-run request `aborted_by_restart` after reconciling any remote intent and never re-executes it automatically. Only `backfill pause`, `destination pause`, and `destination detach` after bound confirmation are routine offline-eligible commands; they acquire both ownership locks, wait/reconcile under the takeover rule, persist the same request/intent record, and leave long-running work for the next `run`. Backfill start/resume/restart, destination add/resume/promote, and replay require a live owner endpoint. Source administration remains maintenance-only. The status/Prometheus listener is read-only under every configuration.

`init`, `destination retire ... --confirm`, `recover promotion ... --adopt-external-fence`, and source-mutating recovery/re-seed commands are maintenance-runtime commands under section 3. Retirement revalidates the greatest selector, grace, absence of leases/pins, and exact generation before loading the ClickHouse maintenance role to delete only that retired generation's partition/data. Promotion recovery reads the externally greatest valid selector, proves it names an existing internally consistent generation, persists `highest_external_fence` plus an adoption record in `destination_promotion_intents`, and permits only a subsequent fence strictly greater than the adopted value. It never changes the external live generation as part of adoption. They refuse to run while ordinary `run` holds either lock. After a clean handoff they become the sole owner, observe the stale-ownership wait, and reconcile local/source/external intents. Only that maintenance owner may load the administration DSN, and only around the exact owner-only request and verification. Read-only commands such as `check` and `status` do not take ownership or load administration credentials. `destination verify` is a nonce-free idempotent request asking the live owner to run its bounded audit now, persist the result through the capture-priority writer, and return that persisted record; it never advances a checkpoint. With no live owner it opens local state and the destination read-only, prints an explicitly `offline_diagnostic_not_persisted` result, and cannot block/unblock a destination or claim durable audit freshness.

### 12.2 Required workflows

The operator documentation and integration tests cover:

1. **Fresh bootstrap:** preflight -> init -> `run --bootstrap` -> anchor completion -> destination verification.
2. **Normal restart:** exclusive lock -> integrity/source reconciliation -> resume from durable source and destination checkpoints.
3. **Destination pause:** stop materialization while retaining its pin and exposing growing backlog.
4. **Destination detach/retire:** detach displays the lost replay guarantee, confirms and releases the retention pin without deleting external data; later maintenance-only retire revalidates selector/grace/no-pin evidence before deleting exactly one non-live generation.
5. **Late destination add:** choose compatible retained anchor -> dry-run required range/storage -> bootstrap -> attach at live checkpoint.
6. **Replay/rewind:** resolve complete transaction boundary, create an explicitly named new generation after dry-run and `--confirm-replay`, verify it, then promote separately; never rewrite live state.
7. **Schema/publication change:** compatible column handling follows the frozen policy. Incompatible changes require one fresh full destination/table-set generation from a compatible anchor; table-only promotion is forbidden. Adding any selected table follows the full-reseed protocol in section 12.3; unexpected publication drift also requires re-seed.
8. **Slot invalidation/source mismatch:** refuse unsafe resume and show re-seed procedure.
9. **Full re-seed:** create a new capture epoch and fresh **full** destination generations, verify each, switch, then retire old state.
10. **Journal corruption:** fail closed, preserve evidence, and require verified restore or re-seed.
11. **Idle-source heartbeat failure:** report degraded progress, retry the narrowly scoped control-row update, and show WAL headroom; never acknowledge keepalive `wal_end`.
12. **Promotion recovery after state loss:** stop ordinary `run`, adopt only the externally greatest valid selector fence after proving its target generation exists and is internally consistent, persist the adoption, then allocate future fences strictly above it; conflicting selector state remains blocked.
13. **Deterministic capture or destination poison:** persist the shared `FailurePolicy` fingerprint and failed boundary and preserve checkpoints. Capture remains alive in `capture_safe_stopped` without replication reconnect while status and safe destination draining continue. A limit failure re-arms only on startup after a changed validated limit/configuration fingerprint and retained-WAL proof; other deterministic capture failures require confirmed re-seed. A corrected destination uses its existing explicit resume workflow after owner revalidation; no generic capture-resume command exists.
14. **Destination integrity mismatch:** the live owner handles `destination verify` by advancing bounded audit cursors and persisting the result without reading PostgreSQL or moving a checkpoint; offline output is diagnostic only. Only the affected destination blocks, and recovery uses a verified new generation from a compatible anchor or explicit re-seed when required journal coverage/anchor is unavailable.

### 12.3 Table-set change through full re-seed

Adding a selected table deliberately abandons online continuity for the old capture epoch. There is no live table-add command, publication-alter intent/state machine, or new-table-only destination candidate in v0.1. `reseed_intents` records the old and proposed new epochs, complete old/new table-set fingerprints, publication/slot identities, full ClickHouse/archive candidate generations, configuration hash, verification evidence, and monotonic phase.

1. Dry-run displays the expanded table set, source interruption, new epoch, required full snapshot/storage, destination generations, and the fact that the old capture epoch cannot continue. The operator must pass `--confirm-data-gap`.
2. Stop ordinary `run`; the maintenance runtime acquires the same exclusive SQLite lock and source-derived PostgreSQL advisory lock, validates old state, and persists `prepared` before any remote mutation. Existing live ClickHouse and archive generations remain reader-visible and immutable.
3. Through short-lived administration connections, drop the configured old slot, recreate the publication with the **complete expanded table set plus both control relations**, and invoke the ClickHouse maintenance-DDL hook owned by M4 for any added table when ClickHouse is configured; then read back the changes and drop all administration credentials. Still under both ownership locks and the persisted re-seed intent, the dedicated capture/bootstrap role, through the connector's intent/name guard, creates the configured one permanent slot with `EXPORT_SNAPSHOT` and enters the initial exported-snapshot protocol for the new `capture_epoch`. M3 evidence runs this protocol without a ClickHouse destination and proves only the hook's ordering/credential boundary; executable ClickHouse object creation and existence evidence belong to `boring-cdc-m4-ddl` and `boring-cdc-m4-promotion`. The same maintenance-owned process continues as `run --bootstrap` for the new epoch on that capture-role exporter session; it never carries the administration DSN into bootstrap, the exported snapshot is never handed to another process or allowed to outlive its exporter connection. Any ambiguous remote response remains a maintenance-owned nonterminal re-seed intent; it is never attached as old-epoch continuity.
4. Snapshot every selected table—not just the added table—under the new epoch, with the corrected exporter/importer lifecycle, DDL guard, concurrent WAL capture, and published post-copy fence. M3 proves a complete expanded-table reconstruction anchor.
5. Build fresh full ClickHouse and archive candidate generations from that anchor. Existing-table and newly added-table counts/checksums/exact committed mutation sets must all verify. The old generations stay live throughout candidate construction.
6. M4 verifies/promotes the full ClickHouse generation; M5 verifies/promotes the full archive generation through each destination's monotonic fenced selector. Promotions are independently crash-recoverable; cross-destination atomicity is not claimed.
7. Mark the re-seed terminal only after required promotions and readback verification. Retire old generations only after their configured grace and operator policy.

The phases are `prepared -> source_recreated -> snapshot_imported -> anchor_complete -> clickhouse_verified -> clickhouse_promoted -> archive_verified -> archive_promoted -> complete`, with explicit `ambiguous_requires_restart` and `failed_requires_fresh_reseed` handling. `prepared`, ambiguous `source_recreated` without a durably bound live snapshot/exporter, `ambiguous_requires_restart`, and `failed_requires_fresh_reseed` are maintenance-owned and require `recover reseed --resume` after complete revalidation. From a durably verified `snapshot_imported` phase onward, the intent is resumable by ordinary `run`, which reports `reseed_incomplete` while its normal destination loops build, verify, and promote candidates through `complete`; it does not reject startup merely because these later phases are nonterminal. This deliberately disruptive protocol is smaller and safer than an online schema control plane and satisfies the requirement to detect an added table and provide a tested re-backfill path.

## 13. Configuration

Use one documented TOML file. Secrets come from environment-variable overrides and are never written to examples, logs, status output, benchmark artifacts, or archive manifests.

Configuration groups:

- runtime capture/bootstrap DSN reference with PostgreSQL `REPLICATION` and a documented connector-enforced intent/name/no-drop guard (PostgreSQL has no per-slot ACL), separately scoped fixed-row control-writer DSN reference, administration-DSN reference for `init`/full re-seed/explicit recovery drop/publication lifecycle only, publication, slot, selected tables, pinned CopyBoth transport, exact `START_REPLICATION`/origin policy, deterministic source advisory-lock derivation, dedicated lock-session probe interval, ownership deadline, maximum operation duration, stale-owner takeover interval, heartbeat cadence, and certificate-verification policy;
- stable logical table identities, canonical effective-replica key/type policy, and frozen relation-contract inputs;
- SQLite path, explicit SQLite temp/spool paths, per-connection `WAL`/`FULL` readback policy, pre-schema incremental auto-vacuum and bounded checkpoint/vacuum settings, named supported Linux filesystems, and explicit unsafe-experiment override;
- `max_wire_frame_bytes`, `max_event_bytes`, `max_transaction_bytes`, `max_transaction_events`, and canonical row/event limits;
- per-filesystem state/archive budgets, `reserved_free_bytes`, maximum SQLite/WAL/temp growth, capture spool, concurrent backfill, and archive-segment reservations;
- replay window, usable-anchor policy, and invalid-generation retention/GC policy;
- finite PostgreSQL WAL cap/free-space/rate/monitor-delay/reaction-reserve headroom thresholds and missing-metric policy;
- backfill chunk rows/bytes/duration/concurrency, exporter/importer/guard/backend lifetime, guard keepalive cadence, admitted role/session timeouts, conflicting-DDL waiter bound, and source-impact limits;
- ClickHouse pinned endpoint/server/client versions, configuration fingerprint, separate runtime-materializer and maintenance roles, pre-created generation-keyed history/selector objects, history quota, synchronous insert/durability and Rust insert-finalization settings, destination generation, query-time greatest-fence selector contract, adopted highest external fence, retirement policy, and incremental audit byte/event/time budgets plus cadence/freshness window;
- archive root, named filesystem, per-filesystem budget, schedule, JSONL/Parquet formats, pinned writer/crate/compression settings, segment size, opaque ASCII path encoding, descriptor-relative/no-follow/exclusive filesystem policy, `SEGMENT_READY` and immutable promotion-fence selector settings, explicit continuity-break policy, and incremental journal/self-consistency audit byte/event/time budgets plus cadence/freshness window;
- warning/action/critical/hard thresholds, condition freshness/hysteresis, and metadata/GC retention limits; and
- structured log/JSON status/Prometheus listener address, authentication, and TLS settings; and
- local operator-command Unix-socket path, ownership/mode, peer-credential policy, request/response byte and timeout limits, request-result retention, and confirmation expiry.

Payload values are excluded from logs by default. Configuration fingerprints are persisted with source, destination, archive, backfill, and promotion state so incompatible changes block rather than mutate continuity silently.

### 13.1 Security and exposure defaults

Status and Prometheus listeners bind to loopback (or a read-only local Unix socket) by default. They expose no mutation route. A non-loopback listener requires explicit authentication and TLS; `check` rejects an exposed unauthenticated or plaintext listener. The separate mutating operator endpoint is Unix-domain only beneath a connector-owned `0700` directory, uses a `0600` socket, validates Linux peer credentials, and enforces frozen message/time bounds. PostgreSQL and ClickHouse connections require certificate verification under the documented TLS policy; an insecure/no-verify connection is allowed only as an explicit local experiment and is permanently reported as degraded/unsafe.

The state, spool, and archive root directories are created with mode `0700`; SQLite, spool, intent, manifest, and configuration files containing secrets use mode `0600` or stricter. `check` rejects weaker permissions on named supported Linux filesystems. Because the archive root is `0700`, v0.1 archive readers run as the connector Unix account; multi-user/group/ACL sharing is out of scope. The administration DSN is loaded only by the sole lock-owning maintenance runtime for the minimum `init`/re-seed/recovery request and readback, then its connection and secret-bearing state are dropped. Normal `run` never loads or retains it. Its control-writer DSN is independently least-privileged to column-limited value updates plus immutable-key-only reads of the two administration-seeded fixed rows, with no insert/delete/key-update or excess-column privilege, and is included in secret redaction.

Redaction applies before structured logging and artifact collection: DSNs, passwords, tokens, connection-string query values, secret environment-variable names/values, driver error strings, and unapproved source paths are never emitted verbatim. Public benchmark artifacts use synthetic data only. Production archive payloads are not copied into diagnostics or public benchmark results, and an error path must be tested for credential redaction.

## 14. Benchmark and correctness harness

Check in:

- deterministic e-commerce generator for customers, products, orders, and order items;
- fixed seed and named scale profiles;
- concurrent transactional writer and application reader;
- application-generated stable `mutation_id` values and diagnostic per-run `mutation_seq` values that are explicitly allowed to contain gaps;
- a published `benchmark_mutation_ledger` source table written in the same PostgreSQL transaction as each workload mutation;
- `last_mutation_id` plus diagnostic `last_mutation_seq` columns on inserted and updated workload rows, with deletes represented by their durable ledger entries;
- keyed count/checksum oracle using canonical typed-row serialization;
- archive reconstruction/verifier command;
- fault-injection scripts and deterministic crash hooks;
- standard raw result JSON schema;
- Boring CDC and Estuary externally visible scenario runbooks/results; and
- a Debezium design-reference appendix limited to capture, offsets, and relevant configuration, with no provider score.

`connector_event_id` is internal to Boring CDC replay/deduplication and is never used as the provider-neutral comparison identity.

The provider-neutral ledger schema is frozen with at least:

```text
run_id
mutation_seq
mutation_id
transaction_group_id
entity_table
canonical_entity_key
operation
expected_after_hash       // null for delete
committed_at
record_kind               // mutation | fence
```

The ledger is included in the same publication, journal, ClickHouse materialization, and JSONL/Parquet archive as the workload tables. A mutation event and its ledger record correlate through the workload row's last-mutation fields for inserts/updates and the ledger's canonical key for deletes. PostgreSQL sequence allocation and failed transactions can create gaps, so `mutation_seq` is diagnostic ordering evidence only; no correctness claim assumes a gapless allocator.

### 14.1 Correctness fence

A result is checked only at a reproducible exact-set fence:

1. quiesce writers, wait for every workload transaction to finish, then commit a unique ledger `record_kind=fence` row;
2. in a repeatable source read that observes that fence, capture the exact committed mutation set for the run as sorted pairs `(mutation_id, canonical_ledger_hash)`, plus its count and deterministic digest over the sorted canonical pairs; `mutation_seq` is recorded only for diagnostics;
3. wait until each destination and archive reconstruction observes the matching fence record, then compute the same exact sorted pair set, count, and digest from its materialized ledger;
4. compare source and downstream sets by mutation ID/hash and report explicit missing, unexpected, and hash-mismatched entries—never merely a maximum or contiguous sequence;
5. compute source and destination keyed counts/checksums from one canonical typed-row serialization; and
6. report correlation failures, duplicate attempts, convergence time, and required operator actions.

`canonical_ledger_hash` excludes sequence values/timestamps and uses the frozen provider-neutral canonical fields (`run_id`, `mutation_id`, transaction group, entity table/key, operation, and expected-after hash). A negative oracle test intentionally omits an intermediate update whose effect is later overwritten. Final row checksums then match, but exact set difference still reports the missing mutation ID. This proves the oracle measures change delivery without falsely failing on legitimate sequence gaps.

“Capture caught up” without a watermark is not a correctness fence.

### 14.2 Common measurements

- source CPU, I/O, connections, free disk, WAL rate, and retained WAL;
- application p50/p95/p99 latency;
- capture and destination lag;
- SQLite, spool, archive, and total local storage;
- logical loss, duplicate attempts, and final convergence;
- recovery time and operator steps;
- backfill duration, snapshot age, dead tuples, and lock/source impact;
- ClickHouse query cost, merge backlog, and storage amplification; and
- deployment/resource cost with explicit assumptions.

Externally visible Estuary comparisons use the common workload, watermark, and oracle—not provider LSN equality. Failed or unfavorable results remain publishable. No other vendors are introduced into the benchmark.

Do not claim 10,000-table or terabyte-scale support without running and publishing those profiles.

## 15. Required test matrix

| Scenario | Required assertion |
|---|---|
| Insert/update/delete | Total event order and final state match source |
| Repeated same-key changes in one transaction | Transaction/mutation ordinals select the final mutation deterministically |
| Decoder positional-identity conflict fixture | Same WAL/snapshot position with a different payload hash blocks instead of acquiring a second event ID |
| Stable identity/hash golden vectors | Changing `captured_at`, `journal_seq`, `run_id`, process time, or retry metadata does not change event identity or canonical payload hash |
| Internal control-event routing | Heartbeat/fence transactions remain ordered and checkpointable but create no user-table row or benchmark mutation |
| Canonical identity/replica identity | Snapshot, update, delete, checksum, and destination grouping use the same complete non-null effective replica identity; absent/partial/TOAST key state blocks |
| Canonical-key-changing update | Old key is tombstoned and complete new tuple converges |
| Key change with unchanged TOAST | Any unchanged TOAST column blocks before acknowledgement and requires explicit recovery/re-seed |
| Multi-table transaction | Journal is all-or-nothing; no destination-atomicity claim |
| Crash before SQLite commit | Transaction replays; no partial transaction is visible |
| Crash after SQLite commit, before PostgreSQL feedback | Duplicate attempt resolves to one connector event identity |
| Crash after PostgreSQL feedback/confirmed flush | Startup reconciliation resumes with no loss |
| CopyBoth restart position matrix | Pinned transport handles bidirectional frames and PostgreSQL uses `max(requested_lsn, confirmed_flush_lsn)` without loss |
| Initial slot creation floor | Null/equal server-confirmed creation-floor variants never populate durable transaction progress or receive proactive acknowledgement; a `prepared` intent with an existing slot but no persisted floor/token enters `bootstrap_ambiguous_requires_restart`; while its snapshot stays eligible later feedback waits for importer acknowledgements, and atomic invalidation releases the gate for durable WAL draining |
| Origin and standby-status protocol | `origin='any'` policy is fingerprinted; parsed `Origin` leaves row ordinals stable; requested keepalive reply sends write/flush/apply at the same durable boundary without copying `wal_end` |
| Crash after destination write, before checkpoint | Retry converges without logical duplication |
| Oversized source transaction | Limit blocks before acknowledgement, persists XID/final-LSN/limit evidence, keeps the process alive without replication reconnect for the same fingerprint, and re-arms only after startup validates a changed limit/configuration fingerprint plus retained WAL; otherwise re-seed |
| Logical-decoding spill/commit burst | With `streaming=false`, a just-under-limit transaction converges under incremental CopyBoth backpressure with a test-attributable positive spill-counter delta, zero streaming-counter delta, and peak RSS within the frozen aggregate receive/decoder/staging budget; an over-limit burst cannot exhaust memory/disk reserves or enter a reconnect hot loop |
| Spool/SQLite `ENOSPC` | No acknowledgement; bounded safe-stop and recovery are visible |
| Near-limit admission accounting | Wire/frame, canonical transaction, SQLite/WAL/temp, backfill, and archive reservations cannot cross the emergency reserve |
| Separate state/archive filesystems | Each independent budget/reserve is validated and a full archive filesystem blocks only the archive before source-safety escalation |
| Repeated crash while transaction spools | Exclusive-lock startup reclaims safe orphans; malformed/ambiguous spools block |
| Unsupported SQLite settings/filesystem | Every connection reads back `WAL`/`FULL`; unknown/non-allowlisted filesystems reject unless explicit unsafe mode disables the durability claim |
| SQLite vacuum/checkpoint lifecycle | Incremental auto-vacuum is set before schema creation; bounded checkpoints/incremental vacuum preserve latency; automatic full `VACUUM` never runs |
| Abrupt host/VM termination | On each named supported Linux filesystem, locally committed source state recovers through the last acknowledged end LSN |
| Network loss | Compose restarts with unlimited attempts while the process honors persisted capped backoff/jitter before reconnect; ownership takeover resumes from the durable position without loss; PostgreSQL zombie-session bounds are no greater than the takeover interval and measured recovery includes both |
| CopyBoth loss while advisory lock remains healthy | v0.1 still declares ownership loss, fences dispatch, exits, and resumes only through supervised restart/takeover; in-process replication reopen is a test failure |
| Idle selected tables with unrelated WAL | Published heartbeat is journaled before feedback; keepalive `wal_end` alone never advances acknowledgement |
| Fixed control-row cardinality and privilege abuse | Heartbeat and fence relations remain exactly one seeded row; writer can update only allowed non-key columns and select only the immutable key; source-observed insert/delete/key-change/unexpected-column, zero-row, and multi-row outcomes block before feedback |
| Heartbeat permission/outage | Role can update only the one bounded heartbeat row; failure degrades health and exposes WAL headroom without synthetic feedback |
| Transient destination failure | Persisted capped backoff/jitter survives process restart, retries one immutable intent, and convergence resumes without checkpoint skip or retry storm |
| Deterministic destination poison/retry exhaustion | Stable failure fingerprint blocks before the journal transaction; explicit resume requires corrected contract/configuration and the unaffected destination plus capture continue within bounds |
| Destination offline | Capture continues within bounds; other destination progresses |
| Paused destination pins retention | Pressure identifies the pin; no unconsumed event is silently removed |
| Journal pressure transitions | Warning/action/critical/hard actions occur in order |
| Journal corruption/missing sequence | Startup blocks and requires verified restore or re-seed |
| Slot invalidation | Connector blocks and requires explicit re-seed |
| Slot ahead of restored SQLite | Startup refuses unsafe resume |
| Timeline/source/publication/slot mismatch | Startup blocks before streaming |
| External live publication mutation | Ownership denies it; if forced administratively, runtime fingerprint drift blocks and requires re-seed |
| Bootstrap crash before/after slot response and local persistence | Durable intent distinguishes retryable, exported, and ambiguous-slot recovery states |
| Bootstrap crash after token persistence, each importer acknowledgement, normal exporter release, and first feedback | Pre-import exporter loss invalidates; normal post-import exporter release remains valid; retained-slot versus full-reseed recovery is correct |
| Lost creation snapshot plus transient insert/delete | Existing retaining slot is drained and existing-slot snapshot converges without losing the transient pair, or ambiguous continuity starts a new epoch/full re-seed |
| Initial exported-snapshot bootstrap | DDL guard locks precede slot creation/export; exporter stays command-idle, capture uses a separate replication connection, `(consistent_point, start_seq)` persists before copy, and every importer runs `SET TRANSACTION SNAPSHOT` first |
| Importer/DDL-guard loss | Importer death before assigned reads or guard loss before durable fence atomically invalidates the generation and releases its feedback gate so durable WAL can drain; normal exporter release does not invalidate |
| Existing-slot lower-stitch proof | Snapshot rows use the last locally durable pre-export LSN/sequence, never a post-export sampled LSN |
| Transactions before/during/after snapshot creation | Snapshot plus WAL total version converges at the transactional fence watermark |
| Snapshot completion before capture fence | Anchor remains unusable until the published fence nonce, target LSN, and complete journal sequence are durably paired |
| Backfill with concurrent writes | Final keyed checksum matches at the mutation watermark |
| Crash during backfill | Old workers are fenced; remaining work resumes under a valid generation |
| Pre-import exporter loss/cancelled incomplete generation | All snapshot chunks/events become ineligible for anchors/live namespaces; a full new generation leaves no stale baseline row |
| Snapshot age/source-impact limit | Workers throttle and generation ends rather than pinning indefinitely |
| Update/delete during chunk copy | Newer WAL total source version wins |
| WAL journaled before delayed snapshot row | Final state converges independent of journal append order |
| Late snapshot + GC + new destination | Only a complete retained anchor can bootstrap |
| Empty destination from arbitrary retained suffix | Request is rejected |
| Established replay candidate without anchor | Suffix-only diagnostic output cannot be promoted; every promotable full candidate requires a compatible complete anchor |
| Nullable/no-default column added outside backfill | Older `absent_for_schema` projects as null and materialization continues |
| Idle/no-row DDL | Catalog fingerprint poll detects the full relation-contract change even without a new row event |
| DDL during active backfill | Canonically ordered `ACCESS SHARE` guards block every admitted contract-changing DDL; a conflicting `pg_locks` waiter beyond the bound raises `backfill_ddl_waiter`, invalidates the generation, releases its feedback gate/guard, and prevents application lock-queue amplification |
| DDL immediately before/after copy fence | Guard/fingerprint protocol yields one frozen schema and deterministic restart/block behavior at both boundaries |
| Changed `Relation` immediately followed by DML | Capture synchronously validates the contract before decoding or acknowledging the DML transaction |
| Defaulted/generated/non-null column addition | Requires fresh generation and re-backfill; no ambiguous projection |
| Incompatible type change | Whole destination blocks before the transaction; checkpoint cannot skip |
| Mixed compatible/incompatible tables in one transaction | No event is skipped behind a global checkpoint |
| Removed replica identity | Unsafe update/delete handling blocks |
| Added selected table | Requires confirmed full re-seed/new epoch, recreated full publication/slot, and complete snapshot of the expanded table set |
| Expanded-table anchor | M3 proves the anchor contains all old and new tables; no new-table-only candidate can be promoted |
| Table-add full-generation promotion | Old ClickHouse/archive generations remain readable until full expanded candidates preserve existing tables and pass M4/M5 promotion |
| Crash during table-add re-seed | `reseed_intents` resumes only after epoch/publication/slot/anchor validation or requires a fresh confirmed re-seed |
| Re-seed administration credential lifetime | Normal `run` never loads the administration DSN; maintenance loads it only around owner-only requests and redacts/drops it afterward |
| Explicit null vs unchanged TOAST | Tagged states remain distinct |
| TOAST patch before/during baseline | Canonical state reconstructs from retained explicit value |
| ClickHouse duplicate batch after write-before-checkpoint crash | Canonical query collapses identical event IDs and blocks conflicting same-ID payloads |
| ClickHouse crash after acknowledged batch | Pinned synchronous/fsync and Rust insert-finalization contract survives restart or checkpoint recovery replays/blocks rather than claiming durability |
| Checkpointed ClickHouse row/marker or contract modified externally | Incremental audits stay within byte/event/time budgets, persist cursor and freshness-window `journal_verified_range`, detect missing/conflicting history or correctness-critical DDL/settings, and block only ClickHouse with replay/rebuild guidance |
| ClickHouse version/tombstone protection | Composite source version is not narrowed into native replacement version; merges/TTL never delete required predecessor values or tombstones |
| ClickHouse history quota/retired generation | Quota blocks the destination without deleting needed history; ordinary run only marks retirement eligibility; maintenance-only `destination retire` revalidates the live selector/grace/no-pin proof before deleting a non-live generation |
| ClickHouse update/delete load | Canonical current-state query remains correct |
| Query before/after merge and `FINAL` | Correctness and cost are documented |
| Re-seed with historically deleted row | Fresh destination generation has no stale row |
| Stale external write and promotion crash | Epoch/generation namespace, lease, external selector, and durable promotion intent reconcile without selecting the wrong live generation |
| Delayed stale promotion after newer switch | Query-time greatest-fence resolution keeps a lower selector row inactive; duplicate identical rows are harmless; more than one distinct candidate at the same greatest fence blocks; no DDL switch occurs |
| Promotion recovery after local state loss | Maintenance verifies and adopts the externally greatest unique selector/target generation, persists `highest_external_fence`, and the next allocated fence is strictly greater; unverifiable external state remains blocked |
| Capture-epoch change | Old destination generations refuse continuation and require re-seed |
| Checkpointed archive artifact corrupted or removed | Incremental audits stay within byte/event/time budgets, persist separate freshness-window `journal_verified_range` and `self_consistent_range`, detect selector/manifest/ready-marker/file-hash mismatch, block only archive, and never call gaps/older history verified |
| Provider-neutral sequence gap | Failed transactions may leave diagnostic `mutation_seq` gaps without failing an otherwise exact set match |
| Provider-neutral omitted intermediate mutation | Final row checksum may match, but exact `(mutation_id, canonical_ledger_hash)` set difference fails |
| Add second destination | Bootstrap uses a retained compatible anchor, not PostgreSQL |
| Destination beyond retention/no anchor | Replay rejects and points to shared re-backfill/re-seed |
| Established-destination replay | Requires dry-run, named new generation, confirmation, verification, and separate promotion; live checkpoint is unchanged |
| Archive crash before/after intent | Retry selects exactly the persisted range |
| Archive crash at every file/manifest/fsync/directory-rename/parent-sync/`SEGMENT_READY`/checkpoint phase | A matching final directory without a marker is revalidated, re-synced, marked, and adopted; temporary/mismatching output is quarantined and blocks |
| Hostile archive identifiers and path races | Raw/sanitized source names never enter paths; opaque mappings round-trip; slash/dot/Unicode collisions, symlinks, hard links, owner/mode/type swaps, and no-follow/exclusive races fail closed |
| Candidate archive segment readiness | Segment-ready candidate data remains invisible until the immutable greatest-fence selector is published and read back |
| JSONL + Parquet partial publication | Shared generation checkpoint waits until both artifact sets are segment-ready |
| Parquet/JSONL deterministic retry | Pinned writer/compression/settings produce byte-identical parts, manifests, and hashes across fresh process/run/time |
| Archive configuration generation change | Existing-lineage promotion requires a compatible anchor; a confirmed continuity break creates a distinct archive destination/root identity and selector lineage with a durable first-boundary discontinuity record, never a suffix-only replacement |
| Snapshot event before anchor completion | Live ClickHouse/archive output ignores it; only a fenced candidate generation may retain it |
| Real SQL `TRUNCATE` on a published table | Detection-only pgoutput message blocks before acknowledgement and marks continuity for re-seed |
| Two runtime instances, same SQLite path | Exclusive state lock prevents concurrent ownership |
| Two runtimes, different SQLite paths, same source/slot | Fixed source-derived PostgreSQL advisory lock prevents split-brain capture/maintenance |
| Advisory-lock backend loss with other connections alive | Ownership deadline expires; all new dispatch stops and generations fence, but deadline-fit admission is required only for source-mutating operations; process exits and successor reconciles immutable destination intents/namespaces without in-place lock reacquire |
| Mutating CLI lost response and replay | One server-issued dry-run nonce makes retries reuse an ID but a later repeat get a new ID; same-ID conflict and stale digest/peer/run/fingerprint reject; a crash marks nonterminal requests `aborted_by_restart` after intent reconciliation and never auto-reexecutes them |
| Offline mutation and status-listener abuse | Eligible offline command acquires both locks and reconciles; status TCP listener has no mutation route and direct second-writer SQLite access fails |
| Stale destination/backfill worker | Generation compare-and-swap plus the external namespace lease prevents checkpoint/chunk and live-side-effect corruption |
| Read-only `check` | Real SQLite/store state is unchanged; persistent PRAGMAs are validated read-only and filesystem experiments use only a disposable probe |
| Exposed listener/insecure TLS/permission/driver error | `check` rejects unsafe defaults and redaction keeps credentials out of status/log/artifact output; command-socket path/mode/peer/message bounds are enforced |
| Concurrent health conditions/stale metrics | Severity precedence, freshness, and hysteresis produce deterministic `overall_health` without hiding conditions |
| Automatic pin-safe GC | Runtime reclaims only complete eligible transaction ranges in bounded batches; dry-run remains inspection-only |
| Process/journal upgrade | Migrations and persisted states recover cleanly |

Every milestone adds its own unit, integration, and fault tests. M6 runs the integrated endurance matrix; it does not postpone foundational safety tests.

## 16. Milestones and dependency order

### M0 — Contract and irreversible decisions

Deliver:

- owner-approved public license;
- versioned schemas/validators for stable IDs, authority/ownership registries, Git/Beads `world_state`, effective work contracts, bounded context packs, impact/staleness, evidence claims/compatibility, generated-view drift, and handoff manifests; repository-local `scripts/agent/` entry-point shape and CI wiring, without a daemon or automatic mutation;
- Rust single-binary workspace scaffold and Docker Compose shape with every Postgres/ClickHouse image pinned to an explicit tested version and immutable digest, connector restart configured for unlimited attempts while the process-owned persisted `FailurePolicy` enforces capped exponential backoff/jitter before transient reconnect, and PostgreSQL TCP keepalive plus `client_connection_check_interval` settings whose measured zombie-backend reap bound does not exceed the ownership takeover interval;
- approved `docs/EVENT_FORMAT.md`, including positional identities, canonical payload hashes, explicit exclusion of every local/volatile field, internal heartbeat/fence routing, byte units, canonical encodings, golden vectors, and full relation-contract fingerprint inputs;
- approved `docs/CLICKHOUSE_MODEL.md`, including pre-created generation-keyed append-only history and selector objects, query-time greatest-fence resolution with same-fence conflict detection and no DDL switch, separate runtime/maintenance privilege matrices, executable DDL/query contract, pinned server/client/crate versions, required Rust insert finalization, synchronous/async policy, fsync/write-concern boundary, batch recovery, shared transient/deterministic failure taxonomy with persisted capped backoff/retry budget and corrected resume, incremental retained-range event/marker audit with cursor, byte/event/time budget and freshness-window `journal_verified_range`, correctness-critical object/query/settings fingerprint contract, history quota, prohibition on native replacement-version narrowing/protective-tombstone deletion, and one-node limitations;
- supported PostgreSQL versions; a selected/pinned CopyBoth-capable Rust replication transport; exact `START_REPLICATION`/`origin='any'` policy, complete standby-status packet, explicit slot-creation floor distinct from durable transaction progress, and effective `max(requested_lsn, confirmed_flush_lsn)` restart fixtures; frozen `logical_decoding_work_mem`/spill-counter capability and commit-burst policy; deterministic-versus-transient failure taxonomy with persisted no-hot-loop behavior; one canonical effective-replica row identity; stable `logical_table_id`; type matrix; and exactly one administration-seeded row in each published fence/heartbeat relation with explicit least-privilege source/ClickHouse role matrices;
- TOAST reconstruction model, including explicit rejection of key-change plus unchanged-TOAST events;
- additive-column policy, snapshot-schema binding, synchronous changed-`Relation` validation, catalog-poll interval, canonically ordered DDL-guard locks, a per-version proof that every admitted contract-changing DDL conflicts, and corrected exporter/importer invalid-generation rules;
- row/event/transaction/wire limits, bounded incremental CopyBoth receive/decoder/staging queues with aggregate runtime-memory admission and peak-RSS evidence, plus per-filesystem admission/reservation equations;
- frozen SQLite-only physical journal contract (logical append-only `journal_events`, no payload-log file), per-connection `WAL`/`FULL` apply/readback, pre-schema incremental auto-vacuum, bounded checkpoint/incremental-vacuum/no-automatic-full-`VACUUM`, named Linux filesystem, permissions, read-only status exposure, owner-executed Unix command endpoint, dedicated advisory-lock session/deadline/takeover, and redaction contracts;
- JSONL/Parquet directory-commit, internal `SEGMENT_READY`, immutable monotonic fence selectors, opaque ASCII path mapping, descriptor-relative no-follow/exclusive operations, same-Unix-account reader, pinned writer/crate/compression/reproducibility, byte-identical golden retry, shared transient/deterministic failure taxonomy with persisted capped backoff/retry budget and corrected resume, incremental selector/manifest/marker/file-hash audit with cursors, byte/event/time budgets, freshness windows, and separate `journal_verified_range`/`self_consistent_range` floors, and named Linux filesystem durability contracts;
- finite source-WAL headroom equation (cap, free space, WAL rate, monitoring delay, reaction reserve), sink-history, archive, metadata-retention, automatic pin-safe GC, and local disk-pressure policies;
- named benchmark profiles; and
- completed decision register with owner, default, validation test, and blocking status for every safety decision.

**Exit:** the decision register contains no `Open`, `Confirm`, or owner-pending safety decision; every M0 row has an approved fixture specification and names its later executing Bead, but M0 never requires runtime results from M1–M7; authority and stable-ID registries validate with one canonical owner per ID; world-state/effective-contract/context/evidence/handoff schemas pass deterministic and hostile/stale fixtures; Compose validates the pinned images; the journal/destination format contracts are approved; no secrets are present; and the license is recorded. M0 is failed—not merely deferred—while a blocking decision remains open. No implementation agent may begin M1 or create journal/destination code before this exit is met.

### M1 — Protocol, workload, and bootstrap design

Depends on M0.

Deliver:

- deterministic workload, published source mutation ledger, exact committed `(mutation_id, canonical_ledger_hash)` set/count/sorted-digest oracle, diagnostic gap-tolerant sequences, and typed-row checksum oracle;
- one versioned runtime `SystemSnapshot` and `ActionPlan` envelope, including snapshot/revision binding, preconditions, transitions, effects, consequences, postconditions, plan/evidence digests, compatibility goldens, and domain-owner references without re-owning domain state;
- one shared CLI contract with a canonical command registry, consistent `--help`, stable exit-code taxonomy, versioned JSON result/error envelope, stdout/stderr discipline, redacted copy-pasteable confirmations, and golden compatibility tests;
- genuinely read-only, stateless source/config/store preflight, including connector-role session timeout/keepalive compatibility, fixed control-row operation/cardinality/grants, restart/takeover bounds, and listener/socket policy; persistent SQLite application and intent reconciliation remain M2 responsibilities;
- connector-owned publication/control **specification and protocol fixtures** with detection-only `TRUNCATE`, exactly one seeded fixed row for each published capture-fence/heartbeat relation, column-limited value `UPDATE` plus immutable-key-only `SELECT` privileges, full-reseed-only table-set changes, maintenance/runtime ownership, and runtime fingerprint rules;
- source identity and capture-epoch model;
- `pgoutput` decoder with unsupported-message fail-closed behavior, explicit rejection of control-relation insert/delete/key/unexpected-column operations, and positional-ID/payload-conflict fixtures;
- deterministic row/mutation ordering;
- full relation-contract catalog fingerprinting, synchronous changed-`Relation` validation, per-version DDL-lock conflict matrix, and idle/immediate-DML/active-backfill fixtures;
- bootstrap state-machine design with DDL guard acquired before export, command-idle exporter, separate capture replication connection, a server-established creation floor explicitly separate from durable transaction progress, importer-gated feedback, first-statement snapshot import, bounded exporter/importer lifecycle, retained-slot recovery after a lost snapshot, and slot-creation/exported-snapshot protocol fixtures for the **one permanent-slot path only**; no auxiliary runtime slots or silent slot recreation; and
- raw event inspection demo and protocol fault tests.

**Exit:** fixed-seed transactions decode reproducibly; CopyBoth/restart, heartbeat, bootstrap, exact-set oracle, and full-reseed states are specified and tested as protocol/state-machine fixtures; actual SQL `TRUNCATE`, publication drift, idle and immediate DDL, and unsupported protocol/table/type cases block clearly. No online table-add state machine exists.

### M2 — Bounded durable journal and JSONL commit engine

Depends on M1.

Deliver:

- SQLite schema and migrations, including bootstrap/import/re-seed intents, durable capture fences/control events, destination leases/promotion intents, archive generation markers, and batch/segment intents;
- sole-owner maintenance runtime for `init`, confirmed full re-seed/resume, and explicit source recovery, including exclusive-lock handoff, ephemeral administration-DSN loading, startup blocking on ambiguous or maintenance-owned re-seed phases, and ordinary-`run` resumption of verified `snapshot_imported` and later phases;
- logical append-only SQLite journal with no separate payload-log file;
- published heartbeat writer/capture/no-op routing and idle-WAL feedback tests;
- per-connection `WAL`/`FULL` apply/readback, pre-schema incremental auto-vacuum, bounded checkpoint/vacuum, named-filesystem validation, permissions, and secure listener/TLS checks;
- one `TxnBuffer`/`MemoryBudget` capture path, logical-decoding spill/commit-burst telemetry, the shared persisted `FailurePolicy`, alive-but-no-reconnect `capture_safe_stopped` behavior with config-fingerprint re-arm, and exclusive-lock orphan-spool recovery;
- pre-allocation wire/frame/event validation, bounded incremental CopyBoth receive/decoder/staging backpressure, aggregate runtime-memory admission/peak-RSS evidence, and per-filesystem admission/reservation controller;
- integrated `boring-cdc run` capture runtime owning CopyBoth receive -> bounded spool -> atomic journal/source-state commit -> complete standby-status feedback, plus supervised restart/takeover and graceful/abrupt shutdown;
- capture-priority writer and durable acknowledgement invariant;
- deterministic positional connector event IDs, separate payload hashes, stable logical table identity, and checksums;
- startup source-position reconciliation;
- integrity checks and corruption state;
- local budget, emergency reserve, automatic audited pin-safe transaction-boundary GC, bounded metadata/intent retention, and pressure runtime;
- exclusive runtime ownership, dedicated advisory-lock session with bounded ownership deadline/takeover, component generations, and destination external-side-effect leases;
- runtime-owned Unix-domain operator-command endpoint with server-issued per-dry-run nonces, persisted idempotent requests/results, peer/fingerprint/digest validation, lost-response replay, prior-run nonterminal request abort after intent reconciliation, and offline lock-owning intent path;
- archive intent/directory-commit engine with JSONL and internal `SEGMENT_READY`, opaque safe paths, gated so candidate data remains generation-invisible before M3 anchors and promotion;
- capture, feedback, ownership-loss, command-response, bootstrap-intent, archive, stale-promotion, and checkpoint crash hooks; and
- read-only status/JSON/metrics listener with `overall_health` plus concurrent conditions, loopback default, and implemented authentication/TLS for explicit non-loopback exposure; and
- canonical fresh `SystemSnapshot` projection plus persisted before-plan-after postcondition evidence, with stable owner/freshness/action/runbook links and no independent status state machine.

**Exit:** acknowledgement—including idle heartbeat progress—never outruns SQLite under the declared storage contract; deterministic capture/limit failures cannot trigger an automatic restart storm or move the checkpoint, while retryable environmental failures preserve capped backoff across restart; the creation floor is never mistaken for durable transaction progress; commit/feedback ambiguity produces deterministic duplicates but no loss; near-limit admission cannot exhaust reserves; orphan spools, pressure, corruption, promotion ambiguity, ownership loss, and bootstrap ambiguity fail explicitly; routine mutations execute through the idempotent owner endpoint and ordinary `run`/maintenance commands cannot own state concurrently; JSONL WAL-only segments survive every directory commit phase but remain invisible without a valid monotonic-fence selector.

### M3 — Chunked backfill, stitching, and anchors

Depends on M2.

Deliver:

- persisted keyset range planner and bounded workers;
- permanent slot creation plus exported-snapshot bootstrap vertical slice using the durable M2 intent;
- crash recovery before/after slot response, local snapshot-token persistence, every importer acknowledgement/assigned read, legitimate exporter release, DDL-guard release, first feedback, and lost-snapshot retained-slot draining including transient insert/delete;
- initial `(consistent_point, start_seq)` and existing-slot last-durable `(lower_stitch_lsn, lower_stitch_seq)` protocols with importer acknowledgements, with the creation floor never fabricated as a source transaction or proactively acknowledged;
- one fixed published control-row post-copy fence updated with a unique nonce, observed through `pgoutput`, and atomically paired `(LSN, seq)` proof; no sampled-LSN shortcut;
- per-table snapshot-schema binding, additive-column policy, canonically ordered `ACCESS SHARE` DDL guards through durable fence observation, immediate changed-`Relation` validation, corrected invalid-generation rules, and live/candidate gates;
- generation fencing, pause/resume, `backfill restart --confirm`, bounded backend-PID cleanup, role/session timeout keepalive, conflicting-DDL waiter detection/release, importer-gate release on invalidation, and source-impact limits;
- reconstruction anchor completion and retention pinning;
- full-reseed table-set expansion from a durable `reseed_intent` through new epoch, recreated full publication/slot, and a complete anchor covering **all** old and new tables; M3 proves only the ordered maintenance-DDL hook boundary, while executable ClickHouse object creation for an added table and destination promotion are M4/M5 evidence;
- exact committed mutation-set/count/sorted-digest and typed-checksum watermark fence;
- naive full-read benchmark control; and
- backfill/restart/late-snapshot/before-during-after-snapshot fault tests.

**Exit:** concurrent and interrupted backfills converge; stale workers cannot commit; legitimate exporter release remains valid while importer/DDL-guard loss invalidates; snapshot age is bounded; no invalid generation can materialize live state; an anchor remains unusable until its exact lower proof and published post-copy fence are durably paired; and a full expanded-table anchor can reconstruct an empty sink. Final table-add destination acceptance waits for M4/M5.

### M4 — ClickHouse correctness and schema generations

Depends on M3.

Deliver:

- executable event-history DDL and canonical query implementation from `docs/CLICKHOUSE_MODEL.md`;
- pinned ClickHouse insert/batch-marker durability implementation, required Rust client finalization, crash-after-ack/restart verification, and incremental read-only audits with cursor, byte/event/time budget, freshness-window journal coverage, and correctness-critical object/query/settings fingerprints;
- canonical current-state view with per-column TOAST patch reconstruction;
- versioned tombstones, complete key-change expansion, and explicit key-change/unchanged-TOAST block;
- event-ID-first physical duplicate collapse, payload-conflict detection, idempotent replay, and checkpointing;
- nullable/no-default additive schema handling and destination-wide incompatible-schema block;
- stable logical-table grouping and fresh **complete** full table-set candidate rebuild from a compatible anchor, with no table-only promotion;
- fresh full table-set generation rebuild in pre-created generation-keyed history, no native replacement-version narrowing or protective-history deletion, durable monotonic promotion fence, query-time greatest-fence selector resolution with no DDL switch, external-selector reconciliation/adoption, history quota, and maintenance-only retirement; and
- update/delete, merge, `FINAL`, TOAST, re-seed, stale-write, promotion-crash, and quota benchmarks/tests.

**Exit:** state is correct before and after merges; explicit null/TOAST patches remain distinct; accepted batches survive their stated/finalized contract or block/replay; historically deleted rows do not survive re-seed; and a full expanded-table ClickHouse generation preserves existing tables before promotion.

### M5 — Parquet, fan-out, replay, and retention workflows

Depends on M3 and M4.

Deliver:

- pinned Parquet writer/crate/format/compression/schema mapping grouped by table/fingerprint, with byte-identical retry goldens;
- shared JSONL/Parquet deterministic segment directory using opaque ASCII components and descriptor-relative/no-follow/exclusive operations, pending manifest, internal `SEGMENT_READY`, immutable promotion-fence selector markers, checkpoint/promotion protocol, and incremental selector/manifest/marker/file-hash audit with separate journal/self-consistency cursors and freshness-window ranges;
- independent ClickHouse/archive scheduling, persisted transient-versus-deterministic failure classification, capped backoff/jitter and retry budgets, explicit corrected resume, leases, and lag;
- archive configuration-generation replay-from-anchor, or explicit continuity break into a new archive destination/root identity and selector lineage with a durable first-boundary discontinuity record; suffix-only output can never promote into the prior lineage, and candidate segment durability remains separate from reader visibility;
- late destination bootstrap from anchors and an anchor requirement for every promotable replay/rebuild candidate;
- explicit pause/detach/blocked-resume workflows, read-only `destination verify`, and new-generation-only replay/rewind through the runtime-owned idempotent Unix command endpoint, with nonce/fingerprint-bound dry-run/confirmation, verification, monotonic external-fence promotion, and maintenance-only retirement; plus `recover promotion ... --adopt-external-fence` for verified highest-fence adoption after local state loss;
- final full expanded-table archive verification/promotion for table-add re-seed, preserving all existing tables while the old generation stays live;
- replay-window, anchor-expiry, intent/audit/invalid-generation retention, and bounded automatic GC;
- independent destination, archive-phase, separate-filesystem, and retention-pressure fault tests.

**Exit:** either destination can stop independently; late bootstrap succeeds only from compatible retained anchors; readers see only `SEGMENT_READY` data under the greatest valid promotion-fence selector; expired requests fail with re-backfill/re-seed guidance; and added-table acceptance passes only after both full destination generations preserve old and new tables.

### M6 — Integrated source-safety and endurance hardening

Depends on M5.

Deliver:

- derived source/local/sink conditions projected from domain facts, stable external names, concurrent-condition freshness/hysteresis, and alerting without an independent safety transition machine;
- complete stable condition/action/runbook registry and tested agent/operator recovery projections, including snapshot-to-action-to-postcondition causality, evidence freshness, and exact safe next commands;
- source free-disk/WAL-headroom and per-filesystem capability reporting;
- snapshot/XID/vacuum-impact, logical-decoding spill/commit-burst, processing-failure/backoff, and destination-audit freshness/range telemetry;
- slot invalidation, local-restore mismatch, source timeline, heartbeat, DDL-waiter recovery using M3 schema-guard evidence, deterministic capture/destination poison, oversized-transaction recovery, destination integrity mismatch, schema/full-reseed table-set change, verified external-fence adoption for promotion recovery, measured zombie-backend reap/takeover bounds, and sink-quota runbooks;
- full clean-environment failure matrix and endurance profiles;
- read-only status/metrics authentication/TLS hardening, Unix command-endpoint ownership/peer/request-bound hardening, advisory-lock-loss/takeover evidence, plus ClickHouse free-space/temporary-part/merge-amplification/quota telemetry; and
- raw benchmark evidence and operational-step accounting.

**Exit:** every unsafe state is visible with a tested runbook; no command silently creates a gap; all earlier milestone fault tests pass under endurance load.

### M7 — Public v0.1 release

Depends on M6.

Deliver:

- Linux artifact and container image;
- reproducibility instructions and raw benchmark data;
- public human projections generated or mechanically checked against canonical requirement/command/condition/scenario/runbook/release registries and measured evidence, with source IDs/digests and no private agent context;
- known limitations, measured source/local/sink capacity boundaries, security policy, and upgrade notes;
- Boring CDC and Estuary external scenario runbooks/results;
- Debezium capture/offset design-reference appendix only;
- `v0.1.0` tag.

**Exit:** a new user can clone, run Compose, bootstrap, mutate data, materialize ClickHouse and JSONL/Parquet, inject required failures, reconstruct archives, and verify exact committed mutation sets plus watermark checksums. The `boring-cdc-m7-release` Bead is the authoritative root-completion record; the root epic cannot close before it.

## 17. v0.1 release acceptance

v0.1 is complete only when:

- all required fault tests pass repeatedly from a clean environment;
- no expected committed event is missing after recoverable failures;
- duplicate attempts are observable while connector identities remain stable;
- PostgreSQL feedback never exceeds the locally durable published transaction end; the server-established slot-creation floor remains separate and is never fabricated or proactively acknowledged; an eligible snapshot gates later feedback until importer acknowledgements, while atomic invalidation releases the gate for durable WAL draining; idle progress comes only from a durably journaled heartbeat, never keepalive `wal_end`;
- startup rejects source/slot/timeline/local-state contradictions, unresolved bootstrap provenance/WAL continuity, ambiguous or maintenance-owned re-seed phases, and unreconciled promotion intents; verified `snapshot_imported` and later re-seed phases resume under ordinary `run` with visible `reseed_incomplete` until complete; the pinned CopyBoth transport passes origin, requested-keepalive, creation-floor, complete-status-packet, and requested/confirmed restart fixtures; dedicated advisory-lock-session loss fences all work and successor takeover waits/reconciles;
- publication ownership prevents ordinary live drift, forced drift is detected as continuity-breaking, and adding a table completes only through a new epoch/full re-seed whose full destination generations preserve existing tables;
- interrupted backfills with concurrent mutations converge at an exact committed mutation-set/count/sorted-digest and typed-row correctness watermark, while legitimate exporter release remains valid and invalid snapshot generations cannot affect live state;
- initial bootstrap persists `(consistent_point, start_seq)`, existing backfills persist the last durable lower stitch pair, DDL guards are acquired before export, connector-role timeouts/keepalives and exporter/importer/backend cleanup are bounded, conflicting DDL waiters invalidate/release before lock-queue amplification, invalid generations release importer feedback gates, lost snapshots retain/drain provably continuous slots, and all anchors use the first matching published transaction fence observed through `pgoutput`;
- every promotable destination generation—new, replayed, rebuilt, or re-seeded—starts from a compatible complete anchor with a durable post-copy capture fence; suffix-only diagnostic generations cannot promote;
- ClickHouse current state and archive reconstruction converge after quiescence; ClickHouse acknowledged batches satisfy pinned settings plus Rust insert finalization; native replacement-version narrowing and deletion of required tombstone/predecessor history are absent; canonical queries resolve a unique greatest selector fence from data without DDL switching; delayed lower fences cannot replace a newer generation; after local state loss the maintenance adoption workflow proves and persists the external high-water fence before any higher allocation;
- archive readers resolve the greatest valid immutable promotion-fence selector and then complete deterministic `SEGMENT_READY` directories whose paths use only opaque ASCII components through descriptor-relative/no-follow/exclusive operations; hostile identifiers and path races fail closed; byte-identical JSONL/Parquet retries pass and configuration changes preserve continuity from an anchor or publish an explicit discontinuity;
- destination outages and deterministic poison failures do not couple destination progress or source acknowledgement; retryable failures preserve capped schedules across restart, deterministic fingerprints cannot hot-loop or skip checkpoints, and recovery is explicit;
- bounded wire/transaction, per-filesystem local/archive, sink-history, snapshot, replay, and WAL limits produce explicit throttle, safe-stop, block, or re-seed states without crossing the emergency reserve;
- unsupported schema/protocol/value cases never advance past the offending source transaction or destination checkpoint;
- schema/delete/TOAST behavior, full relation-contract fingerprint policy, source/sink durability boundaries, and measured limits are public;
- benchmark profiles, seed, downstream-materialized source ledger, gap-tolerant exact-set oracle, raw results, and assumptions are reproducible;
- incremental destination audits stay within per-pass byte/event/time budgets, persist cursor/freshness coverage, detect exercised post-checkpoint ClickHouse/archive corruption and correctness-critical contract drift, publish ClickHouse `journal_verified_range` plus archive `journal_verified_range`/`self_consistent_range`, and never infer gaps/older integrity;
- routine mutating CLI operations execute exactly once through the lock-owning runtime's peer-validated Unix-domain endpoint (or an offline owner that acquires both locks), while status/metrics remain read-only; per-dry-run nonces distinguish later repeated commands, lost-response replay is idempotent, prior-run nonterminal requests abort instead of re-executing, and stale confirmation/direct second-writer tests pass;
- both published control relations retain exactly one seeded fixed row and the control writer cannot insert, delete, or change keys;
- status/metrics security defaults, command-socket permissions/peer/message bounds, TLS verification, filesystem permissions, and credential-redaction tests pass; and
- every durability-bearing SQLite connection reads back required PRAGMAs, incremental auto-vacuum/checkpoint policy passes, and each named supported Linux filesystem has abrupt-restart evidence;
- canonical requirement/decision/command/condition/transition/scenario/release/risk ownership is complete and unique; generated public views carry source IDs/digests; stale contracts/evidence are rejected; and a clean-checkout agent can obtain a bounded task context and deterministic handoff without private context or full-plan ingestion; and
- the M0 decision register is closed with approved contracts, fixture specifications, and named later executing Beads—never downstream runtime results— and at-least-once, single-source, single-slot, non-HA, same-host named-Linux-filesystem archive limitations are prominent; and
- `boring-cdc-m7-release` is closed with the complete clean-checkout release evidence and is the authoritative root-completion record.

## 18. Explicitly out of scope

- Sources other than PostgreSQL.
- More than one source, publication, or slot per process.
- Generic source/sink plugin APIs or arbitrary destinations.
- Kafka, Kafka Connect, schema registry, or a distributed log dependency.
- Kubernetes, an operator, a distributed control plane, distributed HA, a remote/network mutating API, or a separate control daemon; the owner command endpoint is local and inside the one binary.
- A generic agent platform, vector database, remote agent coordinator, autonomous scheduler, cross-repository memory service, or editable generated knowledge base. Agent tooling remains transparent and repository-local over Git, `br`, contracts, and evidence.
- Automatic source failover/PITR continuity.
- Streamed or two-phase logical transactions unless deliberately added and fully tested before v0.1 scope freezes.
- Applying `TRUNCATE` downstream; v0.1 publishes it only so actual truncation is detected and continuity fails closed.
- Universal DDL migration or online selected-table addition; table-set changes require full re-seed in v0.1.
- Per-table destination quarantine/progress independent of the one destination checkpoint.
- Unsafe update/delete capture without usable identity.
- Remote/object-store/network-filesystem archive commit semantics, support inferred only from generic filesystem API compatibility, or multi-user archive sharing.
- Unlimited replay history or WAL retention.
- Global exactly-once semantics.
- Cross-table or cross-destination atomicity.
- Unmeasured 10,000-table or terabyte-scale claims.
- Provider comparison beyond Estuary; Debezium remains a design reference only.

## 19. Decision register and M0 hard gate

This register is an implementation gate, not a future-ideas list. A row marked `Open`, `Confirm`, or `Owner approval` is blocking when it changes capture durability, source ordering, source safety, destination correctness, replay, public security, or a published capacity claim. **For M0 closure, “validation evidence” means an approved, mechanically usable fixture specification plus the named later Bead that will execute it. Runtime pass/fail results are never prerequisites for M0.** M0 **fails** while a contract, owner, fixture specification, or executing-Bead assignment remains unresolved; it does not wait for M1–M7 implementations. Only planning/decision work may proceed until every blocking row is `Closed` or `Resolved` on that basis.

### 19.1 Auditable repository identity evidence

The public-owner decision was verified on **2026-08-28 UTC** with this reproducible GitHub CLI query:

```bash
gh repo view hachej/boring-cdc \
  --json nameWithOwner,visibility,url \
  --jq '{nameWithOwner,visibility,url}'
```

Recorded output:

```json
{"nameWithOwner":"hachej/boring-cdc","url":"https://github.com/hachej/boring-cdc","visibility":"PUBLIC"}
```

The canonical external evidence is the public repository URL: <https://github.com/hachej/boring-cdc>. A future owner transfer or visibility change invalidates this recorded decision and must update this section and the register. This evidence establishes only GitHub owner/name/visibility; it does not approve the license or any technical decision.

### 19.2 Blocking decisions

| Decision | Accountable owner | Proposed default | Required validation / evidence | Blocking status |
|---|---|---|---|---|
| Public owner | GitHub repository owner (`hachej`) | `hachej/boring-cdc` | Reproducible command, dated recorded output, and canonical URL in section 19.1 | **Resolved** |
| License | Repository owner | Apache-2.0 | Explicit owner approval and committed license text | Owner approval — blocking M0 |
| Compose images/runtime recovery | Implementation lead | Pin Postgres and ClickHouse versions/digests; Compose restarts indefinitely while process-owned persisted policy gates transient reconnect with capped backoff/jitter; Postgres keepalive/check interval bounds reap zombie sessions no later than takeover | Approve Compose/restart/partition/zombie fixture specification; scaffold validation is `m0-scaffold`, runtime timing execution is `m6-failure-matrix` | Open — blocking M0 |
| Archive scope | Repository owner | One archive materializer; test JSONL and Parquet | Architecture sign-off and segment-format fixtures | Confirm — blocking M0 |
| Archive durability/continuity | Implementation lead | Named tested Linux filesystems; directory rename; internal `SEGMENT_READY`; immutable monotonic fence selectors; opaque safe paths; shared `FailurePolicy`; incremental audit with separate journal/self-consistency ranges | Approve crash/retry/path/promotion/audit fixture specification; execution is `m5-segment-set`, `m5-loops`, and `m5-faults` | Open — blocking M0 |
| Shared failure/retry policy | Implementation lead | One versioned closed enum, persisted attempt schedule, deterministic redacted fingerprint, capped backoff/jitter, same-fingerprint suppression, and typed domain recovery hooks | Approve exhaustive transition/golden-vector specification; shared implementation is `m2-capture-runtime`, with ClickHouse/archive adapters consuming it | Open — blocking M0 |
| PostgreSQL versions/transport/options | Implementation lead | Explicit matrix; pinned CopyBoth transport/options; separate creation floor; no streamed/two-phase/binary v0.1; capture/bootstrap role creates only the intent-bound configured slot; frozen decoding-memory/spill and shared failure policy | Approve per-version protocol/bootstrap/burst/failure fixture specification; execution is `m1-decoder`, `m3-bootstrap`, `m2-spool`, and `m2-capture-runtime` | Open — blocking M0 |
| Publication/control relations | Implementation lead | One owned publication; fixed control rows; least privilege; capture/bootstrap can only create the configured intent-bound permanent slot while administration owns drop/publication lifecycle; bounded lock/session policy | Approve privilege/cardinality/ownership/control fixture specification; execution is `m1-control-fixtures`, `m1-preflight`, `m2-ownership`, and `m3-bootstrap` | Confirm — blocking M0 |
| Relation fingerprint/DDL guard | Implementation lead | Stable logical table identity plus full versioned contract; canonically ordered `ACCESS SHARE` locks acquired before snapshot export and held through fence; bounded guard keepalive; conflicting waiter limit invalidates/releases; synchronous changed-`Relation` validation | Per-version finite admitted-DDL conflict matrix, waiter/lock-queue release, timeout/keepalive, export-boundary, idle DDL, immediate DDL→DML, and fence fixtures | Open — blocking M0 |
| Supported keys | Implementation lead | One canonical key equal to effective replica identity; frozen non-null comparable forms/composite arity; complete old/new identity; no absent/partial/TOAST key component | Keyset, update/delete identity, and canonical-key-change fixtures | Open — blocking M0 |
| Supported values | Implementation lead | Concrete canonical encoding/type matrix | Encode/decode/hash corpus and unsupported-type blocks | Open — blocking M0 |
| TOAST | Implementation lead | Tagged patch states plus history reconstruction; reject key change when **any** column is unchanged-TOAST | Explicit-null/unchanged-TOAST/baseline-order/key-change fixtures | Confirm — blocking M0 |
| Additive columns | Implementation lead | Only nullable/no-default additions outside active backfill; otherwise fresh generation | Schema-contract and destination projection tests | Confirm — blocking M0 |
| Event identity/payload conflicts | Implementation lead | Positional IDs, canonical payload hashes, no local/volatile fields in either stable hash, explicit control routing | Golden vectors across run/time plus decoder-conflict and duplicate replay tests | Confirm — blocking M0 |
| SQLite physical journal/durability | Implementation lead | Logical append-only `journal_events` in the one SQLite DB; no payload-log file; per-connection `WAL`/`FULL`; pre-schema incremental auto-vacuum; named Linux filesystems | Fresh/pooled/reopened PRAGMA readback, bounded checkpoint/vacuum, no-auto-full-vacuum, and abrupt-restart tests per filesystem | Confirm — blocking M0 |
| Admission limits/budgets | Implementation lead | Frozen wire/event/transaction limits; one `TxnBuffer`/`MemoryBudget`; conservative runtime-memory and per-filesystem reserve equations | Approve near/over-limit/backpressure/cgroup/filesystem fixture specification; execution is `m2-spool`, `m2-pressure`, and `m6-failure-matrix` | Open — blocking M0 |
| ClickHouse durable acceptance | Implementation lead | Pinned single-node durability model; shared `FailurePolicy`; incremental journal audit with cursor/budget/freshness range; correctness-critical contract fingerprint | Approve durability/retry/drift/audit fixture specification; execution is `m4-durability`, `m4-promotion`, and `m5-loops` | Open — blocking M0 |
| ClickHouse history lifecycle | Repository owner | Tested quota/free-space/temp-part/merge-amplification policy, stable logical table grouping, no composite version narrowing, no deletion of required tombstones/predecessors, explicit retirement grace | Merge/TTL guard, disk/quota/retirement tests, and capacity report | Open — blocking M0 |
| WAL cap | Repository owner | Finite `max_slot_wal_keep_size` plus cap/free-space/WAL-rate/monitor-delay/reaction-reserve headroom equation; local-only unsafe bootstrap/backfill override | Threshold, missing-metric, healthy-capture, and re-seed fixtures | Confirm — blocking M0 |
| Initial/restarted backfill | Implementation lead | Pre-export DDL guard, command-idle exporter, separate capture connection, first-statement imports, lower stitch, published fence, retained-slot recovery after lost snapshot, precise invalidation | Before/during/after snapshot, transient insert/delete, importer/guard/exporter, backend cleanup, and DDL-boundary crash matrix | Confirm — blocking M0 |
| Late destination bootstrap | Implementation lead | Completed retained reconstruction anchor for every promotable destination candidate; suffix-only output is diagnostic | Delayed snapshot/GC/late-destination and established-replay fixtures | Confirm — blocking M0 |
| Promotion/re-seed/table-set change | Implementation lead | Any added table forces new epoch, recreated full publication/slot, full snapshot, fresh full destination generations; ClickHouse uses generation-keyed shared history plus query-time append-only greatest-fence selection; verified external high-fence adoption recovers state loss | Existing-table preservation, delayed lower-fence/same-fence conflict, no-DDL switch, external-ahead adoption, stale-write/promotion-crash, deleted-row, and added-table tests | Confirm — blocking M0 |
| Benchmark oracle | Implementation lead | Exact committed `(mutation_id, canonical_ledger_hash)` set/count/sorted digest; sequence only diagnostic | Legitimate sequence-gap pass and omitted-overwritten-mutation fail fixtures | Confirm — blocking M0 |
| Security exposure | Security owner or repository owner | Read-only loopback status; peer-validated bounded `0600` Unix mutating endpoint under `0700`; idempotent request/result persistence; verified source/sink TLS; strict permissions/redaction; fingerprint/digest-bound destructive confirmation | Config rejection, no-TCP-mutation, peer/direct-writer/lost-response/stale-confirmation, listener integration, and secret-redaction tests | Open — blocking M0 |
| Scale profiles | Repository owner | Named table/row/rate/backfill/event-size/outage profiles | Approve checked-in profile definitions and capacity-run specification; execution/results are `m6-endurance` and `m7-docs` | Open — blocking M0 |

## 20. Main risks and mitigations

| Risk | Consequence | Planned mitigation |
|---|---|---|
| SQLite writer bottlenecks before decoding | Lag and retained WAL grow | Capture-priority writer, bounded commits, named throughput profiles, no unmeasured claims |
| Very large source transaction | Memory/disk exhaustion and long writer hold | Bounded spool, hard byte/event limits, emergency reserve, safe-stop before feedback |
| Exported snapshot held too long | Vacuum/XID/source impact | Snapshot-age and `xmin` monitoring, bounded workers, automatic throttle/end generation |
| Snapshot and WAL append in different local order | Stale destination winners | Separate `journal_seq` and total source version; late-snapshot tests |
| Retained suffix or unfenced snapshot lacks a complete baseline | Incorrect late destination | Anchors require lower and post-copy durable `(LSN, seq)` fences; arbitrary suffix rejected |
| Crash after remote slot creation but before snapshot-token persistence | Creation snapshot is lost and a later snapshot alone could miss transient changes | Invalidate only snapshot generation; prove slot provenance/WAL continuity, drain retained WAL, then existing-slot snapshot; otherwise new epoch/full re-seed |
| Exporter/importer lifecycle is confused | Legitimate exporter release falsely invalidates, dead importer yields partial baseline, or an invalid generation pins feedback forever | Command-idle exporter, separate capture connection, first-statement imports, bounded role timeouts/keepalives, per-assignment completion; invalidation makes snapshot events ineligible and releases its importer feedback gate |
| Sampled or post-export LSN is used for snapshot rows | A post-snapshot WAL mutation can lose to stale snapshot data | Persist only initial consistent-point or last-durable lower stitch before export; use published `pgoutput` fence proof |
| Publication drift removes data from the stream | Connector acknowledges later WAL while selected changes are absent | Dedicated owner prevents ordinary drift; periodic fingerprint blocks forced drift and requires re-seed |
| Advisory-lock or CopyBoth session dies while other connections live | Old and successor runtimes can both dispatch, or an in-place reopen weakens fencing | Either loss is ownership loss in v0.1; dedicated non-pooled lock session, deadline-fit admission for source-mutating dispatch, generation-fenced immutable destination intents/namespaces, fail/exit without in-place reacquire, persisted process-owned reconnect schedule, unlimited supervised restarts, bounded zombie-session reap, successor wait plus full reconciliation |
| Routine CLI opens a second SQLite writer, loses its reply, or repeats a later command | Ownership breaks, a transition executes twice, or deterministic ID reuse silently suppresses valid work | Runtime-owned Unix endpoint, server-issued nonce per dry-run, persisted request/result identity, same-ID conflict detection, prior-run nonterminal abort, stale digest/fingerprint checks, and offline dual-lock ownership |
| Contract-changing DDL races or waits behind a guarded backfill | Snapshot/chunks span schemas, or PostgreSQL lock-queue fairness stalls application traffic behind waiting DDL | Acquire canonical `ACCESS SHARE` guard before export; monitor conflicting waiters; invalidate/release at a short waiter bound; finite per-version admitted-DDL conflict proof; fingerprints remain defense in depth |
| Added table requires full re-seed | Source interruption and full-copy cost | Deliberately accept disruption; dry-run capacity/source impact; preserve old full destinations until new full generations verify/promote |
| Idle source receives unrelated WAL | Slot retention grows although selected tables are quiet | Least-privilege fixed-row published heartbeat update; journal before feedback; alert on heartbeat failure; never acknowledge keepalive `wal_end` |
| Slot creation floor is mistaken for a decoded transaction | Local state can claim durability or feedback it never journaled | Separate creation-floor field, null/equal startup reconciliation, no proactive floor acknowledgement, importer gate before later feedback |
| TOAST patches lose prior explicit values | Silent column corruption | Tagged value states and append-only ClickHouse history; no source repair query; key-change patch rejected |
| ClickHouse merge behavior hides needed history | Incorrect current state | Canonical per-column view and pre/post-merge tests before compaction |
| ClickHouse acknowledgement is not durable | Journal checkpoint/GC can make sink loss permanent | Pinned synchronous/fsync/client-finalization contract, batch intents, crash-after-ack restart verification, block/replay on failure |
| Delayed stale worker/promotion or local state loss changes external live state | SQLite CAS cannot undo an artifact; DDL switching can regress; a fresh store can allocate below the external fence | Generation-keyed history, leases, append-only selectors resolved at query time by unique greatest fence, no DDL switch, durable intents, and maintenance-only verified adoption of the external high-water fence |
| Append-only ClickHouse history grows without bound | Destination quota exhaustion creates hidden source pressure | Tested quota, no unsafe compaction, explicit block/retirement behavior, and published capacity limits |
| Paused destination pins all history | Local pressure and eventual slot loss | Explicit pressure states, detach workflow, honest WAL escalation, finite cap |
| SQLite settings/storage do not honor sync | Loss after acknowledged WAL or host crash | Apply/read back WAL/FULL on every connection, pre-schema incremental auto-vacuum, bounded maintenance, and named filesystem crash evidence; reject weaker/unknown settings |
| Orphan spools consume bounded disk | Repeated crashes eventually force `ENOSPC` | Exclusive-lock scan, deterministic classification/removal, quarantine ambiguous files |
| SQLite corruption/local restore divergence | Loss after already acknowledged WAL | Checksums/integrity checks/startup slot reconciliation; block and re-seed |
| Archive crash, hostile identifier, or path race exposes/overwrites unintended state | Duplicate/missing files, directory escape, symlink swap, or unverified generation becomes readable | Exact-range intent, opaque ASCII paths, descriptor-relative no-follow/exclusive operations, ownership/type/link checks, staged directory, internal `SEGMENT_READY`, immutable promotion-fence selector, named-filesystem fsync, deterministic reconciliation |
| Separate state/archive filesystems defeat a single budget | One filesystem reaches `ENOSPC` despite apparent global headroom | Per-filesystem worst-case reservations, reserves, and near-limit tests |
| Parquet schema evolution/retry is non-reproducible | Delays, ambiguous files, or divergent hashes after crash | Pin crate/format/compression/all metadata; group by table/schema; require byte-identical retry goldens; no generic schema platform |
| Remote source metrics unavailable | Hidden source risk | Explicit stale/unknown condition blocks new bootstrap/backfill unless overridden; healthy capture continues; Compose baseline remains measurable |
| Unauthenticated listener, unsafe local command path, stale confirmation, or error logging leaks/destroys state | Public status gains mutation or a duplicate/stale request applies to changed state | Read-only loopback status, peer-validated bounded Unix owner endpoint, request identity `hash(nonce || canonical payload)`, persisted idempotent results, fingerprint-bound confirmations, strict modes, least-privilege admin loading, and redaction tests |
| Slot invalidation or source timeline change | Continuity cannot be proven | Explicit capture epoch boundary and full re-seed; never fake resume |
| Final-state checksum hides dropped intermediate change | False correctness result after a later overwrite | Publish/materialize mutation ledger and compare exact committed ID/hash sets; sequence gaps are diagnostic; negative omission test |
| Exact-set verification is expensive | Fence verification uses more memory/I/O than max-sequence checks | Stream canonical sorted pairs/digests with bounded external sort and publish verification cost; retain explicit set-difference diagnostics |
| Managed Estuary comparison needs access | Incomplete comparative evidence | Keep comparison runbook separate; publish Boring CDC evidence independently |
| Canonical contract is copied into PLAN, Beads, registries, and generated views | Agents act on plausible stale text or update the wrong owner | Explicit authority hierarchy, stable IDs, one owner, digest-bound effective contracts, generated-view drift validation, and impact-based stale marking |
| Context generation hides an omitted safety clause | Agent makes a locally reasonable but globally unsafe change | Bounded packs declare inclusions/omissions and source digests; oversized normative context fails with an explicit expansion plan and never silently truncates |
| Cached evidence is reused after a relevant change | Gate appears green for an untested contract or environment | Immutable claim ledger with exact applicability digests, explicit compatibility ownership, invalidating-input diagnostics, and tier-owning rerun command |
| Interrupted agent work leaves ambiguous local or external effects | Successor repeats a destructive action or trusts an unverified hypothesis | Schema-valid redacted handoff bound to world state, facts/observations/hypotheses separation, active-intent reconciliation, unsafe-repeat list, and exact next safe command |
| Agent ergonomics grows into a second platform or mutable knowledge base | Scope, maintenance cost, and authority ambiguity expand | Repository-local transparent scripts over Git/`br`/contracts/evidence only; no daemon, scheduler, vector store, remote coordinator, automatic claim, commit, push, or product mutation |
