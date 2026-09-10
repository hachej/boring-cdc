# Boring CDC event format v1

This document is the public protocol ABI for captured mutation and control events. The machine authorities are `contracts/event/event-format.json`, `contracts/event/event.schema.json`, and `fixtures/m0/event-format/golden-vectors.json`. All integers used by hashes are unsigned big-endian unless explicitly signed. LSNs are PostgreSQL byte positions rendered as unsigned 64-bit integers. Size limits count decoded canonical octets, not JSON characters or base64 text.

The topology remains one PostgreSQL source, publication, and `pgoutput` slot feeding one local SQLite journal and independent ClickHouse and JSONL/Parquet materializers. Capture is at least once. A source position is never acknowledged before the complete transaction and source checkpoint are locally durable.

## Envelope and field grammar

Every object has `schema_version = boring-cdc/event/v1`, `event_type`, lowercase 32-byte SHA-256 `connector_event_id`, positive local `journal_seq`, positive `capture_epoch`, `source_version`, `operation`, and `routing`. The closed JSON Schema rejects unknown fields. `journal_seq` and `captured_at` are transport/audit fields excluded from stable hashes.

A mutation additionally has `logical_table_id`, `relation_fingerprint`, `key_hash`, nonempty `canonical_key`, `mutation_kind`, positional `columns`, and `payload_hash`; its route is `business`. A control event has `control`, `payload_hash`, and route `control`, and cannot carry a business key, relation, or columns. JSON numbers are transport renderings only: implementations must range-check them before fixed-width encoding. Canonical binary values are unpadded base64url in JSON.

`source_version` is exactly `(lsn_u64, origin_rank, transaction_ordinal, mutation_ordinal, connector_event_id)`. The enclosing event supplies the grouping epoch, table, and canonical key; transaction XID and end LSN are source metadata rather than source-version comparison fields. `transaction_ordinal` is the zero-based decoded row ordinal. A mutable-key update expands into mutation ordinal 0 (old-key delete) and ordinal 1 (new-key upsert). Ordinary rows use ordinal 0.

## Canonical value encodings

`contracts/event/event-format.json#value_types` is the admitted v1 matrix. It freezes booleans, signed integers, OID, IEEE floats, arbitrary-precision numeric, date, timestamp, timestamptz, UUID, UTF-8 text-family values, bytea, and one-dimensional arrays of admitted non-array scalars. Type OID and typmod remain part of a value's identity; no silent coercion, locale collation, Unicode normalization, timezone reinterpretation, or JSON-number conversion is allowed.

A canonical scalar hash is SHA-256 over length-framed domain `boring-cdc/canonical-value/v1`, u32-be type OID, i32-be typmod, and canonical bytes. Numeric uses a unique minimal decimal form. Float signed zero is preserved and each width has one quiet-NaN representation. Temporal infinity uses the documented signed extrema. Unsupported OIDs, malformed canonical forms, multidimensional/non-1-lower-bound arrays, or an inclusive size-limit violation block before journal checkpoint and source feedback with `BCDC_EVENT_UNSUPPORTED_VALUE` or `BCDC_EVENT_VALUE_LIMIT`.

Inclusive limits are scalar 1,048,576 bytes, reconstructed row 4,194,304 bytes, and complete event 8,388,608 bytes.

## Identity and total source order

Each hash field is framed by its u64-be octet length, including the domain. Hashes use SHA-256 and lowercase hex.

A WAL event ID hashes domain `boring-cdc/wal-event/v1` and `(capture_epoch u64, source_slot_identity 32 bytes, transaction_end_lsn u64, row_ordinal u64, mutation_ordinal u8)`. `source_slot_identity` hashes, in order, system identifier u64, timeline u32, and UTF-8 database identity, slot name, and plugin under `boring-cdc/source-slot/v1`. A snapshot event ID hashes `boring-cdc/snapshot-event/v1` and `(capture_epoch u64, generation u64, logical_table_id 32 bytes, chunk_id u64, canonical_key)`. The canonical key is u32-be component count followed by each one-byte representation tag (`bool=1`, `int64=2`, `uint64=3`, `bytes=4`, `text=5`), u32-be PostgreSQL type OID, i32-be typmod, and fixed-width or u64-length-prefixed canonical bytes; the physical `key_hash` is not an identity input. Payload bytes, journal sequence, run ID, wall time, ingest time, cache timing, retries, and destination state are excluded from positional identity.

Within one `(capture_epoch, logical_table_id, canonical_key)`, compare `(lsn_u64, origin_rank, transaction_ordinal, mutation_ordinal, connector_event_id)` lexicographically, with snapshot rank 0 and WAL rank 1. A snapshot has transaction and mutation ordinals zero. Versions from different capture epochs or correctness groups are incomparable; they are never sorted against each other. Same ID and same version/payload hash is an accepted duplicate. Same ID with any different stable position or payload hash is `BCDC_EVENT_PAYLOAD_CONFLICT`, blocking checkpoint and feedback until inspection and correction or confirmed re-seed.

## Relation and row identity

`logical_table_id` is stable across relation OID/schema versions: hash `boring-cdc/logical-table/v1` over system identifier u64 and UTF-8 database identity, schema name, and table name, in that order. A `relation_fingerprint` hashes the exact binary `relation_contract_encoding` in `contracts/event/event-format.json` under `boring-cdc/relation-schema/v1`. Nested expression, index, partition, and publication definitions hash under `boring-cdc/relation-component/v1` over a UTF-8 component-kind discriminator and a canonical catalog-deparsed UTF-8 definition: identifiers remain UTF-8, keywords are lowercase, tokens use one ASCII space, comments/trailing semicolons are absent, and numeric literals are minimal decimal. That closed encoding includes stable table plus versioned relation identity; every physical `attnum`, logical/physical order, name and dropped status; OID, typmod, collation and nullability; canonical hashes (or explicit empty values) for default/generated/identity expressions; primary/unique effective key, replica-identity mode/index; partition root/leaf routing, key and bounds; and publication membership/actions/row filter/column projection. Physical columns are ordered by ascending `attnum`; every optional input has the contract's explicit empty representation.

Canonical row correctness identity is exactly `(capture_epoch, logical_table_id, canonical_key)`. It is distinct from relation/schema identity and remains the grouping key for snapshot keysets, WAL update/delete identity, checksums, and destination state. `key_hash` hashes the same typed, length-safe effective replica-identity encoding under `boring-cdc/physical-key/v1`; it is only a physical sorting/sharding aid and cannot replace or establish correctness identity. Supported key types are listed in the contract, maximum arity is 32, and each canonical component is at most 1,024 bytes. Empty, null, absent, partial, oversized, or unchanged-TOAST key states are forbidden.

The payload hash binds, in order, enclosing capture epoch, the exact source-version numeric tuple, positional ID, relation fingerprint, canonical key plus key hash, one-byte operation tag (`snapshot=0`, `insert=1`, `update=2`, `delete=3`), empty-or-canonical `before_key`, mutation kind, and every column's u32-be positional ID, state tag, admitted type identity, and canonical bytes under `boring-cdc/mutation-payload/v1`. Thus changing operation or old-key semantics changes the conflict hash. It excludes journal sequence, timestamps, run IDs, retries, caches, and destination-local fields.

## TOAST and schema evolution

Every positional column has exactly one state: `explicit_value`, `explicit_null`, `unchanged_toast`, or `absent_for_schema`; tags are 3, 1, 2, and 0 respectively. Explicit null never means unchanged. Unchanged TOAST is a patch requiring an earlier eligible full baseline/predecessor in the same row identity and capture epoch. History must be retained until all consumers that can encounter the patch no longer need it. No source repair query is permitted. A canonical-key change combined with **any** unchanged-TOAST column is rejected before checkpoint/feedback as `BCDC_EVENT_KEY_CHANGE_UNCHANGED_TOAST`.

The sole compatible in-epoch relation change is adding a nullable, non-generated column with no default while no active backfill exists. Older rows project `absent_for_schema` to destination null without pretending an explicit source null; the synchronous changed-Relation creates a new relation fingerprint. Any other change, or that addition during active backfill, invalidates the active generation and blocks destinations pending a fresh generation/full re-seed.

## Control routing

Published fixed-row heartbeat updates and observed snapshot/promotion fences are journaled control events. They participate in durable source progress and fence state but route only to the control path, never to user ClickHouse tables or archive business partitions. Heartbeats may advance feedback only after their transaction is durable. Fences become eligible only after their named generation/baseline conditions are durable. `TRUNCATE` is published as `truncate_observed`, then continuity fails closed; v1 never applies a downstream truncate.

Control IDs use the same positional WAL identity grammar. A control payload hash uses length-framed domain `boring-cdc/control-payload/v1` and `(connector_event_id, kind UTF-8, transaction_end_lsn, optional generation)`, where absent generation is an empty field. Control payloads name a stable kind, transaction end LSN, and optional generation. Raw source relation names and payload values are not control metadata.

## Golden vectors

`fixtures/m0/event-format/golden-vectors.json` is normative. Its `identity_primitives` derive source-slot, logical-table, relation-component, and relation fingerprints from raw inputs rather than trusting precomputed intermediates. Exact IDs cover WAL identity, snapshot identity, same-key total order, mutable-key expansion, canonical values, unsupported values, four TOAST/schema states, forbidden key-change TOAST, heartbeat routing, and fence routing. The fixed fixture IDs are:

- `SCN-M0-EVENT-WAL-IDENTITY`, `SCN-M0-EVENT-SNAPSHOT-IDENTITY`
- `SCN-M0-EVENT-SAME-KEY-ORDER`, `SCN-M0-EVENT-KEY-CHANGE-ORDER`
- `SCN-M0-EVENT-TYPE-CANONICAL`, `SCN-M0-EVENT-TYPE-UNSUPPORTED`, `SCN-M0-EVENT-LIMIT-BOUNDARIES`
- `SCN-M0-EVENT-TOAST-STATES`, `SCN-M0-EVENT-TOAST-KEY-CHANGE-BLOCK`
- `SCN-M0-EVENT-ADDITIVE-OUTSIDE-BACKFILL`, `SCN-M0-EVENT-ADDITIVE-DURING-BACKFILL`
- `SCN-M0-EVENT-RETRY-STABILITY`
- `SCN-M0-EVENT-CONTROL-HEARTBEAT`, `SCN-M0-EVENT-CONTROL-FENCE`

Later execution belongs to `boring-cdc-m1-ordering`, `boring-cdc-m1-decoder`, and `boring-cdc-m2-journal`. M0 validates the specification and exact vectors; it does not claim runtime results.

## Compatibility and versioning

The envelope/schema, hash domains, field order/width, tags, canonical type matrix, size limits, identity inputs, source-order tuple, control routes, and failure outcomes are ABI. Additive optional envelope fields require a new schema minor version only after old readers are proven to ignore them; this v1 schema deliberately rejects unknown fields, so current v1 additions require a new schema URI/version. Any change to canonical bytes, hash inputs/domain/framing, required fields, meanings, routing, or ordering requires a new major event format and explicit migration or new capture epoch/full re-seed. Hash/version comparison across capture epochs is forbidden.

The recommended constants were confirmed by the owner on 2026-09-10 through cards `5a994cfd` and `765bd3b2`; decision Bead state remains separately governed.
