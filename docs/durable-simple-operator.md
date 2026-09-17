# Durable simple-stream operator path

This is the bounded one-source, one-publication, one-slot, one-binary, one-SQLite path. It was exercised against the repository-pinned PostgreSQL 17.6 image. It does not configure backfill or a destination.

## 1. Start with an empty operator database

The PostgreSQL server must use `wal_level=logical`, PostgreSQL 17.6, and a finite `max_slot_wal_keep_size`. Create an empty database as its owner, then execute the checked-in prerequisite SQL as that owner (or a superuser):

```sh
export PGHOST=127.0.0.1 PGPORT=5432 PGDATABASE=boring_cdc PGUSER=boring_cdc
psql -X -v ON_ERROR_STOP=1 \
  -v admin_password="$BORING_CDC_ADMIN_PASSWORD" \
  -v runtime_password="$BORING_CDC_RUNTIME_PASSWORD" \
  -v control_password="$BORING_CDC_CONTROL_PASSWORD" \
  -v application_password="$BORING_CDC_APPLICATION_PASSWORD" \
  -f scripts/setup/durable_simple_prerequisites.sql
```

The script creates four non-superuser login roles and the selected `public.orders` relation. `boring_cdc_admin` owns that relation and has database `CREATE`; `boring_cdc_runtime` has `REPLICATION` plus read-only access to the selected relation; `boring_cdc_control_writer` starts with no table privileges; and `boring_cdc_app` has only application DML on `public.orders`. Use distinct generated passwords. The SQL is rerunnable, but rotating a password requires updating its secret before the next command.

Place `boring-cdc.toml` in the working directory. Its selected relation must be `public.orders`, and its publication and slot must match the DSNs below. Create the state and spool directories with owner-only permissions:

```sh
install -d -m 0700 state state/spool
PG_ADMIN_DSN="postgresql://boring_cdc_admin:${BORING_CDC_ADMIN_PASSWORD}@127.0.0.1:5432/boring_cdc?sslmode=disable"
unset PG_ADMIN PG_RUNTIME PG_CONTROL
```

Use a verified TLS connection instead of `sslmode=disable` outside an isolated local exercise.

## 2. Initialize, bootstrap, then run

Run these commands in this order from the directory containing `boring-cdc.toml`:

```sh
boring-cdc init --dry-run --json > init-plan.json
INIT_TOKEN=$(python3 -c 'import json; print(json.load(open("init-plan.json"))["data"]["confirm_token"])')
export PG_ADMIN="$PG_ADMIN_DSN"
boring-cdc init --confirm --confirm-token "$INIT_TOKEN" --json
unset PG_ADMIN
export PG_RUNTIME="postgresql://boring_cdc_runtime:${BORING_CDC_RUNTIME_PASSWORD}@127.0.0.1:5432/boring_cdc?sslmode=disable"
export PG_CONTROL="postgresql://boring_cdc_control_writer:${BORING_CDC_CONTROL_PASSWORD}@127.0.0.1:5432/boring_cdc?sslmode=disable"
export CH_RUNTIME="https://unused.invalid" # required config reference; not contacted by this capture-only path
boring-cdc run --bootstrap
boring-cdc run
```

- `init --dry-run` requires the public configuration but no administration DSN. It creates only an expiring local confirmation plan; it does not connect to PostgreSQL, initialize SQLite, or create a slot.
- `init --confirm` requires `PG_ADMIN`, exclusive local/source ownership, the exact prerequisite roles/table, and no configured slot. It creates and verifies `boring_cdc_control.heartbeat`, `boring_cdc_control.capture_fences`, their singleton rows, the configured publication, exact control-writer column privileges, and the SQLite source identity. It does **not** create the logical slot. Drop `PG_ADMIN` from the environment immediately afterward.
- `run --bootstrap` requires `PG_RUNTIME`, the initialized SQLite identity, and the absent configured slot. It durably records a bootstrap intent before creating that one permanent `pgoutput` slot with its exported snapshot. Keep it running while the bootstrap snapshot is imported; stopping before import completion deliberately invalidates that snapshot, but does not silently discard the slot.
- `run` requires `PG_RUNTIME`, the initialized identity, the configured publication and slot, and completed/reconciled bootstrap state. It reads the configured publication through the configured slot, commits each complete source transaction to SQLite, and only then sends PostgreSQL feedback. `PG_ADMIN` is neither required nor retained. `PG_CONTROL` is reserved for bounded heartbeat/fence updates and has no publication or slot authority.

Each stage fails closed. Publication errors name the failed check: relation set, owner, or the individual `insert`, `update`, `delete`, or `truncate` publish flag. Control-role errors distinguish missing privileges from excess privileges or role membership. Correct the named prerequisite; do not alter a live publication to work around a mismatch.
