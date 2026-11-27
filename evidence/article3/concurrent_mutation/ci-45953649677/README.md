# Mutations during backfill

The fixture committed updates and deletes in copied, copying, and pending ranges. The journal recorded all 12 events in the targeted transaction. The ClickHouse checkpoint and journal head both reached 316. ClickHouse matched all 199,994 final PostgreSQL keys, including the expected results for every targeted key. [CI run 45953649677](https://github.com/hachej/boring-cdc/actions/runs/45953649677) · tested commit `1530af7d83a1678258b78f387cfe7652031726cf`

Run `python3 evidence/article3/validate_backfill.py evidence/article3/concurrent_mutation/ci-45953649677` from the checkout root. It decompresses the two query outputs and checks their original CI digests before recomputing the oracle.
