# Restart after a killed backfill

The connector was killed after completing 900 of 2,000 chunks. The same-slot restart with `boring-cdc run --bootstrap` invalidated the interrupted generation and copied the table in a new generation. It kept capture epoch 1 and used the interrupted journal head at sequence 91 as the lower stitch boundary.

The replacement reached a complete anchor in 249.264 seconds. The ClickHouse checkpoint and journal head both reached 343. ClickHouse matched all 200,000 PostgreSQL keys, and all 200,147 physical event rows had distinct event IDs. [CI run 45953464555](https://github.com/hachej/boring-cdc/actions/runs/45953464555) · tested commit `3b27454a598eee7ba5b38ce8a77e741e2cb22fbe`

Run `python3 evidence/article3/validate_backfill.py evidence/article3/kill_backfill/ci-45953464555` from the checkout root. It decompresses the two query outputs and checks their original CI digests before recomputing the oracle.
