# Article 2 selector reader probe

The [manifest](manifest.json) binds the [redacted observation](observation.json) to the exact-head [CI run 38857713727](https://github.com/hachej/boring-cdc/actions/runs/38857713727), job `anchored_candidate`. It ran twice on PostgreSQL 17.6 and ClickHouse 25.8.2.29, using a fresh ClickHouse instance per attempt.

After the same-stream M3 snapshot and M2 tail were verified, the test harness inserted a selector for that candidate. The untouched canonical query matched the candidate and independent PostgreSQL oracle. Repeating the identical selector and inserting a lower fence kept that generation live. Inserting a conflicting row at the greatest fence made the query fail with `BCDC_CH_INTEGRITY_CONFLICT`.

The selector is a direct test insertion with a synthetic candidate digest. This packet proves reader visibility and conflict handling on the tested one-relation stream. It does not exercise production promotion intent, lease fencing, candidate admission, or restart reconciliation. Those remain owned by `boring-cdc-m4-promotion`.

Validate offline with `python3 scripts/validate/article2_selector_reader_packet.py`.
