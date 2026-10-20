# Exact-head CI diagnostic: catch-up between samples

Run [39487363894](https://github.com/hachej/boring-cdc/actions/runs/39487363894), repetition 2, used source commit `b7f4d53a151441022d669f864ff12782602b275c`. The unchanged packet records a direct catch-up probe 745,384 ms after ClickHouse restart, peak retained WAL of 288,032 bytes, peak destination lag of 73 events, an exact PostgreSQL keyed oracle match, zero lost IDs, zero duplicates, and no operator steps.

The bundled offline validator failed because the five-second sampler did not observe the brief zero-lag moment that the direct probe asserted. Its last sample had lag 1 as the journal head continued to advance. The follow-up records and validates the direct head/checkpoint pair in one read transaction. This packet remains unchanged and fails its bundled validator.
