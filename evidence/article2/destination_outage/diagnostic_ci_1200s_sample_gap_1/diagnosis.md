# Exact-head CI diagnostic: catch-up between samples

Run [39487346833](https://github.com/hachej/boring-cdc/actions/runs/39487346833), repetition 1, used source commit `304c05dc66e8c751082240bedcdb65cb87fbecca`. The unchanged packet in this directory records a 120-second `docker stop` outage and a direct checkpoint-equals-head probe at 695,540 ms after restart. Source capture continued; the keyed PostgreSQL oracle matched; no business event IDs were lost or duplicated. Peak retained WAL was 236,136 bytes and peak destination lag was 73 events.

The CI job failed its offline validator. The five-second sampler last observed lag 1 immediately after the direct probe found checkpoint equal to head. The journal head continued to advance with control events, so the zero-lag moment fell between samples. The packet generator reported `pass` from the direct probe, while the validator demanded a zero-lag periodic sample. This is an evidence instrumentation disagreement, not proof that the destination failed to catch up.

The follow-up records the exact SQLite head and checkpoint pair from one read transaction when the probe succeeds, then validates that direct observation. It retains the five-second time series and all outage, WAL, oracle, loss, and duplicate assertions. This diagnostic packet remains unchanged and fails its bundled validator.
