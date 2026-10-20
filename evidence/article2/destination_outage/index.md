# Article 2 destination outage evidence

Two exact-head CI repetitions at commit `77e49372740c592bed414a1b62e3800b0e7b24ad` passed the bundled offline validator: [run 39490029321](https://github.com/hachej/boring-cdc/actions/runs/39490029321). The shipped `boring-cdc run` process captured from PostgreSQL 17.6 and delivered to ClickHouse 25.8.2.29. The test stopped the ClickHouse container with `docker stop` for 120 seconds while 24 PostgreSQL transactions continued at five-second intervals. Five-second samples recorded the replication slot, retained WAL, journal head, destination checkpoint and lag, disk usage, and `status --json`. A direct SQLite read transaction recorded head equal to checkpoint at catch-up. The candidate keyed query matched the independent PostgreSQL oracle.

| Packet | Peak retained WAL | Peak destination lag | Catch-up after restart | Business event rows / distinct IDs | Lost IDs | Duplicate rows | Operator steps |
|---|---:|---:|---:|---:|---:|---:|---|
| [Repetition 1](repeat_1/README.md) | 287,888 bytes | 75 events | 803.934 s | 57 / 57 | 0 | 0 | None |
| [Repetition 2](repeat_2/README.md) | 288,720 bytes | 75 events | 775.900 s | 57 / 57 | 0 | 0 | None |

In both repetitions, the slot stayed active, `restart_lsn` advanced repeatedly during the outage, retained WAL stayed below the declared 256 MiB bound, and status reported degraded with `DESTINATION_LAG`. Each packet contains `manifest.json`, `observation.json`, the source oracle, `README.md`, and `validate.py`. Run `python3 validate.py observation.json` inside either packet directory to verify it offline. The manifests bind the code commit, source inputs, configuration digest, image digests, observation, oracle, and validator.

The [original failed packet](README.md) and diagnostic packets remain unchanged. The initial policy exhausted transient destination retries. Later CI runs reached catch-up, but their validator required a five-second sample to coincide with the transient zero-lag point; those failures are retained under `diagnostic_ci_1200s_*`. The successful runs record the direct head/checkpoint pair in one read transaction while preserving the periodic samples and all other assertions. The explicit re-arm command was not needed in either successful outage repetition; its owner-fenced behavior is covered by the M2 tests and contract validator.

Managed Estuary comparison is unavailable. Sponsorship disclosure, client review window, and publication approval remain unverified.
