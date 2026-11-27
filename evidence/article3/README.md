# Article 3 evidence

The initial backfill copied 10,000 rows in 400 chunks. WAL capture continued during the copy. The durable journal recorded three additional business events as completed chunks advanced from 3 to 111. After the durable fence at journal sequence 20, the ClickHouse checkpoint caught up to the journal at sequence 29. The keyed ClickHouse candidate matched PostgreSQL across all 10,000 final keys. The packet contains the executed command, sample values, independent oracle, and an offline validator. [Bootstrap packet](bootstrap/README.md) · [CI run 45839078649](https://github.com/hachej/boring-cdc/actions/runs/45839078649) · tested commit `8c67d9068de3c90e8f3e5b78a9e11905c024a10a`

The killed-backfill run ended at 900 of 2,000 chunks. The restart exited with `M3_BOOTSTRAP_RECOVERY_REQUIRED` before any replacement generation appeared. [Failed recovery packet](kill_backfill/attempt-45950921196/README.md) · [CI run 45950921196](https://github.com/hachej/boring-cdc/actions/runs/45950921196)

The source impact run completed the bounded backfill in 272.570 seconds, and an actual `ALTER TABLE` succeeded after 5.242 seconds once the connector invalidated the generation and released its guard. The plain stream took 0.449 seconds and yielded just five latency samples. [Source impact packet](source_impact/attempt-45950920225/README.md) · [CI run 45950920225](https://github.com/hachej/boring-cdc/actions/runs/45950920225)

The steady initial bootstrap also passed on the updated runtime at `000b0ca3e7ec6fdfa6fc508506eed06519b731da`. [Steady packet](bootstrap/ci-45951142035-steady/observation.json) · [CI run 45951142035](https://github.com/hachej/boring-cdc/actions/runs/45951142035)

The same CI run stopped ClickHouse before baseline delivery and left the completed anchor with no destination checkpoint. After ClickHouse returned, an ordinary `run` on the same configuration recovered baseline delivery. The final ClickHouse checkpoint and journal head were both 123, and all 10,000 candidate keys matched PostgreSQL. [Baseline restart packet](bootstrap/ci-45951142035-baseline-restart/observation.json)

The replacement generation test passed on a 200,000-row workload. After `SIGKILL`, `boring-cdc run --bootstrap` with the same configuration replaced the interrupted generation. The final ClickHouse state matched all PostgreSQL keys, with zero duplicate physical event rows. [Passing recovery packet](kill_backfill/ci-45953464555/README.md)

The targeted mutation test passed with updates and deletes in copied, copying, and pending ranges. All 199,994 final keys matched PostgreSQL, and the journal contained all 12 targeted events. [Passing mutation packet](concurrent_mutation/ci-45953649677/README.md) · [Earlier fixture failure](concurrent_mutation/attempt-45951997029/README.md)
