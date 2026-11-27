# Source impact and DDL guard

[CI run 45950920225](https://github.com/hachej/boring-cdc/actions/runs/45950920225) tested commit `7c1ab2a149a08c8fd7a2df995ee291564d2c72e5` on PostgreSQL 17.6 with a synthetic one-table workload.

| Arm | Observed result |
| --- | --- |
| Shipped CLI to durable anchor | 200,001 rows in 4,001 chunks over 272.570 seconds. The workload committed 55 writes. There were 2,723 reader measurements and peak slot WAL retention was 930,992 bytes. |
| Plain PostgreSQL stream | 200,000 rows in 0.449 seconds, with one workload commit and five reader measurements. There was no replication slot. |
| DDL during copy | The actual `ALTER TABLE` succeeded after 5.242 seconds. The connector invalidated the generation at 80 chunks, verified guard release, and exited with `M3_DDL_WAITER`. |

The source comparison stops at the durable local anchor or completed plain stream. ClickHouse was deliberately unavailable in this source-only test, and the connector logged that condition after the anchor. One concurrent write was visible in the CLI snapshot, accounting for the row count difference. The short control window yielded five latencies, so its p95 and p99 are descriptive samples with limited comparison value. The packet includes the full one-second measurements.
