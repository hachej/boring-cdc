# Local diagnostic: recovery observation ended before convergence

This packet is a real failed 120-second ClickHouse outage run at Git `13429a83` with stable host ports. Capture continued, retained WAL peaked at 38,800 bytes, and status remained degraded. At the end of the 120-second recovery allowance, the destination checkpoint had reached sequence 74 but still lagged the journal by 48 events; the oracle consequently did not match. This packet remains failed.

The checkpoint was advancing after ClickHouse restarted, so the next run allows up to 600 seconds to measure eventual catch-up while preserving the same outage, workload, exact oracle, and zero-loss assertions. The packet also reported `DESTINATION_RETRY_EXHAUSTED` from a historical disarmed failure row. The packet generator will now flag only an armed exhausted failure; this diagnostic packet is unchanged.
