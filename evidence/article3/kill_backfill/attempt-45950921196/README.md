# Killed backfill and failed restart

[CI run 45950921196](https://github.com/hachej/boring-cdc/actions/runs/45950921196) ran the shipped CLI at `94c4f27d0b3a45e7b1a4fc582c50cd8995ee0b47` against the fixed one-table workload. The connector was killed with 900 of 2,000 chunks complete. At interruption the journal held 66 durable business events, and the initial generation had no complete anchor.

The same-slot, same-configuration restart exited with `M3_BOOTSTRAP_RECOVERY_REQUIRED`. The journal head stayed at 87. The run observed neither a replacement generation nor a destination checkpoint. This is a failed recovery attempt, so keyed convergence and duplicate counts were unavailable.
