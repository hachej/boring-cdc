# Article 2 capture crash packet

The [manifest](manifest.json) binds the [redacted observation](observation.json) to CI run `38750001126` at source commit `fa054019e64387467509e0b01d6027033f343fec`. The job ran `scripts/e2e/m4_live_journal_convergence.sh`, which invoked `scripts/acceptance/durable_simple_case.sh` against pinned PostgreSQL 17.6. The capture observation records SIGKILL exit 137, six user transactions, eight journal events at the oracle point, five feedback samples, source-oracle equality, and a gap-free journal. It contains hashes in place of source transaction IDs and row values.

From the repository root, run `python3 scripts/validate/article2_capture_packet.py` to check the retained bytes, source-file hashes, required verdicts, and redaction. To repeat the live scenario, run `TMPDIR=/var/tmp scripts/e2e/m4_live_journal_convergence.sh` with Docker and the pinned Rust toolchain. A new live run produces different source IDs and oracle hashes; it does not reproduce the retained observation byte for byte.

This packet proves the M2 source-to-journal boundary. The same CI job also ran a separate live ClickHouse component probe, but its synthetic selector did not consume the M3 snapshot. An anchored ClickHouse candidate from the same stream and an external Estuary result remain pending.
