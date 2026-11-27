# Article 3 initial backfill

This packet is from the shipped `boring-cdc run --bootstrap` acceptance job. It uses PostgreSQL 17.6, ClickHouse 25.8.2.29, one synthetic table, and a workload that updates, deletes, and inserts rows during initial backfill.

Run `python3 evidence/article3/bootstrap/validate.py` from the repository root to verify the file digests, copy and WAL overlap, checkpoint convergence, and independent keyed comparison. The checked-in candidate and source rows are synthetic fixture data. The manifest identifies the tested code commit and CI attempt.

This packet covers a fresh bootstrap through a completed fence and a caught-up ClickHouse candidate. The killed-backfill recovery, targeted mutation, and source impact runs have separate acceptance criteria.
