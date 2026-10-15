# Article 2 M3 anchor packet

This directory preserves the redacted `m3-bootstrap-writer-handoff` artifact from [CI run `38748581460`](https://github.com/hachej/boring-cdc/actions/runs/38748581460) at source commit `6aaadad690b78197239646d16d7d0c19912621e7`. The original `sha256.txt` covers every copied artifact file. `provenance.json` binds the run, job, artifact name, and original manifest digest without changing those copied bytes.

Run `python3 scripts/validate/article2_anchor_packet.py` from the repository root to check the inventory and required verdicts. To repeat the live component scenario with Docker and the pinned Rust toolchain, run `TMPDIR=/var/tmp scripts/e2e/m3_bootstrap.sh`. The CI packet records two clean PostgreSQL 17.6 attempts. Both completed the guarded imported snapshot, canonical schema proof, original CopyBoth capture, durable M2 post-copy fence, and SQLite anchor.

This component has no ClickHouse snapshot import. Its anchor cannot be combined with the separate M4 selector probe into a same-stream destination claim.
