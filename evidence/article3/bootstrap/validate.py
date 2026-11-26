#!/usr/bin/env python3
"""Verify the saved bootstrap acceptance packet and recompute the keyed oracle."""

import hashlib
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

packet = Path(__file__).resolve().parent
repo = packet.parents[2]


def require(condition, code):
    if not condition:
        raise ValueError(code)


def main():
    manifest = json.loads((packet / "manifest.json").read_text())
    require(re.fullmatch(r"[0-9a-f]{40}", manifest["git_commit"]), "E_COMMIT")
    require(re.fullmatch(r"[0-9]+", manifest["ci_run_id"] or ""), "E_CI_RUN")
    expected = {"backfill-status.json", "candidate-rows.jsonl", "checkpoint.json",
                "observation.json", "oracle.json", "progress.json", "source-oracle.tsv"}
    require(set(manifest["files"]) == expected, "E_INVENTORY")
    for name, digest in manifest["files"].items():
        require(hashlib.sha256((packet / name).read_bytes()).hexdigest() == digest, "E_DIGEST_" + name)

    obs = json.loads((packet / "observation.json").read_text())
    require(obs["result"] == "pass" and obs["stage"] == "complete" and obs["exit_code"] == 0, "E_VERDICT")
    before, after = (obs["overlap"][point] for point in ("before", "after"))
    total = before["chunks_total"]
    require(
        0 < before["chunks_done"] < after["chunks_done"] < total == after["chunks_total"]
        and before["durable_business_events"] < after["durable_business_events"]
        and before["complete_anchor"] is None and after["complete_anchor"] is None,
        "E_OVERLAP",
    )
    convergence = obs["convergence"]
    rows, fence_seq = convergence["complete_anchor"]
    head = convergence["journal_head"]
    require(
        rows > 0 and convergence["chunks_done"] == convergence["chunks_total"] == total
        and 0 < fence_seq <= head and convergence["clickhouse_checkpoints"] == [head],
        "E_CONVERGENCE",
    )
    with tempfile.TemporaryDirectory() as work:
        result = Path(work) / "oracle.json"
        subprocess.run(
            [sys.executable, str(repo / "scripts/e2e/article3_bootstrap_oracle.py"),
             str(packet / "source-oracle.tsv"), str(packet / "candidate-rows.jsonl"), str(result)],
            check=True,
        )
        oracle = json.loads(result.read_text())
        require(oracle == obs["oracle"] == json.loads((packet / "oracle.json").read_text()), "E_ORACLE")
    print(f"Verified bootstrap packet from CI run {manifest['ci_run_id']} at {manifest['git_commit']}")


if __name__ == "__main__":
    main()
