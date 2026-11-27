#!/usr/bin/env python3
"""Verify a passing archived Article 3 run and recompute its keyed oracle."""

import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile


REPO = Path(__file__).resolve().parents[2]
RAW_QUERIES = {"candidate-rows.jsonl", "source-oracle.tsv"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    return json.loads(path.read_text())


def main(packet):
    manifest = read_json(packet / "manifest.json")
    require(re.fullmatch(r"[0-9a-f]{40}", manifest["git_commit"]), "invalid commit")
    require(re.fullmatch(r"[0-9]+", manifest["ci_run_id"]), "invalid run ID")
    files = manifest["files"]
    inventory = {name + ".gz" if name in RAW_QUERIES else name for name in files}
    require({path.name for path in packet.iterdir()} == inventory | {"README.md", "manifest.json"},
            "packet inventory differs from manifest")
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        for name, digest in files.items():
            require(Path(name).name == name, "invalid manifest path")
            if name in RAW_QUERIES:
                data = gzip.decompress((packet / (name + ".gz")).read_bytes())
                (work / name).write_bytes(data)
            else:
                data = (packet / name).read_bytes()
            require(hashlib.sha256(data).hexdigest() == digest, f"digest differs for {name}")
        obs = read_json(packet / "observation.json")
        require((obs["result"], obs["stage"], obs["exit_code"]) == ("pass", "complete", 0),
                "run did not pass")
        output = work / "oracle.json"
        subprocess.run([
            sys.executable, str(REPO / "scripts/e2e/article3_bootstrap_oracle.py"),
            str(work / "source-oracle.tsv"), str(work / "candidate-rows.jsonl"), str(output),
        ], check=True)
        require(read_json(output) == obs["oracle"] == read_json(packet / "oracle.json"),
                "recomputed keyed oracle differs")
    if obs["scenario"] == "article3-kill-backfill":
        interrupted, restart = obs["interrupted"], obs["restart"]
        before, after = restart["generations"]
        require(obs["interrupted_exit"] == 137 and not interrupted["complete_anchors"]
                and interrupted["capture_epoch"] == restart["capture_epoch"]
                and before["state"] == "invalidated" and after["state"] == "complete"
                and before["generation"] < after["generation"]
                and restart["complete_anchors"][0]["lower_stitch_seq"] == interrupted["journal_head"]
                and restart["clickhouse_checkpoints"] == [restart["journal_head"]],
                "restart did not replace and converge")
        duplicates = obs["duplicates"]
        require(duplicates["physical_rows"] - duplicates["distinct_event_ids"]
                == duplicates["duplicate_rows"], "inconsistent duplicate counts")
    else:
        require(obs["scenario"] == "article3-concurrent-mutation", "unexpected scenario")
        mutation = obs["mutation"]
        require(mutation["result"] == "pass" and mutation["durable_events"] > 0
                and not any(mutation[key] for key in ("missing_keys", "stale_values", "resurrected_deletes"))
                and all(row["postgres"] == row["clickhouse"] == row["expected"] for row in mutation["changes"]),
                "targeted mutation check did not pass")
        convergence = obs["convergence"]
        require(convergence["clickhouse_checkpoints"] == [convergence["journal_head"]],
                "destination checkpoint did not reach journal head")
    print(f"Verified {obs['scenario']} from CI run {manifest['ci_run_id']} at {manifest['git_commit']}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
