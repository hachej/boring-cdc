#!/usr/bin/env python3
"""Check that pressure packets agree with their captured test output."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/lib"))
from m2_pressure_component import CAPTURE_COMMAND, COMMAND, PRIVATE, SEED, capture_observation, implementation_digest, observations, sha  # noqa: E402


def main():
    mode = sys.argv[1]
    if mode not in {"e2e", "fault"}:
        raise SystemExit("usage: m2_pressure.py e2e|fault")
    scenario = "SCN-M2-PRESSURE-COMPONENT" if mode == "e2e" else "SCN-M2-PRESSURE-READER-CONTENTION"
    path = ROOT / "artifacts/boring-cdc-m2-pressure" / scenario / SEED
    manifest = json.loads((path / "manifest.json").read_text())
    config = json.loads((path / "config.json").read_text())
    assert config["physical_fill_max_bytes"] == (128 << 20 if mode == "fault" else 0)
    expected = {case["test"] for case in json.loads((ROOT / "contracts/m2/pressure-cases.json").read_text())["cases"]}
    assert manifest["result"]["status"] == "pass"
    assert manifest["result"]["runtime_observed"] is True
    assert manifest["tier_proof"]["clean_environment"] is False
    assert manifest["source_preservation"]["before_sha256"] == implementation_digest()
    assert manifest["source_preservation"]["after_sha256"] == implementation_digest()
    assert len(manifest["commands"]) == (4 if mode == "fault" else 2)
    probes = []
    capture_probes = []
    for command in manifest["commands"]:
        stdout = ROOT / command["stdout_path"]
        stderr = ROOT / command["stderr_path"]
        assert sha(stdout.read_bytes()) == command["stdout_sha256"]
        assert sha(stderr.read_bytes()) == command["stderr_sha256"]
        assert command["exit_code"] == 0
        assert not PRIVATE.search(stdout.read_bytes())
        assert not PRIVATE.search(stderr.read_bytes())
        if command["argv"] == " ".join(COMMAND):
            probes.append(observations(stdout.read_bytes(), expected))
        elif mode == "fault" and command["argv"] == " ".join(CAPTURE_COMMAND):
            capture_probes.append(capture_observation(stdout.read_bytes()))
        else:
            raise AssertionError("unexpected pressure evidence command")
    assert len(probes) == 2 and len(capture_probes) == (2 if mode == "fault" else 0)
    assert probes[0] == probes[1]
    if mode == "fault":
        assert capture_probes[0] == capture_probes[1]
    observed = probes[0]
    selected = (observed["runtime_service"] if mode == "e2e"
                else {"reader_contention": observed["reader_contention"], "wal_recycling": observed["wal_recycling"], "capture_hard_stop": capture_probes[0]})
    timeline = ([observed["pin_gc"], observed["runtime_service"]] if mode == "e2e"
                else [observed["reader_contention"], observed["wal_recycling"], capture_probes[0], observed["pin_gc"]])
    assert json.loads((path / "state/after.json").read_text()) == selected
    assert json.loads((path / "fault-timeline.json").read_text()) == timeline
    events = [json.loads(line) for line in (path / "logs/boring-cdc.jsonl").read_text().splitlines()]
    assert [event["phase"] for event in events] == [item["probe"] for item in timeline]
    for event, item in zip(events, timeline):
        assert event["evidence_digest"] == sha((json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n").encode())
    inventory = {}
    for line in (path / "sha256.txt").read_text().splitlines():
        digest, relative = line.split("  ", 1)
        inventory[relative] = digest
    for relative, digest in inventory.items():
        assert sha((path / relative).read_bytes()) == digest
    assert set(inventory) == {item.relative_to(path).as_posix() for item in path.rglob("*") if item.is_file()} - {"sha256.txt"}
    print(json.dumps({"mode": mode, "status": "pass", "observed_probes": sorted(observed) + (["capture_hard_stop"] if mode == "fault" else [])}, sort_keys=True))


if __name__ == "__main__":
    main()
