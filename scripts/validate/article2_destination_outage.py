#!/usr/bin/env python3
"""Audit a measured Article 2 destination outage observation without contacting services."""

import argparse
import json
import sys
from pathlib import Path


def lsn(value):
    if not isinstance(value, str):
        raise ValueError("LSN is not text")
    if "/" in value:
        high, low = value.split("/", 1)
        return (int(high, 16) << 32) | int(low, 16)
    return int(value, 16)


def require(condition, message, errors):
    if not condition:
        errors.append(message)


def validate(observation):
    errors = []
    require(observation.get("schema_version") == "article2-destination-outage/v1", "schema version", errors)
    require(observation.get("git_commit") and len(observation["git_commit"]) == 40, "exact Git commit", errors)
    require(observation.get("images") == {"postgres": "17.6", "clickhouse": "25.8.2.29"}, "pinned images", errors)
    profile = observation.get("profile", {})
    require(profile.get("sample_interval_seconds") == 5, "five-second sampling", errors)
    require(profile.get("outage_seconds", 0) >= 120, "fixed outage window", errors)
    require(profile.get("workload_transactions", 0) > 0, "measured workload", errors)
    timeline = observation.get("timeline", {})
    stopped = timeline.get("clickhouse_stopped_ms")
    restarted = timeline.get("clickhouse_restarted_ms")
    caught_up = timeline.get("caught_up_ms")
    require(all(isinstance(x, int) for x in (stopped, restarted, caught_up)), "event timestamps", errors)
    if all(isinstance(x, int) for x in (stopped, restarted, caught_up)):
        require(0 <= stopped < restarted <= caught_up, "event timestamp order", errors)
        require(restarted - stopped >= profile.get("outage_seconds", 0) * 1000, "outage duration", errors)
        require(timeline.get("catch_up_ms") == caught_up - restarted, "catch-up duration", errors)
    samples = observation.get("samples", [])
    require(len(samples) >= 3, "time series", errors)
    previous_ms = None
    outage = []
    recovery = []
    for index, sample in enumerate(samples):
        elapsed = sample.get("elapsed_ms")
        require(isinstance(elapsed, int) and elapsed >= 0, f"sample {index} time", errors)
        if isinstance(elapsed, int) and previous_ms is not None:
            require(0 < elapsed - previous_ms <= 8000, f"sample {index} sampling gap", errors)
        if isinstance(elapsed, int):
            previous_ms = elapsed
        slot = sample.get("slot", {})
        journal = sample.get("journal", {})
        destination = sample.get("destination", {})
        sizes = sample.get("sizes", {})
        status = sample.get("status", {})
        for name in ("restart_lsn", "confirmed_flush_lsn"):
            try:
                lsn(slot[name])
            except (KeyError, TypeError, ValueError):
                errors.append(f"sample {index} {name}")
        try:
            lsn(journal["head_lsn"])
        except (KeyError, TypeError, ValueError):
            errors.append(f"sample {index} journal head LSN")
        require(isinstance(slot.get("active"), bool), f"sample {index} slot active", errors)
        for name, value in (
            ("retained_bytes", slot.get("retained_bytes")),
            ("journal_head_seq", journal.get("head_seq")),
            ("checkpoint_seq", destination.get("checkpoint_seq")),
            ("lag_events", destination.get("lag_events")),
            ("sqlite_bytes", sizes.get("sqlite_bytes")),
            ("spool_bytes", sizes.get("spool_bytes")),
        ):
            require(isinstance(value, int) and value >= 0, f"sample {index} {name}", errors)
        if isinstance(journal.get("head_seq"), int) and isinstance(destination.get("checkpoint_seq"), int):
            require(destination["checkpoint_seq"] <= journal["head_seq"], f"sample {index} checkpoint ahead", errors)
            require(destination.get("lag_events") == journal["head_seq"] - destination["checkpoint_seq"], f"sample {index} lag arithmetic", errors)
        require(isinstance(status.get("overall_health"), str), f"sample {index} health", errors)
        require(isinstance(status.get("conditions"), list), f"sample {index} conditions", errors)
        if isinstance(elapsed, int) and isinstance(stopped, int) and isinstance(restarted, int):
            if stopped <= elapsed < restarted:
                outage.append(sample)
            elif elapsed >= restarted:
                recovery.append(sample)
    require(bool(outage), "outage samples", errors)
    require(bool(recovery), "recovery samples", errors)
    if outage:
        require(all(s["slot"].get("active") is True for s in outage), "slot active during outage", errors)
        require(any(s["destination"].get("lag_events", 0) > 0 for s in outage), "destination lag observed", errors)
        require(all(s["status"].get("overall_health") != "healthy" for s in outage if s["destination"].get("lag_events", 0) > 0), "lagging destination must not claim healthy", errors)
        require(all(
            any(condition.get("reason") == "DESTINATION_LAG" for condition in s["status"].get("conditions", []))
            for s in outage if s["destination"].get("lag_events", 0) > 0
        ), "status must identify destination lag", errors)
        try:
            require(lsn(outage[-1]["slot"]["restart_lsn"]) > lsn(outage[0]["slot"]["restart_lsn"]), "restart LSN advanced", errors)
            require(lsn(outage[-1]["slot"]["confirmed_flush_lsn"]) > lsn(outage[0]["slot"]["confirmed_flush_lsn"]), "flush LSN advanced", errors)
            require(all(lsn(right["slot"]["restart_lsn"]) >= lsn(left["slot"]["restart_lsn"]) for left, right in zip(outage, outage[1:])), "restart LSN never regressed", errors)
            require(sum(lsn(right["slot"]["restart_lsn"]) > lsn(left["slot"]["restart_lsn"]) for left, right in zip(outage, outage[1:])) >= 2, "repeated restart LSN progress", errors)
        except (KeyError, TypeError, ValueError):
            errors.append("outage slot progress")
        limit = profile.get("retained_wal_limit_bytes")
        require(isinstance(limit, int) and limit > 0, "declared WAL bound", errors)
        if isinstance(limit, int):
            require(all(s["slot"].get("retained_bytes", limit + 1) <= limit for s in outage), "retained WAL bound", errors)
    outcome = observation.get("outcome", {})
    require(outcome.get("oracle_match") is True, "independent PostgreSQL oracle match", errors)
    require(outcome.get("lost_event_ids") == 0, "zero lost events", errors)
    physical = outcome.get("remote_physical_rows")
    distinct = outcome.get("remote_distinct_event_ids")
    duplicates = outcome.get("duplicate_rows")
    require(all(isinstance(x, int) and x >= 0 for x in (physical, distinct, duplicates)), "physical/distinct/duplicate counts", errors)
    if all(isinstance(x, int) for x in (physical, distinct, duplicates)):
        require(physical - distinct == duplicates, "duplicate arithmetic", errors)
    require(isinstance(outcome.get("operator_steps"), list), "operator steps", errors)
    if samples and isinstance(caught_up, int):
        require(any(s.get("elapsed_ms", -1) >= caught_up and s.get("destination", {}).get("lag_events") == 0 for s in samples), "observed catch-up checkpoint", errors)
    require(observation.get("result") == ("pass" if not errors else "fail"), "result agrees with measurements", errors)
    return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("observation", type=Path)
    args = parser.parse_args()
    try:
        observation = json.loads(args.observation.read_text())
        errors = validate(observation)
    except (OSError, ValueError, TypeError) as error:
        print(f"invalid observation: {error}", file=sys.stderr)
        return 2
    for error in errors:
        print(error, file=sys.stderr)
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
