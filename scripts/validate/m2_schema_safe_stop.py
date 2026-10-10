#!/usr/bin/env python3
"""Validate live deterministic capture safe-stop evidence."""
import hashlib
import json
import pathlib
import re
import sys

out = pathlib.Path(sys.argv[1])


def lsn(value):
    assert re.fullmatch(r'[0-9A-F]+/[0-9A-F]+', value)
    high, low = value.split('/')
    return (int(high, 16) << 32) | int(low, 16)


for attempt in (1, 2):
    for scenario in ('nullable', 'incompatible'):
        observed = json.loads((out / f'attempt-{attempt}' / f'{scenario}-safe-stop.json').read_text())
        assert observed['schema_version'] == 'm2-schema-safe-stop-observation/v1'
        assert observed['scenario'] == scenario
        assert observed['postgres_version'].startswith('17.6')
        assert observed['replication_pid'] > 0 and observed['advisory_pid'] > 0
        assert observed['replication_pid'] != observed['advisory_pid']
        assert observed['durable_transaction_count'] == 1
        durable = int(observed['durable_lsn'], 16)
        assert durable > 0
        assert observed['failure_class'] == 'unsupported'
        assert observed['retry_class'] == 'deterministic'
        assert observed['failure_armed'] is True
        assert observed['process_alive_after_later_commit'] is True
        assert observed['replication_pid_stable'] is True
        assert observed['advisory_pid_stable'] is True
        positions = observed['feedback_positions'].split(',')
        assert len(positions) == 3 and all(lsn(value) <= durable for value in positions)
manifest = json.loads((out / 'evidence.json').read_text())
assert manifest['scenario_id'] == 'SCN-M2-CAPTURE-SAFE-STOP-REARM'
assert manifest['result']['status'] == 'pass'
files = sorted(path for path in out.rglob('*') if path.is_file() and path.name != 'sha256.txt')
actual = [f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out).as_posix()}' for path in files]
assert (out / 'sha256.txt').read_text().splitlines() == actual
for path in files:
    raw = path.read_bytes().lower()
    assert b'postgresql://' not in raw and b'password=' not in raw
print(json.dumps({'status': 'pass', 'scenario_id': manifest['scenario_id']}, sort_keys=True))
