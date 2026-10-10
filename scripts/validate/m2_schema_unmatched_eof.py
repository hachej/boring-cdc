#!/usr/bin/env python3
"""Validate two live unmatched CopyBoth EOF observations after schema safe-stop."""
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


for number in (1, 2):
    observed = json.loads((out / f'attempt-{number}' / 'observation.json').read_text())
    assert observed['schema_version'] == 'm2-schema-unmatched-eof-observation/v1'
    assert observed['postgres_version'].startswith('17.6')
    assert observed['runtime_exit_code'] != 0
    assert int(observed['prior_durable_lsn'], 16) > 0
    assert lsn(observed['slot_confirmed_after_eof']) <= int(observed['prior_durable_lsn'], 16)
    assert observed['durable_transaction_count_after_eof'] == 1
    assert observed['original_replication_pid'] > 0
    assert observed['original_advisory_pid'] > 0
    assert observed['original_replication_pid'] != observed['original_advisory_pid']
    assert observed['active_failure_class'] == 'unsupported'
    assert observed['active_retry_class'] == 'deterministic'
    assert observed['active_failure_count'] == 1
    assert observed['replication_reopened'] is False
    assert observed['advisory_reacquired'] is False
    assert observed['replication_slot_active_after_exit'] is False
manifest = json.loads((out / 'evidence.json').read_text())
assert manifest['scenario_id'] == 'SCN-M2-CAPTURE-COPYBOTH-LOSS'
assert manifest['result']['status'] == 'pass'
files = sorted(path for path in out.rglob('*') if path.is_file() and path.name != 'sha256.txt')
actual = [f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out).as_posix()}' for path in files]
assert (out / 'sha256.txt').read_text().splitlines() == actual
for path in files:
    raw = path.read_bytes().lower()
    assert b'postgresql:' + b'//' not in raw and b'password' + b'=' not in raw
print(json.dumps({'status': 'pass', 'scenario_id': manifest['scenario_id']}, sort_keys=True))
