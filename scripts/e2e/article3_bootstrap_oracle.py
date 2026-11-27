#!/usr/bin/env python3
"""Compare an independent PostgreSQL query with the ClickHouse candidate query."""

import base64
import hashlib
import json
import sys
from pathlib import Path


def decode_cell(cell, oid):
    _, state, actual_oid, _, encoded = cell
    if state != "explicit_value" or int(actual_oid) != oid:
        raise ValueError("unexpected oracle cell state or type")
    return base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))


def read_source(path):
    source = {}
    for line in path.read_text().splitlines():
        id_text, payload = line.split("|", 1)
        key = int(id_text)
        if key in source:
            raise ValueError("duplicate PostgreSQL key")
        source[key] = payload
    return source


def read_candidate(path):
    candidate = {}
    for line in path.read_text().splitlines():
        row = json.loads(line)
        (key_cell,) = json.loads(row["canonical_key"])
        key = int(key_cell["value"])
        cells = {int(cell[0]): cell for cell in row["explicit_cells"]}
        if len(row["explicit_cells"]) != 2 or set(cells) != {0, 1}:
            raise ValueError("unexpected candidate columns")
        pk = int.from_bytes(decode_cell(cells[0], 20), "big", signed=True)
        if pk != key or key in candidate:
            raise ValueError("wrong or duplicate ClickHouse key")
        candidate[key] = decode_cell(cells[1], 25).decode()
    return candidate


def checksum(rows):
    content = json.dumps(sorted(rows.items()), separators=(",", ":")).encode()
    return hashlib.sha256(content).hexdigest()


def main():
    source = read_source(Path(sys.argv[1]))
    candidate = read_candidate(Path(sys.argv[2]))
    counts = {
        "source_rows": len(source),
        "candidate_rows": len(candidate),
        "missing_keys": len(source.keys() - candidate.keys()),
        "extra_keys": len(candidate.keys() - source.keys()),
        "wrong_values": sum(source[k] != candidate[k] for k in source.keys() & candidate.keys()),
        "source_sha256": checksum(source),
        "candidate_sha256": checksum(candidate),
    }
    passed = bool(source) and source == candidate
    counts["result"] = "pass" if passed else "fail"
    Path(sys.argv[3]).write_text(json.dumps(counts, indent=2, sort_keys=True) + "\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
