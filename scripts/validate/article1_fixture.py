#!/usr/bin/env python3
"""Fail-closed static and live validation for the Article 1 PostgreSQL fixture."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "fixtures/article1/compose.yml"
SCHEMA = ROOT / "fixtures/article1/schema-and-seed.sql"
MUTATE = ROOT / "fixtures/article1/mutate.sql"
FIXTURE = ROOT / "fixtures/article1/fixture.json"
EXPECTED_IMAGE = "docker.io/library/postgres:17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929"
EXPECTED_M1_SHA = "48b270382bce0c35688db8a78973f215715f4d301fbe054fcaf4cbeb02a7a4b7"
EXPECTED_TABLES = ["customers", "order_items", "orders", "products"]


def fail(message: str) -> "None":
    raise SystemExit(f"ARTICLE1_FIXTURE_MISMATCH: {message}")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(argv: list[str], *, data: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["TMPDIR"] = "/var/tmp"
    result = subprocess.run(argv, cwd=ROOT, env=env, input=data, text=True, capture_output=True)
    if check and result.returncode:
        fail(f"command failed ({result.returncode}): {' '.join(argv)}: {result.stderr.strip()}")
    return result


def compose(project: str, *args: str, data: str | None = None) -> str:
    return run(["docker", "compose", "-p", project, "-f", str(COMPOSE), *args], data=data).stdout


def psql(project: str, sql: str, *, quiet: bool = True) -> str:
    flags = ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "article1", "-At", "-F", "\t"]
    if quiet:
        flags.append("-q")
    return compose(project, "exec", "-T", "postgres", *flags, data=sql)


def static_check() -> None:
    fixture = json.loads(FIXTURE.read_text())
    compose_text = COMPOSE.read_text()
    if fixture.get("postgres_image") != EXPECTED_IMAGE or f"image: {EXPECTED_IMAGE}" not in compose_text:
        fail("postgres image digest is not the accepted PostgreSQL 17.6 digest")
    if fixture.get("platform") != "linux/amd64" or "platform: linux/amd64" not in compose_text:
        fail("platform is not linux/amd64")
    source = ROOT / fixture["source_workload"]["path"]
    if fixture["source_workload"]["sha256"] != EXPECTED_M1_SHA or sha(source) != EXPECTED_M1_SHA:
        fail("m1-workload source digest drift")
    if fixture.get("seed") != "workload-v1" or sorted(fixture.get("tables", [])) != EXPECTED_TABLES:
        fail("seed identity or commerce table set drift")
    schema = SCHEMA.read_text()
    if schema.count("CREATE PUBLICATION ") != 1 or schema.count("pg_create_logical_replication_slot(") != 1:
        fail("schema must define exactly one publication and one logical slot")
    expected_mutations = [
        "INSERT INTO customers VALUES (1, 'Ada', 1);",
        "INSERT INTO products VALUES (10, 'P10', 12.50);",
        "INSERT INTO orders VALUES (100, 1, 'new');",
        "INSERT INTO order_items VALUES (100, 1, 10, 1);",
        "UPDATE customers SET tier = 2 WHERE id = 1;",
        "UPDATE customers SET tier = 3 WHERE id = 1;",
        "DELETE FROM order_items WHERE order_id = 100 AND line_no = 1;",
        "INSERT INTO order_items VALUES (100, 1, 10, 2);",
        "DELETE FROM customers WHERE id = 1;",
        "INSERT INTO customers VALUES (2, 'Ada', 3);",
    ]
    text = MUTATE.read_text()
    positions = [text.find(statement) for statement in expected_mutations]
    if any(position < 0 for position in positions) or positions != sorted(positions):
        fail("mutation SQL no longer follows the m1-workload-v1 sequence")
    print(f"STATIC_OK image={EXPECTED_IMAGE} platform=linux/amd64 seed=workload-v1 m1_sha256={EXPECTED_M1_SHA}")


def cstring(data: bytes, offset: int) -> tuple[str, int]:
    end = data.index(0, offset)
    return data[offset:end].decode(), end + 1


def tuple_data(data: bytes, offset: int) -> tuple[list[str | None], int]:
    count = struct.unpack_from("!H", data, offset)[0]
    offset += 2
    values: list[str | None] = []
    for _ in range(count):
        kind = chr(data[offset]); offset += 1
        if kind in "nu":
            values.append(None if kind == "n" else "unchanged")
        elif kind in "tb":
            length = struct.unpack_from("!I", data, offset)[0]; offset += 4
            values.append(data[offset:offset + length].decode(errors="replace")); offset += length
        else:
            fail(f"unknown pgoutput tuple kind {kind!r}")
    return values, offset


def pgoutput_old_tuple_summary(hex_rows: str) -> str:
    relations: dict[int, tuple[str, list[str]]] = {}
    observations: list[str] = []
    for encoded in hex_rows.splitlines():
        if not encoded:
            continue
        data = bytes.fromhex(encoded)
        tag = chr(data[0]); offset = 1
        if tag == "R":
            oid = struct.unpack_from("!I", data, offset)[0]; offset += 4
            _, offset = cstring(data, offset)
            name, offset = cstring(data, offset)
            identity = chr(data[offset]); offset += 1
            count = struct.unpack_from("!H", data, offset)[0]; offset += 2
            columns = []
            for _ in range(count):
                offset += 1
                column, offset = cstring(data, offset)
                columns.append(column); offset += 8
            if identity != "d":
                fail(f"relation {name} replica identity is {identity!r}, expected default 'd'")
            relations[oid] = (name, columns)
        elif tag in "UD":
            oid = struct.unpack_from("!I", data, offset)[0]; offset += 4
            if oid not in relations:
                fail(f"pgoutput {tag} references unknown relation oid {oid}")
            name, _ = relations[oid]
            old_marker = "absent"
            old_values: list[str | None] = []
            if chr(data[offset]) in "KO":
                old_marker = "key" if chr(data[offset]) == "K" else "full"
                offset += 1
                old_values, offset = tuple_data(data, offset)
            if tag == "U":
                if chr(data[offset]) != "N":
                    fail(f"update for {name} lacks new tuple")
                _, offset = tuple_data(data, offset + 1)
            observations.append(f"{name}:{'update' if tag == 'U' else 'delete'}:{old_marker}:{len(old_values)}")
    required = {"customers:update:absent:0", "customers:delete:key:3", "order_items:delete:key:4"}
    # TupleData retains one entry per relation column; non-key entries are null tokens in key tuples.
    missing = required.difference(observations)
    if missing:
        fail(f"default replica-identity old tuple behavior missing {sorted(missing)}; observed={observations}")
    return ",".join(observations)


def catalog_check(project: str, expected_version: str, expected_publication: str, expected_slot: str) -> None:
    try:
        major, minor = (int(part) for part in expected_version.split(".", 1))
    except ValueError:
        fail(f"invalid expected version {expected_version!r}; use major.minor")
    expected_version_num = str(major * 10000 + minor)
    query = """
SELECT current_setting('server_version_num');
SELECT count(*) || ':' || coalesce(min(pubname),'') FROM pg_publication;
SELECT count(*) || ':' || coalesce(min(slot_name),'') || ':' || coalesce(min(plugin),'') || ':' || coalesce(min(database),'') || ':' || coalesce(min(slot_type),'') || ':' || coalesce(bool_or(active),false) FROM pg_replication_slots;
SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_publication_tables WHERE pubname = 'article1_publication';
SELECT string_agg(relname || ':' || relreplident::text, ',' ORDER BY relname) FROM pg_class WHERE relname IN ('customers','products','orders','order_items');
SELECT pubinsert::int || ',' || pubupdate::int || ',' || pubdelete::int || ',' || pubtruncate::int FROM pg_publication WHERE pubname = 'article1_publication';
"""
    rows = psql(project, query).splitlines()
    expected = [
        expected_version_num,
        f"1:{expected_publication}",
        f"1:{expected_slot}:pgoutput:article1:logical:false",
        ",".join(EXPECTED_TABLES),
        "customers:d,order_items:d,orders:d,products:d",
        "1,1,1,0",
    ]
    labels = ["version", "publication", "slot", "publication tables", "replica identity", "publication operations"]
    if len(rows) != len(expected):
        fail(f"catalog shape expected {len(expected)} rows, got {rows}")
    for label, actual, wanted in zip(labels, rows, expected):
        if actual != wanted:
            fail(f"{label} expected {wanted!r}, got {actual!r}")


def seed_digest(project: str) -> str:
    sql = """
SELECT 'customers' || E'\\t' || id || E'\\t' || name || E'\\t' || tier FROM customers WHERE id >= 1000
UNION ALL SELECT 'products' || E'\\t' || id || E'\\t' || sku || E'\\t' || price FROM products WHERE id >= 1000
UNION ALL SELECT 'orders' || E'\\t' || id || E'\\t' || customer_id || E'\\t' || status FROM orders WHERE id >= 1000
UNION ALL SELECT 'order_items' || E'\\t' || order_id || E'\\t' || line_no || E'\\t' || product_id || E'\\t' || quantity FROM order_items WHERE order_id >= 1000
ORDER BY 1;
"""
    # Canonical contract order is semantic, not SQL lexical order.
    rows = psql(project, sql).splitlines()
    by_table = {row.split("\t", 1)[0]: row for row in rows}
    canonical = "\n".join(by_table[name] for name in ["customers", "products", "orders", "order_items"]) + "\n"
    return hashlib.sha256(canonical.encode()).hexdigest()


def final_rows(project: str) -> str:
    return psql(project, """
SELECT 'customers',id::text,name || '|' || tier FROM customers
UNION ALL SELECT 'products',id::text,sku || '|' || price FROM products
UNION ALL SELECT 'orders',id::text,customer_id || '|' || status FROM orders
UNION ALL SELECT 'order_items',order_id || ':' || line_no,product_id || '|' || quantity FROM order_items
ORDER BY 1,2;
""")


def cycle(project: str, expected_version: str, expected_publication: str, expected_slot: str) -> tuple[str, str]:
    compose(project, "down", "-v", "--remove-orphans")
    compose(project, "up", "-d", "--wait", "--wait-timeout", "120")
    container = compose(project, "ps", "-q", "postgres").strip()
    runtime_image = run(["docker", "inspect", "--format", "{{.Config.Image}}", container]).stdout.strip()
    architecture = run(["docker", "image", "inspect", "--format", "{{.Architecture}}", EXPECTED_IMAGE]).stdout.strip()
    if runtime_image != EXPECTED_IMAGE:
        fail(f"runtime image expected {EXPECTED_IMAGE!r}, got {runtime_image!r}")
    if architecture != "amd64":
        fail(f"runtime image architecture expected 'amd64', got {architecture!r}")
    catalog_check(project, expected_version, expected_publication, expected_slot)
    fixture = json.loads(FIXTURE.read_text())
    actual_seed = seed_digest(project)
    if actual_seed != fixture["seed_rows_sha256"]:
        fail(f"seed digest expected {fixture['seed_rows_sha256']}, got {actual_seed}")
    psql(project, MUTATE.read_text())
    changes = psql(project, "SELECT encode(data,'hex') FROM pg_logical_slot_get_binary_changes('article1_slot',NULL,NULL,'proto_version','1','publication_names','article1_publication');")
    return final_rows(project), pgoutput_old_tuple_summary(changes)


def live_check(args: argparse.Namespace) -> None:
    project = args.project
    try:
        compose(project, "config", "-q")
        compose(project, "pull", "postgres")
        first = cycle(project, args.expected_version, args.expected_publication, args.expected_slot)
        second = cycle(project, args.expected_version, args.expected_publication, args.expected_slot)
        if first != second:
            fail("reset replay is not deterministic")
        print(f"LIVE_OK version={args.expected_version} publication={args.expected_publication} slot={args.expected_slot} seed_sha256={json.loads(FIXTURE.read_text())['seed_rows_sha256']}")
        print(f"OLD_TUPLES {first[1]}")
        print("FINAL_ROWS_SHA256 " + hashlib.sha256(first[0].encode()).hexdigest())
    finally:
        if not args.keep:
            compose(project, "down", "-v", "--remove-orphans")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["static", "live", "all"])
    parser.add_argument("--project", default="boring-cdc-article1-validator")
    parser.add_argument("--expected-version", default="17.6")
    parser.add_argument("--expected-publication", default="article1_publication")
    parser.add_argument("--expected-slot", default="article1_slot")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    if args.mode in {"static", "all"}:
        static_check()
    if args.mode in {"live", "all"}:
        live_check(args)


if __name__ == "__main__":
    main()
