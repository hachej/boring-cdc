#!/usr/bin/env python3
"""Validate the exact `boring-cdc run` reader against pinned PostgreSQL 17.6."""

import argparse
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "fixtures/article1/compose.yml"
BINARY = ROOT / "target/debug/boring-cdc"
PASSWORD = "article1_fixture_only"


def run(command: list[str], env: dict[str, str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=ROOT, env=env, text=True, capture_output=True)
    if check and result.returncode != 0:
        sys.stderr.write(result.stdout + result.stderr)
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(command)}")
    return result


def recreate(compose: list[str], env: dict[str, str]) -> None:
    run(compose + ["down", "-v", "--remove-orphans"], env)
    run(compose + ["up", "-d", "--wait"], env)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=55666)
    parser.add_argument("--project", default="boring-cdc-article1-cli")
    args = parser.parse_args()
    env = os.environ.copy()
    env["TMPDIR"] = "/var/tmp"
    env["ARTICLE1_PG_PORT"] = str(args.port)
    env["BORING_CDC_ARTICLE1_DSN"] = (
        f"postgresql://postgres:{PASSWORD}@127.0.0.1:{args.port}/article1?sslmode=disable"
    )
    compose = ["docker", "compose", "-p", args.project, "-f", str(COMPOSE)]
    try:
        run(["cargo", "build", "--locked"], env)
        recreate(compose, env)
        reader = subprocess.Popen(
            [str(BINARY), "run"], cwd=ROOT, env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        time.sleep(0.75)
        run(
            compose
            + [
                "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1",
                "-U", "postgres", "-d", "article1", "-c",
                "BEGIN; INSERT INTO customers VALUES (9101, 'CLI', 1); UPDATE customers SET tier=2 WHERE id=9101; DELETE FROM customers WHERE id=9101; COMMIT;",
            ],
            env,
        )
        stdout, stderr = reader.communicate(timeout=15)
        if reader.returncode != 0 or stderr:
            raise RuntimeError(f"reader failed ({reader.returncode}): {stderr}")
        events = [json.loads(line) for line in stdout.splitlines()]
        names = [event["event"] for event in events]
        if names != ["BEGIN", "INSERT", "UPDATE", "DELETE", "COMMIT"]:
            raise RuntimeError(f"unexpected CLI events: {names}")
        if [events[index]["old_state"] for index in (1, 2, 3)] != ["absent", "absent", "key"]:
            raise RuntimeError("CLI transcript old-state contract drifted")

        wrong = env.copy()
        wrong["BORING_CDC_ARTICLE1_DSN"] = wrong["BORING_CDC_ARTICLE1_DSN"].replace(PASSWORD, "super-secret-wrong")
        failure = run([str(BINARY), "run"], wrong, check=False)
        if failure.returncode != 4 or "ARTICLE1_AUTH_FAILED: reader capture failed" not in failure.stderr:
            raise RuntimeError("authentication failure contract drifted")
        if "super-secret-wrong" in failure.stdout + failure.stderr or "postgresql://" in failure.stdout + failure.stderr:
            raise RuntimeError("reader failure leaked its DSN")

        unsupported = run([str(BINARY), "run", "--json"], env, check=False)
        if unsupported.returncode != 2 or "CLI_JSON_UNSUPPORTED" not in unsupported.stdout + unsupported.stderr:
            raise RuntimeError("run grammar changed")
        unrelated = run([str(BINARY), "status"], env, check=False)
        if unrelated.returncode != 4 or "CLI_HANDLER_UNAVAILABLE" not in unrelated.stderr:
            raise RuntimeError("unrelated command handler changed")

        recreate(compose, env)
        cancelled = subprocess.Popen(
            [str(BINARY), "run"], cwd=ROOT, env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        time.sleep(1)
        cancelled.send_signal(signal.SIGTERM)
        cancel_stdout, cancel_stderr = cancelled.communicate(timeout=10)
        if cancelled.returncode != 0 or cancel_stdout or cancel_stderr:
            raise RuntimeError(
                f"SIGTERM was not clean: rc={cancelled.returncode} stdout={cancel_stdout!r} stderr={cancel_stderr!r}"
            )

        print(
            "ARTICLE1_CLI_OK command='BORING_CDC_ARTICLE1_DSN=<redacted> target/debug/boring-cdc run' "
            "postgres=17.6 events=BEGIN,INSERT,UPDATE,DELETE,COMMIT old_states=absent,key "
            "auth=redacted cancellation=clean unrelated=unchanged"
        )
        return 0
    finally:
        subprocess.run(
            compose + ["down", "-v", "--remove-orphans"], cwd=ROOT, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"ARTICLE1_CLI_VALIDATION_FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
