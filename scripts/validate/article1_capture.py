#!/usr/bin/env python3
"""Run the exact Article 1 transport against the pinned PostgreSQL 17.6 fixture."""

import argparse
import os
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "fixtures/article1/compose.yml"
DSN_PASSWORD = "article1_fixture_only"


def run(command: list[str], env: dict[str, str], *, expect_failure: bool = False) -> str:
    result = subprocess.run(command, cwd=ROOT, env=env, text=True, capture_output=True)
    output = result.stdout + result.stderr
    if expect_failure:
        if result.returncode == 0:
            raise RuntimeError(f"expected failure: {' '.join(command)}")
    elif result.returncode != 0:
        sys.stderr.write(output)
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(command)}")
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=55646)
    parser.add_argument("--project", default="boring-cdc-article1-capture")
    args = parser.parse_args()
    env = os.environ.copy()
    env["TMPDIR"] = "/var/tmp"
    env["ARTICLE1_PG_PORT"] = str(args.port)
    compose = ["docker", "compose", "-p", args.project, "-f", str(COMPOSE)]
    try:
        run(compose + ["down", "-v", "--remove-orphans"], env, expect_failure=False)
        run(compose + ["up", "-d", "--wait"], env)
        env["ARTICLE1_DSN"] = (
            f"postgresql://postgres:{DSN_PASSWORD}@127.0.0.1:{args.port}/article1?sslmode=disable"
        )
        focused = run(["cargo", "test", "--locked", "article1_capture::tests"], env)
        preflight = run(
            ["cargo", "test", "--locked", "live_pg17_preflight_negatives_use_capture_boundaries", "--", "--ignored"],
            env,
        )
        connection = run(
            ["cargo", "test", "--locked", "live_pg17_connection_fails_closed", "--", "--ignored"],
            env,
        )
        auth = run(
            ["cargo", "test", "--locked", "live_pg17_wrong_auth_fails_closed", "--", "--ignored"],
            env,
        )
        transcript = run(
            ["cargo", "test", "--locked", "live_pg17_copyboth_insert_update_delete_uses_m1_decoder", "--", "--ignored", "--nocapture"],
            env,
        )
        for event in ("BEGIN", "INSERT", "UPDATE", "DELETE", "COMMIT"):
            if f'"event":"{event}"' not in transcript:
                raise RuntimeError(f"live transcript missing {event}")
        negatives = [
            (["--expected-version", "17.5"], "version"),
            (["--expected-publication", "wrong_publication"], "publication"),
            (["--expected-slot", "wrong_slot"], "slot"),
        ]
        for options, boundary in negatives:
            output = run(
                [sys.executable, "scripts/validate/article1_fixture.py", "live", "--project", args.project, *options],
                env,
                expect_failure=True,
            )
            if boundary not in output:
                raise RuntimeError(f"negative did not name {boundary}")
        print("ARTICLE1_CAPTURE_OK postgres=17.6 copyboth=true events=BEGIN,INSERT,UPDATE,DELETE,COMMIT")
        print("ARTICLE1_LIVE_FAILURES_OK connection/auth/version/publication/slot/continuity=fail-closed")
        print("ARTICLE1_FOCUSED_FAILURES_OK config/protocol=fail-closed")
        assert focused and preflight and connection and auth
        return 0
    finally:
        subprocess.run(compose + ["down", "-v", "--remove-orphans"], cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"ARTICLE1_CAPTURE_VALIDATION_FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
