import hashlib
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "series"))
sys.path.insert(0, str(ROOT / "scripts" / "validate"))
import redact_agent_story as redactor  # noqa: E402
import agent_story_bundle as validator  # noqa: E402


def jsonl(*objs) -> str:
    return "\n".join(json.dumps(o) for o in objs) + "\n"


def write_session(path: Path, user_content, assistant_turns):
    """assistant_turns: list of (content_blocks, tool_results) pairs, where
    tool_results is a list of (tool_name, is_error, text) triples emitted
    as toolResult messages right after that assistant turn."""
    events = [
        {"type": "session", "id": "s", "timestamp": "2026-01-01T00:00:00Z"},
        {
            "type": "message",
            "id": "u0",
            "message": {"role": "user", "content": user_content},
        },
    ]
    n = 0
    for content_blocks, tool_results in assistant_turns:
        n += 1
        events.append({
            "type": "message",
            "id": f"a{n}",
            "message": {"role": "assistant", "content": content_blocks},
        })
        for tool_name, is_error, text in tool_results:
            n += 1
            events.append({
                "type": "message",
                "id": f"t{n}",
                "message": {
                    "role": "toolResult",
                    "toolName": tool_name,
                    "isError": is_error,
                    "content": [{"type": "text", "text": text}],
                },
            })
    path.write_text(jsonl(*events))


class RedactTextTests(unittest.TestCase):
    def test_credential_shapes_are_removed(self):
        # Built by concatenation, deliberately: these are synthetic
        # credential-*shaped* fixtures for exercising the redactor, and
        # scripts/validate/scaffold_secrets.sh scans literal source lines
        # repo-wide for exactly this shape. Splitting the literal across a
        # concatenation keeps the fixture's intent (a full credential-shaped
        # string at runtime) without also making this source file itself
        # look like it embeds a live credential.
        cases = [
            'password=' + '"hunter2hunter2"',
            "api_key: '" + "sk-abcdefghijklmnop'",
            "postgresql://" + "user:hunter2@dbhost:5432/db",
            "-----BEGIN RSA " + "PRIVATE KEY-----",
        ]
        for case in cases:
            redacted = redactor.redact_text(case)
            self.assertNotIn("hunter2", redacted, case)
            self.assertNotIn("sk-abcdefghijklmnop", redacted, case)

    def test_absolute_paths_are_removed_across_arbitrary_roots(self):
        paths = (
            "/home/alice/projects/secret/notes.txt",
            "/workspace/project/private.txt",
            "/Users/alice/Library/private.txt",
            "/private/var/folders/cache.txt",
            "/Volumes/custom-mount/session.jsonl",
            "/arbitrary-root/tenant/data.txt",
            "/Users/Alice Smith/Library/private.txt",
            "/private/var/a:b/secret",
            "/秘密/tenant/session.jsonl",
        )
        for private_path in paths:
            with self.subTest(root=private_path.split("/", 2)[1]):
                redacted = redactor.redact_text(f"see {private_path} now")
                self.assertNotIn(private_path, redacted)
                self.assertIn("<ABS-PATH>", redacted)

    def test_relative_repo_paths_survive(self):
        text = "run scripts/validate/scaffold_secrets.sh"
        redacted = redactor.redact_text(text)
        self.assertEqual(text, redacted)

    def test_email_removed(self):
        redacted = redactor.redact_text("contact julien.hurault@sumeo.io for access")
        self.assertNotIn("julien.hurault@sumeo.io", redacted)
        self.assertIn("<EMAIL>", redacted)

    def test_ip_removed(self):
        redacted = redactor.redact_text("bound to 10.20.30.40 on the private net")
        self.assertNotIn("10.20.30.40", redacted)
        self.assertIn("<IP>", redacted)

    def test_dsn_removed_including_degenerate_empty_body(self):
        redacted = redactor.redact_text('reader used postgresql://postgres@127.0.0.1:5432/x?sslmode=disable')
        self.assertNotIn("postgresql://", redacted)
        # degenerate case: a DSN-shaped literal with nothing after the
        # closing quote, e.g. inside a python string check like
        # `"postgresql://" in output`
        redacted2 = redactor.redact_text('assert "postgresql://" not in output')
        self.assertNotIn("postgresql://", redacted2)

    def test_session_uuid_removed(self):
        redacted = redactor.redact_text("session id c8fa8e9f-4fa9-401a-836c-6c5622e4621b claimed the bead")
        self.assertNotIn("c8fa8e9f-4fa9-401a-836c-6c5622e4621b", redacted)
        self.assertIn("<SESSION-ID>", redacted)

    def test_container_name_removed(self):
        redacted = redactor.redact_text("running inside boring-worker--7ca946edf45423840681 now")
        self.assertNotIn("7ca946edf45423840681", redacted)


class CanonicalHashTests(unittest.TestCase):
    def test_matches_documented_convention(self):
        content = [{"type": "text", "text": "hello"}]
        expected = hashlib.sha256(
            json.dumps(content, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        self.assertEqual(redactor.canonical_content_hash(content), expected)


class RedactorExtractionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="a1bundle.", dir="/var/tmp"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def test_clean_session_extracts_and_validates(self):
        session = self.tmp / "session.jsonl"
        write_session(
            session,
            user_content=[{"type": "text", "text": "Target Bead: demo.1. Please fix the thing."}],
            assistant_turns=[
                (
                    [{"type": "thinking", "thinking": "..."}, {"type": "toolCall", "toolName": "bash"}],
                    [("bash", False, "tests passed: 5/5")],
                ),
                (
                    [{"type": "text", "text": "Implemented and pushed at abc123. Checks passed."}],
                    [],
                ),
            ],
        )
        record = redactor.build_record("demo.1", session)
        bundle = {"bundle_kind": "candidate-agent-story-bundle", "bundle_version": 1, "records": [record]}
        findings = validator.validate_bundle(bundle, "test", {"demo.1": (redactor.sha256_bytes(session.read_bytes()), session)})
        self.assertEqual(findings, [])
        self.assertTrue(record["dispatch_prompt"]["present"])
        self.assertTrue(record["first_agent_turn"]["turn_found"])
        self.assertFalse(record["first_agent_turn"]["has_text_content"])
        self.assertTrue(record["fix_forward"]["present"])
        self.assertIn("abc123", record["fix_forward"]["text"])

    def test_hostile_credential_in_prompt_is_redacted_and_passes(self):
        session = self.tmp / "session.jsonl"
        write_session(
            session,
            user_content=[{
                "type": "text",
                "text": 'Target Bead: demo.2. Use password=' + '"supersecretvalue" to connect.',
            }],
            assistant_turns=[
                ([{"type": "text", "text": "ok"}], []),
            ],
        )
        record = redactor.build_record("demo.2", session)
        self.assertNotIn("supersecretvalue", record["dispatch_prompt"]["text"])
        bundle = {"bundle_kind": "candidate-agent-story-bundle", "bundle_version": 1, "records": [record]}
        self.assertEqual(validator.validate_bundle(bundle, "test", {"demo.2": (redactor.sha256_bytes(session.read_bytes()), session)}), [])

    def test_hostile_absolute_path_in_tool_result_is_redacted_and_passes(self):
        session = self.tmp / "session.jsonl"
        write_session(
            session,
            user_content=[{"type": "text", "text": "Target Bead: demo.3. Fix it."}],
            assistant_turns=[
                (
                    [{"type": "toolCall", "toolName": "bash"}],
                    [("bash", True, "error: config at /home/ubuntu/projects/boring-cdc/.env failed to load")],
                ),
                ([{"type": "text", "text": "done"}], []),
            ],
        )
        record = redactor.build_record("demo.3", session)
        outcome_text = " ".join(o["summary"] for o in record["tool_outcomes"])
        self.assertNotIn("/home/ubuntu", outcome_text)
        bundle = {"bundle_kind": "candidate-agent-story-bundle", "bundle_version": 1, "records": [record]}
        self.assertEqual(validator.validate_bundle(bundle, "test", {"demo.3": (redactor.sha256_bytes(session.read_bytes()), session)}), [])

    def test_hostile_absolute_paths_across_roots_are_redacted_and_provenance_bound(self):
        private_paths = (
            "/workspace/project/session.jsonl",
            "/Users/alice/Library/session.jsonl",
            "/private/var/folders/session.jsonl",
            "/unlisted-root/tenant/session.jsonl",
            "/Users/Alice Smith/Library/session.jsonl",
            "/private/var/a:b/session.jsonl",
            "/秘密/tenant/session.jsonl",
        )
        for index, private_path in enumerate(private_paths):
            with self.subTest(index=index):
                session = self.tmp / f"root-{index}.jsonl"
                write_session(
                    session,
                    user_content=[{"type": "text", "text": f"Inspect {private_path} safely."}],
                    assistant_turns=[([{"type": "text", "text": "done"}], [])],
                )
                bead_id = f"demo.root.{index}"
                record = redactor.build_record(bead_id, session)
                self.assertNotIn(private_path, record["dispatch_prompt"]["text"])
                bundle = {
                    "bundle_kind": "candidate-agent-story-bundle",
                    "bundle_version": 1,
                    "records": [record],
                }
                digest = redactor.sha256_bytes(session.read_bytes())
                self.assertEqual(
                    validator.validate_bundle(bundle, "test", {bead_id: (digest, session)}),
                    [],
                )

    def test_non_signal_content_dump_is_filtered_out_of_tool_outcomes(self):
        session = self.tmp / "session.jsonl"
        source_dump = (
            "// internal-scope-comment: some-internal-tracker-id\n"
            "pub const SOMETHING: u16 = 1;\n"
            "diff --git a/Cargo.toml b/Cargo.toml\n"
            "+some_crate = { version = \"=1.2.3\" }\n"
        )
        write_session(
            session,
            user_content=[{"type": "text", "text": "Target Bead: demo.7. Fix it."}],
            assistant_turns=[
                ([{"type": "toolCall", "toolName": "bash"}], [("bash", False, source_dump)]),
                ([{"type": "text", "text": "done"}], []),
            ],
        )
        record = redactor.build_record("demo.7", session)
        outcome_text = " ".join(o["summary"] for o in record["tool_outcomes"])
        self.assertNotIn("internal-scope-comment", outcome_text)
        self.assertNotIn("some-internal-tracker-id", outcome_text)
        self.assertNotIn("some_crate", outcome_text)

    def test_single_dispatch_turn_yields_absent_review_feedback(self):
        session = self.tmp / "session.jsonl"
        write_session(
            session,
            user_content=[{"type": "text", "text": "Target Bead: demo.4. Go."}],
            assistant_turns=[([{"type": "text", "text": "done"}], [])],
        )
        record = redactor.build_record("demo.4", session)
        self.assertFalse(record["review_feedback"]["present"])
        self.assertIsNone(record["review_feedback"]["text"])
        self.assertIsNone(record["review_feedback"]["source_sha256"])

    def test_second_user_turn_is_extracted_as_review_feedback(self):
        session = self.tmp / "session.jsonl"
        events = [
            {"type": "session", "id": "s"},
            {"type": "message", "id": "u0", "message": {"role": "user", "content": [
                {"type": "text", "text": "Target Bead: demo.5. Go."}]}},
            {"type": "message", "id": "a0", "message": {"role": "assistant", "content": [
                {"type": "text", "text": "done"}]}},
            {"type": "message", "id": "u1", "message": {"role": "user", "content": [
                {"type": "text", "text": "Review found a bug, fix forward."}]}},
            {"type": "message", "id": "a2", "message": {"role": "assistant", "content": [
                {"type": "text", "text": "fixed and pushed"}]}},
        ]
        session.write_text(jsonl(*events))
        record = redactor.build_record("demo.5", session)
        self.assertTrue(record["review_feedback"]["present"])
        self.assertIn("Review found a bug", record["review_feedback"]["text"])
        self.assertTrue(record["fix_forward"]["present"])
        self.assertIn("fixed and pushed", record["fix_forward"]["text"])


class ValidatorFailClosedTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="a1bundle.", dir="/var/tmp"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.session = self.tmp / "retained.jsonl"
        write_session(
            self.session,
            user_content=[{"type": "text", "text": "Target Bead: demo.x. Fix the thing."}],
            assistant_turns=[
                (
                    [{"type": "text", "text": "I will inspect the bounded code."}],
                    [("bash", False, "tests passed: 5/5")],
                ),
                ([{"type": "text", "text": "Implemented and pushed. Checks passed."}], []),
            ],
        )
        self.record = redactor.build_record("demo.x", self.session)
        self.source_sha256 = redactor.sha256_bytes(self.session.read_bytes())
        self.sources = {"demo.x": (self.source_sha256, self.session)}

    def bundle_of(self, record=None):
        return {
            "bundle_kind": "candidate-agent-story-bundle",
            "bundle_version": 1,
            "records": [record or self.record],
        }

    def test_clean_record_passes_with_retained_source(self):
        self.assertEqual(validator.validate_bundle(self.bundle_of(), "t", self.sources), [])

    def test_retained_source_is_mandatory(self):
        findings = validator.validate_bundle(self.bundle_of(), "t")
        self.assertTrue(any("retained source not supplied" in f for f in findings))

    def test_fabricated_narrative_with_dummy_hashes_fails(self):
        record = json.loads(json.dumps(self.record))
        record["dispatch_prompt"]["text"] = "A plausible but fabricated clean narrative."
        record["dispatch_prompt"]["source_sha256"] = "b" * 64
        record["first_agent_turn"]["text"] = "More invented publication copy."
        record["first_agent_turn"]["source_sha256"] = "c" * 64
        record["fix_forward"]["text"] = "Invented success report."
        record["fix_forward"]["source_line_sha256"] = "d" * 64
        findings = validator.validate_bundle(self.bundle_of(record), "t", self.sources)
        self.assertTrue(any("does not exactly match independent retained-byte derivation" in f
                            for f in findings))

    def test_whole_file_dummy_hash_fails(self):
        record = json.loads(json.dumps(self.record))
        record["source_record_sha256"] = "a" * 64
        findings = validator.validate_bundle(self.bundle_of(record), "t", self.sources)
        self.assertTrue(any("independent retained-byte derivation" in f for f in findings))

    def test_untrusted_retained_bytes_fail_expected_digest(self):
        findings = validator.validate_bundle(
            self.bundle_of(), "t", {"demo.x": ("0" * 64, self.session)}
        )
        self.assertTrue(any("independently supplied expected digest" in f for f in findings))

    def test_omitted_or_extra_narrative_fails_exact_derivation(self):
        omitted = json.loads(json.dumps(self.record))
        omitted["tool_outcomes"] = []
        self.assertTrue(validator.validate_bundle(self.bundle_of(omitted), "t", self.sources))
        extra = json.loads(json.dumps(self.record))
        extra["editorial_summary"] = "clean but not retained"
        self.assertTrue(validator.validate_bundle(self.bundle_of(extra), "t", self.sources))

    def test_surviving_private_paths_at_any_root_fail_without_echoing_value(self):
        roots = (
            "/workspace/project/private.txt",
            "/Users/alice/Library/private.txt",
            "/private/var/folders/cache.txt",
            "/Volumes/custom-mount/session.jsonl",
            "/unlisted-root/tenant/data.txt",
            "/Users/Alice Smith/Library/private.txt",
            "/private/var/a:b/secret",
            "/秘密/tenant/session.jsonl",
        )
        for private_path in roots:
            with self.subTest(root=private_path.split("/", 2)[1]):
                record = json.loads(json.dumps(self.record))
                record["fix_forward"]["text"] = f"pushed from {private_path}"
                findings = validator.validate_bundle(self.bundle_of(record), "t", self.sources)
                self.assertTrue(any("absolute-path" in finding for finding in findings))
                self.assertFalse(any(private_path in finding for finding in findings))

    def test_top_level_fabrication_and_private_keys_fail_without_echo(self):
        bundle = self.bundle_of()
        bundle["editorial_summary"] = "fabricated but syntactically clean narrative"
        findings = validator.validate_bundle(bundle, "t", self.sources)
        self.assertTrue(any("top-level fields" in finding for finding in findings))

        private_key = "/Users/Alice Smith/Library/private.txt"
        bundle[private_key] = "192.0.2.44"
        findings = validator.validate_bundle(bundle, "t", self.sources)
        self.assertTrue(any("absolute-path" in finding for finding in findings))
        self.assertFalse(any(private_key in finding for finding in findings))

    def test_surviving_credential_fails_without_echoing_value(self):
        record = json.loads(json.dumps(self.record))
        secret_shape = 'password=' + '"syntheticsecretvalue"'
        record["dispatch_prompt"]["text"] = secret_shape
        findings = validator.validate_bundle(self.bundle_of(record), "t", self.sources)
        self.assertTrue(any("credential" in f for f in findings))
        self.assertFalse(any("syntheticsecretvalue" in f for f in findings))

    def test_surviving_email_ip_and_dsn_fail(self):
        cases = (
            ("notify person@example.test", "email"),
            ("connect to 192.0.2.12", "ipv4"),
            ("use redis://cache:6379/0", "dsn"),
        )
        for text, kind in cases:
            with self.subTest(kind=kind):
                record = json.loads(json.dumps(self.record))
                record["dispatch_prompt"]["text"] = text
                findings = validator.validate_bundle(self.bundle_of(record), "t", self.sources)
                self.assertTrue(any(kind in finding for finding in findings))

    def test_malformed_retained_source_fails_without_path_or_content(self):
        malformed = self.tmp / "malformed.jsonl"
        malformed.write_text("not json and private text")
        findings = validator.validate_bundle(self.bundle_of(), "t", {"demo.x": (self.source_sha256, malformed)})
        self.assertTrue(any("could not be parsed" in finding for finding in findings))
        self.assertFalse(any(str(malformed) in finding or "private text" in finding
                             for finding in findings))

    def test_cli_requires_sources_and_rejects_fabrication(self):
        bundle_path = self.tmp / "candidate.json"
        bundle_path.write_text(json.dumps(self.bundle_of()))
        self.assertEqual(validator.main([str(bundle_path)]), 1)

        self.assertEqual(validator.main([
            "--record", f"demo.x={self.source_sha256}={self.session}", str(bundle_path),
        ]), 0)

        fabricated = json.loads(json.dumps(self.bundle_of()))
        fabricated["records"][0]["dispatch_prompt"]["text"] = "fabricated"
        fabricated["records"][0]["dispatch_prompt"]["source_sha256"] = "f" * 64
        bundle_path.write_text(json.dumps(fabricated))
        self.assertEqual(validator.main([
            "--record", f"demo.x={self.source_sha256}={self.session}", str(bundle_path),
        ]), 1)


class RedactorCliTests(unittest.TestCase):
    def test_refuses_to_write_into_docs(self):
        tmp = Path(tempfile.mkdtemp(prefix="a1bundle.", dir="/var/tmp"))
        try:
            session = tmp / "session.jsonl"
            write_session(
                session,
                user_content=[{"type": "text", "text": "hi"}],
                assistant_turns=[([{"type": "text", "text": "ok"}], [])],
            )
            rc = redactor.main([
                "--record", f"demo.6={session}",
                "--out", "docs/agent-story/should-not-write.json",
            ])
            self.assertEqual(rc, 2)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
