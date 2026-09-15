import json
import shutil
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts/lib"))
from m1_control_evidence import validate

SOURCE = ROOT / "artifacts/boring-cdc-m1-control-fixtures/SCN-M1-CONTROL-COMPONENT/m1-control-v1"


class ControlCommandEvidence(unittest.TestCase):
    def copied(self):
        temporary = tempfile.TemporaryDirectory(dir=ROOT / "tests")
        target = Path(temporary.name) / "artifact"
        shutil.copytree(SOURCE, target)
        return temporary, target

    def assert_fails(self, mutate, code):
        temporary, target = self.copied()
        try:
            mutate(target)
            findings = validate(target)
            self.assertIn(code, {item["code"] for item in findings}, findings)
        finally:
            temporary.cleanup()

    def test_canonical_command_evidence_passes(self):
        self.assertEqual(validate(SOURCE), [])

    def test_validator_record_and_stream_tampering_fail(self):
        def exit_code(root):
            path = root / "validator-evidence.json"
            data = json.loads(path.read_text())
            data["validators"][0]["exit_code"] = 1
            path.write_text(json.dumps(data))
        self.assert_fails(exit_code, "E_VALIDATOR_EXIT")
        self.assert_fails(lambda root: (root / "stdout/validator-generic.txt").write_text("forged\n"), "E_VALIDATOR_STREAM")


    def test_traversal_malformed_and_coordinated_transcript_tampering_fail(self):
        def traversal(root):
            path = root / "validator-evidence.json"
            data = json.loads(path.read_text())
            data["validators"][0]["stdout_path"] = str(Path(data["manifest_path"]).parent / ".." / ".." / "Cargo.toml")
            data["validators"][0]["stdout_sha256"] = "0" * 64
            path.write_text(json.dumps(data))
        self.assert_fails(traversal, "E_VALIDATOR_PATH")

        def malformed(root):
            path = root / "validator-evidence.json"
            data = json.loads(path.read_text())
            data["validators"][0]["argv"] = []
            path.write_text(json.dumps(data))
        self.assert_fails(malformed, "E_VALIDATOR_ARGV")

        def coordinated(root):
            transcript = root / "stdout/validator-generic.txt"
            original = transcript.read_text()
            transcript.write_text("  " + original)
            path = root / "validator-evidence.json"
            data = json.loads(path.read_text())
            import hashlib
            data["validators"][0]["stdout_sha256"] = hashlib.sha256(transcript.read_bytes()).hexdigest()
            path.write_text(json.dumps(data))
        self.assert_fails(coordinated, "E_GENERIC_RESULT")

        def symlink_payload(root):
            target = root / "config.json"
            content = target.read_bytes()
            external = root.parent / "external-config.json"
            external.write_bytes(content)
            target.unlink()
            target.symlink_to(external)
        self.assert_fails(symlink_payload, "E_PATH_SYMLINK")
        self.assert_fails(lambda root: (root / "validator-secret.txt").write_text("TODO secret\n"), "E_PAYLOAD_SEAL")

        def coordinated_rerun(root):
            for name in ("stdout/e2e-1.txt", "stdout/e2e-2.txt"):
                (root / name).write_text("coordinated rewrite\n")
            import hashlib
            inventory = root / "sha256.txt"
            rows = []
            for path in sorted(item for item in root.rglob("*") if item.is_file() and item.name not in {"manifest.json", "sha256.txt", "validator-evidence.json"} and path_name(item, root) not in {"stdout/validator-generic.txt", "stderr/validator-generic.txt", "stdout/validator-specific.txt", "stderr/validator-specific.txt"}):
                rows.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root)}\n")
            inventory.write_text("".join(rows))
        def path_name(path, root):
            return path.relative_to(root).as_posix()
        self.assert_fails(coordinated_rerun, "E_PAYLOAD_SEAL")

    def test_each_validator_argv_is_bound_to_its_canonical_transcript_paths(self):
        def rewrite(root, mutate):
            path = root / "validator-evidence.json"
            data = json.loads(path.read_text())
            mutate(data["validators"])
            path.write_text(json.dumps(data))

        def swap_stdout(rows):
            for field in ("stdout_path", "stdout_sha256"):
                rows[0][field], rows[1][field] = rows[1][field], rows[0][field]
        self.assert_fails(lambda root: rewrite(root, swap_stdout), "E_VALIDATOR_PATH")

        def redirect_to_payload(rows, index, name):
            import hashlib
            target = SOURCE / name
            rows[index]["stdout_path"] = str(Path(rows[index]["stdout_path"]).parent.parent / name)
            rows[index]["stdout_sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()
        self.assert_fails(lambda root: rewrite(root, lambda rows: redirect_to_payload(rows, 0, "config.json")), "E_VALIDATOR_PATH")
        self.assert_fails(lambda root: rewrite(root, lambda rows: redirect_to_payload(rows, 1, "packet.json")), "E_VALIDATOR_PATH")

        def duplicate_empty_stderr(rows):
            rows[1]["stderr_path"] = rows[0]["stderr_path"]
            rows[1]["stderr_sha256"] = rows[0]["stderr_sha256"]
        self.assert_fails(lambda root: rewrite(root, duplicate_empty_stderr), "E_VALIDATOR_PATH")

        def substitute_other_transcript(rows):
            rows[0]["stdout_path"] = rows[1]["stdout_path"]
            rows[0]["stdout_sha256"] = rows[1]["stdout_sha256"]
        self.assert_fails(lambda root: rewrite(root, substitute_other_transcript), "E_VALIDATOR_PATH")

    def test_inventory_log_rerun_unresolved_cleanup_and_redaction_tampering_fail(self):
        self.assert_fails(lambda root: (root / "sha256.txt").write_text("0" * 64 + "  commands.txt\n"), "E_INVENTORY_SEAL")
        self.assert_fails(lambda root: (root / "logs/boring-cdc.jsonl").write_text("{}\n"), "E_PAYLOAD_SEAL")
        self.assert_fails(lambda root: (root / "stdout/e2e-2.txt").write_text("different\n"), "E_PAYLOAD_SEAL")
        self.assert_fails(lambda root: (root / "config.json").write_text("TODO\n"), "E_PAYLOAD_SEAL")

        def manifest_field(root, field, value):
            path = root / "manifest.json"
            data = json.loads(path.read_text())
            data[field] = value
            path.write_text(json.dumps(data))
        self.assert_fails(lambda root: manifest_field(root, "cleanup", {"complete": False, "remaining_paths": ["tmp"]}), "E_CLEANUP")
        self.assert_fails(lambda root: manifest_field(root, "redaction", {"checked": False, "secrets_found": 1}), "E_REDACTION")


if __name__ == "__main__":
    unittest.main()
