from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


aggregate = load_module("agg_rust_baseline", SCRIPTS_DIR / "agg_rust_baseline.py")
capture = load_module(
    "capture_rust_baseline_environment",
    SCRIPTS_DIR / "capture_rust_baseline_environment.py",
)


def git_evidence(repo: Path, args: list[str]) -> bytes:
    """Exercise evidence capture without writing a Git repository or commits."""
    return {
        ("git", "rev-parse", "HEAD"): b"a" * 40 + b"\n",
        ("git", "ls-files", "-z", "--cached"): b"src/tracked.rs\0README.md\0",
        ("git", "ls-files", "-z", "--others", "--exclude-standard"):
            b"src/untracked.py\0.env\0target/generated.rs\0"
            b"benchmarks/rust-baseline/results/record.json\0",
    }[tuple(args)]


class CaptureRustBaselineEnvironmentTests(unittest.TestCase):
    def test_capture_includes_source_and_excludes_artifacts_and_sensitive_files(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)

            (repo / "src").mkdir()
            (repo / "src" / "tracked.rs").write_text("fn tracked() {}\n")
            (repo / "README.md").write_text("tracked docs\n")

            (repo / "src" / "untracked.py").write_text("VALUE = 1\n")
            (repo / ".env").write_text("TOKEN=do-not-record\n")
            (repo / "target").mkdir()
            (repo / "target" / "generated.rs").write_text("generated\n")
            results = repo / "benchmarks" / "rust-baseline" / "results"
            results.mkdir(parents=True)
            (results / "record.json").write_text("{}\n")

            binary = repo / "target" / "release" / "rust-baseline"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"frozen binary")
            current_root = repo / ".tmp" / "evidence" / "trials"

            with unittest.mock.patch.object(capture, "run_command", side_effect=git_evidence), unittest.mock.patch.object(
                capture, "platform_evidence", return_value="test platform\n"
            ):
                manifest_path = capture.capture_environment(
                    current_root,
                    binary,
                    repo_root=repo,
                )

            aggregate.validate_environment_manifest(
                current_root,
                manifest_path,
                scales=aggregate.ALL_SCALES,
                expected_trials=aggregate.FINAL_EXPECTED_TRIALS,
            )
            manifest = json.loads(manifest_path.read_text())
            source_path = current_root / manifest["source_state"]["path"]
            source_state = json.loads(source_path.read_text())
            entries = {entry["path"]: entry for entry in source_state["files"]}
            self.assertEqual(entries["src/tracked.rs"]["origin"], "tracked")
            self.assertEqual(entries["src/untracked.py"]["origin"], "untracked")
            self.assertNotIn(".env", entries)
            self.assertNotIn("target/generated.rs", entries)
            self.assertNotIn("benchmarks/rust-baseline/results/record.json", entries)
            serialized = source_path.read_text()
            self.assertNotIn("do-not-record", serialized)
            self.assertEqual(
                manifest["command"]["argv_template"],
                aggregate.CANONICAL_COMMAND_TEMPLATE,
            )

    def test_capture_refuses_existing_trials(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            (repo / "README.md").write_text("fixture\n")
            binary = repo / "rust-baseline"
            binary.write_bytes(b"binary")
            current_root = repo / "evidence"
            (current_root / "trial-01").mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "before trials"):
                capture.capture_environment(current_root, binary, repo_root=repo)


if __name__ == "__main__":
    unittest.main()
