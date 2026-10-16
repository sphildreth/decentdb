"""Exercise runner failure semantics without invoking repository build suites."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("pre_commit_checks", ROOT / "scripts/do-pre-commit-checks.py")
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class PreCommitChecksTests(unittest.TestCase):
    def setUp(self):
        (ROOT / ".tmp").mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=ROOT / ".tmp", prefix="runner-tests-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(runner, "LOG_ROOT", self.root / "logs"))
        self.stack.enter_context(patch.object(runner, "BENCHMARK_BASELINE_PATH", self.root / "baseline.json"))
        self.stack.enter_context(patch.object(runner, "_lockfile_hashes", {}))
        runner._cancelled.clear()
        self.addCleanup(runner._cancelled.clear)

    def check(self, command="true", **kwargs):
        return runner.Check(key="fixture", title="Fixture", cwd=self.root, command=command, env={}, **kwargs)

    def python_command(self, source):
        return shlex.join([sys.executable, "-c", source])

    def main(self, args, checks, missing=None, rich=False):
        output, errors = io.StringIO(), io.StringIO()
        with patch.object(sys, "argv", ["checks", *args]), \
             patch.object(runner, "build_checks", return_value=checks), \
             patch.object(runner, "_check_skip_reason", side_effect=missing or (lambda check: None)), \
             patch.object(runner, "snapshot_lockfiles", return_value={}), \
             patch.object(runner, "HAS_RICH", rich), \
             contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = runner.main()
        return code, output.getvalue() + errors.getvalue()

    def registry(self):
        with patch.object(runner, "java_binding_env", return_value={}):
            return runner.build_checks()

    def test_fast_mode_contains_cheap_guardrails(self):
        keys = {check.key for check in runner.select_checks(self.registry(), [], "fast")}
        self.assertTrue({"whitespace", "vendored-headers", "release-metadata", "python-tooling-tests"} <= keys)
        self.assertNotIn("flutter-tests", keys)

    def test_registry_covers_independent_projects(self):
        checks = self.registry()
        keys = [check.key for check in checks]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertTrue({"rust-independent-checks", "rust-baseline-tests", "python-benchmark-tests",
                         "dotnet-standalone", "java-dbeaver", "java-ffm", "dart-analysis", "flutter-tests"} <= set(keys))
        audit = next(check.command for check in checks if check.key == "rust-supply-chain")
        self.assertIn("fuzz/Cargo.toml", audit)

    def test_dependency_restores_are_locked(self):
        checks = self.registry()
        for check in checks:
            with self.subTest(check=check.key):
                self.assertIsNone(re.search(r"cargo (build|check|clippy|metadata|test|run|bench|doc)\b(?! --locked)", check.command))
        commands = {check.key: check.command for check in checks}
        for key in ("node-tests", "node-knex-tests", "web-typecheck-build"):
            self.assertIn("npm ci", commands[key])
            check = next(check for check in checks if check.key == key)
            self.assertEqual(check.env["NPM_CONFIG_ENGINE_STRICT"], "true")
        for key in ("dart-tests", "dart-analysis", "flutter-tests"):
            self.assertIn("--enforce-lockfile", commands[key])

    def test_warning_checks_are_enabled(self):
        checks = {check.key: check for check in self.registry()}
        self.assertIn("-Wextra -Werror", checks["c-smoke"].command)
        self.assertIn("--strict", checks["mkdocs-build"].command)
        self.assertIn("-D warnings", checks["rust-docs"].env["RUSTDOCFLAGS"])
        self.assertIn("--fatal-infos", checks["dart-analysis"].command)

    def test_missing_toolchain_cannot_hide_behind_explicit_other_exclusion(self):
        checks = [self.check(), runner.replace(self.check(), key="excluded")]
        code, _ = self.main(["--skip", "excluded"], checks, missing=lambda check: "missing required tool")
        self.assertEqual(code, 2)

    def test_unknown_selection_fails(self):
        with self.assertRaises(SystemExit):
            runner.select_checks([self.check()], ["unknown"], "paranoid")

    def test_explicit_selection_overrides_mode(self):
        self.assertEqual(runner.select_checks([self.check()], ["fixture"], "fast"), [self.check()])

    def test_missing_toolchain_fails_even_for_only_check(self):
        code, output = self.main(["--only", "fixture"], [self.check()], missing=lambda check: "dart not found")
        self.assertEqual(code, 2)
        self.assertIn("dart not found", output)
        self.assertNotIn("PASSED", output)

    def test_all_excluded_cannot_pass(self):
        code, output = self.main(["--skip", "fixture"], [self.check()])
        self.assertEqual(code, 2)
        self.assertIn("no checks selected", output)

    def test_unknown_exclusion_fails(self):
        with self.assertRaises(SystemExit):
            self.main(["--skip", "unknown"], [self.check()])

    def test_paranoid_is_sequential_and_timeout_can_be_overridden(self):
        with patch.object(runner, "_run_plain", return_value=0) as render:
            code, _ = self.main(["--timeout-seconds", "12"], [self.check()])
        self.assertEqual(code, 0)
        self.assertTrue(render.call_args.args[6])
        self.assertEqual(render.call_args.args[0][0].check.timeout_seconds, 12)

    def test_invalid_timeout_fails(self):
        for timeout in ("0", "-1", "nan", "inf"):
            with self.subTest(timeout=timeout), self.assertRaises(SystemExit):
                self.main(["--timeout-seconds", timeout], [self.check()])

    def test_exit_code_and_logs_are_preserved(self):
        state = runner.CheckState(self.check("printf 'failure evidence'; exit 7"))
        runner.run_check(state, self.root, {})
        self.assertEqual((state.status, state.return_code), ("failed", 7))
        self.assertIn("failure evidence", state.log_path.read_text())

    def test_pipeline_failure_is_not_hidden(self):
        state = runner.CheckState(self.check("false | true"))
        runner.run_check(state, self.root, {})
        self.assertEqual(state.status, "failed")

    def test_launch_failure_is_reported(self):
        state = runner.CheckState(self.check())
        with patch.object(runner.subprocess, "Popen", side_effect=OSError("missing bash")):
            runner.run_check(state, self.root, {})
        self.assertEqual(state.status, "failed")
        self.assertIn("Could not start", state.detail)

    def test_dependency_file_changes_fail_check(self):
        path = self.root / "Cargo.lock"
        path.write_text("before")
        with patch.object(runner, "_lockfile_hashes", {path: runner.lockfile_digest(path)}):
            state = runner.CheckState(self.check(self.python_command("from pathlib import Path; Path('Cargo.lock').write_text('after')")))
            runner.run_check(state, self.root, {})
        self.assertEqual(state.status, "failed")
        self.assertIn("Cargo.lock", state.detail)

    def benchmark(self, update=False):
        label = "DecentDB vs SQLite"
        baseline = {"fixture": {label: {"decentdb": ["point reads"], "sqlite": []}}}
        report = "=== DecentDB vs SQLite ===\nDecentDB better at:\n- none\nSQLite better at:\n- point reads: faster\n"
        state = runner.CheckState(self.check(self.python_command(f"print({report!r})"), benchmark_comparison=True),
                                  update_benchmark_baselines=update)
        runner.run_check(state, self.root, baseline)
        return state, baseline, label

    def test_informational_regression_preserves_baseline(self):
        state, baseline, label = self.benchmark()
        self.assertEqual((state.status, state.benchmark_drift), ("passed", "REGRESSED"))
        self.assertEqual(baseline["fixture"][label]["decentdb"], ["point reads"])
        self.assertFalse(runner.BENCHMARK_BASELINE_PATH.exists())
        self.assertTrue((self.root / "fixture.comparison.json").exists())

    def test_explicit_baseline_update_is_persisted(self):
        state, baseline, label = self.benchmark(update=True)
        self.assertEqual(baseline["fixture"][label]["decentdb"], [])
        self.assertEqual(json.loads(runner.BENCHMARK_BASELINE_PATH.read_text()), baseline)
        self.assertEqual(list(self.root.glob("benchmark-baseline-*.tmp")), [])

    def test_renderers_report_exclusions_as_incomplete(self):
        for rich in (False, True) if runner.HAS_RICH else (False,):
            with self.subTest(rich=rich):
                code, output = self.main(["--skip", "excluded"], [self.check(), runner.replace(self.check(), key="excluded")], rich=rich)
                self.assertEqual(code, 0)
                self.assertIn("INCOMPLETE", output)
                summaries = sorted(runner.LOG_ROOT.glob("*/summary.json"))
                self.assertTrue(summaries)
                self.assertTrue(all(not json.loads(path.read_text())["complete"] for path in summaries))

    def test_renderers_propagate_failure_and_fail_fast(self):
        for rich in (False, True) if runner.HAS_RICH else (False,):
            with self.subTest(rich=rich):
                code, output = self.main(["--fail-fast"], [self.check("exit 9"), runner.replace(self.check(), key="later", stage=1)], rich=rich)
                self.assertEqual(code, 1)
                self.assertIn("FAILED", output)

    @unittest.skipUnless(os.name == "posix", "POSIX process group integration")
    def test_timeout_terminates_grandchild(self):
        child_path = self.root / "child-pid"
        child = f"import os, signal, time; from pathlib import Path; signal.signal(signal.SIGTERM, signal.SIG_IGN); Path({str(child_path)!r}).write_text(str(os.getpid())); time.sleep(60)"
        source = f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {child!r}]); time.sleep(60)"
        state = runner.CheckState(self.check(self.python_command(source), timeout_seconds=2))
        runner.run_check(state, self.root, {})
        self.assertEqual((state.status, state.return_code), ("failed", 124))
        self.assertIn("Timed out", state.log_path.read_text())
        self.assertTrue(child_path.exists(), "child should have started before the timeout")
        self.assert_process_stopped(int(child_path.read_text()))

    def assert_process_stopped(self, pid):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            proc = Path(f"/proc/{pid}/stat")
            try:
                if proc.exists() and proc.read_text().split()[2] == "Z":
                    return  # Already terminated, awaiting adoption/reaping by init.
            except FileNotFoundError:
                return
            time.sleep(0.02)
        self.fail(f"process {pid} survived cancellation")

    @unittest.skipUnless(os.name == "posix", "POSIX process group integration")
    def test_parallel_interrupt_stops_running_and_queued_checks(self):
        pid_path = self.root / "pid"
        source = f"import os, time; from pathlib import Path; Path({str(pid_path)!r}).write_text(str(os.getpid())); time.sleep(60)"
        first = runner.CheckState(self.check(self.python_command(source), cargo_bound=True))
        second = runner.CheckState(runner.replace(self.check(cargo_bound=True), key="queued"))
        first_started = threading.Event()
        run_check = runner.run_check

        def wait_for_first_check(state, *args):
            if state is second:
                self.assertTrue(first_started.wait(timeout=5))
            return run_check(state, *args)

        def interrupt_when_started(futures):
            deadline = time.monotonic() + 5
            while (not pid_path.exists() or not pid_path.stat().st_size) and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(pid_path.exists())
            first_started.set()
            while second.status == "pending" and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertEqual(second.status, "running")
            raise KeyboardInterrupt

        with patch.object(runner, "run_check", side_effect=wait_for_first_check), \
             patch.object(runner, "as_completed", side_effect=interrupt_when_started), \
             self.assertRaises(KeyboardInterrupt):
            runner._run_stage([(1, first), (2, second)], self.root, {}, False)
        self.assertEqual(second.status, "cancelled")
        self.assert_process_stopped(int(pid_path.read_text()))
        self.assertEqual(runner._processes, set())


if __name__ == "__main__":
    unittest.main()
