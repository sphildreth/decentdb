#!/usr/bin/env python3
"""
Crash-injection test orchestrator for DecentDb.

Runs scenarios that inject faults at specific points and verify:
- Committed data survives crashes
- Uncommitted data is not visible after recovery
- No corruption occurs
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional


def load_scenario(path: str) -> dict:
    """Load a crash-injection scenario from JSON file."""
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_failpoint_args(failpoint: dict) -> list[str]:
    """Build --walFailpoint CLI arguments from scenario failpoint config."""
    args = []
    if not failpoint:
        return args

    label = failpoint.get("label", "")
    kind = failpoint.get("kind", "error")
    bytes_count = failpoint.get("bytes", "")
    count = failpoint.get("count", "")

    # Format: label:kind[:bytes][:count]
    spec = label + ":" + kind
    if bytes_count:
        spec += ":" + str(bytes_count)
    if count:
        spec += ":" + str(count)

    # cligen expects --walFailpoints=<spec> (repeatable) rather than space-separated args.
    args.append(f"--walFailpoints={spec}")
    return args


def run_with_failpoint(
    engine_path: str, db_path: str, sql: str, failpoint: dict, timeout: float = 5.0
) -> tuple[subprocess.Popen, list[str]]:
    """Run the engine with a failpoint configured."""
    # NOTE: cligen requires --opt=value (not --opt value).
    cmd = [engine_path, "exec", f"--db={db_path}"]

    # Add failpoint arguments
    cmd.extend(build_failpoint_args(failpoint))

    if sql:
        cmd.append(f"--sql={sql}")

    # Start the process
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    return proc, cmd


def run_engine_normal(engine_path: str, db_path: str, sql: str) -> dict:
    """Run a normal SQL command and return the result."""
    cmd = [engine_path, "exec", f"--db={db_path}", f"--sql={sql}"]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()

    try:
        result = json.loads(stdout or "{}")
    except json.JSONDecodeError:
        result = {
            "ok": False,
            "error": {
                "code": "ERR_INTERNAL",
                "message": "Engine returned non-JSON output",
                "context": (stdout or stderr)[:2000],
            },
        }

    # If we got an empty object (common for CLI parse errors), surface stderr.
    if result == {}:
        result = {
            "ok": False,
            "error": {
                "code": "ERR_INTERNAL",
                "message": "Engine returned no JSON output",
                "context": (stderr or stdout)[:2000],
            },
        }

    result["exit_code"] = proc.returncode
    result["stderr"] = stderr
    return result


def run_engine_checkpoint(engine_path: str, db_path: str, failpoint: Optional[dict] = None) -> tuple[subprocess.CompletedProcess, list[str]]:
    """Run a checkpoint operation (optionally with a WAL failpoint)."""
    cmd = [engine_path, "exec", f"--db={db_path}", "--checkpoint"]
    if failpoint:
        cmd.extend(build_failpoint_args(failpoint))
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return proc, cmd


def verify_results(actual_rows: list, expected_rows: list, scenario_name: str) -> bool:
    """Verify actual results match expected results."""
    # Normalize row strings for comparison
    actual_normalized = [str(row).strip() for row in actual_rows]
    expected_normalized = [str(row).strip() for row in expected_rows]

    if actual_normalized == expected_normalized:
        print(f"  [OK] {scenario_name}: Results match")
        return True
    else:
        print(f"  [FAIL] {scenario_name}: Results mismatch")
        print(f"    Expected: {expected_normalized}")
        print(f"    Actual:   {actual_normalized}")
        return False


def run_crash_scenario(engine_path: str, scenario: dict, keep_db: bool = False) -> bool:
    """Run a single crash-injection scenario."""
    name = scenario.get("name", "unknown")
    print(f"\nRunning scenario: {name}")

    # Setup database path
    db_path = scenario.get("db_path", "")
    if not db_path:
        print(f"  [SKIP] {name}: No db_path specified")
        return True

    # Expand environment variables in path
    db_path = os.path.expandvars(os.path.expanduser(db_path))

    # Create temp directory if needed
    db_dir = Path(db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    # Clean up any existing database
    if Path(db_path).exists():
        Path(db_path).unlink()
    # Correct WAL suffix is "-wal", but check both for safety
    for suffix in ["-wal", ".wal"]:
        wal_path = db_path + suffix
        if Path(wal_path).exists():
            Path(wal_path).unlink()

    # Run setup SQL if present
    setup_sql = scenario.get("setup_sql", "")
    if setup_sql:
        print(f"  Running setup SQL...")
        # The CLI/engine expects a single statement per --sql invocation.
        # Many scenarios use semicolons to separate statements; run them sequentially.
        for stmt in [s.strip() for s in setup_sql.split(";")]:
            if not stmt:
                continue
            result = run_engine_normal(engine_path, db_path, stmt)
            if not result.get("ok", False):
                print(f"  [FAIL] {name}: Setup failed - {result.get('error', {})}")
                if result.get("stderr"):
                    print(f"    stderr: {result.get('stderr')}")
                return False

    # Get scenario components
    sql = scenario.get("sql", "")
    failpoint = scenario.get("failpoint", {})
    verify = scenario.get("verify", {})
    expect_crash = verify.get("expect_crash", True)
    run_checkpoint = bool(scenario.get("checkpoint", False))

    if not failpoint:
        print(f"  [SKIP] {name}: No failpoint configured")
        return True

    if run_checkpoint:
        print(f"  Executing checkpoint with failpoint: {failpoint.get('label', 'unknown')}")
        completed, cmd = run_engine_checkpoint(engine_path, db_path, failpoint)
        stdout, stderr = completed.stdout, completed.stderr
        exit_code = completed.returncode
    else:
        # Run with failpoint
        print(f"  Executing with failpoint: {failpoint.get('label', 'unknown')}")
        proc, cmd = run_with_failpoint(engine_path, db_path, sql, failpoint)

        # Wait for the process
        try:
            stdout, stderr = proc.communicate(timeout=10.0)
            exit_code = proc.returncode
        except subprocess.TimeoutExpired:
            # Process might be hanging due to failpoint - kill it
            proc.kill()
            stdout, stderr = proc.communicate()
            exit_code = -1

    # Check if we expected a crash
    if expect_crash:
        # Process should have crashed or exited with error
        if exit_code == 0 and not failpoint.get("count"):
            print(f"  [WARN] {name}: Expected crash but process completed successfully")
        else:
            print(
                f"  [OK] {name}: Process terminated as expected (exit code: {exit_code})"
            )
    else:
        if exit_code != 0:
            print(f"  [FAIL] {name}: Unexpected crash (exit code: {exit_code})")
            print(f"    stderr: {stderr}")
            return False
        else:
            print(f"  [OK] {name}: Process completed successfully")

    # Post-crash verification
    post_crash_sql = verify.get("post_crash_sql", "") or verify.get("post_sql", "")
    if post_crash_sql:
        print(f"  Verifying post-crash state...")
        result = run_engine_normal(engine_path, db_path, post_crash_sql)

        if not result.get("ok", False):
            # Some errors might be expected (e.g., table doesn't exist)
            error_code = result.get("error", {}).get("code", "")
            if error_code == "ERR_SQL":
                # SQL error might be expected if transaction was rolled back
                print(f"  [INFO] {name}: Post-crash SQL failed with {error_code}")
            else:
                print(
                    f"  [FAIL] {name}: Post-crash query failed - {result.get('error', {})}"
                )
                return False

        actual_rows = result.get("rows", [])
        expected_rows = verify.get("expect_rows", [])

        if not verify_results(actual_rows, expected_rows, name):
            return False

    # Cleanup
    if not keep_db:
        if Path(db_path).exists():
            Path(db_path).unlink()
        for suffix in ["-wal", ".wal"]:
            wal_path = db_path + suffix
            if Path(wal_path).exists():
                Path(wal_path).unlink()

    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Crash-injection test orchestrator for DecentDb"
    )
    parser.add_argument(
        "--engine", required=True, help="Path to decentdb CLI executable"
    )
    parser.add_argument("--scenario", help="Path to specific scenario JSON file")
    parser.add_argument(
        "--scenarios-dir", default=None, help="Directory containing scenario files"
    )
    parser.add_argument("--list", action="store_true", help="List available scenarios")
    parser.add_argument(
        "--keep-db", action="store_true", help="Keep database files after test"
    )
    args = parser.parse_args()

    # Determine scenarios to run
    if args.scenario:
        scenario_files = [args.scenario]
    elif args.scenarios_dir:
        scenarios_path = Path(args.scenarios_dir)
        scenario_files = sorted(scenarios_path.glob("*.json"))
    else:
        # Default to tests/harness/scenarios
        repo_root = Path(__file__).resolve().parents[2]
        scenarios_path = repo_root / "tests" / "harness" / "scenarios"
        scenario_files = sorted(scenarios_path.glob("*.json"))

    if not scenario_files:
        print("No scenario files found")
        return 1

    # Filter out non-crash scenarios (like open_close.json)
    crash_scenarios = []
    for f in scenario_files:
        try:
            scenario = load_scenario(str(f))
            if "failpoint" in scenario:
                crash_scenarios.append((f, scenario))
        except (json.JSONDecodeError, IOError):
            continue

    if args.list:
        print("Available crash-injection scenarios:")
        for f, scenario in crash_scenarios:
            print(f"  - {f.name}: {scenario.get('description', 'No description')}")
        return 0

    if not crash_scenarios:
        print("No crash-injection scenarios found")
        return 1

    # Run all scenarios
    print(f"Found {len(crash_scenarios)} crash-injection scenarios")
    print(f"Engine: {args.engine}")

    passed = 0
    failed = 0

    for scenario_file, scenario in crash_scenarios:
        try:
            if run_crash_scenario(args.engine, scenario, args.keep_db):
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  [ERROR] Exception running {scenario.get('name', 'unknown')}: {e}")
            failed += 1

    # Summary
    print(f"\n{'=' * 50}")
    print(f"Crash-injection test results: {passed} passed, {failed} failed")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
