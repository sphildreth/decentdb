#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path


def run_scenario(repo_root: Path, scenario_path: Path) -> int:
    with scenario_path.open("r", encoding="utf-8") as handle:
        scenario = json.load(handle)

    scenario.setdefault("path", str(Path(tempfile.gettempdir()) / f"{scenario.get('name', 'scenario')}.ddb"))

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(scenario, tmp)
        tmp.flush()
        tmp_path = Path(tmp.name)

    command = [
        "cargo",
        "run",
        "-q",
        "-p",
        "decentdb",
        "--bin",
        "decentdb-test-harness",
        "--",
        str(tmp_path),
    ]
    result = subprocess.run(command, cwd=repo_root, text=True, capture_output=True)
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a DecentDB storage harness scenario")
    parser.add_argument("scenario", type=Path, help="Path to the scenario JSON file")
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    return run_scenario(repo_root, args.scenario)


if __name__ == "__main__":
    raise SystemExit(main())
