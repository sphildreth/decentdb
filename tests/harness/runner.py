#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path


def default_db_filename(name: str) -> str:
    safe_name = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name
    )
    return f"{safe_name or 'scenario'}.ddb"


def run_scenario(repo_root: Path, scenario_file: Path) -> int:
    with scenario_file.open("r", encoding="utf-8") as handle:
        scenario = json.load(handle)

    name = str(scenario.get("name", "scenario"))
    with tempfile.TemporaryDirectory(prefix=f"decentdb-harness-{name}-") as temp_dir:
        scenario.setdefault("path", str(Path(temp_dir) / default_db_filename(name)))

        with tempfile.NamedTemporaryFile(
            "w", suffix=".json", dir=temp_dir, delete=False, encoding="utf-8"
        ) as tmp:
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
