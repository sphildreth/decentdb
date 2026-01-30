import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

if __package__:
    from .seed_utils import choose_seed, record_seed, read_seed
else:
    sys.path.append(str(Path(__file__).resolve().parent))
    from seed_utils import choose_seed, record_seed, read_seed


def load_scenario(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_failpoint_args(failpoint: dict) -> list[str]:
    """Build --walFailpoint CLI arguments from failpoint config."""
    if not failpoint:
        return []

    label = failpoint.get("label", "")
    kind = failpoint.get("kind", "error")
    bytes_count = failpoint.get("bytes", "")
    count = failpoint.get("count", "")

    # Format: label:kind[:bytes][:count]
    spec = f"{label}:{kind}"
    if bytes_count != "":
        spec += f":{bytes_count}"
    if count != "":
        spec += f":{count}"

    return ["--walFailpoint", spec]


def build_engine_command(
    engine_path: str,
    db_path: str,
    sql: str | None,
    open_close: bool,
    failpoint: dict | None = None,
) -> list[str]:
    cmd = [engine_path]
    if engine_path.endswith(".py"):
        cmd = [sys.executable, engine_path]
    else:
        engine_name = Path(engine_path).name
        if engine_name in {"decentdb", "decentdb.exe"}:
            cmd.append("exec")
    cmd += ["--db", db_path]

    # Add failpoint arguments if present
    if failpoint:
        cmd.extend(build_failpoint_args(failpoint))

    if open_close:
        cmd += ["--open-close"]
    elif sql:
        cmd += ["--sql", sql]
    return cmd


def run_engine(
    engine_path: str,
    db_path: str,
    sql: str | None,
    open_close: bool,
    failpoint: dict | None = None,
) -> dict:
    cmd = build_engine_command(engine_path, db_path, sql, open_close, failpoint)
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    try:
        payload = json.loads(proc.stdout.strip() or "{}")
    except json.JSONDecodeError:
        payload = {
            "ok": False,
            "error": {
                "code": "ERR_INTERNAL",
                "message": "Engine returned non-JSON output",
                "context": proc.stdout.strip(),
            },
        }
    payload["exit_code"] = proc.returncode
    payload["stderr"] = proc.stderr.strip()
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True, help="Path to engine CLI or script")
    parser.add_argument("--scenario", required=True, help="Path to scenario JSON")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--seed-file")
    parser.add_argument("--seed-log")
    args = parser.parse_args()

    scenario = load_scenario(args.scenario)
    seed_log = args.seed_log or str(Path(__file__).resolve().parent / "seed.log")
    if args.seed_file:
        seed = read_seed(args.seed_file)
    else:
        seed = choose_seed(args.seed)
    record_seed(seed, seed_log)

    db_path = os.path.expandvars(os.path.expanduser(scenario.get("db_path", "")))
    if not db_path:
        return_json = {
            "ok": False,
            "error": {
                "code": "ERR_IO",
                "message": "Scenario missing db_path",
                "context": "",
            },
            "seed": seed,
        }
        print(json.dumps(return_json))
        return 1

    open_close = bool(scenario.get("open_close", False))
    sql = scenario.get("sql")
    failpoint = scenario.get("failpoint")

    engine_result = run_engine(args.engine, db_path, sql, open_close, failpoint)
    result = {
        "scenario": scenario.get("name", "unknown"),
        "seed": seed,
        "engine": args.engine,
        "ok": bool(engine_result.get("ok")),
        "engine_result": engine_result,
    }
    print(json.dumps(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
