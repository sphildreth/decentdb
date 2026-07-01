#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_DIR="$SCRIPT_DIR"
RUNNER="$PROJECT_DIR/run-crm-benchmark.sh"

usage() {
  cat <<'USAGE'
Usage:
  run-crm-benchmark-matrix.sh [options]

Options:
  --out-dir PATH          Output root directory (default: .tmp/crm-benchmark-matrix)
  --size SIZE             Tiny|Small|Medium|Large|Jumbo (default: Small)
  --iterations N          Measurement iterations per mode (default: 3)
  --warmup N              Warmup iterations per mode (default: 1)
  --seed N                Dataset seed (default: 42)
  --run-id ID             Stable matrix output suffix (default: date/timestamp)
  --collect-allocations    Enable managed allocation telemetry for all matrix modes (default: off)
  --no-collect-allocations Explicitly disable managed allocation telemetry (default)
  --native-on             Enable native DecentDB modes (decentdb-only harness)
  --skip-native           Skip native entries regardless of native availability
  --help                  Show this help text

This runs six modes by default:
  - DecentDB ADO.NET vs SQLite (relaxed)
  - DecentDB ADO.NET vs SQLite (durable)
  - SQLite ADO.NET only (relaxed)
  - SQLite ADO.NET only (durable)
  - DecentDB native ADO path only (relaxed) [optional]
  - DecentDB native ADO path only (durable) [optional]

Each mode writes its own JSON, logs, and database artifacts into a mode-specific
subdirectory. Each mode now also emits `validation.json` for summary completeness and
regression checks used by CI.
USAGE
}

OUT_DIR=".tmp/crm-benchmark-matrix"
SIZE="Small"
ITERATIONS=3
WARMUP=1
SEED=42
NATIVE_ON=0
SKIP_NATIVE=0
COLLECT_ALLOCATIONS=0
RUN_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR=$2
      shift 2
      ;;
    --size)
      SIZE=$2
      shift 2
      ;;
    --iterations)
      ITERATIONS=$2
      shift 2
      ;;
    --warmup)
      WARMUP=$2
      shift 2
      ;;
    --seed)
      SEED=$2
      shift 2
      ;;
    --run-id)
      RUN_ID=$2
      shift 2
      ;;
    --collect-allocations)
      COLLECT_ALLOCATIONS=1
      shift
      ;;
    --no-collect-allocations)
      COLLECT_ALLOCATIONS=0
      shift
      ;;
    --native-on)
      NATIVE_ON=1
      shift
      ;;
    --skip-native)
      SKIP_NATIVE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$NATIVE_ON" -eq 1 && "$SKIP_NATIVE" -eq 1 ]]; then
  echo "Conflicting options: --native-on and --skip-native" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
if [[ -n "$RUN_ID" ]]; then
  if [[ "$RUN_ID" == *"/"* ]]; then
    echo "Invalid --run-id '$RUN_ID': slash is not allowed." >&2
    exit 1
  fi
  if [[ "$RUN_ID" == *[[:space:]]* ]]; then
    echo "Invalid --run-id '$RUN_ID': whitespace is not allowed." >&2
    exit 1
  fi
  MATRIX_DIR="$OUT_DIR/$RUN_ID"
else
  MATRIX_DIR="$OUT_DIR/$STAMP"
fi
mkdir -p "$MATRIX_DIR"

run_mode() {
  local mode="$1"
  local durable="$2"
  local native="$3"
  local engines="$4"
  local expected_engines="$5"
  local mode_dir="$MATRIX_DIR/$mode"

  mkdir -p "$mode_dir"
  printf '\n=== Running mode: %s ===\n' "$mode"

  CRM_BENCHMARK_FIXED_RUN_DIR=1 bash "$RUNNER" \
    "$mode_dir" \
    "$SIZE" \
    "$durable" \
    "$ITERATIONS" \
    "$WARMUP" \
    "$([ "$native" == "1" ] && echo 1 || echo 0)" \
    "$SEED" \
    "$engines" \
    "$COLLECT_ALLOCATIONS"

  # Persist an easy mode marker for downstream scripts.
  echo "{\"mode\":\"$mode\",\"size\":\"$SIZE\",\"durability\":\"$durable\",\"seed\":$SEED,\"iterations\":$ITERATIONS,\"warmup\":$WARMUP,\"native\":${native},\"engines\":\"$engines\",\"collect_allocations\":${COLLECT_ALLOCATIONS}}" \
    > "$mode_dir/mode.json"

  if [[ ! -f "$mode_dir/results.json" ]]; then
    echo "Expected results file was not written for $mode at $mode_dir/results.json" >&2
    exit 1
  fi

  local validate_log
  validate_log="$mode_dir/validation.json"
  python "$SCRIPT_DIR/compare-crm-benchmark.py" \
    --current "$mode_dir/results.json" \
    --require-complete \
    --expected-engines "$expected_engines" \
    --output "$validate_log"
}

run_mode "ado-relaxed" "relaxed" 0 all "DecentDB,SQLite"
run_mode "ado-durable" "durable" 0 all "DecentDB,SQLite"
run_mode "sqlite-relaxed" "relaxed" 0 sqlite "SQLite"
run_mode "sqlite-durable" "durable" 0 sqlite "SQLite"

if [[ "$SKIP_NATIVE" -eq 0 ]]; then
  if [[ "$NATIVE_ON" -eq 0 ]]; then
    printf '\nNative modes disabled by default because full-suite native mode can be unstable on this machine.\n'
    printf 'Rerun with --native-on to attempt native modes.\n'
  else
    run_mode "native-relaxed" "relaxed" 1 decentdb "DecentDB"
    run_mode "native-durable" "durable" 1 decentdb "DecentDB"
  fi
fi

python - "$MATRIX_DIR" <<'PY'
import json
from pathlib import Path
import sys


matrix_dir = Path(sys.argv[1])
mode_data = {}
for mode_dir in sorted(path for path in matrix_dir.iterdir() if path.is_dir()):
    result_path = mode_dir / "results.json"
    if not result_path.exists():
        continue

    payload = json.loads(result_path.read_text(encoding="utf-8"))
    manifest = payload.get("Manifest") or payload.get("manifest") or {}
    rows = payload.get("Summary") or payload.get("summary") or payload.get("summaries") or []

    scenario_summary = {}
    for row in rows:
        scenario = row.get("Scenario")
        engine = row.get("Engine")
        if scenario is None or engine is None:
            continue
        key = f"{scenario}|{engine}"
        scenario_summary[key] = {
            "mean_ms": row.get("MeanMs"),
            "iterations": row.get("Iterations", 0),
            "p95_ms": row.get("P95Ms"),
            "stddev_ms": row.get("StdDevMs"),
            "mean_alloc_bytes": row.get("MeanAllocatedBytes"),
        }

    mode_data[mode_dir.name] = {
        "manifest": {
            "benchmark": manifest.get("Benchmark"),
            "scenario_size": manifest.get("ScenarioSize"),
            "durability": manifest.get("DurabilityProfile"),
            "iterations": manifest.get("Iterations"),
            "warmup_iterations": manifest.get("WarmupIterations"),
            "engine_order": manifest.get("engine_order", []),
            "decentdb_native_hot_paths_active": manifest.get("NativeDecentDbHotPathsActive"),
            "use_native_decentdb_hot_paths": manifest.get("UseNativeDecentDbHotPaths"),
            "collect_allocations": manifest.get("CollectAllocations"),
        },
        "scenario_count": len(scenario_summary),
        "scenarios": scenario_summary,
    }

output_path = matrix_dir / "matrix-summary.json"
output_path.write_text(json.dumps(mode_data, indent=2), encoding="utf-8")
print(f"Wrote matrix summary to {output_path}")
PY

printf '\nBenchmark matrix written to: %s\n' "$MATRIX_DIR"
