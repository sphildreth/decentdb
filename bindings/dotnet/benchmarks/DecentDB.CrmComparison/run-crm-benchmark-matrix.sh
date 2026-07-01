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
subdirectory.
USAGE
}

OUT_DIR=".tmp/crm-benchmark-matrix"
SIZE="Small"
ITERATIONS=3
WARMUP=1
SEED=42
NATIVE_ON=0
SKIP_NATIVE=0
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
  local mode_dir="$MATRIX_DIR/$mode"

  mkdir -p "$mode_dir"
  printf '\n=== Running mode: %s ===\n' "$mode"

  bash "$RUNNER" \
    "$mode_dir" \
    "$SIZE" \
    "$durable" \
    "$ITERATIONS" \
    "$WARMUP" \
    "$([ "$native" == "1" ] && echo 1 || echo 0)" \
    "$SEED" \
    "$engines"

  # Persist an easy mode marker for downstream scripts.
  echo "{\"mode\":\"$mode\",\"size\":\"$SIZE\",\"durability\":\"$durable\",\"seed\":$SEED,\"iterations\":$ITERATIONS,\"warmup\":$WARMUP,\"native\":${native},\"engines\":\"$engines\"}" \
    > "$mode_dir/mode.json"
}

run_mode "ado-relaxed" "relaxed" 0 all
run_mode "ado-durable" "durable" 0 all
run_mode "sqlite-relaxed" "relaxed" 0 sqlite
run_mode "sqlite-durable" "durable" 0 sqlite

if [[ "$SKIP_NATIVE" -eq 0 ]]; then
  if [[ "$NATIVE_ON" -eq 0 ]]; then
    printf '\nNative modes disabled by default because full-suite native mode can be unstable on this machine.\n'
    printf 'Rerun with --native-on to attempt native modes.\n'
  else
    run_mode "native-relaxed" "relaxed" 1 decentdb
    run_mode "native-durable" "durable" 1 decentdb
  fi
fi

printf '\nBenchmark matrix written to: %s\n' "$MATRIX_DIR"
