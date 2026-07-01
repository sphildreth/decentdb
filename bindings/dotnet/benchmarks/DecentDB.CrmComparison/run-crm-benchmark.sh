#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_DIR="$SCRIPT_DIR/.."
PROJECT="$PROJECT_DIR/DecentDB.CrmComparison/DecentDB.CrmComparison.csproj"

OUT_ROOT="${1:-$(pwd)/.tmp/decentdb-crm-comparison}"
SIZE="${2:-Small}"
DURABILITY="${3:-relaxed}"
ITERATIONS="${4:-5}"
WARMUP="${5:-1}"
NATIVE="${6:-0}"
SEED="${7:-42}"
ENGINES="${8:-all}"

if [[ "$ENGINES" != "all" && "$ENGINES" != "decentdb" && "$ENGINES" != "sqlite" ]]; then
  echo "Unknown engines '$ENGINES'. Use all, decentdb, or sqlite." >&2
  exit 1
fi

mkdir -p "$OUT_ROOT"
STAMP="$(date +%Y%m%d%H%M%S)"
RUN_DIR="$OUT_ROOT/$STAMP"
mkdir -p "$RUN_DIR"

LOG_PATH="$RUN_DIR/benchmark.log"
JSON_PATH="$RUN_DIR/results.json"

DOTNET_OPTIONS=(
  "dotnet"
  "run"
  "-c"
  "Release"
  "--project"
  "$PROJECT"
  "--"
  "--size"
  "$SIZE"
  "--iterations"
  "$ITERATIONS"
  "--warmup-iterations"
  "$WARMUP"
  "--seed"
  "$SEED"
  "--out-dir"
  "$RUN_DIR"
  "--durability"
  "$DURABILITY"
  "--json"
  "$JSON_PATH"
  "--engines"
  "$ENGINES"
)

if [[ "$NATIVE" == "1" ]]; then
  DOTNET_OPTIONS+=(--decentdb-native-hot-paths)
else
  DOTNET_OPTIONS+=(--no-decentdb-native-hot-paths)
fi

printf 'Running benchmark:\n'
printf 'command: %s\n' "${DOTNET_OPTIONS[*]}"
printf 'out-dir: %s\n' "$RUN_DIR"
printf 'json: %s\n' "$JSON_PATH"
printf 'log: %s\n' "$LOG_PATH"
{
  printf 'Benchmark command: %s\n' "${DOTNET_OPTIONS[*]}"
  printf 'Started UTC: %s\n' "$(date -u +%FT%TZ)"
  printf 'Output directory: %s\n' "$RUN_DIR"
  printf 'JSON file: %s\n' "$JSON_PATH"
  printf '---\n'
} > "$LOG_PATH"

"${DOTNET_OPTIONS[@]}" 2>&1 | tee -a "$LOG_PATH"

echo "Results:"
echo "  out: $RUN_DIR"
echo "  json: $JSON_PATH"
