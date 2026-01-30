#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT/build/coverage"
NIMCACHE_ROOT="$BUILD_DIR/nimcache"
GCOV_DIR="$BUILD_DIR/gcov"
SUMMARY="$BUILD_DIR/summary.txt"
SUMMARY_JSON="$BUILD_DIR/summary.json"

if ! command -v gcov >/dev/null 2>&1; then
  echo "gcov not found. Install GCC gcov to generate coverage." >&2
  exit 1
fi

# Clear build/coverage/ directory
echo "Clearing build/coverage/ directory..."
rm -rf "$BUILD_DIR"
mkdir -p "$NIMCACHE_ROOT" "$GCOV_DIR"

COVERAGE_FLAGS=(
  --passC:-fprofile-arcs
  --passC:-ftest-coverage
  --passL:-fprofile-arcs
  --passL:-ftest-coverage
)

# All tests array
TESTS=(
  tests/nim/test_faulty_vfs.nim
  tests/nim/test_db_header.nim
  tests/nim/test_pager.nim
  tests/nim/test_record.nim
  tests/nim/test_btree.nim
  tests/nim/test_wal.nim
  tests/nim/test_wal_extra.nim
  tests/nim/test_sql_exec.nim
  tests/nim/test_binder.nim
  tests/nim/test_sql_parser.nim
  tests/nim/test_catalog.nim
  tests/nim/test_engine.nim
  tests/nim/test_constraints.nim
  tests/nim/test_trigram.nim
  tests/nim/test_exec.nim
  tests/nim/test_storage.nim
  tests/nim/test_storage_extra.nim
  tests/nim/test_storage_more.nim
  tests/nim/test_sort_spill.nim
  tests/nim/test_bulk_load.nim
  tests/nim/test_exec_plan_coverage.nim
  tests/nim/test_planner_extra.nim
  tests/nim/test_sql_helpers.nim
  tests/nim/test_engine_comprehensive.nim
  tests/nim/test_exec_helpers.nim
  tests/nim/test_engine_edge_cases.nim
  tests/nim/test_exec_comprehensive.nim
  tests/nim/test_storage_deep.nim
  tests/nim/test_vfs_comprehensive.nim
)

TOTAL_TESTS=${#TESTS[@]}
echo "Total tests to run: $TOTAL_TESTS"

# Function to collect gcov data for a test
collect_gcov() {
  local name="$1"
  local cache_dir="$NIMCACHE_ROOT/$name"
  local out_dir="$GCOV_DIR/$name"
  mkdir -p "$out_dir"
  
  while IFS= read -r -d '' obj; do
    obj_dir="$(dirname "$obj")"
    (cd "$out_dir" && gcov -o "$obj_dir" "$obj" >/dev/null)
  done < <(find "$cache_dir" -name '*.c.o' -print0)
}

# Batch 1: First 10 tests
echo ""
echo "=========================================="
echo "BATCH 1: Tests 1-10"
echo "=========================================="
for i in {0..9}; do
  if [ $i -ge $TOTAL_TESTS ]; then break; fi
  test="${TESTS[$i]}"
  name="$(basename "$test" .nim)"
  echo "Running test: $name"
  cache_dir="$NIMCACHE_ROOT/$name"
  mkdir -p "$cache_dir"
  nim c -r "${COVERAGE_FLAGS[@]}" --nimcache:"$cache_dir" "$ROOT/$test"
  collect_gcov "$name"
done

# Batch 2: Next 10 tests
echo ""
echo "=========================================="
echo "BATCH 2: Tests 11-20"
echo "=========================================="
for i in {10..19}; do
  if [ $i -ge $TOTAL_TESTS ]; then break; fi
  test="${TESTS[$i]}"
  name="$(basename "$test" .nim)"
  echo "Running test: $name"
  cache_dir="$NIMCACHE_ROOT/$name"
  mkdir -p "$cache_dir"
  nim c -r "${COVERAGE_FLAGS[@]}" --nimcache:"$cache_dir" "$ROOT/$test"
  collect_gcov "$name"
done

# Batch 3: Remaining tests
echo ""
echo "=========================================="
echo "BATCH 3: Tests 21-$TOTAL_TESTS"
echo "=========================================="
for i in $(seq 20 $(($TOTAL_TESTS - 1))); do
  test="${TESTS[$i]}"
  name="$(basename "$test" .nim)"
  echo "Running test: $name"
  cache_dir="$NIMCACHE_ROOT/$name"
  mkdir -p "$cache_dir"
  nim c -r "${COVERAGE_FLAGS[@]}" --nimcache:"$cache_dir" "$ROOT/$test"
  collect_gcov "$name"
done

# Run coverage summary
echo ""
echo "=========================================="
echo "Generating coverage summary..."
echo "=========================================="
python "$ROOT/scripts/coverage_summary.py" "$GCOV_DIR" "$ROOT" "$SUMMARY" "$SUMMARY_JSON"

echo ""
echo "Coverage summary: $SUMMARY"
echo "Coverage JSON: $SUMMARY_JSON"
echo ""
echo "Overall coverage:"
head -1 "$SUMMARY"
