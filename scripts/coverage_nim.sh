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

rm -rf "$BUILD_DIR"
mkdir -p "$NIMCACHE_ROOT" "$GCOV_DIR"

COVERAGE_FLAGS=(
  --passC:-fprofile-arcs
  --passC:-ftest-coverage
  --passL:-fprofile-arcs
  --passL:-ftest-coverage
)

TESTS=(
  tests/nim/test_faulty_vfs.nim
  tests/nim/test_db_header.nim
  tests/nim/test_pager.nim
  tests/nim/test_record.nim
  tests/nim/test_btree.nim
  tests/nim/test_wal.nim
  tests/nim/test_sql_exec.nim
  tests/nim/test_constraints.nim
  tests/nim/test_trigram.nim
  tests/nim/test_exec.nim
  tests/nim/test_storage.nim
  tests/nim/test_sort_spill.nim
  tests/nim/test_bulk_load.nim
)

for test in "${TESTS[@]}"; do
  name="$(basename "$test" .nim)"
  cache_dir="$NIMCACHE_ROOT/$name"
  out_dir="$GCOV_DIR/$name"
  mkdir -p "$cache_dir" "$out_dir"
  nim c -r "${COVERAGE_FLAGS[@]}" --nimcache:"$cache_dir" "$ROOT/$test"
  while IFS= read -r -d '' obj; do
    obj_dir="$(dirname "$obj")"
    (cd "$out_dir" && gcov -o "$obj_dir" "$obj" >/dev/null)
  done < <(find "$cache_dir" -name '*.c.o' -print0)
done

python "$ROOT/scripts/coverage_summary.py" "$GCOV_DIR" "$ROOT" "$SUMMARY" "$SUMMARY_JSON"

echo "Coverage summary: $SUMMARY"
