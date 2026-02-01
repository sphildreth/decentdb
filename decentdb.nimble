version       = "0.0.1"
author        = "DecentDb contributors"
description   = "DecentDb engine (beta)"
license       = "Apache-2.0"
srcDir        = "src"
bin           = @["decentdb"]

requires "nim >= 1.6.0"
requires "cligen >= 1.7.0"
requires "zip >= 0.3.1"


task build_lib, "Build DecentDb shared library (C API)":
  exec "nim c --app:lib -d:libpg_query -d:release --gc:arc --threads:on --outdir:build src/c_api.nim"

task test, "Run Nim + Python unit tests":
  exec "sh -c 'set -e; for f in $(ls tests/nim/test_*.nim | sort); do nim c --hints:off -r \"$f\"; done'"
  exec "python -m unittest -q tests/harness/test_runner.py"

task test_nim, "Run Nim unit tests":
  exec "sh -c 'set -e; for f in $(ls tests/nim/test_*.nim | sort); do nim c --hints:off -r \"$f\"; done'"

task test_py, "Run Python harness tests":
  exec "python -m unittest -q tests/harness/test_runner.py"

task coverage_nim, "Run Nim unit tests with gcov coverage report":
  exec "scripts/coverage_nim.sh"

task lint, "Static checks for Nim + Python":
  exec "nim check src/decentdb.nim"
  exec "nim check src/decentdb_cli.nim"
  exec "nim check src/engine.nim"
  exec "nim check src/vfs/vfs.nim"
  exec "nim check src/vfs/os_vfs.nim"
  exec "nim check src/vfs/faulty_vfs.nim"
  exec "nim check src/pager/db_header.nim"
  exec "nim check src/pager/pager.nim"
  exec "nim check src/record/record.nim"
  exec "nim check src/btree/btree.nim"
  exec "nim check src/catalog/catalog.nim"
  exec "nim check src/storage/storage.nim"
  exec "nim check src/sql/pg_query_ffi.nim"
  exec "nim check src/sql/sql.nim"
  exec "nim check src/sql/binder.nim"
  exec "nim check src/planner/planner.nim"
  exec "nim check src/exec/exec.nim"
  exec "nim check src/search/search.nim"
  exec "nim check src/wal/wal.nim"
  exec "nim check tests/nim/test_faulty_vfs.nim"
  exec "nim check tests/nim/test_db_header.nim"
  exec "nim check tests/nim/test_pager.nim"
  exec "nim check tests/nim/test_record.nim"
  exec "nim check tests/nim/test_btree.nim"
  exec "nim check tests/nim/test_wal.nim"
  exec "nim check tests/nim/test_wal_extra.nim"
  exec "nim check tests/nim/test_sql_exec.nim"
  exec "nim check tests/nim/test_binder.nim"
  exec "nim check tests/nim/test_sql_parser.nim"
  exec "nim check tests/nim/test_catalog.nim"
  exec "nim check tests/nim/test_engine.nim"
  exec "nim check tests/nim/test_constraints.nim"
  exec "nim check tests/nim/test_trigram.nim"
  exec "nim check tests/nim/test_exec.nim"
  exec "nim check tests/nim/test_storage.nim"
  exec "nim check tests/nim/test_sort_spill.nim"
  exec "nim check tests/nim/test_bulk_load.nim"
  exec "nim check tests/bench/bench.nim"
  exec "nim check tests/bench/bench_large.nim"
  exec "python -m compileall tests/bench"
  exec "python -m compileall tests/harness"

task bench, "Run microbenchmarks":
  exec "nim c -r tests/bench/bench.nim -- tests/bench/results.json"

task bench_compare, "Run microbenchmarks and compare to baseline":
  exec "nim c -r tests/bench/bench.nim -- tests/bench/results.json"
  exec "python tests/bench/compare_bench.py tests/bench/results.json tests/bench/baseline.json tests/bench/thresholds.json"

task bench_large, "Run large/concurrency benchmarks (may be slow)":
  exec "nim c --threads:on -r tests/bench/bench_large.nim -- tests/bench/results_large.json"

task bench_large_compare, "Run large/concurrency benchmarks and compare to baseline (may be slow)":
  exec "nim c --threads:on -r tests/bench/bench_large.nim -- tests/bench/results_large.json"
  exec "python tests/bench/compare_bench.py tests/bench/results_large.json tests/bench/baseline_large.json tests/bench/thresholds_large.json"
