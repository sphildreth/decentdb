version       = "0.1.0"
author        = "DecentDB contributors"
description   = "DecentDB engine"
license       = "Apache-2.0"
srcDir        = "src"
bin           = @["decentdb"]

requires "nim >= 1.6.0"
requires "cligen >= 1.7.0"
requires "zip >= 0.3.1"


task build_lib, "Build DecentDB shared library (C API)":
  exec "nim c --app:lib -d:libpg_query -d:release --mm:arc --threads:on --outdir:build src/c_api.nim"

task test_bindings_dotnet, "Run .NET binding tests":
  exec "dotnet test bindings/dotnet/tests/DecentDB.Tests"

task test_bindings_go, "Run Go binding tests":
  withDir "bindings/go/decentdb-go":
    exec "go test -v ."

task test_bindings_node, "Run Node.js binding tests":
  withDir "bindings/node/decentdb":
    # Ensure native addon is built
    exec "npm run build"
    # Point to the shared library built by build_lib
    exec "export DECENTDB_NATIVE_LIB_PATH=$PWD/../../../build/libc_api.so && npm test"

task test_bindings_python, "Run Python binding tests":
  withDir "bindings/python":
    exec "pytest"

task test_bindings, "Run all binding tests":
  exec "nimble build_lib"
  exec "nimble test_bindings_dotnet"
  exec "nimble test_bindings_go"
  exec "nimble test_bindings_node"
  exec "nimble test_bindings_python"

task test, "Run Nim + Python unit tests + Bindings":
  exec "nimble test_nim"
  exec "python -m unittest -q tests/harness/test_runner.py"
  exec "nimble test_bindings"

task test_nim, "Run Nim unit tests":
  # Use testament for parallel test execution and better reporting
  try:
    exec "testament pattern \"tests/nim/*.nim\""
  finally:
    exec "testament html"

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

task bench_embedded, "Run embedded database comparison benchmarks":
  exec "nim c --warnings:off -d:libpg_query -d:release --mm:arc --threads:on --outdir:build benchmarks/embedded_compare/run_benchmarks.nim"

task bench_embedded_sample, "Run embedded benchmarks and aggregate sample data":
  exec "nimble bench_embedded"
  exec "./build/run_benchmarks benchmarks/embedded_compare/raw/sample --engines=all"

task bench_embedded_run, "Run embedded benchmarks (engines: decentdb,sqlite,duckdb or all)":
  exec "nimble bench_embedded"
  exec "./build/run_benchmarks benchmarks/raw sample --engines=all"

task bench_embedded_aggregate, "Aggregate raw benchmark results":
  exec "python3 benchmarks/embedded_compare/scripts/aggregate_benchmarks.py"

task bench_embedded_chart, "Generate README benchmark chart":
  exec "benchmarks/embedded_compare/venv/bin/python3 benchmarks/embedded_compare/scripts/make_readme_chart.py"

task bench_embedded_pipeline, "Run full embedded benchmark pipeline (run + aggregate + chart)":
  exec "nimble bench_embedded_sample"
  exec "nimble bench_embedded_aggregate"
  exec "nimble bench_embedded_chart"
