#!/bin/bash
set -e

# Ensure we are in the repo root
cd "$(dirname "$0")/.."

echo "Building DecentDb engine..."
nim c -d:release --hints:off src/decentdb.nim

echo "Running crash-injection tests..."
python3 tests/harness/crash_runner.py --engine ./src/decentdb
