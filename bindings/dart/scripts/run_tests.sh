#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DART_DIR="$SCRIPT_DIR/../dart"

cd "$REPO_ROOT"
cargo build -p decentdb >/dev/null

if [ -z "${DECENTDB_NATIVE_LIB:-}" ]; then
  if [ -f "$REPO_ROOT/target/debug/libdecentdb.so" ]; then
    export DECENTDB_NATIVE_LIB="$REPO_ROOT/target/debug/libdecentdb.so"
  elif [ -f "$REPO_ROOT/target/debug/libdecentdb.dylib" ]; then
    export DECENTDB_NATIVE_LIB="$REPO_ROOT/target/debug/libdecentdb.dylib"
  elif [ -f "$REPO_ROOT/target/debug/decentdb.dll" ]; then
    export DECENTDB_NATIVE_LIB="$REPO_ROOT/target/debug/decentdb.dll"
  else
    echo "ERROR: Native library not found after cargo build -p decentdb"
    exit 1
  fi
fi

echo "Using native library: $DECENTDB_NATIVE_LIB"
cd "$DART_DIR"
dart pub get
dart test --reporter expanded
