#!/usr/bin/env bash
# Run Dart tests for the DecentDB Dart binding.
# Assumes the native library has been built (nimble build_lib).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DART_DIR="$SCRIPT_DIR/../dart"

# Find native library
if [ -z "${DECENTDB_NATIVE_LIB:-}" ]; then
  if [ -f "$REPO_ROOT/build/libc_api.so" ]; then
    export DECENTDB_NATIVE_LIB="$REPO_ROOT/build/libc_api.so"
  elif [ -f "$REPO_ROOT/build/libc_api.dylib" ]; then
    export DECENTDB_NATIVE_LIB="$REPO_ROOT/build/libc_api.dylib"
  elif [ -f "$REPO_ROOT/build/c_api.dll" ]; then
    export DECENTDB_NATIVE_LIB="$REPO_ROOT/build/c_api.dll"
  else
    echo "ERROR: Native library not found. Run: nimble build_lib"
    exit 1
  fi
fi

echo "Using native library: $DECENTDB_NATIVE_LIB"

cd "$DART_DIR"

# Get dependencies
echo "Getting Dart dependencies..."
dart pub get

# Run tests
echo "Running Dart tests..."
dart test --reporter expanded

echo ""
echo "All tests passed."
