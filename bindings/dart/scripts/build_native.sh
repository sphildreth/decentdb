#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

cd "$REPO_ROOT"

echo "Building DecentDB shared library..."
cargo build -p decentdb

echo
echo "Build complete. Library location:"
case "$(uname -s)" in
  Linux*)   echo "  $REPO_ROOT/target/debug/libdecentdb.so" ;;
  Darwin*)  echo "  $REPO_ROOT/target/debug/libdecentdb.dylib" ;;
  MINGW*|MSYS*|CYGWIN*)
            echo "  $REPO_ROOT/target/debug/decentdb.dll" ;;
  *)        echo "  $REPO_ROOT/target/debug/" ;;
esac
