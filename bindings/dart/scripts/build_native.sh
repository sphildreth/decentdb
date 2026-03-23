#!/usr/bin/env bash
# Build DecentDB shared library for the current platform.
# Run from the repository root.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
cd "$REPO_ROOT"

echo "Building DecentDB shared library..."
nimble build_lib

echo ""
echo "Build complete. Library location:"
case "$(uname -s)" in
  Linux*)   echo "  $REPO_ROOT/build/libc_api.so" ;;
  Darwin*)  echo "  $REPO_ROOT/build/libc_api.dylib" ;;
  MINGW*|MSYS*|CYGWIN*)
            echo "  $REPO_ROOT/build/c_api.dll" ;;
  *)        echo "  $REPO_ROOT/build/" ;;
esac
