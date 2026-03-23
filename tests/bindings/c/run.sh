#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../../.." && pwd)"
cc \
  -I"$root/include" \
  "$root/tests/bindings/c/smoke.c" \
  -L"$root/target/debug" \
  -Wl,-rpath,"$root/target/debug" \
  -ldecentdb \
  -o "$root/target/bindings-c-smoke"

"$root/target/bindings-c-smoke"
