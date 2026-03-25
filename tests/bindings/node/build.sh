#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../../.." && pwd)"
node_include="$(node -p "process.config.variables.nodedir || require('path').join(process.config.variables.node_prefix, 'include/node')")"
if [[ "$node_include" != */include/node ]]; then
  node_include="$node_include/include/node"
fi

cc \
  -shared \
  -fPIC \
  -I"$node_include" \
  -I"$root/include" \
  "$root/tests/bindings/node/smoke.c" \
  -L"$root/target/debug" \
  -Wl,-rpath,"$root/target/debug" \
  -ldecentdb \
  -o "$root/tests/bindings/node/smoke.node"
