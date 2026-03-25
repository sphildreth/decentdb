#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../../.." && pwd)"
node_prefix="$(node -p "process.config.variables.node_prefix")"

# Usually /usr/include/node or /opt/hostedtoolcache/.../include/node
if [ -d "$node_prefix/include/node" ] && [ -f "$node_prefix/include/node/node_api.h" ]; then
  node_include="$node_prefix/include/node"
elif [ -d "$node_prefix/include" ] && [ -f "$node_prefix/include/node_api.h" ]; then
  node_include="$node_prefix/include"
elif [ -n "${nodedir:-}" ] && [ -d "$nodedir/include/node" ]; then
  node_include="$nodedir/include/node"
else
  # Add node-api header as npm package if it doesn't exist
  # (sometimes github actions node doesn't include headers in prefix)
  echo "Fetching node-addon-api headers..."
  npm install -g node-api-headers || true
  node_headers_dir=$(npm root -g)/node-api-headers/include
  if [ -d "$node_headers_dir" ]; then
      node_include="$node_headers_dir"
  else
      # fallback / debug
      echo "Could not find node_api.h in $node_prefix"
      find "$node_prefix" -name "node_api.h" || true
      node_include="$node_prefix/include/node"
  fi
fi

echo "Using node_include=$node_include"

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
