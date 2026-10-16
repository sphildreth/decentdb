#!/usr/bin/env bash
# Install the official Linux x64 CLI artifact used by browser CI.
set -euo pipefail

version="${1:?Usage: install_wasm_bindgen.sh VERSION}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
destination="$root/.tmp/tools/wasm-bindgen/$version"
archive="wasm-bindgen-$version-x86_64-unknown-linux-musl.tar.gz"
base="https://github.com/wasm-bindgen/wasm-bindgen/releases/download/$version"
mkdir -p "$destination"
curl --fail --silent --show-error --location --retry 3 "$base/$archive" -o "$destination/$archive"
curl --fail --silent --show-error --location --retry 3 "$base/$archive.sha256sum" -o "$destination/$archive.sha256sum"
(
  cd "$destination"
  read -r expected_sha256 checksum_name < "$archive.sha256sum"
  [[ "$expected_sha256" =~ ^[0-9a-fA-F]{64}$ ]]
  printf '%s  %s\n' "$expected_sha256" "$archive" | sha256sum --check --strict
  tar -xzf "$archive" --strip-components=1
)
"$destination/wasm-bindgen" --version
if [[ -n "${GITHUB_PATH:-}" ]]; then
  echo "$destination" >> "$GITHUB_PATH"
fi
