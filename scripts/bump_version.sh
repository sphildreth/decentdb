#!/usr/bin/env bash
#
# bump_version.sh — propagate the version in VERSION to all bindings.
#
# Usage:
#   ./scripts/bump_version.sh          # reads VERSION file
#   ./scripts/bump_version.sh 2.1.0    # override version via argument
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

VERSION="${1:-$(cat VERSION)}"
VERSION="$(echo "$VERSION" | tr -d '[:space:]')"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
  echo "Error: '$VERSION' is not a valid semver string" >&2
  exit 1
fi

echo "Bumping all version strings to: $VERSION"
echo ""

updated=0
update() {
  local file="$1"
  local description="$2"
  if [[ ! -f "$file" ]]; then
    echo "  SKIP  $file (not found)"
    return
  fi
  echo "  OK    $file — $description"
  updated=$((updated + 1))
}

# --- Rust workspace ---
sed -i "0,/^version = \".*\"/s//version = \"$VERSION\"/" Cargo.toml
update "Cargo.toml" "workspace version"

# --- Python ---
sed -i "s/^version = \".*\"/version = \"$VERSION\"/" bindings/python/pyproject.toml
update "bindings/python/pyproject.toml" "project version"

# --- Node.js (decentdb) ---
node_pkg="bindings/node/decentdb/package.json"
if [[ -f "$node_pkg" ]]; then
  tmp=$(mktemp)
  # Update only the top-level "version" field (2nd line typically)
  awk -v ver="$VERSION" '
    !done && /"version":/ { sub(/"version": *"[^"]*"/, "\"version\": \"" ver "\""); done=1 }
    { print }
  ' "$node_pkg" > "$tmp" && mv "$tmp" "$node_pkg"
  update "$node_pkg" "package version"
fi

# --- Node.js (knex-decentdb) ---
knex_pkg="bindings/node/knex-decentdb/package.json"
if [[ -f "$knex_pkg" ]]; then
  tmp=$(mktemp)
  awk -v ver="$VERSION" '
    !done && /"version":/ { sub(/"version": *"[^"]*"/, "\"version\": \"" ver "\""); done=1 }
    { print }
  ' "$knex_pkg" > "$tmp" && mv "$tmp" "$knex_pkg"
  update "$knex_pkg" "package version"
fi

# --- Java JDBC driver ---
java_gradle="bindings/java/driver/build.gradle"
if [[ -f "$java_gradle" ]]; then
  sed -i "s/^version = '.*'/version = '$VERSION'/" "$java_gradle"
  update "$java_gradle" "gradle version"
fi

java_driver="bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBDriver.java"
if [[ -f "$java_driver" ]]; then
  sed -i "s/DRIVER_VERSION = \".*\"/DRIVER_VERSION = \"$VERSION\"/" "$java_driver"
  update "$java_driver" "DRIVER_VERSION constant"
fi

# --- Java DBeaver extension ---
dbeaver_gradle="bindings/java/dbeaver-extension/build.gradle"
if [[ -f "$dbeaver_gradle" ]]; then
  sed -i "s/^version = '.*'/version = '$VERSION'/" "$dbeaver_gradle"
  update "$dbeaver_gradle" "gradle version"
fi

dbeaver_manifest="bindings/java/dbeaver-extension/META-INF/MANIFEST.MF"
if [[ -f "$dbeaver_manifest" ]]; then
  # Read the old version to fix the jar filename reference too
  old_jar_ver=$(grep 'Bundle-Version:' "$dbeaver_manifest" | sed 's/Bundle-Version: *//')
  sed -i "s/Bundle-Version: .*/Bundle-Version: $VERSION/" "$dbeaver_manifest"
  if [[ -n "$old_jar_ver" ]]; then
    sed -i "s/decentdb-jdbc-${old_jar_ver}.jar/decentdb-jdbc-${VERSION}.jar/" "$dbeaver_manifest"
  fi
  update "$dbeaver_manifest" "Bundle-Version + jar reference"
fi

# --- Dart ---
dart_pubspec="bindings/dart/dart/pubspec.yaml"
if [[ -f "$dart_pubspec" ]]; then
  sed -i "s/^version: .*/version: $VERSION/" "$dart_pubspec"
  update "$dart_pubspec" "pubspec version"
fi

# --- Regenerate Cargo.lock ---
echo ""
echo "Regenerating Cargo.lock..."
cargo check --quiet 2>/dev/null || cargo check
echo ""

# --- Summary ---
echo "Updated $updated files to version $VERSION"
echo ""
echo "Remaining steps:"
echo "  1. cd bindings/node/decentdb && npm install  (updates package-lock.json)"
echo "  2. cd bindings/node/knex-decentdb && npm install  (updates package-lock.json)"
echo "  3. Update CHANGELOG.md and docs/about/changelog.md"
echo "  4. Verify: grep -rn 'OLD_VERSION' . --exclude-dir={target,.git,node_modules,.dart_tool,vendor,site,bin,obj,build,.tmp}"
