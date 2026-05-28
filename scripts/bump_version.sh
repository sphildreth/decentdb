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

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?(\+[a-zA-Z0-9.-]+)?$ ]]; then
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

replace_first_in_file() {
  local file="$1"
  local regex="$2"
  local replacement="$3"
  local tmp
  tmp="$(mktemp)"
  if ! awk -v regex="$regex" -v replacement="$replacement" '
    BEGIN { replaced = 0 }
    {
      line = $0
      if (!replaced && sub(regex, replacement, line)) {
        replaced = 1
      }
      print line
    }
    END { exit(replaced ? 0 : 2) }
  ' "$file" > "$tmp"; then
    rm -f "$tmp"
    echo "Error: failed to update $file" >&2
    exit 1
  fi
  mv "$tmp" "$file"
}

update_json_versions() {
  local file="$1"
  shift
  if [[ ! -f "$file" ]]; then
    return
  fi
  python3 - "$file" "$VERSION" "$@" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
version = sys.argv[2]
package_paths = sys.argv[3:]

document = json.loads(path.read_text(encoding="utf-8"))
document["version"] = version

packages = document.get("packages")
if isinstance(packages, dict):
    for package_path in package_paths:
        package = packages.get(package_path)
        if isinstance(package, dict):
            package["version"] = version

path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
PY
}

update_dart_path_lock_versions() {
  local file="$1"
  shift
  if [[ ! -f "$file" ]]; then
    return
  fi
  python3 - "$file" "$VERSION" "$@" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
version = sys.argv[2]
wanted = set(sys.argv[3:])

lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
current_package = None
source_is_path = False
updated = {package: False for package in wanted}

for index, line in enumerate(lines):
    package_match = re.match(r"^  ([A-Za-z0-9_]+):\s*$", line)
    if package_match:
        current_package = package_match.group(1)
        source_is_path = False
        continue

    if current_package in wanted and line.strip() == "source: path":
        source_is_path = True
        continue

    if (
        current_package in wanted
        and source_is_path
        and re.match(r'^    version: ".*"\s*$', line)
    ):
        newline = "\n" if line.endswith("\n") else ""
        lines[index] = f'    version: "{version}"{newline}'
        updated[current_package] = True

missing = [package for package, did_update in updated.items() if not did_update]
if missing:
    raise SystemExit(
        f"{path}: did not find path package version(s): {', '.join(missing)}"
    )

path.write_text("".join(lines), encoding="utf-8")
PY
}

update_cargo_lock_path_versions() {
  local file="$1"
  shift
  if [[ ! -f "$file" ]]; then
    return
  fi
  python3 - "$file" "$VERSION" "$@" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
version = sys.argv[2]
wanted = set(sys.argv[3:])

text = path.read_text(encoding="utf-8")
chunks = re.split(r"(?m)(?=^\[\[package\]\]$)", text)
updated = {package: False for package in wanted}

for index, chunk in enumerate(chunks):
    name_match = re.search(r'^name = "([^"]+)"$', chunk, flags=re.MULTILINE)
    if not name_match:
        continue
    name = name_match.group(1)
    if name not in wanted:
        continue
    if re.search(r"^source = ", chunk, flags=re.MULTILINE):
        continue
    chunks[index], count = re.subn(
        r'^version = "[^"]+"$',
        f'version = "{version}"',
        chunk,
        count=1,
        flags=re.MULTILINE,
    )
    if count:
        updated[name] = True

missing = [package for package, did_update in updated.items() if not did_update]
if missing:
    raise SystemExit(
        f"{path}: did not find path package version(s): {', '.join(missing)}"
    )

path.write_text("".join(chunks), encoding="utf-8")
PY
}

update_release_text_metadata() {
  python3 - "$VERSION" <<'PY'
import re
import sys
from pathlib import Path

version = sys.argv[1]

updates = {
    Path("docs/user-guide/benchmarks.md"): [
        (
            r"(\| DecentDB \| )[^|]+( \| Workspace package version \|)",
            rf"\g<1>{version}\g<2>",
        ),
    ],
    Path("design/FUTURE_WINS.md"): [
        (
            r"(public release in this repository is `)[^`]+(`)",
            rf"\g<1>{version}\g<2>",
        ),
        (
            r"(bucket after `)[^`]+(` only when scope is explicitly accepted)",
            rf"\g<1>{version}\g<2>",
        ),
    ],
}

for path, replacements in updates.items():
    if not path.exists():
        continue
    text = path.read_text(encoding="utf-8")
    for pattern, replacement in replacements:
        text, count = re.subn(pattern, replacement, text, count=1)
        if count != 1:
            raise SystemExit(f"{path}: expected exactly one match for {pattern!r}")
    path.write_text(text, encoding="utf-8")
PY
}

# --- Canonical version file ---
printf "%s\n" "$VERSION" > VERSION
update "VERSION" "canonical release version"

# --- Rust workspace ---
replace_first_in_file "Cargo.toml" '^version = ".*"$' "version = \"$VERSION\""
update "Cargo.toml" "workspace version"

# --- Python ---
replace_first_in_file "bindings/python/pyproject.toml" '^version = ".*"$' "version = \"$VERSION\""
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

node_lock="bindings/node/decentdb/package-lock.json"
if [[ -f "$node_lock" ]]; then
  update_json_versions "$node_lock" ""
  update "$node_lock" "lockfile package version"
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

knex_lock="bindings/node/knex-decentdb/package-lock.json"
if [[ -f "$knex_lock" ]]; then
  update_json_versions "$knex_lock" "" "../decentdb"
  update "$knex_lock" "lockfile package versions"
fi

# --- Java JDBC driver ---
java_gradle="bindings/java/driver/build.gradle"
if [[ -f "$java_gradle" ]]; then
  replace_first_in_file "$java_gradle" "^version = '.*'$" "version = '$VERSION'"
  update "$java_gradle" "gradle version"
fi

java_driver="bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBDriver.java"
if [[ -f "$java_driver" ]]; then
  replace_first_in_file "$java_driver" 'DRIVER_VERSION = "[^"]*"' "DRIVER_VERSION = \"$VERSION\""
  update "$java_driver" "DRIVER_VERSION constant"
fi

# --- Java DBeaver extension ---
dbeaver_gradle="bindings/java/dbeaver-extension/build.gradle"
if [[ -f "$dbeaver_gradle" ]]; then
  replace_first_in_file "$dbeaver_gradle" "^version = '.*'$" "version = '$VERSION'"
  update "$dbeaver_gradle" "gradle version"
fi

dbeaver_manifest="bindings/java/dbeaver-extension/META-INF/MANIFEST.MF"
if [[ -f "$dbeaver_manifest" ]]; then
  # Read the old version to fix the jar filename reference too
  old_jar_ver="$(awk -F': *' '/^Bundle-Version:/ { print $2; exit }' "$dbeaver_manifest")"
  replace_first_in_file "$dbeaver_manifest" '^Bundle-Version: .*$' "Bundle-Version: $VERSION"
  if [[ -n "$old_jar_ver" ]]; then
    replace_first_in_file "$dbeaver_manifest" 'decentdb-jdbc-[^[:space:]]*[.]jar' "decentdb-jdbc-${VERSION}.jar"
  fi
  update "$dbeaver_manifest" "Bundle-Version + jar reference"
fi

# --- Dart ---
dart_pubspec="bindings/dart/dart/pubspec.yaml"
if [[ -f "$dart_pubspec" ]]; then
  replace_first_in_file "$dart_pubspec" '^version: .*$' "version: $VERSION"
  update "$dart_pubspec" "pubspec version"
fi

flutter_pubspec="bindings/dart/flutter/pubspec.yaml"
if [[ -f "$flutter_pubspec" ]]; then
  replace_first_in_file "$flutter_pubspec" '^version: .*$' "version: $VERSION"
  update "$flutter_pubspec" "Flutter pubspec version"
fi

flutter_android_gradle="bindings/dart/flutter/android/build.gradle"
if [[ -f "$flutter_android_gradle" ]]; then
  replace_first_in_file "$flutter_android_gradle" "^version = '.*'$" "version = '$VERSION'"
  update "$flutter_android_gradle" "Flutter Android library version"
fi

flutter_ios_podspec="bindings/dart/flutter/ios/decentdb_flutter.podspec"
if [[ -f "$flutter_ios_podspec" ]]; then
  replace_first_in_file "$flutter_ios_podspec" "s[.]version[[:space:]]*=[[:space:]]*'.*'" "s.version          = '$VERSION'"
  update "$flutter_ios_podspec" "Flutter iOS podspec version"
fi

flutter_example_pubspec="bindings/dart/flutter/example/pubspec.yaml"
if [[ -f "$flutter_example_pubspec" ]]; then
  replace_first_in_file "$flutter_example_pubspec" '^version: .*$' "version: $VERSION"
  update "$flutter_example_pubspec" "Flutter reference app version"
fi

update_dart_path_lock_versions "bindings/dart/flutter/pubspec.lock" decentdb
update "bindings/dart/flutter/pubspec.lock" "local DecentDB path package version"

update_dart_path_lock_versions "bindings/dart/flutter/example/pubspec.lock" decentdb decentdb_flutter
update "bindings/dart/flutter/example/pubspec.lock" "local DecentDB path package versions"

update_dart_path_lock_versions "bindings/dart/examples/console/pubspec.lock" decentdb
update "bindings/dart/examples/console/pubspec.lock" "local DecentDB path package version"

update_dart_path_lock_versions "bindings/dart/examples/console_complex/pubspec.lock" decentdb
update "bindings/dart/examples/console_complex/pubspec.lock" "local DecentDB path package version"

update_dart_path_lock_versions "bindings/dart/examples/flutter_desktop/pubspec.lock" decentdb
update "bindings/dart/examples/flutter_desktop/pubspec.lock" "local DecentDB path package version"

update_dart_path_lock_versions "tests/bindings/dart/pubspec.lock" decentdb
update "tests/bindings/dart/pubspec.lock" "local DecentDB path package version"

# --- Secondary Rust lockfiles outside the workspace root ---
update_cargo_lock_path_versions "benchmarks/rust-baseline/Cargo.lock" decentdb libpg_query_sys
update "benchmarks/rust-baseline/Cargo.lock" "local workspace path package versions"

# --- Release-facing docs and roadmap metadata ---
update_release_text_metadata
update "docs/user-guide/benchmarks.md" "DecentDB benchmark version stamp"
update "design/FUTURE_WINS.md" "current public release marker"

# --- Regenerate Cargo.lock ---
echo ""
echo "Regenerating Cargo.lock..."
cargo check --quiet 2>/dev/null || cargo check
echo ""

# --- Summary ---
echo "Updated $updated files to version $VERSION"
echo ""
echo "Remaining steps:"
echo "  1. Update docs/about/changelog.md"
echo "  2. If Node dependencies changed, refresh lockfiles with npm install --package-lock-only --ignore-scripts"
echo "  3. Verify: grep -rn 'OLD_VERSION' . --exclude-dir={target,.git,node_modules,.dart_tool,vendor,site,bin,obj,build,.tmp}"
