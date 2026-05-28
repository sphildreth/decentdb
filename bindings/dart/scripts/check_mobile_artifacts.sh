#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  check_mobile_artifacts.sh --platform {android|ios} [options]

Options:
  --artifact-root PATH      Artifact directory to validate
  --platform PLATFORM       "android" or "ios"
  --expected-version VALUE  Expected artifact version (defaults to repository VERSION)
  --strict                  Fail on any missing artifact
  -h, --help               Show this help message
EOF
}

PLATFORM=""
ARTIFACT_ROOT="${MOBILE_ARTIFACT_ROOT:-}"
EXPECTED_VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/VERSION")"
STRICT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifact-root)
      ARTIFACT_ROOT="$2"
      shift 2
      ;;
    --platform)
      PLATFORM="$2"
      shift 2
      ;;
    --expected-version)
      EXPECTED_VERSION="$2"
      shift 2
      ;;
    --strict)
      STRICT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$PLATFORM" ]]; then
  echo "Error: --platform is required." >&2
  usage
  exit 1
fi

if [[ "$PLATFORM" != "android" && "$PLATFORM" != "ios" ]]; then
  echo "Error: --platform must be 'android' or 'ios'." >&2
  exit 1
fi

if [[ -z "$ARTIFACT_ROOT" ]]; then
  echo "Error: --artifact-root is required unless MOBILE_ARTIFACT_ROOT is set." >&2
  exit 1
fi

fail=0
expect_file() {
  local label="$1"
  local path="$2"
  if [[ ! -f "$path" && ! -d "$path" ]]; then
    echo "::warning::Missing expected ${label}: $path"
    fail=$((fail + 1))
  fi
}

if [[ ! -f "$ARTIFACT_ROOT/version.txt" ]]; then
  echo "Error: version.txt missing in $ARTIFACT_ROOT" >&2
  exit 1
fi

version_in_file="$(awk -F= '/^version=/{print $2}' "$ARTIFACT_ROOT/version.txt")"
if [[ -z "$version_in_file" ]]; then
  echo "Error: version field missing from $ARTIFACT_ROOT/version.txt" >&2
  exit 1
fi

if [[ "$version_in_file" != "$EXPECTED_VERSION" ]]; then
  echo "::warning::Version mismatch. expected=$EXPECTED_VERSION actual=$version_in_file"
  if [[ "$STRICT" == "1" ]]; then
    fail=$((fail + 1))
  fi
fi

if [[ "$PLATFORM" == "android" ]]; then
  expect_file "Android arm64-v8a library" "$ARTIFACT_ROOT/arm64-v8a/libdecentdb.so"
  expect_file "Android x86_64 library" "$ARTIFACT_ROOT/x86_64/libdecentdb.so"
fi

if [[ "$PLATFORM" == "ios" ]]; then
  expect_file "iOS arm64 static library" "$ARTIFACT_ROOT/libs/ios-arm64/libdecentdb.a"
  expect_file "iOS simulator x86_64 static library" "$ARTIFACT_ROOT/libs/ios-simulator-x86_64/libdecentdb.a"
  expect_file "iOS XCFramework bundle" "$ARTIFACT_ROOT/decentdb.xcframework"
fi

if ! command -v sha256sum >/dev/null 2>&1; then
  echo "::warning::sha256sum is not available; skipping checksum validation."
elif [[ ! -f "$ARTIFACT_ROOT/checksums.sha256" ]]; then
  echo "::warning::checksums.sha256 is missing from $ARTIFACT_ROOT"
  if [[ "$STRICT" == "1" ]]; then
    fail=$((fail + 1))
  fi
fi

if [[ "$fail" -gt 0 ]]; then
  if [[ "$STRICT" == "1" ]]; then
    echo "Error: artifact smoke check failed for $PLATFORM." >&2
    exit 1
  fi
  echo "Warning: artifact smoke check found ${fail} issue(s) for $PLATFORM."
else
  echo "Artifact smoke check passed for $PLATFORM."
fi

exit 0
