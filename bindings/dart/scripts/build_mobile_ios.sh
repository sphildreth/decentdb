#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  build_mobile_ios.sh [options]

Options:
  --artifact-root PATH      Output directory for iOS artifacts
  --profile release|debug   Rust profile (default: release)
  --strict                  Fail the script if a required target is missing
  -h, --help                Show this help message

Environment:
  MOBILE_ARTIFACT_ROOT      Overrides --artifact-root
EOF
}

ARTIFACT_ROOT="${MOBILE_ARTIFACT_ROOT:-$REPO_ROOT/target/mobile-artifacts/ios}"
PROFILE="release"
STRICT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifact-root)
      ARTIFACT_ROOT="$2"
      shift 2
      ;;
    --profile)
      PROFILE="$2"
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

if [[ "$PROFILE" != "release" && "$PROFILE" != "debug" ]]; then
  echo "Error: --profile must be release or debug" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "Error: cargo is required for mobile artifact builds" >&2
  exit 1
fi

if ! command -v rustup >/dev/null 2>&1; then
  echo "Error: rustup is required for target checks" >&2
  exit 1
fi

if ! command -v xcodebuild >/dev/null 2>&1; then
  echo "Warning: xcodebuild not found; skipping XCFramework packaging step."
fi

PROJECT_VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/VERSION")"
mkdir -p "$ARTIFACT_ROOT/libs" "$ARTIFACT_ROOT/"

if [[ "$PROFILE" == "release" ]]; then
  CARGO_PROFILE=(--release)
else
  CARGO_PROFILE=()
fi

has_rust_target() {
  local target="$1"
  rustup target list --installed | grep -qx "$target"
}

build_static_target() {
  local abi="$1"
  local target="$2"
  local out_lib="$REPO_ROOT/target/$target/$PROFILE/libdecentdb.a"
  local dst_dir="$ARTIFACT_ROOT/libs/$abi"

  echo "Building iOS target: $target ($abi)"

  if ! has_rust_target "$target"; then
    echo "::warning::Rust target $target is not installed; skipping $abi."
    return 2
  fi

  if ! cargo rustc -p decentdb --lib --crate-type staticlib --target "$target" "${CARGO_PROFILE[@]}"; then
    echo "::warning::cargo rustc failed for $target, skipping $abi."
    return 1
  fi

  if [[ ! -f "$out_lib" ]]; then
    echo "::warning::Build succeeded but $out_lib was not found for $abi."
    return 1
  fi

  mkdir -p "$dst_dir"
  cp "$out_lib" "$dst_dir/libdecentdb.a"
  echo "Prepared $dst_dir/libdecentdb.a"
  return 0
}

build_xcframework() {
  local device_lib="$ARTIFACT_ROOT/libs/ios-arm64/libdecentdb.a"
  local sim_lib="$ARTIFACT_ROOT/libs/ios-simulator-x86_64/libdecentdb.a"
  local output="$ARTIFACT_ROOT/decentdb.xcframework"

  if ! command -v xcodebuild >/dev/null 2>&1; then
    echo "::warning::xcodebuild missing; cannot create XCFramework."
    return 1
  fi

  if [[ ! -d "$REPO_ROOT/bindings/dart/native" ]]; then
    echo "::warning::iOS XCFramework header directory $REPO_ROOT/bindings/dart/native is missing."
    return 1
  fi

  if [[ ! -f "$device_lib" || ! -f "$sim_lib" ]]; then
    echo "::warning::iOS XCFramework requires both device and simulator archives."
    return 1
  fi

  rm -rf "$output"
  if ! xcodebuild -create-xcframework \
    -library "$device_lib" -headers "$REPO_ROOT/bindings/dart/native" \
    -library "$sim_lib" -headers "$REPO_ROOT/bindings/dart/native" \
    -output "$output"; then
    echo "::warning::xcodebuild failed to generate XCFramework."
    return 1
  fi

  echo "Generated $output"
  return 0
}

declare -a TARGET_SPECS=(
  "ios-arm64:aarch64-apple-ios"
  "ios-simulator-x86_64:x86_64-apple-ios"
)

total_count=${#TARGET_SPECS[@]}
built_count=0

for spec in "${TARGET_SPECS[@]}"; do
  abi="${spec%%:*}"
  target="${spec#*:}"
  if build_static_target "$abi" "$target"; then
    built_count=$((built_count + 1))
  fi
done

if build_xcframework; then
  xcframework_built=1
else
  xcframework_built=0
fi

cat > "$ARTIFACT_ROOT/version.txt" <<EOF
version=$PROJECT_VERSION
platform=ios
profile=$PROFILE
EOF

if command -v sha256sum >/dev/null 2>&1; then
  : > "$ARTIFACT_ROOT/checksums.sha256"
  find "$ARTIFACT_ROOT" -name 'libdecentdb.a' -print0 | while IFS= read -r -d '' lib; do
    sha256sum "$lib" >> "$ARTIFACT_ROOT/checksums.sha256"
  done

  if [[ -d "$ARTIFACT_ROOT/decentdb.xcframework" ]]; then
    find "$ARTIFACT_ROOT/decentdb.xcframework" -name 'Headers' -prune -o -type f -print0 | while IFS= read -r -d '' path; do
      sha256sum "$path" >> "$ARTIFACT_ROOT/checksums.sha256"
    done
  fi
fi

if [[ "$STRICT" == "1" && ("$built_count" -ne "$total_count" || "$xcframework_built" -ne 1) ]]; then
  echo "Error: required iOS targets and XCFramework were not fully produced. strict mode enabled." >&2
  exit 1
fi

if [[ "$built_count" -eq 0 ]]; then
  echo "Warning: no iOS mobile targets were produced."
else
  echo "iOS build completed: $built_count/$total_count target(s) produced."
  if [[ "$xcframework_built" -eq 1 ]]; then
    echo "iOS XCFramework generated at $ARTIFACT_ROOT/decentdb.xcframework"
  fi
fi

exit 0
