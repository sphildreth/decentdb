#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  build_mobile_android.sh [options]

Options:
  --artifact-root PATH      Output directory for Android artifacts
  --api-level N             Android API level used for linkers (default: 26)
  --profile release|debug   Rust profile (default: release)
  --strict                  Fail the script if a required target is missing
  -h, --help                Show this help message

Environment:
  MOBILE_ARTIFACT_ROOT      Overrides --artifact-root
  ANDROID_API_LEVEL         Defaults used unless --api-level is passed
  ANDROID_NDK_HOME          Optional Android NDK root for linker detection
EOF
}

ARTIFACT_ROOT="${MOBILE_ARTIFACT_ROOT:-$REPO_ROOT/target/mobile-artifacts/android}"
API_LEVEL="${ANDROID_API_LEVEL:-26}"
PROFILE="release"
STRICT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifact-root)
      ARTIFACT_ROOT="$2"
      shift 2
      ;;
    --api-level)
      API_LEVEL="$2"
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

PROJECT_VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/VERSION")"
mkdir -p "$ARTIFACT_ROOT"

if [[ "$PROFILE" == "release" ]]; then
  CARGO_PROFILE=(--release)
else
  CARGO_PROFILE=()
fi

has_rust_target() {
  local target="$1"
  rustup target list --installed | grep -qx "$target"
}

find_android_linker() {
  local target="$1"
  local target_suffix
  local env_var

  target_suffix="${target^^}"
  target_suffix="${target_suffix//-/_}"
  env_var="CARGO_TARGET_${target_suffix}_LINKER"
  if [[ -n "${!env_var:-}" ]]; then
    echo "${!env_var}"
    return 0
  fi

  local ndk_home="${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-${NDK_HOME:-${ANDROID_NDK_LATEST_HOME:-}}}}"
  if [[ -z "$ndk_home" || ! -d "$ndk_home" ]]; then
    local sdk_root="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
    if [[ -n "$sdk_root" && -d "$sdk_root/ndk" ]]; then
      ndk_home="$(find "$sdk_root/ndk" -mindepth 1 -maxdepth 1 -type d | sort -V | tail -n 1)"
    fi
  fi
  if [[ -z "$ndk_home" || ! -d "$ndk_home" ]]; then
    return 1
  fi

  local host_tag="linux-x86_64"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    host_tag="darwin-$(uname -m)"
  fi

  local linker="$ndk_home/toolchains/llvm/prebuilt/$host_tag/bin/${target}${API_LEVEL}-clang"
  if [[ -x "$linker" ]]; then
    echo "$linker"
    return 0
  fi

  return 1
}

find_android_tool() {
  local tool="$1"
  local ndk_home="${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-${NDK_HOME:-${ANDROID_NDK_LATEST_HOME:-}}}}"
  if [[ -z "$ndk_home" || ! -d "$ndk_home" ]]; then
    local sdk_root="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
    if [[ -n "$sdk_root" && -d "$sdk_root/ndk" ]]; then
      ndk_home="$(find "$sdk_root/ndk" -mindepth 1 -maxdepth 1 -type d | sort -V | tail -n 1)"
    fi
  fi
  if [[ -z "$ndk_home" || ! -d "$ndk_home" ]]; then
    return 1
  fi

  local host_tag="linux-x86_64"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    host_tag="darwin-$(uname -m)"
  fi

  local path="$ndk_home/toolchains/llvm/prebuilt/$host_tag/bin/$tool"
  if [[ -x "$path" ]]; then
    echo "$path"
    return 0
  fi

  return 1
}

build_target() {
  local abi="$1"
  local target="$2"

  echo "Building Android target: $target ($abi)"

  if ! has_rust_target "$target"; then
    echo "::warning::Rust target $target is not installed; skipping $abi."
    return 2
  fi

  local target_suffix
  local cc_suffix
  local env_var
  local linker
  local ar
  local out_lib
  local dst_dir
  local env_args=()

  out_lib="$REPO_ROOT/target/$target/$PROFILE/libdecentdb.so"
  dst_dir="$ARTIFACT_ROOT/$abi"

  target_suffix="${target^^}"
  target_suffix="${target_suffix//-/_}"
  cc_suffix="${target//-/_}"
  env_var="CARGO_TARGET_${target_suffix}_LINKER"

  linker="${!env_var:-$(find_android_linker "$target" || true)}"
  if [[ -n "$linker" ]]; then
    env_args=(
      "${env_var}=${linker}"
      "CC_${cc_suffix}=${linker}"
    )
    ar="$(find_android_tool llvm-ar || true)"
    if [[ -n "$ar" ]]; then
      env_args+=("AR_${cc_suffix}=${ar}")
    fi
  else
    echo "::warning::No Android linker detected for $target. Build may fail if toolchain is unavailable."
    echo "Hint: export CARGO_TARGET_${target_suffix}_LINKER or set ANDROID_NDK_HOME/NDK_HOME."
  fi

  if ! env "${env_args[@]}" cargo build -p decentdb --target "$target" "${CARGO_PROFILE[@]}"; then
    echo "::warning::cargo build failed for $target, skipping $abi."
    return 1
  fi

  if [[ ! -f "$out_lib" ]]; then
    echo "::warning::Build succeeded but $out_lib was not found for $abi."
    return 1
  fi

  mkdir -p "$dst_dir"
  cp "$out_lib" "$dst_dir/libdecentdb.so"
  echo "Prepared $dst_dir/libdecentdb.so"
  return 0
}

declare -a ABIS=(arm64-v8a x86_64)
declare -A ABI_TO_TARGET=(
  [arm64-v8a]=aarch64-linux-android
  [x86_64]=x86_64-linux-android
)

total_count=${#ABIS[@]}
built_count=0

for abi in "${ABIS[@]}"; do
  target="${ABI_TO_TARGET[$abi]}"
  if build_target "$abi" "$target"; then
    built_count=$((built_count + 1))
  fi
done

cat > "$ARTIFACT_ROOT/version.txt" <<EOF
version=$PROJECT_VERSION
platform=android
profile=$PROFILE
api_level=$API_LEVEL
EOF

if command -v sha256sum >/dev/null 2>&1; then
  : > "$ARTIFACT_ROOT/checksums.sha256"
  find "$ARTIFACT_ROOT" -name 'libdecentdb.so' -print0 | while IFS= read -r -d '' lib; do
    sha256sum "$lib" >> "$ARTIFACT_ROOT/checksums.sha256"
  done
fi

if [[ "$STRICT" == "1" && "$built_count" -ne "$total_count" ]]; then
  echo "Error: required Android targets were not fully built. strict mode enabled." >&2
  exit 1
fi

if [[ "$built_count" -eq 0 ]]; then
  echo "Warning: no Android mobile targets were produced."
else
  echo "Android build completed: $built_count/$total_count target(s) produced."
fi

exit 0
