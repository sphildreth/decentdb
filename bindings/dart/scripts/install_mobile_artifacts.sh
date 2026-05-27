#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  install_mobile_artifacts.sh [options]

Options:
  --android-root PATH   Android artifact root (default: target/mobile-artifacts/android)
  --ios-root PATH       iOS artifact root (default: target/mobile-artifacts/ios)
  --flutter-root PATH   Flutter package root (default: bindings/dart/flutter)
  -h, --help            Show this help message
EOF
}

ANDROID_ROOT="$REPO_ROOT/target/mobile-artifacts/android"
IOS_ROOT="$REPO_ROOT/target/mobile-artifacts/ios"
FLUTTER_ROOT="$REPO_ROOT/bindings/dart/flutter"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --android-root)
      ANDROID_ROOT="$2"
      shift 2
      ;;
    --ios-root)
      IOS_ROOT="$2"
      shift 2
      ;;
    --flutter-root)
      FLUTTER_ROOT="$2"
      shift 2
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

install_android() {
  for abi in arm64-v8a x86_64; do
    local src="$ANDROID_ROOT/$abi/libdecentdb.so"
    local dst_dir="$FLUTTER_ROOT/android/src/main/jniLibs/$abi"
    if [[ -f "$src" ]]; then
      mkdir -p "$dst_dir"
      cp "$src" "$dst_dir/libdecentdb.so"
      echo "Installed Android $abi artifact"
      android_count=$((android_count + 1))
    fi
  done
}

install_ios() {
  local src="$IOS_ROOT/decentdb.xcframework"
  local dst="$FLUTTER_ROOT/ios/Frameworks/decentdb.xcframework"
  if [[ -d "$src" ]]; then
    rm -rf "$dst"
    mkdir -p "$(dirname "$dst")"
    cp -R "$src" "$dst"
    echo "Installed iOS XCFramework"
    return 0
  fi
  return 1
}

android_count=0
install_android

ios_installed=0
if install_ios; then
  ios_installed=1
fi

if [[ "$android_count" -eq 0 && "$ios_installed" -eq 0 ]]; then
  echo "Error: no mobile artifacts were installed." >&2
  exit 1
fi

echo "Mobile artifacts installed into $FLUTTER_ROOT"
