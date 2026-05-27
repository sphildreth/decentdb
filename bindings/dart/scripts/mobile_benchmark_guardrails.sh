#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  mobile_benchmark_guardrails.sh [options]

Options:
  --android-root PATH   Android artifact root (default: target/mobile-artifacts/android)
  --ios-root PATH       iOS artifact root (default: target/mobile-artifacts/ios)
  --output PATH         JSON output path (default: target/mobile-artifacts/mobile-guardrails.json)
  -h, --help            Show this help message

This records artifact-size guardrails and leaves runtime latency fields null
unless a device/simulator benchmark harness writes them in a later step.
EOF
}

ANDROID_ROOT="$REPO_ROOT/target/mobile-artifacts/android"
IOS_ROOT="$REPO_ROOT/target/mobile-artifacts/ios"
OUTPUT="$REPO_ROOT/target/mobile-artifacts/mobile-guardrails.json"

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
    --output)
      OUTPUT="$2"
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

size_or_null() {
  local path="$1"
  if [[ -f "$path" ]]; then
    stat -c%s "$path" 2>/dev/null || stat -f%z "$path"
  elif [[ -d "$path" ]]; then
    du -sk "$path" | awk '{print $1 * 1024}'
  else
    echo null
  fi
}

mkdir -p "$(dirname "$OUTPUT")"
PROJECT_VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/VERSION")"

cat > "$OUTPUT" <<EOF
{
  "version": "$PROJECT_VERSION",
  "artifact_sizes_bytes": {
    "android_arm64_v8a": $(size_or_null "$ANDROID_ROOT/arm64-v8a/libdecentdb.so"),
    "android_x86_64": $(size_or_null "$ANDROID_ROOT/x86_64/libdecentdb.so"),
    "ios_arm64_static": $(size_or_null "$IOS_ROOT/libs/ios-arm64/libdecentdb.a"),
    "ios_simulator_x86_64_static": $(size_or_null "$IOS_ROOT/libs/ios-simulator-x86_64/libdecentdb.a"),
    "ios_xcframework": $(size_or_null "$IOS_ROOT/decentdb.xcframework")
  },
  "runtime_guardrails": {
    "cold_open_ms": null,
    "warm_open_ms": null,
    "first_query_ms": null,
    "prepared_point_lookup_loop_ms": null,
    "insert_transaction_batch_ms": null,
    "checkpoint_ms": null,
    "encrypted_open_overhead_ms": null,
    "sync_changeset_apply_ms": null,
    "large_result_paging_memory_delta_bytes": null
  }
}
EOF

echo "Wrote mobile guardrails to $OUTPUT"
