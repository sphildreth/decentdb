#!/usr/bin/env bash
#
# run-and-display-benchmark-report.sh
#
# Runs a fresh DecentDB benchmark pass, renders the latest HTML dashboard,
# and opens it in your default browser.
#
# Usage:
#   ./scripts/run-and-display-benchmark-report.sh
#   ./scripts/run-and-display-benchmark-report.sh --no-open
#   ./scripts/run-and-display-benchmark-report.sh --profile dev
#   ./scripts/run-and-display-benchmark-report.sh --report-path build/bench/reports/custom.html
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

REPORT_PATH="build/bench/reports/today-dashboard.html"
OPEN_REPORT=true
PROFILE="nightly"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --report-path)
      REPORT_PATH="${2:-}"
      shift 2
      ;;
    --no-open)
      OPEN_REPORT=false
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./scripts/run-and-display-benchmark-report.sh [OPTIONS]

Options:
  --profile <name>     Benchmark profile to run: smoke, dev, or nightly (default: nightly)
  --report-path <path>  Output HTML report path (default: build/bench/reports/today-dashboard.html)
  --no-open             Do not auto-open the generated report
  -h, --help            Show this help
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Use --help for usage." >&2
      exit 1
      ;;
  esac
done

if [[ -z "$REPORT_PATH" ]]; then
  echo "Error: --report-path requires a non-empty path." >&2
  exit 1
fi

case "$PROFILE" in
  smoke|dev|nightly)
    ;;
  *)
    echo "Error: --profile must be one of: smoke, dev, nightly" >&2
    exit 1
    ;;
esac

echo "==> Running benchmark suite (release/$PROFILE/all)..."
cargo run -p decentdb-benchmark --release -- run --profile "$PROFILE" --all

echo ""
echo "==> Rendering latest run dashboard HTML..."
cargo run -p decentdb-benchmark -- report \
  --latest-run \
  --format html \
  --output "$REPORT_PATH"

echo ""
echo "Report generated:"
echo "  $REPORT_PATH"

if [[ "$OPEN_REPORT" == true ]]; then
  if command -v xdg-open >/dev/null 2>&1; then
    echo "==> Opening report with xdg-open..."
    xdg-open "$REPORT_PATH" >/dev/null 2>&1 || {
      echo "Could not auto-open report. Open manually:"
      echo "  $REPORT_PATH"
    }
  else
    echo "xdg-open not found. Open manually:"
    echo "  $REPORT_PATH"
  fi
fi
