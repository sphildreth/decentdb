#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

COMPOSE_FILE="benchmarks/embedded_compare/docker-compose.yml"

OUT_DIR="$ROOT_DIR/benchmarks/embedded_compare/out"
mkdir -p "$OUT_DIR"

# Avoid false "success" by clearing previous outputs.
rm -f \
	"$OUT_DIR/results_py.json" \
	"$OUT_DIR/results_litedb.json" \
	"$OUT_DIR/results_merged.json" \
	"$OUT_DIR/chart.png" \
	"$OUT_DIR/chart_all.png"

pick_compose() {
	# Allow override.
	if [[ -n "${COMPOSE_CMD:-}" ]]; then
		echo "$COMPOSE_CMD"
		return 0
	fi

	# Prefer Podman if present.
	if command -v podman >/dev/null 2>&1; then
		if podman compose version >/dev/null 2>&1; then
			echo "podman compose"
			return 0
		fi
		if command -v podman-compose >/dev/null 2>&1; then
			echo "podman-compose"
			return 0
		fi
		echo "ERROR: podman is installed but neither 'podman compose' nor 'podman-compose' is available." >&2
		echo "Install podman-compose or a Podman version that includes 'podman compose'." >&2
		exit 2
	fi

	# Fall back to Docker.
	if command -v docker >/dev/null 2>&1; then
		echo "docker compose"
		return 0
	fi

	echo "ERROR: neither podman nor docker found in PATH." >&2
	exit 2
}

COMPOSE="$(pick_compose)"
echo "Using compose: $COMPOSE"

# Split compose command into an argv array (handles "podman compose").
IFS=' ' read -r -a COMPOSE_ARR <<<"$COMPOSE"

IS_PODMAN_COMPOSE=0
if [[ "${COMPOSE_ARR[0]}" == "podman" || "${COMPOSE_ARR[0]}" == "podman-compose" ]]; then
	IS_PODMAN_COMPOSE=1
fi

# Podman v5 often wires `podman compose` to the external `podman-compose` provider.
# That provider always attaches a project bridge network, which conflicts with
# forcing `--network=host` (it results in two --network args).
# When we detect the external provider and we want host networking, bypass compose
# and use `podman build/run` directly.
PODMAN_COMPOSE_EXTERNAL=0
if [[ "$IS_PODMAN_COMPOSE" == "1" ]]; then
	# If we're calling podman-compose directly, treat it as the external provider case.
	if [[ "${COMPOSE_ARR[0]}" == "podman-compose" ]]; then
		PODMAN_COMPOSE_EXTERNAL=1
	else
		if podman compose version 2>&1 | grep -q "external compose provider"; then
			PODMAN_COMPOSE_EXTERNAL=1
		fi
	fi
fi

if [[ "${BENCH_DEBUG_ONLY:-}" == "1" ]]; then
	echo "IS_PODMAN_COMPOSE=$IS_PODMAN_COMPOSE PODMAN_COMPOSE_EXTERNAL=$PODMAN_COMPOSE_EXTERNAL PODMAN_NO_HOST_NET=${PODMAN_NO_HOST_NET:-}"
	exit 0
fi

COMPOSE_FLAGS=()

on_fail() {
	code=$?
	if [[ $code -ne 0 ]]; then
		echo "" >&2
		echo "Benchmark run failed (exit=$code). Dumping compose ps/logs (best effort):" >&2
		"${COMPOSE_ARR[@]}" "${COMPOSE_FLAGS[@]}" -f "$COMPOSE_FILE" ps || true
		# podman-compose doesn't support --no-color
		if [[ "$COMPOSE" == podman* || "$COMPOSE" == podman-compose* ]]; then
			"${COMPOSE_ARR[@]}" "${COMPOSE_FLAGS[@]}" -f "$COMPOSE_FILE" logs --tail=200 || true
		else
			"${COMPOSE_ARR[@]}" "${COMPOSE_FLAGS[@]}" -f "$COMPOSE_FILE" logs --no-color --tail=200 || true
		fi
		echo "" >&2
		echo "Common Podman fixes:" >&2
		echo "- If you see permission denied on /out or /db, SELinux volume relabeling is enabled via ':Z' in docker-compose.yml." >&2
		echo "- If you see overlayfs graphdriver errors, install 'fuse-overlayfs' (recommended) or ensure CONTAINERS_STORAGE_CONF points to a vfs config." >&2
	fi

	if [[ -n "${BENCH_STORAGE_DIR:-}" && -d "${BENCH_STORAGE_DIR:-}" ]]; then
		if command -v podman >/dev/null 2>&1; then
			podman unshare rm -rf "$BENCH_STORAGE_DIR" 2>/dev/null || rm -rf "$BENCH_STORAGE_DIR" 2>/dev/null || true
		else
			rm -rf "$BENCH_STORAGE_DIR" 2>/dev/null || true
		fi
	fi

	exit $code
}

trap on_fail EXIT

if [[ "$COMPOSE" == podman* || "$COMPOSE" == podman-compose* ]]; then
	# Rootless podman commonly fails with:
	#   "kernel does not support overlay fs ... backing file system is unsupported"
	# when the graphroot is on ecryptfs/extfs/etc.
	# For this benchmark, DB I/O happens on a bind mount (./db), so using vfs/fuse
	# storage for image layers is acceptable and unblocks running.
	if [[ -z "${CONTAINERS_STORAGE_CONF:-}" ]]; then
		BENCH_STORAGE_DIR="${BENCH_STORAGE_DIR:-$(mktemp -d -t decentdb_bench_storage_XXXXXX)}"
		mkdir -p "$BENCH_STORAGE_DIR"

		STORAGE_CONF="$BENCH_STORAGE_DIR/storage.conf"
		GRAPHROOT="$BENCH_STORAGE_DIR/graphroot"
		RUNROOT="$BENCH_STORAGE_DIR/runroot"
		mkdir -p "$GRAPHROOT" "$RUNROOT"

		if command -v fuse-overlayfs >/dev/null 2>&1; then
			cat >"$STORAGE_CONF" <<EOF
[storage]
driver = "overlay"
graphroot = "$GRAPHROOT"
runroot = "$RUNROOT"

[storage.options]
mount_program = "$(command -v fuse-overlayfs)"
EOF
			echo "Using Podman storage: overlay + fuse-overlayfs (graphroot=$GRAPHROOT)"
		else
			cat >"$STORAGE_CONF" <<EOF
[storage]
driver = "vfs"
graphroot = "$GRAPHROOT"
runroot = "$RUNROOT"
EOF
			echo "Using Podman storage: vfs (graphroot=$GRAPHROOT)"
			echo "Tip: install fuse-overlayfs for faster builds." >&2
		fi

		export CONTAINERS_STORAGE_CONF="$STORAGE_CONF"
	else
		echo "Using CONTAINERS_STORAGE_CONF=$CONTAINERS_STORAGE_CONF"
	fi

	# Podman rootless networking may require /dev/net/tun (pasta/slirp). On systems
	# without working TUN, builds frequently fail (apt-get/nuget can't reach network).
	# Default to host networking for reliability; allow opting out.
	# NOTE: podman-compose's arg parser treats values that start with "--" as new options
	# unless they are passed via the --opt=value form.
	if [[ "${PODMAN_NO_HOST_NET:-}" != "1" ]]; then
		COMPOSE_FLAGS+=(--podman-build-args=--network=host --podman-run-args=--network=host)
		# podman-compose defaults to running services in a pod, which adds its own network.
		# host networking + pod networking triggers: "cannot set multiple networks ... mode host".
		COMPOSE_FLAGS+=(--in-pod=false)
		echo "Podman: forcing host networking for build/run (avoids /dev/net/tun/pasta issues)"
	else
		echo "Podman: host networking disabled (PODMAN_NO_HOST_NET=1); using default networking"
	fi
fi

run_with_podman_direct() {
	local image_py="embedded_compare_bench_py"
	local root="$ROOT_DIR"
	local out="$OUT_DIR"
	local db="$ROOT_DIR/benchmarks/embedded_compare/db"

	echo "Podman: using direct podman build/run (bypassing compose)"

	# Build image (host networking avoids pasta/tun issues during apt/pip)
	podman build --network=host \
		-f "$ROOT_DIR/benchmarks/embedded_compare/Dockerfile.bench" \
		-t "$image_py" \
		"$root"

	# bench_py
	podman run --rm --network=host \
		-v "$out":/out:Z \
		-v "$db":/db:Z \
		-e BENCH_DB_DIR=/db \
		-e BENCH_OUT_DIR=/out \
		"$image_py" \
		python3 -u benchmarks/embedded_compare/run.py \
			--db-dir /db \
			--out /out/results_py.json \
			--plot /out/chart.png \
			--op-counts 10000,100000,1000000 \
			--iterations 7 \
			--warmup 2

	# bench_litedb
	podman run --rm --network=host \
		-w /repo \
		-v "$root":/repo:Z \
		-v "$out":/out:Z \
		-v "$db":/db:Z \
		mcr.microsoft.com/dotnet/sdk:8.0 \
		dotnet run -c Release --project benchmarks/embedded_compare/dotnet/LiteDbBench/LiteDbBench.csproj -- \
			--db-dir /db \
			--out /out/results_litedb.json \
			--op-counts 10000,100000,1000000 \
			--iterations 7 \
			--warmup 2

	# plot_all
	podman run --rm --network=host \
		-v "$out":/out:Z \
		"$image_py" \
		python3 -u benchmarks/embedded_compare/plot.py \
			--in /out/results_py.json \
			--in /out/results_litedb.json \
			--out /out/chart_all.png \
			--merged-json /out/results_merged.json
}

# Run benches first (can be re-run independently)
if [[ "$IS_PODMAN_COMPOSE" == "1" && "${PODMAN_NO_HOST_NET:-}" != "1" && "${PODMAN_FORCE_COMPOSE:-}" != "1" ]]; then
	run_with_podman_direct
else
	"${COMPOSE_ARR[@]}" "${COMPOSE_FLAGS[@]}" -f "$COMPOSE_FILE" up --build --abort-on-container-exit bench_py

	if [[ ! -s "$OUT_DIR/results_py.json" ]]; then
		echo "ERROR: bench_py did not produce $OUT_DIR/results_py.json" >&2
		exit 3
	fi

	"${COMPOSE_ARR[@]}" "${COMPOSE_FLAGS[@]}" -f "$COMPOSE_FILE" up --build --abort-on-container-exit bench_litedb

	if [[ ! -s "$OUT_DIR/results_litedb.json" ]]; then
		echo "ERROR: bench_litedb did not produce $OUT_DIR/results_litedb.json" >&2
		exit 3
	fi

	# Merge + plot
	"${COMPOSE_ARR[@]}" "${COMPOSE_FLAGS[@]}" -f "$COMPOSE_FILE" up --build --abort-on-container-exit plot_all

	if [[ ! -s "$OUT_DIR/results_merged.json" || ! -s "$OUT_DIR/chart_all.png" ]]; then
		echo "ERROR: plot_all did not produce merged outputs in $OUT_DIR" >&2
		exit 3
	fi

	# Cleanup containers
	set +e
	"${COMPOSE_ARR[@]}" "${COMPOSE_FLAGS[@]}" -f "$COMPOSE_FILE" down -v --remove-orphans
	set -e
fi

echo "Wrote: benchmarks/embedded_compare/out/results_py.json"
echo "Wrote: benchmarks/embedded_compare/out/results_litedb.json"
echo "Wrote: benchmarks/embedded_compare/out/results_merged.json"
echo "Wrote: benchmarks/embedded_compare/out/chart_all.png"

trap - EXIT
if [[ -n "${BENCH_STORAGE_DIR:-}" && -d "${BENCH_STORAGE_DIR:-}" ]]; then
	if command -v podman >/dev/null 2>&1; then
		podman unshare rm -rf "$BENCH_STORAGE_DIR" 2>/dev/null || rm -rf "$BENCH_STORAGE_DIR" 2>/dev/null || true
	else
		rm -rf "$BENCH_STORAGE_DIR" 2>/dev/null || true
	fi
fi
