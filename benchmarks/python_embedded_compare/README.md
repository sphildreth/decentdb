# Embedded Engine Comparison Bench (Docker)

This is a dockerized, re-runnable benchmark harness that:

- runs the same microbenchmarks at `10_000`, `100_000`, `1_000_000` operations
- emits machine-readable JSON results + a run manifest
- generates a single chart (initially: `µs/op` vs op count)

## Why Docker

Docker pins dependencies and engine versions so we can re-run over time as DecentDB changes.

Important fairness note:

- Durable-commit performance is sensitive to filesystem semantics.
- When running in Docker, the DB directory is bind-mounted (see `docker-compose.yml`) so we avoid overlayfs effects.

Toolchain note:

- The benchmark image installs a pinned Nim toolchain directly from nim-lang.org tarballs for reproducibility.

## Quick Start

From repo root:

- With Podman: `podman compose -f benchmarks/embedded_compare/docker-compose.yml up --build --abort-on-container-exit`
- With podman-compose: `podman-compose -f benchmarks/embedded_compare/docker-compose.yml up --build --abort-on-container-exit`

If you hit `/dev/net/tun` or networking errors with rootless Podman, run podman-compose like this:

- `podman-compose --in-pod=false --podman-build-args=--network=host --podman-run-args=--network=host -f benchmarks/embedded_compare/docker-compose.yml up --build --abort-on-container-exit`

Or run the helper (auto-detects Podman vs Docker):

- `bash benchmarks/embedded_compare/run_all.sh`

### Podman note (overlayfs errors)

If your rootless Podman storage is on an unsupported filesystem, you may see:

`kernel does not support overlay fs ... backing file system is unsupported`

The helper script works around this by using a temporary Podman storage config:

- prefers `fuse-overlayfs` if installed
- otherwise falls back to the `vfs` driver (slower image builds, but DB I/O is bind-mounted)

### Podman note (/dev/net/tun / pasta)

Some rootless Podman setups fail to build/run containers with:

`Failed to open() /dev/net/tun: No such device`

The helper script forces host networking for Podman builds/runs (via podman-compose flags) so `apt-get` and `dotnet restore` can work without `pasta` needing `/dev/net/tun`.

Outputs:

- JSON: `benchmarks/embedded_compare/out/results_py.json`
- Chart: `benchmarks/embedded_compare/out/chart.png`

## What gets benchmarked

Default benches (can be extended):

- `point_select`: single-row `SELECT ... WHERE id = ?` on a pre-seeded table
- `insert_txn`: `N` inserts inside one explicit transaction (one commit)

The chart uses `p50 µs/op`.

Schema note:

- SQLite uses `INTEGER PRIMARY KEY` in the harness.
- DecentDB uses `INT64 PRIMARY KEY` so it exercises DecentDB's rowid-optimized
	primary-key path (functionally equivalent 64-bit integer semantics).

## Engines

Currently implemented in the Python harness:

- DecentDB (Python bindings)
- SQLite (python `sqlite3`)
- DuckDB (python `duckdb`, if installed)
- H2 / Derby / HSQLDB via JDBC (JayDeBeApi + JPype)

LiteDB is handled by a separate .NET harness (planned next) and can be merged into the same chart.
