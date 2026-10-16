#!/usr/bin/env python3
"""Run pre-commit checks with simple, readable output.

Checks are organized into stages.  Within each stage, independent checks
run in parallel using a thread pool.  Stages execute sequentially so that
build artifacts are available before test stages begin.

Checks that invoke ``cargo`` are serialized within their stage via a
shared lock (cargo holds an exclusive file-lock on the target directory,
so true parallelism would just queue them anyway).

Every selected check is required. Missing toolchains fail preflight; explicit
--skip exclusions are reported as incomplete validation. Paranoid mode runs
sequentially; fast mode parallelizes independent checks within each stage.

When the ``rich`` library is available the output uses a live-updating
table with spinners and a progress bar.  Otherwise it falls back to
plain ANSI text.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
import platform
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Sequence

try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table
    from rich.text import Text

    HAS_RICH = True
except ImportError:
    HAS_RICH = False


REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_ROOT = REPO_ROOT / ".tmp" / "pre-commit-checks"
BENCHMARK_BASELINE_PATH = LOG_ROOT / "benchmark-comparison-baselines.json"
COMPARISON_HEADER_RE = re.compile(r"^===\s*(.+?)\s*===\s*$")

# Serializes cargo invocations (they share a target-directory file lock).
_cargo_lock = threading.Lock()
# Serializes terminal output so parallel checks don't interleave lines.
_baseline_lock = threading.Lock()
_process_lock = threading.Lock()
_processes: set[subprocess.Popen] = set()
_cancelled = threading.Event()
_lockfile_hashes: dict[Path, str | None] = {}


@dataclass(frozen=True)
class Check:
    key: str
    title: str
    cwd: Path
    command: str
    env: dict[str, str]
    stage: int = 0
    modes: tuple[str, ...] = ("paranoid",)
    benchmark_comparison: bool = False
    cargo_bound: bool = False
    requires: str | tuple[str, ...] | None = None
    unset_env: tuple[str, ...] = ()
    timeout_seconds: float = 1800


@dataclass
class CheckState:
    check: Check
    status: str = "pending"
    elapsed_seconds: float = 0.0
    return_code: int | None = None
    log_path: Path | None = None
    benchmark_drift: str | None = None
    detail: str | None = None
    update_benchmark_baselines: bool = False


# ANSI colors (fallback renderer only)
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
DIM = "\033[2m"
RESET = "\033[0m"
BOLD = "\033[1m"


def format_duration(seconds: float) -> str:
    total = int(seconds)
    minutes, secs = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def tail_lines(path: Path, max_lines: int = 80) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-max_lines:])


def extract_comparison_block(log_text: str) -> str | None:
    lines = log_text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.strip().startswith("=== Comparison"):
            start = i
            break
    if start is None:
        return None
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if lines[i].strip().startswith("===") and "Comparison" not in lines[i]:
            end = i
            break
    return "\n".join(lines[start:end])


def parse_benchmark_comparisons(text: str) -> dict[str, dict[str, list[str]]]:
    sections: dict[str, dict[str, list[str]]] = {}
    current_label: str | None = None
    current_side: str | None = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        header_match = COMPARISON_HEADER_RE.match(line)
        if header_match:
            label = header_match.group(1).strip()
            if label.startswith("Comparison (") and label.endswith(")"):
                label = label[len("Comparison (") : -1]
            if "DecentDB vs SQLite" in label:
                current_label = label
                current_side = None
                sections.setdefault(current_label, {"decentdb": [], "sqlite": []})
            else:
                current_label = None
                current_side = None
            continue

        if current_label is None:
            continue
        if line == "DecentDB better at:":
            current_side = "decentdb"
            continue
        if line == "SQLite better at:":
            current_side = "sqlite"
            continue
        if not line.startswith("- ") or current_side is None:
            continue

        item = line[2:].strip()
        if item and item.lower() != "none":
            metric_name = item.split(":", 1)[0].strip()
            sections[current_label][current_side].append(metric_name)

    return sections


def load_benchmark_baselines() -> dict[str, dict[str, dict[str, list[str]]]]:
    if not BENCHMARK_BASELINE_PATH.exists():
        return {}
    try:
        return json.loads(BENCHMARK_BASELINE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_benchmark_baselines(
    baselines: dict[str, dict[str, dict[str, list[str]]]],
) -> None:
    BENCHMARK_BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=BENCHMARK_BASELINE_PATH.parent,
        prefix="benchmark-baseline-", suffix=".tmp", delete=False,
    ) as handle:
        temporary_path = Path(handle.name)
        try:
            handle.write(json.dumps(baselines, indent=2, sort_keys=True) + "\n")
            handle.close()
            temporary_path.replace(BENCHMARK_BASELINE_PATH)
        finally:
            temporary_path.unlink(missing_ok=True)


def compute_benchmark_drift(
    current: dict[str, dict[str, list[str]]],
    baselines: dict[str, dict[str, dict[str, list[str]]]],
    check_key: str,
) -> str | None:
    if not current:
        return None

    previous = baselines.get(check_key, {})
    results = []
    for label in sorted(current):
        current_decent = set(current[label]["decentdb"])
        previous_decent = set(previous.get(label, {}).get("decentdb", []))

        if label not in previous:
            results.append("baseline")
            continue

        gained = current_decent - previous_decent
        lost = previous_decent - current_decent

        if lost:
            results.append("regressed")
        elif gained:
            results.append("improved")
        else:
            results.append("same")

    if "regressed" in results:
        return "REGRESSED"
    if "improved" in results:
        return "IMPROVED"
    if all(r == "baseline" for r in results):
        return "BASELINE"
    return "STABLE"


def _java_home_from_runtime(env: dict[str, str]) -> Path | None:
    try:
        output = subprocess.check_output(
            ["java", "-XshowSettings:properties", "-version"],
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            timeout=10,
        )
    except (subprocess.SubprocessError, OSError):
        return None

    for line in output.splitlines():
        match = re.search(r"\bjava\.home\s*=\s*(.+)$", line.strip())
        if match:
            return Path(match.group(1))
    return None


def _has_jni_headers(java_home: Path) -> bool:
    return (java_home / "include" / "jni.h").exists()


def _java_home_with_jni_headers(env: dict[str, str]) -> Path | None:
    candidates: list[Path] = []
    if env.get("JAVA_HOME"):
        candidates.append(Path(env["JAVA_HOME"]))

    runtime_home = _java_home_from_runtime(env)
    if runtime_home is not None:
        candidates.append(runtime_home)

    # Allows local validation on minimal Java runtime packages without root.
    candidates.extend(
        [
            REPO_ROOT / ".tmp" / "jdk" / "usr" / "lib" / "jvm" / "java-25-openjdk",
            REPO_ROOT / ".tmp" / "jdk25-full" / "usr" / "lib" / "jvm" / "java-25-openjdk",
        ]
    )

    # Runtime-only packages may be the default even when a complete JDK is
    # installed beside them. Prefer the caller's JDK, then discover local ones.
    candidates.extend(sorted(Path("/usr/lib/jvm").glob("*")))

    for candidate in candidates:
        if (candidate / "bin" / "java").exists() and _has_jni_headers(candidate):
            return candidate
    return None


def java_binding_env() -> dict[str, str]:
    env = os.environ.copy()
    java_home = _java_home_with_jni_headers(env)
    if java_home is None:
        return {}

    path = os.environ.get("PATH", "")
    return {
        "JAVA_HOME": str(java_home),
        "PATH": f"{java_home / 'bin'}{os.pathsep}{path}",
    }


def build_checks() -> list[Check]:
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", REPO_ROOT / "target"))
    if not target_dir.is_absolute():
        target_dir = REPO_ROOT / target_dir
    release_dir = target_dir / "release"
    library_name = {"Darwin": "libdecentdb.dylib", "Windows": "decentdb.dll"}.get(
        platform.system(), "libdecentdb.so"
    )
    native_lib = release_dir / library_name
    native_env = {
        "DECENTDB_NATIVE_LIB": str(native_lib),
        **{name: os.pathsep.join(filter(None, (str(release_dir), os.environ.get(name))))
           for name in ("PATH", "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH")},
    }
    include_dir = REPO_ROOT / "include"
    python_env = {
        "DECENTDB_NATIVE_LIB": str(native_lib),
        "PYTHONPATH": ".",
    }
    java_env = java_binding_env()
    python_exec = shlex.quote(sys.executable)
    c_warnings = {"CFLAGS": f"{os.environ.get('CFLAGS', '')} -Werror".strip()}
    npm_env = {"NPM_CONFIG_ENGINE_STRICT": "true"}

    checks = [
        # ── Stage 0: Static analysis (parallel, fast) ──
        Check(
            key="whitespace", title="Working tree and staged whitespace",
            cwd=REPO_ROOT, command="git diff --check && git diff --cached --check",
            env={}, modes=("fast", "paranoid"), requires="git",
        ),
        Check(
            key="vendored-headers", title="Vendored C ABI headers",
            cwd=REPO_ROOT, command=f"{python_exec} scripts/check_vendored_headers.py",
            env={}, modes=("fast", "paranoid"),
        ),
        Check(
            key="python-tooling-tests", title="Repository Python automation tests",
            cwd=REPO_ROOT,
            command=f"{python_exec} -W error -m unittest discover -s scripts/tests -p 'test_*.py'",
            env={}, modes=("fast", "paranoid"),
        ),
        Check(
            key="rust-format",
            title="Rust fmt check",
            cwd=REPO_ROOT,
            command="cargo fmt --check",
            env={},
            stage=0,
            modes=("fast", "paranoid"),
        ),
        Check(
            key="rust-clippy",
            title="Rust clippy (workspace)",
            cwd=REPO_ROOT,
            command="cargo clippy --locked --workspace --all-targets --all-features -- -D warnings",
            env={},
            stage=0,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        Check(
            key="rust-supply-chain",
            title="Rust supply-chain checks",
            cwd=REPO_ROOT,
            command=(
                "cargo deny check && "
                "cargo deny --manifest-path benchmarks/rust-baseline/Cargo.toml check && "
                "cargo deny --manifest-path fuzz/Cargo.toml check"
            ),
            env={},
            stage=0,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        Check(
            key="cargo-metadata",
            title="Cargo metadata parse",
            cwd=REPO_ROOT,
            command="cargo metadata --locked --no-deps --format-version 1 >/dev/null",
            env={},
            stage=0,
            cargo_bound=True,
        ),
        Check(
            key="release-metadata",
            title="Release metadata/version guardrails",
            cwd=REPO_ROOT,
            command=f"{python_exec} scripts/validate_release_metadata.py",
            env={},
            stage=0,
            modes=("fast", "paranoid"),
        ),
        Check(
            key="error-diagnostics-guardrail",
            title="Structured error diagnostics guardrail",
            cwd=REPO_ROOT,
            command=f"{python_exec} scripts/validate_error_diagnostics.py --strict",
            env={},
            stage=0,
            modes=("fast", "paranoid"),
        ),
        Check(
            key="rust-independent-checks", title="Independent Rust workspaces",
            cwd=REPO_ROOT,
            command=(
                "cargo fmt --manifest-path fuzz/Cargo.toml --check && "
                "cargo fmt --manifest-path benchmarks/rust-baseline/Cargo.toml --check && "
                "cargo clippy --locked --manifest-path fuzz/Cargo.toml --all-targets -- -D warnings && "
                "cargo clippy --locked --manifest-path benchmarks/rust-baseline/Cargo.toml "
                "--all-targets --all-features -- -D warnings"
            ),
            env={}, stage=0, cargo_bound=True,
        ),
        # ── Stage 1: Clean + rebuild (guarantees no stale artifacts) ──
        Check(
            key="rust-clean",
            title="Rust clean (both profiles)",
            cwd=REPO_ROOT,
            command="cargo clean --release -p decentdb && cargo clean -p decentdb",
            env={},
            stage=1,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        Check(
            key="rust-release-build",
            title="Rust release build",
            cwd=REPO_ROOT,
            command="cargo build --locked --release -p decentdb -p decentdb-cli",
            env={},
            stage=2,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        # ── Stage 3: Fast regression tests (parallel) ──
        Check(
            key="rust-regressions",
            title="Rust regression tests",
            cwd=REPO_ROOT,
            command=(
                "cargo test --locked -p decentdb --test sql_dml_tests --quiet && "
                "cargo test --locked -p decentdb --test sql_expressions_tests --quiet"
            ),
            env={},
            stage=3,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        Check(
            key="rust-persistence-regression",
            title="Rust persistence regression",
            cwd=REPO_ROOT,
            command=(
                "cargo test --locked -p decentdb --test sql_persistence_tests --quiet "
                "checkpointed_append_only_overflow_rows_keep_primary_key_unique -- --exact"
            ),
            env={},
            stage=3,
            modes=("fast", "paranoid"),
            cargo_bound=True,
        ),
        Check(
            key="python-fastdecode-smoke",
            title="Python fastdecode smoke",
            cwd=REPO_ROOT / "bindings" / "python",
            command=(
                f"{python_exec} ../../scripts/build_python_fastdecode.py && "
                f"{python_exec} -W error -m pytest -q tests/test_basic.py "
                "-k 'fastdecode_imports_with_native_lib_env'"
            ),
            env={**python_env, **c_warnings},
            stage=3,
            modes=("fast", "paranoid"),
        ),
        Check(
            key="python-regressions",
            title="Python regression smoke",
            cwd=REPO_ROOT / "bindings" / "python",
            command=(
                f"{python_exec} -W error -m pytest -q tests/test_comprehensive.py "
                "-k 'cursor_rowcount_after_operations or cursor_execute_two_float_range_scan'"
            ),
            env=python_env,
            stage=3,
            modes=("fast", "paranoid"),
        ),
        # ── Stage 4: Full test suites (parallel, cargo-bound ones serialized) ──
        Check(
            key="rust-core",
            title="Rust core tests",
            cwd=REPO_ROOT,
            command="cargo test --locked -p decentdb",
            env={},
            stage=4,
            cargo_bound=True,
        ),
        Check(
            key="rust-cli",
            title="Rust CLI tests",
            cwd=REPO_ROOT,
            command="cargo test --locked -p decentdb-cli",
            env={},
            stage=4,
            cargo_bound=True,
        ),
        Check(
            key="rust-migrate",
            title="Rust migrate tests",
            cwd=REPO_ROOT,
            command="cargo test --locked -p decentdb-migrate",
            env={},
            stage=4,
            cargo_bound=True,
        ),
        Check(
            key="rust-workspace-extra",
            title="Rust remaining workspace crate tests",
            cwd=REPO_ROOT,
            command=(
                "cargo test --locked --workspace "
                "--exclude decentdb "
                "--exclude decentdb-cli "
                "--exclude decentdb-migrate"
            ),
            env={},
            stage=4,
            cargo_bound=True,
        ),
        Check(
            key="rust-baseline-tests", title="Independent Rust benchmark tests",
            cwd=REPO_ROOT,
            command="cargo test --locked --manifest-path benchmarks/rust-baseline/Cargo.toml --all-features",
            env={}, stage=4, cargo_bound=True,
        ),
        Check(
            key="python-benchmark-tests", title="Python benchmark framework tests",
            cwd=REPO_ROOT / "benchmarks/python_embedded_compare",
            command=f"{python_exec} -W error -m pytest -q tests",
            env=python_env, stage=4,
        ),
        Check(
            key="python-smoke",
            title="Python full test suite",
            cwd=REPO_ROOT / "bindings" / "python",
            command=f"{python_exec} -W error -m pytest -q tests/",
            env=python_env,
            stage=4,
        ),
        Check(
            key="python-ffi",
            title="Python FFI smoke",
            cwd=REPO_ROOT,
            command=f"{python_exec} tests/bindings/python/test_ffi.py",
            env={"DECENTDB_NATIVE_LIB": str(native_lib)},
            stage=4,
        ),
        Check(
            key="dotnet-stack",
            title=".NET clean/build/test",
            cwd=REPO_ROOT / "bindings" / "dotnet",
            command=(
                "dotnet restore DecentDB.NET.sln && "
                "dotnet clean DecentDB.NET.sln -c Release --nologo && "
                "dotnet build DecentDB.NET.sln -c Release --no-restore --nologo -warnaserror && "
                "dotnet test --solution DecentDB.NET.sln -c Release --no-build --nologo"
            ),
            env={},
            stage=4,
        ),
        Check(
            key="dotnet-standalone", title="Standalone .NET smoke and benchmark projects",
            cwd=REPO_ROOT,
            command=(
                "dotnet build tests/bindings/dotnet/Smoke/Smoke.csproj -c Release --nologo -warnaserror && "
                "dotnet build benchmarks/python_embedded_compare/helpers/litedb/LiteDbBenchmark.csproj "
                "-c Release --nologo -warnaserror && "
                "dotnet run --project tests/bindings/dotnet/Smoke/Smoke.csproj -c Release --no-build"
            ),
            env=native_env, stage=4,
        ),
        Check(
            key="go-tests",
            title="Go binding tests",
            cwd=REPO_ROOT / "bindings" / "go" / "decentdb-go",
            command="go test -count=1 ./...",
            env={},
            stage=4,
            requires="go",
        ),
        Check(
            key="go-bun-example",
            title="Go Bun ORM integration example",
            cwd=REPO_ROOT / "bindings" / "go" / "examples" / "bun",
            command="go test -count=1 ./... && go vet ./...",
            env={},
            stage=4,
            requires="go",
        ),
        Check(
            key="java-tests",
            title="Java binding tests",
            cwd=REPO_ROOT / "bindings" / "java",
            command=(
                "./gradlew --warning-mode fail --project-cache-dir ../../.tmp/gradle-java "
                f":driver:clean :driver:test -PnativeLibDir={shlex.quote(str(release_dir))}"
            ),
            env=java_env,
            stage=4,
            requires=("java", "javac", "make", "cc"),
            cargo_bound=True,
        ),
        Check(
            key="java-dbeaver", title="JDBC and DBeaver packages",
            cwd=REPO_ROOT / "bindings/java",
            command=(
                "./gradlew --warning-mode fail --project-cache-dir ../../.tmp/gradle-java "
                f":driver:jar :dbeaver-extension:clean :dbeaver-extension:jar "
                f"-PnativeLibDir={shlex.quote(str(release_dir))}"
            ),
            env=java_env, stage=5, requires=("java", "javac", "make", "cc"), cargo_bound=True,
        ),
        Check(
            key="java-ffm", title="Java stable FFM smoke",
            cwd=REPO_ROOT,
            command=(
                "mkdir -p .tmp/java-ffm && "
                "javac -Xlint:all -Werror --release 25 -d .tmp/java-ffm tests/bindings/java/Smoke.java && "
                "java --enable-native-access=ALL-UNNAMED -cp .tmp/java-ffm Smoke"
            ),
            env={**java_env, "DECENTDB_NATIVE_LIB": str(native_lib)},
            stage=4, requires=("java", "javac"),
        ),
        Check(
            key="node-tests",
            title="Node.js binding tests",
            cwd=REPO_ROOT / "bindings" / "node" / "decentdb",
            command="npm ci && npm test",
            env={"DECENTDB_NATIVE_LIB_PATH": str(native_lib), **c_warnings, **npm_env},
            stage=4,
            requires=("node", "npm"),
        ),
        Check(
            key="node-knex-tests",
            title="Node.js Knex dialect tests",
            cwd=REPO_ROOT / "bindings" / "node" / "knex-decentdb",
            command="npm ci && npm test",
            env={"DECENTDB_NATIVE_LIB_PATH": str(native_lib), **c_warnings, **npm_env},
            stage=5,
            requires=("node", "npm"),
        ),
        Check(
            key="web-typecheck-build",
            title="Web package typecheck/build",
            cwd=REPO_ROOT / "bindings" / "web",
            command="npm ci && npm run typecheck && npm run build",
            env=npm_env,
            stage=4,
            requires=("node", "npm"),
        ),
        Check(
            key="c-smoke",
            title="C binding smoke test",
            cwd=REPO_ROOT,
            command=(
                "mkdir -p .tmp/bindings && "
                f"cc -Wall -Wextra -Werror -I{shlex.quote(str(include_dir))} tests/bindings/c/smoke.c "
                f"-L{shlex.quote(str(release_dir))} -Wl,-rpath,{shlex.quote(str(release_dir))} -ldecentdb "
                "-o .tmp/bindings/c-smoke && .tmp/bindings/c-smoke"
            ),
            env={},
            stage=4,
            requires="cc",
        ),
        Check(
            key="dart-tests",
            title="Dart binding tests",
            cwd=REPO_ROOT / "bindings" / "dart" / "dart",
            command="dart pub get --enforce-lockfile && dart test",
            env={"DECENTDB_NATIVE_LIB": str(native_lib)},
            stage=4,
            requires="dart",
        ),
        Check(
            key="dart-analysis", title="Dart packages and examples analysis",
            cwd=REPO_ROOT,
            command=" && ".join(
                f"(cd {package} && dart pub get --enforce-lockfile && dart analyze --fatal-infos)"
                for package in (
                    "bindings/dart/dart", "bindings/dart/examples/console",
                    "bindings/dart/examples/console_complex", "bindings/dart/examples/flutter_desktop",
                    "tests/bindings/dart",
                )
            ) + " && (cd tests/bindings/dart && dart run smoke.dart)",
            env={"DECENTDB_NATIVE_LIB": str(native_lib)}, stage=5, requires="dart",
        ),
        Check(
            key="flutter-tests", title="Flutter package and reference app validation",
            cwd=REPO_ROOT,
            command=(
                "(cd bindings/dart/flutter && flutter pub get --enforce-lockfile && "
                "flutter analyze --fatal-infos --fatal-warnings && flutter test) && "
                "(cd bindings/dart/flutter/example && flutter pub get --enforce-lockfile && "
                "flutter analyze --fatal-infos --fatal-warnings)"
            ),
            env={"DECENTDB_NATIVE_LIB": str(native_lib)}, stage=5, requires="flutter",
        ),
        # ── Stage 5: Storage harnesses and benchmark smoke checks (parallel) ──
        Check(
            key="storage-short-crash",
            title="Storage short crash harness",
            cwd=REPO_ROOT,
            command=f"{python_exec} tests/harness/runner.py tests/harness/scenarios/short_crash.json",
            env={},
            stage=5,
            cargo_bound=True,
        ),
        Check(
            key="storage-soak",
            title="Storage soak harness",
            cwd=REPO_ROOT,
            command=f"{python_exec} tests/harness/runner.py tests/harness/scenarios/soak_storage.json",
            env={},
            stage=5,
            cargo_bound=True,
        ),
        Check(
            key="grouped-commit-fault-injection",
            title="Grouped commit fault-injection harnesses",
            cwd=REPO_ROOT,
            command=(
                f"{python_exec} tests/harness/runner.py "
                "tests/harness/scenarios/grouped_commit_all_commit.json && "
                f"{python_exec} tests/harness/runner.py "
                "tests/harness/scenarios/grouped_commit_fail_during_second_commit.json && "
                f"{python_exec} tests/harness/runner.py "
                "tests/harness/scenarios/grouped_commit_fail_sync_before_flush.json && "
                f"{python_exec} tests/harness/runner.py "
                "tests/harness/scenarios/grouped_commit_crash_after_sync.json"
            ),
            env={},
            stage=5,
            cargo_bound=True,
        ),
        Check(
            key="rust-benchmark-phase1-native",
            title="Rust benchmark phase 1 direct scenarios",
            cwd=REPO_ROOT,
            command=(
                "cargo run --locked -p decentdb-benchmark -- run "
                "--profile smoke "
                "--scenario durable_commit_single "
                "--scenario read_under_write "
                "--durable-commits 120 "
                "--rows 2000 "
                "--point-reads 1000 "
                "--writer-ops 120 "
                "--reader-threads 2 "
                "--trials 1 "
                "--scratch-root .tmp/decentdb-benchmark-phase1-direct "
                "--artifact-root .tmp/decentdb-benchmark-phase1-direct-artifacts"
            ),
            env={},
            stage=5,
            cargo_bound=True,
        ),
        Check(
            key="rust-benchmark-phase1-queued-writers",
            title="Rust benchmark queued write scenarios",
            cwd=REPO_ROOT,
            command=(
                "cargo run --locked -p decentdb-benchmark -- run "
                "--profile smoke "
                "--scenario queued_writer_single "
                "--scenario queued_writer_read_under_write "
                "--point-reads 100 "
                "--writer-ops 20 "
                "--durable-commits 30 "
                "--rows 500 "
                "--trials 1 "
                "--scratch-root .tmp/decentdb-benchmark-phase1-queued "
                "--artifact-root .tmp/decentdb-benchmark-phase1-queued-artifacts"
            ),
            env={},
            stage=5,
            cargo_bound=True,
        ),
        Check(
            key="python-benchmark",
            title="Python complex benchmark",
            cwd=REPO_ROOT / "bindings" / "python",
            command=(
                f"{python_exec} benchmarks/bench_complex.py "
                "--users 1000 --items 50 --orders 100"
            ),
            env=python_env,
            stage=5,
            benchmark_comparison=True,
        ),
        Check(
            key="dotnet-benchmark",
            title=".NET benchmark harness",
            cwd=REPO_ROOT
            / "bindings"
            / "dotnet"
            / "benchmarks"
            / "DecentDB.Benchmarks",
            command=(
                "dotnet restore DecentDB.Benchmarks.csproj && "
                "dotnet clean DecentDB.Benchmarks.csproj -c Release --nologo && "
                "dotnet build DecentDB.Benchmarks.csproj -c Release --no-restore --nologo -warnaserror && "
                "dotnet run -c Release --no-build --project DecentDB.Benchmarks.csproj -- "
                "--count 50000 --point-reads 50000 --fetchmany-batch 1000"
            ),
            env={},
            stage=5,
            benchmark_comparison=True,
        ),
        # ── Stage 6: Release-facing artifacts, docs, and browser/WASM smoke ──
        Check(
            key="rust-release-metrics-bench",
            title="Rust release metrics benchmark",
            cwd=REPO_ROOT,
            command="cargo bench --locked -p decentdb --bench release_metrics",
            env={},
            stage=6,
            cargo_bound=True,
        ),
        Check(
            key="rust-docs",
            title="Rust doctests and API docs",
            cwd=REPO_ROOT,
            command="cargo test --locked --workspace --doc && cargo doc --locked --workspace --no-deps",
            env={"RUSTDOCFLAGS": f"{os.environ.get('RUSTDOCFLAGS', '')} -D warnings".strip()},
            stage=6,
            cargo_bound=True,
        ),
        Check(
            key="mkdocs-build",
            title="MkDocs site build",
            cwd=REPO_ROOT,
            command=(
                f"{python_exec} -m venv .tmp/precommit-mkdocs-venv && "
                ". .tmp/precommit-mkdocs-venv/bin/activate && "
                "python -m pip install -q -r docs/requirements.txt && "
                "mkdocs build --strict --config-file mkdocs.yml --site-dir .tmp/mkdocs-site"
            ),
            env={},
            stage=6,
        ),
        Check(
            key="web-wasm-browser-smoke",
            title="Web WASM browser OPFS smoke",
            cwd=REPO_ROOT,
            command=(
                "cargo build --locked -p decentdb --target wasm32-unknown-unknown --release && "
                "wasm-bindgen target/wasm32-unknown-unknown/release/decentdb.wasm "
                "--target web "
                "--out-dir bindings/web/dist "
                "--out-name decentdb_wasm && "
                "cd bindings/web && "
                "npm ci && "
                "npm run build && "
                "npm run browser:install && "
                "npm run browser:smoke:ci"
            ),
            env=npm_env,
            stage=6,
            cargo_bound=True,
            requires=("node", "npm", "wasm-bindgen"),
            # Playwright forces color in workers; inherited NO_COLOR conflicts.
            unset_env=("NO_COLOR",),
        ),
    ]
    declared_checks = []
    for check in checks:
        tools = list(_required_tools(check))
        if check.cargo_bound or check.command.startswith("cargo "):
            tools.append("cargo")
        if check.key == "rust-supply-chain":
            tools.append("cargo-deny")
        if check.key.startswith("dotnet-"):
            tools.append("dotnet")
        if check.key == "python-fastdecode-smoke":
            tools.append("cc")
        declared_checks.append(replace(check, requires=tuple(dict.fromkeys(tools))))
    return declared_checks


def check_environment(check: Check) -> dict[str, str]:
    env = os.environ.copy()
    env.update(check.env)
    for key in check.unset_env:
        env.pop(key, None)
    return env


def lockfile_digest(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def snapshot_lockfiles() -> dict[Path, str | None]:
    paths = subprocess.check_output(
        ["git", "ls-files", "-z"], cwd=REPO_ROOT, timeout=10,
    ).decode().split("\0")
    names = {"Cargo.lock", "package-lock.json", "pubspec.lock", "go.sum", "go.mod"}
    return {
        REPO_ROOT / path: lockfile_digest(REPO_ROOT / path)
        for path in paths if Path(path).name in names
    }


def stop_process(process: subprocess.Popen) -> None:
    """Stop the check's entire process tree, including stubborn grandchildren."""
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    else:
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
            check=False,
        )
    process.wait(timeout=10)


def cancel_checks() -> None:
    _cancelled.set()
    with _process_lock:
        processes = list(_processes)
    for process in processes:
        stop_process(process)


def run_check(
    state: CheckState,
    log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    state.log_path = log_root / f"{state.check.key}.log"
    state.status = "running"
    start = time.perf_counter()

    env = check_environment(state.check)

    lock = _cargo_lock if state.check.cargo_bound else contextlib.nullcontext()
    try:
        with lock, state.log_path.open("w", encoding="utf-8") as log_file:
            if _cancelled.is_set():
                state.status = "cancelled"
                state.return_code = 130
                return
            log_file.write(f"$ (cd {state.check.cwd} && {state.check.command})\n\n")
            log_file.flush()
            try:
                process = subprocess.Popen(
                    ["bash", "-o", "pipefail", "-c", state.check.command],
                    cwd=state.check.cwd, env=env, stdout=log_file,
                    stderr=subprocess.STDOUT, text=True,
                    start_new_session=os.name == "posix",
                )
                with _process_lock:
                    _processes.add(process)
                try:
                    if _cancelled.is_set():
                        stop_process(process)
                    process.wait(timeout=state.check.timeout_seconds)
                    state.return_code = process.returncode
                except subprocess.TimeoutExpired:
                    stop_process(process)
                    state.return_code = 124
                    state.detail = f"Timed out after {state.check.timeout_seconds:g}s"
                except BaseException:
                    stop_process(process)
                    raise
                finally:
                    with _process_lock:
                        _processes.discard(process)
            except OSError as error:
                state.return_code = 1
                state.detail = f"Could not start check: {error}"
            changed = [str(path.relative_to(REPO_ROOT)) for path, digest in _lockfile_hashes.items()
                       if lockfile_digest(path) != digest]
            if changed:
                state.return_code = 1
                state.detail = "Tracked dependency files changed: " + ", ".join(changed)
            if state.detail:
                log_file.write(f"\n{state.detail}\n")
            state.status = "cancelled" if _cancelled.is_set() else (
                "passed" if state.return_code == 0 else "failed"
            )
    finally:
        state.elapsed_seconds = time.perf_counter() - start

    if state.return_code == 0 and state.check.benchmark_comparison:
        log_text = state.log_path.read_text(encoding="utf-8", errors="replace")
        current_sections = parse_benchmark_comparisons(log_text)
        with _baseline_lock:
            state.benchmark_drift = compute_benchmark_drift(
                current_sections, benchmark_baselines, state.check.key
            )
            if current_sections:
                (log_root / f"{state.check.key}.comparison.json").write_text(
                    json.dumps(current_sections, indent=2) + "\n", encoding="utf-8"
                )
                if state.update_benchmark_baselines:
                    benchmark_baselines[state.check.key] = current_sections
                    save_benchmark_baselines(benchmark_baselines)


def select_checks(
    all_checks: Sequence[Check], selected_keys: Sequence[str], mode: str
) -> list[Check]:
    if not selected_keys:
        return [check for check in all_checks if mode in check.modes]

    selected_set = set(selected_keys)
    unknown = sorted(selected_set - {check.key for check in all_checks})
    if unknown:
        raise SystemExit(f"Unknown check key(s): {', '.join(unknown)}")
    return [check for check in all_checks if check.key in selected_set]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run pre-commit checks.")
    parser.add_argument(
        "--mode",
        choices=("fast", "paranoid"),
        default="paranoid",
        help="Choose a faster smoke pass or the full paranoia suite.",
    )
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        metavar="CHECK",
        help="Run only the named check key (repeatable).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available check keys and exit.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately after the first failing stage.",
    )
    parser.add_argument(
        "--skip", action="append", default=[], metavar="CHECK",
        help="Explicitly exclude a selected check; the result is reported as incomplete.",
    )
    parser.add_argument(
        "--timeout-seconds", type=float,
        help="Override each check's timeout (default: 1800 seconds).",
    )
    parser.add_argument(
        "--update-benchmark-baselines", action="store_true",
        help="Explicitly replace informational benchmark comparison baselines.",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Disable parallel execution (already the default for paranoid mode).",
    )
    return parser.parse_args()


def _run_stage(
    stage_states: list[tuple[int, CheckState]],
    log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
    sequential: bool,
    on_complete: Callable[[CheckState], None] | None = None,
) -> list[CheckState]:
    """Run all checks in a stage and return any that failed."""
    failures: list[CheckState] = []

    def completed(state: CheckState) -> None:
        if state.status == "failed":
            failures.append(state)
        if on_complete is not None:
            on_complete(state)

    pool = None
    try:
        if sequential or len(stage_states) == 1:
            for _idx, state in stage_states:
                run_check(state, log_root, benchmark_baselines)
                completed(state)
        else:
            pool = ThreadPoolExecutor(max_workers=len(stage_states))
            futures = {
                pool.submit(run_check, state, log_root, benchmark_baselines): state
                for _idx, state in stage_states
            }
            for future in as_completed(futures):
                future.result()
                completed(futures[future])
    except BaseException:
        cancel_checks()
        raise
    finally:
        if pool is not None:
            pool.shutdown(wait=True, cancel_futures=True)

    return failures


def _required_tools(check: Check) -> tuple[str, ...]:
    if check.requires is None:
        return ()
    if isinstance(check.requires, str):
        return (check.requires,)
    return check.requires


def _check_skip_reason(check: Check) -> str | None:
    """Return a human-readable skip reason, or None if the check can run."""
    env = check_environment(check)
    for tool in _required_tools(check):
        if not shutil.which(tool, path=env.get("PATH")):
            return f"{tool} not found"
    # Gradle 8.x doesn't support JDK 25+; detect and skip gracefully.
    if check.key in {"java-tests", "java-dbeaver", "java-ffm"}:
        if _java_home_with_jni_headers(env) is None:
            return "JDK headers (jni.h) not found"
        try:
            ver_out = subprocess.check_output(
                ["java", "-version"], stderr=subprocess.STDOUT, env=env, text=True,
                timeout=10,
            )
            match = re.search(r'"(\d+)', ver_out)
            if match and int(match.group(1)) >= 25:
                props = check.cwd / "gradle" / "wrapper" / "gradle-wrapper.properties"
                if props.exists():
                    text = props.read_text()
                    gv_match = re.search(r"gradle-(\d+)\.", text)
                    if gv_match and int(gv_match.group(1)) < 9:
                        return (
                            f"Gradle {gv_match.group(1)}.x incompatible with JDK {match.group(1)} "
                            "(needs Gradle 9+)"
                        )
        except (subprocess.SubprocessError, OSError, ValueError) as error:
            return f"Java preflight failed: {error}"
    if check.key == "java-dbeaver":
        configured = env.get("DBEAVER_PLUGIN_DIR")
        candidates = [Path(configured)] if configured else []
        candidates += [Path("/usr/lib/dbeaver/plugins"), Path("/usr/share/dbeaver/plugins")]
        if not any(path.is_dir() and any(path.glob("*.jar")) for path in candidates):
            return "DBeaver SDK not found; set DBEAVER_PLUGIN_DIR to its plugins directory"
    return None


def main() -> int:
    global _lockfile_hashes
    args = parse_args()
    all_checks = build_checks()

    if args.list:
        if HAS_RICH:
            return _list_checks_rich(all_checks)
        return _list_checks_plain(all_checks)

    checks = select_checks(all_checks, args.only, args.mode)
    if args.timeout_seconds is not None:
        if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
            raise SystemExit("--timeout-seconds must be finite and greater than zero")
        checks = [replace(check, timeout_seconds=args.timeout_seconds) for check in checks]
    unknown = set(args.skip) - {check.key for check in checks}
    if unknown:
        raise SystemExit("--skip must name selected checks: " + ", ".join(sorted(unknown)))

    active_checks: list[Check] = []
    skipped: list[tuple[Check, str]] = []
    missing: list[tuple[Check, str]] = []
    for check in checks:
        if check.key in args.skip:
            skipped.append((check, "explicit --skip"))
            continue
        skip_reason = _check_skip_reason(check)
        if skip_reason:
            missing.append((check, skip_reason))
        else:
            active_checks.append(check)
    if missing:
        print("FAILED preflight: required checks cannot run:", file=sys.stderr)
        for check, reason in missing:
            print(f"  {check.key}: {reason}", file=sys.stderr)
        print("Install the prerequisites or explicitly exclude a check with --skip CHECK.", file=sys.stderr)
        return 2
    if not active_checks:
        print("FAILED: no checks selected to execute.", file=sys.stderr)
        return 2
    if shutil.which("bash") is None:
        print("FAILED preflight: bash is required.", file=sys.stderr)
        return 2
    try:
        _lockfile_hashes = snapshot_lockfiles()
    except (OSError, subprocess.SubprocessError) as error:
        print(f"FAILED preflight: cannot snapshot dependency files: {error}", file=sys.stderr)
        return 2
    _cancelled.clear()

    LOG_ROOT.mkdir(parents=True, exist_ok=True)
    run_log_root = Path(tempfile.mkdtemp(prefix=time.strftime("%Y%m%d-%H%M%S-"), dir=LOG_ROOT))
    states = [CheckState(check=check, update_benchmark_baselines=args.update_benchmark_baselines)
              for check in active_checks]
    started_at = time.perf_counter()
    benchmark_baselines = load_benchmark_baselines()

    mode_label = args.mode
    sequential = args.sequential or args.mode == "paranoid"
    if sequential:
        mode_label += ", sequential"

    # Group states by stage.
    stages: dict[int, list[tuple[int, CheckState]]] = {}
    for idx, state in enumerate(states, start=1):
        stages.setdefault(state.check.stage, []).append((idx, state))

    if HAS_RICH:
        result = _run_rich(
            states, stages, skipped, run_log_root, benchmark_baselines,
            mode_label, sequential, started_at, args.fail_fast,
        )
    else:
        result = _run_plain(
            states, stages, skipped, run_log_root, benchmark_baselines,
            mode_label, sequential, started_at, args.fail_fast,
        )
    (run_log_root / "summary.json").write_text(json.dumps({
        "mode": args.mode, "sequential": sequential, "exit_code": result,
        "selection": args.only,
        "complete": result == 0 and not skipped and not args.only
                    and all(state.status == "passed" for state in states),
        "checks": [{
            "key": state.check.key, "status": state.status,
            "return_code": state.return_code, "seconds": state.elapsed_seconds,
            "detail": state.detail, "benchmark_comparison": state.benchmark_drift,
        } for state in states],
        "excluded": [check.key for check, _ in skipped],
    }, indent=2) + "\n", encoding="utf-8")
    return result


# ─── Rich renderer ──────────────────────────────────────────────────────────


_STATUS_STYLE = {
    "pending": ("dim", "⋯"),
    "running": ("cyan bold", "⟳"),
    "passed": ("green", "✓"),
    "failed": ("red bold", "✗"),
    "skipped": ("yellow", "○"),
    "cancelled": ("yellow", "○"),
}


def _build_live_table(
    states: list[CheckState],
    skipped: list[tuple[Check, str]],
) -> Table:
    table = Table(
        show_header=True,
        header_style="bold",
        expand=True,
        border_style="dim",
        pad_edge=False,
    )
    table.add_column("", width=3, justify="center", no_wrap=True)
    table.add_column("Check", ratio=3, no_wrap=True)
    table.add_column("Stage", width=5, justify="center")
    table.add_column("Status", width=10, justify="center")
    table.add_column("Time", width=8, justify="right")
    table.add_column("Info", ratio=2)

    for state in states:
        style, icon = _STATUS_STYLE.get(state.status, ("", "?"))
        status_text = Text(state.status.upper(), style=style)
        icon_text = Text(icon, style=style)
        elapsed = format_duration(state.elapsed_seconds) if state.status in ("passed", "failed") else ""
        info = state.detail or ""
        if state.benchmark_drift:
            drift_style = {
                "REGRESSED": "red bold",
                "IMPROVED": "green",
                "STABLE": "dim",
                "BASELINE": "yellow",
            }.get(state.benchmark_drift, "")
            info = f"[{drift_style}]{state.benchmark_drift} (informational)[/]"
        table.add_row(icon_text, state.check.title, str(state.check.stage), status_text, elapsed, info)

    for chk, reason in skipped:
        table.add_row(
            Text("○", style="yellow"),
            Text(chk.title, style="dim"),
            str(chk.stage),
            Text("SKIP", style="yellow"),
            "",
            Text(reason, style="dim italic"),
        )

    return table


def _run_rich(
    states: list[CheckState],
    stages: dict[int, list[tuple[int, CheckState]]],
    skipped: list[tuple[Check, str]],
    run_log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
    mode_label: str,
    sequential: bool,
    started_at: float,
    fail_fast: bool,
) -> int:
    console = Console()
    total = len(states)
    completed = 0

    progress = Progress(
        SpinnerColumn("dots"),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )
    task_id = progress.add_task(
        f"Pre-commit ({mode_label})", total=total,
    )

    def make_display() -> Group:
        return Group(
            _build_live_table(states, skipped),
            progress,
        )

    failures: list[CheckState] = []
    try:
        with Live(make_display(), console=console, refresh_per_second=8, transient=True) as live:
            for stage_num in sorted(stages):
                def completed_check(state: CheckState) -> None:
                    nonlocal completed
                    completed += 1
                    progress.update(task_id, completed=completed)
                    live.update(make_display())

                failures.extend(_run_stage(
                    stages[stage_num], run_log_root, benchmark_baselines,
                    sequential, on_complete=completed_check,
                ))

                if failures and fail_fast:
                    break

    except KeyboardInterrupt:
        console.print("\n[red bold]Interrupted.[/]")
        return 130

    total_elapsed = format_duration(time.perf_counter() - started_at)
    console.print()

    # Final summary table
    final_table = _build_live_table(states, skipped)
    console.print(final_table)
    console.print()

    if failures:
        console.print(
            Panel(
                f"[red bold]FAILED[/] in {total_elapsed}  "
                f"({len(failures)}/{total} failed)",
                border_style="red",
                expand=False,
            )
        )
        for f in failures:
            output = tail_lines(f.log_path, max_lines=40) if f.log_path else ""
            if output:
                console.print(
                    Panel(
                        Text.from_ansi(output),
                        title=Text(f"{f.check.key} failure log", style="red"),
                        border_style="red",
                        expand=True,
                    )
                )
        console.print(Text(f"Logs: {run_log_root}", style="dim"))
        return 1

    if any(state.check.benchmark_comparison for state in states):
        console.print("Benchmark comparisons are informational; baselines change only with --update-benchmark-baselines.")
    console.print(
        Panel(
            ("[yellow bold]INCOMPLETE[/]" if skipped else "[green bold]PASSED[/]")
            + f" in {total_elapsed}  "
            f"({total} passed"
            + (f", {len(skipped)} skipped" if skipped else "")
            + ")",
            border_style="yellow" if skipped else "green",
            expand=False,
        )
    )
    console.print(Text(f"Logs: {run_log_root}", style="dim"))
    return 0


def _list_checks_rich(all_checks: list[Check]) -> int:
    console = Console()
    table = Table(title="Available Checks", show_lines=False, border_style="dim")
    table.add_column("Key", style="cyan", no_wrap=True)
    table.add_column("Stage", justify="center")
    table.add_column("Modes")
    table.add_column("Title")
    table.add_column("Flags", style="dim")

    for check in all_checks:
        modes = ",".join(check.modes)
        flags = []
        if check.cargo_bound:
            flags.append("cargo")
        required_tools = _required_tools(check)
        if required_tools:
            skip = _check_skip_reason(check)
            if skip:
                flags.append(f"[yellow]skip: {skip}[/]")
            else:
                flags.append(f"requires:{','.join(required_tools)}")
        if check.benchmark_comparison:
            flags.append("benchmark")
        table.add_row(check.key, str(check.stage), modes, check.title, ", ".join(flags))

    console.print(table)
    return 0


# ─── Plain ANSI fallback renderer ───────────────────────────────────────────


def _run_plain(
    states: list[CheckState],
    stages: dict[int, list[tuple[int, CheckState]]],
    skipped: list[tuple[Check, str]],
    run_log_root: Path,
    benchmark_baselines: dict[str, dict[str, dict[str, list[str]]]],
    mode_label: str,
    sequential: bool,
    started_at: float,
    fail_fast: bool,
) -> int:
    total = len(states)
    print(
        f"\n{BOLD}Pre-commit checks ({total} checks, {mode_label}){RESET}"
    )
    if skipped:
        for chk, reason in skipped:
            print(f"  {YELLOW}SKIP{RESET} {chk.title} ({reason})")
    print()

    failures: list[CheckState] = []
    try:
        for stage_num in sorted(stages):
            stage_failures = _run_stage(
                stages[stage_num], run_log_root,
                benchmark_baselines, sequential,
            )
            failures.extend(stage_failures)
            if failures and fail_fast:
                break
    except KeyboardInterrupt:
        print(f"\n{RED}Interrupted.{RESET}")
        return 130

    # Print status lines for each state.
    for state in states:
        duration = format_duration(state.elapsed_seconds)
        label = state.check.title
        if state.status == "passed":
            print(f"{GREEN}PASS{RESET}   {label} {DIM}({duration}){RESET}")
        elif state.status == "failed":
            print(f"{RED}FAIL{RESET}   {label} {DIM}({duration}){RESET}")

    total_elapsed = format_duration(time.perf_counter() - started_at)

    print(f"\n{'─' * 60}")
    if failures:
        print(
            f"{RED}{BOLD}FAILED{RESET} in {total_elapsed}  ({len(failures)}/{total} failed)"
        )
        for f in failures:
            log_hint = f" → {f.log_path}" if f.log_path else ""
            print(f"  {RED}✗{RESET} {f.check.key}{log_hint}")
            output = tail_lines(f.log_path, max_lines=40) if f.log_path else ""
            if output:
                print(f"\n{RED}--- {f.check.key} failed output (tail) ---{RESET}")
                print(output)
                print()
        if skipped:
            for chk, reason in skipped:
                print(f"  {YELLOW}○{RESET} {chk.key} (skipped, {reason})")
        print(f"\nLogs: {run_log_root}")
        return 1

    label = f"{YELLOW}{BOLD}INCOMPLETE" if skipped else f"{GREEN}{BOLD}PASSED"
    print(f"{label}{RESET} in {total_elapsed}")
    for state in states:
        drift = ""
        if state.benchmark_drift:
            color = {
                "REGRESSED": RED,
                "IMPROVED": GREEN,
                "STABLE": "",
                "BASELINE": YELLOW,
            }.get(state.benchmark_drift, "")
            drift = f"  [{color}{state.benchmark_drift}{RESET}; informational]"
        print(
            f"  {GREEN}✓{RESET} {state.check.key} {format_duration(state.elapsed_seconds)}{drift}"
        )
    if skipped:
        for chk, reason in skipped:
            print(f"  {YELLOW}○{RESET} {chk.key} (skipped, {reason})")
    if any(state.check.benchmark_comparison for state in states):
        print("Benchmark comparisons are informational; baselines change only with --update-benchmark-baselines.")
    print(f"\nLogs: {run_log_root}")
    return 0


def _list_checks_plain(all_checks: list[Check]) -> int:
    prev_stage = -1
    for check in all_checks:
        if check.stage != prev_stage:
            if prev_stage >= 0:
                print()
            print(f"  {BOLD}Stage {check.stage}{RESET}")
            prev_stage = check.stage
        modes = ",".join(check.modes)
        flags = []
        if check.cargo_bound:
            flags.append("cargo")
        required_tools = _required_tools(check)
        if required_tools:
            flags.append(f"requires:{','.join(required_tools)}")
        if check.benchmark_comparison:
            flags.append("benchmark")
        suffix = f"  [{', '.join(flags)}]" if flags else ""
        print(f"    {check.key:24} {modes:14} {check.title}{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
