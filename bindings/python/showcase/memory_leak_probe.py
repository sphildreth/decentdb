#!/usr/bin/env python3
"""
DecentDB Python + engine memory leak probe.

This script runs two complementary checks:
1. Fresh-process engine check: repeatedly opens the same template DB/WAL in new
   subprocesses and verifies baseline RSS does not trend upward by run index.
2. In-process binding check: repeatedly connect/query/close in one process and
   verifies RSS drift/slope stay within thresholds after warmup.

Usage:
    python memory_leak_probe.py
    python memory_leak_probe.py --rows 10000 --payload-bytes 4096 --engine-runs 30
    python memory_leak_probe.py --binding-iterations 400 --binding-drift-mb-threshold 96
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import decentdb


def rss_kb(pid: int | None = None) -> int:
    status_path = Path(f"/proc/{pid or os.getpid()}/status")
    for line in status_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            if len(parts) >= 2:
                return int(parts[1])
    return 0


def vmhwm_kb(pid: int | None = None) -> int:
    status_path = Path(f"/proc/{pid or os.getpid()}/status")
    for line in status_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("VmHWM:"):
            parts = line.split()
            if len(parts) >= 2:
                return int(parts[1])
    return 0


def db_artifact_paths(db_path: Path) -> list[Path]:
    return [
        db_path,
        Path(f"{db_path}.wal"),
        Path(f"{db_path}.shm"),
        Path(f"{db_path}-wal"),
        Path(f"{db_path}-shm"),
    ]


def remove_db_artifacts(db_path: Path) -> None:
    for path in db_artifact_paths(db_path):
        if path.exists():
            path.unlink()


def copy_db_artifacts(src_db: Path, dst_db: Path) -> None:
    remove_db_artifacts(dst_db)
    for src in db_artifact_paths(src_db):
        if src.exists():
            suffix = str(src).removeprefix(str(src_db))
            dst = Path(f"{dst_db}{suffix}")
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


def collect_sizes(db_path: Path) -> dict[str, int]:
    out: dict[str, int] = {}
    for path in db_artifact_paths(db_path):
        if path.exists():
            out[str(path)] = path.stat().st_size
    return out


def payload_from_u64(value: int, payload_bytes: int) -> bytes:
    seed = (value & 0xFFFFFFFFFFFFFFFF).to_bytes(8, byteorder="little", signed=False)
    return hashlib.shake_256(seed).digest(payload_bytes)


def linear_slope(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    xs = list(range(len(values)))
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(values)
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom == 0:
        return 0.0
    numer = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    return numer / denom


def build_template(
    db_path: Path,
    rows: int,
    payload_bytes: int,
    wal_update_passes: int,
    batch_size: int,
) -> dict[str, Any]:
    remove_db_artifacts(db_path)
    conn = decentdb.connect(str(db_path), mode="create")
    cur = conn.cursor()
    cur.execute("CREATE TABLE memprobe (id INT64 PRIMARY KEY, payload BLOB NOT NULL)")

    cur.execute("BEGIN")
    for idx in range(1, rows + 1):
        cur.execute(
            "INSERT INTO memprobe (id, payload) VALUES ($1, $2)",
            (idx, payload_from_u64(idx, payload_bytes)),
        )
        if idx % batch_size == 0:
            cur.execute("COMMIT")
            cur.execute("BEGIN")
    conn.commit()

    for pass_idx in range(1, wal_update_passes + 1):
        cur.execute("BEGIN")
        salt = 0x9E3779B97F4A7C15 * pass_idx
        for idx in range(1, rows + 1):
            cur.execute(
                "UPDATE memprobe SET payload = $1 WHERE id = $2",
                (payload_from_u64(idx ^ salt, payload_bytes), idx),
            )
            if idx % batch_size == 0:
                cur.execute("COMMIT")
                cur.execute("BEGIN")
        conn.commit()

    state = conn.inspect_storage_state()
    conn.close()
    return {"state": state, "sizes": collect_sizes(db_path)}


def build_template_and_crash(
    db_path: Path,
    rows: int,
    payload_bytes: int,
    wal_update_passes: int,
    batch_size: int,
) -> None:
    remove_db_artifacts(db_path)
    conn = decentdb.connect(str(db_path), mode="create")
    cur = conn.cursor()
    cur.execute("CREATE TABLE memprobe (id INT64 PRIMARY KEY, payload BLOB NOT NULL)")

    cur.execute("BEGIN")
    for idx in range(1, rows + 1):
        cur.execute(
            "INSERT INTO memprobe (id, payload) VALUES ($1, $2)",
            (idx, payload_from_u64(idx, payload_bytes)),
        )
        if idx % batch_size == 0:
            cur.execute("COMMIT")
            cur.execute("BEGIN")
    conn.commit()

    for pass_idx in range(1, wal_update_passes + 1):
        cur.execute("BEGIN")
        salt = 0x9E3779B97F4A7C15 * pass_idx
        for idx in range(1, rows + 1):
            cur.execute(
                "UPDATE memprobe SET payload = $1 WHERE id = $2",
                (payload_from_u64(idx ^ salt, payload_bytes), idx),
            )
            if idx % batch_size == 0:
                cur.execute("COMMIT")
                cur.execute("BEGIN")
        conn.commit()

    payload = {
        "mode": "writer_crash",
        "db_path": str(db_path),
        "state_before_exit": conn.inspect_storage_state(),
        "artifact_sizes_before_exit": collect_sizes(db_path),
    }
    print(json.dumps(payload), flush=True)
    # Preserve WAL state by bypassing close/finalizers.
    os._exit(0)


def reader_once(db_path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "mode": "reader_once",
        "db_path": str(db_path),
        "rss_before_open_kb": rss_kb(),
    }
    open_started = time.perf_counter()
    conn = decentdb.connect(str(db_path), mode="open_existing", stmt_cache_size=32)
    result["open_ms"] = round((time.perf_counter() - open_started) * 1000.0, 2)
    result["rss_after_open_kb"] = rss_kb()
    result["storage_state_after_open"] = conn.inspect_storage_state()
    query_started = time.perf_counter()
    count = conn.execute("SELECT COUNT(*) FROM memprobe").fetchone()[0]
    result["query_ms"] = round((time.perf_counter() - query_started) * 1000.0, 2)
    result["row_count"] = int(count)
    result["rss_after_query_kb"] = rss_kb()
    conn.close()
    result["rss_after_close_kb"] = rss_kb()
    result["vmhwm_kb"] = vmhwm_kb()
    result["artifact_sizes_after"] = collect_sizes(db_path)
    return result


def run_reader_subprocess(script_path: Path, db_path: Path) -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, str(script_path), "--mode", "reader-once", "--db-path", str(db_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"reader subprocess failed rc={proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    payload = None
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        maybe = json.loads(line)
        if maybe.get("mode") == "reader_once":
            payload = maybe
    if payload is None:
        raise RuntimeError(f"reader payload missing\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
    return payload


def run_writer_subprocess(
    script_path: Path,
    db_path: Path,
    rows: int,
    payload_bytes: int,
    wal_update_passes: int,
    batch_size: int,
) -> dict[str, Any]:
    proc = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--mode",
            "writer-crash",
            "--db-path",
            str(db_path),
            "--rows",
            str(rows),
            "--payload-bytes",
            str(payload_bytes),
            "--wal-update-passes",
            str(wal_update_passes),
            "--batch-size",
            str(batch_size),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"writer subprocess failed rc={proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    payload = None
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        maybe = json.loads(line)
        if maybe.get("mode") == "writer_crash":
            payload = maybe
    if payload is None:
        raise RuntimeError(f"writer payload missing\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
    return payload


def summarize_numeric(values: list[float]) -> dict[str, float]:
    return {
        "min": min(values),
        "median": statistics.median(values),
        "mean": statistics.mean(values),
        "max": max(values),
    }


def run_engine_fresh_process_check(
    script_path: Path,
    template_db: Path,
    work_dir: Path,
    engine_runs: int,
    keep_artifacts: bool,
) -> dict[str, Any]:
    runs: list[dict[str, Any]] = []
    for idx in range(1, engine_runs + 1):
        run_db = work_dir / f"engine_run_{idx}.ddb"
        copy_db_artifacts(template_db, run_db)
        payload = run_reader_subprocess(script_path, run_db)
        payload["run_index"] = idx
        runs.append(payload)
        if not keep_artifacts:
            remove_db_artifacts(run_db)

    before_open = [float(run["rss_before_open_kb"]) for run in runs]
    after_open = [float(run["rss_after_open_kb"]) for run in runs]
    peak = [float(run["vmhwm_kb"]) for run in runs]
    before_spread_mb = (max(before_open) - min(before_open)) / 1024.0
    before_slope_kb_per_run = linear_slope(before_open)
    summary = {
        "runs": runs,
        "rss_before_open_kb": summarize_numeric(before_open),
        "rss_after_open_kb": summarize_numeric(after_open),
        "peak_kb": summarize_numeric(peak),
        "before_open_spread_mb": before_spread_mb,
        "before_open_slope_kb_per_run": before_slope_kb_per_run,
    }
    return summary


def run_binding_inprocess_check(
    clean_db: Path,
    binding_iterations: int,
    binding_warmup: int,
) -> dict[str, Any]:
    rss_series: list[int] = []
    open_ms_series: list[float] = []
    query_ms_series: list[float] = []
    for _ in range(binding_iterations):
        open_started = time.perf_counter()
        conn = decentdb.connect(str(clean_db), mode="open_existing", stmt_cache_size=32)
        open_ms_series.append((time.perf_counter() - open_started) * 1000.0)
        query_started = time.perf_counter()
        conn.execute("SELECT COUNT(*) FROM memprobe").fetchone()[0]
        query_ms_series.append((time.perf_counter() - query_started) * 1000.0)
        conn.close()
        gc.collect()
        rss_series.append(rss_kb())

    tail = rss_series[binding_warmup:] if binding_warmup < len(rss_series) else rss_series
    slope = linear_slope([float(v) for v in tail])
    drift_mb = ((tail[-1] - tail[0]) / 1024.0) if len(tail) >= 2 else 0.0
    return {
        "iterations": binding_iterations,
        "warmup": binding_warmup,
        "rss_series_kb": rss_series,
        "open_ms": summarize_numeric(open_ms_series),
        "query_ms": summarize_numeric(query_ms_series),
        "tail_slope_kb_per_iter": slope,
        "tail_drift_mb": drift_mb,
        "tail_min_mb": min(tail) / 1024.0,
        "tail_max_mb": max(tail) / 1024.0,
    }


def print_verdict(title: str, passed: bool, details: str) -> None:
    status = "PASS" if passed else "FAIL"
    print(f"{title}: {status} - {details}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["probe", "reader-once", "writer-crash"], default="probe")
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument("--rows", type=int, default=8000)
    parser.add_argument("--payload-bytes", type=int, default=2048)
    parser.add_argument("--wal-update-passes", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--no-preserve-wal", action="store_true")
    parser.add_argument("--engine-runs", type=int, default=30)
    parser.add_argument("--engine-before-open-slope-kb-threshold", type=float, default=128.0)
    parser.add_argument("--engine-before-open-spread-mb-threshold", type=float, default=24.0)
    parser.add_argument("--binding-iterations", type=int, default=250)
    parser.add_argument("--binding-warmup", type=int, default=20)
    parser.add_argument("--binding-slope-kb-threshold", type=float, default=128.0)
    parser.add_argument("--binding-drift-mb-threshold", type=float, default=64.0)
    parser.add_argument("--include-details", action="store_true")
    parser.add_argument("--json-out", type=Path)
    args = parser.parse_args()

    if args.mode == "reader-once":
        if args.db_path is None:
            parser.error("--db-path is required for --mode reader-once")
        print(json.dumps(reader_once(args.db_path)))
        return 0

    if args.mode == "writer-crash":
        if args.db_path is None:
            parser.error("--db-path is required for --mode writer-crash")
        build_template_and_crash(
            db_path=args.db_path,
            rows=args.rows,
            payload_bytes=args.payload_bytes,
            wal_update_passes=args.wal_update_passes,
            batch_size=args.batch_size,
        )
        return 0

    script_path = Path(__file__).resolve()
    work_dir = (args.work_dir or Path(tempfile.mkdtemp(prefix="decentdb-leak-probe-"))).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    template_db = work_dir / "template.ddb"
    clean_db = work_dir / "clean.ddb"

    started = time.perf_counter()
    print(
        f"Leak probe start rows={args.rows:,} payload_bytes={args.payload_bytes:,} "
        f"wal_update_passes={args.wal_update_passes} engine_runs={args.engine_runs} "
        f"binding_iterations={args.binding_iterations}"
    )

    if args.no_preserve_wal:
        template = build_template(
            template_db,
            rows=args.rows,
            payload_bytes=args.payload_bytes,
            wal_update_passes=args.wal_update_passes,
            batch_size=args.batch_size,
        )
    else:
        writer = run_writer_subprocess(
            script_path=script_path,
            db_path=template_db,
            rows=args.rows,
            payload_bytes=args.payload_bytes,
            wal_update_passes=args.wal_update_passes,
            batch_size=args.batch_size,
        )
        template = {
            "state": writer["state_before_exit"],
            "sizes": writer["artifact_sizes_before_exit"],
        }
    copy_db_artifacts(template_db, clean_db)
    clean_conn = decentdb.connect(str(clean_db), mode="open_existing", stmt_cache_size=16)
    clean_conn.checkpoint()
    clean_state = clean_conn.inspect_storage_state()
    clean_conn.close()

    engine = run_engine_fresh_process_check(
        script_path=script_path,
        template_db=template_db,
        work_dir=work_dir,
        engine_runs=args.engine_runs,
        keep_artifacts=args.keep_artifacts,
    )
    binding = run_binding_inprocess_check(
        clean_db=clean_db,
        binding_iterations=args.binding_iterations,
        binding_warmup=args.binding_warmup,
    )

    engine_pass = (
        abs(engine["before_open_slope_kb_per_run"]) <= args.engine_before_open_slope_kb_threshold
        and engine["before_open_spread_mb"] <= args.engine_before_open_spread_mb_threshold
    )
    binding_pass = (
        abs(binding["tail_slope_kb_per_iter"]) <= args.binding_slope_kb_threshold
        and binding["tail_drift_mb"] <= args.binding_drift_mb_threshold
    )

    print_verdict(
        "Engine fresh-process leak check",
        engine_pass,
        (
            f"slope={engine['before_open_slope_kb_per_run']:.2f} KB/run "
            f"(threshold {args.engine_before_open_slope_kb_threshold:.2f}), "
            f"spread={engine['before_open_spread_mb']:.2f} MB "
            f"(threshold {args.engine_before_open_spread_mb_threshold:.2f})"
        ),
    )
    print_verdict(
        "Python binding in-process leak check",
        binding_pass,
        (
            f"tail_slope={binding['tail_slope_kb_per_iter']:.2f} KB/iter "
            f"(threshold {args.binding_slope_kb_threshold:.2f}), "
            f"tail_drift={binding['tail_drift_mb']:.2f} MB "
            f"(threshold {args.binding_drift_mb_threshold:.2f})"
        ),
    )

    engine_output = engine if args.include_details else {k: v for k, v in engine.items() if k != "runs"}
    binding_output = (
        binding if args.include_details else {k: v for k, v in binding.items() if k != "rss_series_kb"}
    )
    summary = {
        "mode": "probe",
        "template": template,
        "clean_state": clean_state,
        "engine_check": engine_output,
        "binding_check": binding_output,
        "thresholds": {
            "engine_before_open_slope_kb_threshold": args.engine_before_open_slope_kb_threshold,
            "engine_before_open_spread_mb_threshold": args.engine_before_open_spread_mb_threshold,
            "binding_slope_kb_threshold": args.binding_slope_kb_threshold,
            "binding_drift_mb_threshold": args.binding_drift_mb_threshold,
        },
        "verdict": {
            "engine_pass": engine_pass,
            "binding_pass": binding_pass,
            "overall_pass": engine_pass and binding_pass,
        },
        "elapsed_s": round(time.perf_counter() - started, 3),
    }
    print(json.dumps(summary, indent=2))

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if not args.keep_artifacts:
        remove_db_artifacts(template_db)
        remove_db_artifacts(clean_db)
    if summary["verdict"]["overall_pass"]:
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
