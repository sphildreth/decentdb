#!/usr/bin/env python3
"""Generate README-friendly benchmark bar chart(s).

Inputs:
  - data/bench_summary.json

Outputs:
  - assets/decentdb-benchmarks.svg
  - assets/decentdb-benchmarks.png
"""

import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "bench_summary.json"
OUT_SVG = ROOT / "assets" / "decentdb-benchmarks.svg"
OUT_PNG = ROOT / "assets" / "decentdb-benchmarks.png"

METRICS = [
    ("read_p95_ms", "Point read p95", "lower"),
    ("join_p95_ms", "Join p95", "lower"),
    ("commit_p95_ms", "Commit p95", "lower"),
    ("insert_rows_per_sec", "Insert (rows/s)", "higher"),
]

BASELINE_ENGINE = "sqlite"

ENGINE_LABELS = {
    "decentdb": "DecentDB",
    "duckdb": "DuckDB",
    "sqlite": "SQLite",
}


def display_engine_name(engine: str) -> str:
    return ENGINE_LABELS.get(engine, engine)


def ordered_display_engines(engines: list[str]) -> list[str]:
    engine_set = set(engines)
    preferred = [
        display_engine_name("decentdb"),
        display_engine_name(BASELINE_ENGINE),
        display_engine_name("duckdb"),
        "H2",
        "LiteDB",
        "Apache Derby",
        "HSQLDB",
        "Firebird",
    ]
    ordered = [engine for engine in preferred if engine in engine_set]
    ordered.extend(engine for engine in engines if engine not in ordered)
    return ordered


def to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load() -> tuple[list[dict[str, object]], dict[str, object]]:
    with DATA.open("r", encoding="utf-8") as handle:
        doc = json.load(handle)

    rows = []
    for engine, metrics in doc["engines"].items():
        row = {"engine": engine}
        row.update(metrics)
        rows.append(row)
    return rows, doc.get("metadata", {})


def normalize(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    baseline = next((row for row in rows if row["engine"] == BASELINE_ENGINE), None)
    if baseline is None:
        raise SystemExit(f"Baseline engine '{BASELINE_ENGINE}' not found in data.")

    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized_row: dict[str, object] = {"engine": row["engine"]}
        for key, _, direction in METRICS:
            base_value = to_float(baseline.get(key))
            value = to_float(row.get(key))
            if base_value is None or value is None:
                normalized_row[key] = None
            elif direction == "higher":
                normalized_row[key] = value / base_value if base_value != 0 else None
            else:
                normalized_row[key] = base_value / value if value != 0 else None
        normalized.append(normalized_row)
    return normalized


def plot(rows: list[dict[str, object]], meta: dict[str, object]) -> None:
    if not rows:
        raise SystemExit("No benchmark data found.")

    available_metrics = [
        (key, label)
        for key, label, _ in METRICS
        if any(row.get(key) is not None for row in rows)
    ]
    if not available_metrics:
        raise SystemExit("No plottable data. Did you provide metrics?")

    rows_by_display = {
        display_engine_name(str(row["engine"])): row
        for row in rows
    }
    engines = ordered_display_engines(list(rows_by_display.keys()))

    positions = np.arange(len(available_metrics), dtype=float)
    width = 0.8 / max(len(engines), 1)
    offsets = (np.arange(len(engines), dtype=float) - (len(engines) - 1) / 2.0) * width

    fig, ax = plt.subplots(figsize=(12, 5))

    for index, engine in enumerate(engines):
        row = rows_by_display[engine]
        values = [
            to_float(row.get(key)) if row.get(key) is not None else math.nan
            for key, _ in available_metrics
        ]
        ax.bar(positions + offsets[index], values, width=width, label=engine)

    ax.set_ylabel(
        f"Normalized score vs {display_engine_name(BASELINE_ENGINE)} (higher is better)"
    )
    ax.set_xlabel("")
    ax.set_title("DecentDB vs common embedded engines (normalized)")
    ax.set_xticks(positions)
    ax.set_xticklabels([label for _, label in available_metrics])
    ax.axhline(1.0, linewidth=1)
    ax.legend(title="Engine")

    plt.tight_layout()
    OUT_SVG.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_SVG, format="svg")
    plt.savefig(OUT_PNG, format="png", dpi=180)
    plt.close(fig)

    print(f"Wrote: {OUT_SVG}")
    print(f"Wrote: {OUT_PNG}")
    if meta:
        print("Metadata:", meta)


def main() -> None:
    rows, meta = load()
    plot(normalize(rows), meta)


if __name__ == "__main__":
    main()
