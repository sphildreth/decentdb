#!/usr/bin/env python3
"""Generate alternative README benchmark visualizations.

Inputs:
  - data/bench_summary.json

Outputs:
  - assets/decentdb-radar.png
  - assets/decentdb-speedup.png
"""

import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "bench_summary.json"
OUT_RADAR = ROOT / "assets" / "decentdb-radar.png"
OUT_SPEEDUP = ROOT / "assets" / "decentdb-speedup.png"

BASELINE_ENGINE = "sqlite"

METRICS = [
    ("read_p95_ms", "Read Latency", "lower"),
    ("join_p95_ms", "Join Latency", "lower"),
    ("commit_p95_ms", "Commit Latency", "lower"),
    ("insert_rows_per_sec", "Insert Throughput", "higher"),
]

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


def load() -> dict[str, dict[str, object]]:
    if not DATA.exists():
        raise SystemExit(f"Benchmark summary not found: {DATA}")

    with DATA.open("r", encoding="utf-8") as handle:
        doc = json.load(handle)

    return doc["engines"]


def available_metrics(engines: dict[str, dict[str, object]]) -> list[tuple[str, str, str]]:
    return [
        (key, label, direction)
        for key, label, direction in METRICS
        if any(to_float(values.get(key)) is not None for values in engines.values())
    ]


def normalize_radar(engines: dict[str, dict[str, object]]) -> tuple[list[tuple[str, str, str]], dict[str, list[float]]]:
    metrics = available_metrics(engines)
    if not metrics:
        raise SystemExit("No plottable metrics found for radar chart.")

    normalized: dict[str, list[float]] = {}
    for engine, values in engines.items():
        display_name = display_engine_name(engine)
        normalized[display_name] = []
        for key, _, direction in metrics:
            candidates = [
                to_float(candidate_values.get(key))
                for candidate_values in engines.values()
            ]
            finite = [candidate for candidate in candidates if candidate is not None]
            if not finite:
                normalized[display_name].append(math.nan)
                continue

            value = to_float(values.get(key))
            if value is None:
                normalized[display_name].append(math.nan)
                continue

            if direction == "lower":
                best_value = min(finite)
                normalized[display_name].append(best_value / value if value > 0 else math.nan)
            else:
                best_value = max(finite)
                normalized[display_name].append(value / best_value if best_value > 0 else math.nan)

    return metrics, normalized


def plot_radar(engines: dict[str, dict[str, object]]) -> None:
    metrics, normalized = normalize_radar(engines)
    categories = [label for _, label, _ in metrics]
    count = len(categories)
    angles = [index / float(count) * 2 * math.pi for index in range(count)]
    angles += angles[:1]

    fig = plt.figure(figsize=(10, 10))
    ax = plt.subplot(111, polar=True)

    plt.xticks(angles[:-1], categories, color="grey", size=10)
    ax.set_rlabel_position(0)
    plt.yticks(
        [0.25, 0.5, 0.75, 1.0],
        ["0.25", "0.50", "0.75", "1.00"],
        color="grey",
        size=7,
    )
    plt.ylim(0, 1.1)

    colors = plt.rcParams["axes.prop_cycle"].by_key().get(
        "color",
        ["C0", "C1", "C2", "C3", "C4", "C5", "C6"],
    )

    for index, engine in enumerate(ordered_display_engines(list(normalized.keys()))):
        values = [0.0 if math.isnan(value) else value for value in normalized[engine]]
        values += values[:1]
        color = colors[index % len(colors)]
        ax.plot(angles, values, linewidth=2, linestyle="solid", label=engine, color=color)
        ax.fill(angles, values, color, alpha=0.1)

    plt.title("Overall Performance (Outer is Better)", size=16, y=1.1)
    plt.legend(loc="upper right", bbox_to_anchor=(0.1, 1.1))

    OUT_RADAR.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_RADAR, dpi=150)
    plt.close(fig)
    print(f"Wrote: {OUT_RADAR}")


def normalize_speedup(engines: dict[str, dict[str, object]]) -> tuple[list[tuple[str, str, str]], dict[str, list[float]]]:
    if BASELINE_ENGINE not in engines:
        raise SystemExit(f"Baseline engine '{BASELINE_ENGINE}' not found in data.")

    metrics = available_metrics(engines)
    if not metrics:
        raise SystemExit("No plottable metrics found for speedup chart.")

    baseline = engines[BASELINE_ENGINE]
    normalized: dict[str, list[float]] = {}
    for engine, values in engines.items():
        display_name = display_engine_name(engine)
        normalized[display_name] = []
        for key, _, direction in metrics:
            base_value = to_float(baseline.get(key))
            value = to_float(values.get(key))
            if base_value is None or value is None:
                normalized[display_name].append(math.nan)
            elif direction == "lower":
                normalized[display_name].append(base_value / value if value > 0 else math.nan)
            else:
                normalized[display_name].append(value / base_value if base_value > 0 else math.nan)

    return metrics, normalized


def plot_speedup(engines: dict[str, dict[str, object]]) -> None:
    metrics, normalized = normalize_speedup(engines)
    labels = [label for _, label, _ in metrics]
    engine_names = ordered_display_engines(list(normalized.keys()))
    positions = np.arange(len(labels), dtype=float)
    height = 0.8 / max(len(engine_names), 1)

    fig, ax = plt.subplots(figsize=(12, 6))

    for index, engine in enumerate(engine_names):
        values = np.array(normalized[engine], dtype=float)
        y_pos = positions + (len(engine_names) - 1 - index) * height
        ax.barh(y_pos, values, height, label=engine)
        for metric_index, value in enumerate(values):
            if not math.isnan(float(value)):
                ax.text(float(value) + 0.05, y_pos[metric_index], f"{float(value):.2f}x", va="center", size=9)

    baseline_label = display_engine_name(BASELINE_ENGINE)
    ax.axvline(
        1.0,
        color="k",
        linestyle="--",
        linewidth=1,
        label=f"{baseline_label} Baseline (1.0x)",
    )

    ax.set_yticks(positions + height * (len(engine_names) - 1) / 2)
    ax.set_yticklabels(labels)
    ax.set_xlabel(f"Speedup / Efficiency vs {baseline_label} (Higher is Better)")
    ax.set_title(f"Relative Performance vs {baseline_label}")
    ax.legend()

    plt.tight_layout()
    plt.savefig(OUT_SPEEDUP, dpi=150)
    plt.close(fig)
    print(f"Wrote: {OUT_SPEEDUP}")


def main() -> None:
    engines = load()
    plot_radar(engines)
    plot_speedup(engines)


if __name__ == "__main__":
    main()
