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
    ("read_p95_ms", "Point read p95 (ms)", "lower"),
    ("join_p95_ms", "Join p95 (ms)", "lower"),
    ("range_scan_p95_ms", "Range scan p95 (ms)", "lower"),
    ("aggregate_p95_ms", "Aggregate p95 (ms)", "lower"),
    ("concurrent_read_p95_ms", "Concurrent read p95 (ms)", "lower"),
    ("commit_p95_ms", "Commit p95 (ms)", "lower"),
    ("insert_rows_per_sec", "Insert throughput (rows/s)", "higher"),
]

ENGINE_LABELS = {
    "decentdb_default_durable": "DecentDB (default durable)",
    "decentdb_tuned_durable": "DecentDB (tuned durable)",
    "duckdb": "DuckDB",
    "sqlite": "SQLite",
}


def display_engine_name(engine: str) -> str:
    return ENGINE_LABELS.get(engine, engine)


def ordered_display_engines(engines: list[str]) -> list[str]:
    engine_set = set(engines)
    preferred = [
        display_engine_name("decentdb_tuned_durable"),
        display_engine_name("decentdb_default_durable"),
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

    metric_index = {key: idx for idx, (key, _, _) in enumerate(metrics)}
    normalized: dict[str, list[float]] = {}
    for engine, values in engines.items():
        display_name = display_engine_name(engine)
        normalized[display_name] = [math.nan for _ in metrics]
        for key, _, direction in metrics:
            candidates = [
                to_float(candidate_values.get(key))
                for candidate_values in engines.values()
            ]
            finite = [
                candidate for candidate in candidates if candidate is not None and candidate > 0.0
            ]
            if not finite:
                continue

            value = to_float(values.get(key))
            if value is None:
                continue
            if value <= 0.0:
                continue

            idx = metric_index[key]

            if direction == "lower":
                best_value = min(finite)
                normalized[display_name][idx] = best_value / value
            else:
                best_value = max(finite)
                normalized[display_name][idx] = value / best_value

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

    engines_for_radar = ordered_display_engines(list(normalized.keys()))
    complete_engines = [
        engine
        for engine in engines_for_radar
        if engine in normalized and all(not math.isnan(value) for value in normalized[engine])
    ]
    incomplete_engines = [
        engine for engine in engines_for_radar if engine in normalized and engine not in complete_engines
    ]
    if incomplete_engines:
        print(
            "Skipping these engines from radar due incomplete metric coverage: "
            + ", ".join(incomplete_engines)
        )

    for index, engine in enumerate(complete_engines):
        values = normalized[engine]
        wrapped = np.array(values + values[:1], dtype=float)
        color = colors[index % len(colors)]
        ax.plot(
            angles,
            wrapped,
            linewidth=2,
            linestyle="solid",
            label=engine,
            color=color,
        )
        if not np.all(np.isnan(wrapped)):
            fill = np.ma.array(wrapped, mask=np.isnan(wrapped))
            ax.fill(angles, fill, color, alpha=0.1)

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
        first_bar = True
        for metric_index, value in enumerate(values):
            if math.isnan(float(value)):
                ax.text(0.02, y_pos[metric_index], "n/a", va="center", size=9, color="gray")
            else:
                ax.barh(
                    y_pos[metric_index],
                    float(value),
                    height,
                    label=engine if first_bar else None,
                )
                first_bar = False
                ax.text(
                    float(value) + 0.05,
                    y_pos[metric_index],
                    f"{float(value):.2f}x",
                    va="center",
                    size=9,
                )

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
    ax.set_xlim(left=0)
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
