"""Chart export helpers for benchmark result bundles."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from utils.manifest import ResultsBundle


def _flatten_bundle(bundle: ResultsBundle) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for result in bundle.results:
        rows.append(
            {
                "engine": result.engine,
                "benchmark": result.benchmark,
                "operations": result.operations,
                "mean_latency_us": result.latency_ms.get("mean_ms", 0.0) * 1000.0,
                "p50_latency_us": result.latency_ms.get("p50_ms", 0.0) * 1000.0,
                "p95_latency_us": result.latency_ms.get("p95_ms", 0.0) * 1000.0,
                "throughput_ops_sec": result.throughput_ops_sec,
                "scenario": bundle.manifest.scenario_name,
                "workload": bundle.manifest.workload_name,
                "transaction_mode": bundle.manifest.transaction_mode,
                "durability_mode": bundle.manifest.durability_mode,
            }
        )
    return rows


def _write_chart_data(rows: List[Dict[str, object]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    chart_data_path = output_dir / "chart_data.json"
    chart_data_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def _style_axes(axis) -> None:
    axis.set_facecolor("#ffffff")
    axis.grid(True, axis="y", color="#e2e8f0", linewidth=1.0)
    axis.grid(False, axis="x")
    axis.tick_params(axis="x", colors="#475569")
    axis.tick_params(axis="y", colors="#475569")
    for spine in axis.spines.values():
        spine.set_color("#cbd5e1")


def _get_palette() -> List[str]:
    return [
        "#0f766e",
        "#dc2626",
        "#2563eb",
        "#ca8a04",
        "#9333ea",
        "#059669",
        "#db2777",
        "#0891b2",
    ]


def _save_figure(figure, output_dir: Path, base_name: str) -> List[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    svg_path = output_dir / f"{base_name}.svg"
    png_path = output_dir / f"{base_name}.png"
    figure.savefig(svg_path, format="svg", dpi=200)
    figure.savefig(png_path, format="png", dpi=200)
    plt.close(figure)
    return [svg_path, png_path]


def _plot_benchmark_line(rows: List[Dict[str, object]], benchmark: str, output_dir: Path) -> List[Path]:
    benchmark_rows = [row for row in rows if row["benchmark"] == benchmark]
    if not benchmark_rows:
        return []

    engines = sorted({str(row["engine"]) for row in benchmark_rows})
    op_counts = sorted({int(row["operations"]) for row in benchmark_rows})
    palette = _get_palette()

    plt.style.use("default")
    figure, axis = plt.subplots(figsize=(9.5, 5.5), constrained_layout=True)
    figure.patch.set_facecolor("#f8fafc")
    _style_axes(axis)

    for index, engine in enumerate(engines):
        engine_rows = [row for row in benchmark_rows if row["engine"] == engine]
        engine_rows.sort(key=lambda row: int(row["operations"]))
        xs = [int(row["operations"]) for row in engine_rows]
        ys = [float(row["mean_latency_us"]) for row in engine_rows]
        color = palette[index % len(palette)]
        axis.plot(
            xs,
            ys,
            marker="o",
            markersize=6,
            linewidth=2.4,
            color=color,
            label=engine,
        )

    axis.set_title(
        f"{benchmark.replace('_', ' ').title()} Latency",
        fontsize=15,
        color="#0f172a",
        pad=16,
    )
    axis.set_xlabel("Operation Count", fontsize=11, color="#334155")
    axis.set_ylabel("Mean Latency ($\\mu s/op$)", fontsize=11, color="#334155")
    axis.set_xticks(op_counts)
    axis.set_xticklabels([f"{count:,}" for count in op_counts], color="#475569")
    legend = axis.legend(frameon=False, ncol=2, loc="upper left")
    for text in legend.get_texts():
        text.set_color("#0f172a")

    return _save_figure(figure, output_dir, f"{benchmark}-latency")


def _plot_benchmark_bar(rows: List[Dict[str, object]], benchmark: str, output_dir: Path) -> List[Path]:
    benchmark_rows = [row for row in rows if row["benchmark"] == benchmark]
    if not benchmark_rows:
        return []

    benchmark_rows.sort(key=lambda row: float(row["mean_latency_us"]))
    labels = [str(row["engine"]) for row in benchmark_rows]
    values = [float(row["mean_latency_us"]) for row in benchmark_rows]
    colors = [_get_palette()[index % len(_get_palette())] for index in range(len(labels))]
    op_count = int(benchmark_rows[0]["operations"])

    plt.style.use("default")
    figure, axis = plt.subplots(figsize=(10.5, 5.8), constrained_layout=True)
    figure.patch.set_facecolor("#f8fafc")
    _style_axes(axis)

    bars = axis.bar(labels, values, color=colors, width=0.66)
    axis.set_title(
        f"{benchmark.replace('_', ' ').title()} Latency Comparison",
        fontsize=15,
        color="#0f172a",
        pad=16,
    )
    axis.set_xlabel(f"Engine at {op_count:,} operations", fontsize=11, color="#334155")
    axis.set_ylabel("Mean Latency ($\\mu s/op$)", fontsize=11, color="#334155")
    axis.tick_params(axis="x", rotation=25)

    for bar, value in zip(bars, values):
        axis.text(
            bar.get_x() + bar.get_width() / 2,
            value,
            f"{value:.1f}",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#334155",
        )

    return _save_figure(figure, output_dir, f"{benchmark}-latency")


def _plot_overview(rows: List[Dict[str, object]], output_dir: Path) -> List[Path]:
    benchmarks = sorted({str(row["benchmark"]) for row in rows})
    op_counts = sorted({int(row["operations"]) for row in rows})
    palette = _get_palette()

    plt.style.use("default")
    figure, axes = plt.subplots(2, 3, figsize=(14, 8.5), constrained_layout=True)
    figure.patch.set_facecolor("#f8fafc")
    axes_list = list(axes.flat)

    if len(op_counts) > 1:
        engines = sorted({str(row["engine"]) for row in rows})
        color_map = {engine: palette[index % len(palette)] for index, engine in enumerate(engines)}
        for axis, benchmark in zip(axes_list, benchmarks):
            _style_axes(axis)
            benchmark_rows = [row for row in rows if row["benchmark"] == benchmark]
            for engine in engines:
                engine_rows = [row for row in benchmark_rows if row["engine"] == engine]
                if not engine_rows:
                    continue
                engine_rows.sort(key=lambda row: int(row["operations"]))
                axis.plot(
                    [int(row["operations"]) for row in engine_rows],
                    [float(row["mean_latency_us"]) for row in engine_rows],
                    marker="o",
                    markersize=5,
                    linewidth=2.0,
                    color=color_map[engine],
                    label=engine,
                )
            axis.set_title(benchmark.replace("_", " ").title(), fontsize=12, color="#0f172a")
            axis.set_xticks(op_counts)
            axis.set_xticklabels([f"{count:,}" for count in op_counts])
            axis.set_xlabel("Ops", fontsize=10, color="#334155")
            axis.set_ylabel("$\\mu s/op$", fontsize=10, color="#334155")
        legend = axes_list[0].legend(frameon=False, loc="upper left", ncol=2)
        for text in legend.get_texts():
            text.set_color("#0f172a")
        title = "Embedded Engine Latency Sweep"
        base_name = "latency-overview"
    else:
        for axis, benchmark in zip(axes_list, benchmarks):
            _style_axes(axis)
            benchmark_rows = [row for row in rows if row["benchmark"] == benchmark]
            benchmark_rows.sort(key=lambda row: float(row["mean_latency_us"]))
            labels = [str(row["engine"]) for row in benchmark_rows]
            values = [float(row["mean_latency_us"]) for row in benchmark_rows]
            colors = [palette[index % len(palette)] for index in range(len(labels))]
            axis.bar(labels, values, color=colors, width=0.66)
            axis.set_title(benchmark.replace("_", " ").title(), fontsize=12, color="#0f172a")
            axis.tick_params(axis="x", rotation=25)
            axis.set_xlabel("Engine", fontsize=10, color="#334155")
            axis.set_ylabel("$\\mu s/op$", fontsize=10, color="#334155")
        title = f"Embedded Engine Latency Comparison ({op_counts[0]:,} ops)"
        base_name = "latency-comparison-overview"

    for axis in axes_list[len(benchmarks):]:
        axis.remove()

    figure.suptitle(title, fontsize=18, color="#0f172a")
    return _save_figure(figure, output_dir, base_name)


def export_latency_charts(
    bundles: Iterable[ResultsBundle],
    output_dir: Path,
    docs_assets_dir: Path | None = None,
) -> List[Path]:
    rows: List[Dict[str, object]] = []
    for bundle in bundles:
        rows.extend(_flatten_bundle(bundle))

    if not rows:
        return []

    _write_chart_data(rows, output_dir)
    exported: List[Path] = []
    benchmarks = sorted({str(row["benchmark"]) for row in rows})
    op_counts = sorted({int(row["operations"]) for row in rows})

    exported.extend(_plot_overview(rows, output_dir))

    for benchmark in benchmarks:
        if len(op_counts) > 1:
            exported.extend(_plot_benchmark_line(rows, benchmark, output_dir))
        else:
            exported.extend(_plot_benchmark_bar(rows, benchmark, output_dir))

    if docs_assets_dir is not None:
        docs_assets_dir.mkdir(parents=True, exist_ok=True)
        (docs_assets_dir / "chart_data.json").write_text(
            json.dumps(rows, indent=2), encoding="utf-8"
        )
        for path in exported:
            target = docs_assets_dir / path.name
            target.write_bytes(path.read_bytes())

    return exported