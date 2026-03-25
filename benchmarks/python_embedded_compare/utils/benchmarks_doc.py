"""Autogenerate docs/user-guide/benchmarks.md summary sections.

This module consumes chart exports under docs/assets/benchmarks/python-embedded-compare/
and updates marked sections in docs/user-guide/benchmarks.md so release benchmark runs
can refresh standings deterministically.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - python<3.11 fallback
    import tomli as tomllib

METADATA_START = "<!-- BENCHMARK_AUTOGEN_METADATA_START -->"
METADATA_END = "<!-- BENCHMARK_AUTOGEN_METADATA_END -->"
SUMMARY_START = "<!-- BENCHMARK_AUTOGEN_SUMMARY_START -->"
SUMMARY_END = "<!-- BENCHMARK_AUTOGEN_SUMMARY_END -->"
EXAMPLES_START = "<!-- BENCHMARK_AUTOGEN_EXAMPLES_START -->"
EXAMPLES_END = "<!-- BENCHMARK_AUTOGEN_EXAMPLES_END -->"

DECENTDB_ENGINE = "DecentDB"
WORKLOAD_LABELS = {"workload_a": "Workload A", "workload_c": "Workload C"}
BENCHMARK_LABELS = {
    "point_select": "Point select",
    "range_scan": "Range scan",
    "join": "Join",
    "aggregate": "Aggregate",
    "update": "Update",
    "delete": "Delete",
    "full_scan": "Full scan",
}
SUMMARY_ORDER: List[Tuple[str, str]] = [
    ("workload_c", "full_scan"),
    ("workload_c", "point_select"),
    ("workload_a", "point_select"),
    ("workload_a", "aggregate"),
    ("workload_a", "join"),
    ("workload_a", "range_scan"),
    ("workload_a", "delete"),
    ("workload_a", "update"),
]


@dataclass(frozen=True)
class SummaryRow:
    workload: str
    benchmark: str
    leader_engine: str
    leader_latency_us: float
    decentdb_latency_us: float
    decentdb_rank: int
    engine_count: int
    reading: str


def _ordinal(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def _load_chart_rows(chart_path: Path, expected_workload: str) -> List[Dict[str, object]]:
    rows = json.loads(chart_path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError(f"expected list chart data in {chart_path}")
    for row in rows:
        if row.get("workload") != expected_workload:
            raise ValueError(
                f"chart data workload mismatch in {chart_path}: expected {expected_workload}"
            )
    return rows


def _candidate_ops_for_target(
    rows_by_workload: Dict[str, List[Dict[str, object]]]
) -> List[int]:
    op_sets: List[set[int]] = []
    for workload, benchmark in SUMMARY_ORDER:
        ops = {
            int(row["operations"])
            for row in rows_by_workload[workload]
            if row.get("benchmark") == benchmark
        }
        if not ops:
            raise ValueError(f"missing benchmark data for {workload}/{benchmark}")
        op_sets.append(ops)

    common = set.intersection(*op_sets)
    if not common:
        raise ValueError("no common operations count across all summary benchmarks")
    return sorted(common)


def _reading_label(rank: int, engine_count: int, ratio: float) -> str:
    if rank == 1:
        return "Leading"
    if rank == 2:
        return "Near the front"
    if rank == engine_count and ratio >= 5.0:
        return "Trailing heavily"
    return "Trailing"


def _build_summary_rows(
    rows_by_workload: Dict[str, List[Dict[str, object]]], snapshot_ops: int
) -> List[SummaryRow]:
    summary_rows: List[SummaryRow] = []
    for workload, benchmark in SUMMARY_ORDER:
        benchmark_rows = [
            row
            for row in rows_by_workload[workload]
            if row.get("benchmark") == benchmark and int(row["operations"]) == snapshot_ops
        ]
        if not benchmark_rows:
            raise ValueError(
                f"missing snapshot rows for {workload}/{benchmark} at ops={snapshot_ops}"
            )

        ranked = sorted(
            benchmark_rows,
            key=lambda row: (float(row["mean_latency_us"]), str(row["engine"]).lower()),
        )
        leader = ranked[0]
        leader_latency = float(leader["mean_latency_us"])
        decentdb_index = next(
            (index for index, row in enumerate(ranked) if str(row["engine"]) == DECENTDB_ENGINE),
            None,
        )
        if decentdb_index is None:
            raise ValueError(f"{DECENTDB_ENGINE} missing for {workload}/{benchmark}")

        decentdb_latency = float(ranked[decentdb_index]["mean_latency_us"])
        ratio = (
            (decentdb_latency / leader_latency)
            if leader_latency > 0.0
            else (float("inf") if decentdb_latency > 0.0 else 1.0)
        )
        rank = decentdb_index + 1
        summary_rows.append(
            SummaryRow(
                workload=workload,
                benchmark=benchmark,
                leader_engine=str(leader["engine"]),
                leader_latency_us=leader_latency,
                decentdb_latency_us=decentdb_latency,
                decentdb_rank=rank,
                engine_count=len(ranked),
                reading=_reading_label(rank, len(ranked), ratio),
            )
        )
    return summary_rows


def _replace_marked_block(
    markdown: str,
    start_marker: str,
    end_marker: str,
    replacement_lines: Iterable[str],
) -> str:
    lines = markdown.splitlines()
    try:
        start_index = lines.index(start_marker)
    except ValueError as error:
        raise ValueError(f"missing marker: {start_marker}") from error
    try:
        end_index = lines.index(end_marker)
    except ValueError as error:
        raise ValueError(f"missing marker: {end_marker}") from error
    if end_index <= start_index:
        raise ValueError(f"invalid marker order: {start_marker} .. {end_marker}")

    replaced = (
        lines[: start_index + 1]
        + list(replacement_lines)
        + lines[end_index:]
    )
    return "\n".join(replaced) + "\n"


def _workspace_version(workspace_root: Path) -> str:
    cargo_toml_path = workspace_root / "Cargo.toml"
    if not cargo_toml_path.exists():
        raise ValueError(f"missing Cargo.toml at {cargo_toml_path}")
    parsed = tomllib.loads(cargo_toml_path.read_text(encoding="utf-8"))
    version = (
        parsed.get("workspace", {})
        .get("package", {})
        .get("version")
    )
    if not isinstance(version, str) or not version.strip():
        raise ValueError("workspace.package.version is missing in Cargo.toml")
    return version.strip()


def _replace_decentdb_workspace_version_row(markdown: str, version: str) -> str:
    lines = markdown.splitlines()
    replacement = f"| DecentDB | {version} | Workspace package version |"
    for index, line in enumerate(lines):
        if line.startswith("| DecentDB | ") and line.endswith("| Workspace package version |"):
            lines[index] = replacement
            return "\n".join(lines) + "\n"
    raise ValueError("missing DecentDB engine version stamp row in benchmarks markdown")


def _summary_table_lines(rows: List[SummaryRow], snapshot_ops: int) -> List[str]:
    output = [
        f"| Workload | Benchmark | Leader at {snapshot_ops} ops | Leader mean latency (us/op) | DecentDB mean latency (us/op) | DecentDB rank | Reading |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        workload = WORKLOAD_LABELS[row.workload]
        benchmark = BENCHMARK_LABELS.get(row.benchmark, row.benchmark.replace("_", " ").title())
        rank = f"{_ordinal(row.decentdb_rank)} of {row.engine_count}"
        output.append(
            f"| {workload} | {benchmark} | {row.leader_engine} | "
            f"{row.leader_latency_us:.2f} | {row.decentdb_latency_us:.2f} | {rank} | {row.reading} |"
        )
    return output


def _example_lines(rows: List[SummaryRow], snapshot_ops: int) -> List[str]:
    row_map = {(row.workload, row.benchmark): row for row in rows}
    full_scan = row_map[("workload_c", "full_scan")]
    update = row_map[("workload_a", "update")]
    point_select = row_map[("workload_a", "point_select")]

    return [
        f"- In `workload_c / full_scan`, DecentDB is `{_ordinal(full_scan.decentdb_rank)} of {full_scan.engine_count}`, "
        f"so it leads that benchmark at `{snapshot_ops}` ops.",
        f"- In `workload_a / update`, DecentDB is `{_ordinal(update.decentdb_rank)} of {update.engine_count}`, "
        "so it trails the pack on that specific write-path measurement.",
        f"- In `workload_a / point_select`, DecentDB is `{_ordinal(point_select.decentdb_rank)} of {point_select.engine_count}`, "
        "which is a competitive result even though it is not the leader.",
    ]


def update_benchmarks_markdown(
    docs_markdown_path: Path,
    docs_assets_dir: Path,
    snapshot_ops: int | None = None,
    workspace_root: Path | None = None,
) -> int:
    rows_by_workload = {
        "workload_a": _load_chart_rows(
            docs_assets_dir / "workload_a" / "chart_data.json", "workload_a"
        ),
        "workload_c": _load_chart_rows(
            docs_assets_dir / "workload_c" / "chart_data.json", "workload_c"
        ),
    }

    if snapshot_ops is None:
        snapshot_ops = _candidate_ops_for_target(rows_by_workload)[-1]
    summary_rows = _build_summary_rows(rows_by_workload, snapshot_ops)

    markdown = docs_markdown_path.read_text(encoding="utf-8")
    if workspace_root is None:
        if len(docs_markdown_path.parents) >= 3:
            workspace_root = docs_markdown_path.parents[2]
        else:
            workspace_root = docs_markdown_path.parent
    markdown = _replace_decentdb_workspace_version_row(
        markdown,
        _workspace_version(workspace_root),
    )
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    markdown = _replace_marked_block(
        markdown,
        METADATA_START,
        METADATA_END,
        [
            f"- Document updated: {updated_at}",
            f"- Ranking snapshot: final sweep point at `{snapshot_ops}` operations from the exported benchmark bundles in `docs/assets/benchmarks/python-embedded-compare/`",
        ],
    )
    markdown = _replace_marked_block(
        markdown,
        SUMMARY_START,
        SUMMARY_END,
        _summary_table_lines(summary_rows, snapshot_ops),
    )
    markdown = _replace_marked_block(
        markdown,
        EXAMPLES_START,
        EXAMPLES_END,
        _example_lines(summary_rows, snapshot_ops),
    )
    docs_markdown_path.write_text(markdown, encoding="utf-8")
    return snapshot_ops
