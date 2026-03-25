"""Tests for benchmarks markdown auto-refresh helper."""

import json
from pathlib import Path

from utils.benchmarks_doc import update_benchmarks_markdown


def test_update_benchmarks_markdown_rewrites_marked_sections(tmp_path):
    docs_assets_dir = tmp_path / "docs-assets" / "benchmarks" / "python-embedded-compare"
    workload_a_dir = docs_assets_dir / "workload_a"
    workload_c_dir = docs_assets_dir / "workload_c"
    workload_a_dir.mkdir(parents=True, exist_ok=True)
    workload_c_dir.mkdir(parents=True, exist_ok=True)

    workload_a_rows = [
        {
            "engine": "SQLite_wal_full",
            "benchmark": "point_select",
            "operations": 500,
            "mean_latency_us": 7.0,
            "p50_latency_us": 6.5,
            "p95_latency_us": 8.0,
            "throughput_ops_sec": 100.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "DecentDB",
            "benchmark": "point_select",
            "operations": 500,
            "mean_latency_us": 31.0,
            "p50_latency_us": 30.0,
            "p95_latency_us": 35.0,
            "throughput_ops_sec": 20.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "SQLite_wal_full",
            "benchmark": "aggregate",
            "operations": 500,
            "mean_latency_us": 53.0,
            "p50_latency_us": 52.0,
            "p95_latency_us": 55.0,
            "throughput_ops_sec": 30.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "DecentDB",
            "benchmark": "aggregate",
            "operations": 500,
            "mean_latency_us": 292.0,
            "p50_latency_us": 290.0,
            "p95_latency_us": 300.0,
            "throughput_ops_sec": 5.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "SQLite_wal_full",
            "benchmark": "join",
            "operations": 500,
            "mean_latency_us": 58.0,
            "p50_latency_us": 57.0,
            "p95_latency_us": 61.0,
            "throughput_ops_sec": 25.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "DecentDB",
            "benchmark": "join",
            "operations": 500,
            "mean_latency_us": 758.0,
            "p50_latency_us": 750.0,
            "p95_latency_us": 800.0,
            "throughput_ops_sec": 2.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "SQLite_wal_full",
            "benchmark": "range_scan",
            "operations": 500,
            "mean_latency_us": 13.0,
            "p50_latency_us": 12.0,
            "p95_latency_us": 15.0,
            "throughput_ops_sec": 40.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "DecentDB",
            "benchmark": "range_scan",
            "operations": 500,
            "mean_latency_us": 295.0,
            "p50_latency_us": 280.0,
            "p95_latency_us": 330.0,
            "throughput_ops_sec": 6.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "SQLite_wal_full",
            "benchmark": "delete",
            "operations": 500,
            "mean_latency_us": 0.0,
            "p50_latency_us": 0.0,
            "p95_latency_us": 0.0,
            "throughput_ops_sec": 50.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "DecentDB",
            "benchmark": "delete",
            "operations": 500,
            "mean_latency_us": 628.0,
            "p50_latency_us": 620.0,
            "p95_latency_us": 700.0,
            "throughput_ops_sec": 1.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "SQLite_wal_full",
            "benchmark": "update",
            "operations": 500,
            "mean_latency_us": 0.0,
            "p50_latency_us": 0.0,
            "p95_latency_us": 0.0,
            "throughput_ops_sec": 55.0,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "DecentDB",
            "benchmark": "update",
            "operations": 500,
            "mean_latency_us": 1076.0,
            "p50_latency_us": 1060.0,
            "p95_latency_us": 1200.0,
            "throughput_ops_sec": 0.8,
            "scenario": "canonical",
            "workload": "workload_a",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
    ]
    workload_c_rows = [
        {
            "engine": "DecentDB",
            "benchmark": "full_scan",
            "operations": 500,
            "mean_latency_us": 6200.0,
            "p50_latency_us": 6100.0,
            "p95_latency_us": 6500.0,
            "throughput_ops_sec": 1.0,
            "scenario": "canonical",
            "workload": "workload_c",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "SQLite_wal_full",
            "benchmark": "full_scan",
            "operations": 500,
            "mean_latency_us": 7000.0,
            "p50_latency_us": 6900.0,
            "p95_latency_us": 7300.0,
            "throughput_ops_sec": 0.9,
            "scenario": "canonical",
            "workload": "workload_c",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "SQLite_wal_full",
            "benchmark": "point_select",
            "operations": 500,
            "mean_latency_us": 5.0,
            "p50_latency_us": 4.8,
            "p95_latency_us": 6.0,
            "throughput_ops_sec": 100.0,
            "scenario": "canonical",
            "workload": "workload_c",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
        {
            "engine": "DecentDB",
            "benchmark": "point_select",
            "operations": 500,
            "mean_latency_us": 8.0,
            "p50_latency_us": 7.9,
            "p95_latency_us": 9.0,
            "throughput_ops_sec": 80.0,
            "scenario": "canonical",
            "workload": "workload_c",
            "transaction_mode": "batched",
            "durability_mode": "durable",
        },
    ]

    (workload_a_dir / "chart_data.json").write_text(
        json.dumps(workload_a_rows, indent=2), encoding="utf-8"
    )
    (workload_c_dir / "chart_data.json").write_text(
        json.dumps(workload_c_rows, indent=2), encoding="utf-8"
    )

    workspace_root = tmp_path
    (workspace_root / "Cargo.toml").write_text(
        "\n".join(
            [
                "[workspace]",
                "members = []",
                "",
                "[workspace.package]",
                'version = "9.9.9"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    doc = tmp_path / "benchmarks.md"
    doc.write_text(
        "\n".join(
            [
                "# Benchmarks",
                "",
                "| Engine | Version stamp | Source |",
                "| --- | --- | --- |",
                "| DecentDB | 0.0.1 | Workspace package version |",
                "",
                "<!-- BENCHMARK_AUTOGEN_METADATA_START -->",
                "old metadata",
                "<!-- BENCHMARK_AUTOGEN_METADATA_END -->",
                "",
                "## At-a-glance summary",
                "",
                "<!-- BENCHMARK_AUTOGEN_SUMMARY_START -->",
                "old summary",
                "<!-- BENCHMARK_AUTOGEN_SUMMARY_END -->",
                "",
                "Example:",
                "",
                "<!-- BENCHMARK_AUTOGEN_EXAMPLES_START -->",
                "old examples",
                "<!-- BENCHMARK_AUTOGEN_EXAMPLES_END -->",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    snapshot_ops = update_benchmarks_markdown(
        docs_markdown_path=doc,
        docs_assets_dir=docs_assets_dir,
        workspace_root=workspace_root,
    )
    assert snapshot_ops == 500
    updated = doc.read_text(encoding="utf-8")
    assert "old metadata" not in updated
    assert "old summary" not in updated
    assert "old examples" not in updated
    assert "| Workload C | Full scan | DecentDB |" in updated
    assert "| Workload A | Update | SQLite_wal_full |" in updated
    assert "In `workload_c / full_scan`, DecentDB is `1st of 2`" in updated
    assert "| DecentDB | 9.9.9 | Workspace package version |" in updated
