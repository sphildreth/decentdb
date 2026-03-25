"""Tests for manifest generation."""

import pytest

from utils.manifest import (
    RunManifest,
    ResultsBundle,
    ResultRecord,
    get_machine_info,
    get_python_version,
)


class TestRunManifest:
    """Test run manifest creation and serialization."""

    def test_create_manifest(self):
        """Manifest should be created with required fields."""
        manifest = RunManifest.create(
            workload_name="workload_a",
            scenario_name="canonical",
            transaction_mode="batched",
            durability_mode="durable",
            engines=["sqlite", "duckdb"],
            dataset_seed=42,
        )

        assert manifest.run_id is not None
        assert manifest.run_timestamp is not None
        assert manifest.workload_name == "workload_a"
        assert manifest.scenario_name == "canonical"
        assert manifest.transaction_mode == "batched"
        assert manifest.durability_mode == "durable"
        assert manifest.engines == ["sqlite", "duckdb"]
        assert manifest.dataset_seed == 42
        assert manifest.generator_version is not None

    def test_manifest_to_dict(self):
        """Manifest should serialize to dict."""
        manifest = RunManifest.create(
            workload_name="workload_a",
            scenario_name="canonical",
            transaction_mode="batched",
            durability_mode="durable",
            engines=["sqlite"],
            dataset_seed=42,
        )

        d = manifest.to_dict()

        assert isinstance(d, dict)
        assert "run_id" in d
        assert "workload_name" in d
        assert d["workload_name"] == "workload_a"

    def test_machine_info_includes_hostname(self):
        """Machine info should include hostname."""
        info = get_machine_info()

        assert "hostname" in info
        assert "os" in info
        assert info["hostname"] is not None

    def test_python_version_is_recorded(self):
        """Python version should be recorded."""
        version = get_python_version()

        assert version is not None
        assert len(version) > 0


class TestResultRecord:
    """Test result record creation."""

    def test_create_result_record(self):
        """Result record should have all required fields."""
        record = ResultRecord(
            engine="SQLite",
            engine_version="3.44.0",
            benchmark="point_select",
            operations=10000,
            duration_sec=1.5,
            latency_ms={
                "p50_ms": 0.1,
                "p95_ms": 0.5,
                "p99_ms": 1.0,
            },
            throughput_ops_sec=6666.67,
        )

        assert record.engine == "SQLite"
        assert record.benchmark == "point_select"
        assert record.operations == 10000

    def test_result_record_serialization(self):
        """Result record should serialize to dict."""
        record = ResultRecord(
            engine="SQLite",
            engine_version="3.44.0",
            benchmark="point_select",
            operations=10000,
            duration_sec=1.5,
            latency_ms={"p50_ms": 0.1, "p95_ms": 0.5},
            throughput_ops_sec=6666.67,
        )

        d = record.to_dict()

        assert isinstance(d, dict)
        assert d["engine"] == "SQLite"
        assert d["benchmark"] == "point_select"

    def test_result_record_with_metadata(self):
        """Result record should include optional metadata."""
        record = ResultRecord(
            engine="SQLite",
            engine_version="3.44.0",
            benchmark="point_select",
            operations=10000,
            duration_sec=1.5,
            latency_ms={"p50_ms": 0.1},
            throughput_ops_sec=6666.67,
            metadata={"storage_bytes": 1024000},
        )

        d = record.to_dict()

        assert d["metadata"]["storage_bytes"] == 1024000


class TestResultsBundle:
    """Test results bundle."""

    def test_create_bundle(self):
        """Bundle should contain manifest and results."""
        manifest = RunManifest.create(
            workload_name="workload_a",
            scenario_name="canonical",
            transaction_mode="batched",
            durability_mode="durable",
            engines=["sqlite"],
            dataset_seed=42,
        )

        bundle = ResultsBundle(manifest)

        assert bundle.manifest is not None
        assert len(bundle.results) == 0

    def test_add_result_to_bundle(self):
        """Should be able to add results to bundle."""
        manifest = RunManifest.create(
            workload_name="workload_a",
            scenario_name="canonical",
            transaction_mode="batched",
            durability_mode="durable",
            engines=["sqlite"],
            dataset_seed=42,
        )

        bundle = ResultsBundle(manifest)

        record = ResultRecord(
            engine="SQLite",
            engine_version="3.44.0",
            benchmark="point_select",
            operations=10000,
            duration_sec=1.5,
            latency_ms={"p50_ms": 0.1},
            throughput_ops_sec=6666.67,
        )

        bundle.add_result(record)

        assert len(bundle.results) == 1

    def test_bundle_serialization(self):
        """Bundle should serialize to dict."""
        manifest = RunManifest.create(
            workload_name="workload_a",
            scenario_name="canonical",
            transaction_mode="batched",
            durability_mode="durable",
            engines=["sqlite"],
            dataset_seed=42,
        )

        bundle = ResultsBundle(manifest)

        record = ResultRecord(
            engine="SQLite",
            engine_version="3.44.0",
            benchmark="point_select",
            operations=10000,
            duration_sec=1.5,
            latency_ms={"p50_ms": 0.1},
            throughput_ops_sec=6666.67,
        )
        bundle.add_result(record)

        d = bundle.to_dict()

        assert "manifest" in d
        assert "results" in d
        assert len(d["results"]) == 1
