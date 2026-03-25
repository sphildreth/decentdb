#!/usr/bin/env python3
"""Main benchmark comparison runner.

This script orchestrates benchmark runs across multiple database engines,
ensuring fair comparison through deterministic dataset generation and
consistent measurement methodology.
"""

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import yaml

from drivers.base_driver import DatabaseDriver
from drivers.sqlite_driver import SQLiteDriver

# Optional drivers - handle gracefully if not installed
try:
    from drivers.duckdb_driver import DuckDBDriver
except ImportError:
    DuckDBDriver = None

try:
    from drivers.decentdb_driver import DecentDBDriver
except ImportError:
    DecentDBDriver = None

try:
    from drivers.jdbc_driver import JDBCDriver
except ImportError:
    JDBCDriver = None

try:
    from drivers.litedb_driver import LiteDBDriver
except ImportError:
    LiteDBDriver = None

try:
    from drivers.firebird_driver import FirebirdDriver
except ImportError:
    FirebirdDriver = None

from scenarios.canonical_workloads import get_workload, WORKLOADS
from utils.charting import export_latency_charts
from utils.dataset_generator import DatasetGenerator, GeneratorConfig
from utils.manifest import RunManifest, ResultsBundle, ResultRecord


DEFAULT_DOCS_ASSETS_DIR = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "assets"
    / "benchmarks"
    / "python-embedded-compare"
)


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load database configuration from YAML file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        return yaml.safe_load(f)


def create_driver(engine: str, config: Dict[str, Any], db_path: str) -> DatabaseDriver:
    """Create a database driver instance.

    Args:
        engine: Engine name (sqlite, duckdb, decentdb, h2, derby, hsqldb, firebird, litedb)
        config: Engine configuration
        db_path: Path for database files

    Returns:
        DatabaseDriver instance
    """
    engine = engine.lower()

    if engine == "sqlite":
        return SQLiteDriver(
            {
                "database_path": db_path,
                "variant": config.get("variant", "wal_full"),
            }
        )

    elif engine == "duckdb":
        if DuckDBDriver is None:
            raise ImportError("DuckDB not installed: pip install duckdb")
        return DuckDBDriver(
            {
                "database_path": db_path,
            }
        )

    elif engine == "decentdb":
        if DecentDBDriver is None:
            raise ImportError("DecentDB Python bindings not installed")
        return DecentDBDriver(
            {
                "database_path": db_path,
                "temp_dir": config.get("temp_dir", "/tmp/decentdb_bench"),
            }
        )

    elif engine in ("h2", "derby", "hsqldb"):
        if JDBCDriver is None:
            raise ImportError(
                "JDBC drivers not installed: pip install JayDeBeApi JPype1"
            )
        return JDBCDriver(
            {
                "engine": engine,
                "database_path": db_path,
                "jdbc_url": config.get("jdbc_url", ""),
                "driver_class": config.get("driver_class", ""),
                "jar_paths": config.get("jar_paths", []),
            }
        )

    elif engine == "firebird":
        if FirebirdDriver is None:
            raise ImportError("Firebird driver not available")
        return FirebirdDriver(
            {
                "database_path": db_path,
                "jdbc_url": config.get("jdbc_url", ""),
                "driver_class": config.get("driver_class", ""),
                "jar_paths": config.get("jar_paths", []),
            }
        )

    elif engine == "litedb":
        if LiteDBDriver is None:
            raise ImportError("LiteDB driver not available")
        harness_path = config.get("harness_path", "helpers/litedb")
        return LiteDBDriver(
            {
                "database_path": db_path,
                "harness_path": harness_path,
            }
        )

    else:
        raise ValueError(f"Unknown engine: {engine}")


def run_benchmark_for_engine(
    engine: str,
    engine_config: Dict[str, Any],
    workload_name: str,
    scenario_name: str,
    transaction_mode: str,
    durability_mode: str,
    dataset_seed: int,
    dataset_config: Dict[str, Any],
    operations: int,
    warmup: int,
    output_dir: Path,
) -> Dict[str, Any]:
    """Run benchmarks for a single engine.

    Args:
        engine: Engine name
        engine_config: Engine configuration
        workload_name: Workload name
        scenario_name: Scenario name
        transaction_mode: Transaction mode
        durability_mode: Durability mode
        dataset_seed: Random seed for dataset
        dataset_config: Dataset configuration
        operations: Number of operations per benchmark
        warmup: Number of warmup operations
        output_dir: Output directory

    Returns:
        Dictionary containing execution status, optional skip/failure reason,
        and any generated result records.
    """
    if engine == "litedb":
        return {
            "status": "skipped",
            "reason": (
                "LiteDB is a document-store baseline and needs its own mapped "
                "non-SQL workload before it can produce publishable results"
            ),
            "results": [],
        }

    # Create temporary directory for database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, f"{engine}.db")

        # Create driver
        try:
            driver = create_driver(engine, engine_config, db_path)
        except Exception as e:
            print(f"  [SKIP] Cannot create driver for {engine}: {e}")
            return {"status": "skipped", "reason": str(e), "results": []}

        # Connect
        if not driver.connect():
            print(f"  [SKIP] Cannot connect to {engine}")
            return {
                "status": "skipped",
                "reason": "connection failed",
                "results": [],
            }

        try:
            print(f"  Running benchmarks for {driver.name}...")

            # Get engine metadata
            metadata = driver.get_engine_metadata()
            print(f"    Engine: {metadata.name} v{metadata.version}")

            # Generate dataset
            config = GeneratorConfig(seed=dataset_seed, **dataset_config)
            generator = DatasetGenerator(config)
            customers, orders, events = generator.generate()
            print(
                f"    Dataset: {len(customers)} customers, {len(orders)} orders, {len(events)} events"
            )

            # Create schema
            workload = get_workload(workload_name)
            driver.create_schema(workload.get_schema_sql())

            # Load data
            print(f"    Loading data ({transaction_mode} mode)...")
            workload.load_data(
                driver,
                customers,
                orders,
                events,
                transaction_mode=transaction_mode,
                batch_size=1000,
            )

            # Set durability mode
            driver.set_durability_mode(durability_mode)

            # Run benchmarks
            results = {}

            # Point lookup
            print(f"    Running point_lookup benchmark...")
            result = workload.run_point_lookup(driver, operations, warmup)
            results["point_lookup"] = result

            # Range scan
            print(f"    Running range_scan benchmark...")
            result = workload.run_range_scan(driver, operations, warmup)
            results["range_scan"] = result

            # Join
            print(f"    Running join benchmark...")
            result = workload.run_join(driver, operations, warmup)
            results["join"] = result

            # Aggregate
            print(f"    Running aggregate benchmark...")
            result = workload.run_aggregate(driver, operations, warmup)
            results["aggregate"] = result

            # Update
            print(f"    Running update benchmark...")
            result = workload.run_update(driver, operations, warmup)
            results["update"] = result

            # Delete
            print(f"    Running delete benchmark...")
            result = workload.run_delete(driver, operations, warmup)
            results["delete"] = result

            # Get storage size
            storage_bytes = driver.get_storage_size()

            # Create result records
            result_records = []
            for bench_name, bench_result in results.items():
                if bench_result.operations > 0:
                    record = ResultRecord(
                        engine=driver.name,
                        engine_version=metadata.version,
                        benchmark=bench_result.benchmark_name,
                        operations=bench_result.operations,
                        duration_sec=bench_result.duration_sec,
                        latency_ms=bench_result.latency_ms,
                        throughput_ops_sec=bench_result.throughput_ops_sec,
                        metadata={
                            "storage_bytes": storage_bytes,
                            "config_notes": driver.get_config_notes(),
                        },
                    )
                    result_records.append(record)

            # Save individual results
            for record in result_records:
                output_file = output_dir / f"results_{engine}_{record.benchmark}.json"
                with open(output_file, "w") as f:
                    json.dump(record.to_dict(), f, indent=2)

            return {"status": "completed", "reason": "", "results": result_records}

        except Exception as e:
            print(f"  [ERROR] Benchmark failed for {engine}: {e}")
            import traceback

            traceback.print_exc()
            return {"status": "failed", "reason": str(e), "results": []}

        finally:
            driver.disconnect()


def run_comparison(
    engines: List[str],
    config: Dict[str, Any],
    workload_name: str,
    scenario_name: str,
    transaction_mode: str,
    durability_mode: str,
    dataset_seed: int,
    dataset_config: Dict[str, Any],
    operations: int,
    warmup: int,
    output_dir: Path,
) -> ResultsBundle:
    """Run comparison across multiple engines.

    Args:
        engines: List of engine names
        config: Configuration dictionary
        workload_name: Workload name
        scenario_name: Scenario name
        transaction_mode: Transaction mode
        durability_mode: Durability mode
        dataset_seed: Random seed
        dataset_config: Dataset configuration
        operations: Operations per benchmark
        warmup: Warmup operations
        output_dir: Output directory

    Returns:
        ResultsBundle with all results
    """
    # Create manifest
    manifest = RunManifest.create(
        workload_name=workload_name,
        scenario_name=scenario_name,
        transaction_mode=transaction_mode,
        durability_mode=durability_mode,
        engines=engines,
        dataset_seed=dataset_seed,
    )
    manifest.output_dir = str(output_dir)

    # Add generator metadata to manifest
    manifest.config_notes = {
        "dataset_config": json.dumps(dataset_config, sort_keys=True),
        "operation_count": str(operations),
        "warmup_operations": str(warmup),
    }

    bundle = ResultsBundle(manifest)

    # Run benchmarks for each engine
    for engine in engines:
        engine_config = config.get("engines", {}).get(engine, {})

        # Check if engine is enabled
        if not engine_config.get("enabled", True):
            print(f"Skipping disabled engine: {engine}")
            manifest.engine_status[engine] = {
                "status": "disabled",
                "reason": "disabled in configuration",
                "result_count": 0,
            }
            continue

        print(f"\n=== Benchmarking {engine} ===")

        outcome = run_benchmark_for_engine(
            engine=engine,
            engine_config=engine_config,
            workload_name=workload_name,
            scenario_name=scenario_name,
            transaction_mode=transaction_mode,
            durability_mode=durability_mode,
            dataset_seed=dataset_seed,
            dataset_config=dataset_config,
            operations=operations,
            warmup=warmup,
            output_dir=output_dir,
        )

        results = outcome["results"]
        manifest.engine_status[engine] = {
            "status": outcome["status"],
            "reason": outcome["reason"],
            "result_count": len(results),
        }

        if outcome["status"] != "completed":
            continue

        for result in results:
            bundle.add_result(result)
            print(f"  Completed: {result.benchmark}")
            print(f"    Throughput: {result.throughput_ops_sec:.2f} ops/sec")
            print(f"    p95 latency: {result.latency_ms.get('p95_ms', 0):.3f} ms")

    # Save manifest
    manifest_path = output_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest.to_dict(), f, indent=2)

    engine_status_path = output_dir / "engine_status.json"
    with open(engine_status_path, "w") as f:
        json.dump(manifest.engine_status, f, indent=2)

    # Save merged results
    merged_path = output_dir / "results_merged.json"
    bundle.save(merged_path)

    return bundle


def parse_ops_list(ops: int, ops_list: str | None) -> List[int]:
    if not ops_list:
        return [ops]
    parsed = [int(value.strip()) for value in ops_list.split(",") if value.strip()]
    if not parsed:
        return [ops]
    return parsed


def main():
    parser = argparse.ArgumentParser(
        description="Run embedded database benchmark comparisons"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/database_configs.yaml"),
        help="Path to database configuration file",
    )
    parser.add_argument(
        "--engines",
        type=str,
        default="sqlite,duckdb",
        help="Comma-separated list of engines to benchmark",
    )
    parser.add_argument(
        "--workload",
        type=str,
        default="workload_a",
        choices=list(WORKLOADS.keys()),
        help="Workload to run",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default="canonical",
        help="Scenario name",
    )
    parser.add_argument(
        "--transaction-mode",
        type=str,
        default="batched",
        choices=["autocommit", "batched", "explicit"],
        help="Transaction mode",
    )
    parser.add_argument(
        "--durability",
        type=str,
        default="durable",
        choices=["durable", "relaxed"],
        help="Durability mode",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for dataset generation",
    )
    parser.add_argument(
        "--ops",
        type=int,
        default=10000,
        help="Number of operations per benchmark",
    )
    parser.add_argument(
        "--ops-list",
        type=str,
        default=None,
        help="Comma-separated operation counts for a sweep and chart export",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=100,
        help="Number of warmup operations",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("out"),
        help="Output directory",
    )
    parser.add_argument(
        "--docs-assets-dir",
        type=Path,
        default=DEFAULT_DOCS_ASSETS_DIR,
        help="Directory where docs-referenceable benchmark charts should be written",
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=1000,
        help="Number of customers to generate",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=10000,
        help="Number of orders to generate",
    )
    parser.add_argument(
        "--events",
        type=int,
        default=10000,
        help="Number of events to generate",
    )

    args = parser.parse_args()
    args.config = args.config.resolve()
    args.output = args.output.resolve()
    args.docs_assets_dir = args.docs_assets_dir.resolve()

    # Parse engines
    engines = [e.strip() for e in args.engines.split(",")]

    # Load config
    config = {}
    if args.config.exists():
        config = load_config(args.config)
    else:
        print(f"Warning: Config file not found: {args.config}")
        print("Using default configuration")

    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)

    # Dataset config
    dataset_config = {
        "customers_n": args.customers,
        "orders_n": args.orders,
        "events_n": args.events,
    }

    print("=" * 60)
    print("Embedded Database Benchmark Comparison")
    print("=" * 60)
    print(f"Workload: {args.workload}")
    print(f"Scenario: {args.scenario}")
    print(f"Transaction mode: {args.transaction_mode}")
    print(f"Durability: {args.durability}")
    print(f"Dataset seed: {args.seed}")
    op_counts = parse_ops_list(args.ops, args.ops_list)
    if len(op_counts) == 1:
        print(f"Operations: {op_counts[0]}")
    else:
        print(f"Operations sweep: {', '.join(str(value) for value in op_counts)}")
    print(f"Engines: {', '.join(engines)}")
    print("=" * 60)

    bundles: List[ResultsBundle] = []
    for op_count in op_counts:
        run_output_dir = args.output if len(op_counts) == 1 else args.output / f"ops_{op_count}"
        run_output_dir.mkdir(parents=True, exist_ok=True)
        bundles.append(
            run_comparison(
                engines=engines,
                config=config,
                workload_name=args.workload,
                scenario_name=args.scenario,
                transaction_mode=args.transaction_mode,
                durability_mode=args.durability,
                dataset_seed=args.seed,
                dataset_config=dataset_config,
                operations=op_count,
                warmup=args.warmup,
                output_dir=run_output_dir,
            )
        )

    chart_output_dir = args.output / "charts"
    docs_chart_dir = args.docs_assets_dir if args.docs_assets_dir else None
    exported_charts = export_latency_charts(
        bundles,
        output_dir=chart_output_dir,
        docs_assets_dir=docs_chart_dir,
    )

    print("\n" + "=" * 60)
    print("Results Summary")
    print("=" * 60)

    for bundle in bundles:
        for result in bundle.results:
            print(f"\n{result.engine} ({result.benchmark}, ops={result.operations}):")
            print(f"  Throughput: {result.throughput_ops_sec:.2f} ops/sec")
            print(f"  p50 latency: {result.latency_ms.get('p50_ms', 0):.3f} ms")
            print(f"  p95 latency: {result.latency_ms.get('p95_ms', 0):.3f} ms")
            print(f"  p99 latency: {result.latency_ms.get('p99_ms', 0):.3f} ms")

    print(f"\nResults saved to: {args.output}")
    if len(op_counts) == 1:
        print("  - manifest.json")
        print("  - engine_status.json")
        print("  - results_merged.json")
        print("  - results_*.json")
    else:
        print("  - ops_<count>/manifest.json")
        print("  - ops_<count>/engine_status.json")
        print("  - ops_<count>/results_merged.json")
        print("  - ops_<count>/results_*.json")
    if exported_charts:
        print(f"  - charts/*.svg")
        print(f"  - charts/*.png")
        if docs_chart_dir:
            print(f"  - docs assets exported to: {docs_chart_dir}")


if __name__ == "__main__":
    main()
