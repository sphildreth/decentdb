"""Run manifest generation for benchmark reproducibility.

Each benchmark run produces a manifest that captures:
- Engine versions and configuration
- Dataset generation details
- Transaction and durability modes
- Environment details
"""

import os
import platform
import socket
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def get_machine_info() -> Dict[str, str]:
    """Get machine and environment information."""
    info = {
        "hostname": socket.gethostname(),
        "os": platform.system(),
        "os_version": platform.version(),
        "arch": platform.machine(),
    }

    # Try to get CPU info
    try:
        if platform.system() == "Linux" and os.path.exists("/proc/cpuinfo"):
            with open("/proc/cpuinfo", "r") as f:
                for line in f:
                    if line.startswith("model name"):
                        info["cpu_model"] = line.split(":", 1)[1].strip()
                        break
        else:
            info["cpu_model"] = platform.processor()
    except:
        info["cpu_model"] = "unknown"

    # Try to get memory info
    try:
        if platform.system() == "Linux" and os.path.exists("/proc/meminfo"):
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        info["memory_total"] = line.split(":", 1)[1].strip()
                        break
    except:
        pass

    # Filesystem type
    try:
        if platform.system() == "Linux":
            info["fs_type"] = "unknown"
            # Check mount for common locations
            for path in ["/tmp", os.getcwd()]:
                if os.path.exists(path):
                    # Simple heuristic - just note it's filesystem
                    pass
    except:
        pass

    return info


def get_python_version() -> str:
    """Get Python version string."""
    return f"{platform.python_implementation()} {platform.python_version()}"


@dataclass
class RunManifest:
    """Manifest for a benchmark run."""

    # Run identification
    run_id: str
    run_timestamp: str

    # Generator info
    generator_version: str
    dataset_seed: int

    # Workload info
    workload_name: str
    scenario_name: str

    # Transaction and durability
    transaction_mode: str  # autocommit, batched, explicit
    durability_mode: str  # durable, relaxed

    # Engines run
    engines: List[str]

    # Environment
    machine_info: Dict[str, str]
    python_version: str

    # Configuration notes
    config_notes: Dict[str, str] = field(default_factory=dict)

    # Per-engine execution status
    engine_status: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Output path
    output_dir: str = ""

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def create(
        cls,
        workload_name: str,
        scenario_name: str,
        transaction_mode: str,
        durability_mode: str,
        engines: List[str],
        dataset_seed: int = 42,
    ) -> "RunManifest":
        """Create a new manifest.

        Args:
            workload_name: Name of the workload
            scenario_name: Name of the scenario
            transaction_mode: Transaction mode used
            durability_mode: Durability mode used
            engines: List of engine names
            dataset_seed: Seed used for dataset generation

        Returns:
            New RunManifest instance
        """
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        timestamp = datetime.now(timezone.utc).isoformat()

        return cls(
            run_id=run_id,
            run_timestamp=timestamp,
            generator_version="1.0.0",  # Match dataset_generator
            dataset_seed=dataset_seed,
            workload_name=workload_name,
            scenario_name=scenario_name,
            transaction_mode=transaction_mode,
            durability_mode=durability_mode,
            engines=engines,
            machine_info=get_machine_info(),
            python_version=get_python_version(),
        )


class ResultRecord:
    """Individual benchmark result record."""

    def __init__(
        self,
        engine: str,
        engine_version: str,
        benchmark: str,
        operations: int,
        duration_sec: float,
        latency_ms: Dict[str, float],
        throughput_ops_sec: float,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.engine = engine
        self.engine_version = engine_version
        self.benchmark = benchmark
        self.operations = operations
        self.duration_sec = duration_sec
        self.latency_ms = latency_ms
        self.throughput_ops_sec = throughput_ops_sec
        self.metadata = metadata or {}

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "engine": self.engine,
            "engine_version": self.engine_version,
            "benchmark": self.benchmark,
            "operations": self.operations,
            "duration_sec": self.duration_sec,
            "latency_ms": self.latency_ms,
            "throughput_ops_sec": self.throughput_ops_sec,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "ResultRecord":
        """Create from dictionary."""
        return cls(
            engine=data["engine"],
            engine_version=data["engine_version"],
            benchmark=data["benchmark"],
            operations=data["operations"],
            duration_sec=data["duration_sec"],
            latency_ms=data["latency_ms"],
            throughput_ops_sec=data["throughput_ops_sec"],
            metadata=data.get("metadata", {}),
        )


class ResultsBundle:
    """Collection of benchmark results with manifest."""

    def __init__(self, manifest: RunManifest):
        self.manifest = manifest
        self.results: List[ResultRecord] = []

    def add_result(self, result: ResultRecord):
        """Add a result record."""
        self.results.append(result)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "manifest": self.manifest.to_dict(),
            "results": [r.to_dict() for r in self.results],
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "ResultsBundle":
        """Create from dictionary."""
        manifest = RunManifest(**data["manifest"])
        bundle = cls(manifest)
        bundle.results = [ResultRecord.from_dict(r) for r in data["results"]]
        return bundle

    def save(self, path: Path):
        """Save results to JSON file."""
        import json

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "ResultsBundle":
        """Load results from JSON file."""
        import json

        with open(path, "r") as f:
            return cls.from_dict(json.load(f))
