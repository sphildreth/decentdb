#!/usr/bin/env python3
"""
Run all DecentDB examples and report results.

Usage:
    python run_all.py              # run all examples
    python run_all.py --python     # run only Python examples
    python run_all.py --dotnet     # run only .NET examples
    python run_all.py --node       # run only Node.js examples
    python run_all.py --go         # run only Go examples
    python run_all.py --memory     # run only in-memory examples
    python run_all.py --file       # run only file-based examples

Requires:
    - DecentDB native library built (nimble build_lib)
    - .NET SDK 10+ for dotnet examples
    - Node.js addon built (cd bindings/node/decentdb && npm run build)
    - Go module set up for Go examples
    - Python decentdb package available
"""

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples"
LIB_PATH = REPO_ROOT / "build" / "libdecentdb.so"


@dataclass
class Example:
    name: str
    language: str
    storage: str  # "file" or "memory"
    cwd: Path
    command: list[str]
    env_extra: dict[str, str] | None = None


def discover_examples() -> list[Example]:
    """Build the list of all known examples."""
    lib = str(LIB_PATH)
    native_env = {"DECENTDB_LIB_PATH": lib, "DECENTDB_NATIVE_LIB_PATH": lib}

    examples = [
        # Python
        Example(
            name="python/example.py",
            language="python",
            storage="file",
            cwd=EXAMPLES_DIR / "python",
            command=[sys.executable, "example.py"],
            env_extra=native_env,
        ),
        Example(
            name="python/example_memory.py",
            language="python",
            storage="memory",
            cwd=EXAMPLES_DIR / "python",
            command=[sys.executable, "example_memory.py"],
            env_extra=native_env,
        ),
        # Node.js
        Example(
            name="node/example.js",
            language="node",
            storage="file",
            cwd=EXAMPLES_DIR / "node",
            command=["node", "example.js"],
            env_extra=native_env,
        ),
        Example(
            name="node/example_memory.js",
            language="node",
            storage="memory",
            cwd=EXAMPLES_DIR / "node",
            command=["node", "example_memory.js"],
            env_extra=native_env,
        ),
        # .NET
        Example(
            name="dotnet/dapper-basic",
            language="dotnet",
            storage="file",
            cwd=EXAMPLES_DIR / "dotnet" / "dapper-basic",
            command=["dotnet", "run", "--no-build"],
        ),
        Example(
            name="dotnet/dapper-memory",
            language="dotnet",
            storage="memory",
            cwd=EXAMPLES_DIR / "dotnet" / "dapper-memory",
            command=["dotnet", "run", "--no-build"],
        ),
        Example(
            name="dotnet/microorm-linq",
            language="dotnet",
            storage="file",
            cwd=EXAMPLES_DIR / "dotnet" / "microorm-linq",
            command=["dotnet", "run", "--no-build"],
        ),
        Example(
            name="dotnet/microorm-memory",
            language="dotnet",
            storage="memory",
            cwd=EXAMPLES_DIR / "dotnet" / "microorm-memory",
            command=["dotnet", "run", "--no-build"],
        ),
        Example(
            name="dotnet/entityframework",
            language="dotnet",
            storage="memory",
            cwd=EXAMPLES_DIR / "dotnet" / "entityframework",
            command=["dotnet", "run", "--no-build"],
        ),
    ]

    # Go
    go_dir = EXAMPLES_DIR / "go"
    if (go_dir / "go.mod").exists():
        examples.extend([
            Example(
                name="go/main.go",
                language="go",
                storage="file",
                cwd=go_dir,
                command=["go", "run", "main.go"],
                env_extra=native_env,
            ),
            Example(
                name="go/main_memory.go",
                language="go",
                storage="memory",
                cwd=go_dir,
                command=["go", "run", "main_memory.go"],
                env_extra=native_env,
            ),
        ])

    return examples


def build_dotnet_examples() -> bool:
    """Build all .NET examples in one pass."""
    dotnet_dir = EXAMPLES_DIR / "dotnet"
    projects = [
        d for d in dotnet_dir.iterdir()
        if d.is_dir() and any(d.glob("*.csproj"))
    ]
    if not projects:
        return True

    print("Building .NET examples...")
    for project in sorted(projects):
        result = subprocess.run(
            ["dotnet", "build", "--verbosity", "minimal"],
            cwd=project,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            print(f"  ✗ Build failed: {project.name}")
            print(result.stderr[-500:] if result.stderr else result.stdout[-500:])
            return False
        print(f"  ✓ Built {project.name}")
    return True


def run_example(example: Example, verbose: bool = False) -> tuple[bool, float, str]:
    """
    Run a single example.

    Returns (success, elapsed_seconds, error_output).
    """
    env = os.environ.copy()
    if example.env_extra:
        env.update(example.env_extra)

    start = time.monotonic()
    try:
        result = subprocess.run(
            example.command,
            cwd=example.cwd,
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )
        elapsed = time.monotonic() - start

        if verbose and result.stdout:
            for line in result.stdout.strip().splitlines():
                print(f"    {line}")

        if result.returncode != 0:
            error = result.stderr.strip() or result.stdout.strip()
            # Take last 500 chars of error output
            return False, elapsed, error[-500:]

        return True, elapsed, ""

    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - start
        return False, elapsed, "TIMEOUT (300s)"
    except FileNotFoundError as e:
        elapsed = time.monotonic() - start
        return False, elapsed, f"Command not found: {e}"


def main():
    parser = argparse.ArgumentParser(description="Run all DecentDB examples")
    parser.add_argument("--python", action="store_true", help="Run only Python examples")
    parser.add_argument("--dotnet", action="store_true", help="Run only .NET examples")
    parser.add_argument("--node", action="store_true", help="Run only Node.js examples")
    parser.add_argument("--go", action="store_true", help="Run only Go examples")
    parser.add_argument("--memory", action="store_true", help="Run only in-memory examples")
    parser.add_argument("--file", action="store_true", help="Run only file-based examples")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show example output")
    parser.add_argument("--no-build", action="store_true", help="Skip .NET build step")
    args = parser.parse_args()

    # Check native library
    if not LIB_PATH.exists():
        print(f"✗ Native library not found at {LIB_PATH}")
        print("  Run: nimble build_lib")
        sys.exit(1)

    examples = discover_examples()

    # Apply filters
    lang_filter = set()
    if args.python:
        lang_filter.add("python")
    if args.dotnet:
        lang_filter.add("dotnet")
    if args.node:
        lang_filter.add("node")
    if args.go:
        lang_filter.add("go")

    if lang_filter:
        examples = [e for e in examples if e.language in lang_filter]
    if args.memory:
        examples = [e for e in examples if e.storage == "memory"]
    if args.file:
        examples = [e for e in examples if e.storage == "file"]

    if not examples:
        print("No examples matched the given filters.")
        sys.exit(1)

    # Build .NET if needed
    has_dotnet = any(e.language == "dotnet" for e in examples)
    if has_dotnet and not args.no_build:
        if not build_dotnet_examples():
            print("\n✗ .NET build failed — aborting .NET examples")
            examples = [e for e in examples if e.language != "dotnet"]

    # Run examples
    print(f"\nRunning {len(examples)} example(s)...\n")
    passed = 0
    failed = 0
    skipped_names: list[str] = []
    failures: list[tuple[str, str]] = []

    for example in examples:
        label = f"[{example.storage:6s}] {example.name}"
        success, elapsed, error = run_example(example, verbose=args.verbose)

        if success:
            print(f"  ✓ {label}  ({elapsed:.1f}s)")
            passed += 1
        else:
            print(f"  ✗ {label}  ({elapsed:.1f}s)")
            failures.append((example.name, error))
            failed += 1

    # Summary
    total = passed + failed
    print(f"\n{'═' * 60}")
    print(f"Results: {passed}/{total} passed", end="")
    if failed:
        print(f", {failed} failed")
    else:
        print()
    print(f"{'═' * 60}")

    if failures:
        print("\nFailures:")
        for name, error in failures:
            print(f"\n  {name}:")
            for line in error.splitlines()[-5:]:
                print(f"    {line}")

    # Note skipped Go
    go_dir = EXAMPLES_DIR / "go"
    if not (go_dir / "go.mod").exists() and (not lang_filter or "go" in lang_filter):
        print(f"\n⚠ Go examples skipped (no go.mod in {go_dir})")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
