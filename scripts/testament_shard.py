#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a skip list for a Testament shard.")
    parser.add_argument("--pattern", default="tests/nim/*.nim", help="Glob for test files.")
    parser.add_argument("--shard-index", type=int, required=True, help="Zero-based shard index.")
    parser.add_argument("--shard-count", type=int, required=True, help="Total shard count.")
    parser.add_argument("--skip-file", required=True, help="Output path for the skip file.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.shard_count <= 0:
        raise SystemExit("--shard-count must be positive")
    if not 0 <= args.shard_index < args.shard_count:
        raise SystemExit("--shard-index must be in [0, --shard-count)")

    repo_root = Path.cwd()
    files = sorted(path.relative_to(repo_root).as_posix() for path in repo_root.glob(args.pattern))
    if not files:
        raise SystemExit(f"No files matched pattern: {args.pattern}")

    shard_index = args.shard_index
    shard_count = args.shard_count
    selected = {
        path for idx, path in enumerate(files)
        if idx % shard_count == shard_index
    }

    skip_file = Path(args.skip_file)
    skip_file.parent.mkdir(parents=True, exist_ok=True)
    skip_file.write_text(
        "".join(f"{path}\n" for path in files if path not in selected),
        encoding="utf-8",
    )

    print(
        f"Shard {shard_index + 1}/{shard_count}: "
        f"selected {len(selected)} tests, skipped {len(files) - len(selected)} tests"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
