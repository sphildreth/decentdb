#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


def decode_name(filename: str) -> str:
    name = filename
    if name.endswith(".gcov"):
        name = name[:-5]
    if name.startswith("@m"):
        name = name[2:]
    name = name.replace("@@", "@")
    name = name.replace("@s", "/")
    if name.endswith(".c"):
        name = name[:-2]
    return name


def display_path(decoded: str) -> str:
    path = decoded.replace("\\", "/")
    if path.startswith("src/"):
        return path
    idx = path.find("/src/")
    if idx != -1:
        return path[idx + 1 :]
    return path


def is_project_source(decoded: str) -> bool:
    path = decoded.replace("\\", "/")
    return path.startswith("src/") or "/src/" in path


def parse_gcov_file(path: Path) -> tuple[int, int]:
    total = 0
    covered = 0
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split(":", 2)
        if len(parts) < 3:
            continue
        count = parts[0].strip()
        if count in {"-", "====="}:
            continue
        total += 1
        if count != "#####":
            covered += 1
    return covered, total


def main() -> int:
    if len(sys.argv) != 5:
        print("usage: coverage_summary.py <gcov_dir> <repo_root> <summary_txt> <summary_json>")
        return 2

    gcov_dir = Path(sys.argv[1])
    summary_txt = Path(sys.argv[3])
    summary_json = Path(sys.argv[4])

    entries = []
    total_covered = 0
    total_lines = 0

    for gcov_file in sorted(gcov_dir.glob("*.gcov")):
        decoded = decode_name(gcov_file.name)
        if not is_project_source(decoded):
            continue
        covered, lines = parse_gcov_file(gcov_file)
        if lines == 0:
            continue
        total_covered += covered
        total_lines += lines
        entries.append(
            {
                "file": display_path(decoded),
                "covered": covered,
                "total": lines,
                "percent": round(covered / lines * 100.0, 2),
            }
        )

    entries.sort(key=lambda e: e["file"])

    overall_percent = round((total_covered / total_lines * 100.0), 2) if total_lines else 0.0

    summary_txt.parent.mkdir(parents=True, exist_ok=True)
    with summary_txt.open("w", encoding="utf-8") as f:
        f.write(f"Overall: {total_covered}/{total_lines} ({overall_percent}%)\n")
        for entry in entries:
            f.write(
                f"{entry['file']}: {entry['covered']}/{entry['total']} ({entry['percent']}%)\n"
            )

    summary_json.parent.mkdir(parents=True, exist_ok=True)
    with summary_json.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "overall": {
                    "covered": total_covered,
                    "total": total_lines,
                    "percent": overall_percent,
                },
                "files": entries,
            },
            f,
            indent=2,
            sort_keys=True,
        )
        f.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
