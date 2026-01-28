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


def parse_gcov_file(path: Path) -> dict[int, tuple[bool, bool]]:
    # Returns map: line_no -> (executable, covered)
    lines: dict[int, tuple[bool, bool]] = {}
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split(":", 2)
        if len(parts) < 3:
            continue
        count = parts[0].strip()
        line_no_str = parts[1].strip()
        if not line_no_str.isdigit():
            continue
        line_no = int(line_no_str)
        if count in {"-", "====="}:
            continue
        executable = True
        covered = False
        if count == "#####":
            covered = False
        else:
            try:
                covered = int(count) > 0
            except ValueError:
                covered = False
        prev = lines.get(line_no)
        if prev is None:
            lines[line_no] = (executable, covered)
        else:
            prev_exec, prev_cov = prev
            lines[line_no] = (prev_exec or executable, prev_cov or covered)
    return lines


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

    coverage_by_file: dict[str, dict[int, tuple[bool, bool]]] = {}

    for gcov_file in sorted(gcov_dir.rglob("*.gcov")):
        decoded = decode_name(gcov_file.name)
        if not is_project_source(decoded):
            continue
        line_map = parse_gcov_file(gcov_file)
        if not line_map:
            continue
        file_map = coverage_by_file.get(decoded)
        if file_map is None:
            coverage_by_file[decoded] = line_map
        else:
            for line_no, (executable, covered) in line_map.items():
                prev_exec, prev_cov = file_map.get(line_no, (False, False))
                file_map[line_no] = (prev_exec or executable, prev_cov or covered)

    for decoded, line_map in sorted(coverage_by_file.items()):
        total = 0
        covered = 0
        for _, (executable, is_covered) in line_map.items():
            if not executable:
                continue
            total += 1
            if is_covered:
                covered += 1
        if total == 0:
            continue
        total_covered += covered
        total_lines += total
        entries.append(
            {
                "file": display_path(decoded),
                "covered": covered,
                "total": total,
                "percent": round(covered / total * 100.0, 2),
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
