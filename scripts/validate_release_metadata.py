#!/usr/bin/env python3
"""Validate release-facing version metadata and changelog hygiene."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


SEMVER_RE = re.compile(
    r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$"
)


def read_text(path: Path, issues: list[str]) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as error:
        issues.append(f"{path} is missing or unreadable: {error}")
        return ""


def expect_regex(
    root: Path,
    relative_path: str,
    pattern: str,
    expected: str,
    label: str,
    issues: list[str],
) -> None:
    path = root / relative_path
    text = read_text(path, issues)
    if not text:
        return
    match = re.search(pattern, text, flags=re.MULTILINE | re.DOTALL)
    if not match:
        issues.append(f"{relative_path} missing {label}")
        return
    actual = match.group(1)
    if actual != expected:
        issues.append(
            f"{relative_path} {label} is {actual!r}, expected {expected!r}"
        )


def expect_json_version(
    root: Path,
    relative_path: str,
    expected: str,
    issues: list[str],
    *,
    package_paths: tuple[str, ...] = ("",),
) -> None:
    path = root / relative_path
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        issues.append(f"{relative_path} is missing or invalid JSON: {error}")
        return

    actual = document.get("version")
    if actual != expected:
        issues.append(
            f"{relative_path} top-level version is {actual!r}, expected {expected!r}"
        )

    packages = document.get("packages")
    if not isinstance(packages, dict):
        return
    for package_path in package_paths:
        package = packages.get(package_path)
        if not isinstance(package, dict):
            issues.append(f"{relative_path} missing packages[{package_path!r}]")
            continue
        actual = package.get("version")
        if actual != expected:
            issues.append(
                f"{relative_path} packages[{package_path!r}].version is "
                f"{actual!r}, expected {expected!r}"
            )


def expect_dart_path_lock_versions(
    root: Path,
    relative_path: str,
    expected: str,
    issues: list[str],
    package_names: tuple[str, ...],
) -> None:
    text = read_text(root / relative_path, issues)
    if not text:
        return

    wanted = set(package_names)
    actuals: dict[str, str] = {}
    current_package: str | None = None
    source_is_path = False

    for line in text.splitlines():
        package_match = re.match(r"^  ([A-Za-z0-9_]+):\s*$", line)
        if package_match:
            current_package = package_match.group(1)
            source_is_path = False
            continue
        if current_package in wanted and line.strip() == "source: path":
            source_is_path = True
            continue
        if current_package in wanted and source_is_path:
            version_match = re.match(r'^    version: "([^"]+)"\s*$', line)
            if version_match:
                actuals[current_package] = version_match.group(1)

    for package_name in package_names:
        actual = actuals.get(package_name)
        if actual is None:
            issues.append(
                f"{relative_path} missing path package version for {package_name}"
            )
        elif actual != expected:
            issues.append(
                f"{relative_path} path package {package_name} version is "
                f"{actual!r}, expected {expected!r}"
            )


def expect_cargo_lock_path_versions(
    root: Path,
    relative_path: str,
    expected: str,
    issues: list[str],
    package_names: tuple[str, ...],
) -> None:
    text = read_text(root / relative_path, issues)
    if not text:
        return

    wanted = set(package_names)
    actuals: dict[str, str] = {}
    chunks = re.split(r"(?m)(?=^\[\[package\]\]$)", text)

    for chunk in chunks:
        name_match = re.search(r'^name = "([^"]+)"$', chunk, flags=re.MULTILINE)
        if not name_match:
            continue
        package_name = name_match.group(1)
        if package_name not in wanted:
            continue
        if re.search(r"^source = ", chunk, flags=re.MULTILINE):
            continue
        version_match = re.search(
            r'^version = "([^"]+)"$', chunk, flags=re.MULTILINE
        )
        if version_match:
            actuals[package_name] = version_match.group(1)

    for package_name in package_names:
        actual = actuals.get(package_name)
        if actual is None:
            issues.append(
                f"{relative_path} missing path package version for {package_name}"
            )
        elif actual != expected:
            issues.append(
                f"{relative_path} path package {package_name} version is "
                f"{actual!r}, expected {expected!r}"
            )


def git_path_changed(root: Path, relative_path: str) -> bool:
    try:
        unstaged = subprocess.run(
            ["git", "diff", "--quiet", "--", relative_path],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        staged = subprocess.run(
            ["git", "diff", "--cached", "--quiet", "--", relative_path],
            cwd=root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return False
    return unstaged.returncode == 1 or staged.returncode == 1


def validate(root: Path) -> int:
    root = root.resolve()
    issues: list[str] = []

    version = read_text(root / "VERSION", issues).strip()
    if not version:
        issues.append("VERSION is empty")
    elif not SEMVER_RE.match(version):
        issues.append(f"VERSION is not SemVer-compatible: {version!r}")

    if version:
        expect_regex(
            root,
            "Cargo.toml",
            r"^\[workspace\.package\].*?^version\s*=\s*\"([^\"]+)\"",
            version,
            "workspace package version",
            issues,
        )
        expect_regex(
            root,
            "bindings/python/pyproject.toml",
            r"^\[project\].*?^version\s*=\s*\"([^\"]+)\"",
            version,
            "project version",
            issues,
        )
        expect_regex(
            root,
            "bindings/java/driver/build.gradle",
            r"^version\s*=\s*'([^']+)'",
            version,
            "Gradle version",
            issues,
        )
        expect_regex(
            root,
            "bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBDriver.java",
            r'DRIVER_VERSION\s*=\s*"([^"]+)"',
            version,
            "DRIVER_VERSION",
            issues,
        )
        expect_regex(
            root,
            "bindings/java/dbeaver-extension/build.gradle",
            r"^version\s*=\s*'([^']+)'",
            version,
            "Gradle version",
            issues,
        )
        expect_regex(
            root,
            "bindings/java/dbeaver-extension/META-INF/MANIFEST.MF",
            r"^Bundle-Version:\s*([^\r\n]+)",
            version,
            "Bundle-Version",
            issues,
        )
        expect_regex(
            root,
            "bindings/java/dbeaver-extension/META-INF/MANIFEST.MF",
            r"lib/decentdb-jdbc-([0-9A-Za-z.+-]+)\.jar",
            version,
            "embedded JDBC jar version",
            issues,
        )
        expect_regex(
            root,
            "bindings/dart/dart/pubspec.yaml",
            r"^version:\s*([^\r\n]+)",
            version,
            "pubspec version",
            issues,
        )
        expect_regex(
            root,
            "bindings/dart/flutter/pubspec.yaml",
            r"^version:\s*([^\r\n]+)",
            version,
            "pubspec version",
            issues,
        )
        expect_regex(
            root,
            "bindings/dart/flutter/android/build.gradle",
            r"^version\s*=\s*'([^']+)'",
            version,
            "Gradle version",
            issues,
        )
        expect_regex(
            root,
            "bindings/dart/flutter/ios/decentdb_flutter.podspec",
            r"^\s*s\.version\s*=\s*'([^']+)'",
            version,
            "podspec version",
            issues,
        )
        expect_regex(
            root,
            "bindings/dart/flutter/example/pubspec.yaml",
            r"^version:\s*([^\r\n]+)",
            version,
            "reference app version",
            issues,
        )
        expect_dart_path_lock_versions(
            root,
            "bindings/dart/flutter/pubspec.lock",
            version,
            issues,
            ("decentdb",),
        )
        expect_dart_path_lock_versions(
            root,
            "bindings/dart/flutter/example/pubspec.lock",
            version,
            issues,
            ("decentdb", "decentdb_flutter"),
        )
        expect_dart_path_lock_versions(
            root,
            "bindings/dart/examples/console/pubspec.lock",
            version,
            issues,
            ("decentdb",),
        )
        expect_dart_path_lock_versions(
            root,
            "bindings/dart/examples/console_complex/pubspec.lock",
            version,
            issues,
            ("decentdb",),
        )
        expect_dart_path_lock_versions(
            root,
            "bindings/dart/examples/flutter_desktop/pubspec.lock",
            version,
            issues,
            ("decentdb",),
        )
        expect_dart_path_lock_versions(
            root,
            "tests/bindings/dart/pubspec.lock",
            version,
            issues,
            ("decentdb",),
        )
        expect_json_version(
            root,
            "bindings/node/decentdb/package.json",
            version,
            issues,
            package_paths=(),
        )
        expect_json_version(
            root,
            "bindings/node/decentdb/package-lock.json",
            version,
            issues,
        )
        expect_json_version(
            root,
            "bindings/node/knex-decentdb/package.json",
            version,
            issues,
            package_paths=(),
        )
        expect_json_version(
            root,
            "bindings/node/knex-decentdb/package-lock.json",
            version,
            issues,
            package_paths=("", "../decentdb"),
        )
        expect_cargo_lock_path_versions(
            root,
            "benchmarks/rust-baseline/Cargo.lock",
            version,
            issues,
            ("decentdb", "libpg_query_sys"),
        )
        expect_regex(
            root,
            "docs/user-guide/benchmarks.md",
            r"^\| DecentDB \|\s*([^|]+?)\s*\| Workspace package version \|",
            version,
            "benchmark version stamp",
            issues,
        )
        expect_regex(
            root,
            "design/FUTURE_WINS.md",
            r"public release in this repository is `([^`]+)`",
            version,
            "current public release marker",
            issues,
        )
        expect_regex(
            root,
            "design/FUTURE_WINS.md",
            r"bucket after `([^`]+)` only when scope is explicitly accepted",
            version,
            "vNext base release marker",
            issues,
        )

    changelog = root / "docs/about/changelog.md"
    changelog_text = read_text(changelog, issues)
    if changelog_text and "Unreleased" not in changelog_text:
        current_heading = f"## [{version}]"
        if current_heading not in changelog_text:
            issues.append(
                "docs/about/changelog.md has neither an Unreleased bucket nor "
                f"a {current_heading} release heading"
            )

    if git_path_changed(root, "CHANGELOG.md"):
        issues.append(
            "CHANGELOG.md changed; release notes must be edited in "
            "docs/about/changelog.md"
        )

    if issues:
        print("Release metadata validation failed:")
        for issue in issues:
            print(f"- {issue}")
        return 1

    print(f"Release metadata validation passed for {version}.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        default=".",
        help="repository root (default: current directory)",
    )
    args = parser.parse_args()
    return validate(Path(args.root))


if __name__ == "__main__":
    sys.exit(main())
