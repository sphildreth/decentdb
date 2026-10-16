#!/usr/bin/env python3
"""Build and exercise every desktop binding with the selected stable toolchains."""

from __future__ import annotations

import os
from pathlib import Path
import platform
import subprocess
import sys
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]
ENV = os.environ.copy()


def run(*args: str, cwd: str = ".") -> None:
    print(f"[{cwd}] {' '.join(args)}", flush=True)
    subprocess.run(args, cwd=ROOT / cwd, env=ENV, check=True)


def main() -> None:
    system = platform.system()
    name = {"Windows": "decentdb.dll", "Darwin": "libdecentdb.dylib"}.get(
        system, "libdecentdb.so"
    )
    release = ROOT / "target" / "release"
    native = str(release / name)
    ENV.update(
        DECENTDB_NATIVE_LIB=native,
        DECENTDB_NATIVE_LIB_PATH=native,
        PATH=f"{release}{os.pathsep}{ENV['PATH']}",
    )
    run("cargo", "build", "--workspace", "--release", "--locked")
    run("cargo", "check", "--workspace", "--all-targets", "--all-features", "--locked")
    smoke = ROOT / ".tmp" / "dependency-platform" / "c-smoke"
    smoke.parent.mkdir(parents=True, exist_ok=True)
    if system == "Windows":
        smoke = smoke.with_suffix(".exe")
    linker_args = [] if system == "Windows" else [f"-Wl,-rpath,{release}"]
    run(
        "gcc" if system == "Windows" else "cc", "-Wall", "-Wextra", "-Werror",
        f"-I{ROOT / 'include'}", "tests/bindings/c/smoke.c", f"-L{release}",
        "-ldecentdb", *linker_args, "-o", str(smoke),
    )
    run(str(smoke))
    run(sys.executable, "-m", "pip", "install", "-e", "bindings/python[test]")
    run(sys.executable, "scripts/build_python_fastdecode.py")
    run(sys.executable, "-m", "pytest", "-q", "bindings/python/tests")
    run("dotnet", "build", "bindings/dotnet/DecentDB.NET.sln", "-c", "Release", "-warnaserror")
    run("dotnet", "test", "--solution", "bindings/dotnet/DecentDB.NET.sln", "-c", "Release", "--no-build")
    for project in (
        "tests/bindings/dotnet/Smoke/Smoke.csproj",
        "benchmarks/python_embedded_compare/helpers/litedb/LiteDbBenchmark.csproj",
    ):
        run("dotnet", "build", project, "-c", "Release", "-warnaserror")
    for module in ("bindings/go/decentdb-go", "bindings/go/examples/bun"):
        run("go", "test", "-count=1", "./...", cwd=module)
        run("go", "vet", "./...", cwd=module)
    npm = "npm.cmd" if system == "Windows" else "npm"
    for package in ("bindings/node/decentdb", "bindings/node/knex-decentdb"):
        run(npm, "ci", cwd=package)
        run(npm, "test", cwd=package)
    run(npm, "ci", cwd="bindings/web")
    run(npm, "run", "typecheck", cwd="bindings/web")
    run(npm, "run", "build", cwd="bindings/web")
    dart = "dart.bat" if system == "Windows" else "dart"
    for package in (
        "bindings/dart/dart",
        "bindings/dart/examples/console",
        "bindings/dart/examples/console_complex",
        "bindings/dart/examples/flutter_desktop",
        "tests/bindings/dart",
    ):
        run(dart, "pub", "get", cwd=package)
        run(dart, "analyze", "--fatal-infos", cwd=package)
    run(dart, "test", cwd="bindings/dart/dart")
    run(dart, "run", "smoke.dart", cwd="tests/bindings/dart")
    sdk = ROOT / ".tmp" / "dependency-platform" / "dbeaver"
    sdk.mkdir(parents=True, exist_ok=True)
    archive = sdk / "dbeaver.zip"
    urllib.request.urlretrieve(
        "https://github.com/dbeaver/dbeaver/releases/download/26.2.2/"
        "dbeaver-ce-26.2.2-windows-x86_64.zip",
        archive,
    )
    with zipfile.ZipFile(archive) as bundle:
        bundle.extractall(sdk)
    ENV["DBEAVER_PLUGIN_DIR"] = str(sdk / "dbeaver" / "plugins")
    # JDBC's override names the JNI bridge, whereas Python/Dart use the C ABI library.
    ENV.pop("DECENTDB_NATIVE_LIB", None)
    gradle = "gradlew.bat" if system == "Windows" else "./gradlew"
    run(
        gradle, "--no-daemon", "--project-cache-dir", str(ROOT / ".tmp/gradle-platform"),
        "--warning-mode", "fail", ":driver:test",
        ":dbeaver-extension:jar", f"-PnativeLibDir={release}", cwd="bindings/java",
    )
    run("javac", "--release", "25", "tests/bindings/java/Smoke.java")
    run("java", "--enable-native-access=ALL-UNNAMED", "-cp", "tests/bindings/java", "Smoke")


if __name__ == "__main__":
    main()
