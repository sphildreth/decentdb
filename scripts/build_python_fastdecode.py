#!/usr/bin/env python3
"""Build the optional decoder for the active Python ABI and host compiler."""

from pathlib import Path
import sys

from setuptools import Distribution, Extension
from setuptools.command.build_ext import build_ext


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    package = root / "bindings" / "python" / "decentdb"
    extension = Extension(
        "_fastdecode",
        sources=[str(package / "_fastdecode.c")],
        include_dirs=[str(root / "include")],
    )
    distribution = Distribution({"name": "decentdb-fastdecode", "ext_modules": [extension]})
    command = build_ext(distribution)
    command.build_lib = str(package)
    command.build_temp = str(root / ".tmp" / "python-fastdecode" / sys.implementation.cache_tag)
    command.force = True
    command.ensure_finalized()
    command.run()


if __name__ == "__main__":
    main()
