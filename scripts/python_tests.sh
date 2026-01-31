#!/bin/bash
set -e

# Build the native library
echo "Building native library..."
nimble build_lib

# Locate the library
REPO_ROOT=$(pwd)
LIB_PATH="$REPO_ROOT/build/libc_api.so"

if [ ! -f "$LIB_PATH" ]; then
    echo "Error: Native library not found at $LIB_PATH"
    exit 1
fi

export DECENTDB_NATIVE_LIB="$LIB_PATH"
export PYTHONPATH="$REPO_ROOT/bindings/python"

# Setup virtual environment
VENV_DIR="$REPO_ROOT/venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install --upgrade pip
    "$VENV_DIR/bin/pip" install sqlalchemy pytest
fi

# Run tests
echo "Running Python tests..."
cd bindings/python
"$VENV_DIR/bin/python3" -m pytest -v tests
