# Installation

## Prerequisites

- Nim compiler (>= 1.6.0)
- libpg_query (for SQL parsing)

## GitHub release binaries

Prefer a prebuilt download? GitHub Releases publish native archives for:

- `decentdb-<tag>-Linux-x64.tar.gz`
- `decentdb-<tag>-Linux-arm64.tar.gz` — for 64-bit Raspberry Pi OS on Raspberry Pi 3/4/5 and other aarch64 Linux systems
- `decentdb-<tag>-macOS-x64.tar.gz`
- `decentdb-<tag>-Windows-x64.zip`

Each archive contains the DecentDB CLI plus the native C API library. Extract the archive and place
`decentdb` (or `decentdb.exe`) on your `PATH`.

## From Source

### 1. Install Nim

Follow the official [Nim installation guide](https://nim-lang.org/install.html).

### 2. Clone the Repository

```bash
git clone https://github.com/sphildreth/decentdb.git
cd decentdb
```

### 3. Build DecentDB

```bash
nimble build
```

This creates the `decentdb` executable in the project root.

### 4. Run Tests

```bash
nimble test
```

## Docker (Optional)

At the moment, this repository does not ship a maintained `Dockerfile`.

If Docker support is needed, either add a `Dockerfile` (and wire it into CI) or use a dev container tailored to your environment.

## System Installation

To install system-wide:

```bash
nimble install
```

Or copy the binary manually:

```bash
cp decentdb /usr/local/bin/
```

## Verify Installation

```bash
decentdb --help
```

You should see the help output with available commands.

## Language Bindings

To use DecentDB from other languages, build the shared C API library:

```bash
nimble build_lib
```

This produces `build/libc_api.so` (Linux), `build/libc_api.dylib` (macOS), or `build/c_api.dll` (Windows).

See the binding-specific guides for setup:
- [.NET](../api/dotnet.md)
- [Go](../api/go.md)
- [Python](../api/python.md)
- [Node.js](../api/node.md)
