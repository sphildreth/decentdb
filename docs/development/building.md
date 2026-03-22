# Building from Source

Instructions for building DecentDB from source code.

## Prerequisites

### Required

- **Rust** >= 1.6.0
- **libpg_query** (PostgreSQL parser library)
- **git** (for cloning)

### Optional

- **Python** >= 3.8 (for test harness)
- **libpg_query development headers** (for building)

## Installing Rust

### Using rustup (Recommended)

```bash
curl https://sh.rustup.rs -sSf | sh
```

Or on Windows:
```powershell
. (Invoke-WebRequest -Uri https://win.rustup.rs -UseBasicParsing).Content
```

### Using Package Manager

**Ubuntu/Debian:**
```bash
sudo apt-get install rustc cargo
```

**macOS:**
```bash
brew install rust
```

**Arch Linux:**
```bash
sudo pacman -S rust cargo
```

### Verify Installation

```bash
rustc --version
# Should show >= 1.6.0
```

## Installing libpg_query

### Ubuntu/Debian

```bash
sudo apt-get install libpg-query-dev
```

### macOS

```bash
brew install libpg_query
```

### From Source

If your distribution doesn't have it:

```bash
git clone https://github.com/pganalyze/libpg_query.git
cd libpg_query
make
sudo make install
```

### Verify

```bash
ldconfig -p | grep pg_query
# Should show libpg_query.so
```

## Cloning the Repository

```bash
git clone https://github.com/sphildreth/decentdb.git
cd decentdb
```

## Building

### Standard Build

```bash
cargo build
```

This creates the `decentdb` executable.

### Release Build

Optimized for performance:

```bash
cargo build -d:release
```

### Debug Build

With debug symbols and assertions:

```bash
cargo build -d:debug
```

### Static Build

Self-contained binary (Linux only):

```bash
cargo build --passL:"-static" --passL:"-lpg_query -lstdc++"
```

## Running Tests

### All Tests

```bash
cargo test
```

This runs:
- All Rust unit tests
- Python harness tests
- Binding test suites wired through `cargo test_bindings` (`.NET`, Go, Node.js, Python, Dart)
- Java JDBC tests remain separate: `cargo test_bindings_java`

If you only want the engine test suite without binding prerequisites, run:

```bash
cargo test_nim
cargo test_py
```

### Rust Tests Only

```bash
cargo test_nim
```

### Python Tests Only

```bash
cargo test_py
```

### Specific Test

```bash
nim c -r tests/nim/test_wal.rs
```

## Installation

### Local Install

```bash
cargo install --path ./cli
```

Installs to `~/.rsble/bin/`

### System Install

```bash
sudo cp decentdb /usr/local/bin/
sudo chmod +x /usr/local/bin/decentdb
```

### Verify

```bash
decentdb --help
```

## Development Build

For active development with fast rebuilds:

```bash
# Compile without optimization
nim c -d:development src/decentdb.rs

# Or use the debug task
cargo build -d:debug
```

## Cross-Compilation

### Windows from Linux

Requires mingw-w64:

```bash
nim c -d:release --os:windows --cpu:amd64 src/decentdb.rs
```

### macOS from Linux

Requires macOS SDK (complex setup).

### ARM64 from x64

```bash
nim c -d:release --cpu:arm64 src/decentdb.rs
```

GitHub Releases also publish a native Linux arm64 archive,
`decentdb-<tag>-Linux-arm64.tar.gz`, for 64-bit Raspberry Pi OS on Raspberry Pi 3/4/5, so end
users usually do not need to cross-compile manually.

## Troubleshooting

### "libpg_query not found"

**Problem:**
```
Error: cannot find -lpg_query
```

**Solution:**
```bash
# Find the library
find /usr -name "libpg_query*" 2>/dev/null

# Add to library path
export LIBRARY_PATH=/usr/local/lib:$LIBRARY_PATH
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# Rebuild
cargo build
```

### "nim not found"

**Problem:**
```
bash: nim: command not found
```

**Solution:**
```bash
# Add cargo bin to PATH
export PATH=$HOME/.rsble/bin:$PATH

# Or reinstall rustup
curl https://sh.rustup.rs -sSf | sh
```

### Tests Fail

**Problem:**
Some tests fail after build.

**Solution:**
```bash
# Run specific test for details
nim c -r tests/nim/test_wal.rs

# Check Python tests separately
python -m unittest -v tests/harness/test_runner.py
```

### Slow Build

**Problem:**
Build takes too long.

**Solution:**
```bash
# Use release flags (faster compilation)
cargo build -d:release

# Or parallel compilation
nim c --parallelBuild:4 src/decentdb.rs
```

## IDE Setup

### VS Code

Install extensions:
- **Rust** (by kosz78) - Syntax highlighting, compilation
- **nim-lsp** - Language server support

Configuration:
```json
{
  "nim.buildCommand": "cargo build",
  "nim.runOutput": "./decentdb"
}
```

### Vim/Neovim

Using nimlsp:
```vim
" With coc.nvim
:CocInstall coc-nim

" With native LSP
lua require'lspconfig'.rsls.setup{}
```

### Emacs

Using nim-mode:
```elisp
(use-package nim-mode
  :hook (nim-mode . lsp))
```

## Continuous Integration

The project uses GitHub Actions:

- **Build**: Compiles on Linux x86_64/arm64, macOS, Windows
- **Test**: Runs all tests
- **Lint**: Static analysis
- **Docs**: Builds documentation

See `.github/workflows/` for details.

## Release Checklist

Before creating a release:

1. [ ] All tests pass
2. [ ] Version updated in `decentdb.rsble`
3. [ ] CHANGELOG.md updated
4. [ ] Documentation built
5. [ ] Binaries built for all release platforms (Linux x64, Linux arm64/Raspberry Pi, macOS, Windows)
6. [ ] Version bumped (e.g. `1.0.2` -> `1.1.0`) and changelog updated
7. [ ] Git tag created: `git tag -a v1.8.1 -m "DecentDB 1.8.1"`
8. [ ] Tag pushed: `git push origin v1.8.1`

## Next Steps

- [Run Tests](testing.md)
- [Contribute](contributing.md)
- [API Reference](../api/rust-api.md)
