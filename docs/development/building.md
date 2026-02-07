# Building from Source

Instructions for building DecentDB from source code.

## Prerequisites

### Required

- **Nim** >= 1.6.0
- **libpg_query** (PostgreSQL parser library)
- **git** (for cloning)

### Optional

- **Python** >= 3.8 (for test harness)
- **libpg_query development headers** (for building)

## Installing Nim

### Using choosenim (Recommended)

```bash
curl https://nim-lang.org/choosenim/init.sh -sSf | sh
```

Or on Windows:
```powershell
. (Invoke-WebRequest -Uri https://nim-lang.org/choosenim/init.ps1 -UseBasicParsing).Content
```

### Using Package Manager

**Ubuntu/Debian:**
```bash
sudo apt-get install nim
```

**macOS:**
```bash
brew install nim
```

**Arch Linux:**
```bash
sudo pacman -S nim
```

### Verify Installation

```bash
nim --version
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
nimble build
```

This creates the `decentdb` executable.

### Release Build

Optimized for performance:

```bash
nimble build -d:release
```

### Debug Build

With debug symbols and assertions:

```bash
nimble build -d:debug
```

### Static Build

Self-contained binary (Linux only):

```bash
nimble build --passL:"-static" --passL:"-lpg_query -lstdc++"
```

## Running Tests

### All Tests

```bash
nimble test
```

This runs:
- All Nim unit tests
- Python harness tests

### Nim Tests Only

```bash
nimble test_nim
```

### Python Tests Only

```bash
nimble test_py
```

### Specific Test

```bash
nim c -r tests/nim/test_wal.nim
```

## Installation

### Local Install

```bash
nimble install
```

Installs to `~/.nimble/bin/`

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
nim c -d:development src/decentdb.nim

# Or use the debug task
nimble build -d:debug
```

## Cross-Compilation

### Windows from Linux

Requires mingw-w64:

```bash
nim c -d:release --os:windows --cpu:amd64 src/decentdb.nim
```

### macOS from Linux

Requires macOS SDK (complex setup).

### ARM64 from x64

```bash
nim c -d:release --cpu:arm64 src/decentdb.nim
```

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
nimble build
```

### "nim not found"

**Problem:**
```
bash: nim: command not found
```

**Solution:**
```bash
# Add nimble bin to PATH
export PATH=$HOME/.nimble/bin:$PATH

# Or reinstall choosenim
curl https://nim-lang.org/choosenim/init.sh -sSf | sh
```

### Tests Fail

**Problem:**
Some tests fail after build.

**Solution:**
```bash
# Run specific test for details
nim c -r tests/nim/test_wal.nim

# Check Python tests separately
python -m unittest -v tests/harness/test_runner.py
```

### Slow Build

**Problem:**
Build takes too long.

**Solution:**
```bash
# Use release flags (faster compilation)
nimble build -d:release

# Or parallel compilation
nim c --parallelBuild:4 src/decentdb.nim
```

## IDE Setup

### VS Code

Install extensions:
- **Nim** (by kosz78) - Syntax highlighting, compilation
- **nim-lsp** - Language server support

Configuration:
```json
{
  "nim.buildCommand": "nimble build",
  "nim.runOutput": "./decentdb"
}
```

### Vim/Neovim

Using nimlsp:
```vim
" With coc.nvim
:CocInstall coc-nim

" With native LSP
lua require'lspconfig'.nimls.setup{}
```

### Emacs

Using nim-mode:
```elisp
(use-package nim-mode
  :hook (nim-mode . lsp))
```

## Continuous Integration

The project uses GitHub Actions:

- **Build**: Compiles on Linux, macOS, Windows
- **Test**: Runs all tests
- **Lint**: Static analysis
- **Docs**: Builds documentation

See `.github/workflows/` for details.

## Release Checklist

Before creating a release:

1. [ ] All tests pass
2. [ ] Version updated in `decentdb.nimble`
3. [ ] CHANGELOG.md updated
4. [ ] Documentation built
5. [ ] Binaries built for all platforms
6. [ ] Version bumped (e.g. `0.0.1` -> `0.0.2`) and changelog updated
7. [ ] Git tag created: `git tag -a v0.1.1 -m "DecentDB 0.1.1 (NuGet 1.0.0-rc.1)"`
8. [ ] Tag pushed: `git push origin v0.1.1`

## Next Steps

- [Run Tests](testing.md)
- [Contribute](../contributing.md)
- [API Reference](../../api/nim-api.md)
