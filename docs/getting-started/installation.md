# Installation

## Prerequisites

- Nim compiler (>= 1.6.0)
- libpg_query (for SQL parsing)

## From Source

### 1. Install Nim

Follow the official [Nim installation guide](https://nim-lang.org/install.html).

### 2. Clone the Repository

```bash
git clone https://github.com/sphildreth/decentdb.git
cd decentdb
```

### 3. Build DecentDb

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
