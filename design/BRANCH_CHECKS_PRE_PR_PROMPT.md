# DecentDB Pre-PR Checklist Prompt

You are an AI coding agent assisting with the DecentDB repository. Your task
is to verify that the current branch is fully ready to be merged into `main`
and published as a new release.

Rigorously review the workspace against the following checklist. If any steps
are incomplete, perform the necessary updates or instruct the user if a
decision requires human input.

---

### 1. Rust Build Quality

All of the following must pass with zero errors and zero warnings:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

### 2. Binding Test Suites

Build the native cdylib first, then run each binding's tests:

```bash
cargo build -p decentdb
```

| Binding | Commands |
|---------|----------|
| **Python** | `cd bindings/python && pip install -e . && pytest -v tests/` |
| **.NET** | `cd bindings/dotnet && dotnet test DecentDB.NET.sln -v minimal` |
| **Go** | `go run ./tests/bindings/go/smoke.go` |
| **Java** | `export JAVA_HOME=/usr/lib/jvm/java-21-openjdk && export PATH="$JAVA_HOME/bin:$PATH" && javac --enable-preview --release 21 tests/bindings/java/Smoke.java && java --enable-preview --enable-native-access=ALL-UNNAMED -cp tests/bindings/java Smoke` |
| **Node.js** | `bash tests/bindings/node/build.sh && node tests/bindings/node/smoke.js && cd bindings/node/decentdb && npm test` |
| **Dart** | `cd tests/bindings/dart && dart pub get && dart run smoke.dart` |
| **C** | `bash tests/bindings/c/run.sh` |

### 3. Harness & Benchmarks

```bash
python tests/harness/runner.py tests/harness/scenarios/short_crash.json
python tests/harness/runner.py tests/harness/scenarios/soak_storage.json
cargo bench -p decentdb --bench release_metrics
```

Confirm that benchmark results have not significantly regressed compared to
previous runs.

### 4. Documentation & Architecture

- **ADRs**: Ensure an Architectural Decision Record exists in `design/adr/` if
  the branch changes persistent file formats, WAL semantics, concurrency
  handling, the C ABI contract, or core system architecture. Read
  `design/adr/README.md` for guidance.
- **Docs**: Verify that new features, API changes, or behavioral modifications
  are documented under `docs/`.
- **Rustdoc**: `cargo doc --workspace --no-deps` must build cleanly.
- **Site**: `mkdocs build --config-file mkdocs.yml --site-dir site` must
  succeed.
- **README**: If significant features were added, ensure `README.md` reflects
  them.

### 5. Versioning

**Bump rule**: New features → minor bump (e.g., `2.0.0` → `2.1.0`). Bug fixes
only → patch bump (e.g., `2.0.0` → `2.0.1`).

1. Edit the `VERSION` file at the repository root with the new version string.
2. Run `scripts/bump_version.sh` — it propagates the version to all bindings
   (Rust, Python, Node.js, Java, DBeaver, Dart) and regenerates `Cargo.lock`.
3. Update Node.js lockfiles: `cd bindings/node/decentdb && npm install` and
   `cd bindings/node/knex-decentdb && npm install`.
4. Verify no stale references remain:

```bash
grep -rn "OLD_VERSION" . \
  --exclude-dir=target --exclude-dir=.git --exclude-dir=node_modules \
  --exclude-dir=.dart_tool --exclude-dir=vendor --exclude-dir=site \
  --exclude-dir=bin --exclude-dir=obj --exclude-dir=build \
  --exclude-dir=.tmp
```

Replace `OLD_VERSION` with the previous version string. Any remaining hits
(outside of changelogs and historical references) must be updated.

### 6. Changelog

Update `docs/about/changelog.md` under the new version heading. Include
details on fixes, changed behaviors, and additions across the core engine and
all language bindings. The root `CHANGELOG.md` is a pointer to this file and
does not need separate updates.
