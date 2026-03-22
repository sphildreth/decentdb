# DecentDB Pre-PR Checklist Prompt

You are an AI coding agent assisting with the DecentDB repository. Your task is to verify that the current branch is fully ready to be merged into `main` and published as a new release. 

Please rigorously review the workspace against the following checklist. If any steps are incomplete, perform the necessary updates or instruct the user if a decision requires human input.

### 1. Tests & Build Quality
- **Run branch check script**: Run DecentDB tests (`/home/steven/source/decentdb/scripts/do_branch_checks_pre_pr.sh`) all must be successful, no errors and no warnings.
- **No Warnings**: Verify that there are no new build warnings when compiling the DecentDB core engine or any of its language bindings.

### 2. Benchmarks & Performance
- **Benchmarks**: Confirm that changes have not significantly degraded DecentDB's query execution performance. Run `nimble bench_embedded_pipeline` and check for regressions against `/benchmarks/embedded_compare/data/bench_summary.json`.

### 3. Documentation & Architecture
- **ADRs**: Ensure you have created an Architectural Decision Record (ADR) in `/design/adr/` if your changes alter persistent file formats, concurrency handling, or core system architectures. Read `/design/adr/README.md` for more direction on when to create an ADR.
- **Documentation**: Verify that any new features, API changes, or behavioral modifications are documented under the `/docs/` directory.
- **README**: Ensure the repository root `README.md` is updated. If significant features were added, they must be highlighted in the "## Features" section.

### 4. Versioning
- **Bump Version Numbers**: If new features were added to DecentDB or its bindings, this requires a **minor** version bump (e.g., `1.7.0` -> `1.8.0`). Otherwise, a **patch** bump is sufficient (e.g., `1.7.0` -> `1.7.1`).
- **Exhaustive Version Update**: A version bump MUST update the hardcoded version string in all of the following locations:
  - **Nim Core**: `decentdb.nimble` and the exported literal in `src/c_api.nim`
  - **Python**: `bindings/python/pyproject.toml` (and `bindings/python/decentdb.egg-info/PKG-INFO`)
  - **Node**: `bindings/node/decentdb/package.json` and `bindings/node/knex-decentdb/package.json` (plus their respective `package-lock.json` files)
  - **Java / DBeaver**: `bindings/java/driver/build.gradle`, `bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBDriver.java`, `bindings/java/dbeaver-extension/build.gradle`, `bindings/java/dbeaver-extension/META-INF/MANIFEST.MF`
  - **Dart**: `bindings/dart/dart/pubspec.yaml`, `bindings/dart/examples/console/pubspec.lock`, `bindings/dart/dart/lib/src/database.dart`, and `bindings/dart/native/decentdb.h`
  - **Examples**: Verify versioned asset paths like `examples/java/run.sh`
  - **Documentation**: Any explicit mentions of the current version in `docs/user-guide/comparison.md`, `docs/user-guide/dbeaver.md`, `docs/development/building.md`, `docs/development/contributing.md`
  - *Pro Tip*: Use the following command to systematically hunt down any straggling versions: 
    `grep -rn "OLD_VERSION" . --exclude-dir=build --exclude-dir=.git --exclude-dir=nimcache --exclude-dir=venv --exclude-dir=.venv`

### 5. Changelog
- **Update Changelog**: Ensure `/docs/about/changelog.md` has been properly updated under the `[Unreleased]` or newly bumped version heading. Include details on fixes, changed behaviors, and additions across the core engine and all language bindings.