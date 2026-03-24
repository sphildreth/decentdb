# DecentDB Versioning Guide

This guide defines how DecentDB version jumps work and which files must be
updated when the project version changes.

## 1. Versioning policy

DecentDB uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html):

- **Major (`X.0.0`)** for breaking changes to public APIs, the on-disk format, binding behavior, or other compatibility boundaries.
- **Minor (`X.Y.0`)** for backwards-compatible feature additions.
- **Patch (`X.Y.Z`)** for backwards-compatible fixes, packaging adjustments, CI fixes, and documentation updates that do not change the public contract.

### Public release line

The current public DecentDB release line begins at `v2.0.0`.

## 2. Current version-bearing files

When DecentDB's release version changes, update the release-facing metadata that exists in the Rust repository today.

### Core Rust workspace

- `Cargo.toml`  
  Update `[workspace.package].version`. The Rust crates inherit from the workspace version.

### Python binding

- `bindings/python/pyproject.toml`  
  Update `[project].version`.

### Dart binding

- `bindings/dart/dart/pubspec.yaml`  
  Update `version`.

### Node bindings

- `bindings/node/decentdb/package.json`
- `bindings/node/decentdb/package-lock.json`
- `bindings/node/knex-decentdb/package.json`
- `bindings/node/knex-decentdb/package-lock.json`

For the Node packages, update both the manifest and the lockfile's top-level package version entries.

### Documentation

- `docs/about/changelog.md`  
  Add or update release notes under `Unreleased` or under the new version heading, depending on the release process being used.

### Release automation

- `.github/workflows/nuget.yml`  
  The .NET/NuGet packages do **not** hard-code their package versions in the `.csproj` files. CI derives them from Git tags in the format:
  - `vX.Y.Z`
  - `vX.Y.Z-rc.N`

## 3. Files that usually do **not** need a version bump

Do **not** bump unrelated example/demo app versions just to match the DecentDB release unless they explicitly surface the shipped DecentDB version to users.

Examples:

- `bindings/dart/examples/**/pubspec.yaml`
- dependency versions inside `package-lock.json`

Those files may contain version numbers, but they are not automatically part of the DecentDB release version.

## 4. Recommended version-bump procedure

1. Decide the next version according to SemVer.
2. Update `docs/about/changelog.md`.
3. Update the Rust, Python, Dart, and Node version metadata.
4. Re-scan the repository for stale release-version strings.
5. Validate that package metadata still parses and that lockfiles stayed aligned.
6. Create the release tag when the project is ready to publish.

## 5. Node-specific procedure

Prefer using npm to update Node package versions instead of hand-editing the lockfiles.

```bash
cd bindings/node/decentdb
npm version --no-git-tag-version X.Y.Z

cd ../knex-decentdb
npm version --no-git-tag-version X.Y.Z
npm install --package-lock-only --ignore-scripts
```

The second `npm install --package-lock-only --ignore-scripts` step refreshes the lockfile metadata for the local `file:../decentdb` dependency after the underlying package version changes.

## 6. Validation checklist

After a version bump, verify:

- `Cargo.toml` has the intended workspace version.
- Python, Dart, and Node package metadata all reflect the same DecentDB release version.
- `docs/about/changelog.md` explains the release and any important versioning context.
- No stale old-version references remain in the release-facing files.
- The NuGet workflow still matches the current tag format.

Useful commands:

```bash
cargo metadata --no-deps --format-version 1 >/dev/null

rg 'OLD_VERSION|vOLD_VERSION' \
  Cargo.toml \
  bindings/python/pyproject.toml \
  bindings/dart/dart/pubspec.yaml \
  bindings/node/decentdb/package.json \
  bindings/node/decentdb/package-lock.json \
  bindings/node/knex-decentdb/package.json \
  bindings/node/knex-decentdb/package-lock.json \
  docs/about/changelog.md \
  .github/workflows/nuget.yml
```

Replace `OLD_VERSION` with the version you are replacing.

## 7. Release tag rules

When publishing, use Git tags with a leading `v`:

- Stable release: `v2.0.0`
- Release candidate: `v2.1.0-rc.1`

The current NuGet workflow converts those tags into package versions without the leading `v`.
