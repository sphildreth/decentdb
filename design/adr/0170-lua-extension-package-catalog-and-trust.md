# ADR 0170: Lua Extension Package Catalog And Trust Model
**Date:** 2026-05-21
**Status:** Accepted

## Context

Lua extensions introduce a code-loading surface into an embedded database.
DecentDB needs package installation, database-level enablement, connection-level
trust, content hashing, persistence, and inspection semantics before any SQL
function can execute safely.

The design must preserve a hard rule: opening a database must never auto-run
untrusted extension code.

## Decision

DecentDB will separate **installation**, **database enablement**, and
**connection trust**.

### 1. Package format

The package layout is:

```text
extension_name/
  decentdb-extension.toml
  main.lua
  install.sql
  uninstall.sql
  tests/
    behavior.sql
    main_test.lua
  README.md
```

Only the manifest and declared source/test/documentation files participate in
package validation. Hidden files, generated build artifacts, and undeclared
source files do not affect runtime behavior.

### 2. Manifest authority

`decentdb-extension.toml` is the authoritative contract for:

- package name and version;
- API version;
- entry file;
- exported SQL functions;
- argument and return types;
- determinism and NULL handling;
- table-valued output schemas;
- aggregate lifecycle exports;
- collation exports and version metadata;
- persisted-schema eligibility metadata;
- requested permissions;
- runtime limits;
- package signing metadata.

`install.sql` may create ordinary SQL objects, but it cannot define
SQL-visible Lua function signatures that are missing from the manifest.

### 3. Package hashing

The package content hash is SHA-256 over a canonical package representation:

- normalized manifest bytes;
- normalized relative file paths for included source files;
- exact source bytes for included files;
- package format version.

The hash excludes local absolute paths, file mtimes, permissions, and build
machine metadata. The same package content must hash identically on Linux,
macOS, Windows, and CI.

### 4. Installation storage

`decentdb extension install --db <path> <package_dir>` stores a canonical copy
of the manifest, Lua source files, package metadata, validation report, and
content hash inside DecentDB-owned internal catalog storage in the main
database file.

No sidecar source store is used. Bundles and support artifacts include
extension package records by reading the database-owned catalog.

Internal extension catalog objects are hidden from ordinary schema listings,
dump output by default, SQLite compatibility catalog views, and user table
enumeration. Administrative CLI/API commands may inspect them explicitly.

### 5. Enablement and SQL surface

`CREATE EXTENSION name` is transactional database enablement for an already
installed package. It does not read from the filesystem and does not grant trust
to every connection.

`DROP EXTENSION name` transactionally disables the SQL-visible extension.

Package removal is an explicit administrative operation:

```bash
decentdb extension purge --db app.ddb text_tools --confirm
```

`CREATE EXTENSION FROM '/path'` is rejected because SQL text must not
become a filesystem code-loading surface.

### 6. Package Signatures

Package signing is part of the complete package model.

The package signature covers the canonical package hash. The accepted signing
algorithm is Ed25519 with explicit key identifiers. Signatures establish package
publisher identity; they do not replace the connection-level allowlist. Trust
anchors are supplied by application, CLI, or binding configuration, not by
untrusted database content.

Unsigned packages may be installed only when the caller explicitly allows
unsigned local development packages. Documentation examples may use that
development override so they can be copied directly from the repository;
distributable third-party packages should be signed.

Signature validation failures are package validation/install errors before any
Lua code is executable.

### 7. Connection trust

Even when a package is installed and enabled, extension execution is allowed
only when the current connection explicitly allows the package name and content
hash.

CLI shape:

```bash
decentdb exec \
  --db app.ddb \
  --allow-extension text_tools@sha256:abc123 \
  --sql "SELECT slugify(title) FROM posts"
```

Rust configuration uses the same name/hash model. Binding APIs wrap the same
engine policy rather than reimplementing trust logic.

Hash mismatch, missing allowlist entry, disabled package, missing installed
package, or manifest/API incompatibility is a SQL error before any Lua code is
executed.

### 8. Transaction Boundaries

Install, enable, disable, and purge operations are normal durable writes.
Enable and disable participate in SQL transactions. Package install and purge
are administrative APIs/CLI commands and must be atomic from the caller's
perspective.

## Rationale

Separating install, enable, and trust lets databases remain portable without
silently executing code from an untrusted file. Storing canonical source in the
database makes backups and branch/snapshot workflows coherent, while the
connection allowlist gives applications final control over executable code.

Disallowing `CREATE EXTENSION FROM` avoids making SQL injection a code-loading
primitive.

## Consequences

- Extension package content becomes part of the database image.
- Trust decisions remain outside the database file and must be supplied by the
  application, CLI, or binding configuration.
- Internal catalog storage must be durable, hidden, and compatible with branch,
  backup, dump, sync, and support-bundle decisions.
- Administrative tooling needs package validation and stable hash reporting.

## Alternatives Considered

1. **Trust installed packages automatically.** Rejected because opening an
   untrusted database could execute attacker-controlled code.
2. **Store only package paths in the database.** Rejected because paths are not
   portable and make backups non-self-contained.
3. **Allow SQL to install from arbitrary paths.** Rejected because SQL should
   not become a filesystem code-loader.
4. **Use sidecar package storage.** Rejected to keep the backup, branch, and
   bundle story simple.
5. **Defer signed packages.** Rejected because the complete package model needs
   publisher identity in addition to exact content hashes.

## Validation Requirements

Implementation is not complete until tests cover:

- stable package hashes across repeated validation;
- valid signed packages pass signature validation;
- invalid signatures, unknown signing keys, and tampered package content fail
  validation before installation;
- unsigned package installation requires an explicit development override;
- invalid manifest rejection;
- install persists package metadata/source across reopen;
- `CREATE EXTENSION` enablement is transactional;
- `DROP EXTENSION` disablement is transactional;
- opening a database with installed/enabled packages does not execute Lua;
- no allowlist means no execution;
- hash mismatch blocks execution;
- disabled package blocks execution;
- purge removes installed package content;
- internal extension catalog objects are hidden from ordinary schema listings;
- branch, snapshot, and backup paths preserve or intentionally reject extension
  package metadata according to their existing contracts.

## References

- `design/FUTURE_WINS.md`
- `design/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`
- `design/adr/0153-branch-metadata-identity-and-user-surface.md`
- `design/adr/0157-branch-diff-restore-and-merge-semantics.md`
