# ADR 0172: Lua Extension CLI, C ABI, And Binding Contract
**Date:** 2026-05-21
**Status:** Accepted

## Context

Lua extensions must be usable from every maintained binding without each
binding implementing its own Lua runtime or package parser. DecentDB's C ABI is
the shared boundary for Python, Go, Java, Node, Dart, .NET native helpers, and C
smoke tests.

The feature also needs CLI workflows for validation, install, enable, trust, and
testing.

## Decision

DecentDB will expose the complete extension lifecycle and invocation-management
surface through engine-owned Rust APIs, CLI commands, and C ABI JSON
request/response entry points. Bindings wrap the C ABI or Rust APIs and do not
reimplement Lua behavior.

### 1. CLI surface

The CLI owns filesystem package workflows:

```bash
decentdb extension validate ./text_tools
decentdb extension test ./text_tools
decentdb extension install --db app.ddb ./text_tools
decentdb extension list --db app.ddb --format table
decentdb extension show --db app.ddb text_tools --format json
decentdb extension enable --db app.ddb text_tools
decentdb extension disable --db app.ddb text_tools
decentdb extension purge --db app.ddb text_tools --confirm
decentdb extension verify-signature ./text_tools --keyring ./trusted.keys
decentdb extension dependencies --db app.ddb --format table
decentdb extension rebuild --db app.ddb text_tools
```

SQL execution commands require explicit connection trust:

```bash
decentdb exec \
  --db app.ddb \
  --allow-extension text_tools@sha256:abc123 \
  --sql "SELECT slugify('Hello World')"
```

The REPL may accept the same `--allow-extension` option. It must not add a
SQLite-style `.load` command.

### 2. Rust API

The Rust API exposes lifecycle operations through a DecentDB-owned extension
manager, conceptually:

```rust
db.extensions().validate(path)?;
db.extensions().install(path)?;
db.extensions().enable("text_tools")?;
db.extensions().disable("text_tools")?;
db.extensions().purge("text_tools")?;
db.extensions().list()?;
db.extensions().dependencies()?;
db.extensions().rebuild_dependents("text_tools")?;
```

Connection trust is configured through `DbConfig`, not through SQL.

### 3. C ABI baseline

The C ABI surface uses JSON request/response entry points for lifecycle,
dependency, rebuild, validation, and connection allowlist configuration. This
mirrors the binding strategy used by public sync JSON bridges and avoids baking
a large struct graph into the ABI. The JSON bridge is the stable complete ABI
for this feature; typed binding conveniences are layered above it.

Conceptual C ABI:

```c
ddb_extension_validate_json(...);
ddb_extension_install_json(...);
ddb_extension_list_json(...);
ddb_extension_enable_json(...);
ddb_extension_disable_json(...);
ddb_extension_purge_json(...);
ddb_extension_dependencies_json(...);
ddb_extension_rebuild_json(...);
ddb_config_allow_extension(config, "text_tools", "sha256:abc123");
```

Every C ABI function follows ADR 0118 panic-safety rules and returns stable
error codes plus owned JSON/error buffers with explicit free functions.

### 4. Binding policy

Bindings expose extension lifecycle helpers by wrapping the Rust/C ABI engine
surface. They must not:

- parse manifests independently as the authority;
- execute Lua directly;
- compute a different content hash;
- bypass the engine trust policy;
- silently enable extensions on open.

Bindings may offer typed convenience APIs over JSON responses after the C ABI
contract is stable.

### 5. Inspection surfaces

SQL inspection surfaces are read-only:

```sql
SELECT * FROM sys.extensions;
SELECT * FROM sys.extension_functions;
SELECT * FROM sys.extension_validation;
SELECT * FROM sys.extension_dependencies;
SELECT * FROM sys.extension_collations;
```

Raw Lua source is not exposed through `sys.*`. Administrative CLI/API commands
can expose source only through explicit options.

### 6. Documentation and examples

The feature is not complete until user docs explain:

- package layout;
- manifest fields;
- trust model;
- sandbox limits;
- CLI lifecycle;
- binding lifecycle;
- SQL usage;
- browser/wasm behavior;
- scalar function examples;
- table-valued function examples;
- aggregate examples;
- collation examples;
- persisted deterministic expression and index examples;
- package signing examples;
- dependency rebuild workflows.

## Rationale

JSON bridge entry points are the least risky way to give every binding access to
the same engine-owned implementation. The CLI remains the natural place for
package-directory validation and testing because it already has filesystem
access and user-facing diagnostics.

Keeping trust in `DbConfig` prevents SQL text from granting executable-code
permission.

## Consequences

- C ABI grows lifecycle, dependency, rebuild, validation, and config allowlist
  JSON entry points.
- Bindings need smoke tests for install/enable/allow/invoke across scalar,
  table-valued, aggregate, and collation surfaces.
- CLI becomes the primary authoring and validation tool.
- Docs/examples are release-blocking for the feature.
- Typed binding conveniences layer on top of the JSON ABI baseline.

## Alternatives Considered

1. **Only expose extensions in Rust.** Rejected because the runtime must be
   binding-wide.
2. **Give each binding its own manifest parser.** Rejected because it would
   split package and trust semantics.
3. **Design a large typed C ABI first.** Rejected because the JSON bridge is
   already the complete public ABI pattern for complex DecentDB surfaces.
4. **Let SQL grant trust.** Rejected because trust is an application/runtime
   decision, not database content.

## Validation Requirements

Implementation is not complete until tests cover:

- CLI validate/install/list/show/enable/disable/purge;
- CLI signature verification, dependency inspection, and dependent-object
  rebuild;
- CLI `exec` and REPL trust allowlist behavior;
- Rust lifecycle API;
- C ABI JSON allocation/free and panic safety;
- binding smoke tests install and call scalar, table-valued, aggregate, and
  collation extension objects;
- raw source absent from `sys.*`;
- docs example packages validate and execute every supported function kind;
- no binding can execute an enabled extension without allowlist trust.

## References

- `design/FUTURE_WINS.md`
- `design/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`
- `design/adr/0118-rust-ffi-panic-safety.md`
- `design/adr/0167-public-changeset-api.md`
