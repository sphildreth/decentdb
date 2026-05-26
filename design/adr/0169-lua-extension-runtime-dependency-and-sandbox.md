# ADR 0169: Lua Extension Runtime Dependency And Sandbox
**Date:** 2026-05-21
**Status:** Accepted

## Context

`design/FUTURE_WINS.md` tracks a Lua extension runtime and package model as the
final candidate for additional 2.6.0 feature work. The companion spec,
`design/_archive/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`, requires a runtime dependency,
build strategy, sandbox boundary, resource-limit model, and cross-target policy
before implementation.

The extension runtime must let applications add SQL-visible behavior without
turning DecentDB into a native plugin host. Extension code must not gain access
to WAL, pager, B+Tree, catalog internals, process execution, native modules,
network, filesystem, or database mutation APIs.

## Decision

DecentDB will implement the Lua extension runtime with **Lua 5.4 semantics
through `mlua` using vendored Lua** behind DecentDB-owned runtime traits.

### 1. Runtime dependency

Native builds use `mlua` with Lua 5.4 and vendored Lua enabled. The runtime
crate is an implementation detail. Public Rust, C ABI, CLI, and binding APIs
must not expose `mlua` types.

The selected native feature direction is:

```text
mlua = { features = ["lua54", "vendored", "send"] }
```

The implementation must avoid optional `mlua` features that expand the runtime
surface unless this ADR or the Lua extension spec requires them. Native Lua
modules, unrestricted package loading, direct serde-based dynamic conversion,
and cross-thread Lua state sharing are not part of the public contract.

### 2. Build and release policy

Official native 2.6.0 artifacts include Lua extension support by default.
Embedders may build without Lua through the `lua-extensions` cargo feature if
they need a smaller or stricter binary.

Browser/WASM artifacts keep the same package catalog, manifest validation,
trust, and inspection data model, but do not execute Lua in 2.6.0. This is an
intentional target policy: DecentDB should not advertise browser-side execution
until the browser runtime has an equivalently audited Lua 5.4 backend and
resource-limit story. When Lua execution is unavailable, SQL invocation returns
an explicit unsupported-runtime error rather than silently omitting the feature.

### 3. DecentDB-owned runtime boundary

The engine owns an internal abstraction similar to:

```rust
pub(crate) trait ExtensionRuntime {
    fn validate(&self, package: &ExtensionPackage) -> Result<ValidationReport>;
    fn invoke_scalar(
        &mut self,
        function: &ExtensionFunction,
        args: &[Value],
    ) -> Result<Value>;
    fn invoke_table(
        &mut self,
        function: &ExtensionFunction,
        args: &[Value],
    ) -> Result<ExtensionRowStream>;
    fn create_aggregate(
        &mut self,
        function: &ExtensionFunction,
    ) -> Result<ExtensionAggregateState>;
    fn compare_collation(
        &mut self,
        collation: &ExtensionCollation,
        left: &str,
        right: &str,
    ) -> Result<Ordering>;
}
```

SQL planning, expression evaluation, catalog code, C ABI code, and bindings call
DecentDB extension APIs, not `mlua` APIs.

### 4. Lua state ownership

Lua state is scoped to a DecentDB connection and installed package hash. There
is no global mutable Lua state shared across database handles. Runtime caches
must invalidate when the enabled package version/hash changes.

### 5. Sandbox contract

The runtime exposes only a small safe host API:

- selected base Lua functions needed for ordinary pure computation;
- `string`;
- `table`;
- deterministic `math` helpers, excluding random by default;
- UTF-8 helpers if they can be exposed without filesystem/process access;
- DecentDB-owned `ddb.*` constructors and wrappers.

The following are denied by default:

- `io`;
- `os`;
- `debug`;
- `package.loadlib`;
- unrestricted `require`;
- `dofile`;
- `loadfile`;
- environment-variable access;
- filesystem access;
- network access;
- process execution;
- native module loading;
- direct database handles;
- database writes from extension code.

### 6. Resource limits and cancellation

Every Lua invocation runs under explicit limits:

- instruction or step budget;
- memory allocation budget;
- maximum returned string size;
- maximum returned BLOB size;
- maximum table-valued rows;
- maximum table-valued row bytes;
- maximum aggregate state size;
- maximum collation comparison budget;
- recursion depth limit where supported by the runtime boundary;
- cancellation checks integrated with existing statement cancellation paths.

Resource-limit and sandbox failures are SQL errors that name the extension and
function. They must not panic, poison process state, or leave transactions in a
different state than ordinary statement errors.

## Rationale

`mlua` provides a maintained Rust binding with Lua 5.4 and vendored Lua support,
which avoids depending on system Lua installations in release artifacts. Hiding
the crate behind DecentDB-owned traits preserves the option to change the
implementation later.

Vendoring Lua keeps Linux, macOS, Windows, CI, and binding packages aligned.
The DecentDB-owned runtime trait layer keeps the public contract independent
from the selected Rust binding and leaves room for target-specific backend work
without exposing third-party runtime types.

## Consequences

- Native release artifacts gain a vendored Lua dependency.
- The implementation must maintain a no-Lua build path for embedders that
  disable the feature.
- Browser/wasm artifacts retain lifecycle metadata but do not execute Lua until
  a target-specific runtime decision is accepted.
- Security tests must verify denied modules and capabilities.
- Runtime resource limits become part of the public extension contract.

## Alternatives Considered

1. **Use system Lua.** Rejected because official artifacts would depend on host
   package managers and platform-specific library discovery.
2. **Use LuaJIT.** Rejected because the spec targets Lua 5.4 semantics and
   portable official builds.
3. **Use a pure Rust scripting language.** Rejected because the roadmap item is
   specifically a Lua package model and Lua is familiar as an embeddable
   extension language.
4. **Expose host-language callbacks instead.** Rejected because each binding
   would grow a different extension model.
5. **Execute Lua in browser/wasm artifacts immediately.** Rejected for 2.6.0
   because shipping a second target runtime without equivalent resource-limit
   validation would weaken the extension trust contract.

## Validation Requirements

Implementation is not complete until tests cover:

- native build with Lua enabled;
- build with Lua disabled;
- no-Lua and browser/wasm builds expose lifecycle metadata but reject execution
  with an explicit unsupported-runtime error;
- denied `io`, `os`, `debug`, `package.loadlib`, `dofile`, and `loadfile`;
- denied arbitrary `require`;
- CPU/step limit failures;
- memory limit failures where supported;
- returned string/BLOB size failures;
- table-valued row and row-byte limit failures;
- aggregate state limit failures;
- collation comparison budget failures;
- cancellation during extension execution;
- Lua runtime errors converted to SQL errors;
- no panic crossing Rust or C ABI boundaries.

## References

- `design/FUTURE_WINS.md`
- `design/_archive/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`
- `design/adr/0118-rust-ffi-panic-safety.md`
- Lua 5.4 Reference Manual: https://www.lua.org/manual/5.4/
- `mlua` crate documentation: https://docs.rs/crate/mlua/latest
