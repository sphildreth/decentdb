# ADR 0169: Lua Extension Runtime Dependency And Sandbox
**Date:** 2026-05-21
**Status:** Accepted

## Context

`design/FUTURE_WINS.md` tracks a Lua extension runtime and package model as the
final candidate for additional 2.6.0 feature work. The companion spec,
`design/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`, requires a runtime dependency,
build strategy, sandbox boundary, resource-limit model, and wasm/browser policy
before implementation.

The extension runtime must let applications add SQL-visible behavior without
turning DecentDB into a native plugin host. Extension code must not gain access
to WAL, pager, B+Tree, catalog internals, process execution, native modules,
network, filesystem, or database mutation APIs.

## Decision

DecentDB will implement the first Lua extension runtime with **Lua 5.4 semantics
through `mlua` using vendored Lua** behind DecentDB-owned runtime traits.

### 1. Runtime dependency

Native builds use `mlua` with Lua 5.4 and vendored Lua enabled. The runtime
crate is an implementation detail. Public Rust, C ABI, CLI, and binding APIs
must not expose `mlua` types.

The selected feature direction is:

```text
mlua = { features = ["lua54", "vendored"] }
```

The implementation must avoid optional `mlua` features that expand the runtime
surface unless a later ADR accepts them. In particular, the first runtime does
not require async execution, native Lua modules, serde-based dynamic conversion,
or cross-thread Lua state sharing.

### 2. Build and release policy

Official native 2.6.0 artifacts include Lua extension support by default.
Embedders may build without Lua through a cargo feature if they need a smaller
or stricter binary.

WASM/browser builds do not execute Lua extensions in this phase. They must
compile with stable unsupported-capability errors rather than trying to load or
emulate Lua in the browser runtime. Browser support requires a later ADR because
the sandbox, package delivery, and resource-limit model are different in wasm.

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
}
```

SQL planning, expression evaluation, catalog code, C ABI code, and bindings call
DecentDB extension APIs, not `mlua` APIs.

### 4. Lua state ownership

Lua state is scoped to a DecentDB connection and installed package hash. There
is no global mutable Lua state shared across database handles. Runtime caches
must invalidate when the enabled package version/hash changes.

### 5. Sandbox contract

The first runtime exposes only a small safe host API:

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
- recursion depth limit where supported by the runtime boundary;
- cancellation checks integrated with existing statement cancellation paths.

Table-valued row limits and aggregate state limits are not part of the first
runtime slice because those function kinds are deferred by ADR 0173.

Resource-limit and sandbox failures are SQL errors that name the extension and
function. They must not panic, poison process state, or leave transactions in a
different state than ordinary statement errors.

## Rationale

`mlua` provides a maintained Rust binding with Lua 5.4 and vendored Lua support,
which avoids depending on system Lua installations in release artifacts. Hiding
the crate behind DecentDB-owned traits preserves the option to change the
implementation later.

Vendoring Lua keeps Linux, macOS, Windows, CI, and binding packages aligned.
Disabling Lua execution in wasm/browser for the first slice avoids making the
browser runtime responsible for another sandbox and package-delivery model in
the same release.

## Consequences

- Native release artifacts gain a vendored Lua dependency.
- The implementation must maintain a no-Lua build path for embedders that
  disable the feature.
- WASM/browser builds expose unsupported errors for extension execution until a
  future browser extension ADR is accepted.
- Security tests must verify denied modules and capabilities.
- Runtime resource limits become part of the public extension contract.

## Alternatives Considered

1. **Use system Lua.** Rejected because official artifacts would depend on host
   package managers and platform-specific library discovery.
2. **Use LuaJIT.** Rejected for v1 because the spec targets Lua 5.4 semantics
   and portable official builds.
3. **Use a pure Rust scripting language.** Rejected because the roadmap item is
   specifically a Lua package model and Lua is familiar as an embeddable
   extension language.
4. **Expose host-language callbacks instead.** Rejected because each binding
   would grow a different extension model.
5. **Support Lua in browser immediately.** Rejected because browser extension
   code delivery and sandbox policy need separate design.

## Validation Requirements

Implementation is not complete until tests cover:

- native build with Lua enabled;
- build with Lua disabled;
- wasm/browser unsupported behavior compiles and returns stable errors;
- denied `io`, `os`, `debug`, `package.loadlib`, `dofile`, and `loadfile`;
- denied arbitrary `require`;
- CPU/step limit failures;
- memory limit failures where supported;
- returned string/BLOB size failures;
- cancellation during extension execution;
- Lua runtime errors converted to SQL errors;
- no panic crossing Rust or C ABI boundaries.

## References

- `design/FUTURE_WINS.md`
- `design/WIN_LUA_EXTENSION_RUNTIME_SPEC.md`
- `design/adr/0118-rust-ffi-panic-safety.md`
- Lua 5.4 Reference Manual: https://www.lua.org/manual/5.4/
- `mlua` crate documentation: https://docs.rs/crate/mlua/latest
