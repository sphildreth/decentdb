# Query Plan Cache: C ABI Surface, Default-On, Additive Open Options, And Binding Contract

**Date:** 2026-06-13
**Status:** Accepted
**Related spec:** [`../WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md`](../WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md)
**Companion ADRs:** [0190](./0190-query-plan-cache-scope-key-and-lifecycle.md), [0191](./0191-query-plan-cache-memory-accounting-and-eviction.md), [0192](./0192-query-plan-cache-security-generation-and-tde.md)

## Decision

The connection-local plan cache ships through the C ABI as two
**additive** open options on the existing open-with-options functions:

```text
plan_cache_enabled=true|false
plan_cache_max_bytes=<bytes>
```

The C ABI version is **not** bumped. Old binaries that do not set
these options get the new default behavior (connection-local caching
enabled, default 256 KiB per ADR 0191). The
`ddb_db_prepare`, `ddb_db_execute`, and `ddb_db_execute_prepared`
function signatures are unchanged. The `PreparedStatement` lifetime
and ownership contract is unchanged.

Bindings that wrap the C ABI continue to inherit the cache
transparently. Bindings that want to expose the cache configuration
to applications should add the two options to their connection
options; bindings that do not expose them get the default behavior
for free. Bindings must not implement their own plan cache on top
of the C ABI.

The `decentdb plan-cache` CLI subcommands (ADR 0190) are the
recommended operator surface for inspecting and resetting the
cache. Bindings should document whether and how they expose these
subcommands or their equivalents.

## Rationale

A C ABI version bump is a binding-blast-radius event. Every
maintained binding (Python, Go, Node, .NET, Java, Dart) and every
downstream user that has linked against a previous C ABI version
must rebuild and revalidate against the new version. The cost is
high and the value of bumping the version for Phase 1 is zero,
because the new options are *additive*: existing call sites that
do not know about them continue to work, and the new options
extend the open-options parser without changing its existing
behavior.

The default-on decision is the harder call. The conservative
default in many embedded databases is to keep the engine's
behavior byte-identical across minor upgrades, which would mean
defaulting the cache to `false` and letting the user opt in.
That default is wrong here for two reasons:

1. **The cache is correctness-preserving.** Disabling the cache
   and enabling the cache produce the *same query results*. The
   `Disabling the plan cache must produce identical query
   results to enabling it` line in the spec's §11 Compatibility
   Rules is enforced by the test matrix. A binding that depends
   on absolute no-cache behavior is depending on a property the
   engine does not promise to preserve across versions.

2. **The performance win requires default-on.** Every benchmark
   that compares DecentDB to SQLite or PostgreSQL on
   prepared-statement throughput will exercise the default. A
   default-off cache would not show up in those benchmarks and
   would not deliver the win to the most common workloads.

The compromise is:

- default-on for the cache;
- additive C ABI options for opt-out;
- documented behavior change in the spec and in the release
  notes;
- a release note line that tells binary authors who care about
  no-cache behavior to set `plan_cache_enabled=false`;
- a maintained-binding contract that all bindings inherit the
  default and document it.

This is the same trade-off SQLite made with its
`SQLITE_DBCONFIG_DEFENSIVE` flag and PostgreSQL made with
`plan_cache_mode`. DecentDB's default is consistent with the
industry default for prepared-statement plan caching.

The decision to not bump the C ABI version also means the
process-global cache (Phase 2) is a future ADR. A process-global
cache that changes how database handles share state across
connections will almost certainly require a C ABI version bump,
and that bump is the right place to draw the line.

## C ABI contract

### Open options

The C ABI open-options parser already accepts key=value pairs.
Two new keys are added:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `plan_cache_enabled` | bool | `true` | Enable connection-local plan caching for this `Db` handle. |
| `plan_cache_max_bytes` | u64 | `262144` (256 KiB) | Maximum plan cache memory for this handle, in bytes. See ADR 0191 for the rationale. |

Invalid values produce a structured error of subcode
`DDB_E_PLAN_CACHE_CONFIG` (new), following the structured-error
contract in ADR 0185.

### Function signatures

`ddb_db_prepare`, `ddb_db_execute_prepared`, and the related
prepared-statement functions are unchanged. The cache is fully
internal: the caller passes the same SQL text and parameters as
before, and the engine returns the same result types.

### New accessors

Two new C ABI functions are added for diagnostic access:

```c
// Returns 0 on success, non-zero error code on failure.
int32_t ddb_plan_cache_summary(
    ddb_db_t *db,
    ddb_plan_cache_summary_t *out
);

// Resets (evicts all entries and zeros counters) the
// connection-local plan cache. Returns 0 on success.
int32_t ddb_plan_cache_flush(ddb_db_t *db);
```

The `ddb_plan_cache_summary_t` struct is a fixed-layout POD with
fields matching the `sys.plan_cache_summary` columns (see the
spec's §6.2). The struct is forward-compatible: new fields are
appended at the end and existing offsets are preserved.

These accessors are not part of the C ABI version bump because
they are *new* functions, not changes to existing functions.
Adding new functions is the additive direction; existing
binaries that do not call them are unaffected.

## Maintained binding contract

The maintained bindings (Python, Go, Node, .NET, Java, Dart,
WASM/browser) inherit the following contract:

1. **Default behavior is enabled.** Bindings that do not
   surface the new open options still get the cache enabled
   for the C ABI handles they open.

2. **Bindings that surface connection options add the two new
   options.** The option names follow the binding's existing
   convention (snake_case, camelCase, etc.). Bindings should
   document the default values and the behavior change in
   their release notes.

3. **Bindings must not implement their own plan cache on top
   of the C ABI.** The engine owns the cache. A binding that
   implements its own cache is responsible for its own
   correctness.

4. **Bindings that already provide wrapper-level auto-reprepare
   may keep that behavior.** The Rust `PreparedStatement`
   invalidation contract is unchanged (ADR 0190 §"Cache key"),
   and binding-level auto-reprepare is independent of the
   engine's plan cache.

5. **Bindings that want to expose `sys.plan_cache` and
   `sys.plan_cache_summary` to applications** should document
   them as ordinary read-only views, not as a binding-specific
   API. Bindings must not attempt to parse the SQL text
   column (the engine deliberately omits it) or reverse the
   `cache_key_hash` (which is not stable across engine
   versions).

6. **WASM/browser bindings** document the per-worker budget
   caveat (spec §7.4): a multi-worker tab can multiply the
   total plan-cache memory by the worker count, and bindings
   that spawn multiple workers should lower the default
   `plan_cache_max_bytes` accordingly.

## C ABI version

The C ABI version is **not** bumped for Phase 1. The next C ABI
version bump is reserved for one of the following events:

- Phase 2 (process-global cache) that changes how `Db` handles
  share state across connections.
- A change to the lifetime or ownership semantics of
  `ddb_statement_t` or `ddb_db_prepare`.
- A change to the result-row ownership contract of
  `ddb_db_execute` or related functions.

None of these events is in Phase 1.

## Alternatives considered

1. **Bump the C ABI version.** Rejected for Phase 1. The new
   options are additive and the new accessors are new
   functions, not changes to existing ones. Bumping the
   version imposes a binding-rebuild cost with zero
   correctness benefit.
2. **Default-off.** Rejected. The cache is
   correctness-preserving and the performance win requires
   default-on. Bindings that depend on no-cache behavior are
   a vanishingly small audience, and they can opt out with
   `plan_cache_enabled=false`.
3. **Add a `plan_cache_flush` SQL command instead of a C ABI
   accessor.** Rejected. The C ABI accessor is needed by
   bindings that want to expose cache management to
   applications; the `PRAGMA flush_plan_cache` surface is for
   SQL callers and the CLI is for operators.
4. **Require bindings to call `ddb_plan_cache_summary` to
   surface cache state.** Rejected. Bindings can simply
   surface the `sys.plan_cache_summary` view as a normal
   query, which is cheaper and more idiomatic.
5. **Defer the C ABI surface to Phase 2.** Rejected. The C
   ABI surface is the binding-shippability requirement. A
   cache that is only reachable from Rust is invisible to the
   maintained bindings and to downstream users.

## Trade-offs

- **Default-on is a silent behavior change for old C ABI
  binaries.** This is the biggest trade-off and the one the
  release notes must call out. The alternative (default-off)
  is worse for nearly every host.
- **No C ABI version bump means Phase 2 must be designed to
  either keep the additive pattern or carry its own
  version-bump ADR.** This is the right place to draw the
  version-bump line.
- **The two new C ABI accessors are new symbols in the
  shared library.** They are additive and do not change
  existing exports. Bindings that dynamically link against
  the C ABI do not need to be rebuilt to keep working.
- **Bindings must update their connection-options
  documentation.** This is a small per-binding cost. The
  release notes will list the bindings that have already
  shipped the update.

## Consequences

- `crates/decentdb/src/c_api.rs` grows two new functions
  (`ddb_plan_cache_summary`, `ddb_plan_cache_flush`) and
  one new POD struct (`ddb_plan_cache_summary_t`).
- `include/decentdb.h` adds the two function declarations and
  the struct definition. The header's `DDB_C_ABI_VERSION`
  constant is unchanged.
- The C ABI open-options parser is extended to accept
  `plan_cache_enabled` and `plan_cache_max_bytes`.
- A new structured-error subcode `DDB_E_PLAN_CACHE_CONFIG`
  is added to the structured-error registry (ADR 0185).
- `docs/api/configuration.md` documents the two new options
  and the default-on behavior, including the migration
  guidance for binaries that need no-cache behavior.
- The release notes for the version that ships the cache
  call out the default-on behavior and the `false` opt-out.
- All maintained bindings add a one-line "plan cache is on by
  default" note to their documentation, with a per-binding
  example of the new connection option.

## References

- `design/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md` §7,
  §10, §11, §13
- `design/adr/0190-query-plan-cache-scope-key-and-lifecycle.md`
- `design/adr/0191-query-plan-cache-memory-accounting-and-eviction.md`
  (256 KiB default)
- `design/adr/0192-query-plan-cache-security-generation-and-tde.md`
- `design/adr/0185-rich-structured-error-diagnostics-contract.md`
  (structured error contract for the new
  `DDB_E_PLAN_CACHE_CONFIG` subcode)
- `include/decentdb.h`
- `crates/decentdb/src/c_api.rs`
- `docs/api/configuration.md`
- `docs/api/error-codes.md`
