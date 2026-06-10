# Runtime Tracing API And Binding Contract
**Date:** 2026-06-09
**Status:** Accepted

### Decision

Runtime tracing, runtime advisors, and Doctor projection will use SQL views as
the primary cross-language read surface. Rust owns the implementation and typed
configuration. The C ABI and maintained bindings expose configuration and
access in the smallest compatible shape needed for the initial feature.

The first implementation should prefer open-time configuration over broad
runtime mutation. Runtime reconfiguration may be added only if it can be done
without data races, surprising scope changes, or hot-path regressions.

Rust will define typed configuration and snapshot models similar to:

```rust
pub struct RuntimeTracingConfig {
    pub enabled: bool,
    pub slow_query: SlowQueryTraceConfig,
    pub lock_wait: LockWaitTraceConfig,
    pub index_usage: IndexUsageTraceConfig,
    pub sessions: SessionTraceConfig,
    pub sql_text: SqlTextTraceConfig,
    pub memory_budget_bytes: usize,
}
```

The implementation may expose Rust APIs for:

- reading the active runtime tracing configuration;
- resetting runtime trace buffers;
- taking owned runtime trace snapshots;
- generating runtime advisor reports;
- refreshing Doctor reports.

These APIs must return owned data detached from live engine locks. They must
use typed errors and structured diagnostics, not panics.

The C ABI v1 direction is conservative:

- read runtime facts through ordinary SQL queries against `sys.*` views;
- configure tracing through existing open-options mechanisms where possible;
- avoid adding per-view C structs for slow queries, lock waits, index usage, or
  sessions in the first implementation;
- add direct C ABI functions only when SQL plus open options are insufficient.

If direct C ABI functions are required, the preferred shape is versioned JSON
for configuration and snapshots rather than many binding-specific structs:

```c
ddb_result ddb_runtime_tracing_set(ddb_database_t *db, const char *json_config);
ddb_result ddb_runtime_tracing_get(ddb_database_t *db, char **json_config_out);
ddb_result ddb_runtime_tracing_reset(ddb_database_t *db, const char *scope);
ddb_result ddb_runtime_trace_snapshot_json(ddb_database_t *db, const char *json_options, char **json_out);
ddb_result ddb_runtime_advisor_report_json(ddb_database_t *db, const char *json_options, char **json_out);
```

Any new direct C ABI function requires a C ABI version bump, header
documentation, binding ABI expectation updates, and binding smoke tests in the
same implementation slice.

Maintained bindings must not implement their own tracing engines, lock wait
classifiers, index usage trackers, or advisor rules. They should:

- pass runtime tracing configuration to the native engine;
- query the documented `sys.*` views;
- expose Doctor/advisor JSON only when the binding already has a diagnostics
  helper or when a common helper can be implemented consistently;
- preserve DecentDB's redaction defaults;
- document that tracing is disabled by default;
- avoid binding-specific telemetry upload or export behavior.

CLI and Decent Bench should use Rust engine snapshot/report APIs directly where
practical, and may also use SQL views for integration tests. They must report
active tracing configuration when presenting benchmark or advisor output.

Browser, mobile, and WASM surfaces follow the same contract. Runtime tracing is
local diagnostic state. It must not upload telemetry, persist support bundles,
or increase bundle/API surface beyond the feature's configured support tier
without a follow-up ADR.

### Rationale

The C ABI is the shared boundary for maintained bindings, but every C ABI
addition has long-term cost. SQL views are already available to all bindings
and are the most portable read surface for runtime diagnostics.

Rust typed configuration keeps engine invariants explicit. Bindings can map
friendly option names to the native open options without inventing their own
runtime semantics.

Versioned JSON is an acceptable escape hatch for complex snapshots or advisor
reports because Doctor and structured diagnostics already use JSON at
boundaries. It avoids freezing many C structs before the feature matures.

Keeping advisor rules in Rust prevents Python, Node, Go, Java, .NET, Dart, and
WASM packages from drifting in their interpretation of slow queries, lock
waits, index usage, and safety classifications.

### Alternatives Considered

1. **Add complete typed C structs for every runtime view.** Rejected for v1.
   SQL rows already provide cross-language access, and many structs would be
   expensive to stabilize early.
2. **Expose only Rust APIs.** Rejected. Runtime diagnostics are useful from
   every binding and from the CLI.
3. **Let each binding add its own tracing hooks.** Rejected. This fragments
   semantics and risks breaking the one-writer/many-readers model.
4. **Make runtime reconfiguration the primary control surface.** Rejected for
   the first implementation. Open-time configuration is easier to reason about;
   runtime mutation can follow after synchronization and overhead are proven.
5. **Use external telemetry export as the binding contract.** Rejected.
   External export is out of scope and requires separate privacy and transport
   decisions.

### Trade-offs

- SQL-first access means some bindings may need helper code to convert rows
  into idiomatic objects.
- Open-time configuration is less convenient for interactive debugging than
  runtime toggles, but it is safer for the first implementation.
- JSON escape hatches are less type-safe than C structs, but they reduce ABI
  churn while the feature matures.
- Keeping all advisor logic in Rust centralizes correctness but requires
  binding tests to focus on projection rather than local logic.

### Consequences

- `include/decentdb.h` changes are optional for the first slice. If added, they
  require an ABI version bump and binding updates.
- Binding smoke tests must at minimum cover disabled-by-default behavior,
  enabling slow-query tracing through the supported config path, querying
  `sys.slow_queries`, and verifying parameter redaction.
- CLI Doctor/advisor features should not depend on binding-specific code.
- Docs must describe both SQL access and any native configuration options.
- Any future broad direct C ABI telemetry surface, support-bundle export, or
  external telemetry integration requires a follow-up ADR.

### References

- `design/WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md`
- `design/adr/0163-operational-sys-metrics.md`
- `design/adr/0179-cross-process-public-contract-bindings-and-diagnostics.md`
- `design/adr/0185-rich-structured-error-diagnostics-contract.md`
- `include/decentdb.h`
- `tests/bindings/`

