## Production Browser Runtime Contract
**Date:** 2026-05-20
**Status:** Accepted

### Context

ADR 0161 shipped the browser WASM/OPFS v1 runtime with one Dedicated Worker
owner and no multi-tab coordination contract. The `WIN_PRODUCTION_BROWSER_RUNTIM`
Future Win requires production-grade ownership, capability gating, service-worker
policy, diagnostics, tiered browser support, and explicit non-fallback behavior.

### Decision

DecentDB browser production runtime adopts the following contract.

1. **Support tiers and capability contract**
- Tier 1 support targets Chrome and Edge with all required probes passing.
- Firefox is a candidate tier until equivalent CI/runtime evidence is promoted.
- Runtime is capability-gated (Dedicated Worker, BroadcastChannel, Web Locks,
  OPFS directory, OPFS sync access handle behavior) rather than user-agent
  gated.
- Unsupported environments fail with stable browser error codes before opening a
  database handle.

2. **Ownership coordination model**
- Coordination model: **Dedicated Worker owner guarded by Web Locks and
  discovered/routed through BroadcastChannel per logical database path**.
- All tabs route requests through one active owner runtime. A non-owner tab
  forwards RPC requests over BroadcastChannel to the owner page, which forwards
  them to the Dedicated Worker that owns the OPFS access handles.
- SharedWorker ownership is rejected for this phase because the OPFS
  synchronous access handle behavior required by ADR 0161 is not consistently
  available in SharedWorker contexts in the current browser test environment.
- No silent fallback to IndexedDB/localStorage/memory for production durability
  claims.

3. **Service worker policy**
- Service workers cannot own DecentDB browser database handles.
- Browser sync/background activity is owner-routed from supported page/worker
  contexts.

4. **Stale-owner and recovery contract**
- Owner identity and liveness are surfaced in browser diagnostics.
- Open/request timeout is bounded and returns stable retryable owner timeout
  errors.
- Owner-loss recovery is explicit and never creates two write-capable owners for
  one logical path.

5. **Browser sync policy**
- Public browser sync API is owner-routed and shape-compatible with the sync
  roadmap.
- Transport implementation remains explicitly deferred where relay/runtime
  guarantees are not yet complete; deferred behavior uses stable errors/results
  rather than implicit no-ops.

6. **Parser/API parity strategy**
- Browser SQL profile is explicitly named (`browser-app-v1`).
- Tagged browser value parameters are accepted for common app data shapes,
  including binary and typed semantic values.
- Unsupported browser SQL/profile behavior fails with stable profile errors.

7. **Diagnostics contract**
- Browser runtime diagnostics are exposed through `metrics()` and browser system
  views (`sys.browser_runtime`, `sys.browser_owner`, `sys.browser_storage`,
  `sys.browser_sync`) in wasm builds.
- Native hot paths remain unaffected; browser diagnostics stay in web/wasm layers.

8. **Package and CI policy**
- Browser smoke/perf coverage is split into tier-1 and candidate matrices.
- Chrome/Edge tier runs are release-blocking when claimed; candidate runs are
  non-blocking until promoted.

### Consequences

- Browser runtime semantics are explicit for multi-tab ownership, unsupported
  environments, and service-worker behavior.
- Dedicated Worker, BroadcastChannel, Web Locks, and OPFS capability become hard
  preconditions for production browser claims.
- Browser sync transport remains intentionally deferred unless explicitly enabled
  by follow-up sync relay slices.
- Browser-only runtime complexity is isolated to `bindings/web` and wasm-facing
  code paths, keeping native read/write hot paths unchanged.

### References

- `design/_archive/WIN_PRODUCTION_BROWSER_RUNTIM.md`
- `design/FUTURE_WINS.md`
- `design/adr/0161-browser-wasm-opfs-runtime.md`
- `docs/api/wasm.md`
- `bindings/web/`
