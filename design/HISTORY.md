# Retired Design Documents

**Status:** Historical index

**Retired:** 2026-10-31

The WIN*/FUTURE_WINS documents were retired after the DDB ALM project became
the source of truth for remaining work. Their remaining goals, design detail,
and acceptance criteria now live on Hamstik DDB work items. Full document text
is attached to the work items listed below, and the last committed revision is
recoverable from git history with the recorded SHA.

Active roadmap and backlog: [Hamstik project DDB](https://hamstik.com/t/sph/projects/DDB).

## Retired documents

| Document | Last commit | Superseding DDB items | Notes |
|---|---|---|---|
| `design/FUTURE_WINS.md` | `c8b3a08` | DDB-51 | Consolidated roadmap; every remaining row mapped to DDB-18..DDB-45. |
| `design/WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md` | `4223211` | DDB-45 | Advanced SQL compatibility epic. |
| `design/WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md` | `d041b91` | DDB-43 | Sync epic; delivered slices 1-8 remain covered by ADRs 0147-0152 and 0166-0168. |
| `design/WIN_PERFORMANCE_IMPROVEMENTS_01.md` | `650a493` | DDB-47, DDB-48, DDB-49 | Completed WIN01; remaining performance follow-ups are separate items. |
| `design/WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md` | `4223211` | DDB-44 | Runtime tracing, advisor, and Doctor epic. |
| `design/_archive/WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md` | `4223211` | DDB-50, DDB-51 | Browser parity delivered; deferred TDE/branch/cancellation work is in DDB-50. |
| `design/_archive/WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md` | `cde4022` | DDB-18 | Write-queue foundation delivered; remaining binding ergonomics are in DDB-18. |
| `design/_archive/WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md` | `4223211` | DDB-51 | Cross-process WAL foundation delivered; platform/limit follow-ups are covered by DDB-23/DDB-30. |
| `design/_archive/WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md` | `4223211` | DDB-47 | Default-fast program delivered; remaining gaps are in DDB-47. |
| `design/_archive/WIN_FULL_TEXT_SEARCH_BM25_SPEC.md` | `4223211` | DDB-22 | FTS/BM25 delivered; hybrid search follow-ups are in DDB-22. |
| `design/_archive/WIN_LUA_EXTENSION_RUNTIME_SPEC.md` | `cde4022` | DDB-36 | Lua runtime delivered; ecosystem follow-ups are in DDB-36. |
| `design/_archive/WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md` | `4223211` | DDB-50, DDB-51 | Mobile runtime delivered; watch lifecycle/device follow-ups are in DDB-50. |
| `design/_archive/WIN_PRODUCTION_BROWSER_RUNTIM.md` | `cde4022` | DDB-43, DDB-50, DDB-51 | Browser runtime delivered; deferred browser work is in DDB-50 and sync transport in DDB-43. |
| `design/_archive/WIN_PRODUCTION_RELAY_SPEC.md` | `cde4022` | DDB-43, DDB-51 | Relay/changesets delivered; deferred security/modes are in DDB-43. |
| `design/_archive/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md` | `4223211` | DDB-46 | Plan-cache Phases 1A/1B/4 delivered; Phases 2/3 are in DDB-46. |
| `design/_archive/WIN_REACTIVE_QUERY_SUBSCRIPTIONS_CHANGE_STREAMS_SPEC.md` | `cde4022` | DDB-35 | Reactive subscriptions delivered; CDC follow-ups are in DDB-35. |
| `design/_archive/WIN_RICH_STRUCTURED_ERRORS_DEVELOPER_DIAGNOSTICS_SPEC.md` | `4223211` | DDB-51 | Structured errors delivered; ADR 0185 and docs are the active contract. |
| `design/_archive/WIN_SQL_PRAGMA_COMPATIBILITY_QUICK_WINS_SPEC.md` | `cde4022` | DDB-45, DDB-29 | SQL/PRAGMA quick wins delivered; advanced SQL is in DDB-45. |
| `design/_archive/WIP_FUTURE_WINS_SUGGESTIONS.md` | `a9de906` | DDB-51 | Superseded by the consolidated roadmap; unpromoted ideas are recorded as out of scope. |

## FUTURE_WINS row map

1→DDB-18, 2→DDB-43, 3→DDB-19, 4→DDB-44, 5→DDB-20, 6→DDB-21, 7→DDB-22, 8→DDB-23, 9→DDB-24, 10→DDB-25, 11→DDB-26, 12→DDB-27, 13→DDB-28, 14→DDB-29, 15→DDB-30, 16→DDB-31, 17→DDB-32, 18→DDB-33, 19→DDB-34, 20→DDB-35, 21→DDB-36, 22→DDB-37, 23→DDB-38, 24→DDB-45, 25→DDB-39, 26→DDB-40, 27→DDB-41, 28→DDB-42

## Delivered context

Delivered foundations are documented by the ADRs and user/API docs they
introduced. The retired specs remain available through the DDB attachments and
`git show <sha>:<path>`; they are not the active contract.

- Structured errors: ADR 0185, `docs/api/error-codes.md`, `docs/user-guide/error-diagnostics.md`.
- Reactive streams: ADR 0164, `docs/api/sql-functions.md`.
- Sync/relay/changesets: ADRs 0147-0152 and 0166-0168, `docs/user-guide/sync/`.
- Browser/mobile: ADRs 0161, 0165, 0181-0183, `docs/api/wasm.md`, `docs/api/dart.md`.
- Lua: ADRs 0169-0173, `docs/user-guide/lua-extensions.md`.
- Full-text/geospatial/security: ADRs 0124-0128, 0174-0176, 0175-0176, user-guide docs.
- Plan cache: ADRs 0190-0194, `docs/user-guide/performance.md`.
- Default-fast/WIN01: ADRs 0112, 0143-0145, 0184, `docs/user-guide/performance.md`, benchmark docs.
- Cross-process WAL: ADRs 0177-0180, `docs/user-guide/write-concurrency.md`.
- Concurrent write ergonomics: ADR 0162, `docs/user-guide/write-concurrency.md`.
