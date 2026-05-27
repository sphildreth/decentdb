# Overview

This is a list of feedback from the prmpt 

> Review the @design/FUTURE_WINS.md. Think about what other additional "Future Wins" this project would benefit from. 

These should be reviewed and aggregated into a single list. Then ranked in priority with higher priority being the largest impact to the user.

## Coding agent #1 suggestions

```
Here are additional Future Wins that would strengthen the roadmap, ordered by adoption impact and alignment with DecentDB's identity:
High-Impact Additions (vNext/vNext+1)
1. Online Non-Blocking DDL
SQLite blocks readers during ALTER TABLE. DecentDB could differentiate sharply with online schema changes that don't interrupt concurrent readers — especially powerful combined with the shipped branch rehearsal workflow (rehearse on a branch, promote with zero downtime). Distinct from item 6 (migration rehearsal) because this is about the engine-level DDL execution, not the workflow around it.
2. Online Backup and Point-in-Time Recovery
Production apps need live backup without stopping writes, and WAL-based PITR to recover from application-level mistakes. SQLite's backup API is basic and doesn't offer incremental or PITR semantics. Distinct from WAL streaming replication (item 17) — backup is about snapshots and recovery, not live standbys.
3. Rich Structured Error Diagnostics
Machine-readable error codes, suggestion hints, doc links, and context-aware messages. SQLite is notorious for cryptic errors. This aligns directly with the "observable" and "agent-friendly" positioning, and pairs naturally with the Doctor/tracing work (item 5). Low implementation cost, high developer experience impact.
Medium-Impact Additions (vNext+1/Later)
4. Page-Level Compression
Transparent page compression (LZ4/Zstd) for storage efficiency. Related to item 4's storage efficiency goal but a distinct technical initiative with its own format, WAL, and recovery implications. DuckDB offers this; SQLite does not natively.
5. Deterministic Reproducibility
Guarantee that identical operation sequences produce byte-identical database images. Powerful for sync verification, branch comparison, bundle integrity, testing, and reproducible builds. Aligns with branches, bundles, and sync — no competitor offers this for an embedded SQL engine.
6. Extension Registry and Distribution
The Lua runtime and package model are delivered. The follow-on is a curated registry (like crates.io or npm) with trust verification, version resolution, and ddb install <package> CLI workflow. This is the ecosystem growth story that makes the Lua extension investment pay off.
7. ICU Collation and Internationalization
Locale-aware sorting, case-insensitive comparison, and Unicode normalization are adoption blockers for international applications. SQLite requires the ICU extension; DecentDB could offer this natively with collation-per-column and collation-per-query support beyond the shipped built-in collations.
8. Query Plan Stability and Plan Guides
Production applications need protection against query plan regressions after engine upgrades or statistics changes. Plan pinning, plan guides, and plan diff tooling would make DecentDB more trustworthy for production use. Pairs with the tracing/advisor work (item 5).
Lower-Priority Additions (Later/Backlog)
9. Table Partitioning
Range/list/hash partitioning for large local datasets, time-series data, and data lifecycle management (drop old partitions). Important as DecentDB targets larger application databases.
10. Continuous Fuzzing and Input Validation
A structured fuzzing program for the SQL parser, planner, storage engine, and sync merge logic. Aligns with the "durability-first" identity and the unsafe audit discipline already in the engineering standards. Not a feature, but a quality assurance investment that competitors rarely make visible.
11. Read-Only and Immutable Database Mode
First-class support for distributing static datasets, reference data, and edge-cached read replicas as immutable DecentDB files. Useful for the bundle format (item 13) and edge deployment stories.
12. API Stability and Versioning Contract
An explicit, documented semver contract covering the C ABI, binding APIs, on-disk format versioning, and deprecation policy. Not a feature but an adoption enabler — teams choosing an embedded database need to know the upgrade story.
---
The strongest additions for the near-term roadmap are Online Non-Blocking DDL (competitive differentiator), Online Backup/PITR (production requirement), and Rich Error Diagnostics (low-cost developer experience win). These fill gaps that the existing 18 items don't address and directly support the positioning statement.
```

## Coding agent #2 suggestions

```
Gaps in the Current Roadmap
1. Online / Zero-Downtime Schema Modifications
Why it's missing: Item #5 (Branch-Aware Migration Rehearsal) covers the safety problem — branch, migrate, validate, diff, promote. The guardrails on line 347 explicitly call out the gap: "online table rebuilds and dual-schema reads need separate ADR coverage."
Why it matters: Branch rehearsal doesn't solve availability. A mobile or desktop app with a large table cannot block all writes (or reads) while an ALTER TABLE rebuilds data. SQLite doesn't solve this well either. Postgres has CONCURRENTLY index builds. A DecentDB that can execute DDL without fully blocking the single writer would be a genuine differentiator and an adoption requirement for production apps.
Fit: Builds on the shipped write queue (one writer), branch copy-on-write storage (ADR 0154), and btree foundation. A dual-schema read path during rebuild would extend the existing snapshot isolation model.
Suggested ranking: Between items #4 and #5 (vNext+1), or as a sub-item of #3 if positioned as "default-fast writes under schema change."
2. Embedded Multi-Tenancy with Per-Tenant Isolation
Why it's missing: Not mentioned. The roadmap focuses on single-application databases. Multi-tenancy is a server-database concern and DecentDB rightly avoids server features.
Why it matters: Desktop applications serving multiple organizations and mobile applications with multiple user profiles need per-tenant data isolation. SQLite handles this with separate database files and ATTACH, which is fragile. DecentDB's shipped policy engine (ADR 0174), branch infrastructure (ADR 0153-0159), and catalog metadata can provide strong per-tenant logical isolation within a single physical database with tenant-scoped queries enforced at the query planner layer.
Fit: Extends the shipped security/policy foundation. Uses branch-cow storage for safe tenant cleanup. Doesn't require multi-writer. Doesn't duplicate server multi-tenancy features like connection-level SET search_path.
Suggested ranking: BACKLOG, after item #10 (authenticated encryption).
3. Row-Level and Page-Level Data Compression
Why it's missing: ADR 0048 (optional-value-compression) exists but is deferred. Item #3 mentions "file size and WAL/checkpoint storage efficiency" but does not commit to a specific compression strategy.
Why it matters: The PRD §3 states "Minimal Disk Footprint" as a core pillar and explicitly says "Smaller is better, provided it does not compromise ACID guarantees or performance." Mobile and browser deployments are storage-constrained. SQLite has no native compression. DuckDB has excellent compression. Dictionary encoding plus optional ZSTD page compression (behind the page cache, transparent to upper layers) would reduce storage by 2–5× on typical application data without harming the zero-copy read path.
Fit: Builds on the shipped chunked-row storage (ADR 0136), record format (ADR 0030), and btree page layout. Compression happens at page write/flush time, decompression at page read time, invisible to the planner/executor.
Suggested ranking: As an explicit sub-track under item #3, or BACKLOG with note that partial delivery is expected through the performance track.
4. WASI / Edge Runtime Support
Why it's missing: The browser runtime (ADR 0161, 0165) targets browser OPFS and Dedicated Workers. WASI is a distinct target: serverless edge functions without a full OS VFS.
Why it matters: Cloudflare Workers, Fastly Compute, Fermyon Spin, and WasmEdge increasingly support WASI as the standard WebAssembly system interface. DecentDB already compiled to wasm32-unknown-unknown for the browser. Targeting wasm32-wasip1/wasm32-wasip2 with an in-memory VFS + optional object-storage backing would let DecentDB run in edge functions where SQLite/DuckDB WASM can't due to filesystem requirements. This is a smaller investment than the browser runtime and unlocks a deployment lane no other embedded SQL database targets.
Fit: Reuses the WASM compile target, the in-memory VFS (ADR 0105), and the existing C ABI. Distinct from item #17 (Cloud-Native Object Storage VFS) which targets a different architectural layer.
Suggested ranking: BACKLOG, after browser parity (item #1) is delivered. Could be scoped as a vNext+1 item if edge/serverless adoption pressure emerges.
5. Deterministic Simulation Testing Framework
Why it's missing: TESTING_STRATEGY.md covers property testing and crash-injection loops. A FoundationDB-style deterministic simulator is a development infrastructure investment, not a user-facing feature.
Why it matters: For an ACID database, concurrency bugs are the hardest class to reproduce and fix. A deterministic simulator that wraps the engine's concurrency primitives (write queue, reader snapshots, lock acquisition, WAL I/O) and injects faults at controlled points would catch bugs that property tests miss. FoundationDB proved this approach works for a distributed database; DecentDB's single-writer model makes the problem much simpler. No other embedded SQL engine has this quality bar.
Fit: Extends the shipped write queue contract (ADR 0162), the concurrency model, and the existing crash-injection testing. Not product-facing — it's an engineering investment that prevents regressions and accelerates development of higher-risk items (online DDL, cross-process coordination hardening).
Suggested ranking: Development infrastructure track. Not a roadmap item per se, but worth noting in a "Development Quality" section or as a parallel engineering investment to items #4 (tracing/advisors) and #5 (branch-aware migration).
---
Minor Observations
- Covering-index execution appears in both item #3 ("teach the planner/executor to exploit covering indexes") and item #15 ("covering-index execution for existing INCLUDE (...) metadata"). It should live in only one place — item #3 makes more sense since it's a performance win.
- Lua extension registry/curation is not a roadmap item, but the shipped runtime would benefit from a "verified extensions" catalog (e.g., decentdb/extensions) that maintains sandboxed, signed, tested packages for common needs (HTTP, crypto, math, JSON manipulation). This fills the gap between "only Lua" and "arbitrary native extension loading" without adding engine complexity.
- The positioning statement on line 140-142 is strong but only appears once. It should be the tagline that opens the document and is repeated in the "Near-Term Sequence" section as the measuring stick for every priority decision.

```

# Coding agent #3 suggestions

```
Suggested Additional Future Wins
1. Savepoint and Nested Transaction Support
Why: DecentDB currently supports flat BEGIN/COMMIT/ROLLBACK. Every major embedded competitor (SQLite, H2, DuckDB) supports SAVEPOINT and nested rollback. Complex local-first app logic — multi-step writes, partial undo within a transaction, ORM savepoint patterns — hits this gap immediately. Without it, developers either restructure logic into smaller autocommit transactions (weakening atomicity) or avoid the engine entirely.
Extends: The shipped write-queue and group-commit contract (ADR 0162). Savepoints are scoped within the existing single-writer model — no concurrency-model change required.
Suggested priority: vNext+1. This is a practical adoption blocker for ORMs and business-logic-heavy local apps. It does not require a new concurrency model, just rollback-to-mark semantics inside the existing write queue.
Guardrails:
- Stay within the single-writer contract; nested savepoints are a rollback marker, not nested locking
- Do not release savepoint resources until the outermost transaction commits
- Keep savepoint depth bounded and expose it in sys.* diagnostics
---
2. Online Non-Blocking Schema Changes
Why: The current roadmap has branch-aware migration rehearsal (#5), which is about testing migrations safely. But the actual execution of schema changes on production databases without blocking readers is a separate, distinct problem. SQLite's ALTER TABLE surface is famously narrow (add column, rename column, rename table). DuckDB and Postgres can alter columns, change types, and drop columns online. For a durable embedded engine where "fast reads" is priority #2, blocking readers during a migration contradicts the identity.
Extends: Branch-aware migration rehearsal (#5) plans the change; online schema change executes it without reader interruption. Both together make DecentDB the only embedded engine with a complete safe-migration story.
Suggested priority: vNext+1. Should be designed alongside branch-aware migration so the rehearsal spec can validate that online execution is feasible before promotion.
Guardrails:
- Start with a narrow, safe schema-change set (add/drop column, change type, rename column, add/drop constraint, add/drop index)
- Dual-schema reads during transition need separate ADR coverage
- Do not hide destructive schema changes behind automatic promotion
- Keep write-path impact measurable and reported in sys.*
---
3. Offline Conflict Resolution Framework
Why: The shipped sync model records conflicts but leaves resolution partly to the application. Local-first apps that go offline for extended periods will accumulate conflicts, and every app reinvents resolution logic. A framework with pluggable strategies (last-writer-wins with vector clocks, CRDT-informed merge for counters/sets, custom Lua merge functions, declarative conflict rules per table/column) would be a strong differentiator over SQLite's session/changeset model, which lacks built-in merge strategies.
Extends: Shipped sync slices, changeset API, and the Lua extension runtime. Conflict handlers can be Lua-defined, keeping the safe-extensibility contract intact.
Suggested priority: vNext+1. Should follow mobile/browser hardening and backend sync bridge, since those define the conflict-production surfaces.
Guardrails:
- Must not require live connectivity to resolve
- Default strategies must be deterministic and replay-identical across replicas
- Custom merge functions run under the same Lua sandbox/limits as other extensions
- Do not auto-resolve conflicts without application-aware opt-in
- Resolution audit records should be inspectable via sys.*
---
4. Cross-Branch Query and Data Reconciliation
Why: DecentDB's branching model currently requires choosing one branch context per query. For agent workflows (compare two branches, suggest a merge), data reconciliation (find divergent rows), and migration validation, the ability to query across branches in a single statement is far more powerful than external diff + manual inspection. This would make DecentDB uniquely capable for "branchable data" workflows that go beyond what git-style tools offer for code.
Desired capability:
- SELECT * FROM main.users MINUS SELECT * FROM branch:migration-test.users syntax or equivalent
- Cross-branch join, except, and intersect operators
- Agent-friendly diff output as structured query results (not just CLI text)
- Cross-branch constraint and schema comparison
Extends: Shipped branch/diff/time-travel (ADR 0153-0159) and the agent integration mode (#11).
Suggested priority: Later. Depends on branching production hardening and agent tooling metadata.
Guardrails:
- Cross-branch queries are read-only on both sides
- Do not introduce hidden branch-merge semantics through query side effects
- Large cross-branch diffs may need streaming/lazy materialization
- Keep branch namespace syntax unambiguous with existing main./temp. qualifiers
---
5. Multi-Tenant Row-Level Isolation
Why: TDE, audit context, and row policies are shipped. Local-first SaaS apps (field service, healthcare, finance) often need guarantee-enforced tenant isolation at the database level — not just policy queries, but hard enforcement that one tenant never sees another's rows. Building this in application code is error-prone; building it into the engine matches the "durable local data with production diagnostics" identity.
Extends: ADR 0174 security v1, audit context, and durable row policies.
Suggested priority: Later. Should follow authenticated encryption (#10) and branch-aware migration (#5) so the isolation boundary is tamper-evident and migration-safe.
Guardrails:
- Tenant isolation must be enforced at read and write time, not just filtered
- Performance impact on single-tenant workloads must be zero when isolation is not configured
- Tenant key management must use the same platform key-store story as TDE key rotation
- Do not conflate tenant isolation with row-level security policies (they can compose but mean different things)
---
6. Storage Quotas and Resource Governance
Why: Browser OPFS has quota limits. Mobile apps have constrained storage. Server/embedded hosts may want to cap database growth. SQLite offers PRAGMA max_page_count but no per-table or per-tenant limits. For DecentDB to be credible in constrained environments, an explicit quota and governance contract prevents runaway growth from degrading the host.
Desired capability:
- Database-level page/byte quotas with explicit over-limit behavior
- Per-table row-count or byte-size soft/hard limits
- Quota-aware WAL and checkpoint behavior
- sys.quotas and Doctor findings for approaching limits
- OPFS-specific quota guidance and browser integration patterns
Extends: Browser/mobile hardening (#1, #2), default-fast performance (#3), and Doctor diagnostics (#4).
Suggested priority: Later. Most valuable after browser OPFS lifecycle and mobile packaging are hardened.
Guardrails:
- Quota enforcement must not compromise durability (reject writes, not silently drop them)
- Quota limits must be persisted and survive crash recovery
- Soft limits should produce warnings, not errors
- Keep quota accounting cheap — no per-write page-count walks
---
7. Deterministic Debug Replay and Production Reproduction
Why: DecentDB is "durable ACID writes" first. When something goes wrong in production, reproduction is the hardest problem. A built-in mode that captures the logical operation sequence (not raw page writes) and replays it deterministically against the same engine version makes bugs reproducible without shipping raw database files. This fits the "serious application database, not just a file format" identity.
Desired capability:
- Optional logical WAL capture: SQL text, parameters, transaction boundaries, open options
- Deterministic replay mode: same engine, same sequence, same result
- Redaction policy for sensitive parameter values (reuse tracing redaction rules)
- Replay against a specific snapshot/branch state
- sys.replay_status for tracking replay progress and divergence
Extends: Shipped sys.* metrics, tracing/diagnostics roadmap (#4), and the existing WAL/recovery foundation.
Suggested priority: Later. Should follow runtime tracing (#4) so the capture format and redaction policy are aligned.
Guardrails:
- Replay capture is strictly opt-in and never enabled implicitly
- Capture must not introduce hot-path overhead when disabled
- Redaction must be enforced before capture is persisted
- Replay output is for diagnosis, not for replacing crash recovery
- Do not persist captured SQL text with sensitive parameter defaults
---
8. Structured CDC and Event Integration Protocol
Why: The shipped reactive subscriptions and change streams serve in-process consumers. Many production architectures need to feed DecentDB changes into external systems: message queues, webhooks, cloud functions, audit log aggregators. A structured CDC protocol that emits changes in a standardized format (Debezium-compatible, or DecentDB-native changeset-based) would make DecentDB a realistic embedded source in event-driven architectures — without turning it into a message broker.
Extends: Shipped change streams (ADR 0164), changesets, and sync journal. The CDC output can reuse changeset format so sync and CDC share the same serialization contract.
Suggested priority: Later. Should follow sync hardening (#1/#2/#6) and agent integration (#11) so the format is stable.
Guardrails:
- CDC is a consumption surface, not a new write path
- External delivery is best-effort; durability is the local transaction's responsibility
- Do not build a message broker into the engine
- Backpressure from slow consumers must not block the writer
- Rate-limit and buffering behavior must be explicit and configurable
---
Summary Table
#	Future Win
A	Savepoint / Nested Transactions
B	Online Non-Blocking Schema Changes
C	Offline Conflict Resolution Framework
D	Cross-Branch Query and Reconciliation
E	Multi-Tenant Row-Level Isolation
F	Storage Quotas and Resource Governance
G	Deterministic Debug Replay
H	Structured CDC Protocol
Items A-C would fit naturally in the vNext+1 bucket alongside the existing #4-6 items. Items D-H belong in the Later bucket, extending shipped foundations without competing for near-term capacity.
```

## Coding agent #4 suggestions

```
Based on my review of the document, here are additional Future Wins that would complement the existing roadmap:
Potential Additional Future Wins
1. First-Class Lua Extension EcosystemLua extensions are delivered but treated as a finished foundation. Missing:
- Official package registry / curated index of verified packages
- Prebuilt bindings to popular host-language libraries (HTTP, JSON paths, CSV, encryption)
- Package dependency tracking and version resolution- RETURNING TABLE extensions that return typed relation results (not just scalars/arrays)
- Integration with sys.* surfaces so extension authors can emit metrics
Why: SQLite/DuckDB extension ecosystems are a competitive moat. Making Lua packages discoverable and maintainable leverages the shipped sandbox model as a differentiator.
---
2. Zero-Downtime Schema Migration Path
Branch-aware migration rehearsal is listed but focuses on rehearsal. Missing is:
- Persistent migration branch lifecycle (attach, track, promote/demote)
- DDL metadata catalog that tracks column renames, type changes, constraint additions
- Online ALTER TABLE via intermediate writable branch + dual-schema read
- Automated rollback via branch RESTORE snapWhy: Teams choosing SQLite typically cite schema migration friction. DecentDB's branch mechanics could make this a standout capability.
---
3. Bulk Import/Export StreamingExcluded explicitly for Decent Bench, but a middle road is missing:
- COPY ... FROM STDIN with streaming row insertion (not all-memory)
- EXPORT TO DATABASE as a recoverable checkpoint operation
- Chunked table export via cursor pagination
- Progress visibility in sys.progress for long operations
Why: Even teams using Decent Bench for full workflows need embedded bulk hooks for programmatic use. This is below Decent Bench scope but above raw engine scope.
---
4. Explicit Connection/Session Lifecycle
The document focuses on write queuing and cross-process coordination but omits:
- Named connection pool contracts visible in sys.sessions
- Session-scoped settings (SET LOCAL, CURRENT_DATABASE)
- Connection hang detection and forced close via C ABI- Query cancellation propagation through the write queue
Why: Multi-process/binding use patterns need session visibility. Developers debugging connection issues currently lack tooling surface.
---
5. Internationalized Collations (ICU Integration)
The doc mentions "query-time built-in collations" as delivered but limits to ASCII. Missing:
- Unicode-aware collations via bundled or pluggable ICU
- COLLATE NOCASE_UNICODE, COLLATE UNICODE with proper normalization
- Locale-aware numeric, date, currency formatting
- Case-insensitive Unicode matching for non-Latin scripts
Why: SQLite+PGlite have ICU options; DuckDB surfaces ICU quirks differently. Proper Unicode collations matter for healthcare, government, and internationalized consumer apps.
---
6. Change Data Capture (CDC) Surface
Sync is delivered but CDC is a distinct workflow. Missing:
- Structured row-level change events with before/after state- CHANGES() surface that emits change vectors independent of sync- Ledger-style sys.changelog with actor, reason, and timestamp
- Filtered change feeds per table or row policy scope
Why: Append-only change logs enable audit pipelines, event-driven microservices, and "last-write-wins" conflict resolution patterns that go beyond DecentDB's default sync model.
---
7. Adaptive Query Planner and Plan Hints
Tracing/advisors are listed but the planner itself is static. Missing:
- Cost model auto-tuning via collected runtime statistics
- PRAGMA planner_hints surface that lets bindings influence join order, index selection
- Runtime plan selection switching for parameterized queries based on param distributions
- EXPLAIN (ANALYZE, BUFFERS) with WASM/binding-friendly JSON output
Why: SQLite has no plan hints; DuckDB has partial hints. Adaptive planning based on actual runtime feedback would be novel for an embedded engine.
---
8. Managed Storage Compaction and AutoCheckpoint
No explicit future win addresses WAL/page compaction. Missing:
- Configurable auto-checkpoint thresholds (already exists via pragmas but not surfaced as a first-class tuning surface)
- VACUUM with progress reporting and I/O throttling
- Background compaction via idle worker thread
- Cold-storage tiering hints for older branches (separate from full cloud VFS)
Why: Long-running applications accumulate WAL bloat and page fragmentation. Auto-tuning compaction reduces operational burden without requiring users to understand database internals.
---
###9. Trigger and Pub/Sub Notification Framework
Change streams are delivered but trigger notification is absent. Missing:
- CREATE TRIGGER ... ON INSERT|DELETE|UPDATE per table/scope- Row-level trigger bodies that call Lua or invoke webhooks
- NOTIFY channel [payload] / LISTEN channel session-level pub/sub
- Trigger ordering and INSTEAD OF support for views
Why: Complex business logic that needs to react to row changes without poll queries requires triggers. This is standard SQL and a common migration blocker from Postgres-compatible systems.
---
###10. Row-Level Security as First-Class Feature
TDE v1 covers data confidentiality but row-level security (RLS) is absent. Missing:
- CREATE POLICY ... FOR SELECT|INSERT|UPDATE|DELETE USING (bool_expr)
- Role membership and CURRENT_USER scoped to row evaluation
- Column masking via policy WITH CHECK
- Binding-overridable security context for multi-tenant apps
Why: Healthcare, CRM, and multi-tenant SaaS apps need row-level isolation without application-layer filtering. This extends the delivered policy/security story into a first-class SQL surface.
---
###11. Chaotic Reliability Testing SurfaceThe benchmarking guide covers performance but not fault injection. Missing:
- Structured chaos testing akin to SQLancer but embedded (simulated process crash, page corruption, disk full, lock contention)
- Fuzzing corpus integration for SQL parsing and query execution
- Randomized WAL/page layout corruption with recovery validation
- Integration test hooks for binding smoke suitesWhy: DecentDB's durability guarantee is more credible with a documented chaos testing surface. This would also serve as a regression gate for the cross-process WAL coordination work.
---
Items That Don't Need Future Wins
The document correctly excludes:
- Arbitrary native extension loading — Lua + manifest is the right answer
- General ETL/import/export — Decent Bench owns this
- LLM or text-to-SQL — Outside engine scope
- General durable job queue — Not an embedded engine's job
```

## Coding agent #5 suggestions

```
I've reviewed the full roadmap. Here are additional Future Wins worth considering:
---
1. Snapshot Isolation for Long-Running Reads (High Priority)
The one-writer/many-readers model is correct, but without snapshot isolation, a long-running analytical read will either see inconsistent data or block writes. This is a fundamental SQLite pain point that WAL mode mitigates but doesn't fully solve. Providing consistent snapshot reads against a durable point-in-time would be a meaningful architectural differentiator. This isn't "hidden multi-writer MVCC" — it's strengthening the reader contract.
2. WASM-Based UDF Sandbox (Medium Priority)
Lua extensions are delivered, but a WASM sandbox (via wasmtime or similar) would let users write safe extensions in Rust, C, Go, Zig — any language that compiles to WASM. This broadens the extension ecosystem without opening the "arbitrary native extension loading" door. WASM is also browser-native, aligning with the browser/mobile story.
3. Zero-Copy Shared-Memory Cross-Process Reads (Medium Priority)
The shipped cross-process WAL coordination (#77 in delivered context) coordinates writers safely. A natural follow-on is enabling read-only companion processes to access database pages via mmap/memfd/shared memory without copying through the OS page cache. This would be a substantial read-throughput win for multi-process desktop and server deployments.
4. Declarative Schema Validation & Linting (Medium-Low Priority)
A PRAGMA schema_lint or Doctor extension that flags anti-patterns: missing indexes on foreign keys, columns that should be NOT NULL, redundant indexes, type affinity surprises, sync-incompatible schema choices. Complements the shipped Doctor/tracing direction with a static-analysis layer that runs before problems manifest at runtime.
5. Incremental Backup and PITR (Lower Priority)
Beyond the bundle format (#12), production deployments need incremental backups (only changed pages since last checkpoint) and point-in-time recovery from WAL archives. This is distinct from WAL streaming replication (#16) — it's about durable offline backup/restore workflows, not live standby.
6. Memory Budgeting and Resource Limits (Lower Priority)
Embedded databases run inside host applications. Without memory budgets, a complex query or oversized WAL can OOM the host process. Exposing PRAGMA memory_limit, PRAGMA wal_size_limit, and query-timeout guards would make DecentDB safer to embed in constrained environments (mobile, browser, shared servers).
---
What doesn't belong on this list (intentionally):
- CRDT column types — Too narrow; the sync model already handles conflict-aware merge, and schema-level CRDT semantics would constrain the storage layer in ways that don't generalize well.
- Columnar/OLAP storage mode — DuckDB already owns this. DecentDB should stay row-oriented with smart covering-index execution.
- Virtual tables / table-valued functions in user extensions — The Lua runtime already delivers table-valued functions; broadening this to an arbitrary virtual-table interface invites the extension-loading complexity the roadmap explicitly excludes.
- Time-series specialization — The best path for time-series is making default performance fast enough (roadmap #3) and letting covering indexes, projections, and hybrid search handle the rest, rather than a separate storage engine.
The top two — snapshot isolation and WASM UDFs — would each require an ADR and would materially strengthen DecentDB's "durable, safe, extensible" identity against SQLite, libSQL, and the PGlite/Electric ecosystem.
```

## Coding agent #6 suggestions

```
Based on reviewing the FUTURE_WINS.md roadmap, here are a few additional "Future Wins" that would complement DecentDB's mission and align with its priorities:
Additional Future Win Candidates
1. Automated Database Maintenance System
Status: TODO  
Future Version: vNext+1 or Later  
Why This Matters: While DecentDB has performance tuning guides, many embedded/database users expect "zero-maintenance" operation. An automated maintenance system could handle vacuum, analyze, and integrity checks in the background based on usage patterns and workload characteristics, reducing operational overhead for applications that embed DecentDB.
Desired Capability:
- Background maintenance tasks that adapt to database write/read patterns
- Automatic vacuum triggered by fragmentation thresholds or write amplification
- Self-tuning ANALYZE that updates statistics when beneficial
- Integrity checking that runs during low-activity periods
- Maintenance progress visible in sys.* tables
- Configurable aggressiveness (from "set and forget" to "maximum performance")
Guardrails:
- Maintenance must not compromise durable ACID guarantees
- Background work should yield to foreground database operations
- No silent data loss or format changes without explicit user consent
- Maintenance overhead should be measurable and benchmarkable
2. Time-Series Storage Optimizations
Status: TODO  
Future Version: Later  
Why This Matters: Many local-first applications (IoT, monitoring, finance, telemetry) work heavily with time-series data. While DecentDB is a general-purpose SQL engine, adding storage engine hints or specialized table options for time-series workloads could significantly improve compression and query performance for this important use case without complicating the general engine.
Desired Capability:
- WITH TIME_SERIES_OPTIMIZATIONS table option or similar
- Columnar storage hints for timestamp/value pairs
- Specialized indexing for time-range predicates
- Automatic downsampling/retention policies
- Time-series specific SQL functions (time_bucket, rate, etc.)
- WASm/mobile portable implementation
Guardrails:
- Must not alter core SQL semantics or durability guarantees
- Should be opt-in per-table to avoid affecting general workloads
- Format changes would require ADR and migration planning
- Should complement rather than replace the performance tracking work already planned
3. Enhanced Diagnostics and Error Reporting
Status: TODO  
Future Version: vNext+1  
Why This Matters: While DecentDB has operational metrics and Doctor integration, improving the day-to-day developer experience through better error messages, structured error codes, and built-in health checks would make the database easier to embed and troubleshoot in applications.
Desired Capability:
- Structured error codes with SQLSTATE compatibility
- Error messages that include remediation suggestions
- Built-in PRAGMA integrity_check and PRAGMA quick_check equivalents
- SYSTEM.health view showing database status
- Error telemetry that excludes sensitive data by default
- CLI doctor command with fix-it hints
Guardrails:
- Error reporting must not leak sensitive data
- No runtime overhead when diagnostics aren't enabled
- Error messages should be stable for programmatic consumption
- Health checks should be lightweight and non-blocking
These suggestions focus on areas that would make DecentDB more compelling as an embedded database for modern applications while staying true to its durable, fast, correct, and embeddable nature. They address operational simplicity, specific workload optimizations, and developer experience—all key factors in adoption for embedded databases.

```

## Coding agent #6 suggestions

```
I've reviewed the FUTURE_WINS.md roadmap. Based on DecentDB's positioning as a "durable, fast, correct, and easy to embed" embedded SQL engine with local-first, branchable, browser-capable, and agent-friendly workflows, here are additional Future Wins this project would benefit from:
Additional Future Wins to Consider
Storage and Performance Operations
1. Incremental checkpointing and vacuum control - Explicit checkpoint scheduling, incremental vacuum, and WAL truncation policies. Critical for mobile/embedded write-heavy workloads and preventing unbounded growth.
2. Query plan caching and adaptive statistics - Cache compiled plans per connection/session, auto-evolve planner stats without requiring manual ANALYZE, support for prepared statement plan reuse across similar queries.
3. Streaming result set API - Memory-bounded streaming for large result sets in bindings, avoiding full result materialization in WASM/mobile contexts.
Operational Enhancements
4. Multi-database ATTACH support with cross-database transactions - Safe multi-file database attachments with explicit transaction boundaries across files (beyond SQLite's attach model).
5. Schema evolution history and metadata versioning - Queryable schema change history (sys.schema_changes), migration timeline views, and diff-based schema understanding.
6. Time-series optimization patterns - Partitioned table support, time-bucket aggregation helpers, and sequential-append optimizations for IoT/event workloads.
Developer Experience
7. Built-in backup/restore with encryption awareness - Hot backup API that preserves TDE state, optional compression, and point-in-time restore without external tooling.
8. Connection pool management for reader scaling - Explicit pooled connection strategies that leverage the one-writer/many-readers model without introducing multi-writer complexity.
Security and Compliance
9. Attribute-based access control (ABAC) extensions - Row/policy-level access decisions based on dynamic attributes (user roles, time-of-day, data sensitivity) beyond static row policies.
10. Audit log export and compliance views - Structured audit log extraction for SOC2/GDPR workflows, retention policies for audit data, and compliance dashboard queries.
Integration Work
11. Embedded replication slots and logical decoding - Expose logical change streams for external consumers (beyond sync), with replication slot management for progress tracking.
These additions complement the existing roadmap by addressing operational friction in embedded deployments, developer workflow ergonomics, and regulatory compliance needs that are common blockers for SQLite alternatives in production use.
```

## Coding agent #7 suggestions

```
Additional Future Wins to consider adding (ranked by fit to consolidation filter):
18. Workload-driven automatic index advisor and lightweight auto-maintenance (vNext+1, TODO)  
   Builds on shipped sys.* + Doctor; reduces "tune or fail" friction for default-fast perf without planner contract changes.
19. Native UUID type + functions with index-friendly storage (Later, BACKLOG)  
   Fills a common migration/ORM gap; cheap win once JSONB/path work exists, keeps C ABI stable.
20. Declarative retention + compaction policies for WAL/branch/sync journals (Later, BACKLOG)  
   Hardens mobile/browser production runtimes and storage efficiency; follows TDE/security and cross-process foundations.
21. In-process read replica / snapshot isolation helpers for analytics dashboards (Later, BACKLOG)  
   Leverages one-writer model + projections track; attractive for reactive apps without multi-writer or FDW scope creep.
These stay inside durability-first identity, avoid excluded areas, and extend delivered surfaces (metrics, branches, sync, browser/mobile, performance).
```


## Coding agent #7 suggestions

```
Here are additional Future Wins that would strengthen DecentDB's roadmap, organized by where they'd fit in the priority structure:
---
High-impact adoption blockers (vNext candidates)
Hot Online Backup and Point-in-Time Recovery
SQLite has the Online Backup API; Litestream built a product around SQLite WAL streaming for backup. DecentDB has WAL, branches, and snapshots but no first-class hot backup story: consistent live snapshots without stopping writes, incremental backup deltas, and PITR using WAL replay from a base snapshot. This is a production deployment table-stakes feature that teams evaluate before choosing an embedded database for anything beyond toy use. It complements but differs from WAL Streaming Replication (#16) — backup is about durable recovery artifacts, not standby read scaling.
Page-Level Compression (LZ4/Zstd)
DecentDB's storage efficiency roadmap item (#3) targets WAL/checkpoint overhead and file size, but persistent page-level compression is a distinct, format-affecting capability. DuckDB and many server databases offer it. For local-first apps storing large text, JSON, or blob payloads, 2-5x file size reduction directly affects sync bandwidth, OPFS quota pressure in browsers, and mobile storage. This requires an ADR because it touches the on-disk format (ADR 0131 migration implications), but it's a stronger differentiator than generic "storage efficiency" tuning.
---
Medium-impact differentiators (vNext+1 candidates)
Schema Registry and Compatibility Validation
DecentDB has branches, diff, migration rehearsal (#5), and stable tooling metadata. What's missing is a formal schema version registry with compatibility contracts: declare expected schema version, validate on open, detect drift between what the application expects and what the database contains, and surface mismatches through sys.* and Doctor. This is especially valuable for local-first apps where the database file outlives any single application deployment and may be opened by different application versions. It strengthens the agent/tooling story (#11) by giving agents a machine-readable schema contract to validate against before proposing migrations.
Expression Indexes and Functional Indexes
The advanced SQL compatibility track (#15) mentions covering-index execution and partial indexes, but expression indexes (CREATE INDEX ON users (LOWER(email))) are a distinct, high-value capability. They eliminate the need for computed columns or application-side normalization for common query patterns. PostgreSQL, SQLite (via generated columns workaround), and most server databases support them. They integrate cleanly with the existing planner and index infrastructure without requiring format changes, and they're particularly useful for the JSON path queries that JSONB (#8) would enable.
ICU Collation and Advanced Locale-Aware Sorting
DecentDB ships query-time built-in collations as a compatibility quick win, but production applications with international user bases need proper Unicode collation (UCA), locale-specific sort orders, and case-insensitive/accents-insensitive comparison that goes beyond ASCII. SQLite requires a compile-time ICU option; most embedded databases punt on this entirely. First-class ICU collation would be a meaningful differentiator for any app with non-English data and would strengthen the "serious application database" positioning. This needs careful scoping for WASM/mobile portability (ICU data size is non-trivial).
---
Lower-impact but strategically valuable (BACKLOG candidates)
Cross-Database Attach and Federated Local Queries
SQLite's ATTACH DATABASE lets applications query across multiple database files in a single SQL statement. For DecentDB, this enables: multi-tenant local data isolation (separate encrypted databases per user/account), application data + user data separation, read-only reference databases attached to a writable application database, and branch-as-database workflows where a branch is physically a separate file. This is distinct from the backend sync bridge (#6) — it's about local multi-file composition, not remote backend integration. It requires careful transaction and locking semantics across files but builds naturally on the one-writer model.
Query Plan Stability and Regression Prevention
As DecentDB's planner matures, query plan regressions become a real risk: an engine upgrade or ANALYZE refresh changes a plan from fast to slow. Stored outlines or plan pinning would let applications lock known-good plans for critical queries. This complements the tracing/advisors track (#4) — advisors explain problems, plan stability prevents them from recurring after detection. It's a niche feature but valuable for the "serious application database" positioning and particularly relevant for Decent Bench's tooling workflows.
Session Context and Application-Defined Metadata
Allow applications to attach key-value context to a connection/session that flows into audit context, sync metadata, and sys.* views. This bridges the gap between application identity (who is this user, what tenant, what operation) and database observability without requiring schema changes. It strengthens the audit context foundation (ADR 0174), makes the tracing track (#4) more useful, and gives agents (#11) a way to tag their operations for observability.
---
Suggested priority ordering if added to the roadmap
Priority	Feature	Version	Rationale
3.5	Hot Online Backup and PITR	vNext	Production deployment blocker; competitive gap vs SQLite+Litestream
3.6	Page-Level Compression	vNext	Format-affecting; needs ADR; directly supports storage efficiency goals
5.5	Schema Registry and Compatibility Validation	vNext+1	Strengthens migration rehearsal and agent story
8.5	Expression Indexes	Later	Natural complement to JSONB and advanced SQL
15.5	ICU Collation	Later	Internationalization differentiator; WASM size tradeoff
16.5	Cross-Database Attach	Later	Local multi-file composition; distinct from sync bridge
4.5	Query Plan Stability	vNext+1	Prevents regressions once tracing can detect them
11.5	Session Context	Later	Amplifies audit, tracing, and agent surfaces
The fractional priorities indicate where they'd slot between existing items. The strongest additions are Hot Online Backup/PITR and Page-Level Compression — both address production deployment concerns that currently push teams toward SQLite+Litestream or server databases.
```
