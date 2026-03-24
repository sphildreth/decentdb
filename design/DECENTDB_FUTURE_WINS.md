# DecentDB: Future Wins & Bragging Rights

This document outlines a roadmap of high-impact features we can add to DecentDB to position it as a superior alternative to SQLite for modern application development. By targeting SQLite's historical architectural compromises and the most common developer pain points, DecentDB can become the engine of choice for developers building everything from local-first web apps to complex embedded systems.

---

## Effort vs. Impact Matrix

To help prioritize these features, here is an Effort vs. Impact matrix. 
- **Impact** measures the value-add to the developer (solving major pain points, providing a "wow" factor).
- **Effort** measures the engineering complexity required to implement the feature within DecentDB's existing architecture.

```mermaid
quadrantChart
    title Feature Priority Matrix
    x-axis Low Effort --> High Effort
    y-axis Low Impact --> High Impact
    quadrant-1 High Value, Complex
    quadrant-2 Quick Wins
    quadrant-3 Low Priority
    quadrant-4 Nice to Have
  "JSON Table Functions / Operators": [0.20, 0.82]
  "Date/Time + UUID Builtins": [0.15, 0.72]
  "Planner Statistics / ANALYZE": [0.35, 0.78]
    "JSONB Binary Storage": [0.35, 0.85]
    "Built-in CDC (Reactive)": [0.45, 0.80]
    "MVCC (Concurrent Writes)": [0.90, 0.95]
    "Native Vector Index (HNSW)": [0.75, 0.70]
    "Transparent Encryption (TDE)": [0.65, 0.60]
    "Full-Text Search (FTS)": [0.55, 0.75]
  "Non-Blocking Schema Migration": [0.60, 0.65]
    "WAL Streaming Replication": [0.80, 0.55]
    "Group Commit / WAL Batching": [0.35, 0.75]
  "Cross-Process WAL Coordination": [0.55, 0.68]
```

### Implementation Status

#### ✅ Already Implemented (Remove From Future-Wins Queue)

| Feature | Status | Notes |
|---|---|---|
| Bulk Load API | ✅ Shipped | Engine bulk-load helpers exist today; future work should focus on streaming ingest and COPY-style ergonomics rather than treating bulk load itself as unbuilt |
| Shared WAL registry | ✅ Shipped (single process) | Connections to the same on-disk database already share a process-global WAL; the remaining gap is cross-process coordination, not same-process visibility |
| Expanded ALTER TABLE | ✅ Shipped (blocking) | `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and limited `ALTER COLUMN TYPE` are implemented; the remaining win is non-blocking/background migration |

#### ⚠️ Partially Implemented

| Feature | Status | Gap |
|---|---|---|
| JSON support | ⚠️ Partial | `json_extract`, `json_array_length`, and JSON parsing are implemented; missing `->`, `->>` operators plus executable `json_each()` / `json_tree()` runtime support |
| Date/Time functions | ⚠️ Partial | Native `TIMESTAMP` type is shipped; `NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, and `EXTRACT()` still need runtime wiring |
| UUID functions | ⚠️ Partial | Native `UUID` type is shipped; `GEN_RANDOM_UUID()` still needs runtime wiring |
| Window functions | ⚠️ Partial | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, and `LEAD` are implemented; broader PostgreSQL-style window support remains future work |
| Expression / partial indexes | ⚠️ Partial | Both features ship today, but with deliberate v1 limits on supported expressions and predicates |

#### ⏳ Recommended Next Wins

| Feature | Section |
|---|---|
| JSON table functions and operators | Section 0.1 below |
| Date/Time builtins | Section 0.2 below |
| UUID generation builtin | Section 0.3 below |
| Planner statistics / `ANALYZE` | Section 0.4 below |
| JSONB binary storage | Section 2 below |
| CDC / Reactive Subscriptions | Section 3 below |
| MVCC | Section 1 below |
| Vector / HNSW Index | Section 4 below |
| Transparent Data Encryption | Section 5 below |
| Full-Text Search (FTS) | Section 6 below |
| Non-Blocking Schema Migration | Section 7 below |
| WAL Streaming Replication | Section 8 below |
| Bulk Load Follow-Ons | Section 9 below |
| Group Commit / WAL Batching | Section 10 below |
| Cross-Process WAL Coordination | Section 11 below |

### The Strategy
*   **Start with true quick wins:** **JSON table functions/operators**, **Date/Time builtins**, **UUID generation**, and **planner statistics / `ANALYZE`**. These are all adjacent to code that already exists, close visible documentation gaps, and improve SQL ergonomics without destabilizing the storage engine.
*   **Then take on structural wins:** **JSONB Binary Storage**, **Group Commit**, and **Non-Blocking Schema Migration**. These touch storage, planning, and WAL behavior, but build directly on foundations already present in the engine.
*   **The Ultimate Goal (Top Right - High Value, Complex):** **MVCC**. This still requires a concurrency-model rewrite and should stay a deliberate post-1.0 architectural step, not an opportunistic feature add.

---

## 0. Near-Term Wins We Should Prioritize Before New Engine Surfaces

These are the highest-confidence roadmap items because the surrounding infrastructure already exists in code today. They also fix the current mismatch between shipped documentation and executable behavior.

### 0.1 JSON Table Functions and Operators

The engine already has JSON parsing, JSON path lookup, and scalar JSON helpers. The next low-risk step is to finish the missing SQL surface:

*   Add `->` and `->>` operators for PostgreSQL-style JSON ergonomics.
*   Wire `json_each()` and `json_tree()` into executable FROM-clause runtime support.
*   Align engine behavior with ADR 0111 and existing docs so bindings and SQL examples stop advertising unavailable behavior.

This is a better near-term investment than jumping straight to JSONB because it closes an existing product gap with a much smaller blast radius.

### 0.2 Date/Time Builtins

DecentDB already stores native `TIMESTAMP` values efficiently. The remaining work is to expose the normal SQL entry points developers expect:

*   `NOW()`
*   `CURRENT_TIMESTAMP`
*   `CURRENT_DATE`
*   `CURRENT_TIME`
*   `EXTRACT()`

This unlocks defaults, audit columns, and more natural time-based queries across every binding.

### 0.3 UUID Generation Builtin

The type system already supports native `UUID` storage. Shipping `GEN_RANDOM_UUID()` is a high-leverage finish that immediately improves schema ergonomics:

```sql
CREATE TABLE users (
  id UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
  email TEXT NOT NULL
);
```

This is a small implementation with outsized value because it removes a common source of application-side boilerplate.

### 0.4 Planner Statistics / `ANALYZE`

DecentDB already has a cost-based optimizer, but the roadmap should explicitly call out the next maturity step: persistent planner statistics.

*   Add catalog-backed table/index statistics.
*   Implement `ANALYZE` to refresh cardinality/selectivity metadata.
*   Use the stats to make join ordering and index selection less heuristic-heavy.

This is one of the highest-value performance wins still missing from the current planning stack.

---

## 1. Multi-Version Concurrency Control (MVCC) for Concurrent Writers

### The SQLite Pain Point
SQLite uses database-level (or WAL-level) write locks. While Write-Ahead Logging (WAL) allows concurrent readers alongside a *single* writer, any application with high write concurrency inevitably hits `SQLITE_BUSY` errors. Developers are forced to implement complex in-application queuing, connection pooling workarounds, or retry logic.

### The DecentDB Win
Implement true **Multi-Version Concurrency Control (MVCC)** or **Row-Level Locking**. By allowing multiple transactions to write to different rows simultaneously without blocking each other, DecentDB would completely eliminate the `SQLITE_BUSY` bottleneck. This transforms DecentDB from a "single-user embedded database" into an embedded engine capable of handling high-throughput, multi-threaded server workloads directly.

```mermaid
sequenceDiagram
    participant AppThread1
    participant AppThread2
    participant DecentDB_MVCC
    participant SQLite_WAL

    Note over AppThread1, SQLite_WAL: SQLite Behavior
    AppThread1->>SQLite_WAL: BEGIN EXCLUSIVE (Write Row A)
    SQLite_WAL-->>AppThread1: OK
    AppThread2->>SQLite_WAL: BEGIN EXCLUSIVE (Write Row B)
    SQLite_WAL-->>AppThread2: ERROR: SQLITE_BUSY (Locked)

    Note over AppThread1, DecentDB_MVCC: DecentDB Future Behavior
    AppThread1->>DecentDB_MVCC: BEGIN (Write Row A)
    DecentDB_MVCC-->>AppThread1: OK (Row A Locked)
    AppThread2->>DecentDB_MVCC: BEGIN (Write Row B)
    DecentDB_MVCC-->>AppThread2: OK (Row B Locked)
    AppThread1->>DecentDB_MVCC: COMMIT
    AppThread2->>DecentDB_MVCC: COMMIT
    Note over DecentDB_MVCC: Both writes succeed concurrently!
```

---

## 2. JSONB Binary Storage

### Already Implemented: Native Rich Types

DecentDB already ships native rich types that SQLite lacks:

*   **`TIMESTAMP`** (ADR 0114): Stored as zigzag-varint int64 of microseconds since Unix epoch UTC. Type is fully implemented with binding support across .NET, Python, Java, Node.js, and Go. **Note:** `NOW()`, `CURRENT_TIMESTAMP`, and `EXTRACT()` functions are documented but not yet in the scalar dispatcher.
*   **`UUID`** (ADR 0072, 0091): Stored as a highly optimized 16-byte packed structure (`ColumnType::Uuid`). **Note:** `GEN_RANDOM_UUID()` is documented but not yet implemented.
*   **`DECIMAL`** (ADR 0072, 0091): Stored as scaled int64 with explicit scale, avoiding floating-point rounding errors.
*   **JSON scalar functions** (ADR 0102): `json_extract()` and `json_array_length()` are implemented. **Note:** `->`, `->>` operators and `json_each()`, `json_tree()` table-valued functions are documented but not yet implemented.

### The Remaining SQLite Pain Point: JSON as Plain Text
SQLite stores JSON as plain text. Querying JSON requires parsing the string at runtime for *every* row evaluated. While DecentDB already provides `json_extract()` and `json_array_length()` scalar functions (ADR 0102), the underlying storage is still text — the parser runs on every access.

### The DecentDB Win: JSONB
Introduce **JSONB** — Binary JSON (like PostgreSQL). Queries traverse the binary structure directly without parsing strings, making JSON indexing and querying orders of magnitude faster than text-based JSON.

```mermaid
block-beta
  columns 2
  block:SQLite
    title["SQLite JSON Search"]
    ReadPage["Read Page"]
    Parse["Parse Text to JSON"]
    Eval["Evaluate Path '$.user.id'"]
    Match["Match?"]
  end
  block:DecentDB
    title["DecentDB JSONB Search"]
    ReadPage2["Read Page"]
    Jump["Jump Direct to Offset 'user.id'"]
    Match2["Match?"]
  end
```

---

## 3. Built-in Change Data Capture (CDC) & Reactive Subscriptions

### The SQLite Pain Point
Local-first applications (React, Vue, Svelte) and edge architectures need to react to database changes in real-time. Syncing SQLite to an external database or updating a UI requires complex trigger-based workarounds, polling, or heavy external syncing libraries (like ElectricSQL or PowerSync).

### The DecentDB Win
Build a **Native Publish-Subscribe API** by tailing the Write-Ahead Log (WAL). Applications can simply run `SELECT * FROM listen_changes('users')` or hook a callback into the engine to receive an instant, ordered stream of inserts, updates, and deletes as they are committed. 

```mermaid
graph TD
    A[UI Thread / Frontend] -->|1. Subscribe to 'albums'| B(DecentDB Engine)
    C[Background Sync Worker] -->|2. INSERT INTO albums| B
    B -->|3. Commit to WAL| D[(wal file)]
    D -->|4. Tail WAL & Parse Frame| B
    B -.->|5. Push JSON event| A
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#bbf,stroke:#333,stroke-width:4px
```

---

## 4. Native Vector/Embedding Indexes (HNSW)

### The SQLite Pain Point
With the explosion of AI, LLMs, and Retrieval-Augmented Generation (RAG), vector similarity search is a baseline requirement. SQLite users must compile, load, and manage fragile external C-extensions like `sqlite-vss` or `sqlite-vec`. This breaks the "it just works everywhere" promise of embedded databases, especially in mobile or cross-platform CI/CD pipelines.

### The DecentDB Win
Provide a native `VECTOR(dim)` data type and an integrated **HNSW (Hierarchical Navigable Small World) Index**. Developers get out-of-the-box, lightning-fast similarity search (e.g., `SELECT * FROM docs ORDER BY embedding <=> '[0.1, 0.5, ...]' LIMIT 5`) with zero external dependencies.

---

## 5. Transparent Data Encryption (TDE)

### The SQLite Pain Point
If you need an encrypted database on iOS, Android, or desktop (to comply with HIPAA/GDPR), vanilla SQLite cannot help you. You must use **SQLCipher**. SQLCipher requires commercial licensing for many use cases, relies on custom builds, causes massive friction with standard ORMs, and is famously difficult to compile cross-platform.

### The DecentDB Win
Built-in **Page-Level AES-256-GCM Encryption**. Since DecentDB controls the Pager, we can intercept page flushes and reads. The developer simply executes `PRAGMA encryption_key = 'super_secret';` upon connection. The engine transparently encrypts data at rest, including the WAL and temporary files, with zero external build dependencies.

---

## 6. Full-Text Search (FTS) with Ranking

### The SQLite Pain Point
SQLite requires the external FTS5 extension for full-text search. While functional, it must be compiled, loaded, and managed separately. Developers face cross-platform build friction, and the extension lacks native integration with the query planner — FTS queries use virtual table syntax rather than standard SQL.

### The DecentDB Win
Provide a native `TSVECTOR` type and `TSQUERY` operators with integrated **BM25 ranking**, stemming, and phrase search. Developers write standard SQL:

```sql
CREATE INDEX docs_fts ON documents USING gin (to_tsvector('english', body));
SELECT id, ts_rank_cd(to_tsvector('english', body), query) AS rank
FROM documents, plainto_tsquery('embedded database') query
WHERE to_tsvector('english', body) @@ query
ORDER BY rank DESC
LIMIT 10;
```

No extensions, no build steps, no virtual tables — just SQL.

---

## 7. Non-Blocking Schema Migration

### The SQLite Pain Point
SQLite's `ALTER TABLE` is limited to `ADD COLUMN` and `RENAME TABLE`. Modifying a column type, adding a constraint, or dropping a column requires creating a new table, copying all data, dropping the old table, and renaming — all while holding an exclusive lock. For large databases, this blocks all reads and writes for minutes or hours.

### Current DecentDB Status
DecentDB is already ahead of SQLite here in raw DDL coverage: `ALTER TABLE` can add columns, drop columns, rename columns, and perform a limited set of type changes. The remaining gap is that these operations are still synchronous, guarded, and blocking rather than background or lazily migrated.

### The DecentDB Win
Implement **background schema migrations** that don't block reads or writes. The engine maintains both old and new schema versions simultaneously, migrates rows lazily in the background, and atomically swaps the catalog entry when complete:

```sql
-- Instant: adds column metadata, no table copy
ALTER TABLE users ADD COLUMN email TEXT;

-- Background: rebuilds table with new column type, non-blocking
ALTER TABLE users ALTER COLUMN age SET DATA TYPE BIGINT;

-- Background: drops column, non-blocking
ALTER TABLE users DROP COLUMN legacy_field;
```

This is a significant differentiator for applications with evolving schemas and large datasets.

---

## 8. WAL Streaming Replication

### The SQLite Pain Point
SQLite has no native replication. Developers who need high availability or read scaling must use external tools like Litestream (WAL shipping to S3), LiteFS (FUSE-based replication), or custom solutions. These add operational complexity, external dependencies, and often introduce consistency trade-offs.

### The DecentDB Win
Build **native WAL streaming** to a standby database with configurable consistency levels:

*   **Async:** Standby lags behind primary; lowest latency on writes.
*   **Sync:** Primary waits for standby acknowledgment before committing; zero data loss.
*   **Quorum:** Primary waits for N of M standbys; balances latency and durability.

```mermaid
sequenceDiagram
    participant App
    participant Primary
    participant Standby

    App->>Primary: INSERT INTO orders ...
    Primary->>Primary: Append to WAL
    Primary->>Standby: Stream WAL frame
    Standby-->>Primary: ACK (sync mode)
    Primary->>Primary: COMMIT
    Primary-->>App: OK
```

This enables embedded databases that survive hardware failures without external tooling.

---

## 9. Bulk Load Follow-Ons

### Current DecentDB Status
DecentDB already ships a bulk load API in the engine. That means bulk ingestion should no longer be treated as a missing differentiator; the roadmap item now is to extend the shipped foundation into a stronger end-user workflow.

### The Next DecentDB Win
Build higher-level bulk-ingest workflows on top of the existing API:

*   COPY-style SQL or CLI commands for CSV/JSON ingestion
*   Streaming readers for datasets larger than memory
*   Smarter sorted-input hints for index-friendly loading
*   Better progress reporting and resumable import ergonomics

```rust
// Rust API
let mut loader = db.bulk_loader()
    .table("events")
    .columns(&["id", "timestamp", "payload"])
    .batch_size(10_000)
    .start()?;

loader.write_row(&[1i64, ts, json])?;
loader.write_row(&[2i64, ts2, json2])?;
loader.finish()?;
```

This makes DecentDB competitive for ETL pipelines and data migration scenarios.

---

## 10. Group Commit / WAL Batching

### The SQLite Pain Point
SQLite's WAL mode commits each transaction with an `fsync()` call. While durable, this creates a hard limit on write throughput — typically 100-500 TPS on consumer SSDs. High-throughput applications must batch at the application layer.

### The DecentDB Win
Implement **group commit** (ADR 0037) that batches multiple concurrent transactions into a single WAL sync:

*   Multiple transactions share a single `fsync()` when committing simultaneously
*   Configurable latency budget (e.g., 1-10ms batching window)
*   No durability compromise — each transaction still gets a committed LSN

```mermaid
sequenceDiagram
    participant T1 as Transaction 1
    participant T2 as Transaction 2
    participant T3 as Transaction 3
    participant WAL as WAL Manager
    participant Disk

    T1->>WAL: COMMIT
    T2->>WAL: COMMIT
    T3->>WAL: COMMIT
    Note over WAL: Batch window (5ms)
    WAL->>Disk: Single fsync() for all 3
    Disk-->>WAL: ACK
    WAL-->>T1: Committed
    WAL-->>T2: Committed
    WAL-->>T3: Committed
```

This can increase write throughput by 3-10x without sacrificing durability.

---

## 11. Cross-Process WAL Coordination

### Current DecentDB Status
DecentDB already has a shared WAL registry for multiple connections inside the same process. That solves same-process visibility and removes reopen churn, but it does not yet deliver true cross-process coordination.

### The SQLite Pain Point
SQLite allows multiple reader processes but only one writer process at a time. Multi-process architectures (common in Electron apps, microservices, or plugin systems) must coordinate writes through connection pooling or external orchestration.

### The DecentDB Win
Extend the current shared-WAL design into **coordinated multi-process access**:

*   Multiple processes can open the same database file
*   Writer coordination via file locks or shared memory
*   Readers see consistent snapshots across process boundaries
*   WAL retention for long-running cross-process readers

This enables architectures where a background sync process writes while a foreground UI process reads — all without external coordination libraries.

---

## Conclusion

DecentDB has already shipped significant differentiators from SQLite, and this document should treat them as foundations rather than future ideas:

*   **Native rich types:** TIMESTAMP, UUID, DECIMAL with proper storage formats
*   **Advanced indexing:** Trigram indexes for `LIKE '%pattern%'`, plus shipping expression and partial indexes within the current v1 subset
*   **Modern SQL:** Recursive CTEs, savepoints, generated columns, temp tables, and a focused first slice of window functions
*   **Upsert support:** `INSERT ... ON CONFLICT DO UPDATE/NOTHING` and `INSERT ... RETURNING`
*   **ORM integration:** Native EF Core provider with query translation
*   **Developer experience:** In-memory VFS for testing, cost-based optimizer, bulk load API, and shared WAL visibility across same-process connections

The remaining roadmap items now break cleanly into two groups: short-horizon finish work and long-horizon platform bets.

| Category | Features |
|----------|----------|
| **Finish the Surface Area** | JSON table functions/operators, Date/Time builtins, UUID generation, Planner statistics / `ANALYZE` |
| **Performance** | JSONB, Group Commit, Bulk Load follow-ons |
| **Concurrency** | MVCC, Cross-process WAL coordination |
| **Real-time** | CDC / Reactive Subscriptions |
| **AI/ML** | Vector / HNSW Index |
| **Security** | Transparent Data Encryption |
| **Search** | Full-Text Search with BM25 |
| **Operations** | Non-blocking schema migration, WAL streaming replication |

By executing on these features, DecentDB shifts from being "another embedded database" to an indispensable, modern infrastructure component that actively solves the hardest parts of local-first development, AI integration, and high-concurrency embedded systems.
