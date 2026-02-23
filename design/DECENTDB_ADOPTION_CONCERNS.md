# DecentDB vs SQLite: Technology Selection Guide

## **Last Updated**: 2026-02-23 14:01

## Introduction

This document provides a comprehensive feature comparison between DecentDB and SQLite to help developers make an informed decision when selecting an embedded database for new projects (greenfield development).

**Important Note:** DecentDB is NOT intended as a drop-in replacement for SQLite. Both databases are valid choices for embedded applications, but they prioritize different trade-offs:

- **SQLite** prioritizes flexibility, broad SQL support, and universal compatibility
- **DecentDB** prioritizes durable ACID writes, predictable performance, and PostgreSQL-compatible SQL within a single-process model

This comparison helps you understand which database aligns with your project's specific requirements.

## 1. Concurrency and Multi-Process Support

**SQLite:**

- Supports multiple **processes** accessing the same database file concurrently (with varying degrees of concurrency depending on the journal mode, e.g., WAL mode allows concurrent readers and one writer across processes).
- Relies on OS-level file locking (`flock()`, `fcntl()` advisory locks) to manage cross-process concurrency.

**DecentDB:**

- **Architectural Constraint:** Multi-process concurrency / shared-memory locking is explicitly **out of scope** by design.
- **Impact:** DecentDB supports multiple concurrent reader **threads** within a _single process_, but it is strictly limited to a single-process concurrency model. Applications that rely on multiple independent **processes** (e.g., a web server with multiple worker processes like Gunicorn/PHP-FPM, or a background job processor running in a separate OS process from the main application) accessing the same embedded database simultaneously require a different architecture (e.g., proxy/service layer) or should choose SQLite.

### Detailed Analysis

| Aspect                         | SQLite                       | DecentDB                        | Implication                                                    |
| ------------------------------ | ---------------------------- | ------------------------------- | -------------------------------------------------------------- |
| **Cross-process readers**      | ✅ Supported via WAL mode    | ❌ Not supported                | Choose SQLite for multi-process web servers or job queues      |
| **Cross-process writers**      | ✅ Serialized via file locks | ❌ Not supported                | Choose SQLite if multiple OS processes need write access       |
| **Shared-memory coordination** | ✅ `-shm` file for WAL index | ❌ No equivalent                | DecentDB relies on single-process shared memory                |
| **OS-level file locking**      | ✅ `flock()` / `LockFile`    | ❌ No implementation            | `src/vfs/os_vfs.nim` uses Nim locks only, no OS advisory locks |
| **Multi-threaded readers**     | ✅ Supported                 | ✅ Supported                    | Both support concurrent reader threads via snapshot isolation  |
| **Thread-local storage**       | N/A                          | ✅ Heavy use of `{.threadvar.}` | `gInsertValues`, `gEvalContext`, `gTriggerExecutionDepth`      |

**Code Evidence:**

- Thread-local variables: `src/engine.nim:1522`, `src/engine.nim:2062`, `src/exec/exec.nim` (multiple locations)
- Internal locking: `src/pager/pager.nim` (`overlayLock`, `rollbackLock`, cache entry locks)
- No OS file locking: Search for `flock`, `fcntl`, `LockFile` in `src/` returns zero results

**Architectural Decision:** This is a deliberate design choice documented in `AGENTS.md`. It enables simpler testing, stronger durability guarantees, and eliminates an entire class of cross-process coordination bugs.

**Choose SQLite when:** Your application uses multiple OS processes that need database access (web workers, background jobs, microservices).

**Choose DecentDB when:** Your application runs in a single process with multiple threads (desktop application, embedded system, single-service architecture).

---

## 2. Extensibility and Ecosystem

**SQLite:**

- Boasts a massive ecosystem of loadable extensions (e.g., JSON1, FTS5, GeoPoly, spatialite).
- Allows application-defined SQL functions (UDFs - User Defined Functions) in various languages.
- Supports Virtual Tables, allowing SQLite to query external data sources or expose custom data structures as tables.

**DecentDB:**

- **Design Decision:** No loadable extension or UDF plugin surface. Extending the database requires contributing to the core engine (written in Nim).
- **Rationale:** DecentDB provides a constrained, well-tested feature set. Functionality is added through the core development process rather than external plugins, ensuring type safety, memory safety, and ACID compliance.

### Detailed Analysis

| Feature                                | SQLite                       | DecentDB                                         | Implication                                                          |
| -------------------------------------- | ---------------------------- | ------------------------------------------------ | -------------------------------------------------------------------- |
| **Loadable extensions (`.so`/`.dll`)** | ✅ `load_extension()`        | ❌ Not available                                 | SQLite: extend at runtime; DecentDB: extend via PR                   |
| **User-defined functions (UDFs)**      | ✅ `CREATE FUNCTION`         | ❌ Not available                                 | SQLite: custom logic in SQL; DecentDB: logic in application code     |
| **Virtual tables**                     | ✅ `sqlite3_create_module()` | ❌ Not available                                 | SQLite: expose external data as tables; DecentDB: ETL to real tables |
| **Custom collations**                  | ✅ `CREATE COLLATION`        | ❌ Not available                                 | Both: limited to built-in string comparison                          |
| **Custom aggregates**                  | ✅ Application-defined       | ❌ Not available                                 | DecentDB: must use built-in aggregates only                          |
| **Language bindings**                  | ✅ 30+ languages             | ⚠️ 4 languages (Python, Node, Go, .NET)          | C API exists; choose based on your language ecosystem                |
| **JSON1 extension**                    | ✅ Full JSON1                | ⚠️ Partial (`JSON_ARRAY_LENGTH`, `JSON_EXTRACT`) | DecentDB covers common JSON use cases                                |
| **FTS5 extension**                     | ✅ Full FTS5                 | ❌ Not available                                 | DecentDB: trigram index for substring search only                    |

**Code Evidence:**

- No extension loading code: Search for `load_extension`, `createFunction`, `udf` returns zero results in `src/`
- C API surface: `src/c_api.nim` provides FFI but no hook for user-defined functions
- Built-in functions only: All functions hardcoded in `src/exec/exec.nim`

**Adding Functions to DecentDB:**
Unlike SQLite's runtime extension loading, adding functions to DecentDB requires:

1. Modifying `src/exec/exec.nim` to add function implementation
2. Submitting a pull request with tests
3. Waiting for next release

**Choose SQLite when:** You need specialized extensions (geospatial, custom crypto, virtual table adapters) or want to prototype with runtime-loaded functionality.

**Choose DecentDB when:** You prefer a constrained, audited feature set where all functionality is covered by the project's test suite and ACID guarantees.

---

## 3. SQL Dialect and Feature Breadth

**SQLite:**

- Supports a very broad, flexible (and sometimes idiosyncratic) SQL dialect.
- "Duck typing" (type affinity) allows storing almost any data type in any column.
- Extensive support for advanced SQL features (recursive CTEs, a vast array of built-in scalar and aggregate functions).

**DecentDB:**

- **Design Decision:** Implements a deliberately constrained, PostgreSQL-like SQL subset with strict typing.
- **Rationale:** Predictable behavior, easier testing, and compatibility with PostgreSQL migration paths.

### Supported vs Available SQL Features

#### DDL (Data Definition Language)

| Feature                         | SQLite | DecentDB             | Notes                                            |
| ------------------------------- | ------ | -------------------- | ------------------------------------------------ |
| `CREATE TABLE`                  | ✅     | ✅                   | Column constraints + CHECK supported; table-level FKs and composite PK/UNIQUE are not supported in 0.x |
| `CREATE INDEX`                  | ✅     | ✅                   | B-tree, unique, partial, expression indexes      |
| `CREATE VIEW`                   | ✅     | ✅                   | Including `OR REPLACE`, column aliases           |
| `CREATE TRIGGER`                | ✅     | ✅                   | Row-level only; `AFTER` + `INSTEAD OF` only (no `BEFORE`); action via `decentdb_exec_sql(...)` |
| `CREATE TEMP TABLE`             | ✅     | ❌ **Not available** | DecentDB: use regular tables with manual cleanup |
| `CREATE TEMP VIEW`              | ✅     | ❌ **Not available** | DecentDB: use regular views with manual cleanup  |
| `DROP TABLE/INDEX/VIEW/TRIGGER` | ✅     | ✅                   |                                                  |
| `ALTER TABLE`                   | ✅     | ✅                   | ADD/DROP/RENAME COLUMN, RENAME TABLE             |
| `ATTACH DATABASE`               | ✅     | ❌ **Not available** | DecentDB: single database per connection         |

#### DML (Data Manipulation Language)

| Feature                  | SQLite | DecentDB             | Notes                                           |
| ------------------------ | ------ | -------------------- | ----------------------------------------------- |
| `SELECT`                 | ✅     | ✅                   | Full support with WHERE, ORDER BY, LIMIT/OFFSET |
| `INSERT`                 | ✅     | ✅                   | Including multi-row, `INSERT...SELECT`          |
| `UPDATE`                 | ✅     | ✅                   |                                                 |
| `DELETE`                 | ✅     | ✅                   |                                                 |
| `UPSERT` (`ON CONFLICT`) | ✅     | ✅                   | Both support `DO UPDATE`, `DO NOTHING`          |
| `RETURNING` clause       | ✅     | ✅                   | Both support on INSERT/UPDATE/DELETE            |
| `INNER JOIN`             | ✅     | ✅                   |                                                 |
| `LEFT JOIN`              | ✅     | ✅                   |                                                 |
| `CROSS JOIN`             | ✅     | ⚠️                   | DecentDB: via comma syntax                      |
| `FULL OUTER JOIN`        | ✅     | ❌ **Not available** |                                                 |
| `RIGHT JOIN`             | ✅     | ❌ **Not available** |                                                 |
| `GROUP BY`               | ✅     | ✅                   |                                                 |
| `HAVING`                 | ✅     | ✅                   |                                                 |
| `DISTINCT`               | ✅     | ✅                   |                                                 |
| `UNION/UNION ALL`        | ✅     | ✅                   |                                                 |
| `INTERSECT`              | ✅     | ✅                   |                                                 |
| `EXCEPT`                 | ✅     | ✅                   |                                                 |
| Set-op `ORDER BY/LIMIT`  | ✅     | ❌ **Not available** | DecentDB rejects `ORDER BY/LIMIT/OFFSET` directly on set ops in 0.x |
| CTEs (`WITH`)            | ✅     | ✅                   | Non-recursive only in DecentDB                  |
| **Recursive CTEs**       | ✅     | ❌ **Not available** | Parser rejects: `src/sql/sql.nim`               |
| Subqueries               | ✅     | ✅                   | Scalar, `IN`, `EXISTS`                          |

#### Transaction Control

| Feature                              | SQLite | DecentDB             | Notes                             |
| ------------------------------------ | ------ | -------------------- | --------------------------------- |
| `BEGIN`                              | ✅     | ✅                   |                                   |
| `COMMIT`                             | ✅     | ✅                   |                                   |
| `ROLLBACK`                           | ✅     | ✅                   |                                   |
| **SAVEPOINT**                        | ✅     | ❌ **Not available** | DecentDB: flat transactions only  |
| `BEGIN DEFERRED/IMMEDIATE/EXCLUSIVE` | ✅     | ❌ **Not available** | DecentDB: single transaction mode |

#### Scalar Functions

| Category        | Functions                                         | SQLite | DecentDB             |
| --------------- | ------------------------------------------------- | ------ | -------------------- |
| **Math**        | `ABS`, `FLOOR`, `ROUND`                           | ✅     | ✅                   |
|                 | `POWER`, `SQRT`, `MOD`                            | ✅     | ❌ **Not available** |
| **String**      | `LENGTH`, `LOWER`, `UPPER`, `TRIM`, `REPLACE`     | ✅     | ✅                   |
|                 | `SUBSTR`/`SUBSTRING`                              | ✅     | ✅                   |
|                 | `INSTR`, `CHAR`, `HEX`                            | ✅     | ❌ **Not available** |
|                 | `PRINTF`                                          | ✅     | ✅                   |
| **Aggregate**   | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`               | ✅     | ✅                   |
|                 | `GROUP_CONCAT`/`STRING_AGG`                       | ✅     | ✅                   |
|                 | `TOTAL`, `AVG(DISTINCT)`                          | ✅     | ❌ **Not available** |
| **Date/Time**   | `NOW()`, `DATE()`, `DATETIME()`, `STRFTIME()`     | ✅     | ❌ **Not available** |
|                 | `JULIANDAY`, `UNIXEPOCH`                          | ✅     | ❌ **Not available** |
| **JSON**        | `JSON_ARRAY_LENGTH`, `JSON_EXTRACT`               | ✅     | ✅                   |
|                 | `JSON_EACH`, `JSON_TREE`                          | ✅     | ❌ **Not available** |
|                 | `->`, `->>` operators                             | ✅     | ❌ **Not available** |
| **UUID**        | `GEN_RANDOM_UUID`, `UUID_PARSE`, `UUID_TO_STRING` | ❌     | ✅                   |
| **Window**      | `ROW_NUMBER`, `RANK`, `DENSE_RANK`                | ✅     | ✅                   |
|                 | `LAG`, `LEAD`                                     | ✅     | ✅                   |
|                 | `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`          | ✅     | ❌ **Not available** |
| **Conditional** | `COALESCE`, `NULLIF`                              | ✅     | ✅                   |
|                 | `CASE`                                            | ✅     | ✅                   |
|                 | `IF`, `IFF`                                       | ✅     | ❌ **Not available** |
| **Crypto**      | `MD5`, `SHA1`, `SHA256`                           | ✅     | ❌ **Not available** |

#### Behavioral Differences

| Behavior            | SQLite                   | DecentDB              | Implication                                       |
| ------------------- | ------------------------ | --------------------- | ------------------------------------------------- |
| **Type system**     | Type affinity (flexible) | Strict typing         | DecentDB enforces declared types at insert/update |
| **Identifier case** | Preserves case           | Lowercases by default | DecentDB: quote identifiers to preserve case      |
| **String literals** | `'text'` and `"text"`    | `'text'` only         | DecentDB: double quotes reserved for identifiers  |
| **BLOB literals**   | `X'...'` hex             | ❌ Not supported      | DecentDB: use parameterized queries for blobs     |
| **AUTOINCREMENT**   | Supported                | ❌ Not available      | DecentDB: use `INTEGER PRIMARY KEY`               |
| **Rowid**           | Implicit                 | Implicit              | Same behavior for `INTEGER PRIMARY KEY`           |

#### SQLite Features Without a PostgreSQL Equivalent

DecentDB intentionally uses PostgreSQL-style parsing and (where possible) PostgreSQL-like semantics. Some SQLite features do not have a true PostgreSQL equivalent, which makes “Postgres-syntax parity” and “SQLite feature parity” fundamentally in tension for these items:

| SQLite feature / concept | PostgreSQL equivalent? | Notes |
| --- | --- | --- |
| `PRAGMA ...` (hundreds of runtime/config/introspection knobs) | ❌ No direct equivalent | PostgreSQL uses `SET`, `SHOW`, and system catalogs, but there is no single `PRAGMA` surface and many SQLite pragmas are SQLite-specific (journaling, page cache knobs, etc.). |
| Implicit `rowid` / `ROWID` / `_rowid_` pseudo-column | ❌ No stable equivalent | PostgreSQL has `ctid` (physical tuple location) but it is not a stable logical row identifier and is not rowid-compatible. |
| `WITHOUT ROWID` tables | ❌ No equivalent | PostgreSQL tables always have a physical tuple identity; “WITHOUT ROWID” is a SQLite storage layout feature. |
| `ATTACH DATABASE` (multiple DB files in one connection) | ❌ No equivalent | PostgreSQL has schemas and FDWs, but it does not “attach” arbitrary database files into a single connection the way SQLite does. |
| `sqlite_master` / `sqlite_schema` compatibility expectations | ❌ No equivalent | PostgreSQL exposes metadata via catalogs (`pg_catalog.*`, `information_schema.*`), not SQLite’s schema tables. |

**Code Evidence:**

- Function implementations: `src/exec/exec.nim` (lines 2250-2400+)
- Recursive CTE rejection: `src/sql/sql.nim` returns error "WITH RECURSIVE is not supported"
- TEMP rejection: `src/sql/sql.nim` returns error "TEMP/TEMPORARY VIEW not supported"

**Choose SQLite when:** You need recursive CTEs for hierarchical data, extensive date/time handling in SQL, or prefer flexible typing.

**Choose DecentDB when:** You want strict typing enforcement, PostgreSQL-compatible syntax, and a SQL subset that fits in your head.

---

## 4. Full-Text Search (FTS)

**SQLite:**

- Provides robust, industry-standard Full-Text Search via the FTS4 and FTS5 extensions.

**DecentDB:**

- **Design Decision:** No traditional Full-Text Search (tokenization, stemming, ranking).
- **Alternative:** Provides a built-in trigram inverted index specifically for `LIKE '%pattern%'` substring searches.
- **Rationale:** The trigram index solves the most common use case (substring search) without the complexity of tokenizers, stemmers, and BM25 scoring.

### FTS Comparison

| Feature                             | SQLite (FTS5)      | DecentDB         | Use Case                         |
| ----------------------------------- | ------------------ | ---------------- | -------------------------------- |
| **Substring search (`LIKE '%x%')`** | Table scan         | ✅ Trigram index | DecentDB is significantly faster |
| **Word tokenization**               | ✅ Supported       | ❌ Not available | Document search, word boundaries |
| **Stemming**                        | ✅ Supported       | ❌ Not available | "run" matching "running"         |
| **Stop words**                      | ✅ Configurable    | ❌ Not available | Filtering common words           |
| **Relevance ranking (BM25)**        | ✅ Supported       | ❌ Not available | Search result ranking            |
| **Highlighting**                    | ✅ `highlight()`   | ❌ Not available | Showing match context            |
| **Multi-language**                  | ✅ ICU integration | ❌ Not available | Unicode normalization            |

**Implementation Note:** The trigram index (`src/search/search.nim`) is optimized for exact substring matching but cannot be extended to full-text search without significant additional work (tokenizers, stemmers, new postings format).

**Choose SQLite when:** You need document search with ranking, stemming, or multi-language support.

**Choose DecentDB when:** Your search needs are limited to substring matching (e.g., email search, code search, log filtering).

**Hybrid Approach:** Consider using DecentDB for transactional data and a dedicated search engine (Elasticsearch, Typesense) for full-text search.

---

## 5. In-Memory and Temporary Databases

**SQLite:**

- Robust support for purely in-memory databases (`:memory:`) and temporary databases that are automatically deleted.
- Supports `TEMP` views and tables.

**DecentDB:**

- **Design Constraint:** No support for `TEMP` views or `TEMP` tables.
- **Available:** Purely in-memory databases via the `:memory:` connection string (utilizes an in-memory Virtual File System).

### Temporary Objects Comparison

| Feature                     | SQLite                        | DecentDB         | Workaround for DecentDB                |
| --------------------------- | ----------------------------- | ---------------- | -------------------------------------- |
| **`:memory:` database**     | ✅                            | ✅               | Same functionality                     |
| **File-based temporary DB** | ✅ Auto-deleted               | ❌ Not available | Use regular DB + manual cleanup        |
| **`CREATE TEMP TABLE`**     | ✅ Session-scoped             | ❌ Not available | Create regular table + `DROP` manually |
| **`CREATE TEMP VIEW`**      | ✅ Session-scoped             | ❌ Not available | Create regular view + `DROP` manually  |
| **Temp isolation**          | ✅ Only visible to connection | N/A              | Use connection-specific prefixes       |

**Code Evidence:**

- In-memory VFS: `src/vfs/mem_vfs.nim` (fully functional)
- TEMP rejection: `src/sql/sql.nim` explicitly returns error for `CREATE TEMP VIEW`

**Choose SQLite when:** You rely heavily on TEMP tables for complex queries, testing, or ETL workflows.

**Choose DecentDB when:** You can structure queries without TEMP objects or handle cleanup manually.

---

## 6. Portability and Language Support

**SQLite:**

- Written in highly portable C. It compiles and runs virtually everywhere.
- Bindings exist for almost every programming language in existence.

**DecentDB:**

- Written in Nim (compiles to C).
- **Trade-off:** Smaller toolchain ecosystem but yields benefits in readability, correctness, and single-binary compilation without C-dependency hell.

### Language Binding Status

| Language    | Status           | API Type                           |
| ----------- | ---------------- | ---------------------------------- |
| **Python**  | ✅ Available     | DB-API 2.0 + SQLAlchemy dialect    |
| **Node.js** | ✅ Available     | Native addon + Knex adapter        |
| **Go**      | ✅ Available     | CGO bindings                       |
| **.NET**    | ✅ Available     | Native bindings + EF Core provider |
| **Java**    | ❌ Not available | Would require JNI wrapper          |
| **Ruby**    | ❌ Not available | Would require C extension          |
| **Rust**    | ❌ Not available | Would require FFI bindings         |
| **PHP**     | ❌ Not available | Would require extension            |
| **Perl**    | ❌ Not available |                                    |
| **R**       | ❌ Not available |                                    |

**Platform Support:**

- ✅ Linux (primary development target)
- ✅ macOS
- ✅ Windows (limited testing)
- ❌ iOS (no Swift bindings)
- ❌ Android (would require NDK build)
- ❌ WebAssembly (would require WASM target)

**Choose SQLite when:** You need bindings for a language not listed above or require maximum platform portability.

**Choose DecentDB when:** Your project uses one of the supported languages and you value the Nim-based architecture benefits.

---

## 7. Additional Comparison Areas

### Security & Encryption

| Feature                             | SQLite            | DecentDB         |
| ----------------------------------- | ----------------- | ---------------- |
| **Database encryption (SQLCipher)** | ✅ Available      | ❌ Not available |
| **Password protection**             | ✅ Available      | ❌ Not available |
| **Audit logging**                   | ✅ Via extensions | ❌ Not available |

**Recommendation:** If encryption-at-rest is required, choose SQLCipher (SQLite fork) or handle encryption at the filesystem level.

### Administration & Tooling

| Feature                   | SQLite                      | DecentDB            |
| ------------------------- | --------------------------- | ------------------- |
| **CLI shell**             | ✅ `sqlite3`                | ✅ `decentdb`       |
| **Backup API**            | ✅ `sqlite3_backup_*`       | ⚠️ Checkpoint-based |
| **Integrity check**       | ✅ `PRAGMA integrity_check` | ⚠️ Partial          |
| **Query planner hints**   | ✅ Various `PRAGMA`s        | ❌ Limited          |
| **Performance profiling** | ✅ `EXPLAIN QUERY PLAN`     | ✅ `EXPLAIN`        |

### Data Types & Storage

| Feature             | SQLite               | DecentDB         | Notes                                 |
| ------------------- | -------------------- | ---------------- | ------------------------------------- |
| **Type affinity**   | ✅ Flexible          | ❌ Strict        | DecentDB enforces declared types      |
| **Date/Time types** | ✅ Storage classes   | ❌ Not available | Use INT/TEXT + application logic      |
| **Compression**     | ✅ Optional (ZIPVFS) | ✅ Built-in      | DecentDB has transparent compression  |
| **Overflow pages**  | ✅ Supported         | ✅ Supported     | Large TEXT/BLOB handled automatically |

---

## Selection Decision Matrix

### Choose DecentDB When:

✅ **Single-process application** (desktop app, embedded system, single-service backend)
✅ **Durability is critical** (financial data, audit logs, state management)
✅ **Complex multi-table transactions** with foreign keys
✅ **Need ACID guarantees** with minimal configuration
✅ **Prefer PostgreSQL-compatible SQL** subset
✅ **Want static binary** without C dependencies
✅ **Using Python, Node.js, Go, or .NET**
✅ **Substring search is sufficient** (trigram index)
✅ **Strict typing enforcement** is preferred

### Choose SQLite When:

✅ **Multiple processes need database access** (web workers, background jobs, microservices)
✅ **Need FTS5** for document search with ranking/stemming
✅ **Require recursive CTEs** for hierarchical data
✅ **Need extensive custom extensions** (geospatial, crypto, etc.)
✅ **Require encryption-at-rest** (SQLCipher)
✅ **Need maximum language binding ecosystem** (30+ languages)
✅ **Heavy use of TEMP tables/views** in queries
✅ **Prefer flexible typing** (store any value in any column)
✅ **Need date/time functions** in SQL

---

## Implementation Priority (For DecentDB Maintainers)

If adding features to reduce friction for SQLite users (while maintaining architectural principles), priority from lowest to highest effort:

1.  **Date/Time Functions** (Low LOE): Add `NOW()`, `DATE()`, `DATETIME()` to `src/exec/exec.nim`
2.  **Additional String Functions** (Low LOE): `SUBSTR`, `INSTR`, `CHAR`, `HEX`
3.  **TEMP Tables/Views** (Medium LOE): Leverage existing `MemVfs` for session-scoped objects
4.  **More JSON Functions** (Medium LOE): `JSON_EACH`, `JSON_TREE`, operators
5.  **Traditional FTS** (High LOE): Tokenizers, stemmers, BM25 scoring—requires significant work
6.  **UDF/Extension API** (Very High LOE): C-compatible ABI for Nim—complex and risky
7.  **Multi-Process Support** (Highest LOE): Abandons core architectural principle

---

## Summary

DecentDB and SQLite are both excellent embedded databases that serve different needs:

- **SQLite** is the universal choice when you need maximum flexibility, broad ecosystem, and multi-process support.
- **DecentDB** is the specialized choice when you prioritize durability, predictable performance, and PostgreSQL compatibility within a single-process model.

Neither is objectively "better"—they optimize for different constraints. This document helps you match your project's requirements to the right tool.

**Bottom Line:**

- Starting a new project with a single-process architecture? Consider DecentDB.
- Need maximum compatibility and flexibility? Choose SQLite.
- Already using SQLite successfully? There's no compelling reason to switch—DecentDB is for new projects making a fresh choice.
