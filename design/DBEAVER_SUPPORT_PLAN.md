# DBeaver support plan for DecentDB

**Date:** 2026-02-24

This document is a practical, engineering-oriented plan for enabling **DecentDB** to be used from **DBeaver**.

It captures:

- What DBeaver expects for new “database integrations” (providers, drivers, dialects)
- Minimal viable integration options for DecentDB
- What we would likely need to build on the DecentDB side (the gating factor)
- A staged approach so we can get something working quickly and iterate

> Key takeaway: **DBeaver is primarily JDBC-centric.** The simplest path is to provide a **JDBC driver** for DecentDB (even a thin one), then either:
> 1) rely on DBeaver’s “Generic” JDBC support with a manually added driver, or
> 2) ship a small DBeaver extension that pre-registers the driver/provider and a DecentDB SQL dialect.

---

## 1) Goals / non-goals

### Goals

- Connect to a DecentDB database from DBeaver.
- Execute SQL queries and view results.
- Browse basic metadata (tables, columns, indexes) using standard JDBC metadata APIs.
- Keep the first version minimal and robust.

### Non-goals (initially)

- Implementing every DBeaver feature (ER diagrams, advanced DDL editors, custom explain plans, etc.).
- Multi-process server mode or a network protocol, unless we later decide it’s necessary and create an ADR.
- Rewriting DecentDB’s SQL dialect to “match PostgreSQL” for UI convenience.

---

## 2) What DBeaver expects (findings from DBeaver source)

DBeaver is an Eclipse/OSGi application. Database integrations are typically delivered as plugins (bundles) contributing to extension points.

### 2.1 Data source providers

DBeaver loads “data source providers” from the extension point:

- `org.jkiss.dbeaver.dataSourceProvider`

Providers are registered and loaded via DBeaver’s extension registry. Providers commonly subclass:

- `org.jkiss.dbeaver.model.impl.jdbc.JDBCDataSourceProvider` or
- `org.jkiss.dbeaver.ext.generic.GenericDataSourceProvider`

In practice, many databases with “normal JDBC behavior” use `GenericDataSourceProvider` and customize only what they must.

**Pattern:**

- Provider ID (e.g., `sqlite`, `duckdb`) + one or more driver definitions.

### 2.2 Drivers

Drivers are typically declared in `plugin.xml` under the provider extension contribution.

A driver definition typically includes:

- Driver ID (e.g., `sqlite_jdbc`, `duckdb_jdbc`)
- Driver class (e.g., `org.sqlite.JDBC`)
- Sample URL template
- Default host/port/database (if relevant)
- One or more libraries (bundled JARs, Maven artifacts, etc.)

DBeaver internally addresses drivers using a composite reference:

- `(providerId, driverId)` (often represented as a composite object id)

This matters when wiring UI helpers like “file database handlers” that open a local DB file and attach it to a specific driver.

### 2.3 SQL dialects

SQL dialects are registered via the extension point:

- `org.jkiss.dbeaver.sqlDialect`

Dialect implementations commonly extend:

- `org.jkiss.dbeaver.ext.generic.model.GenericSQLDialect` (generic JDBC-friendly base) or
- `org.jkiss.dbeaver.model.impl.jdbc.JDBCSQLDialect` (more direct JDBC dialect base)

**Examples from DBeaver:**

- SQLite: `SQLiteSQLDialect extends GenericSQLDialect` and configures identifier quoting, function lists, feature flags, etc.
- DuckDB: `DuckDBSQLDialect extends GenericSQLDialect`, adds keywords like `INSTALL`, `LOAD`, etc.

A dialect is not strictly required for “query execution” to work, but it strongly improves:

- SQL editor highlighting/formatting
- Autocomplete keyword sets
- DDL generation and feature gating

---

## 3) The real gating factor: how will DecentDB be reachable from Java?

DBeaver is Java. The most straightforward integration paths into DBeaver are:

1) **JDBC driver** (most common)
2) **ODBC** (possible, but DBeaver support varies by edition/extension and often still involves a JDBC bridge)
3) A bespoke DBeaver provider that doesn’t use JDBC (rare; most DBeaver internals assume JDBC-like behavior)

Therefore, the core question is:

> Do we have (or can we build) a DecentDB JDBC driver that implements enough of `java.sql` to satisfy DBeaver?

### 3.1 Minimal JDBC driver expectations (for DBeaver)

For DBeaver to be usable, the driver should provide:

- `java.sql.Driver` implementation that can `connect(url, props)`
- `Connection`, `PreparedStatement`, `Statement`, `ResultSet`
- Metadata:
  - `DatabaseMetaData.getTables`, `getColumns`, `getPrimaryKeys`, `getIndexInfo`, etc.

DBeaver can tolerate imperfect metadata (many real-world drivers have quirks), but missing basics will hurt the UX.

### 3.2 Design constraint: DecentDB is currently a single-process DB

This repository’s North Star and constraints emphasize:

- Durable ACID writes
- One writer / many reader threads
- Single-process model

This influences how we should expose it to Java:

- **Embedded JDBC** (Java loads native library and talks in-process) aligns with “single process” semantics.
- “Client-server over TCP” mode is a bigger step with more surface area (auth, protocol stability, concurrency semantics) and would almost certainly need an ADR.

---

## 4) Integration options (recommended path first)

### Option A (recommended): Provide an embedded JDBC driver + minimal DBeaver extension

**What you ship:**

- A DecentDB JDBC driver JAR
- A small DBeaver plugin that:
  - registers a provider + driver (so users don’t manually configure)
  - registers a `DecentDBSQLDialect`
  - optionally provides a “file database handler” for `.ddb` files (nice-to-have)

**Pros**

- Best DBeaver UX (DecentDB appears as a first-class database type)
- Keeps the driver and dialect “discoverable” and defaulted
- Compatible with DBeaver’s normal workflows

**Cons**

- Requires writing and maintaining a JDBC driver
- If the driver is JNI-backed, it requires careful packaging per OS/arch

### Option B (fastest initial): Provide only a JDBC driver; no DBeaver plugin

**What the user does:**

- In DBeaver: Database → Driver Manager → New
- Add the DecentDB JDBC driver JAR
- Specify:
  - driver class
  - URL template

**Pros**

- No DBeaver plugin development upfront
- Great for early testing and iteration

**Cons**

- Worse UX: manual driver setup
- No DecentDB-specific defaults (dialect, keywords, behavior flags)

### Option C: Use ODBC (only if we already have an ODBC story)

If DecentDB had an ODBC driver, we could evaluate DBeaver’s ODBC support.

**Caution:** ODBC is not the “default path” in DBeaver compared to JDBC. Depending on the distribution (Community vs. Enterprise) and available extensions, this can mean:

- An ODBC-to-JDBC bridge layer
- Extra native dependencies

This is likely **not** the simplest path unless DecentDB already ships a strong ODBC driver.

### Option D: Build a DecentDB server + JDBC network driver (big change)

This is a larger architectural investment. It would touch concurrency semantics, durability behavior under network clients, authentication, and protocol stability.

Per repo process, this almost certainly requires an ADR before implementation.

---

## 5) What the minimal DBeaver plugin looks like

This section describes the plugin “shape” you would create in a separate repository (or in a dedicated integration repo).

### 5.1 Modules

DBeaver often splits database support into:

- a **model** plugin (core integration, JDBC provider, dialect, metadata tweaks)
- an optional **UI** plugin (connection wizard pages, file handlers)

For a minimal DecentDB integration, you can start with just the **model** plugin.

### 5.2 Provider + driver registration (`plugin.xml`)

You contribute to:

- `org.jkiss.dbeaver.dataSourceProvider`

and declare:

- provider id: `decentdb`
- driver id: `decentdb_jdbc`
- driver class: `com.yourorg.decentdb.jdbc.DecentDBDriver` (example)
- sample URL: something like `jdbc:decentdb:{path}` (example)

**Important:** the URL and driver class must match what your JDBC driver actually supports.

### 5.3 SQL dialect registration (`plugin.xml`)

You contribute to:

- `org.jkiss.dbeaver.sqlDialect`

and implement:

- `DecentDBSQLDialect extends GenericSQLDialect` (most likely)

At minimum, this dialect would:

- set a name and id (e.g., `super("DecentDB", "decentdb")`)
- define identifier quoting if it differs from defaults
- define feature flags (ALTER TABLE support, multi-value insert mode, etc.)
- add DecentDB keywords to improve editor UX

This can start tiny and grow as DecentDB’s SQL surface is clarified.

### 5.4 Minimal `plugin.xml` skeleton (copy/paste starting point)

Below is an intentionally small `plugin.xml` sketch showing the shape you need.

Notes:

- The attribute names in DBeaver’s extension points can evolve; treat this as a starting point and validate against the current DBeaver source.
- The key part is: a **provider** contribution that declares a **driver** and points at your JDBC driver class.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<plugin>

  <!-- Data source provider + predefined driver -->
  <extension point="org.jkiss.dbeaver.dataSourceProvider">
    <provider
        id="decentdb"
        name="DecentDB"
        description="DecentDB embedded database"
        class="org.jkiss.dbeaver.ext.generic.GenericDataSourceProvider"
        driversManageable="true">

      <driver
          id="decentdb_jdbc"
          label="DecentDB (Embedded)"
          class="com.decentdb.jdbc.DecentDBDriver"
          sampleURL="jdbc:decentdb:{file}"
          embedded="true"
          threadSafe="true"
          instantiable="true"/>

      <!-- If you bundle the driver jar with the plugin, add a library reference.
           DBeaver supports multiple library source types (bundle, Maven artifact, etc.).
           Use the same style as existing extensions in DBeaver's tree. -->
      <!--
      <driver id="decentdb_jdbc" ...>
        <file type="bundle" path="drivers/decentdb-jdbc.jar" />
      </driver>
      -->
    </provider>
  </extension>

  <!-- Optional: a SQL dialect. Highly recommended for editor UX. -->
  <extension point="org.jkiss.dbeaver.sqlDialect">
    <dialect
        id="decentdb"
        label="DecentDB"
        class="com.decentdb.dbeaver.DecentDBSQLDialect"
        hidden="false"/>
  </extension>

</plugin>
```

### 5.5 What *not* to do in the first plugin

- Don’t fork DBeaver or add large UI flows.
- Don’t implement a custom DBeaver “native” provider unless you have a strong reason.
- Don’t over-customize metadata/DDL generation in the plugin until the JDBC driver is solid.

---

## 6) What the minimal JDBC driver looks like (DecentDB side)

This repository already has multiple language bindings and a C API build output directory, which suggests a plausible JNI-backed JDBC driver design:

- Java: implement `java.sql.*` interfaces
- Native: call into DecentDB C API (or a dedicated stable native ABI for JDBC)

### 6.1 Driver architecture sketch

- `DecentDBDriver` (`java.sql.Driver`):
  - Parse `jdbc:decentdb:` URL
  - Load native library
  - Return a `DecentDBConnection`

- `DecentDBConnection`:
  - Wrap a native connection handle
  - Provide transaction boundaries (`setAutoCommit`, `commit`, `rollback`)

- `DecentDBPreparedStatement` / `DecentDBStatement`:
  - Prepare/execute statements in native layer
  - Bind parameters
  - Stream result rows

- `DecentDBResultSet`:
  - Provide typed getters
  - Provide `ResultSetMetaData`

- `DecentDBDatabaseMetaData`:
  - Use DecentDB’s catalog tables or built-in metadata to implement JDBC metadata queries

### 6.2 Where JDBC gets tricky

- Type mapping: DecentDB types → JDBC types (`java.sql.Types`)
- NULL semantics
- Column metadata completeness (precision/scale, nullability)
- Transaction isolation mapping (DecentDB snapshot semantics vs JDBC isolation levels)
- Concurrency and statement handle lifetimes

For correctness, we should be explicit:

- Which JDBC isolation levels map to DecentDB behavior (and which are rejected)
- Whether the driver is thread-safe per connection

### 6.3 Implementation blueprint: embedded JDBC driver (JNI-backed)

This section is intentionally written as an **implementation checklist** for a coding agent.

Assumption for this blueprint:

- The driver is **embedded**: Java loads a native library (JNI) that calls into the DecentDB native engine (likely via the existing C API or a dedicated stable ABI).

If you later do a client/server mode, you’d implement the same Java classes but the native calls become network calls.

#### 6.3.1 Project layout (suggested)

Keep the JDBC driver in its own repo or its own top-level folder so its Java build toolchain doesn’t leak into the Nim core.

Suggested structure:

```
decentdb-jdbc/
  driver/
    src/main/java/com/decentdb/jdbc/
      DecentDBDriver.java
      DecentDBConnection.java
      DecentDBStatement.java
      DecentDBPreparedStatement.java
      DecentDBResultSet.java
      DecentDBResultSetMetaData.java
      DecentDBDatabaseMetaData.java
      DecentDBTypes.java
      DecentDBSQLException.java
      DecentDBNative.java
    src/test/java/... (JDBC unit/integration tests)
    build.gradle or pom.xml

  native/
    CMakeLists.txt
    src/
      decentdb_jni.c (JNI glue)
      decentdb_jni.h
    build/...
```

#### 6.3.2 JDBC URL + properties

Pick a canonical URL format early; DBeaver needs a stable sample URL.

Common embedded patterns:

- `jdbc:decentdb:/absolute/path/to/db.ddb`
- `jdbc:decentdb:file:/absolute/path/to/db.ddb`
- `jdbc:decentdb:{file}` where `{file}` is replaced by DBeaver UI

Implementation guidance:

- Parse `Properties` for common keys: `user`, `password` (even if ignored), plus DecentDB-specific options.
- Accept URL query parameters for options to make DBeaver configuration easier:
  - `jdbc:decentdb:/path/to/db.ddb?readOnly=true&busyTimeoutMs=5000`

If an option is unknown, prefer:

- ignore with a warning (for forwards compatibility), or
- reject with a clear `SQLException` if it could lead to surprising behavior.

#### 6.3.3 JNI library loading strategy (must be explicit)

You need a deterministic way to load the native library when the driver first connects.

Common strategies:

1) **System library path**: user installs `libdecentdb_jni.so` somewhere and sets `java.library.path`.
   - Pro: simplest.
   - Con: not friendly for DBeaver users.

2) **Extract-from-JAR**: ship native libs inside the driver jar (per OS/arch) and extract to a temp dir, then `System.load(path)`.
   - Pro: best UX.
   - Con: more build/packaging complexity.

3) **DBeaver driver “native client”**: DBeaver can manage native clients for some DBs, but that’s usually for CLI tools; don’t assume it solves JNI.

Practical extraction approach:

- Put resources in paths like:
  - `/native/linux-x86_64/libdecentdb_jni.so`
  - `/native/macos-arm64/libdecentdb_jni.dylib`
  - `/native/windows-x86_64/decentdb_jni.dll`
- On first use:
  - detect OS/arch
  - copy the resource to `${java.io.tmpdir}/decentdb-jdbc/<version>/...`
  - mark executable if needed
  - call `System.load(extractedPath)`

Also:

- Ensure extraction is safe under concurrency (file locks / atomic rename).
- Include driver version in the extracted filename to avoid mismatched binaries.

#### 6.3.4 Native handle ownership (define this early)

For each native pointer/handle exposed to Java, define:

- who allocates
- who frees
- when it becomes invalid
- whether it may be used across threads

Example handles you’ll likely need:

- `db_handle` (connection)
- `stmt_handle` (prepared statement)
- `rs_handle` (result cursor)

Use `Cleaner` or `finalize()`-free patterns:

- Prefer `java.lang.ref.Cleaner` to free native resources if user forgets to close.
- Still implement `close()` everywhere and make it idempotent.

#### 6.3.5 Concurrency model mapping (Java ↔ DecentDB)

DecentDB’s model is “one writer, many readers” within a single process.

Driver guidance:

- Treat a single `Connection` as a unit of concurrency control.
- Make `Statement`/`ResultSet` **not thread-safe**; that matches most JDBC drivers.
- If the native layer cannot handle concurrent calls on one connection, serialize driver calls with a connection-level lock.

DBeaver may open:

- one connection for metadata browsing
- one connection per editor session

So correctness matters more than maximizing intra-connection parallelism.

#### 6.3.6 Minimal class-by-class checklist

Below is a “do these methods first” guide. The idea is: implement a narrow but correct subset and throw `SQLFeatureNotSupportedException` for the rest.

##### `java.sql.Driver` (`DecentDBDriver`)

- `acceptsURL(String url)`
- `connect(String url, Properties info)`
- `getMajorVersion()`, `getMinorVersion()`, `jdbcCompliant()`
- `getPropertyInfo(String url, Properties info)` (can be minimal)

Also ensure `DecentDBDriver` is discoverable via:

- `META-INF/services/java.sql.Driver` with the driver class name

##### `java.sql.Connection` (`DecentDBConnection`)

Must-have:

- `createStatement()`
- `prepareStatement(String sql)`
- `setAutoCommit(boolean)`, `getAutoCommit()`
- `commit()`, `rollback()`
- `close()`, `isClosed()`
- `getMetaData()` (return `DecentDBDatabaseMetaData`)
- `setReadOnly(boolean)`, `isReadOnly()` (even if you enforce at open)
- `setTransactionIsolation(int)`, `getTransactionIsolation()`
- `getCatalog()`, `setCatalog(String)` (can be stubbed)

Nice-to-have for DBeaver:

- `getSchema()`, `setSchema(String)`
- `isValid(int timeoutSeconds)` (DBeaver uses for connection health)

##### `java.sql.Statement` / `PreparedStatement`

Must-have:

- `execute(String sql)` / `executeQuery(String sql)` / `executeUpdate(String sql)`
- `executeQuery()` / `executeUpdate()` for `PreparedStatement`
- parameter binding for the common `setXxx` types:
  - `setNull`, `setBoolean`, `setInt`, `setLong`, `setDouble`
  - `setString`, `setBytes`
  - `setBigDecimal` (optional but helpful)
  - `setObject` (route to the above)
- `getResultSet()`, `getUpdateCount()`, `getMoreResults()`
- `setFetchSize(int)` (can be advisory)
- `close()` idempotent

If you don’t support generated keys initially, throw `SQLFeatureNotSupportedException` for:

- `getGeneratedKeys()` and the `RETURN_GENERATED_KEYS` variants

##### `java.sql.ResultSet`

Must-have:

- `next()`
- getters by index and by label for common types:
  - `getObject`, `getString`, `getLong`, `getInt`, `getBoolean`, `getDouble`, `getBytes`
- `wasNull()`
- `getMetaData()`
- `findColumn(String label)`
- `close()`

Implement `ResultSetMetaData` enough for DBeaver grids:

- `getColumnCount`, `getColumnLabel`, `getColumnName`
- `getColumnType`, `getColumnTypeName`
- `isNullable`, `getPrecision`, `getScale`

##### `java.sql.DatabaseMetaData`

DBeaver relies heavily on this.

Implement at least:

- `getDatabaseProductName()`, `getDatabaseProductVersion()`
- `getDriverName()`, `getDriverVersion()`
- `getIdentifierQuoteString()`
- `supportsTransactions()`, `supportsBatchUpdates()` (return accurate values)
- `getSchemas()`, `getCatalogs()` (can return empty + a single default)
- `getTables(catalog, schemaPattern, tableNamePattern, types)`
- `getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern)`
- `getPrimaryKeys(catalog, schema, table)`
- `getIndexInfo(catalog, schema, table, unique, approximate)`

You can initially throw for:

- procedures/functions metadata
- advanced referential metadata, unless you already expose it

#### 6.3.7 Metadata result set schemas (what to return)

The JDBC spec defines the columns each `DatabaseMetaData.*` method must return. DBeaver assumes those columns exist.

Two implementation approaches:

1) **Build the result sets in Java** (construct an in-memory rowset)
   - Pro: fast to implement.
   - Con: you still need correct schemas.

2) **Query DecentDB system catalog / information schema**
   - Pro: can be consistent with engine.
   - Con: requires those catalogs to exist.

For the first milestone, approach (1) is often simplest: return correct columns with best-effort values.

At minimum, ensure these columns exist:

- `getTables`: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `TABLE_TYPE`, `REMARKS`
- `getColumns`: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `TYPE_NAME`, `COLUMN_SIZE`, `DECIMAL_DIGITS`, `NULLABLE`, `ORDINAL_POSITION`
- `getPrimaryKeys`: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `KEY_SEQ`, `PK_NAME`
- `getIndexInfo`: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `NON_UNIQUE`, `INDEX_NAME`, `ORDINAL_POSITION`, `COLUMN_NAME`

If you omit required columns, DBeaver’s navigator and editors tend to misbehave in confusing ways.

#### 6.3.8 Type mapping (DecentDB → JDBC)

Implement one central mapping function:

- `DecentDBType` (native) → `java.sql.Types` + Java value class

Because DecentDB’s full type system may evolve, make the mapping table data-driven and default unknowns to:

- `Types.OTHER` and `getObject()` returning a string representation.

A pragmatic starter mapping (adjust to DecentDB reality):

| DecentDB type | JDBC `Types.*` | Java getter default |
|---|---:|---|
| BOOL | `Types.BOOLEAN` | `Boolean` |
| INT / INT32 | `Types.INTEGER` | `Integer` |
| BIGINT / INT64 | `Types.BIGINT` | `Long` |
| REAL / FLOAT32 | `Types.REAL` | `Float` |
| DOUBLE / FLOAT64 | `Types.DOUBLE` | `Double` |
| TEXT / VARCHAR | `Types.VARCHAR` | `String` |
| BLOB | `Types.BLOB` | `byte[]` |
| DATE | `Types.DATE` | `java.sql.Date` |
| TIMESTAMP | `Types.TIMESTAMP` | `java.sql.Timestamp` |
| UUID (if any) | `Types.OTHER` or `Types.BINARY` | `java.util.UUID` or `byte[]` |

Decide what `getObject()` returns for each type and keep it consistent.

#### 6.3.9 Transactions + isolation mapping

JDBC requires you to report supported isolation levels.

Implementation guidance that keeps you honest:

- Implement `getTransactionIsolation()` to return the closest level to DecentDB snapshot isolation.
- In `setTransactionIsolation(level)`, accept only levels you truly support and throw `SQLFeatureNotSupportedException` otherwise.
- Ensure `setAutoCommit(true/false)` has correct semantics:
  - If `autoCommit=true`, each statement is its own transaction.
  - If `autoCommit=false`, statements execute within a transaction until `commit/rollback`.

If DecentDB has explicit BEGIN/COMMIT/ROLLBACK behavior, map directly.

#### 6.3.10 Error mapping (native → `SQLException`)

DBeaver inspects `SQLException` fields.

Define a stable mapping:

- vendor error code: DecentDB internal code
- SQLState: best-effort standard class

At minimum:

- constraint violation → SQLState class `23***`
- syntax error → `42***`
- connection error → `08***`

Even if not perfect, consistent SQLState improves how DBeaver categorizes errors.

#### 6.3.11 “Enough for DBeaver” smoke test script

When the driver is wired into DBeaver, these should work:

- Connect
- Run `SELECT 1`
- Create a table, insert rows, select rows
- Expand the connection in the navigator and see tables/columns

Use this as the first end-to-end milestone.

### 6.4 Implementation blueprint: DBeaver extension that ships the driver

This is what a coding agent should implement once the JDBC driver exists.

#### 6.4.1 Plugin modules

Minimal:

- `com.decentdb.dbeaver.model` (register provider/driver/dialect)

Optional later:

- `com.decentdb.dbeaver.ui` (connection wizard, file handler)

#### 6.4.2 Model plugin contents

- `plugin.xml` with:
  - provider + driver declaration
  - SQL dialect declaration
- A tiny dialect class:
  - `DecentDBSQLDialect extends GenericSQLDialect`
  - Start with name/id + a small keyword list + correct identifier quoting

In most cases you do *not* need custom Java code for the provider if you can use `GenericDataSourceProvider`.

#### 6.4.3 Optional: custom `DataSource` class (only if needed)

Only add a custom `GenericDataSource` subclass if you must override behavior, e.g.:

- special type resolution (`resolveDataKind`)
- special internal connection properties
- more accurate error classification

Start without it. Every extra class here becomes ongoing maintenance.

#### 6.4.4 Optional: “file database handler” for `.ddb`

If DecentDB is file-based, a UI plugin can allow:

- “Open database file” → creates a connection using the DecentDB driver

DBeaver patterns exist for file handlers (e.g., SQLite/DuckDB). Implement this only after basic connectivity is stable.

---

## 7) Staged implementation plan

### Stage 0: Implementation prerequisites

- Choose the JDBC URL format and supported connection properties.
- Decide where metadata comes from (native APIs vs catalog queries vs in-memory rowsets).
- Decide how native libraries will be packaged and loaded.

Deliverable: a short “driver contract” note (URL, properties, supported isolation levels, type mapping rules).

### Stage 1: “It connects” milestone (driver-first)

- Build a minimal JDBC driver capable of:
  - `SELECT 1`
  - `CREATE TABLE`, `INSERT`, `SELECT`, `DELETE` for basic testing
  - simple result sets

At this stage, skip fancy metadata: DBeaver can still run SQL queries even if the navigator is limited.

Deliverable: DBeaver can open a connection and run queries.

### Stage 2: Basic metadata (tables/columns)

Implement enough `DatabaseMetaData` to populate DBeaver’s navigator:

- schemas/catalogs (even if “fake” / single schema)
- tables/views
- columns
- primary keys
- indexes

Deliverable: DBeaver shows tables and columns.

### Stage 3: Package as a DBeaver extension

- Add a DecentDB DBeaver plugin that registers:
  - provider + driver + bundled JDBC driver jar
  - DecentDB dialect

Deliverable: Users can select “DecentDB” in DBeaver without manual driver setup.

### Stage 4: UX polish (optional)

- Better dialect behavior
- Value handlers for special types (UUID/BLOB)
- Better error classification (unique constraint violations, FK violations)

---

## 8) Testing plan

### JDBC driver testing (must-have)

- Unit tests for:
  - URL parsing
  - parameter binding
  - result set iteration
  - type conversions

- Integration tests against a temp database:
  - run DDL + DML
  - verify commit/rollback semantics
  - verify concurrent reads while writing (within one process)

### DBeaver plugin testing

- Manual: install plugin into a DBeaver instance and validate:
  - connection wizard works
  - query editor works
  - navigator populates

- Optional automated UI tests are likely overkill initially.

---

## 9) Packaging and distribution notes

If the JDBC driver uses JNI/native libraries:

- We need a distribution plan per OS/arch (Linux/macOS/Windows; x86_64/arm64).
- DBeaver plugin bundling can include driver JARs; native libs may need special handling.

A practical approach is:

- first get it working on Linux (developer workflow)
- then expand to other platforms

---

## 10) Open questions (to resolve early)

1) **Does DecentDB already have a JDBC driver** (even experimental) or must we build it?
2) What should the **JDBC URL format** be?
   - `jdbc:decentdb:/path/to/db.ddb`?
   - `jdbc:decentdb:file:/path/to/db.ddb`?
   - Any query parameters for options (read-only, pragmas, etc.)?
3) How does DecentDB expose metadata today?
   - information_schema-like tables?
   - `SHOW` commands?
   - internal catalog tables?
4) How should JDBC isolation levels map to DecentDB snapshot isolation?

---

## 11) Suggested next concrete step

Before writing any DBeaver plugin code, confirm the DecentDB connectivity story:

- If we can define a JDBC URL + driver class today (even a stub), we can prototype DBeaver integration immediately.
- If not, the immediate work is: build a minimal embedded JDBC driver (or at least a proof-of-concept wrapper around the existing C API).
