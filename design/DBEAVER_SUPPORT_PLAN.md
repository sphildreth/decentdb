# DBeaver support plan for DecentDB

**Date:** 2026-02-24

This document is a practical, engineering-oriented plan for enabling **DecentDB** to be used from **DBeaver**.

It captures:

- What DBeaver expects for new “database integrations” (providers, drivers, dialects)
- Minimal viable integration options for DecentDB
- What we would likely need to build on the DecentDB side (the gating factor)
- A staged approach so we can get something working quickly and iterate

> Key takeaway: **DBeaver is primarily JDBC-centric.** The path forward is to provide a **JDBC driver** for DecentDB and ship a DBeaver extension that pre-registers the driver/provider and a DecentDB SQL dialect.

---

## 1) Goals / non-goals

### Goals

- Connect to a DecentDB database from DBeaver.
- Execute SQL queries and view results.
- Use DBeaver’s ER diagrams to visualize schema relationships.
- Browse basic metadata (tables, columns, indexes) using standard JDBC metadata APIs.
- Keep the first version minimal and robust.

### Non-goals (initially)

- Implementing every DBeaver feature (advanced DDL editors, custom explain plans, etc.).
- Multi-process server mode or a network protocol, unless we later decide it’s necessary and create an ADR.
- Pretending to be PostgreSQL “just for UI convenience” (e.g., misreporting `DatabaseMetaData.getDatabaseProductName()` as PostgreSQL, relying on PostgreSQL-specific system catalogs, or enabling PostgreSQL-only feature flags that DecentDB doesn’t actually support).

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
  - For ER diagrams specifically: `DatabaseMetaData.getImportedKeys`, `getExportedKeys`, and/or `getCrossReference`

DBeaver can tolerate imperfect metadata (many real-world drivers have quirks), but missing basics will hurt the UX.

### 3.2 Design constraint: DecentDB is currently a single-process DB

This repository’s North Star and constraints emphasize:

- Durable ACID writes
- One writer / many reader threads
- Single-process model

This influences how we should expose it to Java:

- **In-process JDBC** (Java loads native library via JNI) aligns with "single process" semantics - the database engine runs inside the DBeaver process.
- “Client-server over TCP” mode is a bigger step with more surface area (auth, protocol stability, concurrency semantics) and would almost certainly need an ADR.

---

## 4) Integration approach: JDBC driver (in-process)

DecentDB will provide a **JDBC driver** that works with DBeaver's standard JDBC support. This is the most straightforward path since DBeaver is Java-centric and JDBC is its primary database connectivity model.

### What "in-process" means

The JDBC driver is a standalone JAR file that users download and add to DBeaver. When DBeaver connects to a DecentDB database:

1. DBeaver loads the JDBC driver JAR
2. The driver loads a native library (JNI) that contains the DecentDB engine
3. The database runs **in the same process as DBeaver** - no separate server

This is the same model used by SQLite JDBC, DuckDB JDBC, and H2. The user does not need to run a separate DecentDB server or have the `decentdb_cli` installed.

**The JDBC driver is a separate artifact from the DecentDB CLI.** Users download the JAR from GitHub releases and use it directly in DBeaver.

### Why JDBC (not ODBC or bespoke provider)

- **JDBC is DBeaver's native path**: Most DBeaver database integrations use JDBC
- **ODBC requires bridges**: DBeaver's ODBC support varies by edition and often involves JDBC bridges anyway
- **Bespoke providers are rare**: Most DBeaver internals assume JDBC-like behavior; custom providers add maintenance burden without significant benefit

### Delivery

**JDBC driver + DBeaver extension (first-class experience)**

Ship a DBeaver plugin that pre-registers the driver and provides a DecentDB-specific SQL dialect. This makes DecentDB appear as a first-class database type in DBeaver's UI.

---

## 5) Existing C API metadata functions (prerequisite for JDBC)

The DecentDB C API already exposes metadata via JSON-returning functions that the JDBC driver can leverage:

| C API Function | Returns | JDBC Mapping |
|---|---|---|
| `decentdb_list_tables_json` | `["table1", "table2", ...]` | `DatabaseMetaData.getTables()` |
| `decentdb_get_table_columns_json` | `[{name, type, not_null, unique, primary_key, ref_table, ref_column, ...}]` | `DatabaseMetaData.getColumns()`, `getPrimaryKeys()`, `getImportedKeys()` |
| `decentdb_list_indexes_json` | `[{name, table, columns, unique, kind}]` | `DatabaseMetaData.getIndexInfo()` |

**Implementation approach for JDBC driver:**

1. Call these C API functions via JNI
2. Parse the JSON responses
3. Construct JDBC-compliant `ResultSet` objects with the required column schemas

This avoids the need for SQL-based metadata queries (e.g., `information_schema`) in the initial implementation. The JSON format is stable and already used by other language bindings.

*Note on performance:* Parsing large JSON strings in Java for every metadata query could become a bottleneck for databases with massive schemas. A future optimization could involve exposing a more direct, tabular C API for metadata if JSON parsing overhead becomes an issue.

---

## 6) What the minimal DBeaver plugin looks like

This section describes the plugin “shape” you would create in a separate repository (or in a dedicated integration repo).

### 6.1 Modules

DBeaver often splits database support into:

- a **model** plugin (core integration, JDBC provider, dialect, metadata tweaks)
- an optional **UI** plugin (connection wizard pages, file handlers)

For a minimal DecentDB integration, you can start with just the **model** plugin.

### 6.2 Provider + driver registration (`plugin.xml`)

You contribute to:

- `org.jkiss.dbeaver.dataSourceProvider`

and declare:

- provider id: `decentdb`
- driver id: `decentdb_jdbc`
- driver class: `com.yourorg.decentdb.jdbc.DecentDBDriver` (example)
- sample URL: something like `jdbc:decentdb:{path}` (example)

**Important:** the URL and driver class must match what your JDBC driver actually supports.

### 6.3 SQL dialect registration (`plugin.xml`)

You contribute to:

- `org.jkiss.dbeaver.sqlDialect`

and implement:

- `DecentDBSQLDialect extends PostgreDialect` (or similar base class)

Because DecentDB uses `libpg_query` and aims for PostgreSQL dialect compatibility, the dialect implementation can be extremely thin. It should inherit from DBeaver's existing PostgreSQL dialect to get all the benefits of PostgreSQL syntax highlighting, keyword autocomplete, and identifier quoting for free.

The primary reason to have a *custom* dialect class (even if it just inherits from PostgreSQL) rather than just telling DBeaver "this is PostgreSQL" is to **disable features DecentDB doesn't support**. 

For example, you would use this class to:
- Disable UI elements for PostgreSQL-specific features (like Tablespaces, Roles, or advanced partitioning).
- Prevent DBeaver from trying to query `pg_catalog` tables that DecentDB doesn't implement.
- Override specific feature flags (e.g., `supportsAlterTable()`) if DecentDB's subset differs from full PostgreSQL.

This keeps DBeaver’s view of capabilities honest, preventing the UI from making incorrect assumptions and throwing errors when users click things.

### 6.4 Minimal `plugin.xml` skeleton (copy/paste starting point)

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

### 6.5 What *not* to do in the first plugin

- Don't fork DBeaver or add large UI flows.
- Don't implement a custom DBeaver "native" provider unless you have a strong reason.
- Don't over-customize metadata/DDL generation in the plugin until the JDBC driver is solid.

---

## 7) What the minimal JDBC driver looks like (DecentDB side)

This repository already has multiple language bindings (`bindings/dotnet`, `bindings/go`, `bindings/node`, `bindings/python`) and a C API build output directory, which suggests a plausible JNI-backed JDBC driver design:

- Java: implement `java.sql.*` interfaces
- Native: call into DecentDB C API (or a dedicated stable native ABI for JDBC)

**Leveraging existing bindings experience:**

The existing bindings provide patterns that can inform the JDBC driver design:

| Binding | Relevant Lessons |
|---------|------------------|
| .NET (`bindings/dotnet`) | Handle lifecycle, type mapping, metadata API patterns |
| Python (`bindings/python`) | JSON metadata parsing from C API, error mapping |
| Go (`bindings/go`) | CGO patterns similar to JNI concerns |

The JDBC driver should follow similar patterns for:
- Native handle ownership and cleanup
- Type conversion between native and managed code
- Error propagation

### 7.1 Driver architecture sketch

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

### 7.2 Where JDBC gets tricky

- Type mapping: DecentDB types → JDBC types (`java.sql.Types`)
- NULL semantics
- Column metadata completeness (precision/scale, nullability)
- Transaction isolation mapping (DecentDB snapshot semantics vs JDBC isolation levels)
- Concurrency and statement handle lifetimes

For correctness, we should be explicit:

- Which JDBC isolation levels map to DecentDB behavior (and which are rejected)
- Whether the driver is thread-safe per connection

### 7.3 Implementation blueprint: JDBC driver (JNI-backed, in-process)

This section is intentionally written as an **implementation checklist** for a coding agent.

Assumption for this blueprint:

- The driver runs **in-process**: Java loads a native library (JNI) that calls into the DecentDB native engine (likely via the existing C API or a dedicated stable ABI). The database engine runs inside the DBeaver process.

If you later do a client/server mode, you’d implement the same Java classes but the native calls become network calls.

#### 7.3.1 Project layout (suggested)

Keep the JDBC driver in the `bindings/` directory to maintain consistency with other language bindings, ensuring its Java build toolchain doesn’t leak into the Nim core.

Suggested structure:

```
bindings/java/
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

#### 7.3.2 JDBC URL + properties

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

#### 7.3.3 JNI library loading strategy (must be explicit)

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

#### 7.3.4 Native handle ownership (define this early)

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

#### 7.3.5 Concurrency model mapping (Java ↔ DecentDB)

DecentDB’s model is “one writer, many readers” within a single process.

Driver guidance:

- Treat a single `Connection` as a unit of concurrency control.
- Make `Statement`/`ResultSet` **not thread-safe**; that matches most JDBC drivers.
- If the native layer cannot handle concurrent calls on one connection, serialize driver calls with a connection-level lock.

DBeaver may open:

- one connection for metadata browsing
- one connection per editor session

So correctness matters more than maximizing intra-connection parallelism.

#### 7.3.6 Minimal class-by-class checklist

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
- For ER diagrams (relationships):
  - `getImportedKeys(catalog, schema, table)`
  - `getExportedKeys(catalog, schema, table)`
  - `getCrossReference(primaryCatalog, primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable)`

You can initially throw for:

- procedures/functions metadata
- advanced referential metadata, unless you already expose it

#### 7.3.7 Metadata result set schemas (what to return)

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
- `getImportedKeys` / `getExportedKeys` / `getCrossReference`:
  - `PKTABLE_CAT`, `PKTABLE_SCHEM`, `PKTABLE_NAME`, `PKCOLUMN_NAME`
  - `FKTABLE_CAT`, `FKTABLE_SCHEM`, `FKTABLE_NAME`, `FKCOLUMN_NAME`
  - `KEY_SEQ`, `UPDATE_RULE`, `DELETE_RULE`, `FK_NAME`, `PK_NAME`, `DEFERRABILITY`

If you omit required columns, DBeaver’s navigator and editors tend to misbehave in confusing ways.

#### 7.3.8 Type mapping (DecentDB → JDBC)

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

#### 7.3.9 Transactions + isolation mapping

JDBC requires you to report supported isolation levels.

**Reference:** ADR-0023 defines DecentDB's isolation model as Snapshot Isolation.

Implementation guidance that keeps you honest:

- Implement `getTransactionIsolation()` to return `Connection.TRANSACTION_REPEATABLE_READ` (the closest JDBC equivalent to snapshot isolation).
- In `setTransactionIsolation(level)`:
  - Accept `TRANSACTION_REPEATABLE_READ` and `TRANSACTION_READ_COMMITTED` (both map to snapshot isolation, which provides stronger guarantees than read committed)
  - Throw `SQLFeatureNotSupportedException` for `TRANSACTION_SERIALIZABLE` (snapshot isolation does not prevent write skews)
  - Throw `SQLFeatureNotSupportedException` for `TRANSACTION_READ_UNCOMMITTED` (dirty reads are not allowed)
- Ensure `setAutoCommit(true/false)` has correct semantics:
  - If `autoCommit=true`, each statement is its own transaction.
  - If `autoCommit=false`, statements execute within a transaction until `commit/rollback`.

If DecentDB has explicit BEGIN/COMMIT/ROLLBACK behavior, map directly.

#### 7.3.10 Error mapping (native → `SQLException`)

DBeaver inspects `SQLException` fields.

Define a stable mapping:

- vendor error code: DecentDB internal code
- SQLState: best-effort standard class

At minimum:

- constraint violation → SQLState class `23***`
- syntax error → `42***`
- connection error → `08***`

Even if not perfect, consistent SQLState improves how DBeaver categorizes errors.

#### 7.3.11 "Enough for DBeaver" smoke test script

When the driver is wired into DBeaver, these should work:

- Connect
- Run `SELECT 1`
- Create a table, insert rows, select rows
- Expand the connection in the navigator and see tables/columns

Use this as the first end-to-end milestone.

### 7.4 Implementation blueprint: DBeaver extension that ships the driver

This is what a coding agent should implement once the JDBC driver exists.

#### 7.4.1 Plugin modules

Minimal:

- `com.decentdb.dbeaver.model` (register provider/driver/dialect)

Optional later:

- `com.decentdb.dbeaver.ui` (connection wizard, file handler)

#### 7.4.2 Model plugin contents

- `plugin.xml` with:
  - provider + driver declaration
  - SQL dialect declaration
- A tiny dialect class:
  - `DecentDBSQLDialect extends GenericSQLDialect`
  - Start with name/id + a small keyword list + correct identifier quoting

In most cases you do *not* need custom Java code for the provider if you can use `GenericDataSourceProvider`.

#### 7.4.3 Optional: custom `DataSource` class (only if needed)

Only add a custom `GenericDataSource` subclass if you must override behavior, e.g.:

- special type resolution (`resolveDataKind`)
- special internal connection properties
- more accurate error classification

Start without it. Every extra class here becomes ongoing maintenance.

#### 7.4.4 Optional: "file database handler" for `.ddb`

If DecentDB is file-based, a UI plugin can allow:

- “Open database file” → creates a connection using the DecentDB driver

DBeaver patterns exist for file handlers (e.g., SQLite/DuckDB). Implement this only after basic connectivity is stable.

---

## 8) Staged implementation plan

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

### Stage 2: ER-diagram-ready metadata (tables/columns/keys)

Implement enough `DatabaseMetaData` to populate DBeaver’s navigator and ER diagrams:

- schemas/catalogs (even if “fake” / single schema)
- tables/views
- columns
- primary keys
- indexes
- foreign keys (relationships)

Deliverable: DBeaver shows tables/columns and ER diagrams show relationships.

### Stage 3: Package as a DBeaver extension

- Add a DecentDB DBeaver plugin that registers:
  - provider + driver + bundled JDBC driver jar
  - DecentDB dialect

Deliverable: Users can select “DecentDB” in DBeaver without manual driver setup.

### Stage 4: Documentation and packaging

- Create user documentation at `docs/user-guide/dbeaver.md`:
  - Installation instructions (with screenshots)
  - Connection URL format and options
  - Supported SQL features in DBeaver context
  - Known limitations and troubleshooting
- Package driver JAR with embedded native libraries
- Publish to GitHub releases

Deliverable: Users can download and install the driver with clear documentation.

### Stage 5: UX polish (optional)

- Better dialect behavior
- Value handlers for special types (UUID/BLOB)
- Better error classification (unique constraint violations, FK violations)

---

## 9) Testing plan

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
  - verify `DatabaseMetaData` foreign key metadata matches created constraints

### DBeaver plugin testing

- Manual: install plugin into a DBeaver instance and validate:
  - connection wizard works
  - query editor works
  - navigator populates
  - ER diagram renders tables + relationships correctly

- Optional automated UI tests are likely overkill initially.

---

## 10) Packaging, distribution, and user installation

### 10.1 Driver packaging

The JDBC driver will be distributed as a single JAR file containing:

- Java classes implementing `java.sql.*` interfaces
- Native libraries for supported platforms (embedded in JAR, extracted at runtime)

**Supported platforms (initial):**

| Platform | Architecture | Native library |
|----------|--------------|----------------|
| Linux | x86_64 | `libdecentdb_jni.so` |
| macOS | Universal (x86_64 + arm64) | `libdecentdb_jni.dylib` |
| Windows | x86_64 | `decentdb_jni.dll` |

*Note: On macOS, it is standard practice to compile a single "Universal Binary" (fat binary) that contains both architectures to simplify distribution and JNI extraction logic.*

**JAR structure:**

```
decentdb-jdbc-{version}.jar
├── com/decentdb/jdbc/*.class
├── META-INF/services/java.sql.Driver
└── native/
    ├── linux-x86_64/libdecentdb_jni.so
    ├── macos-universal/libdecentdb_jni.dylib
    └── windows-x86_64/decentdb_jni.dll
```

### 10.2 Distribution channels

1. **GitHub Releases**: Primary distribution point
   - Users download the JAR directly from the releases page
   - Versioned releases with release notes

2. **Maven Central** (future): For build tool integration
   - Allows users to add as dependency in Maven/Gradle projects
   - Not required for DBeaver usage

### 10.3 User installation in DBeaver

Once a DBeaver extension is available:

1. Download the extension ZIP from GitHub releases
2. In DBeaver: **Help → Install New Software**
3. Add the extension archive as a local update site
4. Restart DBeaver
5. DecentDB appears as a first-class database type in the connection wizard

### 10.4 Documentation requirements

A user-facing documentation page must be created at `docs/user-guide/dbeaver.md` covering:

- Supported DBeaver versions
- Supported platforms and Java versions
- Step-by-step installation instructions (with screenshots)
- Connection URL format and options
- Supported SQL features in DBeaver context
- Known limitations
- Troubleshooting common issues

---

## 11) Risks and mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| JNI native library compatibility across JVM versions | Driver crashes or fails to load | Test against LTS Java versions (8, 11, 17, 21); use stable JNI practices; document supported JVM versions |
| DBeaver internal API changes | Plugin breaks on DBeaver updates | Target stable extension points; avoid internal APIs; test against multiple DBeaver versions |
| Testing matrix complexity (OS × arch × DBeaver × JVM) | QA burden grows quickly | Prioritize Linux x86_64 + DBeaver latest + Java 17 as primary combo; expand incrementally |
| Native library extraction conflicts | Multiple driver versions conflict in temp directory | Include version in extracted filename; use atomic rename for extraction |
| Connection pooling assumptions | DBeaver may pool connections; driver may not be pool-safe | Document whether driver is pool-safe; implement `isValid()` for connection health checks. Explicitly state how multiple `Connection` objects in the same JVM share the underlying DecentDB environment handle and interact with the "one writer" lock. |
| BLOB/large value handling | DBeaver data viewer may fail on large values | Implement streaming for BLOBs; set reasonable fetch size limits |

---

## 12) Resolved Architectural Decisions

1) **Does DecentDB already have a JDBC driver** (even experimental) or must we build it?
   
   > **Answer:** No JDBC driver exists yet. Must be built from scratch using the existing C API as the foundation.

2) What should the **JDBC URL format** be?
   - `jdbc:decentdb:/path/to/db.ddb`?
   - `jdbc:decentdb:file:/path/to/db.ddb`?
   - Any query parameters for options (read-only, pragmas, etc.)?
   
   > **Recommendation:** Use `jdbc:decentdb:/path/to/db.ddb` as the canonical format. Support query parameters for options like `readOnly=true`. This matches common embedded database patterns (SQLite, DuckDB).

3) How does DecentDB expose metadata today?
   - information_schema-like tables?
   - `SHOW` commands?
   - internal catalog tables?
   
   > **Answer:** DecentDB exposes metadata via C API functions that return JSON (see Section 5):
   > - `decentdb_list_tables_json`
   > - `decentdb_get_table_columns_json`
   > - `decentdb_list_indexes_json`
   > 
   > The JDBC driver should call these via JNI and parse the JSON responses.

4) How should JDBC isolation levels map to DecentDB snapshot isolation?
   
   > **Answer:** Per ADR-0023, DecentDB implements Snapshot Isolation. The JDBC driver should:
   > - Report `Connection.TRANSACTION_REPEATABLE_READ` as the default isolation level (closest JDBC equivalent to snapshot isolation)
   > - Accept `TRANSACTION_READ_COMMITTED` and map it to the same underlying behavior (snapshot isolation provides stronger guarantees)
   > - Reject `TRANSACTION_SERIALIZABLE` with `SQLFeatureNotSupportedException` (snapshot isolation does not prevent write skews)
   > - Reject `TRANSACTION_READ_UNCOMMITTED` with `SQLFeatureNotSupportedException` (dirty reads not allowed)

---

## 13) Suggested next concrete step

Before writing any DBeaver plugin code, confirm the DecentDB connectivity story:

- If we can define a JDBC URL + driver class today (even a stub), we can prototype DBeaver integration immediately.
- If not, the immediate work is: build a minimal JDBC driver (or at least a proof-of-concept wrapper around the existing C API).
