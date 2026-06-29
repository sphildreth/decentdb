# DecentDB vs H2: When to Choose Which

This document helps developers decide between **DecentDB** and **H2** for
embedded SQL workloads. Both can run inside an application process, but they
serve different ecosystems and different production assumptions.

> **Versions compared:** DecentDB 2.15.0 workspace behavior vs public H2
> documentation as of 2026-06-28.
>
> **Scope note:** This page focuses on H2 as an embedded Java/JDBC database.
> H2 can also run in server mode and in-memory mode; those are called out where
> they materially change the decision.
>
> **See also:** [DecentDB vs SQLite](decentdb-vs-sqlite.md),
> [DecentDB vs DuckDB](decentdb-vs-duckdb.md), [SQL Feature
> Matrix](sql-feature-matrix.md), and [SQL Reference](sql-reference.md).

## They Overlap Mostly in JVM Applications

H2 is a mature Java relational database with JDBC API support, embedded and
server modes, disk and in-memory databases, transaction support, MVCC, a small
pure-Java footprint, browser console tooling, encryption, and full-text search.

DecentDB is a Rust-native embedded relational database with a C ABI and
multi-language bindings. It is not a Java-only database and does not try to be
an in-memory test double for other database engines.

The short version:

- Choose **H2** when the project is JVM-centered, the JDBC ecosystem is the
  deciding factor, or the database is primarily an in-memory/test database.
- Choose **DecentDB** when the database is durable application state, needs to
  work consistently across multiple languages, or needs DecentDB-native branch,
  sync, policy, and crash-testing surfaces.

## At a Glance

| Dimension | DecentDB | H2 |
|-----------|----------|----|
| **Core identity** | Rust-native embedded relational database | Java SQL database engine |
| **Primary ecosystem** | Rust plus C ABI bindings for many languages | JVM/JDBC |
| **Modes** | Embedded local database; Web/WASM support through DecentDB surfaces | Embedded, server, in-memory, disk-based |
| **SQL direction** | Practical Postgres-like application SQL subset | H2 SQL with compatibility modes for selected dialect behavior |
| **Default durability posture** | WAL + fsync-on-commit by default for native files | Depends on H2 mode, URL options, and storage configuration |
| **Concurrency model** | One writer, many readers; local native cross-process WAL coordination when supported | MVCC and Java engine concurrency; exact behavior depends on mode |
| **Best test use** | Test DecentDB behavior directly | Fast JVM tests, Spring/JDBC fixtures, in-memory database |
| **Best production use** | Durable embedded app data with DecentDB features | JVM-local SQL where H2's engine and tooling are acceptable |
| **Tooling** | DecentDB CLI, SQL/CLI sync and branch tooling, bindings | JDBC, H2 Console, Java tooling |
| **Search** | Full-text and trigram substring indexes | Full-text search support |
| **Security features** | TDE, row policies, projection masks, audit context | Encrypted databases and user/role features |
| **License** | MIT or Apache-2.0 | EPL/MPL dual license |

## When DecentDB Is the Better Fit

### 1. The database is production application state, not just a test fixture

H2 is widely used as a convenient embedded or in-memory database in Java tests.
That is a legitimate use. It is less compelling when the real requirement is a
durable local database used by production applications across runtimes.

DecentDB's default native-file posture is durability-first:

```sql
BEGIN;
INSERT INTO events (kind, payload) VALUES ($1, $2);
COMMIT; -- WAL-backed and fsynced before returning by default
```

If your product ships a local database file that must survive power loss and be
inspected by non-JVM tooling, DecentDB is the more direct fit.

### 2. You need the same database from several languages

DecentDB exposes one engine through a stable C ABI and maintained bindings:

- Rust;
- C/C++;
- Python;
- .NET;
- Go;
- Java;
- Node.js;
- Dart.

H2 is strongest in JVM applications. It can be reached through JDBC and server
mode, but its primary advantage is Java-native deployment. If your product is a
polyglot CLI, desktop app, server component, and binding surface, DecentDB's
language strategy is more aligned.

### 3. You need local branch/diff/restore workflows

DecentDB includes database-native local validation workflows:

```bash
decentdb snapshot create --db app.ddb --name before-migration
decentdb branch create --db app.ddb --name migration-test --from before-migration
decentdb exec --db app.ddb --branch migration-test --sql "ALTER TABLE users ADD COLUMN tier TEXT"
decentdb branch diff --db app.ddb --left main --right migration-test
```

H2 users can copy files, export SQL, use migration frameworks, or rely on JVM
test workflows. DecentDB is better when branch/diff/restore is part of the
database product itself.

### 4. You want local-first sync state inside the database

DecentDB exposes sync state as queryable local data:

```sql
SELECT * FROM sys_sync_status;
SELECT * FROM sys_sync_journal ORDER BY sequence DESC LIMIT 20;
SELECT * FROM sys_sync_conflicts;
SELECT * FROM sys_sync_doctor;
```

H2 can be embedded in applications that implement sync, but H2 itself is not
primarily a local-first sync engine. If sync journals, scoped exchange,
conflicts, retention, and diagnostics should be first-class database surfaces,
DecentDB is the better fit.

### 5. You need row policies, projection masks, and audit context locally

DecentDB includes engine-enforced application data controls:

```sql
SET AUDIT CONTEXT actor = 'support-user-17';

CREATE POLICY tenant_filter ON tickets
USING tenant_id = current_tenant();

CREATE MASK phone_mask ON customers(phone)
USING '***' || right(phone, 4);
```

H2 supports database users and SQL features, but DecentDB's policies, masks,
and audit context are designed for embedded application data governance.

### 6. You do not want a JVM dependency

H2's pure-Java distribution is a strength for Java applications. It is a cost
for native CLIs, Rust services, Go applications, Python packages, or small
native binaries that do not otherwise need a JVM.

DecentDB is a native library and CLI. Choose it when "no Java runtime" is an
operational requirement.

### 7. You need deterministic database crash/fault testing hooks

DecentDB's test strategy includes FaultyVFS and WAL failpoint hooks. That makes
crash-safety testing part of the database engineering surface.

H2 has a mature codebase and useful Java testing ergonomics, but DecentDB is
more explicit about validating local durability behavior under injected storage
failures.

## When H2 Is the Better Fit

### 1. You are building a JVM application and want JDBC-first embedding

H2 is a natural fit when the application is Java/Kotlin/Scala and the database
is accessed through JDBC:

```java
Connection conn = DriverManager.getConnection("jdbc:h2:./appdb");
try (PreparedStatement ps =
    conn.prepareStatement("select * from users where id = ?")) {
  ps.setLong(1, 42);
  ps.executeQuery();
}
```

DecentDB has a Java binding/JDBC surface, but H2's center of gravity is JDBC.
If JDBC compatibility and JVM-local deployment are the main requirements, H2 is
the lower-friction choice.

### 2. You need fast in-memory test databases

H2's in-memory mode is one of its best-known uses:

```text
jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1
```

This is useful for unit tests, quick fixtures, demos, and local development
tools. DecentDB can be used in tests too, but H2 is purpose-built for the JVM
test fixture niche.

The caveat is compatibility. H2 compatibility modes are useful, but they are
not exact emulators of PostgreSQL, MySQL, Oracle, or other engines. Use H2 as a
test database when H2 behavior is acceptable, not as proof that another
production database behaves identically.

### 3. You want one small pure-Java dependency

H2's small pure-Java jar is attractive when deployment simplicity means "add a
jar, no native library." That matters in environments where native shared
libraries are difficult to package or audit.

DecentDB is a native Rust library. That gives it strong performance and
cross-language embedding properties, but it is not as simple as dropping one
Java jar into a classpath.

### 4. You need H2 Console and Java-oriented admin tooling

H2 includes a browser-based console and Java command-line tools. If your team
already uses H2 Console as part of development, demos, or support workflows,
that is a practical reason to keep H2.

DecentDB has a CLI and docs-focused workflow, but it is not trying to replicate
H2 Console.

### 5. You use H2 server mode

H2 can run as an embedded engine or as a server. That flexibility can be useful
for local development, debugging, or small Java deployments that want a network
connection during development.

DecentDB is primarily an embedded local database. If you want a Java database
that can switch between embedded and server modes, H2 is the better fit.

### 6. You depend on H2-specific SQL or compatibility modes

H2 supports its own SQL grammar plus compatibility modes for selected dialects.
If your application or tests already depend on H2 behavior, moving to DecentDB
is a real migration, not a configuration change.

## Side-by-Side Examples

### JVM test fixture

```java
// H2: fast in-memory database for JVM tests.
Connection conn = DriverManager.getConnection("jdbc:h2:mem:testdb");
conn.createStatement().execute("""
  create table users (id bigint primary key, name varchar not null)
""");
```

Prefer **H2** when this is the core use case.

### Production local file with branch validation

```bash
# DecentDB: validate a migration on a branch before touching main.
decentdb snapshot create --db app.ddb --name before-upgrade
decentdb branch create --db app.ddb --name upgrade-test --from before-upgrade
decentdb branch diff --db app.ddb --left main --right upgrade-test
```

Prefer **DecentDB** when local operational workflows matter as database
features.

### Multi-language embedding

```python
# DecentDB: same database engine from Python.
import decentdb

conn = decentdb.connect("app.ddb")
conn.execute("insert into jobs (kind) values ($1)", ("email",))
```

Prefer **DecentDB** when the same engine must be embedded outside Java.

## Summary Decision Matrix

| Your situation | Recommendation |
|----------------|----------------|
| JVM app, JDBC-first database access | **H2** |
| In-memory database for Java tests | **H2** |
| Small pure-Java dependency is the priority | **H2** |
| H2 Console/server mode is useful | **H2** |
| Application already depends on H2 SQL behavior | **H2** |
| Durable local application data across languages | **DecentDB** |
| Need native library/CLI without a JVM | **DecentDB** |
| Need local branch/diff/restore/time-travel workflows | **DecentDB** |
| Need sync journal/conflict/retention inspection | **DecentDB** |
| Need row policies, projection masks, audit context | **DecentDB** |
| Need deterministic local crash/fault validation hooks | **DecentDB** |

## Bottom Line

Pick **H2** for Java-first embedded SQL, in-memory tests, JDBC tooling, and
small pure-Java deployment.

Pick **DecentDB** for durable embedded application data that should behave the
same across languages and expose branch, sync, policy, and crash-safety
workflows as database features.

The fair comparison is not "which database is better." It is whether the
application is really asking for a Java/JDBC database or for a durable
multi-language embedded application database.

## External References

- [H2 Database Engine home page](https://h2database.com/)
- [H2 features](https://www.h2database.com/html/features.html)
- [H2 tutorial](https://www.h2database.com/html/tutorial.html)
- [H2 advanced documentation](https://www.h2database.com/html/advanced.html)
- [H2 repository](https://github.com/h2database/h2database)
