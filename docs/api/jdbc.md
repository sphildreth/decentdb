# JDBC driver (Java)

DecentDB ships an in-tree Java stack under `bindings/java/`:

- `bindings/java/driver/` — JDBC driver (`com.decentdb.jdbc.DecentDBDriver`)
- `bindings/java/native/` — JNI bridge (`libdecentdb_jni.*`)
- `bindings/java/dbeaver-extension/` — DBeaver packaging
- `tests/bindings/java/Smoke.java` — low-level FFM smoke for the raw C ABI

The JDBC driver targets the stable DecentDB C ABI through JNI. Public behavior
should match the engine and C ABI, rather than inventing Java-only semantics.

## Supported JDBC URLs

The driver accepts URLs of the form:

```text
jdbc:decentdb:/absolute/path/to/db.ddb
jdbc:decentdb:/absolute/path/to/db.ddb?mode=open
jdbc:decentdb:/absolute/path/to/db.ddb?readOnly=true
```

Supported connection properties:

- `mode` — `openOrCreate` (default), `open`, or `create`
- `readOnly` — `true` or `false` (default `false`)

Currently unsupported at open time:

- `busyTimeoutMs`
- `cachePages`

The stable C ABI currently exposes only default `create`, `open`, and
`open_or_create` entry points. The Java driver therefore rejects
`busyTimeoutMs` and `cachePages` instead of silently pretending they work.

## Build locally

From the repository root:

```bash
cargo build -p decentdb --release
cd bindings/java
./gradlew :driver:jar
```

This builds the core shared library, the JNI bridge, and the JDBC jar. The jar
embeds the matching native libraries for the current OS/arch from
`target/release/`.

## Validate the Java binding

The Java build itself targets Java 17 bytecode. In this repository, the Gradle
test path is currently validated with JDK 21:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
cd bindings/java
./gradlew :driver:test
```

## Run the standalone JDBC example

The driver now includes a runnable CRUD example that covers schema creation,
prepared statements, `DECIMAL`, `BOOL`, `TIMESTAMP`, rollback, metadata, and
basic error handling:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
cd bindings/java
./gradlew :driver:runCrudExample
```

To target a specific database path:

```bash
./gradlew :driver:runCrudExample -PexampleArgs="/absolute/path/to/example.ddb"
```

## Run the benchmark

From `bindings/java/`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :driver:benchmarkFetch -PbenchmarkArgs="--count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix java_bench_fetch"
```

Supported benchmark options:

- `--engine <all|decentdb|sqlite>`
- `--count <n>`
- `--point-reads <n>`
- `--fetchmany-batch <n>`
- `--point-seed <n>`
- `--db-prefix <prefix>`
- `--sqlite-jdbc <jar_path>`
- `--keep-db`

When `--engine all` is used, the benchmark will try to auto-discover a local
`sqlite-jdbc` jar from common DBeaver, Rider, and Maven cache locations. You
can also pass it explicitly with `--sqlite-jdbc`.

## Run the low-level FFM smoke

`tests/bindings/java/Smoke.java` validates the raw C ABI independently of the
JDBC driver. With JDK 21, this path uses the preview Foreign Function & Memory
API:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
cargo build -p decentdb
javac --enable-preview --release 21 tests/bindings/java/Smoke.java
java --enable-preview --enable-native-access=ALL-UNNAMED -cp tests/bindings/java Smoke
```

The smoke test auto-detects `target/debug/` or `target/release/` for the core
shared library.

## JDBC usage example

```java
import com.decentdb.jdbc.DecentDBConnection;
import com.decentdb.jdbc.DecentDBDataSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;

DecentDBDataSource dataSource = new DecentDBDataSource("jdbc:decentdb:/tmp/shop.ddb");
dataSource.setMode("openOrCreate");

try (Connection connection = dataSource.getConnection()) {
    connection.setAutoCommit(false);

    try (PreparedStatement create = connection.prepareStatement(
        "CREATE TABLE IF NOT EXISTS products (" +
            "id INT64 PRIMARY KEY, " +
            "name TEXT NOT NULL, " +
            "price DECIMAL(12,2) NOT NULL, " +
            "active BOOL NOT NULL, " +
            "updated_at TIMESTAMP)"
    )) {
        create.executeUpdate();
    }

    try (PreparedStatement insert = connection.prepareStatement(
        "INSERT INTO products (id, name, price, active, updated_at) VALUES ($1, $2, $3, $4, $5)"
    )) {
        insert.setLong(1, 1L);
        insert.setString(2, "Keyboard");
        insert.setBigDecimal(3, new BigDecimal("129.99"));
        insert.setBoolean(4, true);
        insert.setTimestamp(5, Timestamp.from(Instant.now()));
        insert.executeUpdate();
    }

    connection.commit();

    try (PreparedStatement query = connection.prepareStatement(
        "SELECT id, name, price FROM products WHERE id = $1"
    )) {
        query.setLong(1, 1L);
        try (ResultSet rs = query.executeQuery()) {
            while (rs.next()) {
                System.out.println(rs.getString("name") + " => " + rs.getBigDecimal("price"));
            }
        }
    }

    if (connection instanceof DecentDBConnection decent) {
        System.out.println("Engine version: " + decent.getEngineVersion());
        decent.checkpoint();
    }
}
```

DecentDB's Java driver currently follows the engine's native positional
placeholder style (`$1`, `$2`, ...) in prepared SQL.

## DecentDB-specific connection helpers

`DecentDBConnection` adds a small number of engine-truth helpers beyond the
standard JDBC `Connection` surface:

- `isInTransaction()` — queries the engine via `ddb_db_in_transaction`
- `getAbiVersion()` — exposes `ddb_abi_version`
- `getEngineVersion()` — exposes `ddb_version`
- `checkpoint()` — runs `ddb_db_checkpoint`
- `saveAs(path)` — runs `ddb_db_save_as`

`saveAs(path)` requires a destination path that does not already exist.

## Thread-safety and pooling

DecentDB uses a single-process, one-writer / many-readers model.

- `Connection` methods are serialized internally per connection
- `Statement`, `PreparedStatement`, and `ResultSet` are not thread-safe
- `DecentDBDataSource` is a thin configuration wrapper, not a pooling
  implementation

If you place DecentDB behind an external pool, keep write-heavy workloads at a
maximum size of `1`.

## Metadata and framework support

The driver includes:

- a `DataSource` implementation: `com.decentdb.jdbc.DecentDBDataSource`
- JDBC metadata for tables, columns, indexes, keys, and type mapping
- DecentDB-specific schema helpers on `DecentDBDatabaseMetaData` for table DDL
  and trigger listing

For DBeaver integration details, see the [DBeaver guide](../user-guide/dbeaver.md).
