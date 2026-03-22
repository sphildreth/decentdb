# DBeaver Support

DecentDB ships a JNI-backed JDBC driver that lets you connect to `.ddb` files directly from
[DBeaver](https://dbeaver.io/) — the popular open-source database tool.  No network server is
required; the driver opens the file in-process.

For JDBC driver details (URL format, properties, isolation mapping), see: [JDBC Driver (Java)](../api/jdbc.md).

---

## Supported versions

| Component | Supported |
|---|---|
| DBeaver | 23.x+ (tested with Community Edition 25.3.x) |
| Java | 17 LTS, 21 LTS (must be Java 17+ at runtime) |
| Platforms | Linux x86\_64/arm64 (including 64-bit Raspberry Pi OS), macOS x86\_64/arm64, Windows x86\_64 |

GitHub Releases include native Linux `arm64` artifacts for 64-bit Raspberry Pi OS on Raspberry Pi
3/4/5: `decentdb-jdbc-<tag>-Linux-arm64.jar` and `decentdb-dbeaver-<tag>-Linux-arm64.zip`.

---

## Installation

### Option A — add the driver jar manually (recommended for now)

1. **Build the driver jar and native library** (or download a release):
   ```bash
   # From the repo root
   cargo build_lib                          # builds build/libc_api.so
   cd bindings/java/native && make           # builds build/libdecentdb_jni.so
   cd ../.. && JAVA_HOME=<jdk17> ./gradlew :driver:jar   # builds driver/build/libs/decentdb-jdbc-*.jar
   ```

2. **Open DBeaver → Database → Driver Manager → New**.

3. Fill in the driver form:

   | Field | Value |
   |---|---|
   | Driver Name | DecentDB |
   | Class Name | `com.decentdb.jdbc.DecentDBDriver` |
   | URL Template | `jdbc:decentdb:{file}` |
   | Default Port | *(leave empty)* |
   | Category | SQL |

4. In the **Libraries** tab, click **Add File** and select the `decentdb-jdbc-*.jar`.

5. Add the JVM flag so the JNI library can be found:
   - Go to **DBeaver → Window → Preferences → Connections → Driver properties**.
   - Or append to `dbeaver.ini`:
     ```
     -Djava.library.path=/absolute/path/to/build
     ```
   Alternatively, place `libdecentdb_jni.so` (and `libc_api.so`) on the system library path
   (`LD_LIBRARY_PATH` on Linux, `DYLD_LIBRARY_PATH` on macOS).

6. Click **OK** to save the driver.

### Option B — DBeaver plugin (first-class database type)

The DecentDB DBeaver extension code lives in `bindings/java/dbeaver-extension/` and registers DecentDB as a first-class database type inside DBeaver.

If you want to use the plugin today, build it from source:

```bash
# From the repo root
cd bindings/java
JAVA_HOME=<jdk17> ./gradlew :dbeaver-extension:jar
```

#### Install location

You must install into the **application** plugins directory (not your workspace).

- Official DBeaver packages (tarball/Windows/macOS): typically `<DBeaver install>/plugins/`
- Arch/Manjaro `extra/dbeaver`: `/usr/lib/dbeaver/plugins/`

#### Install (official DBeaver packages)

Copy the built jar from `bindings/java/dbeaver-extension/build/libs/` into DBeaver’s `plugins/` directory, then restart DBeaver.

#### Install (Arch/Manjaro `extra/dbeaver`)

Arch’s DBeaver packaging uses Equinox “simpleconfigurator”, which means **dropping a jar into `plugins/` is not sufficient**.
You also need to add the bundle to `bundles.info` and start DBeaver once with `-clean`.

1. Copy the jar into the app plugin directory:

    # From the DecentDB repo root
    VERSION=1.8.1

    sudo install -Dm644 \
      bindings/java/dbeaver-extension/build/libs/dbeaver-extension-${VERSION}.jar \
      /usr/lib/dbeaver/plugins/org.jkiss.dbeaver.ext.decentdb_${VERSION}.jar

2. Add it to the bundle list:

    BUNDLES_INFO=/usr/lib/dbeaver/configuration/org.eclipse.equinox.simpleconfigurator/bundles.info

    # Use an existing bundle to copy its (startLevel, autoStart) fields.
    # This prints e.g. "4,false" on many installs.
    START_FIELDS=$(grep -m1 '^org\.jkiss\.dbeaver\.ext\.generic,' "$BUNDLES_INFO" | cut -d, -f4-5)

    # Append DecentDB bundle entry if it isn't already present.
    if ! grep -qi '^org\.jkiss\.dbeaver\.ext\.decentdb,' "$BUNDLES_INFO"; then
      echo "org.jkiss.dbeaver.ext.decentdb,${VERSION},plugins/org.jkiss.dbeaver.ext.decentdb_${VERSION}.jar,${START_FIELDS}" | sudo tee -a "$BUNDLES_INFO" >/dev/null
    fi

3. Start DBeaver once with `-clean` (forces it to rebuild its bundle cache):

    dbeaver -clean -consoleLog

After that, DecentDB should show up under **New Connection → Embedded**.

---

## Creating a connection

1. **Database → New Database Connection → select DecentDB**.
2. Set the **JDBC URL**:
   ```
   jdbc:decentdb:/absolute/path/to/mydb.ddb
   ```
3. Optional connection properties (add in the **Driver Properties** or **Connection Properties** tab):

   | Property | Default | Description |
   |---|---|---|
   | `readOnly` | `false` | Open the database in read-only mode |
   | `busyTimeoutMs` | `0` | Milliseconds to wait when the database is locked by another writer |
   | `cachePages` | `0` | Page cache size (0 = engine default) |

4. Leave the **Username** and **Password** fields empty.
5. Click **Test Connection**, then **Finish**.

### URL examples

```
jdbc:decentdb:/home/alice/data/shop.ddb
jdbc:decentdb:/home/alice/data/shop.ddb?readOnly=true
jdbc:decentdb:/home/alice/data/shop.ddb?busyTimeoutMs=10000
jdbc:decentdb:/home/alice/data/shop.ddb?cachePages=2048
jdbc:decentdb::memory:
```

---

## Basic usage

Once connected, you can:

- **Run queries** — open a SQL console (right-click connection → *SQL Editor*) and execute any
  supported SQL statement.
- **Browse tables** — expand the connection in the Database Navigator to see tables, columns,
  indexes, and foreign keys.
- **View ER diagrams** — right-click a schema or table → *View Diagram* to render the entity
  relationship diagram including FK relationships.
- **Export data** — right-click a table → *Export Data* to CSV, JSON, etc.

---

## Manual smoke-test checklist

After installation, verify the following:

- [ ] Connect to a `.ddb` file — DBeaver shows the connection as *connected*.
- [ ] Open a SQL console and run `SELECT 1` — returns a result row with value `1`.
- [ ] Run `CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)` — statement succeeds.
- [ ] Run `INSERT INTO t VALUES (1, 'hello')` — 1 row affected.
- [ ] Run `SELECT * FROM t` — result grid shows `(1, hello)`.
- [ ] Database Navigator shows table `t` with columns `id` and `name`.
- [ ] Create a second table with a foreign key referencing `t` and verify the ER diagram renders
  the relationship arrow.

---

## Known limitations

- **SERIALIZABLE isolation is not supported.** DecentDB uses Snapshot Isolation (mapped to
  JDBC `TRANSACTION_REPEATABLE_READ`).  Attempting to set `TRANSACTION_SERIALIZABLE` throws
  `SQLFeatureNotSupportedException`.
- **READ_UNCOMMITTED is not supported** for the same reason.
- **One writer at a time.** Only one connection may write concurrently.  Multiple read-only
  connections to the same file are allowed.
- **No network mode.** The JDBC URL must point to a local file path.  Remote file systems may
  work but are not tested.
- **DBeaver auto-commit behaviour.** DBeaver sometimes sets `autoCommit=false` on the
  connection.  DDL statements (`CREATE TABLE`, etc.) are transactional in DecentDB and will not
  persist unless you commit or re-enable auto-commit.
- **No `DatabaseMetaData.getCatalogs()` / `getSchemas()`.** DBeaver may show empty catalog/schema
  lists; this is expected.
- **SQL dialect.** DecentDB uses `$1`, `$2`, … positional parameters, not `?`.  The DBeaver SQL
  editor may not syntax-highlight these as parameters.

---

## Troubleshooting

### Native library fails to load

```
UnsatisfiedLinkError: no decentdb_jni in java.library.path
```

- Confirm `libdecentdb_jni.so` (and `libc_api.so`) exist in the directory you passed via
  `-Djava.library.path`.
- On Linux, also ensure the directory is on `LD_LIBRARY_PATH` or that `libc_api.so` is on the
  linker's rpath:
  ```bash
  export LD_LIBRARY_PATH=/path/to/build:$LD_LIBRARY_PATH
  ```
- On macOS, grant the library permission if Gatekeeper blocks it:
  ```bash
  xattr -d com.apple.quarantine libdecentdb_jni.dylib
  ```

### File permissions / path issues

- The database file must be readable (and writable, unless `readOnly=true`) by the JVM process.
- Use absolute paths in the JDBC URL.  Relative paths are resolved against the JVM working
  directory, which may differ from what you expect inside DBeaver.

### "Database locked" / one-writer behaviour

DecentDB allows only one concurrent writer.  If you see:

```
SQLException: database is locked
```

- Close any other connection that has an open write transaction on the same file.
- Increase `busyTimeoutMs` in the connection URL to retry for longer.
- DBeaver may open multiple internal connections; set the connection pool to a maximum of 1
  connection in **Edit Connection → Connection pool settings**.

### DBeaver shows no tables after connecting

- Refresh the Database Navigator (right-click connection → *Refresh*).
- Confirm you connected to the correct `.ddb` file and that the file contains tables.
  From a terminal you can sanity-check with `decentdb list-tables --db=/path/to/mydb.ddb`.
