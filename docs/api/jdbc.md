# JDBC Driver (Java)

DecentDB ships an **in-process, JNI-backed JDBC driver**.

This driver enables:

- Java applications to connect to `.ddb` database files using JDBC
- GUI tools like DBeaver to connect without a separate server

For DBeaver setup instructions, see the user guide: [DBeaver Integration](../user-guide/dbeaver.md).

---

## Driver class

- `com.decentdb.jdbc.DecentDBDriver`

The driver is discoverable via the standard JDBC SPI (`META-INF/services/java.sql.Driver`).

---

## JDBC URL format

Canonical format:

```
jdbc:decentdb:/absolute/path/to/db.ddb
```

Optional query parameters:

- `readOnly=true|false` (default: `false`)
- `busyTimeoutMs=<int>` (default: `5000`)

Examples:

```
jdbc:decentdb:/home/alice/data/shop.ddb
jdbc:decentdb:/home/alice/data/shop.ddb?readOnly=true
jdbc:decentdb:/home/alice/data/shop.ddb?busyTimeoutMs=10000
```

---

## Transactions and isolation

DecentDB uses Snapshot Isolation.

- Default reported isolation: `Connection.TRANSACTION_REPEATABLE_READ`
- Accepted in `setTransactionIsolation(...)`:
  - `TRANSACTION_REPEATABLE_READ`
  - `TRANSACTION_READ_COMMITTED` (mapped to snapshot isolation)
- Rejected (throws `SQLFeatureNotSupportedException`):
  - `TRANSACTION_SERIALIZABLE`
  - `TRANSACTION_READ_UNCOMMITTED`

Auto-commit behavior:

- When `autoCommit=true`, each statement is its own transaction.
- When `autoCommit=false`, statements run in an explicit transaction until `commit()`/`rollback()`.

---

## Minimal usage

```java
import java.sql.*;

public class Main {
  public static void main(String[] args) throws Exception {
    String url = "jdbc:decentdb:/tmp/my.ddb";

    try (Connection c = DriverManager.getConnection(url);
         Statement s = c.createStatement()) {
      s.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, name TEXT)");
      s.execute("INSERT INTO t (name) VALUES ('hello')");

      try (ResultSet rs = s.executeQuery("SELECT id, name FROM t ORDER BY id")) {
        while (rs.next()) {
          System.out.println(rs.getLong(1) + " " + rs.getString(2));
        }
      }
    }
  }
}
```

---

## Thread-safety and concurrency

- `Connection` objects are intended to be used by a single thread at a time.
- `Statement` and `ResultSet` are not thread-safe.
- DecentDB permits **one writer at a time** to a given database file; multiple concurrent readers are allowed.

---

## Known limitations (current)

- No network mode: the URL points to a local file path.
- Some JDBC features may throw `SQLFeatureNotSupportedException` if not implemented yet.
