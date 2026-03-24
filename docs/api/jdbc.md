# JDBC driver (Java)

DecentDB ships in-tree Java integration under `bindings/java/`.

## Package surfaces

The Java tree currently includes:

- `bindings/java/driver/` — the JDBC driver (`com.decentdb.jdbc.DecentDBDriver`)
- `bindings/java/native/` — the JNI bridge library (`libdecentdb_jni.*`)
- `bindings/java/dbeaver-extension/` — the DBeaver extension bundle

The JDBC driver accepts URLs in the form:

```text
jdbc:decentdb:/absolute/path/to/db.ddb
jdbc:decentdb:/absolute/path/to/db.ddb?readOnly=true&busyTimeoutMs=5000&cachePages=2048
```

Supported connection properties currently include `readOnly`, `busyTimeoutMs`,
and `cachePages`.

## Use the packaged JDBC driver

For application development, prefer the packaged JDBC driver jar instead of
building the Java bindings from source.

The normal consumer path is:

- use a DecentDB release artifact that includes the JDBC driver jar
- or use the jar produced by your own build pipeline or artifact repository

Most developers should not need to rebuild the JNI bridge and driver locally
unless they are working on the Java bindings themselves.

## Build the driver locally

From the repository root:

```bash
cargo build -p decentdb
cd bindings/java/native && make
cd .. && ./gradlew :driver:jar
```

This produces:

- `target/debug/libdecentdb_jni.*` — the JNI bridge
- `bindings/java/driver/build/libs/` — the built JDBC driver jar

When the driver jar is built after the native libraries, it embeds the matching
native resources for the current OS/arch.

## Run the package tests

```bash
cd bindings/java
./gradlew :driver:test
```

## Run the low-level smoke validation

The repository also keeps a direct Java FFM smoke path under
`tests/bindings/java/Smoke.java` to validate the raw C ABI independently of the
packaged JDBC driver.

```bash
cargo build -p decentdb
javac tests/bindings/java/Smoke.java
java --enable-native-access=ALL-UNNAMED -cp tests/bindings/java Smoke
```

For DBeaver integration details, see the
[DBeaver guide](../user-guide/dbeaver.md).
