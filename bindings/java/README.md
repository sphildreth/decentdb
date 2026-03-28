# Java Bindings

This directory includes the in-tree JDBC driver (`bindings/java/driver`) and
native/JNI support.

## Standalone example

From this directory:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :driver:runCrudExample
```

Pass `-PexampleArgs="/absolute/path/to/example.ddb"` to keep the example
database instead of using a temp file.

## Benchmark

From this directory:

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
- `--db-prefix <prefix>` (DecentDB writes `.ddb`, SQLite writes `.db`)
- `--sqlite-jdbc <jar_path>`
- `--keep-db`

The benchmark auto-discovers a local `sqlite-jdbc` jar from common DBeaver,
Rider, and Maven cache locations when possible.
