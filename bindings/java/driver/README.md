# DecentDB JDBC Driver

JDBC driver implementation for DecentDB.

## Semantic result values

JDBC metadata maps `DATE`, `TIME`, and `TIMESTAMPTZ` to standard JDBC temporal
types. `ENUM`, `IPADDR`, `CIDR`, and `INTERVAL` are exposed as strings,
`MACADDR` as `Types.OTHER` with a canonical string value, and `TIMESTAMPTZ`
values are normalized to UTC timestamps.

## Queued write options

JDBC URLs and `Properties` may pass native queue options through at open time:

```text
jdbc:decentdb:/tmp/app.ddb?write_queue_enabled=true&write_queue_capacity=128&write_queue_default_timeout_ms=1000
```

The JNI layer maps queue timeout, cancel, queue-full, queue-closed, and busy
statuses to distinct JDBC transient/timeout exceptions. Prepared statements
remain on the direct prepared path until the C ABI exposes queued
prepared-statement execution.

## Standalone example

Run the standalone JDBC example:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :driver:runCrudExample
```

Pass `-PexampleArgs="/absolute/path/to/example.ddb"` to keep the example
database instead of using a temp file.

## Benchmark

Run the fair DecentDB vs SQLite benchmark task:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
./gradlew :driver:benchmarkFetch -PbenchmarkArgs="--count 100000 --point-reads 5000 --fetchmany-batch 1024 --db-prefix java_bench_fetch"
```

Benchmark options:

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
