# Java Bindings

This directory includes the in-tree JDBC driver (`bindings/java/driver`) and
native/JNI support.

## Benchmark

From this directory:

```bash
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
