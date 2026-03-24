# DecentDB JDBC Driver

JDBC driver implementation for DecentDB.

## Benchmark

Run the fair DecentDB vs SQLite benchmark task:

```bash
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
