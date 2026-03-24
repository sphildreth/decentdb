use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use duckdb::Connection as DuckdbConnection;
use rusqlite::Connection as SqliteConnection;
use serde::Serialize;
use tempfile::TempDir;

const INSERT_COUNT: usize = 100_000;
const READ_COUNT: usize = 100_000;
const COMMIT_COUNT: usize = 1000;
const JOIN_COUNT: usize = 1_000;
const JOIN_DATASET_COUNT: usize = 100;
const POINT_READ_STRIDE: usize = 8_191;
const JOIN_READ_STRIDE: usize = 37;

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn benchmark_storage_root() -> PathBuf {
    Path::new("../../target/embedded_compare").to_path_buf()
}

fn append_path_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn point_read_id(iteration: usize) -> usize {
    (iteration * POINT_READ_STRIDE) % INSERT_COUNT
}

fn join_read_id(iteration: usize) -> usize {
    (iteration * JOIN_READ_STRIDE) % JOIN_DATASET_COUNT
}

fn p95_index(len: usize) -> usize {
    ((len as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(len.saturating_sub(1))
}

#[derive(Serialize)]
struct BenchSummary {
    engines: BTreeMap<String, EngineMetrics>,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize, Default, Clone)]
struct EngineMetrics {
    read_p95_ms: f64,
    join_p95_ms: f64,
    commit_p95_ms: f64,
    insert_rows_per_sec: f64,
    db_size_mb: f64,
}

trait DatabaseBenchmarker {
    fn name(&self) -> &'static str;
    fn setup(&mut self, path: &Path);
    fn insert_batch(&mut self) -> f64; // returns rows per second
    fn random_reads(&mut self) -> Vec<u64>; // returns latencies in ns
    fn durable_commits(&mut self) -> Vec<u64>;
    fn join_reads(&mut self) -> Vec<u64>;
    fn teardown(&mut self) -> u64; // returns db size in bytes
}

// -----------------------------------------------------------------------------
// SQLite Implementation
// -----------------------------------------------------------------------------
struct SqliteBenchmarker {
    conn: Option<SqliteConnection>,
    db_path: PathBuf,
    join_seeded: bool,
}

impl SqliteBenchmarker {
    fn new() -> Self {
        Self {
            conn: None,
            db_path: PathBuf::new(),
            join_seeded: false,
        }
    }

    fn ensure_join_seeded(&mut self) {
        if self.join_seeded {
            return;
        }
        let conn = self.conn.as_mut().unwrap();
        let tx = conn.transaction().unwrap();
        {
            let mut users = tx
                .prepare("INSERT INTO join_users (id, name) VALUES (?, ?)")
                .unwrap();
            let mut profiles = tx
                .prepare("INSERT INTO join_profiles (id, bio) VALUES (?, ?)")
                .unwrap();
            for i in 0..JOIN_DATASET_COUNT {
                users
                    .execute(rusqlite::params![i, format!("Join User {}", i)])
                    .unwrap();
                profiles
                    .execute(rusqlite::params![i, format!("Bio {}", i)])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
        self.join_seeded = true;
    }
}

impl DatabaseBenchmarker for SqliteBenchmarker {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn setup(&mut self, path: &Path) {
        self.db_path = path.join("sqlite.db");
        let conn = SqliteConnection::open(&self.db_path).unwrap();

        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode=WAL;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        conn.execute_batch(
            "PRAGMA synchronous=FULL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
              CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL);
             CREATE TABLE join_users (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE join_profiles (id INTEGER PRIMARY KEY, bio TEXT);",
        )
        .unwrap();
        let synchronous: i64 = conn
            .query_row("PRAGMA synchronous;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(synchronous, 2, "expected SQLite synchronous=FULL");
        let wal_autocheckpoint: i64 = conn
            .query_row("PRAGMA wal_autocheckpoint;", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            wal_autocheckpoint, 0,
            "expected SQLite wal_autocheckpoint=0"
        );

        self.conn = Some(conn);
    }

    fn insert_batch(&mut self) -> f64 {
        let conn = self.conn.as_mut().unwrap();
        let start = Instant::now();

        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO users (id, name) VALUES (?, ?)")
                .unwrap();
            for i in 0..INSERT_COUNT {
                stmt.execute(rusqlite::params![i, format!("User {}", i)])
                    .unwrap();
            }
        }
        tx.commit().unwrap();

        let duration = start.elapsed();
        (INSERT_COUNT as f64) / duration.as_secs_f64()
    }

    fn random_reads(&mut self) -> Vec<u64> {
        let conn = self.conn.as_mut().unwrap();
        let mut latencies = Vec::with_capacity(READ_COUNT);

        let mut stmt = conn.prepare("SELECT name FROM users WHERE id = ?").unwrap();
        let warmup_id = point_read_id(READ_COUNT / 2);
        let _warmup: String = stmt.query_row([warmup_id], |row| row.get(0)).unwrap();
        for i in 0..READ_COUNT {
            let id = point_read_id(i);
            let start = Instant::now();
            let _name: String = stmt.query_row([id], |row| row.get(0)).unwrap();
            latencies.push(elapsed_nanos(start));
        }

        latencies
    }

    fn durable_commits(&mut self) -> Vec<u64> {
        let conn = self.conn.as_mut().unwrap();
        conn.execute_batch("PRAGMA synchronous=FULL;").unwrap();

        let mut latencies = Vec::with_capacity(COMMIT_COUNT);

        let mut stmt = conn
            .prepare("INSERT INTO orders (id, user_id, amount) VALUES (?, ?, ?)")
            .unwrap();
        stmt.execute(rusqlite::params![-1, 0, 9.99]).unwrap();
        for i in 0..COMMIT_COUNT {
            let start = Instant::now();
            stmt.execute(rusqlite::params![i, i % 100, 9.99]).unwrap();
            latencies.push(elapsed_nanos(start));
        }

        latencies
    }

    fn join_reads(&mut self) -> Vec<u64> {
        self.ensure_join_seeded();
        let conn = self.conn.as_mut().unwrap();
        let mut latencies = Vec::with_capacity(JOIN_COUNT);

        let mut stmt = conn
            .prepare(
                "SELECT u.name, p.bio \
                 FROM join_users AS u \
                 JOIN join_profiles AS p ON u.id = p.id \
                 WHERE u.id = ?",
            )
            .unwrap();
        let warmup_id = join_read_id(JOIN_COUNT / 2);
        let _warmup: (String, String) = stmt
            .query_row([warmup_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        for i in 0..JOIN_COUNT {
            let id = join_read_id(i);
            let start = Instant::now();
            let _row: (String, String) = stmt
                .query_row([id], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap();
            latencies.push(elapsed_nanos(start));
        }

        latencies
    }

    fn teardown(&mut self) -> u64 {
        if let Some(conn) = self.conn.as_mut() {
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
        }
        self.conn = None;
        let db_size = fs::metadata(&self.db_path).map(|m| m.len()).unwrap_or(0);
        let wal_path = append_path_suffix(&self.db_path, "-wal");
        let wal_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        db_size + wal_size
    }
}

// -----------------------------------------------------------------------------
// DuckDB Implementation
// -----------------------------------------------------------------------------
struct DuckDbBenchmarker {
    conn: Option<DuckdbConnection>,
    db_path: PathBuf,
    join_seeded: bool,
}

impl DuckDbBenchmarker {
    fn new() -> Self {
        Self {
            conn: None,
            db_path: PathBuf::new(),
            join_seeded: false,
        }
    }

    fn ensure_join_seeded(&mut self) {
        if self.join_seeded {
            return;
        }
        let conn = self.conn.as_mut().unwrap();
        conn.execute_batch("BEGIN TRANSACTION;").unwrap();
        {
            let mut users = conn
                .prepare("INSERT INTO join_users (id, name) VALUES (?, ?)")
                .unwrap();
            let mut profiles = conn
                .prepare("INSERT INTO join_profiles (id, bio) VALUES (?, ?)")
                .unwrap();
            for i in 0..JOIN_DATASET_COUNT {
                users
                    .execute(duckdb::params![i, format!("Join User {}", i)])
                    .unwrap();
                profiles
                    .execute(duckdb::params![i, format!("Bio {}", i)])
                    .unwrap();
            }
        }
        conn.execute_batch("COMMIT;").unwrap();
        self.join_seeded = true;
    }
}

impl DatabaseBenchmarker for DuckDbBenchmarker {
    fn name(&self) -> &'static str {
        "duckdb"
    }

    fn setup(&mut self, path: &Path) {
        self.db_path = path.join("duck.db");
        let conn = DuckdbConnection::open(&self.db_path).unwrap();

        conn.execute_batch(
            "SET threads = 1;
              CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR);
               CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount DOUBLE);
              CREATE TABLE join_users (id INTEGER PRIMARY KEY, name VARCHAR);
              CREATE TABLE join_profiles (id INTEGER PRIMARY KEY, bio VARCHAR);",
        )
        .unwrap();

        self.conn = Some(conn);
    }

    fn insert_batch(&mut self) -> f64 {
        let conn = self.conn.as_mut().unwrap();
        let start = Instant::now();

        conn.execute_batch("BEGIN TRANSACTION;").unwrap();
        {
            let mut stmt = conn
                .prepare("INSERT INTO users (id, name) VALUES (?, ?)")
                .unwrap();
            for i in 0..INSERT_COUNT {
                stmt.execute(duckdb::params![i, format!("User {}", i)])
                    .unwrap();
            }
        }
        conn.execute_batch("COMMIT;").unwrap();

        let duration = start.elapsed();
        (INSERT_COUNT as f64) / duration.as_secs_f64()
    }

    fn random_reads(&mut self) -> Vec<u64> {
        let conn = self.conn.as_mut().unwrap();
        let mut latencies = Vec::with_capacity(READ_COUNT);

        let mut stmt = conn.prepare("SELECT name FROM users WHERE id = ?").unwrap();
        let warmup_id = point_read_id(READ_COUNT / 2);
        let _warmup: String = stmt.query_row([warmup_id], |row| row.get(0)).unwrap();
        for i in 0..READ_COUNT {
            let id = point_read_id(i);
            let start = Instant::now();
            let _name: String = stmt.query_row([id], |row| row.get(0)).unwrap();
            latencies.push(elapsed_nanos(start));
        }

        latencies
    }

    fn durable_commits(&mut self) -> Vec<u64> {
        let conn = self.conn.as_mut().unwrap();
        let mut latencies = Vec::with_capacity(COMMIT_COUNT);

        let mut stmt = conn
            .prepare("INSERT INTO orders (id, user_id, amount) VALUES (?, ?, ?)")
            .unwrap();
        stmt.execute(duckdb::params![-1, 0, 9.99]).unwrap();
        for i in 0..COMMIT_COUNT {
            let start = Instant::now();
            stmt.execute(duckdb::params![i, i % 100, 9.99]).unwrap();
            latencies.push(elapsed_nanos(start));
        }

        latencies
    }

    fn join_reads(&mut self) -> Vec<u64> {
        self.ensure_join_seeded();
        let conn = self.conn.as_mut().unwrap();
        let mut latencies = Vec::with_capacity(JOIN_COUNT);

        let mut stmt = conn
            .prepare(
                "SELECT u.name, p.bio \
                 FROM join_users AS u \
                 JOIN join_profiles AS p ON u.id = p.id \
                 WHERE u.id = ?",
            )
            .unwrap();
        let warmup_id = join_read_id(JOIN_COUNT / 2);
        let _warmup: (String, String) = stmt
            .query_row([warmup_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();
        for i in 0..JOIN_COUNT {
            let id = join_read_id(i);
            let start = Instant::now();
            let _row: (String, String) = stmt
                .query_row([id], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap();
            latencies.push(elapsed_nanos(start));
        }

        latencies
    }

    fn teardown(&mut self) -> u64 {
        if let Some(conn) = self.conn.as_mut() {
            conn.execute_batch("CHECKPOINT;").unwrap();
        }
        self.conn = None;
        let db_size = fs::metadata(&self.db_path).map(|m| m.len()).unwrap_or(0);
        let wal_path = append_path_suffix(&self.db_path, ".wal");
        let wal_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        db_size + wal_size
    }
}

// -----------------------------------------------------------------------------
// DecentDB Implementation
// -----------------------------------------------------------------------------
struct DecentDbBenchmarker {
    db: Option<decentdb::Db>,
    db_path: PathBuf,
    join_seeded: bool,
}

impl DecentDbBenchmarker {
    fn new() -> Self {
        Self {
            db: None,
            db_path: PathBuf::new(),
            join_seeded: false,
        }
    }

    fn ensure_join_seeded(&mut self) {
        if self.join_seeded {
            return;
        }
        let db = self.db.as_ref().unwrap();
        let mut txn = db.transaction().unwrap();
        let users = txn
            .prepare("INSERT INTO join_users (id, name) VALUES ($1, $2);")
            .unwrap();
        let profiles = txn
            .prepare("INSERT INTO join_profiles (id, bio) VALUES ($1, $2);")
            .unwrap();
        for i in 0..JOIN_DATASET_COUNT {
            users
                .execute_in(
                    &mut txn,
                    &[
                        decentdb::Value::Int64(i as i64),
                        decentdb::Value::Text(format!("Join User {}", i)),
                    ],
                )
                .unwrap();
            profiles
                .execute_in(
                    &mut txn,
                    &[
                        decentdb::Value::Int64(i as i64),
                        decentdb::Value::Text(format!("Bio {}", i)),
                    ],
                )
                .unwrap();
        }
        txn.commit().unwrap();
        self.join_seeded = true;
    }
}

impl DatabaseBenchmarker for DecentDbBenchmarker {
    fn name(&self) -> &'static str {
        "decentdb"
    }

    fn setup(&mut self, path: &Path) {
        self.db_path = path.join("decent.db");

        let config = decentdb::DbConfig {
            wal_sync_mode: decentdb::WalSyncMode::Full,
            temp_dir: path.to_path_buf(),
            ..decentdb::DbConfig::default()
        };

        let db = decentdb::Db::create(&self.db_path, config).unwrap();

        db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT);")
            .unwrap();
        db.execute("CREATE TABLE orders (id INT64 PRIMARY KEY, user_id INT64, amount FLOAT64);")
            .unwrap();
        db.execute("CREATE TABLE join_users (id INT64 PRIMARY KEY, name TEXT);")
            .unwrap();
        db.execute("CREATE TABLE join_profiles (id INT64 PRIMARY KEY, bio TEXT);")
            .unwrap();

        self.db = Some(db);
    }

    fn insert_batch(&mut self) -> f64 {
        let db = self.db.as_ref().unwrap();
        let start = Instant::now();
        let mut txn = db.transaction().unwrap();
        let insert = txn
            .prepare("INSERT INTO users (id, name) VALUES ($1, $2);")
            .unwrap();
        for i in 0..INSERT_COUNT {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        decentdb::Value::Int64(i as i64),
                        decentdb::Value::Text(format!("User {}", i)),
                    ],
                )
                .unwrap();
        }
        txn.commit().unwrap();

        let duration = start.elapsed();
        (INSERT_COUNT as f64) / duration.as_secs_f64()
    }

    fn random_reads(&mut self) -> Vec<u64> {
        let db = self.db.as_ref().unwrap();
        let select = db.prepare("SELECT name FROM users WHERE id = $1;").unwrap();
        let mut latencies = Vec::with_capacity(READ_COUNT);

        let warmup_id = point_read_id(READ_COUNT / 2);
        let _warmup = select
            .execute(&[decentdb::Value::Int64(warmup_id as i64)])
            .unwrap();
        for i in 0..READ_COUNT {
            let id = point_read_id(i);
            let start = Instant::now();
            let result = select
                .execute(&[decentdb::Value::Int64(id as i64)])
                .unwrap();
            assert_eq!(result.rows().len(), 1);
            let [decentdb::Value::Text(name)] = result.rows()[0].values() else {
                panic!("expected one TEXT column from point lookup");
            };
            let _name = name.clone();
            latencies.push(elapsed_nanos(start));
        }

        latencies
    }

    fn durable_commits(&mut self) -> Vec<u64> {
        let db = self.db.as_ref().unwrap();

        let insert = db
            .prepare("INSERT INTO orders (id, user_id, amount) VALUES ($1, $2, $3);")
            .unwrap();
        let mut latencies = Vec::with_capacity(COMMIT_COUNT);

        insert
            .execute(&[
                decentdb::Value::Int64(-1),
                decentdb::Value::Int64(0),
                decentdb::Value::Float64(9.99),
            ])
            .unwrap();

        for i in 0..COMMIT_COUNT {
            let start = Instant::now();
            insert
                .execute(&[
                    decentdb::Value::Int64(i as i64),
                    decentdb::Value::Int64((i % 100) as i64),
                    decentdb::Value::Float64(9.99),
                ])
                .unwrap();
            latencies.push(elapsed_nanos(start));
        }

        latencies
    }

    fn join_reads(&mut self) -> Vec<u64> {
        self.ensure_join_seeded();
        let db = self.db.as_ref().unwrap();
        let join = db
            .prepare(
                "SELECT u.name, p.bio \
                 FROM join_users AS u \
                 JOIN join_profiles AS p ON u.id = p.id \
                 WHERE u.id = $1;",
            )
            .unwrap();
        let mut latencies = Vec::with_capacity(JOIN_COUNT);

        let warmup_id = join_read_id(JOIN_COUNT / 2);
        let _warmup = join
            .execute(&[decentdb::Value::Int64(warmup_id as i64)])
            .unwrap();
        for i in 0..JOIN_COUNT {
            let id = join_read_id(i);
            let start = Instant::now();
            let result = join.execute(&[decentdb::Value::Int64(id as i64)]).unwrap();
            assert_eq!(result.rows().len(), 1);
            let [decentdb::Value::Text(name), decentdb::Value::Text(bio)] =
                result.rows()[0].values()
            else {
                panic!("expected two TEXT columns from join lookup");
            };
            let _row = (name.clone(), bio.clone());
            latencies.push(elapsed_nanos(start));
        }

        latencies
    }

    fn teardown(&mut self) -> u64 {
        if let Some(db) = self.db.as_ref() {
            db.checkpoint().unwrap();
        }
        self.db = None;
        let db_size = fs::metadata(&self.db_path).map(|m| m.len()).unwrap_or(0);
        let wal_path = append_path_suffix(&self.db_path, ".wal");
        let wal_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        db_size + wal_size
    }
}

// -----------------------------------------------------------------------------
// Runner
// -----------------------------------------------------------------------------

fn run_engine(benchmarker: &mut dyn DatabaseBenchmarker) -> EngineMetrics {
    println!("Running benchmarks for {}...", benchmarker.name());

    let storage_root = benchmark_storage_root();
    fs::create_dir_all(&storage_root).unwrap();
    let temp_dir = TempDir::new_in(storage_root).unwrap();
    let mut metrics = EngineMetrics::default();

    // 1. Setup
    benchmarker.setup(temp_dir.path());

    // 2. Insert Throughput
    metrics.insert_rows_per_sec = benchmarker.insert_batch();
    println!(
        "  -> Insert throughput: {:.0} rows/sec",
        metrics.insert_rows_per_sec
    );

    // 3. Point Reads
    let mut read_latencies = benchmarker.random_reads();
    read_latencies.sort_unstable();
    let p95_read_ns = read_latencies[p95_index(read_latencies.len())];
    metrics.read_p95_ms = p95_read_ns as f64 / 1_000_000.0;
    println!("  -> Read p95: {:.6} ms", metrics.read_p95_ms);

    // 4. Durable Commits
    let mut commit_latencies = benchmarker.durable_commits();
    commit_latencies.sort_unstable();
    let p95_commit_ns = commit_latencies[p95_index(commit_latencies.len())];
    metrics.commit_p95_ms = p95_commit_ns as f64 / 1_000_000.0;
    println!(
        "  -> Auto-commit insert p95: {:.6} ms",
        metrics.commit_p95_ms
    );

    // 5. Joins
    let mut join_latencies = benchmarker.join_reads();
    join_latencies.sort_unstable();
    let p95_join_ns = join_latencies[p95_index(join_latencies.len())];
    metrics.join_p95_ms = p95_join_ns as f64 / 1_000_000.0;
    println!("  -> Join p95: {:.6} ms", metrics.join_p95_ms);

    // 6. Teardown & Size
    let db_size_bytes = benchmarker.teardown();
    metrics.db_size_mb = db_size_bytes as f64 / (1024.0 * 1024.0);
    println!("  -> DB Size: {:.2} MB", metrics.db_size_mb);

    metrics
}

fn main() {
    println!("Starting Embedded DB Benchmarks");

    let storage_root = benchmark_storage_root();
    let mut summary = BenchSummary {
        engines: BTreeMap::new(),
        metadata: BTreeMap::new(),
    };

    // Add unique run ID (unix timestamp in milliseconds)
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    summary
        .metadata
        .insert("run_id".to_string(), run_id.to_string());

    // Add OS info
    summary
        .metadata
        .insert("os".to_string(), std::env::consts::OS.to_string());

    // Attempt to extract the actual CPU model name from /proc/cpuinfo (Linux)
    let mut cpu_model = std::env::consts::ARCH.to_string(); // Fallback
    if let Ok(cpu_info) = std::fs::read_to_string("/proc/cpuinfo") {
        if let Some(line) = cpu_info.lines().find(|l| l.starts_with("model name")) {
            if let Some(model) = line.split(':').nth(1) {
                cpu_model = model.trim().to_string();
            }
        }
    }
    summary.metadata.insert("machine".to_string(), cpu_model);
    summary.metadata.insert(
        "storage_root".to_string(),
        storage_root.display().to_string(),
    );
    summary.metadata.insert(
        "durability_profile".to_string(),
        "engine_specific".to_string(),
    );
    summary.metadata.insert(
        "sqlite_durability".to_string(),
        "wal+synchronous_full+wal_autocheckpoint_0".to_string(),
    );
    summary.metadata.insert(
        "duckdb_durability".to_string(),
        "engine_default".to_string(),
    );
    summary.metadata.insert(
        "decentdb_durability".to_string(),
        "wal_sync_full".to_string(),
    );
    summary.metadata.insert(
        "benchmark_profile".to_string(),
        "single_thread_prepared_statement_oltp".to_string(),
    );
    summary.metadata.insert(
        "insert_workload".to_string(),
        "prepared_single_row_insert_loop_in_one_explicit_transaction".to_string(),
    );
    summary.metadata.insert(
        "read_workload".to_string(),
        "prepared_point_lookup_with_value_materialization".to_string(),
    );
    summary.metadata.insert(
        "commit_workload".to_string(),
        "prepared_single_row_auto_commit_insert_p95".to_string(),
    );
    summary.metadata.insert(
        "join_workload".to_string(),
        "prepared_inner_join_lookup_with_value_materialization".to_string(),
    );
    summary.metadata.insert(
        "read_pattern".to_string(),
        "deterministic_permutation".to_string(),
    );
    summary.metadata.insert(
        "size_measurement".to_string(),
        "db_plus_wal_after_checkpoint".to_string(),
    );
    summary.metadata.insert(
        "latency_capture_unit".to_string(),
        "nanoseconds".to_string(),
    );
    summary.metadata.insert(
        "latency_report_unit".to_string(),
        "milliseconds".to_string(),
    );

    // SQLite
    let mut sqlite = SqliteBenchmarker::new();
    summary
        .engines
        .insert(sqlite.name().to_string(), run_engine(&mut sqlite));

    // DuckDB
    let mut duckdb = DuckDbBenchmarker::new();
    summary
        .engines
        .insert(duckdb.name().to_string(), run_engine(&mut duckdb));

    // DecentDB (Stub)
    let mut decentdb = DecentDbBenchmarker::new();
    summary
        .engines
        .insert(decentdb.name().to_string(), run_engine(&mut decentdb));

    let out_dir = Path::new("../../data");
    fs::create_dir_all(out_dir).unwrap();
    let json_path = out_dir.join("bench_summary.json");

    let json = serde_json::to_string_pretty(&summary).unwrap();
    fs::write(&json_path, json).unwrap();
    println!("Wrote benchmark summary to {}", json_path.display());
}
