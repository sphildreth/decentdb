use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use crate::catalog::{ColumnSchema, ColumnType, IndexKind, IndexSchema, TableSchema, ViewSchema};
use crate::config::DbConfig;
use crate::db::SqlTxnSlot;
use crate::error::{DbError, Result};
use crate::exec::{
    decode_paged_table_manifest_payload, EngineRuntime, RuntimeIndex, TableData, TableRowSource,
};
use crate::record::overflow::read_overflow;
use crate::storage::header::DB_HEADER_SIZE;
use crate::storage::page::{PageId, PageStore};
use crate::storage::DatabaseHeader;

use crate::exec::dml::{PreparedInsertColumn, PreparedInsertValueSource, PreparedSimpleInsert};
use crate::sql::parser::parse_sql_statement;
use crate::{BulkLoadOptions, Db, QueuedWriteOptions, Value, WalSyncMode};

use super::{
    parse_simple_count_star_sql, parse_simple_row_id_projection_sql,
    parse_simple_row_id_range_projection_sql, simple_single_statement_fast_path_sql,
    split_sql_batch, PreparedInsertCache, StatementCache, TempSchemaState,
};

#[derive(Debug)]
struct PagerReadStore<'a> {
    db: &'a Db,
}

impl PageStore for PagerReadStore<'_> {
    fn page_size(&self) -> u32 {
        self.db.config().page_size
    }

    fn allocate_page(&mut self) -> Result<PageId> {
        Err(DbError::internal(
            "PagerReadStore does not support page allocation",
        ))
    }

    fn free_page(&mut self, _page_id: PageId) -> Result<()> {
        Err(DbError::internal(
            "PagerReadStore does not support freeing pages",
        ))
    }

    fn read_page(&self, page_id: PageId) -> Result<Arc<[u8]>> {
        self.db.read_page(page_id)
    }

    fn advise_sequential(&self) -> Result<()> {
        self.db.advise_sequential()
    }

    fn write_page(&mut self, _page_id: PageId, _data: &[u8]) -> Result<()> {
        Err(DbError::internal(
            "PagerReadStore does not support writing pages",
        ))
    }
}

fn read_header_from_path(path: &Path) -> DatabaseHeader {
    let bytes = std::fs::read(path).expect("read database bytes");
    let mut header = [0_u8; DB_HEADER_SIZE];
    header.copy_from_slice(&bytes[..DB_HEADER_SIZE]);
    DatabaseHeader::decode(&header).expect("decode database header")
}

#[test]
fn queued_writes_batch_ready_commits_and_preserve_results() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("queued-group-commit.ddb");
    let mut config = DbConfig {
        write_queue_capacity: 32,
        write_queue_max_batch: 32,
        write_queue_max_group_delay_us: 50_000,
        ..DbConfig::default()
    };
    config.release_freed_memory_after_checkpoint = false;
    let db = Arc::new(Db::create(&path, config).expect("create db"));
    db.execute("CREATE TABLE queued_items (id INTEGER PRIMARY KEY, value TEXT)")
        .expect("create table");

    let writers = 8;
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::new();
    for index in 0..writers {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            db.execute_queued_with_params(
                "INSERT INTO queued_items (id, value) VALUES ($1, $2)",
                &[
                    Value::Int64(i64::try_from(index).expect("index fits")),
                    Value::Text(format!("value-{index}")),
                ],
            )
            .expect("queued insert");
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("writer thread");
    }

    let result = db
        .execute("SELECT COUNT(*) FROM queued_items")
        .expect("count rows");
    assert_eq!(result.rows()[0].values()[0], Value::Int64(writers as i64));

    let metrics = db.write_queue_metrics();
    assert_eq!(metrics.admitted, writers as u64);
    assert_eq!(metrics.committed, writers as u64);
    assert!(metrics.group_commit_batches >= 1);
    assert!(
        metrics.group_commit_max_batch > 1,
        "expected at least one grouped batch, got {metrics:?}"
    );
    assert!(metrics.group_commit_syncs >= 1);
    assert!(metrics.physical_syncs_saved >= 1);
}

#[test]
fn queued_write_timeout_before_execution_has_no_effect() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("queued-timeout.ddb");
    let db = Arc::new(Db::create(&path, DbConfig::default()).expect("create db"));
    db.execute("CREATE TABLE queued_items (id INTEGER PRIMARY KEY)")
        .expect("create table");

    let writer_guard = db
        .inner
        .sql_write_lock
        .lock()
        .expect("writer lock should not be poisoned");
    let started = Arc::new(AtomicBool::new(false));
    let worker_db = Arc::clone(&db);
    let worker_started = Arc::clone(&started);
    let handle = thread::spawn(move || {
        worker_started.store(true, AtomicOrdering::Release);
        worker_db
            .execute_queued("INSERT INTO queued_items (id) VALUES (1)")
            .expect("first queued insert");
    });
    while !started.load(AtomicOrdering::Acquire) || db.write_queue_metrics().executed == 0 {
        thread::sleep(Duration::from_millis(1));
    }

    let error = db
        .execute_queued_batch_with_options(
            "INSERT INTO queued_items (id) VALUES (2)",
            &[],
            QueuedWriteOptions {
                timeout: Some(Duration::from_millis(10)),
                cancel_token: None,
            },
        )
        .expect_err("second queued insert should time out before execution");
    assert_eq!(error.code(), crate::DbErrorCode::Timeout);

    drop(writer_guard);
    handle.join().expect("writer thread");
    db.execute_queued("INSERT INTO queued_items (id) VALUES (3)")
        .expect("third queued insert drains canceled request");

    let result = db
        .execute("SELECT COUNT(*) FROM queued_items WHERE id IN (1, 3)")
        .expect("count committed rows");
    assert_eq!(result.rows()[0].values()[0], Value::Int64(2));
    let missing = db
        .execute("SELECT COUNT(*) FROM queued_items WHERE id = 2")
        .expect("count timed-out row");
    assert_eq!(missing.rows()[0].values()[0], Value::Int64(0));
}

#[test]
fn queued_execution_rejects_explicit_transaction_control() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("queued-transaction-control.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");

    let error = db
        .execute_queued_batch("BEGIN; CREATE TABLE t (id INTEGER); COMMIT")
        .expect_err("queued transaction control should be rejected");
    assert_eq!(error.code(), crate::DbErrorCode::Transaction);
}

#[test]
fn queued_execution_honors_cancel_before_admission() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("queued-cancel.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");
    let cancel_token = Arc::new(AtomicBool::new(true));

    let error = db
        .execute_queued_batch_with_options(
            "CREATE TABLE t (id INTEGER)",
            &[],
            QueuedWriteOptions {
                timeout: None,
                cancel_token: Some(cancel_token),
            },
        )
        .expect_err("pre-canceled queued write should not be admitted");
    assert_eq!(error.code(), crate::DbErrorCode::Canceled);
    assert_eq!(db.write_queue_metrics().canceled, 1);
}

#[test]
fn execute_batch_schema_only_ddl_is_single_commit_and_queryable() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("ddl-schema-batch.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");

    let before_lsn = db.inner.wal.latest_snapshot();
    let results = db
        .execute_batch(
            "CREATE TABLE artists (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
             );
             CREATE INDEX idx_artists_name ON artists(name);
             CREATE VIEW artist_names AS
                SELECT id, name FROM artists;",
        )
        .expect("execute schema-only batch");
    assert_eq!(results.len(), 3);
    assert!(db.inner.wal.latest_snapshot() > before_lsn);

    db.execute("INSERT INTO artists (id, name) VALUES (1, 'Ada')")
        .expect("seed artist");
    db.execute("INSERT INTO artists (id, name) VALUES (2, 'Bob')")
        .expect("second artist");

    let count = db
        .execute("SELECT COUNT(*) FROM artist_names")
        .expect("query artist view");
    assert_eq!(scalar_i64(&count), 2);
}

#[test]
fn zero_row_prepared_dml_does_not_commit_autocommit_wal_frame() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("zero-row-prepared-dml.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");

    db.execute(
        "CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            value TEXT
         )",
    )
    .expect("create table");
    db.execute("INSERT INTO items (id, value) VALUES (1, 'one')")
        .expect("seed row");

    let before_noop = db.inner.wal.latest_snapshot();
    let deleted = db
        .execute_with_params("DELETE FROM items WHERE id = $1", &[Value::Int64(99)])
        .expect("delete missing row");
    assert_eq!(deleted.affected_rows(), 0);
    assert_eq!(db.inner.wal.latest_snapshot(), before_noop);

    let updated = db
        .execute_with_params(
            "UPDATE items SET value = $1 WHERE id = $2",
            &[Value::Text("missing".to_string()), Value::Int64(99)],
        )
        .expect("update missing row");
    assert_eq!(updated.affected_rows(), 0);
    assert_eq!(db.inner.wal.latest_snapshot(), before_noop);

    let deleted = db
        .execute_with_params("DELETE FROM items WHERE id = $1", &[Value::Int64(1)])
        .expect("delete existing row");
    assert_eq!(deleted.affected_rows(), 1);
    assert!(db.inner.wal.latest_snapshot() > before_noop);
}

#[test]
fn execute_batch_mixed_statements_fallback_to_per_statement_flow() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("ddl-then-dml-fallback.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");

    let results = db
        .execute_batch(
            "CREATE TABLE entities (id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO entities (id, name) VALUES (1, 'value')",
        )
        .expect("mixed batch still executes");
    assert_eq!(results.len(), 2);

    let count = db
        .execute("SELECT COUNT(*) FROM entities")
        .expect("count inserted row");
    assert_eq!(scalar_i64(&count), 1);
}

#[test]
fn statement_cache_reuses_parsed_statement() {
    let mut cache = StatementCache::with_capacity(4);
    let first = cache.get_or_parse("SELECT 1").expect("parse");
    let second = cache.get_or_parse("SELECT 1").expect("cache hit");
    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn statement_cache_evicts_oldest_entry_when_full() {
    let mut cache = StatementCache::with_capacity(1);
    let first = cache.get_or_parse("SELECT 1").expect("parse first");
    let _second = cache.get_or_parse("SELECT 2").expect("parse second");
    let first_again = cache.get_or_parse("SELECT 1").expect("reparse evicted");
    assert!(!Arc::ptr_eq(&first, &first_again));
}

#[test]
fn statement_cache_lru_promotes_on_hit() {
    let mut cache = StatementCache::with_capacity(2);
    let first = cache.get_or_parse("SELECT 1").expect("parse first");
    let second = cache.get_or_parse("SELECT 2").expect("parse second");
    let first_hit = cache.get_or_parse("SELECT 1").expect("cache hit");
    assert!(Arc::ptr_eq(&first, &first_hit));

    let _third = cache.get_or_parse("SELECT 3").expect("parse third");
    let first_again = cache.get_or_parse("SELECT 1").expect("first still cached");
    let second_again = cache.get_or_parse("SELECT 2").expect("reparse evicted");

    assert!(Arc::ptr_eq(&first, &first_again));
    assert!(!Arc::ptr_eq(&second, &second_again));
}

#[test]
fn simple_count_sql_fast_path_parser_accepts_only_plain_count_star() {
    let plan = parse_simple_count_star_sql("SELECT COUNT(*) FROM songs").expect("simple count");
    assert_eq!(plan.table_name, "songs");
    assert!(parse_simple_count_star_sql("SELECT COUNT(id) FROM songs").is_none());
    assert!(parse_simple_count_star_sql("SELECT COUNT(*) FROM songs WHERE id = 1").is_none());
}

#[test]
fn simple_row_id_projection_sql_fast_path_parser_extracts_projection() {
    let plan =
        parse_simple_row_id_projection_sql("SELECT id, name, country FROM artists WHERE id = $1")
            .expect("simple rowid projection");
    assert_eq!(plan.table_name, "artists");
    assert_eq!(plan.projection_columns, vec!["id", "name", "country"]);
    assert_eq!(plan.filter_column, "id");
    assert_eq!(plan.param_index, 0);
    assert!(parse_simple_row_id_projection_sql(
        "SELECT id, upper(name) FROM artists WHERE id = $1"
    )
    .is_none());
}

#[test]
fn simple_row_id_range_projection_sql_parser_extracts_bounds_and_limit() {
    let plan = parse_simple_row_id_range_projection_sql(
        "SELECT name FROM users WHERE id >= $1 AND id < $2 ORDER BY id LIMIT $3",
    )
    .expect("simple rowid range projection");
    assert_eq!(plan.table_name, "users");
    assert_eq!(plan.projection_columns, vec!["name"]);
    assert_eq!(plan.filter_column, "id");
    assert_eq!(
        plan.lower_bound,
        Some(super::PreparedSimpleRangeBoundParam {
            inclusive: true,
            param_index: 0
        })
    );
    assert_eq!(
        plan.upper_bound,
        Some(super::PreparedSimpleRangeBoundParam {
            inclusive: false,
            param_index: 1
        })
    );
    assert_eq!(plan.limit_param_index, 2);

    let reversed = parse_simple_row_id_range_projection_sql(
        "SELECT name FROM users WHERE $1 <= id AND $2 > id ORDER BY id ASC LIMIT $3",
    )
    .expect("reversed range predicates");
    assert_eq!(reversed.lower_bound, plan.lower_bound);
    assert_eq!(reversed.upper_bound, plan.upper_bound);
    assert!(parse_simple_row_id_range_projection_sql(
        "SELECT name FROM users WHERE id >= $1 AND id < $2 ORDER BY id DESC LIMIT $3"
    )
    .is_none());
    assert!(parse_simple_row_id_range_projection_sql(
        "SELECT name FROM users WHERE id >= $1 AND id < $2 ORDER BY name LIMIT $3"
    )
    .is_none());
    assert!(parse_simple_row_id_range_projection_sql(
        "SELECT upper(name) FROM users WHERE id >= $1 AND id < $2 ORDER BY id LIMIT $3"
    )
    .is_none());
}

#[test]
fn single_statement_fast_path_accepts_optional_trailing_semicolon_only() {
    assert_eq!(
        simple_single_statement_fast_path_sql(" SELECT id FROM artists WHERE id = $1; "),
        Some("SELECT id FROM artists WHERE id = $1")
    );
    assert!(simple_single_statement_fast_path_sql("").is_none());
    assert!(simple_single_statement_fast_path_sql("SELECT 1; SELECT 2").is_none());
    assert!(simple_single_statement_fast_path_sql("SELECT 1;;").is_none());
}

#[test]
fn drop_does_not_block_indefinitely() -> Result<()> {
    let dir = TempDir::new().expect("create temp dir");
    let path = dir.path().join("drop-checkpoint.ddb");
    let mut config = DbConfig {
        checkpoint_timeout_sec: 60,
        ..DbConfig::default()
    };
    config.wal_checkpoint_threshold_pages = u32::MAX;
    config.wal_checkpoint_threshold_bytes = u64::MAX;

    let db = Db::open_or_create(&path, config.clone())?;
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT NOT NULL)")?;
    for id in 0..256 {
        db.execute_with_params(
            "INSERT INTO docs (id, body) VALUES ($1, $2)",
            &[
                Value::Int64(id),
                Value::Text(format!("payload-{id}-{}", "x".repeat(256))),
            ],
        )?;
    }

    let started = Instant::now();
    drop(db);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "drop should not wait for the full checkpoint timeout"
    );

    let reopened = Db::open(&path, config)?;
    let result = reopened.execute("SELECT COUNT(*) FROM docs")?;
    assert_eq!(result.rows()[0].values(), &[Value::Int64(256)]);
    Ok(())
}

#[test]
fn split_sql_batch_preserves_legacy_trigger_body_statement() {
    let statements = split_sql_batch(
        "CREATE TRIGGER log_insert AFTER INSERT ON users
             FOR EACH ROW BEGIN
               SELECT decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user added'')');
             END;
             INSERT INTO users VALUES (1, 'Ada');",
    );
    assert_eq!(statements.len(), 2);
    assert_eq!(
            statements[0],
            "CREATE TRIGGER log_insert AFTER INSERT ON users
             FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user added'')')"
        );
    assert_eq!(statements[1], "INSERT INTO users VALUES (1, 'Ada')");
}

#[test]
fn temp_schema_apply_is_shallow_when_unmutated() {
    let table = TableSchema {
        name: "temp_docs".to_string(),
        temporary: true,
        columns: vec![ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
            spatial_type: None,
            enum_type: None,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: true,
            unique: true,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        }],
        checks: Vec::new(),
        foreign_keys: Vec::new(),
        primary_key_columns: vec!["id".to_string()],
        next_row_id: 1,
        pk_index_root: None,
    };
    let view = ViewSchema {
        name: "temp_view".to_string(),
        temporary: true,
        sql_text: "SELECT id FROM temp_docs".to_string(),
        column_names: vec!["id".to_string()],
        dependencies: vec!["temp_docs".to_string()],
    };
    let index = IndexSchema {
        name: "temp_docs_pk".to_string(),
        table_name: "temp_docs".to_string(),
        kind: IndexKind::Btree,
        unique: true,
        columns: Vec::new(),
        include_columns: Vec::new(),
        predicate_sql: None,
        full_text: None,
        fresh: false,
    };
    let state = TempSchemaState {
        schema_cookie: 7,
        tables: Arc::new(BTreeMap::from([(table.name.clone(), table)])),
        table_data: Arc::new(BTreeMap::from([(
            "temp_docs".to_string(),
            Arc::new(TableData::default()),
        )])),
        views: Arc::new(BTreeMap::from([(view.name.clone(), view)])),
        indexes: Arc::new(BTreeMap::from([(index.name.clone(), index)])),
    };
    let mut runtime = EngineRuntime::empty(1);

    state.apply_to_runtime(&mut runtime);

    assert_eq!(runtime.temp_schema_cookie, 7);
    assert!(Arc::ptr_eq(&state.tables, &runtime.temp_tables));
    assert!(Arc::ptr_eq(&state.table_data, &runtime.temp_table_data));
    assert!(Arc::ptr_eq(&state.views, &runtime.temp_views));
    assert!(Arc::ptr_eq(&state.indexes, &runtime.temp_indexes));
}

#[test]
fn pragma_page_size_query() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    let result = db.execute("PRAGMA page_size").expect("pragma page_size");
    assert_eq!(result.columns(), &["page_size".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(i64::from(db.config().page_size))]
    );
}

#[test]
fn pragma_cache_size_query() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    let result = db.execute("PRAGMA cache_size").expect("pragma cache_size");
    assert_eq!(result.columns(), &["cache_size".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(super::cache_size_pages(db.config()))]
    );
}

#[test]
fn pragma_integrity_check_query() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, val TEXT)")
        .expect("create table");
    db.execute("INSERT INTO t VALUES (1, 'a')")
        .expect("insert row");
    let result = db
        .execute("PRAGMA integrity_check")
        .expect("pragma integrity_check");
    assert_eq!(result.columns(), &["integrity_check".to_string()]);
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Text("ok".to_string())]);
}

#[test]
fn pragma_database_list_query() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    let result = db
        .execute("PRAGMA database_list")
        .expect("pragma database_list");
    assert_eq!(
        result.columns(),
        &["seq".to_string(), "name".to_string(), "file".to_string()]
    );
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(0),
            Value::Text("main".to_string()),
            Value::Text(":memory:".to_string())
        ]
    );
}

#[test]
fn enum_columns_store_label_ids_and_dump_labels() -> Result<()> {
    let dir = TempDir::new().expect("create temp dir");
    let path = dir.path().join("enum.ddb");
    let db = Db::open_or_create(&path, DbConfig::default())?;
    db.execute(
        "CREATE TABLE tickets(
                id INT PRIMARY KEY,
                state ENUM('open', 'closed', 'blocked') NOT NULL
            )",
    )?;
    db.execute("INSERT INTO tickets VALUES (1, 'open'), (2, 'closed')")?;

    let result = db.execute("SELECT state FROM tickets ORDER BY id")?;
    let Value::Enum {
        enum_type_id,
        label_id,
    } = result.rows()[0].values()[0]
    else {
        panic!("expected enum value");
    };
    assert_ne!(enum_type_id, 0);
    assert_eq!(label_id, 0);
    assert_eq!(
        result.rows()[1].values()[0],
        Value::Enum {
            enum_type_id,
            label_id: 1
        }
    );
    assert!(db
        .execute("INSERT INTO tickets VALUES (3, 'missing')")
        .is_err());

    let ddl = db.table_ddl("tickets")?;
    assert!(ddl.contains("ENUM('open', 'closed', 'blocked')"));
    let dump = db.dump_sql()?;
    assert!(dump.contains("\"state\" ENUM('open', 'closed', 'blocked') NOT NULL"));
    assert!(dump.contains("VALUES (1, 'open')"));
    drop(db);

    let reopened = Db::open(&path, DbConfig::default())?;
    reopened.execute("INSERT INTO tickets VALUES (3, 'blocked')")?;
    reopened.execute("UPDATE tickets SET state = 'closed' WHERE id = 3")?;
    assert!(reopened
        .execute("INSERT INTO tickets VALUES (4, 'missing')")
        .is_err());
    let reopened_result = reopened.execute("SELECT state FROM tickets ORDER BY id")?;
    assert_eq!(
        reopened_result.rows()[0].values()[0],
        result.rows()[0].values()[0]
    );
    assert_eq!(
        reopened_result.rows()[2].values()[0],
        Value::Enum {
            enum_type_id,
            label_id: 1
        }
    );
    Ok(())
}

#[test]
fn macaddr_columns_store_binary_and_dump_text() -> Result<()> {
    let dir = TempDir::new().expect("create temp dir");
    let path = dir.path().join("macaddr.ddb");
    let db = Db::open_or_create(&path, DbConfig::default())?;
    db.execute("CREATE TABLE devices(id INT PRIMARY KEY, mac MACADDR UNIQUE)")?;
    db.execute(
        "INSERT INTO devices VALUES
                (1, '08:00:2b:01:02:03'),
                (2, '08:00:2b:01:02:03:04:05')",
    )?;

    let result = db.execute("SELECT mac FROM devices ORDER BY mac")?;
    assert_eq!(
        result.rows()[0].values()[0],
        Value::MacAddr {
            len: 6,
            bytes: [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0, 0]
        }
    );
    assert_eq!(
        result.rows()[1].values()[0],
        Value::MacAddr {
            len: 8,
            bytes: [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]
        }
    );
    assert!(db.execute("INSERT INTO devices VALUES (3, 'bad')").is_err());
    let dump = db.dump_sql()?;
    assert!(dump.contains("\"mac\" MACADDR UNIQUE"));
    assert!(dump.contains("'08:00:2b:01:02:03'"));
    drop(db);

    let reopened = Db::open(&path, DbConfig::default())?;
    let reopened_result = reopened.execute("SELECT COUNT(*) FROM devices")?;
    assert_eq!(reopened_result.rows()[0].values(), &[Value::Int64(2)]);
    Ok(())
}

#[test]
fn spatial_geography_points_and_indexed_radius_query() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE places(
                id INT PRIMARY KEY,
                name TEXT,
                geog GEOGRAPHY(POINT,4326)
            )",
    )
    .expect("create places");
    db.execute("CREATE INDEX places_geog_spatial ON places USING spatial (geog)")
        .expect("create spatial index");
    db.execute(
        "INSERT INTO places VALUES
                (1, 'austin', ST_GeogPoint(-97.7431, 30.2672)),
                (2, 'dallas', ST_GeogPoint(-96.7970, 32.7767)),
                (3, 'houston', ST_GeogPoint(-95.3698, 29.7604))",
    )
    .expect("insert places");

    let nearby = db
        .execute(
            "SELECT id FROM places
                 WHERE ST_DWithin(geog, ST_GeogPoint(-97.7431, 30.2672), 5000)
                 ORDER BY id",
        )
        .expect("radius query");
    assert_eq!(nearby.rows().len(), 1);
    assert_eq!(nearby.rows()[0].values(), &[Value::Int64(1)]);

    let accessors = db
        .execute("SELECT ST_SRID(geog), ST_X(geog), ST_Y(geog) FROM places WHERE id = 1")
        .expect("spatial accessors");
    assert_eq!(accessors.rows()[0].values()[0], Value::Int64(4326));
    assert!(
        matches!(accessors.rows()[0].values()[1], Value::Float64(x) if (x + 97.7431).abs() < 1e-9)
    );
    assert!(
        matches!(accessors.rows()[0].values()[2], Value::Float64(y) if (y - 30.2672).abs() < 1e-9)
    );

    let explain = db
        .execute(
            "EXPLAIN SELECT id FROM places
                 WHERE ST_DWithin(geog, ST_GeogPoint(-97.7431, 30.2672), 5000)",
        )
        .expect("explain spatial filter");
    assert!(explain
        .explain_lines()
        .iter()
        .any(|line| line.contains("SpatialFilter")));

    let knn = db
        .execute(
            "EXPLAIN SELECT id FROM places
                 ORDER BY geog <-> ST_GeogPoint(-97.7431, 30.2672)
                 LIMIT 1",
        )
        .expect("explain spatial knn");
    assert!(knn
        .explain_lines()
        .iter()
        .any(|line| line.contains("SpatialKnn")));
}

#[test]
fn spatial_geometry_polygons_predicates_and_measurements() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE regions(
                id INT PRIMARY KEY,
                area GEOMETRY(POLYGON,0)
            )",
    )
    .expect("create regions");
    db.execute("CREATE INDEX regions_area_spatial ON regions USING spatial (area)")
        .expect("create spatial index");
    db.execute(
        "INSERT INTO regions VALUES
             (1, ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'))",
    )
    .expect("insert polygon");

    let result = db
        .execute(
            "SELECT
                    ST_Contains(area, ST_GeomFromText('POINT(2 2)')),
                    ST_Within(ST_GeomFromText('POINT(2 2)'), area),
                    ST_Intersects(area, ST_GeomFromText('POINT(12 12)')),
                    ST_Area(area)
                 FROM regions
                 WHERE ST_Intersects(area, ST_GeomFromText('POINT(2 2)'))",
        )
        .expect("polygon predicates");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values()[0], Value::Bool(true));
    assert_eq!(result.rows()[0].values()[1], Value::Bool(true));
    assert_eq!(result.rows()[0].values()[2], Value::Bool(false));
    assert!(
        matches!(result.rows()[0].values()[3], Value::Float64(area) if (area - 100.0).abs() < 1e-9)
    );
}

#[test]
fn spatial_geometry_point_in_polygon_join_uses_spatial_index() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE houses(
                id INT PRIMARY KEY,
                location GEOMETRY(POINT,0)
            )",
    )
    .expect("create houses");
    db.execute(
        "CREATE TABLE zones(
                id INT PRIMARY KEY,
                boundary GEOMETRY(POLYGON,0)
            )",
    )
    .expect("create zones");
    db.execute("CREATE INDEX zones_boundary_spatial ON zones USING spatial(boundary)")
        .expect("create spatial index");
    db.execute(
        "INSERT INTO houses VALUES
                (1, ST_GeomFromText('POINT(2 2)')),
                (2, ST_GeomFromText('POINT(12 12)')),
                (3, ST_GeomFromText('POINT(22 22)'))",
    )
    .expect("insert houses");
    db.execute(
        "INSERT INTO zones VALUES
                (10, ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))')),
                (20, ST_GeomFromText('POLYGON((10 10,20 10,20 20,10 20,10 10))'))",
    )
    .expect("insert zones");

    let result = db
        .execute(
            "SELECT h.id, z.id
                 FROM houses h JOIN zones z
                   ON ST_Contains(z.boundary, h.location)",
        )
        .expect("spatial join");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(10)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Int64(20)]
    );

    let explain = db
        .execute(
            "EXPLAIN SELECT h.id, z.id
                 FROM houses h JOIN zones z
                   ON ST_Contains(z.boundary, h.location)",
        )
        .expect("explain spatial join");
    assert!(explain
        .explain_lines()
        .iter()
        .any(|line| line.contains("SpatialJoin")));
}

#[test]
fn spatial_schema_values_and_indexes_persist_after_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("spatial.ddb");
    let path = path.to_string_lossy().to_string();
    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        db.execute(
            "CREATE TABLE places(
                    id INT PRIMARY KEY,
                    geog GEOGRAPHY(POINT,4326)
                )",
        )
        .expect("create places");
        db.execute("CREATE INDEX idx_places_geog ON places USING spatial(geog)")
            .expect("create spatial index");
        db.execute("INSERT INTO places VALUES (1, ST_GeogPoint(-97.7431, 30.2672))")
            .expect("insert place");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen db");
    let result = db
        .execute(
            "SELECT ST_SRID(geog), ST_DWithin(geog, ST_GeogPoint(-97.7431, 30.2672), 1)
                 FROM places",
        )
        .expect("query reopened spatial data");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values()[0], Value::Int64(4326));
    assert_eq!(result.rows()[0].values()[1], Value::Bool(true));

    let indexes = db.list_indexes().expect("list indexes");
    assert!(indexes
        .iter()
        .any(|index| index.name == "idx_places_geog" && index.kind == "spatial"));
}

#[test]
fn pragma_table_info_query() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, name TEXT DEFAULT 'anon')")
        .expect("create table");

    let result = db.execute("PRAGMA table_info(t)").expect("table_info");
    assert_eq!(
        result.columns(),
        &[
            "cid".to_string(),
            "name".to_string(),
            "type".to_string(),
            "notnull".to_string(),
            "dflt_value".to_string(),
            "pk".to_string()
        ]
    );
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(0),
            Value::Text("id".to_string()),
            Value::Text("INT64".to_string()),
            Value::Int64(1),
            Value::Null,
            Value::Int64(1)
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Text("name".to_string()),
            Value::Text("TEXT".to_string()),
            Value::Int64(0),
            Value::Text("'anon'".to_string()),
            Value::Int64(0)
        ]
    );
}

#[test]
fn pragma_table_info_assignment_is_rejected() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    let error = db
        .execute("PRAGMA table_info = 1")
        .expect_err("assignment should fail");
    assert!(
        error.to_string().contains("does not support assignment"),
        "unexpected error: {error}"
    );
}

#[test]
fn pragma_assignments_are_limited() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    let page_size_noop = db.execute("PRAGMA page_size = 4096");
    assert!(
        page_size_noop.is_ok(),
        "expected no-op assignment to succeed"
    );

    let cache_size_error = db
        .execute("PRAGMA cache_size = 8")
        .expect_err("cache_size assignment should fail");
    assert!(cache_size_error
        .to_string()
        .contains("cannot be changed on an open connection"));

    let integrity_assignment_error = db
        .execute("PRAGMA integrity_check = 1")
        .expect_err("integrity_check assignment should fail");
    assert!(integrity_assignment_error
        .to_string()
        .contains("does not support assignment"));
}

#[test]
fn unsupported_pragma_reports_sql_error() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    let error = db
        .execute("PRAGMA auto_vacuum")
        .expect_err("unsupported pragma should fail");
    assert!(error.to_string().contains("not supported"));
}

#[test]
fn pragma_compat_queries_are_implemented() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, val TEXT)")
        .expect("create table");
    assert_eq!(
        db.execute("PRAGMA quick_check")
            .expect("quick_check")
            .rows()[0]
            .values(),
        &[Value::Text("ok".to_string())]
    );
    assert_eq!(
        db.execute("PRAGMA foreign_keys")
            .expect("foreign_keys")
            .rows()[0]
            .values(),
        &[Value::Int64(1)]
    );
    assert_eq!(
        db.execute("PRAGMA journal_mode")
            .expect("journal_mode")
            .rows()[0]
            .values(),
        &[Value::Text("wal".to_string())]
    );
    assert_eq!(
        db.execute("PRAGMA synchronous")
            .expect("synchronous")
            .rows()[0]
            .values(),
        &[Value::Int64(2)]
    );
    assert_eq!(
        db.execute("PRAGMA schema_version")
            .expect("schema_version")
            .rows()[0]
            .values(),
        &[Value::Int64(1)]
    );
    assert_eq!(
        db.execute("PRAGMA encoding").expect("encoding").rows()[0].values(),
        &[Value::Text("UTF-8".to_string())]
    );
    assert_eq!(
        db.execute("PRAGMA locking_mode")
            .expect("locking_mode")
            .rows()[0]
            .values(),
        &[Value::Text("normal".to_string())]
    );
    assert_eq!(
        db.execute("PRAGMA temp_store").expect("temp_store").rows()[0].values(),
        &[Value::Int64(1)]
    );
}

#[test]
fn pragma_pragma_assignment_compatibility_is_narrow() {
    let wal_async = DbConfig {
        wal_sync_mode: WalSyncMode::AsyncCommit { interval_ms: 16 },
        ..DbConfig::default()
    };
    let normal_db = Db::open_or_create(":memory:", DbConfig::default())
        .expect("open database for full-mode sync test");
    let normal_assign = normal_db.execute("PRAGMA synchronous = FULL");
    assert!(normal_assign.is_ok(), "expected sync mode match to succeed");
    let async_db =
        Db::open_or_create(":memory:", wal_async).expect("open database for normal-mode sync test");
    let async_assign = async_db.execute("PRAGMA synchronous = NORMAL");
    assert!(
        async_assign.is_ok(),
        "expected async mode sync update to succeed"
    );
    let off_reject = normal_db
        .execute("PRAGMA synchronous = OFF")
        .expect_err("unsafe sync assignment should fail");
    assert!(off_reject.to_string().contains("reopening"));

    assert!(
        normal_db
            .execute("PRAGMA foreign_keys = ON")
            .expect("foreign keys no-op")
            .affected_rows()
            == 0
    );
    assert!(
        normal_db
            .execute("PRAGMA foreign_keys = TRUE")
            .expect("foreign keys no-op")
            .affected_rows()
            == 0
    );
    let foreign_keys_off = normal_db
        .execute("PRAGMA foreign_keys = OFF")
        .expect_err("disabling foreign_keys should fail");
    assert!(foreign_keys_off
        .to_string()
        .contains("cannot disable foreign key enforcement"));

    assert_eq!(
        normal_db
            .execute("PRAGMA journal_mode = wal")
            .expect("journal_mode wal")
            .rows()[0]
            .values(),
        &[Value::Text("wal".to_string())]
    );
    let journal_mode_reject = normal_db
        .execute("PRAGMA journal_mode = OFF")
        .expect_err("unsupported journal mode should fail");
    assert!(journal_mode_reject
        .to_string()
        .contains("supports only WAL"));

    assert!(
        normal_db
            .execute("PRAGMA encoding = utf8")
            .expect("encoding utf-8")
            .affected_rows()
            == 0
    );
    let encoding_reject = normal_db
        .execute("PRAGMA encoding = latin1")
        .expect_err("non-UTF8 encoding should fail");
    assert!(encoding_reject
        .to_string()
        .contains("can only be set to UTF-8"));

    assert!(
        normal_db
            .execute("PRAGMA locking_mode = normal")
            .expect("locking mode")
            .affected_rows()
            == 0
    );
    let locking_mode_reject = normal_db
        .execute("PRAGMA locking_mode = exclusive")
        .expect_err("unsupported locking mode should fail");
    assert!(locking_mode_reject
        .to_string()
        .contains("supports only NORMAL"));

    assert!(
        normal_db
            .execute("PRAGMA temp_store = 1")
            .expect("temp_store")
            .affected_rows()
            == 0
    );
    assert!(
        normal_db
            .execute("PRAGMA temp_store = DEFAULT")
            .expect("temp_store")
            .affected_rows()
            == 0
    );
    let temp_store_reject = normal_db
        .execute("PRAGMA temp_store = memory")
        .expect_err("unsupported temp_store should fail");
    assert!(temp_store_reject.to_string().contains("not supported"));
}

#[test]
fn pragma_main_and_temp_qualifiers_are_supported() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE main_table(id INT)")
        .expect("create main table");
    db.execute("CREATE TEMP TABLE temp_table(id INT)")
        .expect("create temp table");

    assert_eq!(
        db.execute("PRAGMA main.table_info(main_table)")
            .expect("main.schema-qualified table_info")
            .rows()
            .len(),
        1
    );
    assert_eq!(
        db.execute("PRAGMA temp.table_info(temp_table)")
            .expect("temp.schema-qualified table_info")
            .rows()
            .len(),
        1
    );

    let unsupported_schema = db
        .execute("PRAGMA foo.table_info(main_table)")
        .expect_err("unsupported pragma schema qualifier");
    assert!(unsupported_schema
        .to_string()
        .contains("unsupported PRAGMA schema qualifier"));
}

#[test]
fn pragma_extended_compatibility_surface_is_implemented() -> Result<()> {
    let db = Db::open_or_create(":memory:", DbConfig::default())?;
    db.execute("CREATE TABLE parent(id INT PRIMARY KEY)")?;
    db.execute(
        "CREATE TABLE child(
                id INT PRIMARY KEY,
                parent_id INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL,
                name TEXT,
                doubled INT GENERATED ALWAYS AS (id * 2) STORED
            )",
    )?;
    db.execute("CREATE INDEX child_name_idx ON child(name) INCLUDE(parent_id)")?;

    let xinfo = db.execute("PRAGMA table_xinfo(child)")?;
    assert_eq!(
        xinfo.columns(),
        &[
            "cid".to_string(),
            "name".to_string(),
            "type".to_string(),
            "notnull".to_string(),
            "dflt_value".to_string(),
            "pk".to_string(),
            "hidden".to_string()
        ]
    );
    assert!(xinfo.rows().iter().any(|row| {
        row.values()[1] == Value::Text("doubled".to_string()) && row.values()[6] == Value::Int64(3)
    }));

    let table_list = db.execute("PRAGMA table_list")?;
    assert!(table_list.rows().iter().any(|row| {
        row.values()[0] == Value::Text("main".to_string())
            && row.values()[1] == Value::Text("child".to_string())
            && row.values()[2] == Value::Text("table".to_string())
            && row.values()[3] == Value::Int64(4)
    }));

    let index_list = db.execute("PRAGMA index_list(child)")?;
    assert!(index_list
        .rows()
        .iter()
        .any(|row| row.values()[1] == Value::Text("child_name_idx".to_string())));

    let index_xinfo = db.execute("PRAGMA index_xinfo(child_name_idx)")?;
    assert!(index_xinfo.rows().iter().any(|row| {
        row.values()[2] == Value::Text("parent_id".to_string())
            && row.values()[5] == Value::Int64(0)
    }));

    let foreign_keys = db.execute("PRAGMA foreign_key_list(child)")?;
    assert_eq!(foreign_keys.rows().len(), 1);
    assert_eq!(
        foreign_keys.rows()[0].values()[2],
        Value::Text("parent".to_string())
    );
    assert_eq!(
        foreign_keys.rows()[0].values()[6],
        Value::Text("CASCADE".to_string())
    );

    let checkpoint = db.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(
        checkpoint.columns(),
        &[
            "busy".to_string(),
            "log".to_string(),
            "checkpointed".to_string()
        ]
    );
    assert!(checkpoint.rows()[0]
        .values()
        .iter()
        .all(|value| matches!(value, Value::Int64(v) if *v >= 0)));

    assert_eq!(
        db.execute("PRAGMA busy_timeout")?.rows()[0].values(),
        &[Value::Int64(0)]
    );
    db.execute("PRAGMA busy_timeout = 1250")?;
    assert_eq!(
        db.execute("PRAGMA busy_timeout")?.rows()[0].values(),
        &[Value::Int64(1_250)]
    );
    assert_eq!(
        db.execute("PRAGMA journal_mode = WAL")?.rows()[0].values(),
        &[Value::Text("wal".to_string())]
    );
    Ok(())
}

#[test]
fn application_metadata_pragmas_are_durable_and_transactional() -> Result<()> {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("application-pragmas.ddb");
    {
        let db = Db::open_or_create(&path, DbConfig::default())?;
        assert_eq!(
            db.execute("PRAGMA user_version")?.rows()[0].values(),
            &[Value::Int64(0)]
        );
        db.execute("PRAGMA user_version = 42")?;
        db.execute("PRAGMA application_id = -7")?;
        db.execute("BEGIN")?;
        db.execute("PRAGMA user_version = 99")?;
        assert_eq!(
            db.execute("PRAGMA user_version")?.rows()[0].values(),
            &[Value::Int64(99)]
        );
        db.execute("ROLLBACK")?;
        assert_eq!(
            db.execute("PRAGMA user_version")?.rows()[0].values(),
            &[Value::Int64(42)]
        );
    }

    let reopened = Db::open_or_create(&path, DbConfig::default())?;
    assert_eq!(
        reopened.execute("PRAGMA user_version")?.rows()[0].values(),
        &[Value::Int64(42)]
    );
    assert_eq!(
        reopened.execute("PRAGMA application_id")?.rows()[0].values(),
        &[Value::Int64(-7)]
    );
    let out_of_range = reopened
        .execute("PRAGMA user_version = 2147483648")
        .expect_err("out-of-range user_version should fail");
    assert!(out_of_range.to_string().contains("signed 32-bit"));
    Ok(())
}

#[test]
fn compatibility_catalog_views_and_pragma_table_functions_work() -> Result<()> {
    let db = Db::open_or_create(":memory:", DbConfig::default())?;
    db.execute("CREATE TABLE audit(id INT)")?;
    db.execute("CREATE TABLE users(id INT PRIMARY KEY, name TEXT)")?;
    db.execute("CREATE INDEX users_name_idx ON users(name)")?;
    db.execute("CREATE VIEW user_names AS SELECT name FROM users")?;
    db.execute(
        "CREATE TRIGGER users_ai AFTER INSERT ON users
             FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit VALUES (1)')",
    )?;
    db.execute("CREATE TEMP TABLE temp_users(id INT)")?;
    db.execute("CREATE TEMP VIEW temp_user_ids AS SELECT id FROM temp_users")?;

    let schema = db.execute("SELECT type, name, tbl_name FROM sqlite_schema ORDER BY name")?;
    let schema_rows = schema
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert!(schema_rows.contains(&vec![
        Value::Text("table".to_string()),
        Value::Text("users".to_string()),
        Value::Text("users".to_string())
    ]));
    assert!(schema_rows.contains(&vec![
        Value::Text("index".to_string()),
        Value::Text("users_name_idx".to_string()),
        Value::Text("users".to_string())
    ]));
    assert!(schema_rows.contains(&vec![
        Value::Text("view".to_string()),
        Value::Text("user_names".to_string()),
        Value::Text("user_names".to_string())
    ]));
    assert!(schema_rows.contains(&vec![
        Value::Text("trigger".to_string()),
        Value::Text("users_ai".to_string()),
        Value::Text("users".to_string())
    ]));
    assert!(!schema_rows.iter().any(|row| {
        row.iter()
            .any(|value| matches!(value, Value::Text(text) if text.starts_with("__decentdb_")))
    }));

    let temp_schema = db.execute("SELECT type, name FROM temp.sqlite_schema ORDER BY name")?;
    assert!(temp_schema.rows().iter().any(|row| {
        row.values()[0] == Value::Text("table".to_string())
            && row.values()[1] == Value::Text("temp_users".to_string())
    }));
    assert!(temp_schema.rows().iter().any(|row| {
        row.values()[0] == Value::Text("view".to_string())
            && row.values()[1] == Value::Text("temp_user_ids".to_string())
    }));
    assert!(db
            .execute("INSERT INTO sqlite_schema(type, name, tbl_name, rootpage, sql) VALUES ('table', 'x', 'x', 0, NULL)")
            .is_err());

    let table_info = db.execute("SELECT name FROM pragma_table_info('users') WHERE pk = 1")?;
    assert_eq!(
        table_info.rows()[0].values(),
        &[Value::Text("id".to_string())]
    );
    let table_list =
        db.execute("SELECT name FROM pragma_table_list() WHERE schema = 'main' ORDER BY name")?;
    assert!(table_list
        .rows()
        .iter()
        .any(|row| row.values()[0] == Value::Text("users".to_string())));
    let index_info = db.execute("SELECT name FROM pragma_index_info('users_name_idx')")?;
    assert_eq!(
        index_info.rows()[0].values(),
        &[Value::Text("name".to_string())]
    );
    let databases = db.execute("SELECT name FROM pragma_database_list()")?;
    assert_eq!(
        databases.rows()[0].values(),
        &[Value::Text("main".to_string())]
    );
    Ok(())
}

#[test]
fn information_schema_views_expose_minimal_metadata() -> Result<()> {
    let db = Db::open_or_create(":memory:", DbConfig::default())?;
    db.execute("CREATE SCHEMA app")?;
    db.execute("CREATE TABLE users(id INT PRIMARY KEY, name TEXT NOT NULL)")?;
    db.execute("CREATE VIEW user_names AS SELECT name FROM users")?;
    db.execute("CREATE TEMP TABLE temp_users(id INT)")?;

    let schemata =
        db.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")?;
    let schema_names = schemata
        .rows()
        .iter()
        .map(|row| row.values()[0].clone())
        .collect::<Vec<_>>();
    assert!(schema_names.contains(&Value::Text("app".to_string())));
    assert!(schema_names.contains(&Value::Text("main".to_string())));
    assert!(schema_names.contains(&Value::Text("temp".to_string())));

    let tables = db.execute(
            "SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY table_name",
        )?;
    assert!(tables.rows().iter().any(|row| {
        row.values()
            == [
                Value::Text("main".to_string()),
                Value::Text("users".to_string()),
                Value::Text("BASE TABLE".to_string()),
            ]
    }));
    assert!(tables.rows().iter().any(|row| {
        row.values()
            == [
                Value::Text("temp".to_string()),
                Value::Text("temp_users".to_string()),
                Value::Text("LOCAL TEMPORARY".to_string()),
            ]
    }));

    let columns = db.execute(
        "SELECT column_name, ordinal_position, is_nullable, data_type
             FROM information_schema.columns
             WHERE table_name = 'users'
             ORDER BY ordinal_position",
    )?;
    assert_eq!(
        columns.rows()[0].values(),
        &[
            Value::Text("id".to_string()),
            Value::Int64(1),
            Value::Text("NO".to_string()),
            Value::Text("INT64".to_string())
        ]
    );
    assert_eq!(
        columns.rows()[1].values(),
        &[
            Value::Text("name".to_string()),
            Value::Int64(2),
            Value::Text("NO".to_string()),
            Value::Text("TEXT".to_string())
        ]
    );
    Ok(())
}

#[test]
fn generate_series_supports_required_integer_and_temporal_forms() -> Result<()> {
    let db = Db::open_or_create(":memory:", DbConfig::default())?;
    let ints = db.execute("SELECT value FROM generate_series(1, 5, 2)")?;
    assert_eq!(
        ints.rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(1), Value::Int64(3), Value::Int64(5)]
    );
    let descending = db.execute("SELECT value FROM generate_series(3, 1, -1)")?;
    assert_eq!(
        descending
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(3), Value::Int64(2), Value::Int64(1)]
    );
    assert_eq!(
        db.execute("SELECT value FROM generate_series(5, 1)")?
            .rows()
            .len(),
        0
    );
    assert!(db
        .execute("SELECT value FROM generate_series(1, 5, 0)")
        .expect_err("zero step should fail")
        .to_string()
        .contains("step cannot be zero"));
    assert!(db
        .execute("SELECT value FROM generate_series(1, 1000002)")
        .expect_err("large series should fail")
        .to_string()
        .contains("1000000"));

    let timestamps = db.execute(
        "SELECT value FROM generate_series(
                TIMESTAMP '2026-01-01 00:00:00',
                TIMESTAMP '2026-01-01 02:00:00',
                INTERVAL '1 hour'
            )",
    )?;
    assert_eq!(
        timestamps
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::TimestampMicros(1_767_225_600_000_000),
            Value::TimestampMicros(1_767_229_200_000_000),
            Value::TimestampMicros(1_767_232_800_000_000)
        ]
    );
    let dates = db.execute(
        "SELECT value FROM generate_series(
                DATE '2026-01-01',
                DATE '2026-01-03',
                INTERVAL '1 day'
            )",
    )?;
    assert_eq!(
        dates
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::DateDays(20_454),
            Value::DateDays(20_455),
            Value::DateDays(20_456)
        ]
    );

    let prepared = db.prepare("SELECT value FROM generate_series($1, $2, $3)")?;
    let prepared_rows = prepared.execute(&[Value::Int64(2), Value::Int64(6), Value::Int64(2)])?;
    assert_eq!(
        prepared_rows
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(2), Value::Int64(4), Value::Int64(6)]
    );
    Ok(())
}

#[test]
fn main_and_temp_schema_qualified_names_are_narrowly_supported() -> Result<()> {
    let db = Db::open_or_create(":memory:", DbConfig::default())?;
    db.execute("CREATE TABLE main.shadow(id INT PRIMARY KEY, note TEXT)")?;
    db.execute("CREATE TEMP TABLE temp.shadow(id INT PRIMARY KEY)")?;
    db.execute("INSERT INTO main.shadow VALUES (1, 'main')")?;
    db.execute("INSERT INTO temp.shadow VALUES (2)")?;

    assert_eq!(
        db.execute("SELECT id FROM shadow")?.rows()[0].values(),
        &[Value::Int64(2)]
    );
    assert_eq!(
        db.execute("SELECT id FROM main.shadow")?.rows()[0].values(),
        &[Value::Int64(1)]
    );
    assert_eq!(
        db.execute("SELECT id FROM temp.shadow")?.rows()[0].values(),
        &[Value::Int64(2)]
    );

    db.execute("UPDATE main.shadow SET note = 'updated' WHERE id = 1")?;
    db.execute("DELETE FROM temp.shadow WHERE id = 2")?;
    assert_eq!(
        db.execute("SELECT note FROM main.shadow")?.rows()[0].values(),
        &[Value::Text("updated".to_string())]
    );
    assert_eq!(db.execute("SELECT * FROM temp.shadow")?.rows().len(), 0);

    db.execute("CREATE VIEW main.shadow_view AS SELECT id FROM main.shadow")?;
    assert_eq!(
        db.execute("SELECT id FROM main.shadow_view")?.rows()[0].values(),
        &[Value::Int64(1)]
    );
    db.execute("CREATE TEMP VIEW temp.empty_shadow AS SELECT id FROM temp.shadow")?;
    assert_eq!(
        db.execute("SELECT * FROM temp.empty_shadow")?.rows().len(),
        0
    );
    db.execute("CREATE INDEX shadow_note_idx ON main.shadow(note)")?;
    db.execute("DROP INDEX shadow_note_idx")?;
    db.execute("ALTER TABLE main.shadow ADD COLUMN extra TEXT")?;
    db.execute("DROP VIEW main.shadow_view")?;
    db.execute("DROP TABLE main.shadow")?;
    db.execute("DROP TABLE temp.shadow")?;

    db.execute("CREATE SCHEMA app")?;
    let schema_error = db
        .execute("SELECT * FROM app.shadow")
        .expect_err("registered schema object lookup should fail");
    assert!(schema_error.to_string().contains("schema 'app'"));
    assert!(schema_error
        .to_string()
        .contains("advanced compatibility work"));
    Ok(())
}

#[test]
fn query_time_collations_and_scalar_compatibility_helpers_work() -> Result<()> {
    let db = Db::open_or_create(":memory:", DbConfig::default())?;
    db.execute("CREATE TABLE names(name TEXT)")?;
    db.execute("INSERT INTO names VALUES ('b'), ('A'), ('a'), ('a ')")?;

    let ordered = db.execute("SELECT name FROM names ORDER BY name COLLATE NOCASE")?;
    assert_eq!(
        ordered
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Text("A".to_string()),
            Value::Text("a".to_string()),
            Value::Text("a ".to_string()),
            Value::Text("b".to_string())
        ]
    );
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM names WHERE name COLLATE NOCASE = 'a'")?
            .rows()[0]
            .values(),
        &[Value::Int64(2)]
    );
    assert_eq!(
        db.execute("SELECT 'a ' COLLATE RTRIM = 'a'")?.rows()[0].values(),
        &[Value::Bool(true)]
    );
    assert!(db
        .execute("SELECT 'a' COLLATE unicode = 'A'")
        .expect_err("unsupported collation should fail")
        .to_string()
        .contains("unsupported collation"));
    assert!(db
        .execute("CREATE INDEX names_nocase_idx ON names(name COLLATE NOCASE)")
        .expect_err("persistent non-binary index collation should fail")
        .to_string()
        .contains("persistent index collations"));
    assert!(db
        .execute("SELECT DISTINCT name COLLATE NOCASE FROM names")
        .expect_err("collated distinct should fail clearly")
        .to_string()
        .contains("COLLATE in DISTINCT"));
    assert!(db
        .execute("SELECT name COLLATE NOCASE FROM names GROUP BY name COLLATE NOCASE")
        .expect_err("collated group by should fail clearly")
        .to_string()
        .contains("COLLATE in GROUP BY"));

    let helpers =
        db.execute("SELECT current_database(), current_schema(), database(), schema(), version()")?;
    assert_eq!(
        helpers.rows()[0].values()[0],
        Value::Text("main".to_string())
    );
    assert_eq!(
        helpers.rows()[0].values()[1],
        Value::Text("main".to_string())
    );
    assert_eq!(
        helpers.rows()[0].values()[2],
        Value::Text("main".to_string())
    );
    assert_eq!(
        helpers.rows()[0].values()[3],
        Value::Text("main".to_string())
    );
    assert!(
        matches!(&helpers.rows()[0].values()[4], Value::Text(version) if version.starts_with("DecentDB "))
    );
    assert!(db
        .execute("SELECT sqlite_version()")
        .expect_err("sqlite_version should not lie")
        .to_string()
        .contains("not SQLite"));
    assert!(db
        .execute("SELECT pg_backend_pid()")
        .expect_err("pg_backend_pid should not be faked")
        .to_string()
        .contains("embedded"));
    Ok(())
}

#[test]
fn prepared_insert_cache_is_scoped_by_schema_cookie() {
    let mut cache = PreparedInsertCache::with_capacity(4);
    let first = cache
        .get_or_prepare("INSERT INTO users (id) VALUES ($1)", 1, 0, || {
            Ok(Some(dummy_prepared_insert("users_v1")))
        })
        .expect("prepare first")
        .expect("prepared plan");
    let cached = cache
        .get_or_prepare("INSERT INTO users (id) VALUES ($1)", 1, 0, || {
            Ok(Some(dummy_prepared_insert("users_v1_new")))
        })
        .expect("prepare cached")
        .expect("cached plan");
    assert!(Arc::ptr_eq(&first, &cached));

    let second_schema = cache
        .get_or_prepare("INSERT INTO users (id) VALUES ($1)", 2, 0, || {
            Ok(Some(dummy_prepared_insert("users_v2")))
        })
        .expect("prepare second schema")
        .expect("prepared plan");
    assert!(!Arc::ptr_eq(&first, &second_schema));
    assert_eq!(second_schema.table_name, "users_v2");
}

#[test]
fn default_deferred_materialization_keeps_prepared_insert_fast_path_enabled() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    assert!(
        db.can_use_autocommit_prepared_insert_fast_path("t")
            .expect("prepared insert fast path check"),
        "default deferred materialization must not disable prepared inserts"
    );
}

#[test]
fn shared_transaction_prepared_insert_refreshes_generic_cached_plan_once() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("shared-txn-prepared-insert-refresh.ddb");
    let config = DbConfig::default();

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE bench (id INTEGER, val TEXT, f FLOAT64)")
            .expect("create bench table");
        db.execute("CREATE INDEX bench_id_idx ON bench(id)")
            .expect("create bench index");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction().expect("begin shared transaction");
    let mut prepared = db
        .prepare("INSERT INTO bench VALUES ($1, $2, $3)")
        .expect("prepare insert");
    let cached_plan = prepared
        .prepared_insert
        .as_ref()
        .expect("prepared insert plan");
    let mut generic_plan = (**cached_plan).clone();
    generic_plan.use_generic_index_updates = true;
    prepared.prepared_insert = Some(Arc::new(generic_plan));

    prepared
        .execute(&[
            Value::Int64(1),
            Value::Text("value-1".to_string()),
            Value::Float64(1.0),
        ])
        .expect("execute prepared insert in shared transaction");

    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        let cached_runtime_plan = state
            .prepared_insert_runtime_cache
            .get(&Db::prepared_statement_cache_key(&prepared))
            .expect("refreshed runtime insert plan");
        assert!(
                !cached_runtime_plan.use_generic_index_updates,
                "transaction runtime should cache a specialized insert plan once row sources are loaded"
            );
    }

    prepared
        .execute(&[
            Value::Int64(2),
            Value::Text("value-2".to_string()),
            Value::Float64(2.0),
        ])
        .expect("reuse prepared insert in shared transaction");
    db.commit_transaction().expect("commit shared transaction");

    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM bench")
                .expect("count committed rows")
        ),
        2
    );
}

#[test]
fn shared_transaction_prepared_insert_caches_loaded_runtime_plan() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");

    db.begin_transaction().expect("begin shared transaction");
    let prepared = db
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .expect("prepare insert");

    prepared
        .execute(&[Value::Int64(1), Value::Text("value-1".to_string())])
        .expect("execute first prepared insert");

    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        let cached_runtime_plan = state
            .prepared_insert_runtime_cache
            .get(&Db::prepared_statement_cache_key(&prepared))
            .expect("cached runtime insert plan");
        let prepared_plan = prepared
            .prepared_insert
            .as_ref()
            .expect("prepared insert plan");
        assert!(
                Arc::ptr_eq(cached_runtime_plan, prepared_plan),
                "shared transaction should retain the reusable prepared insert plan in the runtime cache"
            );
    }

    prepared
        .execute(&[Value::Int64(2), Value::Text("value-2".to_string())])
        .expect("execute second prepared insert");
    db.commit_transaction().expect("commit shared transaction");

    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
        2
    );
}

#[test]
fn exclusive_transaction_commit_persists_prepared_writes() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");

    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .expect("prepare insert");
    for i in 0_i64..32_i64 {
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(i), Value::Text(format!("value-{i}"))],
            )
            .expect("insert row");
    }
    txn.commit().expect("commit txn");

    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
        32
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT id FROM t WHERE val = 'value-17'")
                .expect("lookup committed row")
        ),
        17
    );
}

#[test]
fn exclusive_transaction_execute_in_mut_reuses_buffered_positional_params() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");

    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .expect("prepare insert");
    let mut params = vec![Value::Int64(1), Value::Text("value-1".to_string())];
    insert
        .execute_in_mut(&mut txn, &mut params)
        .expect("insert row with mutable params");
    params[0] = Value::Int64(2);
    params[1] = Value::Text("value-2".to_string());
    insert
        .execute_in_mut(&mut txn, &mut params)
        .expect("insert second row with mutable params");
    txn.commit().expect("commit txn");

    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
        2
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM t WHERE val IN ('value-1', 'value-2')")
                .expect("lookup committed rows")
        ),
        2
    );
}

#[test]
fn exclusive_transaction_prepared_batch_reuses_insert_plan() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");

    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .expect("prepare insert");
    let mut params = vec![Value::Int64(0), Value::Text(String::new())];
    {
        let mut batch = txn
            .prepared_batch(&insert, params.len())
            .expect("prepare batch");
        for row_id in 1..=4 {
            params[0] = Value::Int64(row_id);
            params[1] = Value::Text(format!("value-{row_id}"));
            assert_eq!(batch.execute_mut(&mut params).expect("insert row"), 1);
        }
    }
    txn.commit().expect("commit txn");

    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
        4
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT id FROM t WHERE val = 'value-3'")
                .expect("lookup committed row")
        ),
        3
    );
}

#[test]
fn autocommit_execute_mut_reuses_buffered_positional_params() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");
    let insert = db
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .expect("prepare insert");
    let mut params = vec![Value::Int64(1), Value::Text("value-1".to_string())];
    insert
        .execute_mut(&mut params)
        .expect("insert row with mutable params");
    params[0] = Value::Int64(2);
    params[1] = Value::Text("value-2".to_string());
    insert
        .execute_mut(&mut params)
        .expect("insert second row with mutable params");

    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
        2
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM t WHERE val IN ('value-1', 'value-2')")
                .expect("lookup committed rows")
        ),
        2
    );
}

#[test]
fn exclusive_transaction_rollback_discards_persistent_changes() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");
    db.execute("INSERT INTO t VALUES (1, 'seed')")
        .expect("seed row");

    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .expect("prepare insert");
    insert
        .execute_in(
            &mut txn,
            &[Value::Int64(2), Value::Text("transient".to_string())],
        )
        .expect("insert transient row");
    txn.rollback().expect("rollback txn");

    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
        1
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM t WHERE id = 2")
                .expect("count rolled back row")
        ),
        0
    );
}

#[test]
fn rebuild_indexes_persists_fresh_metadata_for_deferred_paged_tables() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("rebuild-indexes-fresh-metadata.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, name TEXT)")
        .expect("create table");
    db.execute("CREATE INDEX docs_name_idx ON docs(name)")
        .expect("create index");
    let mut txn = db.transaction().expect("begin seed txn");
    let insert = txn
        .prepare("INSERT INTO docs VALUES ($1, $2)")
        .expect("prepare seed insert");
    let large_name = "x".repeat(2048);
    for i in 0_i64..40_i64 {
        insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(i + 1),
                    Value::Text(format!("name-{i}-{large_name}")),
                ],
            )
            .expect("insert row");
    }
    txn.commit().expect("commit seed rows");
    let mut stale_runtime = db
        .runtime_for_metadata_inspection()
        .expect("runtime for stale setup");
    {
        let catalog = Arc::make_mut(&mut stale_runtime.catalog);
        catalog.schema_cookie = catalog.schema_cookie.wrapping_add(1);
        for index in catalog.indexes.values_mut() {
            index.fresh = false;
        }
    }
    db.persist_runtime_if_latest(stale_runtime, None, false)
        .expect("persist stale index metadata");

    let before = db.list_indexes().expect("list indexes before rebuild");
    assert!(
        before.iter().any(|index| !index.fresh),
        "test setup should leave at least one stale index: {before:?}"
    );

    db.rebuild_indexes().expect("rebuild indexes");

    let after = db.list_indexes().expect("list indexes after rebuild");
    assert!(
        after.iter().all(|index| index.fresh),
        "rebuild_indexes should mark every index fresh: {after:?}"
    );
    drop(db);

    let reopened = Db::open(&path, DbConfig::default()).expect("reopen db");
    let reopened_indexes = reopened.list_indexes().expect("list reopened indexes");
    assert!(
        reopened_indexes.iter().all(|index| index.fresh),
        "fresh index metadata should survive reopen: {reopened_indexes:?}"
    );
}

#[test]
fn checkpoint_compacts_large_persisted_payloads() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("checkpoint-compacts-large-payloads.ddb");
    let db = Db::open_or_create(
        &path,
        DbConfig {
            persistent_pk_index: false,
            paged_row_storage: false,
            ..DbConfig::default()
        },
    )
    .expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs table");

    let large_body = "x".repeat(2048);
    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO docs VALUES ($1, $2)")
        .expect("prepare insert");
    for i in 0_i64..96_i64 {
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(i), Value::Text(large_body.clone())],
            )
            .expect("insert large row");
    }
    txn.commit().expect("commit rows");

    let runtime_before = db
        .runtime_for_metadata_inspection()
        .expect("runtime before checkpoint");
    let docs_before = runtime_before
        .persisted_tables
        .get("docs")
        .expect("persisted docs table before checkpoint");
    assert!(
        docs_before.pointer.logical_len >= 64 * 1024,
        "test setup did not create a large enough payload"
    );
    assert!(
        !docs_before.pointer.is_compressed(),
        "normal commits should leave table payloads uncompressed"
    );

    db.checkpoint().expect("checkpoint");

    let runtime_after = db
        .runtime_for_metadata_inspection()
        .expect("runtime after checkpoint");
    let docs_after = runtime_after
        .persisted_tables
        .get("docs")
        .expect("persisted docs table after checkpoint");
    assert!(
        docs_after.pointer.is_compressed(),
        "checkpoint should compact large persisted payloads"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        96
    );
}

#[test]
fn checkpoint_wal_flushes_without_compacting_large_persisted_payloads() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("checkpoint-wal-skips-large-payload-compaction.ddb");
    let db = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: false,
            ..DbConfig::default()
        },
    )
    .expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs table");

    let large_body = "x".repeat(2048);
    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO docs VALUES ($1, $2)")
        .expect("prepare insert");
    for i in 0_i64..96_i64 {
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(i), Value::Text(large_body.clone())],
            )
            .expect("insert large row");
    }
    txn.commit().expect("commit rows");

    let runtime_before = db
        .runtime_for_metadata_inspection()
        .expect("runtime before checkpoint");
    let docs_before = runtime_before
        .persisted_tables
        .get("docs")
        .expect("persisted docs table before checkpoint");
    assert!(
        docs_before.pointer.logical_len >= 64 * 1024,
        "test setup did not create a large enough payload"
    );
    assert!(
        !docs_before.pointer.is_compressed(),
        "normal commits should leave table payloads uncompressed"
    );

    db.checkpoint_wal().expect("checkpoint wal");

    let storage = db.storage_info().expect("storage info");
    assert_eq!(storage.wal_end_lsn, 0);
    let runtime_after = db
        .runtime_for_metadata_inspection()
        .expect("runtime after checkpoint");
    let docs_after = runtime_after
        .persisted_tables
        .get("docs")
        .expect("persisted docs table after checkpoint");
    assert!(
        !docs_after.pointer.is_compressed(),
        "checkpoint_wal should flush WAL without compacting large payloads"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        96
    );
}

#[test]
fn save_as_flushes_wal_without_compacting_source_payloads() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("save-as-skips-large-payload-compaction.ddb");
    let snapshot_path = tempdir.path().join("snapshot.ddb");
    let db = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: false,
            ..DbConfig::default()
        },
    )
    .expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs table");

    let large_body = "x".repeat(2048);
    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO docs VALUES ($1, $2)")
        .expect("prepare insert");
    for i in 0_i64..96_i64 {
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(i), Value::Text(large_body.clone())],
            )
            .expect("insert large row");
    }
    txn.commit().expect("commit rows");

    db.save_as(&snapshot_path).expect("save as");

    let storage = db.storage_info().expect("source storage info");
    assert_eq!(storage.wal_end_lsn, 0);
    let runtime_after = db
        .runtime_for_metadata_inspection()
        .expect("runtime after save_as");
    let docs_after = runtime_after
        .persisted_tables
        .get("docs")
        .expect("persisted docs table after save_as");
    assert!(
        !docs_after.pointer.is_compressed(),
        "save_as should flush WAL without compacting the source payload"
    );

    let snapshot = Db::open(&snapshot_path, DbConfig::default()).expect("open snapshot");
    assert_eq!(
        scalar_i64(
            &snapshot
                .execute("SELECT COUNT(*) FROM docs")
                .expect("count snapshot docs")
        ),
        96
    );
}

#[test]
fn save_as_file_copy_preserves_checkpointed_snapshot_bytes() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("save-as-file-copy-source.ddb");
    let snapshot_path = tempdir.path().join("save-as-file-copy-snapshot.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs table");
    for id in 1..=32 {
        db.execute(&format!(
            "INSERT INTO docs (id, body) VALUES ({id}, 'body-{id}')"
        ))
        .expect("insert row");
    }

    db.checkpoint_wal().expect("checkpoint wal");
    let before = db.storage_info().expect("storage before save_as");
    let source_bytes_before = std::fs::read(&path).expect("read source bytes before save_as");

    db.save_as(&snapshot_path).expect("save as");

    let after = db.storage_info().expect("storage after save_as");
    assert_eq!(
        before.wal_end_lsn, after.wal_end_lsn,
        "save_as should not checkpoint when already checkpointed"
    );
    assert_eq!(
        before.wal_file_size, after.wal_file_size,
        "save_as should not modify WAL when already checkpointed"
    );

    assert_eq!(
        std::fs::read(&path).expect("read source bytes after save_as"),
        source_bytes_before,
        "save_as should not mutate a checkpointed source database file"
    );
    assert_eq!(
        std::fs::read(&snapshot_path).expect("read snapshot bytes"),
        source_bytes_before
    );
    let snapshot = Db::open(&snapshot_path, DbConfig::default()).expect("open snapshot");
    assert_eq!(
        scalar_i64(
            &snapshot
                .execute("SELECT COUNT(*) FROM docs")
                .expect("count snapshot docs")
        ),
        32
    );
}

#[test]
fn bulk_load_checkpoint_flushes_wal_without_compacting_payloads() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("bulk-load-checkpoint-skips-large-payload-compaction.ddb");
    let db = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: false,
            ..DbConfig::default()
        },
    )
    .expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs table");

    let large_body = "x".repeat(2048);
    let rows = (0_i64..96_i64)
        .map(|i| vec![Value::Int64(i), Value::Text(large_body.clone())])
        .collect::<Vec<_>>();

    let inserted = db
        .bulk_load_rows(
            "docs",
            &["id", "body"],
            &rows,
            BulkLoadOptions {
                batch_size: 16,
                checkpoint_on_complete: true,
                ..BulkLoadOptions::default()
            },
        )
        .expect("bulk load rows");
    assert_eq!(inserted, 96);

    let storage = db.storage_info().expect("storage info after bulk load");
    assert_eq!(storage.wal_end_lsn, 0);
    let runtime_after = db
        .runtime_for_metadata_inspection()
        .expect("runtime after bulk load");
    let docs_after = runtime_after
        .persisted_tables
        .get("docs")
        .expect("persisted docs table after bulk load");
    assert!(
        !docs_after.pointer.is_compressed(),
        "bulk-load completion checkpoint should flush WAL without compacting large payloads"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        96
    );
}

#[test]
fn checkpoint_preserves_large_payloads_with_persistent_pk_index() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("checkpoint-keeps-pk-payloads.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: false,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, config).expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs table");

    let large_body = "x".repeat(2048);
    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO docs VALUES ($1, $2)")
        .expect("prepare insert");
    for i in 0_i64..96_i64 {
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(i), Value::Text(large_body.clone())],
            )
            .expect("insert large row");
    }
    txn.commit().expect("commit rows");

    let runtime_before = db
        .runtime_for_metadata_inspection()
        .expect("runtime before checkpoint");
    let docs_before = runtime_before
        .persisted_tables
        .get("docs")
        .expect("persisted docs before checkpoint");
    assert!(
        docs_before.pointer.logical_len >= 64 * 1024,
        "test setup did not create a large enough payload"
    );
    assert!(
        !docs_before.pointer.is_compressed(),
        "persistent pk writes should keep payload uncompressed"
    );
    assert!(
        docs_before.pk_index_root.is_some(),
        "persistent pk writes should record a locator tree root"
    );

    db.checkpoint().expect("checkpoint");

    let runtime_after = db
        .runtime_for_metadata_inspection()
        .expect("runtime after checkpoint");
    let docs_after = runtime_after
        .persisted_tables
        .get("docs")
        .expect("persisted docs after checkpoint");
    assert!(
        !docs_after.pointer.is_compressed(),
        "checkpoint should not compact payloads with persistent pk roots"
    );
    assert!(
        docs_after.pk_index_root.is_some(),
        "checkpoint should preserve the persistent pk locator tree"
    );
}

#[test]
fn checkpoint_keeps_deferred_paged_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("checkpoint-keeps-deferred-paged-tables-unloaded.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i), Value::Text(large_body.clone())],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );

    db.checkpoint().expect("checkpoint");

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after checkpoint");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected checkpoint compaction to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected checkpoint compaction to keep paged-backed table deferred, got: {json_after}"
    );
}

#[test]
fn refresh_engine_from_snapshot_reloads_when_reader_uses_older_lsn() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("refresh-engine-from-older-reader-snapshot.ddb");
    let config = DbConfig {
        defer_table_materialization: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, config).expect("open db");

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");
    let older_pointer = {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        runtime
            .persisted_tables
            .get("t")
            .expect("empty persisted table state")
            .pointer
    };
    assert_eq!(
        older_pointer.head_page_id, 0,
        "test setup should start with an empty persisted table"
    );

    let reader = db
        .inner
        .wal
        .begin_reader_with_pager(&db.inner.pager)
        .expect("begin reader");
    let older_snapshot_lsn = reader.snapshot_lsn();

    let insert = db
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .expect("prepare newer row insert");
    insert
        .execute(&[Value::Int64(1), Value::Text("x".repeat(70_000))])
        .expect("insert newer row");
    let newer_snapshot_lsn = db.inner.wal.latest_snapshot();
    assert!(
        newer_snapshot_lsn > older_snapshot_lsn,
        "test setup should advance the WAL while the reader holds the older snapshot"
    );
    assert_eq!(
        db.inner.last_runtime_lsn.load(AtomicOrdering::Acquire),
        newer_snapshot_lsn
    );
    let newer_pointer = {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        runtime
            .persisted_tables
            .get("t")
            .expect("newer persisted table state")
            .pointer
    };
    assert_ne!(
        newer_pointer, older_pointer,
        "test setup should persist a newer table pointer"
    );
    assert!(
        newer_pointer.is_table_paged_manifest(),
        "test should exercise deferred paged table metadata"
    );

    db.refresh_engine_from_snapshot(older_snapshot_lsn)
        .expect("refresh from older reader snapshot");

    assert_eq!(
        db.inner.last_runtime_lsn.load(AtomicOrdering::Acquire),
        older_snapshot_lsn
    );
    let runtime = db.inner.engine.read().expect("engine runtime lock");
    let state = runtime
        .persisted_tables
        .get("t")
        .expect("persisted table state");
    assert_eq!(
        state.pointer, older_pointer,
        "runtime table pointer must match the snapshot LSN, not the newer writer commit"
    );
    assert_ne!(
        state.pointer, newer_pointer,
        "runtime table pointer should not reuse newer writer metadata for an older snapshot"
    );
    drop(runtime);
    drop(reader);
}

#[test]
fn deferred_paged_secondary_index_point_lookup_stays_indexed_and_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-paged-secondary-index-point-lookup.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, config).expect("open db");
    db.execute("CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)")
        .expect("create bench");
    db.execute("CREATE INDEX bench_id_idx ON bench(id)")
        .expect("create id index");
    let mut txn = db.transaction().expect("begin txn");
    let insert = txn
        .prepare("INSERT INTO bench VALUES ($1, $2, $3)")
        .expect("prepare insert");
    let large_body = "x".repeat(2048);
    for i in 0_i64..128_i64 {
        insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(i),
                    Value::Text(format!("value_{i}_{large_body}")),
                    Value::Float64(i as f64),
                ],
            )
            .expect("insert row");
    }
    txn.commit().expect("commit rows");

    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        let index = runtime
            .catalog
            .index("bench_id_idx")
            .expect("catalog index after redefer");
        assert!(
            index.fresh,
            "secondary index should stay fresh after redefer"
        );
        assert!(
            matches!(
                runtime.index("bench_id_idx"),
                Some(RuntimeIndex::Btree { .. })
            ),
            "runtime btree index should remain available for deferred lookups"
        );
        assert!(
            runtime
                .deferred_table_names()
                .any(|name| name.eq_ignore_ascii_case("bench")),
            "bench should be deferred after commit"
        );
    }

    let result = db
        .execute("SELECT id, val, f FROM bench WHERE id = 42")
        .expect("indexed point lookup");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(42),
            Value::Text(format!("value_42_{large_body}")),
            Value::Float64(42.0),
        ]
    );
    let json_after = db
        .inspect_storage_state_json()
        .expect("json after indexed lookup");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "indexed deferred lookup should not materialize the table, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "indexed deferred lookup should keep the table deferred, got: {json_after}"
    );
}

#[test]
fn reopen_deferred_paged_secondary_index_lookup_hydrates_runtime_btree_index() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("reopened-deferred-paged-secondary-index-lookup.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        defer_table_materialization: true,
        ..DbConfig::default()
    };
    let large_body = "x".repeat(2048);

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE bench (id INT64 PRIMARY KEY, lookup TEXT, body TEXT)")
            .expect("create bench");
        db.execute("CREATE INDEX bench_lookup_idx ON bench(lookup)")
            .expect("create lookup index");
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO bench VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 1_i64..=128_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Text(format!("lookup-{i}")),
                        Value::Text(format!("body-{i}-{large_body}")),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint before reopen");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        assert!(
            runtime
                .deferred_table_names()
                .any(|name| name.eq_ignore_ascii_case("bench")),
            "bench should be deferred immediately after reopen"
        );
        assert!(
            runtime.index("bench_lookup_idx").is_none(),
            "reopen should stay lazy and avoid hydrating secondary btree indexes"
        );
    }

    let result = db
        .execute("SELECT id, lookup FROM bench WHERE lookup = 'lookup-127' ORDER BY id LIMIT 1")
        .expect("indexed point lookup after reopen");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(127), Value::Text("lookup-127".to_string()),]
    );

    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        assert!(
            matches!(
                runtime.index("bench_lookup_idx"),
                Some(RuntimeIndex::Btree { .. })
            ),
            "lookup should hydrate the runtime btree map for subsequent reads"
        );
        assert!(
            runtime
                .deferred_table_names()
                .any(|name| name.eq_ignore_ascii_case("bench")),
            "hydration should leave the paged table deferred"
        );
    }
    let json_after = db
        .inspect_storage_state_json()
        .expect("json after indexed lookup");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "hydration should not keep the paged row source loaded, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "hydration should not materialize rows into memory, got: {json_after}"
    );
}

#[test]
fn checkpoint_compacts_paged_table_chunks_and_preserves_persistent_pk_index() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("checkpoint-compacts-paged-table.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, config).expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs table");

    let large_body = "x".repeat(2048);
    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO docs VALUES ($1, $2)")
        .expect("prepare insert");
    for i in 0_i64..96_i64 {
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(i), Value::Text(large_body.clone())],
            )
            .expect("insert large row");
    }
    txn.commit().expect("commit rows");

    let runtime_before = db
        .runtime_for_metadata_inspection()
        .expect("runtime before checkpoint");
    let docs_before = runtime_before
        .persisted_tables
        .get("docs")
        .expect("persisted docs before checkpoint");
    assert!(
        docs_before.pointer.is_table_paged_manifest(),
        "paged row storage should persist docs through a paged manifest"
    );
    assert!(
        docs_before.pk_index_root.is_some(),
        "paged row storage should preserve the persistent pk locator tree"
    );
    let page_store = PagerReadStore { db: &db };
    let manifest_before = read_overflow(&page_store, docs_before.pointer)
        .expect("read paged manifest before checkpoint");
    let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
        .expect("decode paged manifest before checkpoint");
    assert!(
        manifest_before
            .chunks
            .iter()
            .any(|chunk| !chunk.pointer.is_compressed()),
        "normal paged writes should leave chunk payloads uncompressed"
    );

    db.checkpoint().expect("checkpoint");

    let runtime_after = db
        .runtime_for_metadata_inspection()
        .expect("runtime after checkpoint");
    let docs_after = runtime_after
        .persisted_tables
        .get("docs")
        .expect("persisted docs after checkpoint");
    assert!(
        docs_after.pointer.is_table_paged_manifest(),
        "checkpoint should preserve paged-table state"
    );
    assert!(
        docs_after.pk_index_root.is_some(),
        "checkpoint should preserve the persistent pk locator tree"
    );
    let manifest_after = read_overflow(&page_store, docs_after.pointer)
        .expect("read paged manifest after checkpoint");
    let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
        .expect("decode paged manifest after checkpoint");
    assert!(
        manifest_after
            .chunks
            .iter()
            .any(|chunk| chunk.pointer.is_compressed()),
        "checkpoint should compact large paged chunk payloads"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows after checkpoint")
        ),
        96
    );
    assert_eq!(
        scalar_text(
            &db.execute("SELECT body FROM docs WHERE id = 17")
                .expect("select row after checkpoint")
        ),
        large_body
    );
}

#[test]
fn persistent_pk_index_backfills_compressed_tables_and_keeps_point_lookup_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("persistent-pk-backfill.ddb");

    {
        let db = Db::open_or_create(
            &path,
            DbConfig {
                paged_row_storage: false,
                persistent_pk_index: false,
                ..DbConfig::default()
            },
        )
        .expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");

        let runtime = db
            .runtime_for_metadata_inspection()
            .expect("runtime after checkpoint");
        let seeded = runtime
            .persisted_tables
            .get("seeded")
            .expect("persisted seeded after checkpoint");
        assert!(
            seeded.pointer.is_compressed(),
            "legacy checkpoint should compact the large payload before backfill"
        );
        assert!(
            seeded.pk_index_root.is_none(),
            "legacy checkpoint should not have a persistent pk root"
        );
    }

    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: false,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, config).expect("reopen with persistent pk index");
    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        let seeded = runtime
            .persisted_tables
            .get("seeded")
            .expect("persisted seeded before backfill");
        assert!(
            seeded.pointer.is_compressed(),
            "open should not backfill every table eagerly"
        );
        assert!(
            seeded.pk_index_root.is_none(),
            "open should leave the missing persistent pk locator for on-demand backfill"
        );
    }

    assert_eq!(
        scalar_text(
            &db.execute("SELECT body FROM seeded WHERE id = 17")
                .expect("select row after targeted backfill")
        ),
        "x".repeat(2048)
    );
    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        let seeded = runtime
            .persisted_tables
            .get("seeded")
            .expect("persisted seeded after backfill");
        assert!(
            !seeded.pointer.is_compressed(),
            "targeted backfill should rewrite the large payload uncompressed"
        );
        assert!(
            seeded.pk_index_root.is_some(),
            "targeted backfill should attach a persistent pk locator tree"
        );
        assert!(
            runtime
                .catalog
                .tables
                .get("seeded")
                .and_then(|table| table.pk_index_root)
                .is_some(),
            "catalog state should retain the persistent pk locator root"
        );
    }

    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected backfilled table to stay deferred at open, got: {json_open}"
    );

    let result = db
        .execute("SELECT n FROM seeded WHERE id = 17")
        .expect("point lookup");
    assert_eq!(scalar_i64(&result), 17);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after point lookup");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected persistent pk point lookup to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected seeded to remain deferred after point lookup, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after persistent pk point lookup, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_backfills_legacy_tables_and_keeps_wildcard_scan_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-backfill.ddb");

    {
        let legacy_config = DbConfig {
            paged_row_storage: false,
            ..DbConfig::default()
        };
        let db = Db::open_or_create(&path, legacy_config).expect("create legacy db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        let seeded = runtime
            .persisted_tables
            .get("seeded")
            .expect("legacy persisted seeded");
        assert!(
            !seeded.pointer.is_table_paged_manifest(),
            "expected legacy setup to use single-payload table storage"
        );
    }

    let (pointer_after_backfill, checksum_after_backfill) = {
        let db = Db::open_or_create(&path, DbConfig::default())
            .expect("reopen legacy db with default paged storage");
        let state_after_backfill = {
            let runtime = db.inner.engine.read().expect("engine runtime lock");
            let seeded = runtime
                .persisted_tables
                .get("seeded")
                .expect("persisted seeded after paged backfill");
            assert!(
                seeded.pointer.is_table_paged_manifest(),
                "expected legacy table to be wrapped in paged manifest storage"
            );
            *seeded
        };

        let json_open = db.inspect_storage_state_json().expect("json snapshot");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at open, got: {json_open}"
        );

        let result = db.execute("SELECT * FROM seeded").expect("wildcard scan");
        assert_eq!(result.rows().len(), 96);
        assert_eq!(
            result.rows()[0].values(),
            &[
                Value::Int64(0),
                Value::Int64(0),
                Value::Text("x".repeat(2048)),
            ]
        );

        let json_after = db.inspect_storage_state_json().expect("json after scan");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged wildcard scan to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after scan, got: {json_after}"
        );

        (state_after_backfill.pointer, state_after_backfill.checksum)
    };

    let db = Db::open_or_create(&path, DbConfig::default())
        .expect("reopen already-backfilled db with default paged storage");
    let runtime = db.inner.engine.read().expect("engine runtime lock");
    let state_after_reopen = *runtime
        .persisted_tables
        .get("seeded")
        .expect("persisted seeded after second reopen");
    assert_eq!(
        state_after_reopen.pointer, pointer_after_backfill,
        "expected second reopen to keep the existing paged manifest pointer"
    );
    assert_eq!(
        state_after_reopen.checksum, checksum_after_backfill,
        "expected second reopen to keep the existing paged manifest checksum"
    );
}

#[test]
fn paged_row_storage_persists_new_large_tables_and_keeps_wildcard_scan_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-new-write.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        let seeded = runtime
            .persisted_tables
            .get("seeded")
            .expect("persisted seeded after paged write");
        assert!(
            seeded.pointer.is_table_paged_manifest(),
            "expected new large table to persist behind paged manifest storage"
        );
    }

    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );

    let result = db.execute("SELECT * FROM seeded").expect("wildcard scan");
    assert_eq!(result.rows().len(), 96);
    assert_eq!(
        result.rows()[95].values(),
        &[
            Value::Int64(95),
            Value::Int64(95),
            Value::Text("x".repeat(2048)),
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after scan");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged wildcard scan to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to remain deferred after scan, got: {json_after}"
    );
}

#[test]
fn single_row_insert_with_default_deferred_loading_completes() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("single-row-insert-default-deferred.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t1(a INT64)")
        .expect("create table");
    db.execute("INSERT INTO t1 VALUES (1)")
        .expect("single-row insert");
    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t1").expect("count rows")),
        1
    );
}

#[test]
fn metadata_inspection_keeps_deferred_paged_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("metadata-inspection-keeps-deferred-paged-tables-unloaded.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );

    let tables = db.list_tables().expect("list tables");
    let seeded = tables
        .iter()
        .find(|table| table.name == "seeded")
        .expect("seeded table metadata");
    assert_eq!(seeded.row_count, 96);

    let described = db.describe_table("seeded").expect("describe seeded");
    assert_eq!(described.row_count, 96);

    let snapshot = db.get_schema_snapshot().expect("schema snapshot");
    let seeded_snapshot = snapshot
        .tables
        .iter()
        .find(|table| table.name == "seeded")
        .expect("seeded table in snapshot");
    assert_eq!(seeded_snapshot.row_count, 96);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after metadata");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected metadata inspection to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected metadata inspection to keep paged-backed table deferred, got: {json_after}"
    );
}

#[test]
fn single_index_admin_paths_keep_deferred_paged_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("single-index-admin-paths-keep-deferred-paged-tables-unloaded.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_docs_n ON docs(n)")
            .expect("create docs index");
        let large_body = "x".repeat(2048);
        let large_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(large_note.clone()),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit rows");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed tables to stay deferred at open, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":2"),
        "expected both paged-backed tables deferred at open, got: {json_open}"
    );

    let verification = db.verify_index("idx_docs_n").expect("verify index");
    assert!(verification.valid);
    assert_eq!(verification.expected_entries, 96);
    assert_eq!(verification.actual_entries, 96);

    let json_after_verify = db.inspect_storage_state_json().expect("json after verify");
    assert!(
        json_after_verify.contains("\"loaded_table_count\":0"),
        "expected verify_index to avoid live materialization, got: {json_after_verify}"
    );
    assert!(
        json_after_verify.contains("\"deferred_table_count\":2"),
        "expected verify_index to keep both paged-backed tables deferred, got: {json_after_verify}"
    );

    db.rebuild_index("idx_docs_n").expect("rebuild index");

    let json_after_rebuild = db.inspect_storage_state_json().expect("json after rebuild");
    assert!(
        json_after_rebuild.contains("\"loaded_table_count\":0"),
        "expected rebuild_index to avoid live materialization, got: {json_after_rebuild}"
    );
    assert!(
            json_after_rebuild.contains("\"deferred_table_count\":2"),
            "expected rebuild_index to keep both paged-backed tables deferred, got: {json_after_rebuild}"
        );

    db.rebuild_indexes().expect("rebuild all indexes");

    let json_after_rebuild_all = db
        .inspect_storage_state_json()
        .expect("json after rebuild all");
    assert!(
        json_after_rebuild_all.contains("\"loaded_table_count\":0"),
        "expected rebuild_indexes to avoid live materialization, got: {json_after_rebuild_all}"
    );
    assert!(
            json_after_rebuild_all.contains("\"deferred_table_count\":2"),
            "expected rebuild_indexes to keep both paged-backed tables deferred, got: {json_after_rebuild_all}"
        );
}

#[test]
fn paged_row_storage_wildcard_ordered_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-wildcard-ordered-projection.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..48_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT * FROM seeded ORDER BY n DESC LIMIT 2 OFFSET 1")
        .expect("ordered wildcard projection");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(46),
            Value::Int64(46),
            Value::Text("x".repeat(2048))
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(45),
            Value::Int64(45),
            Value::Text("x".repeat(2048))
        ]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered wildcard projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged ordered wildcard projection to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after ordered wildcard projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged ordered wildcard projection, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_filtered_projection_with_offset_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-filtered-projection-offset.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..48_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT n FROM seeded WHERE n >= 10 AND n <= 20 ORDER BY n DESC LIMIT 2 OFFSET 1")
        .expect("paged ordered filtered projection with offset");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(19)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(18)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after paged ordered filtered projection with offset");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected paged ordered filtered projection with offset to avoid materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after ordered filtered projection with offset, got: {json_after}"
        );
    assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after paged ordered filtered projection with offset, got: {json_after}"
        );
}

#[test]
fn persistent_pk_index_keeps_paged_row_storage_point_lookup_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-pk-lookup.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage + pk index");
    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        let seeded = runtime
            .persisted_tables
            .get("seeded")
            .expect("persisted seeded after paged write");
        assert!(
            seeded.pointer.is_table_paged_manifest(),
            "expected new large table to persist behind paged manifest storage"
        );
        assert!(
            seeded.pk_index_root.is_some(),
            "expected paged-backed table to retain a persistent pk locator root"
        );
    }

    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );

    let result = db
        .execute("SELECT n FROM seeded WHERE id = 95")
        .expect("point lookup");
    assert_eq!(scalar_i64(&result), 95);

    let json_after = db.inspect_storage_state_json().expect("json after lookup");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged point lookup to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to remain deferred after point lookup, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged point lookup, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_updates_and_deletes_preserve_untouched_chunks() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-update-delete.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };

    let untouched_chunk_pointers = {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");

        let untouched_chunk_pointers = {
            let runtime_before = db.inner.engine.read().expect("engine runtime lock");
            let docs_before = runtime_before
                .persisted_tables
                .get("docs")
                .expect("persisted docs before mutation");
            let page_store = PagerReadStore { db: &db };
            let manifest_before = read_overflow(&page_store, docs_before.pointer)
                .expect("read paged manifest before mutation");
            let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                .expect("decode paged manifest before mutation");
            assert!(
                manifest_before.chunks.len() > 2,
                "expected multiple chunks before mutation"
            );
            manifest_before
                .chunks
                .iter()
                .skip(1)
                .map(|chunk| chunk.pointer)
                .collect::<Vec<_>>()
        };

        db.execute(&format!(
            "UPDATE docs SET n = 500, body = '{}' WHERE id = 6",
            "y".repeat(2600)
        ))
        .expect("update docs row");
        db.execute("DELETE FROM docs WHERE id = 7")
            .expect("delete docs row");

        untouched_chunk_pointers
    };

    let db = Db::open_or_create(&path, config).expect("reopen mutated db");
    let preserved_untouched = {
        let runtime_after = db.inner.engine.read().expect("engine runtime lock");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after mutation");
        assert!(
            docs_after.pointer.is_table_paged_manifest(),
            "mutated table should remain paged"
        );
        assert!(
            docs_after.pk_index_root.is_some(),
            "mutated paged table should retain persistent pk locator root"
        );
        let page_store = PagerReadStore { db: &db };
        let manifest_after = read_overflow(&page_store, docs_after.pointer)
            .expect("read paged manifest after mutation");
        let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
            .expect("decode paged manifest after mutation");
        assert_eq!(
            manifest_after
                .chunks
                .iter()
                .map(|chunk| chunk.row_count)
                .sum::<usize>(),
            95,
            "paged manifest row counts should reflect the delete"
        );
        manifest_after
            .chunks
            .iter()
            .filter_map(|chunk| {
                untouched_chunk_pointers
                    .contains(&chunk.pointer)
                    .then_some(chunk.pointer)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        preserved_untouched, untouched_chunk_pointers,
        "unchanged paged chunks should retain their original pointers"
    );

    let updated = db
        .execute("SELECT n FROM docs WHERE id = 6")
        .expect("point lookup after update");
    assert_eq!(scalar_i64(&updated), 500);
    let deleted = db
        .execute("SELECT n FROM docs WHERE id = 7")
        .expect("point lookup after delete");
    assert!(
        deleted.rows().is_empty(),
        "deleted row should no longer be visible"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        95
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after mutation");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged point lookup to stay deferred after mutation, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_updates_and_deletes_after_reopen_preserve_untouched_chunks() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-update-delete-after-reopen.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let untouched_chunk_pointers = {
        let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at reopen, got: {json_open}"
        );

        let untouched_chunk_pointers = {
            let runtime_before = db.inner.engine.read().expect("engine runtime lock");
            let docs_before = runtime_before
                .persisted_tables
                .get("docs")
                .expect("persisted docs before mutation");
            let page_store = PagerReadStore { db: &db };
            let manifest_before = read_overflow(&page_store, docs_before.pointer)
                .expect("read paged manifest before mutation");
            let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                .expect("decode paged manifest before mutation");
            assert!(
                manifest_before.chunks.len() > 2,
                "expected multiple chunks before mutation"
            );
            manifest_before
                .chunks
                .iter()
                .skip(1)
                .map(|chunk| chunk.pointer)
                .collect::<Vec<_>>()
        };

        db.execute(&format!(
            "UPDATE docs SET n = 500, body = '{}' WHERE id = 6",
            "y".repeat(2600)
        ))
        .expect("update docs row after reopen");
        db.execute("DELETE FROM docs WHERE id = 7")
            .expect("delete docs row after reopen");

        untouched_chunk_pointers
    };

    let db = Db::open_or_create(&path, config).expect("reopen mutated db");
    let preserved_untouched = {
        let runtime_after = db.inner.engine.read().expect("engine runtime lock");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after mutation");
        assert!(
            docs_after.pointer.is_table_paged_manifest(),
            "mutated table should remain paged"
        );
        assert!(
            docs_after.pk_index_root.is_some(),
            "mutated paged table should retain persistent pk locator root"
        );
        let page_store = PagerReadStore { db: &db };
        let manifest_after = read_overflow(&page_store, docs_after.pointer)
            .expect("read paged manifest after mutation");
        let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
            .expect("decode paged manifest after mutation");
        assert_eq!(
            manifest_after
                .chunks
                .iter()
                .map(|chunk| chunk.row_count)
                .sum::<usize>(),
            95,
            "paged manifest row counts should reflect the delete"
        );
        manifest_after
            .chunks
            .iter()
            .filter_map(|chunk| {
                untouched_chunk_pointers
                    .contains(&chunk.pointer)
                    .then_some(chunk.pointer)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        preserved_untouched, untouched_chunk_pointers,
        "unchanged paged chunks should retain their original pointers after reopen-time writes"
    );

    let updated = db
        .execute("SELECT n FROM docs WHERE id = 6")
        .expect("point lookup after reopen update");
    assert_eq!(scalar_i64(&updated), 500);
    let deleted = db
        .execute("SELECT n FROM docs WHERE id = 7")
        .expect("point lookup after reopen delete");
    assert!(
        deleted.rows().is_empty(),
        "deleted row should no longer be visible"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        95
    );
}

#[test]
fn paged_row_storage_prepared_insert_after_reopen_preserves_untouched_chunks() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-insert-after-reopen.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let untouched_chunk_pointers = {
        let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at reopen, got: {json_open}"
        );

        let untouched_chunk_pointers = {
            let runtime_before = db.inner.engine.read().expect("engine runtime lock");
            let docs_before = runtime_before
                .persisted_tables
                .get("docs")
                .expect("persisted docs before insert");
            let page_store = PagerReadStore { db: &db };
            let manifest_before = read_overflow(&page_store, docs_before.pointer)
                .expect("read paged manifest before insert");
            let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                .expect("decode paged manifest before insert");
            assert!(
                manifest_before.chunks.len() > 2,
                "expected multiple chunks before insert"
            );
            manifest_before
                .chunks
                .iter()
                .take(manifest_before.chunks.len() - 1)
                .map(|chunk| chunk.pointer)
                .collect::<Vec<_>>()
        };

        let insert = db
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert after reopen");
        insert
            .execute(&[
                Value::Int64(97),
                Value::Int64(9600),
                Value::Text("z".repeat(2048)),
            ])
            .expect("insert row after reopen");

        untouched_chunk_pointers
    };

    let db = Db::open_or_create(&path, config).expect("reopen mutated db");
    let preserved_untouched = {
        let runtime_after = db.inner.engine.read().expect("engine runtime lock");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after insert");
        assert!(
            docs_after.pointer.is_table_paged_manifest(),
            "inserted table should remain paged"
        );
        assert!(
            docs_after.pk_index_root.is_some(),
            "inserted paged table should retain persistent pk locator root"
        );
        let page_store = PagerReadStore { db: &db };
        let manifest_after = read_overflow(&page_store, docs_after.pointer)
            .expect("read paged manifest after insert");
        let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
            .expect("decode paged manifest after insert");
        assert_eq!(
            manifest_after
                .chunks
                .iter()
                .map(|chunk| chunk.row_count)
                .sum::<usize>(),
            97,
            "paged manifest row counts should reflect the insert"
        );
        manifest_after
            .chunks
            .iter()
            .filter_map(|chunk| {
                untouched_chunk_pointers
                    .contains(&chunk.pointer)
                    .then_some(chunk.pointer)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        preserved_untouched, untouched_chunk_pointers,
        "untouched paged chunks should retain their original pointers after reopen-time insert"
    );

    let inserted = db
        .execute("SELECT n FROM docs WHERE id = 97")
        .expect("point lookup after reopen insert");
    assert_eq!(scalar_i64(&inserted), 9600);
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        97
    );
    let json_after = db
        .inspect_storage_state_json()
        .expect("json after insert reopen");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected inserted paged table to remain off the resident path, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected inserted paged table to remain deferred after reopen, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_prepared_insert_after_reopen_preserves_untouched_chunks_without_persistent_pk_index(
) {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-insert-after-reopen-without-pk-index.ddb");
    let config = DbConfig {
        persistent_pk_index: false,
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let untouched_chunk_pointers = {
        let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at reopen, got: {json_open}"
        );

        let untouched_chunk_pointers = {
            let runtime_before = db.inner.engine.read().expect("engine runtime lock");
            let docs_before = runtime_before
                .persisted_tables
                .get("docs")
                .expect("persisted docs before insert");
            let page_store = PagerReadStore { db: &db };
            let manifest_before = read_overflow(&page_store, docs_before.pointer)
                .expect("read paged manifest before insert");
            let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                .expect("decode paged manifest before insert");
            assert!(
                manifest_before.chunks.len() > 2,
                "expected multiple chunks before insert"
            );
            manifest_before
                .chunks
                .iter()
                .take(manifest_before.chunks.len() - 1)
                .map(|chunk| chunk.pointer)
                .collect::<Vec<_>>()
        };

        let insert = db
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert after reopen");
        insert
            .execute(&[
                Value::Int64(97),
                Value::Int64(9600),
                Value::Text("z".repeat(2048)),
            ])
            .expect("insert row after reopen");

        untouched_chunk_pointers
    };

    let db = Db::open_or_create(&path, config).expect("reopen mutated db");
    let preserved_untouched = {
        let runtime_after = db.inner.engine.read().expect("engine runtime lock");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after insert");
        assert!(
            docs_after.pointer.is_table_paged_manifest(),
            "inserted table should remain paged"
        );
        assert!(
            docs_after.pk_index_root.is_none(),
            "non-persistent pk config should not write a pk locator root"
        );
        let page_store = PagerReadStore { db: &db };
        let manifest_after = read_overflow(&page_store, docs_after.pointer)
            .expect("read paged manifest after insert");
        let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
            .expect("decode paged manifest after insert");
        assert_eq!(
            manifest_after
                .chunks
                .iter()
                .map(|chunk| chunk.row_count)
                .sum::<usize>(),
            97,
            "paged manifest row counts should reflect the insert"
        );
        manifest_after
            .chunks
            .iter()
            .filter_map(|chunk| {
                untouched_chunk_pointers
                    .contains(&chunk.pointer)
                    .then_some(chunk.pointer)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        preserved_untouched, untouched_chunk_pointers,
        "untouched paged chunks should retain their original pointers after reopen-time insert"
    );

    let inserted = db
        .execute("SELECT n FROM docs WHERE id = 97")
        .expect("point lookup after reopen insert");
    assert_eq!(scalar_i64(&inserted), 9600);
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        97
    );
    let json_after = db
        .inspect_storage_state_json()
        .expect("json after insert reopen");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected inserted paged table to remain off the resident path, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected inserted paged table to remain deferred after reopen, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_expression_insert_after_reopen_preserves_untouched_chunks() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-expression-insert-after-reopen.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let untouched_chunk_pointers = {
        let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at reopen, got: {json_open}"
        );

        let untouched_chunk_pointers = {
            let runtime_before = db.inner.engine.read().expect("engine runtime lock");
            let docs_before = runtime_before
                .persisted_tables
                .get("docs")
                .expect("persisted docs before insert");
            let page_store = PagerReadStore { db: &db };
            let manifest_before = read_overflow(&page_store, docs_before.pointer)
                .expect("read paged manifest before insert");
            let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                .expect("decode paged manifest before insert");
            assert!(
                manifest_before.chunks.len() > 2,
                "expected multiple chunks before insert"
            );
            manifest_before
                .chunks
                .iter()
                .take(manifest_before.chunks.len() - 1)
                .map(|chunk| chunk.pointer)
                .collect::<Vec<_>>()
        };

        db.execute_with_params(
            "INSERT INTO docs VALUES (97, 9600 + 1, $1)",
            &[Value::Text("z".repeat(2048))],
        )
        .expect("insert expression row after reopen");

        untouched_chunk_pointers
    };

    let db = Db::open_or_create(&path, config).expect("reopen mutated db");
    let preserved_untouched = {
        let runtime_after = db.inner.engine.read().expect("engine runtime lock");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after insert");
        assert!(
            docs_after.pointer.is_table_paged_manifest(),
            "inserted table should remain paged"
        );
        assert!(
            docs_after.pk_index_root.is_some(),
            "inserted paged table should retain persistent pk locator root"
        );
        let page_store = PagerReadStore { db: &db };
        let manifest_after = read_overflow(&page_store, docs_after.pointer)
            .expect("read paged manifest after insert");
        let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
            .expect("decode paged manifest after insert");
        assert_eq!(
            manifest_after
                .chunks
                .iter()
                .map(|chunk| chunk.row_count)
                .sum::<usize>(),
            97,
            "paged manifest row counts should reflect the insert"
        );
        manifest_after
            .chunks
            .iter()
            .filter_map(|chunk| {
                untouched_chunk_pointers
                    .contains(&chunk.pointer)
                    .then_some(chunk.pointer)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "untouched paged chunks should retain their original pointers after reopen-time expression insert"
        );

    let inserted = db
        .execute("SELECT n FROM docs WHERE id = 97")
        .expect("point lookup after reopen insert");
    assert_eq!(scalar_i64(&inserted), 9601);
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        97
    );
    let json_after = db
        .inspect_storage_state_json()
        .expect("json after insert reopen");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected inserted paged table to remain off the resident path, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected inserted paged table to remain deferred after reopen, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_insert_returning_after_reopen_preserves_untouched_chunks() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-insert-returning-after-reopen.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let untouched_chunk_pointers = {
        let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at reopen, got: {json_open}"
        );

        let untouched_chunk_pointers = {
            let runtime_before = db.inner.engine.read().expect("engine runtime lock");
            let docs_before = runtime_before
                .persisted_tables
                .get("docs")
                .expect("persisted docs before insert");
            let page_store = PagerReadStore { db: &db };
            let manifest_before = read_overflow(&page_store, docs_before.pointer)
                .expect("read paged manifest before insert");
            let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                .expect("decode paged manifest before insert");
            assert!(
                manifest_before.chunks.len() > 2,
                "expected multiple chunks before insert"
            );
            manifest_before
                .chunks
                .iter()
                .take(manifest_before.chunks.len() - 1)
                .map(|chunk| chunk.pointer)
                .collect::<Vec<_>>()
        };

        let returning = db
            .execute_with_params(
                "INSERT INTO docs VALUES (97, 9600 + 1, $1) RETURNING n",
                &[Value::Text("z".repeat(2048))],
            )
            .expect("insert returning row after reopen");
        assert_eq!(scalar_i64(&returning), 9601);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after insert returning");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic INSERT RETURNING to avoid resident table loads, got: {json_after}"
        );
        assert!(
                json_after.contains("\"deferred_table_count\":1"),
                "expected inserted paged table to remain deferred after INSERT RETURNING, got: {json_after}"
            );

        untouched_chunk_pointers
    };

    let db = Db::open_or_create(&path, config).expect("reopen mutated db");
    let preserved_untouched = {
        let runtime_after = db.inner.engine.read().expect("engine runtime lock");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after insert");
        assert!(
            docs_after.pointer.is_table_paged_manifest(),
            "inserted table should remain paged"
        );
        assert!(
            docs_after.pk_index_root.is_some(),
            "inserted paged table should retain persistent pk locator root"
        );
        let page_store = PagerReadStore { db: &db };
        let manifest_after = read_overflow(&page_store, docs_after.pointer)
            .expect("read paged manifest after insert");
        let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
            .expect("decode paged manifest after insert");
        assert_eq!(
            manifest_after
                .chunks
                .iter()
                .map(|chunk| chunk.row_count)
                .sum::<usize>(),
            97,
            "paged manifest row counts should reflect the insert"
        );
        manifest_after
            .chunks
            .iter()
            .filter_map(|chunk| {
                untouched_chunk_pointers
                    .contains(&chunk.pointer)
                    .then_some(chunk.pointer)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "untouched paged chunks should retain their original pointers after reopen-time INSERT RETURNING"
        );
}

#[test]
fn paged_row_storage_insert_on_conflict_do_nothing_after_reopen_preserves_untouched_chunks() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-insert-on-conflict-do-nothing-after-reopen.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let untouched_chunk_pointers = {
        let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to stay deferred at reopen, got: {json_open}"
        );

        let untouched_chunk_pointers = {
            let runtime_before = db.inner.engine.read().expect("engine runtime lock");
            let docs_before = runtime_before
                .persisted_tables
                .get("docs")
                .expect("persisted docs before insert");
            let page_store = PagerReadStore { db: &db };
            let manifest_before = read_overflow(&page_store, docs_before.pointer)
                .expect("read paged manifest before insert");
            let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                .expect("decode paged manifest before insert");
            assert!(
                manifest_before.chunks.len() > 2,
                "expected multiple chunks before insert"
            );
            manifest_before
                .chunks
                .iter()
                .take(manifest_before.chunks.len() - 1)
                .map(|chunk| chunk.pointer)
                .collect::<Vec<_>>()
        };

        let affected = db
            .execute_with_params(
                "INSERT INTO docs VALUES (97, 9600 + 1, $1) ON CONFLICT(id) DO NOTHING",
                &[Value::Text("z".repeat(2048))],
            )
            .expect("insert on conflict do nothing after reopen");
        assert_eq!(affected.affected_rows(), 1);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after on conflict insert");
        assert!(
                json_after.contains("\"loaded_table_count\":0"),
                "expected INSERT .. ON CONFLICT DO NOTHING to avoid resident table loads, got: {json_after}"
            );
        assert!(
                json_after.contains("\"deferred_table_count\":1"),
                "expected inserted paged table to remain deferred after ON CONFLICT insert, got: {json_after}"
            );

        untouched_chunk_pointers
    };

    let db = Db::open_or_create(&path, config).expect("reopen mutated db");
    let preserved_untouched = {
        let runtime_after = db.inner.engine.read().expect("engine runtime lock");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after insert");
        assert!(
            docs_after.pointer.is_table_paged_manifest(),
            "inserted table should remain paged"
        );
        let page_store = PagerReadStore { db: &db };
        let manifest_after = read_overflow(&page_store, docs_after.pointer)
            .expect("read paged manifest after insert");
        let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
            .expect("decode paged manifest after insert");
        assert_eq!(
            manifest_after
                .chunks
                .iter()
                .map(|chunk| chunk.row_count)
                .sum::<usize>(),
            97,
            "paged manifest row counts should reflect the insert"
        );
        manifest_after
            .chunks
            .iter()
            .filter_map(|chunk| {
                untouched_chunk_pointers
                    .contains(&chunk.pointer)
                    .then_some(chunk.pointer)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "untouched paged chunks should retain their original pointers after reopen-time INSERT .. ON CONFLICT DO NOTHING"
        );
}

#[test]
fn paged_row_storage_insert_on_conflict_do_update_after_reopen_keeps_table_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-insert-on-conflict-do-update-after-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json at reopen");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed table to stay deferred at reopen, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to stay deferred at reopen, got: {json_open}"
    );

    let returning = db
        .execute_with_params(
            "INSERT INTO docs VALUES (1, 100, $1) \
                 ON CONFLICT(id) DO UPDATE SET n = excluded.n + 1 \
                 RETURNING n",
            &[Value::Text("z".repeat(2048))],
        )
        .expect("insert on conflict do update after reopen");
    assert_eq!(scalar_i64(&returning), 101);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after upsert update");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected INSERT .. ON CONFLICT DO UPDATE to avoid resident table loads, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected INSERT .. ON CONFLICT DO UPDATE to keep the paged table deferred, got: {json_after}"
        );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT n FROM docs WHERE id = 1")
                .expect("point lookup after upsert update")
        ),
        101
    );
    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM docs").expect("count docs")),
        96
    );
}

#[test]
fn paged_row_storage_insert_on_conflict_parent_key_update_with_setnull_fk_after_reopen_keeps_tables_deferred(
) {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-upsert-parent-key-update-setnull-fk.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code INTEGER, body TEXT)")
            .expect("create parent");
        db.execute("CREATE UNIQUE INDEX parent_code_idx ON parent(code)")
            .expect("create parent code index");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_code INTEGER REFERENCES parent(code) ON UPDATE SET NULL, body TEXT)",
            )
            .expect("create child");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        for i in 0_i64..32_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(parent_body.clone()),
                    ],
                )
                .expect("insert parent row");
            if i < 16 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json at reopen");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":2"),
        "expected parent and child tables to stay deferred at reopen, got: {json_open}"
    );

    let returning = db
        .execute_with_params(
            "INSERT INTO parent VALUES (1, 1001, $1) \
                 ON CONFLICT(id) DO UPDATE SET code = excluded.code \
                 RETURNING code",
            &[Value::Text("z".repeat(2048))],
        )
        .expect("parent-key upsert after reopen");
    assert_eq!(scalar_i64(&returning), 1001);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after parent-key upsert");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected parent-key upsert to re-defer loaded tables, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected parent-key upsert to keep parent and child deferred, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT code FROM parent WHERE id = 1")
                .expect("lookup parent code")
        ),
        1001
    );
    let child_parent = db
        .execute("SELECT parent_code FROM child WHERE id = 1")
        .expect("lookup child parent")
        .rows()[0]
        .values()[0]
        .clone();
    assert_eq!(child_parent, Value::Null);
}

#[test]
fn paged_row_storage_insert_on_conflict_foreign_key_update_after_reopen_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-upsert-foreign-key-update.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create parent");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), body TEXT)",
            )
            .expect("create child");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        for i in 0_i64..96_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                )
                .expect("insert parent row");
            if i < 48 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json at reopen");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":2"),
        "expected parent and child tables to stay deferred at reopen, got: {json_open}"
    );

    let returning = db
        .execute_with_params(
            "INSERT INTO child VALUES (1, 2, $1) \
                 ON CONFLICT(id) DO UPDATE SET parent_id = excluded.parent_id \
                 RETURNING parent_id",
            &[Value::Text("z".repeat(2048))],
        )
        .expect("foreign-key upsert after reopen");
    assert_eq!(scalar_i64(&returning), 2);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after foreign-key upsert");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected foreign-key upsert to re-defer loaded tables, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected parent and child tables to remain deferred, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT parent_id FROM child WHERE id = 1")
                .expect("lookup child parent id")
        ),
        2
    );
}

#[test]
fn paged_row_storage_insert_select_returning_after_reopen_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-insert-select-returning-after-reopen.ddb");
    let config = DbConfig {
        persistent_pk_index: true,
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive table");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let untouched_chunk_pointers = {
        let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
        let json_open = db.inspect_storage_state_json().expect("json at reopen");
        assert!(
            json_open.contains("\"loaded_table_count\":0"),
            "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
        );
        assert!(
            json_open.contains("\"deferred_table_count\":2"),
            "expected both paged-backed tables to stay deferred at reopen, got: {json_open}"
        );

        let untouched_chunk_pointers = {
            let runtime_before = db.inner.engine.read().expect("engine runtime lock");
            let docs_before = runtime_before
                .persisted_tables
                .get("docs")
                .expect("persisted docs before insert");
            let page_store = PagerReadStore { db: &db };
            let manifest_before = read_overflow(&page_store, docs_before.pointer)
                .expect("read paged manifest before insert");
            let manifest_before = decode_paged_table_manifest_payload(&manifest_before)
                .expect("decode paged manifest before insert");
            assert!(
                manifest_before.chunks.len() > 2,
                "expected multiple docs chunks before insert"
            );
            manifest_before
                .chunks
                .iter()
                .take(manifest_before.chunks.len() - 1)
                .map(|chunk| chunk.pointer)
                .collect::<Vec<_>>()
        };

        let returning = db
            .execute(
                "INSERT INTO docs \
                     SELECT doc_id + 96, doc_id + 1000, note \
                     FROM archive \
                     WHERE id = 1 \
                     RETURNING n",
            )
            .expect("insert select returning after reopen");
        assert_eq!(scalar_i64(&returning), 1001);

        let json_after = db
            .inspect_storage_state_json()
            .expect("json after insert select returning");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected INSERT .. SELECT RETURNING to avoid resident table loads, got: {json_after}"
        );
        assert!(
                json_after.contains("\"deferred_table_count\":2"),
                "expected INSERT .. SELECT RETURNING to keep both paged tables deferred, got: {json_after}"
            );

        untouched_chunk_pointers
    };

    let db = Db::open_or_create(&path, config).expect("reopen mutated db");
    let preserved_untouched = {
        let runtime_after = db.inner.engine.read().expect("engine runtime lock");
        let docs_after = runtime_after
            .persisted_tables
            .get("docs")
            .expect("persisted docs after insert");
        assert!(
            docs_after.pointer.is_table_paged_manifest(),
            "inserted table should remain paged"
        );
        let page_store = PagerReadStore { db: &db };
        let manifest_after = read_overflow(&page_store, docs_after.pointer)
            .expect("read paged manifest after insert");
        let manifest_after = decode_paged_table_manifest_payload(&manifest_after)
            .expect("decode paged manifest after insert");
        assert_eq!(
            manifest_after
                .chunks
                .iter()
                .map(|chunk| chunk.row_count)
                .sum::<usize>(),
            97,
            "paged manifest row counts should reflect the insert"
        );
        manifest_after
            .chunks
            .iter()
            .filter_map(|chunk| {
                untouched_chunk_pointers
                    .contains(&chunk.pointer)
                    .then_some(chunk.pointer)
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
            preserved_untouched, untouched_chunk_pointers,
            "untouched paged docs chunks should retain their original pointers after reopen-time INSERT .. SELECT RETURNING"
        );

    let inserted = db
        .execute("SELECT n FROM docs WHERE id = 97")
        .expect("point lookup after insert select");
    assert_eq!(scalar_i64(&inserted), 1001);
}

#[test]
fn paged_row_storage_union_all_after_reopen_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-union-all-after-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json at reopen");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":2"),
        "expected both paged-backed tables deferred at reopen, got: {json_open}"
    );

    let result = db
        .execute(
            "SELECT id FROM docs WHERE id = 1 \
                 UNION ALL \
                 SELECT doc_id FROM archive WHERE id = 2 \
                 ORDER BY id",
        )
        .expect("union all query after reopen");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(1)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(2)]);

    let json_after = db.inspect_storage_state_json().expect("json after union");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected UNION ALL to avoid resident table loads, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected UNION ALL to keep both paged tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_subquery_after_reopen_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-subquery-after-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json at reopen");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":2"),
        "expected both paged-backed tables deferred at reopen, got: {json_open}"
    );

    let result = db
        .execute(
            "SELECT id FROM docs \
                 WHERE id IN (SELECT doc_id FROM archive WHERE id = 2)",
        )
        .expect("subquery after reopen");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after subquery");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected subquery execution to avoid resident table loads, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected subquery execution to keep both paged tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_from_subquery_after_reopen_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-from-subquery-after-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json at reopen");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed tables to stay deferred at reopen, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":2"),
        "expected both paged-backed tables deferred at reopen, got: {json_open}"
    );

    let result = db
        .execute(
            "SELECT q.id, archive.note \
                 FROM (SELECT id FROM docs WHERE id = 2) AS q \
                 JOIN archive ON archive.doc_id = q.id",
        )
        .expect("from-subquery join after reopen");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values()[0], Value::Int64(2));

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after from-subquery");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected FROM-subquery execution to avoid resident table loads, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected FROM-subquery execution to keep both paged tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_shared_sql_transaction_insert_returning_keeps_paged_runtime_state() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare seed insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert seed row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction().expect("begin sql transaction");
    let json_before = db
        .inspect_storage_state_json()
        .expect("inspect state at txn begin");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected no eager resident load at BEGIN, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred at BEGIN, got: {json_before}"
    );

    let inserted = db
        .execute_with_params(
            "INSERT INTO docs VALUES ($1, $2, $3) RETURNING n",
            &[
                Value::Int64(97),
                Value::Int64(9600),
                Value::Text("z".repeat(2048)),
            ],
        )
        .expect("insert inside shared sql transaction");
    assert_eq!(scalar_i64(&inserted), 9600);
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert_eq!(
            state.runtime.tables.len(),
            1,
            "expected transaction runtime to load only the target table for INSERT RETURNING"
        );
        assert!(matches!(
            state.runtime.tables.get("docs"),
            Some(TableRowSource::Paged(_))
        ));
    }
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count inside shared txn")
        ),
        97
    );
    db.commit_transaction().expect("commit shared txn");

    let reopened = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        },
    )
    .expect("reopen committed db");
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT COUNT(*) FROM docs")
                .expect("count after shared txn commit")
        ),
        97
    );
}

#[test]
fn paged_row_storage_shared_sql_transaction_insert_on_conflict_do_update_keeps_paged_runtime_state()
{
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-upsert-update.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            1,
            "expected paged table deferred at BEGIN"
        );
    }

    let returning = db
        .execute_with_params(
            "INSERT INTO docs VALUES (1, 100, $1) \
                 ON CONFLICT(id) DO UPDATE SET n = excluded.n + 1 \
                 RETURNING n",
            &[Value::Text("z".repeat(2048))],
        )
        .expect("insert on conflict do update inside shared sql transaction");
    assert_eq!(scalar_i64(&returning), 101);
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert_eq!(
                state.runtime.tables.len(),
                1,
                "expected transaction runtime to load only the target table for INSERT .. ON CONFLICT DO UPDATE"
            );
        assert!(matches!(
            state.runtime.tables.get("docs"),
            Some(TableRowSource::Paged(_))
        ));
    }
    db.commit_transaction().expect("commit shared txn");

    let reopened = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        },
    )
    .expect("reopen committed db");
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT n FROM docs WHERE id = 1")
                .expect("point lookup after shared txn commit")
        ),
        101
    );
}

#[test]
fn paged_row_storage_shared_sql_transaction_insert_on_conflict_parent_key_update_keeps_paged_runtime_state(
) {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-upsert-parent-key.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code INTEGER, body TEXT)")
            .expect("create parent");
        db.execute("CREATE UNIQUE INDEX parent_code_idx ON parent(code)")
            .expect("create parent code index");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_code INTEGER REFERENCES parent(code) ON UPDATE SET NULL, body TEXT)",
            )
            .expect("create child");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        for i in 0_i64..32_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(parent_body.clone()),
                    ],
                )
                .expect("insert parent row");
            if i < 16 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected parent and child tables deferred at BEGIN"
        );
    }

    let returning = db
        .execute_with_params(
            "INSERT INTO parent VALUES (1, 1001, $1) \
                 ON CONFLICT(id) DO UPDATE SET code = excluded.code \
                 RETURNING code",
            &[Value::Text("z".repeat(2048))],
        )
        .expect("parent-key upsert inside shared sql transaction");
    assert_eq!(scalar_i64(&returning), 1001);
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert_eq!(
                state.runtime.tables.len(),
                2,
                "expected transaction runtime to load target and child tables for parent-key INSERT .. ON CONFLICT DO UPDATE"
            );
        assert!(matches!(
            state.runtime.tables.get("parent"),
            Some(TableRowSource::Paged(_))
        ));
        assert!(matches!(
            state.runtime.tables.get("child"),
            Some(TableRowSource::Paged(_))
        ));
    }
    db.commit_transaction().expect("commit shared txn");

    let reopened = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        },
    )
    .expect("reopen committed db");
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT code FROM parent WHERE id = 1")
                .expect("parent after shared txn commit")
        ),
        1001
    );
    let child_parent = reopened
        .execute("SELECT parent_code FROM child WHERE id = 1")
        .expect("child after shared txn commit")
        .rows()[0]
        .values()[0]
        .clone();
    assert_eq!(child_parent, Value::Null);
}

#[test]
fn paged_row_storage_shared_sql_transaction_insert_on_conflict_foreign_key_update_keeps_targeted_runtime_state(
) {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-upsert-foreign-key.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create parent");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), body TEXT)",
            )
            .expect("create child");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        for i in 0_i64..96_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                )
                .expect("insert parent row");
            if i < 48 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected parent and child tables deferred at BEGIN"
        );
    }

    let returning = db
        .execute_with_params(
            "INSERT INTO child VALUES (1, 2, $1) \
                 ON CONFLICT(id) DO UPDATE SET parent_id = excluded.parent_id \
                 RETURNING parent_id",
            &[Value::Text("z".repeat(2048))],
        )
        .expect("foreign-key upsert inside shared sql transaction");
    assert_eq!(scalar_i64(&returning), 2);
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert_eq!(
                state.runtime.tables.len(),
                2,
                "expected transaction runtime to load target and parent tables for foreign-key INSERT .. ON CONFLICT DO UPDATE"
            );
        assert!(
            state.runtime.tables.contains_key("child"),
            "expected child table to be loaded for foreign-key upsert"
        );
        assert!(
            state.runtime.tables.contains_key("parent"),
            "expected parent table to be loaded for foreign-key validation"
        );
    }
    db.commit_transaction().expect("commit shared txn");

    let reopened = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        },
    )
    .expect("reopen committed db");
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT parent_id FROM child WHERE id = 1")
                .expect("child after shared txn commit")
        ),
        2
    );
}

#[test]
fn paged_row_storage_shared_sql_transaction_insert_select_returning_keeps_paged_runtime_state() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-insert-select.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive table");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected both paged tables deferred at BEGIN"
        );
    }

    let inserted = db
        .execute(
            "INSERT INTO docs \
                 SELECT doc_id + 96, doc_id + 1000, note \
                 FROM archive \
                 WHERE id = 1 \
                 RETURNING n",
        )
        .expect("insert select returning inside shared sql transaction");
    assert_eq!(scalar_i64(&inserted), 1001);
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert_eq!(
                state.runtime.tables.len(),
                2,
                "expected transaction runtime to load only target and source tables for INSERT .. SELECT RETURNING"
            );
        assert!(matches!(
            state.runtime.tables.get("docs"),
            Some(TableRowSource::Paged(_))
        ));
        assert!(matches!(
            state.runtime.tables.get("archive"),
            Some(TableRowSource::Paged(_))
        ));
    }
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count inside shared txn")
        ),
        97
    );
    db.commit_transaction().expect("commit shared txn");

    let reopened = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        },
    )
    .expect("reopen committed db");
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT n FROM docs WHERE id = 97")
                .expect("point lookup after shared txn commit")
        ),
        1001
    );
}

#[test]
fn paged_row_storage_shared_sql_transaction_union_all_keeps_runtime_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-union-all.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin seed txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected both paged tables deferred at BEGIN"
        );
    }

    let result = db
        .execute(
            "SELECT id FROM docs WHERE id = 1 \
                 UNION ALL \
                 SELECT doc_id FROM archive WHERE id = 2 \
                 ORDER BY id",
        )
        .expect("shared transaction union all");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(1)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(2)]);

    {
        let txn = db
            .inner
            .sql_txn
            .lock()
            .expect("lock shared txn slot after union");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected UNION ALL fast path to avoid loading transaction tables"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected UNION ALL fast path to keep both tables deferred in the transaction runtime"
        );
    }
    db.commit_transaction().expect("commit shared txn");
}

#[test]
fn paged_row_storage_shared_sql_transaction_subquery_keeps_runtime_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-subquery.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin seed txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected both paged tables deferred at BEGIN"
        );
    }

    let result = db
        .execute(
            "SELECT id FROM docs \
                 WHERE id IN (SELECT doc_id FROM archive WHERE id = 2)",
        )
        .expect("shared transaction subquery");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);

    {
        let txn = db
            .inner
            .sql_txn
            .lock()
            .expect("lock shared txn slot after subquery");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected subquery fast path to avoid loading transaction tables"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected subquery fast path to keep both tables deferred in the transaction runtime"
        );
    }
    db.commit_transaction().expect("commit shared txn");
}

#[test]
fn paged_row_storage_cte_after_reopen_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-cte-after-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert docs row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "WITH scoped AS (SELECT id, n FROM docs WHERE n >= 90) \
                 SELECT id FROM scoped WHERE n < 95 ORDER BY id",
        )
        .expect("query non-recursive cte");
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Int64(91),
            Value::Int64(92),
            Value::Int64(93),
            Value::Int64(94),
            Value::Int64(95),
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after cte");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected non-recursive CTE query to avoid live-runtime table loads, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected docs to remain deferred after non-recursive CTE query, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_values_cte_after_reopen_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-values-cte-after-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert docs row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "WITH threshold(v) AS (VALUES (90)) \
                 SELECT id FROM docs WHERE n >= (SELECT v FROM threshold) ORDER BY id LIMIT 3",
        )
        .expect("query VALUES CTE");
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values()[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(91), Value::Int64(92), Value::Int64(93)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after values cte");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected VALUES CTE query to avoid live-runtime table loads, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected docs to remain deferred after VALUES CTE query, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_shared_sql_transaction_cte_keeps_runtime_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-cte.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert docs row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            1,
            "expected docs deferred at BEGIN"
        );
    }

    let result = db
        .execute(
            "WITH scoped AS (SELECT id, n FROM docs WHERE n >= 90) \
                 SELECT id FROM scoped WHERE n < 95 ORDER BY id",
        )
        .expect("shared transaction CTE query");
    assert_eq!(result.rows().len(), 5);

    {
        let txn = db
            .inner
            .sql_txn
            .lock()
            .expect("lock shared txn slot after cte");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected CTE fast path to avoid loading transaction tables"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            1,
            "expected CTE fast path to keep docs deferred in the transaction runtime"
        );
    }
    db.commit_transaction().expect("commit shared txn");
}

#[test]
fn paged_row_storage_shared_sql_transaction_from_subquery_keeps_runtime_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-from-subquery.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin seed txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected both paged tables deferred at BEGIN"
        );
    }

    let result = db
        .execute(
            "SELECT q.id, archive.note \
                 FROM (SELECT id FROM docs WHERE id = 2) AS q \
                 JOIN archive ON archive.doc_id = q.id",
        )
        .expect("shared transaction from-subquery");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values()[0], Value::Int64(2));

    {
        let txn = db
            .inner
            .sql_txn
            .lock()
            .expect("lock shared txn slot after from-subquery");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected FROM-subquery fast path to avoid loading transaction tables"
        );
        assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected FROM-subquery fast path to keep both tables deferred in the transaction runtime"
            );
    }
    db.commit_transaction().expect("commit shared txn");
}

#[test]
fn dump_sql_keeps_deferred_paged_tables_unloaded_after_reopen() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("dump-sql-keeps-deferred-paged-tables-unloaded.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );

    let dump = db.dump_sql().expect("dump sql");
    assert!(
        dump.contains("CREATE TABLE \"docs\""),
        "dump missing table DDL: {dump}"
    );
    assert!(
        dump.contains("INSERT INTO \"docs\""),
        "dump missing row inserts: {dump}"
    );

    let json_after = db.inspect_storage_state_json().expect("json after dump");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected dump_sql to avoid live materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected dump_sql to keep paged-backed table deferred, got: {json_after}"
    );
}

#[test]
fn dump_sql_in_shared_sql_transaction_includes_deferred_rows_without_loading_runtime() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("dump-sql-shared-transaction-deferred-rows.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            1,
            "expected paged table deferred at BEGIN"
        );
    }

    let dump = db.dump_sql().expect("dump sql in shared txn");
    assert!(
        dump.contains("CREATE TABLE \"docs\""),
        "dump missing table DDL: {dump}"
    );
    assert!(
        dump.contains("INSERT INTO \"docs\""),
        "dump missing row inserts: {dump}"
    );

    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected dump_sql to keep the shared transaction runtime deferred"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            1,
            "expected dump_sql to leave the shared transaction table deferred"
        );
    }
    db.commit_transaction().expect("commit shared txn");
}

#[test]
fn integrity_check_keeps_deferred_paged_tables_unloaded_after_reopen() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("integrity-check-keeps-deferred-paged-tables-unloaded.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );

    let result = db
        .execute("PRAGMA integrity_check")
        .expect("integrity check");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Text("ok".to_string())]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after integrity check");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected integrity_check to avoid live materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected integrity_check to keep paged-backed table deferred, got: {json_after}"
    );
}

#[test]
fn integrity_check_in_shared_sql_transaction_keeps_runtime_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("integrity-check-shared-transaction-deferred.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            1,
            "expected paged table deferred at BEGIN"
        );
    }

    let result = db
        .execute("PRAGMA integrity_check")
        .expect("integrity check");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Text("ok".to_string())]);

    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected integrity_check to keep the shared transaction runtime deferred"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            1,
            "expected integrity_check to leave the shared transaction table deferred"
        );
    }
    db.commit_transaction().expect("commit shared txn");
}

#[test]
fn bulk_load_keeps_deferred_paged_tables_unloaded_after_reopen() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("bulk-load-keeps-deferred-paged-tables-unloaded.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to stay deferred at open, got: {json_open}"
    );

    let inserted = db
        .bulk_load_rows(
            "docs",
            &["id", "n", "body"],
            &[vec![
                Value::Int64(97),
                Value::Int64(9600),
                Value::Text("z".repeat(2048)),
            ]],
            BulkLoadOptions::default(),
        )
        .expect("bulk load rows");
    assert_eq!(inserted, 1);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after bulk load");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected bulk_load_rows to avoid live materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected bulk_load_rows to keep paged-backed table deferred, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count after bulk load")
        ),
        97
    );
}

#[test]
fn exclusive_transaction_commit_redefers_paged_tables() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("exclusive-transaction-commit-redefers-paged-tables.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    let db = Db::open_or_create(&path, config).expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs");

    let mut txn = db.transaction().expect("begin exclusive txn");
    let insert = txn
        .prepare("INSERT INTO docs VALUES ($1, $2)")
        .expect("prepare insert");
    let large_body = "x".repeat(2048);
    for i in 0_i64..64_i64 {
        insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(i + 1),
                    Value::Text(format!("body-{i}-{large_body}")),
                ],
            )
            .expect("insert row");
    }
    txn.commit().expect("commit exclusive txn");

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after exclusive txn commit");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected exclusive transaction commit to redefer paged table, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected exclusive transaction commit to keep paged table deferred, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count after exclusive txn commit")
        ),
        64
    );

    let mut duplicate_txn = db.transaction().expect("begin duplicate txn");
    let duplicate_insert = duplicate_txn
        .prepare("INSERT INTO docs VALUES ($1, $2)")
        .expect("prepare duplicate insert");
    let duplicate = duplicate_insert.execute_in(
        &mut duplicate_txn,
        &[Value::Int64(1), Value::Text("duplicate".to_string())],
    );
    assert!(
        duplicate.is_err(),
        "expected duplicate primary key insert to fail after index eviction"
    );
}

#[test]
fn nontransaction_view_read_redefers_paged_tables() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("nontransaction-view-read-redefers-paged-tables.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    let db = Db::open_or_create(&path, config).expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs");
    db.execute("CREATE VIEW doc_view AS SELECT id, body FROM docs")
        .expect("create view");
    let mut txn = db.transaction().expect("begin seed txn");
    let insert = txn
        .prepare("INSERT INTO docs VALUES ($1, $2)")
        .expect("prepare insert");
    let large_body = "x".repeat(2048);
    for i in 0_i64..64_i64 {
        insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(i + 1),
                    Value::Text(format!("body-{i}-{large_body}")),
                ],
            )
            .expect("insert row");
    }
    txn.commit().expect("commit seed txn");

    let rows = db
        .execute("SELECT id, body FROM doc_view LIMIT 4")
        .expect("query view");
    assert_eq!(rows.rows().len(), 4);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after view read");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected nontransaction view read to redefer paged table, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected nontransaction view read to leave paged table deferred, got: {json_after}"
    );
}

#[test]
fn prepare_after_redefer_with_partial_unique_index_does_not_rebuild_missing_row_source() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("prepare-after-redefer-partial-unique-index.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    let db = Db::open_or_create(&path, config).expect("open db");
    db.execute("CREATE TABLE Libraries (id INTEGER PRIMARY KEY, name TEXT NOT NULL, kind INTEGER NOT NULL)")
            .expect("create libraries");
    db.execute("CREATE UNIQUE INDEX Libraries_kind_unique ON Libraries (kind) WHERE kind != 3")
        .expect("create partial unique index");

    let large_name = "x".repeat(70_000);
    let insert = db
        .prepare("INSERT INTO Libraries (id, name, kind) VALUES ($1, $2, $3)")
        .expect("prepare first excluded row");
    insert
        .execute(&[
            Value::Int64(11),
            Value::Text(format!("Storage One {large_name}")),
            Value::Int64(3),
        ])
        .expect("insert excluded row");
    let json_after_first = db
        .inspect_storage_state_json()
        .expect("json after first insert");
    assert!(
        json_after_first.contains("\"deferred_table_count\":1"),
        "expected first insert to redefer paged table, got: {json_after_first}"
    );

    let prepared = db
        .prepare("INSERT INTO Libraries (id, name, kind) VALUES ($1, $2, $3)")
        .expect("prepare second excluded row after redefer");
    prepared
        .execute(&[
            Value::Int64(12),
            Value::Text(format!("Storage Two {large_name}")),
            Value::Int64(3),
        ])
        .expect("execute second excluded row after redefer");
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM libraries WHERE kind = 3")
                .expect("count excluded rows")
        ),
        2
    );

    let duplicate =
        db.execute("INSERT INTO Libraries (id, name, kind) VALUES (13, 'Duplicate Covered', 1)");
    assert!(
        duplicate.is_ok(),
        "expected first covered row insert to succeed: {duplicate:?}"
    );
    let duplicate = db.execute(
        "INSERT INTO Libraries (id, name, kind) VALUES (14, 'Duplicate Covered Again', 1)",
    );
    assert!(
        duplicate.is_err(),
        "expected partial unique index to reject duplicate covered rows"
    );
}

#[test]
fn paged_row_storage_shared_sql_transaction_indexed_join_keeps_runtime_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin seed txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected both paged tables deferred at BEGIN"
        );
    }

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8",
        )
        .expect("shared transaction indexed join");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Text(format!("{}-7", "y".repeat(1024)))
        ]
    );

    {
        let txn = db
            .inner
            .sql_txn
            .lock()
            .expect("lock shared txn slot after join");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected indexed join fast path to avoid loading transaction tables"
        );
        assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected indexed join fast path to keep both tables deferred in the transaction runtime"
            );
    }

    db.commit_transaction()
        .expect("commit shared sql transaction");
}

#[test]
fn paged_row_storage_shared_sql_transaction_indexed_left_join_keeps_runtime_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-left-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin seed txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            if i < 48 {
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected both paged tables deferred at BEGIN"
        );
    }

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 LEFT JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 96",
        )
        .expect("shared transaction indexed left join");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(96), Value::Null]);

    {
        let txn = db
            .inner
            .sql_txn
            .lock()
            .expect("lock shared txn slot after join");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected indexed left join fast path to avoid loading transaction tables"
        );
        assert_eq!(
                state.runtime.deferred_tables.len(),
                2,
                "expected indexed left join fast path to keep both tables deferred in the transaction runtime"
            );
    }

    db.commit_transaction()
        .expect("commit shared sql transaction");
}

#[test]
fn paged_row_storage_shared_sql_transaction_generic_join_keeps_runtime_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-generic-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin seed txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected both paged tables deferred at BEGIN"
        );
    }

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id >= 8 AND docs.id < 10 \
                 ORDER BY docs.id",
        )
        .expect("shared transaction generic join");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Text(format!("{}-7", "y".repeat(1024)))
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(9),
            Value::Text(format!("{}-8", "y".repeat(1024)))
        ]
    );

    {
        let txn = db
            .inner
            .sql_txn
            .lock()
            .expect("lock shared txn slot after join");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected generic join path to avoid loading transaction tables"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected generic join path to keep both tables deferred in the transaction runtime"
        );
    }

    db.commit_transaction()
        .expect("commit shared sql transaction");
}

#[test]
fn paged_row_storage_exclusive_sql_transaction_uses_deferred_tables_on_demand() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-exclusive-sql-transaction.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs table");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare seed insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert seed row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let mut txn = db.transaction().expect("begin exclusive sql transaction");
    let insert = txn
        .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
        .expect("prepare insert in exclusive txn");
    insert
        .execute_in(
            &mut txn,
            &[
                Value::Int64(97),
                Value::Int64(9601),
                Value::Text("z".repeat(2048)),
            ],
        )
        .expect("insert inside exclusive sql transaction");
    let count = txn
        .prepare("SELECT COUNT(*) FROM docs")
        .expect("prepare count in exclusive txn");
    assert_eq!(
        scalar_i64(
            &count
                .execute_in(&mut txn, &[])
                .expect("count in exclusive txn")
        ),
        97
    );
    txn.commit().expect("commit exclusive txn");

    let reopened = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        },
    )
    .expect("reopen committed db");
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT COUNT(*) FROM docs")
                .expect("count after exclusive txn commit")
        ),
        97
    );
}

#[test]
fn paged_row_storage_update_loads_all_foreign_key_parents_for_validation() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-update-fk-parents.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE Users (Id INTEGER PRIMARY KEY, Name TEXT)")
            .expect("create users");
        db.execute("CREATE TABLE Songs (Id INTEGER PRIMARY KEY, Title TEXT)")
            .expect("create songs");
        db.execute(
            "CREATE TABLE UserSongs (\
                 Id INTEGER PRIMARY KEY, \
                 UserId INTEGER NOT NULL, \
                 SongId INTEGER NOT NULL, \
                 Note TEXT, \
                 FOREIGN KEY (UserId) REFERENCES Users(Id), \
                 FOREIGN KEY (SongId) REFERENCES Songs(Id))",
        )
        .expect("create user songs");
        db.execute("CREATE UNIQUE INDEX ux_user_songs_user_song ON UserSongs(UserId, SongId)")
            .expect("create user song unique index");
        db.execute("INSERT INTO Users VALUES (1, 'user')")
            .expect("insert user");
        db.execute("INSERT INTO Songs VALUES (1, 'old')")
            .expect("insert old song");
        db.execute("INSERT INTO Songs VALUES (2, 'new')")
            .expect("insert new song");
        db.execute("INSERT INTO UserSongs VALUES (1, 1, 1, 'liked')")
            .expect("insert user song");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction().expect("begin sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert_eq!(
            state.runtime.deferred_tables.len(),
            3,
            "expected all seeded tables to start deferred"
        );
    }

    db.execute("UPDATE UserSongs SET SongId = 2 WHERE Id = 1")
        .expect("update child foreign key while unchanged parent fk is deferred");
    db.commit_transaction().expect("commit sql transaction");

    let result = db
        .execute("SELECT UserId, SongId, Note FROM UserSongs WHERE Id = 1")
        .expect("select updated user song");
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Int64(2),
            Value::Text("liked".to_string())
        ]
    );
}

#[test]
fn paged_row_storage_indexed_join_projection_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-indexed-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both join tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8",
        )
        .expect("indexed join projection query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Text(format!("{}-7", "y".repeat(1024)))
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join projection to avoid resident table materialization, got: {json_after}"
        );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected indexed join projection to leave both paged tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_view_filter_indexed_join_chain_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-view-filtered-join-chain.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE artists (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create artists");
        db.execute(
            "CREATE TABLE albums (id INTEGER PRIMARY KEY, artist_id INTEGER NOT NULL, title TEXT)",
        )
        .expect("create albums");
        db.execute(
                "CREATE TABLE songs (id INTEGER PRIMARY KEY, album_id INTEGER NOT NULL, title TEXT, duration_ms INTEGER NOT NULL)",
            )
            .expect("create songs");
        db.execute("CREATE INDEX idx_albums_artist ON albums (artist_id)")
            .expect("create albums artist index");
        db.execute("CREATE INDEX idx_songs_album ON songs (album_id)")
            .expect("create songs album index");
        db.execute(
            "CREATE VIEW v_artist_songs AS \
                 SELECT a.id AS artist_id, a.name AS artist_name, al.title AS album_title, \
                        s.title AS song_title, s.duration_ms AS duration_ms \
                 FROM artists a JOIN albums al ON al.artist_id = a.id \
                 JOIN songs s ON s.album_id = al.id",
        )
        .expect("create view");

        db.execute("INSERT INTO artists (id, name) VALUES (1, 'a')")
            .expect("insert artist 1");
        db.execute("INSERT INTO artists (id, name) VALUES (2, 'b')")
            .expect("insert artist 2");
        db.execute("INSERT INTO albums (id, artist_id, title) VALUES (10, 1, 'a1')")
            .expect("insert album 1");
        db.execute("INSERT INTO albums (id, artist_id, title) VALUES (20, 2, 'b1')")
            .expect("insert album 2");
        db.execute(
            "INSERT INTO songs (id, album_id, title, duration_ms) VALUES (100, 10, 's1', 1000)",
        )
        .expect("insert song 1");
        db.execute(
            "INSERT INTO songs (id, album_id, title, duration_ms) VALUES (101, 10, 's2', 2000)",
        )
        .expect("insert song 2");
        db.execute(
            "INSERT INTO songs (id, album_id, title, duration_ms) VALUES (200, 20, 's3', 3000)",
        )
        .expect("insert song 3");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db
        .inspect_storage_state_json()
        .expect("json before view query");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected view base tables to start deferred, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":3"),
        "expected all view base tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute_with_params(
            "SELECT album_title, song_title, duration_ms \
                 FROM v_artist_songs WHERE artist_id = $1",
            &[Value::Int64(1)],
        )
        .expect("view filtered join query");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("a1".to_string()),
            Value::Text("s1".to_string()),
            Value::Int64(1000)
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Text("a1".to_string()),
            Value::Text("s2".to_string()),
            Value::Int64(2000)
        ]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after view query");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected deferred view filtered join to avoid resident table materialization, got: {json_after}"
        );
    assert!(
        json_after.contains("\"deferred_table_count\":3"),
        "expected deferred view filtered join to leave base tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_view_limit_indexed_join_chain_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-view-limit-join-chain.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE artists (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create artists");
        db.execute(
            "CREATE TABLE albums (id INTEGER PRIMARY KEY, artist_id INTEGER NOT NULL, title TEXT)",
        )
        .expect("create albums");
        db.execute(
                "CREATE TABLE songs (id INTEGER PRIMARY KEY, album_id INTEGER NOT NULL, title TEXT, duration_ms INTEGER NOT NULL)",
            )
            .expect("create songs");
        db.execute("CREATE INDEX idx_albums_artist ON albums (artist_id)")
            .expect("create albums artist index");
        db.execute("CREATE INDEX idx_songs_album ON songs (album_id)")
            .expect("create songs album index");
        db.execute(
            "CREATE VIEW v_artist_songs AS \
                 SELECT a.id AS artist_id, a.name AS artist_name, al.title AS album_title, \
                        s.title AS song_title, s.duration_ms AS duration_ms \
                 FROM artists a JOIN albums al ON al.artist_id = a.id \
                 JOIN songs s ON s.album_id = al.id",
        )
        .expect("create view");

        db.execute("INSERT INTO artists (id, name) VALUES (1, 'a')")
            .expect("insert artist 1");
        db.execute("INSERT INTO artists (id, name) VALUES (2, 'b')")
            .expect("insert artist 2");
        db.execute("INSERT INTO albums (id, artist_id, title) VALUES (10, 1, 'a1')")
            .expect("insert album 1");
        db.execute("INSERT INTO albums (id, artist_id, title) VALUES (20, 2, 'b1')")
            .expect("insert album 2");
        db.execute(
            "INSERT INTO songs (id, album_id, title, duration_ms) VALUES (100, 10, 's1', 1000)",
        )
        .expect("insert song 1");
        db.execute(
            "INSERT INTO songs (id, album_id, title, duration_ms) VALUES (101, 10, 's2', 2000)",
        )
        .expect("insert song 2");
        db.execute(
            "INSERT INTO songs (id, album_id, title, duration_ms) VALUES (200, 20, 's3', 3000)",
        )
        .expect("insert song 3");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT artist_id, artist_name, album_title, song_title \
                 FROM v_artist_songs LIMIT 2",
        )
        .expect("view limit query");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("a".to_string()),
            Value::Text("a1".to_string()),
            Value::Text("s1".to_string())
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Text("a".to_string()),
            Value::Text("a1".to_string()),
            Value::Text("s2".to_string())
        ]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after view limit query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected deferred view limit to avoid resident table materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":3"),
        "expected deferred view limit to leave base tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_indexed_join_order_limit_offset_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-order-limit-offset.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("note-{i:02}")),
                    ],
                )
                .expect("insert archive row");
        }
        archive_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1001),
                    Value::Int64(8),
                    Value::Text("note-z".to_string()),
                ],
            )
            .expect("insert duplicate archive row");
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both join tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8 \
                 ORDER BY archive.note DESC \
                 LIMIT 1 OFFSET 1",
        )
        .expect("indexed join projection query with ordering");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(8), Value::Text("note-07".to_string())]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join ordering path to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join ordering path to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_join_multi_order_by_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-multi-order.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        db.execute("INSERT INTO docs (id, body) VALUES (8, 'doc-8')")
            .expect("insert doc row");
        for (id, note) in [(1000, "note-z"), (1001, "note-z"), (1002, "note-a")] {
            db.execute(&format!(
                "INSERT INTO archive (id, doc_id, note) VALUES ({id}, 8, '{note}')"
            ))
            .expect("insert archive row");
        }
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT docs.id, archive.id AS archive_id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8 \
                 ORDER BY archive.note DESC, archive_id ASC",
        )
        .expect("indexed join projection query with multi-order");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Int64(1000),
            Value::Text("note-z".to_string())
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(8),
            Value::Int64(1001),
            Value::Text("note-z".to_string())
        ]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[
            Value::Int64(8),
            Value::Int64(1002),
            Value::Text("note-a".to_string())
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join multi-order path to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join multi-order path to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_join_distinct_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-distinct.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        db.execute("INSERT INTO docs (id, body) VALUES (8, 'doc-8')")
            .expect("insert doc row");
        for (id, note) in [(1000, "note-z"), (1001, "note-z"), (1002, "note-a")] {
            db.execute(&format!(
                "INSERT INTO archive (id, doc_id, note) VALUES ({id}, 8, '{note}')"
            ))
            .expect("insert archive row");
        }
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT DISTINCT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8 \
                 ORDER BY note DESC",
        )
        .expect("indexed join projection query with distinct");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(8), Value::Text("note-z".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(8), Value::Text("note-a".to_string())]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join distinct path to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join distinct path to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_join_using_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-using.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX archive_id_idx ON archive(id)")
            .expect("create archive index");
        db.execute("INSERT INTO docs (id, body) VALUES (8, 'doc-8')")
            .expect("insert doc row");
        db.execute("INSERT INTO archive (id, note) VALUES (8, 'note-z')")
            .expect("insert archive row");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive USING (id) \
                 WHERE docs.id = 8 \
                 ORDER BY archive.note DESC",
        )
        .expect("indexed join using query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(8), Value::Text("note-z".to_string())]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join using path to avoid resident table materialization, got: {json_after}"
        );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected indexed join using path to leave both paged tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_indexed_join_using_wildcard_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-using-wildcard.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX archive_id_idx ON archive(id)")
            .expect("create archive index");
        db.execute("INSERT INTO docs (id, body) VALUES (8, 'doc-8')")
            .expect("insert doc row");
        db.execute("INSERT INTO archive (id, note) VALUES (8, 'note-z')")
            .expect("insert archive row");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT * \
                 FROM docs \
                 JOIN archive USING (id) \
                 WHERE docs.id = 8 \
                 ORDER BY note DESC",
        )
        .expect("indexed join using wildcard query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Text("doc-8".to_string()),
            Value::Text("note-z".to_string()),
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join using wildcard to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join using wildcard to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_join_multi_column_using_wildcard_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-multi-column-using-wildcard.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute(
            "CREATE TABLE docs (pk INTEGER PRIMARY KEY, org_id INTEGER, id INTEGER, body TEXT)",
        )
        .expect("create docs");
        db.execute(
                "CREATE TABLE archive (archive_pk INTEGER PRIMARY KEY, org_id INTEGER, id INTEGER, note TEXT)",
            )
            .expect("create archive");
        db.execute("CREATE INDEX archive_org_id_id_idx ON archive(org_id, id)")
            .expect("create archive index");
        db.execute("INSERT INTO docs (pk, org_id, id, body) VALUES (1, 7, 8, 'doc-8')")
            .expect("insert doc row");
        db.execute("INSERT INTO archive (archive_pk, org_id, id, note) VALUES (1, 7, 8, 'note-z')")
            .expect("insert archive row");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT * \
                 FROM docs \
                 JOIN archive USING (org_id, id) \
                 ORDER BY note DESC",
        )
        .expect("indexed join multi-column using wildcard query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(7),
            Value::Int64(8),
            Value::Int64(1),
            Value::Text("doc-8".to_string()),
            Value::Int64(1),
            Value::Text("note-z".to_string()),
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join multi-column using wildcard to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join multi-column using wildcard to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_natural_join_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-natural-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute(
            "CREATE TABLE docs (pk INTEGER PRIMARY KEY, org_id INTEGER, id INTEGER, body TEXT)",
        )
        .expect("create docs");
        db.execute(
            "CREATE TABLE archive (pk INTEGER PRIMARY KEY, org_id INTEGER, id INTEGER, note TEXT)",
        )
        .expect("create archive");
        db.execute("CREATE INDEX archive_org_id_id_idx ON archive(org_id, id)")
            .expect("create archive index");
        db.execute("INSERT INTO docs (pk, org_id, id, body) VALUES (1, 7, 8, 'doc-8')")
            .expect("insert doc row");
        db.execute("INSERT INTO archive (pk, org_id, id, note) VALUES (1, 7, 8, 'note-z')")
            .expect("insert archive row");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT docs.pk, org_id, id, archive.note \
                 FROM docs NATURAL JOIN archive \
                 ORDER BY archive.note DESC",
        )
        .expect("indexed natural join query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Int64(7),
            Value::Int64(8),
            Value::Text("note-z".to_string()),
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected indexed natural join to avoid resident table materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected indexed natural join to leave both paged tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_indexed_join_without_filter_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-without-filter.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..32_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i + 1), Value::Text("x".repeat(2048))],
                )
                .expect("insert docs row");
            if i < 24 {
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("note-{i:02}")),
                        ],
                    )
                    .expect("insert archive row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 ORDER BY docs.id ASC \
                 LIMIT 3",
        )
        .expect("indexed join without filter query");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Text("note-00".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Text("note-01".to_string())]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(3), Value::Text("note-02".to_string())]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join without filter path to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join without filter path to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_composite_indexed_join_without_filter_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-composite-indexed-join-without-filter.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, org_id INTEGER, body TEXT)")
            .expect("create docs");
        db.execute(
                "CREATE TABLE archive (id INTEGER PRIMARY KEY, org_id INTEGER, doc_id INTEGER, note TEXT)",
            )
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_org_doc ON archive(org_id, doc_id)")
            .expect("create composite archive join index");
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3, $4)")
            .expect("prepare archive insert");
        for i in 0_i64..24_i64 {
            let doc_id = i + 1;
            let org_id = if i < 12 { 10 } else { 20 };
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(doc_id),
                        Value::Int64(org_id),
                        Value::Text("x".repeat(2048)),
                    ],
                )
                .expect("insert docs row");
            if i < 18 {
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(1000 + i),
                            Value::Int64(org_id),
                            Value::Int64(doc_id),
                            Value::Text(format!("note-{i:02}")),
                        ],
                    )
                    .expect("insert archive row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected composite join tables to stay deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id \
                 ORDER BY docs.id ASC \
                 LIMIT 3",
        )
        .expect("composite indexed join without filter query");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Text("note-00".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Text("note-01".to_string())]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(3), Value::Text("note-02".to_string())]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected composite indexed join without filter path to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected composite indexed join without filter path to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_composite_indexed_join_with_filter_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-composite-indexed-join-with-filter.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, org_id INTEGER, body TEXT)")
            .expect("create docs");
        db.execute(
                "CREATE TABLE archive (id INTEGER PRIMARY KEY, org_id INTEGER, doc_id INTEGER, note TEXT)",
            )
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_org_doc ON archive(org_id, doc_id)")
            .expect("create composite archive join index");
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3, $4)")
            .expect("prepare archive insert");
        for i in 0_i64..16_i64 {
            let doc_id = i + 1;
            let org_id = if i < 8 { 10 } else { 20 };
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(doc_id),
                        Value::Int64(org_id),
                        Value::Text("x".repeat(1024)),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1000 + i),
                        Value::Int64(org_id),
                        Value::Int64(doc_id),
                        Value::Text(format!("note-{i:02}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id \
                 WHERE docs.id = 2",
        )
        .expect("composite indexed join with filter query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Text("note-01".to_string())]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected composite indexed join with filter path to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected composite indexed join with filter path to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_composite_hashed_join_with_filter_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-composite-hashed-join-with-filter.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, org_id INTEGER, body TEXT)")
            .expect("create docs");
        db.execute(
                "CREATE TABLE archive (id INTEGER PRIMARY KEY, org_id INTEGER, doc_id INTEGER, note TEXT)",
            )
            .expect("create archive");
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3, $4)")
            .expect("prepare archive insert");
        for i in 0_i64..16_i64 {
            let doc_id = i + 1;
            let org_id = if i < 8 { 10 } else { 20 };
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(doc_id),
                        Value::Int64(org_id),
                        Value::Text("x".repeat(1024)),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(1000 + i),
                        Value::Int64(org_id),
                        Value::Int64(doc_id),
                        Value::Text(format!("note-{i:02}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.org_id = archive.org_id AND docs.id = archive.doc_id \
                 WHERE docs.id = 2",
        )
        .expect("composite hashed join with filter query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Text("note-01".to_string())]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected composite hashed join with filter path to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected composite hashed join with filter path to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_left_join_without_filter_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-left-join-without-filter.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..16_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i + 1), Value::Text("x".repeat(1024))],
                )
                .expect("insert docs row");
            if i < 8 {
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("note-{i:02}")),
                        ],
                    )
                    .expect("insert archive row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 LEFT JOIN archive ON docs.id = archive.doc_id \
                 ORDER BY docs.id DESC \
                 LIMIT 1",
        )
        .expect("indexed left join without filter query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(16), Value::Null]);

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed left join without filter path to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed left join without filter path to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_join_expression_projection_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-expression.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..32_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("note-{i:02}")),
                    ],
                )
                .expect("insert archive row");
        }
        archive_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1001),
                    Value::Int64(8),
                    Value::Text("note-z".to_string()),
                ],
            )
            .expect("insert duplicate archive row");
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT docs.id, UPPER(archive.note) AS note_key \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8 \
                 ORDER BY note_key DESC \
                 LIMIT 1 OFFSET 1",
        )
        .expect("indexed join expression projection query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(8), Value::Text("NOTE-07".to_string())]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join expression projection to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join expression projection to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_join_wildcard_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-wildcard.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT * \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8",
        )
        .expect("indexed join wildcard query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Int64(7),
            Value::Text("x".repeat(2048)),
            Value::Int64(8),
            Value::Int64(8),
            Value::Text(format!("{}-7", "y".repeat(1024))),
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected indexed join wildcard to avoid resident table materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected indexed join wildcard to leave both paged tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_indexed_join_qualified_wildcard_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-qualified-wildcard.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT archive.* \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 8",
        )
        .expect("indexed join qualified wildcard query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Int64(8),
            Value::Text(format!("{}-7", "y".repeat(1024))),
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join qualified wildcard to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join qualified wildcard to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_left_join_projection_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-left-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            if i < 48 {
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("{archive_note}-{i}")),
                        ],
                    )
                    .expect("insert archive row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both join tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 LEFT JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 96",
        )
        .expect("indexed left join projection query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(96), Value::Null]);

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed left join projection to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed left join projection to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_left_join_expression_projection_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-left-join-expression.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..32_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            if i < 16 {
                archive_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(format!("note-{i:02}")),
                        ],
                    )
                    .expect("insert archive row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
        .execute(
            "SELECT docs.id, COALESCE(archive.note, 'missing') AS note_key \
                 FROM docs \
                 LEFT JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id = 32",
        )
        .expect("indexed left join expression projection query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(32), Value::Text("missing".to_string())]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed left join expression projection to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed left join expression projection to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_indexed_right_join_projection_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-right-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        archive_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1001),
                    Value::Int64(999),
                    Value::Text("orphan".to_string()),
                ],
            )
            .expect("insert unmatched archive row");
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both join tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.n, archive.id \
                 FROM docs \
                 RIGHT JOIN archive ON docs.id = archive.doc_id \
                 WHERE archive.id = 1001",
        )
        .expect("indexed right join projection query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Null, Value::Int64(1001)]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed right join projection to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed right join projection to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_generic_join_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-generic-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both join tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id >= 8 AND docs.id < 10 \
                 ORDER BY docs.id",
        )
        .expect("generic join query");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(8),
            Value::Text(format!("{}-7", "y".repeat(1024)))
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(9),
            Value::Text(format!("{}-8", "y".repeat(1024)))
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic join execution to avoid resident table materialization, got: {json_after}"
        );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected generic join execution to leave both paged tables deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after generic join execution, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_indexed_join_expression_filter_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-expression-filter.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both join tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id + archive.doc_id >= 18 AND docs.id <= 10 \
                 ORDER BY docs.id",
        )
        .expect("indexed join expression filter query");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(9),
            Value::Text(format!("{}-8", "y".repeat(1024)))
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(10),
            Value::Text(format!("{}-9", "y".repeat(1024)))
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed join expression filter to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed join expression filter to leave both paged tables deferred, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after indexed join expression filter, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_hashed_join_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-hashed-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both join tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.note \
                 FROM docs \
                 JOIN archive ON docs.id = archive.doc_id \
                 WHERE docs.id + archive.doc_id >= 18 AND docs.id <= 10 \
                 ORDER BY docs.id",
        )
        .expect("hashed join query");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(9),
            Value::Text(format!("{}-8", "y".repeat(1024)))
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(10),
            Value::Text(format!("{}-9", "y".repeat(1024)))
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected hashed join to avoid resident table materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected hashed join to leave both paged tables deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after hashed join, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_generic_right_join_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-generic-right-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX archive_doc_idx ON archive (doc_id)")
            .expect("create archive doc index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(format!("{archive_note}-{i}")),
                    ],
                )
                .expect("insert archive row");
        }
        archive_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1001),
                    Value::Int64(999),
                    Value::Text("orphan".to_string()),
                ],
            )
            .expect("insert unmatched archive row");
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both join tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.id \
                 FROM docs \
                 RIGHT JOIN archive ON docs.id = archive.doc_id \
                 WHERE archive.id >= 1000 \
                 ORDER BY archive.id",
        )
        .expect("generic right join query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Null, Value::Int64(1001)]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic right join execution to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected generic right join execution to leave both paged tables deferred, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after generic right join execution, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_indexed_full_join_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-full-join.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, doc_id INTEGER, note TEXT)")
            .expect("create archive");
        db.execute("CREATE INDEX idx_archive_doc_id ON archive(doc_id)")
            .expect("create archive join index");
        let docs_body = "x".repeat(2048);
        let archive_note = "y".repeat(1024);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..2_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(docs_body.clone()),
                    ],
                )
                .expect("insert docs row");
        }
        archive_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1000),
                    Value::Int64(1),
                    Value::Text(format!("{archive_note}-0")),
                ],
            )
            .expect("insert archive row");
        archive_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1001),
                    Value::Int64(99),
                    Value::Text("orphan".to_string()),
                ],
            )
            .expect("insert unmatched archive row");
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before join");
    assert!(
        json_before.contains("\"loaded_table_count\":0"),
        "expected join tables to stay deferred at reopen, got: {json_before}"
    );
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both join tables deferred at reopen, got: {json_before}"
    );

    let result = db
        .execute(
            "SELECT docs.id, archive.note, COALESCE(docs.id, archive.doc_id) AS sort_key \
                 FROM docs \
                 FULL JOIN archive ON docs.id = archive.doc_id \
                 ORDER BY sort_key",
        )
        .expect("indexed full join projection query");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text(format!("{}-0", "y".repeat(1024))),
            Value::Int64(1)
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(2), Value::Null, Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[
            Value::Null,
            Value::Text("orphan".to_string()),
            Value::Int64(99)
        ]
    );

    let json_after = db.inspect_storage_state_json().expect("json after join");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected indexed full join projection to avoid resident table materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected indexed full join projection to leave both paged tables deferred, got: {json_after}"
        );
}

#[test]
fn paged_row_storage_benchmark_history_query_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-benchmark-history-query.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute(
            "CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    total_amount FLOAT64,
                    body TEXT
                )",
        )
        .expect("create orders");
        db.execute(
            "CREATE TABLE payments (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    status TEXT,
                    body TEXT
                )",
        )
        .expect("create payments");
        db.execute(
            "CREATE TABLE order_items (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    item_id INTEGER,
                    quantity INTEGER,
                    price FLOAT64,
                    body TEXT
                )",
        )
        .expect("create order_items");
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, body TEXT)")
            .expect("create items");
        db.execute("CREATE INDEX idx_orders_user_id ON orders(user_id)")
            .expect("create orders user index");
        db.execute("CREATE INDEX idx_payments_order_id ON payments(order_id)")
            .expect("create payments order index");
        db.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id)")
            .expect("create order_items order index");
        db.execute("CREATE INDEX idx_items_id ON items(id)")
            .expect("create items id index");

        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let order_insert = txn
            .prepare("INSERT INTO orders VALUES ($1, $2, $3, $4)")
            .expect("prepare order insert");
        let payment_insert = txn
            .prepare("INSERT INTO payments VALUES ($1, $2, $3, $4)")
            .expect("prepare payment insert");
        let order_item_insert = txn
            .prepare("INSERT INTO order_items VALUES ($1, $2, $3, $4, $5, $6)")
            .expect("prepare order item insert");
        let item_insert = txn
            .prepare("INSERT INTO items VALUES ($1, $2, $3)")
            .expect("prepare item insert");

        item_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1),
                    Value::Text("widget".to_string()),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert item 1");
        item_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(2),
                    Value::Text("gizmo".to_string()),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert item 2");

        order_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(10),
                    Value::Int64(7),
                    Value::Float64(42.0),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert order 10");
        order_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(11),
                    Value::Int64(7),
                    Value::Float64(84.0),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert order 11");

        payment_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1),
                    Value::Int64(10),
                    Value::Text("paid".to_string()),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert payment 1");
        payment_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(2),
                    Value::Int64(11),
                    Value::Text("paid".to_string()),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert payment 2");

        order_item_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1),
                    Value::Int64(10),
                    Value::Int64(1),
                    Value::Int64(2),
                    Value::Float64(5.0),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert order item 1");
        order_item_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(2),
                    Value::Int64(11),
                    Value::Int64(2),
                    Value::Int64(3),
                    Value::Float64(7.5),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert order item 2");
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
            .execute(
                "SELECT orders.id, orders.total_amount, payments.status, items.name, order_items.quantity, order_items.price \
                 FROM ((orders JOIN payments ON orders.id = payments.order_id) \
                 JOIN order_items ON orders.id = order_items.order_id) \
                 JOIN items ON order_items.item_id = items.id \
                 WHERE orders.user_id = 7 \
                 ORDER BY orders.id DESC",
            )
            .expect("benchmark history query");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(11),
            Value::Float64(84.0),
            Value::Text("paid".to_string()),
            Value::Text("gizmo".to_string()),
            Value::Int64(3),
            Value::Float64(7.5),
        ]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after history query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected benchmark history query to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":4"),
        "expected benchmark history query to keep all tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_benchmark_history_query_wildcard_projection_expands_in_standard_path() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-benchmark-history-query-wildcard.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    let db = Db::open_or_create(&path, config.clone()).expect("open db");
    db.execute(
        "CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    total_amount FLOAT64,
                    body TEXT
                )",
    )
    .expect("create orders");
    db.execute(
        "CREATE TABLE payments (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    status TEXT,
                    body TEXT
                )",
    )
    .expect("create payments");
    db.execute(
        "CREATE TABLE order_items (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    item_id INTEGER,
                    quantity INTEGER,
                    price FLOAT64,
                    body TEXT
                )",
    )
    .expect("create order_items");
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, body TEXT)")
        .expect("create items");
    db.execute("CREATE INDEX idx_orders_user_id ON orders(user_id)")
        .expect("create orders user index");
    db.execute("CREATE INDEX idx_payments_order_id ON payments(order_id)")
        .expect("create payments order index");
    db.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id)")
        .expect("create order_items order index");
    db.execute("CREATE INDEX idx_items_id ON items(id)")
        .expect("create items id index");

    db.execute("INSERT INTO items VALUES (1, 'widget', 'x')")
        .expect("insert item");
    db.execute("INSERT INTO orders VALUES (10, 7, 42.0, 'x')")
        .expect("insert order");
    db.execute("INSERT INTO payments VALUES (1, 10, 'paid', 'x')")
        .expect("insert payment");
    db.execute("INSERT INTO order_items VALUES (1, 10, 1, 2, 5.0, 'x')")
        .expect("insert order item");

    let result = db
        .execute(
            "SELECT orders.*, orders.total_amount, payments.status, items.name, order_items.quantity, order_items.price \
                 FROM ((orders JOIN payments ON orders.id = payments.order_id) \
                 JOIN order_items ON orders.id = order_items.order_id) \
                 JOIN items ON order_items.item_id = items.id \
                 WHERE orders.user_id = 7 \
                 ORDER BY orders.id DESC",
        )
        .expect("wildcard benchmark history query");

    assert_eq!(result.columns().len(), 9);
    assert_eq!(result.rows().len(), 1);
}

#[test]
fn paged_row_storage_benchmark_report_query_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-benchmark-report-query.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, body TEXT)")
            .expect("create items");
        db.execute(
            "CREATE TABLE order_items (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    item_id INTEGER,
                    quantity INTEGER,
                    price FLOAT64,
                    body TEXT
                )",
        )
        .expect("create order_items");
        db.execute(
            "CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    status TEXT,
                    body TEXT
                )",
        )
        .expect("create orders");
        db.execute("CREATE INDEX idx_orders_status ON orders(status)")
            .expect("create orders status index");
        db.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id)")
            .expect("create order_items order index");
        db.execute("CREATE INDEX idx_items_id ON items(id)")
            .expect("create items id index");

        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let item_insert = txn
            .prepare("INSERT INTO items VALUES ($1, $2, $3)")
            .expect("prepare item insert");
        let order_insert = txn
            .prepare("INSERT INTO orders VALUES ($1, $2, $3)")
            .expect("prepare order insert");
        let order_item_insert = txn
            .prepare("INSERT INTO order_items VALUES ($1, $2, $3, $4, $5, $6)")
            .expect("prepare order item insert");

        item_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1),
                    Value::Text("widget".to_string()),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert item 1");
        item_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(2),
                    Value::Text("gizmo".to_string()),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert item 2");

        order_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(10),
                    Value::Text("paid".to_string()),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert order 10");
        order_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(11),
                    Value::Text("paid".to_string()),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert order 11");

        order_item_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1),
                    Value::Int64(10),
                    Value::Int64(1),
                    Value::Int64(2),
                    Value::Float64(5.0),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert order item 1");
        order_item_insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(2),
                    Value::Int64(11),
                    Value::Int64(2),
                    Value::Int64(3),
                    Value::Float64(7.5),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert order item 2");
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let result = db
            .execute(
                "SELECT items.name, SUM(order_items.quantity) AS total_quantity, SUM(order_items.quantity * order_items.price) AS revenue \
                 FROM ((items JOIN order_items ON items.id = order_items.item_id) \
                 JOIN orders ON order_items.order_id = orders.id) \
                 WHERE orders.status = 'paid' \
                 GROUP BY items.id, items.name \
                 ORDER BY revenue DESC \
                 LIMIT 2",
            )
            .expect("benchmark report query");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("gizmo".to_string()),
            Value::Int64(3),
            Value::Float64(22.5),
        ]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after report query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected benchmark report query to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":3"),
        "expected benchmark report query to keep all tables deferred, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_generic_literal_update_after_reopen_keeps_table_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-generic-literal-update.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert docs row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("UPDATE docs SET n = 777 WHERE n >= 90")
        .expect("generic literal update");

    let json_after = db.inspect_storage_state_json().expect("json after update");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected generic literal update to re-defer docs after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected docs to be deferred again after generic literal update, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs WHERE n = 777")
                .expect("count updated rows")
        ),
        6
    );
}

#[test]
fn paged_row_storage_generic_expression_update_after_reopen_keeps_table_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-generic-expression-update.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert docs row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("UPDATE docs SET n = n + 10 WHERE n >= 90")
        .expect("generic expression update");

    let json_after = db.inspect_storage_state_json().expect("json after update");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected generic expression update to re-defer docs after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected docs to be deferred again after generic expression update, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT n FROM docs WHERE id = 91")
                .expect("read updated docs row")
        ),
        100
    );
}

#[test]
fn paged_row_storage_generic_foreign_key_update_after_reopen_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-generic-foreign-key-update.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create parent");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), body TEXT)",
            )
            .expect("create child");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        for i in 0_i64..64_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                )
                .expect("insert parent row");
            if i < 32 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("UPDATE child SET parent_id = parent_id + 1 WHERE id = 1")
        .expect("generic foreign-key update");

    let json_after = db.inspect_storage_state_json().expect("json after update");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected generic foreign-key update to re-defer loaded tables, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected parent and child tables to remain deferred, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT parent_id FROM child WHERE id = 1")
                .expect("read updated child row")
        ),
        2
    );
}

#[test]
fn paged_row_storage_shared_sql_transaction_generic_foreign_key_update_keeps_targeted_runtime_state(
) {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-shared-sql-transaction-foreign-key-update.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create parent");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), body TEXT)",
            )
            .expect("create child");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("seed txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        for i in 0_i64..96_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                )
                .expect("insert parent row");
            if i < 48 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.begin_transaction()
        .expect("begin shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert!(
            state.runtime.tables.is_empty(),
            "expected no transaction-local table loads at BEGIN"
        );
        assert_eq!(
            state.runtime.deferred_tables.len(),
            2,
            "expected parent and child tables deferred at BEGIN"
        );
    }

    db.execute("UPDATE child SET parent_id = parent_id + 1 WHERE id = 1")
        .expect("generic foreign-key update inside shared sql transaction");
    {
        let txn = db.inner.sql_txn.lock().expect("lock shared txn slot");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert_eq!(
                state.runtime.tables.len(),
                2,
                "expected transaction runtime to load child and parent tables for generic foreign-key update validation"
            );
        assert!(
            state.runtime.tables.contains_key("child"),
            "expected child table to be loaded for generic foreign-key update"
        );
        assert!(
            state.runtime.tables.contains_key("parent"),
            "expected parent table to be loaded for generic foreign-key update validation"
        );
    }
    db.commit_transaction().expect("commit shared txn");

    let reopened = Db::open_or_create(
        &path,
        DbConfig {
            paged_row_storage: true,
            ..DbConfig::default()
        },
    )
    .expect("reopen committed db");
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT parent_id FROM child WHERE id = 1")
                .expect("child after shared txn commit")
        ),
        2
    );
}

#[test]
fn paged_row_storage_generic_delete_after_reopen_keeps_table_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-generic-delete.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert docs row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("DELETE FROM docs WHERE n >= 90 AND n < 93")
        .expect("generic delete");

    let json_after = db.inspect_storage_state_json().expect("json after delete");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected generic delete to re-defer docs after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected docs to be deferred again after generic delete, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        93
    );
}

#[test]
fn paged_row_storage_generic_parent_key_update_with_setnull_fk_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-parent-key-update-setnull-fk.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, code INTEGER, body TEXT)")
            .expect("create parent");
        db.execute("CREATE UNIQUE INDEX parent_code_idx ON parent(code)")
            .expect("create parent code index");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_code INTEGER REFERENCES parent(code) ON UPDATE SET NULL, body TEXT)",
            )
            .expect("create child");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        for i in 0_i64..96_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i + 1),
                        Value::Text(parent_body.clone()),
                    ],
                )
                .expect("insert parent row");
            if i < 48 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("UPDATE parent SET code = code + 1000 WHERE id = 1")
        .expect("generic parent-key update");

    let json_after = db.inspect_storage_state_json().expect("json after update");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected generic parent-key update with setnull fk to re-defer loaded tables, got: {json_after}"
        );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected both parent and child tables to remain deferred, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT code FROM parent WHERE id = 1")
                .expect("read updated parent code")
        ),
        1001
    );
    let child_parent_code = db
        .execute("SELECT parent_code FROM child WHERE id = 1")
        .expect("read updated child row")
        .rows()
        .first()
        .and_then(|row| row.values().first())
        .cloned()
        .expect("child parent code");
    assert!(matches!(child_parent_code, Value::Null));
}

#[test]
fn paged_row_storage_generic_delete_with_restrict_fk_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-generic-delete-restrict-fk.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create parent");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE RESTRICT, body TEXT)",
            )
            .expect("create child");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        for i in 0_i64..96_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                )
                .expect("insert parent row");
            if i < 48 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("DELETE FROM parent WHERE id = 96")
        .expect("generic delete with restrict fk");

    let json_after = db.inspect_storage_state_json().expect("json after delete");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected generic delete with restrict fk to re-defer loaded tables, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected both parent and child tables to remain deferred, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM parent")
                .expect("count parent rows")
        ),
        95
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM child")
                .expect("count child rows")
        ),
        48
    );
}

#[test]
fn paged_row_storage_generic_delete_with_composite_restrict_fk_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-generic-delete-composite-restrict-fk.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (a INTEGER, b INTEGER, body TEXT, PRIMARY KEY(a, b))")
            .expect("create parent");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_a INTEGER, parent_b INTEGER, body TEXT, \
                 FOREIGN KEY(parent_a, parent_b) REFERENCES parent(a, b) ON DELETE RESTRICT)",
            )
            .expect("create child");
        db.execute("CREATE INDEX parent_a_idx ON parent(a)")
            .expect("create parent lookup index");
        db.execute("CREATE INDEX child_parent_ab_idx ON child(parent_a, parent_b)")
            .expect("create child composite fk index");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3, $4)")
            .expect("prepare child insert");
        for i in 0_i64..96_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(1000 + i),
                        Value::Text(parent_body.clone()),
                    ],
                )
                .expect("insert parent row");
            if i < 48 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Int64(1000 + i),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("DELETE FROM parent WHERE a = 96")
        .expect("composite restrict delete");

    let json_after = db.inspect_storage_state_json().expect("json after delete");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected composite restrict delete to re-defer loaded tables, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected both parent and child tables to remain deferred, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM parent")
                .expect("count parent rows")
        ),
        95
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM child")
                .expect("count child rows")
        ),
        48
    );
}

#[test]
fn paged_row_storage_composite_foreign_key_insert_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-composite-foreign-key-insert.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (a INTEGER, b INTEGER, body TEXT, PRIMARY KEY(a, b))")
            .expect("create parent");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_a INTEGER, parent_b INTEGER, body TEXT, \
                 FOREIGN KEY(parent_a, parent_b) REFERENCES parent(a, b) ON DELETE RESTRICT)",
            )
            .expect("create child");
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2, $3)")
            .expect("prepare parent insert");
        insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(1),
                    Value::Int64(2),
                    Value::Text("p".repeat(2048)),
                ],
            )
            .expect("insert parent row");
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("INSERT INTO child VALUES (1, 1, 2, 'child')")
        .expect("insert child row");

    let json_after = db.inspect_storage_state_json().expect("json after insert");
    assert!(
            !json_after.contains("\"loaded_table_count\":2"),
            "expected composite foreign-key insert to avoid loading both parent and child resident at once, got: {json_after}"
        );
    assert!(
        json_after.contains("\"deferred_table_count\":1")
            || json_after.contains("\"deferred_table_count\":2"),
        "expected at least one table to remain deferred after insert, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM child")
                .expect("count child rows")
        ),
        1
    );
}

#[test]
fn paged_row_storage_generic_delete_with_cascade_fk_keeps_tables_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-generic-delete-cascade-fk.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create parent");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE, body TEXT)",
            )
            .expect("create child");
        db.execute(
                "CREATE TABLE grandchild (id INTEGER PRIMARY KEY, child_id INTEGER REFERENCES child(id) ON DELETE CASCADE, body TEXT)",
            )
            .expect("create grandchild");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let grandchild_body = "g".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        let grandchild_insert = txn
            .prepare("INSERT INTO grandchild VALUES ($1, $2, $3)")
            .expect("prepare grandchild insert");
        for i in 0_i64..96_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                )
                .expect("insert parent row");
            if i < 48 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
                grandchild_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(grandchild_body.clone()),
                        ],
                    )
                    .expect("insert grandchild row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("DELETE FROM parent WHERE id = 1")
        .expect("generic delete with cascade fk");

    let json_after = db.inspect_storage_state_json().expect("json after delete");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected generic delete with cascade fk to re-defer loaded tables, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":3"),
        "expected parent, child, and grandchild tables to remain deferred, got: {json_after}"
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM parent")
                .expect("count parent rows")
        ),
        95
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM child")
                .expect("count child rows")
        ),
        47
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM grandchild")
                .expect("count grandchild rows")
        ),
        47
    );
}

#[test]
fn paged_row_storage_generic_delete_with_restrict_fk_violation_redefers_tables() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-generic-delete-restrict-fk-violation.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, body TEXT)")
            .expect("create parent");
        db.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE RESTRICT, body TEXT)",
            )
            .expect("create child");
        let parent_body = "p".repeat(2048);
        let child_body = "c".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let parent_insert = txn
            .prepare("INSERT INTO parent VALUES ($1, $2)")
            .expect("prepare parent insert");
        let child_insert = txn
            .prepare("INSERT INTO child VALUES ($1, $2, $3)")
            .expect("prepare child insert");
        for i in 0_i64..96_i64 {
            parent_insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i + 1), Value::Text(parent_body.clone())],
                )
                .expect("insert parent row");
            if i < 48 {
                child_insert
                    .execute_in(
                        &mut txn,
                        &[
                            Value::Int64(i + 1),
                            Value::Int64(i + 1),
                            Value::Text(child_body.clone()),
                        ],
                    )
                    .expect("insert child row");
            }
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let err = db
        .execute("DELETE FROM parent WHERE id = 1")
        .expect_err("restrict fk delete should fail");
    assert!(
        err.to_string().contains("violates a foreign key"),
        "expected fk violation, got: {err}"
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after failed delete");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected failed generic delete with restrict fk to re-defer loaded tables, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both parent and child tables to remain deferred after failed delete, got: {json_after}"
        );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM parent")
                .expect("count parent rows")
        ),
        96
    );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM child")
                .expect("count child rows")
        ),
        48
    );
}

#[test]
fn generic_direct_update_after_reopen_only_loads_referenced_deferred_table() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("generic-direct-update-targeted-load.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create archive");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before update");
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both tables deferred at reopen, got: {json_before}"
    );

    db.execute("UPDATE docs SET n = n + 1 WHERE id = 6")
        .expect("generic direct update");

    let json_after = db.inspect_storage_state_json().expect("json after update");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected generic expression update to avoid resident table loads, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both paged tables to remain deferred after generic expression update, got: {json_after}"
        );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT n FROM docs WHERE id = 6")
                .expect("read updated docs row")
        ),
        6
    );
}

#[test]
fn generic_prepared_update_after_reopen_only_loads_referenced_deferred_table() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("generic-prepared-update-targeted-load.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create docs");
        db.execute("CREATE TABLE archive (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create archive");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let docs_insert = txn
            .prepare("INSERT INTO docs VALUES ($1, $2, $3)")
            .expect("prepare docs insert");
        let archive_insert = txn
            .prepare("INSERT INTO archive VALUES ($1, $2, $3)")
            .expect("prepare archive insert");
        for i in 0_i64..96_i64 {
            docs_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert docs row");
            archive_insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert archive row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    let json_before = db.inspect_storage_state_json().expect("json before update");
    assert!(
        json_before.contains("\"deferred_table_count\":2"),
        "expected both tables deferred at reopen, got: {json_before}"
    );

    let prepared = db
        .prepare("UPDATE docs SET n = n + 1 WHERE id = $1")
        .expect("prepare generic update");
    prepared
        .execute(&[Value::Int64(6)])
        .expect("execute generic prepared update");

    let json_after = db.inspect_storage_state_json().expect("json after update");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected prepared generic expression update to avoid resident table loads, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":2"),
            "expected both paged tables to remain deferred after prepared expression update, got: {json_after}"
        );
    assert_eq!(
        scalar_i64(
            &db.execute("SELECT n FROM docs WHERE id = 6")
                .expect("read updated docs row")
        ),
        6
    );
}

#[test]
fn paged_row_storage_grouped_count_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-grouped-count.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 3),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT grp, COUNT(*) FROM seeded GROUP BY grp")
        .expect("grouped count");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(32)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(32)]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(2), Value::Int64(32)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped count");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged grouped count to avoid resident materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to remain deferred after grouped count, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged grouped count, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_indexed_join_grouped_count_keeps_deferred_tables_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-indexed-join-grouped-count.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE artists (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create artists");
        db.execute(
                "CREATE TABLE songs (id INTEGER PRIMARY KEY, artist_id INTEGER NOT NULL, title TEXT, body TEXT)",
            )
            .expect("create songs");
        db.execute("CREATE INDEX idx_songs_artist ON songs (artist_id)")
            .expect("create song artist index");
        db.execute("INSERT INTO artists (id, name) VALUES (1, 'a')")
            .expect("insert artist 1");
        db.execute("INSERT INTO artists (id, name) VALUES (2, 'b')")
            .expect("insert artist 2");
        let large_body = "x".repeat(2048);
        for (id, artist_id) in [(1, 1), (2, 1), (3, 2)] {
            db.execute_with_params(
                "INSERT INTO songs (id, artist_id, title, body) VALUES ($1, $2, $3, $4)",
                &[
                    Value::Int64(id),
                    Value::Int64(artist_id),
                    Value::Text(format!("s{id}")),
                    Value::Text(large_body.clone()),
                ],
            )
            .expect("insert song");
        }
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute(
            "SELECT a.id, a.name, COUNT(s.id) AS song_count \
                 FROM artists a JOIN songs s ON s.artist_id = a.id \
                 GROUP BY a.id, a.name ORDER BY song_count DESC LIMIT 10",
        )
        .expect("indexed grouped count");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("a".to_string()),
            Value::Int64(2)
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(2),
            Value::Text("b".to_string()),
            Value::Int64(1)
        ]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after indexed grouped count");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected indexed grouped count to use cloned row sources only, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected base tables to remain deferred after indexed grouped count, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_grouped_numeric_aggregate_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-grouped-sum.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 2),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT grp, COUNT(*), SUM(n) FROM seeded WHERE n >= 10 AND n <= 19 GROUP BY grp")
        .expect("grouped aggregate");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(5), Value::Int64(70)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(5), Value::Int64(75)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged grouped aggregate to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected paged-backed table to remain deferred after grouped aggregate, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged grouped aggregate, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_scalar_filtered_aggregate_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-scalar-filtered-aggregate.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    amount FLOAT64,
                    body TEXT
                )",
        )
        .expect("create orders");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO orders VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..128_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 4),
                        Value::Float64(i as f64 + 0.5),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let aggregate = db
        .prepare("SELECT COUNT(*), SUM(amount) FROM orders WHERE user_id = $1")
        .expect("prepare aggregate");
    assert!(
        aggregate.simple_scalar_filtered_aggregate.is_some(),
        "expected prepared scalar aggregate cache plan"
    );
    let result = aggregate
        .execute(&[Value::Int64(2)])
        .expect("scalar filtered aggregate");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(32), Value::Float64(2064.0)]
    );
    let plan = aggregate
        .simple_scalar_filtered_aggregate
        .as_ref()
        .expect("prepared scalar aggregate plan");
    assert_eq!(
        plan.cache.lock().expect("cache lock").entries.len(),
        1,
        "expected first aggregate execution to populate the cache"
    );
    let cached = aggregate
        .execute(&[Value::Int64(2)])
        .expect("cached scalar filtered aggregate");
    assert_eq!(cached.rows(), result.rows());
    assert_eq!(
        plan.cache.lock().expect("cache lock").entries.len(),
        1,
        "expected repeated aggregate execution to reuse the cached key"
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after scalar filtered aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected scalar filtered aggregate to keep table deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected scalar filtered aggregate to keep one deferred table, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after scalar filtered aggregate, got: {json_after}"
    );
}

#[test]
fn resident_scalar_filtered_aggregate_cache_invalidates_after_write() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("resident-scalar-filtered-aggregate-cache.ddb");
    let config = DbConfig {
        paged_row_storage: false,
        retain_paged_row_sources_after_commit: true,
        wal_checkpoint_threshold_pages: 0,
        wal_checkpoint_threshold_bytes: 0,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, config).expect("create db");
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount FLOAT64)")
        .expect("create orders");
    let mut txn = db.transaction().expect("begin txn");
    let insert = txn
        .prepare("INSERT INTO orders VALUES ($1, $2, $3)")
        .expect("prepare insert");
    for i in 0_i64..16_i64 {
        insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(i),
                    Value::Int64(i % 4),
                    Value::Float64(i as f64 + 0.5),
                ],
            )
            .expect("insert row");
    }
    txn.commit().expect("commit rows");

    let aggregate = db
        .prepare("SELECT COUNT(*), SUM(amount) FROM orders WHERE user_id = $1")
        .expect("prepare aggregate");
    let plan = aggregate
        .simple_scalar_filtered_aggregate
        .as_ref()
        .expect("prepared scalar aggregate plan");
    let result = aggregate
        .execute(&[Value::Int64(2)])
        .expect("resident aggregate");
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(4), Value::Float64(34.0)]
    );
    assert_eq!(
        plan.cache.lock().expect("cache lock").entries.len(),
        1,
        "expected resident aggregate execution to populate the cache"
    );
    let cached = aggregate
        .execute(&[Value::Int64(2)])
        .expect("cached resident aggregate");
    assert_eq!(cached.rows(), result.rows());
    assert_eq!(
        plan.cache.lock().expect("cache lock").entries.len(),
        1,
        "expected repeated resident aggregate execution to reuse the cache"
    );

    db.prepare("INSERT INTO orders VALUES ($1, $2, $3)")
        .expect("prepare autocommit insert")
        .execute(&[Value::Int64(100), Value::Int64(2), Value::Float64(10.0)])
        .expect("insert new matching row");
    let after_write = aggregate
        .execute(&[Value::Int64(2)])
        .expect("resident aggregate after write");
    assert_eq!(
        after_write.rows()[0].values(),
        &[Value::Int64(5), Value::Float64(44.0)]
    );
    assert_eq!(
        plan.cache.lock().expect("cache lock").entries.len(),
        2,
        "expected post-write aggregate to use a new snapshot cache key"
    );
}

#[test]
fn paged_row_storage_grouped_numeric_aggregate_with_order_limit_offset_keeps_deferred_table_unloaded(
) {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-grouped-sum-ordered.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 2),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c, SUM(n) AS total FROM seeded WHERE n >= 10 AND n <= 19 GROUP BY grp ORDER BY grp DESC LIMIT 1 OFFSET 1",
            )
            .expect("ordered grouped aggregate");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(5), Value::Int64(70)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered grouped aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected ordered paged grouped aggregate to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after ordered grouped aggregate, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after ordered paged grouped aggregate, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_grouped_numeric_aggregate_having_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-grouped-sum-having.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 2),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c, SUM(n) AS total FROM seeded WHERE n >= 10 AND n <= 19 GROUP BY grp HAVING c = 5 AND total > 70 ORDER BY total DESC LIMIT 1",
            )
            .expect("grouped aggregate with having");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(5), Value::Int64(75)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped aggregate with having");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged grouped aggregate with having to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped aggregate with having, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged grouped aggregate with having, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_grouped_avg_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-grouped-avg.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 2),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute(
            "SELECT grp, SUM(n) AS total, AVG(n) AS avg_n FROM seeded \
                 WHERE n >= 10 AND n <= 19 GROUP BY grp HAVING avg_n >= 14 \
                 ORDER BY avg_n DESC LIMIT 1",
        )
        .expect("grouped avg aggregate");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(75), Value::Float64(15.0)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped avg aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged grouped avg aggregate to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped avg aggregate, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged grouped avg aggregate, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_grouped_expression_bucket_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-grouped-expression-bucket.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 2),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute(
            "SELECT n / 5 AS bucket, COUNT(*) AS c, SUM(n) AS total FROM seeded \
                 WHERE n >= 10 AND n <= 19 GROUP BY n / 5 ORDER BY bucket",
        )
        .expect("grouped expression aggregate");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Int64(5), Value::Int64(60)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(3), Value::Int64(5), Value::Int64(85)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped expression aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged grouped expression aggregate to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped expression aggregate, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged grouped expression aggregate, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_grouped_wrapped_sum_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-grouped-wrapped-sum.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..32_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 2),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute(
            "SELECT grp, CAST(SUM(n) AS TEXT) AS total_text FROM seeded \
                 GROUP BY grp ORDER BY total_text DESC LIMIT 1",
        )
        .expect("grouped wrapped sum");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Text("256".to_string())]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped wrapped sum");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged grouped wrapped sum to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped wrapped sum, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged grouped wrapped sum, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_grouped_wrapped_max_expr_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-grouped-wrapped-max-expr.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, word TEXT, body TEXT)",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for (id, grp, word) in [
            (0_i64, 0_i64, "hi"),
            (1, 0, ""),
            (2, 1, "hello"),
            (3, 1, "zebra"),
        ] {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(id),
                        Value::Int64(grp),
                        Value::Text(word.to_string()),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute(
            "SELECT grp, LENGTH(MAX(NULLIF(word, ''))) AS longest FROM seeded \
                 GROUP BY grp ORDER BY longest DESC LIMIT 1",
        )
        .expect("grouped wrapped max expr");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(5)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped wrapped max expr");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged grouped wrapped max expr to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after grouped wrapped max expr, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged grouped wrapped max expr, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_multi_column_grouped_count_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-grouped-count-multi.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp_a INTEGER, grp_b INTEGER, body TEXT)",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 2),
                        Value::Int64(i % 3),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT grp_a, grp_b, COUNT(*) FROM seeded GROUP BY grp_a, grp_b")
        .expect("multi-column grouped count");
    assert_eq!(result.rows().len(), 6);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(0), Value::Int64(16)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(1), Value::Int64(16)]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(0), Value::Int64(2), Value::Int64(16)]
    );
    assert_eq!(
        result.rows()[3].values(),
        &[Value::Int64(1), Value::Int64(0), Value::Int64(16)]
    );
    assert_eq!(
        result.rows()[4].values(),
        &[Value::Int64(0), Value::Int64(1), Value::Int64(16)]
    );
    assert_eq!(
        result.rows()[5].values(),
        &[Value::Int64(1), Value::Int64(2), Value::Int64(16)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after multi-column grouped count");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected multi-column paged grouped count to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after multi-column grouped count, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after multi-column grouped count, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_multi_column_grouped_numeric_aggregate_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-grouped-sum-multi.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
                "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp_a INTEGER, grp_b INTEGER, n INTEGER, body TEXT)",
            )
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4, $5)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 2),
                        Value::Int64(i % 3),
                        Value::Int64(i),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT grp_a, grp_b, COUNT(*), SUM(n) FROM seeded GROUP BY grp_a, grp_b")
        .expect("multi-column grouped aggregate");
    assert_eq!(result.rows().len(), 6);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(0),
            Value::Int64(0),
            Value::Int64(16),
            Value::Int64(720)
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Int64(1),
            Value::Int64(16),
            Value::Int64(736)
        ]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[
            Value::Int64(0),
            Value::Int64(2),
            Value::Int64(16),
            Value::Int64(752)
        ]
    );
    assert_eq!(
        result.rows()[3].values(),
        &[
            Value::Int64(1),
            Value::Int64(0),
            Value::Int64(16),
            Value::Int64(768)
        ]
    );
    assert_eq!(
        result.rows()[4].values(),
        &[
            Value::Int64(0),
            Value::Int64(1),
            Value::Int64(16),
            Value::Int64(784)
        ]
    );
    assert_eq!(
        result.rows()[5].values(),
        &[
            Value::Int64(1),
            Value::Int64(2),
            Value::Int64(16),
            Value::Int64(800)
        ]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after multi-column grouped aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected multi-column paged grouped aggregate to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after multi-column grouped aggregate, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after multi-column paged grouped aggregate, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_expression_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-expression-projection.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(format!("name-{i:03}")),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");

    let result = db
        .execute("SELECT n + 1 AS next_n FROM seeded ORDER BY n LIMIT 1")
        .expect("arithmetic projection");
    assert_eq!(scalar_i64(&result), 1);

    let json_after_arithmetic = db
        .inspect_storage_state_json()
        .expect("json after arithmetic projection");
    assert!(
            json_after_arithmetic.contains("\"loaded_table_count\":0"),
            "expected paged arithmetic projection to avoid materialization, got: {json_after_arithmetic}"
        );
    assert!(
            json_after_arithmetic.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after arithmetic projection, got: {json_after_arithmetic}"
        );
    assert!(
        json_after_arithmetic.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after arithmetic projection, got: {json_after_arithmetic}"
    );

    let upper_result = db
        .execute("SELECT UPPER(name) FROM seeded ORDER BY name LIMIT 1")
        .expect("function projection");
    assert_eq!(scalar_text(&upper_result), "NAME-000");

    let offset_result = db
        .execute("SELECT n + 1 AS next_n FROM seeded ORDER BY n DESC LIMIT 2 OFFSET 1")
        .expect("offset arithmetic projection");
    assert_eq!(offset_result.rows().len(), 2);
    assert_eq!(offset_result.rows()[0].values(), &[Value::Int64(95)]);
    assert_eq!(offset_result.rows()[1].values(), &[Value::Int64(94)]);

    let json_after_function = db
        .inspect_storage_state_json()
        .expect("json after function projection");
    assert!(
        json_after_function.contains("\"loaded_table_count\":0"),
        "expected paged function projection to avoid materialization, got: {json_after_function}"
    );
    assert!(
            json_after_function.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after function projection, got: {json_after_function}"
        );
    assert!(
        json_after_function.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after function projection, got: {json_after_function}"
    );
}

#[test]
fn paged_row_storage_projection_multi_order_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-projection-multi-order.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, body TEXT)",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for (id, grp, n) in [(0_i64, 0_i64, 2_i64), (1, 0, 1), (2, 1, 2), (3, 1, 1)] {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(id),
                        Value::Int64(grp),
                        Value::Int64(n),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT grp, n FROM seeded ORDER BY grp DESC, n ASC LIMIT 3")
        .expect("projection multi-order");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(1)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(0), Value::Int64(1)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after projection multi-order");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged projection multi-order to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after projection multi-order, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after projection multi-order, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_virtual_generated_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-virtual-generated-projection.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (
                    id INTEGER PRIMARY KEY,
                    qty INTEGER,
                    price FLOAT64,
                    total FLOAT64 GENERATED ALWAYS AS (price * qty) VIRTUAL,
                    body TEXT
                )",
        )
        .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded (id, qty, price, body) VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..32_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i + 1),
                        Value::Float64(1.5),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT id, total FROM seeded WHERE id >= 2 AND id < 4 ORDER BY id")
        .expect("virtual generated projection");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Float64(4.5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(3), Value::Float64(6.0)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after generated projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected virtual generated projection to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after generated projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after generated projection, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_filtered_expression_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-filtered-expression-projection.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(format!("name-{i:03}")),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT n + 1 AS next_n FROM seeded WHERE n >= 10 AND n <= 12 ORDER BY n LIMIT 1")
        .expect("filtered arithmetic projection");
    assert_eq!(scalar_i64(&result), 11);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after filtered arithmetic projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged filtered expression projection to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after filtered expression projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after filtered expression projection, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_distinct_expression_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-distinct-expression-projection.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(format!("name-{:02}", i % 4)),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT DISTINCT UPPER(name) FROM seeded")
        .expect("distinct expression projection");
    assert_eq!(result.rows().len(), 4);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after distinct expression projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged distinct expression projection to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after distinct expression projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged distinct expression projection, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_distinct_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-distinct-projection.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 4),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT DISTINCT grp FROM seeded")
        .expect("paged distinct projection");
    assert_eq!(result.rows().len(), 4);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after paged distinct projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected paged distinct projection to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after distinct projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after paged distinct projection, got: {json_after}"
    );
}

#[test]
fn paged_row_storage_ordered_distinct_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-ordered-distinct-projection.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i % 4),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    let result = db
        .execute("SELECT DISTINCT grp FROM seeded ORDER BY grp DESC LIMIT 2 OFFSET 1")
        .expect("ordered paged distinct projection");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(1)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered paged distinct projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected ordered paged distinct projection to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected paged-backed table to remain deferred after ordered distinct projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after ordered paged distinct projection, got: {json_after}"
    );
}

#[test]
fn reader_handle_refreshes_after_external_checkpoint() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("checkpoint-refresh.ddb");
    let config = DbConfig::default();

    let setup = Db::open_or_create(&path, config.clone()).expect("open setup");
    setup
        .execute("CREATE TABLE t (id INTEGER)")
        .expect("create table");
    setup.begin_transaction().expect("begin seed txn");
    for i in 0_i64..100_i64 {
        setup
            .execute_with_params("INSERT INTO t VALUES ($1)", &[Value::Int64(i)])
            .expect("seed insert");
    }
    setup.commit_transaction().expect("commit seed");
    drop(setup);

    let reader = Db::open_or_create(&path, config.clone()).expect("open reader");
    assert_eq!(
        scalar_i64(
            &reader
                .execute("SELECT COUNT(*) FROM t")
                .expect("reader count before")
        ),
        100
    );

    let writer = Db::open_or_create(&path, config).expect("open writer");
    writer.begin_transaction().expect("begin writer txn");
    for i in 100_i64..200_i64 {
        writer
            .execute_with_params("INSERT INTO t VALUES ($1)", &[Value::Int64(i)])
            .expect("writer insert");
    }
    writer.commit_transaction().expect("commit writer");
    writer.checkpoint().expect("checkpoint writer");

    assert_eq!(
        scalar_i64(
            &reader
                .execute("SELECT COUNT(*) FROM t")
                .expect("reader count after")
        ),
        200
    );
}

#[test]
fn checkpoint_with_active_reader_retains_wal_versions_until_reader_drops() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("checkpoint-active-reader-retains-wal.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

    db.execute("CREATE TABLE t (id INTEGER, val TEXT)")
        .expect("create table");
    for i in 0..512_i64 {
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(i), Value::Text("x".repeat(128))],
        )
        .expect("insert row");
    }

    assert!(
        db.inner.wal.version_count().expect("version count before") > 0,
        "expected inserts to populate the WAL before checkpoint"
    );

    let reader = db.inner.wal.begin_reader().expect("begin reader");
    db.checkpoint().expect("checkpoint with active reader");
    assert!(
        db.inner
            .wal
            .version_count()
            .expect("version count with reader")
            > 0,
        "checkpoint should retain WAL versions while a reader is active"
    );

    drop(reader);
    db.checkpoint().expect("checkpoint after reader drop");
    assert_eq!(
        db.inner
            .wal
            .version_count()
            .expect("version count after reader"),
        0,
        "checkpoint should truncate WAL after active readers are gone"
    );
}

#[test]
fn same_handle_explicit_txn_can_commit_after_checkpoint_truncates_wal() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("checkpoint-explicit-txn-rebase.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");
    db.execute("INSERT INTO t VALUES (1, 'before-checkpoint')")
        .expect("insert before checkpoint");
    db.checkpoint().expect("checkpoint");
    assert_eq!(
        db.inner.wal.latest_snapshot(),
        0,
        "checkpoint should truncate WAL"
    );

    db.begin_transaction().expect("begin transaction");
    db.execute("INSERT INTO t VALUES (2, 'after-checkpoint')")
        .expect("insert after checkpoint");
    db.commit_transaction()
        .expect("commit transaction after checkpoint");

    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
        2
    );
}

#[test]
fn same_handle_prepared_explicit_txn_can_commit_after_checkpoint_truncates_wal() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("checkpoint-prepared-explicit-txn-rebase.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");
    db.execute("INSERT INTO t VALUES (1, 'before-checkpoint')")
        .expect("insert before checkpoint");
    db.checkpoint().expect("checkpoint");
    assert_eq!(
        db.inner.wal.latest_snapshot(),
        0,
        "checkpoint should truncate WAL"
    );

    db.begin_transaction().expect("begin transaction");
    let prepared = db
        .prepare("INSERT INTO t VALUES ($1, $2)")
        .expect("prepare insert");
    prepared
        .execute(&[Value::Int64(2), Value::Text("after-checkpoint".to_string())])
        .expect("execute prepared insert after checkpoint");
    db.commit_transaction()
        .expect("commit prepared transaction after checkpoint");

    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
        2
    );
}

#[test]
fn prepared_after_schema_change_in_shared_transaction_uses_current_transaction_schema() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("shared-txn-schema-change-prepare.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

    db.begin_transaction().expect("begin transaction");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create table in shared transaction");

    let prepared = db
        .prepare("INSERT INTO t VALUES ($1)")
        .expect("prepare insert after schema change");

    {
        let txn = db.inner.sql_txn.lock().expect("lock sql transaction");
        let super::SqlTxnSlot::Shared(state) = &*txn else {
            panic!("expected shared sql transaction state");
        };
        assert_eq!(
            prepared.schema_cookie, state.runtime.catalog.schema_cookie,
            "prepared insert should capture the shared transaction schema cookie"
        );
        assert_eq!(
            prepared.temp_schema_cookie, state.runtime.temp_schema_cookie,
            "prepared insert should capture the shared transaction temp schema cookie"
        );
    }

    prepared
        .execute(&[Value::Int64(1)])
        .expect("execute prepared insert after schema change");
    db.commit_transaction().expect("commit transaction");

    assert_eq!(
        scalar_i64(&db.execute("SELECT COUNT(*) FROM t").expect("count rows")),
        1
    );
}

#[test]
fn shared_wal_registry_entry_is_removed_when_last_handle_drops() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("shared-wal-registry-cleanup.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INTEGER)")
        .expect("create table");

    let canonical_path = crate::vfs::VfsHandle::for_path(&path)
        .canonicalize_path(&path)
        .expect("canonicalize path");
    assert!(crate::wal::shared::has_registry_entry_for_tests(
        &canonical_path
    ));

    drop(db);

    let deadline = Instant::now() + Duration::from_secs(2);
    while crate::wal::shared::has_registry_entry_for_tests(&canonical_path)
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(!crate::wal::shared::has_registry_entry_for_tests(
        &canonical_path
    ));
}

#[test]
fn checkpoint_preserves_unchanged_table_payload_pages() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("checkpoint-unchanged-table-payload.ddb");
    let config = DbConfig::default();

    let setup = Db::open_or_create(&path, config.clone()).expect("open setup");
    setup
        .execute("CREATE TABLE initial (id INTEGER, val TEXT)")
        .expect("create initial table");
    setup
        .execute_with_params(
            "INSERT INTO initial VALUES ($1, $2)",
            &[Value::Int64(1), Value::Text("seed".to_string())],
        )
        .expect("seed initial row");
    setup.checkpoint().expect("checkpoint setup");
    drop(setup);

    let writer = Db::open_or_create(&path, config).expect("open writer");
    writer
        .execute("CREATE TABLE new_table (id INTEGER, val TEXT)")
        .expect("create new table");
    writer.begin_transaction().expect("begin writer txn");
    for i in 0_i64..100_i64 {
        writer
            .execute_with_params(
                "INSERT INTO new_table VALUES ($1, $2)",
                &[Value::Int64(i), Value::Text(format!("value-{i}"))],
            )
            .expect("insert new row");
    }
    writer.commit_transaction().expect("commit writer");
    writer.checkpoint().expect("checkpoint writer");
    drop(writer);

    let reopened = Db::open_or_create(&path, DbConfig::default()).expect("reopen database");
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT COUNT(*) FROM initial")
                .expect("count initial rows after checkpoint")
        ),
        1
    );
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT COUNT(*) FROM new_table")
                .expect("count new rows after checkpoint")
        ),
        100
    );
}

#[test]
fn schema_snapshot_projects_rich_schema_metadata() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .expect("create parent table");
    db.execute(
        "CREATE TABLE child (
                id INT PRIMARY KEY,
                parent_id INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL,
                qty INT CHECK (qty > 0),
                price FLOAT64 NOT NULL,
                total_stored FLOAT64 GENERATED ALWAYS AS (price * qty) STORED,
                total_virtual FLOAT64 GENERATED ALWAYS AS (price * qty) VIRTUAL,
                CONSTRAINT child_parent_positive CHECK (parent_id IS NULL OR parent_id > 0),
                CHECK (qty < 1000)
            )",
    )
    .expect("create child table");
    db.execute("INSERT INTO parent VALUES (1)")
        .expect("insert parent row");
    db.execute("INSERT INTO child (id, parent_id, qty, price) VALUES (1, 1, 5, 3.0)")
        .expect("insert child row");

    db.execute("CREATE TEMP TABLE temp_data (id INT PRIMARY KEY)")
        .expect("create temp table");
    db.execute("INSERT INTO temp_data VALUES (7)")
        .expect("insert temp row");
    db.execute("CREATE VIEW child_view AS SELECT id, parent_id FROM child")
        .expect("create view");
    db.execute("CREATE TEMP VIEW temp_child_ids AS SELECT id FROM temp_data")
        .expect("create temp view");
    db.execute("CREATE INDEX child_parent_partial ON child(parent_id) WHERE parent_id IS NOT NULL")
        .expect("create partial index");
    db.execute(
            "CREATE TRIGGER child_after_insert AFTER INSERT ON child FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO parent VALUES (999)')",
        )
        .expect("create table trigger");
    db.execute(
            "CREATE TRIGGER child_view_insert INSTEAD OF INSERT ON child_view FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO child (id, parent_id, qty, price) VALUES (2000, NULL, 1, 1.0)')",
        )
        .expect("create view trigger");

    let snapshot = db.get_schema_snapshot().expect("schema snapshot");
    assert_eq!(snapshot.snapshot_version, 1);

    let child = snapshot
        .tables
        .iter()
        .find(|table| table.name == "child")
        .expect("child table in snapshot");
    assert!(!child.temporary);
    assert_eq!(child.row_count, 1);
    assert!(child.ddl.contains("CREATE TABLE \"child\""));
    assert!(child
        .checks
        .iter()
        .any(|check| check.name.as_deref() == Some("child_parent_positive")));
    assert!(child.checks.iter().any(|check| check.name.is_none()));
    assert!(child
        .foreign_keys
        .iter()
        .any(|fk| fk.on_delete == "CASCADE" && fk.on_update == "SET NULL"));

    let qty_column = child
        .columns
        .iter()
        .find(|column| column.name == "qty")
        .expect("qty column");
    assert!(qty_column.checks.iter().any(|check| check.name.is_none()));

    let stored = child
        .columns
        .iter()
        .find(|column| column.name == "total_stored")
        .expect("stored generated column");
    assert_eq!(stored.generated_sql.as_deref(), Some("(price * qty)"));
    assert!(stored.generated_stored);

    let virtual_column = child
        .columns
        .iter()
        .find(|column| column.name == "total_virtual")
        .expect("virtual generated column");
    assert_eq!(
        virtual_column.generated_sql.as_deref(),
        Some("(price * qty)")
    );
    assert!(!virtual_column.generated_stored);

    let temp_table = snapshot
        .tables
        .iter()
        .find(|table| table.name == "temp_data")
        .expect("temp table in snapshot");
    assert!(temp_table.temporary);
    assert_eq!(temp_table.row_count, 1);
    assert!(temp_table.ddl.contains("CREATE TEMP TABLE"));

    let temp_view = snapshot
        .views
        .iter()
        .find(|view| view.name == "temp_child_ids")
        .expect("temp view in snapshot");
    assert!(temp_view.temporary);
    assert!(temp_view.ddl.contains("CREATE TEMP VIEW"));

    let partial_index = snapshot
        .indexes
        .iter()
        .find(|index| index.name == "child_parent_partial")
        .expect("partial index in snapshot");
    assert_eq!(
        partial_index.predicate_sql.as_deref(),
        Some("parent_id IS NOT NULL")
    );
    assert!(partial_index.ddl.contains("WHERE parent_id IS NOT NULL"));

    let table_trigger = snapshot
        .triggers
        .iter()
        .find(|trigger| trigger.name == "child_after_insert")
        .expect("table trigger in snapshot");
    assert_eq!(table_trigger.target_kind, "table");
    assert_eq!(table_trigger.timing, "after");
    assert_eq!(table_trigger.events, vec!["insert".to_string()]);
    assert_eq!(table_trigger.events_mask, 1);
    assert!(table_trigger.for_each_row);
    assert!(!table_trigger.temporary);
    assert!(table_trigger.ddl.contains("CREATE TRIGGER"));

    let view_trigger = snapshot
        .triggers
        .iter()
        .find(|trigger| trigger.name == "child_view_insert")
        .expect("view trigger in snapshot");
    assert_eq!(view_trigger.target_kind, "view");
    assert_eq!(view_trigger.timing, "instead_of");
}

#[test]
fn schema_snapshot_orders_top_level_collections_by_name() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE z_table (id INT PRIMARY KEY)")
        .expect("create z_table");
    db.execute("CREATE TABLE a_table (id INT PRIMARY KEY)")
        .expect("create a_table");
    db.execute("CREATE TEMP TABLE m_table (id INT PRIMARY KEY)")
        .expect("create m_table");
    db.execute("CREATE VIEW z_view AS SELECT id FROM z_table")
        .expect("create z_view");
    db.execute("CREATE VIEW a_view AS SELECT id FROM a_table")
        .expect("create a_view");
    db.execute("CREATE TEMP VIEW m_view AS SELECT id FROM m_table")
        .expect("create m_view");
    db.execute("CREATE INDEX z_index ON z_table(id)")
        .expect("create z_index");
    db.execute("CREATE INDEX a_index ON a_table(id)")
        .expect("create a_index");
    db.execute(
            "CREATE TRIGGER z_trigger AFTER INSERT ON z_table FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO z_table VALUES (1000)')",
        )
        .expect("create z_trigger");
    db.execute(
            "CREATE TRIGGER a_trigger AFTER INSERT ON a_table FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO a_table VALUES (1000)')",
        )
        .expect("create a_trigger");

    let snapshot = db.get_schema_snapshot().expect("schema snapshot");

    let table_names = snapshot
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    assert_sorted_names(&table_names);

    let view_names = snapshot
        .views
        .iter()
        .map(|view| view.name.clone())
        .collect::<Vec<_>>();
    assert_sorted_names(&view_names);

    let index_names = snapshot
        .indexes
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    assert_sorted_names(&index_names);

    let trigger_names = snapshot
        .triggers
        .iter()
        .map(|trigger| trigger.name.clone())
        .collect::<Vec<_>>();
    assert_sorted_names(&trigger_names);
}

#[test]
fn tooling_metadata_fingerprint_ignores_row_counts_and_captures_spatial_types() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE sites (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                location GEOGRAPHY(POINT,4326),
                boundary GEOMETRY(POLYGON,0)
            )",
    )
    .expect("create sites");

    let metadata = db.get_tooling_metadata().expect("tooling metadata");
    assert_eq!(metadata.metadata_version, 1);
    assert_eq!(
        metadata.schema_fingerprint_algorithm,
        "sha256:decentdb-tooling-schema-v1"
    );
    assert_eq!(metadata.schema_fingerprint.len(), 64);
    assert!(metadata.capabilities.query_describe);
    assert!(metadata.capabilities.deterministic_json);

    let location_type = metadata
        .column_type_metadata
        .iter()
        .find(|column| column.table_name == "sites" && column.column_name == "location")
        .expect("location type metadata");
    assert_eq!(location_type.column_type, "GEOGRAPHY");
    assert_eq!(location_type.type_info.value_kind, "geography_ewkb");
    assert_eq!(location_type.type_info.c_value_tag, 10);
    let spatial = location_type
        .type_info
        .spatial
        .as_ref()
        .expect("location spatial metadata");
    assert_eq!(spatial.subtype, "POINT");
    assert_eq!(spatial.dimensions, "XY");
    assert_eq!(spatial.srid, 4326);
    let name_type = metadata
        .column_type_metadata
        .iter()
        .find(|column| column.table_name == "sites" && column.column_name == "name")
        .expect("name type metadata");
    assert_eq!(name_type.type_info.c_value_tag, 4);

    db.execute(
        "INSERT INTO sites (id, name, location)
             VALUES (1, 'austin', ST_GeogPoint(-97.7431, 30.2672))",
    )
    .expect("insert site");
    let after_insert = db.get_tooling_metadata().expect("metadata after insert");
    assert_eq!(
        metadata.schema_fingerprint, after_insert.schema_fingerprint,
        "data-only row-count changes must not alter the tooling schema fingerprint"
    );

    db.execute("CREATE INDEX sites_name_idx ON sites(name)")
        .expect("create index");
    let after_index = db.get_tooling_metadata().expect("metadata after index");
    assert_ne!(
        metadata.schema_fingerprint, after_index.schema_fingerprint,
        "schema/index changes must alter the tooling schema fingerprint"
    );
}

#[test]
fn describe_query_contract_infers_select_parameters_and_result_columns() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE users (
                id INT64 PRIMARY KEY,
                email TEXT NOT NULL,
                location GEOGRAPHY(POINT,4326)
            )",
    )
    .expect("create users");

    let contract = db
        .describe_query_contract(
            "SELECT id, email, ST_DWithin(location, $1, $2) AS nearby
                 FROM users
                 WHERE id = $3 AND email ILIKE $4",
        )
        .expect("describe select");
    assert_eq!(contract.contract_version, 1);
    assert_eq!(contract.statement_kind, "query");
    assert!(contract.read_only);
    assert_eq!(contract.parameters.len(), 4);
    assert_eq!(contract.parameters[0].name, "$1");
    assert_eq!(
        contract.parameters[0].type_name.as_deref(),
        Some("GEOGRAPHY")
    );
    assert_eq!(contract.parameters[1].name, "$2");
    assert_eq!(contract.parameters[1].type_name.as_deref(), Some("FLOAT64"));
    assert_eq!(contract.parameters[2].name, "$3");
    assert_eq!(contract.parameters[2].type_name.as_deref(), Some("INT64"));
    assert_eq!(
        contract.parameters[2].source_table.as_deref(),
        Some("users")
    );
    assert_eq!(contract.parameters[2].source_column.as_deref(), Some("id"));
    assert_eq!(contract.parameters[3].name, "$4");
    assert_eq!(contract.parameters[3].type_name.as_deref(), Some("TEXT"));

    assert_eq!(contract.result_columns.len(), 3);
    assert_eq!(contract.result_columns[0].name, "id");
    assert_eq!(
        contract.result_columns[0].type_name.as_deref(),
        Some("INT64")
    );
    assert_eq!(contract.result_columns[0].nullable, Some(false));
    assert_eq!(
        contract.result_columns[0].source_table.as_deref(),
        Some("users")
    );
    assert_eq!(
        contract.result_columns[0].source_column.as_deref(),
        Some("id")
    );
    assert_eq!(contract.result_columns[1].name, "email");
    assert_eq!(
        contract.result_columns[1].type_name.as_deref(),
        Some("TEXT")
    );
    assert_eq!(contract.result_columns[1].nullable, Some(false));
    assert_eq!(contract.result_columns[2].name, "nearby");
    assert_eq!(
        contract.result_columns[2].type_name.as_deref(),
        Some("BOOL")
    );
}

#[test]
fn describe_query_contract_infers_insert_returning_contracts() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE users (
                id INT64 PRIMARY KEY,
                email TEXT NOT NULL
            )",
    )
    .expect("create users");

    let contract = db
        .describe_query_contract(
            "INSERT INTO users (id, email) VALUES ($1, $2) RETURNING id, email",
        )
        .expect("describe insert returning");
    assert_eq!(contract.statement_kind, "insert");
    assert!(!contract.read_only);
    assert_eq!(contract.parameters.len(), 2);
    assert_eq!(contract.parameters[0].type_name.as_deref(), Some("INT64"));
    assert_eq!(
        contract.parameters[0].source_table.as_deref(),
        Some("users")
    );
    assert_eq!(contract.parameters[0].source_column.as_deref(), Some("id"));
    assert_eq!(contract.parameters[1].type_name.as_deref(), Some("TEXT"));
    assert_eq!(
        contract.parameters[1].source_table.as_deref(),
        Some("users")
    );
    assert_eq!(
        contract.parameters[1].source_column.as_deref(),
        Some("email")
    );
    assert_eq!(contract.result_columns.len(), 2);
    assert_eq!(contract.result_columns[0].name, "id");
    assert_eq!(
        contract.result_columns[0].type_name.as_deref(),
        Some("INT64")
    );
    assert_eq!(contract.result_columns[1].name, "email");
    assert_eq!(
        contract.result_columns[1].type_name.as_deref(),
        Some("TEXT")
    );
}

#[test]
fn write_transaction_page_allocation_stays_off_main_file_until_commit() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("txn-page-allocation.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

    let initial_page_count = db
        .inner
        .pager
        .on_disk_page_count()
        .expect("initial page count");

    db.begin_write().expect("begin write txn");
    let page_id = db.allocate_page().expect("allocate staged page");
    assert!(page_id > initial_page_count);
    assert_eq!(
        db.inner
            .pager
            .on_disk_page_count()
            .expect("page count after staged allocation"),
        initial_page_count
    );
    assert_eq!(
        db.read_page(page_id).expect("read staged page").to_vec(),
        vec![0_u8; db.config().page_size as usize]
    );
    db.rollback().expect("rollback write txn");

    let reopened = Db::open_or_create(&path, DbConfig::default()).expect("reopen db");
    assert_eq!(
        reopened
            .inner
            .pager
            .on_disk_page_count()
            .expect("page count after rollback"),
        initial_page_count
    );
}

#[test]
fn freed_pages_are_reused_after_commit() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("freelist-reuse.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

    db.begin_write().expect("begin allocate txn");
    let page_id = db.allocate_page().expect("allocate page");
    db.commit().expect("commit allocated page");

    db.begin_write().expect("begin free txn");
    db.free_page(page_id).expect("free page");
    db.commit().expect("commit freed page");

    db.begin_write().expect("begin reuse txn");
    let reused = db.allocate_page().expect("reuse page");
    assert_eq!(reused, page_id);
    db.rollback().expect("rollback reuse txn");
}

#[test]
fn checkpoint_truncates_tail_freelist_pages_and_resets_allocator_state() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("checkpoint-tail-truncation.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");

    db.begin_write().expect("begin allocate txn");
    let page3 = db.allocate_page().expect("allocate page 3");
    let page4 = db.allocate_page().expect("allocate page 4");
    let page5 = db.allocate_page().expect("allocate page 5");
    assert_eq!((page3, page4, page5), (3, 4, 5));
    db.commit().expect("commit allocated pages");

    db.begin_write().expect("begin free txn");
    db.free_page(page5).expect("free page 5");
    db.free_page(page4).expect("free page 4");
    db.commit().expect("commit freed tail pages");
    db.checkpoint().expect("checkpoint freed pages");

    assert_eq!(
        db.inner
            .pager
            .on_disk_page_count()
            .expect("page count after truncation"),
        3
    );
    assert_eq!(
        db.inner
            .pager
            .header_snapshot()
            .expect("header snapshot after truncation")
            .freelist
            .page_count,
        0
    );

    db.begin_write().expect("begin allocate after truncation");
    let reused = db.allocate_page().expect("allocate page after truncation");
    assert_eq!(reused, 4);
    db.rollback().expect("rollback post-truncation allocation");
}

fn dummy_prepared_insert(table_name: &str) -> PreparedSimpleInsert {
    PreparedSimpleInsert {
        table_name: table_name.to_string(),
        catalog_table_name: Some(table_name.to_string()),
        row_source_dependency_tables: Vec::new(),
        columns: vec![PreparedInsertColumn {
            name: "id".to_string(),
            column_type: crate::catalog::ColumnType::Int64,
            auto_increment: false,
        }],
        primary_auto_row_id_column_index: None,
        value_sources: vec![PreparedInsertValueSource::Null],
        required_columns: Vec::new(),
        foreign_keys: Vec::new(),
        unique_indexes: Vec::new(),
        insert_indexes: Vec::new(),
        use_generic_validation: false,
        use_generic_index_updates: false,
        direct_positional_param_count: Some(1),
        has_auto_increment: false,
        compiled_index_state_epoch: 0,
    }
}

fn assert_sorted_names(names: &[String]) {
    let mut sorted = names.to_vec();
    sorted.sort();
    assert_eq!(names, sorted);
}

fn scalar_i64(result: &crate::QueryResult) -> i64 {
    match result.rows()[0].values()[0] {
        Value::Int64(value) => value,
        ref other => panic!("expected INT64 scalar, got {other:?}"),
    }
}

fn scalar_text(result: &crate::QueryResult) -> &str {
    match &result.rows()[0].values()[0] {
        Value::Text(value) => value,
        other => panic!("expected TEXT scalar, got {other:?}"),
    }
}

#[test]
fn concurrent_independent_handles_keep_paged_manifest_valid() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("independent-handles.ddb");
    let rows = 1_000_i64;

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open seed db");
        db.execute(
            "CREATE TABLE bench_read_under_write (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )
        .expect("create table");
        let mut inserted = 0_i64;
        while inserted < rows {
            let mut txn = db.transaction().expect("begin seed txn");
            let insert = txn
                .prepare("INSERT INTO bench_read_under_write (id, payload) VALUES ($1, $2)")
                .expect("prepare insert");
            for id in inserted..(inserted + 100).min(rows) {
                insert
                    .execute_in(
                        &mut txn,
                        &[Value::Int64(id + 1), Value::Text(format!("payload-{id}"))],
                    )
                    .expect("insert row");
            }
            txn.commit().expect("commit seed txn");
            inserted = (inserted + 100).min(rows);
        }
        db.checkpoint().expect("checkpoint seed db");
    }

    let reader_a = Db::open(&path, DbConfig::default()).expect("open reader a");
    let reader_b = Db::open(&path, DbConfig::default()).expect("open reader b");
    let writer = Db::open(&path, DbConfig::default()).expect("open writer");
    let barrier = Arc::new(Barrier::new(3));

    let reader_handles = [reader_a, reader_b]
        .into_iter()
        .enumerate()
        .map(|(reader_index, db)| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let select = db
                    .prepare("SELECT payload FROM bench_read_under_write WHERE id = $1")
                    .expect("prepare reader select");
                barrier.wait();
                for op in 0..500_i64 {
                    let id = ((op * 17 + reader_index as i64) % rows) + 1;
                    let result = select
                        .execute(&[Value::Int64(id)])
                        .expect("reader point lookup");
                    assert_eq!(result.rows().len(), 1);
                }
            })
        })
        .collect::<Vec<_>>();

    let writer_barrier = Arc::clone(&barrier);
    let writer_handle = thread::spawn(move || {
        let update = writer
            .prepare("UPDATE bench_read_under_write SET payload = 'rw' WHERE id = $1")
            .expect("prepare writer update");
        writer_barrier.wait();
        for op in 0..80_i64 {
            let id = ((op * 31) % rows) + 1;
            let mut txn = writer.transaction().expect("begin writer txn");
            update
                .execute_in(&mut txn, &[Value::Int64(id)])
                .expect("writer update");
            txn.commit().expect("commit writer txn");
        }
    });

    for handle in reader_handles {
        handle.join().expect("reader thread");
    }
    writer_handle.join().expect("writer thread");

    let reopened = Db::open(&path, DbConfig::default()).expect("reopen after mixed workload");
    assert_eq!(
        scalar_i64(
            &reopened
                .execute("SELECT COUNT(*) FROM bench_read_under_write")
                .expect("count rows")
        ),
        rows
    );
    let point = reopened
        .prepare("SELECT payload FROM bench_read_under_write WHERE id = $1")
        .expect("prepare final point lookup");
    for id in [1_i64, 7, 127, 509, 1000] {
        let result = point
            .execute(&[Value::Int64(id)])
            .expect("final point lookup");
        assert_eq!(result.rows().len(), 1);
    }
}

/// ADR 0143 Phase A: `inspect_storage_state_json` exposes per-runtime
/// table residency so callers can verify lazy-load progress.
#[test]
fn inspect_storage_state_json_reports_table_memory_totals() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE m (id INTEGER PRIMARY KEY, label TEXT)")
        .expect("create m");
    let label = "value-with-enough-text-to-require-paged-row-storage".repeat(64);
    for i in 0..32 {
        db.prepare("INSERT INTO m (id, label) VALUES ($1, $2)")
            .expect("prepare insert")
            .execute(&[Value::Int64(i), Value::Text(label.clone())])
            .expect("insert m");
    }
    let json = db
        .inspect_storage_state_json()
        .expect("inspect storage state");
    assert!(
        json.contains("\"tables_in_memory_bytes\":"),
        "missing tables_in_memory_bytes: {json}"
    );
    assert!(
        json.contains("\"rows_in_memory_count\":"),
        "missing rows_in_memory_count: {json}"
    );
    assert!(
        json.contains("\"loaded_table_count\":"),
        "missing loaded_table_count: {json}"
    );
    assert!(
        json.contains("\"wal_resident_versions\":"),
        "missing wal_resident_versions: {json}"
    );
    assert!(
        json.contains("\"wal_on_disk_versions\":"),
        "missing wal_on_disk_versions: {json}"
    );
    // With default paged_row_storage: true, tables large enough to need
    // chunked storage stay deferred after autocommit inserts. Only small
    // single-payload tables stay resident in memory.
    assert!(
        json.contains("\"deferred_table_count\":1"),
        "expected one deferred table with paged_row_storage=true, got: {json}"
    );
    // 32 inserted rows are recorded in the WAL but not resident in memory
    // (paged row source keeps data on-disk).
    assert!(
        json.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows with paged_row_storage=true, got: {json}"
    );
}

#[test]
fn paged_row_storage_keeps_small_append_table_single_payload() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount FLOAT64)")
        .expect("create orders");
    let insert = db
        .prepare("INSERT INTO orders (id, user_id, amount) VALUES ($1, $2, $3)")
        .expect("prepare insert");
    for i in 0_i64..128_i64 {
        insert
            .execute(&[Value::Int64(i), Value::Int64(i % 16), Value::Float64(9.99)])
            .expect("insert order");
    }

    let runtime = db.inner.engine.read().expect("runtime lock");
    let state = runtime
        .persisted_tables
        .get("orders")
        .expect("orders persisted state");
    assert!(
        !state.pointer.is_table_paged_manifest(),
        "small append-only tables should avoid paged manifest overhead"
    );
    assert!(
        runtime.tables.contains_key("orders"),
        "small single-payload table should remain resident"
    );
    assert_eq!(runtime.deferred_table_names().count(), 0);
}

#[test]
fn paged_row_storage_converts_small_payload_after_chunk_threshold() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
        .expect("create docs");
    let insert = db
        .prepare("INSERT INTO docs (id, body) VALUES ($1, $2)")
        .expect("prepare insert");
    let body = "x".repeat(2048);
    for i in 0_i64..40_i64 {
        insert
            .execute(&[Value::Int64(i + 1), Value::Text(body.clone())])
            .expect("insert doc");
    }

    {
        let runtime = db.inner.engine.read().expect("runtime lock");
        let state = runtime
            .persisted_tables
            .get("docs")
            .expect("docs persisted state");
        assert!(
            state.pointer.is_table_paged_manifest(),
            "large append-only tables should convert to paged storage"
        );
        assert!(
            runtime
                .deferred_table_names()
                .any(|name| name.eq_ignore_ascii_case("docs")),
            "converted paged table should be re-deferred after write"
        );
    }

    assert_eq!(
        scalar_i64(
            &db.execute("SELECT COUNT(*) FROM docs")
                .expect("count docs rows")
        ),
        40
    );
}

/// ADR 0143 Phase B: by default, re-opening a DB leaves persisted
/// tables in the deferred set until the first SQL statement runs,
/// then materializes them.
#[test]
fn default_defer_table_materialization_skips_eager_load_at_open() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("defer-load.ddb");

    // Seed a DB with a persisted table and close.
    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with default defer");

    // At open, the table should not yet be loaded.
    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected 1 deferred table at open, got: {json_open}"
    );
    assert!(
        json_open.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows at open, got: {json_open}"
    );

    // Simple single-table expression projections now stream directly from
    // persisted rows instead of forcing resident materialization.
    let result = db
        .execute("SELECT n + 1 FROM seeded ORDER BY n LIMIT 1")
        .expect("query after defer");
    assert_eq!(scalar_i64(&result), 1);

    let offset_result = db
        .execute("SELECT n + 1 FROM seeded ORDER BY n DESC LIMIT 2 OFFSET 1")
        .expect("offset query after defer");
    assert_eq!(offset_result.rows().len(), 2);
    assert_eq!(offset_result.rows()[0].values(), &[Value::Int64(49)]);
    assert_eq!(offset_result.rows()[1].values(), &[Value::Int64(48)]);

    let json_after = db.inspect_storage_state_json().expect("json after");
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after first query, got: {json_after}"
    );
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected zero loaded tables after deferred expression projection, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after deferred expression projection, got: {json_after}"
    );
}

#[test]
fn temp_only_writes_do_not_load_deferred_persisted_tables() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("defer-temp-only-writes.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with default defer");
    let json_open = db.inspect_storage_state_json().expect("json at open");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected zero loaded tables at open, got: {json_open}"
    );
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected one deferred table at open, got: {json_open}"
    );

    db.execute("CREATE TEMP TABLE temp_data (id INTEGER PRIMARY KEY)")
        .expect("create temp table");
    db.execute("INSERT INTO temp_data SELECT 1 UNION ALL SELECT 2")
        .expect("insert into temp table");
    db.execute("CREATE TEMP VIEW temp_view AS SELECT id FROM temp_data")
        .expect("create temp view");

    let temp_count = db
        .execute("SELECT COUNT(*) FROM temp_data")
        .expect("count temp rows");
    assert_eq!(scalar_i64(&temp_count), 2);
    let temp_view_count = db
        .execute("SELECT COUNT(*) FROM temp_view")
        .expect("count temp view rows");
    assert_eq!(scalar_i64(&temp_view_count), 2);

    db.execute("DROP VIEW temp_view").expect("drop temp view");
    db.execute("DROP TABLE temp_data").expect("drop temp table");

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after temp writes");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected temp-only DDL and writes to avoid loading persisted tables, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred persisted table to remain deferred after temp-only DDL and writes, got: {json_after}"
        );
}

#[test]
fn filtered_expression_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-filtered-expression-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(format!("name-{i:03}")),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT UPPER(name) FROM seeded WHERE n >= 10 AND n <= 12 ORDER BY n LIMIT 1")
        .expect("filtered deferred expression projection");
    assert_eq!(scalar_text(&result), "NAME-010");

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after filtered deferred expression projection");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected filtered deferred expression projection to avoid materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after filtered expression projection, got: {json_after}"
        );
    assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after filtered deferred expression projection, got: {json_after}"
        );
}

#[test]
fn distinct_expression_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-distinct-expression-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, name TEXT, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded VALUES ($1, $2, $3, $4)")
            .expect("prepare insert");
        for i in 0_i64..96_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i),
                        Value::Int64(i),
                        Value::Text(format!("name-{:02}", i % 4)),
                        Value::Text(large_body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT DISTINCT UPPER(name) FROM seeded")
        .expect("distinct deferred expression projection");
    assert_eq!(result.rows().len(), 4);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after distinct deferred expression projection");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected distinct deferred expression projection to avoid materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after distinct deferred expression projection, got: {json_after}"
        );
    assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after distinct deferred expression projection, got: {json_after}"
        );
}

#[test]
fn row_id_point_lookup_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-row-id.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected deferred table at open, got: {json_open}"
    );

    let result = db
        .execute("SELECT n FROM seeded WHERE id = 17")
        .expect("point lookup");
    assert_eq!(scalar_i64(&result), 17);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after point lookup");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected point lookup to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after deferred point lookup, got: {json_after}"
    );
}

#[test]
fn explain_row_id_lookup_reports_runtime_path_and_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-explain-row-id.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create seeded");
        let body = "x".repeat(2048);
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO seeded (id, n, body) VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for i in 0_i64..128_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[Value::Int64(i), Value::Int64(i), Value::Text(body.clone())],
                )
                .expect("insert");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let json_open = db.inspect_storage_state_json().expect("json open");
    assert!(
        json_open.contains("\"loaded_table_count\":0"),
        "expected zero loaded tables at open, got: {json_open}"
    );

    let explain = db
        .execute("EXPLAIN SELECT n FROM seeded WHERE Id = 17")
        .expect("explain point lookup");
    assert!(
        explain
            .explain_lines()
            .iter()
            .any(|line| line.contains("RowIdLookup(table=seeded, column=id")),
        "expected RowIdLookup in explain, got: {:?}",
        explain.explain_lines()
    );
    assert!(
        explain
            .explain_lines()
            .iter()
            .all(|line| !line.contains("TableScan(table=seeded)")),
        "did not expect TableScan in explain, got: {:?}",
        explain.explain_lines()
    );

    let json_after = db.inspect_storage_state_json().expect("json after explain");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected EXPLAIN to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after EXPLAIN, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after EXPLAIN, got: {json_after}"
    );
}

#[test]
fn simple_count_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-count.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db.execute("SELECT COUNT(*) FROM seeded").expect("count");
    assert_eq!(scalar_i64(&result), 50);

    let json_after = db.inspect_storage_state_json().expect("json after count");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected count to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after count, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after deferred count, got: {json_after}"
    );
}

#[test]
fn simple_min_max_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-min-max.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            let n = if i % 10 == 0 {
                "NULL".to_string()
            } else {
                i.to_string()
            };
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {n})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with defer");
    let max_result = db.execute("SELECT MAX(id) FROM seeded").expect("max");
    assert_eq!(scalar_i64(&max_result), 49);
    let min_result = db.execute("SELECT MIN(n) FROM seeded").expect("min");
    assert_eq!(scalar_i64(&min_result), 1);

    let json_after = db.inspect_storage_state_json().expect("json after min/max");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected min/max to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after min/max, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after deferred min/max, got: {json_after}"
    );
}

#[test]
fn prepared_row_id_point_lookup_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("prepared-deferred-row-id.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let prepared = db
        .prepare("SELECT n FROM seeded WHERE id = $1")
        .expect("prepare point lookup");
    let result = prepared
        .execute(&[Value::Int64(17)])
        .expect("execute prepared point lookup");
    assert_eq!(scalar_i64(&result), 17);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after prepared point lookup");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected prepared point lookup to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after prepared deferred point lookup, got: {json_after}"
    );
}

#[test]
fn prepared_row_id_range_uses_deferred_locator_cache() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("prepared-deferred-row-id-range.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT, body TEXT)")
        .expect("create users");

    let body = "x".repeat(1024);
    let mut txn = db.transaction().expect("begin txn");
    let insert = txn
        .prepare("INSERT INTO users (id, name, body) VALUES ($1, $2, $3)")
        .expect("prepare insert");
    for id in 1_i64..=256_i64 {
        insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(id),
                    Value::Text(format!("u{id}")),
                    Value::Text(body.clone()),
                ],
            )
            .expect("insert");
    }
    txn.commit().expect("commit rows");

    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        let users = runtime
            .persisted_tables
            .get("users")
            .expect("persisted users after commit");
        assert!(
            users.pointer.is_table_paged_manifest(),
            "expected benchmark-shaped table to use paged storage"
        );
        assert!(
            runtime.has_deferred_paged_row_locator_cache_for_tests("users"),
            "expected INT64 primary-key table to build a deferred locator cache"
        );
    }

    let prepared = db
        .prepare("SELECT name FROM users WHERE id >= $1 AND id < $2 ORDER BY id LIMIT $3")
        .expect("prepare range lookup");
    assert!(
        prepared.simple_row_id_range_projection.is_some(),
        "expected prepared range lookup to cache the row-id range plan"
    );
    let result = prepared
        .execute(&[Value::Int64(10), Value::Int64(20), Value::Int64(3)])
        .expect("execute prepared range lookup");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(result.rows()[0].values(), &[Value::Text("u10".to_string())]);
    assert_eq!(result.rows()[2].values(), &[Value::Text("u12".to_string())]);

    let lower_limit = db
        .prepare("SELECT name FROM users WHERE id >= $1 LIMIT $2")
        .expect("prepare lower-bound range lookup");
    let result = lower_limit
        .execute(&[Value::Int64(200), Value::Int64(2)])
        .expect("execute lower-bound range lookup");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("u200".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Text("u201".to_string())]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after prepared range lookup");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected prepared range lookup to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after prepared deferred range lookup, got: {json_after}"
    );
}

#[test]
fn deferred_ordered_projection_paged_limit_offset_keeps_deferred_state() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-ordered-projection-paged.ddb");
    let create_config = DbConfig {
        paged_row_storage: true,
        defer_table_materialization: true,
        persistent_pk_index: false,
        ..DbConfig::default()
    };
    let query_config = DbConfig {
        paged_row_storage: true,
        defer_table_materialization: true,
        persistent_pk_index: true,
        ..DbConfig::default()
    };
    let body = "x".repeat(2048);

    {
        let db = Db::open_or_create(&path, create_config).expect("create db");
        db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT, body TEXT)")
            .expect("create users");
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO users VALUES ($1, $2, $3)")
            .expect("prepare insert");
        for id in 1_i64..=200_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(id),
                        Value::Text(format!("user-{id}")),
                        Value::Text(body.clone()),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit rows");
        db.checkpoint().expect("checkpoint before reopen");
    }

    let db = Db::open_or_create(&path, query_config.clone()).expect("reopen with defer");
    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        let table = runtime
            .catalog
            .table("users")
            .expect("users table after reopen");
        assert!(
            table.pk_index_root.is_none(),
            "legacy database should reopen before targeted primary-key locator backfill"
        );
        let pk_index = runtime
            .catalog
            .indexes
            .values()
            .find(|index| {
                index.table_name.eq_ignore_ascii_case("users")
                    && index.unique
                    && index.kind == IndexKind::Btree
                    && index.columns.len() == 1
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|column| column.eq_ignore_ascii_case("id"))
            })
            .expect("primary-key btree index");
        assert!(
            runtime.index(&pk_index.name).is_none(),
            "reopen should stay lazy and avoid hydrating the primary-key btree"
        );
    };
    let explain = db
        .execute("EXPLAIN SELECT name FROM users ORDER BY id LIMIT 4 OFFSET 2")
        .expect("explain ordered projection");
    assert!(
        explain
            .explain_lines()
            .iter()
            .all(|line| !line.contains("TableScan(table=users)")),
        "expected no TableScan in explain, got: {:?}",
        explain.explain_lines()
    );
    assert!(
        explain
            .explain_lines()
            .iter()
            .all(|line| !line.contains("Sort")),
        "expected no Sort in explain, got: {:?}",
        explain.explain_lines()
    );

    let projected_key = db
        .execute("SELECT id FROM users ORDER BY id LIMIT 1")
        .expect("projected primary-key ordered projection");
    assert_eq!(projected_key.rows().len(), 1);
    assert_eq!(projected_key.rows()[0].values(), &[Value::Int64(1)]);

    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        let table = runtime
            .catalog
            .table("users")
            .expect("users table after ordered projection");
        assert!(
            table.pk_index_root.is_some(),
            "projected primary-key ordering should backfill the persistent locator"
        );
        let pk_index = runtime
            .catalog
            .indexes
            .values()
            .find(|index| {
                index.table_name.eq_ignore_ascii_case("users")
                    && index.unique
                    && index.kind == IndexKind::Btree
                    && index.columns.len() == 1
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|column| column.eq_ignore_ascii_case("id"))
            })
            .expect("primary-key btree index");
        assert!(
                runtime.index(&pk_index.name).is_none(),
                "projected primary-key ordering should use the persistent locator without hydrating a runtime btree"
            );
    }

    let result = db
        .execute("SELECT name FROM users ORDER BY id LIMIT 4 OFFSET 2")
        .expect("simple ordered projection");
    assert_eq!(result.rows().len(), 4);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("user-3".to_string())]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Text("user-4".to_string())]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Text("user-5".to_string())]
    );
    assert_eq!(
        result.rows()[3].values(),
        &[Value::Text("user-6".to_string())]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected ordered projection to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after ordered projection, got: {json_after}"
    );

    drop(db);
    let reopened = Db::open_or_create(&path, query_config).expect("reopen after backfill");
    {
        let runtime = reopened.inner.engine.read().expect("engine runtime lock");
        let table = runtime
            .catalog
            .table("users")
            .expect("users table after backfill reopen");
        assert!(
            table.pk_index_root.is_some(),
            "targeted primary-key locator backfill should persist across reopen"
        );
    }
}

#[test]
fn prepared_row_id_join_uses_deferred_locator_cache() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("prepared-deferred-row-id-join.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE join_users (id INT64 PRIMARY KEY, name TEXT, body TEXT)")
        .expect("create join_users");
    db.execute("CREATE TABLE join_profiles (id INT64 PRIMARY KEY, bio TEXT, body TEXT)")
        .expect("create join_profiles");

    let body = "x".repeat(1024);
    let mut txn = db.transaction().expect("begin txn");
    let users = txn
        .prepare("INSERT INTO join_users (id, name, body) VALUES ($1, $2, $3)")
        .expect("prepare users");
    let profiles = txn
        .prepare("INSERT INTO join_profiles (id, bio, body) VALUES ($1, $2, $3)")
        .expect("prepare profiles");
    for id in 1_i64..=256_i64 {
        users
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(id),
                    Value::Text(format!("u{id}")),
                    Value::Text(body.clone()),
                ],
            )
            .expect("insert user");
        profiles
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(id),
                    Value::Text(format!("b{id}")),
                    Value::Text(body.clone()),
                ],
            )
            .expect("insert profile");
    }
    txn.commit().expect("commit rows");

    {
        let runtime = db.inner.engine.read().expect("engine runtime lock");
        for table_name in ["join_users", "join_profiles"] {
            let table = runtime
                .persisted_tables
                .get(table_name)
                .expect("persisted table after commit");
            assert!(
                table.pointer.is_table_paged_manifest(),
                "expected benchmark-shaped table {table_name} to use paged storage"
            );
            assert!(
                runtime.has_deferred_paged_row_locator_cache_for_tests(table_name),
                "expected INT64 primary-key table {table_name} to build a deferred locator cache"
            );
        }
    }

    let prepared = db
        .prepare(
            "SELECT u.name, p.bio \
                 FROM join_users AS u \
                 JOIN join_profiles AS p ON u.id = p.id \
                 WHERE u.id = $1",
        )
        .expect("prepare join lookup");
    assert!(
        prepared.simple_row_id_join_projection.is_some(),
        "expected prepared join lookup to cache the row-id join plan"
    );
    let result = prepared
        .execute(&[Value::Int64(42)])
        .expect("execute prepared join lookup");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("u42".to_string()),
            Value::Text("b42".to_string())
        ]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after prepared join lookup");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected prepared join lookup to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected both join tables to remain deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after prepared deferred join lookup, got: {json_after}"
    );
}

#[test]
fn disabled_sync_write_does_not_buffer_runtime_mutations() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("disabled-sync-write.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .expect("create users");
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .expect("insert user");

    let runtime = db.inner.engine.read().expect("runtime read lock");
    assert!(
        !runtime.sync_capture_active(),
        "ordinary local writes should leave sync capture inactive"
    );
    assert!(
        runtime.sync_mutations.is_empty(),
        "ordinary local writes should not build sync mutation JSON"
    );
}

#[test]
fn repeated_autocommit_read_reuses_resident_paged_row_source() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("read-reuse-resident-paged-row-source.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create seeded");
        let large_body = "x".repeat(2048);
        for i in 0..50 {
            db.execute(&format!(
                "INSERT INTO seeded (id, n, body) VALUES ({i}, {i}, '{large_body}')"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, config).expect("reopen with paged storage");
    {
        let runtime = db.inner.engine.read().expect("runtime read lock");
        let seeded = runtime
            .persisted_tables
            .get("seeded")
            .expect("persisted table");
        assert!(
            seeded.pointer.is_table_paged_manifest(),
            "expected seeded table to stay behind paged row storage"
        );
    }
    let statement =
        parse_sql_statement("SELECT n FROM seeded WHERE id = 1").expect("parse point lookup");
    assert!(db
        .load_statement_row_sources_at_latest_snapshot(&statement)
        .expect("load table before read"));

    {
        let runtime = db.inner.engine.read().expect("runtime read lock");
        assert!(
            !runtime
                .deferred_table_names()
                .any(|name| name.eq_ignore_ascii_case("seeded")),
            "table should be materialized before read"
        );
    }

    let result = db
        .execute("SELECT n FROM seeded WHERE id = 1")
        .expect("repeated read");
    assert_eq!(scalar_i64(&result), 1);
    {
        let runtime = db.inner.engine.read().expect("runtime read lock");
        assert!(
            !runtime
                .deferred_table_names()
                .any(|name| name.eq_ignore_ascii_case("seeded")),
            "hot read should keep row source resident on first execution"
        );
        let residency = db
            .inner
            .read_only_paged_row_source_residency
            .lock()
            .expect("residency lock");
        assert!(residency
            .table_touch_generation
            .keys()
            .any(|name| name.eq_ignore_ascii_case("seeded")));
    }

    let result = db
        .execute("SELECT n FROM seeded WHERE id = 2")
        .expect("repeated read");
    assert_eq!(scalar_i64(&result), 2);
    {
        let runtime = db.inner.engine.read().expect("runtime read lock");
        assert!(
            !runtime
                .deferred_table_names()
                .any(|name| name.eq_ignore_ascii_case("seeded")),
            "repeated hot read should avoid re-deferral"
        );
        let residency = db
            .inner
            .read_only_paged_row_source_residency
            .lock()
            .expect("residency lock");
        assert_eq!(residency.table_touch_generation.len(), 1);
        assert!(residency
            .table_touch_generation
            .keys()
            .any(|name| name.eq_ignore_ascii_case("seeded")));
    }
}

#[test]
fn retain_paged_row_sources_after_commit_keeps_hot_transaction_rows_loaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("retain-paged-row-sources.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        retain_paged_row_sources_after_commit: true,
        ..DbConfig::default()
    };

    let db = Db::open_or_create(&path, config).expect("create db");
    db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
        .expect("create seeded");
    let mut txn = db.transaction().expect("begin txn");
    let insert = txn
        .prepare("INSERT INTO seeded VALUES ($1, $2, $3)")
        .expect("prepare insert");
    for i in 0_i64..32_i64 {
        insert
            .execute_in(
                &mut txn,
                &[
                    Value::Int64(i),
                    Value::Int64(i * 10),
                    Value::Text("x".repeat(2048)),
                ],
            )
            .expect("insert row");
    }
    txn.commit().expect("commit txn");

    {
        let runtime = db.inner.engine.read().expect("runtime read lock");
        let persisted = runtime
            .persisted_tables
            .get("seeded")
            .expect("persisted seeded");
        assert!(
            persisted.pointer.is_table_paged_manifest(),
            "hot retained table should still persist using paged row storage"
        );
        assert!(
            matches!(
                runtime.tables.get("seeded"),
                Some(TableRowSource::Resident(_))
            ),
            "hot retained table should stay loaded on the current handle"
        );
        assert!(
            !runtime
                .deferred_table_names()
                .any(|name| name.eq_ignore_ascii_case("seeded")),
            "hot retained table should not be re-deferred after commit"
        );
    }

    let json = db
        .inspect_storage_state_json()
        .expect("inspect retained state");
    assert!(
        json.contains("\"loaded_table_count\":1"),
        "expected retained row source to count as loaded, got: {json}"
    );
    assert!(
        json.contains("\"deferred_table_count\":0"),
        "expected no deferred tables with hot retention enabled, got: {json}"
    );

    db.checkpoint().expect("checkpoint");
    drop(db);
    let reopened = Db::open_or_create(&path, DbConfig::default()).expect("reopen default");
    let reopened_json = reopened
        .inspect_storage_state_json()
        .expect("inspect reopened state");
    assert!(
        reopened_json.contains("\"loaded_table_count\":0"),
        "default reopen should keep paged tables deferred, got: {reopened_json}"
    );
    assert!(
        reopened_json.contains("\"deferred_table_count\":1"),
        "default reopen should preserve deferred memory profile, got: {reopened_json}"
    );
}

#[test]
fn prepared_read_statement_schema_change_invalidates_stale_statement() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("prepared-read-schema-invalidates-when-stale.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
        .expect("create seeded");
    db.execute("INSERT INTO seeded (id, n) VALUES (1, 10)")
        .expect("insert");

    let prepared = db
        .prepare("SELECT n FROM seeded WHERE id = $1")
        .expect("prepare lookup");
    let lookup = prepared
        .execute(&[Value::Int64(1)])
        .expect("prepared lookup");
    assert_eq!(scalar_i64(&lookup), 10);

    db.execute("ALTER TABLE seeded ADD COLUMN note TEXT")
        .expect("add column");

    let stale_error = prepared
        .execute(&[Value::Int64(1)])
        .expect_err("stale prepared execute");
    assert!(
        stale_error
            .to_string()
            .contains("prepared statement is no longer valid because the schema changed"),
        "expected schema-change invalidation, got: {stale_error}"
    );

    let repaired = db
        .prepare("SELECT n FROM seeded WHERE id = $1")
        .expect("reprepare lookup");
    let lookup = repaired
        .execute(&[Value::Int64(1)])
        .expect("prepared lookup after re-prepare");
    assert_eq!(scalar_i64(&lookup), 10);
}

#[test]
fn simple_table_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-simple-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT n FROM seeded")
        .expect("simple projection");
    assert_eq!(result.rows().len(), 50);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(0)]);
    assert_eq!(result.rows()[49].values(), &[Value::Int64(49)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after simple projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected simple projection to avoid resident materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after simple projection, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after simple projection, got: {json_after}"
    );
}

#[test]
fn wildcard_table_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-wildcard-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT * FROM seeded")
        .expect("wildcard projection");
    assert_eq!(result.rows().len(), 50);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(0)]
    );
    assert_eq!(
        result.rows()[49].values(),
        &[Value::Int64(49), Value::Int64(49)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after wildcard projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected wildcard projection to avoid resident materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after wildcard projection, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after wildcard projection, got: {json_after}"
    );
}

#[test]
fn simple_filtered_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-filtered-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT n FROM seeded WHERE n >= 10 AND n <= 12 ORDER BY n")
        .expect("filtered projection");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(10)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(11)]);
    assert_eq!(result.rows()[2].values(), &[Value::Int64(12)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after filtered projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected filtered projection to avoid resident materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after filtered projection, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after filtered projection, got: {json_after}"
    );
}

#[test]
fn expression_filtered_column_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-expression-filtered-column-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER, m INTEGER)")
            .expect("create seeded");
        for (id, n, m) in [(0, 1, 1), (1, 2, 4), (2, 3, 1), (3, 1, 0)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, n, m) VALUES ({id}, {n}, {m})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT n FROM seeded WHERE n + m >= 5")
        .expect("expression filtered column projection");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after expression filtered column projection");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected expression filtered column projection to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after expression filtered column projection, got: {json_after}"
        );
    assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after expression filtered column projection, got: {json_after}"
        );
}

#[test]
fn ordered_filtered_projection_with_offset_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-filtered-projection-offset.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT n FROM seeded WHERE n >= 10 AND n <= 20 ORDER BY n DESC LIMIT 2 OFFSET 1")
        .expect("ordered filtered projection with offset");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(19)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(18)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered filtered projection with offset");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered filtered projection with offset to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered filtered projection with offset, got: {json_after}"
        );
    assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered filtered projection with offset, got: {json_after}"
        );
}

#[test]
fn distinct_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-distinct-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER)")
            .expect("create seeded");
        for i in 0..64 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp) VALUES ({i}, {})",
                i % 4
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT DISTINCT grp FROM seeded")
        .expect("distinct projection");
    assert_eq!(result.rows().len(), 4);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after distinct projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected distinct projection to avoid resident materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after distinct projection, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after distinct projection, got: {json_after}"
    );
}

#[test]
fn ordered_distinct_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-ordered-distinct-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER)")
            .expect("create seeded");
        for i in 0..64 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp) VALUES ({i}, {})",
                i % 4
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT DISTINCT grp FROM seeded ORDER BY grp DESC LIMIT 2 OFFSET 1")
        .expect("ordered distinct projection");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(1)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered distinct projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected ordered distinct projection to avoid resident materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered distinct projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after ordered distinct projection, got: {json_after}"
    );
}

#[test]
fn distinct_filtered_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-distinct-filtered-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for i in 0..64 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                i % 4
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT DISTINCT grp FROM seeded WHERE n >= 10 AND n <= 19")
        .expect("distinct filtered projection");
    assert_eq!(result.rows().len(), 4);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after distinct filtered projection");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected distinct filtered projection to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after distinct filtered projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after distinct filtered projection, got: {json_after}"
    );
}

#[test]
fn distinct_expression_filtered_column_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-distinct-expression-filtered-column-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
        )
        .expect("create seeded");
        for (id, grp, n, m) in [(0, 0, 1, 1), (1, 0, 2, 4), (2, 1, 3, 1), (3, 1, 2, 3)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT DISTINCT grp FROM seeded WHERE n + m >= 5 ORDER BY grp ASC")
        .expect("distinct expression filtered column projection");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(0)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(1)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after distinct expression filtered column projection");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected distinct expression filtered column projection to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after distinct expression filtered column projection, got: {json_after}"
        );
    assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after distinct expression filtered column projection, got: {json_after}"
        );
}

#[test]
fn ordered_distinct_filtered_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-ordered-distinct-filtered-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for i in 0..64 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                i % 4
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
            .execute(
                "SELECT DISTINCT grp FROM seeded WHERE n >= 10 AND n <= 20 ORDER BY grp DESC LIMIT 2 OFFSET 1",
            )
            .expect("ordered distinct filtered projection");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(1)]);

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered distinct filtered projection");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered distinct filtered projection to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered distinct filtered projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after ordered distinct filtered projection, got: {json_after}"
    );
}

#[test]
fn ordered_distinct_filtered_projection_multi_order_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-ordered-distinct-filtered-projection-multi-order.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for (id, grp, n) in [
            (0, 0, 10),
            (1, 0, 11),
            (2, 1, 10),
            (3, 1, 11),
            (4, 2, 10),
            (5, 2, 11),
        ] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT DISTINCT grp, n FROM seeded \
                 WHERE n >= 10 AND n <= 11 \
                 ORDER BY grp DESC, n ASC LIMIT 4 OFFSET 1",
        )
        .expect("ordered distinct filtered projection multi-order");
    assert_eq!(result.rows().len(), 4);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Int64(11)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(10)]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(1), Value::Int64(11)]
    );
    assert_eq!(
        result.rows()[3].values(),
        &[Value::Int64(0), Value::Int64(10)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered distinct filtered projection multi-order");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected ordered distinct filtered projection multi-order to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered distinct filtered projection multi-order, got: {json_after}"
        );
    assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after ordered distinct filtered projection multi-order, got: {json_after}"
        );
}

#[test]
fn qualified_wildcard_filtered_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-qualified-wildcard-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT s.* FROM seeded AS s WHERE s.n >= 10 AND s.n <= 12 ORDER BY s.n")
        .expect("qualified wildcard filtered projection");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(10), Value::Int64(10)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(11), Value::Int64(11)]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(12), Value::Int64(12)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after qualified wildcard filtered projection");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected qualified wildcard filtered projection to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after qualified wildcard filtered projection, got: {json_after}"
        );
    assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after qualified wildcard filtered projection, got: {json_after}"
        );
}

#[test]
fn wildcard_ordered_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-wildcard-ordered-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..48 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT * FROM seeded ORDER BY n DESC LIMIT 2 OFFSET 1")
        .expect("ordered wildcard projection");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(46), Value::Int64(46)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(45), Value::Int64(45)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered wildcard projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected ordered wildcard projection to avoid resident materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after ordered wildcard projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after ordered wildcard projection, got: {json_after}"
    );
}

#[test]
fn simple_grouped_numeric_aggregate_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-aggregate.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for i in 0..10 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                i % 2
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT grp, COUNT(*), SUM(n) FROM seeded WHERE n >= 2 AND n <= 7 GROUP BY grp")
        .expect("grouped aggregate");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(3), Value::Int64(12)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(3), Value::Int64(15)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped aggregate to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after grouped aggregate, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped aggregate, got: {json_after}"
    );
}

#[test]
fn simple_grouped_avg_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-avg.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for i in 0..10 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                i % 2
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, SUM(n) AS total, AVG(n) AS avg_n FROM seeded \
                 WHERE n >= 2 AND n <= 7 GROUP BY grp HAVING avg_n >= 4 \
                 ORDER BY avg_n DESC LIMIT 1",
        )
        .expect("grouped avg aggregate");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(15), Value::Float64(5.0)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped avg aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped avg aggregate to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after grouped avg aggregate, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped avg aggregate, got: {json_after}"
    );
}

#[test]
fn simple_grouped_multi_column_numeric_aggregates_keep_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-multi-column-aggregate.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
        )
        .expect("create seeded");
        for (id, grp, n, m) in [(0, 0, 2, 10), (1, 0, 4, 20), (2, 1, 6, 30), (3, 1, 8, 50)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, SUM(n) AS total_n, AVG(m) AS avg_m \
                 FROM seeded GROUP BY grp ORDER BY grp ASC",
        )
        .expect("grouped multi-column numeric aggregate");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(6), Value::Float64(15.0)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(14), Value::Float64(40.0)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped multi-column aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped multi-column aggregate to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped multi-column aggregate, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped multi-column aggregate, got: {json_after}"
    );
}

#[test]
fn simple_grouped_numeric_expression_aggregates_keep_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-expression-aggregate.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
        )
        .expect("create seeded");
        for (id, grp, n, m) in [(0, 0, 2, 10), (1, 0, 4, 20), (2, 1, 6, 30), (3, 1, 8, 50)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, SUM(n + m) AS total, AVG(m - n) AS avg_delta \
                 FROM seeded GROUP BY grp HAVING SUM(n + m) >= 30 \
                 ORDER BY total DESC, grp ASC",
        )
        .expect("grouped numeric expression aggregate");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(94), Value::Float64(33.0)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(0), Value::Int64(36), Value::Float64(12.0)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped expression aggregate");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped expression aggregate to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped expression aggregate, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped expression aggregate, got: {json_after}"
    );
}

#[test]
fn simple_grouped_expression_bucket_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-expression-bucket.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for i in 0..10 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                i % 2
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT n / 2 AS bucket, COUNT(*) FROM seeded GROUP BY n / 2 ORDER BY bucket")
        .expect("grouped expression count");
    assert_eq!(result.rows().len(), 5);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[4].values(),
        &[Value::Int64(4), Value::Int64(2)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped expression count");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped expression count to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped expression count, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped expression count, got: {json_after}"
    );
}

#[test]
fn simple_grouped_wrapped_group_projection_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-wrapped-group-projection.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for (id, grp, n) in [(0, 0, 1), (1, 0, 4), (2, 1, 2), (3, 1, 3)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp + 1 AS next_grp, SUM(n) AS total FROM seeded \
                 GROUP BY grp ORDER BY next_grp DESC",
        )
        .expect("grouped wrapped group projection");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(2), Value::Int64(5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(5)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped wrapped group projection");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped wrapped group projection to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped group projection, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped wrapped group projection, got: {json_after}"
    );
}

#[test]
fn simple_grouped_wrapped_sum_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-wrapped-sum.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for i in 0..10 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                i % 2
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, -SUM(n) AS neg_total FROM seeded \
                 GROUP BY grp ORDER BY neg_total LIMIT 1",
        )
        .expect("grouped wrapped sum");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(-25)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped wrapped sum");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped wrapped sum to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after grouped wrapped sum, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped wrapped sum, got: {json_after}"
    );
}

#[test]
fn simple_grouped_wrapped_min_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-wrapped-min.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, word TEXT)")
            .expect("create seeded");
        for (id, grp, word) in [
            (0, 0, "beta"),
            (1, 0, "alpha"),
            (2, 1, "gamma"),
            (3, 1, "delta"),
        ] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, word) VALUES ({id}, {grp}, '{word}')"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, UPPER(MIN(word)) AS upper_min FROM seeded \
                 GROUP BY grp ORDER BY upper_min DESC LIMIT 1",
        )
        .expect("grouped wrapped min");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Text("DELTA".to_string())]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped wrapped min");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped wrapped min to avoid materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after grouped wrapped min, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped wrapped min, got: {json_after}"
    );
}

#[test]
fn simple_grouped_wrapped_min_having_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-wrapped-min-having.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, word TEXT)")
            .expect("create seeded");
        for (id, grp, word) in [
            (0, 0, "beta"),
            (1, 0, "alpha"),
            (2, 1, "gamma"),
            (3, 1, "delta"),
        ] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, word) VALUES ({id}, {grp}, '{word}')"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, UPPER(MIN(word)) AS upper_min FROM seeded \
                 GROUP BY grp HAVING UPPER(MIN(word)) >= 'DELTA' \
                 ORDER BY upper_min DESC",
        )
        .expect("grouped wrapped min with having");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Text("DELTA".to_string())]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped wrapped min with having");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped wrapped min with having to avoid materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped min with having, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped wrapped min with having, got: {json_after}"
    );
}

#[test]
fn simple_grouped_count_aggregate_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-count.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for i in 0..10 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                i % 2
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute("SELECT grp, COUNT(*) FROM seeded GROUP BY grp")
        .expect("grouped count");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(5)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped count");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped count to avoid resident materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after grouped count, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped count, got: {json_after}"
    );
}

#[test]
fn simple_grouped_count_with_order_limit_offset_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-count-ordered.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for i in 0..10 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                i % 2
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, COUNT(*) AS c FROM seeded GROUP BY grp ORDER BY grp DESC LIMIT 1 OFFSET 1",
        )
        .expect("ordered grouped count");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(5)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after ordered grouped count");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected ordered grouped count to avoid resident materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after ordered grouped count, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after ordered grouped count, got: {json_after}"
    );
}

#[test]
fn simple_grouped_numeric_multi_order_by_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-numeric-multi-order.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for (id, grp, n) in [(0, 0, 1), (1, 0, 4), (2, 1, 2), (3, 1, 3)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, SUM(n) AS total, AVG(n) AS avg FROM seeded \
                 GROUP BY grp HAVING SUM(n) >= 3 \
                 ORDER BY total DESC, grp ASC",
        )
        .expect("grouped numeric multi-order");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(5), Value::Float64(2.5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(5), Value::Float64(2.5)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped numeric multi-order");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped numeric multi-order to avoid resident materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped numeric multi-order, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped numeric multi-order, got: {json_after}"
    );
}

#[test]
fn simple_grouped_wrapped_count_multi_order_by_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-wrapped-count-multi-order.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER)")
            .expect("create seeded");
        for (id, grp) in [(0, 1), (1, 1), (2, 0), (3, 0)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp) VALUES ({id}, {grp})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, COUNT(*) + 1 AS cnt FROM seeded \
                 GROUP BY grp HAVING COUNT(*) + 1 >= 3 \
                 ORDER BY cnt DESC, grp ASC",
        )
        .expect("grouped wrapped count multi-order");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(3)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(3)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped wrapped count multi-order");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped wrapped count multi-order to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped count multi-order, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped wrapped count multi-order, got: {json_after}"
    );
}

#[test]
fn simple_grouped_wrapped_group_count_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-wrapped-group-count.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for id in 0..6 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({id}, {id})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT n / 2 + 1 AS bucket, COUNT(*) AS c FROM seeded \
                 GROUP BY n / 2 ORDER BY bucket DESC",
        )
        .expect("grouped wrapped group count");
    assert_eq!(result.rows().len(), 3);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(3), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[2].values(),
        &[Value::Int64(1), Value::Int64(2)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped wrapped group count");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped wrapped group count to avoid resident materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped wrapped group count, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped wrapped group count, got: {json_after}"
    );
}

#[test]
fn simple_grouped_multiple_count_rows_keep_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-multiple-count-rows.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER)")
            .expect("create seeded");
        for (id, grp) in [(0, 0), (1, 0), (2, 1)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp) VALUES ({id}, {grp})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, COUNT(*) AS c, COUNT(*) + 1 AS c_plus_one \
                 FROM seeded GROUP BY grp ORDER BY grp ASC",
        )
        .expect("grouped repeated count");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(2), Value::Int64(3)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(1), Value::Int64(2)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped repeated count");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped repeated count to avoid resident materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped repeated count, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped repeated count, got: {json_after}"
    );
}

#[test]
fn simple_grouped_count_having_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-count-having.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for i in 0..10 {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({i}, {}, {i})",
                i % 2
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
            .execute(
                "SELECT grp, COUNT(*) AS c FROM seeded GROUP BY grp HAVING c >= 5 ORDER BY grp DESC LIMIT 1 OFFSET 1",
            )
            .expect("grouped count with having");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(5)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped count with having");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped count with having to avoid resident materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped count with having, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped count with having, got: {json_after}"
    );
}

#[test]
fn simple_grouped_count_expr_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-count-expr.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for (id, grp, n) in [(0, 0, "1"), (1, 0, "NULL"), (2, 1, "5"), (3, 1, "7")] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, COUNT(n) AS present, COUNT(n + 1) AS shifted \
                 FROM seeded GROUP BY grp HAVING COUNT(n) >= 1 \
                 ORDER BY present DESC, grp ASC",
        )
        .expect("grouped count expr");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(2), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(0), Value::Int64(1), Value::Int64(1)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped count expr");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped count expr to avoid resident materialization, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected deferred table to remain deferred after grouped count expr, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped count expr, got: {json_after}"
    );
}

#[test]
fn simple_grouped_count_distinct_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-count-distinct.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for (id, grp, n) in [
            (0, 0, "1"),
            (1, 0, "1"),
            (2, 0, "NULL"),
            (3, 1, "2"),
            (4, 1, "3"),
            (5, 1, "3"),
        ] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, COUNT(DISTINCT n) AS uniq, COUNT(DISTINCT n + 1) AS shifted \
                 FROM seeded GROUP BY grp HAVING COUNT(DISTINCT n) >= 1 \
                 ORDER BY uniq DESC, grp ASC",
        )
        .expect("grouped count distinct");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(2), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(0), Value::Int64(1), Value::Int64(1)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped count distinct");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped count distinct to avoid resident materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped count distinct, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped count distinct, got: {json_after}"
    );
}

#[test]
fn simple_grouped_numeric_distinct_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("deferred-grouped-numeric-distinct.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for (id, grp, n) in [
            (0, 0, 1),
            (1, 0, 1),
            (2, 0, 2),
            (3, 1, 2),
            (4, 1, 3),
            (5, 1, 3),
        ] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, SUM(DISTINCT n) AS total, AVG(DISTINCT n + 1) AS shifted_avg \
                 FROM seeded GROUP BY grp HAVING SUM(DISTINCT n) >= 3 \
                 ORDER BY total DESC, grp ASC",
        )
        .expect("grouped numeric distinct");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(5), Value::Float64(3.5)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(0), Value::Int64(3), Value::Float64(2.5)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped numeric distinct");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped numeric distinct to avoid resident materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped numeric distinct, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped numeric distinct, got: {json_after}"
    );
}

#[test]
fn simple_grouped_having_only_aggregate_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-having-only-aggregate.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER)")
            .expect("create seeded");
        for (id, grp, n) in [(0, 0, 1), (1, 0, 4), (2, 1, 1)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n) VALUES ({id}, {grp}, {n})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, COUNT(*) AS c FROM seeded \
                 GROUP BY grp HAVING SUM(n) >= 3 ORDER BY grp ASC",
        )
        .expect("grouped having-only aggregate");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(2)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped having-only aggregate");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped having-only aggregate to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped having-only aggregate, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped having-only aggregate, got: {json_after}"
    );
}

#[test]
fn simple_grouped_count_with_expression_filter_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-count-expression-filter.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
        )
        .expect("create seeded");
        for (id, grp, n, m) in [(0, 0, 1, 1), (1, 0, 2, 4), (2, 1, 3, 1), (3, 1, 1, 0)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, COUNT(*) AS c FROM seeded \
                 WHERE n + m >= 5 GROUP BY grp ORDER BY grp ASC",
        )
        .expect("grouped count with expression filter");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(1)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped count with expression filter");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped count with expression filter to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped count with expression filter, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped count with expression filter, got: {json_after}"
    );
}

#[test]
fn simple_grouped_numeric_with_expression_filter_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-numeric-expression-filter.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, m INTEGER)",
        )
        .expect("create seeded");
        for (id, grp, n, m) in [(0, 0, 1, 1), (1, 0, 2, 4), (2, 1, 3, 1), (3, 1, 1, 0)] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n, m) VALUES ({id}, {grp}, {n}, {m})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, SUM(n) AS total FROM seeded \
                 WHERE n + m >= 4 GROUP BY grp ORDER BY grp ASC",
        )
        .expect("grouped numeric with expression filter");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(2)]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(3)]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped numeric with expression filter");
    assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected grouped numeric with expression filter to avoid resident materialization, got: {json_after}"
        );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped numeric with expression filter, got: {json_after}"
        );
    assert!(
            json_after.contains("\"rows_in_memory_count\":0"),
            "expected zero resident rows after grouped numeric with expression filter, got: {json_after}"
        );
}

#[test]
fn simple_grouped_total_variance_bool_keeps_deferred_table_unloaded() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("deferred-grouped-total-variance-bool.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute(
            "CREATE TABLE seeded (id INTEGER PRIMARY KEY, grp INTEGER, n INTEGER, flag BOOLEAN)",
        )
        .expect("create seeded");
        for (id, grp, n, flag) in [
            (0, 0, 1, true),
            (1, 0, 1, true),
            (2, 0, 3, false),
            (3, 1, 2, true),
            (4, 1, 4, true),
        ] {
            db.execute(&format!(
                "INSERT INTO seeded (id, grp, n, flag) VALUES ({id}, {grp}, {n}, {flag})"
            ))
            .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen with defer");
    let result = db
        .execute(
            "SELECT grp, TOTAL(DISTINCT n) AS total_n, VAR_SAMP(DISTINCT n) AS spread, \
                 BOOL_AND(DISTINCT flag) AS all_true \
                 FROM seeded GROUP BY grp HAVING TOTAL(DISTINCT n) >= 4 ORDER BY grp ASC",
        )
        .expect("grouped total variance bool");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(0),
            Value::Float64(4.0),
            Value::Float64(2.0),
            Value::Bool(false),
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Float64(6.0),
            Value::Float64(2.0),
            Value::Bool(true),
        ]
    );

    let json_after = db
        .inspect_storage_state_json()
        .expect("json after grouped total variance bool");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped total variance bool to avoid resident materialization, got: {json_after}"
    );
    assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected deferred table to remain deferred after grouped total variance bool, got: {json_after}"
        );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0"),
        "expected zero resident rows after grouped total variance bool, got: {json_after}"
    );
}

#[test]
fn defer_table_materialization_false_preserves_eager_load_at_open() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("eager-load.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE seeded (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create seeded");
        for i in 0..50 {
            db.execute(&format!("INSERT INTO seeded (id, n) VALUES ({i}, {i})"))
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(
        &path,
        DbConfig {
            defer_table_materialization: false,
            ..DbConfig::default()
        },
    )
    .expect("reopen with eager load");

    let json_open = db.inspect_storage_state_json().expect("json snapshot");
    assert!(
        json_open.contains("\"deferred_table_count\":0"),
        "expected zero deferred tables at open, got: {json_open}"
    );
    assert!(
        json_open.contains("\"rows_in_memory_count\":50"),
        "expected eager load to materialize rows at open, got: {json_open}"
    );
}

#[test]
fn per_table_load_matches_mixed_case_identifiers_case_insensitively() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("mixed-case.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE \"ArtistStaging\" (id INTEGER PRIMARY KEY, name TEXT)")
            .expect("create mixed-case table");
        db.execute("INSERT INTO \"ArtistStaging\" VALUES (1, 'alpha')")
            .expect("insert seed row");
        db.checkpoint().expect("checkpoint before close");
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen");
    let json_open = db.inspect_storage_state_json().expect("json at open");
    assert!(
        json_open.contains("\"deferred_table_count\":1"),
        "expected deferred table at open, got: {json_open}"
    );

    let result = db
        .execute("SELECT UPPER(name) FROM ArtistStaging ORDER BY name LIMIT 1")
        .expect("query mixed-case table from unquoted SQL");
    assert_eq!(scalar_text(&result), "ALPHA");

    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"loaded_table_count\":0,"),
        "expected mixed-case expression projection to stay deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1}"),
        "expected mixed-case table to remain deferred after query, got: {json_after}"
    );
}

/// ADR 0143 Phase B: per-table on-demand load - query only small table
/// should not materialize the large table.
#[test]
fn per_table_load_skips_large_table() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("per-table.ddb");

    // Seed the DB with both tables - small and large
    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE small (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create small");
        db.execute("CREATE TABLE large (id INTEGER PRIMARY KEY, data TEXT)")
            .expect("create large");

        for i in 0..10 {
            db.execute(&format!("INSERT INTO small (id, n) VALUES ({i}, {i})"))
                .expect("insert small");
        }
        db.checkpoint().expect("checkpoint small");
        for i in 0..1000 {
            let data = format!("data-{}", i);
            db.execute(&format!(
                "INSERT INTO large (id, data) VALUES ({i}, '{}')",
                data
            ))
            .expect("insert large");
        }
        db.checkpoint().expect("checkpoint before close");
    }

    // Re-open with deferred materialization
    let cfg = DbConfig {
        defer_table_materialization: true,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, cfg).expect("reopen with defer");

    // At open, both tables should be deferred
    let json_open = db.inspect_storage_state_json().expect("json at open");
    assert!(
        json_open.contains("\"deferred_table_count\":2"),
        "expected 2 deferred tables at open, got: {json_open}"
    );

    // Query only the small table with a single-table expression
    // projection. The deferred expression path should answer from
    // persisted bytes without loading either table.
    let result = db
        .execute("SELECT n + 1 FROM small ORDER BY n LIMIT 1")
        .expect("query small");
    assert_eq!(scalar_i64(&result), 1);

    // After query both tables should still be deferred because the
    // executor streamed from persisted bytes instead of materializing
    // `small`. Use precise field assertions (with trailing comma) to
    // avoid the substring-match bug where `:10` matches `:1010`.
    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"deferred_table_count\":2}"),
        "expected both tables to remain deferred after small-only query, got: {json_after}"
    );
    assert!(
        json_after.contains("\"loaded_table_count\":0,"),
        "expected zero loaded tables after small-only query, got: {json_after}"
    );
    assert!(
        json_after.contains("\"rows_in_memory_count\":0,"),
        "expected zero resident rows after deferred expression projection, got: {json_after}"
    );
}

/// ADR 0143 Phase B+: safe expression subqueries can now stay on the
/// snapshot-local row-source path instead of forcing a live-runtime
/// load-all fallback.
#[test]
fn per_table_load_keeps_runtime_deferred_for_safe_subquery() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("subq.ddb");
    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("create db");
        db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create a");
        db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, n INTEGER)")
            .expect("create b");
        for i in 0..3 {
            db.execute(&format!("INSERT INTO a VALUES ({i},{i})"))
                .expect("ins a");
            db.execute(&format!("INSERT INTO b VALUES ({i},{i})"))
                .expect("ins b");
        }
        db.checkpoint().expect("checkpoint");
    }
    let cfg = DbConfig {
        defer_table_materialization: true,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, cfg).expect("reopen");
    db.execute("SELECT COUNT(*) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.n = a.n)")
        .expect("query with subquery");
    let json = db.inspect_storage_state_json().expect("json");
    assert!(
        json.contains("\"deferred_table_count\":2"),
        "expected both tables to remain deferred after safe subquery execution, got: {json}"
    );
    assert!(
        json.contains("\"loaded_table_count\":0,"),
        "expected safe subquery execution to avoid live-runtime table loads, got: {json}"
    );
}

#[test]
fn paged_row_storage_mixed_update_and_append_in_transaction_persists_update() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("mixed-update-append.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        defer_table_materialization: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute("CREATE TABLE ef_batch_entities (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .expect("create table");
        db.execute("INSERT INTO ef_batch_entities (name) VALUES ('pre-existing')")
            .expect("insert seed row");
    }

    {
        let db = Db::open_or_create(&path, config.clone()).expect("reopen db");
        db.begin_transaction().expect("begin transaction");
        db.execute("UPDATE ef_batch_entities SET name = 'updated' WHERE id = 1")
            .expect("update seed row");
        for i in 0..5 {
            db.execute(&format!(
                "INSERT INTO ef_batch_entities (name) VALUES ('add{i}')"
            ))
            .expect("insert appended row");
        }
        db.commit_transaction().expect("commit transaction");
    }

    let db = Db::open_or_create(&path, config).expect("reopen verify db");
    let count = db
        .execute("SELECT COUNT(*) FROM ef_batch_entities")
        .expect("count rows");
    assert_eq!(scalar_i64(&count), 6);

    let names = db
        .execute("SELECT name FROM ef_batch_entities ORDER BY id")
        .expect("read names");
    let names = names
        .rows()
        .iter()
        .map(|row| match &row.values()[0] {
            Value::Text(value) => value.as_str(),
            other => panic!("expected TEXT value, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(names, ["updated", "add0", "add1", "add2", "add3", "add4"]);
}

#[test]
fn paged_row_storage_alter_table_add_column_materializes_rows() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-alter-table.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        defer_table_materialization: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("create db");
        db.execute(
            "CREATE TABLE migration_contacts (id INTEGER PRIMARY KEY, display_name TEXT NOT NULL)",
        )
        .expect("create table");
        db.execute("INSERT INTO migration_contacts (display_name) VALUES ('Alice')")
            .expect("insert seed row");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");
    db.execute("ALTER TABLE migration_contacts ADD slug TEXT NOT NULL DEFAULT 'pending'")
        .expect("add defaulted column");

    let row = db
        .execute("SELECT display_name, slug FROM migration_contacts WHERE id = 1")
        .expect("read altered row");
    assert_eq!(row.rows()[0].values()[0], Value::Text("Alice".to_string()));
    assert_eq!(
        row.rows()[0].values()[1],
        Value::Text("pending".to_string())
    );
}

#[test]
fn paged_row_storage_grouped_query_after_reopen_stays_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-grouped-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)")
            .expect("create sales");
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO sales VALUES ($1, $2, $3)")
            .expect("prepare");
        let regions = ["east", "west", "north", "south"];
        for i in 0_i64..200_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Text(regions[(i as usize) % 4].to_string()),
                        Value::Int64((i % 50) + 1),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");

    let result = db
        .execute(
            "SELECT region, GROUP_CONCAT(amount) AS amounts \
                 FROM sales GROUP BY region ORDER BY region",
        )
        .expect("grouped query after reopen");
    assert_eq!(result.rows().len(), 4);

    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped query to re-defer sales after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected sales to be deferred again after grouped query, got: {json_after}"
    );
}

/// D-E1: Reopen-time grouped query with HAVING + ORDER BY on a paged table.
#[test]
fn paged_row_storage_grouped_having_order_by_after_reopen_stays_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-grouped-having-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, total INTEGER)")
            .expect("create orders");
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO orders VALUES ($1, $2, $3)")
            .expect("prepare");
        let customers = ["alice", "bob", "carol"];
        for i in 0_i64..150_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Text(customers[(i as usize) % 3].to_string()),
                        Value::Int64((i % 100) + 10),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");

    let result = db
        .execute(
            "SELECT customer, SUM(total) AS grand_total FROM orders \
                 GROUP BY customer HAVING SUM(total) > 2000 ORDER BY grand_total DESC",
        )
        .expect("grouped having query after reopen");
    assert!(!result.rows().is_empty());

    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped HAVING query to re-defer orders after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected orders to be deferred again after grouped HAVING query, got: {json_after}"
    );
}

/// D-E1: Grouped query after reopen (autocommit path).
#[test]
fn paged_row_storage_grouped_query_autocommit_after_reopen() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-grouped-autocommit.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT, ts INTEGER)")
            .expect("create events");
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO events VALUES ($1, $2, $3)")
            .expect("prepare");
        let kinds = ["login", "logout", "click", "view"];
        for i in 0_i64..120_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Text(kinds[(i as usize) % 4].to_string()),
                        Value::Int64(1700000000 + i * 60),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");

    let result = db
        .execute("SELECT kind, COUNT(*) AS cnt FROM events GROUP BY kind ORDER BY kind")
        .expect("grouped query after reopen");
    assert_eq!(result.rows().len(), 4);

    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected grouped query to keep events deferred, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected events to remain deferred after grouped query, got: {json_after}"
    );
}

/// D-E1: Mixed aggregate projection not handled by current specialization
/// (GROUP_CONCAT + COUNT) exercises the general grouped path.
#[test]
fn paged_row_storage_mixed_aggregate_grouped_after_reopen() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("paged-row-storage-mixed-aggregate.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT, msg TEXT)")
            .expect("create logs");
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO logs VALUES ($1, $2, $3)")
            .expect("prepare");
        let levels = ["INFO", "WARN", "ERROR"];
        for i in 0_i64..90_i64 {
            insert
                .execute_in(
                    &mut txn,
                    &[
                        Value::Int64(i + 1),
                        Value::Text(levels[(i as usize) % 3].to_string()),
                        Value::Text(format!("message {}", i)),
                    ],
                )
                .expect("insert row");
        }
        txn.commit().expect("commit seed txn");
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");

    let result = db
        .execute(
            "SELECT level, COUNT(*) AS cnt, GROUP_CONCAT(msg) AS messages \
                 FROM logs GROUP BY level ORDER BY level",
        )
        .expect("mixed aggregate grouped query after reopen");
    assert_eq!(result.rows().len(), 3);

    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected mixed aggregate grouped query to re-defer logs after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":1"),
        "expected logs to be deferred again after mixed aggregate query, got: {json_after}"
    );
}

/// D-E2: Reopen-time INNER JOIN on paged tables stays deferred.
#[test]
fn paged_row_storage_inner_join_after_reopen_stays_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-inner-join-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)")
            .expect("create authors");
        db.execute("CREATE TABLE books (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)")
            .expect("create books");
        for i in 0_i64..50_i64 {
            db.execute(&format!(
                "INSERT INTO authors VALUES ({}, 'author {}')",
                i + 1,
                i
            ))
            .expect("insert author");
        }
        for i in 0_i64..200_i64 {
            db.execute(&format!(
                "INSERT INTO books VALUES ({}, {}, 'book {}')",
                i + 1,
                (i % 50) + 1,
                i
            ))
            .expect("insert book");
        }
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");

    let result = db
        .execute(
            "SELECT a.name, b.title FROM authors a INNER JOIN books b \
                 ON a.id = b.author_id ORDER BY a.name, b.title",
        )
        .expect("inner join after reopen");
    assert_eq!(result.rows().len(), 200);

    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected inner join to re-defer tables after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected both tables deferred after inner join, got: {json_after}"
    );
}

/// D-E2: Reopen-time LEFT JOIN on paged tables stays deferred.
#[test]
fn paged_row_storage_left_join_after_reopen_stays_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-left-join-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE deps (id INTEGER PRIMARY KEY, name TEXT)")
            .expect("create deps");
        db.execute("CREATE TABLE packages (id INTEGER PRIMARY KEY, dep_id INTEGER, version TEXT)")
            .expect("create packages");
        for i in 0_i64..30_i64 {
            db.execute(&format!("INSERT INTO deps VALUES ({}, 'dep {}')", i + 1, i))
                .expect("insert dep");
        }
        for i in 0_i64..100_i64 {
            db.execute(&format!(
                "INSERT INTO packages VALUES ({}, {}, '1.{}')",
                i + 1,
                (i % 30) + 1,
                i
            ))
            .expect("insert package");
        }
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");

    let result = db
        .execute(
            "SELECT d.name, p.version FROM deps d LEFT JOIN packages p \
                 ON d.id = p.dep_id ORDER BY d.name, p.version",
        )
        .expect("left join after reopen");
    assert!(!result.rows().is_empty());

    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected left join to re-defer tables after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected both tables deferred after left join, got: {json_after}"
    );
}

/// D-E2: Reopen-time JOIN USING on paged tables stays deferred.
#[test]
fn paged_row_storage_join_using_after_reopen_stays_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-join-using-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .expect("create users");
        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)")
            .expect("create orders");
        for i in 0_i64..20_i64 {
            db.execute(&format!(
                "INSERT INTO users VALUES ({}, 'user {}')",
                i + 1,
                i
            ))
            .expect("insert user");
        }
        for i in 0_i64..80_i64 {
            db.execute(&format!(
                "INSERT INTO orders VALUES ({}, {}, {})",
                i + 1,
                (i % 20) + 1,
                (i % 100) + 10
            ))
            .expect("insert order");
        }
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");

    let result = db
        .execute(
            "SELECT u.name, o.amount FROM users u INNER JOIN orders o \
                 ON u.id = o.user_id WHERE o.amount > 50 ORDER BY o.amount",
        )
        .expect("join with filter after reopen");
    assert!(!result.rows().is_empty());

    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected join with filter to re-defer tables after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected both tables deferred after join with filter, got: {json_after}"
    );
}

/// D-E2: Reopen-time FULL JOIN on paged tables stays deferred.
#[test]
fn paged_row_storage_full_join_after_reopen_stays_deferred() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir
        .path()
        .join("paged-row-storage-full-join-reopen.ddb");
    let config = DbConfig {
        paged_row_storage: true,
        ..DbConfig::default()
    };

    {
        let db = Db::open_or_create(&path, config.clone()).expect("open db");
        db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create t1");
        db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create t2");
        for i in 0_i64..10_i64 {
            db.execute(&format!("INSERT INTO t1 VALUES ({}, 't1-{}')", i + 1, i))
                .expect("insert t1");
        }
        for i in 5_i64..15_i64 {
            db.execute(&format!("INSERT INTO t2 VALUES ({}, 't2-{}')", i + 1, i))
                .expect("insert t2");
        }
        db.checkpoint().expect("checkpoint");
    }

    let db = Db::open_or_create(&path, config).expect("reopen db");

    let result = db
        .execute(
            "SELECT t1.val, t2.val FROM t1 FULL JOIN t2 ON t1.id = t2.id \
                 ORDER BY t1.val, t2.val",
        )
        .expect("full join after reopen");
    assert!(!result.rows().is_empty());

    let json_after = db.inspect_storage_state_json().expect("json after query");
    assert!(
        json_after.contains("\"loaded_table_count\":0"),
        "expected full join to re-defer tables after commit, got: {json_after}"
    );
    assert!(
        json_after.contains("\"deferred_table_count\":2"),
        "expected both tables deferred after full join, got: {json_after}"
    );
}

#[test]
#[cfg(feature = "bench-internals")]
fn read_page_without_writer_skips_write_txn_lock() {
    use crate::benchmark;

    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("skip_write_txn_lock.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT NOT NULL)")
        .expect("create table");
    {
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO t (id, val) VALUES ($1, 'a')")
            .expect("prepare");
        for i in 1..=10 {
            insert
                .execute_in(&mut txn, &[Value::Int64(i)])
                .expect("insert");
        }
        txn.commit().expect("commit");
    }
    db.checkpoint().expect("checkpoint");

    benchmark::reset_read_path_counters();
    for _ in 0..20 {
        let page = db
            .read_page(crate::storage::page::CATALOG_ROOT_PAGE_ID)
            .expect("read catalog root page");
        assert_eq!(page.len(), DbConfig::default().page_size as usize);
    }
    let counters = benchmark::take_read_path_counters();
    assert_eq!(
        counters.write_txn_lock_count, 0,
        "read path should skip write_txn lock when no writer is active"
    );
}

#[test]
#[cfg(feature = "bench-internals")]
fn read_page_for_snapshot_counts_held_snapshot_lock() {
    use crate::benchmark;

    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("held_snapshot_counter.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)")
        .expect("create table");
    db.execute("INSERT INTO t VALUES (1)").expect("insert");
    db.checkpoint().expect("checkpoint");

    let token = db.hold_snapshot().expect("hold snapshot");
    benchmark::reset_read_path_counters();
    let page = db
        .read_page_for_snapshot(token, crate::storage::page::CATALOG_ROOT_PAGE_ID)
        .expect("read page for snapshot");
    assert_eq!(page.len(), DbConfig::default().page_size as usize);
    db.release_snapshot(token).expect("release snapshot");

    let counters = benchmark::take_read_path_counters();
    assert_eq!(counters.held_snapshots_lock_count, 1);
    assert_eq!(
        counters.write_txn_lock_count, 0,
        "held-snapshot read should still skip write_txn lock when no writer is active"
    );
}

#[test]
fn process_coordination_refreshes_independent_wal_handle() -> Result<()> {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("process_refresh.ddb");
    let config = DbConfig {
        background_checkpoint_worker: false,
        wal_checkpoint_threshold_pages: 0,
        wal_checkpoint_threshold_bytes: 0,
        ..DbConfig::default()
    };

    let db1 = Db::open_or_create(&path, config.clone())?;
    db1.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")?;
    super::evict_shared_wal(&path)?;
    let db2 = Db::open_or_create(&path, config)?;

    db1.execute("INSERT INTO t VALUES (1, 'alpha')")?;
    let count = scalar_i64(&db2.execute("SELECT COUNT(*) FROM t")?);
    assert_eq!(count, 1);

    let coordination = db2.execute("SELECT * FROM sys.process_coordination")?;
    assert_eq!(coordination.rows()[0].values()[1], Value::Bool(true));
    Ok(())
}

#[test]
fn open_repairs_current_header_with_empty_coordination_identity() -> Result<()> {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("repair-empty-identity.ddb");

    let db = Db::open_or_create(&path, DbConfig::default())?;
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")?;
    db.execute("INSERT INTO t VALUES (1, 'alpha')")?;
    db.checkpoint()?;
    drop(db);

    let original_header = read_header_from_path(&path);
    assert!(!original_header.has_empty_database_id());

    let mut damaged_header = original_header.clone();
    damaged_header.database_id = [0_u8; 16];
    let mut bytes = std::fs::read(&path).expect("read database bytes");
    bytes[..DB_HEADER_SIZE].copy_from_slice(&damaged_header.encode());
    std::fs::write(&path, bytes).expect("write damaged header");
    assert!(read_header_from_path(&path).has_empty_database_id());

    let reopened = Db::open_or_create(&path, DbConfig::default())?;
    let result = reopened.execute("SELECT value FROM t WHERE id = 1")?;
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("alpha".to_string())]
    );
    drop(reopened);

    assert!(!read_header_from_path(&path).has_empty_database_id());
    Ok(())
}

#[test]
fn process_reader_slot_blocks_external_checkpoint_truncation() -> Result<()> {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("process_reader_retention.ddb");
    let config = DbConfig {
        background_checkpoint_worker: false,
        wal_checkpoint_threshold_pages: 0,
        wal_checkpoint_threshold_bytes: 0,
        ..DbConfig::default()
    };

    let db1 = Db::open_or_create(&path, config.clone())?;
    db1.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")?;
    super::evict_shared_wal(&path)?;
    let db2 = Db::open_or_create(&path, config)?;
    let token = db2.hold_snapshot()?;

    db1.execute("INSERT INTO t VALUES (1, 'held')")?;
    db1.checkpoint_wal()?;
    let wal_path = {
        let mut path = path.as_os_str().to_os_string();
        path.push(".wal");
        std::path::PathBuf::from(path)
    };
    let held_len = std::fs::metadata(&wal_path)
        .expect("stat wal with process reader")
        .len();
    assert!(
        held_len > crate::wal::format::WAL_HEADER_SIZE,
        "process reader should prevent WAL truncation, got {held_len}"
    );

    db2.release_snapshot(token)?;
    db1.checkpoint_wal()?;
    let released_len = std::fs::metadata(&wal_path)
        .expect("stat wal after release")
        .len();
    assert_eq!(released_len, crate::wal::format::WAL_HEADER_SIZE);
    Ok(())
}

#[test]
fn read_page_with_active_writer_still_sees_staged_page() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("staged_visible.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT NOT NULL)")
        .expect("create table");
    {
        let mut txn = db.transaction().expect("begin txn");
        let insert = txn
            .prepare("INSERT INTO t (id, val) VALUES ($1, 'a')")
            .expect("prepare");
        for i in 1..=5 {
            insert
                .execute_in(&mut txn, &[Value::Int64(i)])
                .expect("insert");
        }
        txn.commit().expect("commit");
    }
    db.checkpoint().expect("checkpoint");

    let mut txn = db.transaction().expect("begin txn");
    let update = txn
        .prepare("UPDATE t SET val = 'staged' WHERE id = 1")
        .expect("prepare");
    update.execute_in(&mut txn, &[]).expect("update");

    // The staged page should still be visible via the read path before commit.
    let select = txn
        .prepare("SELECT val FROM t WHERE id = 1")
        .expect("prepare select");
    let result = select.execute_in(&mut txn, &[]).expect("select");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("staged".to_string())]
    );
    txn.commit().expect("commit");
}

#[test]
fn repeated_open_create_table_insert_keeps_header_valid() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("reopen_schema_header.ddb");

    for table_id in 0..5 {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        db.execute(&format!(
            "CREATE TABLE table_{table_id} (id INTEGER PRIMARY KEY, val TEXT)"
        ))
        .expect("create table");
        for row_id in 0..3 {
            db.execute(&format!(
                "INSERT INTO table_{table_id} VALUES ({row_id}, 'val_{row_id}')"
            ))
            .expect("insert row");
        }
    }

    let db = Db::open_or_create(&path, DbConfig::default()).expect("reopen db");
    for table_id in 0..5 {
        let result = db
            .execute(&format!("SELECT COUNT(*) FROM table_{table_id}"))
            .expect("count rows");
        assert_eq!(result.rows()[0].values(), &[Value::Int64(3)]);
    }
}

// -------- Plan cache tests (Phase 1A) ----------

#[test]
fn plan_cache_caches_repeated_prepares() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("plan_cache_basic.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        .expect("create table");

    let prep1 = db.prepare("SELECT * FROM t WHERE id = 1").expect("prepare");
    let prep2 = db.prepare("SELECT * FROM t WHERE id = 1").expect("prepare");
    let summary = db.plan_cache_summary().expect("summary");
    assert!(summary.total_hits >= 1);
    assert!(summary.total_misses >= 1);
    let _ = prep1;
    let _ = prep2;
}

#[test]
fn plan_cache_ddl_invalidates_eagerly() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("plan_cache_ddl.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create table");

    let _ = db.prepare("SELECT * FROM t").expect("prepare");
    let before = db.plan_cache_summary().expect("summary");
    assert!(before.total_entries >= 1);

    db.execute("ALTER TABLE t ADD COLUMN val TEXT")
        .expect("alter table");
    let after = db.plan_cache_summary().expect("summary");
    assert_eq!(after.total_entries, 0);
}

#[test]
fn plan_cache_audit_context_does_not_evict() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("plan_cache_audit.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create table");

    let _ = db.prepare("SELECT * FROM t").expect("prepare");
    let before = db.plan_cache_summary().expect("summary");
    let hits_before = before.total_hits;
    let entries_before = before.total_entries;
    assert!(entries_before >= 1);

    db.execute("SET AUDIT CONTEXT actor = 'tester'")
        .expect("audit context");
    let after = db.plan_cache_summary().expect("summary");
    assert_eq!(
        after.total_entries, entries_before,
        "audit context must not evict the plan cache"
    );

    let _ = db.prepare("SELECT * FROM t").expect("prepare");
    let after2 = db.plan_cache_summary().expect("summary");
    assert!(
        after2.total_hits > hits_before,
        "expected at least one new hit on re-prepare after audit context write"
    );
}

#[test]
fn plan_cache_pragma_flush_resets() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("plan_cache_flush.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create table");
    let _ = db.prepare("SELECT * FROM t").expect("prepare");
    assert!(db.plan_cache_summary().expect("summary").total_entries >= 1);
    db.execute("PRAGMA flush_plan_cache").expect("pragma");
    assert_eq!(db.plan_cache_summary().expect("summary").total_entries, 0);
}

#[test]
fn plan_cache_pragma_flush_local_resets() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("plan_cache_flush_local.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create table");
    let _ = db
        .prepare("SELECT * FROM t WHERE id = $1")
        .expect("prepare");
    assert!(db.plan_cache_summary().expect("summary").total_entries >= 1);
    db.execute("PRAGMA flush_plan_cache = local")
        .expect("pragma");
    let summary = db.plan_cache_summary().expect("summary");
    assert_eq!(summary.total_entries, 0);
    assert_eq!(summary.total_hits, 0);
    assert_eq!(summary.total_misses, 0);
}

#[test]
fn plan_cache_summary_api_works() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("plan_cache_view.ddb");
    let db = Db::create(&path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create table");
    let _ = db.prepare("SELECT * FROM t").expect("prepare");
    let result = db
        .execute("SELECT * FROM sys.plan_cache_summary")
        .expect("summary view");
    assert_eq!(result.rows().len(), 1);
    let result = db
        .execute("SELECT * FROM sys.plan_cache")
        .expect("entries view");
    let _ = result;
}

#[test]
fn plan_cache_doctor_reports_disabled_cache() {
    let temp = TempDir::new().expect("tempdir");
    let path = temp.path().join("plan_cache_doctor.ddb");
    let mut config = DbConfig::default();
    config.with_plan_cache(|cfg| cfg.enabled = false);
    let db = Db::create(&path, config).expect("create db");
    let result = db
        .execute("SELECT * FROM sys.doctor_findings")
        .expect("doctor findings");
    assert!(
        result.rows().iter().any(|row| matches!(
            row.values().first(),
            Some(Value::Text(id)) if id == "plan-cache.disabled"
        )),
        "disabled plan cache should be visible through sys.doctor_findings"
    );
}
