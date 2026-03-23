use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use decentdb::{BulkLoadOptions, Db, DbConfig, Value};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn catalog_roundtrip_schema_cookie_and_cross_connection_visibility_work() {
    let path = unique_db_path("phase3-catalog");
    let writer = Db::create(&path, DbConfig::default()).expect("create database");
    let reader = Db::open(&path, DbConfig::default()).expect("open second handle");

    writer
        .execute("CREATE TABLE users (id INT64 PRIMARY KEY, email TEXT NOT NULL)")
        .expect("create table");
    writer
        .execute("CREATE INDEX users_email_idx ON users (email)")
        .expect("create index");
    writer
        .execute("CREATE VIEW user_emails AS SELECT email FROM users")
        .expect("create view");
    assert_eq!(writer.schema_cookie().expect("schema cookie"), 3);

    writer
        .execute("INSERT INTO users (id, email) VALUES (1, 'a@example.com')")
        .expect("insert row");

    let result = reader
        .execute("SELECT email FROM user_emails")
        .expect("reader refreshes from WAL-backed runtime");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("a@example.com".to_string())]
    );

    let reopened = Db::open(&path, DbConfig::default()).expect("reopen database");
    let result = reopened
        .execute("SELECT email FROM user_emails")
        .expect("reopened view query");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("a@example.com".to_string())]
    );

    cleanup_db(&path);
}

#[test]
fn read_executor_supports_joins_aggregates_row_number_and_explain() {
    let path = unique_db_path("phase3-read");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create users");
    db.execute(
        "CREATE TABLE orders (id INT64 PRIMARY KEY, user_id INT64 REFERENCES users(id), total INT64)",
    )
    .expect("create orders");
    db.execute("CREATE INDEX orders_user_id_idx ON orders (user_id)")
        .expect("create orders index");
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT)")
        .expect("create docs");
    db.execute("CREATE INDEX docs_body_trgm_idx ON docs USING gin (body)")
        .expect("create trigram index");

    db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Grace'), (3, 'Linus')")
        .expect("insert users");
    db.execute(
        "INSERT INTO orders (id, user_id, total) VALUES (10, 1, 50), (11, 1, 75), (12, 2, 10)",
    )
    .expect("insert orders");
    db.execute(
        "INSERT INTO docs (id, body) VALUES (1, 'alphabet soup'), (2, 'beta world'), (3, 'alpha numeric')",
    )
    .expect("insert docs");

    let aggregate = db
        .execute(
            "SELECT u.name, COUNT(o.id) AS order_count, SUM(o.total) AS total_sum \
             FROM users AS u \
             LEFT JOIN orders AS o ON u.id = o.user_id \
             GROUP BY u.name \
             HAVING COUNT(o.id) >= 1 \
             ORDER BY total_sum DESC \
             LIMIT 1",
        )
        .expect("aggregate query");
    assert_eq!(aggregate.columns(), &["name", "order_count", "total_sum"]);
    assert_eq!(
        aggregate.rows()[0].values(),
        &[
            Value::Text("Ada".to_string()),
            Value::Int64(2),
            Value::Int64(125),
        ]
    );

    let row_numbers = db
        .execute("SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn FROM users ORDER BY name")
        .expect("row_number query");
    assert_eq!(row_numbers.rows().len(), 3);
    assert_eq!(
        row_numbers
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("Ada".to_string()), Value::Int64(1)],
            vec![Value::Text("Grace".to_string()), Value::Int64(2)],
            vec![Value::Text("Linus".to_string()), Value::Int64(3)],
        ]
    );

    let explain_seek = db
        .execute("EXPLAIN SELECT * FROM orders WHERE user_id = 1")
        .expect("explain btree");
    assert!(
        explain_seek
            .explain_lines()
            .iter()
            .any(|line| line.contains("IndexSeek(table=orders")),
        "expected IndexSeek in {:?}",
        explain_seek.explain_lines()
    );

    let explain_trigram = db
        .execute("EXPLAIN SELECT * FROM docs WHERE body LIKE '%alpha%'")
        .expect("explain trigram");
    assert!(
        explain_trigram
            .explain_lines()
            .iter()
            .any(|line| line.contains("TrigramSearch(table=docs, index=docs_body_trgm_idx")),
        "expected TrigramSearch in {:?}",
        explain_trigram.explain_lines()
    );

    cleanup_db(&path);
}

#[test]
fn read_executor_supports_parameterized_alias_joins_on_either_side() {
    let path = unique_db_path("phase3-join-fastpath");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create users");
    db.execute(
        "CREATE TABLE orders (id INT64 PRIMARY KEY, user_id INT64 REFERENCES users(id), total INT64)",
    )
    .expect("create orders");
    db.execute("CREATE INDEX orders_user_id_idx ON orders (user_id)")
        .expect("create orders index");

    db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Grace')")
        .expect("insert users");
    db.execute(
        "INSERT INTO orders (id, user_id, total) VALUES (10, 1, 50), (11, 1, 75), (12, 2, 10)",
    )
    .expect("insert orders");

    let left_filtered = db
        .execute_with_params(
            "SELECT u.name, o.total \
             FROM users AS u \
             JOIN orders AS o ON u.id = o.user_id \
             WHERE u.id = $1 \
             ORDER BY o.total",
            &[Value::Int64(1)],
        )
        .expect("left-filtered join");
    assert_eq!(
        left_filtered
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("Ada".to_string()), Value::Int64(50)],
            vec![Value::Text("Ada".to_string()), Value::Int64(75)],
        ]
    );

    let right_filtered = db
        .execute_with_params(
            "SELECT u.name, o.total \
             FROM users AS u \
             JOIN orders AS o ON u.id = o.user_id \
             WHERE o.id = $1",
            &[Value::Int64(12)],
        )
        .expect("right-filtered join");
    assert_eq!(
        right_filtered
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![vec![Value::Text("Grace".to_string()), Value::Int64(10)]]
    );

    cleanup_db(&path);
}

#[test]
fn statement_rollback_constraints_and_on_conflict_returning_work() {
    let path = unique_db_path("phase3-dml");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, email TEXT UNIQUE NOT NULL, name TEXT)")
        .expect("create users");
    db.execute(
        "INSERT INTO users (id, email, name) VALUES \
         (1, 'ada@example.com', 'Ada'), \
         (2, 'grace@example.com', 'Grace')",
    )
    .expect("insert seed users");

    let error = db
        .execute(
            "INSERT INTO users (id, email, name) VALUES \
             (3, 'linus@example.com', 'Linus'), \
             (4, 'ada@example.com', 'Duplicate')",
        )
        .expect_err("second row should violate UNIQUE");
    assert!(matches!(error, decentdb::DbError::Constraint { .. }));

    let count = db
        .execute("SELECT COUNT(*) AS count FROM users")
        .expect("count users after failed statement");
    assert_eq!(count.rows()[0].values(), &[Value::Int64(2)]);

    let do_nothing = db
        .execute(
            "INSERT INTO users (id, email, name) VALUES (5, 'grace@example.com', 'Grace 2') \
             ON CONFLICT (email) DO NOTHING \
             RETURNING id",
        )
        .expect("do nothing");
    assert!(do_nothing.rows().is_empty());

    let upsert = db
        .execute(
            "INSERT INTO users (id, email, name) VALUES (5, 'grace@example.com', 'Grace 2') \
             ON CONFLICT (email) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name \
             RETURNING id, name",
        )
        .expect("upsert update");
    assert_eq!(
        upsert.rows()[0].values(),
        &[Value::Int64(5), Value::Text("Grace 2".to_string())]
    );

    let param_query = db
        .execute_with_params(
            "SELECT id, name FROM users WHERE email = $1",
            &[Value::Text("grace@example.com".to_string())],
        )
        .expect("parameterized select");
    assert_eq!(
        param_query.rows()[0].values(),
        &[Value::Int64(5), Value::Text("Grace 2".to_string())]
    );

    cleanup_db(&path);
}

#[test]
fn sql_savepoints_and_begin_variants_work() {
    let path = unique_db_path("phase3-savepoints");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE entries (id INT64 PRIMARY KEY, note TEXT)")
        .expect("create entries");
    db.execute("BEGIN IMMEDIATE")
        .expect("begin immediate is accepted");
    db.execute("INSERT INTO entries (id, note) VALUES (1, 'outer')")
        .expect("insert outer row");
    db.execute("SAVEPOINT alpha")
        .expect("create outer savepoint");
    db.execute("INSERT INTO entries (id, note) VALUES (2, 'alpha child')")
        .expect("insert alpha child");
    db.execute("SAVEPOINT beta")
        .expect("create nested savepoint");
    db.execute("INSERT INTO entries (id, note) VALUES (3, 'beta child')")
        .expect("insert beta child");
    db.execute("ROLLBACK TO SAVEPOINT beta")
        .expect("rollback to nested savepoint");
    db.execute("INSERT INTO entries (id, note) VALUES (4, 'after beta rollback')")
        .expect("insert after beta rollback");
    db.execute("ROLLBACK TO SAVEPOINT alpha")
        .expect("rollback to outer savepoint");
    db.execute("INSERT INTO entries (id, note) VALUES (5, 'after alpha rollback')")
        .expect("insert after outer rollback");
    db.execute("ROLLBACK TO SAVEPOINT alpha")
        .expect("rollback keeps target savepoint active");
    db.execute("INSERT INTO entries (id, note) VALUES (6, 'committed child')")
        .expect("insert final row");
    db.execute("RELEASE SAVEPOINT alpha")
        .expect("release outer savepoint");
    db.execute("COMMIT").expect("commit transaction");

    let rows = db
        .execute("SELECT id, note FROM entries ORDER BY id")
        .expect("read committed rows");
    assert_eq!(
        rows.rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Text("outer".to_string())],
            vec![Value::Int64(6), Value::Text("committed child".to_string())],
        ]
    );

    cleanup_db(&path);
}

#[test]
fn savepoints_require_explicit_transactions_and_known_names() {
    let path = unique_db_path("phase3-savepoint-errors");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE entries (id INT64 PRIMARY KEY, note TEXT)")
        .expect("create entries");

    let outside_txn = db
        .execute("SAVEPOINT orphan")
        .expect_err("savepoints require BEGIN");
    assert!(matches!(outside_txn, decentdb::DbError::Transaction { .. }));

    db.execute("BEGIN EXCLUSIVE")
        .expect("begin exclusive is accepted");
    let missing = db
        .execute("ROLLBACK TO SAVEPOINT missing")
        .expect_err("missing savepoint should fail");
    assert!(matches!(missing, decentdb::DbError::Transaction { .. }));
    db.execute("ROLLBACK").expect("rollback transaction");

    cleanup_db(&path);
}

#[test]
fn foreign_keys_views_and_triggers_work_for_supported_subset() {
    let path = unique_db_path("phase3-ddl");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE parents (id INT64 PRIMARY KEY, name TEXT)")
        .expect("create parents");
    db.execute(
        "CREATE TABLE children (id INT64 PRIMARY KEY, parent_id INT64 REFERENCES parents(id) ON DELETE CASCADE, name TEXT)",
    )
    .expect("create children");
    db.execute("INSERT INTO parents (id, name) VALUES (1, 'p1')")
        .expect("insert parent");
    db.execute("INSERT INTO children (id, parent_id, name) VALUES (10, 1, 'c1'), (11, 1, 'c2')")
        .expect("insert children");
    db.execute("DELETE FROM parents WHERE id = 1")
        .expect("delete cascades");
    let children = db
        .execute("SELECT COUNT(*) FROM children")
        .expect("count children");
    assert_eq!(children.rows()[0].values(), &[Value::Int64(0)]);

    db.execute("CREATE TABLE audit_log (event TEXT)")
        .expect("create audit log");
    db.execute("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)")
        .expect("create items");
    db.execute(
        "CREATE TRIGGER items_after_insert AFTER INSERT ON items FOR EACH ROW \
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log(event) VALUES (''after insert'')')",
    )
    .expect("create after trigger");
    db.execute("INSERT INTO items (id, name) VALUES (1, 'a'), (2, 'b')")
        .expect("insert items");
    let audit = db
        .execute("SELECT COUNT(*) FROM audit_log")
        .expect("count audit rows");
    assert_eq!(audit.rows()[0].values(), &[Value::Int64(2)]);

    db.execute("CREATE TABLE inbox (id INT64 PRIMARY KEY, body TEXT)")
        .expect("create inbox");
    db.execute("CREATE VIEW inbox_view AS SELECT body FROM inbox")
        .expect("create view");
    db.execute(
        "CREATE TRIGGER inbox_insert INSTEAD OF INSERT ON inbox_view FOR EACH ROW \
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO inbox(body) VALUES (''from_view'')')",
    )
    .expect("create instead-of trigger");
    db.execute("INSERT INTO inbox_view (body) VALUES ('ignored')")
        .expect("insert through view");
    let inbox = db
        .execute("SELECT body FROM inbox")
        .expect("select inbox rows");
    assert_eq!(
        inbox.rows()[0].values(),
        &[Value::Text("from_view".to_string())]
    );

    cleanup_db(&path);
}

#[test]
fn alter_table_bulk_load_and_rebuild_entry_points_work() {
    let path = unique_db_path("phase3-maintenance");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE imports (id INT64 PRIMARY KEY, amount TEXT)")
        .expect("create imports");
    db.bulk_load_rows(
        "imports",
        &["id", "amount"],
        &[
            vec![Value::Int64(1), Value::Text("10".to_string())],
            vec![Value::Int64(2), Value::Text("20".to_string())],
        ],
        BulkLoadOptions {
            batch_size: 2,
            sync_interval: 1,
            disable_indexes: true,
            checkpoint_on_complete: false,
        },
    )
    .expect("bulk load rows");

    db.execute("ALTER TABLE imports ADD COLUMN active BOOL DEFAULT TRUE")
        .expect("add column");
    db.execute("ALTER TABLE imports RENAME COLUMN amount TO total")
        .expect("rename column");
    db.execute("ALTER TABLE imports ALTER COLUMN total TYPE INT64")
        .expect("alter type");
    db.execute("CREATE INDEX imports_total_idx ON imports (total)")
        .expect("create total index");

    let rows = db
        .execute("SELECT total, active FROM imports ORDER BY id")
        .expect("read altered table");
    assert_eq!(
        rows.rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(10), Value::Bool(true)],
            vec![Value::Int64(20), Value::Bool(true)],
        ]
    );

    db.rebuild_index("imports_total_idx")
        .expect("rebuild single index");
    db.rebuild_indexes().expect("rebuild all indexes");

    let explain = db
        .execute("EXPLAIN SELECT * FROM imports WHERE total = 10")
        .expect("explain after rebuild");
    assert!(
        explain
            .explain_lines()
            .iter()
            .any(|line| line.contains("IndexSeek(table=imports")),
        "expected IndexSeek in {:?}",
        explain.explain_lines()
    );

    db.execute("DROP INDEX imports_total_idx")
        .expect("drop non-unique index");
    db.execute("DROP TABLE imports").expect("drop table");

    cleanup_db(&path);
}

#[test]
fn explicit_transaction_param_inserts_keep_indexes_usable_and_statement_atomic() {
    let path = unique_db_path("phase0-insert-hot-path");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, email TEXT NOT NULL)")
        .expect("create users");
    db.execute("CREATE INDEX users_email_idx ON users (email)")
        .expect("create email index");

    db.execute("BEGIN").expect("begin");
    for id in 1..=8 {
        db.execute_with_params(
            "INSERT INTO users (id, email) VALUES ($1, $2)",
            &[
                Value::Int64(id),
                Value::Text(format!("user{id}@example.com")),
            ],
        )
        .expect("insert user");
    }

    let duplicate = db
        .execute_with_params(
            "INSERT INTO users (id, email) VALUES ($1, $2)",
            &[Value::Int64(3), Value::Text("duplicate@example.com".to_string())],
        )
        .expect_err("duplicate primary key should fail");
    assert!(matches!(duplicate, decentdb::DbError::Constraint { .. }));

    let count = db
        .execute("SELECT COUNT(*) FROM users")
        .expect("count after failed insert");
    assert_eq!(count.rows()[0].values(), &[Value::Int64(8)]);

    let explain = db
        .execute("EXPLAIN SELECT id FROM users WHERE email = 'user3@example.com'")
        .expect("explain email lookup");
    assert!(
        explain
            .explain_lines()
            .iter()
            .any(|line| line.contains("IndexSeek(table=users")),
        "expected IndexSeek in {:?}",
        explain.explain_lines()
    );

    let row = db
        .execute_with_params(
            "SELECT id FROM users WHERE email = $1",
            &[Value::Text("user3@example.com".to_string())],
        )
        .expect("select by indexed email");
    assert_eq!(row.rows()[0].values(), &[Value::Int64(3)]);

    db.execute("COMMIT").expect("commit");

    let verification = db
        .verify_index("users_email_idx")
        .expect("verify email index");
    assert!(verification.valid, "expected valid index after commit");

    cleanup_db(&path);
}

#[test]
fn autocommit_param_inserts_keep_indexes_usable_and_statement_atomic() {
    let path = unique_db_path("phase0-autocommit-insert-hot-path");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, email TEXT NOT NULL)")
        .expect("create users");
    db.execute("CREATE INDEX users_email_idx ON users (email)")
        .expect("create email index");

    for id in 1..=8 {
        db.execute_with_params(
            "INSERT INTO users (id, email) VALUES ($1, $2)",
            &[
                Value::Int64(id),
                Value::Text(format!("user{id}@example.com")),
            ],
        )
        .expect("insert user");
    }

    let duplicate = db
        .execute_with_params(
            "INSERT INTO users (id, email) VALUES ($1, $2)",
            &[Value::Int64(3), Value::Text("duplicate@example.com".to_string())],
        )
        .expect_err("duplicate primary key should fail");
    assert!(matches!(duplicate, decentdb::DbError::Constraint { .. }));

    let count = db
        .execute("SELECT COUNT(*) FROM users")
        .expect("count after failed insert");
    assert_eq!(count.rows()[0].values(), &[Value::Int64(8)]);

    let explain = db
        .execute("EXPLAIN SELECT id FROM users WHERE email = 'user3@example.com'")
        .expect("explain email lookup");
    assert!(
        explain
            .explain_lines()
            .iter()
            .any(|line| line.contains("IndexSeek(table=users")),
        "expected IndexSeek in {:?}",
        explain.explain_lines()
    );

    let row = db
        .execute_with_params(
            "SELECT id FROM users WHERE email = $1",
            &[Value::Text("user3@example.com".to_string())],
        )
        .expect("select by indexed email");
    assert_eq!(row.rows()[0].values(), &[Value::Int64(3)]);

    let verification = db
        .verify_index("users_email_idx")
        .expect("verify email index");
    assert!(verification.valid, "expected valid index after inserts");

    cleanup_db(&path);
}

#[test]
fn prepared_param_insert_recompiles_after_schema_change() {
    let path = unique_db_path("phase1-prepared-insert-schema-cookie");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, email TEXT NOT NULL)")
        .expect("create users");
    db.execute_with_params(
        "INSERT INTO users (id, email) VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text("before@example.com".to_string())],
    )
    .expect("insert before schema change");

    db.execute("ALTER TABLE users ADD COLUMN active BOOL DEFAULT TRUE")
        .expect("add active column");
    db.execute_with_params(
        "INSERT INTO users (id, email) VALUES ($1, $2)",
        &[Value::Int64(2), Value::Text("after@example.com".to_string())],
    )
    .expect("insert after schema change");

    let rows = db
        .execute("SELECT id, email, active FROM users ORDER BY id")
        .expect("read rows after schema change");
    assert_eq!(
        rows.rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Int64(1),
                Value::Text("before@example.com".to_string()),
                Value::Bool(true),
            ],
            vec![
                Value::Int64(2),
                Value::Text("after@example.com".to_string()),
                Value::Bool(true),
            ],
        ]
    );

    cleanup_db(&path);
}

#[test]
fn prepared_statements_reuse_parameterized_inserts_and_reads() {
    let path = unique_db_path("phase1-prepared-statement");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, email TEXT NOT NULL)")
        .expect("create users");

    let insert = db
        .prepare("INSERT INTO users (id, email) VALUES ($1, $2)")
        .expect("prepare insert");
    db.execute("BEGIN").expect("begin");
    for id in 1..=4 {
        insert
            .execute(&[
                Value::Int64(id),
                Value::Text(format!("user{id}@example.com")),
            ])
            .expect("prepared insert");
    }
    let duplicate = insert
        .execute(&[
            Value::Int64(3),
            Value::Text("duplicate@example.com".to_string()),
        ])
        .expect_err("duplicate primary key should fail");
    assert!(matches!(duplicate, decentdb::DbError::Constraint { .. }));
    db.execute("COMMIT").expect("commit");

    let select = db
        .prepare("SELECT email FROM users WHERE id = $1")
        .expect("prepare select");
    let row = select
        .execute(&[Value::Int64(3)])
        .expect("prepared select by id");
    assert_eq!(
        row.rows()[0].values(),
        &[Value::Text("user3@example.com".to_string())]
    );

    cleanup_db(&path);
}

#[test]
fn prepared_statements_fail_after_schema_change() {
    let path = unique_db_path("phase1-prepared-statement-schema-change");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, email TEXT NOT NULL)")
        .expect("create users");

    let insert = db
        .prepare("INSERT INTO users (id, email) VALUES ($1, $2)")
        .expect("prepare insert");
    insert
        .execute(&[
            Value::Int64(1),
            Value::Text("before@example.com".to_string()),
        ])
        .expect("insert before schema change");

    db.execute("ALTER TABLE users ADD COLUMN active BOOL DEFAULT TRUE")
        .expect("alter table");

    let error = insert
        .execute(&[
            Value::Int64(2),
            Value::Text("after@example.com".to_string()),
        ])
        .expect_err("prepared statement should be invalidated");
    assert!(matches!(
        error,
        decentdb::DbError::Sql { message } if message.contains("schema changed")
    ));

    cleanup_db(&path);
}

#[test]
fn unsupported_insert_expression_falls_back_from_prepared_fast_path() {
    let path = unique_db_path("phase1-prepared-insert-fallback");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, email TEXT NOT NULL)")
        .expect("create users");
    db.execute_with_params(
        "INSERT INTO users (id, email) VALUES ($1 + 0, $2)",
        &[Value::Int64(7), Value::Text("fallback@example.com".to_string())],
    )
    .expect("expression insert should use generic path");

    let row = db
        .execute("SELECT id, email FROM users")
        .expect("select inserted row");
    assert_eq!(
        row.rows()[0].values(),
        &[Value::Int64(7), Value::Text("fallback@example.com".to_string())]
    );

    cleanup_db(&path);
}

fn unique_db_path(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic enough for tests")
        .as_nanos();
    let ordinal = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir().join(format!(
        "decentdb-phase3-{label}-{}-{timestamp}-{ordinal}.ddb",
        std::process::id()
    ))
}

fn wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_os_string();
    wal.push(".wal");
    PathBuf::from(wal)
}

fn cleanup_db(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(wal_path(path));
}
