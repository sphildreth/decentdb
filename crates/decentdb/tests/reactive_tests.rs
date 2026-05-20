use std::time::Duration;

use decentdb::{
    ChangeStreamOptions, Db, DbConfig, QueryWatchOptions, RangeWatchOptions, TableWatchOptions,
    Value, WatchEvent,
};

fn memory_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn recv_event(watch: &decentdb::WatchHandle) -> WatchEvent {
    watch
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .expect("watch event")
}

#[test]
fn table_watch_delivers_initial_and_committed_invalidation_with_rows() {
    let db = memory_db();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();

    let watch = db
        .watch_table(TableWatchOptions {
            tables: vec!["users".to_string()],
            queue_capacity: None,
        })
        .unwrap();
    assert!(matches!(recv_event(&watch), WatchEvent::Initial(_)));

    db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
        .unwrap();
    let event = recv_event(&watch);
    let WatchEvent::Invalidate(event) = event else {
        panic!("expected invalidation");
    };
    assert!(event.commit_lsn > 0);
    assert_eq!(event.tables, vec!["users"]);
    assert_eq!(event.row_changes.len(), 1);
    assert_eq!(event.row_changes[0].table, "users");
    assert_eq!(event.row_changes[0].after.as_ref().unwrap()["name"], "Ada");
}

#[test]
fn query_watch_delivers_initial_result_then_dependency_invalidation() {
    let db = memory_db();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
        .unwrap();

    let watch = db
        .watch_query(
            "SELECT name FROM users WHERE id = 1",
            &[],
            QueryWatchOptions::default(),
        )
        .unwrap();
    let WatchEvent::Initial(initial) = recv_event(&watch) else {
        panic!("expected initial");
    };
    let result = initial.result.expect("initial query result");
    assert_eq!(result.rows()[0].values()[0], Value::Text("Ada".to_string()));

    db.execute("UPDATE users SET name = 'Grace' WHERE id = 1")
        .unwrap();
    let WatchEvent::Invalidate(event) = recv_event(&watch) else {
        panic!("expected invalidation");
    };
    assert_eq!(event.tables, vec!["users"]);
}

#[test]
fn range_watch_filters_row_changes_and_conservatively_invalidates_table_events() {
    let db = memory_db();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    let watch = db
        .watch_range(RangeWatchOptions {
            table: "users".to_string(),
            lower: Some(serde_json::json!(10)),
            upper: Some(serde_json::json!(20)),
            lower_inclusive: true,
            upper_inclusive: true,
            queue_capacity: None,
        })
        .unwrap();
    assert!(matches!(recv_event(&watch), WatchEvent::Initial(_)));

    db.execute("INSERT INTO users (id, name) VALUES (5, 'outside')")
        .unwrap();
    assert!(watch
        .recv_timeout(Duration::from_millis(20))
        .unwrap()
        .is_none());

    db.execute("INSERT INTO users (id, name) VALUES (12, 'inside')")
        .unwrap();
    let WatchEvent::Invalidate(event) = recv_event(&watch) else {
        panic!("expected invalidation");
    };
    assert_eq!(event.row_changes.len(), 1);
    assert_eq!(
        event.row_changes[0].after.as_ref().unwrap()["name"],
        "inside"
    );
}

#[test]
fn change_stream_marks_queued_write_source() {
    let db = memory_db();
    db.execute("CREATE TABLE items (id INT64 PRIMARY KEY)")
        .unwrap();
    let stream = db.change_stream(ChangeStreamOptions::default()).unwrap();
    assert!(matches!(recv_event(&stream), WatchEvent::Initial(_)));

    db.execute_queued("INSERT INTO items (id) VALUES (1)")
        .unwrap();
    let WatchEvent::Change(event) = recv_event(&stream) else {
        panic!("expected change stream event");
    };
    assert_eq!(event.source.as_str(), "queued");
    assert_eq!(event.table_changes[0].table, "items");
}

#[test]
fn file_backed_watch_sees_writes_from_another_handle() {
    let dir = tempfile::TempDir::with_prefix("decentdb-reactive-cross").unwrap();
    let path = dir.path().join("cross.ddb");
    let db1 = Db::open_or_create(&path, DbConfig::default()).unwrap();
    db1.execute("CREATE TABLE users (id INT64 PRIMARY KEY)")
        .unwrap();
    let db2 = Db::open(&path, DbConfig::default()).unwrap();

    let watch = db1
        .watch_table(TableWatchOptions {
            tables: vec!["users".to_string()],
            queue_capacity: None,
        })
        .unwrap();
    assert!(matches!(recv_event(&watch), WatchEvent::Initial(_)));

    db2.execute("INSERT INTO users (id) VALUES (1)").unwrap();
    let WatchEvent::Invalidate(event) = recv_event(&watch) else {
        panic!("expected invalidation");
    };
    assert_eq!(event.tables, vec!["users"]);
}

#[test]
fn watch_queue_overflow_reports_lagged() {
    let config = DbConfig {
        reactive_watch_queue_capacity: 1,
        reactive_watch_queue_max_capacity: 1,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(":memory:", config).unwrap();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY)")
        .unwrap();
    let watch = db
        .watch_table(TableWatchOptions {
            tables: vec!["users".to_string()],
            queue_capacity: None,
        })
        .unwrap();

    db.execute("INSERT INTO users (id) VALUES (1)").unwrap();
    let WatchEvent::Lagged(event) = recv_event(&watch) else {
        panic!("expected lag event");
    };
    assert_eq!(event.reason, "queue_overflow");
    assert!(event.latest_event_id > 0);
}

#[test]
fn sys_reactive_tables_expose_metrics_and_subscriptions() {
    let db = memory_db();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY)")
        .unwrap();
    let watch = db
        .watch_table(TableWatchOptions {
            tables: vec!["users".to_string()],
            queue_capacity: None,
        })
        .unwrap();
    assert!(matches!(recv_event(&watch), WatchEvent::Initial(_)));

    let metrics = db.execute("SELECT * FROM sys.reactive_metrics").unwrap();
    assert_eq!(metrics.rows()[0].values()[0], Value::Int64(1));

    let subscriptions = db
        .execute("SELECT * FROM sys.reactive_subscriptions ORDER BY watch_id")
        .unwrap();
    assert_eq!(subscriptions.rows().len(), 1);
    assert_eq!(
        subscriptions.rows()[0].values()[1],
        Value::Text("table".to_string())
    );
}
