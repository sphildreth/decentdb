use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, Value};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn unique_db_path(label: &str) -> PathBuf {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    let pid = std::process::id();
    std::env::temp_dir().join(format!("decentdb-test-{}-{}-{}-{}", label, pid, stamp, id))
}

#[test]
fn on_conflict_do_update_filter_and_apply() {
    let path = unique_db_path("on-conflict");
    let db = Db::create(&path, DbConfig::default()).expect("create db");

    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)")
        .expect("create table");
    db.execute("INSERT INTO t (id, val) VALUES (1, 10)")
        .expect("insert 1");

    // Conflict where excluded.val > 15 true -> update
    db.execute("INSERT INTO t (id, val) VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE EXCLUDED.val > 15")
        .expect("upsert update");
    let res = db
        .execute("SELECT val FROM t WHERE id = 1")
        .expect("select");
    assert_eq!(res.rows()[0].values()[0], Value::Int64(20));

    // Conflict where excluded.val > 15 false -> no update
    db.execute("INSERT INTO t (id, val) VALUES (1, 12) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE EXCLUDED.val > 15")
        .expect("upsert update maybe no-op");
    let res2 = db
        .execute("SELECT val FROM t WHERE id = 1")
        .expect("select2");
    assert_eq!(res2.rows()[0].values()[0], Value::Int64(20));

    let _ = std::fs::remove_dir_all(&path);
}
