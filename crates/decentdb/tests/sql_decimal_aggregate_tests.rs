use decentdb::{Db, DbConfig, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> decentdb::QueryResult {
    db.execute(sql).unwrap()
}

#[test]
fn decimal_min_max_aggregates_work() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(val DECIMAL(10, 3))");
    exec(&db, "INSERT INTO t VALUES (12.100)");
    exec(&db, "INSERT INTO t VALUES (48.598)");
    exec(&db, "INSERT INTO t VALUES (8.250)");

    let r = exec(&db, "SELECT MIN(val), MAX(val) FROM t");
    let row = r.rows()[0].values();

    assert_eq!(
        row[0],
        Value::Decimal {
            scaled: 825,
            scale: 2
        }
    );
    assert_eq!(
        row[1],
        Value::Decimal {
            scaled: 48598,
            scale: 3
        }
    );
}
