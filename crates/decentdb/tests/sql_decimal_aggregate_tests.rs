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

#[test]
fn decimal_casts_to_float64_and_int64_work() {
    let db = mem_db();

    let r = exec(
        &db,
        "SELECT
            CAST(CAST('12.50' AS DECIMAL(10, 2)) AS FLOAT64),
            CAST(CAST('12.50' AS DECIMAL(10, 2)) AS INT64),
            CAST(CAST('-12.50' AS DECIMAL(10, 2)) AS INT64)",
    );
    let row = r.rows()[0].values();

    assert_eq!(row[0], Value::Float64(12.5));
    assert_eq!(row[1], Value::Int64(12));
    assert_eq!(row[2], Value::Int64(-12));
}

#[test]
fn decimal_aggregates_over_casted_expressions_work() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (10.25)");
    exec(&db, "INSERT INTO t VALUES (20.75)");

    let r = exec(
        &db,
        "SELECT
            AVG(CAST(val AS FLOAT64)),
            SUM(CAST(val AS FLOAT64)),
            AVG(CAST(val AS INT64))
         FROM t",
    );
    let row = r.rows()[0].values();

    assert_eq!(row[0], Value::Float64(15.5));
    assert_eq!(row[1], Value::Float64(31.0));
    assert_eq!(row[2], Value::Float64(15.0));
}

#[test]
fn decimal_sum_and_avg_work_without_explicit_casts() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (10.25)");
    exec(&db, "INSERT INTO t VALUES (20.75)");

    let r = exec(&db, "SELECT SUM(val), AVG(val) FROM t");
    let row = r.rows()[0].values();

    assert_eq!(row[0], Value::Float64(31.0));
    assert_eq!(row[1], Value::Float64(15.5));
}

#[test]
fn grouped_decimal_sum_and_avg_work_without_explicit_casts() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(grp TEXT, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES ('a', 10.25)");
    exec(&db, "INSERT INTO t VALUES ('a', 20.75)");
    exec(&db, "INSERT INTO t VALUES ('b', 5.50)");

    let r = exec(
        &db,
        "SELECT grp, COUNT(*), SUM(val), AVG(val) FROM t GROUP BY grp ORDER BY grp",
    );

    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[0], Value::Text("a".into()));
    assert_eq!(r.rows()[0].values()[2], Value::Float64(31.0));
    assert_eq!(r.rows()[0].values()[3], Value::Float64(15.5));
    assert_eq!(r.rows()[1].values()[0], Value::Text("b".into()));
    assert_eq!(r.rows()[1].values()[2], Value::Float64(5.5));
    assert_eq!(r.rows()[1].values()[3], Value::Float64(5.5));
}
