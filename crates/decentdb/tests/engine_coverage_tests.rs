use decentdb::{Db, DbConfig, Value};

#[test]
fn commit_persists_changes() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("COMMIT").unwrap();

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
}

#[test]
fn rollback_discards_changes() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("ROLLBACK").unwrap();

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
}

#[test]
fn is_null_operator() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();

    let result = db.execute("SELECT id FROM t WHERE val IS NULL").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
}

#[test]
fn is_not_null_operator() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();

    let result = db
        .execute("SELECT id FROM t WHERE val IS NOT NULL")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(2)]);
}

#[test]
fn cast_from_int_to_text() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT CAST(42 AS TEXT)").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Text("42".to_string())]);
}

#[test]
fn cast_from_text_to_int() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT CAST('42' AS INT64)").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(42)]);
}

#[test]
fn cast_parameterized_text_to_decimal() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute_with_params(
            "SELECT CAST($1 AS DECIMAL(10,2))",
            &[Value::Text("19.99".to_string())],
        )
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values(),
        &[Value::Decimal {
            scaled: 1999,
            scale: 2
        }]
    );
}

#[test]
fn count_aggregate() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let result = db
        .execute("SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM t")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values(),
        &[
            Value::Int64(3),
            Value::Int64(60),
            Value::Float64(20.0),
            Value::Int64(10),
            Value::Int64(30)
        ]
    );
}

#[test]
fn group_by() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, cat TEXT, val INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'a', 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'b', 30)").unwrap();

    let result = db
        .execute("SELECT cat, SUM(val) FROM t GROUP BY cat ORDER BY cat")
        .unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values(),
        &[Value::Text("a".to_string()), Value::Int64(30)]
    );
    assert_eq!(
        rows[1].values(),
        &[Value::Text("b".to_string()), Value::Int64(30)]
    );
}

#[test]
fn count_distinct() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'b')").unwrap();

    let result = db.execute("SELECT COUNT(DISTINCT val) FROM t").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(2)]);
}

#[test]
fn string_functions() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT LENGTH('hello'), UPPER('hello'), LOWER('WORLD'), TRIM('  hello  ')")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values(),
        &[
            Value::Int64(5),
            Value::Text("HELLO".to_string()),
            Value::Text("world".to_string()),
            Value::Text("hello".to_string())
        ]
    );
}

#[test]
fn case_when() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Text("yes".to_string())]);
}

#[test]
fn case_with_value() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Text("one".to_string())]);
}

#[test]
fn in_with_list() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    let result = db
        .execute("SELECT id FROM t WHERE id IN (1, 3) ORDER BY id")
        .unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
    assert_eq!(rows[1].values(), &[Value::Int64(3)]);
}

#[test]
fn not_in_with_list() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    let result = db
        .execute("SELECT id FROM t WHERE id NOT IN (1, 3)")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(2)]);
}

#[test]
fn coalesce_function() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();

    let result = db
        .execute("SELECT COALESCE(val, 'default') FROM t ORDER BY id")
        .unwrap();
    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values()[0],
        Value::Text("default".to_string())
    );
    assert_eq!(result.rows()[1].values()[0], Value::Text("x".to_string()));
}

#[test]
fn length_function() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT LENGTH('hello')").unwrap();
    assert_eq!(result.rows()[0].values()[0], Value::Int64(5));
}

#[test]
fn upper_and_lower_functions() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT UPPER('hello'), LOWER('WORLD')").unwrap();
    assert_eq!(
        result.rows()[0].values()[0],
        Value::Text("HELLO".to_string())
    );
    assert_eq!(
        result.rows()[0].values()[1],
        Value::Text("world".to_string())
    );
}

#[test]
fn substr_function() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT SUBSTR('hello', 2, 3)").unwrap();
    assert_eq!(result.rows()[0].values()[0], Value::Text("ell".to_string()));
}

#[test]
fn instr_function() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT INSTR('hello world', 'world')").unwrap();
    assert_eq!(result.rows()[0].values()[0], Value::Int64(7));
}

#[test]
fn replace_function() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT REPLACE('hello world', 'world', 'rust')")
        .unwrap();
    assert_eq!(
        result.rows()[0].values()[0],
        Value::Text("hello rust".to_string())
    );
}
