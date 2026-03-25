use decentdb::{Db, DbConfig, Value};
use tempfile::TempDir;

fn assert_float_close(value: &Value, expected: f64) {
    match value {
        Value::Float64(actual) => assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        ),
        other => panic!("expected FLOAT64 result, got {other:?}"),
    }
}

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

#[test]
fn math_scalar_functions_cover_documented_slice_6_surface() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                ABS(-42),
                CEIL(3.2),
                CEILING(3.2),
                FLOOR(3.8),
                ROUND(3.14159, 2),
                SQRT(144),
                POWER(2, 10),
                POW(2, 3),
                MOD(17, 5),
                SIGN(-99),
                LN(2.718281828),
                LOG(1000),
                LOG(2, 8),
                EXP(1)",
        )
        .unwrap();
    let row = result.rows()[0].values();
    assert_eq!(row[0], Value::Int64(42));
    assert_float_close(&row[1], 4.0);
    assert_float_close(&row[2], 4.0);
    assert_float_close(&row[3], 3.0);
    assert_float_close(&row[4], 3.14);
    assert_float_close(&row[5], 12.0);
    assert_float_close(&row[6], 1024.0);
    assert_float_close(&row[7], 8.0);
    assert_eq!(row[8], Value::Int64(2));
    assert_eq!(row[9], Value::Int64(-1));
    assert_float_close(&row[10], 1.0);
    assert_float_close(&row[11], 3.0);
    assert_float_close(&row[12], 3.0);
    assert_float_close(&row[13], std::f64::consts::E);
}

#[test]
fn math_scalar_functions_preserve_nulls_and_expected_edge_cases() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT ABS(NULL), MOD(5, 0), SQRT(-1), LOG(-10), ROUND(NULL, 2)")
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null
        ]
    );

    let random = db.execute("SELECT RANDOM()").unwrap();
    match random.rows()[0].values()[0] {
        Value::Float64(value) => assert!((0.0..1.0).contains(&value)),
        ref other => panic!("expected RANDOM() to return FLOAT64, got {other:?}"),
    }
}

#[test]
fn string_scalar_functions_cover_documented_slice_6_surface() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                LTRIM('  hello'),
                RTRIM('hello  '),
                LEFT('hello', 3),
                RIGHT('hello', 3),
                LPAD('42', 5, '0'),
                RPAD('hi', 5, '!'),
                REPEAT('ab', 3),
                REVERSE('hello'),
                CHR(65),
                HEX('ABC'),
                SUBSTRING('hello world', 1, 5)",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("hello".to_string()),
            Value::Text("hello".to_string()),
            Value::Text("hel".to_string()),
            Value::Text("llo".to_string()),
            Value::Text("00042".to_string()),
            Value::Text("hi!!!".to_string()),
            Value::Text("ababab".to_string()),
            Value::Text("olleh".to_string()),
            Value::Text("A".to_string()),
            Value::Text("414243".to_string()),
            Value::Text("hello".to_string()),
        ]
    );
}

#[test]
fn json_scalar_functions_cover_documented_slice_6_surface() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                JSON_OBJECT('age', 30, 'name', 'Alice'),
                JSON_ARRAY(1, 2, 'three', NULL),
                JSON_TYPE('{\"a\": 1}', '$.a'),
                JSON_TYPE('[1, 2, 3]'),
                JSON_TYPE('{\"a\": 1}', '$.missing'),
                JSON_VALID('{\"a\":1}'),
                JSON_VALID('not json'),
                JSON_VALID(NULL)",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("{\"age\":30,\"name\":\"Alice\"}".to_string()),
            Value::Text("[1,2,\"three\",null]".to_string()),
            Value::Text("integer".to_string()),
            Value::Text("array".to_string()),
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Null,
        ]
    );
}

#[test]
fn date_time_scalar_functions_cover_documented_slice_6_examples() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                DATE('2024-03-15', '+1 month'),
                DATETIME('2024-03-15 10:30:00', '+2 hours'),
                STRFTIME('%Y-%m-%d', '2024-03-15 14:30:00'),
                STRFTIME('%H:%M:%S', '2024-03-15 14:30:00'),
                STRFTIME('%Y', '2024-03-15'),
                EXTRACT(YEAR FROM '2024-03-15'),
                EXTRACT(MONTH FROM '2024-03-15'),
                EXTRACT(DOW FROM '2024-03-15'),
                EXTRACT(HOUR FROM '2024-03-15 14:30:00')",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("2024-04-15".to_string()),
            Value::Text("2024-03-15 12:30:00".to_string()),
            Value::Text("2024-03-15".to_string()),
            Value::Text("14:30:00".to_string()),
            Value::Text("2024".to_string()),
            Value::Int64(2024),
            Value::Int64(3),
            Value::Int64(5),
            Value::Int64(14),
        ]
    );
}

#[test]
fn current_date_time_functions_return_expected_shapes() {
    use chrono::{Datelike, NaiveDate, NaiveTime, Utc};

    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, NOW()")
        .unwrap();
    let row = result.rows()[0].values();

    let expected_year = i64::from(Utc::now().year());
    assert_eq!(
        db.execute("SELECT EXTRACT(YEAR FROM CURRENT_TIMESTAMP)")
            .unwrap()
            .rows()[0]
            .values()[0],
        Value::Int64(expected_year)
    );

    match &row[0] {
        Value::Text(value) => {
            NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("CURRENT_DATE format")
        }
        other => panic!("expected CURRENT_DATE text output, got {other:?}"),
    };
    match &row[1] {
        Value::Text(value) => {
            NaiveTime::parse_from_str(value, "%H:%M:%S").expect("CURRENT_TIME format")
        }
        other => panic!("expected CURRENT_TIME text output, got {other:?}"),
    };
    assert!(matches!(row[2], Value::TimestampMicros(_)));
    assert!(matches!(row[3], Value::TimestampMicros(_)));
}

#[test]
fn date_time_functions_propagate_nulls() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT DATE(NULL), DATETIME(NULL), STRFTIME('%Y', NULL), EXTRACT(YEAR FROM NULL)")
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Null, Value::Null, Value::Null, Value::Null]
    );
}

#[test]
fn uuid_helper_functions_round_trip_and_generate_v4_values() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                UUID_TO_STRING(UUID_PARSE('550e8400-e29b-41d4-a716-446655440000')),
                GEN_RANDOM_UUID(),
                UUID_TO_STRING(GEN_RANDOM_UUID()),
                UUID_PARSE(NULL),
                UUID_TO_STRING(NULL)",
        )
        .unwrap();
    let row = result.rows()[0].values();

    assert_eq!(
        row[0],
        Value::Text("550e8400-e29b-41d4-a716-446655440000".to_string())
    );
    match &row[1] {
        Value::Uuid(value) => {
            assert_eq!(value[6] & 0xf0, 0x40);
            assert_eq!(value[8] & 0xc0, 0x80);
        }
        other => panic!("expected GEN_RANDOM_UUID() to return UUID, got {other:?}"),
    }
    match &row[2] {
        Value::Text(value) => {
            assert_eq!(value.len(), 36);
            assert_eq!(value.as_bytes()[8], b'-');
            assert_eq!(value.as_bytes()[13], b'-');
            assert_eq!(value.as_bytes()[18], b'-');
            assert_eq!(value.as_bytes()[23], b'-');
        }
        other => panic!("expected UUID_TO_STRING(GEN_RANDOM_UUID()) text, got {other:?}"),
    }
    assert_eq!(row[3], Value::Null);
    assert_eq!(row[4], Value::Null);
}

#[test]
fn json_operators_execute_and_round_trip_through_views() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE VIEW json_ops AS
         SELECT
             '{\"name\":\"Alice\",\"meta\":{\"version\":2}}'->>'name' AS name_text,
             '{\"name\":\"Alice\"}'->'name' AS name_json,
             '[10,20,30]'->>1 AS second_item,
             '{\"meta\":{\"version\":2}}'->'meta'->>'version' AS version_text",
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT name_text, name_json, second_item, version_text,
                    NULL->>'name',
                    '{\"name\":\"Alice\"}'->>'missing'
             FROM json_ops",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("Alice".to_string()),
            Value::Text("\"Alice\"".to_string()),
            Value::Text("20".to_string()),
            Value::Text("2".to_string()),
            Value::Null,
            Value::Null,
        ]
    );
}

#[test]
fn json_table_functions_execute_documented_examples() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let each = db
        .execute("SELECT key, value, type FROM json_each('[10,20,30]') ORDER BY key")
        .unwrap();
    assert_eq!(
        each.rows()[0].values(),
        &[
            Value::Int64(0),
            Value::Int64(10),
            Value::Text("integer".to_string())
        ]
    );
    assert_eq!(
        each.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Int64(20),
            Value::Text("integer".to_string())
        ]
    );
    assert_eq!(
        each.rows()[2].values(),
        &[
            Value::Int64(2),
            Value::Int64(30),
            Value::Text("integer".to_string())
        ]
    );

    let object_each = db
        .execute("SELECT key, value, type FROM json_each('{\"a\":1,\"b\":2}') ORDER BY key")
        .unwrap();
    assert_eq!(
        object_each.rows()[0].values(),
        &[
            Value::Text("a".to_string()),
            Value::Int64(1),
            Value::Text("integer".to_string())
        ]
    );
    assert_eq!(
        object_each.rows()[1].values(),
        &[
            Value::Text("b".to_string()),
            Value::Int64(2),
            Value::Text("integer".to_string())
        ]
    );

    let tree = db
        .execute(
            "SELECT key, value, type, path
             FROM json_tree('{\"a\":{\"b\":1},\"c\":[2,3]}')
             ORDER BY path",
        )
        .unwrap();
    assert_eq!(
        tree.rows()[0].values(),
        &[
            Value::Null,
            Value::Text("{\"a\":{\"b\":1},\"c\":[2,3]}".to_string()),
            Value::Text("object".to_string()),
            Value::Text("$".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[1].values(),
        &[
            Value::Text("a".to_string()),
            Value::Text("{\"b\":1}".to_string()),
            Value::Text("object".to_string()),
            Value::Text("$.a".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[2].values(),
        &[
            Value::Text("b".to_string()),
            Value::Int64(1),
            Value::Text("integer".to_string()),
            Value::Text("$.a.b".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[3].values(),
        &[
            Value::Text("c".to_string()),
            Value::Text("[2,3]".to_string()),
            Value::Text("array".to_string()),
            Value::Text("$.c".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[4].values(),
        &[
            Value::Int64(0),
            Value::Int64(2),
            Value::Text("integer".to_string()),
            Value::Text("$.c[0]".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[5].values(),
        &[
            Value::Int64(1),
            Value::Int64(3),
            Value::Text("integer".to_string()),
            Value::Text("$.c[1]".to_string())
        ]
    );
}

#[test]
fn json_table_functions_handle_null_inputs_and_view_roundtrip() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let each = db.execute("SELECT * FROM json_each(NULL)").unwrap();
    assert!(each.rows().is_empty());

    let tree = db.execute("SELECT * FROM json_tree(NULL)").unwrap();
    assert!(tree.rows().is_empty());

    db.execute("CREATE VIEW json_each_view AS SELECT key, value FROM json_each('[10,20]')")
        .unwrap();
    let view_rows = db
        .execute("SELECT key, value FROM json_each_view ORDER BY key")
        .unwrap();
    assert_eq!(view_rows.rows().len(), 2);
    assert_eq!(
        view_rows.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(10)]
    );
    assert_eq!(
        view_rows.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(20)]
    );
}

#[test]
fn slice_6_scalar_function_type_errors_are_explicit() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let abs_error = db.execute("SELECT ABS('not-a-number')").unwrap_err();
    assert!(abs_error
        .to_string()
        .contains("ABS expects numeric input for first argument"));

    let left_error = db.execute("SELECT LEFT(123, 2)").unwrap_err();
    assert!(left_error
        .to_string()
        .contains("LEFT expects text for first argument"));

    let uuid_error = db.execute("SELECT UUID_PARSE('not-a-uuid')").unwrap_err();
    assert!(uuid_error
        .to_string()
        .contains("UUID_PARSE expects canonical UUID text"));

    let json_operator_error = db.execute("SELECT 1->>'name'").unwrap_err();
    assert!(json_operator_error
        .to_string()
        .contains("JSON operators expect text JSON input"));

    let json_each_error = db
        .execute("SELECT * FROM json_each('not json')")
        .unwrap_err();
    assert!(json_each_error.to_string().contains("invalid JSON"));
}

#[test]
fn string_agg_alias_supports_grouped_concatenation() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE employees (id INT64 PRIMARY KEY, dept TEXT, name TEXT)")
        .unwrap();
    db.execute(
        "INSERT INTO employees VALUES
            (1, 'eng', 'Ada'),
            (2, 'eng', NULL),
            (3, 'eng', 'Linus'),
            (4, 'ops', 'Grace')",
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT dept, STRING_AGG(name, ', ')
             FROM employees
             GROUP BY dept
             ORDER BY dept",
        )
        .unwrap();

    assert_eq!(result.rows().len(), 2);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("eng".to_string()),
            Value::Text("Ada, Linus".to_string())
        ]
    );
    assert_eq!(
        result.rows()[1].values(),
        &[
            Value::Text("ops".to_string()),
            Value::Text("Grace".to_string())
        ]
    );
}

#[test]
fn total_aggregate_uses_float_semantics_and_zero_for_empty_inputs() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE nums (id INT64 PRIMARY KEY, val INT64)")
        .unwrap();
    db.execute("INSERT INTO nums VALUES (1, 10), (2, 10), (3, 20)")
        .unwrap();

    let totals = db
        .execute("SELECT TOTAL(val), TOTAL(DISTINCT val), SUM(val) FROM nums")
        .unwrap();
    assert_eq!(
        totals.rows()[0].values(),
        &[Value::Float64(40.0), Value::Float64(30.0), Value::Int64(40)]
    );

    let mixed = db
        .execute(
            "SELECT TOTAL(CASE id WHEN 1 THEN 1 ELSE 2.5 END)
             FROM nums
             WHERE id < 3",
        )
        .unwrap();
    assert_eq!(mixed.rows()[0].values(), &[Value::Float64(3.5)]);

    let empty = db
        .execute("SELECT TOTAL(val), SUM(val) FROM nums WHERE id > 99")
        .unwrap();
    assert_eq!(
        empty.rows()[0].values(),
        &[Value::Float64(0.0), Value::Null]
    );
}

#[test]
fn set_operation_all_variants_respect_duplicate_counts() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE lhs (id INT64 PRIMARY KEY, value INT64)")
        .unwrap();
    db.execute("CREATE TABLE rhs (id INT64 PRIMARY KEY, value INT64)")
        .unwrap();
    db.execute("INSERT INTO lhs VALUES (1, 1), (2, 1), (3, 2), (4, 2)")
        .unwrap();
    db.execute("INSERT INTO rhs VALUES (1, 1), (2, 2), (3, 2), (4, 2)")
        .unwrap();

    let intersect_all = db
        .execute(
            "SELECT value FROM lhs
             INTERSECT ALL
             SELECT value FROM rhs
             ORDER BY value",
        )
        .unwrap();
    assert_eq!(intersect_all.rows().len(), 3);
    assert_eq!(intersect_all.rows()[0].values(), &[Value::Int64(1)]);
    assert_eq!(intersect_all.rows()[1].values(), &[Value::Int64(2)]);
    assert_eq!(intersect_all.rows()[2].values(), &[Value::Int64(2)]);

    let intersect = db
        .execute(
            "SELECT value FROM lhs
             INTERSECT
             SELECT value FROM rhs
             ORDER BY value",
        )
        .unwrap();
    assert_eq!(intersect.rows().len(), 2);
    assert_eq!(intersect.rows()[0].values(), &[Value::Int64(1)]);
    assert_eq!(intersect.rows()[1].values(), &[Value::Int64(2)]);

    let except_all = db
        .execute(
            "SELECT value FROM lhs
             EXCEPT ALL
             SELECT value FROM rhs
             ORDER BY value",
        )
        .unwrap();
    assert_eq!(except_all.rows().len(), 1);
    assert_eq!(except_all.rows()[0].values(), &[Value::Int64(1)]);

    let except = db
        .execute(
            "SELECT value FROM lhs
             EXCEPT
             SELECT value FROM rhs",
        )
        .unwrap();
    assert!(except.rows().is_empty());
}

#[test]
fn limit_all_keeps_unbounded_results_and_still_allows_offset() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    let all_rows = db
        .execute("SELECT id FROM t ORDER BY id LIMIT ALL")
        .unwrap();
    assert_eq!(all_rows.rows().len(), 3);
    assert_eq!(all_rows.rows()[0].values(), &[Value::Int64(1)]);
    assert_eq!(all_rows.rows()[1].values(), &[Value::Int64(2)]);
    assert_eq!(all_rows.rows()[2].values(), &[Value::Int64(3)]);

    let offset_rows = db
        .execute("SELECT id FROM t ORDER BY id LIMIT ALL OFFSET 1")
        .unwrap();
    assert_eq!(offset_rows.rows().len(), 2);
    assert_eq!(offset_rows.rows()[0].values(), &[Value::Int64(2)]);
    assert_eq!(offset_rows.rows()[1].values(), &[Value::Int64(3)]);
}

#[test]
fn offset_fetch_uses_existing_limit_offset_pipeline() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = db
        .execute("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY")
        .unwrap();
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values(), &[Value::Int64(2)]);
    assert_eq!(result.rows()[1].values(), &[Value::Int64(3)]);
}

#[test]
fn select_distinct_and_distinct_on_apply_runtime_deduplication() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, a INT64, b INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 2, 25), (4, 2, 30)")
        .unwrap();

    let distinct = db.execute("SELECT DISTINCT a FROM t ORDER BY a").unwrap();
    assert_eq!(distinct.rows().len(), 2);
    assert_eq!(distinct.rows()[0].values(), &[Value::Int64(1)]);
    assert_eq!(distinct.rows()[1].values(), &[Value::Int64(2)]);

    let distinct_on = db
        .execute("SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b DESC")
        .unwrap();
    assert_eq!(distinct_on.rows().len(), 2);
    assert_eq!(
        distinct_on.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(20)]
    );
    assert_eq!(
        distinct_on.rows()[1].values(),
        &[Value::Int64(2), Value::Int64(30)]
    );
}

#[test]
fn window_value_accessors_work_with_partitions() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE TABLE employees (
            id INT64 PRIMARY KEY,
            name TEXT,
            department TEXT,
            salary INT64
        )",
    )
    .unwrap();
    db.execute(
        "INSERT INTO employees VALUES
            (1, 'Ada', 'eng', 100),
            (2, 'Grace', 'eng', 90),
            (3, 'Linus', 'eng', 90),
            (4, 'Ken', 'ops', 80),
            (5, 'Denise', 'ops', 70)",
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT name, department, salary,
                    ROW_NUMBER()   OVER (PARTITION BY department ORDER BY salary DESC) AS rn,
                    RANK()         OVER (PARTITION BY department ORDER BY salary DESC) AS rnk,
                    DENSE_RANK()   OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rnk,
                    FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) AS top_earner,
                    LAST_VALUE(name)  OVER (PARTITION BY department ORDER BY salary DESC) AS low_earner,
                    NTH_VALUE(name, 2) OVER (PARTITION BY department ORDER BY salary DESC) AS second_earner
             FROM employees
             ORDER BY department, salary DESC, name",
        )
        .unwrap();

    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("Ada".to_string()),
                Value::Text("eng".to_string()),
                Value::Int64(100),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
                Value::Text("Ada".to_string()),
                Value::Text("Linus".to_string()),
                Value::Text("Grace".to_string()),
            ],
            vec![
                Value::Text("Grace".to_string()),
                Value::Text("eng".to_string()),
                Value::Int64(90),
                Value::Int64(2),
                Value::Int64(2),
                Value::Int64(2),
                Value::Text("Ada".to_string()),
                Value::Text("Linus".to_string()),
                Value::Text("Grace".to_string()),
            ],
            vec![
                Value::Text("Linus".to_string()),
                Value::Text("eng".to_string()),
                Value::Int64(90),
                Value::Int64(3),
                Value::Int64(2),
                Value::Int64(2),
                Value::Text("Ada".to_string()),
                Value::Text("Linus".to_string()),
                Value::Text("Grace".to_string()),
            ],
            vec![
                Value::Text("Ken".to_string()),
                Value::Text("ops".to_string()),
                Value::Int64(80),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
                Value::Text("Ken".to_string()),
                Value::Text("Denise".to_string()),
                Value::Text("Denise".to_string()),
            ],
            vec![
                Value::Text("Denise".to_string()),
                Value::Text("ops".to_string()),
                Value::Int64(70),
                Value::Int64(2),
                Value::Int64(2),
                Value::Int64(2),
                Value::Text("Ken".to_string()),
                Value::Text("Denise".to_string()),
                Value::Text("Denise".to_string()),
            ],
        ]
    );
}

#[test]
fn nth_value_out_of_range_returns_null_and_lag_lead_still_work() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE names (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO names VALUES (1, 'Ada'), (2, 'Grace'), (3, 'Linus')")
        .unwrap();

    let result = db
        .execute(
            "SELECT name,
                    LAG(name, 1) OVER (ORDER BY id) AS prev_name,
                    LEAD(name, 1) OVER (ORDER BY id) AS next_name,
                    NTH_VALUE(name, 10) OVER (ORDER BY id) AS tenth_name
             FROM names
             ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("Ada".to_string()),
                Value::Null,
                Value::Text("Grace".to_string()),
                Value::Null,
            ],
            vec![
                Value::Text("Grace".to_string()),
                Value::Text("Ada".to_string()),
                Value::Text("Linus".to_string()),
                Value::Null,
            ],
            vec![
                Value::Text("Linus".to_string()),
                Value::Text("Grace".to_string()),
                Value::Null,
                Value::Null,
            ],
        ]
    );

    let err = db
        .execute("SELECT NTH_VALUE(name, 0) OVER (ORDER BY id) FROM names")
        .unwrap_err();
    assert!(
        err.to_string().contains("NTH_VALUE position must be >= 1"),
        "unexpected error: {err}"
    );
}

#[test]
fn cross_right_and_full_outer_joins_execute_with_null_extension() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE lhs (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE rhs (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO lhs VALUES (1, 'left-only'), (2, 'shared')")
        .unwrap();
    db.execute("INSERT INTO rhs VALUES (2, 'shared'), (3, 'right-only')")
        .unwrap();

    let cross = db
        .execute(
            "SELECT l.id, r.id
             FROM lhs AS l CROSS JOIN rhs AS r
             ORDER BY l.id, r.id",
        )
        .unwrap();
    assert_eq!(cross.rows().len(), 4);
    assert_eq!(
        cross.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(2)]
    );
    assert_eq!(
        cross.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(3)]
    );
    assert_eq!(
        cross.rows()[2].values(),
        &[Value::Int64(2), Value::Int64(2)]
    );
    assert_eq!(
        cross.rows()[3].values(),
        &[Value::Int64(2), Value::Int64(3)]
    );

    let right = db
        .execute(
            "SELECT l.id, l.name, r.id, r.label
             FROM lhs AS l RIGHT JOIN rhs AS r ON l.id = r.id
             ORDER BY r.id",
        )
        .unwrap();
    assert_eq!(
        right
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Int64(2),
                Value::Text("shared".to_string()),
                Value::Int64(2),
                Value::Text("shared".to_string()),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Int64(3),
                Value::Text("right-only".to_string()),
            ],
        ]
    );

    let full = db
        .execute(
            "SELECT l.id, l.name, r.id, r.label
             FROM lhs AS l FULL OUTER JOIN rhs AS r ON l.id = r.id
             ORDER BY COALESCE(l.id, r.id)",
        )
        .unwrap();
    assert_eq!(
        full.rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Int64(1),
                Value::Text("left-only".to_string()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Int64(2),
                Value::Text("shared".to_string()),
                Value::Int64(2),
                Value::Text("shared".to_string()),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Int64(3),
                Value::Text("right-only".to_string()),
            ],
        ]
    );
}

#[test]
fn analyze_executes_in_autocommit_and_rejects_explicit_transactions() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT)")
        .unwrap();
    db.execute("CREATE INDEX docs_email_idx ON docs (email)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'a@example.com'), (2, 'a@example.com')")
        .unwrap();

    db.execute("ANALYZE docs").unwrap();

    db.execute("BEGIN").unwrap();
    let err = db.execute("ANALYZE docs").unwrap_err();
    assert!(
        err.to_string()
            .contains("ANALYZE is not supported inside an explicit SQL transaction"),
        "unexpected error: {err}"
    );
    db.execute("ROLLBACK").unwrap();
}

#[test]
fn analyze_stats_persist_across_reopen() {
    let tempdir = TempDir::new().unwrap();
    let path = tempdir.path().join("analyze-stats.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT)")
            .unwrap();
        db.execute("CREATE INDEX docs_email_idx ON docs (email)")
            .unwrap();
        db.execute("INSERT INTO docs VALUES (1, 'a@example.com'), (2, 'b@example.com')")
            .unwrap();
        db.execute("ANALYZE docs").unwrap();
    }

    let reopened = Db::open_or_create(&path, DbConfig::default()).unwrap();
    reopened.execute("ANALYZE docs").unwrap();
}

#[test]
fn generated_columns_compute_recompute_and_survive_reopen() {
    let tempdir = TempDir::new().unwrap();
    let path = tempdir.path().join("generated-columns.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
        db.execute(
            "CREATE TABLE products (
                id INT64 PRIMARY KEY,
                price FLOAT64,
                qty INT64,
                total FLOAT64 GENERATED ALWAYS AS (price * qty) STORED
            )",
        )
        .unwrap();
        db.execute("INSERT INTO products (id, price, qty) VALUES (1, 9.99, 3)")
            .unwrap();

        let inserted = db
            .execute("SELECT total FROM products WHERE id = 1")
            .unwrap();
        assert_float_close(&inserted.rows()[0].values()[0], 29.97);

        let insert_err = db
            .execute("INSERT INTO products (id, price, qty, total) VALUES (2, 5.0, 2, 10.0)")
            .unwrap_err();
        assert!(
            insert_err
                .to_string()
                .contains("cannot INSERT into generated column products.total"),
            "unexpected error: {insert_err}"
        );
    }

    let reopened = Db::open_or_create(&path, DbConfig::default()).unwrap();
    reopened
        .execute("UPDATE products SET qty = 4 WHERE id = 1")
        .unwrap();
    let updated = reopened
        .execute("SELECT total FROM products WHERE id = 1")
        .unwrap();
    assert_float_close(&updated.rows()[0].values()[0], 39.96);

    let update_err = reopened
        .execute("UPDATE products SET total = 0 WHERE id = 1")
        .unwrap_err();
    assert!(
        update_err
            .to_string()
            .contains("cannot UPDATE generated column products.total"),
        "unexpected error: {update_err}"
    );
}

#[test]
fn generated_columns_participate_in_unique_constraints() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE TABLE users (
            id INT64 PRIMARY KEY,
            email TEXT,
            email_lc TEXT GENERATED ALWAYS AS (LOWER(email)) STORED UNIQUE
        )",
    )
    .unwrap();
    db.execute("INSERT INTO users (id, email) VALUES (1, 'Ada@Example.com')")
        .unwrap();

    let err = db
        .execute("INSERT INTO users (id, email) VALUES (2, 'ada@example.com')")
        .unwrap_err();
    assert!(
        err.to_string().contains("unique constraint") && err.to_string().contains("users"),
        "unexpected error: {err}"
    );
}

#[test]
fn temp_tables_and_views_are_session_scoped_shadow_persistent_objects_and_do_not_persist() {
    let tempdir = TempDir::new().unwrap();
    let path = tempdir.path().join("temp-objects.ddb");

    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE base (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO base VALUES (1, 'persistent')")
        .unwrap();

    let persistent_schema_cookie = db.schema_cookie().unwrap();
    db.execute("CREATE TEMP TABLE base (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO base VALUES (2, 'temporary')")
        .unwrap();
    db.execute("CREATE TEMP VIEW recent_base AS SELECT id, val FROM base")
        .unwrap();

    assert_eq!(db.schema_cookie().unwrap(), persistent_schema_cookie);
    assert_eq!(
        db.execute("SELECT id, val FROM base").unwrap().rows()[0].values(),
        &[Value::Int64(2), Value::Text("temporary".to_string())]
    );
    assert_eq!(
        db.execute("SELECT id, val FROM recent_base")
            .unwrap()
            .rows()[0]
            .values(),
        &[Value::Int64(2), Value::Text("temporary".to_string())]
    );
    assert!(db
        .table_ddl("base")
        .unwrap()
        .starts_with("CREATE TEMP TABLE"));
    assert!(db
        .view_ddl("recent_base")
        .unwrap()
        .starts_with("CREATE TEMP VIEW"));

    let tables = db.list_tables().unwrap();
    assert!(tables
        .iter()
        .any(|table| table.name == "base" && table.temporary));
    assert!(tables
        .iter()
        .any(|table| table.name == "base" && !table.temporary));
    let views = db.list_views().unwrap();
    assert!(views
        .iter()
        .any(|view| view.name == "recent_base" && view.temporary));

    let other = Db::open_or_create(&path, DbConfig::default()).unwrap();
    assert_eq!(
        other.execute("SELECT id, val FROM base").unwrap().rows()[0].values(),
        &[Value::Int64(1), Value::Text("persistent".to_string())]
    );
    let missing_temp_view = other.execute("SELECT * FROM recent_base").unwrap_err();
    assert!(
        missing_temp_view
            .to_string()
            .contains("unknown table or view recent_base"),
        "unexpected error: {missing_temp_view}"
    );

    drop(db);
    let reopened = Db::open_or_create(&path, DbConfig::default()).unwrap();
    assert_eq!(
        reopened.execute("SELECT id, val FROM base").unwrap().rows()[0].values(),
        &[Value::Int64(1), Value::Text("persistent".to_string())]
    );
    let missing_reopened_temp_view = reopened.execute("SELECT * FROM recent_base").unwrap_err();
    assert!(
        missing_reopened_temp_view
            .to_string()
            .contains("unknown table or view recent_base"),
        "unexpected error: {missing_reopened_temp_view}"
    );
}

#[test]
fn temp_schema_changes_invalidate_prepared_statements_and_drop_reveals_persistent_tables() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'persistent')")
        .unwrap();

    let prepared = db.prepare("SELECT val FROM docs").unwrap();
    assert_eq!(
        prepared.execute(&[]).unwrap().rows()[0].values(),
        &[Value::Text("persistent".to_string())]
    );

    db.execute("CREATE TEMP TABLE docs (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    let stale = prepared.execute(&[]).unwrap_err();
    assert!(
        stale
            .to_string()
            .contains("prepared statement is no longer valid because the schema changed"),
        "unexpected error: {stale}"
    );

    db.execute("INSERT INTO docs VALUES (2, 'temporary')")
        .unwrap();
    assert_eq!(
        db.execute("SELECT val FROM docs").unwrap().rows()[0].values(),
        &[Value::Text("temporary".to_string())]
    );

    db.execute("DROP TABLE docs").unwrap();
    assert_eq!(
        db.execute("SELECT val FROM docs").unwrap().rows()[0].values(),
        &[Value::Text("persistent".to_string())]
    );
}

#[test]
fn string_agg_and_total_cover_default_separator_and_all_null_input() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE agg_edges (id INT64 PRIMARY KEY, name TEXT, val INT64)")
        .unwrap();
    db.execute(
        "INSERT INTO agg_edges VALUES
            (1, 'Ada', NULL),
            (2, NULL, NULL),
            (3, 'Grace', NULL)",
    )
    .unwrap();

    let result = db
        .execute("SELECT STRING_AGG(name), STRING_AGG(name, NULL), TOTAL(val) FROM agg_edges")
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("Ada,Grace".to_string()),
            Value::Text("AdaGrace".to_string()),
            Value::Float64(0.0),
        ]
    );

    let empty = db
        .execute("SELECT STRING_AGG(name), TOTAL(val) FROM agg_edges WHERE id > 99")
        .unwrap();
    assert_eq!(
        empty.rows()[0].values(),
        &[Value::Null, Value::Float64(0.0)]
    );
}

#[test]
fn set_operation_all_variants_cover_multi_column_and_empty_inputs() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE lhs_pairs (id INT64 PRIMARY KEY, a INT64, b TEXT)")
        .unwrap();
    db.execute("CREATE TABLE rhs_pairs (id INT64 PRIMARY KEY, a INT64, b TEXT)")
        .unwrap();
    db.execute(
        "INSERT INTO lhs_pairs VALUES
            (1, 1, 'x'),
            (2, 1, 'x'),
            (3, 2, 'y')",
    )
    .unwrap();
    db.execute(
        "INSERT INTO rhs_pairs VALUES
            (1, 1, 'x'),
            (2, 2, 'y'),
            (3, 2, 'y')",
    )
    .unwrap();

    let intersect_all = db
        .execute(
            "SELECT a, b FROM lhs_pairs
             INTERSECT ALL
             SELECT a, b FROM rhs_pairs
             ORDER BY a, b",
        )
        .unwrap();
    assert_eq!(intersect_all.rows().len(), 2);
    assert_eq!(
        intersect_all.rows()[0].values(),
        &[Value::Int64(1), Value::Text("x".to_string())]
    );
    assert_eq!(
        intersect_all.rows()[1].values(),
        &[Value::Int64(2), Value::Text("y".to_string())]
    );

    let except_all = db
        .execute(
            "SELECT a, b FROM lhs_pairs
             EXCEPT ALL
             SELECT a, b FROM rhs_pairs
             ORDER BY a, b",
        )
        .unwrap();
    assert_eq!(except_all.rows().len(), 1);
    assert_eq!(
        except_all.rows()[0].values(),
        &[Value::Int64(1), Value::Text("x".to_string())]
    );

    let intersect_empty = db
        .execute(
            "SELECT a, b FROM lhs_pairs WHERE id > 99
             INTERSECT ALL
             SELECT a, b FROM rhs_pairs",
        )
        .unwrap();
    assert!(intersect_empty.rows().is_empty());

    let except_empty_right = db
        .execute(
            "SELECT a, b FROM lhs_pairs
             EXCEPT ALL
             SELECT a, b FROM rhs_pairs WHERE id > 99
             ORDER BY a, b",
        )
        .unwrap();
    assert_eq!(
        except_empty_right
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Text("x".to_string())],
            vec![Value::Int64(1), Value::Text("x".to_string())],
            vec![Value::Int64(2), Value::Text("y".to_string())],
        ]
    );
}

#[test]
fn distinct_and_pagination_cover_full_row_dedup_and_empty_fetch() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE distinct_edges (id INT64 PRIMARY KEY, a INT64, b TEXT)")
        .unwrap();
    db.execute(
        "INSERT INTO distinct_edges VALUES
            (1, 1, 'x'),
            (2, 1, 'x'),
            (3, 1, 'y'),
            (4, 2, 'z')",
    )
    .unwrap();

    let distinct = db
        .execute("SELECT DISTINCT a, b FROM distinct_edges ORDER BY a, b")
        .unwrap();
    assert_eq!(
        distinct
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Text("x".to_string())],
            vec![Value::Int64(1), Value::Text("y".to_string())],
            vec![Value::Int64(2), Value::Text("z".to_string())],
        ]
    );

    let distinct_on = db
        .execute(
            "SELECT DISTINCT ON (a) a, b
             FROM distinct_edges
             ORDER BY a, b ASC",
        )
        .unwrap();
    assert_eq!(
        distinct_on
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Text("x".to_string())],
            vec![Value::Int64(2), Value::Text("z".to_string())],
        ]
    );

    let fetch_empty = db
        .execute(
            "SELECT id FROM distinct_edges
             ORDER BY id
             OFFSET 10 ROWS FETCH NEXT 2 ROWS ONLY",
        )
        .unwrap();
    assert!(fetch_empty.rows().is_empty());
}

#[test]
fn window_value_accessors_cover_single_row_and_argument_validation() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE window_edges (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO window_edges VALUES (1, 'Ada'), (2, 'Grace')")
        .unwrap();

    let single = db
        .execute(
            "SELECT FIRST_VALUE(name) OVER (PARTITION BY id ORDER BY id),
                    LAST_VALUE(name) OVER (PARTITION BY id ORDER BY id),
                    NTH_VALUE(name, 1) OVER (PARTITION BY id ORDER BY id)
             FROM window_edges
             WHERE id = 1",
        )
        .unwrap();
    assert_eq!(
        single.rows()[0].values(),
        &[
            Value::Text("Ada".to_string()),
            Value::Text("Ada".to_string()),
            Value::Text("Ada".to_string()),
        ]
    );

    let first_err = db
        .execute("SELECT FIRST_VALUE(name, name) OVER (ORDER BY id) FROM window_edges")
        .unwrap_err();
    assert!(
        first_err
            .to_string()
            .contains("FIRST_VALUE expects exactly 1 argument"),
        "unexpected error: {first_err}"
    );

    let last_err = db
        .execute("SELECT LAST_VALUE() OVER (ORDER BY id) FROM window_edges")
        .unwrap_err();
    assert!(
        last_err
            .to_string()
            .contains("LAST_VALUE expects exactly 1 argument"),
        "unexpected error: {last_err}"
    );

    let nth_type_err = db
        .execute("SELECT NTH_VALUE(name, 'bad') OVER (ORDER BY id) FROM window_edges")
        .unwrap_err();
    assert!(
        nth_type_err
            .to_string()
            .contains("NTH_VALUE position must be INT64"),
        "unexpected error: {nth_type_err}"
    );
}

#[test]
fn joins_cover_empty_sides_and_using_outer_merge_columns() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE left_empty (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE right_populated (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO right_populated VALUES (1, 'r1'), (2, 'r2')")
        .unwrap();

    let cross = db
        .execute(
            "SELECT l.id, r.id
             FROM left_empty AS l CROSS JOIN right_populated AS r",
        )
        .unwrap();
    assert!(cross.rows().is_empty());

    let right = db
        .execute(
            "SELECT l.id, r.id
             FROM left_empty AS l RIGHT JOIN right_populated AS r ON l.id = r.id
             ORDER BY r.id",
        )
        .unwrap();
    assert_eq!(
        right
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Null, Value::Int64(1)],
            vec![Value::Null, Value::Int64(2)],
        ]
    );

    db.execute("CREATE TABLE left_populated (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE right_empty (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO left_populated VALUES (1, 'l1'), (2, 'l2')")
        .unwrap();

    let full = db
        .execute(
            "SELECT l.id, r.id
             FROM left_populated AS l FULL OUTER JOIN right_empty AS r ON l.id = r.id
             ORDER BY l.id",
        )
        .unwrap();
    assert_eq!(
        full.rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Null],
            vec![Value::Int64(2), Value::Null],
        ]
    );

    db.execute("CREATE TABLE full_left (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE full_right (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO full_left VALUES (1, 'l1'), (2, 'l2')")
        .unwrap();
    db.execute("INSERT INTO full_right VALUES (2, 'r2'), (3, 'r3')")
        .unwrap();

    let using_full = db
        .execute("SELECT * FROM full_left FULL OUTER JOIN full_right USING (id) ORDER BY id")
        .unwrap();
    assert_eq!(
        using_full.columns(),
        &["id".to_string(), "name".to_string(), "label".to_string()]
    );
    assert_eq!(
        using_full
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Text("l1".into()), Value::Null],
            vec![
                Value::Int64(2),
                Value::Text("l2".into()),
                Value::Text("r2".into())
            ],
            vec![Value::Int64(3), Value::Null, Value::Text("r3".into())],
        ]
    );
}

#[test]
fn using_and_natural_joins_merge_output_columns_but_keep_qualified_access() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE TABLE using_left (
            id INT64 PRIMARY KEY,
            shared TEXT,
            left_only TEXT
        )",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE using_right (
            id INT64 PRIMARY KEY,
            shared TEXT,
            right_only TEXT
        )",
    )
    .unwrap();
    db.execute("INSERT INTO using_left VALUES (1, 'left-shared', 'l1'), (2, 'left-two', 'l2')")
        .unwrap();
    db.execute(
        "INSERT INTO using_right VALUES (1, 'right-shared', 'r1'), (3, 'right-three', 'r3')",
    )
    .unwrap();

    let using_star = db
        .execute("SELECT * FROM using_left JOIN using_right USING (id) ORDER BY id")
        .unwrap();
    assert_eq!(
        using_star.columns(),
        &[
            "id".to_string(),
            "shared".to_string(),
            "left_only".to_string(),
            "shared".to_string(),
            "right_only".to_string(),
        ]
    );
    assert_eq!(
        using_star
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![vec![
            Value::Int64(1),
            Value::Text("left-shared".into()),
            Value::Text("l1".into()),
            Value::Text("right-shared".into()),
            Value::Text("r1".into()),
        ]]
    );

    db.execute(
        "CREATE VIEW using_join_view AS
         SELECT * FROM using_left JOIN using_right USING (id)",
    )
    .unwrap();
    let using_view = db
        .execute("SELECT * FROM using_join_view ORDER BY id")
        .unwrap();
    assert_eq!(using_view.columns(), using_star.columns());
    assert_eq!(
        using_view
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        using_star
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>()
    );

    let qualified = db
        .execute(
            "SELECT using_left.id, using_right.id, id
             FROM using_left JOIN using_right USING (id)",
        )
        .unwrap();
    assert_eq!(
        qualified.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(1), Value::Int64(1)]
    );

    let table_wildcards = db
        .execute("SELECT using_left.*, using_right.* FROM using_left JOIN using_right USING (id)")
        .unwrap();
    assert_eq!(
        table_wildcards.columns(),
        &[
            "id".to_string(),
            "shared".to_string(),
            "left_only".to_string(),
            "id".to_string(),
            "shared".to_string(),
            "right_only".to_string(),
        ]
    );
    assert_eq!(
        table_wildcards.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("left-shared".into()),
            Value::Text("l1".into()),
            Value::Int64(1),
            Value::Text("right-shared".into()),
            Value::Text("r1".into()),
        ]
    );

    let ambiguous = db
        .execute("SELECT shared FROM using_left JOIN using_right USING (id)")
        .unwrap_err();
    assert!(
        ambiguous
            .to_string()
            .contains("ambiguous column reference shared"),
        "unexpected error: {ambiguous}"
    );

    db.execute(
        "CREATE TABLE natural_left (
            id INT64 PRIMARY KEY,
            shared TEXT,
            left_only TEXT
        )",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE natural_right (
            id INT64 PRIMARY KEY,
            shared TEXT,
            right_only TEXT
        )",
    )
    .unwrap();
    db.execute("INSERT INTO natural_left VALUES (1, 'same', 'l1'), (2, 'two', 'l2')")
        .unwrap();
    db.execute("INSERT INTO natural_right VALUES (1, 'same', 'r1')")
        .unwrap();

    let natural = db
        .execute("SELECT * FROM natural_left NATURAL LEFT JOIN natural_right ORDER BY id")
        .unwrap();
    assert_eq!(
        natural.columns(),
        &[
            "id".to_string(),
            "shared".to_string(),
            "left_only".to_string(),
            "right_only".to_string(),
        ]
    );
    assert_eq!(
        natural
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Int64(1),
                Value::Text("same".into()),
                Value::Text("l1".into()),
                Value::Text("r1".into()),
            ],
            vec![
                Value::Int64(2),
                Value::Text("two".into()),
                Value::Text("l2".into()),
                Value::Null,
            ],
        ]
    );

    db.execute("CREATE TABLE natural_cross_left (left_id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE natural_cross_right (right_id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO natural_cross_left VALUES (1), (2)")
        .unwrap();
    db.execute("INSERT INTO natural_cross_right VALUES (10)")
        .unwrap();

    let natural_cross = db
        .execute(
            "SELECT * FROM natural_cross_left NATURAL JOIN natural_cross_right ORDER BY left_id",
        )
        .unwrap();
    assert_eq!(
        natural_cross
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(2), Value::Int64(10)],
        ]
    );
}

#[test]
fn recursive_ctes_support_sequence_generation_and_tree_traversal() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let sequence = db
        .execute(
            "WITH RECURSIVE cnt(x) AS (
               SELECT 1
               UNION ALL
               SELECT x + 1 FROM cnt WHERE x < 10
             )
             SELECT x FROM cnt ORDER BY x",
        )
        .unwrap();
    assert_eq!(
        sequence
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        (1..=10)
            .map(|value| vec![Value::Int64(value)])
            .collect::<Vec<_>>()
    );

    db.execute("CREATE TABLE categories (id INT64 PRIMARY KEY, name TEXT, parent_id INT64)")
        .unwrap();
    db.execute(
        "INSERT INTO categories VALUES
            (1, 'root', NULL),
            (2, 'child_a', 1),
            (3, 'child_b', 1),
            (4, 'grandchild', 2)",
    )
    .unwrap();

    let descendants = db
        .execute(
            "WITH RECURSIVE descendants AS (
               SELECT id, name, parent_id FROM categories WHERE id = 1
               UNION ALL
               SELECT c.id, c.name, c.parent_id
               FROM categories AS c INNER JOIN descendants AS d ON c.parent_id = d.id
             )
             SELECT id, name, parent_id FROM descendants ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        descendants
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Text("root".into()), Value::Null,],
            vec![
                Value::Int64(2),
                Value::Text("child_a".into()),
                Value::Int64(1),
            ],
            vec![
                Value::Int64(3),
                Value::Text("child_b".into()),
                Value::Int64(1),
            ],
            vec![
                Value::Int64(4),
                Value::Text("grandchild".into()),
                Value::Int64(2),
            ],
        ]
    );
}

#[test]
fn recursive_ctes_enforce_iteration_limit_and_v0_recursive_term_guardrails() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let limit_err = db
        .execute(
            "WITH RECURSIVE cnt(x) AS (
               SELECT 1
               UNION ALL
               SELECT x + 1 FROM cnt
             )
             SELECT x FROM cnt",
        )
        .unwrap_err();
    assert!(
        limit_err
            .to_string()
            .contains("exceeded the 1000 iteration limit"),
        "unexpected error: {limit_err}"
    );

    let distinct_err = db
        .execute(
            "WITH RECURSIVE cnt(x) AS (
               SELECT 1
               UNION ALL
               SELECT DISTINCT x + 1 FROM cnt WHERE x < 3
             )
             SELECT x FROM cnt",
        )
        .unwrap_err();
    assert!(
        distinct_err
            .to_string()
            .contains("recursive term only supports non-distinct SELECT statements"),
        "unexpected error: {distinct_err}"
    );
}
