use decentdb::{Db, DbConfig, Value};

fn db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn values(db: &Db, sql: &str) -> Vec<Value> {
    db.execute(sql).unwrap().rows()[0].values().to_vec()
}

#[test]
fn issue_67_decimal_arithmetic_is_exact() {
    let db = db();
    assert_eq!(
        values(
            &db,
            "SELECT CAST(0.1 AS NUMERIC) + CAST(0.2 AS NUMERIC) = CAST(0.3 AS NUMERIC)"
        ),
        vec![Value::Bool(true)]
    );
    assert_eq!(
        values(
            &db,
            "SELECT
        CAST('1.20' AS NUMERIC) + CAST('0.003' AS NUMERIC),
        CAST('1.20' AS NUMERIC) - CAST('0.003' AS NUMERIC),
        CAST('1.20' AS NUMERIC) * CAST('0.003' AS NUMERIC),
        CAST('1' AS NUMERIC) / CAST('3' AS NUMERIC),
        CAST('-1' AS NUMERIC) / CAST('3' AS NUMERIC),
        CAST('5.5' AS NUMERIC) % CAST('2' AS NUMERIC)"
        ),
        vec![
            Value::Decimal {
                scaled: 1203,
                scale: 3
            },
            Value::Decimal {
                scaled: 1197,
                scale: 3
            },
            Value::Decimal {
                scaled: 36,
                scale: 4
            },
            Value::Decimal {
                scaled: 333333,
                scale: 6
            },
            Value::Decimal {
                scaled: -333333,
                scale: 6
            },
            Value::Decimal {
                scaled: 15,
                scale: 1
            },
        ]
    );
}

#[test]
fn decimal_mixed_operands_negation_and_rounding() {
    let db = db();
    assert_eq!(
        values(
            &db,
            "SELECT
        CAST('1.25' AS NUMERIC) + 2,
        2 - CAST('1.25' AS NUMERIC),
        -CAST('1.25' AS NUMERIC),
        CAST('1' AS NUMERIC) / CAST('128' AS NUMERIC),
        CAST('-1' AS NUMERIC) / CAST('128' AS NUMERIC),
        CAST('1.25' AS NUMERIC) + CAST(0.5 AS FLOAT64)"
        ),
        vec![
            Value::Decimal {
                scaled: 325,
                scale: 2
            },
            Value::Decimal {
                scaled: 75,
                scale: 2
            },
            Value::Decimal {
                scaled: -125,
                scale: 2
            },
            Value::Decimal {
                scaled: 7813,
                scale: 6
            },
            Value::Decimal {
                scaled: -7813,
                scale: 6
            },
            Value::Float64(1.75),
        ]
    );
    assert_eq!(values(&db, "SELECT CAST('1.25' AS NUMERIC) + NULL, CAST('1' AS NUMERIC) / 0, CAST('1' AS NUMERIC) % 0"), vec![Value::Null; 3]);
}

#[test]
fn decimal_overflow_is_a_sql_error() {
    let db = db();
    for sql in [
        "SELECT CAST('9223372036854775807' AS NUMERIC) + 1",
        "SELECT CAST('9223372036854775807' AS NUMERIC) * 2",
        "SELECT -CAST('-9223372036854775808' AS NUMERIC)",
    ] {
        assert!(db
            .execute(sql)
            .unwrap_err()
            .to_string()
            .contains("DECIMAL arithmetic overflow"));
    }
    // Normalize before narrowing the coefficient: the product's intermediate
    // exceeds i64, while the exact normalized result fits.
    assert_eq!(
        values(
            &db,
            "SELECT CAST('1000000000.1' AS NUMERIC) * CAST('1000000000' AS NUMERIC)"
        ),
        vec![Value::Decimal {
            scaled: 1_000_000_000_100_000_000,
            scale: 0
        }]
    );
}

#[test]
fn decimal_arithmetic_in_grouped_queries_and_updates() {
    let db = db();
    db.execute("CREATE TABLE amounts (id INT PRIMARY KEY, amount NUMERIC)")
        .unwrap();
    db.execute("INSERT INTO amounts VALUES (1, '1.25'), (2, '2.50')")
        .unwrap();
    db.execute("UPDATE amounts SET amount = amount + CAST('0.10' AS NUMERIC)")
        .unwrap();
    assert_eq!(
        values(
            &db,
            "SELECT -MIN(amount), MAX(amount) - MIN(amount) FROM amounts"
        ),
        vec![
            Value::Decimal {
                scaled: -135,
                scale: 2
            },
            Value::Decimal {
                scaled: 125,
                scale: 2
            }
        ]
    );
}

#[test]
fn issue_62_cast_stays_fixed_and_timestamp_epoch_has_timezone() {
    let db = db();
    assert_eq!(values(&db, "SELECT CAST(CAST(10.2 AS DECIMAL(10,2)) AS TEXT), TO_TIMESTAMP(1711929600123456 / 1000000.0)"), vec![Value::Text("10.2".to_string()), Value::TimestampTzMicros(1_711_929_600_123_456)]);
    assert_eq!(values(&db, "SELECT TO_TIMESTAMP(CAST('1711929600.123456' AS NUMERIC)), TO_TIMESTAMP(CAST('-0.0000005' AS NUMERIC))"), vec![Value::TimestampTzMicros(1_711_929_600_123_456), Value::TimestampTzMicros(-1)]);
    for parameter in [
        Value::Float64(f64::NAN),
        Value::Float64(f64::INFINITY),
        Value::Float64(1e100),
        Value::Int64(i64::MAX),
    ] {
        assert!(db
            .execute_with_params("SELECT TO_TIMESTAMP($1)", &[parameter])
            .unwrap_err()
            .to_string()
            .contains("out of range"));
    }
}

#[test]
fn to_timestamp_composes_with_existing_temporal_operations() {
    let db = db();
    assert_eq!(
        values(
            &db,
            "SELECT TO_TIMESTAMP(0) + 1000000, TO_TIMESTAMP(0) - 1000000,
            1000000 + TO_TIMESTAMP(0), TO_TIMESTAMP(0) + '1 day',
            TO_TIMESTAMP(0) - '1 day', '1 day' + TO_TIMESTAMP(0),
            TO_TIMESTAMP(0) + INTERVAL '1 second',
            INTERVAL '1 second' + TO_TIMESTAMP(0),
            TO_TIMESTAMP(0) - INTERVAL '1 second'"
        ),
        [
            1_000_000,
            -1_000_000,
            1_000_000,
            86_400_000_000,
            -86_400_000_000,
            86_400_000_000,
            1_000_000,
            1_000_000,
            -1_000_000
        ]
        .map(Value::TimestampTzMicros)
    );
    assert_eq!(
        values(
            &db,
            "SELECT TO_TIMESTAMP(0) = CAST('1970-01-01' AS TIMESTAMP),
        CAST('1970-01-01' AS TIMESTAMP) < TO_TIMESTAMP(1)"
        ),
        vec![Value::Bool(true); 2]
    );
    db.execute("CREATE TABLE stamps (stamp TIMESTAMP)").unwrap();
    db.execute("CREATE INDEX stamps_idx ON stamps(stamp)")
        .unwrap();
    db.execute("INSERT INTO stamps VALUES ('1970-01-01'), ('1970-01-02')")
        .unwrap();
    for sql in [
        "SELECT COUNT(*) FROM stamps WHERE stamp = TO_TIMESTAMP(0)",
        "SELECT COUNT(*) FROM stamps WHERE stamp >= TO_TIMESTAMP(0) AND stamp < TO_TIMESTAMP(1)",
    ] {
        assert_eq!(values(&db, sql), vec![Value::Int64(1)]);
    }
    for sql in [
        "SELECT value FROM GENERATE_SERIES(TO_TIMESTAMP(0), TO_TIMESTAMP(2), INTERVAL '1 second')",
        "SELECT value FROM GENERATE_SERIES(CAST('1970-01-01' AS TIMESTAMP), TO_TIMESTAMP(2), INTERVAL '1 second')",
        "SELECT value FROM GENERATE_SERIES(TO_TIMESTAMP(0), CAST('1970-01-01 00:00:02' AS TIMESTAMP), INTERVAL '1 second')",
    ] {
        let result = db.execute(sql).unwrap();
        let series: Vec<_> = result.rows().iter().map(|row| row.values()[0].clone()).collect();
        assert_eq!(series, [0, 1_000_000, 2_000_000].map(Value::TimestampTzMicros));
    }
    assert!(db
        .execute("SELECT * FROM GENERATE_SERIES(TO_TIMESTAMP(0), TO_TIMESTAMP(2))")
        .unwrap_err()
        .to_string()
        .contains("requires an INTERVAL"));
}

#[test]
fn issue_65_pg_typeof_reports_native_sql_types() {
    let db = db();
    assert_eq!(
        values(
            &db,
            "SELECT
        PG_TYPEOF(TO_TIMESTAMP(1711929600123456 / 1000000.0)),
        pg_catalog.pg_typeof(CAST('0.1' AS NUMERIC)),
        PG_TYPEOF(CAST(NULL AS NUMERIC)),
        PG_TYPEOF(CAST(NULL AS TIMESTAMP)),
        PG_TYPEOF(CAST(NULL AS TIMESTAMPTZ)),
        PG_TYPEOF(NULL), PG_TYPEOF(1), PG_TYPEOF('text'), PG_TYPEOF(TRUE)"
        ),
        [
            "timestamp with time zone",
            "numeric",
            "numeric",
            "timestamp without time zone",
            "timestamp with time zone",
            "unknown",
            "bigint",
            "text",
            "boolean"
        ]
        .map(|value| Value::Text(value.to_string()))
    );
    for sql in ["SELECT PG_TYPEOF()", "SELECT PG_TYPEOF(1, 2)"] {
        assert!(db
            .execute(sql)
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }
}

#[test]
fn pg_typeof_preserves_nullable_column_and_parameter_types() {
    let db = db();
    db.execute("CREATE TABLE typed (amount NUMERIC, stamp TIMESTAMPTZ)")
        .unwrap();
    db.execute("INSERT INTO typed VALUES (NULL, NULL)").unwrap();
    for sql in [
        "SELECT PG_TYPEOF(MIN(t.amount)), PG_TYPEOF(SUM(t.amount)) FROM typed AS t HAVING PG_TYPEOF(MIN(t.amount)) = 'numeric'",
        "SELECT PG_TYPEOF(MIN(amount)), PG_TYPEOF(SUM(amount)) FROM typed WHERE 1 = 0",
    ] {
        assert_eq!(values(&db, sql), vec![Value::Text("numeric".into()), Value::Text("numeric".into())]);
    }
    assert_eq!(
        values(
            &db,
            "SELECT PG_TYPEOF(MIN(amount)), PG_TYPEOF(SUM(amount)) FROM typed"
        ),
        vec![Value::Text("numeric".into()), Value::Text("numeric".into())]
    );
    assert_eq!(
        values(
            &db,
            "SELECT PG_TYPEOF(NULL || 'text'), PG_TYPEOF(NOT CAST(NULL AS BOOL))"
        ),
        vec![Value::Text("text".into()), Value::Text("boolean".into())]
    );
    assert_eq!(
        values(
            &db,
            "SELECT PG_TYPEOF(t.amount), PG_TYPEOF(t.stamp) FROM typed AS t"
        ),
        vec![
            Value::Text("numeric".into()),
            Value::Text("timestamp with time zone".into())
        ]
    );
    assert_eq!(
        db.execute_with_params(
            "SELECT PG_TYPEOF($1), PG_TYPEOF(CAST($2 AS NUMERIC))",
            &[Value::Float64(1.25), Value::Null]
        )
        .unwrap()
        .rows()[0]
            .values(),
        &[
            Value::Text("double precision".into()),
            Value::Text("numeric".into())
        ]
    );
}

#[test]
fn pg_typeof_uses_resolved_schema_for_null_columns() {
    let db = db();
    db.execute("CREATE TABLE typed (amount NUMERIC)").unwrap();
    db.execute("CREATE TEMP TABLE typed (amount TEXT)").unwrap();
    db.execute("INSERT INTO main.typed VALUES (NULL)").unwrap();
    db.execute("INSERT INTO temp.typed VALUES (NULL)").unwrap();
    for sql in [
        "SELECT PG_TYPEOF(amount) FROM main.typed",
        "SELECT PG_TYPEOF(t.amount) FROM main.typed AS t",
        "SELECT PG_TYPEOF(MIN(t.amount)) FROM main.typed AS t",
        "SELECT PG_TYPEOF(t.amount) FROM (SELECT amount FROM main.typed) AS t",
        "SELECT PG_TYPEOF(t.amount) FROM (SELECT * FROM main.typed) AS t",
        "SELECT PG_TYPEOF(t.amount) FROM (SELECT MIN(amount) AS amount FROM main.typed) AS t",
        "SELECT PG_TYPEOF(t.amount) FROM (SELECT MIN(CAST(NULL AS NUMERIC)) AS amount) AS t",
        "SELECT PG_TYPEOF(v.amount) FROM (VALUES (CAST(NULL AS NUMERIC))) AS v(amount)",
        "SELECT PG_TYPEOF(m.amount) FROM main.typed AS m JOIN temp.typed AS t ON 1 = 1",
    ] {
        assert_eq!(
            values(&db, sql),
            vec![Value::Text("numeric".into())],
            "{sql}"
        );
    }
    for sql in [
        "SELECT PG_TYPEOF(amount) FROM typed",
        "SELECT PG_TYPEOF(amount) FROM temp.typed",
    ] {
        assert_eq!(values(&db, sql), vec![Value::Text("text".into())], "{sql}");
    }
}

#[test]
fn mixed_timestamps_use_the_same_sql_equality_keys() {
    let db = db();
    db.execute("CREATE TABLE stamps (id INT, stamp TIMESTAMP)")
        .unwrap();
    db.execute("CREATE TABLE epochs (id INT, stamp TIMESTAMPTZ)")
        .unwrap();
    db.execute("INSERT INTO stamps VALUES (1, '1970-01-01')")
        .unwrap();
    db.execute("INSERT INTO epochs VALUES (1, TO_TIMESTAMP(0))")
        .unwrap();
    for indexed in [false, true] {
        if indexed {
            db.execute("CREATE INDEX stamps_idx ON stamps(id, stamp)")
                .unwrap();
            db.execute("CREATE INDEX epochs_idx ON epochs(id, stamp)")
                .unwrap();
            db.execute("CREATE INDEX stamps_time_idx ON stamps(stamp)")
                .unwrap();
            db.execute("CREATE INDEX epochs_time_idx ON epochs(stamp)")
                .unwrap();
        }
        for sql in [
            "WITH e AS (SELECT TO_TIMESTAMP(0) AS stamp) SELECT COUNT(*) FROM stamps AS s JOIN e ON s.stamp = e.stamp",
            "WITH e AS (SELECT TO_TIMESTAMP(0) AS stamp) SELECT COUNT(*) FROM e JOIN stamps AS s ON e.stamp = s.stamp",
            "SELECT COUNT(*) FROM stamps AS s JOIN epochs AS e ON s.stamp = e.stamp",
            "SELECT COUNT(*) FROM epochs AS e JOIN stamps AS s ON s.stamp = e.stamp",
            "SELECT COUNT(*) FROM stamps AS s JOIN epochs AS e ON s.id = e.id AND s.stamp = e.stamp WHERE s.id = 1",
            "SELECT COUNT(*) FROM epochs AS e JOIN stamps AS s ON s.id = e.id AND s.stamp = e.stamp WHERE e.id = 1",
            "SELECT COUNT(*) FROM stamps AS s LEFT JOIN epochs AS e ON s.stamp = e.stamp WHERE e.id IS NOT NULL",
            "SELECT COUNT(*) FROM stamps AS s JOIN epochs AS e ON s.stamp = e.stamp GROUP BY s.id",
        ] {
            assert_eq!(values(&db, sql), vec![Value::Int64(1)], "indexed={indexed}: {sql}");
        }
    }
    for sql in [
        "SELECT COUNT(*) FROM (SELECT stamp FROM stamps UNION SELECT stamp FROM epochs) AS t",
        "SELECT COUNT(DISTINCT stamp) FROM (SELECT stamp FROM stamps UNION ALL SELECT stamp FROM epochs) AS t",
    ] {
        assert_eq!(values(&db, sql), vec![Value::Int64(1)], "{sql}");
    }
    assert_eq!(values(&db, "SELECT COUNT(*) FROM (SELECT stamp FROM stamps UNION ALL SELECT stamp FROM epochs) AS t GROUP BY stamp"), vec![Value::Int64(2)]);
}

#[test]
fn opposite_timestamp_tags_find_indexed_rows_in_select_and_dml() {
    for (column_type, parameter) in [
        ("TIMESTAMP", Value::TimestampTzMicros(0)),
        ("TIMESTAMPTZ", Value::TimestampMicros(0)),
    ] {
        let db = db();
        db.execute(&format!(
            "CREATE TABLE stamps (stamp {column_type} UNIQUE, n INT)"
        ))
        .unwrap();
        db.execute("INSERT INTO stamps VALUES (TO_TIMESTAMP(0), 1)")
            .unwrap();
        let params = [parameter];
        assert_eq!(
            db.execute_with_params("SELECT n FROM stamps WHERE stamp = $1", &params)
                .unwrap()
                .rows()[0]
                .values(),
            &[Value::Int64(1)]
        );
        assert_eq!(
            db.execute_with_params("UPDATE stamps SET n = 2 WHERE stamp = $1", &params)
                .unwrap()
                .affected_rows(),
            1
        );
        assert_eq!(
            db.prepare("UPDATE stamps SET n = 3 WHERE stamp = $1")
                .unwrap()
                .execute(&params)
                .unwrap()
                .affected_rows(),
            1
        );
        assert_eq!(values(&db, "SELECT n FROM stamps"), vec![Value::Int64(3)]);
        assert_eq!(
            db.prepare("DELETE FROM stamps WHERE stamp = $1")
                .unwrap()
                .execute(&params)
                .unwrap()
                .affected_rows(),
            1
        );
        db.execute("INSERT INTO stamps VALUES (TO_TIMESTAMP(0), 1)")
            .unwrap();
        assert_eq!(
            db.execute_with_params("DELETE FROM stamps WHERE stamp = $1", &params)
                .unwrap()
                .affected_rows(),
            1
        );
    }
}

#[test]
fn pg_typeof_null_temporal_arithmetic_matches_non_null_results() {
    let db = db();
    for (null_expr, value_expr, expected) in [
        (
            "CAST(NULL AS DATE) + INTERVAL '1 day'",
            "CAST('1970-01-01' AS DATE) + INTERVAL '1 day'",
            "timestamp without time zone",
        ),
        (
            "CAST(NULL AS DATE) - INTERVAL '1 day'",
            "CAST('1970-01-01' AS DATE) - INTERVAL '1 day'",
            "timestamp without time zone",
        ),
        (
            "INTERVAL '1 day' + CAST(NULL AS DATE)",
            "INTERVAL '1 day' + CAST('1970-01-01' AS DATE)",
            "timestamp without time zone",
        ),
        (
            "1000000 + CAST(NULL AS TIMESTAMPTZ)",
            "1000000 + TO_TIMESTAMP(0)",
            "timestamp with time zone",
        ),
        (
            "'1 day' + CAST(NULL AS TIMESTAMPTZ)",
            "'1 day' + TO_TIMESTAMP(0)",
            "timestamp with time zone",
        ),
    ] {
        assert_eq!(
            values(
                &db,
                &format!("SELECT PG_TYPEOF({null_expr}), PG_TYPEOF({value_expr})")
            ),
            vec![Value::Text(expected.into()); 2]
        );
    }
}
