use decentdb::{Db, DbConfig, Value};

fn db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn values(db: &Db, sql: &str) -> Vec<Vec<Value>> {
    db.execute(sql)
        .unwrap()
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

#[test]
fn bun_generated_serial_defaults_and_bulk_returning_work() {
    let db = db();
    db.execute(
        "CREATE TABLE bun_items (id BIGSERIAL NOT NULL, name VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        metadata JSONB, receipt BYTEA, PRIMARY KEY(id))",
    )
    .unwrap();
    let rows = values(
        &db,
        "INSERT INTO bun_items AS item (id, name, created_at, metadata, receipt)
        VALUES (DEFAULT, 'one', DEFAULT, '{\"a\":1}', '\\x0001ff'),
               (DEFAULT, 'two', DEFAULT, NULL, '\\x')
        RETURNING item.id, item.created_at, item.metadata, item.receipt",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Int64(1));
    assert_eq!(rows[1][0], Value::Int64(2));
    assert!(matches!(rows[0][1], Value::TimestampTzMicros(_)));
    assert_eq!(rows[0][2], Value::Text("{\"a\":1}".into()));
    assert_eq!(rows[0][3], Value::Blob(vec![0, 1, 255]));
    assert_eq!(rows[1][3], Value::Blob(vec![]));
    for sql in [
        "SELECT CAST('\\x0' AS BYTEA)",
        "SELECT CAST('\\xgg' AS BYTEA)",
        "SELECT CAST('plain text' AS BYTEA)",
    ] {
        assert!(
            db.execute(sql).is_err(),
            "invalid BYTEA input accepted: {sql}"
        );
    }
    assert_eq!(
        values(
            &db,
            "INSERT INTO bun_items (id, name) VALUES (DEFAULT, 'three') RETURNING id"
        ),
        vec![vec![Value::Int64(3)]]
    );
    for sql in [
        "CREATE TABLE invalid (id SERIAL)",
        "CREATE TABLE invalid (id SERIAL, other INT, PRIMARY KEY(id, other))",
    ] {
        assert!(db
            .execute(sql)
            .unwrap_err()
            .to_string()
            .contains("SERIAL requires"));
    }
}

#[test]
fn defaults_are_column_contextual_and_null_is_explicit() {
    let db = db();
    db.execute("CREATE TABLE defaults (id SERIAL PRIMARY KEY, label TEXT DEFAULT 'from-default', required INT NOT NULL DEFAULT 42)").unwrap();
    assert_eq!(values(&db, "INSERT INTO defaults VALUES (DEFAULT, DEFAULT, DEFAULT), (DEFAULT, NULL, 1) RETURNING *"), vec![
        vec![Value::Int64(1), Value::Text("from-default".into()), Value::Int64(42)],
        vec![Value::Int64(2), Value::Null, Value::Int64(1)],
    ]);
    assert_eq!(values(&db, "UPDATE defaults AS d SET label = DEFAULT, required = DEFAULT WHERE d.id = 2 RETURNING d.*"),
        vec![vec![Value::Int64(2), Value::Text("from-default".into()), Value::Int64(42)]]);
    db.execute("CREATE TABLE bad_defaults(id INT PRIMARY KEY, payload JSONB DEFAULT CAST('invalid' AS JSONB))").unwrap();
    assert!(db
        .execute("INSERT INTO bad_defaults(id) VALUES(1)")
        .is_err());
    assert!(db
        .execute("INSERT INTO bad_defaults VALUES(1, DEFAULT)")
        .is_err());
    assert_eq!(
        values(&db, "SELECT COUNT(*) FROM bad_defaults"),
        vec![vec![Value::Int64(0)]]
    );
}

#[test]
fn dml_aliases_bind_correlations_and_respect_inner_shadowing() {
    let db = db();
    db.execute("CREATE TABLE balances(id INT PRIMARY KEY, n INT)")
        .unwrap();
    db.execute("CREATE TABLE other_balances(id INT PRIMARY KEY, n INT)")
        .unwrap();
    db.execute("INSERT INTO balances VALUES(1, 10), (2, 20)")
        .unwrap();
    db.execute("INSERT INTO other_balances VALUES(1, 100), (2, 200)")
        .unwrap();
    assert_eq!(values(&db, "UPDATE balances AS target SET n = target.n + 1 WHERE target.id = 1 RETURNING target.id, target.n, 'target.n'"),
        vec![vec![Value::Int64(1), Value::Int64(11), Value::Text("target.n".into())]]);
    assert_eq!(values(&db, "UPDATE balances AS target SET n = (SELECT MAX(b.n) FROM other_balances AS b WHERE b.id = target.id) WHERE target.id = 1 RETURNING target.n"),
        vec![vec![Value::Int64(100)]]);
    assert_eq!(values(&db, "UPDATE balances AS target SET n = (SELECT target.n FROM other_balances AS target WHERE target.id = 2) WHERE target.id = 1 RETURNING target.n"),
        vec![vec![Value::Int64(200)]]);
    assert_eq!(values(&db, "INSERT INTO balances AS target VALUES(1, 999) ON CONFLICT(id) DO UPDATE SET n = target.n + 1 RETURNING target.*"),
        vec![vec![Value::Int64(1), Value::Int64(201)]]);
    // A local alias matching the physical target name must not capture a
    // correlated reference to the independently named DML target.
    assert_eq!(values(&db, "UPDATE balances AS target SET n = (SELECT balances.n FROM other_balances AS balances WHERE balances.id = target.id) WHERE target.id = 1 RETURNING target.n"),
        vec![vec![Value::Int64(100)]]);
    assert_eq!(
        values(
            &db,
            "DELETE FROM balances AS target WHERE target.id = 2 RETURNING target.*"
        ),
        vec![vec![Value::Int64(2), Value::Int64(20)]]
    );
}

#[test]
fn json_declarations_validate_insert_update_and_casts() {
    let db = db();
    db.execute("CREATE TABLE documents(id INT PRIMARY KEY, body JSONB)")
        .unwrap();
    db.execute("INSERT INTO documents VALUES(1, '{\"n\":1}')")
        .unwrap();
    assert!(db
        .execute("INSERT INTO documents VALUES(2, 'invalid')")
        .is_err());
    assert!(db
        .execute("UPDATE documents AS d SET body = 'invalid' WHERE d.id = 1")
        .is_err());
    assert!(db.execute("SELECT CAST('invalid' AS JSONB)").is_err());
    assert_eq!(
        values(
            &db,
            "SELECT body ->> 'n', CAST(NULL AS JSON), CAST('[]' AS JSONB) FROM documents"
        ),
        vec![vec![
            Value::Text("1".into()),
            Value::Null,
            Value::Text("[]".into())
        ]]
    );
}

#[test]
fn decimal_orm_literals_are_exact_in_arithmetic_and_indexed_filters() {
    let db = db();
    db.execute("CREATE TABLE prices(id INT PRIMARY KEY, amount NUMERIC)")
        .unwrap();
    db.execute("INSERT INTO prices VALUES(1, '0.01'), (2, '0.1'), (3, '19.99')")
        .unwrap();
    for indexed in [false, true] {
        if indexed {
            db.execute("CREATE INDEX prices_amount ON prices(amount)")
                .unwrap();
        }
        assert_eq!(
            values(&db, "SELECT id FROM prices WHERE amount = '0.01'"),
            vec![vec![Value::Int64(1)]]
        );
        let prepared = db
            .prepare("SELECT id FROM prices WHERE amount = '0.01'")
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                prepared.execute(&[]).unwrap().rows()[0].values(),
                &[Value::Int64(1)]
            );
        }
        assert_eq!(
            values(&db, "SELECT id FROM prices WHERE amount < '0.1'"),
            vec![vec![Value::Int64(1)]]
        );
        assert_eq!(values(&db, "SELECT CAST('0.01' AS NUMERIC) < CAST('0.1' AS NUMERIC), CAST('0.1' AS NUMERIC) > '0.01'"), vec![vec![Value::Bool(true), Value::Bool(true)]]);
    }
    assert_eq!(
        values(
            &db,
            "UPDATE prices AS p SET amount = p.amount + '0.01' WHERE p.id = 3 RETURNING p.amount"
        ),
        vec![vec![Value::Decimal {
            scaled: 20,
            scale: 0
        }]]
    );
}

#[test]
fn decimal_reports_preserve_exact_sum_average_and_distinct_scales() {
    let db = db();
    db.execute("CREATE TABLE reports(id INT PRIMARY KEY, group_id INT, amount NUMERIC)")
        .unwrap();
    db.execute(
        "INSERT INTO reports VALUES (1, 1, '0.01'), (2, 1, '0.1'), (3, 1, '0.10'), (4, 2, NULL)",
    )
    .unwrap();
    let expected = vec![vec![
        Value::Decimal {
            scaled: 21,
            scale: 2,
        },
        Value::Decimal {
            scaled: 7,
            scale: 2,
        },
        Value::Decimal {
            scaled: 11,
            scale: 2,
        },
        Value::Decimal {
            scaled: 55,
            scale: 3,
        },
        Value::Int64(2),
    ]];
    for sql in [
        "SELECT SUM(amount), AVG(amount), SUM(DISTINCT amount), AVG(DISTINCT amount), COUNT(DISTINCT amount) FROM reports",
        "SELECT SUM(amount), AVG(amount), SUM(DISTINCT amount), AVG(DISTINCT amount), COUNT(DISTINCT amount) FROM reports GROUP BY group_id HAVING group_id = 1",
        "WITH r AS (SELECT amount FROM reports) SELECT SUM(amount), AVG(amount), SUM(DISTINCT amount), AVG(DISTINCT amount), COUNT(DISTINCT amount) FROM r",
    ] {
        assert_eq!(values(&db, sql), expected, "{sql}");
    }
    assert_eq!(values(&db, "SELECT SUM(amount), AVG(amount), TOTAL(amount), PG_TYPEOF(SUM(amount)), PG_TYPEOF(AVG(amount)) FROM reports WHERE group_id = 2"),
        vec![vec![Value::Null, Value::Null, Value::Float64(0.0), Value::Text("numeric".into()), Value::Text("numeric".into())]]);
    assert_eq!(
        values(
            &db,
            "SELECT SUM(amount) OVER (), AVG(amount) OVER () FROM reports ORDER BY id LIMIT 1"
        ),
        vec![vec![
            Value::Decimal {
                scaled: 21,
                scale: 2
            },
            Value::Decimal {
                scaled: 7,
                scale: 2
            }
        ]]
    );
    assert_eq!(
        values(&db, "SELECT SUM(amount) FROM reports WHERE group_id = 1"),
        vec![vec![Value::Decimal {
            scaled: 21,
            scale: 2
        }]]
    );

    db.execute("DELETE FROM reports").unwrap();
    db.execute(
        "INSERT INTO reports VALUES (1, 1, '9223372036854775807'), (2, 1, '9223372036854775807')",
    )
    .unwrap();
    assert_eq!(
        values(&db, "SELECT AVG(amount) FROM reports"),
        vec![vec![Value::Decimal {
            scaled: i64::MAX,
            scale: 0
        }]]
    );
    assert!(db
        .execute("SELECT SUM(amount) FROM reports")
        .unwrap_err()
        .to_string()
        .contains("overflow"));
    db.execute("DELETE FROM reports").unwrap();
    db.execute("INSERT INTO reports VALUES (1, 1, '-0.000001'), (2, 1, '0')")
        .unwrap();
    assert_eq!(
        values(&db, "SELECT AVG(amount) FROM reports"),
        vec![vec![Value::Decimal {
            scaled: -1,
            scale: 6
        }]]
    );
    let mixed = values(
        &db,
        "SELECT SUM(amount + CAST(0.5 AS FLOAT64)) FROM reports",
    );
    let Value::Float64(total) = mixed[0][0] else {
        panic!("FLOAT64 should promote SUM")
    };
    assert!((total - 0.999999).abs() < 1e-12);
}

#[test]
fn prepared_decimal_dml_filters_match_quoted_orm_values() {
    let db = db();
    db.execute("CREATE TABLE prices(id INT PRIMARY KEY, amount NUMERIC UNIQUE, n INT)")
        .unwrap();
    db.execute("INSERT INTO prices VALUES(1, '0.01', 0), (2, '0.1', 0)")
        .unwrap();
    assert_eq!(
        db.prepare("UPDATE prices AS p SET n = 1 WHERE p.amount = '0.01'")
            .unwrap()
            .execute(&[])
            .unwrap()
            .affected_rows(),
        1
    );
    assert_eq!(
        db.prepare("DELETE FROM prices AS p WHERE p.amount = '0.1'")
            .unwrap()
            .execute(&[])
            .unwrap()
            .affected_rows(),
        1
    );
    assert_eq!(
        values(&db, "SELECT id, n FROM prices"),
        vec![vec![Value::Int64(1), Value::Int64(1)]]
    );
}
