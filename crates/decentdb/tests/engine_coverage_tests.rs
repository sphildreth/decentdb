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
fn joins_cover_empty_side_and_unsupported_remaining_join_forms() {
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

    let natural_err = db
        .execute("SELECT * FROM left_populated NATURAL JOIN right_empty")
        .unwrap_err();
    assert!(
        natural_err
            .to_string()
            .contains("NATURAL JOIN is not supported yet"),
        "unexpected error: {natural_err}"
    );

    let using_err = db
        .execute("SELECT * FROM left_populated JOIN right_empty USING (id)")
        .unwrap_err();
    assert!(
        using_err
            .to_string()
            .contains("JOIN ... USING (...) is not supported yet"),
        "unexpected error: {using_err}"
    );
}
