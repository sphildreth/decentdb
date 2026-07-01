//! CRM-shaped correctness regressions for planned performance work.
//!
//! These tests intentionally avoid asserting specific plan nodes or timing.
//! They pin observable behavior for the safe Phase 4/5/6/8 scopes in
//! `design/2026-06-30_PERF_PLAN.md`.

use decentdb::{Db, DbConfig, QueryResult, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

fn rows(result: &QueryResult) -> Vec<Vec<Value>> {
    result
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn assert_float_close(value: &Value, expected: f64) {
    match value {
        Value::Float64(actual) => assert!(
            (actual - expected).abs() < 0.000_001,
            "expected {expected}, got {actual}"
        ),
        other => panic!("expected FLOAT64 {expected}, got {other:?}"),
    }
}

fn setup_p5_selectivity_update_dataset(db: &Db) {
    exec(
        db,
        "CREATE TABLE crm_p5_selectivity_invoices (
            id INT64 PRIMARY KEY,
            company_id INT64 NOT NULL,
            total FLOAT64 NOT NULL,
            paid BOOL NOT NULL
        )",
    );
    exec(
        db,
        "CREATE INDEX crm_p5_selectivity_paid_total_idx
         ON crm_p5_selectivity_invoices(paid, total)",
    );

    let insert = db
        .prepare(
            "INSERT INTO crm_p5_selectivity_invoices (id, company_id, total, paid)
             VALUES ($1, $2, $3, $4)",
        )
        .unwrap();
    for id in 1_i64..=100_i64 {
        insert
            .execute(&[
                Value::Int64(id),
                Value::Int64(10),
                Value::Float64(id as f64),
                Value::Bool(false),
            ])
            .unwrap();
    }
}

#[test]
fn crm_invoice_item_generated_stored_insert_reuses_prepared_statement() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE crm_p4_invoices (
            id INT64 PRIMARY KEY,
            company_id INT64 NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE TABLE crm_p4_invoice_items (
            id INT64 PRIMARY KEY,
            invoice_id INT64 NOT NULL REFERENCES crm_p4_invoices(id),
            sku TEXT NOT NULL,
            quantity INT64 NOT NULL,
            unit_price FLOAT64 NOT NULL,
            line_total FLOAT64 GENERATED ALWAYS AS (quantity * unit_price) STORED
        )",
    );
    exec(
        &db,
        "CREATE INDEX crm_p4_items_invoice_idx ON crm_p4_invoice_items(invoice_id)",
    );
    exec(&db, "INSERT INTO crm_p4_invoices VALUES (10, 1), (11, 1)");

    let insert_item = db
        .prepare(
            "INSERT INTO crm_p4_invoice_items
             (id, invoice_id, sku, quantity, unit_price)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .unwrap();
    for (id, invoice_id, sku, quantity, unit_price) in [
        (100, 10, "setup", 2, 19.50),
        (101, 10, "seat", 5, 7.25),
        (102, 11, "support", 1, 99.00),
    ] {
        insert_item
            .execute(&[
                Value::Int64(id),
                Value::Int64(invoice_id),
                Value::Text(sku.to_string()),
                Value::Int64(quantity),
                Value::Float64(unit_price),
            ])
            .unwrap();
    }

    let stored_totals = exec(
        &db,
        "SELECT id, line_total FROM crm_p4_invoice_items ORDER BY id",
    );
    let stored_rows = rows(&stored_totals);
    assert_eq!(stored_rows[0][0], Value::Int64(100));
    assert_float_close(&stored_rows[0][1], 39.0);
    assert_eq!(stored_rows[1][0], Value::Int64(101));
    assert_float_close(&stored_rows[1][1], 36.25);
    assert_eq!(stored_rows[2][0], Value::Int64(102));
    assert_float_close(&stored_rows[2][1], 99.0);

    let explicit_generated = db
        .execute(
            "INSERT INTO crm_p4_invoice_items
             (id, invoice_id, sku, quantity, unit_price, line_total)
             VALUES (103, 11, 'bad', 1, 1.0, 1.0)",
        )
        .unwrap_err()
        .to_string();
    assert!(
        explicit_generated.contains("cannot INSERT into generated column"),
        "unexpected error: {explicit_generated}"
    );
}

#[test]
fn crm_invoice_prepared_insert_maintains_partial_covering_index() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE crm_p4_batch_users (
            id INT64 PRIMARY KEY,
            full_name TEXT NOT NULL,
            email TEXT NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE TABLE crm_p4_batch_invoices (
            id INT64 PRIMARY KEY,
            user_id INT64 NOT NULL REFERENCES crm_p4_batch_users(id),
            invoice_number TEXT UNIQUE NOT NULL,
            due_at TEXT NOT NULL,
            total FLOAT64 NOT NULL,
            paid BOOL NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE INDEX crm_p4_batch_unpaid_due_idx
         ON crm_p4_batch_invoices(due_at)
         INCLUDE (user_id, invoice_number, total)
         WHERE paid = FALSE",
    );
    exec(
        &db,
        "INSERT INTO crm_p4_batch_users VALUES
            (1, 'Ada Lovelace', 'ada@example.com'),
            (2, 'Grace Hopper', 'grace@example.com')",
    );
    exec(
        &db,
        "CREATE VIEW crm_p4_batch_unpaid AS
         SELECT i.id, i.invoice_number, u.full_name, u.email, i.total, i.due_at
         FROM crm_p4_batch_invoices AS i
         JOIN crm_p4_batch_users AS u ON u.id = i.user_id
         WHERE i.paid = FALSE",
    );

    let insert_invoice = db
        .prepare(
            "INSERT INTO crm_p4_batch_invoices
             (id, user_id, invoice_number, due_at, total, paid)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .unwrap();
    for (id, user_id, number, due_at, total, paid) in [
        (1, 1, "INV-001", "2026-07-01", 10.0, false),
        (2, 1, "INV-002", "2026-08-01", 20.0, true),
        (3, 2, "INV-003", "2026-07-10", 30.0, false),
        (4, 2, "INV-004", "2026-07-20", 40.0, false),
    ] {
        insert_invoice
            .execute(&[
                Value::Int64(id),
                Value::Int64(user_id),
                Value::Text(number.to_string()),
                Value::Text(due_at.to_string()),
                Value::Float64(total),
                Value::Bool(paid),
            ])
            .unwrap();
    }

    let newest = exec(
        &db,
        "SELECT invoice_number
         FROM crm_p4_batch_unpaid
         ORDER BY due_at DESC
         LIMIT 3",
    );
    assert_eq!(
        rows(&newest)
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Text("INV-004".to_string()),
            Value::Text("INV-003".to_string()),
            Value::Text("INV-001".to_string()),
        ]
    );
}

#[test]
fn crm_indexed_paid_total_update_is_correct_for_repeat_no_rows_and_rollback() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE crm_p5_invoices (
            id INT64 PRIMARY KEY,
            company_id INT64 NOT NULL,
            total FLOAT64 NOT NULL,
            paid BOOL NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE INDEX crm_p5_paid_total_idx ON crm_p5_invoices(paid, total)",
    );
    exec(
        &db,
        "INSERT INTO crm_p5_invoices VALUES
            (1, 10, 25.00, FALSE),
            (2, 10, 75.00, FALSE),
            (3, 20, 125.00, FALSE),
            (4, 20, 45.00, TRUE),
            (5, 30, 60.00, FALSE)",
    );

    let select_candidates = db
        .prepare(
            "SELECT id
             FROM crm_p5_invoices
             WHERE paid = FALSE AND total < $1
             ORDER BY id",
        )
        .unwrap();
    let pre_update = rows(&select_candidates.execute(&[Value::Float64(100.0)]).unwrap());
    assert_eq!(
        pre_update,
        vec![
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Int64(5)],
        ]
    );

    let mark_paid = db
        .prepare("UPDATE crm_p5_invoices SET paid = TRUE WHERE paid = FALSE AND total < $1")
        .unwrap();
    let first = mark_paid.execute(&[Value::Float64(100.0)]).unwrap();
    assert_eq!(first.affected_rows(), pre_update.len() as u64);

    let second = mark_paid.execute(&[Value::Float64(100.0)]).unwrap();
    assert_eq!(second.affected_rows(), 0);

    let post_update = rows(&select_candidates.execute(&[Value::Float64(100.0)]).unwrap());
    assert!(post_update.is_empty());

    let count_paid_under_limit = db
        .prepare(
            "SELECT COUNT(*)
             FROM crm_p5_invoices AS i
             WHERE i.paid = TRUE AND i.total < $1",
        )
        .unwrap();
    let count_result = count_paid_under_limit
        .execute(&[Value::Float64(100.0)])
        .unwrap();
    assert_eq!(rows(&count_result), vec![vec![Value::Int64(4)]]);

    let paid_state = exec(&db, "SELECT id, paid FROM crm_p5_invoices ORDER BY id");
    assert_eq!(
        rows(&paid_state),
        vec![
            vec![Value::Int64(1), Value::Bool(true)],
            vec![Value::Int64(2), Value::Bool(true)],
            vec![Value::Int64(3), Value::Bool(false)],
            vec![Value::Int64(4), Value::Bool(true)],
            vec![Value::Int64(5), Value::Bool(true)],
        ]
    );

    exec(&db, "BEGIN");
    let rollback_update = mark_paid.execute(&[Value::Float64(200.0)]).unwrap();
    assert_eq!(rollback_update.affected_rows(), 1);
    exec(&db, "ROLLBACK");

    let unpaid_after_rollback = exec(
        &db,
        "SELECT id FROM crm_p5_invoices WHERE paid = FALSE ORDER BY id",
    );
    assert_eq!(rows(&unpaid_after_rollback), vec![vec![Value::Int64(3)]]);
}

#[test]
fn crm_indexed_paid_total_update_rechecks_residual_predicates() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE crm_p5_residual_invoices (
            id INT64 PRIMARY KEY,
            company_id INT64 NOT NULL,
            total FLOAT64 NOT NULL,
            paid BOOL NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE INDEX crm_p5_residual_paid_total_idx
         ON crm_p5_residual_invoices(paid, total)",
    );
    exec(
        &db,
        "INSERT INTO crm_p5_residual_invoices VALUES
            (1, 10, 25.00, FALSE),
            (2, 10, 75.00, FALSE),
            (3, 20, 50.00, FALSE),
            (4, 20, 125.00, FALSE),
            (5, 10, 45.00, TRUE)",
    );

    let update = db
        .prepare(
            "UPDATE crm_p5_residual_invoices
             SET paid = TRUE
             WHERE paid = FALSE AND total < $1 AND company_id = $2",
        )
        .unwrap();
    let result = update
        .execute(&[Value::Float64(100.0), Value::Int64(10)])
        .unwrap();
    assert_eq!(result.affected_rows(), 2);

    let paid_state = exec(
        &db,
        "SELECT id, paid FROM crm_p5_residual_invoices ORDER BY id",
    );
    assert_eq!(
        rows(&paid_state),
        vec![
            vec![Value::Int64(1), Value::Bool(true)],
            vec![Value::Int64(2), Value::Bool(true)],
            vec![Value::Int64(3), Value::Bool(false)],
            vec![Value::Int64(4), Value::Bool(false)],
            vec![Value::Int64(5), Value::Bool(true)],
        ]
    );
}

#[test]
fn crm_indexed_paid_total_update_selectivity_matrix() {
    let cases = [
        (1.0, 0_usize),
        (2.0, 1_usize),
        (11.0, 10_usize),
        (51.0, 50_usize),
    ];

    for &(max_total, expected_count) in &cases {
        let db = mem_db();
        setup_p5_selectivity_update_dataset(&db);

        let select_candidates = db
            .prepare(
                "SELECT id
                 FROM crm_p5_selectivity_invoices
                 WHERE paid = FALSE AND total < $1
                 ORDER BY id",
            )
            .unwrap();
        let update = db
            .prepare(
                "UPDATE crm_p5_selectivity_invoices
                 SET paid = TRUE
                 WHERE paid = FALSE AND total < $1",
            )
            .unwrap();

        let pre_update = rows(
            &select_candidates
                .execute(&[Value::Float64(max_total)])
                .unwrap(),
        );
        let expected_ids: Vec<Vec<Value>> = (1_i64..=(expected_count as i64))
            .map(|id| vec![Value::Int64(id)])
            .collect();
        assert_eq!(
            pre_update, expected_ids,
            "select baseline should match configured selectivity for total < {max_total}"
        );

        let first = update.execute(&[Value::Float64(max_total)]).unwrap();
        assert_eq!(
            first.affected_rows(),
            expected_count as u64,
            "first update should affect configured row count for total < {max_total}"
        );

        let second = update.execute(&[Value::Float64(max_total)]).unwrap();
        assert_eq!(
            second.affected_rows(),
            0,
            "repeat update should affect zero rows for total < {max_total}"
        );
    }
}

#[test]
fn crm_indexed_paid_total_delete_rechecks_residual_predicates() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE crm_p5_residual_delete_invoices (
            id INT64 PRIMARY KEY,
            company_id INT64 NOT NULL,
            total FLOAT64 NOT NULL,
            paid BOOL NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE INDEX crm_p5_residual_delete_paid_total_idx
         ON crm_p5_residual_delete_invoices(paid, total)",
    );
    exec(
        &db,
        "INSERT INTO crm_p5_residual_delete_invoices VALUES
            (1, 10, 25.00, FALSE),
            (2, 10, 75.00, FALSE),
            (3, 20, 50.00, FALSE),
            (4, 20, 125.00, FALSE),
            (5, 10, 45.00, TRUE)",
    );

    let delete = db
        .prepare(
            "DELETE FROM crm_p5_residual_delete_invoices
             WHERE paid = FALSE AND total < $1 AND company_id = $2",
        )
        .unwrap();
    let result = delete
        .execute(&[Value::Float64(100.0), Value::Int64(10)])
        .unwrap();
    assert_eq!(result.affected_rows(), 2);

    let remaining = exec(
        &db,
        "SELECT id FROM crm_p5_residual_delete_invoices ORDER BY id",
    );
    assert_eq!(
        rows(&remaining),
        vec![
            vec![Value::Int64(3)],
            vec![Value::Int64(4)],
            vec![Value::Int64(5)],
        ]
    );
}

#[test]
fn crm_prepared_insert_with_virtual_generated_not_null_validates_materialized_value() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE crm_p4_virtual_generated (
            id INT64 PRIMARY KEY,
            quantity INT64 NOT NULL,
            doubled INT64 GENERATED ALWAYS AS (quantity * 2) VIRTUAL NOT NULL
        )",
    );

    let insert = db
        .prepare("INSERT INTO crm_p4_virtual_generated (id, quantity) VALUES ($1, $2)")
        .unwrap();
    let inserted = insert
        .execute(&[Value::Int64(1), Value::Int64(21)])
        .unwrap();
    assert_eq!(inserted.affected_rows(), 1);

    let projected = exec(
        &db,
        "SELECT id, quantity, doubled FROM crm_p4_virtual_generated",
    );
    assert_eq!(
        rows(&projected),
        vec![vec![Value::Int64(1), Value::Int64(21), Value::Int64(42),]]
    );
}

#[test]
fn crm_unpaid_invoices_view_ordered_limit_returns_earliest_unpaid_rows() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE crm_p6_companies (
            id INT64 PRIMARY KEY,
            name TEXT NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE TABLE crm_p6_invoices (
            id INT64 PRIMARY KEY,
            company_id INT64 NOT NULL REFERENCES crm_p6_companies(id),
            due_at TEXT NOT NULL,
            total FLOAT64 NOT NULL,
            paid BOOL NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE INDEX crm_p6_unpaid_due_idx
         ON crm_p6_invoices(due_at)
         WHERE paid = FALSE",
    );
    exec(
        &db,
        "INSERT INTO crm_p6_companies VALUES (1, 'Acme'), (2, 'Globex')",
    );
    exec(
        &db,
        "INSERT INTO crm_p6_invoices VALUES
            (1, 1, '2026-07-10', 50.00, FALSE),
            (2, 1, '2026-07-01', 10.00, TRUE),
            (3, 2, '2026-07-03', 30.00, FALSE),
            (4, 2, '2026-07-02', 20.00, FALSE),
            (5, 1, '2026-07-04', 40.00, FALSE)",
    );
    exec(
        &db,
        "CREATE VIEW v_unpaid_invoices AS
         SELECT i.id, c.name AS company_name, i.due_at, i.total
         FROM crm_p6_invoices AS i
         JOIN crm_p6_companies AS c ON c.id = i.company_id
         WHERE i.paid = FALSE",
    );

    let earliest = exec(
        &db,
        "SELECT id, company_name, due_at, total
         FROM v_unpaid_invoices
         ORDER BY due_at, id
         LIMIT 3",
    );
    assert_eq!(
        rows(&earliest),
        vec![
            vec![
                Value::Int64(4),
                Value::Text("Globex".to_string()),
                Value::Text("2026-07-02".to_string()),
                Value::Float64(20.0),
            ],
            vec![
                Value::Int64(3),
                Value::Text("Globex".to_string()),
                Value::Text("2026-07-03".to_string()),
                Value::Float64(30.0),
            ],
            vec![
                Value::Int64(5),
                Value::Text("Acme".to_string()),
                Value::Text("2026-07-04".to_string()),
                Value::Float64(40.0),
            ],
        ]
    );
}

#[test]
fn crm_unpaid_invoices_view_ordered_desc_limit_tracks_partial_index_writes() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE crm_p6_users (
            id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL,
            email TEXT NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE TABLE crm_p6_desc_invoices (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES crm_p6_users(id),
            invoice_number TEXT NOT NULL,
            due_at TEXT NOT NULL,
            total FLOAT64 NOT NULL,
            paid BOOL NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE INDEX crm_p6_desc_unpaid_due_idx
         ON crm_p6_desc_invoices(due_at)
         INCLUDE (user_id, invoice_number, total)
         WHERE paid = FALSE",
    );
    exec(
        &db,
        "INSERT INTO crm_p6_users VALUES
            (1, 'Ada Lovelace', 'ada@example.com'),
            (2, 'Grace Hopper', 'grace@example.com')",
    );
    exec(
        &db,
        "INSERT INTO crm_p6_desc_invoices VALUES
            (1, 1, 'INV-001', '2026-07-01', 10.00, FALSE),
            (2, 1, 'INV-002', '2026-08-01', 20.00, TRUE),
            (3, 2, 'INV-003', '2026-07-10', 30.00, FALSE),
            (4, 2, 'INV-004', '2026-07-20', 40.00, FALSE),
            (5, 1, 'INV-005', '2026-07-15', 50.00, FALSE)",
    );
    exec(
        &db,
        "CREATE VIEW v_unpaid_invoices_desc AS
         SELECT i.id, i.invoice_number, u.full_name, u.email, i.total, i.due_at
         FROM crm_p6_desc_invoices AS i
         JOIN crm_p6_users AS u ON u.id = i.user_id
         WHERE i.paid = FALSE",
    );

    let newest = exec(
        &db,
        "SELECT *
         FROM v_unpaid_invoices_desc
         ORDER BY due_at DESC
         LIMIT 3",
    );
    assert_eq!(
        rows(&newest)
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(4), Value::Int64(5), Value::Int64(3)]
    );

    exec(
        &db,
        "UPDATE crm_p6_desc_invoices SET paid = TRUE WHERE id = 4",
    );
    exec(
        &db,
        "INSERT INTO crm_p6_desc_invoices VALUES
            (6, 1, 'INV-006', '2026-07-25', 60.00, FALSE)",
    );

    let after_writes = exec(
        &db,
        "SELECT *
         FROM v_unpaid_invoices_desc
         ORDER BY due_at DESC
         LIMIT 3",
    );
    assert_eq!(
        rows(&after_writes)
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int64(6), Value::Int64(5), Value::Int64(3)]
    );
}

#[test]
fn crm_cascade_delete_with_fk_indexes_removes_company_graph_and_rolls_back() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE crm_p8_companies (
            id INT64 PRIMARY KEY,
            name TEXT NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE TABLE crm_p8_users (
            id INT64 PRIMARY KEY,
            company_id INT64 NOT NULL REFERENCES crm_p8_companies(id) ON DELETE CASCADE,
            email TEXT NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE TABLE crm_p8_addresses (
            id INT64 PRIMARY KEY,
            company_id INT64 NOT NULL REFERENCES crm_p8_companies(id) ON DELETE CASCADE,
            city TEXT NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE TABLE crm_p8_invoices (
            id INT64 PRIMARY KEY,
            company_id INT64 NOT NULL REFERENCES crm_p8_companies(id) ON DELETE CASCADE,
            user_id INT64 NOT NULL REFERENCES crm_p8_users(id) ON DELETE CASCADE,
            total FLOAT64 NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE TABLE crm_p8_invoice_items (
            id INT64 PRIMARY KEY,
            invoice_id INT64 NOT NULL REFERENCES crm_p8_invoices(id) ON DELETE CASCADE,
            amount FLOAT64 NOT NULL
        )",
    );
    exec(
        &db,
        "CREATE INDEX crm_p8_users_company_idx ON crm_p8_users(company_id)",
    );
    exec(
        &db,
        "CREATE INDEX crm_p8_addresses_company_idx ON crm_p8_addresses(company_id)",
    );
    exec(
        &db,
        "CREATE INDEX crm_p8_invoices_company_idx ON crm_p8_invoices(company_id)",
    );
    exec(
        &db,
        "CREATE INDEX crm_p8_invoices_user_idx ON crm_p8_invoices(user_id)",
    );
    exec(
        &db,
        "CREATE INDEX crm_p8_invoices_low_total_idx
         ON crm_p8_invoices(total)
         WHERE total < 45",
    );
    exec(
        &db,
        "CREATE INDEX crm_p8_items_invoice_idx ON crm_p8_invoice_items(invoice_id)",
    );

    exec(
        &db,
        "INSERT INTO crm_p8_companies VALUES (1, 'Acme'), (2, 'Globex')",
    );
    exec(
        &db,
        "INSERT INTO crm_p8_users VALUES
            (10, 1, 'a1@example.com'),
            (11, 1, 'a2@example.com'),
            (20, 2, 'g1@example.com')",
    );
    exec(
        &db,
        "INSERT INTO crm_p8_addresses VALUES
            (100, 1, 'Austin'),
            (101, 1, 'Dallas'),
            (200, 2, 'Chicago')",
    );
    exec(
        &db,
        "INSERT INTO crm_p8_invoices VALUES
            (1000, 1, 10, 30.00),
            (1001, 1, 11, 40.00),
            (2000, 2, 20, 50.00)",
    );
    exec(
        &db,
        "INSERT INTO crm_p8_invoice_items VALUES
            (1, 1000, 10.00),
            (2, 1000, 20.00),
            (3, 1001, 40.00),
            (4, 2000, 50.00)",
    );

    exec(&db, "BEGIN");
    let rollback_delete = exec(&db, "DELETE FROM crm_p8_companies WHERE id = 1");
    assert_eq!(rollback_delete.affected_rows(), 1);
    exec(&db, "ROLLBACK");
    let counts_after_rollback = exec(
        &db,
        "SELECT
            (SELECT COUNT(*) FROM crm_p8_companies),
            (SELECT COUNT(*) FROM crm_p8_users),
            (SELECT COUNT(*) FROM crm_p8_addresses),
            (SELECT COUNT(*) FROM crm_p8_invoices),
            (SELECT COUNT(*) FROM crm_p8_invoice_items)",
    );
    assert_eq!(
        rows(&counts_after_rollback),
        vec![vec![
            Value::Int64(2),
            Value::Int64(3),
            Value::Int64(3),
            Value::Int64(3),
            Value::Int64(4),
        ]]
    );
    let rollback_address_lookup = exec(
        &db,
        "SELECT id FROM crm_p8_addresses WHERE company_id = 1 ORDER BY id",
    );
    assert_eq!(
        rows(&rollback_address_lookup),
        vec![vec![Value::Int64(100)], vec![Value::Int64(101)]]
    );
    let rollback_item_lookup = exec(
        &db,
        "SELECT id FROM crm_p8_invoice_items WHERE invoice_id = 1000 ORDER BY id",
    );
    assert_eq!(
        rows(&rollback_item_lookup),
        vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]
    );
    let rollback_partial_index_lookup = exec(
        &db,
        "SELECT id FROM crm_p8_invoices WHERE total < 45 ORDER BY id",
    );
    assert_eq!(
        rows(&rollback_partial_index_lookup),
        vec![vec![Value::Int64(1000)], vec![Value::Int64(1001)]]
    );

    let committed_delete = exec(&db, "DELETE FROM crm_p8_companies WHERE id = 1");
    assert_eq!(committed_delete.affected_rows(), 1);

    let remaining_counts = exec(
        &db,
        "SELECT
            (SELECT COUNT(*) FROM crm_p8_companies),
            (SELECT COUNT(*) FROM crm_p8_users),
            (SELECT COUNT(*) FROM crm_p8_addresses),
            (SELECT COUNT(*) FROM crm_p8_invoices),
            (SELECT COUNT(*) FROM crm_p8_invoice_items)",
    );
    assert_eq!(
        rows(&remaining_counts),
        vec![vec![
            Value::Int64(1),
            Value::Int64(1),
            Value::Int64(1),
            Value::Int64(1),
            Value::Int64(1),
        ]]
    );
    let deleted_address_lookup = exec(
        &db,
        "SELECT id FROM crm_p8_addresses WHERE company_id = 1 ORDER BY id",
    );
    assert_eq!(rows(&deleted_address_lookup), Vec::<Vec<Value>>::new());
    let deleted_item_lookup = exec(
        &db,
        "SELECT id FROM crm_p8_invoice_items WHERE invoice_id = 1000 ORDER BY id",
    );
    assert_eq!(rows(&deleted_item_lookup), Vec::<Vec<Value>>::new());
    let remaining_item_lookup = exec(
        &db,
        "SELECT id FROM crm_p8_invoice_items WHERE invoice_id = 2000 ORDER BY id",
    );
    assert_eq!(rows(&remaining_item_lookup), vec![vec![Value::Int64(4)]]);
    let deleted_partial_index_lookup = exec(
        &db,
        "SELECT id FROM crm_p8_invoices WHERE total < 45 ORDER BY id",
    );
    assert_eq!(
        rows(&deleted_partial_index_lookup),
        Vec::<Vec<Value>>::new()
    );

    let survivors = exec(
        &db,
        "SELECT c.id, u.id, a.id, i.id, ii.id
         FROM crm_p8_companies AS c
         JOIN crm_p8_users AS u ON u.company_id = c.id
         JOIN crm_p8_addresses AS a ON a.company_id = c.id
         JOIN crm_p8_invoices AS i ON i.company_id = c.id
         JOIN crm_p8_invoice_items AS ii ON ii.invoice_id = i.id",
    );
    assert_eq!(
        rows(&survivors),
        vec![vec![
            Value::Int64(2),
            Value::Int64(20),
            Value::Int64(200),
            Value::Int64(2000),
            Value::Int64(4),
        ]]
    );
}
