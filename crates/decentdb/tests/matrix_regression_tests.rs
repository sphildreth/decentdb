use decentdb::{Db, DbConfig, Value};
use tempfile::TempDir;

fn row_values(result: &decentdb::QueryResult) -> Vec<Vec<Value>> {
    result
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

#[test]
fn matrix_ddl_examples_execute() {
    let tempdir = TempDir::new().unwrap();
    let path = tempdir.path().join("matrix-ddl.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            amount DECIMAL(10,2) NOT NULL
        )",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE order_items (
            order_id INTEGER,
            product_id INTEGER,
            qty INTEGER NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        )",
    )
    .unwrap();
    db.execute("CREATE INDEX idx_orders_user ON orders (user_id) WHERE user_id IS NOT NULL")
        .unwrap();
    db.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            price DECIMAL(10,2),
            tax_rate DECIMAL(4,2),
            total DECIMAL(10,2) GENERATED ALWAYS AS (price * (1 + tax_rate)) STORED
        )",
    )
    .unwrap();
    db.execute("CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE TEMP VIEW recent_orders AS SELECT * FROM orders WHERE id > 100")
        .unwrap();
    db.execute("CREATE TABLE audit_log (msg TEXT)").unwrap();
    db.execute(
        "CREATE TRIGGER log_insert AFTER INSERT ON users
         FOR EACH ROW BEGIN
           SELECT decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user added'')');
         END",
    )
    .unwrap();

    db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
        .unwrap();
    db.execute("INSERT INTO scratch VALUES (1, 'temp')")
        .unwrap();

    assert_eq!(
        row_values(&db.execute("SELECT msg FROM audit_log").unwrap()),
        vec![vec![Value::Text("user added".to_string())]]
    );
    assert!(db
        .table_ddl("scratch")
        .unwrap()
        .starts_with("CREATE TEMP TABLE"));
    assert!(db
        .view_ddl("recent_orders")
        .unwrap()
        .starts_with("CREATE TEMP VIEW"));

    db.execute("ALTER TABLE users ADD COLUMN email TEXT")
        .unwrap();
    db.execute("ALTER TABLE users RENAME COLUMN name TO full_name")
        .unwrap();
    db.execute("ALTER TABLE users ALTER COLUMN full_name TYPE TEXT")
        .unwrap();
    db.execute("ALTER TABLE users DROP COLUMN email").unwrap();

    db.execute(
        "CREATE VIEW user_orders AS
         SELECT u.full_name, o.id FROM users u LEFT JOIN orders o ON u.id = o.user_id",
    )
    .unwrap();

    db.execute("DROP TRIGGER log_insert ON users").unwrap();
    db.execute("DROP VIEW user_orders").unwrap();
    db.execute("DROP INDEX idx_orders_user").unwrap();
    db.execute("DROP TABLE order_items").unwrap();
}

#[test]
fn matrix_dml_examples_execute() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let inserted = db
        .execute("INSERT INTO users (name) VALUES ('Alice') RETURNING id")
        .unwrap();
    assert_eq!(row_values(&inserted), vec![vec![Value::Int64(1)]]);

    db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice v2') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name")
        .unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (1, 'ignored') ON CONFLICT (id) DO NOTHING")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Bob'), ('Charlie')")
        .unwrap();
    db.execute("UPDATE users SET name = 'Bobby' WHERE id = 2")
        .unwrap();
    db.execute("DELETE FROM users WHERE id = 3").unwrap();

    assert_eq!(
        row_values(
            &db.execute("SELECT id, name FROM users ORDER BY id")
                .unwrap()
        ),
        vec![
            vec![Value::Int64(1), Value::Text("Alice v2".to_string())],
            vec![Value::Int64(2), Value::Text("Bobby".to_string())],
        ]
    );
}

#[test]
fn matrix_join_examples_execute() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT64 PRIMARY KEY, user_id INT64, amount INT64)")
        .unwrap();
    db.execute("CREATE TABLE products (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE order_details (id INT64 PRIMARY KEY, note TEXT)")
        .unwrap();

    db.execute("INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace')")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1, 5), (11, 3, 7)")
        .unwrap();
    db.execute("INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget')")
        .unwrap();
    db.execute("INSERT INTO order_details VALUES (10, 'shipped'), (12, 'lost')")
        .unwrap();

    assert_eq!(
        row_values(
            &db.execute(
                "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id",
            )
            .unwrap(),
        ),
        vec![vec![Value::Text("Ada".to_string()), Value::Int64(5)]]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id",
            )
            .unwrap(),
        ),
        vec![
            vec![Value::Text("Ada".to_string()), Value::Int64(5)],
            vec![Value::Text("Grace".to_string()), Value::Null],
        ]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT u.name, o.amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id ORDER BY o.id",
            )
            .unwrap(),
        ),
        vec![
            vec![Value::Text("Ada".to_string()), Value::Int64(5)],
            vec![Value::Null, Value::Int64(7)],
        ]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT u.name, o.amount FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id ORDER BY COALESCE(u.id, o.id)",
            )
            .unwrap(),
        ),
        vec![
            vec![Value::Text("Ada".to_string()), Value::Int64(5)],
            vec![Value::Text("Grace".to_string()), Value::Null],
            vec![Value::Null, Value::Int64(7)],
        ]
    );
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM users CROSS JOIN products")
            .unwrap()
            .rows()[0]
            .values(),
        &[Value::Int64(4)]
    );
    assert_eq!(
        row_values(
            &db.execute("SELECT note FROM orders NATURAL JOIN order_details ORDER BY id")
                .unwrap(),
        ),
        vec![vec![Value::Text("shipped".to_string())]]
    );
}

#[test]
fn matrix_query_and_clause_examples_execute() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE TABLE orders (
            id INT64 PRIMARY KEY,
            user_id INT64,
            created_at INT64,
            amount INT64
        )",
    )
    .unwrap();
    db.execute(
        "INSERT INTO orders VALUES
            (1, 1, 10, 50),
            (2, 1, 20, 75),
            (3, 2, 15, 25),
            (4, 2, 30, 25)",
    )
    .unwrap();

    assert_eq!(
        row_values(
            &db.execute(
                "SELECT DISTINCT ON (user_id) user_id, id, created_at
                 FROM orders
                 ORDER BY user_id, created_at DESC",
            )
            .unwrap(),
        ),
        vec![
            vec![Value::Int64(1), Value::Int64(2), Value::Int64(20)],
            vec![Value::Int64(2), Value::Int64(4), Value::Int64(30)],
        ]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT user_id, COUNT(*) FROM orders
                 GROUP BY user_id
                 HAVING COUNT(*) > 1
                 ORDER BY user_id",
            )
            .unwrap(),
        ),
        vec![
            vec![Value::Int64(1), Value::Int64(2)],
            vec![Value::Int64(2), Value::Int64(2)],
        ]
    );
    assert_eq!(
        row_values(
            &db.execute("SELECT id FROM orders ORDER BY id LIMIT ALL OFFSET 2")
                .unwrap(),
        ),
        vec![vec![Value::Int64(3)], vec![Value::Int64(4)]]
    );
    assert_eq!(
        row_values(
            &db.execute("SELECT id FROM orders ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",)
                .unwrap(),
        ),
        vec![vec![Value::Int64(2)], vec![Value::Int64(3)]]
    );
}

#[test]
fn matrix_aggregate_examples_execute() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE TABLE employees (
            id INT64 PRIMARY KEY,
            department TEXT,
            name TEXT,
            amount INT64
        )",
    )
    .unwrap();
    db.execute(
        "INSERT INTO employees VALUES
            (1, 'eng', 'Ada', 10),
            (2, 'eng', 'Grace', 10),
            (3, 'ops', 'Ken', 20)",
    )
    .unwrap();

    assert_eq!(
        row_values(
            &db.execute(
                "SELECT COUNT(*), COUNT(amount), SUM(amount), AVG(amount), MIN(amount), MAX(amount)
                 FROM employees",
            )
            .unwrap(),
        ),
        vec![vec![
            Value::Int64(3),
            Value::Int64(3),
            Value::Int64(40),
            Value::Float64(40.0 / 3.0),
            Value::Int64(10),
            Value::Int64(20),
        ]]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT department, GROUP_CONCAT(name, ', '), STRING_AGG(name, ', ')
                 FROM employees
                 GROUP BY department
                 ORDER BY department",
            )
            .unwrap(),
        ),
        vec![
            vec![
                Value::Text("eng".to_string()),
                Value::Text("Ada, Grace".to_string()),
                Value::Text("Ada, Grace".to_string()),
            ],
            vec![
                Value::Text("ops".to_string()),
                Value::Text("Ken".to_string()),
                Value::Text("Ken".to_string()),
            ],
        ]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT COUNT(DISTINCT amount), SUM(DISTINCT amount), AVG(DISTINCT amount), TOTAL(amount)
                 FROM employees",
            )
            .unwrap(),
        ),
        vec![vec![
            Value::Int64(2),
            Value::Int64(30),
            Value::Float64(15.0),
            Value::Float64(40.0),
        ]]
    );
}

#[test]
fn matrix_window_examples_execute() {
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

    assert_eq!(
        row_values(
            &db.execute(
                "SELECT name,
                        department,
                        salary,
                        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn,
                        RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk,
                        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rnk,
                        FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) AS first_name,
                        LAST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) AS last_name,
                        NTH_VALUE(name, 2) OVER (PARTITION BY department ORDER BY salary DESC) AS second_name
                 FROM employees
                 ORDER BY department, salary DESC, name",
            )
            .unwrap(),
        ),
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
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT name,
                        LAG(name, 1) OVER (ORDER BY id),
                        LEAD(name, 1) OVER (ORDER BY id)
                 FROM employees
                 ORDER BY id",
            )
            .unwrap(),
        ),
        vec![
            vec![
                Value::Text("Ada".to_string()),
                Value::Null,
                Value::Text("Grace".to_string()),
            ],
            vec![
                Value::Text("Grace".to_string()),
                Value::Text("Ada".to_string()),
                Value::Text("Linus".to_string()),
            ],
            vec![
                Value::Text("Linus".to_string()),
                Value::Text("Grace".to_string()),
                Value::Text("Ken".to_string()),
            ],
            vec![
                Value::Text("Ken".to_string()),
                Value::Text("Linus".to_string()),
                Value::Text("Denise".to_string()),
            ],
            vec![
                Value::Text("Denise".to_string()),
                Value::Text("Ken".to_string()),
                Value::Null,
            ],
        ]
    );
}

#[test]
fn matrix_scalar_and_json_examples_execute() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    assert_eq!(
        row_values(
            &db.execute(
                "SELECT ABS(-42), CEIL(3.2), FLOOR(3.8), ROUND(3.14159, 2),
                        SQRT(144), POWER(2, 10), MOD(17, 5), SIGN(-99),
                        ROUND(LN(2.71828), 3), ROUND(LOG(1000), 3), ROUND(EXP(1), 3)",
            )
            .unwrap(),
        ),
        vec![vec![
            Value::Int64(42),
            Value::Float64(4.0),
            Value::Float64(3.0),
            Value::Float64(3.14),
            Value::Float64(12.0),
            Value::Float64(1024.0),
            Value::Int64(2),
            Value::Int64(-1),
            Value::Float64(1.0),
            Value::Float64(3.0),
            Value::Float64(2.718),
        ]]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT LENGTH('hello'), LOWER('HELLO'), UPPER('hello'),
                        TRIM('  hello  '), LTRIM('  hello'), RTRIM('hello  '),
                        SUBSTR('hello world', 1, 5), REPLACE('hello', 'l', 'r'),
                        INSTR('hello world', 'world'), LEFT('hello', 3), RIGHT('hello', 3),
                        LPAD('42', 5, '0'), RPAD('hi', 5, '!'), REPEAT('ab', 3),
                        REVERSE('hello'), CHR(65), HEX('ABC')",
            )
            .unwrap(),
        ),
        vec![vec![
            Value::Int64(5),
            Value::Text("hello".to_string()),
            Value::Text("HELLO".to_string()),
            Value::Text("hello".to_string()),
            Value::Text("hello".to_string()),
            Value::Text("hello".to_string()),
            Value::Text("hello".to_string()),
            Value::Text("herro".to_string()),
            Value::Int64(7),
            Value::Text("hel".to_string()),
            Value::Text("llo".to_string()),
            Value::Text("00042".to_string()),
            Value::Text("hi!!!".to_string()),
            Value::Text("ababab".to_string()),
            Value::Text("olleh".to_string()),
            Value::Text("A".to_string()),
            Value::Text("414243".to_string()),
        ]]
    );
    let datetime = db
        .execute(
            "SELECT CURRENT_TIMESTAMP IS NOT NULL,
                    CURRENT_DATE IS NOT NULL,
                    CURRENT_TIME IS NOT NULL,
                    NOW() IS NOT NULL,
                    date('2024-03-15', '+1 month'),
                    datetime('2024-03-15 10:30:00', '+2 hours'),
                    strftime('%Y', '2024-03-15'),
                    EXTRACT(YEAR FROM '2024-03-15')",
        )
        .unwrap();
    assert_eq!(
        datetime.rows()[0].values(),
        &[
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Text("2024-04-15".to_string()),
            Value::Text("2024-03-15 12:30:00".to_string()),
            Value::Text("2024".to_string()),
            Value::Int64(2024),
        ]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT json_extract('{\"name\":\"Alice\",\"age\":30}', '$.name'),
                        json_array_length('[1,2,3]'),
                        json_type('{\"a\":1}'),
                        json_valid('{\"a\":1}'),
                        json_object('name', 'Alice', 'age', 30),
                        json_array(1, 2, 'three'),
                        '{\"name\":\"Alice\"}'->'name',
                        '{\"name\":\"Alice\"}'->>'name'",
            )
            .unwrap(),
        ),
        vec![vec![
            Value::Text("Alice".to_string()),
            Value::Int64(3),
            Value::Text("object".to_string()),
            Value::Bool(true),
            Value::Text("{\"age\":30,\"name\":\"Alice\"}".to_string()),
            Value::Text("[1,2,\"three\"]".to_string()),
            Value::Text("\"Alice\"".to_string()),
            Value::Text("Alice".to_string()),
        ]]
    );
    assert_eq!(
        row_values(
            &db.execute("SELECT key, value FROM json_each('[10, 20, 30]') ORDER BY key")
                .unwrap(),
        ),
        vec![
            vec![Value::Int64(0), Value::Int64(10)],
            vec![Value::Int64(1), Value::Int64(20)],
            vec![Value::Int64(2), Value::Int64(30)],
        ]
    );
    assert_eq!(
        row_values(
            &db.execute("SELECT key, value FROM json_each('{\"a\":1,\"b\":2}') ORDER BY key")
                .unwrap(),
        ),
        vec![
            vec![Value::Text("a".to_string()), Value::Int64(1)],
            vec![Value::Text("b".to_string()), Value::Int64(2)],
        ]
    );
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM json_tree('{\"a\":{\"b\":1},\"c\":[2,3]}')")
            .unwrap()
            .rows()[0]
            .values(),
        &[Value::Int64(6)]
    );
}

#[test]
fn matrix_operator_examples_execute() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT, email TEXT, score INT64)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', NULL, 0), (2, 'Bob', 'b@example.com', 7)")
        .unwrap();

    assert_eq!(
        row_values(
            &db.execute("SELECT 10 + 3, 10 - 3, 10 * 3, 10 / 3, 10 % 3, 'Hello' || ' ' || 'World'")
                .unwrap(),
        ),
        vec![vec![
            Value::Int64(13),
            Value::Int64(7),
            Value::Int64(30),
            Value::Int64(3),
            Value::Int64(1),
            Value::Text("Hello World".to_string()),
        ]]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT name,
                        email IS NULL,
                        score BETWEEN 1 AND 10,
                        id IN (1, 2, 3),
                        CASE WHEN score > 5 THEN 'high' ELSE 'low' END,
                        COALESCE(email, 'no-email@example.com'),
                        NULLIF(score, 0)
                 FROM users
                 WHERE name ILIKE '%o%' OR name LIKE 'A%'
                 ORDER BY id",
            )
            .unwrap(),
        ),
        vec![
            vec![
                Value::Text("Alice".to_string()),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(true),
                Value::Text("low".to_string()),
                Value::Text("no-email@example.com".to_string()),
                Value::Null,
            ],
            vec![
                Value::Text("Bob".to_string()),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(true),
                Value::Text("high".to_string()),
                Value::Text("b@example.com".to_string()),
                Value::Int64(7),
            ],
        ]
    );
}

#[test]
fn matrix_transaction_examples_execute() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();
    db.execute("ROLLBACK TO SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Charlie')")
        .unwrap();
    db.execute("RELEASE SAVEPOINT sp1").unwrap();
    db.execute("COMMIT").unwrap();

    assert_eq!(
        row_values(&db.execute("SELECT name FROM users ORDER BY id").unwrap()),
        vec![
            vec![Value::Text("Alice".to_string())],
            vec![Value::Text("Charlie".to_string())],
        ]
    );
}

#[test]
fn matrix_data_type_and_constraint_examples_execute() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE TABLE events (
            id UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
            active BOOLEAN DEFAULT TRUE,
            price DECIMAL(10,2),
            event_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            email TEXT NOT NULL UNIQUE,
            qty INTEGER CHECK (qty >= 0)
        )",
    )
    .unwrap();
    db.execute(
        "INSERT INTO events (price, event_date, email, qty)
         VALUES (12.34, '2024-03-15', 'a@example.com', 2)",
    )
    .unwrap();

    assert_eq!(
        db.execute(
            "SELECT active, id IS NOT NULL, event_date IS NOT NULL, created_at IS NOT NULL, email
             FROM events",
        )
        .unwrap()
        .rows()[0]
            .values(),
        &[
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Text("a@example.com".to_string()),
        ]
    );

    let check_err = db
        .execute(
            "INSERT INTO events (price, event_date, email, qty)
             VALUES (9.99, '2024-03-16', 'b@example.com', -1)",
        )
        .unwrap_err();
    assert!(
        check_err.to_string().contains("CHECK constraint failed"),
        "unexpected error: {check_err}"
    );
}

#[test]
fn matrix_set_operation_and_cte_examples_execute() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE employees (name TEXT, user_id INT64)")
        .unwrap();
    db.execute("CREATE TABLE contractors (name TEXT, user_id INT64)")
        .unwrap();
    db.execute("CREATE TABLE returns (user_id INT64)").unwrap();
    db.execute("CREATE TABLE categories (id INT64 PRIMARY KEY, name TEXT, parent_id INT64)")
        .unwrap();

    db.execute("INSERT INTO employees VALUES ('Ada', 1), ('Bob', 2), ('Bob', 2)")
        .unwrap();
    db.execute("INSERT INTO contractors VALUES ('Bob', 2), ('Cara', 3)")
        .unwrap();
    db.execute("INSERT INTO returns VALUES (2), (2), (4)")
        .unwrap();
    db.execute(
        "INSERT INTO categories VALUES
            (1, 'root', NULL),
            (2, 'child', 1),
            (3, 'grandchild', 2)",
    )
    .unwrap();

    assert_eq!(
        row_values(
            &db.execute(
                "SELECT name FROM employees UNION SELECT name FROM contractors ORDER BY name"
            )
            .unwrap(),
        ),
        vec![
            vec![Value::Text("Ada".to_string())],
            vec![Value::Text("Bob".to_string())],
            vec![Value::Text("Cara".to_string())],
        ]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT user_id FROM employees INTERSECT ALL SELECT user_id FROM returns ORDER BY user_id",
            )
            .unwrap(),
        ),
        vec![vec![Value::Int64(2)], vec![Value::Int64(2)]]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "SELECT user_id FROM employees EXCEPT ALL SELECT user_id FROM returns ORDER BY user_id",
            )
            .unwrap(),
        ),
        vec![vec![Value::Int64(1)]]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "WITH active_users AS (
                    SELECT * FROM employees WHERE user_id >= 2
                 )
                 SELECT user_id FROM active_users ORDER BY user_id",
            )
            .unwrap(),
        ),
        vec![vec![Value::Int64(2)], vec![Value::Int64(2)],]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "WITH
                    dept_totals AS (
                        SELECT user_id, COUNT(*) AS total FROM employees GROUP BY user_id
                    ),
                    high_spend AS (
                        SELECT * FROM dept_totals WHERE total > 1
                    )
                 SELECT user_id, total FROM high_spend",
            )
            .unwrap(),
        ),
        vec![vec![Value::Int64(2), Value::Int64(2)]]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "WITH RECURSIVE cnt(x) AS (
                    SELECT 1
                    UNION ALL
                    SELECT x + 1 FROM cnt WHERE x < 3
                 )
                 SELECT x FROM cnt",
            )
            .unwrap(),
        ),
        vec![
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Int64(3)],
        ]
    );
    assert_eq!(
        row_values(
            &db.execute(
                "WITH RECURSIVE descendants AS (
                    SELECT id, name, parent_id FROM categories WHERE id = 1
                    UNION ALL
                    SELECT c.id, c.name, c.parent_id
                    FROM categories c INNER JOIN descendants d ON c.parent_id = d.id
                 )
                 SELECT name FROM descendants ORDER BY id",
            )
            .unwrap(),
        ),
        vec![
            vec![Value::Text("root".to_string())],
            vec![Value::Text("child".to_string())],
            vec![Value::Text("grandchild".to_string())],
        ]
    );
}

#[test]
fn matrix_intentional_rejections_remain_explicit() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let materialized_view = db
        .execute("CREATE MATERIALIZED VIEW mv AS SELECT 1")
        .unwrap_err();
    assert!(
        materialized_view.to_string().contains("not supported")
            || materialized_view.to_string().contains("Invalid statement")
            || materialized_view.to_string().contains("statement kind"),
        "unexpected error: {materialized_view}"
    );

    let generate_series = db
        .execute("SELECT * FROM generate_series(1, 10)")
        .unwrap_err();
    assert!(
        generate_series.to_string().contains("set-returning")
            || generate_series.to_string().contains("generate_series")
            || generate_series.to_string().contains("not supported"),
        "unexpected error: {generate_series}"
    );
}
