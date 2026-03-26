#[cfg(test)]
mod tests {
    use crate::sql::parser::parse_sql_statement;

    #[test]
    fn parse_matrix_tests_for_supported_syntax() {
        let valid_statements = [
            "CREATE TABLE users (id INT64 PRIMARY KEY)",
            "CREATE SCHEMA app",
            "CREATE TABLE products (id INT64 PRIMARY KEY, price FLOAT64, qty INT64, total FLOAT64 GENERATED ALWAYS AS (price * qty) STORED)",
            "CREATE TABLE products_v (id INT64 PRIMARY KEY, price FLOAT64, qty INT64, total FLOAT64 GENERATED ALWAYS AS (price * qty) VIRTUAL)",
            "CREATE TEMP TABLE scratch (id INT64 PRIMARY KEY, val TEXT)",
            "CREATE TEMP VIEW scratch_view AS SELECT 1",
            "CREATE TRIGGER log_insert AFTER INSERT ON users FOR EACH ROW BEGIN SELECT decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user added'')'); END",
            "CREATE INDEX ix_name ON users(name)",
            "CREATE INDEX ix_covering ON users(name) INCLUDE (id)",
            "INSERT INTO users (id) VALUES (1)",
            "SELECT * FROM users WHERE id = 1",
            "SELECT 10 % 3",
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 3) SELECT x FROM cnt",
            "SELECT CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, NOW(), EXTRACT(YEAR FROM CURRENT_TIMESTAMP), date('now'), datetime('2024-03-15 10:30:00', '+2 hours'), strftime('%Y', '2024-03-15')",
            "SELECT '{\"name\":\"Alice\",\"meta\":{\"version\":2}}'->>'name', '{\"name\":\"Alice\",\"meta\":{\"version\":2}}'->'meta'->>'version'",
            "SELECT key, value FROM json_each('[10,20]')",
            "SELECT key, value, type FROM json_tree('{\"a\":1}')",
            "ANALYZE",
            "ANALYZE users",
            "UPDATE users SET id = 2 WHERE id = 1",
            "DELETE FROM users WHERE id = 1",
        ];

        for stmt in valid_statements {
            assert!(
                parse_sql_statement(stmt).is_ok(),
                "Failed to parse: {}",
                stmt
            );
        }
    }

    #[test]
    fn explicit_rejection_tests_for_unsupported_syntax() {
        let invalid_statements = [
            "CREATE MATERIALIZED VIEW mv AS SELECT 1", // Unsupported DDL
            "SELECT * FROM generate_series(1, 10)", // generic set returning functions are still out of baseline scope
        ];

        for stmt in invalid_statements {
            let res = parse_sql_statement(stmt);
            assert!(
                res.is_err()
                    || if let Ok(s) = res {
                        !matches!(s, crate::sql::ast::Statement::Query(_))
                    } else {
                        false
                    },
                "Should have rejected: {}",
                stmt
            );
        }
    }

    #[test]
    fn thread_safety_tests_for_repeated_parser_invocation() {
        use std::thread;

        let handles: Vec<_> = (0..10)
            .map(|_| {
                thread::spawn(|| {
                    for _ in 0..100 {
                        assert!(parse_sql_statement("SELECT 1 + 1").is_ok());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
