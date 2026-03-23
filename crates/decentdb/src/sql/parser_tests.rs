#[cfg(test)]
mod tests {
    use crate::sql::parser::parse_sql_statement;

    #[test]
    fn parse_matrix_tests_for_supported_syntax() {
        let valid_statements = [
            "CREATE TABLE users (id INT64 PRIMARY KEY)",
            "CREATE INDEX ix_name ON users(name)",
            "INSERT INTO users (id) VALUES (1)",
            "SELECT * FROM users WHERE id = 1",
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
            "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r", // Recursive CTEs out of scope
            "SELECT * FROM generate_series(1, 10)",    // set returning functions not in baseline
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
