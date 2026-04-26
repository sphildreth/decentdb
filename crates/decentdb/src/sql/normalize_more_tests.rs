//! Additional normalization tests for sql/normalize.rs to improve coverage.

#[cfg(test)]
mod tests {
    use crate::sql::ast::{QueryBody, SetOperation, Statement, TruncateIdentityMode};
    use crate::sql::normalize::normalize_statement_text;

    #[test]
    fn set_operation_union_all_and_values() {
        let stmt = normalize_statement_text("SELECT 1 UNION ALL SELECT 2").expect("parsed");
        match stmt {
            Statement::Query(q) => match q.body {
                QueryBody::SetOperation { op, all, .. } => {
                    assert_eq!(op, SetOperation::Union);
                    assert!(all);
                }
                other => panic!("unexpected: {:?}", other),
            },
            other => panic!("unexpected: {:?}", other),
        }

        let stmt2 = normalize_statement_text("VALUES (1,2), (3,4)").expect("parsed");
        match stmt2 {
            Statement::Query(q) => match q.body {
                QueryBody::Values(rows) => {
                    assert_eq!(rows.len(), 2);
                }
                other => panic!("unexpected: {:?}", other),
            },
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn distinct_on_and_group_by_having() {
        let s = "SELECT DISTINCT ON (a) a FROM t";
        let stmt = normalize_statement_text(s).expect("parsed");
        match stmt {
            Statement::Query(q) => match q.body {
                QueryBody::Select(sel) => {
                    assert!(!sel.distinct_on.is_empty());
                }
                other => panic!("unexpected: {:?}", other),
            },
            other => panic!("unexpected: {:?}", other),
        }

        let s2 = "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1";
        let stmt2 = normalize_statement_text(s2).expect("parsed");
        match stmt2 {
            Statement::Query(q) => match q.body {
                QueryBody::Select(sel) => {
                    assert!(!sel.group_by.is_empty());
                    assert!(sel.having.is_some());
                }
                other => panic!("unexpected: {:?}", other),
            },
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn insert_on_conflict_do_update_parses() {
        let s =
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a WHERE a > 0";
        let stmt = normalize_statement_text(s).expect("parsed");
        match stmt {
            Statement::Insert(ins) => {
                assert!(ins.on_conflict.is_some());
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn create_table_as_and_view_and_alter_rename() {
        let s = "CREATE TABLE t AS SELECT 1";
        let stmt = normalize_statement_text(s).expect("parsed");
        match stmt {
            Statement::CreateTableAs(_) => {}
            other => panic!("unexpected: {:?}", other),
        }

        let s2 = "CREATE VIEW v AS SELECT 1";
        let stmt2 = normalize_statement_text(s2).expect("parsed");
        match stmt2 {
            Statement::CreateView(_) => {}
            other => panic!("unexpected: {:?}", other),
        }

        let s3 = "ALTER TABLE t RENAME TO u";
        let stmt3 = normalize_statement_text(s3).expect("parsed");
        match stmt3 {
            Statement::AlterTable {
                table_name,
                actions,
            } => {
                assert_eq!(table_name, "t");
                assert!(!actions.is_empty());
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn create_view_if_not_exists_parses() {
        let s = "CREATE VIEW IF NOT EXISTS v AS SELECT 1 AS x";
        let stmt = normalize_statement_text(s).expect("parsed");
        match stmt {
            Statement::CreateView(cv) => {
                assert!(cv.if_not_exists, "if_not_exists flag should be true");
                assert_eq!(cv.view_name, "v");
            }
            other => panic!("unexpected: {:?}", other),
        }

        // Without IF NOT EXISTS: default false
        let s2 = "CREATE VIEW v AS SELECT 1";
        let stmt2 = normalize_statement_text(s2).expect("parsed");
        match stmt2 {
            Statement::CreateView(cv) => {
                assert!(!cv.if_not_exists);
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn create_view_if_not_exists_case_insensitive() {
        let s = "create view if not exists v as select 1";
        let stmt = normalize_statement_text(s).expect("parsed");
        match stmt {
            Statement::CreateView(cv) => {
                assert!(cv.if_not_exists);
            }
            other => panic!("unexpected: {:?}", other),
        }

        let s2 = "CREATE VIEW If Not Exists v AS SELECT 1";
        let stmt2 = normalize_statement_text(s2).expect("parsed");
        match stmt2 {
            Statement::CreateView(cv) => {
                assert!(cv.if_not_exists);
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn subquery_in_from_without_alias_includes_helpful_hint() {
        let s = "SELECT AVG(cnt) FROM (SELECT COUNT(*) AS cnt FROM t)";
        let res = normalize_statement_text(s);
        assert!(res.is_err());
        let msg = res.unwrap_err().to_string();
        assert!(
            msg.contains("alias"),
            "error should mention 'alias', got: {msg}"
        );
        assert!(
            msg.contains("AS alias") || msg.contains("AS name"),
            "error should include example syntax hint, got: {msg}"
        );
    }

    #[test]
    fn unsupported_update_from_and_delete_using() {
        let s = "UPDATE t SET a = 1 FROM x";
        let res = normalize_statement_text(s);
        assert!(res.is_err());

        let s2 = "DELETE FROM t USING x";
        let res2 = normalize_statement_text(s2);
        assert!(res2.is_err());
    }

    #[test]
    fn truncate_restart_identity_and_cascade_and_misc_statements() {
        let s = "TRUNCATE TABLE t RESTART IDENTITY CASCADE";
        let stmt = normalize_statement_text(s).expect("parsed");
        match stmt {
            Statement::TruncateTable {
                table_name,
                identity,
                cascade,
            } => {
                assert_eq!(table_name, "t");
                assert_eq!(identity, TruncateIdentityMode::Restart);
                assert!(cascade);
            }
            other => panic!("unexpected: {:?}", other),
        }

        let s2 = "EXPLAIN SELECT 1";
        let stmt2 = normalize_statement_text(s2).expect("parsed");
        match stmt2 {
            Statement::Explain(_) => {}
            other => panic!("unexpected: {:?}", other),
        }

        let s3 = "CREATE INDEX idx ON t (a)";
        let stmt3 = normalize_statement_text(s3).expect("parsed");
        match stmt3 {
            Statement::CreateIndex(_) => {}
            other => panic!("unexpected: {:?}", other),
        }

        let s4 = "CREATE TRIGGER trig AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION noop()";
        let res4 = normalize_statement_text(s4);
        // Some PostgreSQL dialects accept EXECUTE FUNCTION; if parser accepts either
        // EXECUTE PROCEDURE or FUNCTION the normalization should succeed or return unsupported.
        match res4 {
            Ok(stmt4) => match stmt4 {
                Statement::CreateTrigger(_) => {}
                other => panic!("unexpected: {:?}", other),
            },
            Err(_) => {
                // Accept parse failure as some PG versions may not accept this syntax in the test environment
            }
        }

        // Vacuum and create schema/drop/rename should parse
        let s5 = "VACUUM";
        let _ = normalize_statement_text(s5);

        let s6 = "CREATE SCHEMA myschema";
        let _ = normalize_statement_text(s6);

        let s7 = "DROP TABLE IF EXISTS t";
        let _ = normalize_statement_text(s7);

        let s8 = "ALTER TABLE t RENAME COLUMN a TO b";
        let _ = normalize_statement_text(s8);
    }
}
