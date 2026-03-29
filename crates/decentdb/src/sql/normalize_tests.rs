//! Unit tests for sql normalization logic.

#[cfg(test)]
mod tests {
    use crate::sql::ast::Statement;
    use crate::sql::normalize::normalize_statement_text;

    #[test]
    fn truncate_single_and_options() {
        let stmt =
            normalize_statement_text("TRUNCATE TABLE t RESTART IDENTITY CASCADE").expect("parsed");
        match stmt {
            Statement::TruncateTable {
                table_name,
                identity,
                cascade,
            } => {
                assert_eq!(table_name, "t");
                assert!(cascade);
                use crate::sql::ast::TruncateIdentityMode;
                assert_eq!(identity, TruncateIdentityMode::Restart);
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn truncate_multiple_tables_is_unsupported() {
        let res = normalize_statement_text("TRUNCATE TABLE a, b");
        assert!(res.is_err());
    }

    #[test]
    fn vacuum_is_unsupported() {
        let res = normalize_statement_text("VACUUM FULL");
        assert!(res.is_err());
    }

    #[test]
    fn create_index_concurrent_unsupported() {
        let res = normalize_statement_text("CREATE INDEX CONCURRENTLY idx ON t (a)");
        assert!(res.is_err());
    }

    #[test]
    fn explain_analyze_detects_analyze() {
        let stmt = normalize_statement_text("EXPLAIN ANALYZE SELECT 1").expect("parsed");
        match stmt {
            Statement::Explain(explain) => {
                assert!(explain.analyze);
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn insert_on_conflict_do_nothing_parsed() {
        let stmt = normalize_statement_text("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING")
            .expect("parsed");
        match stmt {
            Statement::Insert(insert) => {
                assert!(insert.on_conflict.is_some());
            }
            other => panic!("unexpected: {:?}", other),
        }
    }
}
