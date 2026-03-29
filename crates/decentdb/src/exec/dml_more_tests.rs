//! Additional unit tests for exec/dml helpers to increase coverage.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::record::value::Value;
    use crate::sql::ast::Statement;
    use crate::sql::parser::parse_sql_statement;

    #[test]
    fn can_execute_and_prepare_and_execute_simple_update() {
        let mut runtime = EngineRuntime::empty(1);
        let table = crate::catalog::TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![
                crate::catalog::ColumnSchema {
                    name: "id".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: false,
                    default_sql: None,
                    generated_sql: None,
                    generated_stored: false,
                    primary_key: true,
                    unique: false,
                    auto_increment: true,
                    checks: vec![],
                    foreign_key: None,
                },
                crate::catalog::ColumnSchema {
                    name: "val".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: false,
                    default_sql: None,
                    generated_sql: None,
                    generated_stored: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
            ],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 2,
        };
        runtime
            .catalog
            .tables
            .insert("t".to_string(), table.clone());

        runtime.tables.insert(
            "t".to_string(),
            TableData {
                rows: vec![StoredRow {
                    row_id: 1,
                    values: vec![Value::Int64(1), Value::Int64(10)],
                }],
            },
        );

        let stmt = parse_sql_statement("UPDATE t SET val = 20 WHERE id = 1").unwrap();
        let update = match stmt {
            Statement::Update(u) => u,
            _ => panic!("expected update"),
        };

        assert!(runtime.can_execute_statement_in_state_without_clone(
            &crate::sql::ast::Statement::Update(update.clone())
        ));
        let prepared = runtime
            .prepare_simple_update(&update)
            .expect("prepare didn't error")
            .expect("expected prepared update");
        let res = runtime
            .execute_prepared_simple_update(&prepared, &[])
            .expect("execute succeeded");
        assert_eq!(res.affected_rows(), 1);
        assert_eq!(
            runtime.tables.get("t").unwrap().rows[0].values[1],
            Value::Int64(20)
        );
    }

    #[test]
    fn prepare_and_execute_simple_delete_by_rowid() {
        let mut runtime = EngineRuntime::empty(1);
        let table = crate::catalog::TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![
                crate::catalog::ColumnSchema {
                    name: "id".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: false,
                    default_sql: None,
                    generated_sql: None,
                    generated_stored: false,
                    primary_key: true,
                    unique: false,
                    auto_increment: true,
                    checks: vec![],
                    foreign_key: None,
                },
                crate::catalog::ColumnSchema {
                    name: "val".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: false,
                    default_sql: None,
                    generated_sql: None,
                    generated_stored: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
            ],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 2,
        };
        runtime
            .catalog
            .tables
            .insert("t".to_string(), table.clone());

        runtime.tables.insert(
            "t".to_string(),
            TableData {
                rows: vec![StoredRow {
                    row_id: 1,
                    values: vec![Value::Int64(1), Value::Int64(10)],
                }],
            },
        );

        let stmt = parse_sql_statement("DELETE FROM t WHERE id = 1").unwrap();
        let delete = match stmt {
            Statement::Delete(d) => d,
            _ => panic!("expected delete"),
        };

        let prepared = runtime
            .prepare_simple_delete(&delete)
            .expect("prepare didn't error")
            .expect("expected prepared delete");
        let res = runtime
            .execute_prepared_simple_delete(&prepared, &[])
            .expect("execute succeeded");
        assert_eq!(res.affected_rows(), 1);
        assert!(runtime.tables.get("t").unwrap().rows.is_empty());
    }
}
