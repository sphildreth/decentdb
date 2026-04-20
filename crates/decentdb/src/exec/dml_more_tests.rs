//! Additional unit tests for exec/dml helpers to increase coverage.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::exec::dml::build_insert_row_values;
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
            .catalog_mut()
            .tables
            .insert("t".to_string(), table.clone());

        runtime.tables_mut().insert(
            "t".to_string(),
            std::sync::Arc::new(TableData {
                rows: vec![StoredRow {
                    row_id: 1,
                    values: vec![Value::Int64(1), Value::Int64(10)],
                }],
            }),
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
            .catalog_mut()
            .tables
            .insert("t".to_string(), table.clone());

        runtime.tables_mut().insert(
            "t".to_string(),
            std::sync::Arc::new(TableData {
                rows: vec![StoredRow {
                    row_id: 1,
                    values: vec![Value::Int64(1), Value::Int64(10)],
                }],
            }),
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

    #[test]
    fn build_insert_row_values_wrong_count() {
        let mut runtime = EngineRuntime::empty(1);
        let mut table = crate::catalog::TableSchema {
            name: "t2".to_string(),
            temporary: false,
            columns: vec![
                crate::catalog::ColumnSchema {
                    name: "a".to_string(),
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
                crate::catalog::ColumnSchema {
                    name: "b".to_string(),
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
            primary_key_columns: vec![],
            next_row_id: 1,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(table.name.clone(), table.clone());
        let res = build_insert_row_values(&runtime, &mut table, &[], vec![Value::Int64(1)], &[]);
        assert!(res.is_err());
    }

    #[test]
    fn build_insert_row_values_generated_column_error() {
        let mut runtime = EngineRuntime::empty(1);
        let mut table = crate::catalog::TableSchema {
            name: "t3".to_string(),
            temporary: false,
            columns: vec![
                crate::catalog::ColumnSchema {
                    name: "g".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: false,
                    default_sql: None,
                    generated_sql: Some("expr".to_string()),
                    generated_stored: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
                crate::catalog::ColumnSchema {
                    name: "a".to_string(),
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
            primary_key_columns: vec![],
            next_row_id: 1,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(table.name.clone(), table.clone());
        let res = build_insert_row_values(
            &runtime,
            &mut table,
            &["g".to_string()],
            vec![Value::Int64(1)],
            &[],
        );
        assert!(res.is_err());
    }

    #[test]
    fn build_insert_row_values_duplicate_assignment_error() {
        let mut runtime = EngineRuntime::empty(1);
        let mut table = crate::catalog::TableSchema {
            name: "t4".to_string(),
            temporary: false,
            columns: vec![crate::catalog::ColumnSchema {
                name: "a".to_string(),
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
            }],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec![],
            next_row_id: 1,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(table.name.clone(), table.clone());
        let res = build_insert_row_values(
            &runtime,
            &mut table,
            &["a".to_string(), "a".to_string()],
            vec![Value::Int64(1), Value::Int64(2)],
            &[],
        );
        assert!(res.is_err());
    }

    #[test]
    fn execute_prepared_simple_insert_positional_params_too_few_params() {
        let mut runtime = EngineRuntime::empty(1);
        let table = crate::catalog::TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![
                crate::catalog::ColumnSchema {
                    name: "a".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: true,
                    default_sql: None,
                    generated_sql: None,
                    generated_stored: false,
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
                crate::catalog::ColumnSchema {
                    name: "b".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: true,
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
            primary_key_columns: vec![],
            next_row_id: 1,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(table.name.clone(), table.clone());
        runtime.tables_mut().insert(
            table.name.clone(),
            std::sync::Arc::new(TableData { rows: vec![] }),
        );
        let prepared = crate::exec::dml::PreparedSimpleInsert {
            table_name: "t".to_string(),
            columns: vec![
                crate::exec::dml::PreparedInsertColumn {
                    name: "a".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    auto_increment: false,
                },
                crate::exec::dml::PreparedInsertColumn {
                    name: "b".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    auto_increment: false,
                },
            ],
            primary_auto_row_id_column_index: None,
            value_sources: vec![],
            required_columns: vec![],
            foreign_keys: vec![],
            unique_indexes: vec![],
            insert_indexes: vec![],
            use_generic_validation: false,
            use_generic_index_updates: false,
            compiled_index_state_epoch: runtime.index_state_epoch,
        };
        let mut params = vec![Value::Int64(1)];
        let res = runtime.execute_prepared_simple_insert_positional_params_in_place(
            &prepared,
            &mut params,
            4096,
        );
        assert!(res.is_err());
    }

    #[test]
    fn execute_prepared_simple_insert_positional_params_auto_increment_type_error() {
        let mut runtime = EngineRuntime::empty(1);
        let table = crate::catalog::TableSchema {
            name: "t2".to_string(),
            temporary: false,
            columns: vec![crate::catalog::ColumnSchema {
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
            }],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(table.name.clone(), table.clone());
        runtime.tables_mut().insert(
            table.name.clone(),
            std::sync::Arc::new(TableData { rows: vec![] }),
        );
        let prepared = crate::exec::dml::PreparedSimpleInsert {
            table_name: "t2".to_string(),
            columns: vec![crate::exec::dml::PreparedInsertColumn {
                name: "id".to_string(),
                column_type: crate::catalog::ColumnType::Int64,
                auto_increment: true,
            }],
            primary_auto_row_id_column_index: Some(0),
            value_sources: vec![],
            required_columns: vec![],
            foreign_keys: vec![],
            unique_indexes: vec![],
            insert_indexes: vec![],
            use_generic_validation: false,
            use_generic_index_updates: false,
            compiled_index_state_epoch: runtime.index_state_epoch,
        };
        let mut params = vec![Value::Text("bad".to_string())];
        let res = runtime.execute_prepared_simple_insert_positional_params_in_place(
            &prepared,
            &mut params,
            4096,
        );
        assert!(res.is_err());
    }
}
