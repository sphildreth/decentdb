//! Unit tests for exec/dml helpers to increase coverage.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::exec::dml::PreparedSimpleInsert;
    use crate::sql::ast::Statement;
    use crate::sql::parser::parse_sql_statement;

    #[test]
    fn prepare_simple_insert_success() {
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
                    primary_key: false,
                    unique: false,
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
                crate::catalog::ColumnSchema {
                    name: "name".to_string(),
                    column_type: crate::catalog::ColumnType::Text,
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
        runtime.catalog.tables.insert("t".to_string(), table);

        let stmt = parse_sql_statement("INSERT INTO t (id, name) VALUES (1, 'x')").unwrap();
        let insert = match stmt {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };
        let result = runtime.prepare_simple_insert(&insert);
        assert!(result.is_ok());
        let prepared = result.unwrap();
        assert!(prepared.is_some());
    }

    #[test]
    fn prepare_simple_insert_mismatched_values_error() {
        let mut runtime = EngineRuntime::empty(1);
        let table = crate::catalog::TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![crate::catalog::ColumnSchema {
                name: "id".to_string(),
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
        runtime.catalog.tables.insert("t".to_string(), table);

        let stmt = parse_sql_statement("INSERT INTO t (id) VALUES (1, 2)").unwrap();
        let insert = match stmt {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };
        let result = runtime.prepare_simple_insert(&insert);
        assert!(result.is_err());
    }

    #[test]
    fn prepare_simple_insert_unknown_column_error() {
        let mut runtime = EngineRuntime::empty(1);
        let table = crate::catalog::TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![crate::catalog::ColumnSchema {
                name: "id".to_string(),
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
        runtime.catalog.tables.insert("t".to_string(), table);
        let stmt = parse_sql_statement("INSERT INTO t (unknown) VALUES (1)").unwrap();
        let insert = match stmt {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };
        let result = runtime.prepare_simple_insert(&insert);
        assert!(result.is_err());
    }

    #[test]
    fn can_reuse_prepared_simple_insert_epoch_matches() {
        let mut runtime = EngineRuntime::empty(1);
        let table = crate::catalog::TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec![],
            next_row_id: 1,
        };
        runtime.catalog.tables.insert("t".to_string(), table);

        let prepared = PreparedSimpleInsert {
            table_name: "t".to_string(),
            columns: vec![],
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
        assert!(runtime.can_reuse_prepared_simple_insert(&prepared));
    }

    #[test]
    fn prepare_and_execute_simple_insert_roundtrip() {
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
            next_row_id: 1,
        };
        runtime
            .catalog
            .tables
            .insert("t".to_string(), table.clone());
        runtime
            .tables
            .insert("t".to_string(), TableData { rows: vec![] });

        let stmt = parse_sql_statement("INSERT INTO t (val) VALUES (20)").unwrap();
        let insert = match stmt {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let prepared = runtime
            .prepare_simple_insert(&insert)
            .expect("prepare didn't error")
            .expect("expected prepared insert");
        let res = runtime
            .execute_prepared_simple_insert(&prepared, &[], 4096)
            .expect("execute succeeded");
        assert_eq!(res.affected_rows(), 1);
        let rows = &runtime.tables.get("t").unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[1], crate::record::value::Value::Int64(20));
        assert_eq!(rows[0].row_id, 1);
    }
}
