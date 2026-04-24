//! Unit tests for exec/dml helpers to increase coverage.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::super::*;
    use crate::exec::dml::PreparedSimpleInsert;
    use crate::sql::ast::Statement;
    use crate::sql::parser::parse_sql_statement;

    fn paged_row_source(rows: Vec<StoredRow>) -> TableRowSource {
        let payload = super::super::encode_table_payload(&crate::exec::TableData { rows })
            .expect("encode paged test payload");
        let manifest = super::super::TablePageManifest::from_payload(Arc::new(payload))
            .expect("build paged test manifest");
        TableRowSource::Paged(Arc::new(manifest))
    }

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
            pk_index_root: None,
        };
        runtime.catalog_mut().tables.insert("t".to_string(), table);

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
            pk_index_root: None,
        };
        runtime.catalog_mut().tables.insert("t".to_string(), table);

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
            pk_index_root: None,
        };
        runtime.catalog_mut().tables.insert("t".to_string(), table);
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
            pk_index_root: None,
        };
        runtime.catalog_mut().tables.insert("t".to_string(), table);

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
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert("t".to_string(), table.clone());
        runtime
            .tables_mut()
            .insert("t".to_string(), TableData { rows: vec![] }.into());

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
        let rows = &runtime.tables.get("t").unwrap().resident_data().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[1], crate::record::value::Value::Int64(20));
        assert_eq!(rows[0].row_id, 1);
    }

    #[test]
    fn prepare_simple_delete_with_restrict_child_succeeds() {
        let mut runtime = EngineRuntime::empty(1);
        let parent = crate::catalog::TableSchema {
            name: "parent".to_string(),
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
            pk_index_root: None,
        };
        let child = crate::catalog::TableSchema {
            name: "child".to_string(),
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
                    name: "parent_id".to_string(),
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
            foreign_keys: vec![crate::catalog::ForeignKeyConstraint {
                name: None,
                columns: vec!["parent_id".to_string()],
                referenced_table: "parent".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: crate::catalog::ForeignKeyAction::Restrict,
                on_update: crate::catalog::ForeignKeyAction::Restrict,
            }],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(parent.name.clone(), parent.clone());
        runtime
            .catalog_mut()
            .tables
            .insert(child.name.clone(), child);

        let stmt = parse_sql_statement("DELETE FROM parent WHERE id = 1").unwrap();
        let delete = match stmt {
            Statement::Delete(delete) => delete,
            _ => panic!("expected delete"),
        };

        let prepared = runtime
            .prepare_simple_delete(&delete)
            .expect("prepare delete");
        assert!(prepared.is_some());
        assert_eq!(prepared.unwrap().restrict_children.len(), 1);
    }

    #[test]
    fn prepare_simple_delete_with_cascade_child_falls_back() {
        let mut runtime = EngineRuntime::empty(1);
        let parent = crate::catalog::TableSchema {
            name: "parent".to_string(),
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
            pk_index_root: None,
        };
        let child = crate::catalog::TableSchema {
            name: "child".to_string(),
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
                    name: "parent_id".to_string(),
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
            foreign_keys: vec![crate::catalog::ForeignKeyConstraint {
                name: None,
                columns: vec!["parent_id".to_string()],
                referenced_table: "parent".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: crate::catalog::ForeignKeyAction::Cascade,
                on_update: crate::catalog::ForeignKeyAction::Restrict,
            }],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(parent.name.clone(), parent.clone());
        runtime
            .catalog_mut()
            .tables
            .insert(child.name.clone(), child);

        let stmt = parse_sql_statement("DELETE FROM parent WHERE id = 1").unwrap();
        let delete = match stmt {
            Statement::Delete(delete) => delete,
            _ => panic!("expected delete"),
        };

        let prepared = runtime
            .prepare_simple_delete(&delete)
            .expect("prepare delete");
        assert!(prepared.is_none());
    }

    #[test]
    fn prepare_simple_delete_with_composite_restrict_child_succeeds() {
        let mut runtime = EngineRuntime::empty(1);
        let parent = crate::catalog::TableSchema {
            name: "parent".to_string(),
            temporary: false,
            columns: vec![
                crate::catalog::ColumnSchema {
                    name: "a".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: false,
                    default_sql: None,
                    generated_sql: None,
                    generated_stored: false,
                    primary_key: true,
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
                    primary_key: true,
                    unique: false,
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
            ],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["a".to_string(), "b".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        let child = crate::catalog::TableSchema {
            name: "child".to_string(),
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
                    name: "parent_a".to_string(),
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
                    name: "parent_b".to_string(),
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
            foreign_keys: vec![crate::catalog::ForeignKeyConstraint {
                name: None,
                columns: vec!["parent_a".to_string(), "parent_b".to_string()],
                referenced_table: "parent".to_string(),
                referenced_columns: vec!["a".to_string(), "b".to_string()],
                on_delete: crate::catalog::ForeignKeyAction::Restrict,
                on_update: crate::catalog::ForeignKeyAction::Restrict,
            }],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(parent.name.clone(), parent.clone());
        runtime.catalog_mut().indexes.insert(
            "parent_a_idx".to_string(),
            crate::catalog::IndexSchema {
                name: "parent_a_idx".to_string(),
                table_name: "parent".to_string(),
                kind: crate::catalog::IndexKind::Btree,
                unique: false,
                columns: vec![crate::catalog::IndexColumn {
                    column_name: Some("a".to_string()),
                    expression_sql: None,
                }],
                include_columns: vec![],
                predicate_sql: None,
                fresh: true,
            },
        );
        runtime
            .catalog_mut()
            .tables
            .insert(child.name.clone(), child);

        let stmt = parse_sql_statement("DELETE FROM parent WHERE a = 1").unwrap();
        let delete = match stmt {
            Statement::Delete(delete) => delete,
            _ => panic!("expected delete"),
        };

        let prepared = runtime
            .prepare_simple_delete(&delete)
            .expect("prepare delete");
        assert!(prepared.is_some());
        assert_eq!(prepared.unwrap().restrict_children.len(), 1);
    }

    #[test]
    fn prepare_simple_insert_with_composite_foreign_key_succeeds() {
        let mut runtime = EngineRuntime::empty(1);
        let parent = crate::catalog::TableSchema {
            name: "parent".to_string(),
            temporary: false,
            columns: vec![
                crate::catalog::ColumnSchema {
                    name: "a".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: false,
                    default_sql: None,
                    generated_sql: None,
                    generated_stored: false,
                    primary_key: true,
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
                    primary_key: true,
                    unique: false,
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
            ],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["a".to_string(), "b".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        let child = crate::catalog::TableSchema {
            name: "child".to_string(),
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
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
                crate::catalog::ColumnSchema {
                    name: "parent_a".to_string(),
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
                    name: "parent_b".to_string(),
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
            foreign_keys: vec![crate::catalog::ForeignKeyConstraint {
                name: None,
                columns: vec!["parent_a".to_string(), "parent_b".to_string()],
                referenced_table: "parent".to_string(),
                referenced_columns: vec!["a".to_string(), "b".to_string()],
                on_delete: crate::catalog::ForeignKeyAction::Restrict,
                on_update: crate::catalog::ForeignKeyAction::Restrict,
            }],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(parent.name.clone(), parent.clone());
        runtime
            .catalog_mut()
            .tables
            .insert(child.name.clone(), child);
        runtime.catalog_mut().indexes.insert(
            "parent_ab_unique".to_string(),
            crate::catalog::IndexSchema {
                name: "parent_ab_unique".to_string(),
                table_name: "parent".to_string(),
                kind: crate::catalog::IndexKind::Btree,
                unique: true,
                columns: vec![
                    crate::catalog::IndexColumn {
                        column_name: Some("a".to_string()),
                        expression_sql: None,
                    },
                    crate::catalog::IndexColumn {
                        column_name: Some("b".to_string()),
                        expression_sql: None,
                    },
                ],
                include_columns: vec![],
                predicate_sql: None,
                fresh: true,
            },
        );
        runtime.indexes_mut().insert(
            "parent_ab_unique".to_string(),
            Arc::new(RuntimeIndex::Btree {
                keys: RuntimeBtreeKeys::UniqueEncoded(BTreeMap::new()),
            }),
        );

        let stmt =
            parse_sql_statement("INSERT INTO child (id, parent_a, parent_b) VALUES (1, 7, 9)")
                .unwrap();
        let insert = match stmt {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let prepared = runtime
            .prepare_simple_insert(&insert)
            .expect("prepare insert");
        assert!(prepared.is_some());
        let prepared = prepared.unwrap();
        assert!(!prepared.use_generic_validation);
        assert_eq!(prepared.foreign_keys.len(), 1);
    }

    #[test]
    fn execute_prepared_simple_insert_with_composite_foreign_key_dirty_paged_parent() {
        let mut runtime = EngineRuntime::empty(1);
        let parent = crate::catalog::TableSchema {
            name: "parent".to_string(),
            temporary: false,
            columns: vec![
                crate::catalog::ColumnSchema {
                    name: "a".to_string(),
                    column_type: crate::catalog::ColumnType::Int64,
                    nullable: false,
                    default_sql: None,
                    generated_sql: None,
                    generated_stored: false,
                    primary_key: true,
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
                    primary_key: true,
                    unique: false,
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
            ],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["a".to_string(), "b".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        let child = crate::catalog::TableSchema {
            name: "child".to_string(),
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
                    auto_increment: false,
                    checks: vec![],
                    foreign_key: None,
                },
                crate::catalog::ColumnSchema {
                    name: "parent_a".to_string(),
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
                    name: "parent_b".to_string(),
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
            foreign_keys: vec![crate::catalog::ForeignKeyConstraint {
                name: None,
                columns: vec!["parent_a".to_string(), "parent_b".to_string()],
                referenced_table: "parent".to_string(),
                referenced_columns: vec!["a".to_string(), "b".to_string()],
                on_delete: crate::catalog::ForeignKeyAction::Restrict,
                on_update: crate::catalog::ForeignKeyAction::Restrict,
            }],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(parent.name.clone(), parent.clone());
        runtime
            .catalog_mut()
            .tables
            .insert(child.name.clone(), child.clone());
        runtime.catalog_mut().indexes.insert(
            "parent_ab_unique".to_string(),
            crate::catalog::IndexSchema {
                name: "parent_ab_unique".to_string(),
                table_name: "parent".to_string(),
                kind: crate::catalog::IndexKind::Btree,
                unique: true,
                columns: vec![
                    crate::catalog::IndexColumn {
                        column_name: Some("a".to_string()),
                        expression_sql: None,
                    },
                    crate::catalog::IndexColumn {
                        column_name: Some("b".to_string()),
                        expression_sql: None,
                    },
                ],
                include_columns: vec![],
                predicate_sql: None,
                fresh: true,
            },
        );
        let mut parent_index_entries = BTreeMap::new();
        parent_index_entries.insert(
            crate::record::row::Row::new(vec![
                crate::record::value::Value::Int64(7),
                crate::record::value::Value::Int64(9),
            ])
            .encode()
            .expect("encode parent composite key"),
            1,
        );
        runtime.indexes_mut().insert(
            "parent_ab_unique".to_string(),
            Arc::new(RuntimeIndex::Btree {
                keys: RuntimeBtreeKeys::UniqueEncoded(parent_index_entries),
            }),
        );
        runtime.tables_mut().insert(
            "parent".to_string(),
            paged_row_source(vec![StoredRow {
                row_id: 1,
                values: vec![Value::Int64(7), Value::Int64(9)],
            }]),
        );
        runtime
            .tables_mut()
            .insert("child".to_string(), TableData { rows: vec![] }.into());
        runtime.mark_table_dirty("parent");

        let stmt =
            parse_sql_statement("INSERT INTO child (id, parent_a, parent_b) VALUES (1, 7, 9)")
                .unwrap();
        let insert = match stmt {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };
        let prepared = runtime
            .prepare_simple_insert(&insert)
            .expect("prepare insert")
            .expect("expected prepared insert");
        assert!(!prepared.use_generic_validation);

        let result = runtime
            .execute_prepared_simple_insert(&prepared, &[], 4096)
            .expect("execute prepared insert");
        assert_eq!(result.affected_rows(), 1);
        let rows = &runtime.tables.get("child").unwrap().resident_data().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            &rows[0].values,
            &[Value::Int64(1), Value::Int64(7), Value::Int64(9)]
        );
    }
}
