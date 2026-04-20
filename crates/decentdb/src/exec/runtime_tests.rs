//! Unit tests for exec module helpers added to improve coverage.

#[cfg(test)]
mod tests {
    use crate::exec::{
        append_table_payload, decode_table_payload, encode_table_payload,
        generated_columns_are_stored, map_get_ci, map_get_ci_mut, splice_updated_rows_payload,
        EngineRuntime, Int64IdentityHasher, Int64Map, PendingIndexInsert, PersistedTableState,
        RuntimeBtreeKey, RuntimeBtreeKeys, RuntimeRowIdSet, StoredRow, TableData,
    };
    use std::collections::BTreeMap;
    use std::hash::Hasher;

    use crate::catalog::{
        ColumnSchema, ColumnType, IndexColumn, IndexKind, IndexSchema, TableSchema,
    };
    use crate::record::value::Value;

    #[test]
    fn map_get_ci_case_insensitive() {
        let mut m: BTreeMap<String, i32> = BTreeMap::new();
        m.insert("KeyName".to_string(), 10);
        assert_eq!(map_get_ci(&m, "keyname"), Some(&10));
        assert_eq!(map_get_ci(&m, "KEYNAME"), Some(&10));
        assert_eq!(map_get_ci(&m, "notfound"), None);
    }

    #[test]
    fn map_get_ci_mut_and_modify() {
        let mut m: BTreeMap<String, i32> = BTreeMap::new();
        m.insert("abc".to_string(), 1);
        let slot = map_get_ci_mut(&mut m, "ABC").expect("expected slot");
        *slot = 42;
        assert_eq!(map_get_ci(&m, "abc"), Some(&42));
    }

    #[test]
    fn runtime_row_id_set_basic() {
        assert_eq!(RuntimeRowIdSet::Empty.len(), 0);
        assert!(RuntimeRowIdSet::Empty.is_empty());
        assert_eq!(RuntimeRowIdSet::Single(5).len(), 1);
        assert!(!RuntimeRowIdSet::Single(5).is_empty());
        let arr = [1_i64, 2, 3];
        let many = RuntimeRowIdSet::Many(&arr);
        assert_eq!(many.len(), 3);
        let mut collected = Vec::new();
        many.for_each(|v| collected.push(v));
        assert_eq!(collected, vec![1, 2, 3]);
    }

    #[test]
    fn runtime_btree_keys_basic() {
        // UniqueEncoded
        let mut map = BTreeMap::<Vec<u8>, i64>::new();
        map.insert(vec![1, 2, 3], 7);
        let keys = RuntimeBtreeKeys::UniqueEncoded(map);
        let key = RuntimeBtreeKey::Encoded(vec![1, 2, 3]);
        match keys.row_id_set_for_key(&key) {
            RuntimeRowIdSet::Single(v) => assert_eq!(v, 7),
            other => panic!("unexpected: {:?}", other),
        }
        assert_eq!(keys.row_ids_for_key(&key), vec![7]);
        assert!(keys.contains_any(&key));

        // insert duplicate into unique should error
        let mut keys2 = RuntimeBtreeKeys::UniqueEncoded(BTreeMap::new());
        assert!(keys2
            .insert_row_id(RuntimeBtreeKey::Encoded(vec![9, 9]), 1)
            .is_ok());
        assert!(keys2
            .insert_row_id(RuntimeBtreeKey::Encoded(vec![9, 9]), 2)
            .is_err());

        // NonUniqueEncoded
        let mut ne = BTreeMap::<Vec<u8>, Vec<i64>>::new();
        ne.insert(vec![4], vec![1, 2]);
        let keys3 = RuntimeBtreeKeys::NonUniqueEncoded(ne);
        let key4 = RuntimeBtreeKey::Encoded(vec![4]);
        match keys3.row_id_set_for_key(&key4) {
            RuntimeRowIdSet::Many(s) => assert_eq!(s, &[1, 2]),
            other => panic!("unexpected: {:?}", other),
        }
        assert_eq!(keys3.row_ids_for_key(&key4), vec![1, 2]);

        // UniqueInt64
        let mut ui: Int64Map<i64> = Int64Map::default();
        ui.insert(5, 33);
        let keys4 = RuntimeBtreeKeys::UniqueInt64(ui);
        let keyi = RuntimeBtreeKey::Int64(5);
        assert_eq!(keys4.row_ids_for_key(&keyi), vec![33]);

        // NonUniqueInt64
        let mut nui: Int64Map<Vec<i64>> = Int64Map::default();
        nui.insert(7, vec![100, 101]);
        let keys5 = RuntimeBtreeKeys::NonUniqueInt64(nui);
        let keyi2 = RuntimeBtreeKey::Int64(7);
        assert_eq!(keys5.row_ids_for_key(&keyi2), vec![100, 101]);

        // mismatch type errors
        let mut keys6 = RuntimeBtreeKeys::UniqueEncoded(BTreeMap::new());
        assert!(keys6.insert_row_id(RuntimeBtreeKey::Int64(10), 10).is_err());
        assert!(keys6
            .remove_row_id(&RuntimeBtreeKey::Int64(10), 10)
            .is_err());
    }

    #[test]
    fn row_ids_for_value_set_encodes() {
        // UniqueEncoded representation
        let mut map = BTreeMap::<Vec<u8>, i64>::new();
        map.insert(
            crate::record::key::encode_index_key(&Value::Int64(123)).unwrap(),
            55,
        );
        let ke = RuntimeBtreeKeys::UniqueEncoded(map);
        let v = Value::Int64(123);
        assert_eq!(ke.row_ids_for_value(&v).unwrap(), vec![55]);

        // UniqueInt64 representation
        let mut map2: Int64Map<i64> = Int64Map::default();
        map2.insert(123, 66);
        let keysii = RuntimeBtreeKeys::UniqueInt64(map2);
        assert_eq!(keysii.row_ids_for_value(&v).unwrap(), vec![66]);
    }

    #[test]
    fn table_data_row_index_and_row_by_id() {
        let td = TableData {
            rows: vec![
                StoredRow {
                    row_id: 1,
                    values: Vec::new(),
                },
                StoredRow {
                    row_id: 2,
                    values: Vec::new(),
                },
            ],
        };
        assert_eq!(td.row_index_by_id(1), Some(0));
        assert_eq!(td.row_by_id(2).unwrap().row_id, 2);

        let td2 = TableData {
            rows: vec![
                StoredRow {
                    row_id: 10,
                    values: Vec::new(),
                },
                StoredRow {
                    row_id: 20,
                    values: Vec::new(),
                },
                StoredRow {
                    row_id: 30,
                    values: Vec::new(),
                },
            ],
        };
        assert_eq!(td2.row_index_by_id(20), Some(1));

        let td3 = TableData {
            rows: vec![
                StoredRow {
                    row_id: 7,
                    values: Vec::new(),
                },
                StoredRow {
                    row_id: 3,
                    values: Vec::new(),
                },
                StoredRow {
                    row_id: 5,
                    values: Vec::new(),
                },
            ],
        };
        assert_eq!(td3.row_index_by_id(3), Some(1));
    }

    #[test]
    fn persisted_table_state_default() {
        let p = PersistedTableState::default();
        assert_eq!(p.row_count, 0);
        assert_eq!(p.checksum, 0);
    }

    #[test]
    fn int64_identity_hasher_behaviour() {
        let mut h = Int64IdentityHasher::default();
        h.write_i64(123);
        assert_eq!(h.finish(), 123);

        let mut h2 = Int64IdentityHasher::default();
        h2.write(&[1, 2, 3]);
        let expected = 1u64 | (2u64 << 8) | (3u64 << 16);
        assert_eq!(h2.finish(), expected);
    }

    #[test]
    fn generated_columns_are_stored_test() {
        let col = ColumnSchema {
            name: "a".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: Some("g()".to_string()),
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let table = TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![col.clone()],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        assert!(!generated_columns_are_stored(&table));
        let mut table2 = table.clone();
        table2.columns[0].generated_stored = true;
        assert!(generated_columns_are_stored(&table2));
    }

    #[test]
    fn runtime_btree_remove_mismatch_errors() {
        let mut ui: Int64Map<i64> = Int64Map::default();
        ui.insert(10, 99);
        let mut keys = RuntimeBtreeKeys::UniqueInt64(ui);
        assert!(keys.remove_row_id(&RuntimeBtreeKey::Int64(10), 98).is_err());
    }

    #[test]
    fn total_row_id_count_and_distinct_key_count_test() {
        // UniqueEncoded
        let mut map = BTreeMap::<Vec<u8>, i64>::new();
        map.insert(vec![1], 1);
        let keys = RuntimeBtreeKeys::UniqueEncoded(map);
        assert_eq!(keys.total_row_id_count(), 1);
        assert_eq!(keys.distinct_key_count(), 1);

        // NonUniqueEncoded
        let mut ne = BTreeMap::<Vec<u8>, Vec<i64>>::new();
        ne.insert(vec![2], vec![1, 2, 3]);
        let kn = RuntimeBtreeKeys::NonUniqueEncoded(ne);
        assert_eq!(kn.total_row_id_count(), 3);
        assert_eq!(kn.distinct_key_count(), 1);
    }

    #[test]
    fn try_execute_simple_count_query() {
        use crate::sql::parser::parse_sql_statement;

        let statement = parse_sql_statement("SELECT COUNT(*) FROM t").expect("parsed");
        let mut runtime = EngineRuntime::empty(1);

        let col = ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let table = TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![col],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        runtime.catalog_mut().tables.insert("t".to_string(), table);
        runtime.tables_mut().insert(
            "t".to_string(),
            TableData {
                rows: vec![
                    StoredRow {
                        row_id: 1,
                        values: Vec::new(),
                    },
                    StoredRow {
                        row_id: 2,
                        values: Vec::new(),
                    },
                ],
            },
        );

        let res = runtime
            .execute_read_statement(&statement, &[], 4096)
            .expect("execute");
        assert_eq!(res.rows().len(), 1);
        assert_eq!(
            res.rows().first().unwrap().values().first(),
            Some(&Value::Int64(2))
        );
    }

    #[test]
    fn mark_table_dirty_variants() {
        let mut runtime = EngineRuntime::empty(1);

        let col = ColumnSchema {
            name: "c".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let table = TableSchema {
            name: "x".to_string(),
            temporary: false,
            columns: vec![col.clone()],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };

        // mark_table_dirty
        runtime
            .catalog_mut()
            .tables
            .insert("x".to_string(), table.clone());
        runtime
            .tables_mut()
            .insert("x".to_string(), TableData::default());
        runtime.mark_table_dirty("x");
        assert!(runtime.dirty_tables.contains("x"));

        // mark_table_append_dirty and mark_table_row_dirty
        let mut runtime2 = EngineRuntime::empty(1);
        runtime2
            .catalog_mut()
            .tables
            .insert("x".to_string(), table.clone());
        runtime2
            .tables_mut()
            .insert("x".to_string(), TableData::default());
        runtime2.mark_table_append_dirty("x");
        assert!(runtime2.append_only_dirty_tables.contains("x"));
        runtime2.mark_table_row_dirty("x", 3);
        // append-only should have been escalated and not converted to row-update
        assert!(!runtime2.append_only_dirty_tables.contains("x"));
        assert!(!runtime2.row_update_dirty.contains_key("x"));

        // mark_all_tables_dirty
        let mut runtime3 = EngineRuntime::empty(1);
        runtime3
            .catalog_mut()
            .tables
            .insert("a".to_string(), table.clone());
        runtime3
            .catalog_mut()
            .tables
            .insert("b".to_string(), table.clone());
        runtime3.mark_all_tables_dirty();
        assert!(runtime3.dirty_tables.contains("a"));
        assert!(runtime3.dirty_tables.contains("b"));
    }

    #[test]
    fn bump_temp_schema_cookie_wrap() {
        let mut runtime = EngineRuntime::empty(0);
        runtime.temp_schema_cookie = u32::MAX;
        runtime.bump_temp_schema_cookie();
        assert_eq!(runtime.temp_schema_cookie, 1);
        runtime.bump_temp_schema_cookie();
        assert_eq!(runtime.temp_schema_cookie, 2);
    }

    #[test]
    fn persistent_resolution_runtime_clears_temps() {
        let mut runtime = EngineRuntime::empty(1);
        let col = ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let table = TableSchema {
            name: "temp".to_string(),
            temporary: true,
            columns: vec![col.clone()],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        runtime
            .temp_tables_mut()
            .insert("temp".to_string(), table.clone());
        runtime
            .temp_table_data_map_mut()
            .insert("temp".to_string(), TableData::default());
        runtime.temp_views_mut().insert(
            "v".to_string(),
            crate::catalog::ViewSchema {
                name: "v".to_string(),
                temporary: true,
                sql_text: "".to_string(),
                column_names: Vec::new(),
                dependencies: Vec::new(),
            },
        );

        let persistent = runtime.persistent_resolution_runtime();
        assert!(persistent.temp_tables.is_empty());
        assert!(persistent.temp_table_data.is_empty());
        assert!(persistent.temp_views.is_empty());
    }

    #[test]
    fn planner_catalog_merges_temp_tables_and_views() {
        let mut runtime = EngineRuntime::empty(1);
        let col = ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let table = TableSchema {
            name: "t".to_string(),
            temporary: true,
            columns: vec![col.clone()],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        runtime
            .temp_tables_mut()
            .insert("t".to_string(), table.clone());
        runtime.temp_views_mut().insert(
            "v".to_string(),
            crate::catalog::ViewSchema {
                name: "v".to_string(),
                temporary: true,
                sql_text: "".to_string(),
                column_names: Vec::new(),
                dependencies: Vec::new(),
            },
        );

        let catalog = runtime.planner_catalog();
        assert!(catalog.table("t").is_some());
        assert!(catalog.view("v").is_some());
    }

    #[test]
    fn encode_decode_table_payload_roundtrip() {
        let mut td = TableData::default();
        td.rows.push(StoredRow {
            row_id: 1,
            values: vec![Value::Int64(10), Value::Text("x".to_string())],
        });
        td.rows.push(StoredRow {
            row_id: 2,
            values: vec![Value::Null],
        });
        let payload = encode_table_payload(&td).expect("encode");
        assert!(!payload.is_empty());
        let decoded = decode_table_payload(&payload).expect("decode");
        assert_eq!(decoded.rows.len(), 2);
        assert_eq!(decoded.rows[0].row_id, 1);
        assert_eq!(decoded.rows[0].values[0], Value::Int64(10));
    }

    #[test]
    fn append_table_payload_appends_new_rows() {
        let mut base = TableData::default();
        base.rows.push(StoredRow {
            row_id: 1,
            values: vec![Value::Int64(1)],
        });
        let mut extended = base.clone();
        extended.rows.push(StoredRow {
            row_id: 2,
            values: vec![Value::Int64(2)],
        });
        let previous = encode_table_payload(&base).expect("encode base");
        let new_payload = append_table_payload(previous, &extended).expect("append");
        let decoded = decode_table_payload(&new_payload).expect("decode appended");
        assert_eq!(decoded.rows.len(), 2);
        assert_eq!(decoded.rows[1].row_id, 2);
    }

    #[test]
    fn splice_updated_rows_payload_falls_back_on_bad_header() {
        let mut data = TableData::default();
        data.rows.push(StoredRow {
            row_id: 1,
            values: vec![Value::Int64(1)],
        });
        let old = vec![0u8, 1, 2]; // invalid short header
        let res = splice_updated_rows_payload(&old, &data, &[]).expect("splice fallback");
        let decoded = decode_table_payload(&res.payload).expect("decode");
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(res.first_dirty_byte, 0);
    }

    #[test]
    fn mark_table_dirty_and_row_append_behaviour() {
        let mut runtime = EngineRuntime::empty(1);
        // create a non-temporary table schema and insert into catalog
        let col = ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let t = TableSchema {
            name: "t".to_string(),
            temporary: false,
            columns: vec![col.clone()],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        runtime.catalog_mut().tables.insert("t".to_string(), t);
        let mut v = runtime
            .catalog
            .tables
            .get("t")
            .cloned()
            .expect("expected test table");
        v.name = "v".to_string();
        runtime.catalog_mut().tables.insert("v".to_string(), v);

        runtime.mark_table_row_dirty("t", 3);
        assert!(runtime.row_update_dirty.contains_key("t"));

        // If a table is already fully dirty (and not append-only) then marking a row dirty is a no-op
        runtime.dirty_tables.insert("w".to_string());
        runtime.mark_table_row_dirty("w", 5);
        assert!(!runtime.row_update_dirty.contains_key("w"));

        // Append-only dirty on a fresh table
        runtime.mark_table_append_dirty("u"); // no-op since u doesn't exist; should not panic
        runtime.mark_table_append_dirty("t");
        // t was already row-update-dirty, so append-only should have no effect
        assert!(runtime.dirty_tables.contains("t"));

        // Now test escalation when append-only present
        runtime.append_only_dirty_tables.insert("v".to_string());
        runtime.mark_table_row_dirty("v", 1);
        assert!(!runtime.append_only_dirty_tables.contains("v"));
    }

    #[test]
    fn apply_insert_index_updates_returns_error_for_missing_index() {
        let mut runtime = EngineRuntime::empty(2);
        let updates = vec![PendingIndexInsert::Trigram {
            name: "missing_idx".to_string(),
            row_id: 1,
            text: "x".to_string(),
        }];
        let res = runtime.apply_insert_index_updates(updates);
        assert!(res.is_err());
    }

    #[test]
    fn group_by_aggregates() {
        use crate::sql::parser::parse_sql_statement;

        let statement = parse_sql_statement(
            "SELECT g, COUNT(*) AS c, SUM(v) AS s FROM grp GROUP BY g ORDER BY g",
        )
        .expect("parsed");
        let mut runtime = EngineRuntime::empty(1);

        let col_g = ColumnSchema {
            name: "g".to_string(),
            column_type: ColumnType::Text,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let col_v = ColumnSchema {
            name: "v".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let table = TableSchema {
            name: "grp".to_string(),
            temporary: false,
            columns: vec![col_g.clone(), col_v.clone()],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        runtime
            .catalog_mut()
            .tables
            .insert("grp".to_string(), table);
        runtime.tables_mut().insert(
            "grp".to_string(),
            TableData {
                rows: vec![
                    StoredRow {
                        row_id: 1,
                        values: vec![Value::Text("a".to_string()), Value::Int64(10)],
                    },
                    StoredRow {
                        row_id: 2,
                        values: vec![Value::Text("a".to_string()), Value::Int64(5)],
                    },
                    StoredRow {
                        row_id: 3,
                        values: vec![Value::Text("b".to_string()), Value::Int64(7)],
                    },
                ],
            },
        );

        let res = runtime
            .execute_read_statement(&statement, &[], 4096)
            .expect("execute");
        assert_eq!(res.rows().len(), 2);
        let row0 = &res.rows()[0];
        assert_eq!(row0.values().first(), Some(&Value::Text("a".to_string())));
        assert_eq!(row0.values().get(1), Some(&Value::Int64(2)));
        assert_eq!(row0.values().get(2), Some(&Value::Int64(15)));
    }

    #[test]
    fn distinct_order_limit_offset() {
        use crate::sql::parser::parse_sql_statement;
        let statement =
            parse_sql_statement("SELECT DISTINCT v FROM t2 ORDER BY v LIMIT 1 OFFSET 1")
                .expect("parsed");
        let mut runtime = EngineRuntime::empty(1);
        let col = ColumnSchema {
            name: "v".to_string(),
            column_type: ColumnType::Text,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let table = TableSchema {
            name: "t2".to_string(),
            temporary: false,
            columns: vec![col.clone()],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        runtime.catalog_mut().tables.insert("t2".to_string(), table);
        runtime.tables_mut().insert(
            "t2".to_string(),
            TableData {
                rows: vec![
                    StoredRow {
                        row_id: 1,
                        values: vec![Value::Text("x".to_string())],
                    },
                    StoredRow {
                        row_id: 2,
                        values: vec![Value::Text("y".to_string())],
                    },
                    StoredRow {
                        row_id: 3,
                        values: vec![Value::Text("x".to_string())],
                    },
                ],
            },
        );
        let res = runtime
            .execute_read_statement(&statement, &[], 4096)
            .expect("execute");
        assert_eq!(res.rows().len(), 1);
        assert_eq!(
            res.rows()[0].values().first(),
            Some(&Value::Text("y".to_string()))
        );
    }

    #[test]
    fn simple_indexed_join_projection_query_positive() {
        use crate::sql::parser::parse_sql_statement;

        let mut runtime = EngineRuntime::empty(1);

        let col_a_id = ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let col_a_filter = ColumnSchema {
            name: "filter".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let table_a = TableSchema {
            name: "a".to_string(),
            temporary: false,
            columns: vec![col_a_id.clone(), col_a_filter.clone()],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
        };
        runtime
            .catalog_mut()
            .tables
            .insert("a".to_string(), table_a);
        runtime.tables_mut().insert(
            "a".to_string(),
            TableData {
                rows: vec![
                    StoredRow {
                        row_id: 1,
                        values: vec![Value::Int64(1), Value::Int64(100)],
                    },
                    StoredRow {
                        row_id: 2,
                        values: vec![Value::Int64(2), Value::Int64(200)],
                    },
                ],
            },
        );

        let col_b_id = ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let col_b_ref = ColumnSchema {
            name: "ref".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let col_b_payload = ColumnSchema {
            name: "payload".to_string(),
            column_type: ColumnType::Text,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: false,
            unique: false,
            auto_increment: false,
            checks: Vec::new(),
            foreign_key: None,
        };
        let table_b = TableSchema {
            name: "b".to_string(),
            temporary: false,
            columns: vec![col_b_id.clone(), col_b_ref.clone(), col_b_payload.clone()],
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 10,
        };
        runtime
            .catalog_mut()
            .tables
            .insert("b".to_string(), table_b);
        runtime.tables_mut().insert(
            "b".to_string(),
            TableData {
                rows: vec![
                    StoredRow {
                        row_id: 10,
                        values: vec![
                            Value::Int64(10),
                            Value::Int64(1),
                            Value::Text("p1".to_string()),
                        ],
                    },
                    StoredRow {
                        row_id: 20,
                        values: vec![
                            Value::Int64(20),
                            Value::Int64(2),
                            Value::Text("p2".to_string()),
                        ],
                    },
                ],
            },
        );

        let idx_a = IndexSchema {
            name: "a_filter_idx".to_string(),
            table_name: "a".to_string(),
            kind: IndexKind::Btree,
            unique: false,
            columns: vec![IndexColumn {
                column_name: Some("filter".to_string()),
                expression_sql: None,
            }],
            include_columns: vec![],
            predicate_sql: None,
            fresh: true,
        };
        let idx_b = IndexSchema {
            name: "b_ref_idx".to_string(),
            table_name: "b".to_string(),
            kind: IndexKind::Btree,
            unique: false,
            columns: vec![IndexColumn {
                column_name: Some("ref".to_string()),
                expression_sql: None,
            }],
            include_columns: vec![],
            predicate_sql: None,
            fresh: true,
        };
        runtime
            .catalog_mut()
            .indexes
            .insert(idx_a.name.clone(), idx_a);
        runtime
            .catalog_mut()
            .indexes
            .insert(idx_b.name.clone(), idx_b);

        let _ = runtime.rebuild_indexes(4096);

        let stmt = parse_sql_statement(
            "SELECT a.filter, b.payload FROM a JOIN b ON a.id = b.ref WHERE a.filter = 100",
        )
        .expect("parsed");
        let res = runtime
            .execute_read_statement(&stmt, &[], 4096)
            .expect("execute");
        assert_eq!(res.rows().len(), 1);
        assert_eq!(res.rows()[0].values().first(), Some(&Value::Int64(100)));
        assert_eq!(
            res.rows()[0].values().get(1),
            Some(&Value::Text("p1".to_string()))
        );
    }
}
