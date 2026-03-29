//! Unit tests for exec module helpers to increase coverage.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::record::value::Value;
    use std::collections::BTreeMap;

    #[test]
    fn map_get_ci_and_mut() {
        let mut map: BTreeMap<String, i32> = BTreeMap::new();
        map.insert("AbC".to_string(), 1);
        assert_eq!(map_get_ci(&map, "abc").copied(), Some(1));

        let mut map2: BTreeMap<String, i32> = BTreeMap::new();
        map2.insert("Xy".to_string(), 2);
        if let Some(v) = map_get_ci_mut(&mut map2, "xy") {
            *v = 3;
        }
        assert_eq!(map2.get("Xy"), Some(&3));
    }

    #[test]
    fn tabledata_row_index_and_by_id() {
        let mut td = TableData::default();
        td.rows.push(StoredRow {
            row_id: 1,
            values: vec![],
        });
        td.rows.push(StoredRow {
            row_id: 3,
            values: vec![],
        });
        assert_eq!(td.row_index_by_id(1), Some(0));
        assert_eq!(td.row_index_by_id(3), Some(1));
        assert_eq!(td.row_by_id(3).unwrap().row_id, 3);

        // binary_search path: non-zero offset
        let mut td2 = TableData::default();
        td2.rows.push(StoredRow {
            row_id: 5,
            values: vec![],
        });
        td2.rows.push(StoredRow {
            row_id: 10,
            values: vec![],
        });
        assert_eq!(td2.row_index_by_id(10), Some(1));
    }

    #[test]
    fn int64_identity_hasher_and_rowidset() {
        let mut h = Int64IdentityHasher::default();
        h.write_i64(42);
        assert_eq!(h.finish(), 42);

        let mut h2 = Int64IdentityHasher::default();
        h2.write(&[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_ne!(h2.finish(), 0);

        assert_eq!(RuntimeRowIdSet::Empty.len(), 0);
        assert!(RuntimeRowIdSet::Empty.is_empty());
        assert_eq!(RuntimeRowIdSet::Single(10).len(), 1);
        let mut seen = vec![];
        RuntimeRowIdSet::Many(&[1, 2, 3]).for_each(|id| seen.push(id));
        assert_eq!(seen, vec![1, 2, 3]);
    }

    #[test]
    fn runtime_btree_keys_encoded_unique_operations() {
        let mut keys_map: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
        let key = vec![1u8, 2, 3];
        keys_map.insert(key.clone(), 7);
        let r = RuntimeBtreeKeys::UniqueEncoded(keys_map);
        assert_eq!(
            r.row_ids_for_key(&RuntimeBtreeKey::Encoded(key.clone())),
            vec![7]
        );
        assert!(r.contains_any(&RuntimeBtreeKey::Encoded(key.clone())));

        // insert duplicate -> error
        let mut r2 = RuntimeBtreeKeys::UniqueEncoded(BTreeMap::new());
        r2.insert_row_id(RuntimeBtreeKey::Encoded(vec![9]), 1)
            .unwrap();
        assert!(r2
            .insert_row_id(RuntimeBtreeKey::Encoded(vec![9]), 2)
            .is_err());

        // type mismatch -> error
        let mut r3 = RuntimeBtreeKeys::UniqueEncoded(BTreeMap::new());
        assert!(r3.insert_row_id(RuntimeBtreeKey::Int64(1), 1).is_err());
    }

    #[test]
    fn remove_row_id_unique_and_nonunique_behaviour() {
        let mut keys_map: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
        keys_map.insert(vec![1], 99);
        let mut rt = RuntimeBtreeKeys::UniqueEncoded(keys_map);
        // mismatch
        assert!(rt
            .remove_row_id(&RuntimeBtreeKey::Encoded(vec![1]), 1)
            .is_err());
        // correct remove
        rt.remove_row_id(&RuntimeBtreeKey::Encoded(vec![1]), 99)
            .unwrap();
        assert!(!rt.contains_any(&RuntimeBtreeKey::Encoded(vec![1])));

        let mut keys2: BTreeMap<Vec<u8>, Vec<i64>> = BTreeMap::new();
        keys2.insert(vec![2], vec![1, 2]);
        let mut rt2 = RuntimeBtreeKeys::NonUniqueEncoded(keys2);
        rt2.remove_row_id(&RuntimeBtreeKey::Encoded(vec![2]), 1)
            .unwrap();
        assert!(rt2.contains_any(&RuntimeBtreeKey::Encoded(vec![2])));
        rt2.remove_row_id(&RuntimeBtreeKey::Encoded(vec![2]), 2)
            .unwrap();
        assert!(!rt2.contains_any(&RuntimeBtreeKey::Encoded(vec![2])));
    }

    #[test]
    fn unique_int64_row_ids_for_key_and_value() {
        let mut m: Int64Map<i64> = Int64Map::default();
        m.insert(9, 33);
        let rt = RuntimeBtreeKeys::UniqueInt64(m);
        assert_eq!(rt.row_ids_for_key(&RuntimeBtreeKey::Int64(9)), vec![33]);
        assert_eq!(rt.row_ids_for_value(&Value::Int64(9)).unwrap(), vec![33]);
    }
}
