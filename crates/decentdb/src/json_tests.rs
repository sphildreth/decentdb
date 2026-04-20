//! Unit tests for JSON parsing utilities.

#[cfg(test)]
mod tests {
    use crate::json::{parse_json, parse_json_path, JsonPathSegment, JsonValue};
    use std::collections::BTreeMap;

    #[test]
    fn test_parse_json_null() {
        let result = parse_json("null").unwrap();
        assert_eq!(result, JsonValue::Null);
    }

    #[test]
    fn test_parse_json_true() {
        let result = parse_json("true").unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_parse_json_false() {
        let result = parse_json("false").unwrap();
        assert_eq!(result, JsonValue::Bool(false));
    }

    #[test]
    fn test_parse_json_integer() {
        let result = parse_json("42").unwrap();
        assert_eq!(result, JsonValue::Number("42".to_string()));
    }

    #[test]
    fn test_parse_json_negative_integer() {
        let result = parse_json("-123").unwrap();
        assert_eq!(result, JsonValue::Number("-123".to_string()));
    }

    #[test]
    fn test_parse_json_float() {
        let result = parse_json("3.14159").unwrap();
        assert_eq!(result, JsonValue::Number("3.14159".to_string()));
    }

    #[test]
    fn test_parse_json_negative_float() {
        let result = parse_json("-2.718").unwrap();
        assert_eq!(result, JsonValue::Number("-2.718".to_string()));
    }

    #[test]
    fn test_parse_json_empty_string() {
        let result = parse_json("\"\"").unwrap();
        assert_eq!(result, JsonValue::String("".to_string()));
    }

    #[test]
    fn test_parse_json_simple_string() {
        let result = parse_json("\"hello\"").unwrap();
        assert_eq!(result, JsonValue::String("hello".to_string()));
    }

    #[test]
    fn test_parse_json_string_with_escapes() {
        let result = parse_json("\"hello\\nworld\"").unwrap();
        assert_eq!(result, JsonValue::String("hello\nworld".to_string()));
    }

    #[test]
    fn test_parse_json_string_with_tab() {
        let result = parse_json("\"tab\\there\"").unwrap();
        assert_eq!(result, JsonValue::String("tab\there".to_string()));
    }

    #[test]
    fn test_parse_json_string_with_carriage_return() {
        let result = parse_json("\"line\\rbreak\"").unwrap();
        assert_eq!(result, JsonValue::String("line\rbreak".to_string()));
    }

    #[test]
    fn test_parse_json_string_with_backslash() {
        let result = parse_json("\"path\\\\to\\\\file\"").unwrap();
        assert_eq!(result, JsonValue::String("path\\to\\file".to_string()));
    }

    #[test]
    fn test_parse_json_string_with_quote() {
        let result = parse_json("\"she said \\\"hello\\\"\"").unwrap();
        assert_eq!(result, JsonValue::String("she said \"hello\"".to_string()));
    }

    #[test]
    fn test_parse_json_string_with_slash_escape() {
        let result = parse_json("\"path\\/to\\/file\"").unwrap();
        assert_eq!(result, JsonValue::String("path/to/file".to_string()));
    }

    #[test]
    fn test_parse_json_empty_array() {
        let result = parse_json("[]").unwrap();
        assert_eq!(result, JsonValue::Array(vec![]));
    }

    #[test]
    fn test_parse_json_single_element_array() {
        let result = parse_json("[1]").unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![JsonValue::Number("1".to_string())])
        );
    }

    #[test]
    fn test_parse_json_mixed_array() {
        let result = parse_json("[1, \"two\", true, null]").unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Number("1".to_string()),
                JsonValue::String("two".to_string()),
                JsonValue::Bool(true),
                JsonValue::Null,
            ])
        );
    }

    #[test]
    fn test_parse_json_nested_array() {
        let result = parse_json("[[1, 2], [3, 4]]").unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Array(vec![
                    JsonValue::Number("1".to_string()),
                    JsonValue::Number("2".to_string()),
                ]),
                JsonValue::Array(vec![
                    JsonValue::Number("3".to_string()),
                    JsonValue::Number("4".to_string()),
                ]),
            ])
        );
    }

    #[test]
    fn test_parse_json_empty_object() {
        let result = parse_json("{}").unwrap();
        assert_eq!(result, JsonValue::Object(BTreeMap::new()));
    }

    #[test]
    fn test_parse_json_single_key_object() {
        let result = parse_json("{\"key\": \"value\"}").unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("key".to_string(), JsonValue::String("value".to_string()));
        assert_eq!(result, JsonValue::Object(expected));
    }

    #[test]
    fn test_parse_json_multi_key_object() {
        let result = parse_json("{\"a\": 1, \"b\": 2}").unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("a".to_string(), JsonValue::Number("1".to_string()));
        expected.insert("b".to_string(), JsonValue::Number("2".to_string()));
        assert_eq!(result, JsonValue::Object(expected));
    }

    #[test]
    fn test_parse_json_nested_object() {
        let result = parse_json("{\"outer\": {\"inner\": true}}").unwrap();
        let mut inner = BTreeMap::new();
        inner.insert("inner".to_string(), JsonValue::Bool(true));
        let mut expected = BTreeMap::new();
        expected.insert("outer".to_string(), JsonValue::Object(inner));
        assert_eq!(result, JsonValue::Object(expected));
    }

    #[test]
    fn test_parse_json_complex_structure() {
        let json = r#"{"name": "test", "values": [1, 2, 3], "nested": {"flag": true}}"#;
        let result = parse_json(json).unwrap();

        let mut nested = BTreeMap::new();
        nested.insert("flag".to_string(), JsonValue::Bool(true));

        let mut expected = BTreeMap::new();
        expected.insert("name".to_string(), JsonValue::String("test".to_string()));
        expected.insert(
            "values".to_string(),
            JsonValue::Array(vec![
                JsonValue::Number("1".to_string()),
                JsonValue::Number("2".to_string()),
                JsonValue::Number("3".to_string()),
            ]),
        );
        expected.insert("nested".to_string(), JsonValue::Object(nested));

        assert_eq!(result, JsonValue::Object(expected));
    }

    #[test]
    fn test_parse_json_with_whitespace() {
        let json = r#"  {  "key"  :  "value"  }  "#;
        let result = parse_json(json).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("key".to_string(), JsonValue::String("value".to_string()));
        assert_eq!(result, JsonValue::Object(expected));
    }

    #[test]
    fn test_parse_json_invalid_empty() {
        let result = parse_json("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("JSON"));
    }

    #[test]
    fn test_parse_json_invalid_trailing_content() {
        let result = parse_json("123abc");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("trailing"));
    }

    #[test]
    fn test_parse_json_invalid_unterminated_string() {
        let result = parse_json("\"hello");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unterminated"));
    }

    #[test]
    fn test_parse_json_invalid_unterminated_array() {
        let result = parse_json("[1, 2");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_invalid_unterminated_object() {
        let result = parse_json("{\"key\": \"value\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_invalid_number_no_digits() {
        let result = parse_json("-");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_invalid_float_no_fractional() {
        let result = parse_json("1.");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_invalid_escape() {
        let result = parse_json("\"\\x\"");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("escape"));
    }

    #[test]
    fn test_parse_json_invalid_literal() {
        let result = parse_json("tru");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_path_simple() {
        let result = parse_json_path("$.key").unwrap();
        assert_eq!(result, vec![JsonPathSegment::Key("key".to_string())]);
    }

    #[test]
    fn test_parse_json_path_nested() {
        let result = parse_json_path("$.outer.inner").unwrap();
        assert_eq!(
            result,
            vec![
                JsonPathSegment::Key("outer".to_string()),
                JsonPathSegment::Key("inner".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_json_path_array_index() {
        let result = parse_json_path("$[0]").unwrap();
        assert_eq!(result, vec![JsonPathSegment::Index(0)]);
    }

    #[test]
    fn test_parse_json_path_mixed() {
        let result = parse_json_path("$.items[2].name").unwrap();
        assert_eq!(
            result,
            vec![
                JsonPathSegment::Key("items".to_string()),
                JsonPathSegment::Index(2),
                JsonPathSegment::Key("name".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_json_path_root_only() {
        let result = parse_json_path("$").unwrap();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_parse_json_path_invalid_no_root() {
        let result = parse_json_path("key");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid JSON path"));
    }

    #[test]
    fn test_parse_json_path_invalid_empty_brackets() {
        let result = parse_json_path("$[]");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_path_invalid_non_numeric_index() {
        let result = parse_json_path("$[abc]");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_json_path_invalid_unclosed_bracket() {
        let result = parse_json_path("$[0");
        assert!(result.is_err());
    }

    #[test]
    fn test_json_value_lookup_object() {
        let mut obj = BTreeMap::new();
        obj.insert("key".to_string(), JsonValue::String("value".to_string()));
        let value = JsonValue::Object(obj);

        let path = vec![JsonPathSegment::Key("key".to_string())];
        let result = value.lookup(&path).unwrap();
        assert_eq!(result, &JsonValue::String("value".to_string()));
    }

    #[test]
    fn test_json_value_lookup_array() {
        let array = JsonValue::Array(vec![
            JsonValue::Number("1".to_string()),
            JsonValue::Number("2".to_string()),
            JsonValue::Number("3".to_string()),
        ]);

        let path = vec![JsonPathSegment::Index(1)];
        let result = array.lookup(&path).unwrap();
        assert_eq!(result, &JsonValue::Number("2".to_string()));
    }

    #[test]
    fn test_json_value_lookup_nested() {
        let mut inner = BTreeMap::new();
        inner.insert("name".to_string(), JsonValue::String("test".to_string()));

        let mut outer = BTreeMap::new();
        outer.insert("data".to_string(), JsonValue::Object(inner));

        let value = JsonValue::Object(outer);

        let path = vec![
            JsonPathSegment::Key("data".to_string()),
            JsonPathSegment::Key("name".to_string()),
        ];
        let result = value.lookup(&path).unwrap();
        assert_eq!(result, &JsonValue::String("test".to_string()));
    }

    #[test]
    fn test_json_value_lookup_missing_key() {
        let obj = BTreeMap::new();
        let value = JsonValue::Object(obj);

        let path = vec![JsonPathSegment::Key("missing".to_string())];
        let result = value.lookup(&path);
        assert!(result.is_none());
    }

    #[test]
    fn test_json_value_lookup_out_of_bounds() {
        let array = JsonValue::Array(vec![JsonValue::Number("1".to_string())]);

        let path = vec![JsonPathSegment::Index(5)];
        let result = array.lookup(&path);
        assert!(result.is_none());
    }

    #[test]
    fn test_json_value_lookup_type_mismatch() {
        let value = JsonValue::Object(BTreeMap::new());

        let path = vec![JsonPathSegment::Index(0)];
        let result = value.lookup(&path);
        assert!(result.is_none());
    }

    #[test]
    fn test_json_value_render_null() {
        let result = JsonValue::Null.render_json();
        assert_eq!(result, "null");
    }

    #[test]
    fn test_json_value_render_bool() {
        assert_eq!(JsonValue::Bool(true).render_json(), "true");
        assert_eq!(JsonValue::Bool(false).render_json(), "false");
    }

    #[test]
    fn test_json_value_render_number() {
        assert_eq!(JsonValue::Number("42".to_string()).render_json(), "42");
        assert_eq!(
            JsonValue::Number("-3.14".to_string()).render_json(),
            "-3.14"
        );
    }

    #[test]
    fn test_json_value_render_string() {
        assert_eq!(
            JsonValue::String("hello".to_string()).render_json(),
            "\"hello\""
        );
    }

    #[test]
    fn test_json_value_render_string_with_escapes() {
        assert_eq!(
            JsonValue::String("hello\nworld".to_string()).render_json(),
            "\"hello\\nworld\""
        );
        assert_eq!(
            JsonValue::String("quote\"here".to_string()).render_json(),
            "\"quote\\\"here\""
        );
    }

    #[test]
    fn test_json_value_render_empty_array() {
        assert_eq!(JsonValue::Array(vec![]).render_json(), "[]");
    }

    #[test]
    fn test_json_value_render_array() {
        let array = JsonValue::Array(vec![
            JsonValue::Number("1".to_string()),
            JsonValue::String("two".to_string()),
        ]);
        assert_eq!(array.render_json(), "[1,\"two\"]");
    }

    #[test]
    fn test_json_value_render_empty_object() {
        assert_eq!(JsonValue::Object(BTreeMap::new()).render_json(), "{}");
    }

    #[test]
    fn test_json_value_render_object() {
        let mut obj = BTreeMap::new();
        obj.insert("a".to_string(), JsonValue::Number("1".to_string()));
        obj.insert("b".to_string(), JsonValue::String("test".to_string()));
        let result = JsonValue::Object(obj).render_json();
        assert_eq!(result, "{\"a\":1,\"b\":\"test\"}");
    }

    #[test]
    fn test_json_roundtrip_simple_object() {
        let json_str = r#"{"key":"value"}"#;
        let parsed = parse_json(json_str).unwrap();
        let rendered = parsed.render_json();
        let reparsed = parse_json(&rendered).unwrap();
        assert_eq!(parsed, reparsed);
    }

    #[test]
    fn test_json_roundtrip_array() {
        let json_str = r#"[1,2,3]"#;
        let parsed = parse_json(json_str).unwrap();
        let rendered = parsed.render_json();
        let reparsed = parse_json(&rendered).unwrap();
        assert_eq!(parsed, reparsed);
    }

    #[test]
    fn test_json_roundtrip_nested() {
        let json_str = r#"{"a":{"b":[1,2,3]}}"#;
        let parsed = parse_json(json_str).unwrap();
        let rendered = parsed.render_json();
        let reparsed = parse_json(&rendered).unwrap();
        assert_eq!(parsed, reparsed);
    }
}

// ── WalIndex unit tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod wal_index_tests {
    use std::sync::Arc;

    use crate::wal::index::{WalIndex, WalVersion};

    fn ver(lsn: u64, byte: u8) -> WalVersion {
        WalVersion {
            lsn,
            data: Arc::<[u8]>::from(vec![byte; 16]),
        }
    }

    #[test]
    fn version_count_is_zero_for_fresh_index() {
        assert_eq!(WalIndex::default().version_count(), 0);
    }

    #[test]
    fn add_with_retain_history_accumulates_versions() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(10, 0xAA), true);
        idx.add_version(1, ver(20, 0xBB), true);
        assert_eq!(idx.version_count(), 2);
    }

    #[test]
    fn add_without_retain_history_replaces_all_existing_versions() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(10, 0xAA), true);
        idx.add_version(1, ver(20, 0xBB), true);
        idx.add_version(1, ver(30, 0xCC), false);
        assert_eq!(idx.version_count(), 1);
        let v = idx.latest_visible(1, u64::MAX).unwrap();
        assert_eq!(v.data[0], 0xCC);
        assert_eq!(v.lsn, 30);
    }

    #[test]
    fn latest_visible_returns_most_recent_version_at_or_before_snapshot() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(10, 0x11), true);
        idx.add_version(1, ver(20, 0x22), true);
        idx.add_version(1, ver(30, 0x33), true);

        let v = idx.latest_visible(1, 15).unwrap();
        assert_eq!(v.lsn, 10);
        assert_eq!(v.data[0], 0x11);

        let v = idx.latest_visible(1, 20).unwrap();
        assert_eq!(v.lsn, 20);
        assert_eq!(v.data[0], 0x22);

        let v = idx.latest_visible(1, 100).unwrap();
        assert_eq!(v.lsn, 30);
        assert_eq!(v.data[0], 0x33);
    }

    #[test]
    fn latest_visible_returns_none_when_snapshot_is_before_all_versions() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(20, 0xAA), true);
        assert!(idx.latest_visible(1, 19).is_none());
        // Exactly at the boundary is visible.
        assert!(idx.latest_visible(1, 20).is_some());
    }

    #[test]
    fn latest_visible_returns_none_for_unknown_page_id() {
        let idx = WalIndex::default();
        assert!(idx.latest_visible(99, u64::MAX).is_none());
    }

    #[test]
    fn version_count_tracks_multiple_independent_pages() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(10, 0x11), true);
        idx.add_version(2, ver(20, 0x22), true);
        idx.add_version(3, ver(30, 0x33), true);
        idx.add_version(1, ver(40, 0x44), true);
        assert_eq!(idx.version_count(), 4);
    }

    #[test]
    fn clear_removes_all_versions_and_pages() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(10, 0xAA), true);
        idx.add_version(2, ver(20, 0xBB), true);
        idx.clear();
        assert_eq!(idx.version_count(), 0);
        assert!(idx.latest_visible(1, u64::MAX).is_none());
        assert!(idx.latest_visible(2, u64::MAX).is_none());
    }

    #[test]
    fn prune_at_or_below_removes_versions_at_and_below_lsn() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(10, 0x11), true);
        idx.add_version(1, ver(20, 0x22), true);
        idx.add_version(1, ver(30, 0x33), true);
        idx.prune_at_or_below(&[1], 20);
        // lsn=10 and lsn=20 are pruned; lsn=30 remains.
        assert_eq!(idx.version_count(), 1);
        assert!(idx.latest_visible(1, 20).is_none());
        let v = idx.latest_visible(1, u64::MAX).unwrap();
        assert_eq!(v.lsn, 30);
    }

    #[test]
    fn prune_removes_page_entry_when_all_versions_are_pruned() {
        let mut idx = WalIndex::default();
        idx.add_version(5, ver(10, 0xAA), true);
        idx.prune_at_or_below(&[5], 10);
        assert_eq!(idx.version_count(), 0);
        assert!(idx.latest_visible(5, u64::MAX).is_none());
    }

    #[test]
    fn prune_does_not_touch_pages_absent_from_list() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(10, 0x11), true);
        idx.add_version(2, ver(10, 0x22), true);
        idx.prune_at_or_below(&[1], 10);
        assert_eq!(idx.version_count(), 1);
        assert!(idx.latest_visible(2, u64::MAX).is_some());
    }

    #[test]
    fn latest_versions_at_or_before_returns_latest_per_page_up_to_lsn() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(10, 0x11), true);
        idx.add_version(1, ver(20, 0x22), true);
        idx.add_version(2, ver(15, 0x33), true);
        idx.add_version(3, ver(25, 0x44), true);

        // safe_lsn=20 includes page 1 @ lsn=20 and page 2 @ lsn=15 but not page 3 @ lsn=25.
        let versions = idx.latest_versions_at_or_before(20);
        assert_eq!(versions.len(), 2);
        // Result is sorted by page_id.
        assert_eq!(versions[0].0, 1);
        assert_eq!(versions[0].1.lsn, 20);
        assert_eq!(versions[1].0, 2);
        assert_eq!(versions[1].1.lsn, 15);
    }

    #[test]
    fn latest_versions_at_or_before_returns_empty_when_all_lsns_exceed_safe_lsn() {
        let mut idx = WalIndex::default();
        idx.add_version(1, ver(100, 0xAA), true);
        idx.add_version(2, ver(200, 0xBB), true);
        let versions = idx.latest_versions_at_or_before(50);
        assert!(versions.is_empty());
    }

    #[test]
    fn latest_versions_at_or_before_empty_index_returns_empty() {
        let idx = WalIndex::default();
        assert!(idx.latest_versions_at_or_before(u64::MAX).is_empty());
    }
}

// ── ReaderRegistry unit tests ─────────────────────────────────────────────────

#[cfg(test)]
mod reader_registry_tests {
    use crate::wal::reader_registry::ReaderRegistry;

    #[test]
    fn active_reader_count_is_zero_for_fresh_registry() {
        let reg = ReaderRegistry::default();
        assert_eq!(reg.active_reader_count().unwrap(), 0);
    }

    #[test]
    fn register_increments_active_reader_count() {
        let reg = ReaderRegistry::default();
        let _g1 = reg.register(100).unwrap();
        assert_eq!(reg.active_reader_count().unwrap(), 1);
        let _g2 = reg.register(200).unwrap();
        assert_eq!(reg.active_reader_count().unwrap(), 2);
    }

    #[test]
    fn dropping_guard_decrements_active_reader_count() {
        let reg = ReaderRegistry::default();
        let guard = reg.register(42).unwrap();
        assert_eq!(reg.active_reader_count().unwrap(), 1);
        drop(guard);
        assert_eq!(reg.active_reader_count().unwrap(), 0);
    }

    #[test]
    fn min_snapshot_lsn_is_none_when_no_readers_are_registered() {
        let reg = ReaderRegistry::default();
        assert!(reg.min_snapshot_lsn().unwrap().is_none());
    }

    #[test]
    fn min_snapshot_lsn_returns_minimum_across_all_active_readers() {
        let reg = ReaderRegistry::default();
        let _g1 = reg.register(100).unwrap();
        let _g2 = reg.register(50).unwrap();
        let _g3 = reg.register(200).unwrap();
        assert_eq!(reg.min_snapshot_lsn().unwrap(), Some(50));
    }

    #[test]
    fn min_snapshot_lsn_updates_after_lowest_reader_drops() {
        let reg = ReaderRegistry::default();
        let _g1 = reg.register(100).unwrap();
        let g2 = reg.register(50).unwrap();
        assert_eq!(reg.min_snapshot_lsn().unwrap(), Some(50));
        drop(g2);
        assert_eq!(reg.min_snapshot_lsn().unwrap(), Some(100));
    }

    #[test]
    fn min_snapshot_lsn_returns_none_after_all_readers_drop() {
        let reg = ReaderRegistry::default();
        let g = reg.register(10).unwrap();
        drop(g);
        assert!(reg.min_snapshot_lsn().unwrap().is_none());
    }

    #[test]
    fn reader_guard_exposes_correct_snapshot_lsn() {
        let reg = ReaderRegistry::default();
        let guard = reg.register(999).unwrap();
        assert_eq!(guard.snapshot_lsn(), 999);
    }

    #[test]
    fn reader_guard_ids_are_unique_within_registry() {
        let reg = ReaderRegistry::default();
        let g1 = reg.register(0).unwrap();
        let g2 = reg.register(0).unwrap();
        assert_ne!(g1.id(), g2.id());
    }

    #[test]
    fn capture_long_reader_warnings_generates_warning_for_zero_timeout() {
        let reg = ReaderRegistry::default();
        let _g1 = reg.register(100).unwrap();
        let _g2 = reg.register(200).unwrap();
        // timeout_sec=0 means any reader age >= 0 triggers a warning.
        let warnings = reg.capture_long_reader_warnings(0).unwrap();
        assert_eq!(warnings.len(), 2);
    }

    #[test]
    fn capture_long_reader_warnings_is_empty_when_threshold_is_max() {
        let reg = ReaderRegistry::default();
        let _g = reg.register(100).unwrap();
        let warnings = reg.capture_long_reader_warnings(u64::MAX).unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn warnings_accumulate_across_multiple_capture_calls() {
        let reg = ReaderRegistry::default();
        let _g = reg.register(100).unwrap();
        reg.capture_long_reader_warnings(0).unwrap();
        reg.capture_long_reader_warnings(0).unwrap();
        let stored = reg.warnings().unwrap();
        // Each call with a live reader at timeout=0 should append at least one warning.
        assert!(!stored.is_empty());
    }

    #[test]
    fn warnings_is_empty_on_fresh_registry() {
        let reg = ReaderRegistry::default();
        assert!(reg.warnings().unwrap().is_empty());
    }
}

// ── StatementSavepoint unit tests ─────────────────────────────────────────────

#[cfg(test)]
mod savepoint_tests {
    use crate::wal::savepoint::StatementSavepoint;

    #[test]
    fn new_stores_snapshot_lsn() {
        let sp = StatementSavepoint::new(42);
        assert_eq!(sp.snapshot_lsn, 42);
    }

    #[test]
    fn savepoint_zero_lsn() {
        let sp = StatementSavepoint::new(0);
        assert_eq!(sp.snapshot_lsn, 0);
    }

    #[test]
    fn savepoint_max_lsn() {
        let sp = StatementSavepoint::new(u64::MAX);
        assert_eq!(sp.snapshot_lsn, u64::MAX);
    }

    #[test]
    fn savepoint_implements_copy_and_eq() {
        let sp1 = StatementSavepoint::new(7);
        let sp2 = sp1; // Copy
        assert_eq!(sp1, sp2);
    }

    #[test]
    fn savepoints_with_different_lsns_are_not_equal() {
        let sp1 = StatementSavepoint::new(1);
        let sp2 = StatementSavepoint::new(2);
        assert_ne!(sp1, sp2);
    }
}

// ── InMemoryPageStore unit tests ──────────────────────────────────────────────

#[cfg(test)]
mod in_memory_page_store_tests {
    use crate::storage::page::{
        InMemoryPageStore, PageStore, CATALOG_ROOT_PAGE_ID, DEFAULT_PAGE_SIZE, SUPPORTED_PAGE_SIZES,
    };

    #[test]
    fn page_size_returns_configured_value() {
        let store = InMemoryPageStore::new(8192);
        assert_eq!(store.page_size(), 8192);
    }

    #[test]
    fn default_page_size_is_default_page_size_constant() {
        let store = InMemoryPageStore::default();
        assert_eq!(store.page_size(), DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn allocated_page_count_starts_at_zero() {
        let store = InMemoryPageStore::default();
        assert_eq!(store.allocated_page_count(), 0);
    }

    #[test]
    fn allocate_page_returns_ids_starting_after_catalog_root() {
        let mut store = InMemoryPageStore::default();
        let first = store.allocate_page().unwrap();
        assert_eq!(first, CATALOG_ROOT_PAGE_ID + 1);
        let second = store.allocate_page().unwrap();
        assert_eq!(second, CATALOG_ROOT_PAGE_ID + 2);
    }

    #[test]
    fn read_page_returns_zeroed_page_after_allocation() {
        let mut store = InMemoryPageStore::default();
        let page_id = store.allocate_page().unwrap();
        let data = store.read_page(page_id).unwrap();
        assert_eq!(data.len(), DEFAULT_PAGE_SIZE as usize);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn write_and_read_page_roundtrip() {
        let mut store = InMemoryPageStore::default();
        let page_id = store.allocate_page().unwrap();
        let mut data = vec![0_u8; DEFAULT_PAGE_SIZE as usize];
        data[0] = 0xFF;
        data[DEFAULT_PAGE_SIZE as usize - 1] = 0xAB;
        store.write_page(page_id, &data).unwrap();
        let read_back = store.read_page(page_id).unwrap();
        assert_eq!(read_back.to_vec(), data);
    }

    #[test]
    fn write_page_wrong_size_returns_internal_error() {
        let mut store = InMemoryPageStore::default();
        let page_id = store.allocate_page().unwrap();
        assert!(store.write_page(page_id, &[0_u8; 100]).is_err());
        assert!(store.write_page(page_id, &[]).is_err());
    }

    #[test]
    fn read_page_with_page_id_zero_returns_corruption_error() {
        let store = InMemoryPageStore::default();
        assert!(store.read_page(0).is_err());
    }

    #[test]
    fn write_page_with_page_id_zero_returns_corruption_error() {
        let mut store = InMemoryPageStore::default();
        let data = vec![0_u8; DEFAULT_PAGE_SIZE as usize];
        assert!(store.write_page(0, &data).is_err());
    }

    #[test]
    fn free_page_with_page_id_zero_returns_corruption_error() {
        let mut store = InMemoryPageStore::default();
        assert!(store.free_page(0).is_err());
    }

    #[test]
    fn free_page_recycles_id_for_next_allocation() {
        let mut store = InMemoryPageStore::default();
        let id1 = store.allocate_page().unwrap();
        store.free_page(id1).unwrap();
        let id2 = store.allocate_page().unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn contains_page_returns_false_before_allocation() {
        let store = InMemoryPageStore::default();
        assert!(!store.contains_page(CATALOG_ROOT_PAGE_ID + 1));
    }

    #[test]
    fn contains_page_returns_true_after_allocation() {
        let mut store = InMemoryPageStore::default();
        let id = store.allocate_page().unwrap();
        assert!(store.contains_page(id));
    }

    #[test]
    fn contains_page_returns_false_after_free() {
        let mut store = InMemoryPageStore::default();
        let id = store.allocate_page().unwrap();
        store.free_page(id).unwrap();
        assert!(!store.contains_page(id));
    }

    #[test]
    fn allocated_page_count_tracks_allocations_and_frees() {
        let mut store = InMemoryPageStore::default();
        let id1 = store.allocate_page().unwrap();
        let _id2 = store.allocate_page().unwrap();
        assert_eq!(store.allocated_page_count(), 2);
        store.free_page(id1).unwrap();
        assert_eq!(store.allocated_page_count(), 1);
    }

    #[test]
    fn read_unallocated_page_returns_zeroed_page() {
        // Page 100 was never allocated; the store returns a zeroed page.
        let store = InMemoryPageStore::default();
        let data = store.read_page(100).unwrap();
        assert_eq!(data.len(), DEFAULT_PAGE_SIZE as usize);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn supported_page_sizes_contains_exactly_three_sizes() {
        assert_eq!(SUPPORTED_PAGE_SIZES, [4096, 8192, 16384]);
    }

    #[test]
    fn is_supported_page_size_accepts_valid_sizes() {
        use crate::storage::page::is_supported_page_size;
        assert!(is_supported_page_size(4096));
        assert!(is_supported_page_size(8192));
        assert!(is_supported_page_size(16384));
    }

    #[test]
    fn is_supported_page_size_rejects_invalid_sizes() {
        use crate::storage::page::is_supported_page_size;
        assert!(!is_supported_page_size(0));
        assert!(!is_supported_page_size(512));
        assert!(!is_supported_page_size(1024));
        assert!(!is_supported_page_size(2048));
        assert!(!is_supported_page_size(65536));
    }

    #[test]
    fn page_offset_page_one_starts_at_zero() {
        use crate::storage::page::page_offset;
        assert_eq!(page_offset(1, 4096), 0);
        assert_eq!(page_offset(1, 8192), 0);
    }

    #[test]
    fn page_offset_page_two_starts_at_one_page_size() {
        use crate::storage::page::page_offset;
        assert_eq!(page_offset(2, 4096), 4096);
        assert_eq!(page_offset(2, 8192), 8192);
    }

    #[test]
    fn page_offset_computes_sequential_layout() {
        use crate::storage::page::page_offset;
        for page_id in 1_u32..=10 {
            let expected = (page_id as u64 - 1) * 4096;
            assert_eq!(page_offset(page_id, 4096), expected);
        }
    }

    #[test]
    fn page_count_for_len_rounds_down() {
        use crate::storage::page::page_count_for_len;
        assert_eq!(page_count_for_len(0, 4096), 0);
        assert_eq!(page_count_for_len(4096, 4096), 1);
        assert_eq!(page_count_for_len(8192, 4096), 2);
        assert_eq!(page_count_for_len(8191, 4096), 1);
        assert_eq!(page_count_for_len(8193, 4096), 2);
    }

    #[test]
    fn validate_page_id_accepts_non_zero() {
        use crate::storage::page::validate_page_id;
        assert!(validate_page_id(1).is_ok());
        assert!(validate_page_id(u32::MAX).is_ok());
    }

    #[test]
    fn validate_page_id_rejects_zero() {
        use crate::storage::page::validate_page_id;
        assert!(validate_page_id(0).is_err());
    }
}

// ── VFS helper function unit tests ────────────────────────────────────────────

#[cfg(test)]
mod vfs_helper_tests {
    use std::path::Path;

    use crate::vfs::mem::MemVfs;
    use crate::vfs::{is_memory_path, read_exact_at, write_all_at, FileKind, OpenMode, Vfs};

    fn mem_file(vfs: &MemVfs, name: &str) -> std::sync::Arc<dyn crate::vfs::VfsFile> {
        vfs.open(Path::new(name), OpenMode::CreateNew, FileKind::Database)
            .expect("create file")
    }

    #[test]
    fn is_memory_path_matches_colon_memory_colon() {
        assert!(is_memory_path(Path::new(":memory:")));
    }

    #[test]
    fn is_memory_path_is_case_insensitive() {
        assert!(is_memory_path(Path::new(":MEMORY:")));
        assert!(is_memory_path(Path::new(":Memory:")));
        assert!(is_memory_path(Path::new(":mEmOrY:")));
    }

    #[test]
    fn is_memory_path_rejects_regular_file_paths() {
        assert!(!is_memory_path(Path::new("/tmp/test.db")));
        assert!(!is_memory_path(Path::new("test.db")));
        assert!(!is_memory_path(Path::new("memory")));
        assert!(!is_memory_path(Path::new("")));
    }

    #[test]
    fn is_memory_path_rejects_partial_memory_token() {
        assert!(!is_memory_path(Path::new(":memory")));
        assert!(!is_memory_path(Path::new("memory:")));
    }

    #[test]
    fn read_exact_at_succeeds_when_data_is_available() {
        let vfs = MemVfs::default();
        let file = mem_file(&vfs, "r1.db");
        write_all_at(file.as_ref(), 0, &[1_u8, 2, 3, 4]).unwrap();
        let mut buf = [0_u8; 4];
        read_exact_at(file.as_ref(), 0, &mut buf).unwrap();
        assert_eq!(buf, [1, 2, 3, 4]);
    }

    #[test]
    fn read_exact_at_succeeds_at_non_zero_offset() {
        let vfs = MemVfs::default();
        let file = mem_file(&vfs, "r2.db");
        write_all_at(file.as_ref(), 0, &[0_u8, 0, 0, 10, 20, 30]).unwrap();
        let mut buf = [0_u8; 3];
        read_exact_at(file.as_ref(), 3, &mut buf).unwrap();
        assert_eq!(buf, [10, 20, 30]);
    }

    #[test]
    fn read_exact_at_returns_error_on_short_read() {
        let vfs = MemVfs::default();
        let file = mem_file(&vfs, "r3.db");
        write_all_at(file.as_ref(), 0, &[1_u8, 2]).unwrap();
        let mut buf = [0_u8; 4]; // request 4, only 2 available
        assert!(read_exact_at(file.as_ref(), 0, &mut buf).is_err());
    }

    #[test]
    fn read_exact_at_returns_error_when_offset_is_past_eof() {
        let vfs = MemVfs::default();
        let file = mem_file(&vfs, "r4.db");
        write_all_at(file.as_ref(), 0, &[1_u8, 2, 3]).unwrap();
        let mut buf = [0_u8; 2];
        assert!(read_exact_at(file.as_ref(), 10, &mut buf).is_err());
    }

    #[test]
    fn read_exact_at_with_empty_buffer_always_succeeds() {
        let vfs = MemVfs::default();
        let file = mem_file(&vfs, "r5.db");
        let mut buf: [u8; 0] = [];
        read_exact_at(file.as_ref(), 0, &mut buf).unwrap();
        read_exact_at(file.as_ref(), 999, &mut buf).unwrap();
    }

    #[test]
    fn write_all_at_succeeds_and_data_is_readable() {
        let vfs = MemVfs::default();
        let file = mem_file(&vfs, "w1.db");
        write_all_at(file.as_ref(), 0, &[0xAA_u8; 8]).unwrap();
        let mut buf = [0_u8; 8];
        read_exact_at(file.as_ref(), 0, &mut buf).unwrap();
        assert_eq!(buf, [0xAA; 8]);
    }

    #[test]
    fn write_all_at_with_empty_slice_succeeds() {
        let vfs = MemVfs::default();
        let file = mem_file(&vfs, "w2.db");
        write_all_at(file.as_ref(), 0, &[]).unwrap();
        assert_eq!(file.file_size().unwrap(), 0);
    }

    #[test]
    fn write_all_at_at_non_zero_offset_fills_gap_with_zeros() {
        let vfs = MemVfs::default();
        let file = mem_file(&vfs, "w3.db");
        write_all_at(file.as_ref(), 4, &[0xFF_u8, 0xFF]).unwrap();
        assert_eq!(file.file_size().unwrap(), 6);
        let mut buf = [0_u8; 6];
        read_exact_at(file.as_ref(), 0, &mut buf).unwrap();
        assert_eq!(buf, [0, 0, 0, 0, 0xFF, 0xFF]);
    }
}
