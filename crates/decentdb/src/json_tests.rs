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
        assert_eq!(result, JsonValue::Array(vec![JsonValue::Number("1".to_string())]));
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
        assert!(result.unwrap_err().to_string().contains("invalid JSON path"));
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
        assert_eq!(
            JsonValue::Number("42".to_string()).render_json(),
            "42"
        );
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
