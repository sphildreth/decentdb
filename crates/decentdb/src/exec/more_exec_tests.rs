//! Additional unit tests for exec helpers to increase coverage.

#[cfg(test)]
mod tests {
    use super::super::*;

    use crate::catalog::ColumnType;
    use crate::sql::ast::BinaryOp;

    #[test]
    fn arithmetic_int_ops() {
        assert_eq!(
            arithmetic(&BinaryOp::Add, Value::Int64(5), Value::Int64(3)).expect("arithmetic add"),
            Value::Int64(8)
        );
        assert_eq!(
            arithmetic(&BinaryOp::Sub, Value::Int64(5), Value::Int64(3)).expect("arithmetic sub"),
            Value::Int64(2)
        );
        assert_eq!(
            arithmetic(&BinaryOp::Mul, Value::Int64(6), Value::Int64(7)).expect("arithmetic mul"),
            Value::Int64(42)
        );
        assert_eq!(
            arithmetic(&BinaryOp::Div, Value::Int64(10), Value::Int64(2)).expect("arithmetic div"),
            Value::Int64(5)
        );
        assert_eq!(
            arithmetic(&BinaryOp::Mod, Value::Int64(10), Value::Int64(3)).expect("arithmetic mod"),
            Value::Int64(1)
        );
    }

    #[test]
    fn arithmetic_div_by_zero_returns_null() {
        let v = arithmetic(&BinaryOp::Div, Value::Int64(1), Value::Int64(0)).expect("div result");
        assert!(matches!(v, Value::Null));

        let v2 = arithmetic(&BinaryOp::Mod, Value::Int64(1), Value::Int64(0)).expect("mod result");
        assert!(matches!(v2, Value::Null));

        let v3 = arithmetic(&BinaryOp::Div, Value::Float64(1.0), Value::Float64(0.0))
            .expect("float div result");
        assert!(matches!(v3, Value::Null));
    }

    #[test]
    fn arithmetic_float_mixed() {
        assert_eq!(
            arithmetic(&BinaryOp::Add, Value::Int64(2), Value::Float64(3.5)).expect("mix add"),
            Value::Float64(5.5)
        );
        assert_eq!(
            arithmetic(&BinaryOp::Mul, Value::Float64(2.0), Value::Int64(3)).expect("mix mul"),
            Value::Float64(6.0)
        );
    }

    #[test]
    fn arithmetic_invalid_types_error() {
        let err = arithmetic(
            &BinaryOp::Add,
            Value::Text("a".to_string()),
            Value::Int64(1),
        );
        assert!(err.is_err());
    }

    #[test]
    fn apply_interval_micros_overflow() {
        let err = apply_interval_micros(i64::MAX, 1, true);
        assert!(err.is_err());
    }

    #[test]
    fn value_to_text_and_blob_error() {
        assert_eq!(value_to_text(&Value::Null).expect("null to text"), "");
        assert_eq!(
            value_to_text(&Value::Int64(-42)).expect("int to text"),
            "-42"
        );
        assert_eq!(
            value_to_text(&Value::Bool(true)).expect("bool to text"),
            "true"
        );
        assert!(value_to_text(&Value::Blob(vec![1, 2, 3])).is_err());
    }

    #[test]
    fn cast_value_examples() {
        // Float -> Int64
        assert_eq!(
            cast_value(Value::Float64(3.9), ColumnType::Int64).expect("float->int"),
            Value::Int64(3)
        );
        // Text -> Int64 parse error
        assert!(cast_value(Value::Text("nope".to_string()), ColumnType::Int64).is_err());
        // Bool -> Int64
        assert_eq!(
            cast_value(Value::Bool(true), ColumnType::Int64).expect("bool->int"),
            Value::Int64(1)
        );
        // Text -> Bool parse (t/1)
        assert_eq!(
            cast_value(Value::Text("t".to_string()), ColumnType::Bool).expect("text->bool"),
            Value::Bool(true)
        );
        assert!(cast_value(Value::Text("x".to_string()), ColumnType::Bool).is_err());
    }

    #[test]
    fn normalize_like_escape_variants() {
        assert_eq!(normalize_like_escape(None).expect("none"), None);
        assert_eq!(
            normalize_like_escape(Some(Value::Null)).expect("null"),
            None
        );
        assert_eq!(
            normalize_like_escape(Some(Value::Text("x".to_string()))).expect("char"),
            Some('x')
        );
        assert!(normalize_like_escape(Some(Value::Text("xy".to_string()))).is_err());
        assert!(normalize_like_escape(Some(Value::Int64(1))).is_err());
    }

    #[test]
    fn infer_column_type_for_ctas_examples() {
        let rows = vec![
            vec![Value::Null, Value::Text("hello".to_string())],
            vec![Value::Int64(1), Value::Text("x".to_string())],
        ];
        assert_eq!(
            infer_column_type_for_ctas(&rows, 0),
            crate::catalog::ColumnType::Int64
        );
        assert_eq!(
            infer_column_type_for_ctas(&rows, 1),
            crate::catalog::ColumnType::Text
        );

        let empty_rows: Vec<Vec<Value>> = vec![];
        assert_eq!(
            infer_column_type_for_ctas(&empty_rows, 0),
            crate::catalog::ColumnType::Text
        );
    }

    #[test]
    fn truthy_behavior() {
        assert_eq!(truthy(&Value::Bool(true)), Some(true));
        assert_eq!(truthy(&Value::Bool(false)), Some(false));
        assert_eq!(truthy(&Value::Null), None);
        assert_eq!(truthy(&Value::Int64(1)), None);
    }
}
