//! Tests for SQL AST to_sql rendering to improve coverage.

#[cfg(test)]
mod tests {
    use crate::catalog::ColumnType;
    use crate::record::value::Value;
    use crate::sql::ast::*;

    #[test]
    fn expr_to_sql_smoke() {
        // Literals
        assert_eq!(Expr::Literal(Value::Null).to_sql(), "NULL");
        assert_eq!(Expr::Literal(Value::Int64(10)).to_sql(), "10");
        assert_eq!(Expr::Literal(Value::Float64(1.5)).to_sql(), "1.5");
        assert_eq!(Expr::Literal(Value::Bool(true)).to_sql(), "TRUE");
        assert_eq!(
            Expr::Literal(Value::Text("O'Connor".to_string())).to_sql(),
            "'O''Connor'"
        );
        assert_eq!(
            Expr::Literal(Value::Blob(vec![0x1, 0x2])).to_sql(),
            "X'0102'"
        );
        assert_eq!(
            Expr::Literal(Value::Decimal {
                scaled: 12,
                scale: 3
            })
            .to_sql(),
            "'12:3'"
        );

        // Column
        assert_eq!(
            Expr::Column {
                table: None,
                column: "c".to_string()
            }
            .to_sql(),
            "c"
        );
        assert_eq!(
            Expr::Column {
                table: Some("t".to_string()),
                column: "c".to_string()
            }
            .to_sql(),
            "t.c"
        );

        // Parameter
        assert_eq!(Expr::Parameter(2).to_sql(), "$2");

        // Unary
        assert_eq!(
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(Expr::Literal(Value::Bool(false)))
            }
            .to_sql(),
            "NOT (FALSE)"
        );
        assert_eq!(
            Expr::Unary {
                op: UnaryOp::Negate,
                expr: Box::new(Expr::Literal(Value::Int64(3)))
            }
            .to_sql(),
            "-(3)"
        );

        // Binary simple
        let left = Box::new(Expr::Literal(Value::Int64(1)));
        let right = Box::new(Expr::Literal(Value::Int64(2)));
        assert_eq!(
            Expr::Binary {
                left: left.clone(),
                op: BinaryOp::Add,
                right: right.clone()
            }
            .to_sql(),
            "(1 + 2)"
        );
        assert_eq!(
            Expr::Binary {
                left: left.clone(),
                op: BinaryOp::Concat,
                right: right.clone()
            }
            .to_sql(),
            "(1 || 2)"
        );

        // Between
        assert_eq!(
            Expr::Between {
                expr: Box::new(Expr::Literal(Value::Int64(5))),
                low: Box::new(Expr::Literal(Value::Int64(1))),
                high: Box::new(Expr::Literal(Value::Int64(10))),
                negated: false
            }
            .to_sql(),
            "(5 BETWEEN 1 AND 10)"
        );
        assert_eq!(
            Expr::Between {
                expr: Box::new(Expr::Literal(Value::Int64(5))),
                low: Box::new(Expr::Literal(Value::Int64(1))),
                high: Box::new(Expr::Literal(Value::Int64(10))),
                negated: true
            }
            .to_sql(),
            "(5 NOT BETWEEN 1 AND 10)"
        );

        // InList
        assert_eq!(
            Expr::InList {
                expr: Box::new(Expr::Literal(Value::Int64(1))),
                items: vec![
                    Expr::Literal(Value::Int64(2)),
                    Expr::Literal(Value::Int64(3))
                ],
                negated: false
            }
            .to_sql(),
            "(1 IN (2, 3))"
        );
        assert_eq!(
            Expr::InList {
                expr: Box::new(Expr::Literal(Value::Int64(1))),
                items: vec![Expr::Literal(Value::Int64(2))],
                negated: true
            }
            .to_sql(),
            "(1 NOT IN (2))"
        );

        // IsNull
        assert_eq!(
            Expr::IsNull {
                expr: Box::new(Expr::Literal(Value::Int64(1))),
                negated: false
            }
            .to_sql(),
            "1 IS NULL"
        );
        assert_eq!(
            Expr::IsNull {
                expr: Box::new(Expr::Literal(Value::Int64(1))),
                negated: true
            }
            .to_sql(),
            "1 IS NOT NULL"
        );

        // Function special no-arg
        assert_eq!(
            Expr::Function {
                name: "current_date".to_string(),
                args: vec![]
            }
            .to_sql(),
            "CURRENT_DATE"
        );
        assert_eq!(
            Expr::Function {
                name: "lower".to_string(),
                args: vec![Expr::Literal(Value::Text("x".to_string()))]
            }
            .to_sql(),
            "lower('x')"
        );

        // Aggregate star
        assert_eq!(
            Expr::Aggregate {
                name: "count".to_string(),
                args: vec![],
                distinct: false,
                star: true,
                order_by: vec![],
                within_group: false
            }
            .to_sql(),
            "count(*)"
        );

        // Case
        let case = Expr::Case {
            operand: None,
            branches: vec![(
                Expr::Literal(Value::Bool(true)),
                Expr::Literal(Value::Int64(1)),
            )],
            else_expr: Some(Box::new(Expr::Literal(Value::Int64(0)))),
        };
        assert_eq!(case.to_sql(), "CASE WHEN TRUE THEN 1 ELSE 0 END");

        // Row and cast
        assert_eq!(
            Expr::Row(vec![
                Expr::Literal(Value::Int64(1)),
                Expr::Literal(Value::Int64(2))
            ])
            .to_sql(),
            "(1, 2)"
        );
        assert_eq!(
            Expr::Cast {
                expr: Box::new(Expr::Literal(Value::Int64(1))),
                target_type: ColumnType::Int64
            }
            .to_sql(),
            "CAST(1 AS INT64)"
        );
    }
}
