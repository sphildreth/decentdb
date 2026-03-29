//! Additional tests for SQL AST to_sql rendering to improve coverage.

#[cfg(test)]
mod tests {
    use crate::record::value::Value;
    use crate::sql::ast::*;

    #[test]
    fn set_operation_and_cte_to_sql() {
        let left = QueryBody::Values(vec![vec![Expr::Literal(Value::Int64(1))]]);
        let right = QueryBody::Values(vec![vec![Expr::Literal(Value::Int64(2))]]);
        let setop = QueryBody::SetOperation {
            op: SetOperation::Union,
            all: true,
            left: Box::new(left),
            right: Box::new(right),
        };
        let s = setop.to_sql();
        assert!(s.contains("UNION"));
        assert!(s.contains("ALL"));
        assert!(s.contains("VALUES"));
    }

    #[test]
    fn fromitem_lateral_subquery_and_function() {
        let q = Query {
            recursive: false,
            ctes: vec![],
            body: QueryBody::Values(vec![vec![Expr::Literal(Value::Int64(3))]]),
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let sub = FromItem::Subquery {
            query: Box::new(q),
            alias: "s".to_string(),
            column_names: vec!["c".to_string()],
            lateral: true,
        };
        assert!(sub.to_sql().starts_with("LATERAL"));

        let func = FromItem::Function {
            name: "f".to_string(),
            args: vec![Expr::Literal(Value::Text("a".to_string()))],
            alias: Some("fn".to_string()),
            lateral: true,
        };
        assert!(func.to_sql().starts_with("LATERAL"));
        assert!(func.to_sql().contains("AS fn"));
    }

    #[test]
    fn join_constraints_and_kinds() {
        let left = Box::new(FromItem::Table {
            name: "a".to_string(),
            alias: None,
        });
        let right = Box::new(FromItem::Table {
            name: "b".to_string(),
            alias: None,
        });

        let natural = FromItem::Join {
            left: left.clone(),
            right: right.clone(),
            kind: JoinKind::Inner,
            constraint: JoinConstraint::Natural,
        };
        assert!(natural.to_sql().contains("NATURAL"));

        let using = FromItem::Join {
            left: left.clone(),
            right: right.clone(),
            kind: JoinKind::Left,
            constraint: JoinConstraint::Using(vec!["x".to_string(), "y".to_string()]),
        };
        assert!(using.to_sql().contains("USING (x, y)"));

        let cross_on = FromItem::Join {
            left: left.clone(),
            right: right.clone(),
            kind: JoinKind::Cross,
            constraint: JoinConstraint::On(Expr::Literal(Value::Bool(true))),
        };
        // Cross join with ON should omit the ON clause per implementation
        assert!(!cross_on.to_sql().contains("ON "));

        let left_on = FromItem::Join {
            left: left.clone(),
            right: right.clone(),
            kind: JoinKind::Left,
            constraint: JoinConstraint::On(Expr::Binary {
                left: Box::new(Expr::Column {
                    table: None,
                    column: "a".to_string(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Column {
                    table: None,
                    column: "b".to_string(),
                }),
            }),
        };
        assert!(left_on.to_sql().contains("ON "));
    }

    #[test]
    fn window_frame_and_bounds() {
        // WindowFrame::to_sql is private; exercise it via Expr::RowNumber which calls into it
        let wf1 = WindowFrame {
            unit: WindowFrameUnit::Rows,
            start: WindowFrameBound::UnboundedPreceding,
            end: None,
        };
        let rn1 = Expr::RowNumber {
            partition_by: vec![],
            order_by: vec![],
            frame: Some(wf1),
        };
        assert!(rn1.to_sql().contains("ROWS UNBOUNDED PRECEDING"));

        let wf2 = WindowFrame {
            unit: WindowFrameUnit::Range,
            start: WindowFrameBound::Preceding(Box::new(Expr::Literal(Value::Int64(5)))),
            end: Some(WindowFrameBound::CurrentRow),
        };
        let rn2 = Expr::RowNumber {
            partition_by: vec![],
            order_by: vec![],
            frame: Some(wf2),
        };
        assert!(rn2.to_sql().contains("BETWEEN"));
    }

    #[test]
    fn row_number_and_window_function_variants() {
        let rn = Expr::RowNumber {
            partition_by: vec![Expr::Column {
                table: None,
                column: "p".to_string(),
            }],
            order_by: vec![OrderBy {
                expr: Expr::Column {
                    table: None,
                    column: "o".to_string(),
                },
                descending: true,
            }],
            frame: Some(WindowFrame {
                unit: WindowFrameUnit::Rows,
                start: WindowFrameBound::Preceding(Box::new(Expr::Literal(Value::Int64(1)))),
                end: None,
            }),
        };
        assert!(rn.to_sql().contains("ROW_NUMBER() OVER"));

        let wf = Expr::WindowFunction {
            name: "row".to_string(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            frame: None,
            distinct: false,
            star: true,
        };
        let s = wf.to_sql();
        assert!(s.contains("OVER"));
        assert!(s.contains("*"));
    }

    #[test]
    fn compare_subquery_scalar_exists_and_orderby() {
        let q = Query {
            recursive: false,
            ctes: vec![],
            body: QueryBody::Values(vec![vec![Expr::Literal(Value::Int64(9))]]),
            order_by: vec![],
            limit: None,
            offset: None,
        };

        let cs = Expr::CompareSubquery {
            expr: Box::new(Expr::Literal(Value::Int64(1))),
            op: BinaryOp::Gt,
            quantifier: SubqueryQuantifier::Any,
            query: Box::new(q.clone()),
        };
        assert!(cs.to_sql().contains("ANY"));

        let scalar = Expr::ScalarSubquery(Box::new(q.clone()));
        assert!(scalar.to_sql().starts_with("("));

        let exists = Expr::Exists(Box::new(q));
        assert!(exists.to_sql().starts_with("EXISTS ("));

        let ob = OrderBy {
            expr: Expr::Literal(Value::Int64(1)),
            descending: true,
        };
        assert_eq!(ob.to_sql(), "1 DESC");
    }

    #[test]
    fn literal_timestamp_and_uuid_rendering() {
        assert_eq!(
            Expr::Literal(Value::TimestampMicros(12345)).to_sql(),
            "12345"
        );
        let uuid = Expr::Literal(Value::Uuid([0u8; 16]));
        let s = uuid.to_sql();
        assert!(s.starts_with("'") && s.ends_with("'"));
        assert!(s.contains('0'));
    }
}
