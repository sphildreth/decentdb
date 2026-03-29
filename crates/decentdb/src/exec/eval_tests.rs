//! Unit tests for expression evaluation (eval_expr) helpers.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::exec::{EngineRuntime, Dataset};
    use crate::record::value::Value;
    use crate::sql::ast::{Expr, BinaryOp, UnaryOp};

    #[test]
    fn eval_binary_add() {
        let runtime = EngineRuntime::empty(1);
        let expr = Expr::Binary {
            left: Box::new(Expr::Literal(Value::Int64(2))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(Value::Int64(3))),
        };
        let v = runtime
            .eval_expr(&expr, &Dataset::empty(), &[], &[], &BTreeMap::new(), None)
            .expect("eval");
        assert_eq!(v, Value::Int64(5));
    }

    #[test]
    fn eval_unary_not_and_negate() {
        let runtime = EngineRuntime::empty(1);
        let not_expr = Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Literal(Value::Bool(false))),
        };
        let v = runtime
            .eval_expr(&not_expr, &Dataset::empty(), &[], &[], &BTreeMap::new(), None)
            .expect("eval not");
        assert_eq!(v, Value::Bool(true));

        let neg_expr = Expr::Unary {
            op: UnaryOp::Negate,
            expr: Box::new(Expr::Literal(Value::Int64(7))),
        };
        let v2 = runtime
            .eval_expr(&neg_expr, &Dataset::empty(), &[], &[], &BTreeMap::new(), None)
            .expect("eval neg");
        assert_eq!(v2, Value::Int64(-7));
    }

    #[test]
    fn eval_between_and_isnull_case() {
        let runtime = EngineRuntime::empty(1);
        let between = Expr::Between {
            expr: Box::new(Expr::Literal(Value::Null)),
            low: Box::new(Expr::Literal(Value::Int64(1))),
            high: Box::new(Expr::Literal(Value::Int64(3))),
            negated: false,
        };
        let v = runtime
            .eval_expr(&between, &Dataset::empty(), &[], &[], &BTreeMap::new(), None)
            .expect("eval between");
        assert!(matches!(v, Value::Null));

        let isnull = Expr::IsNull {
            expr: Box::new(Expr::Literal(Value::Null)),
            negated: false,
        };
        let v2 = runtime
            .eval_expr(&isnull, &Dataset::empty(), &[], &[], &BTreeMap::new(), None)
            .expect("eval isnull");
        assert_eq!(v2, Value::Bool(true));
    }

    #[test]
    fn eval_case_simple() {
        let runtime = EngineRuntime::empty(1);
        let case = Expr::Case {
            operand: None,
            branches: vec![(
                Expr::Literal(Value::Bool(true)),
                Expr::Literal(Value::Int64(9)),
            )],
            else_expr: Some(Box::new(Expr::Literal(Value::Int64(0)))),
        };
        let v = runtime
            .eval_expr(&case, &Dataset::empty(), &[], &[], &BTreeMap::new(), None)
            .expect("eval case");
        assert_eq!(v, Value::Int64(9));
    }

    #[test]
    fn eval_function_coalesce() {
        let runtime = EngineRuntime::empty(1);
        let func = Expr::Function {
            name: "coalesce".to_string(),
            args: vec![Expr::Literal(Value::Null), Expr::Literal(Value::Int64(5))],
        };
        let v = runtime
            .eval_expr(&func, &Dataset::empty(), &[], &[], &BTreeMap::new(), None)
            .expect("eval func");
        assert_eq!(v, Value::Int64(5));
    }
}
