//! Strongly typed internal AST for the supported DecentDB 1.0 SQL subset.

use crate::catalog::ColumnType;
use crate::record::value::Value;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Statement {
    Query(Query),
    Explain(ExplainStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    CreateView(CreateViewStatement),
    CreateTrigger(CreateTriggerStatement),
    DropTable {
        name: String,
        if_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    DropTrigger {
        name: String,
        table_name: String,
        if_exists: bool,
    },
    AlterViewRename {
        view_name: String,
        new_name: String,
    },
    AlterTable {
        table_name: String,
        actions: Vec<AlterTableAction>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExplainStatement {
    pub(crate) analyze: bool,
    pub(crate) statement: Box<Statement>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Query {
    pub(crate) ctes: Vec<CommonTableExpr>,
    pub(crate) body: QueryBody,
    pub(crate) order_by: Vec<OrderBy>,
    pub(crate) limit: Option<Expr>,
    pub(crate) offset: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum QueryBody {
    Select(Select),
    SetOperation {
        op: SetOperation,
        all: bool,
        left: Box<QueryBody>,
        right: Box<QueryBody>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SetOperation {
    Union,
    Intersect,
    Except,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CommonTableExpr {
    pub(crate) name: String,
    pub(crate) column_names: Vec<String>,
    pub(crate) query: Query,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Select {
    pub(crate) projection: Vec<SelectItem>,
    pub(crate) from: Vec<FromItem>,
    pub(crate) filter: Option<Expr>,
    pub(crate) group_by: Vec<Expr>,
    pub(crate) having: Option<Expr>,
    pub(crate) distinct: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SelectItem {
    Expr { expr: Expr, alias: Option<String> },
    Wildcard,
    QualifiedWildcard(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FromItem {
    Table {
        name: String,
        alias: Option<String>,
    },
    Subquery {
        query: Box<Query>,
        alias: String,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        kind: JoinKind,
        on: Expr,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinKind {
    Inner,
    Left,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OrderBy {
    pub(crate) expr: Expr,
    pub(crate) descending: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Expr {
    Literal(Value),
    Column {
        table: Option<String>,
        column: String,
    },
    Parameter(usize),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        items: Vec<Expr>,
        negated: bool,
    },
    Exists(Box<Query>),
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        case_insensitive: bool,
        negated: bool,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
    Aggregate {
        name: String,
        args: Vec<Expr>,
        star: bool,
    },
    RowNumber {
        partition_by: Vec<Expr>,
        order_by: Vec<OrderBy>,
    },
    Case {
        operand: Option<Box<Expr>>,
        branches: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        target_type: ColumnType,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnaryOp {
    Not,
    Negate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Add,
    Sub,
    Mul,
    Div,
    And,
    Or,
    Concat,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InsertStatement {
    pub(crate) table_name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) source: InsertSource,
    pub(crate) on_conflict: Option<ConflictAction>,
    pub(crate) returning: Vec<SelectItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Query(Box<Query>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ConflictTarget {
    Any,
    Columns(Vec<String>),
    Constraint(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ConflictAction {
    DoNothing {
        target: ConflictTarget,
    },
    DoUpdate {
        target: ConflictTarget,
        assignments: Vec<Assignment>,
        filter: Option<Expr>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct UpdateStatement {
    pub(crate) table_name: String,
    pub(crate) assignments: Vec<Assignment>,
    pub(crate) filter: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeleteStatement {
    pub(crate) table_name: String,
    pub(crate) filter: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Assignment {
    pub(crate) column_name: String,
    pub(crate) expr: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateTableStatement {
    pub(crate) table_name: String,
    pub(crate) if_not_exists: bool,
    pub(crate) columns: Vec<ColumnDefinition>,
    pub(crate) constraints: Vec<TableConstraint>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ColumnDefinition {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) nullable: bool,
    pub(crate) default: Option<Expr>,
    pub(crate) primary_key: bool,
    pub(crate) unique: bool,
    pub(crate) checks: Vec<Expr>,
    pub(crate) references: Option<ForeignKeyDefinition>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    Check {
        name: Option<String>,
        expr: Expr,
    },
    ForeignKey(ForeignKeyDefinition),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ForeignKeyActionSpec {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ForeignKeyDefinition {
    pub(crate) name: Option<String>,
    pub(crate) columns: Vec<String>,
    pub(crate) referenced_table: String,
    pub(crate) referenced_columns: Vec<String>,
    pub(crate) on_delete: ForeignKeyActionSpec,
    pub(crate) on_update: ForeignKeyActionSpec,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateIndexStatement {
    pub(crate) index_name: String,
    pub(crate) table_name: String,
    pub(crate) unique: bool,
    pub(crate) if_not_exists: bool,
    pub(crate) access_method: String,
    pub(crate) columns: Vec<IndexExpression>,
    pub(crate) predicate: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum IndexExpression {
    Column(String),
    Expr(Expr),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateViewStatement {
    pub(crate) view_name: String,
    pub(crate) replace: bool,
    pub(crate) column_names: Vec<String>,
    pub(crate) query: Query,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TriggerKindSpec {
    After,
    InsteadOf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TriggerEventSpec {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateTriggerStatement {
    pub(crate) trigger_name: String,
    pub(crate) target_name: String,
    pub(crate) kind: TriggerKindSpec,
    pub(crate) event: TriggerEventSpec,
    pub(crate) action_sql: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AlterTableAction {
    AddColumn(ColumnDefinition),
    DropColumn {
        column_name: String,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    AlterColumnType {
        column_name: String,
        new_type: ColumnType,
    },
}

impl Query {
    #[must_use]
    pub(crate) fn to_sql(&self) -> String {
        let mut parts = Vec::new();
        if !self.ctes.is_empty() {
            parts.push(format!(
                "WITH {}",
                self.ctes
                    .iter()
                    .map(CommonTableExpr::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        parts.push(self.body.to_sql());
        if !self.order_by.is_empty() {
            parts.push(format!(
                "ORDER BY {}",
                self.order_by
                    .iter()
                    .map(OrderBy::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(limit) = &self.limit {
            parts.push(format!("LIMIT {}", limit.to_sql()));
        }
        if let Some(offset) = &self.offset {
            parts.push(format!("OFFSET {}", offset.to_sql()));
        }
        parts.join(" ")
    }
}

impl QueryBody {
    #[must_use]
    pub(crate) fn to_sql(&self) -> String {
        match self {
            Self::Select(select) => select.to_sql(),
            Self::SetOperation {
                op,
                all,
                left,
                right,
            } => format!(
                "({}) {}{} ({})",
                left.to_sql(),
                match op {
                    SetOperation::Union => "UNION",
                    SetOperation::Intersect => "INTERSECT",
                    SetOperation::Except => "EXCEPT",
                },
                if *all { " ALL" } else { "" },
                right.to_sql()
            ),
        }
    }
}

impl CommonTableExpr {
    #[must_use]
    pub(crate) fn to_sql(&self) -> String {
        let columns = if self.column_names.is_empty() {
            String::new()
        } else {
            format!("({})", self.column_names.join(", "))
        };
        format!("{}{} AS ({})", self.name, columns, self.query.to_sql())
    }
}

impl Select {
    #[must_use]
    pub(crate) fn to_sql(&self) -> String {
        let mut parts = Vec::new();
        let select_kw = if self.distinct {
            "SELECT DISTINCT"
        } else {
            "SELECT"
        };
        parts.push(format!(
            "{} {}",
            select_kw,
            self.projection
                .iter()
                .map(SelectItem::to_sql)
                .collect::<Vec<_>>()
                .join(", ")
        ));
        if !self.from.is_empty() {
            parts.push(format!(
                "FROM {}",
                self.from
                    .iter()
                    .map(FromItem::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(filter) = &self.filter {
            parts.push(format!("WHERE {}", filter.to_sql()));
        }
        if !self.group_by.is_empty() {
            parts.push(format!(
                "GROUP BY {}",
                self.group_by
                    .iter()
                    .map(Expr::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(having) = &self.having {
            parts.push(format!("HAVING {}", having.to_sql()));
        }
        parts.join(" ")
    }
}

impl SelectItem {
    #[must_use]
    pub(crate) fn to_sql(&self) -> String {
        match self {
            Self::Expr { expr, alias } => {
                let base = expr.to_sql();
                alias
                    .as_ref()
                    .map_or(base.clone(), |alias| format!("{base} AS {alias}"))
            }
            Self::Wildcard => "*".to_string(),
            Self::QualifiedWildcard(name) => format!("{name}.*"),
        }
    }
}

impl FromItem {
    #[must_use]
    pub(crate) fn to_sql(&self) -> String {
        match self {
            Self::Table { name, alias } => alias
                .as_ref()
                .map_or_else(|| name.clone(), |alias| format!("{name} AS {alias}")),
            Self::Subquery { query, alias } => format!("({}) AS {alias}", query.to_sql()),
            Self::Join {
                left,
                right,
                kind,
                on,
            } => format!(
                "{} {} JOIN {} ON {}",
                left.to_sql(),
                match kind {
                    JoinKind::Inner => "INNER",
                    JoinKind::Left => "LEFT",
                },
                right.to_sql(),
                on.to_sql()
            ),
        }
    }
}

impl OrderBy {
    #[must_use]
    pub(crate) fn to_sql(&self) -> String {
        if self.descending {
            format!("{} DESC", self.expr.to_sql())
        } else {
            self.expr.to_sql()
        }
    }
}

impl Expr {
    #[must_use]
    pub(crate) fn to_sql(&self) -> String {
        match self {
            Self::Literal(value) => literal_to_sql(value),
            Self::Column { table, column } => match table {
                Some(table) => format!("{table}.{column}"),
                None => column.clone(),
            },
            Self::Parameter(number) => format!("${number}"),
            Self::Unary { op, expr } => match op {
                UnaryOp::Not => format!("NOT ({})", expr.to_sql()),
                UnaryOp::Negate => format!("-({})", expr.to_sql()),
            },
            Self::Binary { left, op, right } => format!(
                "({} {} {})",
                left.to_sql(),
                match op {
                    BinaryOp::Eq => "=",
                    BinaryOp::NotEq => "<>",
                    BinaryOp::Lt => "<",
                    BinaryOp::LtEq => "<=",
                    BinaryOp::Gt => ">",
                    BinaryOp::GtEq => ">=",
                    BinaryOp::Add => "+",
                    BinaryOp::Sub => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::Concat => "||",
                    BinaryOp::IsDistinctFrom => "IS DISTINCT FROM",
                    BinaryOp::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
                },
                right.to_sql()
            ),
            Self::Between {
                expr,
                low,
                high,
                negated,
            } => format!(
                "({} {}BETWEEN {} AND {})",
                expr.to_sql(),
                if *negated { "NOT " } else { "" },
                low.to_sql(),
                high.to_sql()
            ),
            Self::InList {
                expr,
                items,
                negated,
            } => format!(
                "({} {}IN ({}))",
                expr.to_sql(),
                if *negated { "NOT " } else { "" },
                items
                    .iter()
                    .map(Expr::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Exists(query) => format!("EXISTS ({})", query.to_sql()),
            Self::Like {
                expr,
                pattern,
                escape,
                case_insensitive,
                negated,
            } => {
                let mut sql = format!(
                    "{} {}{} {}",
                    expr.to_sql(),
                    if *negated { "NOT " } else { "" },
                    if *case_insensitive { "ILIKE" } else { "LIKE" },
                    pattern.to_sql()
                );
                if let Some(escape) = escape {
                    sql.push_str(&format!(" ESCAPE {}", escape.to_sql()));
                }
                sql
            }
            Self::IsNull { expr, negated } => {
                if *negated {
                    format!("{} IS NOT NULL", expr.to_sql())
                } else {
                    format!("{} IS NULL", expr.to_sql())
                }
            }
            Self::Function { name, args } => format!(
                "{}({})",
                name,
                args.iter().map(Expr::to_sql).collect::<Vec<_>>().join(", ")
            ),
            Self::Aggregate { name, args, star } => {
                if *star {
                    format!("{name}(*)")
                } else {
                    format!(
                        "{}({})",
                        name,
                        args.iter().map(Expr::to_sql).collect::<Vec<_>>().join(", ")
                    )
                }
            }
            Self::RowNumber {
                partition_by,
                order_by,
            } => {
                let mut over_parts = Vec::new();
                if !partition_by.is_empty() {
                    over_parts.push(format!(
                        "PARTITION BY {}",
                        partition_by
                            .iter()
                            .map(Expr::to_sql)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                if !order_by.is_empty() {
                    over_parts.push(format!(
                        "ORDER BY {}",
                        order_by
                            .iter()
                            .map(OrderBy::to_sql)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                format!("ROW_NUMBER() OVER ({})", over_parts.join(" "))
            }
            Self::Case {
                operand,
                branches,
                else_expr,
            } => {
                let mut sql = String::from("CASE");
                if let Some(operand) = operand {
                    sql.push(' ');
                    sql.push_str(&operand.to_sql());
                }
                for (condition, result) in branches {
                    sql.push_str(&format!(
                        " WHEN {} THEN {}",
                        condition.to_sql(),
                        result.to_sql()
                    ));
                }
                if let Some(else_expr) = else_expr {
                    sql.push_str(&format!(" ELSE {}", else_expr.to_sql()));
                }
                sql.push_str(" END");
                sql
            }
            Self::Cast { expr, target_type } => {
                format!("CAST({} AS {})", expr.to_sql(), target_type.as_str())
            }
        }
    }
}

fn literal_to_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
        Value::Blob(bytes) => {
            let hex = bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            format!("X'{hex}'")
        }
        Value::Decimal { scaled, scale } => format!("'{}:{}'", scaled, scale),
        Value::Uuid(value) => {
            let hex = value
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            format!("'{hex}'")
        }
        Value::TimestampMicros(value) => value.to_string(),
    }
}
