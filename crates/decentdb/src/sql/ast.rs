//! Strongly typed internal AST for the supported DecentDB 1.0 SQL subset.

use crate::catalog::ColumnType;
use crate::record::value::Value;
use std::collections::BTreeSet;

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Statement {
    Query(Query),
    Explain(ExplainStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Analyze {
        table_name: Option<String>,
    },
    CreateTable(CreateTableStatement),
    CreateTableAs(CreateTableAsStatement),
    CreateSchema {
        name: String,
        if_not_exists: bool,
    },
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
    TruncateTable {
        table_name: String,
        identity: TruncateIdentityMode,
        cascade: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TruncateIdentityMode {
    Continue,
    Restart,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExplainStatement {
    pub(crate) analyze: bool,
    pub(crate) statement: Box<Statement>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Query {
    pub(crate) recursive: bool,
    pub(crate) ctes: Vec<CommonTableExpr>,
    pub(crate) body: QueryBody,
    pub(crate) order_by: Vec<OrderBy>,
    pub(crate) limit: Option<Expr>,
    pub(crate) offset: Option<Expr>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum QueryBody {
    Select(Select),
    Values(Vec<Vec<Expr>>),
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
    pub(crate) distinct_on: Vec<Expr>,
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
        column_names: Vec<String>,
        lateral: bool,
    },
    Function {
        name: String,
        args: Vec<Expr>,
        alias: Option<String>,
        lateral: bool,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        kind: JoinKind,
        constraint: JoinConstraint,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum JoinConstraint {
    On(Expr),
    Using(Vec<String>),
    Natural,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OrderBy {
    pub(crate) expr: Expr,
    pub(crate) descending: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WindowFrameUnit {
    Rows,
    Range,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum WindowFrameBound {
    UnboundedPreceding,
    UnboundedFollowing,
    CurrentRow,
    Preceding(Box<Expr>),
    Following(Box<Expr>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WindowFrame {
    pub(crate) unit: WindowFrameUnit,
    pub(crate) start: WindowFrameBound,
    pub(crate) end: Option<WindowFrameBound>,
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
    InSubquery {
        expr: Box<Expr>,
        query: Box<Query>,
        negated: bool,
    },
    CompareSubquery {
        expr: Box<Expr>,
        op: BinaryOp,
        quantifier: SubqueryQuantifier,
        query: Box<Query>,
    },
    ScalarSubquery(Box<Query>),
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
        distinct: bool,
        star: bool,
        order_by: Vec<OrderBy>,
        within_group: bool,
    },
    RowNumber {
        partition_by: Vec<Expr>,
        order_by: Vec<OrderBy>,
        frame: Option<WindowFrame>,
    },
    WindowFunction {
        name: String,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderBy>,
        frame: Option<WindowFrame>,
        distinct: bool,
        star: bool,
    },
    Case {
        operand: Option<Box<Expr>>,
        branches: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Row(Vec<Expr>),
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
    RegexMatch,
    RegexMatchCaseInsensitive,
    RegexNotMatch,
    RegexNotMatchCaseInsensitive,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    And,
    Or,
    Concat,
    JsonExtract,
    JsonExtractText,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SubqueryQuantifier {
    Any,
    All,
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
    pub(crate) returning: Vec<SelectItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeleteStatement {
    pub(crate) table_name: String,
    pub(crate) filter: Option<Expr>,
    pub(crate) returning: Vec<SelectItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Assignment {
    pub(crate) column_name: String,
    pub(crate) expr: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateTableStatement {
    pub(crate) table_name: String,
    pub(crate) temporary: bool,
    pub(crate) if_not_exists: bool,
    pub(crate) columns: Vec<ColumnDefinition>,
    pub(crate) constraints: Vec<TableConstraint>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateTableAsStatement {
    pub(crate) table_name: String,
    pub(crate) temporary: bool,
    pub(crate) if_not_exists: bool,
    pub(crate) column_names: Vec<String>,
    pub(crate) query: Query,
    pub(crate) with_data: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ColumnDefinition {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) nullable: bool,
    pub(crate) default: Option<Expr>,
    pub(crate) generated: Option<Expr>,
    pub(crate) generated_stored: bool,
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
    pub(crate) include_columns: Vec<String>,
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
    pub(crate) temporary: bool,
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

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AlterTableAction {
    AddColumn(ColumnDefinition),
    RenameTable {
        new_name: String,
    },
    AddConstraint(TableConstraint),
    DropConstraint {
        constraint_name: String,
    },
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
                "WITH{} {}",
                if self.recursive { " RECURSIVE" } else { "" },
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
            Self::Values(rows) => {
                let rendered_rows = rows
                    .iter()
                    .map(|row| {
                        format!(
                            "({})",
                            row.iter().map(Expr::to_sql).collect::<Vec<_>>().join(", ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("VALUES {rendered_rows}")
            }
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
        let select_kw = if !self.distinct_on.is_empty() {
            format!(
                "SELECT DISTINCT ON ({})",
                self.distinct_on
                    .iter()
                    .map(Expr::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else if self.distinct {
            "SELECT DISTINCT".to_string()
        } else {
            "SELECT".to_string()
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
            Self::Subquery {
                query,
                alias,
                column_names,
                lateral,
            } => {
                let alias = if column_names.is_empty() {
                    alias.clone()
                } else {
                    format!("{alias}({})", column_names.join(", "))
                };
                let base = format!("({}) AS {alias}", query.to_sql());
                if *lateral {
                    format!("LATERAL {base}")
                } else {
                    base
                }
            }
            Self::Function {
                name,
                args,
                alias,
                lateral,
            } => {
                let base = format!(
                    "{}({})",
                    name,
                    args.iter().map(Expr::to_sql).collect::<Vec<_>>().join(", ")
                );
                let rendered = alias
                    .as_ref()
                    .map_or(base.clone(), |alias| format!("{base} AS {alias}"));
                if *lateral {
                    format!("LATERAL {rendered}")
                } else {
                    rendered
                }
            }
            Self::Join {
                left,
                right,
                kind,
                constraint,
            } => {
                let join_kw = match kind {
                    JoinKind::Inner => "INNER JOIN".to_string(),
                    JoinKind::Left => "LEFT JOIN".to_string(),
                    JoinKind::Right => "RIGHT JOIN".to_string(),
                    JoinKind::Full => "FULL OUTER JOIN".to_string(),
                    JoinKind::Cross => "CROSS JOIN".to_string(),
                };
                match constraint {
                    JoinConstraint::Natural => {
                        let natural_join_kw = match kind {
                            JoinKind::Inner => "NATURAL JOIN",
                            JoinKind::Left => "NATURAL LEFT JOIN",
                            JoinKind::Right => "NATURAL RIGHT JOIN",
                            JoinKind::Full => "NATURAL FULL OUTER JOIN",
                            JoinKind::Cross => "NATURAL CROSS JOIN",
                        };
                        format!("{} {} {}", left.to_sql(), natural_join_kw, right.to_sql())
                    }
                    JoinConstraint::Using(columns) => format!(
                        "{} {} {} USING ({})",
                        left.to_sql(),
                        if *kind == JoinKind::Inner {
                            "JOIN"
                        } else {
                            join_kw.as_str()
                        },
                        right.to_sql(),
                        columns.join(", ")
                    ),
                    JoinConstraint::On(on) if *kind == JoinKind::Cross => {
                        format!("{} {} {}", left.to_sql(), join_kw, right.to_sql())
                    }
                    JoinConstraint::On(on) => format!(
                        "{} {} {} ON {}",
                        left.to_sql(),
                        join_kw,
                        right.to_sql(),
                        on.to_sql()
                    ),
                }
            }
        }
    }
}

impl JoinConstraint {
    #[must_use]
    pub(crate) fn to_sql(&self) -> String {
        match self {
            Self::On(expr) => format!("ON {}", expr.to_sql()),
            Self::Using(columns) => format!("USING ({})", columns.join(", ")),
            Self::Natural => "NATURAL".to_string(),
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

impl WindowFrameUnit {
    fn to_sql(self) -> &'static str {
        match self {
            Self::Rows => "ROWS",
            Self::Range => "RANGE",
        }
    }
}

impl WindowFrameBound {
    fn to_sql(&self) -> String {
        match self {
            Self::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
            Self::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
            Self::CurrentRow => "CURRENT ROW".to_string(),
            Self::Preceding(expr) => format!("{} PRECEDING", expr.to_sql()),
            Self::Following(expr) => format!("{} FOLLOWING", expr.to_sql()),
        }
    }
}

impl WindowFrame {
    fn to_sql(&self) -> String {
        let unit = self.unit.to_sql();
        if let Some(end) = &self.end {
            format!(
                "{unit} BETWEEN {} AND {}",
                self.start.to_sql(),
                end.to_sql()
            )
        } else {
            format!("{unit} {}", self.start.to_sql())
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
                    BinaryOp::RegexMatch => "~",
                    BinaryOp::RegexMatchCaseInsensitive => "~*",
                    BinaryOp::RegexNotMatch => "!~",
                    BinaryOp::RegexNotMatchCaseInsensitive => "!~*",
                    BinaryOp::Add => "+",
                    BinaryOp::Sub => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                    BinaryOp::Mod => "%",
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::Concat => "||",
                    BinaryOp::JsonExtract => "->",
                    BinaryOp::JsonExtractText => "->>",
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
            Self::InSubquery {
                expr,
                query,
                negated,
            } => format!(
                "({} {}IN ({}))",
                expr.to_sql(),
                if *negated { "NOT " } else { "" },
                query.to_sql()
            ),
            Self::CompareSubquery {
                expr,
                op,
                quantifier,
                query,
            } => format!(
                "({} {} {} ({}))",
                expr.to_sql(),
                match op {
                    BinaryOp::Eq => "=",
                    BinaryOp::NotEq => "<>",
                    BinaryOp::Lt => "<",
                    BinaryOp::LtEq => "<=",
                    BinaryOp::Gt => ">",
                    BinaryOp::GtEq => ">=",
                    BinaryOp::RegexMatch => "~",
                    BinaryOp::RegexMatchCaseInsensitive => "~*",
                    BinaryOp::RegexNotMatch => "!~",
                    BinaryOp::RegexNotMatchCaseInsensitive => "!~*",
                    BinaryOp::Add => "+",
                    BinaryOp::Sub => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                    BinaryOp::Mod => "%",
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::Concat => "||",
                    BinaryOp::JsonExtract => "->",
                    BinaryOp::JsonExtractText => "->>",
                    BinaryOp::IsDistinctFrom => "IS DISTINCT FROM",
                    BinaryOp::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
                },
                match quantifier {
                    SubqueryQuantifier::Any => "ANY",
                    SubqueryQuantifier::All => "ALL",
                },
                query.to_sql()
            ),
            Self::ScalarSubquery(query) => format!("({})", query.to_sql()),
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
            Self::Function { name, args }
                if args.is_empty()
                    && matches!(
                        name.as_str(),
                        "current_date"
                            | "current_time"
                            | "current_timestamp"
                            | "localtime"
                            | "localtimestamp"
                    ) =>
            {
                name.to_ascii_uppercase()
            }
            Self::Function { name, args } => format!(
                "{}({})",
                name,
                args.iter().map(Expr::to_sql).collect::<Vec<_>>().join(", ")
            ),
            Self::Aggregate {
                name,
                args,
                star,
                distinct,
                order_by,
                within_group,
            } => {
                if *star {
                    if order_by.is_empty() {
                        format!("{name}(*)")
                    } else {
                        let order_sql = order_by
                            .iter()
                            .map(OrderBy::to_sql)
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{name}(* ORDER BY {order_sql})")
                    }
                } else {
                    let args_sql = args.iter().map(Expr::to_sql).collect::<Vec<_>>().join(", ");
                    if *within_group {
                        let order_sql = order_by
                            .iter()
                            .map(OrderBy::to_sql)
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{name}({args_sql}) WITHIN GROUP (ORDER BY {order_sql})")
                    } else if !order_by.is_empty() {
                        let order_sql = order_by
                            .iter()
                            .map(OrderBy::to_sql)
                            .collect::<Vec<_>>()
                            .join(", ");
                        if *distinct {
                            format!("{name}(DISTINCT {args_sql} ORDER BY {order_sql})")
                        } else {
                            format!("{name}({args_sql} ORDER BY {order_sql})")
                        }
                    } else if *distinct {
                        format!("{name}(DISTINCT {args_sql})")
                    } else {
                        format!("{name}({args_sql})")
                    }
                }
            }
            Self::RowNumber {
                partition_by,
                order_by,
                frame,
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
                if let Some(frame) = frame {
                    over_parts.push(frame.to_sql());
                }
                format!("ROW_NUMBER() OVER ({})", over_parts.join(" "))
            }
            Self::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
                frame,
                distinct,
                star,
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
                if let Some(frame) = frame {
                    over_parts.push(frame.to_sql());
                }
                let args_sql = if *star {
                    "*".to_string()
                } else if *distinct {
                    format!(
                        "DISTINCT {}",
                        args.iter().map(Expr::to_sql).collect::<Vec<_>>().join(", ")
                    )
                } else {
                    args.iter().map(Expr::to_sql).collect::<Vec<_>>().join(", ")
                };
                format!(
                    "{}({}) OVER ({})",
                    name.to_ascii_uppercase(),
                    args_sql,
                    over_parts.join(" ")
                )
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
            Self::Row(items) => format!(
                "({})",
                items
                    .iter()
                    .map(Expr::to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
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

/// Conservative analyzer for ADR 0143 Phase B per-table on-demand load.
///
/// Returns `Some(tables)` only when the statement's table reference set is
/// **provably exhaustive** — i.e. every table the executor could touch is
/// in the returned set. If the statement contains anything the conservative
/// walker can't fully resolve (CTEs, subqueries anywhere, FROM-functions,
/// set operations, INSERT … SELECT, etc.), returns `None` so the caller
/// falls back to `ensure_all_tables_loaded()`.
///
/// Per the rubber-duck critique: the looser `statement_referenced_tables`
/// helper above is under-inclusive (it ignores CTEs and is incomplete for
/// some expression shapes), so it is **not** safe to gate per-table loading
/// on. This function is the safe gate.
pub(crate) fn safe_referenced_tables(stmt: &Statement) -> Option<BTreeSet<String>> {
    let mut tables = BTreeSet::new();
    if !is_safe_statement(stmt, &mut tables, &BTreeSet::new()) {
        return None;
    }
    Some(tables)
}

fn is_safe_statement(
    stmt: &Statement,
    tables: &mut BTreeSet<String>,
    inherited_ctes: &BTreeSet<String>,
) -> bool {
    match stmt {
        Statement::Query(query) => is_safe_query(query, tables, inherited_ctes),
        Statement::Insert(insert) => {
            tables.insert(insert.table_name.clone());
            let source_safe = match &insert.source {
                InsertSource::Values(rows) => rows.iter().all(|row| {
                    row.iter()
                        .all(|e| is_safe_expr(e, tables, inherited_ctes, &BTreeSet::new()))
                }),
                InsertSource::Query(query) => is_safe_query(query, tables, inherited_ctes),
            };
            if !source_safe {
                return false;
            }
            let conflict_safe = match &insert.on_conflict {
                Some(ConflictAction::DoNothing { .. }) | None => true,
                Some(ConflictAction::DoUpdate {
                    assignments,
                    filter,
                    ..
                }) => {
                    assignments.iter().all(|assignment| {
                        is_safe_expr(&assignment.expr, tables, inherited_ctes, &BTreeSet::new())
                    }) && filter
                        .as_ref()
                        .map(|expr| is_safe_expr(expr, tables, inherited_ctes, &BTreeSet::new()))
                        .unwrap_or(true)
                }
            };
            conflict_safe
                && insert
                    .returning
                    .iter()
                    .all(|item| is_safe_select_item(item, tables, inherited_ctes, &BTreeSet::new()))
        }
        Statement::Update(update) => {
            tables.insert(update.table_name.clone());
            update
                .assignments
                .iter()
                .all(|a| is_safe_expr(&a.expr, tables, inherited_ctes, &BTreeSet::new()))
                && update
                    .filter
                    .as_ref()
                    .map(|f| is_safe_expr(f, tables, inherited_ctes, &BTreeSet::new()))
                    .unwrap_or(true)
        }
        Statement::Delete(delete) => {
            tables.insert(delete.table_name.clone());
            delete
                .filter
                .as_ref()
                .map(|f| is_safe_expr(f, tables, inherited_ctes, &BTreeSet::new()))
                .unwrap_or(true)
        }
        // DDL / metadata: we conservatively report unsafe so the all-loader
        // runs (CREATE INDEX, ALTER TABLE, etc. need the target table data
        // to rebuild structures, and may touch related catalog tables).
        _ => false,
    }
}

fn is_safe_query(
    query: &Query,
    tables: &mut BTreeSet<String>,
    inherited_ctes: &BTreeSet<String>,
) -> bool {
    if query.recursive {
        return false;
    }
    let local_ctes = query
        .ctes
        .iter()
        .map(|cte| cte.name.clone())
        .collect::<BTreeSet<_>>();
    let mut available_ctes = inherited_ctes.clone();
    for cte in &query.ctes {
        if !is_safe_query(&cte.query, tables, &available_ctes) {
            return false;
        }
        available_ctes.insert(cte.name.clone());
    }
    if !is_safe_query_body(&query.body, tables, &available_ctes, &local_ctes) {
        return false;
    }
    if !query
        .order_by
        .iter()
        .all(|o| is_safe_expr(&o.expr, tables, &available_ctes, &local_ctes))
    {
        return false;
    }
    if let Some(limit) = &query.limit {
        if !is_safe_expr(limit, tables, &available_ctes, &local_ctes) {
            return false;
        }
    }
    if let Some(offset) = &query.offset {
        if !is_safe_expr(offset, tables, &available_ctes, &local_ctes) {
            return false;
        }
    }
    true
}

fn is_safe_query_body(
    body: &QueryBody,
    tables: &mut BTreeSet<String>,
    available_ctes: &BTreeSet<String>,
    local_ctes: &BTreeSet<String>,
) -> bool {
    match body {
        QueryBody::Select(select) => is_safe_select(select, tables, available_ctes, local_ctes),
        QueryBody::SetOperation { left, right, .. } => {
            is_safe_query_body(left, tables, available_ctes, local_ctes)
                && is_safe_query_body(right, tables, available_ctes, local_ctes)
        }
        QueryBody::Values(rows) => rows.iter().all(|row| {
            row.iter()
                .all(|expr| is_safe_expr(expr, tables, available_ctes, local_ctes))
        }),
    }
}

fn is_safe_select(
    select: &Select,
    tables: &mut BTreeSet<String>,
    available_ctes: &BTreeSet<String>,
    local_ctes: &BTreeSet<String>,
) -> bool {
    for from in &select.from {
        if !is_safe_from_item(from, tables, available_ctes, local_ctes) {
            return false;
        }
    }
    for item in &select.projection {
        if !is_safe_select_item(item, tables, available_ctes, local_ctes) {
            return false;
        }
    }
    if let Some(filter) = &select.filter {
        if !is_safe_expr(filter, tables, available_ctes, local_ctes) {
            return false;
        }
    }
    if !select
        .group_by
        .iter()
        .all(|e| is_safe_expr(e, tables, available_ctes, local_ctes))
    {
        return false;
    }
    if !select
        .distinct_on
        .iter()
        .all(|e| is_safe_expr(e, tables, available_ctes, local_ctes))
    {
        return false;
    }
    if let Some(having) = &select.having {
        if !is_safe_expr(having, tables, available_ctes, local_ctes) {
            return false;
        }
    }
    true
}

fn is_safe_select_item(
    item: &SelectItem,
    tables: &mut BTreeSet<String>,
    available_ctes: &BTreeSet<String>,
    local_ctes: &BTreeSet<String>,
) -> bool {
    match item {
        SelectItem::Expr { expr, .. } => is_safe_expr(expr, tables, available_ctes, local_ctes),
        // Wildcards (`*`, `tbl.*`) are fine — they only reference tables
        // already in the FROM clause.
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => true,
    }
}

fn is_safe_from_item(
    item: &FromItem,
    tables: &mut BTreeSet<String>,
    available_ctes: &BTreeSet<String>,
    local_ctes: &BTreeSet<String>,
) -> bool {
    match item {
        FromItem::Table { name, .. } => {
            if cte_name_in_scope(name, available_ctes) {
                return true;
            }
            if cte_name_in_scope(name, local_ctes) {
                return false;
            }
            tables.insert(name.clone());
            true
        }
        FromItem::Join {
            left,
            right,
            constraint,
            ..
        } => {
            if !is_safe_from_item(left, tables, available_ctes, local_ctes)
                || !is_safe_from_item(right, tables, available_ctes, local_ctes)
            {
                return false;
            }
            match constraint {
                JoinConstraint::On(expr) => is_safe_expr(expr, tables, available_ctes, local_ctes),
                JoinConstraint::Using(_) | JoinConstraint::Natural => true,
            }
        }
        FromItem::Subquery { query, lateral, .. } => {
            !*lateral && is_safe_query(query, tables, available_ctes)
        }
        // Table-valued functions are not analyzed — fall back to load-all.
        FromItem::Function { .. } => false,
    }
}

#[allow(clippy::only_used_in_recursion)]
fn is_safe_expr(
    expr: &Expr,
    tables: &mut BTreeSet<String>,
    available_ctes: &BTreeSet<String>,
    local_ctes: &BTreeSet<String>,
) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter(_) => true,
        // Function and aggregate args may be expressions — walk them.
        Expr::Function { args, .. } | Expr::Aggregate { args, .. } => args
            .iter()
            .all(|a| is_safe_expr(a, tables, available_ctes, local_ctes)),
        Expr::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .all(|a| is_safe_expr(a, tables, available_ctes, local_ctes))
                && partition_by
                    .iter()
                    .all(|e| is_safe_expr(e, tables, available_ctes, local_ctes))
                && order_by
                    .iter()
                    .all(|o| is_safe_expr(&o.expr, tables, available_ctes, local_ctes))
        }
        Expr::RowNumber {
            partition_by,
            order_by,
            ..
        } => {
            partition_by
                .iter()
                .all(|e| is_safe_expr(e, tables, available_ctes, local_ctes))
                && order_by
                    .iter()
                    .all(|o| is_safe_expr(&o.expr, tables, available_ctes, local_ctes))
        }
        Expr::Unary { expr, .. } => is_safe_expr(expr, tables, available_ctes, local_ctes),
        Expr::Binary { left, right, .. } => {
            is_safe_expr(left, tables, available_ctes, local_ctes)
                && is_safe_expr(right, tables, available_ctes, local_ctes)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            is_safe_expr(expr, tables, available_ctes, local_ctes)
                && is_safe_expr(low, tables, available_ctes, local_ctes)
                && is_safe_expr(high, tables, available_ctes, local_ctes)
        }
        Expr::InList { expr, items, .. } => {
            is_safe_expr(expr, tables, available_ctes, local_ctes)
                && items
                    .iter()
                    .all(|i| is_safe_expr(i, tables, available_ctes, local_ctes))
        }
        Expr::IsNull { expr, .. } => is_safe_expr(expr, tables, available_ctes, local_ctes),
        Expr::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(op) = operand {
                if !is_safe_expr(op, tables, available_ctes, local_ctes) {
                    return false;
                }
            }
            for (when, then) in branches {
                if !is_safe_expr(when, tables, available_ctes, local_ctes)
                    || !is_safe_expr(then, tables, available_ctes, local_ctes)
                {
                    return false;
                }
            }
            if let Some(e) = else_expr {
                if !is_safe_expr(e, tables, available_ctes, local_ctes) {
                    return false;
                }
            }
            true
        }
        Expr::Cast { expr, .. } => is_safe_expr(expr, tables, available_ctes, local_ctes),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            is_safe_expr(expr, tables, available_ctes, local_ctes)
                && is_safe_expr(pattern, tables, available_ctes, local_ctes)
                && escape
                    .as_deref()
                    .map(|e| is_safe_expr(e, tables, available_ctes, local_ctes))
                    .unwrap_or(true)
        }
        Expr::Row(exprs) => exprs
            .iter()
            .all(|e| is_safe_expr(e, tables, available_ctes, local_ctes)),
        Expr::InSubquery { expr, query, .. } | Expr::CompareSubquery { expr, query, .. } => {
            is_safe_expr(expr, tables, available_ctes, local_ctes)
                && is_safe_query(query, tables, available_ctes)
        }
        Expr::ScalarSubquery(query) | Expr::Exists(query) => {
            is_safe_query(query, tables, available_ctes)
        }
    }
}

fn cte_name_in_scope(name: &str, cte_names: &BTreeSet<String>) -> bool {
    cte_names
        .iter()
        .any(|entry| entry.eq_ignore_ascii_case(name))
}
