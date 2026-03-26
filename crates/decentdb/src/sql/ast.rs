//! Strongly typed internal AST for the supported DecentDB 1.0 SQL subset.

use crate::catalog::ColumnType;
use crate::record::value::Value;

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
        restart_identity: bool,
    },
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
                lateral,
            } => {
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
