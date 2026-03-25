//! libpg_query tree normalization for the supported DecentDB SQL subset.

use libpg_query_sys::protobuf;
use libpg_query_sys::protobuf::node::Node as NodeEnum;

use crate::catalog::ColumnType;
use crate::error::{DbError, Result};
use crate::record::value::Value;

use super::ast::{
    AlterTableAction, Assignment, BinaryOp, ColumnDefinition, CommonTableExpr, ConflictAction,
    ConflictTarget, CreateIndexStatement, CreateTableStatement, CreateTriggerStatement,
    CreateViewStatement, DeleteStatement, ExplainStatement, Expr, ForeignKeyActionSpec,
    ForeignKeyDefinition, FromItem, IndexExpression, InsertSource, InsertStatement, JoinConstraint,
    JoinKind, OrderBy, Query, QueryBody, Select, SelectItem, SetOperation, Statement,
    TableConstraint, TriggerEventSpec, TriggerKindSpec, UnaryOp, UpdateStatement,
};

pub(crate) fn normalize_statement_text(sql: &str) -> Result<Statement> {
    let parsed = libpg_query_sys::parse_statement(sql)
        .map_err(|error| DbError::sql(error.message().to_string()))?;
    let raw = parsed
        .stmts
        .first()
        .and_then(|stmt| stmt.stmt.as_ref())
        .and_then(|stmt| stmt.node.as_ref())
        .ok_or_else(|| DbError::sql("parser returned an empty statement"))?;
    normalize_statement(raw, sql)
}

fn normalize_statement(node: &NodeEnum, original_sql: &str) -> Result<Statement> {
    match node {
        NodeEnum::SelectStmt(statement) => Ok(Statement::Query(normalize_query(statement)?)),
        NodeEnum::InsertStmt(statement) => Ok(Statement::Insert(normalize_insert(statement)?)),
        NodeEnum::UpdateStmt(statement) => Ok(Statement::Update(normalize_update(statement)?)),
        NodeEnum::DeleteStmt(statement) => Ok(Statement::Delete(normalize_delete(statement)?)),
        NodeEnum::CreateStmt(statement) => {
            Ok(Statement::CreateTable(normalize_create_table(statement)?))
        }
        NodeEnum::IndexStmt(statement) => {
            Ok(Statement::CreateIndex(normalize_create_index(statement)?))
        }
        NodeEnum::ViewStmt(statement) => {
            Ok(Statement::CreateView(normalize_create_view(statement)?))
        }
        NodeEnum::ExplainStmt(statement) => Ok(Statement::Explain(normalize_explain(
            statement,
            original_sql,
        )?)),
        NodeEnum::VacuumStmt(statement) => normalize_vacuum(statement),
        NodeEnum::DropStmt(statement) => normalize_drop(statement),
        NodeEnum::RenameStmt(statement) => normalize_rename(statement),
        NodeEnum::AlterTableStmt(statement) => {
            normalize_alter_table(statement).map(|(table_name, actions)| Statement::AlterTable {
                table_name,
                actions,
            })
        }
        NodeEnum::CreateTrigStmt(statement) => Ok(Statement::CreateTrigger(
            normalize_create_trigger(statement, original_sql)?,
        )),
        other => Err(unsupported(format!(
            "statement kind {} is not supported in DecentDB 1.0",
            describe_node(other)
        ))),
    }
}

fn normalize_query(statement: &protobuf::SelectStmt) -> Result<Query> {
    let recursive = statement
        .with_clause
        .as_ref()
        .is_some_and(|clause| clause.recursive);
    let ctes = normalize_with_clause(statement.with_clause.as_ref())?;
    let body = normalize_query_body(statement)?;
    let order_by = statement
        .sort_clause
        .iter()
        .map(normalize_order_by_node)
        .collect::<Result<Vec<_>>>()?;
    let limit_option = protobuf::LimitOption::try_from(statement.limit_option)
        .unwrap_or(protobuf::LimitOption::Undefined);
    let limit = match statement.limit_count.as_deref() {
        Some(node)
            if limit_option == protobuf::LimitOption::Count
                && matches!(node_kind(node)?, NodeEnum::AConst(value) if value.isnull) =>
        {
            None
        }
        Some(node) => Some(normalize_expr_node(node)?),
        None => None,
    };
    let offset = statement
        .limit_offset
        .as_deref()
        .map(normalize_expr_node)
        .transpose()?;
    Ok(Query {
        recursive,
        ctes,
        body,
        order_by,
        limit,
        offset,
    })
}

fn normalize_query_body(statement: &protobuf::SelectStmt) -> Result<QueryBody> {
    let op =
        protobuf::SetOperation::try_from(statement.op).unwrap_or(protobuf::SetOperation::SetopNone);
    if op != protobuf::SetOperation::SetopNone {
        let left = statement
            .larg
            .as_deref()
            .ok_or_else(|| unsupported("set operation is missing its left query"))?;
        let right = statement
            .rarg
            .as_deref()
            .ok_or_else(|| unsupported("set operation is missing its right query"))?;
        return Ok(QueryBody::SetOperation {
            op: match op {
                protobuf::SetOperation::SetopUnion => SetOperation::Union,
                protobuf::SetOperation::SetopIntersect => SetOperation::Intersect,
                protobuf::SetOperation::SetopExcept => SetOperation::Except,
                _ => {
                    return Err(unsupported(format!(
                        "set operation {} is not supported",
                        op.as_str_name()
                    )))
                }
            },
            all: statement.all,
            left: Box::new(normalize_query_body(left)?),
            right: Box::new(normalize_query_body(right)?),
        });
    }

    let projection = statement
        .target_list
        .iter()
        .map(normalize_select_item)
        .collect::<Result<Vec<_>>>()?;
    let from = statement
        .from_clause
        .iter()
        .map(normalize_from_item)
        .collect::<Result<Vec<_>>>()?;
    let filter = statement
        .where_clause
        .as_deref()
        .map(normalize_expr_node)
        .transpose()?;
    let group_by = statement
        .group_clause
        .iter()
        .map(normalize_expr_container)
        .collect::<Result<Vec<_>>>()?;
    let having = statement
        .having_clause
        .as_deref()
        .map(normalize_expr_node)
        .transpose()?;
    let (distinct, distinct_on) = normalize_distinct_clause(&statement.distinct_clause)?;
    Ok(QueryBody::Select(Select {
        projection,
        from,
        filter,
        group_by,
        having,
        distinct,
        distinct_on,
    }))
}

fn normalize_distinct_clause(distinct_clause: &[protobuf::Node]) -> Result<(bool, Vec<Expr>)> {
    if distinct_clause.is_empty() {
        return Ok((false, Vec::new()));
    }

    let distinct_on = distinct_clause
        .iter()
        .filter(|node| node.node.is_some())
        .map(normalize_expr_node)
        .collect::<Result<Vec<_>>>()?;
    Ok((true, distinct_on))
}

fn normalize_insert(statement: &protobuf::InsertStmt) -> Result<InsertStatement> {
    let table_name = normalize_range_var(
        statement
            .relation
            .as_ref()
            .ok_or_else(|| unsupported("INSERT is missing a target table"))?,
    )?;
    let columns = statement
        .cols
        .iter()
        .map(normalize_target_column)
        .collect::<Result<Vec<_>>>()?;
    let source_node = statement
        .select_stmt
        .as_deref()
        .ok_or_else(|| unsupported("INSERT is missing its source rows"))?;
    let source = normalize_insert_source(source_node)?;
    let on_conflict = statement
        .on_conflict_clause
        .as_deref()
        .map(normalize_on_conflict)
        .transpose()?;
    let returning = statement
        .returning_list
        .iter()
        .map(normalize_select_item)
        .collect::<Result<Vec<_>>>()?;
    Ok(InsertStatement {
        table_name,
        columns,
        source,
        on_conflict,
        returning,
    })
}

fn normalize_insert_source(node: &protobuf::Node) -> Result<InsertSource> {
    let kind = node_kind(node)?;
    match kind {
        NodeEnum::SelectStmt(select) if !select.values_lists.is_empty() => {
            Ok(InsertSource::Values(
                select
                    .values_lists
                    .iter()
                    .map(|row| match node_kind(row)? {
                        NodeEnum::List(list) => list
                            .items
                            .iter()
                            .map(normalize_expr_container)
                            .collect::<Result<Vec<_>>>(),
                        other => Err(unsupported(format!(
                            "VALUES row kind {} is not supported",
                            describe_node(other)
                        ))),
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
        NodeEnum::SelectStmt(select) => Ok(InsertSource::Query(Box::new(normalize_query(select)?))),
        other => Err(unsupported(format!(
            "INSERT source kind {} is not supported",
            describe_node(other)
        ))),
    }
}

fn normalize_on_conflict(clause: &protobuf::OnConflictClause) -> Result<ConflictAction> {
    let action = protobuf::OnConflictAction::try_from(clause.action)
        .unwrap_or(protobuf::OnConflictAction::Undefined);
    let target = normalize_conflict_target(clause.infer.as_deref())?;
    match action {
        protobuf::OnConflictAction::OnconflictNothing => Ok(ConflictAction::DoNothing { target }),
        protobuf::OnConflictAction::OnconflictUpdate => Ok(ConflictAction::DoUpdate {
            target,
            assignments: clause
                .target_list
                .iter()
                .map(normalize_assignment)
                .collect::<Result<Vec<_>>>()?,
            filter: clause
                .where_clause
                .as_deref()
                .map(normalize_expr_node)
                .transpose()?,
        }),
        _ => Err(unsupported("unsupported ON CONFLICT action")),
    }
}

fn normalize_conflict_target(target: Option<&protobuf::InferClause>) -> Result<ConflictTarget> {
    let Some(target) = target else {
        return Ok(ConflictTarget::Any);
    };
    if !target.conname.is_empty() {
        return Ok(ConflictTarget::Constraint(target.conname.clone()));
    }
    if target.where_clause.is_some() {
        return Err(unsupported(
            "partial conflict-target predicates are not supported",
        ));
    }
    let columns = target
        .index_elems
        .iter()
        .map(|node| match node_kind(node)? {
            NodeEnum::IndexElem(index) if !index.name.is_empty() => Ok(index.name.clone()),
            _ => Err(unsupported("unsupported ON CONFLICT target expression")),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ConflictTarget::Columns(columns))
}

fn normalize_update(statement: &protobuf::UpdateStmt) -> Result<UpdateStatement> {
    if !statement.from_clause.is_empty() {
        return Err(unsupported(
            "UPDATE ... FROM is not supported in DecentDB 1.0",
        ));
    }
    if !statement.returning_list.is_empty() {
        return Err(unsupported("UPDATE ... RETURNING is not supported"));
    }
    Ok(UpdateStatement {
        table_name: normalize_range_var(
            statement
                .relation
                .as_ref()
                .ok_or_else(|| unsupported("UPDATE is missing a target table"))?,
        )?,
        assignments: statement
            .target_list
            .iter()
            .map(normalize_assignment)
            .collect::<Result<Vec<_>>>()?,
        filter: statement
            .where_clause
            .as_deref()
            .map(normalize_expr_node)
            .transpose()?,
    })
}

fn normalize_delete(statement: &protobuf::DeleteStmt) -> Result<DeleteStatement> {
    if !statement.using_clause.is_empty() {
        return Err(unsupported(
            "DELETE ... USING is not supported in DecentDB 1.0",
        ));
    }
    if !statement.returning_list.is_empty() {
        return Err(unsupported("DELETE ... RETURNING is not supported"));
    }
    Ok(DeleteStatement {
        table_name: normalize_range_var(
            statement
                .relation
                .as_ref()
                .ok_or_else(|| unsupported("DELETE is missing a target table"))?,
        )?,
        filter: statement
            .where_clause
            .as_deref()
            .map(normalize_expr_node)
            .transpose()?,
    })
}

fn normalize_create_table(statement: &protobuf::CreateStmt) -> Result<CreateTableStatement> {
    let relation = statement
        .relation
        .as_ref()
        .ok_or_else(|| unsupported("CREATE TABLE is missing a relation name"))?;
    let table_name = normalize_range_var(relation)?;
    let mut columns = Vec::new();
    let mut constraints = Vec::new();
    for element in &statement.table_elts {
        match node_kind(element)? {
            NodeEnum::ColumnDef(column) => columns.push(normalize_column_definition(column)?),
            NodeEnum::Constraint(constraint) => {
                constraints.push(normalize_table_constraint(constraint)?)
            }
            other => {
                return Err(unsupported(format!(
                    "CREATE TABLE element {} is not supported",
                    describe_node(other)
                )))
            }
        }
    }
    Ok(CreateTableStatement {
        table_name,
        temporary: relation.relpersistence == "t",
        if_not_exists: statement.if_not_exists,
        columns,
        constraints,
    })
}

fn normalize_column_definition(column: &protobuf::ColumnDef) -> Result<ColumnDefinition> {
    let mut primary_key = false;
    let mut unique = false;
    let mut not_null = column.is_not_null;
    let mut default = column
        .raw_default
        .as_deref()
        .map(normalize_expr_node)
        .transpose()?;
    let mut generated = None;
    let mut checks = Vec::new();
    let mut references = None;
    for constraint in &column.constraints {
        match node_kind(constraint)? {
            NodeEnum::Constraint(constraint) => {
                match protobuf::ConstrType::try_from(constraint.contype)
                    .unwrap_or(protobuf::ConstrType::Undefined)
                {
                    protobuf::ConstrType::ConstrPrimary => primary_key = true,
                    protobuf::ConstrType::ConstrUnique => unique = true,
                    protobuf::ConstrType::ConstrDefault => {
                        default = constraint
                            .raw_expr
                            .as_deref()
                            .map(normalize_expr_node)
                            .transpose()?;
                    }
                    protobuf::ConstrType::ConstrGenerated => {
                        if constraint.generated_when != "a" {
                            return Err(unsupported("only GENERATED ALWAYS columns are supported"));
                        }
                        generated = Some(normalize_expr_node(
                            constraint.raw_expr.as_deref().ok_or_else(|| {
                                unsupported("GENERATED ALWAYS column is missing its expression")
                            })?,
                        )?);
                    }
                    protobuf::ConstrType::ConstrCheck => checks.push(normalize_expr_node(
                        constraint.raw_expr.as_deref().ok_or_else(|| {
                            unsupported("CHECK constraint is missing its expression")
                        })?,
                    )?),
                    protobuf::ConstrType::ConstrForeign => {
                        references = Some(normalize_foreign_key_constraint(
                            constraint,
                            vec![column.colname.clone()],
                        )?)
                    }
                    protobuf::ConstrType::ConstrNull => {}
                    protobuf::ConstrType::ConstrNotnull => not_null = true,
                    other => {
                        return Err(unsupported(format!(
                            "column constraint {} is not supported",
                            other.as_str_name()
                        )))
                    }
                }
            }
            other => {
                return Err(unsupported(format!(
                    "column constraint element {} is not supported",
                    describe_node(other)
                )))
            }
        }
    }

    Ok(ColumnDefinition {
        name: column.colname.clone(),
        column_type: normalize_type_name(
            column
                .type_name
                .as_ref()
                .ok_or_else(|| unsupported("column definition is missing a type"))?,
        )?,
        nullable: !not_null && !primary_key,
        default,
        generated,
        primary_key,
        unique,
        checks,
        references,
    })
}

fn normalize_table_constraint(constraint: &protobuf::Constraint) -> Result<TableConstraint> {
    let name = (!constraint.conname.is_empty()).then(|| constraint.conname.clone());
    match protobuf::ConstrType::try_from(constraint.contype)
        .unwrap_or(protobuf::ConstrType::Undefined)
    {
        protobuf::ConstrType::ConstrPrimary => Ok(TableConstraint::PrimaryKey {
            name,
            columns: normalize_constraint_keys(&constraint.keys)?,
        }),
        protobuf::ConstrType::ConstrUnique => Ok(TableConstraint::Unique {
            name,
            columns: normalize_constraint_keys(&constraint.keys)?,
        }),
        protobuf::ConstrType::ConstrCheck => Ok(TableConstraint::Check {
            name,
            expr: normalize_expr_node(
                constraint
                    .raw_expr
                    .as_deref()
                    .ok_or_else(|| unsupported("CHECK constraint is missing its expression"))?,
            )?,
        }),
        protobuf::ConstrType::ConstrForeign => Ok(TableConstraint::ForeignKey(
            normalize_foreign_key_constraint(
                constraint,
                normalize_constraint_keys(&constraint.fk_attrs)?,
            )?,
        )),
        other => Err(unsupported(format!(
            "table constraint {} is not supported",
            other.as_str_name()
        ))),
    }
}

fn normalize_foreign_key_constraint(
    constraint: &protobuf::Constraint,
    columns: Vec<String>,
) -> Result<ForeignKeyDefinition> {
    let referenced_table = normalize_range_var(
        constraint
            .pktable
            .as_ref()
            .ok_or_else(|| unsupported("foreign keys require an explicit parent table"))?,
    )?;
    Ok(ForeignKeyDefinition {
        name: (!constraint.conname.is_empty()).then(|| constraint.conname.clone()),
        columns,
        referenced_table,
        referenced_columns: normalize_constraint_keys(&constraint.pk_attrs)?,
        on_delete: normalize_fk_action(constraint.fk_del_action.as_str()),
        on_update: normalize_fk_action(constraint.fk_upd_action.as_str()),
    })
}

fn normalize_fk_action(action: &str) -> ForeignKeyActionSpec {
    match action {
        "a" | "" => ForeignKeyActionSpec::NoAction,
        "r" => ForeignKeyActionSpec::Restrict,
        "c" => ForeignKeyActionSpec::Cascade,
        "n" => ForeignKeyActionSpec::SetNull,
        _ => ForeignKeyActionSpec::NoAction,
    }
}

fn normalize_constraint_keys(nodes: &[protobuf::Node]) -> Result<Vec<String>> {
    nodes.iter().map(normalize_string_node).collect()
}

fn normalize_create_index(statement: &protobuf::IndexStmt) -> Result<CreateIndexStatement> {
    let access_method = if statement.access_method.is_empty() {
        "btree".to_string()
    } else {
        statement.access_method.clone()
    };
    if statement.concurrent {
        return Err(unsupported("CREATE INDEX CONCURRENTLY is not supported"));
    }
    Ok(CreateIndexStatement {
        index_name: statement.idxname.clone(),
        table_name: normalize_range_var(
            statement
                .relation
                .as_ref()
                .ok_or_else(|| unsupported("CREATE INDEX is missing a relation"))?,
        )?,
        unique: statement.unique,
        if_not_exists: statement.if_not_exists,
        access_method,
        columns: statement
            .index_params
            .iter()
            .map(|node| match node_kind(node)? {
                NodeEnum::IndexElem(index) if !index.name.is_empty() => {
                    Ok(IndexExpression::Column(index.name.clone()))
                }
                NodeEnum::IndexElem(index) if index.expr.is_some() => {
                    Ok(IndexExpression::Expr(normalize_expr_node(
                        index
                            .expr
                            .as_deref()
                            .ok_or_else(|| unsupported("index expression is missing its AST"))?,
                    )?))
                }
                _ => Err(unsupported("unsupported index key expression")),
            })
            .collect::<Result<Vec<_>>>()?,
        predicate: statement
            .where_clause
            .as_deref()
            .map(normalize_expr_node)
            .transpose()?,
    })
}

fn normalize_create_view(statement: &protobuf::ViewStmt) -> Result<CreateViewStatement> {
    let column_names = statement
        .aliases
        .iter()
        .map(normalize_string_node)
        .collect::<Result<Vec<_>>>()?;
    let view = statement
        .view
        .as_ref()
        .ok_or_else(|| unsupported("CREATE VIEW is missing a view name"))?;
    Ok(CreateViewStatement {
        view_name: normalize_range_var(view)?,
        temporary: view.relpersistence == "t",
        replace: statement.replace,
        column_names,
        query: normalize_query(as_select_stmt(
            statement
                .query
                .as_deref()
                .ok_or_else(|| unsupported("CREATE VIEW is missing its SELECT"))?,
        )?)?,
    })
}

fn normalize_explain(
    statement: &protobuf::ExplainStmt,
    original_sql: &str,
) -> Result<ExplainStatement> {
    let analyze = original_sql
        .to_ascii_uppercase()
        .contains("EXPLAIN ANALYZE");
    let query = statement
        .query
        .as_deref()
        .ok_or_else(|| unsupported("EXPLAIN is missing its inner statement"))?;
    Ok(ExplainStatement {
        analyze,
        statement: Box::new(normalize_statement(node_kind(query)?, original_sql)?),
    })
}

fn normalize_vacuum(statement: &protobuf::VacuumStmt) -> Result<Statement> {
    if statement.is_vacuumcmd {
        return Err(unsupported("VACUUM is not supported in DecentDB 1.0"));
    }
    if !statement.options.is_empty() {
        return Err(unsupported(
            "ANALYZE options are not supported in DecentDB 1.0",
        ));
    }
    if statement.rels.len() > 1 {
        return Err(unsupported(
            "ANALYZE only supports zero or one target table in DecentDB 1.0",
        ));
    }
    let table_name = statement
        .rels
        .first()
        .map(|relation| match node_kind(relation)? {
            NodeEnum::VacuumRelation(relation) => {
                if !relation.va_cols.is_empty() {
                    return Err(unsupported(
                        "ANALYZE column lists are not supported in DecentDB 1.0",
                    ));
                }
                normalize_range_var(
                    relation
                        .relation
                        .as_ref()
                        .ok_or_else(|| unsupported("ANALYZE is missing its target relation"))?,
                )
            }
            other => Err(unsupported(format!(
                "unexpected ANALYZE target {}",
                describe_node(other)
            ))),
        })
        .transpose()?;
    Ok(Statement::Analyze { table_name })
}

fn normalize_drop(statement: &protobuf::DropStmt) -> Result<Statement> {
    if statement.objects.len() != 1 {
        return Err(unsupported(
            "DROP statements must target exactly one object",
        ));
    }
    let name_parts = normalize_object_name_list(&statement.objects[0])?;
    let object_type = protobuf::ObjectType::try_from(statement.remove_type)
        .unwrap_or(protobuf::ObjectType::Undefined);
    match object_type {
        protobuf::ObjectType::ObjectTable => Ok(Statement::DropTable {
            name: join_name_parts(&name_parts),
            if_exists: statement.missing_ok,
        }),
        protobuf::ObjectType::ObjectIndex => Ok(Statement::DropIndex {
            name: join_name_parts(&name_parts),
            if_exists: statement.missing_ok,
        }),
        protobuf::ObjectType::ObjectView => Ok(Statement::DropView {
            name: join_name_parts(&name_parts),
            if_exists: statement.missing_ok,
        }),
        protobuf::ObjectType::ObjectTrigger => {
            if name_parts.len() < 2 {
                return Err(unsupported(
                    "DROP TRIGGER must include the target table name",
                ));
            }
            Ok(Statement::DropTrigger {
                name: name_parts
                    .last()
                    .cloned()
                    .ok_or_else(|| unsupported("DROP TRIGGER is missing the trigger name"))?,
                table_name: join_name_parts(&name_parts[..name_parts.len() - 1]),
                if_exists: statement.missing_ok,
            })
        }
        other => Err(unsupported(format!(
            "DROP {} is not supported in DecentDB 1.0",
            other.as_str_name()
        ))),
    }
}

fn normalize_rename(statement: &protobuf::RenameStmt) -> Result<Statement> {
    let rename_type = protobuf::ObjectType::try_from(statement.rename_type)
        .unwrap_or(protobuf::ObjectType::Undefined);
    match rename_type {
        protobuf::ObjectType::ObjectView => Ok(Statement::AlterViewRename {
            view_name: normalize_range_var(
                statement
                    .relation
                    .as_ref()
                    .ok_or_else(|| unsupported("ALTER VIEW RENAME is missing the view name"))?,
            )?,
            new_name: statement.newname.clone(),
        }),
        protobuf::ObjectType::ObjectColumn => {
            Ok(Statement::AlterTable {
                table_name: normalize_range_var(statement.relation.as_ref().ok_or_else(|| {
                    unsupported("ALTER TABLE RENAME COLUMN is missing the table")
                })?)?,
                actions: vec![AlterTableAction::RenameColumn {
                    old_name: statement.subname.clone(),
                    new_name: statement.newname.clone(),
                }],
            })
        }
        other => Err(unsupported(format!(
            "RENAME on object type {} is not supported",
            other.as_str_name()
        ))),
    }
}

fn normalize_alter_table(
    statement: &protobuf::AlterTableStmt,
) -> Result<(String, Vec<AlterTableAction>)> {
    let table_name = normalize_range_var(
        statement
            .relation
            .as_ref()
            .ok_or_else(|| unsupported("ALTER TABLE is missing a target table"))?,
    )?;
    let actions = statement
        .cmds
        .iter()
        .map(|node| match node_kind(node)? {
            NodeEnum::AlterTableCmd(command) => normalize_alter_table_command(command),
            _ => Err(unsupported("unsupported ALTER TABLE action")),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((table_name, actions))
}

fn normalize_alter_table_command(command: &protobuf::AlterTableCmd) -> Result<AlterTableAction> {
    let subtype = protobuf::AlterTableType::try_from(command.subtype)
        .unwrap_or(protobuf::AlterTableType::Undefined);
    match subtype {
        protobuf::AlterTableType::AtAddColumn => {
            let definition = command
                .def
                .as_deref()
                .ok_or_else(|| unsupported("ALTER TABLE ADD COLUMN is missing a definition"))?;
            match node_kind(definition)? {
                NodeEnum::ColumnDef(column) => Ok(AlterTableAction::AddColumn(
                    normalize_column_definition(column)?,
                )),
                _ => Err(unsupported("unsupported ALTER TABLE ADD COLUMN definition")),
            }
        }
        protobuf::AlterTableType::AtDropColumn => Ok(AlterTableAction::DropColumn {
            column_name: command.name.clone(),
        }),
        protobuf::AlterTableType::AtAlterColumnType => {
            let type_node = command
                .def
                .as_deref()
                .ok_or_else(|| unsupported("ALTER TABLE ALTER COLUMN TYPE is missing a type"))?;
            match node_kind(type_node)? {
                NodeEnum::TypeName(type_name) => Ok(AlterTableAction::AlterColumnType {
                    column_name: command.name.clone(),
                    new_type: normalize_type_name(type_name)?,
                }),
                NodeEnum::ColumnDef(column) => {
                    Ok(AlterTableAction::AlterColumnType {
                        column_name: command.name.clone(),
                        new_type: normalize_type_name(column.type_name.as_ref().ok_or_else(
                            || unsupported("ALTER COLUMN TYPE is missing its type"),
                        )?)?,
                    })
                }
                _ => Err(unsupported("unsupported ALTER TABLE type specification")),
            }
        }
        other => Err(unsupported(format!(
            "ALTER TABLE action {} is not supported",
            other.as_str_name()
        ))),
    }
}

fn normalize_create_trigger(
    statement: &protobuf::CreateTrigStmt,
    original_sql: &str,
) -> Result<CreateTriggerStatement> {
    if statement.replace || statement.isconstraint || !statement.row {
        return Err(unsupported(
            "only CREATE TRIGGER ... FOR EACH ROW is supported in DecentDB 1.0",
        ));
    }
    let function_name = normalize_qualified_name(&statement.funcname)?;
    if function_name != "decentdb_exec_sql" {
        return Err(unsupported(
            "trigger action must call decentdb_exec_sql('<single DML SQL>')",
        ));
    }
    let action_sql = statement
        .args
        .first()
        .and_then(|node| match node_kind(node).ok()? {
            NodeEnum::String(value) => Some(value.sval.clone()),
            _ => None,
        })
        .ok_or_else(|| unsupported("trigger action SQL must be a single string literal"))?;
    let upper_sql = original_sql.to_ascii_uppercase();
    let kind = if upper_sql.contains("INSTEAD OF") {
        TriggerKindSpec::InsteadOf
    } else if upper_sql.contains("AFTER") {
        TriggerKindSpec::After
    } else {
        return Err(unsupported("unsupported trigger timing"));
    };
    let event = if upper_sql.contains(" INSERT ") {
        TriggerEventSpec::Insert
    } else if upper_sql.contains(" UPDATE ") {
        TriggerEventSpec::Update
    } else if upper_sql.contains(" DELETE ") {
        TriggerEventSpec::Delete
    } else {
        return Err(unsupported("unsupported trigger event"));
    };
    Ok(CreateTriggerStatement {
        trigger_name: statement.trigname.clone(),
        target_name: normalize_range_var(
            statement
                .relation
                .as_ref()
                .ok_or_else(|| unsupported("CREATE TRIGGER is missing its target relation"))?,
        )?,
        kind,
        event,
        action_sql,
    })
}

fn normalize_select_item(node: &protobuf::Node) -> Result<SelectItem> {
    match node_kind(node)? {
        NodeEnum::ResTarget(target) => {
            if let Some(value) = target.val.as_deref() {
                match node_kind(value)? {
                    NodeEnum::ColumnRef(column) if column.fields.len() == 1 => {
                        if matches!(node_kind(&column.fields[0])?, NodeEnum::AStar(_)) {
                            return Ok(SelectItem::Wildcard);
                        }
                    }
                    NodeEnum::ColumnRef(column) if column.fields.len() == 2 => {
                        if matches!(node_kind(&column.fields[1])?, NodeEnum::AStar(_)) {
                            return Ok(SelectItem::QualifiedWildcard(normalize_string_node(
                                &column.fields[0],
                            )?));
                        }
                    }
                    _ => {}
                }
            }
            Ok(SelectItem::Expr {
                expr: normalize_expr_node(
                    target
                        .val
                        .as_deref()
                        .ok_or_else(|| unsupported("SELECT target is missing its value"))?,
                )?,
                alias: (!target.name.is_empty()).then(|| target.name.clone()),
            })
        }
        other => Err(unsupported(format!(
            "select item {} is not supported",
            describe_node(other)
        ))),
    }
}

fn normalize_from_item(node: &protobuf::Node) -> Result<FromItem> {
    match node_kind(node)? {
        NodeEnum::RangeVar(range) => Ok(FromItem::Table {
            name: normalize_range_var(range)?,
            alias: range.alias.as_ref().map(|alias| alias.aliasname.clone()),
        }),
        NodeEnum::RangeSubselect(range) => Ok(FromItem::Subquery {
            query: Box::new(normalize_query(as_select_stmt(
                range
                    .subquery
                    .as_deref()
                    .ok_or_else(|| unsupported("subquery is missing its SELECT"))?,
            )?)?),
            alias: range
                .alias
                .as_ref()
                .map(|alias| alias.aliasname.clone())
                .ok_or_else(|| unsupported("subqueries in FROM require an alias"))?,
        }),
        NodeEnum::RangeFunction(range) => normalize_range_function(range),
        NodeEnum::JoinExpr(join) => Ok(FromItem::Join {
            left: Box::new(normalize_from_item(
                join.larg
                    .as_deref()
                    .ok_or_else(|| unsupported("JOIN is missing its left input"))?,
            )?),
            right: Box::new(normalize_from_item(
                join.rarg
                    .as_deref()
                    .ok_or_else(|| unsupported("JOIN is missing its right input"))?,
            )?),
            kind: match protobuf::JoinType::try_from(join.jointype)
                .unwrap_or(protobuf::JoinType::Undefined)
            {
                protobuf::JoinType::JoinInner
                    if join.quals.is_none() && !join.is_natural && join.using_clause.is_empty() =>
                {
                    JoinKind::Cross
                }
                protobuf::JoinType::JoinInner => JoinKind::Inner,
                protobuf::JoinType::JoinLeft => JoinKind::Left,
                protobuf::JoinType::JoinRight => JoinKind::Right,
                protobuf::JoinType::JoinFull => JoinKind::Full,
                other => {
                    return Err(unsupported(format!(
                        "join type {} is not supported",
                        other.as_str_name()
                    )))
                }
            },
            constraint: if join.is_natural {
                JoinConstraint::Natural
            } else if !join.using_clause.is_empty() {
                JoinConstraint::Using(
                    join.using_clause
                        .iter()
                        .map(normalize_string_node)
                        .collect::<Result<Vec<_>>>()?,
                )
            } else {
                JoinConstraint::On(match join.quals.as_deref() {
                    Some(quals) => normalize_expr_node(quals)?,
                    None => Expr::Literal(Value::Bool(true)),
                })
            },
        }),
        other => Err(unsupported(format!(
            "FROM source {} is not supported",
            describe_node(other)
        ))),
    }
}

fn normalize_range_function(range: &protobuf::RangeFunction) -> Result<FromItem> {
    if range.lateral {
        return Err(unsupported("LATERAL table functions are not supported"));
    }
    if range.ordinality {
        return Err(unsupported("WITH ORDINALITY is not supported"));
    }
    if range.is_rowsfrom {
        return Err(unsupported("ROWS FROM (...) is not supported"));
    }
    if !range.coldeflist.is_empty() {
        return Err(unsupported(
            "table-function column definitions are not supported",
        ));
    }
    if range.functions.len() != 1 {
        return Err(unsupported(
            "FROM only supports one table function at a time in DecentDB v0",
        ));
    }
    let function = range
        .functions
        .first()
        .ok_or_else(|| unsupported("table function is missing its call"))?;
    let NodeEnum::List(list) = node_kind(function)? else {
        return Err(unsupported("table function entry is malformed"));
    };
    let call = list
        .items
        .first()
        .ok_or_else(|| unsupported("table function is missing its call"))?;
    let NodeEnum::FuncCall(call) = node_kind(call)? else {
        return Err(unsupported("table function entry is malformed"));
    };
    let name = normalize_qualified_name(&call.funcname)?;
    if !matches!(
        name.as_str(),
        "json_each" | "json_tree" | "pg_catalog.json_each" | "pg_catalog.json_tree"
    ) {
        return Err(unsupported(format!(
            "table function {name} is not supported"
        )));
    }
    let args = call
        .args
        .iter()
        .map(normalize_expr_container)
        .collect::<Result<Vec<_>>>()?;
    Ok(FromItem::Function {
        name,
        args,
        alias: range.alias.as_ref().map(|alias| alias.aliasname.clone()),
    })
}

fn normalize_assignment(node: &protobuf::Node) -> Result<Assignment> {
    match node_kind(node)? {
        NodeEnum::ResTarget(target) => Ok(Assignment {
            column_name: if target.name.is_empty() {
                return Err(unsupported("assignment target is missing its column name"));
            } else {
                target.name.clone()
            },
            expr: normalize_expr_node(
                target
                    .val
                    .as_deref()
                    .ok_or_else(|| unsupported("assignment target is missing its value"))?,
            )?,
        }),
        other => Err(unsupported(format!(
            "assignment node {} is not supported",
            describe_node(other)
        ))),
    }
}

fn normalize_order_by_node(node: &protobuf::Node) -> Result<OrderBy> {
    match node_kind(node)? {
        NodeEnum::SortBy(sort) => Ok(OrderBy {
            expr: normalize_expr_node(
                sort.node
                    .as_deref()
                    .ok_or_else(|| unsupported("ORDER BY term is missing its expression"))?,
            )?,
            descending: matches!(
                protobuf::SortByDir::try_from(sort.sortby_dir)
                    .unwrap_or(protobuf::SortByDir::SortbyDefault),
                protobuf::SortByDir::SortbyDesc
            ),
        }),
        other => Err(unsupported(format!(
            "ORDER BY node {} is not supported",
            describe_node(other)
        ))),
    }
}

fn normalize_expr_container(node: &protobuf::Node) -> Result<Expr> {
    normalize_expr_node(node)
}

fn normalize_expr_node(node: &protobuf::Node) -> Result<Expr> {
    match node_kind(node)? {
        NodeEnum::AConst(value) => normalize_const(value),
        NodeEnum::ColumnRef(column) => normalize_column_ref(column),
        NodeEnum::ParamRef(parameter) => {
            let number = usize::try_from(parameter.number)
                .map_err(|_| unsupported("parameter number is out of range"))?;
            if number == 0 {
                return Err(unsupported("parameter numbers start at $1"));
            }
            Ok(Expr::Parameter(number))
        }
        NodeEnum::AExpr(expr) => normalize_aexpr(expr),
        NodeEnum::BoolExpr(expr) => normalize_bool_expr(expr),
        NodeEnum::FuncCall(call) => normalize_function_call(call),
        NodeEnum::TypeCast(cast) => Ok(Expr::Cast {
            expr: Box::new(normalize_expr_node(
                cast.arg
                    .as_deref()
                    .ok_or_else(|| unsupported("CAST is missing its expression"))?,
            )?),
            target_type: normalize_type_name(
                cast.type_name
                    .as_ref()
                    .ok_or_else(|| unsupported("CAST is missing its target type"))?,
            )?,
        }),
        NodeEnum::CaseExpr(case) => normalize_case_expr(case),
        NodeEnum::NullTest(test) => Ok(Expr::IsNull {
            expr: Box::new(normalize_expr_node(
                test.arg
                    .as_deref()
                    .ok_or_else(|| unsupported("NULL test is missing its expression"))?,
            )?),
            negated: matches!(
                protobuf::NullTestType::try_from(test.nulltesttype)
                    .unwrap_or(protobuf::NullTestType::IsNull),
                protobuf::NullTestType::IsNotNull
            ),
        }),
        NodeEnum::SqlvalueFunction(value) => normalize_sql_value_function(value),
        NodeEnum::CoalesceExpr(expr) => Ok(Expr::Function {
            name: "coalesce".to_string(),
            args: expr
                .args
                .iter()
                .map(normalize_expr_container)
                .collect::<Result<Vec<_>>>()?,
        }),
        NodeEnum::NullIfExpr(expr) => Ok(Expr::Function {
            name: "nullif".to_string(),
            args: expr
                .args
                .iter()
                .map(normalize_expr_container)
                .collect::<Result<Vec<_>>>()?,
        }),
        NodeEnum::SubLink(link) => normalize_sublink(link),
        NodeEnum::AArrayExpr(array) => Ok(Expr::Function {
            name: "array".to_string(),
            args: array
                .elements
                .iter()
                .map(normalize_expr_container)
                .collect::<Result<Vec<_>>>()?,
        }),
        NodeEnum::JsonArrayConstructor(array) => normalize_json_array_constructor(array),
        NodeEnum::JsonValueExpr(expr) => normalize_json_value_expr(expr),
        other => Err(unsupported(format!(
            "expression node {} is not supported",
            describe_node(other)
        ))),
    }
}

fn normalize_json_array_constructor(array: &protobuf::JsonArrayConstructor) -> Result<Expr> {
    if array.output.is_some() {
        return Err(unsupported("JSON_ARRAY output clauses are not supported"));
    }
    Ok(Expr::Function {
        name: "json_array".to_string(),
        args: array
            .exprs
            .iter()
            .map(normalize_expr_container)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn normalize_json_value_expr(expr: &protobuf::JsonValueExpr) -> Result<Expr> {
    let node = expr
        .raw_expr
        .as_deref()
        .or(expr.formatted_expr.as_deref())
        .ok_or_else(|| unsupported("JSON value expression is missing its input"))?;
    normalize_expr_node(node)
}

fn normalize_sql_value_function(value: &protobuf::SqlValueFunction) -> Result<Expr> {
    let name = match protobuf::SqlValueFunctionOp::try_from(value.op)
        .unwrap_or(protobuf::SqlValueFunctionOp::SqlvalueFunctionOpUndefined)
    {
        protobuf::SqlValueFunctionOp::SvfopCurrentDate => "current_date",
        protobuf::SqlValueFunctionOp::SvfopCurrentTime => "current_time",
        protobuf::SqlValueFunctionOp::SvfopCurrentTimestamp => "current_timestamp",
        protobuf::SqlValueFunctionOp::SvfopLocaltime => "localtime",
        protobuf::SqlValueFunctionOp::SvfopLocaltimestamp => "localtimestamp",
        protobuf::SqlValueFunctionOp::SvfopCurrentTimeN
        | protobuf::SqlValueFunctionOp::SvfopCurrentTimestampN
        | protobuf::SqlValueFunctionOp::SvfopLocaltimeN
        | protobuf::SqlValueFunctionOp::SvfopLocaltimestampN => {
            return Err(unsupported(
                "CURRENT_TIME/LOCALTIME/CURRENT_TIMESTAMP/LOCALTIMESTAMP precision modifiers are not supported yet",
            ));
        }
        other => {
            return Err(unsupported(format!(
                "SQL value function {} is not supported",
                other.as_str_name()
            )))
        }
    };
    Ok(Expr::Function {
        name: name.to_string(),
        args: Vec::new(),
    })
}

fn normalize_const(value: &protobuf::AConst) -> Result<Expr> {
    if value.isnull {
        return Ok(Expr::Literal(Value::Null));
    }
    Ok(Expr::Literal(match value.val.as_ref() {
        Some(protobuf::a_const::Val::Ival(value)) => Value::Int64(i64::from(value.ival)),
        Some(protobuf::a_const::Val::Fval(value)) => Value::Float64(
            value
                .fval
                .parse::<f64>()
                .map_err(|_| unsupported("invalid floating-point literal"))?,
        ),
        Some(protobuf::a_const::Val::Boolval(value)) => Value::Bool(value.boolval),
        Some(protobuf::a_const::Val::Sval(value)) => Value::Text(value.sval.clone()),
        Some(protobuf::a_const::Val::Bsval(value)) => Value::Text(value.bsval.clone()),
        None => Value::Null,
    }))
}

fn normalize_column_ref(column: &protobuf::ColumnRef) -> Result<Expr> {
    let parts = column
        .fields
        .iter()
        .filter_map(|field| match node_kind(field).ok()? {
            NodeEnum::String(value) => Some(value.sval.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [column] => Ok(Expr::Column {
            table: None,
            column: column.clone(),
        }),
        [table, column] => Ok(Expr::Column {
            table: Some(table.clone()),
            column: column.clone(),
        }),
        _ => Err(unsupported("unsupported column reference shape")),
    }
}

fn normalize_aexpr(expr: &protobuf::AExpr) -> Result<Expr> {
    let kind = protobuf::AExprKind::try_from(expr.kind).unwrap_or(protobuf::AExprKind::Undefined);
    match kind {
        protobuf::AExprKind::AexprOp
        | protobuf::AExprKind::AexprDistinct
        | protobuf::AExprKind::AexprNotDistinct => {
            let operator = normalize_operator_name(&expr.name)?;
            if kind == protobuf::AExprKind::AexprOp && operator == "-" && expr.lexpr.is_none() {
                return Ok(Expr::Unary {
                    op: UnaryOp::Negate,
                    expr: Box::new(normalize_expr_node(
                        expr.rexpr
                            .as_deref()
                            .ok_or_else(|| unsupported("unary - is missing its operand"))?,
                    )?),
                });
            }
            Ok(Expr::Binary {
                left: Box::new(normalize_expr_node(expr.lexpr.as_deref().ok_or_else(
                    || unsupported("binary operator is missing its left operand"),
                )?)?),
                op: match (kind, operator.as_str()) {
                    (protobuf::AExprKind::AexprDistinct, _) => BinaryOp::IsDistinctFrom,
                    (protobuf::AExprKind::AexprNotDistinct, _) => BinaryOp::IsNotDistinctFrom,
                    (_, "=") => BinaryOp::Eq,
                    (_, "<>") | (_, "!=") => BinaryOp::NotEq,
                    (_, "<") => BinaryOp::Lt,
                    (_, "<=") => BinaryOp::LtEq,
                    (_, ">") => BinaryOp::Gt,
                    (_, ">=") => BinaryOp::GtEq,
                    (_, "+") => BinaryOp::Add,
                    (_, "-") => BinaryOp::Sub,
                    (_, "*") => BinaryOp::Mul,
                    (_, "/") => BinaryOp::Div,
                    (_, "%") => BinaryOp::Mod,
                    (_, "||") => BinaryOp::Concat,
                    (_, "->") => BinaryOp::JsonExtract,
                    (_, "->>") => BinaryOp::JsonExtractText,
                    _ => return Err(unsupported(format!("operator {operator} is not supported"))),
                },
                right: Box::new(normalize_expr_node(expr.rexpr.as_deref().ok_or_else(
                    || unsupported("binary operator is missing its right operand"),
                )?)?),
            })
        }
        protobuf::AExprKind::AexprIn => {
            let row = expr
                .rexpr
                .as_deref()
                .ok_or_else(|| unsupported("IN is missing its right-hand values"))?;
            let items = match node_kind(row)? {
                NodeEnum::List(list) => list
                    .items
                    .iter()
                    .map(normalize_expr_container)
                    .collect::<Result<Vec<_>>>()?,
                _ => return Err(unsupported("IN only supports explicit value lists")),
            };
            let negated = match expr.name.first().and_then(|node| node_kind(node).ok()) {
                Some(NodeEnum::String(s)) => s.sval == "<>",
                _ => false,
            };
            Ok(Expr::InList {
                expr: Box::new(normalize_expr_node(
                    expr.lexpr
                        .as_deref()
                        .ok_or_else(|| unsupported("IN is missing its left operand"))?,
                )?),
                items,
                negated,
            })
        }
        protobuf::AExprKind::AexprLike | protobuf::AExprKind::AexprIlike => {
            let (pattern, escape) = normalize_like_pattern(
                expr.rexpr
                    .as_deref()
                    .ok_or_else(|| unsupported("LIKE is missing its pattern"))?,
            )?;
            Ok(Expr::Like {
                expr: Box::new(normalize_expr_node(
                    expr.lexpr
                        .as_deref()
                        .ok_or_else(|| unsupported("LIKE is missing its left operand"))?,
                )?),
                pattern: Box::new(pattern),
                escape: escape.map(Box::new),
                case_insensitive: kind == protobuf::AExprKind::AexprIlike,
                negated: false,
            })
        }
        protobuf::AExprKind::AexprBetween | protobuf::AExprKind::AexprNotBetween => {
            let bounds = match node_kind(
                expr.rexpr
                    .as_deref()
                    .ok_or_else(|| unsupported("BETWEEN is missing its bounds"))?,
            )? {
                NodeEnum::List(list) if list.items.len() == 2 => list.items.as_slice(),
                _ => return Err(unsupported("BETWEEN requires exactly two bounds")),
            };
            Ok(Expr::Between {
                expr: Box::new(normalize_expr_node(
                    expr.lexpr
                        .as_deref()
                        .ok_or_else(|| unsupported("BETWEEN is missing its left operand"))?,
                )?),
                low: Box::new(normalize_expr_container(&bounds[0])?),
                high: Box::new(normalize_expr_container(&bounds[1])?),
                negated: kind == protobuf::AExprKind::AexprNotBetween,
            })
        }
        protobuf::AExprKind::AexprNullif => {
            let left = normalize_expr_node(
                expr.lexpr
                    .as_deref()
                    .ok_or_else(|| unsupported("NULLIF is missing its left operand"))?,
            )?;
            let right = normalize_expr_node(
                expr.rexpr
                    .as_deref()
                    .ok_or_else(|| unsupported("NULLIF is missing its right operand"))?,
            )?;
            Ok(Expr::Case {
                operand: None,
                branches: vec![(
                    Expr::Binary {
                        left: Box::new(left.clone()),
                        op: BinaryOp::Eq,
                        right: Box::new(right),
                    },
                    Expr::Literal(Value::Null),
                )],
                else_expr: Some(Box::new(left)),
            })
        }
        other => Err(unsupported(format!(
            "expression kind {} is not supported",
            other.as_str_name()
        ))),
    }
}

fn normalize_bool_expr(expr: &protobuf::BoolExpr) -> Result<Expr> {
    let args = expr
        .args
        .iter()
        .map(normalize_expr_container)
        .collect::<Result<Vec<_>>>()?;
    match protobuf::BoolExprType::try_from(expr.boolop).unwrap_or(protobuf::BoolExprType::AndExpr) {
        protobuf::BoolExprType::NotExpr => Ok(Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(
                args.into_iter()
                    .next()
                    .ok_or_else(|| unsupported("NOT is missing its operand"))?,
            ),
        }),
        protobuf::BoolExprType::AndExpr => fold_binary(BinaryOp::And, args),
        protobuf::BoolExprType::OrExpr => fold_binary(BinaryOp::Or, args),
        protobuf::BoolExprType::Undefined => Err(unsupported("unsupported boolean expression")),
    }
}

fn normalize_function_call(call: &protobuf::FuncCall) -> Result<Expr> {
    let name = normalize_qualified_name(&call.funcname)?;
    let args = call
        .args
        .iter()
        .map(normalize_expr_container)
        .collect::<Result<Vec<_>>>()?;
    if call.over.is_some() {
        if !matches!(
            name.as_str(),
            "row_number"
                | "rank"
                | "dense_rank"
                | "lag"
                | "lead"
                | "first_value"
                | "last_value"
                | "nth_value"
        ) {
            return Err(unsupported(
                "only ROW_NUMBER(), RANK(), DENSE_RANK(), LAG(), LEAD(), FIRST_VALUE(), LAST_VALUE(), and NTH_VALUE() OVER (...) are supported as window functions",
            ));
        }
        let window = call
            .over
            .as_deref()
            .ok_or_else(|| unsupported("window function is missing its OVER clause"))?;
        if window.order_clause.is_empty() {
            return Err(unsupported(
                "window functions require ORDER BY in OVER (...)",
            ));
        }
        let partition_by = window
            .partition_clause
            .iter()
            .map(normalize_expr_container)
            .collect::<Result<Vec<_>>>()?;
        let order_by = window
            .order_clause
            .iter()
            .map(normalize_order_by_node)
            .collect::<Result<Vec<_>>>()?;
        return if name == "row_number" {
            Ok(Expr::RowNumber {
                partition_by,
                order_by,
            })
        } else {
            Ok(Expr::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
            })
        };
    }
    if matches!(
        name.as_str(),
        "count" | "sum" | "avg" | "min" | "max" | "group_concat" | "string_agg" | "total"
    ) {
        return Ok(Expr::Aggregate {
            name,
            args,
            star: call.agg_star,
            distinct: call.agg_distinct,
        });
    }

    Ok(Expr::Function { name, args })
}

fn normalize_like_pattern(node: &protobuf::Node) -> Result<(Expr, Option<Expr>)> {
    match node_kind(node)? {
        NodeEnum::FuncCall(call)
            if normalize_qualified_name(&call.funcname)? == "pg_catalog.like_escape" =>
        {
            if call.args.len() != 2 {
                return Err(unsupported(
                    "pg_catalog.like_escape requires a pattern and escape expression",
                ));
            }
            Ok((
                normalize_expr_container(&call.args[0])?,
                Some(normalize_expr_container(&call.args[1])?),
            ))
        }
        _ => Ok((normalize_expr_node(node)?, None)),
    }
}

fn normalize_case_expr(case: &protobuf::CaseExpr) -> Result<Expr> {
    let operand = case
        .arg
        .as_deref()
        .map(normalize_expr_node)
        .transpose()?
        .map(Box::new);
    let branches = case
        .args
        .iter()
        .map(|node| match node_kind(node)? {
            NodeEnum::CaseWhen(branch) => Ok((
                normalize_expr_node(
                    branch
                        .expr
                        .as_deref()
                        .ok_or_else(|| unsupported("CASE branch is missing its condition"))?,
                )?,
                normalize_expr_node(
                    branch
                        .result
                        .as_deref()
                        .ok_or_else(|| unsupported("CASE branch is missing its result"))?,
                )?,
            )),
            _ => Err(unsupported("CASE contains an unsupported branch")),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Expr::Case {
        operand,
        branches,
        else_expr: case
            .defresult
            .as_deref()
            .map(normalize_expr_node)
            .transpose()?
            .map(Box::new),
    })
}

fn normalize_sublink(link: &protobuf::SubLink) -> Result<Expr> {
    match protobuf::SubLinkType::try_from(link.sub_link_type)
        .unwrap_or(protobuf::SubLinkType::ExistsSublink)
    {
        protobuf::SubLinkType::AnySublink if link.oper_name.is_empty() => {
            let testexpr = link
                .testexpr
                .as_deref()
                .ok_or_else(|| unsupported("IN subquery is missing its test expression"))?;
            Ok(Expr::InSubquery {
                expr: Box::new(normalize_expr_node(testexpr)?),
                query: Box::new(normalize_query(as_select_stmt(
                    link.subselect
                        .as_deref()
                        .ok_or_else(|| unsupported("IN is missing its subquery"))?,
                )?)?),
                negated: false,
            })
        }
        protobuf::SubLinkType::ExistsSublink => {
            Ok(Expr::Exists(Box::new(normalize_query(as_select_stmt(
                link.subselect
                    .as_deref()
                    .ok_or_else(|| unsupported("EXISTS is missing its subquery"))?,
            )?)?)))
        }
        protobuf::SubLinkType::ExprSublink => Ok(Expr::ScalarSubquery(Box::new(normalize_query(
            as_select_stmt(
                link.subselect
                    .as_deref()
                    .ok_or_else(|| unsupported("scalar subquery is missing its SELECT"))?,
            )?,
        )?))),
        other => Err(unsupported(format!(
            "subquery type {} is not supported",
            other.as_str_name()
        ))),
    }
}

fn normalize_target_column(node: &protobuf::Node) -> Result<String> {
    match node_kind(node)? {
        NodeEnum::ResTarget(target) if !target.name.is_empty() => Ok(target.name.clone()),
        _ => Err(unsupported("unsupported INSERT target column")),
    }
}

fn normalize_range_var(range: &protobuf::RangeVar) -> Result<String> {
    if !range.catalogname.is_empty() || !range.schemaname.is_empty() {
        return Err(unsupported(
            "catalog-qualified and schema-qualified names are not supported",
        ));
    }
    if range.relname.is_empty() {
        return Err(unsupported("relation name must not be empty"));
    }
    Ok(range.relname.clone())
}

fn normalize_type_name(type_name: &protobuf::TypeName) -> Result<ColumnType> {
    let raw = normalize_qualified_name(&type_name.names)?.to_ascii_lowercase();
    match raw.as_str() {
        "int" | "int8" | "integer" | "bigint" | "int64" | "smallint" | "pg_catalog.int2"
        | "pg_catalog.int4" | "pg_catalog.int8" => Ok(ColumnType::Int64),
        "real" | "double precision" | "float4" | "float8" | "float64" | "pg_catalog.float4"
        | "pg_catalog.float8" => Ok(ColumnType::Float64),
        "text" | "varchar" | "character varying" | "char" | "character" | "pg_catalog.text"
        | "pg_catalog.varchar" | "pg_catalog.bpchar" => Ok(ColumnType::Text),
        "bool" | "boolean" | "pg_catalog.bool" => Ok(ColumnType::Bool),
        "bytea" | "blob" | "pg_catalog.bytea" => Ok(ColumnType::Blob),
        "decimal" | "numeric" | "pg_catalog.numeric" => Ok(ColumnType::Decimal),
        "uuid" | "pg_catalog.uuid" => Ok(ColumnType::Uuid),
        "timestamp"
        | "timestamp without time zone"
        | "timestamp with time zone"
        | "pg_catalog.timestamp"
        | "pg_catalog.timestamptz"
        | "datetime"
        | "date" => Ok(ColumnType::Timestamp),
        _ => Err(unsupported(format!("type {raw} is not supported"))),
    }
}

fn normalize_string_node(node: &protobuf::Node) -> Result<String> {
    match node_kind(node)? {
        NodeEnum::String(value) => Ok(value.sval.clone()),
        other => Err(unsupported(format!(
            "expected a string node, got {}",
            describe_node(other)
        ))),
    }
}

fn normalize_qualified_name(nodes: &[protobuf::Node]) -> Result<String> {
    let parts = nodes
        .iter()
        .map(normalize_string_node)
        .collect::<Result<Vec<_>>>()?;
    Ok(join_name_parts(&parts))
}

fn normalize_object_name_list(node: &protobuf::Node) -> Result<Vec<String>> {
    match node_kind(node)? {
        NodeEnum::List(list) => list.items.iter().map(normalize_string_node).collect(),
        other => Err(unsupported(format!(
            "object name node {} is not supported",
            describe_node(other)
        ))),
    }
}

fn normalize_operator_name(nodes: &[protobuf::Node]) -> Result<String> {
    normalize_qualified_name(nodes)
}

fn normalize_with_clause(clause: Option<&protobuf::WithClause>) -> Result<Vec<CommonTableExpr>> {
    let Some(clause) = clause else {
        return Ok(Vec::new());
    };
    clause
        .ctes
        .iter()
        .map(|cte| match node_kind(cte)? {
            NodeEnum::CommonTableExpr(cte) => Ok(CommonTableExpr {
                name: cte.ctename.clone(),
                column_names: cte
                    .aliascolnames
                    .iter()
                    .map(normalize_string_node)
                    .collect::<Result<Vec<_>>>()?,
                query: normalize_query(as_select_stmt(
                    cte.ctequery
                        .as_deref()
                        .ok_or_else(|| unsupported("CTE is missing its SELECT"))?,
                )?)?,
            }),
            _ => Err(unsupported("WITH contains an unsupported CTE entry")),
        })
        .collect()
}

fn as_select_stmt(node: &protobuf::Node) -> Result<&protobuf::SelectStmt> {
    match node_kind(node)? {
        NodeEnum::SelectStmt(select) => Ok(select),
        other => Err(unsupported(format!(
            "expected SELECT AST, got {}",
            describe_node(other)
        ))),
    }
}

fn node_kind(node: &protobuf::Node) -> Result<&NodeEnum> {
    node.node
        .as_ref()
        .ok_or_else(|| unsupported("parser emitted an empty AST node"))
}

fn join_name_parts(parts: &[String]) -> String {
    parts.join(".")
}

fn fold_binary(op: BinaryOp, mut args: Vec<Expr>) -> Result<Expr> {
    let first = args
        .drain(..1)
        .next()
        .ok_or_else(|| unsupported("boolean expression is missing operands"))?;
    Ok(args.into_iter().fold(first, |left, right| Expr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}

fn describe_node(node: &NodeEnum) -> &'static str {
    match node {
        NodeEnum::SelectStmt(_) => "SelectStmt",
        NodeEnum::InsertStmt(_) => "InsertStmt",
        NodeEnum::UpdateStmt(_) => "UpdateStmt",
        NodeEnum::DeleteStmt(_) => "DeleteStmt",
        NodeEnum::CreateStmt(_) => "CreateStmt",
        NodeEnum::IndexStmt(_) => "IndexStmt",
        NodeEnum::ViewStmt(_) => "ViewStmt",
        NodeEnum::DropStmt(_) => "DropStmt",
        NodeEnum::RenameStmt(_) => "RenameStmt",
        NodeEnum::AlterTableStmt(_) => "AlterTableStmt",
        NodeEnum::CreateTrigStmt(_) => "CreateTrigStmt",
        NodeEnum::ExplainStmt(_) => "ExplainStmt",
        NodeEnum::AExpr(_) => "AExpr",
        NodeEnum::BoolExpr(_) => "BoolExpr",
        NodeEnum::FuncCall(_) => "FuncCall",
        NodeEnum::ColumnRef(_) => "ColumnRef",
        NodeEnum::AConst(_) => "AConst",
        NodeEnum::RangeVar(_) => "RangeVar",
        NodeEnum::JoinExpr(_) => "JoinExpr",
        NodeEnum::SortBy(_) => "SortBy",
        NodeEnum::ResTarget(_) => "ResTarget",
        NodeEnum::Constraint(_) => "Constraint",
        NodeEnum::ColumnDef(_) => "ColumnDef",
        NodeEnum::IndexElem(_) => "IndexElem",
        NodeEnum::CommonTableExpr(_) => "CommonTableExpr",
        NodeEnum::RangeSubselect(_) => "RangeSubselect",
        NodeEnum::TypeName(_) => "TypeName",
        NodeEnum::TypeCast(_) => "TypeCast",
        NodeEnum::CaseExpr(_) => "CaseExpr",
        NodeEnum::NullTest(_) => "NullTest",
        NodeEnum::SubLink(_) => "SubLink",
        NodeEnum::String(_) => "String",
        NodeEnum::List(_) => "List",
        _ => "Node",
    }
}

fn unsupported(message: impl Into<String>) -> DbError {
    DbError::sql(message.into())
}


#[cfg(test)]
mod tests {
    use super::*;

    fn norm(sql: &str) -> Statement {
        normalize_statement_text(sql).unwrap()
    }

    fn norm_err(sql: &str) -> String {
        normalize_statement_text(sql).unwrap_err().to_string()
    }

    // ── normalize_type_name paths ──────────────────────────────────

    #[test]
    fn type_smallint() {
        let stmt = norm("CREATE TABLE t (a SMALLINT PRIMARY KEY)");
        if let Statement::CreateTable(ct) = stmt {
            assert_eq!(ct.columns[0].column_type, ColumnType::Int64);
        } else { panic!("expected CreateTable"); }
    }

    #[test]
    fn type_real() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a REAL)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Float64);
        }
    }

    #[test]
    fn type_double_precision() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a DOUBLE PRECISION)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Float64);
        }
    }

    #[test]
    fn type_character_varying() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a CHARACTER VARYING)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Text);
        }
    }

    #[test]
    fn type_char() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a CHAR)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Text);
        }
    }

    #[test]
    fn type_bytea() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a BYTEA)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Blob);
        }
    }

    #[test]
    fn type_blob() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a BLOB)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Blob);
        }
    }

    #[test]
    fn type_numeric() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a NUMERIC)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Decimal);
        }
    }

    #[test]
    fn type_decimal() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a DECIMAL)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Decimal);
        }
    }

    #[test]
    fn type_uuid() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a UUID)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Uuid);
        }
    }

    #[test]
    fn type_timestamp_with_tz() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a TIMESTAMP WITH TIME ZONE)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Timestamp);
        }
    }

    #[test]
    fn type_date() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a DATE)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Timestamp);
        }
    }

    #[test]
    fn type_datetime() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a DATETIME)") {
            assert_eq!(ct.columns[1].column_type, ColumnType::Timestamp);
        }
    }

    #[test]
    fn type_unknown_errors() {
        let err = norm_err("CREATE TABLE t (id INT PRIMARY KEY, a JSONB)");
        assert!(err.contains("not supported"), "got: {err}");
    }

    // ── normalize_const paths ──────────────────────────────────────

    #[test]
    fn const_boolean_true() {
        if let Statement::Query(q) = norm("SELECT true") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::Literal(Value::Bool(true)), .. }));
            }
        }
    }

    #[test]
    fn const_boolean_false() {
        if let Statement::Query(q) = norm("SELECT false") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::Literal(Value::Bool(false)), .. }));
            }
        }
    }

    // ── normalize_aexpr paths ──────────────────────────────────────

    #[test]
    fn aexpr_ilike() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE name ILIKE 'alice'") {
            if let QueryBody::Select(s) = q.body {
                if let Some(Expr::Like { case_insensitive, .. }) = &s.filter {
                    assert!(case_insensitive);
                } else { panic!("expected LIKE expr"); }
            }
        }
    }

    #[test]
    fn aexpr_not_between() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10") {
            if let QueryBody::Select(s) = q.body {
                // NOT BETWEEN normalizes to Unary Not wrapping Between, or Between{negated:true}
                assert!(s.filter.is_some());
            }
        }
    }

    #[test]
    fn aexpr_nullif() {
        if let Statement::Query(q) = norm("SELECT NULLIF(1, 2)") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::Case { .. }, .. }));
            }
        }
    }

    #[test]
    fn aexpr_concat_operator() {
        if let Statement::Query(q) = norm("SELECT 'a' || 'b'") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::Binary { op: BinaryOp::Concat, .. }, .. }));
            }
        }
    }

    #[test]
    fn aexpr_modulo_operator() {
        if let Statement::Query(q) = norm("SELECT 10 % 3") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::Binary { op: BinaryOp::Mod, .. }, .. }));
            }
        }
    }

    #[test]
    fn aexpr_not_equal() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE a != b") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.filter, Some(Expr::Binary { op: BinaryOp::NotEq, .. })));
            }
        }
    }

    #[test]
    fn aexpr_unary_negate() {
        if let Statement::Query(q) = norm("SELECT -x FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::Unary { op: UnaryOp::Negate, .. }, .. }));
            }
        }
    }

    // ── normalize_bool_expr paths ──────────────────────────────────

    #[test]
    fn bool_not_expr() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE NOT (a = 1)") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.filter, Some(Expr::Unary { op: UnaryOp::Not, .. })));
            }
        }
    }

    #[test]
    fn bool_or_expr() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE a = 1 OR b = 2") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.filter, Some(Expr::Binary { op: BinaryOp::Or, .. })));
            }
        }
    }

    #[test]
    fn bool_multi_or() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.filter, Some(Expr::Binary { op: BinaryOp::Or, .. })));
            }
        }
    }

    // ── normalize_function_call paths ──────────────────────────────

    #[test]
    fn func_count_distinct() {
        if let Statement::Query(q) = norm("SELECT COUNT(DISTINCT a) FROM t") {
            if let QueryBody::Select(s) = q.body {
                if let SelectItem::Expr { expr: Expr::Aggregate { distinct, .. }, .. } = &s.projection[0] {
                    assert!(distinct);
                } else { panic!("expected aggregate"); }
            }
        }
    }

    #[test]
    fn func_count_star() {
        if let Statement::Query(q) = norm("SELECT COUNT(*) FROM t") {
            if let QueryBody::Select(s) = q.body {
                if let SelectItem::Expr { expr: Expr::Aggregate { star, .. }, .. } = &s.projection[0] {
                    assert!(star);
                }
            }
        }
    }

    #[test]
    fn func_window_row_number() {
        if let Statement::Query(q) = norm("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::RowNumber { .. }, .. }));
            }
        }
    }

    #[test]
    fn func_window_rank() {
        if let Statement::Query(q) = norm("SELECT RANK() OVER (ORDER BY id) FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::WindowFunction { .. }, .. }));
            }
        }
    }

    #[test]
    fn func_window_lag() {
        if let Statement::Query(q) = norm("SELECT LAG(val, 1) OVER (ORDER BY id) FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::WindowFunction { .. }, .. }));
            }
        }
    }

    #[test]
    fn func_window_lead() {
        if let Statement::Query(q) = norm("SELECT LEAD(val, 1) OVER (ORDER BY id) FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::WindowFunction { .. }, .. }));
            }
        }
    }

    #[test]
    fn func_window_first_value() {
        if let Statement::Query(q) = norm("SELECT FIRST_VALUE(val) OVER (ORDER BY id) FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::WindowFunction { .. }, .. }));
            }
        }
    }

    #[test]
    fn func_window_last_value() {
        if let Statement::Query(q) = norm("SELECT LAST_VALUE(val) OVER (ORDER BY id) FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::WindowFunction { .. }, .. }));
            }
        }
    }

    #[test]
    fn func_window_nth_value() {
        if let Statement::Query(q) = norm("SELECT NTH_VALUE(val, 2) OVER (ORDER BY id) FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::WindowFunction { .. }, .. }));
            }
        }
    }

    #[test]
    fn func_window_dense_rank() {
        if let Statement::Query(q) = norm("SELECT DENSE_RANK() OVER (ORDER BY id) FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::WindowFunction { .. }, .. }));
            }
        }
    }

    #[test]
    fn func_window_with_partition() {
        if let Statement::Query(q) = norm("SELECT ROW_NUMBER() OVER (PARTITION BY cat ORDER BY id) FROM t") {
            if let QueryBody::Select(s) = q.body {
                if let SelectItem::Expr { expr: Expr::RowNumber { partition_by, .. }, .. } = &s.projection[0] {
                    assert!(!partition_by.is_empty());
                }
            }
        }
    }

    // ── normalize_sublink paths ────────────────────────────────────

    #[test]
    fn sublink_exists() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u)") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.filter, Some(Expr::Exists(_))));
            }
        }
    }

    #[test]
    fn sublink_in_subquery() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE id IN (SELECT id FROM u)") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.filter, Some(Expr::InSubquery { .. })));
            }
        }
    }

    #[test]
    fn sublink_scalar() {
        if let Statement::Query(q) = norm("SELECT (SELECT MAX(val) FROM u) FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::ScalarSubquery(_), .. }));
            }
        }
    }

    // ── normalize_case_expr paths ──────────────────────────────────

    #[test]
    fn case_with_operand() {
        if let Statement::Query(q) = norm("SELECT CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END FROM t") {
            if let QueryBody::Select(s) = q.body {
                if let SelectItem::Expr { expr: Expr::Case { operand, else_expr, .. }, .. } = &s.projection[0] {
                    assert!(operand.is_some());
                    assert!(else_expr.is_some());
                }
            }
        }
    }

    #[test]
    fn case_without_operand() {
        if let Statement::Query(q) = norm("SELECT CASE WHEN x > 1 THEN 'hi' END FROM t") {
            if let QueryBody::Select(s) = q.body {
                if let SelectItem::Expr { expr: Expr::Case { operand, else_expr, .. }, .. } = &s.projection[0] {
                    assert!(operand.is_none());
                    assert!(else_expr.is_none());
                }
            }
        }
    }

    // ── normalize_expr_node paths ──────────────────────────────────

    #[test]
    fn expr_type_cast() {
        if let Statement::Query(q) = norm("SELECT CAST(1 AS TEXT)") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::Cast { .. }, .. }));
            }
        }
    }

    #[test]
    fn expr_is_null() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE x IS NULL") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.filter, Some(Expr::IsNull { .. })));
            }
        }
    }

    #[test]
    fn expr_is_not_null() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE x IS NOT NULL") {
            if let QueryBody::Select(s) = q.body {
                // IS NOT NULL normalizes to IsNull{negated:true} or Unary Not
                assert!(s.filter.is_some());
            }
        }
    }

    // ── normalize_sql_value_function paths ─────────────────────────

    #[test]
    fn current_timestamp() {
        if let Statement::Query(q) = norm("SELECT CURRENT_TIMESTAMP") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::Function { .. }, .. }));
            }
        }
    }

    #[test]
    fn current_date_fn() {
        if let Statement::Query(q) = norm("SELECT CURRENT_DATE") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::Expr { expr: Expr::Function { .. }, .. }));
            }
        }
    }

    // ── normalize_from_item paths ──────────────────────────────────

    #[test]
    fn from_join_left() {
        if let Statement::Query(q) = norm("SELECT * FROM t LEFT JOIN u ON t.id = u.id") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.from[0], FromItem::Join { kind: JoinKind::Left, .. }));
            }
        }
    }

    #[test]
    fn from_join_right() {
        if let Statement::Query(q) = norm("SELECT * FROM t RIGHT JOIN u ON t.id = u.id") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.from[0], FromItem::Join { kind: JoinKind::Right, .. }));
            }
        }
    }

    #[test]
    fn from_join_full() {
        if let Statement::Query(q) = norm("SELECT * FROM t FULL OUTER JOIN u ON t.id = u.id") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.from[0], FromItem::Join { kind: JoinKind::Full, .. }));
            }
        }
    }

    #[test]
    fn from_join_cross() {
        if let Statement::Query(q) = norm("SELECT * FROM t CROSS JOIN u") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.from[0], FromItem::Join { kind: JoinKind::Cross, .. }));
            }
        }
    }

    #[test]
    fn from_subquery() {
        if let Statement::Query(q) = norm("SELECT * FROM (SELECT 1 AS x) AS sub") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.from[0], FromItem::Subquery { .. }));
            }
        }
    }

    #[test]
    fn from_join_using() {
        if let Statement::Query(q) = norm("SELECT * FROM t JOIN u USING (id)") {
            if let QueryBody::Select(s) = q.body {
                if let FromItem::Join { constraint: JoinConstraint::Using(cols), .. } = &s.from[0] {
                    assert_eq!(cols, &["id"]);
                }
            }
        }
    }

    #[test]
    fn from_function_call() {
        // generate_series is rejected at normalization; test that it returns an error
        let err = norm_err("SELECT * FROM generate_series(1, 5)");
        assert!(err.contains("not supported"), "got: {err}");
    }

    // ── normalize_select_item paths ────────────────────────────────

    #[test]
    fn select_qualified_wildcard() {
        if let Statement::Query(q) = norm("SELECT t.* FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(matches!(&s.projection[0], SelectItem::QualifiedWildcard(_)));
            }
        }
    }

    #[test]
    fn select_alias() {
        if let Statement::Query(q) = norm("SELECT 1 AS my_alias") {
            if let QueryBody::Select(s) = q.body {
                if let SelectItem::Expr { alias, .. } = &s.projection[0] {
                    assert_eq!(alias.as_deref(), Some("my_alias"));
                }
            }
        }
    }

    // ── set operations ─────────────────────────────────────────────

    #[test]
    fn set_union_all() {
        if let Statement::Query(q) = norm("SELECT 1 UNION ALL SELECT 2") {
            assert!(matches!(q.body, QueryBody::SetOperation { op: SetOperation::Union, all: true, .. }));
        }
    }

    #[test]
    fn set_union() {
        if let Statement::Query(q) = norm("SELECT 1 UNION SELECT 2") {
            assert!(matches!(q.body, QueryBody::SetOperation { op: SetOperation::Union, all: false, .. }));
        }
    }

    #[test]
    fn set_intersect() {
        if let Statement::Query(q) = norm("SELECT 1 INTERSECT SELECT 2") {
            assert!(matches!(q.body, QueryBody::SetOperation { op: SetOperation::Intersect, all: false, .. }));
        }
    }

    #[test]
    fn set_intersect_all() {
        if let Statement::Query(q) = norm("SELECT 1 INTERSECT ALL SELECT 2") {
            assert!(matches!(q.body, QueryBody::SetOperation { op: SetOperation::Intersect, all: true, .. }));
        }
    }

    #[test]
    fn set_except() {
        if let Statement::Query(q) = norm("SELECT 1 EXCEPT SELECT 2") {
            assert!(matches!(q.body, QueryBody::SetOperation { op: SetOperation::Except, all: false, .. }));
        }
    }

    #[test]
    fn set_except_all() {
        if let Statement::Query(q) = norm("SELECT 1 EXCEPT ALL SELECT 2") {
            assert!(matches!(q.body, QueryBody::SetOperation { op: SetOperation::Except, all: true, .. }));
        }
    }

    // ── normalize_order_by paths ───────────────────────────────────

    #[test]
    fn order_by_desc() {
        if let Statement::Query(q) = norm("SELECT * FROM t ORDER BY x DESC") {
            assert!(q.order_by[0].descending);
        }
    }

    #[test]
    fn order_by_asc() {
        if let Statement::Query(q) = norm("SELECT * FROM t ORDER BY x ASC") {
            assert!(!q.order_by[0].descending);
        }
    }

    // ── normalize_query paths ──────────────────────────────────────

    #[test]
    fn query_limit_offset() {
        if let Statement::Query(q) = norm("SELECT * FROM t LIMIT 10 OFFSET 5") {
            assert!(q.limit.is_some());
            assert!(q.offset.is_some());
        }
    }

    // ── normalize_insert paths ─────────────────────────────────────

    #[test]
    fn insert_on_conflict_nothing() {
        if let Statement::Insert(ins) = norm("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING") {
            assert!(matches!(ins.on_conflict, Some(ConflictAction::DoNothing { .. })));
        }
    }

    #[test]
    fn insert_on_conflict_update() {
        if let Statement::Insert(ins) = norm("INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val") {
            assert!(matches!(ins.on_conflict, Some(ConflictAction::DoUpdate { .. })));
        }
    }

    #[test]
    fn insert_on_conflict_constraint() {
        if let Statement::Insert(ins) = norm("INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT pk_t DO NOTHING") {
            if let Some(ConflictAction::DoNothing { target }) = ins.on_conflict {
                assert!(matches!(target, ConflictTarget::Constraint(_)));
            }
        }
    }

    #[test]
    fn insert_returning() {
        if let Statement::Insert(ins) = norm("INSERT INTO t VALUES (1) RETURNING id") {
            assert!(!ins.returning.is_empty());
        }
    }

    #[test]
    fn insert_from_query() {
        if let Statement::Insert(ins) = norm("INSERT INTO t SELECT * FROM u") {
            assert!(matches!(ins.source, InsertSource::Query(_)));
        }
    }

    // ── normalize_create_table paths ───────────────────────────────

    #[test]
    fn create_table_temp() {
        if let Statement::CreateTable(ct) = norm("CREATE TEMP TABLE t (id INT PRIMARY KEY)") {
            assert!(ct.temporary);
        }
    }

    #[test]
    fn create_table_if_not_exists() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)") {
            assert!(ct.if_not_exists);
        }
    }

    // ── normalize_column_definition paths ──────────────────────────

    #[test]
    fn column_not_null() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)") {
            assert!(!ct.columns[1].nullable);
        }
    }

    #[test]
    fn column_default_value() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, val INT DEFAULT 42)") {
            assert!(ct.columns[1].default.is_some());
        }
    }

    #[test]
    fn column_unique() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)") {
            assert!(ct.columns[1].unique);
        }
    }

    #[test]
    fn column_generated() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x * 2) STORED)") {
            assert!(ct.columns[2].generated.is_some());
        }
    }

    #[test]
    fn column_check() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, val INT CHECK (val > 0))") {
            assert!(!ct.columns[1].checks.is_empty());
        }
    }

    // ── normalize_table_constraint paths ───────────────────────────

    #[test]
    fn table_constraint_unique() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, UNIQUE (a, b))") {
            assert!(ct.constraints.iter().any(|c| matches!(c, TableConstraint::Unique { .. })));
        }
    }

    #[test]
    fn table_constraint_check() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, a INT, CHECK (a > 0))") {
            assert!(ct.constraints.iter().any(|c| matches!(c, TableConstraint::Check { .. })));
        }
    }

    #[test]
    fn table_constraint_fk() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id))") {
            assert!(ct.constraints.iter().any(|c| matches!(c, TableConstraint::ForeignKey { .. })));
        }
    }

    // ── normalize_fk_action paths ──────────────────────────────────

    #[test]
    fn fk_cascade_actions() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)") {
            if let Some(fk) = &ct.columns[1].references {
                assert_eq!(fk.on_delete, ForeignKeyActionSpec::Cascade);
                assert_eq!(fk.on_update, ForeignKeyActionSpec::Cascade);
            }
        }
    }

    #[test]
    fn fk_set_null_actions() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE SET NULL ON UPDATE SET NULL)") {
            if let Some(fk) = &ct.columns[1].references {
                assert_eq!(fk.on_delete, ForeignKeyActionSpec::SetNull);
                assert_eq!(fk.on_update, ForeignKeyActionSpec::SetNull);
            }
        }
    }

    #[test]
    fn fk_restrict_action() {
        if let Statement::CreateTable(ct) = norm("CREATE TABLE t (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE RESTRICT)") {
            if let Some(fk) = &ct.columns[1].references {
                assert_eq!(fk.on_delete, ForeignKeyActionSpec::Restrict);
            }
        }
    }

    // ── normalize_create_index paths ───────────────────────────────

    #[test]
    fn create_index_unique() {
        if let Statement::CreateIndex(ci) = norm("CREATE UNIQUE INDEX idx ON t (col)") {
            assert!(ci.unique);
        }
    }

    #[test]
    fn create_index_if_not_exists() {
        if let Statement::CreateIndex(ci) = norm("CREATE INDEX IF NOT EXISTS idx ON t (col)") {
            assert!(ci.if_not_exists);
        }
    }

    #[test]
    fn create_index_gin() {
        if let Statement::CreateIndex(ci) = norm("CREATE INDEX idx ON t USING gin (col)") {
            assert_eq!(ci.access_method, "gin");
        }
    }

    #[test]
    fn create_index_partial() {
        if let Statement::CreateIndex(ci) = norm("CREATE INDEX idx ON t (col) WHERE active = true") {
            assert!(ci.predicate.is_some());
        }
    }

    #[test]
    fn create_index_expression() {
        if let Statement::CreateIndex(ci) = norm("CREATE INDEX idx ON t ((col + 1))") {
            assert!(ci.columns.iter().any(|c| matches!(c, IndexExpression::Expr(_))));
        }
    }

    // ── normalize_create_view ──────────────────────────────────────

    #[test]
    fn create_view_normal() {
        if let Statement::CreateView(cv) = norm("CREATE VIEW v AS SELECT * FROM t") {
            assert_eq!(cv.view_name, "v");
            assert!(!cv.temporary);
        }
    }

    #[test]
    fn create_view_temp() {
        if let Statement::CreateView(cv) = norm("CREATE TEMP VIEW v AS SELECT 1") {
            assert!(cv.temporary);
        }
    }

    // ── normalize_explain ──────────────────────────────────────────

    #[test]
    fn explain_simple() {
        assert!(matches!(norm("EXPLAIN SELECT 1"), Statement::Explain(_)));
    }

    #[test]
    fn explain_analyze() {
        if let Statement::Explain(ex) = norm("EXPLAIN ANALYZE SELECT 1") {
            assert!(ex.analyze);
        }
    }

    // ── normalize_drop paths ───────────────────────────────────────

    #[test]
    fn drop_table_if_exists() {
        if let Statement::DropTable { if_exists, .. } = norm("DROP TABLE IF EXISTS t") {
            assert!(if_exists);
        }
    }

    #[test]
    fn drop_index() {
        assert!(matches!(norm("DROP INDEX idx"), Statement::DropIndex { .. }));
    }

    #[test]
    fn drop_view() {
        assert!(matches!(norm("DROP VIEW v"), Statement::DropView { .. }));
    }

    #[test]
    fn drop_trigger() {
        assert!(matches!(norm("DROP TRIGGER trig ON t"), Statement::DropTrigger { .. }));
    }

    // ── normalize_rename paths ─────────────────────────────────────

    #[test]
    fn rename_column() {
        if let Statement::AlterTable { actions, .. } = norm("ALTER TABLE t RENAME COLUMN x TO y") {
            assert!(matches!(&actions[0], AlterTableAction::RenameColumn { old_name, new_name } if old_name == "x" && new_name == "y"));
        } else { panic!("expected AlterTable"); }
    }

    #[test]
    fn rename_view() {
        if let Statement::AlterViewRename { view_name, new_name } = norm("ALTER VIEW v RENAME TO w") {
            assert_eq!(view_name, "v");
            assert_eq!(new_name, "w");
        } else { panic!("expected AlterViewRename"); }
    }

    // ── normalize_alter_table paths ────────────────────────────────

    #[test]
    fn alter_table_add_column() {
        if let Statement::AlterTable { actions, .. } = norm("ALTER TABLE t ADD COLUMN x INT") {
            assert!(matches!(&actions[0], AlterTableAction::AddColumn(_)));
        }
    }

    #[test]
    fn alter_table_drop_column() {
        if let Statement::AlterTable { actions, .. } = norm("ALTER TABLE t DROP COLUMN x") {
            assert!(matches!(&actions[0], AlterTableAction::DropColumn { .. }));
        }
    }

    #[test]
    fn alter_table_alter_column_type() {
        if let Statement::AlterTable { actions, .. } = norm("ALTER TABLE t ALTER COLUMN x TYPE TEXT") {
            assert!(matches!(&actions[0], AlterTableAction::AlterColumnType { .. }));
        }
    }

    // ── normalize_create_trigger paths ─────────────────────────────

    #[test]
    fn create_trigger_after_insert() {
        if let Statement::CreateTrigger(ct) = norm("CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('SELECT 1')") {
            assert_eq!(ct.kind, TriggerKindSpec::After);
            assert_eq!(ct.event, TriggerEventSpec::Insert);
        }
    }

    #[test]
    fn create_trigger_instead_of() {
        if let Statement::CreateTrigger(ct) = norm("CREATE TRIGGER trg INSTEAD OF INSERT ON v FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('SELECT 1')") {
            assert_eq!(ct.kind, TriggerKindSpec::InsteadOf);
        }
    }

    // ── normalize_with_clause paths ────────────────────────────────

    #[test]
    fn with_recursive() {
        if let Statement::Query(q) = norm("WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 5) SELECT * FROM cte") {
            assert!(q.recursive);
        }
    }

    #[test]
    fn with_non_recursive() {
        if let Statement::Query(q) = norm("WITH cte AS (SELECT 1 AS n) SELECT * FROM cte") {
            assert!(!q.recursive);
        }
    }

    // ── normalize_distinct_clause ──────────────────────────────────

    #[test]
    fn select_distinct() {
        if let Statement::Query(q) = norm("SELECT DISTINCT a FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(s.distinct);
            }
        }
    }

    #[test]
    fn select_distinct_on() {
        if let Statement::Query(q) = norm("SELECT DISTINCT ON (a) a, b FROM t") {
            if let QueryBody::Select(s) = q.body {
                assert!(s.distinct);
                assert!(!s.distinct_on.is_empty());
            }
        }
    }

    // ── normalize_like_pattern paths ───────────────────────────────

    #[test]
    fn like_with_escape() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE name LIKE 'a%' ESCAPE '\\'") {
            if let QueryBody::Select(s) = q.body {
                if let Some(Expr::Like { escape, .. }) = &s.filter {
                    assert!(escape.is_some());
                }
            }
        }
    }

    // ── normalize_vacuum / analyze paths ───────────────────────────

    #[test]
    fn analyze_statement() {
        assert!(matches!(norm("ANALYZE t"), Statement::Analyze { .. }));
    }

    // ── parameter expressions ──────────────────────────────────────

    #[test]
    fn parameter_reference() {
        if let Statement::Query(q) = norm("SELECT * FROM t WHERE id = $1") {
            if let QueryBody::Select(s) = q.body {
                if let Some(Expr::Binary { right, .. }) = &s.filter {
                    assert!(matches!(right.as_ref(), Expr::Parameter(1)));
                }
            }
        }
    }

    // ── error paths ────────────────────────────────────────────────

    #[test]
    fn unsupported_statement_produces_error() {
        let err = norm_err("DO $$ BEGIN END $$");
        assert!(!err.is_empty());
    }

    #[test]
    fn empty_sql_errors() {
        let err = norm_err("");
        assert!(!err.is_empty());
    }
}
