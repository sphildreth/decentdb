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
    ForeignKeyDefinition, FromItem, IndexExpression, InsertSource, InsertStatement, JoinKind,
    OrderBy, Query, QueryBody, Select, SelectItem, SetOperation, Statement, TableConstraint,
    TriggerEventSpec, TriggerKindSpec, UnaryOp, UpdateStatement,
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
    let ctes = normalize_with_clause(statement.with_clause.as_ref())?;
    let body = normalize_query_body(statement)?;
    let order_by = statement
        .sort_clause
        .iter()
        .map(normalize_order_by_node)
        .collect::<Result<Vec<_>>>()?;
    let limit = statement
        .limit_count
        .as_deref()
        .map(normalize_expr_node)
        .transpose()?;
    let offset = statement
        .limit_offset
        .as_deref()
        .map(normalize_expr_node)
        .transpose()?;
    Ok(Query {
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
    Ok(QueryBody::Select(Select {
        projection,
        from,
        filter,
        group_by,
        having,
        distinct: !statement.distinct_clause.is_empty(),
    }))
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
    let table_name = normalize_range_var(
        statement
            .relation
            .as_ref()
            .ok_or_else(|| unsupported("CREATE TABLE is missing a relation name"))?,
    )?;
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
        if_not_exists: statement.if_not_exists,
        columns,
        constraints,
    })
}

fn normalize_column_definition(column: &protobuf::ColumnDef) -> Result<ColumnDefinition> {
    let mut primary_key = false;
    let mut unique = false;
    let mut default = column
        .raw_default
        .as_deref()
        .map(normalize_expr_node)
        .transpose()?;
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
                    protobuf::ConstrType::ConstrNotnull => {}
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
        nullable: !column.is_not_null && !primary_key,
        default,
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
                normalize_constraint_keys(&constraint.keys)?,
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
    Ok(CreateViewStatement {
        view_name: normalize_range_var(
            statement
                .view
                .as_ref()
                .ok_or_else(|| unsupported("CREATE VIEW is missing a view name"))?,
        )?,
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
                protobuf::JoinType::JoinInner => JoinKind::Inner,
                protobuf::JoinType::JoinLeft => JoinKind::Left,
                other => {
                    return Err(unsupported(format!(
                        "join type {} is not supported",
                        other.as_str_name()
                    )))
                }
            },
            on: normalize_expr_node(
                join.quals
                    .as_deref()
                    .ok_or_else(|| unsupported("JOIN is missing its ON clause"))?,
            )?,
        }),
        other => Err(unsupported(format!(
            "FROM source {} is not supported",
            describe_node(other)
        ))),
    }
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
        other => Err(unsupported(format!(
            "expression node {} is not supported",
            describe_node(other)
        ))),
    }
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
                    (_, "||") => BinaryOp::Concat,
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
            Ok(Expr::InList {
                expr: Box::new(normalize_expr_node(
                    expr.lexpr
                        .as_deref()
                        .ok_or_else(|| unsupported("IN is missing its left operand"))?,
                )?),
                items,
                negated: false,
            })
        }
        protobuf::AExprKind::AexprLike | protobuf::AExprKind::AexprIlike => Ok(Expr::Like {
            expr: Box::new(normalize_expr_node(
                expr.lexpr
                    .as_deref()
                    .ok_or_else(|| unsupported("LIKE is missing its left operand"))?,
            )?),
            pattern: Box::new(normalize_expr_node(
                expr.rexpr
                    .as_deref()
                    .ok_or_else(|| unsupported("LIKE is missing its pattern"))?,
            )?),
            escape: None,
            case_insensitive: kind == protobuf::AExprKind::AexprIlike,
            negated: false,
        }),
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
    if call.over.is_some() {
        if name != "row_number" {
            return Err(unsupported(
                "only ROW_NUMBER() OVER (...) is supported as a window function",
            ));
        }
        let window = call
            .over
            .as_deref()
            .ok_or_else(|| unsupported("window function is missing its OVER clause"))?;
        if window.order_clause.is_empty() {
            return Err(unsupported("ROW_NUMBER() requires ORDER BY in OVER (...)"));
        }
        return Ok(Expr::RowNumber {
            partition_by: window
                .partition_clause
                .iter()
                .map(normalize_expr_container)
                .collect::<Result<Vec<_>>>()?,
            order_by: window
                .order_clause
                .iter()
                .map(normalize_order_by_node)
                .collect::<Result<Vec<_>>>()?,
        });
    }

    let args = call
        .args
        .iter()
        .map(normalize_expr_container)
        .collect::<Result<Vec<_>>>()?;
    if matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max") {
        return Ok(Expr::Aggregate {
            name,
            args,
            star: call.agg_star,
        });
    }

    Ok(Expr::Function { name, args })
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
        protobuf::SubLinkType::ExistsSublink => {
            Ok(Expr::Exists(Box::new(normalize_query(as_select_stmt(
                link.subselect
                    .as_deref()
                    .ok_or_else(|| unsupported("EXISTS is missing its subquery"))?,
            )?)?)))
        }
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
        "int" | "int8" | "integer" | "bigint" | "int64" => Ok(ColumnType::Int64),
        "real" | "double precision" | "float8" | "float64" => Ok(ColumnType::Float64),
        "text" | "varchar" | "character varying" | "char" | "character" => Ok(ColumnType::Text),
        "bool" | "boolean" => Ok(ColumnType::Bool),
        "bytea" | "blob" => Ok(ColumnType::Blob),
        "decimal" | "numeric" => Ok(ColumnType::Decimal),
        "uuid" => Ok(ColumnType::Uuid),
        "timestamp" | "timestamp without time zone" => Ok(ColumnType::Timestamp),
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
    if clause.recursive {
        return Err(unsupported("WITH RECURSIVE is not supported"));
    }
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
