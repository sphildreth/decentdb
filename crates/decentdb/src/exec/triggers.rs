//! Trigger execution helpers.

use crate::catalog::{TriggerEvent, TriggerKind, TriggerSchema};
use crate::error::{DbError, Result};
use crate::sql::ast::{CreateTriggerStatement, Statement, TriggerEventSpec, TriggerKindSpec};
use crate::sql::parser::parse_sql_statement;

use super::EngineRuntime;

impl EngineRuntime {
    pub(super) fn execute_create_trigger(
        &mut self,
        statement: &CreateTriggerStatement,
    ) -> Result<()> {
        if self.catalog.contains_object(&statement.trigger_name) {
            return Err(DbError::sql(format!(
                "object {} already exists",
                statement.trigger_name
            )));
        }
        let action = parse_sql_statement(&statement.action_sql)?;
        if !matches!(
            action,
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
        ) {
            return Err(DbError::sql(
                "trigger action must be a single INSERT, UPDATE, or DELETE statement",
            ));
        }

        let (kind, on_view) = match statement.kind {
            TriggerKindSpec::After => (TriggerKind::After, false),
            TriggerKindSpec::InsteadOf => (TriggerKind::InsteadOf, true),
        };
        if on_view {
            if !self.catalog.views.contains_key(&statement.target_name) {
                return Err(DbError::sql(format!(
                    "INSTEAD OF triggers require an existing target view {}",
                    statement.target_name
                )));
            }
        } else if !self.catalog.tables.contains_key(&statement.target_name) {
            return Err(DbError::sql(format!(
                "AFTER triggers require an existing target table {}",
                statement.target_name
            )));
        }

        self.catalog.triggers.insert(
            statement.trigger_name.clone(),
            TriggerSchema {
                name: statement.trigger_name.clone(),
                target_name: statement.target_name.clone(),
                kind,
                event: match statement.event {
                    TriggerEventSpec::Insert => TriggerEvent::Insert,
                    TriggerEventSpec::Update => TriggerEvent::Update,
                    TriggerEventSpec::Delete => TriggerEvent::Delete,
                },
                on_view,
                action_sql: statement.action_sql.clone(),
            },
        );
        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_drop_trigger(
        &mut self,
        name: &str,
        table_name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let Some(trigger) = self.catalog.triggers.get(name).cloned() else {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::sql(format!("unknown trigger {name}")));
        };
        if trigger.target_name != table_name {
            return Err(DbError::sql(format!(
                "trigger {} is defined on {}, not {}",
                name, trigger.target_name, table_name
            )));
        }
        self.catalog.triggers.remove(name);
        self.bump_schema_cookie();
        Ok(())
    }

    pub(super) fn execute_instead_of_triggers(
        &mut self,
        target_name: &str,
        event: TriggerEvent,
        invocations: usize,
        page_size: u32,
    ) -> Result<u64> {
        let triggers = matching_triggers(self, target_name, event, true);
        if triggers.is_empty() {
            return Err(DbError::sql(format!(
                "view {} does not define an INSTEAD OF {:?} trigger",
                target_name, event
            )));
        }
        let mut affected_rows = 0_u64;
        for _ in 0..invocations {
            for trigger in &triggers {
                let statement = parse_sql_statement(&trigger.action_sql)?;
                affected_rows += self
                    .execute_statement(&statement, &[], page_size)?
                    .affected_rows();
            }
        }
        Ok(affected_rows)
    }

    pub(super) fn execute_after_triggers(
        &mut self,
        target_name: &str,
        event: TriggerEvent,
        invocations: usize,
        page_size: u32,
    ) -> Result<()> {
        if invocations == 0 {
            return Ok(());
        }
        let triggers = matching_triggers(self, target_name, event, false);
        for _ in 0..invocations {
            for trigger in &triggers {
                let statement = parse_sql_statement(&trigger.action_sql)?;
                self.execute_statement(&statement, &[], page_size)?;
            }
        }
        Ok(())
    }
}

fn matching_triggers(
    runtime: &EngineRuntime,
    target_name: &str,
    event: TriggerEvent,
    on_view: bool,
) -> Vec<TriggerSchema> {
    runtime
        .catalog
        .triggers
        .values()
        .filter(|trigger| {
            trigger.target_name == target_name
                && trigger.event == event
                && trigger.on_view == on_view
        })
        .cloned()
        .collect()
}
