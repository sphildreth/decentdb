//! Local data-security helpers: security DDL parsing, audit context, and
//! hidden catalog table names.

use std::collections::BTreeMap;

use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::sql::parser::parse_expression_sql;

pub(crate) const POLICIES_TABLE: &str = "__decentdb_policies";
pub(crate) const MASKS_TABLE: &str = "__decentdb_masks";
pub(crate) const AUDIT_EVENTS_TABLE: &str = "__decentdb_audit_events";

pub(crate) const POLICIES_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_policies (policy_name TEXT PRIMARY KEY, table_name TEXT NOT NULL, using_sql TEXT NOT NULL, enabled BOOL NOT NULL, created_at_micros INT64 NOT NULL)";
pub(crate) const MASKS_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_masks (mask_name TEXT PRIMARY KEY, table_name TEXT NOT NULL, column_name TEXT NOT NULL, expression_sql TEXT NOT NULL, enabled BOOL NOT NULL, created_at_micros INT64 NOT NULL)";
pub(crate) const AUDIT_EVENTS_DDL: &str = "CREATE TABLE IF NOT EXISTS __decentdb_audit_events (event_id TEXT PRIMARY KEY, created_at_micros INT64 NOT NULL, actor TEXT, tenant TEXT, operation TEXT NOT NULL, target TEXT, statement TEXT, context_json TEXT NOT NULL)";

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct AuditContext {
    values: BTreeMap<String, Value>,
}

impl AuditContext {
    pub(crate) fn set(&mut self, key: impl Into<String>, value: Value) {
        self.values.insert(key.into(), value);
    }

    pub(crate) fn remove(&mut self, key: &str) {
        self.values.remove(key);
    }

    pub(crate) fn get(&self, key: &str) -> Option<Value> {
        self.values.get(key).cloned()
    }

    pub(crate) fn snapshot(&self) -> BTreeMap<String, Value> {
        self.values.clone()
    }

    pub(crate) fn tenant(&self) -> Option<Value> {
        self.get("tenant_id").or_else(|| self.get("tenant"))
    }

    pub(crate) fn actor(&self) -> Option<Value> {
        self.get("actor").or_else(|| self.get("user"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SecurityCommand {
    CreatePolicy {
        name: String,
        table_name: String,
        using_sql: String,
    },
    DropPolicy {
        name: String,
        if_exists: bool,
    },
    AlterPolicy {
        name: String,
        enabled: bool,
    },
    CreateMask {
        name: String,
        table_name: String,
        column_name: String,
        expression_sql: String,
    },
    DropMask {
        name: String,
        if_exists: bool,
    },
    AlterMask {
        name: String,
        enabled: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SetAuditContextCommand {
    pub(crate) key: String,
    pub(crate) value: Option<Value>,
}

pub(crate) fn is_security_internal_table(name: &str) -> bool {
    name.eq_ignore_ascii_case(POLICIES_TABLE)
        || name.eq_ignore_ascii_case(MASKS_TABLE)
        || name.eq_ignore_ascii_case(AUDIT_EVENTS_TABLE)
}

pub(crate) fn parse_security_command(sql: &str) -> Result<Option<SecurityCommand>> {
    let mut cursor = TokenCursor::new(sql);
    let Some(first) = cursor.next_token()? else {
        return Ok(None);
    };
    if first.eq_ignore_ascii_case("CREATE") {
        let Some(kind) = cursor.next_token()? else {
            return Ok(None);
        };
        if kind.eq_ignore_ascii_case("POLICY") {
            return Ok(Some(parse_create_policy(cursor)?));
        }
        if kind.eq_ignore_ascii_case("MASK") {
            return Ok(Some(parse_create_mask(cursor)?));
        }
        return Ok(None);
    }
    if first.eq_ignore_ascii_case("DROP") {
        let Some(kind) = cursor.next_token()? else {
            return Ok(None);
        };
        if kind.eq_ignore_ascii_case("POLICY") {
            return Ok(Some(parse_drop_policy(cursor)?));
        }
        if kind.eq_ignore_ascii_case("MASK") {
            return Ok(Some(parse_drop_mask(cursor)?));
        }
        return Ok(None);
    }
    if first.eq_ignore_ascii_case("ALTER") {
        let Some(kind) = cursor.next_token()? else {
            return Ok(None);
        };
        if kind.eq_ignore_ascii_case("POLICY") {
            return Ok(Some(parse_alter_policy(cursor)?));
        }
        if kind.eq_ignore_ascii_case("MASK") {
            return Ok(Some(parse_alter_mask(cursor)?));
        }
    }
    Ok(None)
}

pub(crate) fn parse_set_audit_context(sql: &str) -> Result<Option<SetAuditContextCommand>> {
    let mut cursor = TokenCursor::new(sql);
    let Some(first) = cursor.next_token()? else {
        return Ok(None);
    };
    if !first.eq_ignore_ascii_case("SET") {
        return Ok(None);
    }
    let Some(audit) = cursor.next_token()? else {
        return Ok(None);
    };
    let Some(context) = cursor.next_token()? else {
        return Ok(None);
    };
    if !audit.eq_ignore_ascii_case("AUDIT") || !context.eq_ignore_ascii_case("CONTEXT") {
        return Ok(None);
    }
    let key = cursor
        .next_token()?
        .ok_or_else(|| DbError::sql("SET AUDIT CONTEXT requires a key"))?;
    let Some(eq) = cursor.next_token()? else {
        return Err(DbError::sql("SET AUDIT CONTEXT requires '='"));
    };
    if eq != "=" {
        return Err(DbError::sql("SET AUDIT CONTEXT requires '='"));
    }
    let value_sql = cursor.remaining_trimmed_without_semicolon();
    if value_sql.eq_ignore_ascii_case("NULL") {
        return Ok(Some(SetAuditContextCommand { key, value: None }));
    }
    Ok(Some(SetAuditContextCommand {
        key,
        value: Some(parse_literal_value(value_sql)?),
    }))
}

fn parse_create_policy(mut cursor: TokenCursor<'_>) -> Result<SecurityCommand> {
    let name = required_token(&mut cursor, "CREATE POLICY requires a policy name")?;
    expect_keyword(&mut cursor, "ON")?;
    let table_name = required_token(&mut cursor, "CREATE POLICY requires a table name")?;
    expect_keyword(&mut cursor, "USING")?;
    let using_sql = cursor.remaining_trimmed_without_semicolon();
    if using_sql.is_empty() {
        return Err(DbError::sql("CREATE POLICY requires a USING expression"));
    }
    parse_expression_sql(using_sql)?;
    Ok(SecurityCommand::CreatePolicy {
        name,
        table_name,
        using_sql: using_sql.to_string(),
    })
}

fn parse_drop_policy(mut cursor: TokenCursor<'_>) -> Result<SecurityCommand> {
    let if_exists = consume_if_exists(&mut cursor)?;
    let name = required_token(&mut cursor, "DROP POLICY requires a policy name")?;
    cursor.expect_end()?;
    Ok(SecurityCommand::DropPolicy { name, if_exists })
}

fn parse_alter_policy(mut cursor: TokenCursor<'_>) -> Result<SecurityCommand> {
    let name = required_token(&mut cursor, "ALTER POLICY requires a policy name")?;
    let action = required_token(&mut cursor, "ALTER POLICY requires ENABLE or DISABLE")?;
    cursor.expect_end()?;
    let enabled = if action.eq_ignore_ascii_case("ENABLE") {
        true
    } else if action.eq_ignore_ascii_case("DISABLE") {
        false
    } else {
        return Err(DbError::sql("ALTER POLICY supports ENABLE or DISABLE"));
    };
    Ok(SecurityCommand::AlterPolicy { name, enabled })
}

fn parse_create_mask(mut cursor: TokenCursor<'_>) -> Result<SecurityCommand> {
    let name = required_token(&mut cursor, "CREATE MASK requires a mask name")?;
    expect_keyword(&mut cursor, "ON")?;
    let table_name = required_token(&mut cursor, "CREATE MASK requires a table name")?;
    let open = required_token(&mut cursor, "CREATE MASK requires a column in parentheses")?;
    if open != "(" {
        return Err(DbError::sql("CREATE MASK requires a column in parentheses"));
    }
    let column_name = required_token(&mut cursor, "CREATE MASK requires a column name")?;
    let close = required_token(&mut cursor, "CREATE MASK requires ')' after the column")?;
    if close != ")" {
        return Err(DbError::sql("CREATE MASK requires ')' after the column"));
    }
    expect_keyword(&mut cursor, "USING")?;
    let expression_sql = cursor.remaining_trimmed_without_semicolon();
    if expression_sql.is_empty() {
        return Err(DbError::sql("CREATE MASK requires a USING expression"));
    }
    parse_expression_sql(expression_sql)?;
    Ok(SecurityCommand::CreateMask {
        name,
        table_name,
        column_name,
        expression_sql: expression_sql.to_string(),
    })
}

fn parse_drop_mask(mut cursor: TokenCursor<'_>) -> Result<SecurityCommand> {
    let if_exists = consume_if_exists(&mut cursor)?;
    let name = required_token(&mut cursor, "DROP MASK requires a mask name")?;
    cursor.expect_end()?;
    Ok(SecurityCommand::DropMask { name, if_exists })
}

fn parse_alter_mask(mut cursor: TokenCursor<'_>) -> Result<SecurityCommand> {
    let name = required_token(&mut cursor, "ALTER MASK requires a mask name")?;
    let action = required_token(&mut cursor, "ALTER MASK requires ENABLE or DISABLE")?;
    cursor.expect_end()?;
    let enabled = if action.eq_ignore_ascii_case("ENABLE") {
        true
    } else if action.eq_ignore_ascii_case("DISABLE") {
        false
    } else {
        return Err(DbError::sql("ALTER MASK supports ENABLE or DISABLE"));
    };
    Ok(SecurityCommand::AlterMask { name, enabled })
}

fn consume_if_exists(cursor: &mut TokenCursor<'_>) -> Result<bool> {
    let checkpoint = cursor.position();
    let Some(first) = cursor.next_token()? else {
        return Ok(false);
    };
    let Some(second) = cursor.next_token()? else {
        cursor.set_position(checkpoint);
        return Ok(false);
    };
    if first.eq_ignore_ascii_case("IF") && second.eq_ignore_ascii_case("EXISTS") {
        Ok(true)
    } else {
        cursor.set_position(checkpoint);
        Ok(false)
    }
}

fn expect_keyword(cursor: &mut TokenCursor<'_>, keyword: &str) -> Result<()> {
    let token = required_token(cursor, format!("expected {keyword}"))?;
    if token.eq_ignore_ascii_case(keyword) {
        Ok(())
    } else {
        Err(DbError::sql(format!("expected {keyword}, got {token}")))
    }
}

fn required_token(cursor: &mut TokenCursor<'_>, message: impl Into<String>) -> Result<String> {
    cursor
        .next_token()?
        .ok_or_else(|| DbError::sql(message.into()))
}

fn parse_literal_value(sql: &str) -> Result<Value> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(Value::Null);
    }
    if trimmed.eq_ignore_ascii_case("TRUE") {
        return Ok(Value::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("FALSE") {
        return Ok(Value::Bool(false));
    }
    if let Some(text) = parse_single_quoted_literal(trimmed)? {
        return Ok(Value::Text(text));
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return Ok(Value::Int64(value));
    }
    Err(DbError::sql(
        "SET AUDIT CONTEXT values must be NULL, BOOL, INT64, or a string literal",
    ))
}

fn parse_single_quoted_literal(input: &str) -> Result<Option<String>> {
    let bytes = input.as_bytes();
    if bytes.first().copied() != Some(b'\'') {
        return Ok(None);
    }
    let mut out = String::new();
    let mut index = 1_usize;
    while index < bytes.len() {
        let ch = bytes[index] as char;
        if ch == '\'' {
            if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                out.push('\'');
                index += 2;
                continue;
            }
            if input[index + 1..].trim().is_empty() {
                return Ok(Some(out));
            }
            return Err(DbError::sql("unexpected text after string literal"));
        }
        out.push(ch);
        index += 1;
    }
    Err(DbError::sql("unterminated string literal"))
}

#[derive(Clone, Copy)]
struct TokenCursor<'a> {
    sql: &'a str,
    index: usize,
}

impl<'a> TokenCursor<'a> {
    fn new(sql: &'a str) -> Self {
        Self { sql, index: 0 }
    }

    fn position(&self) -> usize {
        self.index
    }

    fn set_position(&mut self, position: usize) {
        self.index = position;
    }

    fn next_token(&mut self) -> Result<Option<String>> {
        self.skip_ws();
        let bytes = self.sql.as_bytes();
        if self.index >= bytes.len() || bytes[self.index] == b';' {
            return Ok(None);
        }
        match bytes[self.index] {
            b'(' | b')' | b'=' => {
                let token = (bytes[self.index] as char).to_string();
                self.index += 1;
                Ok(Some(token))
            }
            b'"' => self.next_quoted_identifier(),
            b'\'' => {
                let start = self.index;
                self.consume_single_quoted_literal()?;
                parse_single_quoted_literal(&self.sql[start..self.index])
            }
            _ => {
                let start = self.index;
                while self.index < bytes.len() {
                    let ch = bytes[self.index] as char;
                    if ch.is_ascii_whitespace() || matches!(ch, '(' | ')' | '=' | ';') {
                        break;
                    }
                    self.index += 1;
                }
                Ok(Some(self.sql[start..self.index].to_string()))
            }
        }
    }

    fn remaining_trimmed_without_semicolon(&self) -> &'a str {
        self.sql[self.index..].trim().trim_end_matches(';').trim()
    }

    fn expect_end(&mut self) -> Result<()> {
        if self.remaining_trimmed_without_semicolon().is_empty() {
            Ok(())
        } else {
            Err(DbError::sql(format!(
                "unexpected trailing SQL: {}",
                self.remaining_trimmed_without_semicolon()
            )))
        }
    }

    fn skip_ws(&mut self) {
        let bytes = self.sql.as_bytes();
        while self.index < bytes.len() && (bytes[self.index] as char).is_ascii_whitespace() {
            self.index += 1;
        }
    }

    fn next_quoted_identifier(&mut self) -> Result<Option<String>> {
        let bytes = self.sql.as_bytes();
        self.index += 1;
        let mut out = String::new();
        while self.index < bytes.len() {
            let ch = bytes[self.index] as char;
            if ch == '"' {
                if self.index + 1 < bytes.len() && bytes[self.index + 1] == b'"' {
                    out.push('"');
                    self.index += 2;
                    continue;
                }
                self.index += 1;
                return Ok(Some(out));
            }
            out.push(ch);
            self.index += 1;
        }
        Err(DbError::sql("unterminated quoted identifier"))
    }

    fn consume_single_quoted_literal(&mut self) -> Result<()> {
        let bytes = self.sql.as_bytes();
        self.index += 1;
        while self.index < bytes.len() {
            if bytes[self.index] == b'\'' {
                if self.index + 1 < bytes.len() && bytes[self.index + 1] == b'\'' {
                    self.index += 2;
                    continue;
                }
                self.index += 1;
                return Ok(());
            }
            self.index += 1;
        }
        Err(DbError::sql("unterminated string literal"))
    }
}
