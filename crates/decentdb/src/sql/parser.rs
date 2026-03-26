//! Parser wrapper that normalizes supported SQL into the engine AST.

use std::borrow::Cow;

use crate::error::{DbError, Result};

use super::ast::{Expr, SelectItem, Statement};
use super::normalize::{normalize_statement_text, normalize_statement_text_with_generated_modes};

pub(crate) fn parse_sql_statement(sql: &str) -> Result<Statement> {
    let mut statements = parse_sql_batch(sql)?;
    if statements.len() != 1 {
        return Err(DbError::sql(format!(
            "expected exactly one SQL statement, got {}",
            statements.len()
        )));
    }
    Ok(statements.remove(0))
}

pub(crate) fn parse_sql_batch(sql: &str) -> Result<Vec<Statement>> {
    let (generated_modes, sql_with_generated_rewrite) = rewrite_generated_virtual_columns(sql);
    let compat_sql = rewrite_legacy_trigger_body(sql_with_generated_rewrite.as_ref());
    let statements = libpg_query_sys::split_statements(compat_sql.as_ref())
        .map_err(|error| DbError::sql(error.message().to_string()))?;
    if statements.is_empty() {
        return Err(DbError::sql("no SQL statements found"));
    }

    let mut generated_mode_index = 0_usize;
    statements
        .into_iter()
        .filter(|statement| !statement.trim().is_empty())
        .map(|statement| {
            let generated_count = count_generated_columns(&statement);
            if generated_mode_index.saturating_add(generated_count) > generated_modes.len() {
                return Err(DbError::sql(
                    "failed to normalize generated column definition",
                ));
            }
            let modes = &generated_modes
                [generated_mode_index..generated_mode_index.saturating_add(generated_count)];
            generated_mode_index = generated_mode_index.saturating_add(generated_count);
            if generated_count == 0 {
                normalize_statement_text(&statement)
            } else {
                normalize_statement_text_with_generated_modes(&statement, modes)
            }
        })
        .collect()
}

fn rewrite_generated_virtual_columns(sql: &str) -> (Vec<bool>, Cow<'_, str>) {
    let mut generated_modes = Vec::new();
    let mut output = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut index = 0_usize;
    let mut changed = false;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while index < bytes.len() {
        let ch = bytes[index] as char;
        if in_line_comment {
            output.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            index += 1;
            continue;
        }
        if in_block_comment {
            output.push(ch);
            if ch == '*' && index + 1 < bytes.len() && bytes[index + 1] as char == '/' {
                output.push('/');
                index += 2;
                in_block_comment = false;
            } else {
                index += 1;
            }
            continue;
        }
        if in_single {
            output.push(ch);
            if ch == '\'' {
                if index + 1 < bytes.len() && bytes[index + 1] as char == '\'' {
                    output.push('\'');
                    index += 2;
                } else {
                    index += 1;
                    in_single = false;
                }
            } else {
                index += 1;
            }
            continue;
        }
        if in_double {
            output.push(ch);
            if ch == '"' {
                if index + 1 < bytes.len() && bytes[index + 1] as char == '"' {
                    output.push('"');
                    index += 2;
                } else {
                    index += 1;
                    in_double = false;
                }
            } else {
                index += 1;
            }
            continue;
        }

        if ch == '-' && index + 1 < bytes.len() && bytes[index + 1] as char == '-' {
            output.push('-');
            output.push('-');
            index += 2;
            in_line_comment = true;
            continue;
        }
        if ch == '/' && index + 1 < bytes.len() && bytes[index + 1] as char == '*' {
            output.push('/');
            output.push('*');
            index += 2;
            in_block_comment = true;
            continue;
        }
        if ch == '\'' {
            output.push(ch);
            in_single = true;
            index += 1;
            continue;
        }
        if ch == '"' {
            output.push(ch);
            in_double = true;
            index += 1;
            continue;
        }

        if starts_with_keyword(bytes, index, "GENERATED") {
            let output_len_before = output.len();
            let generated_start = index;
            output.push_str("GENERATED");
            index += "GENERATED".len();
            let mut local_modes = Vec::new();
            let ws_after_generated = consume_and_copy_whitespace(bytes, &mut output, index);
            index = ws_after_generated;
            if starts_with_keyword(bytes, index, "ALWAYS") {
                output.push_str("ALWAYS");
                index += "ALWAYS".len();

                index = consume_and_copy_whitespace(bytes, &mut output, index);
                if starts_with_keyword(bytes, index, "AS") {
                    output.push_str("AS");
                    index += "AS".len();

                    index = consume_and_copy_whitespace(bytes, &mut output, index);
                    if index < bytes.len() && bytes[index] as char == '(' {
                        output.push('(');
                        index += 1;
                        let mut depth = 1_i32;
                        let mut expr_in_single = false;
                        let mut expr_in_double = false;
                        let mut expr_in_line_comment = false;
                        let mut expr_in_block_comment = false;
                        while index < bytes.len() && depth > 0 {
                            let expr_ch = bytes[index] as char;
                            output.push(expr_ch);
                            if expr_in_line_comment {
                                if expr_ch == '\n' {
                                    expr_in_line_comment = false;
                                }
                                index += 1;
                                continue;
                            }
                            if expr_in_block_comment {
                                if expr_ch == '*'
                                    && index + 1 < bytes.len()
                                    && bytes[index + 1] as char == '/'
                                {
                                    output.push('/');
                                    index += 2;
                                    expr_in_block_comment = false;
                                } else {
                                    index += 1;
                                }
                                continue;
                            }
                            if expr_in_single {
                                if expr_ch == '\'' {
                                    if index + 1 < bytes.len() && bytes[index + 1] as char == '\'' {
                                        output.push('\'');
                                        index += 2;
                                    } else {
                                        index += 1;
                                        expr_in_single = false;
                                    }
                                } else {
                                    index += 1;
                                }
                                continue;
                            }
                            if expr_in_double {
                                if expr_ch == '"' {
                                    if index + 1 < bytes.len() && bytes[index + 1] as char == '"' {
                                        output.push('"');
                                        index += 2;
                                    } else {
                                        index += 1;
                                        expr_in_double = false;
                                    }
                                } else {
                                    index += 1;
                                }
                                continue;
                            }
                            if expr_ch == '-'
                                && index + 1 < bytes.len()
                                && bytes[index + 1] as char == '-'
                            {
                                output.push('-');
                                index += 2;
                                expr_in_line_comment = true;
                                continue;
                            }
                            if expr_ch == '/'
                                && index + 1 < bytes.len()
                                && bytes[index + 1] as char == '*'
                            {
                                output.push('*');
                                index += 2;
                                expr_in_block_comment = true;
                                continue;
                            }
                            if expr_ch == '\'' {
                                expr_in_single = true;
                                index += 1;
                                continue;
                            }
                            if expr_ch == '"' {
                                expr_in_double = true;
                                index += 1;
                                continue;
                            }
                            if expr_ch == '(' {
                                depth += 1;
                                index += 1;
                                continue;
                            }
                            if expr_ch == ')' {
                                depth -= 1;
                                index += 1;
                                continue;
                            }
                            index += 1;
                        }

                        index = consume_and_copy_whitespace(bytes, &mut output, index);
                        if starts_with_keyword(bytes, index, "STORED") {
                            output.push_str("STORED");
                            index += "STORED".len();
                            local_modes.push(true);
                        } else if starts_with_keyword(bytes, index, "VIRTUAL") {
                            output.push_str("STORED");
                            index += "VIRTUAL".len();
                            local_modes.push(false);
                            changed = true;
                        } else {
                            local_modes.push(true);
                        }
                    }
                }
            }
            if local_modes.is_empty() {
                output.truncate(output_len_before);
                output.push(bytes[generated_start] as char);
                index = generated_start + 1;
            } else {
                generated_modes.extend(local_modes);
            }
            continue;
        }

        output.push(ch);
        index += 1;
    }

    if changed {
        (generated_modes, Cow::Owned(output))
    } else {
        (generated_modes, Cow::Borrowed(sql))
    }
}

fn count_generated_columns(sql: &str) -> usize {
    let mut count = 0_usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let bytes = sql.as_bytes();
    let mut index = 0_usize;
    while index < bytes.len() {
        let ch = bytes[index] as char;
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            index += 1;
            continue;
        }
        if in_block_comment {
            if ch == '*' && index + 1 < bytes.len() && bytes[index + 1] as char == '/' {
                index += 2;
                in_block_comment = false;
            } else {
                index += 1;
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                if index + 1 < bytes.len() && bytes[index + 1] as char == '\'' {
                    index += 2;
                } else {
                    index += 1;
                    in_single = false;
                }
            } else {
                index += 1;
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                if index + 1 < bytes.len() && bytes[index + 1] as char == '"' {
                    index += 2;
                } else {
                    index += 1;
                    in_double = false;
                }
            } else {
                index += 1;
            }
            continue;
        }
        if ch == '-' && index + 1 < bytes.len() && bytes[index + 1] as char == '-' {
            index += 2;
            in_line_comment = true;
            continue;
        }
        if ch == '/' && index + 1 < bytes.len() && bytes[index + 1] as char == '*' {
            index += 2;
            in_block_comment = true;
            continue;
        }
        if ch == '\'' {
            index += 1;
            in_single = true;
            continue;
        }
        if ch == '"' {
            index += 1;
            in_double = true;
            continue;
        }
        if starts_with_keyword(bytes, index, "GENERATED") {
            let generated_start = index;
            index += "GENERATED".len();
            while index < bytes.len() && (bytes[index] as char).is_ascii_whitespace() {
                index += 1;
            }
            if !starts_with_keyword(bytes, index, "ALWAYS") {
                index = generated_start + 1;
                continue;
            }
            index += "ALWAYS".len();
            while index < bytes.len() && (bytes[index] as char).is_ascii_whitespace() {
                index += 1;
            }
            if !starts_with_keyword(bytes, index, "AS") {
                index = generated_start + 1;
                continue;
            }
            index += "AS".len();
            while index < bytes.len() && (bytes[index] as char).is_ascii_whitespace() {
                index += 1;
            }
            if index < bytes.len() && bytes[index] as char == '(' {
                count += 1;
            } else {
                index = generated_start + 1;
            }
            continue;
        }
        index += 1;
    }
    count
}

fn starts_with_keyword(bytes: &[u8], index: usize, keyword: &str) -> bool {
    let end = index.saturating_add(keyword.len());
    if end > bytes.len() {
        return false;
    }
    if index > 0 {
        let prev = bytes[index - 1] as char;
        if is_keyword_char(prev) {
            return false;
        }
    }
    let mut keyword_bytes = keyword.bytes();
    for &candidate in &bytes[index..end] {
        let Some(keyword_byte) = keyword_bytes.next() else {
            return false;
        };
        if !candidate.eq_ignore_ascii_case(&keyword_byte) {
            return false;
        }
    }
    if end < bytes.len() {
        let next = bytes[end] as char;
        if is_keyword_char(next) {
            return false;
        }
    }
    true
}

fn consume_and_copy_whitespace(bytes: &[u8], output: &mut String, mut index: usize) -> usize {
    while index < bytes.len() && (bytes[index] as char).is_ascii_whitespace() {
        output.push(bytes[index] as char);
        index += 1;
    }
    index
}

pub(crate) fn parse_expression_sql(sql: &str) -> Result<Expr> {
    match parse_sql_statement(&format!("SELECT {sql}"))? {
        Statement::Query(query) => match query.body {
            super::ast::QueryBody::Select(select) if select.projection.len() == 1 => {
                match select
                    .projection
                    .into_iter()
                    .next()
                    .expect("projection exists")
                {
                    SelectItem::Expr { expr, .. } => Ok(expr),
                    _ => Err(DbError::sql(
                        "expression parser expected a scalar SELECT item",
                    )),
                }
            }
            super::ast::QueryBody::Values(rows) if rows.len() == 1 && rows[0].len() == 1 => {
                Ok(rows[0][0].clone())
            }
            _ => Err(DbError::sql("expression parser expected a simple SELECT")),
        },
        _ => Err(DbError::sql(
            "expression parser expected a SELECT statement",
        )),
    }
}

pub(crate) fn rewrite_legacy_trigger_body(sql: &str) -> Cow<'_, str> {
    let trimmed = sql.trim();
    let keywords = top_level_keywords(trimmed);
    if keywords.len() < 2 || keywords[0].2 != "CREATE" || keywords[1].2 != "TRIGGER" {
        return Cow::Borrowed(sql);
    }

    let Some((begin_start, begin_end, _)) =
        keywords.iter().find(|(_, _, keyword)| keyword == "BEGIN")
    else {
        return Cow::Borrowed(sql);
    };
    let Some((end_start, _, _)) = keywords
        .iter()
        .rev()
        .find(|(start, _, keyword)| *start > *begin_start && keyword == "END")
    else {
        return Cow::Borrowed(sql);
    };

    let prefix = trimmed[..*begin_start].trim_end();
    let mut body = trimmed[*begin_end..*end_start].trim();
    while let Some(stripped) = body.strip_suffix(';') {
        body = stripped.trim_end();
    }
    if body.is_empty() || has_top_level_semicolon(body) {
        return Cow::Borrowed(sql);
    }

    let body_keywords = top_level_keywords(body);
    let Some((select_start, select_end, keyword)) = body_keywords.first() else {
        return Cow::Borrowed(sql);
    };
    if *select_start != 0 || keyword != "SELECT" {
        return Cow::Borrowed(sql);
    }
    let action = body[*select_end..].trim();
    if action.is_empty() {
        return Cow::Borrowed(sql);
    }

    Cow::Owned(format!("{prefix} EXECUTE FUNCTION {action}"))
}

fn top_level_keywords(sql: &str) -> Vec<(usize, usize, String)> {
    let mut keywords = Vec::new();
    let mut chars = sql.char_indices().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some((index, ch)) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }
        if in_block_comment {
            if ch == '*' && matches!(chars.peek(), Some((_, '/'))) {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    chars.next();
                } else {
                    in_single = false;
                }
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                if matches!(chars.peek(), Some((_, '"'))) {
                    chars.next();
                } else {
                    in_double = false;
                }
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
            }
            '"' => {
                in_double = true;
            }
            '-' if matches!(chars.peek(), Some((_, '-'))) => {
                chars.next();
                in_line_comment = true;
            }
            '/' if matches!(chars.peek(), Some((_, '*'))) => {
                chars.next();
                in_block_comment = true;
            }
            _ if is_keyword_char(ch) => {
                let mut end = index + ch.len_utf8();
                let mut keyword = ch.to_ascii_uppercase().to_string();
                while let Some((next_index, next)) = chars.peek().copied() {
                    if !is_keyword_char(next) {
                        break;
                    }
                    chars.next();
                    end = next_index + next.len_utf8();
                    keyword.push(next.to_ascii_uppercase());
                }
                keywords.push((index, end, keyword));
            }
            _ => {}
        }
    }

    keywords
}

fn has_top_level_semicolon(sql: &str) -> bool {
    let mut chars = sql.char_indices().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some((_, ch)) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }
        if in_block_comment {
            if ch == '*' && matches!(chars.peek(), Some((_, '/'))) {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    chars.next();
                } else {
                    in_single = false;
                }
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                if matches!(chars.peek(), Some((_, '"'))) {
                    chars.next();
                } else {
                    in_double = false;
                }
            }
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '-' if matches!(chars.peek(), Some((_, '-'))) => {
                chars.next();
                in_line_comment = true;
            }
            '/' if matches!(chars.peek(), Some((_, '*'))) => {
                chars.next();
                in_block_comment = true;
            }
            ';' => return true,
            _ => {}
        }
    }

    false
}

fn is_keyword_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── top_level_keywords ──────────────────────────────────────────

    #[test]
    fn keywords_simple_select() {
        let kw = top_level_keywords("SELECT a FROM t WHERE x = 1");
        let names: Vec<&str> = kw.iter().map(|(_, _, k)| k.as_str()).collect();
        assert_eq!(names, vec!["SELECT", "A", "FROM", "T", "WHERE", "X", "1"]);
    }

    #[test]
    fn keywords_skip_single_quoted_string() {
        let kw = top_level_keywords("SELECT 'hello world' FROM t");
        let names: Vec<&str> = kw.iter().map(|(_, _, k)| k.as_str()).collect();
        assert!(
            !names.contains(&"HELLO"),
            "content inside single quotes should be skipped"
        );
        assert!(names.contains(&"SELECT"));
        assert!(names.contains(&"FROM"));
    }

    #[test]
    fn keywords_escaped_single_quote() {
        let kw = top_level_keywords("SELECT 'it''s' FROM t");
        let names: Vec<&str> = kw.iter().map(|(_, _, k)| k.as_str()).collect();
        assert!(
            !names.contains(&"S"),
            "escaped single quote should not end string"
        );
        assert!(names.contains(&"FROM"));
    }

    #[test]
    fn keywords_skip_double_quoted_identifier() {
        let kw = top_level_keywords(r#"SELECT "My Column" FROM t"#);
        let names: Vec<&str> = kw.iter().map(|(_, _, k)| k.as_str()).collect();
        assert!(!names.contains(&"MY"));
        assert!(!names.contains(&"COLUMN"));
    }

    #[test]
    fn keywords_escaped_double_quote() {
        let kw = top_level_keywords(r#"SELECT "a""b" FROM t"#);
        let names: Vec<&str> = kw.iter().map(|(_, _, k)| k.as_str()).collect();
        assert!(
            !names.contains(&"B"),
            "escaped double quote should stay in identifier"
        );
        assert!(names.contains(&"FROM"));
    }

    #[test]
    fn keywords_skip_line_comment() {
        let kw = top_level_keywords("SELECT a -- this is a comment\nFROM t");
        let names: Vec<&str> = kw.iter().map(|(_, _, k)| k.as_str()).collect();
        assert!(!names.contains(&"THIS"));
        assert!(!names.contains(&"COMMENT"));
        assert!(names.contains(&"FROM"));
    }

    #[test]
    fn keywords_skip_block_comment() {
        let kw = top_level_keywords("SELECT /* hidden keyword CREATE */ a FROM t");
        let names: Vec<&str> = kw.iter().map(|(_, _, k)| k.as_str()).collect();
        assert!(!names.contains(&"HIDDEN"));
        assert!(!names.contains(&"CREATE"));
        assert!(names.contains(&"A"));
        assert!(names.contains(&"FROM"));
    }

    #[test]
    fn keywords_preserves_positions() {
        let kw = top_level_keywords("SELECT a");
        assert_eq!(kw[0], (0, 6, "SELECT".to_string()));
        assert_eq!(kw[1], (7, 8, "A".to_string()));
    }

    #[test]
    fn keywords_empty_input() {
        assert!(top_level_keywords("").is_empty());
    }

    #[test]
    fn keywords_only_comment() {
        assert!(top_level_keywords("-- nothing here").is_empty());
    }

    #[test]
    fn keywords_mixed_comment_and_string() {
        let kw = top_level_keywords("SELECT 'it''s -- not a comment' FROM t -- real comment");
        let names: Vec<&str> = kw.iter().map(|(_, _, k)| k.as_str()).collect();
        assert!(names.contains(&"SELECT"));
        assert!(names.contains(&"FROM"));
        assert!(names.contains(&"T"));
        assert!(!names.contains(&"NOT"));
        assert!(!names.contains(&"REAL"));
    }

    // ── has_top_level_semicolon ─────────────────────────────────────

    #[test]
    fn semicolon_plain() {
        assert!(has_top_level_semicolon("SELECT 1; SELECT 2"));
    }

    #[test]
    fn semicolon_none() {
        assert!(!has_top_level_semicolon("SELECT 1"));
    }

    #[test]
    fn semicolon_inside_single_quotes() {
        assert!(!has_top_level_semicolon("SELECT 'a;b' FROM t"));
    }

    #[test]
    fn semicolon_inside_double_quotes() {
        assert!(!has_top_level_semicolon(r#"SELECT "a;b" FROM t"#));
    }

    #[test]
    fn semicolon_inside_line_comment() {
        assert!(!has_top_level_semicolon("SELECT 1 -- no;semi\n FROM t"));
    }

    #[test]
    fn semicolon_inside_block_comment() {
        assert!(!has_top_level_semicolon("SELECT /* no;semi */ 1 FROM t"));
    }

    #[test]
    fn semicolon_after_escaped_single_quote() {
        assert!(has_top_level_semicolon("SELECT 'it''s'; SELECT 2"));
    }

    #[test]
    fn semicolon_after_escaped_double_quote() {
        assert!(has_top_level_semicolon(r#"SELECT "a""b"; SELECT 2"#));
    }

    #[test]
    fn semicolon_empty_input() {
        assert!(!has_top_level_semicolon(""));
    }

    #[test]
    fn semicolon_after_block_comment_end() {
        assert!(has_top_level_semicolon("SELECT /* x */ 1; SELECT 2"));
    }

    #[test]
    fn semicolon_after_line_comment_newline() {
        assert!(has_top_level_semicolon("SELECT 1 -- comment\n; SELECT 2"));
    }

    // ── rewrite_legacy_trigger_body ─────────────────────────────────

    #[test]
    fn rewrite_passthrough_non_trigger() {
        let sql = "SELECT 1 FROM t";
        assert_eq!(rewrite_legacy_trigger_body(sql).as_ref(), sql);
    }

    #[test]
    fn rewrite_trigger_without_begin() {
        let sql = "CREATE TRIGGER trg AFTER INSERT ON t EXECUTE FUNCTION fn()";
        assert_eq!(rewrite_legacy_trigger_body(sql).as_ref(), sql);
    }

    #[test]
    fn rewrite_trigger_with_begin_select() {
        let sql = "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN SELECT my_func(); END";
        let result = rewrite_legacy_trigger_body(sql);
        assert!(
            result.contains("EXECUTE FUNCTION my_func()"),
            "got: {result}"
        );
        assert!(!result.contains("BEGIN"));
    }

    #[test]
    fn rewrite_trigger_with_trailing_semicolons() {
        let sql = "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN SELECT my_func();;; END";
        let result = rewrite_legacy_trigger_body(sql);
        assert!(
            result.contains("EXECUTE FUNCTION my_func()"),
            "got: {result}"
        );
    }

    #[test]
    fn rewrite_trigger_empty_body() {
        let sql = "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN END";
        // Empty body — should not rewrite
        assert_eq!(rewrite_legacy_trigger_body(sql).as_ref(), sql);
    }

    #[test]
    fn rewrite_trigger_multi_statement_body() {
        let sql =
            "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN SELECT a(); SELECT b(); END";
        // Has semicolon in body — should not rewrite
        assert_eq!(rewrite_legacy_trigger_body(sql).as_ref(), sql);
    }

    #[test]
    fn rewrite_trigger_non_select_body() {
        let sql = "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (1) END";
        // Body starts with INSERT, not SELECT — should not rewrite
        assert_eq!(rewrite_legacy_trigger_body(sql).as_ref(), sql);
    }

    #[test]
    fn rewrite_trigger_missing_end() {
        let sql = "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW BEGIN SELECT fn()";
        // No END keyword — should not rewrite
        assert_eq!(rewrite_legacy_trigger_body(sql).as_ref(), sql);
    }

    // ── parse_expression_sql ────────────────────────────────────────

    #[test]
    fn parse_expr_simple() {
        let expr = parse_expression_sql("1 + 2").unwrap();
        matches!(expr, Expr::Binary { .. });
    }

    #[test]
    fn parse_expr_not_select() {
        let err = parse_expression_sql("INSERT INTO t VALUES (1)");
        assert!(err.is_err());
    }

    // ── parse_sql_statement ─────────────────────────────────────────

    #[test]
    fn parse_multiple_statements_error() {
        let err = parse_sql_statement("SELECT 1; SELECT 2");
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("exactly one"), "got: {msg}");
    }

    #[test]
    fn parse_empty_sql_error() {
        let err = parse_sql_statement("");
        assert!(err.is_err());
    }

    #[test]
    fn parse_virtual_generated_column() {
        let statement = parse_sql_statement(
            "CREATE TABLE t (id INT PRIMARY KEY, x INT, y INT GENERATED ALWAYS AS (x * 2) VIRTUAL)",
        )
        .unwrap();
        let Statement::CreateTable(create) = statement else {
            panic!("expected create table statement");
        };
        assert!(create.columns[2].generated.is_some());
        assert!(!create.columns[2].generated_stored);
    }

    // ── is_keyword_char ─────────────────────────────────────────────

    #[test]
    fn keyword_char_alpha() {
        assert!(is_keyword_char('a'));
        assert!(is_keyword_char('Z'));
    }

    #[test]
    fn keyword_char_digit() {
        assert!(is_keyword_char('0'));
        assert!(is_keyword_char('9'));
    }

    #[test]
    fn keyword_char_underscore() {
        assert!(is_keyword_char('_'));
    }

    #[test]
    fn keyword_char_special() {
        assert!(!is_keyword_char(' '));
        assert!(!is_keyword_char(';'));
        assert!(!is_keyword_char('('));
    }
}
