use super::FTS_QUERY_ERROR_PREFIX;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FtsQueryTermKind {
    Word,
    Phrase,
    Prefix,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FtsQueryTerm {
    pub(crate) kind: FtsQueryTermKind,
    pub(crate) text: String,
    pub(crate) excluded: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FtsQuery {
    pub(crate) clauses: Vec<Vec<FtsQueryTerm>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FtsQueryParseError {
    pub(crate) message: String,
}

pub(crate) type FtsQueryError = std::result::Result<FtsQuery, FtsQueryParseError>;

impl FtsQuery {
    pub(crate) fn has_positive_term(&self) -> bool {
        self.clauses.iter().flatten().any(|term| !term.excluded)
    }
}

pub(crate) fn parse_fts_query(input: &str) -> FtsQueryError {
    let mut cursor = Cursor::new(input);
    cursor.skip_whitespace();
    if cursor.is_eof() {
        return Err(error("empty query"));
    }

    if cursor.is_or() {
        return Err(error("query cannot start with OR"));
    }

    let mut clauses = Vec::new();
    let mut clause = Vec::new();

    loop {
        let term = parse_term(&mut cursor)?;
        clause.push(term);
        let had_separator = cursor.peek().is_none_or(is_whitespace);
        cursor.skip_whitespace();

        if cursor.is_eof() {
            break;
        }

        if cursor.is_or() {
            if clause.is_empty() {
                return Err(error("OR must appear between terms"));
            }
            clauses.push(std::mem::take(&mut clause));
            cursor.consume_or();
            cursor.skip_whitespace();
            if cursor.is_eof() {
                return Err(error("query cannot end with OR"));
            }
            if cursor.is_or() {
                return Err(error("OR must appear between terms"));
            }
            continue;
        }

        if had_separator {
            continue;
        }
        return Err(error("unrecognized character between query terms"));
    }

    if !clause.is_empty() {
        clauses.push(clause);
    }

    if clauses.is_empty() || clauses.iter().any(std::vec::Vec::is_empty) {
        return Err(error("query has no terms"));
    }

    let query = FtsQuery { clauses };
    if !query.has_positive_term() {
        return Err(error("query must contain at least one non-excluded term"));
    }
    Ok(query)
}

fn parse_term(cursor: &mut Cursor<'_>) -> std::result::Result<FtsQueryTerm, FtsQueryParseError> {
    let mut excluded = false;
    if cursor.consume(b'-') {
        if cursor.is_eof() || is_whitespace(cursor.peek().unwrap_or_default()) {
            return Err(error("'-' must prefix a term"));
        }
        excluded = true;
    }

    if cursor.is_eof() {
        return Err(error("query term is empty"));
    }

    if cursor.consume(b'"') {
        let phrase = parse_quoted_phrase(cursor)?;
        if phrase.trim().is_empty() {
            return Err(error("quoted phrase cannot be empty"));
        }
        return Ok(FtsQueryTerm {
            kind: FtsQueryTermKind::Phrase,
            text: phrase,
            excluded,
        });
    }

    let (token, wildcard) = parse_token(cursor)?;
    if token.is_empty() {
        return Err(error("query term is empty"));
    }

    if wildcard {
        if token.len() == 1 {
            return Err(error(
                "prefix term requires at least one character before '*'",
            ));
        }
        return Ok(FtsQueryTerm {
            kind: FtsQueryTermKind::Prefix,
            text: token[..token.len() - 1].to_string(),
            excluded,
        });
    }

    Ok(FtsQueryTerm {
        kind: FtsQueryTermKind::Word,
        text: token,
        excluded,
    })
}

fn parse_quoted_phrase(cursor: &mut Cursor<'_>) -> std::result::Result<String, FtsQueryParseError> {
    let mut phrase = String::new();
    while let Some(byte) = cursor.next() {
        match byte {
            b'\\' => {
                let escaped = cursor
                    .next()
                    .ok_or_else(|| error("trailing backslash in quoted phrase"))?;
                match escaped {
                    b'"' | b'\\' => phrase.push(escaped as char),
                    _ => return Err(error("invalid quoted phrase escape")),
                }
            }
            b'"' => return Ok(phrase),
            _ => phrase.push(byte as char),
        }
    }
    Err(error("unterminated quoted phrase"))
}

fn parse_token(cursor: &mut Cursor<'_>) -> std::result::Result<(String, bool), FtsQueryParseError> {
    let mut token = String::new();
    let mut wildcard = false;

    while let Some(byte) = cursor.peek() {
        if is_whitespace(byte) {
            break;
        }
        match byte {
            b'"' => return Err(error("quote must start a quoted phrase")),
            b'\\' => {
                cursor.advance();
                let escaped = cursor.next().ok_or_else(|| error("trailing backslash"))?;
                match escaped {
                    b'"' | b'\\' | b'*' | b'-' => token.push(escaped as char),
                    _ => return Err(error("invalid escape sequence")),
                }
            }
            b'*' => {
                cursor.advance();
                if cursor.is_eof() || is_whitespace(cursor.peek().unwrap_or_default()) {
                    wildcard = true;
                    token.push('*');
                    break;
                }
                return Err(error("prefix wildcard must terminate term"));
            }
            _ => {
                cursor.advance();
                token.push(byte as char);
            }
        }
    }

    Ok((token, wildcard))
}

fn error(message: &str) -> FtsQueryParseError {
    FtsQueryParseError {
        message: format!("{FTS_QUERY_ERROR_PREFIX} {message}"),
    }
}

fn is_whitespace(byte: u8) -> bool {
    byte.is_ascii_whitespace()
}

fn is_or_starts_at(bytes: &[u8], offset: usize) -> bool {
    if bytes.get(offset) != Some(&b'O') {
        return false;
    }
    if bytes.get(offset + 1) != Some(&b'R') {
        return false;
    }
    bytes
        .get(offset + 2)
        .is_none_or(|next| is_whitespace(*next))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            offset: 0,
        }
    }

    fn is_eof(&self) -> bool {
        self.offset >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.offset).copied()
    }

    fn advance(&mut self) {
        self.offset += 1;
    }

    fn next(&mut self) -> Option<u8> {
        let value = self.peek()?;
        self.advance();
        Some(value)
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(byte) if is_whitespace(byte)) {
            self.advance();
        }
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn is_or(&self) -> bool {
        is_or_starts_at(self.bytes, self.offset)
    }

    fn consume_or(&mut self) {
        self.offset += 2;
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_fts_query, FtsQueryTerm, FtsQueryTermKind, FTS_QUERY_ERROR_PREFIX};

    fn assert_query_error(query: &str) {
        let error = parse_fts_query(query).expect_err("query should fail");
        assert!(
            error.message.starts_with(FTS_QUERY_ERROR_PREFIX),
            "{error:?}"
        );
    }

    fn assert_query_term(value: &FtsQueryTerm, kind: FtsQueryTermKind, text: &str, excluded: bool) {
        assert_eq!(value.kind, kind);
        assert_eq!(value.text, text);
        assert_eq!(value.excluded, excluded);
    }

    #[test]
    fn parse_single_word() {
        let query = parse_fts_query("database").expect("valid query");
        assert_eq!(query.clauses.len(), 1);
        assert_query_term(
            &query.clauses[0][0],
            FtsQueryTermKind::Word,
            "database",
            false,
        );
    }

    #[test]
    fn parse_terms_and_quoted_phrase() {
        let query = parse_fts_query("\"embedded database\" systems").expect("valid query");
        assert_eq!(query.clauses.len(), 1);
        assert_eq!(query.clauses[0].len(), 2);
        assert_query_term(
            &query.clauses[0][0],
            FtsQueryTermKind::Phrase,
            "embedded database",
            false,
        );
        assert_query_term(
            &query.clauses[0][1],
            FtsQueryTermKind::Word,
            "systems",
            false,
        );
    }

    #[test]
    fn parse_or_query() {
        let query = parse_fts_query("database OR systems OR index").expect("valid query");
        assert_eq!(query.clauses.len(), 3);
        assert_eq!(query.clauses[0].len(), 1);
        assert_eq!(query.clauses[1].len(), 1);
        assert_eq!(query.clauses[2].len(), 1);
    }

    #[test]
    fn parse_exclusion() {
        let query = parse_fts_query("database -draft").expect("valid query");
        assert_eq!(query.clauses.len(), 1);
        assert_query_term(
            &query.clauses[0][0],
            FtsQueryTermKind::Word,
            "database",
            false,
        );
        assert_query_term(&query.clauses[0][1], FtsQueryTermKind::Word, "draft", true);
    }

    #[test]
    fn parse_prefix_term() {
        let query = parse_fts_query("dece*").expect("valid query");
        assert_query_term(
            &query.clauses[0][0],
            FtsQueryTermKind::Prefix,
            "dece",
            false,
        );
    }

    #[test]
    fn parse_escaped_spaceless_prefix_literal() {
        let query = parse_fts_query("hello \\\"world\\\"").expect("valid query");
        assert_query_term(&query.clauses[0][0], FtsQueryTermKind::Word, "hello", false);
        assert_query_term(
            &query.clauses[0][1],
            FtsQueryTermKind::Word,
            "\"world\"",
            false,
        );
        assert_eq!(query.clauses[0].len(), 2);
    }

    #[test]
    fn parse_escaped_leading_dash_literally() {
        let query = parse_fts_query("hello \\-world").expect("valid query");
        assert_query_term(
            &query.clauses[0][1],
            FtsQueryTermKind::Word,
            "-world",
            false,
        );
    }

    #[test]
    fn parse_escaped_backslash_outside_phrase() {
        let query = parse_fts_query("hello \\\\world").expect("valid query");
        assert_query_term(
            &query.clauses[0][1],
            FtsQueryTermKind::Word,
            "\\world",
            false,
        );
    }

    #[test]
    fn parse_escaped_quote_and_backslash_in_phrase() {
        let query = parse_fts_query("\"embedded \\\"database\\\"\"").expect("valid query");
        assert_query_term(
            &query.clauses[0][0],
            FtsQueryTermKind::Phrase,
            "embedded \"database\"",
            false,
        );
    }

    #[test]
    fn parse_phrase_is_not_unclosed() {
        let query = parse_fts_query("\"embedded").expect_err("unterminated phrase");
        assert!(query.message.starts_with(FTS_QUERY_ERROR_PREFIX));
    }

    #[test]
    fn reject_empty_query() {
        assert_query_error("");
    }

    #[test]
    fn reject_only_excluded_terms() {
        assert_query_error("-draft -\"old\"");
    }

    #[test]
    fn reject_trailing_or() {
        assert_query_error("database OR");
    }

    #[test]
    fn reject_double_or() {
        assert_query_error("a OR OR b");
    }

    #[test]
    fn reject_invalid_escape() {
        assert_query_error("hello\\x");
    }

    #[test]
    fn reject_trailing_backslash() {
        assert_query_error("hello \\");
    }

    #[test]
    fn reject_unescaped_quote_in_word() {
        assert_query_error("hello\"world");
    }

    #[test]
    fn parse_or_inside_word_as_plain_term() {
        let query = parse_fts_query("helloORworld").expect("valid query");
        assert_query_term(
            &query.clauses[0][0],
            FtsQueryTermKind::Word,
            "helloORworld",
            false,
        );
    }
}
