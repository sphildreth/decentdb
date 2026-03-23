//! Pinned internal wrapper around the PostgreSQL-compatible SQL parser.

pub use pg_query::protobuf;
pub use pg_query::protobuf::node::Node as NodeEnum;

/// Result alias for parser operations.
pub type Result<T> = std::result::Result<T, ParserError>;

/// Deterministic parser error used by the engine wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParserError {
    message: String,
}

impl ParserError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParserError {}

/// Parses a single SQL statement into the libpg_query protobuf tree.
pub fn parse_statement(sql: &str) -> Result<protobuf::ParseResult> {
    let parsed = pg_query::parse(sql).map_err(|error| ParserError::new(error.to_string()))?;
    if parsed.protobuf.stmts.len() != 1 {
        return Err(ParserError::new(
            "expected exactly one SQL statement after parser split",
        ));
    }
    Ok(parsed.protobuf)
}

/// Splits a SQL batch into individual statements using the parser.
pub fn split_statements(sql: &str) -> Result<Vec<String>> {
    pg_query::split_with_parser(sql)
        .map(|parts| parts.into_iter().map(ToOwned::to_owned).collect())
        .map_err(|error| ParserError::new(error.to_string()))
}
