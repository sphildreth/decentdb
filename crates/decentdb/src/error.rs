//! Structured engine error taxonomy.

use thiserror::Error;

/// Stable numeric error codes for the DecentDB engine.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DbErrorCode {
    Io = 1,
    Corruption = 2,
    Constraint = 3,
    Transaction = 4,
    Sql = 5,
    Internal = 6,
    Panic = 7,
}

impl DbErrorCode {
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self as u32
    }
}

/// Canonical engine result type.
pub type Result<T> = std::result::Result<T, DbError>;

/// Canonical engine error type.
#[derive(Debug, Error)]
pub enum DbError {
    #[error("I/O error: {context}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    #[error("database corruption: {message}")]
    Corruption { message: String },
    #[error("constraint violation: {message}")]
    Constraint { message: String },
    #[error("transaction error: {message}")]
    Transaction { message: String },
    #[error("SQL error: {message}")]
    Sql { message: String },
    #[error("internal engine error: {message}")]
    Internal { message: String },
    #[error("panic captured at boundary: {message}")]
    Panic { message: String },
}

impl DbError {
    #[must_use]
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    #[must_use]
    pub fn corruption(message: impl Into<String>) -> Self {
        Self::Corruption {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn constraint(message: impl Into<String>) -> Self {
        Self::Constraint {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn transaction(message: impl Into<String>) -> Self {
        Self::Transaction {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn sql(message: impl Into<String>) -> Self {
        Self::Sql {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn panic(message: impl Into<String>) -> Self {
        Self::Panic {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn code(&self) -> DbErrorCode {
        match self {
            Self::Io { .. } => DbErrorCode::Io,
            Self::Corruption { .. } => DbErrorCode::Corruption,
            Self::Constraint { .. } => DbErrorCode::Constraint,
            Self::Transaction { .. } => DbErrorCode::Transaction,
            Self::Sql { .. } => DbErrorCode::Sql,
            Self::Internal { .. } => DbErrorCode::Internal,
            Self::Panic { .. } => DbErrorCode::Panic,
        }
    }

    #[must_use]
    pub fn numeric_code(&self) -> u32 {
        self.code().as_u32()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;

    use super::{DbError, DbErrorCode};

    #[test]
    fn error_categories_map_to_stable_numeric_codes() {
        let cases = [
            (DbError::io("disk", Error::other("disk")), DbErrorCode::Io),
            (DbError::corruption("bad header"), DbErrorCode::Corruption),
            (
                DbError::constraint("duplicate key"),
                DbErrorCode::Constraint,
            ),
            (DbError::transaction("busy"), DbErrorCode::Transaction),
            (DbError::sql("syntax"), DbErrorCode::Sql),
            (DbError::internal("broken invariant"), DbErrorCode::Internal),
            (DbError::panic("panic payload"), DbErrorCode::Panic),
        ];

        for (error, expected_code) in cases {
            assert_eq!(error.code(), expected_code);
            assert_eq!(error.numeric_code(), expected_code.as_u32());
        }
    }
}
