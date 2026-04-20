//! Public result rows and internal row-set buffers.

use std::sync::Arc;

use crate::record::value::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryRow {
    values: Vec<Value>,
}

impl QueryRow {
    #[must_use]
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    #[must_use]
    pub fn values(&self) -> &[Value] {
        &self.values
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<QueryRow>,
    affected_rows: u64,
    explain_lines: Vec<String>,
}

impl QueryResult {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            explain_lines: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_rows(columns: Vec<String>, rows: Vec<QueryRow>) -> Self {
        let affected_rows = rows.len() as u64;
        Self {
            columns,
            rows,
            affected_rows,
            explain_lines: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_affected_rows(affected_rows: u64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows,
            explain_lines: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_explain(lines: Vec<String>) -> Self {
        Self {
            columns: vec!["plan".to_string()],
            rows: lines
                .iter()
                .cloned()
                .map(|line| QueryRow::new(vec![Value::Text(line)]))
                .collect(),
            affected_rows: 0,
            explain_lines: lines,
        }
    }

    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    #[must_use]
    pub fn rows(&self) -> &[QueryRow] {
        &self.rows
    }

    #[must_use]
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    #[must_use]
    pub fn explain_lines(&self) -> &[String] {
        &self.explain_lines
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnBinding {
    pub(crate) table: Option<String>,
    pub(crate) name: String,
    pub(crate) hidden: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Dataset {
    pub(crate) columns: Vec<ColumnBinding>,
    pub(crate) rows: Arc<Vec<Vec<Value>>>,
}

impl Dataset {
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Arc::new(Vec::new()),
        }
    }

    #[must_use]
    pub(crate) fn with_rows(columns: Vec<ColumnBinding>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            columns,
            rows: Arc::new(rows),
        }
    }

    #[must_use]
    pub(crate) fn share_rows(&self, columns: Vec<ColumnBinding>) -> Self {
        Self {
            columns,
            rows: Arc::clone(&self.rows),
        }
    }

    pub(crate) fn rows_mut(&mut self) -> &mut Vec<Vec<Value>> {
        Arc::make_mut(&mut self.rows)
    }

    #[must_use]
    pub(crate) fn into_rows(self) -> Vec<Vec<Value>> {
        Arc::unwrap_or_clone(self.rows)
    }

    pub(crate) fn set_rows(&mut self, rows: Vec<Vec<Value>>) {
        self.rows = Arc::new(rows);
    }

    pub(crate) fn take_rows(&mut self) -> Vec<Vec<Value>> {
        Arc::unwrap_or_clone(std::mem::take(&mut self.rows))
    }
}

impl ColumnBinding {
    #[must_use]
    pub(crate) fn visible(table: Option<String>, name: String) -> Self {
        Self {
            table,
            name,
            hidden: false,
        }
    }

    #[must_use]
    pub(crate) fn as_output(&self) -> Self {
        Self {
            table: self.table.clone(),
            name: self.name.clone(),
            hidden: false,
        }
    }
}
