//! Canonical catalog metadata for tables, indexes, views, and triggers.

use std::collections::BTreeMap;

#[must_use]
pub(crate) fn identifiers_equal(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn map_get_ci<'a, V>(map: &'a BTreeMap<String, V>, name: &str) -> Option<&'a V> {
    map.get(name).or_else(|| {
        map.iter()
            .find(|(entry_name, _)| identifiers_equal(entry_name, name))
            .map(|(_, value)| value)
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnType {
    Int64,
    Float64,
    Text,
    Bool,
    Blob,
    Decimal,
    Uuid,
    Timestamp,
}

impl ColumnType {
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Int64 => "INT64",
            Self::Float64 => "FLOAT64",
            Self::Text => "TEXT",
            Self::Bool => "BOOL",
            Self::Blob => "BLOB",
            Self::Decimal => "DECIMAL",
            Self::Uuid => "UUID",
            Self::Timestamp => "TIMESTAMP",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckConstraint {
    pub(crate) name: Option<String>,
    pub(crate) expression_sql: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForeignKeyConstraint {
    pub(crate) name: Option<String>,
    pub(crate) columns: Vec<String>,
    pub(crate) referenced_table: String,
    pub(crate) referenced_columns: Vec<String>,
    pub(crate) on_delete: ForeignKeyAction,
    pub(crate) on_update: ForeignKeyAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnSchema {
    pub(crate) name: String,
    pub(crate) column_type: ColumnType,
    pub(crate) nullable: bool,
    pub(crate) default_sql: Option<String>,
    pub(crate) generated_sql: Option<String>,
    pub(crate) primary_key: bool,
    pub(crate) unique: bool,
    pub(crate) auto_increment: bool,
    pub(crate) checks: Vec<CheckConstraint>,
    pub(crate) foreign_key: Option<ForeignKeyConstraint>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexKind {
    Btree,
    Trigram,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexColumn {
    pub(crate) column_name: Option<String>,
    pub(crate) expression_sql: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexSchema {
    pub(crate) name: String,
    pub(crate) table_name: String,
    pub(crate) kind: IndexKind,
    pub(crate) unique: bool,
    pub(crate) columns: Vec<IndexColumn>,
    pub(crate) predicate_sql: Option<String>,
    pub(crate) fresh: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableSchema {
    pub(crate) name: String,
    pub(crate) temporary: bool,
    pub(crate) columns: Vec<ColumnSchema>,
    pub(crate) checks: Vec<CheckConstraint>,
    pub(crate) foreign_keys: Vec<ForeignKeyConstraint>,
    pub(crate) primary_key_columns: Vec<String>,
    pub(crate) next_row_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ViewSchema {
    pub(crate) name: String,
    pub(crate) temporary: bool,
    pub(crate) sql_text: String,
    pub(crate) column_names: Vec<String>,
    pub(crate) dependencies: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TriggerKind {
    After,
    InsteadOf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TriggerSchema {
    pub(crate) name: String,
    pub(crate) target_name: String,
    pub(crate) kind: TriggerKind,
    pub(crate) event: TriggerEvent,
    pub(crate) on_view: bool,
    pub(crate) action_sql: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TableStats {
    pub(crate) row_count: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexStats {
    pub(crate) entry_count: i64,
    pub(crate) distinct_key_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogState {
    pub(crate) schema_cookie: u32,
    pub(crate) tables: BTreeMap<String, TableSchema>,
    pub(crate) indexes: BTreeMap<String, IndexSchema>,
    pub(crate) views: BTreeMap<String, ViewSchema>,
    pub(crate) triggers: BTreeMap<String, TriggerSchema>,
    pub(crate) table_stats: BTreeMap<String, TableStats>,
    pub(crate) index_stats: BTreeMap<String, IndexStats>,
}

impl CatalogState {
    #[must_use]
    pub(crate) fn empty(schema_cookie: u32) -> Self {
        Self {
            schema_cookie,
            tables: BTreeMap::new(),
            indexes: BTreeMap::new(),
            views: BTreeMap::new(),
            triggers: BTreeMap::new(),
            table_stats: BTreeMap::new(),
            index_stats: BTreeMap::new(),
        }
    }

    #[must_use]
    pub(crate) fn contains_object(&self, name: &str) -> bool {
        self.table(name).is_some()
            || self.index(name).is_some()
            || self.view(name).is_some()
            || self.trigger(name).is_some()
    }

    #[must_use]
    pub(crate) fn table(&self, name: &str) -> Option<&TableSchema> {
        map_get_ci(&self.tables, name)
    }

    #[must_use]
    pub(crate) fn index(&self, name: &str) -> Option<&IndexSchema> {
        map_get_ci(&self.indexes, name)
    }

    #[must_use]
    pub(crate) fn view(&self, name: &str) -> Option<&ViewSchema> {
        map_get_ci(&self.views, name)
    }

    #[must_use]
    pub(crate) fn trigger(&self, name: &str) -> Option<&TriggerSchema> {
        map_get_ci(&self.triggers, name)
    }
}
