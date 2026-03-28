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
    pub(crate) generated_stored: bool,
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
    pub(crate) include_columns: Vec<String>,
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
    pub(crate) schemas: BTreeMap<String, SchemaInfo>,
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
            schemas: BTreeMap::new(),
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
        self.schema(name).is_some()
            || self.table(name).is_some()
            || self.index(name).is_some()
            || self.view(name).is_some()
            || self.trigger(name).is_some()
    }

    #[must_use]
    pub(crate) fn schema(&self, name: &str) -> Option<&SchemaInfo> {
        map_get_ci(&self.schemas, name)
    }

    #[must_use]
    pub(crate) fn contains_non_schema_object(&self, name: &str) -> bool {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SchemaInfo {
    pub(crate) name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_type_as_str_returns_expected_values() {
        assert_eq!(ColumnType::Int64.as_str(), "INT64");
        assert_eq!(ColumnType::Float64.as_str(), "FLOAT64");
        assert_eq!(ColumnType::Text.as_str(), "TEXT");
        assert_eq!(ColumnType::Bool.as_str(), "BOOL");
        assert_eq!(ColumnType::Blob.as_str(), "BLOB");
        assert_eq!(ColumnType::Decimal.as_str(), "DECIMAL");
        assert_eq!(ColumnType::Uuid.as_str(), "UUID");
        assert_eq!(ColumnType::Timestamp.as_str(), "TIMESTAMP");
    }

    #[test]
    fn column_type_copy_and_debug() {
        let col_type = ColumnType::Int64;
        let copied = col_type;
        assert_eq!(copied, ColumnType::Int64);
    }

    #[test]
    fn index_kind_copy_and_debug() {
        let kind = IndexKind::Btree;
        let copied = kind;
        assert_eq!(copied, IndexKind::Btree);

        let kind = IndexKind::Trigram;
        let copied = kind;
        assert_eq!(copied, IndexKind::Trigram);
    }

    #[test]
    fn trigger_kind_copy_and_debug() {
        let kind = TriggerKind::After;
        let copied = kind;
        assert_eq!(copied, TriggerKind::After);

        let kind = TriggerKind::InsteadOf;
        let copied = kind;
        assert_eq!(copied, TriggerKind::InsteadOf);
    }

    #[test]
    fn trigger_event_copy_and_debug() {
        let events = [TriggerEvent::Insert, TriggerEvent::Update, TriggerEvent::Delete];
        for event in events {
            let copied = event;
            assert_eq!(copied, event);
        }
    }

    #[test]
    fn table_stats_copy_and_debug() {
        let stats = TableStats { row_count: 100 };
        let copied = stats;
        assert_eq!(copied.row_count, 100);
    }

    #[test]
    fn index_stats_copy_and_debug() {
        let stats = IndexStats {
            entry_count: 500,
            distinct_key_count: 100,
        };
        let copied = stats;
        assert_eq!(copied.entry_count, 500);
        assert_eq!(copied.distinct_key_count, 100);
    }

    #[test]
    fn foreign_key_action_copy_and_debug() {
        let actions = [
            ForeignKeyAction::NoAction,
            ForeignKeyAction::Restrict,
            ForeignKeyAction::Cascade,
            ForeignKeyAction::SetNull,
        ];
        for action in actions {
            let copied = action;
            assert_eq!(copied, action);
        }
    }

    #[test]
    fn catalog_state_empty_has_cookie() {
        let catalog = CatalogState::empty(42);
        assert_eq!(catalog.schema_cookie, 42);
        assert!(catalog.schemas.is_empty());
        assert!(catalog.tables.is_empty());
        assert!(catalog.indexes.is_empty());
        assert!(catalog.views.is_empty());
        assert!(catalog.triggers.is_empty());
        assert!(catalog.table_stats.is_empty());
        assert!(catalog.index_stats.is_empty());
    }

    #[test]
    fn catalog_state_contains_object_when_empty() {
        let catalog = CatalogState::empty(0);
        assert!(!catalog.contains_object("anything"));
        assert!(!catalog.contains_non_schema_object("anything"));
    }

    #[test]
    fn catalog_state_lookups_return_none_when_empty() {
        let catalog = CatalogState::empty(0);
        assert!(catalog.schema("test").is_none());
        assert!(catalog.table("test").is_none());
        assert!(catalog.index("test").is_none());
        assert!(catalog.view("test").is_none());
        assert!(catalog.trigger("test").is_none());
    }

    #[test]
    fn identifiers_equal_is_case_insensitive() {
        assert!(identifiers_equal("foo", "foo"));
        assert!(identifiers_equal("Foo", "foo"));
        assert!(identifiers_equal("FOO", "foo"));
        assert!(identifiers_equal("FooBar", "foobar"));
        assert!(!identifiers_equal("foo", "bar"));
    }

    #[test]
    fn check_constraint_debug_and_copy() {
        let check = CheckConstraint {
            name: Some("chk_test".to_string()),
            expression_sql: "value > 0".to_string(),
        };
        let copied = check.clone();
        assert_eq!(copied.name, Some("chk_test".to_string()));
        assert_eq!(copied.expression_sql, "value > 0");
    }

    #[test]
    fn foreign_key_constraint_debug_and_copy() {
        let fk = ForeignKeyConstraint {
            name: Some("fk_test".to_string()),
            columns: vec!["col1".to_string()],
            referenced_table: "other_table".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::Cascade,
            on_update: ForeignKeyAction::NoAction,
        };
        let copied = fk.clone();
        assert_eq!(copied.name, Some("fk_test".to_string()));
        assert_eq!(copied.columns.len(), 1);
        assert_eq!(copied.on_delete, ForeignKeyAction::Cascade);
        assert_eq!(copied.on_update, ForeignKeyAction::NoAction);
    }

    #[test]
    fn column_schema_debug_and_copy() {
        let col = ColumnSchema {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
            nullable: false,
            default_sql: None,
            generated_sql: None,
            generated_stored: false,
            primary_key: true,
            unique: true,
            auto_increment: true,
            checks: vec![],
            foreign_key: None,
        };
        let copied = col.clone();
        assert_eq!(copied.name, "id");
        assert_eq!(copied.column_type, ColumnType::Int64);
        assert!(!copied.nullable);
        assert!(copied.primary_key);
    }

    #[test]
    fn index_schema_debug_and_copy() {
        let index = IndexSchema {
            name: "idx_test".to_string(),
            table_name: "users".to_string(),
            kind: IndexKind::Btree,
            unique: false,
            columns: vec![IndexColumn {
                column_name: Some("name".to_string()),
                expression_sql: None,
            }],
            include_columns: vec![],
            predicate_sql: None,
            fresh: true,
        };
        let copied = index.clone();
        assert_eq!(copied.name, "idx_test");
        assert_eq!(copied.kind, IndexKind::Btree);
        assert!(copied.fresh);
    }

    #[test]
    fn table_schema_debug_and_copy() {
        let table = TableSchema {
            name: "users".to_string(),
            temporary: false,
            columns: vec![],
            checks: vec![],
            foreign_keys: vec![],
            primary_key_columns: vec!["id".to_string()],
            next_row_id: 1,
        };
        let copied = table.clone();
        assert_eq!(copied.name, "users");
        assert!(!copied.temporary);
        assert_eq!(copied.next_row_id, 1);
    }

    #[test]
    fn view_schema_debug_and_copy() {
        let view = ViewSchema {
            name: "v_users".to_string(),
            temporary: false,
            sql_text: "SELECT * FROM users".to_string(),
            column_names: vec!["id".to_string()],
            dependencies: vec!["users".to_string()],
        };
        let copied = view.clone();
        assert_eq!(copied.name, "v_users");
        assert_eq!(copied.sql_text, "SELECT * FROM users");
    }

    #[test]
    fn trigger_schema_debug_and_copy() {
        let trigger = TriggerSchema {
            name: "trg_insert".to_string(),
            target_name: "users".to_string(),
            kind: TriggerKind::After,
            event: TriggerEvent::Insert,
            on_view: false,
            action_sql: "INSERT INTO log VALUES (1)".to_string(),
        };
        let copied = trigger.clone();
        assert_eq!(copied.name, "trg_insert");
        assert_eq!(copied.kind, TriggerKind::After);
        assert_eq!(copied.event, TriggerEvent::Insert);
    }

    #[test]
    fn index_column_debug_and_copy() {
        let col = IndexColumn {
            column_name: Some("name".to_string()),
            expression_sql: Some("LOWER(name)".to_string()),
        };
        let copied = col.clone();
        assert_eq!(copied.column_name, Some("name".to_string()));
        assert_eq!(copied.expression_sql, Some("LOWER(name)".to_string()));
    }
}
