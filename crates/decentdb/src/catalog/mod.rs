//! Durable catalog metadata and lookup APIs.

pub(crate) mod ddl;
pub(crate) mod maintenance;
pub(crate) mod objects;
pub(crate) mod schema;

pub(crate) use objects::CatalogHandle;
pub(crate) use schema::{
    identifiers_equal, CatalogState, CheckConstraint, ColumnSchema, ColumnType, ForeignKeyAction,
    ForeignKeyConstraint, IndexColumn, IndexKind, IndexSchema, IndexStats, SchemaInfo, TableSchema,
    TableStats, TriggerEvent, TriggerKind, TriggerSchema, ViewSchema,
};
