//! Bulk-load helpers.

use crate::catalog::TriggerEvent;
use crate::error::{DbError, Result};
use crate::record::value::Value;

use super::{BulkLoadOptions, EngineRuntime, StoredRow};

impl EngineRuntime {
    pub(crate) fn bulk_load_rows(
        &mut self,
        table_name: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
        options: BulkLoadOptions,
        page_size: u32,
    ) -> Result<u64> {
        if options.batch_size == 0 || options.sync_interval == 0 {
            return Err(DbError::sql(
                "bulk-load batch_size and sync_interval must be greater than zero",
            ));
        }
        if self.catalog.view(table_name).is_some() {
            return Err(DbError::sql("bulk load targets must be base tables"));
        }

        let column_names = columns
            .iter()
            .map(|column| (*column).to_string())
            .collect::<Vec<_>>();
        let mut affected_rows = 0_u64;
        for source_row in rows {
            let candidate = {
                let mut staged_table = self
                    .catalog
                    .tables
                    .get(table_name)
                    .cloned()
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?;
                let candidate = super::dml::build_insert_row_values(
                    self,
                    &mut staged_table,
                    &column_names,
                    source_row.clone(),
                    &[],
                )?;
                self.catalog_mut()
                    .tables
                    .get_mut(table_name)
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?
                    .next_row_id = staged_table.next_row_id;
                candidate
            };
            self.validate_row(table_name, &candidate, None, &[])?;
            let row_id = super::dml::primary_row_id(
                self.catalog
                    .tables
                    .get(table_name)
                    .ok_or_else(|| DbError::sql(format!("unknown table {}", table_name)))?,
                &candidate,
            )
            .unwrap_or_else(|| super::dml::next_row_id(self, table_name));
            let stored_row = StoredRow {
                row_id,
                values: candidate,
            };
            self.append_stored_row_to_table_row_source(table_name, &stored_row, page_size)?;
            self.mark_table_row_appended(table_name, &stored_row.values);
            affected_rows += 1;
        }
        self.rebuild_indexes(page_size)?;
        self.execute_after_triggers(
            table_name,
            TriggerEvent::Insert,
            affected_rows as usize,
            page_size,
        )?;
        Ok(affected_rows)
    }
}
