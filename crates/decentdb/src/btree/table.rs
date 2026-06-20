//! Typed table-row wrapper over the generic B+Tree storage primitives.

use std::collections::BTreeMap;

use crate::btree::cursor::BtreeCursor;
use crate::btree::page::decode_page;
use crate::btree::page::BtreePage;
use crate::btree::read::find_exact;
use crate::btree::write::Btree;
use crate::error::Result;
use crate::record::compression::CompressionMode;
use crate::record::overflow::free_overflow;
use crate::record::row::{Row, RowOverflowOptions};
use crate::record::value::Value;
use crate::storage::page::{InMemoryPageStore, PageId, PageStore};

const SIGNED_ROW_ID_BIAS: u64 = 0x8000_0000_0000_0000;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TableRow {
    pub(crate) row_id: i64,
    pub(crate) values: Vec<Value>,
}

#[derive(Clone, Debug)]
pub(crate) struct TableBtree<S: PageStore> {
    tree: Btree<S>,
}

#[derive(Debug)]
pub(crate) struct TableBtreeCursor<'a, S: PageStore> {
    inner: BtreeCursor<'a, S>,
}

#[derive(Debug)]
pub(crate) struct TableBtreeView<'a, S: PageStore> {
    store: &'a S,
    root_page_id: Option<PageId>,
}

impl<S: PageStore> TableBtree<S> {
    pub(crate) fn new(store: S) -> Self {
        Self {
            tree: Btree::new(store),
        }
    }

    #[must_use]
    pub(crate) fn root_page_id(&self) -> Option<PageId> {
        self.tree.root_page_id()
    }

    #[must_use]
    pub(crate) fn page_size(&self) -> u32 {
        self.tree.page_size()
    }

    #[must_use]
    pub(crate) fn store(&self) -> &S {
        self.tree.store()
    }

    pub(crate) fn insert_row(
        &mut self,
        row_id: i64,
        values: Vec<Value>,
    ) -> Result<Option<TableRow>> {
        let key = encode_row_id_key(row_id);
        let payload = Row::new(values).encode_with_overflow::<S>(
            None,
            RowOverflowOptions {
                inline_threshold: usize::MAX,
                compression: CompressionMode::Never,
            },
        )?;
        self.tree
            .insert(key, payload)?
            .map(|previous| decode_table_row(row_id, previous))
            .transpose()
    }

    pub(crate) fn replace_rows(&mut self, rows: &[TableRow]) -> Result<()> {
        let entries = rows
            .iter()
            .map(|row| {
                Ok((
                    encode_row_id_key(row.row_id),
                    encode_row_payload::<S>(row.values.clone())?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        self.tree.replace_entries(entries)
    }

    pub(crate) fn get_row(&self, row_id: i64) -> Result<Option<TableRow>> {
        let key = encode_row_id_key(row_id);
        self.tree
            .get(key)?
            .map(|payload| decode_table_row(row_id, payload))
            .transpose()
    }

    pub(crate) fn delete_row(&mut self, row_id: i64) -> Result<Option<TableRow>> {
        let key = encode_row_id_key(row_id);
        self.tree
            .delete(key)?
            .map(|payload| decode_table_row(row_id, payload))
            .transpose()
    }

    pub(crate) fn cursor_from_start(&self) -> Result<TableBtreeCursor<'_, S>> {
        Ok(TableBtreeCursor {
            inner: self.tree.cursor_from_start()?,
        })
    }

    pub(crate) fn cursor_from_end(&self) -> Result<TableBtreeCursor<'_, S>> {
        Ok(TableBtreeCursor {
            inner: self.tree.cursor_from_end()?,
        })
    }

    pub(crate) fn cursor_seek_forward(&self, row_id: i64) -> Result<TableBtreeCursor<'_, S>> {
        Ok(TableBtreeCursor {
            inner: self.tree.cursor_seek_forward(encode_row_id_key(row_id))?,
        })
    }

    pub(crate) fn cursor_seek_backward(&self, row_id: i64) -> Result<TableBtreeCursor<'_, S>> {
        Ok(TableBtreeCursor {
            inner: self.tree.cursor_seek_backward(encode_row_id_key(row_id))?,
        })
    }

    pub(crate) fn into_parts(self) -> (S, Option<PageId>) {
        self.tree.into_parts()
    }
}

impl TableBtree<InMemoryPageStore> {
    pub(crate) fn with_page_size(page_size: u32) -> Self {
        Self::new(InMemoryPageStore::new(page_size))
    }
}

impl<'a, S: PageStore> TableBtreeCursor<'a, S> {
    pub(crate) fn next(&mut self) -> Result<Option<TableRow>> {
        self.inner
            .next()?
            .map(|(key, payload)| decode_table_row(decode_row_id_key(key), payload))
            .transpose()
    }

    pub(crate) fn prev(&mut self) -> Result<Option<TableRow>> {
        self.inner
            .prev()?
            .map(|(key, payload)| decode_table_row(decode_row_id_key(key), payload))
            .transpose()
    }
}

impl<'a, S: PageStore> TableBtreeView<'a, S> {
    pub(crate) fn new(store: &'a S, root_page_id: Option<PageId>) -> Self {
        Self {
            store,
            root_page_id,
        }
    }

    pub(crate) fn root_page_id(&self) -> Option<PageId> {
        self.root_page_id
    }

    pub(crate) fn get_row(&self, row_id: i64) -> Result<Option<TableRow>> {
        find_exact(self.store, self.root_page_id, encode_row_id_key(row_id))?
            .map(|payload| decode_table_row(row_id, payload))
            .transpose()
    }

    pub(crate) fn cursor_from_start(&self) -> Result<TableBtreeCursor<'_, S>> {
        Ok(TableBtreeCursor {
            inner: BtreeCursor::from_start(self.store, self.root_page_id)?,
        })
    }

    pub(crate) fn cursor_seek_forward(&self, row_id: i64) -> Result<TableBtreeCursor<'_, S>> {
        Ok(TableBtreeCursor {
            inner: BtreeCursor::seek_forward(
                self.store,
                self.root_page_id,
                encode_row_id_key(row_id),
            )?,
        })
    }
}

pub(crate) fn free_table_btree<S: PageStore>(
    store: &mut S,
    root_page_id: Option<PageId>,
) -> Result<()> {
    let Some(root_page_id) = root_page_id else {
        return Ok(());
    };
    let mut stack = vec![root_page_id];
    while let Some(page_id) = stack.pop() {
        let page = decode_page(&store.read_page(page_id)?)?;
        match page {
            BtreePage::Leaf(leaf) => {
                for cell in leaf.cells {
                    if let Some(overflow_page_id) = cell.overflow_page_id {
                        free_overflow(store, overflow_page_id)?;
                    }
                }
            }
            BtreePage::Internal(internal) => {
                stack.push(internal.right_child);
                for cell in internal.cells {
                    stack.push(cell.child);
                }
            }
        }
        store.free_page(page_id)?;
    }
    Ok(())
}

fn encode_row_id_key(row_id: i64) -> u64 {
    (row_id as u64) ^ SIGNED_ROW_ID_BIAS
}

fn decode_row_id_key(key: u64) -> i64 {
    (key ^ SIGNED_ROW_ID_BIAS) as i64
}

fn decode_table_row(row_id: i64, payload: Vec<u8>) -> Result<TableRow> {
    let row = Row::decode(&payload)?;
    Ok(TableRow {
        row_id,
        values: row.values().to_vec(),
    })
}

fn encode_row_payload<S: PageStore>(values: Vec<Value>) -> Result<Vec<u8>> {
    Row::new(values).encode_with_overflow::<S>(
        None,
        RowOverflowOptions {
            inline_threshold: usize::MAX,
            compression: CompressionMode::Never,
        },
    )
}

#[cfg(test)]
mod tests {
    use crate::record::value::Value;

    use super::{free_table_btree, TableBtree, TableBtreeView, TableRow};

    fn shuffle_in_place(values: &mut [i64], seed: &mut u64) {
        if values.is_empty() {
            return;
        }

        let mut current = *seed;
        for i in (1..values.len()).rev() {
            current ^= current << 7;
            current ^= current >> 9;
            current ^= current << 8;
            let swap = (current as usize) % (i + 1);
            values.swap(i, swap);
        }

        *seed = current;
    }

    fn collect_forward(
        tree: &TableBtree<crate::storage::page::InMemoryPageStore>,
    ) -> Vec<TableRow> {
        let mut cursor = tree.cursor_from_start().expect("cursor");
        let mut rows = Vec::new();
        while let Some(row) = cursor.next().expect("next") {
            rows.push(row);
        }
        rows
    }

    #[test]
    fn signed_row_ids_roundtrip_in_sorted_order() {
        let mut tree = TableBtree::with_page_size(512);
        for row_id in [0_i64, -5, 9, -2, 7] {
            tree.insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("insert row");
        }

        let row_ids = collect_forward(&tree)
            .into_iter()
            .map(|row| row.row_id)
            .collect::<Vec<_>>();
        assert_eq!(row_ids, vec![-5, -2, 0, 7, 9]);
    }

    #[test]
    fn insert_get_delete_roundtrip_rows() {
        let mut tree = TableBtree::with_page_size(4096);
        tree.insert_row(42, vec![Value::Int64(42), Value::Text("Ada".to_string())])
            .expect("insert row");

        let row = tree.get_row(42).expect("get row").expect("row exists");
        assert_eq!(
            row,
            TableRow {
                row_id: 42,
                values: vec![Value::Int64(42), Value::Text("Ada".to_string())],
            }
        );

        let deleted = tree
            .delete_row(42)
            .expect("delete row")
            .expect("deleted row");
        assert_eq!(deleted, row);
        assert_eq!(tree.get_row(42).expect("get deleted row"), None);
    }

    #[test]
    fn large_rows_roundtrip_via_btree_overflow() {
        let mut tree = TableBtree::with_page_size(512);
        let large = "x".repeat(8_192);
        tree.insert_row(7, vec![Value::Text(large.clone())])
            .expect("insert large row");

        let row = tree.get_row(7).expect("get row").expect("row exists");
        assert_eq!(row.values, vec![Value::Text(large)]);
    }

    #[test]
    fn replace_rows_and_view_roundtrip_existing_root() {
        let rows = vec![
            TableRow {
                row_id: -3,
                values: vec![Value::Int64(-3), Value::Text("left".to_string())],
            },
            TableRow {
                row_id: 9,
                values: vec![Value::Int64(9), Value::Text("right".to_string())],
            },
        ];
        let mut tree = TableBtree::with_page_size(512);
        tree.replace_rows(&rows).expect("bulk replace");
        let root_page_id = tree.root_page_id();
        let view = TableBtreeView::new(tree.store(), root_page_id);

        let loaded = view.get_row(-3).expect("get row").expect("row exists");
        assert_eq!(loaded, rows[0]);

        let visible_rows = {
            let mut cursor = view.cursor_from_start().expect("cursor");
            let mut visible = Vec::new();
            while let Some(row) = cursor.next().expect("next") {
                visible.push(row);
            }
            visible
        };
        assert_eq!(visible_rows, rows);
    }

    #[test]
    fn free_table_btree_releases_pages() {
        let mut tree = TableBtree::with_page_size(512);
        tree.insert_row(1, vec![Value::Text("a".repeat(4_096))])
            .expect("insert row");
        tree.insert_row(2, vec![Value::Text("b".repeat(4_096))])
            .expect("insert row");

        let (mut store, root_page_id) = tree.into_parts();
        assert!(store.allocated_page_count() > 0);
        free_table_btree(&mut store, root_page_id).expect("free tree");
        assert_eq!(store.allocated_page_count(), 0);
    }

    #[test]
    fn free_table_btree_releases_pages_after_many_small_rows() {
        let mut tree = TableBtree::with_page_size(512);
        for row_id in 0..100_i64 {
            tree.insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("insert row");
        }

        let (mut store, root_page_id) = tree.into_parts();
        assert!(store.allocated_page_count() > 1);
        free_table_btree(&mut store, root_page_id).expect("free tree");
        assert_eq!(store.allocated_page_count(), 0);
    }

    #[test]
    fn cursor_navigation_and_seek_operations_follow_row_order() {
        let mut tree = TableBtree::with_page_size(512);
        for row_id in [5_i64, 1, 9, 3, 7] {
            tree.insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("insert row");
        }

        let mut forward = tree.cursor_from_start().expect("cursor");
        let mut forward_rows = Vec::new();
        while let Some(row) = forward.next().expect("next") {
            forward_rows.push(row.row_id);
        }
        assert_eq!(forward_rows, vec![1, 3, 5, 7, 9]);

        let mut backward = tree.cursor_from_end().expect("cursor");
        let mut backward_rows = Vec::new();
        while let Some(row) = backward.prev().expect("prev") {
            backward_rows.push(row.row_id);
        }
        assert_eq!(backward_rows, vec![9, 7, 5, 3, 1]);

        let mut seek_forward = tree.cursor_seek_forward(4).expect("cursor");
        let row = seek_forward.next().expect("next").expect("row");
        assert_eq!(row.row_id, 5);
        assert_eq!(row.values, vec![Value::Int64(5)]);

        let mut seek_past_end = tree.cursor_seek_forward(10).expect("cursor");
        assert!(seek_past_end.next().expect("next").is_none());

        let mut seek_backward = tree.cursor_seek_backward(6).expect("cursor");
        let row = seek_backward.prev().expect("prev").expect("row");
        assert_eq!(row.row_id, 5);
        assert_eq!(row.values, vec![Value::Int64(5)]);

        let mut seek_before_start = tree.cursor_seek_backward(0).expect("cursor");
        assert!(seek_before_start.prev().expect("prev").is_none());
    }

    #[test]
    fn sequential_and_reverse_inserts_are_sorted_in_cursor_order() {
        let mut forward = TableBtree::with_page_size(256);
        for row_id in 0_i64..64 {
            forward
                .insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("insert row");
        }

        assert_eq!(
            collect_forward(&forward)
                .iter()
                .map(|row| row.row_id)
                .collect::<Vec<_>>(),
            (0_i64..64).collect::<Vec<_>>()
        );

        let mut reverse = TableBtree::with_page_size(256);
        for row_id in (0_i64..64).rev() {
            reverse
                .insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("insert row");
        }

        assert_eq!(
            collect_forward(&reverse)
                .iter()
                .map(|row| row.row_id)
                .collect::<Vec<_>>(),
            (0_i64..64).collect::<Vec<_>>()
        );
    }

    #[test]
    fn deterministic_random_order_inserts_use_seed_a11fa11e_beef_cafe() {
        let mut row_ids: Vec<i64> = (0_i64..400).map(|value| value * 2 - 400).collect();
        let expected = {
            let mut expected = row_ids.clone();
            expected.sort_unstable();
            expected
        };

        let mut seed = 0xA11F_A11E_BEEF_CAFE_u64;
        shuffle_in_place(&mut row_ids, &mut seed);

        let mut tree = TableBtree::with_page_size(256);
        for row_id in row_ids {
            tree.insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("insert row");
        }

        let collected = collect_forward(&tree)
            .into_iter()
            .map(|row| row.row_id)
            .collect::<Vec<_>>();
        assert_eq!(collected, expected);
    }

    #[test]
    fn duplicate_row_id_returns_previous_row_payload() {
        let mut tree = TableBtree::with_page_size(128);
        let prior = tree
            .insert_row(42, vec![Value::Text("first".to_string())])
            .expect("insert first row");
        assert!(prior.is_none());

        let prior = tree
            .insert_row(42, vec![Value::Text("second".to_string())])
            .expect("replace row");
        assert_eq!(
            prior.expect("replacement should return previous row"),
            TableRow {
                row_id: 42,
                values: vec![Value::Text("first".to_string())],
            }
        );

        let current = tree.get_row(42).expect("row exists").expect("read row");
        assert_eq!(
            current,
            TableRow {
                row_id: 42,
                values: vec![Value::Text("second".to_string())],
            }
        );
    }

    #[test]
    fn seek_boundary_positions_are_inclusive_and_follow_expected_next_slot() {
        let mut tree = TableBtree::with_page_size(128);
        for row_id in [-10_i64, -2, 0, 4, 5, 8, 11] {
            tree.insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("insert row");
        }

        let mut at_or_after_five = tree.cursor_seek_forward(5).expect("cursor forward");
        assert_eq!(at_or_after_five.next().expect("next").unwrap().row_id, 5);

        let mut after_five = tree
            .cursor_seek_forward(6)
            .expect("cursor forward exclusive equivalent");
        assert_eq!(after_five.next().expect("next").unwrap().row_id, 8);

        let mut at_or_before_five = tree.cursor_seek_backward(5).expect("cursor backward");
        assert_eq!(at_or_before_five.prev().expect("prev").unwrap().row_id, 5);

        let mut before_or_at_zero = tree.cursor_seek_backward(1).expect("cursor backward");
        assert_eq!(before_or_at_zero.prev().expect("prev").unwrap().row_id, 0);
    }

    #[test]
    fn delete_then_reinsert_preserves_lookup_and_ordering() {
        let mut tree = TableBtree::with_page_size(256);
        for row_id in 1_i64..40 {
            tree.insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("insert row");
        }

        tree.delete_row(10)
            .expect("delete row")
            .expect("deleted row");
        tree.delete_row(11)
            .expect("delete row")
            .expect("deleted row");

        tree.insert_row(10, vec![Value::Int64(500)])
            .expect("reinsert row");
        tree.insert_row(11, vec![Value::Int64(501)])
            .expect("reinsert row");

        let values = collect_forward(&tree)
            .into_iter()
            .map(|row| (row.row_id, row.values[0].clone()))
            .collect::<Vec<_>>();

        assert_eq!(values[8], (9_i64, Value::Int64(9)));
        assert_eq!(values[9], (10_i64, Value::Int64(500)));
        assert_eq!(values[10], (11_i64, Value::Int64(501)));
        assert_eq!(values[11], (12_i64, Value::Int64(12)));

        let lookup = tree.get_row(10).expect("get row").expect("row exists");
        assert_eq!(lookup.values[0], Value::Int64(500));
    }

    #[test]
    fn split_and_reopen_preserve_sorted_rows_across_page_boundaries() {
        let mut tree = TableBtree::with_page_size(128);
        for row_id in 0_i64..1024 {
            tree.insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("insert row");
        }

        for row_id in (200_i64..900_i64).step_by(3) {
            tree.delete_row(row_id).expect("delete row");
        }

        for row_id in (200_i64..900_i64).step_by(3) {
            tree.insert_row(row_id, vec![Value::Int64(row_id)])
                .expect("reinsert row");
        }

        let expected = (0_i64..1024).collect::<Vec<_>>();
        let collected = collect_forward(&tree)
            .into_iter()
            .map(|row| row.row_id)
            .collect::<Vec<_>>();

        let (store, root_page_id) = tree.into_parts();
        assert!(store.allocated_page_count() > 1);
        let view = TableBtreeView::new(&store, root_page_id);

        let reopened = {
            let mut cursor = view.cursor_from_start().expect("cursor");
            let mut rows = Vec::new();
            while let Some(row) = cursor.next().expect("next") {
                rows.push(row.row_id);
            }
            rows
        };

        assert_eq!(collected, expected);
        assert_eq!(reopened, expected);
    }
}
