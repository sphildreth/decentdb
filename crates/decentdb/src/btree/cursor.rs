//! Forward and backward B+Tree cursor traversal.

use crate::btree::read::{
    first_position, greatest_less_than, last_position, materialize_current, seek_backward,
    seek_forward, CursorPosition,
};
use crate::error::Result;
use crate::storage::page::{PageId, PageStore};

#[derive(Debug)]
pub(crate) struct BtreeCursor<'a, S: PageStore> {
    store: &'a S,
    root_page_id: Option<PageId>,
    position: Option<CursorPosition>,
}

impl<'a, S: PageStore> BtreeCursor<'a, S> {
    pub(crate) fn from_start(store: &'a S, root_page_id: Option<PageId>) -> Result<Self> {
        Ok(Self {
            store,
            root_page_id,
            position: first_position(store, root_page_id)?,
        })
    }

    pub(crate) fn from_end(store: &'a S, root_page_id: Option<PageId>) -> Result<Self> {
        Ok(Self {
            store,
            root_page_id,
            position: last_position(store, root_page_id)?,
        })
    }

    pub(crate) fn seek_forward(
        store: &'a S,
        root_page_id: Option<PageId>,
        key: u64,
    ) -> Result<Self> {
        Ok(Self {
            store,
            root_page_id,
            position: seek_forward(store, root_page_id, key)?,
        })
    }

    pub(crate) fn seek_backward(
        store: &'a S,
        root_page_id: Option<PageId>,
        key: u64,
    ) -> Result<Self> {
        Ok(Self {
            store,
            root_page_id,
            position: seek_backward(store, root_page_id, key)?,
        })
    }

    pub(crate) fn next(&mut self) -> Result<Option<(u64, Vec<u8>)>> {
        let Some(current) = self.position.clone() else {
            return Ok(None);
        };
        let item = materialize_current(self.store, &current)?;

        if current.index + 1 < current.leaf.cells.len() {
            self.position = Some(CursorPosition {
                index: current.index + 1,
                ..current
            });
        } else if current.leaf.next_leaf != 0 {
            self.position = first_position(self.store, Some(current.leaf.next_leaf))?;
        } else {
            self.position = None;
        }

        Ok(Some(item))
    }

    pub(crate) fn prev(&mut self) -> Result<Option<(u64, Vec<u8>)>> {
        let Some(current) = self.position.clone() else {
            return Ok(None);
        };
        let item = materialize_current(self.store, &current)?;
        let current_key = item.0;

        if current.index > 0 {
            self.position = Some(CursorPosition {
                index: current.index - 1,
                ..current
            });
        } else {
            self.position = greatest_less_than(self.store, self.root_page_id, current_key)?;
        }

        Ok(Some(item))
    }
}
