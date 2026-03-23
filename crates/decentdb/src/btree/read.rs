//! B+Tree search helpers.

use crate::btree::page::{decode_page, BtreePage, LeafPage};
use crate::error::{DbError, Result};
use crate::record::overflow::read_chain;
use crate::storage::page::{PageId, PageStore};

#[derive(Clone, Debug)]
pub(crate) struct CursorPosition {
    pub(crate) leaf: LeafPage,
    pub(crate) index: usize,
}

pub(crate) fn find_exact<S: PageStore>(
    store: &S,
    root_page_id: Option<PageId>,
    key: u64,
) -> Result<Option<Vec<u8>>> {
    let Some(position) = seek_forward(store, root_page_id, key)? else {
        return Ok(None);
    };
    let cell = position
        .leaf
        .cells
        .get(position.index)
        .ok_or_else(|| DbError::internal("cursor position points outside leaf"))?;
    if cell.key != key {
        return Ok(None);
    }
    materialize_leaf_value(store, cell).map(Some)
}

pub(crate) fn seek_forward<S: PageStore>(
    store: &S,
    root_page_id: Option<PageId>,
    key: u64,
) -> Result<Option<CursorPosition>> {
    let Some(root_page_id) = root_page_id else {
        return Ok(None);
    };
    let (_, mut leaf) = descend_to_leaf(store, root_page_id, key)?;
    loop {
        if let Some(index) = leaf.cells.iter().position(|cell| cell.key >= key) {
            return Ok(Some(CursorPosition { leaf, index }));
        }

        if leaf.next_leaf == 0 {
            return Ok(None);
        }

        leaf = load_leaf_page(store, leaf.next_leaf)?;
    }
}

pub(crate) fn seek_backward<S: PageStore>(
    store: &S,
    root_page_id: Option<PageId>,
    key: u64,
) -> Result<Option<CursorPosition>> {
    scan_for_predicate(store, root_page_id, |candidate| candidate <= key)
}

pub(crate) fn greatest_less_than<S: PageStore>(
    store: &S,
    root_page_id: Option<PageId>,
    bound: u64,
) -> Result<Option<CursorPosition>> {
    scan_for_predicate(store, root_page_id, |candidate| candidate < bound)
}

pub(crate) fn first_position<S: PageStore>(
    store: &S,
    root_page_id: Option<PageId>,
) -> Result<Option<CursorPosition>> {
    let Some(root_page_id) = root_page_id else {
        return Ok(None);
    };
    let (_, leaf) = first_leaf(store, root_page_id)?;
    if leaf.cells.is_empty() {
        return Ok(None);
    }
    Ok(Some(CursorPosition { leaf, index: 0 }))
}

pub(crate) fn last_position<S: PageStore>(
    store: &S,
    root_page_id: Option<PageId>,
) -> Result<Option<CursorPosition>> {
    let Some(root_page_id) = root_page_id else {
        return Ok(None);
    };
    let (_, leaf) = last_leaf(store, root_page_id)?;
    if leaf.cells.is_empty() {
        return Ok(None);
    }
    Ok(Some(CursorPosition {
        index: leaf.cells.len() - 1,
        leaf,
    }))
}

pub(crate) fn materialize_current<S: PageStore>(
    store: &S,
    position: &CursorPosition,
) -> Result<(u64, Vec<u8>)> {
    let cell = position
        .leaf
        .cells
        .get(position.index)
        .ok_or_else(|| DbError::internal("cursor position points outside leaf"))?;
    Ok((cell.key, materialize_leaf_value(store, cell)?))
}

fn descend_to_leaf<S: PageStore>(
    store: &S,
    mut page_id: PageId,
    key: u64,
) -> Result<(PageId, LeafPage)> {
    loop {
        let page = decode_page(&store.read_page(page_id)?)?;
        match page {
            BtreePage::Leaf(leaf) => return Ok((page_id, leaf)),
            BtreePage::Internal(internal) => {
                page_id = internal
                    .cells
                    .iter()
                    .find(|cell| key < cell.key)
                    .map(|cell| cell.child)
                    .unwrap_or(internal.right_child);
            }
        }
    }
}

fn first_leaf<S: PageStore>(store: &S, mut page_id: PageId) -> Result<(PageId, LeafPage)> {
    loop {
        let page = decode_page(&store.read_page(page_id)?)?;
        match page {
            BtreePage::Leaf(leaf) => return Ok((page_id, leaf)),
            BtreePage::Internal(internal) => {
                page_id = internal
                    .cells
                    .first()
                    .map(|cell| cell.child)
                    .unwrap_or(internal.right_child);
            }
        }
    }
}

fn last_leaf<S: PageStore>(store: &S, mut page_id: PageId) -> Result<(PageId, LeafPage)> {
    loop {
        let page = decode_page(&store.read_page(page_id)?)?;
        match page {
            BtreePage::Leaf(leaf) => return Ok((page_id, leaf)),
            BtreePage::Internal(internal) => {
                page_id = internal.right_child;
            }
        }
    }
}

fn load_leaf_page<S: PageStore>(store: &S, page_id: PageId) -> Result<LeafPage> {
    match decode_page(&store.read_page(page_id)?)? {
        BtreePage::Leaf(leaf) => Ok(leaf),
        BtreePage::Internal(_) => Err(DbError::corruption(format!(
            "expected leaf page at {page_id}, found internal page"
        ))),
    }
}

fn materialize_leaf_value<S: PageStore>(
    store: &S,
    cell: &crate::btree::page::LeafCell,
) -> Result<Vec<u8>> {
    match cell.overflow_page_id {
        Some(page_id) => read_chain(store, page_id),
        None => Ok(cell.value.clone()),
    }
}

fn scan_for_predicate<S: PageStore, F: Fn(u64) -> bool>(
    store: &S,
    root_page_id: Option<PageId>,
    predicate: F,
) -> Result<Option<CursorPosition>> {
    let Some(root_page_id) = root_page_id else {
        return Ok(None);
    };
    let (_, mut leaf) = first_leaf(store, root_page_id)?;
    let mut last_match = None;

    loop {
        for (index, cell) in leaf.cells.iter().enumerate() {
            if predicate(cell.key) {
                last_match = Some(CursorPosition {
                    leaf: leaf.clone(),
                    index,
                });
            } else {
                return Ok(last_match);
            }
        }

        if leaf.next_leaf == 0 {
            return Ok(last_match);
        }

        leaf = load_leaf_page(store, leaf.next_leaf)?;
    }
}
