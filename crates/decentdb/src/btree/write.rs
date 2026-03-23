//! Deterministic B+Tree mutation helpers with overflow-page support.

use std::collections::BTreeMap;

use crate::btree::cursor::BtreeCursor;
use crate::btree::page::{InternalCell, InternalPage, LeafCell, LeafPage};
use crate::btree::read::find_exact;
use crate::error::{DbError, Result};
use crate::record::compression::CompressionMode;
use crate::record::overflow::{free_overflow, write_overflow};
use crate::storage::page::{InMemoryPageStore, PageId, PageStore};

#[derive(Clone, Debug)]
struct ChildRef {
    first_key: u64,
    page_id: PageId,
}

#[derive(Debug)]
pub(crate) struct Btree<S: PageStore> {
    store: S,
    root_page_id: Option<PageId>,
    entries: BTreeMap<u64, Vec<u8>>,
    btree_pages: Vec<PageId>,
    overflow_heads: Vec<PageId>,
    inline_value_limit: usize,
}

impl<S: PageStore> Btree<S> {
    pub(crate) fn new(store: S) -> Self {
        let inline_value_limit = usize::min(512, store.page_size() as usize / 4);
        Self {
            store,
            root_page_id: None,
            entries: BTreeMap::new(),
            btree_pages: Vec::new(),
            overflow_heads: Vec::new(),
            inline_value_limit,
        }
    }

    #[must_use]
    pub(crate) fn root_page_id(&self) -> Option<PageId> {
        self.root_page_id
    }

    #[must_use]
    pub(crate) fn page_size(&self) -> u32 {
        self.store.page_size()
    }

    #[must_use]
    pub(crate) fn store(&self) -> &S {
        &self.store
    }

    pub(crate) fn get(&self, key: u64) -> Result<Option<Vec<u8>>> {
        find_exact(&self.store, self.root_page_id, key)
    }

    pub(crate) fn insert(&mut self, key: u64, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let previous = self.entries.insert(key, value);
        self.rebuild_pages()?;
        Ok(previous)
    }

    pub(crate) fn update(&mut self, key: u64, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.insert(key, value)
    }

    pub(crate) fn delete(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        let previous = self.entries.remove(&key);
        self.rebuild_pages()?;
        Ok(previous)
    }

    pub(crate) fn clear(&mut self) -> Result<()> {
        self.entries.clear();
        self.rebuild_pages()
    }

    pub(crate) fn cursor_from_start(&self) -> Result<BtreeCursor<'_, S>> {
        BtreeCursor::from_start(&self.store, self.root_page_id)
    }

    pub(crate) fn cursor_from_end(&self) -> Result<BtreeCursor<'_, S>> {
        BtreeCursor::from_end(&self.store, self.root_page_id)
    }

    pub(crate) fn cursor_seek_forward(&self, key: u64) -> Result<BtreeCursor<'_, S>> {
        BtreeCursor::seek_forward(&self.store, self.root_page_id, key)
    }

    pub(crate) fn cursor_seek_backward(&self, key: u64) -> Result<BtreeCursor<'_, S>> {
        BtreeCursor::seek_backward(&self.store, self.root_page_id, key)
    }

    fn rebuild_pages(&mut self) -> Result<()> {
        for head_page_id in self.overflow_heads.drain(..) {
            free_overflow(&mut self.store, head_page_id)?;
        }
        for page_id in self.btree_pages.drain(..) {
            self.store.free_page(page_id)?;
        }
        self.root_page_id = None;

        if self.entries.is_empty() {
            return Ok(());
        }

        let mut leaf_specs = Vec::new();
        let mut current_cells = Vec::new();
        for (&key, value) in &self.entries {
            let cell = if value.len() > self.inline_value_limit {
                let pointer = write_overflow(&mut self.store, value, CompressionMode::Never)?;
                self.overflow_heads.push(pointer.head_page_id);
                LeafCell::overflow(key, pointer.head_page_id)
            } else {
                LeafCell::inline(key, value.clone())
            };

            current_cells.push(cell.clone());
            let candidate = LeafPage {
                next_leaf: 0,
                delta_keys: false,
                cells: current_cells.clone(),
            };
            if candidate.encoded_len() > self.store.page_size() as usize {
                let overflowed = current_cells
                    .pop()
                    .ok_or_else(|| DbError::internal("leaf packing underflow"))?;
                if current_cells.is_empty() {
                    return Err(DbError::constraint(format!(
                        "leaf cell for key {key} exceeds configured page size"
                    )));
                }
                leaf_specs.push(LeafPage {
                    next_leaf: 0,
                    delta_keys: false,
                    cells: std::mem::take(&mut current_cells),
                });
                current_cells.push(overflowed);
            }
        }
        if !current_cells.is_empty() {
            leaf_specs.push(LeafPage {
                next_leaf: 0,
                delta_keys: false,
                cells: current_cells,
            });
        }

        let leaf_page_ids = (0..leaf_specs.len())
            .map(|_| self.store.allocate_page())
            .collect::<Result<Vec<_>>>()?;
        self.btree_pages.extend_from_slice(&leaf_page_ids);

        let mut children = Vec::with_capacity(leaf_specs.len());
        for (index, mut leaf) in leaf_specs.into_iter().enumerate() {
            leaf.next_leaf = leaf_page_ids.get(index + 1).copied().unwrap_or(0);
            let page_id = leaf_page_ids[index];
            let first_key = leaf
                .cells
                .first()
                .map(|cell| cell.key)
                .ok_or_else(|| DbError::internal("leaf page must contain at least one cell"))?;
            let bytes = leaf.encode(self.store.page_size() as usize)?;
            self.store.write_page(page_id, &bytes)?;
            children.push(ChildRef { first_key, page_id });
        }

        while children.len() > 1 {
            let groups =
                group_children_for_internal_pages(&children, self.store.page_size() as usize)?;
            children = groups
                .into_iter()
                .map(|group| {
                    let page = build_internal_page(&group)?;
                    let page_id = self.store.allocate_page()?;
                    let bytes = page.encode(self.store.page_size() as usize)?;
                    self.store.write_page(page_id, &bytes)?;
                    self.btree_pages.push(page_id);
                    Ok(ChildRef {
                        first_key: group[0].first_key,
                        page_id,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        }

        self.root_page_id = Some(children[0].page_id);
        Ok(())
    }
}

impl Btree<InMemoryPageStore> {
    pub(crate) fn with_page_size(page_size: u32) -> Self {
        Self::new(InMemoryPageStore::new(page_size))
    }
}

fn build_internal_page(children: &[ChildRef]) -> Result<InternalPage> {
    if children.len() < 2 {
        return Err(DbError::internal(
            "internal page construction requires at least two children",
        ));
    }

    let cells = children[..children.len() - 1]
        .iter()
        .zip(children[1..].iter())
        .map(|(left, right)| InternalCell {
            key: right.first_key,
            child: left.page_id,
        })
        .collect();

    Ok(InternalPage {
        right_child: children.last().expect("children not empty").page_id,
        delta_keys: false,
        cells,
    })
}

fn group_children_for_internal_pages(
    children: &[ChildRef],
    page_size: usize,
) -> Result<Vec<Vec<ChildRef>>> {
    if children.len() < 2 {
        return Ok(vec![children.to_vec()]);
    }

    let mut groups = Vec::new();
    let mut current = vec![children[0].clone(), children[1].clone()];
    for child in children.iter().skip(2) {
        let mut candidate = current.clone();
        candidate.push(child.clone());
        if build_internal_page(&candidate)?.encoded_len() <= page_size {
            current.push(child.clone());
        } else {
            groups.push(current);
            current = vec![child.clone()];
        }
    }

    if current.len() == 1 {
        let previous = groups
            .last_mut()
            .ok_or_else(|| DbError::internal("cannot balance a singleton internal-page group"))?;
        let borrowed = previous
            .pop()
            .ok_or_else(|| DbError::internal("previous internal-page group is empty"))?;
        current.insert(0, borrowed);
    }
    groups.push(current);

    Ok(groups)
}

#[cfg(test)]
mod tests {
    use crate::btree::write::Btree;

    fn collect_forward(
        tree: &Btree<crate::storage::page::InMemoryPageStore>,
    ) -> Vec<(u64, Vec<u8>)> {
        let mut cursor = tree.cursor_from_start().expect("cursor");
        let mut items = Vec::new();
        while let Some(item) = cursor.next().expect("next") {
            items.push(item);
        }
        items
    }

    fn collect_backward(tree: &Btree<crate::storage::page::InMemoryPageStore>) -> Vec<u64> {
        let mut cursor = tree.cursor_from_end().expect("cursor");
        let mut keys = Vec::new();
        while let Some((key, _)) = cursor.prev().expect("prev") {
            keys.push(key);
        }
        keys
    }

    #[test]
    fn ascending_descending_and_randomized_inserts_produce_searchable_tree() {
        let mut tree = Btree::with_page_size(4096);
        for key in 0_u64..256 {
            tree.insert(key, format!("value-{key}").into_bytes())
                .expect("insert ascending");
        }
        for key in (256_u64..512).rev() {
            tree.insert(key, format!("value-{key}").into_bytes())
                .expect("insert descending");
        }
        for key in [900, 700, 800, 600, 650, 620, 610] {
            tree.insert(key, format!("value-{key}").into_bytes())
                .expect("insert randomized");
        }

        assert!(tree.root_page_id().is_some());
        assert_eq!(tree.get(0).expect("lookup"), Some(b"value-0".to_vec()));
        assert_eq!(tree.get(511).expect("lookup"), Some(b"value-511".to_vec()));
        assert_eq!(tree.get(620).expect("lookup"), Some(b"value-620".to_vec()));
        assert_eq!(tree.get(999).expect("lookup"), None);
    }

    #[test]
    fn cursors_traverse_across_leaf_boundaries() {
        let mut tree = Btree::with_page_size(512);
        for key in 0_u64..64 {
            tree.insert(key, vec![key as u8; 12]).expect("insert");
        }

        let forward_keys = collect_forward(&tree)
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(forward_keys, (0_u64..64).collect::<Vec<_>>());
        assert_eq!(
            collect_backward(&tree),
            (0_u64..64).rev().collect::<Vec<_>>()
        );
    }

    #[test]
    fn large_values_use_overflow_and_delete_update_release_old_chains() {
        let mut tree = Btree::with_page_size(512);
        let large = vec![0xAB; 8_192];
        tree.insert(7, large.clone()).expect("insert");
        assert_eq!(tree.get(7).expect("lookup"), Some(large.clone()));

        let first_head = tree
            .overflow_heads
            .first()
            .copied()
            .expect("first overflow head");
        tree.update(7, vec![0xCD; 4_096]).expect("update");
        assert!(!tree.store().contains_page(first_head));

        let second_head = tree
            .overflow_heads
            .first()
            .copied()
            .expect("second overflow head");
        tree.delete(7).expect("delete");
        assert!(!tree.store().contains_page(second_head));
        assert_eq!(tree.get(7).expect("lookup"), None);
    }

    #[test]
    fn seek_cursors_find_expected_positions() {
        let mut tree = Btree::with_page_size(512);
        for key in [10_u64, 20, 30, 40, 50] {
            tree.insert(key, vec![key as u8]).expect("insert");
        }

        let mut forward = tree.cursor_seek_forward(25).expect("seek forward");
        assert_eq!(forward.next().expect("next").map(|item| item.0), Some(30));

        let mut backward = tree.cursor_seek_backward(25).expect("seek backward");
        assert_eq!(backward.prev().expect("prev").map(|item| item.0), Some(20));
    }
}
