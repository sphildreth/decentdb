//! Compact B+Tree page encoding.
//!
//! Implements:
//! - design/adr/0035-btree-page-layout-v2.md

use crate::error::{DbError, Result};
use crate::record::{decode_varint_u64, encode_varint_u64};
use crate::storage::page::PageId;

pub(crate) const PAGE_TYPE_INTERNAL: u8 = 1;
pub(crate) const PAGE_TYPE_LEAF: u8 = 2;
pub(crate) const PAGE_FLAG_DELTA_KEYS: u8 = 0x01;
pub(crate) const PAGE_HEADER_SIZE: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeafCell {
    pub(crate) key: u64,
    pub(crate) value: Vec<u8>,
    pub(crate) overflow_page_id: Option<PageId>,
}

impl LeafCell {
    #[must_use]
    pub(crate) fn inline(key: u64, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            overflow_page_id: None,
        }
    }

    #[must_use]
    pub(crate) fn overflow(key: u64, overflow_page_id: PageId) -> Self {
        Self {
            key,
            value: Vec::new(),
            overflow_page_id: Some(overflow_page_id),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeafPage {
    pub(crate) next_leaf: PageId,
    pub(crate) delta_keys: bool,
    pub(crate) cells: Vec<LeafCell>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InternalCell {
    pub(crate) key: u64,
    pub(crate) child: PageId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InternalPage {
    pub(crate) right_child: PageId,
    pub(crate) delta_keys: bool,
    pub(crate) cells: Vec<InternalCell>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BtreePage {
    Leaf(LeafPage),
    Internal(InternalPage),
}

impl LeafPage {
    #[must_use]
    pub(crate) fn encoded_len(&self) -> usize {
        PAGE_HEADER_SIZE
            + self
                .cells
                .iter()
                .scan(0_u64, |previous_key, cell| {
                    let encoded_key = if self.delta_keys {
                        cell.key.saturating_sub(*previous_key)
                    } else {
                        cell.key
                    };
                    *previous_key = cell.key;
                    let control = match cell.overflow_page_id {
                        Some(page_id) => (u64::from(page_id) << 1) | 1,
                        None => {
                            (u64::try_from(cell.value.len()).expect("value length fits u64")) << 1
                        }
                    };
                    Some(
                        encode_varint_u64(encoded_key).len()
                            + encode_varint_u64(control).len()
                            + cell.value.len(),
                    )
                })
                .sum::<usize>()
    }

    pub(crate) fn encode(&self, page_size: usize) -> Result<Vec<u8>> {
        let mut output = vec![0_u8; page_size];
        output[0] = PAGE_TYPE_LEAF;
        output[1] = if self.delta_keys {
            PAGE_FLAG_DELTA_KEYS
        } else {
            0
        };
        output[2..4].copy_from_slice(
            &(u16::try_from(self.cells.len())
                .map_err(|_| DbError::constraint("leaf cell count exceeds u16"))?)
            .to_le_bytes(),
        );
        output[4..8].copy_from_slice(&self.next_leaf.to_le_bytes());

        let mut cursor = PAGE_HEADER_SIZE;
        let mut previous_key = 0_u64;
        for cell in &self.cells {
            let encoded_key = if self.delta_keys {
                cell.key.saturating_sub(previous_key)
            } else {
                cell.key
            };
            previous_key = cell.key;
            let key_bytes = encode_varint_u64(encoded_key);
            let control = match cell.overflow_page_id {
                Some(page_id) => (u64::from(page_id) << 1) | 1,
                None => (u64::try_from(cell.value.len()).expect("value length fits u64")) << 1,
            };
            let control_bytes = encode_varint_u64(control);
            let needed = key_bytes.len() + control_bytes.len() + cell.value.len();
            if cursor + needed > output.len() {
                return Err(DbError::constraint(
                    "leaf page exceeds configured page size",
                ));
            }
            output[cursor..cursor + key_bytes.len()].copy_from_slice(&key_bytes);
            cursor += key_bytes.len();
            output[cursor..cursor + control_bytes.len()].copy_from_slice(&control_bytes);
            cursor += control_bytes.len();
            if cell.overflow_page_id.is_none() {
                output[cursor..cursor + cell.value.len()].copy_from_slice(&cell.value);
                cursor += cell.value.len();
            }
        }
        Ok(output)
    }
}

impl InternalPage {
    #[must_use]
    pub(crate) fn encoded_len(&self) -> usize {
        PAGE_HEADER_SIZE
            + self
                .cells
                .iter()
                .scan(0_u64, |previous_key, cell| {
                    let encoded_key = if self.delta_keys {
                        cell.key.saturating_sub(*previous_key)
                    } else {
                        cell.key
                    };
                    *previous_key = cell.key;
                    Some(
                        encode_varint_u64(encoded_key).len()
                            + encode_varint_u64(u64::from(cell.child)).len(),
                    )
                })
                .sum::<usize>()
    }

    pub(crate) fn encode(&self, page_size: usize) -> Result<Vec<u8>> {
        let mut output = vec![0_u8; page_size];
        output[0] = PAGE_TYPE_INTERNAL;
        output[1] = if self.delta_keys {
            PAGE_FLAG_DELTA_KEYS
        } else {
            0
        };
        output[2..4].copy_from_slice(
            &(u16::try_from(self.cells.len())
                .map_err(|_| DbError::constraint("internal cell count exceeds u16"))?)
            .to_le_bytes(),
        );
        output[4..8].copy_from_slice(&self.right_child.to_le_bytes());

        let mut cursor = PAGE_HEADER_SIZE;
        let mut previous_key = 0_u64;
        for cell in &self.cells {
            let encoded_key = if self.delta_keys {
                cell.key.saturating_sub(previous_key)
            } else {
                cell.key
            };
            previous_key = cell.key;
            let key_bytes = encode_varint_u64(encoded_key);
            let child_bytes = encode_varint_u64(u64::from(cell.child));
            let needed = key_bytes.len() + child_bytes.len();
            if cursor + needed > output.len() {
                return Err(DbError::constraint(
                    "internal page exceeds configured page size",
                ));
            }
            output[cursor..cursor + key_bytes.len()].copy_from_slice(&key_bytes);
            cursor += key_bytes.len();
            output[cursor..cursor + child_bytes.len()].copy_from_slice(&child_bytes);
            cursor += child_bytes.len();
        }
        Ok(output)
    }
}

pub(crate) fn decode_page(bytes: &[u8]) -> Result<BtreePage> {
    if bytes.len() < PAGE_HEADER_SIZE {
        return Err(DbError::corruption("B+Tree page shorter than header"));
    }
    let delta_keys = bytes[1] & PAGE_FLAG_DELTA_KEYS != 0;
    let cell_count = u16::from_le_bytes(bytes[2..4].try_into().expect("cell count")) as usize;

    match bytes[0] {
        PAGE_TYPE_LEAF => {
            let next_leaf = u32::from_le_bytes(bytes[4..8].try_into().expect("next leaf"));
            let mut offset = PAGE_HEADER_SIZE;
            let mut previous_key = 0_u64;
            let mut cells = Vec::with_capacity(cell_count);
            for _ in 0..cell_count {
                let (encoded_key, key_bytes) = decode_varint_u64(&bytes[offset..])?;
                offset += key_bytes;
                let key = if delta_keys {
                    previous_key
                        .checked_add(encoded_key)
                        .ok_or_else(|| DbError::corruption("delta-encoded leaf key overflow"))?
                } else {
                    encoded_key
                };
                previous_key = key;

                let (control, control_bytes) = decode_varint_u64(&bytes[offset..])?;
                offset += control_bytes;
                if control & 1 == 1 {
                    cells.push(LeafCell::overflow(
                        key,
                        u32::try_from(control >> 1)
                            .map_err(|_| DbError::corruption("overflow page id exceeds u32"))?,
                    ));
                } else {
                    let len = usize::try_from(control >> 1)
                        .map_err(|_| DbError::corruption("leaf payload length exceeds usize"))?;
                    let end = offset + len;
                    let payload = bytes
                        .get(offset..end)
                        .ok_or_else(|| DbError::corruption("truncated leaf payload"))?;
                    cells.push(LeafCell::inline(key, payload.to_vec()));
                    offset = end;
                }
            }

            Ok(BtreePage::Leaf(LeafPage {
                next_leaf,
                delta_keys,
                cells,
            }))
        }
        PAGE_TYPE_INTERNAL => {
            let right_child = u32::from_le_bytes(bytes[4..8].try_into().expect("right child"));
            let mut offset = PAGE_HEADER_SIZE;
            let mut previous_key = 0_u64;
            let mut cells = Vec::with_capacity(cell_count);
            for _ in 0..cell_count {
                let (encoded_key, key_bytes) = decode_varint_u64(&bytes[offset..])?;
                offset += key_bytes;
                let key = if delta_keys {
                    previous_key
                        .checked_add(encoded_key)
                        .ok_or_else(|| DbError::corruption("delta-encoded internal key overflow"))?
                } else {
                    encoded_key
                };
                previous_key = key;

                let (child, child_bytes) = decode_varint_u64(&bytes[offset..])?;
                offset += child_bytes;
                cells.push(InternalCell {
                    key,
                    child: u32::try_from(child)
                        .map_err(|_| DbError::corruption("child page id exceeds u32"))?,
                });
            }

            Ok(BtreePage::Internal(InternalPage {
                right_child,
                delta_keys,
                cells,
            }))
        }
        other => Err(DbError::corruption(format!(
            "unknown B+Tree page type {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_page, BtreePage, InternalCell, InternalPage, LeafCell, LeafPage};

    #[test]
    fn leaf_page_roundtrip_preserves_cells() {
        let page = LeafPage {
            next_leaf: 42,
            delta_keys: false,
            cells: vec![
                LeafCell::inline(7, b"alpha".to_vec()),
                LeafCell::overflow(19, 88),
            ],
        };

        let encoded = page.encode(256).expect("encode");
        let decoded = decode_page(&encoded).expect("decode");
        assert_eq!(decoded, BtreePage::Leaf(page));
    }

    #[test]
    fn internal_page_roundtrip_preserves_cells() {
        let page = InternalPage {
            right_child: 17,
            delta_keys: true,
            cells: vec![
                InternalCell { key: 5, child: 3 },
                InternalCell { key: 11, child: 9 },
            ],
        };

        let encoded = page.encode(256).expect("encode");
        let decoded = decode_page(&encoded).expect("decode");
        assert_eq!(decoded, BtreePage::Internal(page));
    }
}
