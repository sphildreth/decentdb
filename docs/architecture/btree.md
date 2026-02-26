# B+Tree

DecentDB stores tables and secondary indexes in a page-based B+Tree implementation (`src/btree/btree.nim`) backed by the pager (`src/pager/`).

At this layer, the tree is a mapping:

- **key**: `uint64`
- **value**: `seq[byte]` (inline payload or an overflow chain)

Higher layers (catalog/storage) define how keys/values are encoded for tables vs indexes.

## Page types and common header

Each B+Tree node is stored in exactly one database page (page size is fixed per database; default is 4KB).

The first bytes of every B+Tree page are:

- `byte[0]`: page type (`PageTypeInternal = 1`, `PageTypeLeaf = 2`)
- `byte[1]`: flags (`PageFlagDeltaKeys = 0x01` means keys are delta-encoded)
- `u16le[2..3]`: cell count
- `u32le[4..7]`:
  - internal pages: `rightChild` (page id)
  - leaf pages: `nextLeaf` (page id, 0 if none)

## Leaf pages

Leaf pages store sorted `(key, value)` cells and link to the next leaf to support forward range scans.

Each cell is encoded as:

- `key`: varint
  - if `PageFlagDeltaKeys` is set, this is stored as the delta from the previous key on the page
- `control`: varint
  - low bit: `1` means “value is stored in an overflow chain”
  - remaining bits:
    - if inline: value length (bytes)
    - if overflow: overflow root page id (`uint32`)
- `value bytes`: present only for inline values

Inline values are capped (see `MaxLeafInlineValueBytes` and `maxInlineValue()`), and large values are stored in an overflow chain.

## Internal pages

Internal pages store separator keys and child pointers, plus a `rightChild` pointer.

Each cell is encoded as:

- `key`: varint (optionally delta-encoded)
- `child`: varint (`uint32` page id)

There are `count` `(key, child)` pairs plus a final `rightChild` pointer.

## Overflow values

If a value is too large to store inline in a leaf, it is written to an overflow chain and the leaf cell stores only the overflow root page id.

When reading, the B+Tree materializes the full value by reading the overflow chain and returning the concatenated bytes.

## Cursors and scans

Leaf pages are linked, so cursors can:

- seek to a key and iterate forward
- perform range scans
- stream full scans without materializing per-leaf arrays (`BTreeCursorStream`)

## Fast paths and caching

To keep inserts and point-lookups fast, the implementation includes:

- a thread-local append cache for monotonic key inserts (tracks the last leaf and last key)
- optional per-page decoded indexes cached in the pager’s `CacheEntry.aux` to avoid repeatedly decoding internal/leaf cell headers

## Where it’s used

- **Tables**: key is the rowid, value is the encoded row.
- **BTREE secondary indexes**: key is a `uint64` sort key derived from the indexed value/expression; the value encodes the rowid (and for some TEXT/BLOB indexes may also embed bytes used for post-verification).
