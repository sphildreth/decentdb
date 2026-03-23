# B+Tree

DecentDB stores table payloads and postings blobs in a page-based B+Tree implementation in:

- `crates/decentdb/src/btree/page.rs`
- `crates/decentdb/src/btree/read.rs`
- `crates/decentdb/src/btree/cursor.rs`
- `crates/decentdb/src/btree/write.rs`

At this layer, the tree is a mapping:

- **key**: `uint64`
- **value**: opaque bytes (inline payload or an overflow chain)

Higher layers define how those bytes are produced:

- table rows use `crates/decentdb/src/record/row.rs`
- typed comparable index-key bytes live in `crates/decentdb/src/record/key.rs`
- trigram postings blobs use `crates/decentdb/src/search/postings.rs`

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
- iterate backward by re-seeking the predecessor key when needed
- perform range scans across leaf boundaries

## Where it’s used

- **Tables**: key is the rowid, value is the encoded row.
- **Trigram postings**: key is the packed trigram token; value is the delta-encoded postings blob.
- **Comparable secondary-index encodings**: implemented in `record/key.rs` and consumed by later relational slices.
