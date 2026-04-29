use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Number of write_txn lock acquisitions on the read path.
pub static READ_PATH_WRITE_TXN_LOCK_COUNT: AtomicU64 = AtomicU64::new(0);

/// Number of read_page_at_snapshot_lsn calls that checked read_path_write_txn_lock.
pub static READ_PATH_WAL_READER_BEGIN_COUNT: AtomicU64 = AtomicU64::new(0);

/// Returns the current values of the read-path counters and resets them to zero.
#[cfg(feature = "bench-internals")]
#[must_use]
pub fn take_read_path_counters() -> (u64, u64) {
    let write_txn_lock_count = READ_PATH_WRITE_TXN_LOCK_COUNT.swap(0, Ordering::SeqCst);
    let wal_reader_begin_count = READ_PATH_WAL_READER_BEGIN_COUNT.swap(0, Ordering::SeqCst);
    (write_txn_lock_count, wal_reader_begin_count)
}

/// Resets all read-path counters to zero.
#[cfg(feature = "bench-internals")]
pub fn reset_read_path_counters() {
    READ_PATH_WRITE_TXN_LOCK_COUNT.store(0, Ordering::SeqCst);
    READ_PATH_WAL_READER_BEGIN_COUNT.store(0, Ordering::SeqCst);
}
use crate::btree::page::{decode_page, BtreePage, LeafCell, LeafPage};
use crate::btree::write::Btree;
use crate::error::{DbError, Result};
use crate::record::key::encode_index_key as encode_index_key_internal;
use crate::record::row::Row;
use crate::search::trigram::unique_tokens as unique_trigram_tokens;
use crate::storage::checksum;
use crate::storage::page::{InMemoryPageStore, DEFAULT_PAGE_SIZE};
use crate::vfs::stats;
use crate::wal::format::{FrameType, WalFrame, FRAME_HEADER_SIZE, FRAME_TRAILER_SIZE};
use crate::Value;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VfsFileStats {
    pub open_calls: u64,
    pub read_calls: u64,
    pub write_calls: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub sync_data_calls: u64,
    pub sync_metadata_calls: u64,
    pub set_len_calls: u64,
}

impl VfsFileStats {
    #[must_use]
    pub fn sync_calls(self) -> u64 {
        self.sync_data_calls
            .saturating_add(self.sync_metadata_calls)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VfsStats {
    pub db: VfsFileStats,
    pub wal: VfsFileStats,
    pub open_create_like_calls: u64,
    pub file_exists_calls: u64,
    pub remove_file_calls: u64,
    pub canonicalize_calls: u64,
}

impl VfsStats {
    #[must_use]
    pub fn total(self) -> VfsFileStats {
        VfsFileStats {
            open_calls: self.db.open_calls.saturating_add(self.wal.open_calls),
            read_calls: self.db.read_calls.saturating_add(self.wal.read_calls),
            write_calls: self.db.write_calls.saturating_add(self.wal.write_calls),
            bytes_read: self.db.bytes_read.saturating_add(self.wal.bytes_read),
            bytes_written: self.db.bytes_written.saturating_add(self.wal.bytes_written),
            sync_data_calls: self
                .db
                .sync_data_calls
                .saturating_add(self.wal.sync_data_calls),
            sync_metadata_calls: self
                .db
                .sync_metadata_calls
                .saturating_add(self.wal.sync_metadata_calls),
            set_len_calls: self.db.set_len_calls.saturating_add(self.wal.set_len_calls),
        }
    }
}

#[derive(Debug)]
pub struct VfsStatsScope {
    active: bool,
}

impl VfsStatsScope {
    /// Starts benchmark VFS accounting and optionally clears prior counters.
    #[must_use]
    pub fn begin(reset_counters: bool) -> Self {
        if reset_counters {
            stats::reset();
        }
        stats::set_enabled(true);
        Self { active: true }
    }

    /// Stops accounting before scope drop.
    pub fn end(mut self) {
        if self.active {
            stats::set_enabled(false);
            self.active = false;
        }
    }
}

impl Drop for VfsStatsScope {
    fn drop(&mut self) {
        if self.active {
            stats::set_enabled(false);
            self.active = false;
        }
    }
}

/// Enables benchmark VFS accounting globally for the current process.
pub fn enable_vfs_stats() {
    stats::set_enabled(true);
}

/// Disables benchmark VFS accounting globally for the current process.
pub fn disable_vfs_stats() {
    stats::set_enabled(false);
}

/// Clears all benchmark VFS counters.
pub fn reset_vfs_stats() {
    stats::reset();
}

/// Returns the current benchmark VFS counter snapshot.
#[must_use]
pub fn snapshot_vfs_stats() -> VfsStats {
    let snapshot = stats::snapshot();
    VfsStats {
        db: VfsFileStats {
            open_calls: snapshot.db.open_calls,
            read_calls: snapshot.db.read_calls,
            write_calls: snapshot.db.write_calls,
            bytes_read: snapshot.db.bytes_read,
            bytes_written: snapshot.db.bytes_written,
            sync_data_calls: snapshot.db.sync_data_calls,
            sync_metadata_calls: snapshot.db.sync_metadata_calls,
            set_len_calls: snapshot.db.set_len_calls,
        },
        wal: VfsFileStats {
            open_calls: snapshot.wal.open_calls,
            read_calls: snapshot.wal.read_calls,
            write_calls: snapshot.wal.write_calls,
            bytes_read: snapshot.wal.bytes_read,
            bytes_written: snapshot.wal.bytes_written,
            sync_data_calls: snapshot.wal.sync_data_calls,
            sync_metadata_calls: snapshot.wal.sync_metadata_calls,
            set_len_calls: snapshot.wal.set_len_calls,
        },
        open_create_like_calls: snapshot.open_create_like_calls,
        file_exists_calls: snapshot.file_exists_calls,
        remove_file_calls: snapshot.remove_file_calls,
        canonicalize_calls: snapshot.canonicalize_calls,
    }
}

#[must_use]
pub fn default_page_size() -> u32 {
    DEFAULT_PAGE_SIZE
}

#[derive(Debug)]
pub struct BtreeFixture {
    tree: Btree<InMemoryPageStore>,
}

impl BtreeFixture {
    #[must_use]
    pub fn new_empty(page_size: u32) -> Self {
        Self {
            tree: Btree::with_page_size(page_size),
        }
    }

    pub fn with_sequential_keys(
        page_size: u32,
        start_key: u64,
        count: usize,
        value_len: usize,
    ) -> Result<Self> {
        let mut tree = Btree::with_page_size(page_size);
        let mut entries = BTreeMap::new();
        for offset in 0..count {
            let key = start_key.saturating_add(offset as u64);
            entries.insert(key, benchmark_value_bytes(key, value_len));
        }
        tree.replace_entries(entries)?;
        Ok(Self { tree })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.tree.entry_count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn point_lookup(&self, key: u64) -> Result<Option<Vec<u8>>> {
        self.tree.get(key)
    }

    pub fn insert_generated(&mut self, key: u64, value_len: usize) -> Result<()> {
        self.tree
            .insert(key, benchmark_value_bytes(key, value_len))
            .map(|_| ())
    }

    pub fn advance_scan_from(&self, start_key: u64, max_steps: usize) -> Result<usize> {
        let mut cursor = self.tree.cursor_seek_forward(start_key)?;
        let mut advanced = 0;
        while advanced < max_steps {
            if cursor.next()?.is_none() {
                break;
            }
            advanced += 1;
        }
        Ok(advanced)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeafCellInput {
    pub key: u64,
    pub value: Vec<u8>,
    pub overflow_page_id: Option<u32>,
}

#[must_use]
pub fn benchmark_leaf_cells(start_key: u64, count: usize, value_len: usize) -> Vec<LeafCellInput> {
    (0..count)
        .map(|offset| {
            let key = start_key.saturating_add(offset as u64);
            LeafCellInput {
                key,
                value: benchmark_value_bytes(key, value_len),
                overflow_page_id: None,
            }
        })
        .collect()
}

pub fn encode_leaf_page(
    next_leaf: u32,
    delta_keys: bool,
    cells: &[LeafCellInput],
    page_size: usize,
) -> Result<Vec<u8>> {
    let page = LeafPage {
        next_leaf,
        delta_keys,
        cells: cells
            .iter()
            .map(|cell| match cell.overflow_page_id {
                Some(page_id) => LeafCell::overflow(cell.key, page_id),
                None => LeafCell::inline(cell.key, cell.value.clone()),
            })
            .collect(),
    };
    page.encode(page_size)
}

pub fn decode_leaf_page_cell_count(page_bytes: &[u8]) -> Result<usize> {
    match decode_page(page_bytes)? {
        BtreePage::Leaf(page) => Ok(page.cells.len()),
        BtreePage::Internal(_) => Err(DbError::corruption(
            "expected leaf page bytes, found internal page",
        )),
    }
}

pub fn encode_row(values: &[Value]) -> Result<Vec<u8>> {
    Row::encode_values(values)
}

pub fn decode_row(encoded: &[u8]) -> Result<Vec<Value>> {
    Ok(Row::decode(encoded)?.values().to_vec())
}

pub fn encode_index_key(value: &Value) -> Result<Vec<u8>> {
    encode_index_key_internal(value)
}

pub fn encode_wal_frame_page(page_id: u32, payload: &[u8], page_size: u32) -> Result<Vec<u8>> {
    let frame = WalFrame {
        frame_type: FrameType::Page,
        page_id,
        payload: payload.to_vec(),
    };
    frame.encode(page_size)
}

pub fn decode_wal_frame_payload_len(frame_bytes: &[u8], page_size: u32) -> Result<usize> {
    if frame_bytes.len() < FRAME_HEADER_SIZE + FRAME_TRAILER_SIZE {
        return Err(DbError::corruption(
            "WAL frame is shorter than minimum frame length",
        ));
    }

    let frame_type = FrameType::try_from(frame_bytes[0])?;
    let payload_len = frame_type.payload_size(page_size);
    let expected_total = FRAME_HEADER_SIZE + payload_len + FRAME_TRAILER_SIZE;
    if frame_bytes.len() != expected_total {
        return Err(DbError::corruption(format!(
            "WAL frame length {} does not match expected {}",
            frame_bytes.len(),
            expected_total
        )));
    }

    let page_id = u32::from_le_bytes([
        frame_bytes[1],
        frame_bytes[2],
        frame_bytes[3],
        frame_bytes[4],
    ]);
    if matches!(frame_type, FrameType::Page) && page_id == 0 {
        return Err(DbError::corruption("page WAL frame used page id 0"));
    }
    if !matches!(frame_type, FrameType::Page) && page_id != 0 {
        return Err(DbError::corruption(
            "non-page WAL frame used a non-zero page id",
        ));
    }

    Ok(payload_len)
}

pub fn append_wal_page_frame(
    output: &mut Vec<u8>,
    page_id: u32,
    payload: &[u8],
    page_size: u32,
) -> Result<usize> {
    if page_id == 0 {
        return Err(DbError::corruption(
            "page WAL frames must have a non-zero page id",
        ));
    }
    if payload.len() != page_size as usize {
        return Err(DbError::internal(format!(
            "WAL frame payload length {} does not match expected payload length {}",
            payload.len(),
            page_size
        )));
    }

    output.push(FrameType::Page as u8);
    output.extend_from_slice(&page_id.to_le_bytes());
    output.extend_from_slice(payload);
    output.extend_from_slice(&0_u64.to_le_bytes());
    Ok(FRAME_HEADER_SIZE + payload.len() + FRAME_TRAILER_SIZE)
}

#[must_use]
pub fn crc32c_parts(parts: &[&[u8]]) -> u32 {
    checksum::crc32c_parts(parts)
}

#[must_use]
pub fn copy_page_bytes(payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0_u8; payload.len()];
    out.copy_from_slice(payload);
    out
}

#[must_use]
pub fn trigram_tokens(input: &str) -> Vec<u32> {
    unique_trigram_tokens(input)
}

#[must_use]
pub fn intersect_sorted_postings(postings: &[Vec<u64>]) -> Vec<u64> {
    let mut iter = postings.iter();
    let Some(first) = iter.next() else {
        return Vec::new();
    };

    let mut intersection = first.clone();
    for next in iter {
        intersection = intersect_two_sorted(&intersection, next);
        if intersection.is_empty() {
            break;
        }
    }
    intersection
}

fn intersect_two_sorted(left: &[u64], right: &[u64]) -> Vec<u64> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() {
        if left[i] == right[j] {
            out.push(left[i]);
            i += 1;
            j += 1;
        } else if left[i] < right[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

#[must_use]
fn benchmark_value_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for _ in 0..len {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let byte = (state.wrapping_mul(0x2545_F491_4F6C_DD1D) & 0xFF) as u8;
        out.push(byte);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        append_wal_page_frame, benchmark_leaf_cells, copy_page_bytes, crc32c_parts,
        decode_leaf_page_cell_count, decode_row, decode_wal_frame_payload_len, default_page_size,
        encode_index_key, encode_leaf_page, encode_row, encode_wal_frame_page,
        intersect_sorted_postings, trigram_tokens, BtreeFixture,
    };
    use crate::Value;

    #[test]
    fn btree_fixture_supports_lookup_scan_and_insert() {
        let mut fixture =
            BtreeFixture::with_sequential_keys(default_page_size(), 1, 512, 16).expect("fixture");
        assert_eq!(fixture.len(), 512);
        let row = fixture
            .point_lookup(128)
            .expect("lookup result")
            .expect("existing key");
        assert_eq!(row.len(), 16);
        let advanced = fixture.advance_scan_from(200, 64).expect("scan");
        assert_eq!(advanced, 64);
        fixture.insert_generated(999, 24).expect("insert");
        assert_eq!(
            fixture
                .point_lookup(999)
                .expect("lookup")
                .expect("inserted")
                .len(),
            24
        );
    }

    #[test]
    fn leaf_page_helpers_roundtrip_cell_count() {
        let cells = benchmark_leaf_cells(100, 64, 12);
        let encoded =
            encode_leaf_page(0, false, &cells, default_page_size() as usize).expect("encode");
        let count = decode_leaf_page_cell_count(&encoded).expect("decode");
        assert_eq!(count, 64);
    }

    #[test]
    fn row_encode_decode_roundtrip() {
        let values = vec![
            Value::Int64(42),
            Value::Text("alpha".to_string()),
            Value::Bool(true),
        ];
        let encoded = encode_row(&values).expect("encode row");
        let decoded = decode_row(&encoded).expect("decode row");
        assert_eq!(decoded, values);
    }

    #[test]
    fn index_key_encoding_is_available() {
        let key = encode_index_key(&Value::Text("bench".to_string())).expect("encode key");
        assert!(!key.is_empty());
    }

    #[test]
    fn wal_append_frame_writes_expected_bytes() {
        let page_size = default_page_size();
        let payload = vec![7_u8; page_size as usize];
        let mut output = Vec::new();
        let written = append_wal_page_frame(&mut output, 9, &payload, page_size).expect("append");
        assert_eq!(written, output.len());
        assert_eq!(output[0], 0);
    }

    #[test]
    fn wal_encode_decode_roundtrip_payload_len() {
        let page_size = default_page_size();
        let payload = vec![11_u8; page_size as usize];
        let encoded = encode_wal_frame_page(13, &payload, page_size).expect("encode");
        let decoded_len = decode_wal_frame_payload_len(&encoded, page_size).expect("decode");
        assert_eq!(decoded_len, payload.len());
    }

    #[test]
    fn crc_wrapper_matches_known_vector() {
        assert_eq!(crc32c_parts(&[b"123456789"]), 0xE306_9283);
    }

    #[test]
    fn copy_page_bytes_roundtrip() {
        let payload = vec![0xAB_u8; default_page_size() as usize];
        let copied = copy_page_bytes(&payload);
        assert_eq!(copied, payload);
    }

    #[test]
    fn trigram_tokenization_and_intersection_work() {
        let tokens = trigram_tokens("alphabet soup");
        assert!(!tokens.is_empty());

        let postings = vec![
            vec![1_u64, 3, 5, 7, 9],
            vec![0_u64, 3, 4, 7, 8],
            vec![2_u64, 3, 7, 10],
        ];
        let intersection = intersect_sorted_postings(&postings);
        assert_eq!(intersection, vec![3_u64, 7]);
    }
}
