use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::Serialize;

const DB_HEADER_SIZE: usize = 128;
const DB_MAGIC: [u8; 16] = *b"DECENTDB\0\0\0\0\0\0\0\0";
const PAGE_SIZE_OFFSET: usize = 20;
const CATALOG_ROOT_OFFSET: usize = 32;
const FREELIST_HEAD_OFFSET: usize = 40;
const FREELIST_COUNT_OFFSET: usize = 44;

const ENGINE_ROOT_MAGIC: [u8; 8] = *b"DDBSQL1\0";
const MANIFEST_PAYLOAD_MAGIC: &[u8; 8] = b"DDBMANF1";
const OVERFLOW_HEADER_SIZE: usize = 8;
const OVERFLOW_FLAG_COMPRESSED: u8 = 0x01;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StorageInspection {
    pub db_path: String,
    pub wal_path: String,
    pub page_size: u32,
    pub page_count: u32,
    pub db_file_bytes: u64,
    pub wal_file_bytes: u64,
    pub page_counts: PageCounts,
    pub bytes: StorageBytes,
    pub overflow: OverflowBreakdown,
    pub precision: AttributionPrecision,
    pub warnings: Vec<String>,
    pub tables: Vec<TableOverflowUsage>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PageCounts {
    pub metadata_pages: u32,
    pub manifest_overflow_pages: u32,
    pub table_overflow_pages: u32,
    pub freelist_pages: u32,
    pub unknown_pages: u32,
    pub total_pages: u32,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StorageBytes {
    pub metadata_bytes: u64,
    pub catalog_manifest_bytes: u64,
    pub table_data_bytes: u64,
    pub index_bytes: Option<u64>,
    pub freelist_bytes: u64,
    pub overflow_bytes_total: u64,
    pub unknown_bytes: u64,
    pub wal_bytes: u64,
    pub db_total_bytes: u64,
    pub db_plus_wal_total_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OverflowBreakdown {
    pub manifest_payload_logical_bytes: u64,
    pub table_payload_logical_bytes: u64,
    pub overflow_pages_total: u32,
    pub overflow_pages_unowned: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AttributionPrecision {
    pub metadata: &'static str,
    pub table_data: &'static str,
    pub index: &'static str,
    pub freelist: &'static str,
    pub overflow: &'static str,
    pub unknown: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TableOverflowUsage {
    pub table_name: String,
    pub page_count: u32,
    pub allocated_bytes: u64,
    pub logical_payload_bytes: u64,
    pub compressed: bool,
}

#[derive(Debug, Clone, Copy)]
struct OverflowPointer {
    head_page_id: u32,
    logical_len: u32,
    flags: u8,
}

#[derive(Debug)]
struct ParsedManifest {
    table_pointers: Vec<(String, OverflowPointer)>,
}

#[derive(Debug)]
struct OverflowChainRead {
    page_ids: Vec<u32>,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct HeaderInfo {
    page_size: u32,
    catalog_root_page_id: u32,
    freelist_head_page_id: u32,
    freelist_page_count: u32,
}

pub(crate) fn inspect_db_file(db_path: &Path) -> Result<StorageInspection> {
    let db_path = db_path
        .canonicalize()
        .with_context(|| format!("canonicalize {}", db_path.display()))?;
    let wal_path = wal_path_for_db(&db_path);

    let mut db = File::open(&db_path)
        .with_context(|| format!("open database file {}", db_path.display()))?;
    let db_file_bytes = db
        .metadata()
        .with_context(|| format!("read metadata for {}", db_path.display()))?
        .len();
    if db_file_bytes < DB_HEADER_SIZE as u64 {
        return Err(anyhow!(
            "database file {} is too small to contain a valid header",
            db_path.display()
        ));
    }

    let header = read_header(&mut db)?;
    let page_size_u64 = u64::from(header.page_size);
    if page_size_u64 == 0 {
        return Err(anyhow!("decoded page size was 0"));
    }

    let mut warnings = Vec::new();
    if db_file_bytes % page_size_u64 != 0 {
        warnings.push(format!(
            "database file size {} is not an even multiple of page size {}; trailing bytes are ignored",
            db_file_bytes, header.page_size
        ));
    }
    let page_count = (db_file_bytes / page_size_u64) as u32;

    let mut metadata_pages = BTreeSet::new();
    let mut manifest_pages = BTreeSet::new();
    let mut table_pages = BTreeSet::new();
    let mut freelist_pages = BTreeSet::new();
    metadata_pages.insert(1_u32);

    let mut manifest_payload_logical_bytes = 0_u64;
    let mut table_payload_logical_bytes = 0_u64;
    let mut tables = Vec::new();

    if header.catalog_root_page_id > 0 && header.catalog_root_page_id <= page_count {
        metadata_pages.insert(header.catalog_root_page_id);
        let root_page = read_page(
            &mut db,
            header.catalog_root_page_id,
            header.page_size,
            page_count,
        )
        .with_context(|| "read catalog root page".to_string())?;

        if root_page.iter().all(|byte| *byte == 0) {
            warnings.push("catalog root page is zeroed; manifest pointer is absent".to_string());
        } else if root_page
            .get(..ENGINE_ROOT_MAGIC.len())
            .is_some_and(|prefix| prefix == ENGINE_ROOT_MAGIC)
        {
            let manifest_ptr = OverflowPointer {
                head_page_id: read_u32_from_page(&root_page, 20)?,
                logical_len: read_u32_from_page(&root_page, 24)?,
                flags: *root_page
                    .get(28)
                    .ok_or_else(|| anyhow!("catalog root page truncated at manifest flags"))?,
            };
            if manifest_ptr.head_page_id != 0 && manifest_ptr.logical_len > 0 {
                let manifest_chain = read_overflow_chain(
                    &mut db,
                    manifest_ptr,
                    header.page_size,
                    page_count,
                    &mut warnings,
                )?;
                manifest_pages.extend(manifest_chain.page_ids.iter().copied());
                manifest_payload_logical_bytes = manifest_ptr.logical_len as u64;
                let parsed_manifest =
                    parse_manifest_payload(&manifest_chain.payload, &mut warnings)
                        .context("parse catalog manifest payload")?;

                for (table_name, pointer) in parsed_manifest.table_pointers {
                    if pointer.head_page_id == 0 || pointer.logical_len == 0 {
                        tables.push(TableOverflowUsage {
                            table_name,
                            page_count: 0,
                            allocated_bytes: 0,
                            logical_payload_bytes: pointer.logical_len as u64,
                            compressed: pointer.flags & OVERFLOW_FLAG_COMPRESSED != 0,
                        });
                        continue;
                    }

                    let table_chain = read_overflow_chain(
                        &mut db,
                        pointer,
                        header.page_size,
                        page_count,
                        &mut warnings,
                    )?;
                    for page_id in &table_chain.page_ids {
                        if !table_pages.insert(*page_id) {
                            warnings.push(format!(
                                "table overflow page {} is referenced by multiple chains",
                                page_id
                            ));
                        }
                    }
                    table_payload_logical_bytes =
                        table_payload_logical_bytes.saturating_add(pointer.logical_len as u64);
                    tables.push(TableOverflowUsage {
                        table_name,
                        page_count: table_chain.page_ids.len() as u32,
                        allocated_bytes: (table_chain.page_ids.len() as u64)
                            .saturating_mul(page_size_u64),
                        logical_payload_bytes: pointer.logical_len as u64,
                        compressed: pointer.flags & OVERFLOW_FLAG_COMPRESSED != 0,
                    });
                }
            }
        } else {
            warnings.push(
                "catalog root page magic is not recognized; manifest attribution is unavailable"
                    .to_string(),
            );
        }
    } else {
        warnings.push(format!(
            "catalog root page {} is outside page_count {}; metadata attribution is partial",
            header.catalog_root_page_id, page_count
        ));
    }

    let freelist_chain = read_freelist_chain(
        &mut db,
        header.freelist_head_page_id,
        header.page_size,
        page_count,
        &mut warnings,
    )?;
    freelist_pages.extend(freelist_chain.iter().copied());
    if header.freelist_page_count != 0 && header.freelist_page_count != freelist_chain.len() as u32
    {
        warnings.push(format!(
            "header freelist count {} does not match traversed freelist pages {}",
            header.freelist_page_count,
            freelist_chain.len()
        ));
    }

    let mut classified_pages = BTreeSet::new();
    classified_pages.extend(metadata_pages.iter().copied());
    classified_pages.extend(manifest_pages.iter().copied());
    classified_pages.extend(table_pages.iter().copied());
    classified_pages.extend(freelist_pages.iter().copied());

    let mut unknown_pages = BTreeSet::new();
    for page_id in 1..=page_count {
        if !classified_pages.contains(&page_id) {
            unknown_pages.insert(page_id);
        }
    }

    let overflow_pages_total: BTreeSet<u32> = manifest_pages.union(&table_pages).copied().collect();

    let wal_file_bytes = match std::fs::metadata(&wal_path) {
        Ok(meta) => meta.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => {
            warnings.push(format!(
                "failed to read WAL metadata at {}: {err}",
                wal_path.display()
            ));
            0
        }
    };

    let page_counts = PageCounts {
        metadata_pages: metadata_pages.len() as u32,
        manifest_overflow_pages: manifest_pages.len() as u32,
        table_overflow_pages: table_pages.len() as u32,
        freelist_pages: freelist_pages.len() as u32,
        unknown_pages: unknown_pages.len() as u32,
        total_pages: page_count,
    };

    let metadata_bytes = u64::from(page_counts.metadata_pages).saturating_mul(page_size_u64);
    let catalog_manifest_bytes =
        u64::from(page_counts.manifest_overflow_pages).saturating_mul(page_size_u64);
    let table_data_bytes =
        u64::from(page_counts.table_overflow_pages).saturating_mul(page_size_u64);
    let freelist_bytes = u64::from(page_counts.freelist_pages).saturating_mul(page_size_u64);
    let unknown_bytes = u64::from(page_counts.unknown_pages).saturating_mul(page_size_u64);
    let overflow_bytes_total = u64::from(page_counts.manifest_overflow_pages)
        .saturating_add(u64::from(page_counts.table_overflow_pages))
        .saturating_mul(page_size_u64);

    let bytes = StorageBytes {
        metadata_bytes,
        catalog_manifest_bytes,
        table_data_bytes,
        index_bytes: None,
        freelist_bytes,
        overflow_bytes_total,
        unknown_bytes,
        wal_bytes: wal_file_bytes,
        db_total_bytes: db_file_bytes,
        db_plus_wal_total_bytes: db_file_bytes.saturating_add(wal_file_bytes),
    };

    let precision = AttributionPrecision {
        metadata: "exact_page_allocation",
        table_data: if tables.is_empty() {
            "inferred_unavailable"
        } else {
            "exact_page_allocation"
        },
        index: "inferred_unavailable",
        freelist: "exact_page_allocation",
        overflow: "exact_page_allocation",
        unknown: "exact_by_exclusion",
    };

    if precision.index == "inferred_unavailable" {
        warnings.push(
            "index_bytes is null because current on-disk format does not expose separate persisted index page allocations for direct attribution"
                .to_string(),
        );
    }
    warnings.push(
        "overflow_pages_unowned is null because the inspector does not scan unclassified pages for orphan overflow signatures in Phase 3"
            .to_string(),
    );
    if table_data_bytes > 0 {
        warnings.push(
            "table_data_bytes currently reflects overflow-backed table payload allocation; overflow_bytes_total is a page-mechanism total and is not additive with category bytes"
                .to_string(),
        );
    }

    Ok(StorageInspection {
        db_path: db_path.display().to_string(),
        wal_path: wal_path.display().to_string(),
        page_size: header.page_size,
        page_count,
        db_file_bytes,
        wal_file_bytes,
        page_counts,
        bytes,
        overflow: OverflowBreakdown {
            manifest_payload_logical_bytes,
            table_payload_logical_bytes,
            overflow_pages_total: overflow_pages_total.len() as u32,
            overflow_pages_unowned: None,
        },
        precision,
        warnings,
        tables,
    })
}

fn read_header(db: &mut File) -> Result<HeaderInfo> {
    db.seek(SeekFrom::Start(0))
        .context("seek database header")?;
    let mut header = [0_u8; DB_HEADER_SIZE];
    db.read_exact(&mut header)
        .context("read database header bytes")?;
    if header[0..DB_MAGIC.len()] != DB_MAGIC {
        return Err(anyhow!("database header magic is invalid"));
    }
    let page_size = u32::from_le_bytes(
        header[PAGE_SIZE_OFFSET..PAGE_SIZE_OFFSET + 4]
            .try_into()
            .expect("page size slice"),
    );
    if page_size == 0 {
        return Err(anyhow!("database page size decoded as 0"));
    }
    Ok(HeaderInfo {
        page_size,
        catalog_root_page_id: u32::from_le_bytes(
            header[CATALOG_ROOT_OFFSET..CATALOG_ROOT_OFFSET + 4]
                .try_into()
                .expect("catalog root slice"),
        ),
        freelist_head_page_id: u32::from_le_bytes(
            header[FREELIST_HEAD_OFFSET..FREELIST_HEAD_OFFSET + 4]
                .try_into()
                .expect("freelist head slice"),
        ),
        freelist_page_count: u32::from_le_bytes(
            header[FREELIST_COUNT_OFFSET..FREELIST_COUNT_OFFSET + 4]
                .try_into()
                .expect("freelist count slice"),
        ),
    })
}

fn read_page(db: &mut File, page_id: u32, page_size: u32, page_count: u32) -> Result<Vec<u8>> {
    if page_id == 0 || page_id > page_count {
        return Err(anyhow!(
            "page id {} is outside valid range 1..={}",
            page_id,
            page_count
        ));
    }
    let offset = u64::from(page_id - 1).saturating_mul(u64::from(page_size));
    db.seek(SeekFrom::Start(offset))
        .with_context(|| format!("seek to page {page_id} at offset {offset}"))?;
    let mut page = vec![0_u8; page_size as usize];
    db.read_exact(&mut page)
        .with_context(|| format!("read page {}", page_id))?;
    Ok(page)
}

fn read_overflow_chain(
    db: &mut File,
    pointer: OverflowPointer,
    page_size: u32,
    page_count: u32,
    warnings: &mut Vec<String>,
) -> Result<OverflowChainRead> {
    if pointer.flags & OVERFLOW_FLAG_COMPRESSED != 0 {
        warnings.push(
            "overflow pointer indicates compressed payload; logical payload decoding is not attempted in inspector"
                .to_string(),
        );
    }

    let mut current = pointer.head_page_id;
    let chunk_capacity = (page_size as usize).saturating_sub(OVERFLOW_HEADER_SIZE);
    if chunk_capacity == 0 {
        return Err(anyhow!(
            "page size {} is too small for overflow pages",
            page_size
        ));
    }

    let mut visited = BTreeSet::new();
    let mut pages = Vec::new();
    let mut payload = Vec::new();

    while current != 0 {
        if !visited.insert(current) {
            warnings.push(format!(
                "overflow chain loop detected at page {}; traversal stopped",
                current
            ));
            break;
        }
        if current > page_count {
            warnings.push(format!(
                "overflow page {} exceeds page_count {}; traversal stopped",
                current, page_count
            ));
            break;
        }
        let page = read_page(db, current, page_size, page_count)
            .with_context(|| format!("read overflow page {current}"))?;
        pages.push(current);

        let next = read_u32_from_page(&page, 0)?;
        let chunk_len = read_u32_from_page(&page, 4)? as usize;
        if chunk_len > chunk_capacity {
            warnings.push(format!(
                "overflow page {} chunk length {} exceeds page capacity {}; clamped",
                current, chunk_len, chunk_capacity
            ));
        }
        let clamped_len = chunk_len.min(chunk_capacity);
        payload.extend_from_slice(
            page.get(OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + clamped_len)
                .ok_or_else(|| anyhow!("overflow page {} is truncated", current))?,
        );

        current = next;
        if payload.len() >= pointer.logical_len as usize {
            break;
        }
    }

    if payload.len() < pointer.logical_len as usize {
        warnings.push(format!(
            "overflow chain returned {} payload bytes but logical length is {}",
            payload.len(),
            pointer.logical_len
        ));
    }
    payload.truncate(pointer.logical_len as usize);

    Ok(OverflowChainRead {
        page_ids: pages,
        payload,
    })
}

fn read_freelist_chain(
    db: &mut File,
    head_page_id: u32,
    page_size: u32,
    page_count: u32,
    warnings: &mut Vec<String>,
) -> Result<Vec<u32>> {
    let mut pages = Vec::new();
    let mut visited = BTreeSet::new();
    let mut current = head_page_id;

    while current != 0 {
        if !visited.insert(current) {
            warnings.push(format!(
                "freelist loop detected at page {}; traversal stopped",
                current
            ));
            break;
        }
        if current > page_count {
            warnings.push(format!(
                "freelist page {} exceeds page_count {}; traversal stopped",
                current, page_count
            ));
            break;
        }
        let page = read_page(db, current, page_size, page_count)
            .with_context(|| format!("read freelist page {current}"))?;
        pages.push(current);
        current = read_u32_from_page(&page, 0)?;
    }

    Ok(pages)
}

fn parse_manifest_payload(payload: &[u8], warnings: &mut Vec<String>) -> Result<ParsedManifest> {
    if payload.is_empty() {
        return Ok(ParsedManifest {
            table_pointers: Vec::new(),
        });
    }

    let mut cursor = Cursor::new(payload);
    let magic = cursor.read_bytes(MANIFEST_PAYLOAD_MAGIC.len())?;
    if magic.as_slice() != MANIFEST_PAYLOAD_MAGIC {
        warnings.push(
            "manifest payload magic did not match DDBMANF1; table attribution is unavailable"
                .to_string(),
        );
        return Ok(ParsedManifest {
            table_pointers: Vec::new(),
        });
    }

    let _schema_cookie = cursor.read_u32()?;
    let table_count = cursor.read_u32()?;
    let mut table_pointers = Vec::with_capacity(table_count as usize);

    for _ in 0..table_count {
        let table_name = cursor.read_string()?;
        let column_count = cursor.read_u32()?;
        for _ in 0..column_count {
            let _column_name = cursor.read_string()?;
            let _column_type = cursor.read_u8()?;
            let _nullable = cursor.read_bool()?;
            let _default_sql = cursor.read_optional_string()?;
            let _primary_key = cursor.read_bool()?;
            let _unique = cursor.read_bool()?;
            let _auto_increment = cursor.read_bool()?;
            let check_count = cursor.read_u32()?;
            for _ in 0..check_count {
                let _check_name = cursor.read_optional_string()?;
                let _check_expr = cursor.read_string()?;
            }
            if cursor.read_bool()? {
                cursor.skip_foreign_key()?;
            }
        }

        let table_check_count = cursor.read_u32()?;
        for _ in 0..table_check_count {
            let _check_name = cursor.read_optional_string()?;
            let _check_expr = cursor.read_string()?;
        }

        let fk_count = cursor.read_u32()?;
        for _ in 0..fk_count {
            cursor.skip_foreign_key()?;
        }

        let _primary_key_columns = cursor.read_strings()?;
        let _next_row_id = cursor.read_i64()?;
        let _checksum = cursor.read_u32()?;
        let head_page_id = cursor.read_u32()?;
        let logical_len = cursor.read_u32()?;
        let flags = cursor.read_u8()?;

        table_pointers.push((
            table_name,
            OverflowPointer {
                head_page_id,
                logical_len,
                flags,
            },
        ));
    }

    Ok(ParsedManifest { table_pointers })
}

fn wal_path_for_db(db_path: &Path) -> PathBuf {
    let mut path = db_path.as_os_str().to_os_string();
    path.push(".wal");
    PathBuf::from(path)
}

fn read_u32_from_page(page: &[u8], offset: usize) -> Result<u32> {
    let bytes = page
        .get(offset..offset + 4)
        .ok_or_else(|| anyhow!("page is truncated at u32 offset {}", offset))?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 slice")))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| anyhow!("cursor overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| anyhow!("manifest payload is truncated"))?;
        self.offset = end;
        Ok(bytes.to_vec())
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| anyhow!("manifest payload is truncated"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_u8()? != 0)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes(
            bytes.as_slice().try_into().expect("u32"),
        ))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_bytes(8)?;
        Ok(i64::from_le_bytes(
            bytes.as_slice().try_into().expect("i64"),
        ))
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes).context("manifest string is not valid UTF-8")
    }

    fn read_optional_string(&mut self) -> Result<Option<String>> {
        if self.read_bool()? {
            Ok(Some(self.read_string()?))
        } else {
            Ok(None)
        }
    }

    fn read_strings(&mut self) -> Result<Vec<String>> {
        let len = self.read_u32()? as usize;
        (0..len).map(|_| self.read_string()).collect()
    }

    fn skip_foreign_key(&mut self) -> Result<()> {
        let _name = self.read_optional_string()?;
        let _columns = self.read_strings()?;
        let _referenced_table = self.read_string()?;
        let _referenced_columns = self.read_strings()?;
        let _on_delete = self.read_u8()?;
        let _on_update = self.read_u8()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::inspect_db_file;
    use decentdb::{Db, DbConfig};

    #[test]
    fn inspect_generated_db_reports_page_accounting() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("inspect.ddb");
        let db = Db::open_or_create(&db_path, DbConfig::default()).expect("open db");
        db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, payload TEXT NOT NULL)")
            .expect("create table");
        db.execute("INSERT INTO t (id, payload) VALUES (1, 'x')")
            .expect("insert row");
        db.checkpoint().expect("checkpoint");
        drop(db);

        let report = inspect_db_file(&db_path).expect("inspect storage");
        assert!(report.page_count >= 2);
        assert_eq!(report.bytes.db_total_bytes, report.db_file_bytes);
        assert_eq!(
            report.page_counts.metadata_pages
                + report.page_counts.manifest_overflow_pages
                + report.page_counts.table_overflow_pages
                + report.page_counts.freelist_pages
                + report.page_counts.unknown_pages,
            report.page_counts.total_pages
        );
        assert!(report.bytes.index_bytes.is_none());
        assert_eq!(report.overflow.overflow_pages_unowned, None);
    }
}
