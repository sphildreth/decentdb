use anyhow::{anyhow, Result};
use clap::Parser;
use decentdb::Db;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

mod v3;

const DB_HEADER_SIZE: usize = 128;
const FORMAT_VERSION_OFFSET: usize = 16;
const CHECKSUM_OFFSET: usize = 24;
const DATABASE_ID_OFFSET: usize = 56;
const DATABASE_ID_LEN: usize = 16;
const HEADER_PAGE_ID: u32 = 1;
const WAL_HEADER_SIZE: usize = 32;
const WAL_MAGIC: [u8; 8] = *b"DDBWAL01";
const WAL_VERSION_OFFSET: usize = 8;
const WAL_PAGE_SIZE_OFFSET: usize = 12;
const WAL_END_OFFSET: usize = 16;
const WAL_FRAME_HEADER_SIZE: usize = 5;
const WAL_FRAME_TRAILER_SIZE: usize = 8;
const WAL_DELTA_FRAME_PAYLOAD_SIZE: usize = 512;
const WAL_FRAME_PAGE: u8 = 0;
const WAL_FRAME_COMMIT: u8 = 1;
const WAL_FRAME_CHECKPOINT: u8 = 2;
const WAL_FRAME_PAGE_DELTA: u8 = 3;

/// DecentDB Migration Tool
///
/// Upgrades legacy DecentDB database formats to the current format version.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the legacy source database
    #[arg(short, long)]
    source: String,

    /// Path to the destination database to create
    #[arg(short, long)]
    dest: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let source_path = PathBuf::from(&args.source);
    if !source_path.exists() {
        return Err(anyhow!("Source file not found: {}", args.source));
    }

    let dest_path = PathBuf::from(&args.dest);
    if dest_path.exists() {
        return Err(anyhow!(
            "Destination file already exists. Please provide a path to a non-existent file: {}",
            args.dest
        ));
    }
    let dest_wal_path = wal_path_for_db(&dest_path);
    if dest_wal_path.exists() {
        return Err(anyhow!(
            "Destination WAL file already exists. Please provide a destination whose sidecar is absent: {}",
            dest_wal_path.display()
        ));
    }

    // Determine the source format version
    let header = Db::read_header_info(&args.source)
        .map_err(|e| anyhow!("Failed to read source database header: {}", e))?;
    let source_wal_path = wal_path_for_db(&source_path);

    if header.format_version == decentdb::DB_FORMAT_VERSION {
        return Err(anyhow!(
            "Source database is already the current format version ({}). No migration needed.",
            decentdb::DB_FORMAT_VERSION
        ));
    }

    println!(
        "Migrating database from format version {} to {}...",
        header.format_version,
        decentdb::DB_FORMAT_VERSION
    );

    match header.format_version {
        // Support for Nim-era format version 3
        3 => {
            reject_wal_for_format(&source_wal_path, header.format_version)?;
            println!("Detected Nim-era Version 3 format.");
            let dest_db =
                Db::open_or_create(&args.dest, decentdb::DbConfig::default()).map_err(|e| {
                    anyhow!("Failed to create destination database {}: {}", args.dest, e)
                })?;
            let mut reader = v3::V3Reader::new(&args.source)?;
            reader.migrate_into(&dest_db)?;
        }
        8 => {
            reject_wal_for_format(&source_wal_path, header.format_version)?;
            println!("Detected DecentDB Version 8 format.");
            migrate_v8_file(&source_path, &dest_path)?;
        }
        9 => {
            reject_wal_for_format(&source_wal_path, header.format_version)?;
            println!("Detected DecentDB Version 9 format.");
            migrate_v9_file(&source_path, &dest_path)?;
        }
        10 => {
            println!("Detected DecentDB Version 10 format.");
            migrate_v10_file(&source_path, &dest_path)?;
        }
        11 => {
            println!("Detected DecentDB Version 11 format.");
            migrate_v11_file(&source_path, &dest_path)?;
        }
        12 => {
            println!("Detected DecentDB Version 12 format.");
            migrate_v12_file(&source_path, &dest_path)?;
        }
        13 => {
            println!("Detected DecentDB Version 13 format.");
            migrate_v13_file(&source_path, &dest_path)?;
        }
        _ => {
            return Err(anyhow!("Migration for format version {} is not supported by this version of decentdb-migrate.", header.format_version));
        }
    }

    println!(
        "Migration complete! Your upgraded database is ready at: {}",
        args.dest
    );

    Ok(())
}

fn wal_path_for_db(db_path: &Path) -> PathBuf {
    let mut wal_path = db_path.as_os_str().to_os_string();
    wal_path.push(".wal");
    PathBuf::from(wal_path)
}

fn reject_wal_for_format(wal_path: &Path, format_version: u32) -> Result<()> {
    if wal_path.exists() {
        return Err(anyhow!(
            "A legacy WAL file was found at {}. Migration with an existing WAL is only supported for format 10 to {}. Please run the old engine to checkpoint format {} before migrating.",
            wal_path.display(),
            decentdb::DB_FORMAT_VERSION,
            format_version
        ));
    }
    Ok(())
}

fn migrate_v8_file(source: &Path, dest: &Path) -> Result<()> {
    copy_file_and_patch_format_version(source, dest, 8)?;

    let config = decentdb::DbConfig {
        persistent_pk_index: true,
        ..decentdb::DbConfig::default()
    };
    drop(Db::open_or_create(dest, config).map_err(|error| {
        anyhow!(
            "Failed to finalize persistent primary-key indexes for {}: {}",
            dest.display(),
            error
        )
    })?);
    Ok(())
}

fn migrate_v9_file(source: &Path, dest: &Path) -> Result<()> {
    copy_file_and_patch_format_version(source, dest, 9)
}

fn migrate_v10_file(source: &Path, dest: &Path) -> Result<()> {
    copy_file_and_patch_format_version(source, dest, 10)?;
    copy_wal_sidecar_if_present(source, dest)
}

fn migrate_v11_file(source: &Path, dest: &Path) -> Result<()> {
    copy_file_and_patch_format_version(source, dest, 11)?;
    copy_wal_sidecar_if_present(source, dest)
}

fn migrate_v12_file(source: &Path, dest: &Path) -> Result<()> {
    copy_file_and_patch_format_version(source, dest, 12)
}

fn migrate_v13_file(source: &Path, dest: &Path) -> Result<()> {
    // Format 13 -> 14 introduced resident-table delete tombstones (ADR 0200).
    // Version-13 payloads never contain tombstone slots, so the version-14
    // engine reads them unchanged; the migration is a header-version patch plus
    // WAL-sidecar carry-forward, matching the version-10/11 precedent.
    copy_file_and_patch_format_version(source, dest, 13)?;
    copy_wal_sidecar_if_present(source, dest)
}

fn copy_file_and_patch_format_version(
    source: &Path,
    dest: &Path,
    expected_format_version: u32,
) -> Result<()> {
    std::fs::copy(source, dest).map_err(|error| {
        anyhow!(
            "Failed to copy source database {} to {}: {}",
            source.display(),
            dest.display(),
            error
        )
    })?;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dest)
        .map_err(|error| {
            anyhow!(
                "Failed to open destination database {}: {}",
                dest.display(),
                error
            )
        })?;
    let mut header = [0_u8; DB_HEADER_SIZE];
    file.read_exact(&mut header).map_err(|error| {
        anyhow!(
            "Failed to read destination database header {}: {}",
            dest.display(),
            error
        )
    })?;
    let format_version = u32::from_le_bytes(
        header[FORMAT_VERSION_OFFSET..FORMAT_VERSION_OFFSET + 4]
            .try_into()
            .expect("format version bytes"),
    );
    if format_version != expected_format_version {
        return Err(anyhow!(
            "Expected copied source to be format {}, found format {}",
            expected_format_version,
            format_version
        ));
    }
    patch_header_format_version(&mut header, decentdb::DB_FORMAT_VERSION);
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.write_all(&header))
        .map_err(|error| {
            anyhow!(
                "Failed to rewrite destination database header {}: {}",
                dest.display(),
                error
            )
        })?;
    file.flush().map_err(|error| {
        anyhow!(
            "Failed to flush upgraded database header {}: {}",
            dest.display(),
            error
        )
    })?;
    Ok(())
}

fn copy_wal_sidecar_if_present(source: &Path, dest: &Path) -> Result<()> {
    let source_wal = wal_path_for_db(source);
    if !source_wal.exists() {
        return Ok(());
    }

    let dest_wal = wal_path_for_db(dest);
    if dest_wal.exists() {
        return Err(anyhow!(
            "Destination WAL file already exists. Please provide a destination whose sidecar is absent: {}",
            dest_wal.display()
        ));
    }

    std::fs::copy(&source_wal, &dest_wal).map_err(|error| {
        anyhow!(
            "Failed to copy source WAL {} to {}: {}",
            source_wal.display(),
            dest_wal.display(),
            error
        )
    })?;
    // Page 1 can appear in the WAL. Patch those frame payloads too, or a
    // later checkpoint would write a legacy v10 header back over the migrated
    // main database header.
    patch_wal_header_pages(&dest_wal, dest)?;
    Ok(())
}

fn patch_wal_header_pages(wal_path: &Path, db_path: &Path) -> Result<usize> {
    let mut wal = OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal_path)
        .map_err(|error| {
            anyhow!(
                "Failed to open copied WAL {} for header migration: {}",
                wal_path.display(),
                error
            )
        })?;

    let mut wal_header = [0_u8; WAL_HEADER_SIZE];
    wal.read_exact(&mut wal_header).map_err(|error| {
        anyhow!(
            "Failed to read copied WAL header {}: {}",
            wal_path.display(),
            error
        )
    })?;

    if wal_header[..WAL_MAGIC.len()] != WAL_MAGIC {
        return Err(anyhow!(
            "Invalid WAL header magic in {}",
            wal_path.display()
        ));
    }

    let wal_version = read_u32(&wal_header, WAL_VERSION_OFFSET);
    if wal_version != 1 && wal_version != 2 {
        return Err(anyhow!(
            "Unsupported WAL header version {} in {}",
            wal_version,
            wal_path.display()
        ));
    }

    let page_size = read_u32(&wal_header, WAL_PAGE_SIZE_OFFSET) as usize;
    let wal_end = read_u64(&wal_header, WAL_END_OFFSET);
    let wal_len = wal
        .metadata()
        .map(|metadata| metadata.len())
        .map_err(|error| {
            anyhow!(
                "Failed to read copied WAL metadata {}: {}",
                wal_path.display(),
                error
            )
        })?;
    if wal_end > wal_len {
        return Err(anyhow!(
            "WAL logical end offset {} exceeds file size {} in {}",
            wal_end,
            wal_len,
            wal_path.display()
        ));
    }

    let mut db = OpenOptions::new()
        .read(true)
        .open(db_path)
        .map_err(|error| {
            anyhow!(
                "Failed to open migrated database {} for WAL header migration: {}",
                db_path.display(),
                error
            )
        })?;
    let mut header_page = vec![0_u8; page_size];
    db.read_exact(&mut header_page).map_err(|error| {
        anyhow!(
            "Failed to read migrated database header page {}: {}",
            db_path.display(),
            error
        )
    })?;
    let migrated_database_id =
        read_database_id_from_header_page(&header_page).ok_or_else(|| {
            anyhow!(
                "Migrated database header page {} is too short to read coordination identity",
                db_path.display()
            )
        })?;

    let mut patched = 0_usize;
    let mut offset = WAL_HEADER_SIZE as u64;
    while offset < wal_end {
        let mut frame_header = [0_u8; WAL_FRAME_HEADER_SIZE];
        wal.seek(SeekFrom::Start(offset))
            .and_then(|_| wal.read_exact(&mut frame_header))
            .map_err(|error| {
                anyhow!(
                    "Failed to read WAL frame header at offset {} in {}: {}",
                    offset,
                    wal_path.display(),
                    error
                )
            })?;
        let frame_type = frame_header[0];
        let page_id = u32::from_le_bytes([
            frame_header[1],
            frame_header[2],
            frame_header[3],
            frame_header[4],
        ]);
        let payload_len = wal_frame_payload_len(frame_type, page_size)?;
        let frame_len = WAL_FRAME_HEADER_SIZE
            .checked_add(payload_len)
            .and_then(|len| len.checked_add(WAL_FRAME_TRAILER_SIZE))
            .ok_or_else(|| anyhow!("WAL frame length overflow in {}", wal_path.display()))?;
        if offset + frame_len as u64 > wal_end {
            return Err(anyhow!(
                "WAL frame at offset {} overruns logical end {} in {}",
                offset,
                wal_end,
                wal_path.display()
            ));
        }

        let payload_offset = offset + WAL_FRAME_HEADER_SIZE as u64;
        match (frame_type, page_id) {
            (WAL_FRAME_PAGE, HEADER_PAGE_ID) => {
                let mut page = vec![0_u8; page_size];
                wal.seek(SeekFrom::Start(payload_offset))
                    .and_then(|_| wal.read_exact(&mut page))
                    .map_err(|error| {
                        anyhow!(
                            "Failed to read WAL header-page frame at offset {} in {}: {}",
                            offset,
                            wal_path.display(),
                            error
                        )
                    })?;
                patch_page_header_format_version(
                    &mut page,
                    decentdb::DB_FORMAT_VERSION,
                    Some(&migrated_database_id),
                )?;
                wal.seek(SeekFrom::Start(payload_offset))
                    .and_then(|_| wal.write_all(&page))
                    .map_err(|error| {
                        anyhow!(
                            "Failed to rewrite WAL header-page frame at offset {} in {}: {}",
                            offset,
                            wal_path.display(),
                            error
                        )
                    })?;
                header_page = page;
                patched += 1;
            }
            (WAL_FRAME_PAGE_DELTA, HEADER_PAGE_ID) => {
                let mut payload = [0_u8; WAL_DELTA_FRAME_PAYLOAD_SIZE];
                wal.seek(SeekFrom::Start(payload_offset))
                    .and_then(|_| wal.read_exact(&mut payload))
                    .map_err(|error| {
                        anyhow!(
                            "Failed to read WAL header-page delta at offset {} in {}: {}",
                            offset,
                            wal_path.display(),
                            error
                        )
                    })?;
                let mut updated_page = header_page.clone();
                apply_page_delta_in_place(&mut updated_page, &payload)?;
                patch_page_header_format_version(
                    &mut updated_page,
                    decentdb::DB_FORMAT_VERSION,
                    Some(&migrated_database_id),
                )?;
                let patched_delta = encode_page_delta(&header_page, &updated_page)?;
                wal.seek(SeekFrom::Start(payload_offset))
                    .and_then(|_| wal.write_all(&patched_delta))
                    .map_err(|error| {
                        anyhow!(
                            "Failed to rewrite WAL header-page delta at offset {} in {}: {}",
                            offset,
                            wal_path.display(),
                            error
                        )
                    })?;
                header_page = updated_page;
                patched += 1;
            }
            _ => {}
        }

        offset += frame_len as u64;
    }

    wal.flush().map_err(|error| {
        anyhow!(
            "Failed to flush patched copied WAL {}: {}",
            wal_path.display(),
            error
        )
    })?;
    Ok(patched)
}

fn wal_frame_payload_len(frame_type: u8, page_size: usize) -> Result<usize> {
    match frame_type {
        WAL_FRAME_PAGE => Ok(page_size),
        WAL_FRAME_COMMIT => Ok(0),
        WAL_FRAME_CHECKPOINT => Ok(8),
        WAL_FRAME_PAGE_DELTA => Ok(WAL_DELTA_FRAME_PAYLOAD_SIZE),
        _ => Err(anyhow!("Unknown WAL frame type {}", frame_type)),
    }
}

fn read_database_id_from_header_page(page: &[u8]) -> Option<[u8; DATABASE_ID_LEN]> {
    if page.len() < DATABASE_ID_OFFSET + DATABASE_ID_LEN {
        return None;
    }
    let mut database_id = [0_u8; DATABASE_ID_LEN];
    database_id.copy_from_slice(&page[DATABASE_ID_OFFSET..DATABASE_ID_OFFSET + DATABASE_ID_LEN]);
    Some(database_id)
}

fn patch_page_header_format_version(
    page: &mut [u8],
    format_version: u32,
    database_id: Option<&[u8; DATABASE_ID_LEN]>,
) -> Result<()> {
    if page.len() < DB_HEADER_SIZE {
        return Err(anyhow!(
            "Header page is shorter than fixed database header: {} bytes",
            page.len()
        ));
    }
    let mut header = [0_u8; DB_HEADER_SIZE];
    header.copy_from_slice(&page[..DB_HEADER_SIZE]);
    patch_header_format_version_with_database_id(&mut header, format_version, database_id);
    page[..DB_HEADER_SIZE].copy_from_slice(&header);
    Ok(())
}

fn apply_page_delta_in_place(
    page: &mut [u8],
    payload: &[u8; WAL_DELTA_FRAME_PAYLOAD_SIZE],
) -> Result<()> {
    let patch_count =
        u16::from_le_bytes(payload[..2].try_into().expect("delta patch count bytes")) as usize;

    let mut cursor = 2_usize;
    for _ in 0..patch_count {
        let (offset, len, next_cursor) = decode_delta_patch_header(payload, cursor)?;
        if next_cursor + len > payload.len() {
            return Err(anyhow!("WAL delta patch bytes overrun payload"));
        }
        if offset + len > page.len() {
            return Err(anyhow!("WAL delta patch writes past page end"));
        }
        cursor = next_cursor + len;
    }

    let mut cursor = 2_usize;
    for _ in 0..patch_count {
        let (offset, len, next_cursor) = decode_delta_patch_header(payload, cursor)?;
        let bytes_end = next_cursor + len;
        page[offset..offset + len].copy_from_slice(&payload[next_cursor..bytes_end]);
        cursor = bytes_end;
    }
    Ok(())
}

fn decode_delta_patch_header(
    payload: &[u8; WAL_DELTA_FRAME_PAYLOAD_SIZE],
    cursor: usize,
) -> Result<(usize, usize, usize)> {
    if cursor + 4 > payload.len() {
        return Err(anyhow!("WAL delta patch header overruns payload"));
    }
    let offset = u16::from_le_bytes(
        payload[cursor..cursor + 2]
            .try_into()
            .expect("delta patch offset bytes"),
    ) as usize;
    let len = u16::from_le_bytes(
        payload[cursor + 2..cursor + 4]
            .try_into()
            .expect("delta patch length bytes"),
    ) as usize;
    Ok((offset, len, cursor + 4))
}

fn encode_page_delta(base: &[u8], updated: &[u8]) -> Result<[u8; WAL_DELTA_FRAME_PAYLOAD_SIZE]> {
    if base.len() != updated.len() {
        return Err(anyhow!(
            "Cannot encode WAL delta for pages with different lengths: {} != {}",
            base.len(),
            updated.len()
        ));
    }

    let mut payload = Vec::with_capacity(WAL_DELTA_FRAME_PAYLOAD_SIZE);
    payload.extend_from_slice(&0_u16.to_le_bytes());
    let mut patch_count = 0_u16;
    let mut index = 0_usize;
    while index < updated.len() {
        if base[index] == updated[index] {
            index += 1;
            continue;
        }

        let start = index;
        index += 1;
        while index < updated.len() && base[index] != updated[index] {
            index += 1;
        }
        let len = index - start;
        if payload.len() + 4 + len > WAL_DELTA_FRAME_PAYLOAD_SIZE {
            return Err(anyhow!(
                "Cannot rewrite header-page WAL delta because the patched delta exceeds {} bytes",
                WAL_DELTA_FRAME_PAYLOAD_SIZE
            ));
        }

        let offset = u16::try_from(start)
            .map_err(|_| anyhow!("WAL delta patch offset {} does not fit u16", start))?;
        let patch_len = u16::try_from(len)
            .map_err(|_| anyhow!("WAL delta patch length {} does not fit u16", len))?;
        payload.extend_from_slice(&offset.to_le_bytes());
        payload.extend_from_slice(&patch_len.to_le_bytes());
        payload.extend_from_slice(&updated[start..index]);
        patch_count = patch_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("WAL delta patch count overflow"))?;
    }

    payload[..2].copy_from_slice(&patch_count.to_le_bytes());
    payload.resize(WAL_DELTA_FRAME_PAYLOAD_SIZE, 0);
    let mut encoded = [0_u8; WAL_DELTA_FRAME_PAYLOAD_SIZE];
    encoded.copy_from_slice(&payload);
    Ok(encoded)
}

fn patch_header_format_version(header: &mut [u8; DB_HEADER_SIZE], format_version: u32) {
    patch_header_format_version_with_database_id(header, format_version, None);
}

fn patch_header_format_version_with_database_id(
    header: &mut [u8; DB_HEADER_SIZE],
    format_version: u32,
    database_id: Option<&[u8; DATABASE_ID_LEN]>,
) {
    header[FORMAT_VERSION_OFFSET..FORMAT_VERSION_OFFSET + 4]
        .copy_from_slice(&format_version.to_le_bytes());
    if format_version >= 13 {
        match database_id {
            Some(database_id) => {
                header[DATABASE_ID_OFFSET..DATABASE_ID_OFFSET + DATABASE_ID_LEN]
                    .copy_from_slice(database_id);
            }
            None => seed_header_database_id_if_empty(header),
        }
    }
    recompute_header_checksum(header);
}

fn seed_header_database_id_if_empty(header: &mut [u8; DB_HEADER_SIZE]) {
    let range = DATABASE_ID_OFFSET..DATABASE_ID_OFFSET + DATABASE_ID_LEN;
    if header[range.clone()].iter().any(|byte| *byte != 0) {
        return;
    }
    header[range].copy_from_slice(&random_database_id());
}

fn recompute_header_checksum(header: &mut [u8; DB_HEADER_SIZE]) {
    header[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
    let checksum = crc32c_parts(&[&header[..CHECKSUM_OFFSET], &header[CHECKSUM_OFFSET + 4..]]);
    header[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].copy_from_slice(&checksum.to_le_bytes());
}

fn random_database_id() -> [u8; DATABASE_ID_LEN] {
    let mut database_id = [0_u8; DATABASE_ID_LEN];
    if getrandom::fill(&mut database_id).is_ok() && database_id != [0_u8; DATABASE_ID_LEN] {
        return database_id;
    }
    let fallback = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(1)
        ^ u128::from(std::process::id());
    fallback.to_le_bytes()
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    let mut raw = [0_u8; 4];
    raw.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(raw)
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut raw = [0_u8; 8];
    raw.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(raw)
}

fn crc32c_parts(parts: &[&[u8]]) -> u32 {
    let mut crc = u32::MAX;
    for part in parts {
        crc = crc32c_update(crc, part);
    }
    !crc
}

fn crc32c_update(mut crc: u32, bytes: &[u8]) -> u32 {
    let table = build_crc32c_table();
    for byte in bytes {
        let table_index = ((crc ^ u32::from(*byte)) & 0xFF) as usize;
        crc = (crc >> 8) ^ table[table_index];
    }
    crc
}

const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0_usize;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0_u8;
        while bit < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use super::{
        migrate_v10_file, migrate_v11_file, migrate_v12_file, migrate_v13_file, migrate_v8_file,
        migrate_v9_file, patch_header_format_version, wal_path_for_db,
    };
    use decentdb::{Db, DbConfig, DB_FORMAT_VERSION};
    use std::path::Path;
    use tempfile::TempDir;

    #[test]
    fn wal_path_matches_engine_sidecar_convention() {
        assert_eq!(
            wal_path_for_db(Path::new("/tmp/example.ddb")),
            Path::new("/tmp/example.ddb.wal")
        );
        assert_eq!(
            wal_path_for_db(Path::new("/tmp/example.db")),
            Path::new("/tmp/example.db.wal")
        );
        assert_eq!(
            wal_path_for_db(Path::new("/tmp/example")),
            Path::new("/tmp/example.wal")
        );
    }

    fn rewrite_source_as_legacy_format(path: &Path, format_version: u32) {
        let mut bytes = std::fs::read(path).expect("read source");
        let mut header = [0_u8; super::DB_HEADER_SIZE];
        header.copy_from_slice(&bytes[..super::DB_HEADER_SIZE]);
        header[super::DATABASE_ID_OFFSET..super::DATABASE_ID_OFFSET + super::DATABASE_ID_LEN]
            .fill(0);
        patch_header_format_version(&mut header, format_version);
        bytes[..super::DB_HEADER_SIZE].copy_from_slice(&header);
        std::fs::write(path, bytes).expect("write legacy header");
    }

    fn assert_database_id_nonzero(path: &Path) -> [u8; super::DATABASE_ID_LEN] {
        let bytes = std::fs::read(path).expect("read database bytes");
        let mut database_id = [0_u8; super::DATABASE_ID_LEN];
        database_id.copy_from_slice(
            &bytes[super::DATABASE_ID_OFFSET..super::DATABASE_ID_OFFSET + super::DATABASE_ID_LEN],
        );
        assert!(
            database_id.iter().any(|byte| *byte != 0),
            "expected migrated database to have a nonzero coordination identity"
        );
        database_id
    }

    #[test]
    fn migrate_v8_copy_upgrades_header_version() {
        let tempdir = TempDir::new().expect("tempdir");
        let source = tempdir.path().join("source.ddb");
        let dest = tempdir.path().join("dest.ddb");

        let db = Db::open_or_create(&source, DbConfig::default()).expect("create source");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'alpha')")
            .expect("insert row");
        db.checkpoint().expect("checkpoint");
        drop(db);

        rewrite_source_as_legacy_format(&source, 8);

        migrate_v8_file(&source, &dest).expect("migrate v8 file");
        let header = Db::read_header_info(&dest).expect("read migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);
        assert_database_id_nonzero(&dest);

        let reopened = Db::open_or_create(&dest, DbConfig::default()).expect("open migrated db");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("query migrated row");
        assert_eq!(
            result.rows()[0].values()[0],
            decentdb::Value::Text("alpha".to_string())
        );
    }

    #[test]
    fn migrate_v8_copy_backfills_persistent_pk_indexes_for_large_tables() {
        let tempdir = TempDir::new().expect("tempdir");
        let source = tempdir.path().join("source-large.ddb");
        let dest = tempdir.path().join("dest-large.ddb");

        let db = Db::open_or_create(&source, DbConfig::default()).expect("create source");
        db.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")
            .expect("create table");
        let large_body = "x".repeat(2048);
        for i in 0_i64..96_i64 {
            db.execute(&format!(
                "INSERT INTO docs (id, n, body) VALUES ({i}, {i}, '{}')",
                large_body
            ))
            .expect("insert row");
        }
        db.checkpoint().expect("checkpoint");
        drop(db);

        rewrite_source_as_legacy_format(&source, 8);

        migrate_v8_file(&source, &dest).expect("migrate v8 file");
        assert_database_id_nonzero(&dest);

        let config = DbConfig {
            persistent_pk_index: true,
            ..DbConfig::default()
        };
        let reopened = Db::open_or_create(&dest, config).expect("open migrated db");
        let json_open = reopened
            .inspect_storage_state_json()
            .expect("storage json at open");
        assert!(
            json_open.contains("\"deferred_table_count\":1"),
            "expected migrated table to stay deferred at open, got: {json_open}"
        );

        let result = reopened
            .execute("SELECT n FROM docs WHERE id = 17")
            .expect("query migrated row");
        assert_eq!(result.rows()[0].values()[0], decentdb::Value::Int64(17));

        let json_after = reopened
            .inspect_storage_state_json()
            .expect("storage json after point lookup");
        assert!(
            json_after.contains("\"loaded_table_count\":0"),
            "expected migrated point lookup to avoid materialization, got: {json_after}"
        );
        assert!(
            json_after.contains("\"deferred_table_count\":1"),
            "expected migrated table to remain deferred, got: {json_after}"
        );
    }

    #[test]
    fn migrate_v9_copy_upgrades_header_version() {
        let tempdir = TempDir::new().expect("tempdir");
        let source = tempdir.path().join("source-v9.ddb");
        let dest = tempdir.path().join("dest-v9.ddb");

        let db = Db::open_or_create(&source, DbConfig::default()).expect("create source");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'alpha')")
            .expect("insert row");
        db.checkpoint().expect("checkpoint");
        drop(db);

        rewrite_source_as_legacy_format(&source, 9);

        migrate_v9_file(&source, &dest).expect("migrate v9 file");
        let header = Db::read_header_info(&dest).expect("read migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);
        assert_database_id_nonzero(&dest);

        let reopened = Db::open_or_create(&dest, DbConfig::default()).expect("open migrated db");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("query migrated row");
        assert_eq!(
            result.rows()[0].values()[0],
            decentdb::Value::Text("alpha".to_string())
        );
    }

    #[test]
    fn migrate_v10_copy_upgrades_header_version() {
        let tempdir = TempDir::new().expect("tempdir");
        let source = tempdir.path().join("source-v10.ddb");
        let dest = tempdir.path().join("dest-v10.ddb");

        let db = Db::open_or_create(&source, DbConfig::default()).expect("create source");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'alpha')")
            .expect("insert row");
        db.checkpoint().expect("checkpoint");
        drop(db);

        rewrite_source_as_legacy_format(&source, 10);

        migrate_v10_file(&source, &dest).expect("migrate v10 file");
        let header = Db::read_header_info(&dest).expect("read migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);
        assert_database_id_nonzero(&dest);

        let reopened = Db::open_or_create(&dest, DbConfig::default()).expect("open migrated db");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("query migrated row");
        assert_eq!(
            result.rows()[0].values()[0],
            decentdb::Value::Text("alpha".to_string())
        );
    }

    #[test]
    fn migrate_v11_copy_upgrades_header_version() {
        let tempdir = TempDir::new().expect("tempdir");
        let source = tempdir.path().join("source-v11.ddb");
        let dest = tempdir.path().join("dest-v11.ddb");

        let db = Db::open_or_create(&source, DbConfig::default()).expect("create source");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'alpha')")
            .expect("insert row");
        db.checkpoint().expect("checkpoint");
        drop(db);

        rewrite_source_as_legacy_format(&source, 11);

        migrate_v11_file(&source, &dest).expect("migrate v11 file");
        let header = Db::read_header_info(&dest).expect("read migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);
        assert_database_id_nonzero(&dest);

        let reopened = Db::open_or_create(&dest, DbConfig::default()).expect("open migrated db");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("query migrated row");
        assert_eq!(
            result.rows()[0].values()[0],
            decentdb::Value::Text("alpha".to_string())
        );
    }

    #[test]
    fn migrate_v12_copy_upgrades_header_version() {
        let tempdir = TempDir::new().expect("tempdir");
        let source = tempdir.path().join("source-v12.ddb");
        let dest = tempdir.path().join("dest-v12.ddb");

        let db = Db::open_or_create(&source, DbConfig::default()).expect("create source");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'alpha')")
            .expect("insert row");
        db.checkpoint().expect("checkpoint");
        drop(db);

        rewrite_source_as_legacy_format(&source, 12);

        migrate_v12_file(&source, &dest).expect("migrate v12 file");
        let header = Db::read_header_info(&dest).expect("read migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);
        assert_database_id_nonzero(&dest);

        let reopened = Db::open_or_create(&dest, DbConfig::default()).expect("open migrated db");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("query migrated row");
        assert_eq!(
            result.rows()[0].values()[0],
            decentdb::Value::Text("alpha".to_string())
        );
    }

    #[test]
    fn migrate_v13_copy_upgrades_header_version() {
        let tempdir = TempDir::new().expect("tempdir");
        let source = tempdir.path().join("source-v13.ddb");
        let dest = tempdir.path().join("dest-v13.ddb");

        let db = Db::open_or_create(&source, DbConfig::default()).expect("create source");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'alpha')")
            .expect("insert row");
        db.execute("INSERT INTO t VALUES (2, 'beta')")
            .expect("insert row");
        db.execute("DELETE FROM t WHERE id = 2")
            .expect("delete row");
        db.checkpoint().expect("checkpoint");
        drop(db);

        rewrite_source_as_legacy_format(&source, 13);

        migrate_v13_file(&source, &dest).expect("migrate v13 file");
        let header = Db::read_header_info(&dest).expect("read migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);
        assert_database_id_nonzero(&dest);

        let reopened = Db::open_or_create(&dest, DbConfig::default()).expect("open migrated db");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("query migrated row");
        assert_eq!(
            result.rows()[0].values()[0],
            decentdb::Value::Text("alpha".to_string())
        );
        let deleted = reopened
            .execute("SELECT val FROM t WHERE id = 2")
            .expect("query deleted row");
        assert!(
            deleted.rows().is_empty(),
            "deleted row must remain absent after migration"
        );
    }

    #[test]
    fn migrate_v10_copy_carries_existing_wal_sidecar_forward() {
        let tempdir = TempDir::new().expect("tempdir");
        let source = tempdir.path().join("source-v10-with-wal.ddb");
        let dest = tempdir.path().join("dest-v10-with-wal.ddb");

        let mut config = DbConfig {
            background_checkpoint_worker: false,
            wal_checkpoint_threshold_pages: 0,
            wal_checkpoint_threshold_bytes: 0,
            ..DbConfig::default()
        };
        config.auto_checkpoint_on_open_mb = 0;

        let db = Db::open_or_create(&source, config).expect("create source");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .expect("create table");
        db.execute("INSERT INTO t VALUES (1, 'alpha')")
            .expect("insert row");
        drop(db);

        let source_wal = wal_path_for_db(&source);
        let source_wal_len = std::fs::metadata(&source_wal)
            .expect("source wal metadata")
            .len();
        assert!(
            source_wal_len > 32,
            "expected source WAL to contain uncheckpointed frames"
        );

        rewrite_source_as_legacy_format(&source, 10);

        migrate_v10_file(&source, &dest).expect("migrate v10 file with wal");

        let dest_wal = wal_path_for_db(&dest);
        let dest_wal_len = std::fs::metadata(&dest_wal)
            .expect("dest wal metadata")
            .len();
        assert_eq!(dest_wal_len, source_wal_len);

        let header = Db::read_header_info(&dest).expect("read migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);
        let migrated_database_id = assert_database_id_nonzero(&dest);

        let reopened = Db::open_or_create(&dest, DbConfig::default())
            .expect("open migrated db and recover wal");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("query migrated row");
        assert_eq!(
            result.rows()[0].values()[0],
            decentdb::Value::Text("alpha".to_string())
        );

        reopened
            .checkpoint()
            .expect("checkpoint migrated wal without reverting header");
        drop(reopened);

        let header = Db::read_header_info(&dest).expect("read checkpointed migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);
        assert_eq!(assert_database_id_nonzero(&dest), migrated_database_id);
        let checkpointed_wal_len = std::fs::metadata(&dest_wal)
            .expect("checkpointed wal metadata")
            .len();
        assert_eq!(checkpointed_wal_len, super::WAL_HEADER_SIZE as u64);

        let reopened = Db::open_or_create(&dest, DbConfig::default())
            .expect("reopen checkpointed migrated db");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("query checkpointed migrated row");
        assert_eq!(
            result.rows()[0].values()[0],
            decentdb::Value::Text("alpha".to_string())
        );
    }
}
