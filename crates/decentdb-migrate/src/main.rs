use anyhow::{anyhow, Result};
use clap::Parser;
use decentdb::Db;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

mod v3;

const DB_HEADER_SIZE: usize = 128;
const FORMAT_VERSION_OFFSET: usize = 16;
const CHECKSUM_OFFSET: usize = 24;

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

    // Check for uncheckpointed WAL
    let mut wal_path = source_path.clone();
    wal_path.set_extension("db-wal");
    if wal_path.exists() {
        return Err(anyhow!(
            "A legacy WAL file was found at {}. Please run the old engine to checkpoint the database before migrating.",
            wal_path.display()
        ));
    }

    let dest_path = PathBuf::from(&args.dest);
    if dest_path.exists() {
        return Err(anyhow!(
            "Destination file already exists. Please provide a path to a non-existent file: {}",
            args.dest
        ));
    }

    // Determine the source format version
    let header = Db::read_header_info(&args.source)
        .map_err(|e| anyhow!("Failed to read source database header: {}", e))?;

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
            println!("Detected Nim-era Version 3 format.");
            let dest_db =
                Db::open_or_create(&args.dest, decentdb::DbConfig::default()).map_err(|e| {
                    anyhow!("Failed to create destination database {}: {}", args.dest, e)
                })?;
            let mut reader = v3::V3Reader::new(&args.source)?;
            reader.migrate_into(&dest_db)?;
        }
        8 => {
            println!("Detected DecentDB Version 8 format.");
            migrate_v8_file(&source_path, &dest_path)?;
        }
        9 => {
            println!("Detected DecentDB Version 9 format.");
            migrate_v9_file(&source_path, &dest_path)?;
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

fn migrate_v8_file(source: &PathBuf, dest: &PathBuf) -> Result<()> {
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

fn migrate_v9_file(source: &PathBuf, dest: &PathBuf) -> Result<()> {
    copy_file_and_patch_format_version(source, dest, 9)
}

fn copy_file_and_patch_format_version(
    source: &PathBuf,
    dest: &PathBuf,
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

fn patch_header_format_version(header: &mut [u8; DB_HEADER_SIZE], format_version: u32) {
    header[FORMAT_VERSION_OFFSET..FORMAT_VERSION_OFFSET + 4]
        .copy_from_slice(&format_version.to_le_bytes());
    header[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].fill(0);
    let checksum = crc32c_parts(&[&header[..CHECKSUM_OFFSET], &header[CHECKSUM_OFFSET + 4..]]);
    header[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].copy_from_slice(&checksum.to_le_bytes());
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
    use super::{migrate_v8_file, migrate_v9_file, patch_header_format_version};
    use decentdb::{Db, DbConfig, DB_FORMAT_VERSION};
    use tempfile::TempDir;

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

        let mut bytes = std::fs::read(&source).expect("read source");
        let mut header = [0_u8; super::DB_HEADER_SIZE];
        header.copy_from_slice(&bytes[..super::DB_HEADER_SIZE]);
        patch_header_format_version(&mut header, 8);
        bytes[..super::DB_HEADER_SIZE].copy_from_slice(&header);
        std::fs::write(&source, bytes).expect("write v8 header");

        migrate_v8_file(&source, &dest).expect("migrate v8 file");
        let header = Db::read_header_info(&dest).expect("read migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);

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

        let mut bytes = std::fs::read(&source).expect("read source");
        let mut header = [0_u8; super::DB_HEADER_SIZE];
        header.copy_from_slice(&bytes[..super::DB_HEADER_SIZE]);
        patch_header_format_version(&mut header, 8);
        bytes[..super::DB_HEADER_SIZE].copy_from_slice(&header);
        std::fs::write(&source, bytes).expect("write v8 header");

        migrate_v8_file(&source, &dest).expect("migrate v8 file");

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

        let mut bytes = std::fs::read(&source).expect("read source");
        let mut header = [0_u8; super::DB_HEADER_SIZE];
        header.copy_from_slice(&bytes[..super::DB_HEADER_SIZE]);
        patch_header_format_version(&mut header, 9);
        bytes[..super::DB_HEADER_SIZE].copy_from_slice(&header);
        std::fs::write(&source, bytes).expect("write v9 header");

        migrate_v9_file(&source, &dest).expect("migrate v9 file");
        let header = Db::read_header_info(&dest).expect("read migrated header");
        assert_eq!(header.format_version, DB_FORMAT_VERSION);

        let reopened = Db::open_or_create(&dest, DbConfig::default()).expect("open migrated db");
        let result = reopened
            .execute("SELECT val FROM t WHERE id = 1")
            .expect("query migrated row");
        assert_eq!(
            result.rows()[0].values()[0],
            decentdb::Value::Text("alpha".to_string())
        );
    }
}
