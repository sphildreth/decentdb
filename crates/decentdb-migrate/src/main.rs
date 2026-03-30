use anyhow::{anyhow, Result};
use clap::Parser;
use decentdb::Db;
use std::path::PathBuf;

mod v3;

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

    // Open destination database using the *current* engine
    let dest_db = Db::open_or_create(&args.dest, decentdb::DbConfig::default())
        .map_err(|e| anyhow!("Failed to create destination database {}: {}", args.dest, e))?;

    match header.format_version {
        // Support for Nim-era format version 3
        3 => {
            println!("Detected Nim-era Version 3 format.");
            let mut reader = v3::V3Reader::new(&args.source)?;
            reader.migrate_into(&dest_db)?;
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
