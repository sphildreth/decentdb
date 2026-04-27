//! Memory-behavior probe for DecentDB engine.
//!
//! Goal: prove (or disprove) that engine memory stays bounded for large
//! row-counts when the writer commits in modest batches, vs. one giant
//! transaction.
//!
//! Inserts N total rows into a simple `events(id, kind, payload)` table
//! and samples /proc/self/statm (RSS) at every checkpoint. Reports peak
//! RSS, RSS at end-of-load, RSS after a forced WAL checkpoint, and RSS
//! after dropping the Db handle.

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::Parser;
use decentdb::{DbConfig, Value};

#[derive(Parser, Debug)]
struct Args {
    /// Total rows to insert.
    #[arg(long, default_value_t = 10_000_000)]
    rows: u64,
    /// Rows per transaction (commit cadence).
    #[arg(long, default_value_t = 50_000)]
    batch: u64,
    /// Page-cache size in MB (DbConfig default = 4).
    #[arg(long, default_value_t = 4)]
    cache_mb: usize,
    /// Working directory for the probe DB.
    #[arg(long, default_value = "probe")]
    dir: PathBuf,
    /// Label for the report.
    #[arg(long, default_value = "probe")]
    label: String,
}

fn rss_breakdown() -> (u64, u64, u64) {
    // Returns (VmRSS, RssAnon, RssFile) in bytes from /proc/self/status.
    let s = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let mut vm_rss = 0u64;
    let mut rss_anon = 0u64;
    let mut rss_file = 0u64;
    for line in s.lines() {
        let parse_kb = |line: &str| -> u64 {
            line.split_whitespace()
                .nth(1)
                .and_then(|x| x.parse::<u64>().ok())
                .unwrap_or(0)
                * 1024
        };
        if line.starts_with("VmRSS:") {
            vm_rss = parse_kb(line);
        } else if line.starts_with("RssAnon:") {
            rss_anon = parse_kb(line);
        } else if line.starts_with("RssFile:") {
            rss_file = parse_kb(line);
        }
    }
    (vm_rss, rss_anon, rss_file)
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.dir).ok();
    let db_path = args.dir.join(format!("probe-{}.ddb", args.label));
    // best-effort cleanup
    for suffix in ["", "-wal", ".wal", "-shm", ".shm"] {
        let p = args.dir.join(format!("probe-{}.ddb{suffix}", args.label));
        let _ = fs::remove_file(&p);
    }

    println!(
        "probe label={} rows={} batch={} cache_mb={}",
        args.label, args.rows, args.batch, args.cache_mb
    );

    let mut peak: u64 = 0;
    let mut sample = |label: &str| {
        let (vm, anon, file) = rss_breakdown();
        if vm > peak {
            peak = vm;
        }
        println!(
            "  RSS {label:<28} VmRSS {:>9.1} MB  Anon {:>9.1} MB  File {:>9.1} MB   (peak VmRSS {:>9.1} MB)",
            mb(vm), mb(anon), mb(file), mb(peak)
        );
        vm
    };

    sample("startup");

    let cfg = DbConfig {
        cache_size_mb: args.cache_mb,
        ..DbConfig::default()
    };
    let db = decentdb::Db::create(&db_path, cfg).context("Db::create")?;
    sample("after Db::create");

    db.execute("CREATE TABLE events (id BIGINT PRIMARY KEY, kind INT, payload TEXT)")?;
    sample("after schema");

    let started = Instant::now();
    let mut next_id: u64 = 1;
    let mut batch_index: u64 = 0;
    while next_id <= args.rows {
        let end = (next_id + args.batch - 1).min(args.rows);
        {
            let mut txn = db.transaction()?;
            let prepared =
                txn.prepare("INSERT INTO events (id, kind, payload) VALUES ($1, $2, $3)")?;
            let mut params: [Value; 3] =
                [Value::Int64(0), Value::Int64(0), Value::Text(String::new())];
            for id in next_id..=end {
                params[0] = Value::Int64(id as i64);
                params[1] = Value::Int64((id % 16) as i64);
                params[2] = Value::Text(format!("payload-{id}"));
                prepared.execute_in(&mut txn, &params)?;
            }
            txn.commit()?;
        }
        next_id = end + 1;
        batch_index += 1;
        if batch_index.is_multiple_of(10) || next_id > args.rows {
            sample(&format!("after {} rows", end));
        }
    }
    let load_ns = started.elapsed().as_secs_f64() * 1_000_000_000.0;
    let load_rss = sample("end-of-load");

    // Snapshot engine-side state to understand what's still in the WalIndex etc.
    if let Ok(json) = db.inspect_storage_state_json() {
        println!("  ENGINE state @ end-of-load: {}", json);
    }

    // Try to force a checkpoint to clear WAL.
    let _ = db.checkpoint();
    let after_ckpt = sample("after Db::checkpoint()");
    if let Ok(json) = db.inspect_storage_state_json() {
        println!("  ENGINE state @ post-checkpoint: {}", json);
    }

    // Drop Db, see if RSS releases.
    drop(db);
    let after_drop = sample("after Db drop");

    // Ask glibc to release freed arenas back to the OS.
    unsafe {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        malloc_trim(0);
    }
    let after_trim = sample("after malloc_trim(0)");

    // Run one tiny query to see steady-state read RSS.
    let cfg = DbConfig {
        cache_size_mb: args.cache_mb,
        ..DbConfig::default()
    };
    let db2 = decentdb::Db::open(&db_path, cfg)?;
    sample("re-open Db");
    let r = db2.execute("SELECT COUNT(*) FROM events")?;
    sample("after COUNT(*)");
    let r2 = db2.execute_with_params(
        "SELECT id, kind, payload FROM events WHERE id = $1",
        &[Value::Int64(1234567)],
    )?;
    sample("after by-id lookup");
    drop(r);
    drop(r2);
    drop(db2);
    let final_rss = sample("after second Db drop");

    let db_size = fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
    let wal_size = fs::metadata(format!("{}.wal", db_path.display()))
        .or_else(|_| fs::metadata(format!("{}-wal", db_path.display())))
        .map(|m| m.len())
        .unwrap_or(0);

    println!();
    println!("=== summary [{}] ===", args.label);
    println!("rows inserted        : {}", args.rows);
    println!("batch size           : {}", args.batch);
    println!("cache_size_mb        : {}", args.cache_mb);
    println!(
        "load duration        : {:.0} ms ({:.0} r/s)",
        load_ns / 1_000_000.0,
        args.rows as f64 / (load_ns / 1_000_000_000.0)
    );
    println!("DB size on disk      : {:.1} MB", mb(db_size));
    println!("WAL size on disk     : {:.1} MB", mb(wal_size));
    println!("end-of-load RSS      : {:.1} MB", mb(load_rss));
    println!("after-checkpoint RSS : {:.1} MB", mb(after_ckpt));
    println!("after-Db-drop RSS    : {:.1} MB", mb(after_drop));
    println!("after-malloc_trim RSS: {:.1} MB", mb(after_trim));
    println!("re-open + query RSS  : {:.1} MB", mb(final_rss));
    println!("PEAK RSS             : {:.1} MB", mb(peak));
    println!(
        "ratio peak/db_size   : {:.2}x",
        peak as f64 / db_size.max(1) as f64
    );
    Ok(())
}
