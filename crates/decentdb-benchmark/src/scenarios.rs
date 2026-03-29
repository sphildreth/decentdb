use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use hdrhistogram::Histogram;
use serde_json::json;

use decentdb::{Db, DbConfig, QueryResult, Value, WalSyncMode};

use crate::profiles::ResolvedProfile;
use crate::types::{HistogramSummary, ScenarioId, ScenarioResult, ScenarioStatus};

const LOOKUP_STRIDE: u64 = 8_191;
const RANGE_STRIDE: u64 = 31;

pub(crate) fn run_phase_1_scenario(
    scenario_id: ScenarioId,
    profile: &ResolvedProfile,
    scenario_scratch: &Path,
) -> Result<ScenarioResult> {
    fs::create_dir_all(scenario_scratch)
        .with_context(|| format!("create scenario scratch {}", scenario_scratch.display()))?;
    match scenario_id {
        ScenarioId::DurableCommitSingle => durable_commit_single(profile, scenario_scratch),
        ScenarioId::PointLookupWarm => point_lookup_warm(profile),
        ScenarioId::RangeScanWarm => range_scan_warm(profile),
        ScenarioId::StorageEfficiency => storage_efficiency(profile, scenario_scratch),
    }
}

struct LatencyCollector {
    histogram: Histogram<u64>,
    total_ns: u128,
    sample_count: u64,
}

impl LatencyCollector {
    fn new() -> Result<Self> {
        Ok(Self {
            histogram: Histogram::<u64>::new(3)?,
            total_ns: 0,
            sample_count: 0,
        })
    }

    fn record(&mut self, elapsed: std::time::Duration) -> Result<()> {
        let nanos = elapsed_to_ns(elapsed);
        self.histogram.record(nanos)?;
        self.total_ns = self.total_ns.saturating_add(u128::from(nanos));
        self.sample_count = self.sample_count.saturating_add(1);
        Ok(())
    }

    fn ops_per_sec(&self) -> f64 {
        if self.total_ns == 0 {
            return 0.0;
        }
        let seconds = self.total_ns as f64 / 1_000_000_000.0;
        self.sample_count as f64 / seconds
    }

    fn summary(&self) -> HistogramSummary {
        HistogramSummary {
            unit: "microseconds".to_string(),
            sample_count: self.sample_count,
            p50_us: ns_to_us(self.histogram.value_at_quantile(0.50)),
            p95_us: ns_to_us(self.histogram.value_at_quantile(0.95)),
            p99_us: ns_to_us(self.histogram.value_at_quantile(0.99)),
            max_us: ns_to_us(self.histogram.max()),
            mean_us: ns_to_us_f64(self.histogram.mean()),
            stddev_us: ns_to_us_f64(self.histogram.stdev()),
        }
    }
}

fn durable_commit_single(
    profile: &ResolvedProfile,
    scenario_scratch: &Path,
) -> Result<ScenarioResult> {
    let mut commit_collector = LatencyCollector::new()?;
    let mut txn_collector = LatencyCollector::new()?;
    let mut warnings = Vec::new();

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("create trial dir {}", trial_dir.display()))?;
        let db_path = trial_dir.join("durable_commit_single.ddb");

        let config = DbConfig {
            temp_dir: trial_dir.clone(),
            wal_sync_mode: WalSyncMode::Full,
            ..DbConfig::default()
        };

        let db = Db::open_or_create(&db_path, config).with_context(|| {
            format!(
                "open or create durable_commit_single database {}",
                db_path.display()
            )
        })?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS bench_commit (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )?;
        let insert = db.prepare(
            "INSERT INTO bench_commit (id, payload) VALUES ($1, 'durable-commit-single')",
        )?;

        let trial_base = u64::from(trial) * (profile.warmup_ops + profile.durable_commits) + 1;
        for offset in 0..profile.warmup_ops {
            let id = to_i64(trial_base + offset)?;
            let mut txn = db.transaction()?;
            insert.execute_in(&mut txn, &[Value::Int64(id)])?;
            txn.commit()?;
        }

        for offset in 0..profile.durable_commits {
            let id = to_i64(trial_base + profile.warmup_ops + offset)?;
            let mut txn = db.transaction()?;
            let txn_started = Instant::now();
            insert.execute_in(&mut txn, &[Value::Int64(id)])?;
            let commit_started = Instant::now();
            txn.commit()?;
            commit_collector.record(commit_started.elapsed())?;
            txn_collector.record(txn_started.elapsed())?;
        }

        if db.config().wal_sync_mode != WalSyncMode::Full {
            warnings.push("db.config().wal_sync_mode was not Full".to_string());
        }
        db.checkpoint()?;
    }

    let commit_summary = commit_collector.summary();
    let txn_summary = txn_collector.summary();
    let mut metrics = BTreeMap::new();
    metrics.insert("commit_p50_us".to_string(), json!(commit_summary.p50_us));
    metrics.insert("commit_p95_us".to_string(), json!(commit_summary.p95_us));
    metrics.insert("commit_p99_us".to_string(), json!(commit_summary.p99_us));
    metrics.insert("commit_max_us".to_string(), json!(commit_summary.max_us));
    metrics.insert("commit_mean_us".to_string(), json!(commit_summary.mean_us));
    metrics.insert(
        "commit_stddev_us".to_string(),
        json!(commit_summary.stddev_us),
    );
    metrics.insert("txn_p50_us".to_string(), json!(txn_summary.p50_us));
    metrics.insert("txn_p95_us".to_string(), json!(txn_summary.p95_us));
    metrics.insert("txn_p99_us".to_string(), json!(txn_summary.p99_us));
    metrics.insert("txn_max_us".to_string(), json!(txn_summary.max_us));
    metrics.insert("txn_mean_us".to_string(), json!(txn_summary.mean_us));
    metrics.insert("txn_stddev_us".to_string(), json!(txn_summary.stddev_us));
    metrics.insert(
        "commits_measured".to_string(),
        json!(commit_summary.sample_count),
    );
    metrics.insert(
        "commits_per_sec".to_string(),
        json!(commit_collector.ops_per_sec()),
    );
    metrics.insert(
        "txns_per_sec".to_string(),
        json!(txn_collector.ops_per_sec()),
    );

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::DurableCommitSingle,
        profile: profile.kind,
        workload: ScenarioId::DurableCommitSingle
            .default_workload()
            .to_string(),
        durability_mode: "full".to_string(),
        cache_mode: "real_fs".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings,
        notes: vec![
            "Real filesystem path used for measurement.".to_string(),
            "Prepared insert reused across explicit one-row transactions.".to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(txn_summary),
        vfs_stats: None,
        artifacts: Vec::new(),
    })
}

fn point_lookup_warm(profile: &ResolvedProfile) -> Result<ScenarioResult> {
    let mut collector = LatencyCollector::new()?;
    let mut total_rows_returned = 0_u64;
    let rows = profile.rows;

    for trial in 0..profile.trials {
        let db = Db::open(":memory:", DbConfig::default())?;
        db.execute(
            "CREATE TABLE bench_point_lookup (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )?;
        load_id_table_chunked(
            &db,
            "INSERT INTO bench_point_lookup (id, payload) VALUES ($1, 'point-lookup')",
            rows,
            2_000,
        )?;
        let select = db.prepare("SELECT payload FROM bench_point_lookup WHERE id = $1")?;

        for warmup in 0..profile.warmup_ops {
            let key = deterministic_id(profile.seed, warmup, rows, trial);
            let _ = select.execute(&[Value::Int64(to_i64(key)?)])?;
        }

        for op in 0..profile.point_reads {
            let key = deterministic_id(
                profile.seed,
                op.saturating_add(profile.warmup_ops),
                rows,
                trial,
            );
            let started = Instant::now();
            let result = select.execute(&[Value::Int64(to_i64(key)?)])?;
            collector.record(started.elapsed())?;
            let row_count = result.rows().len();
            if row_count != 1 {
                return Err(anyhow!(
                    "point_lookup_warm expected exactly 1 row, got {row_count}"
                ));
            }
            total_rows_returned = total_rows_returned.saturating_add(row_count as u64);
        }
    }

    let summary = collector.summary();
    let mut metrics = BTreeMap::new();
    metrics.insert("lookup_p50_us".to_string(), json!(summary.p50_us));
    metrics.insert("lookup_p95_us".to_string(), json!(summary.p95_us));
    metrics.insert("lookup_p99_us".to_string(), json!(summary.p99_us));
    metrics.insert("lookup_max_us".to_string(), json!(summary.max_us));
    metrics.insert("lookup_mean_us".to_string(), json!(summary.mean_us));
    metrics.insert("lookup_stddev_us".to_string(), json!(summary.stddev_us));
    metrics.insert("lookups_measured".to_string(), json!(summary.sample_count));
    metrics.insert(
        "lookups_per_sec".to_string(),
        json!(collector.ops_per_sec()),
    );
    metrics.insert(
        "rows_returned_total".to_string(),
        json!(total_rows_returned),
    );

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::PointLookupWarm,
        profile: profile.kind,
        workload: ScenarioId::PointLookupWarm.default_workload().to_string(),
        durability_mode: "n/a".to_string(),
        cache_mode: "in_memory".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings: Vec::new(),
        notes: vec![
            "In-memory mode isolates warm execution path for Phase 1.".to_string(),
            "Prepared select reused in the measured loop.".to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(summary),
        vfs_stats: None,
        artifacts: Vec::new(),
    })
}

fn range_scan_warm(profile: &ResolvedProfile) -> Result<ScenarioResult> {
    let mut collector = LatencyCollector::new()?;
    let mut total_rows_scanned = 0_u64;
    let rows = profile.rows;
    let range_width = profile.range_scan_rows.min(rows.max(1));
    let max_start = rows.saturating_sub(range_width).saturating_add(1).max(1);

    for trial in 0..profile.trials {
        let db = Db::open(":memory:", DbConfig::default())?;
        db.execute("CREATE TABLE bench_range_scan (id INT64 PRIMARY KEY, payload TEXT NOT NULL)")?;
        load_id_table_chunked(
            &db,
            "INSERT INTO bench_range_scan (id, payload) VALUES ($1, 'range-scan')",
            rows,
            2_000,
        )?;
        let select = db.prepare(
            "SELECT id, payload FROM bench_range_scan WHERE id >= $1 AND id < $2 ORDER BY id",
        )?;

        for warmup in 0..profile.warmup_ops {
            let start_key = deterministic_scan_start(profile.seed, warmup, max_start, trial);
            let end_key = start_key.saturating_add(range_width);
            let _ = select.execute(&[
                Value::Int64(to_i64(start_key)?),
                Value::Int64(to_i64(end_key)?),
            ])?;
        }

        for op in 0..profile.range_scans {
            let start_key = deterministic_scan_start(
                profile.seed,
                op.saturating_add(profile.warmup_ops),
                max_start,
                trial,
            );
            let end_key = start_key.saturating_add(range_width);
            let started = Instant::now();
            let result = select.execute(&[
                Value::Int64(to_i64(start_key)?),
                Value::Int64(to_i64(end_key)?),
            ])?;
            collector.record(started.elapsed())?;
            total_rows_scanned = total_rows_scanned.saturating_add(result.rows().len() as u64);
        }
    }

    let summary = collector.summary();
    let mut metrics = BTreeMap::new();
    metrics.insert("scan_p50_us".to_string(), json!(summary.p50_us));
    metrics.insert("scan_p95_us".to_string(), json!(summary.p95_us));
    metrics.insert("scan_p99_us".to_string(), json!(summary.p99_us));
    metrics.insert("scan_max_us".to_string(), json!(summary.max_us));
    metrics.insert("scan_mean_us".to_string(), json!(summary.mean_us));
    metrics.insert("scan_stddev_us".to_string(), json!(summary.stddev_us));
    metrics.insert("scans_measured".to_string(), json!(summary.sample_count));
    metrics.insert("scans_per_sec".to_string(), json!(collector.ops_per_sec()));
    metrics.insert("rows_scanned_total".to_string(), json!(total_rows_scanned));
    let rows_per_sec = if collector.total_ns == 0 {
        0.0
    } else {
        let seconds = collector.total_ns as f64 / 1_000_000_000.0;
        total_rows_scanned as f64 / seconds
    };
    metrics.insert("rows_per_sec".to_string(), json!(rows_per_sec));

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::RangeScanWarm,
        profile: profile.kind,
        workload: ScenarioId::RangeScanWarm.default_workload().to_string(),
        durability_mode: "n/a".to_string(),
        cache_mode: "in_memory".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings: Vec::new(),
        notes: vec![
            "In-memory mode isolates warm execution path for Phase 1.".to_string(),
            "Prepared range select reused in the measured loop.".to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(summary),
        vfs_stats: None,
        artifacts: Vec::new(),
    })
}

fn storage_efficiency(
    profile: &ResolvedProfile,
    scenario_scratch: &Path,
) -> Result<ScenarioResult> {
    let payload = "storage-payload";
    let payload_bytes = payload.len() as u64;
    let row_logical_bytes = 8_u64.saturating_add(payload_bytes);
    let chunk_size = 2_000_u64;

    let mut db_file_sum = 0_u64;
    let mut wal_after_checkpoint_sum = 0_u64;
    let mut bytes_per_row_sum = 0.0_f64;
    let mut space_amp_sum = 0.0_f64;
    let mut wal_file_peak_max = 0_u64;
    let mut warnings = Vec::new();

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("create storage trial dir {}", trial_dir.display()))?;
        let db_path = trial_dir.join("storage_efficiency.ddb");

        let config = DbConfig {
            temp_dir: trial_dir.clone(),
            wal_sync_mode: WalSyncMode::Full,
            ..DbConfig::default()
        };

        let db = Db::open_or_create(&db_path, config.clone())
            .with_context(|| format!("open storage benchmark database {}", db_path.display()))?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS bench_storage_efficiency (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )?;

        let mut inserted = 0_u64;
        let mut wal_peak_this_trial = db.storage_info()?.wal_file_size;
        while inserted < profile.rows {
            let mut txn = db.transaction()?;
            let insert = txn.prepare(
                "INSERT INTO bench_storage_efficiency (id, payload) VALUES ($1, 'storage-payload')",
            )?;
            let end = (inserted + chunk_size).min(profile.rows);
            while inserted < end {
                inserted = inserted.saturating_add(1);
                insert.execute_in(&mut txn, &[Value::Int64(to_i64(inserted)?)])?;
            }
            txn.commit()?;
            wal_peak_this_trial = wal_peak_this_trial.max(db.storage_info()?.wal_file_size);
        }

        db.checkpoint()?;
        let storage_info = db.storage_info()?;
        let db_file_bytes = file_len(db.path())?;
        let wal_after_checkpoint = storage_info.wal_file_size;
        wal_file_peak_max = wal_file_peak_max.max(wal_peak_this_trial);

        let steady_state_file_bytes = db_file_bytes.saturating_add(wal_after_checkpoint);
        let logical_payload_bytes = profile.rows.saturating_mul(row_logical_bytes);
        let bytes_per_row = steady_state_file_bytes as f64 / profile.rows as f64;
        let space_amplification = if logical_payload_bytes == 0 {
            0.0
        } else {
            steady_state_file_bytes as f64 / logical_payload_bytes as f64
        };

        db_file_sum = db_file_sum.saturating_add(db_file_bytes);
        wal_after_checkpoint_sum = wal_after_checkpoint_sum.saturating_add(wal_after_checkpoint);
        bytes_per_row_sum += bytes_per_row;
        space_amp_sum += space_amplification;

        drop(db);
        let reopened = Db::open(&db_path, config)?;
        let count = extract_single_count(
            reopened.execute("SELECT COUNT(*) FROM bench_storage_efficiency")?,
        )?;
        if count != profile.rows {
            return Err(anyhow!(
                "storage_efficiency expected {} rows after reopen, found {count}",
                profile.rows
            ));
        }
        if reopened.config().wal_sync_mode != WalSyncMode::Full {
            warnings.push("reopened db.config().wal_sync_mode was not Full".to_string());
        }
    }

    warnings.push(format!(
        "wal_file_bytes_peak sampled once per {chunk_size} inserted rows; true peak may be higher"
    ));

    let trial_count = f64::from(profile.trials);
    let avg_db_file_bytes = db_file_sum / u64::from(profile.trials);
    let avg_wal_file_bytes_after_checkpoint = wal_after_checkpoint_sum / u64::from(profile.trials);
    let logical_payload_bytes = profile.rows.saturating_mul(row_logical_bytes);
    let steady_state_file_bytes =
        avg_db_file_bytes.saturating_add(avg_wal_file_bytes_after_checkpoint);

    let mut metrics = BTreeMap::new();
    metrics.insert(
        "logical_payload_bytes".to_string(),
        json!(logical_payload_bytes),
    );
    metrics.insert("db_file_bytes".to_string(), json!(avg_db_file_bytes));
    metrics.insert("wal_file_bytes_peak".to_string(), json!(wal_file_peak_max));
    metrics.insert(
        "wal_file_bytes_after_checkpoint".to_string(),
        json!(avg_wal_file_bytes_after_checkpoint),
    );
    metrics.insert(
        "steady_state_file_bytes".to_string(),
        json!(steady_state_file_bytes),
    );
    metrics.insert("row_count".to_string(), json!(profile.rows));
    metrics.insert(
        "bytes_per_logical_row".to_string(),
        json!(bytes_per_row_sum / trial_count),
    );
    metrics.insert(
        "space_amplification".to_string(),
        json!(space_amp_sum / trial_count),
    );

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::StorageEfficiency,
        profile: profile.kind,
        workload: ScenarioId::StorageEfficiency.default_workload().to_string(),
        durability_mode: "full".to_string(),
        cache_mode: "real_fs".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings,
        notes: vec![
            "Storage metrics measured on real filesystem path.".to_string(),
            "Phase 1 uses file-size metrics only; page-category storage breakdown is not implemented yet."
                .to_string(),
        ],
        scale: profile.scale_json(),
        histograms: None,
        vfs_stats: None,
        artifacts: Vec::new(),
    })
}

fn load_id_table_chunked(db: &Db, insert_sql: &str, rows: u64, chunk_size: u64) -> Result<()> {
    let mut inserted = 0_u64;
    while inserted < rows {
        let mut txn = db.transaction()?;
        let insert = txn.prepare(insert_sql)?;
        let end = (inserted + chunk_size).min(rows);
        while inserted < end {
            inserted = inserted.saturating_add(1);
            insert.execute_in(&mut txn, &[Value::Int64(to_i64(inserted)?)])?;
        }
        txn.commit()?;
    }
    Ok(())
}

fn extract_single_count(result: QueryResult) -> Result<u64> {
    let row = result
        .rows()
        .first()
        .ok_or_else(|| anyhow!("expected one row for COUNT(*) result"))?;
    let value = row
        .values()
        .first()
        .ok_or_else(|| anyhow!("expected one column for COUNT(*) result"))?;
    match value {
        Value::Int64(v) if *v >= 0 => Ok(*v as u64),
        _ => Err(anyhow!("COUNT(*) returned unexpected value type")),
    }
}

fn deterministic_id(seed: u64, op: u64, rows: u64, trial: u32) -> u64 {
    let mixed = splitmix64(
        seed ^ op.wrapping_mul(LOOKUP_STRIDE) ^ u64::from(trial).wrapping_mul(0x9e37_79b9),
    );
    (mixed % rows) + 1
}

fn deterministic_scan_start(seed: u64, op: u64, max_start: u64, trial: u32) -> u64 {
    let mixed = splitmix64(
        seed.rotate_left(13)
            ^ op.wrapping_mul(RANGE_STRIDE)
            ^ u64::from(trial).wrapping_mul(0x85eb_ca6b),
    );
    (mixed % max_start) + 1
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn file_len(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)
        .with_context(|| format!("read metadata for {}", path.display()))?
        .len())
}

fn elapsed_to_ns(elapsed: std::time::Duration) -> u64 {
    let nanos = elapsed.as_nanos();
    if nanos > u128::from(u64::MAX) {
        u64::MAX
    } else {
        nanos as u64
    }
}

fn ns_to_us(ns: u64) -> f64 {
    ns as f64 / 1_000.0
}

fn ns_to_us_f64(ns: f64) -> f64 {
    ns / 1_000.0
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("value {value} exceeds i64"))
}

#[cfg(test)]
mod tests {
    use super::{deterministic_id, deterministic_scan_start, run_phase_1_scenario};
    use crate::profiles::ResolvedProfile;
    use crate::types::{ProfileKind, ScenarioId, ScenarioStatus};
    use tempfile::TempDir;

    #[test]
    fn point_lookup_scenario_runs_with_tiny_profile() {
        let temp = TempDir::new().expect("tempdir");
        let profile = ResolvedProfile {
            kind: ProfileKind::Custom,
            rows: 100,
            point_reads: 100,
            range_scan_rows: 10,
            range_scans: 10,
            durable_commits: 10,
            warmup_ops: 5,
            trials: 1,
            seed: 7,
        };
        let result = run_phase_1_scenario(ScenarioId::PointLookupWarm, &profile, temp.path())
            .expect("run point lookup scenario");
        assert!(matches!(result.status, ScenarioStatus::Passed));
        assert!(result.metrics.contains_key("lookup_p95_us"));
    }

    #[test]
    fn deterministic_generators_change_with_seed() {
        assert_ne!(
            deterministic_id(7, 11, 100, 0),
            deterministic_id(9, 11, 100, 0)
        );
        assert_ne!(
            deterministic_scan_start(7, 11, 100, 0),
            deterministic_scan_start(9, 11, 100, 0)
        );
    }
}
