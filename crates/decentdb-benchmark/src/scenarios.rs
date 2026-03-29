use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use serde_json::json;

use decentdb::{Db, DbConfig, QueryResult, Value, WalSyncMode};

use crate::cli::{ColdPointLookupProbeArgs, InternalCommand, RecoveryReopenProbeArgs};
use crate::profiles::ResolvedProfile;
use crate::types::{HistogramSummary, ScenarioId, ScenarioResult, ScenarioStatus};

const LOOKUP_STRIDE: u64 = 8_191;
const RANGE_STRIDE: u64 = 31;

pub(crate) fn run_scenario(
    scenario_id: ScenarioId,
    profile: &ResolvedProfile,
    scenario_scratch: &Path,
) -> Result<ScenarioResult> {
    fs::create_dir_all(scenario_scratch)
        .with_context(|| format!("create scenario scratch {}", scenario_scratch.display()))?;
    match scenario_id {
        ScenarioId::DurableCommitSingle => durable_commit_single(profile, scenario_scratch),
        ScenarioId::DurableCommitBatch => durable_commit_batch(profile, scenario_scratch),
        ScenarioId::PointLookupWarm => point_lookup_warm(profile),
        ScenarioId::PointLookupCold => point_lookup_cold(profile, scenario_scratch),
        ScenarioId::RangeScanWarm => range_scan_warm(profile),
        ScenarioId::Checkpoint => checkpoint(profile, scenario_scratch),
        ScenarioId::RecoveryReopen => recovery_reopen(profile, scenario_scratch),
        ScenarioId::ReadUnderWrite => read_under_write(profile, scenario_scratch),
        ScenarioId::StorageEfficiency => storage_efficiency(profile, scenario_scratch),
    }
}

pub(crate) fn execute_internal_command(command: InternalCommand) -> Result<serde_json::Value> {
    match command {
        InternalCommand::ColdPointLookupProbe(args) => {
            Ok(serde_json::to_value(run_cold_point_lookup_probe(args)?)?)
        }
        InternalCommand::RecoveryReopenProbe(args) => {
            Ok(serde_json::to_value(run_recovery_reopen_probe(args)?)?)
        }
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

    fn record(&mut self, elapsed: Duration) -> Result<()> {
        self.record_ns(elapsed_to_ns(elapsed))
    }

    fn record_ns(&mut self, nanos: u64) -> Result<()> {
        self.histogram.record(nanos)?;
        self.total_ns = self.total_ns.saturating_add(u128::from(nanos));
        self.sample_count = self.sample_count.saturating_add(1);
        Ok(())
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        self.histogram.add(&other.histogram)?;
        self.total_ns = self.total_ns.saturating_add(other.total_ns);
        self.sample_count = self.sample_count.saturating_add(other.sample_count);
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

        let config = real_fs_full_durability_config(&trial_dir);

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

fn durable_commit_batch(
    profile: &ResolvedProfile,
    scenario_scratch: &Path,
) -> Result<ScenarioResult> {
    let mut commit_collector = LatencyCollector::new()?;
    let mut txn_collector = LatencyCollector::new()?;

    let mut wal_before_sum = 0_u64;
    let mut wal_after_sum = 0_u64;
    let mut wal_peak_max = 0_u64;
    let mut total_rows_written = 0_u64;
    let mut warnings = Vec::new();

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("create trial dir {}", trial_dir.display()))?;
        let db_path = trial_dir.join("durable_commit_batch.ddb");

        let config = real_fs_full_durability_config(&trial_dir);
        let db = Db::open_or_create(&db_path, config).with_context(|| {
            format!(
                "open or create durable_commit_batch database {}",
                db_path.display()
            )
        })?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS bench_commit_batch (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )?;
        let insert = db.prepare(
            "INSERT INTO bench_commit_batch (id, payload) VALUES ($1, 'durable-commit-batch')",
        )?;

        let mut ordinal = 0_u64;
        for _ in 0..profile.warmup_ops {
            let mut txn = db.transaction()?;
            for _ in 0..profile.batch_size {
                let id = deterministic_row_id(profile.seed, trial, ordinal);
                insert.execute_in(&mut txn, &[Value::Int64(to_i64(id)?)])?;
                ordinal = ordinal.saturating_add(1);
            }
            txn.commit()?;
        }

        let mut wal_peak_this_trial = db.storage_info()?.wal_file_size;
        let wal_before = wal_peak_this_trial;

        for _ in 0..profile.durable_commits {
            let mut txn = db.transaction()?;
            let txn_started = Instant::now();
            for _ in 0..profile.batch_size {
                let id = deterministic_row_id(profile.seed, trial, ordinal);
                insert.execute_in(&mut txn, &[Value::Int64(to_i64(id)?)])?;
                ordinal = ordinal.saturating_add(1);
                total_rows_written = total_rows_written.saturating_add(1);
            }
            let commit_started = Instant::now();
            txn.commit()?;
            commit_collector.record(commit_started.elapsed())?;
            txn_collector.record(txn_started.elapsed())?;
            wal_peak_this_trial = wal_peak_this_trial.max(db.storage_info()?.wal_file_size);
        }

        let wal_after = db.storage_info()?.wal_file_size;
        if db.config().wal_sync_mode != WalSyncMode::Full {
            warnings.push("db.config().wal_sync_mode was not Full".to_string());
        }

        wal_before_sum = wal_before_sum.saturating_add(wal_before);
        wal_after_sum = wal_after_sum.saturating_add(wal_after);
        wal_peak_max = wal_peak_max.max(wal_peak_this_trial);
    }

    warnings.push(
        "wal_file_bytes_peak sampled at batch-commit boundaries; true intra-batch peak may be higher"
            .to_string(),
    );

    let trial_count = u64::from(profile.trials);
    let wal_before_avg = wal_before_sum / trial_count;
    let wal_after_avg = wal_after_sum / trial_count;
    let wal_growth_bytes = wal_after_avg.saturating_sub(wal_before_avg);

    let commit_summary = commit_collector.summary();
    let txn_summary = txn_collector.summary();
    let rows_per_sec = if txn_collector.total_ns == 0 {
        0.0
    } else {
        let seconds = txn_collector.total_ns as f64 / 1_000_000_000.0;
        total_rows_written as f64 / seconds
    };

    let mut metrics = BTreeMap::new();
    metrics.insert(
        "batch_commit_p50_us".to_string(),
        json!(commit_summary.p50_us),
    );
    metrics.insert(
        "batch_commit_p95_us".to_string(),
        json!(commit_summary.p95_us),
    );
    metrics.insert(
        "batch_commit_p99_us".to_string(),
        json!(commit_summary.p99_us),
    );
    metrics.insert(
        "batch_commit_max_us".to_string(),
        json!(commit_summary.max_us),
    );
    metrics.insert("txn_batch_p50_us".to_string(), json!(txn_summary.p50_us));
    metrics.insert("txn_batch_p95_us".to_string(), json!(txn_summary.p95_us));
    metrics.insert("txn_batch_p99_us".to_string(), json!(txn_summary.p99_us));
    metrics.insert("rows_per_sec".to_string(), json!(rows_per_sec));
    metrics.insert("rows_per_batch".to_string(), json!(profile.batch_size));
    metrics.insert(
        "batches_measured".to_string(),
        json!(commit_summary.sample_count),
    );
    metrics.insert("rows_measured_total".to_string(), json!(total_rows_written));
    metrics.insert(
        "wal_bytes_before_measurement".to_string(),
        json!(wal_before_avg),
    );
    metrics.insert(
        "wal_bytes_after_measurement".to_string(),
        json!(wal_after_avg),
    );
    metrics.insert("wal_growth_bytes".to_string(), json!(wal_growth_bytes));
    metrics.insert("wal_file_bytes_peak".to_string(), json!(wal_peak_max));
    metrics.insert(
        "wal_growth_bytes_per_row".to_string(),
        json!(if total_rows_written == 0 {
            0.0
        } else {
            wal_growth_bytes as f64 / total_rows_written as f64
        }),
    );

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::DurableCommitBatch,
        profile: profile.kind,
        workload: ScenarioId::DurableCommitBatch
            .default_workload()
            .to_string(),
        durability_mode: "full".to_string(),
        cache_mode: "real_fs".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings,
        notes: vec![
            "Real filesystem path and full durability are used by default.".to_string(),
            "Prepared insert is reused inside explicit small-batch transactions.".to_string(),
            "Phase 2 WAL growth uses observable file-size deltas and sampled per-batch peaks only."
                .to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(commit_summary),
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

fn point_lookup_cold(profile: &ResolvedProfile, scenario_scratch: &Path) -> Result<ScenarioResult> {
    let mut first_read_collector = LatencyCollector::new()?;
    let mut cold_batch_collector = LatencyCollector::new()?;

    let mut total_rows_returned = 0_u64;
    let mut warnings = Vec::new();

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("create trial dir {}", trial_dir.display()))?;
        let db_path = trial_dir.join("point_lookup_cold.ddb");

        let config = real_fs_full_durability_config(&trial_dir);
        let db = Db::open_or_create(&db_path, config).with_context(|| {
            format!(
                "open or create point_lookup_cold database {}",
                db_path.display()
            )
        })?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS bench_point_lookup_cold (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )?;
        load_id_table_chunked(
            &db,
            "INSERT INTO bench_point_lookup_cold (id, payload) VALUES ($1, 'point-lookup-cold')",
            profile.rows,
            2_000,
        )?;
        db.checkpoint()?;
        drop(db);

        for batch in 0..profile.cold_batches {
            let probe = invoke_cold_point_lookup_probe(&ColdPointLookupProbeArgs {
                db_path: db_path.clone(),
                rows: profile.rows,
                seed: profile.seed,
                trial,
                start_op: batch.saturating_mul(profile.point_reads),
                lookups: profile.point_reads,
            })?;
            first_read_collector.record_ns(probe.first_read_ns)?;
            cold_batch_collector.record_ns(probe.batch_elapsed_ns)?;
            total_rows_returned = total_rows_returned.saturating_add(probe.rows_returned);
        }
    }

    warnings.push(
        "cold_process measurements are run in child processes; timings exclude parent process spawn overhead."
            .to_string(),
    );

    let first_summary = first_read_collector.summary();
    let cold_batch_summary = cold_batch_collector.summary();

    let mut metrics = BTreeMap::new();
    metrics.insert("first_read_p50_us".to_string(), json!(first_summary.p50_us));
    metrics.insert("first_read_p95_us".to_string(), json!(first_summary.p95_us));
    metrics.insert("first_read_p99_us".to_string(), json!(first_summary.p99_us));
    metrics.insert("first_read_max_us".to_string(), json!(first_summary.max_us));
    metrics.insert(
        "cold_batch_p50_ms".to_string(),
        json!(ns_to_ms(
            cold_batch_collector.histogram.value_at_quantile(0.50)
        )),
    );
    metrics.insert(
        "cold_batch_p95_ms".to_string(),
        json!(ns_to_ms(
            cold_batch_collector.histogram.value_at_quantile(0.95)
        )),
    );
    metrics.insert(
        "cold_batch_p99_ms".to_string(),
        json!(ns_to_ms(
            cold_batch_collector.histogram.value_at_quantile(0.99)
        )),
    );
    metrics.insert(
        "cold_batch_max_ms".to_string(),
        json!(ns_to_ms(cold_batch_collector.histogram.max())),
    );
    metrics.insert(
        "cold_batches_measured".to_string(),
        json!(cold_batch_summary.sample_count),
    );
    metrics.insert(
        "lookups_per_cold_batch".to_string(),
        json!(profile.point_reads),
    );
    metrics.insert(
        "rows_returned_total".to_string(),
        json!(total_rows_returned),
    );
    metrics.insert(
        "lookups_per_sec".to_string(),
        json!(if cold_batch_collector.total_ns == 0 {
            0.0
        } else {
            let seconds = cold_batch_collector.total_ns as f64 / 1_000_000_000.0;
            total_rows_returned as f64 / seconds
        }),
    );

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::PointLookupCold,
        profile: profile.kind,
        workload: ScenarioId::PointLookupCold.default_workload().to_string(),
        durability_mode: "n/a".to_string(),
        cache_mode: "cold_process".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings,
        notes: vec![
            "Portable cold_process mode is used by reopening the database in a child process for each cold batch."
                .to_string(),
            "cold_os_cache is intentionally not attempted in Phase 2.".to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(first_summary),
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

fn checkpoint(profile: &ResolvedProfile, scenario_scratch: &Path) -> Result<ScenarioResult> {
    let mut checkpoint_collector = LatencyCollector::new()?;

    let mut wal_before_sum = 0_u64;
    let mut wal_after_sum = 0_u64;
    let mut wal_peak_max = 0_u64;
    let mut warnings = Vec::new();

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("create trial dir {}", trial_dir.display()))?;
        let db_path = trial_dir.join("checkpoint.ddb");

        let config = real_fs_full_durability_config(&trial_dir);
        let db = Db::open_or_create(&db_path, config)
            .with_context(|| format!("open or create checkpoint database {}", db_path.display()))?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS bench_checkpoint (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )?;

        let mut inserted = 0_u64;
        let mut wal_peak = db.storage_info()?.wal_file_size;
        while inserted < profile.rows {
            let mut txn = db.transaction()?;
            let insert = txn
                .prepare("INSERT INTO bench_checkpoint (id, payload) VALUES ($1, 'checkpoint')")?;
            let end = (inserted + profile.batch_size).min(profile.rows);
            while inserted < end {
                let id = deterministic_row_id(profile.seed, trial, inserted);
                insert.execute_in(&mut txn, &[Value::Int64(to_i64(id)?)])?;
                inserted = inserted.saturating_add(1);
            }
            txn.commit()?;
            wal_peak = wal_peak.max(db.storage_info()?.wal_file_size);
        }

        let wal_before = db.storage_info()?.wal_file_size;
        let checkpoint_started = Instant::now();
        db.checkpoint()?;
        checkpoint_collector.record(checkpoint_started.elapsed())?;
        let wal_after = db.storage_info()?.wal_file_size;

        wal_before_sum = wal_before_sum.saturating_add(wal_before);
        wal_after_sum = wal_after_sum.saturating_add(wal_after);
        wal_peak_max = wal_peak_max.max(wal_peak);
    }

    warnings.push(
        "reader_stall_ms and writer_stall_ms are not exposed by the current public API and are emitted as null in Phase 2."
            .to_string(),
    );

    let summary = checkpoint_collector.summary();
    let trial_count = u64::from(profile.trials);
    let wal_before_avg = wal_before_sum / trial_count;
    let wal_after_avg = wal_after_sum / trial_count;

    let mut metrics = BTreeMap::new();
    let checkpoint_p95_ms = summary.p95_us / 1_000.0;
    metrics.insert("checkpoint_ms".to_string(), json!(checkpoint_p95_ms));
    metrics.insert("checkpoint_total_ms".to_string(), json!(checkpoint_p95_ms));
    metrics.insert(
        "checkpoint_p50_ms".to_string(),
        json!(summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "checkpoint_p95_ms".to_string(),
        json!(summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "checkpoint_p99_ms".to_string(),
        json!(summary.p99_us / 1_000.0),
    );
    metrics.insert(
        "checkpoint_max_ms".to_string(),
        json!(summary.max_us / 1_000.0),
    );
    metrics.insert(
        "wal_bytes_before_checkpoint".to_string(),
        json!(wal_before_avg),
    );
    metrics.insert(
        "wal_bytes_after_checkpoint".to_string(),
        json!(wal_after_avg),
    );
    metrics.insert(
        "wal_bytes_reduced".to_string(),
        json!(wal_before_avg.saturating_sub(wal_after_avg)),
    );
    metrics.insert("wal_file_bytes_peak".to_string(), json!(wal_peak_max));
    metrics.insert("reader_stall_ms".to_string(), serde_json::Value::Null);
    metrics.insert("writer_stall_ms".to_string(), serde_json::Value::Null);

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::Checkpoint,
        profile: profile.kind,
        workload: ScenarioId::Checkpoint.default_workload().to_string(),
        durability_mode: "full".to_string(),
        cache_mode: "real_fs".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings,
        notes: vec![
            "Real filesystem checkpoint benchmark uses Db::checkpoint directly.".to_string(),
            "Phase 2 reports only honestly measurable total checkpoint time and WAL bytes before/after checkpoint."
                .to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(summary),
        vfs_stats: None,
        artifacts: Vec::new(),
    })
}

fn recovery_reopen(profile: &ResolvedProfile, scenario_scratch: &Path) -> Result<ScenarioResult> {
    let mut reopen_collector = LatencyCollector::new()?;
    let mut first_query_collector = LatencyCollector::new()?;

    let mut wal_before_sum = 0_u64;
    let mut wal_after_sum = 0_u64;
    let mut warnings = Vec::new();

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("create trial dir {}", trial_dir.display()))?;
        let db_path = trial_dir.join("recovery_reopen.ddb");

        let config = real_fs_full_durability_config(&trial_dir);
        let db = Db::open_or_create(&db_path, config)
            .with_context(|| format!("open or create recovery database {}", db_path.display()))?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS bench_recovery_reopen (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )?;

        let mut inserted = 0_u64;
        while inserted < profile.rows {
            let mut txn = db.transaction()?;
            let insert = txn.prepare(
                "INSERT INTO bench_recovery_reopen (id, payload) VALUES ($1, 'recovery-reopen')",
            )?;
            let end = (inserted + profile.batch_size).min(profile.rows);
            while inserted < end {
                let id = deterministic_row_id(profile.seed, trial, inserted);
                insert.execute_in(&mut txn, &[Value::Int64(to_i64(id)?)])?;
                inserted = inserted.saturating_add(1);
            }
            txn.commit()?;
        }

        let wal_before = db.storage_info()?.wal_file_size;
        wal_before_sum = wal_before_sum.saturating_add(wal_before);
        drop(db);

        let probe = invoke_recovery_reopen_probe(&RecoveryReopenProbeArgs {
            db_path: db_path.clone(),
            expected_rows: profile.rows,
        })?;
        reopen_collector.record_ns(probe.reopen_elapsed_ns)?;
        first_query_collector.record_ns(probe.first_query_elapsed_ns)?;

        let reopened = Db::open(&db_path, real_fs_default_config(&trial_dir))?;
        wal_after_sum = wal_after_sum.saturating_add(reopened.storage_info()?.wal_file_size);
    }

    warnings.push(
        "pure WAL replay time cannot be isolated from reopen with the current public API; reopen wall time includes open/recovery work."
            .to_string(),
    );

    let reopen_summary = reopen_collector.summary();
    let first_query_summary = first_query_collector.summary();
    let trial_count = u64::from(profile.trials);

    let mut metrics = BTreeMap::new();
    metrics.insert(
        "reopen_p50_ms".to_string(),
        json!(reopen_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "reopen_p95_ms".to_string(),
        json!(reopen_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "reopen_p99_ms".to_string(),
        json!(reopen_summary.p99_us / 1_000.0),
    );
    metrics.insert(
        "reopen_max_ms".to_string(),
        json!(reopen_summary.max_us / 1_000.0),
    );
    metrics.insert(
        "recovery_reopen_ms".to_string(),
        json!(reopen_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "first_query_p50_ms".to_string(),
        json!(first_query_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "first_query_p95_ms".to_string(),
        json!(first_query_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "first_query_p99_ms".to_string(),
        json!(first_query_summary.p99_us / 1_000.0),
    );
    metrics.insert(
        "first_query_max_ms".to_string(),
        json!(first_query_summary.max_us / 1_000.0),
    );
    metrics.insert(
        "wal_bytes_before_reopen".to_string(),
        json!(wal_before_sum / trial_count),
    );
    metrics.insert(
        "wal_bytes_after_reopen".to_string(),
        json!(wal_after_sum / trial_count),
    );

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::RecoveryReopen,
        profile: profile.kind,
        workload: ScenarioId::RecoveryReopen.default_workload().to_string(),
        durability_mode: "full".to_string(),
        cache_mode: "cold_process".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings,
        notes: vec![
            "Cold-process probe reopens the database in a child process for each measured sample."
                .to_string(),
            "Phase 2 reports honest reopen and first-query timings without fabricating replay-only splits."
                .to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(reopen_summary),
        vfs_stats: None,
        artifacts: Vec::new(),
    })
}

fn read_under_write(profile: &ResolvedProfile, scenario_scratch: &Path) -> Result<ScenarioResult> {
    let mut reader_iso_agg = LatencyCollector::new()?;
    let mut reader_under_agg = LatencyCollector::new()?;

    let mut writer_iso_sum = 0.0_f64;
    let mut writer_under_sum = 0.0_f64;

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("create trial dir {}", trial_dir.display()))?;

        let reader_iso_path = trial_dir.join("read_under_write_reader_iso.ddb");
        seed_read_under_write_db(&reader_iso_path, profile)?;
        let reader_iso = run_reader_only_workload(&reader_iso_path, profile, trial)?;
        reader_iso_agg.merge(reader_iso)?;

        let writer_iso_path = trial_dir.join("read_under_write_writer_iso.ddb");
        seed_read_under_write_db(&writer_iso_path, profile)?;
        writer_iso_sum += run_writer_only_workload(&writer_iso_path, profile, trial)?;

        let mixed_path = trial_dir.join("read_under_write_mixed.ddb");
        seed_read_under_write_db(&mixed_path, profile)?;
        let (reader_under, writer_under) = run_mixed_workload(&mixed_path, profile, trial)?;
        reader_under_agg.merge(reader_under)?;
        writer_under_sum += writer_under;
    }

    let reader_iso_summary = reader_iso_agg.summary();
    let reader_under_summary = reader_under_agg.summary();
    let trial_count_f64 = f64::from(profile.trials);

    let writer_iso = writer_iso_sum / trial_count_f64;
    let writer_under = writer_under_sum / trial_count_f64;

    let reader_degradation_ratio = if reader_iso_summary.p95_us == 0.0 {
        0.0
    } else {
        latency_degradation_ratio(reader_iso_summary.p95_us, reader_under_summary.p95_us)
    };
    let writer_degradation_ratio = throughput_degradation_ratio(writer_iso, writer_under);

    let mut metrics = BTreeMap::new();
    metrics.insert(
        "reader_p95_isolation_us".to_string(),
        json!(reader_iso_summary.p95_us),
    );
    metrics.insert(
        "reader_p95_under_write_us".to_string(),
        json!(reader_under_summary.p95_us),
    );
    metrics.insert(
        "reader_degradation_ratio".to_string(),
        json!(reader_degradation_ratio),
    );
    metrics.insert(
        "reader_p95_degradation_ratio".to_string(),
        json!(reader_degradation_ratio),
    );
    metrics.insert(
        "writer_throughput_isolation_ops_per_sec".to_string(),
        json!(writer_iso),
    );
    metrics.insert(
        "writer_throughput_under_readers_ops_per_sec".to_string(),
        json!(writer_under),
    );
    metrics.insert(
        "writer_degradation_ratio".to_string(),
        json!(writer_degradation_ratio),
    );
    metrics.insert(
        "writer_throughput_degradation_ratio".to_string(),
        json!(writer_degradation_ratio),
    );
    metrics.insert("reader_threads".to_string(), json!(profile.reader_threads));
    metrics.insert("writer_ops".to_string(), json!(profile.writer_ops));

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::ReadUnderWrite,
        profile: profile.kind,
        workload: ScenarioId::ReadUnderWrite.default_workload().to_string(),
        durability_mode: "full".to_string(),
        cache_mode: "warm_cache".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings: Vec::new(),
        notes: vec![
            "One writer thread and multiple reader threads run against the same database path using direct Rust API calls."
                .to_string(),
            "reader_degradation_ratio = reader_p95_under_write_us / reader_p95_isolation_us"
                .to_string(),
            "writer_degradation_ratio = writer_throughput_isolation_ops_per_sec / writer_throughput_under_readers_ops_per_sec"
                .to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(reader_under_summary),
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

        let config = real_fs_full_durability_config(&trial_dir);

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
                let id = deterministic_row_id(profile.seed, trial, inserted);
                insert.execute_in(&mut txn, &[Value::Int64(to_i64(id)?)])?;
                inserted = inserted.saturating_add(1);
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

fn seed_read_under_write_db(db_path: &Path, profile: &ResolvedProfile) -> Result<()> {
    let trial_dir = db_path
        .parent()
        .ok_or_else(|| anyhow!("database path {} has no parent", db_path.display()))?;
    let config = real_fs_full_durability_config(trial_dir);

    let db = Db::open_or_create(db_path, config)
        .with_context(|| format!("open read_under_write database {}", db_path.display()))?;
    db.execute(
        "CREATE TABLE IF NOT EXISTS bench_read_under_write (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
    )?;

    let mut inserted = 0_u64;
    while inserted < profile.rows {
        let mut txn = db.transaction()?;
        let insert = txn.prepare(
            "INSERT INTO bench_read_under_write (id, payload) VALUES ($1, 'read-under-write')",
        )?;
        let end = (inserted + profile.batch_size).min(profile.rows);
        while inserted < end {
            inserted = inserted.saturating_add(1);
            insert.execute_in(&mut txn, &[Value::Int64(to_i64(inserted)?)])?;
        }
        txn.commit()?;
    }

    db.checkpoint()?;
    Ok(())
}

fn run_reader_only_workload(
    db_path: &Path,
    profile: &ResolvedProfile,
    trial: u32,
) -> Result<LatencyCollector> {
    let thread_count = profile.reader_threads.max(1);
    let mut handles = Vec::with_capacity(thread_count as usize);

    for thread_index in 0..thread_count {
        let db_path = db_path.to_path_buf();
        let seed = profile.seed;
        let rows = profile.rows;
        let (start_op, op_count) = split_ops(profile.point_reads, thread_count, thread_index);
        let trial_local = trial;
        handles.push(thread::spawn(move || {
            run_reader_thread(
                db_path,
                rows,
                seed,
                trial_local,
                start_op,
                op_count,
                thread_index,
            )
        }));
    }

    let mut aggregate = LatencyCollector::new()?;
    for handle in handles {
        let result = handle
            .join()
            .map_err(|_| anyhow!("reader-only thread panicked"))??;
        aggregate.merge(result)?;
    }
    Ok(aggregate)
}

fn run_writer_only_workload(db_path: &Path, profile: &ResolvedProfile, trial: u32) -> Result<f64> {
    run_writer_thread(
        db_path.to_path_buf(),
        profile.rows,
        profile.writer_ops,
        profile.seed,
        trial,
    )
}

fn run_mixed_workload(
    db_path: &Path,
    profile: &ResolvedProfile,
    trial: u32,
) -> Result<(LatencyCollector, f64)> {
    let thread_count = profile.reader_threads.max(1);
    let barrier = Arc::new(Barrier::new((thread_count + 1) as usize));

    let writer_path = db_path.to_path_buf();
    let writer_barrier = Arc::clone(&barrier);
    let writer_ops = profile.writer_ops;
    let writer_rows = profile.rows;
    let writer_seed = profile.seed;
    let writer_handle = thread::spawn(move || {
        writer_barrier.wait();
        run_writer_thread(writer_path, writer_rows, writer_ops, writer_seed, trial)
    });

    let mut reader_handles = Vec::with_capacity(thread_count as usize);
    for thread_index in 0..thread_count {
        let db_path = db_path.to_path_buf();
        let reader_barrier = Arc::clone(&barrier);
        let seed = profile.seed;
        let rows = profile.rows;
        let (start_op, op_count) = split_ops(profile.point_reads, thread_count, thread_index);
        let trial_local = trial;
        reader_handles.push(thread::spawn(move || {
            let db = Db::open(&db_path, real_fs_default_config(path_parent(&db_path)?))
                .with_context(|| {
                    format!(
                        "open read_under_write reader database {}",
                        db_path.display()
                    )
                })?;
            let select = db.prepare("SELECT payload FROM bench_read_under_write WHERE id = $1")?;
            let mut collector = LatencyCollector::new()?;
            reader_barrier.wait();
            for op in 0..op_count {
                let key = deterministic_id(
                    seed ^ u64::from(thread_index).wrapping_mul(0x9e37_79b9),
                    start_op.saturating_add(op),
                    rows,
                    trial_local,
                );
                let started = Instant::now();
                let result = select.execute(&[Value::Int64(to_i64(key)?)])?;
                collector.record(started.elapsed())?;
                let row_count = result.rows().len();
                if row_count != 1 {
                    return Err(anyhow!(
                        "read_under_write expected exactly 1 row, got {row_count}"
                    ));
                }
            }
            Ok::<LatencyCollector, anyhow::Error>(collector)
        }));
    }

    let writer_throughput = writer_handle
        .join()
        .map_err(|_| anyhow!("writer thread panicked"))??;

    let mut aggregate = LatencyCollector::new()?;
    for handle in reader_handles {
        let result = handle
            .join()
            .map_err(|_| anyhow!("reader-under-write thread panicked"))??;
        aggregate.merge(result)?;
    }

    Ok((aggregate, writer_throughput))
}

fn run_reader_thread(
    db_path: PathBuf,
    rows: u64,
    seed: u64,
    trial: u32,
    start_op: u64,
    op_count: u64,
    thread_index: u32,
) -> Result<LatencyCollector> {
    let db =
        Db::open(&db_path, real_fs_default_config(path_parent(&db_path)?)).with_context(|| {
            format!(
                "open read_under_write reader database {}",
                db_path.display()
            )
        })?;
    let select = db.prepare("SELECT payload FROM bench_read_under_write WHERE id = $1")?;
    let mut collector = LatencyCollector::new()?;

    for op in 0..op_count {
        let key = deterministic_id(
            seed ^ u64::from(thread_index).wrapping_mul(0x85eb_ca6b),
            start_op.saturating_add(op),
            rows,
            trial,
        );
        let started = Instant::now();
        let result = select.execute(&[Value::Int64(to_i64(key)?)])?;
        collector.record(started.elapsed())?;
        let row_count = result.rows().len();
        if row_count != 1 {
            return Err(anyhow!(
                "read_under_write expected exactly 1 row, got {row_count}"
            ));
        }
    }

    Ok(collector)
}

fn run_writer_thread(
    db_path: PathBuf,
    rows: u64,
    writer_ops: u64,
    seed: u64,
    trial: u32,
) -> Result<f64> {
    let db = Db::open_or_create(
        &db_path,
        real_fs_full_durability_config(path_parent(&db_path)?),
    )
    .with_context(|| {
        format!(
            "open read_under_write writer database {}",
            db_path.display()
        )
    })?;
    let update = db.prepare("UPDATE bench_read_under_write SET payload = 'rw' WHERE id = $1")?;

    let started = Instant::now();
    for op in 0..writer_ops {
        let key = deterministic_id(seed.rotate_left(17), op, rows, trial);
        let mut txn = db.transaction()?;
        update.execute_in(&mut txn, &[Value::Int64(to_i64(key)?)])?;
        txn.commit()?;
    }
    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    if elapsed_secs == 0.0 {
        Ok(0.0)
    } else {
        Ok(writer_ops as f64 / elapsed_secs)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ColdPointLookupProbeResult {
    first_read_ns: u64,
    batch_elapsed_ns: u64,
    rows_returned: u64,
}

fn run_cold_point_lookup_probe(
    args: ColdPointLookupProbeArgs,
) -> Result<ColdPointLookupProbeResult> {
    if args.lookups == 0 {
        return Err(anyhow!("--lookups must be greater than 0"));
    }
    let config = real_fs_default_config(path_parent(&args.db_path)?);

    let batch_started = Instant::now();
    let db = Db::open(&args.db_path, config)
        .with_context(|| format!("open cold-point-lookup database {}", args.db_path.display()))?;
    let select = db.prepare("SELECT payload FROM bench_point_lookup_cold WHERE id = $1")?;

    let first_key = deterministic_id(args.seed, args.start_op, args.rows, args.trial);
    let first_started = Instant::now();
    let first_result = select.execute(&[Value::Int64(to_i64(first_key)?)])?;
    let first_read_ns = elapsed_to_ns(first_started.elapsed());
    let mut rows_returned = validate_single_row(&first_result, "cold-point-lookup first")?;

    for offset in 1..args.lookups {
        let key = deterministic_id(
            args.seed,
            args.start_op.saturating_add(offset),
            args.rows,
            args.trial,
        );
        let result = select.execute(&[Value::Int64(to_i64(key)?)])?;
        rows_returned =
            rows_returned.saturating_add(validate_single_row(&result, "cold-point-lookup")?);
    }

    Ok(ColdPointLookupProbeResult {
        first_read_ns,
        batch_elapsed_ns: elapsed_to_ns(batch_started.elapsed()),
        rows_returned,
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct RecoveryReopenProbeResult {
    reopen_elapsed_ns: u64,
    first_query_elapsed_ns: u64,
}

fn run_recovery_reopen_probe(args: RecoveryReopenProbeArgs) -> Result<RecoveryReopenProbeResult> {
    let config = real_fs_default_config(path_parent(&args.db_path)?);

    let reopen_started = Instant::now();
    let db = Db::open(&args.db_path, config)
        .with_context(|| format!("open recovery database {}", args.db_path.display()))?;
    let reopen_elapsed_ns = elapsed_to_ns(reopen_started.elapsed());

    let query_started = Instant::now();
    let result = db.execute("SELECT COUNT(*) FROM bench_recovery_reopen")?;
    let first_query_elapsed_ns = elapsed_to_ns(query_started.elapsed());
    let count = extract_single_count(result)?;
    if count != args.expected_rows {
        return Err(anyhow!(
            "recovery-reopen expected {} rows, found {count}",
            args.expected_rows
        ));
    }

    Ok(RecoveryReopenProbeResult {
        reopen_elapsed_ns,
        first_query_elapsed_ns,
    })
}

fn invoke_cold_point_lookup_probe(
    args: &ColdPointLookupProbeArgs,
) -> Result<ColdPointLookupProbeResult> {
    let output = Command::new(env::current_exe().context("resolve current benchmark binary")?)
        .arg("internal")
        .arg("cold-point-lookup-probe")
        .arg("--db-path")
        .arg(&args.db_path)
        .arg("--rows")
        .arg(args.rows.to_string())
        .arg("--seed")
        .arg(args.seed.to_string())
        .arg("--trial")
        .arg(args.trial.to_string())
        .arg("--start-op")
        .arg(args.start_op.to_string())
        .arg("--lookups")
        .arg(args.lookups.to_string())
        .output()
        .context("launch cold-point-lookup probe child process")?;

    parse_child_json_output(output, "cold-point-lookup-probe")
}

fn invoke_recovery_reopen_probe(
    args: &RecoveryReopenProbeArgs,
) -> Result<RecoveryReopenProbeResult> {
    let output = Command::new(env::current_exe().context("resolve current benchmark binary")?)
        .arg("internal")
        .arg("recovery-reopen-probe")
        .arg("--db-path")
        .arg(&args.db_path)
        .arg("--expected-rows")
        .arg(args.expected_rows.to_string())
        .output()
        .context("launch recovery-reopen probe child process")?;

    parse_child_json_output(output, "recovery-reopen-probe")
}

fn parse_child_json_output<T: serde::de::DeserializeOwned>(
    output: std::process::Output,
    probe_name: &str,
) -> Result<T> {
    if !output.status.success() {
        return Err(anyhow!(
            "{probe_name} child process failed (status={}): stdout={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("decode {probe_name} stdout as UTF-8"))?;
    serde_json::from_str(stdout.trim()).with_context(|| format!("parse {probe_name} JSON output"))
}

fn validate_single_row(result: &QueryResult, context: &str) -> Result<u64> {
    let row_count = result.rows().len();
    if row_count != 1 {
        return Err(anyhow!("{context} expected exactly 1 row, got {row_count}"));
    }
    Ok(1)
}

fn split_ops(total_ops: u64, thread_count: u32, thread_index: u32) -> (u64, u64) {
    let thread_count_u64 = u64::from(thread_count.max(1));
    let index_u64 = u64::from(thread_index);
    let base = total_ops / thread_count_u64;
    let remainder = total_ops % thread_count_u64;
    let count = if index_u64 < remainder {
        base + 1
    } else {
        base
    };
    let start = index_u64
        .saturating_mul(base)
        .saturating_add(index_u64.min(remainder));
    (start, count)
}

fn latency_degradation_ratio(isolation_latency_us: f64, under_load_latency_us: f64) -> f64 {
    if isolation_latency_us == 0.0 {
        0.0
    } else {
        under_load_latency_us / isolation_latency_us
    }
}

fn throughput_degradation_ratio(isolation_ops_per_sec: f64, under_load_ops_per_sec: f64) -> f64 {
    if under_load_ops_per_sec == 0.0 {
        0.0
    } else {
        isolation_ops_per_sec / under_load_ops_per_sec
    }
}

fn real_fs_full_durability_config(temp_dir: &Path) -> DbConfig {
    DbConfig {
        temp_dir: temp_dir.to_path_buf(),
        wal_sync_mode: WalSyncMode::Full,
        ..DbConfig::default()
    }
}

fn real_fs_default_config(temp_dir: &Path) -> DbConfig {
    DbConfig {
        temp_dir: temp_dir.to_path_buf(),
        ..DbConfig::default()
    }
}

fn path_parent(path: &Path) -> Result<&Path> {
    path.parent()
        .ok_or_else(|| anyhow!("path {} has no parent directory", path.display()))
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

fn deterministic_row_id(seed: u64, trial: u32, ordinal: u64) -> u64 {
    let seed_component = seed % 1_000_000;
    seed_component
        .saturating_mul(10_000_000_000)
        .saturating_add(u64::from(trial).saturating_mul(1_000_000_000))
        .saturating_add(ordinal)
        .saturating_add(1)
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

fn elapsed_to_ns(elapsed: Duration) -> u64 {
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

fn ns_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("value {value} exceeds i64"))
}

#[cfg(test)]
mod tests {
    use super::{
        deterministic_id, deterministic_scan_start, run_scenario, throughput_degradation_ratio,
    };
    use crate::profiles::ResolvedProfile;
    use crate::types::{ProfileKind, ScenarioId, ScenarioStatus};
    use tempfile::TempDir;

    fn tiny_profile() -> ResolvedProfile {
        ResolvedProfile {
            kind: ProfileKind::Custom,
            rows: 100,
            point_reads: 100,
            range_scan_rows: 10,
            range_scans: 10,
            durable_commits: 10,
            batch_size: 5,
            cold_batches: 2,
            reader_threads: 2,
            writer_ops: 20,
            warmup_ops: 5,
            trials: 1,
            seed: 7,
        }
    }

    #[test]
    fn point_lookup_scenario_runs_with_tiny_profile() {
        let temp = TempDir::new().expect("tempdir");
        let result = run_scenario(ScenarioId::PointLookupWarm, &tiny_profile(), temp.path())
            .expect("run point lookup scenario");
        assert!(matches!(result.status, ScenarioStatus::Passed));
        assert!(result.metrics.contains_key("lookup_p95_us"));
    }

    #[test]
    fn durable_commit_batch_scenario_runs_with_tiny_profile() {
        let temp = TempDir::new().expect("tempdir");
        let result = run_scenario(ScenarioId::DurableCommitBatch, &tiny_profile(), temp.path())
            .expect("run durable commit batch scenario");
        assert!(matches!(result.status, ScenarioStatus::Passed));
        assert!(result.metrics.contains_key("batch_commit_p95_us"));
        assert!(result.metrics.contains_key("rows_per_batch"));
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

    #[test]
    fn throughput_degradation_ratio_uses_isolation_over_under_load() {
        let ratio = throughput_degradation_ratio(200.0, 150.0);
        assert!((ratio - 1.333_333_333).abs() < 0.000_001);
    }
}
