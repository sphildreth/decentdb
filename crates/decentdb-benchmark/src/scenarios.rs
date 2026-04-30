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

use decentdb::benchmark::{
    reset_read_path_counters, snapshot_vfs_stats, take_read_path_counters, ReadPathCounters,
    VfsStats, VfsStatsScope,
};
use decentdb::{Db, DbConfig, QueryResult, Value, WalSyncMode};

use crate::cli::{ColdPointLookupProbeArgs, InternalCommand, RecoveryReopenProbeArgs};
use crate::profiles::ResolvedProfile;
use crate::storage_inspector::inspect_db_file;
use crate::types::{HistogramSummary, ScenarioId, ScenarioResult, ScenarioStatus};

const LOOKUP_STRIDE: u64 = 8_191;
const RANGE_STRIDE: u64 = 31;
const COMPLEX_ORDER_STATUSES: [&str; 3] = ["COMPLETED", "PENDING", "SHIPPED"];
const COMPLEX_PAYMENT_METHODS: [&str; 3] = ["CREDIT_CARD", "PAYPAL", "CRYPTO"];

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
        ScenarioId::ComplexEcommerce => complex_ecommerce(profile, scenario_scratch),
        ScenarioId::PointLookupWarm => point_lookup_warm(profile),
        ScenarioId::PointLookupCold => point_lookup_cold(profile, scenario_scratch),
        ScenarioId::RangeScanWarm => range_scan_warm(profile),
        ScenarioId::Checkpoint => checkpoint(profile, scenario_scratch),
        ScenarioId::RecoveryReopen => recovery_reopen(profile, scenario_scratch),
        ScenarioId::ReadUnderWrite => read_under_write(profile, scenario_scratch),
        ScenarioId::StorageEfficiency => storage_efficiency(profile, scenario_scratch),
        ScenarioId::MemoryFootprint => memory_footprint(profile, scenario_scratch),
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

fn add_vfs_stats(accum: &mut Option<VfsStats>, sample: VfsStats) {
    match accum {
        Some(existing) => {
            existing.db.open_calls = existing.db.open_calls.saturating_add(sample.db.open_calls);
            existing.db.read_calls = existing.db.read_calls.saturating_add(sample.db.read_calls);
            existing.db.write_calls = existing
                .db
                .write_calls
                .saturating_add(sample.db.write_calls);
            existing.db.bytes_read = existing.db.bytes_read.saturating_add(sample.db.bytes_read);
            existing.db.bytes_written = existing
                .db
                .bytes_written
                .saturating_add(sample.db.bytes_written);
            existing.db.sync_data_calls = existing
                .db
                .sync_data_calls
                .saturating_add(sample.db.sync_data_calls);
            existing.db.sync_metadata_calls = existing
                .db
                .sync_metadata_calls
                .saturating_add(sample.db.sync_metadata_calls);
            existing.db.set_len_calls = existing
                .db
                .set_len_calls
                .saturating_add(sample.db.set_len_calls);

            existing.wal.open_calls = existing
                .wal
                .open_calls
                .saturating_add(sample.wal.open_calls);
            existing.wal.read_calls = existing
                .wal
                .read_calls
                .saturating_add(sample.wal.read_calls);
            existing.wal.write_calls = existing
                .wal
                .write_calls
                .saturating_add(sample.wal.write_calls);
            existing.wal.bytes_read = existing
                .wal
                .bytes_read
                .saturating_add(sample.wal.bytes_read);
            existing.wal.bytes_written = existing
                .wal
                .bytes_written
                .saturating_add(sample.wal.bytes_written);
            existing.wal.sync_data_calls = existing
                .wal
                .sync_data_calls
                .saturating_add(sample.wal.sync_data_calls);
            existing.wal.sync_metadata_calls = existing
                .wal
                .sync_metadata_calls
                .saturating_add(sample.wal.sync_metadata_calls);
            existing.wal.set_len_calls = existing
                .wal
                .set_len_calls
                .saturating_add(sample.wal.set_len_calls);

            existing.open_create_like_calls = existing
                .open_create_like_calls
                .saturating_add(sample.open_create_like_calls);
            existing.file_exists_calls = existing
                .file_exists_calls
                .saturating_add(sample.file_exists_calls);
            existing.remove_file_calls = existing
                .remove_file_calls
                .saturating_add(sample.remove_file_calls);
            existing.canonicalize_calls = existing
                .canonicalize_calls
                .saturating_add(sample.canonicalize_calls);
        }
        None => *accum = Some(sample),
    }
}

fn vfs_stats_to_json(stats: VfsStats) -> serde_json::Value {
    let total = stats.total();
    json!({
        "db": {
            "open_calls": stats.db.open_calls,
            "read_calls": stats.db.read_calls,
            "write_calls": stats.db.write_calls,
            "bytes_read": stats.db.bytes_read,
            "bytes_written": stats.db.bytes_written,
            "sync_data_calls": stats.db.sync_data_calls,
            "sync_metadata_calls": stats.db.sync_metadata_calls,
            "sync_calls": stats.db.sync_calls(),
            "set_len_calls": stats.db.set_len_calls
        },
        "wal": {
            "open_calls": stats.wal.open_calls,
            "read_calls": stats.wal.read_calls,
            "write_calls": stats.wal.write_calls,
            "bytes_read": stats.wal.bytes_read,
            "bytes_written": stats.wal.bytes_written,
            "sync_data_calls": stats.wal.sync_data_calls,
            "sync_metadata_calls": stats.wal.sync_metadata_calls,
            "sync_calls": stats.wal.sync_calls(),
            "set_len_calls": stats.wal.set_len_calls
        },
        "total": {
            "open_calls": total.open_calls,
            "read_calls": total.read_calls,
            "write_calls": total.write_calls,
            "bytes_read": total.bytes_read,
            "bytes_written": total.bytes_written,
            "sync_data_calls": total.sync_data_calls,
            "sync_metadata_calls": total.sync_metadata_calls,
            "sync_calls": total.sync_calls(),
            "set_len_calls": total.set_len_calls
        },
        "open_create_like_calls": stats.open_create_like_calls,
        "file_exists_calls": stats.file_exists_calls,
        "remove_file_calls": stats.remove_file_calls,
        "canonicalize_calls": stats.canonicalize_calls
    })
}

fn durable_commit_single(
    profile: &ResolvedProfile,
    scenario_scratch: &Path,
) -> Result<ScenarioResult> {
    let mut commit_collector = LatencyCollector::new()?;
    let mut txn_collector = LatencyCollector::new()?;
    let mut vfs_stats_accum = None;
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

        let trial_vfs_stats = {
            let _vfs_scope = VfsStatsScope::begin(true);
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
            snapshot_vfs_stats()
        };
        add_vfs_stats(&mut vfs_stats_accum, trial_vfs_stats);

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
    if let Some(vfs) = vfs_stats_accum {
        let total = vfs.total();
        let commits = commit_summary.sample_count.max(1) as f64;
        metrics.insert(
            "bytes_written_per_commit".to_string(),
            json!(total.bytes_written as f64 / commits),
        );
        metrics.insert(
            "write_calls_per_commit".to_string(),
            json!(total.write_calls as f64 / commits),
        );
        metrics.insert(
            "fsyncs_per_commit".to_string(),
            json!(total.sync_calls() as f64 / commits),
        );
        metrics.insert(
            "db_bytes_written_per_commit".to_string(),
            json!(vfs.db.bytes_written as f64 / commits),
        );
        metrics.insert(
            "wal_bytes_written_per_commit".to_string(),
            json!(vfs.wal.bytes_written as f64 / commits),
        );
    }

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
            "VFS write/sync attribution is collected with benchmark-only StatsVfs counters."
                .to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(txn_summary),
        vfs_stats: vfs_stats_accum.map(vfs_stats_to_json),
        artifacts: Vec::new(),
    })
}

fn durable_commit_batch(
    profile: &ResolvedProfile,
    scenario_scratch: &Path,
) -> Result<ScenarioResult> {
    let mut commit_collector = LatencyCollector::new()?;
    let mut txn_collector = LatencyCollector::new()?;
    let mut vfs_stats_accum = None;

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

        let trial_vfs_stats = {
            let _vfs_scope = VfsStatsScope::begin(true);
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
            snapshot_vfs_stats()
        };
        add_vfs_stats(&mut vfs_stats_accum, trial_vfs_stats);

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
    if let Some(vfs) = vfs_stats_accum {
        let total = vfs.total();
        let batches = commit_summary.sample_count.max(1) as f64;
        let rows = total_rows_written.max(1) as f64;
        metrics.insert(
            "bytes_written_per_batch".to_string(),
            json!(total.bytes_written as f64 / batches),
        );
        metrics.insert(
            "bytes_written_per_row".to_string(),
            json!(total.bytes_written as f64 / rows),
        );
        metrics.insert(
            "fsyncs_per_batch".to_string(),
            json!(total.sync_calls() as f64 / batches),
        );
        metrics.insert(
            "write_calls_per_batch".to_string(),
            json!(total.write_calls as f64 / batches),
        );
    }

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
            "VFS write/sync attribution is collected with benchmark-only StatsVfs counters."
                .to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(commit_summary),
        vfs_stats: vfs_stats_accum.map(vfs_stats_to_json),
        artifacts: Vec::new(),
    })
}

#[derive(Debug)]
struct ComplexUserRow {
    id: i64,
    name: String,
    email: String,
}

#[derive(Debug)]
struct ComplexItemRow {
    id: i64,
    name: String,
    price: f64,
    stock: i64,
}

#[derive(Debug)]
struct ComplexOrderRow {
    id: i64,
    user_id: i64,
    status: String,
    total_amount: f64,
}

#[derive(Debug)]
struct ComplexOrderItemRow {
    order_id: i64,
    item_id: i64,
    quantity: i64,
    price: f64,
}

#[derive(Debug)]
struct ComplexPaymentRow {
    id: i64,
    order_id: i64,
    amount: f64,
    method: String,
    status: String,
}

#[derive(Debug)]
struct ComplexWorkloadData {
    users: Vec<ComplexUserRow>,
    items: Vec<ComplexItemRow>,
    orders: Vec<ComplexOrderRow>,
    order_items: Vec<ComplexOrderItemRow>,
    payments: Vec<ComplexPaymentRow>,
    point_lookup_ids: Vec<i64>,
    range_scan_params: Vec<(f64, f64)>,
    join_statuses: Vec<String>,
    aggregate_params: Vec<(f64, f64)>,
    history_user_ids: Vec<i64>,
    update_ops: Vec<(String, i64)>,
    delete_order_ids: Vec<i64>,
}

fn complex_ecommerce(profile: &ResolvedProfile, scenario_scratch: &Path) -> Result<ScenarioResult> {
    let mut point_lookup_collector = LatencyCollector::new()?;
    let mut range_scan_collector = LatencyCollector::new()?;
    let mut join_collector = LatencyCollector::new()?;
    let mut aggregate_collector = LatencyCollector::new()?;
    let mut history_collector = LatencyCollector::new()?;
    let mut update_collector = LatencyCollector::new()?;
    let mut delete_collector = LatencyCollector::new()?;
    let mut table_scan_collector = LatencyCollector::new()?;

    let mut catalog_insert_seconds_sum = 0.0_f64;
    let mut orders_insert_total_ns = 0_u128;
    let mut report_query_seconds_sum = 0.0_f64;

    let mut point_lookup_rows_total = 0_u64;
    let mut range_scan_rows_total = 0_u64;
    let mut join_rows_total = 0_u64;
    let mut aggregate_rows_total = 0_u64;
    let mut report_rows_total = 0_u64;
    let mut history_rows_total = 0_u64;
    let mut table_scan_count_sum = 0_u64;

    let mut catalog_rows = 0_u64;
    let mut orders_rows = 0_u64;
    let mut deletes_measured = 0_u64;

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("create trial dir {}", trial_dir.display()))?;
        let db_path = trial_dir.join("complex_ecommerce.ddb");

        let db = Db::open_or_create(&db_path, real_fs_full_durability_config(&trial_dir))
            .with_context(|| {
                format!(
                    "open or create complex_ecommerce database {}",
                    db_path.display()
                )
            })?;
        setup_complex_schema(&db)?;

        let workload = build_complex_workload_data(profile, trial)?;
        catalog_rows = workload.users.len() as u64 + workload.items.len() as u64;
        orders_rows = workload.orders.len() as u64
            + workload.order_items.len() as u64
            + workload.payments.len() as u64;
        deletes_measured = workload.delete_order_ids.len() as u64;

        let catalog_started = Instant::now();
        {
            let mut txn = db.transaction()?;
            let insert_user =
                txn.prepare("INSERT INTO users (id, name, email) VALUES ($1, $2, $3)")?;
            for user in workload.users {
                insert_user.execute_in(
                    &mut txn,
                    &[
                        Value::Int64(user.id),
                        Value::Text(user.name),
                        Value::Text(user.email),
                    ],
                )?;
            }

            let insert_item =
                txn.prepare("INSERT INTO items (id, name, price, stock) VALUES ($1, $2, $3, $4)")?;
            for item in workload.items {
                insert_item.execute_in(
                    &mut txn,
                    &[
                        Value::Int64(item.id),
                        Value::Text(item.name),
                        Value::Float64(item.price),
                        Value::Int64(item.stock),
                    ],
                )?;
            }
            txn.commit()?;
        }
        let catalog_elapsed = catalog_started.elapsed();
        catalog_insert_seconds_sum += catalog_elapsed.as_secs_f64();

        let orders_started = Instant::now();
        {
            let mut txn = db.transaction()?;
            let insert_order = txn.prepare(
                "INSERT INTO orders (id, user_id, status, total_amount) VALUES ($1, $2, $3, $4)",
            )?;
            for order in workload.orders {
                insert_order.execute_in(
                    &mut txn,
                    &[
                        Value::Int64(order.id),
                        Value::Int64(order.user_id),
                        Value::Text(order.status),
                        Value::Float64(order.total_amount),
                    ],
                )?;
            }

            let insert_order_item = txn.prepare(
                "INSERT INTO order_items (order_id, item_id, quantity, price) VALUES ($1, $2, $3, $4)",
            )?;
            for order_item in workload.order_items {
                insert_order_item.execute_in(
                    &mut txn,
                    &[
                        Value::Int64(order_item.order_id),
                        Value::Int64(order_item.item_id),
                        Value::Int64(order_item.quantity),
                        Value::Float64(order_item.price),
                    ],
                )?;
            }

            let insert_payment = txn.prepare(
                "INSERT INTO payments (id, order_id, amount, method, status) VALUES ($1, $2, $3, $4, $5)",
            )?;
            for payment in workload.payments {
                insert_payment.execute_in(
                    &mut txn,
                    &[
                        Value::Int64(payment.id),
                        Value::Int64(payment.order_id),
                        Value::Float64(payment.amount),
                        Value::Text(payment.method),
                        Value::Text(payment.status),
                    ],
                )?;
            }
            txn.commit()?;
        }
        let orders_elapsed = orders_started.elapsed();
        orders_insert_total_ns = orders_insert_total_ns.saturating_add(orders_elapsed.as_nanos());

        let point_lookup = db.prepare("SELECT id, name, email FROM users WHERE id = $1")?;
        let range_scan = db.prepare(
            "SELECT id, name, price FROM items WHERE price >= $1 AND price < $2 ORDER BY price LIMIT 100",
        )?;
        let join_query = db.prepare(
            "SELECT o.id, o.total_amount, u.name \
             FROM orders o \
             JOIN users u ON o.user_id = u.id \
             WHERE o.status = $1 \
             LIMIT 50",
        )?;
        let aggregate_query = db.prepare(
            "SELECT status, COUNT(*) AS count, SUM(total_amount) AS total \
             FROM orders \
             WHERE total_amount >= $1 AND total_amount < $2 \
             GROUP BY status",
        )?;
        let report_query = db.prepare(
            "SELECT i.name, SUM(oi.quantity), SUM(oi.quantity * oi.price) AS revenue \
             FROM items i \
             JOIN order_items oi ON i.id = oi.item_id \
             JOIN orders o ON oi.order_id = o.id \
             WHERE o.status = 'COMPLETED' \
             GROUP BY i.id, i.name \
             ORDER BY revenue DESC \
             LIMIT 100",
        )?;
        let history_query = db.prepare(
            "SELECT o.id, o.total_amount, p.status, i.name, oi.quantity, oi.price \
             FROM orders o \
             JOIN payments p ON o.id = p.order_id \
             JOIN order_items oi ON o.id = oi.order_id \
             JOIN items i ON oi.item_id = i.id \
             WHERE o.user_id = $1 \
             ORDER BY o.id DESC",
        )?;
        let update_query = db.prepare("UPDATE users SET email = $1 WHERE id = $2")?;
        let delete_order_items = db.prepare("DELETE FROM order_items WHERE order_id = $1")?;
        let delete_payments = db.prepare("DELETE FROM payments WHERE order_id = $1")?;
        let delete_orders = db.prepare("DELETE FROM orders WHERE id = $1")?;
        let table_scan = db.prepare("SELECT COUNT(*) FROM items")?;

        let warm_user_id = *workload
            .point_lookup_ids
            .first()
            .ok_or_else(|| anyhow!("complex_ecommerce point lookup workload was empty"))?;
        validate_single_row(
            &point_lookup.execute(&[Value::Int64(warm_user_id)])?,
            "complex_ecommerce point lookup warmup",
        )?;

        let warm_range = workload
            .range_scan_params
            .first()
            .copied()
            .ok_or_else(|| anyhow!("complex_ecommerce range scan workload was empty"))?;
        let _ =
            range_scan.execute(&[Value::Float64(warm_range.0), Value::Float64(warm_range.1)])?;

        let warm_status = workload
            .join_statuses
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("complex_ecommerce join workload was empty"))?;
        let _ = join_query.execute(&[Value::Text(warm_status)])?;

        let warm_amounts = workload
            .aggregate_params
            .first()
            .copied()
            .ok_or_else(|| anyhow!("complex_ecommerce aggregate workload was empty"))?;
        let _ = aggregate_query.execute(&[
            Value::Float64(warm_amounts.0),
            Value::Float64(warm_amounts.1),
        ])?;

        let report_warm = report_query.execute(&[])?;
        report_rows_total = report_rows_total.saturating_add(report_warm.rows().len() as u64);

        let warm_history_user = *workload
            .history_user_ids
            .first()
            .ok_or_else(|| anyhow!("complex_ecommerce history workload was empty"))?;
        let _ = history_query.execute(&[Value::Int64(warm_history_user)])?;

        let warm_update = workload
            .update_ops
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("complex_ecommerce update workload was empty"))?;
        {
            let mut txn = db.transaction()?;
            update_query.execute_in(
                &mut txn,
                &[
                    Value::Text(warm_update.0.clone()),
                    Value::Int64(warm_update.1),
                ],
            )?;
            txn.rollback()?;
        }

        let warm_delete_order_id = *workload
            .delete_order_ids
            .first()
            .ok_or_else(|| anyhow!("complex_ecommerce delete workload was empty"))?;
        {
            let mut txn = db.transaction()?;
            delete_order_items.execute_in(&mut txn, &[Value::Int64(warm_delete_order_id)])?;
            delete_payments.execute_in(&mut txn, &[Value::Int64(warm_delete_order_id)])?;
            delete_orders.execute_in(&mut txn, &[Value::Int64(warm_delete_order_id)])?;
            txn.rollback()?;
        }

        for user_id in workload.point_lookup_ids {
            let started = Instant::now();
            let result = point_lookup.execute(&[Value::Int64(user_id)])?;
            point_lookup_collector.record(started.elapsed())?;
            point_lookup_rows_total = point_lookup_rows_total.saturating_add(validate_single_row(
                &result,
                "complex_ecommerce point lookup",
            )?);
        }

        for (low, high) in workload.range_scan_params {
            let started = Instant::now();
            let result = range_scan.execute(&[Value::Float64(low), Value::Float64(high)])?;
            range_scan_collector.record(started.elapsed())?;
            range_scan_rows_total =
                range_scan_rows_total.saturating_add(result.rows().len() as u64);
        }

        for status in workload.join_statuses {
            let started = Instant::now();
            let result = join_query.execute(&[Value::Text(status)])?;
            join_collector.record(started.elapsed())?;
            join_rows_total = join_rows_total.saturating_add(result.rows().len() as u64);
        }

        for (low, high) in workload.aggregate_params {
            let started = Instant::now();
            let result = aggregate_query.execute(&[Value::Float64(low), Value::Float64(high)])?;
            aggregate_collector.record(started.elapsed())?;
            aggregate_rows_total = aggregate_rows_total.saturating_add(result.rows().len() as u64);
        }

        let report_started = Instant::now();
        let report_result = report_query.execute(&[])?;
        report_query_seconds_sum += report_started.elapsed().as_secs_f64();
        report_rows_total = report_rows_total.saturating_add(report_result.rows().len() as u64);

        for user_id in workload.history_user_ids {
            let started = Instant::now();
            let result = history_query.execute(&[Value::Int64(user_id)])?;
            history_collector.record(started.elapsed())?;
            history_rows_total = history_rows_total.saturating_add(result.rows().len() as u64);
        }

        for (email, user_id) in workload.update_ops {
            let mut txn = db.transaction()?;
            let started = Instant::now();
            let result =
                update_query.execute_in(&mut txn, &[Value::Text(email), Value::Int64(user_id)])?;
            update_collector.record(started.elapsed())?;
            txn.commit()?;
            if result.affected_rows() != 1 {
                return Err(anyhow!(
                    "complex_ecommerce update expected 1 affected row, got {}",
                    result.affected_rows()
                ));
            }
        }

        for order_id in workload.delete_order_ids {
            let mut txn = db.transaction()?;
            let started = Instant::now();
            let _ = delete_order_items.execute_in(&mut txn, &[Value::Int64(order_id)])?;
            let payment_result = delete_payments.execute_in(&mut txn, &[Value::Int64(order_id)])?;
            let order_result = delete_orders.execute_in(&mut txn, &[Value::Int64(order_id)])?;
            delete_collector.record(started.elapsed())?;
            txn.commit()?;
            if payment_result.affected_rows() != 1 {
                return Err(anyhow!(
                    "complex_ecommerce delete expected 1 payment row, got {}",
                    payment_result.affected_rows()
                ));
            }
            if order_result.affected_rows() != 1 {
                return Err(anyhow!(
                    "complex_ecommerce delete expected 1 order row, got {}",
                    order_result.affected_rows()
                ));
            }
        }

        for _ in 0..profile.complex_table_scans {
            let started = Instant::now();
            let result = table_scan.execute(&[])?;
            table_scan_collector.record(started.elapsed())?;
            table_scan_count_sum =
                table_scan_count_sum.saturating_add(extract_single_count(result)?);
        }
    }

    let point_summary = point_lookup_collector.summary();
    let range_summary = range_scan_collector.summary();
    let join_summary = join_collector.summary();
    let aggregate_summary = aggregate_collector.summary();
    let history_summary = history_collector.summary();
    let update_summary = update_collector.summary();
    let delete_summary = delete_collector.summary();
    let table_scan_summary = table_scan_collector.summary();
    let trial_count_f64 = f64::from(profile.trials);
    let orders_insert_rps = if orders_insert_total_ns == 0 {
        0.0
    } else {
        let seconds = orders_insert_total_ns as f64 / 1_000_000_000.0;
        (orders_rows.saturating_mul(u64::from(profile.trials))) as f64 / seconds
    };

    let mut metrics = BTreeMap::new();
    metrics.insert(
        "catalog_insert_s".to_string(),
        json!(catalog_insert_seconds_sum / trial_count_f64),
    );
    metrics.insert("catalog_rows".to_string(), json!(catalog_rows));
    metrics.insert("orders_insert_rps".to_string(), json!(orders_insert_rps));
    metrics.insert("orders_rows".to_string(), json!(orders_rows));
    metrics.insert(
        "point_lookup_p50_ms".to_string(),
        json!(point_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "point_lookup_p95_ms".to_string(),
        json!(point_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "range_scan_p50_ms".to_string(),
        json!(range_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "range_scan_p95_ms".to_string(),
        json!(range_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "join_p50_ms".to_string(),
        json!(join_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "join_p95_ms".to_string(),
        json!(join_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "aggregate_p50_ms".to_string(),
        json!(aggregate_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "aggregate_p95_ms".to_string(),
        json!(aggregate_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "report_query_s".to_string(),
        json!(report_query_seconds_sum / trial_count_f64),
    );
    metrics.insert(
        "history_p50_ms".to_string(),
        json!(history_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "history_p95_ms".to_string(),
        json!(history_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "update_p50_ms".to_string(),
        json!(update_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "update_p95_ms".to_string(),
        json!(update_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "delete_p50_ms".to_string(),
        json!(delete_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "delete_p95_ms".to_string(),
        json!(delete_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "table_scan_p50_ms".to_string(),
        json!(table_scan_summary.p50_us / 1_000.0),
    );
    metrics.insert(
        "table_scan_p95_ms".to_string(),
        json!(table_scan_summary.p95_us / 1_000.0),
    );
    metrics.insert(
        "point_lookup_rows_total".to_string(),
        json!(point_lookup_rows_total),
    );
    metrics.insert(
        "range_scan_rows_total".to_string(),
        json!(range_scan_rows_total),
    );
    metrics.insert("join_rows_total".to_string(), json!(join_rows_total));
    metrics.insert(
        "aggregate_rows_total".to_string(),
        json!(aggregate_rows_total),
    );
    metrics.insert("report_rows_total".to_string(), json!(report_rows_total));
    metrics.insert("history_rows_total".to_string(), json!(history_rows_total));
    metrics.insert("deletes_measured".to_string(), json!(deletes_measured));
    metrics.insert(
        "table_scan_count_sum".to_string(),
        json!(table_scan_count_sum),
    );

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::ComplexEcommerce,
        profile: profile.kind,
        workload: ScenarioId::ComplexEcommerce.default_workload().to_string(),
        durability_mode: "full".to_string(),
        cache_mode: "mixed".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings: Vec::new(),
        notes: vec![
            "Rust-native parity scenario for bindings/python/benchmarks/bench_complex.py."
                .to_string(),
            "Schema, workload ordering, and primary metric names intentionally mirror the Python complex benchmark."
                .to_string(),
            "Read queries use one warmup execution each, matching the Python benchmark semantics rather than profile.warmup_ops."
                .to_string(),
        ],
        scale: profile.scale_json(),
        histograms: None,
        vfs_stats: None,
        artifacts: Vec::new(),
    })
}

fn setup_complex_schema(db: &Db) -> Result<()> {
    db.execute(
        "CREATE TABLE users (
            id INT64 PRIMARY KEY,
            name TEXT,
            email TEXT
        )",
    )?;
    db.execute(
        "CREATE TABLE items (
            id INT64 PRIMARY KEY,
            name TEXT,
            price FLOAT64,
            stock INT64
        )",
    )?;
    db.execute(
        "CREATE TABLE orders (
            id INT64 PRIMARY KEY,
            user_id INT64,
            status TEXT,
            total_amount FLOAT64,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )",
    )?;
    db.execute(
        "CREATE TABLE order_items (
            order_id INT64,
            item_id INT64,
            quantity INT64,
            price FLOAT64,
            FOREIGN KEY(order_id) REFERENCES orders(id),
            FOREIGN KEY(item_id) REFERENCES items(id)
        )",
    )?;
    db.execute(
        "CREATE TABLE payments (
            id INT64 PRIMARY KEY,
            order_id INT64,
            amount FLOAT64,
            method TEXT,
            status TEXT,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )",
    )?;

    db.execute("CREATE INDEX idx_orders_user_id ON orders(user_id)")?;
    db.execute("CREATE INDEX idx_orders_status ON orders(status)")?;
    db.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id)")?;
    db.execute("CREATE INDEX idx_order_items_item_id ON order_items(item_id)")?;
    db.execute("CREATE INDEX idx_payments_order_id ON payments(order_id)")?;
    Ok(())
}

fn build_complex_workload_data(
    profile: &ResolvedProfile,
    trial: u32,
) -> Result<ComplexWorkloadData> {
    let users_count = profile.complex_users;
    let items_count = profile.complex_items;
    let orders_count = profile.complex_orders;
    let seed = profile.seed ^ 0x9e37_79b9_7f4a_7c15 ^ u64::from(trial).rotate_left(11);

    let mut users = Vec::new();
    for user_id in 1..=users_count {
        users.push(ComplexUserRow {
            id: to_i64(user_id)?,
            name: format!("User_{user_id}"),
            email: format!("user{user_id}@example.com"),
        });
    }

    let mut items = Vec::new();
    let mut min_price = f64::INFINITY;
    let mut max_price = f64::NEG_INFINITY;
    for item_id in 1..=items_count {
        let price_cents = 500_u64.saturating_add(complex_mix(seed, 0x10, item_id) % 49_500);
        let price = price_cents as f64 / 100.0;
        let stock = 10_u64.saturating_add(complex_mix(seed, 0x11, item_id) % 9_991);
        min_price = min_price.min(price);
        max_price = max_price.max(price);
        items.push(ComplexItemRow {
            id: to_i64(item_id)?,
            name: format!("Item_{item_id}"),
            price,
            stock: to_i64(stock)?,
        });
    }

    let mut orders = Vec::new();
    let mut order_items = Vec::new();
    let mut payments = Vec::new();
    let mut min_amount = f64::INFINITY;
    let mut max_amount = f64::NEG_INFINITY;
    for order_id in 1..=orders_count {
        let user_id = complex_pick_id(seed, 0x20, order_id, users_count);
        let item_count = 1_u64.saturating_add(complex_mix(seed, 0x21, order_id) % 5);

        let mut total_amount = 0.0_f64;
        for slot in 0..item_count {
            let item_id = complex_pick_id(
                seed ^ order_id.rotate_left(7),
                0x22_u64.saturating_add(slot),
                order_id,
                items_count,
            );
            let quantity =
                1_u64.saturating_add(complex_mix(seed ^ slot.rotate_left(3), 0x23, order_id) % 3);
            let item = &items[(item_id - 1) as usize];
            total_amount += item.price * quantity as f64;
            order_items.push(ComplexOrderItemRow {
                order_id: to_i64(order_id)?,
                item_id: to_i64(item_id)?,
                quantity: to_i64(quantity)?,
                price: item.price,
            });
        }

        let status_index =
            (complex_mix(seed, 0x24, order_id) % COMPLEX_ORDER_STATUSES.len() as u64) as usize;
        let status = COMPLEX_ORDER_STATUSES[status_index].to_string();
        let payment_method = COMPLEX_PAYMENT_METHODS
            [(complex_mix(seed, 0x25, order_id) % COMPLEX_PAYMENT_METHODS.len() as u64) as usize]
            .to_string();
        let payment_status = if status == "COMPLETED" || status == "SHIPPED" {
            "PAID"
        } else {
            "PENDING"
        }
        .to_string();

        min_amount = min_amount.min(total_amount);
        max_amount = max_amount.max(total_amount);
        orders.push(ComplexOrderRow {
            id: to_i64(order_id)?,
            user_id: to_i64(user_id)?,
            status,
            total_amount,
        });
        payments.push(ComplexPaymentRow {
            id: to_i64(order_id)?,
            order_id: to_i64(order_id)?,
            amount: total_amount,
            method: payment_method,
            status: payment_status,
        });
    }

    let point_lookup_ids = (0..profile.complex_point_lookups)
        .map(|op| to_i64(complex_pick_id(seed, 0x30, op, users_count)))
        .collect::<Result<Vec<_>>>()?;
    let range_scan_params = build_complex_range_params(
        seed,
        0x40,
        profile.complex_range_scans,
        min_price,
        max_price,
    );
    let join_statuses = (0..profile.complex_joins)
        .map(|op| {
            let index =
                (complex_mix(seed, 0x50, op) % COMPLEX_ORDER_STATUSES.len() as u64) as usize;
            COMPLEX_ORDER_STATUSES[index].to_string()
        })
        .collect::<Vec<_>>();
    let aggregate_params = build_complex_range_params(
        seed,
        0x60,
        profile.complex_aggregates,
        min_amount,
        max_amount,
    );
    let history_user_ids = (0..profile.complex_history_reads)
        .map(|op| to_i64(complex_pick_id(seed, 0x70, op, users_count)))
        .collect::<Result<Vec<_>>>()?;
    let update_ops = (0..profile.complex_updates)
        .map(|op| {
            let user_id = complex_pick_id(seed, 0x80, op, users_count);
            Ok((format!("updated_{user_id}@example.com"), to_i64(user_id)?))
        })
        .collect::<Result<Vec<_>>>()?;
    let delete_count = profile.complex_deletes.min(orders_count);
    let delete_order_ids = (0..delete_count)
        .map(|offset| to_i64(orders_count.saturating_sub(offset)))
        .collect::<Result<Vec<_>>>()?;

    Ok(ComplexWorkloadData {
        users,
        items,
        orders,
        order_items,
        payments,
        point_lookup_ids,
        range_scan_params,
        join_statuses,
        aggregate_params,
        history_user_ids,
        update_ops,
        delete_order_ids,
    })
}

fn build_complex_range_params(
    seed: u64,
    namespace: u64,
    count: u64,
    minimum: f64,
    maximum: f64,
) -> Vec<(f64, f64)> {
    let span = (maximum - minimum).max(0.0);
    let mut params = Vec::new();
    for ordinal in 0..count {
        let low = minimum + span * complex_fraction(seed, namespace, ordinal);
        let high = minimum + span * (complex_fraction(seed, namespace + 1, ordinal) + 0.01);
        params.push((low, high));
    }
    params
}

fn complex_pick_id(seed: u64, namespace: u64, ordinal: u64, upper_bound: u64) -> u64 {
    (complex_mix(seed, namespace, ordinal) % upper_bound.max(1)).saturating_add(1)
}

fn complex_mix(seed: u64, namespace: u64, ordinal: u64) -> u64 {
    splitmix64(
        seed.rotate_left((namespace as u32) % 31)
            ^ namespace.wrapping_mul(0x94d0_49bb_1331_11eb)
            ^ ordinal.wrapping_mul(0xbf58_476d_1ce4_e5b9),
    )
}

fn complex_fraction(seed: u64, namespace: u64, ordinal: u64) -> f64 {
    let value = complex_mix(seed, namespace, ordinal) >> 11;
    value as f64 / ((1_u64 << 53) as f64)
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
    let mut vfs_stats_accum = None;

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
        let trial_vfs_stats = {
            let _vfs_scope = VfsStatsScope::begin(true);
            let checkpoint_started = Instant::now();
            db.checkpoint()?;
            checkpoint_collector.record(checkpoint_started.elapsed())?;
            snapshot_vfs_stats()
        };
        add_vfs_stats(&mut vfs_stats_accum, trial_vfs_stats);
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
    if let Some(vfs) = vfs_stats_accum {
        let total = vfs.total();
        let checkpoints = summary.sample_count.max(1) as f64;
        metrics.insert(
            "checkpoint_bytes_written".to_string(),
            json!(total.bytes_written),
        );
        metrics.insert(
            "checkpoint_fsync_calls".to_string(),
            json!(total.sync_calls()),
        );
        metrics.insert(
            "bytes_written_per_checkpoint".to_string(),
            json!(total.bytes_written as f64 / checkpoints),
        );
        metrics.insert(
            "fsyncs_per_checkpoint".to_string(),
            json!(total.sync_calls() as f64 / checkpoints),
        );
    }

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
            "VFS write/sync attribution is collected with benchmark-only StatsVfs counters.".to_string(),
        ],
        scale: profile.scale_json(),
        histograms: Some(summary),
        vfs_stats: vfs_stats_accum.map(vfs_stats_to_json),
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
    let mut reader_iso_counters = ReadPathCounters::default();
    let mut reader_under_counters = ReadPathCounters::default();

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir)
            .with_context(|| format!("create trial dir {}", trial_dir.display()))?;

        let reader_iso_path = trial_dir.join("read_under_write_reader_iso.ddb");
        seed_read_under_write_db(&reader_iso_path, profile)?;
        reset_read_path_counters();
        let reader_iso = run_reader_only_workload(&reader_iso_path, profile, trial)?;
        add_read_path_counters(&mut reader_iso_counters, take_read_path_counters());
        reader_iso_agg.merge(reader_iso)?;

        let writer_iso_path = trial_dir.join("read_under_write_writer_iso.ddb");
        seed_read_under_write_db(&writer_iso_path, profile)?;
        writer_iso_sum += run_writer_only_workload(&writer_iso_path, profile, trial)?;

        let mixed_path = trial_dir.join("read_under_write_mixed.ddb");
        seed_read_under_write_db(&mixed_path, profile)?;
        reset_read_path_counters();
        let (reader_under, writer_under) = run_mixed_workload(&mixed_path, profile, trial)?;
        add_read_path_counters(&mut reader_under_counters, take_read_path_counters());
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
    insert_read_path_counter_metrics(
        &mut metrics,
        "reader_iso",
        reader_iso_counters,
        profile
            .point_reads
            .saturating_mul(u64::from(profile.trials)),
        "per_read",
    );
    insert_read_path_counter_metrics(
        &mut metrics,
        "reader_under_write",
        reader_under_counters,
        profile
            .point_reads
            .saturating_add(profile.writer_ops)
            .saturating_mul(u64::from(profile.trials)),
        "per_measured_operation",
    );
    metrics.insert(
        "read_path_write_txn_lock_count".to_string(),
        json!(reader_under_counters.write_txn_lock_count),
    );
    metrics.insert(
        "read_path_held_snapshots_lock_count".to_string(),
        json!(reader_under_counters.held_snapshots_lock_count),
    );
    metrics.insert(
        "read_path_wal_reader_begin_count".to_string(),
        json!(reader_under_counters.wal_reader_begin_count),
    );

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
            "One writer thread and multiple reader threads run against independent Db handles for the same database path."
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

fn add_read_path_counters(total: &mut ReadPathCounters, sample: ReadPathCounters) {
    total.write_txn_lock_count = total
        .write_txn_lock_count
        .saturating_add(sample.write_txn_lock_count);
    total.held_snapshots_lock_count = total
        .held_snapshots_lock_count
        .saturating_add(sample.held_snapshots_lock_count);
    total.wal_reader_begin_count = total
        .wal_reader_begin_count
        .saturating_add(sample.wal_reader_begin_count);
}

fn insert_read_path_counter_metrics(
    metrics: &mut BTreeMap<String, serde_json::Value>,
    prefix: &str,
    counters: ReadPathCounters,
    measured_ops: u64,
    rate_suffix: &str,
) {
    let rate = |count: u64| {
        if measured_ops == 0 {
            0.0
        } else {
            count as f64 / measured_ops as f64
        }
    };

    metrics.insert(
        format!("{prefix}_read_path_write_txn_lock_count"),
        json!(counters.write_txn_lock_count),
    );
    metrics.insert(
        format!("{prefix}_read_path_write_txn_locks_{rate_suffix}"),
        json!(rate(counters.write_txn_lock_count)),
    );
    metrics.insert(
        format!("{prefix}_read_path_held_snapshots_lock_count"),
        json!(counters.held_snapshots_lock_count),
    );
    metrics.insert(
        format!("{prefix}_read_path_held_snapshot_locks_{rate_suffix}"),
        json!(rate(counters.held_snapshots_lock_count)),
    );
    metrics.insert(
        format!("{prefix}_read_path_wal_reader_begin_count"),
        json!(counters.wal_reader_begin_count),
    );
    metrics.insert(
        format!("{prefix}_read_path_wal_reader_begins_{rate_suffix}"),
        json!(rate(counters.wal_reader_begin_count)),
    );
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
    let mut metadata_bytes_sum = 0_u64;
    let mut catalog_manifest_bytes_sum = 0_u64;
    let mut table_data_bytes_sum = 0_u64;
    let mut index_bytes_sum = Some(0_u64);
    let mut freelist_bytes_sum = 0_u64;
    let mut unknown_bytes_sum = 0_u64;
    let mut overflow_bytes_total_sum = 0_u64;
    let mut vfs_stats_accum = None;
    let mut artifacts = Vec::new();
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

        let (wal_peak_this_trial, trial_vfs_stats) = {
            let mut inserted = 0_u64;
            let mut wal_peak = db.storage_info()?.wal_file_size;
            let _vfs_scope = VfsStatsScope::begin(true);
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
                wal_peak = wal_peak.max(db.storage_info()?.wal_file_size);
            }
            db.checkpoint()?;
            (wal_peak, snapshot_vfs_stats())
        };
        add_vfs_stats(&mut vfs_stats_accum, trial_vfs_stats);
        let storage_info = db.storage_info()?;
        let db_file_bytes = file_len(db.path())?;
        let wal_after_checkpoint = storage_info.wal_file_size;
        wal_file_peak_max = wal_file_peak_max.max(wal_peak_this_trial);

        let inspection = inspect_db_file(&db_path)?;
        metadata_bytes_sum = metadata_bytes_sum.saturating_add(inspection.bytes.metadata_bytes);
        catalog_manifest_bytes_sum =
            catalog_manifest_bytes_sum.saturating_add(inspection.bytes.catalog_manifest_bytes);
        table_data_bytes_sum =
            table_data_bytes_sum.saturating_add(inspection.bytes.table_data_bytes);
        index_bytes_sum = match (index_bytes_sum, inspection.bytes.index_bytes) {
            (Some(sum), Some(value)) => Some(sum.saturating_add(value)),
            _ => None,
        };
        freelist_bytes_sum = freelist_bytes_sum.saturating_add(inspection.bytes.freelist_bytes);
        unknown_bytes_sum = unknown_bytes_sum.saturating_add(inspection.bytes.unknown_bytes);
        overflow_bytes_total_sum =
            overflow_bytes_total_sum.saturating_add(inspection.bytes.overflow_bytes_total);

        let inspection_artifact =
            scenario_scratch.join(format!("storage-inspection-trial-{}.json", trial + 1));
        fs::write(
            &inspection_artifact,
            serde_json::to_vec_pretty(&inspection)?,
        )
        .with_context(|| format!("write storage inspection {}", inspection_artifact.display()))?;
        artifacts.push(inspection_artifact.display().to_string());
        warnings.extend(
            inspection
                .warnings
                .iter()
                .map(|warning| format!("storage_inspector: {warning}")),
        );

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
    warnings.sort();
    warnings.dedup();

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
    metrics.insert(
        "metadata_bytes".to_string(),
        json!(metadata_bytes_sum / u64::from(profile.trials)),
    );
    metrics.insert(
        "catalog_manifest_bytes".to_string(),
        json!(catalog_manifest_bytes_sum / u64::from(profile.trials)),
    );
    metrics.insert(
        "table_data_bytes".to_string(),
        json!(table_data_bytes_sum / u64::from(profile.trials)),
    );
    metrics.insert(
        "index_bytes".to_string(),
        match index_bytes_sum {
            Some(total) => json!(total / u64::from(profile.trials)),
            None => serde_json::Value::Null,
        },
    );
    metrics.insert(
        "freelist_bytes".to_string(),
        json!(freelist_bytes_sum / u64::from(profile.trials)),
    );
    metrics.insert(
        "overflow_bytes_total".to_string(),
        json!(overflow_bytes_total_sum / u64::from(profile.trials)),
    );
    metrics.insert(
        "unknown_bytes".to_string(),
        json!(unknown_bytes_sum / u64::from(profile.trials)),
    );
    metrics.insert(
        "table_data_space_amplification".to_string(),
        json!(if logical_payload_bytes == 0 {
            0.0
        } else {
            (table_data_bytes_sum / u64::from(profile.trials)) as f64 / logical_payload_bytes as f64
        }),
    );
    if let Some(vfs) = vfs_stats_accum {
        let total = vfs.total();
        let total_rows_measured = profile
            .rows
            .saturating_mul(u64::from(profile.trials))
            .max(1);
        metrics.insert(
            "bytes_written_total".to_string(),
            json!(total.bytes_written),
        );
        metrics.insert(
            "bytes_written_per_row".to_string(),
            json!(total.bytes_written as f64 / total_rows_measured as f64),
        );
        metrics.insert("fsync_calls_total".to_string(), json!(total.sync_calls()));
    }

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
            "Phase 3 adds page-category storage attribution via storage inspector output."
                .to_string(),
        ],
        scale: profile.scale_json(),
        histograms: None,
        vfs_stats: vfs_stats_accum.map(vfs_stats_to_json),
        artifacts,
    })
}

fn memory_footprint(profile: &ResolvedProfile, scenario_scratch: &Path) -> Result<ScenarioResult> {
    let payload = "memory-footprint";
    let row_logical_bytes = 8_u64.saturating_add(payload.len() as u64);
    let logical_payload_bytes = profile.rows.saturating_mul(row_logical_bytes);
    let mut steady_sum = 0_u64;
    let mut peak_sum = 0_u64;
    let mut after_reopen_sum = 0_u64;
    let mut steady_count = 0_u32;
    let mut peak_count = 0_u32;
    let mut after_reopen_count = 0_u32;
    let mut sample_count = 0_u64;
    let mut warnings = Vec::new();

    if !cfg!(target_os = "linux") {
        warnings.push(
            "memory_footprint currently measures RSS via /proc/self/status and is only supported on Linux"
                .to_string(),
        );
        let mut metrics = BTreeMap::new();
        metrics.insert("rss_steady_bytes".to_string(), serde_json::Value::Null);
        metrics.insert("rss_peak_bytes".to_string(), serde_json::Value::Null);
        metrics.insert(
            "rss_after_reopen_bytes".to_string(),
            serde_json::Value::Null,
        );
        metrics.insert("memory_amplification".to_string(), serde_json::Value::Null);
        metrics.insert(
            "logical_payload_bytes".to_string(),
            json!(logical_payload_bytes),
        );
        metrics.insert("rss_samples_collected".to_string(), json!(0_u64));
        return Ok(ScenarioResult {
            status: ScenarioStatus::Passed,
            error_class: None,
            scenario_id: ScenarioId::MemoryFootprint,
            profile: profile.kind,
            workload: ScenarioId::MemoryFootprint.default_workload().to_string(),
            durability_mode: "full".to_string(),
            cache_mode: "real_fs".to_string(),
            trial_count: profile.trials,
            metrics,
            warnings,
            notes: vec![
                "RSS is reported as null outside Linux because this phase avoids non-portable guesses."
                    .to_string(),
            ],
            scale: profile.scale_json(),
            histograms: None,
            vfs_stats: None,
            artifacts: Vec::new(),
        });
    }

    for trial in 0..profile.trials {
        let trial_dir = scenario_scratch.join(format!("trial-{}", trial + 1));
        fs::create_dir_all(&trial_dir).with_context(|| {
            format!("create memory footprint trial dir {}", trial_dir.display())
        })?;
        let db_path = trial_dir.join("memory_footprint.ddb");

        let config = real_fs_full_durability_config(&trial_dir);
        let db = Db::open_or_create(&db_path, config.clone())
            .with_context(|| format!("open memory footprint database {}", db_path.display()))?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS bench_memory_footprint (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )?;

        let mut inserted = 0_u64;
        while inserted < profile.rows {
            let mut txn = db.transaction()?;
            let insert = txn.prepare(
                "INSERT INTO bench_memory_footprint (id, payload) VALUES ($1, 'memory-footprint')",
            )?;
            let end = (inserted + profile.batch_size).min(profile.rows);
            while inserted < end {
                let id = inserted.saturating_add(1);
                insert.execute_in(&mut txn, &[Value::Int64(to_i64(id)?)])?;
                inserted = inserted.saturating_add(1);
            }
            txn.commit()?;
        }
        db.checkpoint()?;

        let select = db.prepare("SELECT payload FROM bench_memory_footprint WHERE id = $1")?;
        for warmup in 0..profile.warmup_ops {
            let key = deterministic_id(profile.seed, warmup, profile.rows.max(1), trial);
            let _ = select.execute(&[Value::Int64(to_i64(key)?)])?;
        }

        let before_window = read_linux_rss_snapshot()?;
        let mut sampled_peak = before_window.rss_bytes;
        for op in 0..profile.point_reads {
            let key = deterministic_id(
                profile.seed ^ 0xa5a5_5a5a,
                op.saturating_add(profile.warmup_ops),
                profile.rows.max(1),
                trial,
            );
            let result = select.execute(&[Value::Int64(to_i64(key)?)])?;
            validate_single_row(&result, "memory-footprint lookup")?;
            if op % 64 == 0 {
                let rss = read_linux_rss_snapshot()?.rss_bytes;
                sampled_peak = sampled_peak.max(rss);
                sample_count = sample_count.saturating_add(1);
            }
        }
        let after_window = read_linux_rss_snapshot()?;
        sampled_peak = sampled_peak.max(after_window.rss_bytes);
        if after_window.hwm_bytes > before_window.hwm_bytes {
            sampled_peak = sampled_peak.max(after_window.hwm_bytes);
        }

        steady_sum = steady_sum.saturating_add(before_window.rss_bytes);
        peak_sum = peak_sum.saturating_add(sampled_peak);
        steady_count = steady_count.saturating_add(1);
        peak_count = peak_count.saturating_add(1);

        drop(db);
        let reopened = Db::open(&db_path, config)?;
        let _ =
            extract_single_count(reopened.execute("SELECT COUNT(*) FROM bench_memory_footprint")?)?;
        let reopen_rss = read_linux_rss_snapshot()?.rss_bytes;
        after_reopen_sum = after_reopen_sum.saturating_add(reopen_rss);
        after_reopen_count = after_reopen_count.saturating_add(1);
    }

    warnings.push(
        "RSS values are process-level snapshots from /proc/self/status; transient spikes between samples may be missed."
            .to_string(),
    );
    warnings.push(
        "VmHWM is process-global; window peak may include previous allocations when run in a long-lived process."
            .to_string(),
    );

    let rss_steady_avg = if steady_count == 0 {
        None
    } else {
        Some(steady_sum / u64::from(steady_count))
    };
    let rss_peak_avg = if peak_count == 0 {
        None
    } else {
        Some(peak_sum / u64::from(peak_count))
    };
    let rss_after_reopen_avg = if after_reopen_count == 0 {
        None
    } else {
        Some(after_reopen_sum / u64::from(after_reopen_count))
    };

    let peak_rss_mb = rss_peak_avg.map(|b| b as f64 / (1024.0 * 1024.0));
    let mut metrics = BTreeMap::new();
    metrics.insert(
        "rss_steady_bytes".to_string(),
        rss_steady_avg.map_or(serde_json::Value::Null, |value| json!(value)),
    );
    metrics.insert(
        "rss_peak_bytes".to_string(),
        rss_peak_avg.map_or(serde_json::Value::Null, |value| json!(value)),
    );
    metrics.insert(
        "peak_rss_mb".to_string(),
        peak_rss_mb.map_or(serde_json::Value::Null, |v| json!(v)),
    );
    metrics.insert(
        "rss_after_reopen_bytes".to_string(),
        rss_after_reopen_avg.map_or(serde_json::Value::Null, |value| json!(value)),
    );
    metrics.insert(
        "logical_payload_bytes".to_string(),
        json!(logical_payload_bytes),
    );
    metrics.insert("rss_samples_collected".to_string(), json!(sample_count));
    metrics.insert(
        "memory_amplification".to_string(),
        match rss_steady_avg {
            Some(steady) if logical_payload_bytes > 0 => {
                json!(steady as f64 / logical_payload_bytes as f64)
            }
            _ => serde_json::Value::Null,
        },
    );

    Ok(ScenarioResult {
        status: ScenarioStatus::Passed,
        error_class: None,
        scenario_id: ScenarioId::MemoryFootprint,
        profile: profile.kind,
        workload: ScenarioId::MemoryFootprint.default_workload().to_string(),
        durability_mode: "full".to_string(),
        cache_mode: "real_fs".to_string(),
        trial_count: profile.trials,
        metrics,
        warnings,
        notes: vec![
            "memory_footprint reports Linux RSS snapshots (VmRSS) and sampled peak during measured lookups."
                .to_string(),
            "Values are intentionally process-scoped and avoid allocator-specific attribution claims."
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
    run_writer_on_db_path(
        db_path,
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
        run_writer_on_db_path(&writer_path, writer_rows, writer_ops, writer_seed, trial)
    });

    let mut reader_handles = Vec::with_capacity(thread_count as usize);
    for thread_index in 0..thread_count {
        let reader_path = db_path.to_path_buf();
        let reader_barrier = Arc::clone(&barrier);
        let seed = profile.seed;
        let rows = profile.rows;
        let (start_op, op_count) = split_ops(profile.point_reads, thread_count, thread_index);
        let trial_local = trial;
        reader_handles.push(thread::spawn(move || {
            reader_barrier.wait();
            run_reader_thread(
                reader_path,
                rows,
                seed,
                trial_local,
                start_op,
                op_count,
                thread_index,
            )
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

fn run_writer_on_db(db: &Db, rows: u64, writer_ops: u64, seed: u64, trial: u32) -> Result<f64> {
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

fn run_writer_on_db_path(
    db_path: &Path,
    rows: u64,
    writer_ops: u64,
    seed: u64,
    trial: u32,
) -> Result<f64> {
    let db = Db::open_or_create(
        db_path,
        real_fs_full_durability_config(path_parent(db_path)?),
    )
    .with_context(|| {
        format!(
            "open read_under_write writer database {}",
            db_path.display()
        )
    })?;
    run_writer_on_db(&db, rows, writer_ops, seed, trial)
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

#[derive(Debug, Clone, Copy)]
struct LinuxRssSnapshot {
    rss_bytes: u64,
    hwm_bytes: u64,
}

fn read_linux_rss_snapshot() -> Result<LinuxRssSnapshot> {
    let status = fs::read_to_string("/proc/self/status")
        .context("read /proc/self/status for RSS metrics")?;
    let rss_kib = parse_proc_status_kib(&status, "VmRSS:")
        .ok_or_else(|| anyhow!("VmRSS was not found in /proc/self/status"))?;
    let hwm_kib = parse_proc_status_kib(&status, "VmHWM:")
        .ok_or_else(|| anyhow!("VmHWM was not found in /proc/self/status"))?;
    Ok(LinuxRssSnapshot {
        rss_bytes: rss_kib.saturating_mul(1024),
        hwm_bytes: hwm_kib.saturating_mul(1024),
    })
}

fn parse_proc_status_kib(contents: &str, key: &str) -> Option<u64> {
    contents.lines().find_map(|line| {
        let stripped = line.strip_prefix(key)?;
        stripped
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok())
    })
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
        deterministic_id, deterministic_scan_start, load_id_table_chunked,
        real_fs_full_durability_config, run_cold_point_lookup_probe, run_scenario,
        throughput_degradation_ratio, ColdPointLookupProbeArgs,
    };
    use crate::profiles::ResolvedProfile;
    use crate::types::{ProfileKind, ScenarioId, ScenarioStatus};
    use decentdb::Db;
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
            complex_users: 20,
            complex_items: 10,
            complex_orders: 50,
            complex_history_reads: 20,
            complex_point_lookups: 20,
            complex_range_scans: 20,
            complex_joins: 20,
            complex_aggregates: 20,
            complex_updates: 20,
            complex_deletes: 10,
            complex_table_scans: 5,
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
    fn complex_ecommerce_scenario_runs_with_tiny_profile() {
        let temp = TempDir::new().expect("tempdir");
        let result = run_scenario(ScenarioId::ComplexEcommerce, &tiny_profile(), temp.path())
            .expect("run complex ecommerce scenario");
        assert!(matches!(result.status, ScenarioStatus::Passed));
        assert!(result.metrics.contains_key("orders_insert_rps"));
        assert!(result.metrics.contains_key("report_query_s"));
        assert!(result.metrics.contains_key("update_p95_ms"));
    }

    #[test]
    fn read_under_write_scenario_runs_with_tiny_profile() {
        let temp = TempDir::new().expect("tempdir");
        let result = run_scenario(ScenarioId::ReadUnderWrite, &tiny_profile(), temp.path())
            .expect("run read-under-write scenario");
        assert!(matches!(result.status, ScenarioStatus::Passed));
        assert!(result.metrics.contains_key("reader_p95_degradation_ratio"));
        assert!(result
            .metrics
            .contains_key("writer_throughput_degradation_ratio"));
    }

    #[test]
    fn read_under_write_paged_manifest_checksum_remains_valid() {
        let temp = TempDir::new().expect("tempdir");
        let profile = ResolvedProfile {
            rows: 2_000,
            point_reads: 1_600,
            reader_threads: 4,
            writer_ops: 40,
            batch_size: 50,
            ..tiny_profile()
        };
        let result = run_scenario(ScenarioId::ReadUnderWrite, &profile, temp.path())
            .expect("run read-under-write checksum regression");
        assert!(matches!(result.status, ScenarioStatus::Passed));
    }

    #[test]
    fn cold_point_lookup_probe_finds_seeded_rows() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("cold_probe.ddb");
        let db = Db::open_or_create(&db_path, real_fs_full_durability_config(temp.path()))
            .expect("open db");
        db.execute(
            "CREATE TABLE IF NOT EXISTS bench_point_lookup_cold (id INT64 PRIMARY KEY, payload TEXT NOT NULL)",
        )
        .expect("create table");
        load_id_table_chunked(
            &db,
            "INSERT INTO bench_point_lookup_cold (id, payload) VALUES ($1, 'point-lookup-cold')",
            1_000,
            100,
        )
        .expect("seed rows");
        db.checkpoint().expect("checkpoint");
        drop(db);

        let result = run_cold_point_lookup_probe(ColdPointLookupProbeArgs {
            db_path,
            rows: 1_000,
            seed: 7,
            trial: 0,
            start_op: 0,
            lookups: 32,
        })
        .expect("run cold point lookup probe");
        assert_eq!(result.rows_returned, 32);
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
