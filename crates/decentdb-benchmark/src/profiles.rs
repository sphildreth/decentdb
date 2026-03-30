use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::cli::RunArgs;
use crate::types::ProfileKind;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedProfile {
    pub kind: ProfileKind,
    pub rows: u64,
    pub point_reads: u64,
    pub range_scan_rows: u64,
    pub range_scans: u64,
    pub durable_commits: u64,
    pub batch_size: u64,
    pub cold_batches: u64,
    pub reader_threads: u32,
    pub writer_ops: u64,
    pub complex_users: u64,
    pub complex_items: u64,
    pub complex_orders: u64,
    pub complex_history_reads: u64,
    pub complex_point_lookups: u64,
    pub complex_range_scans: u64,
    pub complex_joins: u64,
    pub complex_aggregates: u64,
    pub complex_updates: u64,
    pub complex_deletes: u64,
    pub complex_table_scans: u64,
    pub warmup_ops: u64,
    pub trials: u32,
    pub seed: u64,
}

impl ResolvedProfile {
    #[must_use]
    pub fn scale_json(&self) -> serde_json::Value {
        json!({
            "rows": self.rows,
            "point_reads": self.point_reads,
            "range_scan_rows": self.range_scan_rows,
            "range_scans": self.range_scans,
            "durable_commits": self.durable_commits,
            "batch_size": self.batch_size,
            "cold_batches": self.cold_batches,
            "reader_threads": self.reader_threads,
            "writer_ops": self.writer_ops,
            "complex_users": self.complex_users,
            "complex_items": self.complex_items,
            "complex_orders": self.complex_orders,
            "complex_history_reads": self.complex_history_reads,
            "complex_point_lookups": self.complex_point_lookups,
            "complex_range_scans": self.complex_range_scans,
            "complex_joins": self.complex_joins,
            "complex_aggregates": self.complex_aggregates,
            "complex_updates": self.complex_updates,
            "complex_deletes": self.complex_deletes,
            "complex_table_scans": self.complex_table_scans,
            "warmup_ops": self.warmup_ops,
            "trials": self.trials,
            "seed": self.seed
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProfileOverrides {
    pub rows: Option<u64>,
    pub point_reads: Option<u64>,
    pub range_scan_rows: Option<u64>,
    pub range_scans: Option<u64>,
    pub durable_commits: Option<u64>,
    pub batch_size: Option<u64>,
    pub cold_batches: Option<u64>,
    pub reader_threads: Option<u32>,
    pub writer_ops: Option<u64>,
    pub complex_users: Option<u64>,
    pub complex_items: Option<u64>,
    pub complex_orders: Option<u64>,
    pub complex_history_reads: Option<u64>,
    pub complex_point_lookups: Option<u64>,
    pub complex_range_scans: Option<u64>,
    pub complex_joins: Option<u64>,
    pub complex_aggregates: Option<u64>,
    pub complex_updates: Option<u64>,
    pub complex_deletes: Option<u64>,
    pub complex_table_scans: Option<u64>,
    pub warmup_ops: Option<u64>,
    pub trials: Option<u32>,
    pub seed: Option<u64>,
}

impl ProfileOverrides {
    #[must_use]
    pub fn from_run_args(args: &RunArgs) -> Self {
        Self {
            rows: args.rows,
            point_reads: args.point_reads,
            range_scan_rows: args.range_scan_rows,
            range_scans: args.range_scans,
            durable_commits: args.durable_commits,
            batch_size: args.batch_size,
            cold_batches: args.cold_batches,
            reader_threads: args.reader_threads,
            writer_ops: args.writer_ops,
            complex_users: args.complex_users,
            complex_items: args.complex_items,
            complex_orders: args.complex_orders,
            complex_history_reads: args.complex_history_reads,
            complex_point_lookups: args.complex_point_lookups,
            complex_range_scans: args.complex_range_scans,
            complex_joins: args.complex_joins,
            complex_aggregates: args.complex_aggregates,
            complex_updates: args.complex_updates,
            complex_deletes: args.complex_deletes,
            complex_table_scans: args.complex_table_scans,
            warmup_ops: args.warmup_ops,
            trials: args.trials,
            seed: args.seed,
        }
    }

    fn any_set(&self) -> bool {
        self.rows.is_some()
            || self.point_reads.is_some()
            || self.range_scan_rows.is_some()
            || self.range_scans.is_some()
            || self.durable_commits.is_some()
            || self.batch_size.is_some()
            || self.cold_batches.is_some()
            || self.reader_threads.is_some()
            || self.writer_ops.is_some()
            || self.complex_users.is_some()
            || self.complex_items.is_some()
            || self.complex_orders.is_some()
            || self.complex_history_reads.is_some()
            || self.complex_point_lookups.is_some()
            || self.complex_range_scans.is_some()
            || self.complex_joins.is_some()
            || self.complex_aggregates.is_some()
            || self.complex_updates.is_some()
            || self.complex_deletes.is_some()
            || self.complex_table_scans.is_some()
            || self.warmup_ops.is_some()
            || self.trials.is_some()
            || self.seed.is_some()
    }
}

pub fn resolve_profile(kind: ProfileKind, overrides: &ProfileOverrides) -> Result<ResolvedProfile> {
    let mut profile = match kind {
        ProfileKind::Smoke => ResolvedProfile {
            kind,
            rows: 10_000,
            point_reads: 5_000,
            range_scan_rows: 128,
            range_scans: 400,
            durable_commits: 500,
            batch_size: 25,
            cold_batches: 5,
            reader_threads: 2,
            writer_ops: 300,
            complex_users: 2_000,
            complex_items: 500,
            complex_orders: 10_000,
            complex_history_reads: 500,
            complex_point_lookups: 500,
            complex_range_scans: 500,
            complex_joins: 500,
            complex_aggregates: 500,
            complex_updates: 500,
            complex_deletes: 500,
            complex_table_scans: 50,
            warmup_ops: 100,
            trials: 1,
            seed: 42,
        },
        ProfileKind::Dev => ResolvedProfile {
            kind,
            rows: 100_000,
            point_reads: 25_000,
            range_scan_rows: 256,
            range_scans: 1_000,
            durable_commits: 2_500,
            batch_size: 50,
            cold_batches: 8,
            reader_threads: 4,
            writer_ops: 1_500,
            complex_users: 10_000,
            complex_items: 2_500,
            complex_orders: 50_000,
            complex_history_reads: 2_000,
            complex_point_lookups: 2_000,
            complex_range_scans: 2_000,
            complex_joins: 2_000,
            complex_aggregates: 2_000,
            complex_updates: 2_000,
            complex_deletes: 2_000,
            complex_table_scans: 200,
            warmup_ops: 250,
            trials: 2,
            seed: 42,
        },
        ProfileKind::Nightly => ResolvedProfile {
            kind,
            rows: 1_000_000,
            point_reads: 200_000,
            range_scan_rows: 512,
            range_scans: 8_000,
            durable_commits: 15_000,
            batch_size: 100,
            cold_batches: 16,
            reader_threads: 8,
            writer_ops: 12_000,
            complex_users: 20_000,
            complex_items: 5_000,
            complex_orders: 100_000,
            complex_history_reads: 5_000,
            complex_point_lookups: 5_000,
            complex_range_scans: 5_000,
            complex_joins: 5_000,
            complex_aggregates: 5_000,
            complex_updates: 5_000,
            complex_deletes: 5_000,
            complex_table_scans: 500,
            warmup_ops: 1_000,
            trials: 3,
            seed: 42,
        },
        ProfileKind::Custom => ResolvedProfile {
            kind,
            rows: 100_000,
            point_reads: 25_000,
            range_scan_rows: 256,
            range_scans: 1_000,
            durable_commits: 2_500,
            batch_size: 50,
            cold_batches: 8,
            reader_threads: 4,
            writer_ops: 1_500,
            complex_users: 10_000,
            complex_items: 2_500,
            complex_orders: 50_000,
            complex_history_reads: 2_000,
            complex_point_lookups: 2_000,
            complex_range_scans: 2_000,
            complex_joins: 2_000,
            complex_aggregates: 2_000,
            complex_updates: 2_000,
            complex_deletes: 2_000,
            complex_table_scans: 200,
            warmup_ops: 250,
            trials: 2,
            seed: 42,
        },
    };

    if matches!(kind, ProfileKind::Custom) && !overrides.any_set() {
        return Err(anyhow!(
            "profile=custom requires at least one override (for example --rows or --trials)"
        ));
    }

    if let Some(rows) = overrides.rows {
        profile.rows = require_non_zero_u64("--rows", rows)?;
    }
    if let Some(point_reads) = overrides.point_reads {
        profile.point_reads = require_non_zero_u64("--point-reads", point_reads)?;
    }
    if let Some(range_scan_rows) = overrides.range_scan_rows {
        profile.range_scan_rows = require_non_zero_u64("--range-scan-rows", range_scan_rows)?;
    }
    if let Some(range_scans) = overrides.range_scans {
        profile.range_scans = require_non_zero_u64("--range-scans", range_scans)?;
    }
    if let Some(durable_commits) = overrides.durable_commits {
        profile.durable_commits = require_non_zero_u64("--durable-commits", durable_commits)?;
    }
    if let Some(batch_size) = overrides.batch_size {
        profile.batch_size = require_non_zero_u64("--batch-size", batch_size)?;
    }
    if let Some(cold_batches) = overrides.cold_batches {
        profile.cold_batches = require_non_zero_u64("--cold-batches", cold_batches)?;
    }
    if let Some(reader_threads) = overrides.reader_threads {
        profile.reader_threads = require_non_zero_u32("--reader-threads", reader_threads)?;
    }
    if let Some(writer_ops) = overrides.writer_ops {
        profile.writer_ops = require_non_zero_u64("--writer-ops", writer_ops)?;
    }
    if let Some(complex_users) = overrides.complex_users {
        profile.complex_users = require_non_zero_u64("--complex-users", complex_users)?;
    }
    if let Some(complex_items) = overrides.complex_items {
        profile.complex_items = require_non_zero_u64("--complex-items", complex_items)?;
    }
    if let Some(complex_orders) = overrides.complex_orders {
        profile.complex_orders = require_non_zero_u64("--complex-orders", complex_orders)?;
    }
    if let Some(complex_history_reads) = overrides.complex_history_reads {
        profile.complex_history_reads =
            require_non_zero_u64("--complex-history-reads", complex_history_reads)?;
    }
    if let Some(complex_point_lookups) = overrides.complex_point_lookups {
        profile.complex_point_lookups =
            require_non_zero_u64("--complex-point-lookups", complex_point_lookups)?;
    }
    if let Some(complex_range_scans) = overrides.complex_range_scans {
        profile.complex_range_scans =
            require_non_zero_u64("--complex-range-scans", complex_range_scans)?;
    }
    if let Some(complex_joins) = overrides.complex_joins {
        profile.complex_joins = require_non_zero_u64("--complex-joins", complex_joins)?;
    }
    if let Some(complex_aggregates) = overrides.complex_aggregates {
        profile.complex_aggregates =
            require_non_zero_u64("--complex-aggregates", complex_aggregates)?;
    }
    if let Some(complex_updates) = overrides.complex_updates {
        profile.complex_updates = require_non_zero_u64("--complex-updates", complex_updates)?;
    }
    if let Some(complex_deletes) = overrides.complex_deletes {
        profile.complex_deletes = require_non_zero_u64("--complex-deletes", complex_deletes)?;
    }
    if let Some(complex_table_scans) = overrides.complex_table_scans {
        profile.complex_table_scans =
            require_non_zero_u64("--complex-table-scans", complex_table_scans)?;
    }
    if let Some(warmup_ops) = overrides.warmup_ops {
        profile.warmup_ops = require_non_zero_u64("--warmup-ops", warmup_ops)?;
    }
    if let Some(trials) = overrides.trials {
        profile.trials = require_non_zero_u32("--trials", trials)?;
    }
    if let Some(seed) = overrides.seed {
        profile.seed = seed;
    }

    Ok(profile)
}

fn require_non_zero_u64(name: &str, value: u64) -> Result<u64> {
    if value == 0 {
        Err(anyhow!("{name} must be greater than 0"))
    } else {
        Ok(value)
    }
}

fn require_non_zero_u32(name: &str, value: u32) -> Result<u32> {
    if value == 0 {
        Err(anyhow!("{name} must be greater than 0"))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_profile, ProfileOverrides};
    use crate::types::ProfileKind;

    #[test]
    fn custom_profile_requires_override() {
        let result = resolve_profile(ProfileKind::Custom, &ProfileOverrides::default());
        assert!(result.is_err());
    }

    #[test]
    fn smoke_profile_defaults_match_phase_1_scale() {
        let profile = resolve_profile(ProfileKind::Smoke, &ProfileOverrides::default())
            .expect("resolve smoke profile");
        assert_eq!(profile.rows, 10_000);
        assert_eq!(profile.point_reads, 5_000);
        assert_eq!(profile.batch_size, 25);
        assert_eq!(profile.reader_threads, 2);
        assert_eq!(profile.complex_orders, 10_000);
        assert_eq!(profile.complex_table_scans, 50);
        assert_eq!(profile.trials, 1);
    }

    #[test]
    fn custom_profile_applies_overrides() {
        let profile = resolve_profile(
            ProfileKind::Custom,
            &ProfileOverrides {
                rows: Some(12_345),
                complex_orders: Some(54_321),
                trials: Some(3),
                ..ProfileOverrides::default()
            },
        )
        .expect("resolve custom profile");
        assert_eq!(profile.rows, 12_345);
        assert_eq!(profile.complex_orders, 54_321);
        assert_eq!(profile.trials, 3);
    }
}
