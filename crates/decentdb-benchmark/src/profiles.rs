use anyhow::{anyhow, Result};
use serde::Serialize;
use serde_json::json;

use crate::cli::RunArgs;
use crate::types::ProfileKind;

#[derive(Debug, Clone, Serialize)]
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
        assert_eq!(profile.trials, 1);
    }

    #[test]
    fn custom_profile_applies_overrides() {
        let profile = resolve_profile(
            ProfileKind::Custom,
            &ProfileOverrides {
                rows: Some(12_345),
                trials: Some(3),
                ..ProfileOverrides::default()
            },
        )
        .expect("resolve custom profile");
        assert_eq!(profile.rows, 12_345);
        assert_eq!(profile.trials, 3);
    }
}
