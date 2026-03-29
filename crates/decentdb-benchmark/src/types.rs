use std::collections::BTreeMap;

use clap::ValueEnum;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ProfileKind {
    Smoke,
    Dev,
    Nightly,
    Custom,
}

impl ProfileKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Dev => "dev",
            Self::Nightly => "nightly",
            Self::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioId {
    #[value(name = "durable_commit_single")]
    DurableCommitSingle,
    #[value(name = "point_lookup_warm")]
    PointLookupWarm,
    #[value(name = "range_scan_warm")]
    RangeScanWarm,
    #[value(name = "storage_efficiency")]
    StorageEfficiency,
}

impl ScenarioId {
    pub const ALL_PHASE1: [Self; 4] = [
        Self::DurableCommitSingle,
        Self::PointLookupWarm,
        Self::RangeScanWarm,
        Self::StorageEfficiency,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DurableCommitSingle => "durable_commit_single",
            Self::PointLookupWarm => "point_lookup_warm",
            Self::RangeScanWarm => "range_scan_warm",
            Self::StorageEfficiency => "storage_efficiency",
        }
    }

    #[must_use]
    pub fn default_workload(self) -> &'static str {
        "oltp_narrow_v1"
    }

    #[must_use]
    pub fn default_durability_mode(self) -> &'static str {
        match self {
            Self::DurableCommitSingle | Self::StorageEfficiency => "full",
            Self::PointLookupWarm | Self::RangeScanWarm => "n/a",
        }
    }

    #[must_use]
    pub fn default_cache_mode(self) -> &'static str {
        match self {
            Self::DurableCommitSingle | Self::StorageEfficiency => "real_fs",
            Self::PointLookupWarm | Self::RangeScanWarm => "in_memory",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistogramSummary {
    pub unit: String,
    pub sample_count: u64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub max_us: f64,
    pub mean_us: f64,
    pub stddev_us: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioResult {
    pub status: ScenarioStatus,
    pub error_class: Option<String>,
    pub scenario_id: ScenarioId,
    pub profile: ProfileKind,
    pub workload: String,
    pub durability_mode: String,
    pub cache_mode: String,
    pub trial_count: u32,
    pub metrics: BTreeMap<String, serde_json::Value>,
    pub warnings: Vec<String>,
    pub notes: Vec<String>,
    pub scale: serde_json::Value,
    pub histograms: Option<HistogramSummary>,
    pub vfs_stats: Option<serde_json::Value>,
    pub artifacts: Vec<String>,
}
