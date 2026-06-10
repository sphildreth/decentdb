//! Advisor engine: consumes trace snapshots and existing sys facts to produce
//! reviewable findings with severity, confidence, evidence, and recommendations.
//!
//! No advisor performs destructive automatic schema changes.

#![allow(dead_code)]

use crate::doctor::{
    DoctorCategory, DoctorEvidence, DoctorEvidenceValue, DoctorFinding, DoctorRecommendation,
    DoctorSeverity,
};
use crate::record::value::Value;
use crate::tracing::buffer::BoundedSnapshot;
use crate::tracing::index_usage::IndexUsageRow;
use crate::tracing::lock_wait::LockWaitEvent;
use crate::tracing::slow_query::SlowQueryEvent;

/// Shared advisor finding model returned by all advisors.
#[derive(Clone, Debug)]
pub struct AdvisorFinding {
    pub advisor_id: String,
    pub category: AdvisorCategory,
    pub severity: AdvisorSeverity,
    pub confidence: AdvisorConfidence,
    pub title: String,
    pub description: String,
    pub evidence: Vec<String>,
    pub recommendation: String,
    pub fix_plan: Option<AdvisorFixPlan>,
}

impl AdvisorFinding {
    pub fn to_query_row(&self) -> Vec<Value> {
        vec![
            Value::Text(self.advisor_id.clone()),
            Value::Text(format!("{:?}", self.category)),
            Value::Text(format!("{:?}", self.severity)),
            Value::Text(format!("{:?}", self.confidence)),
            Value::Text(self.title.clone()),
            Value::Text(self.description.clone()),
            Value::Text(self.evidence.join("; ")),
            Value::Text(self.recommendation.clone()),
            Value::Text(self.fix_plan.as_ref().map_or("".to_string(), |p| p.action.clone())),
            Value::Text(self.fix_plan.as_ref().map_or("".to_string(), |p| p.target.clone())),
            Value::Text(
                self.fix_plan.as_ref().map_or("none".to_string(), |p| p.auto_safe.clone()),
            ),
            Value::Bool(self.fix_plan.is_some()),
        ]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdvisorCategory {
    Query,
    Index,
    Contention,
    Storage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum AdvisorSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum AdvisorConfidence {
    Low,
    Medium,
    High,
}

/// Safe-to-apply fix plan with a target and action.
#[derive(Clone, Debug)]
pub struct AdvisorFixPlan {
    pub action: String,
    pub target: String,
    pub auto_safe: String, // "auto" | "manual" | "review"
}

/// Advisor engine state; lightweight and constructed per-analysis.
pub struct AdvisorEngine {
    findings: Vec<AdvisorFinding>,
}

impl Default for AdvisorEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl AdvisorEngine {
    pub fn new() -> Self {
        Self {
            findings: Vec::new(),
        }
    }

    pub fn findings(&self) -> &[AdvisorFinding] {
        &self.findings
    }

    pub fn into_findings(self) -> Vec<AdvisorFinding> {
        self.findings
    }

    /// Run all advisors against the provided trace state.
    pub fn analyze(
        &mut self,
        slow_queries: &BoundedSnapshot<SlowQueryEvent>,
        lock_waits: &BoundedSnapshot<LockWaitEvent>,
        index_usage: &[IndexUsageRow],
        wal_size_mb: u64,
        uncheckpointed_frames: u64,
    ) {
        self.slow_query_advisor(slow_queries);
        self.lock_wait_advisor(lock_waits);
        self.index_usage_advisor(index_usage);
        self.storage_advisor(wal_size_mb, uncheckpointed_frames);
    }

    fn slow_query_advisor(
        &mut self,
        slow_queries: &BoundedSnapshot<SlowQueryEvent>,
    ) {
        if slow_queries.items.is_empty() {
            return;
        }
        // Group by fingerprint and find the most common slow statement.
        let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        let mut max_fingerprint: Option<(&SlowQueryEvent, u64)> = None;
        for e in &slow_queries.items {
            let key = e.sql_fingerprint.clone();
            let count = counts.entry(key.clone()).or_insert(0);
            *count += 1;
            if max_fingerprint.as_ref().is_none_or(|(_, m)| *count > *m) {
                max_fingerprint = Some((e, *count));
            }
        }
        if let Some((event, count)) = max_fingerprint {
            if count >= 3 {
                self.findings.push(AdvisorFinding {
                    advisor_id: "slow-query-recurring".to_string(),
                    category: AdvisorCategory::Query,
                    severity: AdvisorSeverity::Warning,
                    confidence: AdvisorConfidence::Medium,
                    title: "Recurring slow query pattern detected".to_string(),
                    description: format!(
                        "Fingerprint '{}' appeared {} times above threshold.",
                        event.sql_fingerprint, count
                    ),
                    evidence: vec![format!("duration_us={}", event.duration_us)],
                    recommendation:
                        "Review query plan; consider adding or rebuilding indexes.".to_string(),
                    fix_plan: None,
                });
            }
        }
    }

    fn lock_wait_advisor(
        &mut self,
        lock_waits: &BoundedSnapshot<LockWaitEvent>,
    ) {
        if lock_waits.items.is_empty() {
            return;
        }
        // Find the dominant lock-wait source.
        let mut counts: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        let mut max_wait: Option<(&LockWaitEvent, u64)> = None;
        for e in &lock_waits.items {
            let key = e.wait_source.clone();
            let count = counts.entry(key).or_insert(0);
            *count += 1;
            if max_wait.as_ref().is_none_or(|(_, m)| *count > *m) {
                max_wait = Some((e, *count));
            }
        }
        if let Some((event, count)) = max_wait {
            if count >= 3 {
                self.findings.push(AdvisorFinding {
                    advisor_id: "lock-wait-contention".to_string(),
                    category: AdvisorCategory::Contention,
                    severity: AdvisorSeverity::Warning,
                    confidence: AdvisorConfidence::Medium,
                    title: "Frequent lock-wait contention".to_string(),
                    description: format!(
                        "Source '{}' waited {} times above threshold.",
                        event.wait_source, count
                    ),
                    evidence: vec![format!("max_duration_us={}", event.duration_us)],
                    recommendation:
                        "Consider reducing transaction scope or batching writes.".to_string(),
                    fix_plan: None,
                });
            }
        }
    }

    fn index_usage_advisor(
        &mut self,
        index_usage: &[IndexUsageRow],
    ) {
        if index_usage.is_empty() {
            return;
        }
        for row in index_usage {
            if row.read_count > 0 && row.write_count > 0 {
                self.findings.push(AdvisorFinding {
                    advisor_id: "index-mixed-workload".to_string(),
                    category: AdvisorCategory::Index,
                    severity: AdvisorSeverity::Info,
                    confidence: AdvisorConfidence::High,
                    title: "Index serves both reads and writes".to_string(),
                    description: format!(
                        "Index {} on {} has {} reads and {} writes.",
                        row.index_name, row.table_name, row.read_count, row.write_count
                    ),
                    evidence: vec![format!("reads={} writes={}", row.read_count, row.write_count)],
                    recommendation:
                        "Review write amplification; consider partial indexes if applicable."
                            .to_string(),
                    fix_plan: None,
                });
            }
            if row.read_count == 0 && row.write_count > 0 {
                self.findings.push(AdvisorFinding {
                    advisor_id: "index-write-only".to_string(),
                    category: AdvisorCategory::Index,
                    severity: AdvisorSeverity::Warning,
                    confidence: AdvisorConfidence::Low,
                    title: "Index has write traffic but no observed reads".to_string(),
                    description: format!(
                        "Index {} on {} was updated {} times but never read.",
                        row.index_name, row.table_name, row.write_count
                    ),
                    evidence: vec![format!("writes={}", row.write_count)],
                    recommendation: "Investigate whether the index is needed.".to_string(),
                    fix_plan: None,
                });
            }
        }
    }

    fn storage_advisor(
        &mut self,
        wal_size_mb: u64,
        uncheckpointed_frames: u64,
    ) {
        if wal_size_mb > 100 {
            self.findings.push(AdvisorFinding {
                advisor_id: "storage-large-wal".to_string(),
                category: AdvisorCategory::Storage,
                severity: AdvisorSeverity::Warning,
                confidence: AdvisorConfidence::High,
                title: "WAL size exceeds 100 MB".to_string(),
                description: format!("Current WAL size is {} MB.", wal_size_mb),
                evidence: vec![format!("wal_size_mb={}", wal_size_mb)],
                recommendation: "Run CHECKPOINT or review checkpoint policy.".to_string(),
                fix_plan: Some(AdvisorFixPlan {
                    action: "checkpoint".to_string(),
                    target: "wal".to_string(),
                    auto_safe: "auto".to_string(),
                }),
            });
        }
        if uncheckpointed_frames > 10_000 {
            self.findings.push(AdvisorFinding {
                advisor_id: "storage-many-uncheckpointed-frames".to_string(),
                category: AdvisorCategory::Storage,
                severity: AdvisorSeverity::Info,
                confidence: AdvisorConfidence::High,
                title: "Many uncheckpointed WAL frames".to_string(),
                description: format!(
                    "{} frames have not been checkpointed.",
                    uncheckpointed_frames
                ),
                evidence: vec![format!(
                    "uncheckpointed_frames={}",
                    uncheckpointed_frames
                )],
                recommendation: "Run CHECKPOINT to reclaim space.".to_string(),
                fix_plan: Some(AdvisorFixPlan {
                    action: "checkpoint".to_string(),
                    target: "wal".to_string(),
                    auto_safe: "auto".to_string(),
                }),
            });
        }
    }
}

/// Convert advisor findings to Doctor findings for `sys.doctor_findings`.
pub fn advisor_findings_to_doctor_findings(
    advisor_findings: &[AdvisorFinding],
) -> Vec<DoctorFinding> {
    let mut out = Vec::with_capacity(advisor_findings.len());
    for f in advisor_findings {
        let severity = match f.severity {
            AdvisorSeverity::Info => DoctorSeverity::Info,
            AdvisorSeverity::Warning => DoctorSeverity::Warning,
            AdvisorSeverity::Error => DoctorSeverity::Error,
        };
        let category = match f.category {
            AdvisorCategory::Query => DoctorCategory::Wal,
            AdvisorCategory::Index => DoctorCategory::Indexes,
            AdvisorCategory::Contention => DoctorCategory::Storage,
            AdvisorCategory::Storage => DoctorCategory::Wal,
        };
        let evidence = f
            .evidence
            .iter()
            .map(|e| DoctorEvidence {
                field: "advisor".to_string(),
                value: DoctorEvidenceValue::String(e.clone()),
                unit: None,
            })
            .collect();
        let recommendation = if f.recommendation.is_empty() {
            None
        } else {
            Some(DoctorRecommendation {
                summary: f.recommendation.clone(),
                commands: f
                    .fix_plan
                    .as_ref()
                    .map(|p| vec![p.action.clone()])
                    .unwrap_or_default(),
                safe_to_automate: f.fix_plan.as_ref().is_some_and(|p| p.auto_safe == "auto"),
            })
        };
        out.push(DoctorFinding {
            id: f.advisor_id.clone(),
            category,
            severity,
            title: f.title.clone(),
            message: f.description.clone(),
            evidence,
            recommendation,
        });
    }
    out
}
