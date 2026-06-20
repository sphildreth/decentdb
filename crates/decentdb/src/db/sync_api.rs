use super::*;

pub(super) fn sync_read_metadata_from_runtime(
    runtime: &EngineRuntime,
    key: &str,
) -> Result<Option<String>> {
    let Some(table) = runtime.catalog.table(crate::sync::METADATA_TABLE) else {
        return Ok(None);
    };
    let Some(key_index) = table.columns.iter().position(|column| column.name == "key") else {
        return Ok(None);
    };
    let Some(value_index) = table
        .columns
        .iter()
        .position(|column| column.name == "value")
    else {
        return Ok(None);
    };
    let Some(source) = runtime.table_row_source(&table.name) else {
        return Ok(None);
    };

    for row in source.rows() {
        let row = row?;
        let values = row.values();
        if let (Some(Value::Text(row_key)), Some(Value::Text(row_value))) =
            (values.get(key_index), values.get(value_index))
        {
            if row_key == key {
                return Ok(Some(row_value.clone()));
            }
        }
    }
    Ok(None)
}

pub(super) enum SyncInspectionQuery {
    Status,
    Journal { since_sequence: u64 },
    WalMetrics,
    ProcessCoordination,
    ProcessReaders,
    ProcessLockMetrics,
    WriteQueueMetrics,
    StorageMetrics,
    ReactiveMetrics,
    ReactiveSubscriptions,
    Peers,
    Retention,
    PeerLag,
    Doctor,
    Scopes,
    ScopeTables,
    PeerScopes,
    Sessions,
    ConflictPolicy,
    Conflicts,
    RelayStatus,
    RelaySessions,
    Shapes,
    ShapeClients,
    ChangesetHistory,
    RuntimeSessions,
    SlowQueries,
    LockWaits,
    IndexUsage,
    DoctorFindings,
    FixPlan,
    PlanCache,
    PlanCacheSummary,
}

impl SyncInspectionQuery {
    pub(super) fn parse(normalized: &str) -> Option<Self> {
        match normalized {
            "select * from sys_sync_status" => Some(Self::Status),
            "select * from sys.sync_status" => Some(Self::Status),
            "select * from sys_sync_journal" => Some(Self::Journal { since_sequence: 0 }),
            "select * from sys_sync_journal order by sequence" => {
                Some(Self::Journal { since_sequence: 0 })
            }
            "select * from sys_sync_journal order by sequence asc" => {
                Some(Self::Journal { since_sequence: 0 })
            }
            "select * from sys.wal_metrics" => Some(Self::WalMetrics),
            "select * from sys.process_coordination" => Some(Self::ProcessCoordination),
            "select * from sys.process_readers" => Some(Self::ProcessReaders),
            "select * from sys.process_lock_metrics" => Some(Self::ProcessLockMetrics),
            "select * from sys.write_queue_metrics" => Some(Self::WriteQueueMetrics),
            "select * from sys.storage_metrics" => Some(Self::StorageMetrics),
            "select * from sys.reactive_metrics" => Some(Self::ReactiveMetrics),
            "select * from sys.reactive_subscriptions" => Some(Self::ReactiveSubscriptions),
            "select * from sys.reactive_subscriptions order by watch_id" => {
                Some(Self::ReactiveSubscriptions)
            }
            "select * from sys.reactive_subscriptions order by watch_id asc" => {
                Some(Self::ReactiveSubscriptions)
            }
            "select * from sys_sync_peers" => Some(Self::Peers),
            "select * from sys_sync_peers order by name" => Some(Self::Peers),
            "select * from sys_sync_peers order by name asc" => Some(Self::Peers),
            "select * from sys_sync_retention" => Some(Self::Retention),
            "select * from sys.sync_retention" => Some(Self::Retention),
            "select * from sys_sync_peer_lag" => Some(Self::PeerLag),
            "select * from sys.sync_peer_lag" => Some(Self::PeerLag),
            "select * from sys_sync_peer_lag order by peer_name" => Some(Self::PeerLag),
            "select * from sys_sync_peer_lag order by peer_name asc" => Some(Self::PeerLag),
            "select * from sys_sync_doctor" => Some(Self::Doctor),
            "select * from sys_sync_scopes" => Some(Self::Scopes),
            "select * from sys_sync_scopes order by name" => Some(Self::Scopes),
            "select * from sys_sync_scopes order by name asc" => Some(Self::Scopes),
            "select * from sys_sync_scope_tables" => Some(Self::ScopeTables),
            "select * from sys_sync_scope_tables order by scope_name, table_name" => {
                Some(Self::ScopeTables)
            }
            "select * from sys_sync_scope_tables order by scope_name, table_name asc" => {
                Some(Self::ScopeTables)
            }
            "select * from sys_sync_peer_scopes" => Some(Self::PeerScopes),
            "select * from sys_sync_peer_scopes order by peer_name" => Some(Self::PeerScopes),
            "select * from sys_sync_peer_scopes order by peer_name asc" => Some(Self::PeerScopes),
            "select * from sys_sync_sessions" => Some(Self::Sessions),
            "select * from sys_sync_sessions order by session_id" => Some(Self::Sessions),
            "select * from sys_sync_sessions order by session_id asc" => Some(Self::Sessions),
            "select * from sys_sync_conflict_policy" => Some(Self::ConflictPolicy),
            "select * from sys_sync_conflict_policy order by default_policy" => {
                Some(Self::ConflictPolicy)
            }
            "select * from sys_sync_conflicts" => Some(Self::Conflicts),
            "select * from sys_sync_conflicts order by conflict_id" => Some(Self::Conflicts),
            "select * from sys_sync_conflicts order by conflict_id asc" => Some(Self::Conflicts),
            "select * from sys.sync_relay_status" => Some(Self::RelayStatus),
            "select * from sys.sync_relay_sessions" => Some(Self::RelaySessions),
            "select * from sys.sync_relay_sessions order by started_at_micros, session_id" => {
                Some(Self::RelaySessions)
            }
            "select * from sys.sync_shapes" => Some(Self::Shapes),
            "select * from sys.sync_shapes order by shape_id" => Some(Self::Shapes),
            "select * from sys.sync_shape_clients" => Some(Self::ShapeClients),
            "select * from sys.sync_shape_clients order by shape_id, client_replica_id" => {
                Some(Self::ShapeClients)
            }
            "select * from sys.sync_changeset_history" => Some(Self::ChangesetHistory),
            "select * from sys.sync_changeset_history order by created_at_micros, changeset_id" => {
                Some(Self::ChangesetHistory)
            }
            "select * from sys.sessions" => Some(Self::RuntimeSessions),
            "select * from sys.slow_queries" => Some(Self::SlowQueries),
            "select * from sys.lock_waits" => Some(Self::LockWaits),
            "select * from sys.index_usage" => Some(Self::IndexUsage),
            "select * from sys.doctor_findings" => Some(Self::DoctorFindings),
            "select * from sys.fix_plan" => Some(Self::FixPlan),
            "select * from sys.plan_cache" => Some(Self::PlanCache),
            "select * from sys.plan_cache_summary" => Some(Self::PlanCacheSummary),
            _ => parse_sync_journal_where_sequence(normalized)
                .map(|since_sequence| Self::Journal { since_sequence }),
        }
    }
}

pub(super) fn normalize_sync_inspection_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

pub(super) fn plan_cache_doctor_row(
    id: &str,
    category: &str,
    severity: &str,
    title: &str,
    message: &str,
    evidence: &str,
    recommendation: &str,
) -> Vec<Value> {
    vec![
        Value::Text(id.to_string()),
        Value::Text(category.to_string()),
        Value::Text(severity.to_string()),
        Value::Text(title.to_string()),
        Value::Text(message.to_string()),
        Value::Text(evidence.to_string()),
        Value::Text(recommendation.to_string()),
    ]
}

pub(super) fn parse_sync_journal_where_sequence(normalized: &str) -> Option<u64> {
    let rest = normalized.strip_prefix("select * from sys_sync_journal where sequence > ")?;
    let rest = rest.strip_suffix(" order by sequence asc").unwrap_or(rest);
    let rest = rest.strip_suffix(" order by sequence").unwrap_or(rest);
    rest.parse().ok()
}

pub(super) fn sync_u64_to_i64(value: u64, field_name: &str) -> Result<Value> {
    i64::try_from(value)
        .map(Value::Int64)
        .map_err(|_| DbError::internal(format!("{field_name} exceeds INT64 range")))
}

pub(super) fn sync_usize_to_i64(value: usize, field_name: &str) -> Result<Value> {
    u64::try_from(value)
        .and_then(i64::try_from)
        .map(Value::Int64)
        .map_err(|_| DbError::internal(format!("{field_name} exceeds INT64 range")))
}

pub(super) struct SyncDiffChangesetContext<'a> {
    pub(super) base_kind: &'a str,
    pub(super) from_ref: &'a str,
    pub(super) to_ref: &'a str,
    pub(super) scope_name: Option<&'a str>,
    pub(super) shape_id: Option<&'a str>,
    pub(super) tenant_id: Option<&'a str>,
    pub(super) schema_fingerprint: &'a str,
    pub(super) schema_cookie: u32,
    pub(super) created_at_micros: i64,
    pub(super) max_records: usize,
}

pub(super) struct BranchChangesetTableContext<'a> {
    pub(super) source_replica_id: &'a str,
    pub(super) table_name: &'a str,
    pub(super) primary_key_columns: &'a [String],
    pub(super) column_names: &'a [String],
    pub(super) schema_cookie: u32,
    pub(super) created_at_micros: i64,
}

pub(super) fn sync_changeset_record_from_journal_record(
    record: &SyncJournalRecord,
) -> SyncChangesetRecord {
    SyncChangesetRecord {
        record_version: 1,
        table: record.table.clone(),
        operation: record.operation.clone(),
        primary_key: record.primary_key.clone(),
        origin_replica_id: record.replica_id.clone(),
        origin_sequence: record.sequence,
        transaction_id: format!("txn:{}", record.transaction_lsn),
        transaction_lsn: record.transaction_lsn,
        schema_cookie: record.schema_cookie,
        before_hash: None,
        before: None,
        after: record.after.clone(),
        column_mask: Vec::new(),
        tombstone: record.operation == "delete",
        conflict_metadata: None,
    }
}

pub(super) fn sync_journal_record_from_changeset_record(
    record: &SyncChangesetRecord,
) -> Result<SyncJournalRecord> {
    let after = match record.operation.as_str() {
        "insert" | "update" => Some(record.after.clone().ok_or_else(|| {
            DbError::sql("CHANGESET_INVALID: insert/update changeset record missing after")
        })?),
        "delete" => None,
        other => {
            return Err(DbError::sql(format!(
                "CHANGESET_INVALID: unsupported operation '{other}'"
            )));
        }
    };
    Ok(SyncJournalRecord {
        schema_version: 1,
        sequence: record.origin_sequence,
        replica_id: record.origin_replica_id.clone(),
        transaction_lsn: record.transaction_lsn,
        table: record.table.clone(),
        operation: record.operation.clone(),
        primary_key: record.primary_key.clone(),
        after,
        schema_cookie: record.schema_cookie,
        committed_at_micros: current_time_micros(),
    })
}

pub(super) fn sync_changeset_record_from_branch_row(
    ctx: &BranchChangesetTableContext<'_>,
    origin_sequence: u64,
    operation: &str,
    row: &crate::branch::BranchRowDiff,
) -> Result<SyncChangesetRecord> {
    let primary_key = branch_primary_key_object(ctx.primary_key_columns, &row.primary_key)?;
    let before = row
        .before
        .as_ref()
        .map(|values| branch_row_object(ctx.column_names, values))
        .transpose()?;
    let after = row
        .after
        .as_ref()
        .map(|values| branch_row_object(ctx.column_names, values))
        .transpose()?;
    Ok(SyncChangesetRecord {
        record_version: 1,
        table: ctx.table_name.to_string(),
        operation: operation.to_string(),
        primary_key,
        origin_replica_id: ctx.source_replica_id.to_string(),
        origin_sequence,
        transaction_id: format!("txn:{}:{origin_sequence}", ctx.source_replica_id),
        transaction_lsn: origin_sequence,
        schema_cookie: ctx.schema_cookie,
        before_hash: before.as_ref().map(json_hash).transpose()?,
        before,
        after,
        column_mask: ctx.column_names.to_vec(),
        tombstone: operation == "delete",
        conflict_metadata: Some(serde_json::json!({
            "source": "branch_diff",
            "created_at_micros": ctx.created_at_micros
        })),
    })
}

pub(super) fn branch_primary_key_object(
    columns: &[String],
    values: &[String],
) -> Result<serde_json::Value> {
    if columns.len() != values.len() {
        return Err(DbError::sql("branch diff primary-key width mismatch"));
    }
    let mut object = serde_json::Map::new();
    for (column, value) in columns.iter().zip(values) {
        object.insert(column.clone(), json_from_branch_value(value));
    }
    Ok(serde_json::Value::Object(object))
}

pub(super) fn branch_row_object(
    columns: &[String],
    values: &[String],
) -> Result<serde_json::Value> {
    if columns.len() != values.len() {
        return Err(DbError::sql("branch diff row width mismatch"));
    }
    let mut object = serde_json::Map::new();
    for (column, value) in columns.iter().zip(values) {
        object.insert(column.clone(), json_from_branch_value(value));
    }
    Ok(serde_json::Value::Object(object))
}

pub(super) fn json_from_branch_value(value: &str) -> serde_json::Value {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("null") {
        return serde_json::Value::Null;
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return serde_json::Value::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return serde_json::Value::Bool(false);
    }
    if let Some(quoted) = trimmed
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
    {
        return serde_json::Value::String(quoted.replace("''", "'"));
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return serde_json::Value::Number(value.into());
    }
    if let Ok(value) = trimmed.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(value) {
            return serde_json::Value::Number(number);
        }
    }
    serde_json::Value::String(trimmed.to_string())
}

pub(super) fn sync_changeset_id(
    kind: &str,
    source: &str,
    created_at_micros: i64,
    count: usize,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(kind.as_bytes());
    hasher.update(source.as_bytes());
    hasher.update(created_at_micros.to_le_bytes());
    hasher.update((count as u64).to_le_bytes());
    let digest = hasher.finalize();
    format!(
        "dcs_{created_at_micros:016x}_{}",
        &hex_encode(&digest)[..16]
    )
}

pub(super) fn changeset_applied_key(changeset_id: &str) -> String {
    format!("changeset_applied:{changeset_id}")
}

pub(super) fn json_hash(value: &serde_json::Value) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        DbError::internal(format!("failed to serialize JSON hash input: {error}"))
    })?;
    let digest = Sha256::digest(&bytes);
    Ok(format!("sha256:{}", hex_encode(&digest)))
}

pub(super) fn sync_shape_from_row(row: &QueryRow) -> Result<SyncShape> {
    let values = row.values();
    Ok(SyncShape {
        shape_id: parse_text_value(values.first(), "shape_id")?,
        name: parse_text_value(values.get(1), "name")?,
        scope_name: parse_text_value(values.get(2), "scope_name")?,
        tenant_id: parse_text_value(values.get(3), "tenant_id")?,
        allowed_roles: serde_json::from_str(&parse_text_value(
            values.get(4),
            "allowed_roles_json",
        )?)
        .map_err(|error| DbError::corruption(format!("malformed allowed_roles_json: {error}")))?,
        allowed_subjects: serde_json::from_str(&parse_text_value(
            values.get(5),
            "allowed_subjects_json",
        )?)
        .map_err(|error| {
            DbError::corruption(format!("malformed allowed_subjects_json: {error}"))
        })?,
        created_at_micros: parse_sync_i64(values.get(6), "created_at_micros")?,
        updated_at_micros: parse_sync_i64(values.get(7), "updated_at_micros")?,
        retention_ttl_micros: parse_sync_i64(values.get(8), "retention_ttl_micros")?,
        max_records: parse_u64_value(values.get(9), "max_records")?,
        ack_deadline_micros: parse_sync_i64(values.get(10), "ack_deadline_micros")?,
        heartbeat_micros: parse_sync_i64(values.get(11), "heartbeat_micros")?,
    })
}

pub(super) fn sync_shape_client_from_row(row: &QueryRow) -> Result<SyncShapeClient> {
    let values = row.values();
    Ok(SyncShapeClient {
        shape_id: parse_text_value(values.first(), "shape_id")?,
        tenant_id: parse_text_value(values.get(1), "tenant_id")?,
        client_replica_id: parse_text_value(values.get(2), "client_replica_id")?,
        subject_id: parse_text_value(values.get(3), "subject_id")?,
        session_id: parse_optional_text_value(values.get(4), "session_id")?,
        last_ack_sequence: parse_u64_value(values.get(5), "last_ack_sequence")?,
        last_ack_watermark: parse_u64_value(values.get(6), "last_ack_watermark")?,
        last_changeset_id: parse_optional_text_value(values.get(7), "last_changeset_id")?,
        last_seen_at_micros: parse_sync_i64(values.get(8), "last_seen_at_micros")?,
        retention_blocking: parse_boolish_value(values.get(9), "retention_blocking")?,
        status: parse_text_value(values.get(10), "status")?,
    })
}

pub(super) fn sync_relay_session_from_row(row: &QueryRow) -> Result<SyncRelaySession> {
    let values = row.values();
    Ok(SyncRelaySession {
        session_id: parse_text_value(values.first(), "session_id")?,
        tenant_id: parse_text_value(values.get(1), "tenant_id")?,
        subject_id: parse_text_value(values.get(2), "subject_id")?,
        subject_kind: crate::sync::SyncSubjectKind::from_str(&parse_text_value(
            values.get(3),
            "subject_kind",
        )?)?,
        request_id: parse_text_value(values.get(4), "request_id")?,
        operation: parse_text_value(values.get(5), "operation")?,
        scope_name: parse_optional_text_value(values.get(6), "scope_name")?,
        shape_id: parse_optional_text_value(values.get(7), "shape_id")?,
        started_at_micros: parse_sync_i64(values.get(8), "started_at_micros")?,
        ended_at_micros: parse_optional_i64_value(values.get(9), "ended_at_micros")?,
        status: parse_text_value(values.get(10), "status")?,
        error: parse_optional_text_value(values.get(11), "error")?,
        rows_seen: parse_u64_value(values.get(12), "rows_seen")?,
        bytes_seen: parse_u64_value(values.get(13), "bytes_seen")?,
    })
}

pub(super) fn sync_changeset_history_from_row(row: &QueryRow) -> Result<SyncChangesetHistory> {
    let values = row.values();
    let source_kind = match parse_text_value(values.get(2), "source_kind")?.as_str() {
        "checkpoint" => crate::sync::SyncChangesetSourceKind::Checkpoint,
        "branch" => crate::sync::SyncChangesetSourceKind::Branch,
        "snapshot" => crate::sync::SyncChangesetSourceKind::Snapshot,
        other => {
            return Err(DbError::corruption(format!(
                "malformed changeset history source_kind '{other}'"
            )));
        }
    };
    Ok(SyncChangesetHistory {
        changeset_id: parse_text_value(values.first(), "changeset_id")?,
        source_replica_id: parse_text_value(values.get(1), "source_replica_id")?,
        source_kind,
        scope_name: parse_optional_text_value(values.get(3), "scope_name")?,
        shape_id: parse_optional_text_value(values.get(4), "shape_id")?,
        record_count: parse_u64_value(values.get(5), "record_count")?,
        bytes: parse_u64_value(values.get(6), "bytes")?,
        created_at_micros: parse_sync_i64(values.get(7), "created_at_micros")?,
        applied_at_micros: parse_optional_i64_value(values.get(8), "applied_at_micros")?,
        outcome: parse_text_value(values.get(9), "outcome")?,
        integrity_hash: parse_optional_text_value(values.get(10), "integrity_hash")?,
    })
}

pub(super) fn parse_text_value(value: Option<&Value>, field_name: &str) -> Result<String> {
    match value {
        Some(Value::Text(value)) => Ok(value.clone()),
        _ => Err(DbError::corruption(format!(
            "malformed sync row: {field_name}"
        ))),
    }
}

pub(super) fn parse_optional_text_value(
    value: Option<&Value>,
    field_name: &str,
) -> Result<Option<String>> {
    match value {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        _ => Err(DbError::corruption(format!(
            "malformed sync row: {field_name}"
        ))),
    }
}

pub(super) fn parse_optional_i64_value(
    value: Option<&Value>,
    field_name: &str,
) -> Result<Option<i64>> {
    match value {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Int64(value)) => Ok(Some(*value)),
        Some(Value::Bool(value)) => Ok(Some(i64::from(*value))),
        _ => Err(DbError::corruption(format!(
            "malformed sync row: {field_name}"
        ))),
    }
}

pub(super) fn parse_u64_value(value: Option<&Value>, field_name: &str) -> Result<u64> {
    let value = parse_sync_i64(value, field_name)?;
    u64::try_from(value).map_err(|_| {
        DbError::corruption(format!(
            "malformed sync row: {field_name} must be non-negative"
        ))
    })
}

pub(super) fn parse_boolish_value(value: Option<&Value>, field_name: &str) -> Result<bool> {
    match value {
        Some(Value::Bool(value)) => Ok(*value),
        Some(Value::Int64(value)) => Ok(*value != 0),
        _ => Err(DbError::corruption(format!(
            "malformed sync row: {field_name}"
        ))),
    }
}

pub(super) fn sync_conflict_from_row(row: &QueryRow) -> Result<SyncConflict> {
    let values = row.values();
    let conflict_id = match values.first() {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: conflict_id",
            ))
        }
    };
    let batch_id = match values.get(1) {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption("malformed sync conflict row: batch_id"));
        }
    };
    let remote_replica_id = match values.get(2) {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: remote_replica_id",
            ));
        }
    };
    let remote_sequence = match values.get(3) {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: remote_sequence",
            ));
        }
    };
    let table_name = match values.get(4) {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: table_name",
            ));
        }
    };
    let operation = match values.get(5) {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: operation",
            ));
        }
    };
    let conflict_type = match values.get(6) {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: conflict_type",
            ));
        }
    };
    let message = match values.get(7) {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption("malformed sync conflict row: message"));
        }
    };
    let primary_key_json = match values.get(8) {
        Some(Value::Text(value)) => serde_json::from_str(value).map_err(|error| {
            DbError::corruption(format!("malformed sync conflict primary_key_json: {error}"))
        })?,
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: primary_key_json",
            ));
        }
    };
    let remote_record_json = match values.get(9) {
        Some(Value::Text(value)) => serde_json::from_str(value).map_err(|error| {
            DbError::corruption(format!(
                "malformed sync conflict remote_record_json: {error}"
            ))
        })?,
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: remote_record_json",
            ));
        }
    };
    let local_row_json = match values.get(10) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(serde_json::from_str(value).map_err(|error| {
            DbError::corruption(format!("malformed sync conflict local_row_json: {error}"))
        })?),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: local_row_json",
            ));
        }
    };
    let created_at_micros = match values.get(11) {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: created_at_micros",
            ));
        }
    };
    let resolved = match values.get(12) {
        Some(Value::Int64(value)) => *value != 0,
        Some(Value::Bool(value)) => *value,
        _ => {
            return Err(DbError::corruption("malformed sync conflict row: resolved"));
        }
    };
    let resolution = match values.get(13) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: resolution",
            ));
        }
    };
    let resolved_at_micros = match values.get(14) {
        Some(Value::Null) | None => None,
        Some(Value::Int64(value)) => Some(*value),
        Some(Value::Bool(value)) => Some(i64::from(*value)),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: resolved_at_micros",
            ));
        }
    };
    let resolved_by = match values.get(15) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: resolved_by",
            ));
        }
    };
    let resolution_note = match values.get(16) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: resolution_note",
            ));
        }
    };
    let policy_name = match values.get(17) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: policy_name",
            ));
        }
    };
    let local_record_json = match values.get(18) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(serde_json::from_str(value).map_err(|error| {
            DbError::corruption(format!(
                "malformed sync conflict local_record_json: {error}"
            ))
        })?),
        _ => {
            return Err(DbError::corruption(
                "malformed sync conflict row: local_record_json",
            ));
        }
    };

    Ok(SyncConflict {
        conflict_id,
        batch_id,
        remote_replica_id,
        remote_sequence,
        table_name,
        operation,
        conflict_type,
        message,
        primary_key_json,
        remote_record_json,
        resolution,
        resolved_at_micros,
        resolved_by,
        resolution_note,
        policy_name,
        local_record_json,
        local_row_json,
        created_at_micros,
        resolved,
    })
}

pub(super) fn sync_peer_from_row(row: &QueryRow) -> Result<SyncPeer> {
    let values = row.values();
    let name = match values.first() {
        Some(Value::Text(value)) => value.clone(),
        _ => return Err(DbError::corruption("malformed sync peer row: name")),
    };
    let endpoint = match values.get(1) {
        Some(Value::Text(value)) => value.clone(),
        _ => return Err(DbError::corruption("malformed sync peer row: endpoint")),
    };
    let token_env = match values.get(2) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => return Err(DbError::corruption("malformed sync peer row: token_env")),
    };
    let created_at_micros = match values.get(3) {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync peer row: created_at_micros",
            ))
        }
    };
    let updated_at_micros = match values.get(4) {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync peer row: updated_at_micros",
            ))
        }
    };

    Ok(SyncPeer {
        name,
        endpoint,
        token_env,
        created_at_micros,
        updated_at_micros,
    })
}

pub(super) fn sync_scope_from_row(row: &QueryRow) -> Result<SyncScope> {
    let values = row.values();
    let name = match values.first() {
        Some(Value::Text(value)) => value.clone(),
        _ => return Err(DbError::corruption("malformed sync scope row: name")),
    };
    let include_tables_json = match values.get(1) {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption(
                "malformed sync scope row: include_tables_json",
            ))
        }
    };
    let row_filter = match values.get(2) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => return Err(DbError::corruption("malformed sync scope row: row_filter")),
    };
    let filter_columns_json = match values.get(3) {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption(
                "malformed sync scope row: filter_columns_json",
            ))
        }
    };
    let created_at_micros = match values.get(4) {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync scope row: created_at_micros",
            ))
        }
    };
    let updated_at_micros = match values.get(5) {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync scope row: updated_at_micros",
            ))
        }
    };

    let include_tables: Vec<String> =
        serde_json::from_str(&include_tables_json).map_err(|error| {
            DbError::corruption(format!("malformed sync scope include tables: {error}"))
        })?;
    let filter_columns: Vec<String> =
        serde_json::from_str(&filter_columns_json).map_err(|error| {
            DbError::corruption(format!("malformed sync scope filter columns: {error}"))
        })?;

    Ok(SyncScope {
        name,
        include_tables,
        row_filter,
        filter_columns,
        created_at_micros,
        updated_at_micros,
    })
}

pub(super) fn sync_peer_scope_binding_from_row(row: &QueryRow) -> Result<SyncPeerScopeBinding> {
    let values = row.values();
    let peer_name = match values.first() {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption(
                "malformed sync peer scope row: peer_name",
            ))
        }
    };
    let scope_name = match values.get(1) {
        Some(Value::Text(value)) => value.clone(),
        _ => {
            return Err(DbError::corruption(
                "malformed sync peer scope row: scope_name",
            ))
        }
    };
    let created_at_micros = match values.get(2) {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync peer scope row: created_at_micros",
            ))
        }
    };
    let updated_at_micros = match values.get(3) {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync peer scope row: updated_at_micros",
            ))
        }
    };

    Ok(SyncPeerScopeBinding {
        peer_name,
        scope_name,
        created_at_micros,
        updated_at_micros,
    })
}

pub(super) fn sync_session_from_row(row: &QueryRow) -> Result<SyncSession> {
    let values = row.values();
    let session_id = match values.first() {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync session row: session_id",
            ))
        }
    };
    let peer_name = match values.get(1) {
        Some(Value::Text(value)) => value.clone(),
        _ => return Err(DbError::corruption("malformed sync session row: peer_name")),
    };
    let direction = match values.get(2) {
        Some(Value::Text(value)) => SyncRunDirection::from_str(value)?,
        _ => return Err(DbError::corruption("malformed sync session row: direction")),
    };
    let remote_replica_id = match values.get(3) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => {
            return Err(DbError::corruption(
                "malformed sync session row: remote_replica_id",
            ))
        }
    };
    let started_at_micros = match values.get(4) {
        Some(Value::Int64(value)) => *value,
        Some(Value::Bool(value)) => i64::from(*value),
        _ => {
            return Err(DbError::corruption(
                "malformed sync session row: started_at_micros",
            ))
        }
    };
    let ended_at_micros = match values.get(5) {
        Some(Value::Null) | None => None,
        Some(Value::Int64(value)) => Some(*value),
        Some(Value::Bool(value)) => Some(i64::from(*value)),
        _ => {
            return Err(DbError::corruption(
                "malformed sync session row: ended_at_micros",
            ))
        }
    };
    let status = match values.get(6) {
        Some(Value::Text(value)) => value.clone(),
        _ => return Err(DbError::corruption("malformed sync session row: status")),
    };
    let error = match values.get(7) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => return Err(DbError::corruption("malformed sync session row: error")),
    };
    let pushed_batch_id = match values.get(8) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => {
            return Err(DbError::corruption(
                "malformed sync session row: pushed_batch_id",
            ))
        }
    };
    let pulled_batch_id = match values.get(9) {
        Some(Value::Null) | None => None,
        Some(Value::Text(value)) => Some(value.clone()),
        _ => {
            return Err(DbError::corruption(
                "malformed sync session row: pulled_batch_id",
            ))
        }
    };
    let pushed_seen = parse_sync_i64(values.get(10), "pushed_seen")?;
    let pushed_applied = parse_sync_i64(values.get(11), "pushed_applied")?;
    let pushed_skipped = parse_sync_i64(values.get(12), "pushed_skipped")?;
    let pushed_conflicted = parse_sync_i64(values.get(13), "pushed_conflicted")?;
    let pulled_seen = parse_sync_i64(values.get(14), "pulled_seen")?;
    let pulled_applied = parse_sync_i64(values.get(15), "pulled_applied")?;
    let pulled_skipped = parse_sync_i64(values.get(16), "pulled_skipped")?;
    let pulled_conflicted = parse_sync_i64(values.get(17), "pulled_conflicted")?;
    let retry_count = parse_sync_i64(values.get(18), "retry_count")?;

    Ok(SyncSession {
        session_id,
        peer_name,
        direction,
        remote_replica_id,
        started_at_micros,
        ended_at_micros,
        status,
        error,
        pushed_batch_id,
        pulled_batch_id,
        pushed_seen,
        pushed_applied,
        pushed_skipped,
        pushed_conflicted,
        pulled_seen,
        pulled_applied,
        pulled_skipped,
        pulled_conflicted,
        retry_count,
    })
}

pub(super) fn sql_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) fn sql_nullable_text_literal(value: Option<&str>) -> String {
    value
        .map(sql_text_literal)
        .unwrap_or_else(|| "NULL".to_string())
}

pub(super) fn parse_sync_i64(value: Option<&Value>, field_name: &str) -> Result<i64> {
    match value {
        Some(Value::Int64(value)) => Ok(*value),
        Some(Value::Bool(value)) => Ok(i64::from(*value)),
        _ => Err(DbError::corruption(format!(
            "malformed sync session row: {field_name}"
        ))),
    }
}

pub(super) fn sync_session_summary_counts(
    summary: &SyncRunSummary,
) -> (i64, i64, i64, i64, i64, i64, i64, i64) {
    let pushed = summary.pushed.as_ref();
    let pulled = summary.pulled.as_ref();
    (
        pushed.map_or(0, |value| value.seen as i64),
        pushed.map_or(0, |value| value.applied as i64),
        pushed.map_or(0, |value| value.skipped as i64),
        pushed.map_or(0, |value| value.conflicted as i64),
        pulled.map_or(0, |value| value.seen as i64),
        pulled.map_or(0, |value| value.applied as i64),
        pulled.map_or(0, |value| value.skipped as i64),
        pulled.map_or(0, |value| value.conflicted as i64),
    )
}

pub(super) fn peer_watermark_key(replica_id: &str) -> String {
    format!("peer_watermark:{replica_id}")
}

pub(super) fn peer_out_watermark_key(peer_name: &str) -> String {
    format!("peer_out_watermark:{peer_name}")
}

pub(super) fn imported_record_key(replica_id: &str, sequence: u64) -> String {
    format!("sync_imported:{replica_id}:{sequence}")
}

pub(super) fn parse_uuid_text(input: &str) -> Result<[u8; 16]> {
    let mut hex = input.trim();
    if let Some(dashed) = hex
        .strip_prefix("x'")
        .and_then(|value| value.strip_suffix('\''))
    {
        hex = dashed;
    }
    let hex = hex.replace('-', "");
    if hex.len() != 32 {
        return Err(DbError::sql(format!(
            "uuid must be 32 hex digits, got {}",
            hex.len()
        )));
    }
    let mut bytes = [0u8; 16];
    for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(chunk)
            .map_err(|error| DbError::sql(format!("invalid UUID record: {error}")))?;
        bytes[index] = u8::from_str_radix(pair, 16)
            .map_err(|error| DbError::sql(format!("invalid uuid: {error}")))?;
    }
    Ok(bytes)
}

pub(super) fn parse_json_blob(input: &str) -> Result<Vec<u8>> {
    let hex = if let Some(stripped) = input
        .strip_prefix("x'")
        .and_then(|value| value.strip_suffix('\''))
    {
        stripped
    } else {
        input
    };
    if !hex.len().is_multiple_of(2) {
        return Err(DbError::sql("blob JSON text must have even length"));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(chunk)
            .map_err(|error| DbError::sql(format!("invalid blob hex record: {error}")))?;
        bytes.push(
            u8::from_str_radix(pair, 16)
                .map_err(|error| DbError::sql(format!("invalid blob hex: {error}")))?,
        );
    }
    Ok(bytes)
}

pub(super) fn json_to_column_value(
    table_name: &str,
    column: &ColumnSchema,
    value: &JsonValue,
) -> Result<Value> {
    if column.column_type == ColumnType::Enum {
        let label = value.as_str().ok_or_else(|| {
            DbError::sql(format!(
                "column '{}' in table '{table_name}' must be ENUM label text",
                column.name
            ))
        })?;
        let enum_type = column.enum_type.as_ref().ok_or_else(|| {
            DbError::sql(format!(
                "column '{}' in table '{table_name}' cannot import ENUM without enum metadata",
                column.name
            ))
        })?;
        let label_id = enum_type.label_id(label).ok_or_else(|| {
            DbError::sql(format!(
                "column '{}' in table '{table_name}' has invalid ENUM label '{label}'",
                column.name
            ))
        })?;
        return Ok(Value::Enum {
            enum_type_id: enum_type.type_id,
            label_id,
        });
    }
    json_to_typed_value(table_name, &column.name, &column.column_type, value)
}

pub(super) fn json_to_typed_value(
    table_name: &str,
    column_name: &str,
    column_type: &ColumnType,
    value: &JsonValue,
) -> Result<Value> {
    match column_type {
        ColumnType::Int64 => match value {
            JsonValue::Number(value) => value
                .as_i64()
                .map(Value::Int64)
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "column '{column_name}' in table '{table_name}' is not a valid INT64"
                    ))
                }),
            JsonValue::String(value) => value
                .parse::<i64>()
                .map(Value::Int64)
                .map_err(|error| {
                    DbError::sql(format!(
                        "column '{column_name}' in table '{table_name}' is not a valid INT64: {error}"
                    ))
                }),
            _ => Err(DbError::sql(format!(
                "column '{column_name}' in table '{table_name}' must be INT64"
            ))),
        },
        ColumnType::Float64 => match value {
            JsonValue::Number(value) => value
                .as_f64()
                .map(Value::Float64)
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "column '{column_name}' in table '{table_name}' is not a valid FLOAT64"
                    ))
                }),
            JsonValue::String(value) => value
                .parse::<f64>()
                .map(Value::Float64)
                .map_err(|error| {
                    DbError::sql(format!(
                        "column '{column_name}' in table '{table_name}' is not a valid FLOAT64: {error}"
                    ))
                }),
            _ => Err(DbError::sql(format!(
                "column '{column_name}' in table '{table_name}' must be FLOAT64"
            ))),
        },
        ColumnType::Text => match value {
            JsonValue::String(value) => Ok(Value::Text(value.clone())),
            _ => Err(DbError::sql(format!(
                "column '{column_name}' in table '{table_name}' must be TEXT"
            ))),
        },
        ColumnType::Bool => match value {
            JsonValue::Bool(value) => Ok(Value::Bool(*value)),
            _ => Err(DbError::sql(format!(
                "column '{column_name}' in table '{table_name}' must be BOOL"
            ))),
        },
        ColumnType::Blob => match value {
            JsonValue::String(value) => Ok(Value::Blob(parse_json_blob(value)?)),
            _ => Err(DbError::sql(format!(
                "column '{column_name}' in table '{table_name}' must be BLOB"
            ))),
        },
        ColumnType::Geometry => match value {
            JsonValue::String(value) => Ok(Value::Geometry(parse_json_blob(value)?)),
            _ => Err(DbError::sql(format!(
                "column '{column_name}' in table '{table_name}' must be GEOMETRY EWKB hex text"
            ))),
        },
        ColumnType::Geography => match value {
            JsonValue::String(value) => Ok(Value::Geography(parse_json_blob(value)?)),
            _ => Err(DbError::sql(format!(
                "column '{column_name}' in table '{table_name}' must be GEOGRAPHY EWKB hex text"
            ))),
        },
        ColumnType::Decimal => {
            let text = match value {
                JsonValue::Number(value) => value.to_string(),
                JsonValue::String(value) => value.clone(),
                _ => {
                    return Err(DbError::sql(format!(
                        "column '{column_name}' in table '{table_name}' must be DECIMAL"
                    )));
                }
            };
            let (scaled, scale) = parse_sync_decimal_text(&text)?;
            Ok(Value::Decimal { scaled, scale })
        }
        ColumnType::Uuid => {
            let text = value.as_str().ok_or_else(|| {
                DbError::sql(format!(
                    "column '{column_name}' in table '{table_name}' must be UUID text"
                ))
            })?;
            Ok(Value::Uuid(parse_uuid_text(text)?))
        }
        ColumnType::Timestamp => match value {
            JsonValue::Number(value) => value
                .as_i64()
                .map(Value::TimestampMicros)
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "column '{column_name}' in table '{table_name}' is not a valid TIMESTAMP"
                    ))
                }),
            JsonValue::String(value) => value
                .parse::<i64>()
                .map(Value::TimestampMicros)
                .map_err(|error| {
                    DbError::sql(format!(
                        "column '{column_name}' in table '{table_name}' is not a valid TIMESTAMP: {error}"
                    ))
                }),
            _ => Err(DbError::sql(format!(
                "column '{column_name}' in table '{table_name}' must be TIMESTAMP"
            ))),
        },
        ColumnType::Enum => Err(DbError::sql(format!(
            "column '{column_name}' in table '{table_name}' cannot import ENUM without enum metadata"
        ))),
        ColumnType::IpAddr => {
            let text = value.as_str().ok_or_else(|| {
                DbError::sql(format!(
                    "column '{column_name}' in table '{table_name}' must be IPADDR text"
                ))
            })?;
            let (family, addr) = parse_ip_addr(text)?;
            Ok(Value::IpAddr { family, addr })
        }
        ColumnType::Cidr => {
            let text = value.as_str().ok_or_else(|| {
                DbError::sql(format!(
                    "column '{column_name}' in table '{table_name}' must be CIDR text"
                ))
            })?;
            let (family, prefix_len, network) = parse_cidr(text)?;
            Ok(Value::Cidr {
                family,
                prefix_len,
                network,
            })
        }
        ColumnType::MacAddr => {
            let text = value.as_str().ok_or_else(|| {
                DbError::sql(format!(
                    "column '{column_name}' in table '{table_name}' must be MACADDR text"
                ))
            })?;
            let (len, bytes) = parse_mac_addr(text)?;
            Ok(Value::MacAddr { len, bytes })
        }
        ColumnType::Date => {
            let text = value.as_str().ok_or_else(|| {
                DbError::sql(format!(
                    "column '{column_name}' in table '{table_name}' must be DATE text"
                ))
            })?;
            Ok(Value::DateDays(parse_date_days(text)?))
        }
        ColumnType::Time => {
            let text = value.as_str().ok_or_else(|| {
                DbError::sql(format!(
                    "column '{column_name}' in table '{table_name}' must be TIME text"
                ))
            })?;
            Ok(Value::TimeMicros(parse_time_micros(text)?))
        }
        ColumnType::TimestampTz => match value {
            JsonValue::Number(value) => value
                .as_i64()
                .map(Value::TimestampTzMicros)
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "column '{column_name}' in table '{table_name}' is not a valid TIMESTAMPTZ"
                    ))
                }),
            JsonValue::String(value) => Ok(Value::TimestampTzMicros(parse_timestamp_tz_micros(value)?)),
            _ => Err(DbError::sql(format!(
                "column '{column_name}' in table '{table_name}' must be TIMESTAMPTZ"
            ))),
        },
        ColumnType::Interval => {
            let text = value.as_str().ok_or_else(|| {
                DbError::sql(format!(
                    "column '{column_name}' in table '{table_name}' must be INTERVAL text"
                ))
            })?;
            let (months, days, micros) = parse_interval(text)?;
            Ok(Value::Interval {
                months,
                days,
                micros,
            })
        }
    }
}

pub(super) fn parse_sync_decimal_text(value: &str) -> Result<(i64, u8)> {
    if let Ok(parsed) = parse_decimal_text(value) {
        return Ok(parsed);
    }
    let trimmed = value.trim();
    let (scaled_text, scale_text) = trimmed
        .split_once("e-")
        .or_else(|| trimmed.split_once("E-"))
        .ok_or_else(|| DbError::sql("invalid DECIMAL cast"))?;
    let scaled = scaled_text
        .parse::<i64>()
        .map_err(|_| DbError::sql("invalid DECIMAL cast"))?;
    let scale = scale_text
        .parse::<u8>()
        .map_err(|_| DbError::sql("invalid DECIMAL cast"))?;
    Ok(normalize_decimal(scaled, scale))
}
