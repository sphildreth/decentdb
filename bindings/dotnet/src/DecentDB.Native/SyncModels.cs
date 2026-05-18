using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DecentDB.Native;

public enum SyncRunDirection
{
    Push,
    Pull,
    Both
}

public enum SyncConflictPolicy
{
    Record,
    Stop,
    LastWriterWins,
    OriginPriority
}

public enum SyncConflictResolutionAction
{
    KeepLocal,
    ApplyRemote
}

public enum SyncDoctorSeverity
{
    Info,
    Warning,
    Error
}

public sealed class SyncStatus
{
    public bool Enabled { get; init; }
    public string? ReplicaId { get; init; }
    public ulong NextSequence { get; init; }
    public string? JournalPath { get; init; }
    public ulong JournalSizeBytes { get; init; }
}

public sealed class SyncJournalRecord
{
    public uint SchemaVersion { get; init; }
    public ulong Sequence { get; init; }
    public string ReplicaId { get; init; } = string.Empty;
    public ulong TransactionLsn { get; init; }
    public string Table { get; init; } = string.Empty;
    public string Operation { get; init; } = string.Empty;
    public JsonElement PrimaryKey { get; init; }
    public JsonElement? After { get; init; }
    public uint SchemaCookie { get; init; }
    public long CommittedAtMicros { get; init; }
}

public sealed class SyncChangeBatch
{
    public uint ProtocolVersion { get; init; }
    public string BatchId { get; init; } = string.Empty;
    public string? SourceReplicaId { get; init; }
    public ulong? FirstSequence { get; init; }
    public ulong? LastSequence { get; init; }
    public ulong? SourceHighWatermark { get; init; }
    public int RecordCount { get; init; }
    public List<SyncJournalRecord> Records { get; init; } = new();
}

public sealed class SyncImportSummary
{
    public int Seen { get; init; }
    public int Applied { get; init; }
    public int Skipped { get; init; }
    public int Conflicted { get; init; }
}

public sealed class SyncPeer
{
    public string Name { get; init; } = string.Empty;
    public string Endpoint { get; init; } = string.Empty;
    public string? TokenEnv { get; init; }
    public long CreatedAtMicros { get; init; }
    public long UpdatedAtMicros { get; init; }
}

public sealed class SyncScope
{
    public string Name { get; init; } = string.Empty;
    public List<string> IncludeTables { get; init; } = new();
    public string? RowFilter { get; init; }
    public List<string> FilterColumns { get; init; } = new();
    public long CreatedAtMicros { get; init; }
    public long UpdatedAtMicros { get; init; }
}

public sealed class SyncPeerScopeBinding
{
    public string PeerName { get; init; } = string.Empty;
    public string ScopeName { get; init; } = string.Empty;
    public long CreatedAtMicros { get; init; }
    public long UpdatedAtMicros { get; init; }
}

public sealed class SyncSession
{
    public long SessionId { get; init; }
    public string PeerName { get; init; } = string.Empty;
    public SyncRunDirection Direction { get; init; }
    public string? RemoteReplicaId { get; init; }
    public long StartedAtMicros { get; init; }
    public long? EndedAtMicros { get; init; }
    public string Status { get; init; } = string.Empty;
    public string? Error { get; init; }
    public string? PushedBatchId { get; init; }
    public string? PulledBatchId { get; init; }
    public long PushedSeen { get; init; }
    public long PushedApplied { get; init; }
    public long PushedSkipped { get; init; }
    public long PushedConflicted { get; init; }
    public long PulledSeen { get; init; }
    public long PulledApplied { get; init; }
    public long PulledSkipped { get; init; }
    public long PulledConflicted { get; init; }
    public long RetryCount { get; init; }
}

public sealed class SyncConflict
{
    public long ConflictId { get; init; }
    public string BatchId { get; init; } = string.Empty;
    public string RemoteReplicaId { get; init; } = string.Empty;
    public long RemoteSequence { get; init; }
    public string TableName { get; init; } = string.Empty;
    public string Operation { get; init; } = string.Empty;
    public string ConflictType { get; init; } = string.Empty;
    public string Message { get; init; } = string.Empty;
    public JsonElement PrimaryKeyJson { get; init; }
    public JsonElement RemoteRecordJson { get; init; }
    public string? Resolution { get; init; }
    public long? ResolvedAtMicros { get; init; }
    public string? ResolvedBy { get; init; }
    public string? ResolutionNote { get; init; }
    public string? PolicyName { get; init; }
    public JsonElement? LocalRecordJson { get; init; }
    public JsonElement? LocalRowJson { get; init; }
    public long CreatedAtMicros { get; init; }
    public bool Resolved { get; init; }
}

public sealed class SyncConflictPolicyConfig
{
    public SyncConflictPolicy DefaultPolicy { get; init; }
    public List<string> OriginPriority { get; init; } = new();
}

public sealed class SyncJournalIssue
{
    public int LineNumber { get; init; }
    public ulong? Sequence { get; init; }
    public SyncDoctorSeverity Severity { get; init; }
    public string Code { get; init; } = string.Empty;
    public string Message { get; init; } = string.Empty;
}

public sealed class SyncJournalIntegrityReport
{
    public int TotalRecords { get; init; }
    public ulong? FirstSequence { get; init; }
    public ulong? LastSequence { get; init; }
    public SyncDoctorSeverity HighestSeverity { get; init; }
    public List<SyncJournalIssue> Issues { get; init; } = new();
}

public sealed class SyncPeerLag
{
    public string PeerName { get; init; } = string.Empty;
    public string? RemoteReplicaId { get; init; }
    public ulong? InWatermark { get; init; }
    public ulong? OutWatermark { get; init; }
    public ulong? LocalHighWatermark { get; init; }
    public ulong? InLag { get; init; }
    public ulong? OutLag { get; init; }
}

public sealed class SyncRetentionReport
{
    public int JournalRecords { get; init; }
    public ulong? FirstSequence { get; init; }
    public ulong? LastSequence { get; init; }
    public ulong? SafePruneThrough { get; init; }
    public int PrunableRecords { get; init; }
    public List<string> BlockedBy { get; init; } = new();
    public ulong JournalSizeBytes { get; init; }
}

public sealed class SyncOperationalDoctorReport
{
    public SyncStatus Status { get; init; } = new();
    public SyncJournalIntegrityReport Integrity { get; init; } = new();
    public SyncRetentionReport Retention { get; init; } = new();
    public List<SyncPeerLag> PeerLag { get; init; } = new();
    public int UnresolvedConflicts { get; init; }
    public List<SyncSession> RecentSessions { get; init; } = new();
    public SyncDoctorSeverity HighestSeverity { get; init; }
    public List<SyncJournalIssue> Issues { get; init; } = new();
    public List<string> Guidance { get; init; } = new();
}

public sealed class SyncPruneSummary
{
    public ulong RequestedThrough { get; init; }
    public ulong EffectiveThrough { get; init; }
    public int Pruned { get; init; }
    public bool DryRun { get; init; }
    public bool AllowDataLoss { get; init; }
    public List<string> BlockedBy { get; init; } = new();
}

internal static class SyncJson
{
    internal static readonly JsonSerializerOptions Options = CreateOptions();

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            DictionaryKeyPolicy = JsonNamingPolicy.SnakeCaseLower,
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.Never
        };
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower));
        return options;
    }
}
