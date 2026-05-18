using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DecentDB.Native;

public sealed class DecentDBSyncClient
{
    private readonly DecentDB _db;

    internal DecentDBSyncClient(DecentDB db)
    {
        _db = db;
    }

    public string ExecuteRawJson(string requestJson) => _db.SyncExecuteJson(requestJson);

    public Task<string> ExecuteRawJsonAsync(string requestJson, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ExecuteRawJson(requestJson));
    }

    public SyncStatus GetStatus() => Execute<SyncStatus>(new { op = "status" });

    public Task<SyncStatus> GetStatusAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetStatus());
    }

    public SyncStatus InitializeReplica(string replicaId) =>
        Execute<SyncStatus>(new { op = "init_replica", replica_id = replicaId });

    public Task<SyncStatus> InitializeReplicaAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(InitializeReplica(replicaId));
    }

    public SyncStatus SetEnabled(bool enabled) =>
        Execute<SyncStatus>(new { op = "set_enabled", enabled });

    public Task<SyncStatus> SetEnabledAsync(bool enabled, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(SetEnabled(enabled));
    }

    public List<SyncJournalRecord> GetPendingChanges(ulong since = 0, int limit = 100) =>
        Execute<List<SyncJournalRecord>>(new { op = "pending_changes", since, limit });

    public Task<List<SyncJournalRecord>> GetPendingChangesAsync(ulong since = 0, int limit = 100, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetPendingChanges(since, limit));
    }

    public SyncChangeBatch ExportBatch(ulong since = 0, int limit = 100, string? scope = null) =>
        Execute<SyncChangeBatch>(new { op = "export_batch", since, limit, scope });

    public Task<SyncChangeBatch> ExportBatchAsync(ulong since = 0, int limit = 100, string? scope = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ExportBatch(since, limit, scope));
    }

    public SyncImportSummary ImportBatch(SyncChangeBatch batch, string? scope = null, SyncConflictPolicy? conflictPolicy = null)
    {
        ArgumentNullException.ThrowIfNull(batch);
        return Execute<SyncImportSummary>(new
        {
            op = "import_batch",
            batch,
            scope,
            conflict_policy = conflictPolicy.HasValue ? ConflictPolicyName(conflictPolicy.Value) : null
        });
    }

    public Task<SyncImportSummary> ImportBatchAsync(SyncChangeBatch batch, string? scope = null, SyncConflictPolicy? conflictPolicy = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ImportBatch(batch, scope, conflictPolicy));
    }

    public SyncPeer AddPeer(SyncPeer peer)
    {
        ArgumentNullException.ThrowIfNull(peer);
        return Execute<SyncPeer>(new
        {
            op = "add_peer",
            name = peer.Name,
            endpoint = peer.Endpoint,
            token_env = peer.TokenEnv
        });
    }

    public Task<SyncPeer> AddPeerAsync(SyncPeer peer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(AddPeer(peer));
    }

    public bool RemovePeer(string name) => Execute<RemovedResponse>(new { op = "remove_peer", name }).Removed;

    public Task<bool> RemovePeerAsync(string name, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(RemovePeer(name));
    }

    public List<SyncPeer> ListPeers() => Execute<List<SyncPeer>>(new { op = "peers" });

    public Task<List<SyncPeer>> ListPeersAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ListPeers());
    }

    public SyncScope CreateScope(SyncScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return Execute<SyncScope>(new
        {
            op = "create_scope",
            name = scope.Name,
            include_tables = scope.IncludeTables,
            row_filter = scope.RowFilter
        });
    }

    public Task<SyncScope> CreateScopeAsync(SyncScope scope, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(CreateScope(scope));
    }

    public bool DropScope(string name) => Execute<RemovedResponse>(new { op = "drop_scope", name }).Removed;

    public Task<bool> DropScopeAsync(string name, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(DropScope(name));
    }

    public List<SyncScope> ListScopes() => Execute<List<SyncScope>>(new { op = "scopes" });

    public Task<List<SyncScope>> ListScopesAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ListScopes());
    }

    public SyncPeerScopeBinding BindPeerScope(string peerName, string scopeName) =>
        Execute<SyncPeerScopeBinding>(new { op = "bind_peer_scope", peer_name = peerName, scope_name = scopeName });

    public Task<SyncPeerScopeBinding> BindPeerScopeAsync(string peerName, string scopeName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(BindPeerScope(peerName, scopeName));
    }

    public bool UnbindPeerScope(string peerName) => Execute<RemovedResponse>(new { op = "unbind_peer_scope", peer_name = peerName }).Removed;

    public Task<bool> UnbindPeerScopeAsync(string peerName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(UnbindPeerScope(peerName));
    }

    public List<SyncPeerScopeBinding> ListPeerScopeBindings() =>
        Execute<List<SyncPeerScopeBinding>>(new { op = "peer_scope_bindings" });

    public Task<List<SyncPeerScopeBinding>> ListPeerScopeBindingsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ListPeerScopeBindings());
    }

    public List<SyncSession> ListSessions() => Execute<List<SyncSession>>(new { op = "sessions" });

    public Task<List<SyncSession>> ListSessionsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ListSessions());
    }

    public List<SyncConflict> ListConflicts(bool includeResolved = false) =>
        Execute<List<SyncConflict>>(new { op = "conflicts", all = includeResolved });

    public Task<List<SyncConflict>> ListConflictsAsync(bool includeResolved = false, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ListConflicts(includeResolved));
    }

    public SyncConflict? GetConflict(long id) =>
        ExecuteNullable<SyncConflict>(new { op = "conflict", id });

    public Task<SyncConflict?> GetConflictAsync(long id, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetConflict(id));
    }

    public SyncConflict? ResolveConflict(long id, SyncConflictResolutionAction action, string? by = null, string? note = null)
    {
        return ExecuteNullable<SyncConflict>(new
        {
            op = "resolve_conflict",
            id,
            action = ConflictResolutionActionName(action),
            by,
            note
        });
    }

    public Task<SyncConflict?> ResolveConflictAsync(long id, SyncConflictResolutionAction action, string? by = null, string? note = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ResolveConflict(id, action, by, note));
    }

    public SyncConflict? ReopenConflict(long id) => ExecuteNullable<SyncConflict>(new { op = "reopen_conflict", id });

    public Task<SyncConflict?> ReopenConflictAsync(long id, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ReopenConflict(id));
    }

    public SyncConflictPolicyConfig GetConflictPolicy() => Execute<SyncConflictPolicyConfig>(new { op = "conflict_policy" });

    public Task<SyncConflictPolicyConfig> GetConflictPolicyAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetConflictPolicy());
    }

    public SyncConflictPolicyConfig SetConflictPolicy(SyncConflictPolicy policy, IReadOnlyList<string>? originPriority = null) =>
        Execute<SyncConflictPolicyConfig>(new
        {
            op = "set_conflict_policy",
            policy = ConflictPolicyName(policy),
            origin_priority = originPriority ?? Array.Empty<string>()
        });

    public Task<SyncConflictPolicyConfig> SetConflictPolicyAsync(SyncConflictPolicy policy, IReadOnlyList<string>? originPriority = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(SetConflictPolicy(policy, originPriority));
    }

    public SyncOperationalDoctorReport GetDoctorReport() => Execute<SyncOperationalDoctorReport>(new { op = "doctor" });

    public Task<SyncOperationalDoctorReport> GetDoctorReportAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetDoctorReport());
    }

    public SyncRetentionReport GetRetentionReport() => Execute<SyncRetentionReport>(new { op = "retention" });

    public Task<SyncRetentionReport> GetRetentionReportAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetRetentionReport());
    }

    public List<SyncPeerLag> GetPeerLag() => Execute<List<SyncPeerLag>>(new { op = "peer_lag" });

    public Task<List<SyncPeerLag>> GetPeerLagAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(GetPeerLag());
    }

    public SyncPruneSummary Prune(ulong through, bool dryRun = false, bool allowDataLoss = false) =>
        Execute<SyncPruneSummary>(new { op = "prune", through, dry_run = dryRun, allow_data_loss = allowDataLoss });

    public Task<SyncPruneSummary> PruneAsync(ulong through, bool dryRun = false, bool allowDataLoss = false, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Prune(through, dryRun, allowDataLoss));
    }

    private T Execute<T>(object request)
    {
        var json = ExecuteRawJson(JsonSerializer.Serialize(request, SyncJson.Options));
        return JsonSerializer.Deserialize<T>(json, SyncJson.Options)
            ?? throw new JsonException($"Sync response for {typeof(T).Name} was null.");
    }

    private T? ExecuteNullable<T>(object request) where T : class
    {
        var json = ExecuteRawJson(JsonSerializer.Serialize(request, SyncJson.Options));
        return JsonSerializer.Deserialize<T>(json, SyncJson.Options);
    }

    private static string ConflictPolicyName(SyncConflictPolicy policy) => policy switch
    {
        SyncConflictPolicy.Record => "record",
        SyncConflictPolicy.Stop => "stop",
        SyncConflictPolicy.LastWriterWins => "last_writer_wins",
        SyncConflictPolicy.OriginPriority => "origin_priority",
        _ => throw new ArgumentOutOfRangeException(nameof(policy))
    };

    private static string ConflictResolutionActionName(SyncConflictResolutionAction action) => action switch
    {
        SyncConflictResolutionAction.KeepLocal => "keep_local",
        SyncConflictResolutionAction.ApplyRemote => "apply_remote",
        _ => throw new ArgumentOutOfRangeException(nameof(action))
    };

    private sealed class RemovedResponse
    {
        public bool Removed { get; init; }
    }
}
