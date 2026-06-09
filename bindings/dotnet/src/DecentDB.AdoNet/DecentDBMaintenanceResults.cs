using System;
using System.Collections.Generic;

namespace DecentDB.AdoNet;

/// <summary>
/// File-size snapshot for a DecentDB database and its WAL sidecars.
/// </summary>
public sealed class DecentDBWalStatus
{
    public DecentDBWalStatus(
        string databasePath,
        long databaseBytes,
        string dashWalPath,
        long dashWalBytes,
        string dottedWalPath,
        long dottedWalBytes,
        string coordinationPath,
        long coordinationBytes)
    {
        DatabasePath = databasePath;
        DatabaseBytes = databaseBytes;
        DashWalPath = dashWalPath;
        DashWalBytes = dashWalBytes;
        DottedWalPath = dottedWalPath;
        DottedWalBytes = dottedWalBytes;
        CoordinationPath = coordinationPath;
        CoordinationBytes = coordinationBytes;
    }

    public string DatabasePath { get; }
    public long DatabaseBytes { get; }
    public string DashWalPath { get; }
    public long DashWalBytes { get; }
    public string DottedWalPath { get; }
    public long DottedWalBytes { get; }
    public string CoordinationPath { get; }
    public long CoordinationBytes { get; }
    public long TotalWalBytes => checked(DashWalBytes + DottedWalBytes);
    public bool HasWal => TotalWalBytes > 0;
}

/// <summary>
/// Result from an in-process checkpoint against a DecentDB database file.
/// </summary>
public sealed class DecentDBCheckpointResult
{
    public DecentDBCheckpointResult(
        string databasePath,
        bool databaseExisted,
        DecentDBWalStatus before,
        DecentDBWalStatus after,
        TimeSpan duration)
    {
        DatabasePath = databasePath;
        DatabaseExisted = databaseExisted;
        Before = before;
        After = after;
        Duration = duration;
    }

    public string DatabasePath { get; }
    public bool DatabaseExisted { get; }
    public DecentDBWalStatus Before { get; }
    public DecentDBWalStatus After { get; }
    public TimeSpan Duration { get; }
}

/// <summary>
/// Result from an in-process index rebuild operation.
/// </summary>
public sealed class DecentDBIndexRebuildResult
{
    public DecentDBIndexRebuildResult(
        string databasePath,
        bool databaseExisted,
        IReadOnlyList<string> indexes,
        DecentDBWalStatus before,
        DecentDBWalStatus after,
        TimeSpan duration)
    {
        DatabasePath = databasePath;
        DatabaseExisted = databaseExisted;
        Indexes = indexes;
        Before = before;
        After = after;
        Duration = duration;
    }

    public string DatabasePath { get; }
    public bool DatabaseExisted { get; }
    public IReadOnlyList<string> Indexes { get; }
    public int IndexCount => Indexes.Count;
    public DecentDBWalStatus Before { get; }
    public DecentDBWalStatus After { get; }
    public TimeSpan Duration { get; }
}

/// <summary>
/// Result from an in-process compact/save-as operation.
/// </summary>
public sealed class DecentDBCompactResult
{
    public DecentDBCompactResult(
        string sourceDatabasePath,
        string destinationDatabasePath,
        bool sourceExisted,
        long sourceBytes,
        long destinationBytes,
        TimeSpan duration)
    {
        SourceDatabasePath = sourceDatabasePath;
        DestinationDatabasePath = destinationDatabasePath;
        SourceExisted = sourceExisted;
        SourceBytes = sourceBytes;
        DestinationBytes = destinationBytes;
        Duration = duration;
    }

    public string SourceDatabasePath { get; }
    public string DestinationDatabasePath { get; }
    public bool SourceExisted { get; }
    public long SourceBytes { get; }
    public long DestinationBytes { get; }
    public TimeSpan Duration { get; }
}

/// <summary>
/// Result from an in-process vacuum/compact operation.
/// </summary>
public sealed class DecentDBVacuumResult
{
    public DecentDBVacuumResult(
        string databasePath,
        bool databaseExisted,
        bool backupCreated,
        string? backupPath,
        DecentDBWalStatus before,
        DecentDBWalStatus after,
        TimeSpan duration)
    {
        DatabasePath = databasePath;
        DatabaseExisted = databaseExisted;
        BackupCreated = backupCreated;
        BackupPath = backupPath;
        Before = before;
        After = after;
        Duration = duration;
    }

    public string DatabasePath { get; }
    public bool DatabaseExisted { get; }
    public bool BackupCreated { get; }
    public string? BackupPath { get; }
    public DecentDBWalStatus Before { get; }
    public DecentDBWalStatus After { get; }
    public TimeSpan Duration { get; }
}
