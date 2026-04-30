using System.Data;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class ConnectionStateEventTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_state_evt_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + "-wal"))
            File.Delete(_dbPath + "-wal");
    }

    [Fact]
    public void Open_FiresStateChange_ClosedToOpen()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        var events = new System.Collections.Generic.List<StateChangeEventArgs>();
        conn.StateChange += (s, e) => events.Add(e);

        conn.Open();

        Assert.Single(events);
        Assert.Equal(ConnectionState.Closed, events[0].OriginalState);
        Assert.Equal(ConnectionState.Open, events[0].CurrentState);
    }

    [Fact]
    public void Close_FiresStateChange_OpenToClosed()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        var events = new System.Collections.Generic.List<StateChangeEventArgs>();
        conn.StateChange += (s, e) => events.Add(e);

        conn.Close();

        Assert.Single(events);
        Assert.Equal(ConnectionState.Open, events[0].OriginalState);
        Assert.Equal(ConnectionState.Closed, events[0].CurrentState);
    }

    [Fact]
    public void OpenThenClose_FiresBothEvents()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        var events = new System.Collections.Generic.List<StateChangeEventArgs>();
        conn.StateChange += (s, e) => events.Add(e);

        conn.Open();
        conn.Close();

        Assert.Equal(2, events.Count);
        Assert.Equal(ConnectionState.Closed, events[0].OriginalState);
        Assert.Equal(ConnectionState.Open, events[0].CurrentState);
        Assert.Equal(ConnectionState.Open, events[1].OriginalState);
        Assert.Equal(ConnectionState.Closed, events[1].CurrentState);
    }

    [Fact]
    public void DoubleOpen_FiresEventOnlyOnce()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        var events = new System.Collections.Generic.List<StateChangeEventArgs>();
        conn.StateChange += (s, e) => events.Add(e);

        conn.Open();
        conn.Open(); // second call should be no-op

        Assert.Single(events);
    }

    [Fact]
    public void DoubleClose_FiresEventOnlyOnce()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        conn.Close();

        var events = new System.Collections.Generic.List<StateChangeEventArgs>();
        conn.StateChange += (s, e) => events.Add(e);

        conn.Close(); // already closed
        conn.Close(); // still closed

        Assert.Empty(events);
    }

    [Fact]
    public void Dispose_AfterOpen_FiresStateChangeToClosed()
    {
        var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        var events = new System.Collections.Generic.List<StateChangeEventArgs>();
        conn.StateChange += (s, e) => events.Add(e);

        conn.Dispose();

        Assert.Single(events);
        Assert.Equal(ConnectionState.Open, events[0].OriginalState);
        Assert.Equal(ConnectionState.Closed, events[0].CurrentState);
    }
}
