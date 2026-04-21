using System.Data;
using System.Data.Common;
using System.Reflection;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class CommonAdoNetRegressionTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_adonet_common_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void ParameterCollection_SupportsCommonCollectionOperations()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        var p1 = new DecentDBParameter("@a", 1);
        var p2 = new DecentDBParameter("@b", 2);
        var p3 = new DecentDBParameter("@c", 3);

        command.Parameters.AddRange(new object[] { p1, p2 });
        command.Parameters.Insert(1, p3);

        Assert.Equal(3, command.Parameters.Count);
        Assert.True(command.Parameters.Contains("@a"));
        Assert.True(command.Parameters.Contains(p2));
        Assert.Equal(1, command.Parameters.IndexOf("@c"));

        command.Parameters[0] = new DecentDBParameter("@a", 10);
        command.Parameters["@b"] = new DecentDBParameter("@b", 20);

        var copy = new object[3];
        command.Parameters.CopyTo(copy, 0);
        Assert.All(copy, item => Assert.IsType<DecentDBParameter>(item));

        command.Parameters.RemoveAt("@c");
        Assert.Equal(2, command.Parameters.Count);
        command.Parameters.Remove(command.Parameters["@b"]);
        Assert.Single(command.Parameters.Cast<DbParameter>());

        command.Parameters.Clear();
        Assert.Empty(command.Parameters.Cast<DbParameter>());
    }

    [Fact]
    public void ParameterCollection_RejectsWrongParameterTypes()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.Parameters.Add(new DecentDBParameter("@p0", 1));

        Assert.Throws<ArgumentException>(() => command.Parameters.Add(new object()));
        Assert.Throws<ArgumentException>(() => command.Parameters.Insert(0, new object()));
        Assert.Throws<ArgumentException>(() => command.Parameters.AddRange(new object[] { new DecentDBParameter("@ok", 1), new object() }));
        Assert.Throws<ArgumentException>(() => command.Parameters[0] = new DummyDbParameter());
        Assert.Throws<ArgumentException>(() => command.Parameters["@p0"] = new DummyDbParameter());
        Assert.Throws<IndexOutOfRangeException>(() => _ = command.Parameters["@missing"]);
    }

    [Fact]
    public async Task VacuumAtomicAsync_RejectsBlankPath_AndCleansTempOnCliFailure()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => DecentDBMaintenance.VacuumAtomicAsync(" "));

        using (var connection = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
            command.ExecuteNonQuery();
        }

        var failingCliPath = OperatingSystem.IsWindows()
            ? Path.Combine(Path.GetTempPath(), $"missing_cli_{Guid.NewGuid():N}.exe")
            : CreateFailingCliScript();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => DecentDBMaintenance.VacuumAtomicAsync(_dbPath, failingCliPath));

        Assert.Contains("Vacuum", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(_dbPath + ".vacuum_tmp"));
    }

    [Fact]
    public void Maintenance_PathResolutionHelpers_HandleAbsoluteRelativeAndFallback()
    {
        var maintenanceType = typeof(DecentDBMaintenance);
        var resolveMethod = maintenanceType.GetMethod("ResolveCliExecutablePath", BindingFlags.NonPublic | BindingFlags.Static)!;
        var candidateMethod = maintenanceType.GetMethod("CandidateCliPaths", BindingFlags.NonPublic | BindingFlags.Static)!;

        var tempDir = Path.Combine(Path.GetTempPath(), $"decentdb_maintenance_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        var cliPath = Path.Combine(tempDir, OperatingSystem.IsWindows() ? "decentdb.exe" : "decentdb");
        File.WriteAllText(cliPath, "stub");

        try
        {
            var absolute = (string)resolveMethod.Invoke(null, [cliPath])!;
            Assert.Equal(cliPath, absolute);

            var previousDirectory = Directory.GetCurrentDirectory();
            try
            {
                Directory.SetCurrentDirectory(tempDir);
                var relative = (string)resolveMethod.Invoke(null, [Path.GetFileName(cliPath)])!;
                Assert.Equal(Path.GetFullPath(cliPath), relative);
            }
            finally
            {
                Directory.SetCurrentDirectory(previousDirectory);
            }

            var fallback = (string)resolveMethod.Invoke(null, ["definitely-missing-cli"])!;
            Assert.True(
                fallback == "definitely-missing-cli" || File.Exists(fallback),
                $"Expected unresolved cli name or discovered executable path, got '{fallback}'.");

            var candidates = ((IEnumerable<string>)candidateMethod.Invoke(null, [tempDir])!).ToList();
            Assert.NotEmpty(candidates);
            Assert.All(candidates, candidate => Assert.StartsWith(tempDir, candidate, StringComparison.Ordinal));
        }
        finally
        {
            TryDelete(cliPath);
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
        TryDelete(_dbPath + ".vacuum_tmp");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private static string CreateFailingCliScript()
    {
        var scriptPath = Path.Combine(Path.GetTempPath(), $"decentdb_fail_{Guid.NewGuid():N}.sh");
        File.WriteAllText(scriptPath, "#!/usr/bin/env bash\necho forced failure >&2\nexit 42\n");
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(
                scriptPath,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
                UnixFileMode.GroupRead | UnixFileMode.GroupExecute |
                UnixFileMode.OtherRead | UnixFileMode.OtherExecute);
        }
        return scriptPath;
    }

    #pragma warning disable CS8764, CS8765
    private sealed class DummyDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
        public override bool IsNullable { get; set; }
        public override string? ParameterName { get; set; }
        public override string SourceColumn { get; set; } = string.Empty;
        public override object? Value { get; set; }
        public override bool SourceColumnNullMapping { get; set; }
        public override int Size { get; set; }
        public override void ResetDbType()
        {
        }
    }
    #pragma warning restore CS8764, CS8765
}
