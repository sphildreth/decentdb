using System.Reflection;
using System.Runtime.ExceptionServices;
using DecentDB.EntityFrameworkCore;
using DecentDB.Native;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class ModificationBatchBindValueCoverageTests : IDisposable
{
    private static readonly MethodInfo BindValueMethod = typeof(Microsoft.EntityFrameworkCore.DecentDBDbContextOptionsBuilderExtensions)
        .Assembly
        .GetType("DecentDB.EntityFrameworkCore.Update.Internal.DecentDBModificationCommandBatch", throwOnError: true)!
        .GetMethod("BindValue", BindingFlags.NonPublic | BindingFlags.Static)!;

    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_bind_value_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void BindValue_SupportsAllProviderPrimitiveShapes_AndRejectsUnknownTypes()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        using var statement = db.Prepare("SELECT $1");

        object?[] values =
        [
            null,
            DBNull.Value,
            (byte)1,
            (short)2,
            3,
            4L,
            (ushort)5,
            (uint)6,
            (ulong)7,
            1.25f,
            2.5d,
            3.75m,
            true,
            "hello",
            DateTime.SpecifyKind(new DateTime(2024, 1, 2, 3, 4, 5), DateTimeKind.Utc),
            new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero),
            TimeSpan.FromMinutes(5),
            new DateOnly(2024, 1, 2),
            new TimeOnly(3, 4, 5),
            new byte[] { 1, 2, 3 },
            Guid.NewGuid(),
            TestStatus.Active
        ];

        foreach (var value in values)
        {
            InvokeBindValue(statement, value);
            Assert.Equal(1, statement.Step());
            statement.Reset().ClearBindings();
        }

        Assert.Throws<NotSupportedException>(() => InvokeBindValue(statement, new object()));
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
        {
            File.Delete(_dbPath);
        }

        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }
    }

    private static void InvokeBindValue(PreparedStatement statement, object? value)
    {
        try
        {
            BindValueMethod.Invoke(null, [statement, 1, value]);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
        }
    }

    private enum TestStatus
    {
        Unknown = 0,
        Active = 1
    }
}
