using System.Reflection;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore.Query.Internal;
using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class ModificationCommandBatchFallbackCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_modbatch_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void BuildSqlAndParameters_CoversAddedModifiedDeletedAndUnsupportedState()
    {
        using var context = CreateContext();
        var (batch, batchType) = CreateBatch(context);
        var buildSqlAndParameters = batchType.GetMethod("BuildSqlAndParameters", BindingFlags.Instance | BindingFlags.NonPublic)!;

        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var command = connection.CreateCommand())
        {
            var added = CreateCommand(
                EntityState.Added,
                "fallback_added",
                [
                    CreateColumn("id", parameterName: "@p0", value: 1L, isWrite: true),
                    CreateColumn("name", parameterName: "@p1", value: "alice", isWrite: true),
                    CreateColumn("id", isRead: true)
                ]);

            var readColumns = (List<IColumnModification>)buildSqlAndParameters.Invoke(batch, [command, added])!;
            Assert.Contains("INSERT INTO", command.CommandText, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("RETURNING", command.CommandText, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(2, command.Parameters.Count);
            Assert.Single(readColumns);
        }

        using (var command = connection.CreateCommand())
        {
            var modifiedNoWrite = CreateCommand(
                EntityState.Modified,
                "fallback_modified",
                [
                    CreateColumn("name", originalParameterName: "@orig_name", originalValue: null, useOriginalValue: true, isCondition: true)
                ]);

            _ = buildSqlAndParameters.Invoke(batch, [command, modifiedNoWrite]);
            Assert.Contains("SET", command.CommandText, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("\"name\" = \"name\"", command.CommandText, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("IS NULL", command.CommandText, StringComparison.OrdinalIgnoreCase);
        }

        using (var command = connection.CreateCommand())
        {
            var deleted = CreateCommand(
                EntityState.Deleted,
                "fallback_deleted",
                [
                    CreateColumn("id", originalParameterName: "@orig_id", originalValue: 99L, useOriginalValue: true, isCondition: true)
                ]);

            _ = buildSqlAndParameters.Invoke(batch, [command, deleted]);
            Assert.Contains("DELETE FROM", command.CommandText, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("WHERE", command.CommandText, StringComparison.OrdinalIgnoreCase);
        }

        using var unsupportedCommand = connection.CreateCommand();
        var unsupported = CreateCommand(EntityState.Unchanged, "fallback_unsupported", []);
        var unsupportedEx = Assert.Throws<TargetInvocationException>(() => buildSqlAndParameters.Invoke(batch, [unsupportedCommand, unsupported]));
        Assert.IsType<NotSupportedException>(unsupportedEx.InnerException);
    }

    [Fact]
    public void ExecuteCommand_CoversReadPathAndConcurrencyExceptionPath()
    {
        using var context = CreateContext();
        var (batch, batchType) = CreateBatch(context);
        var executeCommand = batchType.GetMethod("ExecuteCommand", BindingFlags.Instance | BindingFlags.NonPublic)!;

        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var setup = connection.CreateCommand())
        {
            setup.CommandText = """
                CREATE TABLE IF NOT EXISTS fallback_exec (
                  id INTEGER PRIMARY KEY,
                  name TEXT NULL
                );
                """;
            setup.ExecuteNonQuery();
            setup.CommandText = "DELETE FROM fallback_exec;";
            setup.ExecuteNonQuery();
        }

        var readBackId = CreateColumn("id", isRead: true);
        var insert = CreateCommand(
            EntityState.Added,
            "fallback_exec",
            [
                CreateColumn("id", parameterName: "@id", value: 1L, isWrite: true),
                CreateColumn("name", parameterName: "@name", value: "alpha", isWrite: true),
                readBackId
            ]);

        executeCommand.Invoke(batch, [connection, insert]);
        Assert.Equal(1L, readBackId.Value);

        var updateNoMatch = CreateCommand(
            EntityState.Modified,
            "fallback_exec",
            [
                CreateColumn("name", parameterName: "@name", value: "beta", isWrite: true),
                CreateColumn("id", originalParameterName: "@orig_id", originalValue: 999L, useOriginalValue: true, isCondition: true)
            ]);

        var ex = Assert.Throws<TargetInvocationException>(() => executeCommand.Invoke(batch, [connection, updateNoMatch]));
        Assert.IsType<DbUpdateConcurrencyException>(ex.InnerException);
    }

    [Fact]
    public void ConversionHelpers_CoverGuidAndReadConversions()
    {
        using var context = CreateContext();
        var (_, batchType) = CreateBatch(context);
        var convertRead = batchType.GetMethod("ConvertReadValue", BindingFlags.Static | BindingFlags.NonPublic)!;
        var convertToProvider = batchType.GetMethod("ConvertToProviderValue", BindingFlags.Static | BindingFlags.NonPublic)!;

        var shortProperty = CreateProperty(typeof(short));
        var byteProperty = CreateProperty(typeof(byte));
        var boolProperty = CreateProperty(typeof(bool));

        var shortColumn = CreateColumn("v", property: shortProperty);
        var byteColumn = CreateColumn("v", property: byteProperty);
        var boolColumn = CreateColumn("v", property: boolProperty);

        Assert.Null(convertRead.Invoke(null, [shortColumn, DBNull.Value]));
        Assert.Equal((short)7, convertRead.Invoke(null, [shortColumn, 7L]));
        Assert.Equal((byte)8, convertRead.Invoke(null, [byteColumn, 8L]));
        Assert.Equal(true, convertRead.Invoke(null, [boolColumn, 1L]));

        var guidColumn = CreateColumn("g", typeMapping: new BareGuidTypeMapping());
        var invalidBytesEx = Assert.Throws<TargetInvocationException>(() => convertToProvider.Invoke(null, [guidColumn, new byte[15]]));
        Assert.True(invalidBytesEx.InnerException is InvalidOperationException or InvalidCastException);

        var guid = Guid.NewGuid();
        var guidResult = convertToProvider.Invoke(null, [guidColumn, guid.ToString("D")]);
        Assert.Equal(guid, guidResult);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private TestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;
        return new TestContext(options);
    }

    private static (object Batch, Type BatchType) CreateBatch(DbContext context)
    {
        var dependencies = context.GetService<ModificationCommandBatchFactoryDependencies>();
        var batchType = typeof(DecentDBWindowFunctionTranslator).Assembly
            .GetType("DecentDB.EntityFrameworkCore.Update.Internal.DecentDBModificationCommandBatch", throwOnError: true)!;
        return (Activator.CreateInstance(batchType, dependencies)!, batchType);
    }

    private static IReadOnlyModificationCommand CreateCommand(
        EntityState state,
        string tableName,
        IReadOnlyList<IColumnModification> columns)
    {
        return CreateProxy<IReadOnlyModificationCommand>((method, _) => method.Name switch
        {
            "get_EntityState" => state,
            "get_TableName" => tableName,
            "get_Schema" => null,
            "get_ColumnModifications" => columns,
            "get_Entries" => Array.Empty<IUpdateEntry>(),
            _ => throw new NotSupportedException($"Unhandled command member: {method.Name}")
        });
    }

    private static IProperty CreateProperty(Type clrType)
    {
        return CreateProxy<IProperty>((method, _) => method.Name switch
        {
            "get_ClrType" => clrType,
            _ => throw new NotSupportedException($"Unhandled property member: {method.Name}")
        });
    }

    private static IColumnModification CreateColumn(
        string columnName,
        string? parameterName = null,
        object? value = null,
        bool isRead = false,
        bool isWrite = false,
        bool isCondition = false,
        string? originalParameterName = null,
        object? originalValue = null,
        bool useOriginalValue = false,
        IProperty? property = null,
        RelationalTypeMapping? typeMapping = null)
    {
        object? currentValue = value;
        return CreateProxy<IColumnModification>((method, args) =>
        {
            switch (method.Name)
            {
                case "get_ColumnName": return columnName;
                case "get_ParameterName": return parameterName;
                case "get_OriginalParameterName": return originalParameterName;
                case "get_Value": return currentValue;
                case "set_Value":
                    currentValue = args![0];
                    return null;
                case "get_OriginalValue": return originalValue;
                case "get_UseOriginalValue": return useOriginalValue;
                case "get_IsRead": return isRead;
                case "get_IsWrite": return isWrite;
                case "get_IsCondition": return isCondition;
                case "get_Property": return property;
                case "get_TypeMapping": return typeMapping;
                default:
                    throw new NotSupportedException($"Unhandled column member: {method.Name}");
            }
        });
    }

    private static T CreateProxy<T>(Func<MethodInfo, object?[]?, object?> handler) where T : class
    {
        var proxy = DispatchProxy.Create<T, FuncDispatchProxy<T>>();
        ((FuncDispatchProxy<T>)(object)proxy).Handler = handler;
        return proxy;
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class TestContext(DbContextOptions<TestContext> options) : DbContext(options)
    {
    }

    private class FuncDispatchProxy<T> : DispatchProxy
    {
        public Func<MethodInfo, object?[]?, object?> Handler { get; set; } = null!;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod is null)
            {
                throw new InvalidOperationException("Proxy method cannot be null.");
            }

            return Handler(targetMethod, args);
        }
    }

    private sealed class BareGuidTypeMapping : RelationalTypeMapping
    {
        public BareGuidTypeMapping()
            : base(new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(Guid)),
                storeType: "UUID",
                dbType: System.Data.DbType.Guid))
        {
        }

        private BareGuidTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new BareGuidTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
            => throw new NotSupportedException();
    }
}
