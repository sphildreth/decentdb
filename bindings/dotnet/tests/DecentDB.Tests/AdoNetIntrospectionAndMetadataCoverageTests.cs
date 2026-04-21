using System.Reflection;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;
using Xunit;

namespace DecentDB.Tests;

public sealed class AdoNetIntrospectionAndMetadataCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_introspection_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void Connection_IntrospectionApis_ReturnExpectedSchemaArtifacts()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var setup = connection.CreateCommand())
        {
            setup.CommandText = """
                CREATE TABLE schema_items (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL
                );
                CREATE UNIQUE INDEX ix_schema_items_name ON schema_items (name);
                CREATE VIEW schema_items_view AS SELECT id, name FROM schema_items;
                CREATE TRIGGER schema_items_touch AFTER INSERT ON schema_items BEGIN SELECT 1; END;
                """;
            setup.ExecuteNonQuery();
        }

        Assert.True(DecentDBConnection.AbiVersion() > 0);
        Assert.False(string.IsNullOrWhiteSpace(DecentDBConnection.EngineVersion()));

        Assert.Contains("schema_items", connection.GetTableDdl("schema_items"), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("schema_items_view", connection.ListViewsJson(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("schema_items_view", connection.GetViewDdl("schema_items_view"), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("schema_items_touch", connection.ListTriggersJson(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("schema_items", connection.ListTablesJson(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("is_active", connection.GetTableColumnsJson("schema_items"), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ix_schema_items_name", connection.ListIndexesJson(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ConnectionExtension_Checkpoint_ValidatesNullAndDelegates()
    {
        Assert.Throws<ArgumentNullException>(() => DecentDBConnectionExtensions.Checkpoint(null!));

        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();
        DecentDBConnectionExtensions.Checkpoint(connection);
    }

    [Fact]
    public void MicroOrmAttributes_ExposeConfiguredMetadata()
    {
        var table = typeof(AttributeEntity).GetCustomAttribute<TableAttribute>();
        Assert.NotNull(table);
        Assert.Equal("entity_table", table!.Name);

        var idProperty = typeof(AttributeEntity).GetProperty(nameof(AttributeEntity.Id))!;
        Assert.NotNull(idProperty.GetCustomAttribute<PrimaryKeyAttribute>());

        var payloadProperty = typeof(AttributeEntity).GetProperty(nameof(AttributeEntity.Payload))!;
        Assert.NotNull(payloadProperty.GetCustomAttribute<NotNullAttribute>());
        Assert.NotNull(payloadProperty.GetCustomAttribute<ColumnAttribute>());
        Assert.Equal("payload_col", payloadProperty.GetCustomAttribute<ColumnAttribute>()!.Name);

        var optionalProperty = typeof(AttributeEntity).GetProperty(nameof(AttributeEntity.OptionalValue))!;
        Assert.NotNull(optionalProperty.GetCustomAttribute<NullableAttribute>());
        Assert.NotNull(optionalProperty.GetCustomAttribute<IgnoreAttribute>());

        var indexAttributes = payloadProperty.GetCustomAttributes<IndexAttribute>().ToArray();
        Assert.Equal(2, indexAttributes.Length);
        Assert.Contains(indexAttributes, a => a.Name == "ix_payload" && a.Unique);
        Assert.Contains(indexAttributes, a => a.Name == null && !a.Unique);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    [Table("entity_table")]
    private sealed class AttributeEntity
    {
        [PrimaryKey]
        public long Id { get; set; }

        [Column("payload_col")]
        [NotNull]
        [Index(Name = "ix_payload", Unique = true)]
        [Index]
        public string Payload { get; set; } = string.Empty;

        [Nullable]
        [Ignore]
        public string? OptionalValue { get; set; }
    }
}
