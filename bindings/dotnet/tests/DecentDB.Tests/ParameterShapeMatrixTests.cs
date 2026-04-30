using System.Data.Common;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class ParameterShapeMatrixTests : IDisposable
{
    private readonly string _dbPath = ReleaseGateTestHelpers.CreateDbPath("parameter_shape_matrix");

    public void Dispose()
    {
        ReleaseGateTestHelpers.DeleteDbArtifacts(_dbPath);
    }

    public static TheoryData<string, string[], int> SupportedShapeCases => new()
    {
        {
            "SELECT @id, @other",
            new[] { "@id", "@other" },
            2
        },
        {
            "SELECT @p0, @p1, @p0",
            new[] { "@p0", "@p1" },
            2
        },
        {
            "SELECT @__name_0, @__artist_1",
            new[] { "@__name_0", "@__artist_1" },
            2
        },
        {
            """
            INSERT INTO stage_probe (id, payload)
            VALUES (@p0_0, @p0_1),
                   (@p1_0, @p1_1)
            """,
            new[] { "@p0_0", "@p0_1", "@p1_0", "@p1_1" },
            4
        },
        {
            """
            INSERT INTO stage_probe (id, payload)
            VALUES (@p10_11, @p10_12),
                   (@p20_21, @p20_22)
            """,
            new[] { "@p10_11", "@p10_12", "@p20_21", "@p20_22" },
            4
        }
    };

    [Theory]
    [MemberData(nameof(SupportedShapeCases))]
    public void Rewrite_SupportedParameterShapes_ResolvesAllParameters(
        string sql,
        string[] parameterNames,
        int expectedParameterCount)
    {
        var parameters = parameterNames
            .Select((name, index) => (DbParameter)new DecentDBParameter
            {
                ParameterName = name,
                Value = index + 1
            })
            .ToArray();

        var (rewritten, parameterMap) = SqlParameterRewriter.Rewrite(sql, parameters);

        Assert.DoesNotContain('@', rewritten);
        Assert.Equal(expectedParameterCount, parameterMap.Count);
    }

    [Fact]
    public void ExecuteNonQuery_MultiRowEfStyleParameterNames_InsertsAllRows()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var setup = connection.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE stage_probe (id INTEGER PRIMARY KEY, payload INTEGER NOT NULL)";
            setup.ExecuteNonQuery();
        }

        using var command = connection.CreateCommand();
        command.CommandText = """
                              INSERT INTO stage_probe (id, payload)
                              VALUES (@p0_0, @p0_1),
                                     (@p1_0, @p1_1)
                              """;
        AddParameter(command, "@p0_0", 1);
        AddParameter(command, "@p0_1", 10);
        AddParameter(command, "@p1_0", 2);
        AddParameter(command, "@p1_1", 20);

        var affected = command.ExecuteNonQuery();

        Assert.Equal(2, affected);

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM stage_probe";
        Assert.Equal(2L, Convert.ToInt64(verify.ExecuteScalar()));
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
