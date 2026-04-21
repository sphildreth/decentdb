using System.Data;
using System.Reflection;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class AdoNetParameterTransactionSplitterCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_adonet_param_tx_split_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void Parameter_ValidationAndResetBranches_AreCovered()
    {
        var parameter = new DecentDBParameter("value", DbType.Int32);
        parameter.Precision = 4;
        parameter.Scale = 2;
        parameter.SourceColumnNullMapping = true;

        Assert.True(parameter.SourceColumnNullMapping);
        Assert.True(GetInternalBool(parameter, "HasPrecision"));
        Assert.True(GetInternalBool(parameter, "HasScale"));

        parameter.ResetDbType();
        Assert.Equal(DbType.String, parameter.DbType);
        Assert.Equal((byte)0, parameter.Precision);
        Assert.Equal((byte)0, parameter.Scale);
        Assert.False(GetInternalBool(parameter, "HasPrecision"));
        Assert.False(GetInternalBool(parameter, "HasScale"));

        parameter.ParameterName = null!;
        parameter.Value = null;
        Assert.Equal(string.Empty, parameter.ParameterName);
        Assert.Equal(DBNull.Value, parameter.Value);

        Assert.Throws<ArgumentException>(() => parameter.DbType = (DbType)(-1));
        Assert.Throws<NotSupportedException>(() => parameter.Direction = ParameterDirection.Output);
        Assert.Throws<ArgumentException>(() => parameter.Size = -1);
    }

    [Fact]
    public void Transaction_CompletedAndClosedConnectionBranches_AreCovered()
    {
        using var connection = OpenConnection();

        using (var committed = connection.BeginTransaction())
        {
            committed.Commit();
            Assert.Throws<InvalidOperationException>(() => committed.Commit());
        }

        using (var commitWhenClosed = connection.BeginTransaction())
        {
            connection.Close();
            Assert.Throws<InvalidOperationException>(() => commitWhenClosed.Commit());
            connection.Open();
        }

        using (var rolledBack = connection.BeginTransaction())
        {
            rolledBack.Rollback();
            Assert.Throws<InvalidOperationException>(() => rolledBack.Rollback());
        }

        using (var rollbackWhenClosed = connection.BeginTransaction())
        {
            connection.Close();
            rollbackWhenClosed.Rollback();
            connection.Open();
        }
    }

    [Fact]
    public void SqlStatementSplitter_HandlesCommentsQuotesAndIncompleteTokens()
    {
        Assert.Empty(SqlStatementSplitter.Split("   "));

        var withEscapedQuotes = SqlStatementSplitter.Split("SELECT 'a''b'; SELECT \"c\";");
        Assert.Equal(2, withEscapedQuotes.Count);

        var lineCommentNoNewline = SqlStatementSplitter.Split("SELECT 1; -- trailing comment");
        Assert.Equal(2, lineCommentNoNewline.Count);
        Assert.Equal("SELECT 1", lineCommentNoNewline[0]);
        Assert.StartsWith("--", lineCommentNoNewline[1], StringComparison.Ordinal);

        var unterminatedBlockComment = SqlStatementSplitter.Split("SELECT 2; /* unterminated");
        Assert.Equal(2, unterminatedBlockComment.Count);
        Assert.Equal("SELECT 2", unterminatedBlockComment[0]);
        Assert.StartsWith("/*", unterminatedBlockComment[1], StringComparison.Ordinal);

        var unterminatedQuote = SqlStatementSplitter.Split("SELECT 'unterminated");
        Assert.Single(unterminatedQuote);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private DecentDBConnection OpenConnection()
    {
        var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();
        return connection;
    }

    private static bool GetInternalBool(object instance, string propertyName)
    {
        var property = instance.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(property);
        return (bool)property!.GetValue(instance)!;
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
