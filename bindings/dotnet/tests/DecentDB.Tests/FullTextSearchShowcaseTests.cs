using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public class FullTextSearchShowcaseTests : IDisposable
{
    private readonly string _dbPath;

    public FullTextSearchShowcaseTests()
    {
        _dbPath = ReleaseGateTestHelpers.CreateDbPath("fts_showcase");
    }

    public void Dispose()
    {
        ReleaseGateTestHelpers.DeleteDbArtifacts(_dbPath);
    }

    [Fact]
    public void AdoNetRunsFullTextSearchAndBm25Ranking()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE docs (id INT64 PRIMARY KEY, title TEXT, body TEXT)";
        cmd.ExecuteNonQuery();

        cmd.CommandText =
            "CREATE INDEX idx_docs_search ON docs USING fulltext (title, body) " +
            "WITH (prefix = '2,3')";
        cmd.ExecuteNonQuery();

        cmd.CommandText =
            "INSERT INTO docs VALUES " +
            "(1, 'Embedded database search', 'DecentDB adds rust database search search primitives.'), " +
            "(2, 'Rust database notes', 'Durable local database storage.'), " +
            "(3, 'Calendar entry', 'Lunch and project planning.')";
        cmd.ExecuteNonQuery();

        cmd.CommandText =
            "SELECT id, title, bm25('idx_docs_search') AS rank " +
            "FROM docs " +
            "WHERE fulltext_match('idx_docs_search', @query) " +
            "ORDER BY rank DESC, id";
        cmd.Parameters.Add(new DecentDBParameter("@query", "database OR search"));

        using (var reader = cmd.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal(1L, reader.GetInt64(0));
            Assert.Equal("Embedded database search", reader.GetString(1));
            var firstRank = reader.GetDouble(2);

            Assert.True(reader.Read());
            Assert.Equal(2L, reader.GetInt64(0));
            var secondRank = reader.GetDouble(2);

            Assert.False(reader.Read());
            Assert.True(firstRank > secondRank);
        }

        cmd.Parameters.Clear();
        cmd.CommandText =
            "SELECT id FROM docs " +
            "WHERE fulltext_match('idx_docs_search', @query) " +
            "ORDER BY id";
        cmd.Parameters.Add(new DecentDBParameter("@query", "dec*"));

        Assert.Equal(1L, Convert.ToInt64(cmd.ExecuteScalar()));
    }
}
