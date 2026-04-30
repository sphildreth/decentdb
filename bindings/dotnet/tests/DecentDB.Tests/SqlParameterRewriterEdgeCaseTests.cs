using System.Data.Common;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class SqlParameterRewriterEdgeCaseTests
{
    [Fact]
    public void Rewrite_HandlesCommentsQuotesAndExplicitPositionalMarkers()
    {
        const string sql = """
            SELECT '@literal', "@identifier", -- @line_comment
                   /* @block_comment */ ?, $2, ?, @named
            """;

        var parameters = new List<DbParameter>
        {
            new DecentDBParameter { ParameterName = string.Empty, Value = 11 },
            new DecentDBParameter { ParameterName = string.Empty, Value = 33 },
            new DecentDBParameter { ParameterName = "2", Value = 22 },
            new DecentDBParameter { ParameterName = "@named", Value = 44 }
        };

        var (rewritten, map) = SqlParameterRewriter.Rewrite(sql, parameters);

        Assert.Contains("'@literal'", rewritten, StringComparison.Ordinal);
        Assert.Contains("\"@identifier\"", rewritten, StringComparison.Ordinal);
        Assert.Contains("-- @line_comment", rewritten, StringComparison.Ordinal);
        Assert.Contains("/* @block_comment */", rewritten, StringComparison.Ordinal);
        Assert.Contains("$1", rewritten, StringComparison.Ordinal);
        Assert.Contains("$2", rewritten, StringComparison.Ordinal);
        Assert.Contains("$3", rewritten, StringComparison.Ordinal);
        Assert.Contains("$4", rewritten, StringComparison.Ordinal);
        Assert.Equal(4, map.Count);
        Assert.Equal(11, map[1].Value);
        Assert.Equal(22, map[2].Value);
        Assert.Equal(33, map[3].Value);
        Assert.Equal(44, map[4].Value);
    }

    [Fact]
    public void Rewrite_LeavesNonIdentifierAtSignSequencesUntouched()
    {
        const string sql = "SELECT @, @+1, @@";

        var (rewritten, map) = SqlParameterRewriter.Rewrite(sql, Array.Empty<DbParameter>());

        Assert.Equal(sql, rewritten);
        Assert.Empty(map);
    }

    [Fact]
    public void Rewrite_MissingNamedParameter_Throws()
    {
        var parameters = new[] { (DbParameter)new DecentDBParameter { ParameterName = "@other", Value = 1 } };
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlParameterRewriter.Rewrite("SELECT @missing", parameters));

        Assert.Contains("@missing", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Rewrite_MissingPositionalParameter_Throws()
    {
        var parameters = new[] { (DbParameter)new DecentDBParameter { ParameterName = "@other", Value = 1 } };
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlParameterRewriter.Rewrite("SELECT $1", parameters));

        Assert.Contains("$1", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ClampOffsetParameters_ClampsNegativeNumeric_AndIgnoresNonNumeric()
    {
        var negative = new DecentDBParameter { ParameterName = "@off1", Value = -5L };
        var nonNumeric = new DecentDBParameter { ParameterName = "@off2", Value = "oops" };

        var map = new Dictionary<int, DbParameter>
        {
            [1] = negative,
            [2] = nonNumeric
        };

        SqlParameterRewriter.ClampOffsetParameters("SELECT 1 OFFSET $1 ROWS OFFSET $2 ROWS", map);

        Assert.Equal(0L, negative.Value);
        Assert.Equal("oops", nonNumeric.Value);
    }

    [Theory]
    [InlineData("UPDATE users AS u SET name = @p0 WHERE u.id = @p1", "users.id")]
    [InlineData("DELETE FROM users AS u WHERE u.id = @p0", "users.id")]
    public void StripUpdateDeleteAlias_RewritesAliasReferences(string sql, string expectedReference)
    {
        var rewritten = SqlParameterRewriter.StripUpdateDeleteAlias(sql);

        Assert.DoesNotContain(" AS u", rewritten, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(expectedReference, rewritten, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void StripUpdateDeleteAlias_NonAliasToken_AfterAs_IsLeftUnchanged()
    {
        const string sql = "UPDATE users ASSET value = 1";

        var rewritten = SqlParameterRewriter.StripUpdateDeleteAlias(sql);

        Assert.Equal(sql, rewritten);
    }
}
