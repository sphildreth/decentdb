using System.Collections.Generic;
using System.Data.Common;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public class SqlParameterRewriterTests
{
    [Fact]
    public void Rewrite_NamedParam_StartingWithDigit_IsRewritten()
    {
        var sql = "SELECT * FROM t WHERE id = @8__locals2_artistApiKey";
        var parameters = new List<DbParameter>
        {
            new DecentDBParameter { ParameterName = "@8__locals2_artistApiKey", Value = 42 }
        };

        var (rewritten, paramMap) = SqlParameterRewriter.Rewrite(sql, parameters);

        Assert.DoesNotContain("@8__locals2", rewritten);
        Assert.Contains("$", rewritten);
        Assert.Single(paramMap);
    }

    [Fact]
    public void Rewrite_NamedParam_StartingWithLetter_IsRewritten()
    {
        var sql = "SELECT * FROM t WHERE id = @myParam";
        var parameters = new List<DbParameter>
        {
            new DecentDBParameter { ParameterName = "@myParam", Value = 1 }
        };

        var (rewritten, paramMap) = SqlParameterRewriter.Rewrite(sql, parameters);

        Assert.DoesNotContain("@myParam", rewritten);
        Assert.Contains("$", rewritten);
        Assert.Single(paramMap);
    }

    [Fact]
    public void Rewrite_MultipleDigitPrefixedParams_EachGetUniqueIndex()
    {
        var sql = "SELECT * FROM t WHERE a = @8__locals1_x AND b = @8__locals2_y";
        var parameters = new List<DbParameter>
        {
            new DecentDBParameter { ParameterName = "@8__locals1_x", Value = 1 },
            new DecentDBParameter { ParameterName = "@8__locals2_y", Value = 2 }
        };

        var (rewritten, paramMap) = SqlParameterRewriter.Rewrite(sql, parameters);

        Assert.DoesNotContain("@8__locals", rewritten);
        Assert.Equal(2, paramMap.Count);
    }

    [Fact]
    public void Rewrite_SameDigitPrefixedParam_UsedTwice_GetsSameIndex()
    {
        var sql = "SELECT * FROM t WHERE a = @5__2 OR b = @5__2";
        var parameters = new List<DbParameter>
        {
            new DecentDBParameter { ParameterName = "@5__2", Value = 99 }
        };

        var (rewritten, paramMap) = SqlParameterRewriter.Rewrite(sql, parameters);

        Assert.DoesNotContain("@5__2", rewritten);
        Assert.Single(paramMap);
    }
}
