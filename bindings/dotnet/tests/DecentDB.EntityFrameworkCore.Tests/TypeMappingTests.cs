using System.Data;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class TypeMappingTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_type_mapping_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void TypeMappingSource_MapsSupportedTypesToExpectedStoreTypes()
    {
        using var context = CreateContext();
        var mappingSource = context.GetService<IRelationalTypeMappingSource>();

        Assert.Equal("BOOLEAN", GetMapping<bool>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<int>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<long>(mappingSource).StoreType);
        Assert.Equal("REAL", GetMapping<double>(mappingSource).StoreType);
        Assert.Equal("TEXT", GetMapping<string>(mappingSource).StoreType);
        Assert.Equal("BLOB", GetMapping<byte[]>(mappingSource).StoreType);
        Assert.Equal("DECIMAL", GetMapping<decimal>(mappingSource).StoreType);
        Assert.Equal("UUID", GetMapping<Guid>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<DateTime>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<DateTimeOffset>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<DateOnly>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<TimeOnly>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<TimeSpan>(mappingSource).StoreType);
    }

    [Fact]
    public void TypeMappings_RoundTripDateTimeGuidAndDecimal()
    {
        using var context = CreateContext();
        var mappingSource = context.GetService<IRelationalTypeMappingSource>();

        var dateTime = new DateTime(2024, 01, 02, 03, 04, 05, DateTimeKind.Utc);
        var dateTimeOffset = new DateTimeOffset(2024, 02, 03, 04, 05, 06, TimeSpan.Zero);
        var dateOnly = new DateOnly(2024, 02, 04);
        var timeOnly = new TimeOnly(9, 30, 15, 123);
        var timeSpan = TimeSpan.FromHours(12.5);
        var guid = Guid.NewGuid();
        var dec = 1234.5678m;

        var dateTimeMapping = GetMapping<DateTime>(mappingSource);
        var dateTimeOffsetMapping = GetMapping<DateTimeOffset>(mappingSource);
        var dateOnlyMapping = GetMapping<DateOnly>(mappingSource);
        var timeOnlyMapping = GetMapping<TimeOnly>(mappingSource);
        var timeSpanMapping = GetMapping<TimeSpan>(mappingSource);
        var guidMapping = GetMapping<Guid>(mappingSource);
        var decimalMapping = GetMapping<decimal>(mappingSource);

        var dtProvider = (long)dateTimeMapping.Converter!.ConvertToProvider(dateTime)!;
        var dtoProvider = (long)dateTimeOffsetMapping.Converter!.ConvertToProvider(dateTimeOffset)!;
        var dProvider = (long)dateOnlyMapping.Converter!.ConvertToProvider(dateOnly)!;
        var tProvider = (long)timeOnlyMapping.Converter!.ConvertToProvider(timeOnly)!;
        var tsProvider = (long)timeSpanMapping.Converter!.ConvertToProvider(timeSpan)!;
        var gProvider = (byte[])guidMapping.Converter!.ConvertToProvider(guid)!;
        var decProvider = decimalMapping.Converter?.ConvertToProvider(dec) ?? dec;

        var epochDay = DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber;
        Assert.Equal(new DateTimeOffset(dateTime, TimeSpan.Zero).ToUnixTimeMilliseconds(), dtProvider);
        Assert.Equal(dateTimeOffset.ToUnixTimeMilliseconds(), dtoProvider);
        Assert.Equal(dateOnly.DayNumber - epochDay, dProvider);
        Assert.Equal(timeOnly.Ticks, tProvider);
        Assert.Equal(timeSpan.Ticks, tsProvider);
        Assert.Equal(16, gProvider.Length);

        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (dt INTEGER, dto INTEGER, d INTEGER, t INTEGER, s INTEGER, g UUID, dec_val DECIMAL(18,4))";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO t (dt, dto, d, t, s, g, dec_val) VALUES (@dt, @dto, @d, @t, @s, @g, @dec)";
        cmd.Parameters.Clear();
        cmd.Parameters.Add(new DecentDBParameter("@dt", dtProvider));
        cmd.Parameters.Add(new DecentDBParameter("@dto", dtoProvider));
        cmd.Parameters.Add(new DecentDBParameter("@d", dProvider));
        cmd.Parameters.Add(new DecentDBParameter("@t", tProvider));
        cmd.Parameters.Add(new DecentDBParameter("@s", tsProvider));
        cmd.Parameters.Add(new DecentDBParameter("@g", gProvider));
        cmd.Parameters.Add(new DecentDBParameter("@dec", decProvider));
        Assert.Equal(1, cmd.ExecuteNonQuery());

        cmd.CommandText = "SELECT dt, dto, d, t, s, g, dec_val FROM t";
        cmd.Parameters.Clear();

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var dtRoundTrip = (DateTime)dateTimeMapping.Converter!.ConvertFromProvider(reader.GetInt64(0))!;
        var dtoRoundTrip = (DateTimeOffset)dateTimeOffsetMapping.Converter!.ConvertFromProvider(reader.GetInt64(1))!;
        var dRoundTrip = (DateOnly)dateOnlyMapping.Converter!.ConvertFromProvider(reader.GetInt64(2))!;
        var tRoundTrip = (TimeOnly)timeOnlyMapping.Converter!.ConvertFromProvider(reader.GetInt64(3))!;
        var tsRoundTrip = (TimeSpan)timeSpanMapping.Converter!.ConvertFromProvider(reader.GetInt64(4))!;
        var gRoundTrip = (Guid)guidMapping.Converter!.ConvertFromProvider(reader.GetFieldValue<byte[]>(5))!;
        var decRoundTrip = (decimal)(decimalMapping.Converter?.ConvertFromProvider(reader.GetDecimal(6)) ?? reader.GetDecimal(6));

        Assert.Equal(dateTime, dtRoundTrip);
        Assert.Equal(dateTimeOffset, dtoRoundTrip);
        Assert.Equal(dateOnly, dRoundTrip);
        Assert.Equal(timeOnly, tRoundTrip);
        Assert.Equal(timeSpan, tsRoundTrip);
        Assert.Equal(guid, gRoundTrip);
        Assert.Equal(dec, decRoundTrip);
    }

    [Fact]
    public void SqlGenerationHelper_QuotesIdentifiers()
    {
        using var context = CreateContext();
        var sqlHelper = context.GetService<ISqlGenerationHelper>();

        Assert.Equal("\"users\"", sqlHelper.DelimitIdentifier("users"));
        Assert.Equal("\"app\".\"users\"", sqlHelper.DelimitIdentifier("users", "app"));
        Assert.Equal("\"my\"\"table\"", sqlHelper.DelimitIdentifier("my\"table"));
    }

    private DbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<SmokeDbContext>();
        optionsBuilder.UseDecentDb($"Data Source={_dbPath}");
        return new SmokeDbContext(optionsBuilder.Options);
    }

    private static RelationalTypeMapping GetMapping<T>(IRelationalTypeMappingSource mappingSource)
        => (RelationalTypeMapping)mappingSource.FindMapping(typeof(T))!;

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class SmokeDbContext : DbContext
    {
        public SmokeDbContext(DbContextOptions<SmokeDbContext> options)
            : base(options)
        {
        }
    }
}
