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
        Assert.Equal("DECIMAL(18,4)", GetMapping<decimal>(mappingSource).StoreType);
        Assert.Equal("UUID", GetMapping<Guid>(mappingSource).StoreType);
        Assert.Equal("TIMESTAMP", GetMapping<DateTime>(mappingSource).StoreType);
        Assert.Equal("TIMESTAMP", GetMapping<DateTimeOffset>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<DateOnly>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<TimeOnly>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<TimeSpan>(mappingSource).StoreType);
    }

    [Fact]
    public void TypeMappingSource_MapsStoreTypeAliasesUsedBySchemaDiscovery()
    {
        using var context = CreateContext();
        var mappingSource = context.GetService<IRelationalTypeMappingSource>();

        // DecentDB schema discovery currently returns these type names.
        Assert.Equal("INTEGER", mappingSource.FindMapping("INT64")!.StoreType);
        Assert.Equal("INTEGER", mappingSource.FindMapping("INT32")!.StoreType);
        Assert.Equal("REAL", mappingSource.FindMapping("FLOAT64")!.StoreType);
        Assert.Equal("TIMESTAMP", mappingSource.FindMapping("TIMESTAMP")!.StoreType);
        Assert.Equal("TIMESTAMP", mappingSource.FindMapping("DATETIME")!.StoreType);
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
        Assert.Equal((long)(new DateTimeOffset(dateTime, TimeSpan.Zero) - DateTimeOffset.UnixEpoch).TotalMicroseconds, dtProvider);
        Assert.Equal((long)(dateTimeOffset - DateTimeOffset.UnixEpoch).TotalMicroseconds, dtoProvider);
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

    [Fact]
    public void EnsureCreated_WithDateTimeColumns_UsesTimestampSchema()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_datetime_schema_{Guid.NewGuid():N}.ddb");

        try
        {
            var optionsBuilder = new DbContextOptionsBuilder<DateTimeSchemaContext>();
            optionsBuilder.UseDecentDB($"Data Source={dbPath}");

            using var context = new DateTimeSchemaContext(optionsBuilder.Options);
            var createScript = context.Database.GenerateCreateScript();

            Assert.Contains("\"OccurredAt\" TIMESTAMP NOT NULL", createScript);
            Assert.Contains("\"ProcessedAt\" TIMESTAMP NULL", createScript);

            context.Database.EnsureCreated();
        }
        finally
        {
            TryDelete(dbPath);
            TryDelete(dbPath + "-wal");
        }
    }

    [Fact]
    public void DateTimeColumns_RoundTripThroughEntityFramework()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_datetime_roundtrip_{Guid.NewGuid():N}.ddb");

        try
        {
            var optionsBuilder = new DbContextOptionsBuilder<DateTimeSchemaContext>();
            optionsBuilder.UseDecentDB($"Data Source={dbPath}");

            var occurredAt = new DateTime(2020, 3, 4, 0, 0, 0, DateTimeKind.Utc);
            var processedAt = new DateTimeOffset(2020, 5, 6, 7, 8, 9, TimeSpan.Zero);

            using (var writeContext = new DateTimeSchemaContext(optionsBuilder.Options))
            {
                writeContext.Database.EnsureCreated();
                writeContext.Events.Add(new DateTimeEntity
                {
                    OccurredAt = occurredAt,
                    ProcessedAt = processedAt
                });

                writeContext.SaveChanges();
            }

            using (var readContext = new DateTimeSchemaContext(optionsBuilder.Options))
            {
                var entity = readContext.Events.Single();
                Assert.Equal(occurredAt, entity.OccurredAt);
                Assert.Equal(processedAt, entity.ProcessedAt);
            }
        }
        finally
        {
            TryDelete(dbPath);
            TryDelete(dbPath + "-wal");
        }
    }

    private DbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<SmokeDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
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

    private sealed class DateTimeSchemaContext : DbContext
    {
        public DateTimeSchemaContext(DbContextOptions<DateTimeSchemaContext> options)
            : base(options)
        {
        }

        public DbSet<DateTimeEntity> Events => Set<DateTimeEntity>();
    }

    private sealed class DateTimeEntity
    {
        public int Id { get; set; }

        public DateTime OccurredAt { get; set; }

        public DateTimeOffset? ProcessedAt { get; set; }
    }
}
