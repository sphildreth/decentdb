using System.Data;
using System.Globalization;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using NodaTime;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class LiteralExecutabilityContractTests : IDisposable
{
    private readonly string _coreDbPath = Path.Combine(Path.GetTempPath(), $"test_ef_literal_contract_{Guid.NewGuid():N}.ddb");
    private readonly string _nodaDbPath = Path.Combine(Path.GetTempPath(), $"test_ef_literal_contract_noda_{Guid.NewGuid():N}.ddb");

    public static IEnumerable<object?[]> Cases
    {
        get
        {
            yield return Case("bool_false", typeof(bool), false);
            yield return Case("bool_true", typeof(bool), true);
            yield return Case("bool_nullable_true", typeof(bool?), true);
            yield return Case("bool_nullable_null", typeof(bool?), null);
            yield return Case("byte_42", typeof(byte), (byte)42);
            yield return Case("sbyte_negative", typeof(sbyte), (sbyte)-42);
            yield return Case("short_negative", typeof(short), (short)-12345);
            yield return Case("ushort_max", typeof(ushort), ushort.MaxValue);
            yield return Case("int_min", typeof(int), int.MinValue);
            yield return Case("int_max", typeof(int), int.MaxValue);
            yield return Case("uint_large", typeof(uint), 4_000_000_000u);
            yield return Case("long_min", typeof(long), long.MinValue);
            yield return Case("long_max", typeof(long), long.MaxValue);
            yield return Case("ulong_in_range", typeof(ulong), (ulong)long.MaxValue);
            yield return Case("float_pi", typeof(float), 3.14f);
            yield return Case("double_e", typeof(double), 2.718281828d);
            yield return Case("decimal_scale_4", typeof(decimal), 1234.5678m);
            yield return Case("string_ascii", typeof(string), "hello");
            yield return Case("string_quotes", typeof(string), "with 'single' quotes");
            yield return Case("string_newline", typeof(string), "line1\nline2");
            yield return Case("string_unicode", typeof(string), "日本語 🎵 Straße");
            yield return Case("char_ascii", typeof(char), 'A');
            yield return Case("char_quote", typeof(char), '\'');
            yield return Case("blob_bytes", typeof(byte[]), new byte[] { 0x01, 0x02, 0xFF, 0x00, 0xAB }, blockedOnSlice: "S5");
            yield return Case("guid_standard", typeof(Guid), Guid.Parse("11111111-2222-3333-4444-555555555555"));
            yield return Case("guid_empty", typeof(Guid), Guid.Empty);
            yield return Case("guid_nullable_value", typeof(Guid?), Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
            yield return Case("guid_nullable_null", typeof(Guid?), null);
            yield return Case("datetime_utc", typeof(DateTime), new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc));
            yield return Case("datetime_nullable_utc", typeof(DateTime?), new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc));
            yield return Case("datetime_nullable_null", typeof(DateTime?), null);
            yield return Case("datetimeoffset_utc", typeof(DateTimeOffset), new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero));
            yield return Case("datetimeoffset_nullable_utc", typeof(DateTimeOffset?), new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero));
            yield return Case("datetimeoffset_nullable_null", typeof(DateTimeOffset?), null);
            yield return Case("dateonly_basic", typeof(DateOnly), new DateOnly(2024, 2, 4));
            yield return Case("dateonly_nullable_value", typeof(DateOnly?), new DateOnly(2024, 2, 4));
            yield return Case("dateonly_nullable_null", typeof(DateOnly?), null);
            yield return Case("timeonly_basic", typeof(TimeOnly), new TimeOnly(9, 30, 15));
            yield return Case("timeonly_nullable_value", typeof(TimeOnly?), new TimeOnly(9, 30, 15));
            yield return Case("timeonly_nullable_null", typeof(TimeOnly?), null);
            yield return Case("timespan_half_day", typeof(TimeSpan), TimeSpan.FromHours(12.5));
            yield return Case("timespan_nullable_value", typeof(TimeSpan?), TimeSpan.FromHours(12.5));
            yield return Case("timespan_nullable_null", typeof(TimeSpan?), null);
            yield return Case("enum_int_backed", typeof(IntBackedLiteralEnum), IntBackedLiteralEnum.Enabled);
            yield return Case("enum_long_backed", typeof(LongBackedLiteralEnum), LongBackedLiteralEnum.Ready);
            yield return Case(
                "nodatime_instant",
                typeof(Instant),
                Instant.FromDateTimeUtc(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)),
                useNodaTime: true);
            yield return Case(
                "nodatime_localdate",
                typeof(LocalDate),
                new LocalDate(2024, 2, 4),
                useNodaTime: true);
            yield return Case(
                "nodatime_localdatetime",
                typeof(LocalDateTime),
                new LocalDateTime(2024, 2, 4, 9, 30, 15),
                useNodaTime: true);
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void GenerateSqlLiteral_ProducesExecutableRoundTrippableSql(
        string caseName,
        Type clrType,
        object? value,
        string blockedOnSlice,
        bool useNodaTime)
    {
        using var context = useNodaTime ? CreateNodaTimeContext() : CreateCoreContext();

        var mappingSource = context.GetService<IRelationalTypeMappingSource>();
        var mapping = (RelationalTypeMapping?)mappingSource.FindMapping(clrType);
        Assert.NotNull(mapping);

        var result = TryExecuteLiteralRoundTrip(context, mapping!, value);

        if (blockedOnSlice.Length == 0)
        {
            Assert.True(
                result.Success,
                $"{caseName} failed but is expected to pass. {result.Detail}");
            return;
        }

        Assert.False(
            result.Success,
            $"{caseName} unexpectedly passed. Remove the blocker and move this row out of {blockedOnSlice}. {result.Detail}");
    }

    public void Dispose()
    {
        TryDelete(_coreDbPath);
        TryDelete(_coreDbPath + "-wal");
        TryDelete(_nodaDbPath);
        TryDelete(_nodaDbPath + "-wal");
    }

    [Fact]
    public void GenerateSqlLiteral_ULongOverflow_ThrowsOverflowException()
    {
        using var context = CreateCoreContext();
        var mappingSource = context.GetService<IRelationalTypeMappingSource>();
        var mapping = (RelationalTypeMapping?)mappingSource.FindMapping(typeof(ulong));
        Assert.NotNull(mapping);

        var ex = Assert.Throws<OverflowException>(() => mapping!.GenerateSqlLiteral(ulong.MaxValue));
        Assert.Contains("INT64 range", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void GenerateSqlLiteral_CharSurrogate_ThrowsArgumentException()
    {
        using var context = CreateCoreContext();
        var mappingSource = context.GetService<IRelationalTypeMappingSource>();
        var mapping = (RelationalTypeMapping?)mappingSource.FindMapping(typeof(char));
        Assert.NotNull(mapping);

        var ex = Assert.Throws<ArgumentException>(() => mapping!.GenerateSqlLiteral('\uD83D'));
        Assert.Contains("Surrogate", ex.Message, StringComparison.Ordinal);
    }

    private DbContext CreateCoreContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<CoreLiteralContext>();
        optionsBuilder.UseDecentDB($"Data Source={_coreDbPath}");
        return new CoreLiteralContext(optionsBuilder.Options);
    }

    private DbContext CreateNodaTimeContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<NodaLiteralContext>();
        optionsBuilder.UseDecentDB($"Data Source={_nodaDbPath}", options => options.UseNodaTime());
        return new NodaLiteralContext(optionsBuilder.Options);
    }

    private static object?[] Case(
        string caseName,
        Type clrType,
        object? value,
        string blockedOnSlice = "",
        bool useNodaTime = false)
        => new object?[] { caseName, clrType, value, blockedOnSlice, useNodaTime };

    private static RoundTripResult TryExecuteLiteralRoundTrip(DbContext context, RelationalTypeMapping mapping, object? value)
    {
        string? literal;
        try
        {
            literal = mapping.GenerateSqlLiteral(value);
        }
        catch (Exception ex)
        {
            return RoundTripResult.Failure($"GenerateSqlLiteral threw {ex.GetType().Name}: {ex.Message}");
        }

        if (literal is null)
        {
            return RoundTripResult.Failure("GenerateSqlLiteral returned null.");
        }

        try
        {
            var connection = (DecentDBConnection)context.Database.GetDbConnection();
            var shouldClose = connection.State != ConnectionState.Open;
            if (shouldClose)
            {
                connection.Open();
            }

            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"SELECT {literal}";
                using var reader = command.ExecuteReader();
                if (!reader.Read())
                {
                    return RoundTripResult.Failure($"SELECT {literal} returned no rows.");
                }

                var actual = reader.GetValue(0);
                var compareError = CompareRoundTripValue(mapping, value, actual);
                if (compareError is not null)
                {
                    return RoundTripResult.Failure(
                        $"SELECT {literal} produced {DescribeValue(actual)}. {compareError}");
                }

                return RoundTripResult.Successful($"Literal {literal} executed and round-tripped.");
            }
            finally
            {
                if (shouldClose)
                {
                    connection.Close();
                }
            }
        }
        catch (Exception ex)
        {
            return RoundTripResult.Failure($"Executing SELECT {literal} threw {ex.GetType().Name}: {ex.Message}");
        }
    }

    private static string? CompareRoundTripValue(RelationalTypeMapping mapping, object? expectedClrValue, object? actualValue)
    {
        if (expectedClrValue is null)
        {
            return actualValue is null or DBNull
                ? null
                : $"Expected null, got {DescribeValue(actualValue)}.";
        }

        var expectedProvider = mapping.Converter?.ConvertToProvider(expectedClrValue) ?? expectedClrValue;
        if (expectedProvider is null)
        {
            return actualValue is null or DBNull
                ? null
                : $"Expected null, got {DescribeValue(actualValue)}.";
        }

        if (expectedProvider is bool expectedBool)
        {
            if (!TryConvertToBoolean(actualValue, out var actualBool))
            {
                return $"Expected BOOLEAN literal but got {DescribeValue(actualValue)}.";
            }

            return expectedBool == actualBool
                ? null
                : $"Expected {expectedBool}, got {actualBool}.";
        }

        if (expectedProvider is byte[] expectedBytes)
        {
            if (actualValue is not byte[] actualBytes)
            {
                return $"Expected BLOB literal but got {DescribeValue(actualValue)}.";
            }

            return expectedBytes.SequenceEqual(actualBytes)
                ? null
                : $"Expected {DescribeValue(expectedBytes)}, got {DescribeValue(actualBytes)}.";
        }

        if (expectedProvider is float expectedSingle)
        {
            if (!TryConvertToSingle(actualValue, out var actualSingle))
            {
                return $"Expected REAL (float) literal but got {DescribeValue(actualValue)}.";
            }

            return BitConverter.SingleToInt32Bits(expectedSingle) == BitConverter.SingleToInt32Bits(actualSingle)
                ? null
                : $"Expected {expectedSingle.ToString("R", CultureInfo.InvariantCulture)}, got {actualSingle.ToString("R", CultureInfo.InvariantCulture)}.";
        }

        if (expectedProvider is double expectedDouble)
        {
            if (!TryConvertToDouble(actualValue, out var actualDouble))
            {
                return $"Expected REAL (double) literal but got {DescribeValue(actualValue)}.";
            }

            return BitConverter.DoubleToInt64Bits(expectedDouble) == BitConverter.DoubleToInt64Bits(actualDouble)
                ? null
                : $"Expected {expectedDouble.ToString("R", CultureInfo.InvariantCulture)}, got {actualDouble.ToString("R", CultureInfo.InvariantCulture)}.";
        }

        if (expectedProvider is decimal expectedDecimal)
        {
            if (!TryConvertToDecimal(actualValue, out var actualDecimal))
            {
                return $"Expected DECIMAL literal but got {DescribeValue(actualValue)}.";
            }

            return expectedDecimal == actualDecimal
                ? null
                : $"Expected {expectedDecimal.ToString(CultureInfo.InvariantCulture)}, got {actualDecimal.ToString(CultureInfo.InvariantCulture)}.";
        }

        if (expectedProvider is string expectedString)
        {
            if (actualValue is not string actualString)
            {
                return $"Expected TEXT literal but got {DescribeValue(actualValue)}.";
            }

            return string.Equals(expectedString, actualString, StringComparison.Ordinal)
                ? null
                : $"Expected {DescribeValue(expectedString)}, got {DescribeValue(actualString)}.";
        }

        if (TryConvertToInt64(expectedProvider, out var expectedLong))
        {
            if (!TryConvertToInt64(actualValue, out var actualLong))
            {
                return $"Expected INTEGER-compatible literal but got {DescribeValue(actualValue)}.";
            }

            return expectedLong == actualLong
                ? null
                : $"Expected {expectedLong}, got {actualLong}.";
        }

        return Equals(expectedProvider, actualValue)
            ? null
            : $"Expected {DescribeValue(expectedProvider)}, got {DescribeValue(actualValue)}.";
    }

    private static bool TryConvertToBoolean(object? value, out bool converted)
    {
        switch (value)
        {
            case bool b:
                converted = b;
                return true;
            case byte i:
                converted = i != 0;
                return true;
            case short i:
                converted = i != 0;
                return true;
            case int i:
                converted = i != 0;
                return true;
            case long i:
                converted = i != 0;
                return true;
            case string s when bool.TryParse(s, out var parsed):
                converted = parsed;
                return true;
            default:
                converted = default;
                return false;
        }
    }

    private static bool TryConvertToInt64(object? value, out long converted)
    {
        switch (value)
        {
            case byte v:
                converted = v;
                return true;
            case sbyte v:
                converted = v;
                return true;
            case short v:
                converted = v;
                return true;
            case ushort v:
                converted = v;
                return true;
            case int v:
                converted = v;
                return true;
            case uint v:
                converted = v;
                return true;
            case long v:
                converted = v;
                return true;
            case ulong v when v <= long.MaxValue:
                converted = (long)v;
                return true;
            case DateTime v:
                converted = (v.ToUniversalTime().Ticks - DateTime.UnixEpoch.Ticks) / 10L;
                return true;
            case DateTimeOffset v:
                converted = (v.UtcTicks - DateTime.UnixEpoch.Ticks) / 10L;
                return true;
            case bool v:
                converted = v ? 1L : 0L;
                return true;
            case decimal v when decimal.Truncate(v) == v && v >= long.MinValue && v <= long.MaxValue:
                converted = (long)v;
                return true;
            case double v when !double.IsNaN(v) && !double.IsInfinity(v) && Math.Truncate(v) == v && v >= long.MinValue && v <= long.MaxValue:
                converted = (long)v;
                return true;
            case float v when !float.IsNaN(v) && !float.IsInfinity(v) && MathF.Truncate(v) == v && v >= long.MinValue && v <= long.MaxValue:
                converted = (long)v;
                return true;
            case string s when long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed):
                converted = parsed;
                return true;
            default:
                converted = default;
                return false;
        }
    }

    private static bool TryConvertToSingle(object? value, out float converted)
    {
        switch (value)
        {
            case float v:
                converted = v;
                return true;
            case double v:
                converted = (float)v;
                return true;
            case decimal v:
                converted = (float)v;
                return true;
            case string s when float.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed):
                converted = parsed;
                return true;
            default:
                converted = default;
                return false;
        }
    }

    private static bool TryConvertToDouble(object? value, out double converted)
    {
        switch (value)
        {
            case double v:
                converted = v;
                return true;
            case float v:
                converted = v;
                return true;
            case decimal v:
                converted = (double)v;
                return true;
            case string s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed):
                converted = parsed;
                return true;
            default:
                converted = default;
                return false;
        }
    }

    private static bool TryConvertToDecimal(object? value, out decimal converted)
    {
        switch (value)
        {
            case decimal v:
                converted = v;
                return true;
            case long v:
                converted = v;
                return true;
            case int v:
                converted = v;
                return true;
            case short v:
                converted = v;
                return true;
            case byte v:
                converted = v;
                return true;
            case double v when !double.IsNaN(v) && !double.IsInfinity(v):
                converted = (decimal)v;
                return true;
            case float v when !float.IsNaN(v) && !float.IsInfinity(v):
                converted = (decimal)v;
                return true;
            case string s when decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed):
                converted = parsed;
                return true;
            default:
                converted = default;
                return false;
        }
    }

    private static string DescribeValue(object? value)
        => value switch
        {
            null => "null",
            byte[] bytes => $"0x{Convert.ToHexString(bytes)} ({typeof(byte[]).Name})",
            string s => $"\"{s}\" ({typeof(string).Name})",
            _ => $"{value} ({value.GetType().Name})"
        };

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private readonly record struct RoundTripResult(bool Success, string Detail)
    {
        public static RoundTripResult Successful(string detail) => new(true, detail);
        public static RoundTripResult Failure(string detail) => new(false, detail);
    }

    private enum IntBackedLiteralEnum
    {
        Disabled = 0,
        Enabled = 1
    }

    private enum LongBackedLiteralEnum : long
    {
        Unknown = 0,
        Ready = 5_000_000_001L
    }

    private sealed class CoreLiteralContext(DbContextOptions<CoreLiteralContext> options) : DbContext(options);

    private sealed class NodaLiteralContext(DbContextOptions<NodaLiteralContext> options) : DbContext(options);
}
