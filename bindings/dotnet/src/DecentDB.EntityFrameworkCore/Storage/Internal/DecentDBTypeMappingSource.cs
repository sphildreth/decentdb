using System.Data;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage.Internal;

internal sealed class DecentDBTypeMappingSource : RelationalTypeMappingSource
{
    private static readonly DateOnly EpochDate = DateOnly.FromDateTime(DateTime.UnixEpoch);

    private readonly Dictionary<Type, RelationalTypeMapping> _clrMappings;
    private readonly Dictionary<string, RelationalTypeMapping> _storeMappings;

    public DecentDBTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
        var boolMapping = new BoolTypeMapping("BOOLEAN", DbType.Boolean);
        var byteMapping = new ByteTypeMapping("INTEGER", DbType.Byte);
        var shortMapping = new ShortTypeMapping("INTEGER", DbType.Int16);
        var intMapping = new IntTypeMapping("INTEGER", DbType.Int32);
        var longMapping = new LongTypeMapping("INTEGER", DbType.Int64);
        var floatMapping = new FloatTypeMapping("REAL", DbType.Single);
        var doubleMapping = new DoubleTypeMapping("REAL", DbType.Double);
        var decimalMapping = new DecimalTypeMapping("DECIMAL(18,4)", DbType.Decimal, precision: 18, scale: 4);
        var stringMapping = new StringTypeMapping("TEXT", DbType.String);
        var blobMapping = new ByteArrayTypeMapping("BLOB", DbType.Binary);

        var dateTimeMapping = (RelationalTypeMapping)longMapping.WithComposedConverter(
            new ValueConverter<DateTime, long>(
                value => new DateTimeOffset(value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime(), TimeSpan.Zero).ToUnixTimeMilliseconds(),
                value => DateTimeOffset.FromUnixTimeMilliseconds(value).UtcDateTime),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);

        var dateTimeOffsetMapping = (RelationalTypeMapping)longMapping.WithComposedConverter(
            new ValueConverter<DateTimeOffset, long>(
                value => value.ToUniversalTime().ToUnixTimeMilliseconds(),
                value => DateTimeOffset.FromUnixTimeMilliseconds(value)),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);

        var dateOnlyMapping = (RelationalTypeMapping)longMapping.WithComposedConverter(
            new ValueConverter<DateOnly, long>(
                value => value.DayNumber - EpochDate.DayNumber,
                value => EpochDate.AddDays(checked((int)value))),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);

        var timeOnlyMapping = (RelationalTypeMapping)longMapping.WithComposedConverter(
            new ValueConverter<TimeOnly, long>(
                value => value.Ticks,
                value => new TimeOnly(value)),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);

        var timeSpanMapping = (RelationalTypeMapping)longMapping.WithComposedConverter(
            new ValueConverter<TimeSpan, long>(
                value => value.Ticks,
                value => TimeSpan.FromTicks(value)),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);

        var guidMapping = (RelationalTypeMapping)new ByteArrayTypeMapping("UUID", DbType.Binary, size: 16).WithComposedConverter(
            new ValueConverter<Guid, byte[]>(
                value => value.ToByteArray(),
                value => new Guid(value)),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);

        _clrMappings = new Dictionary<Type, RelationalTypeMapping>
        {
            [typeof(bool)] = boolMapping,
            [typeof(byte)] = byteMapping,
            [typeof(short)] = shortMapping,
            [typeof(int)] = intMapping,
            [typeof(long)] = longMapping,
            [typeof(float)] = floatMapping,
            [typeof(double)] = doubleMapping,
            [typeof(decimal)] = decimalMapping,
            [typeof(string)] = stringMapping,
            [typeof(byte[])] = blobMapping,
            [typeof(DateTime)] = dateTimeMapping,
            [typeof(DateTimeOffset)] = dateTimeOffsetMapping,
            [typeof(DateOnly)] = dateOnlyMapping,
            [typeof(TimeOnly)] = timeOnlyMapping,
            [typeof(TimeSpan)] = timeSpanMapping,
            [typeof(Guid)] = guidMapping
        };

        _storeMappings = new Dictionary<string, RelationalTypeMapping>(StringComparer.OrdinalIgnoreCase)
        {
            ["BOOL"] = boolMapping,
            ["BOOLEAN"] = boolMapping,
            ["INTEGER"] = longMapping,
            ["INT"] = longMapping,
            ["INT64"] = longMapping,
            ["INT32"] = longMapping,
            ["INT16"] = longMapping,
            ["INT8"] = longMapping,
            ["BIGINT"] = longMapping,
            ["REAL"] = doubleMapping,
            ["DOUBLE"] = doubleMapping,
            ["FLOAT64"] = doubleMapping,
            ["FLOAT32"] = doubleMapping,
            ["TEXT"] = stringMapping,
            ["BLOB"] = blobMapping,
            ["UUID"] = guidMapping,
            ["DECIMAL"] = decimalMapping,
            ["NUMERIC"] = decimalMapping
        };
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = Nullable.GetUnderlyingType(mappingInfo.ClrType ?? typeof(object)) ?? mappingInfo.ClrType;
        if (clrType != null && _clrMappings.TryGetValue(clrType, out var clrMapping))
        {
            // For decimal types, respect precision/scale from EF Core model configuration
            // (e.g. HavePrecision, HasPrecision, or HasColumnType with precision)
            if (clrType == typeof(decimal))
            {
                return CreateDecimalMapping(mappingInfo, mappingInfo.StoreTypeName);
            }

            return clrMapping;
        }

        var storeType = mappingInfo.StoreTypeNameBase ?? mappingInfo.StoreTypeName;
        if (!string.IsNullOrWhiteSpace(storeType))
        {
            var normalized = NormalizeStoreTypeName(storeType);
            if (_storeMappings.TryGetValue(normalized, out var storeMapping))
            {
                // For DECIMAL/NUMERIC store types, respect precision/scale from store type name or mappingInfo
                if (normalized is "DECIMAL" or "NUMERIC")
                {
                    return CreateDecimalMapping(mappingInfo, mappingInfo.StoreTypeName ?? storeType);
                }

                return storeMapping;
            }
        }

        return null;
    }

    private static DecimalTypeMapping CreateDecimalMapping(
        in RelationalTypeMappingInfo mappingInfo,
        string? storeTypeName)
    {
        const int defaultPrecision = 18;
        const int defaultScale = 4;

        var precision = mappingInfo.Precision;
        var scale = mappingInfo.Scale;

        // If precision/scale not provided by EF Core model, try parsing from store type name
        if (!precision.HasValue && !scale.HasValue && !string.IsNullOrWhiteSpace(storeTypeName))
        {
            (precision, scale) = ParsePrecisionScale(storeTypeName);
        }

        var p = precision ?? defaultPrecision;
        var s = scale ?? defaultScale;

        return new DecimalTypeMapping($"DECIMAL({p},{s})", DbType.Decimal, precision: p, scale: s);
    }

    private static (int? precision, int? scale) ParsePrecisionScale(string storeTypeName)
    {
        var openParen = storeTypeName.IndexOf('(');
        var closeParen = storeTypeName.IndexOf(')');
        if (openParen < 0 || closeParen <= openParen)
        {
            return (null, null);
        }

        var inner = storeTypeName.AsSpan()[(openParen + 1)..closeParen];
        var commaIdx = inner.IndexOf(',');

        if (commaIdx >= 0
            && int.TryParse(inner[..commaIdx].Trim(), out var p)
            && int.TryParse(inner[(commaIdx + 1)..].Trim(), out var s))
        {
            return (p, s);
        }

        if (int.TryParse(inner.Trim(), out var pOnly))
        {
            return (pOnly, null);
        }

        return (null, null);
    }

    private static string NormalizeStoreTypeName(string storeTypeName)
    {
        var idx = storeTypeName.IndexOf('(');
        return (idx >= 0 ? storeTypeName[..idx] : storeTypeName).Trim();
    }
}
