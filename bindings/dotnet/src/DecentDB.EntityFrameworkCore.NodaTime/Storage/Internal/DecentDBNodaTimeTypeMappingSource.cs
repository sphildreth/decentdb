using DecentDB.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage;
using NodaTime;
using System.Data;

namespace DecentDB.EntityFrameworkCore;

internal sealed class DecentDBNodaTimeTypeMappingSource : RelationalTypeMappingSource
{
    private readonly Dictionary<Type, RelationalTypeMapping> _clrMappings;
    private readonly Dictionary<string, RelationalTypeMapping> _storeMappings;

    public DecentDBNodaTimeTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
        var boolMapping = new DecentDBBoolTypeMapping();
        var byteMapping = new ByteTypeMapping("INTEGER", DbType.Byte);
        var shortMapping = new ShortTypeMapping("INTEGER", DbType.Int16);
        var intMapping = new IntTypeMapping("INTEGER", DbType.Int32);
        var longMapping = new LongTypeMapping("INTEGER", DbType.Int64);
        var floatMapping = new FloatTypeMapping("REAL", DbType.Single);
        var doubleMapping = new DoubleTypeMapping("REAL", DbType.Double);
        var decimalMapping = new DecentDBDecimalTypeMapping("DECIMAL(18,4)", DbType.Decimal, precision: 18, scale: 4);
        var stringMapping = new StringTypeMapping("TEXT", DbType.String);
        var blobMapping = new DecentDBByteArrayTypeMapping();
        var dateTimeMapping = new DecentDBDateTimeTypeMapping();
        var dateTimeOffsetMapping = new DecentDBDateTimeOffsetTypeMapping();
        var dateOnlyMapping = new DecentDBDateOnlyTypeMapping();
        var timeOnlyMapping = new DecentDBTimeOnlyTypeMapping();
        var timeSpanMapping = new DecentDBTimeSpanTypeMapping();

        var guidMapping = new DecentDBGuidTypeMapping();
        var instantMapping = new DecentDBInstantTypeMapping();
        var localDateMapping = new DecentDBLocalDateTypeMapping();
        var localDateTimeMapping = new DecentDBLocalDateTimeTypeMapping();

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
            [typeof(Guid)] = guidMapping,
            [typeof(Instant)] = instantMapping,
            [typeof(LocalDate)] = localDateMapping,
            [typeof(LocalDateTime)] = localDateTimeMapping
        };

        _storeMappings = new Dictionary<string, RelationalTypeMapping>(StringComparer.OrdinalIgnoreCase)
        {
            ["BOOL"] = boolMapping,
            ["BOOLEAN"] = boolMapping,
            ["INTEGER"] = longMapping,
            ["INT"] = longMapping,
            ["BIGINT"] = longMapping,
            ["REAL"] = doubleMapping,
            ["DOUBLE"] = doubleMapping,
            ["TEXT"] = stringMapping,
            ["BLOB"] = blobMapping,
            ["UUID"] = guidMapping,
            ["DECIMAL"] = decimalMapping,
            ["NUMERIC"] = decimalMapping,
            ["TIMESTAMP"] = dateTimeMapping,
            ["TIMESTAMPTZ"] = dateTimeMapping,
            ["DATE"] = dateTimeMapping,
            ["DATETIME"] = dateTimeMapping
        };
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = Nullable.GetUnderlyingType(mappingInfo.ClrType ?? typeof(object)) ?? mappingInfo.ClrType;
        if (clrType != null && _clrMappings.TryGetValue(clrType, out var clrMapping))
        {
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
                if (normalized is "DECIMAL" or "NUMERIC")
                {
                    return CreateDecimalMapping(mappingInfo, mappingInfo.StoreTypeName ?? storeType);
                }

                return storeMapping;
            }
        }

        return null;
    }

    private static DecentDBDecimalTypeMapping CreateDecimalMapping(
        in RelationalTypeMappingInfo mappingInfo,
        string? storeTypeName)
    {
        const int defaultPrecision = 18;
        const int defaultScale = 4;

        var precision = mappingInfo.Precision;
        var scale = mappingInfo.Scale;

        if (!precision.HasValue && !scale.HasValue && !string.IsNullOrWhiteSpace(storeTypeName))
        {
            (precision, scale) = ParsePrecisionScale(storeTypeName);
        }

        var p = precision ?? defaultPrecision;
        var s = scale ?? defaultScale;

        return new DecentDBDecimalTypeMapping($"DECIMAL({p},{s})", DbType.Decimal, precision: p, scale: s);
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
