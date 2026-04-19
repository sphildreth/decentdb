using System.Collections.Concurrent;
using System.Data;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage.Internal;

internal sealed class DecentDBTypeMappingSource : RelationalTypeMappingSource
{
    private const string ULongOverflowError = "UInt64 value exceeds DecentDB INT64 range.";

    private readonly LongTypeMapping _longMapping;
    private readonly ConcurrentDictionary<Type, RelationalTypeMapping> _enumMappings = new();
    private readonly Dictionary<Type, RelationalTypeMapping> _clrMappings;
    private readonly Dictionary<string, RelationalTypeMapping> _storeMappings;

    public DecentDBTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
        var boolMapping = new DecentDBBoolTypeMapping();
        var byteMapping = new ByteTypeMapping("INTEGER", DbType.Byte);
        var shortMapping = new ShortTypeMapping("INTEGER", DbType.Int16);
        var intMapping = new IntTypeMapping("INTEGER", DbType.Int32);
        var longMapping = new LongTypeMapping("INTEGER", DbType.Int64);
        _longMapping = longMapping;
        var sbyteMapping = (RelationalTypeMapping)longMapping.WithComposedConverter(
            new ValueConverter<sbyte, long>(
                value => value,
                value => checked((sbyte)value)),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);
        var ushortMapping = (RelationalTypeMapping)longMapping.WithComposedConverter(
            new ValueConverter<ushort, long>(
                value => value,
                value => checked((ushort)value)),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);
        var uintMapping = (RelationalTypeMapping)longMapping.WithComposedConverter(
            new ValueConverter<uint, long>(
                value => value,
                value => checked((uint)value)),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);
        var ulongMapping = (RelationalTypeMapping)longMapping.WithComposedConverter(
            new ValueConverter<ulong, long>(
                value => ConvertULongToLong(value),
                value => checked((ulong)value)),
            comparer: null,
            keyComparer: null,
            elementMapping: null,
            jsonValueReaderWriter: null);
        var floatMapping = new FloatTypeMapping("REAL", DbType.Single);
        var doubleMapping = new DoubleTypeMapping("REAL", DbType.Double);
        var decimalMapping = new DecentDBDecimalTypeMapping("DECIMAL(18,4)", DbType.Decimal, precision: 18, scale: 4);
        var stringMapping = new StringTypeMapping("TEXT", DbType.String);
        var charMapping = new DecentDBCharTypeMapping();
        var blobMapping = new DecentDBByteArrayTypeMapping();
        var dateTimeMapping = new DecentDBDateTimeTypeMapping();
        var dateTimeOffsetMapping = new DecentDBDateTimeOffsetTypeMapping();
        var dateOnlyMapping = new DecentDBDateOnlyTypeMapping();
        var timeOnlyMapping = new DecentDBTimeOnlyTypeMapping();
        var timeSpanMapping = new DecentDBTimeSpanTypeMapping();

        var guidMapping = new DecentDBGuidTypeMapping();

        _clrMappings = new Dictionary<Type, RelationalTypeMapping>
        {
            [typeof(bool)] = boolMapping,
            [typeof(byte)] = byteMapping,
            [typeof(sbyte)] = sbyteMapping,
            [typeof(short)] = shortMapping,
            [typeof(int)] = intMapping,
            [typeof(long)] = longMapping,
            [typeof(ushort)] = ushortMapping,
            [typeof(uint)] = uintMapping,
            [typeof(ulong)] = ulongMapping,
            [typeof(float)] = floatMapping,
            [typeof(double)] = doubleMapping,
            [typeof(decimal)] = decimalMapping,
            [typeof(string)] = stringMapping,
            [typeof(char)] = charMapping,
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
            // For decimal types, respect precision/scale from EF Core model configuration
            // (e.g. HavePrecision, HasPrecision, or HasColumnType with precision)
            if (clrType == typeof(decimal))
            {
                return CreateDecimalMapping(mappingInfo, mappingInfo.StoreTypeName);
            }

            return clrMapping;
        }

        if (clrType != null && clrType.IsEnum)
        {
            return FindOrCreateEnumMapping(clrType);
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

    private static DecentDBDecimalTypeMapping CreateDecimalMapping(
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

    private RelationalTypeMapping FindOrCreateEnumMapping(Type enumType)
    {
        return _enumMappings.GetOrAdd(enumType, static (t, longMapping) =>
        {
            var converterType = typeof(EnumToNumberConverter<,>).MakeGenericType(t, typeof(long));
            var converter = (ValueConverter)Activator.CreateInstance(converterType)!;
            return (RelationalTypeMapping)longMapping.WithComposedConverter(
                converter,
                comparer: null,
                keyComparer: null,
                elementMapping: null,
                jsonValueReaderWriter: null);
        }, _longMapping);
    }

    private static long ConvertULongToLong(ulong value)
    {
        if (value > long.MaxValue)
        {
            throw new OverflowException($"{ULongOverflowError} Value: {value}.");
        }

        return (long)value;
    }
}
