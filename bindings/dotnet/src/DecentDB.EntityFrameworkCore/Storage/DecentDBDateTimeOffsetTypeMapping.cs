using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="DateTimeOffset"/> to DecentDB <c>TIMESTAMP</c> as UTC
/// microseconds since Unix epoch and emits integer SQL literals in the same
/// provider format.
/// </summary>
public sealed class DecentDBDateTimeOffsetTypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<DateTimeOffset, long> DateTimeOffsetMicrosConverter = new(
        value => (value.UtcTicks - DateTime.UnixEpoch.Ticks) / 10L,
        value => new DateTimeOffset(value * 10L + DateTime.UnixEpoch.Ticks, TimeSpan.Zero));

    public DecentDBDateTimeOffsetTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(DateTimeOffset), DateTimeOffsetMicrosConverter),
            storeType: "TIMESTAMP",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64))
    {
    }

    private DecentDBDateTimeOffsetTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBDateTimeOffsetTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateTimestampLiteral(ToProviderMicros(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateTimestampLiteral(ToProviderMicros(value));

    private static long ToProviderMicros(object value)
        => value switch
        {
            DateTimeOffset dateTimeOffset => (long)(DateTimeOffsetMicrosConverter.ConvertToProvider(dateTimeOffset) ?? 0L),
            long micros => micros,
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to DateTimeOffset literal.")
        };

    private static string GenerateTimestampLiteral(long micros)
    {
        var utcDateTime = new DateTimeOffset(micros * 10L + DateTime.UnixEpoch.Ticks, TimeSpan.Zero).UtcDateTime;
        var isoLikeTimestamp = utcDateTime.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);

        return string.Format(
            CultureInfo.InvariantCulture,
            "CAST('{0}' AS TIMESTAMP)",
            isoLikeTimestamp);
    }
}
