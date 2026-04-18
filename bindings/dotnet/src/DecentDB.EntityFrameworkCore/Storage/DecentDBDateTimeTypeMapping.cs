using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="DateTime"/> to DecentDB <c>TIMESTAMP</c> as UTC microseconds
/// since Unix epoch and emits integer SQL literals in the same provider format.
/// </summary>
public sealed class DecentDBDateTimeTypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<DateTime, long> DateTimeMicrosConverter = new(
        value => (value.ToUniversalTime().Ticks - DateTime.UnixEpoch.Ticks) / 10L,
        value => new DateTime(value * 10L + DateTime.UnixEpoch.Ticks, DateTimeKind.Utc));

    public DecentDBDateTimeTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(DateTime), DateTimeMicrosConverter),
            storeType: "TIMESTAMP",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64))
    {
    }

    private DecentDBDateTimeTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBDateTimeTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateTimestampLiteral(ToProviderMicros(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateTimestampLiteral(ToProviderMicros(value));

    private static long ToProviderMicros(object value)
        => value switch
        {
            DateTime dateTime => (long)(DateTimeMicrosConverter.ConvertToProvider(dateTime) ?? 0L),
            long micros => micros,
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to DateTime literal.")
        };

    private static string GenerateTimestampLiteral(long micros)
    {
        var utcDateTime = new DateTime(micros * 10L + DateTime.UnixEpoch.Ticks, DateTimeKind.Utc);
        var isoLikeTimestamp = utcDateTime.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);

        return string.Format(
            CultureInfo.InvariantCulture,
            "CAST('{0}' AS TIMESTAMP)",
            isoLikeTimestamp);
    }
}
