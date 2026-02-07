using System;

namespace DecentDB.MicroOrm;

internal static class DefaultTypeConverters
{
    public static object? ToDbValue(object? value)
    {
        if (value == null || value == DBNull.Value) return DBNull.Value;

        // Normalize common CLR types to the provider/engine-friendly representations.
        // (These match the mapping in design/DAPPER_SUPPORT.md.)
        return value switch
        {
            DateTime dt => new DateTimeOffset((dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime()), TimeSpan.Zero).ToUnixTimeMilliseconds(),
            DateTimeOffset dto => dto.ToUniversalTime().ToUnixTimeMilliseconds(),
            DateOnly d => d.DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber, // days since epoch
            TimeOnly t => t.Ticks, // ticks since midnight
            TimeSpan ts => ts.Ticks, // ticks
            Guid g => g.ToByteArray(),
            decimal dec => dec.ToString(),
            char ch => ch.ToString(),
            Enum e => Convert.ToInt64(e),
            _ => value
        };
    }
}
