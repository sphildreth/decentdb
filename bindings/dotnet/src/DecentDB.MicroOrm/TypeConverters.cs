using System;

namespace DecentDB.MicroOrm;

internal static class DefaultTypeConverters
{
    public static object? ToDbValue(object? value)
    {
        if (value == null || value == DBNull.Value) return DBNull.Value;

        return value switch
        {
            DateTime dt => new DateTimeOffset((dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime()), TimeSpan.Zero).ToUnixTimeMilliseconds() * 1000L,
            DateTimeOffset dto => dto.ToUniversalTime().ToUnixTimeMilliseconds() * 1000L,
            DateOnly d => (d.DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber) * 86_400_000_000L,
            TimeOnly t => t.Ticks / 10L,
            TimeSpan ts => ts.Ticks / 10L,
            Guid g => g.ToByteArray(),
            decimal dec => dec,
            char ch => ch.ToString(),
            Enum e => Convert.ToInt64(e),
            _ => value
        };
    }
}
