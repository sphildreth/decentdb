using System;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("DecentDB.MicroOrm")]
[assembly: InternalsVisibleTo("DecentDB.EntityFrameworkCore")]

namespace DecentDB.AdoNet.Internal;

internal static class ConnectionStringHelper
{
    /// <summary>
    /// If <paramref name="input"/> looks like a path (contains no '='),
    /// returns <c>"Data Source=&lt;input&gt;"</c>. Otherwise returns
    /// the input unchanged.
    /// </summary>
    public static string NormalizeToConnectionString(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            throw new ArgumentException(
                "Connection string or data source path must be provided.",
                nameof(input));
        return input.Contains('=')
            ? input
            : "Data Source=" + input;
    }
}
