using System;
using System.Globalization;

namespace DecentDB.AdoNet;

internal static class DecimalScaleNormalizer
{
    internal static decimal Normalize(decimal value, int scale)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(scale);

        if (scale > 28)
        {
            throw new ArgumentOutOfRangeException(nameof(scale), "Decimal scale must be between 0 and 28.");
        }

        var rounded = decimal.Round(value, scale, MidpointRounding.ToEven);
        var normalized = rounded.ToString($"F{scale}", CultureInfo.InvariantCulture);
        return decimal.Parse(normalized, NumberStyles.Number, CultureInfo.InvariantCulture);
    }
}
