using System;
using System.Globalization;
using System.Text;

namespace DecentDb.MicroOrm;

internal static class Conventions
{
    public static string DefaultTableName(Type entityType)
    {
        var name = entityType.Name;
        return Pluralize(ToSnakeCase(name));
    }

    public static string DefaultColumnName(string propertyName)
    {
        return ToSnakeCase(propertyName);
    }

    public static string ToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return string.Empty;

        var sb = new StringBuilder(name.Length + 8);

        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                var hasPrev = i > 0;
                var prev = hasPrev ? name[i - 1] : '\0';
                var hasNext = i + 1 < name.Length;
                var next = hasNext ? name[i + 1] : '\0';

                var boundary = hasPrev && (char.IsLower(prev) || char.IsDigit(prev) || (hasNext && char.IsLower(next)));
                if (boundary)
                {
                    sb.Append('_');
                }

                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(char.ToLowerInvariant(c));
            }
        }

        return sb.ToString();
    }

    public static string Pluralize(string lowerName)
    {
        if (string.IsNullOrEmpty(lowerName)) return lowerName;

        // Small, dependency-free pluralizer. Good enough for conventions.
        if (lowerName.EndsWith("y", StringComparison.Ordinal) && lowerName.Length > 1)
        {
            var prev = lowerName[^2];
            if (!IsVowel(prev))
            {
                return lowerName[..^1] + "ies";
            }
        }

        if (lowerName.EndsWith("s", StringComparison.Ordinal) ||
            lowerName.EndsWith("x", StringComparison.Ordinal) ||
            lowerName.EndsWith("z", StringComparison.Ordinal) ||
            lowerName.EndsWith("ch", StringComparison.Ordinal) ||
            lowerName.EndsWith("sh", StringComparison.Ordinal))
        {
            return lowerName + "es";
        }

        return lowerName + "s";
    }

    private static bool IsVowel(char c)
    {
        c = char.ToLowerInvariant(c);
        return c is 'a' or 'e' or 'i' or 'o' or 'u';
    }
}
