using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Storage.Internal;

internal sealed class DecentDBSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public DecentDBSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }

    public override string EscapeIdentifier(string identifier)
        => identifier.Replace("\"", "\"\"");

    public override string DelimitIdentifier(string identifier)
        => $"\"{EscapeIdentifier(identifier)}\"";

    public override string DelimitIdentifier(string name, string? schema)
        => string.IsNullOrEmpty(schema)
            ? DelimitIdentifier(name)
            : $"{DelimitIdentifier(schema)}.{DelimitIdentifier(name)}";
}
