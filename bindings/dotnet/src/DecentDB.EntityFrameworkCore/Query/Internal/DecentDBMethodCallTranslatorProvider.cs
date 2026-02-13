using Microsoft.EntityFrameworkCore.Query;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public DecentDBMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        AddTranslators([new DecentDBStringMethodTranslator(dependencies.SqlExpressionFactory)]);
    }
}
