using Microsoft.EntityFrameworkCore.Query;

namespace DecentDB.EntityFrameworkCore.NodaTime.Query.Internal;

public sealed class DecentDBNodaTimeMemberTranslatorPlugin : IMemberTranslatorPlugin
{
    public DecentDBNodaTimeMemberTranslatorPlugin(ISqlExpressionFactory sqlExpressionFactory)
    {
        Translators = [new DecentDBNodaTimeMemberTranslator(sqlExpressionFactory)];
    }

    public IEnumerable<IMemberTranslator> Translators { get; }
}
