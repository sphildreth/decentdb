using Microsoft.EntityFrameworkCore.Query;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

/// <summary>
/// Factory for creating <see cref="DecentDBCorrelatedAggregateRewriter"/> instances.
/// </summary>
public sealed class DecentDBQueryTranslationPostprocessorFactory : IQueryTranslationPostprocessorFactory
{
    private readonly QueryTranslationPostprocessorDependencies _dependencies;
    private readonly RelationalQueryTranslationPostprocessorDependencies _relationalDependencies;
    private readonly bool _disableCorrelatedAggregateRewrite;

    public DecentDBQueryTranslationPostprocessorFactory(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        CorrelatedAggregateRewriteOption rewriteOption)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
        _disableCorrelatedAggregateRewrite = rewriteOption.Disabled;
    }

    public QueryTranslationPostprocessor Create(QueryCompilationContext queryCompilationContext)
    {
        return new DecentDBCorrelatedAggregateRewriter(
            _dependencies,
            _relationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext,
            _disableCorrelatedAggregateRewrite);
    }
}
