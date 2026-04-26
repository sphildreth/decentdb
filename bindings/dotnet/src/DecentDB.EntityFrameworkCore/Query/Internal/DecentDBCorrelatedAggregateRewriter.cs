using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

/// <summary>
/// N14: Query translation post-processor for DecentDB.
/// Currently serves as an extension point for future correlated aggregate rewriting.
/// </summary>
public sealed class DecentDBCorrelatedAggregateRewriter : RelationalQueryTranslationPostprocessor
{
    private readonly bool _disableRewrite;

    public DecentDBCorrelatedAggregateRewriter(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext,
        bool disableRewrite)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _disableRewrite = disableRewrite;
    }

    public override Expression Process(Expression query)
    {
        if (_disableRewrite)
            return base.Process(query);

        // Future: walk the expression tree and rewrite correlated COUNT subqueries
        // into LEFT JOIN ... GROUP BY for better DecentDB performance.
        return base.Process(query);
    }
}
