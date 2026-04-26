using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBEvaluatableExpressionFilter : RelationalEvaluatableExpressionFilter
{
    public DecentDBEvaluatableExpressionFilter(
        EvaluatableExpressionFilterDependencies dependencies,
        RelationalEvaluatableExpressionFilterDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override bool IsEvaluatableExpression(Expression expression, IModel model)
    {
        if (expression is MethodCallExpression mc && mc.Object is not null && mc.Object.Type == typeof(System.Random))
        {
            return true;
        }

        return base.IsEvaluatableExpression(expression, model);
    }
}
