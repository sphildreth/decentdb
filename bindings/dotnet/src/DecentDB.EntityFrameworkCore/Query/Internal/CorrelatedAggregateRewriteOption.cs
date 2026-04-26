namespace DecentDB.EntityFrameworkCore.Query.Internal;

/// <summary>
/// Singleton option carrying the correlated aggregate rewrite flag.
/// </summary>
public sealed class CorrelatedAggregateRewriteOption
{
    public bool Disabled { get; }
    public CorrelatedAggregateRewriteOption(bool disabled) => Disabled = disabled;
}
