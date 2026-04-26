using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace DecentDB.EntityFrameworkCore;

public sealed class DecentDBDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<DecentDBDbContextOptionsBuilder, DecentDBOptionsExtension>
{
    public DecentDBDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : base(optionsBuilder)
    {
    }

    public DbContextOptionsBuilder ContextOptionsBuilder
        => OptionsBuilder;

    /// <summary>
    /// Disables the correlated aggregate rewrite (N14) that transforms
    /// correlated COUNT subqueries into LEFT JOIN ... GROUP BY.
    /// </summary>
    public DecentDBDbContextOptionsBuilder DisableCorrelatedAggregateRewrite()
    {
        var extension = OptionsBuilder.Options.FindExtension<DecentDBOptionsExtension>()
            ?? new DecentDBOptionsExtension();
        ((IDbContextOptionsBuilderInfrastructure)OptionsBuilder).AddOrUpdateExtension(
            extension.WithCorrelatedAggregateRewrite(false));
        return this;
    }
}
