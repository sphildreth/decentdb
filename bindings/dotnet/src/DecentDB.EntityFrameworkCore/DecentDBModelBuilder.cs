using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace DecentDB.EntityFrameworkCore;

/// <summary>
/// Helper for pre-building and caching EF Core models to avoid the first-context startup cost.
/// </summary>
public static class DecentDBModelBuilder
{
    private static readonly ConcurrentDictionary<Type, IModel> Cache = new();

    /// <summary>
    /// Builds the <see cref="IModel"/> for <typeparamref name="TContext"/> and caches it.
    /// The first call constructs the model (expensive); subsequent calls return the cached instance.
    /// </summary>
    /// <typeparam name="TContext">A <see cref="DbContext"/> type.</typeparam>
    /// <returns>The cached or newly built model.</returns>
    public static IModel BuildModel<TContext>() where TContext : DbContext, new()
    {
        return Cache.GetOrAdd(typeof(TContext), _ =>
        {
            using var ctx = new TContext();
            return ctx.Model;
        });
    }
}
