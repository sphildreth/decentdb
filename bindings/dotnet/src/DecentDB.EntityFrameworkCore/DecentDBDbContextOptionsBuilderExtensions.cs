using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Microsoft.EntityFrameworkCore;

public static class DecentDBDbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseDecentDb(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString,
        Action<DecentDB.EntityFrameworkCore.DecentDBDbContextOptionsBuilder>? optionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);

        var extension = GetOrCreateExtension(optionsBuilder);
        extension = extension.WithConnectionString(connectionString);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        ConfigureWarnings(optionsBuilder);

        optionsAction?.Invoke(new DecentDB.EntityFrameworkCore.DecentDBDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseDecentDb(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        bool contextOwnsConnection = false,
        Action<DecentDB.EntityFrameworkCore.DecentDBDbContextOptionsBuilder>? optionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(connection);

        var extension = GetOrCreateExtension(optionsBuilder);
        extension = extension.WithConnection(connection, contextOwnsConnection);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        ConfigureWarnings(optionsBuilder);

        optionsAction?.Invoke(new DecentDB.EntityFrameworkCore.DecentDBDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder<TContext> UseDecentDb<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string connectionString,
        Action<DecentDB.EntityFrameworkCore.DecentDBDbContextOptionsBuilder>? optionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseDecentDb((DbContextOptionsBuilder)optionsBuilder, connectionString, optionsAction);

    public static DbContextOptionsBuilder<TContext> UseDecentDb<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        DbConnection connection,
        bool contextOwnsConnection = false,
        Action<DecentDB.EntityFrameworkCore.DecentDBDbContextOptionsBuilder>? optionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseDecentDb((DbContextOptionsBuilder)optionsBuilder, connection, contextOwnsConnection, optionsAction);

    private static DecentDB.EntityFrameworkCore.DecentDBOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.Options.FindExtension<DecentDB.EntityFrameworkCore.DecentDBOptionsExtension>()
            ?? new DecentDB.EntityFrameworkCore.DecentDBOptionsExtension();

    private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.ConfigureWarnings(w => w.Log(RelationalEventId.AmbientTransactionWarning));
}
