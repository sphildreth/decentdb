using DecentDB.EntityFrameworkCore.Storage.Internal;
using DecentDB.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using DecentDB.EntityFrameworkCore.Update.Internal;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;

namespace DecentDB.EntityFrameworkCore.Extensions;

public static class DecentDBServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkDecentDB(this IServiceCollection serviceCollection)
    {
        var builder = new EntityFrameworkRelationalServicesBuilder(serviceCollection);

        builder.TryAdd<LoggingDefinitions, DecentDBLoggingDefinitions>();
        builder.TryAdd<IDatabaseProvider, DatabaseProvider<DecentDBOptionsExtension>>();
        builder.TryAdd<IRelationalConnection, DecentDBRelationalConnection>();
        builder.TryAdd<IRelationalDatabaseCreator, DecentDBDatabaseCreator>();
        builder.TryAdd<ISqlGenerationHelper, DecentDBSqlGenerationHelper>();
        builder.TryAdd<IRelationalTypeMappingSource, DecentDBTypeMappingSource>();
        builder.TryAdd<IUpdateSqlGenerator, DecentDBUpdateSqlGenerator>();
        builder.TryAdd<IModificationCommandBatchFactory, DecentDBModificationCommandBatchFactory>();
        builder.TryAddCoreServices();

        return serviceCollection;
    }
}
