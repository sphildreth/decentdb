using DecentDB.EntityFrameworkCore.Storage.Internal;
using DecentDB.EntityFrameworkCore.Metadata.Conventions.Internal;
using DecentDB.EntityFrameworkCore.Diagnostics;
using DecentDB.EntityFrameworkCore.Migrations.Internal;
using DecentDB.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
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
        builder.TryAdd<IProviderConventionSetBuilder, DecentDBConventionSetBuilder>();
        builder.TryAdd<IRelationalConnection, DecentDBRelationalConnection>();
        builder.TryAdd<IRelationalTransactionFactory, DecentDBRelationalTransactionFactory>();
        builder.TryAdd<IRelationalDatabaseCreator, DecentDBDatabaseCreator>();
        builder.TryAdd<IHistoryRepository, DecentDBHistoryRepository>();
        builder.TryAdd<IMigrationsSqlGenerator, DecentDBMigrationsSqlGenerator>();
        builder.TryAdd<ISqlGenerationHelper, DecentDBSqlGenerationHelper>();
        builder.TryAdd<IRelationalTypeMappingSource, DecentDBTypeMappingSource>();
        builder.TryAdd<IQuerySqlGeneratorFactory, DecentDBQuerySqlGeneratorFactory>();
        builder.TryAdd<IMethodCallTranslatorProvider, DecentDBMethodCallTranslatorProvider>();
        builder.TryAdd<IMemberTranslatorProvider, DecentDBMemberTranslatorProvider>();
        builder.TryAdd<IUpdateSqlGenerator, DecentDBUpdateSqlGenerator>();
        builder.TryAdd<IModificationCommandBatchFactory, DecentDBModificationCommandBatchFactory>();
        builder.TryAddCoreServices();

        return serviceCollection;
    }
}
