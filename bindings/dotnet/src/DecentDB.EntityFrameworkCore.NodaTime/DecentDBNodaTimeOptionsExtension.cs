using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DecentDB.EntityFrameworkCore;

public sealed class DecentDBNodaTimeOptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
        => services.Replace(ServiceDescriptor.Singleton<IRelationalTypeMappingSource, DecentDBNodaTimeTypeMappingSource>());

    public void Validate(IDbContextOptions options)
    {
    }

    private sealed class ExtensionInfo : DbContextOptionsExtensionInfo
    {
        public ExtensionInfo(IDbContextOptionsExtension extension)
            : base(extension)
        {
        }

        public override bool IsDatabaseProvider
            => false;

        public override string LogFragment
            => "using NodaTime ";

        public override int GetServiceProviderHashCode()
            => 1;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            => debugInfo["DecentDB:NodaTime"] = "1";

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo;
    }
}
