using System.Collections.Generic;
using System.Data.Common;
using DecentDB.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace DecentDB.EntityFrameworkCore;

public sealed class DecentDBOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public DecentDBOptionsExtension()
    {
    }

    private DecentDBOptionsExtension(DecentDBOptionsExtension copyFrom)
        : base(copyFrom)
    {
    }

    public override DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkDecentDB();

    public override void Validate(IDbContextOptions options)
    {
        if (Connection == null && string.IsNullOrWhiteSpace(ConnectionString))
        {
            throw new InvalidOperationException("UseDecentDB requires a non-empty connection string or an existing DbConnection.");
        }
    }

    public override DecentDBOptionsExtension WithConnectionString(string? connectionString)
        => (DecentDBOptionsExtension)base.WithConnectionString(connectionString);

    public override DecentDBOptionsExtension WithConnection(DbConnection? connection, bool owned)
        => (DecentDBOptionsExtension)base.WithConnection(connection, owned);

    protected override RelationalOptionsExtension Clone()
        => new DecentDBOptionsExtension(this);

    private sealed class ExtensionInfo : DbContextOptionsExtensionInfo
    {
        private string? _logFragment;

        public ExtensionInfo(IDbContextOptionsExtension extension)
            : base(extension)
        {
        }

        private new DecentDBOptionsExtension Extension
            => (DecentDBOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider
            => true;

        public override string LogFragment
            => _logFragment ??= string.IsNullOrWhiteSpace(Extension.ConnectionString)
                ? "using DecentDB "
                : $"using DecentDB Data Source={Extension.ConnectionString} ";

        public override int GetServiceProviderHashCode()
            => 0;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            => debugInfo["DecentDB:UseDecentDB"] = "1";

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo;
    }
}
