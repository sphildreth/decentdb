using Microsoft.EntityFrameworkCore.Infrastructure;

namespace DecentDB.EntityFrameworkCore;

public static class DecentDBNodaTimeDbContextOptionsBuilderExtensions
{
    public static DecentDBDbContextOptionsBuilder UseNodaTime(this DecentDBDbContextOptionsBuilder optionsBuilder)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);

        var extension = optionsBuilder.ContextOptionsBuilder.Options.FindExtension<DecentDBNodaTimeOptionsExtension>()
            ?? new DecentDBNodaTimeOptionsExtension();

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder.ContextOptionsBuilder).AddOrUpdateExtension(extension);
        return optionsBuilder;
    }
}
