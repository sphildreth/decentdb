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
}
