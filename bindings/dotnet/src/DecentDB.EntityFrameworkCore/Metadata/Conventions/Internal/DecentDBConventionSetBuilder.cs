using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace DecentDB.EntityFrameworkCore.Metadata.Conventions.Internal;

public sealed class DecentDBConventionSetBuilder : RelationalConventionSetBuilder
{
    public DecentDBConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }
}
