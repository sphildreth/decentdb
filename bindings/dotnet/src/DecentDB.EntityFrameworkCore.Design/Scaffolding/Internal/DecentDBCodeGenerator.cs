using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;

namespace DecentDB.EntityFrameworkCore.Design.Scaffolding.Internal;

public sealed class DecentDBCodeGenerator : ProviderCodeGenerator
{
    public DecentDBCodeGenerator(ProviderCodeGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    public override MethodCallCodeFragment GenerateUseProvider(
        string connectionString,
        MethodCallCodeFragment? providerOptions)
        => new(
            "UseDecentDB",
            providerOptions is null
                ? new object[] { connectionString }
                : new object[] { connectionString, providerOptions });
}
