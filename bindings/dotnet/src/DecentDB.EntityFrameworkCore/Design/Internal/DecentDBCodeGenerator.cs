using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Internal;

namespace DecentDB.EntityFrameworkCore.Design.Internal;

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
            "UseDecentDb",
            providerOptions is null
                ? new object[] { connectionString }
                : new object[] { connectionString, providerOptions });
}
