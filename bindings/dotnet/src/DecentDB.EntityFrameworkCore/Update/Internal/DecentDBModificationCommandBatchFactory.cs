using Microsoft.EntityFrameworkCore.Update;

namespace DecentDB.EntityFrameworkCore.Update.Internal;

internal sealed class DecentDBModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;

    public DecentDBModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public ModificationCommandBatch Create()
        => new DecentDBModificationCommandBatch(_dependencies);
}
