using Microsoft.EntityFrameworkCore.Update;

namespace DecentDB.EntityFrameworkCore.Update.Internal;

internal sealed class DecentDBModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    public ModificationCommandBatch Create()
        => new DecentDBModificationCommandBatch();
}
