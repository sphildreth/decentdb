using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace DecentDB.EntityFrameworkCore.Update.Internal;

internal sealed class DecentDBModificationCommandBatch : ModificationCommandBatch
{
    private readonly List<IReadOnlyModificationCommand> _commands = new();
    private bool _moreBatchesExpected;

    public override IReadOnlyList<IReadOnlyModificationCommand> ModificationCommands
        => _commands;

    public override bool RequiresTransaction
        => false;

    public override bool AreMoreBatchesExpected
        => _moreBatchesExpected;

    public override bool TryAddCommand(IReadOnlyModificationCommand modificationCommand)
    {
        _commands.Add(modificationCommand);
        return true;
    }

    public override void Complete(bool moreBatchesExpected)
        => _moreBatchesExpected = moreBatchesExpected;

    public override void Execute(IRelationalConnection connection)
        => throw new NotSupportedException("SaveChanges SQL batching is not implemented yet for DecentDB EF Core provider.");

    public override Task ExecuteAsync(IRelationalConnection connection, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("SaveChanges SQL batching is not implemented yet for DecentDB EF Core provider.");
}
