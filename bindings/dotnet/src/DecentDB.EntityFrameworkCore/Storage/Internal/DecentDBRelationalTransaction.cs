using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Storage.Internal;

internal sealed class DecentDBRelationalTransaction : RelationalTransaction
{
    public DecentDBRelationalTransaction(
        IRelationalConnection connection,
        DbTransaction transaction,
        Guid transactionId,
        IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
        bool transactionOwned,
        ISqlGenerationHelper sqlGenerationHelper)
        : base(connection, transaction, transactionId, logger, transactionOwned, sqlGenerationHelper)
    {
    }

    public override bool SupportsSavepoints => false;

    public override void CreateSavepoint(string name)
        => throw SavepointsNotSupported();

    public override Task CreateSavepointAsync(string name, CancellationToken cancellationToken = default)
        => Task.FromException(SavepointsNotSupported());

    public override void RollbackToSavepoint(string name)
        => throw SavepointsNotSupported();

    public override Task RollbackToSavepointAsync(string name, CancellationToken cancellationToken = default)
        => Task.FromException(SavepointsNotSupported());

    public override void ReleaseSavepoint(string name)
        => throw SavepointsNotSupported();

    public override Task ReleaseSavepointAsync(string name, CancellationToken cancellationToken = default)
        => Task.FromException(SavepointsNotSupported());

    private static NotSupportedException SavepointsNotSupported()
        => new("DecentDB transactions do not support savepoints.");
}
