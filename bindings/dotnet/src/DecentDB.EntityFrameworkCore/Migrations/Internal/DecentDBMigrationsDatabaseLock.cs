using Microsoft.EntityFrameworkCore.Migrations;

namespace DecentDB.EntityFrameworkCore.Migrations.Internal;

internal sealed class DecentDBMigrationsDatabaseLock : IMigrationsDatabaseLock
{
    public DecentDBMigrationsDatabaseLock(IHistoryRepository historyRepository)
    {
        HistoryRepository = historyRepository;
    }

    public IHistoryRepository HistoryRepository { get; }

    public IMigrationsDatabaseLock ReacquireIfNeeded(bool connectionReopened, bool? transactionRestarted)
        => this;

    public Task<IMigrationsDatabaseLock> ReacquireIfNeededAsync(
        bool connectionReopened,
        bool? transactionRestarted,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IMigrationsDatabaseLock>(this);

    public void Dispose()
    {
    }

    public ValueTask DisposeAsync()
        => ValueTask.CompletedTask;
}
