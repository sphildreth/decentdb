using System;
using System.Data;
using System.Data.Common;

namespace DecentDB.AdoNet
{
    public sealed class DecentDBTransaction : DbTransaction
    {
        private readonly DecentDBConnection _connection;
        private readonly IsolationLevel _isolationLevel;
        private bool _disposed;
        private bool _completed;

        public DecentDBTransaction(DecentDBConnection connection, IsolationLevel isolationLevel)
        {
            _connection = connection;
            _isolationLevel = isolationLevel;
        }

        protected override DbConnection DbConnection => _connection;

        public override IsolationLevel IsolationLevel => _isolationLevel;

        public override void Commit()
        {
            if (_completed)
            {
                throw new InvalidOperationException("Transaction already completed");
            }

            if (_connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is not open");
            }

            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "COMMIT";
            cmd.ExecuteNonQuery();

            _completed = true;
        }

        public override void Rollback()
        {
            if (_completed)
            {
                throw new InvalidOperationException("Transaction already completed");
            }

            if (_connection.State != ConnectionState.Open)
            {
                return;
            }

            try
            {
                using var cmd = _connection.CreateCommand();
                cmd.CommandText = "ROLLBACK";
                cmd.ExecuteNonQuery();
            }
            catch
            {
            }

            _completed = true;
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing && !_completed)
            {
                try
                {
                    Rollback();
                }
                catch
                {
                }
            }

            _disposed = true;
            base.Dispose(disposing);
        }
    }
}
