using System;

namespace DecentDb.AdoNet;

public static class DecentDbConnectionExtensions
{
    public static void Checkpoint(this DecentDbConnection connection)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        connection.Checkpoint();
    }
}
