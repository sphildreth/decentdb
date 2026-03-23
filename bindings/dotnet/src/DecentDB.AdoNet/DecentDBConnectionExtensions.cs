using System;

namespace DecentDB.AdoNet;

public static class DecentDBConnectionExtensions
{
    public static void Checkpoint(this DecentDBConnection connection)
    {
        if (connection == null) throw new ArgumentNullException(nameof(connection));
        connection.Checkpoint();
    }
}
