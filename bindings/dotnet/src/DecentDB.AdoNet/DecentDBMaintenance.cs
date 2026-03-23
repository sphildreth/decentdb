using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DecentDB.AdoNet
{
    /// <summary>
    /// Provides maintenance utilities for DecentDB database files.
    /// </summary>
    public static class DecentDBMaintenance
    {
        /// <summary>
        /// Spawns the DecentDB CLI to perform an offline vacuum.
        /// Performs an atomic swap of the database file if successful.
        /// Ensure no connections are open to the database file before running.
        /// </summary>
        /// <param name="databasePath">The path to the DecentDB database file.</param>
        /// <param name="cliExecutablePath">The path to the DecentDB executable. Defaults to "decentdb" assuming it is in the system PATH.</param>
        /// <param name="createBackup">If true, renames the original database file with a .bak extension instead of deleting it.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        /// <returns>True if vacuum was successful, false if the database file didn't exist.</returns>
        public static async Task<bool> VacuumAtomicAsync(
            string databasePath,
            string cliExecutablePath = "decentdb",
            bool createBackup = false,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(databasePath))
                throw new ArgumentException("Database path cannot be null or empty.", nameof(databasePath));

            if (!File.Exists(databasePath))
                return false;

            var dbFileInfo = new FileInfo(databasePath);
            var tempPath = dbFileInfo.FullName + ".vacuum_tmp";
            var backupPath = dbFileInfo.FullName + ".bak";

            // Ensure previous interrupted temp files are removed
            if (File.Exists(tempPath))
                File.Delete(tempPath);

            var startInfo = new ProcessStartInfo
            {
                FileName = cliExecutablePath,
                Arguments = $"vacuum --db \"{dbFileInfo.FullName}\" --output \"{tempPath}\" --overwrite",
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            using var process = new Process { StartInfo = startInfo };
            
            try
            {
                process.Start();
                await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);

                if (process.ExitCode != 0)
                {
                    var error = await process.StandardError.ReadToEndAsync().ConfigureAwait(false);
                    var stdout = await process.StandardOutput.ReadToEndAsync().ConfigureAwait(false);
                    if (File.Exists(tempPath))
                        File.Delete(tempPath);
                    throw new InvalidOperationException($"Vacuum failed with exit code {process.ExitCode}. Error: {error} | Stdout: {stdout}");
                }

                // Atomic swap
                if (createBackup)
                {
                    if (File.Exists(backupPath))
                        File.Delete(backupPath);
                    File.Move(dbFileInfo.FullName, backupPath);
                }
                else
                {
                    File.Delete(dbFileInfo.FullName);
                }

                File.Move(tempPath, dbFileInfo.FullName);
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException && ex is not InvalidOperationException)
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
                throw new InvalidOperationException($"An error occurred during vacuum: {ex.Message}", ex);
            }
        }
    }
}
