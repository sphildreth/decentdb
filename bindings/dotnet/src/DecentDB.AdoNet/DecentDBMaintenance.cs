using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
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
        /// Returns file-size diagnostics for a DecentDB database and known WAL sidecars.
        /// </summary>
        /// <param name="databasePath">The path to the DecentDB database file.</param>
        public static DecentDBWalStatus GetWalStatus(string databasePath)
        {
            var fullPath = NormalizeDatabasePath(databasePath);
            return new DecentDBWalStatus(
                fullPath,
                FileLengthOrZero(fullPath),
                fullPath + "-wal",
                FileLengthOrZero(fullPath + "-wal"),
                fullPath + ".wal",
                FileLengthOrZero(fullPath + ".wal"),
                fullPath + ".coord",
                FileLengthOrZero(fullPath + ".coord"));
        }

        /// <summary>
        /// Opens the database and performs a DecentDB checkpoint without invoking the CLI.
        /// </summary>
        /// <param name="databasePath">The path to the DecentDB database file.</param>
        /// <param name="cancellationToken">A token to cancel before the operation starts.</param>
        public static Task<DecentDBCheckpointResult> CheckpointAsync(
            string databasePath,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fullPath = NormalizeDatabasePath(databasePath);
            var before = GetWalStatus(fullPath);
            if (!File.Exists(fullPath))
            {
                return Task.FromResult(new DecentDBCheckpointResult(
                    fullPath,
                    databaseExisted: false,
                    before,
                    before,
                    TimeSpan.Zero));
            }

            var stopwatch = Stopwatch.StartNew();
            using (var connection = OpenConnection(fullPath))
            {
                connection.Checkpoint();
            }

            stopwatch.Stop();
            var after = GetWalStatus(fullPath);
            return Task.FromResult(new DecentDBCheckpointResult(
                fullPath,
                databaseExisted: true,
                before,
                after,
                stopwatch.Elapsed));
        }

        /// <summary>
        /// Opens the database and rebuilds one index without invoking the CLI.
        /// </summary>
        /// <param name="databasePath">The path to the DecentDB database file.</param>
        /// <param name="indexName">The index name to rebuild.</param>
        /// <param name="cancellationToken">A token to cancel before the operation starts.</param>
        public static Task<DecentDBIndexRebuildResult> RebuildIndexAsync(
            string databasePath,
            string indexName,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (string.IsNullOrWhiteSpace(indexName))
                throw new ArgumentException("Index name cannot be null or empty.", nameof(indexName));

            var fullPath = NormalizeDatabasePath(databasePath);
            var before = GetWalStatus(fullPath);
            if (!File.Exists(fullPath))
            {
                return Task.FromResult(new DecentDBIndexRebuildResult(
                    fullPath,
                    databaseExisted: false,
                    Array.Empty<string>(),
                    before,
                    before,
                    TimeSpan.Zero));
            }

            var stopwatch = Stopwatch.StartNew();
            using (var connection = OpenConnection(fullPath))
            {
                ExecuteNonQuery(connection, $"ALTER INDEX {QuoteIdentifier(indexName)} REBUILD");
            }

            stopwatch.Stop();
            var after = GetWalStatus(fullPath);
            return Task.FromResult(new DecentDBIndexRebuildResult(
                fullPath,
                databaseExisted: true,
                new[] { indexName },
                before,
                after,
                stopwatch.Elapsed));
        }

        /// <summary>
        /// Opens the database and rebuilds all catalog indexes without invoking the CLI.
        /// </summary>
        /// <param name="databasePath">The path to the DecentDB database file.</param>
        /// <param name="cancellationToken">A token to cancel before the operation starts.</param>
        public static Task<DecentDBIndexRebuildResult> RebuildIndexesAsync(
            string databasePath,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fullPath = NormalizeDatabasePath(databasePath);
            var before = GetWalStatus(fullPath);
            if (!File.Exists(fullPath))
            {
                return Task.FromResult(new DecentDBIndexRebuildResult(
                    fullPath,
                    databaseExisted: false,
                    Array.Empty<string>(),
                    before,
                    before,
                    TimeSpan.Zero));
            }

            var rebuilt = new List<string>();
            var stopwatch = Stopwatch.StartNew();
            using (var connection = OpenConnection(fullPath))
            {
                foreach (var indexName in ListIndexNames(connection))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    ExecuteNonQuery(connection, $"ALTER INDEX {QuoteIdentifier(indexName)} REBUILD");
                    rebuilt.Add(indexName);
                }
            }

            stopwatch.Stop();
            var after = GetWalStatus(fullPath);
            return Task.FromResult(new DecentDBIndexRebuildResult(
                fullPath,
                databaseExisted: true,
                rebuilt,
                before,
                after,
                stopwatch.Elapsed));
        }

        /// <summary>
        /// Saves a compact copy of a database to a new destination file without invoking the CLI.
        /// </summary>
        /// <param name="sourceDatabasePath">The source DecentDB database file.</param>
        /// <param name="destinationDatabasePath">The destination DecentDB database file.</param>
        /// <param name="overwrite">Whether to delete an existing destination and its sidecars first.</param>
        /// <param name="cancellationToken">A token to cancel before the operation starts.</param>
        public static Task<DecentDBCompactResult> CompactAsync(
            string sourceDatabasePath,
            string destinationDatabasePath,
            bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var sourcePath = NormalizeDatabasePath(sourceDatabasePath);
            var destinationPath = NormalizeDatabasePath(destinationDatabasePath);
            if (PathsEqual(sourcePath, destinationPath))
            {
                throw new ArgumentException("Destination database path must be different from the source path.", nameof(destinationDatabasePath));
            }

            var sourceBytes = FileLengthOrZero(sourcePath);
            if (!File.Exists(sourcePath))
            {
                return Task.FromResult(new DecentDBCompactResult(
                    sourcePath,
                    destinationPath,
                    sourceExisted: false,
                    sourceBytes,
                    destinationBytes: 0,
                    TimeSpan.Zero));
            }

            var destinationDirectory = Path.GetDirectoryName(destinationPath);
            if (!string.IsNullOrEmpty(destinationDirectory) && !Directory.Exists(destinationDirectory))
            {
                Directory.CreateDirectory(destinationDirectory);
            }

            var destinationArtifactsExist = HasDatabaseArtifacts(destinationPath);
            if (destinationArtifactsExist)
            {
                if (!overwrite)
                {
                    throw new IOException($"Destination database or sidecar artifacts already exist: {destinationPath}");
                }

                DecentDBConnection.DeleteDatabaseFiles(destinationPath);
            }

            var stopwatch = Stopwatch.StartNew();
            using (var connection = OpenConnection(sourcePath))
            {
                connection.SaveAs(destinationPath);
            }

            stopwatch.Stop();
            return Task.FromResult(new DecentDBCompactResult(
                sourcePath,
                destinationPath,
                sourceExisted: true,
                sourceBytes,
                FileLengthOrZero(destinationPath),
                stopwatch.Elapsed));
        }

        /// <summary>
        /// Compacts a database to a temporary file and replaces the original without invoking the CLI.
        /// Ensure no other connections are open to the database file before running.
        /// </summary>
        /// <param name="databasePath">The path to the DecentDB database file.</param>
        /// <param name="createBackup">If true, renames the original database file with a .bak extension instead of deleting it.</param>
        /// <param name="cancellationToken">A token to cancel before each operation phase starts.</param>
        public static async Task<DecentDBVacuumResult> VacuumAsync(
            string databasePath,
            bool createBackup = false,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fullPath = NormalizeDatabasePath(databasePath);
            var before = GetWalStatus(fullPath);
            if (!File.Exists(fullPath))
            {
                return new DecentDBVacuumResult(
                    fullPath,
                    databaseExisted: false,
                    backupCreated: false,
                    backupPath: null,
                    before,
                    before,
                    TimeSpan.Zero);
            }

            var stopwatch = Stopwatch.StartNew();
            var tempPath = fullPath + ".vacuum_tmp";
            var backupPath = fullPath + ".bak";

            try
            {
                await CheckpointAsync(fullPath, cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                DecentDBConnection.DeleteDatabaseFiles(tempPath);
                await CompactAsync(fullPath, tempPath, overwrite: true, cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                if (createBackup)
                {
                    if (File.Exists(backupPath))
                    {
                        File.Delete(backupPath);
                    }

                    File.Move(fullPath, backupPath);
                }
                else
                {
                    File.Delete(fullPath);
                }

                DeleteSidecars(fullPath);
                File.Move(tempPath, fullPath);
                DeleteSidecars(tempPath);
                stopwatch.Stop();

                return new DecentDBVacuumResult(
                    fullPath,
                    databaseExisted: true,
                    backupCreated: createBackup,
                    backupPath: createBackup ? backupPath : null,
                    before,
                    GetWalStatus(fullPath),
                    stopwatch.Elapsed);
            }
            catch
            {
                DecentDBConnection.DeleteDatabaseFiles(tempPath);
                throw;
            }
        }

        private static string ResolveCliExecutablePath(string cliExecutablePath)
        {
            if (Path.IsPathRooted(cliExecutablePath) && File.Exists(cliExecutablePath))
            {
                return cliExecutablePath;
            }

            if (File.Exists(cliExecutablePath))
            {
                return Path.GetFullPath(cliExecutablePath);
            }

            foreach (var baseDir in new[] { AppContext.BaseDirectory, Directory.GetCurrentDirectory() })
            {
                if (string.IsNullOrWhiteSpace(baseDir))
                {
                    continue;
                }

                var di = new DirectoryInfo(baseDir);
                for (var cursor = di; cursor != null; cursor = cursor.Parent)
                {
                    foreach (var candidate in CandidateCliPaths(cursor.FullName))
                    {
                        if (File.Exists(candidate))
                        {
                            return candidate;
                        }
                    }
                }
            }

            return cliExecutablePath;
        }

        private static string NormalizeDatabasePath(string databasePath)
        {
            if (string.IsNullOrWhiteSpace(databasePath))
                throw new ArgumentException("Database path cannot be null or empty.", nameof(databasePath));

            return Path.GetFullPath(databasePath);
        }

        private static bool PathsEqual(string left, string right)
        {
            var comparison = OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;
            return string.Equals(left, right, comparison);
        }

        private static DecentDBConnection OpenConnection(string databasePath)
        {
            var builder = new DecentDBConnectionStringBuilder
            {
                DataSource = databasePath
            };
            var connection = new DecentDBConnection(builder.ConnectionString);
            connection.Open();
            return connection;
        }

        private static void ExecuteNonQuery(DecentDBConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        private static IReadOnlyList<string> ListIndexNames(DecentDBConnection connection)
        {
            using var document = JsonDocument.Parse(connection.ListIndexesJson());
            if (document.RootElement.ValueKind != JsonValueKind.Array)
            {
                return Array.Empty<string>();
            }

            var names = new List<string>();
            foreach (var element in document.RootElement.EnumerateArray())
            {
                if (element.TryGetProperty("name", out var nameProperty) &&
                    nameProperty.ValueKind == JsonValueKind.String)
                {
                    var name = nameProperty.GetString();
                    if (!string.IsNullOrWhiteSpace(name))
                    {
                        names.Add(name);
                    }
                }
            }

            return names;
        }

        private static string QuoteIdentifier(string identifier)
        {
            return "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
        }

        private static long FileLengthOrZero(string path)
        {
            try
            {
                return File.Exists(path) ? new FileInfo(path).Length : 0;
            }
            catch (IOException)
            {
                return 0;
            }
            catch (UnauthorizedAccessException)
            {
                return 0;
            }
        }

        private static bool HasDatabaseArtifacts(string databasePath)
        {
            return File.Exists(databasePath) ||
                   File.Exists(databasePath + "-wal") ||
                   File.Exists(databasePath + ".wal") ||
                   File.Exists(databasePath + "-shm") ||
                   File.Exists(databasePath + ".coord");
        }

        private static void DeleteSidecars(string databasePath)
        {
            TryDelete(databasePath + "-wal");
            TryDelete(databasePath + ".wal");
            TryDelete(databasePath + "-shm");
            TryDelete(databasePath + ".coord");
        }

        private static void TryDelete(string path)
        {
            try
            {
                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
            catch (FileNotFoundException)
            {
            }
            catch (DirectoryNotFoundException)
            {
            }
        }

        private static IEnumerable<string> CandidateCliPaths(string root)
        {
            if (OperatingSystem.IsWindows())
            {
                yield return Path.Combine(root, "target", "debug", "decentdb.exe");
                yield return Path.Combine(root, "target", "release", "decentdb.exe");
                yield return Path.Combine(root, "decentdb.exe");
                yield return Path.Combine(root, "decentdb");
                yield break;
            }

            yield return Path.Combine(root, "target", "debug", "decentdb");
            yield return Path.Combine(root, "target", "release", "decentdb");
            yield return Path.Combine(root, "decentdb");
        }

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

            cliExecutablePath = ResolveCliExecutablePath(cliExecutablePath);

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
