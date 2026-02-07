using System;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Threading;

namespace DecentDB.Native;

public static class DecentDBNative
{
    private const string NativeLib = "decentdb";

    private static string? s_libraryPath;

    private static int s_resolverRegistered;

    /// <summary>
    /// Overrides the native library file path used to resolve DllImport("decentdb").
    /// Useful for test runs and custom deployments.
    /// </summary>
    public static void SetLibraryPath(string libraryPath)
    {
        if (string.IsNullOrWhiteSpace(libraryPath))
        {
            throw new ArgumentException("Library path must be non-empty.", nameof(libraryPath));
        }

        s_libraryPath = Path.GetFullPath(libraryPath);
    }

    internal static void EnsureInitialized()
    {
        RegisterDllImportResolver();
    }

    private static void RegisterDllImportResolver()
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        if (Interlocked.Exchange(ref s_resolverRegistered, 1) != 0)
        {
            return;
        }

        try
        {
            NativeLibrary.SetDllImportResolver(typeof(DecentDBNative).Assembly, (name, assembly, path) =>
            {
                if (name == NativeLib)
                {
                    foreach (var libPath in EnumerateCandidateLibraryPaths())
                    {
                        if (!File.Exists(libPath))
                        {
                            continue;
                        }

                        return NativeLibrary.Load(libPath);
                    }
                }

                return IntPtr.Zero;
            });
        }
        catch (InvalidOperationException)
        {
            // Another initializer may have already set a resolver for this assembly.
            // In that case we can't override it.
        }
    }

    private static IEnumerable<string> EnumerateCandidateLibraryPaths()
    {
        var envPath = Environment.GetEnvironmentVariable("DECENTDB_NATIVE_LIB_PATH");
        if (!string.IsNullOrWhiteSpace(envPath))
        {
            yield return Path.GetFullPath(envPath);
        }

        if (!string.IsNullOrWhiteSpace(s_libraryPath))
        {
            yield return s_libraryPath!;
        }

        foreach (var dir in EnumerateProbeDirectories())
        {
            yield return Path.Combine(dir, "libdecentdb.so");
            yield return Path.Combine(dir, "libc_api.so");
            yield return Path.Combine(dir, "decentdb.so");
        }

        foreach (var baseDir in EnumerateProbeDirectories())
        {
            var di = new DirectoryInfo(baseDir);
            for (var cursor = di; cursor != null; cursor = cursor.Parent)
            {
                var buildDir = Path.Combine(cursor.FullName, "build");
                yield return Path.Combine(buildDir, "libdecentdb.so");
                yield return Path.Combine(buildDir, "libc_api.so");
                yield return Path.Combine(buildDir, "decentdb.so");
            }
        }
    }

    private static IEnumerable<string> EnumerateProbeDirectories()
    {
        if (!string.IsNullOrWhiteSpace(AppContext.BaseDirectory))
        {
            yield return AppContext.BaseDirectory;
        }

        var assemblyDir = Path.GetDirectoryName(typeof(DecentDBNative).Assembly.Location);
        if (!string.IsNullOrWhiteSpace(assemblyDir))
        {
            yield return assemblyDir;
        }

        var cwd = Directory.GetCurrentDirectory();
        if (!string.IsNullOrWhiteSpace(cwd))
        {
            yield return cwd;
        }
    }

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_close")]
    public static extern int decentdb_close(IntPtr db);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_last_error_code")]
    public static extern int decentdb_last_error_code(IntPtr db);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_reset")]
    public static extern int decentdb_reset(IntPtr stmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_clear_bindings")]
    public static extern int decentdb_clear_bindings(IntPtr stmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_step")]
    public static extern int decentdb_step(IntPtr stmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_column_count")]
    public static extern int decentdb_column_count(IntPtr stmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_column_type")]
    public static extern int decentdb_column_type(IntPtr stmt, int col0Based);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_column_is_null")]
    public static extern int decentdb_column_is_null(IntPtr stmt, int col0Based);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_column_int64")]
    public static extern long decentdb_column_int64(IntPtr stmt, int col0Based);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_column_float64")]
    public static extern double decentdb_column_float64(IntPtr stmt, int col0Based);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_column_blob")]
    public static extern IntPtr decentdb_column_blob(IntPtr stmt, int col0Based, out int outByteLen);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_row_view")]
    public static extern int decentdb_row_view(IntPtr stmt, out IntPtr outValues, out int outCount);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_rows_affected")]
    public static extern long decentdb_rows_affected(IntPtr stmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_finalize")]
    public static extern void decentdb_finalize(IntPtr stmt);
}

public static unsafe class DecentDBNativeUnsafe
{
    private const string NativeLib = "decentdb";

    static DecentDBNativeUnsafe()
    {
        DecentDBNative.EnsureInitialized();
    }

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_open")]
    public static extern IntPtr decentdb_open(byte* pathUtf8, byte* optionsUtf8);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_last_error_message")]
    public static extern byte* decentdb_last_error_message(IntPtr db);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_prepare")]
    public static extern int decentdb_prepare(IntPtr db, byte* sqlUtf8, out IntPtr outStmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_bind_null")]
    public static extern int decentdb_bind_null(IntPtr stmt, int index1Based);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_bind_int64")]
    public static extern int decentdb_bind_int64(IntPtr stmt, int index1Based, long v);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_bind_float64")]
    public static extern int decentdb_bind_float64(IntPtr stmt, int index1Based, double v);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_bind_text")]
    public static extern int decentdb_bind_text(IntPtr stmt, int index1Based, byte* utf8, int byteLen);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_bind_blob")]
    public static extern int decentdb_bind_blob(IntPtr stmt, int index1Based, byte* data, int byteLen);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_column_name")]
    public static extern byte* decentdb_column_name(IntPtr stmt, int col0Based);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "decentdb_column_text")]
    public static extern byte* decentdb_column_text(IntPtr stmt, int col0Based, out int outByteLen);
}

[StructLayout(LayoutKind.Sequential)]
public struct DecentdbValueView
{
    public int kind;
    public int is_null;
    public long int64_val;
    public double float64_val;
    public IntPtr bytes;
    public int bytes_len;
}

public enum DbValueKind : int
{
    Null = 0,
    Int64 = 1,
    Bool = 2,
    Float64 = 3,
    Text = 4,
    Blob = 5
}
