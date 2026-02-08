using System;
using System.Runtime.InteropServices;

namespace DecentDB.Native;

public sealed class DecentDBHandle : CriticalHandle
{
    public IntPtr Handle => handle;

    public DecentDBHandle(IntPtr handle) : base(IntPtr.Zero)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            DecentDBNative.decentdb_close(handle);
            handle = IntPtr.Zero;
        }
        return true;
    }
}

public sealed class DecentDBStatementHandle : CriticalHandle
{
    private readonly DecentDBHandle _dbHandle;

    public IntPtr Handle => handle;

    public DecentDBStatementHandle(IntPtr handle, DecentDBHandle dbHandle) : base(IntPtr.Zero)
    {
        _dbHandle = dbHandle ?? throw new ArgumentNullException(nameof(dbHandle));
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            // Only finalize the statement if the parent database handle is still
            // valid. When the DB is closed first (e.g. during GC finalization at
            // process exit), the native state is already torn down and calling
            // decentdb_finalize would access freed memory (SIGSEGV).
            if (!_dbHandle.IsClosed)
            {
                DecentDBNative.decentdb_finalize(handle);
            }
            handle = IntPtr.Zero;
        }
        return true;
    }
}
