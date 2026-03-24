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
            DecentDBNative.ddb_db_free(ref handle);
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
            DecentDBNative.ddb_stmt_free(ref handle);
        }
        return true;
    }
}
