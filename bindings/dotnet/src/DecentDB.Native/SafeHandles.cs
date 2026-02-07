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
    public IntPtr Handle => handle;

    public DecentDBStatementHandle(IntPtr handle) : base(IntPtr.Zero)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            DecentDBNative.decentdb_finalize(handle);
            handle = IntPtr.Zero;
        }
        return true;
    }
}
