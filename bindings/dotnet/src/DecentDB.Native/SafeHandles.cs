using System;
using System.Runtime.InteropServices;

namespace DecentDb.Native;

public sealed class DecentDbHandle : CriticalHandle
{
    public IntPtr Handle => handle;

    public DecentDbHandle(IntPtr handle) : base(IntPtr.Zero)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            DecentDbNative.decentdb_close(handle);
            handle = IntPtr.Zero;
        }
        return true;
    }
}

public sealed class DecentDbStatementHandle : CriticalHandle
{
    public IntPtr Handle => handle;

    public DecentDbStatementHandle(IntPtr handle) : base(IntPtr.Zero)
    {
        SetHandle(handle);
    }

    public override bool IsInvalid => handle == IntPtr.Zero;

    protected override bool ReleaseHandle()
    {
        if (!IsInvalid)
        {
            DecentDbNative.decentdb_finalize(handle);
            handle = IntPtr.Zero;
        }
        return true;
    }
}
