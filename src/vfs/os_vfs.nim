import os
import locks
import ./types
import ../errors
when defined(windows):
  import winlean
else:
  import posix

type OsVfs* = ref object of Vfs

proc newOsVfs*(): OsVfs =
  OsVfs()

template withFileLock(file: VfsFile, body: untyped) =
  acquire(file.lock)
  try:
    body
  finally:
    release(file.lock)

method open*(vfs: OsVfs, path: string, mode: FileMode, create: bool): Result[VfsFile] =
  var f: File
  try:
    var openMode = mode
    let exists = fileExists(path)
    if not exists:
      if not create:
        return err[VfsFile](ERR_IO, "File does not exist", path)
      if mode == fmRead:
        return err[VfsFile](ERR_IO, "Cannot create file in read mode", path)
      if mode == fmReadWrite:
        openMode = fmReadWrite
      if not open(f, path, openMode):
        return err[VfsFile](ERR_IO, "Failed to create file", path)
    else:
      if mode == fmReadWrite:
        openMode = fmReadWriteExisting
      if not open(f, path, openMode):
        return err[VfsFile](ERR_IO, "Failed to open file", path)
  except OSError:
    return err[VfsFile](ERR_IO, "Failed to open file", path)
  let vf = VfsFile(path: path, file: f)
  initLock(vf.lock)
  ok(vf)

method read*(vfs: OsVfs, file: VfsFile, offset: int64, buf: var openArray[byte]): Result[int] =
  ## Read without lock - pread is thread-safe and position-independent.
  if buf.len == 0:
    return ok(0)
  try:
    when defined(windows):
      var bytesRead: DWORD = 0
      var overlapped: OVERLAPPED
      zeroMem(addr overlapped, sizeof(overlapped))
      overlapped.Offset = DWORD(uint64(offset) and 0xFFFFFFFF'u64)
      overlapped.OffsetHigh = DWORD((uint64(offset) shr 32) and 0xFFFFFFFF'u64)
      let handle = get_osfhandle(file.file.getFileHandle())
      if ReadFile(handle, addr buf[0], DWORD(buf.len), addr bytesRead, addr overlapped) == 0:
        return err[int](ERR_IO, "Read failed", file.path)
      ok(int(bytesRead))
    else:
      let fd = cast[cint](file.file.getFileHandle())
      let res = pread(fd, addr buf[0], buf.len, offset.Off)
      if res < 0:
        return err[int](ERR_IO, "Read failed", file.path)
      ok(int(res))
  except OSError:
    err[int](ERR_IO, "Read failed", file.path)

method readStr*(vfs: OsVfs, file: VfsFile, offset: int64, buf: var string): Result[int] =
  ## Read without lock - pread is thread-safe and position-independent.
  if buf.len == 0:
    return ok(0)
  try:
    when defined(windows):
      var bytesRead: DWORD = 0
      var overlapped: OVERLAPPED
      zeroMem(addr overlapped, sizeof(overlapped))
      overlapped.Offset = DWORD(uint64(offset) and 0xFFFFFFFF'u64)
      overlapped.OffsetHigh = DWORD((uint64(offset) shr 32) and 0xFFFFFFFF'u64)
      let handle = get_osfhandle(file.file.getFileHandle())
      if ReadFile(handle, addr buf[0], DWORD(buf.len), addr bytesRead, addr overlapped) == 0:
        return err[int](ERR_IO, "Read failed", file.path)
      ok(int(bytesRead))
    else:
      let fd = cast[cint](file.file.getFileHandle())
      let res = pread(fd, addr buf[0], buf.len, offset.Off)
      if res < 0:
        return err[int](ERR_IO, "Read failed", file.path)
      ok(int(res))
  except OSError:
    err[int](ERR_IO, "Read failed", file.path)

method write*(vfs: OsVfs, file: VfsFile, offset: int64, buf: openArray[byte]): Result[int] =
  var bytesWritten = 0
  withFileLock(file):
    try:
      if buf.len == 0:
        bytesWritten = 0
      else:
        setFilePos(file.file, offset)
        bytesWritten = file.file.writeBuffer(unsafeAddr buf[0], buf.len)
        flushFile(file.file)
    except IOError, OSError:
      return err[int](ERR_IO, "Write failed", file.path)
    if bytesWritten != buf.len:
      return err[int](ERR_IO, "Write incomplete", file.path)
  ok(bytesWritten)

method writeStr*(vfs: OsVfs, file: VfsFile, offset: int64, buf: string): Result[int] =
  var bytesWritten = 0
  withFileLock(file):
    try:
      if buf.len == 0:
        bytesWritten = 0
      else:
        setFilePos(file.file, offset)
        bytesWritten = file.file.writeBuffer(unsafeAddr buf[0], buf.len)
        flushFile(file.file)
    except IOError, OSError:
      return err[int](ERR_IO, "Write failed", file.path)
    if bytesWritten != buf.len:
      return err[int](ERR_IO, "Write incomplete", file.path)
  ok(bytesWritten)

method fsync*(vfs: OsVfs, file: VfsFile): Result[Void] =
  withFileLock(file):
    try:
      flushFile(file.file)
    except OSError:
      return err[Void](ERR_IO, "Fsync failed", file.path)
  okVoid()

method truncate*(vfs: OsVfs, file: VfsFile, size: int64): Result[Void] =
  withFileLock(file):
    try:
      when defined(windows):
        setFilePos(file.file, size)
        let handle = get_osfhandle(file.file.getFileHandle())
        if setEndOfFile(handle) == 0:
          return err[Void](ERR_IO, "Truncate failed", file.path)
      else:
        let fd = cast[cint](file.file.getFileHandle())
        if ftruncate(fd, size.Off) == -1:
          return err[Void](ERR_IO, "Truncate failed", file.path)
    except OSError:
      return err[Void](ERR_IO, "Truncate failed", file.path)
  okVoid()

method close*(vfs: OsVfs, file: VfsFile): Result[Void] =
  withFileLock(file):
    try:
      close(file.file)
    except OSError:
      return err[Void](ERR_IO, "Close failed", file.path)
  deinitLock(file.lock)
  okVoid()
