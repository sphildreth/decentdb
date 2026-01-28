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
      if not open(f, path, fmWrite):
        return err[VfsFile](ERR_IO, "Failed to create file", path)
      close(f)
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
  var bytesRead = 0
  withFileLock(file):
    try:
      if buf.len == 0:
        bytesRead = 0
      else:
        setFilePos(file.file, offset)
        bytesRead = file.file.readBuffer(addr buf[0], buf.len)
    except OSError:
      return err[int](ERR_IO, "Read failed", file.path)
  ok(bytesRead)

method write*(vfs: OsVfs, file: VfsFile, offset: int64, buf: openArray[byte]): Result[int] =
  var bytesWritten = 0
  withFileLock(file):
    try:
      if buf.len == 0:
        bytesWritten = 0
      else:
        setFilePos(file.file, offset)
        bytesWritten = file.file.writeBuffer(unsafeAddr buf[0], buf.len)
    except OSError:
      return err[int](ERR_IO, "Write failed", file.path)
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
