import os
import locks
import ./types
import ../errors

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
    if not fileExists(path):
      if not create:
        return err[VfsFile](ERR_IO, "File does not exist", path)
      discard open(f, path, fmWrite)
      close(f)
    discard open(f, path, mode)
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

method close*(vfs: OsVfs, file: VfsFile): Result[Void] =
  withFileLock(file):
    try:
      close(file.file)
    except OSError:
      return err[Void](ERR_IO, "Close failed", file.path)
  deinitLock(file.lock)
  okVoid()
