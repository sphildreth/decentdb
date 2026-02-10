import os
import locks
import atomics
import ./types
import ../errors
when defined(windows):
  import winlean
else:
  import posix

type OsVfs* = ref object of Vfs

proc newOsVfs*(): OsVfs =
  OsVfs()

method supportsMmap*(vfs: OsVfs): bool =
  when defined(windows):
    false
  else:
    true

template withFileLock(file: VfsFile, body: untyped) =
  acquire(file.lock)
  try:
    body
  finally:
    release(file.lock)

proc flushBufferedWritesIfNeeded(file: VfsFile): Result[Void] =
  if not file.bufferedDirty.load(moAcquire):
    return okVoid()
  withFileLock(file):
    if file.bufferedDirty.load(moAcquire):
      try:
        flushFile(file.file)
      except IOError, OSError:
        return err[Void](ERR_IO, "Flush failed", file.path)
      file.bufferedDirty.store(false, moRelease)
  okVoid()

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
      when not defined(windows):
        discard posix.chmod(path.cstring, 0o600.Mode)
    else:
      if mode == fmReadWrite:
        openMode = fmReadWriteExisting
      if not open(f, path, openMode):
        return err[VfsFile](ERR_IO, "Failed to open file", path)
  except OSError:
    return err[VfsFile](ERR_IO, "Failed to open file", path)
  let vf = VfsFile(path: path, file: f)
  initLock(vf.lock)
  vf.bufferedDirty.store(false, moRelaxed)
  ok(vf)

method read*(vfs: OsVfs, file: VfsFile, offset: int64, buf: var openArray[byte]): Result[int] =
  ## Read without lock - pread is thread-safe and position-independent.
  if buf.len == 0:
    return ok(0)
  let flushRes = flushBufferedWritesIfNeeded(file)
  if not flushRes.ok:
    return err[int](flushRes.err.code, flushRes.err.message, flushRes.err.context)
  try:
    when defined(windows):
      var bytesRead: DWORD = 0
      var overlapped: OVERLAPPED
      zeroMem(addr overlapped, sizeof(overlapped))
      overlapped.offset = DWORD(uint64(offset) and 0xFFFFFFFF'u64)
      overlapped.offsetHigh = DWORD((uint64(offset) shr 32) and 0xFFFFFFFF'u64)
      let handle = get_osfhandle(cint(file.file.getFileHandle()))
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
  let flushRes = flushBufferedWritesIfNeeded(file)
  if not flushRes.ok:
    return err[int](flushRes.err.code, flushRes.err.message, flushRes.err.context)
  try:
    when defined(windows):
      var bytesRead: DWORD = 0
      var overlapped: OVERLAPPED
      zeroMem(addr overlapped, sizeof(overlapped))
      overlapped.offset = DWORD(uint64(offset) and 0xFFFFFFFF'u64)
      overlapped.offsetHigh = DWORD((uint64(offset) shr 32) and 0xFFFFFFFF'u64)
      let handle = get_osfhandle(cint(file.file.getFileHandle()))
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
        file.bufferedDirty.store(true, moRelease)
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
        file.bufferedDirty.store(true, moRelease)
    except IOError, OSError:
      return err[int](ERR_IO, "Write failed", file.path)
    if bytesWritten != buf.len:
      return err[int](ERR_IO, "Write incomplete", file.path)
  ok(bytesWritten)

method fsync*(vfs: OsVfs, file: VfsFile): Result[Void] =
  ## Synchronize file data to persistent storage using OS-level primitives.
  ## On POSIX: uses fdatasync() (or fsync() on macOS/iOS where fdatasync is unavailable)
  ## On Windows: uses FlushFileBuffers()
  # Optimization: Removed withFileLock(file) wrapper.
  # Correctness relies on the caller (WAL writer) serializing writes/fsyncs via wal.lock.
  try:
    # First flush stdio buffers to ensure all data is in kernel buffers
    flushFile(file.file)
    file.bufferedDirty.store(false, moRelease)
    
    when defined(windows):
      # Windows: Use FlushFileBuffers for OS-level sync
      let handle = get_osfhandle(cint(file.file.getFileHandle()))
      if handle == INVALID_HANDLE_VALUE:
        return err[Void](ERR_IO, "Invalid file handle for fsync", file.path)
      if FlushFileBuffers(handle) == 0:
        return err[Void](ERR_IO, "FlushFileBuffers failed", file.path)
    else:
      # POSIX: Use fdatasync for data-only sync (faster than fsync)
      # Falls back to fsync on platforms where fdatasync is unavailable
      let fd = cint(file.file.getFileHandle())
      when defined(macosx) or defined(ios):
        # macOS/iOS don't have fdatasync, use fsync instead
        if fsync(fd) != 0:
          return err[Void](ERR_IO, "fsync failed: " & $strerror(errno), file.path)
      else:
        # Linux and other POSIX systems: prefer fdatasync
        if fdatasync(fd) != 0:
          # If fdatasync fails (e.g., not implemented), fall back to fsync
          if fsync(fd) != 0:
            return err[Void](ERR_IO, "fdatasync/fsync failed: " & $strerror(errno), file.path)
  except OSError:
    return err[Void](ERR_IO, "Fsync failed", file.path)
  okVoid()

method truncate*(vfs: OsVfs, file: VfsFile, size: int64): Result[Void] =
  withFileLock(file):
    try:
      if file.bufferedDirty.load(moAcquire):
        flushFile(file.file)
        file.bufferedDirty.store(false, moRelease)
      when defined(windows):
        setFilePos(file.file, size)
        let handle = get_osfhandle(cint(file.file.getFileHandle()))
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
      if file.bufferedDirty.load(moAcquire):
        flushFile(file.file)
        file.bufferedDirty.store(false, moRelease)
      close(file.file)
    except OSError:
      return err[Void](ERR_IO, "Close failed", file.path)
  deinitLock(file.lock)
  okVoid()

method mapWritable*(vfs: OsVfs, file: VfsFile, length: int64): Result[MmapRegion] =
  when defined(windows):
    err[MmapRegion](ERR_INTERNAL, "mmap not supported on Windows", file.path)
  else:
    if length <= 0:
      return err[MmapRegion](ERR_INTERNAL, "Invalid mmap length", file.path)
    try:
      let fd = cast[cint](file.file.getFileHandle())
      let region = mmap(nil, int(length), cint(PROT_READ or PROT_WRITE), cint(MAP_SHARED), fd, 0.Off)
      if region == MAP_FAILED:
        return err[MmapRegion](ERR_IO, "mmap failed: " & $strerror(errno), file.path)
      ok(MmapRegion(base: region, len: int(length)))
    except OSError:
      err[MmapRegion](ERR_IO, "mmap failed", file.path)

method unmap*(vfs: OsVfs, region: MmapRegion): Result[Void] =
  when defined(windows):
    err[Void](ERR_INTERNAL, "munmap not supported on Windows", "")
  else:
    if region.base == nil or region.len <= 0:
      return okVoid()
    try:
      if munmap(region.base, int(region.len)) != 0:
        return err[Void](ERR_IO, "munmap failed: " & $strerror(errno))
    except OSError:
      return err[Void](ERR_IO, "munmap failed")
    okVoid()
