import locks
import atomics
import tables
import ./types
import ../errors

type MemVfsFile* = ref object of VfsFile
  data*: seq[byte]

type MemVfs* = ref object of Vfs
  files*: Table[string, MemVfsFile]
  vfsLock*: Lock

proc newMemVfs*(): MemVfs =
  let v = MemVfs()
  initLock(v.vfsLock)
  v

method supportsMmap*(vfs: MemVfs): bool =
  false

template withVfsLock(vfs: MemVfs, body: untyped) =
  acquire(vfs.vfsLock)
  try:
    body
  finally:
    release(vfs.vfsLock)

template withFileLock(file: VfsFile, body: untyped) =
  acquire(file.lock)
  try:
    body
  finally:
    release(file.lock)

method open*(vfs: MemVfs, path: string, mode: FileMode, create: bool): Result[VfsFile] =
  withVfsLock(vfs):
    if vfs.files.hasKey(path):
      let vf = vfs.files[path]
      return ok(VfsFile(vf))
    
    if not create:
      return err[VfsFile](ERR_IO, "File does not exist", path)
      
    let vf = MemVfsFile(path: path, data: newSeq[byte]())
    initLock(vf.lock)
    vf.bufferedDirty.store(false, moRelaxed)
    vfs.files[path] = vf
    return ok(VfsFile(vf))

method read*(vfs: MemVfs, file: VfsFile, offset: int64, buf: var openArray[byte]): Result[int] =
  let mfile = MemVfsFile(file)
  withFileLock(file):
    let fileLen = int64(mfile.data.len)
    if offset >= fileLen:
      return ok(0)
    
    let available = fileLen - offset
    let toRead = min(int64(buf.len), available)
    if toRead > 0:
      copyMem(addr buf[0], unsafeAddr mfile.data[int(offset)], int(toRead))
    return ok(int(toRead))

method readStr*(vfs: MemVfs, file: VfsFile, offset: int64, buf: var string): Result[int] =
  let mfile = MemVfsFile(file)
  withFileLock(file):
    let fileLen = int64(mfile.data.len)
    if offset >= fileLen:
      return ok(0)
    
    let available = fileLen - offset
    let toRead = min(int64(buf.len), available)
    if toRead > 0:
      copyMem(addr buf[0], unsafeAddr mfile.data[int(offset)], int(toRead))
    return ok(int(toRead))

method write*(vfs: MemVfs, file: VfsFile, offset: int64, buf: openArray[byte]): Result[int] =
  if buf.len == 0:
    return ok(0)
  
  let mfile = MemVfsFile(file)
  withFileLock(file):
    let endPos = offset + int64(buf.len)
    if endPos > int64(mfile.data.len):
      mfile.data.setLen(int(endPos))
    
    copyMem(addr mfile.data[int(offset)], unsafeAddr buf[0], buf.len)
    file.bufferedDirty.store(true, moRelease)
    return ok(buf.len)

method writeStr*(vfs: MemVfs, file: VfsFile, offset: int64, buf: string): Result[int] =
  if buf.len == 0:
    return ok(0)
    
  let mfile = MemVfsFile(file)
  withFileLock(file):
    let endPos = offset + int64(buf.len)
    if endPos > int64(mfile.data.len):
      mfile.data.setLen(int(endPos))
    
    copyMem(addr mfile.data[int(offset)], unsafeAddr buf[0], buf.len)
    file.bufferedDirty.store(true, moRelease)
    return ok(buf.len)

method fsync*(vfs: MemVfs, file: VfsFile): Result[Void] =
  file.bufferedDirty.store(false, moRelease)
  okVoid()

method truncate*(vfs: MemVfs, file: VfsFile, size: int64): Result[Void] =
  let mfile = MemVfsFile(file)
  withFileLock(file):
    mfile.data.setLen(int(size))
    return okVoid()

method close*(vfs: MemVfs, file: VfsFile): Result[Void] =
  withVfsLock(vfs):
    if vfs.files.hasKey(file.path):
      vfs.files.del(file.path)
  deinitLock(file.lock)
  okVoid()

method getFileSize*(vfs: MemVfs, path: string): Result[int64] =
  withVfsLock(vfs):
    if vfs.files.hasKey(path):
      let vf = vfs.files[path]
      withFileLock(VfsFile(vf)):
        return ok(int64(vf.data.len))
    return err[int64](ERR_IO, "File does not exist", path)

method fileExists*(vfs: MemVfs, path: string): bool =
  withVfsLock(vfs):
    return vfs.files.hasKey(path)

method removeFile*(vfs: MemVfs, path: string): Result[Void] =
  ## Remove the file entry from the table. Does NOT deinitLock —
  ## the caller must still call close() on any open VfsFile handle.
  withVfsLock(vfs):
    if vfs.files.hasKey(path):
      vfs.files.del(path)
      return okVoid()
    return err[Void](ERR_IO, "File does not exist", path)

method mapWritable*(vfs: MemVfs, file: VfsFile, length: int64): Result[MmapRegion] =
  err[MmapRegion](ERR_INTERNAL, "mmap not supported in MemVfs", file.path)

method unmap*(vfs: MemVfs, region: MmapRegion): Result[Void] =
  err[Void](ERR_INTERNAL, "munmap not supported in MemVfs", "")
