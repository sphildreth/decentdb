import locks
import atomics
import ../errors

type VfsFile* = ref object of RootObj
  path*: string
  lock*: Lock
  bufferedDirty*: Atomic[bool]

type MmapRegion* = object
  base*: pointer
  len*: int

type Vfs* = ref object of RootObj

method supportsMmap*(vfs: Vfs): bool {.base.} =
  false

method open*(vfs: Vfs, path: string, mode: FileMode, create: bool): Result[VfsFile] {.base.} =
  err[VfsFile](ERR_INTERNAL, "VFS.open not implemented", path)

method read*(vfs: Vfs, file: VfsFile, offset: int64, buf: var openArray[byte]): Result[int] {.base.} =
  err[int](ERR_INTERNAL, "VFS.read not implemented", file.path)

method write*(vfs: Vfs, file: VfsFile, offset: int64, buf: openArray[byte]): Result[int] {.base.} =
  err[int](ERR_INTERNAL, "VFS.write not implemented", file.path)

method readStr*(vfs: Vfs, file: VfsFile, offset: int64, buf: var string): Result[int] {.base.} =
  err[int](ERR_INTERNAL, "VFS.readStr not implemented", file.path)

method writeStr*(vfs: Vfs, file: VfsFile, offset: int64, buf: string): Result[int] {.base.} =
  err[int](ERR_INTERNAL, "VFS.writeStr not implemented", file.path)

method fsync*(vfs: Vfs, file: VfsFile): Result[Void] {.base.} =
  err[Void](ERR_INTERNAL, "VFS.fsync not implemented", file.path)

method truncate*(vfs: Vfs, file: VfsFile, size: int64): Result[Void] {.base.} =
  err[Void](ERR_INTERNAL, "VFS.truncate not implemented", file.path)

method close*(vfs: Vfs, file: VfsFile): Result[Void] {.base.} =
  err[Void](ERR_INTERNAL, "VFS.close not implemented", file.path)

method getFileSize*(vfs: Vfs, path: string): Result[int64] {.base.} =
  err[int64](ERR_INTERNAL, "VFS.getFileSize not implemented", path)

method fileExists*(vfs: Vfs, path: string): bool {.base.} =
  false

method removeFile*(vfs: Vfs, path: string): Result[Void] {.base.} =
  err[Void](ERR_INTERNAL, "VFS.removeFile not implemented", path)

method mapWritable*(vfs: Vfs, file: VfsFile, length: int64): Result[MmapRegion] {.base.} =
  err[MmapRegion](ERR_INTERNAL, "VFS.mapWritable not implemented", file.path)

method unmap*(vfs: Vfs, region: MmapRegion): Result[Void] {.base.} =
  err[Void](ERR_INTERNAL, "VFS.unmap not implemented", "")
