import ./errors
import ./vfs/types
import ./vfs/os_vfs

type Db* = ref object
  path*: string
  vfs*: Vfs
  file*: VfsFile
  isOpen*: bool

proc openDb*(path: string): Result[Db] =
  let vfs = newOsVfs()
  let res = vfs.open(path, fmReadWrite, true)
  if not res.ok:
    return err[Db](res.err.code, res.err.message, res.err.context)
  ok(Db(path: path, vfs: vfs, file: res.value, isOpen: true))

proc execSql*(db: Db, sql: string): Result[seq[string]] =
  if not db.isOpen:
    return err[seq[string]](ERR_INTERNAL, "Database not open")
  ok(newSeq[string]())

proc closeDb*(db: Db): Result[Void] =
  if not db.isOpen:
    return okVoid()
  let res = db.vfs.close(db.file)
  if not res.ok:
    return res
  db.isOpen = false
  okVoid()
