import ./types
import ../errors

type FaultOp* = enum
  foOpen
  foRead
  foWrite
  foFsync
  foClose

type FaultActionKind* = enum
  faNone
  faError
  faPartialWrite
  faDropFsync

type FaultAction* = object
  kind*: FaultActionKind
  partialBytes*: int
  errorCode*: ErrorCode
  label*: string

type FaultRule* = object
  label*: string
  op*: FaultOp
  remaining*: int
  action*: FaultAction

type FaultLogEntry* = object
  op*: FaultOp
  label*: string
  action*: FaultAction
  requestedBytes*: int
  appliedBytes*: int
  errorCode*: ErrorCode

type FaultyVfs* = ref object of Vfs
  inner*: Vfs
  rules*: seq[FaultRule]
  log*: seq[FaultLogEntry]
  replayLog*: seq[FaultLogEntry]
  replayIndex*: int

proc newFaultyVfs*(inner: Vfs): FaultyVfs =
  FaultyVfs(inner: inner)

proc newFaultyVfsWithReplay*(inner: Vfs, replay: seq[FaultLogEntry]): FaultyVfs =
  FaultyVfs(inner: inner, replayLog: replay, replayIndex: 0)

proc addRule*(fv: FaultyVfs, rule: FaultRule) =
  fv.rules.add(rule)

proc clearRules*(fv: FaultyVfs) =
  fv.rules = @[]

proc getLog*(fv: FaultyVfs): seq[FaultLogEntry] =
  fv.log

proc nextAction(fv: FaultyVfs, op: FaultOp, requestedBytes: int): FaultAction =
  if fv.replayIndex < fv.replayLog.len:
    let entry = fv.replayLog[fv.replayIndex]
    fv.replayIndex.inc
    return entry.action
  for i in 0 ..< fv.rules.len:
    if fv.rules[i].op == op and fv.rules[i].remaining != 0:
      let action = fv.rules[i].action
      if fv.rules[i].remaining > 0:
        fv.rules[i].remaining.dec
      return action
  FaultAction(kind: faNone)

proc record(fv: FaultyVfs, op: FaultOp, label: string, action: FaultAction, requestedBytes: int, appliedBytes: int, errorCode: ErrorCode) =
  fv.log.add(FaultLogEntry(op: op, label: label, action: action, requestedBytes: requestedBytes, appliedBytes: appliedBytes, errorCode: errorCode))

method open*(fv: FaultyVfs, path: string, mode: FileMode, create: bool): Result[VfsFile] =
  let action = fv.nextAction(foOpen, 0)
  if action.kind == faError:
    fv.record(foOpen, action.label, action, 0, 0, action.errorCode)
    return err[VfsFile](action.errorCode, "Injected error on open", path)
  let res = fv.inner.open(path, mode, create)
  if res.ok:
    fv.record(foOpen, action.label, action, 0, 0, ERR_INTERNAL)
  else:
    fv.record(foOpen, action.label, action, 0, 0, res.err.code)
  res

method read*(fv: FaultyVfs, file: VfsFile, offset: int64, buf: var openArray[byte]): Result[int] =
  let action = fv.nextAction(foRead, buf.len)
  if action.kind == faError:
    fv.record(foRead, action.label, action, buf.len, 0, action.errorCode)
    return err[int](action.errorCode, "Injected error on read", file.path)
  let res = fv.inner.read(file, offset, buf)
  if res.ok:
    fv.record(foRead, action.label, action, buf.len, res.value, ERR_INTERNAL)
  else:
    fv.record(foRead, action.label, action, buf.len, 0, res.err.code)
  res

method write*(fv: FaultyVfs, file: VfsFile, offset: int64, buf: openArray[byte]): Result[int] =
  let action = fv.nextAction(foWrite, buf.len)
  if action.kind == faError:
    fv.record(foWrite, action.label, action, buf.len, 0, action.errorCode)
    return err[int](action.errorCode, "Injected error on write", file.path)
  if action.kind == faPartialWrite:
    let partial = min(action.partialBytes, buf.len)
    let res = fv.inner.write(file, offset, buf[0 ..< partial])
    if res.ok:
      fv.record(foWrite, action.label, action, buf.len, res.value, ERR_INTERNAL)
    else:
      fv.record(foWrite, action.label, action, buf.len, 0, res.err.code)
    return res
  let res = fv.inner.write(file, offset, buf)
  if res.ok:
    fv.record(foWrite, action.label, action, buf.len, res.value, ERR_INTERNAL)
  else:
    fv.record(foWrite, action.label, action, buf.len, 0, res.err.code)
  res

method fsync*(fv: FaultyVfs, file: VfsFile): Result[Void] =
  let action = fv.nextAction(foFsync, 0)
  if action.kind == faError:
    fv.record(foFsync, action.label, action, 0, 0, action.errorCode)
    return err[Void](action.errorCode, "Injected error on fsync", file.path)
  if action.kind == faDropFsync:
    fv.record(foFsync, action.label, action, 0, 0, ERR_INTERNAL)
    return okVoid()
  let res = fv.inner.fsync(file)
  if res.ok:
    fv.record(foFsync, action.label, action, 0, 0, ERR_INTERNAL)
  else:
    fv.record(foFsync, action.label, action, 0, 0, res.err.code)
  res

method close*(fv: FaultyVfs, file: VfsFile): Result[Void] =
  let action = fv.nextAction(foClose, 0)
  if action.kind == faError:
    fv.record(foClose, action.label, action, 0, 0, action.errorCode)
    return err[Void](action.errorCode, "Injected error on close", file.path)
  let res = fv.inner.close(file)
  if res.ok:
    fv.record(foClose, action.label, action, 0, 0, ERR_INTERNAL)
  else:
    fv.record(foClose, action.label, action, 0, 0, res.err.code)
  res
