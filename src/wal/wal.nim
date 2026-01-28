type Wal* = ref object
  path*: string

proc newWal*(path: string): Wal =
  Wal(path: path)
