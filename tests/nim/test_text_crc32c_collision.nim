import unittest, os
import strutils
import tables

import engine
import pager/db_header
import record/record

proc valInt(i: int): Value = Value(kind: vkInt64, int64Val: int64(i))
proc valText(s: string): Value =
  var bytes = newSeq[byte](s.len)
  if s.len > 0:
    copyMem(addr bytes[0], unsafeAddr s[0], s.len)
  Value(kind: vkText, bytes: bytes)

proc xorshift64(x: var uint64): uint64 {.inline.} =
  x = x xor (x shl 13)
  x = x xor (x shr 7)
  x = x xor (x shl 17)
  x

proc findCrc32cCollision(maxI: int): tuple[a: string, b: string] =
  ## Find two distinct strings with the same crc32c.
  ## Uses deterministic pseudo-random generation to avoid structured inputs
  ## that can be accidentally collision-free for long prefixes.
  var seen: Table[uint32, string] = initTable[uint32, string]()
  var state = 0x9e3779b97f4a7c15'u64
  for _ in 0 .. maxI:
    let r1 = xorshift64(state)
    let r2 = xorshift64(state)
    let s = toHex(r1, 16) & toHex(r2, 16)
    let c = crc32c(s)
    if seen.hasKey(c):
      let other = seen[c]
      if other != s:
        return (a: other, b: s)
    else:
      seen[c] = s
  return (a: "", b: "")

suite "TEXT CRC32C collision safety":
  setup:
    let dbPath = "test_text_crc32c_collision.db"
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")

  teardown:
    let dbPath = "test_text_crc32c_collision.db"
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")

  test "CRC32C collisions do not break UNIQUE/FK correctness":
    let pair = findCrc32cCollision(600_000)
    require pair.a.len > 0
    require pair.b.len > 0
    require pair.a != pair.b

    let dbRes = openDb("test_text_crc32c_collision.db")
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)

    # UNIQUE: inserting two distinct colliding strings must succeed.
    require db.execSql("CREATE TABLE u (id INT PRIMARY KEY, s TEXT UNIQUE)").ok
    require db.execSql("INSERT INTO u VALUES ($1, $2)", @[valInt(1), valText(pair.a)]).ok
    require db.execSql("INSERT INTO u VALUES ($1, $2)", @[valInt(2), valText(pair.b)]).ok

    # CREATE UNIQUE INDEX: collision must not be treated as duplication.
    require db.execSql("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)").ok
    require db.execSql("INSERT INTO t VALUES ($1, $2)", @[valInt(1), valText(pair.a)]).ok
    require db.execSql("INSERT INTO t VALUES ($1, $2)", @[valInt(2), valText(pair.b)]).ok
    require db.execSql("CREATE UNIQUE INDEX idx_t_s ON t(s)").ok

    # FOREIGN KEY: a colliding-but-non-matching value must not satisfy the FK.
    require db.execSql("CREATE TABLE p (id INT PRIMARY KEY, s TEXT UNIQUE)").ok
    require db.execSql("CREATE TABLE c (id INT PRIMARY KEY, s TEXT REFERENCES p(s))").ok
    require db.execSql("INSERT INTO p VALUES ($1, $2)", @[valInt(1), valText(pair.a)]).ok

    let badFkRes = db.execSql("INSERT INTO c VALUES ($1, $2)", @[valInt(1), valText(pair.b)])
    check(not badFkRes.ok)
