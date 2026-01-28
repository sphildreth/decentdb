import unittest
import os
import random
import strutils
import engine
import record/record
import storage/storage
import exec/exec
import search/search
import catalog/catalog

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

proc splitRow(row: string): seq[string] =
  if row.len == 0:
    return @[]
  row.split("|")

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

proc randUpperString(minLen: int, maxLen: int): string =
  let length = rand(maxLen - minLen) + minLen
  for _ in 0 ..< length:
    result.add(char(ord('A') + rand(25)))

suite "Trigram":
  test "canonicalize and short trigrams":
    check canonicalize("aBc") == "ABC"
    check trigrams("ab").len == 0
    check trigrams("abc").len == 1

  test "postings edge cases":
    let postings = encodePostings(@[1'u64, 2'u64])
    let added = addRowid(postings, 2'u64)
    check added.ok
    check added.value == postings
    let removed = removeRowid(postings, 3'u64)
    check removed.ok
    let removedDecoded = decodePostings(removed.value)
    check removedDecoded.ok
    check removedDecoded.value == @[1'u64, 2'u64]
    let bad = @[byte(0x80)]
    check postingsCount(bad) == 0
    let badRes = decodePostings(bad)
    check not badRes.ok
    let emptyLists: seq[seq[uint64]] = @[]
    let emptyResult: seq[uint64] = @[]
    check intersectPostings(emptyLists) == emptyResult

  test "postings encode/decode and intersection":
    let postings = encodePostings(@[1'u64, 2'u64, 10'u64])
    let decoded = decodePostings(postings)
    check decoded.ok
    check decoded.value == @[1'u64, 2'u64, 10'u64]
    let added = addRowid(postings, 5'u64)
    check added.ok
    let addedDecoded = decodePostings(added.value)
    check addedDecoded.ok
    check 5'u64 in addedDecoded.value
    let removed = removeRowid(postings, 2'u64)
    check removed.ok
    let removedDecoded = decodePostings(removed.value)
    check removedDecoded.ok
    check 2'u64 notin removedDecoded.value
    let inter = intersectPostings(@[@[1'u64, 2'u64, 3'u64], @[2'u64, 3'u64], @[3'u64, 4'u64]])
    check inter == @[3'u64]

  test "trigram index matches scan for LIKE":
    randomize(42)
    let path = makeTempDb("decentdb_trigram.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").ok
    check execSql(db, "CREATE INDEX docs_body_trgm ON docs USING trigram (body)").ok
    var texts: seq[string] = @[]
    for i in 1 .. 40:
      let text = randUpperString(6, 14)
      texts.add(text)
      discard execSql(db, "INSERT INTO docs (id, body) VALUES ($1, $2)", @[
        Value(kind: vkInt64, int64Val: i),
        Value(kind: vkText, bytes: toBytes(text))
      ])
    let tableRes = db.catalog.getTable("docs")
    check tableRes.ok
    let table = tableRes.value
    let rowsRes = scanTable(db.pager, table)
    check rowsRes.ok
    for _ in 0 ..< 10:
      let idx = rand(texts.len - 1)
      let text = texts[idx]
      let start = rand(text.len - 3)
      let remaining = text.len - start
      let maxLen = min(5, remaining)
      let patLen = rand(maxLen - 3) + 3
      let pattern = text[start ..< start + patLen]
      let query = "SELECT id FROM docs WHERE body LIKE '%" & pattern & "%' ORDER BY id"
      let execRes = execSql(db, query)
      check execRes.ok
      var expected: seq[string] = @[]
      for row in rowsRes.value:
        let body = valueToString(row.values[1])
        if likeMatch(body, "%" & pattern & "%", true):
          expected.add($row.rowid)
      var actual: seq[string] = @[]
      for row in execRes.value:
        actual.add(splitRow(row)[0])
      check actual == expected
    discard closeDb(db)
