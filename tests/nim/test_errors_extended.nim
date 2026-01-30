import unittest
import errors
import json
import strutils

suite "Errors Extended":
  test "all error codes have distinct ordinals":
    # Ensure error codes are properly defined as enum
    check ord(ERR_IO) != ord(ERR_CORRUPTION)
    check ord(ERR_CORRUPTION) != ord(ERR_CONSTRAINT)
    check ord(ERR_CONSTRAINT) != ord(ERR_TRANSACTION)
    check ord(ERR_TRANSACTION) != ord(ERR_SQL)
    check ord(ERR_SQL) != ord(ERR_INTERNAL)

  test "ok with different numeric types":
    check ok(0).value == 0
    check ok(-1).value == -1
    check ok(int64.high).value == int64.high
    check ok(int64.low).value == int64.low

  test "ok with boolean":
    check ok(true).value == true
    check ok(false).value == false

  test "ok with string":
    check ok("hello").value == "hello"
    check ok("").value == ""

  test "err preserves all error information":
    let res = err[string](ERR_CONSTRAINT, "unique constraint failed", "table.users.column.id")
    check not res.ok
    check res.err.code == ERR_CONSTRAINT
    check res.err.message == "unique constraint failed"
    check res.err.context == "table.users.column.id"

  test "errorToJson with ERR_TRANSACTION":
    let err = DbError(code: ERR_TRANSACTION, message: "deadlock", context: "tx:123")
    let json = errorToJson(err)
    check json["code"].getStr() == "ERR_TRANSACTION"
    check json["message"].getStr() == "deadlock"
    check json["context"].getStr() == "tx:123"

  test "errorToJson with ERR_CONSTRAINT":
    let err = DbError(code: ERR_CONSTRAINT, message: "FK violation", context: "")
    let json = errorToJson(err)
    check json["code"].getStr() == "ERR_CONSTRAINT"
    check json["message"].getStr() == "FK violation"

  test "errorToJson with long message":
    let longMsg = "a".repeat(1000)
    let err = DbError(code: ERR_IO, message: longMsg, context: "")
    let json = errorToJson(err)
    check json["message"].getStr().len == 1000

  test "Result with empty sequence":
    let res = ok(newSeq[int]())
    check res.ok
    check res.value.len == 0

  test "Result with nested types":
    type Inner = object
      x: int
    type Outer = object
      inner: Inner
    let val = Outer(inner: Inner(x: 42))
    let res = ok(val)
    check res.value.inner.x == 42

  test "multiple error codes can be checked":
    var errors: seq[ErrorCode] = @[]
    errors.add(ERR_IO)
    errors.add(ERR_SQL)
    errors.add(ERR_INTERNAL)
    check ERR_IO in errors
    check ERR_CORRUPTION notin errors

  test "DbError default construction":
    let err = DbError()
    # Default values
    check err.message == ""
    check err.context == ""

  test "Result default construction for bool":
    let res = Result[bool]()
    check not res.ok  # Default is false
