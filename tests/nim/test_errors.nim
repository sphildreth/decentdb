import unittest
import errors
import json

suite "Errors":
  test "ok result construction":
    let res = ok(42)
    check res.ok == true
    check res.value == 42

  test "okVoid result construction":
    let res = okVoid()
    check res.ok == true

  test "error result construction":
    let res = err[int](ERR_IO, "disk full", "/path/to/file")
    check res.ok == false
    check res.err.code == ERR_IO
    check res.err.message == "disk full"
    check res.err.context == "/path/to/file"

  test "error result with different error codes":
    check err[string](ERR_CORRUPTION, "checksum failed").err.code == ERR_CORRUPTION
    check err[string](ERR_CONSTRAINT, "unique violation").err.code == ERR_CONSTRAINT
    check err[string](ERR_TRANSACTION, "deadlock detected").err.code == ERR_TRANSACTION
    check err[string](ERR_SQL, "syntax error").err.code == ERR_SQL
    check err[string](ERR_INTERNAL, "bug!").err.code == ERR_INTERNAL

  test "errorToJson produces valid JSON":
    let err = DbError(code: ERR_IO, message: "test error", context: "test context")
    let jsonNode = errorToJson(err)
    check jsonNode["code"].getStr() == "ERR_IO"
    check jsonNode["message"].getStr() == "test error"
    check jsonNode["context"].getStr() == "test context"

  test "errorToJson with empty context":
    let err = DbError(code: ERR_SQL, message: "syntax error", context: "")
    let jsonNode = errorToJson(err)
    check jsonNode["code"].getStr() == "ERR_SQL"
    check jsonNode["message"].getStr() == "syntax error"
    check jsonNode["context"].getStr() == ""

  test "Result with complex types":
    type MyObj = object
      x: int
      y: string
    let obj = MyObj(x: 10, y: "hello")
    let res = ok(obj)
    check res.ok == true
    check res.value.x == 10
    check res.value.y == "hello"

  test "Result with sequence":
    let seq_res = ok(@[1, 2, 3])
    check seq_res.ok == true
    check seq_res.value.len == 3
    check seq_res.value[0] == 1

  test "Result error with default context":
    let res = err[int](ERR_IO, "error message")
    check res.ok == false
    check res.err.context == ""  # Default empty context
