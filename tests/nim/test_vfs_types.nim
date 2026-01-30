import unittest
import vfs/types
import errors
import strutils

suite "VFS Types":
  test "base VFS open returns error":
    let vfs = Vfs()
    let res = vfs.open("test.db", fmReadWrite, true)
    check not res.ok
    check res.err.code == ERR_INTERNAL
    check "not implemented" in res.err.message.toLowerAscii

  test "ErrorCode enum values exist":
    check ord(ERR_IO) >= 0
    check ord(ERR_CORRUPTION) >= 0
    check ord(ERR_CONSTRAINT) >= 0
    check ord(ERR_TRANSACTION) >= 0
    check ord(ERR_SQL) >= 0
    check ord(ERR_INTERNAL) >= 0

  test "DbError fields accessible":
    var err = DbError(code: ERR_IO, message: "test", context: "ctx")
    check err.code == ERR_IO
    check err.message == "test"
    check err.context == "ctx"

  test "Result[T] fields accessible":
    var res = Result[int](ok: true, value: 42)
    check res.ok == true
    check res.value == 42
    
    var resErr = Result[int](ok: false, err: DbError(code: ERR_IO, message: "err"))
    check resErr.ok == false
    check resErr.err.code == ERR_IO

  test "Void type exists":
    let v = Void()
    discard v
