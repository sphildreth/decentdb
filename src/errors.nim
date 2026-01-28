import json

type ErrorCode* = enum
  ERR_IO
  ERR_CORRUPTION
  ERR_CONSTRAINT
  ERR_TRANSACTION
  ERR_SQL
  ERR_INTERNAL

type DbError* = object
  code*: ErrorCode
  message*: string
  context*: string

type Result*[T] = object
  ok*: bool
  value*: T
  err*: DbError

type Void* = object

proc ok*[T](value: T): Result[T] =
  Result[T](ok: true, value: value)

proc okVoid*(): Result[Void] =
  ok(Void())

proc err*[T](code: ErrorCode, message: string, context: string = ""): Result[T] =
  Result[T](ok: false, err: DbError(code: code, message: message, context: context))

proc errorToJson*(e: DbError): JsonNode =
  %*{
    "code": $e.code,
    "message": e.message,
    "context": e.context
  }
