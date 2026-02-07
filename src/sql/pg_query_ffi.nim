when defined(libpg_query):
  const PgQueryLib = "libpg_query.so"

  type PgQueryError* {.bycopy.} = object
    message*: cstring
    funcname*: cstring
    filename*: cstring
    lineno*: cint
    cursorpos*: cint
    context*: cstring

  type PgQueryParseResult* {.bycopy.} = object
    parse_tree*: cstring
    stderr_buffer*: cstring
    error*: ptr PgQueryError

  proc pg_query_parse*(input: cstring): PgQueryParseResult {.importc, dynlib: PgQueryLib, cdecl.}
  proc pg_query_free_parse_result*(result: PgQueryParseResult) {.importc, dynlib: PgQueryLib, cdecl.}
else:
  type PgQueryError* = object
  type PgQueryParseResult* = object
    parse_tree*: cstring
    error*: PgQueryError
  proc pg_query_parse*(input: cstring): PgQueryParseResult = discard
  proc pg_query_free_parse_result*(result: PgQueryParseResult) = discard
