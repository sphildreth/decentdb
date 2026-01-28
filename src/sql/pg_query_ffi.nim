when defined(libpg_query):
  type PgQueryError* {.importc, header: "pg_query.h".} = object
    message*: cstring
    funcname*: cstring
    filename*: cstring
    lineno*: cint
    cursorpos*: cint
    context*: cstring

  type PgQueryParseResult* {.importc, header: "pg_query.h".} = object
    parse_tree*: cstring
    stderr_buffer*: cstring
    error*: PgQueryError

  proc pg_query_parse*(input: cstring): PgQueryParseResult {.importc, header: "pg_query.h".}
  proc pg_query_free_parse_result*(result: PgQueryParseResult) {.importc, header: "pg_query.h".}
else:
  type PgQueryError* = object
  type PgQueryParseResult* = object
    parse_tree*: cstring
    error*: PgQueryError
  proc pg_query_parse*(input: cstring): PgQueryParseResult = discard
  proc pg_query_free_parse_result*(result: PgQueryParseResult) = discard
