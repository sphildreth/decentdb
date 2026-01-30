import unittest
import sql/pg_query_ffi

suite "PG Query FFI":
  test "PgQueryError type exists":
    # Just verify the type can be instantiated
    when defined(libpg_query):
      discard
    else:
      let err = PgQueryError()
      discard err

  test "PgQueryParseResult type exists":
    when defined(libpg_query):
      discard
    else:
      let res = PgQueryParseResult()
      discard res

  test "pg_query_parse exists as procedure":
    # Verify the proc is callable (stub version in non-libpg_query mode)
    when not defined(libpg_query):
      let result = pg_query_parse("SELECT 1")
      discard result

  test "pg_query_free_parse_result exists as procedure":
    # Verify the proc is callable
    when not defined(libpg_query):
      let result = PgQueryParseResult()
      pg_query_free_parse_result(result)

  test "PgQueryParseResult has parse_tree field":
    when not defined(libpg_query):
      var res = PgQueryParseResult()
      res.parse_tree = "test"
      check res.parse_tree == "test"

  test "PgQueryParseResult has error field":
    when not defined(libpg_query):
      var res = PgQueryParseResult()
      discard res.error
