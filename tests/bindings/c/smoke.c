#include "decentdb.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void check(ddb_status_t status, const char *context) {
  if (status != DDB_OK) {
    const char *error = ddb_last_error_message();
    fprintf(stderr, "%s failed with status %u: %s\n", context, status,
            error == NULL ? "<null>" : error);
    exit(1);
  }
}

int main(void) {
  ddb_db_t *db = NULL;
  ddb_result_t *result = NULL;
  size_t rows = 0;

  check(ddb_db_open_or_create(":memory:", &db), "open_or_create");
  check(ddb_db_execute(db,
                       "CREATE TABLE smoke (id INT64 PRIMARY KEY, name TEXT)",
                       NULL, 0, &result),
        "create");
  check(ddb_result_free(&result), "free create");

  check(ddb_db_execute(db,
                       "INSERT INTO smoke (id, name) VALUES (1, 'c-smoke')",
                       NULL, 0, &result),
        "insert");
  check(ddb_result_free(&result), "free insert");

  check(ddb_db_execute(db, "SELECT id, name FROM smoke", NULL, 0, &result),
        "select");
  check(ddb_result_row_count(result, &rows), "row_count");
  if (rows != 1) {
    fprintf(stderr, "expected 1 row, got %zu\n", rows);
    return 1;
  }
  check(ddb_result_free(&result), "free select");

  if (ddb_db_execute(db, "SELECT * FROM nope", NULL, 0, &result) !=
      DDB_ERR_SQL) {
    fprintf(stderr, "expected SQL error for missing table\n");
    return 1;
  }
  if (strstr(ddb_last_error_message(), "nope") == NULL) {
    fprintf(stderr, "missing table error message did not mention table name\n");
    return 1;
  }

  check(ddb_db_free(&db), "free db");
  return 0;
}
