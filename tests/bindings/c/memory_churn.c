#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "decentdb.h"

static void check(uint32_t code, const char *what) {
    if (code != DDB_OK) {
        const char *msg = ddb_last_error_message();
        fprintf(stderr, "%s failed (%u): %s\n", what, code, msg ? msg : "<no error>");
        exit(1);
    }
}

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1] : "/tmp/decentdb_memory_churn.ddb";
    int iterations = argc > 2 ? atoi(argv[2]) : 200;

    remove(path);
    char wal_path[1024];
    snprintf(wal_path, sizeof(wal_path), "%s.wal", path);
    remove(wal_path);

    ddb_db_t *db = NULL;
    ddb_result_t *result = NULL;
    uint64_t lsn = 0;

    check(ddb_db_open_or_create(path, &db), "open seed");
    check(
        ddb_db_execute(db, "CREATE TABLE t(id INT64, data TEXT)", NULL, 0, &result),
        "create table"
    );
    check(ddb_result_free(&result), "free create result");
    check(ddb_db_begin_transaction(db), "begin tx");

    char payload[1001];
    memset(payload, 'a', 1000);
    payload[1000] = '\0';
    char sql[1400];
    for (int i = 0; i < 1000; i++) {
        snprintf(sql, sizeof(sql), "INSERT INTO t VALUES (%d, '%s')", i, payload);
        check(ddb_db_execute(db, sql, NULL, 0, &result), "insert");
        check(ddb_result_free(&result), "free insert result");
    }
    check(ddb_db_commit_transaction(db, &lsn), "commit tx");
    check(ddb_db_free(&db), "free seed db");

    for (int i = 0; i < iterations; i++) {
        check(ddb_db_open_or_create(path, &db), "open loop db");
        check(ddb_db_execute(db, "SELECT COUNT(*) FROM t", NULL, 0, &result), "select count");
        check(ddb_result_free(&result), "free select result");
        check(ddb_db_free(&db), "free loop db");
    }

    return 0;
}
