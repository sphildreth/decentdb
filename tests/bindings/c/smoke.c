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

static void expect_u8_eq(uint8_t actual, uint8_t expected,
                         const char *context) {
  if (actual != expected) {
    fprintf(stderr, "%s expected %u, got %u\n", context, expected, actual);
    exit(1);
  }
}

static void expect_i32_eq(int32_t actual, int32_t expected,
                          const char *context) {
  if (actual != expected) {
    fprintf(stderr, "%s expected %d, got %d\n", context, expected, actual);
    exit(1);
  }
}

static void expect_i64_eq(int64_t actual, int64_t expected,
                          const char *context) {
  if (actual != expected) {
    fprintf(stderr, "%s expected %lld, got %lld\n", context,
            (long long)expected, (long long)actual);
    exit(1);
  }
}

static void expect_tag(const ddb_value_t *value, ddb_value_tag_t expected,
                       const char *context) {
  if (value->tag != (uint32_t)expected) {
    fprintf(stderr, "%s expected tag %u, got %u\n", context,
            (uint32_t)expected, value->tag);
    exit(1);
  }
}

static void expect_bytes(const uint8_t *actual, const uint8_t *expected,
                         size_t len, const char *context) {
  if (memcmp(actual, expected, len) != 0) {
    fprintf(stderr, "%s bytes did not match\n", context);
    exit(1);
  }
}

static void expect_last_error_json(void) {
  char *json = NULL;
  check(ddb_last_error_json(&json), "last_error_json");
  if (json == NULL) {
    fprintf(stderr, "last_error_json returned NULL after error\n");
    exit(1);
  }
  if (strstr(json, "\"code_name\":\"ERR_SQL\"") == NULL ||
      strstr(json, "\"subcode\":\"sql.relation_not_found\"") == NULL ||
      strstr(json, "\"relation\":\"nope\"") == NULL) {
    fprintf(stderr, "unexpected diagnostic JSON: %s\n", json);
    exit(1);
  }
  check(ddb_string_free(&json), "free last_error_json");
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

  check(ddb_db_execute_queued(
            db, "INSERT INTO smoke (id, name) VALUES (2, 'c-queued')", NULL, 0,
            DDB_WRITE_QUEUE_TIMEOUT_DEFAULT, &result),
        "queued insert");
  check(ddb_result_free(&result), "free queued insert");
  ddb_write_queue_metrics_t queue_metrics;
  check(ddb_db_write_queue_metrics(db, &queue_metrics), "queue metrics");
  if (queue_metrics.admitted != 1 || queue_metrics.committed != 1 ||
      queue_metrics.failed != 0) {
    fprintf(stderr,
            "unexpected queue metrics: admitted=%llu committed=%llu failed=%llu\n",
            (unsigned long long)queue_metrics.admitted,
            (unsigned long long)queue_metrics.committed,
            (unsigned long long)queue_metrics.failed);
    return 1;
  }

  ddb_watch_t *watch = NULL;
  char *watch_event = NULL;
  check(ddb_db_watch_query_json(
            db, "{\"sql\":\"SELECT id, name FROM smoke ORDER BY id\"}",
            &watch),
        "watch query");
  check(ddb_watch_next_json(watch, 1000, &watch_event), "watch initial");
  if (strstr(watch_event, "\"type\":\"initial\"") == NULL ||
      strstr(watch_event, "\"rows\"") == NULL) {
    fprintf(stderr, "unexpected initial watch event: %s\n", watch_event);
    return 1;
  }
  check(ddb_string_free(&watch_event), "free watch initial");

  check(ddb_db_execute(db,
                       "INSERT INTO smoke (id, name) VALUES (3, 'c-watch')",
                       NULL, 0, &result),
        "watch insert");
  check(ddb_result_free(&result), "free watch insert");
  check(ddb_watch_next_json(watch, 1000, &watch_event), "watch invalidate");
  if (strstr(watch_event, "\"type\":\"invalidate\"") == NULL ||
      strstr(watch_event, "\"smoke\"") == NULL ||
      strstr(watch_event, "\"insert\"") == NULL) {
    fprintf(stderr, "unexpected invalidate watch event: %s\n", watch_event);
    return 1;
  }
  check(ddb_string_free(&watch_event), "free watch invalidate");
  if (ddb_watch_next_json(watch, 1, &watch_event) != DDB_ERR_TIMEOUT) {
    fprintf(stderr, "expected watch timeout after draining events\n");
    return 1;
  }
  check(ddb_watch_close(&watch), "close watch");
  if (watch != NULL) {
    fprintf(stderr, "watch close did not null the handle\n");
    return 1;
  }

  check(ddb_db_execute(db, "SELECT id, name FROM smoke", NULL, 0, &result),
        "select");
  check(ddb_result_row_count(result, &rows), "row_count");
  if (rows != 3) {
    fprintf(stderr, "expected 3 rows, got %zu\n", rows);
    return 1;
  }
  check(ddb_result_free(&result), "free select");

  char *json = NULL;
  check(ddb_db_branch_execute_json(
            db, "{\"op\":\"snapshot_create\",\"name\":\"c-snapshot\"}", &json),
        "snapshot_create json");
  if (strstr(json, "c-snapshot") == NULL) {
    fprintf(stderr, "snapshot JSON did not mention c-snapshot: %s\n", json);
    return 1;
  }
  check(ddb_string_free(&json), "free snapshot json");

  check(ddb_db_branch_execute_json(
            db,
            "{\"op\":\"branch_create\",\"name\":\"work\",\"from\":\"c-snapshot\"}",
            &json),
        "branch_create json");
  if (strstr(json, "work") == NULL) {
    fprintf(stderr, "branch JSON did not mention work: %s\n", json);
    return 1;
  }
  check(ddb_string_free(&json), "free branch json");

  check(ddb_db_branch_execute_json(db, "{\"op\":\"branch_list\"}", &json),
        "branch_list json");
  if (strstr(json, "work") == NULL || strstr(json, "main") == NULL) {
    fprintf(stderr, "branch list JSON missing expected branches: %s\n", json);
    return 1;
  }
  check(ddb_string_free(&json), "free branch list json");

  check(ddb_db_execute(
            db,
            "CREATE TABLE semantic ("
            "id INT64 PRIMARY KEY,"
            "status ENUM('new', 'paid'),"
            "host IPADDR,"
            "block CIDR,"
            "day DATE,"
            "clock TIME,"
            "observed TIMESTAMPTZ,"
            "delay INTERVAL,"
            "mac MACADDR,"
            "eui MACADDR8)",
            NULL, 0, &result),
        "create semantic");
  check(ddb_result_free(&result), "free create semantic");

  check(ddb_db_execute(
            db,
            "INSERT INTO semantic VALUES ("
            "1,"
            "'paid',"
            "'192.168.10.20',"
            "'192.168.10.99/24',"
            "'2026-05-18',"
            "'09:30:00.123456',"
            "'2026-05-18T09:10:11.123456-05:00',"
            "'1 year 2 months 3 days 4.5 seconds',"
            "'08:00:2b:01:02:03',"
            "'08:00:2b:ff:fe:01:02:03')",
            NULL, 0, &result),
        "insert semantic");
  check(ddb_result_free(&result), "free insert semantic");

  check(ddb_db_execute(
            db,
            "SELECT status, host, block, day, clock, observed, delay, mac, eui "
            "FROM semantic",
            NULL, 0, &result),
        "select semantic");
  check(ddb_result_row_count(result, &rows), "semantic row_count");
  if (rows != 1) {
    fprintf(stderr, "expected 1 semantic row, got %zu\n", rows);
    return 1;
  }

  ddb_value_t value;
  check(ddb_value_init(&value), "init copied semantic value");
  check(ddb_result_value_copy(result, 0, 0, &value), "copy enum");
  expect_tag(&value, DDB_VALUE_ENUM, "semantic enum");
  if (value.enum_type_id == 0 || value.enum_label_id != 1) {
    fprintf(stderr, "unexpected enum ids: type=%llu label=%llu\n",
            (unsigned long long)value.enum_type_id,
            (unsigned long long)value.enum_label_id);
    return 1;
  }
  check(ddb_value_dispose(&value), "dispose enum");

  const uint8_t ip_v4[] = {192, 168, 10, 20};
  check(ddb_result_value_copy(result, 0, 1, &value), "copy ipaddr");
  expect_tag(&value, DDB_VALUE_IPADDR, "semantic ipaddr");
  expect_u8_eq(value.ip_family, 4, "ipaddr family");
  expect_bytes(value.ip_cidr_addr_bytes, ip_v4, sizeof(ip_v4), "ipaddr bytes");
  check(ddb_value_dispose(&value), "dispose ipaddr");

  const uint8_t cidr_v4[] = {192, 168, 10, 0};
  check(ddb_result_value_copy(result, 0, 2, &value), "copy cidr");
  expect_tag(&value, DDB_VALUE_CIDR, "semantic cidr");
  expect_u8_eq(value.ip_family, 4, "cidr family");
  expect_u8_eq(value.cidr_prefix_len, 24, "cidr prefix");
  expect_bytes(value.ip_cidr_addr_bytes, cidr_v4, sizeof(cidr_v4),
               "cidr bytes");
  check(ddb_value_dispose(&value), "dispose cidr");

  check(ddb_result_value_copy(result, 0, 3, &value), "copy date");
  expect_tag(&value, DDB_VALUE_DATE, "semantic date");
  expect_i32_eq(value.date_days, 20591, "date days");
  check(ddb_value_dispose(&value), "dispose date");

  check(ddb_result_value_copy(result, 0, 4, &value), "copy time");
  expect_tag(&value, DDB_VALUE_TIME, "semantic time");
  expect_i64_eq(value.time_micros, 34200123456LL, "time micros");
  check(ddb_value_dispose(&value), "dispose time");

  check(ddb_result_value_copy(result, 0, 5, &value), "copy timestamptz");
  expect_tag(&value, DDB_VALUE_TIMESTAMPTZ_MICROS, "semantic timestamptz");
  expect_i64_eq(value.timestamptz_micros, 1779113411123456LL,
                "timestamptz micros");
  check(ddb_value_dispose(&value), "dispose timestamptz");

  check(ddb_result_value_copy(result, 0, 6, &value), "copy interval");
  expect_tag(&value, DDB_VALUE_INTERVAL, "semantic interval");
  expect_i32_eq(value.interval_months, 14, "interval months");
  expect_i32_eq(value.interval_days, 3, "interval days");
  expect_i64_eq(value.interval_micros, 4500000LL, "interval micros");
  check(ddb_value_dispose(&value), "dispose interval");

  const uint8_t mac48[] = {0x08, 0x00, 0x2b, 0x01, 0x02, 0x03};
  check(ddb_result_value_copy(result, 0, 7, &value), "copy macaddr");
  expect_tag(&value, DDB_VALUE_MACADDR, "semantic macaddr");
  expect_u8_eq(value.ip_family, 6, "macaddr length");
  expect_bytes(value.ip_cidr_addr_bytes, mac48, sizeof(mac48), "macaddr bytes");
  check(ddb_value_dispose(&value), "dispose macaddr");

  const uint8_t mac64[] = {0x08, 0x00, 0x2b, 0xff,
                           0xfe, 0x01, 0x02, 0x03};
  check(ddb_result_value_copy(result, 0, 8, &value), "copy macaddr8");
  expect_tag(&value, DDB_VALUE_MACADDR, "semantic macaddr8");
  expect_u8_eq(value.ip_family, 8, "macaddr8 length");
  expect_bytes(value.ip_cidr_addr_bytes, mac64, sizeof(mac64),
               "macaddr8 bytes");
  check(ddb_value_dispose(&value), "dispose macaddr8");
  check(ddb_result_free(&result), "free semantic select");

  if (ddb_db_execute(db, "SELECT * FROM nope", NULL, 0, &result) !=
      DDB_ERR_SQL) {
    fprintf(stderr, "expected SQL error for missing table\n");
    return 1;
  }
  if (strstr(ddb_last_error_message(), "nope") == NULL) {
    fprintf(stderr, "missing table error message did not mention table name\n");
    return 1;
  }
  expect_last_error_json();

  check(ddb_db_free(&db), "free db");
  return 0;
}
