/*
 * DecentDB JNI Bridge (ddb_* ABI)
 */

#include <jni.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "../../../include/decentdb.h"

#if defined(_MSC_VER)
#define DDB_THREAD_LOCAL __declspec(thread)
#elif defined(__GNUC__) || defined(__clang__)
#define DDB_THREAD_LOCAL __thread
#else
#define DDB_THREAD_LOCAL _Thread_local
#endif

static int map_status_to_legacy_code(ddb_status_t status) {
    switch (status) {
        case DDB_OK: return 0;
        case DDB_ERR_IO: return 1;
        case DDB_ERR_CORRUPTION: return 2;
        case DDB_ERR_CONSTRAINT: return 3;
        case DDB_ERR_TRANSACTION: return 4;
        case DDB_ERR_SQL: return 5;
        case DDB_ERR_INTERNAL: return 6;
        case DDB_ERR_PANIC: return 7;
        case DDB_ERR_UNSUPPORTED_FORMAT_VERSION: return 8;
        case DDB_ERR_BUSY: return 9;
        case DDB_ERR_TIMEOUT: return 10;
        case DDB_ERR_CANCELED: return 11;
        case DDB_ERR_QUEUE_FULL: return 12;
        case DDB_ERR_QUEUE_CLOSED: return 13;
        default: return 6;
    }
}

/* Cached last status for callers passing dbHandle=0. */
static DDB_THREAD_LOCAL int g_last_code = 0;

typedef struct {
    jlong stmt_handle;
    const ddb_value_view_t *values;
    size_t cols;
} row_cache_t;

static DDB_THREAD_LOCAL row_cache_t g_row_cache = {0, NULL, 0};

static void set_last_status(ddb_status_t status) {
    g_last_code = map_status_to_legacy_code(status);
}

static void clear_row_cache(void) {
    g_row_cache.stmt_handle = 0;
    g_row_cache.values = NULL;
    g_row_cache.cols = 0;
}

static void clear_row_cache_for_stmt(jlong stmt_handle) {
    if (g_row_cache.stmt_handle == stmt_handle) {
        clear_row_cache();
    }
}

static void cache_row_view(jlong stmt_handle, const ddb_value_view_t *values, size_t cols) {
    g_row_cache.stmt_handle = stmt_handle;
    g_row_cache.values = values;
    g_row_cache.cols = cols;
}

static void civil_from_days(int64_t days, int64_t *out_year, int64_t *out_month, int64_t *out_day) {
    int64_t z = days + 719468;
    int64_t era = z >= 0 ? z : z - 146096;
    era /= 146097;
    int64_t doe = z - era * 146097;
    int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t y = yoe + era * 400;
    int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int64_t mp = (5 * doy + 2) / 153;
    int64_t day = doy - (153 * mp + 2) / 5 + 1;
    int64_t month = mp + (mp < 10 ? 3 : -9);
    int64_t year = y + (month <= 2 ? 1 : 0);
    *out_year = year;
    *out_month = month;
    *out_day = day;
}

static int format_year(int64_t year, char *out, size_t out_len) {
    int written = (year >= 0 && year <= 9999)
        ? snprintf(out, out_len, "%04lld", (long long)year)
        : snprintf(out, out_len, "%lld", (long long)year);
    return written < 0 ? -1 : 0;
}

static int format_date_days(int32_t days, char *out, size_t out_len) {
    int64_t year = 0, month = 0, day = 0;
    char year_buf[32];
    civil_from_days((int64_t)days, &year, &month, &day);
    if (format_year(year, year_buf, sizeof(year_buf)) != 0) return -1;
    return snprintf(out, out_len, "%s-%02lld-%02lld", year_buf, (long long)month, (long long)day) < 0 ? -1 : 0;
}

static int format_time_of_day(int64_t micros, char *out, size_t out_len) {
    int64_t hour = micros / 3600000000LL;
    int64_t minute = (micros % 3600000000LL) / 60000000LL;
    int64_t second = (micros % 60000000LL) / 1000000LL;
    int64_t fraction = micros % 1000000LL;
    return snprintf(
        out,
        out_len,
        "%02lld:%02lld:%02lld.%06lld",
        (long long)hour,
        (long long)minute,
        (long long)second,
        (long long)fraction) < 0 ? -1 : 0;
}

static int format_time_micros(int64_t micros, char *out, size_t out_len) {
    if (micros < 0 || micros >= 86400000000LL) return -1;
    return format_time_of_day(micros, out, out_len);
}

static int format_timestamp_tz_micros(int64_t micros, char *out, size_t out_len) {
    int64_t days = micros / 86400000000LL;
    int64_t time = micros % 86400000000LL;
    if (time < 0) {
        time += 86400000000LL;
        days -= 1;
    }

    char date_buf[32];
    char time_buf[32];
    if (format_date_days((int32_t)days, date_buf, sizeof(date_buf)) != 0) return -1;
    if (format_time_of_day(time, time_buf, sizeof(time_buf)) != 0) return -1;
    return snprintf(out, out_len, "%sT%sZ", date_buf, time_buf) < 0 ? -1 : 0;
}

static int format_interval(int32_t months, int32_t days, int64_t micros, char *out, size_t out_len) {
    return snprintf(out, out_len, "%d %d %lld", months, days, (long long)micros) < 0 ? -1 : 0;
}

static int format_ipv6(const uint8_t addr[16], char *out, size_t out_len) {
    uint16_t words[8];
    for (size_t i = 0; i < 8; i++) {
        words[i] = (uint16_t)((uint16_t)addr[i * 2] << 8) | (uint16_t)addr[i * 2 + 1];
    }

    int best_start = -1;
    int best_len = 0;
    for (int i = 0; i < 8;) {
        if (words[i] == 0) {
            int j = i;
            while (j < 8 && words[j] == 0) j++;
            int len = j - i;
            if (len > best_len && len >= 2) {
                best_start = i;
                best_len = len;
            }
            i = j;
        } else {
            i++;
        }
    }

    size_t pos = 0;
    int need_sep = 0;
    for (int i = 0; i < 8;) {
        if (i == best_start) {
            if (need_sep) {
                if (pos + 1 >= out_len) return -1;
                out[pos++] = ':';
            }
            if (pos + 1 >= out_len) return -1;
            out[pos++] = ':';
            need_sep = 0;
            i += best_len;
            if (i >= 8) break;
            continue;
        }

        if (need_sep) {
            if (pos + 1 >= out_len) return -1;
            out[pos++] = ':';
        }

        int written = snprintf(out + pos, out_len - pos, "%x", words[i]);
        if (written < 0 || (size_t)written >= out_len - pos) return -1;
        pos += (size_t)written;
        need_sep = 1;
        i++;
    }

    if (pos >= out_len) return -1;
    out[pos] = '\0';
    return 0;
}

static int format_ip_addr(uint8_t family, const uint8_t addr[16], char *out, size_t out_len) {
    if (family == 4) {
        return snprintf(out, out_len, "%u.%u.%u.%u", addr[0], addr[1], addr[2], addr[3]) < 0 ? -1 : 0;
    }
    if (family == 6) {
        return format_ipv6(addr, out, out_len);
    }
    return -1;
}

static int format_cidr(uint8_t family, uint8_t prefix_len, const uint8_t addr[16], char *out, size_t out_len) {
    char ip_buf[64];
    if (format_ip_addr(family, addr, ip_buf, sizeof(ip_buf)) != 0) return -1;
    return snprintf(out, out_len, "%s/%u", ip_buf, (unsigned)prefix_len) < 0 ? -1 : 0;
}

static int format_macaddr(uint8_t len, const uint8_t addr[16], char *out, size_t out_len) {
    if (len != 6 && len != 8) return -1;
    size_t needed = (size_t)len * 3;
    if (out_len < needed) return -1;
    size_t pos = 0;
    for (uint8_t i = 0; i < len; i++) {
        int written = snprintf(out + pos, out_len - pos, i == 0 ? "%02x" : ":%02x", addr[i]);
        if (written < 0) return -1;
        pos += (size_t)written;
    }
    return 0;
}

static int format_enum(uint64_t type_id, uint64_t label_id, char *out, size_t out_len) {
    return snprintf(out, out_len, "%llu:%llu", (unsigned long long)type_id, (unsigned long long)label_id) < 0 ? -1 : 0;
}

static char *jstring_to_cstr(JNIEnv *env, jstring js) {
    if (js == NULL) return NULL;
    const char *utf = (*env)->GetStringUTFChars(env, js, NULL);
    if (utf == NULL) return NULL;
    char *copy = strdup(utf);
    (*env)->ReleaseStringUTFChars(env, js, utf);
    return copy;
}

static jstring cstr_to_jstring(JNIEnv *env, const char *s) {
    if (s == NULL) return NULL;
    return (*env)->NewStringUTF(env, s);
}

static ddb_db_t *as_db(jlong handle) {
    return (ddb_db_t *)(uintptr_t)handle;
}

static ddb_stmt_t *as_stmt(jlong handle) {
    return (ddb_stmt_t *)(uintptr_t)handle;
}

static int parse_open_mode(const char *opts) {
    if (opts == NULL || opts[0] == '\0') return 0; /* open_or_create */

    const char *cursor = opts;
    while (*cursor != '\0') {
        const char *entry_end = strchr(cursor, '&');
        size_t entry_len = entry_end == NULL ? strlen(cursor) : (size_t)(entry_end - cursor);
        if (entry_len >= 5 && strncmp(cursor, "mode=", 5) == 0) {
            const char *value = cursor + 5;
            size_t value_len = entry_len - 5;
            if (value_len == 6 && strncmp(value, "create", 6) == 0) return 1;
            if (value_len == 4 && strncmp(value, "open", 4) == 0) return 2;
        }
        if (entry_end == NULL) break;
        cursor = entry_end + 1;
    }

    return 0;
}

static char *native_options_without_mode(const char *opts) {
    if (opts == NULL || opts[0] == '\0') return NULL;
    size_t len = strlen(opts);
    char *out = (char *)malloc(len + 1);
    if (out == NULL) return NULL;
    size_t out_len = 0;

    const char *cursor = opts;
    while (*cursor != '\0') {
        while (*cursor == '&' || *cursor == ';' || *cursor == ',' || *cursor == ' ' ||
               *cursor == '\t' || *cursor == '\r' || *cursor == '\n') {
            cursor++;
        }
        if (*cursor == '\0') break;

        const char *start = cursor;
        while (*cursor != '\0' && *cursor != '&' && *cursor != ';' && *cursor != ',' &&
               *cursor != ' ' && *cursor != '\t' && *cursor != '\r' && *cursor != '\n') {
            cursor++;
        }
        size_t entry_len = (size_t)(cursor - start);
        if (entry_len >= 5 && strncmp(start, "mode=", 5) == 0) continue;

        if (out_len != 0) out[out_len++] = ';';
        memcpy(out + out_len, start, entry_len);
        out_len += entry_len;
    }

    out[out_len] = '\0';
    if (out_len == 0) {
        free(out);
        return NULL;
    }
    return out;
}

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbOpen(JNIEnv *env, jclass cls,
    jstring jpath, jstring joptions)
{
    (void)cls;
    char *path = jstring_to_cstr(env, jpath);
    char *opts = jstring_to_cstr(env, joptions);
    if (path == NULL) {
        free(opts);
        set_last_status(DDB_ERR_INTERNAL);
        return 0;
    }

    ddb_db_t *db = NULL;
    ddb_status_t status = DDB_ERR_INTERNAL;
    int mode = parse_open_mode(opts);
    char *native_opts = native_options_without_mode(opts);
    if (native_opts != NULL && native_opts[0] != '\0') {
        if (mode == 1) {
            status = ddb_db_create_with_options(path, native_opts, &db);
        } else if (mode == 2) {
            status = ddb_db_open_with_options(path, native_opts, &db);
        } else {
            status = ddb_db_open_or_create_with_options(path, native_opts, &db);
        }
    } else if (mode == 1) {
        status = ddb_db_create(path, &db);
    } else if (mode == 2) {
        status = ddb_db_open(path, &db);
    } else {
        status = ddb_db_open_or_create(path, &db);
    }
    set_last_status(status);

    free(path);
    free(opts);
    free(native_opts);
    clear_row_cache();

    if (status != DDB_OK || db == NULL) return 0;
    return (jlong)(uintptr_t)db;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbClose(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    if (handle == 0) return 0;
    ddb_db_t *db = as_db(handle);
    ddb_status_t status = ddb_db_free(&db);
    set_last_status(status);
    clear_row_cache();
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbInTransaction(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    if (handle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    uint8_t in_transaction = 0;
    ddb_status_t status = ddb_db_in_transaction(as_db(handle), &in_transaction);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return in_transaction ? 1 : 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbCheckpoint(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    if (handle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    ddb_status_t status = ddb_db_checkpoint(as_db(handle));
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbBeginTransaction(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    if (handle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    ddb_status_t status = ddb_db_begin_transaction(as_db(handle));
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbCommitTransaction(
    JNIEnv *env, jclass cls, jlong handle, jlongArray outLsn)
{
    (void)cls;
    if (handle == 0 || outLsn == NULL || (*env)->GetArrayLength(env, outLsn) < 1) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    uint64_t lsn = 0;
    ddb_status_t status = ddb_db_commit_transaction(as_db(handle), &lsn);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    jlong value = (jlong)lsn;
    (*env)->SetLongArrayRegion(env, outLsn, 0, 1, &value);
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbRollbackTransaction(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    if (handle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    ddb_status_t status = ddb_db_rollback_transaction(as_db(handle));
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbSaveAs(JNIEnv *env, jclass cls, jlong handle, jstring jdest)
{
    (void)cls;
    if (handle == 0 || jdest == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    char *dest = jstring_to_cstr(env, jdest);
    if (dest == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    ddb_status_t status = ddb_db_save_as(as_db(handle), dest);
    free(dest);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbExecuteImmediate(
    JNIEnv *env, jclass cls, jlong handle, jstring jsql, jlongArray outAffected)
{
    (void)cls;
    if (handle == 0 || jsql == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    char *sql = jstring_to_cstr(env, jsql);
    if (sql == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    ddb_result_t *result = NULL;
    ddb_status_t status = ddb_db_execute(as_db(handle), sql, NULL, 0, &result);
    free(sql);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    uint64_t affected = 0;
    if (result != NULL) {
        status = ddb_result_affected_rows(result, &affected);
        if (status == DDB_OK && outAffected != NULL && (*env)->GetArrayLength(env, outAffected) >= 1) {
            jlong value = (jlong)affected;
            (*env)->SetLongArrayRegion(env, outAffected, 0, 1, &value);
        }
        ddb_result_free(&result);
        set_last_status(status);
        if (status != DDB_OK) return -1;
    }
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbLastErrorCode(JNIEnv *env, jclass cls, jlong handle)
{
    (void)env;
    (void)cls;
    (void)handle;
    return (jint)g_last_code;
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbLastErrorMessage(JNIEnv *env, jclass cls, jlong handle)
{
    (void)cls;
    (void)handle;
    const char *msg = ddb_last_error_message();
    return cstr_to_jstring(env, msg);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_abiVersion(JNIEnv *env, jclass cls)
{
    (void)env;
    (void)cls;
    return (jint)ddb_abi_version();
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_engineVersion(JNIEnv *env, jclass cls)
{
    (void)cls;
    return cstr_to_jstring(env, ddb_version());
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtPrepare(JNIEnv *env, jclass cls,
    jlong dbHandle, jstring jsql, jlongArray outStmt)
{
    (void)cls;
    if (dbHandle == 0 || jsql == NULL || outStmt == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    char *sql = jstring_to_cstr(env, jsql);
    if (sql == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    ddb_stmt_t *stmt = NULL;
    ddb_status_t status = ddb_db_prepare(as_db(dbHandle), sql, &stmt);
    set_last_status(status);
    free(sql);
    clear_row_cache();
    if (status != DDB_OK || stmt == NULL) return -1;
    jlong stmtHandle = (jlong)(uintptr_t)stmt;
    (*env)->SetLongArrayRegion(env, outStmt, 0, 1, &stmtHandle);
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtStep(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    (void)env;
    (void)cls;
    if (stmtHandle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    clear_row_cache_for_stmt(stmtHandle);
    uint8_t has_row = 0;
    ddb_status_t status = ddb_stmt_step(as_stmt(stmtHandle), &has_row);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return has_row ? 1 : 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtStepRowView(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    (void)env;
    (void)cls;
    if (stmtHandle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    const ddb_value_view_t *values = NULL;
    size_t cols = 0;
    uint8_t has_row = 0;
    ddb_status_t status = ddb_stmt_step_row_view(as_stmt(stmtHandle), &values, &cols, &has_row);
    set_last_status(status);
    if (status != DDB_OK) {
        clear_row_cache_for_stmt(stmtHandle);
        return -1;
    }
    if (has_row) {
        cache_row_view(stmtHandle, values, cols);
        return 1;
    }
    clear_row_cache_for_stmt(stmtHandle);
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtBindInt64StepRowView(
    JNIEnv *env, jclass cls, jlong stmtHandle, jint col, jlong value)
{
    (void)env;
    (void)cls;
    if (stmtHandle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    const ddb_value_view_t *values = NULL;
    size_t cols = 0;
    uint8_t has_row = 0;
    ddb_status_t status = ddb_stmt_bind_int64_step_row_view(
        as_stmt(stmtHandle),
        (size_t)col,
        (int64_t)value,
        &values,
        &cols,
        &has_row);
    set_last_status(status);
    if (status != DDB_OK) {
        clear_row_cache_for_stmt(stmtHandle);
        return -1;
    }
    if (has_row) {
        cache_row_view(stmtHandle, values, cols);
        return 1;
    }
    clear_row_cache_for_stmt(stmtHandle);
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtReset(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    (void)env;
    (void)cls;
    if (stmtHandle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    clear_row_cache_for_stmt(stmtHandle);
    ddb_status_t status = ddb_stmt_reset(as_stmt(stmtHandle));
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtClearBindings(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    (void)env;
    (void)cls;
    if (stmtHandle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    clear_row_cache_for_stmt(stmtHandle);
    ddb_status_t status = ddb_stmt_clear_bindings(as_stmt(stmtHandle));
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT void JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtFinalize(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    (void)env;
    (void)cls;
    if (stmtHandle == 0) return;
    clear_row_cache_for_stmt(stmtHandle);
    ddb_stmt_t *stmt = as_stmt(stmtHandle);
    ddb_status_t status = ddb_stmt_free(&stmt);
    set_last_status(status);
}

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtRowsAffected(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    (void)env;
    (void)cls;
    if (stmtHandle == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return 0;
    }
    uint64_t rows = 0;
    ddb_status_t status = ddb_stmt_affected_rows(as_stmt(stmtHandle), &rows);
    set_last_status(status);
    if (status != DDB_OK) return 0;
    return (jlong)rows;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindNull(JNIEnv *env, jclass cls, jlong s, jint col)
{
    (void)env;
    (void)cls;
    clear_row_cache_for_stmt(s);
    ddb_status_t status = ddb_stmt_bind_null(as_stmt(s), (size_t)col);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindInt64(JNIEnv *env, jclass cls, jlong s, jint col, jlong val)
{
    (void)env;
    (void)cls;
    clear_row_cache_for_stmt(s);
    ddb_status_t status = ddb_stmt_bind_int64(as_stmt(s), (size_t)col, (int64_t)val);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindFloat64(JNIEnv *env, jclass cls, jlong s, jint col, jdouble val)
{
    (void)env;
    (void)cls;
    ddb_status_t status = ddb_stmt_bind_float64(as_stmt(s), (size_t)col, (double)val);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindBool(JNIEnv *env, jclass cls, jlong s, jint col, jboolean val)
{
    (void)env;
    (void)cls;
    clear_row_cache_for_stmt(s);
    ddb_status_t status = ddb_stmt_bind_bool(as_stmt(s), (size_t)col, val == JNI_FALSE ? 0 : 1);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindText(JNIEnv *env, jclass cls, jlong s, jint col, jstring jval)
{
    (void)cls;
    if (jval == NULL) {
        clear_row_cache_for_stmt(s);
        ddb_status_t status = ddb_stmt_bind_null(as_stmt(s), (size_t)col);
        set_last_status(status);
        if (status != DDB_OK) return -1;
        return 0;
    }
    const char *utf = (*env)->GetStringUTFChars(env, jval, NULL);
    jsize len = (*env)->GetStringUTFLength(env, jval);
    clear_row_cache_for_stmt(s);
    ddb_status_t status = ddb_stmt_bind_text(as_stmt(s), (size_t)col, utf, (size_t)len);
    (*env)->ReleaseStringUTFChars(env, jval, utf);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindBlob(JNIEnv *env, jclass cls, jlong s, jint col, jbyteArray jdata)
{
    (void)cls;
    if (jdata == NULL) {
        clear_row_cache_for_stmt(s);
        ddb_status_t status = ddb_stmt_bind_null(as_stmt(s), (size_t)col);
        set_last_status(status);
        if (status != DDB_OK) return -1;
        return 0;
    }
    jsize len = (*env)->GetArrayLength(env, jdata);
    jbyte *buf = (*env)->GetPrimitiveArrayCritical(env, jdata, NULL);
    if (buf == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    clear_row_cache_for_stmt(s);
    ddb_status_t status = ddb_stmt_bind_blob(as_stmt(s), (size_t)col, (const uint8_t *)buf, (size_t)len);
    (*env)->ReleasePrimitiveArrayCritical(env, jdata, buf, JNI_ABORT);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindDecimal(
    JNIEnv *env, jclass cls, jlong s, jint col, jlong unscaled, jint scale)
{
    (void)env;
    (void)cls;
    clear_row_cache_for_stmt(s);
    ddb_status_t status = ddb_stmt_bind_decimal(as_stmt(s), (size_t)col, (int64_t)unscaled, (uint8_t)scale);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindDatetime(JNIEnv *env, jclass cls, jlong s, jint col, jlong micros_utc)
{
    (void)env;
    (void)cls;
    clear_row_cache_for_stmt(s);
    ddb_status_t status = ddb_stmt_bind_timestamp_micros(as_stmt(s), (size_t)col, (int64_t)micros_utc);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

static int set_out_affected(JNIEnv *env, jlongArray outAffected, uint64_t affected) {
    if (outAffected == NULL || (*env)->GetArrayLength(env, outAffected) < 1) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    jlong value = (jlong)affected;
    (*env)->SetLongArrayRegion(env, outAffected, 0, 1, &value);
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtExecuteBatchI64(
    JNIEnv *env, jclass cls, jlong s, jlongArray values, jlongArray outAffected)
{
    (void)cls;
    if (s == 0 || values == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    jsize row_count = (*env)->GetArrayLength(env, values);
    if (row_count == 0) {
        return set_out_affected(env, outAffected, 0);
    }
    jlong *java_values = (*env)->GetLongArrayElements(env, values, NULL);
    if (java_values == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    clear_row_cache_for_stmt(s);
    uint64_t affected = 0;
    ddb_status_t status = ddb_stmt_execute_batch_i64(
        as_stmt(s),
        (size_t)row_count,
        (const int64_t *)java_values,
        &affected);
    (*env)->ReleaseLongArrayElements(env, values, java_values, JNI_ABORT);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return set_out_affected(env, outAffected, affected);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtExecuteBatchI64TextF64(
    JNIEnv *env, jclass cls, jlong s, jlongArray valuesI64, jobjectArray valuesText,
    jdoubleArray valuesF64, jlongArray outAffected)
{
    (void)cls;
    if (s == 0 || valuesI64 == NULL || valuesText == NULL || valuesF64 == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    jsize row_count = (*env)->GetArrayLength(env, valuesI64);
    if ((*env)->GetArrayLength(env, valuesText) != row_count || (*env)->GetArrayLength(env, valuesF64) != row_count) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    if (row_count == 0) {
        return set_out_affected(env, outAffected, 0);
    }

    jlong *java_i64 = (*env)->GetLongArrayElements(env, valuesI64, NULL);
    jdouble *java_f64 = (*env)->GetDoubleArrayElements(env, valuesF64, NULL);
    char **text_ptrs = (char **)calloc((size_t)row_count, sizeof(char *));
    size_t *text_lens = (size_t *)calloc((size_t)row_count, sizeof(size_t));
    jstring *strings = (jstring *)calloc((size_t)row_count, sizeof(jstring));
    if (java_i64 == NULL || java_f64 == NULL || text_ptrs == NULL || text_lens == NULL || strings == NULL) {
        free(text_ptrs);
        free(text_lens);
        free(strings);
        if (java_i64 != NULL) (*env)->ReleaseLongArrayElements(env, valuesI64, java_i64, JNI_ABORT);
        if (java_f64 != NULL) (*env)->ReleaseDoubleArrayElements(env, valuesF64, java_f64, JNI_ABORT);
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }

    int rc = 0;
    for (jsize i = 0; i < row_count; i++) {
        jstring value = (jstring)(*env)->GetObjectArrayElement(env, valuesText, i);
        if (value == NULL) {
            set_last_status(DDB_ERR_INTERNAL);
            rc = -1;
            break;
        }
        strings[i] = value;
        text_ptrs[i] = (char *)(*env)->GetStringUTFChars(env, value, NULL);
        if (text_ptrs[i] == NULL) {
            set_last_status(DDB_ERR_INTERNAL);
            rc = -1;
            break;
        }
        text_lens[i] = (size_t)(*env)->GetStringUTFLength(env, value);
    }

    if (rc == 0) {
        clear_row_cache_for_stmt(s);
        uint64_t affected = 0;
        ddb_status_t status = ddb_stmt_execute_batch_i64_text_f64(
            as_stmt(s),
            (size_t)row_count,
            (const int64_t *)java_i64,
            (const char *const *)text_ptrs,
            text_lens,
            (const double *)java_f64,
            &affected);
        set_last_status(status);
        rc = status == DDB_OK ? set_out_affected(env, outAffected, affected) : -1;
    }

    for (jsize i = 0; i < row_count; i++) {
        if (text_ptrs[i] != NULL && strings[i] != NULL) {
            (*env)->ReleaseStringUTFChars(env, strings[i], text_ptrs[i]);
        }
        if (strings[i] != NULL) {
            (*env)->DeleteLocalRef(env, strings[i]);
        }
    }
    free(strings);
    free(text_ptrs);
    free(text_lens);
    (*env)->ReleaseLongArrayElements(env, valuesI64, java_i64, JNI_ABORT);
    (*env)->ReleaseDoubleArrayElements(env, valuesF64, java_f64, JNI_ABORT);
    return rc;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtExecuteBatchTyped(
    JNIEnv *env, jclass cls, jlong s, jstring jsignature, jlongArray valuesI64,
    jdoubleArray valuesF64, jobjectArray valuesText, jlongArray outAffected)
{
    (void)cls;
    if (s == 0 || jsignature == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    const char *signature = (*env)->GetStringUTFChars(env, jsignature, NULL);
    if (signature == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    jsize sig_len = (*env)->GetStringUTFLength(env, jsignature);
    if (sig_len <= 0) {
        (*env)->ReleaseStringUTFChars(env, jsignature, signature);
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }

    int i64_per_row = 0;
    int f64_per_row = 0;
    int text_per_row = 0;
    for (jsize i = 0; i < sig_len; i++) {
        switch ((unsigned char)signature[i]) {
            case 'i': i64_per_row++; break;
            case 'f': f64_per_row++; break;
            case 't': text_per_row++; break;
            default:
                (*env)->ReleaseStringUTFChars(env, jsignature, signature);
                set_last_status(DDB_ERR_INTERNAL);
                return -1;
        }
    }

    jsize row_count = 0;
    if (i64_per_row > 0) {
        if (valuesI64 == NULL) {
            (*env)->ReleaseStringUTFChars(env, jsignature, signature);
            set_last_status(DDB_ERR_INTERNAL);
            return -1;
        }
        jsize len = (*env)->GetArrayLength(env, valuesI64);
        row_count = len / i64_per_row;
        if (len % i64_per_row != 0) {
            (*env)->ReleaseStringUTFChars(env, jsignature, signature);
            set_last_status(DDB_ERR_INTERNAL);
            return -1;
        }
    } else if (f64_per_row > 0) {
        if (valuesF64 == NULL) {
            (*env)->ReleaseStringUTFChars(env, jsignature, signature);
            set_last_status(DDB_ERR_INTERNAL);
            return -1;
        }
        jsize len = (*env)->GetArrayLength(env, valuesF64);
        row_count = len / f64_per_row;
        if (len % f64_per_row != 0) {
            (*env)->ReleaseStringUTFChars(env, jsignature, signature);
            set_last_status(DDB_ERR_INTERNAL);
            return -1;
        }
    } else if (text_per_row > 0) {
        if (valuesText == NULL) {
            (*env)->ReleaseStringUTFChars(env, jsignature, signature);
            set_last_status(DDB_ERR_INTERNAL);
            return -1;
        }
        jsize len = (*env)->GetArrayLength(env, valuesText);
        row_count = len / text_per_row;
        if (len % text_per_row != 0) {
            (*env)->ReleaseStringUTFChars(env, jsignature, signature);
            set_last_status(DDB_ERR_INTERNAL);
            return -1;
        }
    }

    if (row_count == 0) {
        (*env)->ReleaseStringUTFChars(env, jsignature, signature);
        return set_out_affected(env, outAffected, 0);
    }

    if ((i64_per_row > 0 && (*env)->GetArrayLength(env, valuesI64) != row_count * i64_per_row)
        || (f64_per_row > 0 && (*env)->GetArrayLength(env, valuesF64) != row_count * f64_per_row)
        || (text_per_row > 0 && (*env)->GetArrayLength(env, valuesText) != row_count * text_per_row)) {
        (*env)->ReleaseStringUTFChars(env, jsignature, signature);
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }

    jlong *java_i64 = i64_per_row > 0 ? (*env)->GetLongArrayElements(env, valuesI64, NULL) : NULL;
    jdouble *java_f64 = f64_per_row > 0 ? (*env)->GetDoubleArrayElements(env, valuesF64, NULL) : NULL;
    jstring *strings = text_per_row > 0 ? (jstring *)calloc((size_t)(row_count * text_per_row), sizeof(jstring)) : NULL;
    char **text_ptrs = text_per_row > 0 ? (char **)calloc((size_t)(row_count * text_per_row), sizeof(char *)) : NULL;
    size_t *text_lens = text_per_row > 0 ? (size_t *)calloc((size_t)(row_count * text_per_row), sizeof(size_t)) : NULL;

    if ((i64_per_row > 0 && java_i64 == NULL) || (f64_per_row > 0 && java_f64 == NULL)
        || (text_per_row > 0 && (strings == NULL || text_ptrs == NULL || text_lens == NULL))) {
        free(strings);
        free(text_ptrs);
        free(text_lens);
        if (java_i64 != NULL) (*env)->ReleaseLongArrayElements(env, valuesI64, java_i64, JNI_ABORT);
        if (java_f64 != NULL) (*env)->ReleaseDoubleArrayElements(env, valuesF64, java_f64, JNI_ABORT);
        (*env)->ReleaseStringUTFChars(env, jsignature, signature);
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }

    int rc = 0;
    if (text_per_row > 0) {
        jsize total_text = row_count * text_per_row;
        for (jsize i = 0; i < total_text; i++) {
            jstring value = (jstring)(*env)->GetObjectArrayElement(env, valuesText, i);
            if (value == NULL) {
                set_last_status(DDB_ERR_INTERNAL);
                rc = -1;
                break;
            }
            strings[i] = value;
            text_ptrs[i] = (char *)(*env)->GetStringUTFChars(env, value, NULL);
            if (text_ptrs[i] == NULL) {
                set_last_status(DDB_ERR_INTERNAL);
                rc = -1;
                break;
            }
            text_lens[i] = (size_t)(*env)->GetStringUTFLength(env, value);
        }
    }

    if (rc == 0) {
        clear_row_cache_for_stmt(s);
        uint64_t affected = 0;
        ddb_status_t status = ddb_stmt_execute_batch_typed(
            as_stmt(s),
            (size_t)row_count,
            signature,
            (const int64_t *)java_i64,
            (const double *)java_f64,
            (const char *const *)text_ptrs,
            text_lens,
            &affected);
        set_last_status(status);
        rc = status == DDB_OK ? set_out_affected(env, outAffected, affected) : -1;
    }

    if (text_per_row > 0) {
        jsize total_text = row_count * text_per_row;
        for (jsize i = 0; i < total_text; i++) {
            if (text_ptrs[i] != NULL && strings[i] != NULL) {
                (*env)->ReleaseStringUTFChars(env, strings[i], text_ptrs[i]);
            }
            if (strings[i] != NULL) {
                (*env)->DeleteLocalRef(env, strings[i]);
            }
        }
    }
    free(strings);
    free(text_ptrs);
    free(text_lens);
    if (java_i64 != NULL) (*env)->ReleaseLongArrayElements(env, valuesI64, java_i64, JNI_ABORT);
    if (java_f64 != NULL) (*env)->ReleaseDoubleArrayElements(env, valuesF64, java_f64, JNI_ABORT);
    (*env)->ReleaseStringUTFChars(env, jsignature, signature);
    return rc;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtRebindInt64Execute(
    JNIEnv *env, jclass cls, jlong s, jlong value, jlongArray outAffected)
{
    (void)env;
    (void)cls;
    if (s == 0) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    clear_row_cache_for_stmt(s);
    uint64_t affected = 0;
    ddb_status_t status = ddb_stmt_rebind_int64_execute(as_stmt(s), (int64_t)value, &affected);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return set_out_affected(env, outAffected, affected);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtRebindTextInt64Execute(
    JNIEnv *env, jclass cls, jlong s, jstring jtext, jlong value, jlongArray outAffected)
{
    (void)cls;
    if (s == 0 || jtext == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    const char *text = (*env)->GetStringUTFChars(env, jtext, NULL);
    if (text == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    jsize text_len = (*env)->GetStringUTFLength(env, jtext);
    clear_row_cache_for_stmt(s);
    uint64_t affected = 0;
    ddb_status_t status = ddb_stmt_rebind_text_int64_execute(
        as_stmt(s), text, (size_t)text_len, (int64_t)value, &affected);
    (*env)->ReleaseStringUTFChars(env, jtext, text);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return set_out_affected(env, outAffected, affected);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtRebindInt64TextExecute(
    JNIEnv *env, jclass cls, jlong s, jlong value, jstring jtext, jlongArray outAffected)
{
    (void)cls;
    if (s == 0 || jtext == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    const char *text = (*env)->GetStringUTFChars(env, jtext, NULL);
    if (text == NULL) {
        set_last_status(DDB_ERR_INTERNAL);
        return -1;
    }
    jsize text_len = (*env)->GetStringUTFLength(env, jtext);
    clear_row_cache_for_stmt(s);
    uint64_t affected = 0;
    ddb_status_t status = ddb_stmt_rebind_int64_text_execute(
        as_stmt(s), (int64_t)value, text, (size_t)text_len, &affected);
    (*env)->ReleaseStringUTFChars(env, jtext, text);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return set_out_affected(env, outAffected, affected);
}

static int col_type_for_tag(uint32_t tag, const ddb_value_view_t *v) {
    switch (tag) {
        case DDB_VALUE_NULL: return 0;
        case DDB_VALUE_INT64:
            if (v->int64_value == 0) return 15;
            if (v->int64_value == 1) return 16;
            return 1;
        case DDB_VALUE_BOOL: return v->bool_value ? 14 : 13;
        case DDB_VALUE_FLOAT64: return 3;
        case DDB_VALUE_TEXT: return 4;
        case DDB_VALUE_BLOB: return 5;
        case DDB_VALUE_GEOMETRY: return 5;
        case DDB_VALUE_GEOGRAPHY: return 5;
        case DDB_VALUE_UUID: return 5;
        case DDB_VALUE_DECIMAL: return 12;
        case DDB_VALUE_TIMESTAMP_MICROS: return 17;
        case DDB_VALUE_ENUM: return 18;
        case DDB_VALUE_IPADDR: return 19;
        case DDB_VALUE_CIDR: return 20;
        case DDB_VALUE_DATE: return 21;
        case DDB_VALUE_TIME: return 22;
        case DDB_VALUE_TIMESTAMPTZ_MICROS: return 23;
        case DDB_VALUE_INTERVAL: return 24;
        case DDB_VALUE_MACADDR: return 25;
        default: return 0;
    }
}

static int row_view_at(jlong s, jint index, const ddb_value_view_t **out_v, size_t *out_cols) {
    const ddb_value_view_t *values = g_row_cache.values;
    size_t cols = g_row_cache.cols;
    if (g_row_cache.stmt_handle != s || values == NULL) {
        ddb_status_t status = ddb_stmt_row_view(as_stmt(s), &values, &cols);
        set_last_status(status);
        if (status != DDB_OK) return -1;
        cache_row_view(s, values, cols);
    }
    if (index < 0 || (size_t)index >= cols) return -1;
    *out_v = &values[(size_t)index];
    if (out_cols) *out_cols = cols;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colCount(JNIEnv *env, jclass cls, jlong s)
{
    (void)env;
    (void)cls;
    if (s == 0) return 0;
    size_t cols = 0;
    ddb_status_t status = ddb_stmt_column_count(as_stmt(s), &cols);
    set_last_status(status);
    if (status != DDB_OK) return 0;
    return (jint)cols;
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colName(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)cls;
    if (s == 0) return NULL;
    char *name = NULL;
    ddb_status_t status = ddb_stmt_column_name_copy(as_stmt(s), (size_t)index, &name);
    set_last_status(status);
    if (status != DDB_OK || name == NULL) return NULL;
    jstring result = (*env)->NewStringUTF(env, name);
    ddb_string_free(&name);
    return result;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colType(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)env;
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return 0;
    return (jint)col_type_for_tag(v->tag, v);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colIsNull(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)env;
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return 1;
    return (jint)(v->tag == DDB_VALUE_NULL ? 1 : 0);
}

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colInt64(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)env;
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return 0;
    if (v->tag == DDB_VALUE_BOOL) return v->bool_value ? 1 : 0;
    if (v->tag == DDB_VALUE_INT64) return (jlong)v->int64_value;
    if (v->tag == DDB_VALUE_TIMESTAMP_MICROS) return (jlong)v->timestamp_micros;
    if (v->tag == DDB_VALUE_DECIMAL) return (jlong)v->decimal_scaled;
    return 0;
}

JNIEXPORT jdouble JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colFloat64(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)env;
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return 0.0;
    if (v->tag == DDB_VALUE_FLOAT64) return (jdouble)v->float64_value;
    if (v->tag == DDB_VALUE_INT64) return (jdouble)v->int64_value;
    return 0.0;
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colText(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return NULL;
    char buf[128];
    const char *text = NULL;
    switch (v->tag) {
        case DDB_VALUE_TEXT:
            if (v->data == NULL) return (*env)->NewStringUTF(env, "");
            {
                char *tmp = (char *)malloc(v->len + 1);
                if (tmp == NULL) return NULL;
                memcpy(tmp, v->data, v->len);
                tmp[v->len] = '\0';
                jstring result = (*env)->NewStringUTF(env, tmp);
                free(tmp);
                return result;
            }
        case DDB_VALUE_ENUM:
            if (format_enum(v->enum_type_id, v->enum_label_id, buf, sizeof(buf)) != 0) return NULL;
            text = buf;
            break;
        case DDB_VALUE_IPADDR:
            if (format_ip_addr(v->ip_family, v->ip_cidr_addr_bytes, buf, sizeof(buf)) != 0) return NULL;
            text = buf;
            break;
        case DDB_VALUE_CIDR:
            if (format_cidr(v->ip_family, v->cidr_prefix_len, v->ip_cidr_addr_bytes, buf, sizeof(buf)) != 0) return NULL;
            text = buf;
            break;
        case DDB_VALUE_DATE:
            if (format_date_days(v->date_days, buf, sizeof(buf)) != 0) return NULL;
            text = buf;
            break;
        case DDB_VALUE_TIME:
            if (format_time_micros(v->time_micros, buf, sizeof(buf)) != 0) return NULL;
            text = buf;
            break;
        case DDB_VALUE_TIMESTAMPTZ_MICROS:
            if (format_timestamp_tz_micros(v->timestamptz_micros, buf, sizeof(buf)) != 0) return NULL;
            text = buf;
            break;
        case DDB_VALUE_INTERVAL:
            if (format_interval(v->interval_months, v->interval_days, v->interval_micros, buf, sizeof(buf)) != 0) return NULL;
            text = buf;
            break;
        case DDB_VALUE_MACADDR:
            if (format_macaddr(v->ip_family, v->ip_cidr_addr_bytes, buf, sizeof(buf)) != 0) return NULL;
            text = buf;
            break;
        default:
            return NULL;
    }
    return (*env)->NewStringUTF(env, text);
}

JNIEXPORT jbyteArray JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colBlob(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return NULL;
    const uint8_t *data = NULL;
    size_t len = 0;
    if (v->tag == DDB_VALUE_BLOB || v->tag == DDB_VALUE_GEOMETRY || v->tag == DDB_VALUE_GEOGRAPHY) {
        data = v->data;
        len = v->len;
    } else if (v->tag == DDB_VALUE_UUID) {
        data = v->uuid_bytes;
        len = 16;
    } else {
        return NULL;
    }
    if (data == NULL) return NULL;
    jbyteArray arr = (*env)->NewByteArray(env, (jsize)len);
    if (arr == NULL) return NULL;
    (*env)->SetByteArrayRegion(env, arr, 0, (jsize)len, (const jbyte *)data);
    return arr;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colDecimalScale(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)env;
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return 0;
    if (v->tag != DDB_VALUE_DECIMAL) return 0;
    return (jint)v->decimal_scale;
}

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colDecimalUnscaled(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)env;
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return 0;
    if (v->tag != DDB_VALUE_DECIMAL) return 0;
    return (jlong)v->decimal_scaled;
}

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colDatetime(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)env;
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return 0;
    if (v->tag != DDB_VALUE_TIMESTAMP_MICROS) return 0;
    return (jlong)v->timestamp_micros;
}

static jstring json_from_statused_string(JNIEnv *env, ddb_status_t status, char *json) {
    set_last_status(status);
    if (status != DDB_OK || json == NULL) return NULL;
    jstring out = (*env)->NewStringUTF(env, json);
    ddb_string_free(&json);
    return out;
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaListTables(JNIEnv *env, jclass cls, jlong dbHandle)
{
    (void)cls;
    char *json = NULL;
    ddb_status_t status = ddb_db_list_tables_json(as_db(dbHandle), &json);
    return json_from_statused_string(env, status, json);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaListViews(JNIEnv *env, jclass cls, jlong dbHandle)
{
    (void)cls;
    char *json = NULL;
    ddb_status_t status = ddb_db_list_views_json(as_db(dbHandle), &json);
    return json_from_statused_string(env, status, json);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaGetViewDDL(JNIEnv *env, jclass cls,
    jlong dbHandle, jstring jview)
{
    (void)cls;
    if (dbHandle == 0 || jview == NULL) return NULL;
    char *view = jstring_to_cstr(env, jview);
    if (view == NULL) return NULL;
    char *ddl = NULL;
    ddb_status_t status = ddb_db_get_view_ddl(as_db(dbHandle), view, &ddl);
    free(view);
    return json_from_statused_string(env, status, ddl);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaGetTableColumns(JNIEnv *env, jclass cls,
    jlong dbHandle, jstring jtable)
{
    (void)cls;
    if (dbHandle == 0 || jtable == NULL) return NULL;
    char *table = jstring_to_cstr(env, jtable);
    if (table == NULL) return NULL;
    char *json = NULL;
    ddb_status_t status = ddb_db_describe_table_json(as_db(dbHandle), table, &json);
    free(table);
    return json_from_statused_string(env, status, json);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaGetTableDDL(JNIEnv *env, jclass cls,
    jlong dbHandle, jstring jtable)
{
    (void)cls;
    if (dbHandle == 0 || jtable == NULL) return NULL;
    char *table = jstring_to_cstr(env, jtable);
    if (table == NULL) return NULL;
    char *ddl = NULL;
    ddb_status_t status = ddb_db_get_table_ddl(as_db(dbHandle), table, &ddl);
    free(table);
    return json_from_statused_string(env, status, ddl);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaListIndexes(JNIEnv *env, jclass cls, jlong dbHandle)
{
    (void)cls;
    char *json = NULL;
    ddb_status_t status = ddb_db_list_indexes_json(as_db(dbHandle), &json);
    return json_from_statused_string(env, status, json);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaListTriggers(JNIEnv *env, jclass cls, jlong dbHandle)
{
    (void)cls;
    char *json = NULL;
    ddb_status_t status = ddb_db_list_triggers_json(as_db(dbHandle), &json);
    return json_from_statused_string(env, status, json);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaGetToolingMetadata(JNIEnv *env, jclass cls, jlong dbHandle)
{
    (void)cls;
    char *json = NULL;
    ddb_status_t status = ddb_db_get_tooling_metadata_json(as_db(dbHandle), &json);
    return json_from_statused_string(env, status, json);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaDescribeQuery(JNIEnv *env, jclass cls,
    jlong dbHandle, jstring jsql)
{
    (void)cls;
    if (dbHandle == 0 || jsql == NULL) return NULL;
    char *sql = jstring_to_cstr(env, jsql);
    if (sql == NULL) return NULL;
    char *json = NULL;
    ddb_status_t status = ddb_db_describe_query_json(as_db(dbHandle), sql, &json);
    free(sql);
    return json_from_statused_string(env, status, json);
}
