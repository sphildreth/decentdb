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
    if (mode == 1) {
        status = ddb_db_create(path, &db);
    } else if (mode == 2) {
        status = ddb_db_open(path, &db);
    } else {
        status = ddb_db_open_or_create(path, &db);
    }
    set_last_status(status);

    free(path);
    free(opts);
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
        case DDB_VALUE_UUID: return 5;
        case DDB_VALUE_DECIMAL: return 12;
        case DDB_VALUE_TIMESTAMP_MICROS: return 17;
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
    if (v->tag != DDB_VALUE_TEXT || v->data == NULL) return NULL;
    char *tmp = (char *)malloc(v->len + 1);
    if (tmp == NULL) return NULL;
    memcpy(tmp, v->data, v->len);
    tmp[v->len] = '\0';
    jstring result = (*env)->NewStringUTF(env, tmp);
    free(tmp);
    return result;
}

JNIEXPORT jbyteArray JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colBlob(JNIEnv *env, jclass cls, jlong s, jint index)
{
    (void)cls;
    const ddb_value_view_t *v = NULL;
    if (row_view_at(s, index, &v, NULL) != 0) return NULL;
    const uint8_t *data = NULL;
    size_t len = 0;
    if (v->tag == DDB_VALUE_BLOB) {
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
