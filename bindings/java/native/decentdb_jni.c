/*
 * DecentDB JNI Bridge (ddb_* ABI)
 */

#include <jni.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "../../../include/decentdb.h"

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
        default: return 6;
    }
}

/* Cached global last status for callers passing dbHandle=0. */
static int g_last_code = 0;

static void set_last_status(ddb_status_t status) {
    g_last_code = map_status_to_legacy_code(status);
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

static int classify_open_mode(const char *opts) {
    if (opts == NULL || opts[0] == '\0') return 0; /* open_or_create */
    if (strcmp(opts, "mode=create") == 0) return 1;
    if (strcmp(opts, "mode=open") == 0) return 2;
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
    int mode = classify_open_mode(opts);
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
    if (status != DDB_OK) return -1;
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
    uint8_t has_row = 0;
    ddb_status_t status = ddb_stmt_step(as_stmt(stmtHandle), &has_row);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return has_row ? 1 : 0;
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
Java_com_decentdb_jdbc_DecentDBNative_bindText(JNIEnv *env, jclass cls, jlong s, jint col, jstring jval)
{
    (void)cls;
    if (jval == NULL) {
        ddb_status_t status = ddb_stmt_bind_null(as_stmt(s), (size_t)col);
        set_last_status(status);
        if (status != DDB_OK) return -1;
        return 0;
    }
    const char *utf = (*env)->GetStringUTFChars(env, jval, NULL);
    jsize len = (*env)->GetStringUTFLength(env, jval);
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
        ddb_status_t status = ddb_stmt_bind_null(as_stmt(s), (size_t)col);
        set_last_status(status);
        if (status != DDB_OK) return -1;
        return 0;
    }
    jsize len = (*env)->GetArrayLength(env, jdata);
    jbyte *buf = (*env)->GetByteArrayElements(env, jdata, NULL);
    ddb_status_t status = ddb_stmt_bind_blob(as_stmt(s), (size_t)col, (const uint8_t *)buf, (size_t)len);
    (*env)->ReleaseByteArrayElements(env, jdata, buf, JNI_ABORT);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindDatetime(JNIEnv *env, jclass cls, jlong s, jint col, jlong micros_utc)
{
    (void)env;
    (void)cls;
    ddb_status_t status = ddb_stmt_bind_timestamp_micros(as_stmt(s), (size_t)col, (int64_t)micros_utc);
    set_last_status(status);
    if (status != DDB_OK) return -1;
    return 0;
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
    const ddb_value_view_t *values = NULL;
    size_t cols = 0;
    ddb_status_t status = ddb_stmt_row_view(as_stmt(s), &values, &cols);
    set_last_status(status);
    if (status != DDB_OK) return -1;
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
Java_com_decentdb_jdbc_DecentDBNative_metaListIndexes(JNIEnv *env, jclass cls, jlong dbHandle)
{
    (void)cls;
    char *json = NULL;
    ddb_status_t status = ddb_db_list_indexes_json(as_db(dbHandle), &json);
    return json_from_statused_string(env, status, json);
}
