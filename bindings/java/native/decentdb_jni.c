/*
 * DecentDB JNI Bridge
 *
 * This file implements the JNI methods declared in DecentDBNative.java.
 * It delegates all operations to the DecentDB C API (libc_api / libdecentdb_jni).
 *
 * Ownership rules:
 * - DB handles (long) are created by dbOpen() and freed by dbClose().
 * - Statement handles (long) are created by stmtPrepare() and freed by stmtFinalize().
 * - All C API strings are freed by calling decentdb_free() on them.
 * - Java strings passed to native are converted to C strings and freed on the stack.
 *
 * Thread-safety:
 * - Statement/result handles are NOT safe for concurrent use.
 * - Connection-level locking in Java (DecentDBConnection.connectionLock) prevents
 *   concurrent calls to the same DB/statement handle from different threads.
 */

#include <jni.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Forward declarations for the DecentDB C API. These match the symbols
   exported by the Nim-compiled shared library (libc_api.so / libdecentdb_jni.so). */

typedef void (*decentdb_free_fn)(void *p);
typedef void *(*decentdb_open_fn)(const char *path, const char *options);
typedef int  (*decentdb_close_fn)(void *p);
typedef int  (*decentdb_last_error_code_fn)(void *p);
typedef const char *(*decentdb_last_error_message_fn)(void *p);
typedef int  (*decentdb_prepare_fn)(void *db, const char *sql, void **out_stmt);
typedef int  (*decentdb_step_fn)(void *stmt);
typedef int  (*decentdb_reset_fn)(void *stmt);
typedef int  (*decentdb_clear_bindings_fn)(void *stmt);
typedef void (*decentdb_finalize_fn)(void *stmt);
typedef int64_t (*decentdb_rows_affected_fn)(void *stmt);

typedef int  (*decentdb_bind_null_fn)(void *stmt, int col);
typedef int  (*decentdb_bind_int64_fn)(void *stmt, int col, int64_t val);
typedef int  (*decentdb_bind_float64_fn)(void *stmt, int col, double val);
typedef int  (*decentdb_bind_text_fn)(void *stmt, int col, const char *utf8, int byte_len);
typedef int  (*decentdb_bind_blob_fn)(void *stmt, int col, const uint8_t *data, int byte_len);

typedef int  (*decentdb_column_count_fn)(void *stmt);
typedef const char *(*decentdb_column_name_fn)(void *stmt, int col);
typedef int  (*decentdb_column_type_fn)(void *stmt, int col);
typedef int  (*decentdb_column_is_null_fn)(void *stmt, int col);
typedef int64_t (*decentdb_column_int64_fn)(void *stmt, int col);
typedef double (*decentdb_column_float64_fn)(void *stmt, int col);
typedef int  (*decentdb_column_decimal_scale_fn)(void *stmt, int col);
typedef int64_t (*decentdb_column_decimal_unscaled_fn)(void *stmt, int col);
typedef const char *(*decentdb_column_text_fn)(void *stmt, int col, int *out_len);
typedef const uint8_t *(*decentdb_column_blob_fn)(void *stmt, int col, int *out_len);

typedef const char *(*decentdb_list_tables_json_fn)(void *db, int *out_len);
typedef const char *(*decentdb_get_table_columns_json_fn)(void *db, const char *table, int *out_len);
typedef const char *(*decentdb_list_indexes_json_fn)(void *db, int *out_len);

/*
 * When compiled as a standalone JNI library that *embeds* the DecentDB engine,
 * we link directly against the DecentDB symbols (provided at link time).
 * This avoids dlopen() complications and matches how SQLite JDBC works.
 */

/* Direct symbol declarations for static/dynamic linking */
extern void        decentdb_free(void *p);
extern void       *decentdb_open(const char *path, const char *options);
extern int         decentdb_close(void *p);
extern int         decentdb_last_error_code(void *p);
extern const char *decentdb_last_error_message(void *p);
extern int         decentdb_prepare(void *db, const char *sql, void **out_stmt);
extern int         decentdb_step(void *stmt);
extern int         decentdb_reset(void *stmt);
extern int         decentdb_clear_bindings(void *stmt);
extern void        decentdb_finalize(void *stmt);
extern int64_t     decentdb_rows_affected(void *stmt);

extern int         decentdb_bind_null(void *stmt, int col);
extern int         decentdb_bind_int64(void *stmt, int col, int64_t val);
extern int         decentdb_bind_float64(void *stmt, int col, double val);
extern int         decentdb_bind_text(void *stmt, int col, const char *utf8, int byte_len);
extern int         decentdb_bind_blob(void *stmt, int col, const uint8_t *data, int byte_len);
extern int         decentdb_bind_datetime(void *stmt, int col, int64_t micros_utc);

extern int         decentdb_column_count(void *stmt);
extern const char *decentdb_column_name(void *stmt, int col);
extern int         decentdb_column_type(void *stmt, int col);
extern int         decentdb_column_is_null(void *stmt, int col);
extern int64_t     decentdb_column_int64(void *stmt, int col);
extern double      decentdb_column_float64(void *stmt, int col);
extern int         decentdb_column_decimal_scale(void *stmt, int col);
extern int64_t     decentdb_column_decimal_unscaled(void *stmt, int col);
extern int64_t     decentdb_column_datetime(void *stmt, int col);
extern const char *decentdb_column_text(void *stmt, int col, int *out_len);
extern const uint8_t *decentdb_column_blob(void *stmt, int col, int *out_len);

extern const char *decentdb_list_tables_json(void *db, int *out_len);
extern const char *decentdb_list_views_json(void *db, int *out_len);
extern const char *decentdb_get_view_ddl(void *db, const char *view, int *out_len);
extern const char *decentdb_get_table_columns_json(void *db, const char *table, int *out_len);
extern const char *decentdb_list_indexes_json(void *db, int *out_len);

/* Helper: convert jstring to a malloc'd C string. Caller must free(). */
static char *jstring_to_cstr(JNIEnv *env, jstring js) {
    if (js == NULL) return NULL;
    const char *utf = (*env)->GetStringUTFChars(env, js, NULL);
    if (utf == NULL) return NULL;
    char *copy = strdup(utf);
    (*env)->ReleaseStringUTFChars(env, js, utf);
    return copy;
}

/* Helper: create a Java String from a C string (may be NULL → returns NULL). */
static jstring cstr_to_jstring(JNIEnv *env, const char *s) {
    if (s == NULL) return NULL;
    return (*env)->NewStringUTF(env, s);
}

/* ---- Database handle operations ---------------------------------------- */

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbOpen(JNIEnv *env, jclass cls,
    jstring jpath, jstring joptions)
{
    char *path = jstring_to_cstr(env, jpath);
    char *opts = jstring_to_cstr(env, joptions);
    void *handle = decentdb_open(path, opts);
    free(path);
    free(opts);
    return (jlong)(uintptr_t)handle;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbClose(JNIEnv *env, jclass cls, jlong handle)
{
    if (handle == 0) return 0;
    return (jint)decentdb_close((void *)(uintptr_t)handle);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbLastErrorCode(JNIEnv *env, jclass cls, jlong handle)
{
    if (handle == 0) return 0;
    return (jint)decentdb_last_error_code((void *)(uintptr_t)handle);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_dbLastErrorMessage(JNIEnv *env, jclass cls, jlong handle)
{
    if (handle == 0) return NULL;
    const char *msg = decentdb_last_error_message((void *)(uintptr_t)handle);
    return cstr_to_jstring(env, msg);
}

/* ---- Statement operations ---------------------------------------------- */

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtPrepare(JNIEnv *env, jclass cls,
    jlong dbHandle, jstring jsql, jlongArray outStmt)
{
    if (dbHandle == 0 || jsql == NULL) return -1;
    char *sql = jstring_to_cstr(env, jsql);
    void *stmt = NULL;
    int rc = decentdb_prepare((void *)(uintptr_t)dbHandle, sql, &stmt);
    free(sql);
    jlong stmtHandle = (jlong)(uintptr_t)stmt;
    (*env)->SetLongArrayRegion(env, outStmt, 0, 1, &stmtHandle);
    return (jint)rc;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtStep(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    if (stmtHandle == 0) return -1;
    return (jint)decentdb_step((void *)(uintptr_t)stmtHandle);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtReset(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    if (stmtHandle == 0) return -1;
    return (jint)decentdb_reset((void *)(uintptr_t)stmtHandle);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtClearBindings(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    if (stmtHandle == 0) return -1;
    return (jint)decentdb_clear_bindings((void *)(uintptr_t)stmtHandle);
}

JNIEXPORT void JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtFinalize(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    if (stmtHandle == 0) return;
    decentdb_finalize((void *)(uintptr_t)stmtHandle);
}

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_stmtRowsAffected(JNIEnv *env, jclass cls, jlong stmtHandle)
{
    if (stmtHandle == 0) return 0;
    return (jlong)decentdb_rows_affected((void *)(uintptr_t)stmtHandle);
}

/* ---- Bind operations --------------------------------------------------- */

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindNull(JNIEnv *env, jclass cls, jlong s, jint col)
{
    if (s == 0) return -1;
    return (jint)decentdb_bind_null((void *)(uintptr_t)s, (int)col);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindInt64(JNIEnv *env, jclass cls, jlong s, jint col, jlong val)
{
    if (s == 0) return -1;
    return (jint)decentdb_bind_int64((void *)(uintptr_t)s, (int)col, (int64_t)val);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindFloat64(JNIEnv *env, jclass cls, jlong s, jint col, jdouble val)
{
    if (s == 0) return -1;
    return (jint)decentdb_bind_float64((void *)(uintptr_t)s, (int)col, (double)val);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindText(JNIEnv *env, jclass cls, jlong s, jint col, jstring jval)
{
    if (s == 0) return -1;
    if (jval == NULL) {
        return (jint)decentdb_bind_null((void *)(uintptr_t)s, (int)col);
    }
    const char *utf = (*env)->GetStringUTFChars(env, jval, NULL);
    jsize len = (*env)->GetStringUTFLength(env, jval);
    int rc = decentdb_bind_text((void *)(uintptr_t)s, (int)col, utf, (int)len);
    (*env)->ReleaseStringUTFChars(env, jval, utf);
    return (jint)rc;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindBlob(JNIEnv *env, jclass cls, jlong s, jint col, jbyteArray jdata)
{
    if (s == 0) return -1;
    if (jdata == NULL) {
        return (jint)decentdb_bind_null((void *)(uintptr_t)s, (int)col);
    }
    jsize len = (*env)->GetArrayLength(env, jdata);
    jbyte *buf = (*env)->GetByteArrayElements(env, jdata, NULL);
    int rc = decentdb_bind_blob((void *)(uintptr_t)s, (int)col, (const uint8_t *)buf, (int)len);
    (*env)->ReleaseByteArrayElements(env, jdata, buf, JNI_ABORT);
    return (jint)rc;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_bindDatetime(JNIEnv *env, jclass cls, jlong s, jint col, jlong micros_utc)
{
    if (s == 0) return -1;
    return (jint)decentdb_bind_datetime((void *)(uintptr_t)s, (int)col, (int64_t)micros_utc);
}

/* ---- Column access ----------------------------------------------------- */

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colCount(JNIEnv *env, jclass cls, jlong s)
{
    if (s == 0) return 0;
    return (jint)decentdb_column_count((void *)(uintptr_t)s);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colName(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return NULL;
    const char *name = decentdb_column_name((void *)(uintptr_t)s, (int)index);
    return cstr_to_jstring(env, name);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colType(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return 0;
    return (jint)decentdb_column_type((void *)(uintptr_t)s, (int)index);
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colIsNull(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return 1;
    return (jint)decentdb_column_is_null((void *)(uintptr_t)s, (int)index);
}

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colInt64(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return 0;
    return (jlong)decentdb_column_int64((void *)(uintptr_t)s, (int)index);
}

JNIEXPORT jdouble JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colFloat64(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return 0.0;
    return (jdouble)decentdb_column_float64((void *)(uintptr_t)s, (int)index);
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colText(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return NULL;
    int out_len = 0;
    const char *text = decentdb_column_text((void *)(uintptr_t)s, (int)index, &out_len);
    if (text == NULL) return NULL;
    /* The text buffer is NOT null-terminated; copy with explicit length before NewStringUTF. */
    char *tmp = (char *)malloc((size_t)out_len + 1);
    if (tmp == NULL) return NULL;
    memcpy(tmp, text, (size_t)out_len);
    tmp[out_len] = '\0';
    jstring result = (*env)->NewStringUTF(env, tmp);
    free(tmp);
    return result;
}

JNIEXPORT jbyteArray JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colBlob(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return NULL;
    int out_len = 0;
    const uint8_t *data = decentdb_column_blob((void *)(uintptr_t)s, (int)index, &out_len);
    if (data == NULL) return NULL;
    jbyteArray arr = (*env)->NewByteArray(env, (jsize)out_len);
    if (arr == NULL) return NULL;
    (*env)->SetByteArrayRegion(env, arr, 0, (jsize)out_len, (const jbyte *)data);
    return arr;
}

JNIEXPORT jint JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colDecimalScale(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return 0;
    return (jint)decentdb_column_decimal_scale((void *)(uintptr_t)s, (int)index);
}

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colDecimalUnscaled(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return 0;
    return (jlong)decentdb_column_decimal_unscaled((void *)(uintptr_t)s, (int)index);
}

JNIEXPORT jlong JNICALL
Java_com_decentdb_jdbc_DecentDBNative_colDatetime(JNIEnv *env, jclass cls, jlong s, jint index)
{
    if (s == 0) return 0;
    return (jlong)decentdb_column_datetime((void *)(uintptr_t)s, (int)index);
}

/* ---- Metadata (JSON) --------------------------------------------------- */

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaListTables(JNIEnv *env, jclass cls, jlong dbHandle)
{
    if (dbHandle == 0) return NULL;
    int out_len = 0;
    const char *json = decentdb_list_tables_json((void *)(uintptr_t)dbHandle, &out_len);
    if (json == NULL) return NULL;
    jstring result = (*env)->NewStringUTF(env, json);
    decentdb_free((void *)json);
    return result;
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaListViews(JNIEnv *env, jclass cls, jlong dbHandle)
{
    if (dbHandle == 0) return NULL;
    int out_len = 0;
    const char *json = decentdb_list_views_json((void *)(uintptr_t)dbHandle, &out_len);
    if (json == NULL) return NULL;
    jstring result = (*env)->NewStringUTF(env, json);
    decentdb_free((void *)json);
    return result;
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaGetViewDDL(JNIEnv *env, jclass cls,
    jlong dbHandle, jstring jview)
{
    if (dbHandle == 0 || jview == NULL) return NULL;
    char *view = jstring_to_cstr(env, jview);
    if (view == NULL) return NULL;

    int out_len = 0;
    const char *ddl = decentdb_get_view_ddl((void *)(uintptr_t)dbHandle, view, &out_len);
    free(view);

    if (ddl == NULL) return NULL;
    jstring result = (*env)->NewStringUTF(env, ddl);
    decentdb_free((void *)ddl);
    return result;
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaGetTableColumns(JNIEnv *env, jclass cls,
    jlong dbHandle, jstring jtable)
{
    if (dbHandle == 0 || jtable == NULL) return NULL;
    char *table = jstring_to_cstr(env, jtable);
    int out_len = 0;
    const char *json = decentdb_get_table_columns_json((void *)(uintptr_t)dbHandle, table, &out_len);
    free(table);
    if (json == NULL) return NULL;
    jstring result = (*env)->NewStringUTF(env, json);
    decentdb_free((void *)json);
    return result;
}

JNIEXPORT jstring JNICALL
Java_com_decentdb_jdbc_DecentDBNative_metaListIndexes(JNIEnv *env, jclass cls, jlong dbHandle)
{
    if (dbHandle == 0) return NULL;
    int out_len = 0;
    const char *json = decentdb_list_indexes_json((void *)(uintptr_t)dbHandle, &out_len);
    if (json == NULL) return NULL;
    jstring result = (*env)->NewStringUTF(env, json);
    decentdb_free((void *)json);
    return result;
}
