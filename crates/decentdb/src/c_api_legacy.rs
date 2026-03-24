//! Legacy `decentdb_*` C ABI compatibility shim.
//!
//! This layer allows older bindings (Go/Node/Java) to run against the Rust
//! rewrite while they continue migrating to the stable `ddb_*` ABI.

use std::cell::RefCell;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

use crate::c_api::{
    ddb_db_checkpoint, ddb_db_describe_table_json, ddb_db_execute, ddb_db_free,
    ddb_db_get_view_ddl, ddb_db_list_indexes_json, ddb_db_list_tables_json, ddb_db_list_views_json,
    ddb_db_open_or_create, ddb_db_prepare, ddb_db_save_as, ddb_last_error_message,
    ddb_result_affected_rows, ddb_result_free, ddb_stmt_affected_rows, ddb_stmt_bind_blob,
    ddb_stmt_bind_bool, ddb_stmt_bind_decimal, ddb_stmt_bind_float64, ddb_stmt_bind_int64,
    ddb_stmt_bind_null, ddb_stmt_bind_text, ddb_stmt_bind_timestamp_micros,
    ddb_stmt_clear_bindings, ddb_stmt_column_count, ddb_stmt_column_name_copy, ddb_stmt_free,
    ddb_stmt_reset, ddb_stmt_step, ddb_stmt_value_copy, ddb_string_free, ddb_value_dispose,
    ddb_value_init, DbHandle, DdbValue, ResultHandle, StmtHandle,
};

const DDB_OK: u32 = 0;

const LEGACY_KIND_NULL: c_int = 0;
const LEGACY_KIND_INT64: c_int = 1;
const LEGACY_KIND_BOOL: c_int = 2;
const LEGACY_KIND_FLOAT64: c_int = 3;
const LEGACY_KIND_TEXT: c_int = 4;
const LEGACY_KIND_BLOB: c_int = 5;
const LEGACY_KIND_DECIMAL: c_int = 12;
const LEGACY_KIND_DATETIME: c_int = 17;

const DDB_VALUE_NULL: u32 = 0;
const DDB_VALUE_INT64: u32 = 1;
const DDB_VALUE_FLOAT64: u32 = 2;
const DDB_VALUE_BOOL: u32 = 3;
const DDB_VALUE_TEXT: u32 = 4;
const DDB_VALUE_BLOB: u32 = 5;
const DDB_VALUE_DECIMAL: u32 = 6;
const DDB_VALUE_UUID: u32 = 7;
const DDB_VALUE_TIMESTAMP_MICROS: u32 = 8;

const LEGACY_ERR_INTERNAL: c_int = 6;

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct decentdb_value_view {
    pub kind: c_int,
    pub is_null: c_int,
    pub int64_val: i64,
    pub float64_val: f64,
    pub bytes: *const u8,
    pub bytes_len: c_int,
    pub decimal_scale: c_int,
}

#[repr(C)]
pub struct LegacyDb {
    inner: *mut DbHandle,
    last_error_code: c_int,
    last_error_message: CString,
}

#[repr(C)]
pub struct LegacyStmt {
    inner: *mut StmtHandle,
    owner: *mut LegacyDb,
    row_values: Vec<decentdb_value_view>,
    row_buffers: Vec<Option<Box<[u8]>>>,
    column_name_cache: Vec<Option<CString>>,
    direct_sql: Option<CString>,
    direct_executed: bool,
    direct_rows_affected: i64,
}

#[allow(non_camel_case_types)]
pub type decentdb_db = LegacyDb;

#[allow(non_camel_case_types)]
pub type decentdb_stmt = LegacyStmt;

struct LegacyErrorState {
    code: c_int,
    message: CString,
}

impl LegacyErrorState {
    fn new() -> Self {
        Self {
            code: 0,
            message: cstring_from_str(""),
        }
    }
}

thread_local! {
    static LEGACY_LAST_ERROR: RefCell<LegacyErrorState> = RefCell::new(LegacyErrorState::new());
}

fn cstring_from_str(message: &str) -> CString {
    let sanitized = message.replace('\0', " ");
    match CString::new(sanitized) {
        Ok(value) => value,
        Err(_) => {
            // SAFETY: This literal contains no NUL bytes.
            unsafe { CString::from_vec_unchecked(b"invalid error".to_vec()) }
        }
    }
}

fn cstring_ptr(message: &CString) -> *const c_char {
    message.as_ptr()
}

fn set_global_error(code: c_int, message: &str) {
    LEGACY_LAST_ERROR.with(|slot| {
        let mut state = slot.borrow_mut();
        state.code = code;
        state.message = cstring_from_str(message);
    });
}

fn set_db_error(db: *mut decentdb_db, code: c_int, message: &str) {
    if !db.is_null() {
        // SAFETY: `db` is checked non-null and points to a live legacy DB handle.
        unsafe {
            (*db).last_error_code = code;
            (*db).last_error_message = cstring_from_str(message);
        }
    }
    set_global_error(code, message);
}

fn clear_error(db: *mut decentdb_db) {
    set_db_error(db, 0, "");
}

fn status_to_code(status: u32) -> c_int {
    i32::try_from(status).unwrap_or(i32::MAX)
}

fn capture_ddb_error_message() -> String {
    let ptr = ddb_last_error_message();
    if ptr.is_null() {
        return "unknown DecentDB error".to_string();
    }
    // SAFETY: pointer comes from DecentDB C-ABI and is NUL-terminated.
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

fn fail_with_status(db: *mut decentdb_db, status: u32) -> c_int {
    let code = status_to_code(status);
    let message = capture_ddb_error_message();
    set_db_error(db, code, &message);
    -1
}

fn fail_with_message(db: *mut decentdb_db, code: c_int, message: &str) -> c_int {
    set_db_error(db, code, message);
    -1
}

fn is_transaction_control_sql(sql: &str) -> bool {
    let upper = sql.trim().trim_end_matches(';').trim().to_ascii_uppercase();
    upper == "BEGIN"
        || upper == "BEGIN TRANSACTION"
        || upper == "COMMIT"
        || upper == "END"
        || upper == "ROLLBACK"
        || upper.starts_with("SAVEPOINT ")
        || upper.starts_with("ROLLBACK TO SAVEPOINT ")
        || upper.starts_with("RELEASE SAVEPOINT ")
}

fn owner_of_stmt(stmt: *mut decentdb_stmt) -> *mut decentdb_db {
    if stmt.is_null() {
        ptr::null_mut()
    } else {
        // SAFETY: `stmt` is checked non-null.
        unsafe { (*stmt).owner }
    }
}

fn bind_index(index_1_based: c_int) -> Option<usize> {
    if index_1_based <= 0 {
        return None;
    }
    usize::try_from(index_1_based).ok()
}

fn column_index(col_0_based: c_int) -> Option<usize> {
    if col_0_based < 0 {
        return None;
    }
    usize::try_from(col_0_based).ok()
}

fn clear_row_cache(stmt: &mut LegacyStmt) {
    stmt.row_values.clear();
    stmt.row_buffers.clear();
}

fn empty_legacy_value() -> decentdb_value_view {
    decentdb_value_view {
        kind: LEGACY_KIND_NULL,
        is_null: 1,
        int64_val: 0,
        float64_val: 0.0,
        bytes: ptr::null(),
        bytes_len: 0,
        decimal_scale: 0,
    }
}

fn bytes_from_ddb(value: &DdbValue) -> Box<[u8]> {
    if value.len == 0 || value.data.is_null() {
        return Vec::new().into_boxed_slice();
    }
    // SAFETY: `ddb_stmt_value_copy` guarantees `data` points to `len` bytes while `value` is live.
    unsafe { std::slice::from_raw_parts(value.data.cast_const(), value.len) }
        .to_vec()
        .into_boxed_slice()
}

fn make_text_or_blob_view(
    kind: c_int,
    bytes: Box<[u8]>,
) -> (decentdb_value_view, Option<Box<[u8]>>) {
    let len = i32::try_from(bytes.len()).unwrap_or(i32::MAX);
    let ptr = if bytes.is_empty() {
        ptr::null()
    } else {
        bytes.as_ptr()
    };
    (
        decentdb_value_view {
            kind,
            is_null: 0,
            int64_val: 0,
            float64_val: 0.0,
            bytes: ptr,
            bytes_len: len,
            decimal_scale: 0,
        },
        Some(bytes),
    )
}

fn legacy_view_from_ddb(value: &DdbValue) -> (decentdb_value_view, Option<Box<[u8]>>) {
    match value.tag {
        DDB_VALUE_NULL => (empty_legacy_value(), None),
        DDB_VALUE_INT64 => (
            decentdb_value_view {
                kind: LEGACY_KIND_INT64,
                is_null: 0,
                int64_val: value.int64_value,
                float64_val: 0.0,
                bytes: ptr::null(),
                bytes_len: 0,
                decimal_scale: 0,
            },
            None,
        ),
        DDB_VALUE_FLOAT64 => (
            decentdb_value_view {
                kind: LEGACY_KIND_FLOAT64,
                is_null: 0,
                int64_val: 0,
                float64_val: value.float64_value,
                bytes: ptr::null(),
                bytes_len: 0,
                decimal_scale: 0,
            },
            None,
        ),
        DDB_VALUE_BOOL => (
            decentdb_value_view {
                kind: LEGACY_KIND_BOOL,
                is_null: 0,
                int64_val: i64::from(value.bool_value != 0),
                float64_val: 0.0,
                bytes: ptr::null(),
                bytes_len: 0,
                decimal_scale: 0,
            },
            None,
        ),
        DDB_VALUE_TEXT => make_text_or_blob_view(LEGACY_KIND_TEXT, bytes_from_ddb(value)),
        DDB_VALUE_BLOB => make_text_or_blob_view(LEGACY_KIND_BLOB, bytes_from_ddb(value)),
        DDB_VALUE_UUID => {
            let bytes = value.uuid_bytes.to_vec().into_boxed_slice();
            make_text_or_blob_view(LEGACY_KIND_BLOB, bytes)
        }
        DDB_VALUE_DECIMAL => (
            decentdb_value_view {
                kind: LEGACY_KIND_DECIMAL,
                is_null: 0,
                int64_val: value.decimal_scaled,
                float64_val: 0.0,
                bytes: ptr::null(),
                bytes_len: 0,
                decimal_scale: i32::from(value.decimal_scale),
            },
            None,
        ),
        DDB_VALUE_TIMESTAMP_MICROS => (
            decentdb_value_view {
                kind: LEGACY_KIND_DATETIME,
                is_null: 0,
                int64_val: value.timestamp_micros,
                float64_val: 0.0,
                bytes: ptr::null(),
                bytes_len: 0,
                decimal_scale: 0,
            },
            None,
        ),
        _ => (empty_legacy_value(), None),
    }
}

fn with_stmt_mut<T>(
    stmt: *mut decentdb_stmt,
    on_null_message: &str,
    f: impl FnOnce(&mut LegacyStmt) -> T,
) -> Result<T, c_int> {
    if stmt.is_null() {
        return Err(fail_with_message(
            ptr::null_mut(),
            LEGACY_ERR_INTERNAL,
            on_null_message,
        ));
    }
    // SAFETY: pointer is checked non-null and ownership stays with caller.
    let stmt_ref = unsafe { &mut *stmt };
    Ok(f(stmt_ref))
}

fn rebuild_row_cache(stmt: *mut decentdb_stmt) -> c_int {
    let owner = owner_of_stmt(stmt);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        clear_row_cache(stmt_ref);
        let mut col_count: usize = 0;
        let status = ddb_stmt_column_count(stmt_ref.inner, &mut col_count);
        if status != DDB_OK {
            return fail_with_status(owner, status);
        }
        stmt_ref.row_values.reserve(col_count);
        stmt_ref.row_buffers.reserve(col_count);
        for col in 0..col_count {
            let mut value = DdbValue::default();
            let init_status = ddb_value_init(&mut value);
            if init_status != DDB_OK {
                return fail_with_status(owner, init_status);
            }
            let copy_status = ddb_stmt_value_copy(stmt_ref.inner, col, &mut value);
            if copy_status != DDB_OK {
                let _ = ddb_value_dispose(&mut value);
                return fail_with_status(owner, copy_status);
            }
            let (view, backing) = legacy_view_from_ddb(&value);
            stmt_ref.row_values.push(view);
            stmt_ref.row_buffers.push(backing);
            let dispose_status = ddb_value_dispose(&mut value);
            if dispose_status != DDB_OK {
                return fail_with_status(owner, dispose_status);
            }
        }
        clear_error(owner);
        0
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

fn view_at(stmt: &LegacyStmt, col_0_based: c_int) -> Option<&decentdb_value_view> {
    let idx = column_index(col_0_based)?;
    stmt.row_values.get(idx)
}

fn ensure_column_name_cached(
    stmt: &mut LegacyStmt,
    col_0_based: c_int,
) -> Result<*const c_char, c_int> {
    let idx = column_index(col_0_based).ok_or_else(|| {
        fail_with_message(
            stmt.owner,
            LEGACY_ERR_INTERNAL,
            "column index must be non-negative",
        )
    })?;
    if stmt.column_name_cache.len() <= idx {
        stmt.column_name_cache.resize_with(idx + 1, || None);
    }
    if stmt.column_name_cache[idx].is_none() {
        let mut raw: *mut c_char = ptr::null_mut();
        let status = ddb_stmt_column_name_copy(stmt.inner, idx, &mut raw);
        if status != DDB_OK {
            return Err(fail_with_status(stmt.owner, status));
        }
        if raw.is_null() {
            stmt.column_name_cache[idx] = Some(cstring_from_str(""));
        } else {
            // SAFETY: `raw` is returned as a NUL-terminated C string by the ddb API.
            let copied = unsafe { CStr::from_ptr(raw) }
                .to_string_lossy()
                .into_owned();
            stmt.column_name_cache[idx] = Some(cstring_from_str(&copied));
            let _ = ddb_string_free(&mut raw);
        }
    }
    if let Some(name) = stmt.column_name_cache[idx].as_ref() {
        Ok(cstring_ptr(name))
    } else {
        Ok(ptr::null())
    }
}

fn prepare_json_result(
    db: *mut decentdb_db,
    out_len: *mut c_int,
    json_ptr: *mut c_char,
) -> *const c_char {
    if json_ptr.is_null() {
        let _ = fail_with_message(db, LEGACY_ERR_INTERNAL, "metadata result was null");
        return ptr::null();
    }
    if !out_len.is_null() {
        // SAFETY: `json_ptr` is a valid NUL-terminated C string from ddb_* JSON helpers.
        let len = unsafe { CStr::from_ptr(json_ptr) }.to_bytes().len();
        // SAFETY: caller provided valid out pointer by contract.
        unsafe {
            *out_len = i32::try_from(len).unwrap_or(i32::MAX);
        }
    }
    clear_error(db);
    json_ptr.cast_const()
}

fn bind_legacy_param(
    stmt: *mut decentdb_stmt,
    index_1_based: usize,
    value: &decentdb_value_view,
) -> c_int {
    let owner = owner_of_stmt(stmt);
    if value.is_null != 0 || value.kind == LEGACY_KIND_NULL {
        let status = ddb_stmt_bind_null(
            // SAFETY: stmt is valid for this call.
            unsafe { (*stmt).inner },
            index_1_based,
        );
        return if status == DDB_OK {
            0
        } else {
            fail_with_status(owner, status)
        };
    }
    let inner = {
        // SAFETY: stmt is non-null and owned by caller.
        unsafe { (*stmt).inner }
    };
    if inner.is_null() {
        return fail_with_message(
            owner,
            LEGACY_ERR_INTERNAL,
            "bindings are not supported for this statement",
        );
    }
    let status = match value.kind {
        LEGACY_KIND_INT64 => ddb_stmt_bind_int64(inner, index_1_based, value.int64_val),
        LEGACY_KIND_BOOL => {
            ddb_stmt_bind_bool(inner, index_1_based, u8::from(value.int64_val != 0))
        }
        LEGACY_KIND_FLOAT64 => ddb_stmt_bind_float64(inner, index_1_based, value.float64_val),
        LEGACY_KIND_TEXT => {
            let data = value.bytes.cast::<c_char>();
            let len = usize::try_from(value.bytes_len).unwrap_or(0);
            ddb_stmt_bind_text(inner, index_1_based, data, len)
        }
        LEGACY_KIND_BLOB => {
            let len = usize::try_from(value.bytes_len).unwrap_or(0);
            ddb_stmt_bind_blob(inner, index_1_based, value.bytes, len)
        }
        LEGACY_KIND_DECIMAL => {
            let scale = if value.decimal_scale < 0 {
                0
            } else {
                u8::try_from(value.decimal_scale).unwrap_or(u8::MAX)
            };
            ddb_stmt_bind_decimal(inner, index_1_based, value.int64_val, scale)
        }
        LEGACY_KIND_DATETIME => {
            ddb_stmt_bind_timestamp_micros(inner, index_1_based, value.int64_val)
        }
        _ => {
            return fail_with_message(
                owner,
                LEGACY_ERR_INTERNAL,
                "unsupported legacy bind value kind",
            )
        }
    };
    if status == DDB_OK {
        0
    } else {
        fail_with_status(owner, status)
    }
}

#[no_mangle]
pub extern "C" fn decentdb_open(
    path_utf8: *const c_char,
    options_utf8: *const c_char,
) -> *mut decentdb_db {
    let _ = options_utf8;
    if path_utf8.is_null() {
        let _ = fail_with_message(
            ptr::null_mut(),
            LEGACY_ERR_INTERNAL,
            "path must not be null",
        );
        return ptr::null_mut();
    }
    // SAFETY: pointer checked non-null above.
    let path_text = unsafe { CStr::from_ptr(path_utf8) }
        .to_string_lossy()
        .into_owned();

    let mut inner: *mut DbHandle = ptr::null_mut();
    let mut status = ddb_db_open_or_create(path_utf8, &mut inner);
    if (status != DDB_OK || inner.is_null()) && !path_text.is_empty() && path_text != ":memory:" {
        if let Ok(meta) = std::fs::metadata(&path_text) {
            if meta.len() == 0 {
                let _ = std::fs::remove_file(&path_text);
                status = ddb_db_open_or_create(path_utf8, &mut inner);
            }
        }
    }
    if status != DDB_OK || inner.is_null() {
        let _ = if status == DDB_OK {
            fail_with_message(
                ptr::null_mut(),
                LEGACY_ERR_INTERNAL,
                "failed to open database",
            )
        } else {
            fail_with_status(ptr::null_mut(), status)
        };
        return ptr::null_mut();
    }
    clear_error(ptr::null_mut());
    Box::into_raw(Box::new(LegacyDb {
        inner,
        last_error_code: 0,
        last_error_message: cstring_from_str(""),
    }))
}

#[no_mangle]
pub extern "C" fn decentdb_close(db: *mut decentdb_db) -> c_int {
    if db.is_null() {
        return 0;
    }
    // SAFETY: pointer comes from `Box::into_raw` in `decentdb_open`.
    let mut owned = unsafe { Box::from_raw(db) };
    if !owned.inner.is_null() {
        let mut inner = owned.inner;
        let status = ddb_db_free(&mut inner);
        owned.inner = ptr::null_mut();
        if status != DDB_OK {
            return fail_with_status(ptr::null_mut(), status);
        }
    }
    clear_error(ptr::null_mut());
    0
}

#[no_mangle]
pub extern "C" fn decentdb_last_error_code(db: *mut decentdb_db) -> c_int {
    if db.is_null() {
        return LEGACY_LAST_ERROR.with(|slot| slot.borrow().code);
    }
    // SAFETY: caller owns and passes a valid DB handle pointer.
    unsafe { (*db).last_error_code }
}

#[no_mangle]
pub extern "C" fn decentdb_last_error_message(db: *mut decentdb_db) -> *const c_char {
    if db.is_null() {
        return LEGACY_LAST_ERROR.with(|slot| cstring_ptr(&slot.borrow().message));
    }
    // SAFETY: caller owns and passes a valid DB handle pointer.
    unsafe { cstring_ptr(&(*db).last_error_message) }
}

#[no_mangle]
pub extern "C" fn decentdb_prepare(
    db: *mut decentdb_db,
    sql_utf8: *const c_char,
    out_stmt: *mut *mut decentdb_stmt,
) -> c_int {
    if db.is_null() || out_stmt.is_null() || sql_utf8.is_null() {
        return fail_with_message(
            db,
            LEGACY_ERR_INTERNAL,
            "db, sql_utf8, and out_stmt must not be null",
        );
    }
    // SAFETY: pointer checked non-null above.
    let sql_text = unsafe { CStr::from_ptr(sql_utf8) }
        .to_string_lossy()
        .into_owned();
    let mut inner: *mut StmtHandle = ptr::null_mut();
    // SAFETY: pointers were checked above.
    let status = unsafe { ddb_db_prepare((*db).inner, sql_utf8, &mut inner) };
    if status != DDB_OK || inner.is_null() {
        if status != DDB_OK
            && capture_ddb_error_message()
                .to_ascii_lowercase()
                .contains("prepared statements do not support transaction control")
            && is_transaction_control_sql(&sql_text)
        {
            let legacy_stmt = LegacyStmt {
                inner: ptr::null_mut(),
                owner: db,
                row_values: Vec::new(),
                row_buffers: Vec::new(),
                column_name_cache: Vec::new(),
                direct_sql: Some(cstring_from_str(&sql_text)),
                direct_executed: false,
                direct_rows_affected: 0,
            };
            // SAFETY: `out_stmt` checked non-null above.
            unsafe {
                *out_stmt = Box::into_raw(Box::new(legacy_stmt));
            }
            clear_error(db);
            return 0;
        }
        return if status == DDB_OK {
            fail_with_message(db, LEGACY_ERR_INTERNAL, "failed to prepare statement")
        } else {
            fail_with_status(db, status)
        };
    }
    let legacy_stmt = LegacyStmt {
        inner,
        owner: db,
        row_values: Vec::new(),
        row_buffers: Vec::new(),
        column_name_cache: Vec::new(),
        direct_sql: None,
        direct_executed: false,
        direct_rows_affected: 0,
    };
    // SAFETY: `out_stmt` checked non-null above.
    unsafe {
        *out_stmt = Box::into_raw(Box::new(legacy_stmt));
    }
    clear_error(db);
    0
}

#[no_mangle]
pub extern "C" fn decentdb_bind_null(stmt: *mut decentdb_stmt, index_1_based: c_int) -> c_int {
    let owner = owner_of_stmt(stmt);
    let slot = match bind_index(index_1_based) {
        Some(slot) => slot,
        None => {
            return fail_with_message(owner, LEGACY_ERR_INTERNAL, "parameter indexes are 1-based")
        }
    };
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            return fail_with_message(
                owner,
                LEGACY_ERR_INTERNAL,
                "bindings are not supported for this statement",
            );
        }
        let status = ddb_stmt_bind_null(stmt_ref.inner, slot);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_bind_int64(
    stmt: *mut decentdb_stmt,
    index_1_based: c_int,
    v: i64,
) -> c_int {
    let owner = owner_of_stmt(stmt);
    let slot = match bind_index(index_1_based) {
        Some(slot) => slot,
        None => {
            return fail_with_message(owner, LEGACY_ERR_INTERNAL, "parameter indexes are 1-based")
        }
    };
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            return fail_with_message(
                owner,
                LEGACY_ERR_INTERNAL,
                "bindings are not supported for this statement",
            );
        }
        let status = ddb_stmt_bind_int64(stmt_ref.inner, slot, v);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_bind_bool(
    stmt: *mut decentdb_stmt,
    index_1_based: c_int,
    v: c_int,
) -> c_int {
    let owner = owner_of_stmt(stmt);
    let slot = match bind_index(index_1_based) {
        Some(slot) => slot,
        None => {
            return fail_with_message(owner, LEGACY_ERR_INTERNAL, "parameter indexes are 1-based")
        }
    };
    let bool_value = u8::from(v != 0);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            return fail_with_message(
                owner,
                LEGACY_ERR_INTERNAL,
                "bindings are not supported for this statement",
            );
        }
        let status = ddb_stmt_bind_bool(stmt_ref.inner, slot, bool_value);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_bind_float64(
    stmt: *mut decentdb_stmt,
    index_1_based: c_int,
    v: f64,
) -> c_int {
    let owner = owner_of_stmt(stmt);
    let slot = match bind_index(index_1_based) {
        Some(slot) => slot,
        None => {
            return fail_with_message(owner, LEGACY_ERR_INTERNAL, "parameter indexes are 1-based")
        }
    };
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            return fail_with_message(
                owner,
                LEGACY_ERR_INTERNAL,
                "bindings are not supported for this statement",
            );
        }
        let status = ddb_stmt_bind_float64(stmt_ref.inner, slot, v);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_bind_text(
    stmt: *mut decentdb_stmt,
    index_1_based: c_int,
    utf8: *const c_char,
    byte_len: c_int,
) -> c_int {
    let owner = owner_of_stmt(stmt);
    let slot = match bind_index(index_1_based) {
        Some(slot) => slot,
        None => {
            return fail_with_message(owner, LEGACY_ERR_INTERNAL, "parameter indexes are 1-based")
        }
    };
    if byte_len < 0 {
        return fail_with_message(owner, LEGACY_ERR_INTERNAL, "byte_len must be non-negative");
    }
    let len = usize::try_from(byte_len).unwrap_or(0);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            return fail_with_message(
                owner,
                LEGACY_ERR_INTERNAL,
                "bindings are not supported for this statement",
            );
        }
        let status = ddb_stmt_bind_text(stmt_ref.inner, slot, utf8, len);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_bind_blob(
    stmt: *mut decentdb_stmt,
    index_1_based: c_int,
    data: *const u8,
    byte_len: c_int,
) -> c_int {
    let owner = owner_of_stmt(stmt);
    let slot = match bind_index(index_1_based) {
        Some(slot) => slot,
        None => {
            return fail_with_message(owner, LEGACY_ERR_INTERNAL, "parameter indexes are 1-based")
        }
    };
    if byte_len < 0 {
        return fail_with_message(owner, LEGACY_ERR_INTERNAL, "byte_len must be non-negative");
    }
    let len = usize::try_from(byte_len).unwrap_or(0);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            return fail_with_message(
                owner,
                LEGACY_ERR_INTERNAL,
                "bindings are not supported for this statement",
            );
        }
        let status = ddb_stmt_bind_blob(stmt_ref.inner, slot, data, len);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_bind_decimal(
    stmt: *mut decentdb_stmt,
    index_1_based: c_int,
    unscaled: i64,
    scale: c_int,
) -> c_int {
    let owner = owner_of_stmt(stmt);
    let slot = match bind_index(index_1_based) {
        Some(slot) => slot,
        None => {
            return fail_with_message(owner, LEGACY_ERR_INTERNAL, "parameter indexes are 1-based")
        }
    };
    if scale < 0 {
        return fail_with_message(
            owner,
            LEGACY_ERR_INTERNAL,
            "decimal scale must be non-negative",
        );
    }
    let scale_u8 = u8::try_from(scale).unwrap_or(u8::MAX);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            return fail_with_message(
                owner,
                LEGACY_ERR_INTERNAL,
                "bindings are not supported for this statement",
            );
        }
        let status = ddb_stmt_bind_decimal(stmt_ref.inner, slot, unscaled, scale_u8);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_bind_datetime(
    stmt: *mut decentdb_stmt,
    index_1_based: c_int,
    micros_utc: i64,
) -> c_int {
    let owner = owner_of_stmt(stmt);
    let slot = match bind_index(index_1_based) {
        Some(slot) => slot,
        None => {
            return fail_with_message(owner, LEGACY_ERR_INTERNAL, "parameter indexes are 1-based")
        }
    };
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            return fail_with_message(
                owner,
                LEGACY_ERR_INTERNAL,
                "bindings are not supported for this statement",
            );
        }
        let status = ddb_stmt_bind_timestamp_micros(stmt_ref.inner, slot, micros_utc);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_reset(stmt: *mut decentdb_stmt) -> c_int {
    let owner = owner_of_stmt(stmt);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        clear_row_cache(stmt_ref);
        if stmt_ref.inner.is_null() {
            stmt_ref.direct_executed = false;
            stmt_ref.direct_rows_affected = 0;
            clear_error(owner);
            return 0;
        }
        let status = ddb_stmt_reset(stmt_ref.inner);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_clear_bindings(stmt: *mut decentdb_stmt) -> c_int {
    let owner = owner_of_stmt(stmt);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            clear_error(owner);
            return 0;
        }
        let status = ddb_stmt_clear_bindings(stmt_ref.inner);
        if status == DDB_OK {
            clear_error(owner);
            0
        } else {
            fail_with_status(owner, status)
        }
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_step(stmt: *mut decentdb_stmt) -> c_int {
    let owner = owner_of_stmt(stmt);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        clear_row_cache(stmt_ref);
        if stmt_ref.inner.is_null() {
            if stmt_ref.direct_executed {
                clear_error(owner);
                return 0;
            }
            let sql = match stmt_ref.direct_sql.as_ref() {
                Some(sql) => sql,
                None => {
                    return fail_with_message(
                        owner,
                        LEGACY_ERR_INTERNAL,
                        "statement is not executable",
                    );
                }
            };
            let mut result_handle: *mut ResultHandle = ptr::null_mut();
            // SAFETY: `owner` and owned db handle are valid for this statement's lifetime.
            let status = unsafe {
                ddb_db_execute(
                    (*owner).inner,
                    sql.as_ptr(),
                    ptr::null(),
                    0,
                    &mut result_handle,
                )
            };
            if status != DDB_OK {
                return fail_with_status(owner, status);
            }
            let mut rows: u64 = 0;
            if !result_handle.is_null() {
                let rows_status = ddb_result_affected_rows(result_handle, &mut rows);
                if rows_status != DDB_OK {
                    let _ = ddb_result_free(&mut result_handle);
                    return fail_with_status(owner, rows_status);
                }
                let free_status = ddb_result_free(&mut result_handle);
                if free_status != DDB_OK {
                    return fail_with_status(owner, free_status);
                }
            }
            stmt_ref.direct_rows_affected = i64::try_from(rows).unwrap_or(i64::MAX);
            stmt_ref.direct_executed = true;
            clear_error(owner);
            return 0;
        }
        let mut has_row: u8 = 0;
        let status = ddb_stmt_step(stmt_ref.inner, &mut has_row);
        if status != DDB_OK {
            return fail_with_status(owner, status);
        }
        if has_row == 0 {
            clear_error(owner);
            return 0;
        }
        if rebuild_row_cache(stmt) != 0 {
            return -1;
        }
        1
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_column_count(stmt: *mut decentdb_stmt) -> c_int {
    let owner = owner_of_stmt(stmt);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            clear_error(owner);
            return 0;
        }
        let mut cols: usize = 0;
        let status = ddb_stmt_column_count(stmt_ref.inner, &mut cols);
        if status != DDB_OK {
            return fail_with_status(owner, status);
        }
        clear_error(owner);
        i32::try_from(cols).unwrap_or(i32::MAX)
    });
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn decentdb_column_name(
    stmt: *mut decentdb_stmt,
    col_0_based: c_int,
) -> *const c_char {
    let owner = owner_of_stmt(stmt);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            clear_error(owner);
            return ptr::null();
        }
        match ensure_column_name_cached(stmt_ref, col_0_based) {
            Ok(name_ptr) => {
                clear_error(owner);
                name_ptr
            }
            Err(_) => ptr::null(),
        }
    });
    match result {
        Ok(ptr) => ptr,
        Err(_) => ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn decentdb_column_type(stmt: *mut decentdb_stmt, col_0_based: c_int) -> c_int {
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        view_at(stmt_ref, col_0_based)
            .map(|view| view.kind)
            .unwrap_or(LEGACY_KIND_NULL)
    });
    match result {
        Ok(kind) => kind,
        Err(_) => LEGACY_KIND_NULL,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_column_is_null(stmt: *mut decentdb_stmt, col_0_based: c_int) -> c_int {
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        view_at(stmt_ref, col_0_based)
            .map(|view| view.is_null)
            .unwrap_or(1)
    });
    result.unwrap_or(1)
}

#[no_mangle]
pub extern "C" fn decentdb_column_int64(stmt: *mut decentdb_stmt, col_0_based: c_int) -> i64 {
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        view_at(stmt_ref, col_0_based)
            .map(|view| if view.is_null != 0 { 0 } else { view.int64_val })
            .unwrap_or(0)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn decentdb_column_float64(stmt: *mut decentdb_stmt, col_0_based: c_int) -> f64 {
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        view_at(stmt_ref, col_0_based)
            .map(|view| {
                if view.is_null != 0 {
                    0.0
                } else {
                    view.float64_val
                }
            })
            .unwrap_or(0.0)
    });
    result.unwrap_or(0.0)
}

#[no_mangle]
pub extern "C" fn decentdb_column_text(
    stmt: *mut decentdb_stmt,
    col_0_based: c_int,
    out_byte_len: *mut c_int,
) -> *const c_char {
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if let Some(view) = view_at(stmt_ref, col_0_based) {
            if view.is_null != 0 || view.kind != LEGACY_KIND_TEXT {
                if !out_byte_len.is_null() {
                    // SAFETY: out pointer is provided by caller.
                    unsafe {
                        *out_byte_len = 0;
                    }
                }
                return ptr::null();
            }
            if !out_byte_len.is_null() {
                // SAFETY: out pointer is provided by caller.
                unsafe {
                    *out_byte_len = view.bytes_len;
                }
            }
            view.bytes.cast::<c_char>()
        } else {
            if !out_byte_len.is_null() {
                // SAFETY: out pointer is provided by caller.
                unsafe {
                    *out_byte_len = 0;
                }
            }
            ptr::null()
        }
    });
    result.unwrap_or(ptr::null())
}

#[no_mangle]
pub extern "C" fn decentdb_column_blob(
    stmt: *mut decentdb_stmt,
    col_0_based: c_int,
    out_byte_len: *mut c_int,
) -> *const u8 {
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if let Some(view) = view_at(stmt_ref, col_0_based) {
            if view.is_null != 0 || view.kind != LEGACY_KIND_BLOB {
                if !out_byte_len.is_null() {
                    // SAFETY: out pointer is provided by caller.
                    unsafe {
                        *out_byte_len = 0;
                    }
                }
                return ptr::null();
            }
            if !out_byte_len.is_null() {
                // SAFETY: out pointer is provided by caller.
                unsafe {
                    *out_byte_len = view.bytes_len;
                }
            }
            view.bytes
        } else {
            if !out_byte_len.is_null() {
                // SAFETY: out pointer is provided by caller.
                unsafe {
                    *out_byte_len = 0;
                }
            }
            ptr::null()
        }
    });
    result.unwrap_or(ptr::null())
}

#[no_mangle]
pub extern "C" fn decentdb_column_decimal_unscaled(
    stmt: *mut decentdb_stmt,
    col_0_based: c_int,
) -> i64 {
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        view_at(stmt_ref, col_0_based)
            .map(|view| {
                if view.kind == LEGACY_KIND_DECIMAL {
                    view.int64_val
                } else {
                    0
                }
            })
            .unwrap_or(0)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn decentdb_column_decimal_scale(
    stmt: *mut decentdb_stmt,
    col_0_based: c_int,
) -> c_int {
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        view_at(stmt_ref, col_0_based)
            .map(|view| {
                if view.kind == LEGACY_KIND_DECIMAL {
                    view.decimal_scale
                } else {
                    0
                }
            })
            .unwrap_or(0)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn decentdb_column_datetime(stmt: *mut decentdb_stmt, col_0_based: c_int) -> i64 {
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        view_at(stmt_ref, col_0_based)
            .map(|view| {
                if view.kind == LEGACY_KIND_DATETIME {
                    view.int64_val
                } else {
                    0
                }
            })
            .unwrap_or(0)
    });
    result.unwrap_or(0)
}

#[no_mangle]
pub extern "C" fn decentdb_row_view(
    stmt: *mut decentdb_stmt,
    out_values: *mut *const decentdb_value_view,
    out_count: *mut c_int,
) -> c_int {
    if out_values.is_null() || out_count.is_null() {
        return fail_with_message(
            owner_of_stmt(stmt),
            LEGACY_ERR_INTERNAL,
            "out_values and out_count must not be null",
        );
    }
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        // SAFETY: output pointers are checked non-null above.
        unsafe {
            *out_count = i32::try_from(stmt_ref.row_values.len()).unwrap_or(i32::MAX);
            *out_values = if stmt_ref.row_values.is_empty() {
                ptr::null()
            } else {
                stmt_ref.row_values.as_ptr()
            };
        }
        clear_error(stmt_ref.owner);
        0
    });
    match result {
        Ok(code) => code,
        Err(code) => code,
    }
}

#[no_mangle]
pub extern "C" fn decentdb_step_with_params_row_view(
    stmt: *mut decentdb_stmt,
    in_params: *const decentdb_value_view,
    in_count: c_int,
    out_values: *mut *const decentdb_value_view,
    out_count: *mut c_int,
    out_has_row: *mut c_int,
) -> c_int {
    if in_count < 0 {
        return fail_with_message(
            owner_of_stmt(stmt),
            LEGACY_ERR_INTERNAL,
            "in_count must be non-negative",
        );
    }
    if !out_has_row.is_null() {
        // SAFETY: out pointer is provided by caller.
        unsafe {
            *out_has_row = 0;
        }
    }
    let reset_rc = decentdb_reset(stmt);
    if reset_rc != 0 {
        return -1;
    }
    let clear_rc = decentdb_clear_bindings(stmt);
    if clear_rc != 0 {
        return -1;
    }
    let param_len = usize::try_from(in_count).unwrap_or(0);
    let params = if param_len == 0 {
        &[][..]
    } else if in_params.is_null() {
        return fail_with_message(
            owner_of_stmt(stmt),
            LEGACY_ERR_INTERNAL,
            "in_params must not be null when in_count > 0",
        );
    } else {
        // SAFETY: caller provides a contiguous `in_count` array.
        unsafe { std::slice::from_raw_parts(in_params, param_len) }
    };

    for (idx, value) in params.iter().enumerate() {
        let index = idx + 1;
        let bind_rc = bind_legacy_param(stmt, index, value);
        if bind_rc != 0 {
            return -1;
        }
    }

    let step_rc = decentdb_step(stmt);
    if step_rc < 0 {
        return -1;
    }
    if !out_has_row.is_null() {
        // SAFETY: out pointer is provided by caller.
        unsafe {
            *out_has_row = if step_rc == 1 { 1 } else { 0 };
        }
    }
    if step_rc == 1 {
        return decentdb_row_view(stmt, out_values, out_count);
    }
    if !out_values.is_null() {
        // SAFETY: output pointer is provided by caller.
        unsafe {
            *out_values = ptr::null();
        }
    }
    if !out_count.is_null() {
        // SAFETY: output pointer is provided by caller.
        unsafe {
            *out_count = 0;
        }
    }
    0
}

#[no_mangle]
pub extern "C" fn decentdb_rows_affected(stmt: *mut decentdb_stmt) -> i64 {
    let owner = owner_of_stmt(stmt);
    let result = with_stmt_mut(stmt, "statement handle must not be null", |stmt_ref| {
        if stmt_ref.inner.is_null() {
            clear_error(owner);
            return stmt_ref.direct_rows_affected;
        }
        let mut out_rows: u64 = 0;
        let status = ddb_stmt_affected_rows(stmt_ref.inner, &mut out_rows);
        if status != DDB_OK {
            let _ = fail_with_status(owner, status);
            return -1;
        }
        clear_error(owner);
        i64::try_from(out_rows).unwrap_or(i64::MAX)
    });
    result.unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn decentdb_finalize(stmt: *mut decentdb_stmt) {
    if stmt.is_null() {
        return;
    }
    // SAFETY: pointer comes from `Box::into_raw` in `decentdb_prepare`.
    let mut owned = unsafe { Box::from_raw(stmt) };
    clear_row_cache(&mut owned);
    owned.direct_executed = false;
    owned.direct_rows_affected = 0;
    if !owned.inner.is_null() {
        let mut inner = owned.inner;
        let status = ddb_stmt_free(&mut inner);
        owned.inner = ptr::null_mut();
        if status != DDB_OK {
            let _ = fail_with_status(owned.owner, status);
        } else {
            clear_error(owned.owner);
        }
    }
}

#[no_mangle]
pub extern "C" fn decentdb_checkpoint(db: *mut decentdb_db) -> c_int {
    if db.is_null() {
        return fail_with_message(
            ptr::null_mut(),
            LEGACY_ERR_INTERNAL,
            "db handle must not be null",
        );
    }
    // SAFETY: pointer checked above.
    let status = unsafe { ddb_db_checkpoint((*db).inner) };
    if status == DDB_OK {
        clear_error(db);
        0
    } else {
        fail_with_status(db, status)
    }
}

#[no_mangle]
pub extern "C" fn decentdb_save_as(db: *mut decentdb_db, dest_path_utf8: *const c_char) -> c_int {
    if db.is_null() || dest_path_utf8.is_null() {
        return fail_with_message(
            db,
            LEGACY_ERR_INTERNAL,
            "db and dest_path_utf8 must not be null",
        );
    }
    // SAFETY: pointers checked above.
    let status = unsafe { ddb_db_save_as((*db).inner, dest_path_utf8) };
    if status == DDB_OK {
        clear_error(db);
        0
    } else {
        fail_with_status(db, status)
    }
}

#[no_mangle]
pub extern "C" fn decentdb_free(p: *mut c_void) {
    if p.is_null() {
        return;
    }
    let mut as_cstr = p.cast::<c_char>();
    let _ = ddb_string_free(&mut as_cstr);
}

#[no_mangle]
pub extern "C" fn decentdb_list_tables_json(
    db: *mut decentdb_db,
    out_len: *mut c_int,
) -> *const c_char {
    if db.is_null() {
        let _ = fail_with_message(
            ptr::null_mut(),
            LEGACY_ERR_INTERNAL,
            "db handle must not be null",
        );
        return ptr::null();
    }
    let mut json_ptr: *mut c_char = ptr::null_mut();
    // SAFETY: pointer checked above.
    let status = unsafe { ddb_db_list_tables_json((*db).inner, &mut json_ptr) };
    if status != DDB_OK {
        let _ = fail_with_status(db, status);
        return ptr::null();
    }
    prepare_json_result(db, out_len, json_ptr)
}

#[no_mangle]
pub extern "C" fn decentdb_get_table_columns_json(
    db: *mut decentdb_db,
    table_utf8: *const c_char,
    out_len: *mut c_int,
) -> *const c_char {
    if db.is_null() || table_utf8.is_null() {
        let _ = fail_with_message(
            db,
            LEGACY_ERR_INTERNAL,
            "db and table_utf8 must not be null",
        );
        return ptr::null();
    }
    let mut json_ptr: *mut c_char = ptr::null_mut();
    // SAFETY: pointers checked above.
    let status = unsafe { ddb_db_describe_table_json((*db).inner, table_utf8, &mut json_ptr) };
    if status != DDB_OK {
        let _ = fail_with_status(db, status);
        return ptr::null();
    }
    prepare_json_result(db, out_len, json_ptr)
}

#[no_mangle]
pub extern "C" fn decentdb_list_indexes_json(
    db: *mut decentdb_db,
    out_len: *mut c_int,
) -> *const c_char {
    if db.is_null() {
        let _ = fail_with_message(
            ptr::null_mut(),
            LEGACY_ERR_INTERNAL,
            "db handle must not be null",
        );
        return ptr::null();
    }
    let mut json_ptr: *mut c_char = ptr::null_mut();
    // SAFETY: pointer checked above.
    let status = unsafe { ddb_db_list_indexes_json((*db).inner, &mut json_ptr) };
    if status != DDB_OK {
        let _ = fail_with_status(db, status);
        return ptr::null();
    }
    prepare_json_result(db, out_len, json_ptr)
}

#[no_mangle]
pub extern "C" fn decentdb_list_views_json(
    db: *mut decentdb_db,
    out_len: *mut c_int,
) -> *const c_char {
    if db.is_null() {
        let _ = fail_with_message(
            ptr::null_mut(),
            LEGACY_ERR_INTERNAL,
            "db handle must not be null",
        );
        return ptr::null();
    }
    let mut json_ptr: *mut c_char = ptr::null_mut();
    // SAFETY: pointer checked above.
    let status = unsafe { ddb_db_list_views_json((*db).inner, &mut json_ptr) };
    if status != DDB_OK {
        let _ = fail_with_status(db, status);
        return ptr::null();
    }
    prepare_json_result(db, out_len, json_ptr)
}

#[no_mangle]
pub extern "C" fn decentdb_get_view_ddl(
    db: *mut decentdb_db,
    view_utf8: *const c_char,
    out_len: *mut c_int,
) -> *const c_char {
    if db.is_null() || view_utf8.is_null() {
        let _ = fail_with_message(db, LEGACY_ERR_INTERNAL, "db and view_utf8 must not be null");
        return ptr::null();
    }
    let mut ddl_ptr: *mut c_char = ptr::null_mut();
    // SAFETY: pointers checked above.
    let status = unsafe { ddb_db_get_view_ddl((*db).inner, view_utf8, &mut ddl_ptr) };
    if status != DDB_OK {
        let _ = fail_with_status(db, status);
        return ptr::null();
    }
    prepare_json_result(db, out_len, ddl_ptr)
}
