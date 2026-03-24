//! Stable C ABI for DecentDB.

use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::panic::{self, AssertUnwindSafe};
use std::ptr;

use crate::db::PreparedStatement;
use crate::error::{DbError, DbErrorCode, Result};
use crate::metadata::{ColumnInfo, ForeignKeyInfo, IndexInfo, TableInfo, TriggerInfo, ViewInfo};
use crate::{evict_shared_wal, Db, DbConfig, QueryResult, Value};

const DDB_OK: u32 = 0;
const DDB_ABI_VERSION: u32 = 1;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdbValueTag {
    Null = 0,
    Int64 = 1,
    Float64 = 2,
    Bool = 3,
    Text = 4,
    Blob = 5,
    Decimal = 6,
    Uuid = 7,
    TimestampMicros = 8,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DdbValue {
    pub tag: u32,
    pub bool_value: u8,
    pub reserved0: [u8; 7],
    pub int64_value: i64,
    pub float64_value: f64,
    pub decimal_scaled: i64,
    pub decimal_scale: u8,
    pub reserved1: [u8; 7],
    pub data: *mut u8,
    pub len: usize,
    pub uuid_bytes: [u8; 16],
    pub timestamp_micros: i64,
}

impl Default for DdbValue {
    fn default() -> Self {
        Self {
            tag: DdbValueTag::Null as u32,
            bool_value: 0,
            reserved0: [0; 7],
            int64_value: 0,
            float64_value: 0.0,
            decimal_scaled: 0,
            decimal_scale: 0,
            reserved1: [0; 7],
            data: ptr::null_mut(),
            len: 0,
            uuid_bytes: [0; 16],
            timestamp_micros: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DdbValueView {
    pub tag: u32,
    pub bool_value: u8,
    pub reserved0: [u8; 7],
    pub int64_value: i64,
    pub float64_value: f64,
    pub decimal_scaled: i64,
    pub decimal_scale: u8,
    pub reserved1: [u8; 7],
    pub data: *const u8,
    pub len: usize,
    pub uuid_bytes: [u8; 16],
    pub timestamp_micros: i64,
}

impl Default for DdbValueView {
    fn default() -> Self {
        Self {
            tag: DdbValueTag::Null as u32,
            bool_value: 0,
            reserved0: [0; 7],
            int64_value: 0,
            float64_value: 0.0,
            decimal_scaled: 0,
            decimal_scale: 0,
            reserved1: [0; 7],
            data: ptr::null(),
            len: 0,
            uuid_bytes: [0; 16],
            timestamp_micros: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DdbRowI64TextF64View {
    pub int64_value: i64,
    pub text_data: *const u8,
    pub text_len: usize,
    pub float64_value: f64,
}

impl Default for DdbRowI64TextF64View {
    fn default() -> Self {
        Self {
            int64_value: 0,
            text_data: ptr::null(),
            text_len: 0,
            float64_value: 0.0,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct DbHandle {
    db: Db,
}

#[repr(C)]
#[derive(Debug)]
pub struct ResultHandle {
    result: QueryResult,
}

#[repr(C)]
#[derive(Debug)]
pub struct StmtHandle {
    db: Db,
    sql: String,
    prepared: PreparedStatement,
    bindings: Vec<Value>,
    result: Option<QueryResult>,
    current_row: Option<usize>,
    next_row_index: usize,
    row_views: Vec<DdbValueView>,
    row_i64_text_f64_views: Vec<DdbRowI64TextF64View>,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn ffi_boundary<F>(op: F) -> u32
where
    F: FnOnce() -> Result<()>,
{
    clear_last_error();
    match panic::catch_unwind(AssertUnwindSafe(op)) {
        Ok(Ok(())) => DDB_OK,
        Ok(Err(error)) => {
            set_last_error(error.to_string());
            error.numeric_code()
        }
        Err(payload) => {
            set_last_error(panic_payload_message(payload));
            DbErrorCode::Panic.as_u32()
        }
    }
}

fn ffi_cstr_boundary<F>(op: F) -> *const c_char
where
    F: FnOnce() -> *const c_char,
{
    match panic::catch_unwind(AssertUnwindSafe(op)) {
        Ok(value) => value,
        Err(payload) => {
            set_last_error(panic_payload_message(payload));
            ptr::null()
        }
    }
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

fn set_last_error(message: String) {
    let sanitized = message.replace('\0', " ");
    let cstring =
        CString::new(sanitized).unwrap_or_else(|_| CString::new("invalid error").expect("literal"));
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(cstring);
    });
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        format!("panic: {message}")
    } else if let Some(message) = payload.downcast_ref::<String>() {
        format!("panic: {message}")
    } else {
        "panic: non-string payload".to_string()
    }
}

fn cstring_from_string(value: String) -> Result<*mut c_char> {
    CString::new(value)
        .map(CString::into_raw)
        .map_err(|_| DbError::internal("string contains an interior NUL"))
}

fn out_ptr<'a, T>(ptr: *mut T, name: &str) -> Result<&'a mut T> {
    if ptr.is_null() {
        return Err(DbError::internal(format!("{name} must not be null")));
    }
    // SAFETY: null was checked above and the caller owns the out-parameter storage.
    Ok(unsafe { &mut *ptr })
}

fn ref_ptr<'a, T>(ptr: *const T, name: &str) -> Result<&'a T> {
    if ptr.is_null() {
        return Err(DbError::internal(format!("{name} must not be null")));
    }
    // SAFETY: null was checked above and the caller promises the pointer is valid for reads.
    Ok(unsafe { &*ptr })
}

fn handle_ref<'a, T>(ptr: *const T, name: &str) -> Result<&'a T> {
    ref_ptr(ptr, name)
}

fn handle_mut<'a, T>(ptr: *mut T, name: &str) -> Result<&'a mut T> {
    out_ptr(ptr, name)
}

fn c_string_arg<'a>(ptr: *const c_char, name: &str) -> Result<&'a CStr> {
    if ptr.is_null() {
        return Err(DbError::internal(format!("{name} must not be null")));
    }
    // SAFETY: null was checked above and the caller must pass a valid NUL-terminated string.
    Ok(unsafe { CStr::from_ptr(ptr) })
}

fn utf8_arg(ptr: *const c_char, name: &str) -> Result<String> {
    c_string_arg(ptr, name)?
        .to_str()
        .map(|value| value.to_string())
        .map_err(|error| DbError::sql(format!("{name} is not valid UTF-8: {error}")))
}

fn params_slice<'a>(params: *const DdbValue, params_len: usize) -> Result<&'a [DdbValue]> {
    if params.is_null() {
        if params_len == 0 {
            return Ok(&[]);
        }
        return Err(DbError::internal(
            "params pointer must not be null when params_len > 0",
        ));
    }
    // SAFETY: the caller provides a contiguous array of `params_len` values.
    Ok(unsafe { std::slice::from_raw_parts(params, params_len) })
}

fn ptr_slice<'a, T>(ptr: *const T, len: usize, name: &str) -> Result<&'a [T]> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(DbError::internal(format!(
            "{name} pointer must not be null when len > 0"
        )));
    }
    // SAFETY: the caller provides a contiguous buffer of `len` entries.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

fn value_from_ffi(value: &DdbValue) -> Result<Value> {
    match value.tag {
        x if x == DdbValueTag::Null as u32 => Ok(Value::Null),
        x if x == DdbValueTag::Int64 as u32 => Ok(Value::Int64(value.int64_value)),
        x if x == DdbValueTag::Float64 as u32 => Ok(Value::Float64(value.float64_value)),
        x if x == DdbValueTag::Bool as u32 => Ok(Value::Bool(value.bool_value != 0)),
        x if x == DdbValueTag::Text as u32 => {
            let bytes = borrowed_bytes(value.data.cast_const(), value.len)?;
            let text = std::str::from_utf8(bytes).map_err(|error| {
                DbError::sql(format!("TEXT parameter is not valid UTF-8: {error}"))
            })?;
            Ok(Value::Text(text.to_string()))
        }
        x if x == DdbValueTag::Blob as u32 => {
            let bytes = borrowed_bytes(value.data.cast_const(), value.len)?;
            Ok(Value::Blob(bytes.to_vec()))
        }
        x if x == DdbValueTag::Decimal as u32 => Ok(Value::Decimal {
            scaled: value.decimal_scaled,
            scale: value.decimal_scale,
        }),
        x if x == DdbValueTag::Uuid as u32 => Ok(Value::Uuid(value.uuid_bytes)),
        x if x == DdbValueTag::TimestampMicros as u32 => {
            Ok(Value::TimestampMicros(value.timestamp_micros))
        }
        other => Err(DbError::sql(format!("unsupported DDB value tag {other}"))),
    }
}

fn borrowed_bytes<'a>(data: *const u8, len: usize) -> Result<&'a [u8]> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(DbError::internal(
            "buffer pointer must not be null when len > 0",
        ));
    }
    // SAFETY: the caller provides a contiguous buffer of `len` bytes.
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

fn fill_ffi_value(out: &mut DdbValue, value: &Value) {
    ddb_value_reset(out);
    match value {
        Value::Null => out.tag = DdbValueTag::Null as u32,
        Value::Int64(inner) => {
            out.tag = DdbValueTag::Int64 as u32;
            out.int64_value = *inner;
        }
        Value::Float64(inner) => {
            out.tag = DdbValueTag::Float64 as u32;
            out.float64_value = *inner;
        }
        Value::Bool(inner) => {
            out.tag = DdbValueTag::Bool as u32;
            out.bool_value = u8::from(*inner);
        }
        Value::Text(inner) => {
            out.tag = DdbValueTag::Text as u32;
            out.data = owned_bytes(inner.as_bytes().to_vec());
            out.len = inner.len();
        }
        Value::Blob(inner) => {
            out.tag = DdbValueTag::Blob as u32;
            out.data = owned_bytes(inner.clone());
            out.len = inner.len();
        }
        Value::Decimal { scaled, scale } => {
            out.tag = DdbValueTag::Decimal as u32;
            out.decimal_scaled = *scaled;
            out.decimal_scale = *scale;
        }
        Value::Uuid(inner) => {
            out.tag = DdbValueTag::Uuid as u32;
            out.uuid_bytes = *inner;
        }
        Value::TimestampMicros(inner) => {
            out.tag = DdbValueTag::TimestampMicros as u32;
            out.timestamp_micros = *inner;
        }
    }
}

fn fill_ffi_value_view(out: &mut DdbValueView, value: &Value) {
    match value {
        Value::Null => out.tag = DdbValueTag::Null as u32,
        Value::Int64(inner) => {
            out.tag = DdbValueTag::Int64 as u32;
            out.int64_value = *inner;
        }
        Value::Float64(inner) => {
            out.tag = DdbValueTag::Float64 as u32;
            out.float64_value = *inner;
        }
        Value::Bool(inner) => {
            out.tag = DdbValueTag::Bool as u32;
            out.bool_value = u8::from(*inner);
        }
        Value::Text(inner) => {
            out.tag = DdbValueTag::Text as u32;
            out.data = inner.as_bytes().as_ptr();
            out.len = inner.len();
        }
        Value::Blob(inner) => {
            out.tag = DdbValueTag::Blob as u32;
            out.data = inner.as_ptr();
            out.len = inner.len();
        }
        Value::Decimal { scaled, scale } => {
            out.tag = DdbValueTag::Decimal as u32;
            out.decimal_scaled = *scaled;
            out.decimal_scale = *scale;
        }
        Value::Uuid(inner) => {
            out.tag = DdbValueTag::Uuid as u32;
            out.uuid_bytes = *inner;
        }
        Value::TimestampMicros(inner) => {
            out.tag = DdbValueTag::TimestampMicros as u32;
            out.timestamp_micros = *inner;
        }
    }
}

fn populate_stmt_row_views(stmt: &mut StmtHandle) -> Result<()> {
    let result = stmt
        .result
        .as_ref()
        .ok_or_else(|| DbError::sql("statement has not been executed yet"))?;
    let row_index = stmt
        .current_row
        .ok_or_else(|| DbError::sql("statement is not positioned on a row"))?;
    let row = result
        .rows()
        .get(row_index)
        .ok_or_else(|| DbError::internal("statement row cursor is out of bounds"))?;
    let values = row.values();

    stmt.row_views.resize(values.len(), DdbValueView::default());
    for (idx, value) in values.iter().enumerate() {
        fill_ffi_value_view(&mut stmt.row_views[idx], value);
    }
    Ok(())
}

fn row_i64_text_f64_view(result: &QueryResult, row_index: usize) -> Result<DdbRowI64TextF64View> {
    let row = result
        .rows()
        .get(row_index)
        .ok_or_else(|| DbError::internal("statement row cursor is out of bounds"))?;
    let values = row.values();
    if values.len() != 3 {
        return Err(DbError::sql(
            "statement row shape is not compatible with INT64/TEXT/FLOAT64 view",
        ));
    }
    match (&values[0], &values[1], &values[2]) {
        (Value::Int64(id), Value::Text(text), Value::Float64(number)) => Ok(DdbRowI64TextF64View {
            int64_value: *id,
            text_data: text.as_bytes().as_ptr(),
            text_len: text.len(),
            float64_value: *number,
        }),
        _ => Err(DbError::sql(
            "statement row shape is not compatible with INT64/TEXT/FLOAT64 view",
        )),
    }
}

fn populate_stmt_i64_text_f64_row_views(
    stmt: &mut StmtHandle,
    start_index: usize,
    row_count: usize,
) -> Result<()> {
    let result = stmt
        .result
        .as_ref()
        .ok_or_else(|| DbError::sql("statement has not been executed yet"))?;
    stmt.row_i64_text_f64_views.clear();
    stmt.row_i64_text_f64_views.reserve(row_count);
    for row_offset in 0..row_count {
        stmt.row_i64_text_f64_views
            .push(row_i64_text_f64_view(result, start_index + row_offset)?);
    }
    Ok(())
}

fn execute_stmt_if_needed(stmt: &mut StmtHandle) -> Result<()> {
    if stmt.result.is_some() {
        return Ok(());
    }

    let execute = || stmt.prepared.execute(&stmt.bindings);
    match execute() {
        Ok(result) => {
            stmt.result = Some(result);
            stmt.current_row = None;
            stmt.next_row_index = 0;
            Ok(())
        }
        Err(DbError::Sql { message }) if message.contains("schema changed") => {
            stmt.prepared = stmt.db.prepare(&stmt.sql)?;
            let result = stmt.prepared.execute(&stmt.bindings)?;
            stmt.result = Some(result);
            stmt.current_row = None;
            stmt.next_row_index = 0;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn invalidate_stmt_result(stmt: &mut StmtHandle) {
    stmt.result = None;
    stmt.current_row = None;
    stmt.next_row_index = 0;
    stmt.row_views.clear();
    stmt.row_i64_text_f64_views.clear();
}

fn ensure_stmt_binding_slot(stmt: &mut StmtHandle, index_1_based: usize) -> Result<usize> {
    if index_1_based == 0 {
        return Err(DbError::sql("statement parameter indexes are 1-based"));
    }
    let slot = index_1_based - 1;
    if stmt.bindings.len() <= slot {
        stmt.bindings.resize(slot + 1, Value::Null);
    }
    Ok(slot)
}

fn stmt_current_value(stmt: &StmtHandle, column_index: usize) -> Result<&Value> {
    let result = stmt
        .result
        .as_ref()
        .ok_or_else(|| DbError::sql("statement has not been executed yet"))?;
    let row_index = stmt
        .current_row
        .ok_or_else(|| DbError::sql("statement is not positioned on a row"))?;
    let row = result
        .rows()
        .get(row_index)
        .ok_or_else(|| DbError::internal("statement row cursor is out of bounds"))?;
    row.values()
        .get(column_index)
        .ok_or_else(|| DbError::sql(format!("column index {column_index} is out of bounds")))
}

fn owned_bytes(bytes: Vec<u8>) -> *mut u8 {
    if bytes.is_empty() {
        return ptr::null_mut();
    }
    let len = bytes.len();
    let boxed = bytes.into_boxed_slice();
    let raw = Box::into_raw(boxed);
    let raw_u8 = raw.cast::<u8>();
    debug_assert!(!raw_u8.is_null() || len == 0);
    raw_u8
}

fn free_owned_bytes(data: *mut u8, len: usize) {
    if data.is_null() || len == 0 {
        return;
    }
    // SAFETY: `data` was allocated from `owned_bytes` with exactly `len` bytes.
    unsafe {
        let slice = ptr::slice_from_raw_parts_mut(data, len);
        drop(Box::from_raw(slice));
    }
}

fn ddb_value_reset(value: &mut DdbValue) {
    *value = DdbValue::default();
}

fn json_escape(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn json_string(value: &str) -> String {
    format!("\"{}\"", json_escape(value))
}

fn json_bool(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

fn json_optional_string(value: Option<&str>) -> String {
    value.map_or_else(|| "null".to_string(), json_string)
}

fn json_string_list(values: &[String]) -> String {
    let joined = values
        .iter()
        .map(|value| json_string(value))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{joined}]")
}

fn foreign_key_json(info: &ForeignKeyInfo) -> String {
    format!(
        "{{\"name\":{},\"columns\":{},\"referenced_table\":{},\"referenced_columns\":{},\"on_delete\":{},\"on_update\":{}}}",
        json_optional_string(info.name.as_deref()),
        json_string_list(&info.columns),
        json_string(&info.referenced_table),
        json_string_list(&info.referenced_columns),
        json_string(&info.on_delete),
        json_string(&info.on_update),
    )
}

fn column_json(info: &ColumnInfo) -> String {
    let checks = json_string_list(&info.checks);
    let foreign_key = info
        .foreign_key
        .as_ref()
        .map_or_else(|| "null".to_string(), foreign_key_json);
    format!(
        "{{\"name\":{},\"column_type\":{},\"nullable\":{},\"default_sql\":{},\"primary_key\":{},\"unique\":{},\"auto_increment\":{},\"checks\":{},\"foreign_key\":{}}}",
        json_string(&info.name),
        json_string(&info.column_type),
        json_bool(info.nullable),
        json_optional_string(info.default_sql.as_deref()),
        json_bool(info.primary_key),
        json_bool(info.unique),
        json_bool(info.auto_increment),
        checks,
        foreign_key,
    )
}

fn table_json(info: &TableInfo) -> String {
    let columns = info
        .columns
        .iter()
        .map(column_json)
        .collect::<Vec<_>>()
        .join(",");
    let checks = json_string_list(&info.checks);
    let foreign_keys = info
        .foreign_keys
        .iter()
        .map(foreign_key_json)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"name\":{},\"columns\":[{}],\"checks\":{},\"foreign_keys\":[{}],\"primary_key_columns\":{},\"row_count\":{}}}",
        json_string(&info.name),
        columns,
        checks,
        foreign_keys,
        json_string_list(&info.primary_key_columns),
        info.row_count,
    )
}

fn index_json(info: &IndexInfo) -> String {
    format!(
        "{{\"name\":{},\"table_name\":{},\"kind\":{},\"unique\":{},\"columns\":{},\"predicate_sql\":{},\"fresh\":{}}}",
        json_string(&info.name),
        json_string(&info.table_name),
        json_string(&info.kind),
        json_bool(info.unique),
        json_string_list(&info.columns),
        json_optional_string(info.predicate_sql.as_deref()),
        json_bool(info.fresh),
    )
}

fn view_json(info: &ViewInfo) -> String {
    format!(
        "{{\"name\":{},\"sql_text\":{},\"column_names\":{},\"dependencies\":{}}}",
        json_string(&info.name),
        json_string(&info.sql_text),
        json_string_list(&info.column_names),
        json_string_list(&info.dependencies),
    )
}

fn trigger_json(info: &TriggerInfo) -> String {
    format!(
        "{{\"name\":{},\"target_name\":{},\"kind\":{},\"event\":{},\"on_view\":{},\"action_sql\":{}}}",
        json_string(&info.name),
        json_string(&info.target_name),
        json_string(&info.kind),
        json_string(&info.event),
        json_bool(info.on_view),
        json_string(&info.action_sql),
    )
}

fn json_list<T>(items: &[T], render: impl Fn(&T) -> String) -> String {
    let body = items.iter().map(render).collect::<Vec<_>>().join(",");
    format!("[{body}]")
}

#[no_mangle]
pub extern "C" fn ddb_abi_version() -> u32 {
    DDB_ABI_VERSION
}

#[no_mangle]
pub extern "C" fn ddb_version() -> *const c_char {
    static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");
    ffi_cstr_boundary(|| VERSION.as_ptr().cast())
}

#[no_mangle]
pub extern "C" fn ddb_last_error_message() -> *const c_char {
    ffi_cstr_boundary(|| {
        LAST_ERROR.with(|slot| {
            slot.borrow()
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr())
        })
    })
}

#[no_mangle]
pub extern "C" fn ddb_value_init(value: *mut DdbValue) -> u32 {
    ffi_boundary(|| {
        *out_ptr(value, "value")? = DdbValue::default();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_value_dispose(value: *mut DdbValue) -> u32 {
    ffi_boundary(|| {
        let value = out_ptr(value, "value")?;
        if matches!(
            value.tag,
            x if x == DdbValueTag::Text as u32 || x == DdbValueTag::Blob as u32
        ) {
            free_owned_bytes(value.data, value.len);
        }
        ddb_value_reset(value);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_string_free(value: *mut *mut c_char) -> u32 {
    ffi_boundary(|| {
        let value = out_ptr(value, "value")?;
        if (*value).is_null() {
            return Ok(());
        }
        // SAFETY: pointer was created by `CString::into_raw` in this module.
        unsafe {
            drop(CString::from_raw(*value));
        }
        *value = ptr::null_mut();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_create(path: *const c_char, out_db: *mut *mut DbHandle) -> u32 {
    ffi_boundary(|| {
        let path = utf8_arg(path, "path")?;
        let handle = Box::new(DbHandle {
            db: Db::create(path, DbConfig::default())?,
        });
        *out_ptr(out_db, "out_db")? = Box::into_raw(handle);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_open(path: *const c_char, out_db: *mut *mut DbHandle) -> u32 {
    ffi_boundary(|| {
        let path = utf8_arg(path, "path")?;
        let handle = Box::new(DbHandle {
            db: Db::open(path, DbConfig::default())?,
        });
        *out_ptr(out_db, "out_db")? = Box::into_raw(handle);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_open_or_create(path: *const c_char, out_db: *mut *mut DbHandle) -> u32 {
    ffi_boundary(|| {
        let path = utf8_arg(path, "path")?;
        let handle = Box::new(DbHandle {
            db: Db::open_or_create(path, DbConfig::default())?,
        });
        *out_ptr(out_db, "out_db")? = Box::into_raw(handle);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_free(db: *mut *mut DbHandle) -> u32 {
    ffi_boundary(|| {
        let db = out_ptr(db, "db")?;
        if (*db).is_null() {
            return Ok(());
        }
        // SAFETY: pointer was created by `Box::into_raw` in this module.
        unsafe {
            drop(Box::from_raw(*db));
        }
        *db = ptr::null_mut();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_execute(
    db: *mut DbHandle,
    sql: *const c_char,
    params: *const DdbValue,
    params_len: usize,
    out_result: *mut *mut ResultHandle,
) -> u32 {
    ffi_boundary(|| {
        let db = handle_ref(db, "db")?;
        let sql = utf8_arg(sql, "sql")?;
        let rust_params = params_slice(params, params_len)?
            .iter()
            .map(value_from_ffi)
            .collect::<Result<Vec<_>>>()?;
        let result = db.db.execute_with_params(&sql, &rust_params)?;
        *out_ptr(out_result, "out_result")? = Box::into_raw(Box::new(ResultHandle { result }));
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_checkpoint(db: *mut DbHandle) -> u32 {
    ffi_boundary(|| handle_ref(db, "db")?.db.checkpoint())
}

#[no_mangle]
pub extern "C" fn ddb_db_begin_transaction(db: *mut DbHandle) -> u32 {
    ffi_boundary(|| handle_ref(db, "db")?.db.begin_transaction())
}

#[no_mangle]
pub extern "C" fn ddb_db_commit_transaction(db: *mut DbHandle, out_lsn: *mut u64) -> u32 {
    ffi_boundary(|| {
        let lsn = handle_ref(db, "db")?.db.commit_transaction()?;
        *out_ptr(out_lsn, "out_lsn")? = lsn;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_rollback_transaction(db: *mut DbHandle) -> u32 {
    ffi_boundary(|| handle_ref(db, "db")?.db.rollback_transaction())
}

#[no_mangle]
pub extern "C" fn ddb_db_in_transaction(db: *mut DbHandle, out_flag: *mut u8) -> u32 {
    ffi_boundary(|| {
        *out_ptr(out_flag, "out_flag")? = u8::from(handle_ref(db, "db")?.db.in_transaction()?);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_save_as(db: *mut DbHandle, dest_path: *const c_char) -> u32 {
    ffi_boundary(|| {
        let dest = utf8_arg(dest_path, "dest_path")?;
        handle_ref(db, "db")?.db.save_as(dest)
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_prepare(
    db: *mut DbHandle,
    sql: *const c_char,
    out_stmt: *mut *mut StmtHandle,
) -> u32 {
    ffi_boundary(|| {
        let db = handle_ref(db, "db")?;
        let sql = utf8_arg(sql, "sql")?;
        let prepared = db.db.prepare(&sql)?;
        let handle = Box::new(StmtHandle {
            db: db.db.clone(),
            sql,
            prepared,
            bindings: Vec::new(),
            result: None,
            current_row: None,
            next_row_index: 0,
            row_views: Vec::new(),
            row_i64_text_f64_views: Vec::new(),
        });
        *out_ptr(out_stmt, "out_stmt")? = Box::into_raw(handle);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_free(stmt: *mut *mut StmtHandle) -> u32 {
    ffi_boundary(|| {
        let stmt = out_ptr(stmt, "stmt")?;
        if (*stmt).is_null() {
            return Ok(());
        }
        unsafe {
            drop(Box::from_raw(*stmt));
        }
        *stmt = ptr::null_mut();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_reset(stmt: *mut StmtHandle) -> u32 {
    ffi_boundary(|| {
        invalidate_stmt_result(handle_mut(stmt, "stmt")?);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_clear_bindings(stmt: *mut StmtHandle) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        stmt.bindings.clear();
        invalidate_stmt_result(stmt);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_null(stmt: *mut StmtHandle, index_1_based: usize) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::Null;
        invalidate_stmt_result(stmt);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_int64(
    stmt: *mut StmtHandle,
    index_1_based: usize,
    value: i64,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::Int64(value);
        invalidate_stmt_result(stmt);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_int64_step_row_view(
    stmt: *mut StmtHandle,
    index_1_based: usize,
    value: i64,
    out_values: *mut *const DdbValueView,
    out_columns: *mut usize,
    out_has_row: *mut u8,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::Int64(value);
        invalidate_stmt_result(stmt);
        execute_stmt_if_needed(stmt)?;

        let row_count = stmt
            .result
            .as_ref()
            .ok_or_else(|| DbError::internal("statement execution did not produce a result"))?
            .rows()
            .len();
        if stmt.next_row_index >= row_count {
            stmt.current_row = None;
            *out_ptr(out_has_row, "out_has_row")? = 0;
            *out_ptr(out_columns, "out_columns")? = 0;
            *out_ptr(out_values, "out_values")? = ptr::null();
            return Ok(());
        }

        stmt.current_row = Some(stmt.next_row_index);
        stmt.next_row_index += 1;
        populate_stmt_row_views(stmt)?;

        *out_ptr(out_has_row, "out_has_row")? = 1;
        *out_ptr(out_columns, "out_columns")? = stmt.row_views.len();
        *out_ptr(out_values, "out_values")? = if stmt.row_views.is_empty() {
            ptr::null()
        } else {
            stmt.row_views.as_ptr()
        };
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_int64_step_i64_text_f64(
    stmt: *mut StmtHandle,
    index_1_based: usize,
    value: i64,
    out_int64: *mut i64,
    out_text_data: *mut *const u8,
    out_text_len: *mut usize,
    out_float64: *mut f64,
    out_has_row: *mut u8,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::Int64(value);
        invalidate_stmt_result(stmt);
        execute_stmt_if_needed(stmt)?;

        let row_count = stmt
            .result
            .as_ref()
            .ok_or_else(|| DbError::internal("statement execution did not produce a result"))?
            .rows()
            .len();
        if stmt.next_row_index >= row_count {
            stmt.current_row = None;
            *out_ptr(out_has_row, "out_has_row")? = 0;
            *out_ptr(out_int64, "out_int64")? = 0;
            *out_ptr(out_text_data, "out_text_data")? = ptr::null();
            *out_ptr(out_text_len, "out_text_len")? = 0;
            *out_ptr(out_float64, "out_float64")? = 0.0;
            return Ok(());
        }

        let row_index = stmt.next_row_index;
        stmt.current_row = Some(row_index);
        stmt.next_row_index += 1;
        let row = {
            let result = stmt
                .result
                .as_ref()
                .ok_or_else(|| DbError::internal("statement execution did not produce a result"))?;
            row_i64_text_f64_view(result, row_index)?
        };

        *out_ptr(out_has_row, "out_has_row")? = 1;
        *out_ptr(out_int64, "out_int64")? = row.int64_value;
        *out_ptr(out_text_data, "out_text_data")? = row.text_data;
        *out_ptr(out_text_len, "out_text_len")? = row.text_len;
        *out_ptr(out_float64, "out_float64")? = row.float64_value;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_float64(
    stmt: *mut StmtHandle,
    index_1_based: usize,
    value: f64,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::Float64(value);
        invalidate_stmt_result(stmt);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_bool(
    stmt: *mut StmtHandle,
    index_1_based: usize,
    value: u8,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::Bool(value != 0);
        invalidate_stmt_result(stmt);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_text(
    stmt: *mut StmtHandle,
    index_1_based: usize,
    value: *const c_char,
    byte_len: usize,
) -> u32 {
    ffi_boundary(|| {
        let bytes = borrowed_bytes(value.cast::<u8>(), byte_len)?;
        let text = std::str::from_utf8(bytes)
            .map_err(|error| DbError::sql(format!("TEXT parameter is not valid UTF-8: {error}")))?;
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::Text(text.to_string());
        invalidate_stmt_result(stmt);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_blob(
    stmt: *mut StmtHandle,
    index_1_based: usize,
    data: *const u8,
    byte_len: usize,
) -> u32 {
    ffi_boundary(|| {
        let bytes = borrowed_bytes(data, byte_len)?;
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::Blob(bytes.to_vec());
        invalidate_stmt_result(stmt);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_decimal(
    stmt: *mut StmtHandle,
    index_1_based: usize,
    scaled: i64,
    scale: u8,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::Decimal { scaled, scale };
        invalidate_stmt_result(stmt);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_bind_timestamp_micros(
    stmt: *mut StmtHandle,
    index_1_based: usize,
    timestamp_micros: i64,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let slot = ensure_stmt_binding_slot(stmt, index_1_based)?;
        stmt.bindings[slot] = Value::TimestampMicros(timestamp_micros);
        invalidate_stmt_result(stmt);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_execute_batch_i64(
    stmt: *mut StmtHandle,
    row_count: usize,
    values_i64: *const i64,
    out_total_affected_rows: *mut u64,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let ids = ptr_slice(values_i64, row_count, "values_i64")?;
        let total_affected = stmt.db.execute_prepared_batch_with_builder(
            &stmt.prepared,
            row_count,
            1,
            |row_index, params| {
                params[0] = Value::Int64(ids[row_index]);
                Ok(())
            },
        )?;
        invalidate_stmt_result(stmt);

        *out_ptr(out_total_affected_rows, "out_total_affected_rows")? = total_affected;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_execute_batch_i64_text_f64(
    stmt: *mut StmtHandle,
    row_count: usize,
    values_i64: *const i64,
    values_text_ptrs: *const *const c_char,
    values_text_lens: *const usize,
    values_f64: *const f64,
    out_total_affected_rows: *mut u64,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        let ids = ptr_slice(values_i64, row_count, "values_i64")?;
        let text_ptrs = ptr_slice(values_text_ptrs, row_count, "values_text_ptrs")?;
        let text_lens = ptr_slice(values_text_lens, row_count, "values_text_lens")?;
        let floats = ptr_slice(values_f64, row_count, "values_f64")?;
        let total_affected = stmt.db.execute_prepared_batch_with_builder(
            &stmt.prepared,
            row_count,
            3,
            |idx, params| {
                let text_bytes = borrowed_bytes(text_ptrs[idx].cast::<u8>(), text_lens[idx])?;
                let text = std::str::from_utf8(text_bytes).map_err(|error| {
                    DbError::sql(format!(
                        "TEXT parameter at batch row {idx} is not valid UTF-8: {error}"
                    ))
                })?;
                params[0] = Value::Int64(ids[idx]);
                params[1] = Value::Text(text.to_string());
                params[2] = Value::Float64(floats[idx]);
                Ok(())
            },
        )?;
        invalidate_stmt_result(stmt);

        *out_ptr(out_total_affected_rows, "out_total_affected_rows")? = total_affected;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_step(stmt: *mut StmtHandle, out_has_row: *mut u8) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        execute_stmt_if_needed(stmt)?;
        let result = stmt
            .result
            .as_ref()
            .ok_or_else(|| DbError::internal("statement execution did not produce a result"))?;
        if stmt.next_row_index >= result.rows().len() {
            stmt.current_row = None;
            *out_ptr(out_has_row, "out_has_row")? = 0;
            return Ok(());
        }
        stmt.current_row = Some(stmt.next_row_index);
        stmt.next_row_index += 1;
        *out_ptr(out_has_row, "out_has_row")? = 1;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_column_count(stmt: *mut StmtHandle, out_columns: *mut usize) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        execute_stmt_if_needed(stmt)?;
        *out_ptr(out_columns, "out_columns")? = stmt
            .result
            .as_ref()
            .map_or(0, |result| result.columns().len());
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_column_name_copy(
    stmt: *mut StmtHandle,
    column_index: usize,
    out_name: *mut *mut c_char,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        execute_stmt_if_needed(stmt)?;
        let result = stmt
            .result
            .as_ref()
            .ok_or_else(|| DbError::sql("statement has not been executed yet"))?;
        let column = result
            .columns()
            .get(column_index)
            .ok_or_else(|| DbError::sql(format!("column index {column_index} is out of bounds")))?;
        *out_ptr(out_name, "out_name")? = CString::new(column.as_str())
            .map_err(|_| DbError::internal("column name contains an interior NUL"))?
            .into_raw();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_affected_rows(stmt: *mut StmtHandle, out_rows: *mut u64) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        execute_stmt_if_needed(stmt)?;
        *out_ptr(out_rows, "out_rows")? =
            stmt.result.as_ref().map_or(0, QueryResult::affected_rows);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_value_copy(
    stmt: *mut StmtHandle,
    column_index: usize,
    out_value: *mut DdbValue,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        execute_stmt_if_needed(stmt)?;
        let value = stmt_current_value(stmt, column_index)?;
        fill_ffi_value(out_ptr(out_value, "out_value")?, value);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_row_view(
    stmt: *mut StmtHandle,
    out_values: *mut *const DdbValueView,
    out_columns: *mut usize,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        execute_stmt_if_needed(stmt)?;
        populate_stmt_row_views(stmt)?;

        *out_ptr(out_columns, "out_columns")? = stmt.row_views.len();
        *out_ptr(out_values, "out_values")? = if stmt.row_views.is_empty() {
            ptr::null()
        } else {
            stmt.row_views.as_ptr()
        };
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_step_row_view(
    stmt: *mut StmtHandle,
    out_values: *mut *const DdbValueView,
    out_columns: *mut usize,
    out_has_row: *mut u8,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        execute_stmt_if_needed(stmt)?;

        let row_count = stmt
            .result
            .as_ref()
            .ok_or_else(|| DbError::internal("statement execution did not produce a result"))?
            .rows()
            .len();
        if stmt.next_row_index >= row_count {
            stmt.current_row = None;
            *out_ptr(out_has_row, "out_has_row")? = 0;
            *out_ptr(out_columns, "out_columns")? = 0;
            *out_ptr(out_values, "out_values")? = ptr::null();
            return Ok(());
        }

        stmt.current_row = Some(stmt.next_row_index);
        stmt.next_row_index += 1;
        populate_stmt_row_views(stmt)?;

        *out_ptr(out_has_row, "out_has_row")? = 1;
        *out_ptr(out_columns, "out_columns")? = stmt.row_views.len();
        *out_ptr(out_values, "out_values")? = if stmt.row_views.is_empty() {
            ptr::null()
        } else {
            stmt.row_views.as_ptr()
        };
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_fetch_row_views(
    stmt: *mut StmtHandle,
    include_current_row: u8,
    max_rows: usize,
    out_values: *mut *const DdbValueView,
    out_rows: *mut usize,
    out_columns: *mut usize,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        execute_stmt_if_needed(stmt)?;

        let result = stmt
            .result
            .as_ref()
            .ok_or_else(|| DbError::internal("statement execution did not produce a result"))?;
        let total_rows = result.rows().len();
        let col_count = result.columns().len();
        let start_index = if include_current_row != 0 {
            stmt.current_row.unwrap_or(stmt.next_row_index)
        } else {
            stmt.next_row_index
        };
        if start_index >= total_rows {
            stmt.current_row = None;
            stmt.next_row_index = total_rows;
            stmt.row_views.clear();
            *out_ptr(out_rows, "out_rows")? = 0;
            *out_ptr(out_columns, "out_columns")? = col_count;
            *out_ptr(out_values, "out_values")? = ptr::null();
            return Ok(());
        }

        let available_rows = total_rows - start_index;
        let fetch_rows = if max_rows == 0 {
            available_rows
        } else {
            available_rows.min(max_rows)
        };
        let view_len = fetch_rows.saturating_mul(col_count);
        stmt.row_views.resize(view_len, DdbValueView::default());

        for row_offset in 0..fetch_rows {
            let row = &result.rows()[start_index + row_offset];
            for (col, value) in row.values().iter().enumerate() {
                let idx = row_offset * col_count + col;
                fill_ffi_value_view(&mut stmt.row_views[idx], value);
            }
        }

        let last_index = start_index + fetch_rows - 1;
        stmt.current_row = Some(last_index);
        stmt.next_row_index = last_index + 1;

        *out_ptr(out_rows, "out_rows")? = fetch_rows;
        *out_ptr(out_columns, "out_columns")? = col_count;
        *out_ptr(out_values, "out_values")? = if stmt.row_views.is_empty() {
            ptr::null()
        } else {
            stmt.row_views.as_ptr()
        };
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_stmt_fetch_rows_i64_text_f64(
    stmt: *mut StmtHandle,
    include_current_row: u8,
    max_rows: usize,
    out_rows_ptr: *mut *const DdbRowI64TextF64View,
    out_rows: *mut usize,
) -> u32 {
    ffi_boundary(|| {
        let stmt = handle_mut(stmt, "stmt")?;
        execute_stmt_if_needed(stmt)?;

        let result = stmt
            .result
            .as_ref()
            .ok_or_else(|| DbError::internal("statement execution did not produce a result"))?;
        let total_rows = result.rows().len();
        let start_index = if include_current_row != 0 {
            stmt.current_row.unwrap_or(stmt.next_row_index)
        } else {
            stmt.next_row_index
        };
        if start_index >= total_rows {
            stmt.current_row = None;
            stmt.next_row_index = total_rows;
            stmt.row_i64_text_f64_views.clear();
            *out_ptr(out_rows, "out_rows")? = 0;
            *out_ptr(out_rows_ptr, "out_rows_ptr")? = ptr::null();
            return Ok(());
        }

        let available_rows = total_rows - start_index;
        let fetch_rows = if max_rows == 0 {
            available_rows
        } else {
            available_rows.min(max_rows)
        };

        populate_stmt_i64_text_f64_row_views(stmt, start_index, fetch_rows)?;
        let last_index = start_index + fetch_rows - 1;
        stmt.current_row = Some(last_index);
        stmt.next_row_index = last_index + 1;

        *out_ptr(out_rows, "out_rows")? = fetch_rows;
        *out_ptr(out_rows_ptr, "out_rows_ptr")? = if stmt.row_i64_text_f64_views.is_empty() {
            ptr::null()
        } else {
            stmt.row_i64_text_f64_views.as_ptr()
        };
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_list_tables_json(db: *mut DbHandle, out_json: *mut *mut c_char) -> u32 {
    ffi_boundary(|| {
        let tables = handle_ref(db, "db")?.db.list_tables()?;
        *out_ptr(out_json, "out_json")? = cstring_from_string(json_list(&tables, table_json))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_describe_table_json(
    db: *mut DbHandle,
    name: *const c_char,
    out_json: *mut *mut c_char,
) -> u32 {
    ffi_boundary(|| {
        let table = handle_ref(db, "db")?
            .db
            .describe_table(&utf8_arg(name, "name")?)?;
        *out_ptr(out_json, "out_json")? = cstring_from_string(table_json(&table))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_get_table_ddl(
    db: *mut DbHandle,
    name: *const c_char,
    out_ddl: *mut *mut c_char,
) -> u32 {
    ffi_boundary(|| {
        let ddl = handle_ref(db, "db")?
            .db
            .table_ddl(&utf8_arg(name, "name")?)?;
        *out_ptr(out_ddl, "out_ddl")? = cstring_from_string(ddl)?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_list_indexes_json(db: *mut DbHandle, out_json: *mut *mut c_char) -> u32 {
    ffi_boundary(|| {
        let indexes = handle_ref(db, "db")?.db.list_indexes()?;
        *out_ptr(out_json, "out_json")? = cstring_from_string(json_list(&indexes, index_json))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_list_views_json(db: *mut DbHandle, out_json: *mut *mut c_char) -> u32 {
    ffi_boundary(|| {
        let views = handle_ref(db, "db")?.db.list_views()?;
        *out_ptr(out_json, "out_json")? = cstring_from_string(json_list(&views, view_json))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_get_view_ddl(
    db: *mut DbHandle,
    name: *const c_char,
    out_ddl: *mut *mut c_char,
) -> u32 {
    ffi_boundary(|| {
        let ddl = handle_ref(db, "db")?
            .db
            .view_ddl(&utf8_arg(name, "name")?)?;
        *out_ptr(out_ddl, "out_ddl")? = cstring_from_string(ddl)?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_db_list_triggers_json(db: *mut DbHandle, out_json: *mut *mut c_char) -> u32 {
    ffi_boundary(|| {
        let triggers = handle_ref(db, "db")?.db.list_triggers()?;
        *out_ptr(out_json, "out_json")? = cstring_from_string(json_list(&triggers, trigger_json))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_evict_shared_wal(path: *const c_char) -> u32 {
    ffi_boundary(|| {
        let path = utf8_arg(path, "path")?;
        evict_shared_wal(path)
    })
}

#[no_mangle]
pub extern "C" fn ddb_result_free(result: *mut *mut ResultHandle) -> u32 {
    ffi_boundary(|| {
        let result = out_ptr(result, "result")?;
        if (*result).is_null() {
            return Ok(());
        }
        // SAFETY: pointer was created by `Box::into_raw` in this module.
        unsafe {
            drop(Box::from_raw(*result));
        }
        *result = ptr::null_mut();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_result_row_count(result: *mut ResultHandle, out_rows: *mut usize) -> u32 {
    ffi_boundary(|| {
        *out_ptr(out_rows, "out_rows")? = handle_ref(result, "result")?.result.rows().len();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_result_column_count(
    result: *mut ResultHandle,
    out_columns: *mut usize,
) -> u32 {
    ffi_boundary(|| {
        *out_ptr(out_columns, "out_columns")? =
            handle_ref(result, "result")?.result.columns().len();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_result_affected_rows(result: *mut ResultHandle, out_rows: *mut u64) -> u32 {
    ffi_boundary(|| {
        *out_ptr(out_rows, "out_rows")? = handle_ref(result, "result")?.result.affected_rows();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_result_column_name_copy(
    result: *mut ResultHandle,
    column_index: usize,
    out_name: *mut *mut c_char,
) -> u32 {
    ffi_boundary(|| {
        let result = handle_ref(result, "result")?;
        let column =
            result.result.columns().get(column_index).ok_or_else(|| {
                DbError::sql(format!("column index {column_index} is out of bounds"))
            })?;
        let cstring = CString::new(column.as_str())
            .map_err(|_| DbError::internal("column name contains an interior NUL"))?;
        *out_ptr(out_name, "out_name")? = cstring.into_raw();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn ddb_result_value_copy(
    result: *mut ResultHandle,
    row_index: usize,
    column_index: usize,
    out_value: *mut DdbValue,
) -> u32 {
    ffi_boundary(|| {
        let result = handle_ref(result, "result")?;
        let row = result
            .result
            .rows()
            .get(row_index)
            .ok_or_else(|| DbError::sql(format!("row index {row_index} is out of bounds")))?;
        let value = row
            .values()
            .get(column_index)
            .ok_or_else(|| DbError::sql(format!("column index {column_index} is out of bounds")))?;
        fill_ffi_value(out_ptr(out_value, "out_value")?, value);
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    type ExecuteFn = extern "C" fn(
        *mut DbHandle,
        *const c_char,
        *const DdbValue,
        usize,
        *mut *mut ResultHandle,
    ) -> u32;

    #[test]
    fn abi_shape_matches_expected_layout() {
        let _execute: ExecuteFn = ddb_db_execute;
        assert_eq!(std::mem::size_of::<DdbValue>(), 88);
        assert_eq!(std::mem::align_of::<DdbValue>(), 8);
    }

    #[test]
    fn abi_version_is_stable() {
        assert_eq!(ddb_abi_version(), DDB_ABI_VERSION);
    }

    #[test]
    fn ffi_boundary_converts_panics_into_panic_error_code() {
        let code = ffi_boundary(|| -> Result<()> { panic!("boom") });
        assert_eq!(code, DbErrorCode::Panic.as_u32());
        let message = LAST_ERROR.with(|slot| {
            slot.borrow()
                .as_ref()
                .expect("panic message")
                .to_str()
                .expect("utf8")
                .to_string()
        });
        assert!(message.contains("boom"));
    }

    #[test]
    fn result_and_handle_free_are_idempotent_when_callers_null_out_pointers() {
        let mut db = ptr::null_mut();
        let path = CString::new(":memory:").expect("path");
        assert_eq!(
            ddb_db_open_or_create(path.as_ptr(), &mut db),
            DDB_OK,
            "open_or_create failed: {:?}",
            unsafe { CStr::from_ptr(ddb_last_error_message()) }
        );

        let sql = CString::new("SELECT 1").expect("sql");
        let mut result = ptr::null_mut();
        assert_eq!(
            ddb_db_execute(db, sql.as_ptr(), ptr::null(), 0, &mut result),
            DDB_OK
        );
        assert_eq!(ddb_result_free(&mut result), DDB_OK);
        assert_eq!(ddb_result_free(&mut result), DDB_OK);
        assert_eq!(ddb_db_free(&mut db), DDB_OK);
        assert_eq!(ddb_db_free(&mut db), DDB_OK);
    }

    #[test]
    fn ffi_roundtrip_executes_and_copies_values() {
        let mut db = ptr::null_mut();
        let path = CString::new(":memory:").expect("path");
        assert_eq!(ddb_db_open_or_create(path.as_ptr(), &mut db), DDB_OK);

        let create =
            CString::new("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT)").expect("create");
        let mut result = ptr::null_mut();
        assert_eq!(
            ddb_db_execute(db, create.as_ptr(), ptr::null(), 0, &mut result),
            DDB_OK
        );
        assert_eq!(ddb_result_free(&mut result), DDB_OK);

        let params = [
            DdbValue {
                tag: DdbValueTag::Int64 as u32,
                int64_value: 1,
                ..DdbValue::default()
            },
            DdbValue {
                tag: DdbValueTag::Text as u32,
                data: b"Ada".as_ptr().cast_mut(),
                len: 3,
                ..DdbValue::default()
            },
        ];
        let insert = CString::new("INSERT INTO items (id, name) VALUES ($1, $2)").expect("insert");
        assert_eq!(
            ddb_db_execute(
                db,
                insert.as_ptr(),
                params.as_ptr(),
                params.len(),
                &mut result
            ),
            DDB_OK
        );
        assert_eq!(ddb_result_free(&mut result), DDB_OK);

        let select = CString::new("SELECT id, name FROM items").expect("select");
        assert_eq!(
            ddb_db_execute(db, select.as_ptr(), ptr::null(), 0, &mut result),
            DDB_OK
        );

        let mut columns = 0;
        let mut rows = 0;
        assert_eq!(ddb_result_column_count(result, &mut columns), DDB_OK);
        assert_eq!(ddb_result_row_count(result, &mut rows), DDB_OK);
        assert_eq!(columns, 2);
        assert_eq!(rows, 1);

        let mut copied = DdbValue::default();
        assert_eq!(ddb_result_value_copy(result, 0, 1, &mut copied), DDB_OK);
        let text = std::str::from_utf8(unsafe {
            std::slice::from_raw_parts(copied.data.cast_const(), copied.len)
        })
        .expect("text");
        assert_eq!(text, "Ada");
        assert_eq!(ddb_value_dispose(&mut copied), DDB_OK);
        assert_eq!(ddb_result_free(&mut result), DDB_OK);
        assert_eq!(ddb_db_free(&mut db), DDB_OK);
    }

    #[test]
    fn metadata_json_helpers_return_current_catalog_state() {
        let mut db = ptr::null_mut();
        let path = CString::new(":memory:").expect("path");
        assert_eq!(ddb_db_open_or_create(path.as_ptr(), &mut db), DDB_OK);

        let mut result = ptr::null_mut();
        for sql in [
            "CREATE TABLE parent (id INT64 PRIMARY KEY)",
            "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64 REFERENCES parent(id) ON DELETE CASCADE)",
            "CREATE INDEX idx_child_parent ON child (parent_id)",
            "CREATE VIEW child_ids AS SELECT id, parent_id FROM child",
            "CREATE TABLE audit_log (msg TEXT)",
            "CREATE TRIGGER child_ai AFTER INSERT ON child FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log VALUES (''changed'')')",
            "INSERT INTO parent VALUES (1)",
            "INSERT INTO child VALUES (1, 1)",
        ] {
            let sql = CString::new(sql).expect("sql");
            assert_eq!(ddb_db_execute(db, sql.as_ptr(), ptr::null(), 0, &mut result), DDB_OK);
            assert_eq!(ddb_result_free(&mut result), DDB_OK);
        }

        fn take_json(slot: &mut *mut c_char) -> String {
            let raw = *slot;
            assert!(!raw.is_null(), "json pointer should be populated");
            let value = unsafe { CStr::from_ptr(raw) }
                .to_str()
                .expect("utf8")
                .to_string();
            assert_eq!(ddb_string_free(slot), DDB_OK);
            value
        }

        let mut tables_json = ptr::null_mut();
        assert_eq!(ddb_db_list_tables_json(db, &mut tables_json), DDB_OK);
        let tables_json = take_json(&mut tables_json);
        assert!(tables_json.contains("\"name\":\"child\""));
        assert!(tables_json.contains("\"row_count\":1"));

        let child_name = CString::new("child").expect("name");
        let mut describe_json = ptr::null_mut();
        assert_eq!(
            ddb_db_describe_table_json(db, child_name.as_ptr(), &mut describe_json),
            DDB_OK
        );
        let describe_json = take_json(&mut describe_json);
        assert!(describe_json.contains("\"primary_key\":true"));
        assert!(describe_json.contains("\"referenced_table\":\"parent\""));
        assert!(describe_json.contains("\"on_delete\":\"CASCADE\""));

        let mut table_ddl = ptr::null_mut();
        assert_eq!(
            ddb_db_get_table_ddl(db, child_name.as_ptr(), &mut table_ddl),
            DDB_OK
        );
        let table_ddl = take_json(&mut table_ddl);
        assert!(table_ddl.contains("CREATE TABLE \"child\""));

        let mut indexes_json = ptr::null_mut();
        assert_eq!(ddb_db_list_indexes_json(db, &mut indexes_json), DDB_OK);
        let indexes_json = take_json(&mut indexes_json);
        assert!(indexes_json.contains("\"name\":\"idx_child_parent\""));
        assert!(indexes_json.contains("\"kind\":\"btree\""));

        let mut views_json = ptr::null_mut();
        assert_eq!(ddb_db_list_views_json(db, &mut views_json), DDB_OK);
        let views_json = take_json(&mut views_json);
        assert!(views_json.contains("\"name\":\"child_ids\""));
        assert!(views_json.contains("\"dependencies\":[\"child\"]"));

        let view_name = CString::new("child_ids").expect("view");
        let mut view_ddl = ptr::null_mut();
        assert_eq!(
            ddb_db_get_view_ddl(db, view_name.as_ptr(), &mut view_ddl),
            DDB_OK
        );
        let view_ddl = take_json(&mut view_ddl);
        assert!(view_ddl.contains("CREATE VIEW \"child_ids\""));

        let mut triggers_json = ptr::null_mut();
        assert_eq!(ddb_db_list_triggers_json(db, &mut triggers_json), DDB_OK);
        let triggers_json = take_json(&mut triggers_json);
        assert!(triggers_json.contains("\"name\":\"child_ai\""));
        assert!(triggers_json.contains("\"target_name\":\"child\""));

        assert_eq!(ddb_db_free(&mut db), DDB_OK);
    }

    #[test]
    fn ffi_prepared_delete_with_correlated_exists_reports_affected_rows() {
        let mut db = ptr::null_mut();
        let path = CString::new(":memory:").expect("path");
        assert_eq!(ddb_db_open_or_create(path.as_ptr(), &mut db), DDB_OK);

        let mut result = ptr::null_mut();
        for sql in [
            r#"CREATE TABLE "del_artists" ("Id" INT64 PRIMARY KEY, "LibraryId" INT64, "Name" TEXT)"#,
            r#"CREATE TABLE "del_contributors" ("Id" INT64 PRIMARY KEY, "ArtistId" INT64, "Name" TEXT)"#,
            r#"INSERT INTO "del_artists" VALUES (1, 10, 'Artist1')"#,
            r#"INSERT INTO "del_artists" VALUES (2, 20, 'Artist2')"#,
            r#"INSERT INTO "del_contributors" VALUES (1, 1, 'Contrib1')"#,
            r#"INSERT INTO "del_contributors" VALUES (2, 2, 'Contrib2')"#,
        ] {
            let sql = CString::new(sql).expect("sql");
            assert_eq!(
                ddb_db_execute(db, sql.as_ptr(), ptr::null(), 0, &mut result),
                DDB_OK
            );
            assert_eq!(ddb_result_free(&mut result), DDB_OK);
        }

        let delete = CString::new(
            r#"
            DELETE FROM "del_contributors"
            WHERE EXISTS (
                SELECT 1 FROM "del_contributors" AS "c"
                INNER JOIN "del_artists" AS "a" ON "c"."ArtistId" = "a"."Id"
                WHERE "a"."LibraryId" = $1
                AND "del_contributors"."Id" = "c"."Id"
            )"#,
        )
        .expect("delete sql");
        let mut stmt = ptr::null_mut();
        assert_eq!(ddb_db_prepare(db, delete.as_ptr(), &mut stmt), DDB_OK);
        assert_eq!(ddb_stmt_bind_int64(stmt, 1, 10), DDB_OK);

        let mut has_row = 1;
        assert_eq!(ddb_stmt_step(stmt, &mut has_row), DDB_OK);
        assert_eq!(has_row, 0);

        let mut affected_rows = 0;
        assert_eq!(ddb_stmt_affected_rows(stmt, &mut affected_rows), DDB_OK);
        assert_eq!(affected_rows, 1);

        let count = CString::new(r#"SELECT COUNT(*) FROM "del_contributors""#).expect("count sql");
        assert_eq!(
            ddb_db_execute(db, count.as_ptr(), ptr::null(), 0, &mut result),
            DDB_OK
        );
        let mut remaining = DdbValue::default();
        assert_eq!(ddb_result_value_copy(result, 0, 0, &mut remaining), DDB_OK);
        assert_eq!(remaining.int64_value, 1);
        assert_eq!(ddb_value_dispose(&mut remaining), DDB_OK);
        assert_eq!(ddb_result_free(&mut result), DDB_OK);
        assert_eq!(ddb_stmt_free(&mut stmt), DDB_OK);
        assert_eq!(ddb_db_free(&mut db), DDB_OK);
    }

    #[test]
    fn ffi_stmt_execute_batch_i64_text_f64_inserts_rows() {
        let mut db = ptr::null_mut();
        let path = CString::new(":memory:").expect("path");
        assert_eq!(ddb_db_open_or_create(path.as_ptr(), &mut db), DDB_OK);

        let create =
            CString::new("CREATE TABLE t (id INT64, name TEXT, score FLOAT64)").expect("create");
        let mut result = ptr::null_mut();
        assert_eq!(
            ddb_db_execute(db, create.as_ptr(), ptr::null(), 0, &mut result),
            DDB_OK
        );
        assert_eq!(ddb_result_free(&mut result), DDB_OK);

        let insert = CString::new("INSERT INTO t VALUES ($1, $2, $3)").expect("insert");
        let mut stmt = ptr::null_mut();
        assert_eq!(ddb_db_prepare(db, insert.as_ptr(), &mut stmt), DDB_OK);

        let ids = [1_i64, 2_i64, 3_i64];
        let names = [
            CString::new("a").expect("a"),
            CString::new("b").expect("b"),
            CString::new("c").expect("c"),
        ];
        let name_ptrs = names.iter().map(|name| name.as_ptr()).collect::<Vec<_>>();
        let name_lens = names
            .iter()
            .map(|name| name.as_bytes().len())
            .collect::<Vec<_>>();
        let scores = [1.5_f64, 2.5_f64, 3.5_f64];
        let mut affected_rows = 0_u64;
        assert_eq!(
            ddb_stmt_execute_batch_i64_text_f64(
                stmt,
                ids.len(),
                ids.as_ptr(),
                name_ptrs.as_ptr(),
                name_lens.as_ptr(),
                scores.as_ptr(),
                &mut affected_rows
            ),
            DDB_OK
        );
        assert_eq!(affected_rows, 3);

        let count_sql = CString::new("SELECT COUNT(*) FROM t").expect("count");
        assert_eq!(
            ddb_db_execute(db, count_sql.as_ptr(), ptr::null(), 0, &mut result),
            DDB_OK
        );
        let mut count_value = DdbValue::default();
        assert_eq!(
            ddb_result_value_copy(result, 0, 0, &mut count_value),
            DDB_OK
        );
        assert_eq!(count_value.int64_value, 3);
        assert_eq!(ddb_value_dispose(&mut count_value), DDB_OK);
        assert_eq!(ddb_result_free(&mut result), DDB_OK);
        assert_eq!(ddb_stmt_free(&mut stmt), DDB_OK);
        assert_eq!(ddb_db_free(&mut db), DDB_OK);
    }

    #[test]
    fn ffi_stmt_execute_batch_i64_inserts_rows() {
        let mut db = ptr::null_mut();
        let path = CString::new(":memory:").expect("path");
        assert_eq!(ddb_db_open_or_create(path.as_ptr(), &mut db), DDB_OK);

        let create = CString::new("CREATE TABLE t (id INT64)").expect("create");
        let mut result = ptr::null_mut();
        assert_eq!(
            ddb_db_execute(db, create.as_ptr(), ptr::null(), 0, &mut result),
            DDB_OK
        );
        assert_eq!(ddb_result_free(&mut result), DDB_OK);

        let insert = CString::new("INSERT INTO t VALUES ($1)").expect("insert");
        let mut stmt = ptr::null_mut();
        assert_eq!(ddb_db_prepare(db, insert.as_ptr(), &mut stmt), DDB_OK);

        let ids = [10_i64, 20_i64, 30_i64, 40_i64];
        let mut affected_rows = 0_u64;
        assert_eq!(
            ddb_stmt_execute_batch_i64(stmt, ids.len(), ids.as_ptr(), &mut affected_rows),
            DDB_OK
        );
        assert_eq!(affected_rows, 4);

        let count_sql = CString::new("SELECT COUNT(*) FROM t").expect("count");
        assert_eq!(
            ddb_db_execute(db, count_sql.as_ptr(), ptr::null(), 0, &mut result),
            DDB_OK
        );
        let mut count_value = DdbValue::default();
        assert_eq!(
            ddb_result_value_copy(result, 0, 0, &mut count_value),
            DDB_OK
        );
        assert_eq!(count_value.int64_value, 4);
        assert_eq!(ddb_value_dispose(&mut count_value), DDB_OK);
        assert_eq!(ddb_result_free(&mut result), DDB_OK);
        assert_eq!(ddb_stmt_free(&mut stmt), DDB_OK);
        assert_eq!(ddb_db_free(&mut db), DDB_OK);
    }
}
