//! Browser-facing wasm exports for `@decentdb/web`.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use js_sys::Uint8Array;
use serde_json::{json, Value as JsonValue};
use wasm_bindgen::prelude::*;

use crate::browser_result::encode_query_result_binary;
use crate::config::DbConfig;
use crate::db::Db;
use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::vfs::opfs::OpfsVfs;
use crate::vfs::VfsHandle;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_export_db, catch)]
    fn js_export_db(path: &str) -> std::result::Result<Uint8Array, JsValue>;
}

#[wasm_bindgen]
pub struct WebDb {
    path: String,
    db: Option<Db>,
}

#[wasm_bindgen]
impl WebDb {
    #[wasm_bindgen(js_name = execJson)]
    pub fn exec_json(&self, sql: &str, params_json: &str) -> std::result::Result<String, JsValue> {
        let params = parse_params(params_json).map_err(js_db_error)?;
        let result = self
            .db()?
            .execute_with_params(sql, &params)
            .map_err(js_db_error)?;
        result_to_json_string(&result).map_err(js_db_error)
    }

    #[wasm_bindgen(js_name = execBinary)]
    pub fn exec_binary(
        &self,
        sql: &str,
        params_json: &str,
    ) -> std::result::Result<Uint8Array, JsValue> {
        let params = parse_params(params_json).map_err(js_db_error)?;
        let result = self
            .db()?
            .execute_with_params(sql, &params)
            .map_err(js_db_error)?;
        let bytes = encode_query_result_binary(&result).map_err(js_db_error)?;
        Ok(Uint8Array::from(bytes.as_slice()))
    }

    #[wasm_bindgen(js_name = checkpoint)]
    pub fn checkpoint(&self) -> std::result::Result<(), JsValue> {
        self.db()?.checkpoint_wal().map_err(js_db_error)
    }

    #[wasm_bindgen(js_name = exportBytes)]
    pub fn export_bytes(&self) -> std::result::Result<Uint8Array, JsValue> {
        self.db()?.checkpoint_wal().map_err(js_db_error)?;
        js_export_db(&self.path)
    }

    #[wasm_bindgen(js_name = importBytes)]
    pub fn import_bytes(&mut self, _bytes: Uint8Array) -> std::result::Result<(), JsValue> {
        Err(JsValue::from_str(
            "importBytes is managed by the @decentdb/web worker so OPFS handles can be closed and reopened safely",
        ))
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&mut self) {
        let _ = self.db.take();
    }

    fn db(&self) -> std::result::Result<&Db, JsValue> {
        self.db
            .as_ref()
            .ok_or_else(|| JsValue::from_str("DecentDB handle is closed"))
    }
}

#[wasm_bindgen(js_name = decentdbOpen)]
pub fn decentdb_open(path: &str, mode: &str) -> std::result::Result<WebDb, JsValue> {
    Ok(WebDb {
        path: path.to_string(),
        db: Some(open_db(path, mode).map_err(js_db_error)?),
    })
}

#[wasm_bindgen(js_name = decentdbVersion)]
pub fn decentdb_version() -> String {
    crate::version().to_string()
}

fn open_db(path: &str, mode: &str) -> Result<Db> {
    let mut config = DbConfig::default();
    config.background_checkpoint_worker = false;
    config.auto_checkpoint_on_open_mb = 0;
    let vfs = VfsHandle::from_vfs(Arc::new(OpfsVfs));
    match mode {
        "create" | "createNew" => Db::create_with_vfs(Path::new(path), config, vfs),
        "open" | "openExisting" => Db::open_existing_with_vfs(Path::new(path), config, vfs),
        "openOrCreate" | "" => Db::open_or_create_with_vfs(Path::new(path), config, vfs),
        other => Err(DbError::sql(format!(
            "unsupported browser open mode: {other}"
        ))),
    }
}

fn parse_params(params_json: &str) -> Result<Vec<Value>> {
    if params_json.trim().is_empty() {
        return Ok(Vec::new());
    }
    let value: JsonValue = serde_json::from_str(params_json)
        .map_err(|error| DbError::sql(format!("invalid params JSON: {error}")))?;
    let JsonValue::Array(values) = value else {
        return Err(DbError::sql("params JSON must be an array"));
    };
    values.into_iter().map(json_to_value).collect()
}

fn json_to_value(value: JsonValue) -> Result<Value> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(value) => Ok(Value::Bool(value)),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float64(value))
            } else {
                Err(DbError::sql("numeric parameter is outside supported range"))
            }
        }
        JsonValue::String(value) => Ok(Value::Text(value)),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(DbError::sql(
            "browser parameters currently support null, bool, number, and string values",
        )),
    }
}

fn result_to_json_string(result: &crate::QueryResult) -> Result<String> {
    let rows = result
        .rows()
        .iter()
        .map(|row| {
            let mut object = serde_json::Map::new();
            for (index, value) in row.values().iter().enumerate() {
                let column = result
                    .columns()
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| format!("column_{index}"));
                object.insert(column, value_to_json(value));
            }
            JsonValue::Object(object)
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&json!({
        "columns": result.columns(),
        "rows": rows,
        "affectedRows": result.affected_rows(),
    }))
    .map_err(|error| DbError::internal(format!("serialize browser query result: {error}")))
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Int64(value) => json!(value),
        Value::Float64(value) => json!(value),
        Value::Bool(value) => json!(value),
        Value::Text(value) => json!(value),
        Value::Blob(value) | Value::Geometry(value) | Value::Geography(value) => {
            json!({ "kind": "bytes", "base64": base64_encode(value) })
        }
        Value::Decimal { scaled, scale } => json!({
            "kind": "decimal",
            "scaled": scaled.to_string(),
            "scale": scale,
        }),
        Value::Uuid(bytes) => json!({ "kind": "uuid", "bytes": bytes }),
        Value::TimestampMicros(value) => json!({ "kind": "timestampMicros", "value": value }),
        Value::Enum {
            enum_type_id,
            label_id,
        } => json!({
            "kind": "enum",
            "enumTypeId": enum_type_id,
            "labelId": label_id,
        }),
        Value::IpAddr { family, addr } => {
            json!({ "kind": "ipaddr", "family": family, "bytes": addr })
        }
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => json!({
            "kind": "cidr",
            "family": family,
            "prefixLen": prefix_len,
            "bytes": network,
        }),
        Value::MacAddr { len, bytes } => {
            json!({ "kind": "macaddr", "len": len, "bytes": &bytes[..usize::from(*len)] })
        }
        Value::DateDays(value) => json!({ "kind": "dateDays", "value": value }),
        Value::TimeMicros(value) => json!({ "kind": "timeMicros", "value": value }),
        Value::TimestampTzMicros(value) => json!({ "kind": "timestampTzMicros", "value": value }),
        Value::Interval {
            months,
            days,
            micros,
        } => json!({
            "kind": "interval",
            "months": months,
            "days": days,
            "micros": micros,
        }),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        let triple = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        output.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        output.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            output.push(TABLE[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(TABLE[(triple & 0x3f) as usize] as char);
        } else {
            output.push('=');
        }
    }
    output
}

fn js_db_error(error: DbError) -> JsValue {
    let mut message = error.to_string();
    if let Some(source) = error.source() {
        message.push_str(": ");
        message.push_str(&source.to_string());
    }
    JsValue::from_str(&message)
}
