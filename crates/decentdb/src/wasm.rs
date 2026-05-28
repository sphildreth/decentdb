//! Browser-facing wasm exports for `@decentdb/web`.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use js_sys::{Object, Reflect, Uint8Array, JSON};
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

    #[wasm_bindgen(js_name = syncExecuteJson)]
    pub fn sync_execute_json(&self, request_json: &str) -> std::result::Result<String, JsValue> {
        let request: JsonValue = serde_json::from_str(request_json)
            .map_err(|error| js_db_error(DbError::sql(format!("invalid sync JSON: {error}"))))?;
        let object = request
            .as_object()
            .ok_or_else(|| js_db_error(DbError::sql("sync JSON request must be an object")))?;
        let op = object
            .get("op")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| js_db_error(DbError::sql("sync JSON request requires op")))?;
        match op {
            "changeset_apply" => {
                let changeset = object.get("changeset").cloned().ok_or_else(|| {
                    js_db_error(DbError::sql("changeset_apply requires changeset"))
                })?;
                let changeset: crate::sync::SyncChangeset = serde_json::from_value(changeset)
                    .map_err(|error| {
                        js_db_error(DbError::sql(format!("invalid changeset payload: {error}")))
                    })?;
                let options = object
                    .get("options")
                    .cloned()
                    .map(serde_json::from_value::<crate::sync::ApplyChangesetOptions>)
                    .transpose()
                    .map_err(|error| {
                        js_db_error(DbError::sql(format!(
                            "invalid changeset apply options: {error}"
                        )))
                    })?
                    .unwrap_or_default();
                let result = self
                    .db()?
                    .sync_apply_changeset(&changeset, options)
                    .map_err(js_db_error)?;
                serde_json::to_string(&result).map_err(|error| {
                    js_db_error(DbError::internal(format!(
                        "serialize changeset apply result: {error}"
                    )))
                })
            }
            other => Err(js_db_error(DbError::sql(format!(
                "unsupported browser sync op: {other}"
            )))),
        }
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
        JsonValue::Array(_) => Err(DbError::sql(
            "browser parameters currently do not accept bare array values; use tagged values",
        )),
        JsonValue::Object(value) => json_object_to_value(value),
    }
}

fn json_object_to_value(value: serde_json::Map<String, JsonValue>) -> Result<Value> {
    let Some(kind) = value.get("kind").and_then(JsonValue::as_str) else {
        return Err(DbError::sql(
            "browser object parameters require a string 'kind' field",
        ));
    };

    match kind {
        "bytes" => {
            let base64 = value
                .get("base64")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| DbError::sql("bytes parameter requires base64"))?;
            Ok(Value::Blob(base64_decode(base64)?))
        }
        "int64" => {
            let encoded = value
                .get("value")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| DbError::sql("int64 parameter requires string value"))?;
            let parsed = encoded
                .parse::<i64>()
                .map_err(|_| DbError::sql("invalid int64 parameter"))?;
            Ok(Value::Int64(parsed))
        }
        "decimal" => {
            let scaled = value
                .get("scaled")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| DbError::sql("decimal parameter requires scaled string"))?;
            let scaled = scaled
                .parse::<i64>()
                .map_err(|_| DbError::sql("invalid decimal scaled value"))?;
            let scale = value
                .get("scale")
                .and_then(JsonValue::as_u64)
                .ok_or_else(|| DbError::sql("decimal parameter requires numeric scale"))?;
            let scale =
                u8::try_from(scale).map_err(|_| DbError::sql("decimal scale is too large"))?;
            Ok(Value::Decimal { scaled, scale })
        }
        "uuid" => {
            let bytes = parse_fixed_bytes(&value, "bytes", 16)?;
            let mut uuid = [0u8; 16];
            uuid.copy_from_slice(&bytes);
            Ok(Value::Uuid(uuid))
        }
        "timestampMicros" => {
            let encoded = value
                .get("value")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| DbError::sql("timestampMicros requires string value"))?;
            let parsed = encoded
                .parse::<i64>()
                .map_err(|_| DbError::sql("invalid timestampMicros value"))?;
            Ok(Value::TimestampMicros(parsed))
        }
        "dateDays" => {
            let parsed = value
                .get("value")
                .and_then(JsonValue::as_i64)
                .ok_or_else(|| DbError::sql("dateDays requires numeric value"))?;
            let parsed =
                i32::try_from(parsed).map_err(|_| DbError::sql("dateDays value out of range"))?;
            Ok(Value::DateDays(parsed))
        }
        "timeMicros" => {
            let encoded = value
                .get("value")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| DbError::sql("timeMicros requires string value"))?;
            let parsed = encoded
                .parse::<i64>()
                .map_err(|_| DbError::sql("invalid timeMicros value"))?;
            Ok(Value::TimeMicros(parsed))
        }
        "timestampTzMicros" => {
            let encoded = value
                .get("value")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| DbError::sql("timestampTzMicros requires string value"))?;
            let parsed = encoded
                .parse::<i64>()
                .map_err(|_| DbError::sql("invalid timestampTzMicros value"))?;
            Ok(Value::TimestampTzMicros(parsed))
        }
        "interval" => {
            let months = value
                .get("months")
                .and_then(JsonValue::as_i64)
                .ok_or_else(|| DbError::sql("interval requires numeric months"))?;
            let days = value
                .get("days")
                .and_then(JsonValue::as_i64)
                .ok_or_else(|| DbError::sql("interval requires numeric days"))?;
            let micros = value
                .get("micros")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| DbError::sql("interval requires string micros"))?;
            let months =
                i32::try_from(months).map_err(|_| DbError::sql("interval months out of range"))?;
            let days =
                i32::try_from(days).map_err(|_| DbError::sql("interval days out of range"))?;
            let micros = micros
                .parse::<i64>()
                .map_err(|_| DbError::sql("interval micros is invalid"))?;
            Ok(Value::Interval {
                months,
                days,
                micros,
            })
        }
        "geometry" => Ok(Value::Geometry(parse_dynamic_bytes(&value)?)),
        "geography" => Ok(Value::Geography(parse_dynamic_bytes(&value)?)),
        _ => Err(DbError::sql(format!(
            "unsupported browser tagged parameter kind: {kind}"
        ))),
    }
}

fn parse_dynamic_bytes(value: &serde_json::Map<String, JsonValue>) -> Result<Vec<u8>> {
    if let Some(base64) = value.get("base64").and_then(JsonValue::as_str) {
        return base64_decode(base64);
    }
    if let Some(bytes_value) = value.get("bytes") {
        return parse_vec_bytes(bytes_value);
    }
    Err(DbError::sql("tagged bytes value requires base64 or bytes"))
}

fn parse_fixed_bytes(
    value: &serde_json::Map<String, JsonValue>,
    key: &str,
    len: usize,
) -> Result<Vec<u8>> {
    let Some(bytes_value) = value.get(key) else {
        return Err(DbError::sql(format!("{key} is required")));
    };
    let parsed = parse_vec_bytes(bytes_value)?;
    if parsed.len() != len {
        return Err(DbError::sql(format!(
            "{key} must contain exactly {len} bytes"
        )));
    }
    Ok(parsed)
}

fn parse_vec_bytes(value: &JsonValue) -> Result<Vec<u8>> {
    let JsonValue::Array(items) = value else {
        return Err(DbError::sql("bytes must be an array of numbers"));
    };
    items
        .iter()
        .map(|item| {
            let value = item
                .as_u64()
                .ok_or_else(|| DbError::sql("bytes array contains non-numeric value"))?;
            u8::try_from(value).map_err(|_| DbError::sql("bytes array value out of range"))
        })
        .collect()
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

fn base64_decode(encoded: &str) -> Result<Vec<u8>> {
    fn decode_char(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let bytes = encoded.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err(DbError::sql("base64 string length must be a multiple of 4"));
    }

    let mut output = Vec::with_capacity((bytes.len() / 4) * 3);
    for chunk in bytes.chunks(4) {
        let c0 = chunk[0];
        let c1 = chunk[1];
        let c2 = chunk[2];
        let c3 = chunk[3];

        let v0 = decode_char(c0).ok_or_else(|| DbError::sql("invalid base64 character"))?;
        let v1 = decode_char(c1).ok_or_else(|| DbError::sql("invalid base64 character"))?;
        let v2 = if c2 == b'=' {
            0
        } else {
            decode_char(c2).ok_or_else(|| DbError::sql("invalid base64 character"))?
        };
        let v3 = if c3 == b'=' {
            0
        } else {
            decode_char(c3).ok_or_else(|| DbError::sql("invalid base64 character"))?
        };

        let triple =
            (u32::from(v0) << 18) | (u32::from(v1) << 12) | (u32::from(v2) << 6) | u32::from(v3);
        output.push(((triple >> 16) & 0xff) as u8);
        if c2 != b'=' {
            output.push(((triple >> 8) & 0xff) as u8);
        }
        if c3 != b'=' {
            output.push((triple & 0xff) as u8);
        }
    }
    Ok(output)
}

fn js_db_error(error: DbError) -> JsValue {
    let mut message = error.to_string();
    if let Some(source) = error.source() {
        message.push_str(": ");
        message.push_str(&source.to_string());
    }
    let mut diagnostic = error.diagnostic();
    diagnostic.message = message.clone();
    let diagnostic_json = diagnostic.to_json().unwrap_or_else(|_| "{}".to_string());

    let object = Object::new();
    set_js_property(&object, "code", JsValue::from_str(diagnostic.code_name));
    set_js_property(&object, "message", JsValue::from_str(&message));
    set_js_property(
        &object,
        "nativeCode",
        JsValue::from_f64(f64::from(diagnostic.code.as_u32())),
    );
    set_js_property(&object, "subcode", JsValue::from_str(diagnostic.subcode));
    if let Some(sqlstate) = diagnostic.sqlstate {
        set_js_property(&object, "sqlstate", JsValue::from_str(sqlstate));
    }
    set_js_property(
        &object,
        "retryable",
        JsValue::from_bool(diagnostic.retryable),
    );
    set_js_property(
        &object,
        "permanent",
        JsValue::from_bool(diagnostic.permanent),
    );
    set_js_property(
        &object,
        "diagnosticJson",
        JsValue::from_str(&diagnostic_json),
    );
    if let Ok(parsed) = JSON::parse(&diagnostic_json) {
        set_js_property(&object, "diagnostic", parsed);
    }
    object.into()
}

fn set_js_property(object: &Object, key: &str, value: JsValue) {
    let _ = Reflect::set(object, &JsValue::from_str(key), &value);
}
