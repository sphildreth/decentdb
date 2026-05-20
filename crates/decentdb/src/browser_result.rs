//! Browser result transport helpers.
//!
//! The browser worker uses this compact row frame to avoid JSON row/object
//! serialization across the wasm/worker boundary for query results.

use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::QueryResult;

const MAGIC: &[u8; 4] = b"DDBR";
const VERSION: u16 = 1;

const TAG_NULL: u8 = 0;
const TAG_INT64: u8 = 1;
const TAG_FLOAT64: u8 = 2;
const TAG_BOOL: u8 = 3;
const TAG_TEXT: u8 = 4;
const TAG_BYTES: u8 = 5;
const TAG_DECIMAL: u8 = 6;
const TAG_UUID: u8 = 7;
const TAG_TIMESTAMP_MICROS: u8 = 8;
const TAG_GEOMETRY: u8 = 9;
const TAG_GEOGRAPHY: u8 = 10;
const TAG_ENUM: u8 = 11;
const TAG_IPADDR: u8 = 12;
const TAG_CIDR: u8 = 13;
const TAG_MACADDR: u8 = 14;
const TAG_DATE_DAYS: u8 = 15;
const TAG_TIME_MICROS: u8 = 16;
const TAG_TIMESTAMPTZ_MICROS: u8 = 17;
const TAG_INTERVAL: u8 = 18;

pub(crate) fn encode_query_result_binary(result: &QueryResult) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    put_u16(&mut out, VERSION);
    put_u16(&mut out, 0);
    put_u64(&mut out, result.affected_rows());
    put_u32(
        &mut out,
        checked_len(result.columns().len(), "column count")?,
    );
    put_u32(&mut out, checked_len(result.rows().len(), "row count")?);
    for column in result.columns() {
        put_bytes(&mut out, column.as_bytes())?;
    }
    for row in result.rows() {
        if row.values().len() != result.columns().len() {
            return Err(DbError::internal(format!(
                "query result row has {} values for {} columns",
                row.values().len(),
                result.columns().len()
            )));
        }
        for value in row.values() {
            put_value(&mut out, value)?;
        }
    }
    Ok(out)
}

fn put_value(out: &mut Vec<u8>, value: &Value) -> Result<()> {
    match value {
        Value::Null => out.push(TAG_NULL),
        Value::Int64(value) => {
            out.push(TAG_INT64);
            put_i64(out, *value);
        }
        Value::Float64(value) => {
            out.push(TAG_FLOAT64);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Value::Bool(value) => {
            out.push(TAG_BOOL);
            out.push(u8::from(*value));
        }
        Value::Text(value) => {
            out.push(TAG_TEXT);
            put_bytes(out, value.as_bytes())?;
        }
        Value::Blob(value) => {
            out.push(TAG_BYTES);
            put_bytes(out, value)?;
        }
        Value::Decimal { scaled, scale } => {
            out.push(TAG_DECIMAL);
            put_i64(out, *scaled);
            out.push(*scale);
        }
        Value::Uuid(bytes) => {
            out.push(TAG_UUID);
            out.extend_from_slice(bytes);
        }
        Value::TimestampMicros(value) => {
            out.push(TAG_TIMESTAMP_MICROS);
            put_i64(out, *value);
        }
        Value::Geometry(value) => {
            out.push(TAG_GEOMETRY);
            put_bytes(out, value)?;
        }
        Value::Geography(value) => {
            out.push(TAG_GEOGRAPHY);
            put_bytes(out, value)?;
        }
        Value::Enum {
            enum_type_id,
            label_id,
        } => {
            out.push(TAG_ENUM);
            put_u64(out, *enum_type_id);
            put_u64(out, *label_id);
        }
        Value::IpAddr { family, addr } => {
            out.push(TAG_IPADDR);
            out.push(*family);
            out.extend_from_slice(addr);
        }
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => {
            out.push(TAG_CIDR);
            out.push(*family);
            out.push(*prefix_len);
            out.extend_from_slice(network);
        }
        Value::MacAddr { len, bytes } => {
            out.push(TAG_MACADDR);
            out.push(*len);
            out.extend_from_slice(bytes);
        }
        Value::DateDays(value) => {
            out.push(TAG_DATE_DAYS);
            put_i32(out, *value);
        }
        Value::TimeMicros(value) => {
            out.push(TAG_TIME_MICROS);
            put_i64(out, *value);
        }
        Value::TimestampTzMicros(value) => {
            out.push(TAG_TIMESTAMPTZ_MICROS);
            put_i64(out, *value);
        }
        Value::Interval {
            months,
            days,
            micros,
        } => {
            out.push(TAG_INTERVAL);
            put_i32(out, *months);
            put_i32(out, *days);
            put_i64(out, *micros);
        }
    }
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    put_u32(out, checked_len(bytes.len(), "byte payload length")?);
    out.extend_from_slice(bytes);
    Ok(())
}

fn checked_len(len: usize, context: &str) -> Result<u32> {
    u32::try_from(len).map_err(|_| DbError::internal(format!("{context} exceeds u32::MAX")))
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::QueryRow;

    #[test]
    fn binary_result_header_and_basic_values_are_stable() {
        let result = QueryResult::with_rows(
            vec!["id".to_string(), "name".to_string(), "ok".to_string()],
            vec![QueryRow::new(vec![
                Value::Int64(42),
                Value::Text("alice".to_string()),
                Value::Bool(true),
            ])],
        );

        let bytes = encode_query_result_binary(&result).expect("encode should succeed");

        assert_eq!(&bytes[0..4], b"DDBR");
        assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), 1);
        assert_eq!(u64::from_le_bytes(bytes[8..16].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(bytes[16..20].try_into().unwrap()), 3);
        assert_eq!(u32::from_le_bytes(bytes[20..24].try_into().unwrap()), 1);
    }

    #[test]
    fn binary_result_rejects_mismatched_row_width() {
        let result = QueryResult::with_rows(
            vec!["id".to_string(), "name".to_string()],
            vec![QueryRow::new(vec![Value::Int64(42)])],
        );

        let error = encode_query_result_binary(&result).expect_err("row width mismatch");

        assert!(error.to_string().contains("query result row has 1 values"));
    }
}
