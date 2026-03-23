//! Comparable index-key encoding.
//!
//! Implements:
//! - design/adr/0061-typed-index-key-encoding-text-blob.md

use std::cmp::Ordering;

use crate::error::{DbError, Result};
use crate::record::value::{compare_decimal, normalize_decimal, Value};

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT64: u8 = 2;
const TAG_FLOAT64: u8 = 3;
const TAG_DECIMAL: u8 = 4;
const TAG_TIMESTAMP: u8 = 5;
const TAG_UUID: u8 = 6;
const TAG_TEXT: u8 = 7;
const TAG_BLOB: u8 = 8;

pub(crate) fn encode_index_key(value: &Value) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    match value {
        Value::Null => encoded.push(TAG_NULL),
        Value::Bool(value) => {
            encoded.push(TAG_BOOL);
            encoded.push(u8::from(*value));
        }
        Value::Int64(value) => {
            encoded.push(TAG_INT64);
            encoded.extend_from_slice(&sortable_signed_bytes(*value));
        }
        Value::Float64(value) => {
            encoded.push(TAG_FLOAT64);
            encoded.extend_from_slice(&sortable_float_bytes(*value));
        }
        Value::Decimal { scaled, scale } => {
            encoded.push(TAG_DECIMAL);
            encoded.extend_from_slice(&sortable_decimal_bytes(*scaled, *scale));
        }
        Value::TimestampMicros(value) => {
            encoded.push(TAG_TIMESTAMP);
            encoded.extend_from_slice(&sortable_signed_bytes(*value));
        }
        Value::Uuid(value) => {
            encoded.push(TAG_UUID);
            encoded.extend_from_slice(value);
        }
        Value::Text(value) => {
            encoded.push(TAG_TEXT);
            encoded.extend_from_slice(value.as_bytes());
        }
        Value::Blob(value) => {
            encoded.push(TAG_BLOB);
            encoded.extend_from_slice(value);
        }
    }
    Ok(encoded)
}

pub(crate) fn compare_index_values(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        (Value::Int64(left), Value::Int64(right)) => Ok(left.cmp(right)),
        (Value::Float64(left), Value::Float64(right)) => Ok(left.total_cmp(right)),
        (
            Value::Decimal {
                scaled: left_scaled,
                scale: left_scale,
            },
            Value::Decimal {
                scaled: right_scaled,
                scale: right_scale,
            },
        ) => Ok(compare_decimal(
            *left_scaled,
            *left_scale,
            *right_scaled,
            *right_scale,
        )),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => Ok(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Ok(left.cmp(right)),
        (Value::Text(left), Value::Text(right)) => Ok(left.as_bytes().cmp(right.as_bytes())),
        (Value::Blob(left), Value::Blob(right)) => Ok(left.cmp(right)),
        _ => Err(DbError::constraint(
            "index-key comparison requires values of the same indexed type",
        )),
    }
}

fn sortable_signed_bytes(value: i64) -> [u8; 8] {
    ((value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

fn sortable_float_bytes(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let sortable = if bits & (1_u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1_u64 << 63)
    };
    sortable.to_be_bytes()
}

fn sortable_decimal_bytes(scaled: i64, scale: u8) -> Vec<u8> {
    let (scaled, scale) = normalize_decimal(scaled, scale);
    if scaled == 0 {
        return vec![1];
    }

    let digits = scaled.unsigned_abs().to_string();
    let adjusted_exp = digits.len() as i32 - i32::from(scale);
    let biased_exp = u16::try_from(adjusted_exp + 1024).expect("decimal exponent fits u16");
    let mut output = Vec::with_capacity(1 + 2 + digits.len());
    if scaled < 0 {
        output.push(0);
        let inverted = u16::MAX - biased_exp;
        output.extend_from_slice(&inverted.to_be_bytes());
        output.extend(digits.bytes().map(|byte| u8::MAX - byte));
    } else {
        output.push(2);
        output.extend_from_slice(&biased_exp.to_be_bytes());
        output.extend_from_slice(digits.as_bytes());
    }
    output
}

#[cfg(test)]
mod tests {
    use crate::record::value::Value;

    use super::{compare_index_values, encode_index_key};

    fn assert_order(values: &[Value]) {
        let mut encoded = values
            .iter()
            .map(|value| encode_index_key(value).expect("encode"))
            .collect::<Vec<_>>();
        let mut expected = values.to_vec();
        encoded.sort();
        expected.sort_by(|left, right| compare_index_values(left, right).expect("compare"));

        let decoded_order = encoded.to_vec();
        let expected_order = expected
            .iter()
            .map(|value| encode_index_key(value).expect("encode"))
            .collect::<Vec<_>>();
        assert_eq!(decoded_order, expected_order);
    }

    #[test]
    fn integer_decimal_timestamp_bool_float_uuid_text_and_blob_keys_sort_correctly() {
        assert_order(&[
            Value::Int64(-9),
            Value::Int64(0),
            Value::Int64(7),
            Value::Int64(99),
        ]);
        assert_order(&[
            Value::Decimal {
                scaled: -150,
                scale: 2,
            },
            Value::Decimal {
                scaled: -14,
                scale: 1,
            },
            Value::Decimal {
                scaled: 119,
                scale: 2,
            },
            Value::Decimal {
                scaled: 12,
                scale: 1,
            },
            Value::Decimal {
                scaled: 120,
                scale: 2,
            },
        ]);
        assert_order(&[
            Value::TimestampMicros(-10),
            Value::TimestampMicros(0),
            Value::TimestampMicros(10),
        ]);
        assert_order(&[Value::Bool(false), Value::Bool(true)]);
        assert_order(&[
            Value::Float64(f64::NEG_INFINITY),
            Value::Float64(-1.5),
            Value::Float64(0.0),
            Value::Float64(2.0),
            Value::Float64(f64::INFINITY),
        ]);
        assert_order(&[
            Value::Uuid([0; 16]),
            Value::Uuid([1; 16]),
            Value::Uuid([255; 16]),
        ]);
        assert_order(&[
            Value::Text("Alpha".into()),
            Value::Text("Beta".into()),
            Value::Text("beta".into()),
        ]);
        assert_order(&[
            Value::Blob(vec![0x00]),
            Value::Blob(vec![0x00, 0x01]),
            Value::Blob(vec![0xFF]),
        ]);
    }

    #[test]
    fn text_and_blob_keys_are_not_hash_based() {
        let left = encode_index_key(&Value::Text("abcdef".into())).expect("encode");
        let right = encode_index_key(&Value::Text("abcdeg".into())).expect("encode");
        assert_ne!(left, right);
        assert!(left < right);

        let left = encode_index_key(&Value::Blob(vec![0x10, 0x20])).expect("encode");
        let right = encode_index_key(&Value::Blob(vec![0x10, 0x21])).expect("encode");
        assert_ne!(left, right);
        assert!(left < right);
    }
}
