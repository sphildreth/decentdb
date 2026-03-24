//! Row serialization for table payloads.

use crate::error::{DbError, Result};
use crate::record::compression::CompressionMode;
use crate::record::overflow::{read_overflow, write_overflow, OverflowPointer};
use crate::record::value::Value;
use crate::record::{
    decode_varint_u64, encode_varint_u64_into, zigzag_decode_u64, zigzag_encode_i64,
};
use crate::storage::page::PageStore;

const TAG_NULL: u8 = 0;
const TAG_INT64: u8 = 1;
const TAG_FLOAT64: u8 = 2;
const TAG_BOOL: u8 = 3;
const TAG_TEXT: u8 = 4;
const TAG_BLOB: u8 = 5;
const TAG_DECIMAL: u8 = 6;
const TAG_UUID: u8 = 7;
const TAG_TIMESTAMP: u8 = 8;
const TAG_TEXT_OVERFLOW: u8 = 9;
const TAG_BLOB_OVERFLOW: u8 = 10;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Row {
    values: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RowOverflowOptions {
    pub(crate) inline_threshold: usize,
    pub(crate) compression: CompressionMode,
}

impl Default for RowOverflowOptions {
    fn default() -> Self {
        Self {
            inline_threshold: 512,
            compression: CompressionMode::Auto,
        }
    }
}

impl Row {
    #[must_use]
    pub(crate) fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    #[must_use]
    pub(crate) fn values(&self) -> &[Value] {
        &self.values
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        Self::encode_values(self.values())
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_with_overflow::<crate::storage::page::InMemoryPageStore>(bytes, None)
    }

    pub(crate) fn encode_with_overflow<S: PageStore>(
        &self,
        store: Option<&mut S>,
        options: RowOverflowOptions,
    ) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        Self::encode_values_into_with_overflow(self.values(), &mut encoded, store, options)?;
        Ok(encoded)
    }

    pub(crate) fn encode_values(values: &[Value]) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        Self::encode_values_into_with_overflow::<crate::storage::page::InMemoryPageStore>(
            values,
            &mut encoded,
            None,
            RowOverflowOptions {
                inline_threshold: usize::MAX,
                compression: CompressionMode::Auto,
            },
        )?;
        Ok(encoded)
    }

    pub(crate) fn encode_values_into(values: &[Value], output: &mut Vec<u8>) -> Result<()> {
        output.clear();
        Self::encode_values_into_with_overflow::<crate::storage::page::InMemoryPageStore>(
            values,
            output,
            None,
            RowOverflowOptions {
                inline_threshold: usize::MAX,
                compression: CompressionMode::Auto,
            },
        )
    }

    fn encode_values_into_with_overflow<S: PageStore>(
        values: &[Value],
        output: &mut Vec<u8>,
        mut store: Option<&mut S>,
        options: RowOverflowOptions,
    ) -> Result<()> {
        encode_varint_u64_into(
            u64::try_from(values.len())
                .map_err(|_| DbError::constraint("row field count exceeds u64"))?,
            output,
        );

        for value in values {
            match value {
                Value::Null => {
                    output.push(TAG_NULL);
                    encode_varint_u64_into(0, output);
                }
                Value::Int64(value) => {
                    let mut encoded = [0_u8; 10];
                    let len = encode_varint_u64_fixed(zigzag_encode_i64(*value), &mut encoded);
                    output.push(TAG_INT64);
                    encode_varint_u64_into(len as u64, output);
                    output.extend_from_slice(&encoded[..len]);
                }
                Value::Float64(value) => {
                    output.push(TAG_FLOAT64);
                    encode_varint_u64_into(8, output);
                    output.extend_from_slice(&value.to_le_bytes());
                }
                Value::Bool(value) => {
                    output.push(TAG_BOOL);
                    encode_varint_u64_into(1, output);
                    output.push(u8::from(*value));
                }
                Value::Text(text) if text.len() > options.inline_threshold => {
                    let store = store.as_deref_mut().ok_or_else(|| {
                        DbError::constraint("TEXT overflow requires a page store")
                    })?;
                    let pointer = write_overflow(store, text.as_bytes(), options.compression)?;
                    let encoded = encode_overflow_pointer(pointer);
                    output.push(TAG_TEXT_OVERFLOW);
                    encode_varint_u64_into(encoded.len() as u64, output);
                    output.extend_from_slice(&encoded);
                }
                Value::Text(text) => {
                    output.push(TAG_TEXT);
                    encode_varint_u64_into(
                        u64::try_from(text.len())
                            .map_err(|_| DbError::constraint("TEXT payload exceeds u64"))?,
                        output,
                    );
                    output.extend_from_slice(text.as_bytes());
                }
                Value::Blob(blob) if blob.len() > options.inline_threshold => {
                    let store = store.as_deref_mut().ok_or_else(|| {
                        DbError::constraint("BLOB overflow requires a page store")
                    })?;
                    let pointer = write_overflow(store, blob, options.compression)?;
                    let encoded = encode_overflow_pointer(pointer);
                    output.push(TAG_BLOB_OVERFLOW);
                    encode_varint_u64_into(encoded.len() as u64, output);
                    output.extend_from_slice(&encoded);
                }
                Value::Blob(blob) => {
                    output.push(TAG_BLOB);
                    encode_varint_u64_into(
                        u64::try_from(blob.len())
                            .map_err(|_| DbError::constraint("BLOB payload exceeds u64"))?,
                        output,
                    );
                    output.extend_from_slice(blob);
                }
                Value::Decimal { scaled, scale } => {
                    let mut encoded = [0_u8; 10];
                    let len = encode_varint_u64_fixed(zigzag_encode_i64(*scaled), &mut encoded);
                    output.push(TAG_DECIMAL);
                    encode_varint_u64_into((len + 1) as u64, output);
                    output.push(*scale);
                    output.extend_from_slice(&encoded[..len]);
                }
                Value::Uuid(uuid) => {
                    output.push(TAG_UUID);
                    encode_varint_u64_into(16, output);
                    output.extend_from_slice(uuid);
                }
                Value::TimestampMicros(value) => {
                    let mut encoded = [0_u8; 10];
                    let len = encode_varint_u64_fixed(zigzag_encode_i64(*value), &mut encoded);
                    output.push(TAG_TIMESTAMP);
                    encode_varint_u64_into(len as u64, output);
                    output.extend_from_slice(&encoded[..len]);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn decode_with_overflow<S: PageStore>(
        bytes: &[u8],
        store: Option<&S>,
    ) -> Result<Self> {
        let (field_count, mut offset) = decode_varint_u64(bytes)?;
        let mut values = Vec::with_capacity(field_count as usize);

        for _ in 0..field_count {
            let tag = *bytes
                .get(offset)
                .ok_or_else(|| DbError::corruption("truncated row field tag"))?;
            offset += 1;

            let (payload_len, len_bytes) = decode_varint_u64(&bytes[offset..])?;
            offset += len_bytes;
            let payload_len = usize::try_from(payload_len)
                .map_err(|_| DbError::corruption("field payload length exceeds usize"))?;
            let payload_end = offset + payload_len;
            let payload = bytes
                .get(offset..payload_end)
                .ok_or_else(|| DbError::corruption("truncated row field payload"))?;
            offset = payload_end;

            let value = match tag {
                TAG_NULL => {
                    if !payload.is_empty() {
                        return Err(DbError::corruption("NULL field must have empty payload"));
                    }
                    Value::Null
                }
                TAG_INT64 => {
                    let (encoded, consumed) = decode_varint_u64(payload)?;
                    if consumed != payload.len() {
                        return Err(DbError::corruption("INT64 payload has trailing bytes"));
                    }
                    Value::Int64(zigzag_decode_u64(encoded))
                }
                TAG_FLOAT64 => {
                    let bytes: [u8; 8] = payload
                        .try_into()
                        .map_err(|_| DbError::corruption("FLOAT64 payload must be 8 bytes"))?;
                    Value::Float64(f64::from_le_bytes(bytes))
                }
                TAG_BOOL => match payload {
                    [0] => Value::Bool(false),
                    [1] => Value::Bool(true),
                    _ => return Err(DbError::corruption("BOOL payload must be 0 or 1")),
                },
                TAG_TEXT => Value::text_from_bytes(payload.to_vec())?,
                TAG_BLOB => Value::Blob(payload.to_vec()),
                TAG_DECIMAL => {
                    let scale = *payload
                        .first()
                        .ok_or_else(|| DbError::corruption("DECIMAL payload missing scale"))?;
                    let (encoded, consumed) = decode_varint_u64(&payload[1..])?;
                    if consumed + 1 != payload.len() {
                        return Err(DbError::corruption("DECIMAL payload has trailing bytes"));
                    }
                    Value::Decimal {
                        scaled: zigzag_decode_u64(encoded),
                        scale,
                    }
                }
                TAG_UUID => {
                    let bytes: [u8; 16] = payload
                        .try_into()
                        .map_err(|_| DbError::corruption("UUID payload must be 16 bytes"))?;
                    Value::Uuid(bytes)
                }
                TAG_TIMESTAMP => {
                    let (encoded, consumed) = decode_varint_u64(payload)?;
                    if consumed != payload.len() {
                        return Err(DbError::corruption("TIMESTAMP payload has trailing bytes"));
                    }
                    Value::TimestampMicros(zigzag_decode_u64(encoded))
                }
                TAG_TEXT_OVERFLOW => {
                    let store = store.ok_or_else(|| {
                        DbError::constraint("TEXT overflow decoding requires a page store")
                    })?;
                    let pointer = decode_overflow_pointer(payload)?;
                    Value::text_from_bytes(read_overflow(store, pointer)?)?
                }
                TAG_BLOB_OVERFLOW => {
                    let store = store.ok_or_else(|| {
                        DbError::constraint("BLOB overflow decoding requires a page store")
                    })?;
                    let pointer = decode_overflow_pointer(payload)?;
                    Value::Blob(read_overflow(store, pointer)?)
                }
                _ => return Err(DbError::corruption(format!("unknown row value tag {tag}"))),
            };

            values.push(value);
        }

        Ok(Self { values })
    }
}

fn encode_overflow_pointer(pointer: OverflowPointer) -> [u8; 9] {
    let mut payload = [0_u8; 9];
    payload[0] = pointer.flags;
    payload[1..5].copy_from_slice(&pointer.head_page_id.to_le_bytes());
    payload[5..9].copy_from_slice(&pointer.logical_len.to_le_bytes());
    payload
}

fn encode_varint_u64_fixed(mut value: u64, output: &mut [u8; 10]) -> usize {
    let mut cursor = 0;
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output[cursor] = byte;
        cursor += 1;
        if value == 0 {
            return cursor;
        }
    }
}

fn decode_overflow_pointer(payload: &[u8]) -> Result<OverflowPointer> {
    if payload.len() != 9 {
        return Err(DbError::corruption(
            "overflow pointer payload must be 9 bytes",
        ));
    }
    let flags = payload[0];
    let head_page_id = u32::from_le_bytes(payload[1..5].try_into().expect("page id"));
    let logical_len = u32::from_le_bytes(payload[5..9].try_into().expect("logical len"));
    Ok(OverflowPointer {
        head_page_id,
        logical_len,
        flags,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::record::compression::CompressionMode;
    use crate::record::value::Value;
    use crate::storage::page::InMemoryPageStore;

    use super::{Row, RowOverflowOptions};

    fn value_strategy() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<i64>().prop_map(Value::Int64),
            any::<u64>().prop_map(|bits| Value::Float64(f64::from_bits(bits))),
            any::<bool>().prop_map(Value::Bool),
            proptest::string::string_regex(".*")
                .expect("regex")
                .prop_map(Value::Text),
            proptest::collection::vec(any::<u8>(), 0..64).prop_map(Value::Blob),
            (any::<i64>(), any::<u8>())
                .prop_map(|(scaled, scale)| Value::Decimal { scaled, scale }),
            proptest::array::uniform16(any::<u8>()).prop_map(Value::Uuid),
            any::<i64>().prop_map(Value::TimestampMicros),
        ]
    }

    proptest! {
        #[test]
        fn row_roundtrip_preserves_all_value_variants(values in proptest::collection::vec(value_strategy(), 0..16)) {
            let row = Row::new(values.clone());
            let encoded = row.encode().expect("encode");
            let decoded = Row::decode(&encoded).expect("decode");
            prop_assert!(rows_equal(decoded.values(), values.as_slice()));
        }
    }

    #[test]
    fn large_text_and_blob_values_spill_to_overflow_pages() {
        let mut store = InMemoryPageStore::default();
        let row = Row::new(vec![
            Value::Text("z".repeat(4_096)),
            Value::Blob(vec![0xAB; 4_096]),
        ]);

        let encoded = row
            .encode_with_overflow(
                Some(&mut store),
                RowOverflowOptions {
                    inline_threshold: 128,
                    compression: CompressionMode::Auto,
                },
            )
            .expect("encode with overflow");

        let decoded = Row::decode_with_overflow(&encoded, Some(&store)).expect("decode");
        assert_eq!(decoded, row);
        assert!(store.allocated_page_count() >= 2);
    }

    #[test]
    fn row_boundary_values_roundtrip() {
        let row = Row::new(vec![
            Value::Int64(i64::MIN),
            Value::Int64(i64::MAX),
            Value::TimestampMicros(i64::MIN),
            Value::TimestampMicros(i64::MAX),
            Value::Decimal {
                scaled: i64::MIN,
                scale: u8::MAX,
            },
            Value::Decimal {
                scaled: i64::MAX,
                scale: 0,
            },
            Value::Text(String::new()),
            Value::Text("Grüße, 世界".to_string()),
            Value::Blob(Vec::new()),
        ]);

        let encoded = row.encode().expect("encode");
        let decoded = Row::decode(&encoded).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn encode_values_into_matches_row_encode_and_clears_output() {
        let values = vec![
            Value::Int64(42),
            Value::Text("alpha".to_string()),
            Value::Bool(true),
        ];
        let expected = Row::new(values.clone()).encode().expect("encode");

        let mut scratch = vec![9_u8, 9_u8, 9_u8];
        Row::encode_values_into(&values, &mut scratch).expect("encode into scratch");
        assert_eq!(scratch, expected);

        let smaller = vec![Value::Null];
        let expected_smaller = Row::new(smaller.clone()).encode().expect("encode smaller");
        Row::encode_values_into(&smaller, &mut scratch).expect("encode smaller into scratch");
        assert_eq!(scratch, expected_smaller);
    }

    fn rows_equal(left: &[Value], right: &[Value]) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(left, right)| values_equal(left, right))
    }

    fn values_equal(left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Float64(left), Value::Float64(right)) => left.to_bits() == right.to_bits(),
            _ => left == right,
        }
    }
}
