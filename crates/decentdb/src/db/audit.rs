use super::*;

pub(super) fn audit_context_json(context: &BTreeMap<String, Value>) -> Result<String> {
    let mut object = serde_json::Map::new();
    for (key, value) in context {
        object.insert(key.clone(), audit_value_to_json(value));
    }
    serde_json::to_string(&JsonValue::Object(object))
        .map_err(|error| DbError::internal(format!("serialize audit context JSON: {error}")))
}

pub(super) fn audit_value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Int64(value) => JsonValue::Number(serde_json::Number::from(*value)),
        Value::Float64(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Bool(value) => JsonValue::Bool(*value),
        Value::Text(value) => JsonValue::String(value.clone()),
        Value::Blob(value) | Value::Geometry(value) | Value::Geography(value) => {
            JsonValue::String(hex_encode(value))
        }
        Value::Decimal { scaled, scale } => JsonValue::String(decimal_to_text(*scaled, *scale)),
        Value::Uuid(value) => JsonValue::String(hex_encode(value)),
        Value::TimestampMicros(value) => JsonValue::Number(serde_json::Number::from(*value)),
        Value::Enum {
            enum_type_id,
            label_id,
        } => JsonValue::String(format!("{enum_type_id}:{label_id}")),
        Value::IpAddr { family, addr } => format_ip_addr(*family, addr)
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => format_cidr(*family, *prefix_len, network)
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::MacAddr { len, bytes } => format_mac_addr(*len, bytes)
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::DateDays(value) => JsonValue::String(format_date_days(*value)),
        Value::TimeMicros(value) => format_time_micros(*value)
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
        Value::TimestampTzMicros(value) => JsonValue::String(format_timestamp_tz_micros(*value)),
        Value::Interval {
            months,
            days,
            micros,
        } => JsonValue::String(format_interval(*months, *days, *micros)),
    }
}

pub(super) fn audit_value_to_text(value: &Value) -> String {
    match value {
        Value::Text(value) => value.clone(),
        Value::Null => "null".to_string(),
        other => audit_value_to_json(other).to_string(),
    }
}

pub(super) fn decimal_to_text(scaled: i64, scale: u8) -> String {
    if scale == 0 {
        return scaled.to_string();
    }
    let negative = scaled < 0;
    let digits = scaled.unsigned_abs().to_string();
    let scale = usize::from(scale);
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    let mut decimal = format!("{}.{}", &padded[..split], &padded[split..]);
    if negative {
        decimal.insert(0, '-');
    }
    decimal
}

pub(super) fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;

        let _ = write!(output, "{byte:02x}");
    }
    output
}

pub(super) fn json_escape(input: String) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
