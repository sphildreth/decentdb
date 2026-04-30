//! Canonical 1.0 row-value model.

use std::cmp::Ordering;

use crate::error::{DbError, Result};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Text(String),
    Blob(Vec<u8>),
    Decimal { scaled: i64, scale: u8 },
    Uuid([u8; 16]),
    TimestampMicros(i64),
}

impl Value {
    pub(crate) fn text_from_bytes(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes).map(Self::Text).map_err(|error| {
            DbError::corruption(format!("TEXT payload is not valid UTF-8: {error}"))
        })
    }

    /// Approximate heap-allocated bytes owned by this value (excludes the
    /// `Value` discriminant itself, which is accounted by the caller via
    /// `size_of::<Value>()`). Used by storage instrumentation to report
    /// per-table residency. Per ADR 0143 (Phase A).
    #[must_use]
    pub fn approximate_heap_bytes(&self) -> usize {
        match self {
            Value::Null
            | Value::Int64(_)
            | Value::Float64(_)
            | Value::Bool(_)
            | Value::Decimal { .. }
            | Value::Uuid(_)
            | Value::TimestampMicros(_) => 0,
            Value::Text(s) => s.capacity(),
            Value::Blob(b) => b.capacity(),
        }
    }
}

#[must_use]
pub(crate) fn normalize_decimal(mut scaled: i64, mut scale: u8) -> (i64, u8) {
    if scaled == 0 {
        return (0, 0);
    }

    while scale > 0 && scaled % 10 == 0 {
        scaled /= 10;
        scale -= 1;
    }
    (scaled, scale)
}

pub(crate) fn parse_decimal_text(value: &str) -> Result<(i64, u8)> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(DbError::sql("invalid DECIMAL cast"));
    }

    let (negative, digits) = if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else {
        (false, trimmed)
    };

    let mut saw_digit = false;
    let mut saw_decimal_point = false;
    let mut scale = 0_u8;
    let mut scaled_abs = 0_i128;

    for ch in digits.chars() {
        match ch {
            '0'..='9' => {
                saw_digit = true;
                scaled_abs = scaled_abs
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(i128::from(ch as u8 - b'0')))
                    .ok_or_else(|| DbError::sql("invalid DECIMAL cast"))?;
                if saw_decimal_point {
                    scale = scale
                        .checked_add(1)
                        .ok_or_else(|| DbError::sql("invalid DECIMAL cast"))?;
                }
            }
            '.' if !saw_decimal_point => saw_decimal_point = true,
            _ => return Err(DbError::sql("invalid DECIMAL cast")),
        }
    }

    if !saw_digit {
        return Err(DbError::sql("invalid DECIMAL cast"));
    }

    let signed = if negative { -scaled_abs } else { scaled_abs };
    let scaled = i64::try_from(signed).map_err(|_| DbError::sql("invalid DECIMAL cast"))?;
    Ok(normalize_decimal(scaled, scale))
}

pub(crate) fn compare_decimal(
    left_scaled: i64,
    left_scale: u8,
    right_scaled: i64,
    right_scale: u8,
) -> Ordering {
    let (left_scaled, left_scale) = normalize_decimal(left_scaled, left_scale);
    let (right_scaled, right_scale) = normalize_decimal(right_scaled, right_scale);

    if left_scaled == right_scaled && left_scale == right_scale {
        return Ordering::Equal;
    }

    let left_negative = left_scaled < 0;
    let right_negative = right_scaled < 0;
    if left_negative != right_negative {
        return left_scaled.cmp(&right_scaled);
    }

    let ordering = compare_decimal_magnitude(
        left_scaled.unsigned_abs(),
        left_scale,
        right_scaled.unsigned_abs(),
        right_scale,
    );

    if left_negative {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_decimal_magnitude(
    left_abs: u64,
    left_scale: u8,
    right_abs: u64,
    right_scale: u8,
) -> Ordering {
    let left_digits = left_abs.to_string();
    let right_digits = right_abs.to_string();

    let (left_int, left_frac) = split_decimal_parts(&left_digits, left_scale);
    let (right_int, right_frac) = split_decimal_parts(&right_digits, right_scale);

    let integer_order = left_int
        .len()
        .cmp(&right_int.len())
        .then_with(|| left_int.cmp(right_int));
    if integer_order != Ordering::Equal {
        return integer_order;
    }

    let max_fraction_len = left_frac.len().max(right_frac.len());
    for idx in 0..max_fraction_len {
        let left_digit = left_frac.as_bytes().get(idx).copied().unwrap_or(b'0');
        let right_digit = right_frac.as_bytes().get(idx).copied().unwrap_or(b'0');
        match left_digit.cmp(&right_digit) {
            Ordering::Equal => continue,
            non_equal => return non_equal,
        }
    }

    Ordering::Equal
}

fn split_decimal_parts(digits: &str, scale: u8) -> (&str, &str) {
    let scale = usize::from(scale);
    if scale == 0 {
        return (digits, "");
    }

    if digits.len() > scale {
        let split = digits.len() - scale;
        (&digits[..split], &digits[split..])
    } else {
        ("0", digits)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{compare_decimal, parse_decimal_text};

    #[test]
    fn decimal_comparison_normalizes_trailing_zeroes() {
        assert_eq!(compare_decimal(120, 2, 12, 1), Ordering::Equal);
        assert_eq!(compare_decimal(119, 2, 12, 1), Ordering::Less);
        assert_eq!(compare_decimal(-150, 2, -14, 1), Ordering::Less);
        assert_eq!(compare_decimal(0, 0, 1, 2), Ordering::Less);
        assert_eq!(compare_decimal(-1, 2, 0, 0), Ordering::Less);
    }

    #[test]
    fn decimal_text_parser_accepts_signed_fractional_values() {
        assert_eq!(parse_decimal_text("19.990").expect("parse"), (1999, 2));
        assert_eq!(parse_decimal_text("-.5").expect("parse"), (-5, 1));
        assert_eq!(parse_decimal_text("42").expect("parse"), (42, 0));
    }
}
