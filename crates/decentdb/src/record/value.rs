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

    let left_abs = left_scaled.unsigned_abs().to_string();
    let right_abs = right_scaled.unsigned_abs().to_string();
    let left_adjusted_exp = left_abs.len() as i32 - i32::from(left_scale);
    let right_adjusted_exp = right_abs.len() as i32 - i32::from(right_scale);

    let ordering = left_adjusted_exp
        .cmp(&right_adjusted_exp)
        .then_with(|| left_abs.cmp(&right_abs));

    if left_negative {
        ordering.reverse()
    } else {
        ordering
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
    }

    #[test]
    fn decimal_text_parser_accepts_signed_fractional_values() {
        assert_eq!(parse_decimal_text("19.990").expect("parse"), (1999, 2));
        assert_eq!(parse_decimal_text("-.5").expect("parse"), (-5, 1));
        assert_eq!(parse_decimal_text("42").expect("parse"), (42, 0));
    }
}
