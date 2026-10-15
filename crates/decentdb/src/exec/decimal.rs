//! Checked fixed-point arithmetic using the existing DECIMAL representation.

use crate::error::{DbError, Result};
use crate::record::value::Value;
use crate::sql::ast::BinaryOp;

fn overflow() -> DbError {
    DbError::sql("DECIMAL arithmetic overflow")
}

fn rescale(scaled: i64, from: u8, to: u8) -> Result<i128> {
    if scaled == 0 {
        return Ok(0);
    }
    let factor = 10_i128
        .checked_pow(u32::from(to - from))
        .ok_or_else(overflow)?;
    i128::from(scaled).checked_mul(factor).ok_or_else(overflow)
}

fn result(mut scaled: i128, mut scale: u16) -> Result<Value> {
    if scaled == 0 {
        return Ok(Value::Decimal {
            scaled: 0,
            scale: 0,
        });
    }
    while scale > 0 && scaled % 10 == 0 {
        scaled /= 10;
        scale -= 1;
    }
    Ok(Value::Decimal {
        scaled: i64::try_from(scaled).map_err(|_| overflow())?,
        scale: u8::try_from(scale).map_err(|_| overflow())?,
    })
}

/// Wide, checked intermediate for exact NUMERIC SUM/AVG. Narrow only the
/// final result, so an average can fit even when the sum exceeds i64.
#[derive(Clone, Debug, Default)]
pub(super) struct DecimalSum {
    scaled: i128,
    scale: u8,
}

impl DecimalSum {
    pub(super) fn add(&mut self, scaled: i64, scale: u8) -> Result<()> {
        let target_scale = self.scale.max(scale);
        let current = if self.scaled == 0 {
            0
        } else {
            self.scaled
                .checked_mul(
                    10_i128
                        .checked_pow(u32::from(target_scale - self.scale))
                        .ok_or_else(overflow)?,
                )
                .ok_or_else(overflow)?
        };
        self.scaled = current
            .checked_add(rescale(scaled, scale, target_scale)?)
            .ok_or_else(overflow)?;
        self.scale = target_scale;
        Ok(())
    }

    pub(super) fn sum(&self) -> Result<Value> {
        result(self.scaled, u16::from(self.scale))
    }

    pub(super) fn integer_sum(&self) -> Result<Value> {
        Ok(Value::Int64(
            i64::try_from(self.scaled).map_err(|_| DbError::sql("integer SUM overflow"))?,
        ))
    }

    pub(super) fn average(&self, count: i64) -> Result<Value> {
        let scale = self.scale.max(6);
        let factor = 10_i128
            .checked_pow(u32::from(scale - self.scale))
            .ok_or_else(overflow)?;
        let divisor = i128::from(count);
        // Divide before scaling to avoid overflowing an otherwise representable
        // average. The remainder carries all discarded fractional digits.
        let whole = (self.scaled / divisor)
            .checked_mul(factor)
            .ok_or_else(overflow)?;
        let fractional = (self.scaled % divisor)
            .checked_mul(factor)
            .ok_or_else(overflow)?;
        let mut quotient = whole
            .checked_add(fractional / divisor)
            .ok_or_else(overflow)?;
        if (fractional % divisor).unsigned_abs() * 2 >= divisor.unsigned_abs() {
            quotient = quotient
                .checked_add(if self.scaled < 0 { -1 } else { 1 })
                .ok_or_else(overflow)?;
        }
        result(quotient, u16::from(scale))
    }
}

pub(super) fn arithmetic(op: &BinaryOp, left: (i64, u8), right: (i64, u8)) -> Result<Value> {
    let (left, left_scale) = left;
    let (right, right_scale) = right;
    if matches!(op, BinaryOp::Div | BinaryOp::Mod) && right == 0 {
        return Ok(Value::Null);
    }
    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mod => {
            let scale = left_scale.max(right_scale);
            let left = rescale(left, left_scale, scale)?;
            let right = rescale(right, right_scale, scale)?;
            let scaled = match op {
                BinaryOp::Add => left.checked_add(right),
                BinaryOp::Sub => left.checked_sub(right),
                _ => left.checked_rem(right),
            }
            .ok_or_else(overflow)?;
            result(scaled, u16::from(scale))
        }
        BinaryOp::Mul => result(
            i128::from(left) * i128::from(right),
            u16::from(left_scale) + u16::from(right_scale),
        ),
        BinaryOp::Div => {
            let scale = left_scale.max(right_scale).max(6);
            let exponent = u32::from(scale) + u32::from(right_scale) - u32::from(left_scale);
            let numerator = if left == 0 {
                0
            } else {
                i128::from(left)
                    .checked_mul(10_i128.checked_pow(exponent).ok_or_else(overflow)?)
                    .ok_or_else(overflow)?
            };
            let divisor = i128::from(right);
            let mut quotient = numerator.checked_div(divisor).ok_or_else(overflow)?;
            let remainder = numerator.checked_rem(divisor).ok_or_else(overflow)?;
            if remainder.unsigned_abs() * 2 >= divisor.unsigned_abs() {
                quotient = quotient
                    .checked_add(if (numerator < 0) == (divisor < 0) {
                        1
                    } else {
                        -1
                    })
                    .ok_or_else(overflow)?;
            }
            result(quotient, u16::from(scale))
        }
        _ => Err(DbError::internal("invalid DECIMAL arithmetic operator")),
    }
}
