//! Canonical 1.0 row-value model.

use std::cmp::Ordering;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::error::{DbError, Result};

pub(crate) const IP_FAMILY_V4: u8 = 4;
pub(crate) const IP_FAMILY_V6: u8 = 6;
pub(crate) const MACADDR_LEN_6: u8 = 6;
pub(crate) const MACADDR_LEN_8: u8 = 8;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Text(String),
    Blob(Vec<u8>),
    Decimal {
        scaled: i64,
        scale: u8,
    },
    Uuid([u8; 16]),
    TimestampMicros(i64),
    Geometry(Vec<u8>),
    Geography(Vec<u8>),
    Enum {
        enum_type_id: u64,
        label_id: u64,
    },
    IpAddr {
        family: u8,
        addr: [u8; 16],
    },
    Cidr {
        family: u8,
        prefix_len: u8,
        network: [u8; 16],
    },
    MacAddr {
        len: u8,
        bytes: [u8; 8],
    },
    DateDays(i32),
    TimeMicros(i64),
    TimestampTzMicros(i64),
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
}

impl Value {
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

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
            | Value::TimestampMicros(_)
            | Value::Enum { .. }
            | Value::IpAddr { .. }
            | Value::Cidr { .. }
            | Value::MacAddr { .. }
            | Value::DateDays(_)
            | Value::TimeMicros(_)
            | Value::TimestampTzMicros(_)
            | Value::Interval { .. } => 0,
            Value::Text(s) => s.capacity(),
            Value::Blob(b) | Value::Geometry(b) | Value::Geography(b) => b.capacity(),
        }
    }
}

pub(crate) fn parse_mac_addr(input: &str) -> Result<(u8, [u8; 8])> {
    let parts = input.trim().split(':').collect::<Vec<_>>();
    let len = match parts.len() {
        6 => MACADDR_LEN_6,
        8 => MACADDR_LEN_8,
        _ => return Err(DbError::sql("invalid MACADDR cast")),
    };
    let mut bytes = [0_u8; 8];
    for (index, part) in parts.iter().enumerate() {
        if part.len() != 2 {
            return Err(DbError::sql("invalid MACADDR cast"));
        }
        bytes[index] =
            u8::from_str_radix(part, 16).map_err(|_| DbError::sql("invalid MACADDR cast"))?;
    }
    Ok((len, bytes))
}

pub(crate) fn format_mac_addr(len: u8, bytes: &[u8; 8]) -> Result<String> {
    validate_mac_addr_payload(len, bytes)?;
    let mut output = String::with_capacity(usize::from(len) * 3 - 1);
    for (index, byte) in bytes[..usize::from(len)].iter().enumerate() {
        if index > 0 {
            output.push(':');
        }
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    Ok(output)
}

pub(crate) fn compare_mac_addr(
    left_len: u8,
    left_bytes: &[u8; 8],
    right_len: u8,
    right_bytes: &[u8; 8],
) -> Result<Ordering> {
    validate_mac_addr_payload(left_len, left_bytes)?;
    validate_mac_addr_payload(right_len, right_bytes)?;
    let left_len = usize::from(left_len);
    let right_len = usize::from(right_len);
    Ok(left_bytes[..left_len]
        .cmp(&right_bytes[..right_len])
        .then_with(|| left_len.cmp(&right_len)))
}

pub(crate) fn mac_addr_payload_len(len: u8) -> Result<usize> {
    validate_mac_addr_len(len)?;
    Ok(usize::from(len) + 1)
}

pub(crate) fn encode_mac_addr_payload(len: u8, bytes: &[u8; 8], out: &mut Vec<u8>) -> Result<()> {
    validate_mac_addr_payload(len, bytes)?;
    out.push(len);
    out.extend_from_slice(&bytes[..usize::from(len)]);
    Ok(())
}

pub(crate) fn decode_mac_addr_payload(payload: &[u8]) -> Result<(u8, [u8; 8])> {
    let len = *payload
        .first()
        .ok_or_else(|| DbError::corruption("MACADDR payload missing length"))?;
    validate_mac_addr_len(len)?;
    if payload.len() != usize::from(len) + 1 {
        return Err(DbError::corruption("MACADDR payload length mismatch"));
    }
    let mut bytes = [0_u8; 8];
    bytes[..usize::from(len)].copy_from_slice(&payload[1..]);
    Ok((len, bytes))
}

pub(crate) fn parse_ip_addr(input: &str) -> Result<(u8, [u8; 16])> {
    let parsed: IpAddr = input
        .parse()
        .map_err(|_| DbError::sql("invalid IPADDR cast"))?;
    Ok(ip_addr_from_std(parsed))
}

pub(crate) fn format_ip_addr(family: u8, addr: &[u8; 16]) -> Result<String> {
    match family {
        IP_FAMILY_V4 => {
            let bytes: [u8; 4] = addr[..4]
                .try_into()
                .map_err(|_| DbError::corruption("IPv4 address must be 4 bytes"))?;
            Ok(Ipv4Addr::from(bytes).to_string())
        }
        IP_FAMILY_V6 => Ok(Ipv6Addr::from(*addr).to_string()),
        _ => Err(DbError::corruption(format!(
            "invalid IP address family {family}"
        ))),
    }
}

pub(crate) fn compare_ip_addr(
    left_family: u8,
    left_addr: &[u8; 16],
    right_family: u8,
    right_addr: &[u8; 16],
) -> Result<Ordering> {
    validate_ip_family_payload(left_family, left_addr)?;
    validate_ip_family_payload(right_family, right_addr)?;

    Ok(ip_addr_order_bytes(left_family, left_addr)
        .cmp(&ip_addr_order_bytes(right_family, right_addr))
        .then_with(|| left_family.cmp(&right_family)))
}

pub(crate) fn ip_addr_payload_len(family: u8) -> Result<usize> {
    match family {
        IP_FAMILY_V4 => Ok(5),
        IP_FAMILY_V6 => Ok(17),
        _ => Err(DbError::corruption(format!(
            "invalid IP address family {family}"
        ))),
    }
}

pub(crate) fn encode_ip_addr_payload(family: u8, addr: &[u8; 16], out: &mut Vec<u8>) -> Result<()> {
    validate_ip_family_payload(family, addr)?;
    out.push(family);
    match family {
        IP_FAMILY_V4 => out.extend_from_slice(&addr[..4]),
        IP_FAMILY_V6 => out.extend_from_slice(addr),
        _ => unreachable!(),
    }
    Ok(())
}

pub(crate) fn decode_ip_addr_payload(payload: &[u8]) -> Result<(u8, [u8; 16])> {
    let family = *payload
        .first()
        .ok_or_else(|| DbError::corruption("IPADDR payload missing family"))?;
    match (family, payload.len()) {
        (IP_FAMILY_V4, 5) => {
            let mut addr = [0_u8; 16];
            addr[..4].copy_from_slice(&payload[1..5]);
            Ok((family, addr))
        }
        (IP_FAMILY_V6, 17) => {
            let mut addr = [0_u8; 16];
            addr.copy_from_slice(&payload[1..17]);
            Ok((family, addr))
        }
        (IP_FAMILY_V4, _) => Err(DbError::corruption("IPADDR IPv4 payload must be 5 bytes")),
        (IP_FAMILY_V6, _) => Err(DbError::corruption("IPADDR IPv6 payload must be 17 bytes")),
        _ => Err(DbError::corruption(format!(
            "invalid IP address family {family}"
        ))),
    }
}

pub(crate) fn parse_cidr(input: &str) -> Result<(u8, u8, [u8; 16])> {
    let (address_text, prefix_text) = input
        .trim()
        .split_once('/')
        .ok_or_else(|| DbError::sql("invalid CIDR cast"))?;
    let (family, mut addr) = parse_ip_addr(address_text)?;
    let prefix_len: u8 = prefix_text
        .parse()
        .map_err(|_| DbError::sql("invalid CIDR cast"))?;
    let max_prefix = if family == IP_FAMILY_V4 { 32 } else { 128 };
    if prefix_len > max_prefix {
        return Err(DbError::sql("invalid CIDR cast"));
    }
    zero_host_bits(family, prefix_len, &mut addr)?;
    Ok((family, prefix_len, addr))
}

pub(crate) fn format_cidr(family: u8, prefix_len: u8, network: &[u8; 16]) -> Result<String> {
    validate_cidr_payload(family, prefix_len, network)?;
    let network_text = format_ip_addr(family, network)?;
    Ok(format!("{network_text}/{prefix_len}"))
}

pub(crate) fn compare_cidr(
    left_family: u8,
    left_prefix_len: u8,
    left_network: &[u8; 16],
    right_family: u8,
    right_prefix_len: u8,
    right_network: &[u8; 16],
) -> Result<Ordering> {
    validate_cidr_payload(left_family, left_prefix_len, left_network)?;
    validate_cidr_payload(right_family, right_prefix_len, right_network)?;

    Ok(left_family
        .cmp(&right_family)
        .then_with(|| left_prefix_len.cmp(&right_prefix_len))
        .then_with(|| {
            if left_family == IP_FAMILY_V4 {
                left_network[..4].cmp(&right_network[..4])
            } else {
                left_network[..].cmp(&right_network[..])
            }
        }))
}

pub(crate) fn cidr_payload_len(family: u8) -> Result<usize> {
    match family {
        IP_FAMILY_V4 => Ok(6),
        IP_FAMILY_V6 => Ok(18),
        _ => Err(DbError::corruption(format!("invalid CIDR family {family}"))),
    }
}

pub(crate) fn encode_cidr_payload(
    family: u8,
    prefix_len: u8,
    network: &[u8; 16],
    out: &mut Vec<u8>,
) -> Result<()> {
    validate_cidr_payload(family, prefix_len, network)?;
    out.push(family);
    out.push(prefix_len);
    match family {
        IP_FAMILY_V4 => out.extend_from_slice(&network[..4]),
        IP_FAMILY_V6 => out.extend_from_slice(network),
        _ => unreachable!(),
    }
    Ok(())
}

pub(crate) fn decode_cidr_payload(payload: &[u8]) -> Result<(u8, u8, [u8; 16])> {
    let family = *payload
        .first()
        .ok_or_else(|| DbError::corruption("CIDR payload missing family"))?;
    let prefix_len = *payload
        .get(1)
        .ok_or_else(|| DbError::corruption("CIDR payload missing prefix length"))?;

    match (family, payload.len()) {
        (IP_FAMILY_V4, 6) => {
            let mut network = [0_u8; 16];
            network[..4].copy_from_slice(&payload[2..6]);
            validate_cidr_payload(family, prefix_len, &network)?;
            Ok((family, prefix_len, network))
        }
        (IP_FAMILY_V6, 18) => {
            let mut network = [0_u8; 16];
            network.copy_from_slice(&payload[2..18]);
            validate_cidr_payload(family, prefix_len, &network)?;
            Ok((family, prefix_len, network))
        }
        (IP_FAMILY_V4, _) => Err(DbError::corruption("CIDR IPv4 payload must be 6 bytes")),
        (IP_FAMILY_V6, _) => Err(DbError::corruption("CIDR IPv6 payload must be 18 bytes")),
        _ => Err(DbError::corruption(format!("invalid CIDR family {family}"))),
    }
}

pub(crate) fn parse_date_days(input: &str) -> Result<i32> {
    let input = input.trim();
    let mut parts = input.split('-');
    let year = parts
        .next()
        .ok_or_else(|| DbError::sql("invalid DATE cast"))?
        .parse::<i32>()
        .map_err(|_| DbError::sql("invalid DATE cast"))?;
    let month = parts
        .next()
        .ok_or_else(|| DbError::sql("invalid DATE cast"))?
        .parse::<u32>()
        .map_err(|_| DbError::sql("invalid DATE cast"))?;
    let day = parts
        .next()
        .ok_or_else(|| DbError::sql("invalid DATE cast"))?
        .parse::<u32>()
        .map_err(|_| DbError::sql("invalid DATE cast"))?;
    if parts.next().is_some() {
        return Err(DbError::sql("invalid DATE cast"));
    }

    validate_date(year, month, day)?;
    i32::try_from(days_from_civil(year, month, day)).map_err(|_| DbError::sql("invalid DATE cast"))
}

#[must_use]
pub(crate) fn format_date_days(days: i32) -> String {
    let (year, month, day) = civil_from_days(i64::from(days));
    format!("{}-{month:02}-{day:02}", format_year(year))
}

pub(crate) fn parse_time_micros(input: &str) -> Result<i64> {
    let input = input.trim();
    let (hhmmss, fraction_text) = match input.split_once('.') {
        Some((head, tail)) => (head, Some(tail)),
        None => (input, None),
    };

    let mut parts = hhmmss.split(':');
    let hour = parts
        .next()
        .ok_or_else(|| DbError::sql("invalid TIME cast"))?
        .parse::<i64>()
        .map_err(|_| DbError::sql("invalid TIME cast"))?;
    let minute = parts
        .next()
        .ok_or_else(|| DbError::sql("invalid TIME cast"))?
        .parse::<i64>()
        .map_err(|_| DbError::sql("invalid TIME cast"))?;
    let second = parts
        .next()
        .ok_or_else(|| DbError::sql("invalid TIME cast"))?
        .parse::<i64>()
        .map_err(|_| DbError::sql("invalid TIME cast"))?;
    if parts.next().is_some() {
        return Err(DbError::sql("invalid TIME cast"));
    }

    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return Err(DbError::sql("invalid TIME cast"));
    }

    let fraction_micros = if let Some(fraction_text) = fraction_text {
        if fraction_text.is_empty() || fraction_text.len() > 6 {
            return Err(DbError::sql("invalid TIME cast"));
        }
        if !fraction_text.bytes().all(|ch| ch.is_ascii_digit()) {
            return Err(DbError::sql("invalid TIME cast"));
        }

        let mut padded = fraction_text.to_string();
        while padded.len() < 6 {
            padded.push('0');
        }
        padded
            .parse::<i64>()
            .map_err(|_| DbError::sql("invalid TIME cast"))?
    } else {
        0
    };

    Ok(hour * MICROS_PER_HOUR
        + minute * MICROS_PER_MINUTE
        + second * MICROS_PER_SECOND
        + fraction_micros)
}

pub(crate) fn format_time_micros(micros: i64) -> Result<String> {
    if !(0..MICROS_PER_DAY).contains(&micros) {
        return Err(DbError::sql("invalid TIME value"));
    }

    let hour = micros / MICROS_PER_HOUR;
    let minute = (micros % MICROS_PER_HOUR) / MICROS_PER_MINUTE;
    let second = (micros % MICROS_PER_MINUTE) / MICROS_PER_SECOND;
    let fraction = micros % MICROS_PER_SECOND;

    Ok(format!("{hour:02}:{minute:02}:{second:02}.{fraction:06}"))
}

pub(crate) fn parse_timestamp_tz_micros(input: &str) -> Result<i64> {
    let text = input.trim();
    let (body, offset_micros) =
        if let Some(body) = text.strip_suffix('Z').or_else(|| text.strip_suffix('z')) {
            (body, 0_i64)
        } else {
            let mut tz_pos = None;
            for (index, byte) in text.bytes().enumerate().skip(10) {
                if byte == b'+' || byte == b'-' {
                    tz_pos = Some(index);
                }
            }
            let pos = tz_pos.ok_or_else(|| DbError::sql("invalid TIMESTAMPTZ cast"))?;
            let (body, offset_text) = text.split_at(pos);
            (body, parse_timezone_offset_micros(offset_text)?)
        };

    let separator = if body.contains('T') { 'T' } else { ' ' };
    let (date_text, time_text) = body
        .split_once(separator)
        .ok_or_else(|| DbError::sql("invalid TIMESTAMPTZ cast"))?;

    let days = i64::from(parse_date_days(date_text)?);
    let time_micros = parse_time_micros(time_text)?;
    days.checked_mul(MICROS_PER_DAY)
        .and_then(|value| value.checked_add(time_micros))
        .and_then(|value| value.checked_sub(offset_micros))
        .ok_or_else(|| DbError::sql("invalid TIMESTAMPTZ cast"))
}

#[must_use]
pub(crate) fn format_timestamp_tz_micros(micros: i64) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let time = micros.rem_euclid(MICROS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let hour = time / MICROS_PER_HOUR;
    let minute = (time % MICROS_PER_HOUR) / MICROS_PER_MINUTE;
    let second = (time % MICROS_PER_MINUTE) / MICROS_PER_SECOND;
    let fraction = time % MICROS_PER_SECOND;

    format!(
        "{}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{fraction:06}Z",
        format_year(year)
    )
}

pub(crate) fn parse_interval(input: &str) -> Result<(i32, i32, i64)> {
    let tokens = input.split_whitespace().collect::<Vec<_>>();
    if tokens.is_empty() {
        return Err(DbError::sql("invalid INTERVAL cast"));
    }
    if tokens.len() == 3 && tokens.iter().all(|token| token.parse::<i64>().is_ok()) {
        let months = tokens[0]
            .parse::<i32>()
            .map_err(|_| DbError::sql("invalid INTERVAL cast"))?;
        let days = tokens[1]
            .parse::<i32>()
            .map_err(|_| DbError::sql("invalid INTERVAL cast"))?;
        let micros = tokens[2]
            .parse::<i64>()
            .map_err(|_| DbError::sql("invalid INTERVAL cast"))?;
        return Ok((months, days, micros));
    }
    if tokens.len() % 2 != 0 {
        return Err(DbError::sql(
            "INTERVAL text must be pairs of amount and unit",
        ));
    }

    let mut months: i32 = 0;
    let mut days: i32 = 0;
    let mut micros: i64 = 0;
    for pair in tokens.chunks_exact(2) {
        let amount = pair[0];
        let unit = normalize_interval_unit(pair[1]);
        match unit.as_str() {
            "year" | "y" => {
                let years = parse_interval_integer_amount(amount)?;
                let delta = years
                    .checked_mul(12)
                    .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?;
                months = checked_add_i32_i64(months, delta)?;
            }
            "month" | "mon" => {
                months = checked_add_i32_i64(months, parse_interval_integer_amount(amount)?)?;
            }
            "week" | "w" => {
                let weeks = parse_interval_integer_amount(amount)?;
                let delta = weeks
                    .checked_mul(7)
                    .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?;
                days = checked_add_i32_i64(days, delta)?;
            }
            "day" | "d" => {
                days = checked_add_i32_i64(days, parse_interval_integer_amount(amount)?)?;
            }
            "hour" | "hr" | "h" => {
                micros = checked_add_i64_i128(
                    micros,
                    parse_interval_fractional_micros(amount, MICROS_PER_HOUR)?,
                )?;
            }
            "minute" | "min" => {
                micros = checked_add_i64_i128(
                    micros,
                    parse_interval_fractional_micros(amount, MICROS_PER_MINUTE)?,
                )?;
            }
            "second" | "sec" | "s" => {
                micros = checked_add_i64_i128(
                    micros,
                    parse_interval_fractional_micros(amount, MICROS_PER_SECOND)?,
                )?;
            }
            "millisecond" | "msec" | "ms" => {
                micros =
                    checked_add_i64_i128(micros, parse_interval_fractional_micros(amount, 1_000)?)?;
            }
            "microsecond" | "usec" | "us" => {
                micros =
                    checked_add_i64_i128(micros, parse_interval_fractional_micros(amount, 1)?)?;
            }
            _ => {
                return Err(DbError::sql(format!(
                    "INTERVAL unit {} is not supported",
                    pair[1]
                )))
            }
        }
    }
    Ok((months, days, micros))
}

#[must_use]
pub(crate) fn format_interval(months: i32, days: i32, micros: i64) -> String {
    format!("{months} {days} {micros}")
}

#[must_use]
pub(crate) fn compare_interval(
    left_months: i32,
    left_days: i32,
    left_micros: i64,
    right_months: i32,
    right_days: i32,
    right_micros: i64,
) -> Ordering {
    left_months
        .cmp(&right_months)
        .then_with(|| left_days.cmp(&right_days))
        .then_with(|| left_micros.cmp(&right_micros))
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

fn ip_addr_from_std(ip: IpAddr) -> (u8, [u8; 16]) {
    match ip {
        IpAddr::V4(v4) => {
            let mut addr = [0_u8; 16];
            addr[..4].copy_from_slice(&v4.octets());
            (IP_FAMILY_V4, addr)
        }
        IpAddr::V6(v6) => (IP_FAMILY_V6, v6.octets()),
    }
}

fn ip_addr_order_bytes(family: u8, addr: &[u8; 16]) -> [u8; 16] {
    match family {
        IP_FAMILY_V4 => {
            let mut mapped = [0_u8; 16];
            mapped[10] = 0xff;
            mapped[11] = 0xff;
            mapped[12..16].copy_from_slice(&addr[..4]);
            mapped
        }
        IP_FAMILY_V6 => *addr,
        _ => unreachable!(),
    }
}

fn validate_ip_family_payload(family: u8, _addr: &[u8; 16]) -> Result<()> {
    match family {
        IP_FAMILY_V4 | IP_FAMILY_V6 => Ok(()),
        _ => Err(DbError::corruption(format!(
            "invalid IP address family {family}"
        ))),
    }
}

fn validate_cidr_payload(family: u8, prefix_len: u8, network: &[u8; 16]) -> Result<()> {
    validate_ip_family_payload(family, network)?;
    let max_prefix = if family == IP_FAMILY_V4 { 32 } else { 128 };
    if prefix_len > max_prefix {
        return Err(DbError::corruption("CIDR prefix length out of range"));
    }

    let mut normalized = *network;
    zero_host_bits(family, prefix_len, &mut normalized)?;
    if &normalized != network {
        return Err(DbError::corruption(
            "CIDR network address must have host bits cleared",
        ));
    }
    Ok(())
}

fn validate_mac_addr_len(len: u8) -> Result<()> {
    match len {
        MACADDR_LEN_6 | MACADDR_LEN_8 => Ok(()),
        _ => Err(DbError::corruption("MACADDR length must be 6 or 8 bytes")),
    }
}

fn validate_mac_addr_payload(len: u8, bytes: &[u8; 8]) -> Result<()> {
    validate_mac_addr_len(len)?;
    if bytes[usize::from(len)..].iter().any(|byte| *byte != 0) {
        return Err(DbError::corruption("MACADDR trailing bytes must be zero"));
    }
    Ok(())
}

fn zero_host_bits(family: u8, prefix_len: u8, addr: &mut [u8; 16]) -> Result<()> {
    match family {
        IP_FAMILY_V4 => {
            let mut bits = prefix_len;
            for byte in &mut addr[..4] {
                if bits >= 8 {
                    bits -= 8;
                    continue;
                }
                if bits == 0 {
                    *byte = 0;
                } else {
                    let mask = (!0_u8) << (8 - bits);
                    *byte &= mask;
                    bits = 0;
                }
            }
            Ok(())
        }
        IP_FAMILY_V6 => {
            let mut bits = prefix_len;
            for byte in addr.iter_mut() {
                if bits >= 8 {
                    bits -= 8;
                    continue;
                }
                if bits == 0 {
                    *byte = 0;
                } else {
                    let mask = (!0_u8) << (8 - bits);
                    *byte &= mask;
                    bits = 0;
                }
            }
            Ok(())
        }
        _ => Err(DbError::corruption(format!("invalid CIDR family {family}"))),
    }
}

fn parse_timezone_offset_micros(offset_text: &str) -> Result<i64> {
    if offset_text.len() != 6 {
        return Err(DbError::sql("invalid TIMESTAMPTZ cast"));
    }
    let sign = match offset_text.as_bytes()[0] {
        b'+' => 1_i64,
        b'-' => -1_i64,
        _ => return Err(DbError::sql("invalid TIMESTAMPTZ cast")),
    };
    if offset_text.as_bytes()[3] != b':' {
        return Err(DbError::sql("invalid TIMESTAMPTZ cast"));
    }

    let hours: i64 = offset_text[1..3]
        .parse()
        .map_err(|_| DbError::sql("invalid TIMESTAMPTZ cast"))?;
    let minutes: i64 = offset_text[4..6]
        .parse()
        .map_err(|_| DbError::sql("invalid TIMESTAMPTZ cast"))?;

    if hours > 23 || minutes > 59 {
        return Err(DbError::sql("invalid TIMESTAMPTZ cast"));
    }

    Ok(sign * (hours * MICROS_PER_HOUR + minutes * MICROS_PER_MINUTE))
}

fn validate_date(year: i32, month: u32, day: u32) -> Result<()> {
    if month == 0 || month > 12 {
        return Err(DbError::sql("invalid DATE cast"));
    }
    if day == 0 || day > days_in_month(year, month) {
        return Err(DbError::sql("invalid DATE cast"));
    }
    Ok(())
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => unreachable!(),
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let mut y = i64::from(year);
    let m = i64::from(month);
    let d = i64::from(day);
    y -= if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

fn format_year(year: i64) -> String {
    if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else {
        year.to_string()
    }
}

fn normalize_interval_unit(unit: &str) -> String {
    let lower = unit.to_ascii_lowercase();
    match lower.as_str() {
        "years" => "year".to_string(),
        "months" => "month".to_string(),
        "mons" => "mon".to_string(),
        "weeks" => "week".to_string(),
        "days" => "day".to_string(),
        "hours" | "hrs" => "hour".to_string(),
        "minutes" | "mins" => "minute".to_string(),
        "seconds" | "secs" => "second".to_string(),
        "milliseconds" | "msecs" => "millisecond".to_string(),
        "microseconds" | "usecs" => "microsecond".to_string(),
        _ => lower,
    }
}

fn parse_interval_integer_amount(amount: &str) -> Result<i64> {
    amount
        .parse::<i64>()
        .map_err(|_| DbError::sql("INTERVAL calendar-unit amount must be an integer"))
}

fn parse_interval_fractional_micros(amount: &str, multiplier: i64) -> Result<i128> {
    let amount = amount.trim();
    if amount.is_empty() {
        return Err(DbError::sql("INTERVAL amount must not be empty"));
    }
    let (negative, digits) = if let Some(rest) = amount.strip_prefix('+') {
        (false, rest)
    } else if let Some(rest) = amount.strip_prefix('-') {
        (true, rest)
    } else {
        (false, amount)
    };
    let (whole, fraction) = match digits.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (digits, ""),
    };
    if whole.is_empty() && fraction.is_empty() {
        return Err(DbError::sql("INTERVAL amount must contain digits"));
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(DbError::sql("INTERVAL amount must be numeric"));
    }
    let whole_value = if whole.is_empty() {
        0_i128
    } else {
        whole
            .parse::<i128>()
            .map_err(|_| DbError::sql("INTERVAL value overflowed"))?
    };
    let mut value = whole_value
        .checked_mul(i128::from(multiplier))
        .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?;
    if !fraction.is_empty() {
        let denominator = 10_i128
            .checked_pow(
                u32::try_from(fraction.len())
                    .map_err(|_| DbError::sql("INTERVAL fractional precision is too high"))?,
            )
            .ok_or_else(|| DbError::sql("INTERVAL fractional precision is too high"))?;
        let fraction_value = fraction
            .parse::<i128>()
            .map_err(|_| DbError::sql("INTERVAL value overflowed"))?;
        let numerator = fraction_value
            .checked_mul(i128::from(multiplier))
            .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?;
        if numerator % denominator != 0 {
            return Err(DbError::sql(
                "INTERVAL fractional amount is finer than microsecond precision",
            ));
        }
        value = value
            .checked_add(numerator / denominator)
            .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?;
    }
    Ok(if negative { -value } else { value })
}

fn checked_add_i32_i64(current: i32, delta: i64) -> Result<i32> {
    let value = i64::from(current)
        .checked_add(delta)
        .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?;
    i32::try_from(value).map_err(|_| DbError::sql("INTERVAL value overflowed"))
}

fn checked_add_i64_i128(current: i64, delta: i128) -> Result<i64> {
    let value = i128::from(current)
        .checked_add(delta)
        .ok_or_else(|| DbError::sql("INTERVAL value overflowed"))?;
    i64::try_from(value).map_err(|_| DbError::sql("INTERVAL value overflowed"))
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{
        compare_decimal, compare_interval, compare_ip_addr, decode_cidr_payload,
        decode_ip_addr_payload, encode_cidr_payload, encode_ip_addr_payload, format_cidr,
        format_date_days, format_interval, format_ip_addr, format_time_micros,
        format_timestamp_tz_micros, parse_cidr, parse_date_days, parse_decimal_text,
        parse_interval, parse_ip_addr, parse_time_micros, parse_timestamp_tz_micros, IP_FAMILY_V4,
        IP_FAMILY_V6,
    };

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

    #[test]
    fn ipaddr_and_cidr_helpers_roundtrip_and_compare() {
        let (family, addr) = parse_ip_addr("10.1.2.3").expect("parse ip");
        assert_eq!(family, IP_FAMILY_V4);
        assert_eq!(format_ip_addr(family, &addr).expect("format"), "10.1.2.3");

        let mut payload = Vec::new();
        encode_ip_addr_payload(family, &addr, &mut payload).expect("encode");
        assert_eq!(payload.len(), 5);
        let decoded = decode_ip_addr_payload(&payload).expect("decode");
        assert_eq!(decoded, (family, addr));

        let (cidr_family, prefix, network) = parse_cidr("10.1.2.99/24").expect("parse cidr");
        assert_eq!(cidr_family, IP_FAMILY_V4);
        assert_eq!(
            format_cidr(cidr_family, prefix, &network).expect("format"),
            "10.1.2.0/24"
        );

        let mut cidr_payload = Vec::new();
        encode_cidr_payload(cidr_family, prefix, &network, &mut cidr_payload).expect("encode");
        assert_eq!(cidr_payload.len(), 6);
        let decoded_cidr = decode_cidr_payload(&cidr_payload).expect("decode");
        assert_eq!(decoded_cidr, (cidr_family, prefix, network));

        let (mapped_family, mapped) = parse_ip_addr("::ffff:10.1.2.3").expect("parse mapped");
        assert_eq!(mapped_family, IP_FAMILY_V6);
        assert_eq!(
            compare_ip_addr(family, &addr, mapped_family, &mapped).expect("compare"),
            Ordering::Less
        );

        let (v6_family, v6_addr) = parse_ip_addr("2001:db8::1").expect("parse v6");
        assert_eq!(v6_family, IP_FAMILY_V6);
        assert_eq!(
            compare_ip_addr(family, &addr, v6_family, &v6_addr).expect("compare"),
            Ordering::Less
        );
    }

    #[test]
    fn temporal_and_interval_helpers_roundtrip() {
        let days = parse_date_days("2026-05-18").expect("parse date");
        assert_eq!(format_date_days(days), "2026-05-18");

        let time = parse_time_micros("09:10:11.1234").expect("parse time");
        assert_eq!(
            format_time_micros(time).expect("format time"),
            "09:10:11.123400"
        );

        let ts = parse_timestamp_tz_micros("2026-05-18T09:10:11.123400-05:00").expect("parse ts");
        assert_eq!(
            format_timestamp_tz_micros(ts),
            "2026-05-18T14:10:11.123400Z"
        );

        let interval = parse_interval("12 -3 4000000").expect("parse interval");
        assert_eq!(
            format_interval(interval.0, interval.1, interval.2),
            "12 -3 4000000"
        );
        let interval = parse_interval("1 year 2 months 3 weeks 4 days 5.5 seconds")
            .expect("parse interval units");
        assert_eq!(interval, (14, 25, 5_500_000));
        assert_eq!(
            compare_interval(1, 0, 0, 0, 30, 0),
            Ordering::Greater,
            "months compare before days"
        );
    }

    #[test]
    fn rejects_unknown_ip_family() {
        let addr = [0_u8; 16];
        assert!(encode_ip_addr_payload(7, &addr, &mut Vec::new()).is_err());
        assert!(format_ip_addr(7, &addr).is_err());
        assert!(decode_ip_addr_payload(&[7, 0, 0, 0, 0]).is_err());
        assert!(encode_cidr_payload(7, 0, &addr, &mut Vec::new()).is_err());
    }
}
