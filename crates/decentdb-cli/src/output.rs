use clap::ValueEnum;

use decentdb::{DbError, QueryResult, Value};
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Json,
    Csv,
    Table,
    Markdown,
}

pub fn stringify_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Blob(value) | Value::Geometry(value) | Value::Geography(value) => {
            format!("0x{}", hex_encode(value))
        }
        Value::Decimal { scaled, scale } => {
            if *scale == 0 {
                scaled.to_string()
            } else {
                let negative = *scaled < 0;
                let digits = scaled.unsigned_abs().to_string();
                let scale = usize::from(*scale);
                let padded = if digits.len() <= scale {
                    format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
                } else {
                    digits
                };
                let split = padded.len() - scale;
                let mut output = format!("{}.{}", &padded[..split], &padded[split..]);
                if negative {
                    output.insert(0, '-');
                }
                output
            }
        }
        Value::Uuid(value) => hex_encode(value),
        Value::TimestampMicros(value) => value.to_string(),
        Value::Enum {
            enum_type_id,
            label_id,
        } => format!("{enum_type_id}:{label_id}"),
        Value::IpAddr { family, addr } => format_ip_addr(*family, addr),
        Value::Cidr {
            family,
            prefix_len,
            network,
        } => format!("{}/{}", format_ip_addr(*family, network), prefix_len),
        Value::MacAddr { len, bytes } => format_mac_addr(*len, bytes),
        Value::DateDays(days) => format_date_days(*days),
        Value::TimeMicros(micros) => format_time_micros(*micros),
        Value::TimestampTzMicros(micros) => format!("{}Z", format_timestamp_micros(*micros, 'T')),
        Value::Interval {
            months,
            days,
            micros,
        } => format!("{months} {days} {micros}"),
    }
}

pub fn rows_from_query_result(result: &QueryResult) -> Vec<Vec<String>> {
    result
        .rows()
        .iter()
        .map(|row| row.values().iter().map(stringify_value).collect())
        .collect()
}

pub fn render_rows(
    format: OutputFormat,
    columns: &[String],
    rows: &[Vec<String>],
    include_header: bool,
) -> String {
    match format {
        OutputFormat::Json => render_json_rows(columns, rows),
        OutputFormat::Csv => render_csv(columns, rows, include_header),
        OutputFormat::Table | OutputFormat::Markdown => render_table(columns, rows, include_header),
    }
}

pub fn render_exec_success_json(
    results: &[QueryResult],
    elapsed_ms: f64,
    checkpointed: bool,
) -> String {
    let rendered_results = results
        .iter()
        .map(|result| {
            let rows = rows_from_query_result(result)
                .into_iter()
                .map(|row| {
                    format!(
                        "[{}]",
                        row.iter()
                            .map(|value| format!("\"{}\"", json_escape(value)))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "{{\"columns\":[{}],\"rows\":[{}],\"affected_rows\":{},\"explain_lines\":[{}]}}",
                result
                    .columns()
                    .iter()
                    .map(|column| format!("\"{}\"", json_escape(column)))
                    .collect::<Vec<_>>()
                    .join(","),
                rows,
                result.affected_rows(),
                result
                    .explain_lines()
                    .iter()
                    .map(|line| format!("\"{}\"", json_escape(line)))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "{{\"ok\":true,\"elapsed_ms\":{elapsed_ms:.3},\"checkpointed\":{},\"results\":[{rendered_results}]}}",
        if checkpointed { "true" } else { "false" }
    )
}

pub fn render_error_json(message: &str) -> String {
    serde_json::json!({"ok": false, "error": {"message": message}}).to_string()
}

pub fn render_error_json_for_error(error: &anyhow::Error) -> String {
    if let Some(db_error) = error.downcast_ref::<DbError>() {
        let diagnostic = db_error.diagnostic();
        serde_json::json!({
            "ok": false,
            "error": {
                "code": diagnostic.code_name,
                "native_code": db_error.numeric_code(),
                "subcode": diagnostic.subcode,
                "message": db_error.to_string(),
                "diagnostic": diagnostic,
            }
        })
        .to_string()
    } else {
        render_error_json(&error.to_string())
    }
}

pub fn render_key_value_rows(format: OutputFormat, rows: &[(String, String)]) -> String {
    let columns = vec!["field".to_string(), "value".to_string()];
    let rows = rows
        .iter()
        .map(|(field, value)| vec![field.clone(), value.clone()])
        .collect::<Vec<_>>();
    render_rows(format, &columns, &rows, true)
}

fn render_json_rows(columns: &[String], rows: &[Vec<String>]) -> String {
    let rendered_rows = rows
        .iter()
        .map(|row| {
            format!(
                "[{}]",
                row.iter()
                    .map(|value| format!("\"{}\"", json_escape(value)))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"columns\":[{}],\"rows\":[{}]}}",
        columns
            .iter()
            .map(|column| format!("\"{}\"", json_escape(column)))
            .collect::<Vec<_>>()
            .join(","),
        rendered_rows
    )
}

fn render_csv(columns: &[String], rows: &[Vec<String>], include_header: bool) -> String {
    let mut lines = Vec::new();
    if include_header {
        lines.push(
            columns
                .iter()
                .map(|column| csv_escape(column))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    lines.extend(rows.iter().map(|row| {
        row.iter()
            .map(|value| csv_escape(value))
            .collect::<Vec<_>>()
            .join(",")
    }));
    lines.join("\n")
}

fn render_table(columns: &[String], rows: &[Vec<String>], include_header: bool) -> String {
    let mut widths = columns
        .iter()
        .map(|column| column.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            if index >= widths.len() {
                widths.push(value.len());
            } else {
                widths[index] = widths[index].max(value.len());
            }
        }
    }

    let render_row = |row: &[String]| {
        row.iter()
            .enumerate()
            .map(|(index, value)| format!("{value:<width$}", width = widths[index]))
            .collect::<Vec<_>>()
            .join(" | ")
    };

    let mut lines = Vec::new();
    if include_header {
        lines.push(render_row(columns));
        lines.push(
            widths
                .iter()
                .map(|width| "-".repeat(*width))
                .collect::<Vec<_>>()
                .join("-+-"),
        );
    }
    lines.extend(rows.iter().map(|row| render_row(row)));
    lines.join("\n")
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

pub fn json_escape(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;

        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn format_mac_addr(len: u8, bytes: &[u8; 8]) -> String {
    if len != 6 && len != 8 {
        return format!("<invalid-macaddr-length-{len}>");
    }
    bytes[..usize::from(len)]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

fn format_ip_addr(family: u8, addr: &[u8; 16]) -> String {
    match family {
        4 => Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3]).to_string(),
        6 => Ipv6Addr::from(*addr).to_string(),
        _ => format!("<invalid-ip-family-{family}>"),
    }
}

fn format_date_days(days: i32) -> String {
    let (year, month, day) = civil_from_days(i64::from(days));
    format!("{}-{month:02}-{day:02}", format_year(year))
}

fn format_time_micros(micros: i64) -> String {
    if !(0..86_400_000_000).contains(&micros) {
        return format!("<invalid-time-{micros}>");
    }
    format_time_of_day(micros)
}

fn format_timestamp_micros(micros: i64, separator: char) -> String {
    let days = micros.div_euclid(86_400_000_000);
    let time = micros.rem_euclid(86_400_000_000);
    format!(
        "{}{}{}",
        format_date_days(days as i32),
        separator,
        format_time_of_day(time)
    )
}

fn format_time_of_day(micros: i64) -> String {
    let hour = micros / 3_600_000_000;
    let minute = (micros % 3_600_000_000) / 60_000_000;
    let second = (micros % 60_000_000) / 1_000_000;
    let fraction = micros % 1_000_000;
    format!("{hour:02}:{minute:02}:{second:02}.{fraction:06}")
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
