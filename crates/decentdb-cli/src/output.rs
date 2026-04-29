use clap::ValueEnum;

use decentdb::{QueryResult, Value};

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
        Value::Blob(value) => format!("0x{}", hex_encode(value)),
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
    format!(
        "{{\"ok\":false,\"error\":{{\"message\":\"{}\"}}}}",
        json_escape(message)
    )
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
