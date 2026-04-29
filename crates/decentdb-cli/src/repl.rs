use std::path::PathBuf;

use anyhow::Result;
use decentdb::{Db, QueryResult};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::output::{render_exec_success_json, render_rows, rows_from_query_result, OutputFormat};

pub fn run_repl(db: Db, format: OutputFormat) -> Result<()> {
    let mut editor = DefaultEditor::new()?;
    let history_path = history_path();
    let _ = editor.load_history(&history_path);

    let mut buffer = String::new();
    loop {
        let prompt = if db.in_transaction().unwrap_or(false) {
            if buffer.is_empty() {
                "decentdb*> "
            } else {
                "...*> "
            }
        } else if buffer.is_empty() {
            "decentdb> "
        } else {
            "...> "
        };

        match editor.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.eq_ignore_ascii_case(".quit") || trimmed.eq_ignore_ascii_case(".exit") {
                    break;
                }
                if trimmed.eq_ignore_ascii_case(".help") {
                    println!(".help\n.quit\n.exit");
                    continue;
                }
                if !trimmed.is_empty() {
                    editor.add_history_entry(trimmed)?;
                }

                if !buffer.is_empty() {
                    buffer.push('\n');
                }
                buffer.push_str(&line);

                if !statement_complete(&buffer) {
                    continue;
                }

                match db.execute_batch(&buffer) {
                    Ok(results) => print_results(format, &results),
                    Err(error) => eprintln!("{error}"),
                }
                buffer.clear();
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(error) => return Err(error.into()),
        }
    }

    let _ = editor.save_history(&history_path);
    Ok(())
}

fn print_results(format: OutputFormat, results: &[QueryResult]) {
    match format {
        OutputFormat::Json => println!("{}", render_exec_success_json(results, 0.0, false)),
        OutputFormat::Csv | OutputFormat::Table | OutputFormat::Markdown => {
            for (index, result) in results.iter().enumerate() {
                if index > 0 {
                    println!();
                }
                let rows = rows_from_query_result(result);
                println!(
                    "{}",
                    render_rows(
                        format,
                        result.columns(),
                        &rows,
                        !result.columns().is_empty()
                    )
                );
            }
        }
    }
}

fn history_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    home.join(".decentdb_history")
}

fn statement_complete(sql: &str) -> bool {
    let trimmed = sql.trim();
    if trimmed.eq_ignore_ascii_case("BEGIN")
        || trimmed.eq_ignore_ascii_case("BEGIN TRANSACTION")
        || trimmed.eq_ignore_ascii_case("COMMIT")
        || trimmed.eq_ignore_ascii_case("END")
        || trimmed.eq_ignore_ascii_case("ROLLBACK")
    {
        return true;
    }

    let mut in_single = false;
    let mut in_double = false;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_double => {
                if in_single && matches!(chars.peek(), Some('\'')) {
                    let _ = chars.next();
                } else {
                    in_single = !in_single;
                }
            }
            '"' if !in_single => {
                if in_double && matches!(chars.peek(), Some('"')) {
                    let _ = chars.next();
                } else {
                    in_double = !in_double;
                }
            }
            ';' if !in_single && !in_double => return true,
            _ => {}
        }
    }
    false
}
