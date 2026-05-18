use std::path::PathBuf;

use anyhow::Result;
use decentdb::{Db, QueryResult};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::output::{render_exec_success_json, render_rows, rows_from_query_result, OutputFormat};

pub fn run_repl(db: Db, format: OutputFormat, branch: Option<&str>) -> Result<()> {
    let mut editor = DefaultEditor::new()?;
    let history_path = history_path();
    let _ = editor.load_history(&history_path);

    let mut current_branch = branch.unwrap_or("main").to_string();
    if current_branch != "main" && db.branch_lsn(&current_branch)?.is_none() {
        anyhow::bail!("unknown branch '{}'", current_branch);
    }

    let mut buffer = String::new();
    loop {
        let prompt = if current_branch != "main" {
            if buffer.is_empty() {
                format!("decentdb({})> ", current_branch)
            } else {
                "...> ".to_string()
            }
        } else if db.in_transaction().unwrap_or(false) {
            if buffer.is_empty() {
                "decentdb*> ".to_string()
            } else {
                "...*> ".to_string()
            }
        } else if buffer.is_empty() {
            "decentdb> ".to_string()
        } else {
            "...> ".to_string()
        };

        match editor.readline(&prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.eq_ignore_ascii_case(".quit") || trimmed.eq_ignore_ascii_case(".exit") {
                    break;
                }
                if trimmed.eq_ignore_ascii_case(".help") {
                    println!(".branch\n.checkout <branch>\n.help\n.quit\n.exit");
                    continue;
                }
                if trimmed.eq_ignore_ascii_case(".branch") {
                    println!("{current_branch}");
                    continue;
                }
                if let Some(next_branch) = trimmed.strip_prefix(".checkout ") {
                    let next_branch = next_branch.trim();
                    if next_branch.is_empty() {
                        eprintln!("branch name is required");
                    } else if next_branch == "main" || db.branch_lsn(next_branch)?.is_some() {
                        current_branch = next_branch.to_string();
                        println!("{current_branch}");
                    } else {
                        eprintln!("unknown branch '{next_branch}'");
                    }
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

                let result = if current_branch == "main" {
                    db.execute_batch(&buffer)
                } else {
                    db.execute_batch_on_branch(&buffer, &current_branch)
                };
                match result {
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
