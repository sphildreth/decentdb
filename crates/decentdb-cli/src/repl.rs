use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, Result};
use decentdb::{
    BulkLoadOptions, ColumnInfo, Db, ForeignKeyInfo, IndexInfo, QueryResult, SchemaSnapshot,
    TableInfo, Value, ViewInfo,
};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::output::{json_escape, render_exec_success_json, stringify_value, OutputFormat};

pub fn run_repl(db: Db, format: OutputFormat, branch: Option<&str>) -> Result<()> {
    let mut editor = DefaultEditor::new()?;
    let history_path = history_path();
    let _ = editor.load_history(&history_path);

    print_welcome();

    let mut session = ReplSession::new(db, format, branch)?;

    loop {
        match editor.readline(&session.prompt()) {
            Ok(line) => {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    editor.add_history_entry(trimmed)?;
                }
                match session.handle_line(&line) {
                    Ok(ReplAction::Continue) => {}
                    Ok(ReplAction::Quit) => break,
                    Err(error) => eprintln!("{error}"),
                }
            }
            Err(ReadlineError::Interrupted) => {
                if session.clear_buffer() {
                    eprintln!("statement canceled");
                    continue;
                }
                break;
            }
            Err(ReadlineError::Eof) => break,
            Err(error) => return Err(error.into()),
        }
    }

    let _ = editor.save_history(&history_path);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplAction {
    Continue,
    Quit,
}

#[derive(Debug)]
struct ReplSettings {
    format: OutputFormat,
    headers: bool,
    null_value: String,
    widths: Vec<usize>,
    timer: bool,
    output_path: Option<PathBuf>,
    once_path: Option<PathBuf>,
}

struct ReplSession {
    db: Db,
    settings: ReplSettings,
    current_branch: String,
    buffer: String,
    command_history: Vec<String>,
    last_sql: Option<String>,
    params: Vec<Value>,
    read_depth: usize,
}

impl ReplSession {
    fn new(db: Db, format: OutputFormat, branch: Option<&str>) -> Result<Self> {
        let current_branch = branch.unwrap_or("main").to_string();
        if current_branch != "main" && db.branch_lsn(&current_branch)?.is_none() {
            anyhow::bail!("unknown branch '{}'", current_branch);
        }
        Ok(Self {
            db,
            settings: ReplSettings {
                format,
                headers: true,
                null_value: "NULL".to_string(),
                widths: Vec::new(),
                timer: false,
                output_path: None,
                once_path: None,
            },
            current_branch,
            buffer: String::new(),
            command_history: Vec::new(),
            last_sql: None,
            params: Vec::new(),
            read_depth: 0,
        })
    }

    fn prompt(&self) -> String {
        if self.current_branch != "main" {
            if self.buffer.is_empty() {
                format!("decentdb({})> ", self.current_branch)
            } else {
                "...> ".to_string()
            }
        } else if self.db.in_transaction().unwrap_or(false) {
            if self.buffer.is_empty() {
                "decentdb*> ".to_string()
            } else {
                "...*> ".to_string()
            }
        } else if self.buffer.is_empty() {
            "decentdb> ".to_string()
        } else {
            "...> ".to_string()
        }
    }

    fn clear_buffer(&mut self) -> bool {
        let had_buffer = !self.buffer.is_empty();
        self.buffer.clear();
        had_buffer
    }

    fn handle_line(&mut self, line: &str) -> Result<ReplAction> {
        let trimmed = line.trim();
        if self.buffer.is_empty() && self.handle_meta_command(trimmed)? {
            return Ok(ReplAction::Continue);
        }
        if self.buffer.is_empty() && is_quit_command(trimmed) {
            return Ok(ReplAction::Quit);
        }

        if !self.buffer.is_empty() {
            self.buffer.push('\n');
        }
        self.buffer.push_str(line);

        if !statement_complete(&self.buffer) {
            return Ok(ReplAction::Continue);
        }

        let sql = self.buffer.clone();
        self.buffer.clear();
        self.command_history.push(sql.clone());
        self.last_sql = Some(sql.clone());
        self.execute_sql(&sql)?;
        Ok(ReplAction::Continue)
    }

    fn handle_meta_command(&mut self, trimmed: &str) -> Result<bool> {
        if trimmed.is_empty() {
            return Ok(true);
        }
        if is_quit_command(trimmed) {
            return Ok(false);
        }
        if let Some(topic) = help_topic(trimmed) {
            self.emit(print_help(topic))?;
            return Ok(true);
        }
        if trimmed.eq_ignore_ascii_case(".dt") || trimmed.eq_ignore_ascii_case(".tables") {
            self.remember_meta(trimmed);
            self.emit(self.render_tables()?)?;
            return Ok(true);
        }
        if trimmed.eq_ignore_ascii_case(".df") || trimmed.eq_ignore_ascii_case(".functions") {
            self.remember_meta(trimmed);
            self.emit(render_functions(&self.settings))?;
            return Ok(true);
        }
        if trimmed.eq_ignore_ascii_case(".s") || trimmed.eq_ignore_ascii_case(".history") {
            self.emit(render_history(&self.settings, &self.command_history))?;
            return Ok(true);
        }
        if trimmed.eq_ignore_ascii_case(".g") {
            self.rerun_last_sql()?;
            return Ok(true);
        }
        if let Some(table_name) = command_arg(trimmed, ".d") {
            self.remember_meta(trimmed);
            self.emit(self.render_describe(required_arg(table_name, "table name")?)?)?;
            return Ok(true);
        }
        if let Some(name) = command_arg(trimmed, ".schema") {
            self.remember_meta(trimmed);
            self.emit(self.render_schema(name)?)?;
            return Ok(true);
        }
        if let Some(table) = command_arg(trimmed, ".indexes") {
            self.remember_meta(trimmed);
            self.emit(self.render_indexes(table)?)?;
            return Ok(true);
        }
        if trimmed.eq_ignore_ascii_case(".views") {
            self.remember_meta(trimmed);
            self.emit(self.render_views()?)?;
            return Ok(true);
        }
        if let Some(mode) = command_arg(trimmed, ".mode") {
            self.remember_meta(trimmed);
            self.set_mode(mode)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".headers") {
            self.remember_meta(trimmed);
            self.set_headers(value)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".nullvalue") {
            self.remember_meta(trimmed);
            self.settings.null_value = value.to_string();
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".width") {
            self.remember_meta(trimmed);
            self.set_widths(value)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".timer") {
            self.remember_meta(trimmed);
            self.set_timer(value)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".output") {
            self.remember_meta(trimmed);
            self.set_output(value)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".once") {
            self.remember_meta(trimmed);
            self.set_once(value)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".read") {
            self.remember_meta(trimmed);
            self.read_file(required_arg(value, "file path")?)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".import") {
            self.remember_meta(trimmed);
            self.import_csv(value)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".export") {
            self.remember_meta(trimmed);
            self.export_table(value)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".explain-analyze") {
            self.remember_meta(trimmed);
            self.explain(value, true)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".explain") {
            self.remember_meta(trimmed);
            self.explain(value, false)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".plan") {
            self.remember_meta(trimmed);
            self.explain(value, false)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".param") {
            self.remember_meta(trimmed);
            self.handle_params(value)?;
            return Ok(true);
        }
        if let Some(value) = command_arg(trimmed, ".parameter") {
            self.remember_meta(trimmed);
            self.handle_params(value)?;
            return Ok(true);
        }
        if let Some(branch_name) = command_arg(trimmed, ".branch") {
            self.handle_branch(branch_name, trimmed)?;
            return Ok(true);
        }
        if let Some(next_branch) = command_arg(trimmed, ".checkout") {
            self.checkout_branch(next_branch, trimmed)?;
            return Ok(true);
        }
        if trimmed.starts_with('.') || trimmed.starts_with('\\') {
            return Err(anyhow!("unknown REPL command '{trimmed}'"));
        }
        Ok(false)
    }

    fn remember_meta(&mut self, command: &str) {
        if !command.is_empty() {
            self.command_history.push(command.to_string());
        }
    }

    fn emit(&mut self, text: String) -> Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        if let Some(path) = self.settings.once_path.take() {
            write_text_file(&path, &text)?;
            return Ok(());
        }
        if let Some(path) = &self.settings.output_path {
            append_text_file(path, &text)?;
            return Ok(());
        }
        println!("{text}");
        Ok(())
    }

    fn execute_sql(&mut self, sql: &str) -> Result<()> {
        let started = Instant::now();
        let result = if self.current_branch == "main" {
            self.db.execute_batch_with_params(sql, &self.params)
        } else {
            self.db
                .execute_batch_on_branch_with_params(sql, &self.current_branch, &self.params)
        };
        match result {
            Ok(results) => {
                let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
                self.emit(render_results(&self.settings, &results, elapsed_ms))?;
            }
            Err(error) => eprintln!("{error}"),
        }
        Ok(())
    }

    fn rerun_last_sql(&mut self) -> Result<()> {
        if let Some(sql) = self.last_sql.clone() {
            self.command_history.push(sql.clone());
            self.execute_sql(&sql)?;
        } else {
            eprintln!("no previous SQL command");
        }
        Ok(())
    }

    fn render_tables(&self) -> Result<String> {
        let tables = self.db.list_tables()?;
        let rows = tables
            .iter()
            .map(|table| vec![table.name.clone(), table.row_count.to_string()])
            .collect::<Vec<_>>();
        Ok(render_rows_config(
            &self.settings,
            &["name".to_string(), "row_count".to_string()],
            &rows,
        ))
    }

    fn render_describe(&self, table_name: &str) -> Result<String> {
        let table = self.db.describe_table(table_name)?;
        let rows = table
            .columns
            .iter()
            .map(|column| {
                vec![
                    column.name.clone(),
                    column.column_type.clone(),
                    column_constraints(&table, column),
                ]
            })
            .collect::<Vec<_>>();
        Ok(render_rows_config(
            &self.settings,
            &[
                "column".to_string(),
                "type".to_string(),
                "constraints".to_string(),
            ],
            &rows,
        ))
    }

    fn render_schema(&self, name: &str) -> Result<String> {
        let snapshot = self.db.get_schema_snapshot()?;
        if name.is_empty() {
            return Ok(schema_lines(&snapshot).join("\n"));
        }
        let mut lines = Vec::new();
        for table in &snapshot.tables {
            if table.name.eq_ignore_ascii_case(name) {
                lines.push(table.ddl.clone());
                lines.extend(
                    snapshot
                        .indexes
                        .iter()
                        .filter(|index| index.table_name.eq_ignore_ascii_case(name))
                        .map(|index| index.ddl.clone()),
                );
            }
        }
        lines.extend(
            snapshot
                .views
                .iter()
                .filter(|view| view.name.eq_ignore_ascii_case(name))
                .map(|view| view.ddl.clone()),
        );
        lines.extend(
            snapshot
                .indexes
                .iter()
                .filter(|index| index.name.eq_ignore_ascii_case(name))
                .map(|index| index.ddl.clone()),
        );
        lines.extend(
            snapshot
                .triggers
                .iter()
                .filter(|trigger| trigger.name.eq_ignore_ascii_case(name))
                .map(|trigger| trigger.ddl.clone()),
        );
        if lines.is_empty() {
            return Err(anyhow!("no schema object named '{name}'"));
        }
        Ok(lines.join("\n"))
    }

    fn render_indexes(&self, table_filter: &str) -> Result<String> {
        let mut indexes = self.db.list_indexes()?;
        if !table_filter.is_empty() {
            indexes.retain(|index| index.table_name.eq_ignore_ascii_case(table_filter));
        }
        let rows = indexes.iter().map(index_row).collect::<Vec<_>>();
        Ok(render_rows_config(
            &self.settings,
            &[
                "name".to_string(),
                "table".to_string(),
                "kind".to_string(),
                "unique".to_string(),
                "fresh".to_string(),
                "columns".to_string(),
                "predicate".to_string(),
            ],
            &rows,
        ))
    }

    fn render_views(&self) -> Result<String> {
        let views = self.db.list_views()?;
        let rows = views.iter().map(view_row).collect::<Vec<_>>();
        Ok(render_rows_config(
            &self.settings,
            &[
                "name".to_string(),
                "columns".to_string(),
                "dependencies".to_string(),
            ],
            &rows,
        ))
    }

    fn set_mode(&mut self, mode: &str) -> Result<()> {
        let mode = required_arg(mode, "mode")?;
        self.settings.format = match mode.to_ascii_lowercase().as_str() {
            "table" | "column" => OutputFormat::Table,
            "csv" => OutputFormat::Csv,
            "json" => OutputFormat::Json,
            "markdown" | "md" => OutputFormat::Markdown,
            other => return Err(anyhow!("unsupported mode '{other}'")),
        };
        Ok(())
    }

    fn set_headers(&mut self, value: &str) -> Result<()> {
        self.settings.headers = parse_on_off(required_arg(value, "on or off")?)?;
        Ok(())
    }

    fn set_widths(&mut self, value: &str) -> Result<()> {
        let value = value.trim();
        if value.is_empty() || value.eq_ignore_ascii_case("auto") {
            self.settings.widths.clear();
            return Ok(());
        }
        self.settings.widths = value
            .split_whitespace()
            .map(|part| part.parse::<usize>().map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        Ok(())
    }

    fn set_timer(&mut self, value: &str) -> Result<()> {
        self.settings.timer = parse_on_off(required_arg(value, "on or off")?)?;
        Ok(())
    }

    fn set_output(&mut self, value: &str) -> Result<()> {
        let value = value.trim();
        if value.is_empty()
            || value.eq_ignore_ascii_case("stdout")
            || value.eq_ignore_ascii_case("off")
        {
            self.settings.output_path = None;
            println!("output stdout");
            return Ok(());
        }
        let path = PathBuf::from(value);
        write_text_file(&path, "")?;
        self.settings.output_path = Some(path.clone());
        println!("output {}", path.display());
        Ok(())
    }

    fn set_once(&mut self, value: &str) -> Result<()> {
        let path = PathBuf::from(required_arg(value, "file path")?);
        write_text_file(&path, "")?;
        self.settings.once_path = Some(path.clone());
        println!("once {}", path.display());
        Ok(())
    }

    fn read_file(&mut self, path: &str) -> Result<()> {
        if self.read_depth >= 8 {
            return Err(anyhow!(".read nesting is limited to 8 files"));
        }
        let input = fs::read_to_string(path)?;
        self.read_depth += 1;
        for line in input.lines() {
            match self.handle_line(line) {
                Ok(ReplAction::Continue) => {}
                Ok(ReplAction::Quit) => break,
                Err(error) => eprintln!("{error}"),
            }
        }
        self.read_depth -= 1;
        if !self.buffer.is_empty() {
            eprintln!("incomplete statement at end of {path}");
            self.buffer.clear();
        }
        Ok(())
    }

    fn import_csv(&mut self, value: &str) -> Result<()> {
        let args = split_args(value)?;
        if args.len() < 2 || args.len() > 3 {
            return Err(anyhow!("usage: .import <csv-file> <table> [batch-size]"));
        }
        let batch_size = if let Some(raw) = args.get(2) {
            raw.parse::<usize>()?
        } else {
            10_000
        };
        let (columns, rows) = parse_csv_file(Path::new(&args[0]))?;
        let column_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
        let inserted = self.db.bulk_load_rows(
            &args[1],
            &column_refs,
            &rows,
            BulkLoadOptions {
                batch_size,
                ..BulkLoadOptions::default()
            },
        )?;
        self.emit(format!("{inserted}"))?;
        Ok(())
    }

    fn export_table(&mut self, value: &str) -> Result<()> {
        let args = split_args(value)?;
        if args.len() < 2 || args.len() > 3 {
            return Err(anyhow!("usage: .export <table> <output-file> [csv|json]"));
        }
        let format = match args.get(2).map(|value| value.to_ascii_lowercase()) {
            None => OutputFormat::Csv,
            Some(value) if value == "csv" => OutputFormat::Csv,
            Some(value) if value == "json" => OutputFormat::Json,
            Some(value) => return Err(anyhow!("unsupported export format '{value}'")),
        };
        let sql = format!("SELECT * FROM {}", sql_identifier(&args[0]));
        let results = if self.current_branch == "main" {
            self.db.execute_batch(&sql)?
        } else {
            self.db
                .execute_batch_on_branch(&sql, &self.current_branch)?
        };
        let Some(result) = results.first() else {
            return Err(anyhow!("export query returned no result"));
        };
        let rows = result_rows(result, &self.settings.null_value);
        let text = match format {
            OutputFormat::Csv => render_csv(result.columns(), &rows, true),
            OutputFormat::Json => render_json_rows(result.columns(), &rows),
            OutputFormat::Table | OutputFormat::Markdown => unreachable!("export format checked"),
        };
        write_text_file(Path::new(&args[1]), &text)?;
        println!("{}", args[1]);
        Ok(())
    }

    fn explain(&mut self, value: &str, analyze: bool) -> Result<()> {
        let inner = if value.trim().is_empty() {
            self.last_sql
                .as_deref()
                .ok_or_else(|| anyhow!("no previous SQL command"))?
                .trim()
        } else {
            value.trim()
        };
        let prefix = if analyze {
            "EXPLAIN ANALYZE"
        } else {
            "EXPLAIN"
        };
        self.execute_sql(&format!("{prefix} {inner}"))?;
        Ok(())
    }

    fn handle_params(&mut self, value: &str) -> Result<()> {
        let args = split_args(value)?;
        match args.first().map(String::as_str) {
            None | Some("list") => {
                self.emit(render_params(&self.settings, &self.params))?;
            }
            Some("clear") => {
                self.params.clear();
            }
            Some("set") => {
                if args.len() != 3 {
                    return Err(anyhow!("usage: .param set <index> <type:value>"));
                }
                let index = parse_param_index(&args[1])?;
                let value = parse_param_value(&args[2])?;
                if self.params.len() < index {
                    self.params.resize(index, Value::Null);
                }
                self.params[index - 1] = value;
            }
            Some("unset") => {
                if args.len() != 2 {
                    return Err(anyhow!("usage: .param unset <index>"));
                }
                let index = parse_param_index(&args[1])?;
                if let Some(value) = self.params.get_mut(index - 1) {
                    *value = Value::Null;
                }
            }
            Some(other) => return Err(anyhow!("unsupported .param command '{other}'")),
        }
        Ok(())
    }

    fn handle_branch(&mut self, branch_name: &str, raw_command: &str) -> Result<()> {
        if branch_name.is_empty() {
            self.emit(self.current_branch.clone())?;
        } else {
            let from = if self.current_branch == "main" {
                None
            } else {
                Some(self.current_branch.as_str())
            };
            match self.db.branch_create(branch_name, from) {
                Ok(branch) => {
                    self.current_branch = branch.name;
                    self.command_history.push(raw_command.to_string());
                    self.emit(self.current_branch.clone())?;
                }
                Err(error) => eprintln!("{error}"),
            }
        }
        Ok(())
    }

    fn checkout_branch(&mut self, next_branch: &str, raw_command: &str) -> Result<()> {
        let next_branch = required_arg(next_branch, "branch name")?;
        if next_branch == "main" || self.db.branch_lsn(next_branch)?.is_some() {
            self.current_branch = next_branch.to_string();
            self.command_history.push(raw_command.to_string());
            self.emit(self.current_branch.clone())?;
        } else {
            eprintln!("unknown branch '{next_branch}'");
        }
        Ok(())
    }
}

fn print_welcome() {
    println!(
        "DecentDB CLI {}\n  ___                 _   ___  ___  \n |   \\ ___ __ ___ _ _| |_|   \\| _ ) \n | |) / -_) _/ -_) ' \\  _| |) | _ \\ \n |___/\\___\\__\\___|_||_\\__|___/|___/ \n\nType \"help\" for help.",
        decentdb::version()
    );
}

fn is_quit_command(trimmed: &str) -> bool {
    trimmed.eq_ignore_ascii_case(".quit")
        || trimmed.eq_ignore_ascii_case(".exit")
        || trimmed.eq_ignore_ascii_case("\\q")
}

fn help_topic(trimmed: &str) -> Option<&str> {
    if matches!(trimmed, "\\?" | "/?") {
        return Some("");
    }
    ["help", "/help", "\\help", ".help"]
        .iter()
        .find_map(|command| command_arg(trimmed, command))
}

fn command_arg<'a>(trimmed: &'a str, command: &str) -> Option<&'a str> {
    let command_len = command.len();
    let prefix = trimmed.get(..command_len)?;
    if !prefix.eq_ignore_ascii_case(command) {
        return None;
    }
    let rest = trimmed.get(command_len..)?;
    if rest.is_empty() {
        return Some("");
    }
    if rest.starts_with(char::is_whitespace) {
        return Some(rest.trim());
    }
    None
}

fn column_constraints(table: &TableInfo, column: &ColumnInfo) -> String {
    let mut constraints = Vec::new();
    if column.primary_key {
        constraints.push("PRIMARY KEY".to_string());
    } else if table
        .primary_key_columns
        .iter()
        .any(|name| name == &column.name)
    {
        constraints.push("PRIMARY KEY PART".to_string());
    }
    if column.unique {
        constraints.push("UNIQUE".to_string());
    }
    if !column.nullable {
        constraints.push("NOT NULL".to_string());
    }
    if column.auto_increment {
        constraints.push("AUTO INCREMENT".to_string());
    }
    if let Some(default_sql) = &column.default_sql {
        constraints.push(format!("DEFAULT {default_sql}"));
    }
    for check in &column.checks {
        constraints.push(format!("CHECK ({check})"));
    }
    if let Some(foreign_key) = &column.foreign_key {
        constraints.push(format_foreign_key_for_column(foreign_key, &column.name));
    }
    for foreign_key in table
        .foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.columns.iter().any(|name| name == &column.name))
    {
        constraints.push(format_foreign_key_for_column(foreign_key, &column.name));
    }
    constraints.dedup();
    if constraints.is_empty() {
        "-".to_string()
    } else {
        constraints.join(", ")
    }
}

fn format_foreign_key_for_column(foreign_key: &ForeignKeyInfo, column_name: &str) -> String {
    let target = format!(
        "REFERENCES {}({})",
        foreign_key.referenced_table,
        foreign_key.referenced_columns.join(", ")
    );
    let mut formatted = if foreign_key.columns.len() == 1
        && foreign_key.columns.first().map(String::as_str) == Some(column_name)
    {
        target
    } else {
        format!("FOREIGN KEY ({}) {target}", foreign_key.columns.join(", "))
    };
    if !foreign_key.on_delete.eq_ignore_ascii_case("NO ACTION") {
        formatted.push_str(" ON DELETE ");
        formatted.push_str(&foreign_key.on_delete);
    }
    if !foreign_key.on_update.eq_ignore_ascii_case("NO ACTION") {
        formatted.push_str(" ON UPDATE ");
        formatted.push_str(&foreign_key.on_update);
    }
    formatted
}

fn print_help(topic: &str) -> String {
    match topic.to_ascii_lowercase().as_str() {
        "" => [
            "Help topics: schema, output, files, parameters, branches, explain, quit",
            "",
            "help aliases: help, \\?, /?, /help, \\help, .help",
            "quit aliases: .quit, .exit, \\q",
            "",
            ".tables, .dt          List tables",
            ".d <table>            Describe columns, types, and constraints",
            ".schema [object]      Show schema DDL",
            ".indexes [table]      List indexes",
            ".views                List views",
            ".df, .functions       List functions",
            ".g                    Run last SQL command",
            ".s, .history          Show session history",
            ".mode <mode>          Set table, csv, json, or markdown output",
            ".headers on|off       Show or hide result headers",
            ".nullvalue <text>     Set rendered NULL text",
            ".width [n ...|auto]   Set table display widths",
            ".timer on|off         Show execution time",
            ".read <file>          Run commands from a file",
            ".output <file|stdout> Redirect output",
            ".once <file>          Redirect next output only",
            ".import <csv> <table> [batch-size]",
            ".export <table> <file> [csv|json]",
            ".explain <sql>        Run EXPLAIN",
            ".explain-analyze <sql>",
            ".param list|clear|set|unset",
            ".branch [name]        Show or create branch",
            ".checkout <branch>    Checkout branch",
        ]
        .join("\n"),
        "schema" => [
            ".tables or .dt lists tables with row counts.",
            ".d <table> shows columns, types, and constraints.",
            ".schema with no argument prints table, view, index, and trigger DDL.",
            ".schema <object> prints DDL for a table, view, index, or trigger.",
            ".indexes [table] lists all indexes or indexes for one table.",
            ".views lists views.",
        ]
        .join("\n"),
        "output" => [
            ".mode table|csv|json|markdown changes result rendering.",
            ".headers on|off controls table, CSV, and Markdown headers.",
            ".nullvalue <text> controls rendered NULL values in text outputs.",
            ".width n ... sets fixed table column widths; .width auto clears them.",
            ".timer on|off prints elapsed time after SQL execution.",
        ]
        .join("\n"),
        "files" => [
            ".read <file> runs SQL and dot commands from a file.",
            ".output <file> redirects subsequent output to a file.",
            ".output stdout clears output redirection.",
            ".once <file> redirects only the next command output.",
            ".import <csv> <table> [batch-size] bulk-loads CSV rows.",
            ".export <table> <file> [csv|json] exports table rows.",
        ]
        .join("\n"),
        "parameters" | "params" => [
            "Use $1, $2, ... in SQL, then set values with .param.",
            ".param list",
            ".param clear",
            ".param set <index> <type:value>",
            ".param unset <index>",
            "Supported value types: null, int:value, float:value, bool:value, text:value, timestamp:value, blob:hex.",
        ]
        .join("\n"),
        "branches" | "branch" => [
            ".branch shows the active branch.",
            ".branch <name> creates a branch from the current branch and checks it out.",
            ".checkout <branch> switches to an existing branch or main.",
        ]
        .join("\n"),
        "explain" | "plan" => [
            ".explain <sql> runs EXPLAIN for a statement.",
            ".plan <sql> is an alias for .explain.",
            ".explain-analyze <sql> runs EXPLAIN ANALYZE.",
            "Without SQL, explain helpers use the last SQL command.",
        ]
        .join("\n"),
        "quit" | "exit" => ".quit, .exit, and \\q all quit the REPL.".to_string(),
        other => format!("unknown help topic '{other}'"),
    }
}

fn render_functions(settings: &ReplSettings) -> String {
    let rows = BUILTIN_FUNCTIONS
        .iter()
        .map(|(name, kind)| vec![(*name).to_string(), (*kind).to_string()])
        .collect::<Vec<_>>();
    let columns = vec!["name".to_string(), "kind".to_string()];
    render_rows_config(settings, &columns, &rows)
}

fn render_history(settings: &ReplSettings, history: &[String]) -> String {
    let rows = history
        .iter()
        .enumerate()
        .map(|(index, command)| {
            vec![
                (index + 1).to_string(),
                command.replace('\n', "\\n").to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let columns = vec!["index".to_string(), "command".to_string()];
    render_rows_config(settings, &columns, &rows)
}

fn render_params(settings: &ReplSettings, params: &[Value]) -> String {
    let rows = params
        .iter()
        .enumerate()
        .map(|(index, value)| vec![(index + 1).to_string(), stringify_value(value)])
        .collect::<Vec<_>>();
    render_rows_config(settings, &["index".to_string(), "value".to_string()], &rows)
}

fn render_results(settings: &ReplSettings, results: &[QueryResult], elapsed_ms: f64) -> String {
    if settings.format == OutputFormat::Json {
        return render_exec_success_json(results, elapsed_ms, false);
    }
    let mut rendered = Vec::new();
    for result in results {
        let rows = result_rows(result, &settings.null_value);
        let chunk = render_rows_config_with_header(
            settings,
            result.columns(),
            &rows,
            settings.headers && !result.columns().is_empty(),
        );
        if !chunk.is_empty() {
            rendered.push(chunk);
        }
    }
    if settings.timer {
        rendered.push(format!("Time: {elapsed_ms:.3} ms"));
    }
    rendered.join("\n\n")
}

fn result_rows(result: &QueryResult, null_value: &str) -> Vec<Vec<String>> {
    result
        .rows()
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| {
                    if matches!(value, Value::Null) {
                        null_value.to_string()
                    } else {
                        stringify_value(value)
                    }
                })
                .collect()
        })
        .collect()
}

fn render_rows_config(settings: &ReplSettings, columns: &[String], rows: &[Vec<String>]) -> String {
    render_rows_config_with_header(settings, columns, rows, settings.headers)
}

fn render_rows_config_with_header(
    settings: &ReplSettings,
    columns: &[String],
    rows: &[Vec<String>],
    include_header: bool,
) -> String {
    match settings.format {
        OutputFormat::Json => render_json_rows(columns, rows),
        OutputFormat::Csv => render_csv(columns, rows, include_header),
        OutputFormat::Table => render_table(columns, rows, include_header, &settings.widths),
        OutputFormat::Markdown => render_markdown_table(columns, rows, include_header),
    }
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

fn render_table(
    columns: &[String],
    rows: &[Vec<String>],
    include_header: bool,
    configured_widths: &[usize],
) -> String {
    let mut widths = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            configured_widths
                .get(index)
                .copied()
                .unwrap_or(column.len())
        })
        .collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            if index >= widths.len() {
                widths.push(configured_widths.get(index).copied().unwrap_or(value.len()));
            } else if configured_widths.get(index).copied().unwrap_or(0) == 0 {
                widths[index] = widths[index].max(value.len());
            }
        }
    }

    let render_row = |row: &[String]| {
        row.iter()
            .enumerate()
            .map(|(index, value)| {
                let width = widths[index];
                format!("{:<width$}", fit_cell(value, width), width = width)
            })
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

fn render_markdown_table(columns: &[String], rows: &[Vec<String>], include_header: bool) -> String {
    if !include_header {
        return rows
            .iter()
            .map(|row| format!("| {} |", row.join(" | ")))
            .collect::<Vec<_>>()
            .join("\n");
    }
    let header = format!("| {} |", columns.join(" | "));
    let separator = format!(
        "| {} |",
        columns
            .iter()
            .map(|_| "---")
            .collect::<Vec<_>>()
            .join(" | ")
    );
    let mut lines = vec![header, separator];
    lines.extend(rows.iter().map(|row| format!("| {} |", row.join(" | "))));
    lines.join("\n")
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn fit_cell(value: &str, width: usize) -> String {
    if width == 0 {
        return value.to_string();
    }
    let mut output = String::new();
    for ch in value.chars().take(width) {
        output.push(ch);
    }
    output
}

fn schema_lines(snapshot: &SchemaSnapshot) -> Vec<String> {
    let mut lines = Vec::new();
    lines.extend(snapshot.tables.iter().map(|table| table.ddl.clone()));
    lines.extend(snapshot.views.iter().map(|view| view.ddl.clone()));
    lines.extend(snapshot.indexes.iter().map(|index| index.ddl.clone()));
    lines.extend(snapshot.triggers.iter().map(|trigger| trigger.ddl.clone()));
    lines
}

fn index_row(index: &IndexInfo) -> Vec<String> {
    vec![
        index.name.clone(),
        index.table_name.clone(),
        index.kind.clone(),
        index.unique.to_string(),
        index.fresh.to_string(),
        index.columns.join(", "),
        index.predicate_sql.clone().unwrap_or_default(),
    ]
}

fn view_row(view: &ViewInfo) -> Vec<String> {
    vec![
        view.name.clone(),
        view.column_names.join(", "),
        view.dependencies.join(", "),
    ]
}

fn required_arg<'a>(value: &'a str, label: &str) -> Result<&'a str> {
    let value = value.trim();
    if value.is_empty() {
        Err(anyhow!("{label} is required"))
    } else {
        Ok(value)
    }
}

fn parse_on_off(value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" | "1" => Ok(true),
        "off" | "false" | "0" => Ok(false),
        other => Err(anyhow!("expected on or off, got '{other}'")),
    }
}

fn write_text_file(path: &Path, text: &str) -> Result<()> {
    fs::write(path, with_trailing_newline(text)?)?;
    Ok(())
}

fn append_text_file(path: &Path, text: &str) -> Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(with_trailing_newline(text)?.as_bytes())?;
    Ok(())
}

fn with_trailing_newline(text: &str) -> Result<String> {
    if text.is_empty() || text.ends_with('\n') {
        Ok(text.to_string())
    } else {
        Ok(format!("{text}\n"))
    }
}

fn split_args(input: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        match ch {
            '\\' if !in_single => escaped = true,
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            ch if ch.is_whitespace() && !in_single && !in_double => {
                if !current.is_empty() {
                    args.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if escaped {
        current.push('\\');
    }
    if in_single || in_double {
        return Err(anyhow!("unterminated quote"));
    }
    if !current.is_empty() {
        args.push(current);
    }
    Ok(args)
}

fn parse_param_index(raw: &str) -> Result<usize> {
    let index = raw.trim_start_matches('$').parse::<usize>()?;
    if index == 0 {
        return Err(anyhow!("parameter indexes start at 1"));
    }
    Ok(index)
}

fn parse_param_value(raw: &str) -> Result<Value> {
    if raw.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }
    let Some((kind, value)) = raw.split_once(':') else {
        return Err(anyhow!("invalid parameter {raw}; expected type:value"));
    };
    match kind {
        "int" | "int64" => Ok(Value::Int64(value.parse()?)),
        "float" | "float64" => Ok(Value::Float64(value.parse()?)),
        "bool" => Ok(Value::Bool(value.parse()?)),
        "text" => Ok(Value::Text(value.to_string())),
        "timestamp" => Ok(Value::TimestampMicros(value.parse()?)),
        "blob" => Ok(Value::Blob(hex_to_bytes(value)?)),
        _ => Err(anyhow!("unsupported parameter kind {kind}")),
    }
}

fn parse_csv_file(path: &Path) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let input = fs::read_to_string(path)?;
    let mut lines = input.lines();
    let header = lines.next().ok_or_else(|| anyhow!("CSV input is empty"))?;
    let columns = split_csv_line(header);
    let rows = lines
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            split_csv_line(line)
                .into_iter()
                .map(|value| infer_value(&value))
                .collect()
        })
        .collect::<Vec<Vec<Value>>>();
    Ok((columns, rows))
}

fn split_csv_line(line: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && matches!(chars.peek(), Some('"')) {
                    current.push('"');
                    let _ = chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                values.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    values.push(current.trim().to_string());
    values
}

fn infer_value(raw: &str) -> Value {
    if raw.is_empty() {
        Value::Null
    } else if let Ok(value) = raw.parse::<i64>() {
        Value::Int64(value)
    } else if let Ok(value) = raw.parse::<f64>() {
        Value::Float64(value)
    } else if raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("false") {
        Value::Bool(raw.eq_ignore_ascii_case("true"))
    } else {
        Value::Text(raw.to_string())
    }
}

fn sql_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn hex_to_bytes(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(anyhow!("hex blob must have an even number of characters"));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    let chars = input.as_bytes().chunks_exact(2);
    for pair in chars {
        let byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
        bytes.push(byte);
    }
    Ok(bytes)
}

const BUILTIN_FUNCTIONS: &[(&str, &str)] = &[
    ("abs", "scalar"),
    ("acos", "scalar"),
    ("age", "scalar"),
    ("array_agg", "aggregate"),
    ("ascii", "scalar"),
    ("asin", "scalar"),
    ("atan", "scalar"),
    ("atan2", "scalar"),
    ("avg", "aggregate"),
    ("bool_and", "aggregate"),
    ("bool_or", "aggregate"),
    ("ceil", "scalar"),
    ("ceiling", "scalar"),
    ("char", "scalar"),
    ("chr", "scalar"),
    ("coalesce", "scalar"),
    ("concat", "scalar"),
    ("concat_ws", "scalar"),
    ("cos", "scalar"),
    ("cot", "scalar"),
    ("count", "aggregate"),
    ("cume_dist", "window"),
    ("current_date", "scalar"),
    ("current_time", "scalar"),
    ("current_timestamp", "scalar"),
    ("date", "scalar"),
    ("date_diff", "scalar"),
    ("date_part", "scalar"),
    ("date_trunc", "scalar"),
    ("datetime", "scalar"),
    ("degrees", "scalar"),
    ("dense_rank", "window"),
    ("exp", "scalar"),
    ("extract", "scalar"),
    ("first_value", "window"),
    ("floor", "scalar"),
    ("gen_random_uuid", "scalar"),
    ("greatest", "scalar"),
    ("group_concat", "aggregate/window"),
    ("hex", "scalar"),
    ("iif", "scalar"),
    ("initcap", "scalar"),
    ("instr", "scalar"),
    ("interval", "scalar"),
    ("json_array", "scalar"),
    ("json_array_length", "scalar"),
    ("json_extract", "scalar"),
    ("json_object", "scalar"),
    ("json_type", "scalar"),
    ("json_valid", "scalar"),
    ("lag", "window"),
    ("last_day", "scalar"),
    ("last_value", "window"),
    ("lead", "window"),
    ("least", "scalar"),
    ("left", "scalar"),
    ("length", "scalar"),
    ("ln", "scalar"),
    ("localtime", "scalar"),
    ("localtimestamp", "scalar"),
    ("log", "scalar"),
    ("lower", "scalar"),
    ("lpad", "scalar"),
    ("ltrim", "scalar"),
    ("make_date", "scalar"),
    ("make_timestamp", "scalar"),
    ("max", "aggregate"),
    ("md5", "scalar"),
    ("median", "aggregate"),
    ("min", "aggregate"),
    ("mod", "scalar"),
    ("next_day", "scalar"),
    ("nth_value", "window"),
    ("ntile", "window"),
    ("now", "scalar"),
    ("nullif", "scalar"),
    ("percent_rank", "window"),
    ("percentile_cont", "aggregate"),
    ("percentile_disc", "aggregate"),
    ("pi", "scalar"),
    ("position", "scalar"),
    ("pow", "scalar"),
    ("power", "scalar"),
    ("quote_ident", "scalar"),
    ("quote_literal", "scalar"),
    ("radians", "scalar"),
    ("random", "scalar"),
    ("rank", "window"),
    ("regexp_replace", "scalar"),
    ("repeat", "scalar"),
    ("replace", "scalar"),
    ("reverse", "scalar"),
    ("right", "scalar"),
    ("round", "scalar"),
    ("row_number", "window"),
    ("rpad", "scalar"),
    ("rtrim", "scalar"),
    ("sha256", "scalar"),
    ("sign", "scalar"),
    ("sin", "scalar"),
    ("split_part", "scalar"),
    ("sqrt", "scalar"),
    ("st_area", "spatial"),
    ("st_asbinary", "spatial"),
    ("st_asgeojson", "spatial"),
    ("st_astext", "spatial"),
    ("st_contains", "spatial"),
    ("st_distance", "spatial"),
    ("st_dwithin", "spatial"),
    ("st_equals", "spatial"),
    ("st_geogfromgeojson", "spatial"),
    ("st_geogfromtext", "spatial"),
    ("st_geogfromwkb", "spatial"),
    ("st_geogpoint", "spatial"),
    ("st_geogpointm", "spatial"),
    ("st_geogpointz", "spatial"),
    ("st_geogpointzm", "spatial"),
    ("st_geometrytype", "spatial"),
    ("st_geomfromgeojson", "spatial"),
    ("st_geomfromtext", "spatial"),
    ("st_geomfromwkb", "spatial"),
    ("st_intersects", "spatial"),
    ("st_isvalid", "spatial"),
    ("st_length", "spatial"),
    ("st_m", "spatial"),
    ("st_makepoint", "spatial"),
    ("st_point", "spatial"),
    ("st_pointm", "spatial"),
    ("st_pointz", "spatial"),
    ("st_pointzm", "spatial"),
    ("st_setsrid", "spatial"),
    ("st_srid", "spatial"),
    ("st_within", "spatial"),
    ("st_x", "spatial"),
    ("st_y", "spatial"),
    ("st_z", "spatial"),
    ("stddev", "aggregate"),
    ("stddev_pop", "aggregate"),
    ("stddev_samp", "aggregate"),
    ("strftime", "scalar"),
    ("string_agg", "aggregate/window"),
    ("string_to_array", "scalar"),
    ("substr", "scalar"),
    ("substring", "scalar"),
    ("sum", "aggregate"),
    ("tan", "scalar"),
    ("to_timestamp", "scalar"),
    ("total", "aggregate"),
    ("trim", "scalar"),
    ("upper", "scalar"),
    ("uuid_parse", "scalar"),
    ("uuid_to_string", "scalar"),
    ("var_pop", "aggregate"),
    ("var_samp", "aggregate"),
    ("variance", "aggregate"),
];

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
