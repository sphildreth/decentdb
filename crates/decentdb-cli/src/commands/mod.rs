use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand, ValueEnum};
use decentdb::{
    evict_shared_wal, BulkLoadOptions, Db, DbConfig, HeaderInfo, IndexVerification, QueryResult,
    StorageInfo, TableInfo, Value,
};

use crate::output::{
    render_error_json, render_exec_success_json, render_key_value_rows, render_rows,
    rows_from_query_result, stringify_value, OutputFormat,
};
use crate::repl::run_repl;

#[derive(Parser)]
#[command(name = "decentdb")]
#[command(about = "DecentDB Command Line Interface")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Version,
    Exec(ExecCommand),
    Repl(ReplCommand),
    Import(ImportCommand),
    Export(ExportCommand),
    BulkLoad(BulkLoadCommand),
    Checkpoint(DbCommand),
    SaveAs(SaveAsCommand),
    Info(InfoCommand),
    Describe(DescribeCommand),
    ListTables(ListTablesCommand),
    ListIndexes(ListIndexesCommand),
    ListViews(ListViewsCommand),
    Dump(DumpCommand),
    DumpHeader(DumpHeaderCommand),
    RebuildIndex(RebuildIndexCommand),
    RebuildIndexes(RebuildIndexesCommand),
    Completion(CompletionCommand),
    Stats(StatsCommand),
    Vacuum(VacuumCommand),
    VerifyHeader(VerifyHeaderCommand),
    VerifyIndex(VerifyIndexCommand),
}

#[derive(Clone, Debug, Parser)]
pub struct DbCommand {
    #[arg(long)]
    pub db: String,
}

#[derive(Clone, Debug, Parser)]
pub struct ExecCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub sql: Option<String>,
    #[arg(long = "params")]
    pub params: Vec<String>,
    #[arg(long = "openClose")]
    pub open_close: bool,
    #[arg(long)]
    pub timing: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
    #[arg(long = "noRows")]
    pub no_rows: bool,
    #[arg(long = "cachePages", default_value_t = 1024)]
    pub cache_pages: usize,
    #[arg(long = "cacheMb", default_value_t = 0)]
    pub cache_mb: usize,
    #[arg(long)]
    pub checkpoint: bool,
    #[arg(long = "dbInfo")]
    pub db_info: bool,
}

#[derive(Clone, Debug, Parser)]
pub struct ReplCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum DataFormat {
    Csv,
    Json,
}

#[derive(Clone, Debug, Parser)]
pub struct ImportCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: String,
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long, value_enum, default_value_t = DataFormat::Csv)]
    pub format: DataFormat,
    #[arg(long = "batchSize", default_value_t = 10_000)]
    pub batch_size: usize,
}

#[derive(Clone, Debug, Parser)]
pub struct ExportCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: String,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, value_enum, default_value_t = DataFormat::Csv)]
    pub format: DataFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BulkLoadCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: String,
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long = "batchSize", default_value_t = 10_000)]
    pub batch_size: usize,
    #[arg(long = "syncInterval", default_value_t = 10)]
    pub sync_interval: usize,
    #[arg(long = "disableIndexes", default_value_t = false)]
    pub disable_indexes: bool,
    #[arg(long = "noCheckpoint", default_value_t = false)]
    pub no_checkpoint: bool,
}

#[derive(Clone, Debug, Parser)]
pub struct SaveAsCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Clone, Debug, Parser)]
pub struct InfoCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long = "schema-summary")]
    pub schema_summary: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct DescribeCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct ListTablesCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct ListIndexesCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct ListViewsCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct DumpCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub output: Option<PathBuf>,
}

#[derive(Clone, Debug, Parser)]
pub struct DumpHeaderCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct RebuildIndexCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub index: String,
}

#[derive(Clone, Debug, Parser)]
pub struct RebuildIndexesCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ShellKind {
    Bash,
    Zsh,
}

#[derive(Clone, Debug, Parser)]
pub struct CompletionCommand {
    #[arg(long, value_enum, default_value_t = ShellKind::Bash)]
    pub shell: ShellKind,
}

#[derive(Clone, Debug, Parser)]
pub struct StatsCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct VacuumCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, default_value_t = false)]
    pub overwrite: bool,
}

#[derive(Clone, Debug, Parser)]
pub struct VerifyHeaderCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct VerifyIndexCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub index: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

pub fn run(cli: Cli) -> i32 {
    match dispatch(cli) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}

fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Version => {
            println!("DecentDB version: {}", decentdb::version());
        }
        Commands::Exec(command) => {
            if let Err(error) = run_exec(&command) {
                if command.format == OutputFormat::Json {
                    println!("{}", render_error_json(&error.to_string()));
                    return Ok(());
                }
                return Err(error);
            }
        }
        Commands::Repl(command) => {
            run_repl(open_db(&command.db, true, 0, 0)?, command.format)?;
        }
        Commands::Import(command) => run_import(command)?,
        Commands::Export(command) => run_export(command)?,
        Commands::BulkLoad(command) => run_bulk_load(command)?,
        Commands::Checkpoint(command) => {
            open_db(&command.db, false, 0, 0)?.checkpoint()?;
            println!("checkpoint complete");
        }
        Commands::SaveAs(command) => {
            open_db(&command.db, false, 0, 0)?.save_as(&command.output)?;
            println!("{}", command.output.display());
        }
        Commands::Info(command) => run_info(command)?,
        Commands::Describe(command) => run_describe(command)?,
        Commands::ListTables(command) => run_list_tables(command)?,
        Commands::ListIndexes(command) => run_list_indexes(command)?,
        Commands::ListViews(command) => run_list_views(command)?,
        Commands::Dump(command) => run_dump(command)?,
        Commands::DumpHeader(command) => run_dump_header(command)?,
        Commands::RebuildIndex(command) => {
            open_db(&command.db, false, 0, 0)?.rebuild_index(&command.index)?;
            println!("{}", command.index);
        }
        Commands::RebuildIndexes(command) => run_rebuild_indexes(command)?,
        Commands::Completion(command) => print!("{}", completion_script(command.shell)),
        Commands::Stats(command) => run_stats(command)?,
        Commands::Vacuum(command) => run_vacuum(command)?,
        Commands::VerifyHeader(command) => run_verify_header(command)?,
        Commands::VerifyIndex(command) => run_verify_index(command)?,
    }
    Ok(())
}

fn run_exec(command: &ExecCommand) -> Result<()> {
    let db = open_db(&command.db, true, command.cache_pages, command.cache_mb)?;

    if command.db_info {
        print_storage_info(command.format, &db.storage_info()?);
        return Ok(());
    }

    if command.open_close {
        return Ok(());
    }

    if command.sql.is_none() && command.checkpoint {
        db.checkpoint()?;
        match command.format {
            OutputFormat::Json => println!("{}", render_exec_success_json(&[], 0.0, true)),
            _ => println!("checkpoint complete"),
        }
        return Ok(());
    }

    let sql = command.sql.as_deref().ok_or_else(|| {
        anyhow!("--sql is required unless --openClose, --dbInfo, or --checkpoint is used")
    })?;
    let params = command
        .params
        .iter()
        .map(|param| parse_param(param))
        .collect::<Result<Vec<_>>>()?;
    let started = Instant::now();
    let mut results = db.execute_batch_with_params(sql, &params)?;
    if command.no_rows && results.len() == 1 {
        let row_count = results[0].rows().len();
        results = vec![QueryResult::with_affected_rows(row_count as u64)];
    }
    if command.checkpoint {
        db.checkpoint()?;
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    match command.format {
        OutputFormat::Json => {
            println!(
                "{}",
                render_exec_success_json(&results, elapsed_ms, command.checkpoint)
            );
        }
        OutputFormat::Csv | OutputFormat::Table => {
            for (index, result) in results.iter().enumerate() {
                if index > 0 {
                    println!();
                }
                let rows = rows_from_query_result(result);
                println!(
                    "{}",
                    render_rows(
                        command.format,
                        result.columns(),
                        &rows,
                        !result.columns().is_empty()
                    )
                );
            }
        }
    }
    Ok(())
}

fn run_import(command: ImportCommand) -> Result<()> {
    if command.format != DataFormat::Csv {
        return Err(anyhow!("JSON import is not supported by the Rust CLI yet"));
    }
    let db = open_db(&command.db, true, 0, 0)?;
    let (columns, rows) = parse_csv_file(&command.input)?;
    let column_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
    db.bulk_load_rows(
        &command.table,
        &column_refs,
        &rows,
        BulkLoadOptions {
            batch_size: command.batch_size,
            ..BulkLoadOptions::default()
        },
    )?;
    println!("{}", rows.len());
    Ok(())
}

fn run_export(command: ExportCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let result = db.execute(&format!("SELECT * FROM {}", sql_identifier(&command.table)))?;
    let rows = rows_from_query_result(&result);
    let output = match command.format {
        DataFormat::Csv => render_rows(OutputFormat::Csv, result.columns(), &rows, true),
        DataFormat::Json => render_rows(OutputFormat::Json, result.columns(), &rows, false),
    };
    fs::write(&command.output, format!("{output}\n"))?;
    Ok(())
}

fn run_bulk_load(command: BulkLoadCommand) -> Result<()> {
    let db = open_db(&command.db, true, 0, 0)?;
    let (columns, rows) = parse_csv_file(&command.input)?;
    let column_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let inserted = db.bulk_load_rows(
        &command.table,
        &column_refs,
        &rows,
        BulkLoadOptions {
            batch_size: command.batch_size,
            sync_interval: command.sync_interval,
            disable_indexes: command.disable_indexes,
            checkpoint_on_complete: !command.no_checkpoint,
        },
    )?;
    println!("{inserted}");
    Ok(())
}

fn run_info(command: InfoCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let storage = db.storage_info()?;
    if command.schema_summary {
        let tables = db.list_tables()?;
        let indexes = db.list_indexes()?;
        let views = db.list_views()?;
        let rows = vec![
            ("path".to_string(), storage.path.display().to_string()),
            ("page_size".to_string(), storage.page_size.to_string()),
            ("page_count".to_string(), storage.page_count.to_string()),
            (
                "schema_cookie".to_string(),
                storage.schema_cookie.to_string(),
            ),
            ("table_count".to_string(), tables.len().to_string()),
            ("index_count".to_string(), indexes.len().to_string()),
            ("view_count".to_string(), views.len().to_string()),
        ];
        println!("{}", render_key_value_rows(command.format, &rows));
    } else {
        print_storage_info(command.format, &storage);
    }
    Ok(())
}

fn run_describe(command: DescribeCommand) -> Result<()> {
    let table = open_db(&command.db, false, 0, 0)?.describe_table(&command.table)?;
    let rows = table
        .columns
        .iter()
        .map(|column| {
            vec![
                column.name.clone(),
                column.column_type.clone(),
                column.nullable.to_string(),
                column.primary_key.to_string(),
                column.unique.to_string(),
                column.auto_increment.to_string(),
                column.default_sql.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    let columns = vec![
        "name".to_string(),
        "type".to_string(),
        "nullable".to_string(),
        "primary_key".to_string(),
        "unique".to_string(),
        "auto_increment".to_string(),
        "default".to_string(),
    ];
    println!("{}", render_rows(command.format, &columns, &rows, true));
    Ok(())
}

fn run_list_tables(command: ListTablesCommand) -> Result<()> {
    let tables = open_db(&command.db, false, 0, 0)?.list_tables()?;
    let rows = tables
        .iter()
        .map(|table| vec![table.name.clone(), table.row_count.to_string()])
        .collect::<Vec<_>>();
    let columns = vec!["name".to_string(), "row_count".to_string()];
    println!("{}", render_rows(command.format, &columns, &rows, true));
    Ok(())
}

fn run_list_indexes(command: ListIndexesCommand) -> Result<()> {
    let mut indexes = open_db(&command.db, false, 0, 0)?.list_indexes()?;
    if let Some(table) = &command.table {
        indexes.retain(|index| &index.table_name == table);
    }
    let rows = indexes
        .iter()
        .map(|index| {
            vec![
                index.name.clone(),
                index.table_name.clone(),
                index.kind.clone(),
                index.unique.to_string(),
                index.fresh.to_string(),
                index.columns.join(", "),
                index.predicate_sql.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    let columns = vec![
        "name".to_string(),
        "table".to_string(),
        "kind".to_string(),
        "unique".to_string(),
        "fresh".to_string(),
        "columns".to_string(),
        "predicate".to_string(),
    ];
    println!("{}", render_rows(command.format, &columns, &rows, true));
    Ok(())
}

fn run_list_views(command: ListViewsCommand) -> Result<()> {
    let views = open_db(&command.db, false, 0, 0)?.list_views()?;
    let rows = views
        .iter()
        .map(|view| {
            vec![
                view.name.clone(),
                view.column_names.join(", "),
                view.dependencies.join(", "),
            ]
        })
        .collect::<Vec<_>>();
    let columns = vec![
        "name".to_string(),
        "columns".to_string(),
        "dependencies".to_string(),
    ];
    println!("{}", render_rows(command.format, &columns, &rows, true));
    Ok(())
}

fn run_dump(command: DumpCommand) -> Result<()> {
    let dump = open_db(&command.db, false, 0, 0)?.dump_sql()?;
    if let Some(path) = command.output {
        fs::write(path, dump)?;
    } else {
        println!("{dump}");
    }
    Ok(())
}

fn run_dump_header(command: DumpHeaderCommand) -> Result<()> {
    let header = open_db(&command.db, false, 0, 0)?.header_info()?;
    print_header_info(command.format, &header);
    Ok(())
}

fn run_rebuild_indexes(command: RebuildIndexesCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    if let Some(table) = command.table {
        for index in db
            .list_indexes()?
            .into_iter()
            .filter(|index| index.table_name == table)
        {
            db.rebuild_index(&index.name)?;
        }
    } else {
        db.rebuild_indexes()?;
    }
    println!("ok");
    Ok(())
}

fn run_stats(command: StatsCommand) -> Result<()> {
    let storage = open_db(&command.db, false, 0, 0)?.storage_info()?;
    let rows = vec![
        ("page_size".to_string(), storage.page_size.to_string()),
        ("page_count".to_string(), storage.page_count.to_string()),
        (
            "physical_bytes".to_string(),
            (u64::from(storage.page_size) * u64::from(storage.page_count)).to_string(),
        ),
        (
            "cache_size_mb".to_string(),
            storage.cache_size_mb.to_string(),
        ),
    ];
    println!("{}", render_key_value_rows(command.format, &rows));
    Ok(())
}

fn run_vacuum(command: VacuumCommand) -> Result<()> {
    if command.overwrite && command.output.exists() {
        fs::remove_file(&command.output)?;
    }
    let db = open_db(&command.db, false, 0, 0)?;
    db.checkpoint()?;
    db.save_as(&command.output)?;
    evict_shared_wal(&command.output)?;
    println!("{}", command.output.display());
    Ok(())
}

fn run_verify_header(command: VerifyHeaderCommand) -> Result<()> {
    let header = open_db(&command.db, false, 0, 0)?.header_info()?;
    print_header_info(command.format, &header);
    Ok(())
}

fn run_verify_index(command: VerifyIndexCommand) -> Result<()> {
    let verification = open_db(&command.db, false, 0, 0)?.verify_index(&command.index)?;
    print_index_verification(command.format, &verification);
    Ok(())
}

fn print_storage_info(format: OutputFormat, storage: &StorageInfo) {
    let rows = vec![
        ("path".to_string(), storage.path.display().to_string()),
        (
            "wal_path".to_string(),
            storage.wal_path.display().to_string(),
        ),
        ("page_size".to_string(), storage.page_size.to_string()),
        (
            "cache_size_mb".to_string(),
            storage.cache_size_mb.to_string(),
        ),
        ("page_count".to_string(), storage.page_count.to_string()),
        (
            "schema_cookie".to_string(),
            storage.schema_cookie.to_string(),
        ),
        ("wal_end_lsn".to_string(), storage.wal_end_lsn.to_string()),
        (
            "wal_file_size".to_string(),
            storage.wal_file_size.to_string(),
        ),
        (
            "last_checkpoint_lsn".to_string(),
            storage.last_checkpoint_lsn.to_string(),
        ),
        (
            "active_readers".to_string(),
            storage.active_readers.to_string(),
        ),
        ("wal_versions".to_string(), storage.wal_versions.to_string()),
        (
            "warning_count".to_string(),
            storage.warning_count.to_string(),
        ),
        ("shared_wal".to_string(), storage.shared_wal.to_string()),
    ];
    println!("{}", render_key_value_rows(format, &rows));
}

fn print_header_info(format: OutputFormat, header: &HeaderInfo) {
    let rows = vec![
        ("magic_hex".to_string(), header.magic_hex.clone()),
        (
            "format_version".to_string(),
            header.format_version.to_string(),
        ),
        ("page_size".to_string(), header.page_size.to_string()),
        (
            "header_checksum".to_string(),
            header.header_checksum.to_string(),
        ),
        (
            "schema_cookie".to_string(),
            header.schema_cookie.to_string(),
        ),
        (
            "catalog_root_page_id".to_string(),
            header.catalog_root_page_id.to_string(),
        ),
        (
            "freelist_root_page_id".to_string(),
            header.freelist_root_page_id.to_string(),
        ),
        (
            "freelist_head_page_id".to_string(),
            header.freelist_head_page_id.to_string(),
        ),
        (
            "freelist_page_count".to_string(),
            header.freelist_page_count.to_string(),
        ),
        (
            "last_checkpoint_lsn".to_string(),
            header.last_checkpoint_lsn.to_string(),
        ),
    ];
    println!("{}", render_key_value_rows(format, &rows));
}

fn print_index_verification(format: OutputFormat, verification: &IndexVerification) {
    let rows = vec![
        ("name".to_string(), verification.name.clone()),
        ("valid".to_string(), verification.valid.to_string()),
        (
            "expected_entries".to_string(),
            verification.expected_entries.to_string(),
        ),
        (
            "actual_entries".to_string(),
            verification.actual_entries.to_string(),
        ),
    ];
    println!("{}", render_key_value_rows(format, &rows));
}

fn open_db(db: &str, create_if_missing: bool, cache_pages: usize, cache_mb: usize) -> Result<Db> {
    let mut config = DbConfig::default();
    if cache_mb > 0 {
        config.cache_size_mb = cache_mb;
    } else if cache_pages > 0 {
        config.cache_size_mb = ((cache_pages * 4096) / (1024 * 1024)).max(1);
    }
    if create_if_missing {
        Ok(Db::open_or_create(db, config)?)
    } else {
        Ok(Db::open(db, config)?)
    }
}

fn parse_param(raw: &str) -> Result<Value> {
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

fn completion_script(shell: ShellKind) -> &'static str {
    match shell {
        ShellKind::Bash => {
            r#"_decentdb_complete() {
  local commands="version exec repl import export bulk-load checkpoint save-as info describe list-tables list-indexes list-views dump dump-header rebuild-index rebuild-indexes completion stats vacuum verify-header verify-index"
  COMPREPLY=( $(compgen -W "$commands" -- "${COMP_WORDS[1]}") )
}
complete -F _decentdb_complete decentdb
"#
        }
        ShellKind::Zsh => {
            r#"#compdef decentdb
_decentdb() {
  local -a commands
  commands=(
    version
    exec
    repl
    import
    export
    bulk-load
    checkpoint
    save-as
    info
    describe
    list-tables
    list-indexes
    list-views
    dump
    dump-header
    rebuild-index
    rebuild-indexes
    completion
    stats
    vacuum
    verify-header
    verify-index
  )
  _describe 'command' commands
}
_decentdb "$@"
"#
        }
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

#[allow(dead_code)]
fn _table_summary(table: &TableInfo) -> Vec<String> {
    vec![
        table.name.clone(),
        table.row_count.to_string(),
        table
            .columns
            .iter()
            .map(|column| format!("{} {}", column.name, column.column_type))
            .collect::<Vec<_>>()
            .join(", "),
        table
            .columns
            .iter()
            .map(|column| stringify_value(&Value::Text(column.name.clone())))
            .collect::<Vec<_>>()
            .join(", "),
    ]
}
