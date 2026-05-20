use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use decentdb::{Db, DbConfig, StorageInfo, Value};

use crate::output::stringify_value;

pub struct ServeCommandOptions {
    pub db: String,
    pub host: String,
    pub port: u16,
    pub bind: Option<String>,
    pub read_only: bool,
    pub open: bool,
    pub max_result_rows: usize,
    pub query_timeout: String,
    pub max_body_size: String,
    pub max_concurrent_requests: usize,
    pub busy_timeout: String,
    pub token_env: Option<String>,
    pub show_token: bool,
    pub no_auth: bool,
    pub cors_origin: Option<String>,
    pub log_format: String,
}

pub fn run_serve(command: ServeCommandOptions) -> Result<()> {
    validate_limits(&command)?;

    let bind_host = command
        .bind
        .as_deref()
        .map(host_from_bind)
        .unwrap_or_else(|| command.host.clone());
    let localhost_bind = is_localhost_host(&bind_host);

    if command.no_auth && !localhost_bind {
        return Err(anyhow!("--no-auth is only allowed with localhost binding"));
    }
    if !localhost_bind && command.token_env.is_none() {
        return Err(anyhow!(
            "--token-env is required when binding decentdb serve to a non-localhost host"
        ));
    }
    if matches!(command.cors_origin.as_deref(), Some("*")) {
        return Err(anyhow!(
            "--cors-origin '*' is not accepted; configure an explicit origin"
        ));
    }

    let db = if command.read_only {
        Db::open(&command.db, DbConfig::default())?
    } else {
        Db::open_or_create(&command.db, DbConfig::default())?
    };
    let query_timeout = parse_duration(&command.query_timeout, "--query-timeout")?;
    let busy_timeout = parse_duration(&command.busy_timeout, "--busy-timeout")?;
    let max_body_size = parse_byte_size(&command.max_body_size)?;
    let log_format = LogFormat::parse(&command.log_format)?;
    let auth = if command.no_auth {
        AuthMode::Disabled
    } else {
        AuthMode::Bearer(resolve_token(
            command.token_env.as_deref(),
            &command.db,
            &bind_host,
        )?)
    };

    let bind_addr = command
        .bind
        .clone()
        .unwrap_or_else(|| format!("{}:{}", command.host, command.port));
    let listener = TcpListener::bind(&bind_addr)?;
    let bound_addr = listener.local_addr()?;

    let ui_url = local_url(&bind_host, bound_addr.port(), "");
    let api_url = local_url(&bind_host, bound_addr.port(), "/api/v1");

    print_startup(
        &db,
        command.read_only,
        &ui_url,
        &api_url,
        &bound_addr.to_string(),
        &auth,
        command.show_token,
    )?;

    if command.open {
        let _ = open_browser(&ui_url);
    }

    let state = Arc::new(ServeState {
        db,
        read_only: command.read_only,
        bind: bound_addr.to_string(),
        max_result_rows: command.max_result_rows,
        query_timeout,
        max_body_size,
        max_concurrent_requests: command.max_concurrent_requests,
        busy_timeout,
        auth,
        cors_origin: command.cors_origin,
        log_format,
        bootstrap_ui_token: localhost_bind,
        active_requests: AtomicUsize::new(0),
        startup_instant: Instant::now(),
        startup_timestamp: SystemTime::now(),
    });

    for stream in listener.incoming() {
        let mut stream = stream?;
        let active = state.active_requests.fetch_add(1, Ordering::SeqCst);
        if active >= state.max_concurrent_requests {
            state.active_requests.fetch_sub(1, Ordering::SeqCst);
            write_json_response(
                &mut stream,
                503,
                api_error("SERVER_BUSY", "too many concurrent requests"),
                &state,
            )?;
            continue;
        }

        let state = Arc::clone(&state);
        thread::spawn(move || {
            let log = match handle_connection(&state, stream) {
                Ok(log) => log,
                Err(error) => RequestLog {
                    method: "-".to_string(),
                    path: "-".to_string(),
                    status: 500,
                    elapsed_ms: 0.0,
                    error: Some(error.to_string()),
                },
            };
            log_request(&state, &log);
            state.active_requests.fetch_sub(1, Ordering::SeqCst);
        });
    }

    Ok(())
}

struct ServeState {
    db: Db,
    read_only: bool,
    bind: String,
    max_result_rows: usize,
    query_timeout: Duration,
    max_body_size: usize,
    max_concurrent_requests: usize,
    busy_timeout: Duration,
    auth: AuthMode,
    cors_origin: Option<String>,
    log_format: LogFormat,
    bootstrap_ui_token: bool,
    active_requests: AtomicUsize,
    startup_instant: Instant,
    startup_timestamp: SystemTime,
}

enum AuthMode {
    Disabled,
    Bearer(String),
}

enum LogFormat {
    Text,
    Json,
}

impl LogFormat {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(anyhow!(
                "unsupported --log-format {other}; expected text or json"
            )),
        }
    }
}

struct RequestLog {
    method: String,
    path: String,
    status: u16,
    elapsed_ms: f64,
    error: Option<String>,
}

struct RequestContext {
    method: String,
    path: String,
    query: Option<String>,
    authorization: Option<String>,
    body: Vec<u8>,
}

fn handle_connection(state: &ServeState, stream: TcpStream) -> Result<RequestLog> {
    let started = Instant::now();
    let mut reader = BufReader::new(stream);

    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    if request_line.trim().is_empty() {
        return Ok(RequestLog {
            method: "-".to_string(),
            path: "-".to_string(),
            status: 400,
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
            error: Some("empty request".to_string()),
        });
    }

    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow!("malformed request line"))?
        .to_string();
    let target = parts
        .next()
        .ok_or_else(|| anyhow!("malformed request line"))?
        .to_string();
    let _version = parts
        .next()
        .ok_or_else(|| anyhow!("malformed request line"))?;

    let mut content_length: usize = 0;
    let mut authorization: Option<String> = None;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().to_string();
            if name == "content-length" {
                content_length = value
                    .parse::<usize>()
                    .map_err(|_| anyhow!("invalid Content-Length header"))?;
            } else if name == "authorization" {
                authorization = Some(value);
            }
        }
    }

    let (path, query) = split_request_target(&target);
    if content_length > state.max_body_size {
        let mut stream = reader.into_inner();
        let status = write_json_response(
            &mut stream,
            413,
            api_error("REQUEST_TOO_LARGE", "request body exceeds limit"),
            state,
        )?;
        return Ok(RequestLog {
            method,
            path: path.to_string(),
            status,
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
            error: None,
        });
    }

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }
    let mut stream = reader.into_inner();

    let context = RequestContext {
        method,
        path: path.to_string(),
        query: query.map(ToString::to_string),
        authorization,
        body,
    };

    let status = route_request(&mut stream, state, &context).unwrap_or_else(|error| {
        let _ = write_json_response(
            &mut stream,
            500,
            api_error("INTERNAL_ERROR", &error.to_string()),
            state,
        );
        500
    });

    Ok(RequestLog {
        method: context.method,
        path: context.path,
        status,
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        error: None,
    })
}

fn route_request(
    stream: &mut TcpStream,
    state: &ServeState,
    context: &RequestContext,
) -> Result<u16> {
    if context.method == "OPTIONS" {
        return write_empty_response(stream, 204, state);
    }

    if context.path.starts_with("/api/v1") {
        if let Err(error) = authorize(context.authorization.as_deref(), &state.auth) {
            return write_json_response(
                stream,
                error.status,
                api_error(error.code, error.message),
                state,
            );
        }
    }

    match (context.method.as_str(), context.path.as_str()) {
        ("GET", "/") => write_static_page(stream, state),
        ("GET", "/assets/console.css") => {
            write_static_asset(stream, "text/css; charset=utf-8", CONSOLE_CSS, state)
        }
        ("GET", "/assets/console.js") => write_static_asset(
            stream,
            "application/javascript; charset=utf-8",
            CONSOLE_JS,
            state,
        ),
        ("GET", "/healthz") => {
            write_json_response(stream, 200, serde_json::json!({"ok": true}), state)
        }
        ("GET", "/readyz") => write_json_response(
            stream,
            200,
            serde_json::json!({"ok": true, "ready": true}),
            state,
        ),
        ("GET", "/api/v1") | ("GET", "/api/v1/") => handle_api_docs(stream, state),
        ("GET", "/api/v1/info") => handle_info(stream, state),
        ("GET", "/api/v1/schema") => handle_schema(stream, state),
        ("GET", "/api/v1/tables") => handle_tables(stream, state),
        ("GET", "/api/v1/indexes") => handle_indexes(stream, state),
        ("GET", "/api/v1/views") => handle_views(stream, state),
        ("GET", "/api/v1/triggers") => handle_triggers(stream, state),
        ("POST", "/api/v1/sql") => handle_sql(stream, state, context, false),
        ("POST", "/api/v1/explain") => handle_sql(stream, state, context, true),
        _ if context.method == "GET" && context.path.starts_with("/api/v1/tables/") => {
            let name = context.path.trim_start_matches("/api/v1/tables/");
            handle_table_detail(stream, state, name)
        }
        _ => write_json_response(
            stream,
            404,
            api_error("NOT_FOUND", "resource not found"),
            state,
        ),
    }
}

struct AuthError {
    status: u16,
    code: &'static str,
    message: &'static str,
}

fn authorize(header: Option<&str>, auth: &AuthMode) -> std::result::Result<(), AuthError> {
    match auth {
        AuthMode::Disabled => Ok(()),
        AuthMode::Bearer(expected) => match header {
            None => Err(AuthError {
                status: 401,
                code: "AUTH_REQUIRED",
                message: "missing bearer token",
            }),
            Some(value) if value == format!("Bearer {expected}") => Ok(()),
            Some(_) => Err(AuthError {
                status: 401,
                code: "AUTH_INVALID",
                message: "invalid bearer token",
            }),
        },
    }
}

fn handle_api_docs(stream: &mut TcpStream, state: &ServeState) -> Result<u16> {
    write_json_response(
        stream,
        200,
        serde_json::json!({
            "name": "DecentDB local HTTP API",
            "version": "v1",
            "auth": match &state.auth {
                AuthMode::Disabled => "disabled",
                AuthMode::Bearer(_) => "bearer",
            },
            "routes": [
                {"method": "GET", "path": "/healthz"},
                {"method": "GET", "path": "/readyz"},
                {"method": "GET", "path": "/api/v1/info"},
                {"method": "GET", "path": "/api/v1/schema"},
                {"method": "GET", "path": "/api/v1/tables"},
                {"method": "GET", "path": "/api/v1/tables/{tableName}"},
                {"method": "GET", "path": "/api/v1/indexes"},
                {"method": "GET", "path": "/api/v1/views"},
                {"method": "GET", "path": "/api/v1/triggers"},
                {"method": "POST", "path": "/api/v1/sql"},
                {"method": "POST", "path": "/api/v1/explain"}
            ]
        }),
        state,
    )
}

fn handle_info(stream: &mut TcpStream, state: &ServeState) -> Result<u16> {
    let storage = state.db.storage_info()?;
    let file_size = file_size_bytes(&storage).unwrap_or_default();
    let file_name = storage
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown")
        .to_string();
    let started_at = state
        .startup_timestamp
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .unwrap_or_default();

    write_json_response(
        stream,
        200,
        serde_json::json!({
            "database": {
                "fileName": file_name,
                "path": storage.path.to_string_lossy(),
                "sizeBytes": file_size,
                "readOnly": state.read_only,
            },
            "server": {
                "version": decentdb::version(),
                "startedAtUnix": started_at,
                "uptimeSeconds": state.startup_instant.elapsed().as_secs(),
                "bind": state.bind,
                "maxResultRows": state.max_result_rows,
                "queryTimeoutMs": state.query_timeout.as_millis(),
                "maxBodySize": state.max_body_size,
                "maxConcurrentRequests": state.max_concurrent_requests,
                "busyTimeoutMs": state.busy_timeout.as_millis(),
                "corsEnabled": state.cors_origin.is_some(),
            },
            "engine": {
                "version": decentdb::version(),
                "fileFormatVersion": storage.format_version,
                "pageSize": storage.page_size,
                "pageCount": storage.page_count,
                "walEndLsn": storage.wal_end_lsn,
                "walFileSize": storage.wal_file_size,
                "lastCheckpointLsn": storage.last_checkpoint_lsn,
            },
        }),
        state,
    )
}

fn handle_schema(stream: &mut TcpStream, state: &ServeState) -> Result<u16> {
    let snapshot = state.db.get_schema_snapshot()?;
    write_json_response(
        stream,
        200,
        serde_json::json!({
            "snapshotVersion": snapshot.snapshot_version,
            "schemaCookie": snapshot.schema_cookie,
            "tables": snapshot.tables,
            "views": snapshot.views,
            "indexes": snapshot.indexes,
            "triggers": snapshot.triggers,
        }),
        state,
    )
}

fn handle_tables(stream: &mut TcpStream, state: &ServeState) -> Result<u16> {
    write_json_response(
        stream,
        200,
        serde_json::json!({"tables": state.db.list_tables()?}),
        state,
    )
}

fn handle_indexes(stream: &mut TcpStream, state: &ServeState) -> Result<u16> {
    write_json_response(
        stream,
        200,
        serde_json::json!({"indexes": state.db.list_indexes()?}),
        state,
    )
}

fn handle_views(stream: &mut TcpStream, state: &ServeState) -> Result<u16> {
    let snapshot = state.db.get_schema_snapshot()?;
    write_json_response(
        stream,
        200,
        serde_json::json!({"views": snapshot.views}),
        state,
    )
}

fn handle_triggers(stream: &mut TcpStream, state: &ServeState) -> Result<u16> {
    let snapshot = state.db.get_schema_snapshot()?;
    write_json_response(
        stream,
        200,
        serde_json::json!({"triggers": snapshot.triggers}),
        state,
    )
}

fn handle_table_detail(stream: &mut TcpStream, state: &ServeState, encoded: &str) -> Result<u16> {
    let requested = percent_decode(encoded)?;
    let table_name = resolve_table_name(&state.db, &requested)?;
    let table = state.db.describe_table(&table_name)?;
    let ddl = state.db.table_ddl(&table_name).ok();
    let indexes = state
        .db
        .list_indexes()?
        .into_iter()
        .filter(|index| index.table_name.eq_ignore_ascii_case(&table_name))
        .collect::<Vec<_>>();
    let triggers = state
        .db
        .get_schema_snapshot()?
        .triggers
        .into_iter()
        .filter(|trigger| trigger.target_name.eq_ignore_ascii_case(&table_name))
        .collect::<Vec<_>>();

    write_json_response(
        stream,
        200,
        serde_json::json!({
            "table": table,
            "ddl": ddl,
            "indexes": indexes,
            "triggers": triggers,
        }),
        state,
    )
}

fn handle_sql(
    stream: &mut TcpStream,
    state: &ServeState,
    context: &RequestContext,
    explain: bool,
) -> Result<u16> {
    let payload = match serde_json::from_slice::<serde_json::Value>(&context.body) {
        Ok(payload) => payload,
        Err(error) => {
            return write_json_response(
                stream,
                400,
                api_error("INVALID_REQUEST", &format!("invalid JSON body: {error}")),
                state,
            );
        }
    };

    let sql = match payload.get("sql").and_then(|value| value.as_str()) {
        Some(sql) if !sql.trim().is_empty() => sql.trim(),
        _ => {
            return write_json_response(
                stream,
                400,
                api_error("INVALID_REQUEST", "sql is required"),
                state,
            );
        }
    };

    let params = match payload.get("params") {
        None => Vec::new(),
        Some(values) => match values.as_array() {
            Some(values) => values
                .iter()
                .map(json_value_to_db_value)
                .collect::<Result<Vec<_>>>()?,
            None => {
                return write_json_response(
                    stream,
                    400,
                    api_error("INVALID_REQUEST", "params must be an array"),
                    state,
                );
            }
        },
    };

    let request_read_only = payload
        .get("readonly")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let sql = if explain && !sql.trim_start().to_ascii_uppercase().starts_with("EXPLAIN") {
        format!("EXPLAIN {sql}")
    } else {
        sql.to_string()
    };

    let statements = split_sql_batch(&sql);
    if statements.is_empty() {
        return write_json_response(
            stream,
            400,
            api_error("INVALID_REQUEST", "sql must contain at least one statement"),
            state,
        );
    }

    let mut contracts = Vec::with_capacity(statements.len());
    for statement in &statements {
        let contract = match state.db.describe_query_contract(statement) {
            Ok(contract) => contract,
            Err(error) => {
                return write_json_response(
                    stream,
                    400,
                    api_error("SQL_SYNTAX_ERROR", &error.to_string()),
                    state,
                );
            }
        };
        if (state.read_only || request_read_only) && !contract.read_only {
            return write_json_response(
                stream,
                400,
                api_error("READ_ONLY", "read-only mode does not allow mutating SQL"),
                state,
            );
        }
        contracts.push(contract);
    }

    let started = Instant::now();
    let results = match state.db.execute_batch_with_params(&sql, &params) {
        Ok(results) => results,
        Err(error) => {
            return write_json_response(
                stream,
                400,
                api_error("INVALID_REQUEST", &error.to_string()),
                state,
            );
        }
    };
    let elapsed = started.elapsed();
    if elapsed > state.query_timeout {
        return write_json_response(
            stream,
            408,
            api_error("QUERY_TIMEOUT", "query exceeded timeout"),
            state,
        );
    }

    if query_has(&context.query, "format", "ndjson") {
        return write_ndjson_results(stream, state, &results, &contracts, elapsed);
    }

    let (rendered_results, truncated_any) = render_results(state, &results, &contracts);
    write_json_response(
        stream,
        200,
        serde_json::json!({
            "ok": true,
            "elapsedMs": elapsed.as_secs_f64() * 1000.0,
            "results": rendered_results,
            "truncated": truncated_any,
        }),
        state,
    )
}

fn render_results(
    state: &ServeState,
    results: &[decentdb::QueryResult],
    contracts: &[decentdb::QueryContract],
) -> (Vec<serde_json::Value>, bool) {
    let mut truncated_any = false;
    let mut rendered_results = Vec::with_capacity(results.len());

    for (index, result) in results.iter().enumerate() {
        let mut rows = result
            .rows()
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(db_value_to_json)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let total_row_count = rows.len();
        let mut truncated = false;
        if rows.len() > state.max_result_rows {
            rows.truncate(state.max_result_rows);
            truncated = true;
            truncated_any = true;
        }

        let columns = result
            .columns()
            .iter()
            .enumerate()
            .map(|(column_index, name)| {
                let type_name = contracts
                    .get(index)
                    .and_then(|contract| contract.result_columns.get(column_index))
                    .and_then(|column| column.type_name.as_deref())
                    .unwrap_or("UNKNOWN");
                serde_json::json!({"name": name, "type": type_name})
            })
            .collect::<Vec<_>>();

        rendered_results.push(serde_json::json!({
            "columns": columns,
            "rows": rows,
            "rowCount": total_row_count.min(state.max_result_rows),
            "totalRowCount": total_row_count,
            "rowsAffected": result.affected_rows(),
            "explainLines": result.explain_lines(),
            "truncated": truncated,
            "limit": state.max_result_rows,
        }));
    }

    (rendered_results, truncated_any)
}

fn write_ndjson_results(
    stream: &mut TcpStream,
    state: &ServeState,
    results: &[decentdb::QueryResult],
    contracts: &[decentdb::QueryContract],
    elapsed: Duration,
) -> Result<u16> {
    let (rendered_results, truncated) = render_results(state, results, contracts);
    let mut body = String::new();
    body.push_str(
        &serde_json::json!({
            "type": "meta",
            "elapsedMs": elapsed.as_secs_f64() * 1000.0,
            "truncated": truncated,
        })
        .to_string(),
    );
    body.push('\n');
    for (result_index, result) in rendered_results.iter().enumerate() {
        body.push_str(
            &serde_json::json!({
                "type": "result",
                "index": result_index,
                "columns": result["columns"],
                "rowCount": result["rowCount"],
                "totalRowCount": result["totalRowCount"],
                "rowsAffected": result["rowsAffected"],
                "truncated": result["truncated"],
            })
            .to_string(),
        );
        body.push('\n');
        if let Some(rows) = result["rows"].as_array() {
            for row in rows {
                body.push_str(
                    &serde_json::json!({
                        "type": "row",
                        "index": result_index,
                        "row": row,
                    })
                    .to_string(),
                );
                body.push('\n');
            }
        }
    }
    write_response(
        stream,
        200,
        "application/x-ndjson; charset=utf-8",
        body.as_bytes(),
        state,
    )
}

fn json_value_to_db_value(value: &serde_json::Value) -> Result<Value> {
    Ok(match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Bool(*value),
        serde_json::Value::Number(number) if number.is_i64() => {
            Value::Int64(number.as_i64().unwrap_or(0))
        }
        serde_json::Value::Number(number) if number.is_u64() => Value::Int64(
            i64::try_from(
                number
                    .as_u64()
                    .ok_or_else(|| anyhow!("invalid unsigned number"))?,
            )
            .map_err(|_| anyhow!("integer parameter out of range"))?,
        ),
        serde_json::Value::Number(number) => {
            Value::Float64(number.as_f64().ok_or_else(|| anyhow!("invalid number"))?)
        }
        serde_json::Value::String(value) => Value::Text(value.clone()),
        other => Value::Text(other.to_string()),
    })
}

fn db_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Int64(value) => serde_json::Value::from(*value),
        Value::Float64(value) => serde_json::json!(*value),
        Value::Bool(value) => serde_json::Value::from(*value),
        Value::Text(value) => serde_json::Value::from(value.clone()),
        _ => serde_json::Value::from(stringify_value(value)),
    }
}

fn file_size_bytes(storage: &StorageInfo) -> Result<u64> {
    Ok(std::fs::metadata(&storage.path)?.len())
}

fn write_json_response(
    stream: &mut TcpStream,
    status: u16,
    body: serde_json::Value,
    state: &ServeState,
) -> Result<u16> {
    let body = serde_json::to_vec(&body)?;
    write_response(
        stream,
        status,
        "application/json; charset=utf-8",
        &body,
        state,
    )
}

fn write_static_page(stream: &mut TcpStream, state: &ServeState) -> Result<u16> {
    let token = match &state.auth {
        AuthMode::Disabled => "null".to_string(),
        AuthMode::Bearer(token) if state.bootstrap_ui_token => serde_json::to_string(token)?,
        AuthMode::Bearer(_) => "null".to_string(),
    };
    let bootstrap = format!(
        "window.DECENTDB_BOOTSTRAP_TOKEN={token};window.DECENTDB_BOOTSTRAP={{readOnly:{},maxRows:{}}};",
        state.read_only, state.max_result_rows
    );
    let html = CONSOLE_HTML.replace("__DECENTDB_BOOTSTRAP__", &bootstrap);
    write_response(
        stream,
        200,
        "text/html; charset=utf-8",
        html.as_bytes(),
        state,
    )
}

fn write_static_asset(
    stream: &mut TcpStream,
    content_type: &str,
    content: &str,
    state: &ServeState,
) -> Result<u16> {
    write_response(stream, 200, content_type, content.as_bytes(), state)
}

fn write_empty_response(stream: &mut TcpStream, status: u16, state: &ServeState) -> Result<u16> {
    write_response(stream, status, "text/plain; charset=utf-8", &[], state)
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    state: &ServeState,
) -> Result<u16> {
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n",
        status_reason(status),
        body.len()
    )?;
    if let Some(origin) = &state.cors_origin {
        write!(
            stream,
            "Access-Control-Allow-Origin: {origin}\r\nAccess-Control-Allow-Headers: Authorization, Content-Type\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nVary: Origin\r\n"
        )?;
    }
    write!(stream, "\r\n")?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(status)
}

fn status_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

fn api_error(code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({"error": {"code": code, "message": message}})
}

fn validate_limits(command: &ServeCommandOptions) -> Result<()> {
    if command.max_result_rows == 0 {
        return Err(anyhow!("--max-result-rows must be greater than 0"));
    }
    if command.max_concurrent_requests == 0 {
        return Err(anyhow!("--max-concurrent-requests must be greater than 0"));
    }
    if parse_byte_size(&command.max_body_size)? == 0 {
        return Err(anyhow!("--max-body-size must be greater than 0"));
    }
    if parse_duration(&command.query_timeout, "--query-timeout")?.is_zero() {
        return Err(anyhow!("--query-timeout must be greater than 0"));
    }
    if parse_duration(&command.busy_timeout, "--busy-timeout")?.is_zero() {
        return Err(anyhow!("--busy-timeout must be greater than 0"));
    }
    Ok(())
}

fn resolve_token(token_env: Option<&str>, db: &str, bind_host: &str) -> Result<String> {
    if let Some(env_name) = token_env {
        let token = std::env::var(env_name)
            .map_err(|_| anyhow!("required token env var {env_name} is not set"))?;
        if token.is_empty() {
            return Err(anyhow!("required token env var {env_name} is empty"));
        }
        return Ok(token);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    let mut hasher = DefaultHasher::new();
    db.hash(&mut hasher);
    bind_host.hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    now.as_nanos().hash(&mut hasher);
    Ok(format!("ddb-{:016x}-{:x}", hasher.finish(), now.as_nanos()))
}

fn print_startup(
    db: &Db,
    read_only: bool,
    ui_url: &str,
    api_url: &str,
    bind: &str,
    auth: &AuthMode,
    show_token: bool,
) -> Result<()> {
    let mut stdout = std::io::stdout();
    writeln!(stdout, "DecentDB server started")?;
    writeln!(stdout)?;
    writeln!(stdout, "Database:  {}", db.path().display())?;
    writeln!(
        stdout,
        "Mode:      {}",
        if read_only { "read-only" } else { "read-write" }
    )?;
    writeln!(stdout, "Web UI:    {ui_url}")?;
    writeln!(stdout, "HTTP API:  {api_url}")?;
    writeln!(stdout, "Binding:   {bind}")?;
    writeln!(
        stdout,
        "Access:    {}",
        match auth {
            AuthMode::Disabled => "no auth (localhost only)",
            AuthMode::Bearer(_) => "local browser session",
        }
    )?;
    if let (true, AuthMode::Bearer(token)) = (show_token, auth) {
        writeln!(stdout, "Token:     {token}")?;
    }
    writeln!(stdout)?;
    writeln!(stdout, "Press Ctrl+C to stop.")?;
    stdout.flush()?;
    Ok(())
}

fn log_request(state: &ServeState, log: &RequestLog) {
    match state.log_format {
        LogFormat::Text => {
            if let Some(error) = &log.error {
                eprintln!(
                    "{} {} {} {:.2}ms {}",
                    log.method, log.path, log.status, log.elapsed_ms, error
                );
            } else {
                eprintln!(
                    "{} {} {} {:.2}ms",
                    log.method, log.path, log.status, log.elapsed_ms
                );
            }
        }
        LogFormat::Json => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "method": log.method,
                    "path": log.path,
                    "status": log.status,
                    "elapsedMs": log.elapsed_ms,
                    "error": log.error,
                })
            );
        }
    }
}

fn split_request_target(target: &str) -> (&str, Option<&str>) {
    match target.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (target, None),
    }
}

fn query_has(query: &Option<String>, key: &str, value: &str) -> bool {
    let Some(query) = query else {
        return false;
    };
    query.split('&').any(|part| {
        let (part_key, part_value) = part.split_once('=').unwrap_or((part, ""));
        part_key == key && part_value.eq_ignore_ascii_case(value)
    })
}

fn parse_byte_size(raw: &str) -> Result<usize> {
    let value = raw.trim().to_ascii_lowercase();
    if value.is_empty() {
        return Err(anyhow!("--max-body-size is required"));
    }

    let (number_part, multiplier) = if value.ends_with("kb") {
        (&value[..value.len() - 2], 1024usize)
    } else if value.ends_with("mb") {
        (&value[..value.len() - 2], 1024usize.saturating_mul(1024))
    } else if value.ends_with("gb") {
        (&value[..value.len() - 2], 1024usize.saturating_pow(3))
    } else if value.ends_with('k') {
        (&value[..value.len() - 1], 1024usize)
    } else if value.ends_with('m') {
        (&value[..value.len() - 1], 1024usize.saturating_mul(1024))
    } else if value.ends_with('g') {
        (&value[..value.len() - 1], 1024usize.saturating_pow(3))
    } else {
        (value.as_str(), 1)
    };

    let number = number_part.parse::<usize>()?;
    number
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow!("--max-body-size overflows usize"))
}

fn parse_duration(raw: &str, flag_name: &str) -> Result<Duration> {
    let value = raw.trim().to_ascii_lowercase();
    if value.is_empty() {
        return Err(anyhow!("{flag_name} is required"));
    }

    let (number_part, millis) = if value.ends_with("ms") {
        (&value[..value.len() - 2], true)
    } else if value.ends_with('s') {
        (&value[..value.len() - 1], false)
    } else {
        (value.as_str(), false)
    };

    let number = number_part.parse::<u64>()?;
    if millis {
        Ok(Duration::from_millis(number))
    } else {
        Ok(Duration::from_secs(number))
    }
}

fn host_from_bind(bind: &str) -> String {
    if let Some(rest) = bind.strip_prefix('[') {
        if let Some((host, _)) = rest.split_once(']') {
            return host.to_string();
        }
    }
    bind.rsplit_once(':')
        .map(|(host, _)| host.to_string())
        .unwrap_or_else(|| bind.to_string())
}

fn is_localhost_host(host: &str) -> bool {
    let normalized = host.trim_matches(['[', ']']);
    normalized.eq_ignore_ascii_case("localhost")
        || normalized
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

fn local_url(bind_host: &str, port: u16, path: &str) -> String {
    let host = if is_localhost_host(bind_host) {
        "localhost".to_string()
    } else {
        bind_host.to_string()
    };
    format!("http://{host}:{port}{path}")
}

fn open_browser(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        Command::new("open").arg(url).spawn()?;
        return Ok(());
    }

    #[cfg(target_os = "windows")]
    {
        Command::new("cmd")
            .arg("/C")
            .arg("start")
            .arg("")
            .arg(url)
            .spawn()?;
        return Ok(());
    }

    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        Command::new("xdg-open").arg(url).spawn()?;
        Ok(())
    }
}

fn resolve_table_name(db: &Db, requested: &str) -> Result<String> {
    for table in db.list_tables()? {
        if table.name.eq_ignore_ascii_case(requested) {
            return Ok(table.name);
        }
    }
    Err(anyhow!("unknown table {requested}"))
}

fn percent_decode(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(anyhow!("invalid percent-encoded path"));
            }
            let high = hex_value(bytes[i + 1])?;
            let low = hex_value(bytes[i + 2])?;
            decoded.push((high << 4) | low);
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| anyhow!("path is not valid UTF-8"))
}

fn hex_value(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(anyhow!("invalid percent-encoded path")),
    }
}

fn split_sql_batch(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut statement_tokens = Vec::new();
    let mut trigger_body_depth = 0usize;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            current.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            current.push(ch);
            if ch == '*' && matches!(chars.peek(), Some('/')) {
                current.push(chars.next().expect("comment terminator"));
                in_block_comment = false;
            }
            continue;
        }

        if in_single {
            current.push(ch);
            if ch == '\'' {
                if matches!(chars.peek(), Some('\'')) {
                    current.push(chars.next().expect("escaped quote"));
                } else {
                    in_single = false;
                }
            }
            continue;
        }

        if in_double {
            current.push(ch);
            if ch == '"' {
                if matches!(chars.peek(), Some('"')) {
                    current.push(chars.next().expect("escaped quote"));
                } else {
                    in_double = false;
                }
            }
            continue;
        }

        match ch {
            _ if ch.is_ascii_alphanumeric() || ch == '_' => {
                current.push(ch);
                let mut token = ch.to_ascii_uppercase().to_string();
                while let Some(next) = chars.peek().copied() {
                    if !(next.is_ascii_alphanumeric() || next == '_') {
                        break;
                    }
                    let next = chars.next().expect("peeked token char");
                    current.push(next);
                    token.push(next.to_ascii_uppercase());
                }
                if statement_tokens.len() < 2 {
                    statement_tokens.push(token.clone());
                }
                if statement_tokens.as_slice() == ["CREATE", "TRIGGER"] {
                    if token == "BEGIN" {
                        trigger_body_depth += 1;
                    } else if token == "END" && trigger_body_depth > 0 {
                        trigger_body_depth -= 1;
                    }
                }
            }
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '"' => {
                in_double = true;
                current.push(ch);
            }
            '-' if matches!(chars.peek(), Some('-')) => {
                current.push(ch);
                current.push(chars.next().expect("line comment start"));
                in_line_comment = true;
            }
            '/' if matches!(chars.peek(), Some('*')) => {
                current.push(ch);
                current.push(chars.next().expect("block comment start"));
                in_block_comment = true;
            }
            ';' => {
                if trigger_body_depth > 0 {
                    current.push(ch);
                } else if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                    current.clear();
                    statement_tokens.clear();
                    trigger_body_depth = 0;
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    statements
}

const CONSOLE_HTML: &str = r##"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>DecentDB Console</title>
    <link rel="stylesheet" href="/assets/console.css">
  </head>
  <body>
    <header class="topbar">
      <div class="brand">
        <strong>DecentDB Console</strong>
        <span id="db-name">connecting</span>
      </div>
      <div class="statusline">
        <span id="mode-badge" class="badge">mode</span>
        <span id="host-badge" class="badge muted">localhost</span>
        <button id="theme-toggle" type="button">Theme</button>
      </div>
    </header>

    <main class="workspace">
      <aside class="sidebar" aria-label="Schema browser">
        <div class="sidebar-header">
          <h1>Schema</h1>
          <input id="schema-filter" type="search" placeholder="Filter">
        </div>
        <div id="schema-tree" class="schema-tree"></div>
      </aside>

      <section class="content">
        <section class="object-detail" aria-live="polite">
          <div class="section-head">
            <h2 id="detail-title">Database</h2>
            <span id="detail-meta" class="muted"></span>
          </div>
          <div id="detail-body" class="detail-body empty">Select a table to inspect columns, indexes, constraints, and triggers.</div>
        </section>

        <section class="editor">
          <div class="section-head">
            <h2>SQL</h2>
            <div class="actions">
              <select id="history-select" title="Query history">
                <option value="">History</option>
              </select>
              <button id="clear-history" type="button">Clear History</button>
            </div>
          </div>
          <label class="visually-hidden" for="sql">SQL query</label>
          <textarea id="sql" spellcheck="false">SELECT 1;</textarea>
          <div class="toolbar">
            <button id="run" type="button" class="primary">Run</button>
            <button id="explain" type="button">Explain</button>
            <button id="clear" type="button">Clear</button>
            <button id="copy-results" type="button">Copy</button>
            <button id="export-csv" type="button">Export CSV</button>
          </div>
        </section>

        <section class="results">
          <div class="section-head">
            <h2>Results</h2>
            <span id="result-summary" class="muted">No query run</span>
          </div>
          <div id="message" class="message"></div>
          <div id="results-grid" class="grid-wrap empty">Run a query to see results.</div>
        </section>
      </section>
    </main>

    <script>__DECENTDB_BOOTSTRAP__</script>
    <script src="/assets/console.js"></script>
  </body>
</html>"##;

const CONSOLE_CSS: &str = r##":root {
  color-scheme: light;
  --bg: #f5f7fb;
  --surface: #ffffff;
  --surface-2: #eef2f7;
  --border: #d6dde8;
  --text: #18202f;
  --muted: #647084;
  --accent: #1769e0;
  --accent-strong: #0f54b8;
  --danger: #b42318;
  --ok: #167647;
  --shadow: 0 12px 32px rgba(24, 32, 47, 0.08);
  --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  --sans: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

[data-theme="dark"] {
  color-scheme: dark;
  --bg: #111418;
  --surface: #181c22;
  --surface-2: #222833;
  --border: #343c49;
  --text: #eef2f7;
  --muted: #a8b1c1;
  --accent: #69a5ff;
  --accent-strong: #8fbdff;
  --danger: #ff8a80;
  --ok: #6ee7a8;
  --shadow: 0 12px 32px rgba(0, 0, 0, 0.22);
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  min-height: 100vh;
  overflow: hidden;
  background: var(--bg);
  color: var(--text);
  font-family: var(--sans);
}

button,
input,
select,
textarea {
  font: inherit;
}

button,
select,
input {
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--surface);
  color: var(--text);
  min-height: 32px;
}

button {
  padding: 0 10px;
  cursor: pointer;
}

button:hover {
  border-color: var(--accent);
}

button.primary {
  background: var(--accent);
  color: #ffffff;
  border-color: var(--accent);
}

button.primary:hover {
  background: var(--accent-strong);
}

.topbar {
  height: 52px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 0 16px;
  border-bottom: 1px solid var(--border);
  background: var(--surface);
}

.brand,
.statusline,
.section-head,
.toolbar,
.actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.brand strong {
  font-size: 15px;
}

.badge {
  display: inline-flex;
  align-items: center;
  border: 1px solid var(--border);
  border-radius: 999px;
  min-height: 26px;
  padding: 0 9px;
  font-size: 12px;
  color: var(--ok);
  background: var(--surface-2);
}

.badge.muted,
.muted {
  color: var(--muted);
}

.workspace {
  height: calc(100vh - 52px);
  display: grid;
  grid-template-columns: 300px minmax(0, 1fr);
}

.sidebar {
  min-width: 0;
  border-right: 1px solid var(--border);
  background: var(--surface);
  overflow: auto;
}

.sidebar-header {
  position: sticky;
  top: 0;
  z-index: 1;
  padding: 14px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
}

h1,
h2,
h3 {
  margin: 0;
  font-size: 14px;
}

.sidebar h1 {
  margin-bottom: 10px;
}

.sidebar input {
  width: 100%;
  padding: 0 10px;
}

.schema-tree {
  padding: 10px 8px 18px;
}

details {
  margin-bottom: 8px;
}

summary {
  cursor: pointer;
  color: var(--muted);
  font-size: 12px;
  text-transform: uppercase;
}

.schema-item {
  display: block;
  width: 100%;
  text-align: left;
  margin: 4px 0;
  background: transparent;
  border-color: transparent;
  font-family: var(--mono);
}

.schema-item:hover,
.schema-item.active {
  background: var(--surface-2);
  border-color: var(--border);
}

.content {
  min-width: 0;
  display: grid;
  grid-template-rows: minmax(120px, 0.8fr) minmax(210px, 1fr) minmax(180px, 1fr);
  gap: 12px;
  padding: 12px;
  overflow: hidden;
}

.object-detail,
.editor,
.results {
  min-width: 0;
  min-height: 0;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  box-shadow: var(--shadow);
  overflow: hidden;
}

.section-head {
  min-height: 42px;
  justify-content: space-between;
  padding: 10px 12px;
  border-bottom: 1px solid var(--border);
  background: var(--surface-2);
}

.detail-body,
.grid-wrap {
  overflow: auto;
}

.detail-body {
  height: calc(100% - 42px);
  padding: 12px;
}

.detail-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 12px;
}

.detail-grid table,
.grid-wrap table {
  width: 100%;
  border-collapse: collapse;
}

th,
td {
  border-bottom: 1px solid var(--border);
  padding: 7px 8px;
  text-align: left;
  vertical-align: top;
}

th {
  position: sticky;
  top: 0;
  background: var(--surface-2);
  z-index: 1;
  font-size: 12px;
  color: var(--muted);
}

td {
  font-family: var(--mono);
  font-size: 12px;
  max-width: 420px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

pre {
  margin: 0;
  padding: 10px;
  background: var(--surface-2);
  border: 1px solid var(--border);
  border-radius: 6px;
  overflow: auto;
  font-family: var(--mono);
  font-size: 12px;
}

.editor textarea {
  width: calc(100% - 24px);
  height: calc(100% - 96px);
  margin: 12px;
  resize: none;
  color: var(--text);
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 10px;
  font-family: var(--mono);
  font-size: 13px;
  line-height: 1.45;
}

.toolbar {
  padding: 0 12px 12px;
}

.results {
  display: grid;
  grid-template-rows: 42px auto minmax(0, 1fr);
}

.message {
  min-height: 0;
  padding: 0 12px;
  color: var(--danger);
}

.message.ok {
  color: var(--ok);
}

.grid-wrap {
  padding: 0 12px 12px;
}

.empty {
  color: var(--muted);
}

.visually-hidden {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

@media (max-width: 820px) {
  body {
    overflow: auto;
  }

  .workspace {
    height: auto;
    grid-template-columns: 1fr;
  }

  .sidebar {
    max-height: 260px;
    border-right: 0;
    border-bottom: 1px solid var(--border);
  }

  .content {
    height: auto;
    grid-template-rows: auto auto auto;
  }

  .object-detail,
  .editor,
  .results {
    min-height: 260px;
  }
}
"##;

const CONSOLE_JS: &str = r##"const urlToken = new URLSearchParams(window.location.search).get('token') || '';
const token = window.DECENTDB_BOOTSTRAP_TOKEN || urlToken || sessionStorage.getItem('decentdb-api-token') || '';
if (token) {
  sessionStorage.setItem('decentdb-api-token', token);
  if (urlToken) {
    history.replaceState(null, '', window.location.pathname);
  }
}

const state = {
  info: null,
  schema: null,
  currentRows: [],
  currentColumns: [],
  activeTable: '',
};

const els = {
  dbName: document.getElementById('db-name'),
  mode: document.getElementById('mode-badge'),
  host: document.getElementById('host-badge'),
  schemaTree: document.getElementById('schema-tree'),
  schemaFilter: document.getElementById('schema-filter'),
  detailTitle: document.getElementById('detail-title'),
  detailMeta: document.getElementById('detail-meta'),
  detailBody: document.getElementById('detail-body'),
  sql: document.getElementById('sql'),
  run: document.getElementById('run'),
  explain: document.getElementById('explain'),
  clear: document.getElementById('clear'),
  copy: document.getElementById('copy-results'),
  exportCsv: document.getElementById('export-csv'),
  message: document.getElementById('message'),
  resultSummary: document.getElementById('result-summary'),
  grid: document.getElementById('results-grid'),
  history: document.getElementById('history-select'),
  clearHistory: document.getElementById('clear-history'),
  theme: document.getElementById('theme-toggle'),
};

function headers() {
  const saved = sessionStorage.getItem('decentdb-api-token') || '';
  return saved ? {'Authorization': `Bearer ${saved}`} : {};
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      ...headers(),
      ...(options.headers || {}),
    },
  });
  const text = await response.text();
  let body = {};
  try {
    body = text ? JSON.parse(text) : {};
  } catch (_error) {
    body = {error: {code: 'INVALID_RESPONSE', message: text}};
  }
  if (!response.ok) {
    throw body;
  }
  return body;
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[ch]));
}

function setMessage(text, ok = false) {
  els.message.textContent = text || '';
  els.message.classList.toggle('ok', ok);
}

function loadTheme() {
  const theme = localStorage.getItem('decentdb-console-theme') || 'light';
  document.documentElement.dataset.theme = theme;
}

function toggleTheme() {
  const current = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
  document.documentElement.dataset.theme = current;
  localStorage.setItem('decentdb-console-theme', current);
}

function historyItems() {
  try {
    return JSON.parse(localStorage.getItem('decentdb-query-history') || '[]');
  } catch (_error) {
    return [];
  }
}

function saveHistory(sql) {
  const trimmed = sql.trim();
  if (!trimmed) return;
  const next = [trimmed, ...historyItems().filter((item) => item !== trimmed)].slice(0, 30);
  localStorage.setItem('decentdb-query-history', JSON.stringify(next));
  renderHistory();
}

function renderHistory() {
  const items = historyItems();
  els.history.innerHTML = '<option value="">History</option>' + items
    .map((item) => `<option value="${escapeHtml(item)}">${escapeHtml(item.slice(0, 80))}</option>`)
    .join('');
}

function section(title, items, kind) {
  const filtered = filterItems(items);
  const body = filtered.map((item) => {
    const name = item.name || item.table_name || item.target_name || 'object';
    return `<button class="schema-item" data-kind="${kind}" data-name="${escapeHtml(name)}">${escapeHtml(name)}</button>`;
  }).join('');
  return `<details open><summary>${title} ${filtered.length}</summary>${body || '<div class="empty">None</div>'}</details>`;
}

function filterItems(items) {
  const filter = els.schemaFilter.value.trim().toLowerCase();
  if (!filter) return items || [];
  return (items || []).filter((item) => String(item.name || item.table_name || item.target_name || '').toLowerCase().includes(filter));
}

function renderSchema() {
  const schema = state.schema || {};
  els.schemaTree.innerHTML = [
    section('Tables', schema.tables || [], 'table'),
    section('Views', schema.views || [], 'view'),
    section('Indexes', schema.indexes || [], 'index'),
    section('Triggers', schema.triggers || [], 'trigger'),
  ].join('');

  for (const button of els.schemaTree.querySelectorAll('[data-kind="table"]')) {
    button.addEventListener('click', () => showTable(button.dataset.name));
  }
}

function renderOverview() {
  const info = state.info;
  if (!info) return;
  els.dbName.textContent = info.database.fileName;
  els.mode.textContent = info.database.readOnly ? 'READ-ONLY' : 'READ-WRITE';
  els.mode.style.color = info.database.readOnly ? 'var(--danger)' : 'var(--ok)';
  els.host.textContent = info.server.bind;
  els.detailTitle.textContent = 'Database';
  els.detailMeta.textContent = `${info.engine.fileFormatVersion ? 'format v' + info.engine.fileFormatVersion : ''}`;
  els.detailBody.innerHTML = `<div class="detail-grid">
    <table><tbody>
      <tr><th>Path</th><td>${escapeHtml(info.database.path)}</td></tr>
      <tr><th>Size</th><td>${Number(info.database.sizeBytes || 0).toLocaleString()} bytes</td></tr>
      <tr><th>Page size</th><td>${escapeHtml(info.engine.pageSize)}</td></tr>
      <tr><th>Pages</th><td>${escapeHtml(info.engine.pageCount)}</td></tr>
      <tr><th>WAL bytes</th><td>${Number(info.engine.walFileSize || 0).toLocaleString()}</td></tr>
    </tbody></table>
  </div>`;
}

async function showTable(name) {
  state.activeTable = name;
  for (const button of els.schemaTree.querySelectorAll('.schema-item')) {
    button.classList.toggle('active', button.dataset.name === name);
  }
  els.detailTitle.textContent = name;
  els.detailMeta.textContent = 'Loading';
  els.detailBody.textContent = '';
  try {
    const payload = await api(`/api/v1/tables/${encodeURIComponent(name)}`);
    const table = payload.table || {};
    els.detailMeta.textContent = `${table.row_count ?? table.rowCount ?? 0} rows`;
    const columns = (table.columns || []).map((column) => `<tr>
      <td>${escapeHtml(column.name)}</td>
      <td>${escapeHtml(column.column_type || column.type)}</td>
      <td>${column.primary_key ? 'PK' : ''}${column.nullable === false ? ' NOT NULL' : ''}${column.unique ? ' UNIQUE' : ''}</td>
      <td>${escapeHtml(column.default_sql || '')}</td>
    </tr>`).join('');
    const indexes = (payload.indexes || []).map((index) => `<tr>
      <td>${escapeHtml(index.name)}</td>
      <td>${escapeHtml(index.kind)}</td>
      <td>${index.unique ? 'yes' : 'no'}</td>
      <td>${escapeHtml((index.columns || []).join(', '))}</td>
    </tr>`).join('');
    const triggers = (payload.triggers || []).map((trigger) => `<tr>
      <td>${escapeHtml(trigger.name)}</td>
      <td>${escapeHtml((trigger.events || [trigger.event || '']).join(', '))}</td>
      <td>${escapeHtml(trigger.timing || trigger.kind || '')}</td>
    </tr>`).join('');
    els.detailBody.innerHTML = `<div class="detail-grid">
      <section><h3>Columns</h3><table><thead><tr><th>Name</th><th>Type</th><th>Constraints</th><th>Default</th></tr></thead><tbody>${columns}</tbody></table></section>
      <section><h3>Indexes</h3><table><thead><tr><th>Name</th><th>Kind</th><th>Unique</th><th>Columns</th></tr></thead><tbody>${indexes || '<tr><td colspan="4">None</td></tr>'}</tbody></table></section>
      <section><h3>Triggers</h3><table><thead><tr><th>Name</th><th>Events</th><th>Timing</th></tr></thead><tbody>${triggers || '<tr><td colspan="3">None</td></tr>'}</tbody></table></section>
      <section><h3>Create SQL</h3><pre>${escapeHtml(payload.ddl || '')}</pre></section>
    </div>`;
  } catch (error) {
    els.detailMeta.textContent = 'Error';
    els.detailBody.textContent = error.error?.message || String(error);
  }
}

function renderResult(payload) {
  const result = payload.results?.[0];
  state.currentRows = result?.rows || [];
  state.currentColumns = result?.columns || [];
  if (!result) {
    els.grid.textContent = 'No result sets.';
    els.grid.classList.add('empty');
    return;
  }
  els.resultSummary.textContent = `${state.currentRows.length} rows, ${Number(payload.elapsedMs || 0).toFixed(2)} ms`;
  if (result.truncated) {
    setMessage(`Results truncated at ${result.limit} rows. Add an explicit LIMIT for large tables.`, false);
  } else {
    setMessage('OK', true);
  }
  if (!state.currentRows.length) {
    els.grid.textContent = `Rows affected: ${result.rowsAffected || 0}`;
    els.grid.classList.add('empty');
    return;
  }
  const head = state.currentColumns.map((column) => `<th>${escapeHtml(column.name)}<br><span class="muted">${escapeHtml(column.type || '')}</span></th>`).join('');
  const body = state.currentRows.map((row) => `<tr>${row.map((cell) => `<td title="${escapeHtml(cell)}">${escapeHtml(cell === null ? 'NULL' : cell)}</td>`).join('')}</tr>`).join('');
  els.grid.classList.remove('empty');
  els.grid.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

async function runSql(explain = false) {
  const sql = els.sql.value;
  els.resultSummary.textContent = 'Running';
  els.grid.textContent = '';
  setMessage('');
  try {
    const payload = await api(explain ? '/api/v1/explain' : '/api/v1/sql', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({sql, params: []}),
    });
    saveHistory(sql);
    renderResult(payload);
    if (!explain) {
      state.schema = await api('/api/v1/schema');
      renderSchema();
    }
  } catch (error) {
    els.resultSummary.textContent = 'Error';
    els.grid.textContent = '';
    els.grid.classList.add('empty');
    setMessage(`${error.error?.code || 'ERROR'}: ${error.error?.message || error}`, false);
  }
}

function toCsv() {
  const lines = [];
  lines.push(state.currentColumns.map((column) => csvCell(column.name)).join(','));
  for (const row of state.currentRows) {
    lines.push(row.map(csvCell).join(','));
  }
  return lines.join('\n');
}

function csvCell(value) {
  const text = value === null || value === undefined ? '' : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

async function copyResults() {
  await navigator.clipboard.writeText(toCsv());
  setMessage('Results copied as CSV.', true);
}

function exportCsv() {
  const blob = new Blob([toCsv()], {type: 'text/csv'});
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = 'decentdb-results.csv';
  anchor.click();
  URL.revokeObjectURL(url);
}

async function init() {
  loadTheme();
  renderHistory();
  try {
    state.info = await api('/api/v1/info');
    state.schema = await api('/api/v1/schema');
    renderOverview();
    renderSchema();
    setMessage('Ready.', true);
  } catch (error) {
    setMessage(`${error.error?.code || 'ERROR'}: ${error.error?.message || error}`, false);
  }
}

els.run.addEventListener('click', () => runSql(false));
els.explain.addEventListener('click', () => runSql(true));
els.clear.addEventListener('click', () => {
  els.sql.value = '';
  els.grid.textContent = 'Run a query to see results.';
  els.grid.classList.add('empty');
  setMessage('');
});
els.copy.addEventListener('click', copyResults);
els.exportCsv.addEventListener('click', exportCsv);
els.schemaFilter.addEventListener('input', renderSchema);
els.theme.addEventListener('click', toggleTheme);
els.history.addEventListener('change', () => {
  if (els.history.value) els.sql.value = els.history.value;
});
els.clearHistory.addEventListener('click', () => {
  localStorage.removeItem('decentdb-query-history');
  renderHistory();
});
els.sql.addEventListener('keydown', (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
    event.preventDefault();
    runSql(false);
  }
});

init();
"##;
