use std::collections::BTreeMap;
use std::env;
use std::fs;

use decentdb::{Db, DbConfig, WalSyncMode};

fn main() -> Result<(), String> {
    let scenario_path = env::args()
        .nth(1)
        .ok_or_else(|| "usage: decentdb-test-harness <scenario.json>".to_string())?;
    let source = fs::read_to_string(&scenario_path)
        .map_err(|error| format!("read scenario {scenario_path}: {error}"))?;
    let scenario = Scenario::from_json(&source)?;
    match scenario.run() {
        Ok(output) => {
            println!("{output}");
            Ok(())
        }
        Err(output) => {
            println!("{output}");
            std::process::exit(1);
        }
    }
}

struct Scenario {
    name: String,
    path: String,
    config: DbConfig,
    ops: Vec<ScenarioOp>,
}

impl Scenario {
    fn from_json(source: &str) -> Result<Self, String> {
        let value = JsonParser::new(source).parse_value()?;
        let root = value
            .as_object()
            .ok_or_else(|| "scenario root must be an object".to_string())?;

        let name = root
            .get("name")
            .and_then(JsonValue::as_str)
            .unwrap_or("unnamed")
            .to_string();
        let path = root
            .get("path")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "scenario.path is required".to_string())?
            .to_string();

        let mut config = DbConfig::default();
        if let Some(page_size) = root.get("page_size").and_then(JsonValue::as_u64) {
            config.page_size = page_size as u32;
        }
        if let Some(sync_mode) = root.get("wal_sync_mode").and_then(JsonValue::as_str) {
            config.wal_sync_mode = match sync_mode {
                "full" => WalSyncMode::Full,
                "normal" => WalSyncMode::Normal,
                "unsafe_no_sync" => WalSyncMode::TestingOnlyUnsafeNoSync,
                _ => {
                    return Err(format!("unsupported wal_sync_mode {sync_mode}"));
                }
            };
        }

        let ops = root
            .get("ops")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| "scenario.ops must be an array".to_string())?
            .iter()
            .map(ScenarioOp::from_json)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            name,
            path,
            config,
            ops,
        })
    }

    fn run(&self) -> Result<String, String> {
        let mut db: Option<Db> = None;
        let mut results = Vec::new();
        let mut failed = false;

        for op in &self.ops {
            let actual = op.execute(&self.path, &self.config, &mut db);
            let expected_error = op.expect_error();
            match actual {
                Ok(payload) => {
                    if expected_error {
                        failed = true;
                        results.push(format!(
                            "{{\"op\":\"{}\",\"status\":\"failed\",\"message\":\"expected error\"}}",
                            json_escape(&op.name)
                        ));
                    } else {
                        results.push(format!(
                            "{{\"op\":\"{}\",\"status\":\"ok\"{}}}",
                            json_escape(&op.name),
                            payload
                                .map(|payload| format!(",{payload}"))
                                .unwrap_or_default()
                        ));
                    }
                }
                Err(error) => {
                    if expected_error {
                        results.push(format!(
                            "{{\"op\":\"{}\",\"status\":\"ok\",\"expected_error\":true,\"message\":\"{}\"}}",
                            json_escape(&op.name),
                            json_escape(&error)
                        ));
                    } else {
                        failed = true;
                        results.push(format!(
                            "{{\"op\":\"{}\",\"status\":\"error\",\"message\":\"{}\"}}",
                            json_escape(&op.name),
                            json_escape(&error)
                        ));
                    }
                }
            }
        }

        let output = format!(
            "{{\"scenario\":\"{}\",\"path\":\"{}\",\"results\":[{}]}}",
            json_escape(&self.name),
            json_escape(&self.path),
            results.join(",")
        );
        if failed {
            Err(output)
        } else {
            Ok(output)
        }
    }
}

struct ScenarioOp {
    name: String,
    fields: BTreeMap<String, JsonValue>,
}

impl ScenarioOp {
    fn from_json(value: &JsonValue) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "scenario ops must be objects".to_string())?;
        let name = object
            .get("op")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "scenario op missing 'op'".to_string())?
            .to_string();
        Ok(Self {
            name,
            fields: object.clone(),
        })
    }

    fn expect_error(&self) -> bool {
        self.fields
            .get("expect")
            .and_then(JsonValue::as_str)
            .is_some_and(|expect| expect == "error")
    }

    fn execute(
        &self,
        path: &str,
        config: &DbConfig,
        db: &mut Option<Db>,
    ) -> Result<Option<String>, String> {
        match self.name.as_str() {
            "create" => {
                *db = Some(Db::create(path, config.clone()).map_err(error_string)?);
                Ok(None)
            }
            "open" | "reopen" => {
                *db = Some(Db::open(path, config.clone()).map_err(error_string)?);
                Ok(None)
            }
            "install_failpoint" => {
                let label = required_string(&self.fields, "label")?;
                let action = required_string(&self.fields, "action")?;
                let trigger_on = required_u64(&self.fields, "trigger_on")?;
                let value = self
                    .fields
                    .get("value")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or(0) as usize;
                Db::install_failpoint(label, action, trigger_on, value).map_err(error_string)?;
                Ok(None)
            }
            "clear_failpoints" => {
                Db::clear_failpoints().map_err(error_string)?;
                Ok(None)
            }
            "begin" => {
                require_db(db)?.begin_write().map_err(error_string)?;
                Ok(None)
            }
            "write" => {
                let page_id = required_u64(&self.fields, "page_id")? as u32;
                let fill = self
                    .fields
                    .get("fill_byte")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or(0) as u8;
                let payload = vec![fill; require_db(db)?.config().page_size as usize];
                require_db(db)?
                    .write_page(page_id, &payload)
                    .map_err(error_string)?;
                Ok(Some(format!("\"page_id\":{page_id},\"fill_byte\":{fill}")))
            }
            "commit" => {
                let lsn = require_db(db)?.commit().map_err(error_string)?;
                Ok(Some(format!("\"lsn\":{lsn}")))
            }
            "checkpoint" => {
                require_db(db)?.checkpoint().map_err(error_string)?;
                Ok(None)
            }
            "inspect" => {
                let state = require_db(db)?
                    .inspect_storage_state_json()
                    .map_err(error_string)?;
                Ok(Some(format!("\"state\":{state}")))
            }
            "hold_snapshot" => {
                let token = require_db(db)?.hold_snapshot().map_err(error_string)?;
                Ok(Some(format!("\"token\":{token}")))
            }
            "release_snapshot" => {
                let token = required_u64(&self.fields, "token")?;
                require_db(db)?
                    .release_snapshot(token)
                    .map_err(error_string)?;
                Ok(None)
            }
            "read" => {
                let page_id = required_u64(&self.fields, "page_id")? as u32;
                let page = require_db(db)?.read_page(page_id).map_err(error_string)?;
                let first = page.first().copied().unwrap_or(0);
                Ok(Some(format!(
                    "\"page_id\":{page_id},\"first_byte\":{first}"
                )))
            }
            "failpoint_log" => {
                let log = Db::failpoint_log_json().map_err(error_string)?;
                Ok(Some(format!("\"log\":{log}")))
            }
            other => Err(format!("unsupported scenario op {other}")),
        }
    }
}

fn require_db(db: &Option<Db>) -> Result<&Db, String> {
    db.as_ref()
        .ok_or_else(|| "scenario attempted an operation before create/open".to_string())
}

fn required_string<'a>(
    object: &'a BTreeMap<String, JsonValue>,
    field: &str,
) -> Result<&'a str, String> {
    object
        .get(field)
        .and_then(JsonValue::as_str)
        .ok_or_else(|| format!("scenario op missing string field {field}"))
}

fn required_u64(object: &BTreeMap<String, JsonValue>, field: &str) -> Result<u64, String> {
    object
        .get(field)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| format!("scenario op missing numeric field {field}"))
}

fn error_string(error: decentdb::DbError) -> String {
    format!("{} ({})", error, error.numeric_code())
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[derive(Clone, Debug)]
enum JsonValue {
    Object(BTreeMap<String, JsonValue>),
    Array(Vec<JsonValue>),
    String(String),
    Number(u64),
    Bool,
    Null,
}

impl JsonValue {
    fn as_object(&self) -> Option<&BTreeMap<String, JsonValue>> {
        match self {
            Self::Object(object) => Some(object),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&Vec<JsonValue>> {
        match self {
            Self::Array(array) => Some(array),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(string) => Some(string),
            _ => None,
        }
    }

    fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Number(number) => Some(*number),
            _ => None,
        }
    }
}

struct JsonParser<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            offset: 0,
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, String> {
        self.skip_whitespace();
        let byte = self
            .peek()
            .ok_or_else(|| "unexpected end of json input".to_string())?;
        match byte {
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
            b'"' => self.parse_string().map(JsonValue::String),
            b'0'..=b'9' => self.parse_number().map(JsonValue::Number),
            b't' => {
                self.consume_literal(b"true")?;
                Ok(JsonValue::Bool)
            }
            b'f' => {
                self.consume_literal(b"false")?;
                Ok(JsonValue::Bool)
            }
            b'n' => {
                self.consume_literal(b"null")?;
                Ok(JsonValue::Null)
            }
            other => Err(format!("unexpected json byte {}", other as char)),
        }
    }

    fn parse_object(&mut self) -> Result<JsonValue, String> {
        self.expect(b'{')?;
        let mut object = BTreeMap::new();
        loop {
            self.skip_whitespace();
            if self.peek() == Some(b'}') {
                self.offset += 1;
                break;
            }

            let key = self.parse_string()?;
            self.skip_whitespace();
            self.expect(b':')?;
            let value = self.parse_value()?;
            object.insert(key, value);

            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.offset += 1,
                Some(b'}') => {
                    self.offset += 1;
                    break;
                }
                _ => return Err("expected ',' or '}' in object".to_string()),
            }
        }
        Ok(JsonValue::Object(object))
    }

    fn parse_array(&mut self) -> Result<JsonValue, String> {
        self.expect(b'[')?;
        let mut array = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek() == Some(b']') {
                self.offset += 1;
                break;
            }
            array.push(self.parse_value()?);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.offset += 1,
                Some(b']') => {
                    self.offset += 1;
                    break;
                }
                _ => return Err("expected ',' or ']' in array".to_string()),
            }
        }
        Ok(JsonValue::Array(array))
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut string = String::new();
        while let Some(byte) = self.peek() {
            self.offset += 1;
            match byte {
                b'"' => return Ok(string),
                b'\\' => {
                    let escaped = self
                        .peek()
                        .ok_or_else(|| "unterminated escape sequence".to_string())?;
                    self.offset += 1;
                    match escaped {
                        b'"' => string.push('"'),
                        b'\\' => string.push('\\'),
                        b'/' => string.push('/'),
                        b'b' => string.push('\u{0008}'),
                        b'f' => string.push('\u{000C}'),
                        b'n' => string.push('\n'),
                        b'r' => string.push('\r'),
                        b't' => string.push('\t'),
                        _ => return Err(format!("unsupported json escape \\{}", escaped as char)),
                    }
                }
                other => string.push(other as char),
            }
        }
        Err("unterminated json string".to_string())
    }

    fn parse_number(&mut self) -> Result<u64, String> {
        let start = self.offset;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.offset += 1;
        }
        let slice = std::str::from_utf8(&self.input[start..self.offset])
            .map_err(|error| format!("invalid utf8 in number: {error}"))?;
        slice
            .parse::<u64>()
            .map_err(|error| format!("invalid integer {slice}: {error}"))
    }

    fn consume_literal(&mut self, literal: &[u8]) -> Result<(), String> {
        if self.input.get(self.offset..self.offset + literal.len()) == Some(literal) {
            self.offset += literal.len();
            Ok(())
        } else {
            Err("invalid json literal".to_string())
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), String> {
        self.skip_whitespace();
        if self.peek() == Some(byte) {
            self.offset += 1;
            Ok(())
        } else {
            Err(format!("expected json byte {}", byte as char))
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.offset += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.offset).copied()
    }
}
