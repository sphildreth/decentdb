use std::collections::BTreeMap;

use crate::error::{DbError, Result};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum JsonValue {
    Object(BTreeMap<String, JsonValue>),
    Array(Vec<JsonValue>),
    String(String),
    Number(String),
    Bool(bool),
    Null,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JsonPathSegment {
    Key(String),
    Index(usize),
}

pub(crate) fn parse_json(input: &str) -> Result<JsonValue> {
    let mut parser = JsonParser::new(input);
    let value = parser
        .parse_value()
        .map_err(|error| DbError::sql(format!("invalid JSON: {error}")))?;
    parser
        .finish()
        .map_err(|error| DbError::sql(format!("invalid JSON: {error}")))?;
    Ok(value)
}

pub(crate) fn parse_json_path(path: &str) -> Result<Vec<JsonPathSegment>> {
    let bytes = path.as_bytes();
    if !matches!(bytes.first(), Some(b'$')) {
        return Err(DbError::sql("invalid JSON path"));
    }
    let mut offset = 1;
    let mut segments = Vec::new();
    while offset < bytes.len() {
        match bytes[offset] {
            b'.' => {
                offset += 1;
                let start = offset;
                while offset < bytes.len() && !matches!(bytes[offset], b'.' | b'[') {
                    offset += 1;
                }
                if start == offset {
                    return Err(DbError::sql("invalid JSON path"));
                }
                segments.push(JsonPathSegment::Key(path[start..offset].to_string()));
            }
            b'[' => {
                offset += 1;
                let start = offset;
                while offset < bytes.len() && bytes[offset].is_ascii_digit() {
                    offset += 1;
                }
                if start == offset || bytes.get(offset) != Some(&b']') {
                    return Err(DbError::sql("invalid JSON path"));
                }
                let index = path[start..offset]
                    .parse::<usize>()
                    .map_err(|_| DbError::sql("invalid JSON path"))?;
                offset += 1;
                segments.push(JsonPathSegment::Index(index));
            }
            _ => return Err(DbError::sql("invalid JSON path")),
        }
    }
    Ok(segments)
}

impl JsonValue {
    pub(crate) fn lookup<'a>(&'a self, path: &[JsonPathSegment]) -> Option<&'a JsonValue> {
        let mut current = self;
        for segment in path {
            current = match (segment, current) {
                (JsonPathSegment::Key(key), JsonValue::Object(object)) => object.get(key)?,
                (JsonPathSegment::Index(index), JsonValue::Array(array)) => array.get(*index)?,
                _ => return None,
            };
        }
        Some(current)
    }

    pub(crate) fn render_json(&self) -> String {
        match self {
            JsonValue::Object(object) => format!(
                "{{{}}}",
                object
                    .iter()
                    .map(|(key, value)| format!("\"{}\":{}", json_escape(key), value.render_json()))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            JsonValue::Array(array) => format!(
                "[{}]",
                array
                    .iter()
                    .map(JsonValue::render_json)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            JsonValue::String(value) => format!("\"{}\"", json_escape(value)),
            JsonValue::Number(value) => value.clone(),
            JsonValue::Bool(true) => "true".to_string(),
            JsonValue::Bool(false) => "false".to_string(),
            JsonValue::Null => "null".to_string(),
        }
    }
}

fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
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

    fn finish(&mut self) -> std::result::Result<(), String> {
        self.skip_whitespace();
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err("trailing content after JSON value".to_string())
        }
    }

    fn parse_value(&mut self) -> std::result::Result<JsonValue, String> {
        self.skip_whitespace();
        let byte = self
            .peek()
            .ok_or_else(|| "unexpected end of JSON input".to_string())?;
        match byte {
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
            b'"' => self.parse_string().map(JsonValue::String),
            b'-' | b'0'..=b'9' => self.parse_number().map(JsonValue::Number),
            b't' => {
                self.consume_literal(b"true")?;
                Ok(JsonValue::Bool(true))
            }
            b'f' => {
                self.consume_literal(b"false")?;
                Ok(JsonValue::Bool(false))
            }
            b'n' => {
                self.consume_literal(b"null")?;
                Ok(JsonValue::Null)
            }
            other => Err(format!("unexpected JSON byte {}", other as char)),
        }
    }

    fn parse_object(&mut self) -> std::result::Result<JsonValue, String> {
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

    fn parse_array(&mut self) -> std::result::Result<JsonValue, String> {
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

    fn parse_string(&mut self) -> std::result::Result<String, String> {
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
                        _ => return Err(format!("unsupported JSON escape \\{}", escaped as char)),
                    }
                }
                other => string.push(other as char),
            }
        }
        Err("unterminated JSON string".to_string())
    }

    fn parse_number(&mut self) -> std::result::Result<String, String> {
        let start = self.offset;
        if self.peek() == Some(b'-') {
            self.offset += 1;
        }
        let integer_start = self.offset;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.offset += 1;
        }
        if self.offset == integer_start {
            return Err("invalid JSON number".to_string());
        }
        if self.peek() == Some(b'.') {
            self.offset += 1;
            let fractional_start = self.offset;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.offset += 1;
            }
            if self.offset == fractional_start {
                return Err("invalid JSON number".to_string());
            }
        }
        std::str::from_utf8(&self.input[start..self.offset])
            .map(|value| value.to_string())
            .map_err(|error| format!("invalid utf8 in JSON number: {error}"))
    }

    fn consume_literal(&mut self, literal: &[u8]) -> std::result::Result<(), String> {
        if self.input.get(self.offset..self.offset + literal.len()) == Some(literal) {
            self.offset += literal.len();
            Ok(())
        } else {
            Err("invalid JSON literal".to_string())
        }
    }

    fn expect(&mut self, byte: u8) -> std::result::Result<(), String> {
        self.skip_whitespace();
        if self.peek() == Some(byte) {
            self.offset += 1;
            Ok(())
        } else {
            Err(format!("expected JSON byte {}", byte as char))
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
