use anyhow::{anyhow, Result};
use flate2::read::ZlibDecoder;
use serde_json::Value as JsonValue;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

pub const V3_HEADER_SIZE: usize = 128;
pub const V3_MAGIC_BYTES: &[u8; 8] = b"DECENTDB";

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DbHeaderV3 {
    pub format_version: u32,
    pub page_size: u32,
    pub schema_cookie: u32,
    pub root_catalog: u32,
    pub root_freelist: u32,
    pub freelist_head: u32,
    pub freelist_count: u32,
    pub last_checkpoint_lsn: u64,
}

#[derive(Debug, PartialEq)]
pub enum ValueV3 {
    Null,
    Int64(i64),
    Bool(bool),
    Float64(f64),
    Text(String),
    Blob(Vec<u8>),
    Decimal { scale: u8, value: i64 },
    DateTime(i64),
}

/// Reader for the Nim-era Version 3 Database Format.
pub struct V3Reader {
    source_path: std::path::PathBuf,
    header: DbHeaderV3,
    file: File,
}

impl V3Reader {
    pub fn new<P: AsRef<Path>>(source: P) -> Result<Self> {
        let source_path = source.as_ref().to_path_buf();

        let mut file = File::open(&source_path)?;
        let mut buf = [0u8; V3_HEADER_SIZE];
        file.read_exact(&mut buf)?;

        let header = Self::decode_header(&buf)?;

        Ok(Self {
            source_path,
            header,
            file,
        })
    }

    fn decode_header(buf: &[u8; V3_HEADER_SIZE]) -> Result<DbHeaderV3> {
        for i in 0..8 {
            if buf[i] != V3_MAGIC_BYTES[i] {
                return Err(anyhow!("Invalid magic bytes for V3 database"));
            }
        }

        let format_version = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        if format_version != 3 {
            return Err(anyhow!(
                "Expected V3 format, found version {}",
                format_version
            ));
        }

        let page_size = u32::from_le_bytes(buf[20..24].try_into().unwrap());
        let schema_cookie = u32::from_le_bytes(buf[28..32].try_into().unwrap());
        let root_catalog = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        let root_freelist = u32::from_le_bytes(buf[36..40].try_into().unwrap());
        let freelist_head = u32::from_le_bytes(buf[40..44].try_into().unwrap());
        let freelist_count = u32::from_le_bytes(buf[44..48].try_into().unwrap());
        let last_checkpoint_lsn = u64::from_le_bytes(buf[48..56].try_into().unwrap());

        Ok(DbHeaderV3 {
            format_version,
            page_size,
            schema_cookie,
            root_catalog,
            root_freelist,
            freelist_head,
            freelist_count,
            last_checkpoint_lsn,
        })
    }

    fn read_page(&mut self, page_id: u32) -> Result<Vec<u8>> {
        if page_id == 0 {
            return Err(anyhow!("Invalid page ID 0"));
        }
        let offset = (page_id as u64 - 1) * (self.header.page_size as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; self.header.page_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn get_leftmost_leaf(&mut self, root_page: u32) -> Result<u32> {
        if root_page == 0 {
            return Ok(0);
        }
        let mut current = root_page;
        loop {
            let page = self.read_page(current)?;
            let page_type = page[0];
            if page_type == 2 {
                return Ok(current);
            } else if page_type == 1 {
                let count = u16::from_le_bytes(page[2..4].try_into().unwrap());
                if count == 0 {
                    let right_child = u32::from_le_bytes(page[4..8].try_into().unwrap());
                    current = right_child;
                } else {
                    let mut offset = 8;
                    let _ = decode_varint(&page, &mut offset)?;
                    let child = decode_varint(&page, &mut offset)?;
                    current = child as u32;
                }
            } else {
                return Err(anyhow!("Invalid BTree page type {}", page_type));
            }
        }
    }

    fn read_overflow_chain(&mut self, start_page: u32) -> Result<Vec<u8>> {
        let mut current = start_page;
        let mut output = Vec::new();
        while current != 0 {
            let page = self.read_page(current)?;
            let next = u32::from_le_bytes(page[0..4].try_into().unwrap());
            let chunk_len = u32::from_le_bytes(page[4..8].try_into().unwrap()) as usize;
            if 8 + chunk_len > page.len() {
                return Err(anyhow!("Overflow chunk length exceeds page size"));
            }
            output.extend_from_slice(&page[8..8 + chunk_len]);
            current = next;
        }
        Ok(output)
    }

    pub fn scan_btree(&mut self, root_page: u32) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut current = self.get_leftmost_leaf(root_page)?;
        let mut results = Vec::new();

        while current != 0 {
            let page = self.read_page(current)?;
            if page[0] != 2 {
                return Err(anyhow!("Expected leaf page while scanning BTree"));
            }
            let delta_encoded = page[1] == 1;
            let count = u16::from_le_bytes(page[2..4].try_into().unwrap());
            let next_leaf = u32::from_le_bytes(page[4..8].try_into().unwrap());

            let mut offset = 8;
            let mut prev_key = 0;

            for _ in 0..count {
                let k = decode_varint(&page, &mut offset)?;
                let key = if delta_encoded { prev_key + k } else { k };
                prev_key = key;

                let control = decode_varint(&page, &mut offset)?;
                let is_overflow = (control & 1) != 0;
                let val = (control >> 1) as u32;

                if is_overflow {
                    let overflow_page = val;
                    let payload = self.read_overflow_chain(overflow_page)?;
                    results.push((key, payload));
                } else {
                    let value_len = val as usize;
                    if offset + value_len > page.len() {
                        return Err(anyhow!("Leaf inline value exceeds page bounds"));
                    }
                    let payload = page[offset..offset + value_len].to_vec();
                    offset += value_len;
                    results.push((key, payload));
                }
            }

            current = next_leaf;
        }

        Ok(results)
    }

    pub fn migrate_into(&mut self, dest_db: &decentdb::Db) -> Result<()> {
        println!(
            "Extracting schema and data from Version 3 database at {}...",
            self.source_path.display()
        );

        let catalog_records = self.scan_btree(self.header.root_catalog)?;
        let mut table_roots = Vec::new();
        let mut index_sqls = Vec::new();
        let mut view_sqls = Vec::new();
        let mut trigger_sqls = Vec::new();

        for (_key, payload) in catalog_records {
            let values = decode_record(self, &payload)?;
            if values.is_empty() {
                continue;
            }

            let mut is_compact = false;
            let mut record_type = String::new();

            if let ValueV3::Text(ref t) = values[0] {
                let lower = t.to_lowercase();
                if ![
                    "table",
                    "index",
                    "view",
                    "trigger",
                    "stats:table",
                    "stats:index",
                ]
                .contains(&lower.as_str())
                {
                    if values.len() == 4 {
                        is_compact = true;
                        record_type = "table".to_string();
                    }
                } else {
                    record_type = lower;
                }
            }

            match record_type.as_str() {
                "table" => {
                    let (name, root_page, columns_str, checks_str) = if is_compact {
                        let name = match &values[0] {
                            ValueV3::Text(t) => t.clone(),
                            ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                            _ => continue,
                        };
                        let root_page = match &values[1] {
                            ValueV3::Int64(i) => *i as u32,
                            _ => continue,
                        };
                        let columns_str = match &values[3] {
                            ValueV3::Text(t) => t.clone(),
                            ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                            _ => continue,
                        };
                        (name, root_page, columns_str, String::new())
                    } else {
                        if values.len() < 5 {
                            continue;
                        }
                        let name = match &values[1] {
                            ValueV3::Text(t) => t.clone(),
                            ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                            _ => continue,
                        };
                        let root_page = match &values[2] {
                            ValueV3::Int64(i) => *i as u32,
                            _ => continue,
                        };
                        let columns_str = match &values[4] {
                            ValueV3::Text(t) => t.clone(),
                            ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                            _ => continue,
                        };
                        let checks_str = if values.len() >= 6 {
                            match &values[5] {
                                ValueV3::Text(t) => t.clone(),
                                ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                                _ => String::new(),
                            }
                        } else {
                            String::new()
                        };
                        (name, root_page, columns_str, checks_str)
                    };
                    table_roots.push((name, root_page, columns_str, checks_str));
                }
                "index" => {
                    if values.len() < 5 {
                        continue;
                    }
                    let name = match &values[1] {
                        ValueV3::Text(t) => t.clone(),
                        ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => continue,
                    };
                    let table_name = match &values[2] {
                        ValueV3::Text(t) => t.clone(),
                        ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => continue,
                    };
                    let columns_str = match &values[3] {
                        ValueV3::Text(t) => t.clone(),
                        ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => continue,
                    };

                    let mut unique = false;
                    let mut predicate = String::new();
                    if values.len() >= 7 {
                        unique = match &values[6] {
                            ValueV3::Int64(i) => *i != 0,
                            _ => false,
                        };
                    }
                    if values.len() >= 8 {
                        predicate = match &values[7] {
                            ValueV3::Text(t) => t.clone(),
                            ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                            _ => String::new(),
                        };
                    }

                    let columns: Vec<&str> =
                        columns_str.split(';').filter(|s| !s.is_empty()).collect();
                    let cols_sql = columns
                        .iter()
                        .map(|c| {
                            let parts: Vec<&str> = c.split(':').collect();
                            let col = parts[0];
                            let desc = if parts.len() > 1 && parts[1] == "desc" {
                                " DESC"
                            } else {
                                ""
                            };
                            format!("\"{}\"{}", col, desc)
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    let prefix = if unique {
                        "CREATE UNIQUE INDEX"
                    } else {
                        "CREATE INDEX"
                    };
                    let mut sql = format!(
                        "{} \"{}\" ON \"{}\" ({})",
                        prefix, name, table_name, cols_sql
                    );
                    if !predicate.is_empty() {
                        sql.push_str(&format!(" WHERE {}", predicate));
                    }
                    index_sqls.push(sql);
                }
                "view" => {
                    if values.len() < 3 {
                        continue;
                    }
                    let name = match &values[1] {
                        ValueV3::Text(t) => t.clone(),
                        ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => continue,
                    };
                    let sql_text = match &values[2] {
                        ValueV3::Text(t) => t.clone(),
                        ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => continue,
                    };
                    view_sqls.push(format!("CREATE VIEW \"{}\" AS {}", name, sql_text));
                }
                "trigger" => {
                    if values.len() < 5 {
                        continue;
                    }
                    let name = match &values[1] {
                        ValueV3::Text(t) => t.clone(),
                        ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => continue,
                    };
                    let table_name = match &values[2] {
                        ValueV3::Text(t) => t.clone(),
                        ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => continue,
                    };
                    let events_mask = match &values[3] {
                        ValueV3::Int64(i) => *i,
                        _ => continue,
                    };
                    let action_sql = match &values[4] {
                        ValueV3::Text(t) => t.clone(),
                        ValueV3::Blob(b) => String::from_utf8_lossy(b).into_owned(),
                        _ => continue,
                    };

                    // Format the action SQL to be compatible with DecentDB's current parser
                    let formatted_action = if action_sql
                        .trim_start()
                        .to_uppercase()
                        .starts_with("EXECUTE FUNCTION")
                    {
                        action_sql.clone()
                    } else {
                        let escaped_sql = action_sql.replace('\'', "''");
                        format!(
                            "FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('{}')",
                            escaped_sql
                        )
                    };

                    let mut events = Vec::new();
                    if (events_mask & 4) != 0 {
                        events.push("INSERT");
                    }
                    if (events_mask & 8) != 0 {
                        events.push("DELETE");
                    }
                    if (events_mask & 16) != 0 {
                        events.push("UPDATE");
                    }
                    let event_str = events.join(" OR ");
                    let timing = if (events_mask & 64) != 0 {
                        "INSTEAD OF"
                    } else {
                        "AFTER"
                    };

                    trigger_sqls.push(format!(
                        "CREATE TRIGGER \"{}\" {} {} ON \"{}\" {}",
                        name, timing, event_str, table_name, formatted_action
                    ));
                }
                _ => {}
            }
        }

        // Execute schema
        dest_db.execute("BEGIN")?;

        let mut total_records = 0;
        let total_tables = table_roots.len();
        println!(
            "Found {} tables, {} indexes, {} views, {} triggers to migrate.",
            total_tables,
            index_sqls.len(),
            view_sqls.len(),
            trigger_sqls.len()
        );

        for (i, (table_name, root_page, columns_str, checks_str)) in table_roots.iter().enumerate()
        {
            let create_sql = build_create_table(table_name, columns_str, checks_str)?;
            dest_db.execute(&create_sql)?;

            let rows = self.scan_btree(*root_page)?;
            let row_count = rows.len();
            println!(
                "Migrating table '{}' ({} of {}) - {} rows...",
                table_name,
                i + 1,
                total_tables,
                row_count
            );

            let mut dot_count = 0;
            for (j, (_key, payload)) in rows.into_iter().enumerate() {
                let values = decode_record(self, &payload)?;
                let params = map_values_to_params(&values);
                let placeholders = (1..=params.len())
                    .map(|i| format!("${}", i))
                    .collect::<Vec<_>>()
                    .join(", ");
                let insert_sql =
                    format!("INSERT INTO \"{}\" VALUES ({})", table_name, placeholders);
                dest_db.execute_with_params(&insert_sql, &params)?;

                if (j + 1) % 100_000 == 0 {
                    use std::io::Write;
                    print!(".");
                    let _ = std::io::stdout().flush();
                    dot_count += 1;
                }
            }
            if dot_count > 0 {
                println!();
            }
            total_records += row_count;
        }

        if !index_sqls.is_empty() {
            println!("Applying {} indexes...", index_sqls.len());
        }
        for sql in &index_sqls {
            dest_db.execute(sql)?;
        }

        if !view_sqls.is_empty() {
            println!("Applying {} views...", view_sqls.len());
        }
        for sql in &view_sqls {
            dest_db.execute(sql)?;
        }

        if !trigger_sqls.is_empty() {
            println!("Applying {} triggers...", trigger_sqls.len());
        }
        for sql in &trigger_sqls {
            dest_db.execute(sql)?;
        }

        dest_db.execute("COMMIT")?;

        println!("\nMigration summary:");
        println!("  - {} tables migrated", total_tables);
        println!("  - {} total records migrated", total_records);
        println!("  - {} indexes recreated", index_sqls.len());
        println!("  - {} views recreated", view_sqls.len());
        println!("  - {} triggers recreated", trigger_sqls.len());

        Ok(())
    }
}

fn url_decode(s: &str) -> String {
    s.replace("%2C", ",")
        .replace("%3A", ":")
        .replace("%3B", ";")
        .replace("%25", "%")
}

fn build_create_table(name: &str, columns_str: &str, checks_str: &str) -> Result<String> {
    let mut parts = Vec::new();
    let mut pk_cols = Vec::new();

    for part in columns_str.split(';') {
        if part.is_empty() {
            continue;
        }
        let pieces: Vec<&str> = part.splitn(3, ':').collect();
        if pieces.len() >= 2 {
            let col_name = pieces[0];
            let type_str = pieces[1];
            let mut col_sql = format!("\"{}\" {}", col_name, type_str);

            if pieces.len() >= 3 {
                for flag in pieces[2].split(',') {
                    if flag == "nn" {
                        col_sql.push_str(" NOT NULL");
                    } else if flag == "unique" {
                        col_sql.push_str(" UNIQUE");
                    } else if flag == "pk" {
                        pk_cols.push(col_name.to_string());
                    } else if let Some(stripped) = flag.strip_prefix("ref=") {
                        let p: Vec<&str> = stripped.split('.').collect();
                        if p.len() == 2 {
                            col_sql.push_str(&format!(" REFERENCES \"{}\"(\"{}\")", p[0], p[1]));
                        }
                    } else if let Some(stripped) = flag.strip_prefix("refdel=") {
                        let action = match stripped {
                            "r" => "RESTRICT",
                            "c" => "CASCADE",
                            "n" => "SET NULL",
                            _ => "NO ACTION",
                        };
                        col_sql.push_str(&format!(" ON DELETE {}", action));
                    } else if let Some(stripped) = flag.strip_prefix("refupd=") {
                        let action = match stripped {
                            "r" => "RESTRICT",
                            "c" => "CASCADE",
                            "n" => "SET NULL",
                            _ => "NO ACTION",
                        };
                        col_sql.push_str(&format!(" ON UPDATE {}", action));
                    } else if let Some(stripped) = flag.strip_prefix("default=") {
                        col_sql.push_str(&format!(" DEFAULT {}", url_decode(stripped)));
                    } else if let Some(stripped) = flag.strip_prefix("gen=") {
                        col_sql.push_str(&format!(
                            " GENERATED ALWAYS AS ({}) STORED",
                            url_decode(stripped)
                        ));
                    }
                }
            }
            parts.push(col_sql);
        }
    }

    if !pk_cols.is_empty() {
        let pks = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(format!("PRIMARY KEY ({})", pks));
    }

    if !checks_str.is_empty() {
        if let Ok(checks_json) = serde_json::from_str::<JsonValue>(checks_str) {
            if let Some(arr) = checks_json.as_array() {
                for check in arr {
                    if let Some(expr) = check.get("expr").and_then(|e| e.as_str()) {
                        if let Some(n) = check.get("name").and_then(|n| n.as_str()) {
                            if !n.is_empty() {
                                parts.push(format!("CONSTRAINT \"{}\" CHECK ({})", n, expr));
                                continue;
                            }
                        }
                        parts.push(format!("CHECK ({})", expr));
                    }
                }
            }
        }
    }

    Ok(format!("CREATE TABLE \"{}\" ({})", name, parts.join(", ")))
}

fn map_values_to_params(values: &[ValueV3]) -> Vec<decentdb::Value> {
    values
        .iter()
        .map(|v| match v {
            ValueV3::Null => decentdb::Value::Null,
            ValueV3::Int64(i) => decentdb::Value::Int64(*i),
            ValueV3::Bool(b) => decentdb::Value::Bool(*b),
            ValueV3::Float64(f) => decentdb::Value::Float64(*f),
            ValueV3::Text(t) => decentdb::Value::Text(t.clone()),
            ValueV3::Blob(b) => decentdb::Value::Blob(b.clone()),
            ValueV3::Decimal { scale, value } => decentdb::Value::Decimal {
                scale: *scale,
                scaled: *value,
            },
            ValueV3::DateTime(dt) => decentdb::Value::TimestampMicros(*dt),
        })
        .collect()
}

fn zigzag_decode(n: u64) -> i64 {
    let shifted = n >> 1;
    if (n & 1) == 0 {
        shifted as i64
    } else {
        (shifted ^ (!0u64)) as i64
    }
}

pub fn decode_varint(data: &[u8], offset: &mut usize) -> Result<u64> {
    let mut shift = 0;
    let mut value: u64 = 0;
    while *offset < data.len() {
        let b = data[*offset];
        *offset += 1;
        value |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift > 63 {
            return Err(anyhow!("Varint overflow"));
        }
    }
    Err(anyhow!("Unexpected end of varint"))
}

pub fn decompress_data(data: &[u8]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let mut decoder = ZlibDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

pub fn decode_record(reader: &mut V3Reader, data: &[u8]) -> Result<Vec<ValueV3>> {
    let mut offset = 0;
    let count = decode_varint(data, &mut offset)? as usize;
    let mut values = Vec::with_capacity(count);

    for _ in 0..count {
        if offset >= data.len() {
            return Err(anyhow!("Unexpected end of record"));
        }
        let kind_value = data[offset];
        offset += 1;

        let length = decode_varint(data, &mut offset)? as usize;
        if offset + length > data.len() {
            return Err(anyhow!("Record field length out of bounds"));
        }

        let payload = &data[offset..offset + length];
        offset += length;

        let value = match kind_value {
            0 => ValueV3::Null,
            1 => {
                let mut p_offset = 0;
                let v = decode_varint(payload, &mut p_offset)?;
                ValueV3::Int64(zigzag_decode(v))
            }
            2 => {
                if payload.len() != 1 {
                    return Err(anyhow!("Invalid BOOL length"));
                }
                ValueV3::Bool(payload[0] != 0)
            }
            3 => {
                if payload.len() != 8 {
                    return Err(anyhow!("Invalid FLOAT64 length"));
                }
                let bits = u64::from_le_bytes(payload.try_into().unwrap());
                ValueV3::Float64(f64::from_bits(bits))
            }
            4 => ValueV3::Text(String::from_utf8_lossy(payload).into_owned()),
            5 => ValueV3::Blob(payload.to_vec()),
            6 => {
                if payload.len() != 8 {
                    return Err(anyhow!("Invalid overflow pointer length"));
                }
                let page_id = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                let bytes = reader.read_overflow_chain(page_id)?;
                ValueV3::Text(String::from_utf8_lossy(&bytes).into_owned())
            }
            7 => {
                if payload.len() != 8 {
                    return Err(anyhow!("Invalid overflow pointer length"));
                }
                let page_id = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                let bytes = reader.read_overflow_chain(page_id)?;
                ValueV3::Blob(bytes)
            }
            8 => {
                let bytes = decompress_data(payload)?;
                ValueV3::Text(String::from_utf8_lossy(&bytes).into_owned())
            }
            9 => {
                let bytes = decompress_data(payload)?;
                ValueV3::Blob(bytes)
            }
            10 => {
                if payload.len() != 8 {
                    return Err(anyhow!("Invalid overflow pointer length"));
                }
                let page_id = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                let compressed = reader.read_overflow_chain(page_id)?;
                let bytes = decompress_data(&compressed)?;
                ValueV3::Text(String::from_utf8_lossy(&bytes).into_owned())
            }
            11 => {
                if payload.len() != 8 {
                    return Err(anyhow!("Invalid overflow pointer length"));
                }
                let page_id = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                let compressed = reader.read_overflow_chain(page_id)?;
                let bytes = decompress_data(&compressed)?;
                ValueV3::Blob(bytes)
            }
            12 => {
                if payload.len() < 2 {
                    return Err(anyhow!("Invalid DECIMAL length"));
                }
                let scale = payload[0];
                let mut p_offset = 1;
                let v = decode_varint(payload, &mut p_offset)?;
                ValueV3::Decimal {
                    scale,
                    value: zigzag_decode(v),
                }
            }
            13 => ValueV3::Bool(false),
            14 => ValueV3::Bool(true),
            15 => ValueV3::Int64(0),
            16 => ValueV3::Int64(1),
            17 => {
                let mut p_offset = 0;
                let v = decode_varint(payload, &mut p_offset)?;
                ValueV3::DateTime(zigzag_decode(v))
            }
            _ => return Err(anyhow!("Unknown value kind: {}", kind_value)),
        };
        values.push(value);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use decentdb::{Db, DbConfig};
    use tempfile::tempdir;

    #[test]
    fn test_v3_migration() {
        let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("v3_fixture.db");

        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("migrated.db");

        let dest_db = Db::open_or_create(&dest_path, DbConfig::default()).unwrap();
        let mut reader = V3Reader::new(&fixture_path).unwrap();

        reader.migrate_into(&dest_db).unwrap();

        // Now verify the dest_db has everything we expect!

        // 1. Check Tables
        let users_res = dest_db
            .execute("SELECT id, name FROM users ORDER BY id")
            .unwrap();
        assert_eq!(users_res.rows().len(), 2);

        // row 1
        assert_eq!(users_res.rows()[0].values()[0], decentdb::Value::Int64(1));
        assert_eq!(
            users_res.rows()[0].values()[1],
            decentdb::Value::Text("Alice".to_string())
        );

        // row 2
        assert_eq!(users_res.rows()[1].values()[0], decentdb::Value::Int64(2));
        assert_eq!(
            users_res.rows()[1].values()[1],
            decentdb::Value::Text("Bob".to_string())
        );

        // Check long text overflow
        let long_res = dest_db.execute("SELECT bio FROM users WHERE id=2").unwrap();
        if let decentdb::Value::Text(bio) = &long_res.rows()[0].values()[0] {
            assert_eq!(bio.len(), 10000);
            assert!(bio.chars().all(|c| c == 'A'));
        } else {
            panic!("Expected Text for bio");
        }

        // Check blob
        let blob_res = dest_db
            .execute("SELECT avatar FROM users WHERE id=1")
            .unwrap();
        assert_eq!(
            blob_res.rows()[0].values()[0],
            decentdb::Value::Blob(vec![1, 2, 3])
        );

        // Check Decimal
        let dec_res = dest_db
            .execute("SELECT balance FROM users WHERE id=1")
            .unwrap();
        assert_eq!(
            dec_res.rows()[0].values()[0],
            decentdb::Value::Decimal {
                scaled: 10050,
                scale: 2
            }
        );

        // Check DateTime
        let dt_res = dest_db
            .execute("SELECT created_at FROM users WHERE id=1")
            .unwrap();
        assert_eq!(
            dt_res.rows()[0].values()[0],
            decentdb::Value::TimestampMicros(1672574400000)
        );
    }
}
