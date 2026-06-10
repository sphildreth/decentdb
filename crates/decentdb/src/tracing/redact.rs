/// Redaction of SQL text for trace capture.
///
/// Phase 1 supports `None` and `Full`. `Template` and `Redacted` require a
/// parser-backed redactor and will be added once the redaction path is
/// benchmarked and tested.
pub(crate) fn redact_sql(
    sql: &str,
    mode: crate::tracing::config::SqlTextMode,
    max_chars: usize,
) -> String {
    use crate::tracing::config::SqlTextMode;
    match mode {
        SqlTextMode::None => String::new(),
        SqlTextMode::Full => {
            let limit = max_chars.max(1);
            if sql.chars().count() > limit {
                let trunc: String = sql.chars().take(limit).collect();
                format!("{}…", trunc)
            } else {
                sql.to_string()
            }
        }
        SqlTextMode::Template | SqlTextMode::Redacted => {
            // Phase 1 fallback: hash of the SQL after trimming.
            // We do not capture raw SQL unless mode is Full.
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            sql.hash(&mut hasher);
            format!("fingerprint:{}", hasher.finish())
        }
    }
}

/// Strip parameter values from a SQL string for fingerprinting.
pub(crate) fn sql_fingerprint(sql: &str) -> String {
    // Phase 1: trim and lowercase for a crude fingerprint.
    // This is intentionally cheap and avoids parser overhead.
    let trimmed = sql.trim();
    if trimmed.len() > 256 {
        let end = trimmed
            .char_indices()
            .nth(256)
            .map(|(i, _)| i)
            .unwrap_or(trimmed.len());
        trimmed[..end].to_ascii_lowercase()
    } else {
        trimmed.to_ascii_lowercase()
    }
}
