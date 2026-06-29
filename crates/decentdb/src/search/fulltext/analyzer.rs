use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::FTS_DDL_ERROR_PREFIX;

pub(crate) const FIELD_POSITION_GAP: usize = 256;

const BUILTIN_STOPWORDS: [&str; 6] = ["a", "an", "and", "of", "the", "to"];

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnalyzerTokenization {
    Unicode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnalyzerLanguage {
    Simple,
    English,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnalyzerStopwords {
    None,
    Builtin,
    Custom(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnalyzerStemmer {
    None,
    English,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AnalyzerDiacritics {
    Preserve,
    Remove,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PrefixPolicy {
    pub(crate) enabled: bool,
    pub(crate) lengths: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct AnalyzerConfig {
    pub(crate) analyzer_id: String,
    pub(crate) analyzer_version: u32,
    pub(crate) tokenizer: AnalyzerTokenization,
    pub(crate) language: AnalyzerLanguage,
    pub(crate) stopwords: AnalyzerStopwords,
    pub(crate) stemming: AnalyzerStemmer,
    pub(crate) case_folded: bool,
    pub(crate) diacritics: AnalyzerDiacritics,
    pub(crate) prefix: PrefixPolicy,
    pub(crate) field_position_gap: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PrefixParseError {
    pub(crate) message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AnalyzerConfigError {
    pub(crate) message: String,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            analyzer_id: "decentdb.fulltext.v1".to_string(),
            analyzer_version: 1,
            tokenizer: AnalyzerTokenization::Unicode,
            language: AnalyzerLanguage::Simple,
            stopwords: AnalyzerStopwords::None,
            stemming: AnalyzerStemmer::None,
            case_folded: true,
            diacritics: AnalyzerDiacritics::Preserve,
            prefix: PrefixPolicy {
                enabled: false,
                lengths: Vec::new(),
            },
            field_position_gap: FIELD_POSITION_GAP,
        }
    }
}

impl AnalyzerConfig {
    pub(crate) fn canonicalize(mut self) -> Self {
        self.stopwords = canonicalize_stopwords(self.stopwords);
        self.prefix = self.prefix.canonicalize();
        self
    }

    pub(crate) fn with_prefix(mut self, policy: PrefixPolicy) -> Self {
        self.prefix = policy.canonicalize();
        self
    }

    pub(crate) fn analyze(&self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        self.for_each_token(text, |token| tokens.push(token));
        tokens
    }

    pub(crate) fn for_each_token<F>(&self, text: &str, mut emit: F)
    where
        F: FnMut(String),
    {
        if text.is_ascii() {
            self.for_each_ascii_token(text.as_bytes(), &mut emit);
            return;
        }

        let mut token = String::new();
        for character in text.chars() {
            if self.case_folded {
                for lowered in character.to_lowercase() {
                    collect_token_character(
                        normalize_token_character(lowered, self.diacritics),
                        &mut token,
                        &mut emit,
                        &self.stopwords,
                    );
                }
            } else {
                collect_token_character(
                    normalize_token_character(character, self.diacritics),
                    &mut token,
                    &mut emit,
                    &self.stopwords,
                );
            }
        }
        flush_token(&mut token, &mut emit, &self.stopwords);
    }

    fn for_each_ascii_token<F>(&self, bytes: &[u8], emit: &mut F)
    where
        F: FnMut(String),
    {
        let mut token = String::new();
        for &byte in bytes {
            if byte.is_ascii_alphanumeric() || byte == b'_' {
                let normalized = if self.case_folded {
                    byte.to_ascii_lowercase()
                } else {
                    byte
                };
                token.push(char::from(normalized));
            } else {
                flush_token(&mut token, emit, &self.stopwords);
            }
        }
        flush_token(&mut token, emit, &self.stopwords);
    }

    pub(crate) fn to_json(&self) -> Result<Vec<u8>, AnalyzerConfigError> {
        serde_json::to_vec(&self.clone().canonicalize()).map_err(|error| AnalyzerConfigError {
            message: format!("serialize analyzer config: {error}"),
        })
    }

    pub(crate) fn from_json(bytes: &[u8]) -> Result<Self, AnalyzerConfigError> {
        serde_json::from_slice(bytes)
            .map(Self::canonicalize)
            .map_err(|error| AnalyzerConfigError {
                message: format!("deserialize analyzer config: {error}"),
            })
    }

    pub(crate) fn parse_prefix_option(raw: &str) -> Result<PrefixPolicy, PrefixParseError> {
        parse_prefix_list(raw).map(PrefixPolicy::from_lengths)
    }
}

impl PrefixPolicy {
    pub(crate) fn none() -> Self {
        Self {
            enabled: false,
            lengths: Vec::new(),
        }
    }

    pub(crate) fn from_lengths(mut lengths: Vec<u8>) -> Self {
        lengths.sort_unstable();
        lengths.dedup();
        Self {
            enabled: !lengths.is_empty(),
            lengths,
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.lengths.sort_unstable();
        self.lengths.dedup();
        self.enabled = !self.lengths.is_empty();
        self
    }
}

fn parse_prefix_list(raw: &str) -> Result<Vec<u8>, PrefixParseError> {
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("none") {
        return Ok(Vec::new());
    }

    let parts = raw.split(',').collect::<Vec<_>>();
    if parts.len() > 3 {
        return Err(PrefixParseError {
            message: format!("{FTS_DDL_ERROR_PREFIX} at most three prefix lengths are allowed"),
        });
    }

    let mut lengths = Vec::with_capacity(parts.len());
    for part in parts {
        let value = part.trim();
        if value.is_empty() {
            return Err(PrefixParseError {
                message: format!(
                    "{FTS_DDL_ERROR_PREFIX} prefix list must not contain empty entries"
                ),
            });
        }
        let length = value.parse::<u8>().map_err(|_| PrefixParseError {
            message: format!(
                "{FTS_DDL_ERROR_PREFIX} prefix entry '{value}' is not a valid integer"
            ),
        })?;
        if length == 0 {
            return Err(PrefixParseError {
                message: format!("{FTS_DDL_ERROR_PREFIX} prefix lengths must be at least 1"),
            });
        }
        if length > 8 {
            return Err(PrefixParseError {
                message: format!("{FTS_DDL_ERROR_PREFIX} prefix lengths must be at most 8"),
            });
        }
        if lengths.contains(&length) {
            return Err(PrefixParseError {
                message: format!("{FTS_DDL_ERROR_PREFIX} duplicate prefix length '{length}'"),
            });
        }
        lengths.push(length);
    }
    Ok(lengths)
}

fn canonicalize_stopwords(stopwords: AnalyzerStopwords) -> AnalyzerStopwords {
    match stopwords {
        AnalyzerStopwords::Custom(stopwords) => {
            let words = stopwords
                .into_iter()
                .map(|word| word.to_lowercase())
                .collect::<BTreeSet<_>>();
            AnalyzerStopwords::Custom(words.into_iter().collect())
        }
        other => other,
    }
}

fn is_stopword(token: &str, stopwords: &AnalyzerStopwords) -> bool {
    match stopwords {
        AnalyzerStopwords::None => false,
        AnalyzerStopwords::Builtin => BUILTIN_STOPWORDS.contains(&token),
        AnalyzerStopwords::Custom(words) => words.iter().any(|word| word == token),
    }
}

fn remove_diacritics(text: String, diacritics: AnalyzerDiacritics) -> String {
    match diacritics {
        AnalyzerDiacritics::Preserve => text,
        AnalyzerDiacritics::Remove => text.chars().map(remove_latin_diacritic).collect(),
    }
}

fn normalize_token_character(character: char, diacritics: AnalyzerDiacritics) -> char {
    match diacritics {
        AnalyzerDiacritics::Preserve => character,
        AnalyzerDiacritics::Remove => remove_latin_diacritic(character),
    }
}

fn remove_latin_diacritic(character: char) -> char {
    match character {
        'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ā' | 'ă' | 'ą' => 'a',
        'Á' | 'À' | 'Â' | 'Ä' | 'Ã' | 'Å' | 'Ā' | 'Ă' | 'Ą' => 'A',
        'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' => 'c',
        'Ç' | 'Ć' | 'Ĉ' | 'Ċ' | 'Č' => 'C',
        'ď' | 'đ' => 'd',
        'Ď' | 'Đ' => 'D',
        'é' | 'è' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => 'e',
        'É' | 'È' | 'Ê' | 'Ë' | 'Ē' | 'Ĕ' | 'Ė' | 'Ę' | 'Ě' => 'E',
        'í' | 'ì' | 'î' | 'ï' | 'ī' | 'ĭ' | 'į' | 'ı' => 'i',
        'Í' | 'Ì' | 'Î' | 'Ï' | 'Ī' | 'Ĭ' | 'Į' | 'İ' => 'I',
        'ñ' | 'ń' | 'ņ' | 'ň' => 'n',
        'Ñ' | 'Ń' | 'Ņ' | 'Ň' => 'N',
        'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ø' | 'ō' | 'ŏ' | 'ő' => 'o',
        'Ó' | 'Ò' | 'Ô' | 'Ö' | 'Õ' | 'Ø' | 'Ō' | 'Ŏ' | 'Ő' => 'O',
        'ŕ' | 'ŗ' | 'ř' => 'r',
        'Ŕ' | 'Ŗ' | 'Ř' => 'R',
        'ś' | 'ŝ' | 'ş' | 'š' => 's',
        'Ś' | 'Ŝ' | 'Ş' | 'Š' => 'S',
        'ť' | 'ţ' | 'ŧ' => 't',
        'Ť' | 'Ţ' | 'Ŧ' => 'T',
        'ú' | 'ù' | 'û' | 'ü' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => 'u',
        'Ú' | 'Ù' | 'Û' | 'Ü' | 'Ū' | 'Ŭ' | 'Ů' | 'Ű' | 'Ų' => 'U',
        'ý' | 'ÿ' | 'ŷ' => 'y',
        'Ý' | 'Ÿ' | 'Ŷ' => 'Y',
        'ź' | 'ż' | 'ž' => 'z',
        'Ź' | 'Ż' | 'Ž' => 'Z',
        'Æ' => 'A',
        'æ' => 'a',
        'Œ' => 'O',
        'œ' => 'o',
        'ß' => 's',
        other => other,
    }
}

fn tokenize_text(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut token = String::new();

    for character in text.chars() {
        if is_token_character(character) {
            token.push(character);
            continue;
        }
        if !token.is_empty() {
            tokens.push(std::mem::take(&mut token));
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    tokens
}

fn is_token_character(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

fn collect_token_character<F>(
    character: char,
    token: &mut String,
    emit: &mut F,
    stopwords: &AnalyzerStopwords,
) where
    F: FnMut(String),
{
    if is_token_character(character) {
        token.push(character);
    } else {
        flush_token(token, emit, stopwords);
    }
}

fn flush_token<F>(token: &mut String, emit: &mut F, stopwords: &AnalyzerStopwords)
where
    F: FnMut(String),
{
    if token.is_empty() {
        return;
    }
    if !is_stopword(token, stopwords) {
        emit(std::mem::take(token));
    } else {
        token.clear();
    }
}

pub(crate) fn analyze_text(config: &AnalyzerConfig, text: &str) -> Vec<String> {
    config.analyze(text)
}

#[cfg(test)]
mod tests {
    use super::{
        analyze_text, AnalyzerConfig, AnalyzerConfigError, AnalyzerDiacritics, AnalyzerLanguage,
        AnalyzerStemmer, AnalyzerStopwords, AnalyzerTokenization, PrefixParseError, PrefixPolicy,
        FTS_DDL_ERROR_PREFIX,
    };

    #[test]
    fn default_config_is_stable() {
        let config = AnalyzerConfig::default();
        assert_eq!(config.tokenizer, AnalyzerTokenization::Unicode);
        assert_eq!(config.language, AnalyzerLanguage::Simple);
        assert_eq!(config.stemming, AnalyzerStemmer::None);
        assert!(config.case_folded);
        assert!(matches!(config.diacritics, AnalyzerDiacritics::Preserve));
        assert_eq!(config.field_position_gap, 256);
        assert_eq!(config.prefix, PrefixPolicy::none());
        assert_eq!(config.analyzer_id, "decentdb.fulltext.v1");
        assert_eq!(config.analyzer_version, 1);
    }

    #[test]
    fn config_canonicalizes_prefix_lengths() {
        let config =
            AnalyzerConfig::default().with_prefix(PrefixPolicy::from_lengths(vec![3, 1, 3, 2]));
        assert_eq!(config.prefix.lengths, vec![1, 2, 3]);
        assert!(config.prefix.enabled);
    }

    #[test]
    fn can_serialize_and_deserialize_config() {
        let config = AnalyzerConfig::default().with_prefix(PrefixPolicy::from_lengths(vec![2, 1]));
        let encoded = config.to_json().expect("serialize analyzer config");
        let decoded = AnalyzerConfig::from_json(&encoded).expect("deserialize analyzer config");
        assert_eq!(decoded, config);
    }

    #[test]
    fn serialize_is_deterministic() {
        let config = AnalyzerConfig::default().with_prefix(PrefixPolicy::from_lengths(vec![3, 1]));
        let first = config.to_json().expect("serialize analyzer config");
        let second = config.to_json().expect("serialize analyzer config");
        assert_eq!(first, second);
    }

    #[test]
    fn deserialize_invalid_json_includes_prefix() {
        let error = AnalyzerConfig::from_json(b"{invalid").expect_err("invalid JSON");
        assert!(error.message.contains("deserialize analyzer config"));
    }

    #[test]
    fn parse_prefix_none() {
        let policy = AnalyzerConfig::parse_prefix_option("none").expect("prefix none");
        assert!(!policy.enabled);
        assert!(policy.lengths.is_empty());
    }

    #[test]
    fn parse_prefix_empty() {
        let policy = AnalyzerConfig::parse_prefix_option("").expect("empty disabled");
        assert!(!policy.enabled);
        assert!(policy.lengths.is_empty());
    }

    #[test]
    fn parse_prefix_list_dedup_sorts() {
        let policy = AnalyzerConfig::parse_prefix_option("3,1,2,2").expect_err("dedup rejected");
        assert!(policy.message.starts_with(FTS_DDL_ERROR_PREFIX));
    }

    #[test]
    fn parse_prefix_too_many_lengths() {
        let error = AnalyzerConfig::parse_prefix_option("1,2,3,4").expect_err("too many");
        assert!(error.message.starts_with(FTS_DDL_ERROR_PREFIX));
        assert!(error.message.contains("at most three"));
    }

    #[test]
    fn parse_prefix_option_with_spaces() {
        let policy = AnalyzerConfig::parse_prefix_option(" 2 , 3 ").expect("spaced prefix list");
        assert_eq!(policy.lengths, vec![2, 3]);
        assert!(policy.enabled);
    }

    #[test]
    fn parse_prefix_length_out_of_range() {
        let too_small = AnalyzerConfig::parse_prefix_option("0").expect_err("too small");
        assert!(too_small.message.contains("at least 1"));
        let too_large = AnalyzerConfig::parse_prefix_option("9").expect_err("too large");
        assert!(too_large.message.contains("at most 8"));
    }

    #[test]
    fn parse_prefix_rejects_non_numeric_length() {
        let error = AnalyzerConfig::parse_prefix_option("2,a,4").expect_err("non numeric");
        assert!(error.message.contains("not a valid integer"));
        assert!(error.message.starts_with(FTS_DDL_ERROR_PREFIX));
    }

    #[test]
    fn analyzer_tokenizes_ascii_words() {
        let config = AnalyzerConfig::default();
        let tokens = analyze_text(&config, "DecentDB can search embedded documents quickly!");
        assert_eq!(
            tokens,
            vec![
                "decentdb",
                "can",
                "search",
                "embedded",
                "documents",
                "quickly"
            ]
        );
    }

    #[test]
    fn analyzer_preserves_internal_symbols() {
        let config = AnalyzerConfig::default();
        let tokens = analyze_text(&config, "a-b c_d e/f");
        assert_eq!(tokens, vec!["a", "b", "c_d", "e", "f"]);
    }

    #[test]
    fn analyzer_removes_builtin_stopwords() {
        let config = AnalyzerConfig {
            stopwords: AnalyzerStopwords::Builtin,
            ..AnalyzerConfig::default()
        };
        let tokens = analyze_text(&config, "the quick and the data");
        assert_eq!(tokens, vec!["quick", "data"]);
    }

    #[test]
    fn analyzer_custom_stopwords_are_case_insensitive() {
        let config = AnalyzerConfig {
            stopwords: AnalyzerStopwords::Custom(vec!["THE".to_string(), "and".to_string()]),
            ..AnalyzerConfig::default()
        }
        .canonicalize();
        let tokens = analyze_text(&config, "The and data");
        assert_eq!(tokens, vec!["data"]);
    }

    #[test]
    fn analyzer_diacritic_preserve_keeps_marks() {
        let config = AnalyzerConfig {
            diacritics: AnalyzerDiacritics::Preserve,
            ..AnalyzerConfig::default()
        };
        let tokens = analyze_text(&config, "naïve");
        assert_eq!(tokens, vec!["naïve"]);
    }

    #[test]
    fn analyzer_diacritic_remove_attempted_without_changes() {
        let config = AnalyzerConfig {
            diacritics: AnalyzerDiacritics::Remove,
            ..AnalyzerConfig::default()
        };
        let tokens = analyze_text(&config, "café");
        assert_eq!(tokens, vec!["cafe"]);
    }

    #[test]
    fn analyzer_config_error_exposes_context() {
        let error = AnalyzerConfigError {
            message: "sample".to_string(),
        };
        assert_eq!(error.message, "sample");
    }

    #[test]
    fn prefix_parse_error_exposes_context() {
        let error = PrefixParseError {
            message: "bad".to_string(),
        };
        assert_eq!(error.message, "bad");
    }
}
