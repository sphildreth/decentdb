//! Full-text analyzer, query parser, scoring, and runtime index primitives.

use std::collections::{BTreeMap, BTreeSet};

pub(crate) const FTS_DDL_ERROR_PREFIX: &str = "FTS DDL error:";
pub(crate) const FTS_QUERY_ERROR_PREFIX: &str = "FTS query error:";
pub(crate) const FTS_SEMANTIC_ERROR_PREFIX: &str = "FTS semantic error:";

pub(crate) mod analyzer;
pub(crate) mod query;
pub(crate) mod ranking;

pub(crate) use analyzer::{
    AnalyzerConfig, AnalyzerDiacritics, AnalyzerStopwords, AnalyzerTokenization,
};
use query::{parse_fts_query, FtsQuery, FtsQueryTerm, FtsQueryTermKind};
use ranking::{bm25_score, Bm25Context, Bm25DocumentStats, Bm25TermStats};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FullTextIndexError {
    pub(crate) message: String,
}

#[derive(Clone, Debug)]
pub(crate) struct FullTextSearchHit {
    pub(crate) row_id: u64,
    pub(crate) score: f64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct TermDocumentInfo {
    frequency: u32,
    positions: Vec<u32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct FullTextDocument {
    doc_len: u32,
    terms: BTreeMap<String, TermDocumentInfo>,
}

#[derive(Clone, Debug)]
pub(crate) struct FullTextIndex {
    config: AnalyzerConfig,
    documents: BTreeMap<u64, FullTextDocument>,
    postings: BTreeMap<String, BTreeMap<u64, TermDocumentInfo>>,
    non_empty_document_count: u64,
    total_document_len: u64,
}

impl FullTextIndex {
    #[must_use]
    pub(crate) fn new(config: AnalyzerConfig) -> Self {
        Self {
            config: config.canonicalize(),
            documents: BTreeMap::new(),
            postings: BTreeMap::new(),
            non_empty_document_count: 0,
            total_document_len: 0,
        }
    }

    #[must_use]
    pub(crate) fn config(&self) -> &AnalyzerConfig {
        &self.config
    }

    pub(crate) fn insert_document(&mut self, row_id: u64, fields: &[Option<&str>]) {
        self.delete_document(row_id);
        let document = build_document(&self.config, fields);
        if document.doc_len > 0 {
            self.non_empty_document_count += 1;
            self.total_document_len += u64::from(document.doc_len);
        }
        for (term, info) in &document.terms {
            self.postings
                .entry(term.clone())
                .or_default()
                .insert(row_id, info.clone());
        }
        self.documents.insert(row_id, document);
    }

    pub(crate) fn delete_document(&mut self, row_id: u64) {
        let Some(document) = self.documents.remove(&row_id) else {
            return;
        };
        if document.doc_len > 0 {
            self.non_empty_document_count = self.non_empty_document_count.saturating_sub(1);
            self.total_document_len = self
                .total_document_len
                .saturating_sub(u64::from(document.doc_len));
        }
        for term in document.terms.keys() {
            let remove_term = if let Some(rows) = self.postings.get_mut(term) {
                rows.remove(&row_id);
                rows.is_empty()
            } else {
                false
            };
            if remove_term {
                self.postings.remove(term);
            }
        }
    }

    pub(crate) fn replace_document(&mut self, row_id: u64, fields: &[Option<&str>]) {
        self.insert_document(row_id, fields);
    }

    pub(crate) fn matches_query(
        &self,
        row_id: u64,
        query_text: &str,
    ) -> Result<bool, FullTextIndexError> {
        let query = parse_runtime_query(&self.config, query_text)?;
        Ok(self
            .documents
            .get(&row_id)
            .is_some_and(|document| query_matches_document(self, document, &query)))
    }

    pub(crate) fn score_query(
        &self,
        row_id: u64,
        query_text: &str,
    ) -> Result<f64, FullTextIndexError> {
        let query = parse_runtime_query(&self.config, query_text)?;
        let Some(document) = self.documents.get(&row_id) else {
            return Ok(0.0);
        };
        Ok(self.score_parsed_query(document, &query))
    }

    pub(crate) fn search(
        &self,
        query_text: &str,
    ) -> Result<Vec<FullTextSearchHit>, FullTextIndexError> {
        let query = parse_runtime_query(&self.config, query_text)?;
        let mut hits = Vec::new();
        for (row_id, document) in &self.documents {
            if query_matches_document(self, document, &query) {
                hits.push(FullTextSearchHit {
                    row_id: *row_id,
                    score: self.score_parsed_query(document, &query),
                });
            }
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.row_id.cmp(&right.row_id))
        });
        Ok(hits)
    }

    #[must_use]
    pub(crate) fn entry_count(&self) -> usize {
        self.documents.len()
    }

    #[must_use]
    pub(crate) fn term_count(&self) -> usize {
        self.postings.len()
    }

    #[must_use]
    pub(crate) fn average_document_len(&self) -> f64 {
        if self.non_empty_document_count == 0 {
            0.0
        } else {
            self.total_document_len as f64 / self.non_empty_document_count as f64
        }
    }

    fn score_parsed_query(&self, document: &FullTextDocument, query: &FtsQuery) -> f64 {
        let terms = positive_scoring_terms(self, query)
            .into_iter()
            .filter_map(|term| {
                let term_info = document.terms.get(&term)?;
                let doc_freq = self.postings.get(&term).map_or(0_usize, BTreeMap::len);
                Some(Bm25TermStats {
                    term_freq: f64::from(term_info.frequency),
                    doc_freq: doc_freq as f64,
                })
            })
            .collect::<Vec<_>>();
        bm25_score(
            &Bm25Context {
                corpus_size: self.non_empty_document_count as f64,
                avg_doc_len: self.average_document_len(),
                ..Bm25Context::default()
            },
            &Bm25DocumentStats {
                doc_len: f64::from(document.doc_len),
            },
            &terms,
        )
    }
}

fn parse_runtime_query(
    config: &AnalyzerConfig,
    query_text: &str,
) -> Result<FtsQuery, FullTextIndexError> {
    let query = parse_fts_query(query_text).map_err(|error| FullTextIndexError {
        message: error.message,
    })?;
    validate_prefix_terms(config, &query)?;
    Ok(query)
}

fn validate_prefix_terms(
    config: &AnalyzerConfig,
    query: &FtsQuery,
) -> Result<(), FullTextIndexError> {
    for term in query.clauses.iter().flatten() {
        if term.kind != FtsQueryTermKind::Prefix {
            continue;
        }
        let normalized = config.analyze(&term.text);
        let Some(prefix) = normalized.first() else {
            return Err(query_error("prefix term analyzes to no tokens"));
        };
        if normalized.len() != 1 {
            return Err(query_error("prefix term must analyze to exactly one token"));
        }
        if !config.prefix.enabled {
            return Err(query_error(
                "prefix queries require a fulltext index created with prefix lengths",
            ));
        }
        if !config
            .prefix
            .lengths
            .iter()
            .any(|length| usize::from(*length) <= prefix.chars().count())
        {
            return Err(query_error(
                "prefix term is shorter than every configured prefix length",
            ));
        }
    }
    Ok(())
}

fn query_error(message: &str) -> FullTextIndexError {
    FullTextIndexError {
        message: format!("{FTS_QUERY_ERROR_PREFIX} {message}"),
    }
}

fn build_document(config: &AnalyzerConfig, fields: &[Option<&str>]) -> FullTextDocument {
    let mut document = FullTextDocument::default();
    let mut position = 0_u32;
    let field_gap = u32::try_from(config.field_position_gap).unwrap_or(u32::MAX / 2);

    for (field_index, field) in fields.iter().enumerate() {
        if field_index > 0 && document.doc_len > 0 {
            position = position.saturating_add(field_gap);
        }
        let Some(text) = field else {
            continue;
        };
        for token in config.analyze(text) {
            let info = document.terms.entry(token).or_default();
            info.frequency = info.frequency.saturating_add(1);
            info.positions.push(position);
            document.doc_len = document.doc_len.saturating_add(1);
            position = position.saturating_add(1);
        }
    }
    document
}

fn query_matches_document(
    index: &FullTextIndex,
    document: &FullTextDocument,
    query: &FtsQuery,
) -> bool {
    query
        .clauses
        .iter()
        .any(|clause| clause_matches_document(index, document, clause))
}

fn clause_matches_document(
    index: &FullTextIndex,
    document: &FullTextDocument,
    clause: &[FtsQueryTerm],
) -> bool {
    for term in clause.iter().filter(|term| !term.excluded) {
        if !term_matches_document(index, document, term) {
            return false;
        }
    }
    for term in clause.iter().filter(|term| term.excluded) {
        if term_matches_document(index, document, term) {
            return false;
        }
    }
    true
}

fn term_matches_document(
    index: &FullTextIndex,
    document: &FullTextDocument,
    term: &FtsQueryTerm,
) -> bool {
    match term.kind {
        FtsQueryTermKind::Word => index
            .config
            .analyze(&term.text)
            .into_iter()
            .all(|token| document.terms.contains_key(&token)),
        FtsQueryTermKind::Prefix => {
            let normalized = index.config.analyze(&term.text);
            let Some(prefix) = normalized.first() else {
                return false;
            };
            normalized.len() == 1 && document.terms.keys().any(|term| term.starts_with(prefix))
        }
        FtsQueryTermKind::Phrase => phrase_matches_document(index, document, &term.text),
    }
}

fn phrase_matches_document(
    index: &FullTextIndex,
    document: &FullTextDocument,
    phrase: &str,
) -> bool {
    let terms = index.config.analyze(phrase);
    let Some(first_term) = terms.first() else {
        return false;
    };
    let Some(first_info) = document.terms.get(first_term) else {
        return false;
    };
    let position_sets = terms
        .iter()
        .map(|term| {
            document
                .terms
                .get(term)
                .map(|info| info.positions.iter().copied().collect::<BTreeSet<_>>())
        })
        .collect::<Option<Vec<_>>>();
    let Some(position_sets) = position_sets else {
        return false;
    };
    first_info.positions.iter().any(|start| {
        position_sets
            .iter()
            .enumerate()
            .all(|(offset, positions)| positions.contains(&start.saturating_add(offset as u32)))
    })
}

fn positive_scoring_terms(index: &FullTextIndex, query: &FtsQuery) -> Vec<String> {
    let mut terms = BTreeSet::new();
    for term in query.clauses.iter().flatten().filter(|term| !term.excluded) {
        match term.kind {
            FtsQueryTermKind::Word | FtsQueryTermKind::Phrase => {
                terms.extend(index.config.analyze(&term.text));
            }
            FtsQueryTermKind::Prefix => {
                let normalized = index.config.analyze(&term.text);
                if let Some(prefix) = normalized.first().filter(|_| normalized.len() == 1) {
                    terms.extend(
                        index
                            .postings
                            .keys()
                            .filter(|candidate| candidate.starts_with(prefix))
                            .cloned(),
                    );
                }
            }
        }
    }
    terms.into_iter().collect()
}

#[cfg(test)]
mod runtime_tests {
    use super::analyzer::PrefixPolicy;
    use super::{AnalyzerConfig, FullTextIndex};

    #[test]
    fn fulltext_index_matches_terms_and_phrases() {
        let mut index = FullTextIndex::new(AnalyzerConfig::default());
        index.insert_document(
            1,
            &[
                Some("Embedded database engine"),
                Some("fast durable search"),
            ],
        );
        index.insert_document(2, &[Some("Search engine"), Some("not a database")]);

        assert!(index.matches_query(1, "embedded database").expect("query"));
        assert!(index
            .matches_query(1, "\"embedded database\"")
            .expect("query"));
        assert!(!index.matches_query(1, "\"engine fast\"").expect("query"));
        assert!(index.matches_query(2, "search -durable").expect("query"));
    }

    #[test]
    fn prefix_queries_require_prefix_policy() {
        let mut index = FullTextIndex::new(AnalyzerConfig::default());
        index.insert_document(1, &[Some("decentdb")]);

        let error = index.matches_query(1, "dec*").expect_err("prefix disabled");
        assert!(error.message.starts_with(super::FTS_QUERY_ERROR_PREFIX));
    }

    #[test]
    fn prefix_queries_match_when_enabled() {
        let config = AnalyzerConfig::default().with_prefix(PrefixPolicy::from_lengths(vec![2, 3]));
        let mut index = FullTextIndex::new(config);
        index.insert_document(1, &[Some("decentdb")]);

        assert!(index.matches_query(1, "dec*").expect("query"));
    }

    #[test]
    fn bm25_score_orders_more_relevant_document_first() {
        let mut index = FullTextIndex::new(AnalyzerConfig::default());
        index.insert_document(1, &[Some("search search search search database")]);
        index.insert_document(2, &[Some("search database")]);

        let hits = index.search("search").expect("query");
        assert_eq!(hits[0].row_id, 1);
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn null_fields_contribute_no_tokens() {
        let mut index = FullTextIndex::new(AnalyzerConfig::default());
        index.insert_document(1, &[None, Some("")]);

        assert_eq!(index.entry_count(), 1);
        assert_eq!(index.average_document_len(), 0.0);
        assert!(!index.matches_query(1, "anything").expect("query"));
    }
}
