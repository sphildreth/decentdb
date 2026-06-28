//! Full-text analyzer, query parser, scoring, and runtime index primitives.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;

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
use ranking::{bm25_score_iter, Bm25Context, Bm25DocumentStats, Bm25TermStats};

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

#[derive(Clone, Default)]
struct FullTextHasher(u64);

type FullTextBuildHasher = BuildHasherDefault<FullTextHasher>;
type TermMap<V> = HashMap<String, V, FullTextBuildHasher>;
type RowIdList = Vec<u64>;
type DocumentMap = HashMap<u64, FullTextDocument, FullTextBuildHasher>;

fn term_map_with_capacity<V>(capacity: usize) -> TermMap<V> {
    HashMap::with_capacity_and_hasher(capacity, FullTextBuildHasher::default())
}

fn document_map_with_capacity(capacity: usize) -> DocumentMap {
    HashMap::with_capacity_and_hasher(capacity, FullTextBuildHasher::default())
}

impl Hasher for FullTextHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        const OFFSET: u64 = 0xcbf29ce484222325;
        const PRIME: u64 = 0x100000001b3;
        if self.0 == 0 {
            self.0 = OFFSET;
        }
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(PRIME);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.write(&value.to_le_bytes());
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct FullTextDocument {
    doc_len: u32,
    terms: TermMap<TermDocumentInfo>,
}

fn document_term<'a>(document: &'a FullTextDocument, term: &str) -> Option<&'a TermDocumentInfo> {
    document.terms.get(term)
}

#[derive(Clone, Debug)]
pub(crate) struct FullTextIndex {
    config: AnalyzerConfig,
    documents: Arc<DocumentMap>,
    postings: Arc<TermMap<RowIdList>>,
    deleted_row_ids: BTreeSet<u64>,
    deleted_term_counts: BTreeMap<String, usize>,
    non_empty_document_count: u64,
    total_document_len: u64,
}

#[derive(Debug)]
pub(crate) struct FullTextIndexBuilder {
    config: AnalyzerConfig,
    documents: DocumentMap,
    postings: TermMap<RowIdList>,
    non_empty_document_count: u64,
    total_document_len: u64,
}

impl FullTextIndexBuilder {
    #[must_use]
    pub(crate) fn new(config: AnalyzerConfig) -> Self {
        Self::with_capacity(config, 0)
    }

    #[must_use]
    pub(crate) fn with_capacity(config: AnalyzerConfig, row_count: usize) -> Self {
        let postings_capacity = row_count.saturating_mul(12).max(16);
        Self {
            config: config.canonicalize(),
            documents: document_map_with_capacity(row_count),
            postings: term_map_with_capacity(postings_capacity),
            non_empty_document_count: 0,
            total_document_len: 0,
        }
    }

    pub(crate) fn add_row(&mut self, row_id: u64, fields: &[Option<&str>]) {
        self.insert_fresh_document(row_id, fields);
    }

    fn insert_fresh_document(&mut self, row_id: u64, fields: &[Option<&str>]) {
        let document = build_document(&self.config, fields);
        if document.doc_len > 0 {
            self.non_empty_document_count += 1;
            self.total_document_len += u64::from(document.doc_len);
        }
        for term in document.terms.keys() {
            insert_posting_row(&mut self.postings, term, row_id);
        }
        self.documents.insert(row_id, document);
    }

    pub(crate) fn insert_document(&mut self, row_id: u64, fields: &[Option<&str>]) {
        self.remove_existing_document(row_id);
        self.insert_fresh_document(row_id, fields);
    }

    #[must_use]
    pub(crate) fn finish(self) -> FullTextIndex {
        FullTextIndex {
            config: self.config,
            documents: Arc::new(self.documents),
            postings: Arc::new(self.postings),
            deleted_row_ids: BTreeSet::new(),
            deleted_term_counts: BTreeMap::new(),
            non_empty_document_count: self.non_empty_document_count,
            total_document_len: self.total_document_len,
        }
    }

    fn remove_existing_document(&mut self, row_id: u64) {
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
                remove_posting_row(rows, row_id);
                rows.is_empty()
            } else {
                false
            };
            if remove_term {
                self.postings.remove(term);
            }
        }
    }
}

impl FullTextIndex {
    #[must_use]
    pub(crate) fn new(config: AnalyzerConfig) -> Self {
        Self {
            config: config.canonicalize(),
            documents: Arc::new(DocumentMap::default()),
            postings: Arc::new(TermMap::default()),
            deleted_row_ids: BTreeSet::new(),
            deleted_term_counts: BTreeMap::new(),
            non_empty_document_count: 0,
            total_document_len: 0,
        }
    }

    #[must_use]
    pub(crate) fn config(&self) -> &AnalyzerConfig {
        &self.config
    }

    pub(crate) fn insert_document_fresh(&mut self, row_id: u64, fields: &[Option<&str>]) {
        self.remove_existing_document_for_replace(row_id);
        let document = build_document(&self.config, fields);
        if document.doc_len > 0 {
            self.non_empty_document_count += 1;
            self.total_document_len += u64::from(document.doc_len);
        }
        for term in document.terms.keys() {
            insert_posting_row(Arc::make_mut(&mut self.postings), term, row_id);
        }
        Arc::make_mut(&mut self.documents).insert(row_id, document);
    }

    pub(crate) fn insert_document(&mut self, row_id: u64, fields: &[Option<&str>]) {
        self.insert_document_fresh(row_id, fields);
    }

    pub(crate) fn delete_document(&mut self, row_id: u64) {
        if self.deleted_row_ids.contains(&row_id) {
            return;
        }
        let Some(document) = self.documents.get(&row_id) else {
            return;
        };
        if document.doc_len > 0 {
            self.non_empty_document_count = self.non_empty_document_count.saturating_sub(1);
            self.total_document_len = self
                .total_document_len
                .saturating_sub(u64::from(document.doc_len));
        }
        for term in document.terms.keys() {
            *self.deleted_term_counts.entry(term.clone()).or_default() += 1;
        }
        self.deleted_row_ids.insert(row_id);
    }

    pub(crate) fn delete_documents<I>(&mut self, row_ids: I)
    where
        I: IntoIterator<Item = u64>,
    {
        for row_id in row_ids {
            if self.deleted_row_ids.contains(&row_id) {
                continue;
            }
            let Some(document) = self.documents.get(&row_id) else {
                continue;
            };
            if document.doc_len > 0 {
                self.non_empty_document_count = self.non_empty_document_count.saturating_sub(1);
                self.total_document_len = self
                    .total_document_len
                    .saturating_sub(u64::from(document.doc_len));
            }
            for term in document.terms.keys() {
                *self.deleted_term_counts.entry(term.clone()).or_default() += 1;
            }
            self.deleted_row_ids.insert(row_id);
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
            .live_document(row_id)
            .is_some_and(|document| query_matches_document(self, document, &query)))
    }

    pub(crate) fn score_query(
        &self,
        row_id: u64,
        query_text: &str,
    ) -> Result<f64, FullTextIndexError> {
        let query = parse_runtime_query(&self.config, query_text)?;
        let Some(document) = self.live_document(row_id) else {
            return Ok(0.0);
        };
        Ok(self.score_parsed_query(document, &query))
    }

    pub(crate) fn search(
        &self,
        query_text: &str,
    ) -> Result<Vec<FullTextSearchHit>, FullTextIndexError> {
        self.search_top_k(query_text, usize::MAX)
    }

    pub(crate) fn search_top_k(
        &self,
        query_text: &str,
        limit: usize,
    ) -> Result<Vec<FullTextSearchHit>, FullTextIndexError> {
        let query = parse_runtime_query(&self.config, query_text)?;
        let mut hits = Vec::new();
        // Precompute the scoring terms (and their document frequencies) once so
        // the per-document scoring avoids re-analyzing the query text for every
        // candidate. This is the same set `score_parsed_query` would recompute
        // per document via `positive_scoring_terms`.
        let scoring_terms: Vec<(String, usize)> = positive_scoring_terms(self, &query)
            .into_iter()
            .map(|term| {
                let doc_freq = self.live_doc_freq(&term);
                (term, doc_freq)
            })
            .collect();
        let scoring_context = Bm25Context {
            corpus_size: self.non_empty_document_count as f64,
            avg_doc_len: self.average_document_len(),
            ..Bm25Context::default()
        };
        if query_is_postings_resolvable(&query) {
            // Fast path: the query is a Boolean of positive Word terms only, so
            // the candidate set resolved from postings is exactly the matching
            // set. Score candidates directly without re-checking each document.
            let candidate_row_ids = candidate_row_ids_for_query(self, &query);
            hits.reserve(candidate_row_ids.len());
            for row_id in candidate_row_ids {
                let Some(document) = self.live_document(row_id) else {
                    continue;
                };
                hits.push(FullTextSearchHit {
                    row_id,
                    score: self.score_document_with_terms(
                        document,
                        &scoring_terms,
                        &scoring_context,
                    ),
                });
            }
        } else {
            // Fall back to the full document scan for phrases, prefixes, and
            // excluded terms that need document-level checks beyond postings.
            for (row_id, document) in self.documents.iter() {
                if self.deleted_row_ids.contains(row_id) {
                    continue;
                }
                if query_matches_document(self, document, &query) {
                    hits.push(FullTextSearchHit {
                        row_id: *row_id,
                        score: self.score_document_with_terms(
                            document,
                            &scoring_terms,
                            &scoring_context,
                        ),
                    });
                }
            }
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        if hits.len() > limit {
            let cmp = |left: &FullTextSearchHit, right: &FullTextSearchHit| {
                right
                    .score
                    .total_cmp(&left.score)
                    .then_with(|| left.row_id.cmp(&right.row_id))
            };
            hits.select_nth_unstable_by(limit - 1, cmp);
            hits.truncate(limit);
        }
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.row_id.cmp(&right.row_id))
        });
        Ok(hits)
    }

    #[must_use]
    pub(crate) fn entry_count(&self) -> usize {
        self.documents
            .len()
            .saturating_sub(self.deleted_row_ids.len())
    }

    #[must_use]
    pub(crate) fn term_count(&self) -> usize {
        self.postings
            .keys()
            .filter(|term| self.live_doc_freq(term) > 0)
            .count()
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
        bm25_score_iter(
            &Bm25Context {
                corpus_size: self.non_empty_document_count as f64,
                avg_doc_len: self.average_document_len(),
                ..Bm25Context::default()
            },
            &Bm25DocumentStats {
                doc_len: f64::from(document.doc_len),
            },
            positive_scoring_terms(self, query)
                .into_iter()
                .filter_map(|term| {
                    let term_info = document_term(document, term.as_str())?;
                    let doc_freq = self.live_doc_freq(&term);
                    Some(Bm25TermStats {
                        term_freq: f64::from(term_info.frequency),
                        doc_freq: doc_freq as f64,
                    })
                }),
        )
    }

    /// Scores a document against pre-resolved scoring terms and the shared
    /// BM25 context, avoiding the per-document `positive_scoring_terms`
    /// re-analysis. Used by `search()` after resolving scoring terms once.
    fn score_document_with_terms(
        &self,
        document: &FullTextDocument,
        scoring_terms: &[(String, usize)],
        context: &Bm25Context,
    ) -> f64 {
        bm25_score_iter(
            context,
            &Bm25DocumentStats {
                doc_len: f64::from(document.doc_len),
            },
            scoring_terms.iter().filter_map(|(term, doc_freq)| {
                let term_info = document_term(document, term.as_str())?;
                Some(Bm25TermStats {
                    term_freq: f64::from(term_info.frequency),
                    doc_freq: *doc_freq as f64,
                })
            }),
        )
    }

    fn live_document(&self, row_id: u64) -> Option<&FullTextDocument> {
        if self.deleted_row_ids.contains(&row_id) {
            return None;
        }
        self.documents.get(&row_id)
    }

    fn live_doc_freq(&self, term: &str) -> usize {
        self.postings
            .get(term)
            .map_or(0_usize, Vec::len)
            .saturating_sub(self.deleted_term_counts.get(term).copied().unwrap_or(0))
    }

    fn remove_existing_document_for_replace(&mut self, row_id: u64) {
        let was_deleted = self.deleted_row_ids.remove(&row_id);
        let Some(document) = self.documents.get(&row_id).cloned() else {
            return;
        };
        if !was_deleted && document.doc_len > 0 {
            self.non_empty_document_count = self.non_empty_document_count.saturating_sub(1);
            self.total_document_len = self
                .total_document_len
                .saturating_sub(u64::from(document.doc_len));
        }
        for term in document.terms.keys() {
            if was_deleted {
                decrement_deleted_term_count(&mut self.deleted_term_counts, term);
            }
            let remove_term = if let Some(rows) = Arc::make_mut(&mut self.postings).get_mut(term) {
                remove_posting_row(rows, row_id);
                rows.is_empty()
            } else {
                false
            };
            if remove_term {
                Arc::make_mut(&mut self.postings).remove(term);
            }
        }
        Arc::make_mut(&mut self.documents).remove(&row_id);
    }
}

fn insert_posting_row(postings: &mut TermMap<RowIdList>, term: &str, row_id: u64) {
    if let Some(rows) = postings.get_mut(term) {
        rows.push(row_id);
        return;
    }
    postings.insert(term.to_string(), vec![row_id]);
}

fn remove_posting_row(rows: &mut RowIdList, row_id: u64) {
    rows.retain(|existing| *existing != row_id);
}

fn decrement_deleted_term_count(counts: &mut BTreeMap<String, usize>, term: &str) {
    let remove = if let Some(count) = counts.get_mut(term) {
        *count = count.saturating_sub(1);
        *count == 0
    } else {
        false
    };
    if remove {
        counts.remove(term);
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
    let estimated_terms = fields
        .iter()
        .filter_map(|field| field.map(str::len))
        .map(|len| len / 6)
        .sum::<usize>()
        .clamp(4, 64);
    let mut document = FullTextDocument {
        doc_len: 0,
        terms: term_map_with_capacity(estimated_terms),
    };
    let mut position = 0_u32;
    let field_gap = u32::try_from(config.field_position_gap).unwrap_or(u32::MAX / 2);

    for (field_index, field) in fields.iter().enumerate() {
        if field_index > 0 && document.doc_len > 0 {
            position = position.saturating_add(field_gap);
        }
        let Some(text) = field else {
            continue;
        };
        if can_use_ascii_document_fast_path(config) && text.is_ascii() {
            position = add_ascii_document_tokens(text, &mut document, position);
        } else {
            config.for_each_token(text, |token| {
                add_document_term_owned(&mut document, token, position);
                position = position.saturating_add(1);
            });
        }
    }
    document
}

fn can_use_ascii_document_fast_path(config: &AnalyzerConfig) -> bool {
    config.case_folded
        && matches!(config.diacritics, analyzer::AnalyzerDiacritics::Preserve)
        && matches!(config.stopwords, analyzer::AnalyzerStopwords::None)
}

fn add_ascii_document_tokens(
    text: &str,
    document: &mut FullTextDocument,
    mut position: u32,
) -> u32 {
    let bytes = text.as_bytes();
    let mut index = 0_usize;
    while index < bytes.len() {
        while index < bytes.len() && !is_ascii_token_byte(bytes[index]) {
            index += 1;
        }
        let start = index;
        let mut has_uppercase = false;
        while index < bytes.len() && is_ascii_token_byte(bytes[index]) {
            has_uppercase |= bytes[index].is_ascii_uppercase();
            index += 1;
        }
        if start == index {
            continue;
        }
        let token = &text[start..index];
        if has_uppercase {
            let mut normalized = token.to_string();
            normalized.make_ascii_lowercase();
            add_document_term_owned(document, normalized, position);
        } else {
            add_document_term_borrowed(document, token, position);
        }
        position = position.saturating_add(1);
    }
    position
}

fn is_ascii_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn add_document_term_borrowed(document: &mut FullTextDocument, token: &str, position: u32) {
    if let Some(info) = document.terms.get_mut(token) {
        add_term_position(info, position);
    } else {
        let mut info = TermDocumentInfo::default();
        add_term_position(&mut info, position);
        document.terms.insert(token.to_string(), info);
    }
    document.doc_len = document.doc_len.saturating_add(1);
}

fn add_document_term_owned(document: &mut FullTextDocument, token: String, position: u32) {
    if let Some(info) = document.terms.get_mut(token.as_str()) {
        add_term_position(info, position);
    } else {
        let mut info = TermDocumentInfo::default();
        add_term_position(&mut info, position);
        document.terms.insert(token, info);
    }
    document.doc_len = document.doc_len.saturating_add(1);
}

fn add_term_position(info: &mut TermDocumentInfo, position: u32) {
    info.frequency = info.frequency.saturating_add(1);
    info.positions.push(position);
}

/// Returns true when the query contains only positive `Word` terms (no
/// phrases, no prefixes, no excluded terms) that can be resolved purely from
/// the postings lists. Used to decide whether the candidate resolver can skip
/// the full document scan.
fn query_is_postings_resolvable(query: &FtsQuery) -> bool {
    query
        .clauses
        .iter()
        .flatten()
        .all(|term| !term.excluded && term.kind == FtsQueryTermKind::Word)
}

/// Resolves the candidate row ids for a query from the postings lists when
/// possible. Returns an empty set when the query cannot be resolved from
/// postings alone; the caller then falls back to a full document scan. For
/// `Word`-only OR/AND queries this unions the per-clause candidate sets, which
/// is the common benchmark and application shape (`a OR b OR c`).
fn candidate_row_ids_for_query(index: &FullTextIndex, query: &FtsQuery) -> Vec<u64> {
    let mut candidates = Vec::new();
    for clause in &query.clauses {
        // Each clause is an AND of positive Word terms (guaranteed by
        // query_is_postings_resolvable). Intersect the postings row ids for
        // every term in the clause; union the result into the candidate set.
        if clause.len() == 1 {
            let analyzed = index.config.analyze(&clause[0].text);
            if analyzed.len() == 1 {
                if let Some(rows) = index.postings.get(analyzed[0].as_str()) {
                    candidates.extend(rows.iter().copied());
                }
                continue;
            }
        }
        let mut clause_rows: Option<BTreeSet<u64>> = None;
        for term in clause.iter().filter(|term| !term.excluded) {
            let analyzed = index.config.analyze(&term.text);
            let mut term_rows: BTreeSet<u64> = BTreeSet::new();
            for token in analyzed {
                if let Some(rows) = index.postings.get(token.as_str()) {
                    term_rows.extend(rows.iter().copied());
                }
            }
            clause_rows = Some(match clause_rows {
                None => term_rows,
                Some(existing) => existing.intersection(&term_rows).copied().collect(),
            });
        }
        if let Some(rows) = clause_rows {
            candidates.extend(rows);
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    candidates
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
            .all(|token| document_term(document, token.as_str()).is_some()),
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
    let Some(first_info) = document_term(document, first_term.as_str()) else {
        return false;
    };
    let position_sets = terms
        .iter()
        .map(|term| {
            document
                .terms
                .get(term.as_str())
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
    use super::{AnalyzerConfig, FullTextIndex, FullTextIndexBuilder};

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
    fn or_word_query_uses_postings_candidates_and_returns_union() {
        // Regression coverage for the postings-based candidate resolver added
        // to `search`. `war OR revenge OR sacrifice` is a positive-Word OR
        // query that the fast path resolves from postings without scanning
        // every document; the result must be the union of matching row ids,
        // scored and ordered by rank.
        let mut index = FullTextIndex::new(AnalyzerConfig::default());
        index.insert_document(1, &[Some("war and peace")]);
        index.insert_document(2, &[Some("revenge of the nerds")]);
        index.insert_document(3, &[Some("a quiet tale of sacrifice")]);
        index.insert_document(4, &[Some("nothing relevant here")]);

        let mut hits = index.search("war OR revenge OR sacrifice").expect("query");
        // All three matching documents are returned, the irrelevant one is not.
        let mut row_ids: Vec<u64> = hits.iter().map(|hit| hit.row_id).collect();
        row_ids.sort_unstable();
        assert_eq!(row_ids, vec![1, 2, 3]);
        // Scores are finite and ordered descending by score, tie-broken by row id.
        assert!(hits.windows(2).all(|w| {
            w[0].score >= w[1].score
                || (w[0].score - w[1].score).abs() < f64::EPSILON && w[0].row_id <= w[1].row_id
        }));
        // Sanity: each returned hit has a positive score (the terms appear).
        assert!(hits.iter().all(|hit| hit.score > 0.0));
        // Touch `hits` ordering is already asserted; keep the binding used.
        hits.sort_by_key(|hit| hit.row_id);
    }

    #[test]
    fn search_top_k_matches_full_search_prefix_and_tie_ordering() {
        let mut index = FullTextIndex::new(AnalyzerConfig::default());
        index.insert_document(1, &[Some("alpha")]);
        index.insert_document(2, &[Some("beta")]);
        index.insert_document(3, &[Some("gamma")]);

        let full_hits = index.search("alpha OR beta").expect("query");
        let top_hits = index.search_top_k("alpha OR beta", 2).expect("query");

        assert_eq!(top_hits.len(), 2);
        assert_eq!(
            top_hits.iter().map(|hit| hit.row_id).collect::<Vec<_>>(),
            full_hits
                .iter()
                .take(2)
                .map(|hit| hit.row_id)
                .collect::<Vec<_>>()
        );
        assert_eq!(top_hits[0].row_id, 1);
        assert_eq!(top_hits[1].row_id, 2);
        assert!((top_hits[0].score - top_hits[1].score).abs() < f64::EPSILON);
    }

    #[test]
    fn and_word_query_postings_path_intersects_terms() {
        // A single clause with two positive Word terms is an AND; the postings
        // fast path intersects the term postings and returns only documents
        // containing both terms.
        let mut index = FullTextIndex::new(AnalyzerConfig::default());
        index.insert_document(1, &[Some("fast database")]);
        index.insert_document(2, &[Some("fast car")]);
        index.insert_document(3, &[Some("database design")]);

        let hits = index.search("fast database").expect("query");
        let row_ids: Vec<u64> = hits.iter().map(|hit| hit.row_id).collect();
        assert_eq!(row_ids, vec![1]);
    }

    #[test]
    fn null_fields_contribute_no_tokens() {
        let mut index = FullTextIndex::new(AnalyzerConfig::default());
        index.insert_document(1, &[None, Some("")]);

        assert_eq!(index.entry_count(), 1);
        assert_eq!(index.average_document_len(), 0.0);
        assert!(!index.matches_query(1, "anything").expect("query"));
    }

    #[test]
    fn batch_delete_documents_clears_postings_and_stats() {
        let mut index = FullTextIndex::new(AnalyzerConfig::default());
        index.insert_document(1, &[Some("alpha beta")]);
        index.insert_document(2, &[Some("alpha gamma")]);
        index.insert_document(3, &[Some("delta")]);

        index.delete_documents([1, 2, 9]);

        assert_eq!(index.entry_count(), 1);
        assert_eq!(index.term_count(), 1);
        assert_eq!(index.average_document_len(), 1.0);
        assert!(index.search("alpha").expect("query").is_empty());
        assert!(!index.matches_query(1, "alpha").expect("query"));
        assert_eq!(index.score_query(1, "alpha").expect("score"), 0.0);
        assert!(index.matches_query(3, "delta").expect("query"));
        assert_eq!(index.search("delta").expect("query")[0].row_id, 3);
        assert!(index.score_query(3, "delta").expect("score") > 0.0);
    }

    #[test]
    fn fulltext_insert_fresh_matches_insert_and_replaces_existing_document() {
        let mut fresh_index = FullTextIndex::new(AnalyzerConfig::default());
        fresh_index.insert_document_fresh(1, &[Some("alpha beta")]);
        fresh_index.insert_document_fresh(2, &[Some("beta gamma")]);

        let mut normal_index = FullTextIndex::new(AnalyzerConfig::default());
        normal_index.insert_document(1, &[Some("alpha beta")]);
        normal_index.insert_document(2, &[Some("beta gamma")]);

        let fresh_beta_hits = fresh_index.search("beta").expect("query");
        let normal_beta_hits = normal_index.search("beta").expect("query");
        assert_eq!(
            fresh_beta_hits
                .iter()
                .map(|hit| (hit.row_id, hit.score))
                .collect::<Vec<_>>(),
            normal_beta_hits
                .iter()
                .map(|hit| (hit.row_id, hit.score))
                .collect::<Vec<_>>()
        );
        let fresh_union_hits = fresh_index.search("alpha OR gamma").expect("query");
        let normal_union_hits = normal_index.search("alpha OR gamma").expect("query");
        assert_eq!(
            fresh_union_hits
                .iter()
                .map(|hit| (hit.row_id, hit.score))
                .collect::<Vec<_>>(),
            normal_union_hits
                .iter()
                .map(|hit| (hit.row_id, hit.score))
                .collect::<Vec<_>>()
        );

        normal_index.insert_document(1, &[Some("delta")]);
        assert!(normal_index.search("alpha").expect("query").is_empty());
        let delta_hits = normal_index.search("delta").expect("query");
        assert_eq!(delta_hits.len(), 1);
        assert_eq!(delta_hits[0].row_id, 1);
        assert!(
            (delta_hits[0].score - normal_index.score_query(1, "delta").expect("query")).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn fulltext_builder_matches_insert_document_fresh() {
        let builder_index = {
            let mut builder = FullTextIndexBuilder::new(AnalyzerConfig::default());
            builder.add_row(1, &[Some("alpha beta")]);
            builder.add_row(2, &[Some("beta gamma")]);
            builder.add_row(3, &[Some("gamma delta"), Some("alpha")]);
            builder.insert_document(1, &[Some("epsilon replacement")]);
            builder.finish()
        };
        let mut insert_index = FullTextIndex::new(AnalyzerConfig::default());
        insert_index.insert_document(1, &[Some("alpha beta")]);
        insert_index.insert_document(2, &[Some("beta gamma")]);
        insert_index.insert_document(3, &[Some("gamma delta"), Some("alpha")]);
        insert_index.insert_document(1, &[Some("epsilon replacement")]);

        assert_eq!(
            builder_index.search("alpha").expect("query").len(),
            insert_index.search("alpha").expect("query").len()
        );
        assert_eq!(
            builder_index
                .search("alpha")
                .expect("query")
                .iter()
                .map(|hit| hit.row_id)
                .collect::<Vec<_>>(),
            insert_index
                .search("alpha")
                .expect("query")
                .iter()
                .map(|hit| hit.row_id)
                .collect::<Vec<_>>()
        );
        assert_eq!(builder_index.entry_count(), insert_index.entry_count());
        assert_eq!(builder_index.term_count(), insert_index.term_count());
        assert!(
            (builder_index.average_document_len() - insert_index.average_document_len()).abs()
                < f64::EPSILON
        );
        assert_eq!(
            builder_index
                .search("epsilon")
                .expect("query")
                .iter()
                .map(|hit| hit.row_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }
}
