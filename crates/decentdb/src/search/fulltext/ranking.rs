#[derive(Clone, Copy, Debug)]
pub(crate) struct Bm25Context {
    pub(crate) k1: f64,
    pub(crate) b: f64,
    pub(crate) corpus_size: f64,
    pub(crate) avg_doc_len: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Bm25DocumentStats {
    pub(crate) doc_len: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Bm25TermStats {
    pub(crate) term_freq: f64,
    pub(crate) doc_freq: f64,
}

impl Default for Bm25Context {
    fn default() -> Self {
        Self {
            k1: 1.2,
            b: 0.75,
            corpus_size: 0.0,
            avg_doc_len: 0.0,
        }
    }
}

pub(crate) fn bm25_score(
    context: &Bm25Context,
    doc_stats: &Bm25DocumentStats,
    terms: &[Bm25TermStats],
) -> f64 {
    bm25_score_iter(context, doc_stats, terms.iter().copied())
}

pub(crate) fn bm25_score_iter<I>(
    context: &Bm25Context,
    doc_stats: &Bm25DocumentStats,
    terms: I,
) -> f64
where
    I: IntoIterator<Item = Bm25TermStats>,
{
    if context.corpus_size <= 0.0 || context.avg_doc_len <= 0.0 || doc_stats.doc_len <= 0.0 {
        return 0.0;
    }

    let mut score = 0.0;
    for term in terms {
        if term.doc_freq <= 0.0 || term.term_freq <= 0.0 {
            continue;
        }
        let numerator = context.corpus_size - term.doc_freq + 0.5;
        let denominator = term.doc_freq + 0.5;
        let idf = (1.0 + numerator / denominator).ln();
        let length_norm = 1.0 - context.b + context.b * (doc_stats.doc_len / context.avg_doc_len);
        let term_score =
            (term.term_freq * (context.k1 + 1.0)) / (term.term_freq + context.k1 * length_norm);
        score += idf * term_score;
    }
    score
}

#[cfg(test)]
mod tests {
    use super::{bm25_score, bm25_score_iter, Bm25Context, Bm25DocumentStats, Bm25TermStats};

    #[test]
    fn bm25_score_zero_when_context_or_doc_missing() {
        let context = Bm25Context::default();
        let doc = Bm25DocumentStats { doc_len: 100.0 };
        let score = bm25_score(&context, &doc, &[]);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn bm25_score_zero_when_terms_missing() {
        let context = Bm25Context {
            corpus_size: 10.0,
            avg_doc_len: 10.0,
            ..Bm25Context::default()
        };
        let doc = Bm25DocumentStats { doc_len: 4.0 };
        let terms = [Bm25TermStats {
            term_freq: 0.0,
            doc_freq: 0.0,
        }];
        let score = bm25_score(&context, &doc, &terms);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn bm25_score_follows_formula() {
        let context = Bm25Context {
            k1: 1.2,
            b: 0.75,
            corpus_size: 1000.0,
            avg_doc_len: 100.0,
        };
        let doc = Bm25DocumentStats { doc_len: 50.0 };
        let terms = [Bm25TermStats {
            term_freq: 3.0,
            doc_freq: 5.0,
        }];
        let score = bm25_score(&context, &doc, &terms);
        let expected_idf = (1.0_f64 + (1000.0 - 5.0 + 0.5) / (5.0 + 0.5)).ln();
        let expected_term = (3.0 * (1.2 + 1.0)) / (3.0 + 1.2 * (1.0 - 0.75 + 0.75 * 0.5));
        let expected = expected_idf * expected_term;
        let delta = (score - expected).abs();
        assert!(delta < 0.000_000_1);
    }

    #[test]
    fn bm25_score_accumulates_terms() {
        let context = Bm25Context {
            corpus_size: 100.0,
            avg_doc_len: 20.0,
            ..Bm25Context::default()
        };
        let doc = Bm25DocumentStats { doc_len: 18.0 };
        let first = Bm25TermStats {
            term_freq: 4.0,
            doc_freq: 2.0,
        };
        let second = Bm25TermStats {
            term_freq: 1.0,
            doc_freq: 10.0,
        };
        let score = bm25_score(&context, &doc, &[first, second]);
        assert!(score > 0.0);
    }

    #[test]
    fn bm25_score_iter_matches_slice_api() {
        let context = Bm25Context {
            corpus_size: 100.0,
            avg_doc_len: 20.0,
            ..Bm25Context::default()
        };
        let doc = Bm25DocumentStats { doc_len: 18.0 };
        let terms = [
            Bm25TermStats {
                term_freq: 4.0,
                doc_freq: 2.0,
            },
            Bm25TermStats {
                term_freq: 1.0,
                doc_freq: 10.0,
            },
        ];
        let slice_score = bm25_score(&context, &doc, &terms);
        let iter_score = bm25_score_iter(&context, &doc, terms);
        assert!((slice_score - iter_score).abs() < f64::EPSILON);
    }
}
