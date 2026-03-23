#![allow(dead_code)]
//! Trigram indexing and search primitives.

pub(crate) mod postings;
pub(crate) mod rebuild;
pub(crate) mod trigram;

use std::collections::{BTreeMap, BTreeSet};

use crate::btree::write::Btree;
use crate::error::Result;
use crate::search::postings::{decode_postings, encode_postings};
use crate::search::rebuild::{Freshness, RebuildState};
use crate::search::trigram::{decide_guardrails, unique_tokens, GuardrailDecision};
use crate::storage::page::InMemoryPageStore;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingOp {
    Add(u64),
    Remove(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TrigramQueryResult {
    FallbackTooShort,
    FallbackRequiresAdditionalFilter,
    RebuildRequired,
    Candidates(Vec<u64>),
    Capped(Vec<u64>),
}

#[derive(Debug)]
pub(crate) struct TrigramIndex {
    postings_tree: Btree<InMemoryPageStore>,
    pending: BTreeMap<u32, Vec<PendingOp>>,
    rebuild_state: RebuildState,
    postings_threshold: usize,
}

impl TrigramIndex {
    pub(crate) fn new(page_size: u32, postings_threshold: usize) -> Self {
        Self {
            postings_tree: Btree::with_page_size(page_size),
            pending: BTreeMap::new(),
            rebuild_state: RebuildState::default(),
            postings_threshold,
        }
    }

    pub(crate) fn queue_insert(&mut self, row_id: u64, text: &str) {
        for token in unique_tokens(text) {
            self.pending
                .entry(token)
                .or_default()
                .push(PendingOp::Add(row_id));
        }
    }

    pub(crate) fn queue_delete(&mut self, row_id: u64, text: &str) {
        for token in unique_tokens(text) {
            self.pending
                .entry(token)
                .or_default()
                .push(PendingOp::Remove(row_id));
        }
    }

    pub(crate) fn queue_replace(&mut self, row_id: u64, old_text: &str, new_text: &str) {
        self.queue_delete(row_id, old_text);
        self.queue_insert(row_id, new_text);
    }

    #[must_use]
    pub(crate) fn freshness(&self) -> Freshness {
        self.rebuild_state.freshness()
    }

    #[must_use]
    pub(crate) fn planner_may_use_index(&self) -> bool {
        self.freshness() == Freshness::Fresh
    }

    pub(crate) fn mark_recovered_from_loss(&mut self) {
        if !self.pending.is_empty() {
            self.pending.clear();
            self.rebuild_state.mark_stale();
        }
    }

    pub(crate) fn ensure_fresh<F>(&mut self, rebuild: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        if self.rebuild_state.freshness() == Freshness::Stale {
            rebuild(self)?;
            self.rebuild_state.mark_rebuilt();
        }
        Ok(())
    }

    pub(crate) fn rebuild_from_documents<I, T>(&mut self, documents: I) -> Result<()>
    where
        I: IntoIterator<Item = (u64, T)>,
        T: AsRef<str>,
    {
        self.postings_tree.clear()?;
        self.pending.clear();

        let mut postings = BTreeMap::<u32, BTreeSet<u64>>::new();
        for (row_id, text) in documents {
            for token in unique_tokens(text.as_ref()) {
                postings.entry(token).or_default().insert(row_id);
            }
        }
        for (token, row_ids) in postings {
            self.postings_tree.insert(
                u64::from(token),
                encode_postings(&row_ids.into_iter().collect::<Vec<_>>())?,
            )?;
        }
        self.rebuild_state.mark_rebuilt();
        Ok(())
    }

    pub(crate) fn checkpoint(&mut self) -> Result<()> {
        let pending = std::mem::take(&mut self.pending);
        for (token, operations) in pending {
            let mut postings = self.materialized_postings(token)?;
            for operation in operations {
                match operation {
                    PendingOp::Add(row_id) => {
                        postings.insert(row_id);
                    }
                    PendingOp::Remove(row_id) => {
                        postings.remove(&row_id);
                    }
                }
            }

            if postings.is_empty() {
                self.postings_tree.delete(u64::from(token))?;
            } else {
                self.postings_tree.insert(
                    u64::from(token),
                    encode_postings(&postings.into_iter().collect::<Vec<_>>())?,
                )?;
            }
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn entry_count(&self) -> usize {
        self.postings_tree.entry_count() + self.pending.values().map(Vec::len).sum::<usize>()
    }

    pub(crate) fn query_candidates(
        &self,
        pattern: &str,
        has_additional_filter: bool,
    ) -> Result<TrigramQueryResult> {
        if self.freshness() == Freshness::Stale {
            return Ok(TrigramQueryResult::RebuildRequired);
        }

        let tokens = unique_tokens(pattern);
        if tokens.is_empty() {
            return Ok(TrigramQueryResult::FallbackTooShort);
        }

        let mut postings = tokens
            .iter()
            .map(|&token| {
                self.materialized_postings(token)
                    .map(|set| set.into_iter().collect::<Vec<_>>())
            })
            .collect::<Result<Vec<_>>>()?;
        postings.sort_by_key(|set| set.len());
        let rarest_count = postings.first().map_or(0, Vec::len);

        match decide_guardrails(
            pattern,
            rarest_count,
            has_additional_filter,
            self.postings_threshold,
        ) {
            GuardrailDecision::TooShort => Ok(TrigramQueryResult::FallbackTooShort),
            GuardrailDecision::RequireAdditionalFilter => {
                Ok(TrigramQueryResult::FallbackRequiresAdditionalFilter)
            }
            GuardrailDecision::UseIndex => Ok(TrigramQueryResult::Candidates(intersect_postings(
                &postings,
            ))),
            GuardrailDecision::CapResults { limit } => Ok(TrigramQueryResult::Capped(
                intersect_postings(&postings)
                    .into_iter()
                    .take(limit)
                    .collect::<Vec<_>>(),
            )),
        }
    }

    fn materialized_postings(&self, token: u32) -> Result<BTreeSet<u64>> {
        let mut postings = self
            .postings_tree
            .get(u64::from(token))?
            .map(|bytes| decode_postings(&bytes))
            .transpose()?
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();

        if let Some(operations) = self.pending.get(&token) {
            for operation in operations {
                match operation {
                    PendingOp::Add(row_id) => {
                        postings.insert(*row_id);
                    }
                    PendingOp::Remove(row_id) => {
                        postings.remove(row_id);
                    }
                }
            }
        }

        Ok(postings)
    }
}

fn intersect_postings(postings: &[Vec<u64>]) -> Vec<u64> {
    let mut iter = postings.iter();
    let Some(first) = iter.next() else {
        return Vec::new();
    };
    let mut intersection = first.clone();
    for next in iter {
        let set = next.iter().copied().collect::<BTreeSet<_>>();
        intersection.retain(|row_id| set.contains(row_id));
    }
    intersection
}

#[cfg(test)]
mod tests {
    use super::{Freshness, TrigramIndex, TrigramQueryResult};

    #[test]
    fn query_support_uses_checkpointed_and_pending_deltas() {
        let mut index = TrigramIndex::new(1024, 100_000);
        index.queue_insert(1, "alphabet");
        index.queue_insert(2, "alphanumeric");
        index.checkpoint().expect("checkpoint");
        index.queue_insert(3, "alphabet soup");

        let result = index.query_candidates("alpha", false).expect("query");
        assert_eq!(result, TrigramQueryResult::Candidates(vec![1, 2, 3]));
    }

    #[test]
    fn recovery_marks_index_stale_until_lazy_rebuild() {
        let mut index = TrigramIndex::new(1024, 100_000);
        index.queue_insert(7, "needle");
        index.mark_recovered_from_loss();
        assert_eq!(index.freshness(), Freshness::Stale);
        assert_eq!(
            index.query_candidates("needle", false).expect("query"),
            TrigramQueryResult::RebuildRequired
        );

        index
            .ensure_fresh(|index| index.rebuild_from_documents([(7, "needle")]))
            .expect("rebuild");
        assert_eq!(index.freshness(), Freshness::Fresh);
        assert_eq!(
            index.query_candidates("needle", false).expect("query"),
            TrigramQueryResult::Candidates(vec![7])
        );
    }

    #[test]
    fn broad_patterns_require_filter_or_capping() {
        let mut index = TrigramIndex::new(1024, 2);
        index.queue_insert(1, "alphabet soup");
        index.queue_insert(2, "alphabet city");
        index.queue_insert(3, "alphabet code");
        index.checkpoint().expect("checkpoint");

        assert_eq!(
            index.query_candidates("alpha", false).expect("query"),
            TrigramQueryResult::FallbackRequiresAdditionalFilter
        );
        assert!(matches!(
            index.query_candidates("alphabet", false).expect("query"),
            TrigramQueryResult::Capped(_)
        ));
    }
}
