#![allow(dead_code)]
//! Trigram indexing and search primitives.

pub(crate) mod fulltext;
pub(crate) mod postings;
pub(crate) mod rebuild;
#[cfg(test)]
mod rebuild_tests;
pub(crate) mod trigram;

use std::collections::{BTreeMap, BTreeSet};

use crate::error::Result;
use crate::search::rebuild::{Freshness, RebuildState};
use crate::search::trigram::{
    decide_guardrails_for_len, like_required_char_len, like_required_tokens, unique_tokens,
    GuardrailDecision,
};

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

#[derive(Clone, Debug)]
pub(crate) struct TrigramIndex {
    postings: BTreeMap<u32, Vec<u64>>,
    pending: BTreeMap<u32, Vec<PendingOp>>,
    deleted_postings: BTreeMap<u32, BTreeSet<u64>>,
    rebuild_state: RebuildState,
    postings_threshold: usize,
}

#[derive(Debug, Default)]
pub(crate) struct TrigramIndexBuilder {
    postings: BTreeMap<u32, Vec<u64>>,
}

impl TrigramIndexBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, row_id: u64, text: &str) {
        for token in unique_tokens(text) {
            self.postings.entry(token).or_default().push(row_id);
        }
    }

    pub(crate) fn finish_into(self, index: &mut TrigramIndex) -> Result<()> {
        index.postings.clear();
        index.pending.clear();
        index.deleted_postings.clear();
        for (token, mut row_ids) in self.postings {
            row_ids.sort_unstable();
            row_ids.dedup();
            if !row_ids.is_empty() {
                index.postings.insert(token, row_ids);
            }
        }
        index.rebuild_state.mark_rebuilt();
        Ok(())
    }
}

impl TrigramIndex {
    pub(crate) fn new(_page_size: u32, postings_threshold: usize) -> Self {
        Self {
            postings: BTreeMap::new(),
            pending: BTreeMap::new(),
            deleted_postings: BTreeMap::new(),
            rebuild_state: RebuildState::default(),
            postings_threshold,
        }
    }

    pub(crate) fn queue_insert(&mut self, row_id: u64, text: &str) {
        for token in unique_tokens(text) {
            if let Some(deleted) = self.deleted_postings.get_mut(&token) {
                deleted.remove(&row_id);
                if deleted.is_empty() {
                    self.deleted_postings.remove(&token);
                }
            }
            self.pending
                .entry(token)
                .or_default()
                .push(PendingOp::Add(row_id));
        }
    }

    pub(crate) fn queue_delete(&mut self, row_id: u64, text: &str) {
        for token in unique_tokens(text) {
            self.deleted_postings
                .entry(token)
                .or_default()
                .insert(row_id);
            self.pending
                .entry(token)
                .or_default()
                .push(PendingOp::Remove(row_id));
        }
    }

    pub(crate) fn queue_delete_documents<I, T>(&mut self, deletions: I)
    where
        I: IntoIterator<Item = (u64, T)>,
        T: AsRef<str>,
    {
        let mut grouped = BTreeMap::<u32, Vec<u64>>::new();
        for (row_id, text) in deletions {
            for token in unique_tokens(text.as_ref()) {
                grouped.entry(token).or_default().push(row_id);
                self.deleted_postings
                    .entry(token)
                    .or_default()
                    .insert(row_id);
            }
        }
        for (token, row_ids) in grouped {
            self.pending
                .entry(token)
                .or_default()
                .extend(row_ids.into_iter().map(PendingOp::Remove));
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
        self.pending.clear();
        self.deleted_postings.clear();
        self.rebuild_state.mark_stale();
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
        self.deleted_postings.clear();
        self.pending.clear();
        let mut builder = TrigramIndexBuilder::new();
        for (row_id, text) in documents {
            builder.insert(row_id, text.as_ref());
        }
        builder.finish_into(self)
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
                self.postings.remove(&token);
            } else {
                self.postings
                    .insert(token, postings.into_iter().collect::<Vec<_>>());
            }
        }
        self.deleted_postings.clear();
        Ok(())
    }

    #[must_use]
    pub(crate) fn entry_count(&self) -> usize {
        self.postings
            .len()
            .saturating_add(self.pending.values().map(Vec::len).sum::<usize>())
            .saturating_sub(
                self.deleted_postings
                    .values()
                    .map(BTreeSet::len)
                    .sum::<usize>(),
            )
    }

    pub(crate) fn query_candidates(
        &self,
        pattern: &str,
        has_additional_filter: bool,
    ) -> Result<TrigramQueryResult> {
        if self.freshness() == Freshness::Stale {
            return Ok(TrigramQueryResult::RebuildRequired);
        }

        let tokens = like_required_tokens(pattern);
        if tokens.is_empty() {
            return Ok(TrigramQueryResult::FallbackTooShort);
        }

        let mut postings = tokens
            .iter()
            .map(|&token| {
                self.materialized_live_postings(token)
                    .map(|set| set.into_iter().collect::<Vec<_>>())
            })
            .collect::<Result<Vec<_>>>()?;
        postings.sort_by_key(|set| set.len());
        let rarest_count = postings.first().map_or(0, Vec::len);

        match decide_guardrails_for_len(
            like_required_char_len(pattern),
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
            .postings
            .get(&token)
            .cloned()
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

    fn materialized_live_postings(&self, token: u32) -> Result<BTreeSet<u64>> {
        let mut postings = self
            .postings
            .get(&token)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();

        if let Some(operations) = self.pending.get(&token) {
            for operation in operations {
                if let PendingOp::Add(row_id) = operation {
                    postings.insert(*row_id);
                }
            }
        }

        if let Some(deleted) = self.deleted_postings.get(&token) {
            postings.retain(|row_id| !deleted.contains(row_id));
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
    use super::{Freshness, TrigramIndex, TrigramIndexBuilder, TrigramQueryResult};

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
    fn query_supports_like_wildcard_patterns() {
        let mut index = TrigramIndex::new(1024, 100_000);
        index.queue_insert(1, "Motley Crue");
        index.queue_insert(2, "Other Motley");
        index.queue_insert(3, "Unrelated");
        index.checkpoint().expect("checkpoint");

        let result = index.query_candidates("%Motley%", false).expect("query");
        assert_eq!(result, TrigramQueryResult::Candidates(vec![1, 2]));
    }

    #[test]
    fn bulk_builder_creates_queryable_postings() {
        let mut builder = TrigramIndexBuilder::new();
        builder.insert(3, "Motley Crue");
        builder.insert(1, "Other Motley");
        builder.insert(2, "Unrelated");

        let mut index = TrigramIndex::new(1024, 100_000);
        builder.finish_into(&mut index).expect("finish");

        let result = index.query_candidates("%Motley%", false).expect("query");
        assert_eq!(result, TrigramQueryResult::Candidates(vec![1, 3]));
    }

    #[test]
    fn bulk_builder_deduplicates_row_ids() {
        let mut builder = TrigramIndexBuilder::new();
        builder.insert(1, "alphabet soup");
        builder.insert(1, "alphabet soup");
        builder.insert(2, "alphabet city");

        let mut index = TrigramIndex::new(1024, 100_000);
        builder.finish_into(&mut index).expect("finish");

        let result = index.query_candidates("alphabet", false).expect("query");
        assert_eq!(result, TrigramQueryResult::Candidates(vec![1, 2]));
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

    #[test]
    fn batch_delete_documents_removes_pending_postings() {
        let mut index = TrigramIndex::new(1024, 100_000);
        index.queue_insert(1, "alphabet");
        index.queue_insert(2, "alphanumeric");
        index.checkpoint().expect("checkpoint");

        index.queue_delete_documents([(1, "alphabet"), (2, "alphanumeric")]);
        index.checkpoint().expect("checkpoint");

        let result = index.query_candidates("alphabet", false).expect("query");
        assert_eq!(result, TrigramQueryResult::Candidates(Vec::new()));
    }

    #[test]
    fn candidate_lookup_skips_bulk_deleted_row_ids() {
        let mut index = TrigramIndex::new(1024, 100_000);
        for row_id in 1_u64..=20 {
            index.queue_insert(row_id, "alphabet");
        }
        index.checkpoint().expect("checkpoint");

        index.queue_delete_documents((1_u64..=10).map(|row_id| (row_id, "alphabet")));

        let result = index.query_candidates("alpha", false).expect("query");
        assert_eq!(
            result,
            TrigramQueryResult::Candidates((11..=20).collect::<Vec<u64>>())
        );
    }

    #[test]
    fn delete_then_reinsert_restores_trigram_visibility() {
        let mut index = TrigramIndex::new(1024, 100_000);
        index.queue_insert(1, "alphabet");
        index.queue_insert(2, "alphanumeric");
        index.checkpoint().expect("checkpoint");

        index.queue_delete(1, "alphabet");
        let deleted = index.query_candidates("alpha", false).expect("query");
        assert_eq!(deleted, TrigramQueryResult::Candidates(vec![2]));

        index.queue_insert(1, "alphabet");
        let restored = index.query_candidates("alpha", false).expect("query");
        assert_eq!(restored, TrigramQueryResult::Candidates(vec![1, 2]));
    }

    #[test]
    fn replace_row_removes_old_token_candidates_and_readds_new_ones() {
        let mut index = TrigramIndex::new(1024, 100_000);
        index.queue_insert(1, "alphabet");
        index.queue_insert(2, "alphanumeric");
        index.checkpoint().expect("checkpoint");

        index.queue_replace(1, "alphabet", "zyx");

        let old_pattern = index.query_candidates("alpha", false).expect("query");
        assert_eq!(old_pattern, TrigramQueryResult::Candidates(vec![2]));

        let new_pattern = index.query_candidates("zyx", false).expect("query");
        assert_eq!(new_pattern, TrigramQueryResult::Candidates(vec![1]));
    }
}
