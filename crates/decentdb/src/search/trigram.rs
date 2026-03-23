//! Trigram tokenization and guardrails.
//!
//! Implements:
//! - design/adr/0007-trigram-postings-storage-strategy.md
//! - design/adr/0008-trigram-pattern-length-guardrails.md
//! - design/adr/0052-trigram-durability.md

use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GuardrailDecision {
    TooShort,
    RequireAdditionalFilter,
    UseIndex,
    CapResults { limit: usize },
}

#[must_use]
pub(crate) fn normalize(input: &str) -> String {
    input.to_uppercase()
}

#[must_use]
pub(crate) fn pattern_char_len(input: &str) -> usize {
    normalize(input).chars().count()
}

#[must_use]
pub(crate) fn unique_tokens(input: &str) -> Vec<u32> {
    let normalized = normalize(input);
    let chars = normalized.chars().count();
    if chars < 3 {
        return Vec::new();
    }

    let bytes = normalized.into_bytes();
    bytes
        .windows(3)
        .map(|window| pack_trigram([window[0], window[1], window[2]]))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[must_use]
pub(crate) fn decide_guardrails(
    pattern: &str,
    rarest_postings_count: usize,
    has_additional_filter: bool,
    threshold: usize,
) -> GuardrailDecision {
    let len = pattern_char_len(pattern);
    if len < 3 {
        return GuardrailDecision::TooShort;
    }

    if (3..=5).contains(&len) && !has_additional_filter && rarest_postings_count >= threshold {
        return GuardrailDecision::RequireAdditionalFilter;
    }

    if len > 5 && rarest_postings_count > threshold {
        return GuardrailDecision::CapResults { limit: threshold };
    }

    GuardrailDecision::UseIndex
}

#[must_use]
pub(crate) fn pack_trigram(bytes: [u8; 3]) -> u32 {
    (u32::from(bytes[0]) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2])
}

#[cfg(test)]
mod tests {
    use super::{decide_guardrails, normalize, unique_tokens, GuardrailDecision};

    #[test]
    fn trigram_generation_normalizes_to_uppercase() {
        let tokens = unique_tokens("abCd");
        assert_eq!(tokens.len(), 2);
        assert_eq!(normalize("abCd"), "ABCD");
    }

    #[test]
    fn short_and_broad_patterns_trigger_guardrails() {
        assert_eq!(
            decide_guardrails("ab", 10, false, 100_000),
            GuardrailDecision::TooShort
        );
        assert_eq!(
            decide_guardrails("abcd", 100_000, false, 100_000),
            GuardrailDecision::RequireAdditionalFilter
        );
        assert_eq!(
            decide_guardrails("abcdefgh", 100_001, false, 100_000),
            GuardrailDecision::CapResults { limit: 100_000 }
        );
    }
}
