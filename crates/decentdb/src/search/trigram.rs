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
    use super::{
        decide_guardrails, normalize, pack_trigram, pattern_char_len, unique_tokens,
        GuardrailDecision,
    };

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

    #[test]
    fn normalize_converts_to_uppercase() {
        assert_eq!(normalize(""), "");
        assert_eq!(normalize("abc"), "ABC");
        assert_eq!(normalize("Hello World"), "HELLO WORLD");
        assert_eq!(normalize("mixedCASE123"), "MIXEDCASE123");
    }

    #[test]
    fn pattern_char_len_counts_unicode_characters() {
        assert_eq!(pattern_char_len(""), 0);
        assert_eq!(pattern_char_len("abc"), 3);
        assert_eq!(pattern_char_len("αβγ"), 3);
        assert_eq!(pattern_char_len("🎉🎊"), 2);
    }

    #[test]
    fn unique_tokens_returns_empty_for_short_strings() {
        assert!(unique_tokens("").is_empty());
        assert!(unique_tokens("a").is_empty());
        assert!(unique_tokens("ab").is_empty());
    }

    #[test]
    fn unique_tokens_generates_trigrams_for_valid_input() {
        let tokens = unique_tokens("abc");
        assert_eq!(tokens.len(), 1);

        let tokens = unique_tokens("abcd");
        assert_eq!(tokens.len(), 2);

        let tokens = unique_tokens("abcde");
        assert_eq!(tokens.len(), 3);
    }

    #[test]
    fn unique_tokens_deduplicates_via_btree_set() {
        // "ABA" should produce only one unique trigram: "ABA"
        let tokens = unique_tokens("ABA");
        assert_eq!(tokens.len(), 1);

        // "ABABA" should produce two: "ABA", "BAB"
        let tokens = unique_tokens("ABABA");
        assert_eq!(tokens.len(), 2);
    }

    #[test]
    fn pack_trigram_encodes_three_bytes() {
        assert_eq!(pack_trigram([0, 0, 0]), 0);
        assert_eq!(pack_trigram([1, 0, 0]), 0x010000);
        assert_eq!(pack_trigram([0, 1, 0]), 0x000100);
        assert_eq!(pack_trigram([0, 0, 1]), 0x000001);
        assert_eq!(pack_trigram([0x41, 0x42, 0x43]), 0x414243); // "ABC"
    }

    #[test]
    fn pack_trigram_is_order_sensitive() {
        let abc = pack_trigram([0x41, 0x42, 0x43]);
        let acb = pack_trigram([0x41, 0x43, 0x42]);
        let bac = pack_trigram([0x42, 0x41, 0x43]);
        assert_ne!(abc, acb);
        assert_ne!(abc, bac);
        assert_ne!(acb, bac);
    }

    #[test]
    fn guardrail_decision_use_index_when_safe() {
        // Long pattern with low selectivity should use index
        assert_eq!(
            decide_guardrails("abcdefgh", 100, false, 100_000),
            GuardrailDecision::UseIndex
        );

        // Short pattern with additional filter should use index
        assert_eq!(
            decide_guardrails("abcd", 100_000, true, 100_000),
            GuardrailDecision::UseIndex
        );
    }

    #[test]
    fn guardrail_decision_boundary_conditions() {
        // Exactly 3 chars should use index
        assert_eq!(
            decide_guardrails("abc", 100, false, 100_000),
            GuardrailDecision::UseIndex
        );

        // Exactly 5 chars without filter and high selectivity
        assert_eq!(
            decide_guardrails("abcde", 100_000, false, 100_000),
            GuardrailDecision::RequireAdditionalFilter
        );

        // Exactly 6 chars should not trigger cap unless over threshold
        assert_eq!(
            decide_guardrails("abcdef", 100_000, false, 100_000),
            GuardrailDecision::UseIndex
        );
    }

    #[test]
    fn guardrail_decision_too_short_for_various_lengths() {
        assert_eq!(
            decide_guardrails("", 100, false, 100_000),
            GuardrailDecision::TooShort
        );
        assert_eq!(
            decide_guardrails("a", 100, false, 100_000),
            GuardrailDecision::TooShort
        );
        assert_eq!(
            decide_guardrails("ab", 100, false, 100_000),
            GuardrailDecision::TooShort
        );
    }

    #[test]
    fn guardrail_decision_with_unicode_patterns() {
        // Unicode pattern length should be counted in characters
        assert_eq!(
            decide_guardrails("αβγ", 100, false, 100_000),
            GuardrailDecision::UseIndex
        );
        assert_eq!(
            decide_guardrails("αβ", 100, false, 100_000),
            GuardrailDecision::TooShort
        );
    }

    #[test]
    fn unique_tokens_handles_multibyte_unicode() {
        // Trigrams should work with multi-byte UTF-8 characters
        let tokens = unique_tokens("αβγδ");
        assert!(!tokens.is_empty());
    }

    #[test]
    fn guardrail_decision_cap_results_threshold_variations() {
        // Pattern > 5 chars with postings over threshold should cap
        assert_eq!(
            decide_guardrails("abcdef", 100_001, false, 100_000),
            GuardrailDecision::CapResults { limit: 100_000 }
        );

        // Pattern > 5 chars with postings at threshold should use index
        assert_eq!(
            decide_guardrails("abcdef", 100_000, false, 100_000),
            GuardrailDecision::UseIndex
        );
    }
}
