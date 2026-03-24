//! Unit tests for trigram index rebuild state.

#[cfg(test)]
mod tests {
    use crate::search::rebuild::{Freshness, RebuildState};

    #[test]
    fn test_freshness_variants() {
        assert_eq!(Freshness::Fresh, Freshness::Fresh);
        assert_eq!(Freshness::Stale, Freshness::Stale);
        assert_ne!(Freshness::Fresh, Freshness::Stale);
    }

    #[test]
    fn test_freshness_debug() {
        let debug_fresh = format!("{:?}", Freshness::Fresh);
        assert_eq!(debug_fresh, "Fresh");

        let debug_stale = format!("{:?}", Freshness::Stale);
        assert_eq!(debug_stale, "Stale");
    }

    #[test]
    fn test_freshness_copy() {
        let original = Freshness::Fresh;
        let copied = original;
        assert_eq!(original, Freshness::Fresh);
        assert_eq!(copied, Freshness::Fresh);
    }

    #[test]
    fn test_rebuild_state_default() {
        let state = RebuildState::default();
        assert_eq!(state.freshness(), Freshness::Fresh);
    }

    #[test]
    fn test_rebuild_state_mark_stale() {
        let mut state = RebuildState::default();
        assert_eq!(state.freshness(), Freshness::Fresh);

        state.mark_stale();
        assert_eq!(state.freshness(), Freshness::Stale);

        state.mark_stale();
        assert_eq!(state.freshness(), Freshness::Stale);
    }

    #[test]
    fn test_rebuild_state_mark_rebuilt() {
        let mut state = RebuildState::default();
        state.mark_stale();
        assert_eq!(state.freshness(), Freshness::Stale);

        state.mark_rebuilt();
        assert_eq!(state.freshness(), Freshness::Fresh);

        state.mark_rebuilt();
        assert_eq!(state.freshness(), Freshness::Fresh);
    }

    #[test]
    fn test_rebuild_state_mark_stale_then_rebuilt() {
        let mut state = RebuildState::default();
        assert_eq!(state.freshness(), Freshness::Fresh);

        state.mark_stale();
        assert_eq!(state.freshness(), Freshness::Stale);

        state.mark_rebuilt();
        assert_eq!(state.freshness(), Freshness::Fresh);
    }

    #[test]
    fn test_rebuild_state_ensure_fresh_when_fresh() {
        let mut state = RebuildState::default();
        let mut rebuild_called = false;

        let result = state.ensure_fresh(|| {
            rebuild_called = true;
            Ok(())
        });

        assert!(result.is_ok());
        assert!(!rebuild_called);
        assert_eq!(state.freshness(), Freshness::Fresh);
    }

    #[test]
    fn test_rebuild_state_ensure_fresh_when_stale() {
        let mut state = RebuildState::default();
        state.mark_stale();
        let mut rebuild_called = false;

        let result = state.ensure_fresh(|| {
            rebuild_called = true;
            Ok(())
        });

        assert!(result.is_ok());
        assert!(rebuild_called);
        assert_eq!(state.freshness(), Freshness::Fresh);
    }

    #[test]
    fn test_rebuild_state_ensure_fresh_propagates_error() {
        let mut state = RebuildState::default();
        state.mark_stale();
        let mut rebuild_called = false;

        let result = state.ensure_fresh(|| {
            rebuild_called = true;
            Err(crate::error::DbError::internal("rebuild failed"))
        });

        assert!(result.is_err());
        assert!(rebuild_called);
        assert_eq!(state.freshness(), Freshness::Stale);
    }

    #[test]
    fn test_rebuild_state_ensure_fresh_multiple_calls() {
        let mut state = RebuildState::default();
        let mut rebuild_count = 0;

        let result1 = state.ensure_fresh(|| {
            rebuild_count += 1;
            Ok(())
        });
        assert!(result1.is_ok());
        assert_eq!(rebuild_count, 0);

        state.mark_stale();
        let result2 = state.ensure_fresh(|| {
            rebuild_count += 1;
            Ok(())
        });
        assert!(result2.is_ok());
        assert_eq!(rebuild_count, 1);

        let result3 = state.ensure_fresh(|| {
            rebuild_count += 1;
            Ok(())
        });
        assert!(result3.is_ok());
        assert_eq!(rebuild_count, 1);
    }

    #[test]
    fn test_rebuild_state_clone() {
        let mut original = RebuildState::default();
        original.mark_stale();

        let cloned = original.clone();
        assert_eq!(cloned.freshness(), Freshness::Stale);

        original.mark_rebuilt();
        assert_eq!(original.freshness(), Freshness::Fresh);
        assert_eq!(cloned.freshness(), Freshness::Stale);
    }

    #[test]
    fn test_rebuild_state_debug() {
        let state = RebuildState::default();
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("RebuildState"));
    }

    #[test]
    fn test_rebuild_state_stale_flag_persists() {
        let mut state = RebuildState::default();

        for _ in 0..5 {
            state.mark_stale();
            assert_eq!(state.freshness(), Freshness::Stale);
        }

        state.mark_rebuilt();
        assert_eq!(state.freshness(), Freshness::Fresh);

        for _ in 0..5 {
            state.mark_stale();
            assert_eq!(state.freshness(), Freshness::Stale);
        }
    }

    #[test]
    fn test_rebuild_state_ensure_fresh_idempotent() {
        let mut state = RebuildState::default();

        for _ in 0..10 {
            let result = state.ensure_fresh(|| Ok(()));
            assert!(result.is_ok());
            assert_eq!(state.freshness(), Freshness::Fresh);
        }
    }

    #[test]
    fn test_rebuild_state_ensure_fresh_error_preserves_stale() {
        let mut state = RebuildState::default();
        state.mark_stale();

        for i in 0..3 {
            let result = state.ensure_fresh(|| {
                Err(crate::error::DbError::internal(format!(
                    "error {}",
                    i
                )))
            });
            assert!(result.is_err());
            assert_eq!(state.freshness(), Freshness::Stale);
        }
    }

    #[test]
    fn test_rebuild_state_ensure_fresh_success_changes_state() {
        let mut state = RebuildState::default();
        state.mark_stale();

        let mut call_count = 0;
        let result = state.ensure_fresh(|| {
            call_count += 1;
            Ok(())
        });

        assert!(result.is_ok());
        assert_eq!(call_count, 1);
        assert_eq!(state.freshness(), Freshness::Fresh);

        let result2 = state.ensure_fresh(|| {
            call_count += 1;
            Ok(())
        });

        assert!(result2.is_ok());
        assert_eq!(call_count, 1);
        assert_eq!(state.freshness(), Freshness::Fresh);
    }
}
