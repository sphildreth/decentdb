//! Platform-specific memory management helpers.
//!
//! Implements:
//! - design/adr/0138-post-checkpoint-heap-release.md

/// On Linux/glibc, return freed heap arenas to the operating system via
/// `malloc_trim(0)`. No-op on other platforms.
///
/// Safe to call from any thread; the writer invokes this at most once per
/// successful checkpoint, with all WAL locks released. The return value is
/// ignored — `malloc_trim` is best-effort.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) fn release_freed_heap() {
    // SAFETY: `malloc_trim` from glibc takes one `size_t` argument, returns
    // an `int`, and is documented as thread-safe. We call it with `0` (trim
    // as much as possible) and discard the result.
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    unsafe {
        let _ = malloc_trim(0);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(crate) fn release_freed_heap() {}
