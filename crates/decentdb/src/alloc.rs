//! Per-engine allocator boundary — initial scaffold for ADR 0142.
//!
//! This module establishes the type-system seam through which future work
//! will route hot-path allocations (WAL payloads, page cache buffers,
//! `TableData` rows) so that each `Db` instance can be metered or capped
//! independently.
//!
//! Scope of this scaffold:
//! - Define the `EngineAlloc` trait describing the minimum interface
//!   future call sites need (allocate / free a tracked byte buffer).
//! - Provide `DefaultEngineAlloc`, a zero-cost forwarder that delegates to
//!   the global allocator. Today every engine uses this, so behavior is
//!   unchanged.
//!
//! Out of scope (tracked as ADR 0142 follow-up work):
//! - Plumbing the trait through `PagerHandle`, `WalIndex`, `TableData`.
//! - Per-engine accounting, watermarks, and quota enforcement.
//! - Configurable replacement allocators (jemalloc / mimalloc per engine).
//!
//! Keeping the trait private (`pub(crate)`) avoids freezing the surface
//! before the call-site plumbing has informed the final shape.

#![allow(dead_code)]

use std::alloc::Layout;

/// Minimum interface for allocations attributable to a single engine
/// instance. The trait intentionally mirrors the global allocator API so
/// the default implementation is a transparent forwarder.
///
/// # Safety
///
/// Implementors **must** be `Send + Sync` because allocations occur on
/// both the writer thread and reader threads. They must also uphold the
/// same per-pointer invariants as `std::alloc::GlobalAlloc`.
pub(crate) unsafe trait EngineAlloc: Send + Sync {
    /// Allocate `layout.size()` bytes aligned to `layout.align()`.
    /// Returns `None` if the underlying allocator returned null (OOM or
    /// quota exhaustion in a future implementation).
    ///
    /// # Safety
    /// The returned pointer, if `Some`, must be released with
    /// `dealloc_bytes` using the same `layout`.
    unsafe fn alloc_bytes(&self, layout: Layout) -> Option<std::ptr::NonNull<u8>>;

    /// Release a buffer previously returned by `alloc_bytes`.
    ///
    /// # Safety
    /// `ptr` must have been returned by a prior `alloc_bytes` call on
    /// `self` with an identical `layout`.
    unsafe fn dealloc_bytes(&self, ptr: std::ptr::NonNull<u8>, layout: Layout);
}

/// Zero-cost forwarder to the process-wide global allocator. This is the
/// behavior every engine ships with today; future work will let embedders
/// substitute a metered or per-engine implementation.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DefaultEngineAlloc;

// SAFETY: forwards verbatim to `std::alloc::{alloc, dealloc}`. The
// invariants required of callers of `EngineAlloc` are exactly the
// invariants required of the global allocator API.
unsafe impl EngineAlloc for DefaultEngineAlloc {
    unsafe fn alloc_bytes(&self, layout: Layout) -> Option<std::ptr::NonNull<u8>> {
        if layout.size() == 0 {
            return std::ptr::NonNull::new(layout.align() as *mut u8);
        }
        // SAFETY: `layout.size() != 0` per the guard above; alignment is
        // validated by `Layout::from_size_align` at construction.
        let raw = unsafe { std::alloc::alloc(layout) };
        std::ptr::NonNull::new(raw)
    }

    unsafe fn dealloc_bytes(&self, ptr: std::ptr::NonNull<u8>, layout: Layout) {
        if layout.size() == 0 {
            return;
        }
        // SAFETY: caller guarantees `ptr` was produced by `alloc_bytes`
        // with the same `layout`, matching `dealloc`'s requirements.
        unsafe { std::alloc::dealloc(ptr.as_ptr(), layout) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_alloc_round_trips() {
        let alloc = DefaultEngineAlloc;
        let layout = Layout::from_size_align(64, 8).unwrap();
        unsafe {
            let ptr = alloc.alloc_bytes(layout).expect("alloc should succeed");
            // Touch every byte to confirm the allocation is usable.
            std::ptr::write_bytes(ptr.as_ptr(), 0xAB, layout.size());
            alloc.dealloc_bytes(ptr, layout);
        }
    }

    #[test]
    fn default_alloc_handles_zero_sized() {
        let alloc = DefaultEngineAlloc;
        let layout = Layout::from_size_align(0, 8).unwrap();
        unsafe {
            let ptr = alloc
                .alloc_bytes(layout)
                .expect("zero-sized alloc returns sentinel");
            alloc.dealloc_bytes(ptr, layout);
        }
    }
}
