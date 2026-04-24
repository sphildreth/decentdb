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
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::sync::Arc;

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

#[derive(Clone)]
pub(crate) struct EngineAllocHandle(Arc<dyn EngineAlloc>);

impl EngineAllocHandle {
    #[must_use]
    pub(crate) fn new<A: EngineAlloc + 'static>(alloc: A) -> Self {
        Self(Arc::new(alloc))
    }

    unsafe fn alloc_bytes(&self, layout: Layout) -> Option<NonNull<u8>> {
        // SAFETY: forwarded to the wrapped allocator with the same contract.
        unsafe { self.0.alloc_bytes(layout) }
    }

    unsafe fn dealloc_bytes(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: forwarded to the wrapped allocator with the same contract.
        unsafe { self.0.dealloc_bytes(ptr, layout) };
    }
}

impl fmt::Debug for EngineAllocHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EngineAllocHandle(..)")
    }
}

impl Default for EngineAllocHandle {
    fn default() -> Self {
        Self::new(DefaultEngineAlloc)
    }
}

#[derive(Debug)]
pub(crate) struct EngineByteBuf {
    alloc: EngineAllocHandle,
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

impl EngineByteBuf {
    #[must_use]
    pub(crate) fn new_in(alloc: EngineAllocHandle) -> Self {
        Self {
            alloc,
            ptr: NonNull::dangling(),
            len: 0,
            cap: 0,
        }
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn clear(&mut self) {
        self.len = 0;
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        let required = self.len.saturating_add(additional);
        if required <= self.cap {
            return;
        }
        let new_cap = required.max(self.cap.saturating_mul(2)).max(64);
        self.grow(new_cap);
    }

    pub(crate) fn resize(&mut self, new_len: usize, value: u8) {
        if new_len <= self.len {
            self.len = new_len;
            return;
        }
        self.reserve(new_len - self.len);
        let tail = new_len - self.len;
        // SAFETY: `reserve` above guarantees `self.ptr.add(self.len)..+tail`
        // is within the allocated range. We then mark those bytes initialized
        // by updating `self.len`.
        unsafe {
            ptr::write_bytes(self.ptr.as_ptr().add(self.len), value, tail);
        }
        self.len = new_len;
    }

    pub(crate) fn extend_from_slice(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        self.reserve(bytes.len());
        // SAFETY: `reserve` ensures the destination range is allocated and
        // non-overlapping with `bytes`.
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr.as_ptr().add(self.len), bytes.len());
        }
        self.len += bytes.len();
    }

    #[must_use]
    pub(crate) fn as_slice(&self) -> &[u8] {
        self
    }

    fn grow(&mut self, new_cap: usize) {
        debug_assert!(new_cap >= self.len);
        let layout = Layout::array::<u8>(new_cap).expect("valid engine byte buffer layout");
        // SAFETY: `layout` is constructed for `u8` and `new_cap > 0`.
        let new_ptr = unsafe {
            self.alloc
                .alloc_bytes(layout)
                .unwrap_or_else(|| std::alloc::handle_alloc_error(layout))
        };
        if self.len != 0 {
            // SAFETY: both pointers reference disjoint allocations of at least
            // `self.len` bytes.
            unsafe {
                ptr::copy_nonoverlapping(self.ptr.as_ptr(), new_ptr.as_ptr(), self.len);
            }
        }
        if self.cap != 0 {
            let old_layout =
                Layout::array::<u8>(self.cap).expect("valid engine byte buffer layout");
            // SAFETY: `self.ptr` came from the same allocator and `old_layout`.
            unsafe {
                self.alloc.dealloc_bytes(self.ptr, old_layout);
            }
        }
        self.ptr = new_ptr;
        self.cap = new_cap;
    }
}

impl Deref for EngineByteBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: `self.ptr` either points at a valid `cap`-sized allocation
        // or is dangling with `len == 0`. In both cases the first `len` bytes
        // are initialized by the buffer methods above.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for EngineByteBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: same invariants as `Deref`; unique `&mut self` guarantees
        // unique access to the initialized prefix.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for EngineByteBuf {
    fn drop(&mut self) {
        if self.cap == 0 {
            return;
        }
        let layout = Layout::array::<u8>(self.cap).expect("valid engine byte buffer layout");
        // SAFETY: `self.ptr` was allocated by `self.alloc` with `layout`.
        unsafe {
            self.alloc.dealloc_bytes(self.ptr, layout);
        }
    }
}

// SAFETY: `EngineByteBuf` owns its allocation exclusively, only exposes raw
// access through `&mut self`, and delegates allocation/free to an allocator
// that is itself required to be `Send + Sync`.
unsafe impl Send for EngineByteBuf {}

// SAFETY: shared references only expose immutable byte slices. Allocation and
// deallocation still require unique ownership via `&mut self` / `Drop`.
unsafe impl Sync for EngineByteBuf {}

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

    #[test]
    fn engine_byte_buf_round_trips_bytes() {
        let mut buf = EngineByteBuf::new_in(EngineAllocHandle::default());
        buf.extend_from_slice(b"root");
        buf.resize(8, b'!');
        assert_eq!(&buf[..4], b"root");
        assert_eq!(&buf[4..], b"!!!!");
        buf.clear();
        assert!(buf.is_empty());
        buf.extend_from_slice(b"ok");
        assert_eq!(&buf[..], b"ok");
    }
}
