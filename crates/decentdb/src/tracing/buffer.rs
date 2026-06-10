#![allow(dead_code)]

/// Owned snapshot returned from a ring buffer; detached from live locks.
#[derive(Clone, Debug)]
pub struct BoundedSnapshot<T> {
    pub items: Vec<T>,
    pub eviction_count: u64,
    pub newest_event_id: u64,
    pub oldest_event_id: u64,
}

/// Fixed-capacity ring buffer with overwrite-when-full semantics.
///
/// Not Sync on its own; place inside a Mutex if shared across threads.
pub struct BoundedRingBuffer<T> {
    entries: Vec<T>,
    head: usize, // next insertion point
    tail: usize, // oldest valid element
    len: usize,
    eviction_count: u64,
    newest_event_id: u64,
    oldest_event_id: u64,
}

impl<T: std::fmt::Debug> std::fmt::Debug for BoundedRingBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedRingBuffer")
            .field("capacity", &self.entries.capacity())
            .field("len", &self.len)
            .field("eviction_count", &self.eviction_count)
            .field("newest_event_id", &self.newest_event_id)
            .field("oldest_event_id", &self.oldest_event_id)
            .finish_non_exhaustive()
    }
}

impl<T> BoundedRingBuffer<T> {
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            head: 0,
            tail: 0,
            len: 0,
            eviction_count: 0,
            newest_event_id: 0,
            oldest_event_id: 0,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Reset to empty, returning previous metadata counts.
    pub fn reset(&mut self) -> (u64, u64) {
        let evictions = self.eviction_count;
        let dropped = self.len as u64;
        self.entries.clear();
        self.entries.shrink_to_fit();
        self.head = 0;
        self.tail = 0;
        self.len = 0;
        self.eviction_count = 0;
        self.newest_event_id = 0;
        self.oldest_event_id = 0;
        (evictions, dropped)
    }

    pub fn push_back(&mut self, item: T) {
        if self.entries.capacity() == 0 {
            return;
        }
        if self.entries.len() < self.entries.capacity() {
            self.entries.push(item);
            self.head = self.entries.len();
        } else {
            let cap = self.entries.capacity();
            let idx = self.head % cap;
            self.entries[idx] = item;
            self.head = (self.head + 1) % cap;
            self.tail = self.head;
            self.eviction_count = self.eviction_count.wrapping_add(1);
        }
        if self.len < self.entries.capacity() {
            self.len += 1;
        }
        // id tracking is external; callers set ids via set_newest_id
    }

    pub fn set_newest_id(&mut self, id: u64) {
        self.newest_event_id = id;
        if self.len == 1 || self.oldest_event_id == 0 {
            self.oldest_event_id = id;
        }
    }

    /// Produce an owned snapshot under the caller's lock.
    pub fn snapshot<F, S>(&self, mut transform: F) -> BoundedSnapshot<S>
    where
        F: FnMut(&T) -> S,
    {
        let mut items = Vec::with_capacity(self.len);
        if self.len == 0 {
            return BoundedSnapshot {
                items,
                eviction_count: self.eviction_count,
                newest_event_id: self.newest_event_id,
                oldest_event_id: self.oldest_event_id,
            };
        }
        let cap = self.entries.capacity();
        for i in 0..self.len {
            let idx = (self.tail + i) % cap;
            items.push(transform(&self.entries[idx]));
        }
        BoundedSnapshot {
            items,
            eviction_count: self.eviction_count,
            newest_event_id: self.newest_event_id,
            oldest_event_id: self.oldest_event_id,
        }
    }

    /// Drains elements while `predicate` returns `true` and returns their count.
    pub(crate) fn drain_if(&mut self, mut predicate: impl FnMut(&T) -> bool) -> usize {
        let mut removed = 0;
        while self.len > 0 {
            let idx = self.tail % self.entries.len();
            if predicate(&self.entries[idx]) {
                self.tail = (self.tail + 1) % self.entries.len();
                self.len -= 1;
                self.eviction_count = self.eviction_count.wrapping_add(1);
                removed += 1;
                if self.len == 0 {
                    self.head = 0;
                    self.tail = 0;
                    break;
                }
            } else {
                break;
            }
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_buffer_yields_no_items() {
        let buf: BoundedRingBuffer<i32> = BoundedRingBuffer::with_capacity(4);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        let snap = buf.snapshot(|x| *x);
        assert!(snap.items.is_empty());
    }

    #[test]
    fn fill_and_wrap_overwrites_oldest() {
        let mut buf = BoundedRingBuffer::with_capacity(3);
        buf.push_back(1);
        buf.push_back(2);
        buf.push_back(3);
        let s = buf.snapshot(|x| *x);
        assert_eq!(s.items, vec![1, 2, 3]);
        buf.push_back(4);
        let s = buf.snapshot(|x| *x);
        assert_eq!(s.items, vec![2, 3, 4]);
        assert_eq!(s.eviction_count, 1);
    }

    #[test]
    fn push_capacity_zero_is_noop() {
        let mut buf = BoundedRingBuffer::with_capacity(0);
        buf.push_back(1);
        assert!(buf.is_empty());
        assert_eq!(buf.eviction_count, 0);
    }
}
