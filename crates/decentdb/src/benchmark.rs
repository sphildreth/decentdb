//! Benchmark-only instrumentation helpers.
//!
//! This surface is intentionally gated behind the `bench-internals` feature.

use crate::vfs::stats;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VfsFileStats {
    pub open_calls: u64,
    pub read_calls: u64,
    pub write_calls: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub sync_data_calls: u64,
    pub sync_metadata_calls: u64,
    pub set_len_calls: u64,
}

impl VfsFileStats {
    #[must_use]
    pub fn sync_calls(self) -> u64 {
        self.sync_data_calls
            .saturating_add(self.sync_metadata_calls)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VfsStats {
    pub db: VfsFileStats,
    pub wal: VfsFileStats,
    pub open_create_like_calls: u64,
    pub file_exists_calls: u64,
    pub remove_file_calls: u64,
    pub canonicalize_calls: u64,
}

impl VfsStats {
    #[must_use]
    pub fn total(self) -> VfsFileStats {
        VfsFileStats {
            open_calls: self.db.open_calls.saturating_add(self.wal.open_calls),
            read_calls: self.db.read_calls.saturating_add(self.wal.read_calls),
            write_calls: self.db.write_calls.saturating_add(self.wal.write_calls),
            bytes_read: self.db.bytes_read.saturating_add(self.wal.bytes_read),
            bytes_written: self.db.bytes_written.saturating_add(self.wal.bytes_written),
            sync_data_calls: self
                .db
                .sync_data_calls
                .saturating_add(self.wal.sync_data_calls),
            sync_metadata_calls: self
                .db
                .sync_metadata_calls
                .saturating_add(self.wal.sync_metadata_calls),
            set_len_calls: self.db.set_len_calls.saturating_add(self.wal.set_len_calls),
        }
    }
}

#[derive(Debug)]
pub struct VfsStatsScope {
    active: bool,
}

impl VfsStatsScope {
    /// Starts benchmark VFS accounting and optionally clears prior counters.
    #[must_use]
    pub fn begin(reset_counters: bool) -> Self {
        if reset_counters {
            stats::reset();
        }
        stats::set_enabled(true);
        Self { active: true }
    }

    /// Stops accounting before scope drop.
    pub fn end(mut self) {
        if self.active {
            stats::set_enabled(false);
            self.active = false;
        }
    }
}

impl Drop for VfsStatsScope {
    fn drop(&mut self) {
        if self.active {
            stats::set_enabled(false);
            self.active = false;
        }
    }
}

/// Enables benchmark VFS accounting globally for the current process.
pub fn enable_vfs_stats() {
    stats::set_enabled(true);
}

/// Disables benchmark VFS accounting globally for the current process.
pub fn disable_vfs_stats() {
    stats::set_enabled(false);
}

/// Clears all benchmark VFS counters.
pub fn reset_vfs_stats() {
    stats::reset();
}

/// Returns the current benchmark VFS counter snapshot.
#[must_use]
pub fn snapshot_vfs_stats() -> VfsStats {
    let snapshot = stats::snapshot();
    VfsStats {
        db: VfsFileStats {
            open_calls: snapshot.db.open_calls,
            read_calls: snapshot.db.read_calls,
            write_calls: snapshot.db.write_calls,
            bytes_read: snapshot.db.bytes_read,
            bytes_written: snapshot.db.bytes_written,
            sync_data_calls: snapshot.db.sync_data_calls,
            sync_metadata_calls: snapshot.db.sync_metadata_calls,
            set_len_calls: snapshot.db.set_len_calls,
        },
        wal: VfsFileStats {
            open_calls: snapshot.wal.open_calls,
            read_calls: snapshot.wal.read_calls,
            write_calls: snapshot.wal.write_calls,
            bytes_read: snapshot.wal.bytes_read,
            bytes_written: snapshot.wal.bytes_written,
            sync_data_calls: snapshot.wal.sync_data_calls,
            sync_metadata_calls: snapshot.wal.sync_metadata_calls,
            set_len_calls: snapshot.wal.set_len_calls,
        },
        open_create_like_calls: snapshot.open_create_like_calls,
        file_exists_calls: snapshot.file_exists_calls,
        remove_file_calls: snapshot.remove_file_calls,
        canonicalize_calls: snapshot.canonicalize_calls,
    }
}
