#!/bin/bash
sed -i 's|//! Virtual filesystem abstractions for database and WAL I/O.|//! Virtual filesystem abstractions for database and WAL I/O.\n//!\n//! Implements:\n//! - design/adr/0119-rust-vfs-pread-pwrite.md\n//! - design/adr/0105-in-memory-vfs.md|g' crates/decentdb/src/vfs/mod.rs

sed -i 's|//! Pager and direct main-database page access.|//! Pager and direct main-database page access.\n//!\n//! Implements:\n//! - design/adr/0001-page-size.md|g' crates/decentdb/src/storage/pager.rs

sed -i 's|//! WAL header and frame encoding for the v8 layout.|//! WAL header and frame encoding for the v8 layout.\n//!\n//! Implements:\n//! - design/adr/0064-wal-frame-checksum-removal.md\n//! - design/adr/0065-wal-frame-lsn-removal.md\n//! - design/adr/0066-wal-frame-payload-length-removal.md\n//! - design/adr/0068-wal-header-end-offset.md|g' crates/decentdb/src/wal/format.rs

sed -i 's|//! Shared WAL acquisition keyed by canonical database path.|//! Shared WAL acquisition keyed by canonical database path.\n//!\n//! Implements:\n//! - design/adr/0117-shared-wal-registry.md|g' crates/decentdb/src/wal/shared.rs

sed -i 's|//! WAL append and durability logic.|//! WAL append and durability logic.\n//!\n//! Implements:\n//! - design/adr/0003-snapshot-lsn-atomicity.md|g' crates/decentdb/src/wal/writer.rs

sed -i 's|//! Reader-aware checkpoint copyback and WAL pruning.|//! Reader-aware checkpoint copyback and WAL pruning.\n//!\n//! Implements:\n//! - design/adr/0004-wal-checkpoint-strategy.md\n//! - design/adr/0056-wal-index-pruning-on-checkpoint.md|g' crates/decentdb/src/wal/checkpoint.rs

sed -i 's|//! Active-reader tracking for snapshot retention and checkpoint coordination.|//! Active-reader tracking for snapshot retention and checkpoint coordination.\n//!\n//! Implements:\n//! - design/adr/0018-checkpointing-reader-count-mechanism.md\n//! - design/adr/0019-wal-retention-for-active-readers.md\n//! - design/adr/0024-wal-growth-prevention-long-readers.md|g' crates/decentdb/src/wal/reader_registry.rs
