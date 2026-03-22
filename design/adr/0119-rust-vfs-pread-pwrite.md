# Virtual File System (VFS) and Thread-Safe I/O in Rust
**Date:** 2026-03-22
**Status:** Accepted

### Context
DecentDB requires highly concurrent reads (multiple reader threads) and durable writes. In standard Rust `std::fs::File`, reading (`Read` trait) requires a mutable reference (`&mut File`), which forces a lock/mutex if shared across threads, destroying concurrent read performance. Alternatively, `mmap` can be used, but memory-mapped files in Rust are inherently `unsafe` because another process modifying or truncating the file can cause a SIGBUS, crashing the Rust process.

### Decision
1. Do not use `mmap` for the primary page cache to avoid SIGBUS safety violations.
2. Use the OS-specific extensions for positional I/O:
   - On Unix: `std::os::unix::fs::FileExt` (`read_at`, `write_at`).
   - On Windows: `std::os::windows::fs::FileExt` (`seek_read`, `seek_write`).
3. Define a `Vfs` trait in Rust that abstracts these operations.
4. Implement a `FaultyVfs` struct that wraps the standard VFS to inject partial writes and dropped `fsync` calls for testing.

### Rationale
- `read_at` and `write_at` take an immutable reference (`&File`), allowing multiple reader threads to read from the disk concurrently without acquiring a Mutex.
- Completely avoids the `unsafe` requirements of memory-mapped files.
- Simplifies the `FaultyVfs` implementation for crash-injection tests.
