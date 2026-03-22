# Architecture Decision Records (ADRs)

This directory contains the historical and active ADRs for DecentDB. 

> **Note on Language Context:** DecentDB was originally written in Nim and has been completely rewritten in Rust. Many of the ADRs (0001 through 0117) use `.nim` file paths in their examples. The *architectural decisions* (e.g., WAL formats, B-Tree layouts, SQL semantics) remain 100% valid and binding for the Rust engine. 

### Recent Rust-Specific ADRs:
- **0118-rust-ffi-panic-safety.md**: Mandates `catch_unwind` on all C-ABI boundaries.
- **0119-rust-vfs-pread-pwrite.md**: Mandates standard file positional I/O over `unsafe mmap` for the Virtual File System.
