# Rust FFI & Panic Safety Strategy
**Date:** 2026-03-22
**Status:** Accepted

### Context
DecentDB relies heavily on its C-ABI (`extern "C"`) to support Entity Framework, Python, Java, and other bindings. In Rust, if a panic occurs and unwinds across an `extern "C"` boundary into the calling C/C# code, it results in **Undefined Behavior (UB)**, which will immediately crash the host runtime (e.g., the .NET CLR or JVM).

### Decision
1. All functions exported via the C-ABI MUST be wrapped in `std::panic::catch_unwind`.
2. If a panic is caught, the FFI boundary must return a predefined C-compatible error code (e.g., `DDB_ERR_PANIC`) and store the panic string in a thread-local error buffer so the host language can retrieve it.
3. Raw pointers (`*mut T`, `*const T`) passed across the boundary must be explicitly converted using `Box::into_raw` and `Box::from_raw`. 
4. The Rust compiler must be configured to abort on panic in release builds where unwinding isn't strictly necessary, but for the shared library CDYLIB, `catch_unwind` acts as the definitive safety net.

### Rationale
- Prevents host application crashes.
- Ensures the bindings developed for the Nim engine continue to function identically with the Rust engine.
- Satisfies the strict safety guarantees required by the project pillars.
