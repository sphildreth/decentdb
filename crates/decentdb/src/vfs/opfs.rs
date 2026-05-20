//! Browser OPFS-backed VFS for the wasm worker runtime.
//!
//! OPFS sync access handles must be prepared by the JavaScript worker before
//! the Rust engine opens a database. The Rust side intentionally stays
//! synchronous and maps VFS calls onto synchronous host functions.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

use crate::error::{DbError, Result};

use super::{FileKind, OpenMode, Vfs, VfsFile};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_open, catch)]
    fn js_opfs_open(path: &str, mode: &str, kind: &str) -> std::result::Result<(), JsValue>;

    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_exists, catch)]
    fn js_opfs_exists(path: &str, kind: &str) -> std::result::Result<bool, JsValue>;

    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_remove, catch)]
    fn js_opfs_remove(path: &str, kind: &str) -> std::result::Result<(), JsValue>;

    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_read, catch)]
    fn js_opfs_read(
        path: &str,
        kind: &str,
        offset: f64,
        len: u32,
    ) -> std::result::Result<Uint8Array, JsValue>;

    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_write, catch)]
    fn js_opfs_write(
        path: &str,
        kind: &str,
        offset: f64,
        bytes: Uint8Array,
    ) -> std::result::Result<u32, JsValue>;

    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_size, catch)]
    fn js_opfs_size(path: &str, kind: &str) -> std::result::Result<f64, JsValue>;

    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_set_len, catch)]
    fn js_opfs_set_len(path: &str, kind: &str, len: f64) -> std::result::Result<(), JsValue>;

    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_flush, catch)]
    fn js_opfs_flush(path: &str, kind: &str) -> std::result::Result<(), JsValue>;

    #[wasm_bindgen(js_namespace = globalThis, js_name = __decentdb_opfs_close, catch)]
    fn js_opfs_close(path: &str, kind: &str) -> std::result::Result<(), JsValue>;
}

#[derive(Debug, Default)]
pub(crate) struct OpfsVfs;

impl Vfs for OpfsVfs {
    fn open(&self, path: &Path, mode: OpenMode, kind: FileKind) -> Result<Arc<dyn VfsFile>> {
        let path = path.to_string_lossy().to_string();
        js_opfs_open(&path, mode_label(mode), kind_label(kind)).map_err(|source| {
            opfs_error(format!("open {} file at {path}", kind_label(kind)), source)
        })?;
        Ok(Arc::new(OpfsFile {
            kind,
            path: PathBuf::from(path),
        }))
    }

    fn file_exists(&self, path: &Path) -> Result<bool> {
        let path = path.to_string_lossy();
        js_opfs_exists(path.as_ref(), "database")
            .map_err(|source| opfs_error(format!("check OPFS file existence for {path}"), source))
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        let path = path.to_string_lossy();
        js_opfs_remove(path.as_ref(), "database")
            .map_err(|source| opfs_error(format!("remove OPFS file {path}"), source))
    }

    fn canonicalize_path(&self, path: &Path) -> Result<PathBuf> {
        Ok(path.to_path_buf())
    }
}

#[derive(Debug)]
struct OpfsFile {
    kind: FileKind,
    path: PathBuf,
}

impl VfsFile for OpfsFile {
    fn kind(&self) -> FileKind {
        self.kind
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let path = self.path.to_string_lossy();
        let bytes = js_opfs_read(
            path.as_ref(),
            kind_label(self.kind),
            offset as f64,
            buf.len() as u32,
        )
        .map_err(|source| {
            opfs_error(
                format!(
                    "read {} file at {}",
                    kind_label(self.kind),
                    self.path.display()
                ),
                source,
            )
        })?;
        let len = usize::try_from(bytes.length()).map_err(|_| {
            DbError::internal("OPFS read returned a byte buffer that does not fit in usize")
        })?;
        if len > buf.len() {
            return Err(DbError::internal(format!(
                "OPFS read returned {len} bytes into a {} byte buffer",
                buf.len()
            )));
        }
        bytes.copy_to(&mut buf[..len]);
        Ok(len)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        let path = self.path.to_string_lossy();
        let bytes = Uint8Array::from(buf);
        let written = js_opfs_write(path.as_ref(), kind_label(self.kind), offset as f64, bytes)
            .map_err(|source| {
                opfs_error(
                    format!(
                        "write {} file at {}",
                        kind_label(self.kind),
                        self.path.display()
                    ),
                    source,
                )
            })?;
        Ok(written as usize)
    }

    fn advise_sequential(&self) -> Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> Result<()> {
        self.flush()
    }

    fn sync_metadata(&self) -> Result<()> {
        self.flush()
    }

    fn file_size(&self) -> Result<u64> {
        let path = self.path.to_string_lossy();
        let size = js_opfs_size(path.as_ref(), kind_label(self.kind)).map_err(|source| {
            opfs_error(
                format!(
                    "stat {} file at {}",
                    kind_label(self.kind),
                    self.path.display()
                ),
                source,
            )
        })?;
        if size < 0.0 {
            return Err(DbError::corruption("OPFS reported a negative file size"));
        }
        Ok(size as u64)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        let path = self.path.to_string_lossy();
        js_opfs_set_len(path.as_ref(), kind_label(self.kind), len as f64).map_err(|source| {
            opfs_error(
                format!(
                    "resize {} file at {}",
                    kind_label(self.kind),
                    self.path.display()
                ),
                source,
            )
        })
    }
}

impl OpfsFile {
    fn flush(&self) -> Result<()> {
        let path = self.path.to_string_lossy();
        js_opfs_flush(path.as_ref(), kind_label(self.kind)).map_err(|source| {
            opfs_error(
                format!(
                    "flush {} file at {}",
                    kind_label(self.kind),
                    self.path.display()
                ),
                source,
            )
        })
    }
}

impl Drop for OpfsFile {
    fn drop(&mut self) {
        let path = self.path.to_string_lossy();
        let _ = js_opfs_close(path.as_ref(), kind_label(self.kind));
    }
}

fn mode_label(mode: OpenMode) -> &'static str {
    match mode {
        OpenMode::CreateNew => "createNew",
        OpenMode::OpenExisting => "openExisting",
        OpenMode::OpenOrCreate => "openOrCreate",
    }
}

fn kind_label(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Database => "database",
        FileKind::Wal => "wal",
        FileKind::SyncJournal => "sync-journal",
    }
}

fn opfs_error(context: impl Into<String>, source: JsValue) -> DbError {
    let message = source.as_string().unwrap_or_else(|| {
        js_sys::Error::from(source)
            .message()
            .as_string()
            .unwrap_or_else(|| "JavaScript OPFS host error".to_string())
    });
    DbError::io(
        context.into(),
        std::io::Error::new(std::io::ErrorKind::Other, message),
    )
}
