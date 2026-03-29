//! Deterministic fault-injection VFS wrapper.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::ThreadId;

use crate::error::{DbError, Result};

use super::{FileKind, OpenMode, Vfs, VfsFile};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FailAction {
    Error,
    PartialRead { bytes: usize },
    PartialWrite { bytes: usize },
    DropSync,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Failpoint {
    pub(crate) label: String,
    pub(crate) trigger_on: u64,
    pub(crate) action: FailAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FailpointLogEntry {
    pub(crate) label: String,
    pub(crate) hit: u64,
    pub(crate) outcome: String,
}

#[derive(Debug)]
pub(crate) struct FaultyVfs {
    inner: Arc<dyn Vfs>,
    state: Arc<FaultState>,
}

impl FaultyVfs {
    pub(crate) fn wrap(inner: Arc<dyn Vfs>) -> Self {
        Self {
            inner,
            state: global_fault_state(),
        }
    }
}

impl Vfs for FaultyVfs {
    fn open(&self, path: &Path, mode: OpenMode, kind: FileKind) -> Result<Arc<dyn VfsFile>> {
        let inner = self.inner.open(path, mode, kind)?;
        Ok(Arc::new(FaultyVfsFile {
            inner,
            state: Arc::clone(&self.state),
        }))
    }

    fn file_exists(&self, path: &Path) -> Result<bool> {
        self.inner.file_exists(path)
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        self.inner.remove_file(path)
    }

    fn canonicalize_path(&self, path: &Path) -> Result<PathBuf> {
        self.inner.canonicalize_path(path)
    }

    fn is_memory(&self) -> bool {
        self.inner.is_memory()
    }
}

#[derive(Debug)]
struct FaultyVfsFile {
    inner: Arc<dyn VfsFile>,
    state: Arc<FaultState>,
}

impl VfsFile for FaultyVfsFile {
    fn kind(&self) -> FileKind {
        self.inner.kind()
    }

    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let label = classify_read(self.inner.kind());
        match self.state.decision(label) {
            FaultDecision::Untracked => self.inner.read_at(offset, buf),
            FaultDecision::Pass { hit } => {
                self.state.log(label, hit, "pass");
                self.inner.read_at(offset, buf)
            }
            FaultDecision::Error { hit } => {
                self.state.log(label, hit, "error");
                Err(DbError::io(
                    format!("fault injected at {label}"),
                    std::io::Error::other("fault injected read error"),
                ))
            }
            FaultDecision::PartialRead { hit, bytes } => {
                self.state.log(label, hit, &format!("partial_read:{bytes}"));
                let mut scratch = vec![0_u8; bytes.min(buf.len())];
                let read = self.inner.read_at(offset, &mut scratch)?;
                buf[..read].copy_from_slice(&scratch[..read]);
                Ok(read)
            }
            FaultDecision::PartialWrite { hit, .. } => {
                self.state.log(label, hit, "pass");
                self.inner.read_at(offset, buf)
            }
            FaultDecision::DropSync { hit } => {
                self.state.log(label, hit, "pass");
                self.inner.read_at(offset, buf)
            }
        }
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        let label = classify_write(self.inner.kind(), offset, buf);
        match self.state.decision(label) {
            FaultDecision::Untracked => self.inner.write_at(offset, buf),
            FaultDecision::Pass { hit } => {
                self.state.log(label, hit, "pass");
                self.inner.write_at(offset, buf)
            }
            FaultDecision::Error { hit } => {
                self.state.log(label, hit, "error");
                Err(DbError::io(
                    format!("fault injected at {label}"),
                    std::io::Error::other("fault injected write error"),
                ))
            }
            FaultDecision::PartialWrite { hit, bytes } => {
                let shortened = bytes.min(buf.len());
                self.state
                    .log(label, hit, &format!("partial_write:{shortened}"));
                self.inner.write_at(offset, &buf[..shortened])
            }
            FaultDecision::PartialRead { hit, .. } => {
                self.state.log(label, hit, "pass");
                self.inner.write_at(offset, buf)
            }
            FaultDecision::DropSync { hit } => {
                self.state.log(label, hit, "pass");
                self.inner.write_at(offset, buf)
            }
        }
    }

    fn sync_data(&self) -> Result<()> {
        let label = classify_sync(self.inner.kind());
        match self.state.decision(label) {
            FaultDecision::Untracked => self.inner.sync_data(),
            FaultDecision::Pass { hit } => {
                self.state.log(label, hit, "pass");
                self.inner.sync_data()
            }
            FaultDecision::DropSync { hit } => {
                self.state.log(label, hit, "drop_sync");
                Ok(())
            }
            FaultDecision::Error { hit } => {
                self.state.log(label, hit, "error");
                Err(DbError::io(
                    format!("fault injected at {label}"),
                    std::io::Error::other("fault injected sync error"),
                ))
            }
            FaultDecision::PartialRead { hit, .. } | FaultDecision::PartialWrite { hit, .. } => {
                self.state.log(label, hit, "pass");
                self.inner.sync_data()
            }
        }
    }

    fn sync_metadata(&self) -> Result<()> {
        let label = classify_metadata_sync(self.inner.kind());
        match self.state.decision(label) {
            FaultDecision::Untracked => self.inner.sync_metadata(),
            FaultDecision::Pass { hit } => {
                self.state.log(label, hit, "pass");
                self.inner.sync_metadata()
            }
            FaultDecision::DropSync { hit } => {
                self.state.log(label, hit, "drop_sync");
                Ok(())
            }
            FaultDecision::Error { hit } => {
                self.state.log(label, hit, "error");
                Err(DbError::io(
                    format!("fault injected at {label}"),
                    std::io::Error::other("fault injected metadata sync error"),
                ))
            }
            FaultDecision::PartialRead { hit, .. } | FaultDecision::PartialWrite { hit, .. } => {
                self.state.log(label, hit, "pass");
                self.inner.sync_metadata()
            }
        }
    }

    fn file_size(&self) -> Result<u64> {
        self.inner.file_size()
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.inner.set_len(len)
    }
}

#[derive(Debug, Default)]
struct FaultState {
    failpoints: Mutex<HashMap<String, Vec<Failpoint>>>,
    hits: Mutex<HashMap<String, u64>>,
    logs: Mutex<Vec<FailpointLogEntry>>,
    owner_thread: Mutex<Option<ThreadId>>,
}

impl FaultState {
    fn decision(&self, label: &str) -> FaultDecision {
        if !self.is_owner_thread() {
            return FaultDecision::Untracked;
        }

        let failpoints = self.failpoints.lock().expect("fault state lock");
        if failpoints.is_empty() {
            return FaultDecision::Untracked;
        }

        let hit = {
            let mut hits = self.hits.lock().expect("fault hit counter lock");
            let next = hits.entry(label.to_string()).or_insert(0);
            *next += 1;
            *next
        };
        let Some(entries) = failpoints.get(label) else {
            return FaultDecision::Pass { hit };
        };

        for failpoint in entries {
            if failpoint.trigger_on == hit {
                return match failpoint.action {
                    FailAction::Error => FaultDecision::Error { hit },
                    FailAction::PartialRead { bytes } => FaultDecision::PartialRead { hit, bytes },
                    FailAction::PartialWrite { bytes } => {
                        FaultDecision::PartialWrite { hit, bytes }
                    }
                    FailAction::DropSync => FaultDecision::DropSync { hit },
                };
            }
        }

        FaultDecision::Pass { hit }
    }

    fn log(&self, label: &str, hit: u64, outcome: &str) {
        if !self.is_owner_thread() {
            return;
        }
        self.logs
            .lock()
            .expect("fault log lock")
            .push(FailpointLogEntry {
                label: label.to_string(),
                hit,
                outcome: outcome.to_string(),
            });
    }

    fn is_owner_thread(&self) -> bool {
        let owner = self.owner_thread.lock().expect("fault owner lock");
        owner
            .as_ref()
            .is_none_or(|thread_id| *thread_id == std::thread::current().id())
    }
}

enum FaultDecision {
    Untracked,
    Pass { hit: u64 },
    Error { hit: u64 },
    PartialRead { hit: u64, bytes: usize },
    PartialWrite { hit: u64, bytes: usize },
    DropSync { hit: u64 },
}

pub(crate) fn install_failpoint(failpoint: Failpoint) -> Result<()> {
    let state = global_fault_state();
    let mut owner = state
        .owner_thread
        .lock()
        .map_err(|_| DbError::internal("fault owner lock poisoned"))?;
    *owner = Some(std::thread::current().id());
    drop(owner);
    let mut failpoints = state
        .failpoints
        .lock()
        .map_err(|_| DbError::internal("fault state lock poisoned"))?;
    failpoints
        .entry(failpoint.label.clone())
        .or_default()
        .push(failpoint);
    Ok(())
}

pub(crate) fn clear_failpoints() -> Result<()> {
    let state = global_fault_state();
    state
        .failpoints
        .lock()
        .map_err(|_| DbError::internal("fault state lock poisoned"))?
        .clear();
    state
        .hits
        .lock()
        .map_err(|_| DbError::internal("fault state lock poisoned"))?
        .clear();
    state
        .logs
        .lock()
        .map_err(|_| DbError::internal("fault state lock poisoned"))?
        .clear();
    *state
        .owner_thread
        .lock()
        .map_err(|_| DbError::internal("fault owner lock poisoned"))? =
        Some(std::thread::current().id());
    Ok(())
}

pub(crate) fn failpoint_logs() -> Result<Vec<FailpointLogEntry>> {
    let state = global_fault_state();
    state
        .logs
        .lock()
        .map(|logs| logs.clone())
        .map_err(|_| DbError::internal("fault log lock poisoned"))
}

fn global_fault_state() -> Arc<FaultState> {
    static STATE: OnceLock<Arc<FaultState>> = OnceLock::new();
    STATE
        .get_or_init(|| Arc::new(FaultState::default()))
        .clone()
}

fn classify_read(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Database => "db.read",
        FileKind::Wal => "wal.read",
    }
}

fn classify_sync(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Database => "db.fsync",
        FileKind::Wal => "wal.fsync",
    }
}

fn classify_metadata_sync(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Database => "db.sync_metadata",
        FileKind::Wal => "wal.sync_metadata",
    }
}

fn classify_write(kind: FileKind, offset: u64, buf: &[u8]) -> &'static str {
    match kind {
        FileKind::Database => {
            if offset == 0 && buf.len() >= 128 {
                "db.write_header"
            } else {
                "db.write_page"
            }
        }
        FileKind::Wal => {
            if offset == 0 && buf.len() == 32 {
                "wal.write_header"
            } else {
                match buf.first().copied() {
                    Some(0) => "wal.write_frame",
                    Some(1) => "wal.write_commit",
                    Some(2) => "wal.write_checkpoint",
                    _ => "wal.write",
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::{Arc, Mutex, OnceLock};

    use crate::vfs::{write_all_at, FileKind, OpenMode, Vfs};

    use super::{
        clear_failpoints, failpoint_logs, install_failpoint, FailAction, Failpoint, FaultyVfs,
    };

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn partial_write_and_dropped_sync_are_reproducible() {
        let _guard = test_lock().lock().expect("test lock");
        clear_failpoints().expect("clear failpoints");
        install_failpoint(Failpoint {
            label: "db.write_page".to_string(),
            trigger_on: 1,
            action: FailAction::PartialWrite { bytes: 3 },
        })
        .expect("install partial write");
        install_failpoint(Failpoint {
            label: "db.fsync".to_string(),
            trigger_on: 1,
            action: FailAction::DropSync,
        })
        .expect("install dropped sync");

        let vfs = FaultyVfs::wrap(Arc::new(crate::vfs::mem::MemVfs::default()));
        let file = vfs
            .open(
                Path::new(":memory:"),
                OpenMode::CreateNew,
                FileKind::Database,
            )
            .expect("create file");

        let written = file.write_at(4096, &[1, 2, 3, 4, 5]).expect("write page");
        assert_eq!(written, 3);
        file.sync_data().expect("dropped sync returns success");

        let logs = failpoint_logs().expect("read logs");
        let write_index = logs
            .iter()
            .position(|entry| entry.label == "db.write_page" && entry.outcome == "partial_write:3")
            .expect("partial write should be logged");
        let sync_index = logs
            .iter()
            .position(|entry| entry.label == "db.fsync" && entry.outcome == "drop_sync")
            .expect("dropped sync should be logged");
        assert!(
            write_index < sync_index,
            "write should be logged before fsync"
        );

        clear_failpoints().expect("clear failpoints");
    }

    #[test]
    fn failpoint_hits_are_logged_in_order() {
        let _guard = test_lock().lock().expect("test lock");
        clear_failpoints().expect("clear failpoints");
        install_failpoint(Failpoint {
            label: "db.read".to_string(),
            trigger_on: 1,
            action: FailAction::PartialRead { bytes: 2 },
        })
        .expect("install partial read");

        let vfs = FaultyVfs::wrap(Arc::new(crate::vfs::mem::MemVfs::default()));
        let file = vfs
            .open(
                Path::new(":memory:"),
                OpenMode::CreateNew,
                FileKind::Database,
            )
            .expect("create file");
        write_all_at(file.as_ref(), 0, &[9, 8, 7, 6]).expect("seed file");

        let mut buf = [0_u8; 4];
        let read = file.read_at(0, &mut buf).expect("read with failpoint");
        assert_eq!(read, 2);
        assert_eq!(&buf[..2], &[9, 8]);

        let logs = failpoint_logs().expect("read logs");
        let write_index = logs
            .iter()
            .position(|entry| entry.label == "db.write_page")
            .expect("seed write should be logged");
        let read_index = logs
            .iter()
            .position(|entry| entry.label == "db.read" && entry.outcome == "partial_read:2")
            .expect("partial read should be logged");
        assert!(
            write_index < read_index,
            "seed write should be logged before partial read"
        );

        clear_failpoints().expect("clear failpoints");
    }

    #[test]
    fn io_without_failpoints_does_not_accumulate_logs() {
        let _guard = test_lock().lock().expect("test lock");
        clear_failpoints().expect("clear failpoints");

        let vfs = FaultyVfs::wrap(Arc::new(crate::vfs::mem::MemVfs::default()));
        let file = vfs
            .open(
                Path::new(":memory:"),
                OpenMode::CreateNew,
                FileKind::Database,
            )
            .expect("create file");

        write_all_at(file.as_ref(), 0, &[1, 2, 3, 4]).expect("seed file");
        let mut buf = [0_u8; 4];
        let read = file.read_at(0, &mut buf).expect("read without failpoint");
        assert_eq!(read, 4);
        assert_eq!(buf, [1, 2, 3, 4]);

        let logs = failpoint_logs().expect("read logs");
        assert!(logs.is_empty(), "normal I/O should not be logged");

        clear_failpoints().expect("clear failpoints");
    }

    #[test]
    fn classify_functions_return_expected_labels() {
        // read/sync classifications
        assert_eq!(super::classify_read(FileKind::Database), "db.read");
        assert_eq!(super::classify_read(FileKind::Wal), "wal.read");
        assert_eq!(super::classify_sync(FileKind::Database), "db.fsync");
        assert_eq!(super::classify_sync(FileKind::Wal), "wal.fsync");
        assert_eq!(
            super::classify_metadata_sync(FileKind::Database),
            "db.sync_metadata"
        );
        assert_eq!(
            super::classify_metadata_sync(FileKind::Wal),
            "wal.sync_metadata"
        );

        // database write classification
        let header = vec![0u8; 128];
        assert_eq!(
            super::classify_write(FileKind::Database, 0, &header),
            "db.write_header"
        );
        let page = vec![1u8; 10];
        assert_eq!(
            super::classify_write(FileKind::Database, 4096, &page),
            "db.write_page"
        );

        // wal write classification
        let wal_header = vec![0u8; 32];
        assert_eq!(
            super::classify_write(FileKind::Wal, 0, &wal_header),
            "wal.write_header"
        );

        let mut f0 = vec![0u8; 10];
        f0[0] = 0;
        assert_eq!(
            super::classify_write(FileKind::Wal, 10, &f0),
            "wal.write_frame"
        );
        f0[0] = 1;
        assert_eq!(
            super::classify_write(FileKind::Wal, 10, &f0),
            "wal.write_commit"
        );
        f0[0] = 2;
        assert_eq!(
            super::classify_write(FileKind::Wal, 10, &f0),
            "wal.write_checkpoint"
        );
        f0[0] = 3;
        assert_eq!(super::classify_write(FileKind::Wal, 10, &f0), "wal.write");

        clear_failpoints().expect("clear failpoints");
    }

    #[test]
    fn failpoint_error_on_write_and_sync() {
        let _guard = test_lock().lock().expect("test lock");
        clear_failpoints().expect("clear failpoints");

        // Inject a write error and verify write_at returns an error
        install_failpoint(Failpoint {
            label: "db.write_page".to_string(),
            trigger_on: 1,
            action: FailAction::Error,
        })
        .expect("install write error");

        let vfs = FaultyVfs::wrap(Arc::new(crate::vfs::mem::MemVfs::default()));
        let file = vfs
            .open(
                Path::new(":memory:"),
                OpenMode::CreateNew,
                FileKind::Database,
            )
            .expect("create file");

        let res = file.write_at(4096, &[1, 2, 3, 4]);
        assert!(res.is_err(), "write should fail when failpoint is Error");

        // Inject a fsync error and verify sync_data returns an error
        install_failpoint(Failpoint {
            label: "db.fsync".to_string(),
            trigger_on: 1,
            action: FailAction::Error,
        })
        .expect("install fsync error");

        let res2 = file.sync_data();
        assert!(res2.is_err(), "fsync should fail when failpoint is Error");

        clear_failpoints().expect("clear failpoints");
    }
}
