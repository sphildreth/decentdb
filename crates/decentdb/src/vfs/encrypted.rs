//! Transparent local-file encryption VFS wrapper.
//!
//! Implements ADR 0174 without changing the logical pager/WAL byte layout.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chacha20::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
use chacha20::ChaCha20;
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

use crate::config::DbEncryptionConfig;
use crate::error::{DbError, Result};

use super::{write_all_at, FileKind, OpenMode, Vfs, VfsFile};

pub(crate) const TDE_MAGIC: &[u8; 8] = b"DDBTDE1\0";
const TDE_VERSION: u32 = 1;
const TDE_ALGORITHM_CHACHA20_SHA256: u8 = 1;
const TDE_PREFIX_SIZE: u64 = 128;
const TDE_SALT_LEN: usize = 32;
const TDE_SALT_RANGE: std::ops::Range<usize> = 16..48;
const TDE_VERIFIER_RANGE: std::ops::Range<usize> = 48..80;

#[derive(Clone)]
pub(crate) struct EncryptedVfs {
    inner: Arc<dyn Vfs>,
    config: DbEncryptionConfig,
}

impl EncryptedVfs {
    pub(crate) fn wrap(inner: Arc<dyn Vfs>, config: DbEncryptionConfig) -> Self {
        Self { inner, config }
    }
}

impl fmt::Debug for EncryptedVfs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedVfs")
            .field("inner", &self.inner)
            .field("algorithm", &"ChaCha20-SHA256")
            .finish_non_exhaustive()
    }
}

impl Vfs for EncryptedVfs {
    fn open(&self, path: &Path, mode: OpenMode, kind: FileKind) -> Result<Arc<dyn VfsFile>> {
        let file = self.inner.open(path, mode, kind)?;
        let physical_size = file.file_size()?;
        let prefix = match mode {
            OpenMode::CreateNew => initialize_prefix(file.as_ref(), kind, &self.config)?,
            OpenMode::OpenExisting => {
                read_prefix(file.as_ref(), physical_size, kind, &self.config)?
            }
            OpenMode::OpenOrCreate if physical_size == 0 => {
                initialize_prefix(file.as_ref(), kind, &self.config)?
            }
            OpenMode::OpenOrCreate => {
                read_prefix(file.as_ref(), physical_size, kind, &self.config)?
            }
        };
        let crypto = derive_file_crypto(&self.config, kind, &prefix.salt);
        Ok(Arc::new(EncryptedVfsFile {
            inner: file,
            path: path.to_path_buf(),
            kind,
            stream_key: crypto.stream_key,
            nonce: crypto.nonce,
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

struct EncryptedVfsFile {
    inner: Arc<dyn VfsFile>,
    path: PathBuf,
    kind: FileKind,
    stream_key: [u8; 32],
    nonce: [u8; 12],
}

impl fmt::Debug for EncryptedVfsFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedVfsFile")
            .field("path", &self.path)
            .field("kind", &self.kind)
            .field("algorithm", &"ChaCha20-SHA256")
            .finish_non_exhaustive()
    }
}

impl Drop for EncryptedVfsFile {
    fn drop(&mut self) {
        self.stream_key.zeroize();
        self.nonce.zeroize();
    }
}

impl VfsFile for EncryptedVfsFile {
    fn kind(&self) -> FileKind {
        self.kind
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let read = self.inner.read_at(physical_offset(offset)?, buf)?;
        apply_keystream(&self.stream_key, &self.nonce, offset, &mut buf[..read]);
        Ok(read)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        let mut encrypted = buf.to_vec();
        apply_keystream(&self.stream_key, &self.nonce, offset, &mut encrypted);
        self.inner.write_at(physical_offset(offset)?, &encrypted)
    }

    fn write_all_at_many(&self, writes: &[(u64, &[u8])]) -> Result<()> {
        let mut encrypted = Vec::with_capacity(writes.len());
        for (offset, buf) in writes {
            let mut bytes = buf.to_vec();
            apply_keystream(&self.stream_key, &self.nonce, *offset, &mut bytes);
            encrypted.push((physical_offset(*offset)?, bytes));
        }
        let refs = encrypted
            .iter()
            .map(|(offset, bytes)| (*offset, bytes.as_slice()))
            .collect::<Vec<_>>();
        self.inner.write_all_at_many(&refs)
    }

    fn advise_sequential(&self) -> Result<()> {
        self.inner.advise_sequential()
    }

    fn sync_data(&self) -> Result<()> {
        self.inner.sync_data()
    }

    fn sync_metadata(&self) -> Result<()> {
        self.inner.sync_metadata()
    }

    fn file_size(&self) -> Result<u64> {
        Ok(self.inner.file_size()?.saturating_sub(TDE_PREFIX_SIZE))
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.inner.set_len(physical_offset(len)?)
    }
}

#[derive(Clone, Copy)]
struct TdePrefix {
    salt: [u8; TDE_SALT_LEN],
}

struct FileCrypto {
    stream_key: [u8; 32],
    nonce: [u8; 12],
}

fn initialize_prefix(
    file: &dyn VfsFile,
    kind: FileKind,
    config: &DbEncryptionConfig,
) -> Result<TdePrefix> {
    let salt = random_salt()?;
    let bytes = encode_prefix(kind, config, &salt);
    write_all_at(file, 0, &bytes)?;
    file.set_len(TDE_PREFIX_SIZE)?;
    Ok(TdePrefix { salt })
}

fn read_prefix(
    file: &dyn VfsFile,
    physical_size: u64,
    expected_kind: FileKind,
    config: &DbEncryptionConfig,
) -> Result<TdePrefix> {
    if physical_size < TDE_PREFIX_SIZE {
        return Err(DbError::corruption(format!(
            "{} has a truncated DecentDB TDE prefix",
            file.path().display()
        )));
    }
    let mut prefix = [0_u8; TDE_PREFIX_SIZE as usize];
    super::read_exact_at(file, 0, &mut prefix)?;
    decode_prefix(file.path(), &prefix, expected_kind, config)
}

fn encode_prefix(
    kind: FileKind,
    config: &DbEncryptionConfig,
    salt: &[u8; TDE_SALT_LEN],
) -> [u8; TDE_PREFIX_SIZE as usize] {
    let mut prefix = [0_u8; TDE_PREFIX_SIZE as usize];
    prefix[..TDE_MAGIC.len()].copy_from_slice(TDE_MAGIC);
    prefix[8..12].copy_from_slice(&TDE_VERSION.to_le_bytes());
    prefix[12] = file_kind_id(kind);
    prefix[13] = TDE_ALGORITHM_CHACHA20_SHA256;
    prefix[TDE_SALT_RANGE].copy_from_slice(salt);
    let verifier = derive_verifier(config, salt);
    prefix[TDE_VERIFIER_RANGE].copy_from_slice(&verifier);
    prefix
}

fn decode_prefix(
    path: &Path,
    prefix: &[u8; TDE_PREFIX_SIZE as usize],
    expected_kind: FileKind,
    config: &DbEncryptionConfig,
) -> Result<TdePrefix> {
    if &prefix[..TDE_MAGIC.len()] != TDE_MAGIC {
        return Err(DbError::corruption(format!(
            "{} is not encrypted with DecentDB TDE",
            path.display()
        )));
    }
    let version = u32::from_le_bytes(
        prefix[8..12]
            .try_into()
            .map_err(|_| DbError::internal("TDE prefix version slice is invalid"))?,
    );
    if version != TDE_VERSION {
        return Err(DbError::unsupported_format_version(version));
    }
    if prefix[12] != file_kind_id(expected_kind) {
        return Err(DbError::corruption(format!(
            "{} has a TDE prefix for a different file kind",
            path.display()
        )));
    }
    if prefix[13] != TDE_ALGORITHM_CHACHA20_SHA256 {
        return Err(DbError::corruption(format!(
            "{} uses an unsupported TDE algorithm",
            path.display()
        )));
    }

    let salt = prefix[TDE_SALT_RANGE]
        .try_into()
        .map_err(|_| DbError::internal("TDE prefix salt slice is invalid"))?;
    let expected = derive_verifier(config, &salt);
    if prefix[TDE_VERIFIER_RANGE] != expected {
        return Err(DbError::corruption(format!(
            "{} could not be opened with the supplied encryption key",
            path.display()
        )));
    }
    Ok(TdePrefix { salt })
}

fn derive_file_crypto(
    config: &DbEncryptionConfig,
    kind: FileKind,
    salt: &[u8; TDE_SALT_LEN],
) -> FileCrypto {
    let mut master = derive_master_key(config);
    let kind = [file_kind_id(kind)];
    let stream_key = sha256_parts(&[
        b"decentdb tde file key v1",
        master.as_slice(),
        salt.as_slice(),
        kind.as_slice(),
    ]);
    let nonce_hash = sha256_parts(&[
        b"decentdb tde nonce v1",
        master.as_slice(),
        salt.as_slice(),
        kind.as_slice(),
    ]);
    master.zeroize();
    let mut nonce = [0_u8; 12];
    nonce.copy_from_slice(&nonce_hash[..12]);
    FileCrypto { stream_key, nonce }
}

fn derive_verifier(config: &DbEncryptionConfig, salt: &[u8; TDE_SALT_LEN]) -> [u8; 32] {
    let mut master = derive_master_key(config);
    let verifier = sha256_parts(&[
        b"decentdb tde verifier v1",
        master.as_slice(),
        salt.as_slice(),
    ]);
    master.zeroize();
    verifier
}

fn derive_master_key(config: &DbEncryptionConfig) -> [u8; 32] {
    sha256_parts(&[b"decentdb tde master key v1", config.key.expose_secret()])
}

fn sha256_parts(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn apply_keystream(key: &[u8; 32], nonce: &[u8; 12], offset: u64, bytes: &mut [u8]) {
    if bytes.is_empty() {
        return;
    }
    let mut cipher = ChaCha20::new(key.into(), nonce.into());
    cipher.seek(offset);
    cipher.apply_keystream(bytes);
}

fn physical_offset(logical_offset: u64) -> Result<u64> {
    logical_offset
        .checked_add(TDE_PREFIX_SIZE)
        .ok_or_else(|| DbError::internal("encrypted file offset overflow"))
}

fn file_kind_id(kind: FileKind) -> u8 {
    match kind {
        FileKind::Database => 1,
        FileKind::Wal => 2,
        FileKind::SyncJournal => 3,
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn random_salt() -> Result<[u8; TDE_SALT_LEN]> {
    let mut salt = [0_u8; TDE_SALT_LEN];
    getrandom::fill(&mut salt).map_err(|error| {
        DbError::io(
            "generate DecentDB TDE salt",
            std::io::Error::other(error.to_string()),
        )
    })?;
    Ok(salt)
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn random_salt() -> Result<[u8; TDE_SALT_LEN]> {
    Err(DbError::internal(
        "creating encrypted databases requires platform randomness",
    ))
}
