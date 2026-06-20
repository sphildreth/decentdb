use super::*;

pub(super) fn db_open_lock(canonical_path: PathBuf) -> Result<Arc<Mutex<()>>> {
    let mut registry = db_open_lock_registry()
        .lock()
        .map_err(|_| DbError::internal("database open lock registry poisoned"))?;
    if let Some(existing) = registry.get(&canonical_path).and_then(Weak::upgrade) {
        return Ok(existing);
    }

    registry.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(Mutex::new(()));
    registry.insert(canonical_path, Arc::downgrade(&lock));
    Ok(lock)
}

pub(super) fn db_open_lock_registry() -> &'static Mutex<HashMap<PathBuf, Weak<Mutex<()>>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) struct DbOpenLockCleanup(pub(super) Option<PathBuf>);

impl Drop for DbOpenLockCleanup {
    fn drop(&mut self) {
        if let Some(canonical_path) = self.0.as_deref() {
            prune_db_open_lock_registry(canonical_path);
        }
    }
}

pub(super) fn prune_db_open_lock_registry(canonical_path: &Path) {
    let Ok(mut registry) = db_open_lock_registry().lock() else {
        return;
    };
    if registry
        .get(canonical_path)
        .is_some_and(|entry| entry.upgrade().is_none())
    {
        registry.remove(canonical_path);
    }
    if registry.is_empty() {
        registry.shrink_to_fit();
    }
}
