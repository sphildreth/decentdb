//! Engine-owned write queue and strict queued group-commit coordinator.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::config::DbConfig;
use crate::db::Db;
use crate::error::{DbError, Result};
use crate::exec::QueryResult;
use crate::record::value::Value;

#[derive(Clone, Debug, Default)]
pub struct QueuedWriteOptions {
    pub timeout: Option<Duration>,
    pub cancel_token: Option<Arc<AtomicBool>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteQueueMetricsSnapshot {
    pub capacity: usize,
    pub current_depth: usize,
    pub admitted: u64,
    pub rejected: u64,
    pub timed_out: u64,
    pub canceled: u64,
    pub executed: u64,
    pub committed: u64,
    pub failed: u64,
    pub group_commit_batches: u64,
    pub group_commit_syncs: u64,
    pub group_commit_max_batch: u64,
    pub group_commit_commits_covered: u64,
    pub physical_syncs_saved: u64,
    pub total_queue_wait_ns: u64,
}

#[derive(Debug)]
pub(crate) struct WriteQueue {
    capacity: usize,
    default_timeout: Option<Duration>,
    strict_group_commit: bool,
    max_batch: usize,
    max_group_delay: Duration,
    state: Mutex<QueueState>,
    cvar: Condvar,
    metrics: WriteQueueMetrics,
}

#[derive(Debug, Default)]
struct QueueState {
    queue: VecDeque<Arc<QueuedRequest>>,
    executor_active: bool,
    closed: bool,
}

#[derive(Debug)]
struct QueuedRequest {
    sql: String,
    params: Vec<Value>,
    enqueued_at: Instant,
    started: AtomicBool,
    canceled: AtomicBool,
    cancel_token: Option<Arc<AtomicBool>>,
    result: Mutex<Option<Result<Vec<QueryResult>>>>,
}

#[derive(Debug, Default)]
struct WriteQueueMetrics {
    admitted: AtomicU64,
    rejected: AtomicU64,
    timed_out: AtomicU64,
    canceled: AtomicU64,
    executed: AtomicU64,
    committed: AtomicU64,
    failed: AtomicU64,
    group_commit_batches: AtomicU64,
    group_commit_syncs: AtomicU64,
    group_commit_max_batch: AtomicU64,
    group_commit_commits_covered: AtomicU64,
    physical_syncs_saved: AtomicU64,
    total_queue_wait_ns: AtomicU64,
}

impl WriteQueue {
    #[must_use]
    pub(crate) fn new(config: &DbConfig) -> Self {
        let default_timeout = if config.write_queue_default_timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(config.write_queue_default_timeout_ms))
        };
        Self {
            capacity: config.write_queue_capacity.max(1),
            default_timeout,
            strict_group_commit: config.write_queue_strict_group_commit,
            max_batch: config.write_queue_max_batch.max(1),
            max_group_delay: Duration::from_micros(config.write_queue_max_group_delay_us),
            state: Mutex::new(QueueState::default()),
            cvar: Condvar::new(),
            metrics: WriteQueueMetrics::default(),
        }
    }

    pub(crate) fn execute_batch_with_params(
        &self,
        db: &Db,
        sql: &str,
        params: &[Value],
        options: QueuedWriteOptions,
    ) -> Result<Vec<QueryResult>> {
        let timeout = options.timeout.or(self.default_timeout);
        let request = Arc::new(QueuedRequest {
            sql: sql.to_string(),
            params: params.to_vec(),
            enqueued_at: Instant::now(),
            started: AtomicBool::new(false),
            canceled: AtomicBool::new(false),
            cancel_token: options.cancel_token,
            result: Mutex::new(None),
        });

        self.admit(Arc::clone(&request), timeout)?;
        self.wait_for_result_or_execute(db, request, timeout)
    }

    pub(crate) fn snapshot(&self) -> WriteQueueMetricsSnapshot {
        let current_depth = self
            .state
            .lock()
            .map(|state| state.queue.len())
            .unwrap_or_default();
        WriteQueueMetricsSnapshot {
            capacity: self.capacity,
            current_depth,
            admitted: self.metrics.admitted.load(Ordering::Relaxed),
            rejected: self.metrics.rejected.load(Ordering::Relaxed),
            timed_out: self.metrics.timed_out.load(Ordering::Relaxed),
            canceled: self.metrics.canceled.load(Ordering::Relaxed),
            executed: self.metrics.executed.load(Ordering::Relaxed),
            committed: self.metrics.committed.load(Ordering::Relaxed),
            failed: self.metrics.failed.load(Ordering::Relaxed),
            group_commit_batches: self.metrics.group_commit_batches.load(Ordering::Relaxed),
            group_commit_syncs: self.metrics.group_commit_syncs.load(Ordering::Relaxed),
            group_commit_max_batch: self.metrics.group_commit_max_batch.load(Ordering::Relaxed),
            group_commit_commits_covered: self
                .metrics
                .group_commit_commits_covered
                .load(Ordering::Relaxed),
            physical_syncs_saved: self.metrics.physical_syncs_saved.load(Ordering::Relaxed),
            total_queue_wait_ns: self.metrics.total_queue_wait_ns.load(Ordering::Relaxed),
        }
    }

    fn admit(&self, request: Arc<QueuedRequest>, timeout: Option<Duration>) -> Result<()> {
        if Self::external_cancelled(&request) {
            self.metrics.canceled.fetch_add(1, Ordering::Relaxed);
            return Err(DbError::canceled(
                "queued write canceled before queue admission",
            ));
        }

        let deadline = timeout.and_then(|duration| Instant::now().checked_add(duration));
        let mut state = self
            .state
            .lock()
            .map_err(|_| DbError::internal("write queue lock poisoned"))?;
        loop {
            if state.closed {
                return Err(DbError::queue_closed("write queue is closed"));
            }
            if state.queue.len() < self.capacity {
                state.queue.push_back(request);
                self.metrics.admitted.fetch_add(1, Ordering::Relaxed);
                self.cvar.notify_all();
                return Ok(());
            }

            if timeout == Some(Duration::ZERO) {
                self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                return Err(DbError::queue_full(format!(
                    "write queue capacity {} is exhausted",
                    self.capacity
                )));
            }
            if Self::external_cancelled(&request) {
                self.metrics.canceled.fetch_add(1, Ordering::Relaxed);
                return Err(DbError::canceled(
                    "queued write canceled before queue admission",
                ));
            }
            let Some(deadline) = deadline else {
                state = self
                    .cvar
                    .wait(state)
                    .map_err(|_| DbError::internal("write queue lock poisoned"))?;
                continue;
            };
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                self.metrics.timed_out.fetch_add(1, Ordering::Relaxed);
                return Err(DbError::timeout(
                    "queued write timed out before queue admission",
                ));
            };
            let (new_state, wait_result) = self
                .cvar
                .wait_timeout(state, remaining)
                .map_err(|_| DbError::internal("write queue lock poisoned"))?;
            state = new_state;
            if wait_result.timed_out() {
                self.metrics.timed_out.fetch_add(1, Ordering::Relaxed);
                return Err(DbError::timeout(
                    "queued write timed out before queue admission",
                ));
            }
        }
    }

    fn wait_for_result_or_execute(
        &self,
        db: &Db,
        request: Arc<QueuedRequest>,
        timeout: Option<Duration>,
    ) -> Result<Vec<QueryResult>> {
        let deadline = timeout.and_then(|duration| Instant::now().checked_add(duration));
        loop {
            if let Some(result) = Self::take_result(&request)? {
                return result;
            }
            if Self::external_cancelled(&request) && !request.started.load(Ordering::Acquire) {
                request.canceled.store(true, Ordering::Release);
                self.metrics.canceled.fetch_add(1, Ordering::Relaxed);
                self.cvar.notify_all();
                return Err(DbError::canceled(
                    "queued write canceled before execution started",
                ));
            }

            let mut state = self
                .state
                .lock()
                .map_err(|_| DbError::internal("write queue lock poisoned"))?;
            if !state.executor_active {
                state.executor_active = true;
                drop(state);
                self.run_one_executor_batch(db);
                continue;
            }

            if request.started.load(Ordering::Acquire) {
                drop(
                    self.cvar
                        .wait(state)
                        .map_err(|_| DbError::internal("write queue lock poisoned"))?,
                );
                continue;
            }

            let Some(deadline) = deadline else {
                drop(
                    self.cvar
                        .wait(state)
                        .map_err(|_| DbError::internal("write queue lock poisoned"))?,
                );
                continue;
            };
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                request.canceled.store(true, Ordering::Release);
                self.metrics.timed_out.fetch_add(1, Ordering::Relaxed);
                self.cvar.notify_all();
                return Err(DbError::timeout(
                    "queued write timed out before execution started",
                ));
            };
            let (_state, wait_result) = self
                .cvar
                .wait_timeout(state, remaining)
                .map_err(|_| DbError::internal("write queue lock poisoned"))?;
            if wait_result.timed_out() && !request.started.load(Ordering::Acquire) {
                request.canceled.store(true, Ordering::Release);
                self.metrics.timed_out.fetch_add(1, Ordering::Relaxed);
                self.cvar.notify_all();
                return Err(DbError::timeout(
                    "queued write timed out before execution started",
                ));
            }
        }
    }

    fn run_one_executor_batch(&self, db: &Db) {
        let batch = self.drain_executor_batch();
        if batch.is_empty() {
            self.finish_executor_batch();
            return;
        }

        let _group_commit = if self.strict_group_commit {
            Some(db.begin_deferred_group_commit())
        } else {
            None
        };
        let mut successes = Vec::new();

        for request in batch {
            if request.canceled.load(Ordering::Acquire) || Self::external_cancelled(&request) {
                request.canceled.store(true, Ordering::Release);
                self.metrics.canceled.fetch_add(1, Ordering::Relaxed);
                let _ = Self::set_result(
                    &request,
                    Err(DbError::canceled(
                        "queued write canceled before execution started",
                    )),
                );
                continue;
            }
            request.started.store(true, Ordering::Release);
            self.metrics.executed.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .total_queue_wait_ns
                .fetch_add(nanos_since(request.enqueued_at), Ordering::Relaxed);

            match db.execute_batch_direct_with_params(&request.sql, &request.params) {
                Ok(result) => {
                    successes.push((request, result));
                }
                Err(error) => {
                    self.metrics.failed.fetch_add(1, Ordering::Relaxed);
                    let _ = Self::set_result(&request, Err(error));
                }
            }
        }

        let sync_result = if successes.is_empty() || !self.strict_group_commit {
            Ok(false)
        } else {
            db.flush_deferred_group_commit()
        };
        match sync_result {
            Ok(did_sync) => {
                if !successes.is_empty() {
                    let success_count = successes.len() as u64;
                    self.metrics
                        .committed
                        .fetch_add(success_count, Ordering::Relaxed);
                    self.metrics
                        .group_commit_batches
                        .fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .group_commit_commits_covered
                        .fetch_add(success_count, Ordering::Relaxed);
                    self.metrics
                        .group_commit_max_batch
                        .fetch_max(success_count, Ordering::Relaxed);
                    if did_sync {
                        self.metrics
                            .group_commit_syncs
                            .fetch_add(1, Ordering::Relaxed);
                        self.metrics
                            .physical_syncs_saved
                            .fetch_add(success_count.saturating_sub(1), Ordering::Relaxed);
                    }
                }
                for (request, result) in successes {
                    let _ = Self::set_result(&request, Ok(result));
                }
            }
            Err(error) => {
                self.metrics
                    .failed
                    .fetch_add(successes.len() as u64, Ordering::Relaxed);
                let message = error.to_string();
                for (request, _) in successes {
                    let _ = Self::set_result(
                        &request,
                        Err(DbError::io(
                            "queued group commit sync",
                            std::io::Error::other(message.clone()),
                        )),
                    );
                }
            }
        }

        self.finish_executor_batch();
    }

    fn drain_executor_batch(&self) -> Vec<Arc<QueuedRequest>> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Vec::new(),
        };
        if state.queue.is_empty() {
            return Vec::new();
        }
        if !self.max_group_delay.is_zero() && state.queue.len() < self.max_batch {
            match self.cvar.wait_timeout(state, self.max_group_delay) {
                Ok((new_state, _)) => state = new_state,
                Err(_) => return Vec::new(),
            }
        }
        let count = self.max_batch.min(state.queue.len());
        let mut batch = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(request) = state.queue.pop_front() {
                batch.push(request);
            }
        }
        self.cvar.notify_all();
        batch
    }

    fn finish_executor_batch(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.executor_active = false;
        }
        self.cvar.notify_all();
    }

    fn take_result(request: &QueuedRequest) -> Result<Option<Result<Vec<QueryResult>>>> {
        request
            .result
            .lock()
            .map(|mut result| result.take())
            .map_err(|_| DbError::internal("queued write result lock poisoned"))
    }

    fn set_result(request: &QueuedRequest, result: Result<Vec<QueryResult>>) -> Result<()> {
        *request
            .result
            .lock()
            .map_err(|_| DbError::internal("queued write result lock poisoned"))? = Some(result);
        Ok(())
    }

    fn external_cancelled(request: &QueuedRequest) -> bool {
        request
            .cancel_token
            .as_ref()
            .is_some_and(|token| token.load(Ordering::Acquire))
    }
}

fn nanos_since(instant: Instant) -> u64 {
    u64::try_from(instant.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
