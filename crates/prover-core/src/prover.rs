//! Core prover: fetches input via spec, proves via strategy,
//! optionally stores receipt and calls domain hook.

use std::{
    collections::HashMap,
    fmt, slice,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use tokio::sync::{watch, RwLock};
use tracing::{error, info, warn};
use zkaleido::ZkVmHost;
#[cfg(feature = "remote")]
use zkaleido::ZkVmRemoteHost;

use crate::{
    config::{ProverConfig, RetryConfig},
    error::{ProverError, ProverResult},
    receipt::{ReceiptHook, ReceiptStore},
    spec::ProofSpec,
    store::{InMemoryTaskStore, TaskRecord, TaskStore},
    strategy::{NativeStrategy, ProveStrategy},
    task::{TaskResult, TaskStatus},
};

/// Single-proof-type prover.
///
/// Generic over `H` (spec) only. The zkVM host type is erased inside
/// the [`ProveStrategy`] — consumers never see it.
pub struct Prover<H: ProofSpec> {
    spec: Arc<H>,
    strategy: Arc<dyn ProveStrategy<H>>,
    config: ProverConfig,
    task_store: Arc<dyn TaskStore>,
    receipt_store: Option<Arc<dyn ReceiptStore>>,
    receipt_hook: Option<Arc<dyn ReceiptHook<H>>>,
    /// Maps domain task → UUID for idempotent submit.
    task_ids: Arc<RwLock<HashMap<H::Task, String>>>,
    /// Watch channels for notifying waiters when tasks reach terminal states.
    watchers: Arc<RwLock<HashMap<String, watch::Sender<Option<TaskResult>>>>>,
    /// Whether we've run recovery on startup.
    recovered: AtomicBool,
}

impl<H: ProofSpec> fmt::Debug for Prover<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Prover")
            .field("has_retry", &self.config.retry.is_some())
            .field("has_receipt_store", &self.receipt_store.is_some())
            .field("has_receipt_hook", &self.receipt_hook.is_some())
            .finish()
    }
}

impl<H: ProofSpec> Clone for Prover<H> {
    fn clone(&self) -> Self {
        Self {
            spec: self.spec.clone(),
            strategy: self.strategy.clone(),
            config: self.config.clone(),
            task_store: self.task_store.clone(),
            receipt_store: self.receipt_store.clone(),
            receipt_hook: self.receipt_hook.clone(),
            task_ids: self.task_ids.clone(),
            watchers: self.watchers.clone(),
            recovered: AtomicBool::new(self.recovered.load(Ordering::SeqCst)),
        }
    }
}

// ============================================================================
// Consumer API
// ============================================================================

impl<H: ProofSpec> Prover<H> {
    /// Register a task and spawn background proving. Returns UUID. Idempotent.
    pub async fn submit(&self, task: H::Task) -> ProverResult<String> {
        {
            let ids = self.task_ids.read().await;
            if let Some(uuid) = ids.get(&task) {
                return Ok(uuid.clone());
            }
        }

        let uuid = uuid::Uuid::new_v4().to_string();
        self.task_store
            .insert(TaskRecord::new(uuid.clone(), TaskStatus::Pending))?;
        self.task_ids
            .write()
            .await
            .insert(task.clone(), uuid.clone());

        let prover = self.clone();
        let u = uuid.clone();
        tokio::spawn(async move {
            prover.run_task(task, u).await;
        });

        Ok(uuid)
    }

    /// Submit a task and block until it reaches a terminal state.
    pub async fn execute(&self, task: H::Task) -> ProverResult<TaskResult> {
        let uuid = self.submit(task).await?;
        let results = self.wait_for_tasks(slice::from_ref(&uuid)).await?;
        Ok(results.into_iter().next().expect("one result for one uuid"))
    }

    /// Block until all tasks reach terminal states.
    ///
    /// Uses watch channels — zero polling, immediate notification.
    pub async fn wait_for_tasks(&self, uuids: &[String]) -> ProverResult<Vec<TaskResult>> {
        let mut receivers: Vec<(usize, watch::Receiver<Option<TaskResult>>)> = Vec::new();
        let mut results: Vec<Option<TaskResult>> = vec![None; uuids.len()];

        for (i, uuid) in uuids.iter().enumerate() {
            if let Some(record) = self.task_store.get(uuid) {
                match record.status() {
                    TaskStatus::Completed => {
                        results[i] = Some(TaskResult::completed(uuid));
                        continue;
                    }
                    TaskStatus::PermanentFailure { error } => {
                        results[i] = Some(TaskResult::failed(uuid, error));
                        continue;
                    }
                    _ => {}
                }
            }

            let rx = {
                let mut w = self.watchers.write().await;
                if let Some(tx) = w.get(uuid) {
                    tx.subscribe()
                } else {
                    let (tx, rx) = watch::channel(None);
                    w.insert(uuid.to_string(), tx);
                    rx
                }
            };
            receivers.push((i, rx));
        }

        if receivers.is_empty() {
            return Ok(results.into_iter().map(|r| r.unwrap()).collect());
        }

        loop {
            for (i, rx) in &receivers {
                if results[*i].is_some() {
                    continue;
                }
                if let Some(result) = rx.borrow().as_ref() {
                    results[*i] = Some(result.clone());
                }
            }

            if results.iter().all(|r| r.is_some()) {
                return Ok(results.into_iter().map(|r| r.unwrap()).collect());
            }

            let futs: Vec<_> = receivers
                .iter()
                .filter(|(i, _)| results[*i].is_none())
                .map(|(_, rx)| {
                    let mut rx = rx.clone();
                    Box::pin(async move { rx.changed().await })
                })
                .collect();
            use futures::future::select_all;
            let _ = select_all(futs).await;
        }
    }

    /// Get a receipt from the receipt store by UUID.
    ///
    /// Returns `None` if the store has no receipt for this UUID, or `Err` if
    /// no receipt store was configured.
    pub fn get_receipt(
        &self,
        uuid: &str,
    ) -> ProverResult<Option<zkaleido::ProofReceiptWithMetadata>> {
        self.receipt_store
            .as_ref()
            .ok_or_else(|| ProverError::Internal(anyhow::anyhow!("no receipt store configured")))?
            .get(uuid)
    }
}

// ============================================================================
// Internal API (used by PaaS tick, not exposed on ProverHandle)
// ============================================================================

impl<H: ProofSpec> Prover<H> {
    pub fn has_retry(&self) -> bool {
        self.config.retry.is_some()
    }

    pub fn has_receipt_store(&self) -> bool {
        self.receipt_store.is_some()
    }

    pub fn task_store(&self) -> &dyn TaskStore {
        self.task_store.as_ref()
    }

    /// Current task status by UUID.
    pub fn get_status(&self, uuid: &str) -> ProverResult<TaskStatus> {
        self.task_store
            .get(uuid)
            .map(|r| r.status().clone())
            .ok_or_else(|| ProverError::TaskNotFound(uuid.to_string()))
    }

    /// Scan for retriable tasks and re-spawn them. Called by PaaS on tick.
    pub async fn tick(&self) {
        if !self.recovered.swap(true, Ordering::SeqCst) {
            self.recover().await;
        }

        for record in self.task_store.list_retriable(SystemTime::now()) {
            let uuid = record.uuid().to_string();
            if let Some(task) = self.find_task_by_uuid(&uuid).await {
                let prover = self.clone();
                tokio::spawn(async move {
                    prover.run_task(task, uuid).await;
                });
            }
        }
    }

    async fn recover(&self) {
        let in_progress = self.task_store.list_in_progress();
        if in_progress.is_empty() {
            return;
        }
        info!(count = in_progress.len(), "recovering in-progress tasks");
        for record in in_progress {
            let uuid = record.uuid().to_string();
            if let Some(task) = self.find_task_by_uuid(&uuid).await {
                let prover = self.clone();
                tokio::spawn(async move {
                    prover.run_task(task, uuid).await;
                });
            }
        }
    }
}

// ============================================================================
// Proving internals
// ============================================================================

impl<H: ProofSpec> Prover<H> {
    async fn run_task(&self, task: H::Task, uuid: String) {
        use tokio::task::spawn_blocking;

        let _ = self.task_store.update_status(&uuid, TaskStatus::Queued);
        let _ = self.task_store.update_status(&uuid, TaskStatus::Proving);

        // 1. Fetch input
        let input = match self.spec.fetch_input(&task).await {
            Ok(input) => input,
            Err(e) => {
                self.handle_error(&uuid, &e);
                self.notify(&uuid).await;
                return;
            }
        };

        // 2. Prove (blocking — strategy handles native vs remote)
        let strategy = self.strategy.clone();
        let prove_result = spawn_blocking(move || strategy.prove(&input)).await;

        let receipt = match prove_result {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(e)) => {
                error!(%uuid, %e, "prove failed");
                self.handle_error(&uuid, &e);
                self.notify(&uuid).await;
                return;
            }
            Err(e) => {
                error!(%uuid, %e, "prove task panicked");
                let _ = self.task_store.update_status(
                    &uuid,
                    TaskStatus::PermanentFailure {
                        error: e.to_string(),
                    },
                );
                self.notify(&uuid).await;
                return;
            }
        };

        // 3. Store receipt (if configured)
        if let Some(store) = &self.receipt_store {
            if let Err(e) = store.put(&uuid, &receipt) {
                error!(%uuid, %e, "receipt store put failed");
                self.handle_error(&uuid, &e);
                self.notify(&uuid).await;
                return;
            }
        }

        // 4. Domain hook (if configured)
        if let Some(hook) = &self.receipt_hook {
            if let Err(e) = hook.on_receipt(&task, &receipt).await {
                error!(%uuid, %e, "receipt hook failed");
                self.handle_error(&uuid, &e);
                self.notify(&uuid).await;
                return;
            }
        }

        // 5. Done
        let _ = self.task_store.update_status(&uuid, TaskStatus::Completed);
        info!(%uuid, "task completed");
        self.notify(&uuid).await;
    }

    fn handle_error(&self, uuid: &str, err: &ProverError) {
        if err.is_transient() {
            self.schedule_retry(uuid, &err.to_string());
        } else {
            let _ = self.task_store.update_status(
                uuid,
                TaskStatus::PermanentFailure {
                    error: err.to_string(),
                },
            );
        }
    }

    fn schedule_retry(&self, uuid: &str, msg: &str) {
        let current_count = self
            .task_store
            .get(uuid)
            .and_then(|r| match r.status() {
                TaskStatus::TransientFailure { retry_count, .. } => Some(*retry_count),
                _ => None,
            })
            .unwrap_or(0);
        let new_count = current_count + 1;

        if let Some(ref cfg) = self.config.retry {
            if cfg.should_retry(new_count) {
                warn!(%uuid, retry_count = new_count, "transient failure, scheduling retry");
                let _ = self.task_store.update_status(
                    uuid,
                    TaskStatus::TransientFailure {
                        retry_count: new_count,
                        error: msg.to_string(),
                    },
                );
                let delay = Duration::from_secs(cfg.calculate_delay(new_count));
                let _ = self
                    .task_store
                    .set_retry_after(uuid, SystemTime::now() + delay);
                return;
            }
        }

        let _ = self.task_store.update_status(
            uuid,
            TaskStatus::PermanentFailure {
                error: format!("retries exhausted: {msg}"),
            },
        );
    }

    async fn notify(&self, uuid: &str) {
        let result = self.task_store.get(uuid).and_then(|r| match r.status() {
            TaskStatus::Completed => Some(TaskResult::completed(uuid)),
            TaskStatus::PermanentFailure { error } => Some(TaskResult::failed(uuid, error)),
            _ => None,
        });

        if let Some(result) = result {
            if let Some(tx) = self.watchers.read().await.get(uuid) {
                let _ = tx.send(Some(result));
            }
        }
    }

    async fn find_task_by_uuid(&self, uuid: &str) -> Option<H::Task> {
        self.task_ids
            .read()
            .await
            .iter()
            .find(|(_, u)| u.as_str() == uuid)
            .map(|(t, _)| t.clone())
    }
}

// ============================================================================
// Builder
// ============================================================================

/// Builds a [`Prover`].
pub struct ProverBuilder<H: ProofSpec> {
    spec: H,
    task_store: Option<Arc<dyn TaskStore>>,
    receipt_store: Option<Arc<dyn ReceiptStore>>,
    receipt_hook: Option<Arc<dyn ReceiptHook<H>>>,
    retry: Option<RetryConfig>,
}

impl<H: ProofSpec> ProverBuilder<H> {
    pub fn new(spec: H) -> Self {
        Self {
            spec,
            task_store: None,
            receipt_store: None,
            receipt_hook: None,
            retry: None,
        }
    }

    pub fn task_store(mut self, store: impl TaskStore + 'static) -> Self {
        self.task_store = Some(Arc::new(store));
        self
    }

    /// Opt-in receipt persistence. Enables `get_receipt` on the PaaS handle.
    pub fn receipt_store(mut self, store: impl ReceiptStore + 'static) -> Self {
        self.receipt_store = Some(Arc::new(store));
        self
    }

    /// Opt-in domain hook called after receipt storage.
    pub fn receipt_hook(mut self, hook: impl ReceiptHook<H> + 'static) -> Self {
        self.receipt_hook = Some(Arc::new(hook));
        self
    }

    pub fn retry(mut self, config: RetryConfig) -> Self {
        self.retry = Some(config);
        self
    }

    /// Build with a native host (blocking `Program::prove` via `spawn_blocking`).
    pub fn native<Host: ZkVmHost + Send + Sync + 'static>(self, host: Host) -> Prover<H> {
        self.build(Arc::new(NativeStrategy::new(host)))
    }

    /// Build with a remote host (`start_proving` + poll via `LocalSet`).
    #[cfg(feature = "remote")]
    pub fn remote<Host>(self, host: Host) -> Prover<H>
    where
        Host: ZkVmRemoteHost + Send + Sync + 'static,
    {
        use crate::strategy::RemoteStrategy;
        self.build(Arc::new(RemoteStrategy::new(host, Duration::from_secs(10))))
    }

    /// Build with a remote host and custom poll interval.
    #[cfg(feature = "remote")]
    pub fn remote_with_interval<Host>(self, host: Host, poll_interval: Duration) -> Prover<H>
    where
        Host: ZkVmRemoteHost + Send + Sync + 'static,
    {
        use crate::strategy::RemoteStrategy;
        self.build(Arc::new(RemoteStrategy::new(host, poll_interval)))
    }

    fn build(self, strategy: Arc<dyn ProveStrategy<H>>) -> Prover<H> {
        Prover {
            spec: Arc::new(self.spec),
            strategy,
            config: ProverConfig { retry: self.retry },
            task_store: self
                .task_store
                .unwrap_or_else(|| Arc::new(InMemoryTaskStore::new())),
            receipt_store: self.receipt_store,
            receipt_hook: self.receipt_hook,
            task_ids: Arc::new(RwLock::new(HashMap::new())),
            watchers: Arc::new(RwLock::new(HashMap::new())),
            recovered: AtomicBool::new(false),
        }
    }
}

impl<H: ProofSpec> fmt::Debug for ProverBuilder<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProverBuilder").finish()
    }
}
