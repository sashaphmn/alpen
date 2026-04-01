//! Task storage trait.

use std::time::SystemTime;

use crate::{error::ProverResult, task::TaskStatus};

/// A single task record in the store.
#[derive(Debug, Clone)]
pub struct TaskRecord {
    uuid: String,
    status: TaskStatus,
    updated_at: SystemTime,
    retry_after: Option<SystemTime>,
    /// Opaque bytes for strategy-specific state (e.g. remote ProofId for crash recovery).
    metadata: Option<Vec<u8>>,
}

impl TaskRecord {
    pub fn new(uuid: String, status: TaskStatus) -> Self {
        Self {
            uuid,
            status,
            updated_at: SystemTime::now(),
            retry_after: None,
            metadata: None,
        }
    }

    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub fn retry_after(&self) -> Option<SystemTime> {
        self.retry_after
    }

    pub fn metadata(&self) -> Option<&[u8]> {
        self.metadata.as_deref()
    }

    pub fn update_status(&mut self, status: TaskStatus) {
        self.status = status;
        self.updated_at = SystemTime::now();
    }

    pub fn set_retry_after(&mut self, when: Option<SystemTime>) {
        self.retry_after = when;
        self.updated_at = SystemTime::now();
    }

    pub fn set_metadata(&mut self, data: Option<Vec<u8>>) {
        self.metadata = data;
        self.updated_at = SystemTime::now();
    }
}

/// Persistence for task records. Keyed by UUID, no generics.
pub trait TaskStore: Send + Sync + 'static {
    fn get(&self, uuid: &str) -> Option<TaskRecord>;
    fn insert(&self, record: TaskRecord) -> ProverResult<()>;
    fn update_status(&self, uuid: &str, status: TaskStatus) -> ProverResult<()>;
    fn set_retry_after(&self, uuid: &str, when: SystemTime) -> ProverResult<()>;
    fn set_metadata(&self, uuid: &str, data: Vec<u8>) -> ProverResult<()>;
    fn list_retriable(&self, now: SystemTime) -> Vec<TaskRecord>;
    fn list_in_progress(&self) -> Vec<TaskRecord>;
    fn count(&self) -> usize;
}
