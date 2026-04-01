//! Task lifecycle types.

use borsh::{BorshDeserialize, BorshSerialize};

/// Status of a proof task in the lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum TaskStatus {
    Pending,
    Queued,
    Proving,
    Completed,
    TransientFailure { retry_count: u32, error: String },
    PermanentFailure { error: String },
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::PermanentFailure { .. })
    }

    pub fn is_retriable(&self) -> bool {
        matches!(self, Self::TransientFailure { .. })
    }

    pub fn is_in_progress(&self) -> bool {
        matches!(self, Self::Queued | Self::Proving)
    }
}

/// Outcome of a completed (or failed) task. Returned by `execute` and `wait_for_tasks`.
#[derive(Debug, Clone)]
pub enum TaskResult {
    Completed { uuid: String },
    Failed { uuid: String, error: String },
}

impl TaskResult {
    pub fn completed(uuid: impl Into<String>) -> Self {
        Self::Completed { uuid: uuid.into() }
    }

    pub fn failed(uuid: impl Into<String>, error: impl Into<String>) -> Self {
        Self::Failed {
            uuid: uuid.into(),
            error: error.into(),
        }
    }

    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }

    pub fn uuid(&self) -> &str {
        match self {
            Self::Completed { uuid } | Self::Failed { uuid, .. } => uuid,
        }
    }
}
