//! Proof specification trait.
//!
//! Defines what to prove (program), what identifies a task, and how to fetch the input.
//! Receipt storage and domain hooks are separate opt-in concerns
//! (see [`ReceiptStore`](crate::ReceiptStore) and [`ReceiptHook`](crate::ReceiptHook)).

use std::{fmt::Debug, hash::Hash};

use async_trait::async_trait;
use zkaleido::ZkVmProgram;

use crate::error::ProverResult;

/// Specification for a proof type.
///
/// Associates a domain task with a zkaleido program and defines how to
/// produce the program's input from that task. One impl per proof type.
///
/// # Example
///
/// ```rust,ignore
/// struct CheckpointSpec { storage: Arc<NodeStorage> }
///
/// #[async_trait]
/// impl ProofSpec for CheckpointSpec {
///     type Task = Epoch;
///     type Program = CheckpointProgram;
///
///     async fn fetch_input(&self, epoch: &Epoch) -> ProverResult<CheckpointProverInput> {
///         // storage queries ...
///     }
/// }
/// ```
#[async_trait]
pub trait ProofSpec: Send + Sync + 'static {
    /// Identifies a unit of work (e.g. `Epoch`, `ChunkTask`).
    type Task: Clone + Debug + Eq + Hash + Send + Sync + 'static;

    /// The zkaleido program to execute. Input must be `Send` for `spawn_blocking`.
    type Program: ZkVmProgram<Input: Send + Sync> + Send + Sync + 'static;

    /// Fetch the proof input for a task.
    ///
    /// Return [`ProverError::TransientFailure`] for retriable errors,
    /// [`ProverError::PermanentFailure`] for fatal ones.
    async fn fetch_input(
        &self,
        task: &Self::Task,
    ) -> ProverResult<<Self::Program as ZkVmProgram>::Input>;
}
