//! Prove strategies: native and remote.
//!
//! Both are sync/blocking — called inside `spawn_blocking` by the prover.
//! The `Host` type is captured at build time and erased via `dyn ProveStrategy<H>`.

use std::sync::Arc;
#[cfg(feature = "remote")]
use std::time::Duration;

use zkaleido::{ProofReceiptWithMetadata, ZkVmHost, ZkVmProgram};

use crate::{
    error::{ProverError, ProverResult},
    spec::ProofSpec,
};

/// Blocking prove operation. Called inside `spawn_blocking`.
///
/// Implementations capture the zkVM host internally. The `Host` type
/// is erased when stored as `Arc<dyn ProveStrategy<H>>` in the prover.
pub trait ProveStrategy<H: ProofSpec>: Send + Sync + 'static {
    fn prove(
        &self,
        input: &<H::Program as ZkVmProgram>::Input,
    ) -> ProverResult<ProofReceiptWithMetadata>;
}

/// Native execution: `ZkVmProgram::prove` directly.
pub(crate) struct NativeStrategy<Host> {
    host: Arc<Host>,
}

impl<Host> NativeStrategy<Host> {
    pub(crate) fn new(host: Host) -> Self {
        Self {
            host: Arc::new(host),
        }
    }
}

impl<H, Host> ProveStrategy<H> for NativeStrategy<Host>
where
    H: ProofSpec,
    Host: ZkVmHost + Send + Sync + 'static,
{
    fn prove(
        &self,
        input: &<H::Program as ZkVmProgram>::Input,
    ) -> ProverResult<ProofReceiptWithMetadata> {
        H::Program::prove(input, self.host.as_ref())
            .map_err(|e| ProverError::PermanentFailure(e.to_string()))
    }
}

/// Remote execution: `start_proving` + poll + `get_proof` via a `LocalSet`.
///
/// `ZkVmRemoteProver` returns `!Send` futures, so we spin up a current-thread
/// runtime with `LocalSet` inside `spawn_blocking` to contain them.
#[cfg(feature = "remote")]
pub(crate) struct RemoteStrategy<Host> {
    host: Arc<Host>,
    poll_interval: Duration,
}

#[cfg(feature = "remote")]
impl<Host> RemoteStrategy<Host> {
    pub(crate) fn new(host: Host, poll_interval: Duration) -> Self {
        Self {
            host: Arc::new(host),
            poll_interval,
        }
    }
}

#[cfg(feature = "remote")]
impl<H, Host> ProveStrategy<H> for RemoteStrategy<Host>
where
    H: ProofSpec,
    Host: zkaleido::ZkVmRemoteHost + Send + Sync + 'static,
{
    fn prove(
        &self,
        input: &<H::Program as ZkVmProgram>::Input,
    ) -> ProverResult<ProofReceiptWithMetadata> {
        use tokio::{runtime::Builder, task::LocalSet, time::sleep};
        use zkaleido::RemoteProofStatus;

        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ProverError::Internal(e.into()))?;

        let local = LocalSet::new();
        let host = self.host.clone();
        let poll_interval = self.poll_interval;

        local.block_on(&rt, async move {
            // 1. Prepare input and start remote proving.
            let prepared = <H::Program as ZkVmProgram>::prepare_input::<Host::Input<'_>>(input)
                .map_err(|e| ProverError::PermanentFailure(e.to_string()))?;

            let proof_id = host
                .start_proving(prepared, H::Program::proof_type())
                .await
                .map_err(|e| ProverError::TransientFailure(e.to_string()))?;

            tracing::info!(%proof_id, "remote proof submitted");

            // 2. Poll until completion.
            loop {
                let status = host
                    .get_status(&proof_id)
                    .await
                    .map_err(|e| ProverError::TransientFailure(e.to_string()))?;

                match status {
                    RemoteProofStatus::Completed => {
                        tracing::info!(%proof_id, "remote proof completed");
                        break;
                    }
                    RemoteProofStatus::Failed(reason) => {
                        return Err(ProverError::PermanentFailure(format!(
                            "remote proof failed: {reason}"
                        )));
                    }
                    RemoteProofStatus::Requested | RemoteProofStatus::InProgress => {
                        sleep(poll_interval).await;
                    }
                    RemoteProofStatus::Unknown => {
                        tracing::warn!(%proof_id, "unknown remote proof status, retrying");
                        sleep(poll_interval).await;
                    }
                }
            }

            // 3. Retrieve the receipt.
            let receipt = host
                .get_proof(&proof_id)
                .await
                .map_err(|e| ProverError::PermanentFailure(e.to_string()))?;

            // 4. Verify output is well-formed.
            let _ = <H::Program as ZkVmProgram>::process_output::<Host>(
                receipt.receipt().public_values(),
            )
            .map_err(|e| ProverError::PermanentFailure(e.to_string()))?;

            Ok(receipt)
        })
    }
}
