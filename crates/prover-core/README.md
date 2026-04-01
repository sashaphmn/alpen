# strata-prover-core

The core proving engine for Strata. Each prover instance handles one proof type
end-to-end: fetching inputs, generating proofs (locally or via a remote backend),
storing receipts, and retrying on failure.

## What problem does this solve?

Proof generation involves a lot of moving parts — input preparation, host selection,
error handling, retries, persistence, crash recovery. Without a shared engine, every
consumer (checkpoint prover, EE chunk prover, etc.) would re-implement all of that.

prover-core extracts the common lifecycle so consumers only define **what** to prove
and **how to get the input**. Everything else — scheduling, retry, storage, host
dispatch — is handled here.

## How it relates to zkaleido

[zkaleido](../zkaleido) owns the zkVM abstraction: programs, hosts, receipts.
prover-core never talks to a zkVM directly — it calls zkaleido's traits through
a pluggable strategy layer.

```
                      zkaleido land                    prover-core land
                 ┌───────────────────────┐      ┌──────────────────────────┐
 Consumer ──────▶│ ZkVmProgram::Input    │─────▶│ ProofSpec::fetch_input   │
                 └───────────────────────┘      └────────────┬─────────────┘
                                                             │
                 ┌───────────────────────┐      ┌────────────▼─────────────┐
                 │ ZkVmHost / RemoteHost │◀─────│ ProveStrategy::prove()   │
                 │ prove / start+poll    │      │ (NativeStrategy or       │
                 └───────────┬───────────┘      │  RemoteStrategy)         │
                             │                  └────────────┬─────────────┘
                 ┌───────────▼───────────┐      ┌────────────▼─────────────┐
                 │ ProofReceiptWithMeta   │─────▶│ ReceiptStore / Hook      │
                 └───────────────────────┘      └──────────────────────────┘
```

The key zkaleido types prover-core depends on:

- **`ZkVmProgram`** — defines a provable program (`Input`, `Output`, `prove()`).
  Consumed via `ProofSpec::Program`.
- **`ZkVmHost`** / **`ZkVmRemoteHost`** — local and remote proving backends.
  Captured inside strategy implementations at build time.
- **`ProofReceiptWithMetadata`** — the proof artifact that comes out the other end.

## Core concepts

### ProofSpec — the consumer's only job

A `ProofSpec` is the single trait consumers implement. It answers three questions:

1. What identifies a task? (`type Task`)
2. What program runs? (`type Program`)
3. How do you get the input? (`fn fetch_input`)

```rust
#[async_trait]
pub trait ProofSpec: Send + Sync + 'static {
    type Task: Clone + Debug + Eq + Hash + Send + Sync + 'static;
    type Program: ZkVmProgram<Input: Send + Sync> + Send + Sync + 'static;

    async fn fetch_input(
        &self,
        task: &Self::Task,
    ) -> ProverResult<<Self::Program as ZkVmProgram>::Input>;
}
```

A concrete example — proving OL checkpoints:

```rust
struct CheckpointSpec { storage: Arc<NodeStorage> }

#[async_trait]
impl ProofSpec for CheckpointSpec {
    type Task = Epoch;
    type Program = CheckpointProgram;

    async fn fetch_input(&self, epoch: &Epoch) -> ProverResult<CheckpointProverInput> {
        let header = self.storage.get_epoch_header(epoch)
            .map_err(|e| ProverError::TransientFailure(e.to_string()))?;
        let state = self.storage.get_state_at(epoch)
            .map_err(|e| ProverError::TransientFailure(e.to_string()))?;
        Ok(CheckpointProverInput { header, state })
    }
}
```

That's the entire integration surface. No storage wiring, no host selection, no
retry logic.

### ProveStrategy — how proving actually happens

The strategy is the bridge between prover-core and zkaleido's host layer. The host
type is captured at build time and erased, so `Prover<S>` has no host type parameter.

Two built-in strategies:

- **`NativeStrategy`** — calls `ZkVmProgram::prove()` directly. Good for tests, dev,
  and local RISC0.
- **`RemoteStrategy`** (behind the `remote` feature) — drives the async
  `start_proving` → poll `get_status` → `get_proof` cycle for backends like the
  SP1 network.

### Adding a new host (e.g. RISC0 remote, custom backend)

Adding a new proving backend doesn't require touching prover-core at all — it's
entirely a zkaleido concern. The steps:

1. **Implement `ZkVmHost`** in zkaleido for local execution, or `ZkVmRemoteHost`
   for an async remote backend. This is where the actual zkVM integration lives:
   input preparation, proof generation, status polling, receipt retrieval.
2. **Pass it to the builder** — `.native(your_host)` or `.remote(your_host)`.
   That's it. prover-core erases the host type behind a `ProveStrategy` and the
   rest of the system (specs, task lifecycle, PaaS) is completely unaware.

For example, a RISC0 remote prover would implement `ZkVmRemoteHost` with
`start_proving` submitting to Bonsai, `get_status` polling the Bonsai API, and
`get_proof` downloading the receipt. The consumer code and PaaS wiring stay identical
— only the `.remote(risc0_bonsai_host)` builder call changes.

If neither built-in strategy fits (e.g. a backend with a fundamentally different
execution model), you can implement `ProveStrategy<S>` directly and pass it to
`ProverBuilder::build()`.

### Task lifecycle

Every task moves through a simple state machine:

```
Pending → Queued → Proving → Completed
                         ↘ TransientFailure (retried → back to Queued)
                         ↘ PermanentFailure (terminal)
```

Retries are passive — `tick()` scans for retriable tasks and re-spawns them.
No background scheduler thread.

### Prover and ProverBuilder

You build a prover by combining a spec with a strategy and optional extensions:

```rust
let prover = ProverBuilder::new(spec)
    .receipt_store(sled_store)           // opt-in: receipt persistence by UUID
    .receipt_hook(checkpoint_db_hook)    // opt-in: domain-specific side-write
    .task_store(sled_task_store)         // default: InMemoryTaskStore
    .retry(RetryConfig::default())
    .native(host);                       // or .remote(host)
```

The consumer API is intentionally small:

| Method | What it does |
|--------|-------------|
| `submit(task)` | Spawn a background prove. Returns a UUID. Idempotent by task identity. |
| `execute(task)` | Submit + block until done. |
| `wait_for_tasks(uuids)` | Block until all tasks reach a terminal state (watch-channel, zero-poll). |
| `get_receipt(uuid)` | Read the stored receipt (requires a configured `ReceiptStore`). |

## Optional extensions

### ReceiptStore

Persists proof receipts keyed by task UUID. When configured, prover-core
auto-stores after proving and exposes `get_receipt()` on the handle.

`InMemoryReceiptStore` ships for tests. For production, implement against your DB.

### ReceiptHook

A typed callback that fires after a receipt is stored. Useful when you need to
write the receipt to a secondary store keyed by domain identity (e.g., a ProofDB
indexed by epoch number rather than UUID).

Most consumers don't need this.

## Task persistence

`TaskStore` handles task record persistence. Two implementations ship:

- **`InMemoryTaskStore`** — default, for tests and dev.
- **`SledTaskStore`** (behind the `sled` feature) — persistent, supports crash recovery.

Task records include an optional `metadata` field for strategy-specific state
(e.g., a remote `ProofId` for resuming polls after a restart).

## Feature flags

| Feature | What it enables |
|---------|----------------|
| `remote` | `RemoteStrategy` and `ProverBuilder::remote()`. Pulls in `zkaleido/remote-prover`. |
| `sled` | `SledTaskStore`. Pulls in `sled`. |

## Feature status

### Implemented

- **Task lifecycle** — full state machine (`Pending → Queued → Proving → Completed / Failed`)
  with idempotent submission, status tracking, and watch-channel notifications.
- **Native proving** — `NativeStrategy` calls `ZkVmProgram::prove()` via `spawn_blocking`.
  Works with any `ZkVmHost` (SP1 local, RISC0, native mock).
- **Remote proving** — `RemoteStrategy` drives the full async cycle: `start_proving` →
  poll `get_status` → `get_proof`, with configurable poll intervals. Feature-gated
  behind `remote`.
- **Retry with exponential backoff** — configurable base delay, multiplier, max delay,
  and max attempts. Passive: `tick()` scans for retriable tasks, no background threads.
- **Crash recovery** — on first `tick()`, re-spawns all in-progress tasks found in the
  task store. Works with both in-memory and persistent stores.
- **Receipt persistence** — optional `ReceiptStore` trait with UUID-keyed put/get.
  Auto-stores after proving. `InMemoryReceiptStore` ships for tests.
- **Domain hooks** — optional `ReceiptHook` for typed side-writes after receipt storage
  (e.g., writing to a ProofDB keyed by epoch).
- **In-memory task store** — default `InMemoryTaskStore`, thread-safe, no dependencies.
- **Persistent task store (sled)** — `SledTaskStore` with Borsh serialization, temporal
  metadata, and opaque metadata field. Feature-gated behind `sled`.
- **Consumer API** — four methods cover all use cases: `submit`, `execute`,
  `wait_for_tasks`, `get_receipt`.
- **Builder pattern** — `ProverBuilder` with fluent configuration, host type erasure at
  build time, and compile-time strategy selection (`.native()` vs `.remote()`).

### Planned

- **Remote proof resumption** — the `TaskRecord.metadata` field and `SledTaskStore`
  persistence are in place, but `RemoteStrategy` doesn't yet populate it with the
  remote `ProofId`. Once wired, a restarted prover can resume polling for in-flight
  remote proofs instead of re-submitting them.
- **Metrics instrumentation** — counters for tasks submitted/completed/failed, histograms
  for proving duration. The hooks are natural extension points.

## What prover-core does NOT do

- **Service lifecycle** (start, stop, health) — that's PaaS.
- **Tick scheduling** — PaaS calls `tick()` on an interval.
- **Pipeline orchestration** (chunk → acct dependencies) — consumer code.
- **RPC exposure** — consumer's binary.
