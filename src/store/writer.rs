//! T9 dedicated writer thread, `WriterClient`, and `WriterLifecycle`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

use crate::lockfile::OwnerRecord;

use super::{CleanupStats, LedgerEntry, PersistBatch, PersistOp, Store, StoreError};

const WRITER_QUEUE_CAPACITY: usize = 256;

/// Persistence command whose failure determines writer health.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceOperation {
    Apply,
    Cleanup,
    UpdateOwnerLocation,
    ReplaceOwner,
    Barrier,
    Checkpoint,
}

/// Stage at which a persistence command failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistencePhase {
    QueueAdmission,
    CommandExecution,
    PostApplyCommit,
    Acknowledgement,
}

/// What is known about durable state after a persistence failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityDisposition {
    NotApplicable,
    NotCommitted,
    Committed,
    Unknown,
}

/// Closed, privacy-safe class of persistence failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceFailureCode {
    Sqlite,
    Io,
    InvalidData,
    Clock,
    OwnerAbsent,
    CheckpointBusy,
    ChannelClosed,
    AcknowledgementDropped,
}

/// Typed persistence failure without raw private error detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PersistenceFailure {
    pub operation: PersistenceOperation,
    pub phase: PersistencePhase,
    pub code: PersistenceFailureCode,
    pub durability: DurabilityDisposition,
}

/// Process-lifetime persistence health published by the writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PersistenceStatus {
    Healthy,
    Degraded { failure: PersistenceFailure },
}

/// Errors produced by the dedicated SQLite writer.
#[derive(Debug, Error)]
pub enum WriterError {
    /// Pre-client writer startup failed while reading the store.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A runtime persistence operation failed with closed, typed detail.
    #[error("typed persistence failure: {0:?}")]
    Persistence(PersistenceFailure),
    /// The writer stopped before accepting the requested operation.
    #[error("SQLite writer is no longer available")]
    Closed,
    /// The writer accepted an operation but stopped before acknowledging it.
    #[error("SQLite writer stopped before acknowledging the operation")]
    AcknowledgementDropped,
    /// The dedicated OS thread panicked.
    #[error("SQLite writer thread panicked")]
    ThreadPanicked,
    /// The dedicated OS thread could not be created.
    #[error("failed to spawn SQLite writer thread: {0}")]
    ThreadSpawn(#[source] std::io::Error),
}

/// Cloneable, bounded command channel for the single SQLite writer.
#[derive(Clone)]
pub struct WriterClient {
    sender: mpsc::Sender<WriterCommand>,
    ledger: Arc<Mutex<EventLedgerCache>>,
    health: PersistenceHealth,
}

#[derive(Clone)]
struct PersistenceHealth {
    sender: watch::Sender<PersistenceStatus>,
}

impl PersistenceHealth {
    fn new() -> Self {
        let (sender, _receiver) = watch::channel(PersistenceStatus::Healthy);
        Self { sender }
    }

    fn status(&self) -> PersistenceStatus {
        *self.sender.borrow()
    }

    fn subscribe(&self) -> watch::Receiver<PersistenceStatus> {
        self.sender.subscribe()
    }

    fn publish_failure(&self, failure: PersistenceFailure) {
        self.sender.send_if_modified(|status| match status {
            PersistenceStatus::Healthy => {
                *status = PersistenceStatus::Degraded { failure };
                true
            }
            PersistenceStatus::Degraded { .. } => false,
        });
    }

    fn runtime_error(&self, failure: PersistenceFailure) -> WriterError {
        self.publish_failure(failure);
        WriterError::Persistence(failure)
    }
}

struct AcknowledgementObservationGuard {
    health: PersistenceHealth,
    operation: PersistenceOperation,
    armed: bool,
}

impl AcknowledgementObservationGuard {
    fn new(health: PersistenceHealth, operation: PersistenceOperation) -> Self {
        debug_assert!(matches!(
            operation,
            PersistenceOperation::Apply
                | PersistenceOperation::Cleanup
                | PersistenceOperation::UpdateOwnerLocation
                | PersistenceOperation::ReplaceOwner
        ));
        Self {
            health,
            operation,
            armed: false,
        }
    }

    fn arm(&mut self) {
        self.armed = true;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AcknowledgementObservationGuard {
    fn drop(&mut self) {
        if self.armed && !std::thread::panicking() {
            self.health
                .publish_failure(acknowledgement_failure(self.operation));
        }
    }
}

/// In-memory mirror of the durable seven-day event ledger.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventLedgerCache {
    entries: HashMap<String, i64>,
}

impl EventLedgerCache {
    #[must_use]
    pub fn from_entries(entries: impl IntoIterator<Item = LedgerEntry>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|entry| (entry.event_id, entry.seen_at_ms))
                .collect(),
        }
    }

    #[must_use]
    pub fn contains(&self, event_id: &str) -> bool {
        self.entries.contains_key(event_id)
    }

    /// Reserves a new accepted ID. A duplicate leaves the cache unchanged.
    pub fn reserve(&mut self, event_id: String, seen_at_ms: i64) -> bool {
        match self.entries.entry(event_id) {
            std::collections::hash_map::Entry::Occupied(_) => false,
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(seen_at_ms);
                true
            }
        }
    }

    /// Removes only entries that still name the exact durable row deleted by cleanup.
    pub fn apply_cleanup(&mut self, deleted: &[LedgerEntry]) {
        for entry in deleted {
            if self.entries.get(&entry.event_id) == Some(&entry.seen_at_ms) {
                self.entries.remove(&entry.event_id);
            }
        }
    }
}

/// Owned capacity in the bounded writer command channel.
pub struct EnqueuePermit {
    permit: mpsc::OwnedPermit<WriterCommand>,
    ledger: Arc<Mutex<EventLedgerCache>>,
    health: PersistenceHealth,
}

/// An already-enqueued write whose queue admission can no longer fail.
pub struct PendingEnqueue {
    response: oneshot::Receiver<Result<CleanupStats, PersistenceFailure>>,
    ledger: Arc<Mutex<EventLedgerCache>>,
    acknowledgement_observation: AcknowledgementObservationGuard,
}

impl PendingEnqueue {
    /// Waits for commit and applies the exact cleanup response to the dedup mirror.
    pub async fn wait(mut self) -> Result<CleanupStats, WriterError> {
        let response_result = (&mut self.response).await;
        self.acknowledgement_observation.disarm();
        let cleanup = match response_result {
            Ok(Ok(cleanup)) => cleanup,
            Ok(Err(failure)) => return Err(WriterError::Persistence(failure)),
            Err(_) => {
                return Err(self
                    .acknowledgement_observation
                    .health
                    .runtime_error(acknowledgement_failure(PersistenceOperation::Apply)));
            }
        };
        let mut ledger = lock_ledger(&self.ledger);
        ledger.apply_cleanup(&cleanup.deleted_ledger_entries);
        Ok(cleanup)
    }
}

impl EnqueuePermit {
    /// Consumes the permit and enqueues a batch without another fallible channel operation.
    #[must_use]
    pub fn enqueue(self, batch: PersistBatch) -> PendingEnqueue {
        let inserted = ledger_entries(&batch);
        {
            let mut ledger = lock_ledger(&self.ledger);
            for entry in inserted {
                let _ = ledger.reserve(entry.event_id, entry.seen_at_ms);
            }
        }
        let (acknowledgement, response) = oneshot::channel();
        let mut acknowledgement_observation =
            AcknowledgementObservationGuard::new(self.health, PersistenceOperation::Apply);
        self.permit.send(WriterCommand::Apply {
            batch,
            acknowledgement,
        });
        acknowledgement_observation.arm();
        PendingEnqueue {
            response,
            ledger: self.ledger,
            acknowledgement_observation,
        }
    }
}

impl WriterClient {
    /// Returns the current process-lifetime persistence health.
    #[must_use]
    pub fn persistence_status(&self) -> PersistenceStatus {
        self.health.status()
    }

    /// Subscribes to persistence health without exposing mutation or command capability.
    #[must_use]
    pub fn subscribe_persistence(&self) -> watch::Receiver<PersistenceStatus> {
        self.health.subscribe()
    }

    /// Atomically commits one reducer persistence batch.
    pub async fn apply(&self, batch: PersistBatch) -> Result<(), WriterError> {
        let operation = PersistenceOperation::Apply;
        let (acknowledgement, response) = oneshot::channel();
        self.sender
            .send(WriterCommand::Apply {
                batch,
                acknowledgement,
            })
            .await
            .map_err(|_| self.health.runtime_error(queue_failure(operation)))?;
        let cleanup = self.receive_mutating(response, operation).await?;
        let mut ledger = lock_ledger(&self.ledger);
        ledger.apply_cleanup(&cleanup.deleted_ledger_entries);
        Ok(())
    }

    /// Non-blockingly reserves one slot in the bounded writer command channel.
    #[must_use]
    pub fn reserve_enqueue(&self) -> Option<EnqueuePermit> {
        if self.persistence_status() != PersistenceStatus::Healthy {
            return None;
        }

        match self.sender.clone().try_reserve_owned() {
            Ok(permit) if self.persistence_status() == PersistenceStatus::Healthy => {
                Some(EnqueuePermit {
                    permit,
                    ledger: Arc::clone(&self.ledger),
                    health: self.health.clone(),
                })
            }
            Ok(_permit) => None,
            Err(mpsc::error::TrySendError::Full(_)) => None,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.health
                    .publish_failure(queue_failure(PersistenceOperation::Apply));
                None
            }
        }
    }

    /// Returns whether the durable-ledger mirror already contains `event_id`.
    #[must_use]
    pub fn is_duplicate(&self, event_id: &str) -> bool {
        lock_ledger(&self.ledger).contains(event_id)
    }

    /// Drives a periodic retention pass and conditionally evicts its exact deleted rows.
    pub async fn cleanup(&self, now_ms: i64) -> Result<CleanupStats, WriterError> {
        let operation = PersistenceOperation::Cleanup;
        let (acknowledgement, response) = oneshot::channel();
        self.sender
            .send(WriterCommand::Cleanup {
                now_ms,
                acknowledgement,
            })
            .await
            .map_err(|_| self.health.runtime_error(queue_failure(operation)))?;
        let cleanup = self.receive_mutating(response, operation).await?;
        lock_ledger(&self.ledger).apply_cleanup(&cleanup.deleted_ledger_entries);
        Ok(cleanup)
    }

    /// Commits the owner's current terminal and public-pane location.
    pub async fn update_owner_location(
        &self,
        terminal_id: &str,
        pane_id: &str,
    ) -> Result<(), WriterError> {
        self.request_mutating(
            PersistenceOperation::UpdateOwnerLocation,
            |acknowledgement| WriterCommand::UpdateOwnerLocation {
                terminal_id: terminal_id.to_owned(),
                pane_id: pane_id.to_owned(),
                acknowledgement,
            },
        )
        .await
    }

    /// Atomically replaces the owner row and acknowledges after commit.
    pub async fn replace_owner(&self, rec: OwnerRecord) -> Result<(), WriterError> {
        self.request_mutating(PersistenceOperation::ReplaceOwner, |acknowledgement| {
            WriterCommand::ReplaceOwner {
                record: rec,
                acknowledgement,
            }
        })
        .await
    }

    /// Acknowledges after every command queued before this call has completed.
    pub async fn barrier(&self) -> Result<(), WriterError> {
        self.request(PersistenceOperation::Barrier, |acknowledgement| {
            WriterCommand::Barrier { acknowledgement }
        })
        .await
    }

    async fn request(
        &self,
        operation: PersistenceOperation,
        command: impl FnOnce(oneshot::Sender<Result<(), PersistenceFailure>>) -> WriterCommand,
    ) -> Result<(), WriterError> {
        let (acknowledgement, response) = oneshot::channel();
        self.sender
            .send(command(acknowledgement))
            .await
            .map_err(|_| self.health.runtime_error(queue_failure(operation)))?;
        self.receive(response, operation).await
    }

    async fn request_mutating(
        &self,
        operation: PersistenceOperation,
        command: impl FnOnce(oneshot::Sender<Result<(), PersistenceFailure>>) -> WriterCommand,
    ) -> Result<(), WriterError> {
        let (acknowledgement, response) = oneshot::channel();
        self.sender
            .send(command(acknowledgement))
            .await
            .map_err(|_| self.health.runtime_error(queue_failure(operation)))?;
        self.receive_mutating(response, operation).await
    }

    async fn receive<T>(
        &self,
        response: oneshot::Receiver<Result<T, PersistenceFailure>>,
        operation: PersistenceOperation,
    ) -> Result<T, WriterError> {
        let response_result = response.await;
        self.classify_response(response_result, operation)
    }

    async fn receive_mutating<T>(
        &self,
        response: oneshot::Receiver<Result<T, PersistenceFailure>>,
        operation: PersistenceOperation,
    ) -> Result<T, WriterError> {
        let mut acknowledgement_observation =
            AcknowledgementObservationGuard::new(self.health.clone(), operation);
        acknowledgement_observation.arm();
        let response_result = response.await;
        acknowledgement_observation.disarm();
        self.classify_response(response_result, operation)
    }

    fn classify_response<T>(
        &self,
        response_result: Result<Result<T, PersistenceFailure>, oneshot::error::RecvError>,
        operation: PersistenceOperation,
    ) -> Result<T, WriterError> {
        match response_result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(failure)) => Err(WriterError::Persistence(failure)),
            Err(_) => Err(self
                .health
                .runtime_error(acknowledgement_failure(operation))),
        }
    }
}

/// Unique lifecycle owner for the dedicated writer thread.
pub struct WriterLifecycle {
    sender: mpsc::Sender<WriterCommand>,
    thread: Option<JoinHandle<()>>,
    health: PersistenceHealth,
}

impl WriterLifecycle {
    /// Drains queued commands, checkpoints the WAL, and joins the OS thread.
    pub async fn shutdown(mut self) -> Result<(), WriterError> {
        let (acknowledgement, response) = oneshot::channel();
        let send_result = self
            .sender
            .send(WriterCommand::Shutdown { acknowledgement })
            .await;
        drop(self.sender);

        let operation_result = match send_result {
            Ok(()) => match response.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(failure)) => Err(WriterError::Persistence(failure)),
                Err(_) => Err(self
                    .health
                    .runtime_error(acknowledgement_failure(PersistenceOperation::Checkpoint))),
            },
            Err(_) => Err(self
                .health
                .runtime_error(queue_failure(PersistenceOperation::Checkpoint))),
        };
        let join_result = self
            .thread
            .take()
            .ok_or(WriterError::ThreadPanicked)?
            .join()
            .map_err(|_| WriterError::ThreadPanicked);

        operation_result?;
        join_result
    }
}

/// Starts one dedicated OS thread that exclusively owns the supplied store.
pub fn spawn_writer(store: Store) -> Result<(WriterLifecycle, WriterClient), WriterError> {
    spawn_writer_inner(store, super::unix_now_ms)
}

#[cfg(test)]
fn spawn_writer_with_clock(
    store: Store,
    clock: fn() -> Result<i64, StoreError>,
) -> Result<(WriterLifecycle, WriterClient), WriterError> {
    spawn_writer_inner(store, clock)
}

fn spawn_writer_inner(
    store: Store,
    clock: fn() -> Result<i64, StoreError>,
) -> Result<(WriterLifecycle, WriterClient), WriterError> {
    let ledger = Arc::new(Mutex::new(EventLedgerCache::from_entries(
        store.load_event_ledger()?,
    )));
    let (sender, receiver) = mpsc::channel(WRITER_QUEUE_CAPACITY);
    let health = PersistenceHealth::new();
    let writer_health = health.clone();
    let writer_ledger = Arc::clone(&ledger);
    let thread = thread::Builder::new()
        .name("herdr-top-sqlite-writer".to_owned())
        .spawn(move || writer_main(store, receiver, writer_health, writer_ledger, clock))
        .map_err(WriterError::ThreadSpawn)?;
    let client = WriterClient {
        sender: sender.clone(),
        ledger,
        health: health.clone(),
    };
    let lifecycle = WriterLifecycle {
        sender,
        thread: Some(thread),
        health,
    };
    Ok((lifecycle, client))
}

enum WriterCommand {
    Apply {
        batch: PersistBatch,
        acknowledgement: oneshot::Sender<Result<CleanupStats, PersistenceFailure>>,
    },
    Cleanup {
        now_ms: i64,
        acknowledgement: oneshot::Sender<Result<CleanupStats, PersistenceFailure>>,
    },
    UpdateOwnerLocation {
        terminal_id: String,
        pane_id: String,
        acknowledgement: oneshot::Sender<Result<(), PersistenceFailure>>,
    },
    ReplaceOwner {
        record: OwnerRecord,
        acknowledgement: oneshot::Sender<Result<(), PersistenceFailure>>,
    },
    Barrier {
        acknowledgement: oneshot::Sender<Result<(), PersistenceFailure>>,
    },
    Shutdown {
        acknowledgement: oneshot::Sender<Result<(), PersistenceFailure>>,
    },
}

fn writer_main(
    mut store: Store,
    mut receiver: mpsc::Receiver<WriterCommand>,
    health: PersistenceHealth,
    ledger: Arc<Mutex<EventLedgerCache>>,
    clock: fn() -> Result<i64, StoreError>,
) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            WriterCommand::Apply {
                batch,
                acknowledgement,
            } => {
                let inserted = ledger_entries(&batch);
                let result = match store.apply_batch(batch) {
                    Ok(()) => {
                        let mut cache = lock_ledger(&ledger);
                        for entry in inserted {
                            let _ = cache.reserve(entry.event_id, entry.seen_at_ms);
                        }
                        drop(cache);
                        clock()
                            .and_then(|now_ms| store.cleanup_retention(now_ms))
                            .map_err(|error| {
                                store_failure(
                                    PersistenceOperation::Cleanup,
                                    PersistencePhase::PostApplyCommit,
                                    DurabilityDisposition::Committed,
                                    &error,
                                )
                            })
                    }
                    Err(error) => Err(store_failure(
                        PersistenceOperation::Apply,
                        PersistencePhase::CommandExecution,
                        DurabilityDisposition::NotCommitted,
                        &error,
                    )),
                };
                publish_result_failure(&result, &health);
                let _ = acknowledgement.send(result);
            }
            WriterCommand::Cleanup {
                now_ms,
                acknowledgement,
            } => {
                let result = store.cleanup_retention(now_ms).map_err(|error| {
                    store_failure(
                        PersistenceOperation::Cleanup,
                        PersistencePhase::CommandExecution,
                        DurabilityDisposition::NotCommitted,
                        &error,
                    )
                });
                publish_result_failure(&result, &health);
                let _ = acknowledgement.send(result);
            }
            WriterCommand::UpdateOwnerLocation {
                terminal_id,
                pane_id,
                acknowledgement,
            } => {
                let result = store
                    .update_owner_location(&terminal_id, &pane_id)
                    .map_err(|error| {
                        store_failure(
                            PersistenceOperation::UpdateOwnerLocation,
                            PersistencePhase::CommandExecution,
                            DurabilityDisposition::NotCommitted,
                            &error,
                        )
                    });
                publish_result_failure(&result, &health);
                let _ = acknowledgement.send(result);
            }
            WriterCommand::ReplaceOwner {
                record,
                acknowledgement,
            } => {
                let result = store.replace_owner(&record).map_err(|error| {
                    store_failure(
                        PersistenceOperation::ReplaceOwner,
                        PersistencePhase::CommandExecution,
                        DurabilityDisposition::NotCommitted,
                        &error,
                    )
                });
                publish_result_failure(&result, &health);
                let _ = acknowledgement.send(result);
            }
            WriterCommand::Barrier { acknowledgement } => {
                let _ = acknowledgement.send(Ok(()));
            }
            WriterCommand::Shutdown { acknowledgement } => {
                let result = store.checkpoint().map_err(|error| {
                    store_failure(
                        PersistenceOperation::Checkpoint,
                        PersistencePhase::CommandExecution,
                        DurabilityDisposition::NotApplicable,
                        &error,
                    )
                });
                publish_result_failure(&result, &health);
                let _ = acknowledgement.send(result);
                break;
            }
        }
    }
}

fn publish_result_failure<T>(result: &Result<T, PersistenceFailure>, health: &PersistenceHealth) {
    if let Err(failure) = result {
        health.publish_failure(*failure);
    }
}

const fn persistence_failure(
    operation: PersistenceOperation,
    phase: PersistencePhase,
    code: PersistenceFailureCode,
    durability: DurabilityDisposition,
) -> PersistenceFailure {
    PersistenceFailure {
        operation,
        phase,
        code,
        durability,
    }
}

fn store_failure(
    operation: PersistenceOperation,
    phase: PersistencePhase,
    durability: DurabilityDisposition,
    error: &StoreError,
) -> PersistenceFailure {
    persistence_failure(operation, phase, classify_store_error(error), durability)
}

const fn queue_failure(operation: PersistenceOperation) -> PersistenceFailure {
    persistence_failure(
        operation,
        PersistencePhase::QueueAdmission,
        PersistenceFailureCode::ChannelClosed,
        non_acknowledgement_durability(operation),
    )
}

const fn acknowledgement_failure(operation: PersistenceOperation) -> PersistenceFailure {
    let durability = match operation {
        PersistenceOperation::Apply
        | PersistenceOperation::Cleanup
        | PersistenceOperation::UpdateOwnerLocation
        | PersistenceOperation::ReplaceOwner => DurabilityDisposition::Unknown,
        PersistenceOperation::Barrier | PersistenceOperation::Checkpoint => {
            DurabilityDisposition::NotApplicable
        }
    };
    persistence_failure(
        operation,
        PersistencePhase::Acknowledgement,
        PersistenceFailureCode::AcknowledgementDropped,
        durability,
    )
}

const fn non_acknowledgement_durability(operation: PersistenceOperation) -> DurabilityDisposition {
    match operation {
        PersistenceOperation::Apply
        | PersistenceOperation::Cleanup
        | PersistenceOperation::UpdateOwnerLocation
        | PersistenceOperation::ReplaceOwner => DurabilityDisposition::NotCommitted,
        PersistenceOperation::Barrier | PersistenceOperation::Checkpoint => {
            DurabilityDisposition::NotApplicable
        }
    }
}

fn classify_store_error(error: &StoreError) -> PersistenceFailureCode {
    match error {
        StoreError::Sqlite(_) => PersistenceFailureCode::Sqlite,
        StoreError::Io { .. } | StoreError::Backup { .. } => PersistenceFailureCode::Io,
        StoreError::InvalidData { .. }
        | StoreError::IntegerOutOfRange { .. }
        | StoreError::NewerSchema { .. }
        | StoreError::SchemaNotCurrent
        | StoreError::DatabaseAbsent(_)
        | StoreError::PragmaNotApplied { .. } => PersistenceFailureCode::InvalidData,
        StoreError::Clock(_) => PersistenceFailureCode::Clock,
        StoreError::OwnerAbsent => PersistenceFailureCode::OwnerAbsent,
        StoreError::CheckpointBusy { .. } => PersistenceFailureCode::CheckpointBusy,
    }
}

fn lock_ledger(ledger: &Arc<Mutex<EventLedgerCache>>) -> MutexGuard<'_, EventLedgerCache> {
    match ledger.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn ledger_entries(batch: &PersistBatch) -> Vec<LedgerEntry> {
    batch
        .iter()
        .filter_map(|operation| match operation {
            PersistOp::RecordEvent { event, seen_at_ms } => Some(LedgerEntry {
                event_id: normalized_event_id(event).to_owned(),
                seen_at_ms: *seen_at_ms,
            }),
            PersistOp::RecordCollectorGap(gap) => Some(LedgerEntry {
                event_id: gap.event_id.clone(),
                seen_at_ms: gap.seen_at_ms,
            }),
            _ => None,
        })
        .collect()
}

fn normalized_event_id(event: &crate::model::NormalizedEvent) -> &str {
    use crate::model::NormalizedEvent;

    match event {
        NormalizedEvent::ControllerEvent { metadata, .. }
        | NormalizedEvent::TopologyUpsert { metadata, .. }
        | NormalizedEvent::TopologyClosure { metadata, .. }
        | NormalizedEvent::AgentStatusChanged { metadata, .. }
        | NormalizedEvent::AgentNodeUpsert { metadata, .. }
        | NormalizedEvent::AgentActivity { metadata, .. }
        | NormalizedEvent::ExecutionBegin { metadata, .. }
        | NormalizedEvent::ExecutionEnd { metadata, .. } => &metadata.event_id,
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::PathBuf;
    use std::sync::OnceLock;
    use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use rusqlite::{Connection, OpenFlags};
    use serde_json::json;

    use crate::lockfile::{OwnerRecord, StateRoot};
    use crate::model::{EventMetadata, GapKind, NormalizedEvent, TopologyEntity, Workspace};
    use crate::store::{CollectorGap, PersistOp, database_path, open_reader, open_writer};

    use super::*;

    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
    const TEST_RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(1);

    struct PanicReceiptClockRendezvous {
        reached: SyncSender<()>,
        release: Mutex<Receiver<()>>,
    }

    static PANIC_RECEIPT_CLOCK_RENDEZVOUS: OnceLock<PanicReceiptClockRendezvous> = OnceLock::new();

    fn panic_receipt_gated_clock() -> Result<i64, StoreError> {
        let rendezvous = PANIC_RECEIPT_CLOCK_RENDEZVOUS
            .get()
            .expect("panic receipt clock rendezvous must be initialized");
        rendezvous
            .reached
            .try_send(())
            .expect("panic receipt clock must be called exactly once");
        rendezvous
            .release
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv_timeout(TEST_RENDEZVOUS_TIMEOUT)
            .expect("panic receipt clock release must arrive within one second");
        super::super::unix_now_ms()
    }

    fn provider_event(event_id: &str, seen_at_ms: i64) -> PersistOp {
        PersistOp::RecordEvent {
            event: Box::new(NormalizedEvent::TopologyUpsert {
                metadata: EventMetadata {
                    event_id: event_id.to_owned(),
                    timestamp_ms: 1,
                    receipt_time_ms: seen_at_ms,
                    source: "provider".to_owned(),
                    source_event_type: "observed".to_owned(),
                    herdr_session: "session".to_owned(),
                    workspace_id: None,
                    tab_id: None,
                    pane_id: None,
                    terminal_id: None,
                    provider: None,
                    native_session_id: None,
                    task_run_id: None,
                    agent_node_id: None,
                    task_state: None,
                    execution_parent: None,
                    dependency: None,
                    source_coverage: Vec::new(),
                    provider_metadata: None,
                    label: None,
                    reason: None,
                    progress: None,
                    ingest_seq: None,
                },
                entity: TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            }),
            seen_at_ms,
        }
    }

    fn workspace_op(workspace_id: &str) -> PersistOp {
        PersistOp::UpsertWorkspace {
            workspace: Workspace {
                workspace_id: workspace_id.to_owned(),
            },
            display_ordinal: crate::model::DisplayOrdinal::new(1),
        }
    }

    fn owner_record() -> OwnerRecord {
        OwnerRecord {
            pid: 42,
            started_at_ms: 1_000,
            terminal_id: Some("terminal".to_owned()),
            pane_id: Some("pane".to_owned()),
        }
    }

    fn expected_failure(
        operation: PersistenceOperation,
        phase: PersistencePhase,
        code: PersistenceFailureCode,
        durability: DurabilityDisposition,
    ) -> PersistenceFailure {
        PersistenceFailure {
            operation,
            phase,
            code,
            durability,
        }
    }

    fn assert_persistence_error<T: std::fmt::Debug>(
        result: Result<T, WriterError>,
        expected: PersistenceFailure,
    ) {
        match result {
            Err(WriterError::Persistence(actual)) => assert_eq!(actual, expected),
            other => panic!("expected typed persistence failure {expected:?}, got {other:?}"),
        }
    }

    fn install_temp_failure_trigger(store: &Store, name: &str, action: &str, table: &str) {
        store
            .connection
            .execute_batch(&format!(
                "CREATE TEMP TRIGGER {name} BEFORE {action} ON {table} \
                 BEGIN SELECT RAISE(ABORT, 'injected test failure'); END;"
            ))
            .unwrap();
    }

    fn event_with_ingest_sequence(event_id: &str, seen_at_ms: i64, ingest_seq: u64) -> PersistOp {
        let mut operation = provider_event(event_id, seen_at_ms);
        let PersistOp::RecordEvent { event, .. } = &mut operation else {
            unreachable!("provider_event always returns RecordEvent");
        };
        let NormalizedEvent::TopologyUpsert { metadata, .. } = event.as_mut() else {
            unreachable!("provider_event always returns TopologyUpsert");
        };
        metadata.ingest_seq = Some(ingest_seq);
        operation
    }

    #[tokio::test]
    async fn i4_writer_initial_status_is_healthy_and_subscription_is_read_only() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();

        assert_eq!(writer.persistence_status(), PersistenceStatus::Healthy);
        let subscription = writer.subscribe_persistence();
        assert_eq!(*subscription.borrow(), PersistenceStatus::Healthy);
        assert!(!subscription.has_changed().unwrap());

        lifecycle.shutdown().await.unwrap();
    }

    #[test]
    fn i4_writer_health_types_serialize_with_closed_safe_spellings() {
        assert_eq!(
            serde_json::to_value([
                PersistenceOperation::Apply,
                PersistenceOperation::Cleanup,
                PersistenceOperation::UpdateOwnerLocation,
                PersistenceOperation::ReplaceOwner,
                PersistenceOperation::Barrier,
                PersistenceOperation::Checkpoint,
            ])
            .unwrap(),
            json!([
                "apply",
                "cleanup",
                "update_owner_location",
                "replace_owner",
                "barrier",
                "checkpoint"
            ])
        );
        assert_eq!(
            serde_json::to_value([
                PersistencePhase::QueueAdmission,
                PersistencePhase::CommandExecution,
                PersistencePhase::PostApplyCommit,
                PersistencePhase::Acknowledgement,
            ])
            .unwrap(),
            json!([
                "queue_admission",
                "command_execution",
                "post_apply_commit",
                "acknowledgement"
            ])
        );
        assert_eq!(
            serde_json::to_value([
                DurabilityDisposition::NotApplicable,
                DurabilityDisposition::NotCommitted,
                DurabilityDisposition::Committed,
                DurabilityDisposition::Unknown,
            ])
            .unwrap(),
            json!(["not_applicable", "not_committed", "committed", "unknown"])
        );
        assert_eq!(
            serde_json::to_value([
                PersistenceFailureCode::Sqlite,
                PersistenceFailureCode::Io,
                PersistenceFailureCode::InvalidData,
                PersistenceFailureCode::Clock,
                PersistenceFailureCode::OwnerAbsent,
                PersistenceFailureCode::CheckpointBusy,
                PersistenceFailureCode::ChannelClosed,
                PersistenceFailureCode::AcknowledgementDropped,
            ])
            .unwrap(),
            json!([
                "sqlite",
                "io",
                "invalid_data",
                "clock",
                "owner_absent",
                "checkpoint_busy",
                "channel_closed",
                "acknowledgement_dropped"
            ])
        );
        assert_eq!(
            serde_json::to_value(PersistenceStatus::Healthy).unwrap(),
            json!({ "status": "healthy" })
        );
        assert_eq!(
            serde_json::to_value(PersistenceStatus::Degraded {
                failure: expected_failure(
                    PersistenceOperation::Cleanup,
                    PersistencePhase::PostApplyCommit,
                    PersistenceFailureCode::Sqlite,
                    DurabilityDisposition::Committed,
                ),
            })
            .unwrap(),
            json!({
                "status": "degraded",
                "failure": {
                    "operation": "cleanup",
                    "phase": "post_apply_commit",
                    "code": "sqlite",
                    "durability": "committed"
                }
            })
        );
    }

    #[tokio::test]
    async fn i4_writer_first_failure_is_sticky_and_watch_wakes_exactly_once() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut subscription = writer.subscribe_persistence();
        let first = expected_failure(
            PersistenceOperation::UpdateOwnerLocation,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::OwnerAbsent,
            DurabilityDisposition::NotCommitted,
        );

        assert_persistence_error(
            writer.update_owner_location("terminal", "pane").await,
            first,
        );
        subscription.changed().await.unwrap();
        assert_eq!(
            *subscription.borrow_and_update(),
            PersistenceStatus::Degraded { failure: first }
        );
        assert!(writer.reserve_enqueue().is_none());

        lifecycle.shutdown().await.unwrap();
        let later = expected_failure(
            PersistenceOperation::Barrier,
            PersistencePhase::QueueAdmission,
            PersistenceFailureCode::ChannelClosed,
            DurabilityDisposition::NotApplicable,
        );
        assert_persistence_error(writer.barrier().await, later);
        assert_eq!(
            writer.persistence_status(),
            PersistenceStatus::Degraded { failure: first }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), subscription.changed())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn i4_writer_apply_commit_failure_is_not_committed() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        install_temp_failure_trigger(&store, "fail_apply", "INSERT", "workspaces");
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let expected = expected_failure(
            PersistenceOperation::Apply,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::NotCommitted,
        );

        assert_persistence_error(
            writer.apply(vec![workspace_op("rolled-back")]).await,
            expected,
        );
        assert_eq!(
            writer.persistence_status(),
            PersistenceStatus::Degraded { failure: expected }
        );
        lifecycle.shutdown().await.unwrap();

        let reopened = open_reader(&root).unwrap();
        assert!(
            reopened
                .load_restored_state()
                .unwrap()
                .model
                .workspace("rolled-back")
                .is_none()
        );
    }

    #[tokio::test]
    async fn i4_writer_post_commit_cleanup_failure_is_committed_but_degraded() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let now = super::super::unix_now_ms().unwrap();
        store
            .apply_batch(vec![provider_event("old-event", now - 8 * DAY_MS)])
            .unwrap();
        install_temp_failure_trigger(&store, "fail_auto_cleanup", "DELETE", "events");
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let expected = expected_failure(
            PersistenceOperation::Cleanup,
            PersistencePhase::PostApplyCommit,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::Committed,
        );

        assert_persistence_error(
            writer.apply(vec![provider_event("committed", now)]).await,
            expected,
        );
        assert!(writer.is_duplicate("committed"));
        assert_eq!(
            writer.persistence_status(),
            PersistenceStatus::Degraded { failure: expected }
        );
        lifecycle.shutdown().await.unwrap();

        let reopened = open_reader(&root).unwrap();
        let ledger = reopened.load_event_ledger().unwrap();
        assert!(ledger.iter().any(|entry| entry.event_id == "committed"));
        assert!(ledger.iter().any(|entry| entry.event_id == "old-event"));
    }

    #[tokio::test]
    async fn i4_writer_pending_apply_waiter_classifies_disappeared_sender_as_unknown() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer_with_clock(store, || {
            panic!("drop the active PendingEnqueue response sender")
        })
        .unwrap();
        let expected = expected_failure(
            PersistenceOperation::Apply,
            PersistencePhase::Acknowledgement,
            PersistenceFailureCode::AcknowledgementDropped,
            DurabilityDisposition::Unknown,
        );
        let pending = writer.reserve_enqueue().unwrap().enqueue(Vec::new());

        assert_persistence_error(pending.wait().await, expected);
        assert_eq!(
            writer.persistence_status(),
            PersistenceStatus::Degraded { failure: expected }
        );
        drop(lifecycle);
    }

    #[tokio::test]
    async fn i4_writer_late_drop_of_buffered_apply_receipt_publishes_unknown_once() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut subscription = writer.subscribe_persistence();
        let now = super::super::unix_now_ms().unwrap();
        let pending = writer
            .reserve_enqueue()
            .unwrap()
            .enqueue(vec![provider_event("late-unread", now)]);

        writer.barrier().await.unwrap();
        assert_eq!(writer.persistence_status(), PersistenceStatus::Healthy);
        drop(pending);

        tokio::time::timeout(Duration::from_secs(1), subscription.changed())
            .await
            .expect("dropping an armed unread Apply receipt must publish health")
            .unwrap();
        let expected = expected_failure(
            PersistenceOperation::Apply,
            PersistencePhase::Acknowledgement,
            PersistenceFailureCode::AcknowledgementDropped,
            DurabilityDisposition::Unknown,
        );
        assert_eq!(
            *subscription.borrow_and_update(),
            PersistenceStatus::Degraded { failure: expected }
        );
        assert!(writer.is_duplicate("late-unread"));
        lifecycle.shutdown().await.unwrap();
        assert!(!subscription.has_changed().unwrap());

        let reopened = open_reader(&root).unwrap();
        assert!(
            reopened
                .load_event_ledger()
                .unwrap()
                .iter()
                .any(|entry| entry.event_id == "late-unread")
        );
    }

    #[tokio::test]
    async fn i4_writer_abandoned_barrier_after_admission_keeps_health_healthy() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let ledger_guard = lock_ledger(&writer.ledger);
        let (apply_acknowledgement, apply_response) = oneshot::channel();
        writer
            .sender
            .try_send(WriterCommand::Apply {
                batch: Vec::new(),
                acknowledgement: apply_acknowledgement,
            })
            .unwrap();
        let (barrier_acknowledgement, barrier_response) = oneshot::channel();
        writer
            .sender
            .try_send(WriterCommand::Barrier {
                acknowledgement: barrier_acknowledgement,
            })
            .unwrap();
        drop(barrier_response);
        drop(ledger_guard);

        apply_response.await.unwrap().unwrap();
        writer.barrier().await.unwrap();
        assert_eq!(writer.persistence_status(), PersistenceStatus::Healthy);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn i4_writer_panicking_owner_of_armed_apply_receipt_keeps_health_healthy() {
        let probe_directory = tempfile::tempdir().unwrap();
        let probe_root = StateRoot(probe_directory.path().to_path_buf());
        let probe_store = open_writer(&probe_root).unwrap();
        let (probe_lifecycle, probe_writer) = spawn_writer(probe_store).unwrap();
        let probe_pending = probe_writer.reserve_enqueue().unwrap().enqueue(Vec::new());
        probe_writer.barrier().await.unwrap();
        drop(probe_pending);
        assert_eq!(
            probe_writer.persistence_status(),
            PersistenceStatus::Degraded {
                failure: expected_failure(
                    PersistenceOperation::Apply,
                    PersistencePhase::Acknowledgement,
                    PersistenceFailureCode::AcknowledgementDropped,
                    DurabilityDisposition::Unknown,
                ),
            },
            "normal drop proves the same returned receipt path is armed"
        );
        probe_lifecycle.shutdown().await.unwrap();

        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (clock_reached, reached) = sync_channel(1);
        let (release, clock_release) = sync_channel(1);
        assert!(
            PANIC_RECEIPT_CLOCK_RENDEZVOUS
                .set(PanicReceiptClockRendezvous {
                    reached: clock_reached,
                    release: Mutex::new(clock_release),
                })
                .is_ok(),
            "panic receipt clock rendezvous must be initialized exactly once"
        );
        let (lifecycle, writer) =
            spawn_writer_with_clock(store, panic_receipt_gated_clock).unwrap();
        let task_writer = writer.clone();
        let (owned_sender, owned_receiver) = oneshot::channel();
        let now = super::super::unix_now_ms().unwrap();
        let task = tokio::spawn(async move {
            let _armed_pending = task_writer
                .reserve_enqueue()
                .unwrap()
                .enqueue(vec![provider_event("panic-unwind", now)]);
            owned_sender.send(()).unwrap();
            panic!("exercise armed receipt unwind");
        });

        owned_receiver.await.unwrap();
        let join_error = task.await.unwrap_err();
        assert!(join_error.is_panic());
        reached
            .recv_timeout(TEST_RENDEZVOUS_TIMEOUT)
            .expect("writer clock must be reached within one second");
        release
            .try_send(())
            .expect("writer clock must still be waiting for its single release");
        writer.barrier().await.unwrap();
        assert_eq!(writer.persistence_status(), PersistenceStatus::Healthy);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn i4_writer_active_waiters_classify_disappeared_senders_by_operation() {
        let apply_directory = tempfile::tempdir().unwrap();
        let apply_root = StateRoot(apply_directory.path().to_path_buf());
        let apply_store = open_writer(&apply_root).unwrap();
        let (apply_lifecycle, apply_writer) = spawn_writer_with_clock(apply_store, || {
            panic!("drop the active Apply response sender")
        })
        .unwrap();
        assert_persistence_error(
            apply_writer.apply(Vec::new()).await,
            expected_failure(
                PersistenceOperation::Apply,
                PersistencePhase::Acknowledgement,
                PersistenceFailureCode::AcknowledgementDropped,
                DurabilityDisposition::Unknown,
            ),
        );
        drop(apply_lifecycle);

        let barrier_directory = tempfile::tempdir().unwrap();
        let barrier_root = StateRoot(barrier_directory.path().to_path_buf());
        let barrier_store = open_writer(&barrier_root).unwrap();
        let (barrier_lifecycle, barrier_writer) =
            spawn_writer_with_clock(barrier_store, || panic!("drop queued Barrier sender"))
                .unwrap();
        let ledger_guard = lock_ledger(&barrier_writer.ledger);
        let (apply_acknowledgement, _apply_response) = oneshot::channel();
        barrier_writer
            .sender
            .try_send(WriterCommand::Apply {
                batch: Vec::new(),
                acknowledgement: apply_acknowledgement,
            })
            .unwrap();
        let (barrier_acknowledgement, barrier_response) = oneshot::channel();
        barrier_writer
            .sender
            .try_send(WriterCommand::Barrier {
                acknowledgement: barrier_acknowledgement,
            })
            .unwrap();
        let mut barrier_wait =
            Box::pin(barrier_writer.receive(barrier_response, PersistenceOperation::Barrier));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            barrier_wait.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(ledger_guard);

        assert_persistence_error(
            barrier_wait.await,
            expected_failure(
                PersistenceOperation::Barrier,
                PersistencePhase::Acknowledgement,
                PersistenceFailureCode::AcknowledgementDropped,
                DurabilityDisposition::NotApplicable,
            ),
        );
        drop(barrier_lifecycle);
    }

    #[tokio::test]
    async fn i4_writer_standalone_cleanup_failure_is_not_committed() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let now = super::super::unix_now_ms().unwrap();
        store
            .apply_batch(vec![provider_event("retained", now - 8 * DAY_MS)])
            .unwrap();
        install_temp_failure_trigger(&store, "fail_cleanup", "DELETE", "events");
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let expected = expected_failure(
            PersistenceOperation::Cleanup,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::NotCommitted,
        );

        assert_persistence_error(writer.cleanup(now).await, expected);
        lifecycle.shutdown().await.unwrap();

        let reopened = open_reader(&root).unwrap();
        assert!(
            reopened
                .load_event_ledger()
                .unwrap()
                .iter()
                .any(|entry| entry.event_id == "retained")
        );
    }

    #[tokio::test]
    async fn i4_writer_invalid_data_and_integer_range_failures_are_typed() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let invalid = expected_failure(
            PersistenceOperation::Apply,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::InvalidData,
            DurabilityDisposition::NotCommitted,
        );

        assert_persistence_error(
            writer
                .apply(vec![PersistOp::AdvanceIngestSequence { ingest_seq: 0 }])
                .await,
            invalid,
        );
        lifecycle.shutdown().await.unwrap();

        let second_directory = tempfile::tempdir().unwrap();
        let second_root = StateRoot(second_directory.path().to_path_buf());
        let second_store = open_writer(&second_root).unwrap();
        let (second_lifecycle, second_writer) = spawn_writer(second_store).unwrap();
        assert_persistence_error(
            second_writer
                .apply(vec![event_with_ingest_sequence("too-large", 1, u64::MAX)])
                .await,
            invalid,
        );
        second_lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn i4_writer_clock_failure_after_apply_is_committed() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer_with_clock(store, || {
            Err(StoreError::Clock("injected test clock failure".to_owned()))
        })
        .unwrap();
        let expected = expected_failure(
            PersistenceOperation::Cleanup,
            PersistencePhase::PostApplyCommit,
            PersistenceFailureCode::Clock,
            DurabilityDisposition::Committed,
        );

        assert_persistence_error(
            writer.apply(vec![workspace_op("clock-committed")]).await,
            expected,
        );
        lifecycle.shutdown().await.unwrap();

        let reopened = open_reader(&root).unwrap();
        assert!(
            reopened
                .load_restored_state()
                .unwrap()
                .model
                .workspace("clock-committed")
                .is_some()
        );
    }

    #[test]
    fn i4_writer_startup_only_store_errors_have_closed_safe_codes() {
        let cases = [
            (
                StoreError::Io {
                    path: PathBuf::from("private-path"),
                    source: std::io::Error::other("private detail"),
                },
                PersistenceFailureCode::Io,
            ),
            (
                StoreError::Backup {
                    path: PathBuf::from("private-backup"),
                    source: rusqlite::Error::InvalidQuery,
                },
                PersistenceFailureCode::Io,
            ),
            (
                StoreError::NewerSchema {
                    found: 5,
                    supported: 4,
                },
                PersistenceFailureCode::InvalidData,
            ),
            (
                StoreError::SchemaNotCurrent,
                PersistenceFailureCode::InvalidData,
            ),
            (
                StoreError::DatabaseAbsent(PathBuf::from("private-database")),
                PersistenceFailureCode::InvalidData,
            ),
            (
                StoreError::PragmaNotApplied {
                    pragma: "private_pragma",
                    observed: "private value".to_owned(),
                },
                PersistenceFailureCode::InvalidData,
            ),
            (
                StoreError::IntegerOutOfRange {
                    field: "direct-classifier",
                },
                PersistenceFailureCode::InvalidData,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(classify_store_error(&error), expected);
        }
    }

    #[tokio::test]
    async fn i4_writer_channel_close_is_typed_at_apply_admission() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        lifecycle.shutdown().await.unwrap();
        let expected = expected_failure(
            PersistenceOperation::Apply,
            PersistencePhase::QueueAdmission,
            PersistenceFailureCode::ChannelClosed,
            DurabilityDisposition::NotCommitted,
        );

        assert_persistence_error(writer.apply(Vec::new()).await, expected);
        assert_eq!(
            writer.persistence_status(),
            PersistenceStatus::Degraded { failure: expected }
        );
        assert!(writer.reserve_enqueue().is_none());
    }

    #[tokio::test]
    async fn i4_writer_barrier_channel_failure_is_not_applicable() {
        let channel_directory = tempfile::tempdir().unwrap();
        let channel_root = StateRoot(channel_directory.path().to_path_buf());
        let channel_store = open_writer(&channel_root).unwrap();
        let (channel_lifecycle, channel_writer) = spawn_writer(channel_store).unwrap();
        channel_lifecycle.shutdown().await.unwrap();
        let channel_failure = expected_failure(
            PersistenceOperation::Barrier,
            PersistencePhase::QueueAdmission,
            PersistenceFailureCode::ChannelClosed,
            DurabilityDisposition::NotApplicable,
        );
        assert_persistence_error(channel_writer.barrier().await, channel_failure);
    }

    #[tokio::test]
    async fn i4_writer_replace_owner_failure_names_replace_operation() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        install_temp_failure_trigger(&store, "fail_replace", "INSERT", "owner");
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let expected = expected_failure(
            PersistenceOperation::ReplaceOwner,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::NotCommitted,
        );

        assert_persistence_error(writer.replace_owner(owner_record()).await, expected);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn i4_writer_checkpoint_busy_failure_is_typed_and_data_remains_committed() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let reader = Connection::open_with_flags(
            database_path(&root),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        reader.execute_batch("BEGIN").unwrap();
        let _: i64 = reader
            .query_row("SELECT COUNT(*) FROM workspaces", [], |row| row.get(0))
            .unwrap();
        store
            .apply_batch(vec![workspace_op("checkpointed-later")])
            .unwrap();
        store.connection.busy_timeout(Duration::ZERO).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let expected = expected_failure(
            PersistenceOperation::Checkpoint,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::CheckpointBusy,
            DurabilityDisposition::NotApplicable,
        );

        assert_persistence_error(lifecycle.shutdown().await, expected);
        assert_eq!(
            writer.persistence_status(),
            PersistenceStatus::Degraded { failure: expected }
        );
        reader.execute_batch("ROLLBACK").unwrap();
        drop(reader);

        let reopened = open_reader(&root).unwrap();
        assert!(
            reopened
                .load_restored_state()
                .unwrap()
                .model
                .workspace("checkpointed-later")
                .is_some()
        );
    }

    #[test]
    fn dedup_set_pruned_on_ledger_boundary() {
        let mut cache = EventLedgerCache::from_entries([LedgerEntry {
            event_id: "event".to_owned(),
            seen_at_ms: 10,
        }]);
        cache.apply_cleanup(&[LedgerEntry {
            event_id: "event".to_owned(),
            seen_at_ms: 9,
        }]);
        assert!(cache.contains("event"));

        cache.apply_cleanup(&[LedgerEntry {
            event_id: "event".to_owned(),
            seen_at_ms: 10,
        }]);
        assert!(!cache.contains("event"));
    }

    #[tokio::test]
    async fn dedup_set_updated_by_provider_and_gap_events() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let now = super::super::unix_now_ms().unwrap();

        writer
            .apply(vec![provider_event("provider-id", now)])
            .await
            .unwrap();
        writer
            .apply(vec![PersistOp::RecordCollectorGap(CollectorGap {
                event_id: "gap-id".to_owned(),
                herdr_session: "session".to_owned(),
                seen_at_ms: now,
                kind: GapKind::Reconnect,
            })])
            .await
            .unwrap();

        assert!(writer.is_duplicate("provider-id"));
        assert!(writer.is_duplicate("gap-id"));
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_response_delayed_delivery_still_evicts() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let now = super::super::unix_now_ms().unwrap();
        store
            .apply_batch(vec![provider_event("old-id", now - 8 * DAY_MS)])
            .unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        assert!(writer.is_duplicate("old-id"));

        let (acknowledgement, response) = oneshot::channel();
        writer
            .sender
            .send(WriterCommand::Cleanup {
                now_ms: now,
                acknowledgement,
            })
            .await
            .unwrap();
        let report = response.await.unwrap().unwrap();
        assert!(writer.is_duplicate("old-id"));
        lock_ledger(&writer.ledger).apply_cleanup(&report.deleted_ledger_entries);
        assert!(!writer.is_duplicate("old-id"));
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn lost_cleanup_response_healed_by_restart_reseed() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let now = super::super::unix_now_ms().unwrap();
        store
            .apply_batch(vec![provider_event("lost-report", now - 8 * DAY_MS)])
            .unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();

        let (acknowledgement, response) = oneshot::channel();
        writer
            .sender
            .send(WriterCommand::Cleanup {
                now_ms: now,
                acknowledgement,
            })
            .await
            .unwrap();
        drop(response);
        writer.barrier().await.unwrap();
        assert!(writer.is_duplicate("lost-report"));
        lifecycle.shutdown().await.unwrap();

        let store = open_writer(&root).unwrap();
        let (restarted_lifecycle, restarted) = spawn_writer(store).unwrap();
        assert!(!restarted.is_duplicate("lost-report"));
        restarted_lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn enqueue_permit_absent_returns_retryable_no_change() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        lifecycle.shutdown().await.unwrap();

        assert!(writer.reserve_enqueue().is_none());
        assert!(!writer.is_duplicate("never-admitted"));
    }
}
