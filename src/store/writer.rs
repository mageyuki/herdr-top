//! T9 dedicated writer thread, `WriterClient`, and `WriterLifecycle`.

use std::collections::HashMap;
#[cfg(test)]
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

use crate::lockfile::OwnerRecord;
use crate::model::HistoryDrainId;

use super::{
    CleanupStats, HistoryDrainFinalization, LedgerEntry, PersistBatch, PersistOp, PersistV6Batch,
    Store, StoreError,
};

const WRITER_QUEUE_CAPACITY: usize = 256;
/// Maximum UTF-8 byte length retained for a persistence error detail.
pub const PERSISTENCE_DETAIL_MAX_BYTES: usize = 240;

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

/// UTF-8 persistence detail capped to a fixed byte budget.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(transparent)]
pub struct BoundedDetail(String);

impl BoundedDetail {
    /// Captures the longest valid UTF-8 prefix within the persistence detail budget.
    #[must_use]
    pub fn new(detail: impl Into<String>) -> Self {
        let mut detail = detail.into();
        if detail.len() > PERSISTENCE_DETAIL_MAX_BYTES {
            let mut end = PERSISTENCE_DETAIL_MAX_BYTES;
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            detail.truncate(end);
        }
        Self(detail)
    }

    /// Returns the retained detail text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Current writer health and optional detail for the transition that produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistenceHealthSnapshot {
    pub status: PersistenceStatus,
    pub detail: Option<BoundedDetail>,
}

impl PersistenceHealthSnapshot {
    const fn healthy() -> Self {
        Self {
            status: PersistenceStatus::Healthy,
            detail: None,
        }
    }

    fn degraded(failure: PersistenceFailure, detail: Option<BoundedDetail>) -> Self {
        Self {
            status: PersistenceStatus::Degraded { failure },
            detail,
        }
    }
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

/// Bounded command channel for the single SQLite writer.
///
/// ```
/// # fn consumes(_: herdr_top::store::WriterClient) {}
/// ```
/// ```compile_fail
/// # fn duplicate(client: herdr_top::store::WriterClient) {
/// let second = client.clone();
/// # drop(second);
/// # }
/// ```
pub struct WriterClient {
    sender: mpsc::Sender<WriterCommand>,
    ledger: EventLedgerCache,
    health: PersistenceHealth,
    #[cfg(test)]
    after_second_reserve_health_check: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    acknowledgement_test_control: Option<AcknowledgementTestControl>,
}

#[derive(Clone)]
struct PersistenceHealth {
    sender: watch::Sender<PersistenceHealthSnapshot>,
}

impl PersistenceHealth {
    fn new() -> Self {
        let (sender, _receiver) = watch::channel(PersistenceHealthSnapshot::healthy());
        Self { sender }
    }

    fn status(&self) -> PersistenceStatus {
        self.sender.borrow().status
    }

    #[cfg(test)]
    fn snapshot(&self) -> PersistenceHealthSnapshot {
        self.sender.borrow().clone()
    }

    fn subscribe(&self) -> watch::Receiver<PersistenceHealthSnapshot> {
        self.sender.subscribe()
    }

    fn publish_failure(&self, failure: PersistenceFailure) {
        self.publish_failure_with_detail(failure, None);
    }

    fn publish_failure_with_detail(
        &self,
        failure: PersistenceFailure,
        detail: Option<BoundedDetail>,
    ) {
        self.sender
            .send_if_modified(|snapshot| match snapshot.status {
                PersistenceStatus::Healthy => {
                    *snapshot = PersistenceHealthSnapshot::degraded(failure, detail);
                    true
                }
                PersistenceStatus::Degraded { .. } => false,
            });
    }

    fn publish_probe_failure(&self, detail: BoundedDetail) {
        self.sender.send_if_modified(|snapshot| {
            if !matches!(snapshot.status, PersistenceStatus::Degraded { .. })
                || snapshot.detail.as_ref() == Some(&detail)
            {
                return false;
            }
            snapshot.detail = Some(detail);
            true
        });
    }

    fn publish_recovery(&self) {
        self.sender.send_if_modified(|snapshot| {
            if matches!(snapshot.status, PersistenceStatus::Healthy) {
                return false;
            }
            *snapshot = PersistenceHealthSnapshot::healthy();
            true
        });
    }

    fn runtime_error(&self, failure: PersistenceFailure) -> WriterError {
        self.publish_failure(failure);
        WriterError::Persistence(failure)
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcknowledgementTestEvent {
    WaiterConstructed(PersistenceOperation),
    CommandAdmitted(PersistenceOperation),
    WaiterResolved(PersistenceOperation, PersistenceFailure),
    BeforeStore(PersistenceOperation),
    BeforeAcknowledgement(PersistenceOperation),
    FailurePublished(PersistenceOperation),
    AcknowledgementAttempted(PersistenceOperation),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcknowledgementTestMode {
    Observe,
    BlockBeforeStore,
    BlockBeforeAcknowledgement,
    DropAcknowledgement,
    PauseAfterFailurePublication,
}

#[cfg(test)]
#[derive(Clone)]
struct AcknowledgementTestControl {
    operation: PersistenceOperation,
    mode: AcknowledgementTestMode,
    events: tokio::sync::mpsc::UnboundedSender<AcknowledgementTestEvent>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
}

#[cfg(test)]
impl AcknowledgementTestControl {
    fn record(&self, event: AcknowledgementTestEvent) {
        let _ = self.events.send(event);
    }

    fn waiter_constructed(&self, operation: PersistenceOperation) {
        self.record(AcknowledgementTestEvent::WaiterConstructed(operation));
    }

    fn command_admitted(&self, operation: PersistenceOperation) {
        self.record(AcknowledgementTestEvent::CommandAdmitted(operation));
        if self.operation == operation && self.mode == AcknowledgementTestMode::BlockBeforeStore {
            self.record(AcknowledgementTestEvent::BeforeStore(operation));
            self.wait_for_release();
        }
    }

    fn before_acknowledgement(
        &self,
        operation: PersistenceOperation,
        failure_was_published: bool,
    ) -> bool {
        if self.operation != operation {
            return false;
        }
        match self.mode {
            AcknowledgementTestMode::BlockBeforeAcknowledgement => {
                self.record(AcknowledgementTestEvent::BeforeAcknowledgement(operation));
                self.wait_for_release();
                false
            }
            AcknowledgementTestMode::DropAcknowledgement => true,
            AcknowledgementTestMode::PauseAfterFailurePublication => {
                assert!(
                    failure_was_published,
                    "failure-publication pause requires a failing operation"
                );
                self.record(AcknowledgementTestEvent::FailurePublished(operation));
                self.wait_for_release();
                false
            }
            AcknowledgementTestMode::Observe | AcknowledgementTestMode::BlockBeforeStore => false,
        }
    }

    fn acknowledgement_attempted(&self, operation: PersistenceOperation) {
        self.record(AcknowledgementTestEvent::AcknowledgementAttempted(
            operation,
        ));
    }

    fn acknowledgement_dropped(&self, operation: PersistenceOperation) {
        if self.operation == operation && self.mode == AcknowledgementTestMode::DropAcknowledgement
        {
            self.wait_for_release();
        }
    }

    fn wait_for_release(&self) {
        let (released, condition) = self.release.as_ref();
        let mut released = released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*released {
            released = condition
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

#[cfg(test)]
struct AcknowledgementTestHandle {
    events: tokio::sync::mpsc::UnboundedReceiver<AcknowledgementTestEvent>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
    health: PersistenceHealth,
}

#[cfg(test)]
impl AcknowledgementTestHandle {
    async fn next_event(&mut self) -> AcknowledgementTestEvent {
        tokio::time::timeout(std::time::Duration::from_secs(1), self.events.recv())
            .await
            .expect("test-control event must arrive within one second")
            .expect("test-control event channel must remain open")
    }

    async fn wait_for(&mut self, expected: AcknowledgementTestEvent) {
        loop {
            if self.next_event().await == expected {
                return;
            }
        }
    }

    fn publish_failure(&self, failure: PersistenceFailure) {
        self.health.publish_failure(failure);
    }

    fn release(&self) {
        let (released, condition) = self.release.as_ref();
        *released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        condition.notify_all();
    }
}

#[cfg(test)]
#[derive(Clone)]
struct RawCommandInjector {
    sender: mpsc::Sender<WriterCommand>,
    health: PersistenceHealth,
    acknowledgement_test_control: Option<AcknowledgementTestControl>,
}

#[cfg(test)]
impl RawCommandInjector {
    async fn apply(&self, batch: PersistBatch) -> AcknowledgementWaiter<WriterDelta> {
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = AcknowledgementWaiter::new(
            response,
            self.health.clone(),
            PersistenceOperation::Apply,
            self.acknowledgement_test_control.clone(),
        );
        self.sender
            .send(WriterCommand::Apply {
                batch,
                acknowledgement,
            })
            .await
            .expect("raw test Apply command must be admitted");
        waiter.arm();
        waiter
    }

    async fn barrier(&self) -> AcknowledgementWaiter<()> {
        let (acknowledgement, response) = oneshot::channel();
        let waiter = AcknowledgementWaiter::new(
            response,
            self.health.clone(),
            PersistenceOperation::Barrier,
            self.acknowledgement_test_control.clone(),
        );
        self.sender
            .send(WriterCommand::Barrier { acknowledgement })
            .await
            .expect("raw test Barrier command must be admitted");
        waiter
    }

    async fn cleanup(&self, now_ms: i64) -> AcknowledgementWaiter<WriterDelta> {
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = AcknowledgementWaiter::new(
            response,
            self.health.clone(),
            PersistenceOperation::Cleanup,
            self.acknowledgement_test_control.clone(),
        );
        self.sender
            .send(WriterCommand::Cleanup {
                now_ms,
                acknowledgement,
            })
            .await
            .expect("raw test Cleanup command must be admitted");
        waiter.arm();
        waiter
    }

    async fn closed(&self) {
        self.sender.closed().await;
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
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
        if self.armed {
            self.health
                .publish_failure(acknowledgement_failure(self.operation));
        }
    }
}

struct WriterOperationGuard {
    health: PersistenceHealth,
    operation: PersistenceOperation,
    armed: bool,
}

impl WriterOperationGuard {
    fn new(health: PersistenceHealth, operation: PersistenceOperation) -> Self {
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

impl Drop for WriterOperationGuard {
    fn drop(&mut self) {
        if self.armed {
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

#[derive(Debug)]
struct WriterDelta {
    cleanup: CleanupStats,
}

/// Affine capacity in the bounded writer command channel.
pub struct EnqueuePermit<'a> {
    permit: mpsc::Permit<'a, WriterCommand>,
    ledger: &'a mut EventLedgerCache,
    health: PersistenceHealth,
    #[cfg(test)]
    acknowledgement_test_control: Option<AcknowledgementTestControl>,
}

/// An already-enqueued write whose queue admission can no longer fail.
pub struct PendingEnqueue {
    waiter: AcknowledgementWaiter<WriterDelta>,
}

struct AcknowledgementWaiter<T> {
    response: oneshot::Receiver<Result<T, PersistenceFailure>>,
    health_publisher: PersistenceHealth,
    health: watch::Receiver<PersistenceHealthSnapshot>,
    operation: PersistenceOperation,
    acknowledgement_observation: Option<AcknowledgementObservationGuard>,
    #[cfg(test)]
    acknowledgement_test_control: Option<AcknowledgementTestControl>,
}

impl<T> AcknowledgementWaiter<T> {
    fn new(
        response: oneshot::Receiver<Result<T, PersistenceFailure>>,
        health_publisher: PersistenceHealth,
        operation: PersistenceOperation,
        #[cfg(test)] acknowledgement_test_control: Option<AcknowledgementTestControl>,
    ) -> Self {
        let health = health_publisher.subscribe();
        #[cfg(test)]
        if let Some(control) = &acknowledgement_test_control {
            control.waiter_constructed(operation);
        }
        let acknowledgement_observation = matches!(
            operation,
            PersistenceOperation::Apply
                | PersistenceOperation::Cleanup
                | PersistenceOperation::UpdateOwnerLocation
                | PersistenceOperation::ReplaceOwner
        )
        .then(|| AcknowledgementObservationGuard::new(health_publisher.clone(), operation));
        Self {
            response,
            health_publisher,
            health,
            operation,
            acknowledgement_observation,
            #[cfg(test)]
            acknowledgement_test_control,
        }
    }

    fn arm(&mut self) {
        if let Some(guard) = &mut self.acknowledgement_observation {
            guard.arm();
        }
    }

    fn disarm(&mut self) {
        if let Some(guard) = &mut self.acknowledgement_observation {
            guard.disarm();
        }
    }

    fn resolved_failure(&self, _failure: PersistenceFailure) {
        #[cfg(test)]
        if let Some(control) = &self.acknowledgement_test_control {
            control.record(AcknowledgementTestEvent::WaiterResolved(
                self.operation,
                _failure,
            ));
        }
    }

    fn classify_result(&mut self, result: Result<T, PersistenceFailure>) -> Result<T, WriterError> {
        self.disarm();
        match result {
            Ok(value) => Ok(value),
            Err(failure) => {
                self.resolved_failure(failure);
                Err(WriterError::Persistence(failure))
            }
        }
    }

    fn classify_failure(&mut self, failure: PersistenceFailure) -> Result<T, WriterError> {
        self.disarm();
        self.resolved_failure(failure);
        Err(WriterError::Persistence(failure))
    }

    fn classify_closed(&mut self) -> Result<T, WriterError> {
        self.disarm();
        self.health_publisher
            .publish_failure(acknowledgement_failure(self.operation));
        let failure = match self.health.borrow().status {
            PersistenceStatus::Healthy => acknowledgement_failure(self.operation),
            PersistenceStatus::Degraded { failure } => failure,
        };
        self.resolved_failure(failure);
        Err(WriterError::Persistence(failure))
    }

    async fn wait(mut self) -> Result<T, WriterError> {
        loop {
            match self.response.try_recv() {
                Ok(result) => return self.classify_result(result),
                Err(oneshot::error::TryRecvError::Closed) => return self.classify_closed(),
                Err(oneshot::error::TryRecvError::Empty) => {}
            }

            let status = self.health.borrow().status;
            if let PersistenceStatus::Degraded { failure } = status {
                return self.classify_failure(failure);
            }

            tokio::select! {
                biased;
                response = &mut self.response => {
                    return match response {
                        Ok(result) => self.classify_result(result),
                        Err(_) => self.classify_closed(),
                    };
                }
                changed = self.health.changed() => {
                    if changed.is_err() {
                        return self.classify_closed();
                    }
                }
            }
        }
    }

    async fn wait_response_only(mut self) -> Result<T, WriterError> {
        match (&mut self.response).await {
            Ok(result) => self.classify_result(result),
            Err(_) => {
                self.disarm();
                let failure = acknowledgement_failure(self.operation);
                self.health_publisher.publish_failure(failure);
                self.resolved_failure(failure);
                Err(WriterError::Persistence(failure))
            }
        }
    }
}

impl EnqueuePermit<'_> {
    /// Consumes the permit and enqueues a batch without another fallible channel operation.
    #[must_use]
    pub fn enqueue(self, batch: PersistBatch) -> PendingEnqueue {
        for entry in ledger_entries(&batch) {
            let _ = self.ledger.reserve(entry.event_id, entry.seen_at_ms);
        }
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = AcknowledgementWaiter::new(
            response,
            self.health,
            PersistenceOperation::Apply,
            #[cfg(test)]
            self.acknowledgement_test_control,
        );
        self.permit.send(WriterCommand::Apply {
            batch,
            acknowledgement,
        });
        waiter.arm();
        PendingEnqueue { waiter }
    }

    /// Consumes the permit and enqueues one schema-v6 batch without another channel operation.
    #[must_use]
    pub fn enqueue_v6(self, batch: PersistV6Batch) -> PendingEnqueue {
        for entry in ledger_entries(&batch.operations) {
            let _ = self.ledger.reserve(entry.event_id, entry.seen_at_ms);
        }
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = AcknowledgementWaiter::new(
            response,
            self.health,
            PersistenceOperation::Apply,
            #[cfg(test)]
            self.acknowledgement_test_control,
        );
        self.permit.send(WriterCommand::ApplyV6 {
            batch,
            acknowledgement,
        });
        waiter.arm();
        PendingEnqueue { waiter }
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
    pub fn subscribe_persistence(&self) -> watch::Receiver<PersistenceHealthSnapshot> {
        self.health.subscribe()
    }

    #[cfg(test)]
    pub(crate) fn set_after_second_reserve_health_check_failure(
        &mut self,
        failure: PersistenceFailure,
    ) {
        let health = self.health.clone();
        self.after_second_reserve_health_check = Some(Arc::new(move || {
            health.publish_failure(failure);
        }));
    }

    /// Atomically commits one reducer persistence batch.
    pub async fn apply(&mut self, batch: PersistBatch) -> Result<(), WriterError> {
        let operation = PersistenceOperation::Apply;
        for entry in ledger_entries(&batch) {
            let _ = self.ledger.reserve(entry.event_id, entry.seen_at_ms);
        }
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = self.waiter(response, operation);
        if self
            .sender
            .send(WriterCommand::Apply {
                batch,
                acknowledgement,
            })
            .await
            .is_err()
        {
            return Err(self.health.runtime_error(queue_failure(operation)));
        }
        waiter.arm();
        let delta = waiter.wait().await?;
        self.ledger
            .apply_cleanup(&delta.cleanup.deleted_ledger_entries);
        Ok(())
    }

    /// Atomically commits one core-plus-v6 reducer persistence batch.
    pub async fn apply_v6(&mut self, batch: PersistV6Batch) -> Result<(), WriterError> {
        let operation = PersistenceOperation::Apply;
        for entry in ledger_entries(&batch.operations) {
            let _ = self.ledger.reserve(entry.event_id, entry.seen_at_ms);
        }
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = self.waiter(response, operation);
        if self
            .sender
            .send(WriterCommand::ApplyV6 {
                batch,
                acknowledgement,
            })
            .await
            .is_err()
        {
            return Err(self.health.runtime_error(queue_failure(operation)));
        }
        waiter.arm();
        let delta = waiter.wait().await?;
        self.ledger
            .apply_cleanup(&delta.cleanup.deleted_ledger_entries);
        Ok(())
    }

    /// Non-blockingly reserves one slot in the bounded writer command channel.
    #[must_use]
    pub fn reserve_enqueue(&mut self) -> Option<EnqueuePermit<'_>> {
        if self.persistence_status() != PersistenceStatus::Healthy {
            return None;
        }

        let Self {
            sender,
            ledger,
            health,
            #[cfg(test)]
            after_second_reserve_health_check,
            #[cfg(test)]
            acknowledgement_test_control,
        } = self;
        match sender.try_reserve() {
            Ok(permit) if health.status() == PersistenceStatus::Healthy => {
                #[cfg(test)]
                if let Some(hook) = after_second_reserve_health_check {
                    hook();
                }
                Some(EnqueuePermit {
                    permit,
                    ledger,
                    health: health.clone(),
                    #[cfg(test)]
                    acknowledgement_test_control: acknowledgement_test_control.clone(),
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

    /// Waits for an already-admitted batch and applies its cleanup result.
    pub async fn finish_pending(
        &mut self,
        pending: PendingEnqueue,
    ) -> Result<CleanupStats, WriterError> {
        let delta = pending.waiter.wait().await?;
        self.ledger
            .apply_cleanup(&delta.cleanup.deleted_ledger_entries);
        Ok(delta.cleanup)
    }

    /// Returns whether the durable-ledger mirror already contains `event_id`.
    #[must_use]
    pub fn is_duplicate(&self, event_id: &str) -> bool {
        self.ledger.contains(event_id)
    }

    /// Drives a periodic retention pass and conditionally evicts its exact deleted rows.
    pub async fn cleanup(&mut self, now_ms: i64) -> Result<CleanupStats, WriterError> {
        let operation = PersistenceOperation::Cleanup;
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = self.waiter(response, operation);
        if self
            .sender
            .send(WriterCommand::Cleanup {
                now_ms,
                acknowledgement,
            })
            .await
            .is_err()
        {
            return Err(self.health.runtime_error(queue_failure(operation)));
        }
        waiter.arm();
        let delta = waiter.wait().await?;
        self.ledger
            .apply_cleanup(&delta.cleanup.deleted_ledger_entries);
        Ok(delta.cleanup)
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
    pub async fn replace_owner(&mut self, rec: OwnerRecord) -> Result<(), WriterError> {
        self.request_mutating(PersistenceOperation::ReplaceOwner, |acknowledgement| {
            WriterCommand::ReplaceOwner {
                record: rec,
                acknowledgement,
            }
        })
        .await
    }

    /// Atomically finalizes one history drain on the dedicated writer thread.
    pub async fn finalize_history_drain(
        &mut self,
        drain_id: HistoryDrainId,
        observed_at_ms: i64,
    ) -> Result<HistoryDrainFinalization, WriterError> {
        let operation = PersistenceOperation::Apply;
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = self.waiter(response, operation);
        if self
            .sender
            .send(WriterCommand::FinalizeHistoryDrain {
                drain_id,
                observed_at_ms,
                acknowledgement,
            })
            .await
            .is_err()
        {
            return Err(self.health.runtime_error(queue_failure(operation)));
        }
        waiter.arm();
        waiter.wait().await
    }

    /// Reads completion identity directly, including while persistence health is degraded.
    pub async fn history_drain_finalized(
        &self,
        drain_id: &HistoryDrainId,
    ) -> Result<bool, WriterError> {
        let operation = PersistenceOperation::Barrier;
        let (acknowledgement, response) = oneshot::channel();
        let waiter = self.waiter(response, operation);
        if self
            .sender
            .send(WriterCommand::HistoryDrainFinalized {
                drain_id: drain_id.clone(),
                acknowledgement,
            })
            .await
            .is_err()
        {
            return Err(self.health.runtime_error(queue_failure(operation)));
        }
        waiter.wait_response_only().await
    }

    /// Acknowledges after every command queued before this call has completed.
    pub async fn barrier(&self) -> Result<(), WriterError> {
        self.request(PersistenceOperation::Barrier, |acknowledgement| {
            WriterCommand::Barrier { acknowledgement }
        })
        .await
    }

    pub(crate) async fn probe(&self) -> Result<(), WriterError> {
        let operation = PersistenceOperation::ReplaceOwner;
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = self.waiter(response, operation);
        if self
            .sender
            .send(WriterCommand::Probe { acknowledgement })
            .await
            .is_err()
        {
            return Err(self.health.runtime_error(queue_failure(operation)));
        }
        waiter.arm();
        waiter.wait_response_only().await
    }

    async fn request(
        &self,
        operation: PersistenceOperation,
        command: impl FnOnce(oneshot::Sender<Result<(), PersistenceFailure>>) -> WriterCommand,
    ) -> Result<(), WriterError> {
        let (acknowledgement, response) = oneshot::channel();
        let waiter = self.waiter(response, operation);
        if self.sender.send(command(acknowledgement)).await.is_err() {
            return Err(self.health.runtime_error(queue_failure(operation)));
        }
        waiter.wait().await
    }

    async fn request_mutating(
        &self,
        operation: PersistenceOperation,
        command: impl FnOnce(oneshot::Sender<Result<(), PersistenceFailure>>) -> WriterCommand,
    ) -> Result<(), WriterError> {
        let (acknowledgement, response) = oneshot::channel();
        let mut waiter = self.waiter(response, operation);
        if self.sender.send(command(acknowledgement)).await.is_err() {
            return Err(self.health.runtime_error(queue_failure(operation)));
        }
        waiter.arm();
        waiter.wait().await
    }

    fn waiter<T>(
        &self,
        response: oneshot::Receiver<Result<T, PersistenceFailure>>,
        operation: PersistenceOperation,
    ) -> AcknowledgementWaiter<T> {
        AcknowledgementWaiter::new(
            response,
            self.health.clone(),
            operation,
            #[cfg(test)]
            self.acknowledgement_test_control.clone(),
        )
    }
}

/// Unique lifecycle owner for the dedicated writer thread.
pub struct WriterLifecycle {
    sender: mpsc::Sender<WriterCommand>,
    thread: Option<JoinHandle<()>>,
    health: PersistenceHealth,
    #[cfg(test)]
    acknowledgement_test_control: Option<AcknowledgementTestControl>,
}

#[cfg(test)]
#[must_use]
pub(crate) struct WriterCapacityGuard<'a> {
    _permits: Vec<mpsc::Permit<'a, WriterCommand>>,
}

impl WriterLifecycle {
    #[cfg(test)]
    pub(crate) async fn hold_queue_capacity_for_test(&self) -> WriterCapacityGuard<'_> {
        let mut permits = Vec::with_capacity(WRITER_QUEUE_CAPACITY);
        for _ in 0..WRITER_QUEUE_CAPACITY {
            permits.push(
                self.sender
                    .reserve()
                    .await
                    .expect("test capacity guard requires a live writer queue"),
            );
        }
        WriterCapacityGuard { _permits: permits }
    }

    /// Drains queued commands, checkpoints the WAL, and joins the OS thread.
    pub async fn shutdown(mut self) -> Result<(), WriterError> {
        let (acknowledgement, response) = oneshot::channel();
        let waiter = AcknowledgementWaiter::new(
            response,
            self.health.clone(),
            PersistenceOperation::Checkpoint,
            #[cfg(test)]
            self.acknowledgement_test_control.clone(),
        );
        let send_result = self
            .sender
            .send(WriterCommand::Shutdown { acknowledgement })
            .await;
        drop(self.sender);

        let operation_result = match send_result {
            Ok(()) => waiter.wait_response_only().await,
            Err(_) => {
                let failure = queue_failure(PersistenceOperation::Checkpoint);
                self.health.publish_failure(failure);
                Err(WriterError::Persistence(failure))
            }
        };
        let join_result = self
            .thread
            .take()
            .ok_or(WriterError::ThreadPanicked)?
            .join()
            .map_err(|_| WriterError::ThreadPanicked);

        join_result?;
        operation_result
    }
}

/// Starts one dedicated OS thread that exclusively owns the supplied store.
pub fn spawn_writer(store: Store) -> Result<(WriterLifecycle, WriterClient), WriterError> {
    spawn_writer_inner(
        store,
        super::unix_now_ms,
        #[cfg(test)]
        None,
    )
}

#[cfg(test)]
fn spawn_writer_with_clock(
    store: Store,
    clock: fn() -> Result<i64, StoreError>,
) -> Result<(WriterLifecycle, WriterClient), WriterError> {
    spawn_writer_inner(store, clock, None)
}

#[cfg(test)]
fn spawn_writer_with_test_control(
    store: Store,
    clock: fn() -> Result<i64, StoreError>,
    operation: PersistenceOperation,
    mode: AcknowledgementTestMode,
) -> Result<
    (
        WriterLifecycle,
        WriterClient,
        AcknowledgementTestHandle,
        RawCommandInjector,
    ),
    WriterError,
> {
    let (events, event_receiver) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let control = AcknowledgementTestControl {
        operation,
        mode,
        events,
        release: Arc::clone(&release),
    };
    let (lifecycle, writer) = spawn_writer_inner(store, clock, Some(control))?;
    let handle = AcknowledgementTestHandle {
        events: event_receiver,
        release,
        health: writer.health.clone(),
    };
    let injector = RawCommandInjector {
        sender: writer.sender.clone(),
        health: writer.health.clone(),
        acknowledgement_test_control: writer.acknowledgement_test_control.clone(),
    };
    Ok((lifecycle, writer, handle, injector))
}

#[cfg(test)]
pub(super) fn spawn_writer_with_dropped_apply_ack(
    store: Store,
) -> Result<(WriterLifecycle, WriterClient), WriterError> {
    let (lifecycle, writer, handle, _injector) = spawn_writer_with_test_control(
        store,
        super::unix_now_ms,
        PersistenceOperation::Apply,
        AcknowledgementTestMode::DropAcknowledgement,
    )?;
    handle.release();
    Ok((lifecycle, writer))
}

fn spawn_writer_inner(
    store: Store,
    clock: fn() -> Result<i64, StoreError>,
    #[cfg(test)] acknowledgement_test_control: Option<AcknowledgementTestControl>,
) -> Result<(WriterLifecycle, WriterClient), WriterError> {
    let ledger = EventLedgerCache::from_entries(store.load_event_ledger()?);
    let (sender, receiver) = mpsc::channel(WRITER_QUEUE_CAPACITY);
    let health = PersistenceHealth::new();
    let writer_health = health.clone();
    #[cfg(test)]
    let worker_control = acknowledgement_test_control.clone();
    #[cfg(test)]
    let worker = move || {
        writer_main(store, receiver, writer_health, clock, worker_control);
    };
    #[cfg(not(test))]
    let worker = move || writer_main(store, receiver, writer_health, clock);
    let thread = thread::Builder::new()
        .name("herdr-top-sqlite-writer".to_owned())
        .spawn(worker)
        .map_err(WriterError::ThreadSpawn)?;
    let client = WriterClient {
        sender: sender.clone(),
        ledger,
        health: health.clone(),
        #[cfg(test)]
        after_second_reserve_health_check: None,
        #[cfg(test)]
        acknowledgement_test_control: acknowledgement_test_control.clone(),
    };
    let lifecycle = WriterLifecycle {
        sender,
        thread: Some(thread),
        health,
        #[cfg(test)]
        acknowledgement_test_control,
    };
    Ok((lifecycle, client))
}

enum WriterCommand {
    Apply {
        batch: PersistBatch,
        acknowledgement: oneshot::Sender<Result<WriterDelta, PersistenceFailure>>,
    },
    ApplyV6 {
        batch: PersistV6Batch,
        acknowledgement: oneshot::Sender<Result<WriterDelta, PersistenceFailure>>,
    },
    Cleanup {
        now_ms: i64,
        acknowledgement: oneshot::Sender<Result<WriterDelta, PersistenceFailure>>,
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
    FinalizeHistoryDrain {
        drain_id: HistoryDrainId,
        observed_at_ms: i64,
        acknowledgement: oneshot::Sender<Result<HistoryDrainFinalization, PersistenceFailure>>,
    },
    HistoryDrainFinalized {
        drain_id: HistoryDrainId,
        acknowledgement: oneshot::Sender<Result<bool, PersistenceFailure>>,
    },
    Barrier {
        acknowledgement: oneshot::Sender<Result<(), PersistenceFailure>>,
    },
    Probe {
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
    clock: fn() -> Result<i64, StoreError>,
    #[cfg(test)] acknowledgement_test_control: Option<AcknowledgementTestControl>,
) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            WriterCommand::Apply {
                batch,
                acknowledgement,
            } => {
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.command_admitted(PersistenceOperation::Apply);
                }
                let mut operation_guard =
                    WriterOperationGuard::new(health.clone(), PersistenceOperation::Apply);
                operation_guard.arm();
                let result = match store.apply_batch(batch) {
                    Ok(()) => clock()
                        .and_then(|now_ms| store.cleanup_retention(now_ms))
                        .map(|cleanup| WriterDelta { cleanup })
                        .map_err(|error| {
                            store_failure(
                                PersistenceOperation::Cleanup,
                                PersistencePhase::PostApplyCommit,
                                DurabilityDisposition::Committed,
                                &error,
                            )
                        }),
                    Err(error) => Err(store_failure(
                        PersistenceOperation::Apply,
                        PersistencePhase::CommandExecution,
                        DurabilityDisposition::NotCommitted,
                        &error,
                    )),
                };
                let result = publish_store_result(result, &health);
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control
                    && control.before_acknowledgement(PersistenceOperation::Apply, result.is_err())
                {
                    drop(acknowledgement);
                    control.acknowledgement_dropped(PersistenceOperation::Apply);
                    operation_guard.disarm();
                    continue;
                }
                let _ = acknowledgement.send(result);
                operation_guard.disarm();
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.acknowledgement_attempted(PersistenceOperation::Apply);
                }
            }
            WriterCommand::ApplyV6 {
                batch,
                acknowledgement,
            } => {
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.command_admitted(PersistenceOperation::Apply);
                }
                let mut operation_guard =
                    WriterOperationGuard::new(health.clone(), PersistenceOperation::Apply);
                operation_guard.arm();
                let result = match store.apply_v6_batch(batch) {
                    Ok(()) => clock()
                        .and_then(|now_ms| store.cleanup_retention(now_ms))
                        .map(|cleanup| WriterDelta { cleanup })
                        .map_err(|error| {
                            store_failure(
                                PersistenceOperation::Cleanup,
                                PersistencePhase::PostApplyCommit,
                                DurabilityDisposition::Committed,
                                &error,
                            )
                        }),
                    Err(error) => Err(store_failure(
                        PersistenceOperation::Apply,
                        PersistencePhase::CommandExecution,
                        DurabilityDisposition::NotCommitted,
                        &error,
                    )),
                };
                let result = publish_store_result(result, &health);
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control
                    && control.before_acknowledgement(PersistenceOperation::Apply, result.is_err())
                {
                    drop(acknowledgement);
                    control.acknowledgement_dropped(PersistenceOperation::Apply);
                    operation_guard.disarm();
                    continue;
                }
                let _ = acknowledgement.send(result);
                operation_guard.disarm();
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.acknowledgement_attempted(PersistenceOperation::Apply);
                }
            }
            WriterCommand::Cleanup {
                now_ms,
                acknowledgement,
            } => {
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.command_admitted(PersistenceOperation::Cleanup);
                }
                let mut operation_guard =
                    WriterOperationGuard::new(health.clone(), PersistenceOperation::Cleanup);
                operation_guard.arm();
                let result = store
                    .cleanup_retention(now_ms)
                    .map(|cleanup| WriterDelta { cleanup })
                    .map_err(|error| {
                        store_failure(
                            PersistenceOperation::Cleanup,
                            PersistencePhase::CommandExecution,
                            DurabilityDisposition::NotCommitted,
                            &error,
                        )
                    });
                let result = publish_store_result(result, &health);
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control
                    && control
                        .before_acknowledgement(PersistenceOperation::Cleanup, result.is_err())
                {
                    drop(acknowledgement);
                    control.acknowledgement_dropped(PersistenceOperation::Cleanup);
                    operation_guard.disarm();
                    continue;
                }
                let _ = acknowledgement.send(result);
                operation_guard.disarm();
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.acknowledgement_attempted(PersistenceOperation::Cleanup);
                }
            }
            WriterCommand::UpdateOwnerLocation {
                terminal_id,
                pane_id,
                acknowledgement,
            } => {
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.command_admitted(PersistenceOperation::UpdateOwnerLocation);
                }
                let mut operation_guard = WriterOperationGuard::new(
                    health.clone(),
                    PersistenceOperation::UpdateOwnerLocation,
                );
                operation_guard.arm();
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
                let result = publish_store_result(result, &health);
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control
                    && control.before_acknowledgement(
                        PersistenceOperation::UpdateOwnerLocation,
                        result.is_err(),
                    )
                {
                    drop(acknowledgement);
                    control.acknowledgement_dropped(PersistenceOperation::UpdateOwnerLocation);
                    operation_guard.disarm();
                    continue;
                }
                let _ = acknowledgement.send(result);
                operation_guard.disarm();
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.acknowledgement_attempted(PersistenceOperation::UpdateOwnerLocation);
                }
            }
            WriterCommand::ReplaceOwner {
                record,
                acknowledgement,
            } => {
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.command_admitted(PersistenceOperation::ReplaceOwner);
                }
                let mut operation_guard =
                    WriterOperationGuard::new(health.clone(), PersistenceOperation::ReplaceOwner);
                operation_guard.arm();
                let result = store.replace_owner(&record).map_err(|error| {
                    store_failure(
                        PersistenceOperation::ReplaceOwner,
                        PersistencePhase::CommandExecution,
                        DurabilityDisposition::NotCommitted,
                        &error,
                    )
                });
                let result = publish_store_result(result, &health);
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control
                    && control
                        .before_acknowledgement(PersistenceOperation::ReplaceOwner, result.is_err())
                {
                    drop(acknowledgement);
                    control.acknowledgement_dropped(PersistenceOperation::ReplaceOwner);
                    operation_guard.disarm();
                    continue;
                }
                let _ = acknowledgement.send(result);
                operation_guard.disarm();
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.acknowledgement_attempted(PersistenceOperation::ReplaceOwner);
                }
            }
            WriterCommand::FinalizeHistoryDrain {
                drain_id,
                observed_at_ms,
                acknowledgement,
            } => {
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.command_admitted(PersistenceOperation::Apply);
                }
                let mut operation_guard =
                    WriterOperationGuard::new(health.clone(), PersistenceOperation::Apply);
                operation_guard.arm();
                let result = store
                    .finalize_history_drain(&drain_id, observed_at_ms)
                    .map_err(|error| {
                        store_failure(
                            PersistenceOperation::Apply,
                            PersistencePhase::CommandExecution,
                            DurabilityDisposition::NotCommitted,
                            &error,
                        )
                    });
                let result = publish_store_result(result, &health);
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control
                    && control.before_acknowledgement(PersistenceOperation::Apply, result.is_err())
                {
                    drop(acknowledgement);
                    control.acknowledgement_dropped(PersistenceOperation::Apply);
                    operation_guard.disarm();
                    continue;
                }
                let _ = acknowledgement.send(result);
                operation_guard.disarm();
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.acknowledgement_attempted(PersistenceOperation::Apply);
                }
            }
            WriterCommand::HistoryDrainFinalized {
                drain_id,
                acknowledgement,
            } => {
                let result = store.history_drain_finalized(&drain_id).map_err(|error| {
                    store_failure(
                        PersistenceOperation::Barrier,
                        PersistencePhase::CommandExecution,
                        DurabilityDisposition::NotApplicable,
                        &error,
                    )
                });
                let _ = acknowledgement.send(result.map_err(|failure| failure.failure));
            }
            WriterCommand::Barrier { acknowledgement } => {
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.command_admitted(PersistenceOperation::Barrier);
                    if control.before_acknowledgement(PersistenceOperation::Barrier, false) {
                        drop(acknowledgement);
                        control.acknowledgement_dropped(PersistenceOperation::Barrier);
                        continue;
                    }
                }
                let _ = acknowledgement.send(Ok(()));
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.acknowledgement_attempted(PersistenceOperation::Barrier);
                }
            }
            WriterCommand::Probe { acknowledgement } => {
                let result = store
                    .read_owner()
                    .and_then(|owner| owner.ok_or(StoreError::OwnerAbsent))
                    .and_then(|owner| store.replace_owner(&owner))
                    .map_err(|error| {
                        store_failure(
                            PersistenceOperation::ReplaceOwner,
                            PersistencePhase::CommandExecution,
                            DurabilityDisposition::NotCommitted,
                            &error,
                        )
                    });
                let result = match result {
                    Ok(()) => {
                        health.publish_recovery();
                        Ok(())
                    }
                    Err(store_failure) => {
                        health.publish_probe_failure(store_failure.detail);
                        Err(store_failure.failure)
                    }
                };
                let _ = acknowledgement.send(result);
            }
            WriterCommand::Shutdown { acknowledgement } => {
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.command_admitted(PersistenceOperation::Checkpoint);
                }
                let mut operation_guard =
                    WriterOperationGuard::new(health.clone(), PersistenceOperation::Checkpoint);
                operation_guard.arm();
                let result = store.checkpoint().map_err(|error| {
                    store_failure(
                        PersistenceOperation::Checkpoint,
                        PersistencePhase::CommandExecution,
                        DurabilityDisposition::NotApplicable,
                        &error,
                    )
                });
                let result = publish_store_result(result, &health);
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control
                    && control
                        .before_acknowledgement(PersistenceOperation::Checkpoint, result.is_err())
                {
                    drop(acknowledgement);
                    control.acknowledgement_dropped(PersistenceOperation::Checkpoint);
                    operation_guard.disarm();
                    break;
                }
                let _ = acknowledgement.send(result);
                operation_guard.disarm();
                #[cfg(test)]
                if let Some(control) = &acknowledgement_test_control {
                    control.acknowledgement_attempted(PersistenceOperation::Checkpoint);
                }
                break;
            }
        }
    }
}

fn publish_store_result<T>(
    result: Result<T, StoreFailure>,
    health: &PersistenceHealth,
) -> Result<T, PersistenceFailure> {
    result.map_err(|store_failure| {
        health
            .publish_failure_with_detail(store_failure.failure, Some(store_failure.detail.clone()));
        store_failure.failure
    })
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
) -> StoreFailure {
    StoreFailure {
        failure: persistence_failure(operation, phase, classify_store_error(error), durability),
        detail: BoundedDetail::new(error.to_string()),
    }
}

struct StoreFailure {
    failure: PersistenceFailure,
    detail: BoundedDetail,
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
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use rusqlite::{Connection, OpenFlags};
    use serde_json::json;

    use crate::lockfile::{OwnerRecord, StateRoot};
    use crate::model::{
        EventMetadata, GapKind, NormalizedEvent, TopologyAuthority, TopologyEntity, Workspace,
    };
    use crate::store::{CollectorGap, PersistOp, database_path, open_reader, open_writer};

    use super::*;

    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
    const TEST_RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(1);

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
                authority: TopologyAuthority::Partial,
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

    fn install_failure_trigger(store: &Store, name: &str, action: &str, table: &str) {
        store
            .connection
            .execute_batch(&format!(
                "CREATE TRIGGER {name} BEFORE {action} ON {table} \
                 BEGIN SELECT RAISE(ABORT, 'injected probe failure'); END;"
            ))
            .unwrap();
    }

    #[test]
    fn i4_writer_store_failure_detail_is_utf8_bounded_and_synthesized_failures_have_none() {
        let raw = format!("clock detail {}", "界".repeat(100));
        let error = StoreError::Clock(raw);
        let displayed = error.to_string();
        let captured = store_failure(
            PersistenceOperation::Apply,
            PersistencePhase::CommandExecution,
            DurabilityDisposition::NotCommitted,
            &error,
        );

        assert_eq!(captured.failure.code, PersistenceFailureCode::Clock);
        let detail = &captured.detail;
        assert!(detail.as_str().len() <= PERSISTENCE_DETAIL_MAX_BYTES);
        assert!(displayed.starts_with(detail.as_str()));
        assert!(std::str::from_utf8(detail.as_str().as_bytes()).is_ok());

        let health = PersistenceHealth::new();
        health.publish_failure(queue_failure(PersistenceOperation::Apply));
        assert_eq!(health.snapshot().detail, None);
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

    fn open_operation_store(root: &StateRoot) -> Store {
        let mut store = open_writer(root).unwrap();
        store.replace_owner(&owner_record()).unwrap();
        store
    }

    async fn execute_operation(
        mut writer: WriterClient,
        operation: PersistenceOperation,
    ) -> (WriterClient, Result<(), WriterError>) {
        let result = match operation {
            PersistenceOperation::Apply => writer.apply(Vec::new()).await,
            PersistenceOperation::Cleanup => writer.cleanup(i64::MAX).await.map(|_| ()),
            PersistenceOperation::UpdateOwnerLocation => {
                writer
                    .update_owner_location("updated-terminal", "updated-pane")
                    .await
            }
            PersistenceOperation::ReplaceOwner => writer.replace_owner(owner_record()).await,
            PersistenceOperation::Barrier => writer.barrier().await,
            PersistenceOperation::Checkpoint => {
                unreachable!("Checkpoint is owned by WriterLifecycle")
            }
        };
        (writer, result)
    }

    async fn assert_waiter_constructed_before_admission(
        handle: &mut AcknowledgementTestHandle,
        operation: PersistenceOperation,
    ) {
        assert_eq!(
            handle.next_event().await,
            AcknowledgementTestEvent::WaiterConstructed(operation)
        );
        assert_eq!(
            handle.next_event().await,
            AcknowledgementTestEvent::CommandAdmitted(operation)
        );
    }

    #[tokio::test]
    async fn i4_writer_initial_status_is_healthy_and_subscription_is_read_only() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();

        assert_eq!(writer.persistence_status(), PersistenceStatus::Healthy);
        let subscription = writer.subscribe_persistence();
        assert_eq!(subscription.borrow().status, PersistenceStatus::Healthy);
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
    async fn i4_writer_first_failure_is_sticky_until_probe_and_each_recovery_wakes_once() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        store.replace_owner(&owner_record()).unwrap();
        install_failure_trigger(&store, "fail_owner_update", "UPDATE", "owner");
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut subscription = writer.subscribe_persistence();
        let first = expected_failure(
            PersistenceOperation::UpdateOwnerLocation,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::NotCommitted,
        );

        assert_persistence_error(
            writer.update_owner_location("terminal", "pane").await,
            first,
        );
        subscription.changed().await.unwrap();
        assert_eq!(
            subscription.borrow_and_update().status,
            PersistenceStatus::Degraded { failure: first }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), subscription.changed())
                .await
                .is_err(),
            "degradation must not recover without a probe"
        );

        rusqlite::Connection::open(super::super::database_path(&root))
            .unwrap()
            .execute_batch("DROP TRIGGER fail_owner_update;")
            .unwrap();
        writer.probe().await.unwrap();
        subscription.changed().await.unwrap();
        assert_eq!(
            subscription.borrow_and_update().status,
            PersistenceStatus::Healthy
        );
        writer.probe().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), subscription.changed())
                .await
                .is_err(),
            "a successful probe while healthy must not publish another transition"
        );

        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn i4_writer_failed_probe_returns_failure_without_publishing_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        store.replace_owner(&owner_record()).unwrap();
        install_failure_trigger(&store, "fail_owner_probe", "UPDATE", "owner");
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut subscription = writer.subscribe_persistence();
        let expected = expected_failure(
            PersistenceOperation::UpdateOwnerLocation,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::NotCommitted,
        );

        assert_persistence_error(
            writer.update_owner_location("terminal", "pane").await,
            expected,
        );
        subscription.changed().await.unwrap();
        let _ = subscription.borrow_and_update();
        rusqlite::Connection::open(super::super::database_path(&root))
            .unwrap()
            .execute_batch(
                "DROP TRIGGER fail_owner_probe; \
                 CREATE TRIGGER fail_owner_probe_refresh BEFORE UPDATE ON owner \
                 BEGIN SELECT RAISE(ABORT, 'refreshed probe failure'); END;",
            )
            .unwrap();
        assert_persistence_error(
            writer.probe().await,
            expected_failure(
                PersistenceOperation::ReplaceOwner,
                PersistencePhase::CommandExecution,
                PersistenceFailureCode::Sqlite,
                DurabilityDisposition::NotCommitted,
            ),
        );
        subscription.changed().await.unwrap();
        let health = subscription.borrow_and_update().clone();
        assert_eq!(
            health.status,
            PersistenceStatus::Degraded { failure: expected }
        );
        assert!(
            health
                .detail
                .as_ref()
                .unwrap()
                .as_str()
                .contains("refreshed probe failure")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), subscription.changed())
                .await
                .is_err(),
            "a failed probe must not publish recovery after refreshing detail"
        );

        rusqlite::Connection::open(super::super::database_path(&root))
            .unwrap()
            .execute_batch("DROP TRIGGER fail_owner_probe_refresh;")
            .unwrap();
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn i4_writer_apply_commit_failure_is_not_committed() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        install_temp_failure_trigger(&store, "fail_apply", "INSERT", "workspaces");
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
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
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
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
        let (lifecycle, mut writer) = spawn_writer_with_clock(store, || {
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

        assert_persistence_error(writer.finish_pending(pending).await, expected);
        assert_eq!(
            writer.persistence_status(),
            PersistenceStatus::Degraded { failure: expected }
        );
        drop(lifecycle);
    }

    #[tokio::test]
    async fn writer_client_finish_pending_preserves_pending_wait_behavior() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let now = super::super::unix_now_ms().unwrap();
        store
            .apply_batch(vec![provider_event("finish-old", now - 8 * DAY_MS)])
            .unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let pending = writer
            .reserve_enqueue()
            .unwrap()
            .enqueue(vec![provider_event("finish-new", now)]);

        let cleanup = writer.finish_pending(pending).await.unwrap();
        assert_eq!(cleanup.ledger_pruned, 1);
        assert_eq!(
            cleanup.deleted_ledger_entries,
            [LedgerEntry {
                event_id: "finish-old".to_owned(),
                seen_at_ms: now - 8 * DAY_MS,
            }]
        );
        assert!(!writer.is_duplicate("finish-old"));
        assert!(writer.is_duplicate("finish-new"));
        assert_eq!(writer.persistence_status(), PersistenceStatus::Healthy);
        lifecycle.shutdown().await.unwrap();

        let error_directory = tempfile::tempdir().unwrap();
        let error_root = StateRoot(error_directory.path().to_path_buf());
        let error_store = open_writer(&error_root).unwrap();
        install_temp_failure_trigger(&error_store, "fail_pending", "INSERT", "events");
        let (error_lifecycle, mut error_writer) = spawn_writer(error_store).unwrap();
        let error_pending = error_writer
            .reserve_enqueue()
            .unwrap()
            .enqueue(vec![provider_event("finish-error", now)]);
        let expected = expected_failure(
            PersistenceOperation::Apply,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::NotCommitted,
        );

        assert_persistence_error(error_writer.finish_pending(error_pending).await, expected);
        assert!(error_writer.is_duplicate("finish-error"));
        error_lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn i4_writer_late_drop_of_buffered_apply_receipt_publishes_unknown_once() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
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
            subscription.borrow_and_update().status,
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
        let (lifecycle, writer, mut handle, injector) = spawn_writer_with_test_control(
            store,
            super::super::unix_now_ms,
            PersistenceOperation::Apply,
            AcknowledgementTestMode::BlockBeforeStore,
        )
        .unwrap();
        let apply_waiter = injector.apply(Vec::new()).await;
        handle
            .wait_for(AcknowledgementTestEvent::BeforeStore(
                PersistenceOperation::Apply,
            ))
            .await;
        let abandoned_barrier = injector.barrier().await;
        drop(abandoned_barrier);
        handle.release();

        apply_waiter.wait().await.unwrap();
        writer.barrier().await.unwrap();
        assert_eq!(writer.persistence_status(), PersistenceStatus::Healthy);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unread_pending_receipt_dropped_during_owner_unwind_degrades_once() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let mut health = writer.subscribe_persistence();
        let health_keepalive = lifecycle.health.clone();
        let now = super::super::unix_now_ms().unwrap();
        let expected = expected_failure(
            PersistenceOperation::Apply,
            PersistencePhase::Acknowledgement,
            PersistenceFailureCode::AcknowledgementDropped,
            DurabilityDisposition::Unknown,
        );
        let task = tokio::spawn(async move {
            let _armed_pending = writer
                .reserve_enqueue()
                .unwrap()
                .enqueue(vec![provider_event("panic-unwind", now)]);
            panic!("exercise armed receipt unwind");
        });

        let join_error = task.await.unwrap_err();
        assert!(join_error.is_panic());
        let observed = tokio::time::timeout(TEST_RENDEZVOUS_TIMEOUT, health.changed()).await;
        lifecycle.shutdown().await.unwrap();

        observed
            .expect("owner unwind must publish acknowledgement failure")
            .unwrap();
        assert_eq!(
            health.borrow_and_update().status,
            PersistenceStatus::Degraded { failure: expected }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), health.changed())
                .await
                .is_err(),
            "first-wins health must change exactly once"
        );
        drop(health_keepalive);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acknowledgement_waiter_covers_all_six_operations() {
        let operations = [
            PersistenceOperation::Apply,
            PersistenceOperation::Cleanup,
            PersistenceOperation::UpdateOwnerLocation,
            PersistenceOperation::ReplaceOwner,
            PersistenceOperation::Barrier,
        ];
        let published = expected_failure(
            PersistenceOperation::Apply,
            PersistencePhase::Acknowledgement,
            PersistenceFailureCode::AcknowledgementDropped,
            DurabilityDisposition::Unknown,
        );

        for operation in operations {
            let directory = tempfile::tempdir().unwrap();
            let root = StateRoot(directory.path().to_path_buf());
            let store = open_operation_store(&root);
            let (lifecycle, writer, mut handle, _injector) = spawn_writer_with_test_control(
                store,
                super::super::unix_now_ms,
                operation,
                AcknowledgementTestMode::BlockBeforeAcknowledgement,
            )
            .unwrap();
            let mut request = tokio::spawn(execute_operation(writer, operation));

            assert_waiter_constructed_before_admission(&mut handle, operation).await;
            handle
                .wait_for(AcknowledgementTestEvent::BeforeAcknowledgement(operation))
                .await;
            handle.publish_failure(published);
            let early = tokio::time::timeout(TEST_RENDEZVOUS_TIMEOUT, &mut request).await;
            handle.release();
            let (writer, result, returned_before_release) = match early {
                Ok(joined) => {
                    let (writer, result) = joined.unwrap();
                    (writer, result, true)
                }
                Err(_) => {
                    let (writer, result) = request.await.unwrap();
                    (writer, result, false)
                }
            };
            lifecycle.shutdown().await.unwrap();
            drop(writer);

            assert!(
                returned_before_release,
                "{operation:?} waiter must resolve from health while its sender is live"
            );
            assert_persistence_error(result, published);
            assert_eq!(
                handle.health.status(),
                PersistenceStatus::Degraded { failure: published }
            );
        }

        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_operation_store(&root);
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        writer.health.publish_failure(published);
        let (acknowledgement, response) = oneshot::channel();
        let waiter = AcknowledgementWaiter::new(
            response,
            writer.health.clone(),
            PersistenceOperation::Checkpoint,
            None,
        );
        let mut response_only = Box::pin(waiter.wait_response_only());
        let mut context = Context::from_waker(Waker::noop());
        let initial_poll = response_only.as_mut().poll(&mut context);
        let _ = acknowledgement.send(Ok(()));

        assert!(
            matches!(initial_poll, Poll::Pending),
            "Checkpoint waiter must ignore sticky health while its response is empty"
        );
        response_only.await.unwrap();
        assert_eq!(
            writer.persistence_status(),
            PersistenceStatus::Degraded { failure: published }
        );
        lifecycle.shutdown().await.unwrap();

        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_operation_store(&root);
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        assert_eq!(writer.persistence_status(), PersistenceStatus::Healthy);
        let (acknowledgement, response) = oneshot::channel();
        let waiter = AcknowledgementWaiter::new(
            response,
            writer.health.clone(),
            PersistenceOperation::Checkpoint,
            None,
        );
        writer.health.publish_failure(published);
        let mut response_only = Box::pin(waiter.wait_response_only());
        let mut context = Context::from_waker(Waker::noop());
        let health_change_poll = response_only.as_mut().poll(&mut context);
        let _ = acknowledgement.send(Ok(()));

        assert!(
            matches!(health_change_poll, Poll::Pending),
            "Checkpoint waiter must ignore health published after subscription"
        );
        response_only.await.unwrap();
        assert_eq!(
            writer.persistence_status(),
            PersistenceStatus::Degraded { failure: published }
        );
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sender_drop_publishes_acknowledgement_failure_for_all_six_operations() {
        let operations = [
            PersistenceOperation::Apply,
            PersistenceOperation::Cleanup,
            PersistenceOperation::UpdateOwnerLocation,
            PersistenceOperation::ReplaceOwner,
            PersistenceOperation::Barrier,
        ];

        for operation in operations {
            let directory = tempfile::tempdir().unwrap();
            let root = StateRoot(directory.path().to_path_buf());
            let store = open_operation_store(&root);
            let (lifecycle, writer, mut handle, _injector) = spawn_writer_with_test_control(
                store,
                super::super::unix_now_ms,
                operation,
                AcknowledgementTestMode::DropAcknowledgement,
            )
            .unwrap();
            assert_eq!(handle.health.status(), PersistenceStatus::Healthy);
            let request = tokio::spawn(execute_operation(writer, operation));

            assert_waiter_constructed_before_admission(&mut handle, operation).await;
            let (writer, result) = request.await.unwrap();
            let expected = acknowledgement_failure(operation);
            assert_persistence_error(result, expected);
            handle
                .wait_for(AcknowledgementTestEvent::WaiterResolved(
                    operation, expected,
                ))
                .await;
            assert_eq!(
                handle.health.status(),
                PersistenceStatus::Degraded { failure: expected }
            );
            handle.release();
            lifecycle.shutdown().await.unwrap();
            drop(writer);
        }

        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_operation_store(&root);
        let (lifecycle, writer, mut handle, _injector) = spawn_writer_with_test_control(
            store,
            super::super::unix_now_ms,
            PersistenceOperation::Checkpoint,
            AcknowledgementTestMode::DropAcknowledgement,
        )
        .unwrap();
        assert_eq!(handle.health.status(), PersistenceStatus::Healthy);
        drop(writer);
        let shutdown = tokio::spawn(lifecycle.shutdown());

        assert_waiter_constructed_before_admission(&mut handle, PersistenceOperation::Checkpoint)
            .await;
        let expected = acknowledgement_failure(PersistenceOperation::Checkpoint);
        handle
            .wait_for(AcknowledgementTestEvent::WaiterResolved(
                PersistenceOperation::Checkpoint,
                expected,
            ))
            .await;
        handle.release();
        let result = shutdown.await.unwrap();
        assert_persistence_error(result, expected);
        assert_eq!(
            handle.health.status(),
            PersistenceStatus::Degraded { failure: expected }
        );
    }

    #[tokio::test]
    async fn precise_acknowledgement_wins_when_response_and_health_are_ready() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_operation_store(&root);
        let (lifecycle, _writer, mut handle, injector) = spawn_writer_with_test_control(
            store,
            super::super::unix_now_ms,
            PersistenceOperation::Apply,
            AcknowledgementTestMode::BlockBeforeAcknowledgement,
        )
        .unwrap();
        let waiter = injector.apply(Vec::new()).await;

        assert_waiter_constructed_before_admission(&mut handle, PersistenceOperation::Apply).await;
        handle
            .wait_for(AcknowledgementTestEvent::BeforeAcknowledgement(
                PersistenceOperation::Apply,
            ))
            .await;
        let competing_health = expected_failure(
            PersistenceOperation::Barrier,
            PersistencePhase::Acknowledgement,
            PersistenceFailureCode::AcknowledgementDropped,
            DurabilityDisposition::NotApplicable,
        );
        handle.publish_failure(competing_health);
        handle.release();
        handle
            .wait_for(AcknowledgementTestEvent::AcknowledgementAttempted(
                PersistenceOperation::Apply,
            ))
            .await;

        assert_eq!(
            waiter.wait().await.unwrap().cleanup,
            CleanupStats::default()
        );
        assert_eq!(
            handle.health.status(),
            PersistenceStatus::Degraded {
                failure: competing_health
            }
        );
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn normal_failure_published_before_ack_returns_precise_durability() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        install_temp_failure_trigger(&store, "fail_precise_apply", "INSERT", "workspaces");
        let (lifecycle, mut writer, mut handle, _injector) = spawn_writer_with_test_control(
            store,
            super::super::unix_now_ms,
            PersistenceOperation::Apply,
            AcknowledgementTestMode::PauseAfterFailurePublication,
        )
        .unwrap();
        let request = tokio::spawn(async move {
            let result = writer.apply(vec![workspace_op("precise-failure")]).await;
            (writer, result)
        });

        assert_waiter_constructed_before_admission(&mut handle, PersistenceOperation::Apply).await;
        handle
            .wait_for(AcknowledgementTestEvent::FailurePublished(
                PersistenceOperation::Apply,
            ))
            .await;
        let expected = expected_failure(
            PersistenceOperation::Apply,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::NotCommitted,
        );
        assert_eq!(
            handle.health.status(),
            PersistenceStatus::Degraded { failure: expected },
            "precise durability must be visible while acknowledgement remains live and unsent"
        );
        handle.release();
        let (writer, result) = request.await.unwrap();

        assert_persistence_error(result, expected);
        handle.publish_failure(acknowledgement_failure(PersistenceOperation::Apply));
        assert_eq!(
            handle.health.status(),
            PersistenceStatus::Degraded { failure: expected },
            "later acknowledgement state cannot replace the first precise failure"
        );
        lifecycle.shutdown().await.unwrap();
        drop(writer);
    }

    #[tokio::test]
    async fn writer_thread_never_mutates_the_collector_ledger_mirror() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let now = super::super::unix_now_ms().unwrap();
        let old = LedgerEntry {
            event_id: "d1-old".to_owned(),
            seen_at_ms: now - 8 * DAY_MS,
        };
        store
            .apply_batch(vec![provider_event(&old.event_id, old.seen_at_ms)])
            .unwrap();
        let (lifecycle, mut writer, mut handle, injector) = spawn_writer_with_test_control(
            store,
            super::super::unix_now_ms,
            PersistenceOperation::Apply,
            AcknowledgementTestMode::BlockBeforeStore,
        )
        .unwrap();
        let waiter = injector
            .apply(vec![provider_event("d1-writer-only", now)])
            .await;

        assert_waiter_constructed_before_admission(&mut handle, PersistenceOperation::Apply).await;
        handle
            .wait_for(AcknowledgementTestEvent::BeforeStore(
                PersistenceOperation::Apply,
            ))
            .await;
        let before_writer_execution = writer.ledger.clone();
        handle.release();
        handle
            .wait_for(AcknowledgementTestEvent::AcknowledgementAttempted(
                PersistenceOperation::Apply,
            ))
            .await;
        let after_delta_before_finish = writer.ledger.clone();
        let pending = PendingEnqueue { waiter };
        let cleanup = writer.finish_pending(pending).await.unwrap();
        let after_finish = writer.ledger.clone();

        assert_eq!(
            before_writer_execution,
            EventLedgerCache::from_entries([old.clone()])
        );
        assert_eq!(
            after_delta_before_finish, before_writer_execution,
            "writer thread must return deltas without mutating the collector cache"
        );
        assert_eq!(cleanup.deleted_ledger_entries, [old]);
        assert_eq!(after_finish, EventLedgerCache::default());
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn writer_panic_after_issued_permit_degrades_and_unblocks_waiter() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer, _handle, injector) = spawn_writer_with_test_control(
            store,
            || panic!("injected writer clock panic"),
            PersistenceOperation::Apply,
            AcknowledgementTestMode::Observe,
        )
        .unwrap();
        let mut health = writer.subscribe_persistence();
        let permit = writer
            .reserve_enqueue()
            .expect("permit must be issued while health is healthy");
        let panic_waiter = injector.apply(Vec::new()).await;

        let health_before_response =
            tokio::time::timeout(TEST_RENDEZVOUS_TIMEOUT, health.changed()).await;
        tokio::time::timeout(TEST_RENDEZVOUS_TIMEOUT, injector.closed())
            .await
            .expect("writer receiver must close after panic");
        assert!(injector.is_closed());
        let pending = permit.enqueue(Vec::new());
        let pending_result =
            tokio::time::timeout(TEST_RENDEZVOUS_TIMEOUT, writer.finish_pending(pending))
                .await
                .expect("issued permit waiter must not hang after writer panic");
        let panic_response = panic_waiter.wait().await;
        let later_reservation = writer.reserve_enqueue();
        let shutdown_result = lifecycle.shutdown().await;

        health_before_response
            .expect("writer unwind must publish health before response inspection")
            .unwrap();
        let expected = acknowledgement_failure(PersistenceOperation::Apply);
        assert_eq!(
            health.borrow_and_update().status,
            PersistenceStatus::Degraded { failure: expected }
        );
        assert_persistence_error(pending_result, expected);
        assert_persistence_error(panic_response, expected);
        assert!(later_reservation.is_none());
        assert!(matches!(shutdown_result, Err(WriterError::ThreadPanicked)));
    }

    #[tokio::test]
    async fn queued_sibling_waiter_observes_writer_panic_without_acknowledgement() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer, mut handle, injector) = spawn_writer_with_test_control(
            store,
            || panic!("panic ahead of queued sibling"),
            PersistenceOperation::Apply,
            AcknowledgementTestMode::BlockBeforeStore,
        )
        .unwrap();
        let mut health = writer.subscribe_persistence();
        let _panic_response = injector.apply(Vec::new()).await;
        handle
            .wait_for(AcknowledgementTestEvent::CommandAdmitted(
                PersistenceOperation::Apply,
            ))
            .await;
        handle
            .wait_for(AcknowledgementTestEvent::BeforeStore(
                PersistenceOperation::Apply,
            ))
            .await;
        let barrier_waiter = injector.barrier().await;
        handle.release();
        tokio::time::timeout(TEST_RENDEZVOUS_TIMEOUT, health.changed())
            .await
            .expect("writer panic must publish health")
            .unwrap();

        let result = barrier_waiter.wait().await;
        let expected = acknowledgement_failure(PersistenceOperation::Apply);
        assert_persistence_error(result, expected);
        assert_eq!(
            health.borrow_and_update().status,
            PersistenceStatus::Degraded { failure: expected }
        );
        assert!(matches!(
            lifecycle.shutdown().await,
            Err(WriterError::ThreadPanicked)
        ));
    }

    #[tokio::test]
    async fn queued_sibling_health_and_closed_interleavings_return_first_published_failure() {
        let operations = [
            PersistenceOperation::Apply,
            PersistenceOperation::Cleanup,
            PersistenceOperation::UpdateOwnerLocation,
            PersistenceOperation::ReplaceOwner,
            PersistenceOperation::Barrier,
        ];
        let first = acknowledgement_failure(PersistenceOperation::Apply);

        for operation in operations {
            let empty_directory = tempfile::tempdir().unwrap();
            let empty_root = StateRoot(empty_directory.path().to_path_buf());
            let empty_store = open_writer(&empty_root).unwrap();
            let (empty_lifecycle, empty_writer) = spawn_writer(empty_store).unwrap();
            let (response_sender, response) = oneshot::channel();
            let mut waiter =
                AcknowledgementWaiter::new(response, empty_writer.health.clone(), operation, None);
            waiter.arm();
            empty_writer.health.publish_failure(first);
            let mut wait = Box::pin(waiter.wait());
            let early = tokio::time::timeout(Duration::from_millis(100), &mut wait).await;
            let _ = response_sender.send(Ok(()));
            let result = match early {
                Ok(result) => result,
                Err(_) => wait.await,
            };
            assert_persistence_error(result, first);
            empty_lifecycle.shutdown().await.unwrap();

            let closed_directory = tempfile::tempdir().unwrap();
            let closed_root = StateRoot(closed_directory.path().to_path_buf());
            let closed_store = open_writer(&closed_root).unwrap();
            let (closed_lifecycle, closed_writer) = spawn_writer(closed_store).unwrap();
            let (response_sender, response) = oneshot::channel::<Result<(), PersistenceFailure>>();
            let mut waiter =
                AcknowledgementWaiter::new(response, closed_writer.health.clone(), operation, None);
            waiter.arm();
            drop(response_sender);
            closed_writer.health.publish_failure(first);
            let result = waiter.wait().await;
            assert_persistence_error(result, first);
            assert_eq!(
                closed_writer.persistence_status(),
                PersistenceStatus::Degraded { failure: first }
            );
            closed_lifecycle.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn i4_writer_active_waiters_classify_disappeared_senders_by_operation() {
        let apply_directory = tempfile::tempdir().unwrap();
        let apply_root = StateRoot(apply_directory.path().to_path_buf());
        let apply_store = open_writer(&apply_root).unwrap();
        let (apply_lifecycle, mut apply_writer) = spawn_writer_with_clock(apply_store, || {
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
        let (barrier_lifecycle, _barrier_writer, mut handle, injector) =
            spawn_writer_with_test_control(
                barrier_store,
                || panic!("drop queued Barrier sender"),
                PersistenceOperation::Apply,
                AcknowledgementTestMode::BlockBeforeStore,
            )
            .unwrap();
        let _apply_waiter = injector.apply(Vec::new()).await;
        handle
            .wait_for(AcknowledgementTestEvent::BeforeStore(
                PersistenceOperation::Apply,
            ))
            .await;
        let barrier_waiter = injector.barrier().await;
        let mut barrier_wait = Box::pin(barrier_waiter.wait());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            barrier_wait.as_mut().poll(&mut context),
            Poll::Pending
        ));
        handle.release();

        assert_persistence_error(
            barrier_wait.await,
            expected_failure(
                PersistenceOperation::Apply,
                PersistencePhase::Acknowledgement,
                PersistenceFailureCode::AcknowledgementDropped,
                DurabilityDisposition::Unknown,
            ),
        );
        assert!(matches!(
            barrier_lifecycle.shutdown().await,
            Err(WriterError::ThreadPanicked)
        ));
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
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
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
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
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
        let (second_lifecycle, mut second_writer) = spawn_writer(second_store).unwrap();
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
        let (lifecycle, mut writer) = spawn_writer_with_clock(store, || {
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
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
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
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
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
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
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
        let (lifecycle, mut writer, _handle, injector) = spawn_writer_with_test_control(
            store,
            super::super::unix_now_ms,
            PersistenceOperation::Cleanup,
            AcknowledgementTestMode::Observe,
        )
        .unwrap();
        assert!(writer.is_duplicate("old-id"));

        let report = injector.cleanup(now).await.wait().await.unwrap();
        assert!(writer.is_duplicate("old-id"));
        writer
            .ledger
            .apply_cleanup(&report.cleanup.deleted_ledger_entries);
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
        let (lifecycle, writer, mut handle, injector) = spawn_writer_with_test_control(
            store,
            super::super::unix_now_ms,
            PersistenceOperation::Cleanup,
            AcknowledgementTestMode::Observe,
        )
        .unwrap();

        let waiter = injector.cleanup(now).await;
        handle
            .wait_for(AcknowledgementTestEvent::AcknowledgementAttempted(
                PersistenceOperation::Cleanup,
            ))
            .await;
        drop(waiter);
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
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        lifecycle.shutdown().await.unwrap();

        assert!(writer.reserve_enqueue().is_none());
        assert!(!writer.is_duplicate("never-admitted"));
    }
}
