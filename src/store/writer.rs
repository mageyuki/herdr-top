//! T9 dedicated writer thread, `WriterClient`, and `WriterLifecycle`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::lockfile::OwnerRecord;

use super::{CleanupStats, LedgerEntry, PersistBatch, PersistOp, Store, StoreError};

const WRITER_QUEUE_CAPACITY: usize = 256;
/// Errors produced by the dedicated SQLite writer.
#[derive(Debug, Error)]
pub enum WriterError {
    /// A committed SQLite operation or shutdown checkpoint failed.
    #[error(transparent)]
    Store(#[from] StoreError),
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
    healthy: Arc<AtomicBool>,
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
}

/// An already-enqueued write whose queue admission can no longer fail.
pub struct PendingEnqueue {
    response: oneshot::Receiver<Result<CleanupStats, StoreError>>,
    ledger: Arc<Mutex<EventLedgerCache>>,
}

impl PendingEnqueue {
    /// Waits for commit and applies the exact cleanup response to the dedup mirror.
    pub async fn wait(self) -> Result<CleanupStats, WriterError> {
        let cleanup = self
            .response
            .await
            .map_err(|_| WriterError::AcknowledgementDropped)??;
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
        self.permit.send(WriterCommand::Apply {
            batch,
            acknowledgement,
        });
        PendingEnqueue {
            response,
            ledger: self.ledger,
        }
    }
}

impl WriterClient {
    /// Atomically commits one reducer persistence batch.
    pub async fn apply(&self, batch: PersistBatch) -> Result<(), WriterError> {
        let (acknowledgement, response) = oneshot::channel();
        self.sender
            .send(WriterCommand::Apply {
                batch,
                acknowledgement,
            })
            .await
            .map_err(|_| WriterError::Closed)?;
        let cleanup = response
            .await
            .map_err(|_| WriterError::AcknowledgementDropped)??;
        let mut ledger = lock_ledger(&self.ledger);
        ledger.apply_cleanup(&cleanup.deleted_ledger_entries);
        Ok(())
    }

    /// Non-blockingly reserves one slot in the bounded writer command channel.
    #[must_use]
    pub fn reserve_enqueue(&self) -> Option<EnqueuePermit> {
        if !self.healthy.load(Ordering::Acquire) {
            return None;
        }
        self.sender
            .clone()
            .try_reserve_owned()
            .ok()
            .map(|permit| EnqueuePermit {
                permit,
                ledger: Arc::clone(&self.ledger),
            })
    }

    /// Returns whether the durable-ledger mirror already contains `event_id`.
    #[must_use]
    pub fn is_duplicate(&self, event_id: &str) -> bool {
        lock_ledger(&self.ledger).contains(event_id)
    }

    /// Drives a periodic retention pass and conditionally evicts its exact deleted rows.
    pub async fn cleanup(&self, now_ms: i64) -> Result<CleanupStats, WriterError> {
        let (acknowledgement, response) = oneshot::channel();
        self.sender
            .send(WriterCommand::Cleanup {
                now_ms,
                acknowledgement,
            })
            .await
            .map_err(|_| WriterError::Closed)?;
        let cleanup = response
            .await
            .map_err(|_| WriterError::AcknowledgementDropped)??;
        lock_ledger(&self.ledger).apply_cleanup(&cleanup.deleted_ledger_entries);
        Ok(cleanup)
    }

    /// Commits the owner's current terminal and public-pane location.
    pub async fn update_owner_location(
        &self,
        terminal_id: &str,
        pane_id: &str,
    ) -> Result<(), WriterError> {
        self.request(|acknowledgement| WriterCommand::UpdateOwnerLocation {
            terminal_id: terminal_id.to_owned(),
            pane_id: pane_id.to_owned(),
            acknowledgement,
        })
        .await
    }

    /// Atomically replaces the owner row and acknowledges after commit.
    pub async fn replace_owner(&self, rec: OwnerRecord) -> Result<(), WriterError> {
        self.request(|acknowledgement| WriterCommand::ReplaceOwner {
            record: rec,
            acknowledgement,
        })
        .await
    }

    /// Acknowledges after every command queued before this call has completed.
    pub async fn barrier(&self) -> Result<(), WriterError> {
        self.request(|acknowledgement| WriterCommand::Barrier { acknowledgement })
            .await
    }

    async fn request(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<(), StoreError>>) -> WriterCommand,
    ) -> Result<(), WriterError> {
        let (acknowledgement, response) = oneshot::channel();
        self.sender
            .send(command(acknowledgement))
            .await
            .map_err(|_| WriterError::Closed)?;
        response
            .await
            .map_err(|_| WriterError::AcknowledgementDropped)??;
        Ok(())
    }
}

/// Unique lifecycle owner for the dedicated writer thread.
pub struct WriterLifecycle {
    sender: mpsc::Sender<WriterCommand>,
    thread: Option<JoinHandle<()>>,
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
            Ok(()) => response
                .await
                .map_err(|_| WriterError::AcknowledgementDropped)?
                .map_err(WriterError::from),
            Err(_) => Err(WriterError::Closed),
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
    let ledger = Arc::new(Mutex::new(EventLedgerCache::from_entries(
        store.load_event_ledger()?,
    )));
    let (sender, receiver) = mpsc::channel(WRITER_QUEUE_CAPACITY);
    let healthy = Arc::new(AtomicBool::new(true));
    let writer_health = Arc::clone(&healthy);
    let writer_ledger = Arc::clone(&ledger);
    let thread = thread::Builder::new()
        .name("herdr-top-sqlite-writer".to_owned())
        .spawn(move || writer_main(store, receiver, writer_health, writer_ledger))
        .map_err(WriterError::ThreadSpawn)?;
    let client = WriterClient {
        sender: sender.clone(),
        ledger,
        healthy,
    };
    let lifecycle = WriterLifecycle {
        sender,
        thread: Some(thread),
    };
    Ok((lifecycle, client))
}

enum WriterCommand {
    Apply {
        batch: PersistBatch,
        acknowledgement: oneshot::Sender<Result<CleanupStats, StoreError>>,
    },
    Cleanup {
        now_ms: i64,
        acknowledgement: oneshot::Sender<Result<CleanupStats, StoreError>>,
    },
    UpdateOwnerLocation {
        terminal_id: String,
        pane_id: String,
        acknowledgement: oneshot::Sender<Result<(), StoreError>>,
    },
    ReplaceOwner {
        record: OwnerRecord,
        acknowledgement: oneshot::Sender<Result<(), StoreError>>,
    },
    Barrier {
        acknowledgement: oneshot::Sender<Result<(), StoreError>>,
    },
    Shutdown {
        acknowledgement: oneshot::Sender<Result<(), StoreError>>,
    },
}

fn writer_main(
    mut store: Store,
    mut receiver: mpsc::Receiver<WriterCommand>,
    healthy: Arc<AtomicBool>,
    ledger: Arc<Mutex<EventLedgerCache>>,
) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            WriterCommand::Apply {
                batch,
                acknowledgement,
            } => {
                let inserted = ledger_entries(&batch);
                let result = store.apply_batch(batch).and_then(|()| {
                    let mut cache = lock_ledger(&ledger);
                    for entry in inserted {
                        let _ = cache.reserve(entry.event_id, entry.seen_at_ms);
                    }
                    drop(cache);
                    store.cleanup_retention(super::unix_now_ms()?)
                });
                mark_unhealthy_on_error(&result, &healthy);
                let _ = acknowledgement.send(result);
            }
            WriterCommand::Cleanup {
                now_ms,
                acknowledgement,
            } => {
                let result = store.cleanup_retention(now_ms);
                mark_unhealthy_on_error(&result, &healthy);
                let _ = acknowledgement.send(result);
            }
            WriterCommand::UpdateOwnerLocation {
                terminal_id,
                pane_id,
                acknowledgement,
            } => {
                let result = store.update_owner_location(&terminal_id, &pane_id);
                mark_unhealthy_on_error(&result, &healthy);
                let _ = acknowledgement.send(result);
            }
            WriterCommand::ReplaceOwner {
                record,
                acknowledgement,
            } => {
                let result = store.replace_owner(&record);
                mark_unhealthy_on_error(&result, &healthy);
                let _ = acknowledgement.send(result);
            }
            WriterCommand::Barrier { acknowledgement } => {
                let _ = acknowledgement.send(Ok(()));
            }
            WriterCommand::Shutdown { acknowledgement } => {
                let result = store.checkpoint();
                mark_unhealthy_on_error(&result, &healthy);
                let _ = acknowledgement.send(result);
                break;
            }
        }
    }
}

fn mark_unhealthy_on_error<T>(result: &Result<T, StoreError>, healthy: &AtomicBool) {
    if result.is_err() {
        healthy.store(false, Ordering::Release);
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
        | NormalizedEvent::ExecutionBegin { metadata, .. }
        | NormalizedEvent::ExecutionEnd { metadata, .. } => &metadata.event_id,
    }
}

#[cfg(test)]
mod tests {
    use crate::lockfile::StateRoot;
    use crate::model::{EventMetadata, GapKind, NormalizedEvent, TopologyEntity, Workspace};
    use crate::store::{CollectorGap, PersistOp, open_writer};

    use super::*;

    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

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
