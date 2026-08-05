//! T9 dedicated writer thread, `WriterClient`, and `WriterLifecycle`.

use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::lockfile::OwnerRecord;

use super::{PersistBatch, Store, StoreError};

const WRITER_QUEUE_CAPACITY: usize = 256;
const CLEANUP_INTERVAL: Duration = Duration::from_millis(250);

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
}

impl WriterClient {
    /// Atomically commits one reducer persistence batch.
    pub async fn apply(&self, batch: PersistBatch) -> Result<(), WriterError> {
        self.request(|acknowledgement| WriterCommand::Apply {
            batch,
            acknowledgement,
        })
        .await
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
    let (sender, receiver) = mpsc::channel(WRITER_QUEUE_CAPACITY);
    let thread = thread::Builder::new()
        .name("herdr-top-sqlite-writer".to_owned())
        .spawn(move || writer_main(store, receiver))
        .map_err(WriterError::ThreadSpawn)?;
    let client = WriterClient {
        sender: sender.clone(),
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
        acknowledgement: oneshot::Sender<Result<(), StoreError>>,
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

fn writer_main(mut store: Store, mut receiver: mpsc::Receiver<WriterCommand>) {
    let mut last_cleanup = Instant::now();
    while let Some(command) = receiver.blocking_recv() {
        match command {
            WriterCommand::Apply {
                batch,
                acknowledgement,
            } => {
                let result = store.apply_batch(batch).and_then(|()| {
                    if last_cleanup.elapsed() >= CLEANUP_INTERVAL {
                        store.cleanup_retention(unix_now_ms()?)?;
                        last_cleanup = Instant::now();
                    }
                    Ok(())
                });
                let _ = acknowledgement.send(result);
            }
            WriterCommand::UpdateOwnerLocation {
                terminal_id,
                pane_id,
                acknowledgement,
            } => {
                let _ = acknowledgement.send(store.update_owner_location(&terminal_id, &pane_id));
            }
            WriterCommand::ReplaceOwner {
                record,
                acknowledgement,
            } => {
                let _ = acknowledgement.send(store.replace_owner(&record));
            }
            WriterCommand::Barrier { acknowledgement } => {
                let _ = acknowledgement.send(Ok(()));
            }
            WriterCommand::Shutdown { acknowledgement } => {
                let _ = acknowledgement.send(store.checkpoint());
                break;
            }
        }
    }
}

fn unix_now_ms() -> Result<i64, StoreError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| StoreError::Clock(error.to_string()))?;
    i64::try_from(elapsed.as_millis()).map_err(|_| StoreError::IntegerOutOfRange {
        field: "Unix timestamp milliseconds",
    })
}
