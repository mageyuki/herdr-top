//! SQLite-backed restoration, ownership, event ledger, and retention.

use std::collections::{HashMap, HashSet};
use std::num::TryFromIntError;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use thiserror::Error;

use crate::lockfile::{OwnerRecord, StateRoot};
use crate::model::{
    AgentNode, DependencyEdge, DisplayOrdinal, EventMetadata, ExecState, Execution, ExecutionEdge,
    GapKind, NormalizedEvent, Pane, Provider, RunId, RunKey, Tab, TaskRun, TaskState, Workspace,
};

pub mod schema;
pub mod writer;

pub use schema::{SchemaVerdict, database_path, preflight_schema};
pub use writer::{
    EnqueuePermit, EventLedgerCache, PendingEnqueue, WriterClient, WriterError, WriterLifecycle,
    spawn_writer,
};

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
const RUN_RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const EVENT_RING_LIMIT: i64 = 100_000;
const DISPLAY_ORDINAL_BASE: i64 = 1;

/// Errors produced by the SQLite store.
#[derive(Debug, Error)]
pub enum StoreError {
    /// A SQLite operation failed.
    #[error("SQLite store operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// A filesystem operation failed.
    #[error("store I/O error at {path:?}: {source}")]
    Io {
        /// The path involved in the failed operation.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The database was written by a newer Herdr Top schema.
    #[error(
        "database schema version {found} is newer than supported version {supported}; upgrade Herdr Top before opening this database"
    )]
    NewerSchema {
        /// The schema version found on disk.
        found: i64,
        /// The newest schema version supported by this binary.
        supported: i64,
    },
    /// A reader was asked to open a database that still needs migration.
    #[error("database schema is not current; start the Herdr Top owner to migrate it")]
    SchemaNotCurrent,
    /// A database expected by a reader does not exist.
    #[error("database does not exist at {0:?}")]
    DatabaseAbsent(PathBuf),
    /// The online backup operation failed.
    #[error("SQLite online backup to {path:?} failed: {source}")]
    Backup {
        /// The destination backup path.
        path: PathBuf,
        /// The SQLite backup error.
        #[source]
        source: rusqlite::Error,
    },
    /// Persisted data could not be decoded into the domain model.
    #[error("invalid persisted {field} value {value:?}: {reason}")]
    InvalidData {
        /// The column or logical field being decoded.
        field: &'static str,
        /// The invalid stored value.
        value: String,
        /// Why the value is invalid.
        reason: String,
    },
    /// A numeric value does not fit SQLite's signed integer representation.
    #[error("{field} value is outside SQLite's signed integer range")]
    IntegerOutOfRange {
        /// The field whose value was out of range.
        field: &'static str,
    },
    /// The system clock is earlier than the Unix epoch or cannot be represented.
    #[error("system clock cannot provide a Unix-epoch millisecond timestamp: {0}")]
    Clock(String),
    /// An owner-location update was requested before an owner row existed.
    #[error("owner row does not exist")]
    OwnerAbsent,
    /// SQLite could not complete a WAL checkpoint because frames remained busy.
    #[error("WAL checkpoint remained busy with {remaining_frames} frames")]
    CheckpointBusy {
        /// Frames not checkpointed by SQLite.
        remaining_frames: i64,
    },
    /// A configured pragma did not take effect.
    #[error("SQLite pragma {pragma} did not take effect; observed {observed:?}")]
    PragmaNotApplied {
        /// The pragma that was configured.
        pragma: &'static str,
        /// SQLite's observed value after configuration.
        observed: String,
    },
}

impl StoreError {
    fn io(path: PathBuf, source: std::io::Error) -> Self {
        Self::Io { path, source }
    }

    fn invalid(field: &'static str, value: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidData {
            field,
            value: value.into(),
            reason: reason.into(),
        }
    }
}

/// A native session identity durably bound to a task run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeSessionBinding {
    /// The provider that owns the native session namespace.
    pub provider: Provider,
    /// The provider-native session identifier.
    pub native_session_id: String,
}

/// A task-run upsert plus the timestamps and native binding needed by storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistTaskRun {
    /// The domain task run.
    pub task_run: TaskRun,
    /// Optional durable native-session binding.
    pub native_session: Option<NativeSessionBinding>,
    /// First-seen Unix-epoch timestamp in milliseconds.
    pub created_at_ms: i64,
    /// Last-updated Unix-epoch timestamp in milliseconds.
    pub updated_at_ms: i64,
    /// Terminal-state Unix-epoch timestamp in milliseconds, when known.
    pub finished_at_ms: Option<i64>,
}

/// An execution upsert plus lifecycle timestamps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistExecution {
    /// The domain execution.
    pub execution: Execution,
    /// First-seen Unix-epoch timestamp in milliseconds.
    pub started_at_ms: i64,
    /// Last-updated Unix-epoch timestamp in milliseconds.
    pub updated_at_ms: i64,
    /// End Unix-epoch timestamp in milliseconds, when ended.
    pub ended_at_ms: Option<i64>,
}

/// A collector-attested interval where observation continuity is unknown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectorGap {
    /// The idempotency identifier for the gap event.
    pub event_id: String,
    /// The named Herdr session whose observation had a gap.
    pub herdr_session: String,
    /// The collector observation time in Unix-epoch milliseconds.
    pub seen_at_ms: i64,
    /// The kind of observation boundary that caused the gap.
    pub kind: GapKind,
}

/// One typed operation in a persisted reducer batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistOp {
    /// Upsert a physical workspace.
    UpsertWorkspace(Workspace),
    /// Upsert a physical tab.
    UpsertTab(Tab),
    /// Upsert a physical pane.
    UpsertPane(Pane),
    /// Delete a physical workspace and its database-cascaded descendants.
    DeleteWorkspace {
        /// The workspace identity to remove.
        workspace_id: String,
    },
    /// Delete a physical tab and its database-cascaded panes.
    DeleteTab {
        /// The tab identity to remove.
        tab_id: String,
    },
    /// Delete a physical pane.
    DeletePane {
        /// The pane identity to remove.
        pane_id: String,
    },
    /// Upsert semantic task-run state.
    UpsertTaskRun(PersistTaskRun),
    /// Atomically re-key a canonical run while retaining its old key as a durable alias row.
    PromoteTaskRunKey {
        /// The promoted canonical task run and its native binding.
        promoted: PersistTaskRun,
        /// The superseded key retained as an alias.
        old_key: RunKey,
        /// The internal row identity used only for the durable alias.
        alias_run_id: RunId,
    },
    /// Merge an absorbed canonical task run into a surviving canonical task run.
    MergeTaskRuns {
        /// The live canonical run that remains addressable.
        survivor: RunId,
        /// The canonical run converted into a durable alias.
        absorbed: RunId,
    },
    /// Upsert one physical execution of a task run.
    UpsertExecution(PersistExecution),
    /// Upsert a provider-native agent node.
    UpsertAgentNode(AgentNode),
    /// Upsert an explicit dispatch relationship.
    UpsertExecutionEdge {
        /// The domain execution edge.
        edge: ExecutionEdge,
        /// First-seen Unix-epoch timestamp in milliseconds.
        created_at_ms: i64,
    },
    /// Upsert an explicit task dependency.
    UpsertDependencyEdge {
        /// The domain dependency edge.
        edge: DependencyEdge,
        /// First-seen Unix-epoch timestamp in milliseconds.
        created_at_ms: i64,
    },
    /// Record one allowlisted normalized event and its ledger identifier.
    RecordEvent {
        /// The normalized event; boxed to keep persistence operations compact.
        event: Box<NormalizedEvent>,
        /// Collector receipt time used for ring and ledger retention.
        seen_at_ms: i64,
    },
    /// Record a collector-attested observation gap and its ledger identifier.
    RecordCollectorGap(CollectorGap),
    /// Advance the non-pruned Controller ingest-sequence high-water mark.
    AdvanceIngestSequence {
        /// The newly allocated sequence in SQLite's signed integer domain.
        ingest_seq: i64,
    },
}

/// A reducer persistence batch committed in exactly one SQLite transaction.
pub type PersistBatch = Vec<PersistOp>;

/// Counts of rows removed during one retention pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CleanupStats {
    /// Activity-ring rows evicted by age or per-session count.
    pub events_evicted: u64,
    /// Deduplication-ledger rows older than seven days.
    pub ledger_pruned: u64,
    /// Finished task runs older than thirty days and not protected by active runs.
    pub runs_pruned: u64,
    /// Execution rows owned by pruned runs.
    pub executions_pruned: u64,
    /// Agent-node rows owned by pruned runs.
    pub agent_nodes_pruned: u64,
    /// Dispatch edges incident to pruned runs.
    pub execution_edges_pruned: u64,
    /// Dependency edges incident to pruned runs.
    pub dependency_edges_pruned: u64,
    /// Native sessions no longer referenced by a run or agent node.
    pub native_sessions_pruned: u64,
    /// Display-ordinal allocations belonging to pruned runs.
    pub display_ordinals_pruned: u64,
    /// Exact durable ledger rows deleted by this transaction.
    pub deleted_ledger_entries: Vec<LedgerEntry>,
}

/// One durable event-ledger row used to seed and conditionally evict the dedup cache.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerEntry {
    pub event_id: String,
    pub seen_at_ms: i64,
}

/// The durable domain model reconstructed at owner startup.
#[derive(Debug)]
pub struct RestoredState {
    /// Persisted topology and semantic state.
    pub model: crate::model::DomainModel,
    /// The next globally available display ordinal.
    pub next_ordinal: i64,
    /// Next Controller ingest sequence, or `None` when `i64::MAX` was allocated.
    pub next_ingest_seq: Option<i64>,
    /// Durable deduplication rows retained at startup.
    pub event_ledger: Vec<LedgerEntry>,
}

/// A single SQLite connection used either by the writer thread or a reader.
pub struct Store {
    connection: Connection,
}

/// Opens, backs up, migrates, configures, and cleans a writer database.
///
/// A missing database is created without a backup. Every existing supported
/// database is backed up through SQLite's online backup API before migration.
pub fn open_writer(root: &StateRoot) -> Result<Store, StoreError> {
    let verdict = preflight_schema(root)?;
    let path = database_path(root);

    match verdict {
        SchemaVerdict::Absent => schema::create_database_file(&path)?,
        SchemaVerdict::Current | SchemaVerdict::Migratable => {
            schema::online_backup(root)?;
            schema::enforce_private_file(&path)?;
        }
    }

    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let mut connection = Connection::open_with_flags(&path, flags)?;
    schema::migrate(&mut connection, unix_now_ms()?)?;
    configure_writer(&connection)?;
    schema::enforce_private_file(&path)?;

    let mut store = Store { connection };
    store.cleanup_retention(unix_now_ms()?)?;
    Ok(store)
}

/// Opens an existing current-schema database strictly read-only.
pub fn open_reader(root: &StateRoot) -> Result<Store, StoreError> {
    match preflight_schema(root)? {
        SchemaVerdict::Absent => return Err(StoreError::DatabaseAbsent(database_path(root))),
        SchemaVerdict::Migratable => return Err(StoreError::SchemaNotCurrent),
        SchemaVerdict::Current => {}
    }

    Ok(Store {
        connection: schema::open_read_only(&database_path(root))?,
    })
}

fn configure_writer(connection: &Connection) -> Result<(), StoreError> {
    let journal_mode: String =
        connection.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(StoreError::PragmaNotApplied {
            pragma: "journal_mode",
            observed: journal_mode,
        });
    }

    connection.pragma_update(None, "synchronous", "FULL")?;
    let synchronous: i64 = connection.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if synchronous != 2 {
        return Err(StoreError::PragmaNotApplied {
            pragma: "synchronous",
            observed: synchronous.to_string(),
        });
    }

    connection.pragma_update(None, "foreign_keys", "ON")?;
    let foreign_keys: i64 =
        connection.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    if foreign_keys != 1 {
        return Err(StoreError::PragmaNotApplied {
            pragma: "foreign_keys",
            observed: foreign_keys.to_string(),
        });
    }
    connection.busy_timeout(BUSY_TIMEOUT)?;
    Ok(())
}

impl Store {
    pub(crate) fn load_event_ledger(&self) -> Result<Vec<LedgerEntry>, StoreError> {
        let mut entries = Vec::new();
        let mut statement = self
            .connection
            .prepare("SELECT event_id, seen_at_ms FROM event_ledger ORDER BY event_id")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            entries.push(LedgerEntry {
                event_id: row.get(0)?,
                seen_at_ms: row.get(1)?,
            });
        }
        Ok(entries)
    }

    /// Restores persisted topology, task runs, executions, nodes, and edges.
    pub fn load_restored_state(&self) -> Result<RestoredState, StoreError> {
        let mut model = crate::model::DomainModel::default();
        self.restore_workspaces(&mut model)?;
        self.restore_tabs(&mut model)?;
        self.restore_panes(&mut model)?;
        self.restore_task_runs(&mut model)?;
        self.restore_executions(&mut model)?;
        self.restore_agent_nodes(&mut model)?;
        self.restore_execution_edges(&mut model)?;
        self.restore_dependency_edges(&mut model)?;

        let maximum: Option<i64> =
            self.connection
                .query_row("SELECT MAX(ordinal) FROM display_ordinals", [], |row| {
                    row.get(0)
                })?;
        let next_ordinal = match maximum {
            Some(value) => value.checked_add(1).ok_or_else(|| {
                StoreError::invalid(
                    "display_ordinal",
                    value.to_string(),
                    "the ordinal allocator is exhausted",
                )
            })?,
            None => DISPLAY_ORDINAL_BASE,
        };

        let ingest_high_water: i64 = self.connection.query_row(
            "SELECT value FROM meta WHERE key = 'ingest_seq_high_water'",
            [],
            |row| row.get(0),
        )?;
        if !(0..=i64::MAX).contains(&ingest_high_water) {
            return Err(StoreError::invalid(
                "meta.ingest_seq_high_water",
                ingest_high_water.to_string(),
                "ingest sequence high-water mark cannot be negative",
            ));
        }
        let next_ingest_seq = ingest_high_water.checked_add(1);

        let event_ledger = self.load_event_ledger()?;

        Ok(RestoredState {
            model,
            next_ordinal,
            next_ingest_seq,
            event_ledger,
        })
    }

    /// Reads the singleton owner row, returning `None` before first ownership.
    pub fn read_owner(&self) -> Result<Option<OwnerRecord>, StoreError> {
        let stored = self
            .connection
            .query_row(
                "SELECT pid, started_at_ms, terminal_id, pane_id \
                 FROM owner WHERE singleton_id = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;

        stored
            .map(|(pid, started_at_ms, terminal_id, pane_id)| {
                let pid = u32::try_from(pid).map_err(|source| {
                    StoreError::invalid("owner.pid", pid.to_string(), source.to_string())
                })?;
                Ok(OwnerRecord {
                    pid,
                    started_at_ms,
                    terminal_id,
                    pane_id,
                })
            })
            .transpose()
    }

    /// Atomically replaces every field of the singleton owner row.
    pub fn replace_owner(&mut self, rec: &OwnerRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO owner(singleton_id, pid, started_at_ms, terminal_id, pane_id) \
             VALUES (1, ?1, ?2, ?3, ?4) \
             ON CONFLICT(singleton_id) DO UPDATE SET \
                 pid = excluded.pid, \
                 started_at_ms = excluded.started_at_ms, \
                 terminal_id = excluded.terminal_id, \
                 pane_id = excluded.pane_id",
            params![
                i64::from(rec.pid),
                rec.started_at_ms,
                rec.terminal_id,
                rec.pane_id
            ],
        )?;
        Ok(())
    }

    /// Updates the current physical location of the existing owner row.
    pub fn update_owner_location(
        &mut self,
        terminal_id: &str,
        pane_id: &str,
    ) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE owner SET terminal_id = ?1, pane_id = ?2 WHERE singleton_id = 1",
            (terminal_id, pane_id),
        )?;
        if changed == 0 {
            Err(StoreError::OwnerAbsent)
        } else {
            Ok(())
        }
    }

    /// Applies every operation atomically in exactly one SQLite transaction.
    pub fn apply_batch(&mut self, batch: PersistBatch) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        for operation in batch {
            apply_operation(&transaction, operation)?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Applies the section 10.3 activity, ledger, and finished-run retention.
    pub fn cleanup_retention(&mut self, now_ms: i64) -> Result<CleanupStats, StoreError> {
        let event_cutoff = now_ms.saturating_sub(EVENT_RETENTION_MS);
        let run_cutoff = now_ms.saturating_sub(RUN_RETENTION_MS);
        let transaction = self.connection.transaction()?;
        let mut stats = CleanupStats::default();

        stats.events_evicted += changed(
            transaction.execute("DELETE FROM events WHERE seen_at_ms < ?1", [event_cutoff])?,
        );
        stats.events_evicted += changed(transaction.execute(
            "DELETE FROM events \
             WHERE event_row_id IN (\
                 SELECT event_row_id FROM (\
                     SELECT event_row_id, \
                            ROW_NUMBER() OVER (\
                                PARTITION BY herdr_session \
                                ORDER BY seen_at_ms DESC, event_row_id DESC\
                            ) AS session_rank \
                     FROM events\
                 ) \
                 WHERE session_rank > ?1\
             )",
            [EVENT_RING_LIMIT],
        )?);
        {
            let mut statement = transaction.prepare(
                "SELECT event_id, seen_at_ms FROM event_ledger \
                 WHERE seen_at_ms < ?1 ORDER BY event_id",
            )?;
            let mut rows = statement.query([event_cutoff])?;
            while let Some(row) = rows.next()? {
                stats.deleted_ledger_entries.push(LedgerEntry {
                    event_id: row.get(0)?,
                    seen_at_ms: row.get(1)?,
                });
            }
        }
        stats.ledger_pruned = changed(transaction.execute(
            "DELETE FROM event_ledger WHERE seen_at_ms < ?1",
            [event_cutoff],
        )?);

        transaction.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS cleanup_doomed_runs (\
                 run_id TEXT PRIMARY KEY\
             ) WITHOUT ROWID;\
             DELETE FROM cleanup_doomed_runs;",
        )?;
        transaction.execute(
            "WITH RECURSIVE doomed(run_id) AS (\
                 SELECT candidate.run_id \
                 FROM task_runs AS candidate \
                 WHERE candidate.merged_into IS NULL \
                   AND candidate.task_state IN (\
                       'completed', 'failed', 'cancelled', 'ended_unknown'\
                   ) \
                   AND candidate.finished_at_ms < ?1 \
                   AND NOT EXISTS (\
                       SELECT 1 \
                       FROM execution_edges AS edge \
                       JOIN task_runs AS active ON active.run_id = edge.child_run_id \
                       WHERE edge.parent_run_id = candidate.run_id \
                         AND active.task_state NOT IN (\
                             'completed', 'failed', 'cancelled', 'ended_unknown'\
                         )\
                   ) \
                   AND NOT EXISTS (\
                       SELECT 1 \
                       FROM dependency_edges AS edge \
                       JOIN task_runs AS active ON active.run_id = edge.dependent_run_id \
                       WHERE edge.prerequisite_run_id = candidate.run_id \
                         AND active.task_state NOT IN (\
                             'completed', 'failed', 'cancelled', 'ended_unknown'\
                         )\
                   ) \
                 UNION \
                 SELECT alias.run_id \
                 FROM task_runs AS alias \
                 JOIN doomed AS parent ON alias.merged_into = parent.run_id\
             ) \
             INSERT OR IGNORE INTO cleanup_doomed_runs(run_id) SELECT run_id FROM doomed",
            [run_cutoff],
        )?;

        stats.execution_edges_pruned = changed(transaction.execute(
            "DELETE FROM execution_edges \
             WHERE parent_run_id IN (SELECT run_id FROM cleanup_doomed_runs) \
                OR child_run_id IN (SELECT run_id FROM cleanup_doomed_runs)",
            [],
        )?);
        stats.dependency_edges_pruned = changed(transaction.execute(
            "DELETE FROM dependency_edges \
             WHERE prerequisite_run_id IN (SELECT run_id FROM cleanup_doomed_runs) \
                OR dependent_run_id IN (SELECT run_id FROM cleanup_doomed_runs)",
            [],
        )?);
        stats.executions_pruned = changed(transaction.execute(
            "DELETE FROM executions \
             WHERE task_run_id IN (SELECT run_id FROM cleanup_doomed_runs)",
            [],
        )?);
        stats.agent_nodes_pruned = changed(transaction.execute(
            "DELETE FROM agent_nodes \
             WHERE task_run_id IN (SELECT run_id FROM cleanup_doomed_runs)",
            [],
        )?);
        stats.display_ordinals_pruned = changed(transaction.execute(
            "DELETE FROM display_ordinals \
             WHERE entity_kind = 'task_run' \
               AND entity_id IN (SELECT run_id FROM cleanup_doomed_runs)",
            [],
        )?);
        let doomed_run_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM cleanup_doomed_runs", [], |row| {
                row.get(0)
            })?;
        stats.runs_pruned = u64::try_from(doomed_run_count).map_err(|_| {
            StoreError::invalid(
                "cleanup_doomed_runs",
                doomed_run_count.to_string(),
                "row count cannot be negative",
            )
        })?;
        transaction.execute(
            "DELETE FROM task_runs \
             WHERE run_id IN (SELECT run_id FROM cleanup_doomed_runs)",
            [],
        )?;
        stats.native_sessions_pruned = changed(transaction.execute(
            "DELETE FROM native_agent_sessions AS native \
             WHERE NOT EXISTS (\
                 SELECT 1 FROM task_runs AS run \
                 WHERE run.native_provider = native.provider \
                   AND run.native_session_id = native.native_session_id\
             ) \
               AND NOT EXISTS (\
                 SELECT 1 FROM agent_nodes AS node \
                 WHERE node.provider = native.provider \
                   AND node.native_session_id = native.native_session_id\
             )",
            [],
        )?);
        transaction.execute_batch("DROP TABLE cleanup_doomed_runs;")?;
        transaction.commit()?;
        Ok(stats)
    }

    /// Forces a truncating WAL checkpoint, intended for orderly shutdown.
    pub fn checkpoint(&mut self) -> Result<(), StoreError> {
        let (busy, wal_frames, checkpointed_frames): (i64, i64, i64) =
            self.connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
        if busy == 0 {
            Ok(())
        } else {
            Err(StoreError::CheckpointBusy {
                remaining_frames: wal_frames.saturating_sub(checkpointed_frames),
            })
        }
    }

    fn restore_workspaces(&self, model: &mut crate::model::DomainModel) -> Result<(), StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT workspace_id FROM workspaces")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            model.insert_workspace(Workspace {
                workspace_id: row.get(0)?,
            });
        }
        Ok(())
    }

    fn restore_tabs(&self, model: &mut crate::model::DomainModel) -> Result<(), StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT tab_id, workspace_id FROM tabs")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            model.insert_tab(Tab {
                tab_id: row.get(0)?,
                workspace_id: row.get(1)?,
            });
        }
        Ok(())
    }

    fn restore_panes(&self, model: &mut crate::model::DomainModel) -> Result<(), StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT pane_id, workspace_id, tab_id, terminal_id FROM panes")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            model.insert_pane(Pane {
                pane_id: row.get(0)?,
                workspace_id: row.get(1)?,
                tab_id: row.get(2)?,
                terminal_id: row.get(3)?,
            });
        }
        Ok(())
    }

    fn restore_task_runs(&self, model: &mut crate::model::DomainModel) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT run_id, key_kind, key_controller_id, key_provider, key_native_sid, \
                    key_native_path, key_terminal_id, key_start_ms, key_seq, \
                    display_ordinal, task_state, has_controller_task_state_event, merged_into, \
                    native_provider, native_session_id \
             FROM task_runs",
        )?;
        let mut rows = statement.query([])?;
        let mut stored_runs = Vec::new();
        while let Some(row) = rows.next()? {
            let run_id_text: String = row.get(0)?;
            let run_id = parse_run_id(&run_id_text)?;
            let key_kind: String = row.get(1)?;
            let key = decode_run_key(
                &key_kind,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            )?;
            let state_text: String = row.get(10)?;
            let merged_into_text: Option<String> = row.get(12)?;
            stored_runs.push(StoredTaskRun {
                task_run: TaskRun {
                    run_id,
                    key,
                    display_ordinal: DisplayOrdinal::new(row.get(9)?),
                    state: parse_task_state(&state_text)?,
                    has_controller_task_state_event: row.get::<_, i64>(11)? != 0,
                },
                merged_into: merged_into_text.as_deref().map(parse_run_id).transpose()?,
                native_binding: match (
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, Option<String>>(14)?,
                ) {
                    (Some(provider), Some(sid)) => Some((parse_provider(&provider)?, sid)),
                    (None, None) => None,
                    _ => {
                        return Err(StoreError::invalid(
                            "native_session_binding",
                            run_id_text,
                            "provider and native session ID nullability disagree",
                        ));
                    }
                },
            });
        }

        for stored in stored_runs
            .iter()
            .filter(|stored| stored.merged_into.is_none())
        {
            if matches!(stored.task_run.key, RunKey::Provisional { .. }) {
                model.insert_historical_task_run(stored.task_run.clone());
            } else {
                model.insert_task_run(stored.task_run.clone());
            }
            if let Some((provider, sid)) = &stored.native_binding {
                model.insert_task_run_alias(
                    RunKey::Native {
                        provider: *provider,
                        sid: sid.clone(),
                    },
                    stored.task_run.run_id,
                );
            }
        }

        let aliases: HashMap<RunId, RunId> = stored_runs
            .iter()
            .filter_map(|stored| {
                stored
                    .merged_into
                    .map(|target| (stored.task_run.run_id, target))
            })
            .collect();
        for stored in stored_runs
            .iter()
            .filter(|stored| stored.merged_into.is_some())
        {
            let canonical = resolve_alias_root(stored.task_run.run_id, &aliases)?;
            if model.task_run(&canonical).is_none() {
                return Err(StoreError::invalid(
                    "merged_into",
                    canonical.to_string(),
                    format!(
                        "alias task run {} does not resolve to a live canonical root",
                        stored.task_run.run_id
                    ),
                ));
            }
            if !matches!(stored.task_run.key, RunKey::Provisional { .. }) {
                model.insert_task_run_alias(stored.task_run.key.clone(), canonical);
            }
        }
        Ok(())
    }

    fn restore_executions(&self, model: &mut crate::model::DomainModel) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT execution_id, pane_id, terminal_id, task_run_id, exec_state, stale_since_ms \
             FROM executions",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let run_id_text: String = row.get(3)?;
            let state_text: String = row.get(4)?;
            model.insert_execution(Execution {
                execution_id: row.get(0)?,
                pane_id: row.get(1)?,
                terminal_id: row.get(2)?,
                task_run_id: parse_run_id(&run_id_text)?,
                state: parse_exec_state(&state_text, row.get(5)?)?,
            });
        }
        Ok(())
    }

    fn restore_agent_nodes(&self, model: &mut crate::model::DomainModel) -> Result<(), StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT agent_node_id, provider, native_session_id, task_run_id FROM agent_nodes",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let provider_text: String = row.get(1)?;
            let run_id_text: String = row.get(3)?;
            model.insert_agent_node(AgentNode {
                agent_node_id: row.get(0)?,
                provider: parse_provider(&provider_text)?,
                native_session_id: row.get(2)?,
                task_run_id: parse_run_id(&run_id_text)?,
            });
        }
        Ok(())
    }

    fn restore_execution_edges(
        &self,
        model: &mut crate::model::DomainModel,
    ) -> Result<(), StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT parent_run_id, child_run_id FROM execution_edges")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let parent: String = row.get(0)?;
            let child: String = row.get(1)?;
            model.insert_execution_edge(ExecutionEdge {
                parent_run_id: parse_run_id(&parent)?,
                child_run_id: parse_run_id(&child)?,
            });
        }
        Ok(())
    }

    fn restore_dependency_edges(
        &self,
        model: &mut crate::model::DomainModel,
    ) -> Result<(), StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT prerequisite_run_id, dependent_run_id FROM dependency_edges")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let prerequisite: String = row.get(0)?;
            let dependent: String = row.get(1)?;
            model.insert_dependency_edge(DependencyEdge {
                prerequisite_run_id: parse_run_id(&prerequisite)?,
                dependent_run_id: parse_run_id(&dependent)?,
            });
        }
        Ok(())
    }
}

struct StoredTaskRun {
    task_run: TaskRun,
    merged_into: Option<RunId>,
    native_binding: Option<(Provider, String)>,
}

fn resolve_alias_root(alias: RunId, aliases: &HashMap<RunId, RunId>) -> Result<RunId, StoreError> {
    let mut current = alias;
    let mut visited = HashSet::new();
    while let Some(target) = aliases.get(&current) {
        if !visited.insert(current) {
            return Err(StoreError::invalid(
                "merged_into",
                alias.to_string(),
                "alias cycle detected while restoring task runs",
            ));
        }
        current = *target;
    }
    Ok(current)
}

fn apply_operation(transaction: &Transaction<'_>, operation: PersistOp) -> Result<(), StoreError> {
    match operation {
        PersistOp::UpsertWorkspace(workspace) => {
            transaction.execute(
                "INSERT INTO workspaces(workspace_id) VALUES (?1) \
                 ON CONFLICT(workspace_id) DO NOTHING",
                [&workspace.workspace_id],
            )?;
        }
        PersistOp::UpsertTab(tab) => {
            transaction.execute(
                "INSERT INTO tabs(tab_id, workspace_id) VALUES (?1, ?2) \
                 ON CONFLICT(tab_id) DO UPDATE SET workspace_id = excluded.workspace_id",
                (&tab.tab_id, &tab.workspace_id),
            )?;
        }
        PersistOp::UpsertPane(pane) => {
            transaction.execute(
                "INSERT INTO panes(pane_id, workspace_id, tab_id, terminal_id) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(pane_id) DO UPDATE SET \
                     workspace_id = excluded.workspace_id, \
                     tab_id = excluded.tab_id, \
                     terminal_id = excluded.terminal_id",
                params![
                    pane.pane_id,
                    pane.workspace_id,
                    pane.tab_id,
                    pane.terminal_id
                ],
            )?;
        }
        PersistOp::DeleteWorkspace { workspace_id } => {
            transaction.execute(
                "DELETE FROM workspaces WHERE workspace_id = ?1",
                [&workspace_id],
            )?;
        }
        PersistOp::DeleteTab { tab_id } => {
            transaction.execute("DELETE FROM tabs WHERE tab_id = ?1", [&tab_id])?;
        }
        PersistOp::DeletePane { pane_id } => {
            transaction.execute("DELETE FROM panes WHERE pane_id = ?1", [&pane_id])?;
        }
        PersistOp::UpsertTaskRun(task_run) => upsert_task_run(transaction, &task_run)?,
        PersistOp::PromoteTaskRunKey {
            promoted,
            old_key,
            alias_run_id,
        } => promote_task_run_key(transaction, &promoted, &old_key, alias_run_id)?,
        PersistOp::MergeTaskRuns { survivor, absorbed } => {
            merge_task_runs(transaction, survivor, absorbed)?;
        }
        PersistOp::UpsertExecution(execution) => upsert_execution(transaction, &execution)?,
        PersistOp::UpsertAgentNode(agent_node) => upsert_agent_node(transaction, &agent_node)?,
        PersistOp::UpsertExecutionEdge {
            edge,
            created_at_ms,
        } => {
            transaction.execute(
                "INSERT INTO execution_edges(parent_run_id, child_run_id, created_at_ms) \
                 VALUES (?1, ?2, ?3) \
                 ON CONFLICT(parent_run_id, child_run_id) DO NOTHING",
                params![
                    edge.parent_run_id.to_string(),
                    edge.child_run_id.to_string(),
                    created_at_ms
                ],
            )?;
        }
        PersistOp::UpsertDependencyEdge {
            edge,
            created_at_ms,
        } => {
            transaction.execute(
                "INSERT INTO dependency_edges(\
                     prerequisite_run_id, dependent_run_id, created_at_ms\
                 ) VALUES (?1, ?2, ?3) \
                 ON CONFLICT(prerequisite_run_id, dependent_run_id) DO NOTHING",
                params![
                    edge.prerequisite_run_id.to_string(),
                    edge.dependent_run_id.to_string(),
                    created_at_ms
                ],
            )?;
        }
        PersistOp::RecordEvent { event, seen_at_ms } => {
            record_event(transaction, &event, seen_at_ms)?;
        }
        PersistOp::RecordCollectorGap(gap) => record_gap(transaction, &gap)?,
        PersistOp::AdvanceIngestSequence { ingest_seq } => {
            let changed = transaction.execute(
                "UPDATE meta SET value = ?1 \
                 WHERE key = 'ingest_seq_high_water' AND value < ?1",
                [ingest_seq],
            )?;
            if changed != 1 {
                let current: i64 = transaction.query_row(
                    "SELECT value FROM meta WHERE key = 'ingest_seq_high_water'",
                    [],
                    |row| row.get(0),
                )?;
                return Err(StoreError::invalid(
                    "ingest_seq",
                    ingest_seq.to_string(),
                    format!("sequence must advance durable high-water mark {current}"),
                ));
            }
        }
    }
    Ok(())
}

fn promote_task_run_key(
    transaction: &Transaction<'_>,
    promoted: &PersistTaskRun,
    old_key: &RunKey,
    alias_run_id: RunId,
) -> Result<(), StoreError> {
    let canonical_run_id = promoted.task_run.run_id.to_string();
    let stored_key = transaction.query_row(
        "SELECT key_kind, key_controller_id, key_provider, key_native_sid, \
                key_native_path, key_terminal_id, key_start_ms, key_seq \
         FROM task_runs WHERE run_id = ?1 AND merged_into IS NULL",
        [&canonical_run_id],
        |row| {
            let kind: String = row.get(0)?;
            Ok((
                kind,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        },
    )?;
    let decoded = decode_run_key(
        &stored_key.0,
        stored_key.1,
        stored_key.2,
        stored_key.3,
        stored_key.4,
        stored_key.5,
        stored_key.6,
        stored_key.7,
    )?;
    if &decoded != old_key {
        return Err(StoreError::invalid(
            "task_run_key",
            format!("{decoded:?}"),
            format!("expected promotion source key {old_key:?}"),
        ));
    }

    upsert_task_run(transaction, promoted)?;

    let alias_ordinal: i64 = transaction.query_row(
        "SELECT COALESCE(MIN(display_ordinal), 0) - 1 FROM task_runs",
        [],
        |row| row.get(0),
    )?;
    let encoded = EncodedRunKey::from(old_key);
    transaction.execute(
        "INSERT INTO task_runs(\
             run_id, key_kind, key_controller_id, key_provider, key_native_sid, \
             key_native_path, key_terminal_id, key_start_ms, key_seq, display_ordinal, \
             task_state, has_controller_task_state_event, native_provider, native_session_id, \
             merged_into, created_at_ms, updated_at_ms, finished_at_ms\
         ) VALUES (\
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, NULL, NULL, ?13, ?14, ?15, ?16\
         )",
        params![
            alias_run_id.to_string(),
            encoded.kind,
            encoded.controller_id,
            encoded.provider,
            encoded.native_sid,
            encoded.native_path,
            encoded.terminal_id,
            encoded.start_ms,
            encoded.seq,
            alias_ordinal,
            task_state_text(promoted.task_run.state),
            i64::from(promoted.task_run.has_controller_task_state_event),
            canonical_run_id,
            promoted.created_at_ms,
            promoted.updated_at_ms,
            promoted.finished_at_ms,
        ],
    )?;
    Ok(())
}

struct MergeParty {
    binding: Option<(String, String)>,
}

fn merge_task_runs(
    transaction: &Transaction<'_>,
    survivor: RunId,
    absorbed: RunId,
) -> Result<(), StoreError> {
    if survivor == absorbed {
        return Err(StoreError::invalid(
            "task_run_merge",
            survivor.to_string(),
            "survivor and absorbed task runs must be distinct",
        ));
    }

    let survivor_text = survivor.to_string();
    let absorbed_text = absorbed.to_string();
    let survivor_party = read_canonical_merge_party(transaction, &survivor_text)?;
    let absorbed_party = read_canonical_merge_party(transaction, &absorbed_text)?;

    match (&survivor_party.binding, &absorbed_party.binding) {
        (None, Some((provider, native_session_id))) => {
            transaction.execute(
                "UPDATE task_runs \
                 SET native_provider = NULL, native_session_id = NULL \
                 WHERE run_id = ?1",
                [&absorbed_text],
            )?;
            transaction.execute(
                "UPDATE task_runs \
                 SET native_provider = ?2, native_session_id = ?3 \
                 WHERE run_id = ?1",
                (&survivor_text, provider, native_session_id),
            )?;
        }
        (Some(survivor_binding), Some(absorbed_binding))
            if survivor_binding == absorbed_binding =>
        {
            return Err(StoreError::invalid(
                "native_session_binding",
                format!("{}:{}", survivor_binding.0, survivor_binding.1),
                "the survivor and absorbed rows share one binding despite its UNIQUE invariant",
            ));
        }
        (Some(survivor_binding), Some(absorbed_binding)) => {
            return Err(StoreError::invalid(
                "native_session_binding",
                format!(
                    "{}:{} and {}:{}",
                    survivor_binding.0, survivor_binding.1, absorbed_binding.0, absorbed_binding.1
                ),
                "cannot merge task runs with different native-session bindings",
            ));
        }
        (Some(_), None) | (None, None) => {}
    }

    let execution_edges = substituted_execution_edges(transaction, &survivor_text, &absorbed_text)?;
    let dependency_edges =
        substituted_dependency_edges(transaction, &survivor_text, &absorbed_text)?;

    transaction.execute(
        "UPDATE executions SET task_run_id = ?1 WHERE task_run_id = ?2",
        (&survivor_text, &absorbed_text),
    )?;
    transaction.execute(
        "UPDATE agent_nodes SET task_run_id = ?1 WHERE task_run_id = ?2",
        (&survivor_text, &absorbed_text),
    )?;
    transaction.execute(
        "UPDATE events SET task_run_id = ?1 WHERE task_run_id = ?2",
        (&survivor_text, &absorbed_text),
    )?;
    replace_execution_edges(transaction, execution_edges)?;
    replace_dependency_edges(transaction, dependency_edges)?;

    transaction.execute(
        "UPDATE task_runs SET merged_into = ?1 WHERE merged_into = ?2",
        (&survivor_text, &absorbed_text),
    )?;
    transaction.execute(
        "UPDATE task_runs SET merged_into = ?1 WHERE run_id = ?2",
        (&survivor_text, &absorbed_text),
    )?;
    Ok(())
}

fn read_canonical_merge_party(
    transaction: &Transaction<'_>,
    run_id: &str,
) -> Result<MergeParty, StoreError> {
    let stored: Option<(Option<String>, Option<String>, Option<String>)> = transaction
        .query_row(
            "SELECT native_provider, native_session_id, merged_into \
             FROM task_runs WHERE run_id = ?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let (native_provider, native_session_id, merged_into) = stored.ok_or_else(|| {
        StoreError::invalid("task_run_merge", run_id, "merge party does not exist")
    })?;
    if let Some(target) = merged_into {
        return Err(StoreError::invalid(
            "merged_into",
            run_id,
            format!("merge party is already an alias of {target}"),
        ));
    }
    let binding = match (native_provider, native_session_id) {
        (Some(provider), Some(native_session_id)) => Some((provider, native_session_id)),
        (None, None) => None,
        _ => {
            return Err(StoreError::invalid(
                "native_session_binding",
                run_id,
                "provider and native session ID nullability disagree",
            ));
        }
    };
    Ok(MergeParty { binding })
}

fn substituted_execution_edges(
    transaction: &Transaction<'_>,
    survivor: &str,
    absorbed: &str,
) -> Result<Vec<(String, String, i64)>, StoreError> {
    let mut statement = transaction
        .prepare("SELECT parent_run_id, child_run_id, created_at_ms FROM execution_edges")?;
    let mut rows = statement.query([])?;
    let mut substituted: HashMap<String, (String, i64)> = HashMap::new();
    while let Some(row) = rows.next()? {
        let parent = substitute_run_id(row.get(0)?, survivor, absorbed);
        let child = substitute_run_id(row.get(1)?, survivor, absorbed);
        let created_at_ms: i64 = row.get(2)?;
        if parent == child {
            return Err(StoreError::invalid(
                "execution_edge",
                format!("{parent}->{child}"),
                "task-run merge would create a dispatch self-edge",
            ));
        }
        match substituted.entry(child.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert((parent, created_at_ms));
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let (existing_parent, oldest_created_at_ms) = entry.get_mut();
                if existing_parent != &parent {
                    return Err(StoreError::invalid(
                        "execution_edge",
                        child,
                        format!(
                            "task-run merge would give one child differing parents {existing_parent} and {parent}"
                        ),
                    ));
                }
                *oldest_created_at_ms = (*oldest_created_at_ms).min(created_at_ms);
            }
        }
    }
    let mut edges: Vec<_> = substituted
        .into_iter()
        .map(|(child, (parent, created_at_ms))| (parent, child, created_at_ms))
        .collect();
    edges.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    Ok(edges)
}

fn substituted_dependency_edges(
    transaction: &Transaction<'_>,
    survivor: &str,
    absorbed: &str,
) -> Result<Vec<(String, String, i64)>, StoreError> {
    let mut statement = transaction.prepare(
        "SELECT prerequisite_run_id, dependent_run_id, created_at_ms FROM dependency_edges",
    )?;
    let mut rows = statement.query([])?;
    let mut substituted: HashMap<(String, String), i64> = HashMap::new();
    while let Some(row) = rows.next()? {
        let prerequisite = substitute_run_id(row.get(0)?, survivor, absorbed);
        let dependent = substitute_run_id(row.get(1)?, survivor, absorbed);
        let created_at_ms: i64 = row.get(2)?;
        if prerequisite == dependent {
            return Err(StoreError::invalid(
                "dependency_edge",
                format!("{prerequisite}->{dependent}"),
                "task-run merge would create a dependency self-edge",
            ));
        }
        substituted
            .entry((prerequisite, dependent))
            .and_modify(|oldest| *oldest = (*oldest).min(created_at_ms))
            .or_insert(created_at_ms);
    }
    let mut edges: Vec<_> = substituted
        .into_iter()
        .map(|((prerequisite, dependent), created_at_ms)| (prerequisite, dependent, created_at_ms))
        .collect();
    edges.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    Ok(edges)
}

fn substitute_run_id(value: String, survivor: &str, absorbed: &str) -> String {
    if value == absorbed {
        survivor.to_owned()
    } else {
        value
    }
}

fn replace_execution_edges(
    transaction: &Transaction<'_>,
    edges: Vec<(String, String, i64)>,
) -> Result<(), StoreError> {
    transaction.execute("DELETE FROM execution_edges", [])?;
    let mut statement = transaction.prepare(
        "INSERT INTO execution_edges(parent_run_id, child_run_id, created_at_ms) \
         VALUES (?1, ?2, ?3)",
    )?;
    for (parent, child, created_at_ms) in edges {
        statement.execute((parent, child, created_at_ms))?;
    }
    Ok(())
}

fn replace_dependency_edges(
    transaction: &Transaction<'_>,
    edges: Vec<(String, String, i64)>,
) -> Result<(), StoreError> {
    transaction.execute("DELETE FROM dependency_edges", [])?;
    let mut statement = transaction.prepare(
        "INSERT INTO dependency_edges(\
             prerequisite_run_id, dependent_run_id, created_at_ms\
         ) VALUES (?1, ?2, ?3)",
    )?;
    for (prerequisite, dependent, created_at_ms) in edges {
        statement.execute((prerequisite, dependent, created_at_ms))?;
    }
    Ok(())
}

fn upsert_task_run(
    transaction: &Transaction<'_>,
    persisted: &PersistTaskRun,
) -> Result<(), StoreError> {
    let task_run = &persisted.task_run;
    let run_id = task_run.run_id.to_string();
    let existing_merged_into: Option<Option<String>> = transaction
        .query_row(
            "SELECT merged_into FROM task_runs WHERE run_id = ?1",
            [&run_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(Some(canonical)) = existing_merged_into {
        return Err(StoreError::invalid(
            "merged_into",
            run_id,
            format!("cannot upsert an alias of canonical task run {canonical}"),
        ));
    }
    let encoded_key = EncodedRunKey::from(&task_run.key);
    let task_state = task_state_text(task_run.state);
    let finished_at_ms = if task_run.state.is_terminal() {
        persisted.finished_at_ms.or(Some(persisted.updated_at_ms))
    } else {
        None
    };
    let (native_provider, native_session_id) = match &persisted.native_session {
        Some(binding) => {
            let provider = provider_text(binding.provider);
            transaction.execute(
                "INSERT INTO native_agent_sessions(provider, native_session_id) \
                 VALUES (?1, ?2) \
                 ON CONFLICT(provider, native_session_id) DO NOTHING",
                (provider, &binding.native_session_id),
            )?;
            (Some(provider), Some(binding.native_session_id.as_str()))
        }
        None => (None, None),
    };

    transaction.execute(
        "INSERT INTO display_ordinals(entity_kind, entity_id, ordinal) \
         VALUES ('task_run', ?1, ?2) \
         ON CONFLICT(entity_kind, entity_id) DO NOTHING",
        params![run_id, task_run.display_ordinal.get()],
    )?;
    let persisted_ordinal: i64 = transaction.query_row(
        "SELECT ordinal FROM display_ordinals \
         WHERE entity_kind = 'task_run' AND entity_id = ?1",
        [&run_id],
        |row| row.get(0),
    )?;
    if persisted_ordinal != task_run.display_ordinal.get() {
        return Err(StoreError::invalid(
            "display_ordinal",
            task_run.display_ordinal.get().to_string(),
            format!("task run {run_id} already owns ordinal {persisted_ordinal}"),
        ));
    }

    transaction.execute(
        "INSERT INTO task_runs(\
             run_id, key_kind, key_controller_id, key_provider, key_native_sid, \
             key_native_path, key_terminal_id, key_start_ms, key_seq, \
             display_ordinal, task_state, has_controller_task_state_event, \
             native_provider, native_session_id, created_at_ms, updated_at_ms, finished_at_ms\
         ) VALUES (\
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17\
         ) \
         ON CONFLICT(run_id) DO UPDATE SET \
             key_kind = excluded.key_kind, \
             key_controller_id = excluded.key_controller_id, \
             key_provider = excluded.key_provider, \
             key_native_sid = excluded.key_native_sid, \
             key_native_path = excluded.key_native_path, \
             key_terminal_id = excluded.key_terminal_id, \
             key_start_ms = excluded.key_start_ms, \
             key_seq = excluded.key_seq, \
             task_state = excluded.task_state, \
             has_controller_task_state_event = excluded.has_controller_task_state_event, \
             native_provider = excluded.native_provider, \
             native_session_id = excluded.native_session_id, \
             created_at_ms = MIN(task_runs.created_at_ms, excluded.created_at_ms), \
             updated_at_ms = excluded.updated_at_ms, \
             finished_at_ms = excluded.finished_at_ms",
        params![
            run_id,
            encoded_key.kind,
            encoded_key.controller_id,
            encoded_key.provider,
            encoded_key.native_sid,
            encoded_key.native_path,
            encoded_key.terminal_id,
            encoded_key.start_ms,
            encoded_key.seq,
            task_run.display_ordinal.get(),
            task_state,
            i64::from(task_run.has_controller_task_state_event),
            native_provider,
            native_session_id,
            persisted.created_at_ms,
            persisted.updated_at_ms,
            finished_at_ms
        ],
    )?;
    Ok(())
}

fn upsert_execution(
    transaction: &Transaction<'_>,
    persisted: &PersistExecution,
) -> Result<(), StoreError> {
    let (state, stale_since_ms) = encode_exec_state(&persisted.execution.state);
    let ended_at_ms = if persisted.execution.state.is_terminal() {
        persisted.ended_at_ms.or(Some(persisted.updated_at_ms))
    } else {
        None
    };
    transaction.execute(
        "INSERT INTO executions(\
             execution_id, pane_id, terminal_id, task_run_id, exec_state, stale_since_ms, \
             started_at_ms, updated_at_ms, ended_at_ms\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
         ON CONFLICT(execution_id) DO UPDATE SET \
             pane_id = excluded.pane_id, \
             terminal_id = excluded.terminal_id, \
             task_run_id = excluded.task_run_id, \
             exec_state = excluded.exec_state, \
             stale_since_ms = excluded.stale_since_ms, \
             started_at_ms = MIN(executions.started_at_ms, excluded.started_at_ms), \
             updated_at_ms = excluded.updated_at_ms, \
             ended_at_ms = excluded.ended_at_ms",
        params![
            persisted.execution.execution_id,
            persisted.execution.pane_id,
            persisted.execution.terminal_id,
            persisted.execution.task_run_id.to_string(),
            state,
            stale_since_ms,
            persisted.started_at_ms,
            persisted.updated_at_ms,
            ended_at_ms
        ],
    )?;
    Ok(())
}

fn upsert_agent_node(
    transaction: &Transaction<'_>,
    agent_node: &AgentNode,
) -> Result<(), StoreError> {
    let provider = provider_text(agent_node.provider);
    if let Some(native_session_id) = &agent_node.native_session_id {
        transaction.execute(
            "INSERT INTO native_agent_sessions(provider, native_session_id) \
             VALUES (?1, ?2) \
             ON CONFLICT(provider, native_session_id) DO NOTHING",
            (provider, native_session_id),
        )?;
    }
    transaction.execute(
        "INSERT INTO agent_nodes(agent_node_id, provider, native_session_id, task_run_id) \
         VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT(agent_node_id) DO UPDATE SET \
             provider = excluded.provider, \
             native_session_id = excluded.native_session_id, \
             task_run_id = excluded.task_run_id",
        params![
            agent_node.agent_node_id,
            provider,
            agent_node.native_session_id,
            agent_node.task_run_id.to_string()
        ],
    )?;
    Ok(())
}

fn record_event(
    transaction: &Transaction<'_>,
    event: &NormalizedEvent,
    seen_at_ms: i64,
) -> Result<(), StoreError> {
    let (metadata, normalized_kind) = event_metadata(event);
    let inserted = insert_ledger(transaction, &metadata.event_id, seen_at_ms)?;
    if !inserted {
        return Ok(());
    }

    transaction.execute(
        "INSERT INTO herdr_sessions(session_name, updated_at_ms) VALUES (?1, ?2) \
         ON CONFLICT(session_name) DO UPDATE SET updated_at_ms = MAX(\
             herdr_sessions.updated_at_ms, excluded.updated_at_ms\
         )",
        (&metadata.herdr_session, seen_at_ms),
    )?;

    let provider_metadata = metadata.provider_metadata.as_ref();
    let item_count = provider_metadata
        .and_then(|value| value.item_count)
        .map(|value| signed_count("event.item_count", value))
        .transpose()?;
    let byte_count = provider_metadata
        .and_then(|value| value.byte_count)
        .map(|value| signed_count("event.byte_count", value))
        .transpose()?;
    let ingest_seq = metadata
        .ingest_seq
        .map(|value| {
            i64::try_from(value).map_err(|_| StoreError::IntegerOutOfRange {
                field: "event.ingest_seq",
            })
        })
        .transpose()?;
    transaction.execute(
        "INSERT INTO events(\
             event_id, seen_at_ms, event_timestamp_ms, herdr_session, source, normalized_kind, \
             source_event_type, workspace_id, tab_id, pane_id, terminal_id, provider, \
             native_session_id, task_run_id, agent_node_id, task_state, model_id, \
             provider_event_kind, tool_name, item_count, byte_count, gap_kind, ingest_seq\
         ) VALUES (\
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, \
             ?16, ?17, ?18, ?19, ?20, ?21, NULL, ?22\
         )",
        params![
            metadata.event_id,
            seen_at_ms,
            metadata.timestamp_ms,
            metadata.herdr_session,
            metadata.source,
            normalized_kind,
            metadata.source_event_type,
            metadata.workspace_id,
            metadata.tab_id,
            metadata.pane_id,
            metadata.terminal_id,
            metadata.provider.map(provider_text),
            metadata.native_session_id,
            metadata.task_run_id.map(|value| value.to_string()),
            metadata.agent_node_id,
            metadata.task_state.map(task_state_text),
            provider_metadata.and_then(|value| value.model_id.as_deref()),
            provider_metadata.and_then(|value| value.event_kind.as_deref()),
            provider_metadata.and_then(|value| value.tool_name.as_deref()),
            item_count,
            byte_count,
            ingest_seq
        ],
    )?;
    Ok(())
}

fn record_gap(transaction: &Transaction<'_>, gap: &CollectorGap) -> Result<(), StoreError> {
    if !insert_ledger(transaction, &gap.event_id, gap.seen_at_ms)? {
        return Ok(());
    }
    transaction.execute(
        "INSERT INTO herdr_sessions(session_name, updated_at_ms) VALUES (?1, ?2) \
         ON CONFLICT(session_name) DO UPDATE SET updated_at_ms = MAX(\
             herdr_sessions.updated_at_ms, excluded.updated_at_ms\
         )",
        (&gap.herdr_session, gap.seen_at_ms),
    )?;
    transaction.execute(
        "INSERT INTO events(\
             event_id, seen_at_ms, event_timestamp_ms, herdr_session, source, \
             normalized_kind, source_event_type, gap_kind\
         ) VALUES (\
             ?1, ?2, ?2, ?3, 'collector', 'collector_gap', 'observation_gap', ?4\
         )",
        (
            &gap.event_id,
            gap.seen_at_ms,
            &gap.herdr_session,
            gap_kind_text(gap.kind),
        ),
    )?;
    Ok(())
}

fn insert_ledger(
    transaction: &Transaction<'_>,
    event_id: &str,
    seen_at_ms: i64,
) -> Result<bool, StoreError> {
    Ok(transaction.execute(
        "INSERT OR IGNORE INTO event_ledger(event_id, seen_at_ms) VALUES (?1, ?2)",
        (event_id, seen_at_ms),
    )? == 1)
}

fn event_metadata(event: &NormalizedEvent) -> (&EventMetadata, &'static str) {
    match event {
        NormalizedEvent::ControllerEvent { metadata, .. } => (metadata, "controller_event"),
        NormalizedEvent::TopologyUpsert { metadata, .. } => (metadata, "topology_upsert"),
        NormalizedEvent::TopologyClosure { metadata, .. } => (metadata, "topology_closure"),
        NormalizedEvent::AgentStatusChanged { metadata, .. } => (metadata, "agent_status_changed"),
        NormalizedEvent::ExecutionBegin { metadata, .. } => (metadata, "execution_begin"),
        NormalizedEvent::ExecutionEnd { metadata, .. } => (metadata, "execution_end"),
    }
}

struct EncodedRunKey<'a> {
    kind: &'static str,
    controller_id: Option<&'a str>,
    provider: Option<&'static str>,
    native_sid: Option<&'a str>,
    native_path: Option<&'a str>,
    terminal_id: Option<&'a str>,
    start_ms: Option<i64>,
    seq: Option<String>,
}

impl<'a> From<&'a RunKey> for EncodedRunKey<'a> {
    fn from(key: &'a RunKey) -> Self {
        let mut encoded = Self {
            kind: "",
            controller_id: None,
            provider: None,
            native_sid: None,
            native_path: None,
            terminal_id: None,
            start_ms: None,
            seq: None,
        };
        match key {
            RunKey::Controller(controller_id) => {
                encoded.kind = "controller";
                encoded.controller_id = Some(controller_id);
            }
            RunKey::Native { provider, sid } => {
                encoded.kind = "native";
                encoded.provider = Some(provider_text(*provider));
                encoded.native_sid = Some(sid);
            }
            RunKey::NativePath { provider, path } => {
                encoded.kind = "native_path";
                encoded.provider = Some(provider_text(*provider));
                encoded.native_path = Some(path);
            }
            RunKey::Provisional {
                terminal_id,
                start_ms,
                seq,
            } => {
                encoded.kind = "provisional";
                encoded.terminal_id = Some(terminal_id);
                encoded.start_ms = Some(*start_ms);
                encoded.seq = Some(seq.to_string());
            }
        }
        encoded
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_run_key(
    kind: &str,
    controller_id: Option<String>,
    provider: Option<String>,
    native_sid: Option<String>,
    native_path: Option<String>,
    terminal_id: Option<String>,
    start_ms: Option<i64>,
    seq: Option<String>,
) -> Result<RunKey, StoreError> {
    match kind {
        "controller" => Ok(RunKey::Controller(required(
            "key_controller_id",
            controller_id,
        )?)),
        "native" => Ok(RunKey::Native {
            provider: parse_provider(&required("key_provider", provider)?)?,
            sid: required("key_native_sid", native_sid)?,
        }),
        "native_path" => Ok(RunKey::NativePath {
            provider: parse_provider(&required("key_provider", provider)?)?,
            path: required("key_native_path", native_path)?,
        }),
        "provisional" => {
            let seq = required("key_seq", seq)?;
            Ok(RunKey::Provisional {
                terminal_id: required("key_terminal_id", terminal_id)?,
                start_ms: start_ms.ok_or_else(|| {
                    StoreError::invalid("key_start_ms", "NULL", "required for provisional key")
                })?,
                seq: seq.parse().map_err(|source: std::num::ParseIntError| {
                    StoreError::invalid("key_seq", &seq, source.to_string())
                })?,
            })
        }
        value => Err(StoreError::invalid(
            "key_kind",
            value,
            "unknown run-key kind",
        )),
    }
}

fn required(field: &'static str, value: Option<String>) -> Result<String, StoreError> {
    value.ok_or_else(|| StoreError::invalid(field, "NULL", "required by the run-key kind"))
}

fn parse_run_id(value: &str) -> Result<RunId, StoreError> {
    RunId::parse(value).map_err(|source| StoreError::invalid("run_id", value, source.to_string()))
}

const fn provider_text(provider: Provider) -> &'static str {
    match provider {
        Provider::Claude => "claude",
        Provider::Codex => "codex",
    }
}

fn parse_provider(value: &str) -> Result<Provider, StoreError> {
    match value {
        "claude" => Ok(Provider::Claude),
        "codex" => Ok(Provider::Codex),
        _ => Err(StoreError::invalid("provider", value, "unknown provider")),
    }
}

const fn task_state_text(state: TaskState) -> &'static str {
    match state {
        TaskState::Queued => "queued",
        TaskState::Running => "running",
        TaskState::Blocked => "blocked",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
        TaskState::EndedUnknown => "ended_unknown",
    }
}

fn parse_task_state(value: &str) -> Result<TaskState, StoreError> {
    match value {
        "queued" => Ok(TaskState::Queued),
        "running" => Ok(TaskState::Running),
        "blocked" => Ok(TaskState::Blocked),
        "completed" => Ok(TaskState::Completed),
        "failed" => Ok(TaskState::Failed),
        "cancelled" => Ok(TaskState::Cancelled),
        "ended_unknown" => Ok(TaskState::EndedUnknown),
        _ => Err(StoreError::invalid(
            "task_state",
            value,
            "unknown task state",
        )),
    }
}

fn encode_exec_state(state: &ExecState) -> (&'static str, Option<i64>) {
    match state {
        ExecState::Unknown => ("unknown", None),
        ExecState::Idle => ("idle", None),
        ExecState::Working => ("working", None),
        ExecState::Blocked => ("blocked", None),
        ExecState::Stale { since_ms } => ("stale", Some(*since_ms)),
        ExecState::Ended => ("ended", None),
    }
}

fn parse_exec_state(value: &str, stale_since_ms: Option<i64>) -> Result<ExecState, StoreError> {
    match value {
        "unknown" => Ok(ExecState::Unknown),
        "idle" => Ok(ExecState::Idle),
        "working" => Ok(ExecState::Working),
        "blocked" => Ok(ExecState::Blocked),
        "stale" => Ok(ExecState::Stale {
            since_ms: stale_since_ms.ok_or_else(|| {
                StoreError::invalid("stale_since_ms", "NULL", "required for stale execution")
            })?,
        }),
        "ended" => Ok(ExecState::Ended),
        _ => Err(StoreError::invalid(
            "exec_state",
            value,
            "unknown execution state",
        )),
    }
}

const fn gap_kind_text(kind: GapKind) -> &'static str {
    match kind {
        GapKind::Startup => "startup",
        GapKind::Reconnect => "reconnect",
        GapKind::SocketReplacement => "socket_replacement",
    }
}

fn signed_count(field: &'static str, value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_: TryFromIntError| StoreError::IntegerOutOfRange { field })
}

fn changed(rows: usize) -> u64 {
    rows as u64
}

fn unix_now_ms() -> Result<i64, StoreError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| StoreError::Clock(source.to_string()))?;
    i64::try_from(elapsed.as_millis()).map_err(|source| StoreError::Clock(source.to_string()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use rusqlite::Connection;
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use super::*;
    use crate::model::TopologyEntityId;

    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

    #[test]
    fn absent_db_is_v0_created_without_backup() {
        let (directory, root) = test_root();

        assert_eq!(preflight_schema(&root).unwrap(), SchemaVerdict::Absent);
        let store = open_writer(&root).unwrap();

        assert!(database_path(&root).is_file());
        assert!(backup_files(directory.path()).is_empty());
        assert_eq!(schema_version(&store.connection), 2);
    }

    #[test]
    fn newer_schema_refused_readonly_bytes_untouched() {
        let (directory, root) = test_root();
        let database = database_path(&root);
        {
            let connection = Connection::open(&database).unwrap();
            connection
                .pragma_update(None, "journal_mode", "DELETE")
                .unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE schema_migrations(\
                         version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL\
                     );\
                     INSERT INTO schema_migrations(version, applied_at_ms) VALUES (3, 0);",
                )
                .unwrap();
        }
        let before = sha256(&database);

        assert!(matches!(
            preflight_schema(&root),
            Err(StoreError::NewerSchema {
                found: 3,
                supported: 2
            })
        ));
        assert!(matches!(
            open_writer(&root),
            Err(StoreError::NewerSchema {
                found: 3,
                supported: 2
            })
        ));

        assert_eq!(sha256(&database), before);
        assert!(!sidecar_path(&database, "-wal").exists());
        assert!(!sidecar_path(&database, "-shm").exists());
        assert!(backup_files(directory.path()).is_empty());
    }

    #[test]
    fn backup_exists_before_migration() {
        let (directory, root) = test_root();
        let database = database_path(&root);
        {
            let connection = Connection::open(&database).unwrap();
            connection
                .pragma_update(None, "journal_mode", "DELETE")
                .unwrap();
            connection
                .execute_batch("CREATE TABLE legacy_marker(value TEXT);")
                .unwrap();
        }
        assert_eq!(preflight_schema(&root).unwrap(), SchemaVerdict::Migratable);

        let store = open_writer(&root).unwrap();
        let backups = backup_files(directory.path());

        assert_eq!(backups.len(), 1);
        assert_eq!(schema_version(&store.connection), 2);
        let backup = Connection::open_with_flags(
            &backups[0],
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        let has_legacy: bool = backup
            .query_row(
                "SELECT EXISTS(\
                     SELECT 1 FROM sqlite_schema \
                     WHERE type = 'table' AND name = 'legacy_marker'\
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let has_migration_table: bool = backup
            .query_row(
                "SELECT EXISTS(\
                     SELECT 1 FROM sqlite_schema \
                     WHERE type = 'table' AND name = 'schema_migrations'\
                 )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_legacy);
        assert!(!has_migration_table);
    }

    #[test]
    fn schema_v1_to_v2_migration() {
        let (_directory, root) = test_root();
        let database = database_path(&root);
        {
            let connection = Connection::open(&database).unwrap();
            connection.execute_batch(schema::SCHEMA_V1).unwrap();
            connection
                .execute(
                    "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (1, 1)",
                    [],
                )
                .unwrap();
        }

        assert_eq!(preflight_schema(&root).unwrap(), SchemaVerdict::Migratable);
        let store = open_writer(&root).unwrap();
        assert_eq!(schema_version(&store.connection), 2);
        let has_ingest_seq: bool = store
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('events') WHERE name = 'ingest_seq')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let high_water: i64 = store
            .connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'ingest_seq_high_water'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_ingest_seq);
        assert_eq!(high_water, 0);
        assert_eq!(
            store.load_restored_state().unwrap().next_ingest_seq,
            Some(1)
        );
    }

    #[test]
    fn ingest_seq_seeds_from_restore() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![PersistOp::AdvanceIngestSequence { ingest_seq: 41 }])
            .unwrap();

        assert_eq!(
            store.load_restored_state().unwrap().next_ingest_seq,
            Some(42)
        );
    }

    #[test]
    fn ingest_seq_high_water_survives_event_retention() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let mut event = old_topology_event(now - 8 * DAY_MS);
        let NormalizedEvent::TopologyClosure { metadata, .. } = &mut event else {
            panic!("fixture must be a topology closure");
        };
        metadata.ingest_seq = Some(77);
        store
            .apply_batch(vec![
                PersistOp::AdvanceIngestSequence { ingest_seq: 77 },
                PersistOp::RecordEvent {
                    event: Box::new(event),
                    seen_at_ms: now - 8 * DAY_MS,
                },
            ])
            .unwrap();

        let cleanup = store.cleanup_retention(now).unwrap();
        let restored = store.load_restored_state().unwrap();
        assert_eq!(cleanup.ledger_pruned, 1);
        assert_eq!(restored.next_ingest_seq, Some(78));
        assert!(restored.event_ledger.is_empty());
    }

    #[test]
    fn ring_cap_100k_with_ledger_survival() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let transaction = store.connection.transaction().unwrap();
        {
            let mut event_statement = transaction
                .prepare_cached(
                    "INSERT INTO events(\
                         event_id, seen_at_ms, event_timestamp_ms, herdr_session, source, \
                         normalized_kind, source_event_type\
                     ) VALUES (?1, ?2, ?2, 'session-a', 'test', 'test', 'test')",
                )
                .unwrap();
            let mut ledger_statement = transaction
                .prepare_cached("INSERT INTO event_ledger(event_id, seen_at_ms) VALUES (?1, ?2)")
                .unwrap();
            for index in 0..100_050 {
                let event_id = format!("event-{index:06}");
                event_statement.execute((&event_id, now)).unwrap();
                ledger_statement.execute((&event_id, now)).unwrap();
            }
        }
        transaction.commit().unwrap();

        let stats = store.cleanup_retention(now).unwrap();

        assert_eq!(stats.events_evicted, 50);
        assert_eq!(count(&store.connection, "events"), 100_000);
        assert_eq!(count(&store.connection, "event_ledger"), 100_050);
    }

    #[test]
    fn finished_runs_pruned_after_30d() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let stale = RunId::new();
        let recent = RunId::new();
        store
            .apply_batch(vec![
                run_op(stale, 1, TaskState::Completed, now - 31 * DAY_MS, false),
                run_op(recent, 2, TaskState::Failed, now - 29 * DAY_MS, false),
            ])
            .unwrap();

        let stats = store.cleanup_retention(now).unwrap();
        let restored = store.load_restored_state().unwrap();

        assert_eq!(stats.runs_pruned, 1);
        assert!(restored.model.task_run(&stale).is_none());
        assert!(restored.model.task_run(&recent).is_some());
    }

    #[test]
    fn referenced_parents_not_pruned() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let parent = RunId::new();
        let prerequisite = RunId::new();
        let unreferenced = RunId::new();
        let active = RunId::new();
        store
            .apply_batch(vec![
                run_op(parent, 1, TaskState::Completed, now - 31 * DAY_MS, false),
                run_op(
                    prerequisite,
                    2,
                    TaskState::Completed,
                    now - 31 * DAY_MS,
                    false,
                ),
                run_op(
                    unreferenced,
                    3,
                    TaskState::Completed,
                    now - 31 * DAY_MS,
                    false,
                ),
                run_op(active, 4, TaskState::Running, now - 90 * DAY_MS, false),
                PersistOp::UpsertExecutionEdge {
                    edge: ExecutionEdge {
                        parent_run_id: parent,
                        child_run_id: active,
                    },
                    created_at_ms: now - 31 * DAY_MS,
                },
                PersistOp::UpsertDependencyEdge {
                    edge: DependencyEdge {
                        prerequisite_run_id: prerequisite,
                        dependent_run_id: active,
                    },
                    created_at_ms: now - 31 * DAY_MS,
                },
            ])
            .unwrap();

        let stats = store.cleanup_retention(now).unwrap();
        let restored = store.load_restored_state().unwrap();

        assert_eq!(stats.runs_pruned, 1);
        assert!(restored.model.task_run(&parent).is_some());
        assert!(restored.model.task_run(&prerequisite).is_some());
        assert!(restored.model.task_run(&active).is_some());
        assert!(restored.model.task_run(&unreferenced).is_none());
    }

    #[test]
    fn restored_state_carries_model_and_next_ordinal() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let parent = RunId::new();
        let child = RunId::new();
        store
            .apply_batch(vec![
                PersistOp::UpsertWorkspace(Workspace {
                    workspace_id: "workspace-1".to_owned(),
                }),
                PersistOp::UpsertTab(Tab {
                    tab_id: "tab-1".to_owned(),
                    workspace_id: "workspace-1".to_owned(),
                }),
                PersistOp::UpsertPane(Pane {
                    pane_id: "pane-1".to_owned(),
                    workspace_id: "workspace-1".to_owned(),
                    tab_id: "tab-1".to_owned(),
                    terminal_id: "terminal-1".to_owned(),
                }),
                run_op(parent, 3, TaskState::Running, now, false),
                run_op(child, 7, TaskState::Blocked, now, false),
                PersistOp::UpsertExecution(PersistExecution {
                    execution: Execution {
                        execution_id: "execution-1".to_owned(),
                        pane_id: "pane-1".to_owned(),
                        terminal_id: "terminal-1".to_owned(),
                        task_run_id: child,
                        state: ExecState::Stale { since_ms: now - 10 },
                    },
                    started_at_ms: now - 100,
                    updated_at_ms: now,
                    ended_at_ms: None,
                }),
                PersistOp::UpsertAgentNode(AgentNode {
                    agent_node_id: "agent-1".to_owned(),
                    provider: Provider::Codex,
                    native_session_id: Some("native-1".to_owned()),
                    task_run_id: child,
                }),
                PersistOp::UpsertExecutionEdge {
                    edge: ExecutionEdge {
                        parent_run_id: parent,
                        child_run_id: child,
                    },
                    created_at_ms: now,
                },
                PersistOp::UpsertDependencyEdge {
                    edge: DependencyEdge {
                        prerequisite_run_id: parent,
                        dependent_run_id: child,
                    },
                    created_at_ms: now,
                },
            ])
            .unwrap();
        drop(store);

        let reader = open_reader(&root).unwrap();
        let restored = reader.load_restored_state().unwrap();

        assert_eq!(restored.next_ordinal, 8);
        assert!(restored.model.workspace("workspace-1").is_some());
        assert!(restored.model.tab("tab-1").is_some());
        assert!(restored.model.pane("pane-1").is_some());
        assert_eq!(restored.model.task_runs().count(), 2);
        assert_eq!(restored.model.executions().count(), 1);
        assert_eq!(restored.model.agent_nodes().count(), 1);
        assert_eq!(restored.model.execution_edges().count(), 1);
        assert_eq!(restored.model.dependency_edges().count(), 1);
    }

    #[test]
    fn merge_op_moves_native_binding_atomically() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        store
            .apply_batch(vec![
                run_op(survivor, 1, TaskState::Running, now, false),
                run_op_with_key(
                    absorbed,
                    RunKey::Native {
                        provider: Provider::Codex,
                        sid: "native-1".to_owned(),
                    },
                    2,
                    TaskState::Running,
                    now,
                    false,
                    Some(NativeSessionBinding {
                        provider: Provider::Codex,
                        native_session_id: "native-1".to_owned(),
                    }),
                ),
                PersistOp::MergeTaskRuns { survivor, absorbed },
            ])
            .unwrap();

        assert_eq!(
            binding(&store.connection, survivor),
            codex_binding("native-1")
        );
        assert_eq!(binding(&store.connection, absorbed), (None, None));
        assert_eq!(merged_into(&store.connection, absorbed), Some(survivor));

        let bound_survivor = RunId::new();
        let unbound_absorbed = RunId::new();
        store
            .apply_batch(vec![
                run_op_with_key(
                    bound_survivor,
                    RunKey::Controller("bound-survivor".to_owned()),
                    3,
                    TaskState::Running,
                    now,
                    true,
                    Some(NativeSessionBinding {
                        provider: Provider::Codex,
                        native_session_id: "native-2".to_owned(),
                    }),
                ),
                run_op(unbound_absorbed, 4, TaskState::Running, now, false),
                PersistOp::MergeTaskRuns {
                    survivor: bound_survivor,
                    absorbed: unbound_absorbed,
                },
            ])
            .unwrap();
        assert_eq!(
            binding(&store.connection, bound_survivor),
            codex_binding("native-2")
        );
        assert_eq!(binding(&store.connection, unbound_absorbed), (None, None));
    }

    #[test]
    fn merge_op_repoints_all_references_and_dedups_edges() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let parent = RunId::new();
        let dependent = RunId::new();
        store
            .apply_batch(vec![
                PersistOp::UpsertWorkspace(Workspace {
                    workspace_id: "workspace-1".to_owned(),
                }),
                PersistOp::UpsertTab(Tab {
                    tab_id: "tab-1".to_owned(),
                    workspace_id: "workspace-1".to_owned(),
                }),
                PersistOp::UpsertPane(Pane {
                    pane_id: "pane-1".to_owned(),
                    workspace_id: "workspace-1".to_owned(),
                    tab_id: "tab-1".to_owned(),
                    terminal_id: "terminal-1".to_owned(),
                }),
                run_op(survivor, 1, TaskState::Running, now, false),
                run_op(absorbed, 2, TaskState::Running, now, false),
                run_op(parent, 3, TaskState::Running, now, false),
                run_op(dependent, 4, TaskState::Running, now, false),
                PersistOp::UpsertExecution(PersistExecution {
                    execution: Execution {
                        execution_id: "execution-1".to_owned(),
                        pane_id: "pane-1".to_owned(),
                        terminal_id: "terminal-1".to_owned(),
                        task_run_id: absorbed,
                        state: ExecState::Working,
                    },
                    started_at_ms: now,
                    updated_at_ms: now,
                    ended_at_ms: None,
                }),
                PersistOp::UpsertAgentNode(AgentNode {
                    agent_node_id: "agent-1".to_owned(),
                    provider: Provider::Codex,
                    native_session_id: Some("native-1".to_owned()),
                    task_run_id: absorbed,
                }),
                PersistOp::RecordEvent {
                    event: Box::new(run_event("event-1", absorbed, now)),
                    seen_at_ms: now,
                },
                PersistOp::UpsertExecutionEdge {
                    edge: ExecutionEdge {
                        parent_run_id: parent,
                        child_run_id: absorbed,
                    },
                    created_at_ms: now - 10,
                },
                PersistOp::UpsertExecutionEdge {
                    edge: ExecutionEdge {
                        parent_run_id: parent,
                        child_run_id: survivor,
                    },
                    created_at_ms: now,
                },
                PersistOp::UpsertDependencyEdge {
                    edge: DependencyEdge {
                        prerequisite_run_id: absorbed,
                        dependent_run_id: dependent,
                    },
                    created_at_ms: now - 10,
                },
                PersistOp::UpsertDependencyEdge {
                    edge: DependencyEdge {
                        prerequisite_run_id: survivor,
                        dependent_run_id: dependent,
                    },
                    created_at_ms: now,
                },
                PersistOp::MergeTaskRuns { survivor, absorbed },
            ])
            .unwrap();

        assert_eq!(
            referenced_run(&store.connection, "executions", "execution-1"),
            survivor
        );
        assert_eq!(
            referenced_run(&store.connection, "agent_nodes", "agent-1"),
            survivor
        );
        assert_eq!(
            referenced_run(&store.connection, "events", "event-1"),
            survivor
        );
        assert_eq!(count(&store.connection, "execution_edges"), 1);
        assert_eq!(count(&store.connection, "dependency_edges"), 1);
        assert_eq!(
            execution_edge(&store.connection),
            (parent, survivor, now - 10)
        );
        assert_eq!(
            dependency_edge(&store.connection),
            (survivor, dependent, now - 10)
        );
    }

    #[test]
    fn merged_rows_restore_as_aliases_not_runs() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let alias_key = RunKey::NativePath {
            provider: Provider::Claude,
            path: "/sessions/pending.jsonl".to_owned(),
        };
        store
            .apply_batch(vec![
                run_op(survivor, 1, TaskState::Running, now, true),
                run_op_with_key(
                    absorbed,
                    alias_key.clone(),
                    2,
                    TaskState::Running,
                    now,
                    false,
                    None,
                ),
                PersistOp::MergeTaskRuns { survivor, absorbed },
            ])
            .unwrap();

        let restored = store.load_restored_state().unwrap();

        assert_eq!(restored.next_ordinal, 3);
        assert_eq!(restored.model.task_runs().count(), 1);
        assert!(restored.model.task_run(&absorbed).is_none());
        assert_eq!(
            restored.model.task_run_by_key(&alias_key).unwrap().run_id,
            survivor
        );
    }

    #[test]
    fn merge_op_sets_merged_into_and_preserves_alias_key() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let alias_key = RunKey::Native {
            provider: Provider::Codex,
            sid: "native-alias".to_owned(),
        };
        store
            .apply_batch(vec![
                run_op(survivor, 1, TaskState::Running, now, true),
                run_op_with_key(absorbed, alias_key, 2, TaskState::Running, now, false, None),
                PersistOp::MergeTaskRuns { survivor, absorbed },
            ])
            .unwrap();

        let (kind, provider, native_sid, target): (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = store
            .connection
            .query_row(
                "SELECT key_kind, key_provider, key_native_sid, merged_into \
                 FROM task_runs WHERE run_id = ?1",
                [absorbed.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(kind, "native");
        assert_eq!(provider.as_deref(), Some("codex"));
        assert_eq!(native_sid.as_deref(), Some("native-alias"));
        assert_eq!(target, Some(survivor.to_string()));
    }

    #[test]
    fn k3_keys_never_resolve_after_restore() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let survivor = RunId::new();
        let historical = RunId::new();
        let absorbed = RunId::new();
        let provisional_key = RunKey::Provisional {
            terminal_id: "terminal-1".to_owned(),
            start_ms: now,
            seq: 1,
        };
        store
            .apply_batch(vec![
                run_op(survivor, 1, TaskState::Running, now, true),
                run_op_with_key(
                    historical,
                    provisional_key.clone(),
                    2,
                    TaskState::EndedUnknown,
                    now,
                    false,
                    None,
                ),
                run_op_with_key(
                    absorbed,
                    provisional_key.clone(),
                    3,
                    TaskState::Running,
                    now,
                    false,
                    None,
                ),
                PersistOp::MergeTaskRuns { survivor, absorbed },
            ])
            .unwrap();

        let restored = store.load_restored_state().unwrap();

        assert!(restored.model.task_run(&historical).is_some());
        assert!(restored.model.task_run(&absorbed).is_none());
        assert!(restored.model.task_run_by_key(&provisional_key).is_none());
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM pragma_index_list('task_runs') \
                     WHERE name = 'task_runs_provisional_key_idx' AND \"unique\" = 0",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn alias_chains_stay_canonical_flat() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let first_root = RunId::new();
        let first_alias = RunId::new();
        let final_root = RunId::new();
        store
            .apply_batch(vec![
                run_op(first_root, 1, TaskState::Running, now, false),
                run_op(first_alias, 2, TaskState::Running, now, false),
                run_op(final_root, 3, TaskState::Running, now, false),
                PersistOp::MergeTaskRuns {
                    survivor: first_root,
                    absorbed: first_alias,
                },
                PersistOp::MergeTaskRuns {
                    survivor: final_root,
                    absorbed: first_root,
                },
            ])
            .unwrap();

        assert_eq!(
            merged_into(&store.connection, first_alias),
            Some(final_root)
        );
        assert_eq!(merged_into(&store.connection, first_root), Some(final_root));
        assert_eq!(merged_into(&store.connection, final_root), None);

        assert!(matches!(
            store.apply_batch(vec![PersistOp::MergeTaskRuns {
                survivor: final_root,
                absorbed: first_alias,
            }]),
            Err(StoreError::InvalidData { .. })
        ));
        assert_eq!(
            merged_into(&store.connection, first_alias),
            Some(final_root)
        );
    }

    #[test]
    fn restore_defensively_resolves_alias_chains_and_rejects_cycles() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let canonical = RunId::new();
        let middle = RunId::new();
        let leaf = RunId::new();
        let leaf_key = RunKey::Controller(format!("controller-{leaf}"));
        store
            .apply_batch(vec![
                run_op(canonical, 1, TaskState::Running, now, false),
                run_op(middle, 2, TaskState::Running, now, false),
                run_op(leaf, 3, TaskState::Running, now, false),
                PersistOp::MergeTaskRuns {
                    survivor: canonical,
                    absorbed: middle,
                },
                PersistOp::MergeTaskRuns {
                    survivor: canonical,
                    absorbed: leaf,
                },
            ])
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE task_runs SET merged_into = ?1 WHERE run_id = ?2",
                (middle.to_string(), leaf.to_string()),
            )
            .unwrap();

        let restored = store.load_restored_state().unwrap();
        assert_eq!(
            restored.model.task_run_by_key(&leaf_key).unwrap().run_id,
            canonical
        );

        store
            .connection
            .execute(
                "UPDATE task_runs SET merged_into = ?1 WHERE run_id = ?2",
                (leaf.to_string(), middle.to_string()),
            )
            .unwrap();
        assert!(matches!(
            store.load_restored_state(),
            Err(StoreError::InvalidData { .. })
        ));
    }

    #[test]
    fn binding_overlap_is_store_error() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        store
            .apply_batch(vec![
                run_op_with_key(
                    survivor,
                    RunKey::Controller("survivor".to_owned()),
                    1,
                    TaskState::Running,
                    now,
                    true,
                    Some(NativeSessionBinding {
                        provider: Provider::Codex,
                        native_session_id: "native-x".to_owned(),
                    }),
                ),
                run_op_with_key(
                    absorbed,
                    RunKey::Controller("absorbed".to_owned()),
                    2,
                    TaskState::Running,
                    now,
                    true,
                    Some(NativeSessionBinding {
                        provider: Provider::Codex,
                        native_session_id: "native-y".to_owned(),
                    }),
                ),
            ])
            .unwrap();

        let error = store.apply_batch(vec![
            PersistOp::UpsertWorkspace(Workspace {
                workspace_id: "must-roll-back".to_owned(),
            }),
            PersistOp::MergeTaskRuns { survivor, absorbed },
        ]);

        assert!(matches!(error, Err(StoreError::InvalidData { .. })));
        assert_eq!(
            binding(&store.connection, survivor),
            codex_binding("native-x")
        );
        assert_eq!(
            binding(&store.connection, absorbed),
            codex_binding("native-y")
        );
        assert_eq!(merged_into(&store.connection, absorbed), None);
        assert_eq!(count(&store.connection, "workspaces"), 0);
    }

    #[test]
    fn repoint_dedup_preserves_oldest_edge() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let parent = RunId::new();
        let dependent = RunId::new();
        store
            .apply_batch(vec![
                run_op(survivor, 1, TaskState::Running, now, false),
                run_op(absorbed, 2, TaskState::Running, now, false),
                run_op(parent, 3, TaskState::Running, now, false),
                run_op(dependent, 4, TaskState::Running, now, false),
                PersistOp::UpsertExecutionEdge {
                    edge: ExecutionEdge {
                        parent_run_id: parent,
                        child_run_id: survivor,
                    },
                    created_at_ms: now,
                },
                PersistOp::UpsertExecutionEdge {
                    edge: ExecutionEdge {
                        parent_run_id: parent,
                        child_run_id: absorbed,
                    },
                    created_at_ms: now - 20,
                },
                PersistOp::UpsertDependencyEdge {
                    edge: DependencyEdge {
                        prerequisite_run_id: survivor,
                        dependent_run_id: dependent,
                    },
                    created_at_ms: now - 30,
                },
                PersistOp::UpsertDependencyEdge {
                    edge: DependencyEdge {
                        prerequisite_run_id: absorbed,
                        dependent_run_id: dependent,
                    },
                    created_at_ms: now,
                },
                PersistOp::MergeTaskRuns { survivor, absorbed },
            ])
            .unwrap();

        assert_eq!(execution_edge(&store.connection).2, now - 20);
        assert_eq!(dependency_edge(&store.connection).2, now - 30);
    }

    #[test]
    fn merge_op_rejects_invalid_substituted_edges() {
        let now = unix_now_ms().unwrap();
        {
            let (_directory, root) = test_root();
            let mut store = open_writer(&root).unwrap();
            let survivor = RunId::new();
            let absorbed = RunId::new();
            store
                .apply_batch(vec![
                    run_op(survivor, 1, TaskState::Running, now, false),
                    run_op(absorbed, 2, TaskState::Running, now, false),
                    PersistOp::UpsertExecutionEdge {
                        edge: ExecutionEdge {
                            parent_run_id: absorbed,
                            child_run_id: survivor,
                        },
                        created_at_ms: now,
                    },
                ])
                .unwrap();

            assert!(matches!(
                store.apply_batch(vec![PersistOp::MergeTaskRuns { survivor, absorbed }]),
                Err(StoreError::InvalidData { .. })
            ));
            assert_eq!(merged_into(&store.connection, absorbed), None);
            assert_eq!(count(&store.connection, "execution_edges"), 1);
        }

        {
            let (_directory, root) = test_root();
            let mut store = open_writer(&root).unwrap();
            let survivor = RunId::new();
            let absorbed = RunId::new();
            store
                .apply_batch(vec![
                    run_op(survivor, 1, TaskState::Running, now, false),
                    run_op(absorbed, 2, TaskState::Running, now, false),
                    PersistOp::UpsertDependencyEdge {
                        edge: DependencyEdge {
                            prerequisite_run_id: survivor,
                            dependent_run_id: absorbed,
                        },
                        created_at_ms: now,
                    },
                ])
                .unwrap();

            assert!(matches!(
                store.apply_batch(vec![PersistOp::MergeTaskRuns { survivor, absorbed }]),
                Err(StoreError::InvalidData { .. })
            ));
            assert_eq!(merged_into(&store.connection, absorbed), None);
            assert_eq!(count(&store.connection, "dependency_edges"), 1);
        }

        {
            let (_directory, root) = test_root();
            let mut store = open_writer(&root).unwrap();
            let survivor = RunId::new();
            let absorbed = RunId::new();
            let first_parent = RunId::new();
            let second_parent = RunId::new();
            store
                .apply_batch(vec![
                    run_op(survivor, 1, TaskState::Running, now, false),
                    run_op(absorbed, 2, TaskState::Running, now, false),
                    run_op(first_parent, 3, TaskState::Running, now, false),
                    run_op(second_parent, 4, TaskState::Running, now, false),
                    PersistOp::UpsertExecutionEdge {
                        edge: ExecutionEdge {
                            parent_run_id: first_parent,
                            child_run_id: survivor,
                        },
                        created_at_ms: now,
                    },
                    PersistOp::UpsertExecutionEdge {
                        edge: ExecutionEdge {
                            parent_run_id: second_parent,
                            child_run_id: absorbed,
                        },
                        created_at_ms: now,
                    },
                ])
                .unwrap();

            assert!(matches!(
                store.apply_batch(vec![PersistOp::MergeTaskRuns { survivor, absorbed }]),
                Err(StoreError::InvalidData { .. })
            ));
            assert_eq!(merged_into(&store.connection, absorbed), None);
            assert_eq!(count(&store.connection, "execution_edges"), 2);
        }
    }

    #[test]
    fn alias_pruned_only_with_canonical_root() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let root_run = RunId::new();
        let alias = RunId::new();
        store
            .apply_batch(vec![
                run_op(root_run, 1, TaskState::Running, now, false),
                run_op(alias, 2, TaskState::Completed, now - 31 * DAY_MS, false),
                PersistOp::MergeTaskRuns {
                    survivor: root_run,
                    absorbed: alias,
                },
            ])
            .unwrap();

        store.cleanup_retention(now).unwrap();
        assert_eq!(count(&store.connection, "task_runs"), 2);
        assert_eq!(count(&store.connection, "display_ordinals"), 2);

        store
            .apply_batch(vec![run_op(
                root_run,
                1,
                TaskState::Completed,
                now - 31 * DAY_MS,
                false,
            )])
            .unwrap();
        let stats = store.cleanup_retention(now).unwrap();

        assert_eq!(stats.runs_pruned, 2);
        assert_eq!(count(&store.connection, "task_runs"), 0);
        assert_eq!(count(&store.connection, "display_ordinals"), 0);
    }

    #[test]
    fn upsert_rejected_on_merged_row() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        let now = unix_now_ms().unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        store
            .apply_batch(vec![
                run_op(survivor, 1, TaskState::Running, now, false),
                run_op(absorbed, 2, TaskState::Running, now, false),
                PersistOp::MergeTaskRuns { survivor, absorbed },
            ])
            .unwrap();

        let error = store.apply_batch(vec![run_op(
            absorbed,
            2,
            TaskState::Completed,
            now + 1,
            false,
        )]);

        assert!(matches!(error, Err(StoreError::InvalidData { .. })));
        assert_eq!(merged_into(&store.connection, absorbed), Some(survivor));
        assert_eq!(task_state(&store.connection, absorbed), "running");
    }

    #[test]
    fn owner_replace_read_and_location_update() {
        let (_directory, root) = test_root();
        let mut store = open_writer(&root).unwrap();
        assert_eq!(store.read_owner().unwrap(), None);
        let owner = OwnerRecord {
            pid: 42,
            started_at_ms: 123_456,
            terminal_id: Some("terminal-old".to_owned()),
            pane_id: Some("pane-old".to_owned()),
        };

        store.replace_owner(&owner).unwrap();
        assert_eq!(store.read_owner().unwrap(), Some(owner));
        store
            .update_owner_location("terminal-new", "pane-new")
            .unwrap();

        assert_eq!(
            store.read_owner().unwrap(),
            Some(OwnerRecord {
                pid: 42,
                started_at_ms: 123_456,
                terminal_id: Some("terminal-new".to_owned()),
                pane_id: Some("pane-new".to_owned()),
            })
        );
    }

    #[test]
    fn controller_flag_persists_across_reopen() {
        let (_directory, root) = test_root();
        let run_id = RunId::new();
        let now = unix_now_ms().unwrap();
        {
            let mut store = open_writer(&root).unwrap();
            store
                .apply_batch(vec![run_op(run_id, 1, TaskState::Running, now, true)])
                .unwrap();
        }

        let store = open_writer(&root).unwrap();
        let restored = store.load_restored_state().unwrap();

        assert!(
            restored
                .model
                .task_run(&run_id)
                .unwrap()
                .has_controller_task_state_event
        );
    }

    #[test]
    fn startup_cleanup_precedes_restore() {
        let (_directory, root) = test_root();
        let stale_run = RunId::new();
        let now = unix_now_ms().unwrap();
        {
            let mut store = open_writer(&root).unwrap();
            store
                .apply_batch(vec![
                    run_op(
                        stale_run,
                        1,
                        TaskState::EndedUnknown,
                        now - 31 * DAY_MS,
                        false,
                    ),
                    PersistOp::RecordEvent {
                        event: Box::new(old_topology_event(now - 8 * DAY_MS)),
                        seen_at_ms: now - 8 * DAY_MS,
                    },
                ])
                .unwrap();
            assert_eq!(count(&store.connection, "events"), 1);
        }

        let store = open_writer(&root).unwrap();
        let restored = store.load_restored_state().unwrap();

        assert!(restored.model.task_run(&stale_run).is_none());
        assert_eq!(count(&store.connection, "events"), 0);
        assert_eq!(count(&store.connection, "event_ledger"), 0);
    }

    fn test_root() -> (TempDir, StateRoot) {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        (directory, root)
    }

    fn run_op(
        run_id: RunId,
        ordinal: i64,
        state: TaskState,
        state_at_ms: i64,
        controller_flag: bool,
    ) -> PersistOp {
        run_op_with_key(
            run_id,
            RunKey::Controller(format!("controller-{run_id}")),
            ordinal,
            state,
            state_at_ms,
            controller_flag,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_op_with_key(
        run_id: RunId,
        key: RunKey,
        ordinal: i64,
        state: TaskState,
        state_at_ms: i64,
        controller_flag: bool,
        native_session: Option<NativeSessionBinding>,
    ) -> PersistOp {
        PersistOp::UpsertTaskRun(PersistTaskRun {
            task_run: TaskRun {
                run_id,
                key,
                display_ordinal: DisplayOrdinal::new(ordinal),
                state,
                has_controller_task_state_event: controller_flag,
            },
            native_session,
            created_at_ms: state_at_ms,
            updated_at_ms: state_at_ms,
            finished_at_ms: state.is_terminal().then_some(state_at_ms),
        })
    }

    fn old_topology_event(timestamp_ms: i64) -> NormalizedEvent {
        NormalizedEvent::TopologyClosure {
            metadata: EventMetadata {
                event_id: "old-event".to_owned(),
                timestamp_ms,
                receipt_time_ms: timestamp_ms,
                source: "herdr".to_owned(),
                source_event_type: "workspace.closed".to_owned(),
                herdr_session: "session-a".to_owned(),
                workspace_id: Some("old-workspace".to_owned()),
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
            entity: TopologyEntityId::Workspace {
                workspace_id: "old-workspace".to_owned(),
            },
        }
    }

    fn run_event(event_id: &str, task_run_id: RunId, timestamp_ms: i64) -> NormalizedEvent {
        NormalizedEvent::ExecutionEnd {
            metadata: EventMetadata {
                event_id: event_id.to_owned(),
                timestamp_ms,
                receipt_time_ms: timestamp_ms,
                source: "test".to_owned(),
                source_event_type: "execution.end".to_owned(),
                herdr_session: "session-a".to_owned(),
                workspace_id: None,
                tab_id: None,
                pane_id: None,
                terminal_id: None,
                provider: None,
                native_session_id: None,
                task_run_id: Some(task_run_id),
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
            execution_id: "execution-1".to_owned(),
        }
    }

    fn binding(connection: &Connection, run_id: RunId) -> (Option<String>, Option<String>) {
        connection
            .query_row(
                "SELECT native_provider, native_session_id FROM task_runs WHERE run_id = ?1",
                [run_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn codex_binding(native_session_id: &str) -> (Option<String>, Option<String>) {
        (Some("codex".to_owned()), Some(native_session_id.to_owned()))
    }

    fn merged_into(connection: &Connection, run_id: RunId) -> Option<RunId> {
        let value: Option<String> = connection
            .query_row(
                "SELECT merged_into FROM task_runs WHERE run_id = ?1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        value.map(|target| RunId::parse(&target).unwrap())
    }

    fn referenced_run(connection: &Connection, table: &str, entity_id: &str) -> RunId {
        let query = match table {
            "executions" => "SELECT task_run_id FROM executions WHERE execution_id = ?1",
            "agent_nodes" => "SELECT task_run_id FROM agent_nodes WHERE agent_node_id = ?1",
            "events" => "SELECT task_run_id FROM events WHERE event_id = ?1",
            _ => panic!("unsupported reference table {table}"),
        };
        let value: String = connection
            .query_row(query, [entity_id], |row| row.get(0))
            .unwrap();
        RunId::parse(&value).unwrap()
    }

    fn execution_edge(connection: &Connection) -> (RunId, RunId, i64) {
        let (parent, child, created_at_ms): (String, String, i64) = connection
            .query_row(
                "SELECT parent_run_id, child_run_id, created_at_ms FROM execution_edges",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        (
            RunId::parse(&parent).unwrap(),
            RunId::parse(&child).unwrap(),
            created_at_ms,
        )
    }

    fn dependency_edge(connection: &Connection) -> (RunId, RunId, i64) {
        let (prerequisite, dependent, created_at_ms): (String, String, i64) = connection
            .query_row(
                "SELECT prerequisite_run_id, dependent_run_id, created_at_ms \
                 FROM dependency_edges",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        (
            RunId::parse(&prerequisite).unwrap(),
            RunId::parse(&dependent).unwrap(),
            created_at_ms,
        )
    }

    fn task_state(connection: &Connection, run_id: RunId) -> String {
        connection
            .query_row(
                "SELECT task_state FROM task_runs WHERE run_id = ?1",
                [run_id.to_string()],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn backup_files(directory: &Path) -> Vec<PathBuf> {
        let mut files: Vec<_> = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".backup-"))
            })
            .collect();
        files.sort();
        files
    }

    fn sidecar_path(database: &Path, suffix: &str) -> PathBuf {
        PathBuf::from(format!("{}{suffix}", database.display()))
    }

    fn sha256(path: &Path) -> Vec<u8> {
        Sha256::digest(fs::read(path).unwrap()).to_vec()
    }

    fn schema_version(connection: &Connection) -> i64 {
        connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .unwrap()
            .unwrap()
    }

    fn count(connection: &Connection, table: &str) -> i64 {
        connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }
}
