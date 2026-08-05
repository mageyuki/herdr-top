//! SQLite schema v1, read-only preflight, online backup, and migration support.

use std::fs::{self, OpenOptions, Permissions};
use std::io;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, MAIN_DB, OpenFlags};

use super::StoreError;
use crate::lockfile::StateRoot;

pub(super) const CURRENT_SCHEMA_VERSION: i64 = 1;
pub(super) const DATABASE_FILE: &str = "herdr-top.sqlite3";
const FILE_MODE: u32 = 0o600;

/// The result of checking an on-disk database against this binary's schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaVerdict {
    /// No database file exists. This is schema v0 and needs no backup.
    Absent,
    /// The database is already at this binary's schema version.
    Current,
    /// The database exists at an older schema version and can be migrated.
    Migratable,
}

/// Returns the path to the session's SQLite database.
#[must_use]
pub fn database_path(root: &StateRoot) -> PathBuf {
    root.0.join(DATABASE_FILE)
}

/// Checks the database schema without changing the database or its sidecars.
///
/// The connection is opened read-only and no pragmas are applied. A database
/// newer than this binary is rejected before writer startup can create WAL,
/// shared-memory, or backup files.
pub fn preflight_schema(root: &StateRoot) -> Result<SchemaVerdict, StoreError> {
    let path = database_path(root);
    match fs::metadata(&path) {
        Ok(_) => {}
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return Ok(SchemaVerdict::Absent);
        }
        Err(source) => return Err(StoreError::io(path, source)),
    }

    let connection = open_read_only(&path)?;
    let has_migrations: bool = connection.query_row(
        "SELECT EXISTS(\
             SELECT 1 FROM sqlite_schema \
             WHERE type = 'table' AND name = 'schema_migrations'\
         )",
        [],
        |row| row.get(0),
    )?;

    let version = if has_migrations {
        connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get::<_, Option<i64>>(0)
            })?
            .unwrap_or(0)
    } else {
        0
    };

    match version {
        CURRENT_SCHEMA_VERSION => Ok(SchemaVerdict::Current),
        version if version > CURRENT_SCHEMA_VERSION => Err(StoreError::NewerSchema {
            found: version,
            supported: CURRENT_SCHEMA_VERSION,
        }),
        _ => Ok(SchemaVerdict::Migratable),
    }
}

pub(super) fn open_read_only(path: &Path) -> Result<Connection, StoreError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    Ok(Connection::open_with_flags(path, flags)?)
}

pub(super) fn create_database_file(path: &Path) -> Result<(), StoreError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create_new(true)
        .mode(FILE_MODE);
    options
        .open(path)
        .map(|_| ())
        .map_err(|source| StoreError::io(path.to_path_buf(), source))
}

pub(super) fn enforce_private_file(path: &Path) -> Result<(), StoreError> {
    fs::set_permissions(path, Permissions::from_mode(FILE_MODE))
        .map_err(|source| StoreError::io(path.to_path_buf(), source))
}

pub(super) fn online_backup(root: &StateRoot) -> Result<PathBuf, StoreError> {
    let source_path = database_path(root);
    let source = open_read_only(&source_path)?;
    let backup_path = next_backup_path(root)?;

    create_database_file(&backup_path)?;
    if let Err(source_error) = source.backup(MAIN_DB, &backup_path, None) {
        return Err(StoreError::Backup {
            path: backup_path,
            source: source_error,
        });
    }
    enforce_private_file(&backup_path)?;
    Ok(backup_path)
}

fn next_backup_path(root: &StateRoot) -> Result<PathBuf, StoreError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| StoreError::Clock(source.to_string()))?;
    let stem = format!(
        "{DATABASE_FILE}.backup-{}-{:09}-{}",
        elapsed.as_secs(),
        elapsed.subsec_nanos(),
        process::id()
    );
    Ok(root.0.join(stem))
}

pub(super) fn migrate(connection: &mut Connection, now_ms: i64) -> Result<(), StoreError> {
    let transaction = connection.transaction()?;
    transaction.execute_batch(SCHEMA_V1)?;

    let version = transaction
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get::<_, Option<i64>>(0)
        })?
        .unwrap_or(0);
    if version > CURRENT_SCHEMA_VERSION {
        return Err(StoreError::NewerSchema {
            found: version,
            supported: CURRENT_SCHEMA_VERSION,
        });
    }

    transaction.execute(
        "INSERT OR IGNORE INTO schema_migrations(version, applied_at_ms) VALUES (?1, ?2)",
        (CURRENT_SCHEMA_VERSION, now_ms),
    )?;
    transaction.commit()?;
    Ok(())
}

const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version       INTEGER PRIMARY KEY,
    applied_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS herdr_sessions (
    session_name  TEXT PRIMARY KEY,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS workspaces (
    workspace_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS tabs (
    tab_id       TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS panes (
    pane_id       TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL,
    tab_id        TEXT NOT NULL,
    terminal_id   TEXT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    FOREIGN KEY (tab_id) REFERENCES tabs(tab_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS panes_terminal_id_idx ON panes(terminal_id);

CREATE TABLE IF NOT EXISTS native_agent_sessions (
    provider          TEXT NOT NULL,
    native_session_id TEXT NOT NULL,
    PRIMARY KEY (provider, native_session_id)
);

CREATE TABLE IF NOT EXISTS task_runs (
    run_id                           TEXT PRIMARY KEY,
    key_kind                         TEXT NOT NULL,
    key_controller_id                TEXT,
    key_provider                     TEXT,
    key_native_sid                   TEXT,
    key_native_path                  TEXT,
    key_terminal_id                  TEXT,
    key_start_ms                     INTEGER,
    key_seq                          TEXT,
    display_ordinal                  INTEGER UNIQUE NOT NULL,
    task_state                       TEXT NOT NULL,
    has_controller_task_state_event  INTEGER NOT NULL DEFAULT 0
                                         CHECK (has_controller_task_state_event IN (0, 1)),
    native_provider                  TEXT,
    native_session_id                TEXT,
    created_at_ms                    INTEGER NOT NULL,
    updated_at_ms                    INTEGER NOT NULL,
    finished_at_ms                   INTEGER,
    CHECK ((native_provider IS NULL) = (native_session_id IS NULL)),
    FOREIGN KEY (native_provider, native_session_id)
        REFERENCES native_agent_sessions(provider, native_session_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_controller_key_unique
    ON task_runs(key_controller_id) WHERE key_kind = 'controller';
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_native_key_unique
    ON task_runs(key_provider, key_native_sid) WHERE key_kind = 'native';
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_native_path_key_unique
    ON task_runs(key_provider, key_native_path) WHERE key_kind = 'native_path';
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_provisional_key_unique
    ON task_runs(key_terminal_id, key_start_ms, key_seq) WHERE key_kind = 'provisional';
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_native_session_binding_unique
    ON task_runs(native_provider, native_session_id) WHERE native_session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS task_runs_retention_idx
    ON task_runs(task_state, finished_at_ms);

CREATE TABLE IF NOT EXISTS agent_nodes (
    agent_node_id     TEXT PRIMARY KEY,
    provider          TEXT NOT NULL,
    native_session_id TEXT,
    task_run_id       TEXT NOT NULL,
    FOREIGN KEY (task_run_id) REFERENCES task_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (provider, native_session_id)
        REFERENCES native_agent_sessions(provider, native_session_id)
);

CREATE TABLE IF NOT EXISTS executions (
    execution_id  TEXT PRIMARY KEY,
    pane_id       TEXT NOT NULL,
    terminal_id   TEXT NOT NULL,
    task_run_id   TEXT NOT NULL,
    exec_state    TEXT NOT NULL,
    stale_since_ms INTEGER,
    started_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    ended_at_ms   INTEGER,
    FOREIGN KEY (task_run_id) REFERENCES task_runs(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS executions_task_run_idx ON executions(task_run_id);

CREATE TABLE IF NOT EXISTS execution_edges (
    parent_run_id TEXT NOT NULL,
    child_run_id  TEXT NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (parent_run_id, child_run_id),
    CHECK (parent_run_id <> child_run_id),
    FOREIGN KEY (parent_run_id) REFERENCES task_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (child_run_id) REFERENCES task_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dependency_edges (
    prerequisite_run_id TEXT NOT NULL,
    dependent_run_id    TEXT NOT NULL,
    created_at_ms        INTEGER NOT NULL,
    PRIMARY KEY (prerequisite_run_id, dependent_run_id),
    CHECK (prerequisite_run_id <> dependent_run_id),
    FOREIGN KEY (prerequisite_run_id) REFERENCES task_runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (dependent_run_id) REFERENCES task_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS events (
    event_row_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id              TEXT NOT NULL UNIQUE,
    seen_at_ms            INTEGER NOT NULL,
    event_timestamp_ms    INTEGER NOT NULL,
    herdr_session         TEXT NOT NULL,
    source                TEXT NOT NULL,
    normalized_kind       TEXT NOT NULL,
    source_event_type     TEXT NOT NULL,
    workspace_id          TEXT,
    tab_id                TEXT,
    pane_id               TEXT,
    terminal_id           TEXT,
    provider              TEXT,
    native_session_id     TEXT,
    task_run_id           TEXT,
    agent_node_id         TEXT,
    task_state            TEXT,
    model_id              TEXT,
    provider_event_kind   TEXT,
    tool_name             TEXT,
    item_count            INTEGER,
    byte_count            INTEGER,
    gap_kind              TEXT
);
CREATE INDEX IF NOT EXISTS events_session_retention_idx
    ON events(herdr_session, seen_at_ms, event_row_id);

CREATE TABLE IF NOT EXISTS event_ledger (
    event_id   TEXT PRIMARY KEY,
    seen_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS event_ledger_retention_idx ON event_ledger(seen_at_ms);

CREATE TABLE IF NOT EXISTS owner (
    singleton_id  INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    pid           INTEGER NOT NULL,
    started_at_ms INTEGER NOT NULL,
    terminal_id   TEXT,
    pane_id       TEXT
);

CREATE TABLE IF NOT EXISTS display_ordinals (
    entity_kind TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    ordinal     INTEGER UNIQUE NOT NULL,
    PRIMARY KEY (entity_kind, entity_id)
);
"#;
