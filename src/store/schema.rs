//! SQLite schema v6, read-only preflight, online backup, and migration support.

use std::ffi::OsString;
use std::fs::{self, Metadata, OpenOptions, Permissions};
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, MAIN_DB, OpenFlags, OptionalExtension, params};

use super::StoreError;
use crate::lockfile::StateRoot;

pub(super) const CURRENT_SCHEMA_VERSION: i64 = 6;
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

/// Exact version found by a strictly read-only schema inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaVersionVerdict {
    /// No database file exists.
    Absent,
    /// The database is at this binary's supported version.
    Current { found: i64 },
    /// The database is older than this binary and can be migrated by a writer.
    Migratable { found: i64 },
    /// The database was written by a newer binary.
    Newer { found: i64 },
}

/// Returns the single authoritative schema version supported by this binary.
#[must_use]
pub const fn supported_schema_version() -> i64 {
    CURRENT_SCHEMA_VERSION
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
    match inspect_schema_version(root)? {
        SchemaVersionVerdict::Absent => Ok(SchemaVerdict::Absent),
        SchemaVersionVerdict::Current { .. } => Ok(SchemaVerdict::Current),
        SchemaVersionVerdict::Migratable { .. } => Ok(SchemaVerdict::Migratable),
        SchemaVersionVerdict::Newer { found } => Err(StoreError::NewerSchema {
            found,
            supported: CURRENT_SCHEMA_VERSION,
        }),
    }
}

/// Inspects an existing database's schema without writing, migrating, or
/// creating SQLite sidecars.
pub fn inspect_schema_version(root: &StateRoot) -> Result<SchemaVersionVerdict, StoreError> {
    let path = database_path(root);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata,
        Ok(_) => {
            return Err(StoreError::io(
                path,
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "database entry is not a regular file",
                ),
            ));
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return Ok(SchemaVersionVerdict::Absent);
        }
        Err(source) => return Err(StoreError::io(path, source)),
    };

    let wal_path = sqlite_sidecar_path(&path, "-wal");
    let shm_path = sqlite_sidecar_path(&path, "-shm");
    let wal_present = regular_sidecar_present(&wal_path)?;
    regular_sidecar_present(&shm_path)?;
    let connection = open_schema_inspection(&path, wal_present)?;
    let result = inspect_open_schema(&connection);
    drop(connection);

    if !wal_present {
        let after =
            fs::symlink_metadata(&path).map_err(|source| StoreError::io(path.clone(), source))?;
        let wal_appeared = match fs::symlink_metadata(&wal_path) {
            Ok(_) => true,
            Err(source) if source.kind() == io::ErrorKind::NotFound => false,
            Err(source) => return Err(StoreError::io(wal_path, source)),
        };
        if !same_file_snapshot(&metadata, &after) || wal_appeared {
            return Err(StoreError::io(
                path,
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "database changed during immutable schema inspection",
                ),
            ));
        }
    }
    result
}

fn regular_sidecar_present(path: &Path) -> Result<bool, StoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(StoreError::io(
            path.to_path_buf(),
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "database sidecar is not a regular file",
            ),
        )),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(StoreError::io(path.to_path_buf(), source)),
    }
}

fn inspect_open_schema(connection: &Connection) -> Result<SchemaVersionVerdict, StoreError> {
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
        CURRENT_SCHEMA_VERSION => {
            validate_current_schema(connection)?;
            Ok(SchemaVersionVerdict::Current { found: version })
        }
        version if version > CURRENT_SCHEMA_VERSION => {
            Ok(SchemaVersionVerdict::Newer { found: version })
        }
        _ => Ok(SchemaVersionVerdict::Migratable { found: version }),
    }
}

fn open_schema_inspection(path: &Path, wal_present: bool) -> Result<Connection, StoreError> {
    let parameter = if wal_present {
        "readonly_shm=1"
    } else {
        "immutable=1"
    };
    let uri = sqlite_file_uri(path, parameter);
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    Ok(Connection::open_with_flags(uri, flags)?)
}

fn sqlite_file_uri(path: &Path, parameter: &str) -> PathBuf {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut uri = b"file:".to_vec();
    for byte in path.as_os_str().as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            uri.push(*byte);
        } else {
            uri.extend_from_slice(&[b'%', HEX[(byte >> 4) as usize], HEX[(byte & 0x0f) as usize]]);
        }
    }
    uri.push(b'?');
    uri.extend_from_slice(parameter.as_bytes());
    PathBuf::from(OsString::from_vec(uri))
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn schema_inspection_uri_for_test(path: &Path) -> PathBuf {
    sqlite_file_uri(path, "immutable=1")
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

fn same_file_snapshot(before: &Metadata, after: &Metadata) -> bool {
    before.file_type().is_file()
        && after.file_type().is_file()
        && before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

fn validate_current_schema(connection: &Connection) -> Result<(), StoreError> {
    let mut expected = Connection::open_in_memory()?;
    migrate(&mut expected, 0)?;
    let mut statement = expected.prepare(
        "SELECT type, name, sql FROM sqlite_schema \
         WHERE name NOT LIKE 'sqlite_%' \
           AND type IN ('table', 'index', 'view', 'trigger')",
    )?;
    let required = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    for (object_type, name, expected_sql) in required {
        let actual_sql = connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type = ?1 AND name = ?2",
                params![object_type, name],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;
        if actual_sql != Some(expected_sql) {
            return Err(rusqlite::Error::InvalidQuery.into());
        }
    }
    Ok(())
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

    let mut version = transaction
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

    if version == 0 {
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (1, ?1)",
            [now_ms],
        )?;
        version = 1;
    }
    if version < 2 {
        transaction.execute_batch(SCHEMA_V2)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (2, ?1)",
            [now_ms],
        )?;
        version = 2;
    }
    if version < 3 {
        transaction.execute_batch(SCHEMA_V3)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (3, ?1)",
            [now_ms],
        )?;
        version = 3;
    }
    if version < 4 {
        transaction.execute_batch(SCHEMA_V4)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (4, ?1)",
            [now_ms],
        )?;
        version = 4;
    }
    if version < 5 {
        transaction.execute_batch(SCHEMA_V5)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (5, ?1)",
            [now_ms],
        )?;
        version = 5;
    }
    if version < 6 {
        transaction.execute_batch(SCHEMA_V6)?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (6, ?1)",
            [now_ms],
        )?;
        version = 6;
    }
    debug_assert_eq!(version, CURRENT_SCHEMA_VERSION);
    transaction.commit()?;
    Ok(())
}

pub(super) const SCHEMA_V2: &str = r#"
ALTER TABLE events ADD COLUMN ingest_seq INTEGER NULL;

CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
INSERT INTO meta(key, value) VALUES ('ingest_seq_high_water', 0);
"#;

pub(super) const SCHEMA_V3: &str = r#"
ALTER TABLE agent_nodes ADD COLUMN parent_agent_node_id TEXT;
ALTER TABLE agent_nodes ADD COLUMN state TEXT CHECK (state IS NULL OR state = 'working');
ALTER TABLE agent_nodes ADD COLUMN model_id TEXT;
ALTER TABLE agent_nodes ADD COLUMN last_event_kind TEXT;
ALTER TABLE agent_nodes ADD COLUMN last_tool_name TEXT;
ALTER TABLE agent_nodes ADD COLUMN last_item_count INTEGER;
ALTER TABLE agent_nodes ADD COLUMN last_byte_count INTEGER;
ALTER TABLE agent_nodes ADD COLUMN last_activity_at_ms INTEGER;
ALTER TABLE agent_nodes ADD COLUMN session_file TEXT;
CREATE INDEX agent_nodes_parent_idx ON agent_nodes(parent_agent_node_id);

INSERT INTO display_ordinals(entity_kind, entity_id, ordinal)
SELECT
    'agent_node',
    ordered.agent_node_id,
    (SELECT COALESCE(MAX(ordinal), 0) FROM display_ordinals) + ordered.row_number
FROM (
    SELECT
        node.agent_node_id,
        ROW_NUMBER() OVER (
            ORDER BY run.display_ordinal, node.agent_node_id
        ) AS row_number
    FROM agent_nodes AS node
    JOIN task_runs AS run ON run.run_id = node.task_run_id
) AS ordered;

ALTER TABLE events ADD COLUMN provider_agent_id TEXT;
ALTER TABLE events ADD COLUMN provider_parent_agent_id TEXT;
ALTER TABLE events ADD COLUMN source_coverage TEXT;
"#;

pub(super) const SCHEMA_V4: &str = r#"
INSERT INTO display_ordinals(entity_kind, entity_id, ordinal)
SELECT
    'workspace',
    ordered.workspace_id,
    (SELECT COALESCE(MAX(ordinal), 0) FROM display_ordinals) + ordered.row_number
FROM (
    SELECT
        workspace_id,
        ROW_NUMBER() OVER (ORDER BY workspace_id) AS row_number
    FROM workspaces
) AS ordered;

INSERT INTO display_ordinals(entity_kind, entity_id, ordinal)
SELECT
    'tab',
    ordered.tab_id,
    (SELECT COALESCE(MAX(ordinal), 0) FROM display_ordinals) + ordered.row_number
FROM (
    SELECT
        tab_id,
        ROW_NUMBER() OVER (ORDER BY workspace_id, tab_id) AS row_number
    FROM tabs
) AS ordered;

INSERT INTO display_ordinals(entity_kind, entity_id, ordinal)
SELECT
    'pane',
    ordered.pane_id,
    (SELECT COALESCE(MAX(ordinal), 0) FROM display_ordinals) + ordered.row_number
FROM (
    SELECT
        pane_id,
        ROW_NUMBER() OVER (ORDER BY workspace_id, tab_id, pane_id) AS row_number
    FROM panes
) AS ordered;
"#;

pub(super) const SCHEMA_V5: &str = r#"
ALTER TABLE task_runs ADD COLUMN subject TEXT;
ALTER TABLE task_runs ADD COLUMN dismissed_at_ms INTEGER;
ALTER TABLE tabs ADD COLUMN label TEXT;
ALTER TABLE panes ADD COLUMN display_name TEXT;
"#;

pub(super) const SCHEMA_V6: &str = r#"
ALTER TABLE task_runs ADD COLUMN native_session_end_status TEXT
    CHECK (native_session_end_status IS NULL OR native_session_end_status IN (
        'done', 'error', 'cancelled', 'unknown'
    ));
ALTER TABLE task_runs ADD COLUMN native_session_end_at_ms INTEGER;
ALTER TABLE task_runs ADD COLUMN lifecycle_source_at_ms INTEGER;
ALTER TABLE task_runs ADD COLUMN lifecycle_observed_at_ms INTEGER;
ALTER TABLE task_runs ADD COLUMN lifecycle_source_order TEXT;
ALTER TABLE task_runs ADD COLUMN history_ready INTEGER NOT NULL DEFAULT 1
    CHECK (history_ready IN (0, 1));
ALTER TABLE task_runs ADD COLUMN latest_provider_at_ms INTEGER;

CREATE TABLE run_rate_totals (
    run_id       TEXT PRIMARY KEY,
    output_tokens INTEGER NOT NULL CHECK (output_tokens >= 0),
    working_ms    INTEGER NOT NULL CHECK (working_ms >= 0),
    FOREIGN KEY (run_id) REFERENCES task_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE history_drains (
    drain_id        TEXT PRIMARY KEY
                        CHECK (length(drain_id) BETWEEN 1 AND 160),
    provider        TEXT NOT NULL CHECK (provider IN ('claude', 'codex')),
    created_at_ms   INTEGER NOT NULL,
    finalized_at_ms INTEGER
);

CREATE TABLE history_drain_artifacts (
    drain_id    TEXT NOT NULL,
    artifact_id TEXT NOT NULL CHECK (length(artifact_id) > 0),
    generation  TEXT NOT NULL CHECK (length(generation) > 0),
    goalpost    INTEGER NOT NULL CHECK (goalpost >= 0),
    PRIMARY KEY (drain_id, artifact_id),
    FOREIGN KEY (drain_id) REFERENCES history_drains(drain_id) ON DELETE CASCADE
);

CREATE TABLE history_drain_runs (
    drain_id TEXT NOT NULL,
    run_id   TEXT NOT NULL,
    PRIMARY KEY (drain_id, run_id),
    FOREIGN KEY (drain_id) REFERENCES history_drains(drain_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES task_runs(run_id) ON DELETE CASCADE
);
CREATE INDEX history_drain_runs_run_idx ON history_drain_runs(run_id);
"#;

pub(super) const SCHEMA_V1: &str = r#"
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
    merged_into                     TEXT,
    created_at_ms                    INTEGER NOT NULL,
    updated_at_ms                    INTEGER NOT NULL,
    finished_at_ms                   INTEGER,
    CHECK ((native_provider IS NULL) = (native_session_id IS NULL)),
    CHECK (merged_into IS NULL OR merged_into <> run_id),
    FOREIGN KEY (native_provider, native_session_id)
        REFERENCES native_agent_sessions(provider, native_session_id),
    FOREIGN KEY (merged_into) REFERENCES task_runs(run_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_controller_key_unique
    ON task_runs(key_controller_id) WHERE key_kind = 'controller';
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_native_key_unique
    ON task_runs(key_provider, key_native_sid) WHERE key_kind = 'native';
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_native_path_key_unique
    ON task_runs(key_provider, key_native_path) WHERE key_kind = 'native_path';
CREATE INDEX IF NOT EXISTS task_runs_provisional_key_idx
    ON task_runs(key_terminal_id, key_start_ms, key_seq) WHERE key_kind = 'provisional';
CREATE UNIQUE INDEX IF NOT EXISTS task_runs_native_session_binding_unique
    ON task_runs(native_provider, native_session_id) WHERE native_session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS task_runs_retention_idx
    ON task_runs(task_state, finished_at_ms);
CREATE INDEX IF NOT EXISTS task_runs_merged_into_idx ON task_runs(merged_into);

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
