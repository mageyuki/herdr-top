//! Non-mutating local diagnostic probes and private atomic marker writers.

use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{PERSISTENCE_OCCURRENCE_PREFIX, PersistenceCounters};
use crate::lockfile::{ExistingLockVerdict, StateRoot};
use crate::rendezvous::ControllerRuntimeProbe;
use crate::session_key::SessionKey;
use crate::store::StoreError;
use crate::store::schema::SchemaVersionVerdict;
use crate::store::writer::{
    DurabilityDisposition, PersistenceFailure, PersistenceFailureCode, PersistenceOperation,
    PersistencePhase,
};

const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;
const SESSION_NAME_FILE: &str = "session-name.txt";
const BREADCRUMB_FILE: &str = "state-root.txt";
const LOG_FILE: &str = "herdr-top.log";
const MAX_SMALL_FILE_BYTES: u64 = 16 * 1024;
const MAX_LOG_TAIL_BYTES: u64 = 64 * 1024;

/// Schema key for the Controller-integration first-launch notice.
pub const NOTICE_SCHEMA: &str = "controller-integration-v1";

/// Read-only state-name sentinel observation without retaining found bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameRecordVerdict {
    Absent,
    Matches,
    Mismatch,
    Unsafe,
    Unreadable,
}

/// Read-only plugin breadcrumb observation without retaining file contents.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BreadcrumbVerdict {
    Absent,
    Matches,
    OtherRoot,
    Malformed,
    Unreadable,
}

/// Combined owner-lock observation for Doctor and other read-only consumers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnerLockVerdict {
    Missing,
    Available,
    HeldWithOwner,
    HeldWithoutOwner,
    HeldOwnerUnreadable,
    MalformedOrUnsafe,
    Unreadable,
}

/// Closed runtime verdict supplied by the rendezvous module's shared facts.
pub type RuntimeVerdict = ControllerRuntimeProbe;

/// Read-only database-schema observation without SQLite error text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaProbeVerdict {
    Absent { supported: i64 },
    Current { found: i64, supported: i64 },
    Migratable { found: i64, supported: i64 },
    Newer { found: i64, supported: i64 },
    Corrupt { supported: i64 },
    Unreadable { supported: i64 },
}

/// One validated historical persistence-degradation occurrence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PersistenceOccurrenceObservation {
    pub timestamp_ms: i64,
    pub failure: PersistenceFailure,
    pub counters: PersistenceCounters,
}

/// Bounded persistence-log observation without raw lines or parse errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistenceLogVerdict {
    Absent,
    Unreadable,
    NoValidRecord,
    Found(PersistenceOccurrenceObservation),
}

/// Read-only first-launch notice observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoticeVerdict {
    Absent,
    Current { dismissed_at_ms: i64 },
    Malformed,
    Unreadable,
    VersionMismatch,
}

/// Privacy-safe plugin breadcrumb publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BreadcrumbPublishError;

impl fmt::Display for BreadcrumbPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("breadcrumb_publish_failed")
    }
}

impl std::error::Error for BreadcrumbPublishError {}

/// Privacy-safe first-launch notice publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NoticePublishError;

impl fmt::Display for NoticePublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("notice_publish_failed")
    }
}

impl std::error::Error for NoticePublishError {}

/// Inspects the exact-session name sentinel without following its final link.
#[must_use]
pub fn probe_name_record(root: &StateRoot, key: &SessionKey) -> NameRecordVerdict {
    match read_private_file(&root.0.join(SESSION_NAME_FILE), MAX_SMALL_FILE_BYTES) {
        PrivateRead::Absent => NameRecordVerdict::Absent,
        PrivateRead::Unsafe => NameRecordVerdict::Unsafe,
        PrivateRead::Unreadable => NameRecordVerdict::Unreadable,
        PrivateRead::Contents(found) if found == key.name().as_bytes() => {
            NameRecordVerdict::Matches
        }
        PrivateRead::Contents(_) => NameRecordVerdict::Mismatch,
    }
}

/// Inspects an optional plugin breadcrumb without creating its directory.
#[must_use]
pub fn probe_plugin_breadcrumb(
    plugin_state_dir: Option<&OsStr>,
    root: &StateRoot,
) -> BreadcrumbVerdict {
    let Some(plugin_state_dir) = plugin_state_dir.filter(|path| !path.is_empty()) else {
        return BreadcrumbVerdict::Absent;
    };
    let path = Path::new(plugin_state_dir).join(BREADCRUMB_FILE);
    match read_private_file(&path, MAX_SMALL_FILE_BYTES) {
        PrivateRead::Absent => BreadcrumbVerdict::Absent,
        PrivateRead::Unreadable => BreadcrumbVerdict::Unreadable,
        PrivateRead::Unsafe => BreadcrumbVerdict::Malformed,
        PrivateRead::Contents(found) => {
            let Some(line) = single_line(&found) else {
                return BreadcrumbVerdict::Malformed;
            };
            if line == root.0.as_os_str().as_bytes() {
                BreadcrumbVerdict::Matches
            } else {
                BreadcrumbVerdict::OtherRoot
            }
        }
    }
}

/// Publishes one exact state-root line in an already-existing plugin directory.
///
/// The plugin supplies and owns the enclosing directory. This function never
/// creates that directory and refuses an empty, non-directory, or symlink path.
pub fn publish_plugin_breadcrumb(
    plugin_state_dir: &OsStr,
    root: &StateRoot,
) -> Result<(), BreadcrumbPublishError> {
    if plugin_state_dir.is_empty() || !valid_line(root.0.as_os_str().as_bytes()) {
        return Err(BreadcrumbPublishError);
    }
    let directory = Path::new(plugin_state_dir);
    validate_existing_directory(directory, None).map_err(|_| BreadcrumbPublishError)?;
    let destination = directory.join(BREADCRUMB_FILE);
    let temp = directory.join(format!(
        "{BREADCRUMB_FILE}.tmp.{}.{}",
        std::process::id(),
        Ulid::new()
    ));
    let result = publish_atomic_private(&temp, &destination, |file| {
        file.write_all(root.0.as_os_str().as_bytes())?;
        file.write_all(b"\n")
    });
    if result.is_err() {
        let _ = fs::remove_file(&temp);
        return Err(BreadcrumbPublishError);
    }
    sync_directory(directory).map_err(|_| BreadcrumbPublishError)
}

/// Combines the existing-file-only lock probe with a strictly read-only owner
/// record lookup when the lock is held.
#[must_use]
pub fn probe_owner_lock(root: &StateRoot) -> OwnerLockVerdict {
    match crate::lockfile::probe_existing_lock(root) {
        ExistingLockVerdict::Missing => OwnerLockVerdict::Missing,
        ExistingLockVerdict::Available => OwnerLockVerdict::Available,
        ExistingLockVerdict::MalformedOrUnsafe => OwnerLockVerdict::MalformedOrUnsafe,
        ExistingLockVerdict::Unreadable => OwnerLockVerdict::Unreadable,
        ExistingLockVerdict::Held => match crate::store::open_reader(root) {
            Ok(reader) => match reader.read_owner() {
                Ok(Some(_)) => OwnerLockVerdict::HeldWithOwner,
                Ok(None) => OwnerLockVerdict::HeldWithoutOwner,
                Err(_) => OwnerLockVerdict::HeldOwnerUnreadable,
            },
            Err(StoreError::DatabaseAbsent(_)) => OwnerLockVerdict::HeldWithoutOwner,
            Err(StoreError::SchemaNotCurrent) => OwnerLockVerdict::HeldOwnerUnreadable,
            Err(_) => OwnerLockVerdict::HeldOwnerUnreadable,
        },
    }
}

/// Probes an existing Controller rendezvous without publication, removal, or bind.
#[must_use]
pub fn probe_runtime(base: &Path, key: &SessionKey) -> RuntimeVerdict {
    crate::rendezvous::probe_controller_runtime(base, key)
}

/// Maps the authoritative read-only schema inspection into a closed safe type.
#[must_use]
pub fn probe_schema(root: &StateRoot) -> SchemaProbeVerdict {
    let supported = crate::store::schema::supported_schema_version();
    match crate::store::schema::inspect_schema_version(root) {
        Ok(SchemaVersionVerdict::Absent) => SchemaProbeVerdict::Absent { supported },
        Ok(SchemaVersionVerdict::Current { found }) => {
            SchemaProbeVerdict::Current { found, supported }
        }
        Ok(SchemaVersionVerdict::Migratable { found }) => {
            SchemaProbeVerdict::Migratable { found, supported }
        }
        Ok(SchemaVersionVerdict::Newer { found }) => SchemaProbeVerdict::Newer { found, supported },
        Err(StoreError::Io { .. }) => SchemaProbeVerdict::Unreadable { supported },
        Err(StoreError::Sqlite(source)) if sqlite_is_unreadable(&source) => {
            SchemaProbeVerdict::Unreadable { supported }
        }
        Err(_) => SchemaProbeVerdict::Corrupt { supported },
    }
}

/// Reads at most the newest bounded log tail and returns only the last exact,
/// valid persistence occurrence in file order.
#[must_use]
pub fn probe_persistence_log(root: &StateRoot) -> PersistenceLogVerdict {
    let path = root.0.join(LOG_FILE);
    let mut file = match open_private_file(&path) {
        PrivateOpen::Absent => return PersistenceLogVerdict::Absent,
        PrivateOpen::Unsafe | PrivateOpen::Unreadable => {
            return PersistenceLogVerdict::Unreadable;
        }
        PrivateOpen::File(file) => file,
    };
    let length = match file.metadata() {
        Ok(metadata) => metadata.len(),
        Err(_) => return PersistenceLogVerdict::Unreadable,
    };
    let start = length.saturating_sub(MAX_LOG_TAIL_BYTES);
    let starts_at_record_boundary = if start == 0 {
        true
    } else {
        if file.seek(SeekFrom::Start(start - 1)).is_err() {
            return PersistenceLogVerdict::Unreadable;
        }
        let mut predecessor = [0_u8; 1];
        if file.read_exact(&mut predecessor).is_err() {
            return PersistenceLogVerdict::Unreadable;
        }
        predecessor[0] == b'\n'
    };
    if file.seek(SeekFrom::Start(start)).is_err() {
        return PersistenceLogVerdict::Unreadable;
    }
    let mut tail = Vec::with_capacity((length - start) as usize);
    if file
        .take(MAX_LOG_TAIL_BYTES)
        .read_to_end(&mut tail)
        .is_err()
    {
        return PersistenceLogVerdict::Unreadable;
    }
    let tail = if starts_at_record_boundary {
        tail.as_slice()
    } else {
        match tail.iter().position(|byte| *byte == b'\n') {
            Some(boundary) => &tail[boundary + 1..],
            None => &[],
        }
    };
    let mut newest = None;
    for line in tail.split_inclusive(|byte| *byte == b'\n') {
        let Some(line) = line.strip_suffix(b"\n") else {
            continue;
        };
        if let Some(record) = parse_occurrence(line) {
            newest = Some(record);
        }
    }
    newest.map_or(
        PersistenceLogVerdict::NoValidRecord,
        PersistenceLogVerdict::Found,
    )
}

/// Control-escapes an operational path, then abbreviates an exact home prefix.
/// No filesystem resolution is performed.
#[must_use]
pub fn format_operational_path(path: &Path, home: Option<&OsStr>) -> String {
    let escaped = escape_os(path.as_os_str());
    let Some(home) = home.filter(|home| !home.is_empty()) else {
        return escaped;
    };
    let escaped_home = escape_os(home);
    if escaped == escaped_home {
        return "~".to_owned();
    }
    if escaped_home == "/" {
        return escaped
            .strip_prefix('/')
            .map_or(escaped.clone(), |rest| format!("~/{rest}"));
    }
    escaped
        .strip_prefix(&escaped_home)
        .and_then(|rest| rest.strip_prefix('/'))
        .map_or(escaped.clone(), |rest| format!("~/{rest}"))
}

/// Purely derives the global notice marker path.
#[must_use]
pub fn notice_path(state_base: &Path, package_version: &str) -> PathBuf {
    state_base
        .join("herdr-top")
        .join("ui-notices")
        .join(format!("{NOTICE_SCHEMA}-{package_version}.json"))
}

/// Inspects the exact-version notice marker without creating any path.
#[must_use]
pub fn inspect_notice(state_base: &Path, package_version: &str) -> NoticeVerdict {
    if !valid_component(package_version.as_bytes()) {
        return NoticeVerdict::Malformed;
    }
    let path = notice_path(state_base, package_version);
    let found = match read_private_file(&path, MAX_SMALL_FILE_BYTES) {
        PrivateRead::Absent => return NoticeVerdict::Absent,
        PrivateRead::Unsafe => return NoticeVerdict::Malformed,
        PrivateRead::Unreadable => return NoticeVerdict::Unreadable,
        PrivateRead::Contents(found) => found,
    };
    let Ok(marker) = serde_json::from_slice::<NoticeMarker>(&found) else {
        return NoticeVerdict::Malformed;
    };
    if marker.schema != NOTICE_SCHEMA || marker.package_version != package_version {
        NoticeVerdict::VersionMismatch
    } else {
        NoticeVerdict::Current {
            dismissed_at_ms: marker.dismissed_at_ms,
        }
    }
}

/// Atomically publishes an exact-version UI preference marker.
pub fn publish_notice(
    state_base: &Path,
    package_version: &str,
    dismissed_at_ms: i64,
) -> Result<(), NoticePublishError> {
    if !valid_component(package_version.as_bytes()) {
        return Err(NoticePublishError);
    }
    validate_existing_directory(state_base, None).map_err(|_| NoticePublishError)?;
    let application =
        ensure_private_child(state_base, "herdr-top").map_err(|_| NoticePublishError)?;
    let directory =
        ensure_private_child(&application, "ui-notices").map_err(|_| NoticePublishError)?;
    let destination = notice_path(state_base, package_version);
    let temp = directory.join(format!(
        ".{NOTICE_SCHEMA}-{package_version}.tmp.{}.{}",
        std::process::id(),
        Ulid::new()
    ));
    let marker = NoticeMarker {
        schema: NOTICE_SCHEMA.to_owned(),
        package_version: package_version.to_owned(),
        dismissed_at_ms,
    };
    let bytes = serde_json::to_vec(&marker).map_err(|_| NoticePublishError)?;
    let result = publish_atomic_private(&temp, &destination, |file| file.write_all(&bytes));
    if result.is_err() {
        let _ = fs::remove_file(&temp);
        return Err(NoticePublishError);
    }
    sync_directory(&directory).map_err(|_| NoticePublishError)
}

enum PrivateOpen {
    Absent,
    Unsafe,
    Unreadable,
    File(File),
}

enum PrivateRead {
    Absent,
    Unsafe,
    Unreadable,
    Contents(Vec<u8>),
}

fn open_private_file(path: &Path) -> PrivateOpen {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return PrivateOpen::Absent,
        Err(source) if source.raw_os_error() == Some(libc::ELOOP) => return PrivateOpen::Unsafe,
        Err(_) => return PrivateOpen::Unreadable,
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return PrivateOpen::Unreadable,
    };
    if !metadata.file_type().is_file()
        || metadata.mode() & 0o7777 != PRIVATE_FILE_MODE
        || metadata.uid() != crate::rendezvous::effective_uid()
        || metadata.nlink() != 1
    {
        return PrivateOpen::Unsafe;
    }
    PrivateOpen::File(file)
}

fn read_private_file(path: &Path, limit: u64) -> PrivateRead {
    let mut file = match open_private_file(path) {
        PrivateOpen::Absent => return PrivateRead::Absent,
        PrivateOpen::Unsafe => return PrivateRead::Unsafe,
        PrivateOpen::Unreadable => return PrivateRead::Unreadable,
        PrivateOpen::File(file) => file,
    };
    let mut contents = Vec::new();
    match (&mut file)
        .take(limit.saturating_add(1))
        .read_to_end(&mut contents)
    {
        Ok(_) if contents.len() as u64 <= limit => PrivateRead::Contents(contents),
        Ok(_) => PrivateRead::Unsafe,
        Err(_) => PrivateRead::Unreadable,
    }
}

fn single_line(contents: &[u8]) -> Option<&[u8]> {
    let line = contents.strip_suffix(b"\n")?;
    valid_line(line).then_some(line)
}

fn valid_line(line: &[u8]) -> bool {
    !line.is_empty() && !line.iter().any(|byte| matches!(byte, b'\n' | b'\r' | 0))
}

fn validate_existing_directory(path: &Path, mode: Option<u32>) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != crate::rendezvous::effective_uid()
        || mode.is_some_and(|mode| metadata.mode() & 0o7777 != mode)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "directory is not a trusted private directory",
        ));
    }
    Ok(())
}

fn ensure_private_child(parent: &Path, name: &str) -> io::Result<PathBuf> {
    validate_existing_directory(parent, None)?;
    let child = parent.join(name);
    match fs::create_dir(&child) {
        Ok(()) => fs::set_permissions(&child, Permissions::from_mode(PRIVATE_DIRECTORY_MODE))?,
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {}
        Err(source) => return Err(source),
    }
    validate_existing_directory(&child, Some(PRIVATE_DIRECTORY_MODE))?;
    Ok(child)
}

fn publish_atomic_private(
    temp: &Path,
    destination: &Path,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(temp)?;
    file.set_permissions(Permissions::from_mode(PRIVATE_FILE_MODE))?;
    write(&mut file)?;
    file.sync_all()?;
    fs::rename(temp, destination)
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

fn sqlite_is_unreadable(source: &rusqlite::Error) -> bool {
    let rusqlite::Error::SqliteFailure(error, _) = source else {
        return false;
    };
    matches!(
        error.code,
        rusqlite::ErrorCode::PermissionDenied
            | rusqlite::ErrorCode::ReadOnly
            | rusqlite::ErrorCode::CannotOpen
            | rusqlite::ErrorCode::SystemIoFailure
    )
}

fn escape_os(value: &OsStr) -> String {
    let mut escaped = String::new();
    let mut remaining = value.as_bytes();
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                escape_valid(valid, &mut escaped);
                break;
            }
            Err(error) => {
                let (valid, invalid) = remaining.split_at(error.valid_up_to());
                escape_valid(
                    std::str::from_utf8(valid)
                        .expect("valid_up_to identifies a valid UTF-8 prefix"),
                    &mut escaped,
                );
                let invalid_length = error.error_len().unwrap_or(invalid.len());
                for byte in &invalid[..invalid_length] {
                    escaped.push_str(&format!("\\x{byte:02x}"));
                }
                remaining = &invalid[invalid_length..];
            }
        }
    }
    escaped
}

fn escape_valid(value: &str, escaped: &mut String) {
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() && u32::from(character) < 0x80 => {
                escaped.push_str(&format!("\\x{:02x}", u32::from(character)));
            }
            character if character.is_control() => {
                escaped.push_str(&format!("\\u{{{:x}}}", u32::from(character)));
            }
            character => escaped.push(character),
        }
    }
}

fn valid_component(component: &[u8]) -> bool {
    !component.is_empty()
        && component.iter().all(|byte| {
            matches!(
                byte,
                b'a'..=b'z'
                    | b'A'..=b'Z'
                    | b'0'..=b'9'
                    | b'.'
                    | b'_'
                    | b'-'
                    | b'+'
            )
        })
        && component != b"."
        && component != b".."
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOccurrence {
    schema_version: u64,
    kind: RawOccurrenceKind,
    timestamp_ms: i64,
    failure: RawFailure,
    counters: RawCounters,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawOccurrenceKind {
    PersistenceDegraded,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFailure {
    operation: RawOperation,
    phase: RawPhase,
    code: RawFailureCode,
    durability: RawDurability,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawOperation {
    Apply,
    Cleanup,
    UpdateOwnerLocation,
    ReplaceOwner,
    Barrier,
    Checkpoint,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawPhase {
    QueueAdmission,
    CommandExecution,
    PostApplyCommit,
    Acknowledgement,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawFailureCode {
    Sqlite,
    Io,
    InvalidData,
    Clock,
    OwnerAbsent,
    CheckpointBusy,
    ChannelClosed,
    AcknowledgementDropped,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawDurability {
    NotApplicable,
    NotCommitted,
    Committed,
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCounters {
    not_committed_batches: u64,
    durability_unknown_batches: u64,
    committed_but_degraded_batches: u64,
    skipped_batches: u64,
    skipped_owner_updates: u64,
}

fn parse_occurrence(line: &[u8]) -> Option<PersistenceOccurrenceObservation> {
    let json = line.strip_prefix(PERSISTENCE_OCCURRENCE_PREFIX)?;
    let raw: RawOccurrence = serde_json::from_slice(json).ok()?;
    if raw.schema_version != 1 || !matches!(raw.kind, RawOccurrenceKind::PersistenceDegraded) {
        return None;
    }
    Some(PersistenceOccurrenceObservation {
        timestamp_ms: raw.timestamp_ms,
        failure: PersistenceFailure {
            operation: match raw.failure.operation {
                RawOperation::Apply => PersistenceOperation::Apply,
                RawOperation::Cleanup => PersistenceOperation::Cleanup,
                RawOperation::UpdateOwnerLocation => PersistenceOperation::UpdateOwnerLocation,
                RawOperation::ReplaceOwner => PersistenceOperation::ReplaceOwner,
                RawOperation::Barrier => PersistenceOperation::Barrier,
                RawOperation::Checkpoint => PersistenceOperation::Checkpoint,
            },
            phase: match raw.failure.phase {
                RawPhase::QueueAdmission => PersistencePhase::QueueAdmission,
                RawPhase::CommandExecution => PersistencePhase::CommandExecution,
                RawPhase::PostApplyCommit => PersistencePhase::PostApplyCommit,
                RawPhase::Acknowledgement => PersistencePhase::Acknowledgement,
            },
            code: match raw.failure.code {
                RawFailureCode::Sqlite => PersistenceFailureCode::Sqlite,
                RawFailureCode::Io => PersistenceFailureCode::Io,
                RawFailureCode::InvalidData => PersistenceFailureCode::InvalidData,
                RawFailureCode::Clock => PersistenceFailureCode::Clock,
                RawFailureCode::OwnerAbsent => PersistenceFailureCode::OwnerAbsent,
                RawFailureCode::CheckpointBusy => PersistenceFailureCode::CheckpointBusy,
                RawFailureCode::ChannelClosed => PersistenceFailureCode::ChannelClosed,
                RawFailureCode::AcknowledgementDropped => {
                    PersistenceFailureCode::AcknowledgementDropped
                }
            },
            durability: match raw.failure.durability {
                RawDurability::NotApplicable => DurabilityDisposition::NotApplicable,
                RawDurability::NotCommitted => DurabilityDisposition::NotCommitted,
                RawDurability::Committed => DurabilityDisposition::Committed,
                RawDurability::Unknown => DurabilityDisposition::Unknown,
            },
        },
        counters: PersistenceCounters {
            not_committed_batches: raw.counters.not_committed_batches,
            durability_unknown_batches: raw.counters.durability_unknown_batches,
            committed_but_degraded_batches: raw.counters.committed_but_degraded_batches,
            skipped_batches: raw.counters.skipped_batches,
            skipped_owner_updates: raw.counters.skipped_owner_updates,
        },
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct NoticeMarker {
    schema: String,
    package_version: String,
    dismissed_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::os::unix::net::UnixListener;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, ExitStatus};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::*;
    use crate::diagnostics::PersistenceCounters;
    use crate::lockfile::{self, ExistingLockVerdict};
    use crate::session_key::encode;
    use crate::store::writer::{
        DurabilityDisposition, PersistenceFailure, PersistenceFailureCode, PersistenceOperation,
        PersistencePhase,
    };

    fn private_dir(path: &Path) {
        fs::create_dir_all(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn private_file(path: &Path, contents: &[u8]) {
        fs::write(path, contents).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn private_fifo(path: &Path) {
        let status = Command::new("mkfifo")
            .args([OsStr::new("-m"), OsStr::new("600"), path.as_os_str()])
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn entries(path: &Path) -> Vec<PathBuf> {
        let mut found = fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        found.sort();
        found
    }

    #[derive(Debug, Eq, PartialEq)]
    struct StableMetadata {
        dev: u64,
        ino: u64,
        mode: u32,
        nlink: u64,
        uid: u32,
        gid: u32,
        len: u64,
        mtime: i64,
        mtime_nsec: i64,
        ctime: i64,
        ctime_nsec: i64,
    }

    fn stable_metadata(path: &Path) -> StableMetadata {
        let metadata = fs::symlink_metadata(path).unwrap();
        StableMetadata {
            dev: metadata.dev(),
            ino: metadata.ino(),
            mode: metadata.mode(),
            nlink: metadata.nlink(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            len: metadata.len(),
            mtime: metadata.mtime(),
            mtime_nsec: metadata.mtime_nsec(),
            ctime: metadata.ctime(),
            ctime_nsec: metadata.ctime_nsec(),
        }
    }

    fn valid_occurrence(timestamp_ms: i64, operation: &str) -> String {
        format!(
            "HERDR_TOP_PERSISTENCE_V1 {{\"schema_version\":1,\"kind\":\"persistence_degraded\",\"timestamp_ms\":{timestamp_ms},\"failure\":{{\"operation\":\"{operation}\",\"phase\":\"command_execution\",\"code\":\"sqlite\",\"durability\":\"not_committed\"}},\"counters\":{{\"not_committed_batches\":1,\"durability_unknown_batches\":2,\"committed_but_degraded_batches\":3,\"skipped_batches\":4,\"skipped_owner_updates\":5}}}}\n"
        )
    }

    struct WalHolderChild {
        child: Child,
        stop: PathBuf,
        finished: bool,
    }

    impl WalHolderChild {
        fn finish(mut self) -> ExitStatus {
            fs::write(&self.stop, b"stop").unwrap();
            let status = self.child.wait().unwrap();
            self.finished = true;
            status
        }
    }

    impl Drop for WalHolderChild {
        fn drop(&mut self) {
            if !self.finished {
                let _ = fs::write(&self.stop, b"stop");
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }

    #[test]
    fn schema_wal_holder_helper() {
        let Some(database) = std::env::var_os("HERDR_TOP_TEST_WAL_DATABASE") else {
            return;
        };
        let ready = PathBuf::from(std::env::var_os("HERDR_TOP_TEST_WAL_READY").unwrap());
        let stop = PathBuf::from(std::env::var_os("HERDR_TOP_TEST_WAL_STOP").unwrap());
        let supported = crate::store::schema::supported_schema_version();
        let writer = Connection::open(PathBuf::from(database)).unwrap();
        writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        writer
            .execute(
                "UPDATE schema_migrations SET version = ?1 \
                 WHERE version = (SELECT MAX(version) FROM schema_migrations)",
                [supported + 1],
            )
            .unwrap();
        fs::write(&ready, b"ready").unwrap();
        while !stop.exists() {
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn i4_local_missing_tree_probe_remains_noncreating() {
        let directory = tempdir().unwrap();
        let state_base = directory.path().join("missing-state");
        let runtime_base = directory.path().join("missing-runtime");
        let plugin_dir = directory.path().join("missing-plugin");
        let key = encode("noncreating").unwrap();
        let root = lockfile::derive_state_root(&state_base, &key);

        assert_eq!(probe_name_record(&root, &key), NameRecordVerdict::Absent);
        assert_eq!(
            probe_plugin_breadcrumb(Some(plugin_dir.as_os_str()), &root),
            BreadcrumbVerdict::Absent
        );
        assert_eq!(probe_owner_lock(&root), OwnerLockVerdict::Missing);
        assert_eq!(probe_runtime(&runtime_base, &key), RuntimeVerdict::Absent);
        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Absent {
                supported: crate::store::schema::supported_schema_version()
            }
        );
        assert_eq!(probe_persistence_log(&root), PersistenceLogVerdict::Absent);
        assert_eq!(
            inspect_notice(&state_base, env!("CARGO_PKG_VERSION")),
            NoticeVerdict::Absent
        );
        assert!(!state_base.exists());
        assert!(!runtime_base.exists());
        assert!(!plugin_dir.exists());
        assert!(entries(directory.path()).is_empty());
    }

    #[test]
    fn i4_local_name_breadcrumb_lock_owner_matrix() {
        let directory = tempdir().unwrap();
        let key = encode("matrix session").unwrap();
        let root = lockfile::state_root_in(directory.path(), &key).unwrap();
        let name_path = root.0.join("session-name.txt");

        fs::remove_file(&name_path).unwrap();
        assert_eq!(probe_name_record(&root, &key), NameRecordVerdict::Absent);
        private_file(&name_path, key.name().as_bytes());
        assert_eq!(probe_name_record(&root, &key), NameRecordVerdict::Matches);
        private_file(&name_path, b"other session");
        assert_eq!(probe_name_record(&root, &key), NameRecordVerdict::Mismatch);
        fs::set_permissions(&name_path, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(probe_name_record(&root, &key), NameRecordVerdict::Unsafe);
        fs::set_permissions(&name_path, fs::Permissions::from_mode(0o000)).unwrap();
        assert_eq!(
            probe_name_record(&root, &key),
            NameRecordVerdict::Unreadable
        );
        fs::set_permissions(&name_path, fs::Permissions::from_mode(0o600)).unwrap();

        let plugin = directory.path().join("plugin");
        private_dir(&plugin);
        assert_eq!(
            probe_plugin_breadcrumb(Some(plugin.as_os_str()), &root),
            BreadcrumbVerdict::Absent
        );
        publish_plugin_breadcrumb(plugin.as_os_str(), &root).unwrap();
        assert_eq!(
            probe_plugin_breadcrumb(Some(plugin.as_os_str()), &root),
            BreadcrumbVerdict::Matches
        );
        private_file(&plugin.join("state-root.txt"), b"/different/root\n");
        assert_eq!(
            probe_plugin_breadcrumb(Some(plugin.as_os_str()), &root),
            BreadcrumbVerdict::OtherRoot
        );
        private_file(&plugin.join("state-root.txt"), b"not-one-line");
        assert_eq!(
            probe_plugin_breadcrumb(Some(plugin.as_os_str()), &root),
            BreadcrumbVerdict::Malformed
        );
        fs::set_permissions(
            plugin.join("state-root.txt"),
            fs::Permissions::from_mode(0o000),
        )
        .unwrap();
        assert_eq!(
            probe_plugin_breadcrumb(Some(plugin.as_os_str()), &root),
            BreadcrumbVerdict::Unreadable
        );
        fs::set_permissions(
            plugin.join("state-root.txt"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();

        assert_eq!(probe_owner_lock(&root), OwnerLockVerdict::Missing);
        let mut store = crate::store::open_writer(&root).unwrap();
        store
            .replace_owner(&lockfile::OwnerRecord {
                pid: 42,
                started_at_ms: 123,
                terminal_id: None,
                pane_id: None,
            })
            .unwrap();
        drop(store);
        let owner = lockfile::try_acquire(&root).unwrap().unwrap();
        assert_eq!(probe_owner_lock(&root), OwnerLockVerdict::HeldWithOwner);
        assert!(lockfile::try_acquire(&root).unwrap().is_none());
        drop(owner);
        assert_eq!(probe_owner_lock(&root), OwnerLockVerdict::Available);

        let lock_path = root.0.join("collector.lock");
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            lockfile::probe_existing_lock(&root),
            ExistingLockVerdict::MalformedOrUnsafe
        );
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o000)).unwrap();
        assert_eq!(
            lockfile::probe_existing_lock(&root),
            ExistingLockVerdict::Unreadable
        );
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600)).unwrap();

        let other_key = encode("held without owner").unwrap();
        let other_root = lockfile::state_root_in(directory.path(), &other_key).unwrap();
        let other_owner = lockfile::try_acquire(&other_root).unwrap().unwrap();
        assert_eq!(
            probe_owner_lock(&other_root),
            OwnerLockVerdict::HeldWithoutOwner
        );
        drop(other_owner);
    }

    #[test]
    fn i4_local_special_files_never_block() {
        let directory = tempdir().unwrap();
        let root = lockfile::StateRoot(directory.path().to_path_buf());
        private_fifo(&root.0.join("collector.lock"));

        let plugin = directory.path().join("plugin");
        private_dir(&plugin);
        private_fifo(&plugin.join("state-root.txt"));

        let (lock_tx, lock_rx) = mpsc::channel();
        let lock_root = root.clone();
        thread::spawn(move || {
            let _ = lock_tx.send(lockfile::probe_existing_lock(&lock_root));
        });

        let (breadcrumb_tx, breadcrumb_rx) = mpsc::channel();
        let breadcrumb_plugin = plugin.clone();
        let breadcrumb_root = root.clone();
        thread::spawn(move || {
            let _ = breadcrumb_tx.send(probe_plugin_breadcrumb(
                Some(breadcrumb_plugin.as_os_str()),
                &breadcrumb_root,
            ));
        });

        let lock = lock_rx.recv_timeout(Duration::from_secs(1));
        let breadcrumb = breadcrumb_rx.recv_timeout(Duration::from_secs(1));
        assert_eq!(
            (lock, breadcrumb),
            (
                Ok(ExistingLockVerdict::MalformedOrUnsafe),
                Ok(BreadcrumbVerdict::Malformed)
            )
        );
    }

    #[test]
    fn i4_local_held_migratable_owner_is_unreadable() {
        let directory = tempdir().unwrap();
        let key = encode("held migratable owner").unwrap();
        let root = lockfile::state_root_in(directory.path(), &key).unwrap();
        let database = crate::store::database_path(&root);
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_migrations(\
                     version INTEGER PRIMARY KEY,\
                     applied_at_ms INTEGER NOT NULL\
                 );\
                 INSERT INTO schema_migrations(version, applied_at_ms) VALUES (1, 0);",
            )
            .unwrap();
        drop(connection);

        let owner = lockfile::try_acquire(&root).unwrap().unwrap();
        assert_eq!(
            probe_owner_lock(&root),
            OwnerLockVerdict::HeldOwnerUnreadable
        );
        drop(owner);
    }

    #[test]
    fn i4_local_runtime_orphan_malformed_collision_live_too_long_matrix() {
        let directory = tempdir().unwrap();
        let runtime_base = directory.path().join("runtime");
        let runtime_dir = runtime_base.join("herdr-top");
        private_dir(&runtime_base);
        private_dir(&runtime_dir);
        let key = encode("runtime matrix").unwrap();
        let paths = crate::rendezvous::controller_runtime_paths(&runtime_base, &key);

        assert_eq!(probe_runtime(&runtime_base, &key), RuntimeVerdict::Absent);

        let orphan = UnixListener::bind(&paths.socket).unwrap();
        assert_eq!(
            probe_runtime(&runtime_base, &key),
            RuntimeVerdict::OrphanSocket
        );
        drop(orphan);
        fs::remove_file(&paths.socket).unwrap();

        private_file(&paths.sentinel, key.name().as_bytes());
        fs::set_permissions(&paths.sentinel, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            probe_runtime(&runtime_base, &key),
            RuntimeVerdict::MalformedSentinel
        );
        private_file(&paths.sentinel, b"different session");
        assert_eq!(
            probe_runtime(&runtime_base, &key),
            RuntimeVerdict::Collision
        );
        private_file(&paths.sentinel, key.name().as_bytes());
        assert_eq!(
            probe_runtime(&runtime_base, &key),
            RuntimeVerdict::NoLiveEndpoint
        );

        let live = UnixListener::bind(&paths.socket).unwrap();
        assert_eq!(probe_runtime(&runtime_base, &key), RuntimeVerdict::Live);
        drop(live);
        assert_eq!(probe_runtime(&runtime_base, &key), RuntimeVerdict::Stale);

        let too_long = PathBuf::from(format!("/tmp/{}", "x".repeat(200)));
        assert!(matches!(
            probe_runtime(&too_long, &key),
            RuntimeVerdict::PathTooLong { .. }
        ));
    }

    #[test]
    fn i4_local_runtime_malformed_entry_without_sentinel() {
        let directory = tempdir().unwrap();
        let runtime_base = directory.path().join("runtime");
        let runtime_dir = runtime_base.join("herdr-top");
        private_dir(&runtime_base);
        private_dir(&runtime_dir);
        let key = encode("runtime malformed entry").unwrap();
        let paths = crate::rendezvous::controller_runtime_paths(&runtime_base, &key);

        private_file(&paths.socket, b"");
        assert_eq!(
            probe_runtime(&runtime_base, &key),
            RuntimeVerdict::MalformedSocket
        );
        fs::remove_file(&paths.socket).unwrap();

        private_fifo(&paths.socket);
        assert_eq!(
            probe_runtime(&runtime_base, &key),
            RuntimeVerdict::MalformedSocket
        );
        fs::remove_file(&paths.socket).unwrap();

        private_dir(&paths.socket);
        assert_eq!(
            probe_runtime(&runtime_base, &key),
            RuntimeVerdict::MalformedSocket
        );
    }

    #[test]
    fn i4_local_schema_matrix_is_read_only() {
        let directory = tempdir().unwrap();
        let key = encode("schema matrix").unwrap();
        let root = lockfile::derive_state_root(directory.path(), &key);
        private_dir(&root.0);
        let database = crate::store::database_path(&root);
        let supported = crate::store::schema::supported_schema_version();

        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Absent { supported }
        );
        assert!(!database.exists());

        {
            let connection = Connection::open(&database).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);",
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (?1, 0)",
                    [supported],
                )
                .unwrap();
        }
        let before = entries(&root.0);
        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Corrupt { supported }
        );
        assert_eq!(entries(&root.0), before);

        fs::remove_file(&database).unwrap();
        drop(crate::store::open_writer(&root).unwrap());
        let before = entries(&root.0);
        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Current {
                found: supported,
                supported
            }
        );
        assert_eq!(entries(&root.0), before);

        {
            let connection = Connection::open(&database).unwrap();
            connection
                .execute("DELETE FROM schema_migrations WHERE version > 1", [])
                .unwrap();
        }
        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Migratable {
                found: 1,
                supported
            }
        );

        {
            let connection = Connection::open(&database).unwrap();
            connection
                .execute("UPDATE schema_migrations SET version = ?1", [supported + 1])
                .unwrap();
        }
        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Newer {
                found: supported + 1,
                supported
            }
        );

        fs::write(&database, b"not sqlite PRIVATE_DATABASE_TEXT").unwrap();
        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Corrupt { supported }
        );
        fs::set_permissions(&database, fs::Permissions::from_mode(0o000)).unwrap();
        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Unreadable { supported }
        );
        fs::set_permissions(&database, fs::Permissions::from_mode(0o600)).unwrap();
        fs::remove_file(&database).unwrap();
        let symlink_target = root.0.join("private-symlink-target");
        private_file(&symlink_target, b"PRIVATE_SYMLINK_TARGET");
        std::os::unix::fs::symlink(&symlink_target, &database).unwrap();
        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Unreadable { supported }
        );
        assert_eq!(
            fs::read(&symlink_target).unwrap(),
            b"PRIVATE_SYMLINK_TARGET"
        );
        assert!(!root.0.join("herdr-top.sqlite3-wal").exists());
        assert!(!root.0.join("herdr-top.sqlite3-shm").exists());
        assert!(entries(&root.0).iter().all(|path| {
            !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("backup")
        }));
    }

    #[test]
    fn i4_local_schema_incomplete_current_is_corrupt() {
        let directory = tempdir().unwrap();
        let root = lockfile::StateRoot(directory.path().to_path_buf());
        let database = crate::store::database_path(&root);
        let supported = crate::store::schema::supported_schema_version();
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_migrations(\
                     version INTEGER PRIMARY KEY,\
                     applied_at_ms INTEGER NOT NULL\
                 );",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO schema_migrations(version, applied_at_ms) VALUES (?1, 0)",
                [supported],
            )
            .unwrap();
        drop(connection);

        assert_eq!(
            probe_schema(&root),
            SchemaProbeVerdict::Corrupt { supported }
        );
    }

    #[test]
    fn i4_local_schema_wal_mode_creates_no_sidecars_or_stale_verdict() {
        let directory = tempdir().unwrap();
        let supported = crate::store::schema::supported_schema_version();

        let no_sidecar_root = lockfile::StateRoot(directory.path().join("no-sidecars"));
        private_dir(&no_sidecar_root.0);
        drop(crate::store::open_writer(&no_sidecar_root).unwrap());
        let no_sidecar_database = crate::store::database_path(&no_sidecar_root);
        let no_sidecar_wal = no_sidecar_database.with_extension("sqlite3-wal");
        let no_sidecar_shm = no_sidecar_database.with_extension("sqlite3-shm");
        assert!(!no_sidecar_wal.exists());
        assert!(!no_sidecar_shm.exists());
        let no_sidecar_before = entries(&no_sidecar_root.0);
        assert_eq!(
            probe_schema(&no_sidecar_root),
            SchemaProbeVerdict::Current {
                found: supported,
                supported
            }
        );
        assert_eq!(entries(&no_sidecar_root.0), no_sidecar_before);

        let source_root = lockfile::StateRoot(directory.path().join("wal-source"));
        private_dir(&source_root.0);
        drop(crate::store::open_writer(&source_root).unwrap());
        let source_database = crate::store::database_path(&source_root);
        let ready = directory.path().join("wal-ready");
        let stop = directory.path().join("wal-stop");
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                OsStr::new("--exact"),
                OsStr::new("diagnostics::local::tests::schema_wal_holder_helper"),
                OsStr::new("--nocapture"),
            ])
            .env("HERDR_TOP_TEST_WAL_DATABASE", &source_database)
            .env("HERDR_TOP_TEST_WAL_READY", &ready)
            .env("HERDR_TOP_TEST_WAL_STOP", &stop)
            .spawn()
            .unwrap();
        let mut holder = WalHolderChild {
            child,
            stop,
            finished: false,
        };
        for _ in 0..500 {
            if ready.exists() {
                break;
            }
            assert!(holder.child.try_wait().unwrap().is_none());
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "WAL holder did not become ready");
        let source_wal = source_database.with_extension("sqlite3-wal");
        let source_shm = source_database.with_extension("sqlite3-shm");
        assert!(source_wal.exists());
        assert!(source_shm.exists());
        let source_before = entries(&source_root.0);
        let source_wal_before = fs::read(&source_wal).unwrap();
        let source_shm_before = fs::read(&source_shm).unwrap();
        assert_eq!(
            probe_schema(&source_root),
            SchemaProbeVerdict::Newer {
                found: supported + 1,
                supported
            }
        );
        assert_eq!(entries(&source_root.0), source_before);
        assert_eq!(fs::read(&source_wal).unwrap(), source_wal_before);
        assert_eq!(fs::read(&source_shm).unwrap(), source_shm_before);

        let copied_root = lockfile::StateRoot(directory.path().join("wal-copy"));
        private_dir(&copied_root.0);
        let copied_database = crate::store::database_path(&copied_root);
        let copied_wal = copied_database.with_extension("sqlite3-wal");
        let copied_shm = copied_database.with_extension("sqlite3-shm");
        fs::copy(&source_database, &copied_database).unwrap();
        fs::copy(&source_wal, &copied_wal).unwrap();
        fs::set_permissions(&copied_database, fs::Permissions::from_mode(0o600)).unwrap();
        fs::set_permissions(&copied_wal, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(!copied_shm.exists());
        let copied_before = entries(&copied_root.0);

        let verdict = probe_schema(&copied_root);
        assert_ne!(
            verdict,
            SchemaProbeVerdict::Current {
                found: supported,
                supported
            }
        );
        assert!(!copied_shm.exists());
        assert_eq!(entries(&copied_root.0), copied_before);
        assert_eq!(verdict, SchemaProbeVerdict::Unreadable { supported });
        assert!(holder.finish().success());
    }

    #[test]
    fn i4_local_schema_special_sidecars_never_block() {
        let directory = tempdir().unwrap();
        let supported = crate::store::schema::supported_schema_version();
        let wal_root = lockfile::StateRoot(directory.path().join("fifo-wal"));
        let shm_root = lockfile::StateRoot(directory.path().join("fifo-shm"));
        private_dir(&wal_root.0);
        private_dir(&shm_root.0);
        drop(crate::store::open_writer(&wal_root).unwrap());
        drop(crate::store::open_writer(&shm_root).unwrap());

        let wal_database = crate::store::database_path(&wal_root);
        let shm_database = crate::store::database_path(&shm_root);
        let wal = wal_database.with_extension("sqlite3-wal");
        let shm = shm_database.with_extension("sqlite3-shm");
        private_fifo(&wal);
        private_fifo(&shm);
        let wal_entries_before = entries(&wal_root.0);
        let shm_entries_before = entries(&shm_root.0);
        let wal_metadata_before = stable_metadata(&wal);
        let shm_metadata_before = stable_metadata(&shm);

        let (wal_sender, wal_receiver) = mpsc::channel();
        thread::spawn(move || {
            let _ = wal_sender.send(probe_schema(&wal_root));
        });
        let (shm_sender, shm_receiver) = mpsc::channel();
        thread::spawn(move || {
            let _ = shm_sender.send(probe_schema(&shm_root));
        });
        let wal_result = wal_receiver.recv_timeout(Duration::from_secs(1));
        let shm_result = shm_receiver.recv_timeout(Duration::from_secs(1));

        assert_eq!(
            entries(&directory.path().join("fifo-wal")),
            wal_entries_before
        );
        assert_eq!(
            entries(&directory.path().join("fifo-shm")),
            shm_entries_before
        );
        assert_eq!(stable_metadata(&wal), wal_metadata_before);
        assert_eq!(stable_metadata(&shm), shm_metadata_before);
        assert_eq!(
            (wal_result, shm_result),
            (
                Ok(SchemaProbeVerdict::Unreadable { supported }),
                Ok(SchemaProbeVerdict::Unreadable { supported })
            )
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn i4_local_schema_double_slash_uri_preserves_path() {
        let directory = tempdir().unwrap();
        let root = lockfile::StateRoot(directory.path().to_path_buf());
        drop(crate::store::open_writer(&root).unwrap());
        let database = crate::store::database_path(&root);

        let mut alias_root_bytes = vec![b'/'];
        alias_root_bytes.extend_from_slice(directory.path().as_os_str().as_bytes());
        let alias_root = lockfile::StateRoot(PathBuf::from(OsString::from_vec(alias_root_bytes)));
        let alias_database = crate::store::database_path(&alias_root);
        let original_metadata = fs::metadata(&database).unwrap();
        let alias_metadata = fs::metadata(&alias_database).unwrap();
        if original_metadata.dev() != alias_metadata.dev()
            || original_metadata.ino() != alias_metadata.ino()
        {
            eprintln!("skipping: this platform gives // implementation-defined semantics");
            return;
        }

        let before = entries(&root.0);
        let supported = crate::store::schema::supported_schema_version();
        assert_eq!(
            crate::store::schema::inspect_schema_version(&alias_root).unwrap(),
            crate::store::schema::SchemaVersionVerdict::Current { found: supported }
        );
        assert_eq!(
            probe_schema(&alias_root),
            SchemaProbeVerdict::Current {
                found: supported,
                supported
            }
        );

        let uri = crate::store::schema::schema_inspection_uri_for_test(&alias_database);
        let uri_bytes = uri.as_os_str().as_bytes();
        assert!(uri_bytes.starts_with(b"file:"));
        assert!(!uri_bytes.starts_with(b"file://"));
        let query = uri_bytes.iter().position(|byte| *byte == b'?').unwrap();
        let mut decoded = Vec::new();
        let mut index = b"file:".len();
        while index < query {
            if uri_bytes[index] == b'%' {
                let digits = std::str::from_utf8(&uri_bytes[index + 1..index + 3]).unwrap();
                decoded.push(u8::from_str_radix(digits, 16).unwrap());
                index += 3;
            } else {
                decoded.push(uri_bytes[index]);
                index += 1;
            }
        }
        assert_eq!(decoded, alias_database.as_os_str().as_bytes());
        assert_eq!(entries(&root.0), before);
        assert!(!database.with_extension("sqlite3-wal").exists());
        assert!(!database.with_extension("sqlite3-shm").exists());
        assert!(!database.with_extension("sqlite3-journal").exists());
        assert!(entries(&root.0).iter().all(|path| {
            !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("backup")
        }));
    }

    #[test]
    fn i4_local_log_probe_absent_unreadable_valid_malformed_irrelevant_matrix() {
        let directory = tempdir().unwrap();
        let root = lockfile::StateRoot(directory.path().to_path_buf());
        let log = directory.path().join("herdr-top.log");

        assert_eq!(probe_persistence_log(&root), PersistenceLogVerdict::Absent);
        private_file(&log, b"unrelated\nHERDR_TOP_PERSISTENCE_V1 {bad json}\n");
        assert_eq!(
            probe_persistence_log(&root),
            PersistenceLogVerdict::NoValidRecord
        );
        private_file(&log, valid_occurrence(123, "apply").as_bytes());
        assert!(matches!(
            probe_persistence_log(&root),
            PersistenceLogVerdict::Found(PersistenceOccurrenceObservation {
                timestamp_ms: 123,
                ..
            })
        ));
        fs::set_permissions(&log, fs::Permissions::from_mode(0o000)).unwrap();
        assert_eq!(
            probe_persistence_log(&root),
            PersistenceLogVerdict::Unreadable
        );
        fs::set_permissions(&log, fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[test]
    fn i4_local_log_probe_returns_newest_valid_record_without_raw_text() {
        let directory = tempdir().unwrap();
        let root = lockfile::StateRoot(directory.path().to_path_buf());
        let log = directory.path().join("herdr-top.log");
        let padding = "x".repeat(MAX_LOG_TAIL_BYTES as usize + 128);
        let contents = format!(
            "{}{padding}\nPRIVATE_RAW_LINE\nHERDR_TOP_PERSISTENCE_V1 {{\"schema_version\":1,\"kind\":\"wrong\"}}\n{}",
            valid_occurrence(111, "cleanup"),
            valid_occurrence(222, "apply")
        );
        private_file(&log, contents.as_bytes());
        assert!(fs::metadata(&log).unwrap().len() > MAX_LOG_TAIL_BYTES);

        let expected = PersistenceOccurrenceObservation {
            timestamp_ms: 222,
            failure: PersistenceFailure {
                operation: PersistenceOperation::Apply,
                phase: PersistencePhase::CommandExecution,
                code: PersistenceFailureCode::Sqlite,
                durability: DurabilityDisposition::NotCommitted,
            },
            counters: PersistenceCounters {
                not_committed_batches: 1,
                durability_unknown_batches: 2,
                committed_but_degraded_batches: 3,
                skipped_batches: 4,
                skipped_owner_updates: 5,
            },
        };
        let verdict = probe_persistence_log(&root);
        assert_eq!(verdict, PersistenceLogVerdict::Found(expected));
        let rendered = format!("{verdict:?}");
        assert!(!rendered.contains("PRIVATE_RAW_LINE"));
        assert!(!rendered.contains("HERDR_TOP_PERSISTENCE_V1"));
    }

    #[test]
    fn i4_local_log_tail_exact_boundary_retains_record() {
        let directory = tempdir().unwrap();
        let root = lockfile::StateRoot(directory.path().to_path_buf());
        let log = directory.path().join("herdr-top.log");

        let boundary_record = valid_occurrence(333, "cleanup");
        let padding = "x".repeat(MAX_LOG_TAIL_BYTES as usize - boundary_record.len());
        private_file(
            &log,
            format!("outside-tail\n{boundary_record}{padding}").as_bytes(),
        );
        assert_eq!(
            probe_persistence_log(&root),
            PersistenceLogVerdict::Found(PersistenceOccurrenceObservation {
                timestamp_ms: 333,
                failure: PersistenceFailure {
                    operation: PersistenceOperation::Cleanup,
                    phase: PersistencePhase::CommandExecution,
                    code: PersistenceFailureCode::Sqlite,
                    durability: DurabilityDisposition::NotCommitted,
                },
                counters: PersistenceCounters {
                    not_committed_batches: 1,
                    durability_unknown_batches: 2,
                    committed_but_degraded_batches: 3,
                    skipped_batches: 4,
                    skipped_owner_updates: 5,
                },
            })
        );

        let newest = valid_occurrence(444, "apply");
        let cut_prefix = "q".repeat(128);
        let cut_padding = "z".repeat(MAX_LOG_TAIL_BYTES as usize - newest.len() - 64);
        private_file(
            &log,
            format!("{cut_prefix}\npartial-record-fragment{cut_padding}\n{newest}").as_bytes(),
        );
        assert!(fs::metadata(&log).unwrap().len() > MAX_LOG_TAIL_BYTES);
        assert!(matches!(
            probe_persistence_log(&root),
            PersistenceLogVerdict::Found(PersistenceOccurrenceObservation {
                timestamp_ms: 444,
                ..
            })
        ));
    }

    #[test]
    fn i4_local_operational_path_abbreviates_and_escapes() {
        let home = Path::new("/home/safe-user");
        let path = Path::new("/home/safe-user/資料/line\nnext\t\u{1b}");
        let formatted = format_operational_path(path, Some(home.as_os_str()));

        assert_eq!(formatted, "~/資料/line\\nnext\\t\\x1b");
        assert!(!formatted.chars().any(char::is_control));
        assert_eq!(
            format_operational_path(Path::new("/other/資料"), Some(home.as_os_str())),
            "/other/資料"
        );
    }

    #[test]
    fn i4_local_operational_path_escape_is_injective() {
        let home_with_newline = OsStr::new("/home/line\nbreak");
        let literal_escape_path = Path::new("/home/line\\nbreak/private\rvalue");

        let formatted = format_operational_path(literal_escape_path, Some(home_with_newline));
        assert_eq!(formatted, "/home/line\\\\nbreak/private\\rvalue");
        assert_ne!(formatted, "~/private\\rvalue");
        assert!(!formatted.chars().any(char::is_control));

        let actual_home_path = Path::new("/home/line\nbreak/private\tvalue");
        let abbreviated = format_operational_path(actual_home_path, Some(home_with_newline));
        assert_eq!(abbreviated, "~/private\\tvalue");
        assert!(!abbreviated.chars().any(char::is_control));
    }

    #[test]
    fn i4_local_operational_path_c1_and_invalid_byte_are_distinct() {
        let valid_home = OsString::from_vec(b"/home/c1-\xc2\x80".to_vec());
        let invalid_home = OsString::from_vec(b"/home/c1-\x80".to_vec());
        let valid_path = PathBuf::from(OsString::from_vec(b"/home/c1-\xc2\x80/private".to_vec()));
        let invalid_path = PathBuf::from(OsString::from_vec(b"/home/c1-\x80/private".to_vec()));

        let valid_rendered = format_operational_path(&valid_path, Some(valid_home.as_os_str()));
        let invalid_rendered = format_operational_path(&invalid_path, Some(valid_home.as_os_str()));
        assert_eq!(valid_rendered, "~/private");
        assert_eq!(invalid_rendered, "/home/c1-\\x80/private");
        assert_ne!(valid_rendered, invalid_rendered);
        let valid_home_rendered = format_operational_path(Path::new(&valid_home), None);
        let invalid_home_rendered = format_operational_path(Path::new(&invalid_home), None);
        assert_eq!(valid_home_rendered, "/home/c1-\\u{80}");
        assert_eq!(invalid_home_rendered, "/home/c1-\\x80");
        assert_ne!(valid_home_rendered, invalid_home_rendered);
        assert!(!valid_rendered.chars().any(char::is_control));
        assert!(!invalid_rendered.chars().any(char::is_control));
    }

    #[test]
    fn i4_local_notice_inspection_absent_current_malformed_unreadable_mismatch() {
        let directory = tempdir().unwrap();
        let version = env!("CARGO_PKG_VERSION");
        let path = notice_path(directory.path(), version);

        assert_eq!(
            inspect_notice(directory.path(), version),
            NoticeVerdict::Absent
        );
        private_dir(&directory.path().join("herdr-top"));
        private_dir(path.parent().unwrap());
        private_file(&path, b"not json PRIVATE_NOTICE");
        assert_eq!(
            inspect_notice(directory.path(), version),
            NoticeVerdict::Malformed
        );
        private_file(
            &path,
            format!(
                "{{\"schema\":\"{NOTICE_SCHEMA}\",\"package_version\":\"different\",\"dismissed_at_ms\":1}}"
            )
            .as_bytes(),
        );
        assert_eq!(
            inspect_notice(directory.path(), version),
            NoticeVerdict::VersionMismatch
        );
        publish_notice(directory.path(), version, 456).unwrap();
        assert_eq!(
            inspect_notice(directory.path(), version),
            NoticeVerdict::Current {
                dismissed_at_ms: 456
            }
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        assert_eq!(
            inspect_notice(directory.path(), version),
            NoticeVerdict::Unreadable
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[test]
    fn i4_local_notice_modes_contents_atomicity_and_version_key() {
        let directory = tempdir().unwrap();
        let version = env!("CARGO_PKG_VERSION");
        publish_notice(directory.path(), version, 1).unwrap();
        publish_notice(directory.path(), version, 2).unwrap();
        let path = notice_path(directory.path(), version);
        let notice_dir = path.parent().unwrap();

        assert_eq!(
            fs::metadata(notice_dir).unwrap().permissions().mode() & 0o7777,
            0o700
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
            0o600
        );
        assert_eq!(
            path.file_name().unwrap(),
            OsStr::new(&format!("{NOTICE_SCHEMA}-{version}.json"))
        );
        let value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "schema": NOTICE_SCHEMA,
                "package_version": version,
                "dismissed_at_ms": 2
            })
        );
        assert_eq!(value.as_object().unwrap().len(), 3);
        assert_eq!(entries(notice_dir), vec![path]);
    }

    #[test]
    fn i4_local_notice_failure_retains_no_private_values() {
        let directory = tempdir().unwrap();
        let private = "PRIVATE_NOTICE_PATH_8B81";
        let state_base = directory.path().join(private);
        fs::write(&state_base, b"blocks directory creation").unwrap();

        let error = publish_notice(&state_base, private, 1).unwrap_err();
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert_eq!(display, "notice_publish_failed");
        assert!(!display.contains(private));
        assert!(!debug.contains(private));
    }
}
