//! Evidence-gated provider log admission.

use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, DirEntry, FileType};
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

use crate::model::Provider;

use super::facts::{EvidenceId, SessionScope};
use super::{DiscoveryRoot, ProviderDiagnostics};

#[cfg(test)]
use std::cell::RefCell;

/// Default bounded provider-log backfill window: one day.
pub const DEFAULT_BACKFILL_WINDOW_MS: i64 = 86_400_000;

/// One provider artifact kind retained by the evidence index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiscoveredArtifactKind {
    /// Claude root transcript.
    ClaudeSession {
        /// Provider-native Claude session ID.
        session_id: String,
    },
    /// Claude subagent transcript or metadata sidecar.
    ClaudeSubagent {
        /// Owning Claude root session ID.
        parent: String,
        /// Provider-native Claude subagent ID.
        agent_id: String,
    },
    /// Codex rollout transcript.
    CodexRollout {
        /// Provider-native Codex rollout ID.
        rollout_id: String,
    },
}

/// One artifact observed by bounded provider discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredArtifact {
    /// Provider that owns the artifact.
    pub provider: Provider,
    /// Absolute path observed during discovery.
    pub path: PathBuf,
    /// Provider-specific artifact identity and kind.
    pub kind: DiscoveredArtifactKind,
}

/// Identity-keyed artifact inventory used only for evidence matching.
#[derive(Clone, Debug, Default)]
pub struct AdmissionIndex {
    by_identity: HashMap<String, Vec<DiscoveredArtifact>>,
    had_errors: bool,
}

impl AdmissionIndex {
    /// Creates an empty evidence-matching index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one discovered Claude root transcript.
    pub fn insert_claude_session(&mut self, session_id: &str, path: PathBuf) {
        self.insert(
            session_id,
            DiscoveredArtifact {
                provider: Provider::Claude,
                path,
                kind: DiscoveredArtifactKind::ClaudeSession {
                    session_id: session_id.to_owned(),
                },
            },
        );
    }

    /// Records one discovered Claude subagent transcript or metadata sidecar.
    pub fn insert_claude_subagent(&mut self, parent: &str, agent_id: &str, path: PathBuf) {
        self.insert(
            agent_id,
            DiscoveredArtifact {
                provider: Provider::Claude,
                path,
                kind: DiscoveredArtifactKind::ClaudeSubagent {
                    parent: parent.to_owned(),
                    agent_id: agent_id.to_owned(),
                },
            },
        );
    }

    /// Records one discovered Codex rollout transcript.
    pub fn insert_codex_rollout(&mut self, rollout_id: &str, path: PathBuf) {
        self.insert(
            rollout_id,
            DiscoveredArtifact {
                provider: Provider::Codex,
                path,
                kind: DiscoveredArtifactKind::CodexRollout {
                    rollout_id: rollout_id.to_owned(),
                },
            },
        );
    }

    fn insert(&mut self, identity: &str, artifact: DiscoveredArtifact) {
        if identity.is_empty() {
            return;
        }
        let artifacts = self.by_identity.entry(identity.to_owned()).or_default();
        if !artifacts.contains(&artifact) {
            artifacts.push(artifact);
        }
    }

    /// Indexes rollout filenames only in UTC date shards on or after `anchor_ms`.
    ///
    /// Within the anchor day, parseable filename timestamps before the anchor are skipped.
    /// Individual nested-entry errors are recorded and do not discard healthy siblings.
    pub fn discover_codex_date_shards(root: &Path, anchor_ms: i64) -> io::Result<Self> {
        let result = Self::discover_codex_date_shards_inner(root, anchor_ms);
        #[cfg(test)]
        CODEX_SHARD_SCAN_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        #[cfg(test)]
        CODEX_DISCOVERY_FILE_TYPE_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        result
    }

    fn discover_codex_date_shards_inner(root: &Path, anchor_ms: i64) -> io::Result<Self> {
        const MILLIS_PER_DAY: i64 = 86_400_000;

        let anchor_day = anchor_ms.div_euclid(MILLIS_PER_DAY);
        let mut index = Self::new();
        for year in sorted_directory_entries(root, true, &mut index.had_errors)? {
            let Some(year_value) = fixed_decimal(&year.file_name(), 4) else {
                continue;
            };
            let year_path = year.path();
            let year_kind = match codex_discovery_file_type(&year, &year_path) {
                Ok(kind) => kind,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => {
                    index.had_errors = true;
                    continue;
                }
            };
            if !year_kind.is_dir() {
                continue;
            }
            for month in sorted_directory_entries(&year_path, false, &mut index.had_errors)? {
                let Some(month_value) = fixed_decimal(&month.file_name(), 2) else {
                    continue;
                };
                let month_path = month.path();
                let month_kind = match codex_discovery_file_type(&month, &month_path) {
                    Ok(kind) => kind,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(_) => {
                        index.had_errors = true;
                        continue;
                    }
                };
                if !month_kind.is_dir() {
                    continue;
                }
                for day in sorted_directory_entries(&month_path, false, &mut index.had_errors)? {
                    let Some(day_value) = fixed_decimal(&day.file_name(), 2) else {
                        continue;
                    };
                    let day_path = day.path();
                    let day_kind = match codex_discovery_file_type(&day, &day_path) {
                        Ok(kind) => kind,
                        Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                        Err(_) => {
                            index.had_errors = true;
                            continue;
                        }
                    };
                    if !day_kind.is_dir() {
                        continue;
                    }
                    let Some(shard_day) = civil_day(year_value, month_value, day_value) else {
                        continue;
                    };
                    if shard_day < anchor_day {
                        continue;
                    }
                    record_codex_shard_scan(&day_path);
                    for artifact in
                        sorted_directory_entries(&day_path, false, &mut index.had_errors)?
                    {
                        let artifact_path = artifact.path();
                        let artifact_kind =
                            match codex_discovery_file_type(&artifact, &artifact_path) {
                                Ok(kind) => kind,
                                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                                Err(_) => {
                                    index.had_errors = true;
                                    continue;
                                }
                            };
                        if !artifact_kind.is_file() {
                            continue;
                        }
                        let file_name = artifact.file_name();
                        let Some(file_name) = file_name.to_str() else {
                            continue;
                        };
                        if !file_name.starts_with("rollout-") || !file_name.ends_with(".jsonl") {
                            continue;
                        }
                        if shard_day == anchor_day
                            && rollout_filename_timestamp_ms(file_name)
                                .is_some_and(|timestamp_ms| timestamp_ms < anchor_ms)
                        {
                            continue;
                        }
                        let rollout_id = super::facts::scan_raw_ids(file_name)
                            .into_iter()
                            .filter_map(|id| match id {
                                EvidenceId::Uuid(uuid) => Some(uuid),
                                EvidenceId::ConfigDir(_) => None,
                            })
                            .next_back();
                        if let Some(rollout_id) = rollout_id {
                            index.insert_codex_rollout(&rollout_id, artifact_path);
                        }
                    }
                }
            }
        }
        Ok(index)
    }

    /// Reports whether an exact discovered artifact is keyed by `uuid`.
    #[must_use]
    pub fn contains_uuid(&self, uuid: &str) -> bool {
        self.by_identity.contains_key(uuid)
    }

    /// Reports whether any nested directory entry could not be inspected.
    #[must_use]
    pub const fn had_errors(&self) -> bool {
        self.had_errors
    }

    /// Returns every discovered artifact exactly keyed by `uuid`.
    #[must_use]
    pub fn artifacts_for_uuid(&self, uuid: &str) -> &[DiscoveredArtifact] {
        self.by_identity.get(uuid).map_or(&[], Vec::as_slice)
    }
}

/// A discovery root whose membership stays scoped to its evidence parent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedRoot {
    /// Already-admitted scope whose evidence produced this root.
    pub scope: SessionScope,
    /// Provider and scoped configuration path to discover.
    pub root: DiscoveryRoot,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ScopeKey {
    ClaudeRoot(String),
    ClaudeSubagent { parent: String, agent_id: String },
    Codex(String),
}

impl From<&SessionScope> for ScopeKey {
    fn from(scope: &SessionScope) -> Self {
        match scope {
            SessionScope::ClaudeRoot(session_id) => Self::ClaudeRoot(session_id.clone()),
            SessionScope::ClaudeSubagent { parent, agent_id } => Self::ClaudeSubagent {
                parent: parent.clone(),
                agent_id: agent_id.clone(),
            },
            SessionScope::Codex { rollout_id } => Self::Codex(rollout_id.clone()),
        }
    }
}

/// Evidence-gated provider artifact admission state.
#[derive(Clone, Debug)]
pub struct Admission {
    anchor_ms: i64,
    claude_sessions: HashSet<String>,
    codex_rollouts: HashSet<String>,
    admitted_scopes: HashSet<ScopeKey>,
    admitted_paths: HashSet<PathBuf>,
    derived_roots: Vec<DerivedRoot>,
}

impl Admission {
    /// Creates an empty admission graph with a hard file-mtime anchor.
    #[must_use]
    pub fn new(anchor_ms: i64) -> Self {
        Self {
            anchor_ms,
            claude_sessions: HashSet::new(),
            codex_rollouts: HashSet::new(),
            admitted_scopes: HashSet::new(),
            admitted_paths: HashSet::new(),
            derived_roots: Vec::new(),
        }
    }

    /// Admits the provider-native session identity observed in a herdr pane.
    pub fn admit_pane_session(&mut self, provider: Provider, session_id: &str) {
        if session_id.is_empty() {
            return;
        }
        match provider {
            Provider::Claude => {
                self.claude_sessions.insert(session_id.to_owned());
                self.admitted_scopes
                    .insert(ScopeKey::ClaudeRoot(session_id.to_owned()));
            }
            Provider::Codex => {
                self.codex_rollouts.insert(session_id.to_owned());
                self.admitted_scopes
                    .insert(ScopeKey::Codex(session_id.to_owned()));
            }
        }
    }

    /// Applies one allowlisted evidence ID emitted by an already-admitted parent scope.
    ///
    /// UUID evidence admits only exact paths present in `discovered`. Configuration-directory
    /// evidence derives a provider root without enumerating or opening it.
    pub fn on_evidence(
        &mut self,
        parent: &SessionScope,
        id: &EvidenceId,
        discovered: &AdmissionIndex,
    ) -> Option<SessionScope> {
        if !self.admitted_scopes.contains(&ScopeKey::from(parent)) {
            return None;
        }
        match id {
            EvidenceId::ConfigDir(config_dir) => {
                if !config_dir.is_absolute() {
                    return None;
                }
                let derived = DerivedRoot {
                    scope: parent.clone(),
                    root: DiscoveryRoot {
                        provider: Provider::Claude,
                        path: config_dir.join("projects"),
                    },
                };
                if !self.derived_roots.contains(&derived) {
                    self.derived_roots.push(derived);
                }
                Some(parent.clone())
            }
            EvidenceId::Uuid(uuid) => {
                let artifacts = discovered.artifacts_for_uuid(uuid);
                let scope = artifacts.first()?.scope();
                if artifacts.iter().any(|artifact| artifact.scope() != scope) {
                    return None;
                }
                for artifact in artifacts {
                    self.admitted_paths.insert(artifact.path.clone());
                }
                self.admitted_scopes.insert(ScopeKey::from(&scope));
                Some(scope)
            }
        }
    }

    /// Reports whether a path is admitted by pane identity or exact lineage evidence.
    ///
    /// Any path containing a `tool-results` component is categorically rejected.
    #[must_use]
    pub fn is_admitted_path(&self, path: &Path) -> bool {
        if has_component(path, OsStr::new("tool-results")) {
            return false;
        }
        if self.admitted_paths.contains(path) {
            return true;
        }
        if self
            .codex_rollouts
            .iter()
            .any(|rollout_id| codex_path_matches(path, rollout_id))
        {
            return true;
        }
        self.claude_sessions
            .iter()
            .any(|session_id| claude_session_path_matches(path, session_id))
    }

    /// Applies the hard backfill anchor to an otherwise admitted regular file.
    #[must_use]
    pub fn is_admitted_file(&self, path: &Path, modified_ms: i64) -> bool {
        self.is_admitted_path(path)
            && (modified_ms >= self.anchor_ms
                || self.admitted_paths.contains(path)
                || self.is_pane_root_path(path))
    }

    /// Returns provider roots derived from allowlisted configuration-directory evidence.
    #[must_use]
    pub fn derived_roots(&self) -> &[DerivedRoot] {
        &self.derived_roots
    }

    /// Returns the immutable hard backfill anchor for this admission graph.
    #[must_use]
    pub const fn anchor_ms(&self) -> i64 {
        self.anchor_ms
    }

    fn is_pane_root_path(&self, path: &Path) -> bool {
        self.codex_rollouts
            .iter()
            .any(|rollout_id| codex_path_matches(path, rollout_id))
            || self
                .claude_sessions
                .iter()
                .any(|session_id| claude_root_path_matches(path, session_id))
    }
}

/// Computes the hard backfill anchor, allowing the database only to narrow the window.
#[must_use]
pub fn backfill_anchor_ms(earliest_db_event: Option<i64>, now_ms: i64, window_ms: i64) -> i64 {
    let window_anchor = now_ms.saturating_sub(window_ms);
    earliest_db_event.map_or(window_anchor, |earliest| earliest.max(window_anchor))
}

/// Parses a positive UTF-8 decimal millisecond window or returns the one-day default.
#[must_use]
pub fn parse_backfill_window_ms(value: Option<&OsStr>) -> i64 {
    value
        .and_then(OsStr::to_str)
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BACKFILL_WINDOW_MS)
}

/// Recomputes the zero-based record ordinal by counting newlines before `byte_offset`.
///
/// The read is routed through the admission-gated open seam and is used when reopening an
/// artifact at a nonzero byte offset without a persisted ordinal.
pub fn record_ordinal_at_offset(
    root: &Path,
    relative: &Path,
    byte_offset: u64,
    admission: &Admission,
    diagnostics: &ProviderDiagnostics,
) -> io::Result<u64> {
    let Some(mut file) = super::open_admitted_regular_file(root, relative, admission, diagnostics)?
    else {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "provider artifact is not admitted",
        ));
    };
    let mut remaining = byte_offset;
    let mut ordinal = 0_u64;
    let mut buffer = [0_u8; 8 * 1024];
    while remaining > 0 {
        let limit = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let read = file.read(&mut buffer[..limit])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "provider offset exceeds artifact length",
            ));
        }
        ordinal = ordinal.saturating_add(
            u64::try_from(buffer[..read].iter().filter(|byte| **byte == b'\n').count())
                .unwrap_or(u64::MAX),
        );
        remaining -= read as u64;
    }
    Ok(ordinal)
}

#[cfg(test)]
type CodexShardScanHook = Box<dyn FnMut(&Path)>;

#[cfg(test)]
type CodexDiscoveryFileTypeHook = Box<dyn FnMut(&Path) -> io::Result<()>>;

#[cfg(test)]
thread_local! {
    static CODEX_SHARD_SCAN_HOOK: RefCell<Option<CodexShardScanHook>> =
        const { RefCell::new(None) };
    static CODEX_DISCOVERY_FILE_TYPE_HOOK: RefCell<Option<CodexDiscoveryFileTypeHook>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
fn set_codex_shard_scan_hook(hook: impl FnMut(&Path) + 'static) {
    CODEX_SHARD_SCAN_HOOK.with(|slot| {
        assert!(
            slot.borrow_mut().replace(Box::new(hook)).is_none(),
            "codex shard scan hook was already installed"
        );
    });
}

#[cfg(test)]
fn set_codex_discovery_file_type_hook(hook: impl FnMut(&Path) -> io::Result<()> + 'static) {
    CODEX_DISCOVERY_FILE_TYPE_HOOK.with(|slot| {
        assert!(
            slot.borrow_mut().replace(Box::new(hook)).is_none(),
            "codex discovery file-type hook was already installed"
        );
    });
}

fn record_codex_shard_scan(path: &Path) {
    #[cfg(test)]
    CODEX_SHARD_SCAN_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(path);
        }
    });
    #[cfg(not(test))]
    let _ = path;
}

impl DiscoveredArtifact {
    fn scope(&self) -> SessionScope {
        match &self.kind {
            DiscoveredArtifactKind::ClaudeSession { session_id } => {
                SessionScope::ClaudeRoot(session_id.clone())
            }
            DiscoveredArtifactKind::ClaudeSubagent { parent, agent_id } => {
                SessionScope::ClaudeSubagent {
                    parent: parent.clone(),
                    agent_id: agent_id.clone(),
                }
            }
            DiscoveredArtifactKind::CodexRollout { rollout_id } => SessionScope::Codex {
                rollout_id: rollout_id.clone(),
            },
        }
    }
}

fn sorted_directory_entries(
    path: &Path,
    is_root: bool,
    had_errors: &mut bool,
) -> io::Result<Vec<DirEntry>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) if is_root => return Err(error),
        Err(_) => {
            *had_errors = true;
            return Ok(Vec::new());
        }
    };
    let mut found = Vec::new();
    for entry in entries {
        match entry {
            Ok(entry) => found.push(entry),
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => *had_errors = true,
        }
    }
    let mut entries = found;
    entries.sort_by_key(DirEntry::file_name);
    Ok(entries)
}

fn codex_discovery_file_type(entry: &DirEntry, path: &Path) -> io::Result<FileType> {
    #[cfg(test)]
    CODEX_DISCOVERY_FILE_TYPE_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(path)?;
        }
        Ok::<(), io::Error>(())
    })?;
    #[cfg(not(test))]
    let _ = path;
    entry.file_type()
}

fn fixed_decimal(value: &OsString, width: usize) -> Option<u32> {
    let value = value.to_str()?;
    (value.len() == width && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse().ok())?
}

fn civil_day(year: u32, month: u32, day: u32) -> Option<i64> {
    if year > 9_999
        || !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
    {
        return None;
    }
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

fn rollout_filename_timestamp_ms(file_name: &str) -> Option<i64> {
    const MILLIS_PER_DAY: i64 = 86_400_000;

    let value = file_name.strip_prefix("rollout-")?;
    let timestamp = value.get(..19)?;
    if value.as_bytes().get(19) != Some(&b'-')
        || timestamp.as_bytes().get(4) != Some(&b'-')
        || timestamp.as_bytes().get(7) != Some(&b'-')
        || timestamp.as_bytes().get(10) != Some(&b'T')
        || timestamp.as_bytes().get(13) != Some(&b'-')
        || timestamp.as_bytes().get(16) != Some(&b'-')
    {
        return None;
    }
    let year = timestamp.get(0..4)?.parse::<u32>().ok()?;
    let month = timestamp.get(5..7)?.parse::<u32>().ok()?;
    let day = timestamp.get(8..10)?.parse::<u32>().ok()?;
    let hour = timestamp.get(11..13)?.parse::<u32>().ok()?;
    let minute = timestamp.get(14..16)?.parse::<u32>().ok()?;
    let second = timestamp.get(17..19)?.parse::<u32>().ok()?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    civil_day(year, month, day)?
        .checked_mul(MILLIS_PER_DAY)?
        .checked_add(i64::from(hour * 3_600 + minute * 60 + second) * 1_000)
}

const fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(400) || (year.is_multiple_of(4) && !year.is_multiple_of(100)) => {
            29
        }
        2 => 28,
        _ => 0,
    }
}

fn has_component(path: &Path, expected: &OsStr) -> bool {
    path.components()
        .any(|component| matches!(component, Component::Normal(value) if value == expected))
}

fn normal_components(path: &Path) -> Option<Vec<&OsStr>> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(Some(value)),
            Component::CurDir | Component::RootDir => None,
            Component::ParentDir | Component::Prefix(_) => Some(None),
        })
        .collect()
}

fn codex_path_matches(path: &Path, rollout_id: &str) -> bool {
    let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
        return false;
    };
    let Some(stem) = file_name.strip_suffix(".jsonl") else {
        return false;
    };
    stem == rollout_id
        || stem
            .strip_suffix(rollout_id)
            .is_some_and(|prefix| prefix.ends_with('-'))
}

fn claude_session_path_matches(path: &Path, session_id: &str) -> bool {
    claude_root_path_matches(path, session_id)
        || claude_subagent_path_matches(path, session_id, None)
}

fn claude_root_path_matches(path: &Path, session_id: &str) -> bool {
    path.extension() == Some(OsStr::new("jsonl"))
        && path.file_stem().and_then(OsStr::to_str) == Some(session_id)
}

fn claude_subagent_path_matches(
    path: &Path,
    parent: &str,
    expected_agent_id: Option<&str>,
) -> bool {
    let Some(components) = normal_components(path) else {
        return false;
    };
    components.windows(2).enumerate().any(|(index, pair)| {
        if pair[0] != OsStr::new(parent) || pair[1] != OsStr::new("subagents") {
            return false;
        }
        match components.get(index + 2) {
            None => true,
            Some(file_name) if components.len() == index + 3 => subagent_artifact_id(file_name)
                .is_some_and(|agent_id| {
                    expected_agent_id.is_none_or(|expected| agent_id == expected)
                }),
            Some(_) => false,
        }
    })
}

fn subagent_artifact_id(file_name: &OsStr) -> Option<&str> {
    let file_name = file_name.to_str()?;
    let stem = file_name
        .strip_suffix(".jsonl")
        .or_else(|| file_name.strip_suffix(".meta.json"))?;
    stem.strip_prefix("agent-").filter(|id| !id.is_empty())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs::{self, OpenOptions};
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use crate::model::Provider;
    use crate::provider::claude::{ClaudePathTopology, path_topology};
    use crate::provider::facts::{EvidenceId, SessionScope};
    use crate::provider::tail::{MAX_TAIL_RECORD_BYTES, RECORD_TOO_LONG_ERROR};
    use crate::provider::{
        FirstSeenBaseline, FsReadBoundary, ProviderDiagnostics, TailFile,
        open_admitted_regular_file,
    };

    use super::*;

    const PARENT: &str = "11111111-1111-4111-8111-111111111111";
    const ROLLOUT: &str = "22222222-2222-4222-8222-222222222222";
    const STRANGER: &str = "33333333-3333-4333-8333-333333333333";

    #[test]
    fn open_admitted_regular_file_gates_strangers_and_tool_results() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("projects");
        let admitted = PathBuf::from(format!("workspace/{PARENT}.jsonl"));
        let stranger = PathBuf::from(format!("workspace/{STRANGER}.jsonl"));
        let tool_results = [
            PathBuf::from(format!("workspace/tool-results/{PARENT}.jsonl")),
            PathBuf::from(format!(
                "workspace/tool-results/{PARENT}/subagents/agent-child.jsonl"
            )),
        ];
        for relative in [&admitted, &stranger]
            .into_iter()
            .chain(tool_results.iter())
        {
            let path = root.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"{}\n").unwrap();
        }

        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);

        let admitted_diagnostics = ProviderDiagnostics::default();
        assert!(
            open_admitted_regular_file(&root, &admitted, &admission, &admitted_diagnostics)
                .unwrap()
                .is_some()
        );
        assert_eq!(admitted_diagnostics.admission_open_attempts(), 1);

        let stranger_diagnostics = ProviderDiagnostics::default();
        assert!(
            open_admitted_regular_file(&root, &stranger, &admission, &stranger_diagnostics)
                .unwrap()
                .is_none()
        );
        assert_eq!(stranger_diagnostics.admission_open_attempts(), 0);

        for tool_result in tool_results {
            let tool_diagnostics = ProviderDiagnostics::default();
            assert!(
                open_admitted_regular_file(&root, &tool_result, &admission, &tool_diagnostics)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(tool_diagnostics.admission_open_attempts(), 0);
        }
    }

    #[test]
    fn tool_results_contents_are_never_enumerated() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("projects");
        fs::create_dir_all(root.join("workspace/tool-results")).unwrap();
        fs::write(root.join("workspace/allowed.jsonl"), b"{}\n").unwrap();
        fs::write(
            root.join("workspace/tool-results/private-output.jsonl"),
            b"{}\n",
        )
        .unwrap();

        let discovery = crate::provider::discover_artifacts(&root).unwrap();

        assert_eq!(discovery.paths, [PathBuf::from("workspace/allowed.jsonl")]);
    }

    #[test]
    fn subagents_dir_admitted_via_parent() {
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);
        let directory = PathBuf::from(format!("projects/workspace/{PARENT}/subagents"));

        assert!(admission.is_admitted_path(&directory));
        assert!(admission.is_admitted_path(&directory.join("agent-child.jsonl")));
        assert!(admission.is_admitted_path(&directory.join("agent-child.meta.json")));
    }

    #[test]
    fn evidence_uuid_admits_matching_rollout_only() {
        let rollout_path = PathBuf::from(format!(
            "/home/user/.codex/sessions/2026/08/24/rollout-2026-08-24T00-00-00-{ROLLOUT}.jsonl"
        ));
        let mut discovered = AdmissionIndex::new();
        discovered.insert_codex_rollout(ROLLOUT, rollout_path.clone());
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert_eq!(
            admission.on_evidence(&parent, &EvidenceId::Uuid(STRANGER.to_owned()), &discovered),
            None
        );
        assert!(!admission.is_admitted_path(Path::new(&format!("/tmp/rollout-{STRANGER}.jsonl"))));
        assert_eq!(
            admission.on_evidence(&parent, &EvidenceId::Uuid(ROLLOUT.to_owned()), &discovered),
            Some(SessionScope::Codex {
                rollout_id: ROLLOUT.to_owned(),
            })
        );
        assert!(admission.is_admitted_path(&rollout_path));
    }

    #[test]
    fn evidence_admission_is_path_exact_across_shards() {
        let file_name = format!("rollout-2026-08-24T00-00-00-{ROLLOUT}.jsonl");
        let admitted_path = PathBuf::from("/home/user/.codex/sessions/2026/08/24").join(&file_name);
        let anchored_out_copy =
            PathBuf::from("/home/user/.codex/sessions/2026/08/20").join(&file_name);
        let mut discovered = AdmissionIndex::new();
        discovered.insert_codex_rollout(ROLLOUT, admitted_path.clone());
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert!(
            admission
                .on_evidence(&parent, &EvidenceId::Uuid(ROLLOUT.to_owned()), &discovered,)
                .is_some()
        );
        assert!(admission.is_admitted_path(&admitted_path));
        assert!(!admission.is_admitted_path(&anchored_out_copy));
    }

    #[test]
    fn evidence_requires_an_already_admitted_parent() {
        let rollout_path = PathBuf::from(format!(
            "/home/user/.codex/sessions/2026/08/24/rollout-2026-08-24T00-00-00-{ROLLOUT}.jsonl"
        ));
        let mut discovered = AdmissionIndex::new();
        discovered.insert_codex_rollout(ROLLOUT, rollout_path.clone());
        let mut admission = Admission::new(0);

        assert_eq!(
            admission.on_evidence(
                &SessionScope::ClaudeRoot(PARENT.to_owned()),
                &EvidenceId::Uuid(ROLLOUT.to_owned()),
                &discovered,
            ),
            None
        );
        assert!(!admission.is_admitted_path(&rollout_path));
    }

    #[test]
    fn config_dir_evidence_creates_scoped_root() {
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let config_dir = PathBuf::from("/opt/claude-secondary");
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert_eq!(
            admission.on_evidence(
                &parent,
                &EvidenceId::ConfigDir(config_dir.clone()),
                &AdmissionIndex::new(),
            ),
            Some(parent.clone())
        );
        assert_eq!(
            admission.derived_roots(),
            &[DerivedRoot {
                scope: parent,
                root: crate::provider::DiscoveryRoot {
                    provider: Provider::Claude,
                    path: config_dir.join("projects"),
                },
            }]
        );
    }

    #[test]
    fn anchor_is_max_of_db_event_and_window() {
        assert_eq!(backfill_anchor_ms(None, 1_000, 400), 600);
        assert_eq!(backfill_anchor_ms(Some(550), 1_000, 400), 600);
        assert_eq!(backfill_anchor_ms(Some(850), 1_000, 400), 850);
    }

    #[test]
    fn per_file_anchor_bounds_descendants_but_not_exact_or_pane_roots() {
        let mut admission = Admission::new(1_000);
        admission.admit_pane_session(Provider::Claude, PARENT);
        let pane_root = PathBuf::from(format!("/logs/workspace/{PARENT}.jsonl"));
        let subagent = PathBuf::from(format!(
            "/logs/workspace/{PARENT}/subagents/agent-child.jsonl"
        ));
        let evidence_path = PathBuf::from(format!(
            "/logs/sessions/2026/08/24/rollout-2026-08-24T12-00-00-{ROLLOUT}.jsonl"
        ));
        let mut index = AdmissionIndex::new();
        index.insert_codex_rollout(ROLLOUT, evidence_path.clone());
        assert!(
            admission
                .on_evidence(
                    &SessionScope::ClaudeRoot(PARENT.to_owned()),
                    &EvidenceId::Uuid(ROLLOUT.to_owned()),
                    &index,
                )
                .is_some()
        );

        assert!(admission.is_admitted_file(&pane_root, 100));
        assert!(!admission.is_admitted_file(&subagent, 999));
        assert!(admission.is_admitted_file(&evidence_path, 100));
        assert!(admission.is_admitted_file(&subagent, 1_000));
    }

    #[test]
    fn date_shard_scan_bounded_by_anchor() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("sessions");
        let shards = [
            ("2026/08/22", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            ("2026/08/23", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            ("2026/08/24", "cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        ];
        for (shard, id) in shards {
            let path = root.join(shard);
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join(format!("rollout-example-{id}.jsonl")), b"{}\n").unwrap();
        }
        let scanned = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&scanned);
        set_codex_shard_scan_hook(move |path| observed.lock().unwrap().push(path.to_path_buf()));

        let index = AdmissionIndex::discover_codex_date_shards(
            &root,
            1_787_486_400_000, // 2026-08-23T12:00:00Z
        )
        .unwrap();

        assert!(!index.contains_uuid("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"));
        assert!(index.contains_uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"));
        assert!(index.contains_uuid("cccccccc-cccc-4ccc-8ccc-cccccccccccc"));
        assert_eq!(
            *scanned.lock().unwrap(),
            [root.join("2026/08/23"), root.join("2026/08/24")]
        );
    }

    #[test]
    fn anchor_day_rollouts_are_bounded_by_filename_timestamp() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("sessions");
        let anchor_day = root.join("2026/08/23");
        let later_day = root.join("2026/08/24");
        fs::create_dir_all(&anchor_day).unwrap();
        fs::create_dir_all(&later_day).unwrap();
        let before = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
        let equal = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
        let after = "ffffffff-ffff-4fff-8fff-ffffffffffff";
        let malformed = "99999999-9999-4999-8999-999999999999";
        let later = "77777777-7777-4777-8777-777777777777";
        for (name, id) in [
            ("rollout-2026-08-23T11-59-59", before),
            ("rollout-2026-08-23T12-00-00", equal),
            ("rollout-2026-08-23T12-00-01", after),
            ("rollout-not-a-time", malformed),
        ] {
            fs::write(anchor_day.join(format!("{name}-{id}.jsonl")), b"{}\n").unwrap();
        }
        fs::write(
            later_day.join(format!("rollout-2026-08-24T00-00-00-{later}.jsonl")),
            b"{}\n",
        )
        .unwrap();

        let index = AdmissionIndex::discover_codex_date_shards(
            &root,
            1_787_486_400_000, // 2026-08-23T12:00:00Z
        )
        .unwrap();

        assert!(!index.contains_uuid(before));
        assert!(index.contains_uuid(equal));
        assert!(index.contains_uuid(after));
        assert!(index.contains_uuid(malformed));
        assert!(index.contains_uuid(later));
    }

    #[test]
    fn codex_shard_entry_error_retains_sibling_rollouts() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("sessions");
        let shard = root.join("2026/08/24");
        fs::create_dir_all(&shard).unwrap();
        let unreadable_id = "11111111-aaaa-4111-8111-111111111111";
        let sibling_id = "22222222-bbbb-4222-8222-222222222222";
        let unreadable = shard.join(format!("rollout-2026-08-24T00-00-00-{unreadable_id}.jsonl"));
        let sibling = shard.join(format!("rollout-2026-08-24T00-00-01-{sibling_id}.jsonl"));
        fs::write(&unreadable, b"{}\n").unwrap();
        fs::write(&sibling, b"{}\n").unwrap();
        let injected_path = unreadable.clone();
        set_codex_discovery_file_type_hook(move |path| {
            if path == injected_path {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected unreadable rollout",
                ))
            } else {
                Ok(())
            }
        });

        let index = AdmissionIndex::discover_codex_date_shards(&root, 0).unwrap();

        assert!(index.had_errors());
        assert!(!index.contains_uuid(unreadable_id));
        assert!(index.contains_uuid(sibling_id));
    }

    #[test]
    fn offsets_advance_incrementally_via_existing_tail() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("rollout.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"").unwrap();
        let mut boundary = FsReadBoundary;
        let mut tail = TailFile::open(
            directory.path(),
            relative,
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();

        let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
        writer.write_all(b"one\n").unwrap();
        writer.flush().unwrap();
        assert_eq!(tail.poll(&mut boundary).unwrap().len(), 1);
        assert_eq!(tail.offset(), 4);

        writer.write_all(b"two\n").unwrap();
        writer.flush().unwrap();
        assert_eq!(tail.poll(&mut boundary).unwrap().len(), 1);
        assert_eq!(tail.offset(), 8);
    }

    #[test]
    fn record_ordinal_is_stable_after_reopen_at_nonzero_offset() {
        let directory = tempfile::tempdir().unwrap();
        let relative = PathBuf::from(format!("rollout-example-{ROLLOUT}.jsonl"));
        let mut fixture = b"first\r\n".to_vec();
        fixture.extend(vec![b'x'; MAX_TAIL_RECORD_BYTES + 1]);
        fixture.push(b'\n');
        fixture.extend_from_slice(b"trailing-partial");
        fs::write(directory.path().join(&relative), &fixture).unwrap();
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Codex, ROLLOUT);
        let mut boundary = FsReadBoundary;
        let mut tail = TailFile::open(
            directory.path(),
            &relative,
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();
        let mut records = Vec::new();
        while tail.offset() < fixture.len() as u64 {
            records.extend(tail.poll(&mut boundary).unwrap());
        }

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].bytes, b"first");
        assert_eq!(records[0].error_code, None);
        assert!(records[1].bytes.is_empty());
        assert_eq!(records[1].error_code, Some(RECORD_TOO_LONG_ERROR));
        assert!(tail.poll(&mut boundary).unwrap().is_empty());

        for (record_index, record) in records.iter().enumerate() {
            assert_eq!(
                record_ordinal_at_offset(
                    directory.path(),
                    &relative,
                    record.offset,
                    &admission,
                    &ProviderDiagnostics::default(),
                )
                .unwrap(),
                record_index as u64
            );
        }

        let reopened_ordinal = record_ordinal_at_offset(
            directory.path(),
            &relative,
            records[1].offset,
            &admission,
            &ProviderDiagnostics::default(),
        )
        .unwrap();
        assert_eq!(reopened_ordinal, 1);
    }

    #[test]
    fn backfill_window_parser_uses_positive_utf8_i64_or_default() {
        assert_eq!(parse_backfill_window_ms(None), DEFAULT_BACKFILL_WINDOW_MS);
        assert_eq!(parse_backfill_window_ms(Some(OsStr::new("42"))), 42);
        for value in ["", "0", "-1", "1.5", " 42", "9223372036854775808"] {
            assert_eq!(
                parse_backfill_window_ms(Some(OsStr::new(value))),
                DEFAULT_BACKFILL_WINDOW_MS
            );
        }
    }

    #[test]
    fn claude_topology_represents_subagent_directory_and_sidecars() {
        assert_eq!(
            path_topology(Path::new(&format!("workspace/{PARENT}/subagents"))),
            Some(ClaudePathTopology::SubagentsDir {
                parent_session: PARENT.to_owned(),
            })
        );
        assert_eq!(
            path_topology(Path::new(&format!(
                "workspace/{PARENT}/subagents/agent-child.jsonl"
            ))),
            Some(ClaudePathTopology::Subagent {
                parent_session: PARENT.to_owned(),
                agent_id: "child".to_owned(),
            })
        );
        assert_eq!(
            path_topology(Path::new(&format!(
                "workspace/{PARENT}/subagents/agent-child.meta.json"
            ))),
            Some(ClaudePathTopology::SubagentMeta {
                parent_session: PARENT.to_owned(),
                agent_id: "child".to_owned(),
            })
        );
        assert_eq!(
            path_topology(Path::new(&format!(
                "workspace/{PARENT}/subagents/agent-.jsonl"
            ))),
            None
        );
        assert_eq!(
            path_topology(Path::new(&format!(
                "workspace/{PARENT}/subagents/agent-.meta.json"
            ))),
            None
        );
    }
}
