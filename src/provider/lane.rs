//! Evidence-gated provider log admission.

use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, DirEntry};
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
    ClaudeSession { session_id: String },
    /// Claude subagent transcript or metadata sidecar.
    ClaudeSubagent { parent: String, agent_id: String },
    /// Codex rollout transcript.
    CodexRollout { rollout_id: String },
}

/// One artifact observed by bounded provider discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredArtifact {
    pub provider: Provider,
    pub path: PathBuf,
    pub kind: DiscoveredArtifactKind,
}

/// Identity-keyed artifact inventory used only for evidence matching.
#[derive(Clone, Debug, Default)]
pub struct DiscoveredIndex {
    by_identity: HashMap<String, Vec<DiscoveredArtifact>>,
}

impl DiscoveredIndex {
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
    pub fn discover_codex_date_shards(root: &Path, anchor_ms: i64) -> io::Result<Self> {
        let result = Self::discover_codex_date_shards_inner(root, anchor_ms);
        #[cfg(test)]
        CODEX_SHARD_SCAN_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        result
    }

    fn discover_codex_date_shards_inner(root: &Path, anchor_ms: i64) -> io::Result<Self> {
        const MILLIS_PER_DAY: i64 = 86_400_000;

        let anchor_day = anchor_ms.div_euclid(MILLIS_PER_DAY);
        let mut index = Self::new();
        for year in sorted_directory_entries(root)? {
            let Some(year_value) = fixed_decimal(&year.file_name(), 4) else {
                continue;
            };
            if !year.file_type()?.is_dir() {
                continue;
            }
            for month in sorted_directory_entries(&year.path())? {
                let Some(month_value) = fixed_decimal(&month.file_name(), 2) else {
                    continue;
                };
                if !month.file_type()?.is_dir() {
                    continue;
                }
                for day in sorted_directory_entries(&month.path())? {
                    let Some(day_value) = fixed_decimal(&day.file_name(), 2) else {
                        continue;
                    };
                    if !day.file_type()?.is_dir() {
                        continue;
                    }
                    let Some(shard_day) = civil_day(year_value, month_value, day_value) else {
                        continue;
                    };
                    if shard_day < anchor_day {
                        continue;
                    }
                    record_codex_shard_scan(&day.path());
                    for artifact in sorted_directory_entries(&day.path())? {
                        if !artifact.file_type()?.is_file() {
                            continue;
                        }
                        let file_name = artifact.file_name();
                        let Some(file_name) = file_name.to_str() else {
                            continue;
                        };
                        if !file_name.starts_with("rollout-") || !file_name.ends_with(".jsonl") {
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
                            index.insert_codex_rollout(&rollout_id, artifact.path());
                        }
                    }
                }
            }
        }
        Ok(index)
    }

    #[must_use]
    pub fn contains_uuid(&self, uuid: &str) -> bool {
        self.by_identity.contains_key(uuid)
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
    pub scope: SessionScope,
    pub root: DiscoveryRoot,
}

/// Evidence-gated provider artifact admission state.
#[derive(Clone, Debug)]
pub struct Admission {
    anchor_ms: i64,
    claude_sessions: HashSet<String>,
    claude_subagents: HashSet<(String, String)>,
    codex_rollouts: HashSet<String>,
    admitted_paths: HashSet<PathBuf>,
    derived_roots: Vec<DerivedRoot>,
}

impl Admission {
    #[must_use]
    pub fn new(anchor_ms: i64) -> Self {
        Self {
            anchor_ms,
            claude_sessions: HashSet::new(),
            claude_subagents: HashSet::new(),
            codex_rollouts: HashSet::new(),
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
            }
            Provider::Codex => {
                self.codex_rollouts.insert(session_id.to_owned());
            }
        }
    }

    pub fn on_evidence(
        &mut self,
        parent: &SessionScope,
        id: &EvidenceId,
        discovered: &DiscoveredIndex,
    ) -> Option<SessionScope> {
        match id {
            EvidenceId::ConfigDir(config_dir) => {
                if !config_dir.is_absolute() {
                    return None;
                }
                self.admit_scope(parent);
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
                self.admit_scope(&scope);
                Some(scope)
            }
        }
    }

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
        if self
            .claude_sessions
            .iter()
            .any(|session_id| claude_session_path_matches(path, session_id))
        {
            return true;
        }
        self.claude_subagents
            .iter()
            .any(|(parent, agent_id)| claude_subagent_path_matches(path, parent, Some(agent_id)))
    }

    #[must_use]
    pub fn derived_roots(&self) -> &[DerivedRoot] {
        &self.derived_roots
    }

    /// Returns the immutable hard backfill anchor for this admission graph.
    #[must_use]
    pub const fn anchor_ms(&self) -> i64 {
        self.anchor_ms
    }

    fn admit_scope(&mut self, scope: &SessionScope) {
        match scope {
            SessionScope::ClaudeRoot(session_id) => {
                self.claude_sessions.insert(session_id.clone());
            }
            SessionScope::ClaudeSubagent { parent, agent_id } => {
                self.claude_sessions.insert(parent.clone());
                self.claude_subagents
                    .insert((parent.clone(), agent_id.clone()));
            }
            SessionScope::Codex { rollout_id } => {
                self.codex_rollouts.insert(rollout_id.clone());
            }
        }
    }
}

#[must_use]
pub fn backfill_anchor_ms(earliest_db_event: Option<i64>, now_ms: i64, window_ms: i64) -> i64 {
    let window_anchor = now_ms.saturating_sub(window_ms);
    earliest_db_event.map_or(window_anchor, |earliest| earliest.max(window_anchor))
}

#[must_use]
pub fn parse_backfill_window_ms(value: Option<&OsStr>) -> i64 {
    value
        .and_then(OsStr::to_str)
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BACKFILL_WINDOW_MS)
}

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
thread_local! {
    static CODEX_SHARD_SCAN_HOOK: RefCell<Option<CodexShardScanHook>> =
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

fn sorted_directory_entries(path: &Path) -> io::Result<Vec<DirEntry>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut entries = entries.collect::<io::Result<Vec<_>>>()?;
    entries.sort_by_key(DirEntry::file_name);
    Ok(entries)
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
    path.extension() == Some(OsStr::new("jsonl"))
        && path.file_stem().and_then(OsStr::to_str) == Some(session_id)
        || claude_subagent_path_matches(path, session_id, None)
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
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use crate::model::Provider;
    use crate::provider::claude::{ClaudePathTopology, path_topology};
    use crate::provider::facts::{EvidenceId, SessionScope};
    use crate::provider::{
        FirstSeenBaseline, FsReadBoundary, ProviderDiagnostics, TailFile,
        open_admitted_regular_file,
    };

    use super::*;

    const PARENT: &str = "11111111-1111-4111-8111-111111111111";
    const ROLLOUT: &str = "22222222-2222-4222-8222-222222222222";
    const STRANGER: &str = "33333333-3333-4333-8333-333333333333";

    #[test]
    fn unadmitted_files_are_never_opened() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("projects");
        let admitted = PathBuf::from(format!("workspace/{PARENT}.jsonl"));
        let stranger = PathBuf::from(format!("workspace/{STRANGER}.jsonl"));
        let tool_result = PathBuf::from(format!(
            "workspace/{PARENT}/tool-results/private-output.jsonl"
        ));
        for relative in [&admitted, &stranger, &tool_result] {
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

        let tool_diagnostics = ProviderDiagnostics::default();
        assert!(
            open_admitted_regular_file(&root, &tool_result, &admission, &tool_diagnostics)
                .unwrap()
                .is_none()
        );
        assert_eq!(tool_diagnostics.admission_open_attempts(), 0);
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
        let mut discovered = DiscoveredIndex::new();
        discovered.insert_codex_rollout(ROLLOUT, rollout_path.clone());
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let mut admission = Admission::new(0);

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
    fn config_dir_evidence_creates_scoped_root() {
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let config_dir = PathBuf::from("/opt/claude-secondary");
        let mut admission = Admission::new(0);

        assert_eq!(
            admission.on_evidence(
                &parent,
                &EvidenceId::ConfigDir(config_dir.clone()),
                &DiscoveredIndex::new(),
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

        let index = DiscoveredIndex::discover_codex_date_shards(
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
        fs::write(directory.path().join(&relative), b"zero\none\ntwo\n").unwrap();
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Codex, ROLLOUT);
        let second_record_offset = 5;

        let before_restart = record_ordinal_at_offset(
            directory.path(),
            &relative,
            second_record_offset,
            &admission,
            &ProviderDiagnostics::default(),
        )
        .unwrap();
        let after_restart = record_ordinal_at_offset(
            directory.path(),
            &relative,
            second_record_offset,
            &admission,
            &ProviderDiagnostics::default(),
        )
        .unwrap();

        assert_eq!(before_restart, 1);
        assert_eq!(after_restart, 1);
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
    }
}
