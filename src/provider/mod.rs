#![allow(unsafe_code)]
//! Provider discovery, coalescing, notification, and dedicated I/O-thread substrate.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{CStr, CString, OsStr};
use std::fs::{self, File};
use std::io::{self, Read};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use crate::model::{ExecState, MinimalProviderMetadata, Provider, ProviderDiagnosticsHandle};

pub mod claude;
pub mod codex;
pub mod tail;

pub use tail::{
    FileSnapshot, FirstSeenBaseline, FsReadBoundary, ReadBoundary, ReadChunk, TailFile, TailRecord,
};

/// Provider filesystem fallback interval.
pub const RESCAN_INTERVAL: Duration = Duration::from_secs(2);
/// Fixed control-channel capacity.
pub const CONTROL_CHANNEL_CAPACITY: usize = 256;
/// Maximum number of entities retained before file advancement must pause.
pub const PENDING_ENTITY_CAPACITY: usize = 4_096;
/// Maximum number of directories with active filesystem watches.
pub const WATCH_DIRECTORY_CAPACITY: usize = 1_024;
/// Maximum structural-bootstrap prefix in bytes.
pub const BOOTSTRAP_MAX_BYTES: usize = 64 * 1024;
/// Maximum complete records inspected for structural bootstrap.
pub const BOOTSTRAP_MAX_RECORDS: usize = 64;
/// Maximum malformed samples retained for one file generation.
pub const MALFORMED_SAMPLES_PER_GENERATION: usize = 4;
/// Maximum time to wait for the provider I/O thread's exit acknowledgement.
pub const PROVIDER_EXIT_TIMEOUT: Duration = Duration::from_secs(1);

/// Source position meaningful only within one discovered file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourcePosition {
    pub path_id: u32,
    pub generation: u64,
    pub offset: u64,
}

/// Availability of one optional provider source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSourceState {
    Available,
    Unavailable { detail: String },
    NotApplicable,
}

/// Allowlisted event emitted by a provider adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderEvent {
    SessionResolved {
        provider: Provider,
        agent_thread_id: String,
        owner_session_id: Option<String>,
        parent_thread_id: Option<String>,
        path: PathBuf,
        model_id: Option<String>,
        depth: Option<u32>,
        event_id: String,
        observed_at_ms: i64,
        position: SourcePosition,
    },
    AgentUpsert {
        provider: Provider,
        agent_thread_id: String,
        owner_session_id: Option<String>,
        parent_thread_id: Option<String>,
        state: Option<ExecState>,
        model_id: Option<String>,
        depth: Option<u32>,
        event_id: String,
        observed_at_ms: i64,
        position: SourcePosition,
    },
    Activity {
        provider: Provider,
        agent_thread_id: String,
        activity: MinimalProviderMetadata,
        depth: Option<u32>,
        event_id: String,
        observed_at_ms: i64,
        position: SourcePosition,
    },
    SourceState {
        provider: Provider,
        state: ProviderSourceState,
    },
    Malformed {
        provider: Provider,
        path_display: String,
        generation: u64,
        byte_offset: u64,
        error_code: &'static str,
    },
}

impl ProviderEvent {
    fn entity_key(&self) -> Option<EntityKey> {
        match self {
            Self::SessionResolved {
                provider,
                agent_thread_id,
                ..
            }
            | Self::AgentUpsert {
                provider,
                agent_thread_id,
                ..
            }
            | Self::Activity {
                provider,
                agent_thread_id,
                ..
            } => Some(EntityKey {
                provider: *provider,
                thread_id: agent_thread_id.clone(),
            }),
            Self::SourceState { .. } | Self::Malformed { .. } => None,
        }
    }

    fn slot_details(&self) -> Option<(&str, i64, SourcePosition)> {
        match self {
            Self::SessionResolved {
                event_id,
                observed_at_ms,
                position,
                ..
            }
            | Self::AgentUpsert {
                event_id,
                observed_at_ms,
                position,
                ..
            }
            | Self::Activity {
                event_id,
                observed_at_ms,
                position,
                ..
            } => Some((event_id, *observed_at_ms, *position)),
            Self::SourceState { .. } | Self::Malformed { .. } => None,
        }
    }
}

/// Returns whether a provider-native identifier fits the colon-free grammar.
#[must_use]
pub fn valid_native_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// One provider root supplied independently of process home-directory state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryRoot {
    pub provider: Provider,
    pub path: PathBuf,
}

/// Allowlisted identity returned by a bounded structural-bootstrap parser.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapIdentity {
    pub thread_id: String,
    pub owner_session_id: Option<String>,
    pub parent_thread_id: Option<String>,
    pub model_id: Option<String>,
    pub depth: Option<u32>,
    pub agent_path: Option<String>,
    pub byte_offset: u64,
}

/// Callback used to identify the first structural record in a file prefix.
pub trait BootstrapParser {
    /// Returns `Some` only for a structural record. Raw bytes must never be retained.
    fn parse_structural(
        &mut self,
        provider: Provider,
        relative_path: &Path,
        record: &[u8],
    ) -> Option<BootstrapIdentity>;
}

/// Discovery data retained for one allowlisted provider artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredFile {
    pub provider: Provider,
    pub root: PathBuf,
    pub relative_path: PathBuf,
    pub path_id: u32,
    pub bootstrap: Option<BootstrapIdentity>,
}

/// Thread-ID lookup entry produced by structural bootstrap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredIdentity {
    pub path: PathBuf,
    pub parent_thread_id: Option<String>,
}

/// Run-scoped absolute-path interner shared by every discovery root.
///
/// Entries are intentionally never pruned or reused during a run so coalescing positions remain
/// unambiguous after root overlap, deletion, and recreation.
#[derive(Debug, Default)]
pub struct PathInterner {
    path_ids: HashMap<PathBuf, u32>,
    next_path_id: u32,
}

impl PathInterner {
    fn intern(&mut self, path: &Path) -> io::Result<u32> {
        if let Some(path_id) = self.path_ids.get(path) {
            return Ok(*path_id);
        }
        let path_id = self.next_path_id;
        self.next_path_id = self
            .next_path_id
            .checked_add(1)
            .ok_or_else(|| io::Error::other("provider path-id space exhausted"))?;
        self.path_ids.insert(path.to_path_buf(), path_id);
        Ok(path_id)
    }
}

/// Bounded bootstrap and first-seen state for configured roots.
#[derive(Debug)]
pub struct DiscoveryIndex {
    roots: Vec<DiscoveryRoot>,
    files: HashMap<(Provider, PathBuf), DiscoveredFile>,
    identities: HashMap<(Provider, String), DiscoveredIdentity>,
    agent_paths: HashMap<(Provider, Option<String>, String), String>,
    baseline: FirstSeenBaseline,
}

/// Per-file results from a successful root discovery pass.
#[derive(Debug, Default)]
pub struct DiscoveryScanOutcome {
    file_io_error: bool,
    removed_path_ids: Vec<u32>,
}

impl DiscoveryScanOutcome {
    /// Reports whether any individual file could not be interned or bootstrapped.
    #[must_use]
    pub const fn had_file_io_error(&self) -> bool {
        self.file_io_error
    }

    /// Returns paths removed from discovery during this scan.
    #[must_use]
    pub fn removed_path_ids(&self) -> &[u32] {
        &self.removed_path_ids
    }
}

impl DiscoveryIndex {
    /// Captures the per-run first-seen baseline immediately for all supplied roots.
    pub fn new(roots: Vec<DiscoveryRoot>) -> io::Result<Self> {
        let mut baseline = FirstSeenBaseline::default();
        for root in &roots {
            let discovery = discover_artifacts(&root.path)?;
            if discovery.had_errors {
                return Err(io::Error::other("provider baseline discovery incomplete"));
            }
            for relative in discovery.paths {
                baseline.record(root.path.join(relative));
            }
        }
        Ok(Self {
            roots,
            files: HashMap::new(),
            identities: HashMap::new(),
            agent_paths: HashMap::new(),
            baseline,
        })
    }

    /// Rescans roots, bootstrapping only newly discovered allowlisted files.
    pub fn scan(
        &mut self,
        parser: &mut impl BootstrapParser,
        interner: &mut PathInterner,
    ) -> io::Result<DiscoveryScanOutcome> {
        let mut seen = HashSet::new();
        let mut dirty_roots = HashSet::new();
        let mut outcome = DiscoveryScanOutcome::default();
        for root in self.roots.clone() {
            let mut root_seen = HashSet::new();
            let discovery = discover_artifacts(&root.path)?;
            outcome.file_io_error |= discovery.had_errors;
            if discovery.had_errors {
                dirty_roots.insert(root.path.clone());
            }
            for relative in discovery.paths {
                let absolute = root.path.join(&relative);
                let file_key = (root.provider, absolute.clone());
                root_seen.insert(absolute.clone());
                seen.insert(file_key.clone());
                if self.files.contains_key(&file_key) {
                    continue;
                }
                let path_id = match interner.intern(&absolute) {
                    Ok(path_id) => path_id,
                    Err(_) => {
                        outcome.file_io_error = true;
                        continue;
                    }
                };
                let bootstrap = match bootstrap_file(&root, &relative, parser) {
                    Ok(bootstrap) => bootstrap,
                    Err(_) => {
                        outcome.file_io_error = true;
                        continue;
                    }
                };
                if let Some(identity) = bootstrap.as_ref()
                    && valid_native_id(&identity.thread_id)
                    && identity
                        .parent_thread_id
                        .as_deref()
                        .is_none_or(valid_native_id)
                {
                    self.identities.insert(
                        (root.provider, identity.thread_id.clone()),
                        DiscoveredIdentity {
                            path: absolute.clone(),
                            parent_thread_id: identity.parent_thread_id.clone(),
                        },
                    );
                }
                self.files.insert(
                    file_key,
                    DiscoveredFile {
                        provider: root.provider,
                        root: root.path.clone(),
                        relative_path: relative,
                        path_id,
                        bootstrap,
                    },
                );
            }
            if !discovery.had_errors {
                self.baseline.retain_existing(&root.path, &root_seen);
            }
        }
        self.files.retain(|key, file| {
            if seen.contains(key) || dirty_roots.contains(&file.root) {
                true
            } else {
                outcome.removed_path_ids.push(file.path_id);
                false
            }
        });
        outcome.removed_path_ids.sort_unstable();
        outcome.removed_path_ids.dedup();
        self.rebuild_identities();
        Ok(outcome)
    }

    fn rebuild_identities(&mut self) {
        self.identities.clear();
        self.agent_paths.clear();
        for file in self.files.values() {
            let Some(identity) = file.bootstrap.as_ref() else {
                continue;
            };
            if valid_native_id(&identity.thread_id)
                && identity
                    .parent_thread_id
                    .as_deref()
                    .is_none_or(valid_native_id)
            {
                self.identities.insert(
                    (file.provider, identity.thread_id.clone()),
                    DiscoveredIdentity {
                        path: file.root.join(&file.relative_path),
                        parent_thread_id: identity.parent_thread_id.clone(),
                    },
                );
                let agent_path = identity.agent_path.clone().or_else(|| {
                    (file.provider == Provider::Codex && identity.depth == Some(0))
                        .then(|| "/root".to_owned())
                });
                if let Some(agent_path) = agent_path {
                    self.agent_paths.insert(
                        (file.provider, identity.owner_session_id.clone(), agent_path),
                        identity.thread_id.clone(),
                    );
                }
            }
        }
    }

    /// Returns the discovered files in stable path-ID order.
    pub fn files(&self) -> Vec<&DiscoveredFile> {
        let mut files = self.files.values().collect::<Vec<_>>();
        files.sort_by_key(|file| file.path_id);
        files
    }

    /// Resolves a provider-native thread ID to its file and optional parent.
    #[must_use]
    pub fn resolve(&self, provider: Provider, thread_id: &str) -> Option<&DiscoveredIdentity> {
        self.identities.get(&(provider, thread_id.to_owned()))
    }

    /// Resolves one provider-local agent path within its owner session.
    #[must_use]
    pub fn resolve_agent_path(
        &self,
        provider: Provider,
        owner_session_id: Option<&str>,
        agent_path: &str,
    ) -> Option<&str> {
        self.agent_paths
            .get(&(
                provider,
                owner_session_id.map(str::to_owned),
                agent_path.to_owned(),
            ))
            .map(String::as_str)
    }

    /// Returns the immutable run-start file baseline used by late targets.
    #[must_use]
    pub const fn baseline(&self) -> &FirstSeenBaseline {
        &self.baseline
    }
}

fn bootstrap_file(
    root: &DiscoveryRoot,
    relative: &Path,
    parser: &mut impl BootstrapParser,
) -> io::Result<Option<BootstrapIdentity>> {
    let mut file = open_contained_regular_file(&root.path, relative)?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(BOOTSTRAP_MAX_BYTES as u64)
        .read_to_end(&mut bytes)?;
    let mut start = 0;
    let mut records = 0;
    while records < BOOTSTRAP_MAX_RECORDS {
        let Some(end) = bytes[start..].iter().position(|byte| *byte == b'\n') else {
            break;
        };
        let end = start + end;
        let mut record = &bytes[start..end];
        if record.last() == Some(&b'\r') {
            record = &record[..record.len() - 1];
        }
        records += 1;
        if let Some(mut identity) = parser.parse_structural(root.provider, relative, record) {
            identity.byte_offset = start as u64;
            return Ok(Some(identity));
        }
        start = end + 1;
    }
    Ok(None)
}

#[derive(Debug, Default)]
struct ArtifactDiscovery {
    paths: Vec<PathBuf>,
    had_errors: bool,
}

#[cfg(test)]
type DiscoveryFileTypeHook = Box<dyn FnOnce(&Path) -> io::Result<()>>;

#[cfg(test)]
thread_local! {
    static DISCOVERY_FILE_TYPE_HOOK: std::cell::RefCell<Option<DiscoveryFileTypeHook>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_discovery_file_type_hook(hook: impl FnOnce(&Path) -> io::Result<()> + 'static) {
    DISCOVERY_FILE_TYPE_HOOK.with(|slot| {
        assert!(
            slot.borrow_mut().replace(Box::new(hook)).is_none(),
            "discovery file-type hook was already installed"
        );
    });
}

fn discovery_file_type(entry: &fs::DirEntry, path: &Path) -> io::Result<fs::FileType> {
    #[cfg(test)]
    if let Some(hook) = DISCOVERY_FILE_TYPE_HOOK.with(|slot| slot.borrow_mut().take()) {
        hook(path)?;
    }
    #[cfg(not(test))]
    let _ = path;
    entry.file_type()
}

fn discover_artifacts(root: &Path) -> io::Result<ArtifactDiscovery> {
    let mut found = Vec::new();
    let mut had_errors = false;
    let mut directories = vec![PathBuf::new()];
    while let Some(relative_directory) = directories.pop() {
        let directory = root.join(&relative_directory);
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if relative_directory.as_os_str().is_empty() => {
                if error.kind() == io::ErrorKind::NotFound {
                    return Ok(ArtifactDiscovery::default());
                }
                return Err(error);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => {
                had_errors = true;
                continue;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => {
                    had_errors = true;
                    continue;
                }
            };
            let relative = relative_directory.join(entry.file_name());
            let kind = match discovery_file_type(&entry, &root.join(&relative)) {
                Ok(kind) => kind,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => {
                    had_errors = true;
                    continue;
                }
            };
            if kind.is_dir() {
                directories.push(relative);
            } else if kind.is_file() && is_provider_artifact(&relative) {
                found.push(relative);
            }
        }
    }
    found.sort();
    Ok(ArtifactDiscovery {
        paths: found,
        had_errors,
    })
}

/// Returns whether a discovered entry is an adapter input artifact.
#[must_use]
pub fn is_provider_artifact(path: &Path) -> bool {
    path.extension() == Some(OsStr::new("jsonl"))
        && !path
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.ends_with(".meta.json"))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct EntityKey {
    provider: Provider,
    thread_id: String,
}

#[derive(Clone, Debug)]
struct PendingSlot {
    event: ProviderEvent,
}

#[derive(Clone, Debug, Default)]
struct PendingEntity {
    depth: Option<u32>,
    parent: Option<String>,
    identity: Option<PendingSlot>,
    upsert: Option<PendingSlot>,
    activity: Option<PendingSlot>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MalformedKey {
    provider: Provider,
    path_display: String,
    generation: u64,
}

#[derive(Clone, Debug, Default)]
struct PendingMalformed {
    samples: VecDeque<ProviderEvent>,
}

/// Result of merging an event into the pending buffer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergeOutcome {
    Accepted,
    Coalesced,
    Duplicate,
    AtCapacity(Box<ProviderEvent>),
}

#[derive(Clone, Debug)]
enum PendingToken {
    Source(Provider),
    Identity(EntityKey),
    Upsert(EntityKey),
    Activity(EntityKey),
    Malformed(MalformedKey),
}

/// Fixed-capacity, per-entity merge buffer in front of provider egress.
#[derive(Debug)]
pub struct PendingEvents {
    capacity: usize,
    sources: HashMap<Provider, ProviderEvent>,
    entities: HashMap<EntityKey, PendingEntity>,
    malformed: HashMap<MalformedKey, PendingMalformed>,
    diagnostics: ProviderDiagnostics,
}

impl PendingEvents {
    /// Creates the production-sized pending buffer.
    #[must_use]
    pub fn new(diagnostics: ProviderDiagnostics) -> Self {
        Self {
            capacity: PENDING_ENTITY_CAPACITY,
            sources: HashMap::new(),
            entities: HashMap::new(),
            malformed: HashMap::new(),
            diagnostics,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_capacity(capacity: usize, diagnostics: ProviderDiagnostics) -> Self {
        Self {
            capacity,
            sources: HashMap::new(),
            entities: HashMap::new(),
            malformed: HashMap::new(),
            diagnostics,
        }
    }

    /// Merges one event. A returned `AtCapacity` event must be retried before tail advancement.
    pub fn merge(&mut self, event: ProviderEvent) -> MergeOutcome {
        match event {
            event @ ProviderEvent::SourceState { provider, .. } => {
                if self.sources.insert(provider, event).is_some() {
                    self.diagnostics.record_coalesced();
                    MergeOutcome::Coalesced
                } else {
                    MergeOutcome::Accepted
                }
            }
            event @ ProviderEvent::Malformed { .. } => {
                let (provider, path_display, generation) = match &event {
                    ProviderEvent::Malformed {
                        provider,
                        path_display,
                        generation,
                        ..
                    } => (*provider, path_display.clone(), *generation),
                    _ => unreachable!(),
                };
                let key = MalformedKey {
                    provider,
                    path_display,
                    generation,
                };
                self.diagnostics.record_malformed();
                let pending = self.malformed.entry(key).or_default();
                if pending.samples.len() < MALFORMED_SAMPLES_PER_GENERATION {
                    pending.samples.push_back(event);
                    MergeOutcome::Accepted
                } else {
                    self.diagnostics.record_coalesced();
                    MergeOutcome::Coalesced
                }
            }
            event => self.merge_entity(event),
        }
    }

    fn merge_entity(&mut self, event: ProviderEvent) -> MergeOutcome {
        let key = event.entity_key().expect("entity event has a key");
        if !self.entities.contains_key(&key) && self.entities.len() >= self.capacity {
            return MergeOutcome::AtCapacity(Box::new(event));
        }
        let entity = self.entities.entry(key).or_default();
        let existing_slot = match &event {
            ProviderEvent::SessionResolved { .. } => entity.identity.as_ref(),
            ProviderEvent::AgentUpsert { .. } => entity.upsert.as_ref(),
            ProviderEvent::Activity { .. } => entity.activity.as_ref(),
            ProviderEvent::SourceState { .. } | ProviderEvent::Malformed { .. } => unreachable!(),
        };
        if existing_slot.is_some_and(|stored| {
            stored.event.slot_details().map(|details| details.0)
                == event.slot_details().map(|details| details.0)
        }) {
            self.diagnostics.record_duplicate();
            return MergeOutcome::Duplicate;
        }
        let incoming_parent = match &event {
            ProviderEvent::SessionResolved {
                parent_thread_id,
                depth,
                ..
            } => {
                if let Some(depth) = depth {
                    entity.depth = Some(*depth);
                }
                parent_thread_id.clone()
            }
            ProviderEvent::AgentUpsert {
                parent_thread_id,
                depth,
                ..
            } => {
                if let Some(depth) = depth {
                    entity.depth = Some(*depth);
                }
                parent_thread_id.clone()
            }
            ProviderEvent::Activity {
                activity, depth, ..
            } => {
                if let Some(depth) = depth {
                    entity.depth = Some(*depth);
                }
                activity.parent_agent_id.clone()
            }
            ProviderEvent::SourceState { .. } | ProviderEvent::Malformed { .. } => None,
        };
        if incoming_parent.is_some() {
            entity.parent = incoming_parent;
        }

        let slot = match event {
            ProviderEvent::SessionResolved { .. } => &mut entity.identity,
            ProviderEvent::AgentUpsert { .. } => &mut entity.upsert,
            ProviderEvent::Activity { .. } => &mut entity.activity,
            ProviderEvent::SourceState { .. } | ProviderEvent::Malformed { .. } => unreachable!(),
        };
        merge_slot(slot, event, &self.diagnostics)
    }

    /// Returns whether another previously unseen entity can be admitted.
    #[must_use]
    pub fn has_entity_capacity(&self) -> bool {
        self.entities.len() < self.capacity
    }

    /// Returns the number of entities with at least one unsent event.
    #[must_use]
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    /// Flushes in deterministic source/depth/slot/malformed order without blocking.
    pub fn flush_to(&mut self, sender: &tokio_mpsc::Sender<ProviderEvent>) {
        while let Some((token, event)) = self.next_event() {
            match sender.try_send(event) {
                Ok(()) => self.remove(token),
                Err(tokio_mpsc::error::TrySendError::Full(_)) => {
                    self.diagnostics.record_egress_saturation();
                    break;
                }
                Err(tokio_mpsc::error::TrySendError::Closed(_)) => {
                    self.diagnostics.record_egress_closed();
                    break;
                }
            }
        }
    }

    fn next_event(&self) -> Option<(PendingToken, ProviderEvent)> {
        for provider in [Provider::Claude, Provider::Codex] {
            if let Some(event) = self.sources.get(&provider) {
                return Some((PendingToken::Source(provider), event.clone()));
            }
        }

        let mut keys = self.entities.keys().collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            let left_depth = self.entities[left].depth;
            let right_depth = self.entities[right].depth;
            left_depth
                .is_none()
                .cmp(&right_depth.is_none())
                .then_with(|| left_depth.cmp(&right_depth))
                .then_with(|| left.thread_id.cmp(&right.thread_id))
                .then_with(|| provider_rank(left.provider).cmp(&provider_rank(right.provider)))
        });
        for key in keys {
            let entity = &self.entities[key];
            if let Some(slot) = entity.identity.as_ref() {
                return Some((PendingToken::Identity(key.clone()), slot.event.clone()));
            }
            if let Some(slot) = entity.upsert.as_ref() {
                return Some((PendingToken::Upsert(key.clone()), slot.event.clone()));
            }
            if let Some(slot) = entity.activity.as_ref() {
                return Some((PendingToken::Activity(key.clone()), slot.event.clone()));
            }
        }

        let mut malformed = self.malformed.keys().collect::<Vec<_>>();
        malformed.sort_by(|left, right| {
            provider_rank(left.provider)
                .cmp(&provider_rank(right.provider))
                .then_with(|| left.path_display.cmp(&right.path_display))
                .then_with(|| left.generation.cmp(&right.generation))
        });
        malformed.first().and_then(|key| {
            self.malformed[*key]
                .samples
                .front()
                .map(|event| (PendingToken::Malformed((*key).clone()), event.clone()))
        })
    }

    fn remove(&mut self, token: PendingToken) {
        match token {
            PendingToken::Source(provider) => {
                self.sources.remove(&provider);
            }
            PendingToken::Identity(key) => {
                if let Some(entity) = self.entities.get_mut(&key) {
                    entity.identity = None;
                }
                self.remove_empty_entity(&key);
            }
            PendingToken::Upsert(key) => {
                if let Some(entity) = self.entities.get_mut(&key) {
                    entity.upsert = None;
                }
                self.remove_empty_entity(&key);
            }
            PendingToken::Activity(key) => {
                if let Some(entity) = self.entities.get_mut(&key) {
                    entity.activity = None;
                }
                self.remove_empty_entity(&key);
            }
            PendingToken::Malformed(key) => {
                if let Some(pending) = self.malformed.get_mut(&key) {
                    pending.samples.pop_front();
                    if pending.samples.is_empty() {
                        self.malformed.remove(&key);
                    }
                }
            }
        }
    }

    fn remove_empty_entity(&mut self, key: &EntityKey) {
        if self.entities.get(key).is_some_and(|entity| {
            entity.identity.is_none() && entity.upsert.is_none() && entity.activity.is_none()
        }) {
            self.entities.remove(key);
        }
    }
}

fn merge_slot(
    slot: &mut Option<PendingSlot>,
    incoming: ProviderEvent,
    diagnostics: &ProviderDiagnostics,
) -> MergeOutcome {
    let Some(stored) = slot.as_ref() else {
        *slot = Some(PendingSlot { event: incoming });
        return MergeOutcome::Accepted;
    };
    let (stored_id, stored_time, stored_position) = stored
        .event
        .slot_details()
        .expect("pending entity slot has position");
    let (incoming_id, incoming_time, incoming_position) = incoming
        .slot_details()
        .expect("incoming entity event has position");
    if stored_id == incoming_id {
        diagnostics.record_duplicate();
        return MergeOutcome::Duplicate;
    }

    let replace = if stored_position.path_id == incoming_position.path_id {
        (incoming_position.generation, incoming_position.offset)
            >= (stored_position.generation, stored_position.offset)
    } else {
        incoming_time > stored_time
    };
    diagnostics.record_coalesced();
    if replace {
        *slot = Some(PendingSlot { event: incoming });
    }
    MergeOutcome::Coalesced
}

const fn provider_rank(provider: Provider) -> u8 {
    match provider {
        Provider::Claude => 0,
        Provider::Codex => 1,
    }
}

/// Shareable provider diagnostic counters.
#[derive(Clone, Debug, Default)]
pub struct ProviderDiagnostics(ProviderDiagnosticsHandle);

impl ProviderDiagnostics {
    pub(crate) const fn from_model_handle(handle: ProviderDiagnosticsHandle) -> Self {
        Self(handle)
    }

    fn record_dropped_hint(&self) {
        self.0.record_dropped_hint();
    }
    fn record_coalesced(&self) {
        self.0.record_coalesced_update();
    }
    fn record_duplicate(&self) {
        self.0.record_duplicate_event();
    }
    pub(crate) fn record_invalid_target(&self) {
        self.0.record_invalid_target();
    }
    pub(crate) fn record_duplicate_path_target(&self) {
        self.0.record_duplicate_path_target();
    }
    fn record_egress_saturation(&self) {
        self.0.record_egress_saturation();
    }
    fn record_egress_closed(&self) {
        self.0.record_egress_closed();
    }
    fn record_malformed(&self) {
        self.0.record_malformed_record();
    }
    fn record_watch_cap_fallback(&self) {
        self.0.record_watch_cap_fallback();
    }
    pub(crate) fn record_baseline_approximation(&self) {
        self.0.record_baseline_approximation();
    }
    fn record_notify_creation_failure(&self) {
        self.0.record_notify_creation_failure();
    }
    fn record_cycle(&self) {
        self.0.record_provider_cycle();
    }
    fn record_io_error(&self) {
        self.0.record_provider_io_error();
    }

    #[must_use]
    pub fn dropped_hints(&self) -> u64 {
        self.0.dropped_hints()
    }
    #[must_use]
    pub fn coalesced_updates(&self) -> u64 {
        self.0.coalesced_updates()
    }
    #[must_use]
    pub fn duplicate_events(&self) -> u64 {
        self.0.duplicate_events()
    }
    #[must_use]
    pub fn invalid_targets(&self) -> u64 {
        self.0.invalid_targets()
    }
    #[must_use]
    pub fn duplicate_path_targets(&self) -> u64 {
        self.0.duplicate_path_targets()
    }
    #[must_use]
    pub fn egress_saturations(&self) -> u64 {
        self.0.egress_saturations()
    }
    #[must_use]
    pub fn egress_closed(&self) -> u64 {
        self.0.egress_closed()
    }
    #[must_use]
    pub fn malformed_records(&self) -> u64 {
        self.0.malformed_records()
    }
    #[must_use]
    pub fn watch_cap_fallbacks(&self) -> u64 {
        self.0.watch_cap_fallbacks()
    }
    #[must_use]
    pub fn baseline_approximations(&self) -> u64 {
        self.0.baseline_approximations()
    }
    #[must_use]
    pub fn notify_creation_failures(&self) -> u64 {
        self.0.notify_creation_failures()
    }
    #[must_use]
    pub fn provider_cycles(&self) -> u64 {
        self.0.provider_cycles()
    }
    #[must_use]
    pub fn provider_io_errors(&self) -> u64 {
        self.0.provider_io_errors()
    }
}

/// Notify implementation boundary; tests can supply a watcher without OS events.
pub trait NotifyWatcher: Send {
    fn watch(&mut self, path: &Path) -> notify::Result<()>;
    fn unwatch(&mut self, path: &Path) -> notify::Result<()>;
}

impl NotifyWatcher for notify::RecommendedWatcher {
    fn watch(&mut self, path: &Path) -> notify::Result<()> {
        notify::Watcher::watch(self, path, notify::RecursiveMode::NonRecursive)
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        notify::Watcher::unwatch(self, path)
    }
}

/// Callback sink shared with a notify backend.
#[derive(Clone)]
pub struct NotifySink {
    control: SyncSender<Control>,
    force_rescan: Arc<AtomicBool>,
    diagnostics: ProviderDiagnostics,
}

impl NotifySink {
    /// Sends a path hint without waiting; full queues force the next rescan.
    pub fn hint(&self, path: PathBuf) {
        match self.control.try_send(Control::Hint(path)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.force_rescan.store(true, Ordering::Release);
                self.diagnostics.record_dropped_hint();
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    /// Marks notification coverage unreliable until the provider thread rescans.
    pub fn force_rescan(&self) {
        self.force_rescan.store(true, Ordering::Release);
    }
}

/// Factory boundary for the real or a fake notification backend.
pub trait NotifyFactory: Send {
    fn create(self: Box<Self>, sink: NotifySink) -> notify::Result<Box<dyn NotifyWatcher>>;
}

/// `notify` crate factory used in production.
#[derive(Debug, Default)]
pub struct RecommendedNotifyFactory;

impl NotifyFactory for RecommendedNotifyFactory {
    fn create(self: Box<Self>, sink: NotifySink) -> notify::Result<Box<dyn NotifyWatcher>> {
        let watcher =
            notify::recommended_watcher(
                move |result: notify::Result<notify::Event>| match result {
                    Ok(event) => {
                        for path in event.paths {
                            sink.hint(path);
                        }
                    }
                    Err(_) => sink.force_rescan(),
                },
            )?;
        Ok(Box::new(watcher))
    }
}

/// Result of requesting a per-directory watch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchDisposition {
    Watched,
    AlreadyWatched,
    RescanOnly,
}

#[derive(Default)]
struct NoopWatcher;

impl NotifyWatcher for NoopWatcher {
    fn watch(&mut self, _path: &Path) -> notify::Result<()> {
        Ok(())
    }
    fn unwatch(&mut self, _path: &Path) -> notify::Result<()> {
        Ok(())
    }
}

/// Active watch set enforcing the fixed per-directory budget.
pub struct WatchRegistry {
    watcher: Box<dyn NotifyWatcher>,
    watched: HashSet<PathBuf>,
    rescan_only: HashSet<PathBuf>,
    capacity: usize,
    diagnostics: ProviderDiagnostics,
}

impl WatchRegistry {
    /// Creates a registry with the production watch cap.
    pub fn new(watcher: Box<dyn NotifyWatcher>, diagnostics: ProviderDiagnostics) -> Self {
        Self::with_capacity(watcher, WATCH_DIRECTORY_CAPACITY, diagnostics)
    }

    fn with_capacity(
        watcher: Box<dyn NotifyWatcher>,
        capacity: usize,
        diagnostics: ProviderDiagnostics,
    ) -> Self {
        Self {
            watcher,
            watched: HashSet::new(),
            rescan_only: HashSet::new(),
            capacity,
            diagnostics,
        }
    }

    /// Watches a directory or records deterministic rescan-only fallback at the cap.
    pub fn add(&mut self, directory: PathBuf) -> notify::Result<WatchDisposition> {
        if self.watched.contains(&directory) {
            return Ok(WatchDisposition::AlreadyWatched);
        }
        if self.watched.len() >= self.capacity {
            if self.rescan_only.insert(directory) {
                self.diagnostics.record_watch_cap_fallback();
            }
            return Ok(WatchDisposition::RescanOnly);
        }
        self.watcher.watch(&directory)?;
        self.watched.insert(directory);
        Ok(WatchDisposition::Watched)
    }

    /// Returns active per-directory watch count.
    #[must_use]
    pub fn watched_count(&self) -> usize {
        self.watched.len()
    }
}

impl Drop for WatchRegistry {
    fn drop(&mut self) {
        for path in self.watched.drain() {
            let _ = self.watcher.unwatch(&path);
        }
    }
}

/// One provider-attributed session path requested by the collector.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderTarget {
    pub provider: Provider,
    pub path: PathBuf,
}

/// Latest provider-attributed collector target set.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TargetSet {
    targets: HashSet<ProviderTarget>,
}

impl TargetSet {
    /// Creates a target set from provider-session references.
    pub fn new(targets: impl IntoIterator<Item = ProviderTarget>) -> Self {
        Self {
            targets: targets.into_iter().collect(),
        }
    }

    /// Returns provider-attributed targets.
    pub fn iter(&self) -> impl Iterator<Item = &ProviderTarget> {
        self.targets.iter()
    }
}

/// Inputs made available to one provider I/O cycle.
pub struct ProviderCycle<'a> {
    pub targets: &'a TargetSet,
    pub hint: Option<&'a Path>,
    pub force_rescan: bool,
    pub pending: &'a mut PendingEvents,
    stop_flag: &'a AtomicBool,
    watch_requests: &'a mut Vec<PathBuf>,
}

impl ProviderCycle<'_> {
    /// Requests a non-recursive per-directory watch.
    pub fn request_watch(&mut self, directory: PathBuf) {
        self.watch_requests.push(directory);
    }

    /// Reports whether provider-thread shutdown has been requested.
    #[must_use]
    pub fn should_stop(&self) -> bool {
        self.stop_flag.load(Ordering::Acquire)
    }
}

#[cfg(test)]
static TEST_PROVIDER_STOP_FLAG: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
pub(crate) fn test_provider_cycle<'a>(
    targets: &'a TargetSet,
    pending: &'a mut PendingEvents,
    watch_requests: &'a mut Vec<PathBuf>,
) -> ProviderCycle<'a> {
    ProviderCycle {
        targets,
        hint: None,
        force_rescan: true,
        pending,
        stop_flag: &TEST_PROVIDER_STOP_FLAG,
        watch_requests,
    }
}

#[cfg(test)]
pub(crate) fn test_provider_cycle_with_stop<'a>(
    targets: &'a TargetSet,
    pending: &'a mut PendingEvents,
    stop_flag: &'a AtomicBool,
    watch_requests: &'a mut Vec<PathBuf>,
) -> ProviderCycle<'a> {
    ProviderCycle {
        targets,
        hint: None,
        force_rescan: true,
        pending,
        stop_flag,
        watch_requests,
    }
}

/// Adapter-owned discovery, tailing, and parsing work executed on the provider thread.
pub trait ProviderWorker: Send + 'static {
    fn process(&mut self, cycle: &mut ProviderCycle<'_>) -> io::Result<()>;
}

#[derive(Debug)]
enum Control {
    Hint(PathBuf),
    TargetsUpdated,
    Stop,
}

/// Provider-thread spawn failures.
#[derive(Debug, Error)]
pub enum ProviderSpawnError {
    #[error("provider notify setup failed: {0}")]
    Notify(#[from] notify::Error),
    #[error("provider thread spawn failed: {0}")]
    ThreadSpawn(#[source] io::Error),
}

/// Provider-thread shutdown failures.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ProviderThreadError {
    #[error("provider I/O thread panicked or exited without acknowledgement")]
    ThreadPanicked,
    #[error("provider I/O thread did not stop before timeout and was detached")]
    DetachedTimeout,
}

/// Unique lifecycle owner for the dedicated provider I/O thread.
pub struct ProviderThreadHandle {
    control: SyncSender<Control>,
    targets: Arc<Mutex<Option<TargetSet>>>,
    force_rescan: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
    watcher: Arc<Mutex<Option<WatchRegistry>>>,
    exit: oneshot::Receiver<()>,
    thread: Option<JoinHandle<()>>,
    diagnostics: ProviderDiagnostics,
}

/// Cloneable latest-value target publisher retained by the collector task.
#[derive(Clone)]
pub struct ProviderTargetPublisher {
    control: SyncSender<Control>,
    targets: Arc<Mutex<Option<TargetSet>>>,
}

impl ProviderTargetPublisher {
    /// Publishes the latest target set and sends a best-effort wake notification.
    pub fn update_targets(&self, targets: TargetSet) {
        *lock_unpoisoned(&self.targets) = Some(targets);
        let _ = self.control.try_send(Control::TargetsUpdated);
    }
}

impl ProviderThreadHandle {
    /// Publishes the latest target set and sends a best-effort wake notification.
    pub fn update_targets(&self, targets: TargetSet) {
        self.target_publisher().update_targets(targets);
    }

    /// Returns a cloneable target publisher that does not own thread shutdown.
    #[must_use]
    pub fn target_publisher(&self) -> ProviderTargetPublisher {
        ProviderTargetPublisher {
            control: self.control.clone(),
            targets: Arc::clone(&self.targets),
        }
    }

    /// Sends one non-blocking filesystem hint, forcing fallback rescan on saturation.
    pub fn hint(&self, path: PathBuf) {
        NotifySink {
            control: self.control.clone(),
            force_rescan: Arc::clone(&self.force_rescan),
            diagnostics: self.diagnostics.clone(),
        }
        .hint(path);
    }

    /// Returns the shared diagnostics handle.
    #[must_use]
    pub fn diagnostics(&self) -> ProviderDiagnostics {
        self.diagnostics.clone()
    }

    /// Stops notification, waits for the thread acknowledgement, and joins when safe.
    pub async fn stop(mut self) -> Result<(), ProviderThreadError> {
        self.stop_flag.store(true, Ordering::Release);
        let _ = self.control.try_send(Control::Stop);
        lock_unpoisoned(&self.watcher).take();

        match tokio::time::timeout(PROVIDER_EXIT_TIMEOUT, &mut self.exit).await {
            Ok(Ok(())) => self
                .thread
                .take()
                .ok_or(ProviderThreadError::ThreadPanicked)?
                .join()
                .map_err(|_| ProviderThreadError::ThreadPanicked),
            Ok(Err(_)) => {
                if let Some(thread) = self.thread.take() {
                    let _ = thread.join();
                }
                Err(ProviderThreadError::ThreadPanicked)
            }
            Err(_) => {
                drop(self.thread.take());
                Err(ProviderThreadError::DetachedTimeout)
            }
        }
    }
}

/// Starts the dedicated provider I/O thread and optional trait-backed notify watcher.
pub fn spawn_provider_thread(
    worker: impl ProviderWorker,
    egress: tokio_mpsc::Sender<ProviderEvent>,
    notify_factory: Option<Box<dyn NotifyFactory>>,
) -> Result<ProviderThreadHandle, ProviderSpawnError> {
    spawn_provider_thread_configured(
        worker,
        egress,
        notify_factory,
        ProviderDiagnostics::default(),
        RESCAN_INTERVAL,
    )
}

/// Starts provider I/O with an explicit fallback interval for watcher verification.
pub fn spawn_provider_thread_with_rescan_interval(
    worker: impl ProviderWorker,
    egress: tokio_mpsc::Sender<ProviderEvent>,
    notify_factory: Option<Box<dyn NotifyFactory>>,
    rescan_interval: Duration,
) -> Result<ProviderThreadHandle, ProviderSpawnError> {
    spawn_provider_thread_configured(
        worker,
        egress,
        notify_factory,
        ProviderDiagnostics::default(),
        rescan_interval,
    )
}

pub(crate) fn spawn_provider_thread_with_diagnostics(
    worker: impl ProviderWorker,
    egress: tokio_mpsc::Sender<ProviderEvent>,
    notify_factory: Option<Box<dyn NotifyFactory>>,
    diagnostics: ProviderDiagnostics,
) -> Result<ProviderThreadHandle, ProviderSpawnError> {
    spawn_provider_thread_configured(worker, egress, notify_factory, diagnostics, RESCAN_INTERVAL)
}

fn spawn_provider_thread_configured(
    worker: impl ProviderWorker,
    egress: tokio_mpsc::Sender<ProviderEvent>,
    notify_factory: Option<Box<dyn NotifyFactory>>,
    diagnostics: ProviderDiagnostics,
    rescan_interval: Duration,
) -> Result<ProviderThreadHandle, ProviderSpawnError> {
    let (control, receiver) = mpsc::sync_channel(CONTROL_CHANNEL_CAPACITY);
    let targets = Arc::new(Mutex::new(None));
    let force_rescan = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let sink = NotifySink {
        control: control.clone(),
        force_rescan: Arc::clone(&force_rescan),
        diagnostics: diagnostics.clone(),
    };
    let watcher_backend = match notify_factory {
        Some(factory) => match factory.create(sink) {
            Ok(watcher) => watcher,
            Err(error) => {
                tracing::warn!(
                    error_code = notify_error_code(&error),
                    "provider notify setup failed; falling back to polling"
                );
                diagnostics.record_notify_creation_failure();
                Box::new(NoopWatcher)
            }
        },
        None => Box::new(NoopWatcher),
    };
    let watcher = Arc::new(Mutex::new(Some(WatchRegistry::new(
        watcher_backend,
        diagnostics.clone(),
    ))));
    let (exit_sender, exit) = oneshot::channel();

    let thread_targets = Arc::clone(&targets);
    let thread_force_rescan = Arc::clone(&force_rescan);
    let thread_stop_flag = Arc::clone(&stop_flag);
    let thread_watcher = Arc::clone(&watcher);
    let thread_diagnostics = diagnostics.clone();
    let thread = thread::Builder::new()
        .name("herdr-top-provider-io".to_owned())
        .spawn(move || {
            provider_thread_main(
                worker,
                egress,
                receiver,
                thread_targets,
                thread_force_rescan,
                thread_stop_flag,
                thread_watcher,
                thread_diagnostics,
                rescan_interval,
            );
            let _ = exit_sender.send(());
        })
        .map_err(ProviderSpawnError::ThreadSpawn)?;

    Ok(ProviderThreadHandle {
        control,
        targets,
        force_rescan,
        stop_flag,
        watcher,
        exit,
        thread: Some(thread),
        diagnostics,
    })
}

const fn notify_error_code(error: &notify::Error) -> &'static str {
    match &error.kind {
        notify::ErrorKind::Generic(_) => "notify_generic",
        notify::ErrorKind::Io(_) => "notify_io",
        notify::ErrorKind::PathNotFound => "notify_path_not_found",
        notify::ErrorKind::WatchNotFound => "notify_watch_not_found",
        notify::ErrorKind::InvalidConfig(_) => "notify_invalid_config",
        notify::ErrorKind::MaxFilesWatch => "notify_max_files_watch",
    }
}

#[allow(clippy::too_many_arguments)]
fn provider_thread_main(
    mut worker: impl ProviderWorker,
    egress: tokio_mpsc::Sender<ProviderEvent>,
    receiver: Receiver<Control>,
    targets: Arc<Mutex<Option<TargetSet>>>,
    force_rescan: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
    watcher: Arc<Mutex<Option<WatchRegistry>>>,
    diagnostics: ProviderDiagnostics,
    rescan_interval: Duration,
) {
    let mut pending = PendingEvents::new(diagnostics.clone());
    run_provider_cycle(
        &mut worker,
        &egress,
        &targets,
        &force_rescan,
        &stop_flag,
        &watcher,
        &diagnostics,
        &mut pending,
        None,
        true,
    );

    loop {
        let control = receiver.recv_timeout(rescan_interval);
        if stop_flag.load(Ordering::Acquire) {
            break;
        }
        let (hint, timed_out) = match control {
            Ok(Control::Stop) => break,
            Ok(Control::Hint(path)) => (Some(path), false),
            Ok(Control::TargetsUpdated) => (None, false),
            Err(mpsc::RecvTimeoutError::Timeout) => (None, true),
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        run_provider_cycle(
            &mut worker,
            &egress,
            &targets,
            &force_rescan,
            &stop_flag,
            &watcher,
            &diagnostics,
            &mut pending,
            hint.as_deref(),
            timed_out,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn run_provider_cycle(
    worker: &mut impl ProviderWorker,
    egress: &tokio_mpsc::Sender<ProviderEvent>,
    targets: &Arc<Mutex<Option<TargetSet>>>,
    force_rescan: &AtomicBool,
    stop_flag: &AtomicBool,
    watcher: &Arc<Mutex<Option<WatchRegistry>>>,
    diagnostics: &ProviderDiagnostics,
    pending: &mut PendingEvents,
    hint: Option<&Path>,
    timed_out: bool,
) {
    pending.flush_to(egress);
    let current_targets = lock_unpoisoned(targets).clone().unwrap_or_default();
    let force = force_rescan.swap(false, Ordering::AcqRel) || timed_out;
    let mut watch_requests = Vec::new();
    let mut cycle = ProviderCycle {
        targets: &current_targets,
        hint,
        force_rescan: force,
        pending,
        stop_flag,
        watch_requests: &mut watch_requests,
    };
    if worker.process(&mut cycle).is_err() {
        diagnostics.record_io_error();
    }
    if let Some(registry) = lock_unpoisoned(watcher).as_mut() {
        for directory in watch_requests {
            if registry.add(directory).is_err() {
                diagnostics.record_io_error();
                force_rescan.store(true, Ordering::Release);
            }
        }
    }
    pending.flush_to(egress);
    diagnostics.record_cycle();
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn open_contained_regular_file(root: &Path, relative: &Path) -> io::Result<File> {
    if relative.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provider path must be relative to its configured root",
        ));
    }
    let components = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(Ok(value)),
            Component::CurDir => None,
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                Some(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "provider path escapes its root",
                )))
            }
        })
        .collect::<io::Result<Vec<_>>>()?;
    let (last, directories) = components
        .split_last()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "provider path is empty"))?;

    let root_name = path_cstring(root)?;
    let mut parent = open_path_owned(
        &root_name,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
    )?;
    for component in directories {
        let name = component_cstring(component)?;
        parent = openat_owned(
            parent.as_raw_fd(),
            &name,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
        )?;
    }
    let last = component_cstring(last)?;
    let file = openat_owned(
        parent.as_raw_fd(),
        &last,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
    )?;
    let stat = fstat_fd(file.as_raw_fd())?;
    if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provider target is not a regular file",
        ));
    }
    Ok(File::from(file))
}

fn path_cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "filesystem path contains an interior NUL byte",
        )
    })
}

fn component_cstring(component: &OsStr) -> io::Result<CString> {
    CString::new(component.as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "filesystem component contains an interior NUL byte",
        )
    })
}

fn open_path_owned(path: &CStr, flags: libc::c_int) -> io::Result<OwnedFd> {
    // SAFETY: `path` is NUL-terminated for the call and `open` retains no pointer.
    let result = unsafe { libc::open(path.as_ptr(), flags) };
    owned_fd_result(result)
}

fn openat_owned(parent: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<OwnedFd> {
    // SAFETY: `parent` is live, `name` is NUL-terminated, and `openat` retains neither.
    let result = unsafe { libc::openat(parent, name.as_ptr(), flags) };
    owned_fd_result(result)
}

fn owned_fd_result(result: libc::c_int) -> io::Result<OwnedFd> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: a successful open returns a new descriptor transferred exactly once.
        Ok(unsafe { OwnedFd::from_raw_fd(result) })
    }
}

fn fstat_fd(fd: RawFd) -> io::Result<libc::stat> {
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `fd` is live and `stat` points to enough writable storage.
    let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: successful `fstat` initialized the value.
        Ok(unsafe { stat.assume_init() })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    use super::*;

    // I2a test list, written before implementation:
    // - bootstrap skips non-structural lines and obeys byte/record caps
    // - discovery filters non-jsonl artifacts and maps thread IDs to paths/parents
    // - coalescing keeps only the newest same-path activity under saturation
    // - activity and session identity occupy distinct slots
    // - same-ID rotation replay is first-wins
    // - cross-path freshness uses observed time, never source position
    // - flush orders depth, unknown depth, identity, activity, and malformed samples
    // - saturated egress never blocks provider-thread progress
    // - saturated-egress shutdown completes within its bounded timeout
    // - Stop wakes the single control-channel park immediately
    // - the watch cap falls back to rescans
    // - descriptor-relative containment rejects symlinks and outside-root paths

    struct LineParser {
        calls: usize,
    }

    impl BootstrapParser for LineParser {
        fn parse_structural(
            &mut self,
            _provider: Provider,
            _relative_path: &Path,
            record: &[u8],
        ) -> Option<BootstrapIdentity> {
            self.calls += 1;
            let text = std::str::from_utf8(record).ok()?;
            let thread_id = text.strip_prefix("struct:")?;
            Some(BootstrapIdentity {
                thread_id: thread_id.to_owned(),
                owner_session_id: None,
                parent_thread_id: Some("parent-1".to_owned()),
                model_id: None,
                depth: None,
                agent_path: None,
                byte_offset: 0,
            })
        }
    }

    #[test]
    fn bootstrap_skips_non_structural_first_line_and_obeys_caps() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("session.jsonl"),
            b"last-prompt\nstruct:thread-1\nignored\n",
        )
        .unwrap();
        let mut index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Claude,
            path: directory.path().to_path_buf(),
        }])
        .unwrap();
        let mut parser = LineParser { calls: 0 };
        let mut interner = PathInterner::default();

        index.scan(&mut parser, &mut interner).unwrap();

        assert_eq!(parser.calls, 2);
        assert_eq!(
            index.resolve(Provider::Claude, "thread-1").unwrap(),
            &DiscoveredIdentity {
                path: directory.path().join("session.jsonl"),
                parent_thread_id: Some("parent-1".to_owned())
            }
        );

        let capped = tempfile::tempdir().unwrap();
        let mut records = vec![b"noise\n".to_vec(); BOOTSTRAP_MAX_RECORDS];
        records.push(b"struct:too-late\n".to_vec());
        fs::write(capped.path().join("record-cap.jsonl"), records.concat()).unwrap();
        fs::write(
            capped.path().join("byte-cap.jsonl"),
            [
                vec![b'x'; BOOTSTRAP_MAX_BYTES],
                b"\nstruct:past-byte-cap\n".to_vec(),
            ]
            .concat(),
        )
        .unwrap();
        let mut capped_index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Codex,
            path: capped.path().to_path_buf(),
        }])
        .unwrap();
        let mut capped_parser = LineParser { calls: 0 };
        let mut capped_interner = PathInterner::default();

        capped_index
            .scan(&mut capped_parser, &mut capped_interner)
            .unwrap();

        assert!(capped_index.resolve(Provider::Codex, "too-late").is_none());
        assert!(
            capped_index
                .resolve(Provider::Codex, "past-byte-cap")
                .is_none()
        );
    }

    #[test]
    fn discovery_filters_artifacts_and_preserves_path_ids() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir(directory.path().join("subagents")).unwrap();
        fs::write(directory.path().join("main.jsonl"), b"struct:main\n").unwrap();
        fs::write(directory.path().join("ignored.txt"), b"struct:no\n").unwrap();
        fs::write(
            directory.path().join("subagents/agent.meta.json"),
            b"struct:no\n",
        )
        .unwrap();
        let mut index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Claude,
            path: directory.path().to_path_buf(),
        }])
        .unwrap();
        let mut parser = LineParser { calls: 0 };
        let mut interner = PathInterner::default();

        index.scan(&mut parser, &mut interner).unwrap();
        let original_id = index.files()[0].path_id;
        index.scan(&mut parser, &mut interner).unwrap();

        assert_eq!(index.files().len(), 1);
        assert_eq!(index.files()[0].path_id, original_id);
        assert_eq!(parser.calls, 1);
    }

    #[test]
    fn same_provider_files_in_different_roots_use_distinct_path_ids_for_freshness() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        fs::write(first.path().join("session.jsonl"), b"struct:first\n").unwrap();
        fs::write(second.path().join("session.jsonl"), b"struct:second\n").unwrap();
        let mut first_index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Codex,
            path: first.path().to_path_buf(),
        }])
        .unwrap();
        let mut second_index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Codex,
            path: second.path().to_path_buf(),
        }])
        .unwrap();
        let mut parser = LineParser { calls: 0 };
        let mut interner = PathInterner::default();
        first_index.scan(&mut parser, &mut interner).unwrap();
        second_index.scan(&mut parser, &mut interner).unwrap();
        let first_path_id = first_index.files()[0].path_id;
        let second_path_id = second_index.files()[0].path_id;

        assert_ne!(first_path_id, second_path_id);

        let mut pending = PendingEvents::new(ProviderDiagnostics::default());
        pending.merge(activity(
            Provider::Codex,
            "shared-agent",
            "fresh-event",
            20,
            position(first_path_id, 0, 100),
            "fresh",
        ));
        pending.merge(activity(
            Provider::Codex,
            "shared-agent",
            "older-event",
            10,
            position(second_path_id, 0, 200),
            "older",
        ));
        let (sender, mut receiver) = tokio_mpsc::channel(1);
        pending.flush_to(&sender);
        assert_eq!(event_kind(receiver.try_recv().unwrap()), "fresh");
    }

    #[test]
    fn missing_discovered_file_is_pruned_from_first_seen_baseline() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("recreated.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"struct:original\n").unwrap();
        let mut index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Claude,
            path: directory.path().to_path_buf(),
        }])
        .unwrap();
        let mut parser = LineParser { calls: 0 };
        let mut interner = PathInterner::default();
        index.scan(&mut parser, &mut interner).unwrap();

        fs::remove_file(&path).unwrap();
        index.scan(&mut parser, &mut interner).unwrap();

        assert!(!index.baseline().contained(directory.path(), relative));
    }

    fn position(path_id: u32, generation: u64, offset: u64) -> SourcePosition {
        SourcePosition {
            path_id,
            generation,
            offset,
        }
    }

    fn activity(
        provider: Provider,
        thread_id: &str,
        event_id: &str,
        observed: i64,
        position: SourcePosition,
        kind: &str,
    ) -> ProviderEvent {
        ProviderEvent::Activity {
            provider,
            agent_thread_id: thread_id.to_owned(),
            activity: MinimalProviderMetadata {
                event_kind: Some(kind.to_owned()),
                ..MinimalProviderMetadata::default()
            },
            depth: None,
            event_id: event_id.to_owned(),
            observed_at_ms: observed,
            position,
        }
    }

    fn identity(
        provider: Provider,
        thread_id: &str,
        depth: Option<u32>,
        observed: i64,
    ) -> ProviderEvent {
        ProviderEvent::SessionResolved {
            provider,
            agent_thread_id: thread_id.to_owned(),
            owner_session_id: None,
            parent_thread_id: None,
            path: PathBuf::from(format!("{thread_id}.jsonl")),
            model_id: None,
            depth,
            event_id: format!("identity-{thread_id}"),
            observed_at_ms: observed,
            position: position(observed as u32 + 10, 0, 0),
        }
    }

    fn event_kind(event: ProviderEvent) -> String {
        match event {
            ProviderEvent::Activity { activity, .. } => activity.event_kind.unwrap(),
            ProviderEvent::SessionResolved {
                agent_thread_id, ..
            } => format!("identity:{agent_thread_id}"),
            ProviderEvent::SourceState { .. } => "source".to_owned(),
            ProviderEvent::Malformed { .. } => "malformed".to_owned(),
            ProviderEvent::AgentUpsert { .. } => "upsert".to_owned(),
        }
    }

    #[test]
    fn rapid_activity_under_full_egress_coalesces_to_newest() {
        let diagnostics = ProviderDiagnostics::default();
        let mut pending = PendingEvents::new(diagnostics.clone());
        for index in 0..10 {
            pending.merge(activity(
                Provider::Codex,
                "thread",
                &format!("event-{index}"),
                index,
                position(1, 0, index as u64),
                &format!("state-{index}"),
            ));
        }
        let (sender, mut receiver) = tokio_mpsc::channel(1);
        sender
            .try_send(ProviderEvent::SourceState {
                provider: Provider::Claude,
                state: ProviderSourceState::Available,
            })
            .unwrap();

        pending.flush_to(&sender);

        assert_eq!(diagnostics.coalesced_updates(), 9);
        assert_eq!(pending.entity_count(), 1);
        assert_eq!(diagnostics.egress_saturations(), 1);
        receiver.try_recv().unwrap();
        pending.flush_to(&sender);
        assert_eq!(event_kind(receiver.try_recv().unwrap()), "state-9");
    }

    #[test]
    fn activity_never_displaces_pending_identity() {
        let mut pending = PendingEvents::new(ProviderDiagnostics::default());
        pending.merge(identity(Provider::Claude, "agent", Some(0), 1));
        pending.merge(activity(
            Provider::Claude,
            "agent",
            "activity",
            2,
            position(2, 0, 2),
            "working",
        ));
        let (sender, mut receiver) = tokio_mpsc::channel(2);

        pending.flush_to(&sender);

        assert_eq!(event_kind(receiver.try_recv().unwrap()), "identity:agent");
        assert_eq!(event_kind(receiver.try_recv().unwrap()), "working");
    }

    #[test]
    fn same_id_rotation_replay_is_first_wins() {
        let diagnostics = ProviderDiagnostics::default();
        let mut pending = PendingEvents::new(diagnostics.clone());
        pending.merge(activity(
            Provider::Codex,
            "agent",
            "call-1",
            10,
            position(1, 0, 50),
            "newer-payload",
        ));

        assert_eq!(
            pending.merge(activity(
                Provider::Codex,
                "agent",
                "call-1",
                20,
                position(1, 1, 0),
                "replayed-different-payload",
            )),
            MergeOutcome::Duplicate
        );

        let (sender, mut receiver) = tokio_mpsc::channel(1);
        pending.flush_to(&sender);
        assert_eq!(event_kind(receiver.try_recv().unwrap()), "newer-payload");
        assert_eq!(diagnostics.duplicate_events(), 1);
    }

    #[test]
    fn newer_generation_at_lower_offset_replaces_distinct_pending_event() {
        let mut pending = PendingEvents::new(ProviderDiagnostics::default());
        pending.merge(activity(
            Provider::Codex,
            "agent",
            "event-x",
            10,
            position(1, 100, 100),
            "old-generation",
        ));

        assert_eq!(
            pending.merge(activity(
                Provider::Codex,
                "agent",
                "event-y",
                10,
                position(1, 101, 0),
                "replacement-generation",
            )),
            MergeOutcome::Coalesced
        );

        let (sender, mut receiver) = tokio_mpsc::channel(1);
        pending.flush_to(&sender);
        assert_eq!(
            event_kind(receiver.try_recv().unwrap()),
            "replacement-generation"
        );
    }

    #[test]
    fn cross_path_replacement_uses_semantic_freshness() {
        let mut pending = PendingEvents::new(ProviderDiagnostics::default());
        pending.merge(activity(
            Provider::Codex,
            "agent",
            "fresh",
            20,
            position(1, 0, 1),
            "fresh",
        ));
        pending.merge(activity(
            Provider::Codex,
            "agent",
            "old",
            10,
            position(2, 99, 99),
            "old",
        ));
        pending.merge(activity(
            Provider::Codex,
            "agent",
            "newest",
            30,
            position(2, 0, 0),
            "newest",
        ));
        let (sender, mut receiver) = tokio_mpsc::channel(1);

        pending.flush_to(&sender);

        assert_eq!(event_kind(receiver.try_recv().unwrap()), "newest");
    }

    #[test]
    fn flush_orders_sources_depth_slots_unknown_and_malformed() {
        let mut pending = PendingEvents::new(ProviderDiagnostics::default());
        pending.merge(activity(
            Provider::Codex,
            "unknown",
            "unknown-act",
            1,
            position(1, 0, 1),
            "unknown-activity",
        ));
        pending.merge(activity(
            Provider::Claude,
            "child",
            "child-act",
            2,
            position(2, 0, 1),
            "child-activity",
        ));
        pending.merge(identity(Provider::Claude, "child", Some(1), 2));
        pending.merge(activity(
            Provider::Codex,
            "parent",
            "parent-act",
            3,
            position(3, 0, 1),
            "parent-activity",
        ));
        pending.merge(identity(Provider::Codex, "parent", Some(0), 3));
        pending.merge(ProviderEvent::Malformed {
            provider: Provider::Codex,
            path_display: "~/bad.jsonl".to_owned(),
            generation: 0,
            byte_offset: 4,
            error_code: "json",
        });
        pending.merge(ProviderEvent::SourceState {
            provider: Provider::Codex,
            state: ProviderSourceState::Available,
        });
        let (sender, mut receiver) = tokio_mpsc::channel(8);

        pending.flush_to(&sender);

        let mut actual = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            actual.push(event_kind(event));
        }
        assert_eq!(
            actual,
            [
                "source",
                "identity:parent",
                "parent-activity",
                "identity:child",
                "child-activity",
                "unknown-activity",
                "malformed"
            ]
        );
    }

    #[test]
    fn agent_upsert_depth_orders_entity_before_unknown_activity() {
        let mut pending = PendingEvents::new(ProviderDiagnostics::default());
        pending.merge(activity(
            Provider::Codex,
            "000-unknown",
            "unknown-event",
            1,
            position(10, 0, 0),
            "unknown",
        ));
        pending.merge(ProviderEvent::AgentUpsert {
            provider: Provider::Codex,
            agent_thread_id: "depth-two-agent".to_owned(),
            owner_session_id: None,
            parent_thread_id: None,
            state: Some(ExecState::Working),
            model_id: None,
            depth: Some(2),
            event_id: "depth-two-upsert".to_owned(),
            observed_at_ms: 2,
            position: position(11, 0, 0),
        });
        let (sender, mut receiver) = tokio_mpsc::channel(2);

        pending.flush_to(&sender);

        assert!(matches!(
            receiver.try_recv().unwrap(),
            ProviderEvent::AgentUpsert {
                agent_thread_id,
                ..
            } if agent_thread_id == "depth-two-agent"
        ));
    }

    #[derive(Clone)]
    struct CountingWorker {
        calls: Arc<AtomicUsize>,
        emit: bool,
    }

    struct FailingNotifyFactory;

    impl NotifyFactory for FailingNotifyFactory {
        fn create(self: Box<Self>, _sink: NotifySink) -> notify::Result<Box<dyn NotifyWatcher>> {
            Err(notify::Error::generic("synthetic notify creation failure"))
        }
    }

    struct SentinelFailingNotifyFactory;

    impl NotifyFactory for SentinelFailingNotifyFactory {
        fn create(self: Box<Self>, _sink: NotifySink) -> notify::Result<Box<dyn NotifyWatcher>> {
            Err(notify::Error::generic("NOTIFY_MESSAGE_SENTINEL")
                .add_path(PathBuf::from("NOTIFY_PATH_SENTINEL")))
        }
    }

    struct AppendDetectingWorker {
        path: PathBuf,
        observed_len: u64,
    }

    impl ProviderWorker for AppendDetectingWorker {
        fn process(&mut self, cycle: &mut ProviderCycle<'_>) -> io::Result<()> {
            let length = fs::metadata(&self.path)?.len();
            if length > self.observed_len {
                self.observed_len = length;
                let _ = cycle.pending.merge(activity(
                    Provider::Codex,
                    "polling-worker",
                    "polling-append",
                    1,
                    position(99, 0, length),
                    "append-detected",
                ));
            }
            Ok(())
        }
    }

    impl ProviderWorker for CountingWorker {
        fn process(&mut self, cycle: &mut ProviderCycle<'_>) -> io::Result<()> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            if self.emit {
                cycle.pending.merge(activity(
                    Provider::Codex,
                    "thread",
                    &format!("event-{call}"),
                    call as i64,
                    position(1, 0, call as u64),
                    "working",
                ));
            }
            Ok(())
        }
    }

    fn run_bounded(name: &'static str, body: impl FnOnce() + Send + 'static) {
        let (sender, receiver) = mpsc::channel();
        let helper = thread::spawn(move || {
            let outcome = panic::catch_unwind(AssertUnwindSafe(body));
            let _ = sender.send(outcome);
        });
        let outcome = receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("{name} timed out: {error}"));
        outcome.unwrap_or_else(|panic| panic::resume_unwind(panic));
        helper.join().unwrap();
    }

    #[test]
    fn saturated_egress_never_blocks_provider_thread_progress() {
        run_bounded("saturated egress progress", || {
            let calls = Arc::new(AtomicUsize::new(0));
            let worker = CountingWorker {
                calls: Arc::clone(&calls),
                emit: true,
            };
            let (egress, _receiver) = tokio_mpsc::channel(1);
            egress
                .try_send(ProviderEvent::SourceState {
                    provider: Provider::Claude,
                    state: ProviderSourceState::Available,
                })
                .unwrap();
            let handle = spawn_provider_thread(worker, egress, None).unwrap();
            for index in 0..12 {
                handle.hint(PathBuf::from(format!("hint-{index}")));
            }
            let deadline = Instant::now() + Duration::from_secs(1);
            while calls.load(Ordering::Relaxed) < 4 && Instant::now() < deadline {
                thread::yield_now();
            }
            assert!(calls.load(Ordering::Relaxed) >= 4);
            assert!(handle.diagnostics().egress_saturations() > 0);
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.stop())
                .unwrap();
        });
    }

    #[test]
    fn notify_creation_failure_falls_back_to_rescan_detection() {
        run_bounded("notify creation fallback", || {
            use std::io::Write;

            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("append.log");
            fs::write(&path, b"").unwrap();
            let worker = AppendDetectingWorker {
                path: path.clone(),
                observed_len: 0,
            };
            let (egress, mut events) = tokio_mpsc::channel(4);
            let handle = spawn_provider_thread_with_rescan_interval(
                worker,
                egress,
                Some(Box::new(FailingNotifyFactory)),
                Duration::from_millis(10),
            )
            .expect("watcher creation failure should fall back to polling");
            let diagnostics = handle.diagnostics();

            writeln!(
                fs::OpenOptions::new().append(true).open(path).unwrap(),
                "append"
            )
            .unwrap();
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let event = runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(1), events.recv()).await
                })
                .expect("fallback rescan did not detect append")
                .expect("provider egress closed before append detection");

            assert_eq!(event_kind(event), "append-detected");
            assert_eq!(diagnostics.notify_creation_failures(), 1);
            runtime.block_on(handle.stop()).unwrap();
        });
    }

    #[test]
    fn notify_creation_warning_uses_frozen_code_without_error_payloads() {
        run_bounded("content-free notify creation warning", || {
            let directory = tempfile::tempdir().unwrap();
            let log_path = directory.path().join("notify.log");
            let log = fs::File::create(&log_path).unwrap();
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .without_time()
                .with_writer(log)
                .finish();
            let worker = CountingWorker {
                calls: Arc::new(AtomicUsize::new(0)),
                emit: false,
            };
            let (egress, _events) = tokio_mpsc::channel(1);

            let handle = tracing::subscriber::with_default(subscriber, || {
                spawn_provider_thread(worker, egress, Some(Box::new(SentinelFailingNotifyFactory)))
                    .unwrap()
            });
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.stop())
                .unwrap();
            let contents = fs::read_to_string(log_path).unwrap();

            assert!(
                contents.contains("notify_generic"),
                "notify warning omitted the frozen error code: {contents}"
            );
            assert!(!contents.contains("NOTIFY_MESSAGE_SENTINEL"));
            assert!(!contents.contains("NOTIFY_PATH_SENTINEL"));
        });
    }

    #[test]
    fn shutdown_under_saturated_egress_is_bounded() {
        run_bounded("saturated egress shutdown", || {
            let worker = CountingWorker {
                calls: Arc::new(AtomicUsize::new(0)),
                emit: true,
            };
            let (egress, _receiver) = tokio_mpsc::channel(1);
            egress
                .try_send(ProviderEvent::SourceState {
                    provider: Provider::Claude,
                    state: ProviderSourceState::Available,
                })
                .unwrap();
            let handle = spawn_provider_thread(worker, egress, None).unwrap();
            let started = Instant::now();

            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.stop())
                .unwrap();

            assert!(started.elapsed() < PROVIDER_EXIT_TIMEOUT);
        });
    }

    #[test]
    fn stop_wakes_the_single_park_immediately() {
        run_bounded("Stop wake", || {
            let worker = CountingWorker {
                calls: Arc::new(AtomicUsize::new(0)),
                emit: false,
            };
            let (egress, _receiver) = tokio_mpsc::channel(1);
            let handle = spawn_provider_thread(worker, egress, None).unwrap();
            thread::sleep(Duration::from_millis(20));
            let started = Instant::now();

            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.stop())
                .unwrap();

            assert!(started.elapsed() < Duration::from_millis(250));
        });
    }

    #[derive(Default)]
    struct RecordingWatcher {
        watched: Arc<AtomicUsize>,
    }

    impl NotifyWatcher for RecordingWatcher {
        fn watch(&mut self, _path: &Path) -> notify::Result<()> {
            self.watched.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn unwatch(&mut self, _path: &Path) -> notify::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn watch_cap_falls_back_to_rescan_without_extra_watch() {
        let diagnostics = ProviderDiagnostics::default();
        let watched = Arc::new(AtomicUsize::new(0));
        let mut registry = WatchRegistry::with_capacity(
            Box::new(RecordingWatcher {
                watched: Arc::clone(&watched),
            }),
            1,
            diagnostics.clone(),
        );

        assert_eq!(
            registry.add(PathBuf::from("one")).unwrap(),
            WatchDisposition::Watched
        );
        assert_eq!(
            registry.add(PathBuf::from("two")).unwrap(),
            WatchDisposition::RescanOnly
        );
        assert_eq!(watched.load(Ordering::Relaxed), 1);
        assert_eq!(registry.watched_count(), 1);
        assert_eq!(diagnostics.watch_cap_fallbacks(), 1);
    }

    #[test]
    fn containment_rejects_symlinked_component_and_outside_root() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("session.jsonl"), b"outside\n").unwrap();
        symlink(outside.path(), root.path().join("link")).unwrap();

        assert!(open_contained_regular_file(root.path(), Path::new("link/session.jsonl")).is_err());
        assert!(open_contained_regular_file(root.path(), outside.path()).is_err());
        assert!(
            open_contained_regular_file(root.path(), Path::new("../outside/session.jsonl"))
                .is_err()
        );
    }

    #[test]
    fn native_id_grammar_is_colon_free() {
        assert!(valid_native_id("call_123.agent-4"));
        assert!(!valid_native_id("provider:ambiguous"));
        assert!(!valid_native_id(""));
    }

    #[test]
    fn disconnected_control_receiver_does_not_spin() {
        let (sender, receiver) = mpsc::sync_channel::<Control>(1);
        drop(sender);
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }
}
