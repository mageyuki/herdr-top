#![allow(unsafe_code)]
//! Provider discovery, coalescing, notification, and dedicated I/O-thread substrate.

use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::ffi::{CStr, CString, OsStr};
use std::fs::{self, File};
use std::io::{self, Read};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use crate::model::{
    ControllerEvent, ExecState, HistoryDrainId, MinimalProviderMetadata, ObservationOrigin,
    Provider, ProviderDiagnosticsHandle, RunId, RunKey, TokenBreakdown,
};
use crate::store::PersistHistoryDrain;

pub mod claude;
pub mod claude_facts;
pub mod codex;
pub mod codex_facts;
pub mod facts;
pub mod lane;
pub mod tail;

pub use tail::{
    FileSnapshot, FirstSeenBaseline, FsReadBoundary, ReadBoundary, ReadChunk, TailFile, TailRecord,
};

/// Provider filesystem fallback interval.
pub const RESCAN_INTERVAL: Duration = Duration::from_secs(2);
const HINT_DEBOUNCE: Duration = Duration::from_millis(150);
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

const HISTORY_DRAIN_DOMAIN: &[u8] = b"herdr-top:history-drain:v1";

/// One immutable member of a provider's frozen startup/backfill manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenHistoryArtifact {
    pub provider: Provider,
    pub artifact_id: String,
    pub generation: String,
    pub goalpost: u64,
}

/// Computes a stable bounded identity from a canonical frozen artifact manifest.
pub fn stable_history_drain_id(
    provider: Provider,
    artifacts: impl IntoIterator<Item = FrozenHistoryArtifact>,
) -> Result<HistoryDrainId, &'static str> {
    let mut artifacts = artifacts.into_iter().collect::<Vec<_>>();
    if artifacts
        .iter()
        .any(|artifact| artifact.provider != provider)
    {
        return Err("history drain manifest mixes providers");
    }
    artifacts.sort_by(|left, right| {
        provider_rank(left.provider)
            .cmp(&provider_rank(right.provider))
            .then_with(|| left.artifact_id.cmp(&right.artifact_id))
            .then_with(|| left.generation.cmp(&right.generation))
            .then_with(|| left.goalpost.cmp(&right.goalpost))
    });
    let mut digest = Sha256::new();
    digest.update((HISTORY_DRAIN_DOMAIN.len() as u64).to_be_bytes());
    digest.update(HISTORY_DRAIN_DOMAIN);
    let manifest_provider = match provider {
        Provider::Claude => b"claude".as_slice(),
        Provider::Codex => b"codex".as_slice(),
    };
    digest.update((manifest_provider.len() as u64).to_be_bytes());
    digest.update(manifest_provider);
    digest.update((artifacts.len() as u64).to_be_bytes());
    for artifact in artifacts {
        let provider = match artifact.provider {
            Provider::Claude => b"claude".as_slice(),
            Provider::Codex => b"codex".as_slice(),
        };
        for field in [
            provider,
            artifact.artifact_id.as_bytes(),
            artifact.generation.as_bytes(),
        ] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field);
        }
        digest.update(8_u64.to_be_bytes());
        digest.update(artifact.goalpost.to_be_bytes());
    }
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(67);
    encoded.push_str("v1:");
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    HistoryDrainId::new(encoded)
}

const HISTORY_BARRIER_PENDING: u8 = 0;
const HISTORY_BARRIER_COMMITTED: u8 = 1;

/// Cloneable bounded acknowledgement shared by one provider barrier and the collector.
#[derive(Clone, Debug)]
pub struct HistoryDrainAcknowledgement(Arc<AtomicU8>);

impl HistoryDrainAcknowledgement {
    #[must_use]
    pub fn is_committed(&self) -> bool {
        self.0.load(Ordering::Acquire) == HISTORY_BARRIER_COMMITTED
    }

    pub fn acknowledge_committed(&self) {
        self.0.store(HISTORY_BARRIER_COMMITTED, Ordering::Release);
    }
}

impl PartialEq for HistoryDrainAcknowledgement {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for HistoryDrainAcknowledgement {}

/// Final ordered token for one frozen historical drain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryDrainBarrier {
    pub drain_id: HistoryDrainId,
    pub observed_at_ms: i64,
    acknowledgement: HistoryDrainAcknowledgement,
}

impl HistoryDrainBarrier {
    #[must_use]
    pub fn new(drain_id: HistoryDrainId, observed_at_ms: i64) -> Self {
        Self {
            drain_id,
            observed_at_ms,
            acknowledgement: HistoryDrainAcknowledgement(Arc::new(AtomicU8::new(
                HISTORY_BARRIER_PENDING,
            ))),
        }
    }

    #[must_use]
    pub fn acknowledgement(&self) -> HistoryDrainAcknowledgement {
        self.acknowledgement.clone()
    }
}

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
    AvailableWithBindings {
        bindings: Vec<CodexPaneBinding>,
    },
    Unavailable {
        detail: String,
    },
    NotApplicable,
    /// Internal ordered control carrying one completed historical drain to the collector.
    HistoryDrainBarrier(HistoryDrainBarrier),
}

/// One globally unambiguous sessionless Codex pane-to-rollout match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexPaneBinding {
    pub run_id: RunId,
    pub sid: String,
    pub observed_at_ms: i64,
}

/// Allowlisted event emitted by a provider adapter.
// The plan intentionally carries the existing public `ControllerEvent` by value.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderEvent {
    /// Controller-compatible lifecycle event synthesized from an admitted provider log.
    Synthesized(ControllerEvent),
    /// Append-time signal consumed by the lifecycle lane in Increment 9 Task 6.
    RunLiveness { key: RunKey, at_ms: i64 },
    /// Inactivity transition consumed through the lane-specific reducer close path.
    LaneClose { key: RunKey, at_ms: i64 },
    /// Allowlisted usage sample consumed by telemetry in Increment 9 Task 7.
    Telemetry {
        key: RunKey,
        at_ms: i64,
        output_tokens: u64,
        token_breakdown: TokenBreakdown,
        model: Option<String>,
        effort: Option<String>,
        sandbox: Option<String>,
    },
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
    pub(crate) fn provider(&self) -> Option<Provider> {
        match self {
            Self::Synthesized(event) => event.metadata.provider,
            Self::RunLiveness { key, .. }
            | Self::LaneClose { key, .. }
            | Self::Telemetry { key, .. } => match key {
                RunKey::Controller(key) if key.starts_with("hook:claude-code:") => {
                    Some(Provider::Claude)
                }
                RunKey::Controller(key) if key.starts_with("hook:codex:") => Some(Provider::Codex),
                RunKey::Native { provider, .. } | RunKey::NativePath { provider, .. } => {
                    Some(*provider)
                }
                RunKey::Controller(_) | RunKey::Provisional { .. } => None,
            },
            Self::SessionResolved { provider, .. }
            | Self::AgentUpsert { provider, .. }
            | Self::Activity { provider, .. }
            | Self::SourceState { provider, .. }
            | Self::Malformed { provider, .. } => Some(*provider),
        }
    }

    pub(crate) const fn requires_admission(&self) -> bool {
        match self {
            Self::Synthesized(_)
            | Self::RunLiveness { .. }
            | Self::LaneClose { .. }
            | Self::Telemetry { .. }
            | Self::SessionResolved { .. }
            | Self::AgentUpsert { .. }
            | Self::Activity { .. } => true,
            Self::SourceState { .. } | Self::Malformed { .. } => false,
        }
    }

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
            Self::Synthesized(_)
            | Self::RunLiveness { .. }
            | Self::LaneClose { .. }
            | Self::Telemetry { .. }
            | Self::SourceState { .. }
            | Self::Malformed { .. } => None,
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
            Self::Synthesized(_)
            | Self::RunLiveness { .. }
            | Self::LaneClose { .. }
            | Self::Telemetry { .. }
            | Self::SourceState { .. }
            | Self::Malformed { .. } => None,
        }
    }
}

enum ProviderEventSender {
    Raw(tokio::sync::mpsc::Sender<ProviderEvent>),
    Tracked {
        sender: tokio::sync::mpsc::Sender<ProviderIngressEvent>,
        ingress: crate::performance::PerformanceIngress,
    },
}

pub(crate) struct ProviderIngressEvent {
    pub event: ProviderEvent,
    pub admission: Option<crate::performance::Admission>,
    pub origin: ObservationOrigin,
    pub history_manifest: Option<Arc<PersistHistoryDrain>>,
}

impl ProviderEventSender {
    // Full and closed reservations must return the original provider event without boxing it.
    #[allow(clippy::result_large_err)]
    fn try_send(
        &self,
        event: ProviderEvent,
        origin: ObservationOrigin,
        history_manifest: Option<Arc<PersistHistoryDrain>>,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<ProviderEvent>> {
        match self {
            Self::Raw(sender) => sender.try_send(event),
            Self::Tracked { sender, ingress } => {
                let permit = match sender.try_reserve() {
                    Ok(permit) => permit,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(())) => {
                        return Err(tokio::sync::mpsc::error::TrySendError::Full(event));
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => {
                        return Err(tokio::sync::mpsc::error::TrySendError::Closed(event));
                    }
                };
                let admission = event.requires_admission().then(|| ingress.admit());
                permit.send(ProviderIngressEvent {
                    event,
                    admission,
                    origin,
                    history_manifest,
                });
                Ok(())
            }
        }
    }

    fn blocking_send(&self, event: ProviderEvent) -> bool {
        match self {
            Self::Raw(sender) => sender.blocking_send(event).is_ok(),
            Self::Tracked { sender, ingress } => {
                let admission = event.requires_admission().then(|| ingress.admit());
                sender
                    .blocking_send(ProviderIngressEvent {
                        event,
                        admission,
                        origin: ObservationOrigin::Live,
                        history_manifest: None,
                    })
                    .is_ok()
            }
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

/// Derives the two standard provider roots without touching the filesystem.
#[must_use]
pub fn standard_discovery_roots(home: Option<&OsStr>) -> Vec<DiscoveryRoot> {
    let Some(home) = home.filter(|value| !value.is_empty()) else {
        return Vec::new();
    };
    let home = Path::new(home);
    if !home.is_absolute() {
        return Vec::new();
    }
    vec![
        DiscoveryRoot {
            provider: Provider::Claude,
            path: home.join(".claude/projects"),
        },
        DiscoveryRoot {
            provider: Provider::Codex,
            path: home.join(".codex/sessions"),
        },
    ]
}

/// Derives standard provider roots from the process home-directory environment.
#[must_use]
pub fn standard_discovery_roots_from_env() -> Vec<DiscoveryRoot> {
    standard_discovery_roots(env::var_os("HOME").as_deref())
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
    /// File mtime captured during directory discovery, before any descriptor is opened.
    pub modified_ms: i64,
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

/// Completion state for a discovery scan that may be interrupted between artifacts.
#[derive(Debug)]
pub enum DiscoveryScanStatus {
    /// The complete inventory was scanned and cleanup was applied.
    Completed(DiscoveryScanOutcome),
    /// The supplied stop predicate requested interruption before scan cleanup.
    Interrupted,
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
            let discovery = discover_artifacts(&root.path, false)?;
            if discovery.had_errors {
                return Err(io::Error::other("provider baseline discovery incomplete"));
            }
            for artifact in discovery.artifacts {
                baseline.record(root.path.join(artifact.relative_path));
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

impl DiscoveryIndex {
    /// Provider-lane rescan that may stop cooperatively between artifacts.
    pub fn scan_admitted_interruptible(
        &mut self,
        parser: &mut impl BootstrapParser,
        interner: &mut PathInterner,
        admission: &lane::Admission,
        admission_index: &mut lane::AdmissionIndex,
        diagnostics: &ProviderDiagnostics,
        should_stop: impl FnMut() -> bool,
    ) -> io::Result<DiscoveryScanStatus> {
        let mut should_stop = should_stop;
        let mut seen = HashSet::new();
        let mut dirty_roots = HashSet::new();
        let mut clean_root_seen = Vec::new();
        let mut outcome = DiscoveryScanOutcome::default();
        for root in self.roots.clone() {
            let mut root_seen = HashSet::new();
            let discovery = discover_artifacts(&root.path, true)?;
            if should_stop() {
                return Ok(DiscoveryScanStatus::Interrupted);
            }
            outcome.file_io_error |= discovery.had_errors;
            if discovery.had_errors {
                dirty_roots.insert(root.path.clone());
            }

            let mut newest_pane_artifact: HashMap<(Provider, String), (i64, PathBuf)> =
                HashMap::new();
            for artifact in &discovery.artifacts {
                if should_stop() {
                    return Ok(DiscoveryScanStatus::Interrupted);
                }
                let absolute = root.path.join(&artifact.relative_path);
                admission_index.observe_discovered_path(
                    root.provider,
                    &absolute,
                    artifact.modified_ms,
                );
                if let Some(identity) = admission.pane_root_identity(root.provider, &absolute) {
                    let candidate = (artifact.modified_ms, absolute);
                    newest_pane_artifact
                        .entry((root.provider, identity))
                        .and_modify(|current| {
                            if candidate > *current {
                                *current = candidate.clone();
                            }
                        })
                        .or_insert(candidate);
                }
            }

            if should_stop() {
                return Ok(DiscoveryScanStatus::Interrupted);
            }
            for artifact in discovery.artifacts {
                if should_stop() {
                    return Ok(DiscoveryScanStatus::Interrupted);
                }
                let relative = artifact.relative_path;
                let absolute = root.path.join(&relative);
                if !admission.is_admitted_file(&absolute, artifact.modified_ms) {
                    continue;
                }
                if let Some(identity) = admission.pane_root_identity(root.provider, &absolute)
                    && newest_pane_artifact
                        .get(&(root.provider, identity))
                        .is_some_and(|(_, newest)| newest != &absolute)
                {
                    continue;
                }
                let file_key = (root.provider, absolute.clone());
                root_seen.insert(absolute.clone());
                seen.insert(file_key.clone());
                if let Some(existing) = self.files.get_mut(&file_key) {
                    existing.modified_ms = artifact.modified_ms;
                    continue;
                }
                let path_id = match interner.intern(&absolute) {
                    Ok(path_id) => path_id,
                    Err(_) => {
                        outcome.file_io_error = true;
                        continue;
                    }
                };
                let is_meta = relative
                    .file_name()
                    .and_then(OsStr::to_str)
                    .is_some_and(|name| name.ends_with(".meta.json"));
                let bootstrap = if is_meta {
                    None
                } else {
                    match bootstrap_file_admitted(&root, &relative, parser, admission, diagnostics)
                    {
                        Ok(bootstrap) => bootstrap,
                        Err(_) => {
                            outcome.file_io_error = true;
                            continue;
                        }
                    }
                };
                self.files.insert(
                    file_key,
                    DiscoveredFile {
                        provider: root.provider,
                        root: root.path.clone(),
                        relative_path: relative,
                        modified_ms: artifact.modified_ms,
                        path_id,
                        bootstrap,
                    },
                );
            }
            if should_stop() {
                return Ok(DiscoveryScanStatus::Interrupted);
            }
            if !discovery.had_errors {
                clean_root_seen.push((root.path, root_seen));
            }
        }
        if should_stop() {
            return Ok(DiscoveryScanStatus::Interrupted);
        }
        for (root, root_seen) in clean_root_seen {
            self.baseline.retain_existing(&root, &root_seen);
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
        Ok(DiscoveryScanStatus::Completed(outcome))
    }

    /// Production provider-lane rescan. Identity inventory is built from names and discovery
    /// metadata; admission is checked before structural bootstrap opens a descriptor.
    pub fn scan_admitted(
        &mut self,
        parser: &mut impl BootstrapParser,
        interner: &mut PathInterner,
        admission: &lane::Admission,
        admission_index: &mut lane::AdmissionIndex,
        diagnostics: &ProviderDiagnostics,
    ) -> io::Result<DiscoveryScanOutcome> {
        match self.scan_admitted_interruptible(
            parser,
            interner,
            admission,
            admission_index,
            diagnostics,
            || false,
        )? {
            DiscoveryScanStatus::Completed(outcome) => Ok(outcome),
            DiscoveryScanStatus::Interrupted => {
                unreachable!("non-interruptible provider discovery cannot be interrupted")
            }
        }
    }
}

fn bootstrap_file_admitted(
    root: &DiscoveryRoot,
    relative: &Path,
    parser: &mut impl BootstrapParser,
    admission: &lane::Admission,
    diagnostics: &ProviderDiagnostics,
) -> io::Result<Option<BootstrapIdentity>> {
    let Some(mut file) = open_admitted_regular_file(&root.path, relative, admission, diagnostics)?
    else {
        return Ok(None);
    };
    parse_bootstrap_prefix(root, relative, parser, &mut file)
}

fn parse_bootstrap_prefix(
    root: &DiscoveryRoot,
    relative: &Path,
    parser: &mut impl BootstrapParser,
    file: &mut File,
) -> io::Result<Option<BootstrapIdentity>> {
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
    artifacts: Vec<DiscoveredArtifactEntry>,
    had_errors: bool,
}

#[derive(Debug)]
struct DiscoveredArtifactEntry {
    relative_path: PathBuf,
    modified_ms: i64,
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

fn discover_artifacts(root: &Path, include_meta: bool) -> io::Result<ArtifactDiscovery> {
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
            if kind.is_dir() && entry.file_name() != OsStr::new("tool-results") {
                directories.push(relative);
            } else if kind.is_file()
                && (is_provider_artifact(&relative)
                    || include_meta
                        && relative
                            .file_name()
                            .and_then(OsStr::to_str)
                            .is_some_and(|name| name.ends_with(".meta.json")))
            {
                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(_) => {
                        had_errors = true;
                        continue;
                    }
                };
                let modified_ms = match metadata.modified() {
                    Ok(modified) => system_time_ms(modified),
                    Err(_) => {
                        had_errors = true;
                        continue;
                    }
                };
                found.push(DiscoveredArtifactEntry {
                    relative_path: relative,
                    modified_ms,
                });
            }
        }
    }
    found.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(ArtifactDiscovery {
        artifacts: found,
        had_errors,
    })
}

fn system_time_ms(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(error) => -i64::try_from(error.duration().as_millis()).unwrap_or(i64::MAX),
    }
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
    origin: ObservationOrigin,
    history_manifest: Option<Arc<PersistHistoryDrain>>,
}

#[derive(Clone, Debug, Default)]
struct PendingEntity {
    depth: Option<u32>,
    parent: Option<String>,
    identity: Option<PendingSlot>,
    upsert: Option<PendingSlot>,
    activity: Option<PendingSlot>,
    lane_activity: Option<PendingSlot>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MalformedKey {
    provider: Provider,
    path_display: String,
    generation: u64,
}

#[derive(Clone, Debug, Default)]
struct PendingMalformed {
    samples: VecDeque<PendingSlot>,
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
    Lane,
    Identity(EntityKey),
    Upsert(EntityKey),
    Activity(EntityKey),
    LaneActivity(EntityKey),
    Malformed(MalformedKey),
    HistoryDrainBarrier,
}

/// Fixed-capacity, per-entity merge buffer in front of provider egress.
#[derive(Clone, Debug)]
pub struct PendingEvents {
    capacity: usize,
    sources: HashMap<Provider, PendingSlot>,
    lane: VecDeque<PendingSlot>,
    entities: HashMap<EntityKey, PendingEntity>,
    malformed: HashMap<MalformedKey, PendingMalformed>,
    history_barrier: Option<PendingSlot>,
    diagnostics: ProviderDiagnostics,
}

impl PendingEvents {
    /// Creates the production-sized pending buffer.
    #[must_use]
    pub fn new(diagnostics: ProviderDiagnostics) -> Self {
        Self {
            capacity: PENDING_ENTITY_CAPACITY,
            sources: HashMap::new(),
            lane: VecDeque::new(),
            entities: HashMap::new(),
            malformed: HashMap::new(),
            history_barrier: None,
            diagnostics,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_capacity(capacity: usize, diagnostics: ProviderDiagnostics) -> Self {
        Self {
            capacity,
            sources: HashMap::new(),
            lane: VecDeque::new(),
            entities: HashMap::new(),
            malformed: HashMap::new(),
            history_barrier: None,
            diagnostics,
        }
    }

    /// Merges one event. A returned `AtCapacity` event must be retried before tail advancement.
    pub fn merge(&mut self, event: ProviderEvent) -> MergeOutcome {
        self.merge_with_origin(event, ObservationOrigin::Live, None)
    }

    /// Merges one event while retaining its reducer-visible historical provenance and manifest.
    pub(crate) fn merge_with_origin(
        &mut self,
        event: ProviderEvent,
        origin: ObservationOrigin,
        history_manifest: Option<Arc<PersistHistoryDrain>>,
    ) -> MergeOutcome {
        match event {
            event @ ProviderEvent::SourceState {
                state: ProviderSourceState::HistoryDrainBarrier(_),
                ..
            } => {
                if self
                    .history_barrier
                    .as_ref()
                    .is_some_and(|stored| stored.event == event)
                {
                    self.diagnostics.record_duplicate();
                    MergeOutcome::Duplicate
                } else if self.history_barrier.is_some() {
                    MergeOutcome::AtCapacity(Box::new(event))
                } else {
                    self.history_barrier = Some(PendingSlot {
                        event,
                        origin,
                        history_manifest,
                    });
                    MergeOutcome::Accepted
                }
            }
            event @ ProviderEvent::SourceState { provider, .. } => {
                let incoming = PendingSlot {
                    event,
                    origin,
                    history_manifest,
                };
                if let Some(stored) = self.sources.get_mut(&provider) {
                    *stored = incoming;
                    self.diagnostics.record_coalesced();
                    MergeOutcome::Coalesced
                } else {
                    self.sources.insert(provider, incoming);
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
                    pending.samples.push_back(PendingSlot {
                        event,
                        origin,
                        history_manifest,
                    });
                    MergeOutcome::Accepted
                } else {
                    self.diagnostics.record_coalesced();
                    MergeOutcome::Coalesced
                }
            }
            event @ (ProviderEvent::Synthesized(_)
            | ProviderEvent::RunLiveness { .. }
            | ProviderEvent::LaneClose { .. }
            | ProviderEvent::Telemetry { .. }) => {
                if self.lane.len() >= self.capacity {
                    MergeOutcome::AtCapacity(Box::new(event))
                } else {
                    self.lane.push_back(PendingSlot {
                        event,
                        origin,
                        history_manifest,
                    });
                    MergeOutcome::Accepted
                }
            }
            event => self.merge_entity(event, origin, history_manifest),
        }
    }

    fn merge_entity(
        &mut self,
        event: ProviderEvent,
        origin: ObservationOrigin,
        history_manifest: Option<Arc<PersistHistoryDrain>>,
    ) -> MergeOutcome {
        let lane_activity = matches!(
            &event,
            ProviderEvent::Activity { activity, .. }
                if activity.event_kind.as_deref() == Some(lane::LIVE_LINE_EVENT_KIND)
        );
        let key = event.entity_key().expect("entity event has a key");
        if !self.entities.contains_key(&key) && self.entities.len() >= self.capacity {
            return MergeOutcome::AtCapacity(Box::new(event));
        }
        let entity = self.entities.entry(key).or_default();
        let existing_slot = match &event {
            ProviderEvent::SessionResolved { .. } => entity.identity.as_ref(),
            ProviderEvent::AgentUpsert { .. } => entity.upsert.as_ref(),
            ProviderEvent::Activity { .. } if lane_activity => entity.lane_activity.as_ref(),
            ProviderEvent::Activity { .. } => entity.activity.as_ref(),
            ProviderEvent::Synthesized(_)
            | ProviderEvent::RunLiveness { .. }
            | ProviderEvent::LaneClose { .. }
            | ProviderEvent::Telemetry { .. }
            | ProviderEvent::SourceState { .. }
            | ProviderEvent::Malformed { .. } => unreachable!(),
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
            ProviderEvent::Synthesized(_)
            | ProviderEvent::RunLiveness { .. }
            | ProviderEvent::LaneClose { .. }
            | ProviderEvent::Telemetry { .. }
            | ProviderEvent::SourceState { .. }
            | ProviderEvent::Malformed { .. } => None,
        };
        if incoming_parent.is_some() {
            entity.parent = incoming_parent;
        }

        let slot = match event {
            ProviderEvent::SessionResolved { .. } => &mut entity.identity,
            ProviderEvent::AgentUpsert { .. } => &mut entity.upsert,
            ProviderEvent::Activity { .. } if lane_activity => &mut entity.lane_activity,
            ProviderEvent::Activity { .. } => &mut entity.activity,
            ProviderEvent::Synthesized(_)
            | ProviderEvent::RunLiveness { .. }
            | ProviderEvent::LaneClose { .. }
            | ProviderEvent::Telemetry { .. }
            | ProviderEvent::SourceState { .. }
            | ProviderEvent::Malformed { .. } => unreachable!(),
        };
        merge_slot(slot, event, origin, history_manifest, &self.diagnostics)
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

    #[cfg(test)]
    pub(crate) fn total_count_for_test(&self) -> usize {
        self.sources.len()
            + self.lane.len()
            + self
                .entities
                .values()
                .map(|entity| {
                    usize::from(entity.identity.is_some())
                        + usize::from(entity.upsert.is_some())
                        + usize::from(entity.activity.is_some())
                        + usize::from(entity.lane_activity.is_some())
                })
                .sum::<usize>()
            + self
                .malformed
                .values()
                .map(|pending| pending.samples.len())
                .sum::<usize>()
            + usize::from(self.history_barrier.is_some())
    }

    #[cfg(test)]
    pub(crate) fn originated_events_for_test(
        &self,
    ) -> Vec<(
        ProviderEvent,
        ObservationOrigin,
        Option<Arc<PersistHistoryDrain>>,
    )> {
        let mut snapshot = self.clone();
        let mut events = Vec::new();
        while let Some((token, pending)) = snapshot.next_event() {
            events.push((pending.event, pending.origin, pending.history_manifest));
            snapshot.remove(token);
        }
        events
    }

    /// Flushes in deterministic source/depth/slot/malformed order without blocking.
    pub fn flush_to(&mut self, sender: &tokio_mpsc::Sender<ProviderEvent>) {
        self.flush_to_sender(&ProviderEventSender::Raw(sender.clone()));
    }

    fn flush_to_sender(&mut self, sender: &ProviderEventSender) {
        while let Some((token, pending)) = self.next_event() {
            match sender.try_send(pending.event, pending.origin, pending.history_manifest) {
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

    fn next_event(&self) -> Option<(PendingToken, PendingSlot)> {
        for provider in [Provider::Claude, Provider::Codex] {
            if let Some(event) = self.sources.get(&provider) {
                return Some((PendingToken::Source(provider), event.clone()));
            }
        }

        if let Some(event) = self.lane.front() {
            return Some((PendingToken::Lane, event.clone()));
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
                return Some((PendingToken::Identity(key.clone()), slot.clone()));
            }
            if let Some(slot) = entity.upsert.as_ref() {
                return Some((PendingToken::Upsert(key.clone()), slot.clone()));
            }
            if let Some(slot) = entity.activity.as_ref() {
                return Some((PendingToken::Activity(key.clone()), slot.clone()));
            }
            if let Some(slot) = entity.lane_activity.as_ref() {
                return Some((PendingToken::LaneActivity(key.clone()), slot.clone()));
            }
        }

        let mut malformed = self.malformed.keys().collect::<Vec<_>>();
        malformed.sort_by(|left, right| {
            provider_rank(left.provider)
                .cmp(&provider_rank(right.provider))
                .then_with(|| left.path_display.cmp(&right.path_display))
                .then_with(|| left.generation.cmp(&right.generation))
        });
        malformed
            .first()
            .and_then(|key| {
                self.malformed[*key]
                    .samples
                    .front()
                    .map(|event| (PendingToken::Malformed((*key).clone()), event.clone()))
            })
            .or_else(|| {
                self.history_barrier
                    .as_ref()
                    .map(|event| (PendingToken::HistoryDrainBarrier, event.clone()))
            })
    }

    fn remove(&mut self, token: PendingToken) {
        match token {
            PendingToken::Source(provider) => {
                self.sources.remove(&provider);
            }
            PendingToken::Lane => {
                self.lane.pop_front();
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
            PendingToken::LaneActivity(key) => {
                if let Some(entity) = self.entities.get_mut(&key) {
                    entity.lane_activity = None;
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
            PendingToken::HistoryDrainBarrier => {
                self.history_barrier = None;
            }
        }
    }

    fn remove_empty_entity(&mut self, key: &EntityKey) {
        if self.entities.get(key).is_some_and(|entity| {
            entity.identity.is_none()
                && entity.upsert.is_none()
                && entity.activity.is_none()
                && entity.lane_activity.is_none()
        }) {
            self.entities.remove(key);
        }
    }
}

fn merge_slot(
    slot: &mut Option<PendingSlot>,
    incoming: ProviderEvent,
    incoming_origin: ObservationOrigin,
    incoming_manifest: Option<Arc<PersistHistoryDrain>>,
    diagnostics: &ProviderDiagnostics,
) -> MergeOutcome {
    let Some(stored) = slot.as_mut() else {
        *slot = Some(PendingSlot {
            event: incoming,
            origin: incoming_origin,
            history_manifest: incoming_manifest,
        });
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
        promote_live_origin(stored, incoming_origin, incoming_manifest);
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
        *slot = Some(PendingSlot {
            event: incoming,
            origin: incoming_origin,
            history_manifest: incoming_manifest,
        });
    } else {
        promote_live_origin(stored, incoming_origin, incoming_manifest);
    }
    MergeOutcome::Coalesced
}

fn promote_live_origin(
    stored: &mut PendingSlot,
    incoming_origin: ObservationOrigin,
    incoming_manifest: Option<Arc<PersistHistoryDrain>>,
) {
    if matches!(incoming_origin, ObservationOrigin::Live) {
        stored.origin = ObservationOrigin::Live;
        stored.history_manifest = None;
    } else if stored.history_manifest.is_none() {
        stored.history_manifest = incoming_manifest;
    }
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
    #[cfg(any(test, feature = "workload-harness"))]
    pub(crate) fn record_admission_open_attempt(&self) {
        self.0.record_admission_open_attempt();
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
    fn record_watcher_observation(&self, at_ms: i64) {
        self.0.record_watcher_observation(at_ms);
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
    pub fn last_watcher_observation_ms(&self) -> Option<i64> {
        self.0.last_watcher_observation_ms()
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
    #[cfg(any(test, feature = "workload-harness"))]
    #[must_use]
    pub fn admission_open_attempts(&self) -> u64 {
        self.0.admission_open_attempts()
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
        let watcher = notify::recommended_watcher(move |result| {
            forward_notify_event(&sink, result);
        })?;
        Ok(Box::new(watcher))
    }
}

fn forward_notify_event(sink: &NotifySink, result: notify::Result<notify::Event>) {
    match result {
        Ok(event) => {
            if matches!(
                event.kind,
                notify::EventKind::Access(kind)
                    if kind
                        != notify::event::AccessKind::Close(notify::event::AccessMode::Write)
            ) {
                return;
            }
            for path in event.paths {
                sink.hint(path);
            }
        }
        Err(_) => sink.force_rescan(),
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

/// One live sessionless Codex pane awaiting a one-shot native rollout binding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CodexPaneTarget {
    pub run_id: RunId,
    pub detected_at_ms: i64,
}

/// Latest provider-attributed collector target set.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TargetSet {
    targets: HashSet<ProviderTarget>,
    sessions: HashSet<(Provider, String)>,
    codex_panes: HashSet<CodexPaneTarget>,
    owned_codex_sessions: HashSet<String>,
}

impl TargetSet {
    /// Creates a target set from provider-session references.
    pub fn new(targets: impl IntoIterator<Item = ProviderTarget>) -> Self {
        Self {
            targets: targets.into_iter().collect(),
            sessions: HashSet::new(),
            codex_panes: HashSet::new(),
            owned_codex_sessions: HashSet::new(),
        }
    }

    /// Creates a target set from provider paths and pane-session identities.
    pub fn new_with_sessions(
        targets: impl IntoIterator<Item = ProviderTarget>,
        sessions: impl IntoIterator<Item = (Provider, String)>,
    ) -> Self {
        Self {
            targets: targets.into_iter().collect(),
            sessions: sessions.into_iter().collect(),
            codex_panes: HashSet::new(),
            owned_codex_sessions: HashSet::new(),
        }
    }

    /// Creates a target set including live sessionless Codex pane identities.
    pub(crate) fn new_with_sessions_and_codex_panes(
        targets: impl IntoIterator<Item = ProviderTarget>,
        sessions: impl IntoIterator<Item = (Provider, String)>,
        codex_panes: impl IntoIterator<Item = CodexPaneTarget>,
        owned_codex_sessions: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            targets: targets.into_iter().collect(),
            sessions: sessions.into_iter().collect(),
            codex_panes: codex_panes.into_iter().collect(),
            owned_codex_sessions: owned_codex_sessions.into_iter().collect(),
        }
    }

    /// Returns provider-attributed targets.
    pub fn iter(&self) -> impl Iterator<Item = &ProviderTarget> {
        self.targets.iter()
    }

    /// Returns provider-attributed pane-session identities.
    pub fn sessions(&self) -> impl Iterator<Item = (Provider, &str)> {
        self.sessions
            .iter()
            .map(|(provider, session_id)| (*provider, session_id.as_str()))
    }

    /// Returns live sessionless Codex panes awaiting native identity.
    pub(crate) fn codex_panes(&self) -> impl Iterator<Item = CodexPaneTarget> + '_ {
        self.codex_panes.iter().copied()
    }

    /// Returns Codex native sessions that already have authoritative run owners.
    pub(crate) fn owned_codex_sessions(&self) -> impl Iterator<Item = &str> {
        self.owned_codex_sessions.iter().map(String::as_str)
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

    /// Emits final provider events that must traverse normal egress on graceful shutdown.
    fn graceful_stop(&mut self) -> Vec<ProviderEvent> {
        Vec::new()
    }
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
        ProviderEventSender::Raw(egress),
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
        ProviderEventSender::Raw(egress),
        notify_factory,
        ProviderDiagnostics::default(),
        rescan_interval,
    )
}

#[cfg(test)]
pub(crate) fn spawn_provider_thread_with_diagnostics(
    worker: impl ProviderWorker,
    egress: tokio_mpsc::Sender<ProviderEvent>,
    notify_factory: Option<Box<dyn NotifyFactory>>,
    diagnostics: ProviderDiagnostics,
) -> Result<ProviderThreadHandle, ProviderSpawnError> {
    spawn_provider_thread_configured(
        worker,
        ProviderEventSender::Raw(egress),
        notify_factory,
        diagnostics,
        RESCAN_INTERVAL,
    )
}

pub(crate) fn spawn_provider_thread_with_diagnostics_and_performance(
    worker: impl ProviderWorker,
    egress: tokio_mpsc::Sender<ProviderIngressEvent>,
    notify_factory: Option<Box<dyn NotifyFactory>>,
    diagnostics: ProviderDiagnostics,
    performance: crate::performance::PerformanceIngress,
) -> Result<ProviderThreadHandle, ProviderSpawnError> {
    spawn_provider_thread_configured(
        worker,
        ProviderEventSender::Tracked {
            sender: egress,
            ingress: performance,
        },
        notify_factory,
        diagnostics,
        RESCAN_INTERVAL,
    )
}

#[cfg(feature = "workload-harness")]
pub(crate) fn spawn_provider_thread_with_diagnostics_performance_and_rescan_interval(
    worker: impl ProviderWorker,
    egress: tokio_mpsc::Sender<ProviderIngressEvent>,
    notify_factory: Option<Box<dyn NotifyFactory>>,
    diagnostics: ProviderDiagnostics,
    performance: crate::performance::PerformanceIngress,
    rescan_interval: Duration,
) -> Result<ProviderThreadHandle, ProviderSpawnError> {
    spawn_provider_thread_configured(
        worker,
        ProviderEventSender::Tracked {
            sender: egress,
            ingress: performance,
        },
        notify_factory,
        diagnostics,
        rescan_interval,
    )
}

fn spawn_provider_thread_configured(
    worker: impl ProviderWorker,
    egress: ProviderEventSender,
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
    egress: ProviderEventSender,
    receiver: Receiver<Control>,
    targets: Arc<Mutex<Option<TargetSet>>>,
    force_rescan: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
    watcher: Arc<Mutex<Option<WatchRegistry>>>,
    diagnostics: ProviderDiagnostics,
    rescan_interval: Duration,
) {
    let mut pending = PendingEvents::new(diagnostics.clone());
    force_rescan.swap(false, Ordering::AcqRel);
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
    let mut last_full_rescan = Instant::now();

    'provider: loop {
        if stop_flag.load(Ordering::Acquire) {
            flush_graceful_stop(&mut worker, &egress);
            break;
        }
        let until_periodic_rescan = rescan_interval.saturating_sub(last_full_rescan.elapsed());
        let control = receiver.recv_timeout(until_periodic_rescan);
        let first_control = match control {
            Ok(Control::Stop) => {
                flush_graceful_stop(&mut worker, &egress);
                break;
            }
            Ok(control) => control,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                force_rescan.swap(false, Ordering::AcqRel);
                if stop_flag.load(Ordering::Acquire) {
                    flush_graceful_stop(&mut worker, &egress);
                    break;
                }
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
                last_full_rescan = Instant::now();
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };

        let debounce_hints = matches!(first_control, Control::Hint(_));
        let mut hints = HashSet::new();
        if let Control::Hint(path) = first_control {
            hints.insert(path);
        }
        let effective_debounce = if debounce_hints {
            HINT_DEBOUNCE.min(rescan_interval)
        } else {
            Duration::ZERO
        };
        let debounce_deadline = Instant::now() + effective_debounce;
        let periodic_deadline = last_full_rescan + rescan_interval;
        let collection_deadline = debounce_deadline.min(periodic_deadline);
        let mut disconnected = false;
        loop {
            while Instant::now() < collection_deadline {
                if stop_flag.load(Ordering::Acquire) {
                    flush_graceful_stop(&mut worker, &egress);
                    break 'provider;
                }
                match receiver.try_recv() {
                    Ok(Control::Stop) => {
                        flush_graceful_stop(&mut worker, &egress);
                        break 'provider;
                    }
                    Ok(Control::Hint(path)) => {
                        hints.insert(path);
                    }
                    Ok(Control::TargetsUpdated) => {}
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }
            if disconnected {
                break;
            }
            let Some(remaining) = collection_deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            match receiver.recv_timeout(remaining) {
                Ok(Control::Stop) => {
                    flush_graceful_stop(&mut worker, &egress);
                    break 'provider;
                }
                Ok(Control::Hint(path)) => {
                    hints.insert(path);
                }
                Ok(Control::TargetsUpdated) => {}
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        let full_rescan = last_full_rescan.elapsed() >= rescan_interval;
        let hint = if !full_rescan && hints.len() == 1 {
            hints.iter().next().map(PathBuf::as_path)
        } else {
            None
        };
        if full_rescan {
            // Periodic full rescans guarantee recovery for force requests.
            force_rescan.swap(false, Ordering::AcqRel);
        }
        if stop_flag.load(Ordering::Acquire) {
            flush_graceful_stop(&mut worker, &egress);
            break;
        }
        run_provider_cycle(
            &mut worker,
            &egress,
            &targets,
            &force_rescan,
            &stop_flag,
            &watcher,
            &diagnostics,
            &mut pending,
            hint,
            full_rescan,
        );
        if full_rescan {
            last_full_rescan = Instant::now();
        }
        if disconnected {
            break;
        }
    }
}

fn flush_graceful_stop(worker: &mut impl ProviderWorker, egress: &ProviderEventSender) {
    for event in worker.graceful_stop() {
        if !egress.blocking_send(event) {
            break;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_provider_cycle(
    worker: &mut impl ProviderWorker,
    egress: &ProviderEventSender,
    targets: &Arc<Mutex<Option<TargetSet>>>,
    force_rescan: &AtomicBool,
    stop_flag: &AtomicBool,
    watcher: &Arc<Mutex<Option<WatchRegistry>>>,
    diagnostics: &ProviderDiagnostics,
    pending: &mut PendingEvents,
    hint: Option<&Path>,
    force_rescan_cycle: bool,
) {
    pending.flush_to_sender(egress);
    if pending.next_event().is_some() {
        diagnostics.record_watcher_observation(system_time_ms(SystemTime::now()));
        diagnostics.record_cycle();
        return;
    }
    let current_targets = lock_unpoisoned(targets).clone().unwrap_or_default();
    let mut watch_requests = Vec::new();
    let mut cycle = ProviderCycle {
        targets: &current_targets,
        hint,
        force_rescan: force_rescan_cycle,
        pending,
        stop_flag,
        watch_requests: &mut watch_requests,
    };
    if worker.process(&mut cycle).is_err() {
        diagnostics.record_io_error();
    }
    diagnostics.record_watcher_observation(system_time_ms(SystemTime::now()));
    if let Some(registry) = lock_unpoisoned(watcher).as_mut() {
        for directory in watch_requests {
            if registry.add(directory).is_err() {
                diagnostics.record_io_error();
                force_rescan.store(true, Ordering::Release);
            }
        }
    }
    pending.flush_to_sender(egress);
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

/// Admission seam that Task 5 routes the existing provider worker through before opening.
pub(crate) fn open_admitted_regular_file(
    root: &Path,
    relative: &Path,
    admission: &lane::Admission,
    diagnostics: &ProviderDiagnostics,
) -> io::Result<Option<File>> {
    if !admission.is_admitted_path(&root.join(relative)) {
        return Ok(None);
    }
    #[cfg(any(test, feature = "workload-harness"))]
    diagnostics.record_admission_open_attempt();
    #[cfg(not(any(test, feature = "workload-harness")))]
    let _ = diagnostics;
    open_contained_regular_file(root, relative).map(Some)
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
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    use super::*;
    use crate::activity::OperatorSnapshot;
    use crate::model::DomainModel;
    use crate::performance::{TestPerformanceClock, performance_tracker};

    const CLAUDE_FIXTURE_A: &str = "11111111-1111-4111-8111-111111111111";
    const CLAUDE_FIXTURE_B: &str = "22222222-2222-4222-8222-222222222222";
    const CODEX_FIXTURE: &str = "rollout-fixture-33333333-3333-4333-8333-333333333333.jsonl";

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

    fn scan_admitted_fixture(
        index: &mut DiscoveryIndex,
        parser: &mut impl BootstrapParser,
        interner: &mut PathInterner,
    ) -> io::Result<DiscoveryScanOutcome> {
        let mut admission = lane::Admission::new(0);
        for root in index.roots.clone() {
            for artifact in discover_artifacts(&root.path, false)?.artifacts {
                let _ = admission
                    .admit_pane_artifact(root.provider, &root.path.join(artifact.relative_path));
            }
        }
        index.scan_admitted(
            parser,
            interner,
            &admission,
            &mut lane::AdmissionIndex::new(),
            &ProviderDiagnostics::default(),
        )
    }

    #[test]
    fn bootstrap_skips_non_structural_first_line_and_obeys_caps() {
        let directory = tempfile::tempdir().unwrap();
        let session_file = format!("{CLAUDE_FIXTURE_A}.jsonl");
        fs::write(
            directory.path().join(&session_file),
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

        scan_admitted_fixture(&mut index, &mut parser, &mut interner).unwrap();

        assert_eq!(parser.calls, 2);
        assert_eq!(
            index.resolve(Provider::Claude, "thread-1").unwrap(),
            &DiscoveredIdentity {
                path: directory.path().join(session_file),
                parent_thread_id: Some("parent-1".to_owned())
            }
        );

        let capped = tempfile::tempdir().unwrap();
        let mut records = vec![b"noise\n".to_vec(); BOOTSTRAP_MAX_RECORDS];
        records.push(b"struct:too-late\n".to_vec());
        fs::write(capped.path().join(CODEX_FIXTURE), records.concat()).unwrap();
        fs::write(
            capped
                .path()
                .join("rollout-byte-cap-44444444-4444-4444-8444-444444444444.jsonl"),
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

        scan_admitted_fixture(&mut capped_index, &mut capped_parser, &mut capped_interner).unwrap();

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
        fs::write(
            directory.path().join(format!("{CLAUDE_FIXTURE_A}.jsonl")),
            b"struct:main\n",
        )
        .unwrap();
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

        scan_admitted_fixture(&mut index, &mut parser, &mut interner).unwrap();
        let original_id = index.files()[0].path_id;
        scan_admitted_fixture(&mut index, &mut parser, &mut interner).unwrap();

        assert_eq!(index.files().len(), 1);
        assert_eq!(index.files()[0].path_id, original_id);
        assert_eq!(parser.calls, 1);
    }

    #[test]
    fn scan_admitted_never_bootstraps_an_unadmitted_fixture() {
        let directory = tempfile::tempdir().unwrap();
        let admitted = directory.path().join(format!("{CLAUDE_FIXTURE_A}.jsonl"));
        let stranger = directory.path().join(format!("{CLAUDE_FIXTURE_B}.jsonl"));
        fs::write(&admitted, b"struct:admitted\n").unwrap();
        fs::write(&stranger, b"struct:stranger\n").unwrap();
        let mut index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Claude,
            path: directory.path().to_path_buf(),
        }])
        .unwrap();
        let mut admission = lane::Admission::new(0);
        assert!(admission.admit_pane_artifact(Provider::Claude, &admitted));
        let mut parser = LineParser { calls: 0 };

        index
            .scan_admitted(
                &mut parser,
                &mut PathInterner::default(),
                &admission,
                &mut lane::AdmissionIndex::new(),
                &ProviderDiagnostics::default(),
            )
            .unwrap();

        assert_eq!(parser.calls, 1);
        assert_eq!(index.files().len(), 1);
        assert_eq!(
            index.files()[0].relative_path,
            Path::new(&format!("{CLAUDE_FIXTURE_A}.jsonl"))
        );
        assert!(index.resolve(Provider::Claude, "stranger").is_none());
    }

    #[test]
    fn same_provider_files_in_different_roots_use_distinct_path_ids_for_freshness() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        fs::write(first.path().join(CODEX_FIXTURE), b"struct:first\n").unwrap();
        fs::write(second.path().join(CODEX_FIXTURE), b"struct:second\n").unwrap();
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
        scan_admitted_fixture(&mut first_index, &mut parser, &mut interner).unwrap();
        scan_admitted_fixture(&mut second_index, &mut parser, &mut interner).unwrap();
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
        scan_admitted_fixture(&mut index, &mut parser, &mut interner).unwrap();

        fs::remove_file(&path).unwrap();
        scan_admitted_fixture(&mut index, &mut parser, &mut interner).unwrap();

        assert!(!index.baseline().contained(directory.path(), relative));
    }

    #[test]
    fn scan_admitted_interrupts_between_artifacts_without_partial_cleanup() {
        const CLAUDE_FIXTURE_C: &str = "33333333-3333-4333-8333-333333333333";

        struct CountingLineParser {
            calls: Arc<AtomicUsize>,
        }

        impl BootstrapParser for CountingLineParser {
            fn parse_structural(
                &mut self,
                _provider: Provider,
                _relative_path: &Path,
                record: &[u8],
            ) -> Option<BootstrapIdentity> {
                self.calls.fetch_add(1, Ordering::Release);
                let text = std::str::from_utf8(record).ok()?;
                let thread_id = text.strip_prefix("struct:")?;
                Some(BootstrapIdentity {
                    thread_id: thread_id.to_owned(),
                    owner_session_id: None,
                    parent_thread_id: None,
                    model_id: None,
                    depth: None,
                    agent_path: None,
                    byte_offset: 0,
                })
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let removed_relative = PathBuf::from(format!("{CLAUDE_FIXTURE_A}.jsonl"));
        let added_relative = PathBuf::from(format!("{CLAUDE_FIXTURE_B}.jsonl"));
        let retained_relative = PathBuf::from(format!("{CLAUDE_FIXTURE_C}.jsonl"));
        fs::write(
            directory.path().join(&removed_relative),
            b"struct:removed-identity\n",
        )
        .unwrap();
        fs::write(
            directory.path().join(&retained_relative),
            b"struct:retained-identity\n",
        )
        .unwrap();
        let mut index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Claude,
            path: directory.path().to_path_buf(),
        }])
        .unwrap();
        let mut parser = LineParser { calls: 0 };
        let mut interner = PathInterner::default();
        scan_admitted_fixture(&mut index, &mut parser, &mut interner).unwrap();

        fs::remove_file(directory.path().join(&removed_relative)).unwrap();
        fs::write(
            directory.path().join(&added_relative),
            b"struct:added-identity\n",
        )
        .unwrap();
        let mut admission = lane::Admission::new(0);
        for relative in [&added_relative, &retained_relative] {
            assert!(
                admission.admit_pane_artifact(Provider::Claude, &directory.path().join(relative))
            );
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut parser = CountingLineParser {
            calls: Arc::clone(&calls),
        };

        let result = index
            .scan_admitted_interruptible(
                &mut parser,
                &mut interner,
                &admission,
                &mut lane::AdmissionIndex::new(),
                &ProviderDiagnostics::default(),
                || calls.load(Ordering::Acquire) > 0,
            )
            .unwrap();

        assert!(matches!(result, DiscoveryScanStatus::Interrupted));
        assert!(
            index
                .files()
                .iter()
                .any(|file| file.relative_path == removed_relative),
            "partial seen coverage removed an unvisited file"
        );
        assert!(
            index
                .baseline()
                .contained(directory.path(), &removed_relative),
            "partial root coverage pruned the run baseline"
        );
        assert!(
            index
                .resolve(Provider::Claude, "removed-identity")
                .is_some(),
            "interrupted scan rebuilt identities from partial coverage"
        );
        assert!(
            index.resolve(Provider::Claude, "added-identity").is_none(),
            "interrupted scan published a partially rebuilt identity"
        );
    }

    fn position(path_id: u32, generation: u64, offset: u64) -> SourcePosition {
        SourcePosition {
            path_id,
            generation,
            offset,
        }
    }

    fn source_state(provider: Provider) -> ProviderEvent {
        ProviderEvent::SourceState {
            provider,
            state: ProviderSourceState::Available,
        }
    }

    fn agent_upsert(provider: Provider) -> ProviderEvent {
        ProviderEvent::AgentUpsert {
            provider,
            agent_thread_id: "tracked-agent".to_owned(),
            owner_session_id: None,
            parent_thread_id: None,
            state: Some(ExecState::Working),
            model_id: None,
            depth: Some(0),
            event_id: "tracked-agent-upsert".to_owned(),
            observed_at_ms: 1,
            position: position(20, 0, 1),
        }
    }

    fn agent_activity(provider: Provider) -> ProviderEvent {
        ProviderEvent::Activity {
            provider,
            agent_thread_id: "tracked-agent".to_owned(),
            activity: MinimalProviderMetadata {
                event_kind: Some("working".to_owned()),
                ..MinimalProviderMetadata::default()
            },
            depth: Some(0),
            event_id: "tracked-agent-activity".to_owned(),
            observed_at_ms: 2,
            position: position(20, 0, 2),
        }
    }

    fn empty_performance_inputs() -> (DomainModel, OperatorSnapshot) {
        (
            DomainModel::default(),
            OperatorSnapshot {
                activity: Arc::from(Vec::new()),
                terminal_times: Arc::new(HashMap::new()),
            },
        )
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
            ProviderEvent::Synthesized(_) => "synthesized".to_owned(),
            ProviderEvent::RunLiveness { .. } => "liveness".to_owned(),
            ProviderEvent::LaneClose { .. } => "lane-close".to_owned(),
            ProviderEvent::Telemetry { .. } => "telemetry".to_owned(),
            ProviderEvent::Activity { activity, .. } => activity.event_kind.unwrap(),
            ProviderEvent::SessionResolved {
                agent_thread_id, ..
            } => format!("identity:{agent_thread_id}"),
            ProviderEvent::SourceState {
                state: ProviderSourceState::HistoryDrainBarrier(_),
                ..
            } => "history-barrier".to_owned(),
            ProviderEvent::SourceState { .. } => "source".to_owned(),
            ProviderEvent::Malformed { .. } => "malformed".to_owned(),
            ProviderEvent::AgentUpsert { .. } => "upsert".to_owned(),
        }
    }

    #[test]
    fn stable_history_drain_id_is_manifest_order_independent_and_goalpost_sensitive() {
        let first = FrozenHistoryArtifact {
            provider: Provider::Codex,
            artifact_id: "artifact-a".to_owned(),
            generation: "device-1:inode-10".to_owned(),
            goalpost: 101,
        };
        let second = FrozenHistoryArtifact {
            provider: Provider::Codex,
            artifact_id: "artifact-b".to_owned(),
            generation: "device-1:inode-11".to_owned(),
            goalpost: 202,
        };

        let forward =
            stable_history_drain_id(Provider::Codex, [first.clone(), second.clone()]).unwrap();
        let reverse =
            stable_history_drain_id(Provider::Codex, [second.clone(), first.clone()]).unwrap();
        assert_eq!(forward, reverse);
        assert!(forward.as_str().len() <= crate::model::HistoryDrainId::MAX_BYTES);

        let changed_goalpost = stable_history_drain_id(
            Provider::Codex,
            [
                first.clone(),
                FrozenHistoryArtifact {
                    goalpost: second.goalpost + 1,
                    ..second.clone()
                },
            ],
        )
        .unwrap();
        let changed_identity = stable_history_drain_id(
            Provider::Codex,
            [
                FrozenHistoryArtifact {
                    artifact_id: "artifact-c".to_owned(),
                    ..first.clone()
                },
                second.clone(),
            ],
        )
        .unwrap();
        let changed_generation = stable_history_drain_id(
            Provider::Codex,
            [
                first,
                FrozenHistoryArtifact {
                    generation: "device-1:inode-12".to_owned(),
                    ..second
                },
            ],
        )
        .unwrap();

        assert_ne!(forward, changed_goalpost);
        assert_ne!(forward, changed_identity);
        assert_ne!(forward, changed_generation);
    }

    #[test]
    fn pending_history_barrier_never_overtakes_ordinary_slots() {
        let mut pending = PendingEvents::with_capacity(8, ProviderDiagnostics::default());
        assert_eq!(
            pending.merge(ProviderEvent::RunLiveness {
                key: RunKey::Controller("lane".to_owned()),
                at_ms: 10,
            }),
            MergeOutcome::Accepted
        );
        assert_eq!(
            pending.merge(identity(Provider::Codex, "entity", Some(0), 11)),
            MergeOutcome::Accepted
        );
        assert_eq!(
            pending.merge(activity(
                Provider::Codex,
                "entity",
                "coalesced-old",
                12,
                position(41, 0, 12),
                "old"
            )),
            MergeOutcome::Accepted
        );
        assert_eq!(
            pending.merge(activity(
                Provider::Codex,
                "entity",
                "coalesced-new",
                13,
                position(41, 0, 13),
                "new"
            )),
            MergeOutcome::Coalesced
        );
        assert_eq!(
            pending.merge(ProviderEvent::Malformed {
                provider: Provider::Codex,
                path_display: "artifact.jsonl".to_owned(),
                generation: 0,
                byte_offset: 14,
                error_code: "json",
            }),
            MergeOutcome::Accepted
        );
        let barrier = HistoryDrainBarrier::new(
            crate::model::HistoryDrainId::new("history:v1:barrier-order").unwrap(),
            15,
        );
        assert_eq!(
            pending.merge(ProviderEvent::SourceState {
                provider: Provider::Codex,
                state: ProviderSourceState::HistoryDrainBarrier(barrier.clone()),
            }),
            MergeOutcome::Accepted
        );
        assert_eq!(
            pending.merge(ProviderEvent::SourceState {
                provider: Provider::Codex,
                state: ProviderSourceState::HistoryDrainBarrier(barrier.clone()),
            }),
            MergeOutcome::Duplicate,
            "replaying the held barrier must not allocate a second slot"
        );

        let (sender, mut receiver) = tokio_mpsc::channel(16);
        pending.flush_to(&sender);
        let mut actual = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            actual.push(event_kind(event));
        }
        assert_eq!(
            actual,
            [
                "liveness",
                "identity:entity",
                "new",
                "malformed",
                "history-barrier"
            ]
        );
        assert!(!barrier.acknowledgement().is_committed());
    }

    #[test]
    fn full_pending_buffer_preserves_unaccepted_parser_cursor_and_pauses_worker() {
        struct CursorWorker {
            calls: usize,
            cursor: u64,
        }

        impl ProviderWorker for CursorWorker {
            fn process(&mut self, cycle: &mut ProviderCycle<'_>) -> io::Result<()> {
                self.calls += 1;
                let next_cursor = self.cursor + 1;
                if !matches!(
                    cycle.pending.merge(ProviderEvent::RunLiveness {
                        key: RunKey::Controller(format!("cursor-{next_cursor}")),
                        at_ms: next_cursor as i64,
                    }),
                    MergeOutcome::AtCapacity(_)
                ) {
                    self.cursor = next_cursor;
                }
                Ok(())
            }
        }

        let diagnostics = ProviderDiagnostics::default();
        let mut pending = PendingEvents::with_capacity(1, diagnostics.clone());
        assert_eq!(
            pending.merge(ProviderEvent::RunLiveness {
                key: RunKey::Controller("already-pending".to_owned()),
                at_ms: 1,
            }),
            MergeOutcome::Accepted
        );
        let (egress, _receiver) = tokio_mpsc::channel(1);
        egress.try_send(source_state(Provider::Claude)).unwrap();
        let targets = Arc::new(Mutex::new(Some(TargetSet::default())));
        let force_rescan = AtomicBool::new(false);
        let stop_flag = AtomicBool::new(false);
        let watcher = Arc::new(Mutex::new(Some(WatchRegistry::new(
            Box::new(NoopWatcher),
            diagnostics.clone(),
        ))));
        let mut worker = CursorWorker {
            calls: 0,
            cursor: 77,
        };

        run_provider_cycle(
            &mut worker,
            &ProviderEventSender::Raw(egress),
            &targets,
            &force_rescan,
            &stop_flag,
            &watcher,
            &diagnostics,
            &mut pending,
            None,
            false,
        );

        assert_eq!(
            worker.calls, 0,
            "worker parsed while prior output was blocked"
        );
        assert_eq!(worker.cursor, 77, "unaccepted parser cursor advanced");
        assert!(matches!(
            pending.next_event(),
            Some((PendingToken::Lane, _))
        ));
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

    #[test]
    fn provider_flush_admits_only_events_that_enter_the_bounded_queue() {
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let mut pending = PendingEvents::new(ProviderDiagnostics::default());
        assert_eq!(
            pending.merge(source_state(Provider::Claude)),
            MergeOutcome::Accepted
        );
        assert_eq!(
            pending.merge(agent_upsert(Provider::Codex)),
            MergeOutcome::Accepted
        );
        assert_eq!(
            pending.merge(agent_activity(Provider::Codex)),
            MergeOutcome::Accepted
        );
        pending.flush_to_sender(&ProviderEventSender::Tracked { sender, ingress });
        let (model, operator) = empty_performance_inputs();
        let full = sampler.sample(&model, &operator, 0);
        assert_eq!(
            (
                full.pending_events,
                full.admission_high_water,
                full.events_one_second
            ),
            (1, 1, 1)
        );
        let control = receiver.blocking_recv().unwrap();
        assert!(matches!(control.event, ProviderEvent::SourceState { .. }));
        assert!(control.admission.is_none());
        let reducer_bound = receiver.blocking_recv().unwrap();
        assert!(matches!(
            reducer_bound.event,
            ProviderEvent::AgentUpsert { .. }
        ));
        reducer_bound.admission.unwrap().complete();
        assert_eq!(sampler.sample(&model, &operator, 0).pending_events, 0);
        assert!(matches!(
            pending.next_event(),
            Some((PendingToken::Activity(_), _))
        ));
    }

    #[test]
    fn provider_closed_queue_returns_the_original_pending_event_without_lag_leak() {
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock);
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        let mut pending = PendingEvents::new(ProviderDiagnostics::default());
        assert_eq!(
            pending.merge(agent_upsert(Provider::Claude)),
            MergeOutcome::Accepted
        );
        pending.flush_to_sender(&ProviderEventSender::Tracked { sender, ingress });
        let (model, operator) = empty_performance_inputs();
        let closed = sampler.sample(&model, &operator, 0);
        assert_eq!(
            (
                closed.pending_events,
                closed.admission_high_water,
                closed.events_one_second,
                closed.events_ten_seconds,
                closed.events_sixty_seconds
            ),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(closed.event_lag, Duration::ZERO);
        assert!(matches!(
            pending.next_event(),
            Some((PendingToken::Upsert(_), _))
        ));
    }

    #[test]
    fn provider_malformed_event_carries_no_admission_or_performance_change() {
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let mut pending = PendingEvents::new(ProviderDiagnostics::default());
        assert_eq!(
            pending.merge(ProviderEvent::Malformed {
                provider: Provider::Codex,
                path_display: "~/malformed.jsonl".to_owned(),
                generation: 0,
                byte_offset: 7,
                error_code: "json",
            }),
            MergeOutcome::Accepted
        );

        pending.flush_to_sender(&ProviderEventSender::Tracked { sender, ingress });

        let malformed = receiver.blocking_recv().unwrap();
        assert!(matches!(malformed.event, ProviderEvent::Malformed { .. }));
        assert!(malformed.admission.is_none());
        let (model, operator) = empty_performance_inputs();
        let snapshot = sampler.sample(&model, &operator, 0);
        assert_eq!(
            (
                snapshot.pending_events,
                snapshot.admission_high_water,
                snapshot.events_one_second,
                snapshot.events_ten_seconds,
                snapshot.events_sixty_seconds,
            ),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(snapshot.event_lag, Duration::ZERO);
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

    fn notify_test_sink() -> (NotifySink, Receiver<Control>, Arc<AtomicBool>) {
        let (control, receiver) = mpsc::sync_channel(8);
        let force_rescan = Arc::new(AtomicBool::new(false));
        (
            NotifySink {
                control,
                force_rescan: Arc::clone(&force_rescan),
                diagnostics: ProviderDiagnostics::default(),
            },
            receiver,
            force_rescan,
        )
    }

    #[test]
    fn notify_access_open_and_read_close_events_are_ignored() {
        use notify::event::{AccessKind, AccessMode};

        for kind in [
            notify::EventKind::Access(AccessKind::Open(AccessMode::Any)),
            notify::EventKind::Access(AccessKind::Open(AccessMode::Read)),
            notify::EventKind::Access(AccessKind::Close(AccessMode::Read)),
        ] {
            let (sink, receiver, force_rescan) = notify_test_sink();

            forward_notify_event(
                &sink,
                Ok(notify::Event::new(kind).add_path(PathBuf::from("scan.jsonl"))),
            );

            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            assert!(!force_rescan.load(Ordering::Acquire));
        }
    }

    #[test]
    fn notify_mutations_and_write_close_forward_every_path() {
        use notify::event::{
            AccessKind, AccessMode, CreateKind, DataChange, ModifyKind, RemoveKind,
        };

        for kind in [
            notify::EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            notify::EventKind::Create(CreateKind::File),
            notify::EventKind::Remove(RemoveKind::File),
            notify::EventKind::Access(AccessKind::Close(AccessMode::Write)),
        ] {
            let (sink, receiver, force_rescan) = notify_test_sink();
            let first = PathBuf::from("first.jsonl");
            let second = PathBuf::from("second.jsonl");

            forward_notify_event(
                &sink,
                Ok(notify::Event::new(kind)
                    .add_path(first.clone())
                    .add_path(second.clone())),
            );

            assert!(matches!(receiver.try_recv(), Ok(Control::Hint(path)) if path == first));
            assert!(matches!(receiver.try_recv(), Ok(Control::Hint(path)) if path == second));
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            assert!(!force_rescan.load(Ordering::Acquire));
        }
    }

    #[test]
    fn notify_error_requests_a_full_rescan() {
        let (sink, receiver, force_rescan) = notify_test_sink();

        forward_notify_event(
            &sink,
            Err(notify::Error::generic("synthetic notify failure")),
        );

        assert!(force_rescan.load(Ordering::Acquire));
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
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

    #[derive(Clone, Debug)]
    struct ObservedCycle {
        at: Instant,
        hint: Option<PathBuf>,
        force_rescan: bool,
    }

    struct CycleRecordingWorker {
        cycles: Arc<Mutex<Vec<ObservedCycle>>>,
    }

    impl ProviderWorker for CycleRecordingWorker {
        fn process(&mut self, cycle: &mut ProviderCycle<'_>) -> io::Result<()> {
            lock_unpoisoned(&self.cycles).push(ObservedCycle {
                at: Instant::now(),
                hint: cycle.hint.map(Path::to_path_buf),
                force_rescan: cycle.force_rescan,
            });
            Ok(())
        }
    }

    struct GatedCycleRecordingWorker {
        cycles: Arc<Mutex<Vec<ObservedCycle>>>,
        entered: SyncSender<()>,
        release: Receiver<()>,
        remaining_gates: usize,
    }

    impl ProviderWorker for GatedCycleRecordingWorker {
        fn process(&mut self, cycle: &mut ProviderCycle<'_>) -> io::Result<()> {
            lock_unpoisoned(&self.cycles).push(ObservedCycle {
                at: Instant::now(),
                hint: cycle.hint.map(Path::to_path_buf),
                force_rescan: cycle.force_rescan,
            });
            if self.remaining_gates > 0 {
                self.remaining_gates -= 1;
                self.entered
                    .send(())
                    .map_err(|_| io::Error::other("cycle gate observer disconnected"))?;
                self.release
                    .recv()
                    .map_err(|_| io::Error::other("cycle gate release disconnected"))?;
            }
            Ok(())
        }
    }

    fn wait_for_initial_cycle(cycles: &Mutex<Vec<ObservedCycle>>) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while lock_unpoisoned(cycles).is_empty() && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(!lock_unpoisoned(cycles).is_empty());
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
    fn sustained_hint_overflow_respects_full_rescan_cooldown() {
        run_bounded("sustained hint overflow cooldown", || {
            const MEASURED_CYCLES: usize = 8;

            let rescan_interval = Duration::from_secs(1);
            let cycles = Arc::new(Mutex::new(Vec::new()));
            let (entered_sender, entered_receiver) = mpsc::sync_channel(0);
            let (release_sender, release_receiver) = mpsc::channel();
            let worker = GatedCycleRecordingWorker {
                cycles: Arc::clone(&cycles),
                entered: entered_sender,
                release: release_receiver,
                remaining_gates: MEASURED_CYCLES + 1,
            };
            let (egress, _events) = tokio_mpsc::channel(1);
            let handle =
                spawn_provider_thread_with_rescan_interval(worker, egress, None, rescan_interval)
                    .unwrap();
            let diagnostics = handle.diagnostics();
            let flood_sink = NotifySink {
                control: handle.control.clone(),
                force_rescan: Arc::clone(&handle.force_rescan),
                diagnostics: diagnostics.clone(),
            };
            let mut gate_error = entered_receiver.recv_timeout(Duration::from_secs(1)).err();
            let started = Instant::now();
            for _ in 0..MEASURED_CYCLES {
                if gate_error.is_some() {
                    break;
                }
                for _ in 0..=CONTROL_CHANNEL_CAPACITY {
                    flood_sink.hint(PathBuf::from("overflow.jsonl"));
                }
                if release_sender.send(()).is_err() {
                    gate_error = Some(mpsc::RecvTimeoutError::Disconnected);
                    break;
                }
                gate_error = entered_receiver.recv_timeout(Duration::from_secs(1)).err();
            }
            let elapsed = started.elapsed();

            for _ in 0..=MEASURED_CYCLES {
                let _ = release_sender.send(());
            }
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.stop())
                .unwrap();

            assert!(gate_error.is_none(), "provider cycle gate timed out");
            let full_rescans = lock_unpoisoned(&cycles)
                .iter()
                .filter(|cycle| cycle.at >= started && cycle.force_rescan)
                .count();
            let loose_bound = elapsed.as_millis() / rescan_interval.as_millis() + 2;
            assert!(
                diagnostics.dropped_hints() > 0,
                "the gated flood never saturated the channel"
            );
            assert!(full_rescans > 0, "the periodic fallback never ran");
            assert!(
                full_rescans as u128 <= loose_bound,
                "{full_rescans} full rescans exceeded cooldown bound {loose_bound} in {elapsed:?}"
            );
        });
    }

    #[test]
    fn rapid_duplicate_hints_are_debounced_and_deduplicated() {
        run_bounded("duplicate hint debounce", || {
            let cycles = Arc::new(Mutex::new(Vec::new()));
            let worker = CycleRecordingWorker {
                cycles: Arc::clone(&cycles),
            };
            let (egress, _events) = tokio_mpsc::channel(1);
            let handle = spawn_provider_thread_with_rescan_interval(
                worker,
                egress,
                None,
                Duration::from_secs(1),
            )
            .unwrap();
            wait_for_initial_cycle(&cycles);
            let path = PathBuf::from("same.jsonl");
            let burst_started = Instant::now();

            for _ in 0..64 {
                handle.hint(path.clone());
            }
            thread::sleep(Duration::from_millis(350));
            let mixed_started = Instant::now();
            for _ in 0..32 {
                handle.hint(PathBuf::from("first.jsonl"));
                handle.hint(PathBuf::from("second.jsonl"));
            }
            thread::sleep(Duration::from_millis(350));
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.stop())
                .unwrap();

            let recorded = lock_unpoisoned(&cycles);
            let single_path_cycles = recorded
                .iter()
                .filter(|cycle| cycle.at >= burst_started && cycle.at < mixed_started)
                .collect::<Vec<_>>();
            assert!(
                single_path_cycles
                    .iter()
                    .any(|cycle| cycle.hint.as_ref() == Some(&path)),
                "the single-path hint was never processed"
            );
            assert!(
                single_path_cycles.len() <= 3,
                "duplicate burst produced {} cycles",
                single_path_cycles.len()
            );
            let mixed_cycles = recorded
                .iter()
                .filter(|cycle| cycle.at >= mixed_started && !cycle.force_rescan)
                .collect::<Vec<_>>();
            assert_eq!(
                mixed_cycles.len(),
                1,
                "mixed burst must produce one non-full cycle"
            );
            assert!(
                mixed_cycles[0].hint.is_none(),
                "mixed burst must not select one arbitrary hint"
            );
        });
    }

    #[test]
    fn force_request_inside_cooldown_is_deferred_but_not_lost() {
        run_bounded("deferred force rescan", || {
            let rescan_interval = Duration::from_secs(1);
            let cycles = Arc::new(Mutex::new(Vec::new()));
            let (entered_sender, entered_receiver) = mpsc::sync_channel(0);
            let (release_sender, release_receiver) = mpsc::channel();
            let worker = GatedCycleRecordingWorker {
                cycles: Arc::clone(&cycles),
                entered: entered_sender,
                release: release_receiver,
                remaining_gates: 2,
            };
            let (egress, _events) = tokio_mpsc::channel(1);
            let handle =
                spawn_provider_thread_with_rescan_interval(worker, egress, None, rescan_interval)
                    .unwrap();
            let path = PathBuf::from("deferred.jsonl");
            let initial_gate_error = entered_receiver.recv_timeout(Duration::from_secs(1)).err();
            let requested_at = Instant::now();
            if initial_gate_error.is_none() {
                handle.force_rescan.store(true, Ordering::Release);
                handle.hint(path.clone());
            }
            let _ = release_sender.send(());

            let scoped_gate_error = if initial_gate_error.is_none() {
                entered_receiver.recv_timeout(Duration::from_secs(2)).err()
            } else {
                Some(mpsc::RecvTimeoutError::Disconnected)
            };
            // Observe the deferred request while the scoped cycle cannot advance and clear it.
            let scoped_cycle = scoped_gate_error
                .is_none()
                .then(|| lock_unpoisoned(&cycles).last().cloned())
                .flatten();
            let force_request_pending =
                scoped_gate_error.is_none() && handle.force_rescan.load(Ordering::Acquire);

            let full_wait_started = Instant::now();
            let _ = release_sender.send(());
            let full_deadline = full_wait_started + rescan_interval + Duration::from_secs(2);
            let mut deferred_full_rescan_observed = false;
            if scoped_gate_error.is_none() {
                while Instant::now() < full_deadline {
                    if lock_unpoisoned(&cycles)
                        .iter()
                        .any(|cycle| cycle.at >= requested_at && cycle.force_rescan)
                    {
                        deferred_full_rescan_observed = true;
                        break;
                    }
                    thread::yield_now();
                }
            }

            let stop_result = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(handle.stop());

            assert!(
                initial_gate_error.is_none(),
                "initial provider cycle gate timed out: {initial_gate_error:?}"
            );
            assert!(
                scoped_gate_error.is_none(),
                "scoped provider cycle gate timed out: {scoped_gate_error:?}"
            );
            stop_result.unwrap();
            let scoped_cycle = scoped_cycle.expect("scoped cycle was not recorded");
            assert_eq!(
                scoped_cycle.hint.as_ref(),
                Some(&path),
                "the gated scoped cycle did not carry the queued hint"
            );
            assert!(
                !scoped_cycle.force_rescan,
                "force request bypassed the cooldown"
            );
            assert!(
                force_request_pending,
                "deferred force request was cleared inside the cooldown"
            );
            assert!(
                deferred_full_rescan_observed,
                "deferred full rescan was lost"
            );
        });
    }

    #[test]
    fn saturated_egress_pauses_worker_without_blocking_provider_thread() {
        run_bounded("saturated egress pause", || {
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
            let deadline = Instant::now() + Duration::from_secs(1);
            while calls.load(Ordering::Relaxed) < 1 && Instant::now() < deadline {
                thread::yield_now();
            }
            for index in 0..3 {
                handle.hint(PathBuf::from(format!("hint-{index}")));
            }
            while handle.diagnostics().egress_saturations() == 0 && Instant::now() < deadline {
                thread::yield_now();
            }
            assert_eq!(calls.load(Ordering::Relaxed), 1);
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
