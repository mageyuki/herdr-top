//! T4C entity structs, `DomainModel`, `SharedModel`, `NormalizedEvent`, `GapKind`,
//! `ReconcileBatch`, and `TopologySnapshot`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use super::ids::{DisplayOrdinal, Provider, RunId, RunKey};
use super::state::{ExecState, TaskState};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Workspace {
    pub workspace_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Tab {
    pub tab_id: String,
    pub workspace_id: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Pane {
    pub pane_id: String,
    pub workspace_id: String,
    pub tab_id: String,
    pub terminal_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Execution {
    pub execution_id: String,
    pub pane_id: String,
    pub terminal_id: String,
    pub task_run_id: RunId,
    pub state: ExecState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskRun {
    pub run_id: RunId,
    pub key: RunKey,
    pub display_ordinal: DisplayOrdinal,
    pub state: TaskState,
    pub has_controller_task_state_event: bool,
    #[serde(default)]
    pub created_at_ms: Option<i64>,
    #[serde(default)]
    pub updated_at_ms: Option<i64>,
    #[serde(default)]
    pub finished_at_ms: Option<i64>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub dismissed_at_ms: Option<i64>,
}

impl TaskRun {
    /// Advances bookkeeping for a mutation observed at `timestamp_ms` receipt time.
    pub fn touch(&mut self, timestamp_ms: i64) {
        self.updated_at_ms = Some(timestamp_ms);
        if self.state.is_terminal() {
            self.finished_at_ms.get_or_insert(timestamp_ms);
        } else {
            self.finished_at_ms = None;
            self.dismissed_at_ms = None;
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentNode {
    pub agent_node_id: String,
    pub provider: Provider,
    /// This node's own provider-native thread/session identifier.
    pub native_session_id: Option<String>,
    pub task_run_id: RunId,
    pub display_ordinal: DisplayOrdinal,
    pub parent_agent_node_id: Option<String>,
    pub state: Option<ExecState>,
    pub model_id: Option<String>,
    pub last_event_kind: Option<String>,
    pub last_tool_name: Option<String>,
    pub last_item_count: Option<u64>,
    pub last_byte_count: Option<u64>,
    pub last_activity_at_ms: Option<i64>,
    pub session_file: Option<String>,
}

/// Provider observation used to insert or patch an [`AgentNode`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentNodeObservation {
    pub agent_node_id: String,
    pub provider: Provider,
    pub native_session_id: Option<String>,
    pub task_run_id: RunId,
    pub parent_agent_node_id: Option<String>,
    pub state: Option<ExecState>,
    pub model_id: Option<String>,
    pub session_file: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ExecutionEdge {
    pub parent_run_id: RunId,
    pub child_run_id: RunId,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct DependencyEdge {
    pub prerequisite_run_id: RunId,
    pub dependent_run_id: RunId,
}

/// Counters maintained by the serialized reducer for Controller-input diagnostics.
#[derive(Clone, Debug, Default)]
pub struct ControllerDiagnostics {
    binding_conflicts: u64,
    terminal_blocked_progress_noops: u64,
    terminal_forward_reference_creations: u64,
    dangling_announcement_components: u64,
    ingest_sequence_exhaustions: u64,
    provider_parent_conflicts: u64,
    provider_identity_disagreements: u64,
    acceptor: ControllerDiagnosticsHandle,
}

impl ControllerDiagnostics {
    /// Identity-binding observations dropped after transactional preflight.
    #[must_use]
    pub const fn binding_conflicts(&self) -> u64 {
        self.binding_conflicts
    }

    /// Blocked/progress observations accepted as no-ops on terminal runs.
    #[must_use]
    pub const fn terminal_blocked_progress_noops(&self) -> u64 {
        self.terminal_blocked_progress_noops
    }

    /// Terminal Controller observations that created an unknown run.
    #[must_use]
    pub const fn terminal_forward_reference_creations(&self) -> u64 {
        self.terminal_forward_reference_creations
    }

    /// Dangling relationship-only components observed by the reducer.
    #[must_use]
    pub const fn dangling_announcement_components(&self) -> u64 {
        self.dangling_announcement_components
    }

    /// Admissions rejected because the durable ingest-sequence domain is exhausted.
    #[must_use]
    pub const fn ingest_sequence_exhaustions(&self) -> u64 {
        self.ingest_sequence_exhaustions
    }

    /// Provider parent links rejected because they were self-links or cycles.
    #[must_use]
    pub const fn provider_parent_conflicts(&self) -> u64 {
        self.provider_parent_conflicts
    }

    /// Herdr ID-kind identity facts that disagreed with adapter-resolved identity.
    #[must_use]
    pub const fn provider_identity_disagreements(&self) -> u64 {
        self.provider_identity_disagreements
    }

    /// Controller socket admissions rejected because the acceptor was saturated.
    #[must_use]
    pub fn socket_saturations(&self) -> u64 {
        self.acceptor.socket_saturations()
    }

    /// Controller listener accept calls that failed transiently.
    #[must_use]
    pub fn accept_failures(&self) -> u64 {
        self.acceptor.accept_failures()
    }

    /// Returns the atomic counter handle shared with the socket acceptor.
    #[must_use]
    pub fn acceptor_handle(&self) -> ControllerDiagnosticsHandle {
        self.acceptor.clone()
    }

    pub(crate) fn record_binding_conflict(&mut self) {
        self.binding_conflicts = self.binding_conflicts.saturating_add(1);
    }

    pub(crate) fn record_terminal_blocked_progress_noops(&mut self, count: u64) {
        self.terminal_blocked_progress_noops =
            self.terminal_blocked_progress_noops.saturating_add(count);
    }

    pub(crate) fn record_terminal_forward_reference_creations(&mut self, count: u64) {
        self.terminal_forward_reference_creations = self
            .terminal_forward_reference_creations
            .saturating_add(count);
    }

    pub(crate) fn set_dangling_announcement_components(&mut self, count: u64) {
        self.dangling_announcement_components = count;
    }

    pub(crate) fn record_ingest_sequence_exhaustion(&mut self) {
        self.ingest_sequence_exhaustions = self.ingest_sequence_exhaustions.saturating_add(1);
    }

    pub(crate) fn record_provider_parent_conflict(&mut self) {
        self.provider_parent_conflicts = self.provider_parent_conflicts.saturating_add(1);
    }

    pub(crate) fn record_provider_identity_disagreement(&mut self) {
        self.provider_identity_disagreements =
            self.provider_identity_disagreements.saturating_add(1);
    }
}

/// Acceptor-shareable atomic portion of the Controller diagnostic counters.
#[derive(Clone, Debug, Default)]
pub struct ControllerDiagnosticsHandle {
    socket_saturations: Arc<AtomicU64>,
    accept_failures: Arc<AtomicU64>,
}

/// Reader-shareable atomic counters for the pane-status enrichment stream.
#[derive(Clone, Debug, Default)]
pub struct EnrichmentDiagnosticsHandle {
    channel_full_drops: Arc<AtomicU64>,
    episode_discards: Arc<AtomicU64>,
}

/// Provider-I/O counters exposed through coherent domain-model snapshots.
#[derive(Clone, Debug, Default)]
pub struct ProviderDiagnostics {
    handle: ProviderDiagnosticsHandle,
}

impl ProviderDiagnostics {
    #[must_use]
    pub fn dropped_hints(&self) -> u64 {
        self.handle.dropped_hints()
    }

    #[must_use]
    pub fn coalesced_updates(&self) -> u64 {
        self.handle.coalesced_updates()
    }

    #[must_use]
    pub fn duplicate_events(&self) -> u64 {
        self.handle.duplicate_events()
    }

    /// Invalid provider targets skipped without interrupting sibling work.
    #[must_use]
    pub fn invalid_targets(&self) -> u64 {
        self.handle.invalid_targets()
    }

    /// Duplicate target decompositions collapsed to one path owner per cycle.
    #[must_use]
    pub fn duplicate_path_targets(&self) -> u64 {
        self.handle.duplicate_path_targets()
    }

    #[must_use]
    pub fn egress_saturations(&self) -> u64 {
        self.handle.egress_saturations()
    }

    #[must_use]
    pub fn egress_closed(&self) -> u64 {
        self.handle.egress_closed()
    }

    #[must_use]
    pub fn malformed_records(&self) -> u64 {
        self.handle.malformed_records()
    }

    #[must_use]
    pub fn watch_cap_fallbacks(&self) -> u64 {
        self.handle.watch_cap_fallbacks()
    }

    /// Roots whose baseline was captured late or approximated at their first scan.
    #[must_use]
    pub fn baseline_approximations(&self) -> u64 {
        self.handle.baseline_approximations()
    }

    /// Notify backends that failed creation and fell back to periodic polling.
    #[must_use]
    pub fn notify_creation_failures(&self) -> u64 {
        self.handle.notify_creation_failures()
    }

    #[must_use]
    pub fn provider_cycles(&self) -> u64 {
        self.handle.provider_cycles()
    }

    #[must_use]
    pub fn provider_io_errors(&self) -> u64 {
        self.handle.provider_io_errors()
    }

    /// Returns the atomic handle shared with the provider I/O thread.
    #[must_use]
    pub fn handle(&self) -> ProviderDiagnosticsHandle {
        self.handle.clone()
    }
}

/// Provider-thread-shareable atomic diagnostic counters.
#[derive(Clone, Debug, Default)]
pub struct ProviderDiagnosticsHandle {
    dropped_hints: Arc<AtomicU64>,
    coalesced_updates: Arc<AtomicU64>,
    duplicate_events: Arc<AtomicU64>,
    invalid_targets: Arc<AtomicU64>,
    duplicate_path_targets: Arc<AtomicU64>,
    egress_saturations: Arc<AtomicU64>,
    egress_closed: Arc<AtomicU64>,
    malformed_records: Arc<AtomicU64>,
    watch_cap_fallbacks: Arc<AtomicU64>,
    baseline_approximations: Arc<AtomicU64>,
    notify_creation_failures: Arc<AtomicU64>,
    provider_cycles: Arc<AtomicU64>,
    provider_io_errors: Arc<AtomicU64>,
}

impl ProviderDiagnosticsHandle {
    fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }

    pub fn record_dropped_hint(&self) {
        Self::increment(&self.dropped_hints);
    }

    pub fn record_coalesced_update(&self) {
        Self::increment(&self.coalesced_updates);
    }

    pub fn record_duplicate_event(&self) {
        Self::increment(&self.duplicate_events);
    }

    pub fn record_invalid_target(&self) {
        Self::increment(&self.invalid_targets);
    }

    pub fn record_duplicate_path_target(&self) {
        Self::increment(&self.duplicate_path_targets);
    }

    pub fn record_egress_saturation(&self) {
        Self::increment(&self.egress_saturations);
    }

    pub fn record_egress_closed(&self) {
        Self::increment(&self.egress_closed);
    }

    pub fn record_malformed_record(&self) {
        Self::increment(&self.malformed_records);
    }

    pub fn record_watch_cap_fallback(&self) {
        Self::increment(&self.watch_cap_fallbacks);
    }

    /// Records one late or first-scan-approximated root baseline.
    pub fn record_baseline_approximation(&self) {
        Self::increment(&self.baseline_approximations);
    }

    /// Records one notify backend creation failure handled by polling fallback.
    pub fn record_notify_creation_failure(&self) {
        Self::increment(&self.notify_creation_failures);
    }

    pub fn record_provider_cycle(&self) {
        Self::increment(&self.provider_cycles);
    }

    pub fn record_provider_io_error(&self) {
        Self::increment(&self.provider_io_errors);
    }

    #[must_use]
    pub fn dropped_hints(&self) -> u64 {
        self.dropped_hints.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn coalesced_updates(&self) -> u64 {
        self.coalesced_updates.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn duplicate_events(&self) -> u64 {
        self.duplicate_events.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn invalid_targets(&self) -> u64 {
        self.invalid_targets.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn duplicate_path_targets(&self) -> u64 {
        self.duplicate_path_targets.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn egress_saturations(&self) -> u64 {
        self.egress_saturations.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn egress_closed(&self) -> u64 {
        self.egress_closed.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn malformed_records(&self) -> u64 {
        self.malformed_records.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn watch_cap_fallbacks(&self) -> u64 {
        self.watch_cap_fallbacks.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn baseline_approximations(&self) -> u64 {
        self.baseline_approximations.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn notify_creation_failures(&self) -> u64 {
        self.notify_creation_failures.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn provider_cycles(&self) -> u64 {
        self.provider_cycles.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn provider_io_errors(&self) -> u64 {
        self.provider_io_errors.load(Ordering::Relaxed)
    }
}

impl ControllerDiagnosticsHandle {
    /// Records one failed Controller listener accept call.
    pub fn record_accept_failure(&self) {
        let _ = self
            .accept_failures
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(1))
            });
    }

    /// Returns the current failed-accept count.
    #[must_use]
    pub fn accept_failures(&self) -> u64 {
        self.accept_failures.load(Ordering::Relaxed)
    }

    /// Records one admission rejected because the Controller socket was saturated.
    pub fn record_socket_saturation(&self) {
        let _ =
            self.socket_saturations
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_add(1))
                });
    }

    /// Returns the current saturation count.
    #[must_use]
    pub fn socket_saturations(&self) -> u64 {
        self.socket_saturations.load(Ordering::Relaxed)
    }
}

impl EnrichmentDiagnosticsHandle {
    fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }

    /// Records one parsed receipt dropped because the enrichment channel was full.
    pub fn record_channel_full_drop(&self) {
        Self::increment(&self.channel_full_drops);
    }

    /// Records one queued receipt discarded outside the Live phase.
    pub fn record_episode_discard(&self) {
        Self::increment(&self.episode_discards);
    }

    /// Returns the process-lifetime full-channel drop count.
    #[must_use]
    pub fn channel_full_drops(&self) -> u64 {
        self.channel_full_drops.load(Ordering::Relaxed)
    }

    /// Returns the process-lifetime non-Live discard count.
    #[must_use]
    pub fn episode_discards(&self) -> u64 {
        self.episode_discards.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug, Default)]
pub struct DomainModel {
    workspaces: HashMap<String, Workspace>,
    tabs: HashMap<String, Tab>,
    panes: HashMap<String, Pane>,
    topology_ordinals: HashMap<(TopologyKind, String), DisplayOrdinal>,
    task_runs: HashMap<RunId, TaskRun>,
    run_ids_by_key: HashMap<RunKey, RunId>,
    executions: HashMap<String, Execution>,
    agent_nodes: HashMap<String, AgentNode>,
    execution_edges: HashSet<ExecutionEdge>,
    dependency_edges: HashSet<DependencyEdge>,
    controller_diagnostics: ControllerDiagnostics,
    provider_diagnostics: ProviderDiagnostics,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum TopologyKind {
    Workspace,
    Tab,
    Pane,
}

impl DomainModel {
    /// Returns the reducer-owned Controller diagnostic counters.
    #[must_use]
    pub const fn controller_diagnostics(&self) -> &ControllerDiagnostics {
        &self.controller_diagnostics
    }

    pub(crate) const fn controller_diagnostics_mut(&mut self) -> &mut ControllerDiagnostics {
        &mut self.controller_diagnostics
    }

    /// Returns the provider I/O diagnostic counters.
    #[must_use]
    pub const fn provider_diagnostics(&self) -> &ProviderDiagnostics {
        &self.provider_diagnostics
    }

    pub fn insert_workspace(&mut self, workspace: Workspace) -> Option<Workspace> {
        self.workspaces
            .insert(workspace.workspace_id.clone(), workspace)
    }

    #[must_use]
    pub fn workspace(&self, workspace_id: &str) -> Option<&Workspace> {
        self.workspaces.get(workspace_id)
    }

    pub fn workspaces(&self) -> impl Iterator<Item = &Workspace> {
        self.workspaces.values()
    }

    /// Returns a workspace's persisted display ordinal.
    #[must_use]
    pub fn workspace_ordinal(&self, workspace_id: &str) -> Option<DisplayOrdinal> {
        self.topology_ordinal(TopologyKind::Workspace, workspace_id)
    }

    pub(crate) fn set_workspace_ordinal(
        &mut self,
        workspace_id: String,
        ordinal: DisplayOrdinal,
    ) -> Option<DisplayOrdinal> {
        self.set_topology_ordinal(TopologyKind::Workspace, workspace_id, ordinal)
    }

    pub(crate) fn remove_workspace_ordinal(
        &mut self,
        workspace_id: &str,
    ) -> Option<DisplayOrdinal> {
        self.remove_topology_ordinal(TopologyKind::Workspace, workspace_id)
    }

    pub fn insert_tab(&mut self, tab: Tab) -> Option<Tab> {
        self.tabs.insert(tab.tab_id.clone(), tab)
    }

    #[must_use]
    pub fn tab(&self, tab_id: &str) -> Option<&Tab> {
        self.tabs.get(tab_id)
    }

    pub fn tabs(&self) -> impl Iterator<Item = &Tab> {
        self.tabs.values()
    }

    /// Returns a tab's persisted display ordinal.
    #[must_use]
    pub fn tab_ordinal(&self, tab_id: &str) -> Option<DisplayOrdinal> {
        self.topology_ordinal(TopologyKind::Tab, tab_id)
    }

    pub(crate) fn set_tab_ordinal(
        &mut self,
        tab_id: String,
        ordinal: DisplayOrdinal,
    ) -> Option<DisplayOrdinal> {
        self.set_topology_ordinal(TopologyKind::Tab, tab_id, ordinal)
    }

    pub(crate) fn remove_tab_ordinal(&mut self, tab_id: &str) -> Option<DisplayOrdinal> {
        self.remove_topology_ordinal(TopologyKind::Tab, tab_id)
    }

    pub fn insert_pane(&mut self, pane: Pane) -> Option<Pane> {
        self.panes.insert(pane.pane_id.clone(), pane)
    }

    #[must_use]
    pub fn pane(&self, pane_id: &str) -> Option<&Pane> {
        self.panes.get(pane_id)
    }

    pub fn panes(&self) -> impl Iterator<Item = &Pane> {
        self.panes.values()
    }

    /// Returns a pane's persisted display ordinal.
    #[must_use]
    pub fn pane_ordinal(&self, pane_id: &str) -> Option<DisplayOrdinal> {
        self.topology_ordinal(TopologyKind::Pane, pane_id)
    }

    pub(crate) fn set_pane_ordinal(
        &mut self,
        pane_id: String,
        ordinal: DisplayOrdinal,
    ) -> Option<DisplayOrdinal> {
        self.set_topology_ordinal(TopologyKind::Pane, pane_id, ordinal)
    }

    pub(crate) fn remove_pane_ordinal(&mut self, pane_id: &str) -> Option<DisplayOrdinal> {
        self.remove_topology_ordinal(TopologyKind::Pane, pane_id)
    }

    fn topology_ordinal(&self, kind: TopologyKind, id: &str) -> Option<DisplayOrdinal> {
        self.topology_ordinals.get(&(kind, id.to_owned())).copied()
    }

    fn set_topology_ordinal(
        &mut self,
        kind: TopologyKind,
        id: String,
        ordinal: DisplayOrdinal,
    ) -> Option<DisplayOrdinal> {
        self.topology_ordinals.insert((kind, id), ordinal)
    }

    fn remove_topology_ordinal(&mut self, kind: TopologyKind, id: &str) -> Option<DisplayOrdinal> {
        self.topology_ordinals.remove(&(kind, id.to_owned()))
    }

    pub fn insert_task_run(&mut self, task_run: TaskRun) -> Option<TaskRun> {
        let run_id = task_run.run_id;
        let key = task_run.key.clone();
        let replaced = self.task_runs.insert(run_id, task_run);
        if let Some(previous) = &replaced
            && self.run_ids_by_key.get(&previous.key) == Some(&run_id)
        {
            self.run_ids_by_key.remove(&previous.key);
        }
        self.run_ids_by_key.insert(key, run_id);
        replaced
    }

    #[must_use]
    pub fn task_run(&self, run_id: &RunId) -> Option<&TaskRun> {
        self.task_runs.get(run_id)
    }

    #[must_use]
    pub fn task_run_by_key(&self, key: &RunKey) -> Option<&TaskRun> {
        self.run_ids_by_key
            .get(key)
            .and_then(|run_id| self.task_runs.get(run_id))
    }

    pub fn task_runs(&self) -> impl Iterator<Item = &TaskRun> {
        self.task_runs.values()
    }

    pub fn insert_execution(&mut self, execution: Execution) -> Option<Execution> {
        self.executions
            .insert(execution.execution_id.clone(), execution)
    }

    #[must_use]
    pub fn execution(&self, execution_id: &str) -> Option<&Execution> {
        self.executions.get(execution_id)
    }

    pub fn executions(&self) -> impl Iterator<Item = &Execution> {
        self.executions.values()
    }

    pub fn insert_agent_node(&mut self, agent_node: AgentNode) -> Option<AgentNode> {
        self.agent_nodes
            .insert(agent_node.agent_node_id.clone(), agent_node)
    }

    #[must_use]
    pub fn agent_node(&self, agent_node_id: &str) -> Option<&AgentNode> {
        self.agent_nodes.get(agent_node_id)
    }

    pub fn agent_nodes(&self) -> impl Iterator<Item = &AgentNode> {
        self.agent_nodes.values()
    }

    pub fn insert_execution_edge(&mut self, edge: ExecutionEdge) -> bool {
        self.execution_edges.insert(edge)
    }

    pub fn execution_edges(&self) -> impl Iterator<Item = &ExecutionEdge> {
        self.execution_edges.iter()
    }

    pub fn insert_dependency_edge(&mut self, edge: DependencyEdge) -> bool {
        self.dependency_edges.insert(edge)
    }

    pub fn dependency_edges(&self) -> impl Iterator<Item = &DependencyEdge> {
        self.dependency_edges.iter()
    }

    /// Adds a superseded key that resolves directly to its canonical task run.
    pub fn insert_task_run_alias(&mut self, key: RunKey, canonical_run_id: RunId) -> Option<RunId> {
        self.run_ids_by_key.insert(key, canonical_run_id)
    }

    /// Restores a historical run without making its collector-scoped key addressable.
    pub fn insert_historical_task_run(&mut self, task_run: TaskRun) -> Option<TaskRun> {
        self.task_runs.insert(task_run.run_id, task_run)
    }

    pub(crate) fn remove_task_run_record(&mut self, run_id: &RunId) -> Option<TaskRun> {
        self.task_runs.remove(run_id)
    }

    pub(crate) fn task_run_bindings(&self) -> impl Iterator<Item = (&RunKey, &RunId)> {
        self.run_ids_by_key.iter()
    }

    pub(crate) fn executions_mut(&mut self) -> impl Iterator<Item = &mut Execution> {
        self.executions.values_mut()
    }

    pub(crate) fn agent_nodes_mut(&mut self) -> impl Iterator<Item = &mut AgentNode> {
        self.agent_nodes.values_mut()
    }

    pub(crate) fn execution_edges_mut(&mut self) -> &mut HashSet<ExecutionEdge> {
        &mut self.execution_edges
    }

    pub(crate) fn dependency_edges_mut(&mut self) -> &mut HashSet<DependencyEdge> {
        &mut self.dependency_edges
    }

    /// Removes one workspace from the current physical topology.
    pub fn remove_workspace(&mut self, workspace_id: &str) -> Option<Workspace> {
        self.workspaces.remove(workspace_id)
    }

    /// Removes one tab from the current physical topology.
    pub fn remove_tab(&mut self, tab_id: &str) -> Option<Tab> {
        self.tabs.remove(tab_id)
    }

    /// Removes one pane from the current physical topology.
    pub fn remove_pane(&mut self, pane_id: &str) -> Option<Pane> {
        self.panes.remove(pane_id)
    }
}

pub type SharedModel = tokio::sync::watch::Receiver<Arc<DomainModel>>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceCoverage {
    pub source: String,
    pub available: bool,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MinimalProviderMetadata {
    pub agent_id: Option<String>,
    pub parent_agent_id: Option<String>,
    pub model_id: Option<String>,
    pub event_kind: Option<String>,
    pub tool_name: Option<String>,
    pub item_count: Option<u64>,
    pub byte_count: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventMetadata {
    pub event_id: String,
    /// Source-emitted time. This is display metadata, never an ordering or retention key.
    pub timestamp_ms: i64,
    /// Collector-local receipt time used for persistence and retention.
    pub receipt_time_ms: i64,
    pub source: String,
    pub source_event_type: String,
    pub herdr_session: String,
    pub workspace_id: Option<String>,
    pub tab_id: Option<String>,
    pub pane_id: Option<String>,
    pub terminal_id: Option<String>,
    pub provider: Option<Provider>,
    pub native_session_id: Option<String>,
    pub task_run_id: Option<RunId>,
    pub agent_node_id: Option<String>,
    pub task_state: Option<TaskState>,
    pub execution_parent: Option<ExecutionEdge>,
    pub dependency: Option<DependencyEdge>,
    pub source_coverage: Vec<SourceCoverage>,
    pub provider_metadata: Option<MinimalProviderMetadata>,
    pub label: Option<String>,
    pub reason: Option<String>,
    /// Validated Controller progress in basis points (`0..=10_000`).
    pub progress: Option<u16>,
    /// Reducer-assigned global Controller ingest sequence.
    pub ingest_seq: Option<u64>,
}

/// One validated Controller event before its raw keys are resolved to internal run IDs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControllerEvent {
    pub schema_version: u64,
    pub task_run_id: String,
    pub metadata: EventMetadata,
    pub event: ControllerEventKind,
}

/// The nine Controller event types with their required endpoint carried by the variant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "event_type")]
pub enum ControllerEventKind {
    Dispatch { parent_task_run_id: String },
    TaskStarted,
    DependsOn { depends_on_id: String },
    Blocked,
    Progress,
    Complete,
    Failed,
    Cancelled,
    Dismiss,
}

/// Returned when a wire progress number is not finite or outside `0.0..=1.0`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgressOutOfRange;

/// Quantizes a wire progress number to basis points, rounding half away from zero.
pub fn quantize_progress(value: f64) -> Result<u16, ProgressOutOfRange> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(ProgressOutOfRange);
    }
    let rounded = (value * 10_000.0).round();
    if !(0.0..=10_000.0).contains(&rounded) {
        return Err(ProgressOutOfRange);
    }
    Ok(rounded as u16)
}

/// Escapes control characters and truncates Controller display text to 256 UTF-8 bytes.
#[must_use]
pub fn sanitize_controller_text(value: &str) -> String {
    const MAX_BYTES: usize = 256;

    let mut sanitized = String::new();
    for character in value.chars() {
        if character.is_control() {
            let escaped = character.escape_default().collect::<String>();
            if sanitized.len().saturating_add(escaped.len()) > MAX_BYTES {
                return sanitized;
            }
            sanitized.push_str(&escaped);
        } else {
            if sanitized.len().saturating_add(character.len_utf8()) > MAX_BYTES {
                break;
            }
            sanitized.push(character);
        }
    }
    sanitized
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TopologyEntity {
    Workspace(Workspace),
    Tab(Tab),
    Pane(Pane),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TopologyEntityId {
    Workspace { workspace_id: String },
    Tab { tab_id: String },
    Pane { pane_id: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NormalizedEvent {
    ControllerEvent {
        metadata: EventMetadata,
        event: ControllerEventKind,
    },
    TopologyUpsert {
        metadata: EventMetadata,
        entity: TopologyEntity,
    },
    TopologyClosure {
        metadata: EventMetadata,
        entity: TopologyEntityId,
    },
    AgentStatusChanged {
        metadata: EventMetadata,
        execution_id: String,
        state: ExecState,
    },
    AgentNodeUpsert {
        metadata: EventMetadata,
        node: AgentNodeObservation,
    },
    AgentActivity {
        metadata: EventMetadata,
        agent_node_id: String,
        activity: MinimalProviderMetadata,
    },
    ExecutionBegin {
        metadata: EventMetadata,
        execution: Execution,
    },
    ExecutionEnd {
        metadata: EventMetadata,
        execution_id: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum GapKind {
    Startup,
    Reconnect,
    SocketReplacement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReconcileBatch {
    pub topology: TopologySnapshot,
    pub gap_kind: GapKind,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TopologySnapshot {
    pub workspaces: Vec<Workspace>,
    pub tabs: Vec<Tab>,
    pub panes: Vec<PaneSnapshot>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PaneSnapshot {
    pub pane_id: String,
    pub workspace_id: String,
    pub tab_id: String,
    pub terminal_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub agent: Option<SnapshotAgent>,
    pub agent_session: Option<AgentSessionReference>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotAgent {
    pub agent_name: String,
    pub state: ExecState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentSessionReference {
    pub source: String,
    pub agent: String,
    pub kind: AgentSessionReferenceKind,
    pub value: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AgentSessionReferenceKind {
    Id,
    Path,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ids::{DisplayOrdinal, Provider, RunId, RunKey};
    use crate::model::state::{ExecState, TaskState};

    #[test]
    fn domain_model_construction() {
        let mut model = DomainModel::default();
        let run_id = RunId::new();
        let dependency_id = RunId::new();
        let run_key = RunKey::Provisional {
            terminal_id: "terminal-1".to_owned(),
            start_ms: 100,
            seq: 1,
        };

        model.insert_workspace(Workspace {
            workspace_id: "workspace-1".to_owned(),
        });
        model.insert_tab(Tab {
            tab_id: "tab-1".to_owned(),
            workspace_id: "workspace-1".to_owned(),
            label: None,
        });
        model.insert_pane(Pane {
            pane_id: "pane-1".to_owned(),
            workspace_id: "workspace-1".to_owned(),
            tab_id: "tab-1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            display_name: None,
        });
        model.insert_task_run(TaskRun {
            run_id,
            key: run_key.clone(),
            display_ordinal: DisplayOrdinal::new(3),
            state: TaskState::Running,
            has_controller_task_state_event: false,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
        model.insert_task_run(TaskRun {
            run_id: dependency_id,
            key: RunKey::Controller("controller-run".to_owned()),
            display_ordinal: DisplayOrdinal::new(4),
            state: TaskState::Queued,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
        model.insert_execution(Execution {
            execution_id: "execution-1".to_owned(),
            pane_id: "pane-1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            task_run_id: run_id,
            state: ExecState::Working,
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "agent-1".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("session-1".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(5),
            parent_agent_node_id: None,
            state: None,
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        });
        let execution_edge = ExecutionEdge {
            parent_run_id: dependency_id,
            child_run_id: run_id,
        };
        let dependency_edge = DependencyEdge {
            prerequisite_run_id: dependency_id,
            dependent_run_id: run_id,
        };
        model.insert_execution_edge(execution_edge.clone());
        model.insert_dependency_edge(dependency_edge.clone());

        assert_eq!(
            model.workspace("workspace-1").unwrap().workspace_id,
            "workspace-1"
        );
        assert_eq!(model.tab("tab-1").unwrap().workspace_id, "workspace-1");
        assert_eq!(model.pane("pane-1").unwrap().terminal_id, "terminal-1");
        assert_eq!(model.task_run(&run_id).unwrap().key, run_key);
        assert_eq!(model.task_run_by_key(&run_key).unwrap().run_id, run_id);
        assert_eq!(model.execution("execution-1").unwrap().task_run_id, run_id);
        assert_eq!(model.agent_node("agent-1").unwrap().task_run_id, run_id);
        assert!(model.execution_edges().any(|edge| edge == &execution_edge));
        assert!(
            model
                .dependency_edges()
                .any(|edge| edge == &dependency_edge)
        );
        assert_eq!(model.workspaces().count(), 1);
        assert_eq!(model.tabs().count(), 1);
        assert_eq!(model.panes().count(), 1);
        assert_eq!(model.task_runs().count(), 2);
        assert_eq!(model.executions().count(), 1);
        assert_eq!(model.agent_nodes().count(), 1);
    }

    #[test]
    fn controller_diagnostics_saturation_handle_is_shared() {
        let model = DomainModel::default();
        let cloned = model.clone();
        let acceptor = model.controller_diagnostics().acceptor_handle();

        acceptor.record_socket_saturation();

        assert_eq!(model.controller_diagnostics().socket_saturations(), 1);
        assert_eq!(cloned.controller_diagnostics().socket_saturations(), 1);
    }

    #[test]
    fn controller_diagnostics_accept_failure_handle_is_shared() {
        let model = DomainModel::default();
        let acceptor = model.controller_diagnostics().acceptor_handle();

        acceptor.record_accept_failure();

        assert_eq!(acceptor.accept_failures(), 1);
        assert_eq!(model.controller_diagnostics().accept_failures(), 1);
    }

    #[test]
    fn topology_snapshot_serde_roundtrip() {
        let snapshot = TopologySnapshot {
            workspaces: vec![Workspace {
                workspace_id: "workspace-1".to_owned(),
            }],
            tabs: vec![Tab {
                tab_id: "tab-1".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                label: None,
            }],
            panes: vec![PaneSnapshot {
                pane_id: "pane-1".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                tab_id: "tab-1".to_owned(),
                terminal_id: "terminal-1".to_owned(),
                display_name: Some("Build".to_owned()),
                agent: Some(SnapshotAgent {
                    agent_name: "codex".to_owned(),
                    state: ExecState::Working,
                }),
                agent_session: Some(AgentSessionReference {
                    source: "herdr".to_owned(),
                    agent: "codex".to_owned(),
                    kind: AgentSessionReferenceKind::Id,
                    value: "session-1".to_owned(),
                }),
            }],
        };
        let encoded = serde_json::to_string(&snapshot).unwrap();

        assert_eq!(
            serde_json::from_str::<TopologySnapshot>(&encoded).unwrap(),
            snapshot
        );
    }

    #[test]
    fn pane_snapshot_display_name_defaults_to_none_when_absent() {
        let pane = serde_json::from_value::<PaneSnapshot>(serde_json::json!({
            "pane_id": "pane-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "terminal_id": "terminal-1",
            "agent": null,
            "agent_session": null,
        }))
        .unwrap();

        assert_eq!(pane.display_name, None);
    }

    #[test]
    fn progress_quantization_boundaries() {
        assert_eq!(quantize_progress(0.0), Ok(0));
        assert_eq!(quantize_progress(1.0), Ok(10_000));
        assert_eq!(quantize_progress(0.00005), Ok(1));
        assert_eq!(quantize_progress(0.99995), Ok(10_000));
        assert_eq!(quantize_progress(-0.000_001), Err(ProgressOutOfRange));
        assert_eq!(quantize_progress(1.000_001), Err(ProgressOutOfRange));
    }

    #[test]
    fn controller_text_escape_and_utf8_truncation_are_safe() {
        let value = format!("line\n{}tail", "雪".repeat(100));
        let sanitized = sanitize_controller_text(&value);

        assert!(sanitized.starts_with("line\\n"));
        assert!(sanitized.len() <= 256);
        assert!(std::str::from_utf8(sanitized.as_bytes()).is_ok());
    }

    #[test]
    fn controller_text_escape_is_atomic_at_byte_limit() {
        let truncated = sanitize_controller_text(&("a".repeat(255) + "\n"));
        assert_eq!(truncated, "a".repeat(255));

        let exact = sanitize_controller_text(&("a".repeat(254) + "\n"));
        assert_eq!(exact.len(), 256);
        assert!(exact.ends_with("\\n"));
    }

    #[test]
    fn controller_event_variant_roundtrips() {
        let metadata = EventMetadata {
            event_id: "controller-event".to_owned(),
            timestamp_ms: 11,
            receipt_time_ms: 22,
            source: "orchestrator".to_owned(),
            source_event_type: "depends_on".to_owned(),
            herdr_session: "session".to_owned(),
            workspace_id: None,
            tab_id: None,
            pane_id: None,
            terminal_id: None,
            provider: None,
            native_session_id: None,
            task_run_id: Some(RunId::new()),
            agent_node_id: None,
            task_state: None,
            execution_parent: None,
            dependency: None,
            source_coverage: Vec::new(),
            provider_metadata: None,
            label: Some("label".to_owned()),
            reason: None,
            progress: Some(5_000),
            ingest_seq: Some(7),
        };
        let event = NormalizedEvent::ControllerEvent {
            metadata,
            event: ControllerEventKind::DependsOn {
                depends_on_id: "raw prerequisite".to_owned(),
            },
        };
        let encoded = serde_json::to_string(&event).unwrap();

        assert_eq!(
            serde_json::from_str::<NormalizedEvent>(&encoded).unwrap(),
            event
        );
    }
}
