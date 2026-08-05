//! T7 reducer state machines, ordinal allocator, and gap reconciliation.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::watch;

use crate::identity::{BindingEvidence, MergeConflict, apply_binding_plan, plan_binding};
use crate::model::{
    AgentNode, AgentSessionReferenceKind, DisplayOrdinal, DomainModel, EventMetadata, ExecState,
    Execution, NormalizedEvent, Pane, Provider, ReconcileBatch, RunId, RunKey, SharedModel,
    TaskRun, TaskState, TopologyEntity, TopologyEntityId,
};
use crate::store::{
    NativeSessionBinding, PersistBatch, PersistExecution, PersistOp, PersistTaskRun, RestoredState,
};

const STALE_GRACE_MS: i64 = 30_000;

/// Errors that reject a reducer transition before any model or persistence mutation escapes.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ReducerError {
    /// No immutable display ordinal remains for a newly observed Task Run.
    #[error("display ordinal allocator is exhausted")]
    OrdinalExhausted,
    /// Identity evidence conflicted with the current binding graph.
    #[error("identity binding conflict: {0}")]
    BindingConflict(#[from] MergeConflict),
}

/// Serialized owner of domain transitions and display-ordinal allocation.
pub struct Reducer {
    model: DomainModel,
    next_ordinal: i64,
    publisher: watch::Sender<Arc<DomainModel>>,
}

impl Reducer {
    /// Restores reducer state and returns a receiver for coherent model snapshots.
    #[must_use]
    pub fn new(restored: RestoredState) -> (Self, SharedModel) {
        let (publisher, shared) = watch::channel(Arc::new(restored.model.clone()));
        (
            Self {
                model: restored.model,
                next_ordinal: restored.next_ordinal,
                publisher,
            },
            shared,
        )
    }

    /// Applies one normalized event and publishes exactly one resulting snapshot.
    pub fn apply(&mut self, event: NormalizedEvent) -> Result<PersistBatch, ReducerError> {
        self.apply_observation(vec![event])
    }

    /// Applies one source observation and publishes exactly one resulting snapshot.
    pub fn apply_observation(
        &mut self,
        events: Vec<NormalizedEvent>,
    ) -> Result<PersistBatch, ReducerError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let original_model = self.model.clone();
        let original_next_ordinal = self.next_ordinal;
        let mut persist = Vec::new();
        for event in events {
            match self.apply_inner(event) {
                Ok(event_persist) => persist.extend(event_persist),
                Err(error) => {
                    self.model = original_model;
                    self.next_ordinal = original_next_ordinal;
                    return Err(error);
                }
            }
        }
        self.publish();
        Ok(persist)
    }

    fn apply_inner(&mut self, event: NormalizedEvent) -> Result<PersistBatch, ReducerError> {
        let metadata = event_metadata(&event).clone();
        let mut persist = Vec::new();

        self.ensure_event_runs(&event, &metadata, &mut persist)?;
        self.apply_controller_metadata(&metadata, &mut persist);
        self.apply_event_body(&event, &metadata, &mut persist);
        self.apply_identity_metadata(&event, &metadata, &mut persist)?;
        self.persist_event_execution(&event, metadata.timestamp_ms, &mut persist);
        persist.push(PersistOp::RecordEvent {
            event: Box::new(event),
            seen_at_ms: metadata.timestamp_ms,
        });

        Ok(persist)
    }

    /// Replaces physical topology across an observation gap in one coherent batch.
    pub fn reconcile_gap(&mut self, batch: ReconcileBatch) -> Result<PersistBatch, ReducerError> {
        let original_model = self.model.clone();
        let original_next_ordinal = self.next_ordinal;
        match self.reconcile_gap_inner(batch) {
            Ok(persist) => Ok(persist),
            Err(error) => {
                self.model = original_model;
                self.next_ordinal = original_next_ordinal;
                Err(error)
            }
        }
    }

    fn reconcile_gap_inner(&mut self, batch: ReconcileBatch) -> Result<PersistBatch, ReducerError> {
        let ReconcileBatch {
            topology,
            gap_kind: _,
        } = batch;
        let now_ms = unix_now_ms();
        let mut persist = Vec::new();
        let mut pre_gap_executions: Vec<_> = self.model.executions().cloned().collect();
        pre_gap_executions.sort_by(|left, right| left.execution_id.cmp(&right.execution_id));
        let mut pre_gap_runs: Vec<_> = pre_gap_executions
            .iter()
            .map(|execution| execution.task_run_id)
            .collect();
        pre_gap_runs.sort_unstable();
        pre_gap_runs.dedup();

        for mut execution in pre_gap_executions {
            execution.state = ExecState::Ended;
            self.model.insert_execution(execution.clone());
            persist.push(persist_execution(execution, now_ms));
        }

        self.replace_topology(&topology, &mut persist);

        for pane in topology.panes {
            let Some(agent) = pane.agent else {
                continue;
            };
            let provider = snapshot_provider(
                &agent.agent_name,
                pane.agent_session
                    .as_ref()
                    .map(|reference| reference.agent.as_str()),
            );
            let native_sid = pane.agent_session.as_ref().and_then(|reference| {
                (reference.kind == AgentSessionReferenceKind::Id && !reference.value.is_empty())
                    .then(|| reference.value.clone())
            });
            let existing_run = match (provider, native_sid.as_deref()) {
                (Some(provider), Some(sid)) => self.run_for_native_session(provider, sid),
                _ => None,
            };
            let run_id = match existing_run {
                Some(run_id) => run_id,
                None => self.insert_snapshot_run(
                    provider,
                    native_sid.as_deref(),
                    pane.agent_session.as_ref().and_then(|reference| {
                        (reference.kind == AgentSessionReferenceKind::Path
                            && !reference.value.is_empty())
                        .then_some(reference.value.as_str())
                    }),
                    &pane.terminal_id,
                    now_ms,
                    &mut persist,
                )?,
            };
            let token = RunId::new().to_string();
            let execution_id = format!("gap-execution-{token}");
            let execution = Execution {
                execution_id: execution_id.clone(),
                pane_id: pane.pane_id,
                terminal_id: pane.terminal_id,
                task_run_id: run_id,
                state: agent.state,
            };
            self.model.insert_execution(execution.clone());
            persist.push(persist_execution(execution.clone(), now_ms));
            if !execution.state.is_terminal() {
                self.activate_for_live_execution(run_id, now_ms, &mut persist);
            }

            if let Some(provider) = provider {
                let existing_node_id = native_sid
                    .as_deref()
                    .filter(|sid| !sid.is_empty())
                    .and_then(|sid| {
                        self.model
                            .agent_nodes()
                            .filter(|node| {
                                node.task_run_id == run_id
                                    && node.provider == provider
                                    && node.native_session_id.as_deref() == Some(sid)
                            })
                            .map(|node| node.agent_node_id.clone())
                            .min()
                    });
                let agent_node = AgentNode {
                    agent_node_id: existing_node_id.unwrap_or_else(|| format!("gap-agent-{token}")),
                    provider,
                    native_session_id: native_sid.clone(),
                    task_run_id: run_id,
                };
                self.model.insert_agent_node(agent_node.clone());
                persist.push(PersistOp::UpsertAgentNode(agent_node));
            }
        }

        for run_id in pre_gap_runs {
            self.close_run_without_live_execution(run_id, now_ms, &mut persist);
        }

        self.publish();
        Ok(persist)
    }

    fn ensure_event_runs(
        &mut self,
        event: &NormalizedEvent,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        if let NormalizedEvent::ExecutionBegin { execution, .. } = event {
            self.ensure_execution_run(execution, metadata, persist)?;
        }

        if let Some(run_id) = metadata.task_run_id {
            let controller_reference = metadata.task_state.is_some()
                || metadata.execution_parent.is_some()
                || metadata.dependency.is_some();
            let initial_state = match metadata.task_state {
                Some(state) => initial_controller_state(&metadata.source_event_type, state),
                None if controller_reference => TaskState::Queued,
                None => TaskState::Running,
            };
            self.ensure_metadata_run(
                run_id,
                metadata,
                controller_reference,
                initial_state,
                persist,
            )?;
        }

        if let Some(edge) = &metadata.execution_parent {
            self.ensure_controller_placeholder(edge.parent_run_id, metadata.timestamp_ms, persist)?;
            self.ensure_controller_placeholder(edge.child_run_id, metadata.timestamp_ms, persist)?;
        }
        if let Some(edge) = &metadata.dependency {
            self.ensure_controller_placeholder(
                edge.prerequisite_run_id,
                metadata.timestamp_ms,
                persist,
            )?;
            self.ensure_controller_placeholder(
                edge.dependent_run_id,
                metadata.timestamp_ms,
                persist,
            )?;
        }
        Ok(())
    }

    fn ensure_execution_run(
        &mut self,
        execution: &Execution,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        if self.model.task_run(&execution.task_run_id).is_some() {
            return Ok(());
        }
        let ordinal = self.allocate_ordinal()?;
        let native_key = metadata
            .provider
            .zip(metadata.native_session_id.as_deref())
            .filter(|(_, sid)| !sid.is_empty())
            .map(|(provider, sid)| RunKey::Native {
                provider,
                sid: sid.to_owned(),
            });
        let key = match native_key {
            Some(key) if self.model.task_run_by_key(&key).is_none() => key,
            _ => provisional_key(&execution.terminal_id, metadata.timestamp_ms, ordinal),
        };
        let task_run = TaskRun {
            run_id: execution.task_run_id,
            key,
            display_ordinal: ordinal,
            state: TaskState::Running,
            has_controller_task_state_event: false,
        };
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, metadata.timestamp_ms));
        Ok(())
    }

    fn ensure_metadata_run(
        &mut self,
        run_id: RunId,
        metadata: &EventMetadata,
        controller_reference: bool,
        initial_state: TaskState,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        if self.model.task_run(&run_id).is_some() {
            return Ok(());
        }
        let ordinal = self.allocate_ordinal()?;
        let native_key = metadata
            .provider
            .zip(metadata.native_session_id.as_deref())
            .filter(|(_, sid)| !sid.is_empty())
            .map(|(provider, sid)| RunKey::Native {
                provider,
                sid: sid.to_owned(),
            });
        let key = if controller_reference {
            RunKey::Controller(run_id.to_string())
        } else {
            match native_key {
                Some(key) if self.model.task_run_by_key(&key).is_none() => key,
                _ => provisional_key(
                    metadata
                        .terminal_id
                        .as_deref()
                        .map_or("unknown-terminal", |terminal_id| terminal_id),
                    metadata.timestamp_ms,
                    ordinal,
                ),
            }
        };
        let task_run = TaskRun {
            run_id,
            key,
            display_ordinal: ordinal,
            state: initial_state,
            has_controller_task_state_event: metadata.task_state.is_some(),
        };
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, metadata.timestamp_ms));
        Ok(())
    }

    fn ensure_controller_placeholder(
        &mut self,
        run_id: RunId,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        if self.model.task_run(&run_id).is_some() {
            return Ok(());
        }
        let ordinal = self.allocate_ordinal()?;
        let task_run = TaskRun {
            run_id,
            key: RunKey::Controller(run_id.to_string()),
            display_ordinal: ordinal,
            state: TaskState::Queued,
            has_controller_task_state_event: false,
        };
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
        Ok(())
    }

    fn apply_controller_metadata(&mut self, metadata: &EventMetadata, persist: &mut PersistBatch) {
        if let (Some(run_id), Some(target)) = (metadata.task_run_id, metadata.task_state)
            && let Some(mut task_run) = self.model.task_run(&run_id).cloned()
        {
            task_run.has_controller_task_state_event = true;
            task_run.state =
                controller_task_transition(task_run.state, &metadata.source_event_type, target);
            self.model.insert_task_run(task_run.clone());
            persist.push(self.persist_task_run(task_run, metadata.timestamp_ms));
        }

        if let Some(edge) = &metadata.execution_parent
            && self.model.insert_execution_edge(edge.clone())
        {
            persist.push(PersistOp::UpsertExecutionEdge {
                edge: edge.clone(),
                created_at_ms: metadata.timestamp_ms,
            });
        }
        if let Some(edge) = &metadata.dependency
            && self.model.insert_dependency_edge(edge.clone())
        {
            persist.push(PersistOp::UpsertDependencyEdge {
                edge: edge.clone(),
                created_at_ms: metadata.timestamp_ms,
            });
        }
    }

    fn apply_event_body(
        &mut self,
        event: &NormalizedEvent,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) {
        match event {
            NormalizedEvent::TopologyUpsert { entity, .. } => match entity {
                TopologyEntity::Workspace(workspace) => {
                    self.model.insert_workspace(workspace.clone());
                    persist.push(PersistOp::UpsertWorkspace(workspace.clone()));
                }
                TopologyEntity::Tab(tab) => {
                    self.model.insert_tab(tab.clone());
                    persist.push(PersistOp::UpsertTab(tab.clone()));
                }
                TopologyEntity::Pane(pane) => {
                    self.model.insert_pane(pane.clone());
                    persist.push(PersistOp::UpsertPane(pane.clone()));
                }
            },
            NormalizedEvent::TopologyClosure { entity, .. } => {
                self.apply_topology_closure(entity, metadata.timestamp_ms, persist);
            }
            NormalizedEvent::AgentStatusChanged {
                execution_id,
                state,
                ..
            } => {
                self.apply_execution_state(execution_id, state, metadata, persist);
            }
            NormalizedEvent::ExecutionBegin { execution, .. } => {
                self.model.insert_execution(execution.clone());
            }
            NormalizedEvent::ExecutionEnd { execution_id, .. } => {
                self.end_execution(execution_id, metadata.timestamp_ms, persist);
            }
        }
    }

    fn apply_topology_closure(
        &mut self,
        entity: &TopologyEntityId,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) {
        match entity {
            TopologyEntityId::Workspace { workspace_id } => {
                let tab_ids: Vec<_> = self
                    .model
                    .tabs()
                    .filter(|tab| tab.workspace_id == *workspace_id)
                    .map(|tab| tab.tab_id.clone())
                    .collect();
                for tab_id in tab_ids {
                    self.close_tab(&tab_id, timestamp_ms, persist);
                }
                if self.model.remove_workspace(workspace_id).is_some() {
                    persist.push(PersistOp::DeleteWorkspace {
                        workspace_id: workspace_id.clone(),
                    });
                }
            }
            TopologyEntityId::Tab { tab_id } => {
                self.close_tab(tab_id, timestamp_ms, persist);
            }
            TopologyEntityId::Pane { pane_id } => {
                self.close_pane(pane_id, timestamp_ms, persist);
            }
        }
    }

    fn close_tab(&mut self, tab_id: &str, timestamp_ms: i64, persist: &mut PersistBatch) {
        let pane_ids: Vec<_> = self
            .model
            .panes()
            .filter(|pane| pane.tab_id == tab_id)
            .map(|pane| pane.pane_id.clone())
            .collect();
        for pane_id in pane_ids {
            self.close_pane(&pane_id, timestamp_ms, persist);
        }
        if self.model.remove_tab(tab_id).is_some() {
            persist.push(PersistOp::DeleteTab {
                tab_id: tab_id.to_owned(),
            });
        }
    }

    fn close_pane(&mut self, pane_id: &str, timestamp_ms: i64, persist: &mut PersistBatch) {
        let execution_ids: Vec<_> = self
            .model
            .executions()
            .filter(|execution| execution.pane_id == pane_id && !execution.state.is_terminal())
            .map(|execution| execution.execution_id.clone())
            .collect();
        for execution_id in execution_ids {
            self.end_execution(&execution_id, timestamp_ms, persist);
        }
        if self.model.remove_pane(pane_id).is_some() {
            persist.push(PersistOp::DeletePane {
                pane_id: pane_id.to_owned(),
            });
        }
    }

    fn apply_execution_state(
        &mut self,
        execution_id: &str,
        observed: &ExecState,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) {
        let Some(mut execution) = self.model.execution(execution_id).cloned() else {
            return;
        };
        execution.state = next_execution_state(&execution.state, observed, metadata);
        let run_id = execution.task_run_id;
        let ended = execution.state.is_terminal();
        self.model.insert_execution(execution);
        if ended {
            self.close_run_without_live_execution(run_id, metadata.timestamp_ms, persist);
        }
    }

    fn end_execution(&mut self, execution_id: &str, timestamp_ms: i64, persist: &mut PersistBatch) {
        let Some(mut execution) = self.model.execution(execution_id).cloned() else {
            return;
        };
        execution.state = ExecState::Ended;
        let run_id = execution.task_run_id;
        self.model.insert_execution(execution.clone());
        persist.push(persist_execution(execution, timestamp_ms));
        self.close_run_without_live_execution(run_id, timestamp_ms, persist);
    }

    fn apply_identity_metadata(
        &mut self,
        event: &NormalizedEvent,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        let event_run = match event {
            NormalizedEvent::ExecutionBegin { execution, .. } => Some(execution.task_run_id),
            NormalizedEvent::AgentStatusChanged { execution_id, .. }
            | NormalizedEvent::ExecutionEnd { execution_id, .. } => self
                .model
                .execution(execution_id)
                .map(|execution| execution.task_run_id),
            NormalizedEvent::TopologyUpsert { .. } | NormalizedEvent::TopologyClosure { .. } => {
                None
            }
        };
        let run_id = metadata.task_run_id.or(event_run);
        let Some(run_id) = run_id else {
            return Ok(());
        };
        let Some(task_run) = self.model.task_run(&run_id) else {
            return Ok(());
        };

        if let Some((provider, sid)) = metadata
            .provider
            .zip(metadata.native_session_id.as_deref())
            .filter(|(_, sid)| !sid.is_empty())
        {
            let evidence = if matches!(task_run.key, RunKey::Controller(_)) {
                BindingEvidence::ControllerNativeSession {
                    controller_run: run_id,
                    provider,
                    sid: sid.to_owned(),
                }
            } else {
                BindingEvidence::NativeSession {
                    run: run_id,
                    provider,
                    sid: sid.to_owned(),
                }
            };
            self.apply_binding(evidence, persist)?;
        }

        if matches!(
            self.model.task_run(&run_id).map(|run| &run.key),
            Some(RunKey::Controller(_))
        ) && let Some(terminal_id) = metadata.terminal_id.as_deref()
            && !terminal_id.is_empty()
        {
            self.apply_binding(
                BindingEvidence::ControllerTerminal {
                    controller_run: run_id,
                    terminal_id: terminal_id.to_owned(),
                },
                persist,
            )?;
        }

        if let NormalizedEvent::ExecutionBegin { execution, .. } = event
            && let Some(current) = self.model.execution(&execution.execution_id)
            && !current.state.is_terminal()
        {
            self.activate_for_live_execution(current.task_run_id, metadata.timestamp_ms, persist);
        }
        Ok(())
    }

    fn apply_binding(
        &mut self,
        evidence: BindingEvidence,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        let plan = plan_binding(&self.model, &evidence);
        persist.extend(apply_binding_plan(&mut self.model, plan)?);
        Ok(())
    }

    fn persist_event_execution(
        &self,
        event: &NormalizedEvent,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) {
        let execution_id = match event {
            NormalizedEvent::AgentStatusChanged { execution_id, .. } => Some(execution_id.as_str()),
            NormalizedEvent::ExecutionEnd { .. } => None,
            NormalizedEvent::ExecutionBegin { execution, .. } => {
                Some(execution.execution_id.as_str())
            }
            NormalizedEvent::TopologyUpsert { .. } | NormalizedEvent::TopologyClosure { .. } => {
                None
            }
        };
        if let Some(execution_id) = execution_id
            && let Some(execution) = self.model.execution(execution_id)
        {
            persist.push(persist_execution(execution.clone(), timestamp_ms));
        }
    }

    fn close_run_without_live_execution(
        &mut self,
        run_id: RunId,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) {
        if self
            .model
            .executions()
            .any(|execution| execution.task_run_id == run_id && !execution.state.is_terminal())
        {
            return;
        }
        let Some(mut task_run) = self.model.task_run(&run_id).cloned() else {
            return;
        };
        if task_run.has_controller_task_state_event
            || matches!(
                task_run.state,
                TaskState::Completed
                    | TaskState::Failed
                    | TaskState::Cancelled
                    | TaskState::EndedUnknown
            )
        {
            return;
        }
        task_run.state = TaskState::EndedUnknown;
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
    }

    fn activate_for_live_execution(
        &mut self,
        run_id: RunId,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) {
        let Some(mut task_run) = self.model.task_run(&run_id).cloned() else {
            return;
        };
        let should_activate = task_run.state == TaskState::EndedUnknown
            || (task_run.state == TaskState::Queued && !task_run.has_controller_task_state_event);
        if !should_activate {
            return;
        }
        task_run.state = TaskState::Running;
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
    }

    fn replace_topology(
        &mut self,
        topology: &crate::model::TopologySnapshot,
        persist: &mut PersistBatch,
    ) {
        let workspace_ids: Vec<_> = self
            .model
            .workspaces()
            .map(|workspace| workspace.workspace_id.clone())
            .collect();
        let tab_ids: Vec<_> = self.model.tabs().map(|tab| tab.tab_id.clone()).collect();
        let pane_ids: Vec<_> = self
            .model
            .panes()
            .map(|pane| pane.pane_id.clone())
            .collect();
        for pane_id in pane_ids {
            self.close_pane(&pane_id, unix_now_ms(), persist);
        }
        for tab_id in tab_ids {
            if self.model.remove_tab(&tab_id).is_some() {
                persist.push(PersistOp::DeleteTab { tab_id });
            }
        }
        for workspace_id in workspace_ids {
            if self.model.remove_workspace(&workspace_id).is_some() {
                persist.push(PersistOp::DeleteWorkspace { workspace_id });
            }
        }

        for workspace in &topology.workspaces {
            self.model.insert_workspace(workspace.clone());
            persist.push(PersistOp::UpsertWorkspace(workspace.clone()));
        }
        for tab in &topology.tabs {
            self.model.insert_tab(tab.clone());
            persist.push(PersistOp::UpsertTab(tab.clone()));
        }
        for pane in &topology.panes {
            let pane = Pane {
                pane_id: pane.pane_id.clone(),
                workspace_id: pane.workspace_id.clone(),
                tab_id: pane.tab_id.clone(),
                terminal_id: pane.terminal_id.clone(),
            };
            self.model.insert_pane(pane.clone());
            persist.push(PersistOp::UpsertPane(pane));
        }
    }

    fn insert_snapshot_run(
        &mut self,
        provider: Option<Provider>,
        native_sid: Option<&str>,
        native_path: Option<&str>,
        terminal_id: &str,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) -> Result<RunId, ReducerError> {
        let run_id = RunId::new();
        let ordinal = self.allocate_ordinal()?;
        let key = match (provider, native_sid, native_path) {
            (Some(provider), Some(sid), _) => RunKey::Native {
                provider,
                sid: sid.to_owned(),
            },
            (Some(provider), None, Some(path)) => RunKey::NativePath {
                provider,
                path: path.to_owned(),
            },
            _ => provisional_key(terminal_id, timestamp_ms, ordinal),
        };
        let task_run = TaskRun {
            run_id,
            key,
            display_ordinal: ordinal,
            state: TaskState::Running,
            has_controller_task_state_event: false,
        };
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
        Ok(run_id)
    }

    fn run_for_native_session(&self, provider: Provider, sid: &str) -> Option<RunId> {
        let key = RunKey::Native {
            provider,
            sid: sid.to_owned(),
        };
        if let Some(task_run) = self.model.task_run_by_key(&key) {
            return Some(task_run.run_id);
        }

        let mut runs = self
            .model
            .agent_nodes()
            .filter(|node| {
                node.provider == provider
                    && node.native_session_id.as_deref() == Some(sid)
                    && self.model.task_run(&node.task_run_id).is_some_and(|run| {
                        matches!(run.key, RunKey::Controller(_) | RunKey::Native { .. })
                    })
            })
            .map(|node| node.task_run_id);
        let first = runs.next()?;
        runs.all(|run_id| run_id == first).then_some(first)
    }

    fn persist_task_run(&self, task_run: TaskRun, timestamp_ms: i64) -> PersistOp {
        let native_session = native_binding(&self.model, task_run.run_id);
        let finished_at_ms = task_run.state.is_terminal().then_some(timestamp_ms);
        PersistOp::UpsertTaskRun(PersistTaskRun {
            task_run,
            native_session,
            created_at_ms: timestamp_ms,
            updated_at_ms: timestamp_ms,
            finished_at_ms,
        })
    }

    fn allocate_ordinal(&mut self) -> Result<DisplayOrdinal, ReducerError> {
        let ordinal = DisplayOrdinal::new(self.next_ordinal);
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or(ReducerError::OrdinalExhausted)?;
        Ok(ordinal)
    }

    /// Ends executions whose live-observation stale grace has expired.
    pub fn sweep_stale(&mut self, now_ms: i64) -> PersistBatch {
        let execution_ids: Vec<_> = self
            .model
            .executions()
            .filter_map(|execution| match execution.state {
                ExecState::Stale { since_ms }
                    if now_ms.saturating_sub(since_ms) >= STALE_GRACE_MS =>
                {
                    Some(execution.execution_id.clone())
                }
                _ => None,
            })
            .collect();
        if execution_ids.is_empty() {
            return Vec::new();
        }
        let mut persist = Vec::new();
        for execution_id in execution_ids {
            self.end_execution(&execution_id, now_ms, &mut persist);
        }
        self.publish();
        persist
    }

    fn publish(&self) {
        self.publisher.send_replace(Arc::new(self.model.clone()));
    }
}

fn event_metadata(event: &NormalizedEvent) -> &EventMetadata {
    match event {
        NormalizedEvent::TopologyUpsert { metadata, .. }
        | NormalizedEvent::TopologyClosure { metadata, .. }
        | NormalizedEvent::AgentStatusChanged { metadata, .. }
        | NormalizedEvent::ExecutionBegin { metadata, .. }
        | NormalizedEvent::ExecutionEnd { metadata, .. } => metadata,
    }
}

fn initial_controller_state(source_event_type: &str, supplied: TaskState) -> TaskState {
    match controller_event_kind(source_event_type) {
        ControllerEventKind::Started => TaskState::Running,
        ControllerEventKind::Blocked => TaskState::Blocked,
        ControllerEventKind::Progress => TaskState::Queued,
        ControllerEventKind::Complete => TaskState::Completed,
        ControllerEventKind::Failed => TaskState::Failed,
        ControllerEventKind::Cancelled => TaskState::Cancelled,
        ControllerEventKind::Other => supplied,
    }
}

fn controller_task_transition(
    current: TaskState,
    source_event_type: &str,
    supplied: TaskState,
) -> TaskState {
    match controller_event_kind(source_event_type) {
        ControllerEventKind::Started => match current {
            TaskState::Queued | TaskState::Blocked | TaskState::EndedUnknown => TaskState::Running,
            _ => current,
        },
        ControllerEventKind::Blocked => match current {
            TaskState::Queued | TaskState::Running | TaskState::EndedUnknown => TaskState::Blocked,
            _ => current,
        },
        ControllerEventKind::Progress => {
            if current == TaskState::EndedUnknown {
                TaskState::Running
            } else {
                current
            }
        }
        ControllerEventKind::Complete => terminal_transition(current, TaskState::Completed),
        ControllerEventKind::Failed => terminal_transition(current, TaskState::Failed),
        ControllerEventKind::Cancelled => terminal_transition(current, TaskState::Cancelled),
        ControllerEventKind::Other => match supplied {
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled => {
                terminal_transition(current, supplied)
            }
            TaskState::Running => match current {
                TaskState::Queued | TaskState::Blocked | TaskState::EndedUnknown => {
                    TaskState::Running
                }
                _ => current,
            },
            TaskState::Blocked => match current {
                TaskState::Queued | TaskState::Running | TaskState::EndedUnknown => {
                    TaskState::Blocked
                }
                _ => current,
            },
            TaskState::Queued => {
                if current == TaskState::EndedUnknown {
                    TaskState::Running
                } else {
                    current
                }
            }
            TaskState::EndedUnknown => current,
        },
    }
}

fn terminal_transition(current: TaskState, target: TaskState) -> TaskState {
    match current {
        TaskState::Completed | TaskState::Failed | TaskState::Cancelled if current != target => {
            current
        }
        _ => target,
    }
}

#[derive(Clone, Copy)]
enum ControllerEventKind {
    Started,
    Blocked,
    Progress,
    Complete,
    Failed,
    Cancelled,
    Other,
}

fn controller_event_kind(source_event_type: &str) -> ControllerEventKind {
    match source_event_type {
        "task_started" => ControllerEventKind::Started,
        "blocked" => ControllerEventKind::Blocked,
        "progress" => ControllerEventKind::Progress,
        "complete" => ControllerEventKind::Complete,
        "failed" => ControllerEventKind::Failed,
        "cancelled" => ControllerEventKind::Cancelled,
        _ => ControllerEventKind::Other,
    }
}

fn next_execution_state(
    current: &ExecState,
    observed: &ExecState,
    metadata: &EventMetadata,
) -> ExecState {
    if metadata.source == "herdr" && metadata.source_event_type == "done" {
        return if current.is_terminal() {
            ExecState::Ended
        } else {
            ExecState::Idle
        };
    }
    match observed {
        ExecState::Stale { .. } => match current {
            ExecState::Ended => ExecState::Ended,
            ExecState::Stale { since_ms }
                if metadata.timestamp_ms.saturating_sub(*since_ms) >= STALE_GRACE_MS =>
            {
                ExecState::Ended
            }
            ExecState::Stale { since_ms } => ExecState::Stale {
                since_ms: *since_ms,
            },
            _ => ExecState::Stale {
                since_ms: metadata.timestamp_ms,
            },
        },
        ExecState::Ended => ExecState::Ended,
        _state if current.is_terminal() => ExecState::Ended,
        state => state.clone(),
    }
}

fn provisional_key(terminal_id: &str, timestamp_ms: i64, ordinal: DisplayOrdinal) -> RunKey {
    let seq = u64::try_from(ordinal.get()).map_or(0, |value| value);
    RunKey::Provisional {
        terminal_id: terminal_id.to_owned(),
        start_ms: timestamp_ms,
        seq,
    }
}

fn native_binding(model: &DomainModel, run_id: RunId) -> Option<NativeSessionBinding> {
    let bound = model
        .task_run_bindings()
        .filter_map(|(key, owner)| {
            if *owner != run_id {
                return None;
            }
            match key {
                RunKey::Native { provider, sid } => Some(NativeSessionBinding {
                    provider: *provider,
                    native_session_id: sid.clone(),
                }),
                _ => None,
            }
        })
        .next();
    if bound.is_some() {
        return bound;
    }

    let mut bindings = model
        .agent_nodes()
        .filter(|node| node.task_run_id == run_id)
        .filter_map(|node| {
            node.native_session_id
                .as_ref()
                .map(|sid| NativeSessionBinding {
                    provider: node.provider,
                    native_session_id: sid.clone(),
                })
        });
    let first = bindings.next()?;
    bindings.all(|binding| binding == first).then_some(first)
}

fn persist_execution(execution: Execution, timestamp_ms: i64) -> PersistOp {
    let ended_at_ms = execution.state.is_terminal().then_some(timestamp_ms);
    PersistOp::UpsertExecution(PersistExecution {
        execution,
        started_at_ms: timestamp_ms,
        updated_at_ms: timestamp_ms,
        ended_at_ms,
    })
}

fn snapshot_provider(agent_name: &str, session_agent: Option<&str>) -> Option<Provider> {
    session_agent
        .and_then(provider_from_name)
        .or_else(|| provider_from_name(agent_name))
}

fn provider_from_name(name: &str) -> Option<Provider> {
    let normalized = name.to_ascii_lowercase();
    if normalized.contains("codex") {
        Some(Provider::Codex)
    } else if normalized.contains("claude") {
        Some(Provider::Claude)
    } else {
        None
    }
}

fn unix_now_ms() -> i64 {
    let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    elapsed.as_millis().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::model::{
        AgentNode, AgentSessionReference, AgentSessionReferenceKind, DependencyEdge,
        DisplayOrdinal, DomainModel, EventMetadata, ExecState, Execution, ExecutionEdge, GapKind,
        NormalizedEvent, PaneSnapshot, Provider, ReconcileBatch, RunId, RunKey, SnapshotAgent,
        TaskRun, TaskState, TopologyEntity, TopologySnapshot, Workspace,
    };
    use crate::store::{PersistOp, RestoredState};

    use super::{Reducer, ReducerError};

    fn metadata(event_id: &str, timestamp_ms: i64) -> EventMetadata {
        EventMetadata {
            event_id: event_id.to_owned(),
            timestamp_ms,
            source: "herdr".to_owned(),
            source_event_type: "agent_status_changed".to_owned(),
            herdr_session: "session".to_owned(),
            workspace_id: None,
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
        }
    }

    fn run(run_id: RunId, key: RunKey, ordinal: i64, state: TaskState) -> TaskRun {
        TaskRun {
            run_id,
            key,
            display_ordinal: DisplayOrdinal::new(ordinal),
            state,
            has_controller_task_state_event: false,
        }
    }

    fn execution(run_id: RunId, execution_id: &str, state: ExecState) -> Execution {
        Execution {
            execution_id: execution_id.to_owned(),
            pane_id: "pane-1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            task_run_id: run_id,
            state,
        }
    }

    fn restored(model: DomainModel, next_ordinal: i64) -> RestoredState {
        RestoredState {
            model,
            next_ordinal,
        }
    }

    fn status_event(
        event_id: &str,
        timestamp_ms: i64,
        execution_id: &str,
        state: ExecState,
    ) -> NormalizedEvent {
        NormalizedEvent::AgentStatusChanged {
            metadata: metadata(event_id, timestamp_ms),
            execution_id: execution_id.to_owned(),
            state,
        }
    }

    fn topology_event(metadata: EventMetadata, workspace_id: &str) -> NormalizedEvent {
        NormalizedEvent::TopologyUpsert {
            metadata,
            entity: TopologyEntity::Workspace(Workspace {
                workspace_id: workspace_id.to_owned(),
            }),
        }
    }

    fn native_snapshot(sid: &str) -> TopologySnapshot {
        TopologySnapshot {
            workspaces: vec![Workspace {
                workspace_id: "workspace-1".to_owned(),
            }],
            tabs: Vec::new(),
            panes: vec![PaneSnapshot {
                pane_id: "pane-1".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                tab_id: "tab-1".to_owned(),
                terminal_id: "terminal-1".to_owned(),
                agent: Some(SnapshotAgent {
                    agent_name: "codex".to_owned(),
                    state: ExecState::Working,
                }),
                agent_session: Some(AgentSessionReference {
                    source: "herdr".to_owned(),
                    agent: "codex".to_owned(),
                    kind: AgentSessionReferenceKind::Id,
                    value: sid.to_owned(),
                }),
            }],
        }
    }

    #[test]
    fn done_maps_to_idle_never_ended() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_execution(execution(run_id, "execution-1", ExecState::Working));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let mut done = metadata("done", 1_000);
        done.source_event_type = "done".to_owned();

        reducer
            .apply(NormalizedEvent::AgentStatusChanged {
                metadata: done,
                execution_id: "execution-1".to_owned(),
                state: ExecState::Ended,
            })
            .unwrap();

        assert_eq!(
            shared.borrow().execution("execution-1").unwrap().state,
            ExecState::Idle
        );
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::Running
        );
    }

    #[test]
    fn stale_grace_live_observation_only() {
        let live_run = RunId::new();
        let mut live_model = DomainModel::default();
        live_model.insert_task_run(run(
            live_run,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "live-sid".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        live_model.insert_execution(execution(live_run, "live-execution", ExecState::Working));
        let (mut live_reducer, live_shared) = Reducer::new(restored(live_model, 2));

        live_reducer
            .apply(status_event(
                "missing-1",
                1_000,
                "live-execution",
                ExecState::Stale { since_ms: 0 },
            ))
            .unwrap();
        live_reducer
            .apply(status_event(
                "missing-2",
                30_999,
                "live-execution",
                ExecState::Stale { since_ms: 0 },
            ))
            .unwrap();
        assert_eq!(
            live_shared
                .borrow()
                .execution("live-execution")
                .unwrap()
                .state,
            ExecState::Stale { since_ms: 1_000 }
        );

        live_reducer
            .apply(status_event(
                "missing-3",
                31_000,
                "live-execution",
                ExecState::Stale { since_ms: 0 },
            ))
            .unwrap();
        assert_eq!(
            live_shared
                .borrow()
                .execution("live-execution")
                .unwrap()
                .state,
            ExecState::Ended
        );

        let gap_run = RunId::new();
        let mut gap_model = DomainModel::default();
        gap_model.insert_task_run(run(
            gap_run,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "gap-sid".to_owned(),
            },
            2,
            TaskState::Running,
        ));
        gap_model.insert_execution(execution(gap_run, "gap-execution", ExecState::Working));
        let (mut gap_reducer, gap_shared) = Reducer::new(restored(gap_model, 3));

        gap_reducer
            .reconcile_gap(ReconcileBatch {
                topology: TopologySnapshot::default(),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();

        assert_eq!(
            gap_shared
                .borrow()
                .execution("gap-execution")
                .unwrap()
                .state,
            ExecState::Ended
        );
        assert_eq!(
            gap_shared.borrow().task_run(&gap_run).unwrap().state,
            TaskState::EndedUnknown
        );
    }

    #[test]
    fn no_transient_ended_unknown_inside_reconcile() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            7,
            TaskState::Running,
        ));
        model.insert_execution(execution(run_id, "pre-gap", ExecState::Working));
        let (mut reducer, shared) = Reducer::new(restored(model, 8));

        let batch = reducer
            .reconcile_gap(ReconcileBatch {
                topology: native_snapshot("sid-1"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::Running
        );
        assert!(shared.borrow().executions().any(|value| {
            value.task_run_id == run_id
                && value.execution_id != "pre-gap"
                && !value.state.is_terminal()
        }));
        assert!(!batch.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTaskRun(value)
                if value.task_run.run_id == run_id
                    && value.task_run.state == TaskState::EndedUnknown
        )));
    }

    #[test]
    fn reactivation_on_new_execution() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            1,
            TaskState::EndedUnknown,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let mut begin_metadata = metadata("begin", 2_000);
        begin_metadata.provider = Some(Provider::Codex);
        begin_metadata.native_session_id = Some("sid-1".to_owned());

        reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: begin_metadata,
                execution: execution(run_id, "resumed", ExecState::Working),
            })
            .unwrap();

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::Running
        );
    }

    #[test]
    fn ordinal_assigned_once_survivor_keeps_it() {
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let provisional_key = RunKey::Provisional {
            terminal_id: "terminal-1".to_owned(),
            start_ms: 1_000,
            seq: 1,
        };
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            survivor,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            10,
            TaskState::Running,
        ));
        model.insert_task_run(run(
            absorbed,
            provisional_key.clone(),
            11,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 12));
        let mut begin_metadata = metadata("bind", 2_000);
        begin_metadata.provider = Some(Provider::Codex);
        begin_metadata.native_session_id = Some("sid-1".to_owned());

        let batch = reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: begin_metadata,
                execution: execution(absorbed, "execution-1", ExecState::Working),
            })
            .unwrap();

        assert!(batch.iter().any(|operation| matches!(
            operation,
            PersistOp::MergeTaskRuns {
                survivor: actual_survivor,
                absorbed: actual_absorbed,
            } if *actual_survivor == survivor && *actual_absorbed == absorbed
        )));
        assert_eq!(
            shared.borrow().task_run(&survivor).unwrap().display_ordinal,
            DisplayOrdinal::new(10)
        );
        assert!(shared.borrow().task_run(&absorbed).is_none());
        assert_eq!(
            shared
                .borrow()
                .task_run_by_key(&provisional_key)
                .unwrap()
                .run_id,
            survivor
        );
    }

    #[test]
    fn ordinal_allocator_seeds_from_restore() {
        let first = RunId::new();
        let second = RunId::new();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 42));
        let mut first_metadata = metadata("first", 1_000);
        first_metadata.source = "controller".to_owned();
        first_metadata.source_event_type = "task_started".to_owned();
        first_metadata.task_run_id = Some(first);
        first_metadata.task_state = Some(TaskState::Running);
        let mut second_metadata = first_metadata.clone();
        second_metadata.event_id = "second".to_owned();
        second_metadata.task_run_id = Some(second);

        reducer
            .apply(topology_event(first_metadata, "workspace-1"))
            .unwrap();
        reducer
            .apply(topology_event(second_metadata, "workspace-1"))
            .unwrap();

        assert_eq!(
            shared.borrow().task_run(&first).unwrap().display_ordinal,
            DisplayOrdinal::new(42)
        );
        assert_eq!(
            shared.borrow().task_run(&second).unwrap().display_ordinal,
            DisplayOrdinal::new(43)
        );
    }

    #[test]
    fn state_refresh_emits_no_reorder() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Controller("controller-1".to_owned()),
            9,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 10));
        let mut refresh = metadata("refresh", 3_000);
        refresh.source = "controller".to_owned();
        refresh.source_event_type = "task_started".to_owned();
        refresh.task_run_id = Some(run_id);
        refresh.task_state = Some(TaskState::Running);

        reducer
            .apply(topology_event(refresh, "workspace-1"))
            .unwrap();

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().display_ordinal,
            DisplayOrdinal::new(9)
        );
    }

    #[test]
    fn relationship_events_do_not_make_controller_owned() {
        let parent = RunId::new();
        let prerequisite = RunId::new();
        let subject = RunId::new();
        let mut relationship = metadata("relationships", 4_000);
        relationship.source = "controller".to_owned();
        relationship.source_event_type = "dispatch".to_owned();
        relationship.task_run_id = Some(subject);
        relationship.execution_parent = Some(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: subject,
        });
        relationship.dependency = Some(DependencyEdge {
            prerequisite_run_id: prerequisite,
            dependent_run_id: subject,
        });
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        reducer
            .apply(topology_event(relationship, "workspace-1"))
            .unwrap();

        assert!([parent, prerequisite, subject].iter().all(|run_id| {
            let snapshot = shared.borrow();
            let task_run = snapshot.task_run(run_id).unwrap();
            !task_run.has_controller_task_state_event && task_run.state == TaskState::Queued
        }));
        assert_eq!(shared.borrow().execution_edges().count(), 1);
        assert_eq!(shared.borrow().dependency_edges().count(), 1);
    }

    #[test]
    fn snapshot_published_after_each_apply() {
        let (mut reducer, mut shared) = Reducer::new(restored(DomainModel::default(), 1));
        let initial = Arc::clone(&shared.borrow());

        reducer
            .apply(topology_event(metadata("one", 1_000), "workspace-1"))
            .unwrap();
        assert!(shared.has_changed().unwrap());
        let first = Arc::clone(&shared.borrow_and_update());
        assert!(!Arc::ptr_eq(&initial, &first));
        assert!(first.workspace("workspace-1").is_some());

        reducer
            .apply(topology_event(metadata("two", 2_000), "workspace-2"))
            .unwrap();
        assert!(shared.has_changed().unwrap());
        let second = Arc::clone(&shared.borrow_and_update());
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(second.workspace("workspace-1").is_some());
        assert!(second.workspace("workspace-2").is_some());
    }

    #[test]
    fn stale_sweep_ends_after_grace() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "stale-sweep".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_execution(execution(
            run_id,
            "stale-execution",
            ExecState::Stale { since_ms: 1_000 },
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(reducer.sweep_stale(30_999).is_empty());
        assert_eq!(
            shared.borrow().execution("stale-execution").unwrap().state,
            ExecState::Stale { since_ms: 1_000 }
        );
        let persist = reducer.sweep_stale(31_000);

        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertExecution(value)
                if value.execution.execution_id == "stale-execution"
                    && value.execution.state == ExecState::Ended
        )));
        assert_eq!(
            shared.borrow().execution("stale-execution").unwrap().state,
            ExecState::Ended
        );
    }

    #[test]
    fn gap_reattach_reuses_agent_node() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "reattach-sid".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_agent_node(AgentNode {
            agent_node_id: "stable-top-level-node".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("reattach-sid".to_owned()),
            task_run_id: run_id,
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "sub-agent-node".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("child-sid".to_owned()),
            task_run_id: run_id,
        });
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: native_snapshot("reattach-sid"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();

        assert!(
            shared
                .borrow()
                .agent_node("stable-top-level-node")
                .is_some()
        );
        assert!(shared.borrow().agent_node("sub-agent-node").is_some());
        assert_eq!(
            shared
                .borrow()
                .agent_nodes()
                .filter(|node| {
                    node.task_run_id == run_id
                        && node.provider == Provider::Codex
                        && node.native_session_id.as_deref() == Some("reattach-sid")
                })
                .count(),
            1
        );
    }

    #[test]
    fn ordinal_exhaustion_returns_error_without_mutation() {
        let (mut reducer, mut shared) = Reducer::new(restored(DomainModel::default(), i64::MAX));
        let initial = Arc::clone(&shared.borrow_and_update());
        let run_id = RunId::new();
        let mut value = metadata("ordinal-exhaustion", 1_000);
        value.task_run_id = Some(run_id);

        let result = reducer.apply(topology_event(value, "must-not-exist"));

        assert_eq!(result, Err(ReducerError::OrdinalExhausted));
        assert!(!shared.has_changed().unwrap());
        assert_eq!(shared.borrow().task_runs().count(), 0);
        assert!(shared.borrow().workspace("must-not-exist").is_none());
        assert!(Arc::ptr_eq(&initial, &shared.borrow()));
    }
}
