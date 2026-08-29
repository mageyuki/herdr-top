//! Occurrence-specific run status derived from semantic, lifecycle, and runtime evidence.

#[cfg(test)]
use std::cell::Cell;
use std::collections::HashMap;

use crate::model::{
    AgentNode, DomainModel, ExecState, NativeSessionEndStatus, PaneAgentStatus, Provider, RunId,
    RunKey, TaskRun, TaskState,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskDisplayStatus {
    Queued,
    Working,
    Idle,
    Blocked,
    Done,
    Error,
    Cancelled,
    Unknown,
}

impl TaskDisplayStatus {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Working => "working",
            Self::Idle => "idle",
            Self::Blocked => "blocked",
            Self::Done => "done",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatusSource {
    TaskState,
    NativeSessionLifecycle,
    PaneAgentStatus,
    ExecutionState,
    AgentNodeState,
    Fallback,
}

impl StatusSource {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::TaskState => "task_state",
            Self::NativeSessionLifecycle => "native_session_lifecycle",
            Self::PaneAgentStatus => "pane_agent_status",
            Self::ExecutionState => "execution_state",
            Self::AgentNodeState => "agent_node_state",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DisplayStatus {
    pub(crate) status: TaskDisplayStatus,
    pub(crate) source: StatusSource,
    pub(crate) stalled: bool,
}

impl DisplayStatus {
    pub(crate) const fn new(status: TaskDisplayStatus, source: StatusSource) -> Self {
        Self {
            status,
            source,
            stalled: false,
        }
    }

    fn with_stalled(mut self, stalled: bool) -> Self {
        if !matches!(
            self.status,
            TaskDisplayStatus::Done | TaskDisplayStatus::Error | TaskDisplayStatus::Cancelled
        ) {
            self.stalled |= stalled;
        }
        self
    }

    const fn with_source(mut self, source: StatusSource) -> Self {
        self.source = source;
        self
    }

    pub(crate) const fn glyph(self) -> &'static str {
        if self.stalled {
            return "⚠";
        }
        match self.status {
            TaskDisplayStatus::Queued => "◌",
            TaskDisplayStatus::Working | TaskDisplayStatus::Blocked => "●",
            TaskDisplayStatus::Idle => "○",
            TaskDisplayStatus::Done => "✓",
            TaskDisplayStatus::Error => "✗",
            TaskDisplayStatus::Cancelled => "⊘",
            TaskDisplayStatus::Unknown => "?",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RunRateActivity {
    Working,
    Paused,
}

#[derive(Clone, Debug)]
struct AgentStatusEvidence {
    state: Option<ExecState>,
    last_activity_at_ms: Option<i64>,
    agent_node_id: String,
}

#[cfg(test)]
thread_local! {
    static RATE_STATUS_EVIDENCE_VISITS: Cell<usize> = const { Cell::new(0) };
    static RATE_PANE_EXECUTION_CANDIDATE_VISITS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_rate_status_evidence_visits() {
    RATE_STATUS_EVIDENCE_VISITS.set(0);
    RATE_PANE_EXECUTION_CANDIDATE_VISITS.set(0);
}

#[cfg(test)]
pub(crate) fn rate_status_evidence_visits() -> usize {
    RATE_STATUS_EVIDENCE_VISITS.get()
}

#[cfg(test)]
pub(crate) fn rate_pane_execution_candidate_visits() -> usize {
    RATE_PANE_EXECUTION_CANDIDATE_VISITS.get()
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StatusReadModel {
    pane_executions: HashMap<RunId, HashMap<String, ExecState>>,
    root_agents: HashMap<RunId, AgentStatusEvidence>,
    /// Presentation-only newest Agent Node per Task Run exact `RunKey::Native` alias.
    ///
    /// Ownership, parentage, and display staleness are deliberately ignored: a real child
    /// completion stays parented and owned by its controller/root run, while the child Task Run
    /// is only addressable through its exact provider-and-session alias. This map never makes
    /// an Agent row visible and never feeds `run_rate_activity`.
    durable_native_agents: HashMap<RunId, AgentStatusEvidence>,
}

fn insert_newest_agent_evidence(
    selected: &mut HashMap<RunId, AgentStatusEvidence>,
    run_id: RunId,
    candidate: AgentStatusEvidence,
) {
    selected
        .entry(run_id)
        .and_modify(|current| {
            if (current.last_activity_at_ms, current.agent_node_id.as_str())
                < (
                    candidate.last_activity_at_ms,
                    candidate.agent_node_id.as_str(),
                )
            {
                current.clone_from(&candidate);
            }
        })
        .or_insert(candidate);
}

impl StatusReadModel {
    pub(crate) fn from_model(model: &DomainModel, now_ms: i64) -> Self {
        Self::from_model_filtered(model, now_ms, None)
    }

    fn from_model_filtered(model: &DomainModel, now_ms: i64, run_id: Option<RunId>) -> Self {
        let mut selected_executions = HashMap::<(RunId, String), (bool, String, ExecState)>::new();
        for execution in model.executions() {
            #[cfg(test)]
            RATE_STATUS_EVIDENCE_VISITS.set(RATE_STATUS_EVIDENCE_VISITS.get() + 1);
            if run_id.is_some_and(|run_id| execution.task_run_id != run_id) {
                continue;
            }
            let key = (execution.task_run_id, execution.pane_id.clone());
            let candidate = (
                execution.state.is_terminal(),
                execution.execution_id.clone(),
                execution.state.clone(),
            );
            selected_executions
                .entry(key)
                .and_modify(|selected| {
                    if (candidate.0, candidate.1.as_str()) < (selected.0, selected.1.as_str()) {
                        selected.clone_from(&candidate);
                    }
                })
                .or_insert(candidate);
        }
        let mut pane_executions = HashMap::<RunId, HashMap<String, ExecState>>::new();
        for ((run_id, pane_id), (_, _, state)) in selected_executions {
            pane_executions
                .entry(run_id)
                .or_default()
                .insert(pane_id, state);
        }

        // The exact-native alias lookup exists only for the full display projection. The
        // filtered rate-only projection never builds or consults durable presentation evidence.
        let native_aliases = run_id.is_none().then(|| {
            model
                .task_run_bindings()
                .filter_map(|(key, run_id)| match key {
                    RunKey::Native { provider, sid } => Some(((*provider, sid.as_str()), *run_id)),
                    RunKey::NativePath { .. }
                    | RunKey::Controller(_)
                    | RunKey::Provisional { .. } => None,
                })
                .collect::<HashMap<(Provider, &str), RunId>>()
        });

        let mut root_agents = HashMap::<RunId, AgentStatusEvidence>::new();
        let mut durable_native_agents = HashMap::<RunId, AgentStatusEvidence>::new();
        for agent in model.agent_nodes() {
            #[cfg(test)]
            RATE_STATUS_EVIDENCE_VISITS.set(RATE_STATUS_EVIDENCE_VISITS.get() + 1);
            let is_live_line = agent.last_event_kind.as_deref()
                == Some(crate::provider::lane::LIVE_LINE_EVENT_KIND);
            if let Some(native_aliases) = native_aliases.as_ref()
                && !is_live_line
                && let Some(sid) = agent
                    .native_session_id
                    .as_deref()
                    .filter(|sid| !sid.is_empty())
                && let Some(target_run_id) = native_aliases.get(&(agent.provider, sid))
            {
                insert_newest_agent_evidence(
                    &mut durable_native_agents,
                    *target_run_id,
                    AgentStatusEvidence {
                        state: agent.state.clone(),
                        last_activity_at_ms: agent.last_activity_at_ms,
                        agent_node_id: agent.agent_node_id.clone(),
                    },
                );
            }
            if run_id.is_some_and(|run_id| agent.task_run_id != run_id) {
                continue;
            }
            if agent.parent_agent_node_id.is_some()
                || is_live_line
                || agent_node_is_display_stale(agent, now_ms)
                || model
                    .task_run(&agent.task_run_id)
                    .and_then(task_run_provider)
                    .is_some_and(|provider| provider != agent.provider)
            {
                continue;
            }
            insert_newest_agent_evidence(
                &mut root_agents,
                agent.task_run_id,
                AgentStatusEvidence {
                    state: agent.state.clone(),
                    last_activity_at_ms: agent.last_activity_at_ms,
                    agent_node_id: agent.agent_node_id.clone(),
                },
            );
        }

        Self {
            pane_executions,
            root_agents,
            durable_native_agents,
        }
    }

    /// Returns Agent-Node-sourced `done` only when the newest exact-native evidence is ended.
    fn durable_ended_status(&self, run_id: RunId, inactive: bool) -> Option<DisplayStatus> {
        self.durable_native_agents
            .get(&run_id)
            .and_then(|evidence| {
                matches!(evidence.state.as_ref(), Some(ExecState::Ended)).then_some(
                    DisplayStatus::new(TaskDisplayStatus::Done, StatusSource::AgentNodeState)
                        .with_stalled(inactive),
                )
            })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn run_rate_activity_from_model(
        model: &DomainModel,
        run: &TaskRun,
        now_ms: i64,
    ) -> RunRateActivity {
        Self::from_model_filtered(model, now_ms, Some(run.run_id)).run_rate_activity(model, run)
    }

    pub(crate) fn task_display_status(
        &self,
        model: &DomainModel,
        run: &TaskRun,
        pane_id: Option<&str>,
        inactive: bool,
    ) -> DisplayStatus {
        // Semantic Completed, Failed, and Cancelled stay authoritative. Only EndedUnknown may be
        // refined by durable exact-native ended evidence; otherwise it stays Task-State unknown.
        let terminal = match run.state {
            TaskState::Completed => Some(TaskDisplayStatus::Done),
            TaskState::Failed => Some(TaskDisplayStatus::Error),
            TaskState::Cancelled => Some(TaskDisplayStatus::Cancelled),
            TaskState::EndedUnknown => {
                if let Some(status) = self.durable_ended_status(run.run_id, inactive) {
                    return status;
                }
                Some(TaskDisplayStatus::Unknown)
            }
            TaskState::Queued | TaskState::Blocked | TaskState::Running => None,
        };
        if let Some(status) = terminal {
            return DisplayStatus::new(status, StatusSource::TaskState).with_stalled(inactive);
        }

        // Native Done, Error, and Cancelled stay authoritative. Only native Unknown may be
        // refined by durable exact-native ended evidence; otherwise it stays lifecycle unknown.
        if let Some(end) = model
            .task_run_v6_state(&run.run_id)
            .and_then(|state| state.native_session_end.as_ref())
        {
            let status = match end.status {
                NativeSessionEndStatus::Done => TaskDisplayStatus::Done,
                NativeSessionEndStatus::Error => TaskDisplayStatus::Error,
                NativeSessionEndStatus::Cancelled => TaskDisplayStatus::Cancelled,
                NativeSessionEndStatus::Unknown => {
                    if let Some(status) = self.durable_ended_status(run.run_id, inactive) {
                        return status;
                    }
                    TaskDisplayStatus::Unknown
                }
            };
            return DisplayStatus::new(status, StatusSource::NativeSessionLifecycle)
                .with_stalled(inactive);
        }

        if matches!(run.state, TaskState::Queued | TaskState::Blocked) {
            let status = if run.state == TaskState::Queued {
                TaskDisplayStatus::Queued
            } else {
                TaskDisplayStatus::Blocked
            };
            return DisplayStatus::new(status, StatusSource::TaskState).with_stalled(inactive);
        }

        if let Some(pane_id) = pane_id
            && let Some(execution) = self
                .pane_executions
                .get(&run.run_id)
                .and_then(|executions| executions.get(pane_id))
        {
            if !execution.is_terminal()
                && let Some(status) = model.pane_agent_status(pane_id)
            {
                return pane_agent_display_status(status).with_stalled(inactive);
            }
            return execution_display_status(execution, false).with_stalled(inactive);
        }

        if let Some(evidence) = self.root_agents.get(&run.run_id)
            && let Some(state) = evidence.state.as_ref()
        {
            return execution_display_status(state, false)
                .with_source(StatusSource::AgentNodeState)
                .with_stalled(inactive);
        }

        DisplayStatus::new(TaskDisplayStatus::Working, StatusSource::TaskState)
            .with_stalled(inactive)
    }

    pub(crate) fn run_rate_activity(&self, model: &DomainModel, run: &TaskRun) -> RunRateActivity {
        if run.state.is_terminal()
            || matches!(run.state, TaskState::Queued | TaskState::Blocked)
            || model
                .task_run_v6_state(&run.run_id)
                .is_some_and(|state| state.native_session_end.is_some())
        {
            return RunRateActivity::Paused;
        }
        if let Some(executions) = self.pane_executions.get(&run.run_id) {
            for (pane_id, execution) in executions {
                #[cfg(test)]
                RATE_PANE_EXECUTION_CANDIDATE_VISITS
                    .set(RATE_PANE_EXECUTION_CANDIDATE_VISITS.get() + 1);
                if execution.is_terminal() {
                    continue;
                }
                match model.pane_agent_status(pane_id) {
                    Some(PaneAgentStatus::Working) => return RunRateActivity::Working,
                    Some(_) => {}
                    None if matches!(execution, ExecState::Working) => {
                        return RunRateActivity::Working;
                    }
                    None => {}
                }
            }
            return RunRateActivity::Paused;
        }
        if let Some(evidence) = self.root_agents.get(&run.run_id) {
            return if evidence
                .state
                .as_ref()
                .is_some_and(|state| matches!(state, ExecState::Working))
            {
                RunRateActivity::Working
            } else {
                RunRateActivity::Paused
            };
        }
        RunRateActivity::Working
    }
}

fn task_run_provider(run: &TaskRun) -> Option<Provider> {
    match &run.key {
        RunKey::Native { provider, .. } | RunKey::NativePath { provider, .. } => Some(*provider),
        RunKey::Controller(_) | RunKey::Provisional { .. } => None,
    }
}

fn pane_agent_display_status(status: PaneAgentStatus) -> DisplayStatus {
    let status = match status {
        PaneAgentStatus::Idle => TaskDisplayStatus::Idle,
        PaneAgentStatus::Working => TaskDisplayStatus::Working,
        PaneAgentStatus::Blocked => TaskDisplayStatus::Blocked,
        PaneAgentStatus::Done => TaskDisplayStatus::Done,
        PaneAgentStatus::Unknown => TaskDisplayStatus::Unknown,
    };
    DisplayStatus::new(status, StatusSource::PaneAgentStatus)
}

fn execution_display_status(state: &ExecState, ended_is_done: bool) -> DisplayStatus {
    let (status, stalled) = match state {
        ExecState::Unknown => (TaskDisplayStatus::Unknown, false),
        ExecState::Idle => (TaskDisplayStatus::Idle, false),
        ExecState::Working => (TaskDisplayStatus::Working, false),
        ExecState::Blocked => (TaskDisplayStatus::Blocked, false),
        ExecState::Stale { .. } => (TaskDisplayStatus::Unknown, true),
        ExecState::Ended if ended_is_done => (TaskDisplayStatus::Done, false),
        ExecState::Ended => (TaskDisplayStatus::Unknown, false),
    };
    DisplayStatus {
        status,
        source: StatusSource::ExecutionState,
        stalled,
    }
}

pub(crate) fn native_agent_display_status(agent: &AgentNode) -> DisplayStatus {
    agent.state.as_ref().map_or_else(
        || DisplayStatus::new(TaskDisplayStatus::Unknown, StatusSource::Fallback),
        |state| execution_display_status(state, true).with_source(StatusSource::AgentNodeState),
    )
}

pub(crate) fn agent_node_is_display_stale(agent: &AgentNode, now_ms: i64) -> bool {
    matches!(
        agent.state.as_ref(),
        None | Some(ExecState::Unknown) | Some(ExecState::Ended)
    ) && agent
        .last_activity_at_ms
        .is_some_and(|last_activity_at_ms| {
            now_ms.saturating_sub(last_activity_at_ms) >= crate::activity::headless_inactivity_ms()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AgentNode, DisplayOrdinal, Execution};

    #[test]
    fn recent_matching_root_agent_unknown_precedes_running_fallback() {
        let run = TaskRun {
            run_id: RunId::new(),
            key: RunKey::Native {
                provider: Provider::Codex,
                sid: "session-42".to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: Some(100),
            updated_at_ms: Some(100),
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        model.insert_agent_node(AgentNode {
            agent_node_id: "root-agent".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("session-42".to_owned()),
            task_run_id: run.run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Unknown),
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: Some(100),
            session_file: None,
        });
        let statuses = StatusReadModel::from_model(&model, 100);

        assert_eq!(
            statuses.task_display_status(&model, &run, None, false),
            DisplayStatus::new(TaskDisplayStatus::Unknown, StatusSource::AgentNodeState)
        );
    }

    #[test]
    fn run_rate_activity_uses_authoritative_exact_pane_status_with_or_semantics() {
        let run = TaskRun {
            run_id: RunId::new(),
            key: RunKey::Controller("rate-status".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: Some(1),
            updated_at_ms: Some(1),
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        model.insert_execution(Execution {
            execution_id: "pane-a-execution".to_owned(),
            pane_id: "pane-a".to_owned(),
            terminal_id: "terminal-a".to_owned(),
            task_run_id: run.run_id,
            state: ExecState::Working,
        });
        model.set_pane_agent_status("pane-a".to_owned(), PaneAgentStatus::Idle);

        let statuses = StatusReadModel::from_model(&model, 10);
        assert_eq!(
            statuses.run_rate_activity(&model, &run),
            RunRateActivity::Paused
        );

        model.insert_execution(Execution {
            execution_id: "pane-b-execution".to_owned(),
            pane_id: "pane-b".to_owned(),
            terminal_id: "terminal-b".to_owned(),
            task_run_id: run.run_id,
            state: ExecState::Idle,
        });
        model.set_pane_agent_status("pane-b".to_owned(), PaneAgentStatus::Working);
        let statuses = StatusReadModel::from_model(&model, 10);
        assert_eq!(
            statuses.run_rate_activity(&model, &run),
            RunRateActivity::Working
        );
    }

    #[test]
    fn exact_run_rate_activity_matches_full_status_projection_across_precedence_cases() {
        fn task_run(label: &str, ordinal: i64, state: TaskState) -> TaskRun {
            TaskRun {
                run_id: RunId::new(),
                key: RunKey::Controller(label.to_owned()),
                display_ordinal: DisplayOrdinal::new(ordinal),
                state,
                has_controller_task_state_event: true,
                created_at_ms: Some(1),
                updated_at_ms: Some(1),
                finished_at_ms: state.is_terminal().then_some(2),
                subject: None,
                dismissed_at_ms: None,
            }
        }

        fn execution(run_id: RunId, id: &str, pane_id: &str, state: ExecState) -> Execution {
            Execution {
                execution_id: id.to_owned(),
                pane_id: pane_id.to_owned(),
                terminal_id: format!("terminal-{pane_id}"),
                task_run_id: run_id,
                state,
            }
        }

        fn root_agent(
            run_id: RunId,
            id: &str,
            state: ExecState,
            last_activity_at_ms: i64,
        ) -> AgentNode {
            AgentNode {
                agent_node_id: id.to_owned(),
                provider: Provider::Codex,
                native_session_id: None,
                task_run_id: run_id,
                display_ordinal: DisplayOrdinal::new(last_activity_at_ms),
                parent_agent_node_id: None,
                state: Some(state),
                model_id: None,
                last_event_kind: None,
                last_tool_name: None,
                last_item_count: None,
                last_byte_count: None,
                last_activity_at_ms: Some(last_activity_at_ms),
                session_file: None,
            }
        }

        let pane_or = task_run("pane-or", 1, TaskState::Running);
        let terminal = task_run("terminal", 2, TaskState::Completed);
        let queued = task_run("queued", 3, TaskState::Queued);
        let blocked = task_run("blocked", 4, TaskState::Blocked);
        let mut headless = task_run("headless", 5, TaskState::Running);
        headless.key = RunKey::Native {
            provider: Provider::Codex,
            sid: "headless".to_owned(),
        };
        let mut stale_descendant = task_run("stale-descendant", 6, TaskState::Running);
        stale_descendant.key = RunKey::Native {
            provider: Provider::Codex,
            sid: "stale-descendant".to_owned(),
        };
        let fallback = task_run("fallback", 7, TaskState::Running);

        let mut model = DomainModel::default();
        for run in [
            &pane_or,
            &terminal,
            &queued,
            &blocked,
            &headless,
            &stale_descendant,
            &fallback,
        ] {
            model.insert_task_run(run.clone());
        }
        model.insert_execution(execution(
            pane_or.run_id,
            "pane-idle",
            "pane-idle",
            ExecState::Working,
        ));
        model.set_pane_agent_status("pane-idle".to_owned(), PaneAgentStatus::Idle);
        model.insert_execution(execution(
            pane_or.run_id,
            "pane-working",
            "pane-working",
            ExecState::Idle,
        ));
        model.set_pane_agent_status("pane-working".to_owned(), PaneAgentStatus::Working);
        model.insert_agent_node(root_agent(
            headless.run_id,
            "headless-old-working",
            ExecState::Working,
            10,
        ));
        model.insert_agent_node(root_agent(
            headless.run_id,
            "headless-new-idle",
            ExecState::Idle,
            20,
        ));
        let mut stale_root =
            root_agent(stale_descendant.run_id, "stale-root", ExecState::Unknown, 1);
        stale_root.native_session_id = Some("stale-descendant".to_owned());
        model.insert_agent_node(stale_root);
        let mut descendant = root_agent(
            stale_descendant.run_id,
            "ineligible-descendant",
            ExecState::Working,
            30,
        );
        descendant.parent_agent_node_id = Some("stale-root".to_owned());
        model.insert_agent_node(descendant);

        let now_ms = crate::activity::headless_inactivity_ms() + 2;
        let full = StatusReadModel::from_model(&model, now_ms);
        let expected = [
            (&pane_or, RunRateActivity::Working),
            (&terminal, RunRateActivity::Paused),
            (&queued, RunRateActivity::Paused),
            (&blocked, RunRateActivity::Paused),
            (&headless, RunRateActivity::Paused),
            (&stale_descendant, RunRateActivity::Working),
            (&fallback, RunRateActivity::Working),
        ];
        for (run, expected) in expected {
            let full_activity = full.run_rate_activity(&model, run);
            let exact_activity = StatusReadModel::run_rate_activity_from_model(&model, run, now_ms);
            assert_eq!(full_activity, expected, "full projection for {:?}", run.key);
            assert_eq!(
                exact_activity, expected,
                "exact projection for {:?}",
                run.key
            );
            assert_eq!(
                exact_activity, full_activity,
                "projection mismatch for {:?}",
                run.key
            );
        }
    }

    // Durable child-status projection fixtures.
    //
    // The target Task Run is Controller-keyed and independently addressable through an exact
    // `RunKey::Native` alias. Agent Nodes are owned by a *different* controller/root Task Run and
    // are parented, matching the persisted production topology of a real Codex child.

    const CHILD_SID: &str = "child-sid";
    const ROOT_AGENT_ID: &str = "root-agent";

    fn durable_target_run(state: TaskState) -> TaskRun {
        TaskRun {
            run_id: RunId::new(),
            key: RunKey::Controller(format!("hook:codex:{CHILD_SID}")),
            display_ordinal: DisplayOrdinal::new(2),
            state,
            has_controller_task_state_event: true,
            created_at_ms: Some(1),
            updated_at_ms: Some(1),
            finished_at_ms: state.is_terminal().then_some(2),
            subject: None,
            dismissed_at_ms: None,
        }
    }

    /// Returns a model holding the target run, its exact native alias, and a separate owner run.
    fn durable_model(target: &TaskRun) -> (DomainModel, RunId) {
        let owner = TaskRun {
            run_id: RunId::new(),
            key: RunKey::Controller("hook:codex:controller-root".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: Some(1),
            updated_at_ms: Some(1),
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let mut model = DomainModel::default();
        model.insert_task_run(owner.clone());
        model.insert_task_run(target.clone());
        assert_eq!(
            model.insert_task_run_alias(
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: CHILD_SID.to_owned(),
                },
                target.run_id,
            ),
            None
        );
        (model, owner.run_id)
    }

    fn parented_agent(
        agent_node_id: &str,
        owner_run_id: RunId,
        provider: Provider,
        native_session_id: Option<&str>,
        state: ExecState,
        last_event_kind: Option<&str>,
        last_activity_at_ms: i64,
    ) -> AgentNode {
        AgentNode {
            agent_node_id: agent_node_id.to_owned(),
            provider,
            native_session_id: native_session_id.map(str::to_owned),
            task_run_id: owner_run_id,
            display_ordinal: DisplayOrdinal::new(last_activity_at_ms),
            parent_agent_node_id: Some(ROOT_AGENT_ID.to_owned()),
            state: Some(state),
            model_id: None,
            last_event_kind: last_event_kind.map(str::to_owned),
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: Some(last_activity_at_ms),
            session_file: None,
        }
    }

    fn exact_ended_agent(owner_run_id: RunId, last_activity_at_ms: i64) -> AgentNode {
        parented_agent(
            "exact-ended-child",
            owner_run_id,
            Provider::Codex,
            Some(CHILD_SID),
            ExecState::Ended,
            None,
            last_activity_at_ms,
        )
    }

    fn display_status_at(model: &DomainModel, run: &TaskRun, now_ms: i64) -> DisplayStatus {
        StatusReadModel::from_model(model, now_ms).task_display_status(model, run, None, false)
    }

    fn agent_done() -> DisplayStatus {
        DisplayStatus::new(TaskDisplayStatus::Done, StatusSource::AgentNodeState)
    }

    fn task_state_unknown() -> DisplayStatus {
        DisplayStatus::new(TaskDisplayStatus::Unknown, StatusSource::TaskState)
    }

    fn set_native_end(model: &mut DomainModel, run_id: RunId, status: NativeSessionEndStatus) {
        model.set_task_run_v6_state(
            run_id,
            crate::model::TaskRunV6State {
                native_session_end: Some(crate::model::NativeSessionEnd { status, at_ms: 100 }),
                ..crate::model::TaskRunV6State::default()
            },
        );
    }

    #[test]
    fn ended_unknown_uses_exact_native_ended_agent_across_staleness() {
        let target = durable_target_run(TaskState::EndedUnknown);
        let (mut model, owner) = durable_model(&target);
        model.insert_agent_node(exact_ended_agent(owner, 100));
        let staleness_deadline_ms = 100 + crate::activity::headless_inactivity_ms();

        assert_eq!(
            display_status_at(&model, &target, staleness_deadline_ms - 1),
            agent_done(),
            "immediately before the Agent row staleness boundary"
        );
        assert_eq!(
            display_status_at(&model, &target, staleness_deadline_ms),
            agent_done(),
            "exactly at the Agent row staleness boundary"
        );
    }

    #[test]
    fn unknown_refinement_requires_exact_native_binding() {
        let now_ms = 100 + crate::activity::headless_inactivity_ms();

        // Negative control 1: same provider, different SID.
        let target = durable_target_run(TaskState::EndedUnknown);
        let (mut model, owner) = durable_model(&target);
        model.insert_agent_node(parented_agent(
            "different-sid",
            owner,
            Provider::Codex,
            Some("other-sid"),
            ExecState::Ended,
            None,
            100,
        ));
        assert_eq!(
            display_status_at(&model, &target, now_ms),
            task_state_unknown(),
            "a different SID must not refine unknown"
        );

        // Negative control 2: foreign provider with the same SID.
        let target = durable_target_run(TaskState::EndedUnknown);
        let (mut model, owner) = durable_model(&target);
        model.insert_agent_node(parented_agent(
            "foreign-provider",
            owner,
            Provider::Claude,
            Some(CHILD_SID),
            ExecState::Ended,
            None,
            100,
        ));
        assert_eq!(
            display_status_at(&model, &target, now_ms),
            task_state_unknown(),
            "a foreign provider must not refine unknown"
        );

        // Negative control 3: exact binding, but a synthetic live-line node.
        let target = durable_target_run(TaskState::EndedUnknown);
        let (mut model, owner) = durable_model(&target);
        model.insert_agent_node(parented_agent(
            "live-line-exact",
            owner,
            Provider::Codex,
            Some(CHILD_SID),
            ExecState::Ended,
            Some(crate::provider::lane::LIVE_LINE_EVENT_KIND),
            100,
        ));
        assert_eq!(
            display_status_at(&model, &target, now_ms),
            task_state_unknown(),
            "a synthetic live-line node must not refine unknown"
        );

        // Positive control: exact provider and SID, non-live-line, ended.
        let target = durable_target_run(TaskState::EndedUnknown);
        let (mut model, owner) = durable_model(&target);
        model.insert_agent_node(exact_ended_agent(owner, 100));
        assert_eq!(
            display_status_at(&model, &target, now_ms),
            agent_done(),
            "an exact ended binding must refine unknown to Agent-Node-sourced done"
        );
    }

    #[test]
    fn newest_exact_native_agent_must_be_ended() {
        fn model_with(ended_at_ms: i64, working_at_ms: i64) -> (DomainModel, TaskRun) {
            let target = durable_target_run(TaskState::EndedUnknown);
            let (mut model, owner) = durable_model(&target);
            model.insert_agent_node(parented_agent(
                "exact-ended",
                owner,
                Provider::Codex,
                Some(CHILD_SID),
                ExecState::Ended,
                None,
                ended_at_ms,
            ));
            model.insert_agent_node(parented_agent(
                "exact-working",
                owner,
                Provider::Codex,
                Some(CHILD_SID),
                ExecState::Working,
                None,
                working_at_ms,
            ));
            (model, target)
        }
        let now_ms = 20 + crate::activity::headless_inactivity_ms();

        let (model, target) = model_with(10, 20);
        assert_eq!(
            display_status_at(&model, &target, now_ms),
            task_state_unknown(),
            "an older ended node must not override a newer working exact node"
        );

        let (model, target) = model_with(20, 10);
        assert_eq!(
            display_status_at(&model, &target, now_ms),
            agent_done(),
            "the newest exact node being ended must refine unknown"
        );
    }

    #[test]
    fn native_unknown_uses_exact_native_ended_agent_but_definitive_outcomes_win() {
        let target = durable_target_run(TaskState::Running);
        let (mut model, owner) = durable_model(&target);
        model.insert_agent_node(exact_ended_agent(owner, 100));
        let now_ms = 100 + crate::activity::headless_inactivity_ms();

        set_native_end(&mut model, target.run_id, NativeSessionEndStatus::Unknown);
        assert_eq!(
            display_status_at(&model, &target, now_ms),
            agent_done(),
            "native Unknown must use durable exact ended evidence"
        );

        for (status, expected) in [
            (NativeSessionEndStatus::Done, TaskDisplayStatus::Done),
            (NativeSessionEndStatus::Error, TaskDisplayStatus::Error),
            (
                NativeSessionEndStatus::Cancelled,
                TaskDisplayStatus::Cancelled,
            ),
        ] {
            set_native_end(&mut model, target.run_id, status);
            assert_eq!(
                display_status_at(&model, &target, now_ms),
                DisplayStatus::new(expected, StatusSource::NativeSessionLifecycle),
                "native {status:?} must stay authoritative"
            );
        }
    }

    #[test]
    fn semantic_definitive_outcomes_override_exact_native_ended_agent() {
        let now_ms = 100 + crate::activity::headless_inactivity_ms();
        for (state, expected) in [
            (TaskState::Completed, TaskDisplayStatus::Done),
            (TaskState::Failed, TaskDisplayStatus::Error),
            (TaskState::Cancelled, TaskDisplayStatus::Cancelled),
        ] {
            let target = durable_target_run(state);
            let (mut model, owner) = durable_model(&target);
            model.insert_agent_node(exact_ended_agent(owner, 100));
            assert_eq!(
                display_status_at(&model, &target, now_ms),
                DisplayStatus::new(expected, StatusSource::TaskState),
                "semantic {state:?} must stay authoritative"
            );
        }
    }

    #[test]
    fn durable_native_projection_does_not_change_run_rate_activity() {
        let now_ms = 100 + crate::activity::headless_inactivity_ms();
        for (state, expected) in [
            (TaskState::Running, RunRateActivity::Working),
            (TaskState::EndedUnknown, RunRateActivity::Paused),
        ] {
            let target = durable_target_run(state);
            let (mut model, owner) = durable_model(&target);
            model.insert_agent_node(exact_ended_agent(owner, 100));
            let owner_run = model.task_run(&owner).cloned().unwrap();

            let full = StatusReadModel::from_model(&model, now_ms);
            assert_eq!(
                full.run_rate_activity(&model, &target),
                expected,
                "full projection for {state:?}"
            );
            assert_eq!(
                full.run_rate_activity(&model, &owner_run),
                RunRateActivity::Working,
                "full projection for the owner run"
            );

            reset_rate_status_evidence_visits();
            assert_eq!(
                StatusReadModel::run_rate_activity_from_model(&model, &target, now_ms),
                expected,
                "exact projection for {state:?}"
            );
            assert_eq!(
                rate_status_evidence_visits(),
                1,
                "the rate-only projection visits each Agent Node exactly once"
            );
            assert_eq!(rate_pane_execution_candidate_visits(), 0);

            reset_rate_status_evidence_visits();
            let _ = StatusReadModel::from_model(&model, now_ms);
            assert_eq!(
                rate_status_evidence_visits(),
                1,
                "the full projection visits each Agent Node exactly once"
            );
        }
    }
}
