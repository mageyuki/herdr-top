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

        let mut root_agents = HashMap::<RunId, AgentStatusEvidence>::new();
        for agent in model.agent_nodes() {
            #[cfg(test)]
            RATE_STATUS_EVIDENCE_VISITS.set(RATE_STATUS_EVIDENCE_VISITS.get() + 1);
            if run_id.is_some_and(|run_id| agent.task_run_id != run_id) {
                continue;
            }
            if agent.parent_agent_node_id.is_some()
                || agent.last_event_kind.as_deref()
                    == Some(crate::provider::lane::LIVE_LINE_EVENT_KIND)
                || agent_node_is_display_stale(agent, now_ms)
                || model
                    .task_run(&agent.task_run_id)
                    .and_then(task_run_provider)
                    .is_some_and(|provider| provider != agent.provider)
            {
                continue;
            }
            let candidate = AgentStatusEvidence {
                state: agent.state.clone(),
                last_activity_at_ms: agent.last_activity_at_ms,
                agent_node_id: agent.agent_node_id.clone(),
            };
            root_agents
                .entry(agent.task_run_id)
                .and_modify(|selected| {
                    if (
                        selected.last_activity_at_ms,
                        selected.agent_node_id.as_str(),
                    ) < (
                        candidate.last_activity_at_ms,
                        candidate.agent_node_id.as_str(),
                    ) {
                        selected.clone_from(&candidate);
                    }
                })
                .or_insert(candidate);
        }

        Self {
            pane_executions,
            root_agents,
        }
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
        let terminal = match run.state {
            TaskState::Completed => Some(TaskDisplayStatus::Done),
            TaskState::Failed => Some(TaskDisplayStatus::Error),
            TaskState::Cancelled => Some(TaskDisplayStatus::Cancelled),
            TaskState::EndedUnknown => Some(TaskDisplayStatus::Unknown),
            TaskState::Queued | TaskState::Blocked | TaskState::Running => None,
        };
        if let Some(status) = terminal {
            return DisplayStatus::new(status, StatusSource::TaskState).with_stalled(inactive);
        }

        if let Some(end) = model
            .task_run_v6_state(&run.run_id)
            .and_then(|state| state.native_session_end.as_ref())
        {
            let status = match end.status {
                NativeSessionEndStatus::Done => TaskDisplayStatus::Done,
                NativeSessionEndStatus::Error => TaskDisplayStatus::Error,
                NativeSessionEndStatus::Cancelled => TaskDisplayStatus::Cancelled,
                NativeSessionEndStatus::Unknown => TaskDisplayStatus::Unknown,
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
}
