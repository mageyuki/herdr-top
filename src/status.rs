//! Occurrence-specific run status derived from semantic, lifecycle, and runtime evidence.

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
#[allow(dead_code)]
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

#[derive(Clone, Debug, Default)]
pub(crate) struct StatusReadModel {
    pane_executions: HashMap<(RunId, String), ExecState>,
    root_agents: HashMap<RunId, AgentStatusEvidence>,
}

impl StatusReadModel {
    pub(crate) fn from_model(model: &DomainModel, now_ms: i64) -> Self {
        let mut selected_executions = HashMap::<(RunId, String), (bool, String, ExecState)>::new();
        for execution in model.executions() {
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
        let pane_executions = selected_executions
            .into_iter()
            .map(|(key, (_, _, state))| (key, state))
            .collect();

        let mut root_agents = HashMap::<RunId, AgentStatusEvidence>::new();
        for agent in model.agent_nodes() {
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
            && let Some(execution) = self.pane_executions.get(&(run.run_id, pane_id.to_owned()))
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

    #[allow(dead_code)]
    pub(crate) fn run_rate_activity(&self, model: &DomainModel, run: &TaskRun) -> RunRateActivity {
        if run.state.is_terminal()
            || matches!(run.state, TaskState::Queued | TaskState::Blocked)
            || model
                .task_run_v6_state(&run.run_id)
                .is_some_and(|state| state.native_session_end.is_some())
        {
            return RunRateActivity::Paused;
        }
        let mut has_pane_occurrence = false;
        for ((run_id, pane_id), execution) in &self.pane_executions {
            if *run_id != run.run_id {
                continue;
            }
            has_pane_occurrence = true;
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
        if has_pane_occurrence {
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
}
