//! T4C `ExecState` and `TaskState` value types; transition logic belongs to the reducer.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ExecState {
    Unknown,
    Idle,
    Working,
    Blocked,
    Stale { since_ms: i64 },
    Ended,
}

impl ExecState {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Ended)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TaskState {
    Queued,
    Running,
    Blocked,
    Completed,
    Failed,
    Cancelled,
    EndedUnknown,
}

impl TaskState {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::EndedUnknown
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_state_terminality() {
        let non_terminal = [
            ExecState::Unknown,
            ExecState::Idle,
            ExecState::Working,
            ExecState::Blocked,
            ExecState::Stale { since_ms: 42 },
        ];

        assert!(non_terminal.iter().all(|state| !state.is_terminal()));
        assert!(ExecState::Ended.is_terminal());
    }

    #[test]
    fn task_state_terminality() {
        let non_terminal = [TaskState::Queued, TaskState::Running, TaskState::Blocked];
        let terminal = [
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Cancelled,
            TaskState::EndedUnknown,
        ];

        assert!(non_terminal.iter().all(|state| !state.is_terminal()));
        assert!(terminal.iter().all(TaskState::is_terminal));
    }

    #[test]
    fn state_serde_roundtrip() {
        let exec_state = ExecState::Stale { since_ms: 123 };
        let task_state = TaskState::EndedUnknown;

        assert_eq!(
            serde_json::from_str::<ExecState>(&serde_json::to_string(&exec_state).unwrap())
                .unwrap(),
            exec_state
        );
        assert_eq!(
            serde_json::from_str::<TaskState>(&serde_json::to_string(&task_state).unwrap())
                .unwrap(),
            task_state
        );
    }
}
