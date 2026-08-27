//! T4C `ExecState` and `TaskState` value types; transition logic belongs to the reducer.

use serde::{Deserialize, Serialize};

/// Largest non-negative total that can be represented by SQLite's INTEGER domain.
pub const PERSISTED_RATE_TOTAL_MAX: u64 = i64::MAX as u64;

/// Bounded stable identity for one frozen provider-history manifest.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct HistoryDrainId(String);

impl HistoryDrainId {
    /// Maximum encoded identity length retained in memory and SQLite.
    pub const MAX_BYTES: usize = 160;

    /// Validates a non-empty, printable, bounded drain identity.
    pub fn new(value: impl Into<String>) -> Result<Self, &'static str> {
        let value = value.into();
        if value.is_empty() {
            return Err("history drain identity cannot be empty");
        }
        if value.len() > Self::MAX_BYTES {
            return Err("history drain identity exceeds the bounded byte length");
        }
        if value.chars().any(char::is_control) {
            return Err("history drain identity cannot contain control characters");
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Persisted native-session lifecycle outcome, independently of semantic task state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NativeSessionEndStatus {
    Done,
    Error,
    Cancelled,
    Unknown,
}

/// Persisted evidence that one native session ended.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NativeSessionEnd {
    pub status: NativeSessionEndStatus,
    pub at_ms: i64,
}

/// Total ordering key for native-session end and reopen observations.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NativeLifecycleWatermark {
    pub source_at_ms: i64,
    pub observed_at_ms: i64,
    pub source_order: String,
}

/// Persisted v6 state associated with one task run.
///
/// This is kept beside the stable `TaskRun` core so existing producers can migrate to v6
/// atomically without fabricating lifecycle or historical evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskRunV6State {
    pub native_session_end: Option<NativeSessionEnd>,
    pub lifecycle_watermark: Option<NativeLifecycleWatermark>,
    pub history_ready: bool,
    pub latest_provider_at_ms: Option<i64>,
}

impl Default for TaskRunV6State {
    fn default() -> Self {
        Self {
            native_session_end: None,
            lifecycle_watermark: None,
            history_ready: true,
            latest_provider_at_ms: None,
        }
    }
}

/// Persisted closed active-time rate ledger for one task run.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunRateTotals {
    pub output_tokens: u64,
    pub working_ms: i64,
}

impl RunRateTotals {
    /// Clamps both components to the non-negative SQLite INTEGER domain.
    #[must_use]
    pub fn clamped(self) -> (Self, bool) {
        let output_tokens = self.output_tokens.min(PERSISTED_RATE_TOTAL_MAX);
        let working_ms = self.working_ms.max(0);
        (
            Self {
                output_tokens,
                working_ms,
            },
            output_tokens != self.output_tokens || working_ms != self.working_ms,
        )
    }

    /// Adds closed totals and reports whether either persisted component saturated.
    pub fn saturating_add(&mut self, delta: Self) -> bool {
        let delta_working_ms = delta.working_ms.max(0);
        let (current, preexisting_clamp) = self.clamped();
        let output_sum = current.output_tokens.checked_add(delta.output_tokens);
        let working_sum = current.working_ms.checked_add(delta_working_ms);
        let output_tokens = output_sum.unwrap_or(u64::MAX).min(PERSISTED_RATE_TOTAL_MAX);
        let working_ms = working_sum.unwrap_or(i64::MAX);
        let saturated = preexisting_clamp
            || delta.working_ms < 0
            || output_sum.is_none_or(|sum| sum > PERSISTED_RATE_TOTAL_MAX)
            || working_sum.is_none();
        *self = Self {
            output_tokens,
            working_ms,
        };
        saturated
    }
}

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
pub enum PaneAgentStatus {
    Idle,
    Working,
    Blocked,
    Done,
    Unknown,
}

impl PaneAgentStatus {
    #[must_use]
    pub fn from_wire(value: Option<&str>) -> Self {
        match value {
            Some("idle") => Self::Idle,
            Some("working") => Self::Working,
            Some("blocked") => Self::Blocked,
            Some("done") => Self::Done,
            Some("unknown") | None => Self::Unknown,
            Some(_) => Self::Unknown,
        }
    }

    #[must_use]
    pub const fn execution_state(self) -> ExecState {
        match self {
            Self::Idle | Self::Done => ExecState::Idle,
            Self::Working => ExecState::Working,
            Self::Blocked => ExecState::Blocked,
            Self::Unknown => ExecState::Unknown,
        }
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

    #[test]
    fn pane_agent_status_preserves_wire_vocabulary() {
        let cases = [
            (Some("idle"), PaneAgentStatus::Idle),
            (Some("working"), PaneAgentStatus::Working),
            (Some("blocked"), PaneAgentStatus::Blocked),
            (Some("done"), PaneAgentStatus::Done),
            (Some("unknown"), PaneAgentStatus::Unknown),
            (None, PaneAgentStatus::Unknown),
            (Some("unrecognized"), PaneAgentStatus::Unknown),
        ];

        for (wire, expected) in cases {
            assert_eq!(PaneAgentStatus::from_wire(wire), expected);
        }
        assert_ne!(PaneAgentStatus::Done, PaneAgentStatus::Idle);
    }

    #[test]
    fn pane_agent_status_maps_to_existing_execution_states() {
        let cases = [
            (PaneAgentStatus::Idle, ExecState::Idle),
            (PaneAgentStatus::Working, ExecState::Working),
            (PaneAgentStatus::Blocked, ExecState::Blocked),
            (PaneAgentStatus::Done, ExecState::Idle),
            (PaneAgentStatus::Unknown, ExecState::Unknown),
        ];

        for (status, expected) in cases {
            assert_eq!(status.execution_state(), expected);
        }
    }

    #[test]
    fn negative_working_delta_records_saturation() {
        let mut totals = RunRateTotals {
            output_tokens: 41,
            working_ms: 500,
        };
        assert!(totals.saturating_add(RunRateTotals {
            output_tokens: 9,
            working_ms: -250,
        }));
        assert_eq!(
            totals,
            RunRateTotals {
                output_tokens: 50,
                working_ms: 500,
            }
        );

        let mut model = crate::model::DomainModel::default();
        let run_id = crate::model::RunId::new();
        assert!(model.accumulate_run_rate_totals(
            run_id,
            RunRateTotals {
                output_tokens: 7,
                working_ms: -1,
            }
        ));
        assert_eq!(model.controller_diagnostics().rate_total_saturations(), 1);
        assert_eq!(
            model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 7,
                working_ms: 0,
            })
        );
    }

    #[test]
    fn rate_totals_saturate_at_persisted_domain_maximum() {
        let mut totals = RunRateTotals {
            output_tokens: i64::MAX as u64 - 2,
            working_ms: i64::MAX - 2,
        };

        assert!(totals.saturating_add(RunRateTotals {
            output_tokens: 10,
            working_ms: 10,
        }));
        assert_eq!(
            totals,
            RunRateTotals {
                output_tokens: i64::MAX as u64,
                working_ms: i64::MAX,
            }
        );

        assert!(totals.saturating_add(RunRateTotals {
            output_tokens: u64::MAX,
            working_ms: i64::MAX,
        }));
        assert_eq!(totals.output_tokens, i64::MAX as u64);
        assert_eq!(totals.working_ms, i64::MAX);

        let mut model = crate::model::DomainModel::default();
        let run_id = crate::model::RunId::new();
        assert!(model.accumulate_run_rate_totals(
            run_id,
            RunRateTotals {
                output_tokens: u64::MAX,
                working_ms: i64::MAX,
            }
        ));
        assert_eq!(model.controller_diagnostics().rate_total_saturations(), 1);
        assert_eq!(
            model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: i64::MAX as u64,
                working_ms: i64::MAX,
            })
        );
    }
}
