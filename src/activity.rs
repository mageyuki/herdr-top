//! Immutable operator activity and terminal-visibility read models.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::model::{Provider, RunId, RunKey, TaskRun, TaskState};

pub const DEFAULT_TERMINAL_VISIBILITY_MS: i64 = 60 * 60 * 1_000;
pub const HOOK_ONLY_STALE_VISIBILITY_MS: i64 = 24 * 60 * 60 * 1_000;
pub const DEFAULT_STALL_WARN_MS: i64 = 300_000;
pub const DEFAULT_GHOST_VISIBILITY_MS: i64 = 300_000;
pub const DEFAULT_HEADLESS_INACTIVITY_MS: i64 =
    crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS;

static STALL_WARN_MS: AtomicI64 = AtomicI64::new(DEFAULT_STALL_WARN_MS);
static GHOST_VISIBILITY_MS: AtomicI64 = AtomicI64::new(DEFAULT_GHOST_VISIBILITY_MS);
static HEADLESS_INACTIVITY_MS: AtomicI64 = AtomicI64::new(DEFAULT_HEADLESS_INACTIVITY_MS);

/// Installs process-wide display timing resolved once by the monitor entrypoint.
pub fn configure_display_timing(
    stall_warn_ms: i64,
    ghost_visibility_ms: i64,
    headless_inactivity_ms: i64,
) {
    STALL_WARN_MS.store(stall_warn_ms, Ordering::Relaxed);
    GHOST_VISIBILITY_MS.store(ghost_visibility_ms, Ordering::Relaxed);
    HEADLESS_INACTIVITY_MS.store(headless_inactivity_ms, Ordering::Relaxed);
}

#[must_use]
pub(crate) fn stall_warn_ms() -> i64 {
    STALL_WARN_MS.load(Ordering::Relaxed)
}

#[must_use]
pub(crate) fn ghost_visibility_ms() -> i64 {
    GHOST_VISIBILITY_MS.load(Ordering::Relaxed)
}

#[must_use]
pub(crate) fn headless_inactivity_ms() -> i64 {
    HEADLESS_INACTIVITY_MS.load(Ordering::Relaxed)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ActivityIdentity {
    pub event_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivityDurability {
    Durable,
    CurrentOnly,
    DurabilityUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityItem {
    pub identity: ActivityIdentity,
    pub event_timestamp_ms: i64,
    pub seen_at_ms: i64,
    pub ingest_seq: Option<u64>,
    pub source: String,
    pub normalized_kind: String,
    pub source_event_type: String,
    pub workspace_id: Option<String>,
    pub tab_id: Option<String>,
    pub pane_id: Option<String>,
    pub terminal_id: Option<String>,
    pub provider: Option<Provider>,
    pub native_session_id: Option<String>,
    pub task_run_id: Option<RunId>,
    pub agent_node_id: Option<String>,
    pub task_state: Option<TaskState>,
    pub model_id: Option<String>,
    pub provider_event_kind: Option<String>,
    pub tool_name: Option<String>,
    pub item_count: Option<u64>,
    pub byte_count: Option<u64>,
    pub provider_agent_id: Option<String>,
    pub provider_parent_agent_id: Option<String>,
    pub controller_label: Option<String>,
    pub controller_reason: Option<String>,
    pub durability: ActivityDurability,
}

pub struct RestoredOperatorState {
    pub activity: Vec<ActivityItem>,
    pub terminal_times: HashMap<RunId, i64>,
}

pub struct OperatorSnapshot {
    pub activity: Arc<[ActivityItem]>,
    pub terminal_times: Arc<HashMap<RunId, i64>>,
}

#[must_use]
pub fn runs_with_executions(model: &crate::model::DomainModel) -> HashSet<RunId> {
    model
        .executions()
        .map(|execution| execution.task_run_id)
        .collect()
}

/// Returns whether a Controller-keyed run without an execution reached its visibility deadline.
#[must_use]
pub fn is_hook_only_stale_task_run(
    run: &TaskRun,
    runs_with_executions: &HashSet<RunId>,
    now_ms: i64,
) -> bool {
    let hook_only =
        matches!(run.key, RunKey::Controller(_)) && !runs_with_executions.contains(&run.run_id);
    hook_only
        && run.updated_at_ms.is_some_and(|updated_at_ms| {
            now_ms >= updated_at_ms.saturating_add(HOOK_ONLY_STALE_VISIBILITY_MS)
        })
}

/// `runs_with_executions` must be derived from the same model snapshot as `run`.
#[must_use]
pub fn is_default_visible_task_run(
    model: &crate::model::DomainModel,
    run: &crate::model::TaskRun,
    runs_with_executions: &HashSet<RunId>,
    now_ms: i64,
) -> bool {
    is_default_visible_task_run_with_ghost_window(
        model,
        run,
        runs_with_executions,
        now_ms,
        ghost_visibility_ms(),
    )
}

/// Computes the single default-visible run set and closes it over execution ancestors.
#[must_use]
pub(crate) fn default_visible_task_run_ids(
    model: &crate::model::DomainModel,
    _operator: &OperatorSnapshot,
    now_ms: i64,
) -> HashSet<RunId> {
    let execution_run_ids = runs_with_executions(model);
    let mut visible = model
        .task_runs()
        .filter(|run| is_default_visible_task_run(model, run, &execution_run_ids, now_ms))
        .map(|run| run.run_id)
        .collect::<HashSet<_>>();
    loop {
        let ancestors = model
            .execution_edges()
            .filter(|edge| visible.contains(&edge.child_run_id))
            .map(|edge| edge.parent_run_id)
            .filter(|run_id| !visible.contains(run_id))
            .filter_map(|run_id| model.task_run(&run_id))
            .filter(|run| is_expired_lifecycle_ancestor(model, run, &execution_run_ids, now_ms))
            .map(|run| run.run_id)
            .collect::<Vec<_>>();
        if ancestors.is_empty() {
            break;
        }
        visible.extend(ancestors);
    }
    visible
}

/// Returns default visibility with an explicit provisional-run window.
///
/// `RunKey::Provisional` is minted for terminal occupancy without provider identity, so its
/// latest observation bounds the lifetime of a possible herdr misdetection independently of the
/// ordinary terminal and hook-only windows.
#[must_use]
pub fn is_default_visible_task_run_with_ghost_window(
    model: &crate::model::DomainModel,
    run: &crate::model::TaskRun,
    runs_with_executions: &HashSet<RunId>,
    now_ms: i64,
    ghost_visibility_ms: i64,
) -> bool {
    let lifecycle_end_ms = model.effective_lifecycle_end_ms(run);
    if !is_default_visibility_policy_eligible(
        run,
        runs_with_executions,
        lifecycle_end_ms,
        now_ms,
        ghost_visibility_ms,
    ) {
        return false;
    }
    lifecycle_end_ms
        .is_none_or(|end_ms| now_ms < end_ms.saturating_add(DEFAULT_TERMINAL_VISIBILITY_MS))
}

fn is_expired_lifecycle_ancestor(
    model: &crate::model::DomainModel,
    run: &TaskRun,
    runs_with_executions: &HashSet<RunId>,
    now_ms: i64,
) -> bool {
    let lifecycle_end_ms = model.effective_lifecycle_end_ms(run);
    is_default_visibility_policy_eligible(
        run,
        runs_with_executions,
        lifecycle_end_ms,
        now_ms,
        ghost_visibility_ms(),
    ) && lifecycle_end_ms
        .is_some_and(|end_ms| now_ms >= end_ms.saturating_add(DEFAULT_TERMINAL_VISIBILITY_MS))
}

fn is_default_visibility_policy_eligible(
    run: &TaskRun,
    runs_with_executions: &HashSet<RunId>,
    lifecycle_end_ms: Option<i64>,
    now_ms: i64,
    ghost_visibility_ms: i64,
) -> bool {
    if run.dismissed_at_ms.is_some() {
        return false;
    }
    if lifecycle_end_ms.is_none()
        && let RunKey::Provisional { start_ms, .. } = &run.key
    {
        let last_observed_ms = run.updated_at_ms.or(run.created_at_ms).unwrap_or(*start_ms);
        if now_ms >= last_observed_ms.saturating_add(ghost_visibility_ms) {
            return false;
        }
    }
    if is_hook_only_stale_task_run(run, runs_with_executions, now_ms) {
        return false;
    }
    true
}

#[must_use]
pub fn default_visible_task_run_count(
    model: &crate::model::DomainModel,
    operator: &OperatorSnapshot,
    now_ms: i64,
) -> usize {
    default_visible_task_run_ids(model, operator, now_ms).len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        DisplayOrdinal, DomainModel, ExecState, Execution, ExecutionEdge, NativeSessionEnd,
        NativeSessionEndStatus, RunKey, TaskRun, TaskRunV6State,
    };

    fn task_run(key: RunKey, state: TaskState, updated_at_ms: Option<i64>) -> TaskRun {
        TaskRun {
            run_id: RunId::new(),
            key,
            display_ordinal: DisplayOrdinal::new(1),
            state,
            has_controller_task_state_event: true,
            created_at_ms: updated_at_ms,
            updated_at_ms,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        }
    }

    fn empty_operator() -> OperatorSnapshot {
        OperatorSnapshot {
            activity: Arc::from(Vec::new()),
            terminal_times: Arc::new(HashMap::new()),
        }
    }

    fn visibility_fixture(now_ms: i64) -> (DomainModel, OperatorSnapshot) {
        let run_ids = [
            RunId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            RunId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
            RunId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap(),
            RunId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAY").unwrap(),
        ];
        let states = [
            TaskState::Running,
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Cancelled,
        ];
        let mut model = DomainModel::default();
        for (index, (run_id, state)) in run_ids.into_iter().zip(states).enumerate() {
            let finished_at_ms = match index {
                1 => Some(now_ms - 3_599_999),
                2 => Some(now_ms - 3_600_000),
                _ => None,
            };
            model.insert_task_run(TaskRun {
                run_id,
                key: RunKey::Controller(format!("visibility-{index}")),
                display_ordinal: DisplayOrdinal::new(index as i64 + 1),
                state,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms,
                subject: None,
                dismissed_at_ms: None,
            });
        }
        let operator = OperatorSnapshot {
            activity: Arc::from(Vec::new()),
            terminal_times: Arc::new(HashMap::from([
                (run_ids[1], now_ms - 3_599_999),
                (run_ids[2], now_ms - 3_600_000),
            ])),
        };
        (model, operator)
    }

    #[test]
    fn default_visible_count_matches_live_and_one_hour_terminal_policy() {
        let now_ms = 7_200_000;
        let (model, operator) = visibility_fixture(now_ms);
        assert_eq!(default_visible_task_run_count(&model, &operator, now_ms), 3);
    }

    #[test]
    fn semantic_terminal_visibility_takes_precedence_over_expired_native_lifecycle() {
        let now_ms = 7_200_000;
        let semantic_end_ms = now_ms - DEFAULT_TERMINAL_VISIBILITY_MS + 1;
        let mut run = task_run(
            RunKey::Native {
                provider: Provider::Codex,
                sid: "semantic-terminal".to_owned(),
            },
            TaskState::Completed,
            Some(now_ms),
        );
        run.finished_at_ms = Some(semantic_end_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        model.set_task_run_v6_state(
            run.run_id,
            TaskRunV6State {
                native_session_end: Some(NativeSessionEnd {
                    status: NativeSessionEndStatus::Done,
                    at_ms: now_ms - DEFAULT_TERMINAL_VISIBILITY_MS - 1,
                }),
                ..TaskRunV6State::default()
            },
        );
        assert!(is_default_visible_task_run(
            &model,
            &run,
            &HashSet::new(),
            now_ms,
        ));
    }

    #[test]
    fn model_visibility_uses_effective_endpoint_precedence() {
        let now_ms = 7_200_000;
        let semantic_end_ms = now_ms - DEFAULT_TERMINAL_VISIBILITY_MS + 1;
        let mut run = task_run(
            RunKey::Native {
                provider: Provider::Codex,
                sid: "effective-endpoint".to_owned(),
            },
            TaskState::Running,
            Some(semantic_end_ms),
        );
        run.finished_at_ms = Some(semantic_end_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        model.set_task_run_v6_state(
            run.run_id,
            TaskRunV6State {
                native_session_end: Some(NativeSessionEnd {
                    status: NativeSessionEndStatus::Done,
                    at_ms: now_ms - DEFAULT_TERMINAL_VISIBILITY_MS - 1,
                }),
                ..TaskRunV6State::default()
            },
        );

        assert!(is_default_visible_task_run(
            &model,
            &run,
            &HashSet::new(),
            now_ms,
        ));
    }

    #[test]
    fn native_lifecycle_end_expires_at_the_exact_one_hour_boundary() {
        let now_ms = 7_200_000;
        let mut model = DomainModel::default();
        for sid in ["root", "child", "grandchild"] {
            let run = task_run(
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: sid.to_owned(),
                },
                TaskState::Running,
                Some(now_ms - DEFAULT_TERMINAL_VISIBILITY_MS),
            );
            model.insert_task_run(run.clone());
            model.set_task_run_v6_state(
                run.run_id,
                TaskRunV6State {
                    native_session_end: Some(NativeSessionEnd {
                        status: NativeSessionEndStatus::Done,
                        at_ms: now_ms - DEFAULT_TERMINAL_VISIBILITY_MS,
                    }),
                    ..TaskRunV6State::default()
                },
            );

            assert!(
                !is_default_visible_task_run(&model, &run, &HashSet::new(), now_ms,),
                "{sid} must expire exactly at the one-hour boundary"
            );
        }
    }

    #[test]
    fn visible_grandchild_keeps_expired_execution_ancestors_as_structure() {
        let now_ms = 7_200_000;
        let mut model = DomainModel::default();
        let mut root = task_run(
            RunKey::Controller("root".to_owned()),
            TaskState::Completed,
            None,
        );
        root.finished_at_ms = Some(now_ms - DEFAULT_TERMINAL_VISIBILITY_MS);
        let mut child = task_run(
            RunKey::Controller("child".to_owned()),
            TaskState::Completed,
            None,
        );
        child.finished_at_ms = Some(now_ms - DEFAULT_TERMINAL_VISIBILITY_MS);
        let grandchild = task_run(
            RunKey::Controller("grandchild".to_owned()),
            TaskState::Running,
            Some(now_ms),
        );
        for run in [&root, &child, &grandchild] {
            model.insert_task_run(run.clone());
        }
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: root.run_id,
            child_run_id: child.run_id,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: child.run_id,
            child_run_id: grandchild.run_id,
        });
        let execution_run_ids = runs_with_executions(&model);
        assert!(!is_default_visible_task_run(
            &model,
            &root,
            &execution_run_ids,
            now_ms,
        ));
        assert!(!is_default_visible_task_run(
            &model,
            &child,
            &execution_run_ids,
            now_ms,
        ));
        assert!(is_default_visible_task_run(
            &model,
            &grandchild,
            &execution_run_ids,
            now_ms,
        ));

        assert_eq!(
            default_visible_task_run_ids(&model, &empty_operator(), now_ms),
            HashSet::from([root.run_id, child.run_id, grandchild.run_id])
        );
    }

    #[test]
    fn dismissed_execution_parent_is_not_retained_for_visible_child() {
        let now_ms = 7_200_000;
        let mut model = DomainModel::default();
        let mut parent = task_run(
            RunKey::Controller("dismissed-parent".to_owned()),
            TaskState::Running,
            Some(now_ms),
        );
        parent.dismissed_at_ms = Some(now_ms);
        let child = task_run(
            RunKey::Controller("visible-child".to_owned()),
            TaskState::Running,
            Some(now_ms),
        );
        for run in [&parent, &child] {
            model.insert_task_run(run.clone());
        }
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent.run_id,
            child_run_id: child.run_id,
        });

        assert_eq!(
            default_visible_task_run_ids(&model, &empty_operator(), now_ms),
            HashSet::from([child.run_id])
        );
    }

    #[test]
    fn stale_hook_and_expired_ghost_parents_are_not_retained() {
        let now_ms = 100_000_000;
        let mut model = DomainModel::default();
        let stale_hook_parent = task_run(
            RunKey::Controller("stale-hook-parent".to_owned()),
            TaskState::Running,
            Some(now_ms - HOOK_ONLY_STALE_VISIBILITY_MS),
        );
        let ghost_observed_ms = now_ms - ghost_visibility_ms();
        let expired_ghost_parent = task_run(
            RunKey::Provisional {
                terminal_id: "ghost-terminal".to_owned(),
                start_ms: ghost_observed_ms,
                seq: 1,
            },
            TaskState::Running,
            Some(ghost_observed_ms),
        );
        let hook_child = task_run(
            RunKey::Controller("hook-child".to_owned()),
            TaskState::Running,
            Some(now_ms),
        );
        let ghost_child = task_run(
            RunKey::Controller("ghost-child".to_owned()),
            TaskState::Running,
            Some(now_ms),
        );
        for run in [
            &stale_hook_parent,
            &expired_ghost_parent,
            &hook_child,
            &ghost_child,
        ] {
            model.insert_task_run(run.clone());
        }
        for (parent_run_id, child_run_id) in [
            (stale_hook_parent.run_id, hook_child.run_id),
            (expired_ghost_parent.run_id, ghost_child.run_id),
        ] {
            model.insert_execution_edge(ExecutionEdge {
                parent_run_id,
                child_run_id,
            });
        }

        assert_eq!(
            default_visible_task_run_ids(&model, &empty_operator(), now_ms),
            HashSet::from([hook_child.run_id, ghost_child.run_id])
        );
    }

    #[test]
    fn runs_with_executions_collects_distinct_run_ids() {
        let first = RunId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let second = RunId::parse("01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap();
        let mut model = DomainModel::default();
        for (execution_id, task_run_id) in
            [("first", first), ("duplicate", first), ("second", second)]
        {
            model.insert_execution(Execution {
                execution_id: execution_id.to_owned(),
                pane_id: "pane".to_owned(),
                terminal_id: "terminal".to_owned(),
                task_run_id,
                state: ExecState::Working,
            });
        }

        assert_eq!(
            runs_with_executions(&model),
            std::collections::HashSet::from([first, second])
        );
    }

    #[test]
    fn precomputed_execution_membership_controls_hook_only_expiry() {
        let run = task_run(
            RunKey::Controller("membership".to_owned()),
            TaskState::Running,
            Some(100),
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        let now_ms = 100 + HOOK_ONLY_STALE_VISIBILITY_MS;

        assert!(is_default_visible_task_run(
            &model,
            &run,
            &std::collections::HashSet::from([run.run_id]),
            now_ms,
        ));
        assert!(!is_default_visible_task_run(
            &model,
            &run,
            &std::collections::HashSet::new(),
            now_ms,
        ));
        assert_eq!(model.executions().count(), 0);
    }

    #[test]
    fn hook_only_run_expires_at_exactly_twenty_four_hours() {
        let updated_at_ms = 100;
        let run = task_run(
            RunKey::Controller("hook-only".to_owned()),
            TaskState::Running,
            Some(updated_at_ms),
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        let execution_run_ids = runs_with_executions(&model);

        assert!(is_default_visible_task_run(
            &model,
            &run,
            &execution_run_ids,
            updated_at_ms + 86_400_000 - 1,
        ));
        assert!(!is_default_visible_task_run(
            &model,
            &run,
            &execution_run_ids,
            updated_at_ms + 86_400_000,
        ));

        let mut fresh = run.clone();
        fresh.updated_at_ms = Some(updated_at_ms + 86_400_000);
        model.insert_task_run(fresh.clone());
        assert!(is_default_visible_task_run(
            &model,
            &fresh,
            &execution_run_ids,
            updated_at_ms + 86_400_000,
        ));
    }

    #[test]
    fn native_keyed_run_is_never_hidden_by_hook_only_expiry() {
        let run = task_run(
            RunKey::Native {
                provider: Provider::Codex,
                sid: "native".to_owned(),
            },
            TaskState::Running,
            Some(100),
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        let execution_run_ids = runs_with_executions(&model);

        assert!(is_default_visible_task_run(
            &model,
            &run,
            &execution_run_ids,
            86_400_100,
        ));
    }

    #[test]
    fn controller_keyed_run_with_execution_never_expires_as_hook_only() {
        let run = task_run(
            RunKey::Controller("attached".to_owned()),
            TaskState::Running,
            Some(100),
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        model.insert_execution(Execution {
            execution_id: "execution".to_owned(),
            pane_id: "pane".to_owned(),
            terminal_id: "terminal".to_owned(),
            task_run_id: run.run_id,
            state: ExecState::Working,
        });
        let execution_run_ids = runs_with_executions(&model);

        assert!(is_default_visible_task_run(
            &model,
            &run,
            &execution_run_ids,
            i64::MAX,
        ));
    }

    #[test]
    fn dismissed_run_is_hidden_while_fresh_and_non_terminal() {
        let mut run = task_run(
            RunKey::Controller("dismissed".to_owned()),
            TaskState::Running,
            Some(100),
        );
        run.dismissed_at_ms = Some(101);
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        let execution_run_ids = runs_with_executions(&model);

        assert!(!is_default_visible_task_run(
            &model,
            &run,
            &execution_run_ids,
            101,
        ));
    }

    #[test]
    fn hook_only_run_without_updated_time_never_expires() {
        let run = task_run(
            RunKey::Controller("restored".to_owned()),
            TaskState::Running,
            None,
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        let execution_run_ids = runs_with_executions(&model);

        assert!(is_default_visible_task_run(
            &model,
            &run,
            &execution_run_ids,
            i64::MAX,
        ));
    }

    #[test]
    fn terminal_visibility_remains_one_hour_with_exact_boundary() {
        let mut run = task_run(
            RunKey::Native {
                provider: Provider::Codex,
                sid: "terminal".to_owned(),
            },
            TaskState::Completed,
            Some(50),
        );
        run.finished_at_ms = Some(100);
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        let execution_run_ids = runs_with_executions(&model);

        assert!(is_default_visible_task_run(
            &model,
            &run,
            &execution_run_ids,
            3_600_099,
        ));
        assert!(!is_default_visible_task_run(
            &model,
            &run,
            &execution_run_ids,
            3_600_100,
        ));
    }

    #[test]
    fn ghost_provisional_short_window() {
        let updated_at_ms = 100;
        let ghost = task_run(
            RunKey::Provisional {
                terminal_id: "misdetected".to_owned(),
                start_ms: updated_at_ms,
                seq: 1,
            },
            TaskState::Running,
            Some(updated_at_ms),
        );
        let ordinary = task_run(
            RunKey::Native {
                provider: Provider::Codex,
                sid: "real-session".to_owned(),
            },
            TaskState::Running,
            Some(updated_at_ms),
        );
        let mut terminal_ghost = task_run(
            RunKey::Provisional {
                terminal_id: "completed".to_owned(),
                start_ms: updated_at_ms,
                seq: 2,
            },
            TaskState::Completed,
            Some(updated_at_ms),
        );
        terminal_ghost.finished_at_ms = Some(updated_at_ms);
        let execution_run_ids =
            HashSet::from([ghost.run_id, ordinary.run_id, terminal_ghost.run_id]);
        let mut model = DomainModel::default();
        for run in [&ghost, &ordinary, &terminal_ghost] {
            model.insert_task_run(run.clone());
        }

        assert!(is_default_visible_task_run_with_ghost_window(
            &model,
            &ghost,
            &execution_run_ids,
            updated_at_ms + DEFAULT_GHOST_VISIBILITY_MS - 1,
            DEFAULT_GHOST_VISIBILITY_MS,
        ));
        assert!(!is_default_visible_task_run_with_ghost_window(
            &model,
            &ghost,
            &execution_run_ids,
            updated_at_ms + DEFAULT_GHOST_VISIBILITY_MS,
            DEFAULT_GHOST_VISIBILITY_MS,
        ));
        assert!(is_default_visible_task_run_with_ghost_window(
            &model,
            &ordinary,
            &execution_run_ids,
            updated_at_ms + DEFAULT_GHOST_VISIBILITY_MS,
            DEFAULT_GHOST_VISIBILITY_MS,
        ));
        assert!(is_default_visible_task_run_with_ghost_window(
            &model,
            &terminal_ghost,
            &execution_run_ids,
            updated_at_ms + DEFAULT_GHOST_VISIBILITY_MS,
            DEFAULT_GHOST_VISIBILITY_MS,
        ));
        assert!(is_default_visible_task_run_with_ghost_window(
            &model,
            &terminal_ghost,
            &execution_run_ids,
            updated_at_ms + DEFAULT_TERMINAL_VISIBILITY_MS - 1,
            DEFAULT_GHOST_VISIBILITY_MS,
        ));
        assert!(!is_default_visible_task_run_with_ghost_window(
            &model,
            &terminal_ghost,
            &execution_run_ids,
            updated_at_ms + DEFAULT_TERMINAL_VISIBILITY_MS,
            DEFAULT_GHOST_VISIBILITY_MS,
        ));
    }
}
