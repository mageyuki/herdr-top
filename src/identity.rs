//! T6 binding evidence, `BindingPlan`, and `plan_binding`/`apply_binding_plan` merge machinery.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::model::{DependencyEdge, DomainModel, ExecutionEdge, Provider, RunId, RunKey};
use crate::store::{NativeSessionBinding, PersistBatch, PersistOp, PersistTaskRun};

/// Explicit evidence that can bind or merge a task-run identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindingEvidence {
    /// A provider session ID was observed for an existing run.
    NativeSession {
        /// The run whose execution supplied the evidence.
        run: RunId,
        /// The provider that owns the session namespace.
        provider: Provider,
        /// The provider-native session ID.
        sid: String,
    },
    /// A Controller event explicitly tied its run to a native session.
    ControllerNativeSession {
        /// The Controller-keyed run being bound.
        controller_run: RunId,
        /// The provider that owns the session namespace.
        provider: Provider,
        /// The provider-native session ID carried by the event.
        sid: String,
    },
    /// A Controller event explicitly tied its run to a terminal's live run.
    ControllerTerminal {
        /// The Controller-keyed run being bound.
        controller_run: RunId,
        /// The point-in-time terminal selector carried by the event.
        terminal_id: String,
    },
}

/// A pure identity-binding decision ready for atomic application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BindingPlan {
    /// The evidence already resolves to the desired canonical run.
    NoChange,
    /// Add or promote one key binding on an existing canonical run.
    Bind {
        /// The canonical run receiving the binding.
        run: RunId,
        /// The resolved key being bound.
        key: RunKey,
    },
    /// Contract `absorbed` into `survivor` in memory and durably.
    Merge {
        /// The higher-priority canonical run that remains live.
        survivor: RunId,
        /// The canonical run converted into an alias.
        absorbed: RunId,
    },
    /// Defer evidence that cannot safely change the model.
    Conflict(MergeConflict),
}

/// A reason that identity evidence was deferred without changing state.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MergeConflict {
    /// A referenced canonical run is absent.
    #[error("task run {run} does not exist")]
    MissingRun { run: RunId },
    /// A merge requires two distinct canonical runs.
    #[error("task run {run} cannot be merged into itself")]
    SameRun { run: RunId },
    /// The requested survivor does not own the highest-priority key.
    #[error("task run {survivor} cannot absorb higher-priority task run {absorbed}")]
    LowerPrioritySurvivor {
        /// The proposed survivor.
        survivor: RunId,
        /// The higher-priority proposed absorbed run.
        absorbed: RunId,
    },
    /// Evidence does not describe the run's current identity.
    #[error("binding evidence does not match task run {run}")]
    EvidenceMismatch { run: RunId },
    /// Only explicit Controller evidence may merge a native run into K1.
    #[error(
        "merging task run {observed} into Controller run {controller} requires explicit Controller evidence"
    )]
    ExplicitControllerEvidenceRequired {
        /// The higher-priority Controller run.
        controller: RunId,
        /// The lower-priority observed run.
        observed: RunId,
    },
    /// The supplied Controller target is not keyed by a Controller ID.
    #[error("task run {run} is not Controller-keyed")]
    ControllerRunRequired { run: RunId },
    /// Explicit Controller evidence tried to combine two K1 runs.
    #[error("Controller run {claimant} cannot bind to identity owned by Controller run {owner}")]
    ControllerIdentityAlreadyBound {
        /// The existing K1 owner.
        owner: RunId,
        /// The conflicting K1 claimant.
        claimant: RunId,
    },
    /// One native session is already bound to a different Controller run.
    #[error("native session {provider:?}:{sid} is already bound to {owner}, not {claimant}")]
    NativeSessionAlreadyBound {
        /// The provider that owns the session namespace.
        provider: Provider,
        /// The provider-native session ID.
        sid: String,
        /// The existing K1 owner.
        owner: RunId,
        /// The conflicting K1 claimant.
        claimant: RunId,
    },
    /// The merge parties carry different native-session identities.
    #[error("task runs {survivor} and {absorbed} carry different native-session bindings")]
    DifferingNativeSessions {
        /// The proposed survivor.
        survivor: RunId,
        /// The proposed absorbed run.
        absorbed: RunId,
    },
    /// One run has accumulated multiple native identities in memory.
    #[error("task run {run} has multiple native-session bindings")]
    MultipleNativeSessions { run: RunId },
    /// A bind tried to replace a run's existing native-session identity.
    #[error("task run {run} cannot be rebound to a different native session")]
    NativeSessionRebind { run: RunId },
    /// A key is already owned by a different canonical run.
    #[error("identity key is already bound to task run {owner}, not {claimant}")]
    KeyAlreadyBound {
        /// The existing canonical owner.
        owner: RunId,
        /// The conflicting claimant.
        claimant: RunId,
    },
    /// `Bind` only accepts a resolved native-session key.
    #[error("only a resolved native-session key can be bound")]
    UnsupportedBindingKey,
    /// More than one live run occupies a point-in-time terminal selector.
    #[error("terminal {terminal_id} has multiple live task runs")]
    AmbiguousTerminal {
        /// The ambiguous terminal identity.
        terminal_id: String,
        /// The distinct live canonical runs, in stable ID order.
        runs: Vec<RunId>,
    },
    /// Contracting the merge parties would create a dispatch self-edge.
    #[error("task-run merge would create a dispatch self-edge at {run}")]
    DispatchSelfEdge { run: RunId },
    /// Contracting the merge parties would create a dependency self-edge.
    #[error("task-run merge would create a dependency self-edge at {run}")]
    DependencySelfEdge { run: RunId },
    /// Contracting the merge parties would create a dispatch cycle.
    #[error("task-run merge would create a dispatch cycle")]
    DispatchCycle,
    /// Contracting the merge parties would create a dependency cycle.
    #[error("task-run merge would create a dependency cycle")]
    DependencyCycle,
    /// Contracting the merge parties would give a child two parents.
    #[error(
        "task-run merge would give {child} differing dispatch parents {first_parent} and {second_parent}"
    )]
    DifferingDispatchParents {
        /// The contracted child.
        child: RunId,
        /// The lower sorted parent ID.
        first_parent: RunId,
        /// The higher sorted parent ID.
        second_parent: RunId,
    },
}

/// Plans an identity binding without mutating `model`.
#[must_use]
pub fn plan_binding(model: &DomainModel, ev: &BindingEvidence) -> BindingPlan {
    match ev {
        BindingEvidence::NativeSession { run, provider, sid } => {
            plan_native_session(model, *run, *provider, sid)
        }
        BindingEvidence::ControllerNativeSession {
            controller_run,
            provider,
            sid,
        } => plan_controller_native(model, *controller_run, *provider, sid),
        BindingEvidence::ControllerTerminal {
            controller_run,
            terminal_id,
        } => plan_controller_terminal(model, *controller_run, terminal_id),
    }
}

/// Applies the sole public identity mutation path and returns its durable half.
///
/// Reducers must resolve aliases through the model immediately after admission,
/// before event-ledger lookup and before staging any placeholder Task Runs.
pub fn apply_binding_plan(
    model: &mut DomainModel,
    plan: BindingPlan,
) -> Result<PersistBatch, MergeConflict> {
    apply_binding_plan_at(model, plan, unix_now_ms())
}

/// Applies a binding plan using the collector-captured receipt time for persistence.
pub fn apply_binding_plan_at(
    model: &mut DomainModel,
    plan: BindingPlan,
    receipt_time_ms: i64,
) -> Result<PersistBatch, MergeConflict> {
    match plan {
        BindingPlan::NoChange => Ok(Vec::new()),
        BindingPlan::Conflict(conflict) => Err(conflict),
        BindingPlan::Bind { run, key } => apply_bind(model, run, key, receipt_time_ms),
        BindingPlan::Merge { survivor, absorbed } => {
            let contracted = preflight_merge(model, survivor, absorbed)?;
            merge_in_memory(model, survivor, absorbed, contracted);
            Ok(vec![PersistOp::MergeTaskRuns { survivor, absorbed }])
        }
    }
}

/// Preflights one candidate dispatch edge against the current live graph.
pub fn preflight_execution_edge(
    model: &DomainModel,
    candidate: &ExecutionEdge,
) -> Result<(), MergeConflict> {
    let mut parent_by_child = HashMap::new();
    let mut pairs = Vec::new();
    for edge in model.execution_edges().chain(std::iter::once(candidate)) {
        if edge.parent_run_id == edge.child_run_id {
            return Err(MergeConflict::DispatchSelfEdge {
                run: edge.parent_run_id,
            });
        }
        if let Some(existing_parent) = parent_by_child.get(&edge.child_run_id) {
            if *existing_parent != edge.parent_run_id {
                let (first_parent, second_parent) =
                    sorted_pair(*existing_parent, edge.parent_run_id);
                return Err(MergeConflict::DifferingDispatchParents {
                    child: edge.child_run_id,
                    first_parent,
                    second_parent,
                });
            }
        } else {
            parent_by_child.insert(edge.child_run_id, edge.parent_run_id);
        }
        pairs.push((edge.parent_run_id, edge.child_run_id));
    }
    if graph_has_cycle(&pairs) {
        Err(MergeConflict::DispatchCycle)
    } else {
        Ok(())
    }
}

/// Preflights one candidate dependency edge against the current live graph.
pub fn preflight_dependency_edge(
    model: &DomainModel,
    candidate: &DependencyEdge,
) -> Result<(), MergeConflict> {
    let mut pairs = Vec::new();
    for edge in model.dependency_edges().chain(std::iter::once(candidate)) {
        if edge.prerequisite_run_id == edge.dependent_run_id {
            return Err(MergeConflict::DependencySelfEdge {
                run: edge.prerequisite_run_id,
            });
        }
        pairs.push((edge.prerequisite_run_id, edge.dependent_run_id));
    }
    if graph_has_cycle(&pairs) {
        Err(MergeConflict::DependencyCycle)
    } else {
        Ok(())
    }
}

fn plan_native_session(
    model: &DomainModel,
    run: RunId,
    provider: Provider,
    sid: &str,
) -> BindingPlan {
    let Some(observed) = model.task_run(&run) else {
        return BindingPlan::Conflict(MergeConflict::MissingRun { run });
    };
    let key = RunKey::Native {
        provider,
        sid: sid.to_owned(),
    };
    match model.task_run_by_key(&key) {
        Some(owner) if owner.run_id == run => BindingPlan::NoChange,
        Some(owner)
            if matches!(owner.key, RunKey::Controller(_))
                || matches!(observed.key, RunKey::Controller(_)) =>
        {
            let (controller, observed) = if matches!(owner.key, RunKey::Controller(_)) {
                (owner.run_id, run)
            } else {
                (run, owner.run_id)
            };
            BindingPlan::Conflict(MergeConflict::ExplicitControllerEvidenceRequired {
                controller,
                observed,
            })
        }
        Some(owner) if matches!(observed.key, RunKey::Provisional { .. }) => {
            planned_merge(model, owner.run_id, run)
        }
        Some(_) => BindingPlan::Conflict(MergeConflict::EvidenceMismatch { run }),
        None if matches!(observed.key, RunKey::Provisional { .. }) => {
            BindingPlan::Bind { run, key }
        }
        None if observed.key == key => BindingPlan::Bind { run, key },
        None if matches!(observed.key, RunKey::Controller(_)) => {
            BindingPlan::Conflict(MergeConflict::ExplicitControllerEvidenceRequired {
                controller: run,
                observed: run,
            })
        }
        None => BindingPlan::Conflict(MergeConflict::EvidenceMismatch { run }),
    }
}

fn plan_controller_native(
    model: &DomainModel,
    controller_run: RunId,
    provider: Provider,
    sid: &str,
) -> BindingPlan {
    let Some(controller) = model.task_run(&controller_run) else {
        return BindingPlan::Conflict(MergeConflict::MissingRun {
            run: controller_run,
        });
    };
    if !matches!(controller.key, RunKey::Controller(_)) {
        return BindingPlan::Conflict(MergeConflict::ControllerRunRequired {
            run: controller_run,
        });
    }
    let key = RunKey::Native {
        provider,
        sid: sid.to_owned(),
    };
    match model.task_run_by_key(&key) {
        Some(owner) if owner.run_id == controller_run => BindingPlan::NoChange,
        Some(owner) if matches!(owner.key, RunKey::Controller(_)) => {
            BindingPlan::Conflict(MergeConflict::NativeSessionAlreadyBound {
                provider,
                sid: sid.to_owned(),
                owner: owner.run_id,
                claimant: controller_run,
            })
        }
        Some(owner) => planned_merge(model, controller_run, owner.run_id),
        None => match single_native_binding(model, controller_run) {
            Err(conflict) => BindingPlan::Conflict(conflict),
            Ok(Some((bound_provider, bound_sid)))
                if bound_provider != provider || bound_sid != sid =>
            {
                BindingPlan::Conflict(MergeConflict::NativeSessionRebind {
                    run: controller_run,
                })
            }
            Ok(_) => BindingPlan::Bind {
                run: controller_run,
                key,
            },
        },
    }
}

fn plan_controller_terminal(
    model: &DomainModel,
    controller_run: RunId,
    terminal_id: &str,
) -> BindingPlan {
    let Some(controller) = model.task_run(&controller_run) else {
        return BindingPlan::Conflict(MergeConflict::MissingRun {
            run: controller_run,
        });
    };
    if !matches!(controller.key, RunKey::Controller(_)) {
        return BindingPlan::Conflict(MergeConflict::ControllerRunRequired {
            run: controller_run,
        });
    }

    let mut live_runs: Vec<_> = model
        .executions()
        .filter(|execution| execution.terminal_id == terminal_id && !execution.state.is_terminal())
        .map(|execution| execution.task_run_id)
        .collect();
    live_runs.sort_unstable();
    live_runs.dedup();
    match live_runs.as_slice() {
        [] => BindingPlan::NoChange,
        [run] if *run == controller_run => BindingPlan::NoChange,
        [run] => match model.task_run(run) {
            None => BindingPlan::Conflict(MergeConflict::MissingRun { run: *run }),
            Some(target) if matches!(target.key, RunKey::Controller(_)) => {
                BindingPlan::Conflict(MergeConflict::ControllerIdentityAlreadyBound {
                    owner: *run,
                    claimant: controller_run,
                })
            }
            Some(_) => planned_merge(model, controller_run, *run),
        },
        _ => BindingPlan::Conflict(MergeConflict::AmbiguousTerminal {
            terminal_id: terminal_id.to_owned(),
            runs: live_runs,
        }),
    }
}

fn planned_merge(model: &DomainModel, survivor: RunId, absorbed: RunId) -> BindingPlan {
    match preflight_merge(model, survivor, absorbed) {
        Ok(_) => BindingPlan::Merge { survivor, absorbed },
        Err(conflict) => BindingPlan::Conflict(conflict),
    }
}

fn apply_bind(
    model: &mut DomainModel,
    run: RunId,
    key: RunKey,
    receipt_time_ms: i64,
) -> Result<PersistBatch, MergeConflict> {
    let (provider, sid) = match &key {
        RunKey::Native { provider, sid } => (*provider, sid.clone()),
        _ => return Err(MergeConflict::UnsupportedBindingKey),
    };
    let task_run = model
        .task_run(&run)
        .cloned()
        .ok_or(MergeConflict::MissingRun { run })?;
    if let Some(owner) = model.task_run_by_key(&key) {
        if owner.run_id == run {
            return Ok(Vec::new());
        }
        return Err(MergeConflict::KeyAlreadyBound {
            owner: owner.run_id,
            claimant: run,
        });
    }
    if let Some((bound_provider, bound_sid)) = single_native_binding(model, run)?
        && (bound_provider != provider || bound_sid != sid)
    {
        return Err(MergeConflict::NativeSessionRebind { run });
    }

    let mut promoted_from = None;
    let persisted_task_run = match &task_run.key {
        RunKey::Controller(_) => {
            model.insert_task_run_alias(key, run);
            task_run
        }
        RunKey::Native {
            provider: current_provider,
            sid: current_sid,
        } if *current_provider == provider && current_sid == &sid => task_run,
        RunKey::Native { .. } => {
            return Err(MergeConflict::EvidenceMismatch { run });
        }
        RunKey::NativePath { .. } | RunKey::Provisional { .. } => {
            let old_key = task_run.key.clone();
            let mut promoted = task_run;
            promoted.key = key.clone();
            model.insert_task_run(promoted.clone());
            model.insert_task_run_alias(old_key.clone(), run);
            promoted_from = Some(old_key);
            promoted
        }
    };
    let persisted = PersistTaskRun {
        task_run: persisted_task_run,
        native_session: Some(NativeSessionBinding {
            provider,
            native_session_id: sid,
        }),
        created_at_ms: receipt_time_ms,
        updated_at_ms: receipt_time_ms,
        finished_at_ms: None,
    };
    Ok(match promoted_from {
        Some(old_key @ RunKey::NativePath { .. }) => vec![PersistOp::PromoteTaskRunKey {
            promoted: persisted,
            old_key,
            alias_run_id: RunId::new(),
        }],
        Some(_) | None => vec![PersistOp::UpsertTaskRun(persisted)],
    })
}

fn unix_now_ms() -> i64 {
    let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    elapsed.as_millis().min(i64::MAX as u128) as i64
}

struct ContractedGraphs {
    execution_edges: HashSet<ExecutionEdge>,
    dependency_edges: HashSet<DependencyEdge>,
}

fn preflight_merge(
    model: &DomainModel,
    survivor: RunId,
    absorbed: RunId,
) -> Result<ContractedGraphs, MergeConflict> {
    let survivor_run = model
        .task_run(&survivor)
        .ok_or(MergeConflict::MissingRun { run: survivor })?;
    let absorbed_run = model
        .task_run(&absorbed)
        .ok_or(MergeConflict::MissingRun { run: absorbed })?;
    if survivor == absorbed {
        return Err(MergeConflict::SameRun { run: survivor });
    }
    if key_priority(&survivor_run.key) < key_priority(&absorbed_run.key) {
        return Err(MergeConflict::LowerPrioritySurvivor { survivor, absorbed });
    }
    if matches!(survivor_run.key, RunKey::Controller(_))
        && matches!(absorbed_run.key, RunKey::Controller(_))
    {
        return Err(MergeConflict::ControllerIdentityAlreadyBound {
            owner: absorbed,
            claimant: survivor,
        });
    }
    let survivor_binding = single_native_binding(model, survivor)?;
    let absorbed_binding = single_native_binding(model, absorbed)?;
    if let (Some(left), Some(right)) = (&survivor_binding, &absorbed_binding) {
        if left != right {
            return Err(MergeConflict::DifferingNativeSessions { survivor, absorbed });
        }
        return Err(MergeConflict::NativeSessionAlreadyBound {
            provider: left.0,
            sid: left.1.clone(),
            owner: survivor,
            claimant: absorbed,
        });
    }

    let execution_edges = contract_execution_edges(model, survivor, absorbed)?;
    let dependency_edges = contract_dependency_edges(model, survivor, absorbed)?;
    Ok(ContractedGraphs {
        execution_edges,
        dependency_edges,
    })
}

fn key_priority(key: &RunKey) -> u8 {
    match key {
        RunKey::Controller(_) => 3,
        RunKey::Native { .. } => 2,
        RunKey::NativePath { .. } => 1,
        RunKey::Provisional { .. } => 0,
    }
}

fn single_native_binding(
    model: &DomainModel,
    run: RunId,
) -> Result<Option<(Provider, String)>, MergeConflict> {
    let mut bindings = HashSet::new();
    if let Some(task_run) = model.task_run(&run)
        && let RunKey::Native { provider, sid } = &task_run.key
    {
        bindings.insert((*provider, sid.clone()));
    }
    for (key, owner) in model.task_run_bindings() {
        if *owner == run
            && let RunKey::Native { provider, sid } = key
        {
            bindings.insert((*provider, sid.clone()));
        }
    }
    if bindings.len() > 1 {
        return Err(MergeConflict::MultipleNativeSessions { run });
    }
    Ok(bindings.into_iter().next())
}

fn contract_execution_edges(
    model: &DomainModel,
    survivor: RunId,
    absorbed: RunId,
) -> Result<HashSet<ExecutionEdge>, MergeConflict> {
    let mut contracted = HashSet::new();
    let mut parent_by_child = HashMap::new();
    for edge in model.execution_edges() {
        let parent = substitute(edge.parent_run_id, survivor, absorbed);
        let child = substitute(edge.child_run_id, survivor, absorbed);
        if parent == child {
            return Err(MergeConflict::DispatchSelfEdge { run: parent });
        }
        if let Some(existing_parent) = parent_by_child.get(&child) {
            if *existing_parent != parent {
                let (first_parent, second_parent) = sorted_pair(*existing_parent, parent);
                return Err(MergeConflict::DifferingDispatchParents {
                    child,
                    first_parent,
                    second_parent,
                });
            }
        } else {
            parent_by_child.insert(child, parent);
        }
        contracted.insert(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: child,
        });
    }
    let pairs: Vec<_> = contracted
        .iter()
        .map(|edge| (edge.parent_run_id, edge.child_run_id))
        .collect();
    if graph_has_cycle(&pairs) {
        return Err(MergeConflict::DispatchCycle);
    }
    Ok(contracted)
}

fn contract_dependency_edges(
    model: &DomainModel,
    survivor: RunId,
    absorbed: RunId,
) -> Result<HashSet<DependencyEdge>, MergeConflict> {
    let mut contracted = HashSet::new();
    for edge in model.dependency_edges() {
        let prerequisite = substitute(edge.prerequisite_run_id, survivor, absorbed);
        let dependent = substitute(edge.dependent_run_id, survivor, absorbed);
        if prerequisite == dependent {
            return Err(MergeConflict::DependencySelfEdge { run: prerequisite });
        }
        contracted.insert(DependencyEdge {
            prerequisite_run_id: prerequisite,
            dependent_run_id: dependent,
        });
    }
    let pairs: Vec<_> = contracted
        .iter()
        .map(|edge| (edge.prerequisite_run_id, edge.dependent_run_id))
        .collect();
    if graph_has_cycle(&pairs) {
        return Err(MergeConflict::DependencyCycle);
    }
    Ok(contracted)
}

fn graph_has_cycle(edges: &[(RunId, RunId)]) -> bool {
    let mut indegrees = HashMap::<RunId, usize>::new();
    let mut children = HashMap::<RunId, Vec<RunId>>::new();
    for (parent, child) in edges {
        indegrees.entry(*parent).or_insert(0);
        *indegrees.entry(*child).or_insert(0) += 1;
        children.entry(*parent).or_default().push(*child);
    }
    let mut queue: VecDeque<_> = indegrees
        .iter()
        .filter_map(|(run, degree)| (*degree == 0).then_some(*run))
        .collect();
    let mut visited = 0;
    while let Some(run) = queue.pop_front() {
        visited += 1;
        if let Some(next_runs) = children.get(&run) {
            for next in next_runs {
                if let Some(degree) = indegrees.get_mut(next) {
                    if *degree == 0 {
                        continue;
                    }
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(*next);
                    }
                }
            }
        }
    }
    visited != indegrees.len()
}

fn substitute(run: RunId, survivor: RunId, absorbed: RunId) -> RunId {
    if run == absorbed { survivor } else { run }
}

fn sorted_pair(first: RunId, second: RunId) -> (RunId, RunId) {
    if first <= second {
        (first, second)
    } else {
        (second, first)
    }
}

fn merge_in_memory(
    model: &mut DomainModel,
    survivor: RunId,
    absorbed: RunId,
    contracted: ContractedGraphs,
) {
    let mut aliases: Vec<_> = model
        .task_run_bindings()
        .filter_map(|(key, owner)| (*owner == absorbed).then_some(key.clone()))
        .collect();
    if let Some(task_run) = model.task_run(&absorbed)
        && !aliases.contains(&task_run.key)
    {
        aliases.push(task_run.key.clone());
    }

    model.remove_task_run_record(&absorbed);
    for execution in model.executions_mut() {
        if execution.task_run_id == absorbed {
            execution.task_run_id = survivor;
        }
    }
    for node in model.agent_nodes_mut() {
        if node.task_run_id == absorbed {
            node.task_run_id = survivor;
        }
    }
    *model.execution_edges_mut() = contracted.execution_edges;
    *model.dependency_edges_mut() = contracted.dependency_edges;
    for alias in aliases {
        model.insert_task_run_alias(alias, survivor);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::lockfile::StateRoot;
    use crate::model::{
        AgentNode, DependencyEdge, DisplayOrdinal, ExecState, Execution, ExecutionEdge, TaskRun,
        TaskState,
    };
    use crate::store::{NativeSessionBinding, PersistOp, PersistTaskRun, open_writer};

    use super::*;

    fn provisional(terminal_id: &str, seq: u64) -> RunKey {
        RunKey::Provisional {
            terminal_id: terminal_id.to_owned(),
            start_ms: 1_000,
            seq,
        }
    }

    fn native(provider: Provider, sid: &str) -> RunKey {
        RunKey::Native {
            provider,
            sid: sid.to_owned(),
        }
    }

    fn insert_run(model: &mut DomainModel, key: RunKey, ordinal: i64) -> RunId {
        let run_id = RunId::new();
        model.insert_task_run(TaskRun {
            run_id,
            key,
            display_ordinal: DisplayOrdinal::new(ordinal),
            state: TaskState::Running,
            has_controller_task_state_event: false,
        });
        run_id
    }

    fn native_evidence(run: RunId, sid: &str) -> BindingEvidence {
        BindingEvidence::NativeSession {
            run,
            provider: Provider::Codex,
            sid: sid.to_owned(),
        }
    }

    #[test]
    fn native_path_resolved_removed_compiles() {
        fn assert_exhaustive(evidence: BindingEvidence) {
            match evidence {
                BindingEvidence::NativeSession { .. }
                | BindingEvidence::ControllerNativeSession { .. }
                | BindingEvidence::ControllerTerminal { .. } => {}
            }
        }

        assert_exhaustive(BindingEvidence::NativeSession {
            run: RunId::new(),
            provider: Provider::Codex,
            sid: "native-sid".to_owned(),
        });
        assert!(matches!(
            RunKey::NativePath {
                provider: Provider::Claude,
                path: "/sessions/pending.jsonl".to_owned(),
            },
            RunKey::NativePath { .. }
        ));
    }

    fn merge_fixture() -> (DomainModel, RunId, RunId) {
        let mut model = DomainModel::default();
        let survivor = insert_run(&mut model, native(Provider::Codex, "sid-1"), 1);
        let absorbed = insert_run(&mut model, provisional("terminal-1", 1), 2);
        (model, survivor, absorbed)
    }

    fn model_fingerprint(model: &DomainModel) -> Vec<String> {
        let mut values = Vec::new();
        values.extend(model.task_runs().map(|run| format!("run:{run:?}")));
        values.extend(
            model
                .executions()
                .map(|execution| format!("exec:{execution:?}")),
        );
        values.extend(model.agent_nodes().map(|node| format!("node:{node:?}")));
        values.extend(
            model
                .execution_edges()
                .map(|edge| format!("dispatch:{edge:?}")),
        );
        values.extend(
            model
                .dependency_edges()
                .map(|edge| format!("dependency:{edge:?}")),
        );
        values.sort();
        values
    }

    fn persisted_run(run_id: RunId, controller_id: &str, ordinal: i64, sid: &str) -> PersistOp {
        PersistOp::UpsertTaskRun(PersistTaskRun {
            task_run: TaskRun {
                run_id,
                key: RunKey::Controller(controller_id.to_owned()),
                display_ordinal: DisplayOrdinal::new(ordinal),
                state: TaskState::Running,
                has_controller_task_state_event: true,
            },
            native_session: Some(NativeSessionBinding {
                provider: Provider::Codex,
                native_session_id: sid.to_owned(),
            }),
            created_at_ms: 1_000,
            updated_at_ms: 1_000,
            finished_at_ms: None,
        })
    }

    #[test]
    fn k3_merges_into_k2_on_sid() {
        let (mut model, survivor, absorbed) = merge_fixture();

        let plan = plan_binding(&model, &native_evidence(absorbed, "sid-1"));

        assert_eq!(plan, BindingPlan::Merge { survivor, absorbed });
        assert_eq!(
            apply_binding_plan(&mut model, plan).unwrap(),
            vec![PersistOp::MergeTaskRuns { survivor, absorbed }]
        );
        assert!(model.task_run(&absorbed).is_none());
        assert_eq!(
            model
                .task_run_by_key(&provisional("terminal-1", 1))
                .unwrap()
                .run_id,
            survivor
        );
    }

    #[test]
    fn k3_or_k2_merge_into_k1_with_explicit_evidence_only() {
        let mut model = DomainModel::default();
        let k1_native = insert_run(
            &mut model,
            RunKey::Controller("controller-native".to_owned()),
            1,
        );
        let k2 = insert_run(&mut model, native(Provider::Codex, "sid-1"), 2);

        assert_eq!(
            plan_binding(&model, &native_evidence(k2, "sid-1")),
            BindingPlan::NoChange
        );
        assert_eq!(
            plan_binding(
                &model,
                &BindingEvidence::ControllerNativeSession {
                    controller_run: k1_native,
                    provider: Provider::Codex,
                    sid: "sid-1".to_owned(),
                }
            ),
            BindingPlan::Merge {
                survivor: k1_native,
                absorbed: k2,
            }
        );

        let k1_terminal = insert_run(
            &mut model,
            RunKey::Controller("controller-terminal".to_owned()),
            3,
        );
        let k3 = insert_run(&mut model, provisional("terminal-3", 3), 4);
        model.insert_execution(Execution {
            execution_id: "live-k3".to_owned(),
            pane_id: "pane-3".to_owned(),
            terminal_id: "terminal-3".to_owned(),
            task_run_id: k3,
            state: ExecState::Working,
        });
        assert_eq!(
            plan_binding(
                &model,
                &BindingEvidence::ControllerTerminal {
                    controller_run: k1_terminal,
                    terminal_id: "terminal-3".to_owned(),
                }
            ),
            BindingPlan::Merge {
                survivor: k1_terminal,
                absorbed: k3,
            }
        );
    }

    #[test]
    fn two_k3_merge_via_same_native_sid() {
        let mut model = DomainModel::default();
        let first = insert_run(&mut model, provisional("terminal-1", 1), 1);
        let second = insert_run(&mut model, provisional("terminal-2", 2), 2);

        let first_plan = plan_binding(&model, &native_evidence(first, "shared-sid"));
        assert_eq!(
            first_plan,
            BindingPlan::Bind {
                run: first,
                key: native(Provider::Codex, "shared-sid"),
            }
        );
        apply_binding_plan(&mut model, first_plan).unwrap();

        let second_plan = plan_binding(&model, &native_evidence(second, "shared-sid"));
        assert_eq!(
            second_plan,
            BindingPlan::Merge {
                survivor: first,
                absorbed: second,
            }
        );
        apply_binding_plan(&mut model, second_plan).unwrap();
        assert_eq!(model.task_runs().count(), 1);
    }

    #[test]
    fn preflight_defers_self_edge() {
        let (mut model, survivor, absorbed) = merge_fixture();
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: survivor,
            child_run_id: absorbed,
        });

        assert!(matches!(
            plan_binding(&model, &native_evidence(absorbed, "sid-1")),
            BindingPlan::Conflict(MergeConflict::DispatchSelfEdge { run }) if run == survivor
        ));
    }

    #[test]
    fn preflight_defers_dependency_cycle() {
        let (mut model, survivor, absorbed) = merge_fixture();
        let middle = insert_run(&mut model, provisional("middle", 3), 3);
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: survivor,
            dependent_run_id: middle,
        });
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: middle,
            dependent_run_id: absorbed,
        });

        assert!(matches!(
            plan_binding(&model, &native_evidence(absorbed, "sid-1")),
            BindingPlan::Conflict(MergeConflict::DependencyCycle)
        ));
    }

    #[test]
    fn preflight_defers_dispatch_cycle() {
        let (mut model, survivor, absorbed) = merge_fixture();
        let middle = insert_run(&mut model, provisional("middle", 3), 3);
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: survivor,
            child_run_id: middle,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: middle,
            child_run_id: absorbed,
        });

        assert!(matches!(
            plan_binding(&model, &native_evidence(absorbed, "sid-1")),
            BindingPlan::Conflict(MergeConflict::DispatchCycle)
        ));
    }

    #[test]
    fn preflight_defers_differing_dispatch_parents() {
        let (mut model, survivor, absorbed) = merge_fixture();
        let first_parent = insert_run(&mut model, provisional("parent-1", 3), 3);
        let second_parent = insert_run(&mut model, provisional("parent-2", 4), 4);
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: first_parent,
            child_run_id: survivor,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: second_parent,
            child_run_id: absorbed,
        });

        assert!(matches!(
            plan_binding(&model, &native_evidence(absorbed, "sid-1")),
            BindingPlan::Conflict(MergeConflict::DifferingDispatchParents { child, .. })
                if child == survivor
        ));
    }

    #[test]
    fn live_graph_preflight_detects_self_and_multihop_cycle() {
        let mut model = DomainModel::default();
        let first = insert_run(&mut model, provisional("first", 1), 1);
        let second = insert_run(&mut model, provisional("second", 2), 2);
        let third = insert_run(&mut model, provisional("third", 3), 3);
        assert!(matches!(
            preflight_execution_edge(
                &model,
                &ExecutionEdge {
                    parent_run_id: first,
                    child_run_id: first,
                }
            ),
            Err(MergeConflict::DispatchSelfEdge { run }) if run == first
        ));

        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: first,
            child_run_id: second,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: second,
            child_run_id: third,
        });
        assert_eq!(
            preflight_execution_edge(
                &model,
                &ExecutionEdge {
                    parent_run_id: third,
                    child_run_id: first,
                }
            ),
            Err(MergeConflict::DispatchCycle)
        );

        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: first,
            dependent_run_id: second,
        });
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: second,
            dependent_run_id: third,
        });
        assert_eq!(
            preflight_dependency_edge(
                &model,
                &DependencyEdge {
                    prerequisite_run_id: third,
                    dependent_run_id: first,
                }
            ),
            Err(MergeConflict::DependencyCycle)
        );
    }

    #[test]
    fn single_parent_carry_over_merges_clean() {
        let (mut model, survivor, absorbed) = merge_fixture();
        let parent = insert_run(&mut model, provisional("parent", 3), 3);
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: absorbed,
        });

        let plan = plan_binding(&model, &native_evidence(absorbed, "sid-1"));
        assert_eq!(plan, BindingPlan::Merge { survivor, absorbed });
        apply_binding_plan(&mut model, plan).unwrap();

        assert_eq!(
            model.execution_edges().collect::<HashSet<_>>(),
            HashSet::from([&ExecutionEdge {
                parent_run_id: parent,
                child_run_id: survivor,
            }])
        );
    }

    #[test]
    fn alias_resolves_before_dedup_and_placeholders() {
        let (mut model, survivor, absorbed) = merge_fixture();
        let alias = provisional("terminal-1", 1);
        let plan = plan_binding(&model, &native_evidence(absorbed, "sid-1"));
        apply_binding_plan(&mut model, plan).unwrap();

        let resolved = model.task_run_by_key(&alias).map(|run| run.run_id);
        let would_create_placeholder = resolved.is_none();

        assert_eq!(resolved, Some(survivor));
        assert!(!would_create_placeholder);
        assert_eq!(model.task_runs().count(), 1);
    }

    #[test]
    fn k1_uniqueness_enforced_in_db_and_memory() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let first = RunId::new();
        let second = RunId::new();
        store
            .apply_batch(vec![persisted_run(first, "first", 1, "one-sid")])
            .unwrap();
        assert!(
            store
                .apply_batch(vec![persisted_run(second, "second", 2, "one-sid")])
                .is_err()
        );

        let mut model = DomainModel::default();
        let first = insert_run(&mut model, RunKey::Controller("first".to_owned()), 1);
        let second = insert_run(&mut model, RunKey::Controller("second".to_owned()), 2);
        apply_binding_plan(
            &mut model,
            BindingPlan::Bind {
                run: first,
                key: native(Provider::Codex, "one-sid"),
            },
        )
        .unwrap();

        assert!(matches!(
            plan_binding(
                &model,
                &BindingEvidence::ControllerNativeSession {
                    controller_run: second,
                    provider: Provider::Codex,
                    sid: "one-sid".to_owned(),
                }
            ),
            BindingPlan::Conflict(MergeConflict::NativeSessionAlreadyBound {
                owner,
                claimant,
                ..
            }) if owner == first && claimant == second
        ));
    }

    #[test]
    fn merge_repoints_both_edge_sets_atomically() {
        let (mut model, survivor, absorbed) = merge_fixture();
        let parent = insert_run(&mut model, provisional("parent", 3), 3);
        let dependent = insert_run(&mut model, provisional("dependent", 4), 4);
        model.insert_execution(Execution {
            execution_id: "execution".to_owned(),
            pane_id: "pane".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            task_run_id: absorbed,
            state: ExecState::Working,
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "node".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("sid-1".to_owned()),
            task_run_id: absorbed,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: absorbed,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: survivor,
        });
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: absorbed,
            dependent_run_id: dependent,
        });
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: survivor,
            dependent_run_id: dependent,
        });

        let plan = plan_binding(&model, &native_evidence(absorbed, "sid-1"));
        let batch = apply_binding_plan(&mut model, plan).unwrap();

        assert_eq!(batch, vec![PersistOp::MergeTaskRuns { survivor, absorbed }]);
        assert_eq!(model.execution("execution").unwrap().task_run_id, survivor);
        assert_eq!(model.agent_node("node").unwrap().task_run_id, survivor);
        assert_eq!(model.execution_edges().count(), 1);
        assert_eq!(model.dependency_edges().count(), 1);
        assert!(
            model
                .execution_edges()
                .any(|edge| { edge.parent_run_id == parent && edge.child_run_id == survivor })
        );
        assert!(model.dependency_edges().any(|edge| {
            edge.prerequisite_run_id == survivor && edge.dependent_run_id == dependent
        }));
    }

    #[test]
    fn terminal_identity_selects_live_execution_only() {
        let mut model = DomainModel::default();
        let controller = insert_run(&mut model, RunKey::Controller("controller".to_owned()), 1);
        let ended = insert_run(&mut model, provisional("terminal-1", 2), 2);
        let live = insert_run(&mut model, provisional("terminal-1", 3), 3);
        model.insert_execution(Execution {
            execution_id: "ended".to_owned(),
            pane_id: "old-pane".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            task_run_id: ended,
            state: ExecState::Ended,
        });
        model.insert_execution(Execution {
            execution_id: "live".to_owned(),
            pane_id: "live-pane".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            task_run_id: live,
            state: ExecState::Working,
        });

        assert_eq!(
            plan_binding(
                &model,
                &BindingEvidence::ControllerTerminal {
                    controller_run: controller,
                    terminal_id: "terminal-1".to_owned(),
                }
            ),
            BindingPlan::Merge {
                survivor: controller,
                absorbed: live,
            }
        );
    }

    #[test]
    fn conflict_leaves_model_and_batch_untouched() {
        let (mut model, survivor, absorbed) = merge_fixture();
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: survivor,
            dependent_run_id: absorbed,
        });
        let before = model_fingerprint(&model);
        let plan = plan_binding(&model, &native_evidence(absorbed, "sid-1"));
        let conflict = match plan {
            BindingPlan::Conflict(conflict) => conflict,
            other => panic!("expected conflict, got {other:?}"),
        };

        let result = apply_binding_plan(&mut model, BindingPlan::Conflict(conflict.clone()));

        assert_eq!(result, Err(conflict));
        assert_eq!(model_fingerprint(&model), before);
    }

    #[test]
    fn native_binding_round_trips_restore_without_unique_collision() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let run_id = RunId::new();
        store
            .apply_batch(vec![persisted_run(
                run_id,
                "controller-native",
                1,
                "round-trip-sid",
            )])
            .unwrap();

        let restored = store.load_restored_state().unwrap();
        let native_key = native(Provider::Codex, "round-trip-sid");
        assert_eq!(
            restored
                .model
                .task_run_by_key(&native_key)
                .expect("native binding must be restored as an alias")
                .run_id,
            run_id
        );
        assert_eq!(
            plan_binding(
                &restored.model,
                &BindingEvidence::NativeSession {
                    run: run_id,
                    provider: Provider::Codex,
                    sid: "round-trip-sid".to_owned(),
                },
            ),
            BindingPlan::NoChange
        );
    }
}
