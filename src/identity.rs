//! T6 binding evidence, `BindingPlan`, and `plan_binding`/`apply_binding_plan` merge machinery.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::model::{DependencyEdge, DomainModel, ExecutionEdge, Provider, RunId, RunKey, TaskRun};
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
    /// A globally unambiguous sessionless Codex pane was matched to one rollout.
    HeuristicNativeSession {
        /// The still-provisional pane run receiving the one-shot binding.
        run: RunId,
        /// Provider-native Codex rollout ID selected by discovery.
        sid: String,
    },
    /// A provider adapter resolved a path-keyed run to its native session ID.
    NativePathResolved {
        /// The path-keyed run whose provider file supplied the evidence.
        run: RunId,
        /// The provider that owns both the path and session namespaces.
        provider: Provider,
        /// The provider-native session ID resolved from the file.
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
        BindingEvidence::HeuristicNativeSession { run, sid } => {
            plan_heuristic_native_session(model, *run, sid)
        }
        BindingEvidence::NativePathResolved { run, provider, sid } => {
            plan_native_path_resolution(model, *run, *provider, sid)
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

fn plan_heuristic_native_session(model: &DomainModel, run: RunId, sid: &str) -> BindingPlan {
    let Some(observed) = model.task_run(&run) else {
        return BindingPlan::Conflict(MergeConflict::MissingRun { run });
    };
    match &observed.key {
        RunKey::Provisional { .. } => {
            let key = RunKey::Native {
                provider: Provider::Codex,
                sid: sid.to_owned(),
            };
            match model.task_run_by_key(&key) {
                Some(owner) => BindingPlan::Conflict(MergeConflict::NativeSessionAlreadyBound {
                    provider: Provider::Codex,
                    sid: sid.to_owned(),
                    owner: owner.run_id,
                    claimant: run,
                }),
                None => plan_native_session(model, run, Provider::Codex, sid),
            }
        }
        RunKey::Native {
            provider,
            sid: bound,
        } if *provider == Provider::Codex && bound == sid => BindingPlan::NoChange,
        RunKey::Controller(_) | RunKey::Native { .. } | RunKey::NativePath { .. } => {
            BindingPlan::Conflict(MergeConflict::EvidenceMismatch { run })
        }
    }
}

fn plan_native_path_resolution(
    model: &DomainModel,
    run: RunId,
    provider: Provider,
    sid: &str,
) -> BindingPlan {
    let Some(observed) = model.task_run(&run) else {
        return BindingPlan::Conflict(MergeConflict::MissingRun { run });
    };
    if !matches!(observed.key, RunKey::NativePath { provider: current, .. } if current == provider)
    {
        return BindingPlan::Conflict(MergeConflict::EvidenceMismatch { run });
    }
    let key = RunKey::Native {
        provider,
        sid: sid.to_owned(),
    };
    match model.task_run_by_key(&key) {
        Some(owner) if owner.run_id == run => BindingPlan::NoChange,
        Some(owner) => planned_merge(model, owner.run_id, run),
        None => BindingPlan::Bind { run, key },
    }
}

/// Applies the sole public identity mutation path and returns its durable half.
///
/// Reducers resolve aliases inside Controller validation against current model state before
/// staging any placeholder Task Runs. Event-ledger lookup is keyed by `event_id` alone, so its
/// ordering relative to alias resolution is unobservable.
pub fn apply_binding_plan(
    model: &mut DomainModel,
    plan: BindingPlan,
) -> Result<PersistBatch, MergeConflict> {
    apply_binding_plan_at(model, plan, unix_now_ms())
}

/// Applies a binding plan using the selected bookkeeping time for persistence.
pub fn apply_binding_plan_at(
    model: &mut DomainModel,
    plan: BindingPlan,
    bookkeeping_time_ms: i64,
) -> Result<PersistBatch, MergeConflict> {
    match plan {
        BindingPlan::NoChange => Ok(Vec::new()),
        BindingPlan::Conflict(conflict) => Err(conflict),
        BindingPlan::Bind { run, key } => apply_bind(model, run, key, bookkeeping_time_ms),
        BindingPlan::Merge { survivor, absorbed } => {
            let contracted = preflight_merge(model, survivor, absorbed)?;
            merge_in_memory(model, survivor, absorbed, contracted, bookkeeping_time_ms);
            let mut task_run = model
                .task_run(&survivor)
                .cloned()
                .ok_or(MergeConflict::MissingRun { run: survivor })?;
            touch_task_run(&mut task_run, bookkeeping_time_ms);
            model.insert_task_run(task_run.clone());
            let native_session =
                single_native_binding(model, survivor)?.map(|(provider, native_session_id)| {
                    NativeSessionBinding {
                        provider,
                        native_session_id,
                    }
                });
            let created_at_ms = task_run.created_at_ms.unwrap_or(bookkeeping_time_ms);
            let updated_at_ms = task_run.updated_at_ms.unwrap_or(bookkeeping_time_ms);
            let finished_at_ms = task_run.finished_at_ms;
            Ok(vec![
                PersistOp::MergeTaskRuns { survivor, absorbed },
                PersistOp::UpsertTaskRun(PersistTaskRun {
                    task_run,
                    native_session,
                    created_at_ms,
                    updated_at_ms,
                    finished_at_ms,
                }),
            ])
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
    bookkeeping_time_ms: i64,
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
    let mut persisted_task_run = match &task_run.key {
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
            promoted_from = Some(old_key);
            promoted
        }
    };
    touch_task_run(&mut persisted_task_run, bookkeeping_time_ms);
    model.insert_task_run(persisted_task_run.clone());
    if let Some(old_key) = promoted_from.as_ref() {
        model.insert_task_run_alias(old_key.clone(), run);
    }
    let created_at_ms = persisted_task_run
        .created_at_ms
        .unwrap_or(bookkeeping_time_ms);
    let updated_at_ms = persisted_task_run
        .updated_at_ms
        .unwrap_or(bookkeeping_time_ms);
    let finished_at_ms = persisted_task_run.finished_at_ms;
    let persisted = PersistTaskRun {
        task_run: persisted_task_run,
        native_session: Some(NativeSessionBinding {
            provider,
            native_session_id: sid,
        }),
        created_at_ms,
        updated_at_ms,
        finished_at_ms,
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
    bookkeeping_time_ms: i64,
) {
    let absorbed_has_controller_evidence = model
        .task_run(&absorbed)
        .is_some_and(|task_run| task_run.has_controller_task_state_event);
    if absorbed_has_controller_evidence
        && let Some(mut task_run) = model.task_run(&survivor).cloned()
    {
        task_run.has_controller_task_state_event = true;
        touch_task_run(&mut task_run, bookkeeping_time_ms);
        model.insert_task_run(task_run);
    }
    let mut aliases: Vec<_> = model
        .task_run_bindings()
        .filter_map(|(key, owner)| (*owner == absorbed).then_some(key.clone()))
        .collect();
    if let Some(task_run) = model.task_run(&absorbed)
        && !aliases.contains(&task_run.key)
    {
        aliases.push(task_run.key.clone());
    }

    // The identity proof makes this fold double-count-safe: there is one telemetry key per
    // scope, the lane is the sole Telemetry emitter, its (scope, sample_id) deduplication lasts
    // for the process lifetime, and merged runs have disjoint ScopeKeys.
    model.fold_telemetry(survivor, absorbed);
    model.fold_run_kind(survivor, absorbed);
    model.fold_task_run_v6_state(survivor, absorbed);
    model.fold_run_rate_totals(survivor, absorbed);
    // Rate cursors are deliberately process-local and are not part of DomainModel persistence.
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

fn touch_task_run(task_run: &mut TaskRun, bookkeeping_time_ms: i64) {
    task_run.touch(
        task_run
            .updated_at_ms
            .unwrap_or(bookkeeping_time_ms)
            .max(bookkeeping_time_ms),
    );
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::lockfile::StateRoot;
    use crate::model::{
        AgentNode, DependencyEdge, DisplayOrdinal, ExecState, Execution, ExecutionEdge,
        HistoryDrainId, NativeLifecycleWatermark, NativeSessionEnd, NativeSessionEndStatus,
        RunRateTotals, TaskRun, TaskRunV6State, TaskState, graph::is_relationship_only,
    };
    use crate::store::{
        NativeSessionBinding, PersistHistoryDrain, PersistHistoryDrainRun, PersistOp,
        PersistTaskRun, PersistTaskRunV6, PersistV6Batch, open_reader, open_writer,
    };

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
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
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
    fn merge_controller_evidence_flag_flip_touches_survivor_bookkeeping() {
        let mut model = DomainModel::default();
        let survivor = insert_run(
            &mut model,
            RunKey::Controller("controller-survivor".to_owned()),
            1,
        );
        let absorbed = insert_run(&mut model, native(Provider::Codex, "absorbed-sid"), 2);
        let mut survivor_run = model.task_run(&survivor).cloned().unwrap();
        survivor_run.created_at_ms = Some(100);
        survivor_run.updated_at_ms = Some(500);
        survivor_run.dismissed_at_ms = Some(750);
        model.insert_task_run(survivor_run);
        let mut absorbed_run = model.task_run(&absorbed).cloned().unwrap();
        absorbed_run.has_controller_task_state_event = true;
        model.insert_task_run(absorbed_run);

        let batch =
            apply_binding_plan_at(&mut model, BindingPlan::Merge { survivor, absorbed }, 2_000)
                .unwrap();

        let survivor_run = model.task_run(&survivor).unwrap();
        assert!(survivor_run.has_controller_task_state_event);
        assert_eq!(survivor_run.created_at_ms, Some(100));
        assert_eq!(survivor_run.updated_at_ms, Some(2_000));
        assert_eq!(survivor_run.finished_at_ms, None);
        assert_eq!(survivor_run.dismissed_at_ms, None);
        let PersistOp::UpsertTaskRun(persisted) = &batch[1] else {
            panic!("expected survivor task-run persistence after merge");
        };
        assert_eq!(&persisted.task_run, survivor_run);
        assert_eq!(persisted.created_at_ms, 100);
        assert_eq!(persisted.updated_at_ms, 2_000);
        assert_eq!(persisted.finished_at_ms, None);
    }

    #[test]
    fn older_binding_time_does_not_move_merge_survivor_bookkeeping_backward() {
        let mut model = DomainModel::default();
        let survivor = insert_run(
            &mut model,
            RunKey::Controller("controller-survivor".to_owned()),
            1,
        );
        let absorbed = insert_run(&mut model, native(Provider::Codex, "absorbed-sid"), 2);
        let mut survivor_run = model.task_run(&survivor).cloned().unwrap();
        survivor_run.created_at_ms = Some(100);
        survivor_run.updated_at_ms = Some(2_500);
        model.insert_task_run(survivor_run);
        let mut absorbed_run = model.task_run(&absorbed).cloned().unwrap();
        absorbed_run.has_controller_task_state_event = true;
        model.insert_task_run(absorbed_run);

        let batch =
            apply_binding_plan_at(&mut model, BindingPlan::Merge { survivor, absorbed }, 2_000)
                .unwrap();

        let survivor_run = model.task_run(&survivor).unwrap();
        assert_eq!(survivor_run.updated_at_ms, Some(2_500));
        let PersistOp::UpsertTaskRun(persisted) = &batch[1] else {
            panic!("expected survivor task-run persistence after merge");
        };
        assert_eq!(persisted.updated_at_ms, 2_500);
    }

    #[test]
    fn controller_native_merge_rekeys_lane_telemetry_without_a_phantom_entry() {
        let mut model = DomainModel::default();
        let survivor = insert_run(
            &mut model,
            RunKey::Controller("controller-survivor".to_owned()),
            1,
        );
        let absorbed = insert_run(&mut model, native(Provider::Codex, "lane-rollout"), 2);
        model.telemetry_entry(absorbed, 1_100).accumulate(
            17,
            Some("gpt-5.6-terra".to_owned()),
            Some("high".to_owned()),
            None,
            true,
        );
        model.telemetry_entry(absorbed, 1_100).accumulate(
            25,
            Some("gpt-5.6-sol".to_owned()),
            Some("xhigh".to_owned()),
            None,
            true,
        );

        let plan = plan_binding(
            &model,
            &BindingEvidence::ControllerNativeSession {
                controller_run: survivor,
                provider: Provider::Codex,
                sid: "lane-rollout".to_owned(),
            },
        );
        assert_eq!(plan, BindingPlan::Merge { survivor, absorbed });
        apply_binding_plan_at(&mut model, plan, 2_000).unwrap();

        assert_eq!(
            model.telemetry(&survivor).map(|telemetry| (
                telemetry.output_tokens,
                telemetry.started_wall_ms,
                telemetry.model.as_deref(),
                telemetry.effort.as_deref(),
                telemetry.per_turn.len(),
            )),
            Some((42, 1_100, Some("gpt-5.6-sol"), Some("xhigh"), 2))
        );
        assert!(model.telemetry(&absorbed).is_none());
        assert_eq!(
            model
                .telemetry_entries()
                .map(|(run_id, _)| *run_id)
                .collect::<Vec<_>>(),
            vec![survivor],
            "the absorbed RunId must not survive as a phantom telemetry entry"
        );
    }

    #[test]
    fn merge_telemetry_fold_keeps_canonical_attribution_with_absorbed_fallbacks() {
        let mut model = DomainModel::default();
        let survivor = insert_run(
            &mut model,
            RunKey::Controller("controller-survivor".to_owned()),
            1,
        );
        let absorbed = insert_run(&mut model, native(Provider::Codex, "lane-rollout"), 2);
        model.telemetry_entry(survivor, 1_200).accumulate(
            u64::MAX - 10,
            Some("controller-model".to_owned()),
            None,
            None,
            true,
        );
        model.telemetry_entry(absorbed, 1_100).accumulate(
            17,
            Some("controller-model".to_owned()),
            None,
            None,
            true,
        );
        model.telemetry_entry(absorbed, 1_150).accumulate(
            25,
            Some("gpt-5.6-sol".to_owned()),
            Some("xhigh".to_owned()),
            None,
            true,
        );

        apply_binding_plan_at(&mut model, BindingPlan::Merge { survivor, absorbed }, 2_000)
            .unwrap();

        let telemetry = model.telemetry(&survivor).unwrap();
        assert_eq!(telemetry.output_tokens, u64::MAX);
        assert_eq!(telemetry.started_wall_ms, 1_100);
        assert_eq!(telemetry.model.as_deref(), Some("controller-model"));
        assert_eq!(telemetry.effort.as_deref(), Some("xhigh"));
        assert_eq!(telemetry.per_turn.len(), 2);
        assert_eq!(
            telemetry
                .per_turn
                .iter()
                .map(|attribution| (attribution.model.as_deref(), attribution.effort.as_deref(),))
                .collect::<Vec<_>>(),
            vec![
                (Some("controller-model"), None),
                (Some("gpt-5.6-sol"), Some("xhigh")),
            ],
            "the identical attribution at the fold boundary must stay coalesced"
        );
        assert!(model.telemetry(&absorbed).is_none());
    }

    #[test]
    fn merge_run_kind_keeps_canonical_value_or_adopts_absorbed_value() {
        for (canonical_kind, expected) in [
            (Some("canonical-kind"), "canonical-kind"),
            (None, "absorbed-kind"),
        ] {
            let mut model = DomainModel::default();
            let survivor = insert_run(
                &mut model,
                RunKey::Controller("controller-survivor".to_owned()),
                1,
            );
            let absorbed = insert_run(&mut model, native(Provider::Codex, "lane-rollout"), 2);
            if let Some(kind) = canonical_kind {
                model.set_run_kind(survivor, kind.to_owned());
            }
            model.set_run_kind(absorbed, "absorbed-kind".to_owned());

            apply_binding_plan_at(&mut model, BindingPlan::Merge { survivor, absorbed }, 2_000)
                .unwrap();

            assert_eq!(model.run_kind(&survivor), Some(expected));
            assert_eq!(model.run_kind(&absorbed), None);
        }
    }

    #[test]
    fn merge_survivor_persist_touches_bookkeeping_without_flag_flip() {
        let mut model = DomainModel::default();
        let survivor = insert_run(&mut model, native(Provider::Codex, "survivor-sid"), 1);
        let absorbed = insert_run(&mut model, provisional("absorbed-terminal", 2), 2);
        let mut survivor_run = model.task_run(&survivor).cloned().unwrap();
        survivor_run.created_at_ms = Some(125);
        survivor_run.updated_at_ms = Some(600);
        survivor_run.dismissed_at_ms = Some(800);
        model.insert_task_run(survivor_run);

        let batch =
            apply_binding_plan_at(&mut model, BindingPlan::Merge { survivor, absorbed }, 2_500)
                .unwrap();

        let survivor_run = model.task_run(&survivor).unwrap();
        assert!(!survivor_run.has_controller_task_state_event);
        assert_eq!(survivor_run.created_at_ms, Some(125));
        assert_eq!(survivor_run.updated_at_ms, Some(2_500));
        assert_eq!(survivor_run.finished_at_ms, None);
        assert_eq!(survivor_run.dismissed_at_ms, None);
        let PersistOp::UpsertTaskRun(persisted) = &batch[1] else {
            panic!("expected survivor task-run persistence after merge");
        };
        assert_eq!(&persisted.task_run, survivor_run);
        assert_eq!(persisted.created_at_ms, 125);
        assert_eq!(persisted.updated_at_ms, 2_500);
        assert_eq!(persisted.finished_at_ms, None);
    }

    #[test]
    fn native_path_key_promotion_touches_run_bookkeeping() {
        let mut model = DomainModel::default();
        let run = insert_run(
            &mut model,
            RunKey::NativePath {
                provider: Provider::Codex,
                path: "/sessions/pending.jsonl".to_owned(),
            },
            1,
        );
        let mut task_run = model.task_run(&run).cloned().unwrap();
        task_run.created_at_ms = Some(150);
        task_run.updated_at_ms = Some(700);
        task_run.dismissed_at_ms = Some(900);
        model.insert_task_run(task_run);
        let native_key = native(Provider::Codex, "resolved-sid");

        let batch = apply_binding_plan_at(
            &mut model,
            BindingPlan::Bind {
                run,
                key: native_key.clone(),
            },
            3_000,
        )
        .unwrap();

        let task_run = model.task_run(&run).unwrap();
        assert_eq!(task_run.key, native_key);
        assert_eq!(task_run.created_at_ms, Some(150));
        assert_eq!(task_run.updated_at_ms, Some(3_000));
        assert_eq!(task_run.finished_at_ms, None);
        assert_eq!(task_run.dismissed_at_ms, None);
        let PersistOp::PromoteTaskRunKey { promoted, .. } = &batch[0] else {
            panic!("expected native-path task-run key promotion");
        };
        assert_eq!(&promoted.task_run, task_run);
        assert_eq!(promoted.created_at_ms, 150);
        assert_eq!(promoted.updated_at_ms, 3_000);
        assert_eq!(promoted.finished_at_ms, None);
    }

    #[test]
    fn native_path_resolution_promotes_an_unowned_session_id() {
        fn assert_exhaustive(evidence: BindingEvidence) {
            match evidence {
                BindingEvidence::NativeSession { .. }
                | BindingEvidence::HeuristicNativeSession { .. }
                | BindingEvidence::NativePathResolved { .. }
                | BindingEvidence::ControllerNativeSession { .. }
                | BindingEvidence::ControllerTerminal { .. } => {}
            }
        }

        let mut model = DomainModel::default();
        let run = insert_run(
            &mut model,
            RunKey::NativePath {
                provider: Provider::Codex,
                path: "/sessions/pending.jsonl".to_owned(),
            },
            1,
        );
        let evidence = BindingEvidence::NativePathResolved {
            run,
            provider: Provider::Codex,
            sid: "native-sid".to_owned(),
        };

        assert_exhaustive(evidence.clone());
        assert_eq!(
            plan_binding(&model, &evidence),
            BindingPlan::Bind {
                run,
                key: RunKey::Native {
                    provider: Provider::Codex,
                    sid: "native-sid".to_owned(),
                },
            }
        );
    }

    #[test]
    fn native_path_resolution_merges_into_the_existing_session_owner() {
        let mut model = DomainModel::default();
        let owner = insert_run(&mut model, native(Provider::Claude, "native-sid"), 1);
        let path_run = insert_run(
            &mut model,
            RunKey::NativePath {
                provider: Provider::Claude,
                path: "/sessions/pending.jsonl".to_owned(),
            },
            2,
        );

        assert_eq!(
            plan_binding(
                &model,
                &BindingEvidence::NativePathResolved {
                    run: path_run,
                    provider: Provider::Claude,
                    sid: "native-sid".to_owned(),
                },
            ),
            BindingPlan::Merge {
                survivor: owner,
                absorbed: path_run,
            }
        );
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
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
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
        let batch = apply_binding_plan(&mut model, plan).unwrap();
        assert!(matches!(
            batch.as_slice(),
            [
                PersistOp::MergeTaskRuns {
                    survivor: actual_survivor,
                    absorbed: actual_absorbed,
                },
                PersistOp::UpsertTaskRun(persisted),
            ] if *actual_survivor == survivor
                && *actual_absorbed == absorbed
                && persisted.task_run.run_id == survivor
                && persisted.native_session.as_ref().is_some_and(|binding| {
                    binding.provider == Provider::Codex
                        && binding.native_session_id == "sid-1"
                })
        ));
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
    fn heuristic_native_session_conflicts_with_existing_owner_without_merging() {
        let mut model = DomainModel::default();
        let owner = insert_run(&mut model, native(Provider::Codex, "owned-sid"), 1);
        let claimant = insert_run(&mut model, provisional("claimant-terminal", 2), 2);

        assert_eq!(
            plan_binding(
                &model,
                &BindingEvidence::HeuristicNativeSession {
                    run: claimant,
                    sid: "owned-sid".to_owned(),
                },
            ),
            BindingPlan::Conflict(MergeConflict::NativeSessionAlreadyBound {
                provider: Provider::Codex,
                sid: "owned-sid".to_owned(),
                owner,
                claimant,
            })
        );
        assert_eq!(model.task_runs().count(), 2);
    }

    #[test]
    fn explicit_native_session_merge_allows_live_executions_on_different_terminals() {
        let mut model = DomainModel::default();
        let first = insert_run(&mut model, provisional("terminal-1", 1), 1);
        let second = insert_run(&mut model, provisional("terminal-2", 2), 2);
        model.insert_execution(Execution {
            execution_id: "first-execution".to_owned(),
            pane_id: "first-pane".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            task_run_id: first,
            state: ExecState::Working,
        });
        model.insert_execution(Execution {
            execution_id: "second-execution".to_owned(),
            pane_id: "second-pane".to_owned(),
            terminal_id: "terminal-2".to_owned(),
            task_run_id: second,
            state: ExecState::Working,
        });

        let first_plan = plan_binding(&model, &native_evidence(first, "shared-sid"));
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
        assert!(
            model
                .executions()
                .all(|execution| execution.task_run_id == first)
        );
    }

    #[test]
    fn merge_allows_executed_provisional_to_join_executionless_native_run() {
        let (mut model, survivor, absorbed) = merge_fixture();
        model.insert_execution(Execution {
            execution_id: "provisional-execution".to_owned(),
            pane_id: "provisional-pane".to_owned(),
            terminal_id: "provisional-terminal".to_owned(),
            task_run_id: absorbed,
            state: ExecState::Working,
        });

        let batch =
            apply_binding_plan_at(&mut model, BindingPlan::Merge { survivor, absorbed }, 2_000)
                .unwrap();

        assert!(matches!(
            batch.as_slice(),
            [
                PersistOp::MergeTaskRuns {
                    survivor: actual_survivor,
                    absorbed: actual_absorbed,
                },
                PersistOp::UpsertTaskRun(persisted),
            ] if *actual_survivor == survivor
                && *actual_absorbed == absorbed
                && persisted.task_run.run_id == survivor
        ));
        assert!(model.task_run(&absorbed).is_none());
        assert_eq!(
            model
                .execution("provisional-execution")
                .unwrap()
                .task_run_id,
            survivor
        );
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

        assert!(matches!(
            batch.as_slice(),
            [
                PersistOp::MergeTaskRuns {
                    survivor: actual_survivor,
                    absorbed: actual_absorbed,
                },
                PersistOp::UpsertTaskRun(persisted),
            ] if *actual_survivor == survivor
                && *actual_absorbed == absorbed
                && persisted.task_run.run_id == survivor
        ));
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

    #[test]
    fn merge_propagates_controller_evidence_in_memory_and_across_restart() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let native_key = native(Provider::Codex, "absorbed-native");
        let survivor_run = TaskRun {
            run_id: survivor,
            key: RunKey::Controller("survivor".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Queued,
            has_controller_task_state_event: false,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let absorbed_run = TaskRun {
            run_id: absorbed,
            key: native_key.clone(),
            display_ordinal: DisplayOrdinal::new(2),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let native_session = NativeSessionBinding {
            provider: Provider::Codex,
            native_session_id: "absorbed-native".to_owned(),
        };
        store
            .apply_batch(vec![
                PersistOp::UpsertTaskRun(PersistTaskRun {
                    task_run: survivor_run.clone(),
                    native_session: None,
                    created_at_ms: 1_000,
                    updated_at_ms: 1_000,
                    finished_at_ms: None,
                }),
                PersistOp::UpsertTaskRun(PersistTaskRun {
                    task_run: absorbed_run.clone(),
                    native_session: Some(native_session),
                    created_at_ms: 1_000,
                    updated_at_ms: 1_000,
                    finished_at_ms: None,
                }),
            ])
            .unwrap();
        let mut model = DomainModel::default();
        model.insert_task_run(survivor_run);
        model.insert_task_run(absorbed_run);

        let batch =
            apply_binding_plan_at(&mut model, BindingPlan::Merge { survivor, absorbed }, 2_000)
                .unwrap();
        let in_memory_flag = model
            .task_run(&survivor)
            .unwrap()
            .has_controller_task_state_event;
        let in_memory_relationship_only = is_relationship_only(&model, survivor);
        store.apply_batch(batch).unwrap();
        drop(store);
        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        let durable_survivor = restored.model.task_run(&survivor).unwrap();
        let durable_native_owner = restored.model.task_run_by_key(&native_key).unwrap().run_id;

        assert_eq!(
            (
                in_memory_flag,
                in_memory_relationship_only,
                durable_survivor.has_controller_task_state_event,
                durable_native_owner,
            ),
            (true, false, true, survivor)
        );
    }

    #[test]
    fn binding_merge_folds_all_v6_run_state_and_restores() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let outstanding = HistoryDrainId::new("codex:outstanding").unwrap();
        let completed = HistoryDrainId::new("codex:completed").unwrap();
        let survivor_run = TaskRun {
            run_id: survivor,
            key: RunKey::Controller("v6-survivor".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: false,
            created_at_ms: Some(1_000),
            updated_at_ms: Some(1_000),
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let absorbed_run = TaskRun {
            run_id: absorbed,
            key: native(Provider::Codex, "v6-absorbed"),
            display_ordinal: DisplayOrdinal::new(2),
            state: TaskState::Completed,
            has_controller_task_state_event: true,
            created_at_ms: Some(1_100),
            updated_at_ms: Some(1_100),
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let survivor_state = TaskRunV6State {
            native_session_end: Some(NativeSessionEnd {
                status: NativeSessionEndStatus::Done,
                at_ms: 1_500,
            }),
            lifecycle_watermark: Some(NativeLifecycleWatermark {
                source_at_ms: 1_500,
                observed_at_ms: 1_600,
                source_order: "provider:a".to_owned(),
            }),
            history_ready: true,
            latest_provider_at_ms: Some(1_500),
        };
        let mut absorbed_state = TaskRunV6State {
            native_session_end: Some(NativeSessionEnd {
                status: NativeSessionEndStatus::Error,
                at_ms: 1_500,
            }),
            lifecycle_watermark: Some(NativeLifecycleWatermark {
                source_at_ms: 1_500,
                observed_at_ms: 1_600,
                source_order: "provider:z".to_owned(),
            }),
            history_ready: false,
            latest_provider_at_ms: Some(1_900),
        };
        let survivor_totals = RunRateTotals {
            output_tokens: 13,
            working_ms: 700,
        };
        let absorbed_totals = RunRateTotals {
            output_tokens: 29,
            working_ms: 1_300,
        };
        store
            .apply_v6_batch(PersistV6Batch {
                task_runs: vec![
                    PersistTaskRunV6 {
                        task_run: PersistTaskRun {
                            task_run: survivor_run.clone(),
                            native_session: None,
                            created_at_ms: 1_000,
                            updated_at_ms: 1_000,
                            finished_at_ms: None,
                        },
                        state: survivor_state.clone(),
                    },
                    PersistTaskRunV6 {
                        task_run: PersistTaskRun {
                            task_run: absorbed_run.clone(),
                            native_session: Some(NativeSessionBinding {
                                provider: Provider::Codex,
                                native_session_id: "v6-absorbed".to_owned(),
                            }),
                            created_at_ms: 1_100,
                            updated_at_ms: 1_100,
                            finished_at_ms: None,
                        },
                        state: absorbed_state.clone(),
                    },
                ],
                rate_totals: vec![(survivor, survivor_totals), (absorbed, absorbed_totals)],
                history_drains: vec![
                    PersistHistoryDrain {
                        drain_id: outstanding.clone(),
                        provider: Provider::Codex,
                        created_at_ms: 900,
                        artifacts: Vec::new(),
                    },
                    PersistHistoryDrain {
                        drain_id: completed.clone(),
                        provider: Provider::Codex,
                        created_at_ms: 925,
                        artifacts: Vec::new(),
                    },
                ],
                history_associations: vec![
                    PersistHistoryDrainRun {
                        drain_id: outstanding,
                        run_id: absorbed,
                    },
                    PersistHistoryDrainRun {
                        drain_id: completed.clone(),
                        run_id: absorbed,
                    },
                ],
                ..PersistV6Batch::default()
            })
            .unwrap();
        let completed_result = store.finalize_history_drain(&completed, 1_700).unwrap();
        absorbed_state = completed_result.runs[0].state.clone();
        assert!(absorbed_state.history_ready);

        let mut model = DomainModel::default();
        model.insert_task_run(survivor_run);
        model.insert_task_run(absorbed_run);
        model.set_task_run_v6_state(survivor, survivor_state);
        model.set_task_run_v6_state(absorbed, absorbed_state.clone());
        model.set_run_rate_totals(survivor, survivor_totals);
        model.set_run_rate_totals(absorbed, absorbed_totals);

        let batch =
            apply_binding_plan_at(&mut model, BindingPlan::Merge { survivor, absorbed }, 2_000)
                .unwrap();
        assert!(batch.iter().any(|operation| matches!(
            operation,
            PersistOp::MergeTaskRuns {
                survivor: actual_survivor,
                absorbed: actual_absorbed,
            } if *actual_survivor == survivor && *actual_absorbed == absorbed
        )));
        assert_eq!(model.task_run_v6_state(&absorbed), None);
        assert_eq!(model.run_rate_totals(&absorbed), None);
        assert_eq!(
            model.task_run_v6_state(&survivor),
            Some(&TaskRunV6State {
                native_session_end: absorbed_state.native_session_end,
                lifecycle_watermark: absorbed_state.lifecycle_watermark,
                history_ready: true,
                latest_provider_at_ms: Some(1_900),
            })
        );
        assert_eq!(
            model.run_rate_totals(&survivor),
            Some(&RunRateTotals {
                output_tokens: 42,
                working_ms: 2_000,
            })
        );

        store.apply_batch(batch).unwrap();
        let restored = store.load_restored_state().unwrap();
        assert_eq!(
            restored.model.task_run_v6_state(&survivor),
            model.task_run_v6_state(&survivor)
        );
        assert_eq!(
            restored.model.run_rate_totals(&survivor),
            model.run_rate_totals(&survivor)
        );
        assert_eq!(restored.model.run_rate_totals(&absorbed), None);
        assert_eq!(
            store
                .history_drain_run_ids(&HistoryDrainId::new("codex:outstanding").unwrap())
                .unwrap(),
            vec![survivor]
        );
        assert_eq!(
            store.history_drain_run_ids(&completed).unwrap(),
            vec![survivor]
        );
    }
}
