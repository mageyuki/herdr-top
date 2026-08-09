//! Reusable task-run graph analysis.

use std::collections::{HashMap, HashSet};

use super::{DomainModel, RunId, TaskRun, TaskState};

fn relationship_only_with_bindings(
    task_run: &TaskRun,
    execution_bound_runs: &HashSet<RunId>,
) -> bool {
    let relationship_only = !task_run.has_controller_task_state_event
        && !execution_bound_runs.contains(&task_run.run_id);
    if relationship_only {
        debug_assert_eq!(
            task_run.state,
            TaskState::Queued,
            "relationship-only task runs must remain queued"
        );
    }
    relationship_only
}

/// Returns whether one live task run is represented only by relationship events.
#[must_use]
pub fn is_relationship_only(model: &DomainModel, run_id: RunId) -> bool {
    let Some(task_run) = model.task_run(&run_id) else {
        return false;
    };
    let execution_bound_runs = model
        .executions()
        .map(|execution| execution.task_run_id)
        .collect();
    relationship_only_with_bindings(task_run, &execution_bound_runs)
}

/// Counts dangling relationship-only components in the current model.
#[must_use]
pub fn dangling_announcement_components(model: &DomainModel) -> u64 {
    let execution_bound_runs: HashSet<_> = model
        .executions()
        .map(|execution| execution.task_run_id)
        .collect();
    let mut adjacency = HashMap::<RunId, Vec<RunId>>::new();
    for (first, second) in model
        .execution_edges()
        .map(|edge| (edge.parent_run_id, edge.child_run_id))
        .chain(
            model
                .dependency_edges()
                .map(|edge| (edge.prerequisite_run_id, edge.dependent_run_id)),
        )
    {
        adjacency.entry(first).or_default().push(second);
        adjacency.entry(second).or_default().push(first);
    }
    let relationship_only_runs: HashSet<_> = adjacency
        .keys()
        .filter_map(|run_id| model.task_run(run_id))
        .filter(|task_run| relationship_only_with_bindings(task_run, &execution_bound_runs))
        .map(|task_run| task_run.run_id)
        .collect();

    let mut visited = HashSet::new();
    let mut dangling_components = 0_u64;
    for start in &relationship_only_runs {
        if !visited.insert(*start) {
            continue;
        }
        let mut stack = vec![*start];
        let mut has_non_terminal_outside_neighbor = false;
        while let Some(run_id) = stack.pop() {
            for neighbor in adjacency.get(&run_id).into_iter().flatten() {
                if relationship_only_runs.contains(neighbor) {
                    if visited.insert(*neighbor) {
                        stack.push(*neighbor);
                    }
                } else if model
                    .task_run(neighbor)
                    .is_some_and(|task_run| !task_run.state.is_terminal())
                {
                    has_non_terminal_outside_neighbor = true;
                }
            }
        }
        if !has_non_terminal_outside_neighbor {
            dangling_components = dangling_components.saturating_add(1);
        }
    }
    dangling_components
}
