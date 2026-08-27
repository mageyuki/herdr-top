//! Stable dependency-DAG ordering and row construction.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use crate::model::{DomainModel, RunId};

use super::app::NodeKey;
use super::projection::StatusReadModel;
use super::view::{TreeRow, short_run_name, task_run_label};

/// Session-local dependency order derived from the last displayed DAG.
#[derive(Debug, Default)]
pub(crate) struct DagOrder {
    run_ids: Vec<RunId>,
    previous_positions: HashMap<RunId, usize>,
    cycle_fallbacks: u64,
}

impl DagOrder {
    /// Recomputes the full dependency order from one coherent model snapshot.
    pub(crate) fn recompute(&mut self, model: &DomainModel) {
        let ordinals = model
            .task_runs()
            .map(|run| (run.run_id, run.display_ordinal.get()))
            .collect::<HashMap<_, _>>();
        let nodes = ordinals.keys().copied().collect::<HashSet<_>>();
        let mut outgoing = nodes
            .iter()
            .copied()
            .map(|run_id| (run_id, Vec::new()))
            .collect::<HashMap<_, _>>();
        let mut reverse = outgoing.clone();
        let mut indegree = nodes
            .iter()
            .copied()
            .map(|run_id| (run_id, 0usize))
            .collect::<HashMap<_, _>>();

        for edge in model.dependency_edges() {
            if !nodes.contains(&edge.prerequisite_run_id) || !nodes.contains(&edge.dependent_run_id)
            {
                continue;
            }
            outgoing
                .get_mut(&edge.prerequisite_run_id)
                .expect("known prerequisite")
                .push(edge.dependent_run_id);
            reverse
                .get_mut(&edge.dependent_run_id)
                .expect("known dependent")
                .push(edge.prerequisite_run_id);
            *indegree
                .get_mut(&edge.dependent_run_id)
                .expect("known dependent") = indegree[&edge.dependent_run_id].saturating_add(1);
        }

        // Propagate the earliest prior position backwards. A new prerequisite thereby inherits
        // the position of its earliest positioned transitive dependent.
        let mut inherited_positions = HashMap::new();
        let mut propagation = VecDeque::new();
        for run_id in &nodes {
            if let Some(position) = self.previous_positions.get(run_id).copied() {
                inherited_positions.insert(*run_id, position);
                propagation.push_back(*run_id);
            }
        }
        while let Some(dependent) = propagation.pop_front() {
            let position = inherited_positions[&dependent];
            for prerequisite in &reverse[&dependent] {
                let current = inherited_positions
                    .get(prerequisite)
                    .copied()
                    .unwrap_or(usize::MAX);
                if position < current {
                    inherited_positions.insert(*prerequisite, position);
                    propagation.push_back(*prerequisite);
                }
            }
        }

        let effective_positions = nodes
            .iter()
            .copied()
            .map(|run_id| {
                let position = self
                    .previous_positions
                    .get(&run_id)
                    .copied()
                    .or_else(|| inherited_positions.get(&run_id).copied())
                    .unwrap_or(usize::MAX);
                (run_id, position)
            })
            .collect::<HashMap<_, _>>();
        let key = |run_id: RunId| (effective_positions[&run_id], ordinals[&run_id], run_id);

        let mut ready = BinaryHeap::new();
        for run_id in &nodes {
            if indegree[run_id] == 0 {
                ready.push(Reverse(key(*run_id)));
            }
        }
        let mut ordered = Vec::with_capacity(nodes.len());
        let mut emitted = HashSet::with_capacity(nodes.len());
        while let Some(Reverse((_, _, run_id))) = ready.pop() {
            ordered.push(run_id);
            emitted.insert(run_id);
            for dependent in &outgoing[&run_id] {
                let remaining = indegree.get_mut(dependent).expect("known dependent");
                *remaining = remaining.saturating_sub(1);
                if *remaining == 0 {
                    ready.push(Reverse(key(*dependent)));
                }
            }
        }

        if ordered.len() != nodes.len() {
            let mut remainder = nodes
                .into_iter()
                .filter(|run_id| !emitted.contains(run_id))
                .collect::<Vec<_>>();
            remainder.sort_by_key(|run_id| key(*run_id));
            ordered.extend(remainder);
            self.cycle_fallbacks = self.cycle_fallbacks.saturating_add(1);
        }

        self.previous_positions = ordered
            .iter()
            .copied()
            .enumerate()
            .map(|(position, run_id)| (run_id, position))
            .collect();
        self.run_ids = ordered;
    }

    pub(crate) fn run_ids(&self) -> &[RunId] {
        &self.run_ids
    }

    #[cfg(test)]
    pub(crate) const fn cycle_fallbacks(&self) -> u64 {
        self.cycle_fallbacks
    }
}

#[cfg(test)]
pub(crate) fn build_rows(model: &DomainModel, order: &DagOrder, now_ms: i64) -> Vec<TreeRow> {
    let statuses = StatusReadModel::from_model(model, now_ms);
    build_rows_with_statuses(model, order, now_ms, &statuses)
}

pub(crate) fn build_rows_with_statuses(
    model: &DomainModel,
    order: &DagOrder,
    now_ms: i64,
    statuses: &StatusReadModel,
) -> Vec<TreeRow> {
    let stalled_runs =
        super::projection::stalled_run_ids(model, now_ms, crate::activity::stall_warn_ms());
    let mut prerequisites = HashMap::<RunId, Vec<RunId>>::new();
    let mut dependents = HashMap::<RunId, Vec<RunId>>::new();
    for edge in model.dependency_edges() {
        if model.task_run(&edge.prerequisite_run_id).is_none()
            || model.task_run(&edge.dependent_run_id).is_none()
        {
            continue;
        }
        prerequisites
            .entry(edge.dependent_run_id)
            .or_default()
            .push(edge.prerequisite_run_id);
        dependents
            .entry(edge.prerequisite_run_id)
            .or_default()
            .push(edge.dependent_run_id);
    }
    let neighbor_names = |run_ids: Option<&Vec<RunId>>| {
        let mut neighbors = run_ids
            .into_iter()
            .flatten()
            .filter_map(|run_id| model.task_run(run_id))
            .collect::<Vec<_>>();
        neighbors.sort_by_key(|run| (run.display_ordinal.get(), run.run_id));
        neighbors
            .into_iter()
            .map(short_run_name)
            .collect::<Vec<_>>()
    };
    order
        .run_ids()
        .iter()
        .filter_map(|run_id| model.task_run(run_id))
        .map(|run| {
            let display_status =
                statuses.task_display_status(model, run, None, stalled_runs.contains(&run.run_id));
            TreeRow {
                key: NodeKey::Run {
                    run_id: run.run_id,
                    pane_id: None,
                },
                depth: 0,
                label: task_run_label(model, run, display_status, false, now_ms, true),
                label_without_duration_suffix: None,
                display_status: Some(display_status),
                prerequisites: neighbor_names(prerequisites.get(&run.run_id)),
                dependents: neighbor_names(dependents.get(&run.run_id)),
            }
        })
        // DAG rows are run-only today. Keep the shared Agent Node boundary on the final row
        // stream so a future Agent row cannot bypass the tree's display-staleness rule.
        .filter(|row| match &row.key {
            NodeKey::Agent { agent_node_id, .. } => model
                .agent_node(agent_node_id)
                .is_none_or(|agent| !super::projection::agent_node_is_display_stale(agent, now_ms)),
            _ => true,
        })
        .collect()
}

/// Retains direct DAG matches and every reverse-transitive prerequisite in stable order.
pub(crate) fn retain_matches_with_prerequisites(
    model: &DomainModel,
    stable_order: &[RunId],
    direct_matches: &HashSet<RunId>,
) -> Vec<RunId> {
    let mut reverse = HashMap::<RunId, Vec<RunId>>::new();
    for edge in model.dependency_edges() {
        if model.task_run(&edge.prerequisite_run_id).is_some()
            && model.task_run(&edge.dependent_run_id).is_some()
        {
            reverse
                .entry(edge.dependent_run_id)
                .or_default()
                .push(edge.prerequisite_run_id);
        }
    }
    let mut retained = direct_matches.clone();
    let mut pending = direct_matches.iter().copied().collect::<Vec<_>>();
    while let Some(dependent) = pending.pop() {
        for prerequisite in reverse.get(&dependent).into_iter().flatten() {
            if retained.insert(*prerequisite) {
                pending.push(*prerequisite);
            }
        }
    }
    stable_order
        .iter()
        .copied()
        .filter(|run_id| retained.contains(run_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::model::{
        AgentNode, DependencyEdge, DisplayOrdinal, DomainModel, ExecState, ExecutionEdge, Provider,
        RunId, RunKey, TaskRun, TaskState, Workspace,
    };

    use super::DagOrder;

    fn model(
        labels: &[(&str, i64)],
        edges: &[(&str, &str)],
    ) -> (DomainModel, HashMap<String, RunId>) {
        let mut model = DomainModel::default();
        let ids = labels
            .iter()
            .map(|(label, _)| ((*label).to_owned(), RunId::new()))
            .collect::<HashMap<_, _>>();
        for (label, ordinal) in labels {
            model.insert_task_run(TaskRun {
                run_id: ids[*label],
                key: RunKey::Controller((*label).to_owned()),
                display_ordinal: DisplayOrdinal::new(*ordinal),
                state: TaskState::Queued,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            });
        }
        for (prerequisite, dependent) in edges {
            model.insert_dependency_edge(DependencyEdge {
                prerequisite_run_id: ids[*prerequisite],
                dependent_run_id: ids[*dependent],
            });
        }
        (model, ids)
    }

    fn labels(order: &DagOrder, model: &DomainModel) -> Vec<String> {
        order
            .run_ids()
            .iter()
            .map(|run_id| match &model.task_run(run_id).unwrap().key {
                RunKey::Controller(label) => label.clone(),
                _ => unreachable!("test creates controller runs"),
            })
            .collect()
    }

    struct Scenario {
        order: DagOrder,
        ids: HashMap<String, RunId>,
    }

    fn refresh(
        scenario: &mut Scenario,
        labels_with_ordinals: &[(&str, i64)],
        edges: &[(&str, &str)],
    ) -> DomainModel {
        let mut model = DomainModel::default();
        for (label, _) in labels_with_ordinals {
            scenario.ids.entry((*label).to_owned()).or_default();
        }
        for (label, ordinal) in labels_with_ordinals {
            model.insert_task_run(TaskRun {
                run_id: scenario.ids[*label],
                key: RunKey::Controller((*label).to_owned()),
                display_ordinal: DisplayOrdinal::new(*ordinal),
                state: TaskState::Queued,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            });
        }
        for (prerequisite, dependent) in edges {
            model.insert_dependency_edge(DependencyEdge {
                prerequisite_run_id: scenario.ids[*prerequisite],
                dependent_run_id: scenario.ids[*dependent],
            });
        }
        scenario.order.recompute(&model);
        model
    }

    fn cold(
        labels_with_ordinals: &[(&str, i64)],
        edges: &[(&str, &str)],
    ) -> (Scenario, DomainModel) {
        let (model, ids) = model(labels_with_ordinals, edges);
        let mut order = DagOrder::default();
        order.recompute(&model);
        (Scenario { order, ids }, model)
    }

    #[test]
    fn cold_build_is_canonical_topological_and_restart_stable() {
        let (order, model) = cold(&[("A", 1), ("B", 2), ("C", 3)], &[("C", "A")]);
        assert_eq!(labels(&order.order, &model), ["B", "C", "A"]);

        let mut restarted = DagOrder::default();
        restarted.recompute(&model);
        assert_eq!(restarted.run_ids(), order.order.run_ids());
    }

    #[test]
    fn unchanged_refresh_is_identical_and_new_isolated_runs_append_by_ordinal() {
        let (mut order, _) = cold(&[("A", 1), ("B", 2), ("C", 3)], &[("C", "A")]);
        let previous = order.order.run_ids().to_vec();
        let unchanged = refresh(&mut order, &[("A", 1), ("B", 2), ("C", 3)], &[("C", "A")]);
        assert_eq!(order.order.run_ids(), previous);
        assert_eq!(labels(&order.order, &unchanged), ["B", "C", "A"]);

        let appended = refresh(
            &mut order,
            &[("A", 1), ("B", 2), ("C", 3), ("E", 5), ("D", 4)],
            &[("C", "A")],
        );
        assert_eq!(labels(&order.order, &appended), ["B", "C", "A", "D", "E"]);
    }

    #[test]
    fn new_pure_prerequisites_insert_before_earliest_dependent() {
        let (mut order, _) = cold(&[("A", 1), ("D", 2), ("B", 3), ("C", 4)], &[]);
        let refreshed = refresh(
            &mut order,
            &[("A", 1), ("D", 2), ("B", 3), ("C", 4), ("P1", 6), ("P2", 5)],
            &[("P1", "B"), ("P2", "B")],
        );
        assert_eq!(
            labels(&order.order, &refreshed),
            ["A", "D", "P2", "P1", "B", "C"]
        );
    }

    #[test]
    fn new_run_with_incoming_edges_forces_required_reordering() {
        let (mut order, _) = cold(&[("D", 1), ("P", 2)], &[]);
        let refreshed = refresh(
            &mut order,
            &[("D", 1), ("P", 2), ("N", 3)],
            &[("P", "N"), ("N", "D")],
        );
        assert_eq!(labels(&order.order, &refreshed), ["P", "N", "D"]);
    }

    #[test]
    fn isolated_new_edge_moves_only_blocked_interval_after_prerequisite() {
        let (mut order, _) = cold(
            &[("A", 1), ("V", 2), ("X", 3), ("U", 4), ("W", 5)],
            &[("V", "X")],
        );
        let refreshed = refresh(
            &mut order,
            &[("A", 1), ("V", 2), ("X", 3), ("U", 4), ("W", 5)],
            &[("V", "X"), ("U", "V")],
        );
        assert_eq!(labels(&order.order, &refreshed), ["A", "U", "V", "X", "W"]);
    }

    #[test]
    fn merge_contraction_reorders_survivor_in_both_edge_directions() {
        let (mut blocked, _) = cold(&[("S", 1), ("P", 2), ("A", 3)], &[("P", "A")]);
        let blocked_model = refresh(&mut blocked, &[("S", 1), ("P", 2)], &[("P", "S")]);
        assert_eq!(labels(&blocked.order, &blocked_model), ["P", "S"]);

        let (mut prerequisite, _) = cold(&[("A", 1), ("D", 2), ("S", 3)], &[("A", "D")]);
        let prerequisite_model = refresh(&mut prerequisite, &[("D", 2), ("S", 3)], &[("S", "D")]);
        assert_eq!(labels(&prerequisite.order, &prerequisite_model), ["S", "D"]);
    }

    #[test]
    fn batched_edges_are_topological_and_cadence_is_explicit() {
        let labels_with_ordinals = [("A", 1), ("V", 2), ("X", 3), ("U", 4)];
        let (mut batched, _) = cold(&labels_with_ordinals, &[]);
        let batched_model = refresh(
            &mut batched,
            &labels_with_ordinals,
            &[("U", "V"), ("X", "A")],
        );
        assert_eq!(labels(&batched.order, &batched_model), ["X", "A", "U", "V"]);

        let (mut sequential, _) = cold(&labels_with_ordinals, &[]);
        let first = refresh(&mut sequential, &labels_with_ordinals, &[("U", "V")]);
        assert_eq!(labels(&sequential.order, &first), ["A", "X", "U", "V"]);
        let second = refresh(
            &mut sequential,
            &labels_with_ordinals,
            &[("U", "V"), ("X", "A")],
        );
        assert_eq!(labels(&sequential.order, &second), ["X", "A", "U", "V"]);
    }

    #[test]
    fn cycle_defense_appends_a_deterministic_remainder_and_counts_diagnostic() {
        let (mut order, _) = cold(&[("B", 2), ("A", 1)], &[("B", "A")]);
        let cyclic = refresh(&mut order, &[("B", 2), ("A", 1)], &[("A", "B"), ("B", "A")]);
        assert_eq!(labels(&order.order, &cyclic), ["B", "A"]);
        assert_eq!(order.order.cycle_fallbacks(), 1);
    }

    #[test]
    fn thousand_edge_chain_builds_in_topological_order() {
        let mut model = DomainModel::default();
        let ids = (0..=1_000).map(|_| RunId::new()).collect::<Vec<_>>();
        for (index, run_id) in ids.iter().copied().enumerate() {
            model.insert_task_run(TaskRun {
                run_id,
                key: RunKey::Controller(format!("run-{index:04}")),
                display_ordinal: DisplayOrdinal::new(index as i64),
                state: TaskState::Queued,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            });
        }
        for pair in ids.windows(2) {
            model.insert_dependency_edge(DependencyEdge {
                prerequisite_run_id: pair[0],
                dependent_run_id: pair[1],
            });
        }

        let mut order = DagOrder::default();
        order.recompute(&model);

        assert_eq!(order.run_ids(), ids);
        assert_eq!(order.cycle_fallbacks(), 0);
    }

    #[test]
    fn dag_rows_use_the_shared_status_prefix_without_unlinked() {
        let (mut model, ids) = model(
            &[
                ("Dispatcher", 1),
                ("Q", 2),
                ("P", 3),
                ("D", 4),
                ("U", 5),
                ("Z1", 6),
                ("Z2", 7),
            ],
            &[("P", "D"), ("Q", "D"), ("D", "Z2"), ("D", "Z1")],
        );
        model.insert_workspace(Workspace {
            workspace_id: "not-a-dag-row".to_owned(),
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "not-a-dag-row".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("agent".to_owned()),
            task_run_id: ids["D"],
            display_ordinal: DisplayOrdinal::new(8),
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
            parent_run_id: ids["Dispatcher"],
            child_run_id: ids["D"],
        });
        let mut order = DagOrder::default();
        order.recompute(&model);

        let rows = super::build_rows(&model, &order, 0);

        assert_eq!(rows.len(), model.task_runs().count());
        assert!(
            rows.iter()
                .all(|row| matches!(row.key, crate::tui::app::NodeKey::Run { pane_id: None, .. }))
        );
        let dependent = rows
            .iter()
            .find(|row| {
                matches!(
                    row.key,
                    crate::tui::app::NodeKey::Run { run_id, .. } if run_id == ids["D"]
                )
            })
            .unwrap();
        assert_eq!(dependent.label, "◌ queued D D [dispatched by: Dispatcher]");
        assert_eq!(
            dependent.display_status,
            Some(crate::tui::projection::DisplayStatus::new(
                crate::tui::projection::TaskDisplayStatus::Queued,
                crate::tui::projection::StatusSource::TaskState,
            )),
        );
        assert_eq!(dependent.prerequisites, ["Q", "P"]);
        assert_eq!(dependent.dependents, ["Z1", "Z2"]);
        let second_hop = rows
            .iter()
            .find(|row| {
                matches!(
                    row.key,
                    crate::tui::app::NodeKey::Run { run_id, .. } if run_id == ids["Z1"]
                )
            })
            .unwrap();
        assert_eq!(second_hop.prerequisites, ["D"]);
        assert!(second_hop.dependents.is_empty());
        let first_hop = rows
            .iter()
            .find(|row| {
                matches!(
                    row.key,
                    crate::tui::app::NodeKey::Run { run_id, .. } if run_id == ids["P"]
                )
            })
            .unwrap();
        assert!(first_hop.prerequisites.is_empty());
        assert_eq!(first_hop.dependents, ["D"]);
        let isolated = rows
            .iter()
            .find(|row| {
                matches!(
                    row.key,
                    crate::tui::app::NodeKey::Run { run_id, .. } if run_id == ids["U"]
                )
            })
            .unwrap();
        assert_eq!(isolated.label, "◌ queued U U");
        assert!(!isolated.label.contains("[unlinked]"));
    }

    #[test]
    fn display_stale_unknown_agent_is_absent_from_dag_rows() {
        let (mut model, ids) = model(&[("run", 1)], &[]);
        model.insert_agent_node(AgentNode {
            agent_node_id: "stale-unknown".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("stale-unknown".to_owned()),
            task_run_id: ids["run"],
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Unknown),
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: Some(0),
            session_file: None,
        });
        let mut order = DagOrder::default();
        order.recompute(&model);

        let rows = super::build_rows(
            &model,
            &order,
            crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS,
        );

        assert!(
            rows.iter()
                .all(|row| !matches!(row.key, crate::tui::app::NodeKey::Agent { .. }))
        );
    }

    #[test]
    fn rows_use_stall_glyph_suppress_codex_worker_subject_and_omit_live_line() {
        let (mut model, ids) = model(&[("Parent", 1)], &[]);
        let worker_id = RunId::new();
        model.insert_task_run(TaskRun {
            run_id: worker_id,
            key: RunKey::Native {
                provider: Provider::Codex,
                sid: "worker-session".to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(2),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: Some(0),
            finished_at_ms: None,
            subject: Some("must not render".to_owned()),
            dismissed_at_ms: None,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: ids["Parent"],
            child_run_id: worker_id,
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "worker-agent".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("worker-session".to_owned()),
            task_run_id: worker_id,
            display_ordinal: DisplayOrdinal::new(3),
            parent_agent_node_id: None,
            state: None,
            model_id: Some("must-not-render-model".to_owned()),
            last_event_kind: Some("tool_use".to_owned()),
            last_tool_name: Some("must not render live".to_owned()),
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: Some(0),
            session_file: None,
        });
        let mut order = DagOrder::default();
        order.recompute(&model);
        let rows = super::build_rows(&model, &order, crate::activity::DEFAULT_STALL_WARN_MS + 1);
        let worker = rows
            .iter()
            .find(|row| row.key.run_id() == Some(worker_id))
            .expect("DAG has the worker row");

        assert_eq!(worker.label, "⚠ working Codex [dispatched by: Parent]");
    }

    #[test]
    fn i4_filtered_diamond_retains_every_prerequisite_path() {
        let (model, ids) = model(
            &[
                ("root", 1),
                ("left", 2),
                ("right", 3),
                ("match", 4),
                ("other", 5),
            ],
            &[
                ("root", "left"),
                ("root", "right"),
                ("left", "match"),
                ("right", "match"),
            ],
        );
        let mut order = DagOrder::default();
        order.recompute(&model);

        let retained = super::retain_matches_with_prerequisites(
            &model,
            order.run_ids(),
            &std::collections::HashSet::from([ids["match"]]),
        );

        assert_eq!(
            retained,
            [ids["root"], ids["left"], ids["right"], ids["match"]]
        );
    }
}
