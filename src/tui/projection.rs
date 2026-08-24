//! Pure, privacy-safe TUI projection tests and implementation.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::Path;

use crate::activity::{
    ActivityDurability, ActivityItem, DEFAULT_TERMINAL_VISIBILITY_MS,
    HOOK_ONLY_STALE_VISIBILITY_MS, OperatorSnapshot, ghost_visibility_ms,
    is_default_visible_task_run, runs_with_executions,
};
use crate::diagnostics::{ControllerInputStatus, RuntimeDiagnosticsSnapshot};
use crate::herdr::collector::ObservationQuality;
use crate::model::{DomainModel, ExecState, Provider, RunId, RunKey, TaskState};
use crate::store::writer::PersistenceStatus;

use super::app::{NodeKey, ViewMode};
use super::dag;
use super::view::TreeRow;

pub(crate) const DETAIL_ACTIVITY_LIMIT: usize = 100;

#[derive(Debug)]
pub(crate) struct RowProjection {
    pub(crate) rows: Vec<TreeRow>,
    pub(crate) next_expiry_ms: Option<i64>,
}

pub(crate) fn stalled_run_ids(
    model: &DomainModel,
    now_ms: i64,
    stall_warn_ms: i64,
) -> HashSet<RunId> {
    let active = model
        .task_runs()
        .filter(|run| !run.state.is_terminal())
        .filter(|run| {
            run.updated_at_ms
                .or(run.created_at_ms)
                .is_some_and(|last_activity_ms| {
                    now_ms < last_activity_ms.saturating_add(stall_warn_ms)
                })
        })
        .map(|run| run.run_id)
        .collect::<HashSet<_>>();
    let mut children = HashMap::<RunId, Vec<RunId>>::new();
    for edge in model.execution_edges() {
        if model.task_run(&edge.parent_run_id).is_some()
            && model.task_run(&edge.child_run_id).is_some()
        {
            children
                .entry(edge.parent_run_id)
                .or_default()
                .push(edge.child_run_id);
        }
    }

    model
        .task_runs()
        .filter(|run| !run.state.is_terminal())
        .filter(|run| {
            run.updated_at_ms
                .or(run.created_at_ms)
                .is_some_and(|last_activity_ms| {
                    now_ms >= last_activity_ms.saturating_add(stall_warn_ms)
                })
        })
        .filter(|run| !has_active_descendant(run.run_id, &children, &active))
        .map(|run| run.run_id)
        .collect()
}

fn has_active_descendant(
    run_id: RunId,
    children: &HashMap<RunId, Vec<RunId>>,
    active: &HashSet<RunId>,
) -> bool {
    let mut pending = children.get(&run_id).cloned().unwrap_or_default();
    let mut visited = HashSet::from([run_id]);
    while let Some(descendant) = pending.pop() {
        if !visited.insert(descendant) {
            continue;
        }
        if active.contains(&descendant) {
            return true;
        }
        if let Some(grandchildren) = children.get(&descendant) {
            pending.extend(grandchildren);
        }
    }
    false
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DetailEntity {
    Session,
    Workspace {
        workspace_id: String,
    },
    Tab {
        tab_id: String,
        workspace_id: String,
    },
    Pane {
        pane_id: String,
        workspace_id: String,
        tab_id: String,
    },
    Run {
        run_id: RunId,
        key: String,
        name: String,
        native_session_id: Option<String>,
        state: TaskState,
        created_at_ms: Option<i64>,
        updated_at_ms: Option<i64>,
        finished_at_ms: Option<i64>,
        dismissed_at_ms: Option<i64>,
    },
    Agent {
        agent_node_id: String,
        native_session_id: Option<String>,
        provider: Provider,
        state: Option<ExecState>,
        model_id: Option<String>,
        parent_agent_node_id: Option<String>,
        session_file: Option<String>,
    },
    Unattached,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActivityWindow {
    pub(crate) items: Vec<ActivityItem>,
    pub(crate) retained_count: usize,
    pub(crate) matching_count: usize,
    pub(crate) bound: usize,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DetailProjection {
    pub(crate) entity: DetailEntity,
    pub(crate) activity: ActivityWindow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeStrip {
    pub(crate) quality: ObservationQuality,
    pub(crate) persistence: &'static str,
    pub(crate) controller: &'static str,
    pub(crate) d4: u64,
}

pub(crate) fn project_rows(
    model: &DomainModel,
    full_rows: &[TreeRow],
    operator: &OperatorSnapshot,
    query: &str,
    collapsed: &HashSet<NodeKey>,
    mode: ViewMode,
    now_ms: i64,
) -> RowProjection {
    if !query.is_empty() {
        let direct = full_rows
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row_matches(model, row, query).then_some(index))
            .collect::<Vec<_>>();
        let rows = match mode {
            ViewMode::ExecutionTree => retain_tree_matches(full_rows, &direct),
            ViewMode::DependencyDag => {
                let matches = direct
                    .into_iter()
                    .filter_map(|index| full_rows[index].key.run_id())
                    .collect::<HashSet<_>>();
                let order = full_rows
                    .iter()
                    .filter_map(|row| row.key.run_id())
                    .collect::<Vec<_>>();
                let retained = dag::retain_matches_with_prerequisites(model, &order, &matches)
                    .into_iter()
                    .collect::<HashSet<_>>();
                full_rows
                    .iter()
                    .filter(|row| {
                        row.key
                            .run_id()
                            .is_some_and(|run_id| retained.contains(&run_id))
                    })
                    .cloned()
                    .collect()
            }
        };
        return RowProjection {
            next_expiry_ms: next_live_duration_expiry_ms(model, &rows, now_ms),
            rows,
        };
    }

    let mut visible = Vec::with_capacity(full_rows.len());
    let mut hidden_subtree_depth = None;
    let mut next_expiry_ms: Option<i64> = None;
    let execution_run_ids = runs_with_executions(model);
    for row in full_rows {
        if hidden_subtree_depth.is_some_and(|depth| row.depth > depth) {
            continue;
        }
        hidden_subtree_depth = None;
        let Some(run_id) = row.key.run_id() else {
            visible.push(row.clone());
            continue;
        };
        let Some(run) = model.task_run(&run_id) else {
            continue;
        };
        if is_default_visible_task_run(run, operator, &execution_run_ids, now_ms) {
            if let RunKey::Provisional { start_ms, .. } = &run.key {
                let last_observed_ms = run.updated_at_ms.or(run.created_at_ms).unwrap_or(*start_ms);
                let expiry = last_observed_ms.saturating_add(ghost_visibility_ms());
                next_expiry_ms = Some(next_expiry_ms.map_or(expiry, |current| current.min(expiry)));
            } else if run.state.is_terminal()
                && let Some(first_terminal_ms) = operator.terminal_times.get(&run_id).copied()
            {
                let expiry = first_terminal_ms.saturating_add(DEFAULT_TERMINAL_VISIBILITY_MS);
                next_expiry_ms = Some(next_expiry_ms.map_or(expiry, |current| current.min(expiry)));
            }
            if matches!(run.key, RunKey::Controller(_))
                && !execution_run_ids.contains(&run.run_id)
                && let Some(updated_at_ms) = run.updated_at_ms
            {
                let expiry = updated_at_ms.saturating_add(HOOK_ONLY_STALE_VISIBILITY_MS);
                next_expiry_ms = Some(next_expiry_ms.map_or(expiry, |current| current.min(expiry)));
            }
            visible.push(row.clone());
        } else if mode == ViewMode::ExecutionTree {
            hidden_subtree_depth = Some(row.depth);
        }
    }

    let rows = if mode == ViewMode::ExecutionTree {
        apply_collapse(&visible, collapsed)
    } else {
        visible
    };
    if let Some(duration_expiry_ms) = next_live_duration_expiry_ms(model, &rows, now_ms) {
        next_expiry_ms = Some(next_expiry_ms.map_or(duration_expiry_ms, |current| {
            current.min(duration_expiry_ms)
        }));
    }
    RowProjection {
        rows,
        next_expiry_ms,
    }
}

fn next_live_duration_expiry_ms(model: &DomainModel, rows: &[TreeRow], now_ms: i64) -> Option<i64> {
    let renders_live_duration = rows
        .iter()
        .filter_map(|row| row.key.run_id())
        .filter_map(|run_id| model.task_run(&run_id))
        .filter(|run| !run.state.is_terminal())
        .filter_map(|run| run.created_at_ms)
        .filter_map(|created_at_ms| now_ms.checked_sub(created_at_ms))
        .any(|elapsed_ms| elapsed_ms >= 0);
    if !renders_live_duration {
        return None;
    }

    now_ms
        .checked_sub(now_ms.rem_euclid(1_000))?
        .checked_add(1_000)
}

fn retain_tree_matches(rows: &[TreeRow], direct: &[usize]) -> Vec<TreeRow> {
    let mut retained = HashSet::new();
    for &index in direct {
        retained.insert(index);
        let mut target_depth = rows[index].depth;
        for ancestor in (0..index).rev() {
            if rows[ancestor].depth < target_depth {
                retained.insert(ancestor);
                target_depth = rows[ancestor].depth;
                if target_depth == 0 {
                    break;
                }
            }
        }
    }
    rows.iter()
        .enumerate()
        .filter(|(index, _)| retained.contains(index))
        .map(|(_, row)| row.clone())
        .collect()
}

fn apply_collapse(rows: &[TreeRow], collapsed: &HashSet<NodeKey>) -> Vec<TreeRow> {
    let mut output = Vec::with_capacity(rows.len());
    let mut hidden_below = None;
    for row in rows {
        if hidden_below.is_some_and(|depth| row.depth > depth) {
            continue;
        }
        hidden_below = None;
        output.push(row.clone());
        if collapsed.contains(&row.key) {
            hidden_below = Some(row.depth);
        }
    }
    output
}

pub(crate) fn row_matches(model: &DomainModel, row: &TreeRow, query: &str) -> bool {
    let query = query.to_lowercase();
    if query.is_empty() {
        return true;
    }
    searchable_fields(model, row)
        .into_iter()
        .any(|field| field.to_lowercase().contains(&query))
}

fn searchable_fields(model: &DomainModel, row: &TreeRow) -> Vec<String> {
    match &row.key {
        NodeKey::Session => vec![row.label.clone()],
        NodeKey::Workspace(id) => vec!["workspace".to_owned(), id.clone()],
        NodeKey::Tab(id) => {
            let mut fields = vec!["tab".to_owned(), id.clone()];
            if let Some(tab) = model.tab(id) {
                fields.push(tab.workspace_id.clone());
            }
            fields
        }
        NodeKey::Pane(id) => {
            let mut fields = vec!["pane".to_owned(), id.clone()];
            if let Some(pane) = model.pane(id) {
                fields.extend([
                    pane.workspace_id.clone(),
                    pane.tab_id.clone(),
                    pane.terminal_id.clone(),
                ]);
            }
            fields
        }
        NodeKey::Run { run_id, .. } => {
            let mut fields = vec!["task run".to_owned(), run_id.to_string()];
            if let Some(run) = model.task_run(run_id) {
                fields.push(task_state_name(run.state).to_owned());
                match &run.key {
                    RunKey::Native { provider, sid } => {
                        fields.push(provider_name(*provider).to_owned());
                        fields.push(sid.clone());
                    }
                    RunKey::Provisional { terminal_id, .. } => {
                        fields.push("provisional".to_owned());
                        fields.push(terminal_id.clone());
                    }
                    // Controller text and operational paths are deliberately not searchable.
                    RunKey::Controller(_) | RunKey::NativePath { .. } => {}
                }
                for agent in model
                    .agent_nodes()
                    .filter(|agent| agent.task_run_id == *run_id)
                {
                    fields.push(provider_name(agent.provider).to_owned());
                    if let Some(model_id) = &agent.model_id {
                        fields.push(model_id.clone());
                    }
                }
                if model
                    .execution_edges()
                    .any(|edge| edge.child_run_id == *run_id)
                {
                    fields.push("dispatched by".to_owned());
                }
                if !row.prerequisites.is_empty()
                    || model
                        .dependency_edges()
                        .any(|edge| edge.dependent_run_id == *run_id)
                {
                    fields.extend(["prerequisite".to_owned(), "prereqs".to_owned()]);
                }
                if !row.dependents.is_empty()
                    || model
                        .dependency_edges()
                        .any(|edge| edge.prerequisite_run_id == *run_id)
                {
                    fields.push("dependents".to_owned());
                }
                let linked = model
                    .execution_edges()
                    .any(|edge| edge.parent_run_id == *run_id || edge.child_run_id == *run_id)
                    || model.dependency_edges().any(|edge| {
                        edge.prerequisite_run_id == *run_id || edge.dependent_run_id == *run_id
                    });
                if !linked {
                    fields.push("unlinked".to_owned());
                }
                if row.label.contains("[shared]") {
                    fields.push("shared".to_owned());
                }
            }
            fields
        }
        NodeKey::Agent { agent_node_id, .. } => {
            let mut fields = vec![
                "native agent".to_owned(),
                "parent".to_owned(),
                "child".to_owned(),
                agent_node_id.clone(),
            ];
            if let Some(agent) = model.agent_node(agent_node_id) {
                fields.push(provider_name(agent.provider).to_owned());
                fields.push(agent.task_run_id.to_string());
                fields.extend(agent.native_session_id.iter().cloned());
                fields.extend(agent.model_id.iter().cloned());
                fields.extend(agent.parent_agent_node_id.iter().cloned());
                if let Some(state) = &agent.state {
                    fields.push(exec_state_name(state).to_owned());
                }
            } else {
                // A row can transiently outlive its entity during a coherent refresh. Its label is
                // still the closed Agent-row vocabulary and never contains session_file/activity.
                fields.push(row.label.clone());
            }
            fields
        }
        NodeKey::UnattachedGroup => vec!["unattached task runs".to_owned()],
    }
}

pub(crate) fn detail_projection(
    model: &DomainModel,
    full_rows: &[TreeRow],
    operator: &OperatorSnapshot,
    selected: &NodeKey,
    mode: ViewMode,
    home: Option<&OsStr>,
) -> DetailProjection {
    let entity = detail_entity(model, selected, home);
    let mut matching = operator
        .activity
        .iter()
        .filter(|item| activity_matches_scope(model, full_rows, selected, mode, item))
        .cloned()
        .collect::<Vec<_>>();
    matching.sort_by(compare_activity);
    let matching_count = matching.len();
    matching.truncate(DETAIL_ACTIVITY_LIMIT);
    DetailProjection {
        entity,
        activity: ActivityWindow {
            items: matching,
            retained_count: operator.activity.len(),
            matching_count,
            bound: DETAIL_ACTIVITY_LIMIT,
            truncated: matching_count > DETAIL_ACTIVITY_LIMIT,
        },
    }
}

fn detail_entity(model: &DomainModel, selected: &NodeKey, home: Option<&OsStr>) -> DetailEntity {
    match selected {
        NodeKey::Session => DetailEntity::Session,
        NodeKey::Workspace(workspace_id) => DetailEntity::Workspace {
            workspace_id: escape_controls(workspace_id),
        },
        NodeKey::Tab(tab_id) => {
            model
                .tab(tab_id)
                .map_or(DetailEntity::Missing, |tab| DetailEntity::Tab {
                    tab_id: escape_controls(&tab.tab_id),
                    workspace_id: escape_controls(&tab.workspace_id),
                })
        }
        NodeKey::Pane(pane_id) => {
            model
                .pane(pane_id)
                .map_or(DetailEntity::Missing, |pane| DetailEntity::Pane {
                    pane_id: escape_controls(&pane.pane_id),
                    workspace_id: escape_controls(&pane.workspace_id),
                    tab_id: escape_controls(&pane.tab_id),
                })
        }
        NodeKey::Run { run_id, .. } => {
            model
                .task_run(run_id)
                .map_or(DetailEntity::Missing, |run| DetailEntity::Run {
                    run_id: *run_id,
                    key: detail_run_key(run_id, &run.key),
                    name: safe_run_name(run_id, &run.key),
                    native_session_id: bound_native_session_id(model, run_id, &run.key),
                    state: run.state,
                    created_at_ms: run.created_at_ms,
                    updated_at_ms: run.updated_at_ms,
                    finished_at_ms: run.finished_at_ms,
                    dismissed_at_ms: run.dismissed_at_ms,
                })
        }
        NodeKey::Agent { agent_node_id, .. } => {
            model
                .agent_node(agent_node_id)
                .map_or(DetailEntity::Missing, |agent| DetailEntity::Agent {
                    agent_node_id: escape_controls(&agent.agent_node_id),
                    native_session_id: agent.native_session_id.as_deref().map(escape_controls),
                    provider: agent.provider,
                    state: agent.state.clone(),
                    model_id: agent.model_id.as_deref().map(escape_controls),
                    parent_agent_node_id: agent
                        .parent_agent_node_id
                        .as_deref()
                        .map(escape_controls),
                    session_file: agent.session_file.as_deref().map(|path| {
                        crate::diagnostics::local::format_operational_path(Path::new(path), home)
                    }),
                })
        }
        NodeKey::UnattachedGroup => DetailEntity::Unattached,
    }
}

fn activity_matches_scope(
    model: &DomainModel,
    full_rows: &[TreeRow],
    selected: &NodeKey,
    _mode: ViewMode,
    item: &ActivityItem,
) -> bool {
    match selected {
        NodeKey::Session => true,
        NodeKey::Run { run_id, .. } => item.task_run_id == Some(*run_id),
        NodeKey::Agent { agent_node_id, .. } => {
            let descendants = agent_descendants(model, agent_node_id);
            item.agent_node_id
                .as_ref()
                .is_some_and(|id| descendants.contains(id))
        }
        NodeKey::Workspace(workspace_id) => activity_matches_topology_scope(
            item.workspace_id.as_deref() == Some(workspace_id),
            full_rows,
            selected,
            item,
        ),
        NodeKey::Tab(tab_id) => activity_matches_topology_scope(
            item.tab_id.as_deref() == Some(tab_id),
            full_rows,
            selected,
            item,
        ),
        NodeKey::Pane(pane_id) => activity_matches_topology_scope(
            item.pane_id.as_deref() == Some(pane_id),
            full_rows,
            selected,
            item,
        ),
        NodeKey::UnattachedGroup => activity_matches_display_subtree(full_rows, selected, item),
    }
}

fn activity_matches_topology_scope(
    topology_matches: bool,
    rows: &[TreeRow],
    selected: &NodeKey,
    item: &ActivityItem,
) -> bool {
    if item.task_run_id.is_some() || item.agent_node_id.is_some() {
        activity_matches_display_subtree(rows, selected, item)
    } else {
        topology_matches
    }
}

fn activity_matches_display_subtree(
    rows: &[TreeRow],
    selected: &NodeKey,
    item: &ActivityItem,
) -> bool {
    let Some(index) = rows.iter().position(|row| &row.key == selected) else {
        return false;
    };
    let depth = rows[index].depth;
    rows[index..]
        .iter()
        .take_while(|row| row.key == *selected || row.depth > depth)
        .any(|row| match &row.key {
            NodeKey::Run { run_id, .. } => item.task_run_id == Some(*run_id),
            NodeKey::Agent { agent_node_id, .. } => {
                item.agent_node_id.as_deref() == Some(agent_node_id)
            }
            _ => false,
        })
}

fn agent_descendants(model: &DomainModel, selected: &str) -> HashSet<String> {
    let mut descendants = HashSet::from([selected.to_owned()]);
    loop {
        let before = descendants.len();
        for agent in model.agent_nodes() {
            if agent
                .parent_agent_node_id
                .as_ref()
                .is_some_and(|parent| descendants.contains(parent))
            {
                descendants.insert(agent.agent_node_id.clone());
            }
        }
        if descendants.len() == before {
            return descendants;
        }
    }
}

fn compare_activity(left: &ActivityItem, right: &ActivityItem) -> Ordering {
    right
        .event_timestamp_ms
        .cmp(&left.event_timestamp_ms)
        .then_with(|| right.seen_at_ms.cmp(&left.seen_at_ms))
        .then_with(|| right.ingest_seq.is_some().cmp(&left.ingest_seq.is_some()))
        .then_with(|| right.ingest_seq.cmp(&left.ingest_seq))
        .then_with(|| right.identity.event_id.cmp(&left.identity.event_id))
}

pub(crate) fn detail_lines(detail: &DetailProjection) -> Vec<String> {
    let mut lines = entity_lines(&detail.entity);
    lines.push(format!(
        "activity window: matching={} shown={} bound={} truncated={} retained_session={}",
        detail.activity.matching_count,
        detail.activity.items.len(),
        detail.activity.bound,
        detail.activity.truncated,
        detail.activity.retained_count,
    ));
    for item in &detail.activity.items {
        lines.push(activity_line(item));
    }
    lines
}

fn entity_lines(entity: &DetailEntity) -> Vec<String> {
    match entity {
        DetailEntity::Session => vec![
            "entity: session".to_owned(),
            "scope: all activity".to_owned(),
        ],
        DetailEntity::Workspace { workspace_id } => vec![
            "entity: workspace".to_owned(),
            format!("workspace_id: {workspace_id}"),
            "scope: current display subtree".to_owned(),
        ],
        DetailEntity::Tab {
            tab_id,
            workspace_id,
        } => vec![
            "entity: tab".to_owned(),
            format!("tab_id: {tab_id}"),
            format!("workspace_id: {workspace_id}"),
            "scope: current display subtree".to_owned(),
        ],
        DetailEntity::Pane {
            pane_id,
            workspace_id,
            tab_id,
        } => vec![
            "entity: pane".to_owned(),
            format!("pane_id: {pane_id}"),
            format!("workspace_id: {workspace_id}"),
            format!("tab_id: {tab_id}"),
            "scope: current display subtree".to_owned(),
        ],
        DetailEntity::Run {
            run_id,
            key,
            name,
            native_session_id,
            state,
            created_at_ms,
            updated_at_ms,
            finished_at_ms,
            dismissed_at_ms,
        } => vec![
            "entity: task_run".to_owned(),
            format!("key: {key}"),
            format!("run_id: {run_id}"),
            format!("name: {name}"),
            format!(
                "native_session_id: {}",
                native_session_id.as_deref().unwrap_or("unknown")
            ),
            format!("state: {}", task_state_name(*state)),
            format!("created_at_ms: {}", timestamp_text(*created_at_ms)),
            format!("updated_at_ms: {}", timestamp_text(*updated_at_ms)),
            format!("finished_at_ms: {}", timestamp_text(*finished_at_ms)),
            format!("dismissed_at_ms: {}", timestamp_text(*dismissed_at_ms)),
            "scope: semantic run and agent descendants".to_owned(),
        ],
        DetailEntity::Agent {
            agent_node_id,
            native_session_id,
            provider,
            state,
            model_id,
            parent_agent_node_id,
            session_file,
        } => vec![
            "entity: agent".to_owned(),
            format!("agent_node_id: {agent_node_id}"),
            format!(
                "native_session_id: {}",
                native_session_id.as_deref().unwrap_or("unknown")
            ),
            format!("provider: {}", provider_name(*provider)),
            format!(
                "state: {}",
                state.as_ref().map_or("unknown", exec_state_name)
            ),
            format!("model: {}", model_id.as_deref().unwrap_or("unknown")),
            format!(
                "parent_agent_node_id: {}",
                parent_agent_node_id.as_deref().unwrap_or("none")
            ),
            format!(
                "session_file: {}",
                session_file.as_deref().unwrap_or("unknown")
            ),
            "scope: agent and native descendants".to_owned(),
        ],
        DetailEntity::Unattached => vec![
            "entity: unattached".to_owned(),
            "scope: current display subtree".to_owned(),
        ],
        DetailEntity::Missing => vec!["entity: unavailable".to_owned()],
    }
}

pub(crate) fn activity_line(item: &ActivityItem) -> String {
    let durability = match item.durability {
        ActivityDurability::Durable => "durable",
        ActivityDurability::CurrentOnly => "current_only",
        ActivityDurability::DurabilityUnknown => "durability_unknown",
    };
    let provider = item.provider.map_or("unknown", provider_name);
    let mut line = format!(
        "at={} kind={} source_type={} provider={} durability={}",
        item.event_timestamp_ms,
        escape_controls(&item.normalized_kind),
        escape_controls(&item.source_event_type),
        provider,
        durability,
    );
    if let Some(model) = &item.model_id {
        line.push_str(&format!(" model={}", escape_controls(model)));
    }
    if let Some(tool) = &item.tool_name {
        line.push_str(&format!(" tool={}", escape_controls(tool)));
    }
    if let Some(label) = &item.controller_label {
        line.push_str(&format!(" label={}", escape_controls(label)));
    }
    if let Some(reason) = &item.controller_reason {
        line.push_str(&format!(" reason={}", escape_controls(reason)));
    }
    line
}

pub(crate) fn runtime_strip(
    quality: ObservationQuality,
    diagnostics: &RuntimeDiagnosticsSnapshot,
) -> RuntimeStrip {
    let (quality, persistence) = match diagnostics.persistence {
        PersistenceStatus::Healthy => (quality, "healthy"),
        PersistenceStatus::Degraded { .. } => (ObservationQuality::Degraded, "degraded"),
    };
    let controller = match diagnostics.controller_input {
        ControllerInputStatus::Available => "available",
        ControllerInputStatus::Unavailable { .. } => "unavailable",
    };
    RuntimeStrip {
        quality,
        persistence,
        controller,
        d4: diagnostics.dangling_announcement_components,
    }
}

pub(crate) fn safe_run_name(run_id: &RunId, key: &RunKey) -> String {
    match key {
        RunKey::Controller(_) => run_id.to_string(),
        RunKey::Native { provider, sid } => {
            format!("{} {}", provider_name(*provider), escape_controls(sid))
        }
        RunKey::NativePath { provider, .. } => {
            format!("{} {run_id}", provider_name(*provider))
        }
        RunKey::Provisional {
            terminal_id,
            start_ms,
            seq,
        } => format!(
            "provisional {}:{start_ms}:{seq}",
            escape_controls(terminal_id)
        ),
    }
}

fn detail_run_key(run_id: &RunId, key: &RunKey) -> String {
    match key {
        RunKey::Controller(name) => escape_controls(name),
        RunKey::Native { .. } | RunKey::NativePath { .. } | RunKey::Provisional { .. } => {
            safe_run_name(run_id, key)
        }
    }
}

/// Uses a native run's own session id; otherwise chooses the bound agent session with the
/// greatest `(last_activity_at_ms, agent_node_id)` tuple so unordered model iteration is stable.
fn bound_native_session_id(model: &DomainModel, run_id: &RunId, key: &RunKey) -> Option<String> {
    if let RunKey::Native { sid, .. } = key {
        return Some(escape_controls(sid));
    }
    model
        .agent_nodes()
        .filter(|agent| agent.task_run_id == *run_id)
        .filter_map(|agent| {
            agent
                .native_session_id
                .as_deref()
                .map(|native_session_id| (agent, native_session_id))
        })
        .max_by(|(left, _), (right, _)| {
            (left.last_activity_at_ms, left.agent_node_id.as_str())
                .cmp(&(right.last_activity_at_ms, right.agent_node_id.as_str()))
        })
        .map(|(_, native_session_id)| escape_controls(native_session_id))
}

fn timestamp_text(timestamp_ms: Option<i64>) -> String {
    timestamp_ms.map_or_else(|| "unknown".to_owned(), |value| value.to_string())
}

pub(crate) const fn provider_name(provider: Provider) -> &'static str {
    match provider {
        Provider::Claude => "Claude",
        Provider::Codex => "Codex",
    }
}

pub(crate) const fn task_state_name(state: TaskState) -> &'static str {
    match state {
        TaskState::Queued => "queued",
        TaskState::Running => "running",
        TaskState::Blocked => "blocked",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
        TaskState::EndedUnknown => "ended_unknown",
    }
}

pub(crate) const fn exec_state_name(state: &ExecState) -> &'static str {
    match state {
        ExecState::Unknown => "unknown",
        ExecState::Idle => "idle",
        ExecState::Working => "working",
        ExecState::Blocked => "blocked",
        ExecState::Stale { .. } => "stale",
        ExecState::Ended => "ended",
    }
}

pub(crate) fn escape_controls(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                escaped.push_str(&format!("\\u{{{:x}}}", u32::from(character)));
            }
            character => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::ffi::OsStr;
    use std::sync::Arc;

    use crate::activity::{ActivityDurability, ActivityIdentity, ActivityItem, OperatorSnapshot};
    use crate::diagnostics::{
        ControllerCounterSnapshot, ControllerInputStatus, OccurrenceLogStatus, OwnerFreshness,
        PersistenceCounters, RuntimeDiagnosticsSnapshot,
    };
    use crate::herdr::collector::ObservationQuality;
    use crate::model::{
        AgentNode, DependencyEdge, DisplayOrdinal, DomainModel, ExecState, ExecutionEdge, Provider,
        RunId, RunKey, TaskRun, TaskState,
    };
    use crate::store::writer::{
        DurabilityDisposition, PersistenceFailure, PersistenceFailureCode, PersistenceOperation,
        PersistencePhase, PersistenceStatus,
    };
    use crate::tui::app::{NodeKey, ViewMode};
    use crate::tui::view::TreeRow;

    use super::*;

    fn run(label: &str, ordinal: i64, state: TaskState) -> TaskRun {
        TaskRun {
            run_id: RunId::new(),
            key: RunKey::Controller(label.to_owned()),
            display_ordinal: DisplayOrdinal::new(ordinal),
            state,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        }
    }

    #[test]
    fn stall_suppressed_while_descendant_active() {
        let mut parent = run("parent", 1, TaskState::Running);
        parent.updated_at_ms = Some(0);
        let mut child = run("child", 2, TaskState::Running);
        child.updated_at_ms = Some(900);
        let mut model = DomainModel::default();
        model.insert_task_run(parent.clone());
        model.insert_task_run(child.clone());
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent.run_id,
            child_run_id: child.run_id,
        });

        let stalled = stalled_run_ids(&model, 1_000, 500);
        assert!(!stalled.contains(&parent.run_id));
        assert!(!stalled.contains(&child.run_id));

        child.updated_at_ms = Some(0);
        model.insert_task_run(child.clone());
        let stalled = stalled_run_ids(&model, 1_000, 500);
        assert!(stalled.contains(&parent.run_id));
        assert!(stalled.contains(&child.run_id));

        child.updated_at_ms = None;
        child.created_at_ms = Some(0);
        model.insert_task_run(child.clone());
        let stalled = stalled_run_ids(&model, 1_000, 500);
        assert!(stalled.contains(&child.run_id));
    }

    fn row(key: NodeKey, depth: usize, label: &str) -> TreeRow {
        TreeRow {
            key,
            depth,
            label: label.to_owned(),
            prerequisites: Vec::new(),
            dependents: Vec::new(),
        }
    }

    fn operator(
        activity: Vec<ActivityItem>,
        terminal_times: HashMap<RunId, i64>,
    ) -> OperatorSnapshot {
        OperatorSnapshot {
            activity: Arc::from(activity),
            terminal_times: Arc::new(terminal_times),
        }
    }

    fn activity(
        event_id: impl Into<String>,
        timestamp: i64,
        run_id: Option<RunId>,
        agent_id: Option<&str>,
        durability: ActivityDurability,
    ) -> ActivityItem {
        ActivityItem {
            identity: ActivityIdentity {
                event_id: event_id.into(),
            },
            event_timestamp_ms: timestamp,
            seen_at_ms: timestamp,
            ingest_seq: Some(timestamp.unsigned_abs()),
            source: "provider".to_owned(),
            normalized_kind: "agent_activity".to_owned(),
            source_event_type: "item".to_owned(),
            workspace_id: Some("workspace".to_owned()),
            tab_id: Some("tab".to_owned()),
            pane_id: Some("pane".to_owned()),
            terminal_id: Some("terminal".to_owned()),
            provider: Some(Provider::Codex),
            native_session_id: Some("native".to_owned()),
            task_run_id: run_id,
            agent_node_id: agent_id.map(str::to_owned),
            task_state: Some(TaskState::Running),
            model_id: Some("gpt-safe".to_owned()),
            provider_event_kind: Some("assistant".to_owned()),
            tool_name: Some("Read".to_owned()),
            item_count: Some(1),
            byte_count: Some(2),
            provider_agent_id: agent_id.map(str::to_owned),
            provider_parent_agent_id: None,
            controller_label: None,
            controller_reason: None,
            durability,
        }
    }

    fn diagnostics(
        persistence: PersistenceStatus,
        controller_input: ControllerInputStatus,
    ) -> RuntimeDiagnosticsSnapshot {
        RuntimeDiagnosticsSnapshot {
            persistence,
            controller_input,
            owner: OwnerFreshness::Current,
            persistence_counters: PersistenceCounters::default(),
            controller_counters: ControllerCounterSnapshot::default(),
            enrichment_counters: crate::diagnostics::EnrichmentCounterSnapshot::default(),
            source_coverage: Vec::new(),
            dangling_announcement_components: 7,
            first_failure_log: OccurrenceLogStatus::NotAttempted,
        }
    }

    #[test]
    fn i4_filtered_tree_retains_complete_ancestor_chain() {
        let matching = run("controller-private-label", 1, TaskState::Running);
        let sibling = run("sibling", 2, TaskState::Running);
        let mut model = DomainModel::default();
        model.insert_task_run(matching.clone());
        model.insert_task_run(sibling.clone());
        let rows = vec![
            row(NodeKey::Session, 0, "Session: demo"),
            row(
                NodeKey::Workspace("workspace".into()),
                1,
                "Workspace: workspace",
            ),
            row(NodeKey::Tab("tab".into()), 2, "Tab: tab"),
            row(NodeKey::Pane("pane".into()), 3, "Pane: pane"),
            row(
                NodeKey::Run {
                    run_id: matching.run_id,
                    pane_id: Some("pane".into()),
                },
                4,
                "Task Run: private [running]",
            ),
            row(
                NodeKey::Agent {
                    agent_node_id: "needle-agent".into(),
                    pane_id: Some("pane".into()),
                },
                5,
                "Codex native agent: needle-agent",
            ),
            row(
                NodeKey::Run {
                    run_id: sibling.run_id,
                    pane_id: Some("pane".into()),
                },
                4,
                "Task Run: sibling [running]",
            ),
        ];
        let projected = project_rows(
            &model,
            &rows,
            &operator(Vec::new(), HashMap::new()),
            "NEEDLE-agent",
            &HashSet::new(),
            ViewMode::ExecutionTree,
            10,
        );

        assert_eq!(
            projected
                .rows
                .iter()
                .map(|item| &item.key)
                .collect::<Vec<_>>(),
            rows[..6].iter().map(|item| &item.key).collect::<Vec<_>>()
        );
    }

    #[test]
    fn i4_terminal_expiry_filter_restore_and_no_event_wakeup() {
        let mut old = run("controller-private", 1, TaskState::Completed);
        old.key = RunKey::Native {
            provider: Provider::Codex,
            sid: "old-searchable".to_owned(),
        };
        let fresh = run("fresh", 2, TaskState::Completed);
        let live = run("live", 3, TaskState::Running);
        let mut model = DomainModel::default();
        for item in [&old, &fresh, &live] {
            model.insert_task_run(item.clone());
        }
        let rows = [
            (&old, "Task Run: old-searchable [completed]"),
            (&fresh, "Task Run: fresh [completed]"),
            (&live, "Task Run: live [running]"),
        ]
        .into_iter()
        .map(|(item, label)| {
            row(
                NodeKey::Run {
                    run_id: item.run_id,
                    pane_id: None,
                },
                0,
                label,
            )
        })
        .collect::<Vec<_>>();
        let now = 7_200_000;
        let terminal_times = HashMap::from([(old.run_id, 0), (fresh.run_id, now - 3_599_999)]);
        let operator = operator(Vec::new(), terminal_times);

        let default = project_rows(
            &model,
            &rows,
            &operator,
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            now,
        );
        assert_eq!(default.rows.len(), 2);
        assert_eq!(default.next_expiry_ms, Some(now + 1));
        let after_deadline = project_rows(
            &model,
            &rows,
            &operator,
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            now + 1,
        );
        assert_eq!(after_deadline.rows.len(), 1);
        let filtered = project_rows(
            &model,
            &rows,
            &operator,
            "old-searchable",
            &HashSet::new(),
            ViewMode::DependencyDag,
            now + 1,
        );
        assert_eq!(filtered.rows.len(), 1);
        assert_eq!(filtered.rows[0].key.run_id(), Some(old.run_id));
        let restored = project_rows(
            &model,
            &rows,
            &operator,
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            now + 1,
        );
        assert_eq!(restored.rows[0].key.run_id(), Some(live.run_id));
    }

    #[test]
    fn hook_only_run_schedules_twenty_four_hour_expiry() {
        let mut hook_only = run("hook-only", 1, TaskState::Running);
        hook_only.updated_at_ms = Some(10);
        let mut model = DomainModel::default();
        model.insert_task_run(hook_only.clone());
        let rows = vec![row(
            NodeKey::Run {
                run_id: hook_only.run_id,
                pane_id: None,
            },
            0,
            "Task Run: hook-only [running]",
        )];

        let projection = project_rows(
            &model,
            &rows,
            &operator(Vec::new(), HashMap::new()),
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            20,
        );

        assert_eq!(projection.rows.len(), 1);
        assert_eq!(projection.next_expiry_ms, Some(86_400_010));
    }

    #[test]
    fn hook_only_and_live_duration_deadlines_choose_the_minimum_in_both_directions() {
        let mut hook_only = run("hook-only", 1, TaskState::Running);
        hook_only.created_at_ms = Some(0);
        hook_only.updated_at_ms = Some(1);
        let mut model = DomainModel::default();
        model.insert_task_run(hook_only.clone());
        let rows = vec![row(
            NodeKey::Run {
                run_id: hook_only.run_id,
                pane_id: None,
            },
            0,
            "Task Run: hook-only [running]",
        )];
        let operator = operator(Vec::new(), HashMap::new());

        let duration_first = project_rows(
            &model,
            &rows,
            &operator,
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            500,
        );
        assert_eq!(duration_first.next_expiry_ms, Some(1_000));

        let hook_only_first = project_rows(
            &model,
            &rows,
            &operator,
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            86_400_000,
        );
        assert_eq!(hook_only_first.next_expiry_ms, Some(86_400_001));
    }

    #[test]
    fn projection_uses_earliest_terminal_or_hook_only_expiry() {
        let mut hook_only = run("hook-only", 1, TaskState::Running);
        hook_only.updated_at_ms = Some(16_000_001);
        let terminal = run("terminal", 2, TaskState::Completed);
        let mut model = DomainModel::default();
        model.insert_task_run(hook_only.clone());
        model.insert_task_run(terminal.clone());
        let rows = [&hook_only, &terminal]
            .into_iter()
            .map(|task_run| {
                row(
                    NodeKey::Run {
                        run_id: task_run.run_id,
                        pane_id: None,
                    },
                    0,
                    "Task Run",
                )
            })
            .collect::<Vec<_>>();

        let projection = project_rows(
            &model,
            &rows,
            &operator(Vec::new(), HashMap::from([(terminal.run_id, 99_000_000)])),
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            100_000_000,
        );

        assert_eq!(projection.rows.len(), 2);
        assert_eq!(projection.next_expiry_ms, Some(102_400_001));
    }

    #[test]
    fn filtered_visible_live_duration_ticks_but_filtered_or_collapsed_rows_do_not() {
        let mut live = run("live", 1, TaskState::Running);
        live.created_at_ms = Some(100);
        let mut model = DomainModel::default();
        model.insert_task_run(live.clone());
        let run_row = row(
            NodeKey::Run {
                run_id: live.run_id,
                pane_id: None,
            },
            1,
            "live row",
        );
        let operator = operator(Vec::new(), HashMap::new());

        let filtered_visible = project_rows(
            &model,
            std::slice::from_ref(&run_row),
            &operator,
            &live.run_id.to_string(),
            &HashSet::new(),
            ViewMode::DependencyDag,
            500,
        );
        assert_eq!(filtered_visible.rows.len(), 1);
        assert_eq!(filtered_visible.next_expiry_ms, Some(1_000));

        let filtered_out = project_rows(
            &model,
            std::slice::from_ref(&run_row),
            &operator,
            "no-match",
            &HashSet::new(),
            ViewMode::DependencyDag,
            500,
        );
        assert!(filtered_out.rows.is_empty());
        assert_eq!(filtered_out.next_expiry_ms, None);

        let parent = row(NodeKey::Pane("pane".to_owned()), 0, "pane");
        let collapsed = project_rows(
            &model,
            &[parent.clone(), run_row],
            &operator,
            "",
            &HashSet::from([parent.key]),
            ViewMode::ExecutionTree,
            500,
        );
        assert_eq!(collapsed.rows.len(), 1);
        assert_eq!(collapsed.next_expiry_ms, None);
    }

    #[test]
    fn phase_distinct_live_runs_share_one_wall_aligned_duration_deadline() {
        let mut model = DomainModel::default();
        let runs = [
            ("phase-zero", 0),
            ("phase-quarter", 250),
            ("phase-nine-hundred", 900),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (label, created_at_ms))| {
            let mut live = run(label, index as i64 + 1, TaskState::Running);
            live.created_at_ms = Some(created_at_ms);
            model.insert_task_run(live.clone());
            live
        })
        .collect::<Vec<_>>();
        let rows = runs
            .iter()
            .map(|live| {
                row(
                    NodeKey::Run {
                        run_id: live.run_id,
                        pane_id: None,
                    },
                    0,
                    "live row",
                )
            })
            .collect::<Vec<_>>();

        let projection = project_rows(
            &model,
            &rows,
            &operator(Vec::new(), HashMap::new()),
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            1_500,
        );

        assert_eq!(projection.rows.len(), 3);
        assert_eq!(projection.next_expiry_ms, Some(2_000));
    }

    #[test]
    fn clock_skewed_live_duration_does_not_arm_a_deadline() {
        let mut live = run("live", 1, TaskState::Running);
        live.created_at_ms = Some(501);
        let mut model = DomainModel::default();
        model.insert_task_run(live.clone());
        let rows = vec![row(
            NodeKey::Run {
                run_id: live.run_id,
                pane_id: None,
            },
            0,
            "live row",
        )];

        let projection = project_rows(
            &model,
            &rows,
            &operator(Vec::new(), HashMap::new()),
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            500,
        );

        assert_eq!(projection.rows.len(), 1);
        assert_eq!(projection.next_expiry_ms, None);
    }

    #[test]
    fn dismissed_run_contributes_no_projection_expiry() {
        let mut dismissed = run("dismissed", 1, TaskState::Running);
        dismissed.updated_at_ms = Some(10);
        dismissed.dismissed_at_ms = Some(20);
        let mut model = DomainModel::default();
        model.insert_task_run(dismissed.clone());
        let rows = vec![row(
            NodeKey::Run {
                run_id: dismissed.run_id,
                pane_id: None,
            },
            0,
            "Task Run: dismissed [running]",
        )];

        let projection = project_rows(
            &model,
            &rows,
            &operator(Vec::new(), HashMap::new()),
            "",
            &HashSet::new(),
            ViewMode::DependencyDag,
            20,
        );

        assert!(projection.rows.is_empty());
        assert_eq!(projection.next_expiry_ms, None);
    }

    #[test]
    fn run_detail_carries_hidden_controller_identity_session_and_timestamps() {
        let full_key = "hook:claude-code:session-42:task:T";
        let mut selected = run(full_key, 1, TaskState::Completed);
        selected.subject = Some("Readable subject".to_owned());
        selected.created_at_ms = Some(1_000);
        selected.updated_at_ms = Some(4_000);
        selected.finished_at_ms = Some(5_000);
        selected.dismissed_at_ms = None;
        let lower = AgentNode {
            agent_node_id: "agent-a".to_owned(),
            provider: Provider::Claude,
            native_session_id: Some("native-lower".to_owned()),
            task_run_id: selected.run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Ended),
            model_id: Some("model-lower".to_owned()),
            last_event_kind: Some("message".to_owned()),
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: Some(4_000),
            session_file: None,
        };
        let higher = AgentNode {
            agent_node_id: "agent-z".to_owned(),
            native_session_id: Some("native-higher".to_owned()),
            ..lower.clone()
        };
        let mut model = DomainModel::default();
        model.insert_task_run(selected.clone());
        for agent in [higher, lower] {
            model.insert_agent_node(agent);
        }

        let newest_agents = crate::tui::view::newest_agent_nodes(&model);
        let label = crate::tui::view::task_run_label(
            &model,
            &selected,
            false,
            9_000,
            newest_agents.get(&selected.run_id).copied(),
        );
        assert!(!label.contains(full_key));
        let selected_row = row(
            NodeKey::Run {
                run_id: selected.run_id,
                pane_id: None,
            },
            0,
            &label,
        );
        let detail = detail_projection(
            &model,
            std::slice::from_ref(&selected_row),
            &operator(Vec::new(), HashMap::new()),
            &selected_row.key,
            ViewMode::DependencyDag,
            None,
        );
        let lines = detail_lines(&detail);

        assert!(lines.contains(&format!("key: {full_key}")));
        assert!(lines.contains(&format!("run_id: {}", selected.run_id)));
        assert!(lines.contains(&"native_session_id: native-higher".to_owned()));
        assert!(lines.contains(&"created_at_ms: 1000".to_owned()));
        assert!(lines.contains(&"updated_at_ms: 4000".to_owned()));
        assert!(lines.contains(&"finished_at_ms: 5000".to_owned()));
        assert!(lines.contains(&"dismissed_at_ms: unknown".to_owned()));
    }

    #[test]
    fn native_run_detail_prefers_key_session_and_marks_missing_times_unknown() {
        let mut selected = run("native", 1, TaskState::Running);
        selected.key = RunKey::Native {
            provider: Provider::Codex,
            sid: "key-native-session".to_owned(),
        };
        let agent = AgentNode {
            agent_node_id: "agent-newer".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("agent-native-session".to_owned()),
            task_run_id: selected.run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: Some(10),
            session_file: None,
        };
        let mut model = DomainModel::default();
        model.insert_task_run(selected.clone());
        model.insert_agent_node(agent);
        let selected_row = row(
            NodeKey::Run {
                run_id: selected.run_id,
                pane_id: None,
            },
            0,
            "native run",
        );

        let detail = detail_projection(
            &model,
            std::slice::from_ref(&selected_row),
            &operator(Vec::new(), HashMap::new()),
            &selected_row.key,
            ViewMode::DependencyDag,
            None,
        );
        let lines = detail_lines(&detail);

        assert!(lines.contains(&"native_session_id: key-native-session".to_owned()));
        for field in [
            "created_at_ms: unknown",
            "updated_at_ms: unknown",
            "finished_at_ms: unknown",
            "dismissed_at_ms: unknown",
        ] {
            assert!(lines.contains(&field.to_owned()));
        }
    }

    #[test]
    fn i4_detail_scope_order_bound_current_only_and_dag_semantics() {
        let prerequisite = run("prerequisite", 1, TaskState::Running);
        let selected = run("selected", 2, TaskState::Running);
        let dependent = run("dependent", 3, TaskState::Running);
        let mut model = DomainModel::default();
        for item in [&prerequisite, &selected, &dependent] {
            model.insert_task_run(item.clone());
        }
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: prerequisite.run_id,
            dependent_run_id: selected.run_id,
        });
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: selected.run_id,
            dependent_run_id: dependent.run_id,
        });
        let rows = [&prerequisite, &selected, &dependent]
            .into_iter()
            .map(|item| {
                row(
                    NodeKey::Run {
                        run_id: item.run_id,
                        pane_id: None,
                    },
                    0,
                    "run",
                )
            })
            .collect::<Vec<_>>();
        let mut items = (0..105)
            .map(|index| {
                activity(
                    format!("selected-{index:03}"),
                    index,
                    Some(selected.run_id),
                    None,
                    ActivityDurability::CurrentOnly,
                )
            })
            .collect::<Vec<_>>();
        let mut prerequisite_activity = activity(
            "prerequisite-private",
            1_000,
            Some(prerequisite.run_id),
            None,
            ActivityDurability::Durable,
        );
        prerequisite_activity.pane_id = Some("unrelated-pane-a".to_owned());
        items.push(prerequisite_activity);
        let mut dependent_activity = activity(
            "dependent-private",
            1_001,
            Some(dependent.run_id),
            None,
            ActivityDurability::Durable,
        );
        dependent_activity.pane_id = Some("unrelated-pane-b".to_owned());
        items.push(dependent_activity);
        let mut lineage_free = activity(
            "lineage-free",
            2_000,
            None,
            None,
            ActivityDurability::Durable,
        );
        lineage_free.workspace_id = None;
        lineage_free.tab_id = None;
        lineage_free.pane_id = None;
        lineage_free.terminal_id = None;
        items.push(lineage_free);
        let operator_snapshot = operator(items, HashMap::new());

        let detail = detail_projection(
            &model,
            &rows,
            &operator_snapshot,
            &NodeKey::Run {
                run_id: selected.run_id,
                pane_id: None,
            },
            ViewMode::DependencyDag,
            None,
        );

        assert_eq!(detail.activity.matching_count, 105);
        assert_eq!(detail.activity.items.len(), 100);
        assert!(detail.activity.truncated);
        assert_eq!(detail.activity.bound, 100);
        assert_eq!(detail.activity.items[0].identity.event_id, "selected-104");
        assert!(
            detail
                .activity
                .items
                .iter()
                .all(|item| item.task_run_id == Some(selected.run_id))
        );
        assert!(
            detail
                .activity
                .items
                .iter()
                .all(|item| item.durability == ActivityDurability::CurrentOnly)
        );

        let displayed_subtree = vec![
            row(NodeKey::Workspace("workspace".to_owned()), 0, "workspace"),
            row(
                NodeKey::Run {
                    run_id: selected.run_id,
                    pane_id: Some("pane".to_owned()),
                },
                1,
                "selected",
            ),
        ];
        let workspace_detail = detail_projection(
            &model,
            &displayed_subtree,
            &operator_snapshot,
            &displayed_subtree[0].key,
            ViewMode::ExecutionTree,
            None,
        );
        assert_eq!(workspace_detail.activity.matching_count, 105);
        assert!(
            workspace_detail
                .activity
                .items
                .iter()
                .all(|item| item.task_run_id == Some(selected.run_id)),
            "off-screen runs sharing topology metadata are outside current display subtree",
        );

        let session_detail = detail_projection(
            &model,
            &rows,
            &operator_snapshot,
            &NodeKey::Session,
            ViewMode::ExecutionTree,
            None,
        );
        assert_eq!(session_detail.activity.matching_count, 108);
        assert_eq!(session_detail.activity.items.len(), 100);
        assert!(session_detail.activity.truncated);
        assert_eq!(
            session_detail.activity.items[0].identity.event_id,
            "lineage-free"
        );
        assert_eq!(
            session_detail.activity.items[1].identity.event_id,
            "dependent-private"
        );
        assert_eq!(
            session_detail.activity.items[2].identity.event_id,
            "prerequisite-private"
        );

        let agents = [
            ("parent-agent", None, 10),
            ("child-agent", Some("parent-agent"), 11),
            ("grandchild-agent", Some("child-agent"), 12),
            ("sibling-agent", None, 13),
        ];
        for (agent_node_id, parent_agent_node_id, ordinal) in agents {
            model.insert_agent_node(AgentNode {
                agent_node_id: agent_node_id.to_owned(),
                provider: Provider::Codex,
                native_session_id: Some(format!("native-{agent_node_id}")),
                task_run_id: selected.run_id,
                display_ordinal: DisplayOrdinal::new(ordinal),
                parent_agent_node_id: parent_agent_node_id.map(str::to_owned),
                state: Some(ExecState::Working),
                model_id: Some("gpt-safe".to_owned()),
                last_event_kind: None,
                last_tool_name: None,
                last_item_count: None,
                last_byte_count: None,
                last_activity_at_ms: None,
                session_file: None,
            });
        }
        let descendant_operator = operator(
            vec![
                activity(
                    "parent-activity",
                    10,
                    Some(selected.run_id),
                    Some("parent-agent"),
                    ActivityDurability::Durable,
                ),
                activity(
                    "child-activity",
                    20,
                    Some(selected.run_id),
                    Some("child-agent"),
                    ActivityDurability::Durable,
                ),
                activity(
                    "grandchild-activity",
                    30,
                    Some(selected.run_id),
                    Some("grandchild-agent"),
                    ActivityDurability::Durable,
                ),
                activity(
                    "sibling-activity",
                    40,
                    Some(selected.run_id),
                    Some("sibling-agent"),
                    ActivityDurability::Durable,
                ),
                activity(
                    "unowned-activity",
                    50,
                    Some(selected.run_id),
                    None,
                    ActivityDurability::Durable,
                ),
            ],
            HashMap::new(),
        );
        let agent_detail = detail_projection(
            &model,
            &[row(
                NodeKey::Agent {
                    agent_node_id: "parent-agent".to_owned(),
                    pane_id: Some("pane".to_owned()),
                },
                0,
                "parent agent",
            )],
            &descendant_operator,
            &NodeKey::Agent {
                agent_node_id: "parent-agent".to_owned(),
                pane_id: Some("pane".to_owned()),
            },
            ViewMode::ExecutionTree,
            None,
        );
        assert_eq!(agent_detail.activity.matching_count, 3);
        assert_eq!(
            agent_detail
                .activity
                .items
                .iter()
                .map(|item| item.identity.event_id.as_str())
                .collect::<Vec<_>>(),
            ["grandchild-activity", "child-activity", "parent-activity"]
        );
    }

    #[test]
    fn i4_activity_identity_and_native_path_never_render() {
        const IDENTITY_SENTINEL: &str = "ZZZ_ACTIVITY_IDENTITY_FORBIDDEN_I4_A1";
        const NATIVE_PATH: &str = "/home/alice/private/NATIVE_PATH_FORBIDDEN_I4_A2.jsonl";

        let mut parent = run("parent", 1, TaskState::Running);
        parent.key = RunKey::NativePath {
            provider: Provider::Codex,
            path: NATIVE_PATH.to_owned(),
        };
        let child = run("child", 2, TaskState::Running);
        let mut model = DomainModel::default();
        model.insert_task_run(parent.clone());
        model.insert_task_run(child.clone());
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent.run_id,
            child_run_id: child.run_id,
        });
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: parent.run_id,
            dependent_run_id: child.run_id,
        });

        let tree_label = crate::tui::view::task_run_label(&model, &child, false, 0, None);
        let mut dag_order = crate::tui::dag::DagOrder::default();
        dag_order.recompute(&model);
        let newest_agents = crate::tui::view::newest_agent_nodes(&model);
        let dag_text = crate::tui::dag::build_rows(&model, &dag_order, 0, &newest_agents)
            .into_iter()
            .flat_map(|row| {
                std::iter::once(row.label)
                    .chain(row.prerequisites)
                    .chain(row.dependents)
            })
            .collect::<Vec<_>>()
            .join("\n");
        for rendered in [&tree_label, &dag_text] {
            assert!(!rendered.contains(NATIVE_PATH));
            assert!(!rendered.contains("NATIVE_PATH_FORBIDDEN_I4_A2.jsonl"));
            assert!(rendered.contains(&parent.run_id.to_string()));
        }

        let child_row = row(
            NodeKey::Run {
                run_id: child.run_id,
                pane_id: Some("pane".to_owned()),
            },
            0,
            &crate::tui::view::task_run_label(&model, &child, false, 0, None),
        );
        let tied = operator(
            vec![
                activity(
                    "AAA-order-second",
                    10,
                    Some(child.run_id),
                    None,
                    ActivityDurability::Durable,
                ),
                activity(
                    IDENTITY_SENTINEL,
                    10,
                    Some(child.run_id),
                    None,
                    ActivityDurability::Durable,
                ),
            ],
            HashMap::new(),
        );
        let detail = detail_projection(
            &model,
            std::slice::from_ref(&child_row),
            &tied,
            &child_row.key,
            ViewMode::ExecutionTree,
            None,
        );
        assert_eq!(
            detail.activity.items[0].identity.event_id, IDENTITY_SENTINEL,
            "identity remains an internal deterministic tie-breaker"
        );
        assert!(!detail_lines(&detail).join("\n").contains(IDENTITY_SENTINEL));
        assert!(!activity_line(&detail.activity.items[0]).contains(IDENTITY_SENTINEL));
        assert!(!row_matches(&model, &child_row, IDENTITY_SENTINEL));
    }

    #[test]
    fn i4_detail_distinguishes_durable_current_only_and_unknown() {
        let selected = run("selected", 1, TaskState::Running);
        let mut model = DomainModel::default();
        model.insert_task_run(selected.clone());
        let rows = vec![row(
            NodeKey::Run {
                run_id: selected.run_id,
                pane_id: None,
            },
            0,
            "run",
        )];
        let operator = operator(
            vec![
                activity(
                    "durable",
                    3,
                    Some(selected.run_id),
                    None,
                    ActivityDurability::Durable,
                ),
                activity(
                    "current",
                    2,
                    Some(selected.run_id),
                    None,
                    ActivityDurability::CurrentOnly,
                ),
                activity(
                    "unknown",
                    1,
                    Some(selected.run_id),
                    None,
                    ActivityDurability::DurabilityUnknown,
                ),
            ],
            HashMap::new(),
        );
        let detail = detail_projection(
            &model,
            &rows,
            &operator,
            &rows[0].key,
            ViewMode::DependencyDag,
            None,
        );
        let rendered = detail_lines(&detail).join("\n");

        assert!(rendered.contains("durable"));
        assert!(rendered.contains("current_only"));
        assert!(rendered.contains("durability_unknown"));
    }

    #[test]
    fn i4_agent_detail_formats_session_file_without_searching_it() {
        let selected = run("selected", 1, TaskState::Running);
        let secret = "/home/alice/private/SECRET_SESSION_FILE.jsonl";
        let agent = AgentNode {
            agent_node_id: "agent".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("native".to_owned()),
            task_run_id: selected.run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: Some("gpt-safe".to_owned()),
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: Some(secret.to_owned()),
        };
        let mut model = DomainModel::default();
        model.insert_task_run(selected);
        model.insert_agent_node(agent);
        let rows = vec![row(
            NodeKey::Agent {
                agent_node_id: "agent".into(),
                pane_id: None,
            },
            0,
            "agent",
        )];
        let detail = detail_projection(
            &model,
            &rows,
            &operator(Vec::new(), HashMap::new()),
            &rows[0].key,
            ViewMode::ExecutionTree,
            Some(OsStr::new("/home/alice")),
        );

        assert!(
            detail_lines(&detail)
                .join("\n")
                .contains("~/private/SECRET_SESSION_FILE.jsonl")
        );
        assert!(!row_matches(&model, &rows[0], "secret_session_file"));
    }

    #[test]
    fn i4_runtime_strip_persistence_overrides_quality_controller_does_not() {
        let available = diagnostics(PersistenceStatus::Healthy, ControllerInputStatus::Available);
        let unavailable = diagnostics(
            PersistenceStatus::Healthy,
            ControllerInputStatus::Unavailable {
                reason: crate::diagnostics::ControllerInputUnavailableReason::ListenerUnavailable,
            },
        );
        let failure = PersistenceFailure {
            operation: PersistenceOperation::Apply,
            phase: PersistencePhase::CommandExecution,
            code: PersistenceFailureCode::Sqlite,
            durability: DurabilityDisposition::NotCommitted,
        };
        let degraded = diagnostics(
            PersistenceStatus::Degraded { failure },
            ControllerInputStatus::Available,
        );

        assert_eq!(
            runtime_strip(ObservationQuality::Live, &available).quality,
            ObservationQuality::Live
        );
        assert_eq!(
            runtime_strip(ObservationQuality::Live, &unavailable).quality,
            ObservationQuality::Live
        );
        let strip = runtime_strip(ObservationQuality::Live, &degraded);
        assert_eq!(strip.quality, ObservationQuality::Degraded);
        assert_eq!(strip.persistence, "degraded");
        assert_eq!(strip.controller, "available");
        assert_eq!(strip.d4, 7);
    }
}
