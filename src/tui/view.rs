//! T10 execution-tree rendering and truncation rules.

use std::collections::{HashMap, HashSet};

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::herdr::collector::ObservationQuality;
use crate::model::{
    AgentNode, DisplayOrdinal, DomainModel, ExecState, Provider, RunId, RunKey, TaskRun, TaskState,
};

use super::app::{AppState, HeaderInputs, NodeKey, ViewMode};
use super::dag;

const MIN_WIDTH: u16 = 48;
const MIN_HEIGHT: u16 = 10;
type RunPlacement = (RunId, bool);
type PaneRuns = HashMap<String, Vec<RunPlacement>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TreeRow {
    pub(crate) key: NodeKey,
    pub(crate) depth: usize,
    pub(crate) label: String,
    pub(crate) prerequisites: Vec<String>,
    pub(crate) dependents: Vec<String>,
}

/// Draws a complete fixed-screen frame from immutable model and UI snapshots.
pub(super) fn render(
    frame: &mut Frame<'_>,
    model: &DomainModel,
    quality: ObservationQuality,
    header: &HeaderInputs,
    state: &AppState,
) {
    let area = frame.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        let message = format!("Terminal too small (minimum {MIN_WIDTH}x{MIN_HEIGHT})");
        frame.render_widget(
            Paragraph::new(truncate_to_width(&message, usize::from(area.width))),
            area,
        );
        return;
    }

    let header_area = Rect::new(area.x, area.y, area.width, 3);
    let footer_y = area.y.saturating_add(area.height.saturating_sub(1));
    let footer_area = Rect::new(area.x, footer_y, area.width, 1);
    let activity_y = footer_y.saturating_sub(4);
    let activity_area = Rect::new(area.x, activity_y, area.width, 4);
    let tree_y = area.y.saturating_add(3);
    let tree_area = Rect::new(
        area.x,
        tree_y,
        area.width,
        activity_y.saturating_sub(tree_y),
    );

    render_header(frame, header_area, model, quality, header);
    let rows = build_rows_named(model, state, Some(&header.session));
    match state.view_mode() {
        ViewMode::ExecutionTree => render_tree(frame, tree_area, &rows, state),
        ViewMode::DependencyDag => render_dag(frame, tree_area, &rows, state),
    }
    render_activity(frame, activity_area, &rows, state);
    frame.render_widget(
        Paragraph::new(footer_line(usize::from(footer_area.width))),
        footer_area,
    );
}

fn render_header(
    frame: &mut Frame<'_>,
    area: Rect,
    model: &DomainModel,
    quality: ObservationQuality,
    inputs: &HeaderInputs,
) {
    let block = Block::default().borders(Borders::ALL).title(" Herdr Top ");
    let inner_width = usize::from(area.width.saturating_sub(2));
    let line = header_line(area.width, inner_width, model, quality, inputs);
    frame.render_widget(Paragraph::new(line).block(block), area);
}

fn render_tree(frame: &mut Frame<'_>, area: Rect, rows: &[TreeRow], state: &AppState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Execution tree ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let viewport_height = usize::from(inner.height);
    if viewport_height == 0 {
        return;
    }
    let start = viewport_start(rows, state, viewport_height);
    let width = usize::from(inner.width);
    let lines = rows
        .iter()
        .skip(start)
        .take(viewport_height)
        .map(|row| {
            let selected = state.selected() == Some(&row.key);
            let marker = if selected { "> " } else { "  " };
            let indent = "  ".repeat(row.depth);
            let text = truncate_to_width(&format!("{marker}{indent}{}", row.label), width);
            let style = if selected {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };
            Line::from(Span::styled(text, style))
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(lines), inner);
}

fn render_dag(frame: &mut Frame<'_>, area: Rect, rows: &[TreeRow], state: &AppState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Dependency DAG ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let width = usize::from(inner.width);
    let (run_width, prerequisite_width, dependent_width) = dag_column_widths(width);
    let heading = format!(
        "{} │ {} │ {}",
        pad_to_width("Task Run", run_width),
        pad_to_width("Prereqs", prerequisite_width),
        pad_to_width("Dependents", dependent_width),
    );
    frame.render_widget(
        Paragraph::new(Line::styled(
            truncate_to_width(&heading, width),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Rect::new(inner.x, inner.y, inner.width, 1),
    );

    let viewport_height = usize::from(inner.height.saturating_sub(1));
    if viewport_height == 0 {
        return;
    }
    let start = viewport_start(rows, state, viewport_height);
    let lines = rows
        .iter()
        .skip(start)
        .take(viewport_height)
        .map(|row| {
            let selected = state.selected() == Some(&row.key);
            let marker = if selected { "> " } else { "  " };
            let run = pad_to_width(&format!("{marker}{}", row.label), run_width);
            let prerequisites = pad_to_width(&row.prerequisites.join(", "), prerequisite_width);
            let dependents = pad_to_width(&row.dependents.join(", "), dependent_width);
            let text = truncate_to_width(&format!("{run} │ {prerequisites} │ {dependents}"), width);
            let style = if selected {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };
            Line::from(Span::styled(text, style))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines),
        Rect::new(
            inner.x,
            inner.y.saturating_add(1),
            inner.width,
            inner.height.saturating_sub(1),
        ),
    );
}

fn viewport_start(rows: &[TreeRow], state: &AppState, viewport_height: usize) -> usize {
    if state.is_following() {
        return rows.len().saturating_sub(viewport_height);
    }
    state
        .selected()
        .and_then(|selected| rows.iter().position(|row| &row.key == selected))
        .map(|index| {
            if index >= viewport_height {
                index.saturating_add(1).saturating_sub(viewport_height)
            } else {
                0
            }
        })
        .unwrap_or(0)
}

fn dag_column_widths(width: usize) -> (usize, usize, usize) {
    let content = width.saturating_sub(6);
    let prerequisite = content / 4;
    let dependent = content / 4;
    let run = content.saturating_sub(prerequisite.saturating_add(dependent));
    (run, prerequisite, dependent)
}

fn pad_to_width(value: &str, width: usize) -> String {
    let value = truncate_to_width(value, width);
    let padding = width.saturating_sub(Span::raw(value.as_str()).width());
    format!("{value}{}", " ".repeat(padding))
}

fn render_activity(frame: &mut Frame<'_>, area: Rect, rows: &[TreeRow], state: &AppState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Activity for selected item ");
    let inner_width = usize::from(area.width.saturating_sub(2));
    let selected_label = state
        .selected()
        .and_then(|selected| rows.iter().find(|row| &row.key == selected))
        .map(|row| row.label.as_str())
        .unwrap_or("none");
    let selected = truncate_to_width(&format!("Selected: {selected_label}"), inner_width);
    let status = state
        .selection_reason()
        .unwrap_or("No activity event feed is wired to this TUI slice; no events are fabricated.");
    let status = truncate_to_width(status, inner_width);
    frame.render_widget(
        Paragraph::new(vec![Line::raw(selected), Line::raw(status)]).block(block),
        area,
    );
}

fn footer_line(width: usize) -> String {
    let full = "q: stop Top only; agents continue | detach: Top runs | ↑↓ select | f/End follow | tab view | / filter | ? help";
    let compact = "q:stop Top; agents continue | detach:Top runs";
    if width >= 70 {
        truncate_to_width(full, width)
    } else {
        truncate_to_width(compact, width)
    }
}

#[derive(Debug)]
struct HeaderField {
    prefix: &'static str,
    value: String,
    shrinkable: bool,
}

impl HeaderField {
    fn text(&self) -> String {
        format!("{}{}", self.prefix, self.value)
    }

    fn value_width(&self) -> usize {
        Span::raw(self.value.as_str()).width()
    }
}

fn header_line(
    screen_width: u16,
    available_width: usize,
    model: &DomainModel,
    quality: ObservationQuality,
    inputs: &HeaderInputs,
) -> Line<'static> {
    let mut fields = Vec::new();
    if screen_width >= 60 {
        fields.push(HeaderField {
            prefix: "host:",
            value: safe_text(&inputs.host),
            shrinkable: true,
        });
    }
    fields.push(HeaderField {
        prefix: "session:",
        value: safe_text(&inputs.session),
        shrinkable: true,
    });
    if screen_width >= 72 {
        fields.push(HeaderField {
            prefix: "workspaces:",
            value: model.workspaces().count().to_string(),
            shrinkable: true,
        });
    }
    fields.push(HeaderField {
        prefix: "",
        value: quality_label(quality).to_owned(),
        shrinkable: false,
    });
    if screen_width >= 88 {
        fields.push(HeaderField {
            prefix: "lag:",
            value: format!("{}ms", inputs.event_lag.as_millis()),
            shrinkable: true,
        });
    }
    if screen_width >= 100 {
        let coverage = inputs.source_coverage.borrow().summary();
        fields.push(HeaderField {
            prefix: "sources:",
            value: safe_text(&coverage),
            shrinkable: true,
        });
    }

    shrink_header_fields(&mut fields, available_width);
    let text = fields
        .iter()
        .map(HeaderField::text)
        .collect::<Vec<_>>()
        .join(" | ");
    let text = truncate_to_width(&text, available_width);
    let quality_text = quality_label(quality);
    let spans = if let Some(index) = text.find(quality_text) {
        let before = text.get(..index).unwrap_or_default().to_owned();
        let after_start = index.saturating_add(quality_text.len());
        let after = text.get(after_start..).unwrap_or_default().to_owned();
        vec![
            Span::raw(before),
            Span::styled(quality_text.to_owned(), quality_style(quality)),
            Span::raw(after),
        ]
    } else {
        vec![Span::raw(text)]
    };
    Line::from(spans)
}

fn shrink_header_fields(fields: &mut [HeaderField], available_width: usize) {
    let priorities = ["sources:", "lag:", "workspaces:", "host:", "session:"];
    loop {
        let current = fields_width(fields);
        if current <= available_width {
            return;
        }
        let overflow = current.saturating_sub(available_width);
        let mut changed = false;
        for prefix in priorities {
            let Some(field) = fields
                .iter_mut()
                .find(|field| field.prefix == prefix && field.shrinkable)
            else {
                continue;
            };
            let value_width = field.value_width();
            if value_width <= 1 {
                continue;
            }
            let reduction = overflow.min(value_width.saturating_sub(1));
            let target = value_width.saturating_sub(reduction);
            field.value = truncate_to_width(&field.value, target);
            changed = true;
            break;
        }
        if !changed {
            return;
        }
    }
}

fn fields_width(fields: &[HeaderField]) -> usize {
    let content = fields
        .iter()
        .map(|field| Span::raw(field.text()).width())
        .fold(0usize, usize::saturating_add);
    content.saturating_add(fields.len().saturating_sub(1).saturating_mul(3))
}

fn quality_label(quality: ObservationQuality) -> &'static str {
    match quality {
        ObservationQuality::Live => "LIVE",
        ObservationQuality::Reconciling => "RECONCILING",
        ObservationQuality::Disconnected => "DISCONNECTED",
        ObservationQuality::Degraded => "DEGRADED",
    }
}

fn quality_style(quality: ObservationQuality) -> Style {
    let color = match quality {
        ObservationQuality::Live => Color::Green,
        ObservationQuality::Reconciling => Color::Yellow,
        ObservationQuality::Disconnected => Color::Red,
        ObservationQuality::Degraded => Color::Magenta,
    };
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

#[cfg(test)]
pub(crate) fn build_tree_rows(model: &DomainModel, state: &AppState) -> Vec<TreeRow> {
    build_tree_rows_named(model, state, None)
}

pub(crate) fn build_rows(model: &DomainModel, state: &AppState) -> Vec<TreeRow> {
    build_rows_named(model, state, None)
}

fn build_rows_named(model: &DomainModel, state: &AppState, session: Option<&str>) -> Vec<TreeRow> {
    match state.view_mode() {
        ViewMode::ExecutionTree => build_tree_rows_named(model, state, session),
        ViewMode::DependencyDag => dag::build_rows(model, state.dag_order()),
    }
}

fn build_tree_rows_named(
    model: &DomainModel,
    state: &AppState,
    session: Option<&str>,
) -> Vec<TreeRow> {
    let mut rows = vec![TreeRow {
        key: NodeKey::Session,
        depth: 0,
        label: session
            .map(|name| format!("Session: {}", safe_text(name)))
            .unwrap_or_else(|| "Session".to_owned()),
        prerequisites: Vec::new(),
        dependents: Vec::new(),
    }];
    let (mut pane_runs, unattached) = place_runs(model, state);

    let mut workspaces = model.workspaces().collect::<Vec<_>>();
    workspaces.sort_by_key(|workspace| {
        (
            model
                .workspace_ordinal(&workspace.workspace_id)
                .map(DisplayOrdinal::get)
                .unwrap_or(i64::MAX),
            workspace.workspace_id.as_str(),
        )
    });
    for workspace in workspaces {
        rows.push(TreeRow {
            key: NodeKey::Workspace(workspace.workspace_id.clone()),
            depth: 1,
            label: format!("Workspace: {}", safe_text(&workspace.workspace_id)),
            prerequisites: Vec::new(),
            dependents: Vec::new(),
        });
        let mut tabs = model
            .tabs()
            .filter(|tab| tab.workspace_id == workspace.workspace_id)
            .collect::<Vec<_>>();
        tabs.sort_by_key(|tab| {
            (
                model
                    .tab_ordinal(&tab.tab_id)
                    .map(DisplayOrdinal::get)
                    .unwrap_or(i64::MAX),
                tab.tab_id.as_str(),
            )
        });
        for tab in tabs {
            rows.push(TreeRow {
                key: NodeKey::Tab(tab.tab_id.clone()),
                depth: 2,
                label: format!("Tab: {}", safe_text(&tab.tab_id)),
                prerequisites: Vec::new(),
                dependents: Vec::new(),
            });
            let mut panes = model
                .panes()
                .filter(|pane| {
                    pane.workspace_id == workspace.workspace_id && pane.tab_id == tab.tab_id
                })
                .collect::<Vec<_>>();
            panes.sort_by_key(|pane| {
                (
                    model
                        .pane_ordinal(&pane.pane_id)
                        .map(DisplayOrdinal::get)
                        .unwrap_or(i64::MAX),
                    pane.pane_id.as_str(),
                )
            });
            for pane in panes {
                rows.push(TreeRow {
                    key: NodeKey::Pane(pane.pane_id.clone()),
                    depth: 3,
                    label: format!("Pane: {}", safe_text(&pane.pane_id)),
                    prerequisites: Vec::new(),
                    dependents: Vec::new(),
                });
                if let Some(runs) = pane_runs.remove(&pane.pane_id) {
                    append_run_rows(&mut rows, model, runs, Some(&pane.pane_id), 4);
                }
            }
        }
    }

    if !unattached.is_empty() {
        rows.push(TreeRow {
            key: NodeKey::UnattachedGroup,
            depth: 1,
            label: "Unattached Task Runs".to_owned(),
            prerequisites: Vec::new(),
            dependents: Vec::new(),
        });
        let runs = unattached
            .into_iter()
            .map(|run_id| (run_id, false))
            .collect();
        append_run_rows(&mut rows, model, runs, None, 2);
    }
    rows
}

fn place_runs(model: &DomainModel, state: &AppState) -> (PaneRuns, Vec<RunId>) {
    let mut pane_runs = PaneRuns::new();
    let mut unattached = Vec::new();
    let mut runs = model.task_runs().collect::<Vec<_>>();
    runs.sort_by_key(|run| (run.display_ordinal.get(), run.run_id));

    for run in runs {
        let mut executions = model
            .executions()
            .filter(|execution| execution.task_run_id == run.run_id)
            .filter(|execution| pane_is_renderable(model, &execution.pane_id))
            .collect::<Vec<_>>();
        executions.sort_by_key(|execution| {
            (
                state.execution_order(&execution.execution_id),
                execution.execution_id.as_str(),
            )
        });
        let mut live_panes = Vec::new();
        let mut seen_panes = HashSet::new();
        for execution in executions
            .iter()
            .filter(|execution| !execution.state.is_terminal())
        {
            if seen_panes.insert(execution.pane_id.as_str()) {
                live_panes.push(execution.pane_id.clone());
            }
        }
        if !live_panes.is_empty() {
            let shared = live_panes.len() > 1;
            for pane_id in live_panes {
                pane_runs
                    .entry(pane_id)
                    .or_default()
                    .push((run.run_id, shared));
            }
        } else if let Some(execution) = executions.last() {
            pane_runs
                .entry(execution.pane_id.clone())
                .or_default()
                .push((run.run_id, false));
        } else {
            unattached.push(run.run_id);
        }
    }
    for runs in pane_runs.values_mut() {
        runs.sort_by_key(|(run_id, _)| {
            model
                .task_run(run_id)
                .map(|run| (run.display_ordinal.get(), run.run_id))
                .unwrap_or((i64::MAX, *run_id))
        });
        runs.dedup_by_key(|(run_id, _)| *run_id);
    }
    (pane_runs, unattached)
}

fn pane_is_renderable(model: &DomainModel, pane_id: &str) -> bool {
    let Some(pane) = model.pane(pane_id) else {
        return false;
    };
    model.workspace(&pane.workspace_id).is_some()
        && model
            .tab(&pane.tab_id)
            .is_some_and(|tab| tab.workspace_id == pane.workspace_id)
}

fn append_run_rows(
    rows: &mut Vec<TreeRow>,
    model: &DomainModel,
    runs: Vec<RunPlacement>,
    pane_id: Option<&str>,
    depth: usize,
) {
    for (run_id, shared) in runs {
        let Some(run) = model.task_run(&run_id) else {
            continue;
        };
        rows.push(TreeRow {
            key: NodeKey::Run {
                run_id,
                pane_id: pane_id.map(str::to_owned),
            },
            depth,
            label: task_run_label(model, run, shared),
            prerequisites: Vec::new(),
            dependents: Vec::new(),
        });
        let agents = model
            .agent_nodes()
            .filter(|agent| agent.task_run_id == run_id)
            .filter(|agent| {
                provider_from_key(&run.key).is_none_or(|provider| provider == agent.provider)
            })
            .collect::<Vec<_>>();
        append_agent_rows(rows, agents, pane_id, depth.saturating_add(1));
    }
}

fn append_agent_rows(
    rows: &mut Vec<TreeRow>,
    agents: Vec<&AgentNode>,
    pane_id: Option<&str>,
    depth: usize,
) {
    let by_id = agents
        .iter()
        .map(|agent| (agent.agent_node_id.as_str(), *agent))
        .collect::<HashMap<_, _>>();
    let mut children = HashMap::<&str, Vec<&AgentNode>>::new();
    let mut roots = Vec::new();
    for agent in agents {
        match agent.parent_agent_node_id.as_deref() {
            Some(parent_id) if by_id.contains_key(parent_id) => {
                children.entry(parent_id).or_default().push(agent);
            }
            Some(_) | None => roots.push(agent),
        }
    }
    sort_agents(&mut roots);
    for values in children.values_mut() {
        sort_agents(values);
    }

    let mut visited = HashSet::new();
    for root in roots {
        append_agent_subtree(rows, root, &children, pane_id, depth, &mut visited);
    }
}

fn sort_agents(agents: &mut Vec<&AgentNode>) {
    agents.sort_by_key(|agent| (agent.display_ordinal.get(), agent.agent_node_id.as_str()));
}

fn append_agent_subtree(
    rows: &mut Vec<TreeRow>,
    agent: &AgentNode,
    children: &HashMap<&str, Vec<&AgentNode>>,
    pane_id: Option<&str>,
    depth: usize,
    visited: &mut HashSet<String>,
) {
    if !visited.insert(agent.agent_node_id.clone()) {
        return;
    }
    rows.push(TreeRow {
        key: NodeKey::Agent {
            agent_node_id: agent.agent_node_id.clone(),
            pane_id: pane_id.map(str::to_owned),
        },
        depth,
        label: agent_node_label(agent),
        prerequisites: Vec::new(),
        dependents: Vec::new(),
    });
    if let Some(child_nodes) = children.get(agent.agent_node_id.as_str()) {
        for child in child_nodes {
            append_agent_subtree(
                rows,
                child,
                children,
                pane_id,
                depth.saturating_add(1),
                visited,
            );
        }
    }
}

fn agent_node_label(agent: &AgentNode) -> String {
    let identity = agent
        .native_session_id
        .as_deref()
        .unwrap_or(&agent.agent_node_id);
    let state = agent
        .state
        .as_ref()
        .map(exec_state_label)
        .unwrap_or("unknown");
    let model = agent
        .model_id
        .as_deref()
        .map(safe_text)
        .unwrap_or_else(|| "unknown".to_owned());
    let activity = agent
        .last_activity_at_ms
        .map_or_else(|| "unknown".to_owned(), |value| format!("{value}ms"));
    format!(
        "{} native agent: {} [state:{state}] [model:{model}] [last:{activity}]",
        provider_label(agent.provider),
        safe_text(identity),
    )
}

pub(crate) fn task_run_label(model: &DomainModel, run: &TaskRun, shared: bool) -> String {
    let mut label = format!(
        "Task Run: {} [{}]",
        run_name(run),
        task_state_label(run.state)
    );
    if shared {
        label.push_str(" [shared]");
    }
    let mut parents = model
        .execution_edges()
        .filter(|edge| edge.child_run_id == run.run_id)
        .filter_map(|edge| model.task_run(&edge.parent_run_id))
        .collect::<Vec<_>>();
    parents.sort_by_key(|parent| (parent.display_ordinal.get(), parent.run_id));
    if let Some(parent) = parents.first() {
        label.push_str(&format!(" [dispatched by: {}]", short_run_name(parent)));
    }
    let linked = model
        .execution_edges()
        .any(|edge| edge.parent_run_id == run.run_id || edge.child_run_id == run.run_id)
        || model.dependency_edges().any(|edge| {
            edge.prerequisite_run_id == run.run_id || edge.dependent_run_id == run.run_id
        });
    if !linked {
        label.push_str(" [unlinked]");
    }
    label
}

fn run_name(run: &TaskRun) -> String {
    match &run.key {
        RunKey::Controller(name) => safe_text(name),
        RunKey::Native { provider, sid } => {
            format!("{} {}", provider_label(*provider), safe_text(sid))
        }
        RunKey::NativePath { provider, path } => {
            format!("{} {}", provider_label(*provider), safe_text(path))
        }
        RunKey::Provisional {
            terminal_id,
            start_ms,
            seq,
        } => format!("provisional {}:{start_ms}:{seq}", safe_text(terminal_id)),
    }
}

pub(crate) fn short_run_name(run: &TaskRun) -> String {
    match &run.key {
        RunKey::Controller(name) => safe_text(name),
        RunKey::Native { sid, .. } => safe_text(sid),
        RunKey::NativePath { path, .. } => safe_text(path),
        RunKey::Provisional { terminal_id, .. } => safe_text(terminal_id),
    }
}

fn provider_from_key(key: &RunKey) -> Option<Provider> {
    match key {
        RunKey::Native { provider, .. } | RunKey::NativePath { provider, .. } => Some(*provider),
        RunKey::Controller(_) | RunKey::Provisional { .. } => None,
    }
}

fn provider_label(provider: Provider) -> &'static str {
    match provider {
        Provider::Claude => "Claude",
        Provider::Codex => "Codex",
    }
}

fn task_state_label(state: TaskState) -> &'static str {
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

fn exec_state_label(state: &ExecState) -> &'static str {
    match state {
        ExecState::Unknown => "unknown",
        ExecState::Idle => "idle",
        ExecState::Working => "working",
        ExecState::Blocked => "blocked",
        ExecState::Ended => "ended",
        ExecState::Stale { .. } => "stale",
    }
}

fn safe_text(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                '�'
            } else {
                character
            }
        })
        .collect()
}

pub(crate) fn truncate_to_width(value: &str, max_width: usize) -> String {
    if Span::raw(value).width() <= max_width {
        return value.to_owned();
    }
    if max_width == 0 {
        return String::new();
    }
    let ellipsis = "…";
    let ellipsis_width = Span::raw(ellipsis).width();
    if max_width < ellipsis_width {
        return String::new();
    }
    let content_width = max_width.saturating_sub(ellipsis_width);
    let mut output = String::new();
    let mut width = 0usize;
    for character in value.chars() {
        let character_width = Span::raw(character.to_string()).width();
        if width.saturating_add(character_width) > content_width {
            break;
        }
        output.push(character);
        width = width.saturating_add(character_width);
    }
    output.push_str(ellipsis);
    output
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::{Buffer, CellWidth};
    use ratatui::text::{Line, Span};
    use tokio::sync::watch;

    use super::*;
    use crate::herdr::collector::ObservationQuality;
    use crate::model::{
        AgentNode, DependencyEdge, DisplayOrdinal, DomainModel, ExecState, Execution,
        ExecutionEdge, Pane, Provider, RunId, RunKey, Tab, TaskRun, TaskState, Workspace,
    };
    use crate::tui::app::{App, AppState, HeaderInputs};

    fn run_id(value: &str) -> RunId {
        RunId::parse(value).unwrap()
    }

    fn populated_model() -> DomainModel {
        let run = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "api".to_owned(),
        });
        model.insert_tab(Tab {
            tab_id: "implementation".to_owned(),
            workspace_id: "api".to_owned(),
        });
        model.insert_pane(Pane {
            pane_id: "w1:p1".to_owned(),
            workspace_id: "api".to_owned(),
            tab_id: "implementation".to_owned(),
            terminal_id: "terminal-1".to_owned(),
        });
        model.insert_task_run(TaskRun {
            run_id: run,
            key: RunKey::Native {
                provider: Provider::Codex,
                sid: "controller".to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(7),
            state: TaskState::Running,
            has_controller_task_state_event: false,
        });
        model.insert_execution(Execution {
            execution_id: "execution-1".to_owned(),
            pane_id: "w1:p1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            task_run_id: run,
            state: ExecState::Working,
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "agent-1".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("investigate".to_owned()),
            task_run_id: run,
            display_ordinal: DisplayOrdinal::new(8),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: Some("gpt-test".to_owned()),
            last_event_kind: Some("assistant".to_owned()),
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: Some(42),
            session_file: None,
        });
        model
    }

    fn app(model: DomainModel, quality: ObservationQuality, session: &str) -> App {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(model));
        let (_quality_sender, quality_receiver) = watch::channel(quality);
        let mut coverage = crate::herdr::collector::SourceCoverageRegistry::new(
            crate::herdr::collector::SourceAvailability::NotApplicable,
        );
        coverage.set(
            crate::herdr::collector::CoverageSource::Codex,
            crate::herdr::collector::SourceAvailability::Available,
        );
        let (_coverage_sender, source_coverage) = watch::channel(coverage);
        App::new(
            model_receiver,
            quality_receiver,
            HeaderInputs {
                host: "build-host".to_owned(),
                session: session.to_owned(),
                event_lag: Duration::from_millis(23),
                source_coverage,
            },
        )
    }

    fn buffer_rows(buffer: &Buffer) -> Vec<String> {
        let mut rows = Vec::new();
        for y in 0..buffer.area.height {
            let mut row = String::new();
            let mut hidden_columns = 0usize;
            for x in 0..buffer.area.width {
                let cell = buffer.cell((x, y)).unwrap();
                if hidden_columns == 0 {
                    row.push_str(cell.symbol());
                    hidden_columns = cell.cell_width().saturating_sub(1) as usize;
                } else {
                    hidden_columns = hidden_columns.saturating_sub(1);
                }
            }
            rows.push(row);
        }
        rows
    }

    fn render(app: &App, width: u16, height: u16) -> Vec<String> {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| app.render(frame)).unwrap();
        buffer_rows(terminal.backend().buffer())
    }

    fn header(rows: &[String]) -> &str {
        rows.iter()
            .find(|row| row.contains("session:"))
            .map(String::as_str)
            .unwrap()
    }

    #[test]
    fn layout_at_100_cols() {
        let app = app(populated_model(), ObservationQuality::Live, "demo");
        let rows = render(&app, 100, 18);
        for row in &rows {
            println!("{row}");
        }
        let screen = rows.join("\n");
        let header = header(&rows);

        assert!(header.contains("host:build-host"));
        assert!(header.contains("session:demo"));
        assert!(header.contains("workspaces:1"));
        assert!(header.contains("LIVE"));
        assert!(header.contains("lag:23ms"));
        assert!(header.contains("sources:h"));
        assert!(screen.contains("q: stop Top only"));
        assert!(screen.contains("agents continue"));
        assert!(screen.contains("detach: Top runs"));
        assert!(screen.contains("tab view"));

        let session_x = rows
            .iter()
            .find(|row| row.contains("Session: demo"))
            .unwrap()
            .find("Session: demo")
            .unwrap();
        let workspace_x = rows
            .iter()
            .find(|row| row.contains("Workspace: api"))
            .unwrap()
            .find("Workspace: api")
            .unwrap();
        let tab_x = rows
            .iter()
            .find(|row| row.contains("Tab: implementation"))
            .unwrap()
            .find("Tab: implementation")
            .unwrap();
        let pane_x = rows
            .iter()
            .find(|row| row.contains("Pane: w1:p1"))
            .unwrap()
            .find("Pane: w1:p1")
            .unwrap();
        let run_x = rows
            .iter()
            .find(|row| row.contains("Task Run: Codex controller"))
            .unwrap()
            .find("Task Run: Codex controller")
            .unwrap();
        let agent_x = rows
            .iter()
            .find(|row| row.contains("Codex native agent: investigate"))
            .unwrap()
            .find("Codex native agent: investigate")
            .unwrap();
        assert_eq!(workspace_x, session_x + 2);
        assert_eq!(tab_x, workspace_x + 2);
        assert_eq!(pane_x, tab_x + 2);
        assert_eq!(run_x, pane_x + 2);
        assert_eq!(agent_x, run_x + 2);
    }

    #[test]
    fn layout_at_48_cols_truncation_order() {
        let cases = [
            (99, true, true, true, false),
            (87, true, true, false, false),
            (71, true, false, false, false),
            (59, false, false, false, false),
            (48, false, false, false, false),
        ];
        for (width, host_visible, workspaces_visible, lag_visible, coverage_visible) in cases {
            let app = app(populated_model(), ObservationQuality::Live, "demo");
            let rows = render(&app, width, 18);
            let header = header(&rows);
            assert_eq!(
                header.contains("host:"),
                host_visible,
                "width {width}: {header}"
            );
            assert_eq!(
                header.contains("workspaces:"),
                workspaces_visible,
                "width {width}: {header}"
            );
            assert_eq!(
                header.contains("lag:"),
                lag_visible,
                "width {width}: {header}"
            );
            assert_eq!(
                header.contains("sources:"),
                coverage_visible,
                "width {width}: {header}"
            );
            assert!(header.contains("LIVE"), "width {width}: {header}");
            assert!(header.contains("session:"), "width {width}: {header}");
            if width == 48 {
                for row in &rows {
                    println!("{row}");
                }
            }
        }
    }

    #[test]
    fn quality_and_session_never_dropped() {
        let variants = [
            (ObservationQuality::Live, "LIVE"),
            (ObservationQuality::Reconciling, "RECONCILING"),
            (ObservationQuality::Disconnected, "DISCONNECTED"),
            (ObservationQuality::Degraded, "DEGRADED"),
        ];
        for (quality, indicator) in variants {
            let app = app(
                populated_model(),
                quality,
                "敵対的な非常に長いセッション名🙂🙂🙂🙂🙂",
            );
            let rows = render(&app, 48, 18);
            let header = header(&rows);
            assert!(header.contains(indicator), "{indicator}: {header}");
            assert!(header.contains("session:"), "{indicator}: {header}");
        }
    }

    fn ordinal_model(lexical_first: RunId, ordinal_first: RunId, state: TaskState) -> DomainModel {
        let model = populated_model();
        let existing = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut replacement = DomainModel::default();
        for workspace in model.workspaces() {
            replacement.insert_workspace(workspace.clone());
        }
        for tab in model.tabs() {
            replacement.insert_tab(tab.clone());
        }
        for pane in model.panes() {
            replacement.insert_pane(pane.clone());
        }
        assert!(model.task_run(&existing).is_some());
        for (id, label, ordinal) in [
            (lexical_first, "lexical-first", 20),
            (ordinal_first, "ordinal-first", 10),
        ] {
            replacement.insert_task_run(TaskRun {
                run_id: id,
                key: RunKey::Controller(label.to_owned()),
                display_ordinal: DisplayOrdinal::new(ordinal),
                state,
                has_controller_task_state_event: true,
            });
            replacement.insert_execution(Execution {
                execution_id: format!("execution-{label}"),
                pane_id: "w1:p1".to_owned(),
                terminal_id: "terminal-1".to_owned(),
                task_run_id: id,
                state: ExecState::Working,
            });
        }
        replacement
    }

    fn run_row_order(rows: &[String]) -> Vec<&'static str> {
        let tree_start = rows
            .iter()
            .position(|row| row.contains("Execution tree"))
            .unwrap();
        let activity_start = rows
            .iter()
            .position(|row| row.contains("Activity for selected item"))
            .unwrap();
        rows[tree_start..activity_start]
            .iter()
            .filter_map(|row| {
                if row.contains("Task Run:") && row.contains("ordinal-first") {
                    Some("ordinal-first")
                } else if row.contains("Task Run:") && row.contains("lexical-first") {
                    Some("lexical-first")
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn ordinal_stable_across_refresh() {
        let lexical_first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let ordinal_first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let initial = ordinal_model(lexical_first, ordinal_first, TaskState::Running);
        let (model_sender, model_receiver) = watch::channel(Arc::new(initial));
        let (_quality_sender, quality_receiver) = watch::channel(ObservationQuality::Live);
        let mut app = App::new(model_receiver, quality_receiver, HeaderInputs::default());

        let before = render(&app, 100, 18);
        model_sender
            .send(Arc::new(ordinal_model(
                lexical_first,
                ordinal_first,
                TaskState::Blocked,
            )))
            .unwrap();
        app.refresh();
        let after = render(&app, 100, 18);

        assert_eq!(
            run_row_order(&before),
            vec!["ordinal-first", "lexical-first"]
        );
        assert_eq!(run_row_order(&after), run_row_order(&before));
    }

    #[test]
    fn topology_sibling_rows_follow_persisted_ordinals() {
        let mut model = DomainModel::default();
        for (workspace_id, ordinal) in [("workspace-a", 2), ("workspace-z", 1)] {
            model.insert_workspace(Workspace {
                workspace_id: workspace_id.to_owned(),
            });
            model.set_workspace_ordinal(workspace_id.to_owned(), DisplayOrdinal::new(ordinal));
        }
        for (tab_id, ordinal) in [("tab-a", 4), ("tab-z", 3)] {
            model.insert_tab(Tab {
                tab_id: tab_id.to_owned(),
                workspace_id: "workspace-z".to_owned(),
            });
            model.set_tab_ordinal(tab_id.to_owned(), DisplayOrdinal::new(ordinal));
        }
        for (pane_id, ordinal) in [("pane-a", 6), ("pane-z", 5)] {
            model.insert_pane(Pane {
                pane_id: pane_id.to_owned(),
                workspace_id: "workspace-z".to_owned(),
                tab_id: "tab-z".to_owned(),
                terminal_id: format!("terminal-{pane_id}"),
            });
            model.set_pane_ordinal(pane_id.to_owned(), DisplayOrdinal::new(ordinal));
        }

        let rows = build_tree_rows(&model, &AppState::default());
        let topology = rows
            .iter()
            .filter_map(|row| match &row.key {
                NodeKey::Workspace(id) | NodeKey::Tab(id) | NodeKey::Pane(id) => Some(id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            topology,
            [
                "workspace-z",
                "tab-z",
                "pane-z",
                "pane-a",
                "tab-a",
                "workspace-a",
            ]
        );
    }

    #[test]
    fn execution_placement_order_remains_in_session_across_refresh() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut initial = DomainModel::default();
        initial.insert_workspace(Workspace {
            workspace_id: "workspace".to_owned(),
        });
        initial.set_workspace_ordinal("workspace".to_owned(), DisplayOrdinal::new(1));
        initial.insert_tab(Tab {
            tab_id: "tab".to_owned(),
            workspace_id: "workspace".to_owned(),
        });
        initial.set_tab_ordinal("tab".to_owned(), DisplayOrdinal::new(2));
        for (pane_id, ordinal) in [("pane-old", 3), ("pane-new", 4)] {
            initial.insert_pane(Pane {
                pane_id: pane_id.to_owned(),
                workspace_id: "workspace".to_owned(),
                tab_id: "tab".to_owned(),
                terminal_id: format!("terminal-{pane_id}"),
            });
            initial.set_pane_ordinal(pane_id.to_owned(), DisplayOrdinal::new(ordinal));
        }
        initial.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller("placement".to_owned()),
            display_ordinal: DisplayOrdinal::new(5),
            state: TaskState::Completed,
            has_controller_task_state_event: true,
        });
        initial.insert_execution(Execution {
            execution_id: "z-old-execution".to_owned(),
            pane_id: "pane-old".to_owned(),
            terminal_id: "terminal-pane-old".to_owned(),
            task_run_id: run_id,
            state: ExecState::Ended,
        });
        let mut refreshed = initial.clone();
        refreshed.insert_execution(Execution {
            execution_id: "a-new-execution".to_owned(),
            pane_id: "pane-new".to_owned(),
            terminal_id: "terminal-pane-new".to_owned(),
            task_run_id: run_id,
            state: ExecState::Ended,
        });
        let (model_sender, model_receiver) = watch::channel(Arc::new(initial));
        let (_quality_sender, quality_receiver) = watch::channel(ObservationQuality::Live);
        let mut app = App::new(model_receiver, quality_receiver, HeaderInputs::default());

        model_sender.send(Arc::new(refreshed)).unwrap();
        app.refresh();
        let rows = build_tree_rows(app.model(), app.state());
        let hosting_pane = rows.iter().find_map(|row| match &row.key {
            NodeKey::Run {
                run_id: actual,
                pane_id,
            } if *actual == run_id => pane_id.as_deref(),
            _ => None,
        });

        assert_eq!(hosting_pane, Some("pane-new"));
    }

    #[test]
    fn agent_rows_follow_persisted_ordinals_and_render_recursive_parentage() {
        let mut model = populated_model();
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        model.insert_agent_node(AgentNode {
            agent_node_id: "agent-missing-parent".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("orphan".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(9),
            parent_agent_node_id: Some("agent-never-arrived".to_owned()),
            state: None,
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "agent-child".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("child".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(10),
            parent_agent_node_id: Some("agent-1".to_owned()),
            state: Some(ExecState::Working),
            model_id: Some("gpt-child".to_owned()),
            last_event_kind: Some("tool".to_owned()),
            last_tool_name: Some("Read".to_owned()),
            last_item_count: Some(3),
            last_byte_count: Some(512),
            last_activity_at_ms: Some(99),
            session_file: None,
        });
        let app = app(model, ObservationQuality::Live, "demo");

        let rows = build_tree_rows(app.model(), app.state());
        let agent_rows = rows
            .iter()
            .filter(|row| matches!(row.key, NodeKey::Agent { .. }))
            .collect::<Vec<_>>();

        assert_eq!(agent_rows.len(), 3);
        assert!(agent_rows[0].label.contains("investigate"));
        assert_eq!(agent_rows[0].depth + 1, agent_rows[1].depth);
        assert!(agent_rows[1].label.contains("child"));
        assert!(agent_rows[1].label.contains("state:working"));
        assert!(agent_rows[1].label.contains("model:gpt-child"));
        assert!(agent_rows[1].label.contains("last:99ms"));
        assert_eq!(agent_rows[0].depth, agent_rows[2].depth);
        assert!(agent_rows[2].label.contains("orphan"));
    }

    #[test]
    fn wide_unicode_truncation_safe() {
        let mut model = populated_model();
        model.insert_workspace(Workspace {
            workspace_id: "作業領域🙂very-long-workspace-name".to_owned(),
        });
        let app = app(
            model,
            ObservationQuality::Disconnected,
            "セッション🙂with-a-long-tail",
        );
        let rows = render(&app, 48, 18);

        for row in &rows {
            assert!(
                Line::raw(row.as_str()).width() <= 48,
                "overflowing row: {row:?}"
            );
            assert!(!row.contains('\u{fffd}'));
        }
        let truncated = truncate_to_width("ab界🙂cd", 6);
        assert_eq!(truncated, "ab界…");
        assert!(Span::raw(truncated).width() <= 6);
    }

    #[test]
    fn shared_runs_dispatch_annotations_and_unattached_runs_are_truthful() {
        let parent = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let shared = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let unattached = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "workspace".to_owned(),
        });
        model.insert_tab(Tab {
            tab_id: "tab".to_owned(),
            workspace_id: "workspace".to_owned(),
        });
        for pane_id in ["pane-1", "pane-2"] {
            model.insert_pane(Pane {
                pane_id: pane_id.to_owned(),
                workspace_id: "workspace".to_owned(),
                tab_id: "tab".to_owned(),
                terminal_id: format!("terminal-{pane_id}"),
            });
        }
        for (id, label, ordinal) in [
            (parent, "parent", 1),
            (shared, "shared", 2),
            (unattached, "orphan", 3),
        ] {
            model.insert_task_run(TaskRun {
                run_id: id,
                key: RunKey::Controller(label.to_owned()),
                display_ordinal: DisplayOrdinal::new(ordinal),
                state: TaskState::Running,
                has_controller_task_state_event: true,
            });
        }
        model.insert_execution(Execution {
            execution_id: "parent-execution".to_owned(),
            pane_id: "pane-1".to_owned(),
            terminal_id: "terminal-pane-1".to_owned(),
            task_run_id: parent,
            state: ExecState::Working,
        });
        for pane_id in ["pane-1", "pane-2"] {
            model.insert_execution(Execution {
                execution_id: format!("shared-{pane_id}"),
                pane_id: pane_id.to_owned(),
                terminal_id: format!("terminal-{pane_id}"),
                task_run_id: shared,
                state: ExecState::Working,
            });
        }
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: shared,
        });
        let app = app(model, ObservationQuality::Live, "session");
        let rows = build_tree_rows(app.model(), app.state());
        let labels = rows
            .iter()
            .map(|row| row.label.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            labels
                .iter()
                .filter(|label| label.contains("[shared]"))
                .count(),
            2
        );
        assert!(
            labels
                .iter()
                .any(|label| label.contains("[dispatched by: parent]"))
        );
        assert!(
            labels
                .iter()
                .any(|label| label.contains("Unattached Task Runs"))
        );
        assert!(labels.iter().any(|label| label.contains("orphan")));
    }

    #[test]
    fn dag_renders_direct_columns_unicode_safely_at_minimum_width_and_tracks_activity() {
        let prerequisite = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let dependent = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let mut model = DomainModel::default();
        for (run_id, label, ordinal) in [
            (prerequisite, "前提🙂", 1),
            (dependent, "依存先🙂with-a-long-tail", 2),
        ] {
            model.insert_task_run(TaskRun {
                run_id,
                key: RunKey::Controller(label.to_owned()),
                display_ordinal: DisplayOrdinal::new(ordinal),
                state: TaskState::Running,
                has_controller_task_state_event: true,
            });
        }
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: prerequisite,
            dependent_run_id: dependent,
        });
        let mut app = app(model, ObservationQuality::Live, "dag-session");
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        let rows = render(&app, 48, 18);
        let screen = rows.join("\n");

        assert!(screen.contains("Dependency DAG"));
        assert!(screen.contains("Task Run"));
        assert!(screen.contains("Prereqs"));
        assert!(screen.contains("Dependents"));
        assert!(screen.contains("前提"));
        assert!(screen.contains("Selected: Task Run: 依存先"));
        for row in &rows {
            assert!(
                Line::raw(row.as_str()).width() <= 48,
                "overflowing row: {row:?}"
            );
            assert!(!row.contains('\u{fffd}'));
        }
    }

    #[test]
    fn thousand_edge_dag_follow_window_shows_exact_tail() {
        let mut model = DomainModel::default();
        let ids = (0..=1_000).map(|_| RunId::new()).collect::<Vec<_>>();
        for (index, run_id) in ids.iter().copied().enumerate() {
            model.insert_task_run(TaskRun {
                run_id,
                key: RunKey::Controller(format!("run-{index:04}")),
                display_ordinal: DisplayOrdinal::new(index as i64),
                state: TaskState::Running,
                has_controller_task_state_event: true,
            });
        }
        for pair in ids.windows(2) {
            model.insert_dependency_edge(DependencyEdge {
                prerequisite_run_id: pair[0],
                dependent_run_id: pair[1],
            });
        }
        let mut app = app(model, ObservationQuality::Live, "large-dag");
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        let rows = render(&app, 100, 14);
        let dag = rows[3..9].join("\n");

        assert!(dag.contains("Task Run: run-0998"));
        assert!(dag.contains("Task Run: run-0999"));
        assert!(dag.contains("Task Run: run-1000"));
        assert!(!dag.contains("Task Run: run-0997"));
    }
}
