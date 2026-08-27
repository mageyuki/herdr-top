//! T10 execution-tree rendering and truncation rules.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{HashMap, HashSet};

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use unicode_segmentation::UnicodeSegmentation;

use crate::diagnostics::{
    ControllerInputStatus, ControllerInputUnavailableReason, DiagnosticSource, InputAvailability,
    OccurrenceLogStatus, OwnerFreshness, RuntimeDiagnosticsSnapshot,
};
use crate::herdr::collector::{ObservationQuality, PerformancePublication};
use crate::model::{AgentNode, DisplayOrdinal, DomainModel, Provider, RunId, RunKey, TaskRun};
use crate::performance::PerformanceDegradationReason;
use crate::store::writer::{
    DurabilityDisposition, PersistenceFailureCode, PersistenceOperation, PersistencePhase,
    PersistenceStatus,
};

use super::app::{AppState, HeaderInputs, NodeKey, Overlay, TuiSetup, ViewMode};
use super::dag;
use super::projection::{self, DisplayStatus, RowProjection, StatusReadModel};

const MIN_WIDTH: u16 = 48;
const MIN_HEIGHT: u16 = 14;
type RunPlacement = (RunId, bool);
type PaneRuns = HashMap<String, Vec<RunPlacement>>;
type NestedRuns = HashMap<RunId, Vec<RunId>>;
pub(crate) type NewestAgentNodes<'a> = HashMap<RunId, &'a AgentNode>;

#[derive(Default)]
pub(crate) struct LiveLineReadModel {
    by_run: HashMap<RunId, String>,
}

impl LiveLineReadModel {
    fn from_model(model: &DomainModel) -> Self {
        let mut selected = HashMap::<RunId, (Option<i64>, String, String)>::new();
        for agent in model.agent_nodes().filter(|agent| {
            agent.last_event_kind.as_deref() == Some(crate::provider::lane::LIVE_LINE_EVENT_KIND)
        }) {
            let candidate = (
                agent.last_activity_at_ms,
                agent.agent_node_id.clone(),
                agent.last_tool_name.clone().unwrap_or_default(),
            );
            selected
                .entry(agent.task_run_id)
                .and_modify(|current| {
                    if (current.0, current.1.as_str()) < (candidate.0, candidate.1.as_str()) {
                        current.clone_from(&candidate);
                    }
                })
                .or_insert(candidate);
        }
        Self {
            by_run: selected
                .into_iter()
                .filter_map(|(run_id, (_, _, line))| (!line.is_empty()).then_some((run_id, line)))
                .collect(),
        }
    }

    fn get(&self, run_id: &RunId) -> Option<&str> {
        self.by_run.get(run_id).map(String::as_str)
    }
}

#[derive(Clone, Copy)]
struct RunRowSignals<'a> {
    live_lines: &'a LiveLineReadModel,
    display_status: DisplayStatus,
    show_duration_suffix: bool,
}

struct RunRowContext<'model, 'data> {
    model: &'model DomainModel,
    nested_runs: &'data NestedRuns,
    newest_agents: &'data NewestAgentNodes<'model>,
    live_lines: &'data LiveLineReadModel,
    stalled_runs: &'data HashSet<RunId>,
    statuses: &'data StatusReadModel,
    now_ms: i64,
}

#[derive(Default)]
struct RunRenderState {
    ancestors: HashSet<RunId>,
    expanded_shared_runs: HashSet<RunId>,
}

#[cfg(test)]
thread_local! {
    static NEWEST_AGENT_SCAN_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn record_newest_agent_scan() {
    NEWEST_AGENT_SCAN_COUNT.set(NEWEST_AGENT_SCAN_COUNT.get().saturating_add(1));
}

#[cfg(test)]
fn reset_newest_agent_scan_count() {
    NEWEST_AGENT_SCAN_COUNT.set(0);
}

#[cfg(test)]
fn newest_agent_scan_count() -> usize {
    NEWEST_AGENT_SCAN_COUNT.get()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TreeRow {
    pub(crate) key: NodeKey,
    pub(crate) depth: usize,
    pub(crate) label: String,
    pub(crate) label_without_duration_suffix: Option<String>,
    pub(crate) display_status: Option<DisplayStatus>,
    pub(crate) prerequisites: Vec<String>,
    pub(crate) dependents: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetricColumn {
    Model,
    Effort,
    Tok,
    TokPerSecond,
    Time,
}

impl MetricColumn {
    const fn width(self) -> usize {
        match self {
            Self::Model => 11,
            Self::Effort | Self::Tok | Self::TokPerSecond => 5,
            Self::Time => 6,
        }
    }
}

const ALL_METRIC_COLUMNS: &[MetricColumn] = &[
    MetricColumn::Model,
    MetricColumn::Effort,
    MetricColumn::Tok,
    MetricColumn::TokPerSecond,
    MetricColumn::Time,
];
const WITHOUT_MODEL_COLUMNS: &[MetricColumn] = &[
    MetricColumn::Effort,
    MetricColumn::Tok,
    MetricColumn::TokPerSecond,
    MetricColumn::Time,
];
const TOKEN_RATE_TIME_COLUMNS: &[MetricColumn] = &[
    MetricColumn::Tok,
    MetricColumn::TokPerSecond,
    MetricColumn::Time,
];
const TOKEN_TIME_COLUMNS: &[MetricColumn] = &[MetricColumn::Tok, MetricColumn::Time];
const TIME_COLUMN: &[MetricColumn] = &[MetricColumn::Time];

/// Selects the fixed metric band for the total tree-row width.
///
/// Narrowing drops MODEL, then EFF, TOK-S, TOK, and finally TIME. Label text is
/// truncated and deep indentation is compressed before the active band's columns
/// are allowed to disappear at the next declared threshold.
fn visible_metric_columns(width: usize) -> &'static [MetricColumn] {
    match width {
        120.. => ALL_METRIC_COLUMNS,
        104..=119 => WITHOUT_MODEL_COLUMNS,
        90..=103 => TOKEN_RATE_TIME_COLUMNS,
        76..=89 => TOKEN_TIME_COLUMNS,
        62..=75 => TIME_COLUMN,
        _ => &[],
    }
}

fn metric_block_width(columns: &[MetricColumn]) -> usize {
    columns
        .iter()
        .copied()
        .map(MetricColumn::width)
        .sum::<usize>()
        .saturating_add(columns.len().saturating_sub(1))
}

fn right_align_to_width(value: &str, width: usize) -> String {
    let value = truncate_to_width(value, width);
    let padding = width.saturating_sub(Span::raw(value.as_str()).width());
    format!("{}{value}", " ".repeat(padding))
}

fn render_metric_block(
    metrics: Option<&projection::RunMetricInputs>,
    columns: &[MetricColumn],
    now_ms: i64,
) -> String {
    columns
        .iter()
        .copied()
        .map(|column| {
            let value = metrics.map_or_else(String::new, |metrics| {
                format_metric_value(column, metrics, now_ms)
            });
            right_align_to_width(&value, column.width())
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn format_metric_value(
    column: MetricColumn,
    metrics: &projection::RunMetricInputs,
    now_ms: i64,
) -> String {
    match column {
        MetricColumn::Model => format_model_value(metrics.model.as_deref()),
        MetricColumn::Effort => format_effort_value(metrics.effort.as_deref()),
        MetricColumn::Tok => metrics
            .output_tokens
            .map_or_else(|| "—".to_owned(), format_token_count),
        MetricColumn::TokPerSecond => format_token_rate_value(metrics, now_ms),
        MetricColumn::Time => format_time_value(metrics, now_ms),
    }
}

fn format_model_value(model: Option<&str>) -> String {
    let Some(model) = model else {
        return "—".to_owned();
    };
    let model = safe_text(model);
    let shortened = model.strip_prefix("claude-").unwrap_or(&model);
    let shortened = strip_model_date_suffix(shortened);
    if shortened.is_empty() {
        "—".to_owned()
    } else {
        truncate_to_width(shortened, MetricColumn::Model.width())
    }
}

fn strip_model_date_suffix(model: &str) -> &str {
    if let Some(prefix) = model.get(..model.len().saturating_sub(11))
        && let Some(suffix) = model.get(model.len().saturating_sub(11)..)
        && suffix.len() == 11
        && suffix.starts_with('-')
        && suffix.as_bytes()[1..5].iter().all(u8::is_ascii_digit)
        && suffix.as_bytes()[5] == b'-'
        && suffix.as_bytes()[6..8].iter().all(u8::is_ascii_digit)
        && suffix.as_bytes()[8] == b'-'
        && suffix.as_bytes()[9..11].iter().all(u8::is_ascii_digit)
    {
        return prefix;
    }
    if let Some(prefix) = model.get(..model.len().saturating_sub(9))
        && let Some(suffix) = model.get(model.len().saturating_sub(9)..)
        && suffix.len() == 9
        && suffix.starts_with('-')
        && suffix.as_bytes()[1..].iter().all(u8::is_ascii_digit)
    {
        return prefix;
    }
    if let Some(prefix) = model.get(..model.len().saturating_sub(8))
        && let Some(suffix) = model.get(model.len().saturating_sub(8)..)
        && suffix.len() == 8
        && suffix.as_bytes().iter().all(u8::is_ascii_digit)
    {
        return prefix.trim_end_matches('-');
    }
    model
}

fn format_effort_value(effort: Option<&str>) -> String {
    let Some(effort) = effort.filter(|effort| !effort.is_empty()) else {
        return "—".to_owned();
    };
    let effort = safe_text(effort);
    match effort.to_ascii_lowercase().as_str() {
        "minimal" => "min".to_owned(),
        "low" => "low".to_owned(),
        "medium" => "med".to_owned(),
        "high" => "high".to_owned(),
        "xhigh" => "xhigh".to_owned(),
        "max" => "max".to_owned(),
        _ => truncate_to_width(&effort, MetricColumn::Effort.width()),
    }
}

fn format_token_count(tokens: u64) -> String {
    if tokens < 10_000 {
        return tokens.to_string();
    }
    if tokens < 1_000_000 {
        return format_scaled_token_count(tokens, 1_000, 'k');
    }
    format_scaled_token_count(tokens, 1_000_000, 'M')
}

fn format_scaled_token_count(tokens: u64, divisor: u64, suffix: char) -> String {
    let scaled = tokens as f64 / divisor as f64;
    if scaled < 100.0 {
        let rounded = (scaled * 10.0).round() / 10.0;
        if rounded < 100.0 {
            return format!("{rounded:.1}{suffix}");
        }
    }
    let rounded = scaled.round().clamp(0.0, 9_999.0);
    format!("{rounded:.0}{suffix}")
}

fn format_token_rate_value(metrics: &projection::RunMetricInputs, now_ms: i64) -> String {
    projection::run_token_rate(metrics, now_ms).map_or_else(|| "—".to_owned(), format_token_rate)
}

fn format_token_rate(rate: f64) -> String {
    if rate < 10.0 {
        let rounded = (rate * 10.0).round() / 10.0;
        if rounded < 10.0 {
            return format!("{rounded:.1}/s");
        }
    }
    if rate < 1_000.0 {
        let rounded = rate.round();
        if rounded < 1_000.0 {
            return format!("{rounded:.0}/s");
        }
    }
    let thousands = (rate / 1_000.0).round().clamp(1.0, 99.0);
    format!("{thousands:.0}k/s")
}

fn format_time_value(metrics: &projection::RunMetricInputs, now_ms: i64) -> String {
    let Some(created_at_ms) = metrics.created_at_ms else {
        return "—".to_owned();
    };
    let Some(end_ms) = metric_end_ms(metrics, now_ms) else {
        return "—".to_owned();
    };
    let Some(elapsed_ms) = end_ms
        .checked_sub(created_at_ms)
        .filter(|elapsed_ms| *elapsed_ms >= 0)
    else {
        return "—".to_owned();
    };
    truncate_to_width(&format_duration(elapsed_ms), MetricColumn::Time.width())
}

fn metric_end_ms(metrics: &projection::RunMetricInputs, now_ms: i64) -> Option<i64> {
    if metrics.terminal {
        metrics.finished_at_ms
    } else {
        Some(now_ms)
    }
}

#[derive(Clone, Copy)]
pub(super) struct PaintSnapshot<'a> {
    state: &'a AppState,
    rows: &'a [TreeRow],
    now_ms: i64,
    session_start_ms: i64,
}

impl<'a> PaintSnapshot<'a> {
    pub(super) const fn new(
        state: &'a AppState,
        rows: &'a [TreeRow],
        now_ms: i64,
        session_start_ms: i64,
    ) -> Self {
        Self {
            state,
            rows,
            now_ms,
            session_start_ms,
        }
    }
}

/// Draws a complete fixed-screen frame from immutable model and UI snapshots.
pub(super) fn render(
    frame: &mut Frame<'_>,
    model: &DomainModel,
    performance: &PerformancePublication,
    header: &HeaderInputs,
    paint: PaintSnapshot<'_>,
    diagnostics: &RuntimeDiagnosticsSnapshot,
    setup: &TuiSetup,
) {
    let state = paint.state;
    let rows = paint.rows;
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
    let activity_y = footer_y.saturating_sub(6);
    let activity_area = Rect::new(area.x, activity_y, area.width, 6);
    let tree_y = area.y.saturating_add(3);
    let tree_area = Rect::new(
        area.x,
        tree_y,
        area.width,
        activity_y.saturating_sub(tree_y),
    );

    let strip = projection::runtime_strip(performance.effective_quality, diagnostics);
    render_header(
        frame,
        header_area,
        model,
        strip.quality,
        performance,
        header,
        paint.now_ms.saturating_sub(paint.session_start_ms).max(0),
    );
    match state.view_mode() {
        ViewMode::ExecutionTree => {
            render_tree(
                frame,
                tree_area,
                model,
                rows,
                state,
                setup.ascii_tree(),
                paint.now_ms,
            );
        }
        ViewMode::DependencyDag => render_dag(frame, tree_area, rows, state),
    }
    render_activity(frame, activity_area, model, rows, state, diagnostics);
    let committed_filter = (!state.filter_query().is_empty()).then(|| state.filter_query());
    frame.render_widget(
        Paragraph::new(footer_line(
            usize::from(footer_area.width),
            committed_filter,
        )),
        footer_area,
    );
    render_interaction_layer(frame, area, model, paint, diagnostics, setup);
}

// increment5-workload-header-projection-begin
#[cfg(feature = "workload-harness")]
/// Feature-only header projection used to exercise measured missing-label failures.
pub(super) mod workload_header_projection {
    use super::*;

    /// Explicit test-only selection of the single permitted header omission.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(in crate::tui) struct WorkloadHeaderProjection {
        pub(in crate::tui) omit_performance_label: bool,
    }

    /// Delegates to the ordinary renderer after projecting only the visible reason label.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::tui) fn render_with_workload_projection(
        frame: &mut Frame<'_>,
        model: &DomainModel,
        performance: &PerformancePublication,
        header: &HeaderInputs,
        paint: PaintSnapshot<'_>,
        diagnostics: &RuntimeDiagnosticsSnapshot,
        setup: &TuiSetup,
        projection: WorkloadHeaderProjection,
    ) {
        let mut projected = performance.clone();
        if projection.omit_performance_label {
            projected
                .snapshot
                .reasons
                .remove(&PerformanceDegradationReason::EventsSixtySeconds);
        }
        super::render(frame, model, &projected, header, paint, diagnostics, setup);
    }
}
// increment5-workload-header-projection-end

fn render_header(
    frame: &mut Frame<'_>,
    area: Rect,
    model: &DomainModel,
    quality: ObservationQuality,
    performance: &PerformancePublication,
    inputs: &HeaderInputs,
    session_elapsed_ms: i64,
) {
    let block = Block::default().borders(Borders::ALL).title(" Herdr Top ");
    let inner_width = usize::from(area.width.saturating_sub(2));
    let line = header_line(
        area.width,
        inner_width,
        model,
        quality,
        performance,
        inputs,
        session_elapsed_ms,
    );
    frame.render_widget(Paragraph::new(line).block(block), area);
}

fn styled_status_line(
    text: String,
    status_start: usize,
    display: Option<DisplayStatus>,
    selected: bool,
) -> Line<'static> {
    let mut base_style = Style::default();
    if selected {
        base_style = base_style.add_modifier(Modifier::REVERSED);
    }
    let Some(display) = display else {
        return Line::from(Span::styled(text, base_style));
    };
    if status_start >= text.len() || !text.is_char_boundary(status_start) {
        return Line::from(Span::styled(text, base_style));
    }

    let token = format!("{} {}", display.glyph(), display.status.label());
    let mut status_end = status_start.saturating_add(token.len()).min(text.len());
    while status_end > status_start && !text.is_char_boundary(status_end) {
        status_end = status_end.saturating_sub(1);
    }
    if status_end == status_start {
        return Line::from(Span::styled(text, base_style));
    }

    let before = text[..status_start].to_owned();
    let status = text[status_start..status_end].to_owned();
    let after = text[status_end..].to_owned();
    let mut status_style = if display.stalled {
        Style::default().fg(Color::Yellow)
    } else {
        match display.status {
            projection::TaskDisplayStatus::Working | projection::TaskDisplayStatus::Done => {
                Style::default().fg(Color::Green)
            }
            projection::TaskDisplayStatus::Blocked | projection::TaskDisplayStatus::Error => {
                Style::default().fg(Color::Red)
            }
            projection::TaskDisplayStatus::Cancelled => Style::default().fg(Color::Yellow),
            projection::TaskDisplayStatus::Queued
            | projection::TaskDisplayStatus::Idle
            | projection::TaskDisplayStatus::Unknown => {
                Style::default().add_modifier(Modifier::DIM)
            }
        }
    };
    if selected {
        status_style = status_style.add_modifier(Modifier::REVERSED);
    }
    Line::from(vec![
        Span::styled(before, base_style),
        Span::styled(status, status_style),
        Span::styled(after, base_style),
    ])
}

fn render_tree(
    frame: &mut Frame<'_>,
    area: Rect,
    model: &DomainModel,
    rows: &[TreeRow],
    state: &AppState,
    ascii: bool,
    now_ms: i64,
) {
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
    let end = start.saturating_add(viewport_height).min(rows.len());
    let columns = visible_metric_columns(width);
    let indent_style = tree_indent_style(&rows[start..end], width, columns);
    let prefixes = tree_connector_prefixes_with_style(rows, ascii, indent_style);
    let lines = rows
        .iter()
        .zip(prefixes.iter())
        .skip(start)
        .take(viewport_height)
        .map(|(row, prefix)| {
            let selected = state.selected() == Some(&row.key);
            let marker = if selected { "> " } else { "  " };
            let metrics = row
                .key
                .run_id()
                .and_then(|run_id| model.task_run(&run_id))
                .map(|run| projection::run_metric_inputs(model, run));
            let metric_width = metric_block_width(columns);
            let reserved_width = if columns.is_empty() {
                0
            } else {
                metric_width.saturating_add(1)
            };
            let label_width = width.saturating_sub(reserved_width);
            let painted_label = if columns.contains(&MetricColumn::Time) {
                row.label_without_duration_suffix
                    .as_deref()
                    .unwrap_or(&row.label)
            } else {
                &row.label
            };
            let label = truncate_to_width(&format!("{marker}{prefix}{painted_label}"), label_width);
            let text = if columns.is_empty() {
                label
            } else {
                format!(
                    "{} {}",
                    pad_to_width(&label, label_width),
                    render_metric_block(metrics.as_ref(), columns, now_ms)
                )
            };
            styled_status_line(
                text,
                marker.len().saturating_add(prefix.len()),
                row.display_status,
                selected,
            )
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(lines), inner);
}

#[cfg(test)]
fn tree_connector_prefixes(rows: &[TreeRow], ascii: bool) -> Vec<String> {
    tree_connector_prefixes_with_style(rows, ascii, TreeIndentStyle::Normal)
}

const MIN_TREE_LABEL_WIDTH: usize = 20;
const TREE_SELECTION_MARKER_WIDTH: usize = 2;

/// One frame-wide connector shape keeps vertical guides aligned while deep trees narrow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TreeIndentStyle {
    Normal,
    Compressed,
    Clamped { max_levels: usize },
}

fn tree_indent_style(
    painted_rows: &[TreeRow],
    width: usize,
    columns: &[MetricColumn],
) -> TreeIndentStyle {
    let max_depth = painted_rows
        .iter()
        .map(|row| row.depth)
        .max()
        .unwrap_or_default();
    let reserved_metrics = if columns.is_empty() {
        0
    } else {
        metric_block_width(columns).saturating_add(1)
    };
    let indent_budget = width
        .saturating_sub(reserved_metrics)
        .saturating_sub(TREE_SELECTION_MARKER_WIDTH)
        .saturating_sub(MIN_TREE_LABEL_WIDTH);
    if max_depth.saturating_mul(4) <= indent_budget {
        return TreeIndentStyle::Normal;
    }
    if max_depth.saturating_mul(2) <= indent_budget {
        return TreeIndentStyle::Compressed;
    }
    TreeIndentStyle::Clamped {
        max_levels: indent_budget.saturating_sub(1) / 2,
    }
}

fn tree_connector_prefixes_with_style(
    rows: &[TreeRow],
    ascii: bool,
    style: TreeIndentStyle,
) -> Vec<String> {
    let mut last_child = vec![false; rows.len()];
    let mut next_at_depth = Vec::<Option<usize>>::new();
    for (index, row) in rows.iter().enumerate().rev() {
        if next_at_depth.len() <= row.depth {
            next_at_depth.resize(row.depth.saturating_add(1), None);
        }
        last_child[index] = next_at_depth[row.depth].is_none();
        next_at_depth.truncate(row.depth.saturating_add(1));
        next_at_depth[row.depth] = Some(index);
    }

    let compressed = !matches!(style, TreeIndentStyle::Normal);
    let (branch, last, vertical, blank) = if ascii && compressed {
        ("|-", "`-", "| ", "  ")
    } else if ascii {
        ("|-- ", "`-- ", "|   ", "    ")
    } else if compressed {
        ("├─", "└─", "│ ", "  ")
    } else {
        ("├── ", "└── ", "│   ", "    ")
    };
    let mut ancestors = Vec::<Option<usize>>::new();
    let mut prefixes = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        ancestors.truncate(row.depth);
        ancestors.resize(row.depth, None);
        let mut components = Vec::with_capacity(row.depth);
        for ancestor in ancestors.iter().take(row.depth).skip(1) {
            if ancestor.is_some_and(|ancestor| !last_child[ancestor]) {
                components.push(vertical);
            } else {
                components.push(blank);
            }
        }
        if row.depth > 0 {
            components.push(if last_child[index] { last } else { branch });
        }
        let mut prefix = String::new();
        if let TreeIndentStyle::Clamped { max_levels } = style
            && components.len() > max_levels
        {
            prefix.push('…');
            prefix.push_str(&components[components.len().saturating_sub(max_levels)..].concat());
        } else {
            prefix.push_str(&components.concat());
        }
        prefixes.push(prefix);
        ancestors.push(Some(index));
    }
    prefixes
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

    if rows
        .iter()
        .all(|row| row.prerequisites.is_empty() && row.dependents.is_empty())
    {
        frame.render_widget(Paragraph::new("no dependency edges recorded"), inner);
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
            styled_status_line(text, marker.len(), row.display_status, selected)
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

fn render_activity(
    frame: &mut Frame<'_>,
    area: Rect,
    model: &DomainModel,
    rows: &[TreeRow],
    state: &AppState,
    diagnostics: &RuntimeDiagnosticsSnapshot,
) {
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
    let strip = projection::runtime_strip(ObservationQuality::Live, diagnostics);
    let runtime = truncate_to_width(
        &format!(
            "p:{} | ctl:{} | D4:{}",
            strip.persistence, strip.controller, strip.d4
        ),
        inner_width,
    );
    let status = state
        .selection_reason_text()
        .or(state.safe_warning())
        .unwrap_or("selection: stable");
    let status = truncate_to_width(status, inner_width);
    let newest = state.selected().and_then(|selected_key| {
        let operator = state.operator_snapshot();
        let detail = projection::detail_projection(
            model,
            rows,
            &operator,
            selected_key,
            state.view_mode(),
            None,
        );
        detail.activity.items.first().map(projection::activity_line)
    });
    let newest = truncate_to_width(
        &format!(
            "Newest: {}",
            newest.as_deref().unwrap_or("none in selected scope")
        ),
        inner_width,
    );
    frame.render_widget(
        Paragraph::new(vec![
            Line::raw(runtime),
            Line::raw(selected),
            Line::raw(status),
            Line::raw(newest),
        ])
        .block(block),
        area,
    );
}

fn footer_line(width: usize, committed_filter: Option<&str>) -> String {
    const FULL: &[&str] = &[
        "q: stop Top only; agents continue",
        "detach: Top runs",
        "↑↓ select",
        "f/End follow",
        "tab view",
        "/ filter",
        "s summary",
        "? help",
        "c clear",
    ];
    const COMPACT: &[&str] = &["q:stop Top; agents continue", "detach:Top runs"];
    const COMMITTED_FILTER_MAX_WIDTH: usize = 32;
    if let Some(query) = committed_filter {
        let filter = truncate_to_width(
            &format!("filter:{}", safe_text(query)),
            width.min(COMMITTED_FILTER_MAX_WIDTH),
        );
        if Span::raw(filter.as_str()).width() >= width {
            return truncate_to_width(&filter, width);
        }
        let hints = if width >= 70 { FULL } else { COMPACT };
        for hint_count in (1..=hints.len()).rev() {
            let candidate = format!("{filter} | {}", hints[..hint_count].join(" | "));
            if Span::raw(candidate.as_str()).width() <= width {
                return candidate;
            }
        }
        return filter;
    }
    let floor = COMPACT[0];
    if Span::raw(floor).width() > width {
        return truncate_to_width(floor, width);
    }

    let hints = if width >= 70 { FULL } else { COMPACT };
    for hint_count in (1..=hints.len()).rev() {
        let candidate = hints[..hint_count].join(" | ");
        if Span::raw(candidate.as_str()).width() <= width {
            return candidate;
        }
    }
    floor.to_owned()
}

fn render_interaction_layer(
    frame: &mut Frame<'_>,
    area: Rect,
    model: &DomainModel,
    paint: PaintSnapshot<'_>,
    diagnostics: &RuntimeDiagnosticsSnapshot,
    setup: &TuiSetup,
) {
    let state = paint.state;
    if let Some(draft) = state.filter_draft() {
        let line = truncate_to_width(&format!("/ filter: {draft}"), usize::from(area.width));
        frame.render_widget(
            Paragraph::new(line),
            Rect::new(
                area.x,
                area.y.saturating_add(area.height.saturating_sub(1)),
                area.width,
                1,
            ),
        );
    }
    let Some(overlay) = state.overlay() else {
        return;
    };
    let modal = Rect::new(
        area.x.saturating_add(2),
        area.y.saturating_add(2),
        area.width.saturating_sub(4),
        area.height.saturating_sub(4),
    );
    if modal.width == 0 || modal.height == 0 {
        return;
    }
    let (title, lines) = match overlay {
        Overlay::Notice => (" Setup notice ", notice_lines(setup)),
        Overlay::Help => (" Help ", help_lines(diagnostics, setup)),
        Overlay::Summary => (
            " Summary ",
            summary_lines(
                model,
                paint.rows,
                state.selected(),
                state.summary_scope(),
                paint.now_ms,
            ),
        ),
        Overlay::Detail => {
            let detail = state.selected().map_or_else(
                || projection::DetailProjection {
                    entity: projection::DetailEntity::Missing,
                    activity: projection::ActivityWindow {
                        items: Vec::new(),
                        retained_count: state.operator_snapshot().activity.len(),
                        matching_count: 0,
                        bound: projection::DETAIL_ACTIVITY_LIMIT,
                        truncated: false,
                    },
                },
                |selected| {
                    projection::detail_projection(
                        model,
                        paint.rows,
                        &state.operator_snapshot(),
                        selected,
                        state.view_mode(),
                        setup.home(),
                    )
                },
            );
            (" Selected detail ", projection::detail_lines(&detail))
        }
    };
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(modal);
    let width = usize::from(inner.width);
    let scroll =
        state.normalize_overlay_scroll(lines.len().saturating_sub(usize::from(inner.height)));
    let visible = lines
        .into_iter()
        .skip(scroll)
        .take(usize::from(inner.height))
        .map(|line| Line::raw(truncate_to_width(&line, width)))
        .collect::<Vec<_>>();
    frame.render_widget(Clear, modal);
    frame.render_widget(Paragraph::new(visible).block(block), modal);
}

fn summary_lines(
    model: &DomainModel,
    rows: &[TreeRow],
    selected: Option<&NodeKey>,
    scope: projection::SummaryScope,
    now_ms: i64,
) -> Vec<String> {
    let summary = projection::summary_projection(model, rows, selected, scope, now_ms);
    let mut lines = vec![match (&summary.scope, &summary.workspace_id) {
        (projection::SummaryScope::SelectionWorkspace, Some(workspace_id)) => {
            format!("scope: workspace {} (w: session)", safe_text(workspace_id))
        }
        (projection::SummaryScope::SelectionWorkspace, None) => {
            "scope: session (selection has no workspace; w toggles scope mode)".to_owned()
        }
        (projection::SummaryScope::Session, _) => {
            "scope: session (w: selection workspace)".to_owned()
        }
    }];
    append_summary_table(&mut lines, "worker kind", summary.worker_kinds);
    append_summary_table(&mut lines, "model", summary.models);
    lines
}

fn append_summary_table(
    lines: &mut Vec<String>,
    dimension: &str,
    rows: Vec<projection::SummaryRow>,
) {
    lines.push(String::new());
    lines.push(format!("per {dimension}"));
    lines.push(format!(
        "{dimension} | runs | live | total | mean | tok | mean tok/s"
    ));
    if rows.is_empty() {
        lines.push("- | 0 | 0 | 00s | - | - | -".to_owned());
        return;
    }
    lines.extend(rows.into_iter().map(|row| {
        let mean = row
            .mean_duration_ms
            .map_or_else(|| "-".to_owned(), format_duration);
        let tokens = row
            .total_output_tokens
            .map_or_else(|| "-".to_owned(), format_token_count);
        let rate = row
            .mean_tokens_per_second
            .map_or_else(|| "-".to_owned(), format_token_rate);
        format!(
            "{} | {} | {} | {} | {mean} | {tokens} | {rate}",
            safe_text(&row.label),
            row.run_count,
            row.live_count,
            format_duration(row.total_duration_ms),
        )
    }));
}

fn notice_lines(setup: &TuiSetup) -> Vec<String> {
    vec![
        "Standalone herdr-top does not exactly match this package.".to_owned(),
        format!("probe: {}", standalone_status(setup)),
        "Controller integration is optional; monitoring continues.".to_owned(),
        "Enter/Esc dismisses; marker publication is best effort.".to_owned(),
    ]
}

fn help_lines(diagnostics: &RuntimeDiagnosticsSnapshot, setup: &TuiSetup) -> Vec<String> {
    let mut lines = vec![
        "q stop Top only; monitored agents continue; detach also leaves Top running".to_owned(),
        "Up/Down select; f or End resumes follow; Tab toggles tree/DAG".to_owned(),
        "/ edits a draft; Enter trims/commits; Esc cancels; empty clears".to_owned(),
        "c persistently clears terminal and hook-only stale runs".to_owned(),
        "Filter: literal Unicode lowercase substring; interior whitespace is literal".to_owned(),
        "Filter excludes paths, activity, Controller free text, content, and raw events".to_owned(),
        "Tree: Left collapse/parent; Right expand/child; Enter toggles branch".to_owned(),
        "Collapse is ignored while filtering and in DAG; stored state survives view toggles"
            .to_owned(),
        "i detail; s summary; w toggles Summary workspace/session scope; ? help".to_owned(),
        "Esc/opening key closes; Up/Down scrolls overlays".to_owned(),
        "Follow pins selection and viewport to newest; manual navigation disables it".to_owned(),
        "Recovery: ancestor, stable neighbor, first; reasons are typed".to_owned(),
        "Controller input is optional capability; standalone setup is optional".to_owned(),
    ];
    lines.extend([
        "Task status: queued=announced, working=active, idle=waiting, blocked=needs attention"
            .to_owned(),
        "Task status: done=finished, error=failed, cancelled=stopped, unknown=insufficient evidence"
            .to_owned(),
        "Warning: ⚠ means stalled; it does not replace the status word".to_owned(),
        "Status source: pane-backed rows use Herdr; headless rows use task/agent evidence".to_owned(),
    ]);
    lines.push(match diagnostics.persistence {
        PersistenceStatus::Healthy => "persistence: healthy".to_owned(),
        PersistenceStatus::Degraded { failure } => format!(
            "persistence: degraded operation={} phase={} code={} durability={}",
            persistence_operation(failure.operation),
            persistence_phase(failure.phase),
            persistence_code(failure.code),
            durability(failure.durability),
        ),
    });
    lines.push(match diagnostics.controller_input {
        ControllerInputStatus::Available => "controller: available".to_owned(),
        ControllerInputStatus::Unavailable { reason } => {
            format!(
                "controller: unavailable reason={}",
                controller_reason(reason)
            )
        }
    });
    lines.push(format!(
        "owner: {}",
        match diagnostics.owner {
            OwnerFreshness::Current => "current",
            OwnerFreshness::Stale => "stale",
        }
    ));
    let persistence = diagnostics.persistence_counters;
    lines.push(format!(
        "persistence counters: not_committed={} durability_unknown={} committed_but_degraded={} skipped={} skipped_owner_updates={}",
        persistence.not_committed_batches,
        persistence.durability_unknown_batches,
        persistence.committed_but_degraded_batches,
        persistence.skipped_batches,
        persistence.skipped_owner_updates,
    ));
    let controller = diagnostics.controller_counters;
    lines.push(format!(
        "controller counters: binding_conflicts={} terminal_blocked_progress_noops={} terminal_forward_reference_creations={} unknown_lane_terminal_drops={}",
        controller.binding_conflicts,
        controller.terminal_blocked_progress_noops,
        controller.terminal_forward_reference_creations,
        controller.unknown_lane_terminal_drops,
    ));
    lines.push(format!(
        "controller counters continued: ingest_sequence_exhaustions={} provider_parent_conflicts={} provider_identity_disagreements={} socket_saturations={} accept_failures={}",
        controller.ingest_sequence_exhaustions,
        controller.provider_parent_conflicts,
        controller.provider_identity_disagreements,
        controller.socket_saturations,
        controller.accept_failures,
    ));
    for source in [
        DiagnosticSource::Herdr,
        DiagnosticSource::Controller,
        DiagnosticSource::Claude,
        DiagnosticSource::Codex,
    ] {
        let availability = diagnostics
            .source_coverage
            .iter()
            .find(|item| item.source == source)
            .map_or(InputAvailability::Unavailable, |item| item.availability);
        lines.push(format!(
            "source {}: {}",
            diagnostic_source(source),
            input_availability(availability)
        ));
    }
    lines.push(format!(
        "occurrence log: {}",
        match diagnostics.first_failure_log {
            OccurrenceLogStatus::NotAttempted => "not_attempted",
            OccurrenceLogStatus::Emitted => "emitted",
            OccurrenceLogStatus::Failed => "failed",
        }
    ));
    lines.push(format!(
        "D4: {}",
        diagnostics.dangling_announcement_components
    ));
    lines.push(format!("standalone probe: {}", standalone_status(setup)));
    lines
}

fn standalone_status(setup: &TuiSetup) -> String {
    match setup.standalone_status() {
        Some(crate::diagnostics::remote::StandaloneVersionStatus::Compatible {
            version,
            stderr_present,
        }) => {
            format!("compatible {version} stderr_present={stderr_present}")
        }
        Some(crate::diagnostics::remote::StandaloneVersionStatus::Mismatch {
            version,
            stderr_present,
        }) => {
            format!("mismatch {version} stderr_present={stderr_present}")
        }
        Some(crate::diagnostics::remote::StandaloneVersionStatus::Unavailable { reason }) => {
            format!("unavailable {}", version_probe_failure(*reason))
        }
        None => "not evaluated (non-owner/default)".to_owned(),
    }
}

const fn persistence_operation(value: PersistenceOperation) -> &'static str {
    match value {
        PersistenceOperation::Apply => "apply",
        PersistenceOperation::Cleanup => "cleanup",
        PersistenceOperation::UpdateOwnerLocation => "update_owner_location",
        PersistenceOperation::ReplaceOwner => "replace_owner",
        PersistenceOperation::Barrier => "barrier",
        PersistenceOperation::Checkpoint => "checkpoint",
    }
}

const fn persistence_phase(value: PersistencePhase) -> &'static str {
    match value {
        PersistencePhase::QueueAdmission => "queue_admission",
        PersistencePhase::CommandExecution => "command_execution",
        PersistencePhase::PostApplyCommit => "post_apply_commit",
        PersistencePhase::Acknowledgement => "acknowledgement",
    }
}

const fn persistence_code(value: PersistenceFailureCode) -> &'static str {
    match value {
        PersistenceFailureCode::Sqlite => "sqlite",
        PersistenceFailureCode::Io => "io",
        PersistenceFailureCode::InvalidData => "invalid_data",
        PersistenceFailureCode::Clock => "clock",
        PersistenceFailureCode::OwnerAbsent => "owner_absent",
        PersistenceFailureCode::CheckpointBusy => "checkpoint_busy",
        PersistenceFailureCode::ChannelClosed => "channel_closed",
        PersistenceFailureCode::AcknowledgementDropped => "acknowledgement_dropped",
    }
}

const fn durability(value: DurabilityDisposition) -> &'static str {
    match value {
        DurabilityDisposition::NotApplicable => "not_applicable",
        DurabilityDisposition::NotCommitted => "not_committed",
        DurabilityDisposition::Committed => "committed",
        DurabilityDisposition::Unknown => "unknown",
    }
}

const fn controller_reason(value: ControllerInputUnavailableReason) -> &'static str {
    match value {
        ControllerInputUnavailableReason::ListenerUnavailable => "listener_unavailable",
        ControllerInputUnavailableReason::RuntimeUnsafe => "runtime_unsafe",
        ControllerInputUnavailableReason::PersistenceUnavailable => "persistence_unavailable",
        ControllerInputUnavailableReason::AcceptorStopped => "acceptor_stopped",
    }
}

const fn diagnostic_source(value: DiagnosticSource) -> &'static str {
    match value {
        DiagnosticSource::Herdr => "Herdr",
        DiagnosticSource::Controller => "Controller",
        DiagnosticSource::Claude => "Claude",
        DiagnosticSource::Codex => "Codex",
    }
}

const fn input_availability(value: InputAvailability) -> &'static str {
    match value {
        InputAvailability::Available => "available",
        InputAvailability::Partial => "partial",
        InputAvailability::Unavailable => "unavailable",
    }
}

const fn version_probe_failure(
    value: crate::diagnostics::remote::VersionProbeFailure,
) -> &'static str {
    use crate::diagnostics::remote::VersionProbeFailure;
    match value {
        VersionProbeFailure::NotFound => "not_found",
        VersionProbeFailure::SpawnFailed => "spawn_failed",
        VersionProbeFailure::TimedOut => "timed_out",
        VersionProbeFailure::ExecutionFailed => "execution_failed",
        VersionProbeFailure::UnsuccessfulExit => "unsuccessful_exit",
        VersionProbeFailure::OutputReadFailed => "output_read_failed",
        VersionProbeFailure::OutputTooLarge => "output_too_large",
        VersionProbeFailure::InvalidOutput => "invalid_output",
    }
}

#[derive(Debug)]
struct HeaderField {
    prefix: &'static str,
    value: String,
    shrinkable: bool,
    droppable: bool,
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
    performance: &PerformancePublication,
    inputs: &HeaderInputs,
    session_elapsed_ms: i64,
) -> Line<'static> {
    let mut fields = Vec::new();
    if screen_width >= 60 {
        fields.push(HeaderField {
            prefix: "host:",
            value: safe_text(&inputs.host),
            shrinkable: true,
            droppable: true,
        });
    }
    fields.push(HeaderField {
        prefix: "session:",
        value: safe_text(&inputs.session),
        shrinkable: true,
        droppable: false,
    });
    fields.push(HeaderField {
        prefix: "up:",
        value: format_session_elapsed(session_elapsed_ms),
        shrinkable: true,
        droppable: true,
    });
    if screen_width >= 72 {
        fields.push(HeaderField {
            prefix: "workspaces:",
            value: model.workspaces().count().to_string(),
            shrinkable: true,
            droppable: true,
        });
    }
    fields.push(HeaderField {
        prefix: "",
        value: quality_label(quality).to_owned(),
        shrinkable: false,
        droppable: false,
    });
    if screen_width >= 88 {
        fields.push(HeaderField {
            prefix: "lag:",
            value: format!("{}ms", performance.snapshot.event_lag.as_millis()),
            shrinkable: true,
            droppable: true,
        });
    }
    if !performance.snapshot.reasons.is_empty() {
        fields.push(HeaderField {
            prefix: "perf:",
            value: performance
                .snapshot
                .reasons
                .iter()
                .copied()
                .map(performance_reason_label)
                .collect::<Vec<_>>()
                .join("+"),
            shrinkable: true,
            droppable: true,
        });
    }
    if screen_width >= 100 {
        let coverage = inputs.source_coverage.borrow().summary();
        fields.push(HeaderField {
            prefix: "sources:",
            value: safe_text(&coverage),
            shrinkable: true,
            droppable: true,
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

fn format_session_elapsed(elapsed_ms: i64) -> String {
    let total_seconds = elapsed_ms.max(0) / 1_000;
    let hours = total_seconds / 3_600;
    let minutes = total_seconds % 3_600 / 60;
    let seconds = total_seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

fn shrink_header_fields(fields: &mut Vec<HeaderField>, available_width: usize) {
    let priorities = [
        "sources:",
        "lag:",
        "workspaces:",
        "host:",
        "session:",
        "perf:",
        "up:",
    ];
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
            break;
        }
    }

    let drop_priorities = ["sources:", "lag:", "workspaces:", "host:", "perf:", "up:"];
    for prefix in drop_priorities {
        if fields_width(fields) <= available_width {
            return;
        }
        if let Some(index) = fields
            .iter()
            .position(|field| field.prefix == prefix && field.droppable)
        {
            fields.remove(index);
        }
    }
}

fn performance_reason_label(reason: PerformanceDegradationReason) -> &'static str {
    match reason {
        PerformanceDegradationReason::LivePanes => "panes",
        PerformanceDegradationReason::DefaultVisibleTaskRuns => "visible_runs",
        PerformanceDegradationReason::DependencyEdges => "dependency_edges",
        PerformanceDegradationReason::EventsOneSecond => "events_1s",
        PerformanceDegradationReason::EventsTenSeconds => "events_10s",
        PerformanceDegradationReason::EventsSixtySeconds => "events_60s",
        PerformanceDegradationReason::EventLag => "event_lag",
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
pub(crate) fn build_tree_rows(
    model: &DomainModel,
    state: &AppState,
    newest_agents: &NewestAgentNodes<'_>,
) -> Vec<TreeRow> {
    let statuses = StatusReadModel::from_model(model, state.now_ms());
    let visible = crate::activity::default_visible_task_run_ids(
        model,
        &state.operator_snapshot(),
        state.now_ms(),
    );
    build_tree_rows_with_statuses(model, state, newest_agents, &statuses, &visible)
}

fn build_tree_rows_with_statuses(
    model: &DomainModel,
    state: &AppState,
    newest_agents: &NewestAgentNodes<'_>,
    statuses: &StatusReadModel,
    visible_runs: &HashSet<RunId>,
) -> Vec<TreeRow> {
    let mut rows = vec![TreeRow {
        key: NodeKey::Session,
        depth: 0,
        label: format!("Session: {}", state.session_display_name()),
        label_without_duration_suffix: None,
        display_status: None,
        prerequisites: Vec::new(),
        dependents: Vec::new(),
    }];
    append_execution_tree_rows(
        &mut rows,
        model,
        state,
        newest_agents,
        statuses,
        visible_runs,
    );
    rows
}

pub(crate) fn build_rows(model: &DomainModel, state: &AppState) -> Vec<TreeRow> {
    build_projection(model, state).rows
}

pub(crate) fn build_projection(model: &DomainModel, state: &AppState) -> RowProjection {
    #[cfg(test)]
    state.record_projection_build();
    let operator = state.operator_snapshot();
    let visible_runs =
        crate::activity::default_visible_task_run_ids(model, &operator, state.now_ms());
    let full_rows = build_full_rows(model, state, &visible_runs);
    projection::project_rows_with_visible(
        model,
        &full_rows,
        &operator,
        &visible_runs,
        state.filter_query(),
        state.collapsed(),
        state.view_mode(),
        state.now_ms(),
    )
}

pub(crate) fn build_uncollapsed_rows(model: &DomainModel, state: &AppState) -> Vec<TreeRow> {
    #[cfg(test)]
    state.record_projection_build();
    let operator = state.operator_snapshot();
    let visible_runs =
        crate::activity::default_visible_task_run_ids(model, &operator, state.now_ms());
    let full_rows = build_full_rows(model, state, &visible_runs);
    projection::project_rows_with_visible(
        model,
        &full_rows,
        &operator,
        &visible_runs,
        state.filter_query(),
        &HashSet::new(),
        state.view_mode(),
        state.now_ms(),
    )
    .rows
}

fn build_full_rows(
    model: &DomainModel,
    state: &AppState,
    visible_runs: &HashSet<RunId>,
) -> Vec<TreeRow> {
    let statuses = StatusReadModel::from_model(model, state.now_ms());
    match state.view_mode() {
        ViewMode::ExecutionTree => {
            let newest_agents = newest_agent_nodes(model, state.now_ms());
            build_tree_rows_with_statuses(model, state, &newest_agents, &statuses, visible_runs)
        }
        ViewMode::DependencyDag => dag::build_rows_with_statuses_visible(
            model,
            state.dag_order(),
            state.now_ms(),
            &statuses,
            visible_runs,
        ),
    }
}

fn append_execution_tree_rows<'model>(
    rows: &mut Vec<TreeRow>,
    model: &'model DomainModel,
    state: &AppState,
    newest_agents: &NewestAgentNodes<'model>,
    statuses: &StatusReadModel,
    visible_runs: &HashSet<RunId>,
) {
    let (mut pane_runs, unattached, nested_runs) = place_runs(model, state, visible_runs);
    let live_lines = LiveLineReadModel::from_model(model);
    let stalled_runs =
        projection::stalled_run_ids(model, state.now_ms(), crate::activity::stall_warn_ms());
    let run_row_context = RunRowContext {
        model,
        nested_runs: &nested_runs,
        newest_agents,
        live_lines: &live_lines,
        stalled_runs: &stalled_runs,
        statuses,
        now_ms: state.now_ms(),
    };
    let mut run_render_state = RunRenderState::default();

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
            label_without_duration_suffix: None,
            display_status: None,
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
                label: topology_row_label("Tab", &tab.tab_id, tab.label.as_deref()),
                label_without_duration_suffix: None,
                display_status: None,
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
                    label: topology_row_label("Pane", &pane.pane_id, pane.display_name.as_deref()),
                    label_without_duration_suffix: None,
                    display_status: None,
                    prerequisites: Vec::new(),
                    dependents: Vec::new(),
                });
                if let Some(runs) = pane_runs.remove(&pane.pane_id) {
                    append_run_rows(
                        rows,
                        runs,
                        Some(&pane.pane_id),
                        4,
                        &run_row_context,
                        &mut run_render_state,
                    );
                }
            }
        }
    }

    if !unattached.is_empty() {
        rows.push(TreeRow {
            key: NodeKey::UnattachedGroup,
            depth: 1,
            label: "Unattached Task Runs".to_owned(),
            label_without_duration_suffix: None,
            display_status: None,
            prerequisites: Vec::new(),
            dependents: Vec::new(),
        });
        let runs = unattached
            .into_iter()
            .map(|run_id| (run_id, false))
            .collect();
        append_run_rows(rows, runs, None, 2, &run_row_context, &mut run_render_state);
    }
}

fn place_runs(
    model: &DomainModel,
    state: &AppState,
    default_visible_runs: &HashSet<RunId>,
) -> (PaneRuns, Vec<RunId>, NestedRuns) {
    let mut pane_runs = PaneRuns::new();
    let mut unattached = Vec::new();
    let mut unplaced = Vec::new();
    let mut candidate_parents = HashMap::new();
    let mut runs = model
        .task_runs()
        .filter(|run| default_visible_runs.contains(&run.run_id))
        .collect::<Vec<_>>();
    // Keep discovery deterministic; each output collection owns its display ordering below.
    runs.sort_by_key(|run| run.run_id);
    for run in runs {
        let all_executions = model
            .executions()
            .filter(|execution| execution.task_run_id == run.run_id)
            .collect::<Vec<_>>();
        let has_execution_history = !all_executions.is_empty();
        let mut executions = all_executions
            .into_iter()
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
            unplaced.push(run.run_id);
            if !has_execution_history
                && let Some(parent) = dispatch_parent_run(model, run.run_id)
                && default_visible_runs.contains(&parent.run_id)
            {
                candidate_parents.insert(run.run_id, parent.run_id);
            }
        }
    }
    let mut nested_runs = NestedRuns::new();
    for run_id in unplaced {
        if let Some(parent_run_id) = candidate_parents.get(&run_id).copied()
            && !parent_chain_has_cycle(run_id, &candidate_parents)
        {
            nested_runs.entry(parent_run_id).or_default().push(run_id);
        } else {
            unattached.push(run_id);
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
    for runs in nested_runs.values_mut() {
        runs.sort_by_key(|run_id| {
            model
                .task_run(run_id)
                .map(|run| (run.display_ordinal.get(), run.run_id))
                .unwrap_or((i64::MAX, *run_id))
        });
        runs.dedup();
    }
    unattached.sort_by_key(|run_id| {
        model
            .task_run(run_id)
            .map(|run| (run.display_ordinal.get(), run.run_id))
            .unwrap_or((i64::MAX, *run_id))
    });
    unattached.dedup();
    (pane_runs, unattached, nested_runs)
}

fn parent_chain_has_cycle(run_id: RunId, candidate_parents: &HashMap<RunId, RunId>) -> bool {
    let mut seen = HashSet::new();
    let mut current = run_id;
    while seen.insert(current) {
        let Some(parent_run_id) = candidate_parents.get(&current).copied() else {
            return false;
        };
        current = parent_run_id;
    }
    true
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

fn topology_row_label(kind: &str, id: &str, name: Option<&str>) -> String {
    let id = safe_text(id);
    match name.filter(|name| !name.is_empty()) {
        Some(name) => format!("{kind}: {id} ({})", safe_text(name)),
        None => format!("{kind}: {id}"),
    }
}

fn append_run_rows(
    rows: &mut Vec<TreeRow>,
    runs: Vec<RunPlacement>,
    pane_id: Option<&str>,
    depth: usize,
    context: &RunRowContext<'_, '_>,
    render_state: &mut RunRenderState,
) {
    for placement in runs {
        append_run_subtree(
            rows,
            placement,
            pane_id,
            depth,
            pane_id.is_some(),
            context,
            render_state,
        );
    }
}

fn append_run_subtree(
    rows: &mut Vec<TreeRow>,
    (run_id, shared): RunPlacement,
    pane_id: Option<&str>,
    depth: usize,
    show_dispatch_parent: bool,
    context: &RunRowContext<'_, '_>,
    render_state: &mut RunRenderState,
) {
    if !render_state.ancestors.insert(run_id) {
        return;
    }
    let Some(run) = context.model.task_run(&run_id) else {
        render_state.ancestors.remove(&run_id);
        return;
    };
    let newest_agent = context.newest_agents.get(&run_id).copied();
    let display_status = context.statuses.task_display_status(
        context.model,
        run,
        pane_id,
        context.stalled_runs.contains(&run_id),
    );
    let signals = RunRowSignals {
        live_lines: context.live_lines,
        display_status,
        show_duration_suffix: true,
    };
    rows.push(TreeRow {
        key: NodeKey::Run {
            run_id,
            pane_id: pane_id.map(str::to_owned),
        },
        depth,
        label: task_run_label_for_placement(
            context.model,
            run,
            shared,
            context.now_ms,
            newest_agent,
            show_dispatch_parent,
            signals,
        ),
        label_without_duration_suffix: Some(task_run_label_for_placement(
            context.model,
            run,
            shared,
            context.now_ms,
            newest_agent,
            show_dispatch_parent,
            RunRowSignals {
                show_duration_suffix: false,
                ..signals
            },
        )),
        display_status: Some(display_status),
        prerequisites: Vec::new(),
        dependents: Vec::new(),
    });
    let agents = context
        .model
        .agent_nodes()
        .filter(|agent| agent.task_run_id == run_id)
        .filter(|agent| agent.parent_agent_node_id.is_some())
        .filter(|agent| !is_live_line_agent(agent))
        .filter(|agent| !projection::agent_node_is_display_stale(agent, context.now_ms))
        .filter(|agent| {
            provider_from_key(&run.key).is_none_or(|provider| provider == agent.provider)
        })
        .collect::<Vec<_>>();
    append_agent_rows(rows, agents, pane_id, depth.saturating_add(1));
    let expand_nested_runs = !shared || render_state.expanded_shared_runs.insert(run_id);
    if expand_nested_runs && let Some(children) = context.nested_runs.get(&run_id) {
        for child_run_id in children {
            append_run_subtree(
                rows,
                (*child_run_id, false),
                None,
                depth.saturating_add(1),
                false,
                context,
                render_state,
            );
        }
    }
    render_state.ancestors.remove(&run_id);
}

fn append_agent_rows(
    rows: &mut Vec<TreeRow>,
    agents: Vec<&AgentNode>,
    pane_id: Option<&str>,
    depth: usize,
) {
    // Filtering happens before this hierarchy is built. A visible child whose stale parent was
    // removed therefore becomes a root under the owning run instead of being silently orphaned.
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
    let display_status = projection::native_agent_display_status(agent);
    rows.push(TreeRow {
        key: NodeKey::Agent {
            agent_node_id: agent.agent_node_id.clone(),
            pane_id: pane_id.map(str::to_owned),
        },
        depth,
        label: agent_node_label(agent, display_status),
        label_without_duration_suffix: None,
        display_status: Some(display_status),
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

fn agent_node_label(agent: &AgentNode, display_status: DisplayStatus) -> String {
    let identity = agent
        .native_session_id
        .as_deref()
        .unwrap_or(&agent.agent_node_id);
    let mut label = format!(
        "{} {} {} native agent: {}",
        display_status.glyph(),
        display_status.status.label(),
        provider_label(agent.provider),
        safe_text(identity),
    );
    if let Some(model) = agent.model_id.as_deref() {
        label.push_str(&format!(" [model:{}]", safe_text(model)));
    }
    if let Some(activity) = agent.last_activity_at_ms {
        label.push_str(&format!(" [last:{activity}ms]"));
    }
    label
}

pub(crate) fn task_run_label(
    model: &DomainModel,
    run: &TaskRun,
    display_status: DisplayStatus,
    shared: bool,
    now_ms: i64,
    show_duration_suffix: bool,
) -> String {
    let mut label = run_row_head(model, run, display_status);
    if show_duration_suffix {
        append_run_duration(&mut label, run, now_ms);
    }
    append_task_run_annotations(model, run, label, shared, true)
}

fn task_run_label_for_placement(
    model: &DomainModel,
    run: &TaskRun,
    shared: bool,
    now_ms: i64,
    newest_agent: Option<&AgentNode>,
    show_dispatch_parent: bool,
    signals: RunRowSignals<'_>,
) -> String {
    let label = run_row_label_with_agent(
        model,
        run,
        newest_agent,
        now_ms,
        signals.live_lines,
        signals.display_status,
        signals.show_duration_suffix,
    );
    append_task_run_annotations(model, run, label, shared, show_dispatch_parent)
}

fn append_task_run_annotations(
    model: &DomainModel,
    run: &TaskRun,
    mut label: String,
    shared: bool,
    show_dispatch_parent: bool,
) -> String {
    if shared {
        label.push_str(" [shared]");
    }
    if show_dispatch_parent && let Some(parent) = dispatch_parent_run(model, run.run_id) {
        label.push_str(&format!(" [dispatched by: {}]", short_run_name(parent)));
    }
    label
}

fn dispatch_parent_run(model: &DomainModel, child_run_id: RunId) -> Option<&TaskRun> {
    let mut parents = model
        .execution_edges()
        .filter(|edge| edge.child_run_id == child_run_id)
        .filter_map(|edge| model.task_run(&edge.parent_run_id))
        .collect::<Vec<_>>();
    parents.sort_by_key(|parent| (parent.display_ordinal.get(), parent.run_id));
    parents.into_iter().next()
}

pub(crate) fn newest_agent_nodes(model: &DomainModel, now_ms: i64) -> NewestAgentNodes<'_> {
    #[cfg(test)]
    record_newest_agent_scan();
    let mut newest = NewestAgentNodes::new();
    for agent in model
        .agent_nodes()
        .filter(|agent| !is_live_line_agent(agent))
        .filter(|agent| !projection::agent_node_is_display_stale(agent, now_ms))
    {
        newest
            .entry(agent.task_run_id)
            .and_modify(|current| {
                if (current.last_activity_at_ms, current.agent_node_id.as_str())
                    < (agent.last_activity_at_ms, agent.agent_node_id.as_str())
                {
                    *current = agent;
                }
            })
            .or_insert(agent);
    }
    newest
}

fn is_live_line_agent(agent: &AgentNode) -> bool {
    agent.last_event_kind.as_deref() == Some(crate::provider::lane::LIVE_LINE_EVENT_KIND)
}

#[cfg(test)]
fn newest_agent_node(model: &DomainModel, run_id: RunId, now_ms: i64) -> Option<&AgentNode> {
    newest_agent_nodes(model, now_ms).get(&run_id).copied()
}

#[cfg(test)]
fn run_row_label(model: &DomainModel, run: &TaskRun, now_ms: i64) -> String {
    let newest_agent = newest_agent_node(model, run.run_id, now_ms);
    let stalled = projection::stalled_run_ids(model, now_ms, crate::activity::stall_warn_ms())
        .contains(&run.run_id);
    let statuses = StatusReadModel::from_model(model, now_ms);
    let display_status = statuses.task_display_status(model, run, None, stalled);
    run_row_label_with_agent(
        model,
        run,
        newest_agent,
        now_ms,
        &LiveLineReadModel::default(),
        display_status,
        true,
    )
}

fn run_row_label_with_agent(
    model: &DomainModel,
    run: &TaskRun,
    newest_agent: Option<&AgentNode>,
    now_ms: i64,
    live_lines: &LiveLineReadModel,
    display_status: DisplayStatus,
    show_duration_suffix: bool,
) -> String {
    let mut label = run_row_head(model, run, display_status);
    let live_line = live_lines.get(&run.run_id).map(str::to_owned).or_else(|| {
        newest_agent
            .filter(|agent| run_uses_claude_tool_line(run, agent))
            .and_then(|agent| {
                agent
                    .last_event_kind
                    .as_deref()
                    .map(|event_kind| (agent, event_kind))
            })
            .map(|(agent, event_kind)| {
                let mut line = safe_text(event_kind);
                if let Some(tool_name) = agent.last_tool_name.as_deref() {
                    line.push_str(": ");
                    line.push_str(&safe_text(tool_name));
                }
                line
            })
    });
    let has_native_session_end = model
        .task_run_v6_state(&run.run_id)
        .is_some_and(|state| state.native_session_end.is_some());
    if !run.state.is_terminal()
        && !has_native_session_end
        && let Some(live_line) = live_line
    {
        label.push_str(" — ");
        label.push_str(&live_line);
    }
    if show_duration_suffix {
        append_run_duration(&mut label, run, now_ms);
    }
    label
}

fn run_row_head(model: &DomainModel, run: &TaskRun, display_status: DisplayStatus) -> String {
    let mut label = format!(
        "{} {} ",
        display_status.glyph(),
        display_status.status.label()
    );
    let run_kind = projection::run_kind_label(model, run);
    label.push_str(&run_kind);
    let subject = if is_codex_worker(model, run) {
        String::new()
    } else {
        run.subject
            .as_deref()
            .map_or_else(|| run_subject_fallback(run), safe_text)
    };
    if !subject.is_empty() {
        label.push(' ');
        label.push_str(&subject);
    }
    label
}

fn append_run_duration(label: &mut String, run: &TaskRun, now_ms: i64) {
    let elapsed_ms = run.created_at_ms.and_then(|created_at_ms| {
        let end_ms = if run.state.is_terminal() {
            run.finished_at_ms?
        } else {
            now_ms
        };
        end_ms
            .checked_sub(created_at_ms)
            .filter(|elapsed_ms| *elapsed_ms >= 0)
    });
    if let Some(elapsed_ms) = elapsed_ms {
        label.push_str(" · ");
        label.push_str(&format_duration(elapsed_ms));
    }
}

fn run_uses_claude_tool_line(run: &TaskRun, agent: &AgentNode) -> bool {
    match &run.key {
        RunKey::Controller(name) => name.starts_with("hook:claude-code:"),
        RunKey::Native { provider, .. } | RunKey::NativePath { provider, .. } => {
            *provider == Provider::Claude
        }
        RunKey::Provisional { .. } => agent.provider == Provider::Claude,
    }
}

fn is_codex_worker(model: &DomainModel, run: &TaskRun) -> bool {
    matches!(
        &run.key,
        RunKey::Native {
            provider: Provider::Codex,
            ..
        } | RunKey::NativePath {
            provider: Provider::Codex,
            ..
        }
    ) && model
        .execution_edges()
        .any(|edge| edge.child_run_id == run.run_id)
}

fn run_subject_fallback(run: &TaskRun) -> String {
    match &run.key {
        RunKey::Controller(name) => name
            .strip_prefix("hook:claude-code:")
            .filter(|suffix| !suffix.contains(':'))
            .map_or_else(|| run_name(run), safe_text),
        RunKey::Native { sid, .. } => safe_text(sid),
        RunKey::NativePath { .. } => run.run_id.to_string(),
        RunKey::Provisional {
            terminal_id,
            start_ms,
            seq,
        } => format!("{}:{start_ms}:{seq}", safe_text(terminal_id)),
    }
}

fn format_duration(elapsed_ms: i64) -> String {
    let total_seconds = elapsed_ms / 1_000;
    if total_seconds >= 3_600 {
        let hours = total_seconds / 3_600;
        let minutes = total_seconds % 3_600 / 60;
        format!("{hours}h{minutes:02}m")
    } else if total_seconds >= 60 {
        let minutes = total_seconds / 60;
        let seconds = total_seconds % 60;
        format!("{minutes:02}m{seconds:02}s")
    } else {
        format!("{total_seconds:02}s")
    }
}

fn run_name(run: &TaskRun) -> String {
    match &run.key {
        RunKey::Controller(name) => safe_text(name),
        RunKey::Native { provider, sid } => {
            format!("{} {}", provider_label(*provider), safe_text(sid))
        }
        RunKey::NativePath { provider, path } => {
            let _ = path;
            format!("{} {}", provider_label(*provider), run.run_id)
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
        RunKey::NativePath { provider, .. } => {
            format!("{} {}", provider_label(*provider), run.run_id)
        }
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

fn safe_text(value: &str) -> String {
    projection::escape_controls(value)
}

pub(crate) fn truncate_to_width(value: &str, max_width: usize) -> String {
    let value = projection::escape_controls(value);
    if Span::raw(value.as_str()).width() <= max_width {
        return value;
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
    for grapheme in value.graphemes(true) {
        let grapheme_width = Span::raw(grapheme).width();
        if width.saturating_add(grapheme_width) > content_width {
            break;
        }
        output.push_str(grapheme);
        width = width.saturating_add(grapheme_width);
    }
    output.push_str(ellipsis);
    output
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::{Buffer, Cell, CellWidth};
    use ratatui::text::{Line, Span};
    use tokio::sync::watch;

    use super::*;
    use crate::activity::{ActivityDurability, ActivityIdentity, ActivityItem, OperatorSnapshot};
    use crate::diagnostics::{
        ControllerCounterSnapshot, ControllerInputStatus, OccurrenceLogStatus, OwnerFreshness,
        PersistenceCounters, RuntimeDiagnosticsSnapshot,
    };
    use crate::herdr::collector::{
        ObservationQuality, PerformancePublication, SourceCoverageRegistry,
    };
    use crate::model::{
        AgentNode, DependencyEdge, DisplayOrdinal, DomainModel, ExecState, Execution,
        ExecutionEdge, NativeSessionEnd, NativeSessionEndStatus, Pane, PaneAgentStatus, Provider,
        RunId, RunKey, Tab, TaskRun, TaskRunV6State, TaskState, Workspace,
    };
    use crate::performance::{PerformanceDegradationReason, PerformanceSnapshot};
    use crate::store::writer::PersistenceStatus;
    use crate::tui::app::{App, AppState, Clock, HeaderInputs, SystemClock, TuiSetup};

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
            label: None,
        });
        model.insert_pane(Pane {
            pane_id: "w1:p1".to_owned(),
            workspace_id: "api".to_owned(),
            tab_id: "implementation".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            display_name: None,
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
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
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
            parent_agent_node_id: Some("provider-root".to_owned()),
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

    fn label_run(
        run_id: RunId,
        key: RunKey,
        state: TaskState,
        created_at_ms: Option<i64>,
        finished_at_ms: Option<i64>,
        subject: Option<&str>,
    ) -> TaskRun {
        TaskRun {
            run_id,
            key,
            display_ordinal: DisplayOrdinal::new(1),
            state,
            has_controller_task_state_event: true,
            created_at_ms,
            updated_at_ms: created_at_ms,
            finished_at_ms,
            subject: subject.map(str::to_owned),
            dismissed_at_ms: None,
        }
    }

    fn label_agent(
        agent_node_id: &str,
        run_id: RunId,
        last_activity_at_ms: Option<i64>,
        last_event_kind: Option<&str>,
        last_tool_name: Option<&str>,
        model_id: Option<&str>,
    ) -> AgentNode {
        AgentNode {
            agent_node_id: agent_node_id.to_owned(),
            provider: Provider::Codex,
            native_session_id: Some(format!("native-{agent_node_id}")),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: model_id.map(str::to_owned),
            last_event_kind: last_event_kind.map(str::to_owned),
            last_tool_name: last_tool_name.map(str::to_owned),
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms,
            session_file: None,
        }
    }

    fn visibility_agent(
        agent_node_id: &str,
        run_id: RunId,
        ordinal: i64,
        state: Option<ExecState>,
        last_activity_at_ms: Option<i64>,
        parent_agent_node_id: Option<&str>,
    ) -> AgentNode {
        AgentNode {
            agent_node_id: agent_node_id.to_owned(),
            provider: Provider::Codex,
            native_session_id: Some(agent_node_id.to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(ordinal),
            parent_agent_node_id: parent_agent_node_id.map(str::to_owned),
            state,
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms,
            session_file: None,
        }
    }

    fn has_agent_row(rows: &[TreeRow], agent_node_id: &str) -> bool {
        rows.iter().any(|row| {
            matches!(
                &row.key,
                NodeKey::Agent {
                    agent_node_id: candidate,
                    ..
                } if candidate == agent_node_id
            )
        })
    }

    fn display(status: projection::TaskDisplayStatus) -> projection::DisplayStatus {
        projection::DisplayStatus::new(status, projection::StatusSource::TaskState)
    }

    #[test]
    fn task_rows_write_glyph_status_and_worker_kind() {
        for (state, status, run_kind, subject, expected) in [
            (
                TaskState::Queued,
                projection::TaskDisplayStatus::Queued,
                "Codex",
                "Prepare worker",
                "◌ queued Codex Prepare worker",
            ),
            (
                TaskState::Running,
                projection::TaskDisplayStatus::Working,
                "Codex",
                "Run tests",
                "● working Codex Run tests",
            ),
            (
                TaskState::Running,
                projection::TaskDisplayStatus::Idle,
                "Claude",
                "Wait",
                "○ idle Claude Wait",
            ),
            (
                TaskState::Blocked,
                projection::TaskDisplayStatus::Blocked,
                "Codex",
                "Approval gate",
                "● blocked Codex Approval gate",
            ),
            (
                TaskState::Completed,
                projection::TaskDisplayStatus::Done,
                "Claude",
                "Finished",
                "✓ done Claude Finished",
            ),
            (
                TaskState::Failed,
                projection::TaskDisplayStatus::Error,
                "Codex",
                "Failed",
                "✗ error Codex Failed",
            ),
            (
                TaskState::Cancelled,
                projection::TaskDisplayStatus::Cancelled,
                "Claude",
                "Stopped",
                "⊘ cancelled Claude Stopped",
            ),
            (
                TaskState::EndedUnknown,
                projection::TaskDisplayStatus::Unknown,
                "provisional",
                "unknown-run",
                "? unknown provisional unknown-run",
            ),
        ] {
            let task_run = label_run(
                RunId::new(),
                RunKey::Controller("fixture".to_owned()),
                state,
                None,
                None,
                Some(subject),
            );
            let mut model = DomainModel::default();
            model.set_run_kind(task_run.run_id, run_kind.to_owned());

            assert_eq!(
                task_run_label(&model, &task_run, display(status), false, 0, false),
                expected,
            );
        }
    }

    #[test]
    fn stalled_rows_keep_their_base_status_word() {
        let task_run = label_run(
            RunId::new(),
            RunKey::Controller("fixture".to_owned()),
            TaskState::Running,
            None,
            None,
            Some("Quiet worker"),
        );
        let mut model = DomainModel::default();
        model.set_run_kind(task_run.run_id, "Codex".to_owned());
        let stalled = projection::DisplayStatus {
            status: projection::TaskDisplayStatus::Working,
            source: projection::StatusSource::TaskState,
            stalled: true,
        };

        assert_eq!(
            task_run_label(&model, &task_run, stalled, false, 0, false),
            "⚠ working Codex Quiet worker",
        );
    }

    #[test]
    fn tree_shared_run_uses_occurrence_specific_status() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = placement_model(&["pane-a", "pane-b"]);
        insert_placement_run(&mut model, run_id, "shared", 1);
        for (pane_id, status) in [
            ("pane-a", PaneAgentStatus::Working),
            ("pane-b", PaneAgentStatus::Blocked),
        ] {
            insert_placement_execution(
                &mut model,
                run_id,
                &format!("execution-{pane_id}"),
                pane_id,
                ExecState::Working,
            );
            model.set_pane_agent_status(pane_id.to_owned(), status);
        }

        let rows = build_rows(&model, &AppState::default());
        let row_for = |pane_id: &str| {
            rows.iter()
                .find(|row| {
                    matches!(
                        &row.key,
                        NodeKey::Run {
                            run_id: candidate,
                            pane_id: Some(candidate_pane),
                        } if *candidate == run_id && candidate_pane == pane_id
                    )
                })
                .unwrap()
        };

        assert_eq!(
            row_for("pane-a").display_status,
            Some(projection::DisplayStatus::new(
                projection::TaskDisplayStatus::Working,
                projection::StatusSource::PaneAgentStatus,
            )),
        );
        assert!(row_for("pane-a").label.starts_with("● working "));
        assert_eq!(
            row_for("pane-b").display_status,
            Some(projection::DisplayStatus::new(
                projection::TaskDisplayStatus::Blocked,
                projection::StatusSource::PaneAgentStatus,
            )),
        );
        assert!(row_for("pane-b").label.starts_with("● blocked "));
    }

    #[test]
    fn pane_parent_keeps_headless_child_and_grandchild_statuses_independent() {
        let parent = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let child = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let grandchild = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let mut model = placement_model(&["pane-parent"]);
        for (run_id, label, ordinal) in [
            (parent, "parent", 1),
            (child, "child", 2),
            (grandchild, "grandchild", 3),
        ] {
            insert_placement_run(&mut model, run_id, label, ordinal);
        }
        insert_placement_execution(
            &mut model,
            parent,
            "parent-execution",
            "pane-parent",
            ExecState::Working,
        );
        model.set_pane_agent_status("pane-parent".to_owned(), PaneAgentStatus::Working);
        for (parent_run_id, child_run_id) in [(parent, child), (child, grandchild)] {
            model.insert_execution_edge(ExecutionEdge {
                parent_run_id,
                child_run_id,
            });
        }
        model.insert_agent_node(visibility_agent(
            "child-root",
            child,
            4,
            Some(ExecState::Idle),
            Some(10),
            None,
        ));
        model.insert_agent_node(visibility_agent(
            "grandchild-root",
            grandchild,
            5,
            Some(ExecState::Blocked),
            Some(20),
            None,
        ));

        let rows = build_rows(&model, &AppState::default());
        for (run_id, expected_status, expected_source) in [
            (
                parent,
                projection::TaskDisplayStatus::Working,
                projection::StatusSource::PaneAgentStatus,
            ),
            (
                child,
                projection::TaskDisplayStatus::Idle,
                projection::StatusSource::AgentNodeState,
            ),
            (
                grandchild,
                projection::TaskDisplayStatus::Blocked,
                projection::StatusSource::AgentNodeState,
            ),
        ] {
            let row = only_run_row(&rows, run_id);
            assert_eq!(
                row.display_status,
                Some(projection::DisplayStatus::new(
                    expected_status,
                    expected_source
                )),
            );
            if run_id != parent {
                assert!(matches!(row.key, NodeKey::Run { pane_id: None, .. }));
            }
        }
    }

    #[test]
    fn root_agent_node_is_hidden_but_parented_descendants_remain() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = DomainModel::default();
        insert_placement_run(&mut model, run_id, "run", 1);
        for agent in [
            visibility_agent("root", run_id, 2, Some(ExecState::Working), Some(1), None),
            visibility_agent(
                "child",
                run_id,
                3,
                Some(ExecState::Idle),
                Some(2),
                Some("root"),
            ),
            visibility_agent(
                "grandchild",
                run_id,
                4,
                Some(ExecState::Blocked),
                Some(3),
                Some("child"),
            ),
        ] {
            model.insert_agent_node(agent);
        }

        let rows = build_rows(&model, &AppState::default());
        assert!(!has_agent_row(&rows, "root"));
        let child = rows
            .iter()
            .find(|row| matches!(&row.key, NodeKey::Agent { agent_node_id, .. } if agent_node_id == "child"))
            .unwrap();
        let grandchild = rows
            .iter()
            .find(|row| matches!(&row.key, NodeKey::Agent { agent_node_id, .. } if agent_node_id == "grandchild"))
            .unwrap();
        let run = only_run_row(&rows, run_id);
        assert_eq!(child.depth, run.depth + 1);
        assert_eq!(grandchild.depth, child.depth + 1);
    }

    #[test]
    fn agent_rows_use_shared_status_vocabulary() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = DomainModel::default();
        insert_placement_run(&mut model, run_id, "run", 1);
        model.insert_agent_node(visibility_agent(
            "root",
            run_id,
            2,
            Some(ExecState::Working),
            Some(0),
            None,
        ));
        for (index, (agent_node_id, state, expected)) in [
            ("idle", Some(ExecState::Idle), "○ idle"),
            ("working", Some(ExecState::Working), "● working"),
            ("blocked", Some(ExecState::Blocked), "● blocked"),
            ("ended", Some(ExecState::Ended), "✓ done"),
            ("stale", Some(ExecState::Stale { since_ms: 0 }), "⚠ unknown"),
            ("unknown", Some(ExecState::Unknown), "? unknown"),
            ("absent", None, "? unknown"),
        ]
        .into_iter()
        .enumerate()
        {
            model.insert_agent_node(AgentNode {
                model_id: Some("model".to_owned()),
                ..visibility_agent(
                    agent_node_id,
                    run_id,
                    index as i64 + 3,
                    state,
                    Some(index as i64),
                    Some("root"),
                )
            });
            let _ = expected;
        }

        let rows = build_rows(&model, &AppState::default());
        for (agent_node_id, expected) in [
            ("idle", "○ idle"),
            ("working", "● working"),
            ("blocked", "● blocked"),
            ("ended", "✓ done"),
            ("stale", "⚠ unknown"),
            ("unknown", "? unknown"),
            ("absent", "? unknown"),
        ] {
            let row = rows
                .iter()
                .find(|row| matches!(&row.key, NodeKey::Agent { agent_node_id: candidate, .. } if candidate == agent_node_id))
                .unwrap();
            assert!(
                row.label
                    .starts_with(&format!("{expected} Codex native agent:")),
                "{}",
                row.label,
            );
            assert!(!row.label.contains("[state:"));
            assert_eq!(
                row.display_status,
                Some(projection::native_agent_display_status(
                    model.agent_node(agent_node_id).unwrap()
                ))
            );
        }
    }

    #[test]
    fn task_row_labels_never_append_unlinked() {
        let task_run = label_run(
            RunId::new(),
            RunKey::Controller("fixture".to_owned()),
            TaskState::Running,
            None,
            None,
            Some("subject"),
        );
        let label = task_run_label(
            &DomainModel::default(),
            &task_run,
            display(projection::TaskDisplayStatus::Working),
            false,
            0,
            false,
        );

        assert!(!label.contains("[unlinked]"), "{label}");
    }

    #[test]
    fn glyph_reflects_state_and_stall() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let live_lines = LiveLineReadModel::default();
        for (state, status, stalled, glyph) in [
            (
                TaskState::Running,
                projection::TaskDisplayStatus::Working,
                false,
                "●",
            ),
            (
                TaskState::Blocked,
                projection::TaskDisplayStatus::Blocked,
                false,
                "●",
            ),
            (
                TaskState::Queued,
                projection::TaskDisplayStatus::Queued,
                false,
                "◌",
            ),
            (
                TaskState::Completed,
                projection::TaskDisplayStatus::Done,
                false,
                "✓",
            ),
            (
                TaskState::Failed,
                projection::TaskDisplayStatus::Error,
                false,
                "✗",
            ),
            (
                TaskState::Cancelled,
                projection::TaskDisplayStatus::Cancelled,
                false,
                "⊘",
            ),
            (
                TaskState::EndedUnknown,
                projection::TaskDisplayStatus::Unknown,
                false,
                "?",
            ),
            (
                TaskState::Running,
                projection::TaskDisplayStatus::Working,
                true,
                "⚠",
            ),
        ] {
            let run = label_run(
                run_id,
                RunKey::Controller("hook:claude-code:session".to_owned()),
                state,
                None,
                None,
                Some("subject"),
            );
            let model = DomainModel::default();
            let display_status = projection::DisplayStatus {
                status,
                source: projection::StatusSource::TaskState,
                stalled,
            };

            assert_eq!(
                run_row_label_with_agent(
                    &model,
                    &run,
                    None,
                    1_000,
                    &live_lines,
                    display_status,
                    true,
                ),
                format!("{glyph} {} claude-code subject", status.label())
            );
        }
    }

    #[test]
    fn native_session_end_suppresses_live_line_but_runtime_status_does_not() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let run = label_run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "native-end".to_owned(),
            },
            TaskState::Running,
            None,
            None,
            Some("subject"),
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        model.insert_agent_node(label_agent(
            "live-line",
            run_id,
            Some(2),
            Some(crate::provider::lane::LIVE_LINE_EVENT_KIND),
            Some("must stay while resumable"),
            None,
        ));
        let live_lines = LiveLineReadModel::from_model(&model);

        for status in [
            projection::TaskDisplayStatus::Idle,
            projection::TaskDisplayStatus::Blocked,
            projection::TaskDisplayStatus::Unknown,
        ] {
            let label = run_row_label_with_agent(
                &model,
                &run,
                None,
                3,
                &live_lines,
                projection::DisplayStatus::new(status, projection::StatusSource::AgentNodeState),
                true,
            );
            assert!(label.contains(" — must stay while resumable"), "{label}");
        }

        model.set_task_run_v6_state(
            run_id,
            TaskRunV6State {
                native_session_end: Some(NativeSessionEnd {
                    status: NativeSessionEndStatus::Cancelled,
                    at_ms: 3,
                }),
                ..TaskRunV6State::default()
            },
        );
        let terminal_label = run_row_label_with_agent(
            &model,
            &run,
            None,
            3,
            &live_lines,
            projection::DisplayStatus::new(
                projection::TaskDisplayStatus::Cancelled,
                projection::StatusSource::NativeSessionLifecycle,
            ),
            true,
        );

        assert_eq!(terminal_label, "⊘ cancelled Codex subject");
    }

    #[test]
    fn build_rows_marks_quiet_run_stalled() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let now_ms = SystemClock.now_ms();
        let mut run = label_run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "quiet-session".to_owned(),
            },
            TaskState::Running,
            None,
            None,
            Some("quiet work"),
        );
        run.updated_at_ms = Some(
            now_ms
                .saturating_sub(crate::activity::DEFAULT_STALL_WARN_MS)
                .saturating_sub(1),
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run);
        let (_sender, receiver) = watch::channel(Arc::new(model));
        let app = App::new(receiver, HeaderInputs::default());

        let row = build_rows(app.model(), app.state())
            .into_iter()
            .find(|row| row.key.run_id() == Some(run_id))
            .expect("build_rows renders the quiet run");

        assert!(
            row.label.starts_with("⚠ "),
            "quiet run did not render the stall glyph: {}",
            row.label
        );
    }

    #[test]
    fn codex_worker_rows_have_no_subject() {
        let root_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let worker_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let root = label_run(
            root_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "root-session".to_owned(),
            },
            TaskState::Running,
            None,
            None,
            Some("root subject"),
        );
        let worker = label_run(
            worker_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "worker-session".to_owned(),
            },
            TaskState::Running,
            None,
            None,
            Some("must not render"),
        );
        let mut model = DomainModel::default();
        model.insert_task_run(root.clone());
        model.insert_task_run(worker.clone());
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: root_id,
            child_run_id: worker_id,
        });
        let live_lines = LiveLineReadModel::default();

        assert_eq!(
            run_row_label_with_agent(
                &model,
                &root,
                None,
                0,
                &live_lines,
                display(projection::TaskDisplayStatus::Working),
                true,
            ),
            "● working Codex root subject"
        );
        assert_eq!(
            run_row_label_with_agent(
                &model,
                &worker,
                None,
                0,
                &live_lines,
                display(projection::TaskDisplayStatus::Working),
                true,
            ),
            "● working Codex"
        );
    }

    #[test]
    fn summary_rows_group_all_runs_and_count_only_valid_terminal_durations() {
        let mut model = DomainModel::default();
        let fixtures = [
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                RunKey::Controller("hook:claude-code:S:task:A".to_owned()),
                TaskState::Completed,
                Some(1_000),
                Some(10_000),
                Some("model-a"),
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FAW",
                RunKey::Controller("hook:claude-code:S:task:B".to_owned()),
                TaskState::Running,
                Some(2_000),
                Some(99_000),
                Some("model-a"),
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                RunKey::Controller("hook:claude-code:S:task:C".to_owned()),
                TaskState::Failed,
                None,
                None,
                Some("model-a"),
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FAY",
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "codex-b".to_owned(),
                },
                TaskState::Completed,
                Some(1_000),
                Some(8_000),
                Some("model-b"),
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
                RunKey::Controller("hook:claude-code:S:task:D".to_owned()),
                TaskState::Cancelled,
                Some(4_000),
                Some(11_000),
                Some("model-b"),
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FB0",
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "codex-a-terminal".to_owned(),
                },
                TaskState::EndedUnknown,
                Some(4_000),
                Some(11_000),
                Some("model-a"),
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FB1",
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "codex-a-live".to_owned(),
                },
                TaskState::Blocked,
                Some(2_000),
                Some(90_000),
                Some("model-a"),
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FB2",
                RunKey::Provisional {
                    terminal_id: "unknown-model".to_owned(),
                    start_ms: 1,
                    seq: 1,
                },
                TaskState::Completed,
                Some(9_000),
                Some(8_000),
                None,
            ),
        ];

        for (id, key, state, created, finished, model_id) in fixtures {
            let run_id = run_id(id);
            model.insert_task_run(label_run(run_id, key, state, created, finished, None));
            if let Some(model_id) = model_id {
                model.telemetry_entry(run_id, 0).accumulate(
                    10,
                    Some(model_id.to_owned()),
                    None,
                    None,
                    false,
                );
                model.set_run_rate_totals(
                    run_id,
                    crate::model::RunRateTotals {
                        output_tokens: 10,
                        working_ms: 1_000,
                    },
                );
            }
        }

        let summary = projection::summary_projection(
            &model,
            &[],
            None,
            projection::SummaryScope::Session,
            123_456,
        );
        let worker_kinds = summary
            .worker_kinds
            .iter()
            .map(|row| {
                (
                    row.label.clone(),
                    row.run_count,
                    row.live_count,
                    row.total_duration_ms,
                    row.mean_duration_ms,
                    row.total_output_tokens,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            worker_kinds,
            [
                (
                    "claude-code".to_owned(),
                    4,
                    1,
                    16_000,
                    Some(8_000),
                    Some(40),
                ),
                ("Codex".to_owned(), 3, 1, 14_000, Some(7_000), Some(30),),
                ("provisional".to_owned(), 1, 0, 0, None, None,),
            ]
        );
        assert_eq!(
            summary
                .models
                .iter()
                .map(|row| (
                    row.label.as_str(),
                    row.run_count,
                    row.total_duration_ms,
                    row.total_output_tokens,
                ))
                .collect::<Vec<_>>(),
            [
                ("model-a", 5, 16_000, Some(50)),
                ("model-b", 2, 14_000, Some(20)),
                ("unknown", 1, 0, None),
            ]
        );
        assert!(
            summary
                .worker_kinds
                .iter()
                .filter(|row| row.total_output_tokens.is_some())
                .all(|row| row.mean_tokens_per_second.is_some())
        );
    }

    #[test]
    fn unattached_run_after_workspace_renders_honest_session_scope() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FB3");
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "preceding-workspace".to_owned(),
        });
        model.insert_task_run(label_run(
            run_id,
            RunKey::Controller("unattached".to_owned()),
            TaskState::Running,
            None,
            None,
            None,
        ));
        let selected = NodeKey::Run {
            run_id,
            pane_id: None,
        };
        let rows = vec![
            TreeRow {
                key: NodeKey::Workspace("preceding-workspace".to_owned()),
                depth: 1,
                label: "preceding-workspace".to_owned(),
                label_without_duration_suffix: None,
                display_status: None,
                prerequisites: Vec::new(),
                dependents: Vec::new(),
            },
            TreeRow {
                key: NodeKey::UnattachedGroup,
                depth: 1,
                label: "Unattached Task Runs".to_owned(),
                label_without_duration_suffix: None,
                display_status: None,
                prerequisites: Vec::new(),
                dependents: Vec::new(),
            },
            TreeRow {
                key: selected.clone(),
                depth: 2,
                label: "unattached".to_owned(),
                label_without_duration_suffix: None,
                display_status: None,
                prerequisites: Vec::new(),
                dependents: Vec::new(),
            },
        ];

        let summary = projection::summary_projection(
            &model,
            &rows,
            Some(&selected),
            projection::SummaryScope::SelectionWorkspace,
            10,
        );
        let lines = summary_lines(
            &model,
            &rows,
            Some(&selected),
            projection::SummaryScope::SelectionWorkspace,
            10,
        );

        assert_eq!(summary.workspace_id, None);
        assert_eq!(
            lines.first().map(String::as_str),
            Some("scope: session (selection has no workspace; w toggles scope mode)")
        );
        assert!(
            lines
                .first()
                .is_some_and(|line| !line.starts_with("scope: workspace "))
        );
    }

    #[test]
    fn task_run_rows_use_readable_fixed_time_grammar() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let cases = [
            (
                label_run(
                    run_id,
                    RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
                    TaskState::Running,
                    Some(10_000),
                    None,
                    Some("Implement I7 Task 2 wire tolerance"),
                ),
                Some(label_agent(
                    "agent-a",
                    run_id,
                    Some(1_032_000),
                    Some("tool_use"),
                    Some("Bash"),
                    Some("gpt-5.6-sol"),
                )),
                1_033_000,
                "⚠ working claude-code Implement I7 Task 2 wire tolerance — tool_use: Bash · 17m03s",
            ),
            (
                label_run(
                    run_id,
                    RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
                    TaskState::Queued,
                    None,
                    None,
                    None,
                ),
                None,
                5_000,
                "◌ queued claude-code hook:claude-code:S:task:T",
            ),
            (
                label_run(
                    run_id,
                    RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
                    TaskState::Completed,
                    Some(10_000),
                    Some(3_671_000),
                    Some("Finish work"),
                ),
                Some(label_agent(
                    "agent-a",
                    run_id,
                    Some(3_670_000),
                    Some("tool_use"),
                    Some("Bash"),
                    Some("gpt-terminal"),
                )),
                9_000_000,
                "✓ done claude-code Finish work · 1h01m",
            ),
            (
                label_run(
                    run_id,
                    RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
                    TaskState::Running,
                    None,
                    None,
                    Some("No timing"),
                ),
                Some(label_agent(
                    "agent-a",
                    run_id,
                    Some(5_000),
                    Some("message"),
                    None,
                    None,
                )),
                5_000,
                "● working claude-code No timing — message",
            ),
            (
                label_run(
                    run_id,
                    RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
                    TaskState::Running,
                    Some(5_001),
                    None,
                    Some("Clock skew"),
                ),
                None,
                5_000,
                "● working claude-code Clock skew",
            ),
        ];

        for (run, agent, now_ms, expected) in cases {
            let mut model = DomainModel::default();
            model.insert_task_run(run.clone());
            if let Some(agent) = agent {
                model.insert_agent_node(agent);
            }
            assert_eq!(run_row_label(&model, &run, now_ms), expected);
        }
    }

    #[test]
    fn newest_agent_activity_and_model_use_deterministic_recency_order() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let run = label_run(
            run_id,
            RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
            TaskState::Running,
            None,
            None,
            Some("Tie break"),
        );
        let older = label_agent(
            "agent-z",
            run_id,
            Some(99),
            Some("older"),
            None,
            Some("model-older"),
        );
        let newer = label_agent(
            "agent-a",
            run_id,
            Some(100),
            Some("newer"),
            Some("Read"),
            Some("model-newer"),
        );
        let tied_low = label_agent(
            "agent-a",
            run_id,
            Some(100),
            Some("tie-low"),
            None,
            Some("model-low"),
        );
        let tied_high = label_agent(
            "agent-z",
            run_id,
            Some(100),
            Some("tie-high"),
            Some("Bash"),
            Some("model-high"),
        );

        let mut recency_model = DomainModel::default();
        recency_model.insert_task_run(run.clone());
        recency_model.insert_agent_node(older);
        recency_model.insert_agent_node(newer);
        assert_eq!(
            run_row_label(&recency_model, &run, 1_000),
            "● working claude-code Tie break — newer: Read"
        );

        for agents in [
            [tied_low.clone(), tied_high.clone()],
            [tied_high.clone(), tied_low.clone()],
        ] {
            let mut tie_model = DomainModel::default();
            tie_model.insert_task_run(run.clone());
            for agent in agents {
                tie_model.insert_agent_node(agent);
            }
            for _ in 0..8 {
                assert_eq!(
                    run_row_label(&tie_model, &run, 1_000),
                    "● working claude-code Tie break — tie-high: Bash"
                );
            }
        }
    }

    #[test]
    fn display_stale_newest_agent_does_not_supply_run_live_line_fallback() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let run = label_run(
            run_id,
            RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
            TaskState::Running,
            None,
            None,
            Some("Fallback source"),
        );
        let mut hidden_stale = label_agent(
            "hidden-stale",
            run_id,
            Some(100),
            Some("hidden-stale-event"),
            Some("Bash"),
            None,
        );
        hidden_stale.state = Some(ExecState::Ended);
        let visible_fresh = label_agent(
            "visible-fresh",
            run_id,
            Some(50),
            Some("visible-fresh-event"),
            Some("Read"),
            None,
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        model.insert_agent_node(hidden_stale);
        model.insert_agent_node(visible_fresh);
        let now_ms = 100_i64.saturating_add(crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS);

        let label = run_row_label(&model, &run, now_ms);

        assert!(
            label.contains("visible-fresh-event: Read"),
            "visible fresh agent fallback missing from run row: {label}"
        );
        assert!(
            !label.contains("hidden-stale-event: Bash"),
            "display-stale agent fallback leaked into run row: {label}"
        );
    }

    #[test]
    fn only_display_stale_agent_supplies_no_run_live_line_fallback() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let run = label_run(
            run_id,
            RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
            TaskState::Running,
            None,
            None,
            Some("Only stale"),
        );
        let mut hidden_stale = label_agent(
            "hidden-stale",
            run_id,
            Some(100),
            Some("hidden-only-event"),
            Some("Bash"),
            None,
        );
        hidden_stale.state = Some(ExecState::Ended);
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        model.insert_agent_node(hidden_stale);
        let now_ms = 100_i64.saturating_add(crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS);

        let label = run_row_label(&model, &run, now_ms);

        assert_eq!(label, "● working claude-code Only stale");
        assert!(
            !label.contains("hidden-only-event: Bash"),
            "display-stale only agent leaked into run row: {label}"
        );
    }

    #[test]
    fn visible_agent_keeps_run_live_line_fallback() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let run = label_run(
            run_id,
            RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
            TaskState::Running,
            None,
            None,
            Some("Visible fallback"),
        );
        let visible = label_agent(
            "visible",
            run_id,
            Some(50),
            Some("visible-event"),
            Some("Read"),
            None,
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run.clone());
        model.insert_agent_node(visible);
        let now_ms = 100_i64.saturating_add(crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS);

        assert_eq!(
            run_row_label(&model, &run, now_ms),
            "● working claude-code Visible fallback — visible-event: Read"
        );
    }

    #[test]
    fn full_projection_indexes_newest_agent_nodes_once() {
        let mut model = DomainModel::default();
        for (index, label) in ["first", "second", "third"].into_iter().enumerate() {
            let run_id = RunId::new();
            let run = label_run(
                run_id,
                RunKey::Controller(label.to_owned()),
                TaskState::Running,
                None,
                None,
                Some(label),
            );
            model.insert_task_run(run);
            model.insert_agent_node(label_agent(
                &format!("agent-{index}"),
                run_id,
                Some(index as i64),
                Some("message"),
                None,
                Some("model"),
            ));
        }

        reset_newest_agent_scan_count();
        let projection = build_projection(&model, &AppState::default());

        assert_eq!(
            projection
                .rows
                .iter()
                .filter(|row| row.key.run_id().is_some())
                .count(),
            3
        );
        assert_eq!(newest_agent_scan_count(), 1);
    }

    #[test]
    fn task_run_annotations_follow_the_new_head_in_existing_order() {
        let parent_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let child_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let orphan_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let parent = label_run(
            parent_id,
            RunKey::Controller("Parent".to_owned()),
            TaskState::Running,
            None,
            None,
            Some("Parent subject"),
        );
        let child = label_run(
            child_id,
            RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
            TaskState::Running,
            None,
            None,
            Some("Shared child"),
        );
        let orphan = label_run(
            orphan_id,
            RunKey::Controller("hook:codex:S:task:U".to_owned()),
            TaskState::Queued,
            None,
            None,
            Some("Orphan"),
        );
        let mut model = DomainModel::default();
        for run in [&parent, &child, &orphan] {
            model.insert_task_run(run.clone());
        }
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent_id,
            child_run_id: child_id,
        });

        assert_eq!(
            task_run_label(
                &model,
                &child,
                display(projection::TaskDisplayStatus::Working),
                true,
                10,
                true,
            ),
            "● working claude-code Shared child [shared] [dispatched by: Parent]"
        );
        assert_eq!(
            task_run_label(
                &model,
                &orphan,
                display(projection::TaskDisplayStatus::Queued),
                false,
                10,
                true,
            ),
            "◌ queued codex Orphan"
        );
    }

    #[test]
    fn duration_formatter_uses_fixed_boundaries() {
        for (elapsed_ms, expected) in [
            (7_000, "07s"),
            (59_000, "59s"),
            (60_000, "01m00s"),
            (3_599_000, "59m59s"),
            (3_600_000, "1h00m"),
            (443_100_000, "123h05m"),
        ] {
            assert_eq!(format_duration(elapsed_ms), expected);
        }
    }

    #[test]
    fn columns_shed_at_declared_thresholds() {
        use MetricColumn::{Effort, Model, Time, Tok, TokPerSecond};

        for (width, expected) in [
            (usize::MAX, &[Model, Effort, Tok, TokPerSecond, Time][..]),
            (120, &[Model, Effort, Tok, TokPerSecond, Time][..]),
            (119, &[Effort, Tok, TokPerSecond, Time][..]),
            (104, &[Effort, Tok, TokPerSecond, Time][..]),
            (103, &[Tok, TokPerSecond, Time][..]),
            (90, &[Tok, TokPerSecond, Time][..]),
            (89, &[Tok, Time][..]),
            (76, &[Tok, Time][..]),
            (75, &[Time][..]),
            (62, &[Time][..]),
            (61, &[][..]),
            (0, &[][..]),
        ] {
            assert_eq!(visible_metric_columns(width), expected, "width {width}");
        }
    }

    #[test]
    fn model_names_shorten_and_ellipsize() {
        for (model, expected) in [
            ("claude-fable-5", "fable-5"),
            ("claude-3-5-sonnet-20241022", "3-5-sonnet"),
            ("claude-sonnet-4-2025-01-01", "sonnet-4"),
            ("gpt-4o-20240513", "gpt-4o"),
            ("gpt4o20240513", "gpt4o"),
            ("gpt-5.6-sol", "gpt-5.6-sol"),
            ("gpt-5.6-terra", "gpt-5.6-te…"),
            ("long-model-name", "long-model…"),
        ] {
            let formatted = format_model_value(Some(model));
            assert_eq!(formatted, expected, "model {model}");
            assert!(Span::raw(formatted.as_str()).width() <= 11);
        }
        assert_eq!(format_model_value(None), "—");
        assert_eq!(
            Span::raw(format_model_value(Some("gpt-5.6-terra"))).width(),
            11
        );
    }

    #[test]
    fn effort_values_abbreviate_and_treat_empty_as_absent() {
        assert_eq!(format_effort_value(Some("minimal")), "min");
        assert_eq!(format_effort_value(Some("medium")), "med");
        assert_eq!(format_effort_value(Some("xhigh")), "xhigh");
        assert_eq!(format_effort_value(None), "—");
        assert_eq!(format_effort_value(Some("")), "—");
    }

    #[test]
    fn lifetime_output_and_measured_rate_are_stable_across_completion() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let running = label_run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "telemetry".to_owned(),
            },
            TaskState::Running,
            Some(0),
            None,
            Some("telemetry"),
        );
        let mut model = DomainModel::default();
        model.telemetry_entry(run_id, 0).accumulate(
            83,
            Some("gpt-5.6-sol".to_owned()),
            Some("xhigh".to_owned()),
            None,
            true,
        );
        model.set_run_rate_totals(
            run_id,
            crate::model::RunRateTotals {
                output_tokens: 83,
                working_ms: 10_000,
            },
        );
        let running_metrics = projection::run_metric_inputs(&model, &running);

        assert_eq!(
            format_metric_value(MetricColumn::Tok, &running_metrics, 10_000),
            "83"
        );
        assert_eq!(
            format_metric_value(MetricColumn::TokPerSecond, &running_metrics, 10_000),
            "8.3/s"
        );

        let mut completed = running.clone();
        completed.state = TaskState::Completed;
        completed.finished_at_ms = Some(10_000);
        let completed_metrics = projection::run_metric_inputs(&model, &completed);
        assert_eq!(
            format_metric_value(MetricColumn::TokPerSecond, &completed_metrics, 90_000),
            "8.3/s",
            "the cumulative mean must stop at the terminal timestamp"
        );

        let mut unrated = running_metrics.clone();
        unrated.measured_working_ms = Some(0);
        assert_eq!(
            format_metric_value(MetricColumn::TokPerSecond, &unrated, 10_000),
            "—"
        );
        unrated.measured_working_ms = None;
        assert_eq!(
            format_metric_value(MetricColumn::TokPerSecond, &unrated, 10_000),
            "—"
        );
    }

    #[test]
    fn key_fallback_subject_does_not_repeat_native_or_provisional_worker_kind() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        for (key, expected) in [
            (
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "native-session".to_owned(),
                },
                "● working Codex native-session",
            ),
            (
                RunKey::NativePath {
                    provider: Provider::Codex,
                    path: "/private/session.jsonl".to_owned(),
                },
                "● working Codex 01ARZ3NDEKTSV4RRFFQ69G5FAV",
            ),
            (
                RunKey::Provisional {
                    terminal_id: "terminal".to_owned(),
                    start_ms: 1,
                    seq: 2,
                },
                "● working provisional terminal:1:2",
            ),
            (
                RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
                "● working claude-code hook:claude-code:S:task:T",
            ),
        ] {
            let run = label_run(run_id, key, TaskState::Running, None, None, None);
            let mut model = DomainModel::default();
            model.insert_task_run(run.clone());

            assert_eq!(run_row_label(&model, &run, 0), expected);
        }
    }

    #[test]
    fn worker_kind_labels_follow_key_variants_and_escape_controller_text() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        for (key, expected) in [
            (
                RunKey::Native {
                    provider: Provider::Claude,
                    sid: "native".to_owned(),
                },
                "Claude",
            ),
            (
                RunKey::NativePath {
                    provider: Provider::Codex,
                    path: "/private/path".to_owned(),
                },
                "Codex",
            ),
            (
                RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
                "claude-code",
            ),
            (
                RunKey::Controller("not-a-hook\nname".to_owned()),
                "not-a-hook\\nname",
            ),
            (
                RunKey::Controller("hook:missing-second-colon".to_owned()),
                "hook:missing-second-colon",
            ),
            (
                RunKey::Provisional {
                    terminal_id: "terminal".to_owned(),
                    start_ms: 1,
                    seq: 2,
                },
                "provisional",
            ),
        ] {
            let run = label_run(run_id, key, TaskState::Running, None, None, None);
            assert_eq!(projection::worker_kind_label(&run), expected);
        }
    }

    #[test]
    fn run_row_head_keeps_kind_for_every_current_run_class() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        for (key, expected_kind) in [
            (
                RunKey::Controller("hook:claude-code:root-session".to_owned()),
                "claude-code",
            ),
            (
                RunKey::Controller("hook:claude-code:root-session:agent:agent-7".to_owned()),
                "claude-code",
            ),
            (
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "rollout".to_owned(),
                },
                "Codex",
            ),
            (
                RunKey::Native {
                    provider: Provider::Claude,
                    sid: "session".to_owned(),
                },
                "Claude",
            ),
            (
                RunKey::NativePath {
                    provider: Provider::Codex,
                    path: "/private/rollout.jsonl".to_owned(),
                },
                "Codex",
            ),
            (
                RunKey::Provisional {
                    terminal_id: "terminal".to_owned(),
                    start_ms: 1,
                    seq: 2,
                },
                "provisional",
            ),
        ] {
            let run = label_run(run_id, key, TaskState::Running, None, None, Some("subject"));
            let head = run_row_head(
                &DomainModel::default(),
                &run,
                display(projection::TaskDisplayStatus::Working),
            );
            assert!(
                head.starts_with(&format!("● working {expected_kind}")),
                "missing current kind {expected_kind}: {head}"
            );
        }
    }

    fn app(model: DomainModel, quality: ObservationQuality, session: &str) -> App {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(model));
        let (_performance_sender, performance) = watch::channel(PerformancePublication {
            snapshot: PerformanceSnapshot {
                event_lag: Duration::from_millis(23),
                ..PerformanceSnapshot::default()
            },
            effective_quality: quality,
            #[cfg(feature = "workload-harness")]
            workload_sample_stamp: None,
        });
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
            HeaderInputs {
                host: "build-host".to_owned(),
                session: session.to_owned(),
                source_coverage,
                performance,
            },
        )
    }

    fn healthy_runtime_diagnostics() -> RuntimeDiagnosticsSnapshot {
        RuntimeDiagnosticsSnapshot {
            persistence: PersistenceStatus::Healthy,
            persistence_detail: None,
            controller_input: ControllerInputStatus::Available,
            owner: OwnerFreshness::Current,
            persistence_counters: PersistenceCounters::default(),
            controller_counters: ControllerCounterSnapshot::default(),
            enrichment_counters: crate::diagnostics::EnrichmentCounterSnapshot::default(),
            provider_counters: crate::diagnostics::ProviderCounterSnapshot::default(),
            source_coverage: Vec::new(),
            dangling_announcement_components: 0,
            first_failure_log: OccurrenceLogStatus::NotAttempted,
        }
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

    fn render_status_row(
        display_status: projection::DisplayStatus,
        dag: bool,
        width: u16,
    ) -> Buffer {
        let label = format!(
            "{} {} Codex A deliberately long subject",
            display_status.glyph(),
            display_status.status.label(),
        );
        let rows = vec![TreeRow {
            key: NodeKey::Run {
                run_id: RunId::new(),
                pane_id: None,
            },
            depth: 0,
            label,
            label_without_duration_suffix: None,
            display_status: Some(display_status),
            prerequisites: if dag {
                vec!["prerequisite".to_owned()]
            } else {
                Vec::new()
            },
            dependents: Vec::new(),
        }];
        let backend = TestBackend::new(width, 4);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                if dag {
                    render_dag(
                        frame,
                        Rect::new(0, 0, width, 4),
                        &rows,
                        &AppState::default(),
                    );
                } else {
                    render_tree(
                        frame,
                        Rect::new(0, 0, width, 4),
                        &DomainModel::default(),
                        &rows,
                        &AppState::default(),
                        false,
                        0,
                    );
                }
            })
            .unwrap();
        terminal.backend().buffer().clone()
    }

    fn status_glyph_cell(buffer: &Buffer, glyph: &str) -> Cell {
        buffer
            .content
            .iter()
            .find(|cell| cell.symbol() == glyph)
            .cloned()
            .unwrap_or_else(|| panic!("missing status glyph {glyph:?}: {:?}", buffer_rows(buffer)))
    }

    #[test]
    fn status_tokens_use_accessible_colors_in_tree_and_dag() {
        for (status, expected_color, expected_dim) in [
            (projection::TaskDisplayStatus::Working, Color::Green, false),
            (projection::TaskDisplayStatus::Done, Color::Green, false),
            (projection::TaskDisplayStatus::Blocked, Color::Red, false),
            (projection::TaskDisplayStatus::Error, Color::Red, false),
            (
                projection::TaskDisplayStatus::Cancelled,
                Color::Yellow,
                false,
            ),
            (projection::TaskDisplayStatus::Queued, Color::Reset, true),
            (projection::TaskDisplayStatus::Idle, Color::Reset, true),
            (projection::TaskDisplayStatus::Unknown, Color::Reset, true),
        ] {
            let display_status = display(status);
            for dag in [false, true] {
                let buffer = render_status_row(display_status, dag, 72);
                let cell = status_glyph_cell(&buffer, display_status.glyph());
                assert_eq!(cell.fg, expected_color, "{status:?} dag={dag}");
                assert_eq!(
                    cell.modifier.contains(Modifier::DIM),
                    expected_dim,
                    "{status:?} dag={dag}: {:?}",
                    cell.modifier,
                );
            }
        }
    }

    #[test]
    fn blocked_status_is_red_and_distinct_from_green_working() {
        let working = render_status_row(display(projection::TaskDisplayStatus::Working), false, 64);
        let blocked = render_status_row(display(projection::TaskDisplayStatus::Blocked), false, 64);
        let error = render_status_row(display(projection::TaskDisplayStatus::Error), true, 64);

        assert_eq!(status_glyph_cell(&working, "●").fg, Color::Green);
        assert_eq!(status_glyph_cell(&blocked, "●").fg, Color::Red);
        assert_eq!(status_glyph_cell(&error, "✗").fg, Color::Red);
    }

    #[test]
    fn stalled_status_uses_yellow_warning_style() {
        let stalled = projection::DisplayStatus {
            status: projection::TaskDisplayStatus::Working,
            source: projection::StatusSource::TaskState,
            stalled: true,
        };
        let stalled_buffer = render_status_row(stalled, false, 64);
        let cancelled_buffer =
            render_status_row(display(projection::TaskDisplayStatus::Cancelled), true, 64);

        assert_eq!(status_glyph_cell(&stalled_buffer, "⚠").fg, Color::Yellow);
        assert_eq!(status_glyph_cell(&cancelled_buffer, "⊘").fg, Color::Yellow);
    }

    #[test]
    fn selected_status_remains_readable_under_reversal() {
        let display_status = display(projection::TaskDisplayStatus::Working);
        let line = styled_status_line(
            "  ● working Codex subject".to_owned(),
            2,
            Some(display_status),
            true,
        );

        assert!(
            line.spans
                .iter()
                .all(|span| span.style.add_modifier.contains(Modifier::REVERSED))
        );
        let status = line
            .spans
            .iter()
            .find(|span| span.content.contains("● working"))
            .unwrap();
        assert_eq!(status.style.fg, Some(Color::Green));
    }

    #[test]
    fn narrow_tree_retains_glyph_and_status_before_subject() {
        let buffer = render_status_row(display(projection::TaskDisplayStatus::Working), false, 16);
        let rendered = buffer_rows(&buffer).join("\n");

        assert!(rendered.contains("● working"), "{rendered}");
        assert!(!rendered.contains("deliberately long subject"));
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

    fn performance_snapshot(
        lag: Duration,
        reasons: impl IntoIterator<Item = PerformanceDegradationReason>,
    ) -> PerformanceSnapshot {
        PerformanceSnapshot {
            event_lag: lag,
            reasons: reasons.into_iter().collect(),
            ..PerformanceSnapshot::default()
        }
    }

    fn rendered_header(snapshot: PerformanceSnapshot, width: u16) -> String {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (_coverage_sender, source_coverage) = watch::channel(SourceCoverageRegistry::default());
        let (_performance_sender, performance) = watch::channel(PerformancePublication {
            snapshot,
            effective_quality: ObservationQuality::Degraded,
            #[cfg(feature = "workload-harness")]
            workload_sample_stamp: None,
        });
        let app = App::new(
            model_receiver,
            HeaderInputs {
                host: "build-host".to_owned(),
                session: "dynamic-performance".to_owned(),
                source_coverage,
                performance,
            },
        );
        let rows = render(&app, width, 18);
        let matching = rows
            .into_iter()
            .filter(|row| row.contains("session:"))
            .collect::<Vec<_>>();
        assert_eq!(
            matching.len(),
            1,
            "one bordered header body row is required"
        );
        matching.into_iter().next().unwrap()
    }

    #[test]
    fn wide_header_renders_live_lag_and_stable_performance_reasons() {
        let snapshot = performance_snapshot(
            Duration::from_millis(1_234),
            [
                PerformanceDegradationReason::EventsSixtySeconds,
                PerformanceDegradationReason::DependencyEdges,
            ],
        );
        let line = rendered_header(snapshot, 160);
        assert!(line.contains("lag:1234ms"));
        assert!(line.contains("perf:dependency_edges+events_60s"));
    }

    #[test]
    fn performance_reason_labels_match_workload_schema_v1() {
        let fixture = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/workload-schema-v1.json"),
        )
        .unwrap();
        let fixture: serde_json::Value = serde_json::from_slice(&fixture).unwrap();
        let variants = [
            PerformanceDegradationReason::LivePanes,
            PerformanceDegradationReason::DefaultVisibleTaskRuns,
            PerformanceDegradationReason::DependencyEdges,
            PerformanceDegradationReason::EventsOneSecond,
            PerformanceDegradationReason::EventsTenSeconds,
            PerformanceDegradationReason::EventsSixtySeconds,
            PerformanceDegradationReason::EventLag,
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
        let actual = variants
            .into_iter()
            .map(|variant| {
                let reason = match variant {
                    PerformanceDegradationReason::LivePanes => "live_panes",
                    PerformanceDegradationReason::DefaultVisibleTaskRuns => {
                        "default_visible_task_runs"
                    }
                    PerformanceDegradationReason::DependencyEdges => "dependency_edges",
                    PerformanceDegradationReason::EventsOneSecond => "events_one_second",
                    PerformanceDegradationReason::EventsTenSeconds => "events_ten_seconds",
                    PerformanceDegradationReason::EventsSixtySeconds => "events_sixty_seconds",
                    PerformanceDegradationReason::EventLag => "event_lag",
                };
                serde_json::json!({
                    "reason": reason,
                    "label": performance_reason_label(variant),
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::to_vec(&actual).unwrap(),
            serde_json::to_vec(&fixture["performance_reason_labels"]).unwrap()
        );
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

        let label_column = |label: &str| {
            let row = rows.iter().find(|row| row.contains(label)).unwrap();
            let byte_offset = row.find(label).unwrap();
            Span::raw(&row[..byte_offset]).width()
        };
        let session_x = label_column("Session: demo");
        let workspace_x = label_column("Workspace: api");
        let tab_x = label_column("Tab: implementation");
        let pane_x = label_column("Pane: w1:p1");
        let run_x = label_column("Codex controller");
        let agent_x = label_column("Codex native agent: investigate");
        assert_eq!(workspace_x, session_x + 4);
        assert_eq!(tab_x, workspace_x + 4);
        assert_eq!(pane_x, tab_x + 4);
        assert_eq!(run_x, pane_x + 14);
        assert_eq!(agent_x, run_x + 4);
    }

    #[test]
    fn tree_rows_append_safe_optional_tab_and_pane_names() {
        let unnamed = populated_model();
        let unnamed_rows = build_rows(&unnamed, &AppState::default());
        assert!(
            unnamed_rows
                .iter()
                .any(|row| row.label == "Tab: implementation")
        );
        assert!(unnamed_rows.iter().any(|row| row.label == "Pane: w1:p1"));

        let mut named = populated_model();
        named.insert_tab(Tab {
            tab_id: "implementation".to_owned(),
            workspace_id: "api".to_owned(),
            label: Some("レビュー\n".to_owned()),
        });
        named.insert_pane(Pane {
            pane_id: "w1:p1".to_owned(),
            workspace_id: "api".to_owned(),
            tab_id: "implementation".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            display_name: Some("UI修正\t".to_owned()),
        });
        let named_rows = build_rows(&named, &AppState::default());
        assert!(
            named_rows
                .iter()
                .any(|row| row.label == "Tab: implementation (レビュー\\n)")
        );
        assert!(
            named_rows
                .iter()
                .any(|row| row.label == "Pane: w1:p1 (UI修正\\t)")
        );
    }

    #[test]
    fn topology_rows_omit_absent_names() {
        assert_eq!(topology_row_label("Tab", "w1:t1", None), "Tab: w1:t1");
        assert_eq!(topology_row_label("Pane", "w1:p1", Some("")), "Pane: w1:p1");
    }

    #[test]
    fn topology_rows_omit_empty_names() {
        assert_eq!(topology_row_label("Tab", "w1:t1", Some("")), "Tab: w1:t1");
        assert_eq!(topology_row_label("Pane", "w1:p1", None), "Pane: w1:p1");
    }

    fn connector_fixture_rows() -> Vec<TreeRow> {
        [
            (NodeKey::Session, 0, "root"),
            (NodeKey::Workspace("a".to_owned()), 1, "a"),
            (NodeKey::Tab("a1".to_owned()), 2, "a1"),
            (NodeKey::Pane("a1x".to_owned()), 3, "a1x"),
            (NodeKey::Tab("a2".to_owned()), 2, "a2"),
            (NodeKey::Workspace("b".to_owned()), 1, "b"),
            (NodeKey::Tab("b1".to_owned()), 2, "b1"),
        ]
        .into_iter()
        .map(|(key, depth, label)| TreeRow {
            key,
            depth,
            label: label.to_owned(),
            label_without_duration_suffix: None,
            display_status: None,
            prerequisites: Vec::new(),
            dependents: Vec::new(),
        })
        .collect()
    }

    #[test]
    fn tree_connector_prefixes_render_exact_utf8_structure() {
        assert_eq!(
            tree_connector_prefixes(&connector_fixture_rows(), false),
            [
                "",
                "├── ",
                "│   ├── ",
                "│   │   └── ",
                "│   └── ",
                "└── ",
                "    └── ",
            ]
        );
    }

    #[test]
    fn tree_connector_prefixes_render_exact_ascii_structure() {
        assert_eq!(
            tree_connector_prefixes(&connector_fixture_rows(), true),
            [
                "",
                "|-- ",
                "|   |-- ",
                "|   |   `-- ",
                "|   `-- ",
                "`-- ",
                "    `-- ",
            ]
        );
    }

    #[test]
    fn compressed_tree_connector_prefixes_render_exact_shapes() {
        assert_eq!(
            tree_connector_prefixes_with_style(
                &connector_fixture_rows(),
                false,
                TreeIndentStyle::Compressed,
            ),
            ["", "├─", "│ ├─", "│ │ └─", "│ └─", "└─", "  └─"]
        );
        assert_eq!(
            tree_connector_prefixes_with_style(
                &connector_fixture_rows(),
                true,
                TreeIndentStyle::Compressed,
            ),
            ["", "|-", "| |-", "| | `-", "| `-", "`-", "  `-"]
        );
    }

    #[test]
    fn clamped_tree_connector_prefixes_elide_leading_levels() {
        let style = TreeIndentStyle::Clamped { max_levels: 2 };
        assert_eq!(
            tree_connector_prefixes_with_style(&connector_fixture_rows(), false, style),
            ["", "├─", "│ ├─", "…│ └─", "│ └─", "└─", "  └─"]
        );
        assert_eq!(
            tree_connector_prefixes_with_style(&connector_fixture_rows(), true, style),
            ["", "|-", "| |-", "…| `-", "| `-", "`-", "  `-"]
        );
    }

    #[test]
    fn tree_indent_style_switches_at_exact_budget_boundaries() {
        let max_depth = 4;
        let rows = vec![TreeRow {
            key: NodeKey::Session,
            depth: max_depth,
            label: "deep".to_owned(),
            label_without_duration_suffix: None,
            display_status: None,
            prerequisites: Vec::new(),
            dependents: Vec::new(),
        }];
        let columns = &[MetricColumn::Time];
        let fixed_width = metric_block_width(columns)
            .saturating_add(1)
            .saturating_add(TREE_SELECTION_MARKER_WIDTH)
            .saturating_add(MIN_TREE_LABEL_WIDTH);
        let normal_budget = max_depth * 4;
        let compressed_budget = max_depth * 2;

        assert_eq!(
            tree_indent_style(&rows, fixed_width + normal_budget, columns),
            TreeIndentStyle::Normal
        );
        assert_eq!(
            tree_indent_style(&rows, fixed_width + normal_budget - 1, columns),
            TreeIndentStyle::Compressed
        );
        assert_eq!(
            tree_indent_style(&rows, fixed_width + compressed_budget, columns),
            TreeIndentStyle::Compressed
        );
        assert_eq!(
            tree_indent_style(&rows, fixed_width + compressed_budget - 1, columns),
            TreeIndentStyle::Clamped { max_levels: 3 }
        );
    }

    #[test]
    fn deep_indent_compresses_when_narrow() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let run = label_run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "deep".to_owned(),
            },
            TaskState::Running,
            Some(0),
            None,
            Some("deeply nested work"),
        );
        let mut model = DomainModel::default();
        model.insert_task_run(run);
        model.telemetry_entry(run_id, 0).accumulate(
            83,
            Some("gpt-5.6-sol".to_owned()),
            Some("xhigh".to_owned()),
            None,
            true,
        );
        let rows = vec![TreeRow {
            key: NodeKey::Run {
                run_id,
                pane_id: None,
            },
            depth: 12,
            label: "● working Codex deeply nested work".to_owned(),
            label_without_duration_suffix: None,
            display_status: Some(display(projection::TaskDisplayStatus::Working)),
            prerequisites: Vec::new(),
            dependents: Vec::new(),
        }];
        let width = 78;
        let backend = TestBackend::new(width, 3);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                render_tree(
                    frame,
                    Rect::new(0, 0, width, 3),
                    &model,
                    &rows,
                    &AppState::default(),
                    false,
                    10_000,
                );
            })
            .unwrap();
        let rendered = buffer_rows(terminal.backend().buffer());
        let row = rendered
            .iter()
            .find(|row| row.contains('●'))
            .expect("the deeply nested status glyph remains visible");
        let glyph_byte = row.find('●').unwrap();
        let glyph_column = Span::raw(&row[..glyph_byte]).width();

        assert_eq!(
            glyph_column,
            1 + TREE_SELECTION_MARKER_WIDTH + 12 * 2,
            "compressed indent changed: {row}"
        );
        assert!(
            row.contains("   83    10s"),
            "metric columns missing: {row}"
        );
    }

    #[test]
    fn execution_tree_row_depths_remain_unchanged() {
        let rows = build_rows(&populated_model(), &AppState::default());
        assert_eq!(
            rows.iter().map(|row| row.depth).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4, 5]
        );
    }

    fn render_dag_fixture(rows: &[TreeRow], width: u16, height: u16) -> Vec<String> {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                render_dag(
                    frame,
                    Rect::new(0, 0, width, height),
                    rows,
                    &AppState::default(),
                );
            })
            .unwrap();
        buffer_rows(terminal.backend().buffer())
    }

    fn dag_inner_content(rows: &[String], width: usize) -> Vec<String> {
        rows[1..rows.len().saturating_sub(1)]
            .iter()
            .map(|row| {
                row.chars()
                    .skip(1)
                    .take(width.saturating_sub(2))
                    .collect::<String>()
                    .trim()
                    .to_owned()
            })
            .filter(|row| !row.is_empty())
            .collect()
    }

    #[test]
    fn dag_zero_edges_renders_exact_one_placeholder_line() {
        let rendered = render_dag_fixture(&connector_fixture_rows(), 60, 7);
        assert_eq!(
            dag_inner_content(&rendered, 60),
            ["no dependency edges recorded"]
        );
        let screen = rendered.join("\n");
        assert!(!screen.contains("Task Run"));
        assert!(!screen.contains("Prereqs"));
        assert!(!screen.contains("Dependents"));
    }

    #[test]
    fn dag_with_edges_keeps_heading_and_rows() {
        let mut rows = connector_fixture_rows()[..2].to_vec();
        rows[0].dependents = vec!["a".to_owned()];
        rows[1].prerequisites = vec!["root".to_owned()];

        let rendered = render_dag_fixture(&rows, 60, 7).join("\n");

        assert!(rendered.contains("Task Run"));
        assert!(rendered.contains("Prereqs"));
        assert!(rendered.contains("Dependents"));
        assert!(rendered.contains("root"));
        assert!(rendered.contains("a"));
        assert!(!rendered.contains("no dependency edges recorded"));
    }

    #[test]
    fn footer_drops_only_whole_trailing_hints() {
        let full = [
            "q: stop Top only; agents continue",
            "detach: Top runs",
            "↑↓ select",
            "f/End follow",
            "tab view",
            "/ filter",
            "s summary",
            "? help",
            "c clear",
        ];
        let compact = ["q:stop Top; agents continue", "detach:Top runs"];
        assert_eq!(footer_line(140, None), full.join(" | "));
        let without_clear = full[..full.len() - 1].join(" | ");
        assert_eq!(
            footer_line(Span::raw(without_clear.as_str()).width(), None),
            without_clear
        );

        for width in [100, 72, 69, 45, 27] {
            let rendered = footer_line(width, None);
            let expected_tier = if width >= 70 { &full[..] } else { &compact[..] };
            let pieces = rendered.split(" | ").collect::<Vec<_>>();
            assert_eq!(pieces, expected_tier[..pieces.len()], "width {width}");
            assert!(Span::raw(rendered.as_str()).width() <= width);
        }
    }

    #[test]
    fn footer_preserves_mandated_floor() {
        const FLOOR: &str = "q:stop Top; agents continue";
        assert_eq!(Span::raw(FLOOR).width(), 27);
        assert_eq!(footer_line(27, None), FLOOR);
        assert_eq!(footer_line(26, None), truncate_to_width(FLOOR, 26));
    }

    #[test]
    fn committed_filter_indicator_persists_and_draft_overrides() {
        let mut app = app(
            DomainModel::default(),
            ObservationQuality::Live,
            "filter-footer",
        );
        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        for character in "needle".chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE));
        }
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        for width in [48, 62, 76, 104, 160] {
            let footer = render(&app, width, 18).pop().unwrap();
            assert!(
                footer.contains("filter:needle"),
                "committed filter disappeared at width {width}: {footer}"
            );
            assert!(
                !footer.contains("/ filter: needle"),
                "width {width}: {footer}"
            );
        }

        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        app.handle_key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));
        let footer = render(&app, 104, 18).pop().unwrap();
        assert!(footer.contains("/ filter: needlex"), "{footer}");
        assert!(!footer.contains("filter:needle |"), "{footer}");
    }

    #[test]
    fn header_fields_drop_in_declared_order_and_up_last() {
        let model = populated_model();
        let inputs = HeaderInputs::default();
        let mut performance = inputs.performance.borrow().clone();
        performance
            .snapshot
            .reasons
            .insert(PerformanceDegradationReason::LivePanes);
        let header_field_names = |available_width| {
            let header = header_line(
                160,
                available_width,
                &model,
                ObservationQuality::Live,
                &performance,
                &inputs,
                3_661_000,
            )
            .spans
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<String>();
            header
                .split(" | ")
                .map(|field| {
                    if field == "LIVE" {
                        "quality"
                    } else {
                        field.split_once(':').map_or(field, |(prefix, _)| prefix)
                    }
                })
                .collect::<Vec<_>>()
                .join(" | ")
        };

        for (width, expected) in [
            (
                76,
                "host | session | up | workspaces | quality | lag | perf | sources",
            ),
            (
                75,
                "host | session | up | workspaces | quality | lag | perf",
            ),
            (63, "host | session | up | workspaces | quality | perf"),
            (55, "host | session | up | quality | perf"),
            (40, "session | up | quality | perf"),
            (31, "session | up | quality"),
            (22, "session | quality"),
        ] {
            assert_eq!(header_field_names(width), expected, "width {width}");
        }
    }

    #[test]
    fn up_field_shrinks_and_drops_last() {
        let model = populated_model();
        let inputs = HeaderInputs::default();
        let performance = inputs.performance.borrow().clone();
        let render_header_text = |available_width| {
            header_line(
                160,
                available_width,
                &model,
                ObservationQuality::Live,
                &performance,
                &inputs,
                3_661_000,
            )
            .spans
            .into_iter()
            .map(|span| span.content.into_owned())
            .collect::<String>()
        };

        let moderate = render_header_text(70);
        assert!(moderate.contains("up:"), "{moderate}");
        assert!(!moderate.contains("up:01:01:01"), "{moderate}");

        let after_another_drop = render_header_text(58);
        assert!(after_another_drop.contains("up:"), "{after_another_drop}");
        assert!(
            !after_another_drop.contains("sources:"),
            "{after_another_drop}"
        );

        let extreme = render_header_text(18);
        assert!(!extreme.contains("up:"), "{extreme}");
        assert!(extreme.contains("session:"), "{extreme}");
        assert!(extreme.contains("LIVE"), "{extreme}");
    }

    #[test]
    fn help_lists_every_task_status_and_stall_semantics() {
        let help_lines = help_lines(&healthy_runtime_diagnostics(), &TuiSetup::default());
        assert!(
            help_lines.iter().any(|line| line
                == "Task status: queued=announced, working=active, idle=waiting, blocked=needs attention"),
            "{help_lines:#?}"
        );
        assert!(
            help_lines.iter().any(|line| line
                == "Task status: done=finished, error=failed, cancelled=stopped, unknown=insufficient evidence"),
            "{help_lines:#?}"
        );
        let help = help_lines.join("\n");

        assert!(help.contains("blocked=needs attention"), "{help}");
        assert!(!help.contains("blocked=approval"), "{help}");
        assert!(!help.contains("approval required"), "{help}");
        assert!(help.contains("⚠ means stalled"), "{help}");
        assert!(
            help.contains("it does not replace the status word"),
            "{help}"
        );
    }

    #[test]
    fn help_explains_pane_backed_and_headless_sources() {
        let help = help_lines(&healthy_runtime_diagnostics(), &TuiSetup::default()).join("\n");

        assert!(help.contains("pane-backed rows use Herdr"), "{help}");
        assert!(
            help.contains("headless rows use task/agent evidence"),
            "{help}"
        );
    }

    #[test]
    fn help_status_section_scrolls_in_narrow_height() {
        let mut app = app(
            DomainModel::default(),
            ObservationQuality::Live,
            "status-help",
        );
        app.handle_key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));

        let mut transcript = String::new();
        for _ in 0..40 {
            transcript.push_str(&render(&app, 120, 14).join("\n"));
            transcript.push('\n');
            app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        }

        assert!(
            transcript.contains("Task status: queued=announced"),
            "{transcript}"
        );
        assert!(transcript.contains("persistence: healthy"), "{transcript}");
        assert!(
            transcript.contains("standalone probe: not evaluated (non-owner/default)"),
            "{transcript}"
        );
    }

    #[test]
    fn summary_is_discoverable_in_full_footer_and_help() {
        let mut app = app(
            DomainModel::default(),
            ObservationQuality::Live,
            "summary-help",
        );
        let footer = render(&app, 160, 18).pop().unwrap();
        assert!(footer.contains("s summary"), "{footer}");

        app.handle_key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
        let help = render(&app, 160, 18).join("\n");
        assert!(help.contains("s summary"), "{help}");
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
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
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
                if row.contains("ordinal-first ordinal-first") {
                    Some("ordinal-first")
                } else if row.contains("lexical-first lexical-first") {
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
        let mut app = App::new(model_receiver, HeaderInputs::default());

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
                label: None,
            });
            model.set_tab_ordinal(tab_id.to_owned(), DisplayOrdinal::new(ordinal));
        }
        for (pane_id, ordinal) in [("pane-a", 6), ("pane-z", 5)] {
            model.insert_pane(Pane {
                pane_id: pane_id.to_owned(),
                workspace_id: "workspace-z".to_owned(),
                tab_id: "tab-z".to_owned(),
                terminal_id: format!("terminal-{pane_id}"),
                display_name: None,
            });
            model.set_pane_ordinal(pane_id.to_owned(), DisplayOrdinal::new(ordinal));
        }

        let state = AppState::default();
        let newest_agents = newest_agent_nodes(&model, state.now_ms());
        let rows = build_tree_rows(&model, &state, &newest_agents);
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
            label: None,
        });
        initial.set_tab_ordinal("tab".to_owned(), DisplayOrdinal::new(2));
        for (pane_id, ordinal) in [("pane-old", 3), ("pane-new", 4)] {
            initial.insert_pane(Pane {
                pane_id: pane_id.to_owned(),
                workspace_id: "workspace".to_owned(),
                tab_id: "tab".to_owned(),
                terminal_id: format!("terminal-{pane_id}"),
                display_name: None,
            });
            initial.set_pane_ordinal(pane_id.to_owned(), DisplayOrdinal::new(ordinal));
        }
        initial.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller("placement".to_owned()),
            display_ordinal: DisplayOrdinal::new(5),
            state: TaskState::Completed,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
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
        let mut app = App::new(model_receiver, HeaderInputs::default());

        model_sender.send(Arc::new(refreshed)).unwrap();
        app.refresh();
        let newest_agents = newest_agent_nodes(app.model(), app.state().now_ms());
        let rows = build_tree_rows(app.model(), app.state(), &newest_agents);
        let hosting_pane = rows.iter().find_map(|row| match &row.key {
            NodeKey::Run {
                run_id: actual,
                pane_id,
            } if *actual == run_id => pane_id.as_deref(),
            _ => None,
        });

        assert_eq!(hosting_pane, Some("pane-new"));
    }

    fn placement_model(pane_ids: &[&str]) -> DomainModel {
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "workspace".to_owned(),
        });
        model.insert_tab(Tab {
            tab_id: "tab".to_owned(),
            workspace_id: "workspace".to_owned(),
            label: None,
        });
        for (index, pane_id) in pane_ids.iter().enumerate() {
            model.insert_pane(Pane {
                pane_id: (*pane_id).to_owned(),
                workspace_id: "workspace".to_owned(),
                tab_id: "tab".to_owned(),
                terminal_id: format!("terminal-{pane_id}"),
                display_name: None,
            });
            model.set_pane_ordinal((*pane_id).to_owned(), DisplayOrdinal::new(index as i64 + 1));
        }
        model
    }

    fn insert_placement_run(model: &mut DomainModel, run_id: RunId, label: &str, ordinal: i64) {
        model.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller(label.to_owned()),
            display_ordinal: DisplayOrdinal::new(ordinal),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
    }

    fn insert_placement_execution(
        model: &mut DomainModel,
        run_id: RunId,
        execution_id: &str,
        pane_id: &str,
        state: ExecState,
    ) {
        model.insert_execution(Execution {
            execution_id: execution_id.to_owned(),
            pane_id: pane_id.to_owned(),
            terminal_id: format!("terminal-{pane_id}"),
            task_run_id: run_id,
            state,
        });
    }

    fn only_run_row(rows: &[TreeRow], run_id: RunId) -> &TreeRow {
        let matches = rows
            .iter()
            .filter(|row| row.key.run_id() == Some(run_id))
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1, "expected exactly one row for {run_id}");
        matches[0]
    }

    #[test]
    fn headless_child_nests_under_dispatch_parent() {
        let parent = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let child = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let mut model = placement_model(&["pane-parent"]);
        insert_placement_run(&mut model, parent, "parent", 1);
        insert_placement_run(&mut model, child, "child", 2);
        insert_placement_execution(
            &mut model,
            parent,
            "parent-execution",
            "pane-parent",
            ExecState::Working,
        );
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: child,
        });

        let state = AppState::default();
        let newest_agents = newest_agent_nodes(&model, state.now_ms());
        let rows = build_tree_rows(&model, &state, &newest_agents);
        let parent_row = only_run_row(&rows, parent);
        let child_row = only_run_row(&rows, child);
        let parent_index = rows.iter().position(|row| row == parent_row).unwrap();
        let child_index = rows.iter().position(|row| row == child_row).unwrap();

        assert_eq!(parent_row.depth, 4);
        assert_eq!(child_row.depth, 5);
        assert_eq!(
            child_row.key,
            NodeKey::Run {
                run_id: child,
                pane_id: None,
            }
        );
        assert!(parent_index < child_index);
        assert!(!child_row.label.contains("[dispatched by:"));
        assert!(rows.iter().all(|row| row.key != NodeKey::UnattachedGroup));
    }

    #[test]
    fn shared_parent_renders_nested_descendants_once() {
        let parent = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let child = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let mut model = placement_model(&["pane-first", "pane-second"]);
        insert_placement_run(&mut model, parent, "parent", 1);
        insert_placement_run(&mut model, child, "child", 2);
        for pane_id in ["pane-second", "pane-first"] {
            insert_placement_execution(
                &mut model,
                parent,
                &format!("parent-execution-{pane_id}"),
                pane_id,
                ExecState::Working,
            );
        }
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: child,
        });

        let state = AppState::default();
        let newest_agents = newest_agent_nodes(&model, state.now_ms());
        let rows = build_tree_rows(&model, &state, &newest_agents);
        let parent_rows = rows
            .iter()
            .filter(|row| row.key.run_id() == Some(parent))
            .collect::<Vec<_>>();
        let child_rows = rows
            .iter()
            .filter(|row| row.key.run_id() == Some(child))
            .collect::<Vec<_>>();

        assert_eq!(parent_rows.len(), 2);
        assert!(parent_rows.iter().all(|row| row.label.contains("[shared]")));
        assert_eq!(child_rows.len(), 1);
        assert_eq!(
            child_rows[0].key,
            NodeKey::Run {
                run_id: child,
                pane_id: None,
            }
        );
    }

    #[test]
    fn ended_exec_pane_fallback_still_wins_over_parent() {
        let parent = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let ended_child = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let headless_grandchild = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let mut model = placement_model(&["pane-parent", "pane-ended"]);
        insert_placement_run(&mut model, parent, "parent", 1);
        insert_placement_run(&mut model, ended_child, "ended-child", 2);
        insert_placement_run(&mut model, headless_grandchild, "headless-grandchild", 3);
        insert_placement_execution(
            &mut model,
            parent,
            "parent-execution",
            "pane-parent",
            ExecState::Working,
        );
        insert_placement_execution(
            &mut model,
            ended_child,
            "ended-child-execution",
            "pane-ended",
            ExecState::Ended,
        );
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: ended_child,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: ended_child,
            child_run_id: headless_grandchild,
        });

        let state = AppState::default();
        let newest_agents = newest_agent_nodes(&model, state.now_ms());
        let rows = build_tree_rows(&model, &state, &newest_agents);
        let ended_row = only_run_row(&rows, ended_child);
        let grandchild_row = only_run_row(&rows, headless_grandchild);

        assert_eq!(ended_row.depth, 4);
        assert_eq!(
            ended_row.key,
            NodeKey::Run {
                run_id: ended_child,
                pane_id: Some("pane-ended".to_owned()),
            }
        );
        assert!(ended_row.label.contains("[dispatched by: parent]"));
        assert_eq!(grandchild_row.depth, 5);
        assert_eq!(
            grandchild_row.key,
            NodeKey::Run {
                run_id: headless_grandchild,
                pane_id: None,
            }
        );
    }

    #[test]
    fn three_level_chain_renders() {
        let root = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let child = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let grandchild = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let mut model = placement_model(&["pane-root"]);
        for (run_id, label, ordinal) in [
            (root, "root", 1),
            (child, "child", 2),
            (grandchild, "grandchild", 3),
        ] {
            insert_placement_run(&mut model, run_id, label, ordinal);
        }
        insert_placement_execution(
            &mut model,
            root,
            "root-execution",
            "pane-root",
            ExecState::Working,
        );
        for (parent_run_id, child_run_id) in [(root, child), (child, grandchild)] {
            model.insert_execution_edge(ExecutionEdge {
                parent_run_id,
                child_run_id,
            });
        }

        let state = AppState::default();
        let newest_agents = newest_agent_nodes(&model, state.now_ms());
        let rows = build_tree_rows(&model, &state, &newest_agents);
        let run_rows = rows
            .iter()
            .filter(|row| row.key.run_id().is_some())
            .collect::<Vec<_>>();

        assert_eq!(run_rows.len(), 3);
        assert_eq!(
            run_rows
                .iter()
                .map(|row| (row.key.run_id().unwrap(), row.depth))
                .collect::<Vec<_>>(),
            [(root, 4), (child, 5), (grandchild, 6)]
        );
        assert!(matches!(
            &run_rows[0].key,
            NodeKey::Run {
                pane_id: Some(pane_id),
                ..
            } if pane_id == "pane-root"
        ));
        assert!(
            run_rows[1..]
                .iter()
                .all(|row| matches!(row.key, NodeKey::Run { pane_id: None, .. }))
        );
    }

    #[test]
    fn hidden_parent_remains_structural_for_visible_grandchild() {
        let root = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let hidden_middle = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let visible_grandchild = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let mut model = placement_model(&["pane-root"]);
        insert_placement_run(&mut model, root, "root", 1);
        model.insert_task_run(TaskRun {
            run_id: hidden_middle,
            key: RunKey::Controller("hidden-middle".to_owned()),
            display_ordinal: DisplayOrdinal::new(2),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: Some(10),
        });
        insert_placement_run(&mut model, visible_grandchild, "visible-grandchild", 3);
        insert_placement_execution(
            &mut model,
            root,
            "root-execution",
            "pane-root",
            ExecState::Working,
        );
        for (parent_run_id, child_run_id) in
            [(root, hidden_middle), (hidden_middle, visible_grandchild)]
        {
            model.insert_execution_edge(ExecutionEdge {
                parent_run_id,
                child_run_id,
            });
        }

        let rows = build_uncollapsed_rows(&model, &AppState::default());
        let middle_row = only_run_row(&rows, hidden_middle);
        let grandchild_row = only_run_row(&rows, visible_grandchild);
        assert_eq!(middle_row.depth, 5);
        assert_eq!(grandchild_row.depth, 6);
        assert_eq!(
            grandchild_row.key,
            NodeKey::Run {
                run_id: visible_grandchild,
                pane_id: None,
            }
        );
        assert!(!grandchild_row.label.contains("[dispatched by:"));
        let middle_index = rows.iter().position(|row| row == middle_row).unwrap();
        let grandchild_index = rows.iter().position(|row| row == grandchild_row).unwrap();
        assert!(middle_index < grandchild_index);
    }

    #[test]
    fn cycle_bails_to_unattached() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let second = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let mut model = placement_model(&[]);
        insert_placement_run(&mut model, first, "first", 2);
        insert_placement_run(&mut model, second, "second", 1);
        for (parent_run_id, child_run_id) in [(first, second), (second, first)] {
            model.insert_execution_edge(ExecutionEdge {
                parent_run_id,
                child_run_id,
            });
        }

        let rows = build_uncollapsed_rows(&model, &AppState::default());
        let run_rows = rows
            .iter()
            .filter(|row| row.key.run_id().is_some())
            .collect::<Vec<_>>();

        assert_eq!(run_rows.len(), 2);
        assert_eq!(
            run_rows
                .iter()
                .map(|row| (row.key.run_id().unwrap(), row.depth))
                .collect::<Vec<_>>(),
            [(second, 2), (first, 2)]
        );
        assert!(run_rows.iter().all(|row| {
            matches!(row.key, NodeKey::Run { pane_id: None, .. })
                && !row.label.contains("[dispatched by:")
        }));
    }

    #[test]
    fn sibling_order_is_dispatch_order() {
        let parent = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let run_id_first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let dispatch_first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let mut model = placement_model(&["pane-parent"]);
        insert_placement_run(&mut model, parent, "parent", 1);
        insert_placement_run(&mut model, run_id_first, "run-id-first", 3);
        insert_placement_run(&mut model, dispatch_first, "dispatch-first", 2);
        insert_placement_execution(
            &mut model,
            parent,
            "parent-execution",
            "pane-parent",
            ExecState::Working,
        );
        for child_run_id in [run_id_first, dispatch_first] {
            model.insert_execution_edge(ExecutionEdge {
                parent_run_id: parent,
                child_run_id,
            });
        }

        let state = AppState::default();
        let newest_agents = newest_agent_nodes(&model, state.now_ms());
        let rows = build_tree_rows(&model, &state, &newest_agents);
        let run_rows = rows
            .iter()
            .filter_map(|row| row.key.run_id().map(|run_id| (run_id, row.depth)))
            .collect::<Vec<_>>();

        assert_eq!(
            run_rows,
            [(parent, 4), (dispatch_first, 5), (run_id_first, 5),]
        );
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

        let newest_agents = newest_agent_nodes(app.model(), app.state().now_ms());
        let rows = build_tree_rows(app.model(), app.state(), &newest_agents);
        let agent_rows = rows
            .iter()
            .filter(|row| matches!(row.key, NodeKey::Agent { .. }))
            .collect::<Vec<_>>();

        assert_eq!(agent_rows.len(), 3);
        assert!(agent_rows[0].label.contains("investigate"));
        assert_eq!(agent_rows[0].depth + 1, agent_rows[1].depth);
        assert!(agent_rows[1].label.contains("child"));
        assert!(
            agent_rows[1]
                .label
                .starts_with("● working Codex native agent:")
        );
        assert!(!agent_rows[1].label.contains("[state:"));
        assert!(agent_rows[1].label.contains("model:gpt-child"));
        assert!(agent_rows[1].label.contains("last:99ms"));
        assert_eq!(agent_rows[0].depth, agent_rows[2].depth);
        assert!(agent_rows[2].label.contains("orphan"));
    }

    #[test]
    fn display_stale_unknown_agent_is_absent_from_tree_rows() {
        let mut model = populated_model();
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        model.insert_agent_node(visibility_agent(
            "stale-unknown",
            run_id,
            9,
            Some(ExecState::Unknown),
            Some(-crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS),
            Some("provider-root"),
        ));

        let rows = build_rows(&model, &AppState::default());

        assert!(
            !has_agent_row(&rows, "stale-unknown"),
            "an unknown agent at the inactivity boundary must be hidden"
        );
    }

    #[test]
    fn recent_unknown_agent_remains_in_tree_rows() {
        let mut model = populated_model();
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        model.insert_agent_node(visibility_agent(
            "recent-unknown",
            run_id,
            9,
            Some(ExecState::Unknown),
            Some(1_i64.saturating_sub(crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS)),
            Some("provider-root"),
        ));

        let rows = build_rows(&model, &AppState::default());

        assert!(has_agent_row(&rows, "recent-unknown"));
    }

    #[test]
    fn old_working_agent_remains_in_tree_rows() {
        let mut model = populated_model();
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        model.insert_agent_node(visibility_agent(
            "old-working",
            run_id,
            9,
            Some(ExecState::Working),
            Some(-crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS),
            Some("provider-root"),
        ));

        let rows = build_rows(&model, &AppState::default());

        assert!(has_agent_row(&rows, "old-working"));
    }

    #[test]
    fn display_stale_ended_agent_is_absent_from_tree_rows() {
        let mut model = populated_model();
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        model.insert_agent_node(visibility_agent(
            "stale-ended",
            run_id,
            9,
            Some(ExecState::Ended),
            Some(-crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS),
            Some("provider-root"),
        ));

        let rows = build_rows(&model, &AppState::default());

        assert!(
            !has_agent_row(&rows, "stale-ended"),
            "an ended agent at the inactivity boundary must be hidden"
        );
    }

    #[test]
    fn unknown_without_activity_and_old_stale_agent_remain_in_tree_rows() {
        let mut model = populated_model();
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        model.insert_agent_node(visibility_agent(
            "unknown-without-activity",
            run_id,
            9,
            Some(ExecState::Unknown),
            None,
            Some("provider-root"),
        ));
        model.insert_agent_node(visibility_agent(
            "known-stale",
            run_id,
            10,
            Some(ExecState::Stale { since_ms: 0 }),
            Some(-crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS),
            Some("provider-root"),
        ));

        let rows = build_rows(&model, &AppState::default());

        assert!(has_agent_row(&rows, "unknown-without-activity"));
        assert!(has_agent_row(&rows, "known-stale"));
    }

    #[test]
    fn live_child_of_display_stale_parent_is_reparented_to_the_run() {
        let mut model = populated_model();
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        model.insert_agent_node(visibility_agent(
            "stale-parent",
            run_id,
            9,
            Some(ExecState::Unknown),
            Some(-crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS),
            None,
        ));
        model.insert_agent_node(visibility_agent(
            "live-child",
            run_id,
            10,
            Some(ExecState::Working),
            Some(-crate::provider::lane::DEFAULT_HEADLESS_INACTIVITY_MS),
            Some("stale-parent"),
        ));

        let rows = build_rows(&model, &AppState::default());
        let run_depth = rows
            .iter()
            .find(|row| row.key.run_id() == Some(run_id))
            .expect("owning run renders")
            .depth;
        let child_depth = rows
            .iter()
            .find(|row| {
                matches!(
                    &row.key,
                    NodeKey::Agent { agent_node_id, .. } if agent_node_id == "live-child"
                )
            })
            .expect("live child renders")
            .depth;

        assert!(!has_agent_row(&rows, "stale-parent"));
        assert_eq!(child_depth, run_depth + 1);
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
            label: None,
        });
        for pane_id in ["pane-1", "pane-2"] {
            model.insert_pane(Pane {
                pane_id: pane_id.to_owned(),
                workspace_id: "workspace".to_owned(),
                tab_id: "tab".to_owned(),
                terminal_id: format!("terminal-{pane_id}"),
                display_name: None,
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
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
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
        let newest_agents = newest_agent_nodes(app.model(), app.state().now_ms());
        let rows = build_tree_rows(app.model(), app.state(), &newest_agents);
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
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            });
        }
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: prerequisite,
            dependent_run_id: dependent,
        });
        let mut app = app(model, ObservationQuality::Live, "dag-session");
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        let rows = render(&app, 120, 18);
        let columns = |row: &str| {
            let parts = row.split('│').collect::<Vec<_>>();
            assert_eq!(
                parts.len(),
                5,
                "expected bordered three-column row: {row:?}"
            );
            [
                parts[1].trim().to_owned(),
                parts[2].trim().to_owned(),
                parts[3].trim().to_owned(),
            ]
        };
        let prerequisite_row = rows
            .iter()
            .find(|row| {
                row.split('│')
                    .nth(1)
                    .is_some_and(|run| run.contains("● working 前提🙂"))
            })
            .unwrap();
        let dependent_row = rows
            .iter()
            .find(|row| {
                row.split('│')
                    .nth(1)
                    .is_some_and(|run| run.contains("● working 依存先🙂"))
            })
            .unwrap();
        let prerequisite_columns = columns(prerequisite_row);
        let dependent_columns = columns(dependent_row);

        assert!(prerequisite_columns[1].is_empty());
        assert_eq!(prerequisite_columns[2], "依存先🙂with-a-long-tail");
        assert_eq!(dependent_columns[1], "前提🙂");
        assert!(dependent_columns[2].is_empty());
        assert!(!prerequisite_columns[1].contains("依存先"));
        assert!(!dependent_columns[2].contains("前提"));

        let initial_activity = rows.iter().find(|row| row.contains("Selected:")).unwrap();
        assert!(initial_activity.contains("Selected: ● working 依存先🙂with-a-long-tail"));
        assert!(!initial_activity.contains("Selected: ● working 前提🙂"));

        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Run {
                run_id: prerequisite,
                pane_id: None,
            })
        );
        let moved_rows = render(&app, 120, 18);
        let moved_activity = moved_rows
            .iter()
            .find(|row| row.contains("Selected:"))
            .unwrap();
        assert!(moved_activity.contains("Selected: ● working 前提🙂"));
        assert!(!moved_activity.contains("Selected: ● working 依存先"));

        let minimum_rows = render(&app, 48, 18);
        let screen = minimum_rows.join("\n");

        assert!(screen.contains("Dependency DAG"));
        assert!(screen.contains("Task Run"));
        assert!(screen.contains("Prereqs"));
        assert!(screen.contains("Dependents"));
        assert!(screen.contains("前提"));
        assert!(screen.contains("Selected: ● working 前提"));
        for row in &minimum_rows {
            assert!(
                Line::raw(row.as_str()).width() <= 48,
                "overflowing row: {row:?}"
            );
            assert!(!row.contains('\u{fffd}'));
        }
    }

    #[test]
    fn thousand_edge_dag_follow_and_non_follow_windows_are_exact() {
        let mut model = DomainModel::default();
        let ids = (0..=1_000).map(|_| RunId::new()).collect::<Vec<_>>();
        for (index, run_id) in ids.iter().copied().enumerate() {
            model.insert_task_run(TaskRun {
                run_id,
                key: RunKey::Controller(format!("run-{index:04}")),
                display_ordinal: DisplayOrdinal::new(index as i64),
                state: TaskState::Running,
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
        let mut app = app(model, ObservationQuality::Live, "large-dag");
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        let rows = render(&app, 100, 16);
        let visible_run_names = |rows: &[String]| {
            rows[3..9]
                .iter()
                .filter_map(|row| {
                    row.split_whitespace()
                        .find(|field| field.starts_with("run-"))
                })
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };

        assert_eq!(
            visible_run_names(&rows),
            ["run-0998", "run-0999", "run-1000"]
        );

        for _ in 0..500 {
            app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        }
        assert!(!app.is_following());
        assert_eq!(app.selected_run_id(), Some(ids[500]));
        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Run {
                run_id: ids[500],
                pane_id: None,
            })
        );

        let rows = render(&app, 100, 16);
        let visible = visible_run_names(&rows);
        assert_eq!(visible, ["run-0498", "run-0499", "run-0500"]);
        assert!(!visible.iter().any(|name| name == "run-0497"));
        assert!(!visible.iter().any(|name| name == "run-0501"));
    }

    #[test]
    fn i4_activity_identity_and_native_path_never_render() {
        const IDENTITY_SENTINEL: &str = "ACTIVITY_IDENTITY_FORBIDDEN_I4_A1_VIEW";
        let model = populated_model();
        let run_id = model.task_runs().next().unwrap().run_id;
        let activity = ActivityItem {
            identity: ActivityIdentity {
                event_id: IDENTITY_SENTINEL.to_owned(),
            },
            event_timestamp_ms: 10,
            seen_at_ms: 10,
            ingest_seq: Some(10),
            source: "provider".to_owned(),
            normalized_kind: "agent_activity".to_owned(),
            source_event_type: "item".to_owned(),
            workspace_id: Some("api".to_owned()),
            tab_id: Some("implementation".to_owned()),
            pane_id: Some("w1:p1".to_owned()),
            terminal_id: Some("terminal-1".to_owned()),
            provider: Some(Provider::Codex),
            native_session_id: Some("investigate".to_owned()),
            task_run_id: Some(run_id),
            agent_node_id: Some("agent-1".to_owned()),
            task_state: Some(TaskState::Running),
            model_id: Some("gpt-test".to_owned()),
            provider_event_kind: Some("assistant".to_owned()),
            tool_name: Some("Read".to_owned()),
            item_count: Some(1),
            byte_count: Some(2),
            provider_agent_id: Some("agent-1".to_owned()),
            provider_parent_agent_id: None,
            controller_label: None,
            controller_reason: None,
            durability: ActivityDurability::Durable,
        };
        let (_model_sender, model_receiver) = watch::channel(Arc::new(model));
        let (_diagnostics_sender, diagnostics_receiver) =
            watch::channel(RuntimeDiagnosticsSnapshot {
                persistence: PersistenceStatus::Healthy,
                persistence_detail: None,
                controller_input: ControllerInputStatus::Available,
                owner: OwnerFreshness::Current,
                persistence_counters: PersistenceCounters::default(),
                controller_counters: ControllerCounterSnapshot::default(),
                enrichment_counters: crate::diagnostics::EnrichmentCounterSnapshot::default(),
                provider_counters: crate::diagnostics::ProviderCounterSnapshot::default(),
                source_coverage: Vec::new(),
                dangling_announcement_components: 0,
                first_failure_log: OccurrenceLogStatus::NotAttempted,
            });
        let (_operator_sender, operator_receiver) = watch::channel(OperatorSnapshot {
            activity: Arc::from(vec![activity]),
            terminal_times: Arc::new(HashMap::new()),
        });
        let mut app = App::with_inputs(
            model_receiver,
            HeaderInputs {
                session: "identity-private".to_owned(),
                ..HeaderInputs::default()
            },
            diagnostics_receiver,
            operator_receiver,
            TuiSetup::default(),
            Arc::new(SystemClock),
        );

        let lower = render(&app, 220, 18).join("\n");
        assert!(!lower.contains(IDENTITY_SENTINEL));
        app.handle_key(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE));
        let detail = render(&app, 220, 18).join("\n");
        assert!(!detail.contains(IDENTITY_SENTINEL));
        app.handle_key(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE));
        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        for character in IDENTITY_SENTINEL.chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE));
        }
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(build_rows(app.model(), app.state()).is_empty());
    }

    #[test]
    fn i4_minimum_48x14_renders_tree_dag_and_runtime_strip() {
        let mut app = app(populated_model(), ObservationQuality::Live, "min-geometry");
        for mode in [ViewMode::ExecutionTree, ViewMode::DependencyDag] {
            if app.state().view_mode() != mode {
                app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
            }
            let rows = render(&app, 48, 14);
            let screen = rows.join("\n");
            assert!(screen.contains("session:min-geometry"));
            assert!(screen.contains("LIVE"));
            assert!(screen.contains("p:healthy"));
            assert!(screen.contains("ctl:unavailable"));
            assert!(screen.contains("D4:0"));
            match mode {
                ViewMode::ExecutionTree => {
                    assert!(screen.contains("native agent") || screen.contains("[running]"));
                    assert!(
                        rows[4..=6]
                            .iter()
                            .any(|row| row.contains("Codex controller")),
                        "the 48x14 tree body must contain its known Task Run row"
                    );
                }
                ViewMode::DependencyDag => {
                    assert!(
                        rows[4].contains("no dependency edges recorded"),
                        "the 48x14 DAG data coordinate must contain the zero-edge placeholder"
                    );
                    assert!(!screen.contains("Task Run"));
                }
            }
            for row in rows {
                assert!(Line::raw(row.as_str()).width() <= 48, "{row:?}");
            }
        }

        let too_short = render(&app, 48, 13).join("\n");
        assert!(too_short.contains("Terminal too small (minimum 48x14)"));
        assert!(!too_short.contains("Dependency DAG"));
    }

    #[test]
    fn i4_grapheme_combining_cjk_flag_skin_tone_zwj_at_48() {
        let vectors = [
            ("e\u{301}xy", 2, "e\u{301}…"),
            ("界xy", 3, "界…"),
            ("🇯🇵xy", 3, "🇯🇵…"),
            ("👍🏽xy", 3, "👍🏽…"),
            ("👩‍💻xy", 3, "👩‍💻…"),
        ];
        for (input, width, expected) in vectors {
            let truncated = truncate_to_width(input, width);
            assert_eq!(truncated, expected, "grapheme vector {input:?}");
            assert!(Line::raw(truncated.as_str()).width() <= width);
            assert!(!truncated.contains('\u{fffd}'));
        }
        assert_eq!(truncate_to_width("界x", 1), "…");
        assert_eq!(truncate_to_width("界x", 0), "");
        assert_eq!(truncate_to_width("control\ntext", 48), "control\\ntext");

        let app = app(
            populated_model(),
            ObservationQuality::Live,
            "e\u{301} 界 🇯🇵 👍🏽 👩‍💻 control\ntext",
        );
        let rows = render(&app, 48, 18);
        for rendered in rows {
            assert!(Line::raw(rendered.as_str()).width() <= 48, "{rendered:?}");
            assert!(!rendered.contains('\u{fffd}'));
            assert!(!rendered.contains('\n'));
        }
    }
}
