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
use crate::model::{
    AgentNode, DisplayOrdinal, DomainModel, ExecState, Provider, RunId, RunKey, TaskRun, TaskState,
};
use crate::performance::PerformanceDegradationReason;
use crate::store::writer::{
    DurabilityDisposition, PersistenceFailureCode, PersistenceOperation, PersistencePhase,
    PersistenceStatus,
};

use super::app::{AppState, HeaderInputs, NodeKey, Overlay, TuiSetup, ViewMode};
use super::dag;
use super::projection::{self, RowProjection};

const MIN_WIDTH: u16 = 48;
const MIN_HEIGHT: u16 = 14;
type RunPlacement = (RunId, bool);
type PaneRuns = HashMap<String, Vec<RunPlacement>>;
pub(crate) type NewestAgentNodes<'a> = HashMap<RunId, &'a AgentNode>;

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
    pub(crate) prerequisites: Vec<String>,
    pub(crate) dependents: Vec<String>,
}

/// Draws a complete fixed-screen frame from immutable model and UI snapshots.
pub(super) fn render(
    frame: &mut Frame<'_>,
    model: &DomainModel,
    performance: &PerformancePublication,
    header: &HeaderInputs,
    state: &AppState,
    diagnostics: &RuntimeDiagnosticsSnapshot,
    setup: &TuiSetup,
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
    );
    let projection = build_projection(model, state);
    let rows = projection.rows;
    match state.view_mode() {
        ViewMode::ExecutionTree => render_tree(frame, tree_area, &rows, state),
        ViewMode::DependencyDag => render_dag(frame, tree_area, &rows, state),
    }
    render_activity(frame, activity_area, model, &rows, state, diagnostics);
    frame.render_widget(
        Paragraph::new(footer_line(usize::from(footer_area.width))),
        footer_area,
    );
    render_interaction_layer(frame, area, model, state, diagnostics, setup);
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
        state: &AppState,
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
        super::render(frame, model, &projected, header, state, diagnostics, setup);
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
) {
    let block = Block::default().borders(Borders::ALL).title(" Herdr Top ");
    let inner_width = usize::from(area.width.saturating_sub(2));
    let line = header_line(area.width, inner_width, model, quality, performance, inputs);
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

fn footer_line(width: usize) -> String {
    let full = "q: stop Top only; agents continue | detach: Top runs | ↑↓ select | f/End follow | tab view | / filter | ? help";
    let compact = "q:stop Top; agents continue | detach:Top runs";
    if width >= 70 {
        truncate_to_width(full, width)
    } else {
        truncate_to_width(compact, width)
    }
}

fn render_interaction_layer(
    frame: &mut Frame<'_>,
    area: Rect,
    model: &DomainModel,
    state: &AppState,
    diagnostics: &RuntimeDiagnosticsSnapshot,
    setup: &TuiSetup,
) {
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
        Overlay::Detail => {
            let displayed_rows = build_rows(model, state);
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
                        &displayed_rows,
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
        "Filter: literal Unicode lowercase substring; interior whitespace is literal".to_owned(),
        "Filter excludes paths, activity, Controller free text, content, and raw events".to_owned(),
        "Tree: Left collapse/parent; Right expand/child; Enter toggles branch".to_owned(),
        "Collapse is ignored while filtering and in DAG; stored state survives view toggles"
            .to_owned(),
        "i detail; ? help; Esc/opening key closes; Up/Down scrolls overlays".to_owned(),
        "Follow pins selection and viewport to newest; manual navigation disables it".to_owned(),
        "Recovery: ancestor, stable neighbor, first; reasons are typed".to_owned(),
        "Controller input is optional capability; standalone setup is optional".to_owned(),
    ];
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
        "controller counters: binding_conflicts={} terminal_blocked_progress_noops={} terminal_forward_reference_creations={}",
        controller.binding_conflicts,
        controller.terminal_blocked_progress_noops,
        controller.terminal_forward_reference_creations,
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
            value: format!("{}ms", performance.snapshot.event_lag.as_millis()),
            shrinkable: true,
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
    let priorities = [
        "sources:",
        "lag:",
        "workspaces:",
        "host:",
        "session:",
        "perf:",
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
            return;
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

pub(crate) fn build_tree_rows(
    model: &DomainModel,
    state: &AppState,
    newest_agents: &NewestAgentNodes<'_>,
) -> Vec<TreeRow> {
    let mut rows = vec![TreeRow {
        key: NodeKey::Session,
        depth: 0,
        label: format!("Session: {}", state.session_display_name()),
        prerequisites: Vec::new(),
        dependents: Vec::new(),
    }];
    append_execution_tree_rows(&mut rows, model, state, newest_agents);
    rows
}

pub(crate) fn build_rows(model: &DomainModel, state: &AppState) -> Vec<TreeRow> {
    build_projection(model, state).rows
}

pub(crate) fn build_projection(model: &DomainModel, state: &AppState) -> RowProjection {
    #[cfg(test)]
    state.record_projection_build();
    let full_rows = build_full_rows(model, state);
    projection::project_rows(
        model,
        &full_rows,
        &state.operator_snapshot(),
        state.filter_query(),
        state.collapsed(),
        state.view_mode(),
        state.now_ms(),
    )
}

pub(crate) fn build_uncollapsed_rows(model: &DomainModel, state: &AppState) -> Vec<TreeRow> {
    #[cfg(test)]
    state.record_projection_build();
    let full_rows = build_full_rows(model, state);
    projection::project_rows(
        model,
        &full_rows,
        &state.operator_snapshot(),
        state.filter_query(),
        &HashSet::new(),
        state.view_mode(),
        state.now_ms(),
    )
    .rows
}

fn build_full_rows(model: &DomainModel, state: &AppState) -> Vec<TreeRow> {
    let newest_agents = newest_agent_nodes(model);
    match state.view_mode() {
        ViewMode::ExecutionTree => build_tree_rows(model, state, &newest_agents),
        ViewMode::DependencyDag => {
            dag::build_rows(model, state.dag_order(), state.now_ms(), &newest_agents)
        }
    }
}

fn append_execution_tree_rows(
    rows: &mut Vec<TreeRow>,
    model: &DomainModel,
    state: &AppState,
    newest_agents: &NewestAgentNodes<'_>,
) {
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
                    append_run_rows(
                        rows,
                        model,
                        runs,
                        Some(&pane.pane_id),
                        4,
                        state.now_ms(),
                        newest_agents,
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
            prerequisites: Vec::new(),
            dependents: Vec::new(),
        });
        let runs = unattached
            .into_iter()
            .map(|run_id| (run_id, false))
            .collect();
        append_run_rows(rows, model, runs, None, 2, state.now_ms(), newest_agents);
    }
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
    now_ms: i64,
    newest_agents: &NewestAgentNodes<'_>,
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
            label: task_run_label(
                model,
                run,
                shared,
                now_ms,
                newest_agents.get(&run_id).copied(),
            ),
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

pub(crate) fn task_run_label(
    model: &DomainModel,
    run: &TaskRun,
    shared: bool,
    now_ms: i64,
    newest_agent: Option<&AgentNode>,
) -> String {
    let mut label = run_row_label_with_agent(run, newest_agent, now_ms);
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

fn worker_kind_label(run: &TaskRun) -> String {
    match &run.key {
        RunKey::Native { provider, .. } | RunKey::NativePath { provider, .. } => {
            provider_label(*provider).to_owned()
        }
        RunKey::Controller(name) => {
            let label = name
                .strip_prefix("hook:")
                .and_then(|suffix| suffix.split_once(':').map(|(selector, _)| selector))
                .unwrap_or(name);
            safe_text(label)
        }
        RunKey::Provisional { .. } => "provisional".to_owned(),
    }
}

pub(crate) fn newest_agent_nodes(model: &DomainModel) -> NewestAgentNodes<'_> {
    #[cfg(test)]
    record_newest_agent_scan();
    let mut newest = NewestAgentNodes::new();
    for agent in model.agent_nodes() {
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

#[cfg(test)]
fn newest_agent_node(model: &DomainModel, run_id: RunId) -> Option<&AgentNode> {
    newest_agent_nodes(model).get(&run_id).copied()
}

#[cfg(test)]
fn run_row_label(model: &DomainModel, run: &TaskRun, now_ms: i64) -> String {
    let newest_agent = newest_agent_node(model, run.run_id);
    run_row_label_with_agent(run, newest_agent, now_ms)
}

fn run_row_label_with_agent(
    run: &TaskRun,
    newest_agent: Option<&AgentNode>,
    now_ms: i64,
) -> String {
    let mut label = worker_kind_label(run);
    let subject = run
        .subject
        .as_deref()
        .map_or_else(|| run_subject_fallback(run), safe_text);
    if !subject.is_empty() {
        label.push(' ');
        label.push_str(&subject);
    }

    if !run.state.is_terminal()
        && let Some(event_kind) = newest_agent.and_then(|agent| agent.last_event_kind.as_deref())
    {
        label.push_str(" — ");
        label.push_str(&safe_text(event_kind));
        if let Some(tool_name) = newest_agent.and_then(|agent| agent.last_tool_name.as_deref()) {
            label.push_str(": ");
            label.push_str(&safe_text(tool_name));
        }
    }
    if let Some(model_id) = newest_agent.and_then(|agent| agent.model_id.as_deref()) {
        label.push_str(" [model:");
        label.push_str(&safe_text(model_id));
        label.push(']');
    }
    label.push_str(&format!(" [{}]", task_state_label(run.state)));

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
    label
}

fn run_subject_fallback(run: &TaskRun) -> String {
    match &run.key {
        RunKey::Controller(_) => run_name(run),
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
    use ratatui::buffer::{Buffer, CellWidth};
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
        ExecutionEdge, Pane, Provider, RunId, RunKey, Tab, TaskRun, TaskState, Workspace,
    };
    use crate::performance::{PerformanceDegradationReason, PerformanceSnapshot};
    use crate::store::writer::PersistenceStatus;
    use crate::tui::app::{App, AppState, HeaderInputs, SystemClock, TuiSetup};

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
                "claude-code Implement I7 Task 2 wire tolerance — tool_use: Bash [model:gpt-5.6-sol] [running] · 17m03s",
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
                "claude-code hook:claude-code:S:task:T [queued]",
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
                "claude-code Finish work [model:gpt-terminal] [completed] · 1h01m",
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
                "claude-code No timing — message [running]",
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
                "claude-code Clock skew [running]",
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
            "claude-code Tie break — newer: Read [model:model-newer] [running]"
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
                    "claude-code Tie break — tie-high: Bash [model:model-high] [running]"
                );
            }
        }
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
            task_run_label(&model, &child, true, 10, None),
            "claude-code Shared child [running] [shared] [dispatched by: Parent]"
        );
        assert_eq!(
            task_run_label(&model, &orphan, false, 10, None),
            "codex Orphan [queued] [unlinked]"
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
    fn key_fallback_subject_does_not_repeat_native_or_provisional_worker_kind() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        for (key, expected) in [
            (
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "native-session".to_owned(),
                },
                "Codex native-session [running]",
            ),
            (
                RunKey::NativePath {
                    provider: Provider::Codex,
                    path: "/private/session.jsonl".to_owned(),
                },
                "Codex 01ARZ3NDEKTSV4RRFFQ69G5FAV [running]",
            ),
            (
                RunKey::Provisional {
                    terminal_id: "terminal".to_owned(),
                    start_ms: 1,
                    seq: 2,
                },
                "provisional terminal:1:2 [running]",
            ),
            (
                RunKey::Controller("hook:claude-code:S:task:T".to_owned()),
                "claude-code hook:claude-code:S:task:T [running]",
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
            assert_eq!(worker_kind_label(&run), expected);
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
        let line = rendered_header(snapshot, 140);
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
            .find(|row| row.contains("Codex controller"))
            .unwrap()
            .find("Codex controller")
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

        let newest_agents = newest_agent_nodes(&model);
        let rows = build_tree_rows(&model, &AppState::default(), &newest_agents);
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
        let newest_agents = newest_agent_nodes(app.model());
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

        let newest_agents = newest_agent_nodes(app.model());
        let rows = build_tree_rows(app.model(), app.state(), &newest_agents);
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
        let newest_agents = newest_agent_nodes(app.model());
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
            .find(|row| row.contains("前提🙂 前提🙂"))
            .unwrap();
        let dependent_row = rows
            .iter()
            .find(|row| row.contains("依存先🙂with-a-long-tail 依存先🙂with-a-long-tail"))
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
        assert!(initial_activity.contains("Selected: 依存先🙂with-a-long-tail"));
        assert!(!initial_activity.contains("Selected: 前提🙂"));

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
        assert!(moved_activity.contains("Selected: 前提🙂"));
        assert!(!moved_activity.contains("Selected: 依存先"));

        let minimum_rows = render(&app, 48, 18);
        let screen = minimum_rows.join("\n");

        assert!(screen.contains("Dependency DAG"));
        assert!(screen.contains("Task Run"));
        assert!(screen.contains("Prereqs"));
        assert!(screen.contains("Dependents"));
        assert!(screen.contains("前提"));
        assert!(screen.contains("Selected: 前提"));
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
                controller_input: ControllerInputStatus::Available,
                owner: OwnerFreshness::Current,
                persistence_counters: PersistenceCounters::default(),
                controller_counters: ControllerCounterSnapshot::default(),
                enrichment_counters: crate::diagnostics::EnrichmentCounterSnapshot::default(),
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
                    assert!(rows[4].contains("Task Run"));
                    assert!(
                        rows[5].contains("Codex controller"),
                        "the 48x14 DAG data coordinate must contain its known Task Run"
                    );
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
