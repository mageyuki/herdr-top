//! T10 fixed-screen TUI application loop and key handling.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::{Frame, Terminal};

use crate::herdr::collector::{ObservationQuality, SourceCoverageRegistry};
use crate::model::{DomainModel, RunId, RunKey, SharedModel};

use super::dag::DagOrder;
use super::view::{self, TreeRow};

const FRAME_INTERVAL: Duration = Duration::from_millis(100);
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Static header values plus the collector-published dynamic source coverage.
#[derive(Clone, Debug)]
pub struct HeaderInputs {
    /// Host that owns the monitored Herdr session.
    pub host: String,
    /// Human-facing Herdr named session.
    pub session: String,
    /// Age of the oldest received event that has not yet been applied.
    pub event_lag: Duration,
    /// Honest summary of currently available sources.
    pub source_coverage: tokio::sync::watch::Receiver<SourceCoverageRegistry>,
}

impl Default for HeaderInputs {
    fn default() -> Self {
        let (_coverage_sender, source_coverage) =
            tokio::sync::watch::channel(SourceCoverageRegistry::default());
        Self {
            host: "unknown".to_owned(),
            session: "unknown".to_owned(),
            event_lag: Duration::ZERO,
            source_coverage,
        }
    }
}

/// Result of applying one input event to the monitor UI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoopControl {
    /// Keep the monitor loop running.
    Continue,
    /// Exit only the monitor loop; no producer is stopped.
    Exit,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ViewMode {
    #[default]
    ExecutionTree,
    DependencyDag,
}

impl ViewMode {
    const fn toggled(self) -> Self {
        match self {
            Self::ExecutionTree => Self::DependencyDag,
            Self::DependencyDag => Self::ExecutionTree,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum NodeKey {
    Session,
    Workspace(String),
    Tab(String),
    Pane(String),
    Run {
        run_id: RunId,
        pane_id: Option<String>,
    },
    Agent {
        agent_node_id: String,
        pane_id: Option<String>,
    },
    UnattachedGroup,
}

impl NodeKey {
    fn run_id(&self) -> Option<RunId> {
        match self {
            Self::Run { run_id, .. } => Some(*run_id),
            Self::Session
            | Self::Workspace(_)
            | Self::Tab(_)
            | Self::Pane(_)
            | Self::Agent { .. }
            | Self::UnattachedGroup => None,
        }
    }
}

#[derive(Debug, Default)]
struct StableOrder {
    by_id: HashMap<String, u64>,
    next: u64,
}

impl StableOrder {
    fn adopt(&mut self, ids: impl IntoIterator<Item = String>) {
        let mut unseen = ids
            .into_iter()
            .filter(|id| !self.by_id.contains_key(id))
            .collect::<Vec<_>>();
        unseen.sort();
        unseen.dedup();
        for id in unseen {
            self.by_id.insert(id, self.next);
            self.next = self.next.saturating_add(1);
        }
    }

    fn get(&self, id: &str) -> u64 {
        self.by_id.get(id).copied().unwrap_or(u64::MAX)
    }
}

/// Mutable, presentation-only state owned by [`App`].
#[derive(Debug, Default)]
pub(crate) struct AppState {
    selected: Option<NodeKey>,
    selected_run_key: Option<RunKey>,
    follow: bool,
    selection_reason: Option<String>,
    execution_order: StableOrder,
    view_mode: ViewMode,
    dag_order: DagOrder,
}

impl AppState {
    fn adopt_model(&mut self, model: &DomainModel) {
        // Topology rows and semantic rows use persisted model ordinals. Executions are not rows;
        // their in-session adoption order chooses a run's most recent known hosting pane.
        self.execution_order
            .adopt(model.executions().map(|item| item.execution_id.clone()));
        self.dag_order.recompute(model);
    }

    pub(super) fn execution_order(&self, id: &str) -> u64 {
        self.execution_order.get(id)
    }

    pub(super) fn selected(&self) -> Option<&NodeKey> {
        self.selected.as_ref()
    }

    pub(super) fn is_following(&self) -> bool {
        self.follow
    }

    pub(super) fn selection_reason(&self) -> Option<&str> {
        self.selection_reason.as_deref()
    }

    pub(super) const fn view_mode(&self) -> ViewMode {
        self.view_mode
    }

    pub(super) const fn dag_order(&self) -> &DagOrder {
        &self.dag_order
    }
}

/// Fixed-screen monitor state backed only by model and quality watch receivers.
pub struct App {
    model_receiver: SharedModel,
    quality_receiver: tokio::sync::watch::Receiver<ObservationQuality>,
    model: Arc<DomainModel>,
    quality: ObservationQuality,
    header: HeaderInputs,
    state: AppState,
}

impl App {
    /// Creates an application from observation receivers and display-only header inputs.
    #[must_use]
    pub fn new(
        model_receiver: SharedModel,
        quality_receiver: tokio::sync::watch::Receiver<ObservationQuality>,
        header: HeaderInputs,
    ) -> Self {
        let model = Arc::clone(&model_receiver.borrow());
        let quality = *quality_receiver.borrow();
        let mut app = Self {
            model_receiver,
            quality_receiver,
            model,
            quality,
            header,
            state: AppState {
                follow: true,
                ..AppState::default()
            },
        };
        app.state.adopt_model(app.model.as_ref());
        let last = view::build_rows(app.model.as_ref(), &app.state)
            .last()
            .map(|row| row.key.clone());
        app.set_selection(last);
        app
    }

    /// Refreshes the cached coherent model and independently published quality.
    pub fn refresh(&mut self) {
        let old_rows = view::build_rows(self.model.as_ref(), &self.state);
        let new_model = Arc::clone(&self.model_receiver.borrow_and_update());
        self.quality = *self.quality_receiver.borrow_and_update();
        self.header.source_coverage.borrow_and_update();
        self.state.adopt_model(new_model.as_ref());
        self.model = new_model;
        let new_rows = view::build_rows(self.model.as_ref(), &self.state);
        self.recover_selection(&old_rows, &new_rows);
    }

    fn refresh_if_changed(&mut self) -> io::Result<bool> {
        let model_changed = self.model_receiver.has_changed().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "model watch closed; collector is no longer publishing state",
            )
        })?;
        let quality_changed = self.quality_receiver.has_changed().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "quality watch closed; collector is no longer publishing diagnostics",
            )
        })?;
        let coverage_changed = self.header.source_coverage.has_changed().unwrap_or(false);
        if model_changed || quality_changed || coverage_changed {
            self.refresh();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Applies one keyboard event without touching the collector, writer, or monitored agents.
    pub fn handle_key(&mut self, key: KeyEvent) -> LoopControl {
        if key.kind == KeyEventKind::Release {
            return LoopControl::Continue;
        }
        match key.code {
            KeyCode::Char('q') => LoopControl::Exit,
            KeyCode::Up => {
                self.move_selection(false);
                LoopControl::Continue
            }
            KeyCode::Down => {
                self.move_selection(true);
                LoopControl::Continue
            }
            KeyCode::Char('f') | KeyCode::End => {
                self.resume_follow();
                LoopControl::Continue
            }
            KeyCode::Tab => {
                self.toggle_view();
                LoopControl::Continue
            }
            _ => LoopControl::Continue,
        }
    }

    /// Renders the current cached state into one frame.
    pub fn render(&self, frame: &mut Frame<'_>) {
        view::render(
            frame,
            self.model.as_ref(),
            self.quality,
            &self.header,
            &self.state,
        );
    }

    /// Runs the real-terminal event loop until `q` requests a monitor-only exit.
    pub fn run(&mut self) -> io::Result<()> {
        let _terminal_session = TerminalSession::enter()?;
        let backend = CrosstermBackend::new(io::stdout());
        let mut terminal = Terminal::new(backend)?;
        let started = Instant::now();
        let mut limiter = FrameLimiter::default();
        let mut dirty = true;
        loop {
            dirty |= self.refresh_if_changed()?;
            let now = started.elapsed();
            if limiter.ready(dirty, now) {
                terminal.draw(|frame| self.render(frame))?;
                limiter.record(now);
                dirty = false;
            }

            let poll_for = limiter.poll_duration(dirty, started.elapsed());
            if event::poll(poll_for)? {
                match event::read()? {
                    Event::Key(key) if self.handle_key(key) == LoopControl::Exit => return Ok(()),
                    Event::Key(_) | Event::Resize(_, _) => dirty = true,
                    _ => {}
                }
            }
        }
    }

    /// Returns the coherent model currently displayed by the application.
    #[must_use]
    pub fn model(&self) -> &DomainModel {
        self.model.as_ref()
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn state(&self) -> &AppState {
        &self.state
    }

    /// Returns whether the active viewport is pinned to its newest rows.
    #[must_use]
    pub const fn is_following(&self) -> bool {
        self.state.follow
    }

    /// Returns the selected Task Run when the selected row is a run occurrence.
    #[must_use]
    pub fn selected_run_id(&self) -> Option<RunId> {
        self.state.selected.as_ref().and_then(NodeKey::run_id)
    }

    fn move_selection(&mut self, down: bool) {
        self.state.follow = false;
        self.state.selection_reason = None;
        let rows = view::build_rows(self.model.as_ref(), &self.state);
        if rows.is_empty() {
            self.set_selection(None);
            return;
        }
        let current = self
            .state
            .selected
            .as_ref()
            .and_then(|selected| rows.iter().position(|row| &row.key == selected))
            .unwrap_or_else(|| rows.len().saturating_sub(1));
        let next = if down {
            current.saturating_add(1).min(rows.len().saturating_sub(1))
        } else {
            current.saturating_sub(1)
        };
        self.set_selection(rows.get(next).map(|row| row.key.clone()));
    }

    fn resume_follow(&mut self) {
        self.state.follow = true;
        self.state.selection_reason = None;
        let selected = view::build_rows(self.model.as_ref(), &self.state)
            .last()
            .map(|row| row.key.clone());
        self.set_selection(selected);
    }

    fn toggle_view(&mut self) {
        let previous = self.state.selected.clone();
        self.state.view_mode = self.state.view_mode.toggled();
        self.state.selection_reason = None;
        let rows = view::build_rows(self.model.as_ref(), &self.state);
        let replacement = previous.as_ref().and_then(|selected| {
            selected.run_id().and_then(|run_id| {
                if self.state.view_mode == ViewMode::ExecutionTree {
                    preferred_run_row(&rows, run_id, selected).map(|row| row.key.clone())
                } else {
                    rows.iter()
                        .find(|row| row.key.run_id() == Some(run_id))
                        .map(|row| row.key.clone())
                }
            })
        });
        if replacement.is_some() {
            self.set_selection(replacement);
        } else {
            self.resume_follow();
        }
    }

    fn set_selection(&mut self, selected: Option<NodeKey>) {
        self.state.selected_run_key = selected
            .as_ref()
            .and_then(NodeKey::run_id)
            .and_then(|run_id| self.model.task_run(&run_id))
            .map(|run| run.key.clone());
        self.state.selected = selected;
    }

    fn recover_selection(&mut self, old_rows: &[TreeRow], new_rows: &[TreeRow]) {
        let Some(selected) = self.state.selected.clone() else {
            self.set_selection(new_rows.last().map(|row| row.key.clone()));
            return;
        };
        if new_rows.iter().any(|row| row.key == selected) {
            return;
        }

        if let Some(old_run_id) = selected.run_id()
            && let Some(selected_key) = self.state.selected_run_key.as_ref()
            && let Some(survivor) = self.model.task_run_by_key(selected_key)
            && let Some(row) = preferred_run_row(new_rows, survivor.run_id, &selected)
        {
            let survivor_id = survivor.run_id;
            let survivor_label = row.label.clone();
            let replacement = row.key.clone();
            self.set_selection(Some(replacement));
            self.state.selection_reason = Some(if survivor_id == old_run_id {
                format!("Selection moved with Task Run: {survivor_label}")
            } else {
                format!("Selection merged into {survivor_label}")
            });
            return;
        }

        let replacement = surviving_neighbor(old_rows, new_rows, &selected)
            .or_else(|| new_rows.first().map(|row| row.key.clone()));
        let label = replacement
            .as_ref()
            .and_then(|key| new_rows.iter().find(|row| &row.key == key))
            .map(|row| row.label.clone())
            .unwrap_or_else(|| "no surviving row".to_owned());
        self.set_selection(replacement);
        self.state.selection_reason = Some(format!("Selection moved: closed; now {label}"));
    }
}

#[derive(Debug, Default)]
struct FrameLimiter {
    last_draw: Option<Duration>,
}

impl FrameLimiter {
    fn ready(&self, dirty: bool, now: Duration) -> bool {
        dirty
            && self
                .last_draw
                .is_none_or(|last| now.saturating_sub(last) >= FRAME_INTERVAL)
    }

    fn record(&mut self, now: Duration) {
        self.last_draw = Some(now);
    }

    fn poll_duration(&self, dirty: bool, now: Duration) -> Duration {
        if !dirty {
            return WATCH_POLL_INTERVAL;
        }
        self.last_draw.map_or(Duration::ZERO, |last| {
            FRAME_INTERVAL
                .saturating_sub(now.saturating_sub(last))
                .min(WATCH_POLL_INTERVAL)
        })
    }
}

fn preferred_run_row<'a>(
    rows: &'a [TreeRow],
    run_id: RunId,
    previous: &NodeKey,
) -> Option<&'a TreeRow> {
    let previous_pane = match previous {
        NodeKey::Run { pane_id, .. } => pane_id.as_ref(),
        _ => None,
    };
    rows.iter()
        .find(|row| {
            matches!(
                &row.key,
                NodeKey::Run {
                    run_id: candidate,
                    pane_id,
                } if *candidate == run_id && pane_id.as_ref() == previous_pane
            )
        })
        .or_else(|| {
            rows.iter().find(|row| {
                matches!(&row.key, NodeKey::Run { run_id: candidate, .. } if *candidate == run_id)
            })
        })
}

fn surviving_neighbor(
    old_rows: &[TreeRow],
    new_rows: &[TreeRow],
    selected: &NodeKey,
) -> Option<NodeKey> {
    let old_index = old_rows.iter().position(|row| &row.key == selected)?;
    let selected_depth = old_rows.get(old_index)?.depth;
    for same_depth_only in [true, false] {
        for distance in 1..old_rows.len() {
            let after = old_index.saturating_add(distance);
            let candidates = [
                old_rows.get(after),
                old_index
                    .checked_sub(distance)
                    .and_then(|i| old_rows.get(i)),
            ];
            for candidate in candidates.into_iter().flatten() {
                if (!same_depth_only || candidate.depth == selected_depth)
                    && new_rows.iter().any(|row| row.key == candidate.key)
                {
                    return Some(candidate.key.clone());
                }
            }
        }
    }
    None
}

struct TerminalSession;

impl TerminalSession {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        if let Err(error) = execute!(io::stdout(), EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(error);
        }
        Ok(Self)
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use tokio::sync::watch;

    use super::*;
    use crate::herdr::collector::ObservationQuality;
    use crate::herdr::collector::{CoverageSource, SourceAvailability, SourceCoverageRegistry};
    use crate::model::{
        DependencyEdge, DisplayOrdinal, DomainModel, ExecState, Execution, Pane, RunId, RunKey,
        Tab, TaskRun, TaskState, Workspace,
    };

    fn run_id(value: &str) -> RunId {
        RunId::parse(value).unwrap()
    }

    fn model_with_runs(runs: &[(RunId, &str, i64, TaskState)]) -> DomainModel {
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "workspace".to_owned(),
        });
        model.insert_tab(Tab {
            tab_id: "tab".to_owned(),
            workspace_id: "workspace".to_owned(),
        });
        model.insert_pane(Pane {
            pane_id: "pane".to_owned(),
            workspace_id: "workspace".to_owned(),
            tab_id: "tab".to_owned(),
            terminal_id: "terminal".to_owned(),
        });
        for (run_id, label, ordinal, state) in runs {
            model.insert_task_run(TaskRun {
                run_id: *run_id,
                key: RunKey::Controller((*label).to_owned()),
                display_ordinal: DisplayOrdinal::new(*ordinal),
                state: *state,
                has_controller_task_state_event: true,
            });
            model.insert_execution(Execution {
                execution_id: format!("execution-{label}"),
                pane_id: "pane".to_owned(),
                terminal_id: "terminal".to_owned(),
                task_run_id: *run_id,
                state: ExecState::Working,
            });
        }
        model
    }

    fn app_with_model(model: DomainModel) -> (App, watch::Sender<Arc<DomainModel>>) {
        let (model_sender, model_receiver) = watch::channel(Arc::new(model));
        let (_quality_sender, quality_receiver) = watch::channel(ObservationQuality::Live);
        let app = App::new(
            model_receiver,
            quality_receiver,
            HeaderInputs {
                host: "host".to_owned(),
                session: "session".to_owned(),
                event_lag: Duration::ZERO,
                ..HeaderInputs::default()
            },
        );
        (app, model_sender)
    }

    fn render(app: &App) -> String {
        render_at_width(app, 100)
    }

    fn render_at_width(app: &App, width: u16) -> String {
        let backend = TestBackend::new(width, 18);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| app.render(frame)).unwrap();
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    fn render_lines(app: &App, width: u16, height: u16) -> Vec<String> {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| app.render(frame)).unwrap();
        terminal
            .backend()
            .buffer()
            .content()
            .chunks(usize::from(width))
            .map(|cells| cells.iter().map(|cell| cell.symbol()).collect())
            .collect()
    }

    fn displayed_run_names(app: &App) -> Vec<String> {
        view::build_rows(app.model(), app.state())
            .into_iter()
            .filter_map(|row| row.key.run_id())
            .map(|run_id| match &app.model().task_run(&run_id).unwrap().key {
                RunKey::Controller(name) => name.clone(),
                _ => unreachable!("test creates controller runs"),
            })
            .collect()
    }

    #[test]
    fn tab_round_trip_preserves_selected_run_across_modes() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let last = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let (mut app, _model_sender) = app_with_model(model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (last, "last", 2, TaskState::Running),
        ]));
        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(app.selected_run_id(), Some(first));
        assert!(!app.is_following());

        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(app.state().view_mode(), ViewMode::DependencyDag);
        assert_eq!(app.selected_run_id(), Some(first));
        assert!(!app.is_following());
        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Run {
                run_id: first,
                pane_id: None,
            })
        );
        assert!(render(&app).contains("Selected: Task Run: first"));

        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(app.state().view_mode(), ViewMode::ExecutionTree);
        assert_eq!(app.selected_run_id(), Some(first));
        assert!(matches!(
            app.state().selected(),
            Some(NodeKey::Run {
                run_id,
                pane_id: Some(_),
            }) if *run_id == first
        ));
    }

    #[test]
    fn tree_to_dag_maps_a_shared_run_occurrence_to_one_run_row() {
        let shared = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = model_with_runs(&[(shared, "shared", 1, TaskState::Running)]);
        model.insert_pane(Pane {
            pane_id: "pane-2".to_owned(),
            workspace_id: "workspace".to_owned(),
            tab_id: "tab".to_owned(),
            terminal_id: "terminal-2".to_owned(),
        });
        model.insert_execution(Execution {
            execution_id: "execution-shared-2".to_owned(),
            pane_id: "pane-2".to_owned(),
            terminal_id: "terminal-2".to_owned(),
            task_run_id: shared,
            state: ExecState::Working,
        });
        let (mut app, _sender) = app_with_model(model);
        app.state.follow = false;
        app.set_selection(Some(NodeKey::Run {
            run_id: shared,
            pane_id: Some("pane-2".to_owned()),
        }));

        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));

        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Run {
                run_id: shared,
                pane_id: None,
            })
        );
        assert_eq!(
            view::build_rows(app.model(), app.state())
                .iter()
                .filter(|row| row.key.run_id() == Some(shared))
                .count(),
            1
        );
    }

    #[test]
    fn tab_from_non_run_selection_falls_back_to_follow() {
        let run = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let (mut app, _model_sender) =
            app_with_model(model_with_runs(&[(run, "run", 1, TaskState::Running)]));
        app.state.follow = false;
        app.set_selection(Some(NodeKey::Workspace("workspace".to_owned())));

        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));

        assert_eq!(app.state().view_mode(), ViewMode::DependencyDag);
        assert!(app.is_following());
        assert_eq!(app.selected_run_id(), Some(run));
    }

    #[test]
    fn dag_mode_refresh_consumes_one_coalesced_edge_batch() {
        let a = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let v = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let x = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let u = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAY");
        let initial = model_with_runs(&[
            (a, "A", 1, TaskState::Running),
            (v, "V", 2, TaskState::Running),
            (x, "X", 3, TaskState::Running),
            (u, "U", 4, TaskState::Running),
        ]);
        let mut refreshed = initial.clone();
        refreshed.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: u,
            dependent_run_id: v,
        });
        refreshed.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: x,
            dependent_run_id: a,
        });
        let (mut app, model_sender) = app_with_model(initial);
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(displayed_run_names(&app), ["A", "V", "X", "U"]);

        model_sender.send(Arc::new(refreshed)).unwrap();
        app.refresh();

        assert_eq!(displayed_run_names(&app), ["X", "A", "U", "V"]);
    }

    #[test]
    fn dag_refresh_keeps_neighbor_and_merge_selection_reasons() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let selected = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let next = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let (mut closed, sender) = app_with_model(model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (selected, "selected", 2, TaskState::Running),
            (next, "next", 3, TaskState::Running),
        ]));
        closed.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        closed.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        sender
            .send(Arc::new(model_with_runs(&[
                (first, "first", 1, TaskState::Running),
                (next, "next", 3, TaskState::Running),
            ])))
            .unwrap();
        closed.refresh();
        assert_eq!(closed.selected_run_id(), Some(next));
        assert!(
            closed
                .state()
                .selection_reason()
                .unwrap()
                .contains("closed")
        );

        let survivor = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAY");
        let absorbed_key = RunKey::Controller("absorbed".to_owned());
        let (mut merged, sender) = app_with_model(model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (selected, "absorbed", 2, TaskState::Running),
            (survivor, "survivor", 3, TaskState::Running),
        ]));
        merged.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        merged.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        let mut replacement = model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (survivor, "survivor", 3, TaskState::Running),
        ]);
        replacement.insert_task_run_alias(absorbed_key, survivor);
        sender.send(Arc::new(replacement)).unwrap();
        merged.refresh();
        assert_eq!(merged.selected_run_id(), Some(survivor));
        assert!(
            merged
                .state()
                .selection_reason()
                .unwrap()
                .contains("merged into")
        );
    }

    #[test]
    fn dag_selection_movement_and_resume_follow_use_dag_rows() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let middle = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let last = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let mut model = model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (middle, "middle", 2, TaskState::Running),
            (last, "last", 3, TaskState::Running),
        ]);
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: last,
            dependent_run_id: first,
        });
        model.insert_dependency_edge(DependencyEdge {
            prerequisite_run_id: first,
            dependent_run_id: middle,
        });
        let (mut app, _model_sender) = app_with_model(model);
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(displayed_run_names(&app), ["last", "first", "middle"]);
        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Run {
                run_id: last,
                pane_id: None,
            })
        );

        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Run {
                run_id: first,
                pane_id: None,
            })
        );
        assert!(!app.is_following());

        app.handle_key(KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE));
        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Run {
                run_id: middle,
                pane_id: None,
            })
        );
        assert!(app.is_following());
    }

    #[test]
    fn toggled_viewport_pins_bottom_only_while_following() {
        let ids = [
            run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
            run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW"),
            run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX"),
            run_id("01ARZ3NDEKTSV4RRFFQ69G5FAY"),
            run_id("01ARZ3NDEKTSV4RRFFQ69G5FAZ"),
            run_id("01ARZ3NDEKTSV4RRFFQ69G5FB0"),
            run_id("01ARZ3NDEKTSV4RRFFQ69G5FB1"),
            run_id("01ARZ3NDEKTSV4RRFFQ69G5FB2"),
        ];
        let runs = ids
            .iter()
            .enumerate()
            .map(|(index, run_id)| (*run_id, format!("run-{index}"), index as i64 + 1))
            .collect::<Vec<_>>();
        let input = runs
            .iter()
            .map(|(run_id, label, ordinal)| (*run_id, label.as_str(), *ordinal, TaskState::Running))
            .collect::<Vec<_>>();

        let (mut following, _sender) = app_with_model(model_with_runs(&input));
        following.set_selection(Some(NodeKey::Run {
            run_id: ids[0],
            pane_id: Some("pane".to_owned()),
        }));
        assert!(following.is_following());
        following.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        let following_rows = render_lines(&following, 100, 14);
        let following_view = following_rows[3..9].join("\n");
        assert!(following_view.contains("Dependency DAG"));
        assert!(!following_view.contains("> Task Run: run-0"));
        assert!(following_view.contains("Task Run: run-7"));

        let (mut manual, _sender) = app_with_model(model_with_runs(&input));
        manual.state.follow = false;
        manual.set_selection(Some(NodeKey::Run {
            run_id: ids[0],
            pane_id: Some("pane".to_owned()),
        }));
        manual.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        let manual_rows = render_lines(&manual, 100, 14);
        let manual_view = manual_rows[3..9].join("\n");
        assert!(manual_view.contains("> Task Run: run-0"));
    }

    #[test]
    fn source_coverage_refreshes_dynamically() {
        let (model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (_quality_sender, quality_receiver) = watch::channel(ObservationQuality::Live);
        let (coverage_sender, coverage_receiver) =
            watch::channel(SourceCoverageRegistry::new(SourceAvailability::Available));
        let mut app = App::new(
            model_receiver,
            quality_receiver,
            HeaderInputs {
                source_coverage: coverage_receiver,
                ..HeaderInputs::default()
            },
        );
        assert!(render_at_width(&app, 220).contains("codex=n/a"));

        let mut updated = coverage_sender.borrow().clone();
        updated.set(
            CoverageSource::Codex,
            SourceAvailability::Unavailable {
                detail: "read_failed".to_owned(),
            },
        );
        coverage_sender.send(updated).unwrap();
        assert!(app.refresh_if_changed().unwrap());

        assert!(render_at_width(&app, 220).contains("codex=unavailable(read_failed)"));
        drop(model_sender);
    }

    #[test]
    fn selection_recovers_to_neighbor() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let selected = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let next = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let (mut app, model_sender) = app_with_model(model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (selected, "selected", 2, TaskState::Running),
            (next, "next", 3, TaskState::Running),
        ]));

        assert_eq!(app.selected_run_id(), Some(next));
        assert_eq!(
            app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE)),
            LoopControl::Continue
        );
        assert_eq!(app.selected_run_id(), Some(selected));

        model_sender
            .send(Arc::new(model_with_runs(&[
                (first, "first", 1, TaskState::Running),
                (next, "next", 3, TaskState::Running),
            ])))
            .unwrap();
        app.refresh();

        assert_eq!(app.selected_run_id(), Some(next));
        let screen = render(&app);
        assert!(
            screen.contains("closed"),
            "screen did not show recovery: {screen}"
        );
        assert!(
            screen.contains("next"),
            "screen did not show neighbor: {screen}"
        );
    }

    #[test]
    fn selection_follows_identity_merge() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let absorbed = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let survivor = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let absorbed_key = RunKey::Controller("absorbed".to_owned());
        let (mut app, model_sender) = app_with_model(model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (absorbed, "absorbed", 2, TaskState::Running),
            (survivor, "survivor", 3, TaskState::Running),
        ]));
        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(app.selected_run_id(), Some(absorbed));

        let mut merged = model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (survivor, "survivor", 3, TaskState::Running),
        ]);
        merged.insert_task_run_alias(absorbed_key, survivor);
        model_sender.send(Arc::new(merged)).unwrap();
        app.refresh();

        assert_eq!(app.selected_run_id(), Some(survivor));
        assert!(render(&app).contains("merged into"));
    }

    #[test]
    fn manual_selection_disables_follow_and_f_or_end_resumes_it() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let last = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let (mut app, _model_sender) = app_with_model(model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (last, "last", 2, TaskState::Running),
        ]));

        assert!(app.is_following());
        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert!(!app.is_following());
        app.handle_key(KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE));
        assert!(app.is_following());
        assert_eq!(app.selected_run_id(), Some(last));
        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        app.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert!(app.is_following());
        assert_eq!(app.selected_run_id(), Some(last));
    }

    #[test]
    fn q_quits_monitor_only() {
        let run = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let model = model_with_runs(&[(run, "run", 1, TaskState::Running)]);
        let expected_run_count = model.task_runs().count();
        let (model_sender, model_receiver) = watch::channel(Arc::new(model));
        let (quality_sender, quality_receiver) = watch::channel(ObservationQuality::Live);
        let mut app = App::new(model_receiver, quality_receiver, HeaderInputs::default());

        let outcome = app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE));

        assert_eq!(outcome, LoopControl::Exit);
        assert_eq!(app.model().task_runs().count(), expected_run_count);
        assert!(model_sender.send(Arc::new(DomainModel::default())).is_ok());
        assert!(quality_sender.send(ObservationQuality::Degraded).is_ok());
        assert_eq!(app.model().task_runs().count(), expected_run_count);
    }

    #[test]
    fn tui_exits_on_closed_watch() {
        let (model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (_quality_sender, quality_receiver) = watch::channel(ObservationQuality::Live);
        let mut app = App::new(model_receiver, quality_receiver, HeaderInputs::default());
        drop(model_sender);

        let error = app
            .refresh_if_changed()
            .expect_err("closed model watch must terminate the TUI loop");

        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(error.to_string().contains("model watch closed"));
    }

    #[test]
    fn tui_redraw_is_dirty_driven_and_capped_at_ten_fps() {
        let mut limiter = FrameLimiter::default();
        assert!(limiter.ready(true, Duration::ZERO));
        limiter.record(Duration::ZERO);
        assert!(!limiter.ready(false, Duration::from_secs(1)));
        assert!(!limiter.ready(true, Duration::from_millis(99)));
        assert!(limiter.ready(true, Duration::from_millis(100)));
        limiter.record(Duration::from_millis(100));
        assert!(!limiter.ready(true, Duration::from_millis(199)));
        assert!(limiter.ready(true, Duration::from_millis(200)));
        assert_eq!(
            limiter.poll_duration(false, Duration::from_millis(200)),
            WATCH_POLL_INTERVAL
        );
    }
}
