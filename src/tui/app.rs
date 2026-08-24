//! T10 fixed-screen TUI application loop and key handling.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::{Frame, Terminal};

use crate::activity::{self, ActivityItem, OperatorSnapshot};
use crate::diagnostics::local::{self, NoticeVerdict};
use crate::diagnostics::remote::{self, StandaloneVersionStatus, VersionCommandRunner};
use crate::diagnostics::{
    ControllerCounterSnapshot, ControllerInputStatus, OccurrenceLogStatus, OwnerFreshness,
    PersistenceCounters, RuntimeDiagnosticsSnapshot,
};
#[cfg(any(test, feature = "workload-harness"))]
use crate::herdr::collector::ObservationQuality;
use crate::herdr::collector::{PerformancePublication, SourceCoverageRegistry};
use crate::model::{DomainModel, OperatorCommand, RunId, RunKey, SharedModel};
#[cfg(any(test, feature = "workload-harness"))]
use crate::performance::PerformanceSnapshot;
use crate::store::writer::PersistenceStatus;

use super::dag::DagOrder;
use super::view::{self, TreeRow};

const FRAME_INTERVAL: Duration = Duration::from_millis(100);
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Injectable wall clock used by terminal-expiry and notice policy.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

/// Production Unix-epoch clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX)
    }
}

fn next_wall_second_ms(now_ms: i64) -> i64 {
    let until_next = 1_000_i64.saturating_sub(now_ms.rem_euclid(1_000));
    now_ms.checked_add(until_next).unwrap_or(i64::MAX)
}

trait NoticePublicationBoundary: fmt::Debug + Send + Sync {
    fn inspect(&self, state_base: &Path, package_version: &str) -> NoticeVerdict;
    fn publish(
        &self,
        state_base: &Path,
        package_version: &str,
        dismissed_at_ms: i64,
    ) -> Result<(), ()>;
}

#[derive(Debug, Default)]
struct LocalNoticePublication;

impl NoticePublicationBoundary for LocalNoticePublication {
    fn inspect(&self, state_base: &Path, package_version: &str) -> NoticeVerdict {
        local::inspect_notice(state_base, package_version)
    }

    fn publish(
        &self,
        state_base: &Path,
        package_version: &str,
        dismissed_at_ms: i64,
    ) -> Result<(), ()> {
        local::publish_notice(state_base, package_version, dismissed_at_ms).map_err(|_| ())
    }
}

/// Owner-only setup facts evaluated before the TUI is constructed.
#[derive(Clone, Debug)]
pub struct TuiSetup {
    standalone_status: Option<StandaloneVersionStatus>,
    notice_state_base: Option<PathBuf>,
    home: Option<OsString>,
    notice_visible: bool,
    ascii_tree: bool,
    notice_publication: Arc<dyn NoticePublicationBoundary>,
}

impl Default for TuiSetup {
    fn default() -> Self {
        Self {
            standalone_status: None,
            notice_state_base: None,
            home: None,
            notice_visible: false,
            ascii_tree: false,
            notice_publication: Arc::new(LocalNoticePublication),
        }
    }
}

impl TuiSetup {
    /// Evaluates exactly the standalone compatibility probe and read-only notice marker.
    #[must_use]
    pub fn for_owner(
        state_base: PathBuf,
        home: Option<OsString>,
        runner: &impl VersionCommandRunner,
    ) -> Self {
        Self::for_owner_with_notice(state_base, home, runner, Arc::new(LocalNoticePublication))
    }

    fn for_owner_with_notice(
        state_base: PathBuf,
        home: Option<OsString>,
        runner: &impl VersionCommandRunner,
        notice_publication: Arc<dyn NoticePublicationBoundary>,
    ) -> Self {
        let standalone_status = remote::standalone_version_status(runner);
        let compatible = matches!(
            standalone_status,
            StandaloneVersionStatus::Compatible { .. }
        );
        let dismissed = matches!(
            notice_publication.inspect(&state_base, env!("CARGO_PKG_VERSION")),
            NoticeVerdict::Current { .. }
        );
        Self {
            standalone_status: Some(standalone_status),
            notice_state_base: Some(state_base),
            home,
            notice_visible: !compatible && !dismissed,
            ascii_tree: false,
            notice_publication,
        }
    }

    /// Selects ASCII connector glyphs for execution-tree rendering.
    #[must_use]
    pub const fn with_ascii_tree(mut self, ascii_tree: bool) -> Self {
        self.ascii_tree = ascii_tree;
        self
    }

    #[must_use]
    pub const fn notice_visible(&self) -> bool {
        self.notice_visible
    }

    #[must_use]
    pub const fn ascii_tree(&self) -> bool {
        self.ascii_tree
    }

    pub(super) fn standalone_status(&self) -> Option<&StandaloneVersionStatus> {
        self.standalone_status.as_ref()
    }

    pub(super) fn home(&self) -> Option<&std::ffi::OsStr> {
        self.home.as_deref()
    }
}

/// Static header values plus collector-published dynamic header inputs.
#[derive(Clone, Debug)]
pub struct HeaderInputs {
    /// Host that owns the monitored Herdr session.
    pub host: String,
    /// Human-facing Herdr named session.
    pub session: String,
    /// Honest summary of currently available sources.
    pub source_coverage: tokio::sync::watch::Receiver<SourceCoverageRegistry>,
    /// Coherent performance snapshot and effective-quality generation.
    pub performance: tokio::sync::watch::Receiver<PerformancePublication>,
}

/// Fixture/test default that deliberately leaks one watch-channel sender pair per call so the
/// channels never close. Production code must build the literal directly rather than call
/// `Default` in a loop.
#[cfg(any(test, feature = "workload-harness"))]
impl Default for HeaderInputs {
    fn default() -> Self {
        let (coverage_sender, source_coverage) =
            tokio::sync::watch::channel(SourceCoverageRegistry::default());
        // Deliberately leak this fixture sender so the coverage watch never closes.
        std::mem::forget(coverage_sender);
        let (performance_sender, performance) =
            tokio::sync::watch::channel(PerformancePublication {
                snapshot: PerformanceSnapshot::default(),
                effective_quality: ObservationQuality::Live,
                #[cfg(feature = "workload-harness")]
                workload_sample_stamp: None,
            });
        // Deliberately leak this fixture sender so the performance watch never closes.
        std::mem::forget(performance_sender);
        Self {
            host: "unknown".to_owned(),
            session: "unknown".to_owned(),
            source_coverage,
            performance,
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
    pub(crate) fn run_id(&self) -> Option<RunId> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Overlay {
    Notice,
    Help,
    Detail,
    Summary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectionReason {
    Merged,
    Closed,
    Filtered,
    Collapsed,
    AgedOut,
}

impl SelectionReason {
    #[cfg(test)]
    const fn name(self) -> &'static str {
        match self {
            Self::Merged => "merged",
            Self::Closed => "closed",
            Self::Filtered => "filtered",
            Self::Collapsed => "collapsed",
            Self::AgedOut => "aged_out",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::Merged => "Selection merged into the visible canonical identity",
            Self::Closed => "Selection moved: closed",
            Self::Filtered => "Selection moved: filtered",
            Self::Collapsed => "Selection moved: collapsed",
            Self::AgedOut => "Selection moved: aged_out",
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
    selection_reason: Option<SelectionReason>,
    execution_order: StableOrder,
    view_mode: ViewMode,
    dag_order: DagOrder,
    collapsed: HashSet<NodeKey>,
    filter_query: String,
    filter_draft: Option<String>,
    overlay: Option<Overlay>,
    summary_session_wide: bool,
    overlay_scroll: AtomicUsize,
    overlay_scroll_max: AtomicUsize,
    safe_warning: Option<&'static str>,
    operator_activity: Arc<[ActivityItem]>,
    terminal_times: Arc<HashMap<RunId, i64>>,
    now_ms: i64,
    session_display_name: String,
    #[cfg(test)]
    projection_build_count: AtomicUsize,
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

    pub(super) fn selection_reason_text(&self) -> Option<&'static str> {
        self.selection_reason.map(SelectionReason::message)
    }

    #[cfg(test)]
    fn selection_reason(&self) -> Option<&'static str> {
        self.selection_reason.map(SelectionReason::name)
    }

    pub(super) const fn view_mode(&self) -> ViewMode {
        self.view_mode
    }

    pub(super) const fn dag_order(&self) -> &DagOrder {
        &self.dag_order
    }

    pub(super) fn collapsed(&self) -> &HashSet<NodeKey> {
        &self.collapsed
    }

    pub(super) fn filter_query(&self) -> &str {
        &self.filter_query
    }

    pub(super) fn filter_draft(&self) -> Option<&str> {
        self.filter_draft.as_deref()
    }

    pub(super) const fn overlay(&self) -> Option<Overlay> {
        self.overlay
    }

    pub(super) const fn summary_scope(&self) -> super::projection::SummaryScope {
        if self.summary_session_wide {
            super::projection::SummaryScope::Session
        } else {
            super::projection::SummaryScope::SelectionWorkspace
        }
    }

    pub(super) fn overlay_scroll(&self) -> usize {
        self.overlay_scroll.load(AtomicOrdering::Relaxed)
    }

    fn reset_overlay_scroll(&self) {
        self.overlay_scroll.store(0, AtomicOrdering::Relaxed);
        self.overlay_scroll_max
            .store(usize::MAX, AtomicOrdering::Relaxed);
    }

    fn scroll_overlay_up(&self) {
        let current = self.overlay_scroll();
        self.overlay_scroll
            .store(current.saturating_sub(1), AtomicOrdering::Relaxed);
    }

    fn scroll_overlay_down(&self) {
        let current = self.overlay_scroll();
        let maximum = self.overlay_scroll_max.load(AtomicOrdering::Relaxed);
        self.overlay_scroll.store(
            current.saturating_add(1).min(maximum),
            AtomicOrdering::Relaxed,
        );
    }

    pub(super) fn normalize_overlay_scroll(&self, maximum: usize) -> usize {
        self.overlay_scroll_max
            .store(maximum, AtomicOrdering::Relaxed);
        let normalized = self.overlay_scroll().min(maximum);
        self.overlay_scroll
            .store(normalized, AtomicOrdering::Relaxed);
        normalized
    }

    pub(super) const fn safe_warning(&self) -> Option<&'static str> {
        self.safe_warning
    }

    pub(super) fn operator_snapshot(&self) -> OperatorSnapshot {
        OperatorSnapshot {
            activity: Arc::clone(&self.operator_activity),
            terminal_times: Arc::clone(&self.terminal_times),
        }
    }

    pub(super) const fn now_ms(&self) -> i64 {
        self.now_ms
    }

    pub(super) fn session_display_name(&self) -> &str {
        &self.session_display_name
    }

    #[cfg(test)]
    pub(super) fn record_projection_build(&self) {
        self.projection_build_count
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    #[cfg(test)]
    fn reset_projection_build_count(&self) {
        self.projection_build_count
            .store(0, AtomicOrdering::Relaxed);
    }

    #[cfg(test)]
    fn projection_build_count(&self) -> usize {
        self.projection_build_count.load(AtomicOrdering::Relaxed)
    }

    #[cfg(test)]
    fn is_collapsed(&self, key: &NodeKey) -> bool {
        self.collapsed.contains(key)
    }

    #[cfg(test)]
    fn collapsed_len(&self) -> usize {
        self.collapsed.len()
    }
}

fn default_diagnostics() -> RuntimeDiagnosticsSnapshot {
    RuntimeDiagnosticsSnapshot {
        persistence: PersistenceStatus::Healthy,
        controller_input: ControllerInputStatus::Unavailable {
            reason: crate::diagnostics::ControllerInputUnavailableReason::ListenerUnavailable,
        },
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

fn default_operator() -> OperatorSnapshot {
    OperatorSnapshot {
        activity: Arc::from(Vec::new()),
        terminal_times: Arc::new(HashMap::new()),
    }
}

/// Fixed-screen monitor state backed by coherent model and performance publications.
pub struct App {
    model_receiver: SharedModel,
    operator_commands: tokio::sync::mpsc::Sender<OperatorCommand>,
    diagnostics_receiver: tokio::sync::watch::Receiver<RuntimeDiagnosticsSnapshot>,
    operator_receiver: tokio::sync::watch::Receiver<OperatorSnapshot>,
    model: Arc<DomainModel>,
    performance: PerformancePublication,
    diagnostics: RuntimeDiagnosticsSnapshot,
    header: HeaderInputs,
    state: AppState,
    setup: TuiSetup,
    clock: Arc<dyn Clock>,
    rows: Vec<TreeRow>,
    app_started_at_ms: i64,
    next_expiry_ms: Option<i64>,
    next_paint_ms: i64,
}

impl App {
    /// Creates an application from observation receivers and display-only header inputs.
    ///
    /// This convenience constructor discards the command receiver, so `c` is inert. Use
    /// [`Self::with_operator_commands`] when the application needs a live collector command path.
    #[must_use]
    pub fn new(model_receiver: SharedModel, header: HeaderInputs) -> Self {
        let (operator_commands, operator_command_receiver) = tokio::sync::mpsc::channel(1);
        drop(operator_command_receiver);
        debug_assert!(operator_commands.is_closed());
        Self::with_operator_commands(model_receiver, operator_commands, header)
    }

    /// Creates an application with an injectable clock and no collector command path.
    #[must_use]
    pub fn with_clock(
        model_receiver: SharedModel,
        header: HeaderInputs,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let (operator_commands, operator_command_receiver) = tokio::sync::mpsc::channel(1);
        drop(operator_command_receiver);
        debug_assert!(operator_commands.is_closed());
        let (_diagnostics_sender, diagnostics_receiver) =
            tokio::sync::watch::channel(default_diagnostics());
        let (_operator_sender, operator_receiver) = tokio::sync::watch::channel(default_operator());
        Self::with_operator_commands_and_inputs(
            model_receiver,
            operator_commands,
            header,
            diagnostics_receiver,
            operator_receiver,
            TuiSetup::default(),
            clock,
        )
    }

    /// Creates an application with a bounded command path to the collector.
    #[must_use]
    pub fn with_operator_commands(
        model_receiver: SharedModel,
        operator_commands: tokio::sync::mpsc::Sender<OperatorCommand>,
        header: HeaderInputs,
    ) -> Self {
        let (_diagnostics_sender, diagnostics_receiver) =
            tokio::sync::watch::channel(default_diagnostics());
        let (_operator_sender, operator_receiver) = tokio::sync::watch::channel(default_operator());
        Self::with_operator_commands_and_inputs(
            model_receiver,
            operator_commands,
            header,
            diagnostics_receiver,
            operator_receiver,
            TuiSetup::default(),
            Arc::new(SystemClock),
        )
    }

    /// Creates an application with every read-only diagnostic, operator, setup, and time input.
    ///
    /// This convenience constructor discards the command receiver, so `c` is inert. Use
    /// [`Self::with_operator_commands_and_inputs`] for a live collector command path.
    #[must_use]
    pub fn with_inputs(
        model_receiver: SharedModel,
        header: HeaderInputs,
        diagnostics_receiver: tokio::sync::watch::Receiver<RuntimeDiagnosticsSnapshot>,
        operator_receiver: tokio::sync::watch::Receiver<OperatorSnapshot>,
        setup: TuiSetup,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let (operator_commands, operator_command_receiver) = tokio::sync::mpsc::channel(1);
        drop(operator_command_receiver);
        debug_assert!(operator_commands.is_closed());
        Self::with_operator_commands_and_inputs(
            model_receiver,
            operator_commands,
            header,
            diagnostics_receiver,
            operator_receiver,
            setup,
            clock,
        )
    }

    /// Creates an application with every observation input plus the collector command path.
    #[must_use]
    pub fn with_operator_commands_and_inputs(
        model_receiver: SharedModel,
        operator_commands: tokio::sync::mpsc::Sender<OperatorCommand>,
        header: HeaderInputs,
        diagnostics_receiver: tokio::sync::watch::Receiver<RuntimeDiagnosticsSnapshot>,
        operator_receiver: tokio::sync::watch::Receiver<OperatorSnapshot>,
        setup: TuiSetup,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let model = Arc::clone(&model_receiver.borrow());
        let performance = header.performance.borrow().clone();
        let diagnostics = diagnostics_receiver.borrow().clone();
        let operator_activity = Arc::clone(&operator_receiver.borrow().activity);
        let terminal_times = Arc::clone(&operator_receiver.borrow().terminal_times);
        let now_ms = clock.now_ms();
        let notice_visible = setup.notice_visible;
        let session_display_name = super::projection::escape_controls(&header.session);
        let mut app = Self {
            model_receiver,
            operator_commands,
            diagnostics_receiver,
            operator_receiver,
            model,
            performance,
            diagnostics,
            header,
            state: AppState {
                follow: true,
                overlay: notice_visible.then_some(Overlay::Notice),
                operator_activity,
                terminal_times,
                now_ms,
                session_display_name,
                ..AppState::default()
            },
            setup,
            clock,
            rows: Vec::new(),
            app_started_at_ms: now_ms,
            next_expiry_ms: None,
            next_paint_ms: next_wall_second_ms(now_ms),
        };
        app.state.adopt_model(app.model.as_ref());
        let rows = app.rebuild_rows();
        let last = rows.last().map(|row| row.key.clone());
        app.set_selection(last);
        app
    }

    fn rebuild_rows(&mut self) -> Vec<TreeRow> {
        self.state.now_ms = self.clock.now_ms();
        let projection = view::build_projection(self.model.as_ref(), &self.state);
        self.next_expiry_ms = projection.next_expiry_ms;
        self.rows = projection.rows;
        self.rows.clone()
    }

    /// Refreshes the cached coherent model and performance publication.
    pub fn refresh(&mut self) {
        self.refresh_cached(true);
    }

    fn refresh_cached(&mut self, recompute_projection: bool) {
        let old_rows = if recompute_projection && !self.state.follow {
            // Keep the cached time for historical disappearance classification. This lookup does
            // not become the visible row set and must not replace the pending visible deadline.
            view::build_rows(self.model.as_ref(), &self.state)
        } else {
            Vec::new()
        };
        self.performance = self.header.performance.borrow_and_update().clone();
        self.diagnostics = self.diagnostics_receiver.borrow_and_update().clone();
        self.header.source_coverage.borrow_and_update();
        if !recompute_projection {
            return;
        }
        let new_model = Arc::clone(&self.model_receiver.borrow_and_update());
        let operator = self.operator_receiver.borrow_and_update();
        self.state.operator_activity = Arc::clone(&operator.activity);
        self.state.terminal_times = Arc::clone(&operator.terminal_times);
        drop(operator);
        self.state.adopt_model(new_model.as_ref());
        self.model = new_model;
        let new_rows = self.rebuild_rows();
        if self.state.follow {
            self.state.selection_reason = None;
            self.set_selection(new_rows.last().map(|row| row.key.clone()));
        } else {
            let hint = self.disappearance_reason(&old_rows, &new_rows);
            self.recover_selection(&old_rows, &new_rows, hint);
        }
    }

    fn refresh_if_changed(&mut self) -> io::Result<bool> {
        let model_changed = self.model_receiver.has_changed().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "model watch closed; collector is no longer publishing state",
            )
        })?;
        let performance_changed = self.header.performance.has_changed().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "performance watch closed; collector is no longer publishing performance state",
            )
        })?;
        let coverage_changed = self.header.source_coverage.has_changed().unwrap_or(false);
        let diagnostics_changed = self.diagnostics_receiver.has_changed().unwrap_or(false);
        let operator_changed = self.operator_receiver.has_changed().unwrap_or(false);
        let deadline_due = self
            .next_expiry_ms
            .is_some_and(|deadline| self.clock.now_ms() >= deadline);
        if model_changed
            || performance_changed
            || coverage_changed
            || diagnostics_changed
            || operator_changed
            || deadline_due
        {
            self.refresh_cached(model_changed || operator_changed || deadline_due);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Applies one keyboard event, sending only bounded operator intent to the collector.
    pub fn handle_key(&mut self, key: KeyEvent) -> LoopControl {
        if key.kind == KeyEventKind::Release {
            return LoopControl::Continue;
        }
        if let Some(overlay) = self.state.overlay {
            return self.handle_overlay_key(overlay, key.code);
        }
        if self.state.filter_draft.is_some() {
            return self.handle_filter_key(key.code);
        }
        match key.code {
            KeyCode::Char('q') => LoopControl::Exit,
            KeyCode::Char('/') => {
                self.state.filter_draft = Some(self.state.filter_query.clone());
                LoopControl::Continue
            }
            KeyCode::Char('c') if key.modifiers.is_empty() => {
                // Never block the UI: a full queue already has a receipt-time dismissal pending,
                // while a closed queue has no collector that could service another command.
                let _ = self
                    .operator_commands
                    .try_send(OperatorCommand::DismissClearable);
                LoopControl::Continue
            }
            KeyCode::Char('i') => {
                self.state.overlay = Some(Overlay::Detail);
                self.state.reset_overlay_scroll();
                LoopControl::Continue
            }
            KeyCode::Char('s') if key.modifiers.is_empty() => {
                self.state.overlay = Some(Overlay::Summary);
                self.state.summary_session_wide = false;
                self.state.reset_overlay_scroll();
                LoopControl::Continue
            }
            KeyCode::Char('?') => {
                self.state.overlay = Some(Overlay::Help);
                self.state.reset_overlay_scroll();
                LoopControl::Continue
            }
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
            KeyCode::Left => {
                self.navigate_left();
                LoopControl::Continue
            }
            KeyCode::Right => {
                self.navigate_right();
                LoopControl::Continue
            }
            KeyCode::Enter => {
                if let Some(selected) = self.state.selected.clone() {
                    self.toggle_collapse(selected);
                }
                LoopControl::Continue
            }
            _ => LoopControl::Continue,
        }
    }

    /// Renders the current cached state into one frame.
    pub fn render(&self, frame: &mut Frame<'_>) {
        let now_ms = self.clock.now_ms();
        view::render(
            frame,
            self.model.as_ref(),
            &self.performance,
            &self.header,
            view::PaintSnapshot::new(&self.state, &self.rows, now_ms, self.session_start_ms()),
            &self.diagnostics,
            &self.setup,
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
            dirty |= self.paint_tick_due();
            let now = started.elapsed();
            if limiter.ready(dirty, now) {
                terminal.draw(|frame| self.render(frame))?;
                limiter.record(now);
                dirty = false;
            }

            let poll_for = self.poll_duration(&limiter, dirty, started.elapsed());
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

    #[cfg(test)]
    const fn next_expiry_ms(&self) -> Option<i64> {
        self.next_expiry_ms
    }

    #[cfg(test)]
    fn reset_projection_build_count(&self) {
        self.state.reset_projection_build_count();
    }

    #[cfg(test)]
    fn projection_build_count(&self) -> usize {
        self.state.projection_build_count()
    }

    fn session_start_ms(&self) -> i64 {
        self.model
            .task_runs()
            .filter_map(|run| run.created_at_ms)
            .min()
            .unwrap_or(self.app_started_at_ms)
    }

    fn paint_tick_due(&mut self) -> bool {
        let now_ms = self.clock.now_ms();
        if now_ms < self.next_paint_ms {
            return false;
        }
        self.next_paint_ms = next_wall_second_ms(now_ms);
        true
    }

    fn handle_overlay_key(&mut self, overlay: Overlay, code: KeyCode) -> LoopControl {
        match overlay {
            Overlay::Notice if matches!(code, KeyCode::Enter | KeyCode::Esc) => {
                self.state.overlay = None;
                if let Some(state_base) = &self.setup.notice_state_base {
                    let package_version = env!("CARGO_PKG_VERSION");
                    if self
                        .setup
                        .notice_publication
                        .publish(state_base, package_version, self.clock.now_ms())
                        .is_err()
                    {
                        let observable = matches!(
                            self.setup
                                .notice_publication
                                .inspect(state_base, package_version),
                            NoticeVerdict::Current { .. }
                        );
                        let warning = if observable {
                            "notice_marker_sync_unconfirmed"
                        } else {
                            "notice_marker_write_failed"
                        };
                        self.state.safe_warning = Some(warning);
                        tracing::warn!(code = warning, "TUI notice marker publication uncertain");
                    }
                }
            }
            Overlay::Help if matches!(code, KeyCode::Esc | KeyCode::Char('?')) => {
                self.state.overlay = None;
            }
            Overlay::Detail if matches!(code, KeyCode::Esc | KeyCode::Char('i')) => {
                self.state.overlay = None;
            }
            Overlay::Summary if matches!(code, KeyCode::Esc | KeyCode::Char('s')) => {
                // Modifier-blind closing stays local because it is a reversible UI-only toggle.
                self.state.overlay = None;
            }
            Overlay::Summary if code == KeyCode::Char('w') => {
                self.state.summary_session_wide = !self.state.summary_session_wide;
                self.state.reset_overlay_scroll();
            }
            Overlay::Help | Overlay::Detail | Overlay::Summary => match code {
                KeyCode::Up => self.state.scroll_overlay_up(),
                KeyCode::Down => self.state.scroll_overlay_down(),
                _ => {}
            },
            Overlay::Notice => {}
        }
        LoopControl::Continue
    }

    fn handle_filter_key(&mut self, code: KeyCode) -> LoopControl {
        match code {
            KeyCode::Esc => self.state.filter_draft = None,
            KeyCode::Enter => self.commit_filter(),
            KeyCode::Backspace => {
                if let Some(draft) = &mut self.state.filter_draft {
                    draft.pop();
                }
            }
            KeyCode::Char(character) => {
                if let Some(draft) = &mut self.state.filter_draft {
                    draft.push(character);
                }
            }
            _ => {}
        }
        LoopControl::Continue
    }

    fn commit_filter(&mut self) {
        let old_rows = self.rebuild_rows();
        let query = self
            .state
            .filter_draft
            .take()
            .unwrap_or_default()
            .trim()
            .to_owned();
        self.state.filter_query = query;
        self.state.follow = false;
        let new_rows = self.rebuild_rows();
        self.recover_selection(&old_rows, &new_rows, Some(SelectionReason::Filtered));
    }

    fn collapse_available(&self) -> bool {
        self.state.view_mode == ViewMode::ExecutionTree && self.state.filter_query.is_empty()
    }

    fn toggle_collapse(&mut self, key: NodeKey) {
        if !self.collapse_available() {
            return;
        }
        let old_rows = self.rebuild_rows();
        let full_rows = view::build_uncollapsed_rows(self.model.as_ref(), &self.state);
        let Some(index) = full_rows.iter().position(|row| row.key == key) else {
            return;
        };
        let depth = full_rows[index].depth;
        let is_branch = full_rows
            .get(index.saturating_add(1))
            .is_some_and(|next| next.depth > depth);
        if !is_branch {
            return;
        }
        self.state.follow = false;
        self.state.selection_reason = None;
        let collapsed = if self.state.collapsed.remove(&key) {
            false
        } else {
            self.state.collapsed.insert(key);
            true
        };
        let new_rows = self.rebuild_rows();
        if collapsed {
            self.recover_selection(&old_rows, &new_rows, Some(SelectionReason::Collapsed));
        }
    }

    fn navigate_right(&mut self) {
        if !self.collapse_available() {
            return;
        }
        let Some(selected) = self.state.selected.clone() else {
            return;
        };
        if self.state.collapsed.contains(&selected) {
            self.toggle_collapse(selected);
            return;
        }
        let rows = self.rebuild_rows();
        let Some(index) = rows.iter().position(|row| row.key == selected) else {
            return;
        };
        let depth = rows[index].depth;
        if let Some(child) = rows
            .get(index.saturating_add(1))
            .filter(|row| row.depth > depth)
        {
            self.state.follow = false;
            self.state.selection_reason = None;
            self.set_selection(Some(child.key.clone()));
        }
    }

    fn navigate_left(&mut self) {
        if !self.collapse_available() {
            return;
        }
        let Some(selected) = self.state.selected.clone() else {
            return;
        };
        let rows = self.rebuild_rows();
        let Some(index) = rows.iter().position(|row| row.key == selected) else {
            return;
        };
        let depth = rows[index].depth;
        let expanded_branch = !self.state.collapsed.contains(&selected)
            && rows
                .get(index.saturating_add(1))
                .is_some_and(|next| next.depth > depth);
        if expanded_branch {
            self.toggle_collapse(selected);
            return;
        }
        let parent = rows[..index]
            .iter()
            .rev()
            .find(|row| row.depth < depth)
            .map(|row| row.key.clone());
        if let Some(parent) = parent {
            self.state.follow = false;
            self.state.selection_reason = None;
            self.set_selection(Some(parent));
        }
    }

    fn poll_duration(&self, limiter: &FrameLimiter, dirty: bool, elapsed: Duration) -> Duration {
        let base = limiter.poll_duration(dirty, elapsed);
        let now_ms = self.clock.now_ms();
        let until_expiry = self
            .next_expiry_ms
            .map(|deadline| Duration::from_millis(deadline.saturating_sub(now_ms).max(0) as u64));
        let until_paint =
            Duration::from_millis(next_wall_second_ms(now_ms).saturating_sub(now_ms).max(0) as u64);
        until_expiry
            .map_or(base, |expiry| base.min(expiry))
            .min(until_paint)
    }

    fn move_selection(&mut self, down: bool) {
        self.state.follow = false;
        self.state.selection_reason = None;
        let rows = self.rebuild_rows();
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
        let selected = self.rebuild_rows().last().map(|row| row.key.clone());
        self.set_selection(selected);
    }

    fn toggle_view(&mut self) {
        let previous = self.state.selected.clone();
        let old_rows = self.rebuild_rows();
        let was_following = self.state.follow;
        let semantic_run_id = previous.as_ref().and_then(|selected| {
            selected.run_id().or_else(|| match selected {
                NodeKey::Agent { agent_node_id, .. } => self
                    .model
                    .agent_node(agent_node_id)
                    .map(|agent| agent.task_run_id),
                _ => None,
            })
        });
        self.state.view_mode = self.state.view_mode.toggled();
        self.state.selection_reason = None;
        let rows = self.rebuild_rows();
        if was_following {
            self.set_selection(rows.last().map(|row| row.key.clone()));
            return;
        }
        let replacement = previous.as_ref().and_then(|selected| {
            semantic_run_id.and_then(|run_id| {
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
            let replacement = previous
                .as_ref()
                .and_then(|selected| nearest_visible_ancestor(&old_rows, &rows, selected))
                .or_else(|| {
                    previous
                        .as_ref()
                        .and_then(|selected| surviving_neighbor(&old_rows, &rows, selected))
                })
                .or_else(|| rows.first().map(|row| row.key.clone()));
            self.set_selection(replacement);
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

    fn disappearance_reason(
        &self,
        old_rows: &[TreeRow],
        new_rows: &[TreeRow],
    ) -> Option<SelectionReason> {
        let selected = self.state.selected.as_ref()?;
        if !old_rows.iter().any(|row| &row.key == selected)
            || new_rows.iter().any(|row| &row.key == selected)
        {
            return None;
        }
        if let (Some(old_run_id), Some(selected_key)) =
            (selected.run_id(), self.state.selected_run_key.as_ref())
            && self
                .model
                .task_run_by_key(selected_key)
                .is_some_and(|run| run.run_id != old_run_id)
        {
            return Some(SelectionReason::Merged);
        }
        if selected.run_id().is_some_and(|run_id| {
            self.model
                .task_run(&run_id)
                .is_some_and(|run| run.state.is_terminal())
                && self.state.terminal_times.get(&run_id).is_some_and(|time| {
                    self.state.now_ms
                        >= time.saturating_add(activity::DEFAULT_TERMINAL_VISIBILITY_MS)
                })
        }) {
            return Some(SelectionReason::AgedOut);
        }
        Some(SelectionReason::Closed)
    }

    fn recover_selection(
        &mut self,
        old_rows: &[TreeRow],
        new_rows: &[TreeRow],
        reason_hint: Option<SelectionReason>,
    ) {
        let Some(selected) = self.state.selected.clone() else {
            self.set_selection(new_rows.last().map(|row| row.key.clone()));
            return;
        };
        if new_rows.iter().any(|row| row.key == selected) {
            self.state.selection_reason = None;
            return;
        }

        if let Some(old_run_id) = selected.run_id()
            && let Some(selected_key) = self.state.selected_run_key.as_ref()
            && let Some(survivor) = self.model.task_run_by_key(selected_key)
            && survivor.run_id != old_run_id
            && let Some(row) = preferred_run_row(new_rows, survivor.run_id, &selected)
        {
            let survivor_id = survivor.run_id;
            let replacement = row.key.clone();
            self.set_selection(Some(replacement));
            self.state.selection_reason = (survivor_id != old_run_id)
                .then_some(SelectionReason::Merged)
                .or(reason_hint);
            return;
        }

        let replacement = nearest_visible_ancestor(old_rows, new_rows, &selected)
            .or_else(|| surviving_neighbor(old_rows, new_rows, &selected))
            .or_else(|| new_rows.first().map(|row| row.key.clone()));
        self.set_selection(replacement);
        self.state.selection_reason = reason_hint.or(Some(SelectionReason::Closed));
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
// increment5-workload-harness: begin production frame limiter driver
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadFrameObservation {
    /// Ordinal assigned to this draw, or `None` when the limiter skipped it.
    pub draw_ordinal: Option<u64>,
    /// Complete App-cached publication used by the successful draw.
    pub performance_publication: Option<PerformancePublication>,
    /// Monotonic timestamp observed immediately after the successful draw.
    pub rendered_at: Option<Duration>,
    /// Raw header body row emitted by the real renderer.
    pub rendered_header_line: Option<String>,
    /// The production poll duration computed after this iteration.
    pub poll_duration: Duration,
}

/// Feature-only driver for workload measurements through the production frame limiter.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
pub struct WorkloadFrameDriver {
    app: App,
    terminal: Terminal<ratatui::backend::TestBackend>,
    limiter: FrameLimiter,
    clock: Box<dyn FnMut() -> Duration>,
    dirty: bool,
    next_draw_ordinal: u64,
    header_projection: view::workload_header_projection::WorkloadHeaderProjection,
}

#[cfg(feature = "workload-harness")]
impl WorkloadFrameDriver {
    /// Creates a driver with the same initially-dirty state as [`App::run`].
    #[must_use]
    pub fn new(
        app: App,
        terminal: Terminal<ratatui::backend::TestBackend>,
        clock: impl FnMut() -> Duration + 'static,
    ) -> Self {
        Self {
            app,
            terminal,
            limiter: FrameLimiter::default(),
            clock: Box::new(clock),
            dirty: true,
            next_draw_ordinal: 0,
            header_projection: view::workload_header_projection::WorkloadHeaderProjection::default(
            ),
        }
    }

    /// Creates a driver with an explicit feature-only performance-label projection.
    #[must_use]
    pub fn new_with_header_projection(
        app: App,
        terminal: Terminal<ratatui::backend::TestBackend>,
        clock: impl FnMut() -> Duration + 'static,
        omit_performance_label: bool,
    ) -> Self {
        let mut driver = Self::new(app, terminal, clock);
        driver.header_projection = view::workload_header_projection::WorkloadHeaderProjection {
            omit_performance_label,
        };
        driver
    }

    /// Runs one production-ordered refresh, limiter, draw, record, and poll iteration.
    pub fn step(&mut self, dirty: bool) -> io::Result<WorkloadFrameObservation> {
        self.step_inner(dirty, true)
    }

    /// Applies the ordinary App watch refresh without entering the draw phase.
    pub fn refresh_app_if_changed(&mut self) -> io::Result<bool> {
        let changed = self.app.refresh_if_changed()?;
        self.dirty |= changed;
        Ok(changed)
    }

    /// Runs a limiter/draw iteration from the already refreshed App cache.
    pub fn step_without_refresh(&mut self, dirty: bool) -> io::Result<WorkloadFrameObservation> {
        self.step_inner(dirty, false)
    }

    fn step_inner(
        &mut self,
        dirty: bool,
        refresh_app: bool,
    ) -> io::Result<WorkloadFrameObservation> {
        self.dirty |= dirty;
        if refresh_app {
            self.dirty |= self.app.refresh_if_changed()?;
        }
        let now = (self.clock)();
        let mut performance_publication = None;
        let mut rendered_at = None;
        let mut rendered_header_line = None;
        let draw_ordinal = if self.limiter.ready(self.dirty, now) {
            let draw_ordinal = self.next_draw_ordinal;
            let next_draw_ordinal = draw_ordinal
                .checked_add(1)
                .ok_or_else(|| io::Error::other("workload frame draw ordinal exceeded u64"))?;
            let publication = self.app.performance.clone();
            let projection = self.header_projection;
            match self.terminal.draw(|frame| {
                if projection.omit_performance_label {
                    let now_ms = self.app.clock.now_ms();
                    view::workload_header_projection::render_with_workload_projection(
                        frame,
                        self.app.model.as_ref(),
                        &publication,
                        &self.app.header,
                        view::PaintSnapshot::new(
                            &self.app.state,
                            &self.app.rows,
                            now_ms,
                            self.app.session_start_ms(),
                        ),
                        &self.app.diagnostics,
                        &self.app.setup,
                        projection,
                    );
                } else {
                    self.app.render(frame);
                }
            }) {
                Ok(_) => {}
                Err(never) => match never {},
            }
            performance_publication = Some(publication);
            rendered_header_line = Some(workload_rendered_header_line(&self.terminal)?);
            self.limiter.record(now);
            self.dirty = false;
            self.next_draw_ordinal = next_draw_ordinal;
            Some(draw_ordinal)
        } else {
            None
        };
        let after_iteration = (self.clock)();
        if draw_ordinal.is_some() {
            rendered_at = Some(after_iteration);
        }
        let poll_duration = self
            .app
            .poll_duration(&self.limiter, self.dirty, after_iteration);
        Ok(WorkloadFrameObservation {
            draw_ordinal,
            performance_publication,
            rendered_at,
            rendered_header_line,
            poll_duration,
        })
    }

    /// Applies one key and waits for the first limiter-eligible frame that reflects it.
    pub fn handle_key_and_wait(&mut self, key: KeyEvent) -> io::Result<WorkloadFrameObservation> {
        if self.app.handle_key(key) == LoopControl::Exit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "exit keys do not produce a workload response frame",
            ));
        }
        self.dirty = true;
        loop {
            let observation = self.step(false)?;
            if observation.draw_ordinal.is_some() {
                return Ok(observation);
            }
            std::thread::sleep(observation.poll_duration);
        }
    }

    /// Returns the test terminal containing the most recently drawn frame.
    #[must_use]
    pub const fn terminal(&self) -> &Terminal<ratatui::backend::TestBackend> {
        &self.terminal
    }

    /// Returns the complete publication currently cached by App.
    #[must_use]
    pub const fn cached_performance_publication(&self) -> &PerformancePublication {
        &self.app.performance
    }

    /// Returns the exclusive draw watermark for the next successful draw.
    #[must_use]
    pub const fn next_draw_ordinal(&self) -> u64 {
        self.next_draw_ordinal
    }
}

#[cfg(feature = "workload-harness")]
fn workload_rendered_header_line(
    terminal: &Terminal<ratatui::backend::TestBackend>,
) -> io::Result<String> {
    let buffer = terminal.backend().buffer();
    let width = usize::from(buffer.area.width);
    buffer
        .content()
        .chunks(width)
        .map(|cells| cells.iter().map(|cell| cell.symbol()).collect::<String>())
        .find(|row| row.contains("session:"))
        .map(|row| row.trim_end().to_owned())
        .ok_or_else(|| io::Error::other("workload render omitted the header body row"))
}
// increment5-workload-harness: end production frame limiter driver

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

fn nearest_visible_ancestor(
    old_rows: &[TreeRow],
    new_rows: &[TreeRow],
    selected: &NodeKey,
) -> Option<NodeKey> {
    let index = old_rows.iter().position(|row| &row.key == selected)?;
    let mut depth = old_rows[index].depth;
    for row in old_rows[..index].iter().rev() {
        if row.depth < depth {
            depth = row.depth;
            if new_rows.iter().any(|candidate| candidate.key == row.key) {
                return Some(row.key.clone());
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
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use tokio::sync::watch;

    use super::*;
    use crate::activity::{ActivityDurability, ActivityIdentity, ActivityItem, OperatorSnapshot};
    use crate::diagnostics::remote::{
        VersionCommand, VersionCommandRunner, VersionProbeFailure, VersionProbeResult,
    };
    use crate::diagnostics::{
        ControllerCounterSnapshot, ControllerInputStatus, ControllerInputUnavailableReason,
        DiagnosticSource, InputAvailability, OccurrenceLogStatus, OwnerFreshness,
        PersistenceCounters, RuntimeDiagnosticsSnapshot, SourceCoverageSnapshot,
    };
    use crate::herdr::collector::{
        CoverageSource, ObservationQuality, PerformancePublication, SourceAvailability,
        SourceCoverageRegistry,
    };
    use crate::model::{
        AgentNode, DependencyEdge, DisplayOrdinal, DomainModel, ExecState, Execution,
        OperatorCommand, Pane, Provider, RunId, RunKey, Tab, TaskRun, TaskState, Workspace,
    };
    use crate::performance::{PerformanceDegradationReason, PerformanceSnapshot};
    use crate::store::writer::{
        DurabilityDisposition, PersistenceFailure, PersistenceFailureCode, PersistenceOperation,
        PersistencePhase, PersistenceStatus,
    };
    use crate::tui::projection::SummaryScope;

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
            label: None,
        });
        model.insert_pane(Pane {
            pane_id: "pane".to_owned(),
            workspace_id: "workspace".to_owned(),
            tab_id: "tab".to_owned(),
            terminal_id: "terminal".to_owned(),
            display_name: None,
        });
        for (run_id, label, ordinal, state) in runs {
            model.insert_task_run(TaskRun {
                run_id: *run_id,
                key: RunKey::Controller((*label).to_owned()),
                display_ordinal: DisplayOrdinal::new(*ordinal),
                state: *state,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
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

    fn shared_occurrence_model(
        shared: RunId,
        sibling: RunId,
        shared_state: TaskState,
        include_pane_one_occurrence: bool,
    ) -> DomainModel {
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
        model.insert_task_run(TaskRun {
            run_id: shared,
            key: RunKey::Controller("shared".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: shared_state,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
        model.insert_task_run(TaskRun {
            run_id: sibling,
            key: RunKey::Controller("sibling".to_owned()),
            display_ordinal: DisplayOrdinal::new(2),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
        if include_pane_one_occurrence {
            model.insert_execution(Execution {
                execution_id: "shared-pane-1".to_owned(),
                pane_id: "pane-1".to_owned(),
                terminal_id: "terminal-pane-1".to_owned(),
                task_run_id: shared,
                state: ExecState::Working,
            });
        }
        model.insert_execution(Execution {
            execution_id: "shared-pane-2".to_owned(),
            pane_id: "pane-2".to_owned(),
            terminal_id: "terminal-pane-2".to_owned(),
            task_run_id: shared,
            state: ExecState::Working,
        });
        model.insert_execution(Execution {
            execution_id: "sibling-pane-1".to_owned(),
            pane_id: "pane-1".to_owned(),
            terminal_id: "terminal-pane-1".to_owned(),
            task_run_id: sibling,
            state: ExecState::Working,
        });
        model
    }

    fn app_with_model(model: DomainModel) -> (App, watch::Sender<Arc<DomainModel>>) {
        let (model_sender, model_receiver) = watch::channel(Arc::new(model));
        let app = App::new(
            model_receiver,
            HeaderInputs {
                host: "host".to_owned(),
                session: "session".to_owned(),
                ..HeaderInputs::default()
            },
        );
        (app, model_sender)
    }

    fn performance_publication(
        quality: ObservationQuality,
        reasons: impl IntoIterator<Item = PerformanceDegradationReason>,
    ) -> PerformancePublication {
        PerformancePublication {
            snapshot: PerformanceSnapshot {
                reasons: reasons.into_iter().collect(),
                ..PerformanceSnapshot::default()
            },
            effective_quality: quality,
            #[cfg(feature = "workload-harness")]
            workload_sample_stamp: None,
        }
    }

    #[test]
    fn performance_only_refresh_does_not_rebuild_row_projection() {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (performance_sender, performance_receiver) =
            watch::channel(performance_publication(ObservationQuality::Live, []));
        let mut app = App::new(
            model_receiver,
            HeaderInputs {
                performance: performance_receiver,
                ..HeaderInputs::default()
            },
        );
        app.reset_projection_build_count();

        performance_sender
            .send(performance_publication(
                ObservationQuality::Degraded,
                [PerformanceDegradationReason::DependencyEdges],
            ))
            .unwrap();

        assert!(app.refresh_if_changed().unwrap());
        assert_eq!(app.projection_build_count(), 0);
        assert_eq!(
            app.performance,
            performance_publication(
                ObservationQuality::Degraded,
                [PerformanceDegradationReason::DependencyEdges],
            )
        );
    }

    #[test]
    fn app_never_joins_performance_and_quality_from_different_generations() {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let live = performance_publication(ObservationQuality::Live, []);
        let degraded = performance_publication(
            ObservationQuality::Degraded,
            [PerformanceDegradationReason::EventsSixtySeconds],
        );
        let (performance_sender, performance_receiver) = watch::channel(live.clone());
        let mut app = App::new(
            model_receiver,
            HeaderInputs {
                performance: performance_receiver,
                ..HeaderInputs::default()
            },
        );

        for publication in [
            degraded.clone(),
            live.clone(),
            degraded.clone(),
            live.clone(),
        ] {
            performance_sender.send(publication.clone()).unwrap();
            assert!(app.refresh_if_changed().unwrap());
            let screen = render_at_width(&app, 140);
            let rendered_pair = (
                screen.contains("LIVE"),
                screen.contains("DEGRADED"),
                screen.contains("perf:events_60s"),
            );
            assert!(
                matches!(rendered_pair, (true, false, false) | (false, true, true)),
                "rendered a mixed coherent-publication pair: {screen}"
            );
            assert_eq!(app.performance, publication);
        }
    }

    #[test]
    fn summary_overlay_opens_closes_and_scrolls_with_its_keys() {
        let run = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let (mut app, _senders) = app_with_runtime(
            model_with_runs(&[(run, "run", 1, TaskState::Running)]),
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            TestClock::at(0),
        );

        app.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), Some(Overlay::Summary));
        assert_eq!(
            app.state().summary_scope(),
            SummaryScope::SelectionWorkspace
        );
        app.handle_key(KeyEvent::new(KeyCode::Char('w'), KeyModifiers::NONE));
        assert_eq!(app.state().summary_scope(), SummaryScope::Session);
        app.handle_key(KeyEvent::new(KeyCode::Char('w'), KeyModifiers::NONE));
        assert_eq!(
            app.state().summary_scope(),
            SummaryScope::SelectionWorkspace
        );
        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(app.state().overlay_scroll(), 1);
        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(app.state().overlay_scroll(), 0);
        app.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), None);

        app.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), Some(Overlay::Summary));
        assert_eq!(app.state().overlay_scroll(), 0);
        app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), None);
        app.handle_key(KeyEvent::new(KeyCode::Char('w'), KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), None);
    }

    #[test]
    fn summary_overlay_renders_header_and_one_placeholder_row_per_group() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let second = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let mut model = model_with_runs(&[
            (first, "hook:claude-code:S:task:A", 1, TaskState::Completed),
            (second, "native", 2, TaskState::Running),
        ]);
        let mut terminal = model.task_run(&first).unwrap().clone();
        terminal.created_at_ms = Some(1_000);
        terminal.finished_at_ms = Some(61_000);
        model.insert_task_run(terminal);
        let mut native = model.task_run(&second).unwrap().clone();
        native.key = RunKey::Native {
            provider: Provider::Codex,
            sid: "native".to_owned(),
        };
        model.insert_task_run(native);
        for (run_id, agent_node_id, model_id, activity_at) in [
            (first, "agent-alpha", "model-alpha", 10),
            (second, "agent-beta", "model-beta", 20),
        ] {
            model.insert_agent_node(AgentNode {
                agent_node_id: agent_node_id.to_owned(),
                provider: Provider::Codex,
                native_session_id: Some(agent_node_id.to_owned()),
                task_run_id: run_id,
                display_ordinal: DisplayOrdinal::new(activity_at),
                parent_agent_node_id: None,
                state: Some(ExecState::Working),
                model_id: Some(model_id.to_owned()),
                last_event_kind: None,
                last_tool_name: None,
                last_item_count: None,
                last_byte_count: None,
                last_activity_at_ms: Some(activity_at),
                session_file: None,
            });
        }
        let (mut app, _senders) = app_with_runtime(
            model,
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            TestClock::at(80_000),
        );

        app.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
        let lines = render_lines(&app, 160, 18);
        let screen = lines.join("\n");
        assert!(screen.contains(" Summary "));
        assert!(screen.contains("worker kind | runs | live | total | mean | tok | mean tok/s"));
        assert!(screen.contains("model | runs | live | total | mean | tok | mean tok/s"));
        let data_rows = lines
            .iter()
            .filter(|line| line.contains("claude-code |") || line.contains("Codex |"))
            .collect::<Vec<_>>();
        assert_eq!(data_rows.len(), 2, "{screen}");
        assert!(data_rows.iter().all(|line| line.contains(" | - | -")));
        assert!(screen.contains("claude-code | 1 | 0 | 01m00s | 01m00s | - | -"));
        assert!(screen.contains("Codex | 1 | 1 | 00s | - | - | -"));
        assert!(screen.contains("unknown | 2 | 1 | 01m00s | 01m00s | - | -"));
    }

    #[test]
    fn session_elapsed_header_uses_earliest_run_start_and_repaints_from_clock() {
        let run = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = model_with_runs(&[(run, "run", 1, TaskState::Completed)]);
        let mut timed = model.task_run(&run).unwrap().clone();
        timed.created_at_ms = Some(5_000);
        timed.finished_at_ms = Some(6_000);
        model.insert_task_run(timed);
        let clock = TestClock::at(3_605_000);
        let (app, _senders) = app_with_runtime(
            model,
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            clock.clone(),
        );

        let initial = render_lines(&app, 180, 18)
            .into_iter()
            .find(|line| line.contains("session:"))
            .unwrap();
        assert!(initial.contains(" | up:01:00:00 | "), "{initial}");

        clock.set(3_666_000);
        let repainted = render_lines(&app, 180, 18)
            .into_iter()
            .find(|line| line.contains("session:"))
            .unwrap();
        assert!(repainted.contains(" | up:01:01:01 | "), "{repainted}");
        assert!(!repainted.contains("up:01:00:00"));
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
                RunKey::Native { sid, .. } => sid.clone(),
                RunKey::NativePath { .. } => run_id.to_string(),
                RunKey::Provisional { terminal_id, .. } => terminal_id.clone(),
            })
            .collect()
    }

    #[derive(Debug, Default)]
    struct TestClock(AtomicI64);

    impl TestClock {
        fn at(now_ms: i64) -> Arc<Self> {
            Arc::new(Self(AtomicI64::new(now_ms)))
        }

        fn set(&self, now_ms: i64) {
            self.0.store(now_ms, Ordering::Relaxed);
        }
    }

    impl Clock for TestClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn healthy_diagnostics() -> RuntimeDiagnosticsSnapshot {
        RuntimeDiagnosticsSnapshot {
            persistence: PersistenceStatus::Healthy,
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

    fn empty_operator(terminal_times: HashMap<RunId, i64>) -> OperatorSnapshot {
        OperatorSnapshot {
            activity: Arc::from(Vec::new()),
            terminal_times: Arc::new(terminal_times),
        }
    }

    fn activity_for_run(run_id: RunId, timestamp: i64) -> ActivityItem {
        ActivityItem {
            identity: ActivityIdentity {
                event_id: format!("activity-{timestamp}"),
            },
            event_timestamp_ms: timestamp,
            seen_at_ms: timestamp,
            ingest_seq: Some(timestamp as u64),
            source: "provider".to_owned(),
            normalized_kind: "agent_activity".to_owned(),
            source_event_type: "item".to_owned(),
            workspace_id: Some("workspace".to_owned()),
            tab_id: Some("tab".to_owned()),
            pane_id: Some("pane".to_owned()),
            terminal_id: Some("terminal".to_owned()),
            provider: Some(Provider::Codex),
            native_session_id: None,
            task_run_id: Some(run_id),
            agent_node_id: None,
            task_state: Some(TaskState::Running),
            model_id: None,
            provider_event_kind: Some("assistant".to_owned()),
            tool_name: None,
            item_count: None,
            byte_count: None,
            provider_agent_id: None,
            provider_parent_agent_id: None,
            controller_label: None,
            controller_reason: None,
            durability: ActivityDurability::Durable,
        }
    }

    struct RuntimeSenders {
        model: watch::Sender<Arc<DomainModel>>,
        _performance: watch::Sender<PerformancePublication>,
        diagnostics: watch::Sender<RuntimeDiagnosticsSnapshot>,
        operator: watch::Sender<OperatorSnapshot>,
    }

    fn app_with_runtime(
        model: DomainModel,
        operator: OperatorSnapshot,
        setup: TuiSetup,
        clock: Arc<dyn Clock>,
    ) -> (App, RuntimeSenders) {
        let (model_sender, model_receiver) = watch::channel(Arc::new(model));
        let (performance_sender, performance) =
            watch::channel(performance_publication(ObservationQuality::Live, []));
        let (diagnostics_sender, diagnostics_receiver) = watch::channel(healthy_diagnostics());
        let (operator_sender, operator_receiver) = watch::channel(operator);
        let app = App::with_inputs(
            model_receiver,
            HeaderInputs {
                session: "session".to_owned(),
                performance,
                ..HeaderInputs::default()
            },
            diagnostics_receiver,
            operator_receiver,
            setup,
            clock,
        );
        (
            app,
            RuntimeSenders {
                model: model_sender,
                _performance: performance_sender,
                diagnostics: diagnostics_sender,
                operator: operator_sender,
            },
        )
    }

    #[derive(Debug)]
    struct RecordingVersionRunner {
        result: VersionProbeResult,
        commands: Mutex<Vec<VersionCommand>>,
    }

    impl RecordingVersionRunner {
        fn new(result: VersionProbeResult) -> Self {
            Self {
                result,
                commands: Mutex::new(Vec::new()),
            }
        }
    }

    impl VersionCommandRunner for RecordingVersionRunner {
        fn run(&self, command: VersionCommand) -> VersionProbeResult {
            self.commands.lock().unwrap().push(command);
            self.result.clone()
        }
    }

    #[derive(Debug, Default)]
    struct FakeNoticeState {
        attempted: bool,
        inspect_calls: usize,
    }

    #[derive(Debug)]
    struct FakeNoticeBoundary {
        publish_ok: bool,
        current_after_attempt: bool,
        state: Mutex<FakeNoticeState>,
    }

    impl FakeNoticeBoundary {
        fn new(publish_ok: bool, current_after_attempt: bool) -> Arc<Self> {
            Arc::new(Self {
                publish_ok,
                current_after_attempt,
                state: Mutex::new(FakeNoticeState::default()),
            })
        }
    }

    impl NoticePublicationBoundary for FakeNoticeBoundary {
        fn inspect(&self, _state_base: &std::path::Path, _package_version: &str) -> NoticeVerdict {
            let mut state = self.state.lock().unwrap();
            state.inspect_calls += 1;
            if state.attempted && self.current_after_attempt {
                NoticeVerdict::Current {
                    dismissed_at_ms: 123,
                }
            } else {
                NoticeVerdict::Absent
            }
        }

        fn publish(
            &self,
            _state_base: &std::path::Path,
            _package_version: &str,
            _dismissed_at_ms: i64,
        ) -> Result<(), ()> {
            self.state.lock().unwrap().attempted = true;
            self.publish_ok.then_some(()).ok_or(())
        }
    }

    fn type_text(app: &mut App, value: &str) {
        for character in value.chars() {
            assert_eq!(
                app.handle_key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE)),
                LoopControl::Continue
            );
        }
    }

    #[test]
    fn i4_filter_draft_commit_cancel_clear_and_safe_fields() {
        let private = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let searchable = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let mut model = model_with_runs(&[
            (
                private,
                "CONTROLLER_PRIVATE_FREE_TEXT",
                1,
                TaskState::Running,
            ),
            (searchable, "display", 2, TaskState::Running),
        ]);
        model.insert_task_run(TaskRun {
            run_id: searchable,
            key: RunKey::Native {
                provider: Provider::Codex,
                sid: "Native-Identifier".to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(2),
            state: TaskState::Running,
            has_controller_task_state_event: false,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "agent-safe".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("native  cafe\u{301}".to_owned()),
            task_run_id: searchable,
            display_ordinal: DisplayOrdinal::new(3),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: Some("GPT-SAFE".to_owned()),
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: Some("/private/SESSION_FILE_FORBIDDEN".to_owned()),
        });
        let (mut app, _senders) = app_with_runtime(
            model,
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            TestClock::at(0),
        );

        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        type_text(&mut app, "  GpT-SaFe  ");
        assert_eq!(app.state().filter_draft(), Some("  GpT-SaFe  "));
        app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(app.state().filter_query(), "");
        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        type_text(&mut app, "  GpT-SaFe  ");
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(app.state().filter_query(), "GpT-SaFe");
        assert_eq!(displayed_run_names(&app), ["Native-Identifier"]);
        assert!(!app.is_following());

        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        for _ in 0..app.state().filter_query().chars().count() {
            app.handle_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        }
        type_text(&mut app, "   ");
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(app.state().filter_query(), "");

        for forbidden in [
            "CONTROLLER_PRIVATE_FREE_TEXT",
            "SESSION_FILE_FORBIDDEN",
            "No activity event feed",
            ".*",
            "native cafe\u{301}",
            "native  café",
        ] {
            app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
            type_text(&mut app, forbidden);
            app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
            assert!(
                displayed_run_names(&app).is_empty(),
                "unsafe field matched {forbidden}"
            );
            app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
            for _ in 0..app.state().filter_query().chars().count() {
                app.handle_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
            }
            app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        }

        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        type_text(&mut app, "native  cafe\u{301}");
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(displayed_run_names(&app), ["Native-Identifier"]);
    }

    #[test]
    fn i4_session_named_projection_is_shared_by_render_and_navigation() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let second = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let (_model_sender, model_receiver) = watch::channel(Arc::new(model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (second, "second", 2, TaskState::Running),
        ])));
        let session_name = "named-session-i4-a3";
        let mut app = App::new(
            model_receiver,
            HeaderInputs {
                session: session_name.to_owned(),
                ..HeaderInputs::default()
            },
        );

        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        type_text(&mut app, session_name);
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let navigable = view::build_rows(app.model(), app.state());
        assert_eq!(
            navigable.iter().map(|row| &row.key).collect::<Vec<_>>(),
            [&NodeKey::Session],
            "the named row rendered by the TUI must be the same row interaction projects"
        );
        let screen = render(&app);
        assert!(screen.contains("Session: named-session-i4-a3"));
        assert!(
            app.state()
                .selected()
                .is_some_and(|selected| navigable.iter().any(|row| &row.key == selected))
        );

        app.handle_key(KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE));
        assert_eq!(app.state().selected(), navigable.last().map(|row| &row.key));
        app.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert_eq!(app.state().selected(), navigable.last().map(|row| &row.key));
    }

    #[test]
    fn i4_view_toggle_follow_manual_and_empty_matrix() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let last = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let input = [
            (first, "first", 1, TaskState::Running),
            (last, "last", 2, TaskState::Running),
        ];

        let (mut following, _) = app_with_model(model_with_runs(&input));
        following.set_selection(Some(NodeKey::Run {
            run_id: first,
            pane_id: Some("pane".to_owned()),
        }));
        assert!(following.is_following());
        following.toggle_view();
        assert!(following.is_following());
        assert_eq!(following.selected_run_id(), Some(last));
        assert_eq!(following.state().selection_reason(), None);

        let (mut empty, _) = app_with_model(model_with_runs(&input));
        empty.state.filter_query = "session".to_owned();
        empty.resume_follow();
        assert_eq!(empty.state().selected(), Some(&NodeKey::Session));
        empty.toggle_view();
        assert!(empty.is_following());
        assert_eq!(empty.state().selected(), None);
        assert_eq!(empty.state().selection_reason(), None);

        let (mut manual_semantic, _) = app_with_model(model_with_runs(&input));
        manual_semantic.state.follow = false;
        manual_semantic.set_selection(Some(NodeKey::Run {
            run_id: first,
            pane_id: Some("pane".to_owned()),
        }));
        manual_semantic.toggle_view();
        assert!(!manual_semantic.is_following());
        assert_eq!(manual_semantic.selected_run_id(), Some(first));
        assert_eq!(manual_semantic.state().selection_reason(), None);

        let (mut manual_missing, _) = app_with_model(model_with_runs(&input));
        manual_missing.state.follow = false;
        manual_missing.set_selection(Some(NodeKey::Workspace("workspace".to_owned())));
        manual_missing.toggle_view();
        assert!(!manual_missing.is_following());
        assert_eq!(manual_missing.selected_run_id(), Some(first));
        assert_eq!(manual_missing.state().selection_reason(), None);
    }

    #[test]
    fn i4_shared_occurrence_recovery_prefers_merge_then_ancestor() {
        let shared = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let sibling = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let selected_pane_one = NodeKey::Run {
            run_id: shared,
            pane_id: Some("pane-1".to_owned()),
        };

        let (mut collapsed, _) = app_with_model(shared_occurrence_model(
            shared,
            sibling,
            TaskState::Running,
            true,
        ));
        collapsed.state.follow = false;
        collapsed.set_selection(Some(selected_pane_one.clone()));
        collapsed.toggle_collapse(NodeKey::Pane("pane-1".to_owned()));
        assert_eq!(
            collapsed.state().selected(),
            Some(&NodeKey::Pane("pane-1".to_owned()))
        );
        assert_eq!(collapsed.state().selection_reason(), Some("collapsed"));

        let (mut filtered, _) = app_with_model(shared_occurrence_model(
            shared,
            sibling,
            TaskState::Running,
            true,
        ));
        filtered.state.follow = false;
        filtered.set_selection(Some(selected_pane_one.clone()));
        filtered.toggle_collapse(NodeKey::Pane("pane-1".to_owned()));
        assert!(
            filtered
                .state()
                .is_collapsed(&NodeKey::Pane("pane-1".to_owned()))
        );
        filtered.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        type_text(&mut filtered, "shared");
        filtered.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        filtered.set_selection(Some(selected_pane_one.clone()));
        assert_eq!(filtered.state().selected(), Some(&selected_pane_one));
        filtered.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        for _ in "shared".chars() {
            filtered.handle_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        }
        filtered.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(
            filtered.state().selected(),
            Some(&NodeKey::Pane("pane-1".to_owned())),
            "restored P1 collapse must recover to its visible ancestor, not P2"
        );
        assert_eq!(filtered.state().selection_reason(), Some("filtered"));
        assert_ne!(
            filtered.state().selected(),
            Some(&NodeKey::Run {
                run_id: shared,
                pane_id: Some("pane-2".to_owned()),
            })
        );
        assert_ne!(filtered.state().selected(), Some(&NodeKey::Session));

        let (mut closed, closed_sender) = app_with_model(shared_occurrence_model(
            shared,
            sibling,
            TaskState::Running,
            true,
        ));
        closed.state.follow = false;
        closed.set_selection(Some(selected_pane_one.clone()));
        closed_sender
            .send(Arc::new(shared_occurrence_model(
                shared,
                sibling,
                TaskState::Running,
                false,
            )))
            .unwrap();
        closed.refresh();
        assert_eq!(
            closed.state().selected(),
            Some(&NodeKey::Pane("pane-1".to_owned())),
            "closed occurrence ancestor outranks surviving sibling and another occurrence"
        );
        assert_eq!(closed.state().selection_reason(), Some("closed"));

        let clock = TestClock::at(activity::DEFAULT_TERMINAL_VISIBILITY_MS - 1);
        let (mut aged, _aged_senders) = app_with_runtime(
            shared_occurrence_model(shared, sibling, TaskState::Completed, true),
            empty_operator(HashMap::from([(shared, 0)])),
            TuiSetup::default(),
            clock.clone(),
        );
        aged.state.follow = false;
        aged.set_selection(Some(selected_pane_one.clone()));
        clock.set(activity::DEFAULT_TERMINAL_VISIBILITY_MS);
        assert!(aged.refresh_if_changed().unwrap());
        assert_eq!(
            aged.state().selected(),
            Some(&NodeKey::Pane("pane-1".to_owned())),
            "aged-out ancestor outranks the stable sibling"
        );
        assert_eq!(aged.state().selection_reason(), Some("aged_out"));

        let absorbed = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let survivor = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAY");
        let absorbed_key = RunKey::Controller("absorbed".to_owned());
        let (mut merged, merged_sender) = app_with_model(model_with_runs(&[
            (absorbed, "absorbed", 1, TaskState::Running),
            (survivor, "survivor", 2, TaskState::Running),
        ]));
        merged.state.follow = false;
        merged.set_selection(Some(NodeKey::Run {
            run_id: absorbed,
            pane_id: Some("pane".to_owned()),
        }));
        let mut merged_model = model_with_runs(&[(survivor, "survivor", 2, TaskState::Running)]);
        merged_model.insert_task_run_alias(absorbed_key, survivor);
        merged_sender.send(Arc::new(merged_model)).unwrap();
        merged.refresh();
        assert_eq!(merged.selected_run_id(), Some(survivor));
        assert_eq!(merged.state().selection_reason(), Some("merged"));

        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAZ");
        let removed = run_id("01ARZ3NDEKTSV4RRFFQ69G5FB0");
        let next = run_id("01ARZ3NDEKTSV4RRFFQ69G5FB1");
        let (mut disappeared, disappeared_sender) = app_with_model(model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (removed, "removed", 2, TaskState::Running),
            (next, "next", 3, TaskState::Running),
        ]));
        disappeared.state.follow = false;
        disappeared.set_selection(Some(NodeKey::Run {
            run_id: removed,
            pane_id: Some("pane".to_owned()),
        }));
        disappeared.toggle_view();
        disappeared_sender
            .send(Arc::new(model_with_runs(&[
                (first, "first", 1, TaskState::Running),
                (next, "next", 3, TaskState::Running),
            ])))
            .unwrap();
        disappeared.refresh();
        assert!(!disappeared.is_following());
        assert_eq!(disappeared.selected_run_id(), Some(next));
        assert_eq!(disappeared.state().selection_reason(), Some("closed"));
    }

    #[test]
    fn i4_collapse_occurrences_filter_override_and_ancestor_recovery() {
        let run = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = model_with_runs(&[(run, "run", 1, TaskState::Running)]);
        model.insert_agent_node(AgentNode {
            agent_node_id: "agent-child".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("filter-me".to_owned()),
            task_run_id: run,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        });
        let (mut app, _senders) = app_with_runtime(
            model,
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            TestClock::at(0),
        );
        let pane = NodeKey::Pane("pane".to_owned());
        let agent = NodeKey::Agent {
            agent_node_id: "agent-child".to_owned(),
            pane_id: Some("pane".to_owned()),
        };
        app.state.follow = false;
        app.set_selection(Some(agent.clone()));
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(
            app.selected_run_id(),
            Some(run),
            "Agent selection preserves its semantic run in DAG"
        );
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        app.set_selection(Some(agent.clone()));
        app.toggle_collapse(pane.clone());
        assert_eq!(app.state().selected(), Some(&pane));
        assert_eq!(app.state().selection_reason(), Some("collapsed"));
        assert!(app.state().is_collapsed(&pane));
        assert!(
            !view::build_rows(app.model(), app.state())
                .iter()
                .any(|item| item.key == agent)
        );

        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        type_text(&mut app, "filter-me");
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(
            view::build_rows(app.model(), app.state())
                .iter()
                .any(|item| item.key == agent)
        );
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(
            app.state().is_collapsed(&pane),
            "collapse input is ignored under filter"
        );

        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        for _ in 0.."filter-me".len() {
            app.handle_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        }
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.state().is_collapsed(&pane));
        app.set_selection(Some(pane.clone()));
        app.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
        assert!(!app.state().is_collapsed(&pane));
        app.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
        assert!(matches!(
            app.state().selected(),
            Some(NodeKey::Run { .. }) | Some(NodeKey::Tab(_))
        ));
        app.set_selection(Some(pane.clone()));
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.state().is_collapsed(&pane));
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        let stored = app.state().collapsed_len();
        assert_eq!(stored, 1);
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(
            app.state().collapsed_len(),
            stored,
            "DAG has no collapse semantics"
        );
        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(app.state().collapsed_len(), stored);
    }

    #[test]
    fn i4_help_notice_modal_refresh_and_safe_dismissal() {
        let directory = tempfile::tempdir().unwrap();
        let runner = RecordingVersionRunner::new(VersionProbeResult::Available {
            version: "999.0.0".to_owned(),
            stderr_present: false,
        });
        let setup = TuiSetup::for_owner(
            directory.path().to_path_buf(),
            Some(OsString::from("/home/test")),
            &runner,
        );
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let second = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let (mut app, senders) = app_with_runtime(
            model_with_runs(&[(first, "first", 1, TaskState::Running)]),
            empty_operator(HashMap::new()),
            setup,
            TestClock::at(10),
        );
        assert_eq!(app.state().overlay(), Some(Overlay::Notice));
        assert_eq!(
            app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE)),
            LoopControl::Continue
        );
        senders
            .model
            .send(Arc::new(model_with_runs(&[(
                second,
                "second",
                2,
                TaskState::Running,
            )])))
            .unwrap();
        let mut refreshed_diagnostics = healthy_diagnostics();
        refreshed_diagnostics.dangling_announcement_components = 9;
        senders.diagnostics.send(refreshed_diagnostics).unwrap();
        assert!(app.refresh_if_changed().unwrap());
        assert_eq!(app.selected_run_id(), Some(second));
        assert_eq!(app.diagnostics.dangling_announcement_components, 9);
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), None);
        assert!(
            crate::diagnostics::local::notice_path(directory.path(), env!("CARGO_PKG_VERSION"))
                .exists()
        );

        app.handle_key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), Some(Overlay::Help));
        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(app.state().overlay_scroll(), 1);
        app.handle_key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), None);
        app.handle_key(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), Some(Overlay::Detail));
        app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), None);

        let blocked_base = directory.path().join("blocked");
        std::fs::write(&blocked_base, b"not a directory").unwrap();
        let failed_setup = TuiSetup::for_owner(blocked_base, None, &runner);
        let (mut failed, _senders) = app_with_runtime(
            DomainModel::default(),
            empty_operator(HashMap::new()),
            failed_setup,
            TestClock::at(11),
        );
        failed.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(failed.state().overlay(), None);
        assert_eq!(
            failed.state().safe_warning(),
            Some("notice_marker_write_failed")
        );
        assert!(!render(&failed).contains("blocked"));
    }

    #[test]
    fn i4_help_renders_and_refreshes_complete_runtime_diagnostics() {
        let setup = TuiSetup {
            standalone_status: Some(StandaloneVersionStatus::Mismatch {
                version: "9.9.9".to_owned(),
                stderr_present: true,
            }),
            ..TuiSetup::default()
        };
        let run = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let (mut app, senders) = app_with_runtime(
            model_with_runs(&[(run, "run", 1, TaskState::Running)]),
            empty_operator(HashMap::new()),
            setup,
            TestClock::at(0),
        );
        app.handle_key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
        assert_eq!(app.state().overlay(), Some(Overlay::Help));

        let complete = RuntimeDiagnosticsSnapshot {
            persistence: PersistenceStatus::Degraded {
                failure: PersistenceFailure {
                    operation: PersistenceOperation::Cleanup,
                    phase: PersistencePhase::PostApplyCommit,
                    code: PersistenceFailureCode::Sqlite,
                    durability: DurabilityDisposition::Committed,
                },
            },
            controller_input: ControllerInputStatus::Unavailable {
                reason: ControllerInputUnavailableReason::PersistenceUnavailable,
            },
            owner: OwnerFreshness::Stale,
            persistence_counters: PersistenceCounters {
                not_committed_batches: 1,
                durability_unknown_batches: 2,
                committed_but_degraded_batches: 3,
                skipped_batches: 4,
                skipped_owner_updates: 5,
            },
            controller_counters: ControllerCounterSnapshot {
                binding_conflicts: 11,
                terminal_blocked_progress_noops: 12,
                terminal_forward_reference_creations: 13,
                dangling_announcement_components: 19,
                ingest_sequence_exhaustions: 14,
                provider_parent_conflicts: 15,
                provider_identity_disagreements: 16,
                socket_saturations: 17,
                accept_failures: 18,
            },
            enrichment_counters: crate::diagnostics::EnrichmentCounterSnapshot::default(),
            provider_counters: crate::diagnostics::ProviderCounterSnapshot::default(),
            source_coverage: vec![
                SourceCoverageSnapshot {
                    source: DiagnosticSource::Herdr,
                    availability: InputAvailability::Available,
                },
                SourceCoverageSnapshot {
                    source: DiagnosticSource::Controller,
                    availability: InputAvailability::Partial,
                },
                SourceCoverageSnapshot {
                    source: DiagnosticSource::Claude,
                    availability: InputAvailability::Unavailable,
                },
                SourceCoverageSnapshot {
                    source: DiagnosticSource::Codex,
                    availability: InputAvailability::Available,
                },
            ],
            dangling_announcement_components: 19,
            first_failure_log: OccurrenceLogStatus::Failed,
        };
        senders.diagnostics.send(complete).unwrap();
        assert!(app.refresh_if_changed().unwrap());
        assert_eq!(app.state().overlay(), Some(Overlay::Help));

        let mut transcript = String::new();
        for _ in 0..40 {
            transcript.push_str(&render_lines(&app, 220, 24).join("\n"));
            transcript.push('\n');
            app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        }
        for expected in [
            "persistence: degraded operation=cleanup phase=post_apply_commit code=sqlite durability=committed",
            "controller: unavailable reason=persistence_unavailable",
            "owner: stale",
            "persistence counters: not_committed=1 durability_unknown=2 committed_but_degraded=3 skipped=4 skipped_owner_updates=5",
            "controller counters: binding_conflicts=11 terminal_blocked_progress_noops=12 terminal_forward_reference_creations=13",
            "ingest_sequence_exhaustions=14 provider_parent_conflicts=15 provider_identity_disagreements=16 socket_saturations=17 accept_failures=18",
            "source Herdr: available",
            "source Controller: partial",
            "source Claude: unavailable",
            "source Codex: available",
            "occurrence log: failed",
            "D4: 19",
            "standalone probe: mismatch 9.9.9 stderr_present=true",
        ] {
            assert!(
                transcript.contains(expected),
                "missing Help fact {expected:?}"
            );
        }
        assert!(!transcript.contains("PRIVATE"));
        assert!(!transcript.contains("controller_dangling"));

        for _ in 0..200 {
            app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        }
        let bounded = render_lines(&app, 220, 24).join("\n");
        assert!(
            bounded.contains("standalone probe: mismatch 9.9.9 stderr_present=true"),
            "Help scrolling must clamp at the last nonempty window"
        );
    }

    #[test]
    fn i4_help_and_detail_scroll_store_bounded_bidirectional_offsets() {
        let run = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let (mut help, _) = app_with_runtime(
            model_with_runs(&[(run, "run", 1, TaskState::Running)]),
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            TestClock::at(0),
        );
        help.handle_key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
        for _ in 0..100 {
            help.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        }
        assert_eq!(help.state().overlay_scroll(), 100);
        let help_bottom = render_lines(&help, 100, 14);
        let help_max = help.state().overlay_scroll();
        assert!(
            help_max < 100,
            "render must normalize the stored Help offset"
        );
        assert!(help_max > 0);
        help.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(help.state().overlay_scroll(), help_max);
        help.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(help.state().overlay_scroll(), help_max - 1);
        assert_ne!(render_lines(&help, 100, 14), help_bottom);

        let initial_activity = (0..30)
            .map(|timestamp| activity_for_run(run, timestamp))
            .collect::<Vec<_>>();
        let (mut detail, senders) = app_with_runtime(
            model_with_runs(&[(run, "run", 1, TaskState::Running)]),
            OperatorSnapshot {
                activity: Arc::from(initial_activity),
                terminal_times: Arc::new(HashMap::new()),
            },
            TuiSetup::default(),
            TestClock::at(0),
        );
        detail.handle_key(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE));
        for _ in 0..200 {
            detail.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        }
        assert_eq!(detail.state().overlay_scroll(), 200);
        let _ = render_lines(&detail, 100, 14);
        let original_detail_max = detail.state().overlay_scroll();
        assert!(original_detail_max < 200);

        let reduced_activity = (0..10)
            .map(|timestamp| activity_for_run(run, timestamp))
            .collect::<Vec<_>>();
        senders
            .operator
            .send(OperatorSnapshot {
                activity: Arc::from(reduced_activity),
                terminal_times: Arc::new(HashMap::new()),
            })
            .unwrap();
        assert!(detail.refresh_if_changed().unwrap());
        let detail_bottom = render_lines(&detail, 100, 14);
        let reduced_detail_max = detail.state().overlay_scroll();
        assert!(
            reduced_detail_max < original_detail_max,
            "live Detail shrink must normalize the stored offset"
        );
        detail.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(detail.state().overlay_scroll(), reduced_detail_max);
        detail.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(detail.state().overlay_scroll(), reduced_detail_max - 1);
        assert_ne!(render_lines(&detail, 100, 14), detail_bottom);
    }

    #[test]
    fn i4_notice_owner_only_exact_version_matrix() {
        let directory = tempfile::tempdir().unwrap();
        let exact = RecordingVersionRunner::new(VersionProbeResult::Available {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            stderr_present: true,
        });
        let exact_setup = TuiSetup::for_owner(directory.path().to_path_buf(), None, &exact);
        assert!(!exact_setup.notice_visible());
        assert_eq!(
            *exact.commands.lock().unwrap(),
            [VersionCommand::Standalone]
        );

        for result in [
            VersionProbeResult::Available {
                version: "9.9.9".to_owned(),
                stderr_present: false,
            },
            VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::NotFound,
            },
            VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::ExecutionFailed,
            },
        ] {
            let base = tempfile::tempdir().unwrap();
            let runner = RecordingVersionRunner::new(result);
            let setup = TuiSetup::for_owner(base.path().to_path_buf(), None, &runner);
            assert!(setup.notice_visible());
            assert_eq!(
                *runner.commands.lock().unwrap(),
                [VersionCommand::Standalone]
            );
        }

        crate::diagnostics::local::publish_notice(directory.path(), env!("CARGO_PKG_VERSION"), 1)
            .unwrap();
        let mismatch = RecordingVersionRunner::new(VersionProbeResult::Available {
            version: "9.9.9".to_owned(),
            stderr_present: false,
        });
        let dismissed = TuiSetup::for_owner(directory.path().to_path_buf(), None, &mismatch);
        assert!(!dismissed.notice_visible());
        assert_eq!(
            *mismatch.commands.lock().unwrap(),
            [VersionCommand::Standalone]
        );
        assert!(
            !TuiSetup::default().notice_visible(),
            "non-owner/default setup never probes or notices"
        );
    }

    #[test]
    fn tui_setup_ascii_tree_defaults_false_and_can_be_enabled() {
        let setup = TuiSetup::default();
        assert!(!setup.ascii_tree());
        assert!(setup.with_ascii_tree(true).ascii_tree());
    }

    #[test]
    fn i4_notice_post_publish_sync_unknown_and_pre_publish_failure() {
        let runner = RecordingVersionRunner::new(VersionProbeResult::Available {
            version: "9.9.9".to_owned(),
            stderr_present: false,
        });
        for (publish_ok, current_after_attempt, expected_warning, reappears) in [
            (true, true, None, false),
            (false, true, Some("notice_marker_sync_unconfirmed"), false),
            (false, false, Some("notice_marker_write_failed"), true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let private_base = directory.path().join("PRIVATE_NOTICE_PATH_I4_A7");
            std::fs::create_dir(&private_base).unwrap();
            let boundary = FakeNoticeBoundary::new(publish_ok, current_after_attempt);
            let setup = TuiSetup::for_owner_with_notice(
                private_base.clone(),
                None,
                &runner,
                boundary.clone(),
            );
            assert!(setup.notice_visible());
            let (mut app, _) = app_with_runtime(
                DomainModel::default(),
                empty_operator(HashMap::new()),
                setup,
                TestClock::at(10),
            );

            app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
            assert_eq!(app.state().overlay(), None);
            assert_eq!(app.state().safe_warning(), expected_warning);
            let screen = render(&app);
            assert!(!screen.contains("PRIVATE_NOTICE_PATH_I4_A7"));
            if expected_warning == Some("notice_marker_sync_unconfirmed") {
                assert!(!screen.contains("notice_marker_write_failed"));
            }

            let repeated =
                TuiSetup::for_owner_with_notice(private_base, None, &runner, boundary.clone());
            assert_eq!(repeated.notice_visible(), reappears);
            let state = boundary.state.lock().unwrap();
            assert_eq!(
                state.inspect_calls,
                if publish_ok { 2 } else { 3 },
                "Ok skips post-publish inspection; Err inspects immediately"
            );
        }
    }

    #[test]
    fn i4_idle_deadline_cache_avoids_projection_rebuilds() {
        let terminal = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let live = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let deadline = activity::DEFAULT_TERMINAL_VISIBILITY_MS + 1_000;
        let clock = TestClock::at(1_000);
        let (mut app, _senders) = app_with_runtime(
            model_with_runs(&[
                (terminal, "terminal", 1, TaskState::Completed),
                (live, "live", 2, TaskState::Running),
            ]),
            empty_operator(HashMap::from([(terminal, 1_000)])),
            TuiSetup::default(),
            clock.clone(),
        );
        assert_eq!(app.next_expiry_ms(), Some(deadline));
        app.reset_projection_build_count();
        let limiter = FrameLimiter::default();
        for _ in 0..25 {
            assert!(!app.refresh_if_changed().unwrap());
            assert!(app.poll_duration(&limiter, false, Duration::ZERO) > Duration::ZERO);
        }
        assert_eq!(app.projection_build_count(), 0);

        clock.set(deadline);
        assert_eq!(
            app.poll_duration(&limiter, false, Duration::ZERO),
            Duration::ZERO
        );
        assert!(app.refresh_if_changed().unwrap());
        assert_eq!(displayed_run_names(&app), ["live"]);
        assert_eq!(app.next_expiry_ms(), None);
        let deadline_builds = app.projection_build_count();
        assert!(deadline_builds > 0);

        for _ in 0..25 {
            assert!(!app.refresh_if_changed().unwrap());
            let _ = app.poll_duration(&limiter, false, Duration::ZERO);
        }
        assert_eq!(app.projection_build_count(), deadline_builds);
    }

    #[test]
    fn idle_without_visible_live_duration_never_rebuilds_or_zero_polls() {
        let live = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let clock = TestClock::at(0);
        let (mut app, _senders) = app_with_runtime(
            model_with_runs(&[(live, "live", 1, TaskState::Running)]),
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            clock.clone(),
        );
        assert_eq!(app.next_expiry_ms(), None);
        app.reset_projection_build_count();
        let limiter = FrameLimiter::default();

        for now_ms in (1..=25).map(|seconds| seconds * 1_000) {
            clock.set(now_ms);
            assert!(!app.refresh_if_changed().unwrap());
            assert!(app.poll_duration(&limiter, false, Duration::ZERO) > Duration::ZERO);
        }
        assert_eq!(app.projection_build_count(), 0);
    }

    #[test]
    fn paint_ticks_advance_elapsed_without_rebuilding_rows_when_no_live_duration_exists() {
        let terminal = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let clock = TestClock::at(1_000);
        let (mut app, _senders) = app_with_runtime(
            model_with_runs(&[(terminal, "terminal", 1, TaskState::Completed)]),
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            clock.clone(),
        );
        assert_eq!(app.next_expiry_ms(), None);
        app.reset_projection_build_count();

        for (now_ms, expected) in [
            (2_000, "up:00:00:01"),
            (3_000, "up:00:00:02"),
            (4_000, "up:00:00:03"),
        ] {
            clock.set(now_ms);
            assert!(app.paint_tick_due());
            assert!(!app.refresh_if_changed().unwrap());
            assert!(render_at_width(&app, 160).contains(expected));
        }
        assert_eq!(app.projection_build_count(), 0);
    }

    #[test]
    fn live_duration_deadline_refreshes_through_cached_watch_path_and_advances_label() {
        let live = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = model_with_runs(&[(live, "live", 1, TaskState::Running)]);
        let mut timed = model.task_run(&live).unwrap().clone();
        timed.created_at_ms = Some(0);
        timed.updated_at_ms = Some(0);
        timed.subject = Some("Visible work".to_owned());
        model.insert_task_run(timed);
        let clock = TestClock::at(7_000);
        let (mut app, senders) = app_with_runtime(
            model,
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            clock.clone(),
        );
        let run_label = |app: &App| {
            view::build_rows(app.model(), app.state())
                .into_iter()
                .find(|row| row.key.run_id() == Some(live))
                .unwrap()
                .label
        };
        assert!(run_label(&app).contains(" · 07s"));
        assert_eq!(app.next_expiry_ms(), Some(8_000));
        app.reset_projection_build_count();

        senders
            .operator
            .send(empty_operator(HashMap::new()))
            .unwrap();
        assert!(app.operator_receiver.has_changed().unwrap());
        clock.set(8_000);
        assert!(app.refresh_if_changed().unwrap());
        assert_eq!(app.projection_build_count(), 1);
        assert!(!app.operator_receiver.has_changed().unwrap());
        assert!(run_label(&app).contains(" · 08s"));
        assert_eq!(app.next_expiry_ms(), Some(9_000));
        assert!(
            app.poll_duration(&FrameLimiter::default(), false, Duration::ZERO) > Duration::ZERO,
            "the refreshed duration deadline must be strictly in the future"
        );
    }

    #[test]
    fn expanding_collapsed_live_run_rearms_duration_without_watch_traffic() {
        let live = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = model_with_runs(&[(live, "live", 1, TaskState::Running)]);
        let mut timed = model.task_run(&live).unwrap().clone();
        timed.created_at_ms = Some(0);
        timed.updated_at_ms = Some(0);
        timed.subject = Some("Visible work".to_owned());
        model.insert_task_run(timed);
        let clock = TestClock::at(7_000);
        let (mut app, _senders) = app_with_runtime(
            model,
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            clock.clone(),
        );
        let pane = NodeKey::Pane("pane".to_owned());

        app.toggle_collapse(pane.clone());
        assert_eq!(app.next_expiry_ms(), None);

        app.toggle_collapse(pane);
        assert_eq!(app.next_expiry_ms(), Some(8_000));
        let label = view::build_rows(app.model(), app.state())
            .into_iter()
            .find(|row| row.key.run_id() == Some(live))
            .unwrap()
            .label;
        assert!(label.contains(" · 07s"));

        clock.set(8_000);
        assert!(app.refresh_if_changed().unwrap());
        let label = view::build_rows(app.model(), app.state())
            .into_iter()
            .find(|row| row.key.run_id() == Some(live))
            .unwrap()
            .label;
        assert!(label.contains(" · 08s"));
    }

    #[test]
    fn toggling_view_to_reveal_live_run_rearms_duration_without_watch_traffic() {
        let live = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut model = model_with_runs(&[(live, "live", 1, TaskState::Running)]);
        let mut timed = model.task_run(&live).unwrap().clone();
        timed.created_at_ms = Some(0);
        timed.updated_at_ms = Some(0);
        timed.subject = Some("Visible work".to_owned());
        model.insert_task_run(timed);
        let clock = TestClock::at(7_000);
        let (mut app, _senders) = app_with_runtime(
            model,
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            clock.clone(),
        );

        app.toggle_collapse(NodeKey::Pane("pane".to_owned()));
        assert_eq!(app.next_expiry_ms(), None);

        app.toggle_view();
        assert_eq!(app.next_expiry_ms(), Some(8_000));
        let label = view::build_rows(app.model(), app.state())
            .into_iter()
            .find(|row| row.key.run_id() == Some(live))
            .unwrap()
            .label;
        assert!(label.contains(" · 07s"));

        clock.set(8_000);
        assert!(app.refresh_if_changed().unwrap());
        let label = view::build_rows(app.model(), app.state())
            .into_iter()
            .find(|row| row.key.run_id() == Some(live))
            .unwrap()
            .label;
        assert!(label.contains(" · 08s"));
    }

    #[test]
    fn phase_distinct_live_runs_refresh_once_per_wall_aligned_second() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let second = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let third = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let mut model = model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (second, "second", 2, TaskState::Running),
            (third, "third", 3, TaskState::Running),
        ]);
        for (run_id, created_at_ms) in [(first, 0), (second, 250), (third, 900)] {
            let mut timed = model.task_run(&run_id).unwrap().clone();
            timed.created_at_ms = Some(created_at_ms);
            timed.updated_at_ms = Some(created_at_ms);
            model.insert_task_run(timed);
        }
        let clock = TestClock::at(1_500);
        let (mut app, _senders) = app_with_runtime(
            model,
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            clock.clone(),
        );
        assert_eq!(app.next_expiry_ms(), Some(2_000));

        let mut refreshes = 0;
        for now_ms in (1_600..=2_500).step_by(100) {
            clock.set(now_ms);
            refreshes += usize::from(app.refresh_if_changed().unwrap());
        }

        assert_eq!(refreshes, 1);
        assert_eq!(app.next_expiry_ms(), Some(3_000));
    }

    #[test]
    fn live_duration_bounds_poll_and_terminal_transition_disarms_cadence() {
        let live = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let mut live_model = model_with_runs(&[(live, "live", 1, TaskState::Running)]);
        let mut timed = live_model.task_run(&live).unwrap().clone();
        timed.created_at_ms = Some(0);
        timed.updated_at_ms = Some(0);
        live_model.insert_task_run(timed);
        let clock = TestClock::at(7_000);
        let (mut app, senders) = app_with_runtime(
            live_model.clone(),
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            clock.clone(),
        );
        let limiter = FrameLimiter::default();

        assert_eq!(app.next_expiry_ms(), Some(8_000));
        let poll = app.poll_duration(&limiter, false, Duration::ZERO);
        assert!(poll > Duration::ZERO);
        assert!(poll <= Duration::from_secs(1));

        let mut terminal = live_model.task_run(&live).unwrap().clone();
        terminal.state = TaskState::Completed;
        terminal.updated_at_ms = Some(8_000);
        terminal.finished_at_ms = Some(8_000);
        live_model.insert_task_run(terminal);
        senders.model.send(Arc::new(live_model)).unwrap();
        clock.set(8_000);
        assert!(app.refresh_if_changed().unwrap());
        assert_eq!(app.next_expiry_ms(), None);
    }

    #[test]
    fn hook_only_expiry_uses_cached_deadline_without_advance_clock() {
        let run_id = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let updated_at_ms = 100;
        let deadline = updated_at_ms + activity::HOOK_ONLY_STALE_VISIBILITY_MS;
        let mut model = DomainModel::default();
        model.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller("hook-only".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: Some(updated_at_ms),
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
        let clock = TestClock::at(updated_at_ms);
        let (mut app, _senders) = app_with_runtime(
            model,
            empty_operator(HashMap::new()),
            TuiSetup::default(),
            clock.clone(),
        );
        assert_eq!(displayed_run_names(&app), ["hook-only"]);
        assert_eq!(app.next_expiry_ms(), Some(deadline));

        clock.set(deadline);
        let limiter = FrameLimiter::default();
        assert_eq!(
            app.poll_duration(&limiter, false, Duration::ZERO),
            Duration::ZERO
        );
        assert!(app.refresh_if_changed().unwrap());
        assert!(displayed_run_names(&app).is_empty());
        assert_eq!(app.next_expiry_ms(), None);
    }

    #[test]
    fn i4_non_row_refresh_preserves_pending_row_watches() {
        let old = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let terminal = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let terminal_at_ms = 100;
        let deadline = terminal_at_ms + activity::DEFAULT_TERMINAL_VISIBILITY_MS;
        let clock = TestClock::at(terminal_at_ms);
        let (model_sender, model_receiver) = watch::channel(Arc::new(model_with_runs(&[(
            old,
            "old",
            1,
            TaskState::Running,
        )])));
        let (performance_sender, performance) =
            watch::channel(performance_publication(ObservationQuality::Live, []));
        let (diagnostics_sender, diagnostics_receiver) = watch::channel(healthy_diagnostics());
        let (operator_sender, operator_receiver) = watch::channel(empty_operator(HashMap::new()));
        let (coverage_sender, coverage_receiver) = watch::channel(SourceCoverageRegistry::new(
            SourceAvailability::NotApplicable,
        ));
        let mut app = App::with_inputs(
            model_receiver,
            HeaderInputs {
                session: "session".to_owned(),
                source_coverage: coverage_receiver,
                performance,
                ..HeaderInputs::default()
            },
            diagnostics_receiver,
            operator_receiver,
            TuiSetup::default(),
            clock.clone(),
        );

        model_sender
            .send(Arc::new(model_with_runs(&[
                (old, "old", 1, TaskState::Running),
                (terminal, "terminal", 2, TaskState::Completed),
            ])))
            .unwrap();
        operator_sender
            .send(empty_operator(HashMap::from([(terminal, terminal_at_ms)])))
            .unwrap();
        performance_sender
            .send(performance_publication(
                ObservationQuality::Degraded,
                [PerformanceDegradationReason::DependencyEdges],
            ))
            .unwrap();
        let mut changed_diagnostics = healthy_diagnostics();
        changed_diagnostics.dangling_announcement_components = 41;
        diagnostics_sender.send(changed_diagnostics).unwrap();
        let mut changed_coverage = SourceCoverageRegistry::new(SourceAvailability::NotApplicable);
        changed_coverage.set(CoverageSource::Codex, SourceAvailability::Available);
        coverage_sender.send(changed_coverage).unwrap();

        app.reset_projection_build_count();
        app.refresh_cached(false);
        assert_eq!(
            app.performance.effective_quality,
            ObservationQuality::Degraded
        );
        assert_eq!(app.diagnostics.dangling_announcement_components, 41);
        assert_eq!(
            app.header
                .source_coverage
                .borrow()
                .state(CoverageSource::Codex),
            &SourceAvailability::Available
        );
        assert!(app.model.task_run(&terminal).is_none());
        assert!(app.state.terminal_times.is_empty());
        assert!(app.model_receiver.has_changed().unwrap());
        assert!(app.operator_receiver.has_changed().unwrap());
        assert_eq!(app.projection_build_count(), 0);

        assert!(app.refresh_if_changed().unwrap());
        assert_eq!(
            app.projection_build_count(),
            1,
            "the pending model/operator pair needs one following-mode projection recomputation"
        );
        assert!(app.model.task_run(&terminal).is_some());
        assert_eq!(
            app.state.terminal_times.get(&terminal),
            Some(&terminal_at_ms)
        );
        assert!(app.is_following());
        assert_eq!(app.selected_run_id(), Some(terminal));
        assert_eq!(app.next_expiry_ms(), Some(deadline));
        assert_eq!(displayed_run_names(&app), ["old", "terminal"]);

        clock.set(deadline);
        let limiter = FrameLimiter::default();
        assert_eq!(
            app.poll_duration(&limiter, false, Duration::ZERO),
            Duration::ZERO
        );
        app.reset_projection_build_count();
        assert!(app.refresh_if_changed().unwrap());
        assert_eq!(app.projection_build_count(), 1);
        assert_eq!(app.next_expiry_ms(), None);
        assert_eq!(app.selected_run_id(), Some(old));
        assert_eq!(displayed_run_names(&app), ["old"]);
    }

    #[test]
    fn i4_follow_and_selection_survive_all_disappearance_paths() {
        let first = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let selected = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        let last = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAX");
        let clock = TestClock::at(3_599_999);
        let (mut app, senders) = app_with_runtime(
            model_with_runs(&[
                (first, "first", 1, TaskState::Running),
                (selected, "selected", 2, TaskState::Completed),
                (last, "last", 3, TaskState::Running),
            ]),
            empty_operator(HashMap::from([(selected, 0)])),
            TuiSetup::default(),
            clock.clone(),
        );
        assert_eq!(app.selected_run_id(), Some(last));
        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(app.selected_run_id(), Some(selected));
        clock.set(3_600_000);
        assert!(
            app.refresh_if_changed().unwrap(),
            "deadline wakes without a watch event"
        );
        assert_eq!(app.state().selection_reason(), Some("aged_out"));

        app.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert_eq!(app.selected_run_id(), Some(last));
        let first_query = first.to_string();
        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        type_text(&mut app, &first_query);
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(app.state().selection_reason(), Some("filtered"));
        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Pane("pane".to_owned()))
        );
        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        for _ in 0..first_query.len() {
            app.handle_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        }
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        app.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert!(app.is_following());
        assert_eq!(app.selected_run_id(), Some(last));

        let survivor = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAY");
        let last_key = RunKey::Controller("last".to_owned());
        let mut merged = model_with_runs(&[
            (first, "first", 1, TaskState::Running),
            (survivor, "survivor", 4, TaskState::Running),
        ]);
        merged.insert_task_run_alias(last_key, survivor);
        senders.model.send(Arc::new(merged)).unwrap();
        senders
            .operator
            .send(empty_operator(HashMap::new()))
            .unwrap();
        app.refresh();
        assert_eq!(app.selected_run_id(), Some(survivor));
        assert!(
            app.is_following(),
            "follow remains pinned to newest visible identity"
        );

        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        senders
            .model
            .send(Arc::new(model_with_runs(&[(
                survivor,
                "survivor",
                4,
                TaskState::Running,
            )])))
            .unwrap();
        app.refresh();
        assert_eq!(app.state().selection_reason(), Some("closed"));
        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Pane("pane".to_owned()))
        );
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
        assert!(render(&app).contains("Selected: ● first first"));

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
            display_name: None,
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
    fn tab_from_non_run_selection_stays_manual() {
        let run = run_id("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let (mut app, _model_sender) =
            app_with_model(model_with_runs(&[(run, "run", 1, TaskState::Running)]));
        app.state.follow = false;
        app.set_selection(Some(NodeKey::Workspace("workspace".to_owned())));

        app.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));

        assert_eq!(app.state().view_mode(), ViewMode::DependencyDag);
        assert!(!app.is_following());
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
                .contains("merged")
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
                run_id: middle,
                pane_id: None,
            })
        );

        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
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

        let edgeful_model = || {
            let mut model = model_with_runs(&input);
            for pair in ids.windows(2) {
                model.insert_dependency_edge(DependencyEdge {
                    prerequisite_run_id: pair[0],
                    dependent_run_id: pair[1],
                });
            }
            model
        };

        let (mut following, _sender) = app_with_model(edgeful_model());
        following.set_selection(Some(NodeKey::Run {
            run_id: ids[0],
            pane_id: Some("pane".to_owned()),
        }));
        assert!(following.is_following());
        following.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        let following_rows = render_lines(&following, 100, 14);
        let following_view = following_rows[3..9].join("\n");
        assert!(following_view.contains("Dependency DAG"));
        assert!(!following_view.contains("> ● run-0 run-0"));
        assert!(following_view.contains("run-7 run-7"));

        let (mut manual, _sender) = app_with_model(edgeful_model());
        manual.state.follow = false;
        manual.set_selection(Some(NodeKey::Run {
            run_id: ids[0],
            pane_id: Some("pane".to_owned()),
        }));
        manual.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        let manual_rows = render_lines(&manual, 100, 14);
        let manual_view = manual_rows[3..9].join("\n");
        assert!(manual_view.contains("> ● run-0 run-0"));
    }

    #[test]
    fn source_coverage_refreshes_dynamically() {
        let (model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (coverage_sender, coverage_receiver) =
            watch::channel(SourceCoverageRegistry::new(SourceAvailability::Available));
        let mut app = App::new(
            model_receiver,
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
    fn selection_recovers_to_ancestor_before_neighbor() {
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

        assert_eq!(
            app.state().selected(),
            Some(&NodeKey::Pane("pane".to_owned()))
        );
        let screen = render(&app);
        assert!(
            screen.contains("closed"),
            "screen did not show recovery: {screen}"
        );
        assert!(
            screen.contains("Pane: pane"),
            "screen did not show ancestor: {screen}"
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
        let (performance_sender, performance) =
            watch::channel(performance_publication(ObservationQuality::Live, []));
        let mut app = App::new(
            model_receiver,
            HeaderInputs {
                performance,
                ..HeaderInputs::default()
            },
        );

        let outcome = app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE));

        assert_eq!(outcome, LoopControl::Exit);
        assert_eq!(app.model().task_runs().count(), expected_run_count);
        assert!(model_sender.send(Arc::new(DomainModel::default())).is_ok());
        assert!(
            performance_sender
                .send(performance_publication(
                    ObservationQuality::Degraded,
                    [PerformanceDegradationReason::EventLag],
                ))
                .is_ok()
        );
        assert_eq!(app.model().task_runs().count(), expected_run_count);
    }

    #[test]
    fn c_sends_exactly_one_dismiss_clearable_command() {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (command_sender, mut command_receiver) = tokio::sync::mpsc::channel(2);
        let mut app =
            App::with_operator_commands(model_receiver, command_sender, HeaderInputs::default());

        assert_eq!(
            app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE)),
            LoopControl::Continue
        );

        assert_eq!(
            command_receiver.try_recv(),
            Ok(OperatorCommand::DismissClearable)
        );
        assert!(matches!(
            command_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn control_c_sends_no_command_and_leaves_app_state_unchanged() {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (command_sender, mut command_receiver) = tokio::sync::mpsc::channel(1);
        let mut app =
            App::with_operator_commands(model_receiver, command_sender, HeaderInputs::default());
        let before = (
            app.state.selected.clone(),
            app.state.follow,
            app.state.view_mode,
            app.state.filter_query.clone(),
            app.state.filter_draft.clone(),
            app.state.overlay,
        );

        assert_eq!(
            app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            LoopControl::Continue
        );

        assert!(matches!(
            command_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            (
                app.state.selected.clone(),
                app.state.follow,
                app.state.view_mode,
                app.state.filter_query.clone(),
                app.state.filter_draft.clone(),
                app.state.overlay,
            ),
            before
        );
    }

    #[test]
    fn control_s_does_not_open_summary_overlay() {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let mut app = App::new(model_receiver, HeaderInputs::default());

        assert_eq!(
            app.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL)),
            LoopControl::Continue
        );

        assert_eq!(app.state().overlay(), None);
    }

    #[test]
    fn c_silently_drops_when_operator_command_queue_is_full() {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (command_sender, mut command_receiver) = tokio::sync::mpsc::channel(1);
        command_sender
            .try_send(OperatorCommand::DismissClearable)
            .unwrap();
        let mut app =
            App::with_operator_commands(model_receiver, command_sender, HeaderInputs::default());
        let before = (
            app.state.selected.clone(),
            app.state.follow,
            app.state.filter_draft.clone(),
            app.state.overlay,
        );

        assert_eq!(
            app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE)),
            LoopControl::Continue
        );

        assert_eq!(
            command_receiver.try_recv(),
            Ok(OperatorCommand::DismissClearable)
        );
        assert!(matches!(
            command_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            (
                app.state.selected.clone(),
                app.state.follow,
                app.state.filter_draft.clone(),
                app.state.overlay,
            ),
            before
        );
    }

    #[test]
    fn c_in_filter_draft_is_text_and_sends_no_command() {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (command_sender, mut command_receiver) = tokio::sync::mpsc::channel(2);
        let mut app =
            App::with_operator_commands(model_receiver, command_sender, HeaderInputs::default());

        app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE));

        assert_eq!(app.state().filter_draft(), Some("c"));
        assert!(matches!(
            command_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn tui_exits_on_closed_watch() {
        let (model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let mut app = App::new(model_receiver, HeaderInputs::default());
        drop(model_sender);

        let error = app
            .refresh_if_changed()
            .expect_err("closed model watch must terminate the TUI loop");

        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(error.to_string().contains("model watch closed"));
    }

    #[test]
    fn tui_exits_on_closed_performance_watch() {
        let (_model_sender, model_receiver) = watch::channel(Arc::new(DomainModel::default()));
        let (performance_sender, performance) =
            watch::channel(performance_publication(ObservationQuality::Live, []));
        let mut app = App::new(
            model_receiver,
            HeaderInputs {
                performance,
                ..HeaderInputs::default()
            },
        );
        drop(performance_sender);

        let error = app
            .refresh_if_changed()
            .expect_err("closed performance watch must terminate the TUI loop");

        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(error.to_string().contains("performance watch closed"));
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
