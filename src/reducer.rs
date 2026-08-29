//! T7 reducer state machines, ordinal allocator, and gap reconciliation.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::watch;

use crate::activity::{self, OperatorSnapshot, RestoredOperatorState};
use crate::diagnostics::RuntimeWriteOutcome;
use crate::identity::{
    BindingEvidence, MergeConflict, apply_binding_plan_at, plan_binding, preflight_dependency_edge,
    preflight_execution_edge,
};
use crate::model::{
    AgentNode, AgentNodeObservation, AgentSessionReferenceKind, ControllerDiagnosticsHandle,
    ControllerEvent, ControllerEventKind, DependencyEdge, DisplayOrdinal, DomainModel,
    EventMetadata, ExecState, Execution, ExecutionEdge, MinimalProviderMetadata,
    NativeLifecycleWatermark, NativeSessionEnd, NativeSessionEndStatus, NormalizedEvent,
    ObservationOrigin, OperatorCommand, Pane, PaneAgentStatusObservation, Provider,
    ProviderDiagnosticsHandle, ReconcileBatch, RunId, RunKey, RunRateCursor, RunRateTotals,
    SharedModel, TaskRun, TaskRunV6State, TaskState, TopologyAuthority, TopologyEntity,
    TopologyEntityId, TopologySnapshot, sanitize_controller_text,
};
use crate::operator::OperatorProjection;
use crate::store::{
    EnqueuePermit, HistoryDrainFinalization, NativeSessionBinding, PendingEnqueue, PersistBatch,
    PersistExecution, PersistHistoryDrain, PersistHistoryDrainRun, PersistOp, PersistTaskRun,
    PersistTaskRunV6, PersistV6Batch, RestoredState,
};
// increment5-workload-harness: begin reducer timing callback ABI
#[cfg(feature = "workload-harness")]
use std::cell::RefCell;
#[cfg(feature = "workload-harness")]
use std::time::{Duration, Instant};

/// Closed workload timing scopes emitted by the real reducer paths.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadTimingKind {
    ControllerEvent,
    StartupRestore,
    FallbackNotification,
    FallbackRescan,
}

/// Aggregate timing collected from one real reducer scope.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadTimingObservation {
    pub kind: WorkloadTimingKind,
    pub sequence: u64,
    pub d4_segment_count: u32,
    pub d4_analysis_ns: u64,
    pub reducer_plus_publish_ns: u64,
    pub model_clone_publish_segment_count: u32,
    pub model_clone_publish_ns: u64,
}

/// Feature-only callback receiving one successful real reducer scope.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
pub type WorkloadTimingObserver = Arc<dyn Fn(WorkloadTimingObservation) + Send + Sync>;

#[cfg(feature = "workload-harness")]
struct WorkloadTimingState {
    kind: WorkloadTimingKind,
    sequence: u64,
    active_started: Option<Instant>,
    reducer_plus_publish: Duration,
    d4_segment_count: u32,
    d4_analysis_ns: u64,
    model_clone_publish_segment_count: u32,
    model_clone_publish_ns: u64,
    observer: WorkloadTimingObserver,
}

#[cfg(feature = "workload-harness")]
thread_local! {
    static WORKLOAD_TIMING_STATE: RefCell<Option<WorkloadTimingState>> = const { RefCell::new(None) };
}

#[cfg(feature = "workload-harness")]
enum WorkloadTimingSegment {
    D4,
    ModelClonePublish,
}

#[cfg(feature = "workload-harness")]
pub(crate) struct WorkloadTimingScope {
    active: bool,
}

#[cfg(feature = "workload-harness")]
struct SuspendedWorkloadTimingState(WorkloadTimingState);

#[cfg(feature = "workload-harness")]
pub(crate) fn workload_timing_scope(
    kind: WorkloadTimingKind,
    sequence: u64,
    observer: WorkloadTimingObserver,
) -> WorkloadTimingScope {
    WORKLOAD_TIMING_STATE.with(|slot| {
        let previous = slot.replace(Some(WorkloadTimingState {
            kind,
            sequence,
            active_started: Some(Instant::now()),
            reducer_plus_publish: Duration::ZERO,
            d4_segment_count: 0,
            d4_analysis_ns: 0,
            model_clone_publish_segment_count: 0,
            model_clone_publish_ns: 0,
            observer,
        }));
        assert!(
            previous.is_none(),
            "workload timing scopes must not overlap"
        );
    });
    WorkloadTimingScope { active: true }
}

#[cfg(feature = "workload-harness")]
impl WorkloadTimingScope {
    pub(crate) fn finish(mut self) {
        let state = WORKLOAD_TIMING_STATE.with(|slot| slot.borrow_mut().take());
        let mut state = state.expect("workload timing scope must remain installed until finish");
        // ControllerEvent's 2/2 counts are one D4 + clone/publish pair from the
        // validate_controller_event scratch Self::new, then the post-transition
        // D4 analysis and committed model publication.
        let expected_segments = match state.kind {
            WorkloadTimingKind::ControllerEvent => 2,
            WorkloadTimingKind::StartupRestore
            | WorkloadTimingKind::FallbackNotification
            | WorkloadTimingKind::FallbackRescan => 1,
        };
        assert_eq!(
            state.d4_segment_count, expected_segments,
            "workload timing scope observed a missing or duplicate D4 segment"
        );
        assert_eq!(
            state.model_clone_publish_segment_count, expected_segments,
            "workload timing scope observed a missing or duplicate clone/publication segment"
        );
        state.pause();
        let reducer_plus_publish_ns = u64::try_from(state.reducer_plus_publish.as_nanos())
            .expect("workload reducer timing exceeded u64 nanoseconds");
        (state.observer)(WorkloadTimingObservation {
            kind: state.kind,
            sequence: state.sequence,
            d4_segment_count: state.d4_segment_count,
            d4_analysis_ns: state.d4_analysis_ns,
            reducer_plus_publish_ns,
            model_clone_publish_segment_count: state.model_clone_publish_segment_count,
            model_clone_publish_ns: state.model_clone_publish_ns,
        });
        self.active = false;
    }

    fn suspend(mut self) -> SuspendedWorkloadTimingState {
        let state = WORKLOAD_TIMING_STATE.with(|slot| slot.borrow_mut().take());
        let mut state = state.expect("workload timing scope must remain installed until suspend");
        state.pause();
        self.active = false;
        SuspendedWorkloadTimingState(state)
    }
}

#[cfg(feature = "workload-harness")]
impl WorkloadTimingState {
    fn pause(&mut self) {
        let started = self
            .active_started
            .take()
            .expect("installed workload timing state must be active");
        self.reducer_plus_publish = self
            .reducer_plus_publish
            .checked_add(started.elapsed())
            .expect("workload reducer timing overflowed");
    }
}

#[cfg(feature = "workload-harness")]
impl SuspendedWorkloadTimingState {
    fn resume(mut self) -> WorkloadTimingScope {
        assert!(
            self.0.active_started.is_none(),
            "suspended workload timing state must be inactive"
        );
        self.0.active_started = Some(Instant::now());
        WORKLOAD_TIMING_STATE.with(|slot| {
            let mut slot = slot.borrow_mut();
            assert!(slot.is_none(), "workload timing scopes must not overlap");
            *slot = Some(self.0);
        });
        WorkloadTimingScope { active: true }
    }
}

#[cfg(feature = "workload-harness")]
impl Drop for WorkloadTimingScope {
    fn drop(&mut self) {
        if self.active {
            WORKLOAD_TIMING_STATE.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
}

#[cfg(feature = "workload-harness")]
fn record_workload_timing_segment(segment: WorkloadTimingSegment, elapsed: std::time::Duration) {
    let elapsed_ns = u64::try_from(elapsed.as_nanos())
        .expect("workload timing segment exceeded u64 nanoseconds");
    WORKLOAD_TIMING_STATE.with(|slot| {
        let mut state = slot.borrow_mut();
        let Some(state) = state.as_mut() else {
            return;
        };
        match segment {
            WorkloadTimingSegment::D4 => {
                state.d4_segment_count = state
                    .d4_segment_count
                    .checked_add(1)
                    .expect("workload D4 segment count overflowed");
                state.d4_analysis_ns = state
                    .d4_analysis_ns
                    .checked_add(elapsed_ns)
                    .expect("workload D4 timing overflowed");
            }
            WorkloadTimingSegment::ModelClonePublish => {
                state.model_clone_publish_segment_count = state
                    .model_clone_publish_segment_count
                    .checked_add(1)
                    .expect("workload clone/publication segment count overflowed");
                state.model_clone_publish_ns = state
                    .model_clone_publish_ns
                    .checked_add(elapsed_ns)
                    .expect("workload clone/publication timing overflowed");
            }
        }
    });
}

#[cfg(feature = "workload-harness")]
struct WorkloadObservationTiming {
    kind: WorkloadTimingKind,
    next_sequence: u64,
    setup_observations_to_skip: u32,
    observer: WorkloadTimingObserver,
}
// increment5-workload-harness: end reducer timing callback ABI

const STALE_GRACE_MS: i64 = 30_000;

/// Errors that reject a reducer transition before any model or persistence mutation escapes.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ReducerError {
    /// No immutable display ordinal remains for a newly observed Task Run.
    #[error("display ordinal allocator is exhausted")]
    OrdinalExhausted,
    /// Identity evidence conflicted with the current binding graph.
    #[error("identity binding conflict: {0}")]
    BindingConflict(#[from] MergeConflict),
}

/// Transactional result of applying one reducer observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    /// The observation committed in memory and produced this durable batch.
    Applied(PersistBatch),
    /// Identity evidence conflicted and the complete observation was rolled back.
    DroppedBindingConflict(MergeConflict),
}

/// Stable Controller rejection taxonomy used by the later transport surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectReason {
    Invalid,
    Cycle,
    Conflict,
    StaleEvent,
    UnsupportedVersion,
}

/// Reducer-owned diagnostic increments captured during scratch validation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ControllerDiagnosticDeltas {
    pub terminal_blocked_progress_noops: u64,
    pub terminal_forward_reference_creations: u64,
    pub unknown_lane_terminal_drops: u64,
    pub post_dangling_announcement_components: u64,
}

/// Fully materialized result of successful Controller validation.
pub struct MaterializedDelta {
    pub post_model: DomainModel,
    pub post_next_ordinal: i64,
    pub post_terminal_event_sources: HashMap<RunId, String>,
    pub post_non_lane_task_state_runs: HashSet<RunId>,
    pub diagnostic_deltas: ControllerDiagnosticDeltas,
    pub batch: PersistBatch,
}

/// Retryable failure before any staged domain mutation is committed.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CommitStagedError {
    #[error("Controller ingest sequence is exhausted")]
    IngestSequenceExhausted,
}

/// Immutable request staged before the writer decides whether finalization committed.
///
/// The staged request retains the barrier's exact frozen manifest allocation so the writer can
/// upsert and finalize it in one transaction; its identity is `manifest.drain_id`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedHistoryFinalization {
    pub manifest: Arc<PersistHistoryDrain>,
    pub observed_at_ms: i64,
}

/// One provider submission's publication work, consumed only by its matching acknowledgement.
pub(crate) struct ProviderSubmissionReceipt {
    kind: ProviderSubmissionReceiptKind,
    #[cfg(feature = "workload-harness")]
    workload_timing: Option<SuspendedWorkloadTimingState>,
}

enum ProviderSubmissionReceiptKind {
    Historical,
    Live { submission_id: Option<u64> },
}

impl ProviderSubmissionReceipt {
    fn historical() -> Self {
        Self {
            kind: ProviderSubmissionReceiptKind::Historical,
            #[cfg(feature = "workload-harness")]
            workload_timing: None,
        }
    }

    fn live(submission_id: Option<u64>) -> Self {
        Self {
            kind: ProviderSubmissionReceiptKind::Live { submission_id },
            #[cfg(feature = "workload-harness")]
            workload_timing: None,
        }
    }
}

static NEXT_PROVIDER_SUBMISSION_TOKEN: AtomicU64 = AtomicU64::new(0);

fn allocate_provider_submission_token() -> u64 {
    NEXT_PROVIDER_SUBMISSION_TOKEN
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |token| {
            token.checked_add(1)
        })
        .expect("live provider submission tokens are exhausted")
}

/// Maximum unattributed provider-usage samples retained across all run scopes.
///
/// The reducer evicts the globally oldest sample first. Since each retained sample has exactly
/// one [`RunKey`], this also bounds the number of pending scopes.
const PENDING_TELEMETRY_SAMPLE_CAPACITY: usize = 4_096;

#[derive(Clone, Debug)]
struct PendingTelemetry {
    at_ms: i64,
    output_tokens: u64,
    token_breakdown: crate::model::TokenBreakdown,
    attribution: crate::model::TurnAttr,
}

/// Pre-observation state retained only for the provider route that needs it.
pub(crate) enum ProviderObservationPrior {
    Historical(Box<DomainModel>),
    Live(Box<LiveProviderObservationCheckpoint>),
}

/// Reducer-owned state a private live observation may mutate before persistence acknowledgement.
pub(crate) struct LiveProviderObservationCheckpoint {
    model: DomainModel,
    next_ordinal: i64,
    next_ingest_seq: Option<i64>,
    terminal_event_sources: HashMap<RunId, String>,
    non_lane_task_state_runs: HashSet<RunId>,
    pending_telemetry: HashMap<RunKey, VecDeque<PendingTelemetry>>,
    pending_telemetry_order: VecDeque<RunKey>,
    pending_telemetry_count: usize,
    dirty_rate_totals: HashSet<RunId>,
    pending_rate_observation_runs: HashSet<RunId>,
}

/// One unresolved live provider write and the reducer-owned work its acknowledgement may release.
struct PendingLiveProviderSubmission {
    submission_id: u64,
    checkpoint: Option<Box<LiveProviderObservationCheckpoint>>,
    ready_run_ids: Vec<RunId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RateObservationOrigin {
    Historical,
    Live { epoch: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RateObservation {
    pub(crate) run_id: RunId,
    pub(crate) origin: RateObservationOrigin,
    pub(crate) observed_at_ms: i64,
}

/// Serialized owner of domain transitions and display-ordinal allocation.
pub struct Reducer {
    model: DomainModel,
    next_ordinal: i64,
    next_ingest_seq: Option<i64>,
    terminal_event_sources: HashMap<RunId, String>,
    non_lane_task_state_runs: HashSet<RunId>,
    pending_telemetry: HashMap<RunKey, VecDeque<PendingTelemetry>>,
    pending_telemetry_order: VecDeque<RunKey>,
    pending_telemetry_count: usize,
    rate_epoch: u64,
    rate_epoch_active: bool,
    dirty_rate_totals: HashSet<RunId>,
    pending_rate_observation_runs: HashSet<RunId>,
    rate_observation_context: Option<(RateObservationOrigin, i64)>,
    publisher: watch::Sender<Arc<DomainModel>>,
    operator: OperatorProjection,
    defer_provider_publication: bool,
    defer_provider_model_publication: bool,
    provider_model_publication_pending: bool,
    deferred_provider_drain: Option<crate::model::HistoryDrainId>,
    provider_observation_private_run_ids: HashSet<RunId>,
    pending_live_provider_submission: Option<PendingLiveProviderSubmission>,
    published_history_drains: HashSet<crate::model::HistoryDrainId>,
    #[cfg(test)]
    publish_count: std::cell::Cell<u64>,
    #[cfg(test)]
    shared_publish_count: Arc<AtomicU64>,
    #[cfg(test)]
    rate_observation_count: usize,
    // increment5-workload-harness: begin reducer timing configuration field
    #[cfg(feature = "workload-harness")]
    workload_observation_timing: Option<WorkloadObservationTiming>,
    #[cfg(feature = "workload-harness")]
    pending_workload_timing: Option<SuspendedWorkloadTimingState>,
    // increment5-workload-harness: end reducer timing configuration field
}

impl LiveProviderObservationCheckpoint {
    fn capture(reducer: &Reducer) -> Self {
        Self {
            model: reducer.model.clone(),
            next_ordinal: reducer.next_ordinal,
            next_ingest_seq: reducer.next_ingest_seq,
            terminal_event_sources: reducer.terminal_event_sources.clone(),
            non_lane_task_state_runs: reducer.non_lane_task_state_runs.clone(),
            pending_telemetry: reducer.pending_telemetry.clone(),
            pending_telemetry_order: reducer.pending_telemetry_order.clone(),
            pending_telemetry_count: reducer.pending_telemetry_count,
            dirty_rate_totals: reducer.dirty_rate_totals.clone(),
            pending_rate_observation_runs: reducer.pending_rate_observation_runs.clone(),
        }
    }

    fn restore(self, reducer: &mut Reducer) {
        reducer.model = self.model;
        reducer.next_ordinal = self.next_ordinal;
        reducer.next_ingest_seq = self.next_ingest_seq;
        reducer.terminal_event_sources = self.terminal_event_sources;
        reducer.non_lane_task_state_runs = self.non_lane_task_state_runs;
        reducer.pending_telemetry = self.pending_telemetry;
        reducer.pending_telemetry_order = self.pending_telemetry_order;
        reducer.pending_telemetry_count = self.pending_telemetry_count;
        reducer.dirty_rate_totals = self.dirty_rate_totals;
        reducer.pending_rate_observation_runs = self.pending_rate_observation_runs;
    }
}

fn provider_v6_batch_is_empty(batch: &PersistV6Batch) -> bool {
    batch.operations.is_empty()
        && batch.task_runs.is_empty()
        && batch.rate_totals.is_empty()
        && batch.history_drains.is_empty()
        && batch.history_associations.is_empty()
        && batch.history_publications.is_empty()
        && batch.history_event_drain.is_none()
}

impl Reducer {
    /// Restores reducer state and returns a receiver for coherent model snapshots.
    #[must_use]
    pub fn new(restored: RestoredState) -> (Self, SharedModel) {
        let (reducer, shared, _operator) = Self::new_with_operator(
            restored,
            RestoredOperatorState {
                activity: Vec::new(),
                terminal_times: std::collections::HashMap::new(),
            },
        );
        (reducer, shared)
    }

    /// Restores reducer and operator state and returns both immutable receivers.
    #[must_use]
    pub fn new_with_operator(
        restored: RestoredState,
        restored_operator: RestoredOperatorState,
    ) -> (Self, SharedModel, watch::Receiver<OperatorSnapshot>) {
        let mut model = restored.model;
        // increment5-workload-harness: begin startup D4 timing start
        #[cfg(feature = "workload-harness")]
        let workload_d4_started = Instant::now();
        // increment5-workload-harness: end startup D4 timing start
        let dangling_components = crate::model::graph::dangling_announcement_components(&model);
        // increment5-workload-harness: begin startup D4 timing finish
        #[cfg(feature = "workload-harness")]
        record_workload_timing_segment(WorkloadTimingSegment::D4, workload_d4_started.elapsed());
        // increment5-workload-harness: end startup D4 timing finish
        model
            .controller_diagnostics_mut()
            .set_dangling_announcement_components(dangling_components);
        let deferred_history_activity = model.take_deferred_history_activity();
        // increment5-workload-harness: begin startup clone publication timing start
        #[cfg(feature = "workload-harness")]
        let workload_publish_started = Instant::now();
        // increment5-workload-harness: end startup clone publication timing start
        let (publisher, shared) = watch::channel(Arc::new(model.publication_snapshot()));
        // increment5-workload-harness: begin startup clone publication timing finish
        #[cfg(feature = "workload-harness")]
        record_workload_timing_segment(
            WorkloadTimingSegment::ModelClonePublish,
            workload_publish_started.elapsed(),
        );
        // increment5-workload-harness: end startup clone publication timing finish
        let (mut operator, operator_receiver) = OperatorProjection::new(restored_operator);
        operator.restore_deferred_history(deferred_history_activity, &model);
        (
            Self {
                model,
                next_ordinal: restored.next_ordinal,
                next_ingest_seq: restored.next_ingest_seq,
                terminal_event_sources: HashMap::new(),
                non_lane_task_state_runs: HashSet::new(),
                pending_telemetry: HashMap::new(),
                pending_telemetry_order: VecDeque::new(),
                pending_telemetry_count: 0,
                rate_epoch: 0,
                rate_epoch_active: false,
                dirty_rate_totals: HashSet::new(),
                pending_rate_observation_runs: HashSet::new(),
                rate_observation_context: None,
                publisher,
                operator,
                defer_provider_publication: false,
                defer_provider_model_publication: false,
                provider_model_publication_pending: false,
                deferred_provider_drain: None,
                provider_observation_private_run_ids: HashSet::new(),
                pending_live_provider_submission: None,
                published_history_drains: HashSet::new(),
                #[cfg(test)]
                publish_count: std::cell::Cell::new(0),
                #[cfg(test)]
                shared_publish_count: Arc::new(AtomicU64::new(0)),
                #[cfg(test)]
                rate_observation_count: 0,
                // increment5-workload-harness: begin reducer timing configuration initialization
                #[cfg(feature = "workload-harness")]
                workload_observation_timing: None,
                #[cfg(feature = "workload-harness")]
                pending_workload_timing: None,
                // increment5-workload-harness: end reducer timing configuration initialization
            },
            shared,
            operator_receiver,
        )
    }

    // increment5-workload-harness: begin observed reducer adapters
    /// Restores through the production constructor while observing its real D4 and clone spans.
    #[cfg(feature = "workload-harness")]
    #[doc(hidden)]
    #[must_use]
    pub fn new_with_operator_observed(
        restored: RestoredState,
        restored_operator: RestoredOperatorState,
        sequence: u64,
        observer: WorkloadTimingObserver,
    ) -> (Self, SharedModel, watch::Receiver<OperatorSnapshot>) {
        let timing = workload_timing_scope(WorkloadTimingKind::StartupRestore, sequence, observer);
        let restored = Self::new_with_operator(restored, restored_operator);
        timing.finish();
        restored
    }

    /// Arms each successful production `apply_observation` call for one fallback timing sample.
    #[cfg(feature = "workload-harness")]
    #[doc(hidden)]
    pub fn configure_workload_observation_timing(
        &mut self,
        kind: WorkloadTimingKind,
        first_sequence: u64,
        observer: WorkloadTimingObserver,
    ) {
        assert!(
            matches!(
                kind,
                WorkloadTimingKind::FallbackNotification | WorkloadTimingKind::FallbackRescan
            ),
            "automatic observation timing is reserved for fallback arms"
        );
        self.workload_observation_timing = Some(WorkloadObservationTiming {
            kind,
            next_sequence: first_sequence,
            setup_observations_to_skip: 1,
            observer,
        });
    }

    // increment5-workload-harness: end observed reducer adapters
    /// Applies one normalized event and publishes exactly one resulting snapshot.
    pub fn apply(&mut self, event: NormalizedEvent) -> Result<ApplyOutcome, ReducerError> {
        self.apply_observation(vec![event])
    }

    /// Applies one globally unambiguous sessionless Codex pane binding.
    pub(crate) fn apply_heuristic_binding(
        &mut self,
        run: RunId,
        sid: String,
        bookkeeping_time_ms: i64,
    ) -> ApplyOutcome {
        let plan = plan_binding(
            &self.model,
            &BindingEvidence::HeuristicNativeSession { run, sid },
        );
        let mut persist = match apply_binding_plan_at(&mut self.model, plan, bookkeeping_time_ms) {
            Ok(persist) => persist,
            Err(conflict) => return ApplyOutcome::DroppedBindingConflict(conflict),
        };
        self.recompute_dangling_announcement_components();
        normalize_persist_batch_lineage(&mut persist);
        self.apply_operator_submission(&persist);
        self.publish();
        ApplyOutcome::Applied(persist)
    }

    /// Applies one source observation and publishes exactly one resulting snapshot.
    pub fn apply_observation(
        &mut self,
        events: Vec<NormalizedEvent>,
    ) -> Result<ApplyOutcome, ReducerError> {
        self.apply_observation_inner(events, None)
    }

    pub(crate) fn begin_rate_epoch(&mut self) -> u64 {
        self.rate_epoch = self.rate_epoch.wrapping_add(1);
        self.rate_epoch_active = false;
        self.model.clear_run_rate_cursors();
        self.pending_rate_observation_runs.clear();
        self.rate_epoch
    }

    pub(crate) fn activate_rate_epoch(&mut self, observed_at_ms: i64) {
        self.rate_epoch_active = true;
        let statuses = crate::status::StatusReadModel::from_model(&self.model, observed_at_ms);
        let live_execution_runs = self
            .model
            .executions()
            .filter(|execution| !execution.state.is_terminal())
            .map(|execution| execution.task_run_id)
            .collect::<HashSet<_>>();
        let mut candidates = self
            .model
            .task_runs()
            .filter_map(|run| {
                let working = matches!(
                    statuses.run_rate_activity(&self.model, run),
                    crate::status::RunRateActivity::Working
                );
                let live_paused_execution = live_execution_runs.contains(&run.run_id)
                    && !run.state.is_terminal()
                    && !self
                        .model
                        .task_run_v6_state(&run.run_id)
                        .is_some_and(|state| state.native_session_end.is_some());
                (working || live_paused_execution).then_some((run.run_id, working))
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|(run_id, _)| *run_id);
        for (run_id, working) in candidates {
            self.observe_run_rates_with_activity(
                RateObservation {
                    run_id,
                    origin: RateObservationOrigin::Live {
                        epoch: self.rate_epoch,
                    },
                    observed_at_ms,
                },
                working,
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn rate_epoch(&self) -> u64 {
        self.rate_epoch
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn observe_run_rates(&mut self, observation: RateObservation) -> PersistBatch {
        let Some(run) = self.model.task_run(&observation.run_id).cloned() else {
            self.model.remove_run_rate_cursor(&observation.run_id);
            return Vec::new();
        };
        let working = matches!(
            crate::status::StatusReadModel::run_rate_activity_from_model(
                &self.model,
                &run,
                observation.observed_at_ms,
            ),
            crate::status::RunRateActivity::Working
        );
        self.observe_run_rates_with_activity(observation, working)
    }

    fn observe_run_rates_with_activity(
        &mut self,
        observation: RateObservation,
        working: bool,
    ) -> PersistBatch {
        #[cfg(test)]
        {
            self.rate_observation_count += 1;
        }
        let Some(run) = self.model.task_run(&observation.run_id).cloned() else {
            self.model.remove_run_rate_cursor(&observation.run_id);
            return Vec::new();
        };
        let output_tokens = self
            .model
            .telemetry(&observation.run_id)
            .map_or(0, |telemetry| telemetry.output_tokens);
        let (measurement_epoch, live_baseline) = match observation.origin {
            RateObservationOrigin::Historical => (self.rate_epoch, false),
            RateObservationOrigin::Live { epoch } => (epoch, true),
        };
        let Some(cursor) = self.model.run_rate_cursor(&observation.run_id).cloned() else {
            self.model.set_run_rate_cursor(
                observation.run_id,
                RunRateCursor {
                    baseline_output_tokens: output_tokens,
                    last_observed_at_ms: observation.observed_at_ms,
                    working,
                    measurement_epoch,
                    identity_basis: run.key,
                    live_baseline,
                },
            );
            return Vec::new();
        };

        let observed_at_ms = cursor.last_observed_at_ms.max(observation.observed_at_ms);
        let trustworthy_live_baseline = live_baseline
            && cursor.live_baseline
            && cursor.measurement_epoch == measurement_epoch
            && cursor.identity_basis == run.key;
        if trustworthy_live_baseline {
            let output_delta = output_tokens.saturating_sub(cursor.baseline_output_tokens);
            let counter_regressed = output_tokens < cursor.baseline_output_tokens;
            let working_ms = if cursor.working {
                observed_at_ms.saturating_sub(cursor.last_observed_at_ms)
            } else {
                0
            };
            if output_delta > 0 || working_ms > 0 {
                self.model.accumulate_run_rate_totals(
                    observation.run_id,
                    RunRateTotals {
                        output_tokens: if counter_regressed { 0 } else { output_delta },
                        working_ms,
                    },
                );
                self.dirty_rate_totals.insert(observation.run_id);
            }
        }
        self.model.set_run_rate_cursor(
            observation.run_id,
            RunRateCursor {
                baseline_output_tokens: output_tokens,
                last_observed_at_ms: observed_at_ms,
                working,
                measurement_epoch,
                identity_basis: run.key,
                live_baseline,
            },
        );
        Vec::new()
    }

    /// Closes the current live intervals and returns only totals changed since the last flush.
    pub(crate) fn checkpoint_run_rates(
        &mut self,
        observed_at_ms: i64,
    ) -> Vec<(RunId, RunRateTotals)> {
        let mut run_ids = self
            .model
            .run_rate_cursors()
            .filter_map(|(run_id, cursor)| cursor.working.then_some(*run_id))
            .collect::<Vec<_>>();
        run_ids.sort_unstable();
        let before = run_ids
            .iter()
            .map(|run_id| (*run_id, self.model.run_rate_totals(run_id).copied()))
            .collect::<HashMap<_, _>>();
        if self.rate_epoch_active {
            for run_id in &run_ids {
                self.observe_run_rates_with_activity(
                    RateObservation {
                        run_id: *run_id,
                        origin: RateObservationOrigin::Live {
                            epoch: self.rate_epoch,
                        },
                        observed_at_ms,
                    },
                    true,
                );
            }
        }
        if before
            .iter()
            .any(|(run_id, totals)| self.model.run_rate_totals(run_id).copied() != *totals)
        {
            self.publish_snapshot();
        }

        let mut dirty = self.dirty_rate_totals.drain().collect::<Vec<_>>();
        dirty.sort_unstable();
        dirty
            .into_iter()
            .filter_map(|run_id| {
                self.model
                    .run_rate_totals(&run_id)
                    .copied()
                    .map(|totals| (run_id, totals))
            })
            .collect()
    }

    pub(crate) fn restore_dirty_rate_totals(&mut self, totals: &[(RunId, RunRateTotals)]) {
        self.dirty_rate_totals
            .extend(totals.iter().map(|(run_id, _)| *run_id));
    }

    pub(crate) fn has_dirty_rate_totals(&self) -> bool {
        !self.dirty_rate_totals.is_empty()
    }

    #[cfg(test)]
    fn take_rate_observation_count(&mut self) -> usize {
        std::mem::take(&mut self.rate_observation_count)
    }

    /// Stages one drain finalization request without mutating or publishing the live model.
    #[must_use]
    pub(crate) fn stage_history_finalization(
        &self,
        barrier: &crate::provider::HistoryDrainBarrier,
    ) -> StagedHistoryFinalization {
        StagedHistoryFinalization {
            manifest: Arc::clone(&barrier.manifest),
            observed_at_ms: barrier.observed_at_ms,
        }
    }

    /// Applies the exact page returned by durable finalization and publishes at most once.
    ///
    /// Replaying the same page is idempotent and deliberately emits no duplicate snapshot.
    pub(crate) fn apply_history_finalization(
        &mut self,
        finalization: &HistoryDrainFinalization,
    ) -> bool {
        if !self
            .published_history_drains
            .insert(finalization.drain_id.clone())
        {
            return false;
        }
        self.published_history_drains
            .extend(finalization.completed_drains.iter().cloned());
        for finalized in &finalization.runs {
            if self.model.task_run(&finalized.run_id).is_none() {
                continue;
            }
            if self.model.task_run_v6_state(&finalized.run_id) != Some(&finalized.state) {
                self.model
                    .set_task_run_v6_state(finalized.run_id, finalized.state.clone());
            }
            if finalized.state.history_ready {
                self.model.release_history_publications(finalized.run_id);
            }
            self.pending_rate_observation_runs.insert(finalized.run_id);
        }
        let published_model = self.model.publication_snapshot();
        self.operator.publish_accumulated(
            &finalization.completed_drains,
            &finalization.runs,
            &published_model,
        );
        let prior_rate_context = self.rate_observation_context.replace((
            RateObservationOrigin::Historical,
            finalization.finalized_at_ms,
        ));
        self.publish();
        self.rate_observation_context = prior_rate_context;
        true
    }

    /// Captures provider transaction state and suppresses intermediate publication.
    pub(crate) fn begin_provider_observation(
        &mut self,
        origin: &ObservationOrigin,
        observed_at_ms: i64,
    ) -> Option<ProviderObservationPrior> {
        assert!(
            self.pending_live_provider_submission.is_none(),
            "a live provider submission must complete before another provider observation begins"
        );
        self.rate_observation_context = Some((
            match origin {
                ObservationOrigin::Historical { .. } => RateObservationOrigin::Historical,
                ObservationOrigin::Live => RateObservationOrigin::Live {
                    epoch: self.rate_epoch,
                },
            },
            observed_at_ms,
        ));
        if let ObservationOrigin::Historical { drain_id, .. } = origin {
            self.defer_provider_publication = true;
            self.defer_provider_model_publication = false;
            self.provider_model_publication_pending = false;
            self.deferred_provider_drain = Some(drain_id.clone());
            self.provider_observation_private_run_ids.clear();
            Some(ProviderObservationPrior::Historical(Box::new(
                self.model.clone(),
            )))
        } else {
            self.defer_provider_model_publication = true;
            self.provider_model_publication_pending = false;
            self.provider_observation_private_run_ids = self
                .model
                .task_runs()
                .filter_map(|run| {
                    self.model
                        .task_run_v6_state(&run.run_id)
                        .is_some_and(|state| !state.history_ready)
                        .then_some(run.run_id)
                })
                .collect();
            (!self.provider_observation_private_run_ids.is_empty()).then(|| {
                ProviderObservationPrior::Live(Box::new(
                    LiveProviderObservationCheckpoint::capture(self),
                ))
            })
        }
    }

    /// Attaches readiness and a drain association to the same transaction as core mutations.
    pub(crate) fn finish_provider_observation(
        &mut self,
        prior: Option<ProviderObservationPrior>,
        operations: PersistBatch,
        origin: &ObservationOrigin,
        history_manifest: Option<&PersistHistoryDrain>,
        provider_at_ms: i64,
    ) -> (PersistV6Batch, ProviderSubmissionReceipt) {
        let historical = matches!(origin, ObservationOrigin::Historical { .. });
        let changed = prior.as_ref().map_or_else(HashSet::new, |prior| {
            let before = match prior {
                ProviderObservationPrior::Historical(before) => before.as_ref(),
                ProviderObservationPrior::Live(before) => &before.model,
            };
            self.model.changed_task_run_ids_since(before)
        });
        let mut touched = Vec::new();
        for operation in &operations {
            match operation {
                PersistOp::UpsertTaskRun(task_run) => touched.push(task_run.task_run.run_id),
                PersistOp::PromoteTaskRunKey { promoted, .. } => {
                    touched.push(promoted.task_run.run_id);
                }
                PersistOp::MergeTaskRuns { survivor, absorbed } => {
                    touched.extend([*survivor, *absorbed]);
                }
                PersistOp::UpsertExecution(value) => {
                    touched.push(value.execution.task_run_id);
                }
                PersistOp::UpsertAgentNode(node) => touched.push(node.task_run_id),
                PersistOp::UpsertExecutionEdge { edge, .. } => {
                    touched.extend([edge.parent_run_id, edge.child_run_id]);
                }
                PersistOp::UpsertDependencyEdge { edge, .. } => {
                    touched.extend([edge.prerequisite_run_id, edge.dependent_run_id]);
                }
                PersistOp::RecordEvent { event, .. } => {
                    touched.extend(event_metadata(event).task_run_id);
                }
                _ => {}
            }
        }
        touched.extend(changed.iter().copied());
        touched.sort_unstable();
        touched.dedup();
        let private_live_run_ids = self
            .provider_observation_private_run_ids
            .drain()
            .map(|run_id| canonical_run_after_batch(run_id, &operations))
            .collect::<HashSet<_>>();

        let mut history_publications = Vec::new();
        if historical && let Some(ProviderObservationPrior::Historical(before)) = prior.as_ref() {
            for run_id in &touched {
                if before
                    .task_run_v6_state(run_id)
                    .is_some_and(|state| state.history_ready)
                    && let Some(mut publication) = before.capture_history_publication(*run_id)
                {
                    publication.canonical_run_id = canonical_run_after_batch(*run_id, &operations);
                    if self.model.install_history_publication(publication.clone()) {
                        history_publications.push(publication);
                    }
                }
            }
        }

        let mut task_runs = Vec::with_capacity(touched.len());
        let mut associations = Vec::with_capacity(touched.len());
        let mut live_ready_run_ids = Vec::new();
        for run_id in touched {
            let canonical_run_id = canonical_run_after_batch(run_id, &operations);
            let Some(current) = self.model.task_run(&canonical_run_id).cloned() else {
                continue;
            };
            let mut state = self
                .model
                .task_run_v6_state(&canonical_run_id)
                .cloned()
                .unwrap_or_default();

            let mut persisted_state = state.clone();
            if historical {
                state.history_ready = false;
                persisted_state.history_ready = false;
            } else {
                if private_live_run_ids.contains(&canonical_run_id) {
                    live_ready_run_ids.push(canonical_run_id);
                }
                persisted_state.history_ready = true;
            }
            state.latest_provider_at_ms = Some(
                state
                    .latest_provider_at_ms
                    .map_or(provider_at_ms, |stored| stored.max(provider_at_ms)),
            );
            persisted_state.latest_provider_at_ms = state.latest_provider_at_ms;
            self.model.insert_task_run(current.clone());
            self.model
                .set_task_run_v6_state(canonical_run_id, state.clone());

            let mut persisted = operations
                .iter()
                .filter_map(|operation| match operation {
                    PersistOp::UpsertTaskRun(task_run)
                        if task_run.task_run.run_id == canonical_run_id =>
                    {
                        Some(task_run.clone())
                    }
                    PersistOp::PromoteTaskRunKey { promoted, .. }
                        if promoted.task_run.run_id == canonical_run_id =>
                    {
                        Some(promoted.clone())
                    }
                    _ => None,
                })
                .next_back()
                .unwrap_or_else(
                    || match self.persist_task_run(current.clone(), provider_at_ms) {
                        PersistOp::UpsertTaskRun(task_run) => task_run,
                        _ => unreachable!("persist_task_run always returns an upsert"),
                    },
                );
            persisted.task_run = current.clone();
            persisted.created_at_ms = current.created_at_ms.unwrap_or(persisted.created_at_ms);
            persisted.updated_at_ms = current.updated_at_ms.unwrap_or(persisted.updated_at_ms);
            persisted.finished_at_ms = current.finished_at_ms;
            task_runs.push(PersistTaskRunV6 {
                task_run: persisted,
                state: persisted_state,
            });
            if let ObservationOrigin::Historical { drain_id, .. } = origin {
                associations.push(PersistHistoryDrainRun {
                    drain_id: drain_id.clone(),
                    run_id: canonical_run_id,
                });
            }
        }
        task_runs.sort_by_key(|run| run.task_run.task_run.run_id);
        task_runs.dedup_by_key(|run| run.task_run.task_run.run_id);
        associations.sort_by_key(|association| association.run_id);
        associations.dedup_by_key(|association| association.run_id);
        live_ready_run_ids.sort_unstable();
        live_ready_run_ids.dedup();
        self.defer_provider_publication = false;
        self.defer_provider_model_publication = false;
        self.provider_model_publication_pending = false;
        self.deferred_provider_drain = None;
        self.rate_observation_context = None;
        let batch = PersistV6Batch {
            operations,
            task_runs,
            history_drains: if historical {
                history_manifest.cloned().into_iter().collect()
            } else {
                Vec::new()
            },
            history_associations: associations,
            history_publications,
            history_event_drain: match origin {
                ObservationOrigin::Historical { drain_id, .. } => Some(drain_id.clone()),
                ObservationOrigin::Live => None,
            },
            ..PersistV6Batch::default()
        };
        let requires_persistence_completion = !provider_v6_batch_is_empty(&batch);
        let has_ready_runs = !live_ready_run_ids.is_empty();
        let receipt = if historical {
            ProviderSubmissionReceipt::historical()
        } else if requires_persistence_completion {
            let checkpoint = match prior {
                Some(ProviderObservationPrior::Live(checkpoint)) if has_ready_runs => {
                    Some(checkpoint)
                }
                Some(ProviderObservationPrior::Live(_)) | None => None,
                Some(ProviderObservationPrior::Historical(_)) => {
                    panic!("a live observation cannot use a historical checkpoint")
                }
            };
            assert!(
                !has_ready_runs || checkpoint.is_some(),
                "a live observation touching private history must retain its checkpoint"
            );
            let submission_id = allocate_provider_submission_token();
            assert!(
                self.pending_live_provider_submission.is_none(),
                "a live provider submission must complete before another is installed"
            );
            self.pending_live_provider_submission = Some(PendingLiveProviderSubmission {
                submission_id,
                checkpoint,
                ready_run_ids: live_ready_run_ids,
            });
            ProviderSubmissionReceipt::live(Some(submission_id))
        } else {
            ProviderSubmissionReceipt::live(None)
        };
        #[cfg(feature = "workload-harness")]
        let receipt = if !historical && has_ready_runs {
            ProviderSubmissionReceipt {
                workload_timing: self.pending_workload_timing.take(),
                ..receipt
            }
        } else {
            receipt
        };
        if !historical && !has_ready_runs {
            #[cfg(feature = "workload-harness")]
            {
                let workload_timing = self.pending_workload_timing.take();
                self.publish_with_workload_timing(workload_timing);
            }
            #[cfg(not(feature = "workload-harness"))]
            self.publish();
        }
        (batch, receipt)
    }

    pub(crate) fn cancel_provider_observation(&mut self) {
        let publish = self.provider_model_publication_pending;
        self.defer_provider_publication = false;
        self.defer_provider_model_publication = false;
        self.provider_model_publication_pending = false;
        self.deferred_provider_drain = None;
        self.provider_observation_private_run_ids.clear();
        self.rate_observation_context = None;
        #[cfg(feature = "workload-harness")]
        self.pending_workload_timing.take();
        if publish {
            self.publish();
        }
    }

    pub(crate) fn apply_pane_agent_observation(
        &mut self,
        events: Vec<NormalizedEvent>,
        observation: PaneAgentStatusObservation,
    ) -> Result<ApplyOutcome, ReducerError> {
        self.apply_observation_inner(events, Some(observation))
    }

    fn apply_observation_inner(
        &mut self,
        events: Vec<NormalizedEvent>,
        pane_agent_observation: Option<PaneAgentStatusObservation>,
    ) -> Result<ApplyOutcome, ReducerError> {
        let pane_agent_status_changed =
            pane_agent_observation.as_ref().is_some_and(|observation| {
                self.model.pane(&observation.pane_id).is_some()
                    && self.model.pane_agent_status(&observation.pane_id)
                        != Some(observation.status)
            });
        if events.is_empty() && !pane_agent_status_changed {
            return Ok(ApplyOutcome::Applied(Vec::new()));
        }
        // increment5-workload-harness: begin observed apply scope start
        #[cfg(feature = "workload-harness")]
        let workload_timing = self
            .workload_observation_timing
            .as_mut()
            .and_then(|timing| {
                if timing.setup_observations_to_skip > 0 {
                    timing.setup_observations_to_skip -= 1;
                    return None;
                }
                let sequence = timing.next_sequence;
                timing.next_sequence = timing
                    .next_sequence
                    .checked_add(1)
                    .expect("workload fallback sequence overflowed");
                Some(workload_timing_scope(
                    timing.kind,
                    sequence,
                    Arc::clone(&timing.observer),
                ))
            });
        // increment5-workload-harness: end observed apply scope start
        let original_model = self.model.clone();
        let original_next_ordinal = self.next_ordinal;
        let original_terminal_event_sources = self.terminal_event_sources.clone();
        let original_non_lane_task_state_runs = self.non_lane_task_state_runs.clone();
        if pane_agent_status_changed {
            let observation = pane_agent_observation
                .expect("a changed pane-agent status must have an observation");
            self.pending_rate_observation_runs.extend(
                self.model
                    .executions()
                    .filter(|execution| execution.pane_id == observation.pane_id)
                    .map(|execution| execution.task_run_id),
            );
            self.model
                .set_pane_agent_status(observation.pane_id, observation.status);
        }
        let mut persist = Vec::new();
        for event in events {
            match self.apply_inner(event) {
                Ok(event_persist) => persist.extend(event_persist),
                Err(ReducerError::BindingConflict(conflict)) => {
                    self.model = original_model;
                    self.next_ordinal = original_next_ordinal;
                    self.terminal_event_sources = original_terminal_event_sources;
                    self.non_lane_task_state_runs = original_non_lane_task_state_runs;
                    return Ok(ApplyOutcome::DroppedBindingConflict(conflict));
                }
                Err(error) => {
                    self.model = original_model;
                    self.next_ordinal = original_next_ordinal;
                    self.terminal_event_sources = original_terminal_event_sources;
                    self.non_lane_task_state_runs = original_non_lane_task_state_runs;
                    return Err(error);
                }
            }
        }
        self.recompute_dangling_announcement_components();
        normalize_persist_batch_lineage(&mut persist);
        self.apply_operator_submission(&persist);
        self.publish();
        // increment5-workload-harness: begin observed apply scope finish
        #[cfg(feature = "workload-harness")]
        if let Some(timing) = workload_timing {
            if self.provider_model_publication_pending {
                assert!(
                    self.pending_workload_timing.is_none(),
                    "deferred workload timing scopes must not overlap"
                );
                self.pending_workload_timing = Some(timing.suspend());
            } else if !self.defer_provider_publication {
                timing.finish();
            }
        }
        // increment5-workload-harness: end observed apply scope finish
        Ok(ApplyOutcome::Applied(persist))
    }

    /// Returns the atomic diagnostics handle intended for the socket acceptor.
    #[must_use]
    pub fn controller_diagnostics_handle(&self) -> ControllerDiagnosticsHandle {
        self.model.controller_diagnostics().acceptor_handle()
    }

    #[cfg(test)]
    pub(crate) fn shared_publish_count(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.shared_publish_count)
    }

    /// Returns the atomic diagnostics handle intended for the provider I/O thread.
    #[must_use]
    pub fn provider_diagnostics_handle(&self) -> ProviderDiagnosticsHandle {
        self.model.provider_diagnostics().handle()
    }

    pub(crate) fn record_binding_conflict(&mut self) {
        self.model
            .controller_diagnostics_mut()
            .record_binding_conflict();
        self.publish();
    }

    pub(crate) fn record_provider_identity_disagreement(&mut self) {
        self.model
            .controller_diagnostics_mut()
            .record_provider_identity_disagreement();
        self.publish();
    }

    /// Resolves a raw Controller key through canonical and durable alias bindings.
    #[must_use]
    pub fn resolve_controller_run(&self, raw: &str) -> Option<RunId> {
        let exact = self
            .model
            .task_run_by_key(&RunKey::Controller(raw.to_owned()))
            .map(|run| run.run_id);
        if exact.is_some() {
            return exact;
        }

        // Shipped lane-created Codex runs used the bare SID as their Controller primary key.
        // Bridge only a root hook key to that restored owner; suffixed subagent/task keys and
        // unrelated Controller claimants must continue through normal K1 collision handling.
        let sid = raw.strip_prefix("hook:codex:")?;
        if sid.is_empty() || sid.contains(':') {
            return None;
        }
        self.model
            .task_run_by_key(&RunKey::Native {
                provider: Provider::Codex,
                sid: sid.to_owned(),
            })
            .filter(
                |owner| matches!(&owner.key, RunKey::Controller(legacy_sid) if legacy_sid == sid),
            )
            .map(|owner| owner.run_id)
    }

    /// Rebuilds transient reducer state from a durable-ledger duplicate during log replay.
    pub(crate) fn restore_replayed_controller_transients(&mut self, event: &ControllerEvent) {
        if event.metadata.source != crate::provider::lane::SOURCE_LOG_LANE {
            return;
        }
        let Some(run_id) = self.resolve_controller_run(&event.task_run_id) else {
            return;
        };
        match &event.event {
            ControllerEventKind::Dispatch { .. } | ControllerEventKind::TaskStarted => {
                let Some(kind) = event
                    .metadata
                    .provider_metadata
                    .as_ref()
                    .and_then(|provider| provider.event_kind.as_ref())
                else {
                    return;
                };
                if self.model.run_kind(&run_id).is_none() && !kind.is_empty() {
                    self.model.set_run_kind(run_id, kind.clone());
                    self.publish();
                }
            }
            ControllerEventKind::Complete
            | ControllerEventKind::Failed
            | ControllerEventKind::Cancelled => {
                self.terminal_event_sources
                    .entry(run_id)
                    .or_insert_with(|| crate::provider::lane::SOURCE_LOG_LANE.to_owned());
            }
            _ => {}
        }
    }

    /// Installs terminal-event provenance reconstructed from the durable event read model.
    pub fn restore_terminal_event_sources(&mut self, mut sources: HashMap<RunId, String>) {
        sources.retain(|run_id, _| self.model.task_run(run_id).is_some());
        self.terminal_event_sources = sources;
    }

    /// Installs non-lane task-state ownership reconstructed from the durable event read model.
    pub fn restore_non_lane_task_state_runs(&mut self, mut runs: HashSet<RunId>) {
        runs.retain(|run_id| self.model.task_run(run_id).is_some());
        self.non_lane_task_state_runs = runs;
    }

    /// Refreshes one visible, non-terminal run from a provider-log append.
    ///
    /// Dismissal is checked before [`TaskRun::touch`] because a non-terminal touch clears it.
    pub fn touch_run_liveness(&mut self, key: &RunKey, at_ms: i64) -> Vec<PersistOp> {
        self.touch_run_liveness_observed(key, at_ms, at_ms)
    }

    pub(crate) fn touch_run_liveness_observed(
        &mut self,
        key: &RunKey,
        at_ms: i64,
        observed_at_ms: i64,
    ) -> Vec<PersistOp> {
        let Some(mut task_run) = self.model.task_run_by_key(key).cloned() else {
            return Vec::new();
        };
        if task_run.dismissed_at_ms.is_some() {
            return Vec::new();
        }
        if task_run.state.is_terminal() {
            let mut persist = Vec::new();
            self.apply_native_lifecycle(
                task_run.run_id,
                None,
                NativeLifecycleWatermark {
                    source_at_ms: at_ms,
                    observed_at_ms,
                    source_order: format!(
                        "provider-liveness:{}",
                        stable_provider_lifecycle_run_key_identity(key)
                    ),
                },
                &mut persist,
            );
            if !persist.is_empty() {
                self.apply_operator_submission(&persist);
                self.publish();
            }
            return persist;
        }
        if task_run.updated_at_ms.is_some_and(|updated_at_ms| {
            updated_at_ms >= at_ms
                && self
                    .model
                    .task_run_v6_state(&task_run.run_id)
                    .is_none_or(|state| state.native_session_end.is_none())
        }) {
            return Vec::new();
        }
        Self::touch_task_run(&mut task_run, at_ms);
        self.model.insert_task_run(task_run.clone());
        let mut persist = vec![self.persist_task_run(task_run.clone(), at_ms)];
        self.apply_native_lifecycle(
            task_run.run_id,
            None,
            NativeLifecycleWatermark {
                source_at_ms: at_ms,
                observed_at_ms,
                source_order: format!(
                    "provider-liveness:{}",
                    stable_provider_lifecycle_run_key_identity(key)
                ),
            },
            &mut persist,
        );
        self.apply_operator_submission(&persist);
        self.publish();
        persist
    }

    /// Accumulates one lane-deduplicated output-token delta in transient model state.
    ///
    /// Telemetry deliberately emits no persistence operations. The provider lane owns usage
    /// sample deduplication, so this path adds every delta it receives exactly once.
    #[cfg(test)]
    pub(crate) fn apply_telemetry(
        &mut self,
        key: &RunKey,
        at_ms: i64,
        output_tokens: u64,
        model: Option<String>,
        effort: Option<String>,
        sandbox: Option<String>,
    ) -> Vec<PersistOp> {
        self.apply_telemetry_with_breakdown(
            key,
            at_ms,
            output_tokens,
            crate::model::TokenBreakdown::default(),
            crate::model::TurnAttr {
                model,
                effort,
                sandbox,
            },
        )
    }

    /// Accumulates one lane-deduplicated token sample in transient model state.
    ///
    /// Samples observed before their run exists remain transiently queued until any subsequent
    /// model publication exposes that run. The pending queue is globally FIFO-bounded by
    /// [`PENDING_TELEMETRY_SAMPLE_CAPACITY`].
    pub fn apply_telemetry_with_breakdown(
        &mut self,
        key: &RunKey,
        at_ms: i64,
        output_tokens: u64,
        token_breakdown: crate::model::TokenBreakdown,
        attribution: crate::model::TurnAttr,
    ) -> Vec<PersistOp> {
        let sample = PendingTelemetry {
            at_ms,
            output_tokens,
            token_breakdown,
            attribution,
        };
        if self.model.task_run_by_key(key).is_some() {
            let applied = self.accumulate_telemetry(key, sample);
            debug_assert!(applied, "an existing telemetry run must remain resolvable");
            self.publish();
        } else {
            self.enqueue_pending_telemetry(key.clone(), sample);
        }
        Vec::new()
    }

    fn accumulate_telemetry(&mut self, key: &RunKey, sample: PendingTelemetry) -> bool {
        let Some(run_id) = self
            .model
            .task_run_by_key(key)
            .map(|task_run| task_run.run_id)
        else {
            return false;
        };
        let retain_turn = matches!(
            key,
            RunKey::Native {
                provider: Provider::Codex,
                ..
            }
        );
        let telemetry = self.model.telemetry_entry(run_id, sample.at_ms);
        telemetry.token_breakdown.accumulate(sample.token_breakdown);
        telemetry.accumulate(
            sample.output_tokens,
            sample.attribution.model,
            sample.attribution.effort,
            sample.attribution.sandbox,
            retain_turn,
        );
        self.pending_rate_observation_runs.insert(run_id);
        true
    }

    fn enqueue_pending_telemetry(&mut self, key: RunKey, sample: PendingTelemetry) {
        while self.pending_telemetry_count >= PENDING_TELEMETRY_SAMPLE_CAPACITY {
            if !self.evict_oldest_pending_telemetry() {
                break;
            }
        }
        self.pending_telemetry
            .entry(key.clone())
            .or_default()
            .push_back(sample);
        self.pending_telemetry_order.push_back(key);
        self.pending_telemetry_count += 1;
    }

    fn evict_oldest_pending_telemetry(&mut self) -> bool {
        while let Some(key) = self.pending_telemetry_order.pop_front() {
            let Some(samples) = self.pending_telemetry.get_mut(&key) else {
                continue;
            };
            if samples.pop_front().is_none() {
                continue;
            }
            self.pending_telemetry_count -= 1;
            if samples.is_empty() {
                self.pending_telemetry.remove(&key);
            }
            return true;
        }
        debug_assert_eq!(self.pending_telemetry_count, 0);
        // Self-heal the counter after exhausting the authoritative eviction order.
        self.pending_telemetry_count = 0;
        false
    }

    fn apply_pending_telemetry_for_known_runs(&mut self) {
        let mut seen = HashSet::new();
        let ready = self
            .pending_telemetry_order
            .iter()
            .filter(|key| self.model.task_run_by_key(key).is_some())
            .filter(|key| seen.insert((*key).clone()))
            .cloned()
            .collect::<Vec<_>>();
        let ready_keys = ready.iter().cloned().collect::<HashSet<_>>();
        self.pending_telemetry_order
            .retain(|pending_key| !ready_keys.contains(pending_key));
        for key in ready {
            let Some(samples) = self.pending_telemetry.remove(&key) else {
                continue;
            };
            self.pending_telemetry_count =
                self.pending_telemetry_count.saturating_sub(samples.len());
            for sample in samples {
                let applied = self.accumulate_telemetry(&key, sample);
                debug_assert!(
                    applied,
                    "a ready pending-telemetry run must remain resolvable"
                );
            }
        }
    }

    /// Closes one active provider-log run after append inactivity.
    ///
    /// This lane-specific path deliberately ignores `has_controller_task_state_event`, which is
    /// set by synthesized events and makes the observation-close path unsuitable here.
    pub fn apply_lane_close(&mut self, key: &RunKey, at_ms: i64) -> Vec<PersistOp> {
        self.apply_lane_close_observed(key, at_ms, at_ms)
    }

    pub(crate) fn apply_lane_close_observed(
        &mut self,
        key: &RunKey,
        at_ms: i64,
        observed_at_ms: i64,
    ) -> Vec<PersistOp> {
        let Some(mut task_run) = self.model.task_run_by_key(key).cloned() else {
            return Vec::new();
        };
        let has_live_execution = self.model.executions().any(|execution| {
            execution.task_run_id == task_run.run_id && !execution.state.is_terminal()
        });
        if task_run.dismissed_at_ms.is_some()
            || task_run.state.is_terminal()
            || has_live_execution
            || self.non_lane_task_state_runs.contains(&task_run.run_id)
        {
            return Vec::new();
        }
        if native_binding(&self.model, task_run.run_id).is_some() {
            let mut persist = Vec::new();
            self.apply_native_lifecycle(
                task_run.run_id,
                Some(NativeSessionEndStatus::Unknown),
                NativeLifecycleWatermark {
                    source_at_ms: at_ms,
                    observed_at_ms,
                    source_order: format!(
                        "provider-lane-close:{}",
                        stable_provider_lifecycle_run_key_identity(key)
                    ),
                },
                &mut persist,
            );
            self.apply_operator_submission(&persist);
            self.publish();
            return persist;
        }
        task_run.state = TaskState::EndedUnknown;
        Self::touch_task_run(&mut task_run, at_ms);
        self.model.insert_task_run(task_run.clone());
        let persist = vec![self.persist_task_run(task_run, at_ms)];
        self.apply_operator_submission(&persist);
        self.publish();
        persist
    }

    fn apply_native_lifecycle(
        &mut self,
        run_id: RunId,
        end_status: Option<NativeSessionEndStatus>,
        watermark: NativeLifecycleWatermark,
        persist: &mut PersistBatch,
    ) {
        let mut state = self
            .model
            .task_run_v6_state(&run_id)
            .cloned()
            .unwrap_or_else(TaskRunV6State::default);
        if state
            .lifecycle_watermark
            .as_ref()
            .is_some_and(|stored| stored >= &watermark)
        {
            return;
        }
        state.native_session_end = end_status.map(|status| NativeSessionEnd {
            status,
            at_ms: watermark.source_at_ms,
        });
        state.lifecycle_watermark = Some(watermark.clone());
        self.model.set_task_run_v6_state(run_id, state);
        let already_persisted = persist.iter().any(|operation| match operation {
            PersistOp::UpsertTaskRun(value) => value.task_run.run_id == run_id,
            PersistOp::PromoteTaskRunKey { promoted, .. } => promoted.task_run.run_id == run_id,
            _ => false,
        });
        if !already_persisted && let Some(run) = self.model.task_run(&run_id).cloned() {
            persist.push(self.persist_task_run(run, watermark.observed_at_ms));
        }
    }

    /// Executes the complete Controller mutation sequence on one throwaway clone.
    pub fn validate_controller_event(
        &self,
        event: &ControllerEvent,
    ) -> Result<MaterializedDelta, RejectReason> {
        if event.schema_version > 1 {
            return Err(RejectReason::UnsupportedVersion);
        }
        if event.schema_version != 1
            || event.metadata.event_id.is_empty()
            || event.metadata.event_id.starts_with("prov:")
            || event.metadata.source.is_empty()
            || event.task_run_id.is_empty()
            || event.metadata.native_session_id.is_some() && event.metadata.provider.is_none()
            || event.metadata.source == crate::provider::lane::SOURCE_LOG_LANE
                && event.metadata.timestamp_ms <= 0
            || event
                .metadata
                .progress
                .is_some_and(|progress| progress > 10_000)
        {
            return Err(RejectReason::Invalid);
        }

        let subject_was_unknown = self.resolve_controller_run(&event.task_run_id).is_none();
        let subject = self
            .resolve_controller_run(&event.task_run_id)
            .unwrap_or_default();
        let mut metadata = event.metadata.clone();
        metadata.label = metadata.label.as_deref().map(sanitize_controller_text);
        metadata.reason = metadata.reason.as_deref().map(sanitize_controller_text);
        metadata.task_run_id = Some(subject);
        metadata.source_event_type = controller_kind_name(&event.event).to_owned();
        metadata.task_state = controller_target_state(&event.event);
        metadata.execution_parent = None;
        metadata.dependency = None;
        metadata.ingest_seq = None;
        let lane_native_end = metadata.source == crate::provider::lane::SOURCE_LOG_LANE
            && metadata.provider.is_some()
            && metadata
                .native_session_id
                .as_deref()
                .is_some_and(|sid| !sid.is_empty())
            && matches!(
                event.event,
                ControllerEventKind::Complete
                    | ControllerEventKind::Failed
                    | ControllerEventKind::Cancelled
            );
        if lane_native_end {
            metadata.task_state = None;
        }

        match &event.event {
            ControllerEventKind::Dispatch { parent_task_run_id } => {
                if parent_task_run_id.is_empty() {
                    return Err(RejectReason::Invalid);
                }
                let parent = if parent_task_run_id == &event.task_run_id {
                    subject
                } else {
                    self.resolve_controller_run(parent_task_run_id)
                        .unwrap_or_default()
                };
                metadata.execution_parent = Some(ExecutionEdge {
                    parent_run_id: parent,
                    child_run_id: subject,
                });
            }
            ControllerEventKind::DependsOn { depends_on_id } => {
                if depends_on_id.is_empty() {
                    return Err(RejectReason::Invalid);
                }
                let prerequisite = if depends_on_id == &event.task_run_id {
                    subject
                } else {
                    self.resolve_controller_run(depends_on_id)
                        .unwrap_or_default()
                };
                metadata.dependency = Some(DependencyEdge {
                    prerequisite_run_id: prerequisite,
                    dependent_run_id: subject,
                });
            }
            ControllerEventKind::TaskStarted
            | ControllerEventKind::Blocked
            | ControllerEventKind::Progress
            | ControllerEventKind::Complete
            | ControllerEventKind::Failed
            | ControllerEventKind::Cancelled
            | ControllerEventKind::SessionEnded
            | ControllerEventKind::Dismiss => {}
        }

        let (mut scratch, _scratch_shared) = Self::new(RestoredState {
            model: self.model.clone(),
            next_ordinal: self.next_ordinal,
            next_ingest_seq: self.next_ingest_seq,
            event_ledger: Vec::new(),
        });
        scratch.terminal_event_sources = self.terminal_event_sources.clone();
        scratch.non_lane_task_state_runs = self.non_lane_task_state_runs.clone();
        if matches!(event.event, ControllerEventKind::SessionEnded) {
            let bound = self
                .resolve_controller_run(&event.task_run_id)
                .and_then(|run_id| native_binding(&self.model, run_id))
                .filter(|binding| {
                    metadata.provider == Some(binding.provider)
                        && metadata.native_session_id.as_deref()
                            == Some(binding.native_session_id.as_str())
                });
            if bound.is_none() {
                metadata.task_run_id = None;
                let normalized = NormalizedEvent::ControllerEvent {
                    metadata: metadata.clone(),
                    event: event.event.clone(),
                };
                return Ok(MaterializedDelta {
                    post_model: scratch.model,
                    post_next_ordinal: scratch.next_ordinal,
                    post_terminal_event_sources: scratch.terminal_event_sources,
                    post_non_lane_task_state_runs: scratch.non_lane_task_state_runs,
                    diagnostic_deltas: ControllerDiagnosticDeltas {
                        post_dangling_announcement_components:
                            crate::model::graph::dangling_announcement_components(&self.model),
                        ..ControllerDiagnosticDeltas::default()
                    },
                    batch: vec![PersistOp::RecordEvent {
                        event: Box::new(normalized),
                        seen_at_ms: metadata.receipt_time_ms,
                    }],
                });
            }
        }
        let drops_unknown_lane_terminal = subject_was_unknown
            && metadata.source == crate::provider::lane::SOURCE_LOG_LANE
            && matches!(
                event.event,
                ControllerEventKind::Complete
                    | ControllerEventKind::Failed
                    | ControllerEventKind::Cancelled
            );
        if subject_was_unknown
            && (matches!(event.event, ControllerEventKind::Dismiss) || drops_unknown_lane_terminal)
        {
            metadata.task_run_id = None;
        }
        let normalized = NormalizedEvent::ControllerEvent {
            metadata: metadata.clone(),
            event: event.event.clone(),
        };
        let mut persist = Vec::new();
        if drops_unknown_lane_terminal {
            let diagnostic_deltas = ControllerDiagnosticDeltas {
                unknown_lane_terminal_drops: 1,
                post_dangling_announcement_components:
                    crate::model::graph::dangling_announcement_components(&scratch.model),
                ..ControllerDiagnosticDeltas::default()
            };
            return Ok(MaterializedDelta {
                post_model: scratch.model,
                post_next_ordinal: scratch.next_ordinal,
                post_terminal_event_sources: scratch.terminal_event_sources,
                post_non_lane_task_state_runs: scratch.non_lane_task_state_runs,
                diagnostic_deltas,
                batch: persist,
            });
        }
        if matches!(event.event, ControllerEventKind::Dismiss) {
            if let Some(mut task_run) = scratch.model.task_run(&subject).cloned() {
                task_run.dismissed_at_ms = Some(metadata.receipt_time_ms);
                scratch.model.insert_task_run(task_run.clone());
                persist.push(scratch.persist_task_run(task_run, metadata.receipt_time_ms));
            }
            persist.push(PersistOp::RecordEvent {
                event: Box::new(normalized),
                seen_at_ms: metadata.receipt_time_ms,
            });
            let diagnostic_deltas = ControllerDiagnosticDeltas {
                post_dangling_announcement_components:
                    crate::model::graph::dangling_announcement_components(&scratch.model),
                ..ControllerDiagnosticDeltas::default()
            };
            return Ok(MaterializedDelta {
                post_model: scratch.model,
                post_next_ordinal: scratch.next_ordinal,
                post_terminal_event_sources: scratch.terminal_event_sources,
                post_non_lane_task_state_runs: scratch.non_lane_task_state_runs,
                diagnostic_deltas,
                batch: persist,
            });
        }
        let initial_state = metadata.task_state.map_or(TaskState::Queued, |state| {
            initial_controller_state(&metadata.source_event_type, state)
        });
        scratch
            .ensure_metadata_run(
                subject,
                &metadata,
                true,
                Some(&event.task_run_id),
                initial_state,
                &mut persist,
            )
            .map_err(|_| RejectReason::Conflict)?;
        match (
            &event.event,
            &metadata.execution_parent,
            &metadata.dependency,
        ) {
            (ControllerEventKind::Dispatch { parent_task_run_id }, Some(edge), None) => {
                scratch
                    .ensure_controller_placeholder(
                        edge.parent_run_id,
                        Some(parent_task_run_id),
                        Self::run_bookkeeping_time_ms(&metadata),
                        &mut persist,
                    )
                    .map_err(|_| RejectReason::Conflict)?;
            }
            (ControllerEventKind::DependsOn { depends_on_id }, None, Some(edge)) => {
                scratch
                    .ensure_controller_placeholder(
                        edge.prerequisite_run_id,
                        Some(depends_on_id),
                        Self::run_bookkeeping_time_ms(&metadata),
                        &mut persist,
                    )
                    .map_err(|_| RejectReason::Conflict)?;
            }
            (ControllerEventKind::Dispatch { .. }, _, _)
            | (ControllerEventKind::DependsOn { .. }, _, _) => {
                return Err(RejectReason::Invalid);
            }
            _ => {}
        }

        let allow_lane_reopen = false;
        let mut diagnostic_deltas = validate_controller_transition(
            &scratch.model,
            &event.event,
            subject,
            subject_was_unknown,
            metadata.execution_parent.as_ref(),
            metadata.dependency.as_ref(),
            lane_native_end,
        )?;
        scratch.apply_controller_metadata(&metadata, allow_lane_reopen, &mut persist);
        scratch
            .apply_event_body(&normalized, &metadata, &mut persist)
            .map_err(|_| RejectReason::Conflict)?;
        scratch
            .apply_identity_metadata(&normalized, &metadata, &mut persist)
            .map_err(|error| match error {
                ReducerError::BindingConflict(conflict) => reject_merge_conflict(&conflict),
                ReducerError::OrdinalExhausted => RejectReason::Conflict,
            })?;
        let lifecycle_run_id = metadata
            .provider
            .zip(metadata.native_session_id.as_deref())
            .and_then(|(provider, sid)| {
                scratch
                    .model
                    .task_run_by_key(&RunKey::Native {
                        provider,
                        sid: sid.to_owned(),
                    })
                    .map(|run| run.run_id)
            })
            .or_else(|| scratch.resolve_controller_run(&event.task_run_id));
        if let Some(run_id) = lifecycle_run_id {
            let end_status = if matches!(event.event, ControllerEventKind::SessionEnded) {
                Some(NativeSessionEndStatus::Done)
            } else if lane_native_end {
                match event.event {
                    ControllerEventKind::Complete => Some(NativeSessionEndStatus::Done),
                    ControllerEventKind::Failed => Some(NativeSessionEndStatus::Error),
                    ControllerEventKind::Cancelled => Some(NativeSessionEndStatus::Cancelled),
                    _ => None,
                }
            } else {
                None
            };
            if end_status.is_some() || matches!(event.event, ControllerEventKind::TaskStarted) {
                scratch.apply_native_lifecycle(
                    run_id,
                    end_status,
                    NativeLifecycleWatermark {
                        source_at_ms: metadata.timestamp_ms,
                        observed_at_ms: metadata.receipt_time_ms,
                        source_order: metadata.event_id.clone(),
                    },
                    &mut persist,
                );
            }
        }
        scratch.persist_event_execution(&normalized, metadata.receipt_time_ms, &mut persist);
        if !lane_native_end {
            scratch.update_terminal_event_source(&normalized);
        }
        persist.push(PersistOp::RecordEvent {
            event: Box::new(normalized),
            seen_at_ms: metadata.receipt_time_ms,
        });
        // increment5-workload-harness: begin controller D4 timing start
        #[cfg(feature = "workload-harness")]
        let workload_d4_started = Instant::now();
        // increment5-workload-harness: end controller D4 timing start
        diagnostic_deltas.post_dangling_announcement_components =
            crate::model::graph::dangling_announcement_components(&scratch.model);
        // increment5-workload-harness: begin controller D4 timing finish
        #[cfg(feature = "workload-harness")]
        record_workload_timing_segment(WorkloadTimingSegment::D4, workload_d4_started.elapsed());
        // increment5-workload-harness: end controller D4 timing finish

        Ok(MaterializedDelta {
            post_model: scratch.model,
            post_next_ordinal: scratch.next_ordinal,
            post_terminal_event_sources: scratch.terminal_event_sources,
            post_non_lane_task_state_runs: scratch.non_lane_task_state_runs,
            diagnostic_deltas,
            batch: persist,
        })
    }

    /// Allocates/stamps one sequence, swaps the staged state, publishes once, and consumes a permit.
    pub fn commit_staged(
        &mut self,
        mut delta: MaterializedDelta,
        permit: EnqueuePermit<'_>,
    ) -> Result<PendingEnqueue, CommitStagedError> {
        let Some(ingest_seq) = self.next_ingest_seq.filter(|value| *value > 0) else {
            self.model
                .controller_diagnostics_mut()
                .record_ingest_sequence_exhaustion();
            self.publish();
            return Err(CommitStagedError::IngestSequenceExhausted);
        };
        let Ok(ingest_seq_u64) = u64::try_from(ingest_seq) else {
            self.model
                .controller_diagnostics_mut()
                .record_ingest_sequence_exhaustion();
            self.publish();
            return Err(CommitStagedError::IngestSequenceExhausted);
        };
        for operation in &mut delta.batch {
            if let PersistOp::RecordEvent { event, .. } = operation {
                event_metadata_mut(event).ingest_seq = Some(ingest_seq_u64);
            }
        }
        let mut batch = vec![PersistOp::AdvanceIngestSequence { ingest_seq }];
        batch.extend(delta.batch);
        normalize_persist_batch_lineage(&mut batch);

        self.model = delta.post_model;
        self.next_ordinal = delta.post_next_ordinal;
        self.terminal_event_sources = delta.post_terminal_event_sources;
        self.non_lane_task_state_runs = delta.post_non_lane_task_state_runs;
        self.next_ingest_seq = ingest_seq.checked_add(1);
        self.apply_controller_diagnostic_deltas(delta.diagnostic_deltas);
        self.apply_operator_submission(&batch);
        self.publish();
        Ok(permit.enqueue_v6(self.decorate_v6_batch(batch)))
    }

    /// Installs a validated provider delta while returning its transaction for v6 decoration.
    pub(crate) fn commit_staged_unqueued(
        &mut self,
        mut delta: MaterializedDelta,
    ) -> Result<PersistBatch, CommitStagedError> {
        let Some(ingest_seq) = self.next_ingest_seq.filter(|value| *value > 0) else {
            self.model
                .controller_diagnostics_mut()
                .record_ingest_sequence_exhaustion();
            self.publish();
            return Err(CommitStagedError::IngestSequenceExhausted);
        };
        let Ok(ingest_seq_u64) = u64::try_from(ingest_seq) else {
            self.model
                .controller_diagnostics_mut()
                .record_ingest_sequence_exhaustion();
            self.publish();
            return Err(CommitStagedError::IngestSequenceExhausted);
        };
        for operation in &mut delta.batch {
            if let PersistOp::RecordEvent { event, .. } = operation {
                event_metadata_mut(event).ingest_seq = Some(ingest_seq_u64);
            }
        }
        let mut batch = vec![PersistOp::AdvanceIngestSequence { ingest_seq }];
        batch.extend(delta.batch);
        normalize_persist_batch_lineage(&mut batch);

        self.model = delta.post_model;
        self.next_ordinal = delta.post_next_ordinal;
        self.terminal_event_sources = delta.post_terminal_event_sources;
        self.non_lane_task_state_runs = delta.post_non_lane_task_state_runs;
        self.next_ingest_seq = ingest_seq.checked_add(1);
        self.apply_controller_diagnostic_deltas(delta.diagnostic_deltas);
        self.apply_operator_submission(&batch);
        self.publish();
        Ok(batch)
    }

    fn apply_controller_diagnostic_deltas(&mut self, deltas: ControllerDiagnosticDeltas) {
        let diagnostics = self.model.controller_diagnostics_mut();
        diagnostics.record_terminal_blocked_progress_noops(deltas.terminal_blocked_progress_noops);
        diagnostics.record_terminal_forward_reference_creations(
            deltas.terminal_forward_reference_creations,
        );
        diagnostics.record_unknown_lane_terminal_drops(deltas.unknown_lane_terminal_drops);
        diagnostics
            .set_dangling_announcement_components(deltas.post_dangling_announcement_components);
    }

    pub(crate) fn decorate_v6_batch(&self, operations: PersistBatch) -> PersistV6Batch {
        let touched = operations
            .iter()
            .filter_map(|operation| match operation {
                PersistOp::UpsertTaskRun(value) => Some(value.task_run.run_id),
                PersistOp::PromoteTaskRunKey { promoted, .. } => Some(promoted.task_run.run_id),
                _ => None,
            })
            .collect::<HashSet<_>>();
        let task_runs = touched
            .into_iter()
            .filter_map(|run_id| self.persist_task_run_v6(run_id))
            .collect();
        PersistV6Batch {
            operations,
            task_runs,
            ..PersistV6Batch::default()
        }
    }

    pub(crate) fn decorate_reconciliation_batch(&self, operations: PersistBatch) -> PersistV6Batch {
        let referenced_runs = operations
            .iter()
            .filter_map(|operation| match operation {
                PersistOp::UpsertExecution(value) => Some(value.execution.task_run_id),
                PersistOp::UpsertAgentNode(node) => Some(node.task_run_id),
                _ => None,
            })
            .collect::<HashSet<_>>();
        let mut batch = self.decorate_v6_batch(operations);
        let mut seeded = batch
            .task_runs
            .iter()
            .map(|run| run.task_run.task_run.run_id)
            .collect::<HashSet<_>>();
        for run_id in referenced_runs {
            if seeded.insert(run_id)
                && let Some(task_run) = self.persist_task_run_v6(run_id)
            {
                batch.task_runs.push(task_run);
            }
        }
        batch
    }

    pub(crate) fn decorate_rate_checkpoint(
        &self,
        operations: PersistBatch,
        rate_totals: Vec<(RunId, RunRateTotals)>,
    ) -> PersistV6Batch {
        let mut batch = self.decorate_v6_batch(operations);
        let mut seeded = batch
            .task_runs
            .iter()
            .map(|run| run.task_run.task_run.run_id)
            .collect::<HashSet<_>>();
        for (run_id, _) in &rate_totals {
            if seeded.insert(*run_id)
                && let Some(task_run) = self.persist_task_run_v6(*run_id)
            {
                batch.task_runs.push(task_run);
            }
        }
        batch.rate_totals = rate_totals;
        batch
    }

    fn persist_task_run_v6(&self, run_id: RunId) -> Option<PersistTaskRunV6> {
        let run = self.model.task_run(&run_id)?.clone();
        let state = self.model.task_run_v6_state(&run_id)?.clone();
        let PersistOp::UpsertTaskRun(task_run) = self.persist_task_run(
            run,
            state
                .lifecycle_watermark
                .as_ref()
                .map_or(0, |watermark| watermark.observed_at_ms),
        ) else {
            unreachable!("persist_task_run always returns an upsert");
        };
        Some(PersistTaskRunV6 { task_run, state })
    }

    fn apply_inner(&mut self, mut event: NormalizedEvent) -> Result<PersistBatch, ReducerError> {
        let lane_native_terminal = matches!(
            &event,
            NormalizedEvent::ControllerEvent {
                metadata,
                event:
                    ControllerEventKind::Complete
                    | ControllerEventKind::Failed
                    | ControllerEventKind::Cancelled,
            } if metadata.source == crate::provider::lane::SOURCE_LOG_LANE
                && metadata.provider.is_some()
                && metadata
                    .native_session_id
                    .as_deref()
                    .is_some_and(|sid| !sid.is_empty())
        );
        if lane_native_terminal {
            event_metadata_mut(&mut event).task_state = None;
        }
        let metadata = event_metadata(&event).clone();
        let mut persist = Vec::new();

        self.ensure_event_runs(&event, &metadata, &mut persist)?;
        self.apply_controller_metadata(&metadata, false, &mut persist);
        self.apply_event_body(&event, &metadata, &mut persist)?;
        self.apply_identity_metadata(&event, &metadata, &mut persist)?;
        let lifecycle_end = match &event {
            NormalizedEvent::ControllerEvent {
                event: ControllerEventKind::Complete,
                ..
            } if lane_native_terminal => Some(NativeSessionEndStatus::Done),
            NormalizedEvent::ControllerEvent {
                event: ControllerEventKind::Failed,
                ..
            } if lane_native_terminal => Some(NativeSessionEndStatus::Error),
            NormalizedEvent::ControllerEvent {
                event: ControllerEventKind::Cancelled,
                ..
            } if lane_native_terminal => Some(NativeSessionEndStatus::Cancelled),
            _ => None,
        };
        let lifecycle_reopen = matches!(
            &event,
            NormalizedEvent::ControllerEvent {
                event: ControllerEventKind::TaskStarted,
                ..
            }
        ) && metadata.provider.is_some()
            && metadata
                .native_session_id
                .as_deref()
                .is_some_and(|sid| !sid.is_empty());
        if (lifecycle_end.is_some() || lifecycle_reopen)
            && let Some(run_id) = metadata
                .provider
                .zip(metadata.native_session_id.as_deref())
                .and_then(|(provider, sid)| {
                    self.model
                        .task_run_by_key(&RunKey::Native {
                            provider,
                            sid: sid.to_owned(),
                        })
                        .map(|run| run.run_id)
                })
        {
            self.apply_native_lifecycle(
                run_id,
                lifecycle_end,
                NativeLifecycleWatermark {
                    source_at_ms: metadata.timestamp_ms,
                    observed_at_ms: metadata.receipt_time_ms,
                    source_order: metadata.event_id.clone(),
                },
                &mut persist,
            );
        }
        let matching_live = match &event {
            NormalizedEvent::ExecutionBegin { execution, .. } => !execution.state.is_terminal(),
            NormalizedEvent::AgentStatusChanged { execution_id, .. } => self
                .model
                .execution(execution_id)
                .is_some_and(|execution| !execution.state.is_terminal()),
            NormalizedEvent::AgentNodeUpsert { node, .. } => node
                .state
                .as_ref()
                .is_some_and(|state| !state.is_terminal()),
            NormalizedEvent::AgentActivity { .. } => true,
            _ => false,
        };
        if matching_live
            && let Some(run_id) = metadata
                .provider
                .zip(metadata.native_session_id.as_deref())
                .and_then(|(provider, sid)| {
                    self.model
                        .task_run_by_key(&RunKey::Native {
                            provider,
                            sid: sid.to_owned(),
                        })
                        .map(|run| run.run_id)
                })
        {
            self.apply_native_lifecycle(
                run_id,
                None,
                NativeLifecycleWatermark {
                    source_at_ms: metadata.timestamp_ms,
                    observed_at_ms: metadata.receipt_time_ms,
                    source_order: metadata.event_id.clone(),
                },
                &mut persist,
            );
        }
        self.persist_event_execution(&event, metadata.receipt_time_ms, &mut persist);
        if !lane_native_terminal {
            self.update_terminal_event_source(&event);
        }
        persist.push(PersistOp::RecordEvent {
            event: Box::new(event),
            seen_at_ms: metadata.receipt_time_ms,
        });

        Ok(persist)
    }

    /// Replaces physical topology across an observation gap in one coherent batch.
    pub fn reconcile_gap(&mut self, batch: ReconcileBatch) -> Result<PersistBatch, ReducerError> {
        self.reconcile_snapshot(batch.topology)
    }

    /// Atomically replaces physical topology from one complete authoritative snapshot.
    pub fn reconcile_snapshot(
        &mut self,
        topology: TopologySnapshot,
    ) -> Result<PersistBatch, ReducerError> {
        let original_model = self.model.clone();
        let original_next_ordinal = self.next_ordinal;
        match self.reconcile_snapshot_inner(topology) {
            Ok(mut persist) => {
                normalize_persist_batch_lineage(&mut persist);
                self.recompute_dangling_announcement_components();
                self.apply_operator_submission(&persist);
                self.apply_pending_telemetry_for_known_runs();
                self.begin_rate_epoch();
                self.activate_rate_epoch(unix_now_ms());
                self.publish();
                Ok(persist)
            }
            Err(error) => {
                self.model = original_model;
                self.next_ordinal = original_next_ordinal;
                Err(error)
            }
        }
    }

    /// Applies the runtime durability truth to the preceding operator submission.
    pub(crate) fn complete_operator_submission(&mut self, outcome: RuntimeWriteOutcome) {
        self.operator.complete_submission(outcome);
    }

    /// Applies one provider acknowledgement to only the readiness transitions it submitted.
    pub(crate) fn complete_provider_submission(
        &mut self,
        receipt: ProviderSubmissionReceipt,
        outcome: RuntimeWriteOutcome,
    ) {
        #[cfg(feature = "workload-harness")]
        let workload_timing = receipt.workload_timing;
        let ProviderSubmissionReceiptKind::Live { submission_id } = receipt.kind else {
            self.complete_deferred_operator_submission(outcome);
            return;
        };
        let Some(submission_id) = submission_id else {
            return;
        };
        if self
            .pending_live_provider_submission
            .as_ref()
            .map(|pending| pending.submission_id)
            != Some(submission_id)
        {
            return;
        }
        let pending = self
            .pending_live_provider_submission
            .take()
            .expect("the matching live provider submission must remain pending");
        self.complete_operator_submission(outcome);
        if !matches!(
            outcome,
            RuntimeWriteOutcome::Durable | RuntimeWriteOutcome::CommittedButDegraded(_)
        ) {
            if let Some(checkpoint) = pending.checkpoint {
                (*checkpoint).restore(self);
            }
            return;
        }

        let mut publish = pending.checkpoint.is_some();
        for run_id in pending.ready_run_ids {
            if let Some(mut state) = self.model.task_run_v6_state(&run_id).cloned() {
                if !state.history_ready {
                    state.history_ready = true;
                    self.model.set_task_run_v6_state(run_id, state);
                    publish = true;
                }
                let released = self.model.release_history_publications(run_id);
                publish |= released;
            }
        }
        if publish {
            #[cfg(feature = "workload-harness")]
            self.publish_with_workload_timing(workload_timing);
            #[cfg(not(feature = "workload-harness"))]
            self.publish();
        }
    }

    /// Applies durability truth to a historical provider submission without publishing it.
    pub(crate) fn complete_deferred_operator_submission(&mut self, outcome: RuntimeWriteOutcome) {
        self.operator
            .complete_submission_without_publishing(outcome);
    }

    fn apply_operator_submission(&mut self, persist: &[PersistOp]) {
        for operation in persist {
            let touched_run = match operation {
                PersistOp::UpsertTaskRun(value) => Some(value.task_run.run_id),
                PersistOp::PromoteTaskRunKey { promoted, .. } => Some(promoted.task_run.run_id),
                PersistOp::MergeTaskRuns { survivor, absorbed } => {
                    self.dirty_rate_totals.remove(absorbed);
                    self.dirty_rate_totals.insert(*survivor);
                    Some(*survivor)
                }
                PersistOp::UpsertExecution(value) => Some(value.execution.task_run_id),
                PersistOp::UpsertAgentNode(node) => Some(node.task_run_id),
                PersistOp::RecordEvent { event, .. } => event_metadata(event).task_run_id,
                _ => None,
            };
            if let Some(run_id) = touched_run {
                self.pending_rate_observation_runs.insert(run_id);
            }
        }
        if self.defer_provider_publication {
            self.operator.apply_submission_without_publishing(
                self.deferred_provider_drain
                    .as_ref()
                    .expect("a deferred provider observation must name its history drain"),
                persist,
            );
        } else {
            self.operator.apply_submission(persist);
        }
    }

    fn reconcile_snapshot_inner(
        &mut self,
        topology: TopologySnapshot,
    ) -> Result<PersistBatch, ReducerError> {
        let now_ms = unix_now_ms();
        let mut persist = Vec::new();
        let mut pre_gap_executions: Vec<_> = self.model.executions().cloned().collect();
        pre_gap_executions.sort_by(|left, right| left.execution_id.cmp(&right.execution_id));
        let mut pre_gap_runs: Vec<_> = pre_gap_executions
            .iter()
            .map(|execution| execution.task_run_id)
            .collect();
        pre_gap_runs.sort_unstable();
        pre_gap_runs.dedup();

        for mut execution in pre_gap_executions {
            if execution.state.is_terminal() {
                continue;
            }
            execution.state = ExecState::Ended;
            self.model.insert_execution(execution.clone());
            persist.push(persist_execution(execution, now_ms));
        }

        self.replace_topology(&topology, &mut persist)?;

        let pane_agent_statuses = topology
            .panes
            .iter()
            .filter(|pane| self.model.pane(&pane.pane_id).is_some())
            .filter_map(|pane| {
                pane.agent
                    .as_ref()
                    .map(|agent| (pane.pane_id.clone(), agent.status))
            })
            .collect();
        self.model.replace_pane_agent_statuses(pane_agent_statuses);

        for pane in topology.panes {
            let Some(agent) = pane.agent else {
                continue;
            };
            let provider = snapshot_provider(
                &agent.agent_name,
                pane.agent_session
                    .as_ref()
                    .map(|reference| reference.agent.as_str()),
            );
            let native_sid = pane.agent_session.as_ref().and_then(|reference| {
                (reference.kind == AgentSessionReferenceKind::Id && !reference.value.is_empty())
                    .then(|| reference.value.clone())
            });
            let native_path = pane.agent_session.as_ref().and_then(|reference| {
                (reference.kind == AgentSessionReferenceKind::Path && !reference.value.is_empty())
                    .then_some(reference.value.as_str())
            });
            let existing_run = match (provider, native_sid.as_deref(), native_path) {
                (Some(provider), Some(sid), _) => self.run_for_native_session(provider, sid),
                (Some(provider), None, Some(path)) => self
                    .model
                    .task_run_by_key(&RunKey::NativePath {
                        provider,
                        path: path.to_owned(),
                    })
                    .map(|task_run| task_run.run_id),
                _ => None,
            };
            let run_id = match existing_run {
                Some(run_id) => run_id,
                None => self.insert_snapshot_run(
                    provider,
                    native_sid.as_deref(),
                    native_path,
                    &pane.terminal_id,
                    now_ms,
                    &mut persist,
                )?,
            };
            let existing_execution_id = self
                .model
                .executions()
                .filter(|execution| {
                    execution.pane_id.as_str() == pane.pane_id.as_str()
                        && execution.terminal_id.as_str() == pane.terminal_id.as_str()
                        && execution.task_run_id == run_id
                })
                .min_by_key(|execution| execution.execution_id.as_str())
                .map(|execution| execution.execution_id.clone());
            let token = RunId::new().to_string();
            let execution_id =
                existing_execution_id.unwrap_or_else(|| format!("gap-execution-{token}"));
            let execution = Execution {
                execution_id: execution_id.clone(),
                pane_id: pane.pane_id,
                terminal_id: pane.terminal_id,
                task_run_id: run_id,
                state: agent.status.execution_state(),
            };
            self.model.insert_execution(execution.clone());
            persist.push(persist_execution(execution.clone(), now_ms));
            if !execution.state.is_terminal() {
                self.activate_for_live_execution(run_id, now_ms, &mut persist);
            }

            if let Some(provider) = provider {
                let existing_node = match native_sid.as_deref().filter(|sid| !sid.is_empty()) {
                    Some(sid) => self
                        .model
                        .agent_nodes()
                        .filter(|node| {
                            node.task_run_id == run_id
                                && node.provider == provider
                                && node.native_session_id.as_deref() == Some(sid)
                        })
                        .min_by_key(|node| node.agent_node_id.as_str())
                        .cloned(),
                    None => self
                        .model
                        .agent_nodes()
                        .filter(|node| node.task_run_id == run_id && node.provider == provider)
                        .min_by_key(|node| {
                            (
                                node.native_session_id.is_none(),
                                node.agent_node_id.as_str(),
                            )
                        })
                        .cloned(),
                };
                let agent_node = match existing_node {
                    Some(node) => node,
                    None => AgentNode {
                        agent_node_id: format!("gap-agent-{token}"),
                        provider,
                        native_session_id: native_sid.clone(),
                        task_run_id: run_id,
                        display_ordinal: self.allocate_ordinal()?,
                        parent_agent_node_id: None,
                        state: None,
                        model_id: None,
                        last_event_kind: None,
                        last_tool_name: None,
                        last_item_count: None,
                        last_byte_count: None,
                        last_activity_at_ms: None,
                        session_file: None,
                    },
                };
                self.model.insert_agent_node(agent_node.clone());
                persist.push(PersistOp::UpsertAgentNode(agent_node));
            }
        }

        for run_id in pre_gap_runs {
            self.close_run_without_live_execution(run_id, now_ms, &mut persist);
        }

        Ok(persist)
    }

    fn ensure_event_runs(
        &mut self,
        event: &NormalizedEvent,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        if let NormalizedEvent::ExecutionBegin { execution, .. } = event {
            self.ensure_execution_run(execution, metadata, persist)?;
        }

        if let Some(run_id) = metadata.task_run_id
            && metadata.source != "provider"
        {
            let controller_reference = metadata.task_state.is_some()
                || metadata.execution_parent.is_some()
                || metadata.dependency.is_some();
            let initial_state = match metadata.task_state {
                Some(state) => initial_controller_state(&metadata.source_event_type, state),
                None if controller_reference => TaskState::Queued,
                None => TaskState::Running,
            };
            self.ensure_metadata_run(
                run_id,
                metadata,
                controller_reference,
                None,
                initial_state,
                persist,
            )?;
        }

        if let Some(edge) = &metadata.execution_parent {
            self.ensure_controller_placeholder(
                edge.parent_run_id,
                None,
                Self::run_bookkeeping_time_ms(metadata),
                persist,
            )?;
            self.ensure_controller_placeholder(
                edge.child_run_id,
                None,
                Self::run_bookkeeping_time_ms(metadata),
                persist,
            )?;
        }
        if let Some(edge) = &metadata.dependency {
            self.ensure_controller_placeholder(
                edge.prerequisite_run_id,
                None,
                Self::run_bookkeeping_time_ms(metadata),
                persist,
            )?;
            self.ensure_controller_placeholder(
                edge.dependent_run_id,
                None,
                Self::run_bookkeeping_time_ms(metadata),
                persist,
            )?;
        }
        Ok(())
    }

    fn ensure_execution_run(
        &mut self,
        execution: &Execution,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        if self.model.task_run(&execution.task_run_id).is_some() {
            return Ok(());
        }
        let timestamp_ms = Self::run_bookkeeping_time_ms(metadata);
        let ordinal = self.allocate_ordinal()?;
        let native_key = metadata
            .provider
            .zip(metadata.native_session_id.as_deref())
            .filter(|(_, sid)| !sid.is_empty())
            .map(|(provider, sid)| RunKey::Native {
                provider,
                sid: sid.to_owned(),
            });
        let key = match native_key {
            Some(key) if self.model.task_run_by_key(&key).is_none() => key,
            _ => provisional_key(&execution.terminal_id, timestamp_ms, ordinal),
        };
        let mut task_run = TaskRun {
            run_id: execution.task_run_id,
            key,
            display_ordinal: ordinal,
            state: TaskState::Running,
            has_controller_task_state_event: false,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        Self::stamp_new_task_run(&mut task_run, timestamp_ms);
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
        Ok(())
    }

    fn ensure_metadata_run(
        &mut self,
        run_id: RunId,
        metadata: &EventMetadata,
        controller_reference: bool,
        controller_key: Option<&str>,
        initial_state: TaskState,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        if self.model.task_run(&run_id).is_some() {
            return Ok(());
        }
        let timestamp_ms = Self::run_bookkeeping_time_ms(metadata);
        let ordinal = self.allocate_ordinal()?;
        let native_key = metadata
            .provider
            .zip(metadata.native_session_id.as_deref())
            .filter(|(_, sid)| !sid.is_empty())
            .map(|(provider, sid)| RunKey::Native {
                provider,
                sid: sid.to_owned(),
            });
        let key = if controller_reference {
            RunKey::Controller(controller_key.map_or_else(|| run_id.to_string(), ToOwned::to_owned))
        } else {
            match native_key {
                Some(key) if self.model.task_run_by_key(&key).is_none() => key,
                _ => provisional_key(
                    metadata
                        .terminal_id
                        .as_deref()
                        .map_or("unknown-terminal", |terminal_id| terminal_id),
                    timestamp_ms,
                    ordinal,
                ),
            }
        };
        let mut task_run = TaskRun {
            run_id,
            key,
            display_ordinal: ordinal,
            state: initial_state,
            has_controller_task_state_event: metadata.task_state.is_some(),
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        Self::stamp_new_task_run(&mut task_run, timestamp_ms);
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
        Ok(())
    }

    fn ensure_controller_placeholder(
        &mut self,
        run_id: RunId,
        controller_key: Option<&str>,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        if self.model.task_run(&run_id).is_some() {
            return Ok(());
        }
        let ordinal = self.allocate_ordinal()?;
        let mut task_run = TaskRun {
            run_id,
            key: RunKey::Controller(
                controller_key.map_or_else(|| run_id.to_string(), ToOwned::to_owned),
            ),
            display_ordinal: ordinal,
            state: TaskState::Queued,
            has_controller_task_state_event: false,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        Self::stamp_new_task_run(&mut task_run, timestamp_ms);
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
        Ok(())
    }

    fn apply_controller_metadata(
        &mut self,
        metadata: &EventMetadata,
        allow_lane_reopen: bool,
        persist: &mut PersistBatch,
    ) {
        let timestamp_ms = Self::run_bookkeeping_time_ms(metadata);
        if let (Some(run_id), Some(target)) = (metadata.task_run_id, metadata.task_state)
            && let Some(mut task_run) = self.model.task_run(&run_id).cloned()
        {
            task_run.has_controller_task_state_event = true;
            if metadata.source != crate::provider::lane::SOURCE_LOG_LANE {
                self.non_lane_task_state_runs.insert(run_id);
            }
            task_run.state = controller_task_transition(
                task_run.state,
                &metadata.source_event_type,
                target,
                allow_lane_reopen,
            );
            let lane_claude_root_subject = metadata.source
                == crate::provider::lane::SOURCE_LOG_LANE
                && metadata.source_event_type == "progress"
                && metadata.provider == Some(Provider::Claude)
                && metadata.native_session_id.is_some();
            if (task_run.subject.is_none() || lane_claude_root_subject)
                && let Some(label) = metadata.label.as_ref().filter(|label| !label.is_empty())
            {
                task_run.subject = Some(label.clone());
            }
            Self::touch_task_run(&mut task_run, timestamp_ms);
            self.model.insert_task_run(task_run.clone());
            persist.push(self.persist_task_run(task_run, timestamp_ms));
        }

        if let Some(edge) = &metadata.execution_parent
            && self.model.insert_execution_edge(edge.clone())
        {
            persist.push(PersistOp::UpsertExecutionEdge {
                edge: edge.clone(),
                created_at_ms: metadata.receipt_time_ms,
            });
        }
        if let Some(edge) = &metadata.dependency
            && self.model.insert_dependency_edge(edge.clone())
        {
            persist.push(PersistOp::UpsertDependencyEdge {
                edge: edge.clone(),
                created_at_ms: metadata.receipt_time_ms,
            });
        }
    }

    fn update_terminal_event_source(&mut self, event: &NormalizedEvent) {
        let NormalizedEvent::ControllerEvent { metadata, event } = event else {
            return;
        };
        let Some(run_id) = metadata.task_run_id else {
            return;
        };
        match event {
            ControllerEventKind::Complete
            | ControllerEventKind::Failed
            | ControllerEventKind::Cancelled => {
                self.terminal_event_sources
                    .insert(run_id, metadata.source.clone());
            }
            ControllerEventKind::TaskStarted
                if self
                    .model
                    .task_run(&run_id)
                    .is_some_and(|run| !run.state.is_terminal()) =>
            {
                self.terminal_event_sources.remove(&run_id);
            }
            _ => {}
        }
    }

    fn apply_event_body(
        &mut self,
        event: &NormalizedEvent,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        match event {
            NormalizedEvent::ControllerEvent { event, .. } => {
                if matches!(
                    event,
                    ControllerEventKind::Dispatch { .. } | ControllerEventKind::TaskStarted
                ) && let Some(run_id) = metadata.task_run_id
                    && let Some(kind) = metadata
                        .provider_metadata
                        .as_ref()
                        .and_then(|provider| provider.event_kind.as_ref())
                {
                    self.model.set_run_kind(run_id, kind.clone());
                }
            }
            NormalizedEvent::TopologyUpsert {
                authority, entity, ..
            } => match entity {
                TopologyEntity::Workspace(workspace) => {
                    let display_ordinal =
                        self.workspace_ordinal_or_allocate(&workspace.workspace_id)?;
                    self.model.insert_workspace(workspace.clone());
                    persist.push(PersistOp::UpsertWorkspace {
                        workspace: workspace.clone(),
                        display_ordinal,
                    });
                }
                TopologyEntity::Tab(tab) => {
                    if self.model.workspace(&tab.workspace_id).is_none() {
                        return Ok(());
                    }
                    let mut tab = tab.clone();
                    tab.label = match authority {
                        TopologyAuthority::Partial => tab
                            .label
                            .as_deref()
                            .map(sanitize_controller_text)
                            .or_else(|| {
                                self.model
                                    .tab(&tab.tab_id)
                                    .and_then(|current| current.label.clone())
                            }),
                        TopologyAuthority::Authoritative => tab
                            .label
                            .as_deref()
                            .map(sanitize_controller_text)
                            .filter(|label| !label.is_empty()),
                    };
                    let display_ordinal = self.tab_ordinal_or_allocate(&tab.tab_id)?;
                    self.model.insert_tab(tab.clone());
                    persist.push(PersistOp::UpsertTab {
                        tab: tab.clone(),
                        display_ordinal,
                    });
                    if *authority == TopologyAuthority::Authoritative && tab.label.is_none() {
                        persist.push(PersistOp::ClearTabLabel {
                            tab_id: tab.tab_id.clone(),
                        });
                    }
                }
                TopologyEntity::Pane(pane) => {
                    if self.model.tab(&pane.tab_id).is_none() {
                        return Ok(());
                    }
                    let mut pane = pane.clone();
                    pane.display_name = match authority {
                        TopologyAuthority::Partial => pane
                            .display_name
                            .as_deref()
                            .map(sanitize_controller_text)
                            .or_else(|| {
                                self.model
                                    .pane(&pane.pane_id)
                                    .and_then(|current| current.display_name.clone())
                            }),
                        TopologyAuthority::Authoritative => pane
                            .display_name
                            .as_deref()
                            .map(sanitize_controller_text)
                            .filter(|display_name| !display_name.is_empty()),
                    };
                    let display_ordinal = self.pane_ordinal_or_allocate(&pane.pane_id)?;
                    self.model.insert_pane(pane.clone());
                    persist.push(PersistOp::UpsertPane {
                        pane: pane.clone(),
                        display_ordinal,
                    });
                    if *authority == TopologyAuthority::Authoritative && pane.display_name.is_none()
                    {
                        persist.push(PersistOp::ClearPaneDisplayName {
                            pane_id: pane.pane_id.clone(),
                        });
                    }
                }
            },
            NormalizedEvent::TopologyClosure { entity, .. } => {
                self.apply_topology_closure(entity, metadata.receipt_time_ms, persist);
            }
            NormalizedEvent::AgentStatusChanged {
                execution_id,
                state,
                ..
            } => {
                self.apply_execution_state(execution_id, state, metadata, persist);
            }
            NormalizedEvent::AgentNodeUpsert { node, .. } => {
                self.apply_agent_node_observation(node, persist)?;
            }
            NormalizedEvent::AgentActivity {
                agent_node_id,
                activity,
                ..
            } => self.apply_agent_activity(agent_node_id, activity, metadata, persist),
            NormalizedEvent::ExecutionBegin { execution, .. } => {
                self.model.insert_execution(execution.clone());
            }
            NormalizedEvent::ExecutionEnd { execution_id, .. } => {
                self.end_execution(execution_id, metadata.receipt_time_ms, persist);
            }
        }
        Ok(())
    }

    fn apply_agent_node_observation(
        &mut self,
        observed: &AgentNodeObservation,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        if self.model.task_run(&observed.task_run_id).is_none() {
            return Ok(());
        }
        let existing_id = observed
            .native_session_id
            .as_deref()
            .and_then(|sid| {
                self.agent_node_id_for_native(observed.provider, sid, observed.task_run_id)
            })
            .or_else(|| {
                self.model
                    .agent_node(&observed.agent_node_id)
                    .map(|node| node.agent_node_id.clone())
            });
        let mut node = match existing_id
            .as_deref()
            .and_then(|node_id| self.model.agent_node(node_id))
            .cloned()
        {
            Some(node) => node,
            None => AgentNode {
                agent_node_id: observed.native_session_id.as_deref().map_or_else(
                    || observed.agent_node_id.clone(),
                    |sid| deterministic_agent_node_id(observed.provider, sid),
                ),
                provider: observed.provider,
                native_session_id: observed.native_session_id.clone(),
                task_run_id: observed.task_run_id,
                display_ordinal: self.allocate_ordinal()?,
                parent_agent_node_id: None,
                state: None,
                model_id: None,
                last_event_kind: None,
                last_tool_name: None,
                last_item_count: None,
                last_byte_count: None,
                last_activity_at_ms: None,
                session_file: None,
            },
        };
        if let Some(native_session_id) = &observed.native_session_id {
            node.native_session_id = Some(native_session_id.clone());
        }
        match observed.state.as_ref() {
            Some(ExecState::Ended) => node.state = Some(ExecState::Ended),
            Some(ExecState::Working) if node.state != Some(ExecState::Ended) => {
                node.state = Some(ExecState::Working);
            }
            _ => {}
        }
        if let Some(model_id) = &observed.model_id {
            node.model_id = Some(model_id.clone());
        }
        if let Some(session_file) = &observed.session_file {
            node.session_file = Some(session_file.clone());
        }
        if let Some(parent_id) = &observed.parent_agent_node_id {
            let parent_id = self.resolve_parent_node_id(parent_id, node.provider, node.task_run_id);
            if self.provider_parent_is_valid(&node.agent_node_id, &parent_id) {
                node.parent_agent_node_id = Some(parent_id);
            } else {
                self.model
                    .controller_diagnostics_mut()
                    .record_provider_parent_conflict();
            }
        }
        self.model.insert_agent_node(node.clone());
        persist.push(PersistOp::UpsertAgentNode(node));
        Ok(())
    }

    fn apply_agent_activity(
        &mut self,
        requested_id: &str,
        activity: &MinimalProviderMetadata,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) {
        let resolved_id = self
            .model
            .agent_node(requested_id)
            .map(|node| node.agent_node_id.clone())
            .or_else(|| {
                metadata
                    .provider
                    .zip(activity.agent_id.as_deref())
                    .and_then(|(provider, sid)| {
                        self.model
                            .agent_nodes()
                            .filter(|node| {
                                node.provider == provider
                                    && node.native_session_id.as_deref() == Some(sid)
                            })
                            .map(|node| node.agent_node_id.clone())
                            .min()
                    })
            });
        let Some(mut node) = resolved_id
            .as_deref()
            .and_then(|node_id| self.model.agent_node(node_id))
            .cloned()
        else {
            return;
        };
        if node
            .last_activity_at_ms
            .is_some_and(|observed| observed > metadata.timestamp_ms)
        {
            return;
        }
        node.last_activity_at_ms = Some(metadata.timestamp_ms);
        if let Some(model_id) = &activity.model_id {
            node.model_id = Some(model_id.clone());
        }
        if let Some(event_kind) = &activity.event_kind {
            node.last_event_kind = Some(event_kind.clone());
        }
        if let Some(tool_name) = &activity.tool_name {
            node.last_tool_name = Some(tool_name.clone());
        }
        if let Some(item_count) = activity.item_count {
            node.last_item_count = Some(item_count);
        }
        if let Some(byte_count) = activity.byte_count {
            node.last_byte_count = Some(byte_count);
        }
        self.model.insert_agent_node(node.clone());
        persist.push(PersistOp::UpsertAgentNode(node));
    }

    fn agent_node_id_for_native(
        &self,
        provider: Provider,
        sid: &str,
        run_id: RunId,
    ) -> Option<String> {
        self.model
            .agent_nodes()
            .filter(|node| {
                node.task_run_id == run_id
                    && node.provider == provider
                    && node.native_session_id.as_deref() == Some(sid)
            })
            .map(|node| node.agent_node_id.clone())
            .min()
    }

    fn resolve_parent_node_id(
        &self,
        requested_id: &str,
        provider: Provider,
        run_id: RunId,
    ) -> String {
        self.model
            .agent_nodes()
            .find(|node| {
                node.task_run_id == run_id
                    && node.provider == provider
                    && node.native_session_id.as_deref().is_some_and(|sid| {
                        deterministic_agent_node_id(provider, sid) == requested_id
                    })
            })
            .map_or_else(
                || requested_id.to_owned(),
                |node| node.agent_node_id.clone(),
            )
    }

    fn provider_parent_is_valid(&self, child_id: &str, parent_id: &str) -> bool {
        if child_id == parent_id {
            return false;
        }
        let mut current = Some(parent_id);
        let mut visited = std::collections::HashSet::new();
        while let Some(node_id) = current {
            if node_id == child_id || !visited.insert(node_id.to_owned()) {
                return false;
            }
            current = self
                .model
                .agent_node(node_id)
                .and_then(|node| node.parent_agent_node_id.as_deref());
        }
        true
    }

    fn apply_topology_closure(
        &mut self,
        entity: &TopologyEntityId,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) {
        match entity {
            TopologyEntityId::Workspace { workspace_id } => {
                let tab_ids: Vec<_> = self
                    .model
                    .tabs()
                    .filter(|tab| tab.workspace_id == *workspace_id)
                    .map(|tab| tab.tab_id.clone())
                    .collect();
                for tab_id in tab_ids {
                    self.close_tab(&tab_id, timestamp_ms, persist);
                }
                if self.model.remove_workspace(workspace_id).is_some() {
                    self.model.remove_workspace_ordinal(workspace_id);
                    persist.push(PersistOp::DeleteWorkspace {
                        workspace_id: workspace_id.clone(),
                    });
                }
            }
            TopologyEntityId::Tab { tab_id } => {
                self.close_tab(tab_id, timestamp_ms, persist);
            }
            TopologyEntityId::Pane { pane_id } => {
                self.close_pane(pane_id, timestamp_ms, persist);
            }
        }
    }

    fn close_tab(&mut self, tab_id: &str, timestamp_ms: i64, persist: &mut PersistBatch) {
        let pane_ids: Vec<_> = self
            .model
            .panes()
            .filter(|pane| pane.tab_id == tab_id)
            .map(|pane| pane.pane_id.clone())
            .collect();
        for pane_id in pane_ids {
            self.close_pane(&pane_id, timestamp_ms, persist);
        }
        if self.model.remove_tab(tab_id).is_some() {
            self.model.remove_tab_ordinal(tab_id);
            persist.push(PersistOp::DeleteTab {
                tab_id: tab_id.to_owned(),
            });
        }
    }

    fn close_pane(&mut self, pane_id: &str, timestamp_ms: i64, persist: &mut PersistBatch) {
        let execution_ids: Vec<_> = self
            .model
            .executions()
            .filter(|execution| execution.pane_id == pane_id && !execution.state.is_terminal())
            .map(|execution| execution.execution_id.clone())
            .collect();
        for execution_id in execution_ids {
            self.end_execution(&execution_id, timestamp_ms, persist);
        }
        if self.model.remove_pane(pane_id).is_some() {
            self.model.remove_pane_ordinal(pane_id);
            persist.push(PersistOp::DeletePane {
                pane_id: pane_id.to_owned(),
            });
        }
    }

    fn apply_execution_state(
        &mut self,
        execution_id: &str,
        observed: &ExecState,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) {
        let Some(mut execution) = self.model.execution(execution_id).cloned() else {
            return;
        };
        execution.state = next_execution_state(&execution.state, observed, metadata);
        let run_id = execution.task_run_id;
        let ended = execution.state.is_terminal();
        self.model.insert_execution(execution);
        if ended {
            self.close_run_without_live_execution(run_id, metadata.receipt_time_ms, persist);
        }
    }

    fn end_execution(&mut self, execution_id: &str, timestamp_ms: i64, persist: &mut PersistBatch) {
        let Some(mut execution) = self.model.execution(execution_id).cloned() else {
            return;
        };
        execution.state = ExecState::Ended;
        let run_id = execution.task_run_id;
        self.model.insert_execution(execution.clone());
        persist.push(persist_execution(execution, timestamp_ms));
        self.close_run_without_live_execution(run_id, timestamp_ms, persist);
    }

    fn apply_identity_metadata(
        &mut self,
        event: &NormalizedEvent,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        let timestamp_ms = Self::run_bookkeeping_time_ms(metadata);
        let event_run = match event {
            NormalizedEvent::ControllerEvent { .. } => None,
            NormalizedEvent::ExecutionBegin { execution, .. } => Some(execution.task_run_id),
            NormalizedEvent::AgentNodeUpsert { node, .. } => Some(node.task_run_id),
            NormalizedEvent::AgentActivity { .. } => None,
            NormalizedEvent::AgentStatusChanged { execution_id, .. }
            | NormalizedEvent::ExecutionEnd { execution_id, .. } => self
                .model
                .execution(execution_id)
                .map(|execution| execution.task_run_id),
            NormalizedEvent::TopologyUpsert { .. } | NormalizedEvent::TopologyClosure { .. } => {
                None
            }
        };
        let run_id = metadata.task_run_id.or(event_run);
        let Some(run_id) = run_id else {
            return Ok(());
        };
        let Some(task_run) = self.model.task_run(&run_id) else {
            return Ok(());
        };

        if let Some((provider, sid)) = metadata
            .provider
            .zip(metadata.native_session_id.as_deref())
            .filter(|(_, sid)| !sid.is_empty())
        {
            let evidence = if metadata.source == "provider"
                && metadata.source_event_type == "session_resolved"
                && matches!(task_run.key, RunKey::NativePath { .. })
            {
                BindingEvidence::NativePathResolved {
                    run: run_id,
                    provider,
                    sid: sid.to_owned(),
                }
            } else if matches!(task_run.key, RunKey::Controller(_)) {
                BindingEvidence::ControllerNativeSession {
                    controller_run: run_id,
                    provider,
                    sid: sid.to_owned(),
                }
            } else {
                BindingEvidence::NativeSession {
                    run: run_id,
                    provider,
                    sid: sid.to_owned(),
                }
            };
            self.apply_binding(evidence, timestamp_ms, persist)?;
        }

        if matches!(
            self.model.task_run(&run_id).map(|run| &run.key),
            Some(RunKey::Controller(_))
        ) && let Some(terminal_id) = metadata.terminal_id.as_deref()
            && !terminal_id.is_empty()
        {
            self.apply_binding(
                BindingEvidence::ControllerTerminal {
                    controller_run: run_id,
                    terminal_id: terminal_id.to_owned(),
                },
                timestamp_ms,
                persist,
            )?;
        }

        if let NormalizedEvent::ExecutionBegin { execution, .. } = event
            && let Some(current) = self.model.execution(&execution.execution_id)
            && !current.state.is_terminal()
        {
            self.activate_for_live_execution(current.task_run_id, timestamp_ms, persist);
        }
        Ok(())
    }

    fn apply_binding(
        &mut self,
        evidence: BindingEvidence,
        bookkeeping_time_ms: i64,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        let plan = plan_binding(&self.model, &evidence);
        persist.extend(apply_binding_plan_at(
            &mut self.model,
            plan,
            bookkeeping_time_ms,
        )?);
        Ok(())
    }

    fn persist_event_execution(
        &self,
        event: &NormalizedEvent,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) {
        let execution_id = match event {
            NormalizedEvent::ControllerEvent { .. } => None,
            NormalizedEvent::AgentStatusChanged { execution_id, .. } => Some(execution_id.as_str()),
            NormalizedEvent::ExecutionEnd { .. } => None,
            NormalizedEvent::AgentNodeUpsert { .. } | NormalizedEvent::AgentActivity { .. } => None,
            NormalizedEvent::ExecutionBegin { execution, .. } => {
                Some(execution.execution_id.as_str())
            }
            NormalizedEvent::TopologyUpsert { .. } | NormalizedEvent::TopologyClosure { .. } => {
                None
            }
        };
        if let Some(execution_id) = execution_id
            && let Some(execution) = self.model.execution(execution_id)
        {
            persist.push(persist_execution(execution.clone(), timestamp_ms));
        }
    }

    fn close_run_without_live_execution(
        &mut self,
        run_id: RunId,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) {
        if self
            .model
            .executions()
            .any(|execution| execution.task_run_id == run_id && !execution.state.is_terminal())
        {
            return;
        }
        let Some(mut task_run) = self.model.task_run(&run_id).cloned() else {
            return;
        };
        if task_run.has_controller_task_state_event
            || matches!(
                task_run.state,
                TaskState::Completed
                    | TaskState::Failed
                    | TaskState::Cancelled
                    | TaskState::EndedUnknown
            )
        {
            return;
        }
        task_run.state = TaskState::EndedUnknown;
        Self::touch_task_run(&mut task_run, timestamp_ms);
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
    }

    fn activate_for_live_execution(
        &mut self,
        run_id: RunId,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) {
        let Some(mut task_run) = self.model.task_run(&run_id).cloned() else {
            return;
        };
        let should_activate = task_run.state == TaskState::EndedUnknown
            || (task_run.state == TaskState::Queued && !task_run.has_controller_task_state_event);
        if !should_activate {
            return;
        }
        task_run.state = TaskState::Running;
        Self::touch_task_run(&mut task_run, timestamp_ms);
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
    }

    fn replace_topology(
        &mut self,
        topology: &crate::model::TopologySnapshot,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        let retained_workspace_ordinals = topology
            .workspaces
            .iter()
            .filter_map(|workspace| {
                self.model
                    .workspace_ordinal(&workspace.workspace_id)
                    .map(|ordinal| (workspace.workspace_id.clone(), ordinal))
            })
            .collect::<Vec<_>>();
        let retained_tab_ordinals = topology
            .tabs
            .iter()
            .filter_map(|tab| {
                self.model
                    .tab_ordinal(&tab.tab_id)
                    .map(|ordinal| (tab.tab_id.clone(), ordinal))
            })
            .collect::<Vec<_>>();
        let retained_pane_ordinals = topology
            .panes
            .iter()
            .filter_map(|pane| {
                self.model
                    .pane_ordinal(&pane.pane_id)
                    .map(|ordinal| (pane.pane_id.clone(), ordinal))
            })
            .collect::<Vec<_>>();
        let workspace_ids: Vec<_> = self
            .model
            .workspaces()
            .map(|workspace| workspace.workspace_id.clone())
            .collect();
        let tab_ids: Vec<_> = self.model.tabs().map(|tab| tab.tab_id.clone()).collect();
        let pane_ids: Vec<_> = self
            .model
            .panes()
            .map(|pane| pane.pane_id.clone())
            .collect();
        for pane_id in pane_ids {
            self.close_pane(&pane_id, unix_now_ms(), persist);
        }
        for tab_id in tab_ids {
            if self.model.remove_tab(&tab_id).is_some() {
                self.model.remove_tab_ordinal(&tab_id);
                persist.push(PersistOp::DeleteTab { tab_id });
            }
        }
        for workspace_id in workspace_ids {
            if self.model.remove_workspace(&workspace_id).is_some() {
                self.model.remove_workspace_ordinal(&workspace_id);
                persist.push(PersistOp::DeleteWorkspace { workspace_id });
            }
        }

        for (workspace_id, ordinal) in retained_workspace_ordinals {
            self.model.set_workspace_ordinal(workspace_id, ordinal);
        }
        for (tab_id, ordinal) in retained_tab_ordinals {
            self.model.set_tab_ordinal(tab_id, ordinal);
        }
        for (pane_id, ordinal) in retained_pane_ordinals {
            self.model.set_pane_ordinal(pane_id, ordinal);
        }

        for workspace in &topology.workspaces {
            let display_ordinal = self.workspace_ordinal_or_allocate(&workspace.workspace_id)?;
            self.model.insert_workspace(workspace.clone());
            persist.push(PersistOp::UpsertWorkspace {
                workspace: workspace.clone(),
                display_ordinal,
            });
        }
        for tab in &topology.tabs {
            let mut tab = tab.clone();
            tab.label = tab
                .label
                .as_deref()
                .map(sanitize_controller_text)
                .filter(|label| !label.is_empty());
            let display_ordinal = self.tab_ordinal_or_allocate(&tab.tab_id)?;
            self.model.insert_tab(tab.clone());
            persist.push(PersistOp::UpsertTab {
                tab: tab.clone(),
                display_ordinal,
            });
            if tab.label.is_none() {
                persist.push(PersistOp::ClearTabLabel {
                    tab_id: tab.tab_id.clone(),
                });
            }
        }
        for pane in &topology.panes {
            let pane = Pane {
                pane_id: pane.pane_id.clone(),
                workspace_id: pane.workspace_id.clone(),
                tab_id: pane.tab_id.clone(),
                terminal_id: pane.terminal_id.clone(),
                display_name: pane
                    .display_name
                    .as_deref()
                    .map(sanitize_controller_text)
                    .filter(|display_name| !display_name.is_empty()),
            };
            let display_ordinal = self.pane_ordinal_or_allocate(&pane.pane_id)?;
            self.model.insert_pane(pane.clone());
            persist.push(PersistOp::UpsertPane {
                pane: pane.clone(),
                display_ordinal,
            });
            if pane.display_name.is_none() {
                persist.push(PersistOp::ClearPaneDisplayName {
                    pane_id: pane.pane_id.clone(),
                });
            }
        }
        Ok(())
    }

    fn insert_snapshot_run(
        &mut self,
        provider: Option<Provider>,
        native_sid: Option<&str>,
        native_path: Option<&str>,
        terminal_id: &str,
        timestamp_ms: i64,
        persist: &mut PersistBatch,
    ) -> Result<RunId, ReducerError> {
        let run_id = RunId::new();
        let ordinal = self.allocate_ordinal()?;
        let key = match (provider, native_sid, native_path) {
            (Some(provider), Some(sid), _) => RunKey::Native {
                provider,
                sid: sid.to_owned(),
            },
            (Some(provider), None, Some(path)) => RunKey::NativePath {
                provider,
                path: path.to_owned(),
            },
            _ => provisional_key(terminal_id, timestamp_ms, ordinal),
        };
        let mut task_run = TaskRun {
            run_id,
            key,
            display_ordinal: ordinal,
            state: TaskState::Running,
            has_controller_task_state_event: false,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        Self::stamp_new_task_run(&mut task_run, timestamp_ms);
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
        Ok(run_id)
    }

    /// Resolves an alias/native key first, then unanimous agent-node evidence from
    /// non-provisional owners; ambiguous claims remain unresolved.
    fn run_for_native_session(&self, provider: Provider, sid: &str) -> Option<RunId> {
        let key = RunKey::Native {
            provider,
            sid: sid.to_owned(),
        };
        if let Some(task_run) = self.model.task_run_by_key(&key) {
            return Some(task_run.run_id);
        }

        let mut runs = self
            .model
            .agent_nodes()
            .filter(|node| {
                node.provider == provider
                    && node.native_session_id.as_deref() == Some(sid)
                    && self.model.task_run(&node.task_run_id).is_some_and(|run| {
                        matches!(
                            run.key,
                            RunKey::Controller(_)
                                | RunKey::Native { .. }
                                | RunKey::NativePath { .. }
                        )
                    })
            })
            .map(|node| node.task_run_id);
        let first = runs.next()?;
        runs.all(|run_id| run_id == first).then_some(first)
    }

    fn persist_task_run(&self, task_run: TaskRun, timestamp_ms: i64) -> PersistOp {
        let native_session = native_binding(&self.model, task_run.run_id);
        let created_at_ms = task_run.created_at_ms.unwrap_or(timestamp_ms);
        let updated_at_ms = task_run.updated_at_ms.unwrap_or(timestamp_ms);
        let finished_at_ms = task_run.finished_at_ms;
        PersistOp::UpsertTaskRun(PersistTaskRun {
            task_run,
            native_session,
            created_at_ms,
            updated_at_ms,
            finished_at_ms,
        })
    }

    fn stamp_new_task_run(task_run: &mut TaskRun, timestamp_ms: i64) {
        task_run.created_at_ms.get_or_insert(timestamp_ms);
        Self::touch_task_run(task_run, timestamp_ms);
    }

    fn touch_task_run(task_run: &mut TaskRun, timestamp_ms: i64) {
        task_run.touch(
            task_run
                .updated_at_ms
                .unwrap_or(timestamp_ms)
                .max(timestamp_ms),
        );
    }

    fn run_bookkeeping_time_ms(metadata: &EventMetadata) -> i64 {
        if metadata.source == crate::provider::lane::SOURCE_LOG_LANE {
            metadata.timestamp_ms
        } else {
            metadata.receipt_time_ms
        }
    }

    fn allocate_ordinal(&mut self) -> Result<DisplayOrdinal, ReducerError> {
        let ordinal = DisplayOrdinal::new(self.next_ordinal);
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or(ReducerError::OrdinalExhausted)?;
        Ok(ordinal)
    }

    fn workspace_ordinal_or_allocate(
        &mut self,
        workspace_id: &str,
    ) -> Result<DisplayOrdinal, ReducerError> {
        if let Some(ordinal) = self.model.workspace_ordinal(workspace_id) {
            return Ok(ordinal);
        }
        let ordinal = self.allocate_ordinal()?;
        self.model
            .set_workspace_ordinal(workspace_id.to_owned(), ordinal);
        Ok(ordinal)
    }

    fn tab_ordinal_or_allocate(&mut self, tab_id: &str) -> Result<DisplayOrdinal, ReducerError> {
        if let Some(ordinal) = self.model.tab_ordinal(tab_id) {
            return Ok(ordinal);
        }
        let ordinal = self.allocate_ordinal()?;
        self.model.set_tab_ordinal(tab_id.to_owned(), ordinal);
        Ok(ordinal)
    }

    fn pane_ordinal_or_allocate(&mut self, pane_id: &str) -> Result<DisplayOrdinal, ReducerError> {
        if let Some(ordinal) = self.model.pane_ordinal(pane_id) {
            return Ok(ordinal);
        }
        let ordinal = self.allocate_ordinal()?;
        self.model.set_pane_ordinal(pane_id.to_owned(), ordinal);
        Ok(ordinal)
    }

    /// Ends executions whose live-observation stale grace has expired.
    pub fn sweep_stale(&mut self, now_ms: i64) -> PersistBatch {
        let execution_ids: Vec<_> = self
            .model
            .executions()
            .filter_map(|execution| match execution.state {
                ExecState::Stale { since_ms }
                    if now_ms.saturating_sub(since_ms) >= STALE_GRACE_MS =>
                {
                    Some(execution.execution_id.clone())
                }
                _ => None,
            })
            .collect();
        let mut persist = Vec::new();
        for execution_id in execution_ids {
            self.end_execution(&execution_id, now_ms, &mut persist);
        }
        // Dispatch-only closure must run even when no execution reaches its stale deadline.
        persist.extend(self.close_inactive_dispatch_only_runs(now_ms));
        persist.extend(self.close_inactive_fact_timed_runs(now_ms));
        if persist.is_empty() {
            return persist;
        }
        self.recompute_dangling_announcement_components();
        self.apply_operator_submission(&persist);
        self.publish();
        persist
    }

    fn close_inactive_dispatch_only_runs(&mut self, now_ms: i64) -> PersistBatch {
        let runs_with_executions = activity::runs_with_executions(&self.model);
        let mut run_ids: Vec<_> = self
            .model
            .task_runs()
            .filter(|run| matches!(run.key, RunKey::Controller(_)))
            .filter(|run| !runs_with_executions.contains(&run.run_id))
            .filter(|run| run.state == TaskState::Queued)
            .filter(|run| run.dismissed_at_ms.is_none())
            .filter(|run| {
                // Missing restored timestamps are not evidence that the run is old enough.
                let Some(anchor_ms) = run.updated_at_ms.or(run.created_at_ms) else {
                    return false;
                };
                now_ms.saturating_sub(anchor_ms) >= activity::headless_inactivity_ms()
            })
            .map(|run| run.run_id)
            .collect();
        run_ids.sort_unstable();

        let mut persist = Vec::with_capacity(run_ids.len());
        for run_id in run_ids {
            let mut task_run = self
                .model
                .task_run(&run_id)
                .cloned()
                .expect("collected task run must remain present");
            task_run.state = TaskState::EndedUnknown;
            Self::touch_task_run(&mut task_run, now_ms);
            self.model.insert_task_run(task_run.clone());
            persist.push(self.persist_task_run(task_run, now_ms));
        }
        persist
    }

    fn close_inactive_fact_timed_runs(&mut self, now_ms: i64) -> PersistBatch {
        let runs_with_live_executions: HashSet<_> = self
            .model
            .executions()
            .filter(|execution| !execution.state.is_terminal())
            .map(|execution| execution.task_run_id)
            .collect();
        let mut candidates: Vec<_> = self
            .model
            .task_runs()
            .filter(|run| {
                matches!(
                    (&run.key, run.state),
                    (RunKey::Controller(_), TaskState::Running)
                        | (RunKey::Provisional { .. }, TaskState::Running)
                )
            })
            .filter(|run| run.dismissed_at_ms.is_none())
            .filter(|run| !self.non_lane_task_state_runs.contains(&run.run_id))
            .filter(|run| !runs_with_live_executions.contains(&run.run_id))
            .filter_map(|run| {
                let anchor_ms = run.updated_at_ms.or(run.created_at_ms)?;
                (now_ms.saturating_sub(anchor_ms) >= activity::headless_inactivity_ms())
                    .then_some((run.run_id, anchor_ms))
            })
            .collect();
        candidates.sort_unstable_by_key(|(run_id, _)| *run_id);

        let mut persist = Vec::with_capacity(candidates.len());
        for (run_id, anchor_ms) in candidates {
            let mut task_run = self
                .model
                .task_run(&run_id)
                .cloned()
                .expect("collected task run must remain present");
            task_run.state = TaskState::EndedUnknown;
            Self::touch_task_run(&mut task_run, anchor_ms);
            self.model.insert_task_run(task_run.clone());
            persist.push(self.persist_task_run(task_run, anchor_ms));
        }
        persist
    }

    /// Applies one operator command at collector receipt time.
    pub fn apply_operator_command(
        &mut self,
        command: OperatorCommand,
        now_ms: i64,
    ) -> PersistBatch {
        let runs_with_executions = activity::runs_with_executions(&self.model);
        let run_ids: Vec<_> = self
            .model
            .task_runs()
            .filter(|run| run.dismissed_at_ms.is_none())
            .filter(|run| match command {
                OperatorCommand::DismissClearable => {
                    run.state.is_terminal()
                        || activity::is_hook_only_stale_task_run(run, &runs_with_executions, now_ms)
                }
            })
            .map(|run| run.run_id)
            .collect();
        if run_ids.is_empty() {
            return Vec::new();
        }
        let mut persist = Vec::with_capacity(run_ids.len());
        for run_id in run_ids {
            let mut task_run = self
                .model
                .task_run(&run_id)
                .cloned()
                .expect("collected task run must remain present");
            task_run.dismissed_at_ms = Some(now_ms);
            // Deliberately leave updated_at_ms unchanged: it clocks hook-only staleness, not operator activity.
            self.model.insert_task_run(task_run.clone());
            persist.push(self.persist_task_run(task_run, now_ms));
        }
        self.apply_operator_submission(&persist);
        self.publish();
        persist
    }

    fn recompute_dangling_announcement_components(&mut self) {
        // increment5-workload-harness: begin reducer D4 timing start
        #[cfg(feature = "workload-harness")]
        let workload_d4_started = Instant::now();
        // increment5-workload-harness: end reducer D4 timing start
        let count = crate::model::graph::dangling_announcement_components(&self.model);
        // increment5-workload-harness: begin reducer D4 timing finish
        #[cfg(feature = "workload-harness")]
        record_workload_timing_segment(WorkloadTimingSegment::D4, workload_d4_started.elapsed());
        // increment5-workload-harness: end reducer D4 timing finish
        self.model
            .controller_diagnostics_mut()
            .set_dangling_announcement_components(count);
    }

    #[cfg(feature = "workload-harness")]
    fn publish_with_workload_timing(
        &mut self,
        workload_timing: Option<SuspendedWorkloadTimingState>,
    ) {
        let workload_timing = workload_timing.map(SuspendedWorkloadTimingState::resume);
        self.publish();
        if let Some(workload_timing) = workload_timing {
            workload_timing.finish();
        }
    }

    fn publish(&mut self) {
        self.apply_pending_telemetry_for_known_runs();
        let (origin, observed_at_ms) = self.rate_observation_context.unwrap_or((
            RateObservationOrigin::Live {
                epoch: self.rate_epoch,
            },
            unix_now_ms(),
        ));
        let mut run_ids = self
            .pending_rate_observation_runs
            .drain()
            .collect::<Vec<_>>();
        run_ids.sort_unstable();
        if matches!(origin, RateObservationOrigin::Historical) || self.rate_epoch_active {
            let statuses = crate::status::StatusReadModel::from_model(&self.model, observed_at_ms);
            for run_id in run_ids {
                let Some(run) = self.model.task_run(&run_id).cloned() else {
                    self.model.remove_run_rate_cursor(&run_id);
                    continue;
                };
                let working = matches!(
                    statuses.run_rate_activity(&self.model, &run),
                    crate::status::RunRateActivity::Working
                );
                self.observe_run_rates_with_activity(
                    RateObservation {
                        run_id,
                        origin,
                        observed_at_ms,
                    },
                    working,
                );
            }
        }
        self.publish_snapshot();
    }

    fn publish_snapshot(&mut self) {
        if self.defer_provider_publication {
            return;
        }
        if self.defer_provider_model_publication {
            self.provider_model_publication_pending = true;
            return;
        }
        if self
            .pending_live_provider_submission
            .as_ref()
            .is_some_and(|pending| pending.checkpoint.is_some())
        {
            return;
        }
        #[cfg(test)]
        {
            self.publish_count.set(self.publish_count.get() + 1);
            self.shared_publish_count.fetch_add(1, Ordering::Relaxed);
        }
        // increment5-workload-harness: begin reducer clone publication timing start
        #[cfg(feature = "workload-harness")]
        let workload_publish_started = Instant::now();
        // increment5-workload-harness: end reducer clone publication timing start
        self.publisher
            .send_replace(Arc::new(self.model.publication_snapshot()));
        // increment5-workload-harness: begin reducer clone publication timing finish
        #[cfg(feature = "workload-harness")]
        record_workload_timing_segment(
            WorkloadTimingSegment::ModelClonePublish,
            workload_publish_started.elapsed(),
        );
        // increment5-workload-harness: end reducer clone publication timing finish
    }
}

fn validate_controller_transition(
    model: &DomainModel,
    event: &ControllerEventKind,
    subject: RunId,
    subject_was_unknown: bool,
    execution_edge: Option<&ExecutionEdge>,
    dependency_edge: Option<&DependencyEdge>,
    native_lifecycle_terminal: bool,
) -> Result<ControllerDiagnosticDeltas, RejectReason> {
    let mut deltas = ControllerDiagnosticDeltas::default();
    let state = model
        .task_run(&subject)
        .map(|run| run.state)
        .ok_or(RejectReason::Invalid)?;
    match event {
        ControllerEventKind::Dispatch { .. } => {
            let edge = execution_edge.ok_or(RejectReason::Invalid)?;
            preflight_execution_edge(model, edge)
                .map_err(|conflict| reject_merge_conflict(&conflict))?;
        }
        ControllerEventKind::DependsOn { .. } => {
            let edge = dependency_edge.ok_or(RejectReason::Invalid)?;
            let restatement = model.dependency_edges().any(|existing| existing == edge);
            if state.is_terminal() && !restatement {
                return Err(RejectReason::StaleEvent);
            }
            preflight_dependency_edge(model, edge)
                .map_err(|conflict| reject_merge_conflict(&conflict))?;
        }
        ControllerEventKind::TaskStarted => {
            if matches!(
                state,
                TaskState::Completed | TaskState::Failed | TaskState::Cancelled
            ) && native_binding(model, subject).is_none()
            {
                return Err(RejectReason::StaleEvent);
            }
        }
        ControllerEventKind::Blocked | ControllerEventKind::Progress => {
            if matches!(
                state,
                TaskState::Completed | TaskState::Failed | TaskState::Cancelled
            ) {
                deltas.terminal_blocked_progress_noops = 1;
            }
        }
        ControllerEventKind::Dismiss => {}
        ControllerEventKind::SessionEnded => {}
        ControllerEventKind::Complete
        | ControllerEventKind::Failed
        | ControllerEventKind::Cancelled => {
            if native_lifecycle_terminal {
                return Ok(deltas);
            }
            let target = controller_target_state(event).ok_or(RejectReason::Invalid)?;
            if matches!(
                state,
                TaskState::Completed | TaskState::Failed | TaskState::Cancelled
            ) && state != target
            {
                return Err(RejectReason::Conflict);
            }
            if subject_was_unknown {
                deltas.terminal_forward_reference_creations = 1;
            }
        }
    }
    Ok(deltas)
}

fn reject_merge_conflict(conflict: &MergeConflict) -> RejectReason {
    match conflict {
        MergeConflict::DispatchSelfEdge { .. }
        | MergeConflict::DependencySelfEdge { .. }
        | MergeConflict::DispatchCycle
        | MergeConflict::DependencyCycle => RejectReason::Cycle,
        _ => RejectReason::Conflict,
    }
}

fn controller_kind_name(event: &ControllerEventKind) -> &'static str {
    match event {
        ControllerEventKind::Dispatch { .. } => "dispatch",
        ControllerEventKind::TaskStarted => "task_started",
        ControllerEventKind::DependsOn { .. } => "depends_on",
        ControllerEventKind::Blocked => "blocked",
        ControllerEventKind::Progress => "progress",
        ControllerEventKind::Complete => "complete",
        ControllerEventKind::Failed => "failed",
        ControllerEventKind::Cancelled => "cancelled",
        ControllerEventKind::SessionEnded => "session_ended",
        ControllerEventKind::Dismiss => "dismiss",
    }
}

fn controller_target_state(event: &ControllerEventKind) -> Option<TaskState> {
    match event {
        ControllerEventKind::Dispatch { .. }
        | ControllerEventKind::DependsOn { .. }
        | ControllerEventKind::SessionEnded
        | ControllerEventKind::Dismiss => None,
        ControllerEventKind::TaskStarted => Some(TaskState::Running),
        ControllerEventKind::Blocked => Some(TaskState::Blocked),
        ControllerEventKind::Progress => Some(TaskState::Queued),
        ControllerEventKind::Complete => Some(TaskState::Completed),
        ControllerEventKind::Failed => Some(TaskState::Failed),
        ControllerEventKind::Cancelled => Some(TaskState::Cancelled),
    }
}

fn event_metadata(event: &NormalizedEvent) -> &EventMetadata {
    match event {
        NormalizedEvent::ControllerEvent { metadata, .. }
        | NormalizedEvent::TopologyUpsert { metadata, .. }
        | NormalizedEvent::TopologyClosure { metadata, .. }
        | NormalizedEvent::AgentStatusChanged { metadata, .. }
        | NormalizedEvent::AgentNodeUpsert { metadata, .. }
        | NormalizedEvent::AgentActivity { metadata, .. }
        | NormalizedEvent::ExecutionBegin { metadata, .. }
        | NormalizedEvent::ExecutionEnd { metadata, .. } => metadata,
    }
}

fn event_metadata_mut(event: &mut NormalizedEvent) -> &mut EventMetadata {
    match event {
        NormalizedEvent::ControllerEvent { metadata, .. }
        | NormalizedEvent::TopologyUpsert { metadata, .. }
        | NormalizedEvent::TopologyClosure { metadata, .. }
        | NormalizedEvent::AgentStatusChanged { metadata, .. }
        | NormalizedEvent::AgentNodeUpsert { metadata, .. }
        | NormalizedEvent::AgentActivity { metadata, .. }
        | NormalizedEvent::ExecutionBegin { metadata, .. }
        | NormalizedEvent::ExecutionEnd { metadata, .. } => metadata,
    }
}

fn normalize_persist_batch_lineage(batch: &mut PersistBatch) {
    let mut merged_into = HashMap::new();
    for operation in batch {
        match operation {
            PersistOp::MergeTaskRuns { survivor, absorbed } => {
                merged_into.insert(*absorbed, *survivor);
            }
            PersistOp::RecordEvent { event, .. } => {
                let metadata = event_metadata_mut(event);
                let Some(mut run_id) = metadata.task_run_id else {
                    continue;
                };
                for _ in 0..merged_into.len() {
                    let Some(survivor) = merged_into.get(&run_id) else {
                        break;
                    };
                    run_id = *survivor;
                }
                metadata.task_run_id = Some(run_id);
            }
            _ => {}
        }
    }
}

fn canonical_run_after_batch(mut run_id: RunId, batch: &[PersistOp]) -> RunId {
    for operation in batch {
        if let PersistOp::MergeTaskRuns { survivor, absorbed } = operation
            && *absorbed == run_id
        {
            run_id = *survivor;
        }
    }
    run_id
}

fn deterministic_agent_node_id(provider: Provider, sid: &str) -> String {
    let provider = match provider {
        Provider::Claude => "claude",
        Provider::Codex => "codex",
    };
    format!("agent:{provider}:{sid}")
}

fn initial_controller_state(source_event_type: &str, supplied: TaskState) -> TaskState {
    match controller_event_kind(source_event_type) {
        LegacyControllerEventKind::Started => TaskState::Running,
        LegacyControllerEventKind::Blocked => TaskState::Blocked,
        LegacyControllerEventKind::Progress => TaskState::Queued,
        LegacyControllerEventKind::Complete => TaskState::Completed,
        LegacyControllerEventKind::Failed => TaskState::Failed,
        LegacyControllerEventKind::Cancelled => TaskState::Cancelled,
        LegacyControllerEventKind::Other => supplied,
    }
}

fn controller_task_transition(
    current: TaskState,
    source_event_type: &str,
    supplied: TaskState,
    allow_lane_reopen: bool,
) -> TaskState {
    match controller_event_kind(source_event_type) {
        LegacyControllerEventKind::Started => match current {
            TaskState::Queued | TaskState::Blocked | TaskState::EndedUnknown => TaskState::Running,
            TaskState::Completed | TaskState::Cancelled if allow_lane_reopen => TaskState::Running,
            _ => current,
        },
        LegacyControllerEventKind::Blocked => match current {
            TaskState::Queued | TaskState::Running | TaskState::EndedUnknown => TaskState::Blocked,
            _ => current,
        },
        LegacyControllerEventKind::Progress => {
            if current == TaskState::EndedUnknown {
                TaskState::Running
            } else {
                current
            }
        }
        LegacyControllerEventKind::Complete => terminal_transition(current, TaskState::Completed),
        LegacyControllerEventKind::Failed => terminal_transition(current, TaskState::Failed),
        LegacyControllerEventKind::Cancelled => terminal_transition(current, TaskState::Cancelled),
        LegacyControllerEventKind::Other => match supplied {
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled => {
                terminal_transition(current, supplied)
            }
            TaskState::Running => match current {
                TaskState::Queued | TaskState::Blocked | TaskState::EndedUnknown => {
                    TaskState::Running
                }
                _ => current,
            },
            TaskState::Blocked => match current {
                TaskState::Queued | TaskState::Running | TaskState::EndedUnknown => {
                    TaskState::Blocked
                }
                _ => current,
            },
            TaskState::Queued => {
                if current == TaskState::EndedUnknown {
                    TaskState::Running
                } else {
                    current
                }
            }
            TaskState::EndedUnknown => current,
        },
    }
}

fn terminal_transition(current: TaskState, target: TaskState) -> TaskState {
    match current {
        TaskState::Completed | TaskState::Failed | TaskState::Cancelled if current != target => {
            current
        }
        _ => target,
    }
}

#[derive(Clone, Copy)]
enum LegacyControllerEventKind {
    Started,
    Blocked,
    Progress,
    Complete,
    Failed,
    Cancelled,
    Other,
}

fn controller_event_kind(source_event_type: &str) -> LegacyControllerEventKind {
    match source_event_type {
        "task_started" => LegacyControllerEventKind::Started,
        "blocked" => LegacyControllerEventKind::Blocked,
        "progress" => LegacyControllerEventKind::Progress,
        "complete" => LegacyControllerEventKind::Complete,
        "failed" => LegacyControllerEventKind::Failed,
        "cancelled" => LegacyControllerEventKind::Cancelled,
        _ => LegacyControllerEventKind::Other,
    }
}

fn next_execution_state(
    current: &ExecState,
    observed: &ExecState,
    metadata: &EventMetadata,
) -> ExecState {
    if metadata.source == "herdr" && metadata.source_event_type == "done" {
        return if current.is_terminal() {
            ExecState::Ended
        } else {
            ExecState::Idle
        };
    }
    match observed {
        ExecState::Stale { .. } => match current {
            ExecState::Ended => ExecState::Ended,
            ExecState::Stale { since_ms }
                if metadata.receipt_time_ms.saturating_sub(*since_ms) >= STALE_GRACE_MS =>
            {
                ExecState::Ended
            }
            ExecState::Stale { since_ms } => ExecState::Stale {
                since_ms: *since_ms,
            },
            _ => ExecState::Stale {
                since_ms: metadata.receipt_time_ms,
            },
        },
        ExecState::Ended => ExecState::Ended,
        _state if current.is_terminal() => ExecState::Ended,
        state => state.clone(),
    }
}

fn provisional_key(terminal_id: &str, timestamp_ms: i64, ordinal: DisplayOrdinal) -> RunKey {
    let seq = u64::try_from(ordinal.get()).unwrap_or(0);
    RunKey::Provisional {
        terminal_id: terminal_id.to_owned(),
        start_ms: timestamp_ms,
        seq,
    }
}

fn native_binding(model: &DomainModel, run_id: RunId) -> Option<NativeSessionBinding> {
    let bound = model
        .task_run_bindings()
        .filter_map(|(key, owner)| {
            if *owner != run_id {
                return None;
            }
            match key {
                RunKey::Native { provider, sid } => Some(NativeSessionBinding {
                    provider: *provider,
                    native_session_id: sid.clone(),
                }),
                _ => None,
            }
        })
        .next();
    if bound.is_some() {
        return bound;
    }

    let mut bindings = model
        .agent_nodes()
        .filter(|node| node.task_run_id == run_id)
        .filter_map(|node| {
            node.native_session_id
                .as_ref()
                .map(|sid| NativeSessionBinding {
                    provider: node.provider,
                    native_session_id: sid.clone(),
                })
        });
    let first = bindings.next()?;
    bindings.all(|binding| binding == first).then_some(first)
}

fn persist_execution(execution: Execution, timestamp_ms: i64) -> PersistOp {
    let ended_at_ms = execution.state.is_terminal().then_some(timestamp_ms);
    PersistOp::UpsertExecution(PersistExecution {
        execution,
        started_at_ms: timestamp_ms,
        updated_at_ms: timestamp_ms,
        ended_at_ms,
    })
}

fn snapshot_provider(agent_name: &str, session_agent: Option<&str>) -> Option<Provider> {
    session_agent
        .and_then(provider_from_name)
        .or_else(|| provider_from_name(agent_name))
}

fn stable_provider_lifecycle_run_key_identity(key: &RunKey) -> String {
    match key {
        RunKey::Controller(controller_id) => {
            format!("v1:controller:{}:{controller_id}", controller_id.len())
        }
        RunKey::Native { provider, sid } => format!(
            "v1:native:{}:{}:{sid}",
            stable_provider_token(*provider),
            sid.len()
        ),
        RunKey::NativePath { provider, path } => format!(
            "v1:native-path:{}:{}:{path}",
            stable_provider_token(*provider),
            path.len()
        ),
        RunKey::Provisional {
            terminal_id,
            start_ms,
            seq,
        } => format!(
            "v1:provisional:{}:{terminal_id}:{start_ms}:{seq}",
            terminal_id.len()
        ),
    }
}

const fn stable_provider_token(provider: Provider) -> &'static str {
    match provider {
        Provider::Claude => "claude",
        Provider::Codex => "codex",
    }
}

fn provider_from_name(name: &str) -> Option<Provider> {
    let normalized = name.to_ascii_lowercase();
    if normalized.contains("codex") {
        Some(Provider::Codex)
    } else if normalized.contains("claude") {
        Some(Provider::Claude)
    } else {
        None
    }
}

fn unix_now_ms() -> i64 {
    let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    elapsed.as_millis().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::Arc;
    #[cfg(feature = "workload-harness")]
    use std::sync::Mutex;

    use crate::activity::ActivityDurability;
    use crate::diagnostics::RuntimeWriteOutcome;
    use crate::lockfile::StateRoot;
    use crate::model::{
        AgentNode, AgentNodeObservation, AgentSessionReference, AgentSessionReferenceKind,
        ControllerEvent, ControllerEventKind, DependencyEdge, DisplayOrdinal, DomainModel,
        EventMetadata, ExecState, Execution, ExecutionEdge, GapKind, MinimalProviderMetadata,
        NativeLifecycleWatermark, NativeSessionEndStatus, NormalizedEvent, OperatorCommand, Pane,
        PaneAgentStatus, PaneAgentStatusObservation, PaneSnapshot, Provider, ReconcileBatch, RunId,
        RunKey, RunRateTotals, SharedModel, SnapshotAgent, Tab, TaskRun, TaskRunV6State, TaskState,
        TopologyAuthority, TopologyEntity, TopologyEntityId, TopologySnapshot, TurnAttr, Workspace,
    };
    use crate::provider::claude_facts::{extract_claude_line, extract_meta_json};
    use crate::provider::facts::{EvidenceId, LogFact, SessionScope};
    use crate::provider::lane::{
        Admission, AdmissionIndex, SOURCE_LOG_LANE, Synthesis, run_key_for_scope,
    };
    use crate::provider::{HistoryDrainBarrier, ProviderEvent};
    use crate::store::{
        NativeSessionBinding, PersistHistoryDrain, PersistHistoryDrainRun, PersistOp,
        PersistTaskRun, PersistTaskRunV6, PersistV6Batch, RestoredState, WriterClient,
        database_path, open_reader, open_writer, spawn_writer,
    };

    use super::{
        ApplyOutcome, CommitStagedError, ProviderSubmissionReceipt, RateObservation,
        RateObservationOrigin, Reducer, ReducerError, RejectReason, unix_now_ms,
    };
    #[cfg(feature = "workload-harness")]
    use super::{
        WORKLOAD_TIMING_STATE, WorkloadObservationTiming, WorkloadTimingKind,
        WorkloadTimingObservation, WorkloadTimingObserver,
    };

    fn rate_reducer() -> (Reducer, SharedModel, RunId, RunKey) {
        let run_id = RunId::new();
        let key = RunKey::Controller("rate-ledger".to_owned());
        let mut model = DomainModel::default();
        model.insert_task_run(run(run_id, key.clone(), 1, TaskState::Running));
        model.insert_execution(execution(run_id, "rate-execution", ExecState::Working));
        model.set_pane_agent_status("pane-1".to_owned(), PaneAgentStatus::Working);
        let (reducer, shared) = Reducer::new(restored(model, 2));
        (reducer, shared, run_id, key)
    }

    fn add_rate_tokens(reducer: &mut Reducer, run_id: RunId, at_ms: i64, output_tokens: u64) {
        reducer.model.telemetry_entry(run_id, at_ms).accumulate(
            output_tokens,
            None,
            None,
            None,
            false,
        );
    }

    fn observe_live_rate(reducer: &mut Reducer, run_id: RunId, epoch: u64, at_ms: i64) {
        let persist = reducer.observe_run_rates(RateObservation {
            run_id,
            origin: RateObservationOrigin::Live { epoch },
            observed_at_ms: at_ms,
        });
        assert!(
            persist.is_empty(),
            "rate observations are checkpointed separately"
        );
    }

    #[test]
    fn active_rate_ledger_uses_prior_working_intervals_and_delayed_idle_tokens() {
        let (mut reducer, _shared, run_id, _) = rate_reducer();
        let epoch = reducer.begin_rate_epoch();

        add_rate_tokens(&mut reducer, run_id, 1_000, 100);
        observe_live_rate(&mut reducer, run_id, epoch, 1_000);
        add_rate_tokens(&mut reducer, run_id, 3_000, 40);
        observe_live_rate(&mut reducer, run_id, epoch, 3_000);

        reducer
            .model
            .set_pane_agent_status("pane-1".to_owned(), PaneAgentStatus::Idle);
        observe_live_rate(&mut reducer, run_id, epoch, 3_000);
        add_rate_tokens(&mut reducer, run_id, 13_000, 10);
        observe_live_rate(&mut reducer, run_id, epoch, 13_000);

        reducer
            .model
            .set_pane_agent_status("pane-1".to_owned(), PaneAgentStatus::Working);
        observe_live_rate(&mut reducer, run_id, epoch, 13_000);
        add_rate_tokens(&mut reducer, run_id, 14_000, 20);
        observe_live_rate(&mut reducer, run_id, epoch, 14_000);

        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 70,
                working_ms: 3_000,
            })
        );
    }

    #[test]
    fn blocked_queued_unknown_and_terminal_rate_states_remain_paused() {
        let cases = [
            ("blocked", TaskState::Blocked, None, None),
            ("queued", TaskState::Queued, None, None),
            (
                "unknown",
                TaskState::Running,
                Some(ExecState::Unknown),
                None,
            ),
            ("semantic-terminal", TaskState::Completed, None, None),
            (
                "native-terminal",
                TaskState::Running,
                None,
                Some(NativeSessionEndStatus::Done),
            ),
        ];

        for (label, state, execution_state, native_end) in cases {
            let run_id = RunId::new();
            let mut model = DomainModel::default();
            model.insert_task_run(run_with_controller_evidence(
                run_id,
                RunKey::Controller(format!("rate-{label}")),
                1,
                state,
            ));
            if let Some(execution_state) = execution_state {
                model.insert_execution(execution(run_id, "paused-execution", execution_state));
            }
            if let Some(status) = native_end {
                model.set_task_run_v6_state(
                    run_id,
                    TaskRunV6State {
                        native_session_end: Some(crate::model::NativeSessionEnd {
                            status,
                            at_ms: 500,
                        }),
                        ..TaskRunV6State::default()
                    },
                );
            }
            let (mut reducer, _shared) = Reducer::new(restored(model, 2));
            let epoch = reducer.begin_rate_epoch();
            add_rate_tokens(&mut reducer, run_id, 1_000, 100);
            observe_live_rate(&mut reducer, run_id, epoch, 1_000);
            add_rate_tokens(&mut reducer, run_id, 11_000, 10);
            observe_live_rate(&mut reducer, run_id, epoch, 11_000);

            assert_eq!(
                reducer
                    .model
                    .run_rate_totals(&run_id)
                    .map(|totals| totals.working_ms),
                Some(0),
                "case {label} must add no working duration"
            );
        }
    }

    #[test]
    fn shared_working_pane_occurrences_or_without_multiplying_elapsed_time() {
        let (mut reducer, _shared, run_id, _) = rate_reducer();
        reducer.model.insert_execution(Execution {
            execution_id: "rate-execution-2".to_owned(),
            pane_id: "pane-2".to_owned(),
            terminal_id: "terminal-2".to_owned(),
            task_run_id: run_id,
            state: ExecState::Idle,
        });
        reducer
            .model
            .set_pane_agent_status("pane-1".to_owned(), PaneAgentStatus::Idle);
        reducer
            .model
            .set_pane_agent_status("pane-2".to_owned(), PaneAgentStatus::Working);
        let epoch = reducer.begin_rate_epoch();

        add_rate_tokens(&mut reducer, run_id, 1_000, 100);
        observe_live_rate(&mut reducer, run_id, epoch, 1_000);
        add_rate_tokens(&mut reducer, run_id, 2_000, 10);
        observe_live_rate(&mut reducer, run_id, epoch, 2_000);

        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 10,
                working_ms: 1_000,
            })
        );
    }

    #[test]
    fn same_observation_uses_post_transition_status_and_tokens_coherently() {
        let (mut reducer, _shared, run_id, _) = rate_reducer();
        let epoch = reducer.begin_rate_epoch();
        add_rate_tokens(&mut reducer, run_id, 1_000, 100);
        observe_live_rate(&mut reducer, run_id, epoch, 1_000);

        reducer
            .model
            .set_pane_agent_status("pane-1".to_owned(), PaneAgentStatus::Idle);
        add_rate_tokens(&mut reducer, run_id, 3_000, 40);
        observe_live_rate(&mut reducer, run_id, epoch, 3_000);
        add_rate_tokens(&mut reducer, run_id, 13_000, 10);
        observe_live_rate(&mut reducer, run_id, epoch, 13_000);

        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 50,
                working_ms: 2_000,
            })
        );
    }

    #[test]
    fn historical_replay_only_rebaselines_before_the_first_live_sample() {
        let (mut reducer, _shared, run_id, _) = rate_reducer();
        let historical = RateObservationOrigin::Historical;
        add_rate_tokens(&mut reducer, run_id, 1_000, 100);
        reducer.observe_run_rates(RateObservation {
            run_id,
            origin: historical,
            observed_at_ms: 1_000,
        });
        add_rate_tokens(&mut reducer, run_id, 3_000, 40);
        reducer.observe_run_rates(RateObservation {
            run_id,
            origin: historical,
            observed_at_ms: 3_000,
        });
        assert_eq!(reducer.model.run_rate_totals(&run_id), None);

        let epoch = reducer.begin_rate_epoch();
        observe_live_rate(&mut reducer, run_id, epoch, 4_000);
        add_rate_tokens(&mut reducer, run_id, 5_000, 10);
        observe_live_rate(&mut reducer, run_id, epoch, 5_000);

        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 10,
                working_ms: 1_000,
            })
        );
    }

    #[test]
    fn counter_regression_adds_no_tokens_but_closes_prior_working_time() {
        let (mut reducer, _shared, run_id, _) = rate_reducer();
        let epoch = reducer.begin_rate_epoch();
        add_rate_tokens(&mut reducer, run_id, 1_000, 100);
        observe_live_rate(&mut reducer, run_id, epoch, 1_000);
        add_rate_tokens(&mut reducer, run_id, 2_000, 10);
        observe_live_rate(&mut reducer, run_id, epoch, 2_000);

        reducer.model.telemetry_entry(run_id, 2_500).output_tokens = 25;
        observe_live_rate(&mut reducer, run_id, epoch, 2_500);

        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 10,
                working_ms: 1_500,
            })
        );
        assert_eq!(
            reducer
                .model
                .run_rate_cursor(&run_id)
                .map(|cursor| cursor.baseline_output_tokens),
            Some(25)
        );
    }

    #[test]
    fn clock_reversal_adds_positive_token_delta_once_and_zero_duration() {
        let (mut reducer, _shared, run_id, _) = rate_reducer();
        let epoch = reducer.begin_rate_epoch();
        add_rate_tokens(&mut reducer, run_id, 1_000, 100);
        observe_live_rate(&mut reducer, run_id, epoch, 1_000);
        add_rate_tokens(&mut reducer, run_id, 2_000, 10);
        observe_live_rate(&mut reducer, run_id, epoch, 2_000);

        reducer.model.telemetry_entry(run_id, 1_500).output_tokens = 115;
        observe_live_rate(&mut reducer, run_id, epoch, 1_500);
        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 15,
                working_ms: 1_000,
            })
        );

        observe_live_rate(&mut reducer, run_id, epoch, 2_500);
        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 15,
                working_ms: 1_500,
            }),
            "the monotonic cumulative delta from the reversed timestamp must not repeat"
        );
    }

    #[test]
    fn new_epoch_and_cold_restore_drop_cursors_without_losing_closed_totals() {
        let (mut reducer, _shared, run_id, _) = rate_reducer();
        let first_epoch = reducer.begin_rate_epoch();
        add_rate_tokens(&mut reducer, run_id, 1_000, 100);
        observe_live_rate(&mut reducer, run_id, first_epoch, 1_000);
        add_rate_tokens(&mut reducer, run_id, 2_000, 10);
        observe_live_rate(&mut reducer, run_id, first_epoch, 2_000);

        let second_epoch = reducer.begin_rate_epoch();
        add_rate_tokens(&mut reducer, run_id, 12_000, 90);
        observe_live_rate(&mut reducer, run_id, second_epoch, 12_000);
        add_rate_tokens(&mut reducer, run_id, 13_000, 10);
        observe_live_rate(&mut reducer, run_id, second_epoch, 13_000);
        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 20,
                working_ms: 2_000,
            })
        );

        let mut restored_model = DomainModel::default();
        restored_model.insert_task_run(run(
            run_id,
            RunKey::Controller("rate-ledger".to_owned()),
            1,
            TaskState::Running,
        ));
        restored_model.insert_execution(execution(
            run_id,
            "restored-rate-execution",
            ExecState::Working,
        ));
        restored_model.set_run_rate_totals(
            run_id,
            RunRateTotals {
                output_tokens: 20,
                working_ms: 2_000,
            },
        );
        let (mut restarted, _restarted_shared) = Reducer::new(restored(restored_model, 2));
        let restarted_epoch = restarted.begin_rate_epoch();
        add_rate_tokens(&mut restarted, run_id, 23_000, 1_000);
        observe_live_rate(&mut restarted, run_id, restarted_epoch, 23_000);

        assert_eq!(
            restarted.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 20,
                working_ms: 2_000,
            })
        );
    }

    #[test]
    fn sweep_checkpoint_emits_only_changed_totals_and_graceful_tail() {
        let (mut reducer, _shared, run_id, _) = rate_reducer();
        reducer.begin_rate_epoch();
        add_rate_tokens(&mut reducer, run_id, 1_000, 100);
        reducer.activate_rate_epoch(1_000);
        assert!(reducer.checkpoint_run_rates(1_000).is_empty());

        add_rate_tokens(&mut reducer, run_id, 3_000, 20);
        assert_eq!(
            reducer.checkpoint_run_rates(3_000),
            vec![(
                run_id,
                RunRateTotals {
                    output_tokens: 20,
                    working_ms: 2_000,
                },
            )]
        );
        assert!(
            reducer.checkpoint_run_rates(3_000).is_empty(),
            "an unchanged sweep must not rewrite closed totals"
        );

        add_rate_tokens(&mut reducer, run_id, 4_000, 5);
        assert_eq!(
            reducer.checkpoint_run_rates(4_000),
            vec![(
                run_id,
                RunRateTotals {
                    output_tokens: 25,
                    working_ms: 3_000,
                },
            )],
            "the graceful final checkpoint must include the unflushed tail"
        );
    }

    #[test]
    fn inactive_gap_epoch_ignores_publications_tokens_and_checkpoint_time() {
        let (mut reducer, _shared, run_id, key) = rate_reducer();
        reducer.begin_rate_epoch();
        reducer.apply_telemetry(&key, 10_000, 150, None, None, None);

        assert!(reducer.checkpoint_run_rates(20_000).is_empty());
        assert_eq!(reducer.model.run_rate_cursor(&run_id), None);
        assert_eq!(reducer.model.run_rate_totals(&run_id), None);

        reducer.activate_rate_epoch(20_000);
        assert!(reducer.checkpoint_run_rates(20_000).is_empty());
        assert_eq!(
            reducer
                .model
                .run_rate_cursor(&run_id)
                .map(|cursor| cursor.baseline_output_tokens),
            Some(150),
            "the first post-reconciliation baseline must exclude offline tokens"
        );
    }

    #[test]
    fn reconciliation_drains_newly_attributable_telemetry_before_live_baseline() {
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let key = RunKey::Native {
            provider: Provider::Codex,
            sid: "pending-before-snapshot".to_owned(),
        };
        reducer.apply_telemetry(&key, 1_000, 100, None, None, None);

        reducer
            .reconcile_snapshot(native_snapshot("pending-before-snapshot"))
            .unwrap();

        let run_id = reducer.model.task_run_by_key(&key).unwrap().run_id;
        assert_eq!(
            reducer
                .model
                .telemetry(&run_id)
                .map(|value| value.output_tokens),
            Some(100),
            "the authoritative snapshot must make the queued sample attributable"
        );
        assert_eq!(
            reducer
                .model
                .run_rate_cursor(&run_id)
                .map(|cursor| cursor.baseline_output_tokens),
            Some(100),
            "pre-snapshot tokens belong in the fresh live baseline"
        );
        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            None,
            "pre-snapshot tokens must never enter the measured numerator"
        );
    }

    #[test]
    fn authoritative_idle_reconciliation_baselines_before_delayed_tokens() {
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut snapshot = native_snapshot("idle-reconciliation-rate");
        snapshot.panes[0].agent.as_mut().unwrap().status = PaneAgentStatus::Idle;
        reducer.reconcile_snapshot(snapshot).unwrap();

        let key = RunKey::Native {
            provider: Provider::Codex,
            sid: "idle-reconciliation-rate".to_owned(),
        };
        let run_id = reducer.model.task_run_by_key(&key).unwrap().run_id;
        reducer.apply_telemetry(&key, 10_000, 25, None, None, None);
        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 25,
                working_ms: 0,
            }),
            "the first delayed Idle report must be measured from the authoritative baseline"
        );

        let epoch = reducer.rate_epoch();
        observe_live_rate(&mut reducer, run_id, epoch, 20_000);
        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 25,
                working_ms: 0,
            }),
            "the same cumulative Idle tokens must be counted exactly once"
        );
    }

    #[test]
    fn durable_history_finalization_rebaselines_before_live_measurement_resumes() {
        let (mut reducer, _shared, run_id, _) = rate_reducer();
        let epoch = reducer.begin_rate_epoch();
        let finalized_at_ms = unix_now_ms();
        reducer.activate_rate_epoch(finalized_at_ms.saturating_sub(4_000));

        add_rate_tokens(
            &mut reducer,
            run_id,
            finalized_at_ms.saturating_sub(3_000),
            100,
        );
        reducer.observe_run_rates(RateObservation {
            run_id,
            origin: RateObservationOrigin::Historical,
            observed_at_ms: finalized_at_ms.saturating_sub(3_000),
        });
        add_rate_tokens(
            &mut reducer,
            run_id,
            finalized_at_ms.saturating_sub(2_000),
            40,
        );
        reducer.observe_run_rates(RateObservation {
            run_id,
            origin: RateObservationOrigin::Historical,
            observed_at_ms: finalized_at_ms.saturating_sub(2_000),
        });

        let drain_id = crate::model::HistoryDrainId::new("codex:rate-finalization").unwrap();
        assert!(
            reducer.apply_history_finalization(&crate::store::HistoryDrainFinalization {
                completed_drains: vec![drain_id.clone()],
                drain_id,
                finalized_at_ms,
                runs: vec![crate::store::FinalizedHistoryRun {
                    run_id,
                    state: reducer
                        .model
                        .task_run_v6_state(&run_id)
                        .cloned()
                        .unwrap_or_default(),
                }],
            })
        );

        add_rate_tokens(&mut reducer, run_id, finalized_at_ms + 1_000, 10);
        observe_live_rate(&mut reducer, run_id, epoch, finalized_at_ms + 1_000);
        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            None,
            "the first real post-barrier live observation establishes a baseline only"
        );

        add_rate_tokens(&mut reducer, run_id, finalized_at_ms + 2_000, 10);
        observe_live_rate(&mut reducer, run_id, epoch, finalized_at_ms + 2_000);
        assert_eq!(
            reducer.model.run_rate_totals(&run_id),
            Some(&RunRateTotals {
                output_tokens: 10,
                working_ms: 1_000,
            })
        );
    }

    #[test]
    fn retained_history_does_not_expand_activation_or_checkpoint_rate_operations() {
        let active_run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            active_run_id,
            RunKey::Controller("active-rate".to_owned()),
            1,
            TaskState::Running,
        ));
        model.insert_execution(execution(
            active_run_id,
            "active-rate-execution",
            ExecState::Working,
        ));
        model.set_pane_agent_status("pane-1".to_owned(), PaneAgentStatus::Working);
        for ordinal in 2..=257 {
            let run_id = RunId::new();
            model.insert_task_run(run_with_controller_evidence(
                run_id,
                RunKey::Controller(format!("retained-terminal-{ordinal}")),
                ordinal,
                TaskState::Completed,
            ));
        }
        model
            .telemetry_entry(active_run_id, 1_000)
            .accumulate(100, None, None, None, false);
        let (mut reducer, _shared) = Reducer::new(restored(model, 258));
        reducer.begin_rate_epoch();

        reducer.take_rate_observation_count();
        reducer.activate_rate_epoch(1_000);
        let activation_observations = reducer.take_rate_observation_count();
        reducer.checkpoint_run_rates(2_000);
        let checkpoint_observations = reducer.take_rate_observation_count();

        assert_eq!(
            (activation_observations, checkpoint_observations),
            (1, 1),
            "256 retained terminal runs must add zero rate operations"
        );
    }

    #[test]
    fn one_publication_derives_status_evidence_once_for_multiple_touched_runs() {
        let mut model = DomainModel::default();
        let mut touched = Vec::new();
        for ordinal in 1..=64 {
            let run_id = RunId::new();
            let key = RunKey::Controller(format!("publication-rate-{ordinal}"));
            model.insert_task_run(run(run_id, key.clone(), ordinal, TaskState::Running));
            model.insert_execution(Execution {
                execution_id: format!("publication-execution-{ordinal}"),
                pane_id: format!("publication-pane-{ordinal}"),
                terminal_id: format!("publication-terminal-{ordinal}"),
                task_run_id: run_id,
                state: ExecState::Working,
            });
            if ordinal <= 2 {
                touched.push((run_id, key));
            }
        }
        let (mut reducer, _shared) = Reducer::new(restored(model, 65));
        let epoch = reducer.begin_rate_epoch();
        reducer.rate_epoch_active = true;
        for (run_id, key) in &touched {
            reducer.model.set_run_rate_cursor(
                *run_id,
                crate::model::RunRateCursor {
                    baseline_output_tokens: 0,
                    last_observed_at_ms: 1_000,
                    working: true,
                    measurement_epoch: epoch,
                    identity_basis: key.clone(),
                    live_baseline: true,
                },
            );
            reducer.pending_rate_observation_runs.insert(*run_id);
        }

        crate::status::reset_rate_status_evidence_visits();
        reducer.publish();
        assert_eq!(
            crate::status::rate_status_evidence_visits(),
            64,
            "two touched runs must share one 64-execution status derivation"
        );
        assert_eq!(
            crate::status::rate_pane_execution_candidate_visits(),
            2,
            "each touched run must inspect only its own execution evidence"
        );
    }

    #[test]
    fn history_finalization_is_staged_then_published_once_after_known_commit() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let run_id = RunId::new();
        let drain_id = crate::model::HistoryDrainId::new("codex:reducer-finalize").unwrap();
        let manifest = Arc::new(PersistHistoryDrain {
            drain_id: drain_id.clone(),
            provider: Provider::Codex,
            created_at_ms: 1_000,
            artifacts: Vec::new(),
        });
        let task_run = run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "reducer-finalize".to_owned(),
            },
            1,
            TaskState::Queued,
        );
        store
            .apply_v6_batch(PersistV6Batch {
                task_runs: vec![PersistTaskRunV6 {
                    task_run: PersistTaskRun {
                        task_run,
                        native_session: Some(NativeSessionBinding {
                            provider: Provider::Codex,
                            native_session_id: "reducer-finalize".to_owned(),
                        }),
                        created_at_ms: 1_000,
                        updated_at_ms: 4_000,
                        finished_at_ms: None,
                    },
                    state: TaskRunV6State {
                        history_ready: false,
                        latest_provider_at_ms: Some(4_000),
                        ..TaskRunV6State::default()
                    },
                }],
                history_drains: vec![manifest.as_ref().clone()],
                history_associations: vec![PersistHistoryDrainRun {
                    drain_id: drain_id.clone(),
                    run_id,
                }],
                ..PersistV6Batch::default()
            })
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (mut reducer, shared) = Reducer::new(restored);
        let publish_count = reducer.shared_publish_count();
        let barrier = HistoryDrainBarrier::new(Arc::clone(&manifest), 5_000);

        let staged = reducer.stage_history_finalization(&barrier);

        assert!(Arc::ptr_eq(&staged.manifest, &manifest));
        assert_eq!(staged.manifest.drain_id, drain_id);
        assert_eq!(publish_count.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(shared.borrow().task_run(&run_id).is_none());

        let durable_page = store
            .finalize_history_drain(staged.manifest.as_ref(), staged.observed_at_ms)
            .unwrap();
        assert!(reducer.apply_history_finalization(&durable_page));
        assert_eq!(publish_count.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(
            shared
                .borrow()
                .task_run_v6_state(&run_id)
                .unwrap()
                .history_ready
        );

        assert!(!reducer.apply_history_finalization(&durable_page));
        assert_eq!(publish_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn staged_history_finalization_retains_barrier_manifest() {
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let manifest = std::sync::Arc::new(PersistHistoryDrain {
            drain_id: crate::model::HistoryDrainId::new("codex:staged-manifest").unwrap(),
            provider: Provider::Codex,
            created_at_ms: 1_000,
            artifacts: Vec::new(),
        });
        let barrier = HistoryDrainBarrier::new(std::sync::Arc::clone(&manifest), 2_000);

        let staged = reducer.stage_history_finalization(&barrier);

        assert!(std::sync::Arc::ptr_eq(&staged.manifest, &manifest));
        assert!(std::sync::Arc::ptr_eq(&staged.manifest, &barrier.manifest));
        assert_eq!(staged.observed_at_ms, 2_000);
    }

    #[test]
    fn historical_activity_publishes_model_and_operator_only_at_durable_finalization() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let drain_id = crate::model::HistoryDrainId::new("codex:deferred-operator").unwrap();
        let manifest = Arc::new(PersistHistoryDrain {
            drain_id: drain_id.clone(),
            provider: Provider::Codex,
            created_at_ms: 1_000,
            artifacts: Vec::new(),
        });
        let sibling_drain_id =
            crate::model::HistoryDrainId::new("claude:deferred-operator").unwrap();
        let sibling_manifest = Arc::new(PersistHistoryDrain {
            drain_id: sibling_drain_id.clone(),
            provider: Provider::Claude,
            created_at_ms: 1_000,
            artifacts: Vec::new(),
        });
        store
            .apply_v6_batch(PersistV6Batch {
                history_drains: vec![manifest.as_ref().clone(), sibling_manifest.as_ref().clone()],
                ..PersistV6Batch::default()
            })
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (mut reducer, shared, mut operator) = Reducer::new_with_operator(
            restored,
            crate::activity::RestoredOperatorState {
                activity: Vec::new(),
                terminal_times: HashMap::new(),
            },
        );
        let origin = crate::model::ObservationOrigin::Historical {
            drain_id: drain_id.clone(),
            artifact_id: "frozen.jsonl".to_owned(),
        };
        let mut controller = provider_lane_event(
            "historical-new-run",
            "historical-new-run",
            ControllerEventKind::TaskStarted,
            2_000,
            2_100,
        );
        controller.metadata.provider = Some(Provider::Codex);
        controller.metadata.native_session_id = Some("historical-new-run".to_owned());

        let prior = reducer.begin_provider_observation(&origin, 2_000);
        let delta = reducer.validate_controller_event(&controller).unwrap();
        let operations = reducer.commit_staged_unqueued(delta).unwrap();
        let (batch, receipt) = reducer.finish_provider_observation(
            prior,
            operations,
            &origin,
            Some(manifest.as_ref()),
            2_000,
        );
        assert_eq!(batch.history_associations.len(), 1);
        store.apply_v6_batch(batch).unwrap();
        reducer.complete_provider_submission(receipt, RuntimeWriteOutcome::Durable);

        let sibling_origin = crate::model::ObservationOrigin::Historical {
            drain_id: sibling_drain_id.clone(),
            artifact_id: "sibling.jsonl".to_owned(),
        };
        let mut sibling_controller = provider_lane_event(
            "historical-sibling-run",
            "historical-sibling-run",
            ControllerEventKind::TaskStarted,
            2_200,
            2_300,
        );
        sibling_controller.metadata.provider = Some(Provider::Claude);
        sibling_controller.metadata.native_session_id = Some("historical-sibling-run".to_owned());
        let sibling_prior = reducer.begin_provider_observation(&sibling_origin, 2_200);
        let sibling_delta = reducer
            .validate_controller_event(&sibling_controller)
            .unwrap();
        let sibling_operations = reducer.commit_staged_unqueued(sibling_delta).unwrap();
        let (sibling_batch, sibling_receipt) = reducer.finish_provider_observation(
            sibling_prior,
            sibling_operations,
            &sibling_origin,
            Some(sibling_manifest.as_ref()),
            2_200,
        );
        assert_eq!(sibling_batch.history_associations.len(), 1);
        store.apply_v6_batch(sibling_batch).unwrap();
        reducer.complete_provider_submission(sibling_receipt, RuntimeWriteOutcome::Durable);

        assert!(shared.borrow().task_runs().next().is_none());
        assert!(
            operator.borrow().activity.is_empty(),
            "historical activity escaped through OperatorSnapshot before finalization"
        );

        assert!(operator.borrow().activity.is_empty());

        let barrier = HistoryDrainBarrier::new(Arc::clone(&manifest), 3_000);
        let staged = reducer.stage_history_finalization(&barrier);
        let page = store
            .finalize_history_drain(staged.manifest.as_ref(), staged.observed_at_ms)
            .unwrap();
        assert_eq!(page.runs.len(), 1);
        assert!(reducer.apply_history_finalization(&page));
        assert_eq!(shared.borrow().task_runs().count(), 1);
        assert_eq!(operator.borrow().activity.len(), 1);
        assert_eq!(
            operator.borrow().activity[0].identity.event_id,
            "historical-new-run"
        );

        operator.borrow_and_update();
        assert!(!reducer.apply_history_finalization(&page));
        assert!(!operator.has_changed().unwrap());
        assert_eq!(shared.borrow().task_runs().count(), 1);
        assert_eq!(operator.borrow().activity.len(), 1);

        let sibling_barrier = HistoryDrainBarrier::new(Arc::clone(&sibling_manifest), 3_100);
        let sibling_staged = reducer.stage_history_finalization(&sibling_barrier);
        let sibling_page = store
            .finalize_history_drain(
                sibling_staged.manifest.as_ref(),
                sibling_staged.observed_at_ms,
            )
            .unwrap();
        assert!(reducer.apply_history_finalization(&sibling_page));
        assert_eq!(shared.borrow().task_runs().count(), 2);
        let operator_event_ids = operator
            .borrow()
            .activity
            .iter()
            .map(|activity| activity.identity.event_id.clone())
            .collect::<HashSet<_>>();
        assert_eq!(
            operator_event_ids,
            HashSet::from([
                "historical-new-run".to_owned(),
                "historical-sibling-run".to_owned(),
            ])
        );
    }

    #[test]
    fn historical_agent_mutation_of_ready_run_stays_at_published_before_image() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "ready-before-image".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.set_task_run_v6_state(run_id, TaskRunV6State::default());
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let drain_id = crate::model::HistoryDrainId::new("codex:ready-before-image").unwrap();
        let origin = crate::model::ObservationOrigin::Historical {
            drain_id: drain_id.clone(),
            artifact_id: "ready.jsonl".to_owned(),
        };
        let manifest = PersistHistoryDrain {
            drain_id,
            provider: Provider::Codex,
            created_at_ms: 1_000,
            artifacts: Vec::new(),
        };
        let prior = reducer.begin_provider_observation(&origin, 2_000);
        let outcome = reducer
            .apply(NormalizedEvent::AgentNodeUpsert {
                metadata: EventMetadata {
                    event_id: "historical-ready-agent".to_owned(),
                    timestamp_ms: 2_000,
                    receipt_time_ms: 2_000,
                    source: "provider".to_owned(),
                    source_event_type: "agent.updated".to_owned(),
                    herdr_session: "history-test".to_owned(),
                    workspace_id: None,
                    tab_id: None,
                    pane_id: None,
                    terminal_id: None,
                    provider: Some(Provider::Codex),
                    native_session_id: Some("ready-before-image".to_owned()),
                    task_run_id: Some(run_id),
                    agent_node_id: Some("historical-ready-agent".to_owned()),
                    task_state: None,
                    execution_parent: None,
                    dependency: None,
                    source_coverage: Vec::new(),
                    provider_metadata: None,
                    label: None,
                    reason: None,
                    progress: None,
                    ingest_seq: None,
                },
                node: AgentNodeObservation {
                    agent_node_id: "historical-ready-agent".to_owned(),
                    provider: Provider::Codex,
                    native_session_id: Some("ready-before-image".to_owned()),
                    task_run_id: run_id,
                    parent_agent_node_id: None,
                    state: Some(ExecState::Working),
                    model_id: Some("historical-model".to_owned()),
                    session_file: None,
                },
            })
            .unwrap();
        let ApplyOutcome::Applied(operations) = outcome else {
            panic!("historical agent observation must apply");
        };
        let _batch =
            reducer.finish_provider_observation(prior, operations, &origin, Some(&manifest), 2_000);
        reducer.complete_deferred_operator_submission(RuntimeWriteOutcome::Durable);

        assert!(
            shared
                .borrow()
                .agent_nodes()
                .all(|node| node.model_id.as_deref() != Some("historical-model"))
        );
        reducer.record_provider_identity_disagreement();
        assert!(
            shared
                .borrow()
                .agent_nodes()
                .all(|node| node.model_id.as_deref() != Some("historical-model")),
            "an ordinary publication exposed a historical Agent projection before finalization"
        );
    }

    #[test]
    fn historical_activity_only_record_preserves_the_ready_run_before_image() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "ready-activity-only".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.set_task_run_v6_state(run_id, TaskRunV6State::default());
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let drain_id = crate::model::HistoryDrainId::new("codex:ready-activity-only").unwrap();
        let origin = crate::model::ObservationOrigin::Historical {
            drain_id: drain_id.clone(),
            artifact_id: "activity-only.jsonl".to_owned(),
        };
        let prior = reducer.begin_provider_observation(&origin, 2_000);
        let mut event_metadata = metadata("ready-activity-only-event", 2_000);
        event_metadata.task_run_id = Some(run_id);
        let (batch, _receipt) = reducer.finish_provider_observation(
            prior,
            vec![PersistOp::RecordEvent {
                event: Box::new(NormalizedEvent::AgentActivity {
                    metadata: event_metadata,
                    agent_node_id: "activity-only-agent".to_owned(),
                    activity: MinimalProviderMetadata::default(),
                }),
                seen_at_ms: 2_000,
            }],
            &origin,
            Some(&PersistHistoryDrain {
                drain_id,
                provider: Provider::Codex,
                created_at_ms: 1_000,
                artifacts: Vec::new(),
            }),
            2_000,
        );

        assert_eq!(batch.history_publications.len(), 1);
        reducer.record_provider_identity_disagreement();
        assert!(shared.borrow().task_run(&run_id).is_some());
    }

    #[test]
    fn live_provider_start_readies_history_created_run_and_survives_finalization() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let drain_id = crate::model::HistoryDrainId::new("codex:history-live-start").unwrap();
        let manifest = PersistHistoryDrain {
            drain_id: drain_id.clone(),
            provider: Provider::Codex,
            created_at_ms: 1_000,
            artifacts: Vec::new(),
        };
        store
            .apply_v6_batch(PersistV6Batch {
                history_drains: vec![manifest.clone()],
                ..PersistV6Batch::default()
            })
            .unwrap();
        let (mut reducer, shared) = Reducer::new(store.load_restored_state().unwrap());
        let historical_origin = crate::model::ObservationOrigin::Historical {
            drain_id: drain_id.clone(),
            artifact_id: "history-live-start.jsonl".to_owned(),
        };
        let mut historical = provider_lane_event(
            "history-live-start-historical",
            "history-live-start",
            ControllerEventKind::TaskStarted,
            2_000,
            2_100,
        );
        historical.metadata.provider = Some(Provider::Codex);
        historical.metadata.native_session_id = Some("history-live-start".to_owned());
        let prior = reducer.begin_provider_observation(&historical_origin, 2_100);
        let delta = reducer.validate_controller_event(&historical).unwrap();
        let operations = reducer.commit_staged_unqueued(delta).unwrap();
        let (historical_batch, historical_receipt) = reducer.finish_provider_observation(
            prior,
            operations,
            &historical_origin,
            Some(&manifest),
            2_000,
        );
        let run_id = historical_batch.history_associations[0].run_id;
        store.apply_v6_batch(historical_batch).unwrap();
        reducer.complete_provider_submission(historical_receipt, RuntimeWriteOutcome::Durable);
        assert!(shared.borrow().task_run(&run_id).is_none());

        let mut live = provider_lane_event(
            "history-live-start-current",
            "history-live-start",
            ControllerEventKind::TaskStarted,
            3_000,
            3_100,
        );
        live.metadata.provider = Some(Provider::Codex);
        live.metadata.native_session_id = Some("history-live-start".to_owned());
        let live_origin = crate::model::ObservationOrigin::Live;
        let prior = reducer.begin_provider_observation(&live_origin, 3_100);
        let delta = reducer.validate_controller_event(&live).unwrap();
        let operations = reducer.commit_staged_unqueued(delta).unwrap();
        let (live_batch, live_receipt) =
            reducer.finish_provider_observation(prior, operations, &live_origin, None, 3_000);
        assert!(
            live_batch
                .task_runs
                .iter()
                .find(|persisted| persisted.task_run.task_run.run_id == run_id)
                .unwrap()
                .state
                .history_ready
        );
        store.apply_v6_batch(live_batch).unwrap();
        reducer.complete_provider_submission(live_receipt, RuntimeWriteOutcome::Durable);
        let current = shared.borrow();
        assert_eq!(current.task_run(&run_id).unwrap().state, TaskState::Running);
        let current_state = current.task_run_v6_state(&run_id).unwrap();
        assert!(current_state.history_ready);
        assert_eq!(current_state.native_session_end, None);
        assert_eq!(
            current_state
                .lifecycle_watermark
                .as_ref()
                .map(|watermark| watermark.source_at_ms),
            Some(3_000)
        );
        drop(current);

        let cold = store.load_restored_state().unwrap();
        let (_cold_reducer, cold_shared) = Reducer::new(cold);
        assert!(cold_shared.borrow().task_run(&run_id).is_some());
        assert_eq!(
            cold_shared
                .borrow()
                .task_run_v6_state(&run_id)
                .unwrap()
                .native_session_end,
            None
        );

        store.finalize_history_drain(&manifest, 4_000).unwrap();
        let finalized = store.load_restored_state().unwrap();
        let state = finalized.model.task_run_v6_state(&run_id).unwrap();
        assert!(state.history_ready);
        assert_eq!(state.native_session_end, None);
        assert_eq!(
            state
                .lifecycle_watermark
                .as_ref()
                .map(|watermark| watermark.source_at_ms),
            Some(3_000)
        );
    }

    #[test]
    fn live_identity_merge_releases_ready_run_before_image_without_lifecycle_regression() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let native_key = RunKey::Native {
            provider: Provider::Codex,
            sid: "ready-live-merge".to_owned(),
        };
        let provisional_key = RunKey::Provisional {
            terminal_id: "ready-live-merge-terminal".to_owned(),
            start_ms: 1_000,
            seq: 1,
        };
        let mut ready_run = run(survivor, native_key.clone(), 1, TaskState::Running);
        ready_run.has_controller_task_state_event = true;
        ready_run.created_at_ms = Some(500);
        ready_run.updated_at_ms = Some(1_000);
        let mut provisional_run = run(absorbed, provisional_key, 2, TaskState::Queued);
        provisional_run.created_at_ms = Some(1_000);
        provisional_run.updated_at_ms = Some(1_000);
        let ready_state = TaskRunV6State {
            native_session_end: Some(crate::model::NativeSessionEnd {
                status: NativeSessionEndStatus::Done,
                at_ms: 1_000,
            }),
            lifecycle_watermark: Some(NativeLifecycleWatermark {
                source_at_ms: 1_000,
                observed_at_ms: 1_000,
                source_order: "live:terminal-before-history".to_owned(),
            }),
            history_ready: true,
            latest_provider_at_ms: Some(1_000),
        };
        store
            .apply_v6_batch(PersistV6Batch {
                task_runs: vec![
                    PersistTaskRunV6 {
                        task_run: PersistTaskRun {
                            task_run: ready_run,
                            native_session: Some(NativeSessionBinding {
                                provider: Provider::Codex,
                                native_session_id: "ready-live-merge".to_owned(),
                            }),
                            created_at_ms: 500,
                            updated_at_ms: 1_000,
                            finished_at_ms: None,
                        },
                        state: ready_state,
                    },
                    PersistTaskRunV6 {
                        task_run: PersistTaskRun {
                            task_run: provisional_run,
                            native_session: None,
                            created_at_ms: 1_000,
                            updated_at_ms: 1_000,
                            finished_at_ms: None,
                        },
                        state: TaskRunV6State::default(),
                    },
                ],
                ..PersistV6Batch::default()
            })
            .unwrap();
        let drain_id = crate::model::HistoryDrainId::new("codex:ready-live-merge").unwrap();
        let manifest = PersistHistoryDrain {
            drain_id: drain_id.clone(),
            provider: Provider::Codex,
            created_at_ms: 1_500,
            artifacts: Vec::new(),
        };
        store
            .apply_v6_batch(PersistV6Batch {
                history_drains: vec![manifest.clone()],
                ..PersistV6Batch::default()
            })
            .unwrap();
        let (mut reducer, shared) = Reducer::new(store.load_restored_state().unwrap());
        let historical_origin = crate::model::ObservationOrigin::Historical {
            drain_id: drain_id.clone(),
            artifact_id: "ready-live-merge.jsonl".to_owned(),
        };
        let prior = reducer.begin_provider_observation(&historical_origin, 2_000);
        let historical_operations = reducer.touch_run_liveness_observed(&native_key, 2_000, 2_000);
        let (historical_batch, historical_receipt) = reducer.finish_provider_observation(
            prior,
            historical_operations,
            &historical_origin,
            Some(&manifest),
            2_000,
        );
        store.apply_v6_batch(historical_batch).unwrap();
        reducer.complete_provider_submission(historical_receipt, RuntimeWriteOutcome::Durable);
        reducer.record_provider_identity_disagreement();
        let before_live = shared.borrow();
        assert_eq!(
            before_live.task_run(&survivor).unwrap().state,
            TaskState::Running
        );
        assert_eq!(
            before_live
                .task_run_v6_state(&survivor)
                .unwrap()
                .native_session_end
                .as_ref()
                .map(|end| (end.status, end.at_ms)),
            Some((NativeSessionEndStatus::Done, 1_000))
        );
        drop(before_live);

        let mut live_metadata = metadata("ready-live-merge-current", 3_000);
        live_metadata.source = "provider".to_owned();
        live_metadata.source_event_type = "execution.begin".to_owned();
        live_metadata.provider = Some(Provider::Codex);
        live_metadata.native_session_id = Some("ready-live-merge".to_owned());
        live_metadata.task_run_id = Some(absorbed);
        let live_origin = crate::model::ObservationOrigin::Live;
        let prior = reducer.begin_provider_observation(&live_origin, 3_000);
        let ApplyOutcome::Applied(operations) = reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: live_metadata,
                execution: Execution {
                    execution_id: "ready-live-merge-current".to_owned(),
                    pane_id: "ready-live-merge-pane".to_owned(),
                    terminal_id: "ready-live-merge-terminal".to_owned(),
                    task_run_id: absorbed,
                    state: ExecState::Working,
                },
            })
            .unwrap()
        else {
            panic!("live identity evidence must merge into the native run");
        };
        assert!(operations.iter().any(|operation| matches!(
            operation,
            PersistOp::MergeTaskRuns {
                survivor: actual_survivor,
                absorbed: actual_absorbed,
            } if *actual_survivor == survivor && *actual_absorbed == absorbed
        )));
        let (live_batch, live_receipt) =
            reducer.finish_provider_observation(prior, operations, &live_origin, None, 3_000);
        store.apply_v6_batch(live_batch).unwrap();
        reducer.complete_provider_submission(live_receipt, RuntimeWriteOutcome::Durable);
        let current = shared.borrow();
        assert!(current.task_run(&absorbed).is_none());
        assert!(current.task_run_v6_state(&survivor).unwrap().history_ready);
        assert_eq!(
            current
                .task_run_v6_state(&survivor)
                .unwrap()
                .native_session_end,
            None
        );
        assert!(current.executions().any(|execution| {
            execution.execution_id == "ready-live-merge-current"
                && execution.task_run_id == survivor
                && !execution.state.is_terminal()
        }));
        drop(current);

        store.finalize_history_drain(&manifest, 4_000).unwrap();
        let restored = store.load_restored_state().unwrap();
        let restored_state = restored.model.task_run_v6_state(&survivor).unwrap();
        assert!(restored_state.history_ready);
        assert_eq!(restored_state.native_session_end, None);
        assert_eq!(
            restored_state
                .lifecycle_watermark
                .as_ref()
                .map(|watermark| watermark.source_at_ms),
            Some(3_000)
        );
    }

    #[test]
    fn provider_observation_keeps_pre_merge_survivor_upsert_unbound() {
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let sid = "binding-order-sid";
        let native_key = RunKey::Native {
            provider: Provider::Codex,
            sid: sid.to_owned(),
        };
        let mut model = DomainModel::default();
        let mut final_survivor = run_with_controller_evidence(
            survivor,
            RunKey::Controller("binding-order-controller".to_owned()),
            1,
            TaskState::Running,
        );
        final_survivor.created_at_ms = Some(1_000);
        final_survivor.updated_at_ms = Some(1_200);
        model.insert_task_run(final_survivor);
        model.insert_task_run_alias(native_key.clone(), survivor);
        model.set_task_run_v6_state(survivor, TaskRunV6State::default());
        let (mut reducer, _shared) = Reducer::new(restored(model, 3));

        let upsert = |run_id, key, ordinal, at_ms, controller_evidence, native_session| {
            let mut task_run = run(run_id, key, ordinal, TaskState::Running);
            task_run.has_controller_task_state_event = controller_evidence;
            task_run.created_at_ms = Some(at_ms);
            task_run.updated_at_ms = Some(at_ms);
            PersistOp::UpsertTaskRun(PersistTaskRun {
                task_run,
                native_session,
                created_at_ms: at_ms,
                updated_at_ms: at_ms,
                finished_at_ms: None,
            })
        };
        let operations = vec![
            upsert(
                survivor,
                RunKey::Controller("binding-order-controller".to_owned()),
                1,
                1_000,
                true,
                None,
            ),
            upsert(
                absorbed,
                native_key.clone(),
                2,
                1_100,
                false,
                Some(NativeSessionBinding {
                    provider: Provider::Codex,
                    native_session_id: sid.to_owned(),
                }),
            ),
            PersistOp::MergeTaskRuns { survivor, absorbed },
            upsert(
                survivor,
                RunKey::Controller("binding-order-controller".to_owned()),
                1,
                1_200,
                true,
                Some(NativeSessionBinding {
                    provider: Provider::Codex,
                    native_session_id: sid.to_owned(),
                }),
            ),
        ];

        let drain_id = crate::model::HistoryDrainId::new("codex:binding-order").unwrap();
        let origin = crate::model::ObservationOrigin::Historical {
            drain_id: drain_id.clone(),
            artifact_id: "binding-order.jsonl".to_owned(),
        };
        let manifest = PersistHistoryDrain {
            drain_id,
            provider: Provider::Codex,
            created_at_ms: 1_000,
            artifacts: Vec::new(),
        };
        let prior = reducer.begin_provider_observation(&origin, 1_200);
        let (batch, _receipt) =
            reducer.finish_provider_observation(prior, operations, &origin, Some(&manifest), 1_200);

        assert_eq!(batch.operations.len(), 4);
        assert!(matches!(
            &batch.operations[0],
            PersistOp::UpsertTaskRun(value) if value.task_run.run_id == survivor
        ));
        assert!(matches!(
            &batch.operations[1],
            PersistOp::UpsertTaskRun(value)
                if value.task_run.run_id == absorbed
                    && value.task_run.key == native_key
                    && value
                        .native_session
                        .as_ref()
                        .map(|binding| binding.native_session_id.as_str())
                        == Some(sid)
        ));
        assert!(matches!(
            &batch.operations[2],
            PersistOp::MergeTaskRuns {
                survivor: actual_survivor,
                absorbed: actual_absorbed,
            } if *actual_survivor == survivor && *actual_absorbed == absorbed
        ));
        assert!(matches!(
            &batch.operations[3],
            PersistOp::UpsertTaskRun(value) if value.task_run.run_id == survivor
        ));

        let survivor_upserts = batch
            .operations
            .iter()
            .filter_map(|operation| match operation {
                PersistOp::UpsertTaskRun(value) if value.task_run.run_id == survivor => Some(value),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(survivor_upserts.len(), 2);
        assert!(matches!(
            survivor_upserts[0].task_run.key,
            RunKey::Controller(ref controller) if controller == "binding-order-controller"
        ));
        assert_eq!(survivor_upserts[0].native_session, None);
        assert!(matches!(
            survivor_upserts[1].task_run.key,
            RunKey::Controller(ref controller) if controller == "binding-order-controller"
        ));
        assert_eq!(
            survivor_upserts[1]
                .native_session
                .as_ref()
                .map(|binding| binding.native_session_id.as_str()),
            Some(sid)
        );
        assert_eq!(
            batch
                .operations
                .iter()
                .filter(|operation| matches!(operation, PersistOp::MergeTaskRuns { .. }))
                .count(),
            1
        );

        assert_eq!(batch.task_runs.len(), 1);
        let final_run = batch
            .task_runs
            .iter()
            .find(|value| value.task_run.task_run.run_id == survivor)
            .unwrap();
        assert_eq!(
            final_run
                .task_run
                .native_session
                .as_ref()
                .map(|binding| binding.native_session_id.as_str()),
            Some(sid)
        );
        assert!(matches!(
            final_run.task_run.task_run.key,
            RunKey::Controller(ref controller) if controller == "binding-order-controller"
        ));
        assert!(!final_run.state.history_ready);
    }

    struct LiveSubmissionFixture {
        reducer: Reducer,
        shared: SharedModel,
        operator: tokio::sync::watch::Receiver<crate::activity::OperatorSnapshot>,
        run_id: RunId,
        untouched_private_run_id: Option<RunId>,
        agent_node_id: String,
        receipt: ProviderSubmissionReceipt,
    }

    fn live_submission_fixture(
        label: &str,
        private: bool,
        include_untouched_private_run: bool,
    ) -> LiveSubmissionFixture {
        live_submission_fixture_inner(
            label,
            private,
            include_untouched_private_run,
            #[cfg(feature = "workload-harness")]
            None,
        )
    }

    #[cfg(feature = "workload-harness")]
    fn workload_timing_live_submission_fixture(
        label: &str,
        private: bool,
        sequence: u64,
        observer: WorkloadTimingObserver,
    ) -> LiveSubmissionFixture {
        live_submission_fixture_inner(label, private, false, Some((sequence, observer)))
    }

    fn live_submission_fixture_inner(
        label: &str,
        private: bool,
        include_untouched_private_run: bool,
        #[cfg(feature = "workload-harness")] workload_timing: Option<(u64, WorkloadTimingObserver)>,
    ) -> LiveSubmissionFixture {
        let run_id = RunId::new();
        let native_session_id = format!("live-submission-{label}");
        let agent_node_id = format!("live-submission-agent-{label}");
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: native_session_id.clone(),
            },
            1,
            TaskState::Running,
        ));
        model.set_task_run_v6_state(
            run_id,
            TaskRunV6State {
                history_ready: !private,
                latest_provider_at_ms: Some(1_000),
                ..TaskRunV6State::default()
            },
        );
        model.insert_agent_node(AgentNode {
            agent_node_id: agent_node_id.clone(),
            provider: Provider::Codex,
            native_session_id: Some(native_session_id.clone()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: Some("before-submission".to_owned()),
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        });
        let untouched_private_run_id = include_untouched_private_run.then(|| {
            let untouched_run_id = RunId::new();
            model.insert_task_run(run_with_controller_evidence(
                untouched_run_id,
                RunKey::Controller(format!("live-submission-untouched-{label}")),
                3,
                TaskState::Queued,
            ));
            model.set_task_run_v6_state(
                untouched_run_id,
                TaskRunV6State {
                    history_ready: false,
                    ..TaskRunV6State::default()
                },
            );
            untouched_run_id
        });
        let (mut reducer, shared, operator) = Reducer::new_with_operator(
            restored(model, 4),
            crate::activity::RestoredOperatorState {
                activity: Vec::new(),
                terminal_times: HashMap::new(),
            },
        );
        #[cfg(feature = "workload-harness")]
        if let Some((sequence, observer)) = workload_timing {
            reducer.workload_observation_timing = Some(WorkloadObservationTiming {
                kind: WorkloadTimingKind::FallbackNotification,
                next_sequence: sequence,
                setup_observations_to_skip: 0,
                observer,
            });
        }
        let origin = crate::model::ObservationOrigin::Live;
        let prior = reducer.begin_provider_observation(&origin, 2_000);
        let mut event_metadata = metadata(&format!("live-submission-event-{label}"), 2_000);
        event_metadata.source = "provider".to_owned();
        event_metadata.source_event_type = "agent.activity".to_owned();
        event_metadata.provider = Some(Provider::Codex);
        event_metadata.native_session_id = Some(native_session_id);
        event_metadata.task_run_id = Some(run_id);
        event_metadata.agent_node_id = Some(agent_node_id.clone());
        let ApplyOutcome::Applied(operations) = reducer
            .apply(NormalizedEvent::AgentActivity {
                metadata: event_metadata,
                agent_node_id: agent_node_id.clone(),
                activity: MinimalProviderMetadata {
                    model_id: Some("after-submission".to_owned()),
                    event_kind: Some("live-submission".to_owned()),
                    ..MinimalProviderMetadata::default()
                },
            })
            .unwrap()
        else {
            panic!("the live activity must apply");
        };
        let (batch, receipt) =
            reducer.finish_provider_observation(prior, operations, &origin, None, 2_000);
        assert!(!batch.operations.is_empty());
        assert!(!batch.task_runs.is_empty());
        LiveSubmissionFixture {
            reducer,
            shared,
            operator,
            run_id,
            untouched_private_run_id,
            agent_node_id,
            receipt,
        }
    }

    #[cfg(feature = "workload-harness")]
    fn workload_timing_recorder() -> (
        Arc<Mutex<Vec<WorkloadTimingObservation>>>,
        WorkloadTimingObserver,
    ) {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer: WorkloadTimingObserver = {
            let observations = Arc::clone(&observations);
            Arc::new(move |observation| observations.lock().unwrap().push(observation))
        };
        (observations, observer)
    }

    #[cfg(feature = "workload-harness")]
    fn assert_workload_timing_tls_empty_and_reusable(sequence: u64) {
        WORKLOAD_TIMING_STATE.with(|slot| {
            assert!(slot.borrow().is_none(), "workload timing TLS must be empty");
        });
        let (observations, observer) = workload_timing_recorder();
        let _ = Reducer::new_with_operator_observed(
            restored(DomainModel::default(), 1),
            crate::activity::RestoredOperatorState {
                activity: Vec::new(),
                terminal_times: HashMap::new(),
            },
            sequence,
            observer,
        );
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].kind, WorkloadTimingKind::StartupRestore);
        assert_eq!(observations[0].sequence, sequence);
        assert_eq!(observations[0].d4_segment_count, 1);
        assert_eq!(observations[0].model_clone_publish_segment_count, 1);
        drop(observations);
        WORKLOAD_TIMING_STATE.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "workload timing TLS must be reusable"
            );
        });
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn workload_timing_live_private_success_waits_for_matching_receipt() {
        let cases = [
            ("durable", RuntimeWriteOutcome::Durable),
            (
                "committed-but-degraded",
                RuntimeWriteOutcome::CommittedButDegraded(
                    crate::store::writer::PersistenceFailure {
                        operation: crate::store::writer::PersistenceOperation::Apply,
                        phase: crate::store::writer::PersistencePhase::PostApplyCommit,
                        code: crate::store::writer::PersistenceFailureCode::Io,
                        durability: crate::store::writer::DurabilityDisposition::Committed,
                    },
                ),
            ),
        ];
        let (observations, observer) = workload_timing_recorder();

        for (index, (label, outcome)) in cases.into_iter().enumerate() {
            let sequence = u64::try_from(index).unwrap() + 40;
            let LiveSubmissionFixture {
                mut reducer,
                receipt,
                ..
            } = workload_timing_live_submission_fixture(
                label,
                true,
                sequence,
                Arc::clone(&observer),
            );

            assert_eq!(observations.lock().unwrap().len(), index, "{label}");
            let suspended = receipt
                .workload_timing
                .as_ref()
                .expect("private readiness must suspend timing on its receipt");
            assert!(suspended.0.active_started.is_none(), "{label}");
            let reducer_duration_before_wait = suspended.0.reducer_plus_publish;
            std::thread::sleep(std::time::Duration::from_millis(20));
            assert_eq!(
                suspended.0.reducer_plus_publish, reducer_duration_before_wait,
                "persistence wait must not accrue reducer duration: {label}"
            );

            reducer.complete_provider_submission(receipt, outcome);

            let observations = observations.lock().unwrap();
            assert_eq!(observations.len(), index + 1, "{label}");
            let observation = &observations[index];
            assert_eq!(observation.kind, WorkloadTimingKind::FallbackNotification);
            assert_eq!(observation.sequence, sequence);
            assert_eq!(observation.d4_segment_count, 1, "{label}");
            assert_eq!(observation.model_clone_publish_segment_count, 1, "{label}");
        }
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn workload_timing_live_private_failures_emit_nothing_and_leave_tls_reusable() {
        let cases = [
            ("not-committed", not_committed_outcome()),
            (
                "durability-unknown",
                RuntimeWriteOutcome::DurabilityUnknown(crate::store::writer::PersistenceFailure {
                    operation: crate::store::writer::PersistenceOperation::Apply,
                    phase: crate::store::writer::PersistencePhase::Acknowledgement,
                    code: crate::store::writer::PersistenceFailureCode::AcknowledgementDropped,
                    durability: crate::store::writer::DurabilityDisposition::Unknown,
                }),
            ),
            ("skipped", RuntimeWriteOutcome::Skipped),
        ];

        for (index, (label, outcome)) in cases.into_iter().enumerate() {
            let (observations, observer) = workload_timing_recorder();
            let LiveSubmissionFixture {
                mut reducer,
                receipt,
                ..
            } = workload_timing_live_submission_fixture(
                label,
                true,
                u64::try_from(index).unwrap() + 50,
                observer,
            );

            reducer.complete_provider_submission(receipt, outcome);

            assert!(observations.lock().unwrap().is_empty(), "{label}");
            assert_workload_timing_tls_empty_and_reusable(u64::try_from(index).unwrap() + 60);
        }
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn workload_timing_foreign_receipt_preserves_matching_timed_submission() {
        let (observations, observer) = workload_timing_recorder();
        let LiveSubmissionFixture {
            reducer: _foreign_reducer,
            receipt: foreign_receipt,
            ..
        } = workload_timing_live_submission_fixture(
            "timed-foreign-source",
            true,
            70,
            Arc::clone(&observer),
        );
        let LiveSubmissionFixture {
            mut reducer,
            receipt,
            ..
        } = workload_timing_live_submission_fixture("timed-foreign-target", true, 71, observer);
        let pending_submission_id = reducer
            .pending_live_provider_submission
            .as_ref()
            .map(|pending| pending.submission_id);

        reducer.complete_provider_submission(foreign_receipt, RuntimeWriteOutcome::Durable);

        assert!(observations.lock().unwrap().is_empty());
        assert_eq!(
            reducer
                .pending_live_provider_submission
                .as_ref()
                .map(|pending| pending.submission_id),
            pending_submission_id
        );

        reducer.complete_provider_submission(receipt, RuntimeWriteOutcome::Durable);

        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].sequence, 71);
        assert_eq!(observations[0].d4_segment_count, 1);
        assert_eq!(observations[0].model_clone_publish_segment_count, 1);
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn workload_timing_dropped_receipt_emits_nothing_and_leaves_tls_reusable() {
        let (observations, observer) = workload_timing_recorder();
        let LiveSubmissionFixture {
            reducer, receipt, ..
        } = workload_timing_live_submission_fixture("timed-drop", true, 80, observer);

        assert!(observations.lock().unwrap().is_empty());
        drop(receipt);

        assert!(observations.lock().unwrap().is_empty());
        assert!(
            reducer.pending_live_provider_submission.is_some(),
            "dropping the receipt does not complete the pending reducer submission"
        );
        assert_workload_timing_tls_empty_and_reusable(81);
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn workload_timing_historical_nonpublication_emits_nothing_and_leaves_tls_reusable() {
        let LiveSubmissionFixture {
            mut reducer,
            run_id,
            agent_node_id,
            receipt,
            ..
        } = live_submission_fixture("timed-historical-seed", false, false);
        reducer.complete_provider_submission(receipt, RuntimeWriteOutcome::Durable);
        let (observations, observer) = workload_timing_recorder();
        reducer.workload_observation_timing = Some(WorkloadObservationTiming {
            kind: WorkloadTimingKind::FallbackRescan,
            next_sequence: 90,
            setup_observations_to_skip: 0,
            observer,
        });
        let drain_id = crate::model::HistoryDrainId::new("codex:timed-historical").unwrap();
        let origin = crate::model::ObservationOrigin::Historical {
            drain_id: drain_id.clone(),
            artifact_id: "timed-historical.jsonl".to_owned(),
        };
        let prior = reducer.begin_provider_observation(&origin, 3_000);
        let mut event_metadata = metadata("timed-historical-event", 3_000);
        event_metadata.source = "provider".to_owned();
        event_metadata.source_event_type = "agent.activity".to_owned();
        event_metadata.provider = Some(Provider::Codex);
        event_metadata.native_session_id = Some("live-submission-timed-historical-seed".to_owned());
        event_metadata.task_run_id = Some(run_id);
        event_metadata.agent_node_id = Some(agent_node_id.clone());
        let ApplyOutcome::Applied(operations) = reducer
            .apply(NormalizedEvent::AgentActivity {
                metadata: event_metadata,
                agent_node_id,
                activity: MinimalProviderMetadata {
                    model_id: Some("historical-private-model".to_owned()),
                    event_kind: Some("historical-private-event".to_owned()),
                    ..MinimalProviderMetadata::default()
                },
            })
            .unwrap()
        else {
            panic!("historical activity must apply");
        };
        let (_batch, receipt) = reducer.finish_provider_observation(
            prior,
            operations,
            &origin,
            Some(&PersistHistoryDrain {
                drain_id,
                provider: Provider::Codex,
                created_at_ms: 2_000,
                artifacts: Vec::new(),
            }),
            3_000,
        );
        reducer.complete_provider_submission(receipt, RuntimeWriteOutcome::Durable);

        assert!(observations.lock().unwrap().is_empty());
        assert_workload_timing_tls_empty_and_reusable(91);
    }

    #[test]
    fn live_provider_submission_rejects_second_begin_before_mutation() {
        let LiveSubmissionFixture {
            mut reducer,
            receipt: _receipt,
            ..
        } = live_submission_fixture("overlap", false, false);
        let before_rate_context = reducer.rate_observation_context;
        let before_defer_publication = reducer.defer_provider_publication;
        let before_defer_model_publication = reducer.defer_provider_model_publication;
        let before_model_publication_pending = reducer.provider_model_publication_pending;
        let before_deferred_drain = reducer.deferred_provider_drain.clone();
        let before_private_runs = reducer.provider_observation_private_run_ids.clone();
        let before_pending_submission =
            reducer
                .pending_live_provider_submission
                .as_ref()
                .map(|pending| {
                    (
                        pending.submission_id,
                        pending.checkpoint.is_some(),
                        pending.ready_run_ids.clone(),
                    )
                });
        let second_origin = crate::model::ObservationOrigin::Historical {
            drain_id: crate::model::HistoryDrainId::new("codex:overlapping-begin").unwrap(),
            artifact_id: "overlapping-begin.jsonl".to_owned(),
        };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reducer.begin_provider_observation(&second_origin, 3_000);
        }));

        assert_eq!(reducer.rate_observation_context, before_rate_context);
        assert_eq!(reducer.defer_provider_publication, before_defer_publication);
        assert_eq!(
            reducer.defer_provider_model_publication,
            before_defer_model_publication
        );
        assert_eq!(
            reducer.provider_model_publication_pending,
            before_model_publication_pending
        );
        assert_eq!(reducer.deferred_provider_drain, before_deferred_drain);
        assert_eq!(
            reducer.provider_observation_private_run_ids,
            before_private_runs
        );
        assert_eq!(
            reducer
                .pending_live_provider_submission
                .as_ref()
                .map(|pending| {
                    (
                        pending.submission_id,
                        pending.checkpoint.is_some(),
                        pending.ready_run_ids.clone(),
                    )
                }),
            before_pending_submission
        );
        assert!(
            result.is_err(),
            "a second begin must reject an unresolved ordinary live submission"
        );
    }

    #[test]
    fn live_provider_submission_receipt_is_bound_across_reducers() {
        let LiveSubmissionFixture {
            receipt: foreign_receipt,
            ..
        } = live_submission_fixture("foreign-receipt-a", false, false);
        let LiveSubmissionFixture {
            mut reducer,
            shared,
            operator,
            run_id,
            agent_node_id,
            receipt,
            ..
        } = live_submission_fixture("foreign-receipt-b", true, false);
        let before_pending_submission =
            reducer
                .pending_live_provider_submission
                .as_ref()
                .map(|pending| {
                    (
                        pending.submission_id,
                        pending.checkpoint.is_some(),
                        pending.ready_run_ids.clone(),
                    )
                });
        let before_activity = operator.borrow().activity.to_vec();
        let before_model_id = reducer
            .model
            .agent_node(&agent_node_id)
            .and_then(|node| node.model_id.clone());
        let before_history_ready = reducer
            .model
            .task_run_v6_state(&run_id)
            .unwrap()
            .history_ready;
        let before_published_model_id = shared
            .borrow()
            .agent_node(&agent_node_id)
            .and_then(|node| node.model_id.clone());
        let before_publish_count = reducer.publish_count.get();

        reducer.complete_provider_submission(foreign_receipt, RuntimeWriteOutcome::Durable);

        assert_eq!(
            reducer
                .pending_live_provider_submission
                .as_ref()
                .map(|pending| {
                    (
                        pending.submission_id,
                        pending.checkpoint.is_some(),
                        pending.ready_run_ids.clone(),
                    )
                }),
            before_pending_submission,
            "a receipt from another reducer must not consume the pending slot"
        );
        assert_eq!(
            operator.borrow().activity.as_ref(),
            before_activity.as_slice(),
            "a receipt from another reducer must not complete operator state"
        );
        assert_eq!(
            reducer
                .model
                .agent_node(&agent_node_id)
                .and_then(|node| node.model_id.clone()),
            before_model_id,
            "a receipt from another reducer must not roll back or publish model state"
        );
        assert_eq!(
            reducer
                .model
                .task_run_v6_state(&run_id)
                .unwrap()
                .history_ready,
            before_history_ready,
            "a receipt from another reducer must not release private readiness"
        );
        assert_eq!(
            shared
                .borrow()
                .agent_node(&agent_node_id)
                .and_then(|node| node.model_id.clone()),
            before_published_model_id,
            "a receipt from another reducer must not publish a model snapshot"
        );
        assert_eq!(reducer.publish_count.get(), before_publish_count);

        reducer.complete_provider_submission(receipt, RuntimeWriteOutcome::Durable);

        assert!(reducer.pending_live_provider_submission.is_none());
        assert_eq!(
            operator.borrow().activity[0].durability,
            ActivityDurability::Durable
        );
        assert!(
            reducer
                .model
                .task_run_v6_state(&run_id)
                .unwrap()
                .history_ready
        );
        assert_eq!(
            shared
                .borrow()
                .agent_node(&agent_node_id)
                .and_then(|node| node.model_id.as_deref()),
            Some("after-submission")
        );
        assert_eq!(reducer.publish_count.get(), before_publish_count + 1);
    }

    #[test]
    fn live_provider_submission_receipt_releases_only_reducer_owned_ready_set() {
        let LiveSubmissionFixture {
            mut reducer,
            run_id,
            untouched_private_run_id: Some(untouched_run_id),
            receipt,
            ..
        } = live_submission_fixture("owned-ready-set", true, true)
        else {
            panic!("the fixture must include an untouched private run");
        };

        reducer.complete_provider_submission(receipt, RuntimeWriteOutcome::Durable);

        assert!(
            reducer
                .model
                .task_run_v6_state(&run_id)
                .unwrap()
                .history_ready,
            "the reducer-owned touched run must be released"
        );
        assert!(
            !reducer
                .model
                .task_run_v6_state(&untouched_run_id)
                .unwrap()
                .history_ready,
            "a receipt must not redirect readiness to an untouched run"
        );
    }

    #[test]
    fn live_provider_submission_assigns_pending_slot_to_ordinary_nonempty_batch() {
        let LiveSubmissionFixture {
            mut reducer,
            receipt,
            ..
        } = live_submission_fixture("ordinary-slot", false, false);

        assert!(
            reducer.pending_live_provider_submission.is_some(),
            "ordinary live persistence must also require completion"
        );
        reducer.complete_provider_submission(receipt, RuntimeWriteOutcome::Durable);
        assert!(reducer.pending_live_provider_submission.is_none());
    }

    #[test]
    fn ordinary_live_submission_ignores_unrelated_private_checkpoint_on_failure() {
        let cases = [
            (
                "not-committed",
                not_committed_outcome(),
                ActivityDurability::CurrentOnly,
            ),
            (
                "durability-unknown",
                RuntimeWriteOutcome::DurabilityUnknown(crate::store::writer::PersistenceFailure {
                    operation: crate::store::writer::PersistenceOperation::Apply,
                    phase: crate::store::writer::PersistencePhase::Acknowledgement,
                    code: crate::store::writer::PersistenceFailureCode::AcknowledgementDropped,
                    durability: crate::store::writer::DurabilityDisposition::Unknown,
                }),
                ActivityDurability::DurabilityUnknown,
            ),
            (
                "skipped",
                RuntimeWriteOutcome::Skipped,
                ActivityDurability::CurrentOnly,
            ),
        ];

        for (label, outcome, expected_durability) in cases {
            let LiveSubmissionFixture {
                mut reducer,
                shared,
                operator,
                run_id,
                untouched_private_run_id: Some(untouched_run_id),
                agent_node_id,
                receipt,
            } = live_submission_fixture(label, false, true)
            else {
                panic!("the fixture must include an untouched private run");
            };

            assert_eq!(
                shared
                    .borrow()
                    .agent_node(&agent_node_id)
                    .and_then(|node| node.model_id.as_deref()),
                Some("after-submission"),
                "ordinary mutations must publish before acknowledgement: {label}"
            );

            reducer.complete_provider_submission(receipt, outcome);

            assert!(
                reducer.pending_live_provider_submission.is_none(),
                "{label}"
            );
            assert_eq!(
                reducer
                    .model
                    .agent_node(&agent_node_id)
                    .and_then(|node| node.model_id.as_deref()),
                Some("after-submission"),
                "an unrelated private checkpoint must not roll back ordinary state: {label}"
            );
            assert!(
                reducer
                    .model
                    .task_run_v6_state(&run_id)
                    .unwrap()
                    .history_ready,
                "the ordinary run must remain ready: {label}"
            );
            assert!(
                !reducer
                    .model
                    .task_run_v6_state(&untouched_run_id)
                    .unwrap()
                    .history_ready,
                "the unrelated private run must remain private: {label}"
            );
            assert_eq!(
                shared
                    .borrow()
                    .agent_node(&agent_node_id)
                    .and_then(|node| node.model_id.as_deref()),
                Some("after-submission"),
                "the published ordinary value must remain current: {label}"
            );
            assert_eq!(
                operator.borrow().activity[0].durability,
                expected_durability,
                "{label}"
            );
        }
    }

    #[test]
    fn live_provider_submission_empty_batch_has_no_completion() {
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let origin = crate::model::ObservationOrigin::Live;
        let prior = reducer.begin_provider_observation(&origin, 2_000);

        let (batch, receipt) =
            reducer.finish_provider_observation(prior, Vec::new(), &origin, None, 2_000);

        assert!(batch.operations.is_empty());
        assert!(batch.task_runs.is_empty());
        assert!(batch.rate_totals.is_empty());
        assert!(batch.history_drains.is_empty());
        assert!(batch.history_associations.is_empty());
        assert!(batch.history_publications.is_empty());
        assert!(batch.history_event_drain.is_none());
        assert!(reducer.pending_live_provider_submission.is_none());
        reducer.complete_provider_submission(receipt, not_committed_outcome());
        assert!(reducer.pending_live_provider_submission.is_none());
    }

    #[test]
    fn live_provider_submission_outcome_matrix_preserves_checkpoint_semantics() {
        let cases = [
            (
                "not-committed",
                not_committed_outcome(),
                false,
                ActivityDurability::CurrentOnly,
            ),
            (
                "durability-unknown",
                RuntimeWriteOutcome::DurabilityUnknown(crate::store::writer::PersistenceFailure {
                    operation: crate::store::writer::PersistenceOperation::Apply,
                    phase: crate::store::writer::PersistencePhase::Acknowledgement,
                    code: crate::store::writer::PersistenceFailureCode::AcknowledgementDropped,
                    durability: crate::store::writer::DurabilityDisposition::Unknown,
                }),
                false,
                ActivityDurability::DurabilityUnknown,
            ),
            (
                "skipped",
                RuntimeWriteOutcome::Skipped,
                false,
                ActivityDurability::CurrentOnly,
            ),
            (
                "durable",
                RuntimeWriteOutcome::Durable,
                true,
                ActivityDurability::Durable,
            ),
            (
                "committed-but-degraded",
                RuntimeWriteOutcome::CommittedButDegraded(
                    crate::store::writer::PersistenceFailure {
                        operation: crate::store::writer::PersistenceOperation::Apply,
                        phase: crate::store::writer::PersistencePhase::PostApplyCommit,
                        code: crate::store::writer::PersistenceFailureCode::Io,
                        durability: crate::store::writer::DurabilityDisposition::Committed,
                    },
                ),
                true,
                ActivityDurability::Durable,
            ),
        ];

        for (label, outcome, committed, expected_durability) in cases {
            let LiveSubmissionFixture {
                mut reducer,
                shared,
                operator,
                run_id,
                agent_node_id,
                receipt,
                ..
            } = live_submission_fixture(label, true, false);

            reducer.complete_provider_submission(receipt, outcome);

            assert!(
                reducer.pending_live_provider_submission.is_none(),
                "{label}"
            );
            assert_eq!(
                reducer
                    .model
                    .task_run_v6_state(&run_id)
                    .unwrap()
                    .history_ready,
                committed,
                "{label}"
            );
            assert_eq!(
                reducer
                    .model
                    .agent_node(&agent_node_id)
                    .and_then(|node| node.model_id.as_deref()),
                Some(if committed {
                    "after-submission"
                } else {
                    "before-submission"
                }),
                "{label}"
            );
            assert_eq!(
                operator.borrow().activity[0].durability,
                expected_durability,
                "{label}"
            );
            if committed {
                assert_eq!(
                    shared
                        .borrow()
                        .agent_node(&agent_node_id)
                        .and_then(|node| node.model_id.as_deref()),
                    Some("after-submission"),
                    "{label}"
                );
            }
        }
    }

    #[test]
    fn failed_live_mutation_is_rolled_back_before_history_finalization() {
        let run_id = RunId::new();
        let key = RunKey::Native {
            provider: Provider::Codex,
            sid: "failed-live-finalization".to_owned(),
        };
        let agent_node_id = "failed-live-finalization-agent";
        let mut task_run = run_with_controller_evidence(run_id, key, 1, TaskState::Running);
        task_run.subject = Some("public-subject".to_owned());
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        model.set_task_run_v6_state(
            run_id,
            TaskRunV6State {
                history_ready: true,
                latest_provider_at_ms: Some(1_000),
                ..TaskRunV6State::default()
            },
        );
        model.insert_agent_node(AgentNode {
            agent_node_id: agent_node_id.to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("failed-live-finalization".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: Some("public-model".to_owned()),
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        });
        let (mut reducer, shared) = Reducer::new(restored(model, 3));
        let drain_id = crate::model::HistoryDrainId::new("codex:failed-live-finalization").unwrap();
        let historical_origin = crate::model::ObservationOrigin::Historical {
            drain_id: drain_id.clone(),
            artifact_id: "failed-live-finalization.jsonl".to_owned(),
        };
        let historical_prior = reducer.begin_provider_observation(&historical_origin, 2_000);
        let mut historical_metadata = metadata("failed-live-finalization-history", 2_000);
        historical_metadata.provider = Some(Provider::Codex);
        historical_metadata.native_session_id = Some("failed-live-finalization".to_owned());
        historical_metadata.task_run_id = Some(run_id);
        historical_metadata.agent_node_id = Some(agent_node_id.to_owned());
        let ApplyOutcome::Applied(historical_operations) = reducer
            .apply(NormalizedEvent::AgentActivity {
                metadata: historical_metadata,
                agent_node_id: agent_node_id.to_owned(),
                activity: MinimalProviderMetadata {
                    event_kind: Some("history-private-kind".to_owned()),
                    model_id: Some("history-private-model".to_owned()),
                    ..MinimalProviderMetadata::default()
                },
            })
            .unwrap()
        else {
            panic!("historical activity must apply");
        };
        let (_historical_batch, historical_receipt) = reducer.finish_provider_observation(
            historical_prior,
            historical_operations,
            &historical_origin,
            Some(&PersistHistoryDrain {
                drain_id: drain_id.clone(),
                provider: Provider::Codex,
                created_at_ms: 1_500,
                artifacts: Vec::new(),
            }),
            2_000,
        );
        reducer.complete_provider_submission(historical_receipt, RuntimeWriteOutcome::Durable);
        let pre_live_task = reducer.model.task_run(&run_id).unwrap().clone();
        let pre_live_state = reducer.model.task_run_v6_state(&run_id).unwrap().clone();
        let pre_live_agent = reducer.model.agent_node(agent_node_id).unwrap().clone();
        assert_eq!(
            shared
                .borrow()
                .agent_node(agent_node_id)
                .unwrap()
                .model_id
                .as_deref(),
            Some("public-model")
        );

        let live_origin = crate::model::ObservationOrigin::Live;
        let live_prior = reducer.begin_provider_observation(&live_origin, 3_000);
        let mut live_activity_metadata = metadata("failed-live-activity", 3_000);
        live_activity_metadata.provider = Some(Provider::Codex);
        live_activity_metadata.native_session_id = Some("failed-live-finalization".to_owned());
        live_activity_metadata.task_run_id = Some(run_id);
        live_activity_metadata.agent_node_id = Some(agent_node_id.to_owned());
        let mut live_execution_metadata = metadata("failed-live-execution", 3_000);
        live_execution_metadata.source_event_type = "execution.begin".to_owned();
        live_execution_metadata.provider = Some(Provider::Codex);
        live_execution_metadata.native_session_id = Some("failed-live-finalization".to_owned());
        live_execution_metadata.task_run_id = Some(run_id);
        let ApplyOutcome::Applied(live_operations) = reducer
            .apply_observation(vec![
                NormalizedEvent::AgentActivity {
                    metadata: live_activity_metadata,
                    agent_node_id: agent_node_id.to_owned(),
                    activity: MinimalProviderMetadata {
                        event_kind: Some("failed-live-kind".to_owned()),
                        model_id: Some("failed-live-model".to_owned()),
                        ..MinimalProviderMetadata::default()
                    },
                },
                NormalizedEvent::ExecutionBegin {
                    metadata: live_execution_metadata,
                    execution: Execution {
                        execution_id: "failed-live-execution".to_owned(),
                        pane_id: "failed-live-pane".to_owned(),
                        terminal_id: "failed-live-terminal".to_owned(),
                        task_run_id: run_id,
                        state: ExecState::Working,
                    },
                },
            ])
            .unwrap()
        else {
            panic!("live mutation must apply");
        };
        let (_live_batch, live_receipt) = reducer.finish_provider_observation(
            live_prior,
            live_operations,
            &live_origin,
            None,
            3_000,
        );
        reducer.complete_provider_submission(
            live_receipt,
            RuntimeWriteOutcome::NotCommitted(crate::store::writer::PersistenceFailure {
                operation: crate::store::writer::PersistenceOperation::Apply,
                phase: crate::store::writer::PersistencePhase::CommandExecution,
                code: crate::store::writer::PersistenceFailureCode::Sqlite,
                durability: crate::store::writer::DurabilityDisposition::NotCommitted,
            }),
        );

        let mut finalized_state = pre_live_state.clone();
        finalized_state.history_ready = true;
        assert!(
            reducer.apply_history_finalization(&crate::store::HistoryDrainFinalization {
                completed_drains: vec![drain_id.clone()],
                drain_id,
                finalized_at_ms: 4_000,
                runs: vec![crate::store::FinalizedHistoryRun {
                    run_id,
                    state: finalized_state,
                }],
            })
        );
        let published = shared.borrow();
        assert_eq!(published.task_run(&run_id), Some(&pre_live_task));
        assert_eq!(published.agent_node(agent_node_id), Some(&pre_live_agent));
        assert!(published.execution("failed-live-execution").is_none());
        assert_eq!(
            published
                .task_run_v6_state(&run_id)
                .and_then(|state| state.lifecycle_watermark.as_ref())
                .map(|watermark| watermark.source_at_ms),
            pre_live_state
                .lifecycle_watermark
                .as_ref()
                .map(|watermark| watermark.source_at_ms)
        );
    }

    #[test]
    fn failed_private_live_submission_restores_ordinal_and_event_ownership() {
        let private_run_id = RunId::new();
        let created_run_id = RunId::new();
        let later_run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            private_run_id,
            RunKey::Controller("checkpoint-private-run".to_owned()),
            1,
            TaskState::Running,
        ));
        model.set_task_run_v6_state(
            private_run_id,
            TaskRunV6State {
                history_ready: false,
                ..TaskRunV6State::default()
            },
        );
        let (mut reducer, _) = Reducer::new(restored(model, 2));
        let before_terminal_sources = reducer.terminal_event_sources.clone();
        let before_non_lane_runs = reducer.non_lane_task_state_runs.clone();

        let origin = crate::model::ObservationOrigin::Live;
        let prior = reducer.begin_provider_observation(&origin, 2_000);
        let mut terminal_metadata = metadata("checkpoint-private-terminal", 2_000);
        terminal_metadata.source = "hook".to_owned();
        terminal_metadata.source_event_type = "complete".to_owned();
        terminal_metadata.task_run_id = Some(private_run_id);
        terminal_metadata.task_state = Some(TaskState::Completed);
        let mut created_metadata = metadata("checkpoint-private-created", 2_000);
        created_metadata.source_event_type = "execution.begin".to_owned();
        created_metadata.task_run_id = Some(created_run_id);
        created_metadata.terminal_id = Some("checkpoint-created-terminal".to_owned());
        let ApplyOutcome::Applied(operations) = reducer
            .apply_observation(vec![
                NormalizedEvent::ControllerEvent {
                    metadata: terminal_metadata,
                    event: ControllerEventKind::Complete,
                },
                NormalizedEvent::ExecutionBegin {
                    metadata: created_metadata,
                    execution: Execution {
                        execution_id: "checkpoint-created-execution".to_owned(),
                        pane_id: "checkpoint-created-pane".to_owned(),
                        terminal_id: "checkpoint-created-terminal".to_owned(),
                        task_run_id: created_run_id,
                        state: ExecState::Working,
                    },
                },
            ])
            .unwrap()
        else {
            panic!("the live observation must apply before persistence");
        };
        assert_eq!(reducer.next_ordinal, 3);
        assert_eq!(
            reducer
                .terminal_event_sources
                .get(&private_run_id)
                .map(String::as_str),
            Some("hook")
        );
        assert!(reducer.non_lane_task_state_runs.contains(&private_run_id));
        let (_batch, receipt) =
            reducer.finish_provider_observation(prior, operations, &origin, None, 2_000);

        reducer.complete_provider_submission(receipt, not_committed_outcome());

        assert_eq!(reducer.next_ordinal, 2);
        assert_eq!(reducer.terminal_event_sources, before_terminal_sources);
        assert_eq!(reducer.non_lane_task_state_runs, before_non_lane_runs);
        assert!(reducer.model.task_run(&created_run_id).is_none());

        let mut later_metadata = metadata("checkpoint-later-created", 3_000);
        later_metadata.source_event_type = "execution.begin".to_owned();
        later_metadata.task_run_id = Some(later_run_id);
        later_metadata.terminal_id = Some("checkpoint-later-terminal".to_owned());
        let ApplyOutcome::Applied(_) = reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: later_metadata,
                execution: Execution {
                    execution_id: "checkpoint-later-execution".to_owned(),
                    pane_id: "checkpoint-later-pane".to_owned(),
                    terminal_id: "checkpoint-later-terminal".to_owned(),
                    task_run_id: later_run_id,
                    state: ExecState::Working,
                },
            })
            .unwrap()
        else {
            panic!("the later observation must apply");
        };
        assert_eq!(
            reducer
                .model
                .task_run(&later_run_id)
                .unwrap()
                .display_ordinal,
            DisplayOrdinal::new(2)
        );
    }

    #[test]
    fn failed_private_synthesized_submission_restores_ingest_sequence() {
        let private_run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            private_run_id,
            RunKey::Controller("checkpoint-staged-parent".to_owned()),
            1,
            TaskState::Running,
        ));
        model.set_task_run_v6_state(
            private_run_id,
            TaskRunV6State {
                history_ready: false,
                ..TaskRunV6State::default()
            },
        );
        let (mut reducer, _) = Reducer::new(RestoredState {
            model,
            next_ordinal: 2,
            next_ingest_seq: Some(7),
            event_ledger: Vec::new(),
        });
        let origin = crate::model::ObservationOrigin::Live;
        let prior = reducer.begin_provider_observation(&origin, 2_000);
        let event = controller_event(
            "checkpoint-staged-failed",
            "checkpoint-staged-child",
            ControllerEventKind::Dispatch {
                parent_task_run_id: "checkpoint-staged-parent".to_owned(),
            },
        );
        let delta = reducer.validate_controller_event(&event).unwrap();
        let operations = reducer.commit_staged_unqueued(delta).unwrap();
        assert_eq!(reducer.next_ordinal, 3);
        assert_eq!(reducer.next_ingest_seq, Some(8));
        let (_batch, receipt) =
            reducer.finish_provider_observation(prior, operations, &origin, None, 2_000);

        reducer.complete_provider_submission(receipt, not_committed_outcome());

        assert_eq!(reducer.next_ordinal, 2);
        assert_eq!(reducer.next_ingest_seq, Some(7));
        let retry = controller_event(
            "checkpoint-staged-retry",
            "checkpoint-staged-retry-child",
            ControllerEventKind::Dispatch {
                parent_task_run_id: "checkpoint-staged-parent".to_owned(),
            },
        );
        let retry_delta = reducer.validate_controller_event(&retry).unwrap();
        assert_eq!(retry_delta.post_next_ordinal, 3);
        let retry_operations = reducer.commit_staged_unqueued(retry_delta).unwrap();
        assert!(matches!(
            retry_operations.first(),
            Some(PersistOp::AdvanceIngestSequence { ingest_seq: 7 })
        ));
    }

    #[test]
    fn failed_private_live_submission_restores_pending_telemetry_and_rate_work() {
        let private_run_id = RunId::new();
        let pending_rate_run_id = RunId::new();
        let pending_telemetry_run_id = RunId::new();
        let private_key = RunKey::Controller("checkpoint-rate-private".to_owned());
        let pending_key = RunKey::Native {
            provider: Provider::Codex,
            sid: "checkpoint-pending-telemetry".to_owned(),
        };
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            private_run_id,
            private_key,
            1,
            TaskState::Running,
        ));
        model.set_task_run_v6_state(
            private_run_id,
            TaskRunV6State {
                history_ready: false,
                ..TaskRunV6State::default()
            },
        );
        model.insert_execution(execution(
            private_run_id,
            "checkpoint-rate-execution",
            ExecState::Working,
        ));
        model.insert_task_run(run(
            pending_rate_run_id,
            RunKey::Controller("checkpoint-pending-rate".to_owned()),
            2,
            TaskState::Queued,
        ));
        model
            .telemetry_entry(private_run_id, 1_000)
            .accumulate(100, None, None, None, false);
        let (mut reducer, _) = Reducer::new(restored(model, 3));
        reducer.begin_rate_epoch();
        reducer.activate_rate_epoch(1_000);
        assert!(
            reducer
                .apply_telemetry(&pending_key, 1_500, 17, None, None, None)
                .is_empty()
        );
        reducer.dirty_rate_totals.insert(pending_rate_run_id);
        reducer
            .pending_rate_observation_runs
            .insert(pending_rate_run_id);
        let before_rate_cursor = reducer.model.run_rate_cursor(&private_run_id).cloned();

        let origin = crate::model::ObservationOrigin::Live;
        let prior = reducer.begin_provider_observation(&origin, 3_000);
        assert!(
            reducer
                .apply_telemetry(
                    &RunKey::Controller("checkpoint-rate-private".to_owned()),
                    3_000,
                    20,
                    None,
                    None,
                    None,
                )
                .is_empty()
        );
        let mut private_metadata = metadata("checkpoint-rate-touch-private", 3_000);
        private_metadata.task_run_id = Some(private_run_id);
        let mut created_metadata = metadata("checkpoint-rate-create-pending", 3_000);
        created_metadata.source_event_type = "execution.begin".to_owned();
        created_metadata.provider = Some(Provider::Codex);
        created_metadata.native_session_id = Some("checkpoint-pending-telemetry".to_owned());
        created_metadata.task_run_id = Some(pending_telemetry_run_id);
        created_metadata.terminal_id = Some("checkpoint-pending-terminal".to_owned());
        let ApplyOutcome::Applied(operations) = reducer
            .apply_observation(vec![
                NormalizedEvent::AgentActivity {
                    metadata: private_metadata,
                    agent_node_id: "checkpoint-missing-agent".to_owned(),
                    activity: MinimalProviderMetadata::default(),
                },
                NormalizedEvent::ExecutionBegin {
                    metadata: created_metadata,
                    execution: Execution {
                        execution_id: "checkpoint-pending-execution".to_owned(),
                        pane_id: "checkpoint-pending-pane".to_owned(),
                        terminal_id: "checkpoint-pending-terminal".to_owned(),
                        task_run_id: pending_telemetry_run_id,
                        state: ExecState::Working,
                    },
                },
            ])
            .unwrap()
        else {
            panic!("the live telemetry observation must apply");
        };
        assert_eq!(reducer.pending_telemetry_count, 0);
        assert!(reducer.pending_telemetry.is_empty());
        assert!(reducer.pending_telemetry_order.is_empty());
        assert!(reducer.dirty_rate_totals.contains(&private_run_id));
        assert!(reducer.pending_rate_observation_runs.is_empty());
        let (_batch, receipt) =
            reducer.finish_provider_observation(prior, operations, &origin, None, 3_000);

        reducer.complete_provider_submission(receipt, not_committed_outcome());

        assert_eq!(reducer.pending_telemetry_count, 1);
        assert_eq!(
            reducer.pending_telemetry_order,
            std::collections::VecDeque::from([pending_key.clone()])
        );
        let restored_samples = reducer.pending_telemetry.get(&pending_key).unwrap();
        assert_eq!(restored_samples.len(), 1);
        assert_eq!(restored_samples[0].output_tokens, 17);
        assert_eq!(
            reducer.dirty_rate_totals,
            HashSet::from([pending_rate_run_id])
        );
        assert_eq!(
            reducer.pending_rate_observation_runs,
            HashSet::from([pending_rate_run_id])
        );
        assert_eq!(
            reducer
                .model
                .telemetry(&private_run_id)
                .unwrap()
                .output_tokens,
            100
        );
        assert_eq!(
            reducer.model.run_rate_cursor(&private_run_id),
            before_rate_cursor.as_ref()
        );
        assert!(reducer.model.task_run(&pending_telemetry_run_id).is_none());
    }

    #[test]
    fn failed_live_merge_into_ready_survivor_restores_exact_public_state() {
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let native_sid = "failed-ready-survivor";
        let mut survivor_run = run_with_controller_evidence(
            survivor,
            RunKey::Native {
                provider: Provider::Codex,
                sid: native_sid.to_owned(),
            },
            1,
            TaskState::Running,
        );
        survivor_run.subject = Some("original-public-survivor".to_owned());
        survivor_run.created_at_ms = Some(500);
        survivor_run.updated_at_ms = Some(1_000);
        let survivor_state = TaskRunV6State {
            native_session_end: Some(crate::model::NativeSessionEnd {
                status: NativeSessionEndStatus::Done,
                at_ms: 1_000,
            }),
            lifecycle_watermark: Some(NativeLifecycleWatermark {
                source_at_ms: 1_000,
                observed_at_ms: 1_000,
                source_order: "failed-ready-survivor-before".to_owned(),
            }),
            history_ready: true,
            latest_provider_at_ms: Some(1_000),
        };
        let mut absorbed_run = run(
            absorbed,
            RunKey::Provisional {
                terminal_id: "failed-ready-survivor-terminal".to_owned(),
                start_ms: 1_000,
                seq: 1,
            },
            2,
            TaskState::Queued,
        );
        absorbed_run.subject = Some("history-private-absorbed".to_owned());
        let mut model = DomainModel::default();
        model.insert_task_run(survivor_run.clone());
        model.set_task_run_v6_state(survivor, survivor_state.clone());
        model.insert_task_run(absorbed_run);
        model.set_task_run_v6_state(
            absorbed,
            TaskRunV6State {
                history_ready: false,
                latest_provider_at_ms: Some(1_000),
                ..TaskRunV6State::default()
            },
        );
        let (mut reducer, shared) = Reducer::new(restored(model, 3));

        let origin = crate::model::ObservationOrigin::Live;
        let prior = reducer.begin_provider_observation(&origin, 2_000);
        let mut live_metadata = metadata("failed-ready-survivor-live", 2_000);
        live_metadata.source_event_type = "execution.begin".to_owned();
        live_metadata.provider = Some(Provider::Codex);
        live_metadata.native_session_id = Some(native_sid.to_owned());
        live_metadata.task_run_id = Some(absorbed);
        let ApplyOutcome::Applied(operations) = reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: live_metadata,
                execution: Execution {
                    execution_id: "failed-ready-survivor-live".to_owned(),
                    pane_id: "failed-ready-survivor-pane".to_owned(),
                    terminal_id: "failed-ready-survivor-terminal".to_owned(),
                    task_run_id: absorbed,
                    state: ExecState::Working,
                },
            })
            .unwrap()
        else {
            panic!("live identity evidence must merge into the ready survivor");
        };
        let (_batch, receipt) =
            reducer.finish_provider_observation(prior, operations, &origin, None, 2_000);
        assert!(
            reducer
                .model
                .task_run_v6_state(&survivor)
                .unwrap()
                .history_ready,
            "a ready survivor must not be demoted while its live merge is held"
        );
        reducer.record_provider_identity_disagreement();
        assert_eq!(shared.borrow().task_run(&survivor), Some(&survivor_run));
        assert!(
            shared
                .borrow()
                .execution("failed-ready-survivor-live")
                .is_none(),
            "the held ready-survivor merge must not escape before acknowledgement"
        );
        reducer.complete_provider_submission(
            receipt,
            RuntimeWriteOutcome::NotCommitted(crate::store::writer::PersistenceFailure {
                operation: crate::store::writer::PersistenceOperation::Apply,
                phase: crate::store::writer::PersistencePhase::CommandExecution,
                code: crate::store::writer::PersistenceFailureCode::Sqlite,
                durability: crate::store::writer::DurabilityDisposition::NotCommitted,
            }),
        );
        reducer.record_provider_identity_disagreement();

        assert_eq!(reducer.model.task_run(&survivor), Some(&survivor_run));
        assert_eq!(
            reducer.model.task_run_v6_state(&survivor),
            Some(&survivor_state)
        );
        assert!(reducer.model.task_run(&absorbed).is_some());
        let published = shared.borrow();
        assert_eq!(published.task_run(&survivor), Some(&survivor_run));
        assert_eq!(
            published.task_run_v6_state(&survivor),
            Some(&survivor_state)
        );
        assert!(published.task_run(&absorbed).is_none());
        assert!(published.execution("failed-ready-survivor-live").is_none());
    }

    #[test]
    fn complete_history_publication_set_survives_live_holdback_and_canonicalization() {
        fn fixture() -> (Reducer, SharedModel, RunId, RunId, RunId, RunKey) {
            let canonical = RunId::new();
            let published_alias = RunId::new();
            let merge_survivor = RunId::new();
            let unrelated = RunId::new();
            let mut model = DomainModel::default();

            let mut published_canonical = run(
                canonical,
                RunKey::Provisional {
                    terminal_id: "publication-set-terminal".to_owned(),
                    start_ms: 1_000,
                    seq: 1,
                },
                1,
                TaskState::Queued,
            );
            published_canonical.subject = Some("published-canonical".to_owned());
            model.insert_task_run(published_canonical.clone());
            model.set_task_run_v6_state(canonical, TaskRunV6State::default());
            let canonical_publication = model.capture_history_publication(canonical).unwrap();
            let mut private_canonical = published_canonical;
            private_canonical.subject = Some("private-canonical".to_owned());
            model.insert_task_run(private_canonical);
            model.set_task_run_v6_state(
                canonical,
                TaskRunV6State {
                    history_ready: false,
                    latest_provider_at_ms: Some(2_000),
                    ..TaskRunV6State::default()
                },
            );
            assert!(model.install_history_publication(canonical_publication));

            let mut published_alias_run = run_with_controller_evidence(
                published_alias,
                RunKey::Controller("published-alias".to_owned()),
                2,
                TaskState::Running,
            );
            published_alias_run.subject = Some("published-alias".to_owned());
            model.insert_task_run(published_alias_run);
            model.set_task_run_v6_state(published_alias, TaskRunV6State::default());
            let mut alias_publication = model.capture_history_publication(published_alias).unwrap();
            alias_publication.canonical_run_id = canonical;
            assert!(model.install_history_publication(alias_publication));
            model.remove_task_run_record(&published_alias);

            model.insert_task_run(run(
                merge_survivor,
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "publication-set-survivor".to_owned(),
                },
                3,
                TaskState::Queued,
            ));
            model.set_task_run_v6_state(
                merge_survivor,
                TaskRunV6State {
                    history_ready: false,
                    latest_provider_at_ms: Some(2_000),
                    ..TaskRunV6State::default()
                },
            );

            let unrelated_key = RunKey::Native {
                provider: Provider::Claude,
                sid: "publication-set-unrelated".to_owned(),
            };
            model.insert_task_run(run(unrelated, unrelated_key.clone(), 4, TaskState::Queued));
            model.set_task_run_v6_state(unrelated, TaskRunV6State::default());

            let (reducer, shared) = Reducer::new(restored(model, 5));
            assert_eq!(
                shared
                    .borrow()
                    .task_run(&canonical)
                    .and_then(|run| run.subject.as_deref()),
                Some("published-canonical")
            );
            assert_eq!(
                shared
                    .borrow()
                    .task_run(&published_alias)
                    .and_then(|run| run.subject.as_deref()),
                Some("published-alias")
            );
            (
                reducer,
                shared,
                canonical,
                published_alias,
                merge_survivor,
                unrelated_key,
            )
        }

        let (mut reducer, shared, canonical, published_alias, _, unrelated_key) = fixture();
        let origin = crate::model::ObservationOrigin::Live;
        let prior = reducer.begin_provider_observation(&origin, 3_000);
        let operations = reducer.touch_run_liveness_observed(&unrelated_key, 3_000, 3_000);
        let (_batch, receipt) =
            reducer.finish_provider_observation(prior, operations, &origin, None, 3_000);
        reducer.complete_provider_submission(receipt, RuntimeWriteOutcome::Durable);
        assert!(shared.borrow().task_run(&canonical).is_some());
        assert!(shared.borrow().task_run(&published_alias).is_some());

        for (outcome, durable) in [
            (RuntimeWriteOutcome::Durable, true),
            (
                RuntimeWriteOutcome::NotCommitted(crate::store::writer::PersistenceFailure {
                    operation: crate::store::writer::PersistenceOperation::Apply,
                    phase: crate::store::writer::PersistencePhase::CommandExecution,
                    code: crate::store::writer::PersistenceFailureCode::Sqlite,
                    durability: crate::store::writer::DurabilityDisposition::NotCommitted,
                }),
                false,
            ),
        ] {
            let (mut reducer, shared, canonical, published_alias, merge_survivor, _) = fixture();
            let origin = crate::model::ObservationOrigin::Live;
            let prior = reducer.begin_provider_observation(&origin, 3_000);
            let mut live_metadata = metadata("publication-set-merge", 3_000);
            live_metadata.source_event_type = "execution.begin".to_owned();
            live_metadata.provider = Some(Provider::Codex);
            live_metadata.native_session_id = Some("publication-set-survivor".to_owned());
            live_metadata.task_run_id = Some(canonical);
            let ApplyOutcome::Applied(operations) = reducer
                .apply(NormalizedEvent::ExecutionBegin {
                    metadata: live_metadata,
                    execution: Execution {
                        execution_id: "publication-set-merge".to_owned(),
                        pane_id: "publication-set-pane".to_owned(),
                        terminal_id: "publication-set-terminal".to_owned(),
                        task_run_id: canonical,
                        state: ExecState::Working,
                    },
                })
                .unwrap()
            else {
                panic!("private canonical run must merge into its native survivor");
            };
            let (_batch, receipt) =
                reducer.finish_provider_observation(prior, operations, &origin, None, 3_000);
            assert!(shared.borrow().task_run(&canonical).is_some());
            assert!(shared.borrow().task_run(&published_alias).is_some());
            reducer.complete_provider_submission(receipt, outcome);
            reducer.record_provider_identity_disagreement();

            let published = shared.borrow();
            if durable {
                assert!(published.task_run(&canonical).is_none());
                assert!(published.task_run(&published_alias).is_none());
                assert!(
                    published
                        .task_run_v6_state(&merge_survivor)
                        .is_some_and(|state| state.history_ready)
                );
            } else {
                assert_eq!(
                    published
                        .task_run(&canonical)
                        .and_then(|run| run.subject.as_deref()),
                    Some("published-canonical")
                );
                assert_eq!(
                    published
                        .task_run(&published_alias)
                        .and_then(|run| run.subject.as_deref()),
                    Some("published-alias")
                );
                assert!(published.task_run(&merge_survivor).is_none());
            }
        }
    }

    #[test]
    fn not_committed_historical_write_leaves_published_model_and_activity_unchanged() {
        let run_id = RunId::new();
        let key = RunKey::Native {
            provider: Provider::Codex,
            sid: "not-committed-history".to_owned(),
        };
        let agent_node_id = "not-committed-agent";
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            key,
            1,
            TaskState::Running,
        ));
        model.insert_agent_node(AgentNode {
            agent_node_id: agent_node_id.to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("not-committed-history".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: Some("published-model".to_owned()),
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        });
        let (mut reducer, shared, operator) = Reducer::new_with_operator(
            restored(model, 3),
            crate::activity::RestoredOperatorState {
                activity: Vec::new(),
                terminal_times: HashMap::new(),
            },
        );
        let drain_id = crate::model::HistoryDrainId::new("codex:not-committed-history").unwrap();
        let origin = crate::model::ObservationOrigin::Historical {
            drain_id: drain_id.clone(),
            artifact_id: "not-committed.jsonl".to_owned(),
        };
        let prior = reducer.begin_provider_observation(&origin, 2_000);
        let outcome = reducer
            .apply(NormalizedEvent::AgentActivity {
                metadata: EventMetadata {
                    event_id: "not-committed-history-event".to_owned(),
                    timestamp_ms: 2_000,
                    receipt_time_ms: 2_000,
                    source: "provider".to_owned(),
                    source_event_type: "agent.activity".to_owned(),
                    herdr_session: "history-test".to_owned(),
                    workspace_id: None,
                    tab_id: None,
                    pane_id: None,
                    terminal_id: None,
                    provider: Some(Provider::Codex),
                    native_session_id: Some("not-committed-history".to_owned()),
                    task_run_id: Some(run_id),
                    agent_node_id: Some(agent_node_id.to_owned()),
                    task_state: None,
                    execution_parent: None,
                    dependency: None,
                    source_coverage: Vec::new(),
                    provider_metadata: None,
                    label: None,
                    reason: None,
                    progress: None,
                    ingest_seq: None,
                },
                agent_node_id: agent_node_id.to_owned(),
                activity: MinimalProviderMetadata {
                    event_kind: Some("historical-private-kind".to_owned()),
                    model_id: Some("historical-private-model".to_owned()),
                    ..MinimalProviderMetadata::default()
                },
            })
            .unwrap();
        let ApplyOutcome::Applied(operations) = outcome else {
            panic!("historical activity must apply");
        };
        let _batch = reducer.finish_provider_observation(
            prior,
            operations,
            &origin,
            Some(&PersistHistoryDrain {
                drain_id,
                provider: Provider::Codex,
                created_at_ms: 1_000,
                artifacts: Vec::new(),
            }),
            2_000,
        );
        reducer.complete_deferred_operator_submission(RuntimeWriteOutcome::NotCommitted(
            crate::store::writer::PersistenceFailure {
                operation: crate::store::writer::PersistenceOperation::Apply,
                phase: crate::store::writer::PersistencePhase::CommandExecution,
                code: crate::store::writer::PersistenceFailureCode::Sqlite,
                durability: crate::store::writer::DurabilityDisposition::NotCommitted,
            },
        ));

        reducer.record_provider_identity_disagreement();
        let snapshot = shared.borrow();
        let published = snapshot.agent_node(agent_node_id).unwrap();
        assert_eq!(published.model_id.as_deref(), Some("published-model"));
        assert_eq!(published.last_event_kind, None);
        assert!(operator.borrow().activity.is_empty());
    }

    #[test]
    fn i4_operator_merge_triggering_record_round_trips_canonical_lineage() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let now = super::unix_now_ms();
        let survivor_run = run(
            survivor,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "canonical-sid".to_owned(),
            },
            1,
            TaskState::Queued,
        );
        let absorbed_run = run(
            absorbed,
            RunKey::NativePath {
                provider: Provider::Codex,
                path: "/tmp/merge-trigger.jsonl".to_owned(),
            },
            2,
            TaskState::Queued,
        );
        store
            .apply_batch(vec![
                PersistOp::UpsertTaskRun(PersistTaskRun {
                    task_run: survivor_run.clone(),
                    native_session: Some(NativeSessionBinding {
                        provider: Provider::Codex,
                        native_session_id: "canonical-sid".to_owned(),
                    }),
                    created_at_ms: now,
                    updated_at_ms: now,
                    finished_at_ms: None,
                }),
                PersistOp::UpsertTaskRun(PersistTaskRun {
                    task_run: absorbed_run.clone(),
                    native_session: None,
                    created_at_ms: now,
                    updated_at_ms: now,
                    finished_at_ms: None,
                }),
            ])
            .unwrap();

        let mut model = DomainModel::default();
        model.insert_task_run(survivor_run);
        model.insert_task_run(absorbed_run);
        let (mut reducer, _shared, operator) = Reducer::new_with_operator(
            restored(model, 3),
            crate::activity::RestoredOperatorState {
                activity: Vec::new(),
                terminal_times: std::collections::HashMap::new(),
            },
        );
        let mut event_metadata = metadata("merge-triggering-record", now);
        event_metadata.source = "provider".to_owned();
        event_metadata.source_event_type = "session_resolved".to_owned();
        event_metadata.provider = Some(Provider::Codex);
        event_metadata.native_session_id = Some("canonical-sid".to_owned());
        event_metadata.task_run_id = Some(absorbed);
        let outcome = reducer
            .apply(NormalizedEvent::TopologyUpsert {
                metadata: event_metadata,
                authority: TopologyAuthority::Partial,
                entity: TopologyEntity::Workspace(Workspace {
                    workspace_id: "merge-trigger-workspace".to_owned(),
                }),
            })
            .unwrap();
        let ApplyOutcome::Applied(batch) = outcome else {
            panic!("legitimate native-path resolution must apply");
        };
        assert!(batch.iter().any(|operation| matches!(
            operation,
            PersistOp::MergeTaskRuns {
                survivor: actual_survivor,
                absorbed: actual_absorbed,
            } if *actual_survivor == survivor && *actual_absorbed == absorbed
        )));
        store.apply_batch(batch).unwrap();
        reducer.complete_operator_submission(RuntimeWriteOutcome::Durable);

        let live = operator.borrow().activity.to_vec();
        let cold = store.load_restored_operator_state().unwrap().activity;
        assert_eq!(live, cold);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].task_run_id, Some(survivor));
    }

    #[test]
    fn i4_operator_transitive_batch_lineage_normalization() {
        let first = RunId::new();
        let second = RunId::new();
        let final_survivor = RunId::new();
        let record = |event_id: &str, task_run_id: RunId| PersistOp::RecordEvent {
            event: Box::new(NormalizedEvent::TopologyUpsert {
                metadata: {
                    let mut metadata = metadata(event_id, 1);
                    metadata.task_run_id = Some(task_run_id);
                    metadata
                },
                authority: TopologyAuthority::Partial,
                entity: TopologyEntity::Workspace(Workspace {
                    workspace_id: format!("workspace-{event_id}"),
                }),
            }),
            seen_at_ms: 1,
        };
        let mut batch = vec![
            record("before-merge", first),
            PersistOp::MergeTaskRuns {
                survivor: second,
                absorbed: first,
            },
            PersistOp::MergeTaskRuns {
                survivor: final_survivor,
                absorbed: second,
            },
            record("after-chain", first),
        ];

        super::normalize_persist_batch_lineage(&mut batch);

        let recorded_lineage: Vec<_> = batch
            .iter()
            .filter_map(|operation| match operation {
                PersistOp::RecordEvent { event, .. } => super::event_metadata(event).task_run_id,
                _ => None,
            })
            .collect();
        assert_eq!(recorded_lineage, vec![first, final_survivor]);
    }

    #[test]
    fn i4_operator_d4_updates_as_gauge_not_counter() {
        let parent = RunId::new();
        let child = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            parent,
            RunKey::Controller("d4-parent".to_owned()),
            1,
            TaskState::Queued,
        ));
        model.insert_task_run(run(
            child,
            RunKey::Controller("d4-child".to_owned()),
            2,
            TaskState::Queued,
        ));
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: child,
        });
        let (mut reducer, shared, _operator) = Reducer::new_with_operator(
            restored(model, 3),
            crate::activity::RestoredOperatorState {
                activity: Vec::new(),
                terminal_times: std::collections::HashMap::new(),
            },
        );

        assert_eq!(
            crate::diagnostics::controller_counter_snapshot(&shared.borrow())
                .dangling_announcement_components,
            1
        );
        reducer
            .apply(topology_event(
                metadata("d4-unrelated-a", 10),
                "workspace-a",
            ))
            .unwrap();
        reducer
            .apply(topology_event(
                metadata("d4-unrelated-b", 11),
                "workspace-b",
            ))
            .unwrap();

        let counters = crate::diagnostics::controller_counter_snapshot(&shared.borrow());
        assert_eq!(counters.dangling_announcement_components, 1);
        assert_eq!(
            counters.dangling_announcement_components,
            shared
                .borrow()
                .controller_diagnostics()
                .dangling_announcement_components()
        );
    }

    fn metadata(event_id: &str, timestamp_ms: i64) -> EventMetadata {
        EventMetadata {
            event_id: event_id.to_owned(),
            timestamp_ms,
            receipt_time_ms: timestamp_ms,
            source: "herdr".to_owned(),
            source_event_type: "agent_status_changed".to_owned(),
            herdr_session: "session".to_owned(),
            workspace_id: None,
            tab_id: None,
            pane_id: None,
            terminal_id: None,
            provider: None,
            native_session_id: None,
            task_run_id: None,
            agent_node_id: None,
            task_state: None,
            execution_parent: None,
            dependency: None,
            source_coverage: Vec::new(),
            provider_metadata: None,
            label: None,
            reason: None,
            progress: None,
            ingest_seq: None,
        }
    }

    fn not_committed_outcome() -> RuntimeWriteOutcome {
        RuntimeWriteOutcome::NotCommitted(crate::store::writer::PersistenceFailure {
            operation: crate::store::writer::PersistenceOperation::Apply,
            phase: crate::store::writer::PersistencePhase::CommandExecution,
            code: crate::store::writer::PersistenceFailureCode::Sqlite,
            durability: crate::store::writer::DurabilityDisposition::NotCommitted,
        })
    }

    fn provider_lane_event(
        event_id: &str,
        raw_run_id: &str,
        event: ControllerEventKind,
        timestamp_ms: i64,
        receipt_time_ms: i64,
    ) -> ControllerEvent {
        let mut event = controller_event(event_id, raw_run_id, event);
        event.metadata.source = SOURCE_LOG_LANE.to_owned();
        event.metadata.timestamp_ms = timestamp_ms;
        event.metadata.receipt_time_ms = receipt_time_ms;
        event
    }

    #[test]
    fn provider_diagnostics_handle_lands_counters_in_shared_model() {
        let (reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let diagnostics = reducer.provider_diagnostics_handle();

        diagnostics.record_egress_saturation();
        diagnostics.record_coalesced_update();
        diagnostics.record_dropped_hint();
        diagnostics.record_watch_cap_fallback();
        diagnostics.record_malformed_record();

        let model = shared.borrow();
        let landed = model.provider_diagnostics();
        assert_eq!(landed.egress_saturations(), 1);
        assert_eq!(landed.coalesced_updates(), 1);
        assert_eq!(landed.dropped_hints(), 1);
        assert_eq!(landed.watch_cap_fallbacks(), 1);
        assert_eq!(landed.malformed_records(), 1);
    }

    #[test]
    fn native_lifecycle_watermark_orders_end_and_reopen_by_all_three_fields() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "watermark".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        let (mut reducer, _) = Reducer::new(restored(model, 2));
        let mut persist = Vec::new();
        reducer.apply_native_lifecycle(
            run_id,
            Some(NativeSessionEndStatus::Error),
            NativeLifecycleWatermark {
                source_at_ms: 100,
                observed_at_ms: 200,
                source_order: "b-end".to_owned(),
            },
            &mut persist,
        );
        let after_first_end = persist.len();
        reducer.apply_native_lifecycle(
            run_id,
            Some(NativeSessionEndStatus::Error),
            NativeLifecycleWatermark {
                source_at_ms: 100,
                observed_at_ms: 200,
                source_order: "b-end".to_owned(),
            },
            &mut persist,
        );
        assert_eq!(
            persist.len(),
            after_first_end,
            "same watermark/status is idempotent"
        );
        reducer.apply_native_lifecycle(
            run_id,
            None,
            NativeLifecycleWatermark {
                source_at_ms: 100,
                observed_at_ms: 200,
                source_order: "c-reopen".to_owned(),
            },
            &mut persist,
        );
        reducer.apply_native_lifecycle(
            run_id,
            Some(NativeSessionEndStatus::Done),
            NativeLifecycleWatermark {
                source_at_ms: 100,
                observed_at_ms: 199,
                source_order: "z-delayed-end".to_owned(),
            },
            &mut persist,
        );

        let state = reducer.model.task_run_v6_state(&run_id).unwrap();
        assert!(state.native_session_end.is_none());
        assert_eq!(
            state.lifecycle_watermark.as_ref().unwrap().source_order,
            "c-reopen"
        );
    }

    #[test]
    fn provider_liveness_watermark_uses_stable_versioned_native_identity() {
        let run_id = RunId::new();
        let key = RunKey::Native {
            provider: Provider::Codex,
            sid: "session-42".to_owned(),
        };
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            key.clone(),
            1,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(
            !reducer
                .touch_run_liveness_observed(&key, 500, 600)
                .is_empty()
        );
        assert_eq!(
            shared
                .borrow()
                .task_run_v6_state(&run_id)
                .and_then(|state| state.lifecycle_watermark.as_ref())
                .map(|watermark| watermark.source_order.as_str()),
            Some("provider-liveness:v1:native:codex:10:session-42")
        );
    }

    #[test]
    fn provider_lane_close_watermark_uses_stable_versioned_native_identity() {
        let run_id = RunId::new();
        let key = RunKey::Native {
            provider: Provider::Codex,
            sid: "session-42".to_owned(),
        };
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            key.clone(),
            1,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(!reducer.apply_lane_close_observed(&key, 300, 400).is_empty());
        assert_eq!(
            shared
                .borrow()
                .task_run_v6_state(&run_id)
                .and_then(|state| state.lifecycle_watermark.as_ref())
                .map(|watermark| watermark.source_order.as_str()),
            Some("provider-lane-close:v1:native:codex:10:session-42")
        );
    }

    #[test]
    fn native_root_abort_failure_and_lane_close_are_resumable_lifecycle_only() {
        for (event, expected) in [
            (
                ControllerEventKind::Cancelled,
                NativeSessionEndStatus::Cancelled,
            ),
            (ControllerEventKind::Failed, NativeSessionEndStatus::Error),
        ] {
            let sid = format!("root-{expected:?}");
            let run_id = RunId::new();
            let mut model = DomainModel::default();
            model.insert_task_run(run_with_controller_evidence(
                run_id,
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: sid.clone(),
                },
                1,
                TaskState::Running,
            ));
            model.insert_task_run_alias(RunKey::Controller(sid.clone()), run_id);
            let (reducer, _) = Reducer::new(restored(model, 2));
            let mut terminal = provider_lane_event("root-terminal", &sid, event, 100, 200);
            terminal.metadata.provider = Some(Provider::Codex);
            terminal.metadata.native_session_id = Some(sid);
            let delta = reducer.validate_controller_event(&terminal).unwrap();
            assert_eq!(
                delta.post_model.task_run(&run_id).unwrap().state,
                TaskState::Running
            );
            assert_eq!(
                delta
                    .post_model
                    .task_run_v6_state(&run_id)
                    .and_then(|state| state.native_session_end.as_ref())
                    .map(|end| end.status),
                Some(expected)
            );
        }

        let run_id = RunId::new();
        let key = RunKey::Native {
            provider: Provider::Codex,
            sid: "lane-close".to_owned(),
        };
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            key.clone(),
            1,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        assert!(!reducer.apply_lane_close_observed(&key, 300, 400).is_empty());
        let model = shared.borrow();
        assert_eq!(model.task_run(&run_id).unwrap().state, TaskState::Running);
        assert_eq!(
            model
                .task_run_v6_state(&run_id)
                .and_then(|state| state.native_session_end.as_ref())
                .map(|end| end.status),
            Some(NativeSessionEndStatus::Unknown)
        );
        drop(model);

        assert!(
            !reducer
                .touch_run_liveness_observed(&key, 500, 600)
                .is_empty()
        );
        assert!(
            shared
                .borrow()
                .task_run_v6_state(&run_id)
                .and_then(|state| state.native_session_end.as_ref())
                .is_none(),
            "newer provider liveness must clear only the native lifecycle end"
        );
    }

    fn controller_event(
        event_id: &str,
        raw_run_id: &str,
        event: ControllerEventKind,
    ) -> ControllerEvent {
        let mut metadata = metadata(event_id, 10);
        metadata.source = "controller".to_owned();
        metadata.receipt_time_ms = 20;
        ControllerEvent {
            schema_version: 1,
            task_run_id: raw_run_id.to_owned(),
            metadata,
            event,
        }
    }

    async fn commit_controller(
        reducer: &mut Reducer,
        writer: &mut WriterClient,
        event: ControllerEvent,
    ) {
        let delta = reducer.validate_controller_event(&event).unwrap();
        let permit = writer.reserve_enqueue().unwrap();
        let pending = reducer.commit_staged(delta, permit).unwrap();
        writer.finish_pending(pending).await.unwrap();
    }

    fn dangling_gauge(shared: &SharedModel) -> u64 {
        shared
            .borrow()
            .controller_diagnostics()
            .dangling_announcement_components()
    }

    #[test]
    fn controller_reserved_provider_event_id_is_invalid_at_schema_v1() {
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let event = controller_event(
            "prov:codex:node:native",
            "run",
            ControllerEventKind::TaskStarted,
        );

        assert!(matches!(
            reducer.validate_controller_event(&event),
            Err(RejectReason::Invalid)
        ));
    }

    #[test]
    fn provider_lane_run_uses_fact_time_while_event_keeps_receipt_time() {
        let fact_time_ms = 100;
        let receipt_time_ms = 10_000;
        let reducer = Reducer::new(restored(DomainModel::default(), 1)).0;
        let delta = reducer
            .validate_controller_event(&provider_lane_event(
                "fact-time-started",
                "fact-time-run",
                ControllerEventKind::TaskStarted,
                fact_time_ms,
                receipt_time_ms,
            ))
            .unwrap();

        let run = delta
            .post_model
            .task_run_by_key(&RunKey::Controller("fact-time-run".to_owned()))
            .unwrap();
        assert_eq!(run.created_at_ms, Some(fact_time_ms));
        assert_eq!(run.updated_at_ms, Some(fact_time_ms));
        assert!(delta.batch.iter().any(|operation| matches!(
            operation,
            PersistOp::RecordEvent { seen_at_ms, event }
                if *seen_at_ms == receipt_time_ms
                    && super::event_metadata(event).receipt_time_ms == receipt_time_ms
                    && super::event_metadata(event).timestamp_ms == fact_time_ms
        )));
    }

    #[test]
    fn provider_lane_zero_fact_time_is_rejected_before_run_minting() {
        let reducer = Reducer::new(restored(DomainModel::default(), 1)).0;

        assert!(matches!(
            reducer.validate_controller_event(&provider_lane_event(
                "zero-fact-time",
                "zero-fact-time-run",
                ControllerEventKind::TaskStarted,
                0,
                10_000,
            )),
            Err(RejectReason::Invalid)
        ));
        assert!(
            reducer
                .model
                .task_run_by_key(&RunKey::Controller("zero-fact-time-run".to_owned()))
                .is_none()
        );
    }

    #[test]
    fn older_provider_fact_does_not_regress_run_timestamp() {
        let reducer = Reducer::new(restored(DomainModel::default(), 1)).0;
        let started = reducer
            .validate_controller_event(&provider_lane_event(
                "monotonic-started",
                "monotonic-run",
                ControllerEventKind::TaskStarted,
                200,
                1_000,
            ))
            .unwrap();
        let run_id = started
            .post_model
            .task_run_by_key(&RunKey::Controller("monotonic-run".to_owned()))
            .unwrap()
            .run_id;
        let reducer = Reducer::new(RestoredState {
            model: started.post_model,
            next_ordinal: started.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        })
        .0;

        let progressed = reducer
            .validate_controller_event(&provider_lane_event(
                "monotonic-older-progress",
                "monotonic-run",
                ControllerEventKind::Progress,
                100,
                2_000,
            ))
            .unwrap();

        assert_eq!(
            progressed
                .post_model
                .task_run(&run_id)
                .unwrap()
                .updated_at_ms,
            Some(200)
        );
    }

    fn controller_model(raw_run_id: &str, state: TaskState) -> (DomainModel, RunId) {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Controller(raw_run_id.to_owned()),
            1,
            state,
        ));
        (model, run_id)
    }

    #[tokio::test]
    async fn reopen_only_for_lane_provenance() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));

        for (raw_run_id, source) in [
            ("hook-terminal", "hook"),
            ("lane-terminal", SOURCE_LOG_LANE),
        ] {
            let mut started = controller_event(
                &format!("{raw_run_id}-started"),
                raw_run_id,
                ControllerEventKind::TaskStarted,
            );
            started.metadata.source = source.to_owned();
            commit_controller(&mut reducer, &mut writer, started).await;
            let mut complete = controller_event(
                &format!("{raw_run_id}-complete"),
                raw_run_id,
                ControllerEventKind::Complete,
            );
            complete.metadata.source = source.to_owned();
            commit_controller(&mut reducer, &mut writer, complete).await;
        }

        let mut hook_reopen = controller_event(
            "hook-terminal-reopen",
            "hook-terminal",
            ControllerEventKind::TaskStarted,
        );
        hook_reopen.metadata.source = SOURCE_LOG_LANE.to_owned();
        hook_reopen.metadata.timestamp_ms = 21;
        assert!(matches!(
            reducer.validate_controller_event(&hook_reopen),
            Err(RejectReason::StaleEvent)
        ));

        let mut lane_reopen = controller_event(
            "lane-terminal-reopen",
            "lane-terminal",
            ControllerEventKind::TaskStarted,
        );
        lane_reopen.metadata.source = SOURCE_LOG_LANE.to_owned();
        lane_reopen.metadata.timestamp_ms = 21;
        assert!(matches!(
            reducer.validate_controller_event(&lane_reopen),
            Err(RejectReason::StaleEvent)
        ));
        lifecycle.shutdown().await.unwrap();
    }

    #[test]
    fn lane_reopen_requires_timestamp_strictly_after_finish() {
        for (case, finished_at_ms, timestamp_ms, expected_state) in [
            ("before", Some(100), 99, None),
            ("equal", Some(100), 100, None),
            ("after", Some(100), 101, None),
            ("missing-finish", None, 101, None),
        ] {
            let (mut model, run_id) = controller_model(case, TaskState::Completed);
            let mut run = model.task_run(&run_id).unwrap().clone();
            run.finished_at_ms = finished_at_ms;
            model.insert_task_run(run);
            let (mut reducer, _shared) = Reducer::new(restored(model, 2));
            reducer.restore_terminal_event_sources(HashMap::from([(
                run_id,
                SOURCE_LOG_LANE.to_owned(),
            )]));
            let mut reopen = controller_event(
                &format!("lane-reopen-{case}"),
                case,
                ControllerEventKind::TaskStarted,
            );
            reopen.metadata.source = SOURCE_LOG_LANE.to_owned();
            reopen.metadata.timestamp_ms = timestamp_ms;

            let result = reducer.validate_controller_event(&reopen);
            match expected_state {
                Some(expected_state) => assert_eq!(
                    result
                        .expect("a strictly newer lane start must reopen")
                        .post_model
                        .task_run(&run_id)
                        .unwrap()
                        .state,
                    expected_state,
                    "case {case}"
                ),
                None => assert!(
                    matches!(result, Err(RejectReason::StaleEvent)),
                    "case {case} unexpectedly reopened"
                ),
            }
        }
    }

    #[test]
    fn cancelled_lane_run_reopens_only_for_strictly_newer_lane_start() {
        for (case, timestamp_ms, expected_state) in [
            ("older", 99, None),
            ("equal", 100, None),
            ("newer", 101, None),
        ] {
            let raw_run_id = format!("cancelled-lane-{case}");
            let (mut model, run_id) = controller_model(&raw_run_id, TaskState::Cancelled);
            let mut run = model.task_run(&run_id).unwrap().clone();
            run.finished_at_ms = Some(100);
            model.insert_task_run(run);
            let (mut reducer, _shared) = Reducer::new(restored(model, 2));
            reducer.restore_terminal_event_sources(HashMap::from([(
                run_id,
                SOURCE_LOG_LANE.to_owned(),
            )]));

            let reopen = provider_lane_event(
                &format!("cancelled-lane-reopen-{case}"),
                &raw_run_id,
                ControllerEventKind::TaskStarted,
                timestamp_ms,
                1_000 + timestamp_ms,
            );
            let result = reducer.validate_controller_event(&reopen);

            match expected_state {
                Some(expected_state) => assert_eq!(
                    result
                        .expect("a strictly newer lane start must reopen a cancelled lane run")
                        .post_model
                        .task_run(&run_id)
                        .unwrap()
                        .state,
                    expected_state,
                    "case {case}"
                ),
                None => assert!(
                    matches!(result, Err(RejectReason::StaleEvent)),
                    "case {case} unexpectedly reopened"
                ),
            }
        }
    }

    #[test]
    fn failed_and_non_lane_terminal_runs_do_not_reopen() {
        for (case, terminal_state, terminal_source, start_source) in [
            (
                "failed-lane",
                TaskState::Failed,
                SOURCE_LOG_LANE,
                SOURCE_LOG_LANE,
            ),
            (
                "cancelled-hook-terminal",
                TaskState::Cancelled,
                "hook",
                SOURCE_LOG_LANE,
            ),
            (
                "cancelled-controller-start",
                TaskState::Cancelled,
                SOURCE_LOG_LANE,
                "controller",
            ),
            (
                "cancelled-manual-start",
                TaskState::Cancelled,
                SOURCE_LOG_LANE,
                "manual",
            ),
        ] {
            let (mut model, run_id) = controller_model(case, terminal_state);
            let mut run = model.task_run(&run_id).unwrap().clone();
            run.finished_at_ms = Some(100);
            model.insert_task_run(run);
            let (mut reducer, _shared) = Reducer::new(restored(model, 2));
            reducer.restore_terminal_event_sources(HashMap::from([(
                run_id,
                terminal_source.to_owned(),
            )]));
            let mut reopen = controller_event(
                &format!("terminal-reopen-{case}"),
                case,
                ControllerEventKind::TaskStarted,
            );
            reopen.metadata.source = start_source.to_owned();
            reopen.metadata.timestamp_ms = 101;
            reopen.metadata.receipt_time_ms = 1_101;

            assert!(
                matches!(
                    reducer.validate_controller_event(&reopen),
                    Err(RejectReason::StaleEvent)
                ),
                "case {case} unexpectedly reopened"
            );
        }

        let (mut model, run_id) =
            controller_model("cancelled-lane-positive-control", TaskState::Cancelled);
        let mut run = model.task_run(&run_id).unwrap().clone();
        run.finished_at_ms = Some(100);
        model.insert_task_run(run);
        let (mut reducer, _shared) = Reducer::new(restored(model, 2));
        reducer
            .restore_terminal_event_sources(HashMap::from([(run_id, SOURCE_LOG_LANE.to_owned())]));
        let reopen = provider_lane_event(
            "cancelled-lane-positive-control-reopen",
            "cancelled-lane-positive-control",
            ControllerEventKind::TaskStarted,
            101,
            1_101,
        );

        assert!(matches!(
            reducer.validate_controller_event(&reopen),
            Err(RejectReason::StaleEvent)
        ));
    }

    #[test]
    fn native_start_preserves_irreversible_semantic_terminal_state_and_identity() {
        let raw_run_id = "cancelled-lane-identity";
        let native_key = RunKey::Native {
            provider: Provider::Codex,
            sid: raw_run_id.to_owned(),
        };
        let run_id = RunId::new();
        let parent_run_id = RunId::new();
        let prerequisite_run_id = RunId::new();
        let execution_edge = ExecutionEdge {
            parent_run_id,
            child_run_id: run_id,
        };
        let dependency_edge = DependencyEdge {
            prerequisite_run_id,
            dependent_run_id: run_id,
        };
        let mut cancelled = run_with_controller_evidence(
            run_id,
            RunKey::Controller(raw_run_id.to_owned()),
            7,
            TaskState::Cancelled,
        );
        cancelled.created_at_ms = Some(10);
        cancelled.updated_at_ms = Some(100);
        cancelled.finished_at_ms = Some(100);
        cancelled.subject = Some("preserved subject".to_owned());
        cancelled.dismissed_at_ms = Some(105);
        let preserved_execution = execution(run_id, "preserved-execution", ExecState::Working);
        let preserved_node = native_agent_node("preserved-node", raw_run_id, run_id, 8);
        let mut model = DomainModel::default();
        model.insert_task_run(cancelled.clone());
        model.insert_task_run(run(
            parent_run_id,
            RunKey::Controller("preserved-parent".to_owned()),
            5,
            TaskState::Queued,
        ));
        model.insert_task_run(run(
            prerequisite_run_id,
            RunKey::Controller("preserved-prerequisite".to_owned()),
            6,
            TaskState::Queued,
        ));
        model.insert_task_run_alias(native_key.clone(), run_id);
        model.insert_execution(preserved_execution.clone());
        model.insert_agent_node(preserved_node.clone());
        model.insert_execution_edge(execution_edge.clone());
        model.insert_dependency_edge(dependency_edge.clone());
        model.set_run_kind(run_id, "codex_cli_rs".to_owned());
        let initial_run_count = model.task_runs().count();
        let initial_execution_count = model.executions().count();
        let initial_node_count = model.agent_nodes().count();
        let (mut reducer, _shared) = Reducer::new(restored(model, 9));
        reducer
            .restore_terminal_event_sources(HashMap::from([(run_id, SOURCE_LOG_LANE.to_owned())]));
        reducer.apply_telemetry(
            &native_key,
            50,
            42,
            Some("gpt-5.6-sol".to_owned()),
            Some("xhigh".to_owned()),
            Some("workspace-write".to_owned()),
        );
        let preserved_telemetry = reducer.model.telemetry(&run_id).unwrap().clone();
        let reopen = provider_lane_event(
            "cancelled-lane-identity-reopen",
            raw_run_id,
            ControllerEventKind::TaskStarted,
            101,
            1_101,
        );

        let reopened = reducer
            .validate_controller_event(&reopen)
            .expect("a newer lane start must reopen the cancelled run");
        let reopened_run = reopened.post_model.task_run(&run_id).unwrap();
        assert_eq!(reopened_run.run_id, run_id);
        assert_eq!(reopened_run.key, cancelled.key);
        assert_eq!(reopened_run.display_ordinal, cancelled.display_ordinal);
        assert_eq!(reopened_run.subject, cancelled.subject);
        assert!(reopened_run.has_controller_task_state_event);
        assert_eq!(reopened_run.state, TaskState::Cancelled);
        assert_eq!(reopened_run.finished_at_ms, Some(100));
        assert_eq!(reopened_run.dismissed_at_ms, Some(105));
        assert_eq!(reopened.post_model.task_runs().count(), initial_run_count);
        assert_eq!(
            reopened.post_model.executions().count(),
            initial_execution_count
        );
        assert_eq!(
            reopened.post_model.agent_nodes().count(),
            initial_node_count
        );
        assert_eq!(
            reopened
                .post_model
                .task_run_by_key(&native_key)
                .unwrap()
                .run_id,
            run_id
        );
        assert_eq!(
            reopened.post_model.execution("preserved-execution"),
            Some(&preserved_execution)
        );
        assert_eq!(
            reopened.post_model.agent_node("preserved-node"),
            Some(&preserved_node)
        );
        assert!(
            reopened
                .post_model
                .execution_edges()
                .any(|edge| edge == &execution_edge)
        );
        assert!(
            reopened
                .post_model
                .dependency_edges()
                .any(|edge| edge == &dependency_edge)
        );
        assert_eq!(
            reopened.post_model.telemetry(&run_id),
            Some(&preserved_telemetry)
        );
        assert_eq!(reopened.post_model.run_kind(&run_id), Some("codex_cli_rs"));
        assert_eq!(
            reopened
                .post_terminal_event_sources
                .get(&run_id)
                .map(String::as_str),
            Some(SOURCE_LOG_LANE)
        );

        let mut reducer = Reducer::new(RestoredState {
            model: reopened.post_model,
            next_ordinal: reopened.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        })
        .0;
        reducer.restore_terminal_event_sources(reopened.post_terminal_event_sources);
        let mut native_complete = provider_lane_event(
            "cancelled-lane-identity-complete",
            raw_run_id,
            ControllerEventKind::Complete,
            102,
            1_102,
        );
        native_complete.metadata.provider = Some(Provider::Codex);
        native_complete.metadata.native_session_id = Some(raw_run_id.to_owned());
        let completed = reducer
            .validate_controller_event(&native_complete)
            .expect("a later terminal event must be accepted after reopen");
        let completed_run = completed.post_model.task_run(&run_id).unwrap();
        assert_eq!(completed_run.state, TaskState::Cancelled);
        assert_eq!(completed_run.finished_at_ms, Some(100));
        assert_eq!(
            completed
                .post_model
                .task_run_v6_state(&run_id)
                .and_then(|state| state.native_session_end.as_ref())
                .map(|end| end.status),
            Some(NativeSessionEndStatus::Done)
        );
    }

    #[test]
    fn replayed_lane_terminal_restores_only_missing_provenance() {
        for (case, terminal_event) in [
            ("complete", ControllerEventKind::Complete),
            ("failed", ControllerEventKind::Failed),
            ("cancelled", ControllerEventKind::Cancelled),
        ] {
            let (mut model, run_id) = controller_model(case, TaskState::Completed);
            let mut run = model.task_run(&run_id).unwrap().clone();
            run.finished_at_ms = Some(100);
            model.insert_task_run(run);
            let (mut reducer, _shared) = Reducer::new(restored(model, 2));
            let mut replayed_terminal =
                controller_event(&format!("replayed-{case}"), case, terminal_event);
            replayed_terminal.metadata.source = SOURCE_LOG_LANE.to_owned();

            reducer.restore_replayed_controller_transients(&replayed_terminal);

            assert_eq!(
                reducer
                    .terminal_event_sources
                    .get(&run_id)
                    .map(String::as_str),
                Some(SOURCE_LOG_LANE),
                "replayed {case} did not rebuild missing lane provenance"
            );
            let mut historical_start = controller_event(
                &format!("historical-start-after-{case}"),
                case,
                ControllerEventKind::TaskStarted,
            );
            historical_start.metadata.source = SOURCE_LOG_LANE.to_owned();
            historical_start.metadata.timestamp_ms = 99;
            assert!(
                matches!(
                    reducer.validate_controller_event(&historical_start),
                    Err(RejectReason::StaleEvent)
                ),
                "rebuilt {case} provenance allowed a historical start to reopen"
            );
            let mut resumed_start = controller_event(
                &format!("resumed-start-after-{case}"),
                case,
                ControllerEventKind::TaskStarted,
            );
            resumed_start.metadata.source = SOURCE_LOG_LANE.to_owned();
            resumed_start.metadata.timestamp_ms = 101;
            assert!(
                matches!(
                    reducer.validate_controller_event(&resumed_start),
                    Err(RejectReason::StaleEvent)
                ),
                "replayed {case} unexpectedly reopened semantic terminal state"
            );
        }

        let (mut model, run_id) = controller_model("hook", TaskState::Completed);
        let mut run = model.task_run(&run_id).unwrap().clone();
        run.finished_at_ms = Some(100);
        model.insert_task_run(run);
        let (mut reducer, _shared) = Reducer::new(restored(model, 2));
        reducer.restore_terminal_event_sources(HashMap::from([(run_id, "hook".to_owned())]));
        let mut replayed_terminal = controller_event(
            "replayed-lane-complete",
            "hook",
            ControllerEventKind::Complete,
        );
        replayed_terminal.metadata.source = SOURCE_LOG_LANE.to_owned();

        reducer.restore_replayed_controller_transients(&replayed_terminal);

        assert_eq!(
            reducer
                .terminal_event_sources
                .get(&run_id)
                .map(String::as_str),
            Some("hook"),
            "log replay overwrote existing hook provenance"
        );
        let mut attempted_reopen = controller_event(
            "hook-reopen-after-lane-replay",
            "hook",
            ControllerEventKind::TaskStarted,
        );
        attempted_reopen.metadata.source = SOURCE_LOG_LANE.to_owned();
        attempted_reopen.metadata.timestamp_ms = 101;
        assert!(matches!(
            reducer.validate_controller_event(&attempted_reopen),
            Err(RejectReason::StaleEvent)
        ));
    }

    #[tokio::test]
    async fn reopen_provenance_survives_restart() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        for (event_id, kind) in [
            ("restart-lane-started", ControllerEventKind::TaskStarted),
            ("restart-lane-complete", ControllerEventKind::Complete),
        ] {
            let mut event = controller_event(event_id, "restart-lane", kind);
            event.metadata.source = SOURCE_LOG_LANE.to_owned();
            event.metadata.timestamp_ms = unix_now_ms();
            event.metadata.receipt_time_ms = event.metadata.timestamp_ms;
            commit_controller(&mut reducer, &mut writer, event).await;
        }
        lifecycle.shutdown().await.unwrap();

        let store = open_reader(&root).unwrap();
        let terminal_sources = store.terminal_event_sources().unwrap();
        let restored = store.load_restored_state().unwrap();
        let run_id = restored
            .model
            .task_run_by_key(&RunKey::Controller("restart-lane".to_owned()))
            .unwrap()
            .run_id;
        assert_eq!(
            terminal_sources.get(&run_id).map(String::as_str),
            Some(SOURCE_LOG_LANE)
        );
        let finished_at_ms = restored
            .model
            .task_run(&run_id)
            .unwrap()
            .finished_at_ms
            .unwrap();
        let (mut reducer, _shared) = Reducer::new(restored);
        reducer.restore_terminal_event_sources(terminal_sources);
        let mut reopen = controller_event(
            "restart-lane-reopen",
            "restart-lane",
            ControllerEventKind::TaskStarted,
        );
        reopen.metadata.source = SOURCE_LOG_LANE.to_owned();
        reopen.metadata.timestamp_ms = finished_at_ms + 1;

        assert!(matches!(
            reducer.validate_controller_event(&reopen),
            Err(RejectReason::StaleEvent)
        ));
    }

    #[tokio::test]
    async fn non_lane_task_state_evidence_survives_restart_and_blocks_lane_close() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut started = controller_event(
            "restart-hook-started",
            "restart-hook-managed",
            ControllerEventKind::TaskStarted,
        );
        started.metadata.source = "hook".to_owned();
        started.metadata.timestamp_ms = unix_now_ms();
        started.metadata.receipt_time_ms = started.metadata.timestamp_ms;
        commit_controller(&mut reducer, &mut writer, started).await;
        lifecycle.shutdown().await.unwrap();

        let store = open_reader(&root).unwrap();
        let non_lane_runs = store.non_lane_task_state_runs().unwrap();
        let restored = store.load_restored_state().unwrap();
        let key = RunKey::Controller("restart-hook-managed".to_owned());
        let run_id = restored.model.task_run_by_key(&key).unwrap().run_id;
        assert!(
            non_lane_runs.contains(&run_id),
            "restored non-lane runs {non_lane_runs:?} did not contain {run_id}"
        );
        let (mut reducer, shared) = Reducer::new(restored);
        reducer.restore_non_lane_task_state_runs(non_lane_runs);

        assert!(reducer.apply_lane_close(&key, 700).is_empty());
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::Running
        );
    }

    #[test]
    fn inactivity_closes_lane_created_runs() {
        let run_id = RunId::new();
        let key = RunKey::Controller("lane-created".to_owned());
        let mut lane_created =
            run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        lane_created.updated_at_ms = Some(100);
        let mut model = DomainModel::default();
        model.insert_task_run(lane_created);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        let persist = reducer.apply_lane_close(&key, 700);

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );
        assert!(matches!(
            persist.as_slice(),
            [PersistOp::UpsertTaskRun(value)]
                if value.task_run.run_id == run_id
                    && value.task_run.state == TaskState::EndedUnknown
        ));
    }

    #[tokio::test]
    async fn ended_unknown_reopens_on_append_triggered_task_started() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let run_id = RunId::new();
        let key = RunKey::Controller("lane-resume".to_owned());
        let mut lane_created =
            run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        lane_created.updated_at_ms = Some(100);
        let mut model = DomainModel::default();
        model.insert_task_run(lane_created);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(!reducer.apply_lane_close(&key, 700).is_empty());
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );

        let mut append_resume = controller_event(
            "lane-resume-append",
            "lane-resume",
            ControllerEventKind::TaskStarted,
        );
        append_resume.metadata.source = SOURCE_LOG_LANE.to_owned();
        append_resume.metadata.timestamp_ms = 800;
        append_resume.metadata.receipt_time_ms = 800;
        commit_controller(&mut reducer, &mut writer, append_resume).await;

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::Running
        );
        lifecycle.shutdown().await.unwrap();
    }

    #[test]
    fn lane_close_skips_runs_with_executions() {
        let run_id = RunId::new();
        let key = RunKey::Controller("pane-occupied".to_owned());
        let mut task_run = run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        task_run.updated_at_ms = Some(100);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run.clone());
        model.insert_execution(execution(run_id, "live-pane", ExecState::Working));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(reducer.apply_lane_close(&key, 700).is_empty());
        assert_eq!(shared.borrow().task_run(&run_id), Some(&task_run));
    }

    #[test]
    fn lane_close_allows_runs_with_only_terminal_executions() {
        let run_id = RunId::new();
        let key = RunKey::Controller("pane-ended".to_owned());
        let mut task_run = run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        task_run.updated_at_ms = Some(100);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        model.insert_execution(execution(run_id, "ended-pane", ExecState::Ended));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        let persist = reducer.apply_lane_close(&key, 700);

        assert!(!persist.is_empty());
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );
    }

    #[test]
    fn lane_close_skips_runs_with_non_lane_task_state_evidence() {
        let run_id = RunId::new();
        let key = RunKey::Controller("hook-managed".to_owned());
        let mut task_run = run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        task_run.updated_at_ms = Some(100);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let mut hook_started = metadata("hook-managed-started", 200);
        hook_started.source = "hook".to_owned();
        hook_started.source_event_type = "task_started".to_owned();
        hook_started.task_run_id = Some(run_id);
        hook_started.task_state = Some(TaskState::Running);
        reducer
            .apply(NormalizedEvent::ControllerEvent {
                metadata: hook_started,
                event: ControllerEventKind::TaskStarted,
            })
            .unwrap();

        assert!(reducer.apply_lane_close(&key, 700).is_empty());
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::Running
        );
    }

    #[test]
    fn lane_close_never_touches_terminal_or_dismissed() {
        let cases = [
            ("completed", TaskState::Completed, false),
            ("failed", TaskState::Failed, false),
            ("cancelled", TaskState::Cancelled, false),
            ("ended-unknown", TaskState::EndedUnknown, false),
            ("dismissed", TaskState::Running, true),
        ];
        let mut model = DomainModel::default();
        let mut expected = HashMap::new();
        let mut keys = Vec::new();
        for (ordinal, (name, state, dismissed)) in cases.into_iter().enumerate() {
            let run_id = RunId::new();
            let key = RunKey::Controller(name.to_owned());
            let mut task_run =
                run_with_controller_evidence(run_id, key.clone(), ordinal as i64 + 1, state);
            task_run.updated_at_ms = Some(100);
            task_run.finished_at_ms = state.is_terminal().then_some(100);
            task_run.dismissed_at_ms = dismissed.then_some(110);
            expected.insert(run_id, task_run.clone());
            keys.push(key);
            model.insert_task_run(task_run);
        }
        let (mut reducer, shared) = Reducer::new(restored(model, 6));

        for key in keys {
            assert!(reducer.apply_lane_close(&key, 700).is_empty());
        }

        let snapshot = shared.borrow();
        for (run_id, task_run) in expected {
            assert_eq!(snapshot.task_run(&run_id), Some(&task_run));
        }
    }

    #[test]
    fn liveness_touch_checks_dismissal_first() {
        let run_id = RunId::new();
        let key = RunKey::Controller("dismissed-live".to_owned());
        let mut dismissed =
            run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        dismissed.updated_at_ms = Some(100);
        dismissed.dismissed_at_ms = Some(110);
        let expected = dismissed.clone();
        let mut model = DomainModel::default();
        model.insert_task_run(dismissed);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(reducer.touch_run_liveness(&key, 200).is_empty());
        assert_eq!(shared.borrow().task_run(&run_id), Some(&expected));
    }

    #[test]
    fn liveness_touch_does_not_regress_restored_timestamp() {
        let run_id = RunId::new();
        let key = RunKey::Controller("restored-live".to_owned());
        let mut restored_run =
            run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        restored_run.updated_at_ms = Some(200);
        let mut model = DomainModel::default();
        model.insert_task_run(restored_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(reducer.touch_run_liveness(&key, 100).is_empty());
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().updated_at_ms,
            Some(200)
        );
    }

    #[test]
    fn telemetry_accumulates_deduped_output_tokens() {
        let run_id = RunId::new();
        let key = RunKey::Controller("telemetry-run".to_owned());
        let task_run = run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(
            reducer
                .apply_telemetry(
                    &key,
                    1_100,
                    17,
                    Some("claude-opus-4-1".to_owned()),
                    Some("high".to_owned()),
                    None,
                )
                .is_empty()
        );
        assert!(
            reducer
                .apply_telemetry(
                    &key,
                    1_200,
                    25,
                    Some("claude-opus-4-1".to_owned()),
                    Some("high".to_owned()),
                    None,
                )
                .is_empty()
        );

        let snapshot = shared.borrow();
        let telemetry = snapshot.telemetry(&run_id).unwrap();
        assert_eq!(telemetry.output_tokens, 42);
        assert_eq!(telemetry.started_wall_ms, 1_100);
    }

    #[test]
    fn telemetry_uses_earliest_log_time_after_late_monitor_start() {
        let run_id = RunId::new();
        let key = RunKey::Controller("late-backfill".to_owned());
        let mut task_run = run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        task_run.created_at_ms = Some(50_000);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(
            reducer
                .apply_telemetry(&key, 1_000, 17, None, None, None)
                .is_empty()
        );
        assert!(
            reducer
                .apply_telemetry(&key, 2_000, 25, None, None, None)
                .is_empty()
        );

        assert_eq!(
            shared.borrow().telemetry(&run_id).unwrap().started_wall_ms,
            1_000
        );
    }

    #[tokio::test]
    async fn telemetry_survives_backfill_replay_identically() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        apply_telemetry_fixture_events(&mut reducer, &mut writer, synthesize_telemetry_fixture())
            .await;
        let key = telemetry_fixture_key();
        let run_id = shared.borrow().task_run_by_key(&key).unwrap().run_id;
        let first = shared.borrow().telemetry(&run_id).unwrap().clone();
        assert_eq!(first.output_tokens, 42);
        assert_eq!(first.started_wall_ms, 1_100);
        assert_eq!(first.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(first.effort.as_deref(), Some("xhigh"));
        assert_eq!(
            first.per_turn.as_slice(),
            &[
                TurnAttr {
                    model: Some("gpt-5.6-terra".to_owned()),
                    effort: Some("high".to_owned()),
                    sandbox: Some("workspace-write".to_owned()),
                },
                TurnAttr {
                    model: Some("gpt-5.6-sol".to_owned()),
                    effort: Some("xhigh".to_owned()),
                    sandbox: Some("workspace-write".to_owned()),
                },
            ]
        );
        lifecycle.shutdown().await.unwrap();

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert!(
            restored.model.telemetry(&run_id).is_none(),
            "the persisted restart seed must not contain transient telemetry"
        );
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored);

        apply_telemetry_fixture_events(&mut reducer, &mut writer, synthesize_telemetry_fixture())
            .await;
        {
            let snapshot = shared.borrow();
            let second = snapshot.telemetry(&run_id).unwrap();
            assert_eq!(second, &first);
        }
        lifecycle.shutdown().await.unwrap();
    }

    const WORKER_PARENT: &str = "13f03635-c1f6-46e2-8e52-83d217b6f01c";
    const WORKER_AGENT: &str = "a7189abbf3c5741ac";
    const WORKER_MODEL: &str = "claude-sonnet-5";
    const WORKER_EFFORT: &str = "high";
    const WORKER_OUTPUT_TOKENS: u64 = 843;

    fn worker_scope(agent_id: &str) -> SessionScope {
        SessionScope::ClaudeSubagent {
            parent: WORKER_PARENT.to_owned(),
            agent_id: agent_id.to_owned(),
        }
    }

    fn worker_transcript_events(
        synthesis: &mut Synthesis,
        scope: &SessionScope,
        artifact: &Path,
        admission: &mut Admission,
        discovered: &AdmissionIndex,
    ) -> Vec<ProviderEvent> {
        let facts = include_str!("../tests/fixtures/provider-logs/claude-subagent.jsonl")
            .lines()
            .enumerate()
            .flat_map(|(ordinal, line)| {
                extract_claude_line(scope, line)
                    .into_iter()
                    .map(move |fact| (u64::try_from(ordinal).unwrap(), fact))
            })
            .collect::<Vec<_>>();
        synthesis.synthesize_batch(artifact, facts, admission, discovered)
    }

    fn worker_meta_events(
        synthesis: &mut Synthesis,
        agent_id: &str,
        artifact: &Path,
        modified_ms: i64,
        admission: &mut Admission,
        discovered: &AdmissionIndex,
    ) -> Vec<ProviderEvent> {
        let fact = extract_meta_json(
            WORKER_PARENT,
            agent_id,
            modified_ms,
            include_bytes!("../tests/fixtures/provider-logs/claude-subagent-meta.json"),
        )
        .expect("the real-shape worker metadata fixture must parse");
        synthesis.synthesize_batch(artifact, [(0, fact)], admission, discovered)
    }

    fn assert_worker_telemetry(telemetry: &crate::model::RunTelemetry) {
        assert_eq!(telemetry.model.as_deref(), Some(WORKER_MODEL));
        assert_eq!(telemetry.effort.as_deref(), Some(WORKER_EFFORT));
        assert_eq!(telemetry.output_tokens, WORKER_OUTPUT_TOKENS);
        assert_eq!(telemetry.token_breakdown.input_tokens, Some(663));
        assert_eq!(telemetry.token_breakdown.cached_input_tokens, Some(224));
        assert_eq!(telemetry.token_breakdown.cache_write_input_tokens, Some(96));
    }

    async fn apply_worker_transcript_then_meta(
        reducer: &mut Reducer,
        writer: &mut WriterClient,
        synthesis: &mut Synthesis,
        scope: &SessionScope,
        admission: &mut Admission,
        discovered: &AdmissionIndex,
    ) {
        let SessionScope::ClaudeSubagent { agent_id, .. } = scope else {
            panic!("worker fixture scope must be a Claude lineage child");
        };
        let transcript = std::path::PathBuf::from(format!("agent-{agent_id}.jsonl"));
        let transcript_events =
            worker_transcript_events(synthesis, scope, &transcript, admission, discovered);
        apply_telemetry_fixture_events(reducer, writer, transcript_events).await;
        let meta = std::path::PathBuf::from(format!("agent-{agent_id}.meta.json"));
        let meta_events = worker_meta_events(
            synthesis,
            agent_id,
            &meta,
            1_800_000_000_000,
            admission,
            discovered,
        );
        apply_telemetry_fixture_events(reducer, writer, meta_events).await;
    }

    #[tokio::test]
    async fn real_shape_worker_usage_preceding_meta_is_attributed() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let scope = worker_scope(WORKER_AGENT);
        let mut synthesis = Synthesis::default();
        let mut admission = Admission::new(0);
        let discovered = AdmissionIndex::new();

        apply_worker_transcript_then_meta(
            &mut reducer,
            &mut writer,
            &mut synthesis,
            &scope,
            &mut admission,
            &discovered,
        )
        .await;

        let snapshot = shared.borrow();
        let run = snapshot
            .task_run_by_key(&run_key_for_scope(&scope))
            .expect("worker metadata must mint the lineage-child run");
        let telemetry = snapshot
            .telemetry(&run.run_id)
            .expect("usage preceding worker metadata must be attributed");
        assert_worker_telemetry(telemetry);
        drop(snapshot);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn real_shape_worker_restart_rebackfill_is_identical() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let scope = worker_scope(WORKER_AGENT);
        let key = run_key_for_scope(&scope);

        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut synthesis = Synthesis::default();
        let mut admission = Admission::new(0);
        apply_worker_transcript_then_meta(
            &mut reducer,
            &mut writer,
            &mut synthesis,
            &scope,
            &mut admission,
            &AdmissionIndex::new(),
        )
        .await;
        let run_id = shared.borrow().task_run_by_key(&key).unwrap().run_id;
        let first = shared.borrow().telemetry(&run_id).cloned();
        lifecycle.shutdown().await.unwrap();

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert!(restored.model.telemetry(&run_id).is_none());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored);
        let mut synthesis = Synthesis::default();
        let mut admission = Admission::new(0);
        apply_worker_transcript_then_meta(
            &mut reducer,
            &mut writer,
            &mut synthesis,
            &scope,
            &mut admission,
            &AdmissionIndex::new(),
        )
        .await;
        let second = shared.borrow().telemetry(&run_id).cloned();

        assert_eq!(second, first, "restart replay must reproduce exact totals");
        assert_worker_telemetry(first.as_ref().expect("first pass telemetry must exist"));
        assert_worker_telemetry(second.as_ref().expect("restart telemetry must exist"));
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn late_worker_meta_cycle_applies_retained_usage() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let scope = worker_scope(WORKER_AGENT);
        let key = run_key_for_scope(&scope);
        let mut synthesis = Synthesis::default();
        let mut admission = Admission::new(0);
        let discovered = AdmissionIndex::new();

        let transcript_events = worker_transcript_events(
            &mut synthesis,
            &scope,
            Path::new("agent-a7189abbf3c5741ac.jsonl"),
            &mut admission,
            &discovered,
        );
        apply_telemetry_fixture_events(&mut reducer, &mut writer, transcript_events).await;
        assert!(
            shared.borrow().task_run_by_key(&key).is_none(),
            "the transcript-only cycle must precede run-minting metadata"
        );

        let meta_events = worker_meta_events(
            &mut synthesis,
            WORKER_AGENT,
            Path::new("agent-a7189abbf3c5741ac.meta.json"),
            1_800_000_000_000,
            &mut admission,
            &discovered,
        );
        apply_telemetry_fixture_events(&mut reducer, &mut writer, meta_events).await;

        let snapshot = shared.borrow();
        let run_id = snapshot.task_run_by_key(&key).unwrap().run_id;
        assert_worker_telemetry(
            snapshot
                .telemetry(&run_id)
                .expect("a later metadata cycle must apply retained usage"),
        );
        drop(snapshot);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn out_of_window_lineage_child_stays_unattributed() {
        const ANCHOR_MS: i64 = 1_800_000_000_000;
        const FRESH_AGENT: &str = "feedfacefeedface";
        const STALE_AGENT: &str = "deadbeefdeadbeef";
        let directory = tempfile::tempdir().unwrap();
        let state_root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&state_root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let artifact_root = directory.path().join(WORKER_PARENT).join("subagents");
        let fresh_transcript = artifact_root.join(format!("agent-{FRESH_AGENT}.jsonl"));
        let fresh_meta = artifact_root.join(format!("agent-{FRESH_AGENT}.meta.json"));
        let stale_transcript = artifact_root.join(format!("agent-{STALE_AGENT}.jsonl"));
        let stale_meta = artifact_root.join(format!("agent-{STALE_AGENT}.meta.json"));
        let mut discovered = AdmissionIndex::new();
        discovered.insert_claude_subagent(
            WORKER_PARENT,
            FRESH_AGENT,
            fresh_transcript.clone(),
            ANCHOR_MS,
        );
        discovered.insert_claude_subagent(
            WORKER_PARENT,
            FRESH_AGENT,
            fresh_meta.clone(),
            ANCHOR_MS,
        );
        discovered.insert_claude_subagent(
            WORKER_PARENT,
            STALE_AGENT,
            stale_transcript.clone(),
            ANCHOR_MS - 1,
        );
        discovered.insert_claude_subagent(
            WORKER_PARENT,
            STALE_AGENT,
            stale_meta.clone(),
            ANCHOR_MS,
        );
        let parent = SessionScope::ClaudeRoot(WORKER_PARENT.to_owned());
        let mut admission = Admission::new(ANCHOR_MS);
        admission.admit_pane_session(Provider::Claude, WORKER_PARENT);
        assert!(
            admission
                .on_evidence(
                    &parent,
                    &EvidenceId::Uuid(FRESH_AGENT.to_owned()),
                    &discovered,
                )
                .is_some()
        );
        assert!(
            admission
                .on_evidence(
                    &parent,
                    &EvidenceId::Uuid(STALE_AGENT.to_owned()),
                    &discovered,
                )
                .is_some()
        );
        assert!(admission.is_admitted_file(&fresh_transcript, ANCHOR_MS));
        assert!(
            !admission.is_admitted_file(&stale_transcript, ANCHOR_MS - 1),
            "the out-of-window artifact must be a lineage child, not an anchor-exempt pane root"
        );

        let mut synthesis = Synthesis::default();
        let fresh_scope = worker_scope(FRESH_AGENT);
        let fresh_events = worker_transcript_events(
            &mut synthesis,
            &fresh_scope,
            &fresh_transcript,
            &mut admission,
            &discovered,
        );
        apply_telemetry_fixture_events(&mut reducer, &mut writer, fresh_events).await;
        for (agent_id, meta) in [(FRESH_AGENT, &fresh_meta), (STALE_AGENT, &stale_meta)] {
            let events = worker_meta_events(
                &mut synthesis,
                agent_id,
                meta,
                ANCHOR_MS,
                &mut admission,
                &discovered,
            );
            apply_telemetry_fixture_events(&mut reducer, &mut writer, events).await;
        }

        let snapshot = shared.borrow();
        let fresh_run = snapshot
            .task_run_by_key(&run_key_for_scope(&fresh_scope))
            .unwrap();
        assert_worker_telemetry(
            snapshot
                .telemetry(&fresh_run.run_id)
                .expect("the in-window lineage child is the positive control"),
        );
        let stale_scope = worker_scope(STALE_AGENT);
        let stale_run = snapshot
            .task_run_by_key(&run_key_for_scope(&stale_scope))
            .unwrap();
        assert!(
            snapshot.telemetry(&stale_run.run_id).is_none(),
            "the out-of-window lineage child must keep the metrics placeholder"
        );
        drop(snapshot);
        lifecycle.shutdown().await.unwrap();
    }

    #[test]
    fn pending_telemetry_fifo_evicts_oldest_sample_at_cap() {
        const PENDING_CAP: usize = super::PENDING_TELEMETRY_SAMPLE_CAPACITY;
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        for index in 0..=PENDING_CAP {
            let key = RunKey::Controller(format!("pending-{index}"));
            assert!(
                reducer
                    .apply_telemetry(
                        &key,
                        i64::try_from(index).unwrap(),
                        u64::try_from(index + 1).unwrap(),
                        Some(WORKER_MODEL.to_owned()),
                        Some(WORKER_EFFORT.to_owned()),
                        None,
                    )
                    .is_empty()
            );
        }

        let oldest_key = RunKey::Controller("pending-0".to_owned());
        let second_oldest_key = RunKey::Controller("pending-1".to_owned());
        let newest_key = RunKey::Controller(format!("pending-{PENDING_CAP}"));
        let oldest_id = RunId::new();
        let second_oldest_id = RunId::new();
        let newest_id = RunId::new();
        reducer.model.insert_task_run(run_with_controller_evidence(
            oldest_id,
            oldest_key,
            1,
            TaskState::Running,
        ));
        reducer.model.insert_task_run(run_with_controller_evidence(
            second_oldest_id,
            second_oldest_key,
            2,
            TaskState::Running,
        ));
        reducer.model.insert_task_run(run_with_controller_evidence(
            newest_id,
            newest_key,
            3,
            TaskState::Running,
        ));
        reducer.publish();

        let snapshot = shared.borrow();
        assert!(
            snapshot.telemetry(&oldest_id).is_none(),
            "the global FIFO cap must evict the oldest pending sample"
        );
        let second_oldest = snapshot
            .telemetry(&second_oldest_id)
            .expect("the second-oldest pending sample must survive FIFO eviction");
        assert_eq!(second_oldest.output_tokens, 2);
        let newest = snapshot
            .telemetry(&newest_id)
            .expect("the newest pending sample must survive FIFO eviction");
        // Hard-coded on purpose as the exact-capacity pin; deriving this from
        // PENDING_CAP would defeat the pin.
        assert_eq!(newest.output_tokens, 4_097);
    }

    #[test]
    fn pending_telemetry_retains_multiple_samples_for_one_scope() {
        let key = RunKey::Controller("pending-multiple".to_owned());
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        for (at_ms, output_tokens) in [(1_000, 11), (1_100, 13), (1_200, 17)] {
            assert!(
                reducer
                    .apply_telemetry(
                        &key,
                        at_ms,
                        output_tokens,
                        Some(WORKER_MODEL.to_owned()),
                        Some(WORKER_EFFORT.to_owned()),
                        None,
                    )
                    .is_empty()
            );
        }
        assert_eq!(reducer.pending_telemetry_count, 3);
        assert_eq!(reducer.pending_telemetry.get(&key).unwrap().len(), 3);

        let run_id = RunId::new();
        reducer.model.insert_task_run(run_with_controller_evidence(
            run_id,
            key,
            1,
            TaskState::Running,
        ));
        reducer.publish();
        assert_eq!(reducer.pending_telemetry_count, 0);
        assert!(reducer.pending_telemetry.is_empty());
        assert!(reducer.pending_telemetry_order.is_empty());

        let snapshot = shared.borrow();
        let telemetry = snapshot
            .telemetry(&run_id)
            .expect("all pending samples for one scope must be attributed");
        assert_eq!(telemetry.output_tokens, 41);
        assert_eq!(telemetry.started_wall_ms, 1_000);
    }

    #[test]
    fn pending_telemetry_resolves_through_promotion_rekey() {
        let native_key = RunKey::Native {
            provider: Provider::Codex,
            sid: "pending-promotion".to_owned(),
        };
        let provisional_key = RunKey::Provisional {
            terminal_id: "pending-promotion-terminal".to_owned(),
            start_ms: 1_000,
            seq: 1,
        };
        let run_id = RunId::new();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        assert!(
            reducer
                .apply_telemetry(
                    &native_key,
                    1_100,
                    37,
                    Some("gpt-5.6-sol".to_owned()),
                    Some("xhigh".to_owned()),
                    Some("workspace-write".to_owned()),
                )
                .is_empty()
        );
        reducer
            .model
            .insert_task_run(run(run_id, provisional_key.clone(), 1, TaskState::Running));
        reducer.publish();
        assert!(
            shared.borrow().telemetry(&run_id).is_none(),
            "a different provisional key must not consume native-key telemetry"
        );
        assert_eq!(reducer.pending_telemetry_count, 1);

        let mut promoted = reducer.model.task_run(&run_id).unwrap().clone();
        promoted.key = native_key;
        reducer.model.insert_task_run(promoted);
        reducer.model.insert_task_run_alias(provisional_key, run_id);
        reducer.publish();

        let snapshot = shared.borrow();
        let telemetry = snapshot
            .telemetry(&run_id)
            .expect("promotion must resolve pending native-key telemetry");
        assert_eq!(telemetry.output_tokens, 37);
        assert_eq!(telemetry.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(telemetry.per_turn.len(), 1);
    }

    #[test]
    fn pending_telemetry_resolves_through_merge_alias() {
        let absorbed_key = RunKey::Controller("pending-absorbed".to_owned());
        let survivor_key = RunKey::Controller("pending-survivor".to_owned());
        let absorbed_id = RunId::new();
        let survivor_id = RunId::new();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        assert!(
            reducer
                .apply_telemetry(
                    &absorbed_key,
                    1_100,
                    53,
                    Some(WORKER_MODEL.to_owned()),
                    Some(WORKER_EFFORT.to_owned()),
                    None,
                )
                .is_empty()
        );
        reducer
            .model
            .insert_task_run(run(survivor_id, survivor_key, 1, TaskState::Running));
        reducer.model.insert_task_run(run(
            absorbed_id,
            absorbed_key.clone(),
            2,
            TaskState::Running,
        ));
        reducer.model.remove_task_run_record(&absorbed_id);
        reducer
            .model
            .insert_task_run_alias(absorbed_key, survivor_id);
        reducer.publish();

        let snapshot = shared.borrow();
        let telemetry = snapshot
            .telemetry(&survivor_id)
            .expect("merge alias must route absorbed-key telemetry to the survivor");
        assert_eq!(telemetry.output_tokens, 53);
    }

    #[test]
    fn telemetry_is_not_persisted() {
        let run_id = RunId::new();
        let key = RunKey::Controller("transient-telemetry".to_owned());
        let task_run = run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        let persist = reducer.apply_telemetry(
            &key,
            4_000,
            9_876_543_210,
            Some("distinctive-transient-model".to_owned()),
            Some("distinctive-transient-effort".to_owned()),
            None,
        );
        assert!(persist.is_empty());
        let snapshot = shared.borrow();
        assert_eq!(
            snapshot.telemetry(&run_id).unwrap().output_tokens,
            9_876_543_210
        );
        assert_eq!(snapshot.telemetry(&run_id).unwrap().started_wall_ms, 4_000);

        // RunTelemetry has no serde implementation, which compile-enforces the transient
        // invariant. TaskRun is the serializable projection that reaches persistence, so the
        // real persistence surface is still checked alongside the empty PersistOp batch.
        let serialized = serde_json::to_string(snapshot.task_run(&run_id).unwrap()).unwrap();
        assert!(!serialized.contains("\"telemetry\":"));
        assert!(!serialized.contains("\"output_tokens\":"));
        assert!(!serialized.contains("9876543210"));
        assert!(!serialized.contains("distinctive-transient-model"));
        assert!(!serialized.contains("distinctive-transient-effort"));
    }

    #[test]
    fn per_turn_model_effort_latest_wins_for_display() {
        let run_id = RunId::new();
        let key = RunKey::Native {
            provider: Provider::Codex,
            sid: "per-turn-rollout".to_owned(),
        };
        let task_run = run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        for (at_ms, tokens, model, effort) in [
            (2_000, 10, "gpt-5.6-terra", "high"),
            (2_100, 20, "gpt-5.6-terra", "high"),
            (2_200, 30, "gpt-5.6-sol", "xhigh"),
        ] {
            assert!(
                reducer
                    .apply_telemetry(
                        &key,
                        at_ms,
                        tokens,
                        Some(model.to_owned()),
                        Some(effort.to_owned()),
                        Some("workspace-write".to_owned()),
                    )
                    .is_empty()
            );
        }

        let snapshot = shared.borrow();
        let telemetry = snapshot.telemetry(&run_id).unwrap();
        assert_eq!(telemetry.output_tokens, 60);
        assert_eq!(telemetry.started_wall_ms, 2_000);
        assert_eq!(telemetry.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(telemetry.effort.as_deref(), Some("xhigh"));
        assert_eq!(
            telemetry.per_turn,
            vec![
                TurnAttr {
                    model: Some("gpt-5.6-terra".to_owned()),
                    effort: Some("high".to_owned()),
                    sandbox: Some("workspace-write".to_owned()),
                },
                TurnAttr {
                    model: Some("gpt-5.6-sol".to_owned()),
                    effort: Some("xhigh".to_owned()),
                    sandbox: Some("workspace-write".to_owned()),
                },
            ],
            "consecutive identical attributions are one observed turn context"
        );
    }

    fn telemetry_fixture_key() -> RunKey {
        RunKey::Native {
            provider: Provider::Codex,
            sid: "telemetry-replay-rollout".to_owned(),
        }
    }

    fn synthesize_telemetry_fixture() -> Vec<ProviderEvent> {
        let rollout_id = "telemetry-replay-rollout";
        let scope = SessionScope::Codex {
            rollout_id: rollout_id.to_owned(),
        };
        let facts = [
            (
                0,
                LogFact::Append {
                    scope: scope.clone(),
                    at_ms: 1_000,
                },
            ),
            (
                1,
                LogFact::CodexMeta {
                    rollout_id: rollout_id.to_owned(),
                    cwd: "/workspace".to_owned(),
                    originator: "codex_cli_rs".to_owned(),
                    internal: None,
                    cli_version: "0.1.0".to_owned(),
                },
            ),
            (
                2,
                LogFact::CodexTurn {
                    rollout_id: rollout_id.to_owned(),
                    turn_id: "turn-1".to_owned(),
                    model: "gpt-5.6-terra".to_owned(),
                    effort: Some("high".to_owned()),
                    sandbox: Some("workspace-write".to_owned()),
                },
            ),
            (
                3,
                LogFact::Usage {
                    scope: scope.clone(),
                    at_ms: 1_100,
                    sample_id: "turn-1:sample".to_owned(),
                    output_tokens: 17,
                    token_breakdown: crate::model::TokenBreakdown::default(),
                    model: None,
                    effort: None,
                },
            ),
            (
                4,
                // The repeated (scope, sample_id) is deliberately poisoned: lane deduplication
                // excludes 999, so the fixture total proves 42 = 17 + 25.
                LogFact::Usage {
                    scope: scope.clone(),
                    at_ms: 1_101,
                    sample_id: "turn-1:sample".to_owned(),
                    output_tokens: 999,
                    token_breakdown: crate::model::TokenBreakdown::default(),
                    model: None,
                    effort: None,
                },
            ),
            (
                5,
                LogFact::CodexTurn {
                    rollout_id: rollout_id.to_owned(),
                    turn_id: "turn-2".to_owned(),
                    model: "gpt-5.6-sol".to_owned(),
                    effort: Some("xhigh".to_owned()),
                    sandbox: Some("workspace-write".to_owned()),
                },
            ),
            (
                6,
                LogFact::Usage {
                    scope,
                    at_ms: 1_200,
                    sample_id: "turn-2:sample".to_owned(),
                    output_tokens: 25,
                    token_breakdown: crate::model::TokenBreakdown::default(),
                    model: None,
                    effort: None,
                },
            ),
        ];
        Synthesis::default().synthesize_batch(
            Path::new("rollout-telemetry-replay.jsonl"),
            facts,
            &mut Admission::new(0),
            &AdmissionIndex::new(),
        )
    }

    async fn apply_telemetry_fixture_events(
        reducer: &mut Reducer,
        writer: &mut WriterClient,
        events: Vec<ProviderEvent>,
    ) {
        for event in events {
            match event {
                ProviderEvent::Synthesized(event) => {
                    if !writer.is_duplicate(&event.metadata.event_id) {
                        commit_controller(reducer, writer, event).await;
                    }
                }
                ProviderEvent::RunLiveness { key, at_ms } => {
                    let persist = reducer.touch_run_liveness(&key, at_ms);
                    if !persist.is_empty() {
                        writer.apply(persist).await.unwrap();
                    }
                }
                ProviderEvent::Telemetry {
                    key,
                    at_ms,
                    output_tokens,
                    token_breakdown,
                    model,
                    effort,
                    sandbox,
                } => {
                    assert!(
                        reducer
                            .apply_telemetry_with_breakdown(
                                &key,
                                at_ms,
                                output_tokens,
                                token_breakdown,
                                crate::model::TurnAttr {
                                    model,
                                    effort,
                                    sandbox,
                                },
                            )
                            .is_empty()
                    );
                }
                other => panic!("unexpected telemetry fixture event: {other:?}"),
            }
        }
    }

    #[test]
    fn root_liveness_defers_hook_only_expiry() {
        let run_id = RunId::new();
        let key = RunKey::Controller("hook-root".to_owned());
        let mut root = run_with_controller_evidence(run_id, key.clone(), 1, TaskState::Running);
        root.updated_at_ms = Some(100);
        let mut model = DomainModel::default();
        model.insert_task_run(root);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let old_expiry = 100 + crate::activity::HOOK_ONLY_STALE_VISIBILITY_MS;

        assert_eq!(reducer.touch_run_liveness(&key, old_expiry - 1).len(), 1);
        assert!(
            reducer
                .apply_operator_command(OperatorCommand::DismissClearable, old_expiry)
                .is_empty()
        );
        let snapshot = shared.borrow();
        let root = snapshot.task_run(&run_id).unwrap();
        assert_eq!(root.updated_at_ms, Some(old_expiry - 1));
        assert_eq!(root.dismissed_at_ms, None);
    }

    #[test]
    fn dismiss_known_run_sets_receipt_time_without_transition_or_touch() {
        let run_id = RunId::new();
        let mut task_run = run_with_controller_evidence(
            run_id,
            RunKey::Controller("run".to_owned()),
            1,
            TaskState::Running,
        );
        task_run.created_at_ms = Some(5);
        task_run.updated_at_ms = Some(7);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (reducer, _) = Reducer::new(restored(model, 2));

        let delta = reducer
            .validate_controller_event(&controller_event(
                "dismiss-known",
                "run",
                ControllerEventKind::Dismiss,
            ))
            .unwrap();

        let dismissed = delta.post_model.task_run(&run_id).unwrap();
        assert_eq!(dismissed.dismissed_at_ms, Some(20));
        assert_eq!(dismissed.state, TaskState::Running);
        assert_eq!(dismissed.updated_at_ms, Some(7));
        assert!(delta.batch.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTaskRun(value)
                if value.task_run.run_id == run_id
                    && value.task_run.dismissed_at_ms == Some(20)
                    && value.task_run.updated_at_ms == Some(7)
                    && value.updated_at_ms == 7
        )));
        let recorded = delta
            .batch
            .iter()
            .find_map(|operation| match operation {
                PersistOp::RecordEvent { event, .. } => Some(super::event_metadata(event)),
                _ => None,
            })
            .unwrap();
        assert_eq!(recorded.task_run_id, Some(run_id));
    }

    #[test]
    fn dismiss_unknown_run_is_true_noop_without_placeholder() {
        let model = DomainModel::default();
        let original_count = model.task_runs().count();
        let (reducer, _) = Reducer::new(restored(model, 1));

        let delta = reducer
            .validate_controller_event(&controller_event(
                "dismiss-unknown",
                "unknown",
                ControllerEventKind::Dismiss,
            ))
            .unwrap();

        assert_eq!(delta.post_model.task_runs().count(), original_count);
        assert!(
            delta
                .post_model
                .task_run_by_key(&RunKey::Controller("unknown".to_owned()))
                .is_none()
        );
        assert!(
            delta
                .batch
                .iter()
                .all(|operation| !matches!(operation, PersistOp::UpsertTaskRun(_)))
        );
        let recorded = delta
            .batch
            .iter()
            .find_map(|operation| match operation {
                PersistOp::RecordEvent { event, .. } => Some(super::event_metadata(event)),
                _ => None,
            })
            .unwrap();
        assert_eq!(recorded.task_run_id, None);
    }

    #[test]
    fn lane_terminal_events_for_unknown_runs_are_dropped_without_ledger_or_placeholders() {
        for (event_slug, event_kind) in [
            ("complete", ControllerEventKind::Complete),
            ("failed", ControllerEventKind::Failed),
            ("cancelled", ControllerEventKind::Cancelled),
        ] {
            let controller_key = format!("unknown-lane-{event_slug}");
            let (reducer, _) = Reducer::new(restored(DomainModel::default(), 41));
            let mut event =
                controller_event(&format!("lane-{event_slug}"), &controller_key, event_kind);
            event.metadata.source = SOURCE_LOG_LANE.to_owned();

            let delta = reducer
                .validate_controller_event(&event)
                .expect("a dropped lane terminal must be handled without rejection");

            assert_eq!(
                delta.post_model.task_runs().count(),
                0,
                "an unknown lane {event_slug} must not create a run"
            );
            assert!(
                delta
                    .post_model
                    .task_run_by_key(&RunKey::Controller(controller_key.clone()))
                    .is_none(),
                "the exact lane {event_slug} Controller key must remain absent"
            );
            assert_eq!(
                delta.post_next_ordinal, 41,
                "dropping lane {event_slug} must not consume a display ordinal"
            );
            assert_eq!(
                delta.diagnostic_deltas.terminal_forward_reference_creations, 0,
                "a dropped lane {event_slug} is not a terminal forward-reference creation"
            );
            assert_eq!(
                delta.diagnostic_deltas.unknown_lane_terminal_drops, 1,
                "a dropped lane {event_slug} must increment its diagnostic exactly once"
            );
            assert!(
                delta.batch.iter().all(|operation| !matches!(
                    operation,
                    PersistOp::UpsertTaskRun(_)
                        | PersistOp::UpsertExecution(_)
                        | PersistOp::UpsertExecutionEdge { .. }
                        | PersistOp::UpsertDependencyEdge { .. }
                )),
                "dropping lane {event_slug} must not persist runs, executions, or edges"
            );
            assert!(
                delta
                    .batch
                    .iter()
                    .all(|operation| !matches!(operation, PersistOp::RecordEvent { .. })),
                "the dropped lane {event_slug} must not enter the durable ledger"
            );
        }
    }

    #[tokio::test]
    async fn dropped_unknown_lane_terminal_replays_after_creator_interleaving() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let controller_key = "unknown-lane-failed-replay";
        let mut terminal = controller_event(
            "lane-failed-replay",
            controller_key,
            ControllerEventKind::Failed,
        );
        terminal.metadata.source = SOURCE_LOG_LANE.to_owned();
        terminal.metadata.timestamp_ms = unix_now_ms();
        terminal.metadata.receipt_time_ms = terminal.metadata.timestamp_ms;

        let dropped = reducer
            .validate_controller_event(&terminal)
            .expect("the early lane terminal must be dropped without rejection");
        let dropped_recorded = dropped
            .batch
            .iter()
            .any(|operation| matches!(operation, PersistOp::RecordEvent { .. }));
        let permit = writer.reserve_enqueue().unwrap();
        let pending = reducer.commit_staged(dropped, permit).unwrap();
        writer.finish_pending(pending).await.unwrap();
        assert_eq!(
            shared
                .borrow()
                .controller_diagnostics()
                .unknown_lane_terminal_drops(),
            1,
            "committing the drop must publish its diagnostic"
        );
        assert!(
            shared
                .borrow()
                .task_run_by_key(&RunKey::Controller(controller_key.to_owned()))
                .is_none(),
            "the early lane terminal must not create its target run"
        );

        let mut dispatch = controller_event(
            "lane-dispatch-after-failed",
            controller_key,
            ControllerEventKind::Dispatch {
                parent_task_run_id: "lane-parent".to_owned(),
            },
        );
        dispatch.metadata.source = SOURCE_LOG_LANE.to_owned();
        commit_controller(&mut reducer, &mut writer, dispatch).await;
        let mut started = controller_event(
            "lane-started-after-failed",
            controller_key,
            ControllerEventKind::TaskStarted,
        );
        started.metadata.source = SOURCE_LOG_LANE.to_owned();
        commit_controller(&mut reducer, &mut writer, started).await;
        assert_eq!(
            shared
                .borrow()
                .task_run_by_key(&RunKey::Controller(controller_key.to_owned()))
                .expect("the creator events must create the exact child run")
                .state,
            TaskState::Running
        );

        let replay_admitted = !writer.is_duplicate(&terminal.metadata.event_id);
        if replay_admitted {
            commit_controller(&mut reducer, &mut writer, terminal.clone()).await;
        }
        let final_state = shared
            .borrow()
            .task_run_by_key(&RunKey::Controller(controller_key.to_owned()))
            .expect("the creator events must preserve the child run")
            .state;
        lifecycle.shutdown().await.unwrap();

        assert_eq!(
            (dropped_recorded, replay_admitted, final_state),
            (false, true, TaskState::Failed),
            "the dropped terminal must write no ledger row so its identical event-id replay can apply"
        );
    }

    #[tokio::test]
    async fn unknown_lane_terminal_drop_counter_is_recorded_once_per_drop() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut dropped = controller_event(
            "counter-dropped-lane-terminal",
            "counter-unknown-lane-run",
            ControllerEventKind::Failed,
        );
        dropped.metadata.source = SOURCE_LOG_LANE.to_owned();
        commit_controller(&mut reducer, &mut writer, dropped).await;

        let mut started = controller_event(
            "counter-existing-lane-started",
            "counter-existing-lane-run",
            ControllerEventKind::TaskStarted,
        );
        started.metadata.source = SOURCE_LOG_LANE.to_owned();
        commit_controller(&mut reducer, &mut writer, started).await;
        let mut applied = controller_event(
            "counter-existing-lane-terminal",
            "counter-existing-lane-run",
            ControllerEventKind::Failed,
        );
        applied.metadata.source = SOURCE_LOG_LANE.to_owned();
        commit_controller(&mut reducer, &mut writer, applied).await;

        let counters = serde_json::to_value(crate::diagnostics::controller_counter_snapshot(
            &shared.borrow(),
        ))
        .unwrap();
        lifecycle.shutdown().await.unwrap();
        assert_eq!(
            counters.get("unknown_lane_terminal_drops"),
            Some(&serde_json::json!(1)),
            "one unknown lane terminal must increment the counter exactly once, while an applied lane terminal must not"
        );
    }

    #[test]
    fn non_lane_terminal_for_unknown_run_keeps_forward_reference_creation() {
        let controller_key = "hook-terminal-forward-reference";
        let (reducer, _) = Reducer::new(restored(DomainModel::default(), 1));
        let mut event = controller_event(
            "hook-complete-unknown",
            controller_key,
            ControllerEventKind::Complete,
        );
        event.metadata.source = "hook:claude-code".to_owned();

        let delta = reducer
            .validate_controller_event(&event)
            .expect("a hook terminal forward reference must retain existing semantics");

        let run = delta
            .post_model
            .task_run_by_key(&RunKey::Controller(controller_key.to_owned()))
            .expect("the non-lane terminal must create its Controller run");
        assert_eq!(run.state, TaskState::Completed);
        assert_eq!(delta.post_next_ordinal, 2);
        assert_eq!(
            delta.diagnostic_deltas.terminal_forward_reference_creations,
            1
        );
        let recorded = delta
            .batch
            .iter()
            .find_map(|operation| match operation {
                PersistOp::RecordEvent { event, .. } => Some(super::event_metadata(event)),
                _ => None,
            })
            .expect("the hook terminal must be recorded");
        assert_eq!(recorded.task_run_id, Some(run.run_id));
    }

    #[tokio::test]
    async fn dismiss_then_task_started_resumes_and_clears_dismissal() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event("started", "run", ControllerEventKind::TaskStarted),
        )
        .await;
        let mut dismiss = controller_event("dismiss", "run", ControllerEventKind::Dismiss);
        dismiss.metadata.receipt_time_ms = 30;
        commit_controller(&mut reducer, &mut writer, dismiss).await;
        let run_id = reducer.resolve_controller_run("run").unwrap();
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().dismissed_at_ms,
            Some(30)
        );

        let mut resumed = controller_event("resumed", "run", ControllerEventKind::TaskStarted);
        resumed.metadata.receipt_time_ms = 40;
        commit_controller(&mut reducer, &mut writer, resumed).await;

        let snapshot = shared.borrow();
        let run = snapshot.task_run(&run_id).unwrap();
        assert_eq!(run.dismissed_at_ms, None);
        assert_eq!(run.state, TaskState::Running);
        lifecycle.shutdown().await.unwrap();
    }

    fn run(run_id: RunId, key: RunKey, ordinal: i64, state: TaskState) -> TaskRun {
        TaskRun {
            run_id,
            key,
            display_ordinal: DisplayOrdinal::new(ordinal),
            state,
            has_controller_task_state_event: false,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        }
    }

    #[test]
    fn run_for_native_session_resolves_registered_native_alias() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Controller("controller-run".to_owned()),
            1,
            TaskState::Queued,
        ));
        model.insert_task_run_alias(
            RunKey::Native {
                provider: Provider::Codex,
                sid: "aliased-sid".to_owned(),
            },
            run_id,
        );
        let (reducer, _shared) = Reducer::new(restored(model, 2));

        assert_eq!(
            reducer.run_for_native_session(Provider::Codex, "aliased-sid"),
            Some(run_id)
        );
    }

    #[test]
    fn run_for_native_session_resolves_native_key() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "native-sid".to_owned(),
            },
            1,
            TaskState::Queued,
        ));
        let (reducer, _shared) = Reducer::new(restored(model, 2));

        assert_eq!(
            reducer.run_for_native_session(Provider::Codex, "native-sid"),
            Some(run_id)
        );
    }

    #[test]
    fn run_for_native_session_resolves_native_path_owner_agent_node() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::NativePath {
                provider: Provider::Codex,
                path: "/tmp/native-path-owner.jsonl".to_owned(),
            },
            1,
            TaskState::Queued,
        ));
        model.insert_agent_node(native_agent_node(
            "native-path-owner",
            "native-path-sid",
            run_id,
            2,
        ));
        let (reducer, _shared) = Reducer::new(restored(model, 3));

        assert_eq!(
            reducer.run_for_native_session(Provider::Codex, "native-path-sid"),
            Some(run_id)
        );
    }

    #[test]
    fn run_for_native_session_rejects_ambiguous_agent_node_claims() {
        let first_run_id = RunId::new();
        let second_run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            first_run_id,
            RunKey::Controller("ambiguous-first-owner".to_owned()),
            1,
            TaskState::Queued,
        ));
        model.insert_task_run(run(
            second_run_id,
            RunKey::NativePath {
                provider: Provider::Codex,
                path: "/tmp/second-owner.jsonl".to_owned(),
            },
            2,
            TaskState::Queued,
        ));
        model.insert_agent_node(native_agent_node(
            "first-owner",
            "ambiguous-sid",
            first_run_id,
            3,
        ));
        model.insert_agent_node(native_agent_node(
            "second-owner",
            "ambiguous-sid",
            second_run_id,
            4,
        ));
        let (reducer, _shared) = Reducer::new(restored(model, 5));

        assert_eq!(
            reducer.run_for_native_session(Provider::Codex, "ambiguous-sid"),
            None
        );
    }

    fn native_agent_node(
        agent_node_id: &str,
        native_session_id: &str,
        task_run_id: RunId,
        ordinal: i64,
    ) -> AgentNode {
        AgentNode {
            agent_node_id: agent_node_id.to_owned(),
            provider: Provider::Codex,
            native_session_id: Some(native_session_id.to_owned()),
            task_run_id,
            display_ordinal: DisplayOrdinal::new(ordinal),
            parent_agent_node_id: None,
            state: None,
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        }
    }

    fn run_with_controller_evidence(
        run_id: RunId,
        key: RunKey,
        ordinal: i64,
        state: TaskState,
    ) -> TaskRun {
        let mut task_run = run(run_id, key, ordinal, state);
        task_run.has_controller_task_state_event = true;
        task_run
    }

    #[test]
    fn execution_run_construction_uses_receipt_time_for_bookkeeping() {
        let run_id = RunId::new();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut event_metadata = metadata("execution-created", 10);
        event_metadata.receipt_time_ms = 25;

        reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: event_metadata,
                execution: execution(run_id, "execution-created", ExecState::Working),
            })
            .unwrap();

        let model = shared.borrow();
        let task_run = model.task_run(&run_id).unwrap();
        assert_eq!(task_run.created_at_ms, Some(25));
        assert_eq!(task_run.updated_at_ms, Some(25));
        assert_eq!(task_run.finished_at_ms, None);
    }

    #[test]
    fn non_controller_run_creation_sites_stamp_updated_at() {
        for (site, now_ms) in [("metadata", 30), ("placeholder", 40), ("snapshot", 50)] {
            let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
            let mut persist = Vec::new();
            let run_id = match site {
                "metadata" => {
                    let run_id = RunId::new();
                    let mut event_metadata = metadata("metadata-created", 10);
                    event_metadata.receipt_time_ms = now_ms;
                    reducer
                        .ensure_metadata_run(
                            run_id,
                            &event_metadata,
                            false,
                            None,
                            TaskState::Running,
                            &mut persist,
                        )
                        .unwrap();
                    run_id
                }
                "placeholder" => {
                    let run_id = RunId::new();
                    reducer
                        .ensure_controller_placeholder(run_id, None, now_ms, &mut persist)
                        .unwrap();
                    run_id
                }
                "snapshot" => reducer
                    .insert_snapshot_run(
                        None,
                        None,
                        None,
                        "snapshot-terminal",
                        now_ms,
                        &mut persist,
                    )
                    .unwrap(),
                _ => unreachable!(),
            };

            let task_run = reducer.model.task_run(&run_id).unwrap();
            assert_eq!(task_run.created_at_ms, Some(now_ms), "site: {site}");
            assert_eq!(task_run.updated_at_ms, Some(now_ms), "site: {site}");
        }
    }

    #[test]
    fn non_controller_state_mutations_advance_updated_at_and_clear_dismissal() {
        let close_run_id = RunId::new();
        let mut close_run = run(
            close_run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "close-run".to_owned(),
            },
            1,
            TaskState::Running,
        );
        close_run.created_at_ms = Some(1);
        close_run.updated_at_ms = Some(2);
        close_run.dismissed_at_ms = Some(3);
        let mut close_model = DomainModel::default();
        close_model.insert_task_run(close_run);
        close_model.insert_execution(execution(close_run_id, "close-ended", ExecState::Ended));
        let (mut close_reducer, _shared) = Reducer::new(restored(close_model, 2));
        let mut close_persist = Vec::new();

        close_reducer.close_run_without_live_execution(close_run_id, 30, &mut close_persist);

        let closed = close_reducer.model.task_run(&close_run_id).unwrap();
        assert_eq!(closed.state, TaskState::EndedUnknown);
        assert_eq!(closed.updated_at_ms, Some(30));
        assert_eq!(closed.finished_at_ms, Some(30));
        assert_eq!(closed.dismissed_at_ms, Some(3));

        let activate_run_id = RunId::new();
        let mut activate_run = run(
            activate_run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "activate-run".to_owned(),
            },
            1,
            TaskState::EndedUnknown,
        );
        activate_run.created_at_ms = Some(1);
        activate_run.updated_at_ms = Some(10);
        activate_run.finished_at_ms = Some(10);
        activate_run.dismissed_at_ms = Some(12);
        let mut activate_model = DomainModel::default();
        activate_model.insert_task_run(activate_run);
        activate_model.insert_execution(execution(
            activate_run_id,
            "activate-ended",
            ExecState::Ended,
        ));
        let (mut activate_reducer, _shared) = Reducer::new(restored(activate_model, 2));
        let mut activate_persist = Vec::new();

        activate_reducer.activate_for_live_execution(activate_run_id, 40, &mut activate_persist);

        let activated = activate_reducer.model.task_run(&activate_run_id).unwrap();
        assert_eq!(activated.state, TaskState::Running);
        assert_eq!(activated.updated_at_ms, Some(40));
        assert_eq!(activated.finished_at_ms, None);
        assert_eq!(activated.dismissed_at_ms, None);
    }

    #[test]
    fn controller_mutations_advance_updated_at_capture_subject_and_clear_dismissal() {
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let started = reducer
            .validate_controller_event(&controller_event(
                "subject-started",
                "subject-run",
                ControllerEventKind::TaskStarted,
            ))
            .unwrap();
        let run_id = started
            .post_model
            .task_run_by_key(&RunKey::Controller("subject-run".to_owned()))
            .unwrap()
            .run_id;
        let mut task_run = started.post_model.task_run(&run_id).unwrap().clone();
        assert_eq!(task_run.created_at_ms, Some(20));
        assert_eq!(task_run.updated_at_ms, Some(20));
        task_run.dismissed_at_ms = Some(25);
        let mut model = started.post_model;
        model.insert_task_run(task_run);
        let (reducer, _shared) = Reducer::new(RestoredState {
            model,
            next_ordinal: started.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let mut progress = controller_event(
            "subject-progress",
            "subject-run",
            ControllerEventKind::Progress,
        );
        progress.metadata.receipt_time_ms = 30;
        progress.metadata.label = Some("Map hook payloads".to_owned());

        let progressed = reducer.validate_controller_event(&progress).unwrap();
        let task_run = progressed.post_model.task_run(&run_id).unwrap();
        assert_eq!(task_run.updated_at_ms, Some(30));
        assert_eq!(task_run.subject.as_deref(), Some("Map hook payloads"));
        assert_eq!(task_run.dismissed_at_ms, None);

        let (reducer, _shared) = Reducer::new(RestoredState {
            model: progressed.post_model,
            next_ordinal: progressed.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let mut no_label = controller_event(
            "subject-progress-no-label",
            "subject-run",
            ControllerEventKind::Progress,
        );
        no_label.metadata.receipt_time_ms = 40;
        let progressed = reducer.validate_controller_event(&no_label).unwrap();
        let task_run = progressed.post_model.task_run(&run_id).unwrap();
        assert_eq!(task_run.updated_at_ms, Some(40));
        assert_eq!(task_run.subject.as_deref(), Some("Map hook payloads"));
    }

    #[test]
    fn first_terminal_transition_stamps_finished_at_once() {
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let started = reducer
            .validate_controller_event(&controller_event(
                "terminal-started",
                "terminal-run",
                ControllerEventKind::TaskStarted,
            ))
            .unwrap();
        let run_id = started
            .post_model
            .task_run_by_key(&RunKey::Controller("terminal-run".to_owned()))
            .unwrap()
            .run_id;
        let (reducer, _shared) = Reducer::new(RestoredState {
            model: started.post_model,
            next_ordinal: started.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let mut complete = controller_event(
            "terminal-complete",
            "terminal-run",
            ControllerEventKind::Complete,
        );
        complete.metadata.receipt_time_ms = 30;
        let completed = reducer.validate_controller_event(&complete).unwrap();
        let task_run = completed.post_model.task_run(&run_id).unwrap();
        assert_eq!(task_run.updated_at_ms, Some(30));
        assert_eq!(task_run.finished_at_ms, Some(30));

        let (reducer, _shared) = Reducer::new(RestoredState {
            model: completed.post_model,
            next_ordinal: completed.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        complete.metadata.event_id = "terminal-complete-duplicate".to_owned();
        complete.metadata.receipt_time_ms = 40;
        let duplicate = reducer.validate_controller_event(&complete).unwrap();
        let task_run = duplicate.post_model.task_run(&run_id).unwrap();
        assert_eq!(task_run.updated_at_ms, Some(40));
        assert_eq!(task_run.finished_at_ms, Some(30));
    }

    #[test]
    fn persist_task_run_passes_model_bookkeeping_through() {
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut task_run = run(
            RunId::new(),
            RunKey::Controller("persist-bookkeeping".to_owned()),
            1,
            TaskState::Completed,
        );
        task_run.created_at_ms = Some(5);
        task_run.updated_at_ms = Some(7);
        task_run.finished_at_ms = Some(6);

        let PersistOp::UpsertTaskRun(persisted) = reducer.persist_task_run(task_run, 99) else {
            unreachable!();
        };

        assert_eq!(persisted.created_at_ms, 5);
        assert_eq!(persisted.updated_at_ms, 7);
        assert_eq!(persisted.finished_at_ms, Some(6));
    }

    fn execution(run_id: RunId, execution_id: &str, state: ExecState) -> Execution {
        Execution {
            execution_id: execution_id.to_owned(),
            pane_id: "pane-1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            task_run_id: run_id,
            state,
        }
    }

    fn restored(model: DomainModel, next_ordinal: i64) -> RestoredState {
        RestoredState {
            model,
            next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        }
    }

    fn status_event(
        event_id: &str,
        timestamp_ms: i64,
        execution_id: &str,
        state: ExecState,
    ) -> NormalizedEvent {
        NormalizedEvent::AgentStatusChanged {
            metadata: metadata(event_id, timestamp_ms),
            execution_id: execution_id.to_owned(),
            state,
        }
    }

    fn provider_node_event(
        event_id: &str,
        run_id: RunId,
        native_session_id: &str,
        state: Option<ExecState>,
        model_id: Option<&str>,
        parent_agent_node_id: Option<&str>,
    ) -> NormalizedEvent {
        let mut metadata = metadata(event_id, 100);
        metadata.source = "provider".to_owned();
        metadata.source_event_type = "agent_upsert".to_owned();
        metadata.provider = Some(Provider::Codex);
        metadata.task_run_id = Some(run_id);
        NormalizedEvent::AgentNodeUpsert {
            metadata,
            node: AgentNodeObservation {
                agent_node_id: format!("agent:codex:{native_session_id}"),
                provider: Provider::Codex,
                native_session_id: Some(native_session_id.to_owned()),
                task_run_id: run_id,
                parent_agent_node_id: parent_agent_node_id.map(str::to_owned),
                state,
                model_id: model_id.map(str::to_owned),
                session_file: None,
            },
        }
    }

    fn provider_activity_event(
        event_id: &str,
        observed_at_ms: i64,
        agent_node_id: &str,
        event_kind: &str,
    ) -> NormalizedEvent {
        let mut metadata = metadata(event_id, observed_at_ms);
        metadata.source = "provider".to_owned();
        metadata.source_event_type = "activity".to_owned();
        metadata.provider = Some(Provider::Codex);
        NormalizedEvent::AgentActivity {
            metadata,
            agent_node_id: agent_node_id.to_owned(),
            activity: MinimalProviderMetadata {
                event_kind: Some(event_kind.to_owned()),
                item_count: Some(3),
                ..MinimalProviderMetadata::default()
            },
        }
    }

    #[test]
    fn provider_upsert_reuses_gap_node_patches_fields_and_never_reactivates_run() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "root-session".to_owned(),
            },
            7,
            TaskState::EndedUnknown,
        ));
        model.insert_agent_node(AgentNode {
            agent_node_id: "gap-agent-existing".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("child-1".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(8),
            parent_agent_node_id: None,
            state: None,
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: Some("/kept/session.jsonl".to_owned()),
        });
        let (mut reducer, shared) = Reducer::new(restored(model, 20));

        let outcome = reducer
            .apply(provider_node_event(
                "prov:codex:up:call-1",
                run_id,
                "child-1",
                Some(ExecState::Working),
                Some("gpt-5.6"),
                None,
            ))
            .unwrap();

        assert!(matches!(outcome, ApplyOutcome::Applied(_)));
        let model = shared.borrow();
        assert_eq!(model.agent_nodes().count(), 1);
        let node = model.agent_node("gap-agent-existing").unwrap();
        assert_eq!(node.display_ordinal, DisplayOrdinal::new(8));
        assert_eq!(node.state, Some(ExecState::Working));
        assert_eq!(node.model_id.as_deref(), Some("gpt-5.6"));
        assert_eq!(node.session_file.as_deref(), Some("/kept/session.jsonl"));
        assert_eq!(
            model.task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );
    }

    #[test]
    fn provider_ended_agent_node_is_terminal_and_keeps_ownership() {
        let root_run_id = RunId::new();
        let child_run_id = RunId::new();
        let mut root_run = run_with_controller_evidence(
            root_run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "root-session".to_owned(),
            },
            1,
            TaskState::Running,
        );
        root_run.created_at_ms = Some(10);
        root_run.updated_at_ms = Some(20);
        root_run.subject = Some("root lifecycle".to_owned());
        let mut child_run = run_with_controller_evidence(
            child_run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "child-session".to_owned(),
            },
            2,
            TaskState::Running,
        );
        child_run.created_at_ms = Some(30);
        child_run.updated_at_ms = Some(40);
        child_run.subject = Some("child lifecycle".to_owned());
        let mut model = DomainModel::default();
        model.insert_task_run(root_run.clone());
        model.insert_task_run(child_run.clone());
        let root_v6 = TaskRunV6State {
            latest_provider_at_ms: Some(50),
            ..TaskRunV6State::default()
        };
        let child_v6 = TaskRunV6State {
            latest_provider_at_ms: Some(60),
            ..TaskRunV6State::default()
        };
        model.set_task_run_v6_state(root_run_id, root_v6.clone());
        model.set_task_run_v6_state(child_run_id, child_v6.clone());
        let original_node = AgentNode {
            agent_node_id: "agent:codex:child-session".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("child-session".to_owned()),
            task_run_id: root_run_id,
            display_ordinal: DisplayOrdinal::new(3),
            parent_agent_node_id: Some("agent:codex:root-session".to_owned()),
            state: Some(ExecState::Working),
            model_id: Some("gpt-terminal".to_owned()),
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: Some("/root/child-session.jsonl".to_owned()),
        };
        model.insert_agent_node(original_node.clone());
        let (mut reducer, shared) = Reducer::new(restored(model, 4));

        let mut ended = provider_node_event(
            "prov:codex:up:ended",
            root_run_id,
            "child-session",
            Some(ExecState::Ended),
            None,
            Some("agent:codex:root-session"),
        );
        let NormalizedEvent::AgentNodeUpsert { metadata, .. } = &mut ended else {
            unreachable!();
        };
        metadata.timestamp_ms = 100;
        metadata.receipt_time_ms = 100;
        reducer.apply(ended).unwrap();

        for (event_id, state, observed_at_ms) in [
            ("prov:codex:up:idle", ExecState::Idle, 200),
            ("prov:codex:up:working", ExecState::Working, 300),
        ] {
            let mut observation = provider_node_event(
                event_id,
                child_run_id,
                "child-session",
                Some(state),
                None,
                Some("agent:codex:root-session"),
            );
            let NormalizedEvent::AgentNodeUpsert { metadata, .. } = &mut observation else {
                unreachable!();
            };
            metadata.timestamp_ms = observed_at_ms;
            metadata.receipt_time_ms = observed_at_ms;
            reducer.apply(observation).unwrap();
        }

        let model = shared.borrow();
        let mut expected_node = original_node;
        expected_node.state = Some(ExecState::Ended);
        assert_eq!(model.agent_nodes().count(), 1);
        assert_eq!(
            model.agent_node("agent:codex:child-session"),
            Some(&expected_node)
        );
        assert_eq!(model.task_run(&root_run_id), Some(&root_run));
        assert_eq!(model.task_run(&child_run_id), Some(&child_run));
        assert_eq!(model.task_run_v6_state(&root_run_id), Some(&root_v6));
        assert_eq!(model.task_run_v6_state(&child_run_id), Some(&child_v6));
    }

    #[test]
    fn provider_upsert_mints_the_frozen_deterministic_node_id() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "root-session".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let mut event = provider_node_event(
            "prov:codex:node:child-1",
            run_id,
            "child-1",
            None,
            None,
            None,
        );
        let NormalizedEvent::AgentNodeUpsert { node, .. } = &mut event else {
            unreachable!();
        };
        node.agent_node_id = "caller-chosen-id".to_owned();

        reducer.apply(event).unwrap();
        for state in [
            ExecState::Idle,
            ExecState::Blocked,
            ExecState::Ended,
            ExecState::Stale { since_ms: 1 },
        ] {
            reducer
                .apply(provider_node_event(
                    &format!("prov:codex:up:{state:?}"),
                    run_id,
                    "child-1",
                    Some(state),
                    None,
                    None,
                ))
                .unwrap();
        }

        assert!(shared.borrow().agent_node("caller-chosen-id").is_none());
        assert_eq!(
            shared
                .borrow()
                .agent_node("agent:codex:child-1")
                .unwrap()
                .state,
            Some(ExecState::Ended)
        );
    }

    #[test]
    fn provider_activity_keeps_newest_observed_time_then_latest_arrival() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "root-session".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        reducer
            .apply(provider_node_event(
                "prov:codex:node:child-1",
                run_id,
                "child-1",
                None,
                None,
                None,
            ))
            .unwrap();
        reducer
            .apply(provider_activity_event(
                "prov:codex:act:new",
                200,
                "agent:codex:child-1",
                "new",
            ))
            .unwrap();
        reducer
            .apply(provider_activity_event(
                "prov:codex:act:old",
                199,
                "agent:codex:child-1",
                "old",
            ))
            .unwrap();
        reducer
            .apply(provider_activity_event(
                "prov:codex:act:tie",
                200,
                "agent:codex:child-1",
                "tie-wins",
            ))
            .unwrap();

        let model = shared.borrow();
        let node = model.agent_node("agent:codex:child-1").unwrap();
        assert_eq!(node.last_activity_at_ms, Some(200));
        assert_eq!(node.last_event_kind.as_deref(), Some("tie-wins"));
        assert_eq!(node.last_item_count, Some(3));
        assert_eq!(node.display_ordinal, DisplayOrdinal::new(2));
    }

    #[test]
    fn provider_parent_self_link_and_cycle_are_dropped_and_counted_without_dropping_nodes() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "root-session".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        reducer
            .apply(provider_node_event(
                "prov:codex:link:a:b",
                run_id,
                "a",
                None,
                None,
                Some("agent:codex:b"),
            ))
            .unwrap();
        reducer
            .apply(provider_node_event(
                "prov:codex:link:b:a",
                run_id,
                "b",
                None,
                None,
                Some("agent:codex:a"),
            ))
            .unwrap();
        reducer
            .apply(provider_node_event(
                "prov:codex:link:c:c",
                run_id,
                "c",
                None,
                None,
                Some("agent:codex:c"),
            ))
            .unwrap();

        let model = shared.borrow();
        assert!(model.agent_node("agent:codex:b").is_some());
        assert!(model.agent_node("agent:codex:c").is_some());
        assert_eq!(
            model
                .agent_node("agent:codex:b")
                .unwrap()
                .parent_agent_node_id,
            None
        );
        assert_eq!(
            model
                .agent_node("agent:codex:c")
                .unwrap()
                .parent_agent_node_id,
            None
        );
        assert_eq!(
            model.controller_diagnostics().provider_parent_conflicts(),
            2
        );
    }

    fn topology_event(metadata: EventMetadata, workspace_id: &str) -> NormalizedEvent {
        NormalizedEvent::TopologyUpsert {
            metadata,
            authority: TopologyAuthority::Partial,
            entity: TopologyEntity::Workspace(Workspace {
                workspace_id: workspace_id.to_owned(),
            }),
        }
    }

    fn topology_entity_event(event_id: &str, entity: TopologyEntity) -> NormalizedEvent {
        NormalizedEvent::TopologyUpsert {
            metadata: metadata(event_id, 1_000),
            authority: TopologyAuthority::Partial,
            entity,
        }
    }

    fn topology_entity_event_with_authority(
        event_id: &str,
        entity: TopologyEntity,
        authority: &str,
    ) -> NormalizedEvent {
        serde_json::from_value(serde_json::json!({
            "TopologyUpsert": {
                "metadata": metadata(event_id, 1_000),
                "entity": entity,
                "authority": authority,
            },
        }))
        .unwrap()
    }

    fn topology_snapshot(
        workspaces: &[&str],
        tabs: &[(&str, &str)],
        panes: &[(&str, &str, &str)],
    ) -> TopologySnapshot {
        TopologySnapshot {
            workspaces: workspaces
                .iter()
                .map(|workspace_id| Workspace {
                    workspace_id: (*workspace_id).to_owned(),
                })
                .collect(),
            tabs: tabs
                .iter()
                .map(|(tab_id, workspace_id)| Tab {
                    tab_id: (*tab_id).to_owned(),
                    workspace_id: (*workspace_id).to_owned(),
                    label: None,
                })
                .collect(),
            panes: panes
                .iter()
                .map(|(pane_id, workspace_id, tab_id)| PaneSnapshot {
                    pane_id: (*pane_id).to_owned(),
                    workspace_id: (*workspace_id).to_owned(),
                    tab_id: (*tab_id).to_owned(),
                    terminal_id: format!("terminal-{pane_id}"),
                    display_name: None,
                    agent: None,
                    agent_session: None,
                })
                .collect(),
        }
    }

    fn reducer_with_named_topology() -> (Reducer, SharedModel) {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        for (event_id, entity) in [
            (
                "named-workspace",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            ),
            (
                "named-tab",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: Some("old tab".to_owned()),
                }),
            ),
            (
                "named-pane",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "terminal".to_owned(),
                    display_name: Some("old pane".to_owned()),
                }),
            ),
        ] {
            reducer
                .apply(topology_entity_event(event_id, entity))
                .unwrap();
        }
        (reducer, shared)
    }

    fn names_cleared_by_isolated_operation(operation: &PersistOp) -> (bool, bool) {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![
                PersistOp::UpsertWorkspace {
                    workspace: Workspace {
                        workspace_id: "workspace".to_owned(),
                    },
                    display_ordinal: DisplayOrdinal::new(1),
                },
                PersistOp::UpsertTab {
                    tab: Tab {
                        tab_id: "tab".to_owned(),
                        workspace_id: "workspace".to_owned(),
                        label: Some("old tab".to_owned()),
                    },
                    display_ordinal: DisplayOrdinal::new(2),
                },
                PersistOp::UpsertPane {
                    pane: Pane {
                        pane_id: "pane".to_owned(),
                        workspace_id: "workspace".to_owned(),
                        tab_id: "tab".to_owned(),
                        terminal_id: "terminal".to_owned(),
                        display_name: Some("old pane".to_owned()),
                    },
                    display_ordinal: DisplayOrdinal::new(3),
                },
            ])
            .unwrap();
        store.apply_batch(vec![operation.clone()]).unwrap();
        let restored = store.load_restored_state().unwrap();
        (
            restored
                .model
                .tab("tab")
                .is_some_and(|tab| tab.label.is_none()),
            restored
                .model
                .pane("pane")
                .is_some_and(|pane| pane.display_name.is_none()),
        )
    }

    #[test]
    fn partial_topology_upsert_preserves_names() {
        let (mut reducer, shared) = reducer_with_named_topology();
        let mut tab_event = topology_entity_event_with_authority(
            "partial-tab",
            TopologyEntity::Tab(Tab {
                tab_id: "tab".to_owned(),
                workspace_id: "workspace".to_owned(),
                label: None,
            }),
            "Partial",
        );
        let NormalizedEvent::TopologyUpsert { metadata, .. } = &mut tab_event else {
            unreachable!();
        };
        metadata.source_event_type = "tab_renamed".to_owned();

        let ApplyOutcome::Applied(tab_batch) = reducer.apply(tab_event).unwrap() else {
            panic!("partial tab observation should apply");
        };
        let ApplyOutcome::Applied(pane_batch) = reducer
            .apply(topology_entity_event_with_authority(
                "partial-pane",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "terminal".to_owned(),
                    display_name: None,
                }),
                "Partial",
            ))
            .unwrap()
        else {
            panic!("partial pane observation should apply");
        };

        assert_eq!(
            shared.borrow().tab("tab").unwrap().label.as_deref(),
            Some("old tab")
        );
        assert_eq!(
            shared
                .borrow()
                .pane("pane")
                .unwrap()
                .display_name
                .as_deref(),
            Some("old pane")
        );
        assert_eq!(
            tab_batch.len(),
            2,
            "partial tab upsert must not append a clear"
        );
        assert_eq!(
            pane_batch.len(),
            2,
            "partial pane upsert must not append a clear"
        );
    }

    #[test]
    fn authoritative_topology_upsert_sets_and_clears_names() {
        let (mut reducer, shared) = reducer_with_named_topology();
        for (event_id, entity) in [
            (
                "set-tab",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: Some("new\ntab".to_owned()),
                }),
            ),
            (
                "set-pane",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "terminal".to_owned(),
                    display_name: Some("new\npane".to_owned()),
                }),
            ),
        ] {
            reducer
                .apply(topology_entity_event_with_authority(
                    event_id,
                    entity,
                    "Authoritative",
                ))
                .unwrap();
        }
        assert_eq!(
            shared.borrow().tab("tab").unwrap().label.as_deref(),
            Some("new\\ntab")
        );
        assert_eq!(
            shared
                .borrow()
                .pane("pane")
                .unwrap()
                .display_name
                .as_deref(),
            Some("new\\npane")
        );

        let ApplyOutcome::Applied(tab_batch) = reducer
            .apply(topology_entity_event_with_authority(
                "clear-tab",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: None,
                }),
                "Authoritative",
            ))
            .unwrap()
        else {
            panic!("authoritative tab clear should apply");
        };
        let ApplyOutcome::Applied(pane_batch) = reducer
            .apply(topology_entity_event_with_authority(
                "clear-pane",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "terminal".to_owned(),
                    display_name: Some(String::new()),
                }),
                "Authoritative",
            ))
            .unwrap()
        else {
            panic!("authoritative pane clear should apply");
        };

        assert_eq!(shared.borrow().tab("tab").unwrap().label, None);
        assert_eq!(shared.borrow().pane("pane").unwrap().display_name, None);
        assert_eq!(
            tab_batch.len(),
            3,
            "authoritative tab clear must be explicit"
        );
        assert_eq!(
            pane_batch.len(),
            3,
            "authoritative pane clear must be explicit"
        );
    }

    #[test]
    fn authoritative_snapshot_clear_orders_upsert_before_clear() {
        let (mut reducer, _shared) = reducer_with_named_topology();
        let batch = reducer
            .reconcile_gap(ReconcileBatch {
                topology: topology_snapshot(
                    &["workspace"],
                    &[("tab", "workspace")],
                    &[("pane", "workspace", "tab")],
                ),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let upsert_tab = batch
            .iter()
            .position(|operation| matches!(operation, PersistOp::UpsertTab { tab, .. } if tab.tab_id == "tab"))
            .unwrap();
        let upsert_pane = batch
            .iter()
            .position(|operation| matches!(operation, PersistOp::UpsertPane { pane, .. } if pane.pane_id == "pane"))
            .unwrap();
        let cleared_names = batch
            .iter()
            .enumerate()
            .map(|(index, operation)| (index, names_cleared_by_isolated_operation(operation)))
            .collect::<Vec<_>>();
        let clear_tab = cleared_names
            .iter()
            .find_map(|(index, (tab, _))| tab.then_some(*index))
            .expect("snapshot batch must explicitly clear the tab label");
        let clear_pane = cleared_names
            .iter()
            .find_map(|(index, (_, pane))| pane.then_some(*index))
            .expect("snapshot batch must explicitly clear the pane display name");

        assert!(upsert_tab < clear_tab);
        assert!(upsert_pane < clear_pane);
    }

    #[test]
    fn authoritative_snapshot_publishes_once_and_rolls_back_on_late_error() {
        let (mut reducer, mut shared) = Reducer::new(restored(DomainModel::default(), 1));
        let before_success = reducer.publish_count.get();
        let successful = topology_snapshot(
            &["workspace"],
            &[("tab", "workspace")],
            &[("pane", "workspace", "tab")],
        );

        let batch = reducer
            .reconcile_snapshot(successful)
            .expect("an authoritative snapshot should install atomically");

        assert_eq!(reducer.publish_count.get() - before_success, 1);
        assert!(shared.has_changed().unwrap());
        let installed = Arc::clone(&shared.borrow_and_update());
        assert!(installed.workspace("workspace").is_some());
        assert!(installed.tab("tab").is_some());
        assert!(installed.pane("pane").is_some());
        assert!(!batch.is_empty());

        let before_next_ordinal = i64::MAX - 1;
        reducer.next_ordinal = before_next_ordinal;
        let before_error = reducer.publish_count.get();
        let before_model = Arc::clone(&shared.borrow());
        let mut late = topology_snapshot(
            &["late-workspace"],
            &[("late-tab", "late-workspace")],
            &[("late-pane", "late-workspace", "late-tab")],
        );
        late.panes[0].agent = Some(SnapshotAgent {
            agent_name: "codex".to_owned(),
            status: PaneAgentStatus::Working,
        });
        let result = reducer.reconcile_snapshot(late);

        assert_eq!(result, Err(ReducerError::OrdinalExhausted));
        assert_eq!(reducer.publish_count.get(), before_error);
        assert_eq!(
            reducer.next_ordinal, before_next_ordinal,
            "the run ordinal allocated before the later agent-node failure must roll back"
        );
        assert!(!shared.has_changed().unwrap());
        assert!(Arc::ptr_eq(&before_model, &shared.borrow()));
        assert!(shared.borrow().workspace("late-workspace").is_none());
    }

    #[test]
    fn authoritative_snapshot_removes_absent_entities_immediately() {
        let mut initial = topology_snapshot(
            &["workspace", "absent-workspace"],
            &[("tab", "workspace"), ("absent-tab", "absent-workspace")],
            &[
                ("pane", "workspace", "tab"),
                ("absent-pane", "absent-workspace", "absent-tab"),
            ],
        );
        initial
            .panes
            .iter_mut()
            .find(|pane| pane.pane_id == "absent-pane")
            .unwrap()
            .agent = Some(SnapshotAgent {
            agent_name: "codex".to_owned(),
            status: PaneAgentStatus::Working,
        });
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        reducer
            .reconcile_gap(ReconcileBatch {
                topology: initial,
                gap_kind: GapKind::Startup,
            })
            .unwrap();

        let batch = reducer
            .reconcile_snapshot(topology_snapshot(
                &["workspace"],
                &[("tab", "workspace")],
                &[("pane", "workspace", "tab")],
            ))
            .unwrap();

        let model = shared.borrow();
        assert!(model.workspace("absent-workspace").is_none());
        assert!(model.tab("absent-tab").is_none());
        assert!(model.pane("absent-pane").is_none());
        assert!(batch.iter().any(
            |operation| matches!(operation, PersistOp::DeletePane { pane_id } if pane_id == "absent-pane")
        ));
        assert!(batch.iter().any(
            |operation| matches!(operation, PersistOp::DeleteTab { tab_id } if tab_id == "absent-tab")
        ));
        assert!(batch.iter().any(|operation| matches!(
            operation,
            PersistOp::DeleteWorkspace { workspace_id } if workspace_id == "absent-workspace"
        )));
    }

    #[test]
    fn authoritative_orphans_emit_no_upsert_or_clear() {
        let (mut reducer, _shared) = reducer_with_named_topology();
        for (event_id, entity) in [
            (
                "orphan-tab",
                TopologyEntity::Tab(Tab {
                    tab_id: "orphan-tab".to_owned(),
                    workspace_id: "missing-workspace".to_owned(),
                    label: None,
                }),
            ),
            (
                "orphan-pane",
                TopologyEntity::Pane(Pane {
                    pane_id: "orphan-pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "missing-tab".to_owned(),
                    terminal_id: "orphan-terminal".to_owned(),
                    display_name: None,
                }),
            ),
        ] {
            let ApplyOutcome::Applied(batch) = reducer
                .apply(topology_entity_event_with_authority(
                    event_id,
                    entity,
                    "Authoritative",
                ))
                .unwrap()
            else {
                panic!("orphan observation should still be recorded");
            };
            assert_eq!(batch.len(), 1, "orphan must emit only its event record");
        }

        for (event_id, entity) in [
            (
                "known-tab-clear-control",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: None,
                }),
            ),
            (
                "known-pane-clear-control",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "terminal".to_owned(),
                    display_name: None,
                }),
            ),
        ] {
            let ApplyOutcome::Applied(batch) = reducer
                .apply(topology_entity_event_with_authority(
                    event_id,
                    entity,
                    "Authoritative",
                ))
                .unwrap()
            else {
                panic!("known authoritative clear should apply");
            };
            assert_eq!(
                batch.len(),
                3,
                "positive control must emit upsert, clear, and record"
            );
        }
    }

    #[test]
    fn topology_upsert_drops_pane_after_parent_tab_closes_and_snapshot_restores() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        for (event_id, entity) in [
            (
                "workspace-created",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            ),
            (
                "tab-created",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: None,
                }),
            ),
        ] {
            reducer
                .apply(topology_entity_event(event_id, entity))
                .unwrap();
        }
        reducer
            .apply(NormalizedEvent::TopologyClosure {
                metadata: metadata("tab-closed", 1_001),
                entity: TopologyEntityId::Tab {
                    tab_id: "tab".to_owned(),
                },
            })
            .unwrap();

        let ApplyOutcome::Applied(orphan_batch) = reducer
            .apply(topology_entity_event(
                "pane-created-after-tab-closed",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "terminal".to_owned(),
                    display_name: None,
                }),
            ))
            .unwrap()
        else {
            panic!("orphan pane observation should still be recorded");
        };
        assert!(
            !orphan_batch.iter().any(|operation| matches!(
                operation,
                PersistOp::UpsertPane { pane, .. } if pane.pane_id == "pane"
            )),
            "orphan pane must not emit UpsertPane"
        );
        assert!(
            shared.borrow().pane("pane").is_none(),
            "orphan pane must not enter the model"
        );

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: topology_snapshot(
                    &["workspace"],
                    &[("tab", "workspace")],
                    &[("pane", "workspace", "tab")],
                ),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let model = shared.borrow();
        assert!(model.tab("tab").is_some());
        assert!(model.pane("pane").is_some());
    }

    #[test]
    fn topology_upsert_existing_tab_pane_uses_ordinal_not_consumed_by_orphan() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 10));
        for (event_id, entity) in [
            (
                "workspace-created",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            ),
            (
                "tab-created",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: None,
                }),
            ),
            (
                "orphan-pane-created",
                TopologyEntity::Pane(Pane {
                    pane_id: "orphan-pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "missing-tab".to_owned(),
                    terminal_id: "orphan-terminal".to_owned(),
                    display_name: None,
                }),
            ),
        ] {
            reducer
                .apply(topology_entity_event(event_id, entity))
                .unwrap();
        }

        let ApplyOutcome::Applied(valid_batch) = reducer
            .apply(topology_entity_event(
                "valid-pane-created",
                TopologyEntity::Pane(Pane {
                    pane_id: "valid-pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "valid-terminal".to_owned(),
                    display_name: None,
                }),
            ))
            .unwrap()
        else {
            panic!("valid pane should apply");
        };
        assert!(
            valid_batch.iter().any(|operation| matches!(
                operation,
                PersistOp::UpsertPane {
                    pane,
                    display_ordinal,
                } if pane.pane_id == "valid-pane"
                    && *display_ordinal == DisplayOrdinal::new(12)
            )),
            "valid pane must use the first ordinal after its existing parents"
        );
        assert!(shared.borrow().pane("valid-pane").is_some());
    }

    #[test]
    fn topology_upsert_drops_orphan_tab_without_blocking_valid_tab_or_ordinal() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 20));
        let ApplyOutcome::Applied(orphan_batch) = reducer
            .apply(topology_entity_event(
                "orphan-tab-created",
                TopologyEntity::Tab(Tab {
                    tab_id: "orphan-tab".to_owned(),
                    workspace_id: "missing-workspace".to_owned(),
                    label: Some("orphan".to_owned()),
                }),
            ))
            .unwrap()
        else {
            panic!("orphan tab observation should still be recorded");
        };
        assert!(
            !orphan_batch.iter().any(|operation| matches!(
                operation,
                PersistOp::UpsertTab { tab, .. } if tab.tab_id == "orphan-tab"
            )),
            "orphan tab must not emit UpsertTab"
        );
        assert!(
            !orphan_batch.iter().any(|operation| matches!(
                operation,
                PersistOp::ClearTabLabel { tab_id } if tab_id == "orphan-tab"
            )),
            "orphan tab must not emit ClearTabLabel"
        );
        assert!(
            shared.borrow().tab("orphan-tab").is_none(),
            "orphan tab must not enter the model"
        );

        reducer
            .apply(topology_entity_event(
                "workspace-created",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            ))
            .unwrap();
        let ApplyOutcome::Applied(valid_batch) = reducer
            .apply(topology_entity_event(
                "valid-tab-created",
                TopologyEntity::Tab(Tab {
                    tab_id: "valid-tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: None,
                }),
            ))
            .unwrap()
        else {
            panic!("valid tab should apply");
        };
        assert!(valid_batch.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTab {
                tab,
                display_ordinal,
            } if tab.tab_id == "valid-tab" && *display_ordinal == DisplayOrdinal::new(21)
        )));
        assert!(shared.borrow().tab("valid-tab").is_some());
    }

    #[test]
    fn topology_upsert_drops_orphan_tab_rename_without_clearing_label() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut event_metadata = metadata("orphan-tab-renamed", 1_000);
        event_metadata.source_event_type = "tab_renamed".to_owned();
        let ApplyOutcome::Applied(batch) = reducer
            .apply(NormalizedEvent::TopologyUpsert {
                metadata: event_metadata,
                authority: TopologyAuthority::Partial,
                entity: TopologyEntity::Tab(Tab {
                    tab_id: "orphan-tab".to_owned(),
                    workspace_id: "missing-workspace".to_owned(),
                    label: None,
                }),
            })
            .unwrap()
        else {
            panic!("orphan tab rename observation should still be recorded");
        };

        assert!(
            !batch.iter().any(|operation| matches!(
                operation,
                PersistOp::ClearTabLabel { tab_id } if tab_id == "orphan-tab"
            )),
            "orphan tab rename must not emit ClearTabLabel"
        );
        assert!(!batch.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTab { tab, .. } if tab.tab_id == "orphan-tab"
        )));
        assert!(shared.borrow().tab("orphan-tab").is_none());
    }

    #[test]
    fn reconcile_gap_propagates_pane_snapshot_display_name() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut topology = topology_snapshot(
            &["workspace"],
            &[("tab", "workspace")],
            &[("pane", "workspace", "tab")],
        );
        topology.panes[0].display_name = Some("UI修正".to_owned());

        let persist = reducer
            .reconcile_gap(ReconcileBatch {
                topology,
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();

        assert_eq!(
            shared
                .borrow()
                .pane("pane")
                .unwrap()
                .display_name
                .as_deref(),
            Some("UI修正")
        );
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertPane { pane, .. }
                if pane.display_name.as_deref() == Some("UI修正")
        )));
    }

    #[test]
    fn observational_nameless_tab_upsert_preserves_live_and_store_label_without_clear() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![
                PersistOp::UpsertWorkspace {
                    workspace: Workspace {
                        workspace_id: "workspace".to_owned(),
                    },
                    display_ordinal: DisplayOrdinal::new(1),
                },
                PersistOp::UpsertTab {
                    tab: Tab {
                        tab_id: "tab".to_owned(),
                        workspace_id: "workspace".to_owned(),
                        label: Some("observed name".to_owned()),
                    },
                    display_ordinal: DisplayOrdinal::new(2),
                },
            ])
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (mut reducer, shared) = Reducer::new(restored);

        let ApplyOutcome::Applied(batch) = reducer
            .apply(topology_entity_event(
                "observational-tab",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: None,
                }),
            ))
            .unwrap()
        else {
            panic!("observational tab upsert should apply");
        };
        assert!(!batch.iter().any(|operation| matches!(
            operation,
            PersistOp::ClearTabLabel { tab_id } if tab_id == "tab"
        )));
        assert_eq!(
            shared.borrow().tab("tab").unwrap().label.as_deref(),
            Some("observed name")
        );

        store.apply_batch(batch).unwrap();
        let restored = store.load_restored_state().unwrap();
        assert_eq!(
            restored.model.tab("tab").unwrap().label.as_deref(),
            Some("observed name")
        );
    }

    #[test]
    fn topology_upsert_allocates_once_and_failed_batch_rolls_allocator_back() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 40));

        let outcome = reducer
            .apply(topology_entity_event(
                "workspace-first",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            ))
            .unwrap();
        let ApplyOutcome::Applied(batch) = outcome else {
            panic!("topology upsert should apply");
        };
        assert!(batch.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertWorkspace {
                workspace,
                display_ordinal,
            } if workspace.workspace_id == "workspace"
                && *display_ordinal == DisplayOrdinal::new(40)
        )));
        assert_eq!(
            shared.borrow().workspace_ordinal("workspace"),
            Some(DisplayOrdinal::new(40))
        );

        reducer
            .apply(topology_entity_event(
                "workspace-refresh",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            ))
            .unwrap();
        assert_eq!(
            shared.borrow().workspace_ordinal("workspace"),
            Some(DisplayOrdinal::new(40))
        );

        let (mut exhausting, exhausting_shared) =
            Reducer::new(restored(DomainModel::default(), i64::MAX - 1));
        let rejected = exhausting.apply_observation(vec![
            topology_entity_event(
                "rolled-back-first",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "rolled-back-first".to_owned(),
                }),
            ),
            topology_entity_event(
                "rolled-back-second",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "rolled-back-second".to_owned(),
                }),
            ),
        ]);
        assert_eq!(rejected, Err(ReducerError::OrdinalExhausted));
        assert!(exhausting_shared.borrow().workspaces().next().is_none());

        exhausting
            .apply(topology_entity_event(
                "after-rollback",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "after-rollback".to_owned(),
                }),
            ))
            .unwrap();
        assert_eq!(
            exhausting_shared
                .borrow()
                .workspace_ordinal("after-rollback"),
            Some(DisplayOrdinal::new(i64::MAX - 1))
        );
    }

    #[test]
    fn snapshot_replacement_retains_survivors_drops_absent_and_closure_reallocates() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 10));
        for (event_id, entity) in [
            (
                "workspace-keep",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace-keep".to_owned(),
                }),
            ),
            (
                "workspace-drop",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace-drop".to_owned(),
                }),
            ),
            (
                "tab-keep",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab-keep".to_owned(),
                    workspace_id: "workspace-keep".to_owned(),
                    label: None,
                }),
            ),
            (
                "pane-keep",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane-keep".to_owned(),
                    workspace_id: "workspace-keep".to_owned(),
                    tab_id: "tab-keep".to_owned(),
                    terminal_id: "terminal-keep".to_owned(),
                    display_name: None,
                }),
            ),
        ] {
            reducer
                .apply(topology_entity_event(event_id, entity))
                .unwrap();
        }
        let before = shared.borrow();
        let workspace_ordinal = before.workspace_ordinal("workspace-keep").unwrap();
        let tab_ordinal = before.tab_ordinal("tab-keep").unwrap();
        let pane_ordinal = before.pane_ordinal("pane-keep").unwrap();
        drop(before);

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: topology_snapshot(
                    &["workspace-keep"],
                    &[("tab-keep", "workspace-keep")],
                    &[("pane-keep", "workspace-keep", "tab-keep")],
                ),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        assert_eq!(
            shared.borrow().workspace_ordinal("workspace-keep"),
            Some(workspace_ordinal)
        );
        assert_eq!(shared.borrow().tab_ordinal("tab-keep"), Some(tab_ordinal));
        assert_eq!(
            shared.borrow().pane_ordinal("pane-keep"),
            Some(pane_ordinal)
        );
        assert_eq!(shared.borrow().workspace_ordinal("workspace-drop"), None);

        reducer
            .apply(NormalizedEvent::TopologyClosure {
                metadata: metadata("pane-closed", 2_000),
                entity: TopologyEntityId::Pane {
                    pane_id: "pane-keep".to_owned(),
                },
            })
            .unwrap();
        assert_eq!(shared.borrow().pane_ordinal("pane-keep"), None);

        reducer
            .apply(topology_entity_event(
                "pane-reobserved",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane-keep".to_owned(),
                    workspace_id: "workspace-keep".to_owned(),
                    tab_id: "tab-keep".to_owned(),
                    terminal_id: "terminal-keep".to_owned(),
                    display_name: None,
                }),
            ))
            .unwrap();
        assert!(shared.borrow().pane_ordinal("pane-keep").unwrap() > pane_ordinal);
    }

    #[test]
    fn gap_replacement_delete_reinsert_round_trips_topology_ordinals() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let restored_state = store.load_restored_state().unwrap();
        let (mut reducer, shared) = Reducer::new(restored_state);

        for (event_id, entity) in [
            (
                "round-trip-workspace",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            ),
            (
                "round-trip-tab",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: None,
                }),
            ),
            (
                "round-trip-pane",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "terminal".to_owned(),
                    display_name: None,
                }),
            ),
        ] {
            let ApplyOutcome::Applied(batch) = reducer
                .apply(topology_entity_event(event_id, entity))
                .unwrap()
            else {
                panic!("topology event should apply");
            };
            store.apply_batch(batch).unwrap();
        }
        let before = shared.borrow();
        let expected = (
            before.workspace_ordinal("workspace").unwrap(),
            before.tab_ordinal("tab").unwrap(),
            before.pane_ordinal("pane").unwrap(),
        );
        drop(before);

        let batch = reducer
            .reconcile_gap(ReconcileBatch {
                topology: topology_snapshot(
                    &["workspace"],
                    &[("tab", "workspace")],
                    &[("pane", "workspace", "tab")],
                ),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        store.apply_batch(batch).unwrap();
        drop(store);

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(
            restored.model.workspace_ordinal("workspace"),
            Some(expected.0)
        );
        assert_eq!(restored.model.tab_ordinal("tab"), Some(expected.1));
        assert_eq!(restored.model.pane_ordinal("pane"), Some(expected.2));
    }

    #[test]
    fn reconcile_gap_clears_names_durably() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let restored_state = store.load_restored_state().unwrap();
        let (mut reducer, shared) = Reducer::new(restored_state);

        for (event_id, entity) in [
            (
                "named-workspace",
                TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            ),
            (
                "named-tab",
                TopologyEntity::Tab(Tab {
                    tab_id: "tab".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    label: Some("stored tab".to_owned()),
                }),
            ),
            (
                "named-pane",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "terminal".to_owned(),
                    display_name: Some("stored pane".to_owned()),
                }),
            ),
        ] {
            let ApplyOutcome::Applied(batch) = reducer
                .apply(topology_entity_event(event_id, entity))
                .unwrap()
            else {
                panic!("topology event should apply");
            };
            store.apply_batch(batch).unwrap();
        }

        let batch = reducer
            .reconcile_gap(ReconcileBatch {
                topology: topology_snapshot(
                    &["workspace"],
                    &[("tab", "workspace")],
                    &[("pane", "workspace", "tab")],
                ),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let delete_tab = batch
            .iter()
            .position(
                |operation| matches!(operation, PersistOp::DeleteTab { tab_id } if tab_id == "tab"),
            )
            .unwrap();
        let upsert_tab = batch
            .iter()
            .position(|operation| matches!(operation, PersistOp::UpsertTab { tab, .. } if tab.tab_id == "tab"))
            .unwrap();
        let delete_pane = batch
            .iter()
            .position(|operation| matches!(operation, PersistOp::DeletePane { pane_id } if pane_id == "pane"))
            .unwrap();
        let upsert_pane = batch
            .iter()
            .position(|operation| matches!(operation, PersistOp::UpsertPane { pane, .. } if pane.pane_id == "pane"))
            .unwrap();
        assert!(delete_tab < upsert_tab);
        assert!(delete_pane < upsert_pane);
        assert_eq!(shared.borrow().tab("tab").unwrap().label.as_deref(), None);
        assert_eq!(
            shared
                .borrow()
                .pane("pane")
                .unwrap()
                .display_name
                .as_deref(),
            None
        );
        store.apply_batch(batch).unwrap();
        let restored = store.load_restored_state().unwrap();
        assert_eq!(restored.model.tab("tab").unwrap().label.as_deref(), None);
        assert_eq!(
            restored.model.pane("pane").unwrap().display_name.as_deref(),
            None
        );

        let mut named_topology = topology_snapshot(
            &["workspace"],
            &[("tab", "workspace")],
            &[("pane", "workspace", "tab")],
        );
        named_topology.tabs[0].label = Some("snapshot tab".to_owned());
        named_topology.panes[0].display_name = Some("snapshot pane".to_owned());
        let batch = reducer
            .reconcile_gap(ReconcileBatch {
                topology: named_topology,
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        assert_eq!(
            shared.borrow().tab("tab").unwrap().label.as_deref(),
            Some("snapshot tab")
        );
        assert_eq!(
            shared
                .borrow()
                .pane("pane")
                .unwrap()
                .display_name
                .as_deref(),
            Some("snapshot pane")
        );
        store.apply_batch(batch).unwrap();
        let restored = store.load_restored_state().unwrap();
        assert_eq!(
            restored.model.tab("tab").unwrap().label.as_deref(),
            Some("snapshot tab")
        );
        assert_eq!(
            restored.model.pane("pane").unwrap().display_name.as_deref(),
            Some("snapshot pane")
        );
    }

    fn native_snapshot(sid: &str) -> TopologySnapshot {
        TopologySnapshot {
            workspaces: vec![Workspace {
                workspace_id: "workspace-1".to_owned(),
            }],
            tabs: Vec::new(),
            panes: vec![PaneSnapshot {
                pane_id: "pane-1".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                tab_id: "tab-1".to_owned(),
                terminal_id: "terminal-1".to_owned(),
                display_name: None,
                agent: Some(SnapshotAgent {
                    agent_name: "codex".to_owned(),
                    status: PaneAgentStatus::Working,
                }),
                agent_session: Some(AgentSessionReference {
                    source: "herdr".to_owned(),
                    agent: "codex".to_owned(),
                    kind: AgentSessionReferenceKind::Id,
                    value: sid.to_owned(),
                }),
            }],
        }
    }

    fn path_snapshot(path: &str) -> TopologySnapshot {
        let mut snapshot = native_snapshot(path);
        snapshot.panes[0].agent_session.as_mut().unwrap().kind = AgentSessionReferenceKind::Path;
        snapshot
    }

    fn provisional_snapshot() -> TopologySnapshot {
        let mut snapshot = native_snapshot("");
        snapshot.panes[0].agent.as_mut().unwrap().agent_name = "unknown".to_owned();
        snapshot.panes[0].agent_session = None;
        snapshot
    }

    #[test]
    fn path_keyed_occupant_reuses_run_and_execution() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: path_snapshot("/tmp/session.jsonl"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let (first_run_id, first_execution_id, first_agent_node_id, first_display_ordinal) = {
            let model = shared.borrow();
            let execution = model
                .executions()
                .find(|execution| !execution.state.is_terminal())
                .unwrap();
            let agent_node = model.agent_nodes().next().unwrap();
            assert_eq!(model.task_runs().count(), 1);
            assert_eq!(model.executions().count(), 1);
            (
                execution.task_run_id,
                execution.execution_id.clone(),
                agent_node.agent_node_id.clone(),
                agent_node.display_ordinal,
            )
        };

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: path_snapshot("/tmp/session.jsonl"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let model = shared.borrow();
        let live_executions = model
            .executions()
            .filter(|execution| !execution.state.is_terminal())
            .collect::<Vec<_>>();

        assert_eq!(model.task_runs().count(), 1);
        assert_eq!(model.executions().count(), 1);
        assert_eq!(live_executions.len(), 1);
        assert_eq!(live_executions[0].task_run_id, first_run_id);
        assert_eq!(live_executions[0].execution_id, first_execution_id);
        assert_eq!(model.agent_nodes().count(), 1);
        let agent_node = model.agent_nodes().next().unwrap();
        assert_eq!(agent_node.agent_node_id, first_agent_node_id);
        assert_eq!(agent_node.display_ordinal, first_display_ordinal);
    }

    #[test]
    fn provisional_occupant_keeps_minting_per_observation() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: provisional_snapshot(),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let first_execution_id = shared
            .borrow()
            .executions()
            .find(|execution| !execution.state.is_terminal())
            .unwrap()
            .execution_id
            .clone();

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: provisional_snapshot(),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let model = shared.borrow();
        let live_execution = model
            .executions()
            .find(|execution| !execution.state.is_terminal())
            .unwrap();

        assert_eq!(model.task_runs().count(), 2);
        assert_eq!(model.executions().count(), 2);
        assert_ne!(live_execution.execution_id, first_execution_id);
    }

    #[test]
    fn reconnect_reuses_gap_execution_same_occupant() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: native_snapshot("sid-1"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let (first_count, first_execution_id) = {
            let model = shared.borrow();
            let execution = model
                .executions()
                .find(|execution| !execution.state.is_terminal())
                .unwrap();
            (model.executions().count(), execution.execution_id.clone())
        };

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: native_snapshot("sid-1"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let model = shared.borrow();
        let execution = model
            .executions()
            .find(|execution| !execution.state.is_terminal())
            .unwrap();

        assert_eq!(model.executions().count(), first_count);
        assert_eq!(execution.execution_id, first_execution_id);
    }

    #[test]
    fn occupant_change_mints_new_execution() {
        let first_run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            first_run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_execution(execution(
            first_run_id,
            "gap-execution-existing",
            ExecState::Working,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: native_snapshot("sid-1"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let first_execution_id = shared
            .borrow()
            .executions()
            .find(|execution| !execution.state.is_terminal())
            .unwrap()
            .execution_id
            .clone();

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: native_snapshot("sid-2"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let model = shared.borrow();
        let new_execution = model
            .executions()
            .find(|execution| !execution.state.is_terminal())
            .unwrap();
        let previous_execution = model.execution(&first_execution_id).unwrap();

        assert_ne!(new_execution.execution_id, first_execution_id);
        // Store upserts rewrite `task_run_id` on execution-id conflict, so preserving the old
        // execution id and owner is what protects the previous occupant's history.
        assert_eq!(previous_execution.task_run_id, first_run_id);
        assert_eq!(previous_execution.state, ExecState::Ended);
        assert_ne!(new_execution.task_run_id, first_run_id);
        assert_eq!(model.executions().count(), 2);
    }

    #[test]
    fn terminal_executions_not_repersisted() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_execution(execution(run_id, "pre-gap", ExecState::Working));
        let (mut reducer, _shared) = Reducer::new(restored(model, 2));

        let first = reducer
            .reconcile_gap(ReconcileBatch {
                topology: TopologySnapshot::default(),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let second = reducer
            .reconcile_gap(ReconcileBatch {
                topology: TopologySnapshot::default(),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let execution_ops = |batch: &[PersistOp]| {
            batch
                .iter()
                .filter(|operation| matches!(operation, PersistOp::UpsertExecution(_)))
                .count()
        };

        assert_eq!(execution_ops(&first), 1);
        assert_eq!(execution_ops(&second), 0);
    }

    #[test]
    fn done_maps_to_idle_never_ended() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_execution(execution(run_id, "execution-1", ExecState::Working));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let mut done = metadata("done", 1_000);
        done.source_event_type = "done".to_owned();

        reducer
            .apply(NormalizedEvent::AgentStatusChanged {
                metadata: done,
                execution_id: "execution-1".to_owned(),
                state: ExecState::Ended,
            })
            .unwrap();

        assert_eq!(
            shared.borrow().execution("execution-1").unwrap().state,
            ExecState::Idle
        );
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::Running
        );
    }

    #[test]
    fn stale_grace_live_observation_only() {
        let live_run = RunId::new();
        let mut live_model = DomainModel::default();
        live_model.insert_task_run(run(
            live_run,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "live-sid".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        live_model.insert_execution(execution(live_run, "live-execution", ExecState::Working));
        let (mut live_reducer, live_shared) = Reducer::new(restored(live_model, 2));

        live_reducer
            .apply(status_event(
                "missing-1",
                1_000,
                "live-execution",
                ExecState::Stale { since_ms: 0 },
            ))
            .unwrap();
        live_reducer
            .apply(status_event(
                "missing-2",
                30_999,
                "live-execution",
                ExecState::Stale { since_ms: 0 },
            ))
            .unwrap();
        assert_eq!(
            live_shared
                .borrow()
                .execution("live-execution")
                .unwrap()
                .state,
            ExecState::Stale { since_ms: 1_000 }
        );

        live_reducer
            .apply(status_event(
                "missing-3",
                31_000,
                "live-execution",
                ExecState::Stale { since_ms: 0 },
            ))
            .unwrap();
        assert_eq!(
            live_shared
                .borrow()
                .execution("live-execution")
                .unwrap()
                .state,
            ExecState::Ended
        );

        let gap_run = RunId::new();
        let mut gap_model = DomainModel::default();
        gap_model.insert_task_run(run(
            gap_run,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "gap-sid".to_owned(),
            },
            2,
            TaskState::Running,
        ));
        gap_model.insert_execution(execution(gap_run, "gap-execution", ExecState::Working));
        let (mut gap_reducer, gap_shared) = Reducer::new(restored(gap_model, 3));

        gap_reducer
            .reconcile_gap(ReconcileBatch {
                topology: TopologySnapshot::default(),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();

        assert_eq!(
            gap_shared
                .borrow()
                .execution("gap-execution")
                .unwrap()
                .state,
            ExecState::Ended
        );
        assert_eq!(
            gap_shared.borrow().task_run(&gap_run).unwrap().state,
            TaskState::EndedUnknown
        );
    }

    #[test]
    fn no_transient_ended_unknown_inside_reconcile() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            7,
            TaskState::Running,
        ));
        model.insert_execution(execution(run_id, "pre-gap", ExecState::Working));
        let (mut reducer, shared) = Reducer::new(restored(model, 8));

        let batch = reducer
            .reconcile_gap(ReconcileBatch {
                topology: native_snapshot("sid-1"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::Running
        );
        // The same occupant reuses its pre-gap execution while remaining live throughout the
        // reconciliation batch.
        assert!(shared.borrow().executions().any(|value| {
            value.task_run_id == run_id
                && value.execution_id == "pre-gap"
                && !value.state.is_terminal()
        }));
        assert!(!batch.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTaskRun(value)
                if value.task_run.run_id == run_id
                    && value.task_run.state == TaskState::EndedUnknown
        )));
    }

    #[test]
    fn reactivation_on_new_execution() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            1,
            TaskState::EndedUnknown,
        ));
        model.insert_execution(execution(run_id, "ended", ExecState::Ended));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let mut begin_metadata = metadata("begin", 2_000);
        begin_metadata.provider = Some(Provider::Codex);
        begin_metadata.native_session_id = Some("sid-1".to_owned());

        reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: begin_metadata,
                execution: execution(run_id, "resumed", ExecState::Working),
            })
            .unwrap();

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::Running
        );
    }

    #[test]
    fn ordinal_assigned_once_survivor_keeps_it() {
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let provisional_key = RunKey::Provisional {
            terminal_id: "terminal-1".to_owned(),
            start_ms: 1_000,
            seq: 1,
        };
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            survivor,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid-1".to_owned(),
            },
            10,
            TaskState::Running,
        ));
        model.insert_task_run(run_with_controller_evidence(
            absorbed,
            provisional_key.clone(),
            11,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 12));
        let mut begin_metadata = metadata("bind", 2_000);
        begin_metadata.provider = Some(Provider::Codex);
        begin_metadata.native_session_id = Some("sid-1".to_owned());

        let outcome = reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: begin_metadata,
                execution: execution(absorbed, "execution-1", ExecState::Working),
            })
            .unwrap();
        let ApplyOutcome::Applied(batch) = outcome else {
            panic!("non-conflicting binding should apply");
        };

        assert!(batch.iter().any(|operation| matches!(
            operation,
            PersistOp::MergeTaskRuns {
                survivor: actual_survivor,
                absorbed: actual_absorbed,
            } if *actual_survivor == survivor && *actual_absorbed == absorbed
        )));
        assert_eq!(
            shared.borrow().task_run(&survivor).unwrap().display_ordinal,
            DisplayOrdinal::new(10)
        );
        assert!(shared.borrow().task_run(&absorbed).is_none());
        assert_eq!(
            shared
                .borrow()
                .task_run_by_key(&provisional_key)
                .unwrap()
                .run_id,
            survivor
        );
    }

    #[test]
    fn ordinal_allocator_seeds_from_restore() {
        let first = RunId::new();
        let second = RunId::new();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 42));
        let mut first_metadata = metadata("first", 1_000);
        first_metadata.source = "controller".to_owned();
        first_metadata.source_event_type = "task_started".to_owned();
        first_metadata.task_run_id = Some(first);
        first_metadata.task_state = Some(TaskState::Running);
        let mut second_metadata = first_metadata.clone();
        second_metadata.event_id = "second".to_owned();
        second_metadata.task_run_id = Some(second);

        reducer
            .apply(topology_event(first_metadata, "workspace-1"))
            .unwrap();
        reducer
            .apply(topology_event(second_metadata, "workspace-1"))
            .unwrap();

        assert_eq!(
            shared.borrow().task_run(&first).unwrap().display_ordinal,
            DisplayOrdinal::new(42)
        );
        assert_eq!(
            shared.borrow().task_run(&second).unwrap().display_ordinal,
            DisplayOrdinal::new(44)
        );
    }

    #[test]
    fn state_refresh_emits_no_reorder() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            run_id,
            RunKey::Controller("controller-1".to_owned()),
            9,
            TaskState::Running,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 10));
        let mut refresh = metadata("refresh", 3_000);
        refresh.source = "controller".to_owned();
        refresh.source_event_type = "task_started".to_owned();
        refresh.task_run_id = Some(run_id);
        refresh.task_state = Some(TaskState::Running);

        reducer
            .apply(topology_event(refresh, "workspace-1"))
            .unwrap();

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().display_ordinal,
            DisplayOrdinal::new(9)
        );
    }

    #[test]
    fn relationship_events_do_not_make_controller_owned() {
        let parent = RunId::new();
        let prerequisite = RunId::new();
        let subject = RunId::new();
        let mut relationship = metadata("relationships", 4_000);
        relationship.source = "controller".to_owned();
        relationship.source_event_type = "dispatch".to_owned();
        relationship.task_run_id = Some(subject);
        relationship.execution_parent = Some(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: subject,
        });
        relationship.dependency = Some(DependencyEdge {
            prerequisite_run_id: prerequisite,
            dependent_run_id: subject,
        });
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        reducer
            .apply(topology_event(relationship, "workspace-1"))
            .unwrap();

        assert!([parent, prerequisite, subject].iter().all(|run_id| {
            let snapshot = shared.borrow();
            let task_run = snapshot.task_run(run_id).unwrap();
            !task_run.has_controller_task_state_event && task_run.state == TaskState::Queued
        }));
        assert_eq!(shared.borrow().execution_edges().count(), 1);
        assert_eq!(shared.borrow().dependency_edges().count(), 1);
    }

    #[test]
    fn snapshot_published_after_each_apply() {
        let (mut reducer, mut shared) = Reducer::new(restored(DomainModel::default(), 1));
        let initial = Arc::clone(&shared.borrow());

        reducer
            .apply(topology_event(metadata("one", 1_000), "workspace-1"))
            .unwrap();
        assert!(shared.has_changed().unwrap());
        let first = Arc::clone(&shared.borrow_and_update());
        assert!(!Arc::ptr_eq(&initial, &first));
        assert!(first.workspace("workspace-1").is_some());

        reducer
            .apply(topology_event(metadata("two", 2_000), "workspace-2"))
            .unwrap();
        assert!(shared.has_changed().unwrap());
        let second = Arc::clone(&shared.borrow_and_update());
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(second.workspace("workspace-1").is_some());
        assert!(second.workspace("workspace-2").is_some());
    }

    #[test]
    fn stale_sweep_ends_after_grace() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "stale-sweep".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_execution(execution(
            run_id,
            "stale-execution",
            ExecState::Stale { since_ms: 1_000 },
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(reducer.sweep_stale(30_999).is_empty());
        assert_eq!(
            shared.borrow().execution("stale-execution").unwrap().state,
            ExecState::Stale { since_ms: 1_000 }
        );
        let persist = reducer.sweep_stale(31_000);

        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertExecution(value)
                if value.execution.execution_id == "stale-execution"
                    && value.execution.state == ExecState::Ended
        )));
        assert_eq!(
            shared.borrow().execution("stale-execution").unwrap().state,
            ExecState::Ended
        );
    }

    #[test]
    fn hours_old_replayed_running_run_closes_with_only_terminal_execution() {
        let anchor_ms = 100;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let reducer = Reducer::new(restored(DomainModel::default(), 1)).0;
        let started = reducer
            .validate_controller_event(&provider_lane_event(
                "aged-replay-started",
                "aged-replay",
                ControllerEventKind::TaskStarted,
                anchor_ms,
                now_ms,
            ))
            .unwrap();
        let run_id = started
            .post_model
            .task_run_by_key(&RunKey::Controller("aged-replay".to_owned()))
            .unwrap()
            .run_id;
        let mut model = started.post_model;
        model.insert_execution(execution(run_id, "aged-terminal", ExecState::Ended));
        let (mut reducer, shared) = Reducer::new(RestoredState {
            model,
            next_ordinal: started.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });

        reducer.sweep_stale(now_ms);

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );
    }

    #[test]
    fn fact_timed_sweep_uses_post_expiry_live_execution_set() {
        let anchor_ms = 200;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let working = RunId::new();
        let terminal = RunId::new();
        let expiring = RunId::new();
        let mut model = DomainModel::default();
        for (run_id, key, ordinal) in [
            (working, RunKey::Controller("fact-working".to_owned()), 1),
            (
                terminal,
                RunKey::Provisional {
                    terminal_id: "fact-terminal".to_owned(),
                    start_ms: anchor_ms,
                    seq: 2,
                },
                2,
            ),
            (expiring, RunKey::Controller("fact-expiring".to_owned()), 3),
        ] {
            let mut task_run =
                run_with_controller_evidence(run_id, key, ordinal, TaskState::Running);
            task_run.created_at_ms = Some(anchor_ms);
            task_run.updated_at_ms = Some(anchor_ms);
            model.insert_task_run(task_run);
        }
        model.insert_execution(execution(working, "fact-working", ExecState::Working));
        model.insert_execution(execution(terminal, "fact-terminal", ExecState::Ended));
        model.insert_execution(execution(
            expiring,
            "fact-expiring",
            ExecState::Stale {
                since_ms: now_ms - super::STALE_GRACE_MS,
            },
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 4));

        let publish_count = reducer.publish_count.get();
        let persist = reducer.sweep_stale(now_ms);

        assert_eq!(reducer.publish_count.get(), publish_count + 1);
        let snapshot = shared.borrow();
        assert_eq!(
            snapshot.task_run(&working).unwrap().state,
            TaskState::Running
        );
        for run_id in [terminal, expiring] {
            let task_run = snapshot.task_run(&run_id).unwrap();
            assert_eq!(task_run.state, TaskState::EndedUnknown);
            assert_eq!(task_run.updated_at_ms, Some(anchor_ms));
            assert_eq!(task_run.finished_at_ms, Some(anchor_ms));
        }
        assert_eq!(
            snapshot.execution("fact-expiring").unwrap().state,
            ExecState::Ended
        );
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertExecution(value)
                if value.execution.execution_id == "fact-expiring"
                    && value.execution.state == ExecState::Ended
                    && value.ended_at_ms == Some(now_ms)
        )));
        assert_eq!(
            persist
                .iter()
                .filter(|operation| matches!(
                    operation,
                    PersistOp::UpsertTaskRun(value)
                        if [terminal, expiring].contains(&value.task_run.run_id)
                            && value.task_run.state == TaskState::EndedUnknown
                            && value.updated_at_ms == anchor_ms
                ))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn restored_hook_ownership_protects_only_owned_stale_running_root() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let anchor_ms = 200;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut started = controller_event(
            "restored-owned-hook-started",
            "restored-owned-hook",
            ControllerEventKind::TaskStarted,
        );
        started.metadata.source = "hook".to_owned();
        started.metadata.timestamp_ms = unix_now_ms();
        started.metadata.receipt_time_ms = started.metadata.timestamp_ms;
        commit_controller(&mut reducer, &mut writer, started).await;
        lifecycle.shutdown().await.unwrap();

        let store = open_reader(&root).unwrap();
        let ownership = store.non_lane_task_state_runs().unwrap();
        let mut restored = store.load_restored_state().unwrap();
        let owned = restored
            .model
            .task_run_by_key(&RunKey::Controller("restored-owned-hook".to_owned()))
            .unwrap()
            .run_id;
        assert!(
            ownership.contains(&owned),
            "restored ownership: {ownership:?}"
        );
        let mut owned_run = restored.model.task_run(&owned).unwrap().clone();
        owned_run.created_at_ms = Some(anchor_ms);
        owned_run.updated_at_ms = Some(anchor_ms);
        restored.model.insert_task_run(owned_run);
        let unowned = RunId::new();
        let mut unowned_run = run_with_controller_evidence(
            unowned,
            RunKey::Controller("restored-unowned-lane".to_owned()),
            restored.next_ordinal,
            TaskState::Running,
        );
        unowned_run.created_at_ms = Some(anchor_ms);
        unowned_run.updated_at_ms = Some(anchor_ms);
        restored.model.insert_task_run(unowned_run);
        restored.next_ordinal += 1;
        let (mut reducer, shared) = Reducer::new(restored);
        reducer.restore_non_lane_task_state_runs(ownership);

        reducer.sweep_stale(now_ms);

        let snapshot = shared.borrow();
        assert_eq!(snapshot.task_run(&owned).unwrap().state, TaskState::Running);
        assert_eq!(
            snapshot.task_run(&unowned).unwrap().state,
            TaskState::EndedUnknown
        );
    }

    #[test]
    fn stale_provisional_run_closes_and_dismisses_while_unanchored_stays_open() {
        let anchor_ms = 300;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let anchored = RunId::new();
        let unanchored = RunId::new();
        let mut anchored_run = run(
            anchored,
            RunKey::Provisional {
                terminal_id: "anchored-terminal".to_owned(),
                start_ms: anchor_ms,
                seq: 1,
            },
            1,
            TaskState::Running,
        );
        anchored_run.created_at_ms = Some(anchor_ms);
        anchored_run.updated_at_ms = Some(anchor_ms);
        let unanchored_run = run(
            unanchored,
            RunKey::Provisional {
                terminal_id: "unanchored-terminal".to_owned(),
                start_ms: anchor_ms,
                seq: 2,
            },
            2,
            TaskState::Running,
        );
        let mut model = DomainModel::default();
        model.insert_task_run(anchored_run);
        model.insert_task_run(unanchored_run);
        model.insert_execution(execution(anchored, "anchored-ended", ExecState::Ended));
        model.insert_execution(execution(unanchored, "unanchored-ended", ExecState::Ended));
        let (mut reducer, shared) = Reducer::new(restored(model, 3));

        reducer.sweep_stale(now_ms);
        assert_eq!(
            shared.borrow().task_run(&anchored).unwrap().state,
            TaskState::EndedUnknown
        );
        assert_eq!(
            shared.borrow().task_run(&unanchored).unwrap().state,
            TaskState::Running
        );

        reducer.apply_operator_command(OperatorCommand::DismissClearable, now_ms + 1);
        let snapshot = shared.borrow();
        assert_eq!(
            snapshot.task_run(&anchored).unwrap().dismissed_at_ms,
            Some(now_ms + 1)
        );
        assert_eq!(
            snapshot.task_run(&unanchored).unwrap().dismissed_at_ms,
            None
        );
    }

    #[test]
    fn fact_timed_closure_uses_last_fact_while_queued_closure_uses_sweep_time() {
        let anchor_ms = 400;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let queued = RunId::new();
        let running = RunId::new();
        let mut queued_run = run_with_controller_evidence(
            queued,
            RunKey::Controller("queued-anchor".to_owned()),
            1,
            TaskState::Queued,
        );
        queued_run.created_at_ms = Some(anchor_ms);
        queued_run.updated_at_ms = Some(anchor_ms);
        let mut running_run = run_with_controller_evidence(
            running,
            RunKey::Controller("running-anchor".to_owned()),
            2,
            TaskState::Running,
        );
        running_run.created_at_ms = Some(anchor_ms);
        running_run.updated_at_ms = Some(anchor_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(queued_run);
        model.insert_task_run(running_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 3));

        reducer.sweep_stale(now_ms);

        let snapshot = shared.borrow();
        let queued = snapshot.task_run(&queued).unwrap();
        let running = snapshot.task_run(&running).unwrap();
        assert_eq!(queued.state, TaskState::EndedUnknown);
        assert_eq!(queued.finished_at_ms, Some(now_ms));
        assert_eq!(running.state, TaskState::EndedUnknown);
        assert_eq!(running.finished_at_ms, Some(anchor_ms));
    }

    #[test]
    fn fact_timed_running_closure_reopens_on_newer_task_started() {
        let anchor_ms = 500;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let run_id = RunId::new();
        let mut task_run = run_with_controller_evidence(
            run_id,
            RunKey::Controller("fact-reopen".to_owned()),
            1,
            TaskState::Running,
        );
        task_run.created_at_ms = Some(anchor_ms);
        task_run.updated_at_ms = Some(anchor_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        reducer.sweep_stale(now_ms);
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );

        let reopened = reducer
            .validate_controller_event(&provider_lane_event(
                "fact-reopen-started",
                "fact-reopen",
                ControllerEventKind::TaskStarted,
                now_ms + 1,
                now_ms + 100,
            ))
            .unwrap();
        let run = reopened.post_model.task_run(&run_id).unwrap();
        assert_eq!(run.state, TaskState::Running);
        assert_eq!(run.finished_at_ms, None);
        assert_eq!(run.updated_at_ms, Some(now_ms + 1));
    }

    #[test]
    fn stale_sweep_closes_dispatch_only_run_at_updated_inactivity_boundary() {
        let anchor_ms = 100;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let dispatch_only = RunId::new();
        let stale_run = RunId::new();
        let mut task_run = run_with_controller_evidence(
            dispatch_only,
            RunKey::Controller("updated-anchor".to_owned()),
            1,
            TaskState::Queued,
        );
        task_run.created_at_ms = Some(50);
        task_run.updated_at_ms = Some(anchor_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        model.insert_task_run(run(
            stale_run,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "updated-anchor-stale".to_owned(),
            },
            2,
            TaskState::Running,
        ));
        model.insert_execution(execution(
            stale_run,
            "updated-anchor-stale",
            ExecState::Stale {
                since_ms: now_ms - super::STALE_GRACE_MS,
            },
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 3));

        let publish_count = reducer.publish_count.get();
        assert!(reducer.sweep_stale(now_ms - 1).is_empty());
        assert_eq!(reducer.publish_count.get(), publish_count);
        assert_eq!(
            shared.borrow().task_run(&dispatch_only).unwrap().state,
            TaskState::Queued
        );

        let persist = reducer.sweep_stale(now_ms);

        let snapshot = shared.borrow();
        let closed = snapshot.task_run(&dispatch_only).unwrap();
        assert_eq!(closed.state, TaskState::EndedUnknown);
        assert_eq!(closed.updated_at_ms, Some(now_ms));
        assert_eq!(closed.finished_at_ms, Some(now_ms));
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTaskRun(value)
                if value.task_run.run_id == dispatch_only
                    && value.task_run.state == TaskState::EndedUnknown
                    && value.task_run.finished_at_ms == Some(now_ms)
        )));
    }

    #[test]
    fn stale_sweep_uses_created_at_when_dispatch_only_run_has_no_update() {
        let anchor_ms = 200;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let dispatch_only = RunId::new();
        let stale_run = RunId::new();
        let mut task_run = run_with_controller_evidence(
            dispatch_only,
            RunKey::Controller("created-anchor".to_owned()),
            1,
            TaskState::Queued,
        );
        task_run.created_at_ms = Some(anchor_ms);
        task_run.updated_at_ms = None;
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        model.insert_task_run(run(
            stale_run,
            RunKey::Native {
                provider: Provider::Claude,
                sid: "created-anchor-stale".to_owned(),
            },
            2,
            TaskState::Running,
        ));
        model.insert_execution(execution(
            stale_run,
            "created-anchor-stale",
            ExecState::Stale {
                since_ms: now_ms - super::STALE_GRACE_MS,
            },
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 3));

        let persist = reducer.sweep_stale(now_ms);

        let snapshot = shared.borrow();
        let closed = snapshot.task_run(&dispatch_only).unwrap();
        assert_eq!(closed.state, TaskState::EndedUnknown);
        assert_eq!(closed.finished_at_ms, Some(now_ms));
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTaskRun(value) if value.task_run.run_id == dispatch_only
        )));
    }

    #[test]
    fn stale_sweep_leaves_unanchored_dispatch_only_run_open() {
        let now_ms = crate::activity::headless_inactivity_ms() + 500;
        let unanchored = RunId::new();
        let anchored = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            unanchored,
            RunKey::Controller("unanchored".to_owned()),
            1,
            TaskState::Queued,
        ));
        let mut anchored_run = run_with_controller_evidence(
            anchored,
            RunKey::Controller("anchored-control".to_owned()),
            2,
            TaskState::Queued,
        );
        anchored_run.updated_at_ms = Some(500);
        model.insert_task_run(anchored_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 3));

        let persist = reducer.sweep_stale(now_ms);

        let snapshot = shared.borrow();
        assert_eq!(
            snapshot.task_run(&unanchored).unwrap().state,
            TaskState::Queued
        );
        assert_eq!(
            snapshot.task_run(&anchored).unwrap().state,
            TaskState::EndedUnknown
        );
        assert!(persist.iter().all(|operation| !matches!(
            operation,
            PersistOp::UpsertTaskRun(value) if value.task_run.run_id == unanchored
        )));
    }

    #[test]
    fn stale_sweep_closes_controller_queued_and_unowned_running_runs() {
        let anchor_ms = 700;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let queued = RunId::new();
        let running = RunId::new();
        let blocked = RunId::new();
        let native = RunId::new();
        let mut model = DomainModel::default();
        for (ordinal, (run_id, name, state)) in [
            (queued, "queued", TaskState::Queued),
            (running, "running", TaskState::Running),
            (blocked, "blocked", TaskState::Blocked),
        ]
        .into_iter()
        .enumerate()
        {
            let mut task_run = run_with_controller_evidence(
                run_id,
                RunKey::Controller(name.to_owned()),
                ordinal as i64 + 1,
                state,
            );
            task_run.updated_at_ms = Some(anchor_ms);
            model.insert_task_run(task_run);
        }
        let mut native_run = run(
            native,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "native-queued".to_owned(),
            },
            4,
            TaskState::Queued,
        );
        native_run.updated_at_ms = Some(anchor_ms);
        model.insert_task_run(native_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 5));

        reducer.sweep_stale(now_ms);

        let snapshot = shared.borrow();
        assert_eq!(
            snapshot.task_run(&queued).unwrap().state,
            TaskState::EndedUnknown
        );
        assert_eq!(
            snapshot.task_run(&running).unwrap().state,
            TaskState::EndedUnknown
        );
        assert_eq!(
            snapshot.task_run(&blocked).unwrap().state,
            TaskState::Blocked
        );
        assert_eq!(snapshot.task_run(&native).unwrap().state, TaskState::Queued);
    }

    #[test]
    fn stale_sweep_leaves_dispatch_only_run_with_execution_open() {
        let anchor_ms = 800;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let attached = RunId::new();
        let unattached = RunId::new();
        let mut model = DomainModel::default();
        for (ordinal, (run_id, name)) in [(attached, "attached"), (unattached, "unattached")]
            .into_iter()
            .enumerate()
        {
            let mut task_run = run_with_controller_evidence(
                run_id,
                RunKey::Controller(name.to_owned()),
                ordinal as i64 + 1,
                TaskState::Queued,
            );
            task_run.updated_at_ms = Some(anchor_ms);
            model.insert_task_run(task_run);
        }
        model.insert_execution(execution(attached, "live-attached", ExecState::Working));
        let (mut reducer, shared) = Reducer::new(restored(model, 3));

        reducer.sweep_stale(now_ms);

        let snapshot = shared.borrow();
        assert_eq!(
            snapshot.task_run(&attached).unwrap().state,
            TaskState::Queued
        );
        assert_eq!(
            snapshot.task_run(&unattached).unwrap().state,
            TaskState::EndedUnknown
        );
    }

    #[test]
    fn stale_sweep_leaves_dismissed_and_terminal_dispatch_only_runs_unchanged() {
        let anchor_ms = 900;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let eligible = RunId::new();
        let dismissed = RunId::new();
        let terminal_states = [
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Cancelled,
            TaskState::EndedUnknown,
        ];
        let mut model = DomainModel::default();
        let mut eligible_run = run_with_controller_evidence(
            eligible,
            RunKey::Controller("eligible".to_owned()),
            1,
            TaskState::Queued,
        );
        eligible_run.updated_at_ms = Some(anchor_ms);
        model.insert_task_run(eligible_run);
        let mut dismissed_run = run_with_controller_evidence(
            dismissed,
            RunKey::Controller("dismissed-queued".to_owned()),
            2,
            TaskState::Queued,
        );
        dismissed_run.updated_at_ms = Some(anchor_ms);
        dismissed_run.dismissed_at_ms = Some(anchor_ms + 1);
        let expected_dismissed = dismissed_run.clone();
        model.insert_task_run(dismissed_run);
        let mut expected_terminal = HashMap::new();
        for (index, state) in terminal_states.into_iter().enumerate() {
            let run_id = RunId::new();
            let mut task_run = run_with_controller_evidence(
                run_id,
                RunKey::Controller(format!("terminal-{state:?}")),
                index as i64 + 3,
                state,
            );
            task_run.updated_at_ms = Some(anchor_ms);
            task_run.finished_at_ms = Some(anchor_ms);
            expected_terminal.insert(run_id, task_run.clone());
            model.insert_task_run(task_run);
        }
        let (mut reducer, shared) = Reducer::new(restored(model, 7));

        reducer.sweep_stale(now_ms);

        let snapshot = shared.borrow();
        assert_eq!(
            snapshot.task_run(&eligible).unwrap().state,
            TaskState::EndedUnknown
        );
        assert_eq!(snapshot.task_run(&dismissed), Some(&expected_dismissed));
        for (run_id, expected) in expected_terminal {
            assert_eq!(snapshot.task_run(&run_id), Some(&expected));
        }
    }

    #[test]
    fn stale_sweep_closes_dispatch_only_run_without_stale_executions() {
        let anchor_ms = 1_000;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let run_id = RunId::new();
        let mut task_run = run_with_controller_evidence(
            run_id,
            RunKey::Controller("no-executions".to_owned()),
            1,
            TaskState::Queued,
        );
        task_run.updated_at_ms = Some(anchor_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        assert_eq!(model.executions().count(), 0);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));
        let publish_count = reducer.publish_count.get();

        let persist = reducer.sweep_stale(now_ms);

        assert_eq!(persist.len(), 1);
        assert_eq!(reducer.publish_count.get() - publish_count, 1);
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );
    }

    #[test]
    fn stale_sweep_combines_execution_and_dispatch_only_closures_in_one_publish() {
        let anchor_ms = 1_100;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let dispatch_only = RunId::new();
        let stale_run = RunId::new();
        let mut dispatch_run = run_with_controller_evidence(
            dispatch_only,
            RunKey::Controller("combined-dispatch".to_owned()),
            1,
            TaskState::Queued,
        );
        dispatch_run.updated_at_ms = Some(anchor_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(dispatch_run);
        model.insert_task_run(run(
            stale_run,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "combined-stale".to_owned(),
            },
            2,
            TaskState::Running,
        ));
        model.insert_execution(execution(
            stale_run,
            "combined-stale",
            ExecState::Stale {
                since_ms: now_ms - super::STALE_GRACE_MS,
            },
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 3));
        let publish_count = reducer.publish_count.get();

        let persist = reducer.sweep_stale(now_ms);

        assert_eq!(reducer.publish_count.get() - publish_count, 1);
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertExecution(value)
                if value.execution.execution_id == "combined-stale"
                    && value.execution.state == ExecState::Ended
        )));
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTaskRun(value) if value.task_run.run_id == dispatch_only
        )));
        let snapshot = shared.borrow();
        assert_eq!(
            snapshot.task_run(&dispatch_only).unwrap().state,
            TaskState::EndedUnknown
        );
        assert_eq!(
            snapshot.execution("combined-stale").unwrap().state,
            ExecState::Ended
        );
    }

    #[tokio::test]
    async fn dispatch_only_closure_reopens_on_controller_task_started() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let anchor_ms = 1_200;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let run_id = RunId::new();
        let mut task_run = run_with_controller_evidence(
            run_id,
            RunKey::Controller("reopen-started".to_owned()),
            1,
            TaskState::Queued,
        );
        task_run.updated_at_ms = Some(anchor_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(!reducer.sweep_stale(now_ms).is_empty());
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );

        let mut started = controller_event(
            "reopen-started-event",
            "reopen-started",
            ControllerEventKind::TaskStarted,
        );
        started.metadata.timestamp_ms = now_ms + 1;
        started.metadata.receipt_time_ms = now_ms + 1;
        commit_controller(&mut reducer, &mut writer, started).await;

        let snapshot = shared.borrow();
        let reopened = snapshot.task_run(&run_id).unwrap();
        assert_eq!(reopened.state, TaskState::Running);
        assert_eq!(reopened.finished_at_ms, None);
        assert_eq!(reopened.updated_at_ms, Some(now_ms + 1));
        lifecycle.shutdown().await.unwrap();
    }

    #[test]
    fn dispatch_only_closure_reopens_on_execution_begin() {
        let anchor_ms = 1_300;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let run_id = RunId::new();
        let mut task_run = run_with_controller_evidence(
            run_id,
            RunKey::Controller("reopen-execution".to_owned()),
            1,
            TaskState::Queued,
        );
        task_run.updated_at_ms = Some(anchor_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(!reducer.sweep_stale(now_ms).is_empty());
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );
        let mut begin_metadata = metadata("reopen-execution-event", now_ms + 1);
        begin_metadata.terminal_id = Some("terminal-reopen".to_owned());

        let outcome = reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: begin_metadata,
                execution: Execution {
                    execution_id: "reopen-execution-live".to_owned(),
                    pane_id: "pane-reopen".to_owned(),
                    terminal_id: "terminal-reopen".to_owned(),
                    task_run_id: run_id,
                    state: ExecState::Working,
                },
            })
            .unwrap();
        let ApplyOutcome::Applied(persist) = outcome else {
            panic!("execution begin must apply");
        };

        let snapshot = shared.borrow();
        let reopened = snapshot.task_run(&run_id).unwrap();
        assert_eq!(reopened.state, TaskState::Running);
        assert_eq!(reopened.finished_at_ms, None);
        assert_eq!(reopened.updated_at_ms, Some(now_ms + 1));
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTaskRun(value)
                if value.task_run.run_id == run_id
                    && value.task_run.state == TaskState::Running
        )));
    }

    #[test]
    fn dispatch_only_closure_is_immediately_dismissible() {
        let anchor_ms = 1_400;
        let now_ms = anchor_ms + crate::activity::headless_inactivity_ms();
        let run_id = RunId::new();
        let mut task_run = run_with_controller_evidence(
            run_id,
            RunKey::Controller("immediately-clearable".to_owned()),
            1,
            TaskState::Queued,
        );
        task_run.updated_at_ms = Some(anchor_ms);
        let mut model = DomainModel::default();
        model.insert_task_run(task_run);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        assert!(!reducer.sweep_stale(now_ms).is_empty());
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().state,
            TaskState::EndedUnknown
        );

        let persist = reducer.apply_operator_command(OperatorCommand::DismissClearable, now_ms + 1);

        assert_eq!(persist.len(), 1);
        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().dismissed_at_ms,
            Some(now_ms + 1)
        );
    }

    #[test]
    fn operator_command_dismisses_terminal_runs_inside_visibility_window() {
        let now_ms = 3_600_000;
        let run_id = RunId::new();
        let mut terminal = run_with_controller_evidence(
            run_id,
            RunKey::Controller("recent-terminal".to_owned()),
            1,
            TaskState::Completed,
        );
        terminal.updated_at_ms = Some(now_ms - 60_000);
        terminal.finished_at_ms = Some(now_ms - 60_000);
        let mut model = DomainModel::default();
        model.insert_task_run(terminal);
        let (mut reducer, shared) = Reducer::new(restored(model, 2));

        let persist = reducer.apply_operator_command(OperatorCommand::DismissClearable, now_ms);

        assert_eq!(
            shared.borrow().task_run(&run_id).unwrap().dismissed_at_ms,
            Some(now_ms)
        );
        assert_eq!(persist.len(), 1);
        assert!(matches!(
            &persist[0],
            PersistOp::UpsertTaskRun(value)
                if value.task_run.run_id == run_id
                    && value.task_run.dismissed_at_ms == Some(now_ms)
        ));
    }

    #[test]
    fn operator_command_dismisses_hook_only_at_boundary_but_not_attached_controller_run() {
        let updated_at_ms = 100;
        let now_ms = updated_at_ms + crate::activity::HOOK_ONLY_STALE_VISIBILITY_MS;
        let hook_only = RunId::new();
        let attached = RunId::new();
        let mut model = DomainModel::default();
        for (run_id, key) in [(hook_only, "hook-only"), (attached, "attached")] {
            let mut task_run = run_with_controller_evidence(
                run_id,
                RunKey::Controller(key.to_owned()),
                if run_id == hook_only { 1 } else { 2 },
                TaskState::Running,
            );
            task_run.updated_at_ms = Some(updated_at_ms);
            model.insert_task_run(task_run);
        }
        model.insert_execution(execution(
            attached,
            "attached-execution",
            ExecState::Working,
        ));
        let (mut reducer, shared) = Reducer::new(restored(model, 3));

        let persist = reducer.apply_operator_command(OperatorCommand::DismissClearable, now_ms);

        assert_eq!(
            shared
                .borrow()
                .task_run(&hook_only)
                .unwrap()
                .dismissed_at_ms,
            Some(now_ms)
        );
        assert_eq!(
            shared.borrow().task_run(&attached).unwrap().dismissed_at_ms,
            None
        );
        assert_eq!(persist.len(), 1);
        assert!(matches!(
            &persist[0],
            PersistOp::UpsertTaskRun(value) if value.task_run.run_id == hook_only
        ));
    }

    #[test]
    fn operator_command_leaves_live_and_already_dismissed_runs_byte_identical_without_persisting() {
        let live = RunId::new();
        let dismissed = RunId::new();
        let mut live_run = run(
            live,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "live".to_owned(),
            },
            1,
            TaskState::Running,
        );
        live_run.updated_at_ms = Some(10);
        let mut dismissed_run = run_with_controller_evidence(
            dismissed,
            RunKey::Controller("dismissed".to_owned()),
            2,
            TaskState::Failed,
        );
        dismissed_run.updated_at_ms = Some(20);
        dismissed_run.finished_at_ms = Some(20);
        dismissed_run.dismissed_at_ms = Some(25);
        let expected_live = live_run.clone();
        let expected_dismissed = dismissed_run.clone();
        let mut model = DomainModel::default();
        model.insert_task_run(live_run);
        model.insert_task_run(dismissed_run);
        model.insert_execution(execution(live, "live-execution", ExecState::Working));
        let (mut reducer, shared) = Reducer::new(restored(model, 3));

        let persist = reducer.apply_operator_command(OperatorCommand::DismissClearable, 86_400_100);

        let snapshot = shared.borrow();
        assert_eq!(snapshot.task_run(&live), Some(&expected_live));
        assert_eq!(snapshot.task_run(&dismissed), Some(&expected_dismissed));
        assert_eq!(
            snapshot.task_run(&dismissed).unwrap().dismissed_at_ms,
            Some(25)
        );
        assert!(persist.is_empty());
    }

    #[test]
    fn operator_command_dismissal_survives_store_restore() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let run_id = RunId::new();
        let mut terminal = run_with_controller_evidence(
            run_id,
            RunKey::Controller("restart-clearable".to_owned()),
            1,
            TaskState::Completed,
        );
        terminal.created_at_ms = Some(10);
        terminal.updated_at_ms = Some(20);
        terminal.finished_at_ms = Some(20);
        let mut model = DomainModel::default();
        model.insert_task_run(terminal);
        let (mut reducer, _shared) = Reducer::new(restored(model, 2));

        let persist = reducer.apply_operator_command(OperatorCommand::DismissClearable, 30);
        store.apply_batch(persist).unwrap();
        drop(store);

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(
            restored.model.task_run(&run_id).unwrap().dismissed_at_ms,
            Some(30)
        );
    }

    #[test]
    fn gap_reattach_reuses_agent_node() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "reattach-sid".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_execution(execution(run_id, "pre-gap", ExecState::Working));
        model.insert_agent_node(AgentNode {
            agent_node_id: "stable-top-level-node".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("reattach-sid".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(2),
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
        model.insert_agent_node(AgentNode {
            agent_node_id: "sub-agent-node".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("child-sid".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(3),
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
        let (mut reducer, shared) = Reducer::new(restored(model, 4));

        reducer
            .reconcile_gap(ReconcileBatch {
                topology: native_snapshot("reattach-sid"),
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();

        assert!(
            shared
                .borrow()
                .agent_node("stable-top-level-node")
                .is_some()
        );
        assert!(shared.borrow().agent_node("sub-agent-node").is_some());
        assert_eq!(
            shared
                .borrow()
                .agent_nodes()
                .filter(|node| {
                    node.task_run_id == run_id
                        && node.provider == Provider::Codex
                        && node.native_session_id.as_deref() == Some("reattach-sid")
                })
                .count(),
            1
        );
    }

    #[test]
    fn other_reducer_errors_stay_fatal() {
        let (mut reducer, mut shared) = Reducer::new(restored(DomainModel::default(), i64::MAX));
        let initial = Arc::clone(&shared.borrow_and_update());
        let run_id = RunId::new();
        let mut value = metadata("ordinal-exhaustion", 1_000);
        value.task_run_id = Some(run_id);

        let result = reducer.apply(topology_event(value, "must-not-exist"));

        assert_eq!(result, Err(ReducerError::OrdinalExhausted));
        assert!(!shared.has_changed().unwrap());
        assert_eq!(shared.borrow().task_runs().count(), 0);
        assert!(shared.borrow().workspace("must-not-exist").is_none());
        assert!(Arc::ptr_eq(&initial, &shared.borrow()));
    }

    #[test]
    fn apply_observation_publishes_exactly_once() {
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let first = topology_event(metadata("publish-once-a", 1_000), "ws-a");
        let second = topology_event(metadata("publish-once-b", 1_001), "ws-b");

        let before = reducer.publish_count.get();
        reducer
            .apply_observation(vec![first, second])
            .expect("observation should apply");
        assert_eq!(reducer.publish_count.get() - before, 1);
        assert!(shared.borrow().workspace("ws-a").is_some());
        assert!(shared.borrow().workspace("ws-b").is_some());

        reducer.next_ordinal = i64::MAX;
        let mut value = metadata("publish-once-err", 1_002);
        value.task_run_id = Some(RunId::new());
        let before_err = reducer.publish_count.get();
        let result = reducer.apply_observation(vec![topology_event(value, "ws-err")]);
        assert_eq!(result, Err(ReducerError::OrdinalExhausted));
        assert_eq!(reducer.publish_count.get(), before_err);
    }

    #[test]
    fn pane_status_only_observation_publishes_once_without_persistence() {
        let mut model = DomainModel::default();
        model.insert_pane(Pane {
            pane_id: "pane-1".to_owned(),
            workspace_id: "workspace-1".to_owned(),
            tab_id: "tab-1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            display_name: None,
        });
        let (mut reducer, shared) = Reducer::new(restored(model, 1));
        let before = reducer.publish_count.get();

        let ApplyOutcome::Applied(batch) = reducer
            .apply_pane_agent_observation(
                Vec::new(),
                PaneAgentStatusObservation {
                    pane_id: "pane-1".to_owned(),
                    status: PaneAgentStatus::Working,
                },
            )
            .unwrap()
        else {
            panic!("status-only observation should apply");
        };

        assert!(batch.is_empty());
        assert!(
            !batch
                .iter()
                .any(|operation| matches!(operation, PersistOp::RecordEvent { .. }))
        );
        assert_eq!(reducer.publish_count.get() - before, 1);
        assert_eq!(
            shared.borrow().pane_agent_status("pane-1"),
            Some(PaneAgentStatus::Working)
        );

        let before_duplicate = reducer.publish_count.get();
        let duplicate = reducer
            .apply_pane_agent_observation(
                Vec::new(),
                PaneAgentStatusObservation {
                    pane_id: "pane-1".to_owned(),
                    status: PaneAgentStatus::Working,
                },
            )
            .unwrap();
        assert_eq!(duplicate, ApplyOutcome::Applied(Vec::new()));
        assert_eq!(reducer.publish_count.get(), before_duplicate);
    }

    #[test]
    fn idle_to_done_updates_transient_status_without_execution_change() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_pane(Pane {
            pane_id: "pane-1".to_owned(),
            workspace_id: "workspace-1".to_owned(),
            tab_id: "tab-1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            display_name: None,
        });
        model.insert_execution(execution(run_id, "execution-1", ExecState::Idle));
        model.set_pane_agent_status("pane-1".to_owned(), PaneAgentStatus::Idle);
        let (mut reducer, shared) = Reducer::new(restored(model, 1));

        let outcome = reducer
            .apply_pane_agent_observation(
                Vec::new(),
                PaneAgentStatusObservation {
                    pane_id: "pane-1".to_owned(),
                    status: PaneAgentStatus::Done,
                },
            )
            .unwrap();

        assert_eq!(outcome, ApplyOutcome::Applied(Vec::new()));
        assert_eq!(
            shared.borrow().pane_agent_status("pane-1"),
            Some(PaneAgentStatus::Done)
        );
        assert_eq!(
            shared.borrow().execution("execution-1").unwrap().state,
            ExecState::Idle
        );
    }

    #[test]
    fn pane_status_observation_rolls_back_with_failed_event_batch() {
        let mut model = DomainModel::default();
        model.insert_pane(Pane {
            pane_id: "pane-1".to_owned(),
            workspace_id: "workspace-1".to_owned(),
            tab_id: "tab-1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            display_name: None,
        });
        model.set_pane_agent_status("pane-1".to_owned(), PaneAgentStatus::Idle);
        let (mut reducer, shared) = Reducer::new(restored(model, i64::MAX));
        let mut failing_metadata = metadata("status-batch-failure", 1_000);
        failing_metadata.task_run_id = Some(RunId::new());
        let before = reducer.publish_count.get();

        let result = reducer.apply_pane_agent_observation(
            vec![topology_event(failing_metadata, "must-not-exist")],
            PaneAgentStatusObservation {
                pane_id: "pane-1".to_owned(),
                status: PaneAgentStatus::Working,
            },
        );

        assert_eq!(result, Err(ReducerError::OrdinalExhausted));
        assert_eq!(reducer.publish_count.get(), before);
        assert_eq!(
            shared.borrow().pane_agent_status("pane-1"),
            Some(PaneAgentStatus::Idle)
        );
        assert!(shared.borrow().workspace("must-not-exist").is_none());
    }

    #[test]
    fn snapshot_reconciliation_replaces_transient_pane_statuses() {
        let mut model = DomainModel::default();
        model.set_pane_agent_status("obsolete".to_owned(), PaneAgentStatus::Blocked);
        let (mut reducer, shared) = Reducer::new(restored(model, 1));
        let mut snapshot = topology_snapshot(
            &["workspace"],
            &[("tab", "workspace")],
            &[
                ("idle-pane", "workspace", "tab"),
                ("done-pane", "workspace", "tab"),
                ("agentless-pane", "workspace", "tab"),
            ],
        );
        snapshot.panes[0].agent = Some(SnapshotAgent {
            agent_name: "codex".to_owned(),
            status: PaneAgentStatus::Idle,
        });
        snapshot.panes[1].agent = Some(SnapshotAgent {
            agent_name: "codex".to_owned(),
            status: PaneAgentStatus::Done,
        });

        reducer.reconcile_snapshot(snapshot).unwrap();

        let installed = shared.borrow();
        assert_eq!(
            installed.pane_agent_status("idle-pane"),
            Some(PaneAgentStatus::Idle)
        );
        assert_eq!(
            installed.pane_agent_status("done-pane"),
            Some(PaneAgentStatus::Done)
        );
        assert_eq!(installed.pane_agent_status("agentless-pane"), None);
        assert_eq!(installed.pane_agent_status("obsolete"), None);
        assert!(
            installed
                .executions()
                .filter(|execution| execution.pane_id == "idle-pane"
                    || execution.pane_id == "done-pane")
                .all(|execution| execution.state == ExecState::Idle)
        );
    }

    #[test]
    fn snapshot_reconciliation_clears_orphan_transient_pane_statuses() {
        let mut model = DomainModel::default();
        model.replace_pane_agent_statuses(HashMap::from([(
            "orphan-pane".to_owned(),
            PaneAgentStatus::Working,
        )]));
        let (mut reducer, shared) = Reducer::new(restored(model, 1));

        reducer
            .reconcile_snapshot(TopologySnapshot::default())
            .unwrap();

        assert_eq!(shared.borrow().pane_agent_status("orphan-pane"), None);
    }

    #[test]
    fn pane_closure_removes_transient_status() {
        let mut model = DomainModel::default();
        model.insert_pane(Pane {
            pane_id: "pane-1".to_owned(),
            workspace_id: "workspace-1".to_owned(),
            tab_id: "tab-1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            display_name: None,
        });
        model.set_pane_agent_status("pane-1".to_owned(), PaneAgentStatus::Blocked);
        let (mut reducer, shared) = Reducer::new(restored(model, 1));

        reducer
            .apply(NormalizedEvent::TopologyClosure {
                metadata: metadata("pane-closed", 1_000),
                entity: TopologyEntityId::Pane {
                    pane_id: "pane-1".to_owned(),
                },
            })
            .unwrap();

        assert_eq!(shared.borrow().pane_agent_status("pane-1"), None);
    }

    #[test]
    fn staged_validate_is_pure_no_mutation() {
        let (reducer, mut shared) = Reducer::new(restored(DomainModel::default(), 1));
        let initial = Arc::clone(&shared.borrow_and_update());

        let delta = reducer
            .validate_controller_event(&controller_event(
                "pure",
                "raw run",
                ControllerEventKind::TaskStarted,
            ))
            .unwrap();

        assert!(
            delta
                .post_model
                .task_run_by_key(&RunKey::Controller("raw run".to_owned()))
                .is_some()
        );
        assert!(reducer.resolve_controller_run("raw run").is_none());
        assert!(!shared.has_changed().unwrap());
        assert!(Arc::ptr_eq(&initial, &shared.borrow()));
    }

    #[test]
    fn controller_event_progress_must_be_within_basis_point_range() {
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut above_max = controller_event(
            "progress-above-max",
            "raw run",
            ControllerEventKind::Progress,
        );
        above_max.metadata.progress = Some(10_001);
        assert!(matches!(
            reducer.validate_controller_event(&above_max),
            Err(RejectReason::Invalid)
        ));

        let mut at_max =
            controller_event("progress-at-max", "raw run", ControllerEventKind::Progress);
        at_max.metadata.progress = Some(10_000);
        assert!(reducer.validate_controller_event(&at_max).is_ok());
    }

    #[tokio::test]
    async fn commit_assigns_monotonic_ingest_seq() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let writer = &mut writer;
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));

        for (event_id, raw) in [("sequence-1", "raw-1"), ("sequence-2", "raw-2")] {
            let mut event = controller_event(event_id, raw, ControllerEventKind::TaskStarted);
            event.metadata.receipt_time_ms = super::unix_now_ms();
            let delta = reducer.validate_controller_event(&event).unwrap();
            let permit = writer.reserve_enqueue().expect("writer must have capacity");
            let pending = reducer
                .commit_staged(delta, permit)
                .expect("sequence must be available");
            writer.finish_pending(pending).await.unwrap();
        }
        lifecycle.shutdown().await.unwrap();

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(restored.next_ingest_seq, Some(3));
        let connection = rusqlite::Connection::open(database_path(&root)).unwrap();
        let mut statement = connection
            .prepare("SELECT ingest_seq FROM events ORDER BY ingest_seq")
            .unwrap();
        let sequences: Vec<i64> = statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(sequences, vec![1, 2]);
    }

    #[tokio::test]
    async fn ledger_reservation_returns_duplicate_before_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![PersistOp::RecordEvent {
                event: Box::new(topology_event(metadata("duplicate", 20), "workspace")),
                seen_at_ms: 20,
            }])
            .unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let writer = &mut writer;
        let (reducer, mut shared) = Reducer::new(restored(DomainModel::default(), 1));
        let initial = Arc::clone(&shared.borrow_and_update());

        assert!(writer.is_duplicate("duplicate"));
        assert!(reducer.resolve_controller_run("must-not-stage").is_none());
        assert!(!shared.has_changed().unwrap());
        assert!(Arc::ptr_eq(&initial, &shared.borrow()));
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rejected_event_id_is_reusable() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let writer = &mut writer;
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let event_id = "reusable";

        assert!(matches!(
            reducer.validate_controller_event(&controller_event(
                event_id,
                "same",
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "same".to_owned(),
                },
            )),
            Err(RejectReason::Cycle)
        ));
        assert!(!writer.is_duplicate(event_id));
        assert!(
            reducer
                .validate_controller_event(&controller_event(
                    event_id,
                    "same",
                    ControllerEventKind::TaskStarted,
                ))
                .is_ok()
        );
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn writer_health_gate_returns_retryable_no_change() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let writer = &mut writer;
        assert!(
            writer
                .apply(vec![PersistOp::UpsertTab {
                    tab: crate::model::Tab {
                        tab_id: "orphan-tab".to_owned(),
                        workspace_id: "missing-workspace".to_owned(),
                        label: None,
                    },
                    display_ordinal: DisplayOrdinal::new(1),
                }])
                .await
                .is_err()
        );
        let (reducer, mut shared) = Reducer::new(restored(DomainModel::default(), 1));
        let initial = Arc::clone(&shared.borrow_and_update());
        let _delta = reducer
            .validate_controller_event(&controller_event(
                "unhealthy",
                "raw",
                ControllerEventKind::TaskStarted,
            ))
            .unwrap();

        assert!(writer.reserve_enqueue().is_none());
        assert!(!shared.has_changed().unwrap());
        assert!(Arc::ptr_eq(&initial, &shared.borrow()));
        lifecycle.shutdown().await.unwrap();
    }

    #[test]
    fn receipt_time_distinct_from_envelope_time() {
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let delta = reducer
            .validate_controller_event(&controller_event(
                "receipt",
                "child",
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "parent".to_owned(),
                },
            ))
            .unwrap();

        assert!(delta.batch.iter().all(|operation| match operation {
            PersistOp::UpsertTaskRun(value) =>
                value.created_at_ms == 20 && value.updated_at_ms == 20,
            PersistOp::UpsertExecutionEdge { created_at_ms, .. } => *created_at_ms == 20,
            PersistOp::RecordEvent { seen_at_ms, event } => {
                *seen_at_ms == 20 && super::event_metadata(event).timestamp_ms == 10
            }
            _ => true,
        }));
    }

    #[tokio::test]
    async fn diagnostics_counters_increment() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let writer = &mut writer;
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        for (event_id, kind) in [
            ("terminal-forward", ControllerEventKind::Complete),
            ("terminal-noop", ControllerEventKind::Blocked),
            (
                "dangling",
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "dangling-parent".to_owned(),
                },
            ),
        ] {
            let raw = if event_id == "dangling" {
                "dangling-child"
            } else {
                "raw"
            };
            let delta = reducer
                .validate_controller_event(&controller_event(event_id, raw, kind))
                .unwrap();
            let permit = writer.reserve_enqueue().unwrap();
            let pending = reducer.commit_staged(delta, permit).unwrap();
            writer.finish_pending(pending).await.unwrap();
        }

        let snapshot = shared.borrow();
        assert_eq!(
            snapshot
                .controller_diagnostics()
                .terminal_forward_reference_creations(),
            1
        );
        assert_eq!(
            snapshot
                .controller_diagnostics()
                .terminal_blocked_progress_noops(),
            1
        );
        assert_eq!(
            snapshot
                .controller_diagnostics()
                .dangling_announcement_components(),
            1
        );
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dispatch_placeholder_under_running_parent_is_not_dangling() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event("parent-started", "parent", ControllerEventKind::TaskStarted),
        )
        .await;
        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event(
                "child-dispatched",
                "child",
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "parent".to_owned(),
                },
            ),
        )
        .await;

        assert_eq!(dangling_gauge(&shared), 0);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn task_started_resolves_a_dangling_component_member() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event(
                "island",
                "child",
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "parent".to_owned(),
                },
            ),
        )
        .await;
        assert_eq!(dangling_gauge(&shared), 1);

        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event("child-started", "child", ControllerEventKind::TaskStarted),
        )
        .await;

        assert_eq!(dangling_gauge(&shared), 0);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn execution_binding_resolves_a_dangling_component_member() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event(
                "island",
                "child",
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "parent".to_owned(),
                },
            ),
        )
        .await;
        assert_eq!(dangling_gauge(&shared), 1);
        let child = reducer.resolve_controller_run("child").unwrap();
        let mut begin_metadata = metadata("child-execution", 100);
        begin_metadata.terminal_id = Some("terminal-child".to_owned());

        reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: begin_metadata,
                execution: Execution {
                    execution_id: "child-execution".to_owned(),
                    pane_id: "pane-child".to_owned(),
                    terminal_id: "terminal-child".to_owned(),
                    task_run_id: child,
                    state: ExecState::Working,
                },
            })
            .unwrap();

        assert_eq!(dangling_gauge(&shared), 0);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dangling_gauge_replaces_old_value_after_resolution_and_creation() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event(
                "first-island",
                "child-1",
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "parent-1".to_owned(),
                },
            ),
        )
        .await;
        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event(
                "first-resolved",
                "child-1",
                ControllerEventKind::TaskStarted,
            ),
        )
        .await;
        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event(
                "second-island",
                "child-2",
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "parent-2".to_owned(),
                },
            ),
        )
        .await;

        assert_eq!(dangling_gauge(&shared), 1);
        lifecycle.shutdown().await.unwrap();
    }

    #[test]
    fn restored_dangling_island_initializes_gauge_before_first_event() {
        let parent = RunId::new();
        let child = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            parent,
            RunKey::Controller("parent".to_owned()),
            1,
            TaskState::Queued,
        ));
        model.insert_task_run(run(
            child,
            RunKey::Controller("child".to_owned()),
            2,
            TaskState::Queued,
        ));
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: child,
        });

        let (_reducer, shared) = Reducer::new(restored(model, 3));

        assert_eq!(dangling_gauge(&shared), 1);
    }

    #[test]
    fn restored_isolated_relationship_only_run_initializes_gauge_before_first_event() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            run_id,
            RunKey::Controller("isolated".to_owned()),
            1,
            TaskState::Queued,
        ));

        let (_reducer, shared) = Reducer::new(restored(model, 2));

        assert_eq!(dangling_gauge(&shared), 1);
    }

    #[test]
    fn stale_sweep_terminal_neighbor_flips_dangling_gauge() {
        let outside = RunId::new();
        let relationship_only = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            outside,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "outside".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_task_run(run(
            relationship_only,
            RunKey::Controller("relationship-only".to_owned()),
            2,
            TaskState::Queued,
        ));
        model.insert_execution(execution(
            outside,
            "outside-execution",
            ExecState::Stale { since_ms: 1_000 },
        ));
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: outside,
            child_run_id: relationship_only,
        });
        let (mut reducer, shared) = Reducer::new(restored(model, 3));
        assert_eq!(dangling_gauge(&shared), 0);

        reducer.sweep_stale(31_000);

        assert_eq!(dangling_gauge(&shared), 1);
        assert_eq!(
            shared.borrow().task_run(&outside).unwrap().state,
            TaskState::EndedUnknown
        );
    }

    #[test]
    fn rejected_controller_event_leaves_dangling_gauge_unchanged() {
        let parent = RunId::new();
        let child = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            parent,
            RunKey::Controller("parent".to_owned()),
            1,
            TaskState::Queued,
        ));
        model.insert_task_run(run(
            child,
            RunKey::Controller("child".to_owned()),
            2,
            TaskState::Queued,
        ));
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: parent,
            child_run_id: child,
        });
        let (reducer, shared) = Reducer::new(restored(model, 3));
        assert_eq!(dangling_gauge(&shared), 1);

        let result = reducer.validate_controller_event(&controller_event(
            "rejected-cycle",
            "self",
            ControllerEventKind::Dispatch {
                parent_task_run_id: "self".to_owned(),
            },
        ));

        assert!(matches!(result, Err(RejectReason::Cycle)));
        assert_eq!(dangling_gauge(&shared), 1);
    }

    #[test]
    fn progress_forward_reference_is_not_relationship_only() {
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let delta = reducer
            .validate_controller_event(&controller_event(
                "progress-forward",
                "progress-run",
                ControllerEventKind::Progress,
            ))
            .unwrap();
        let run_id = delta
            .post_model
            .task_run_by_key(&RunKey::Controller("progress-run".to_owned()))
            .unwrap()
            .run_id;

        assert!(!crate::model::graph::is_relationship_only(
            &delta.post_model,
            run_id
        ));
    }

    #[tokio::test]
    async fn two_disjoint_placeholder_islands_count_as_two() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        for (event_id, child, parent) in [
            ("island-1", "child-1", "parent-1"),
            ("island-2", "child-2", "parent-2"),
        ] {
            commit_controller(
                &mut reducer,
                &mut writer,
                controller_event(
                    event_id,
                    child,
                    ControllerEventKind::Dispatch {
                        parent_task_run_id: parent.to_owned(),
                    },
                ),
            )
            .await;
        }

        assert_eq!(dangling_gauge(&shared), 2);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn zero_outside_neighbor_island_is_dangling() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event(
                "dependency-island",
                "dependent",
                ControllerEventKind::DependsOn {
                    depends_on_id: "prerequisite".to_owned(),
                },
            ),
        )
        .await;

        assert_eq!(dangling_gauge(&shared), 1);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn completed_only_outside_neighbor_keeps_component_dangling() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event(
                "island",
                "child",
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "parent".to_owned(),
                },
            ),
        )
        .await;
        commit_controller(
            &mut reducer,
            &mut writer,
            controller_event("parent-complete", "parent", ControllerEventKind::Complete),
        )
        .await;

        assert_eq!(dangling_gauge(&shared), 1);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn commit_is_infallible_after_permit() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let writer = &mut writer;
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let delta = reducer
            .validate_controller_event(&controller_event(
                "commit",
                "raw",
                ControllerEventKind::TaskStarted,
            ))
            .unwrap();
        let permit = writer.reserve_enqueue().unwrap();

        let pending = reducer.commit_staged(delta, permit).unwrap();
        writer.finish_pending(pending).await.unwrap();
        assert!(
            shared
                .borrow()
                .task_run_by_key(&RunKey::Controller("raw".to_owned()))
                .is_some()
        );
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sequence_exhaustion_before_mutation_answers_retryable() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let writer = &mut writer;
        let (mut reducer, shared) = Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: None,
            event_ledger: Vec::new(),
        });
        let delta = reducer
            .validate_controller_event(&controller_event(
                "exhausted",
                "raw",
                ControllerEventKind::TaskStarted,
            ))
            .unwrap();
        let permit = writer.reserve_enqueue().unwrap();

        assert!(matches!(
            reducer.commit_staged(delta, permit),
            Err(CommitStagedError::IngestSequenceExhausted)
        ));
        assert!(
            shared
                .borrow()
                .task_run_by_key(&RunKey::Controller("raw".to_owned()))
                .is_none()
        );
        assert_eq!(
            shared
                .borrow()
                .controller_diagnostics()
                .ingest_sequence_exhaustions(),
            1
        );
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn non_ulid_controller_key_resolves_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let writer = &mut writer;
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let raw = "not a ULID / task #1";
        let delta = reducer
            .validate_controller_event(&controller_event(
                "raw-key",
                raw,
                ControllerEventKind::TaskStarted,
            ))
            .unwrap();
        let permit = writer.reserve_enqueue().unwrap();
        let pending = reducer.commit_staged(delta, permit).unwrap();
        writer.finish_pending(pending).await.unwrap();
        let original = shared
            .borrow()
            .task_run_by_key(&RunKey::Controller(raw.to_owned()))
            .unwrap()
            .run_id;
        lifecycle.shutdown().await.unwrap();

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(
            restored
                .model
                .task_run_by_key(&RunKey::Controller(raw.to_owned()))
                .unwrap()
                .run_id,
            original
        );
    }

    #[tokio::test]
    async fn restored_legacy_codex_controller_accepts_canonical_hook_key() {
        const SID: &str = "22222222-2222-4222-8222-222222222222";
        const CANONICAL: &str = "hook:codex:22222222-2222-4222-8222-222222222222";
        const OTHER_CANONICAL: &str = "hook:codex:33333333-3333-4333-8333-333333333333";

        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, mut writer) = spawn_writer(store).unwrap();
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut legacy = controller_event(
            "legacy-codex-started",
            SID,
            ControllerEventKind::TaskStarted,
        );
        legacy.metadata.provider = Some(Provider::Codex);
        legacy.metadata.native_session_id = Some(SID.to_owned());
        commit_controller(&mut reducer, &mut writer, legacy).await;
        let original = reducer.resolve_controller_run(SID).unwrap();
        lifecycle.shutdown().await.unwrap();

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(
            restored
                .model
                .task_run_by_key(&RunKey::Controller(SID.to_owned()))
                .unwrap()
                .run_id,
            original
        );
        assert_eq!(
            restored
                .model
                .task_run_by_key(&RunKey::Native {
                    provider: Provider::Codex,
                    sid: SID.to_owned(),
                })
                .unwrap()
                .run_id,
            original
        );

        let (reducer, _shared) = Reducer::new(restored);
        let mut arbitrary = controller_event(
            "arbitrary-codex-started",
            "different-controller-key",
            ControllerEventKind::TaskStarted,
        );
        arbitrary.metadata.provider = Some(Provider::Codex);
        arbitrary.metadata.native_session_id = Some(SID.to_owned());
        assert!(matches!(
            reducer.validate_controller_event(&arbitrary),
            Err(RejectReason::Conflict)
        ));
        assert_eq!(reducer.resolve_controller_run(OTHER_CANONICAL), None);
        assert_eq!(
            reducer.resolve_controller_run(&format!("{CANONICAL}:agent:child")),
            None
        );

        let mut canonical = controller_event(
            "canonical-codex-started",
            CANONICAL,
            ControllerEventKind::TaskStarted,
        );
        canonical.metadata.provider = Some(Provider::Codex);
        canonical.metadata.native_session_id = Some(SID.to_owned());
        let delta = match reducer.validate_controller_event(&canonical) {
            Ok(delta) => delta,
            Err(reason) => panic!("canonical restored Codex start was rejected: {reason:?}"),
        };

        assert_eq!(delta.post_model.task_runs().count(), 1);
        assert_eq!(
            delta
                .post_model
                .task_run_by_key(&RunKey::Controller(SID.to_owned()))
                .unwrap()
                .run_id,
            original
        );
        assert_eq!(
            delta
                .post_model
                .task_run_by_key(&RunKey::Native {
                    provider: Provider::Codex,
                    sid: SID.to_owned(),
                })
                .unwrap()
                .run_id,
            original
        );
        assert!(delta.batch.iter().any(|operation| matches!(
            operation,
            PersistOp::RecordEvent { event, .. }
                if super::event_metadata(event).task_run_id == Some(original)
        )));
        assert_eq!(
            delta
                .post_model
                .controller_diagnostics()
                .binding_conflicts(),
            0
        );
    }

    #[test]
    fn controller_resolution_prioritizes_exact_key_over_legacy_codex_fallback() {
        const SID: &str = "44444444-4444-4444-8444-444444444444";
        let canonical = format!("hook:codex:{SID}");
        let legacy = RunId::new();
        let exact = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            legacy,
            RunKey::Controller(SID.to_owned()),
            1,
            TaskState::Queued,
        ));
        model.insert_task_run_alias(
            RunKey::Native {
                provider: Provider::Codex,
                sid: SID.to_owned(),
            },
            legacy,
        );
        model.insert_task_run(run(
            exact,
            RunKey::Controller(canonical.clone()),
            2,
            TaskState::Queued,
        ));
        let (reducer, _shared) = Reducer::new(restored(model, 3));

        assert_eq!(reducer.resolve_controller_run(&canonical), Some(exact));
    }

    #[test]
    fn codex_compatibility_fallback_requires_bare_sid_controller_primary() {
        const SID: &str = "55555555-5555-4555-8555-555555555555";
        let owner = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            owner,
            RunKey::Controller("different-controller-primary".to_owned()),
            1,
            TaskState::Queued,
        ));
        model.insert_task_run_alias(
            RunKey::Native {
                provider: Provider::Codex,
                sid: SID.to_owned(),
            },
            owner,
        );
        let (reducer, _shared) = Reducer::new(restored(model, 2));
        let canonical = format!("hook:codex:{SID}");

        assert_eq!(reducer.resolve_controller_run(&canonical), None);
        let mut claimant = controller_event(
            "non-legacy-primary-claim",
            &canonical,
            ControllerEventKind::TaskStarted,
        );
        claimant.metadata.provider = Some(Provider::Codex);
        claimant.metadata.native_session_id = Some(SID.to_owned());
        assert!(matches!(
            reducer.validate_controller_event(&claimant),
            Err(RejectReason::Conflict)
        ));
    }

    #[test]
    fn receipt_time_covers_native_binding_persistence() {
        let (reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));
        let mut event = controller_event(
            "native-receipt",
            "controller raw",
            ControllerEventKind::TaskStarted,
        );
        event.metadata.provider = Some(Provider::Codex);
        event.metadata.native_session_id = Some("native-session".to_owned());
        let delta = reducer.validate_controller_event(&event).unwrap();

        assert!(
            delta
                .batch
                .iter()
                .filter_map(|operation| match operation {
                    PersistOp::UpsertTaskRun(value) if value.native_session.is_some() =>
                        Some(value),
                    _ => None,
                })
                .all(|value| value.created_at_ms == 20 && value.updated_at_ms == 20)
        );
        assert!(delta.batch.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTaskRun(value) if value.native_session.is_some()
        )));
    }

    #[test]
    fn edge_then_binding_composite_self_cycle_rejected_in_validation() {
        let native_run = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run(
            native_run,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "native".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_task_run_alias(RunKey::Controller("parent".to_owned()), native_run);
        model.insert_execution(execution(native_run, "live", ExecState::Working));
        let (reducer, _shared) = Reducer::new(restored(model, 2));
        let mut event = controller_event(
            "composite",
            "child",
            ControllerEventKind::Dispatch {
                parent_task_run_id: "parent".to_owned(),
            },
        );
        event.metadata.terminal_id = Some("terminal-1".to_owned());

        assert!(matches!(
            reducer.validate_controller_event(&event),
            Err(RejectReason::Cycle)
        ));
        assert!(reducer.resolve_controller_run("child").is_none());
    }

    #[test]
    fn dual_binding_native_then_terminal_ordering() {
        let native = RunId::new();
        let terminal = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(run_with_controller_evidence(
            native,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "sid".to_owned(),
            },
            1,
            TaskState::Running,
        ));
        model.insert_task_run(run(
            terminal,
            RunKey::Provisional {
                terminal_id: "terminal-1".to_owned(),
                start_ms: 1,
                seq: 2,
            },
            2,
            TaskState::Running,
        ));
        model.insert_execution(execution(terminal, "terminal-live", ExecState::Working));
        let (reducer, _shared) = Reducer::new(restored(model, 3));
        let mut event = controller_event("dual", "controller", ControllerEventKind::TaskStarted);
        event.metadata.provider = Some(Provider::Codex);
        event.metadata.native_session_id = Some("sid".to_owned());
        event.metadata.terminal_id = Some("terminal-1".to_owned());

        let delta = reducer.validate_controller_event(&event).unwrap();
        let merges: Vec<_> = delta
            .batch
            .iter()
            .filter_map(|operation| match operation {
                PersistOp::MergeTaskRuns { absorbed, .. } => Some(*absorbed),
                _ => None,
            })
            .collect();
        assert_eq!(merges, vec![native, terminal]);
    }

    #[test]
    fn transition_matrix_task_started() {
        let (running, _) = controller_model("run", TaskState::Running);
        let (reducer, _) = Reducer::new(restored(running, 2));
        assert!(
            reducer
                .validate_controller_event(&controller_event(
                    "started-noop",
                    "run",
                    ControllerEventKind::TaskStarted
                ))
                .is_ok()
        );

        let (completed, _) = controller_model("run", TaskState::Completed);
        let (reducer, _) = Reducer::new(restored(completed, 2));
        assert!(matches!(
            reducer.validate_controller_event(&controller_event(
                "started-stale",
                "run",
                ControllerEventKind::TaskStarted
            )),
            Err(RejectReason::StaleEvent)
        ));
    }

    #[test]
    fn transition_matrix_blocked() {
        let (blocked, _) = controller_model("run", TaskState::Blocked);
        let (reducer, _) = Reducer::new(restored(blocked, 2));
        assert!(
            reducer
                .validate_controller_event(&controller_event(
                    "blocked-noop",
                    "run",
                    ControllerEventKind::Blocked
                ))
                .is_ok()
        );

        let (terminal, _) = controller_model("run", TaskState::Completed);
        let (reducer, _) = Reducer::new(restored(terminal, 2));
        let delta = reducer
            .validate_controller_event(&controller_event(
                "blocked-terminal",
                "run",
                ControllerEventKind::Blocked,
            ))
            .unwrap();
        assert_eq!(delta.diagnostic_deltas.terminal_blocked_progress_noops, 1);
    }

    #[test]
    fn transition_matrix_progress() {
        let (queued, run_id) = controller_model("run", TaskState::Queued);
        let (reducer, _) = Reducer::new(restored(queued, 2));
        let delta = reducer
            .validate_controller_event(&controller_event(
                "progress-noop",
                "run",
                ControllerEventKind::Progress,
            ))
            .unwrap();
        assert_eq!(
            delta.post_model.task_run(&run_id).unwrap().state,
            TaskState::Queued
        );

        let (terminal, _) = controller_model("run", TaskState::Failed);
        let (reducer, _) = Reducer::new(restored(terminal, 2));
        let delta = reducer
            .validate_controller_event(&controller_event(
                "progress-terminal",
                "run",
                ControllerEventKind::Progress,
            ))
            .unwrap();
        assert_eq!(delta.diagnostic_deltas.terminal_blocked_progress_noops, 1);
    }

    #[test]
    fn transition_matrix_terminal() {
        let (completed, _) = controller_model("run", TaskState::Completed);
        let (reducer, _) = Reducer::new(restored(completed, 2));
        assert!(
            reducer
                .validate_controller_event(&controller_event(
                    "same-terminal",
                    "run",
                    ControllerEventKind::Complete
                ))
                .is_ok()
        );

        let (completed, _) = controller_model("run", TaskState::Completed);
        let (reducer, _) = Reducer::new(restored(completed, 2));
        assert!(matches!(
            reducer.validate_controller_event(&controller_event(
                "different-terminal",
                "run",
                ControllerEventKind::Failed
            )),
            Err(RejectReason::Conflict)
        ));

        let (ended, run_id) = controller_model("run", TaskState::EndedUnknown);
        let (reducer, _) = Reducer::new(restored(ended, 2));
        let delta = reducer
            .validate_controller_event(&controller_event(
                "refine-terminal",
                "run",
                ControllerEventKind::Cancelled,
            ))
            .unwrap();
        assert_eq!(
            delta.post_model.task_run(&run_id).unwrap().state,
            TaskState::Cancelled
        );
    }
}
