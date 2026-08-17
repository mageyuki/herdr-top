//! T7 reducer state machines, ordinal allocator, and gap reconciliation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::watch;

use crate::activity::{OperatorSnapshot, RestoredOperatorState};
use crate::diagnostics::RuntimeWriteOutcome;
use crate::identity::{
    BindingEvidence, MergeConflict, apply_binding_plan_at, plan_binding, preflight_dependency_edge,
    preflight_execution_edge,
};
use crate::model::{
    AgentNode, AgentNodeObservation, AgentSessionReferenceKind, ControllerDiagnosticsHandle,
    ControllerEvent, ControllerEventKind, DependencyEdge, DisplayOrdinal, DomainModel,
    EventMetadata, ExecState, Execution, ExecutionEdge, MinimalProviderMetadata, NormalizedEvent,
    Pane, Provider, ProviderDiagnosticsHandle, ReconcileBatch, RunId, RunKey, SharedModel, TaskRun,
    TaskState, TopologyEntity, TopologyEntityId, sanitize_controller_text,
};
use crate::operator::OperatorProjection;
use crate::store::{
    EnqueuePermit, NativeSessionBinding, PendingEnqueue, PersistBatch, PersistExecution, PersistOp,
    PersistTaskRun, RestoredState,
};
// increment5-workload-harness: begin reducer timing callback ABI
#[cfg(feature = "workload-harness")]
use std::cell::RefCell;
#[cfg(feature = "workload-harness")]
use std::time::Instant;

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
    started: Instant,
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
pub(crate) fn workload_timing_scope(
    kind: WorkloadTimingKind,
    sequence: u64,
    observer: WorkloadTimingObserver,
) -> WorkloadTimingScope {
    WORKLOAD_TIMING_STATE.with(|slot| {
        let previous = slot.replace(Some(WorkloadTimingState {
            kind,
            sequence,
            started: Instant::now(),
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
        let state = state.expect("workload timing scope must remain installed until finish");
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
        let reducer_plus_publish_ns = u64::try_from(state.started.elapsed().as_nanos())
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
    pub post_dangling_announcement_components: u64,
}

/// Fully materialized result of successful Controller validation.
pub struct MaterializedDelta {
    pub post_model: DomainModel,
    pub post_next_ordinal: i64,
    pub diagnostic_deltas: ControllerDiagnosticDeltas,
    pub batch: PersistBatch,
}

/// Retryable failure before any staged domain mutation is committed.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CommitStagedError {
    #[error("Controller ingest sequence is exhausted")]
    IngestSequenceExhausted,
}

/// Serialized owner of domain transitions and display-ordinal allocation.
pub struct Reducer {
    model: DomainModel,
    next_ordinal: i64,
    next_ingest_seq: Option<i64>,
    publisher: watch::Sender<Arc<DomainModel>>,
    operator: OperatorProjection,
    #[cfg(test)]
    publish_count: std::cell::Cell<u64>,
    // increment5-workload-harness: begin reducer timing configuration field
    #[cfg(feature = "workload-harness")]
    workload_observation_timing: Option<WorkloadObservationTiming>,
    // increment5-workload-harness: end reducer timing configuration field
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
        // increment5-workload-harness: begin startup clone publication timing start
        #[cfg(feature = "workload-harness")]
        let workload_publish_started = Instant::now();
        // increment5-workload-harness: end startup clone publication timing start
        let (publisher, shared) = watch::channel(Arc::new(model.clone()));
        // increment5-workload-harness: begin startup clone publication timing finish
        #[cfg(feature = "workload-harness")]
        record_workload_timing_segment(
            WorkloadTimingSegment::ModelClonePublish,
            workload_publish_started.elapsed(),
        );
        // increment5-workload-harness: end startup clone publication timing finish
        let (operator, operator_receiver) = OperatorProjection::new(restored_operator);
        (
            Self {
                model,
                next_ordinal: restored.next_ordinal,
                next_ingest_seq: restored.next_ingest_seq,
                publisher,
                operator,
                #[cfg(test)]
                publish_count: std::cell::Cell::new(0),
                // increment5-workload-harness: begin reducer timing configuration initialization
                #[cfg(feature = "workload-harness")]
                workload_observation_timing: None,
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

    /// Applies one source observation and publishes exactly one resulting snapshot.
    pub fn apply_observation(
        &mut self,
        events: Vec<NormalizedEvent>,
    ) -> Result<ApplyOutcome, ReducerError> {
        if events.is_empty() {
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
        let mut persist = Vec::new();
        for event in events {
            match self.apply_inner(event) {
                Ok(event_persist) => persist.extend(event_persist),
                Err(ReducerError::BindingConflict(conflict)) => {
                    self.model = original_model;
                    self.next_ordinal = original_next_ordinal;
                    return Ok(ApplyOutcome::DroppedBindingConflict(conflict));
                }
                Err(error) => {
                    self.model = original_model;
                    self.next_ordinal = original_next_ordinal;
                    return Err(error);
                }
            }
        }
        self.recompute_dangling_announcement_components();
        normalize_persist_batch_lineage(&mut persist);
        self.operator.apply_submission(&persist);
        self.publish();
        // increment5-workload-harness: begin observed apply scope finish
        #[cfg(feature = "workload-harness")]
        if let Some(timing) = workload_timing {
            timing.finish();
        }
        // increment5-workload-harness: end observed apply scope finish
        Ok(ApplyOutcome::Applied(persist))
    }

    /// Returns the atomic diagnostics handle intended for the socket acceptor.
    #[must_use]
    pub fn controller_diagnostics_handle(&self) -> ControllerDiagnosticsHandle {
        self.model.controller_diagnostics().acceptor_handle()
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
        self.model
            .task_run_by_key(&RunKey::Controller(raw.to_owned()))
            .map(|run| run.run_id)
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
            | ControllerEventKind::Cancelled => {}
        }

        let (mut scratch, _scratch_shared) = Self::new(RestoredState {
            model: self.model.clone(),
            next_ordinal: self.next_ordinal,
            next_ingest_seq: self.next_ingest_seq,
            event_ledger: Vec::new(),
        });
        let normalized = NormalizedEvent::ControllerEvent {
            metadata: metadata.clone(),
            event: event.event.clone(),
        };
        let mut persist = Vec::new();
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
                        metadata.receipt_time_ms,
                        &mut persist,
                    )
                    .map_err(|_| RejectReason::Conflict)?;
            }
            (ControllerEventKind::DependsOn { depends_on_id }, None, Some(edge)) => {
                scratch
                    .ensure_controller_placeholder(
                        edge.prerequisite_run_id,
                        Some(depends_on_id),
                        metadata.receipt_time_ms,
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

        let mut diagnostic_deltas = validate_controller_transition(
            &scratch.model,
            &event.event,
            subject,
            subject_was_unknown,
            metadata.execution_parent.as_ref(),
            metadata.dependency.as_ref(),
        )?;
        scratch.apply_controller_metadata(&metadata, &mut persist);
        scratch
            .apply_event_body(&normalized, &metadata, &mut persist)
            .map_err(|_| RejectReason::Conflict)?;
        scratch
            .apply_identity_metadata(&normalized, &metadata, &mut persist)
            .map_err(|error| match error {
                ReducerError::BindingConflict(conflict) => reject_merge_conflict(&conflict),
                ReducerError::OrdinalExhausted => RejectReason::Conflict,
            })?;
        scratch.persist_event_execution(&normalized, metadata.receipt_time_ms, &mut persist);
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
            diagnostic_deltas,
            batch: persist,
        })
    }

    /// Allocates/stamps one sequence, swaps the staged state, publishes once, and consumes a permit.
    pub fn commit_staged(
        &mut self,
        mut delta: MaterializedDelta,
        permit: EnqueuePermit,
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
        self.next_ingest_seq = ingest_seq.checked_add(1);
        self.apply_controller_diagnostic_deltas(delta.diagnostic_deltas);
        self.operator.apply_submission(&batch);
        self.publish();
        Ok(permit.enqueue(batch))
    }

    fn apply_controller_diagnostic_deltas(&mut self, deltas: ControllerDiagnosticDeltas) {
        let diagnostics = self.model.controller_diagnostics_mut();
        diagnostics.record_terminal_blocked_progress_noops(deltas.terminal_blocked_progress_noops);
        diagnostics.record_terminal_forward_reference_creations(
            deltas.terminal_forward_reference_creations,
        );
        diagnostics
            .set_dangling_announcement_components(deltas.post_dangling_announcement_components);
    }

    fn apply_inner(&mut self, event: NormalizedEvent) -> Result<PersistBatch, ReducerError> {
        let metadata = event_metadata(&event).clone();
        let mut persist = Vec::new();

        self.ensure_event_runs(&event, &metadata, &mut persist)?;
        self.apply_controller_metadata(&metadata, &mut persist);
        self.apply_event_body(&event, &metadata, &mut persist)?;
        self.apply_identity_metadata(&event, &metadata, &mut persist)?;
        self.persist_event_execution(&event, metadata.receipt_time_ms, &mut persist);
        persist.push(PersistOp::RecordEvent {
            event: Box::new(event),
            seen_at_ms: metadata.receipt_time_ms,
        });

        Ok(persist)
    }

    /// Replaces physical topology across an observation gap in one coherent batch.
    pub fn reconcile_gap(&mut self, batch: ReconcileBatch) -> Result<PersistBatch, ReducerError> {
        let original_model = self.model.clone();
        let original_next_ordinal = self.next_ordinal;
        match self.reconcile_gap_inner(batch) {
            Ok(mut persist) => {
                normalize_persist_batch_lineage(&mut persist);
                self.operator.apply_submission(&persist);
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

    fn reconcile_gap_inner(&mut self, batch: ReconcileBatch) -> Result<PersistBatch, ReducerError> {
        let ReconcileBatch {
            topology,
            gap_kind: _,
        } = batch;
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
            execution.state = ExecState::Ended;
            self.model.insert_execution(execution.clone());
            persist.push(persist_execution(execution, now_ms));
        }

        self.replace_topology(&topology, &mut persist)?;

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
            let existing_run = match (provider, native_sid.as_deref()) {
                (Some(provider), Some(sid)) => self.run_for_native_session(provider, sid),
                _ => None,
            };
            let run_id = match existing_run {
                Some(run_id) => run_id,
                None => self.insert_snapshot_run(
                    provider,
                    native_sid.as_deref(),
                    pane.agent_session.as_ref().and_then(|reference| {
                        (reference.kind == AgentSessionReferenceKind::Path
                            && !reference.value.is_empty())
                        .then_some(reference.value.as_str())
                    }),
                    &pane.terminal_id,
                    now_ms,
                    &mut persist,
                )?,
            };
            let token = RunId::new().to_string();
            let execution_id = format!("gap-execution-{token}");
            let execution = Execution {
                execution_id: execution_id.clone(),
                pane_id: pane.pane_id,
                terminal_id: pane.terminal_id,
                task_run_id: run_id,
                state: agent.state,
            };
            self.model.insert_execution(execution.clone());
            persist.push(persist_execution(execution.clone(), now_ms));
            if !execution.state.is_terminal() {
                self.activate_for_live_execution(run_id, now_ms, &mut persist);
            }

            if let Some(provider) = provider {
                let existing_node = native_sid
                    .as_deref()
                    .filter(|sid| !sid.is_empty())
                    .and_then(|sid| {
                        self.model
                            .agent_nodes()
                            .filter(|node| {
                                node.task_run_id == run_id
                                    && node.provider == provider
                                    && node.native_session_id.as_deref() == Some(sid)
                            })
                            .min_by_key(|node| node.agent_node_id.as_str())
                            .cloned()
                    });
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

        self.recompute_dangling_announcement_components();
        self.publish();
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
                metadata.receipt_time_ms,
                persist,
            )?;
            self.ensure_controller_placeholder(
                edge.child_run_id,
                None,
                metadata.receipt_time_ms,
                persist,
            )?;
        }
        if let Some(edge) = &metadata.dependency {
            self.ensure_controller_placeholder(
                edge.prerequisite_run_id,
                None,
                metadata.receipt_time_ms,
                persist,
            )?;
            self.ensure_controller_placeholder(
                edge.dependent_run_id,
                None,
                metadata.receipt_time_ms,
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
            _ => provisional_key(&execution.terminal_id, metadata.receipt_time_ms, ordinal),
        };
        let task_run = TaskRun {
            run_id: execution.task_run_id,
            key,
            display_ordinal: ordinal,
            state: TaskState::Running,
            has_controller_task_state_event: false,
        };
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, metadata.receipt_time_ms));
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
                    metadata.receipt_time_ms,
                    ordinal,
                ),
            }
        };
        let task_run = TaskRun {
            run_id,
            key,
            display_ordinal: ordinal,
            state: initial_state,
            has_controller_task_state_event: metadata.task_state.is_some(),
        };
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, metadata.receipt_time_ms));
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
        let task_run = TaskRun {
            run_id,
            key: RunKey::Controller(
                controller_key.map_or_else(|| run_id.to_string(), ToOwned::to_owned),
            ),
            display_ordinal: ordinal,
            state: TaskState::Queued,
            has_controller_task_state_event: false,
        };
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
        Ok(())
    }

    fn apply_controller_metadata(&mut self, metadata: &EventMetadata, persist: &mut PersistBatch) {
        if let (Some(run_id), Some(target)) = (metadata.task_run_id, metadata.task_state)
            && let Some(mut task_run) = self.model.task_run(&run_id).cloned()
        {
            task_run.has_controller_task_state_event = true;
            task_run.state =
                controller_task_transition(task_run.state, &metadata.source_event_type, target);
            self.model.insert_task_run(task_run.clone());
            persist.push(self.persist_task_run(task_run, metadata.receipt_time_ms));
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

    fn apply_event_body(
        &mut self,
        event: &NormalizedEvent,
        metadata: &EventMetadata,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        match event {
            NormalizedEvent::ControllerEvent { .. } => {}
            NormalizedEvent::TopologyUpsert { entity, .. } => match entity {
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
                    let display_ordinal = self.tab_ordinal_or_allocate(&tab.tab_id)?;
                    self.model.insert_tab(tab.clone());
                    persist.push(PersistOp::UpsertTab {
                        tab: tab.clone(),
                        display_ordinal,
                    });
                }
                TopologyEntity::Pane(pane) => {
                    let display_ordinal = self.pane_ordinal_or_allocate(&pane.pane_id)?;
                    self.model.insert_pane(pane.clone());
                    persist.push(PersistOp::UpsertPane {
                        pane: pane.clone(),
                        display_ordinal,
                    });
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
        if observed.state == Some(ExecState::Working) {
            node.state = Some(ExecState::Working);
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
            self.apply_binding(evidence, metadata.receipt_time_ms, persist)?;
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
                metadata.receipt_time_ms,
                persist,
            )?;
        }

        if let NormalizedEvent::ExecutionBegin { execution, .. } = event
            && let Some(current) = self.model.execution(&execution.execution_id)
            && !current.state.is_terminal()
        {
            self.activate_for_live_execution(
                current.task_run_id,
                metadata.receipt_time_ms,
                persist,
            );
        }
        Ok(())
    }

    fn apply_binding(
        &mut self,
        evidence: BindingEvidence,
        receipt_time_ms: i64,
        persist: &mut PersistBatch,
    ) -> Result<(), ReducerError> {
        let plan = plan_binding(&self.model, &evidence);
        persist.extend(apply_binding_plan_at(
            &mut self.model,
            plan,
            receipt_time_ms,
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
            let display_ordinal = self.tab_ordinal_or_allocate(&tab.tab_id)?;
            self.model.insert_tab(tab.clone());
            persist.push(PersistOp::UpsertTab {
                tab: tab.clone(),
                display_ordinal,
            });
        }
        for pane in &topology.panes {
            let pane = Pane {
                pane_id: pane.pane_id.clone(),
                workspace_id: pane.workspace_id.clone(),
                tab_id: pane.tab_id.clone(),
                terminal_id: pane.terminal_id.clone(),
            };
            let display_ordinal = self.pane_ordinal_or_allocate(&pane.pane_id)?;
            self.model.insert_pane(pane.clone());
            persist.push(PersistOp::UpsertPane {
                pane,
                display_ordinal,
            });
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
        let task_run = TaskRun {
            run_id,
            key,
            display_ordinal: ordinal,
            state: TaskState::Running,
            has_controller_task_state_event: false,
        };
        self.model.insert_task_run(task_run.clone());
        persist.push(self.persist_task_run(task_run, timestamp_ms));
        Ok(run_id)
    }

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
                        matches!(run.key, RunKey::Controller(_) | RunKey::Native { .. })
                    })
            })
            .map(|node| node.task_run_id);
        let first = runs.next()?;
        runs.all(|run_id| run_id == first).then_some(first)
    }

    fn persist_task_run(&self, task_run: TaskRun, timestamp_ms: i64) -> PersistOp {
        let native_session = native_binding(&self.model, task_run.run_id);
        let finished_at_ms = task_run.state.is_terminal().then_some(timestamp_ms);
        PersistOp::UpsertTaskRun(PersistTaskRun {
            task_run,
            native_session,
            created_at_ms: timestamp_ms,
            updated_at_ms: timestamp_ms,
            finished_at_ms,
        })
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
        if execution_ids.is_empty() {
            return Vec::new();
        }
        let mut persist = Vec::new();
        for execution_id in execution_ids {
            self.end_execution(&execution_id, now_ms, &mut persist);
        }
        self.recompute_dangling_announcement_components();
        self.operator.apply_submission(&persist);
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

    fn publish(&self) {
        #[cfg(test)]
        self.publish_count.set(self.publish_count.get() + 1);
        // increment5-workload-harness: begin reducer clone publication timing start
        #[cfg(feature = "workload-harness")]
        let workload_publish_started = Instant::now();
        // increment5-workload-harness: end reducer clone publication timing start
        self.publisher.send_replace(Arc::new(self.model.clone()));
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
            ) {
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
        ControllerEventKind::Complete
        | ControllerEventKind::Failed
        | ControllerEventKind::Cancelled => {
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
    }
}

fn controller_target_state(event: &ControllerEventKind) -> Option<TaskState> {
    match event {
        ControllerEventKind::Dispatch { .. } | ControllerEventKind::DependsOn { .. } => None,
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
) -> TaskState {
    match controller_event_kind(source_event_type) {
        LegacyControllerEventKind::Started => match current {
            TaskState::Queued | TaskState::Blocked | TaskState::EndedUnknown => TaskState::Running,
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
    let seq = u64::try_from(ordinal.get()).map_or(0, |value| value);
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
    use std::sync::Arc;

    use crate::diagnostics::RuntimeWriteOutcome;
    use crate::lockfile::StateRoot;
    use crate::model::{
        AgentNode, AgentNodeObservation, AgentSessionReference, AgentSessionReferenceKind,
        ControllerEvent, ControllerEventKind, DependencyEdge, DisplayOrdinal, DomainModel,
        EventMetadata, ExecState, Execution, ExecutionEdge, GapKind, MinimalProviderMetadata,
        NormalizedEvent, Pane, PaneSnapshot, Provider, ReconcileBatch, RunId, RunKey, SharedModel,
        SnapshotAgent, Tab, TaskRun, TaskState, TopologyEntity, TopologyEntityId, TopologySnapshot,
        Workspace,
    };
    use crate::store::{
        NativeSessionBinding, PersistOp, PersistTaskRun, RestoredState, WriterClient,
        database_path, open_reader, open_writer, spawn_writer,
    };

    use super::{ApplyOutcome, CommitStagedError, Reducer, ReducerError, RejectReason};

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
        writer: &WriterClient,
        event: ControllerEvent,
    ) {
        let delta = reducer.validate_controller_event(&event).unwrap();
        let permit = writer.reserve_enqueue().unwrap();
        reducer
            .commit_staged(delta, permit)
            .unwrap()
            .wait()
            .await
            .unwrap();
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

    fn run(run_id: RunId, key: RunKey, ordinal: i64, state: TaskState) -> TaskRun {
        TaskRun {
            run_id,
            key,
            display_ordinal: DisplayOrdinal::new(ordinal),
            state,
            has_controller_task_state_event: false,
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
            None
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
            entity: TopologyEntity::Workspace(Workspace {
                workspace_id: workspace_id.to_owned(),
            }),
        }
    }

    fn topology_entity_event(event_id: &str, entity: TopologyEntity) -> NormalizedEvent {
        NormalizedEvent::TopologyUpsert {
            metadata: metadata(event_id, 1_000),
            entity,
        }
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
                })
                .collect(),
            panes: panes
                .iter()
                .map(|(pane_id, workspace_id, tab_id)| PaneSnapshot {
                    pane_id: (*pane_id).to_owned(),
                    workspace_id: (*workspace_id).to_owned(),
                    tab_id: (*tab_id).to_owned(),
                    terminal_id: format!("terminal-{pane_id}"),
                    agent: None,
                    agent_session: None,
                })
                .collect(),
        }
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
                }),
            ),
            (
                "pane-keep",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane-keep".to_owned(),
                    workspace_id: "workspace-keep".to_owned(),
                    tab_id: "tab-keep".to_owned(),
                    terminal_id: "terminal-keep".to_owned(),
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
                }),
            ),
            (
                "round-trip-pane",
                TopologyEntity::Pane(Pane {
                    pane_id: "pane".to_owned(),
                    workspace_id: "workspace".to_owned(),
                    tab_id: "tab".to_owned(),
                    terminal_id: "terminal".to_owned(),
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
                agent: Some(SnapshotAgent {
                    agent_name: "codex".to_owned(),
                    state: ExecState::Working,
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
        assert!(shared.borrow().executions().any(|value| {
            value.task_run_id == run_id
                && value.execution_id != "pre-gap"
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, _shared) = Reducer::new(restored(DomainModel::default(), 1));

        for (event_id, raw) in [("sequence-1", "raw-1"), ("sequence-2", "raw-2")] {
            let mut event = controller_event(event_id, raw, ControllerEventKind::TaskStarted);
            event.metadata.receipt_time_ms = super::unix_now_ms();
            let delta = reducer.validate_controller_event(&event).unwrap();
            let permit = writer.reserve_enqueue().expect("writer must have capacity");
            reducer
                .commit_staged(delta, permit)
                .expect("sequence must be available")
                .wait()
                .await
                .unwrap();
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        assert!(
            writer
                .apply(vec![PersistOp::UpsertTab {
                    tab: crate::model::Tab {
                        tab_id: "orphan-tab".to_owned(),
                        workspace_id: "missing-workspace".to_owned(),
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
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
            reducer
                .commit_staged(delta, permit)
                .unwrap()
                .wait()
                .await
                .unwrap();
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &writer,
            controller_event("parent-started", "parent", ControllerEventKind::TaskStarted),
        )
        .await;
        commit_controller(
            &mut reducer,
            &writer,
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &writer,
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
            &writer,
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &writer,
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &writer,
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
            &writer,
            controller_event(
                "first-resolved",
                "child-1",
                ControllerEventKind::TaskStarted,
            ),
        )
        .await;
        commit_controller(
            &mut reducer,
            &writer,
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        for (event_id, child, parent) in [
            ("island-1", "child-1", "parent-1"),
            ("island-2", "child-2", "parent-2"),
        ] {
            commit_controller(
                &mut reducer,
                &writer,
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &writer,
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));

        commit_controller(
            &mut reducer,
            &writer,
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
            &writer,
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored(DomainModel::default(), 1));
        let delta = reducer
            .validate_controller_event(&controller_event(
                "commit",
                "raw",
                ControllerEventKind::TaskStarted,
            ))
            .unwrap();
        let permit = writer.reserve_enqueue().unwrap();

        reducer
            .commit_staged(delta, permit)
            .unwrap()
            .wait()
            .await
            .unwrap();
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
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
        let (lifecycle, writer) = spawn_writer(store).unwrap();
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
        reducer
            .commit_staged(delta, permit)
            .unwrap()
            .wait()
            .await
            .unwrap();
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
