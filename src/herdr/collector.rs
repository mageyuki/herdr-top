//! T9 subscribe/buffer/snapshot/replay collector, convergence, and gap reconciliation.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::env;
use std::ffi::OsStr;
use std::future::{Future, pending};
use std::io::{self, Read};
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::activity::{OperatorSnapshot, RestoredOperatorState};
use crate::diagnostics::{
    ControllerInputStatus, ControllerInputUnavailableReason, DiagnosticSource,
    EnrichmentCounterSnapshot, InputAvailability, OccurrenceLogStatus, OwnerFreshness,
    PersistenceCounters, PersistenceOccurrenceSink, PrimaryStreamCounterSnapshot,
    PrimaryStreamDiagnosticsHandle, RuntimeDiagnosticsSnapshot, RuntimeWriteOutcome,
    SourceCoverageSnapshot, controller_counter_snapshot, encode_persistence_occurrence,
};
use crate::lockfile::OwnerRecord;
use crate::model::{
    AgentNodeObservation, AgentSessionReference, AgentSessionReferenceKind,
    ControllerDiagnosticsHandle, DomainModel, EnrichmentDiagnosticsHandle, EventMetadata,
    ExecState, Execution, GapKind, MinimalProviderMetadata, NormalizedEvent, OperatorCommand, Pane,
    PaneSnapshot, Provider, ReconcileBatch, RunId, RunKey, SharedModel, SnapshotAgent,
    SourceCoverage, Tab, TopologyEntity, TopologyEntityId, TopologySnapshot, Workspace,
    sanitize_controller_text,
};
use crate::performance::{
    Admission, Admitted, PerformanceClock, PerformanceIngress, PerformanceSampler,
    PerformanceSnapshot, SystemPerformanceClock, admitted_channel, performance_tracker,
};
use crate::provider::lane::LogLaneConfig;
#[cfg(test)]
use crate::provider::spawn_provider_thread_with_diagnostics;
use crate::provider::{
    BootstrapIdentity, BootstrapParser, DiscoveryIndex, DiscoveryRoot, FsReadBoundary,
    MergeOutcome, PathInterner, PendingEvents, ProviderCycle, ProviderEvent, ProviderIngressEvent,
    ProviderSourceState, ProviderSpawnError, ProviderTarget, ProviderTargetPublisher,
    ProviderThreadError, ProviderThreadHandle, ProviderWorker, RecommendedNotifyFactory, TailFile,
    TargetSet, spawn_provider_thread_with_diagnostics_and_performance,
};
use crate::reducer::{ApplyOutcome, CommitStagedError, Reducer, ReducerError};
use crate::store::writer::{
    DurabilityDisposition, PendingEnqueue, PersistenceFailure, PersistenceStatus, WriterClient,
    WriterError,
};
use crate::store::{CollectorGap, PersistBatch, PersistOp, RestoredState};

use super::controller::{
    self, ControllerRequestReceiver, ControllerRuntimeEvent, ControllerServerError,
};
use super::types::{AgentSessionKind, PaneInfo, Snapshot, Subscription, TabInfo, WorkspaceInfo};
use super::wire::{self, EventStream, WireError};

const EVENT_QUEUE_CAPACITY: usize = 64;
const ENRICHMENT_QUEUE_CAPACITY: usize = 64;
const RESNAPSHOT_ATTEMPTS: usize = 3;
const RECONNECT_DELAY: Duration = Duration::from_millis(50);
const ENRICHMENT_RECONNECT_MAX_DEFERRAL: Duration = RECONNECT_DELAY.saturating_mul(5);
const DRAIN_QUIET_PERIOD: Duration = Duration::from_millis(5);
const STALE_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const PERFORMANCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LivenessPolicy {
    timeout_ms: i64,
}

impl Default for LivenessPolicy {
    fn default() -> Self {
        Self { timeout_ms: 30_000 }
    }
}

fn liveness_timeout(policy: &LivenessPolicy) -> Duration {
    Duration::from_millis(u64::try_from(policy.timeout_ms.max(0)).unwrap_or(0))
}

fn silence_deadline(last_event_at: Instant, policy: &LivenessPolicy) -> Instant {
    last_event_at + liveness_timeout(policy)
}

fn backoff_delay_ms(consecutive_failures: u32) -> u64 {
    if consecutive_failures >= 6 {
        60_000
    } else {
        1_000_u64
            .saturating_mul(1_u64 << consecutive_failures)
            .min(60_000)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReconnectBackoff {
    consecutive_failures: u32,
}

impl ReconnectBackoff {
    fn on_watchdog_silence(&mut self) -> u64 {
        let delay_ms = backoff_delay_ms(self.consecutive_failures);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        delay_ms
    }

    fn on_event(&mut self) {
        self.consecutive_failures = 0;
    }
}

#[derive(Default)]
struct HealthEdge {
    failed: AtomicBool,
}

impl HealthEdge {
    fn record_failure(&self) -> bool {
        !self.failed.swap(true, Ordering::Relaxed)
    }

    fn record_recovery(&self) -> bool {
        self.failed.swap(false, Ordering::Relaxed)
    }
}

#[derive(Default)]
struct EnrichmentHealth {
    subscription: HealthEdge,
    stream: HealthEdge,
}

/// Current quality of the Herdr physical-state observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationQuality {
    /// Subscription is active and the latest buffer generation drained cleanly.
    Live,
    /// The collector is restoring, reconnecting, or resnapshotting.
    Reconciling,
    /// The Herdr socket or subscription is unavailable.
    Disconnected,
    /// Physical state is available but another source or target is unavailable.
    Degraded,
}

/// One coherent performance snapshot and the quality derived from the same generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PerformancePublication {
    pub snapshot: PerformanceSnapshot,
    pub effective_quality: ObservationQuality,
    #[cfg(feature = "workload-harness")]
    #[doc(hidden)]
    pub workload_sample_stamp: Option<WorkloadSampleStamp>,
}

/// Feature-only identity and monotonic timestamp for one raw monitor sample.
#[cfg(feature = "workload-harness")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct WorkloadSampleStamp {
    pub sample_ordinal: u64,
    pub sampled_at_ns: u64,
}

/// Feature-only raw monitor observation retained before watch coalescing.
#[cfg(feature = "workload-harness")]
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct WorkloadPerformanceSample {
    pub source_quality: ObservationQuality,
    pub publication: PerformancePublication,
}

/// Feature-only observer for every raw performance-monitor generation.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
pub type WorkloadPerformanceObserver =
    Arc<dyn Fn(&WorkloadPerformanceSample) + Send + Sync + 'static>;

fn initial_performance_publication() -> PerformancePublication {
    PerformancePublication {
        snapshot: PerformanceSnapshot::default(),
        effective_quality: ObservationQuality::Reconciling,
        #[cfg(feature = "workload-harness")]
        workload_sample_stamp: None,
    }
}

fn compose_quality(
    source_quality: ObservationQuality,
    snapshot: &PerformanceSnapshot,
) -> ObservationQuality {
    match source_quality {
        ObservationQuality::Disconnected => ObservationQuality::Disconnected,
        ObservationQuality::Reconciling => ObservationQuality::Reconciling,
        ObservationQuality::Degraded => ObservationQuality::Degraded,
        ObservationQuality::Live if snapshot.reasons.is_empty() => ObservationQuality::Live,
        ObservationQuality::Live => ObservationQuality::Degraded,
    }
}

fn same_render_payload(left: &PerformancePublication, right: &PerformancePublication) -> bool {
    left.snapshot == right.snapshot && left.effective_quality == right.effective_quality
}

#[cfg(feature = "workload-harness")]
fn publish_performance_generation(
    performance_sender: &watch::Sender<PerformancePublication>,
    quality_sender: &watch::Sender<ObservationQuality>,
    source_quality: ObservationQuality,
    publication: PerformancePublication,
    observer: Option<&WorkloadPerformanceObserver>,
    #[cfg(test)] after_performance_publication: Option<&(dyn Fn() + Send + Sync)>,
) {
    assert!(
        publication.workload_sample_stamp.is_some(),
        "workload observer samples must carry their exact monitor stamp"
    );
    if let Some(observer) = observer {
        observer(&WorkloadPerformanceSample {
            source_quality,
            publication: publication.clone(),
        });
    }
    performance_sender.send_if_modified(|current| {
        if current.workload_sample_stamp.is_none() || !same_render_payload(current, &publication) {
            *current = publication.clone();
            true
        } else {
            false
        }
    });
    #[cfg(test)]
    if let Some(after_performance_publication) = after_performance_publication {
        after_performance_publication();
    }
    quality_sender.send_if_modified(|current| {
        if *current == publication.effective_quality {
            false
        } else {
            *current = publication.effective_quality;
            true
        }
    });
}

#[cfg(feature = "workload-harness")]
fn record_monitor_control_failure() {
    tracing::warn!(
        warning_code = "performance_monitor_control_failure",
        "performance monitor stopped after an internal control value overflowed"
    );
}

#[allow(clippy::too_many_arguments)]
fn spawn_performance_monitor(
    mut sampler: PerformanceSampler,
    model: SharedModel,
    operator: watch::Receiver<OperatorSnapshot>,
    mut source_quality: watch::Receiver<ObservationQuality>,
    performance_sender: watch::Sender<PerformancePublication>,
    quality_sender: watch::Sender<ObservationQuality>,
    cancellation: CancellationToken,
    #[cfg(feature = "workload-harness")] performance_observer: Option<WorkloadPerformanceObserver>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PERFORMANCE_SAMPLE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        #[cfg(feature = "workload-harness")]
        let mut sample_ordinal = 0_u64;
        loop {
            tokio::select! {
                () = cancellation.cancelled() => break,
                _ = interval.tick() => {}
                changed = source_quality.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }

            let snapshot = sampler.sample(&model.borrow(), &operator.borrow(), unix_now_ms());
            let source_quality = *source_quality.borrow();
            let effective_quality = compose_quality(source_quality, &snapshot);

            #[cfg(feature = "workload-harness")]
            {
                let sampled_at_ns = match u64::try_from(sampler.workload_sampled_at().as_nanos()) {
                    Ok(value) => value,
                    Err(_) => {
                        record_monitor_control_failure();
                        break;
                    }
                };
                let current_ordinal = sample_ordinal;
                sample_ordinal = match sample_ordinal.checked_add(1) {
                    Some(next) => next,
                    None => {
                        record_monitor_control_failure();
                        break;
                    }
                };
                let publication = PerformancePublication {
                    snapshot,
                    effective_quality,
                    workload_sample_stamp: Some(WorkloadSampleStamp {
                        sample_ordinal: current_ordinal,
                        sampled_at_ns,
                    }),
                };
                publish_performance_generation(
                    &performance_sender,
                    &quality_sender,
                    source_quality,
                    publication,
                    performance_observer.as_ref(),
                    #[cfg(test)]
                    None,
                );
            }

            #[cfg(not(feature = "workload-harness"))]
            {
                let publication = PerformancePublication {
                    snapshot,
                    effective_quality,
                };
                performance_sender.send_if_modified(|current| {
                    if same_render_payload(current, &publication) {
                        false
                    } else {
                        *current = publication.clone();
                        true
                    }
                });
                quality_sender.send_if_modified(|current| {
                    if *current == effective_quality {
                        false
                    } else {
                        *current = effective_quality;
                        true
                    }
                });
            }
        }
    })
}

/// One of the four fixed observation and input sources shown in coverage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageSource {
    Herdr,
    Controller,
    Claude,
    Codex,
}

/// Tri-state availability retained independently for each fixed source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceAvailability {
    Available,
    Unavailable { detail: String },
    NotApplicable,
}

/// Latest coverage for Herdr, Controller input, Claude, and Codex.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCoverageRegistry {
    herdr: SourceAvailability,
    controller: SourceAvailability,
    claude: SourceAvailability,
    codex: SourceAvailability,
}

impl SourceCoverageRegistry {
    /// Creates startup coverage; optional providers begin as not applicable.
    #[must_use]
    pub fn new(controller: SourceAvailability) -> Self {
        Self {
            herdr: SourceAvailability::Available,
            controller,
            claude: SourceAvailability::NotApplicable,
            codex: SourceAvailability::NotApplicable,
        }
    }

    /// Returns one source's latest tri-state.
    #[must_use]
    pub const fn state(&self, source: CoverageSource) -> &SourceAvailability {
        match source {
            CoverageSource::Herdr => &self.herdr,
            CoverageSource::Controller => &self.controller,
            CoverageSource::Claude => &self.claude,
            CoverageSource::Codex => &self.codex,
        }
    }

    /// Replaces one source's state.
    pub fn set(&mut self, source: CoverageSource, state: SourceAvailability) {
        *match source {
            CoverageSource::Herdr => &mut self.herdr,
            CoverageSource::Controller => &mut self.controller,
            CoverageSource::Claude => &mut self.claude,
            CoverageSource::Codex => &mut self.codex,
        } = state;
    }

    /// Stable header summary without operational paths.
    #[must_use]
    pub fn summary(&self) -> String {
        [
            ("herdr", &self.herdr),
            ("controller", &self.controller),
            ("claude", &self.claude),
            ("codex", &self.codex),
        ]
        .into_iter()
        .map(|(name, state)| match state {
            SourceAvailability::Available => format!("{name}=available"),
            SourceAvailability::Unavailable { detail } => {
                format!("{name}=unavailable({detail})")
            }
            SourceAvailability::NotApplicable => format!("{name}=n/a"),
        })
        .collect::<Vec<_>>()
        .join(";")
    }

    fn provider_metadata(&self) -> Vec<SourceCoverage> {
        [
            ("herdr", &self.herdr),
            ("controller", &self.controller),
            ("claude", &self.claude),
            ("codex", &self.codex),
        ]
        .into_iter()
        .filter_map(|(source, state)| match state {
            SourceAvailability::Available => Some(SourceCoverage {
                source: source.to_owned(),
                available: true,
                detail: None,
            }),
            SourceAvailability::Unavailable { detail } => Some(SourceCoverage {
                source: source.to_owned(),
                available: false,
                detail: Some(detail.clone()),
            }),
            SourceAvailability::NotApplicable => None,
        })
        .collect()
    }

    fn effective_quality(&self, herdr_quality: ObservationQuality) -> ObservationQuality {
        if herdr_quality == ObservationQuality::Live
            && [&self.claude, &self.codex]
                .into_iter()
                .any(|state| matches!(state, SourceAvailability::Unavailable { .. }))
        {
            ObservationQuality::Degraded
        } else {
            herdr_quality
        }
    }
}

impl Default for SourceCoverageRegistry {
    fn default() -> Self {
        Self::new(SourceAvailability::NotApplicable)
    }
}

#[derive(Clone, Copy)]
enum RuntimeCommandClass {
    Batch,
    OwnerLocation,
}

struct UnavailableOccurrenceSink;

impl PersistenceOccurrenceSink for UnavailableOccurrenceSink {
    fn append(&self, _record: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no process-owned occurrence log was supplied",
        ))
    }
}

pub(crate) struct RuntimePersistence {
    writer: WriterClient,
    writer_health: watch::Receiver<PersistenceStatus>,
    snapshot: RuntimeDiagnosticsSnapshot,
    publisher: watch::Sender<RuntimeDiagnosticsSnapshot>,
    occurrence_sink: Arc<dyn PersistenceOccurrenceSink>,
    occurrence_attempted: bool,
    acceptor_diagnostics: ControllerDiagnosticsHandle,
    enrichment_diagnostics: EnrichmentDiagnosticsHandle,
}

impl RuntimePersistence {
    fn new(
        writer: WriterClient,
        model: &DomainModel,
        coverage: &SourceCoverageRegistry,
        occurrence_sink: Arc<dyn PersistenceOccurrenceSink>,
    ) -> (Self, watch::Receiver<RuntimeDiagnosticsSnapshot>) {
        let writer_health = writer.subscribe_persistence();
        let controller_input =
            controller_input_from_coverage(coverage.state(CoverageSource::Controller));
        let controller_counters = controller_counter_snapshot(model);
        let acceptor_diagnostics = model.controller_diagnostics().acceptor_handle();
        let enrichment_diagnostics = EnrichmentDiagnosticsHandle::default();
        let snapshot = RuntimeDiagnosticsSnapshot {
            persistence: writer.persistence_status(),
            controller_input,
            owner: OwnerFreshness::Current,
            persistence_counters: PersistenceCounters::default(),
            controller_counters,
            enrichment_counters: EnrichmentCounterSnapshot::default(),
            source_coverage: diagnostic_source_coverage(coverage, controller_input),
            dangling_announcement_components: controller_counters.dangling_announcement_components,
            first_failure_log: OccurrenceLogStatus::NotAttempted,
        };
        let (publisher, diagnostics) = watch::channel(snapshot.clone());
        (
            Self {
                writer,
                writer_health,
                snapshot,
                publisher,
                occurrence_sink,
                occurrence_attempted: false,
                acceptor_diagnostics,
                enrichment_diagnostics,
            },
            diagnostics,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        writer: WriterClient,
        occurrence_sink: Arc<dyn PersistenceOccurrenceSink>,
    ) -> (Self, watch::Receiver<RuntimeDiagnosticsSnapshot>) {
        Self::new(
            writer,
            &DomainModel::default(),
            &SourceCoverageRegistry::new(SourceAvailability::Available),
            occurrence_sink,
        )
    }

    fn diagnostics(&self) -> watch::Receiver<RuntimeDiagnosticsSnapshot> {
        self.publisher.subscribe()
    }

    fn enrichment_diagnostics(&self) -> EnrichmentDiagnosticsHandle {
        self.enrichment_diagnostics.clone()
    }

    pub(crate) fn is_duplicate(&self, event_id: &str) -> bool {
        self.writer.is_duplicate(event_id)
    }

    pub(crate) fn reserve_enqueue(&mut self) -> Option<crate::store::EnqueuePermit<'_>> {
        self.observe_writer_health();
        if self.snapshot.persistence != PersistenceStatus::Healthy {
            return None;
        }

        let Self {
            writer,
            writer_health,
            snapshot,
            publisher,
            occurrence_sink,
            occurrence_attempted,
            acceptor_diagnostics,
            enrichment_diagnostics,
        } = self;
        let permit = writer.reserve_enqueue();
        let status = {
            let status = writer_health.borrow();
            *status
        };
        Self::ingest_writer_status(
            status,
            snapshot,
            publisher,
            occurrence_sink.as_ref(),
            occurrence_attempted,
            acceptor_diagnostics,
            enrichment_diagnostics,
        );
        match status {
            PersistenceStatus::Healthy => permit,
            PersistenceStatus::Degraded { .. } => {
                drop(permit);
                None
            }
        }
    }

    pub(crate) async fn finish_pending(
        &mut self,
        pending: PendingEnqueue,
    ) -> Result<RuntimeWriteOutcome, WriterError> {
        let result = self.writer.finish_pending(pending).await.map(|_| ());
        self.classify_result(result, RuntimeCommandClass::Batch)
    }

    async fn apply(&mut self, batch: PersistBatch) -> Result<RuntimeWriteOutcome, WriterError> {
        if self.skip_if_degraded(RuntimeCommandClass::Batch) {
            return Ok(RuntimeWriteOutcome::Skipped);
        }
        let result = self.writer.apply(batch).await;
        self.classify_result(result, RuntimeCommandClass::Batch)
    }

    async fn cleanup(&mut self, now_ms: i64) -> Result<RuntimeWriteOutcome, WriterError> {
        if self.skip_if_degraded(RuntimeCommandClass::Batch) {
            return Ok(RuntimeWriteOutcome::Skipped);
        }
        let result = self.writer.cleanup(now_ms).await.map(|_| ());
        self.classify_result(result, RuntimeCommandClass::Batch)
    }

    async fn update_owner_location(
        &mut self,
        terminal_id: &str,
        pane_id: &str,
    ) -> Result<RuntimeWriteOutcome, WriterError> {
        if self.skip_if_degraded(RuntimeCommandClass::OwnerLocation) {
            return Ok(RuntimeWriteOutcome::Skipped);
        }
        let result = self
            .writer
            .update_owner_location(terminal_id, pane_id)
            .await;
        self.classify_result(result, RuntimeCommandClass::OwnerLocation)
    }

    fn classify_result(
        &mut self,
        result: Result<(), WriterError>,
        class: RuntimeCommandClass,
    ) -> Result<RuntimeWriteOutcome, WriterError> {
        match result {
            Ok(()) => Ok(RuntimeWriteOutcome::Durable),
            Err(WriterError::Persistence(failure)) => Ok(self.record_failure(failure, class)),
            Err(error) => Err(error),
        }
    }

    fn skip_if_degraded(&mut self, class: RuntimeCommandClass) -> bool {
        self.observe_writer_health();
        if self.snapshot.persistence == PersistenceStatus::Healthy {
            return false;
        }
        match class {
            RuntimeCommandClass::Batch => {
                self.snapshot.persistence_counters.skipped_batches = self
                    .snapshot
                    .persistence_counters
                    .skipped_batches
                    .saturating_add(1);
            }
            RuntimeCommandClass::OwnerLocation => {
                self.snapshot.owner = OwnerFreshness::Stale;
                self.snapshot.persistence_counters.skipped_owner_updates = self
                    .snapshot
                    .persistence_counters
                    .skipped_owner_updates
                    .saturating_add(1);
            }
        }
        self.publish();
        true
    }

    fn observe_writer_health(&mut self) {
        let status = {
            let status = self.writer_health.borrow();
            *status
        };
        let Self {
            snapshot,
            publisher,
            occurrence_sink,
            occurrence_attempted,
            acceptor_diagnostics,
            enrichment_diagnostics,
            ..
        } = self;
        Self::ingest_writer_status(
            status,
            snapshot,
            publisher,
            occurrence_sink.as_ref(),
            occurrence_attempted,
            acceptor_diagnostics,
            enrichment_diagnostics,
        );
    }

    fn ingest_writer_status(
        status: PersistenceStatus,
        snapshot: &mut RuntimeDiagnosticsSnapshot,
        publisher: &watch::Sender<RuntimeDiagnosticsSnapshot>,
        occurrence_sink: &dyn PersistenceOccurrenceSink,
        occurrence_attempted: &mut bool,
        acceptor_diagnostics: &ControllerDiagnosticsHandle,
        enrichment_diagnostics: &EnrichmentDiagnosticsHandle,
    ) {
        if snapshot.persistence != PersistenceStatus::Healthy {
            return;
        }
        if let PersistenceStatus::Degraded { failure } = status {
            let class = if failure.operation
                == crate::store::writer::PersistenceOperation::UpdateOwnerLocation
            {
                RuntimeCommandClass::OwnerLocation
            } else {
                RuntimeCommandClass::Batch
            };
            let _ = Self::record_facade_failure(
                failure,
                class,
                snapshot,
                publisher,
                occurrence_sink,
                occurrence_attempted,
                acceptor_diagnostics,
                enrichment_diagnostics,
            );
        }
    }

    fn record_failure(
        &mut self,
        failure: PersistenceFailure,
        class: RuntimeCommandClass,
    ) -> RuntimeWriteOutcome {
        Self::record_facade_failure(
            failure,
            class,
            &mut self.snapshot,
            &self.publisher,
            self.occurrence_sink.as_ref(),
            &mut self.occurrence_attempted,
            &self.acceptor_diagnostics,
            &self.enrichment_diagnostics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn record_facade_failure(
        failure: PersistenceFailure,
        class: RuntimeCommandClass,
        snapshot: &mut RuntimeDiagnosticsSnapshot,
        publisher: &watch::Sender<RuntimeDiagnosticsSnapshot>,
        occurrence_sink: &dyn PersistenceOccurrenceSink,
        occurrence_attempted: &mut bool,
        acceptor_diagnostics: &ControllerDiagnosticsHandle,
        enrichment_diagnostics: &EnrichmentDiagnosticsHandle,
    ) -> RuntimeWriteOutcome {
        let outcome = match failure.durability {
            DurabilityDisposition::Committed => RuntimeWriteOutcome::CommittedButDegraded(failure),
            DurabilityDisposition::Unknown => RuntimeWriteOutcome::DurabilityUnknown(failure),
            DurabilityDisposition::NotCommitted | DurabilityDisposition::NotApplicable => {
                RuntimeWriteOutcome::NotCommitted(failure)
            }
        };
        if snapshot.persistence != PersistenceStatus::Healthy {
            return outcome;
        }

        snapshot.persistence = PersistenceStatus::Degraded { failure };
        snapshot.controller_input = ControllerInputStatus::Unavailable {
            reason: ControllerInputUnavailableReason::PersistenceUnavailable,
        };
        set_controller_coverage_unavailable(&mut snapshot.source_coverage);
        match class {
            RuntimeCommandClass::OwnerLocation => {
                snapshot.owner = OwnerFreshness::Stale;
            }
            RuntimeCommandClass::Batch => match outcome {
                RuntimeWriteOutcome::CommittedButDegraded(_) => {
                    snapshot.persistence_counters.committed_but_degraded_batches = snapshot
                        .persistence_counters
                        .committed_but_degraded_batches
                        .saturating_add(1);
                }
                RuntimeWriteOutcome::NotCommitted(_) => {
                    snapshot.persistence_counters.not_committed_batches = snapshot
                        .persistence_counters
                        .not_committed_batches
                        .saturating_add(1);
                }
                RuntimeWriteOutcome::DurabilityUnknown(_) => {
                    snapshot.persistence_counters.durability_unknown_batches = snapshot
                        .persistence_counters
                        .durability_unknown_batches
                        .saturating_add(1);
                }
                RuntimeWriteOutcome::Durable | RuntimeWriteOutcome::Skipped => {}
            },
        }
        if !*occurrence_attempted {
            *occurrence_attempted = true;
            let record = encode_persistence_occurrence(
                unix_now_ms(),
                failure,
                snapshot.persistence_counters,
            );
            snapshot.first_failure_log = if occurrence_sink.append(&record).is_ok() {
                OccurrenceLogStatus::Emitted
            } else {
                OccurrenceLogStatus::Failed
            };
        }
        Self::publish_facade(
            snapshot,
            publisher,
            acceptor_diagnostics,
            enrichment_diagnostics,
        );
        outcome
    }

    fn refresh_snapshot(&mut self, model: &DomainModel, coverage: &SourceCoverageRegistry) {
        let controller_counters = controller_counter_snapshot(model);
        self.snapshot.controller_counters = controller_counters;
        self.snapshot.dangling_announcement_components =
            controller_counters.dangling_announcement_components;
        self.snapshot.source_coverage =
            diagnostic_source_coverage(coverage, self.snapshot.controller_input);
        self.publish();
    }

    fn mark_acceptor_stopped(&mut self) {
        if self.snapshot.persistence == PersistenceStatus::Healthy {
            self.snapshot.controller_input = ControllerInputStatus::Unavailable {
                reason: ControllerInputUnavailableReason::AcceptorStopped,
            };
            set_controller_coverage_unavailable(&mut self.snapshot.source_coverage);
            self.publish();
        }
    }

    fn publish(&mut self) {
        Self::publish_facade(
            &mut self.snapshot,
            &self.publisher,
            &self.acceptor_diagnostics,
            &self.enrichment_diagnostics,
        );
    }

    fn publish_facade(
        snapshot: &mut RuntimeDiagnosticsSnapshot,
        publisher: &watch::Sender<RuntimeDiagnosticsSnapshot>,
        acceptor_diagnostics: &ControllerDiagnosticsHandle,
        enrichment_diagnostics: &EnrichmentDiagnosticsHandle,
    ) {
        snapshot.controller_counters.socket_saturations = acceptor_diagnostics.socket_saturations();
        snapshot.controller_counters.accept_failures = acceptor_diagnostics.accept_failures();
        snapshot.enrichment_counters = EnrichmentCounterSnapshot {
            channel_full_drops: enrichment_diagnostics.channel_full_drops(),
            episode_discards: enrichment_diagnostics.episode_discards(),
        };
        let publication = snapshot.clone();
        publisher.send_if_modified(|current| {
            if *current == publication {
                false
            } else {
                *current = publication;
                true
            }
        });
    }
}

async fn persist_submission(
    persistence: &mut RuntimePersistence,
    reducer: &mut Reducer,
    batch: PersistBatch,
) -> Result<RuntimeWriteOutcome, WriterError> {
    let outcome = persistence.apply(batch).await?;
    reducer.complete_operator_submission(outcome);
    Ok(outcome)
}

fn set_controller_coverage_unavailable(coverage: &mut [SourceCoverageSnapshot]) {
    if let Some(controller) = coverage
        .iter_mut()
        .find(|snapshot| snapshot.source == DiagnosticSource::Controller)
    {
        controller.availability = InputAvailability::Unavailable;
    }
}

fn controller_input_from_coverage(coverage: &SourceAvailability) -> ControllerInputStatus {
    match coverage {
        SourceAvailability::Available => ControllerInputStatus::Available,
        SourceAvailability::NotApplicable => ControllerInputStatus::Unavailable {
            reason: ControllerInputUnavailableReason::ListenerUnavailable,
        },
        SourceAvailability::Unavailable { detail }
            if matches!(detail.as_str(), "not_bound" | "bind_failure") =>
        {
            ControllerInputStatus::Unavailable {
                reason: ControllerInputUnavailableReason::ListenerUnavailable,
            }
        }
        SourceAvailability::Unavailable { .. } => ControllerInputStatus::Unavailable {
            reason: ControllerInputUnavailableReason::RuntimeUnsafe,
        },
    }
}

fn diagnostic_source_coverage(
    coverage: &SourceCoverageRegistry,
    controller_input: ControllerInputStatus,
) -> Vec<SourceCoverageSnapshot> {
    [
        (DiagnosticSource::Herdr, CoverageSource::Herdr),
        (DiagnosticSource::Controller, CoverageSource::Controller),
        (DiagnosticSource::Claude, CoverageSource::Claude),
        (DiagnosticSource::Codex, CoverageSource::Codex),
    ]
    .into_iter()
    .map(|(source, coverage_source)| SourceCoverageSnapshot {
        source,
        availability: if source == DiagnosticSource::Controller
            && !matches!(controller_input, ControllerInputStatus::Available)
        {
            InputAvailability::Unavailable
        } else {
            match coverage.state(coverage_source) {
                SourceAvailability::Available => InputAvailability::Available,
                SourceAvailability::Unavailable { .. } => InputAvailability::Unavailable,
                SourceAvailability::NotApplicable => InputAvailability::Unavailable,
            }
        },
    })
    .collect()
}

/// Errors surfaced by collector startup or orderly shutdown.
#[derive(Debug, Error)]
pub enum CollectorError {
    /// The SQLite writer rejected an operation.
    #[error(transparent)]
    Writer(#[from] WriterError),
    /// Herdr returned an invalid or failed wire exchange.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// A snapshot pane did not carry the tab identity required by the domain model.
    #[error("snapshot pane {pane_id:?} has no tab_id")]
    MissingTabId { pane_id: String },
    /// A reducer rejected the transition before publication.
    #[error(transparent)]
    Reducer(#[from] ReducerError),
    /// The collector task panicked or was cancelled externally.
    #[error("collector task failed: {0}")]
    Task(String),
    /// The collector ignored cancellation until it had to be aborted and joined.
    #[error("collector did not stop within {seconds} seconds and was aborted")]
    StopTimeout { seconds: u64 },
    /// The Controller socket acceptor could not be started or stopped cleanly.
    #[error(transparent)]
    Controller(#[from] ControllerServerError),
    /// The Controller acceptor task panicked or was cancelled externally.
    #[error("Controller acceptor task failed: {0}")]
    ControllerTask(String),
    /// The provider I/O thread or notify backend could not be started.
    #[error(transparent)]
    ProviderSpawn(#[from] ProviderSpawnError),
    /// The provider I/O thread did not stop cleanly.
    #[error(transparent)]
    ProviderThread(#[from] ProviderThreadError),
}

/// Handle to the collector's coherent model, quality, source-coverage, and diagnostics streams.
pub struct CollectorHandle {
    /// Coherent performance snapshot and effective-quality generations.
    pub performance: watch::Receiver<PerformancePublication>,
    /// Compatibility projection of [`Self::performance`]'s effective quality.
    pub quality: watch::Receiver<ObservationQuality>,
    /// Dynamic four-source coverage published independently of model snapshots.
    pub source_coverage: watch::Receiver<SourceCoverageRegistry>,
    /// Immutable runtime diagnostics; no command-capable writer leaves the collector.
    pub diagnostics: watch::Receiver<RuntimeDiagnosticsSnapshot>,
    /// Immutable bounded operator activity and terminal-timing projection.
    pub operator: watch::Receiver<OperatorSnapshot>,
    /// Coherent reducer-owned domain snapshots.
    pub model: SharedModel,
    primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle,
    cancellation: CancellationToken,
    task: JoinHandle<Result<(), CollectorError>>,
    performance_monitor: JoinHandle<()>,
    controller_acceptor: Option<JoinHandle<Result<(), ControllerServerError>>>,
    provider_thread: Option<ProviderThreadHandle>,
    provider_events_drained: Option<oneshot::Receiver<()>>,
}

impl CollectorHandle {
    /// Returns primary-stream tolerance counters for this collector process.
    #[must_use]
    pub fn primary_stream_counters(&self) -> PrimaryStreamCounterSnapshot {
        self.primary_stream_diagnostics.snapshot()
    }

    /// Cancels the collector and waits for its subscription task to exit.
    pub async fn stop(self) -> Result<(), CollectorError> {
        self.stop_with_timeout(STOP_TIMEOUT).await
    }

    async fn stop_with_timeout(self, timeout: Duration) -> Result<(), CollectorError> {
        self.cancellation.cancel();
        let provider_result = match self.provider_thread {
            Some(provider) => provider.stop().await,
            None => Ok(()),
        };
        let provider_drain_result = if provider_result.is_ok() {
            match self.provider_events_drained {
                Some(mut drained) => match tokio::time::timeout(timeout, &mut drained).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(_)) => Err(CollectorError::Task(
                        "provider event drain acknowledgement dropped".to_owned(),
                    )),
                    Err(_) => Err(CollectorError::StopTimeout {
                        seconds: timeout.as_secs(),
                    }),
                },
                None => Ok(()),
            }
        } else {
            Ok(())
        };
        let mut task = self.task;
        let collector_result = match tokio::time::timeout(timeout, &mut task).await {
            Ok(result) => result
                .map_err(|error| CollectorError::Task(error.to_string()))
                .and_then(std::convert::identity),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(CollectorError::StopTimeout {
                    seconds: timeout.as_secs(),
                })
            }
        };
        let performance_result = self
            .performance_monitor
            .await
            .map_err(|error| CollectorError::Task(error.to_string()));
        let controller_result = if let Some(mut acceptor) = self.controller_acceptor {
            match tokio::time::timeout(timeout, &mut acceptor).await {
                Ok(result) => result
                    .map_err(|error| CollectorError::ControllerTask(error.to_string()))
                    .and_then(|result| result.map_err(CollectorError::from)),
                Err(_) => {
                    acceptor.abort();
                    let _ = acceptor.await;
                    Err(CollectorError::StopTimeout {
                        seconds: timeout.as_secs(),
                    })
                }
            }
        } else {
            Ok(())
        };
        if let Err(error) = &provider_result
            && (collector_result.is_err()
                || performance_result.is_err()
                || controller_result.is_err()
                || provider_drain_result.is_err())
        {
            let provider_error_code = match error {
                ProviderThreadError::ThreadPanicked => "provider_thread_panicked",
                ProviderThreadError::DetachedTimeout => "provider_thread_detached_timeout",
            };
            tracing::warn!(
                warning_code = "provider_shutdown_failure_masked",
                provider_error_code,
                "provider shutdown error masked by earlier shutdown failure"
            );
        }

        collector_result?;
        performance_result?;
        controller_result?;
        provider_result?;
        provider_drain_result?;
        Ok(())
    }
}
// increment5-workload-harness: begin collector workload spawn adapter
/// Feature-only inputs for real-path workload execution.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
pub struct WorkloadCollectorConfig {
    pub controller_hooks: controller::WorkloadControllerHooks,
    pub performance_clock: Arc<dyn PerformanceClock>,
    pub performance_observer: WorkloadPerformanceObserver,
    pub provider_roots: Vec<crate::provider::DiscoveryRoot>,
    pub notify_factory: Option<Box<dyn crate::provider::NotifyFactory>>,
    pub rescan_interval: Option<Duration>,
    pub fallback_timing: Option<(
        crate::reducer::WorkloadTimingKind,
        u64,
        crate::reducer::WorkloadTimingObserver,
    )>,
}

/// Real collector handle plus its feature-only bounded Controller producer.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
pub struct WorkloadCollectorHandle {
    pub collector: CollectorHandle,
    pub controller: controller::ControllerRequestSender,
}

/// Launches workload controls while delegating all event handling to `run_collector`.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
pub async fn spawn_workload_collector(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    mut writer: WriterClient,
    config: WorkloadCollectorConfig,
) -> Result<WorkloadCollectorHandle, CollectorError> {
    let owner = OwnerTracker::from_environment();
    writer.replace_owner(owner.record()).await?;
    let (mut reducer, model, operator) =
        Reducer::new_with_operator(restored, empty_operator_seed());
    if let Some((kind, first_sequence, observer)) = config.fallback_timing {
        reducer.configure_workload_observation_timing(kind, first_sequence, observer);
    }
    let (performance_sender, performance) = watch::channel(initial_performance_publication());
    let (quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
    let (source_quality_sender, source_quality) = watch::channel(ObservationQuality::Reconciling);
    let controller_coverage = SourceAvailability::Available;
    let initial_coverage = SourceCoverageRegistry::new(controller_coverage.clone());
    let (coverage_sender, source_coverage) = watch::channel(initial_coverage);
    let (persistence, diagnostics) = RuntimePersistence::new(
        writer,
        &model.borrow(),
        &source_coverage.borrow(),
        Arc::new(UnavailableOccurrenceSink),
    );
    let cancellation = CancellationToken::new();
    let (performance_ingress, performance_sampler) =
        performance_tracker(Arc::clone(&config.performance_clock));
    let (controller_sender, controller_requests) =
        controller::request_channel_with_workload_harness(
            controller::CONTROLLER_REQUEST_QUEUE_CAPACITY,
            reducer.controller_diagnostics_handle(),
            performance_ingress.clone(),
            config.controller_hooks,
        );
    let (provider_sender, provider_events) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let provider_diagnostics = crate::provider::ProviderDiagnostics::from_model_handle(
        reducer.provider_diagnostics_handle(),
    );
    let provider_thread =
        crate::provider::spawn_provider_thread_with_diagnostics_performance_and_rescan_interval(
            AdapterProviderWorker::new(config.provider_roots, provider_diagnostics.clone()),
            provider_sender,
            config.notify_factory,
            provider_diagnostics,
            performance_ingress.clone(),
            config
                .rescan_interval
                .unwrap_or(crate::provider::RESCAN_INTERVAL),
        )?;
    let provider_publisher = provider_thread.target_publisher();
    let restored_targets = derive_provider_targets(&model.borrow());
    provider_publisher.update_targets(restored_targets.clone());
    let coverage =
        CoverageTracker::new(controller_coverage, coverage_sender, source_quality_sender);
    let (provider_events_drained_sender, provider_events_drained) = oneshot::channel();
    let provider_integration = ProviderIntegration::new_with_drain_acknowledgement(
        provider_events,
        provider_publisher,
        restored_targets,
        coverage,
        provider_events_drained_sender,
    );
    let task_cancellation = cancellation.clone();
    let task_model = model.clone();
    let primary_stream_diagnostics = PrimaryStreamDiagnosticsHandle::default();
    let task_primary_stream_diagnostics = primary_stream_diagnostics.clone();
    let performance_observer = config.performance_observer;
    let performance_monitor = spawn_performance_monitor(
        performance_sampler,
        model.clone(),
        operator.clone(),
        source_quality,
        performance_sender,
        quality_sender,
        cancellation.clone(),
        Some(performance_observer),
    );
    let task = tokio::spawn(async move {
        run_collector(
            sock,
            session,
            persistence,
            reducer,
            task_model,
            performance_ingress,
            task_cancellation,
            owner,
            Some(controller_requests),
            None,
            provider_integration,
            LivenessPolicy::default(),
            task_primary_stream_diagnostics,
        )
        .await
    });
    let collector = CollectorHandle {
        performance,
        quality,
        source_coverage,
        diagnostics,
        operator,
        model,
        primary_stream_diagnostics,
        cancellation,
        task,
        performance_monitor,
        controller_acceptor: None,
        provider_thread: Some(provider_thread),
        provider_events_drained: Some(provider_events_drained),
    };
    Ok(WorkloadCollectorHandle {
        collector,
        controller: controller_sender,
    })
}
// increment5-workload-harness: end collector workload spawn adapter

/// Commits the new owner record, then launches subscribe-first convergence.
pub async fn spawn(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
) -> Result<CollectorHandle, CollectorError> {
    spawn_configured(
        sock,
        session,
        restored,
        writer,
        None,
        SourceAvailability::NotApplicable,
        Arc::new(UnavailableOccurrenceSink),
        empty_operator_seed(),
        None,
    )
    .await
}

/// Commits the owner record and launches convergence plus an optional Controller acceptor.
pub async fn spawn_with_controller(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
    controller_listener: Option<StdUnixListener>,
) -> Result<CollectorHandle, CollectorError> {
    let controller_coverage = if controller_listener.is_some() {
        SourceAvailability::Available
    } else {
        SourceAvailability::Unavailable {
            detail: "not_bound".to_owned(),
        }
    };
    spawn_configured(
        sock,
        session,
        restored,
        writer,
        controller_listener,
        controller_coverage,
        Arc::new(UnavailableOccurrenceSink),
        empty_operator_seed(),
        None,
    )
    .await
}

/// Launches the Controller-enabled runtime with an injected performance clock and raw observer.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn spawn_with_controller_and_performance_clock(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
    controller_listener: Option<StdUnixListener>,
    performance_clock: Arc<dyn PerformanceClock>,
    performance_observer: WorkloadPerformanceObserver,
) -> Result<CollectorHandle, CollectorError> {
    let controller_coverage = if controller_listener.is_some() {
        SourceAvailability::Available
    } else {
        SourceAvailability::Unavailable {
            detail: "not_bound".to_owned(),
        }
    };
    spawn_configured_inner(
        sock,
        session,
        restored,
        writer,
        controller_listener,
        controller_coverage,
        Arc::new(UnavailableOccurrenceSink),
        empty_operator_seed(),
        None,
        HashMap::new(),
        HashSet::new(),
        LogLaneConfig::default(),
        performance_clock,
        Some(performance_observer),
    )
    .await
}

/// Launches convergence with the already-resolved Controller coverage detail.
pub async fn spawn_with_controller_coverage(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
    controller_listener: Option<StdUnixListener>,
    controller_coverage: SourceAvailability,
) -> Result<CollectorHandle, CollectorError> {
    spawn_configured(
        sock,
        session,
        restored,
        writer,
        controller_listener,
        controller_coverage,
        Arc::new(UnavailableOccurrenceSink),
        empty_operator_seed(),
        None,
    )
    .await
}

/// Launches the fully configured runtime with the process-owned occurrence sink.
pub async fn spawn_with_controller_coverage_and_occurrence_sink(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
    controller_listener: Option<StdUnixListener>,
    controller_coverage: SourceAvailability,
    occurrence_sink: Arc<dyn PersistenceOccurrenceSink>,
) -> Result<CollectorHandle, CollectorError> {
    spawn_configured(
        sock,
        session,
        restored,
        writer,
        controller_listener,
        controller_coverage,
        occurrence_sink,
        empty_operator_seed(),
        None,
    )
    .await
}

/// Launches the fully configured runtime with its restored operator seed.
#[allow(clippy::too_many_arguments)]
pub async fn spawn_with_controller_coverage_occurrence_sink_and_operator_seed(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
    controller_listener: Option<StdUnixListener>,
    controller_coverage: SourceAvailability,
    occurrence_sink: Arc<dyn PersistenceOccurrenceSink>,
    restored_operator: RestoredOperatorState,
    terminal_event_sources: HashMap<RunId, String>,
    non_lane_task_state_runs: HashSet<RunId>,
    log_lane_config: LogLaneConfig,
    operator_commands: mpsc::Receiver<OperatorCommand>,
) -> Result<CollectorHandle, CollectorError> {
    spawn_configured_with_lane_lifecycle(
        sock,
        session,
        restored,
        writer,
        controller_listener,
        controller_coverage,
        occurrence_sink,
        restored_operator,
        terminal_event_sources,
        non_lane_task_state_runs,
        log_lane_config,
        Some(operator_commands),
    )
    .await
}

fn empty_operator_seed() -> RestoredOperatorState {
    RestoredOperatorState {
        activity: Vec::new(),
        terminal_times: HashMap::new(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn spawn_configured(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
    controller_listener: Option<StdUnixListener>,
    controller_coverage: SourceAvailability,
    occurrence_sink: Arc<dyn PersistenceOccurrenceSink>,
    restored_operator: RestoredOperatorState,
    operator_commands: Option<mpsc::Receiver<OperatorCommand>>,
) -> Result<CollectorHandle, CollectorError> {
    spawn_configured_inner(
        sock,
        session,
        restored,
        writer,
        controller_listener,
        controller_coverage,
        occurrence_sink,
        restored_operator,
        operator_commands,
        HashMap::new(),
        HashSet::new(),
        LogLaneConfig::default(),
        Arc::new(SystemPerformanceClock::new()),
        #[cfg(feature = "workload-harness")]
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn spawn_configured_with_lane_lifecycle(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
    controller_listener: Option<StdUnixListener>,
    controller_coverage: SourceAvailability,
    occurrence_sink: Arc<dyn PersistenceOccurrenceSink>,
    restored_operator: RestoredOperatorState,
    terminal_event_sources: HashMap<RunId, String>,
    non_lane_task_state_runs: HashSet<RunId>,
    log_lane_config: LogLaneConfig,
    operator_commands: Option<mpsc::Receiver<OperatorCommand>>,
) -> Result<CollectorHandle, CollectorError> {
    spawn_configured_inner(
        sock,
        session,
        restored,
        writer,
        controller_listener,
        controller_coverage,
        occurrence_sink,
        restored_operator,
        operator_commands,
        terminal_event_sources,
        non_lane_task_state_runs,
        log_lane_config,
        Arc::new(SystemPerformanceClock::new()),
        #[cfg(feature = "workload-harness")]
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn spawn_configured_inner(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    mut writer: WriterClient,
    controller_listener: Option<StdUnixListener>,
    controller_coverage: SourceAvailability,
    occurrence_sink: Arc<dyn PersistenceOccurrenceSink>,
    restored_operator: RestoredOperatorState,
    operator_commands: Option<mpsc::Receiver<OperatorCommand>>,
    terminal_event_sources: HashMap<RunId, String>,
    non_lane_task_state_runs: HashSet<RunId>,
    log_lane_config: LogLaneConfig,
    performance_clock: Arc<dyn PerformanceClock>,
    #[cfg(feature = "workload-harness")] performance_observer: Option<WorkloadPerformanceObserver>,
) -> Result<CollectorHandle, CollectorError> {
    let owner = OwnerTracker::from_environment();
    writer.replace_owner(owner.record()).await?;

    let (mut reducer, model, operator) = Reducer::new_with_operator(restored, restored_operator);
    reducer.restore_terminal_event_sources(terminal_event_sources);
    reducer.restore_non_lane_task_state_runs(non_lane_task_state_runs);
    let (performance_sender, performance) = watch::channel(initial_performance_publication());
    let (quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
    let (source_quality_sender, source_quality) = watch::channel(ObservationQuality::Reconciling);
    let initial_coverage = SourceCoverageRegistry::new(controller_coverage.clone());
    let (coverage_sender, source_coverage) = watch::channel(initial_coverage);
    let (persistence, diagnostics) = RuntimePersistence::new(
        writer,
        &model.borrow(),
        &source_coverage.borrow(),
        occurrence_sink,
    );
    let cancellation = CancellationToken::new();
    let (performance_ingress, performance_sampler) = performance_tracker(performance_clock);
    let (controller_sender, controller_requests) =
        controller_listener.as_ref().map_or((None, None), |_| {
            let (sender, receiver) = controller::request_channel(
                controller::CONTROLLER_REQUEST_QUEUE_CAPACITY,
                reducer.controller_diagnostics_handle(),
                performance_ingress.clone(),
            );
            (Some(sender), Some(receiver))
        });
    let mut controller_acceptor = match (controller_listener, controller_sender) {
        (Some(listener), Some(sender)) => Some(controller::spawn_acceptor_with_diagnostics(
            listener,
            sender,
            cancellation.clone(),
            persistence.diagnostics(),
        )?),
        _ => None,
    };
    let (provider_sender, provider_events) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let provider_diagnostics = crate::provider::ProviderDiagnostics::from_model_handle(
        reducer.provider_diagnostics_handle(),
    );
    let standard_provider_roots = crate::provider::standard_discovery_roots_from_env();
    let provider_thread = match spawn_provider_thread_with_diagnostics_and_performance(
        AdapterProviderWorker::new_with_log_lane_config(
            standard_provider_roots,
            provider_diagnostics.clone(),
            log_lane_config,
        ),
        provider_sender,
        Some(Box::new(RecommendedNotifyFactory)),
        provider_diagnostics,
        performance_ingress.clone(),
    ) {
        Ok(handle) => handle,
        Err(error) => {
            cancellation.cancel();
            if let Some(mut acceptor) = controller_acceptor.take()
                && tokio::time::timeout(STOP_TIMEOUT, &mut acceptor)
                    .await
                    .is_err()
            {
                acceptor.abort();
                let _ = acceptor.await;
            }
            return Err(error.into());
        }
    };
    let provider_publisher = provider_thread.target_publisher();
    let restored_targets = derive_provider_targets(&model.borrow());
    provider_publisher.update_targets(restored_targets.clone());
    let coverage =
        CoverageTracker::new(controller_coverage, coverage_sender, source_quality_sender);
    let (provider_events_drained_sender, provider_events_drained) = oneshot::channel();
    let provider_integration = ProviderIntegration::new_with_drain_acknowledgement(
        provider_events,
        provider_publisher,
        restored_targets,
        coverage,
        provider_events_drained_sender,
    );
    let task_cancellation = cancellation.clone();
    let task_model = model.clone();
    let primary_stream_diagnostics = PrimaryStreamDiagnosticsHandle::default();
    let task_primary_stream_diagnostics = primary_stream_diagnostics.clone();
    let performance_monitor = spawn_performance_monitor(
        performance_sampler,
        model.clone(),
        operator.clone(),
        source_quality,
        performance_sender,
        quality_sender,
        cancellation.clone(),
        #[cfg(feature = "workload-harness")]
        performance_observer,
    );
    let task = tokio::spawn(async move {
        run_collector(
            sock,
            session,
            persistence,
            reducer,
            task_model,
            performance_ingress,
            task_cancellation,
            owner,
            controller_requests,
            operator_commands,
            provider_integration,
            LivenessPolicy::default(),
            task_primary_stream_diagnostics,
        )
        .await
    });

    Ok(CollectorHandle {
        performance,
        quality,
        source_coverage,
        diagnostics,
        operator,
        model,
        primary_stream_diagnostics,
        cancellation,
        task,
        performance_monitor,
        controller_acceptor,
        provider_thread: Some(provider_thread),
        provider_events_drained: Some(provider_events_drained),
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_collector(
    sock: PathBuf,
    session: String,
    mut persistence: RuntimePersistence,
    mut reducer: Reducer,
    shared: SharedModel,
    performance: PerformanceIngress,
    cancellation: CancellationToken,
    mut owner: OwnerTracker,
    mut controller_requests: Option<ControllerRequestReceiver>,
    mut operator_commands: Option<mpsc::Receiver<OperatorCommand>>,
    mut provider: ProviderIntegration,
    liveness_policy: LivenessPolicy,
    primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle,
) -> Result<(), CollectorError> {
    let mut first_subscription = true;
    let mut previous_socket = None;
    let mut reconnect_backoff = ReconnectBackoff::default();
    let subscription_health = HealthEdge::default();
    // Keep this outside the primary retry loop. spawn_enrichment_reader runs once per
    // primary subscription generation, so this one shared value preserves its
    // independent subscription and stream edges across reconnects. The regression
    // test a3_enrichment_subscription_health_persists_across_reader_generations
    // shares one health value across two direct run_enrichment_reader calls: it pins
    // the shared-health semantics, not this wiring. Moving construction into the loop
    // would silently restore one warning per flapping primary generation during a
    // persistent enrichment outage (roughly 10-20 lines per second).
    //
    // Accepted trade-off: a genuine socket replacement during a persistent
    // enrichment outage does not emit a fresh warning. Increment 6 design spec A3
    // requires one warning on degrade, one notice on recovery, and a silent steady
    // failed state. This is intentional; do not reset health on socket replacement.
    let enrichment_health = Arc::new(EnrichmentHealth::default());
    let mut retention_cleanup = tokio::time::interval(STALE_SWEEP_INTERVAL);
    retention_cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    retention_cleanup.tick().await;

    loop {
        if cancellation.is_cancelled() {
            break;
        }

        let socket_identity = socket_identity(&sock);
        let subscriptions = subscriptions();
        let stream = match tokio::select! {
            () = cancellation.cancelled() => break,
            _ = retention_cleanup.tick() => {
                let _ = persistence.cleanup(unix_now_ms()).await?;
                continue;
            }
            request = receive_controller(&mut controller_requests) => {
                service_controller(
                    request,
                    &mut controller_requests,
                    &session,
                    &mut reducer,
                    &mut persistence,
                    &shared,
                    &provider.coverage.registry,
                ).await;
                provider.publish_targets(&shared);
                continue;
            }
            command = receive_operator_command(&mut operator_commands) => {
                if service_operator_command(
                    command,
                    &mut operator_commands,
                    &mut reducer,
                    &mut persistence,
                    &shared,
                    &provider.coverage.registry,
                ).await? {
                    provider.publish_targets(&shared);
                }
                continue;
            }
            result = wire::subscribe(&sock, &subscriptions) => result,
        } {
            Ok(stream) => {
                if subscription_health.record_recovery() {
                    // WARN is required because the production subscriber caps at WARN (src/main.rs).
                    // Keep this before the Reconciling publication below: the shared helper
                    // capture_primary_subscribe_recovery uses it as a happens-after barrier for
                    // a3_primary_subscribe_recovery_logs_one_notice and
                    // a3_recovery_notice_survives_production_warn_level_cap.
                    tracing::warn!(
                        notice_code = "herdr_subscription_recovered",
                        "Herdr event subscription recovered"
                    );
                }
                stream
            }
            Err(error) => {
                if subscription_health.record_failure() {
                    tracing::warn!(
                        warning_code = "herdr_subscription_failed",
                        error = %error,
                        "Herdr event subscription failed; retrying"
                    );
                }
                provider.set_herdr_quality(
                    ObservationQuality::Disconnected,
                    &mut persistence,
                    &shared,
                );
                if wait_or_service_controller(
                    &cancellation,
                    RECONNECT_DELAY,
                    &mut controller_requests,
                    &mut operator_commands,
                    &session,
                    &mut reducer,
                    &mut persistence,
                    &shared,
                    &mut provider,
                )
                .await?
                {
                    break;
                }
                continue;
            }
        };
        let gap_kind = if first_subscription {
            GapKind::Startup
        } else if previous_socket.is_some() && previous_socket != socket_identity {
            GapKind::SocketReplacement
        } else {
            GapKind::Reconnect
        };
        provider.set_herdr_quality(ObservationQuality::Reconciling, &mut persistence, &shared);

        let reader_cancellation = cancellation.child_token();
        let (events, overflowed, reader) = spawn_event_reader(
            stream,
            reader_cancellation.clone(),
            performance.clone(),
            primary_stream_diagnostics.clone(),
        );
        let enrichment_cancellation = cancellation.child_token();
        let (target_publisher, target_receiver) = watch::channel(BTreeSet::new());
        let (enrichment_sender, enrichment_events) = mpsc::channel(ENRICHMENT_QUEUE_CAPACITY);
        let (prune_sender, prune_events) = mpsc::unbounded_channel();
        let enrichment_diagnostics = persistence.enrichment_diagnostics();
        let enrichment_reader = spawn_enrichment_reader(
            sock.clone(),
            target_receiver,
            enrichment_sender,
            prune_sender,
            enrichment_diagnostics.clone(),
            Arc::clone(&enrichment_health),
            enrichment_cancellation.clone(),
        );
        let mut enrichment = EnrichmentConverge {
            target_set: BTreeSet::new(),
            target_publisher,
            published: false,
            events: enrichment_events,
            prunes: prune_events,
            diagnostics: enrichment_diagnostics,
            performance: performance.clone(),
        };
        let outcome = converge(
            &sock,
            &mut persistence,
            &mut reducer,
            &shared,
            &cancellation,
            &mut owner,
            &session,
            gap_kind,
            events,
            Arc::clone(&overflowed),
            &mut enrichment,
            &mut controller_requests,
            &mut operator_commands,
            &mut provider,
            liveness_policy,
            &primary_stream_diagnostics,
        )
        .await;

        reader_cancellation.cancel();
        enrichment_cancellation.cancel();
        let reader_report = reader
            .await
            .map_err(|error| CollectorError::Task(error.to_string()))?;
        enrichment_reader
            .await
            .map_err(|error| CollectorError::Task(error.to_string()))?;
        let outcome = outcome?;
        if outcome.gap_committed {
            first_subscription = false;
            previous_socket = socket_identity;
        }
        if reader_report.received_event {
            reconnect_backoff.on_event();
        }
        match reader_report.reason {
            EventReaderExitReason::WireError(error) => {
                provider.set_herdr_quality(
                    ObservationQuality::Disconnected,
                    &mut persistence,
                    &shared,
                );
                if matches!(outcome.outcome, SubscriptionOutcome::Cancelled) {
                    break;
                }
                let _ = error;
            }
            EventReaderExitReason::Clean => {}
        }
        match outcome.outcome {
            SubscriptionOutcome::Cancelled => break,
            SubscriptionOutcome::WatchdogReconnect(reason) => {
                provider.set_herdr_quality(
                    ObservationQuality::Disconnected,
                    &mut persistence,
                    &shared,
                );
                tracing::warn!(
                    warning_code = "herdr_primary_stream_watchdog_silence",
                    reason = reason.as_str(),
                    "Herdr event subscription failed its silence probe; reconnecting"
                );
                let delay = Duration::from_millis(reconnect_backoff.on_watchdog_silence());
                if wait_or_service_controller(
                    &cancellation,
                    delay,
                    &mut controller_requests,
                    &mut operator_commands,
                    &session,
                    &mut reducer,
                    &mut persistence,
                    &shared,
                    &mut provider,
                )
                .await?
                {
                    break;
                }
            }
            SubscriptionOutcome::Ended => {
                provider.set_herdr_quality(
                    ObservationQuality::Disconnected,
                    &mut persistence,
                    &shared,
                );
            }
        }
    }

    drain_provider_events(
        &mut provider,
        &session,
        &mut reducer,
        &shared,
        &mut persistence,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn converge(
    sock: &Path,
    persistence: &mut RuntimePersistence,
    reducer: &mut Reducer,
    shared: &SharedModel,
    cancellation: &CancellationToken,
    owner: &mut OwnerTracker,
    session: &str,
    gap_kind: GapKind,
    mut events: mpsc::Receiver<Admitted<ReceivedEvent>>,
    overflowed: Arc<AtomicBool>,
    enrichment: &mut EnrichmentConverge,
    controller_requests: &mut Option<ControllerRequestReceiver>,
    operator_commands: &mut Option<mpsc::Receiver<OperatorCommand>>,
    provider: &mut ProviderIntegration,
    liveness_policy: LivenessPolicy,
    primary_stream_diagnostics: &PrimaryStreamDiagnosticsHandle,
) -> Result<ConvergeOutcome, CollectorError> {
    let mut first_generation = true;
    let mut resnapshot_attempts = 0;
    let mut pending_closures = PendingTopologyClosures::default();

    loop {
        enrichment.discard_episode_payloads();
        overflowed.store(false, Ordering::Release);
        let snapshot = tokio::select! {
            () = cancellation.cancelled() => return Ok(ConvergeOutcome::new(SubscriptionOutcome::Cancelled, !first_generation)),
            request = receive_controller(controller_requests) => {
                service_controller(
                    request,
                    controller_requests,
                    session,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await;
                provider.publish_targets(shared);
                continue;
            }
            command = receive_operator_command(operator_commands) => {
                if service_operator_command(
                    command,
                    operator_commands,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await? {
                    provider.publish_targets(shared);
                }
                continue;
            }
            result = wire::request(sock, "session.snapshot", json!({})) => {
                match result.and_then(|value| value.into_snapshot()) {
                    Ok(snapshot) => snapshot,
                    Err(_) => return Ok(ConvergeOutcome::new(SubscriptionOutcome::Ended, !first_generation)),
                }
            }
        };
        let topology = topology_from_snapshot(&snapshot)?;
        let mut batch = if first_generation {
            pending_closures = PendingTopologyClosures::default();
            let mut batch = reducer.reconcile_gap(ReconcileBatch { topology, gap_kind })?;
            batch.push(PersistOp::RecordCollectorGap(CollectorGap {
                event_id: format!("collector-gap-{}", ulid::Ulid::new()),
                herdr_session: session.to_owned(),
                seen_at_ms: unix_now_ms(),
                kind: gap_kind,
            }));
            first_generation = false;
            batch
        } else {
            apply_snapshot_in_place(reducer, shared, topology, session, &mut pending_closures)?
        };
        let _ = persist_submission(persistence, reducer, std::mem::take(&mut batch)).await?;
        enrichment.replace_targets(
            snapshot
                .panes
                .iter()
                .map(|pane| pane.pane_id.clone())
                .collect(),
        );
        provider.publish_targets(shared);
        owner.refresh_from_snapshot(&snapshot, persistence).await?;
        persistence.refresh_snapshot(&shared.borrow(), &provider.coverage.registry);

        let replay = replay_generation(
            reducer,
            shared,
            persistence,
            owner,
            session,
            &snapshot,
            &mut events,
            &overflowed,
            enrichment,
            cancellation,
            &mut pending_closures,
            controller_requests,
            operator_commands,
            provider,
        )
        .await?;
        match replay {
            ReplayOutcome::Cancelled => {
                return Ok(ConvergeOutcome::new(
                    SubscriptionOutcome::Cancelled,
                    !first_generation,
                ));
            }
            ReplayOutcome::Ended => {
                return Ok(ConvergeOutcome::new(
                    SubscriptionOutcome::Ended,
                    !first_generation,
                ));
            }
            ReplayOutcome::Clean => {
                enrichment.discard_episode_payloads();
                enrichment.activate();
                provider.set_herdr_quality(ObservationQuality::Live, persistence, shared);
                match monitor_live(
                    sock,
                    reducer,
                    shared,
                    persistence,
                    owner,
                    session,
                    &mut events,
                    &overflowed,
                    enrichment,
                    cancellation,
                    &mut pending_closures,
                    controller_requests,
                    operator_commands,
                    provider,
                    liveness_policy,
                    primary_stream_diagnostics,
                )
                .await?
                {
                    ReplayOutcome::Dirty => {
                        provider.set_herdr_quality(
                            ObservationQuality::Reconciling,
                            persistence,
                            shared,
                        );
                        resnapshot_attempts = 0;
                    }
                    ReplayOutcome::Ended => {
                        return Ok(ConvergeOutcome::new(
                            SubscriptionOutcome::Ended,
                            !first_generation,
                        ));
                    }
                    ReplayOutcome::Cancelled => {
                        return Ok(ConvergeOutcome::new(
                            SubscriptionOutcome::Cancelled,
                            !first_generation,
                        ));
                    }
                    ReplayOutcome::WatchdogReconnect(reason) => {
                        return Ok(ConvergeOutcome::new(
                            SubscriptionOutcome::WatchdogReconnect(reason),
                            !first_generation,
                        ));
                    }
                    ReplayOutcome::Clean => {}
                }
            }
            ReplayOutcome::Dirty => {
                provider.set_herdr_quality(ObservationQuality::Reconciling, persistence, shared);
                if resnapshot_attempts == RESNAPSHOT_ATTEMPTS {
                    match monitor_reconciling(
                        sock,
                        reducer,
                        shared,
                        persistence,
                        owner,
                        session,
                        &mut events,
                        enrichment,
                        cancellation,
                        &mut pending_closures,
                        controller_requests,
                        operator_commands,
                        provider,
                        liveness_policy,
                        primary_stream_diagnostics,
                    )
                    .await?
                    {
                        ReconcilingOutcome::RestartGeneration => continue,
                        ReconcilingOutcome::Ended => {
                            return Ok(ConvergeOutcome::new(
                                SubscriptionOutcome::Ended,
                                !first_generation,
                            ));
                        }
                        ReconcilingOutcome::Cancelled => {
                            return Ok(ConvergeOutcome::new(
                                SubscriptionOutcome::Cancelled,
                                !first_generation,
                            ));
                        }
                        ReconcilingOutcome::WatchdogReconnect(reason) => {
                            return Ok(ConvergeOutcome::new(
                                SubscriptionOutcome::WatchdogReconnect(reason),
                                !first_generation,
                            ));
                        }
                    }
                }
                resnapshot_attempts += 1;
            }
            ReplayOutcome::WatchdogReconnect(reason) => {
                return Ok(ConvergeOutcome::new(
                    SubscriptionOutcome::WatchdogReconnect(reason),
                    !first_generation,
                ));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn replay_generation(
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
    owner: &mut OwnerTracker,
    session: &str,
    snapshot: &Snapshot,
    events: &mut mpsc::Receiver<Admitted<ReceivedEvent>>,
    overflowed: &AtomicBool,
    enrichment: &mut EnrichmentConverge,
    cancellation: &CancellationToken,
    pending_closures: &mut PendingTopologyClosures,
    controller_requests: &mut Option<ControllerRequestReceiver>,
    operator_commands: &mut Option<mpsc::Receiver<OperatorCommand>>,
    provider: &mut ProviderIntegration,
) -> Result<ReplayOutcome, CollectorError> {
    let snapshot_entities = snapshot_entity_keys(snapshot);
    let mut buffered = VecDeque::new();
    let mut created = HashSet::new();
    let mut closures: HashMap<EntityKey, Vec<usize>> = HashMap::new();
    let mut candidates = Vec::new();
    let mut next = 0;
    let mut channel_state = drain_events(events, &mut buffered);

    loop {
        enrichment.discard_episode_payloads();
        while let Some(admitted) = buffered.pop_front() {
            let (received, admission) = admitted.into_parts();
            let target_delta = enrichment_target_delta(&received, shared);
            record_replay_facts(
                next,
                &received,
                &mut created,
                &mut closures,
                &mut candidates,
            );
            apply_received_event(
                reducer,
                shared,
                persistence,
                owner,
                session,
                received,
                admission,
                pending_closures,
                provider,
            )
            .await?;
            enrichment.apply_target_delta(target_delta);
            next += 1;
            channel_state = drain_events(events, &mut buffered);
        }
        if channel_state == EventChannelState::Closed {
            return Ok(ReplayOutcome::Ended);
        }

        let next_received = tokio::select! {
            () = cancellation.cancelled() => return Ok(ReplayOutcome::Cancelled),
            request = receive_controller(controller_requests) => {
                service_controller(
                    request,
                    controller_requests,
                    session,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await;
                provider.publish_targets(shared);
                continue;
            }
            command = receive_operator_command(operator_commands) => {
                if service_operator_command(
                    command,
                    operator_commands,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await? {
                    provider.publish_targets(shared);
                }
                continue;
            }
            result = tokio::time::timeout(DRAIN_QUIET_PERIOD, events.recv()) => result,
        };
        match next_received {
            Ok(Some(received)) => buffered.push_back(received),
            Ok(None) => {
                channel_state = drain_events(events, &mut buffered);
                debug_assert_eq!(channel_state, EventChannelState::Closed);
            }
            Err(_) => break,
        }
    }

    let anomalous = candidates.into_iter().any(|(index, entity)| {
        !snapshot_entities.contains(&entity)
            && !created.contains(&entity)
            && !closures
                .get(&entity)
                .is_some_and(|positions| positions.iter().any(|position| *position > index))
    });
    if anomalous || overflowed.load(Ordering::Acquire) {
        Ok(ReplayOutcome::Dirty)
    } else if cancellation.is_cancelled() {
        Ok(ReplayOutcome::Cancelled)
    } else {
        Ok(ReplayOutcome::Clean)
    }
}

enum WatchdogProbeOutcome {
    HealthyIdle,
    Inconclusive,
    Reconnect(WatchdogReconnectReason),
}

type WatchdogProbeFuture<'a> = Pin<Box<dyn Future<Output = WatchdogProbeOutcome> + Send + 'a>>;

async fn probe_primary_topology(
    sock: &Path,
    shared: &SharedModel,
    pending_closures: PendingTopologyClosures,
    liveness_policy: LivenessPolicy,
) -> WatchdogProbeOutcome {
    let result = tokio::time::timeout(
        liveness_timeout(&liveness_policy),
        wire::request(sock, "session.snapshot", json!({})),
    )
    .await;
    let snapshot = match result {
        Ok(Ok(value)) => match value.into_snapshot() {
            Ok(snapshot) => snapshot,
            Err(_) => {
                return WatchdogProbeOutcome::Reconnect(WatchdogReconnectReason::ProbeFailed);
            }
        },
        Ok(Err(_)) | Err(_) => {
            return WatchdogProbeOutcome::Reconnect(WatchdogReconnectReason::ProbeFailed);
        }
    };
    let Ok(probed) = topology_from_snapshot(&snapshot) else {
        return WatchdogProbeOutcome::Reconnect(WatchdogReconnectReason::ProbeFailed);
    };
    let Some(current) = current_model_topology(shared, &pending_closures) else {
        return WatchdogProbeOutcome::Inconclusive;
    };
    if probe_topology_matches_model(probed, current) {
        WatchdogProbeOutcome::HealthyIdle
    } else {
        WatchdogProbeOutcome::Reconnect(WatchdogReconnectReason::TopologyDiverged)
    }
}

fn probe_topology_matches_model(probed: TopologySnapshot, current: TopologySnapshot) -> bool {
    let mut probed = canonical_topology(probed);
    let current = canonical_topology(current);
    for (probed, current) in probed.tabs.iter_mut().zip(&current.tabs) {
        if probed.tab_id == current.tab_id && probed.label.is_none() {
            probed.label.clone_from(&current.label);
        }
    }
    for (probed, current) in probed.panes.iter_mut().zip(&current.panes) {
        if probed.pane_id == current.pane_id && probed.display_name.is_none() {
            probed.display_name.clone_from(&current.display_name);
        }
    }
    probed == current
}

fn canonical_topology(mut topology: TopologySnapshot) -> TopologySnapshot {
    for pane in &mut topology.panes {
        let Some(agent) = &mut pane.agent else {
            pane.agent_session = None;
            continue;
        };
        // Reconciliation retains the resolved provider and non-empty native session identity,
        // not Herdr's raw agent, session-source, or session-agent spellings.
        let provider = snapshot_provider_name(
            &agent.agent_name,
            pane.agent_session
                .as_ref()
                .map(|session| session.agent.as_str()),
        );
        agent.agent_name = provider.map(provider_name).unwrap_or("unknown").to_owned();
        pane.agent_session = match (provider, pane.agent_session.take()) {
            (Some(_), Some(mut session)) if !session.value.is_empty() => {
                session.source.clear();
                session.agent.clear();
                Some(session)
            }
            _ => None,
        };
    }
    topology
        .workspaces
        .sort_by(|left, right| left.workspace_id.cmp(&right.workspace_id));
    topology
        .tabs
        .sort_by(|left, right| left.tab_id.cmp(&right.tab_id));
    topology
        .panes
        .sort_by(|left, right| left.pane_id.cmp(&right.pane_id));
    topology
}

fn current_model_topology(
    shared: &SharedModel,
    pending_closures: &PendingTopologyClosures,
) -> Option<TopologySnapshot> {
    let model = shared.borrow();
    let workspaces = model
        .workspaces()
        .filter(|workspace| {
            !pending_closures
                .workspaces
                .contains(&workspace.workspace_id)
        })
        .cloned()
        .collect();
    let tabs = model
        .tabs()
        .filter(|tab| !pending_closures.tabs.contains(&tab.tab_id))
        .cloned()
        .collect();
    let panes = model
        .panes()
        .filter(|pane| !pending_closures.panes.contains(&pane.pane_id))
        .map(|pane| {
            let mut executions = model.executions().filter(|execution| {
                execution.pane_id == pane.pane_id
                    && execution.terminal_id == pane.terminal_id
                    && !execution.state.is_terminal()
                    && !matches!(execution.state, ExecState::Stale { .. })
            });
            let execution = executions.next();
            if executions.next().is_some() {
                return None;
            }
            let (agent, agent_session) = execution.map_or((None, None), |execution| {
                let (agent_name, agent_session) = current_execution_identity(&model, execution);
                (
                    Some(SnapshotAgent {
                        agent_name,
                        state: execution.state.clone(),
                    }),
                    agent_session,
                )
            });
            Some(PaneSnapshot {
                pane_id: pane.pane_id.clone(),
                workspace_id: pane.workspace_id.clone(),
                tab_id: pane.tab_id.clone(),
                terminal_id: pane.terminal_id.clone(),
                display_name: pane.display_name.clone(),
                agent,
                agent_session,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(TopologySnapshot {
        workspaces,
        tabs,
        panes,
    })
}

fn current_execution_identity(
    model: &DomainModel,
    execution: &Execution,
) -> (String, Option<AgentSessionReference>) {
    let identity = model
        .task_run(&execution.task_run_id)
        .and_then(|run| match &run.key {
            RunKey::Native { provider, sid } => Some((
                *provider,
                Some((AgentSessionReferenceKind::Id, sid.clone())),
            )),
            RunKey::NativePath { provider, path } => Some((
                *provider,
                Some((AgentSessionReferenceKind::Path, path.clone())),
            )),
            RunKey::Controller(_) | RunKey::Provisional { .. } => None,
        })
        .or_else(|| {
            model
                .task_run_bindings()
                .filter_map(|(key, owner)| {
                    if *owner != execution.task_run_id {
                        return None;
                    }
                    match key {
                        RunKey::Native { provider, sid } => Some((
                            0_u8,
                            provider_name(*provider),
                            sid.clone(),
                            *provider,
                            AgentSessionReferenceKind::Id,
                        )),
                        RunKey::NativePath { provider, path } => Some((
                            1_u8,
                            provider_name(*provider),
                            path.clone(),
                            *provider,
                            AgentSessionReferenceKind::Path,
                        )),
                        RunKey::Controller(_) | RunKey::Provisional { .. } => None,
                    }
                })
                .min_by(|left, right| {
                    (&left.0, left.1, &left.2).cmp(&(&right.0, right.1, &right.2))
                })
                .map(|(_, _, value, provider, kind)| (provider, Some((kind, value))))
        })
        .or_else(|| {
            model
                .agent_nodes()
                .filter(|node| node.task_run_id == execution.task_run_id)
                .min_by(|left, right| {
                    left.parent_agent_node_id
                        .is_some()
                        .cmp(&right.parent_agent_node_id.is_some())
                        .then_with(|| left.agent_node_id.cmp(&right.agent_node_id))
                })
                .map(|node| {
                    let session = node
                        .native_session_id
                        .as_ref()
                        .map(|sid| (AgentSessionReferenceKind::Id, sid.clone()))
                        .or_else(|| {
                            node.session_file
                                .as_ref()
                                .map(|path| (AgentSessionReferenceKind::Path, path.clone()))
                        });
                    (node.provider, session)
                })
        });
    let Some((provider, session)) = identity else {
        return ("unknown".to_owned(), None);
    };
    let name = provider_name(provider);
    (
        name.to_owned(),
        session.map(|(kind, value)| AgentSessionReference {
            source: format!("herdr:{name}"),
            agent: name.to_owned(),
            kind,
            value,
        }),
    )
}

#[allow(clippy::too_many_arguments)]
async fn monitor_live(
    sock: &Path,
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
    owner: &mut OwnerTracker,
    session: &str,
    events: &mut mpsc::Receiver<Admitted<ReceivedEvent>>,
    overflowed: &AtomicBool,
    enrichment: &mut EnrichmentConverge,
    cancellation: &CancellationToken,
    pending_closures: &mut PendingTopologyClosures,
    controller_requests: &mut Option<ControllerRequestReceiver>,
    operator_commands: &mut Option<mpsc::Receiver<OperatorCommand>>,
    provider: &mut ProviderIntegration,
    liveness_policy: LivenessPolicy,
    primary_stream_diagnostics: &PrimaryStreamDiagnosticsHandle,
) -> Result<ReplayOutcome, CollectorError> {
    enum LiveReceipt {
        Primary(Admitted<ReceivedEvent>),
        Enrichment(EnrichmentPayload),
        Probe(WatchdogProbeOutcome),
        Sweep,
    }

    let mut stale_sweep = tokio::time::interval(STALE_SWEEP_INTERVAL);
    stale_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    stale_sweep.tick().await;
    let mut watchdog_deadline = silence_deadline(Instant::now(), &liveness_policy);
    let mut watchdog_probe: Option<WatchdogProbeFuture<'_>> = None;
    let mut enrichment_events_open = true;
    let mut enrichment_prunes_open = true;
    loop {
        let received = tokio::select! {
            () = cancellation.cancelled() => return Ok(ReplayOutcome::Cancelled),
            request = receive_controller(controller_requests) => {
                service_controller(
                    request,
                    controller_requests,
                    session,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await;
                provider.publish_targets(shared);
                continue;
            }
            command = receive_operator_command(operator_commands) => {
                if service_operator_command(
                    command,
                    operator_commands,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await? {
                    provider.publish_targets(shared);
                }
                continue;
            }
            event = receive_provider(&mut provider.events) => {
                service_provider_event(
                    event,
                    provider,
                    session,
                    reducer,
                    shared,
                    persistence,
                ).await?;
                provider.publish_targets(shared);
                continue;
            }
            received = events.recv() => match received {
                Some(received) => LiveReceipt::Primary(received),
                None => return Ok(ReplayOutcome::Ended),
            },
            payload = enrichment.events.recv(), if enrichment_events_open => match payload {
                Some(payload) => LiveReceipt::Enrichment(payload),
                None => {
                    enrichment_events_open = false;
                    continue;
                }
            },
            prune = enrichment.prunes.recv(), if enrichment_prunes_open => {
                let Some(prune) = prune else {
                    enrichment_prunes_open = false;
                    continue;
                };
                enrichment.apply_prune(prune);
                continue;
            },
            probe = async {
                match watchdog_probe.as_mut() {
                    Some(probe) => probe.await,
                    None => pending().await,
                }
            } => LiveReceipt::Probe(probe),
            () = tokio::time::sleep_until(watchdog_deadline), if watchdog_probe.is_none() => {
                watchdog_probe = Some(Box::pin(probe_primary_topology(
                    sock,
                    shared,
                    pending_closures.clone(),
                    liveness_policy,
                )));
                continue;
            },
            _ = stale_sweep.tick() => LiveReceipt::Sweep,
        };
        match received {
            LiveReceipt::Probe(WatchdogProbeOutcome::HealthyIdle) => {
                watchdog_probe = None;
                tracing::debug!("silent Herdr event subscription passed its topology probe");
                watchdog_deadline = silence_deadline(Instant::now(), &liveness_policy);
                continue;
            }
            LiveReceipt::Probe(WatchdogProbeOutcome::Inconclusive) => {
                watchdog_probe = None;
                primary_stream_diagnostics.record_inconclusive_topology_probe();
                tracing::debug!("silent Herdr event subscription topology probe was inconclusive");
                watchdog_deadline = silence_deadline(Instant::now(), &liveness_policy);
                continue;
            }
            LiveReceipt::Probe(WatchdogProbeOutcome::Reconnect(reason)) => {
                return Ok(ReplayOutcome::WatchdogReconnect(reason));
            }
            LiveReceipt::Sweep => {
                let mut persist = reducer.sweep_stale(unix_now_ms());
                persist.extend(apply_pending_topology_closures(
                    reducer,
                    shared,
                    session,
                    pending_closures,
                )?);
                if !persist.is_empty() {
                    let _ = persist_submission(persistence, reducer, persist).await?;
                    provider.publish_targets(shared);
                }
                let _ = persistence.cleanup(unix_now_ms()).await?;
                persistence.refresh_snapshot(&shared.borrow(), &provider.coverage.registry);
                continue;
            }
            LiveReceipt::Enrichment(payload) => {
                apply_enrichment_payload(
                    reducer,
                    shared,
                    persistence,
                    session,
                    payload,
                    &enrichment.target_set,
                    &enrichment.performance,
                    pending_closures,
                    provider,
                )
                .await?;
                continue;
            }
            LiveReceipt::Primary(received) => {
                watchdog_probe = None;
                watchdog_deadline = silence_deadline(Instant::now(), &liveness_policy);
                let (received, admission) = received.into_parts();
                let target_delta = enrichment_target_delta(&received, shared);
                let anomalous =
                    updated_entity(&received).is_some_and(|entity| !entity_exists(shared, &entity));
                apply_received_event(
                    reducer,
                    shared,
                    persistence,
                    owner,
                    session,
                    received,
                    admission,
                    pending_closures,
                    provider,
                )
                .await?;
                enrichment.apply_target_delta(target_delta);
                if anomalous || overflowed.swap(false, Ordering::AcqRel) {
                    return Ok(ReplayOutcome::Dirty);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn monitor_reconciling(
    sock: &Path,
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
    owner: &mut OwnerTracker,
    session: &str,
    events: &mut mpsc::Receiver<Admitted<ReceivedEvent>>,
    enrichment: &mut EnrichmentConverge,
    cancellation: &CancellationToken,
    pending_closures: &mut PendingTopologyClosures,
    controller_requests: &mut Option<ControllerRequestReceiver>,
    operator_commands: &mut Option<mpsc::Receiver<OperatorCommand>>,
    provider: &mut ProviderIntegration,
    liveness_policy: LivenessPolicy,
    primary_stream_diagnostics: &PrimaryStreamDiagnosticsHandle,
) -> Result<ReconcilingOutcome, CollectorError> {
    enum ReconcilingReceipt {
        Primary(Admitted<ReceivedEvent>),
        Probe(WatchdogProbeOutcome),
        Sweep,
    }

    let mut stale_sweep = tokio::time::interval(STALE_SWEEP_INTERVAL);
    stale_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    stale_sweep.tick().await;
    let mut watchdog_deadline = silence_deadline(Instant::now(), &liveness_policy);
    let mut watchdog_probe: Option<WatchdogProbeFuture<'_>> = None;
    loop {
        enrichment.discard_episode_payloads();
        let received = tokio::select! {
            () = cancellation.cancelled() => return Ok(ReconcilingOutcome::Cancelled),
            request = receive_controller(controller_requests) => {
                service_controller(
                    request,
                    controller_requests,
                    session,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await;
                provider.publish_targets(shared);
                continue;
            }
            command = receive_operator_command(operator_commands) => {
                if service_operator_command(
                    command,
                    operator_commands,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await? {
                    provider.publish_targets(shared);
                }
                continue;
            }
            event = receive_provider(&mut provider.events) => {
                service_provider_event(
                    event,
                    provider,
                    session,
                    reducer,
                    shared,
                    persistence,
                ).await?;
                provider.publish_targets(shared);
                continue;
            }
            received = events.recv() => match received {
                Some(received) => ReconcilingReceipt::Primary(received),
                None => return Ok(ReconcilingOutcome::Ended),
            },
            probe = async {
                match watchdog_probe.as_mut() {
                    Some(probe) => probe.await,
                    None => pending().await,
                }
            } => ReconcilingReceipt::Probe(probe),
            () = tokio::time::sleep_until(watchdog_deadline), if watchdog_probe.is_none() => {
                watchdog_probe = Some(Box::pin(probe_primary_topology(
                    sock,
                    shared,
                    pending_closures.clone(),
                    liveness_policy,
                )));
                continue;
            },
            _ = stale_sweep.tick() => ReconcilingReceipt::Sweep,
        };
        let received = match received {
            ReconcilingReceipt::Probe(WatchdogProbeOutcome::HealthyIdle) => {
                tracing::debug!("silent reconciling Herdr subscription passed its topology probe");
                return Ok(ReconcilingOutcome::RestartGeneration);
            }
            ReconcilingReceipt::Probe(WatchdogProbeOutcome::Inconclusive) => {
                watchdog_probe = None;
                primary_stream_diagnostics.record_inconclusive_topology_probe();
                tracing::debug!(
                    "silent reconciling Herdr subscription topology probe was inconclusive"
                );
                watchdog_deadline = silence_deadline(Instant::now(), &liveness_policy);
                continue;
            }
            ReconcilingReceipt::Probe(WatchdogProbeOutcome::Reconnect(reason)) => {
                return Ok(ReconcilingOutcome::WatchdogReconnect(reason));
            }
            ReconcilingReceipt::Sweep => {
                let mut persist = reducer.sweep_stale(unix_now_ms());
                persist.extend(apply_pending_topology_closures(
                    reducer,
                    shared,
                    session,
                    pending_closures,
                )?);
                if !persist.is_empty() {
                    let _ = persist_submission(persistence, reducer, persist).await?;
                    provider.publish_targets(shared);
                }
                let _ = persistence.cleanup(unix_now_ms()).await?;
                persistence.refresh_snapshot(&shared.borrow(), &provider.coverage.registry);
                continue;
            }
            ReconcilingReceipt::Primary(received) => received,
        };
        watchdog_probe = None;
        watchdog_deadline = silence_deadline(Instant::now(), &liveness_policy);
        let (received, admission) = received.into_parts();
        let target_delta = enrichment_target_delta(&received, shared);
        apply_received_event(
            reducer,
            shared,
            persistence,
            owner,
            session,
            received,
            admission,
            pending_closures,
            provider,
        )
        .await?;
        enrichment.apply_target_delta(target_delta);
    }
}

type EventReader = (
    mpsc::Receiver<Admitted<ReceivedEvent>>,
    Arc<AtomicBool>,
    JoinHandle<EventReaderReport>,
);

#[derive(Debug)]
enum EventReaderExitReason {
    Clean,
    WireError(WireError),
}

#[derive(Debug)]
struct EventReaderReport {
    reason: EventReaderExitReason,
    received_event: bool,
}

impl EventReaderReport {
    const fn new(reason: EventReaderExitReason, received_event: bool) -> Self {
        Self {
            reason,
            received_event,
        }
    }
}

#[derive(Clone, Debug)]
struct EnrichmentPayload {
    pane_id: String,
    terminal_id: Option<String>,
    state: ExecState,
    timestamp_ms: i64,
    receipt_time_ms: i64,
}

struct EnrichmentPrune {
    pane_id: String,
    acknowledgement: tokio::sync::oneshot::Sender<()>,
}

struct EnrichmentConverge {
    target_set: BTreeSet<String>,
    target_publisher: watch::Sender<BTreeSet<String>>,
    published: bool,
    events: mpsc::Receiver<EnrichmentPayload>,
    prunes: mpsc::UnboundedReceiver<EnrichmentPrune>,
    diagnostics: EnrichmentDiagnosticsHandle,
    performance: PerformanceIngress,
}

impl EnrichmentConverge {
    fn replace_targets(&mut self, targets: BTreeSet<String>) {
        if self.target_set == targets {
            return;
        }
        self.target_set = targets;
        self.publish_targets();
    }

    fn activate(&mut self) {
        if self.published {
            return;
        }
        self.published = true;
        self.publish_targets();
    }

    fn publish_targets(&self) {
        if !self.published {
            return;
        }
        let targets = self.target_set.clone();
        self.target_publisher.send_if_modified(|current| {
            if *current == targets {
                false
            } else {
                *current = targets;
                true
            }
        });
    }

    fn apply_target_delta(&mut self, delta: EnrichmentTargetDelta) {
        let mut targets = self.target_set.clone();
        for pane_id in delta.removed {
            targets.remove(&pane_id);
        }
        targets.extend(delta.created);
        self.replace_targets(targets);
    }

    fn drain_prunes(&mut self) {
        while let Ok(prune) = self.prunes.try_recv() {
            self.apply_prune(prune);
        }
    }

    fn apply_prune(&mut self, prune: EnrichmentPrune) {
        let mut targets = self.target_set.clone();
        targets.remove(&prune.pane_id);
        self.replace_targets(targets);
        let _ = prune.acknowledgement.send(());
    }

    fn discard_episode_payloads(&mut self) {
        self.drain_prunes();
        for _ in 0..ENRICHMENT_QUEUE_CAPACITY {
            match self.events.try_recv() {
                Ok(_) => self.diagnostics.record_episode_discard(),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
    }
}

#[derive(Default)]
struct EnrichmentTargetDelta {
    created: BTreeSet<String>,
    removed: BTreeSet<String>,
}

fn enrichment_target_delta(
    received: &ReceivedEvent,
    shared: &SharedModel,
) -> EnrichmentTargetDelta {
    let mut delta = EnrichmentTargetDelta::default();
    match received.event.as_str() {
        "pane_created" => {
            if let Some(pane_id) = nested_string(&received.data, "pane", "pane_id") {
                delta.created.insert(pane_id);
            }
        }
        "pane_closed" => {
            if let Some(pane_id) = string_field(&received.data, "pane_id") {
                delta.removed.insert(pane_id);
            }
        }
        "pane_moved" => {
            if let Some(pane_id) = nested_string(&received.data, "pane", "pane_id") {
                delta.created.insert(pane_id);
            }
            if let Some(pane_id) = string_field(&received.data, "previous_pane_id") {
                delta.removed.insert(pane_id);
            }
        }
        "tab_closed" => {
            if let Some(tab_id) = string_field(&received.data, "tab_id") {
                delta.removed.extend(
                    shared
                        .borrow()
                        .panes()
                        .filter(|pane| pane.tab_id == tab_id)
                        .map(|pane| pane.pane_id.clone()),
                );
            }
        }
        "workspace_closed" => {
            if let Some(workspace_id) = string_field(&received.data, "workspace_id") {
                delta.removed.extend(
                    shared
                        .borrow()
                        .panes()
                        .filter(|pane| pane.workspace_id == workspace_id)
                        .map(|pane| pane.pane_id.clone()),
                );
            }
        }
        _ => {}
    }
    delta
}

fn spawn_enrichment_reader(
    sock: PathBuf,
    targets: watch::Receiver<BTreeSet<String>>,
    sender: mpsc::Sender<EnrichmentPayload>,
    prunes: mpsc::UnboundedSender<EnrichmentPrune>,
    diagnostics: EnrichmentDiagnosticsHandle,
    health: Arc<EnrichmentHealth>,
    cancellation: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(run_enrichment_reader(
        sock,
        targets,
        sender,
        prunes,
        diagnostics,
        health,
        cancellation,
    ))
}

struct EnrichmentDeferral {
    first: Option<tokio::time::Instant>,
}

impl EnrichmentDeferral {
    fn on_change(&mut self, now: Instant) -> Instant {
        let first = *self.first.get_or_insert(now);
        (now + RECONNECT_DELAY).min(first + ENRICHMENT_RECONNECT_MAX_DEFERRAL)
    }

    fn on_swap(&mut self) {
        self.first = None;
    }

    fn on_empty(&mut self) {
        self.first = None;
    }
}

async fn run_enrichment_reader(
    sock: PathBuf,
    mut targets: watch::Receiver<BTreeSet<String>>,
    sender: mpsc::Sender<EnrichmentPayload>,
    prunes: mpsc::UnboundedSender<EnrichmentPrune>,
    diagnostics: EnrichmentDiagnosticsHandle,
    health: Arc<EnrichmentHealth>,
    cancellation: CancellationToken,
) {
    let mut target_set = targets.borrow_and_update().clone();
    let mut deferral = EnrichmentDeferral { first: None };
    loop {
        while target_set.is_empty() {
            deferral.on_empty();
            tokio::select! {
                () = cancellation.cancelled() => return,
                changed = targets.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    target_set = targets.borrow_and_update().clone();
                    if !target_set.is_empty() {
                        let _ = deferral.on_change(Instant::now());
                    }
                }
            }
        }

        let delayed = loop {
            let deadline = deferral.on_change(Instant::now());
            tokio::select! {
                () = cancellation.cancelled() => return,
                changed = targets.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    target_set = targets.borrow_and_update().clone();
                    if target_set.is_empty() {
                        break false;
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    target_set = targets.borrow_and_update().clone();
                    break !target_set.is_empty();
                }
            }
        };
        if !delayed {
            continue;
        }
        deferral.on_swap();

        let subscriptions = enrichment_subscriptions(&target_set);
        let subscribed = tokio::select! {
            () = cancellation.cancelled() => return,
            result = wire::subscribe(&sock, &subscriptions) => result,
        };
        let mut stream = match subscribed {
            Ok(stream) => {
                if health.subscription.record_recovery() {
                    // WARN is required because the production subscriber caps at WARN (src/main.rs).
                    tracing::warn!(
                        notice_code = "herdr_enrichment_subscription_recovered",
                        "Herdr enrichment subscription recovered"
                    );
                }
                stream
            }
            Err(WireError::Server { code, message }) if code == "pane_not_found" => {
                let Some(pane_id) = rejected_enrichment_pane(&message, &target_set) else {
                    if health.subscription.record_failure() {
                        tracing::warn!(
                            warning_code = "herdr_enrichment_subscription_failed",
                            error = %WireError::Server { code, message },
                            "Herdr enrichment subscription failed; retrying"
                        );
                    }
                    continue;
                };
                let (acknowledgement, applied) = tokio::sync::oneshot::channel();
                if prunes
                    .send(EnrichmentPrune {
                        pane_id: pane_id.clone(),
                        acknowledgement,
                    })
                    .is_err()
                {
                    return;
                }
                tokio::select! {
                    () = cancellation.cancelled() => return,
                    result = applied => {
                        if result.is_err() {
                            return;
                        }
                    }
                }
                target_set.remove(&pane_id);
                target_set = targets.borrow_and_update().clone();
                continue;
            }
            Err(error) => {
                if health.subscription.record_failure() {
                    tracing::warn!(
                        warning_code = "herdr_enrichment_subscription_failed",
                        error = %error,
                        "Herdr enrichment subscription failed; retrying"
                    );
                }
                continue;
            }
        };

        loop {
            let received = tokio::select! {
                () = cancellation.cancelled() => {
                    let _ = stream.close().await;
                    return;
                }
                changed = targets.changed() => {
                    if changed.is_err() {
                        let _ = stream.close().await;
                        return;
                    }
                    target_set = targets.borrow_and_update().clone();
                    let _ = deferral.on_change(Instant::now());
                    let _ = stream.close().await;
                    break;
                }
                received = stream.next_event() => received,
            };
            let (event, data) = match received {
                Ok(Some(received)) => {
                    if health.stream.record_recovery() {
                        // WARN is required because the production subscriber caps at WARN (src/main.rs).
                        tracing::warn!(
                            notice_code = "herdr_enrichment_stream_recovered",
                            "Herdr enrichment stream recovered"
                        );
                    }
                    received
                }
                Ok(None) => {
                    if health.stream.record_failure() {
                        // WARN is required because the production subscriber caps at WARN (src/main.rs).
                        tracing::warn!(
                            warning_code = "herdr_enrichment_stream_failed",
                            "Herdr enrichment stream closed; retrying"
                        );
                    }
                    break;
                }
                Err(error) => {
                    if health.stream.record_failure() {
                        tracing::warn!(
                            warning_code = "herdr_enrichment_stream_failed",
                            error = %error,
                            "Herdr enrichment stream ended; retrying"
                        );
                    }
                    break;
                }
            };
            let Some(payload) = enrichment_payload(&event, &data) else {
                continue;
            };
            match sender.try_send(payload) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    diagnostics.record_channel_full_drop();
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return,
            }
        }
    }
}

fn rejected_enrichment_pane(message: &str, targets: &BTreeSet<String>) -> Option<String> {
    message
        .strip_prefix("pane ")
        .and_then(|message| message.strip_suffix(" not found"))
        .filter(|pane_id| targets.contains(*pane_id))
        .map(str::to_owned)
}

fn enrichment_payload(event: &str, data: &Value) -> Option<EnrichmentPayload> {
    if event != "pane_agent_status_changed" {
        return None;
    }
    let pane_id =
        string_field(data, "pane_id").or_else(|| nested_string(data, "pane", "pane_id"))?;
    let terminal_id =
        string_field(data, "terminal_id").or_else(|| nested_string(data, "pane", "terminal_id"));
    let state = status_from_value(
        data.get("agent_status")
            .or_else(|| data.get("status"))
            .or_else(|| data.get("new_status")),
    );
    let receipt_time_ms = unix_now_ms();
    Some(EnrichmentPayload {
        pane_id,
        terminal_id,
        state,
        timestamp_ms: receipt_time_ms,
        receipt_time_ms,
    })
}

fn spawn_event_reader(
    mut stream: EventStream,
    cancellation: CancellationToken,
    performance: PerformanceIngress,
    diagnostics: PrimaryStreamDiagnosticsHandle,
) -> EventReader {
    let (sender, receiver) = admitted_channel(EVENT_QUEUE_CAPACITY, performance);
    let overflowed = Arc::new(AtomicBool::new(false));
    let reader_overflowed = Arc::clone(&overflowed);
    let task = tokio::spawn(async move {
        let mut received_event = false;
        loop {
            let received = tokio::select! {
                () = cancellation.cancelled() => {
                    return EventReaderReport::new(EventReaderExitReason::Clean, received_event);
                }
                received = stream.next_event() => match received {
                    Ok(received) => received,
                    Err(error) => {
                        return EventReaderReport::new(
                            EventReaderExitReason::WireError(error),
                            received_event,
                        );
                    }
                },
            };
            let Some((event, data)) = received else {
                return EventReaderReport::new(EventReaderExitReason::Clean, received_event);
            };
            received_event = true;
            let received = ReceivedEvent {
                event,
                data,
                primary_stream_diagnostics: diagnostics.clone(),
            };
            match sender.try_send(received) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(received)) => {
                    reader_overflowed.store(true, Ordering::Release);
                    tokio::select! {
                        () = cancellation.cancelled() => {
                            return EventReaderReport::new(
                                EventReaderExitReason::Clean,
                                received_event,
                            );
                        }
                        result = sender.send(received) => {
                            if result.is_err() {
                                return EventReaderReport::new(
                                    EventReaderExitReason::Clean,
                                    received_event,
                                );
                            }
                        }
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return EventReaderReport::new(EventReaderExitReason::Clean, received_event);
                }
            }
        }
    });
    (receiver, overflowed, task)
}

struct ReceivedEvent {
    event: String,
    data: Value,
    primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle,
}

#[derive(Debug)]
struct AdapterRootState {
    discovery: DiscoveryIndex,
}

impl AdapterRootState {
    fn new(provider: Provider, root: PathBuf) -> io::Result<Self> {
        Ok(Self {
            discovery: DiscoveryIndex::new(vec![DiscoveryRoot {
                provider,
                path: root,
            }])?,
        })
    }
}

type RootStateFactory = Box<dyn FnMut(Provider, PathBuf) -> io::Result<AdapterRootState> + Send>;
#[cfg(test)]
type AfterTailChunkHook = Box<dyn FnMut(&Path, u64) + Send>;

#[derive(Debug, Default)]
struct AdapterBootstrapParser {
    codex: crate::provider::codex::CodexBootstrapParser,
    claude: crate::provider::claude::ClaudeBootstrapParser,
}

impl BootstrapParser for AdapterBootstrapParser {
    fn parse_structural(
        &mut self,
        provider: Provider,
        relative_path: &Path,
        record: &[u8],
    ) -> Option<BootstrapIdentity> {
        match provider {
            Provider::Codex => self.codex.parse_structural(provider, relative_path, record),
            Provider::Claude => self
                .claude
                .parse_structural(provider, relative_path, record),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResumeCursor {
    provider: Provider,
    root: PathBuf,
    path_id: u32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum AvailabilitySweepMember {
    Root(PathBuf),
    File(u32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AvailabilitySweepFailure {
    RootNotFound,
    RootPermissionDenied,
    FileIoError,
}

impl AvailabilitySweepFailure {
    const fn detail(self) -> &'static str {
        match self {
            Self::RootNotFound => "root_not_found",
            Self::RootPermissionDenied => "root_permission_denied",
            Self::FileIoError => "file_io_error",
        }
    }
}

#[derive(Debug, Default)]
struct ProviderAvailabilitySweep {
    targeted: Option<bool>,
    visited: HashSet<AvailabilitySweepMember>,
    failure: Option<AvailabilitySweepFailure>,
}

#[derive(Clone, Debug)]
struct AdapterWorkItem {
    provider: Provider,
    root: PathBuf,
    file: crate::provider::DiscoveredFile,
}

fn compare_adapter_key(
    left_provider: Provider,
    left_root: &PathBuf,
    left_path_id: u32,
    right_provider: Provider,
    right_root: &PathBuf,
    right_path_id: u32,
) -> std::cmp::Ordering {
    integration_provider_rank(left_provider)
        .cmp(&integration_provider_rank(right_provider))
        .then_with(|| left_root.cmp(right_root))
        .then_with(|| left_path_id.cmp(&right_path_id))
}

fn cursor_for_work(work: &AdapterWorkItem) -> ResumeCursor {
    ResumeCursor {
        provider: work.provider,
        root: work.root.clone(),
        path_id: work.file.path_id,
    }
}

fn resume_start(work: &[AdapterWorkItem], cursor: Option<&ResumeCursor>) -> usize {
    let Some(cursor) = cursor else {
        return 0;
    };
    if let Some(index) = work.iter().position(|item| {
        compare_adapter_key(
            item.provider,
            &item.root,
            item.file.path_id,
            cursor.provider,
            &cursor.root,
            cursor.path_id,
        ) == std::cmp::Ordering::Equal
    }) {
        return index;
    }
    work.iter()
        .position(|item| {
            compare_adapter_key(
                item.provider,
                &item.root,
                item.file.path_id,
                cursor.provider,
                &cursor.root,
                cursor.path_id,
            ) == std::cmp::Ordering::Greater
        })
        .unwrap_or(0)
}

struct AdapterProviderWorker {
    roots: HashMap<(Provider, PathBuf), AdapterRootState>,
    interner: PathInterner,
    tails: HashMap<u32, TailFile>,
    bootstrap_emitted: HashSet<u32>,
    meta_emitted: HashSet<u32>,
    record_ordinals: HashMap<u32, (u64, u64)>,
    log_admission: crate::provider::lane::Admission,
    admission_index: crate::provider::lane::AdmissionIndex,
    synthesis: crate::provider::lane::Synthesis,
    /// Highest generation observed per absolute path. Tombstones live for the whole run.
    generations: HashMap<PathBuf, u64>,
    deferred: VecDeque<ProviderEvent>,
    standard_roots: Vec<DiscoveryRoot>,
    late_standard_baselines: HashSet<(Provider, PathBuf)>,
    root_state_factory: RootStateFactory,
    diagnostics: crate::provider::ProviderDiagnostics,
    resume_cursor: Option<ResumeCursor>,
    availability_sweeps: HashMap<Provider, ProviderAvailabilitySweep>,
    #[cfg(test)]
    after_tail_chunk: Option<AfterTailChunkHook>,
}

impl AdapterProviderWorker {
    fn new(
        standard_roots: Vec<DiscoveryRoot>,
        diagnostics: crate::provider::ProviderDiagnostics,
    ) -> Self {
        Self::new_with_log_lane_config(standard_roots, diagnostics, LogLaneConfig::default())
    }

    fn new_with_log_lane_config(
        standard_roots: Vec<DiscoveryRoot>,
        diagnostics: crate::provider::ProviderDiagnostics,
        config: LogLaneConfig,
    ) -> Self {
        let now_ms = unix_now_ms();
        let anchor_ms =
            crate::provider::lane::backfill_anchor_ms(None, now_ms, config.backfill_window_ms);
        Self {
            roots: HashMap::new(),
            interner: PathInterner::default(),
            tails: HashMap::new(),
            bootstrap_emitted: HashSet::new(),
            meta_emitted: HashSet::new(),
            record_ordinals: HashMap::new(),
            log_admission: crate::provider::lane::Admission::new(anchor_ms),
            admission_index: crate::provider::lane::AdmissionIndex::new(),
            synthesis: crate::provider::lane::Synthesis::with_lifecycle_timing_at(
                config.complete_grace_ms,
                config.headless_inactivity_ms,
                now_ms,
            ),
            generations: HashMap::new(),
            deferred: VecDeque::new(),
            standard_roots,
            late_standard_baselines: HashSet::new(),
            root_state_factory: Box::new(AdapterRootState::new),
            diagnostics,
            resume_cursor: None,
            availability_sweeps: HashMap::new(),
            #[cfg(test)]
            after_tail_chunk: None,
        }
    }

    #[cfg(test)]
    fn new_with_root_state_factory(
        standard_roots: Vec<DiscoveryRoot>,
        diagnostics: crate::provider::ProviderDiagnostics,
        factory: impl FnMut(Provider, PathBuf) -> io::Result<AdapterRootState> + Send + 'static,
    ) -> Self {
        let mut worker = Self::new(standard_roots, diagnostics);
        worker.root_state_factory = Box::new(factory);
        worker
    }

    #[cfg(test)]
    fn set_after_tail_chunk(&mut self, hook: impl FnMut(&Path, u64) + Send + 'static) {
        self.after_tail_chunk = Some(Box::new(hook));
    }

    fn emit_due_lifecycle_events(&mut self, pending: &mut PendingEvents) {
        let events = self.synthesis.advance_lifecycle(unix_now_ms());
        let _ = merge_adapter_events(events, pending, &mut self.deferred);
    }

    fn initialize_standard_baselines(&mut self) -> HashMap<(Provider, PathBuf), io::ErrorKind> {
        let mut failures = HashMap::new();
        for root in self.standard_roots.clone() {
            let key = (root.provider, root.path.clone());
            if !self.roots.contains_key(&key) {
                let state = (self.root_state_factory)(root.provider, root.path);
                match state {
                    Ok(state) => {
                        if self.late_standard_baselines.remove(&key) {
                            self.diagnostics.record_baseline_approximation();
                        }
                        self.roots.insert(key, state);
                    }
                    Err(error) => {
                        if error.kind() != io::ErrorKind::NotFound {
                            self.late_standard_baselines.insert(key.clone());
                        }
                        failures.insert(key, error.kind());
                    }
                }
            }
        }
        failures
    }

    fn is_standard_root(&self, provider: Provider, root: &Path) -> bool {
        self.standard_roots
            .iter()
            .any(|standard| standard.provider == provider && standard.path == root)
    }

    #[cfg(test)]
    fn next_open_generation(&mut self, path: &Path) -> u64 {
        let generation = self
            .generations
            .get(path)
            .map_or(0, |generation| generation.saturating_add(1));
        self.generations.insert(path.to_path_buf(), generation);
        generation
    }

    #[cfg(test)]
    fn observe_generation(&mut self, path: PathBuf, observed: u64) {
        self.generations
            .entry(path)
            .and_modify(|generation| *generation = (*generation).max(observed))
            .or_insert(observed);
    }

    fn update_targeted_transition(
        &mut self,
        provider: Provider,
        targeted: bool,
        pending: &mut PendingEvents,
    ) {
        let sweep = self.availability_sweeps.entry(provider).or_default();
        if sweep.targeted == Some(targeted) {
            return;
        }
        sweep.targeted = Some(targeted);
        if !targeted {
            sweep.visited.clear();
            sweep.failure = None;
        }
        let state = if targeted {
            ProviderSourceState::Available
        } else {
            ProviderSourceState::NotApplicable
        };
        let _ = pending.merge(ProviderEvent::SourceState { provider, state });
    }

    fn visit_sweep_member(&mut self, provider: Provider, member: AvailabilitySweepMember) {
        self.availability_sweeps
            .entry(provider)
            .or_default()
            .visited
            .insert(member);
    }

    fn record_sweep_failure(&mut self, provider: Provider, failure: AvailabilitySweepFailure) {
        self.availability_sweeps
            .entry(provider)
            .or_default()
            .failure
            .get_or_insert(failure);
    }

    fn finish_completed_sweeps(
        &mut self,
        universes: &HashMap<Provider, HashSet<AvailabilitySweepMember>>,
        pending: &mut PendingEvents,
    ) {
        for provider in [Provider::Claude, Provider::Codex] {
            let Some(universe) = universes.get(&provider) else {
                continue;
            };
            let sweep = self.availability_sweeps.entry(provider).or_default();
            sweep.visited.retain(|member| universe.contains(member));
            if !universe.is_subset(&sweep.visited) {
                continue;
            }
            let state = sweep
                .failure
                .map_or(ProviderSourceState::Available, |failure| {
                    ProviderSourceState::Unavailable {
                        detail: failure.detail().to_owned(),
                    }
                });
            let _ = pending.merge(ProviderEvent::SourceState { provider, state });
            sweep.visited.clear();
            sweep.failure = None;
        }
    }

    fn parse_record(
        &mut self,
        item: &AdapterWorkItem,
        record: &crate::provider::TailRecord,
    ) -> Vec<ProviderEvent> {
        let mut events = {
            let discovery = &self
                .roots
                .get(&(item.provider, item.root.clone()))
                .expect("work root remains while parsing")
                .discovery;
            parse_adapter_record(item.provider, discovery, &item.file, record)
        };
        if record.error_code.is_some() {
            return events;
        }
        let ordinal = {
            let state = self
                .record_ordinals
                .entry(item.file.path_id)
                .or_insert((record.generation, 0));
            if state.0 != record.generation {
                *state = (record.generation, 0);
            }
            let ordinal = state.1;
            state.1 = state.1.saturating_add(1);
            ordinal
        };
        let Ok(line) = std::str::from_utf8(&record.bytes) else {
            return events;
        };
        let facts = match item.provider {
            Provider::Claude => {
                let scope = match crate::provider::claude::path_topology(&item.file.relative_path) {
                    Some(crate::provider::claude::ClaudePathTopology::Main { thread_id }) => {
                        crate::provider::facts::SessionScope::ClaudeRoot(thread_id)
                    }
                    Some(crate::provider::claude::ClaudePathTopology::Subagent {
                        parent_session,
                        agent_id,
                    }) => crate::provider::facts::SessionScope::ClaudeSubagent {
                        parent: parent_session,
                        agent_id,
                    },
                    _ => return events,
                };
                crate::provider::claude_facts::extract_claude_line(&scope, line)
            }
            Provider::Codex => {
                let Some(rollout_id) = codex_rollout_id(&item.file.relative_path) else {
                    return events;
                };
                crate::provider::codex_facts::extract_codex_line(&rollout_id, ordinal, line)
            }
        };
        events.extend(self.synthesis.synthesize_batch(
            &item.file.root.join(&item.file.relative_path),
            facts.into_iter().map(|fact| (ordinal, fact)),
            &mut self.log_admission,
            &self.admission_index,
        ));
        events
    }
}

impl Default for AdapterProviderWorker {
    fn default() -> Self {
        Self::new(Vec::new(), crate::provider::ProviderDiagnostics::default())
    }
}

impl ProviderWorker for AdapterProviderWorker {
    fn process(&mut self, cycle: &mut ProviderCycle<'_>) -> io::Result<()> {
        let baseline_failures = self.initialize_standard_baselines();
        let mut targets_by_root: HashMap<(Provider, PathBuf), HashSet<PathBuf>> = HashMap::new();
        self.log_admission.begin_pane_cycle();
        for target in cycle.targets.iter() {
            let Ok(root) = provider_root_for_target(target.provider, &target.path) else {
                self.diagnostics.record_invalid_target();
                continue;
            };
            if !self
                .log_admission
                .admit_pane_artifact(target.provider, &target.path)
            {
                self.diagnostics.record_invalid_target();
                continue;
            }
            targets_by_root
                .entry((target.provider, root))
                .or_default()
                .insert(target.path.clone());
        }
        for provider in [Provider::Claude, Provider::Codex] {
            let targeted = targets_by_root
                .keys()
                .any(|(current, _)| *current == provider);
            self.update_targeted_transition(provider, targeted, cycle.pending);
        }
        if !drain_deferred_provider_events(&mut self.deferred, cycle.pending) {
            return Ok(());
        }

        let mut roots = targets_by_root.into_iter().collect::<Vec<_>>();
        roots.sort_by(|left, right| {
            integration_provider_rank((left.0).0)
                .cmp(&integration_provider_rank((right.0).0))
                .then_with(|| (left.0).1.cmp(&(right.0).1))
        });
        let mut universes: HashMap<Provider, HashSet<AvailabilitySweepMember>> = HashMap::new();
        let mut work = Vec::new();
        for ((provider, root), _targets) in roots {
            let root_member = AvailabilitySweepMember::Root(root.clone());
            universes
                .entry(provider)
                .or_default()
                .insert(root_member.clone());
            if let Err(error) = std::fs::read_dir(&root) {
                let failure = match error.kind() {
                    io::ErrorKind::NotFound => AvailabilitySweepFailure::RootNotFound,
                    io::ErrorKind::PermissionDenied => {
                        AvailabilitySweepFailure::RootPermissionDenied
                    }
                    _ => AvailabilitySweepFailure::FileIoError,
                };
                self.record_sweep_failure(provider, failure);
                self.visit_sweep_member(provider, root_member);
                continue;
            }
            let fallback_root = !self.is_standard_root(provider, &root);
            let root_key = (provider, root.clone());
            if let Some(kind) = baseline_failures.get(&root_key) {
                self.record_sweep_failure(
                    provider,
                    if *kind == io::ErrorKind::PermissionDenied {
                        AvailabilitySweepFailure::RootPermissionDenied
                    } else {
                        AvailabilitySweepFailure::FileIoError
                    },
                );
                self.visit_sweep_member(provider, root_member);
                continue;
            }
            if !self.roots.contains_key(&root_key) {
                match (self.root_state_factory)(provider, root.clone()) {
                    Ok(state) => {
                        self.roots.insert(root_key.clone(), state);
                    }
                    Err(error) => {
                        if error.kind() != io::ErrorKind::NotFound {
                            self.record_sweep_failure(
                                provider,
                                if error.kind() == io::ErrorKind::PermissionDenied {
                                    AvailabilitySweepFailure::RootPermissionDenied
                                } else {
                                    AvailabilitySweepFailure::FileIoError
                                },
                            );
                        }
                        self.visit_sweep_member(provider, root_member);
                        continue;
                    }
                }
                if fallback_root {
                    self.diagnostics.record_baseline_approximation();
                }
            }
            cycle.request_watch(root.clone());
            let scan_result = {
                let state = self
                    .roots
                    .get_mut(&root_key)
                    .expect("root state inserted before scan");
                let mut parser = AdapterBootstrapParser::default();
                state.discovery.scan_admitted(
                    &mut parser,
                    &mut self.interner,
                    &self.log_admission,
                    &mut self.admission_index,
                    &self.diagnostics,
                )
            };
            let scan_outcome = match scan_result {
                Ok(outcome) => outcome,
                Err(error) => {
                    if error.kind() != io::ErrorKind::NotFound {
                        self.record_sweep_failure(
                            provider,
                            if error.kind() == io::ErrorKind::PermissionDenied {
                                AvailabilitySweepFailure::RootPermissionDenied
                            } else {
                                AvailabilitySweepFailure::FileIoError
                            },
                        );
                    }
                    self.visit_sweep_member(provider, root_member);
                    continue;
                }
            };
            if scan_outcome.had_file_io_error() {
                self.record_sweep_failure(provider, AvailabilitySweepFailure::FileIoError);
            }
            for path_id in scan_outcome.removed_path_ids() {
                if let Some(tail) = self.tails.remove(path_id) {
                    self.generations
                        .entry(tail.absolute_path())
                        .and_modify(|generation| *generation = (*generation).max(tail.generation()))
                        .or_insert(tail.generation());
                }
                self.bootstrap_emitted.remove(path_id);
                self.meta_emitted.remove(path_id);
                self.record_ordinals.remove(path_id);
            }
            self.visit_sweep_member(provider, root_member);
            let state = self
                .roots
                .get_mut(&root_key)
                .expect("root state remains after scan");
            let files = state
                .discovery
                .files()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let mut relevant = files
                .into_iter()
                .filter(|file| {
                    self.log_admission
                        .is_admitted_file(&file.root.join(&file.relative_path), file.modified_ms)
                })
                .collect::<Vec<_>>();
            relevant.sort_by_key(|file| file.path_id);
            universes.entry(provider).or_default().extend(
                relevant
                    .iter()
                    .map(|file| AvailabilitySweepMember::File(file.path_id)),
            );
            work.extend(relevant.into_iter().map(|file| AdapterWorkItem {
                provider,
                root: root.clone(),
                file,
            }));
        }

        for provider in [Provider::Claude, Provider::Codex] {
            if let Some(universe) = universes.get(&provider) {
                self.availability_sweeps
                    .entry(provider)
                    .or_default()
                    .visited
                    .retain(|member| universe.contains(member));
            }
        }

        work.sort_by(|left, right| {
            compare_adapter_key(
                left.provider,
                &left.root,
                left.file.path_id,
                right.provider,
                &right.root,
                right.file.path_id,
            )
        });
        let mut owned_paths = HashSet::new();
        work.retain(|item| {
            if owned_paths.insert(item.file.path_id) {
                true
            } else {
                self.diagnostics.record_duplicate_path_target();
                false
            }
        });
        if work.is_empty() {
            self.finish_completed_sweeps(&universes, cycle.pending);
            self.emit_due_lifecycle_events(cycle.pending);
            return Ok(());
        }
        let start = resume_start(&work, self.resume_cursor.as_ref());
        for (index, item) in work.iter().enumerate().skip(start) {
            let provider = item.provider;
            let file = &item.file;
            if let Some(parent) = file.root.join(&file.relative_path).parent()
                && parent.is_dir()
            {
                cycle.request_watch(parent.to_path_buf());
            }
            let absolute = file.root.join(&file.relative_path);
            if !self
                .log_admission
                .is_admitted_file(&absolute, file.modified_ms)
            {
                continue;
            }
            if let Some(crate::provider::claude::ClaudePathTopology::SubagentMeta {
                parent_session,
                agent_id,
            }) = (provider == Provider::Claude)
                .then(|| crate::provider::claude::path_topology(&file.relative_path))
                .flatten()
            {
                if !self.meta_emitted.contains(&file.path_id) {
                    let mut meta_io_error = false;
                    let fact = match crate::provider::open_admitted_regular_file(
                        &file.root,
                        &file.relative_path,
                        &self.log_admission,
                        &self.diagnostics,
                    ) {
                        Ok(Some(file_handle)) => {
                            let mut bytes = Vec::new();
                            match file_handle
                                .take(crate::provider::BOOTSTRAP_MAX_BYTES as u64)
                                .read_to_end(&mut bytes)
                            {
                                Ok(_) => crate::provider::claude_facts::extract_meta_json(
                                    &parent_session,
                                    &agent_id,
                                    &bytes,
                                ),
                                Err(_) => {
                                    meta_io_error = true;
                                    None
                                }
                            }
                        }
                        Ok(None) => None,
                        Err(_) => {
                            meta_io_error = true;
                            None
                        }
                    };
                    if meta_io_error {
                        self.availability_sweeps
                            .entry(provider)
                            .or_default()
                            .failure
                            .get_or_insert(AvailabilitySweepFailure::FileIoError);
                    }
                    if let Some(fact) = fact {
                        self.meta_emitted.insert(file.path_id);
                        let events = self.synthesis.synthesize_batch(
                            &absolute,
                            [(0, fact)],
                            &mut self.log_admission,
                            &self.admission_index,
                        );
                        if !merge_adapter_events(events, cycle.pending, &mut self.deferred) {
                            self.resume_cursor = Some(cursor_for_work(item));
                            self.finish_completed_sweeps(&universes, cycle.pending);
                            return Ok(());
                        }
                    }
                }
                self.availability_sweeps
                    .entry(provider)
                    .or_default()
                    .visited
                    .insert(AvailabilitySweepMember::File(file.path_id));
                continue;
            }
            if !self.tails.contains_key(&file.path_id) {
                let mut boundary = FsReadBoundary;
                let generation = self
                    .generations
                    .get(&absolute)
                    .map_or(0, |generation| generation.saturating_add(1));
                self.generations.insert(absolute, generation);
                let baseline = self
                    .roots
                    .get(&(provider, item.root.clone()))
                    .expect("work root was scanned")
                    .discovery
                    .baseline()
                    .clone();
                let tail = match TailFile::open(
                    &file.root,
                    &file.relative_path,
                    &baseline,
                    generation,
                    &mut boundary,
                ) {
                    Ok(tail) => tail,
                    Err(error) => {
                        let member = AvailabilitySweepMember::File(file.path_id);
                        let sweep = self.availability_sweeps.entry(provider).or_default();
                        if error.kind() != io::ErrorKind::NotFound {
                            sweep
                                .failure
                                .get_or_insert(AvailabilitySweepFailure::FileIoError);
                        }
                        sweep.visited.insert(member);
                        continue;
                    }
                };
                let ordinal = if tail.offset() == 0 {
                    0
                } else {
                    match crate::provider::lane::record_ordinal_at_offset(
                        &file.root,
                        &file.relative_path,
                        tail.offset(),
                        &self.log_admission,
                        &self.diagnostics,
                    ) {
                        Ok(ordinal) => ordinal,
                        Err(_) => {
                            self.availability_sweeps
                                .entry(provider)
                                .or_default()
                                .failure
                                .get_or_insert(AvailabilitySweepFailure::FileIoError);
                            continue;
                        }
                    }
                };
                self.record_ordinals
                    .insert(file.path_id, (tail.generation(), ordinal));
                self.tails.insert(file.path_id, tail);
            }
            let mut boundary = FsReadBoundary;
            let mut goalpost = match self
                .tails
                .get_mut(&file.path_id)
                .expect("tail inserted for relevant file")
                .cycle_goalpost(&mut boundary)
            {
                Ok(goalpost) => goalpost,
                Err(error) => {
                    let sweep = self.availability_sweeps.entry(provider).or_default();
                    if error.kind() != io::ErrorKind::NotFound {
                        sweep
                            .failure
                            .get_or_insert(AvailabilitySweepFailure::FileIoError);
                    }
                    sweep
                        .visited
                        .insert(AvailabilitySweepMember::File(file.path_id));
                    continue;
                }
            };
            let tail_generation = self
                .tails
                .get(&file.path_id)
                .expect("tail remains after goalpost snapshot")
                .generation();
            if self.bootstrap_emitted.insert(file.path_id) {
                let event = match provider {
                    Provider::Codex => crate::provider::codex::CodexAdapter.bootstrap_event(
                        file,
                        tail_generation,
                        unix_now_ms(),
                    ),
                    Provider::Claude => crate::provider::claude::ClaudeAdapter.bootstrap_event(
                        file,
                        tail_generation,
                        unix_now_ms(),
                    ),
                };
                if let Some(event) = event
                    && !merge_adapter_events(
                        std::iter::once(event),
                        cycle.pending,
                        &mut self.deferred,
                    )
                {
                    self.resume_cursor = Some(cursor_for_work(item));
                    self.finish_completed_sweeps(&universes, cycle.pending);
                    return Ok(());
                }
            }
            self.availability_sweeps
                .entry(provider)
                .or_default()
                .visited
                .insert(AvailabilitySweepMember::File(file.path_id));
            let mut replacement_goalpost_refrozen = false;
            loop {
                let (previous_generation, previous_offset) = self
                    .tails
                    .get(&file.path_id)
                    .map(|tail| (tail.generation(), tail.offset()))
                    .expect("tail inserted for relevant file");
                if previous_offset >= goalpost || cycle.should_stop() {
                    break;
                }
                let mut boundary = FsReadBoundary;
                let poll_result = self
                    .tails
                    .get_mut(&file.path_id)
                    .expect("tail inserted for relevant file")
                    .poll_to_goalpost(&mut boundary, goalpost);
                let tail = self
                    .tails
                    .get(&file.path_id)
                    .expect("tail remains after poll");
                let tail_generation = tail.generation();
                let current_offset = tail.offset();
                #[cfg(test)]
                if current_offset != previous_offset
                    && let Some(hook) = self.after_tail_chunk.as_mut()
                {
                    hook(&file.root.join(&file.relative_path), current_offset);
                }
                self.generations
                    .entry(file.root.join(&file.relative_path))
                    .and_modify(|generation| *generation = (*generation).max(tail_generation))
                    .or_insert(tail_generation);
                let mut records = match poll_result {
                    Ok(records) => records.into_iter(),
                    Err(error) => {
                        if error.kind() != io::ErrorKind::NotFound {
                            self.availability_sweeps
                                .entry(provider)
                                .or_default()
                                .failure
                                .get_or_insert(AvailabilitySweepFailure::FileIoError);
                        }
                        break;
                    }
                };
                while let Some(record) = records.next() {
                    let events = self.parse_record(item, &record);
                    if !merge_adapter_events(events, cycle.pending, &mut self.deferred) {
                        let mut remaining = Vec::new();
                        for record in records {
                            remaining.extend(self.parse_record(item, &record));
                        }
                        self.deferred.extend(remaining);
                        let next = if index + 1 < work.len() {
                            &work[index + 1]
                        } else {
                            &work[0]
                        };
                        self.resume_cursor = Some(cursor_for_work(next));
                        self.finish_completed_sweeps(&universes, cycle.pending);
                        return Ok(());
                    }
                }
                if tail_generation != previous_generation {
                    if replacement_goalpost_refrozen {
                        break;
                    }
                    replacement_goalpost_refrozen = true;
                    let mut boundary = FsReadBoundary;
                    goalpost = match self
                        .tails
                        .get_mut(&file.path_id)
                        .expect("tail remains after rotation")
                        .cycle_goalpost(&mut boundary)
                    {
                        Ok(goalpost) => goalpost,
                        Err(error) => {
                            if error.kind() != io::ErrorKind::NotFound {
                                self.availability_sweeps
                                    .entry(provider)
                                    .or_default()
                                    .failure
                                    .get_or_insert(AvailabilitySweepFailure::FileIoError);
                            }
                            break;
                        }
                    };
                }
                if tail_generation == previous_generation && current_offset <= previous_offset {
                    break;
                }
            }
            if cycle.should_stop() {
                return Ok(());
            }
        }
        self.resume_cursor = None;
        self.finish_completed_sweeps(&universes, cycle.pending);
        self.emit_due_lifecycle_events(cycle.pending);
        Ok(())
    }

    fn graceful_stop(&mut self) -> Vec<ProviderEvent> {
        self.synthesis.flush_pending_completes()
    }
}

fn parse_adapter_record(
    provider: Provider,
    discovery: &DiscoveryIndex,
    file: &crate::provider::DiscoveredFile,
    record: &crate::provider::TailRecord,
) -> Vec<ProviderEvent> {
    match provider {
        Provider::Codex => {
            crate::provider::codex::CodexAdapter.parse_record(discovery, file, record)
        }
        Provider::Claude => {
            crate::provider::claude::ClaudeAdapter.parse_record(discovery, file, record)
        }
    }
}

fn codex_rollout_id(path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    crate::provider::facts::scan_raw_ids(file_name)
        .into_iter()
        .filter_map(|id| match id {
            crate::provider::facts::EvidenceId::Uuid(uuid) => Some(uuid),
            crate::provider::facts::EvidenceId::ConfigDir(_) => None,
        })
        .next_back()
}

fn drain_deferred_provider_events(
    deferred: &mut VecDeque<ProviderEvent>,
    pending: &mut PendingEvents,
) -> bool {
    while let Some(event) = deferred.pop_front() {
        match pending.merge(event) {
            MergeOutcome::AtCapacity(event) => {
                deferred.push_front(*event);
                return false;
            }
            MergeOutcome::Accepted | MergeOutcome::Coalesced | MergeOutcome::Duplicate => {}
        }
    }
    true
}

fn merge_adapter_events(
    events: impl IntoIterator<Item = ProviderEvent>,
    pending: &mut PendingEvents,
    deferred: &mut VecDeque<ProviderEvent>,
) -> bool {
    let mut events = events.into_iter();
    while let Some(event) = events.next() {
        match pending.merge(event) {
            MergeOutcome::AtCapacity(event) => {
                deferred.push_back(*event);
                deferred.extend(events);
                return false;
            }
            MergeOutcome::Accepted | MergeOutcome::Coalesced | MergeOutcome::Duplicate => {}
        }
    }
    true
}

fn provider_root_for_target(provider: Provider, target: &Path) -> io::Result<PathBuf> {
    if !target.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provider target must be an absolute path",
        ));
    }
    let anchor = match provider {
        Provider::Codex => "sessions",
        Provider::Claude => "projects",
    };
    if let Some(root) = target.ancestors().find(|ancestor| {
        ancestor.file_name() == Some(OsStr::new(anchor))
            && ancestor.parent().is_some_and(|parent| {
                parent.file_name()
                    == Some(OsStr::new(match provider {
                        Provider::Codex => ".codex",
                        Provider::Claude => ".claude",
                    }))
            })
    }) {
        return Ok(root.to_path_buf());
    }
    target
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "provider target has no parent"))
}

const fn integration_provider_rank(provider: Provider) -> u8 {
    match provider {
        Provider::Claude => 0,
        Provider::Codex => 1,
    }
}

struct ProviderIntegration {
    events: Option<mpsc::Receiver<ProviderIngressEvent>>,
    events_drained: Option<oneshot::Sender<()>>,
    target_publisher: ProviderTargetPublisher,
    published_targets: TargetSet,
    coverage: CoverageTracker,
}

struct CoverageTracker {
    registry: SourceCoverageRegistry,
    herdr_quality: ObservationQuality,
    coverage_sender: watch::Sender<SourceCoverageRegistry>,
    source_quality_sender: watch::Sender<ObservationQuality>,
}

impl CoverageTracker {
    fn new(
        controller: SourceAvailability,
        coverage_sender: watch::Sender<SourceCoverageRegistry>,
        source_quality_sender: watch::Sender<ObservationQuality>,
    ) -> Self {
        Self {
            registry: SourceCoverageRegistry::new(controller),
            herdr_quality: ObservationQuality::Reconciling,
            coverage_sender,
            source_quality_sender,
        }
    }

    fn set_herdr_quality(&mut self, quality: ObservationQuality) {
        self.herdr_quality = quality;
        let state = match quality {
            ObservationQuality::Disconnected => SourceAvailability::Unavailable {
                detail: "disconnected".to_owned(),
            },
            ObservationQuality::Live
            | ObservationQuality::Reconciling
            | ObservationQuality::Degraded => SourceAvailability::Available,
        };
        self.registry.set(CoverageSource::Herdr, state);
        self.publish();
    }

    fn update_provider_state(&mut self, provider: Provider, state: ProviderSourceState) {
        let source = match provider {
            Provider::Claude => CoverageSource::Claude,
            Provider::Codex => CoverageSource::Codex,
        };
        let state = match state {
            ProviderSourceState::Available => SourceAvailability::Available,
            ProviderSourceState::Unavailable { detail } => {
                SourceAvailability::Unavailable { detail }
            }
            ProviderSourceState::NotApplicable => SourceAvailability::NotApplicable,
        };
        self.registry.set(source, state);
        self.publish();
    }

    fn mark_egress_closed(&mut self) {
        for source in [CoverageSource::Claude, CoverageSource::Codex] {
            if !matches!(
                self.registry.state(source),
                SourceAvailability::NotApplicable
            ) {
                self.registry.set(
                    source,
                    SourceAvailability::Unavailable {
                        detail: "provider_thread_closed".to_owned(),
                    },
                );
            }
        }
        self.publish();
    }

    fn publish(&self) {
        self.coverage_sender.send_replace(self.registry.clone());
        self.source_quality_sender
            .send_replace(self.registry.effective_quality(self.herdr_quality));
    }
}

impl ProviderIntegration {
    #[cfg(test)]
    fn new(
        events: mpsc::Receiver<ProviderIngressEvent>,
        target_publisher: ProviderTargetPublisher,
        published_targets: TargetSet,
        coverage: CoverageTracker,
    ) -> Self {
        Self {
            events: Some(events),
            events_drained: None,
            target_publisher,
            published_targets,
            coverage,
        }
    }

    fn new_with_drain_acknowledgement(
        events: mpsc::Receiver<ProviderIngressEvent>,
        target_publisher: ProviderTargetPublisher,
        published_targets: TargetSet,
        coverage: CoverageTracker,
        events_drained: oneshot::Sender<()>,
    ) -> Self {
        Self {
            events: Some(events),
            events_drained: Some(events_drained),
            target_publisher,
            published_targets,
            coverage,
        }
    }

    fn acknowledge_events_drained(&mut self) {
        if let Some(events_drained) = self.events_drained.take() {
            let _ = events_drained.send(());
        }
    }

    fn set_herdr_quality(
        &mut self,
        quality: ObservationQuality,
        persistence: &mut RuntimePersistence,
        shared: &SharedModel,
    ) {
        self.coverage.set_herdr_quality(quality);
        persistence.refresh_snapshot(&shared.borrow(), &self.coverage.registry);
    }

    fn update_source_state(&mut self, provider: Provider, state: ProviderSourceState) {
        self.coverage.update_provider_state(provider, state);
    }

    fn publish_targets(&mut self, shared: &SharedModel) {
        let targets = derive_provider_targets(&shared.borrow());
        if targets != self.published_targets {
            self.target_publisher.update_targets(targets.clone());
            self.published_targets = targets;
        }
    }
}

fn derive_provider_targets(model: &DomainModel) -> TargetSet {
    let run_targets = model.task_runs().filter_map(|run| match &run.key {
        RunKey::NativePath { provider, path } if !path.is_empty() => Some(ProviderTarget {
            provider: *provider,
            path: PathBuf::from(path),
        }),
        RunKey::Controller(_)
        | RunKey::Native { .. }
        | RunKey::NativePath { .. }
        | RunKey::Provisional { .. } => None,
    });
    let node_targets = model.agent_nodes().filter_map(|node| {
        node.session_file
            .as_deref()
            .filter(|path| !path.is_empty())
            .map(|path| ProviderTarget {
                provider: node.provider,
                path: PathBuf::from(path),
            })
    });
    TargetSet::new(run_targets.chain(node_targets))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum EntityKey {
    Workspace(String),
    Tab(String),
    Pane(String),
}

#[derive(Clone, Copy)]
enum ReplayOutcome {
    Clean,
    Dirty,
    Ended,
    Cancelled,
    WatchdogReconnect(WatchdogReconnectReason),
}

#[derive(Clone, Copy)]
enum ReconcilingOutcome {
    RestartGeneration,
    Ended,
    Cancelled,
    WatchdogReconnect(WatchdogReconnectReason),
}

#[derive(Clone, Copy)]
enum SubscriptionOutcome {
    Ended,
    Cancelled,
    WatchdogReconnect(WatchdogReconnectReason),
}

#[derive(Clone, Copy)]
enum WatchdogReconnectReason {
    ProbeFailed,
    TopologyDiverged,
}

impl WatchdogReconnectReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ProbeFailed => "snapshot_probe_failed",
            Self::TopologyDiverged => "topology_diverged",
        }
    }
}

struct ConvergeOutcome {
    outcome: SubscriptionOutcome,
    gap_committed: bool,
}

#[derive(Clone, Default)]
struct PendingTopologyClosures {
    workspaces: HashSet<String>,
    tabs: HashSet<String>,
    panes: HashSet<String>,
}

fn cancel_pending_topology_closures(
    received: &ReceivedEvent,
    pending: &mut PendingTopologyClosures,
) {
    let mut reobserved = created_entities(received);
    reobserved.extend(updated_entity(received));
    for entity in reobserved {
        match entity {
            EntityKey::Workspace(workspace_id) => {
                pending.workspaces.remove(&workspace_id);
            }
            EntityKey::Tab(tab_id) => {
                pending.tabs.remove(&tab_id);
            }
            EntityKey::Pane(pane_id) => {
                pending.panes.remove(&pane_id);
            }
        }
    }
}

impl ConvergeOutcome {
    const fn new(outcome: SubscriptionOutcome, gap_committed: bool) -> Self {
        Self {
            outcome,
            gap_committed,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EventChannelState {
    Open,
    Closed,
}

fn drain_events(
    events: &mut mpsc::Receiver<Admitted<ReceivedEvent>>,
    buffered: &mut VecDeque<Admitted<ReceivedEvent>>,
) -> EventChannelState {
    loop {
        match events.try_recv() {
            Ok(received) => buffered.push_back(received),
            Err(mpsc::error::TryRecvError::Empty) => return EventChannelState::Open,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                return EventChannelState::Closed;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_received_event(
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
    owner: &mut OwnerTracker,
    session: &str,
    received: ReceivedEvent,
    admission: Admission,
    pending_closures: &mut PendingTopologyClosures,
    provider: &mut ProviderIntegration,
) -> Result<(), CollectorError> {
    if received.event == "pane_moved"
        && let Err(error) = owner.refresh_from_move(&received.data, persistence).await
    {
        admission.complete();
        return Err(error.into());
    }
    let normalized = match normalize_event(shared, session, &received) {
        Ok(normalized) => normalized,
        Err(error) => {
            admission.complete();
            return Err(error);
        }
    };
    let outcome = apply_collector_observation(reducer, normalized);
    admission.complete();
    let Some(persist) = outcome? else {
        persistence.refresh_snapshot(&shared.borrow(), &provider.coverage.registry);
        return Ok(());
    };
    if !persist.is_empty() {
        let _ = persist_submission(persistence, reducer, persist).await?;
    }
    persistence.refresh_snapshot(&shared.borrow(), &provider.coverage.registry);
    provider.publish_targets(shared);
    cancel_pending_topology_closures(&received, pending_closures);
    Ok(())
}

fn cancel_pending_pane_closure(pane_id: &str, pending: &mut PendingTopologyClosures) {
    pending.panes.remove(pane_id);
}

#[allow(clippy::too_many_arguments)]
async fn apply_enrichment_payload(
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
    session: &str,
    payload: EnrichmentPayload,
    target_set: &BTreeSet<String>,
    performance: &PerformanceIngress,
    pending_closures: &mut PendingTopologyClosures,
    provider: &mut ProviderIntegration,
) -> Result<(), CollectorError> {
    cancel_pending_pane_closure(&payload.pane_id, pending_closures);
    if !target_set.contains(&payload.pane_id) {
        return Ok(());
    }

    let events = agent_status_events(
        shared,
        session,
        &payload.pane_id,
        payload.terminal_id.as_deref(),
        payload.state,
        Some((payload.timestamp_ms, payload.receipt_time_ms)),
    );
    if events.is_empty() {
        return Ok(());
    }

    let admission = performance.admit();
    let outcome = apply_collector_observation(reducer, events);
    admission.complete();
    if let Some(persist) = outcome?
        && !persist.is_empty()
    {
        let _ = persist_submission(persistence, reducer, persist).await?;
    }
    persistence.refresh_snapshot(&shared.borrow(), &provider.coverage.registry);
    provider.publish_targets(shared);
    Ok(())
}

fn apply_collector_event(
    reducer: &mut Reducer,
    event: NormalizedEvent,
) -> Result<Option<PersistBatch>, ReducerError> {
    let outcome = reducer.apply(event)?;
    Ok(collector_apply_outcome(reducer, outcome))
}

fn apply_collector_observation(
    reducer: &mut Reducer,
    events: Vec<NormalizedEvent>,
) -> Result<Option<PersistBatch>, ReducerError> {
    let outcome = reducer.apply_observation(events)?;
    Ok(collector_apply_outcome(reducer, outcome))
}

#[cfg(test)]
async fn apply_provider_event(
    event: ProviderEvent,
    session: &str,
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
    coverage: &SourceCoverageRegistry,
) -> Result<(), CollectorError> {
    apply_provider_event_with_admission(
        event,
        None,
        session,
        reducer,
        shared,
        persistence,
        coverage,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn apply_provider_event_with_admission(
    event: ProviderEvent,
    mut admission: Option<Admission>,
    session: &str,
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
    coverage: &SourceCoverageRegistry,
) -> Result<(), CollectorError> {
    match event {
        ProviderEvent::Synthesized(mut event) => {
            event.metadata.herdr_session = session.to_owned();
            event.metadata.receipt_time_ms = unix_now_ms();
            event.metadata.source_coverage = coverage.provider_metadata();
            if persistence.is_duplicate(&event.metadata.event_id) {
                if let Some(admission) = admission.take() {
                    admission.complete();
                }
                return Ok(());
            }
            let delta = match reducer.validate_controller_event(&event) {
                Ok(delta) => delta,
                Err(reason) => {
                    tracing::warn!(
                        warning_code = "provider_synthesized_event_rejected",
                        event_id = event.metadata.event_id,
                        ?reason,
                        "rejected provider-log synthesized event"
                    );
                    if let Some(admission) = admission.take() {
                        admission.complete();
                    }
                    return Ok(());
                }
            };
            let Some(permit) = persistence.reserve_enqueue() else {
                if let Some(admission) = admission.take() {
                    admission.complete();
                }
                return Ok(());
            };
            let pending = match reducer.commit_staged(delta, permit) {
                Ok(pending) => pending,
                Err(CommitStagedError::IngestSequenceExhausted) => {
                    if let Some(admission) = admission.take() {
                        admission.complete();
                    }
                    return Ok(());
                }
            };
            if let Some(admission) = admission.take() {
                admission.complete();
            }
            let outcome = persistence.finish_pending(pending).await?;
            reducer.complete_operator_submission(outcome);
            Ok(())
        }
        ProviderEvent::RunLiveness { key, at_ms } => {
            let persist = reducer.touch_run_liveness(&key, at_ms);
            if let Some(admission) = admission.take() {
                admission.complete();
            }
            if !persist.is_empty() {
                let _ = persist_submission(persistence, reducer, persist).await?;
            }
            persistence.refresh_snapshot(&shared.borrow(), coverage);
            Ok(())
        }
        ProviderEvent::LaneClose { key, at_ms } => {
            let persist = reducer.apply_lane_close(&key, at_ms);
            if let Some(admission) = admission.take() {
                admission.complete();
            }
            if !persist.is_empty() {
                let _ = persist_submission(persistence, reducer, persist).await?;
            }
            persistence.refresh_snapshot(&shared.borrow(), coverage);
            Ok(())
        }
        ProviderEvent::Telemetry {
            key,
            at_ms,
            output_tokens,
            model,
            effort,
        } => {
            let persist = reducer.apply_telemetry(&key, at_ms, output_tokens, model, effort);
            debug_assert!(persist.is_empty(), "telemetry must remain transient");
            if let Some(admission) = admission.take() {
                admission.complete();
            }
            Ok(())
        }
        event => {
            apply_normalized_provider_event(
                event,
                admission,
                session,
                reducer,
                shared,
                persistence,
                coverage,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_normalized_provider_event(
    event: ProviderEvent,
    mut admission: Option<Admission>,
    session: &str,
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
    coverage: &SourceCoverageRegistry,
) -> Result<(), CollectorError> {
    let normalized = normalize_provider_event(shared, session, event, coverage);
    let identity_disagreement = normalized.identity_disagreement;
    let events = normalized
        .events
        .into_iter()
        .filter(|event| !persistence.is_duplicate(&normalized_metadata(event).event_id))
        .collect::<Vec<_>>();
    if events.is_empty() {
        if let Some(admission) = admission.take() {
            admission.complete();
        }
        return Ok(());
    }
    let disagreement_is_new = identity_disagreement
        && events
            .iter()
            .any(|event| normalized_metadata(event).source_event_type == "session_resolved");
    let outcome = apply_collector_observation(reducer, events);
    if let Some(admission) = admission.take() {
        admission.complete();
    }
    if let Some(persist) = outcome?
        && !persist.is_empty()
    {
        let _ = persist_submission(persistence, reducer, persist).await?;
    }
    if disagreement_is_new {
        reducer.record_provider_identity_disagreement();
    }
    Ok(())
}

fn collector_apply_outcome(reducer: &mut Reducer, outcome: ApplyOutcome) -> Option<PersistBatch> {
    match outcome {
        ApplyOutcome::Applied(persist) => Some(persist),
        ApplyOutcome::DroppedBindingConflict(_conflict) => {
            reducer.record_binding_conflict();
            None
        }
    }
}

fn normalize_event(
    shared: &SharedModel,
    session: &str,
    received: &ReceivedEvent,
) -> Result<Vec<NormalizedEvent>, CollectorError> {
    let mut events = Vec::new();
    match received.event.as_str() {
        "workspace_created" => {
            if let Some(value) = received.data.get("workspace") {
                let workspace: WorkspaceInfo = decode_wire(value)?;
                events.push(topology_upsert(
                    session,
                    &received.event,
                    TopologyEntity::Workspace(Workspace {
                        workspace_id: workspace.workspace_id,
                    }),
                ));
            }
        }
        "workspace_renamed" => {
            if let Some(value) = received.data.get("workspace") {
                let workspace: WorkspaceInfo = decode_wire(value)?;
                if shared.borrow().workspace(&workspace.workspace_id).is_some() {
                    events.push(topology_upsert(
                        session,
                        &received.event,
                        TopologyEntity::Workspace(Workspace {
                            workspace_id: workspace.workspace_id,
                        }),
                    ));
                }
            }
        }
        "workspace_closed" => {
            if let Some(workspace_id) = string_field(&received.data, "workspace_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Workspace { workspace_id },
                ));
            }
        }
        "tab_created" => {
            if let Some(value) = received.data.get("tab") {
                let tab: TabInfo = decode_wire(value)?;
                events.push(topology_upsert(
                    session,
                    &received.event,
                    TopologyEntity::Tab(tab_entity(tab)),
                ));
            }
        }
        "tab_renamed" => {
            if let Some(tab_id) = string_field(&received.data, "tab_id")
                && let Some(tab) = shared.borrow().tab(&tab_id).cloned()
            {
                events.push(topology_upsert(
                    session,
                    &received.event,
                    TopologyEntity::Tab(Tab {
                        tab_id,
                        workspace_id: tab.workspace_id,
                        label: string_field(&received.data, "label")
                            .as_deref()
                            .and_then(sanitized_name),
                    }),
                ));
            }
        }
        "tab_closed" => {
            if let Some(tab_id) = string_field(&received.data, "tab_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Tab { tab_id },
                ));
            }
        }
        "pane_created" => {
            if let Some(value) = received.data.get("pane") {
                let pane: PaneInfo = decode_wire(value)?;
                append_pane_upsert(shared, session, &received.event, pane, &mut events)?;
            }
        }
        "pane_updated" | "pane_agent_detected" => {
            if let Some(value) = received.data.get("pane") {
                let pane: PaneInfo = decode_wire(value)?;
                if shared.borrow().pane(&pane.pane_id).is_some() {
                    append_pane_upsert(shared, session, &received.event, pane, &mut events)?;
                }
            } else if received.event == "pane_agent_detected"
                && string_field(&received.data, "pane_id").is_some()
            {
                received
                    .primary_stream_diagnostics
                    .record_flat_pane_agent_detected();
            }
        }
        "pane_closed" => {
            if let Some(pane_id) = string_field(&received.data, "pane_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Pane { pane_id },
                ));
            }
        }
        "pane_exited" => {
            let pane_id = string_field(&received.data, "pane_id");
            append_execution_ends(
                shared,
                session,
                &received.event,
                pane_id.as_deref(),
                &mut events,
            );
        }
        "pane_moved" => {
            if let Some(value) = received.data.get("created_tab") {
                let tab: TabInfo = decode_wire(value)?;
                events.push(topology_upsert(
                    session,
                    &received.event,
                    TopologyEntity::Tab(tab_entity(tab)),
                ));
            }
            if let Some(value) = received.data.get("pane") {
                let pane: PaneInfo = decode_wire(value)?;
                append_pane_move(shared, session, &received.event, pane, &mut events)?;
            }
            if let Some(tab_id) = string_field(&received.data, "closed_tab_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Tab { tab_id },
                ));
            }
            if let Some(workspace_id) = string_field(&received.data, "closed_workspace_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Workspace { workspace_id },
                ));
            }
            if let Some(previous_pane_id) = string_field(&received.data, "previous_pane_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Pane {
                        pane_id: previous_pane_id,
                    },
                ));
            }
        }
        "pane_agent_status_changed" => {
            let state = status_from_value(
                received
                    .data
                    .get("agent_status")
                    .or_else(|| received.data.get("status"))
                    .or_else(|| received.data.get("new_status")),
            );
            let pane_id = string_field(&received.data, "pane_id")
                .or_else(|| nested_string(&received.data, "pane", "pane_id"));
            let terminal_id = string_field(&received.data, "terminal_id")
                .or_else(|| nested_string(&received.data, "pane", "terminal_id"));
            if let Some(pane_id) = pane_id {
                events.extend(agent_status_events(
                    shared,
                    session,
                    &pane_id,
                    terminal_id.as_deref(),
                    state,
                    None,
                ));
            }
        }
        _ => {}
    }
    Ok(events)
}

fn agent_status_events(
    shared: &SharedModel,
    session: &str,
    pane_id: &str,
    terminal_id: Option<&str>,
    state: ExecState,
    receipt_instants: Option<(i64, i64)>,
) -> Vec<NormalizedEvent> {
    let executions: Vec<_> = shared
        .borrow()
        .executions()
        .filter(|execution| {
            !execution.state.is_terminal()
                && (execution.pane_id == pane_id
                    || terminal_id == Some(execution.terminal_id.as_str()))
                && std::mem::discriminant(&execution.state) != std::mem::discriminant(&state)
        })
        .cloned()
        .collect();
    executions
        .into_iter()
        .map(|execution| {
            let mut metadata = metadata(session, "pane_agent_status_changed");
            metadata.pane_id = Some(pane_id.to_owned());
            metadata.terminal_id = terminal_id.map(str::to_owned);
            if let Some((timestamp_ms, receipt_time_ms)) = receipt_instants {
                metadata.timestamp_ms = timestamp_ms;
                metadata.receipt_time_ms = receipt_time_ms;
            }
            NormalizedEvent::AgentStatusChanged {
                metadata,
                execution_id: execution.execution_id,
                state: state.clone(),
            }
        })
        .collect()
}

fn append_pane_move(
    shared: &SharedModel,
    session: &str,
    event_kind: &str,
    pane: PaneInfo,
    events: &mut Vec<NormalizedEvent>,
) -> Result<(), CollectorError> {
    let pane_entity = pane_entity(&pane)?;
    events.push(topology_upsert(
        session,
        event_kind,
        TopologyEntity::Pane(pane_entity.clone()),
    ));
    for execution in shared.borrow().executions().filter(|execution| {
        execution.terminal_id == pane.terminal_id && !execution.state.is_terminal()
    }) {
        let mut moved = execution.clone();
        moved.pane_id.clone_from(&pane_entity.pane_id);
        events.push(NormalizedEvent::ExecutionBegin {
            metadata: pane_metadata(session, event_kind, &pane),
            execution: moved,
        });
    }
    Ok(())
}

fn append_pane_upsert(
    shared: &SharedModel,
    session: &str,
    event_kind: &str,
    pane: PaneInfo,
    events: &mut Vec<NormalizedEvent>,
) -> Result<(), CollectorError> {
    events.push(topology_upsert(
        session,
        event_kind,
        TopologyEntity::Pane(pane_entity(&pane)?),
    ));
    let current: Vec<_> = shared
        .borrow()
        .executions()
        .filter(|execution| {
            execution.terminal_id == pane.terminal_id && !execution.state.is_terminal()
        })
        .cloned()
        .collect();
    let provider = pane_provider(&pane);
    let incoming_sid = pane.agent_session.as_ref().and_then(|reference| {
        (reference.kind == AgentSessionKind::Id && !reference.value.is_empty())
            .then_some(reference.value.as_str())
    });
    if pane.agent.is_none() {
        if incoming_sid.is_some() {
            for execution in current {
                let has_native = run_has_native_binding(shared, execution.task_run_id);
                let same_native = provider.zip(incoming_sid).is_some_and(|(provider, sid)| {
                    run_owns_native(shared, execution.task_run_id, provider, sid)
                });
                if has_native && !same_native {
                    // An agent-less pane carries no liveness that could justify a
                    // replacement run, so conflicting latched evidence is dropped.
                    tracing::warn!(
                        warning_code = "agentless_session_evidence_conflict",
                        "skipped conflicting agent-session evidence from an agent-less pane update"
                    );
                    continue;
                }
                events.push(NormalizedEvent::ExecutionBegin {
                    metadata: pane_metadata(session, event_kind, &pane),
                    execution,
                });
            }
        }
        return Ok(());
    }
    let mut reused = false;
    for execution in current {
        let has_native = run_has_native_binding(shared, execution.task_run_id);
        let same_native = provider.zip(incoming_sid).is_some_and(|(provider, sid)| {
            run_owns_native(shared, execution.task_run_id, provider, sid)
        });
        let should_reuse = incoming_sid.is_none() || same_native || !has_native;
        if should_reuse && !reused {
            let mut updated = execution;
            updated.pane_id.clone_from(&pane.pane_id);
            updated.state = status_from_str(pane.agent_status.as_deref());
            events.push(NormalizedEvent::ExecutionBegin {
                metadata: pane_metadata(session, event_kind, &pane),
                execution: updated,
            });
            reused = true;
        } else {
            events.push(NormalizedEvent::ExecutionEnd {
                metadata: metadata(session, event_kind),
                execution_id: execution.execution_id,
            });
        }
    }
    if !reused {
        events.push(execution_begin(session, event_kind, &pane));
    }
    Ok(())
}

fn run_has_native_binding(shared: &SharedModel, run_id: RunId) -> bool {
    let model = shared.borrow();
    model
        .task_run_bindings()
        .any(|(key, owner)| *owner == run_id && matches!(key, crate::model::RunKey::Native { .. }))
        || model.agent_nodes().any(|node| {
            node.task_run_id == run_id
                && node
                    .native_session_id
                    .as_ref()
                    .is_some_and(|sid| !sid.is_empty())
        })
}

fn run_owns_native(shared: &SharedModel, run_id: RunId, provider: Provider, sid: &str) -> bool {
    let model = shared.borrow();
    model
        .task_run_by_key(&crate::model::RunKey::Native {
            provider,
            sid: sid.to_owned(),
        })
        .is_some_and(|run| run.run_id == run_id)
        || model.agent_nodes().any(|node| {
            node.task_run_id == run_id
                && node.provider == provider
                && node.native_session_id.as_deref() == Some(sid)
        })
}

fn append_execution_ends(
    shared: &SharedModel,
    session: &str,
    event_kind: &str,
    pane_id: Option<&str>,
    events: &mut Vec<NormalizedEvent>,
) {
    for execution in shared.borrow().executions().filter(|execution| {
        !execution.state.is_terminal() && pane_id == Some(execution.pane_id.as_str())
    }) {
        events.push(NormalizedEvent::ExecutionEnd {
            metadata: metadata(session, event_kind),
            execution_id: execution.execution_id.clone(),
        });
    }
}

fn apply_snapshot_in_place(
    reducer: &mut Reducer,
    shared: &SharedModel,
    topology: TopologySnapshot,
    session: &str,
    pending_closures: &mut PendingTopologyClosures,
) -> Result<PersistBatch, ReducerError> {
    let mut persist = Vec::new();
    let snapshot_workspace_ids: HashSet<_> = topology
        .workspaces
        .iter()
        .map(|workspace| workspace.workspace_id.clone())
        .collect();
    let snapshot_tab_ids: HashSet<_> = topology.tabs.iter().map(|tab| tab.tab_id.clone()).collect();
    let snapshot_pane_ids: HashSet<_> = topology
        .panes
        .iter()
        .map(|pane| pane.pane_id.clone())
        .collect();

    for workspace in &topology.workspaces {
        if let Some(batch) = apply_collector_event(
            reducer,
            topology_upsert(
                session,
                "snapshot_workspace",
                TopologyEntity::Workspace(workspace.clone()),
            ),
        )? {
            persist.extend(batch);
        }
    }
    for tab in &topology.tabs {
        if let Some(batch) = apply_collector_event(
            reducer,
            topology_upsert(session, "snapshot_tab", TopologyEntity::Tab(tab.clone())),
        )? {
            persist.extend(batch);
        }
    }
    for pane in &topology.panes {
        let mut observation = vec![topology_upsert(
            session,
            "snapshot_pane",
            TopologyEntity::Pane(Pane {
                pane_id: pane.pane_id.clone(),
                workspace_id: pane.workspace_id.clone(),
                tab_id: pane.tab_id.clone(),
                terminal_id: pane.terminal_id.clone(),
                display_name: pane.display_name.clone(),
            }),
        )];
        let provider = pane.agent.as_ref().and_then(|agent| {
            snapshot_provider_name(
                &agent.agent_name,
                pane.agent_session
                    .as_ref()
                    .map(|session| session.agent.as_str()),
            )
        });
        let incoming_sid = pane.agent_session.as_ref().and_then(|reference| {
            (reference.kind == AgentSessionReferenceKind::Id && !reference.value.is_empty())
                .then_some(reference.value.as_str())
        });
        let current: Vec<_> = shared
            .borrow()
            .executions()
            .filter(|execution| {
                execution.terminal_id == pane.terminal_id && !execution.state.is_terminal()
            })
            .cloned()
            .collect();
        let mut reused = false;
        if let Some(agent) = &pane.agent {
            for mut execution in current {
                let has_native = run_has_native_binding(shared, execution.task_run_id);
                let same_native = provider.zip(incoming_sid).is_some_and(|(provider, sid)| {
                    run_owns_native(shared, execution.task_run_id, provider, sid)
                });
                let should_reuse = incoming_sid.is_none() || same_native || !has_native;
                if should_reuse && !reused {
                    execution.pane_id.clone_from(&pane.pane_id);
                    execution.state = agent.state.clone();
                    observation.push(NormalizedEvent::ExecutionBegin {
                        metadata: snapshot_pane_metadata(session, "snapshot_execution", pane),
                        execution,
                    });
                    reused = true;
                } else {
                    observation.push(NormalizedEvent::ExecutionEnd {
                        metadata: metadata(session, "snapshot_different_session"),
                        execution_id: execution.execution_id,
                    });
                }
            }
            if !reused {
                observation.push(snapshot_execution_begin(session, pane, agent));
            }
        }
        if let Some(batch) = apply_collector_observation(reducer, observation)? {
            persist.extend(batch);
        }
    }

    let stale_ids: Vec<_> = shared
        .borrow()
        .executions()
        .filter(|execution| {
            !execution.state.is_terminal()
                && !topology
                    .panes
                    .iter()
                    .any(|pane| pane.terminal_id == execution.terminal_id && pane.agent.is_some())
        })
        .map(|execution| execution.execution_id.clone())
        .collect();
    for execution_id in stale_ids {
        if let Some(batch) = apply_collector_event(
            reducer,
            NormalizedEvent::AgentStatusChanged {
                metadata: metadata(session, "snapshot_execution_missing"),
                execution_id,
                state: ExecState::Stale { since_ms: 0 },
            },
        )? {
            persist.extend(batch);
        }
    }

    let old_panes: Vec<_> = shared
        .borrow()
        .panes()
        .filter(|pane| !snapshot_pane_ids.contains(&pane.pane_id))
        .map(|pane| pane.pane_id.clone())
        .collect();
    let old_tabs: Vec<_> = shared
        .borrow()
        .tabs()
        .filter(|tab| !snapshot_tab_ids.contains(&tab.tab_id))
        .map(|tab| tab.tab_id.clone())
        .collect();
    let old_workspaces: Vec<_> = shared
        .borrow()
        .workspaces()
        .filter(|workspace| !snapshot_workspace_ids.contains(&workspace.workspace_id))
        .map(|workspace| workspace.workspace_id.clone())
        .collect();
    pending_closures.panes = old_panes.iter().cloned().collect();
    pending_closures.tabs = old_tabs.iter().cloned().collect();
    pending_closures.workspaces = old_workspaces.iter().cloned().collect();
    for pane_id in old_panes {
        let in_grace = shared.borrow().executions().any(|execution| {
            execution.pane_id == pane_id && matches!(execution.state, ExecState::Stale { .. })
        });
        if !in_grace {
            if let Some(batch) = apply_collector_event(
                reducer,
                topology_closure(
                    session,
                    "snapshot_pane_missing",
                    TopologyEntityId::Pane {
                        pane_id: pane_id.clone(),
                    },
                ),
            )? {
                persist.extend(batch);
            }
            pending_closures.panes.remove(&pane_id);
        }
    }
    for tab_id in old_tabs {
        if !shared.borrow().panes().any(|pane| pane.tab_id == tab_id) {
            if let Some(batch) = apply_collector_event(
                reducer,
                topology_closure(
                    session,
                    "snapshot_tab_missing",
                    TopologyEntityId::Tab {
                        tab_id: tab_id.clone(),
                    },
                ),
            )? {
                persist.extend(batch);
            }
            pending_closures.tabs.remove(&tab_id);
        }
    }
    for workspace_id in old_workspaces {
        if !shared
            .borrow()
            .tabs()
            .any(|tab| tab.workspace_id == workspace_id)
        {
            if let Some(batch) = apply_collector_event(
                reducer,
                topology_closure(
                    session,
                    "snapshot_workspace_missing",
                    TopologyEntityId::Workspace {
                        workspace_id: workspace_id.clone(),
                    },
                ),
            )? {
                persist.extend(batch);
            }
            pending_closures.workspaces.remove(&workspace_id);
        }
    }
    Ok(persist)
}

fn apply_pending_topology_closures(
    reducer: &mut Reducer,
    shared: &SharedModel,
    session: &str,
    pending: &mut PendingTopologyClosures,
) -> Result<PersistBatch, ReducerError> {
    let mut persist = Vec::new();
    let pane_ids: Vec<_> = pending.panes.iter().cloned().collect();
    for pane_id in pane_ids {
        let has_live_execution = shared
            .borrow()
            .executions()
            .any(|execution| execution.pane_id == pane_id && !execution.state.is_terminal());
        if !has_live_execution {
            if shared.borrow().pane(&pane_id).is_some()
                && let Some(batch) = apply_collector_event(
                    reducer,
                    topology_closure(
                        session,
                        "stale_grace_expired_pane",
                        TopologyEntityId::Pane {
                            pane_id: pane_id.clone(),
                        },
                    ),
                )?
            {
                persist.extend(batch);
            }
            pending.panes.remove(&pane_id);
        }
    }

    let tab_ids: Vec<_> = pending.tabs.iter().cloned().collect();
    for tab_id in tab_ids {
        if !shared.borrow().panes().any(|pane| pane.tab_id == tab_id) {
            if shared.borrow().tab(&tab_id).is_some()
                && let Some(batch) = apply_collector_event(
                    reducer,
                    topology_closure(
                        session,
                        "stale_grace_expired_tab",
                        TopologyEntityId::Tab {
                            tab_id: tab_id.clone(),
                        },
                    ),
                )?
            {
                persist.extend(batch);
            }
            pending.tabs.remove(&tab_id);
        }
    }

    let workspace_ids: Vec<_> = pending.workspaces.iter().cloned().collect();
    for workspace_id in workspace_ids {
        if !shared
            .borrow()
            .tabs()
            .any(|tab| tab.workspace_id == workspace_id)
        {
            if shared.borrow().workspace(&workspace_id).is_some()
                && let Some(batch) = apply_collector_event(
                    reducer,
                    topology_closure(
                        session,
                        "stale_grace_expired_workspace",
                        TopologyEntityId::Workspace {
                            workspace_id: workspace_id.clone(),
                        },
                    ),
                )?
            {
                persist.extend(batch);
            }
            pending.workspaces.remove(&workspace_id);
        }
    }
    Ok(persist)
}

fn snapshot_provider_name(agent_name: &str, session_agent: Option<&str>) -> Option<Provider> {
    session_agent
        .and_then(provider_from_name)
        .or_else(|| provider_from_name(agent_name))
}

fn snapshot_pane_metadata(session: &str, kind: &str, pane: &PaneSnapshot) -> EventMetadata {
    let mut value = metadata(session, kind);
    value.workspace_id = Some(pane.workspace_id.clone());
    value.tab_id = Some(pane.tab_id.clone());
    value.pane_id = Some(pane.pane_id.clone());
    value.terminal_id = Some(pane.terminal_id.clone());
    value.provider = pane.agent.as_ref().and_then(|agent| {
        snapshot_provider_name(
            &agent.agent_name,
            pane.agent_session
                .as_ref()
                .map(|session| session.agent.as_str()),
        )
    });
    value.native_session_id = pane.agent_session.as_ref().and_then(|reference| {
        (reference.kind == AgentSessionReferenceKind::Id && !reference.value.is_empty())
            .then(|| reference.value.clone())
    });
    value
}

fn snapshot_execution_begin(
    session: &str,
    pane: &PaneSnapshot,
    agent: &SnapshotAgent,
) -> NormalizedEvent {
    NormalizedEvent::ExecutionBegin {
        metadata: snapshot_pane_metadata(session, "snapshot_execution_discovered", pane),
        execution: Execution {
            execution_id: format!("herdr-execution-{}", ulid::Ulid::new()),
            pane_id: pane.pane_id.clone(),
            terminal_id: pane.terminal_id.clone(),
            task_run_id: RunId::new(),
            state: agent.state.clone(),
        },
    }
}

fn topology_from_snapshot(snapshot: &Snapshot) -> Result<TopologySnapshot, CollectorError> {
    let workspaces = snapshot
        .workspaces
        .iter()
        .map(|workspace| Workspace {
            workspace_id: workspace.workspace_id.clone(),
        })
        .collect();
    let tabs = snapshot.tabs.iter().cloned().map(tab_entity).collect();
    let panes = snapshot
        .panes
        .iter()
        .map(|pane| {
            let tab_id = pane
                .tab_id
                .clone()
                .ok_or_else(|| CollectorError::MissingTabId {
                    pane_id: pane.pane_id.clone(),
                })?;
            Ok(PaneSnapshot {
                pane_id: pane.pane_id.clone(),
                workspace_id: pane.workspace_id.clone(),
                tab_id,
                terminal_id: pane.terminal_id.clone(),
                display_name: pane_display_name(pane),
                agent: pane.agent.as_ref().map(|agent| SnapshotAgent {
                    agent_name: agent.clone(),
                    state: status_from_str(pane.agent_status.as_deref()),
                }),
                agent_session: pane.agent_session.as_ref().map(agent_session_reference),
            })
        })
        .collect::<Result<Vec<_>, CollectorError>>()?;
    Ok(TopologySnapshot {
        workspaces,
        tabs,
        panes,
    })
}

fn subscriptions() -> Vec<Subscription> {
    [
        "workspace.created",
        "workspace.renamed",
        "workspace.closed",
        "workspace.focused",
        "tab.created",
        "tab.renamed",
        "tab.closed",
        "tab.focused",
        "pane.created",
        "pane.closed",
        "pane.updated",
        "pane.focused",
        "pane.moved",
        "pane.exited",
        "pane.agent_detected",
        // Pane-scoped status events ride the isolated enrichment connection. Keeping them off
        // this primary stream preserves its gap and convergence semantics.
        "layout.updated",
    ]
    .into_iter()
    .map(Subscription::new)
    .collect()
}

fn enrichment_subscriptions(pane_ids: &BTreeSet<String>) -> Vec<Subscription> {
    pane_ids
        .iter()
        .map(|pane_id| Subscription::for_pane("pane.agent_status_changed", pane_id))
        .collect()
}

fn topology_upsert(session: &str, kind: &str, entity: TopologyEntity) -> NormalizedEvent {
    NormalizedEvent::TopologyUpsert {
        metadata: metadata(session, kind),
        entity,
    }
}

fn topology_closure(session: &str, kind: &str, entity: TopologyEntityId) -> NormalizedEvent {
    NormalizedEvent::TopologyClosure {
        metadata: metadata(session, kind),
        entity,
    }
}

fn execution_begin(session: &str, kind: &str, pane: &PaneInfo) -> NormalizedEvent {
    NormalizedEvent::ExecutionBegin {
        metadata: pane_metadata(session, kind, pane),
        execution: Execution {
            execution_id: format!("herdr-execution-{}", ulid::Ulid::new()),
            pane_id: pane.pane_id.clone(),
            terminal_id: pane.terminal_id.clone(),
            task_run_id: RunId::new(),
            state: status_from_str(pane.agent_status.as_deref()),
        },
    }
}

fn pane_metadata(session: &str, kind: &str, pane: &PaneInfo) -> EventMetadata {
    let mut metadata = metadata(session, kind);
    metadata.workspace_id = Some(pane.workspace_id.clone());
    metadata.tab_id.clone_from(&pane.tab_id);
    metadata.pane_id = Some(pane.pane_id.clone());
    metadata.terminal_id = Some(pane.terminal_id.clone());
    metadata.provider = pane_provider(pane);
    metadata.native_session_id = pane.agent_session.as_ref().and_then(|reference| {
        (reference.kind == AgentSessionKind::Id && !reference.value.is_empty())
            .then(|| reference.value.clone())
    });
    metadata
}

fn metadata(session: &str, kind: &str) -> EventMetadata {
    let receipt_time_ms = unix_now_ms();
    EventMetadata {
        event_id: format!("herdr-event-{}", ulid::Ulid::new()),
        timestamp_ms: receipt_time_ms,
        receipt_time_ms,
        source: "herdr".to_owned(),
        source_event_type: kind.to_owned(),
        herdr_session: session.to_owned(),
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

struct NormalizedProviderObservation {
    events: Vec<NormalizedEvent>,
    identity_disagreement: bool,
}

fn normalize_provider_event(
    shared: &SharedModel,
    session: &str,
    event: ProviderEvent,
    coverage: &SourceCoverageRegistry,
) -> NormalizedProviderObservation {
    match event {
        ProviderEvent::SessionResolved {
            provider,
            agent_thread_id,
            owner_session_id,
            parent_thread_id,
            path,
            model_id,
            event_id,
            observed_at_ms,
            ..
        } => {
            let path_text = path.to_string_lossy().into_owned();
            let model = shared.borrow();
            let path_key = RunKey::NativePath {
                provider,
                path: path_text.clone(),
            };
            let path_run = model.task_run_by_key(&path_key).map(|run| run.run_id);
            let file_run = model
                .agent_nodes()
                .filter(|node| {
                    node.provider == provider
                        && node.session_file.as_deref() == Some(path_text.as_str())
                })
                .map(|node| node.task_run_id)
                .min();
            let owner_run = owner_session_id.as_deref().and_then(|sid| {
                model
                    .task_run_by_key(&RunKey::Native {
                        provider,
                        sid: sid.to_owned(),
                    })
                    .map(|run| run.run_id)
            });
            let Some(run_id) = path_run.or(file_run).or(owner_run) else {
                return NormalizedProviderObservation {
                    events: Vec::new(),
                    identity_disagreement: false,
                };
            };
            let identity_disagreement = owner_session_id.as_deref().is_some_and(|resolved_sid| {
                matches!(
                    model.task_run(&run_id).map(|run| &run.key),
                    Some(RunKey::Native { provider: current_provider, sid })
                        if *current_provider == provider && sid != resolved_sid
                )
            });
            drop(model);

            let node_id = deterministic_agent_node_id(provider, &agent_thread_id);
            let parent_node_id = parent_thread_id
                .as_deref()
                .map(|parent| deterministic_agent_node_id(provider, parent));
            let provider_metadata = MinimalProviderMetadata {
                agent_id: Some(agent_thread_id.clone()),
                parent_agent_id: parent_thread_id.clone(),
                model_id: model_id.clone(),
                event_kind: Some("session_resolved".to_owned()),
                ..MinimalProviderMetadata::default()
            };
            let mut events = Vec::new();

            let mut node_metadata = provider_metadata_for(
                session,
                provider,
                format!("prov:{}:node:{agent_thread_id}", provider_name(provider)),
                "agent_node",
                observed_at_ms,
                run_id,
                &node_id,
                provider_metadata.clone(),
                coverage,
            );
            node_metadata.native_session_id = None;
            events.push(NormalizedEvent::AgentNodeUpsert {
                metadata: node_metadata,
                node: AgentNodeObservation {
                    agent_node_id: node_id.clone(),
                    provider,
                    native_session_id: Some(agent_thread_id.clone()),
                    task_run_id: run_id,
                    parent_agent_node_id: None,
                    state: None,
                    model_id: None,
                    session_file: None,
                },
            });

            if let Some(parent_agent_node_id) = parent_node_id {
                let link_metadata = provider_metadata_for(
                    session,
                    provider,
                    format!(
                        "prov:{}:link:{agent_thread_id}:{}",
                        provider_name(provider),
                        parent_thread_id.as_deref().expect("parent ID exists")
                    ),
                    "agent_parent_link",
                    observed_at_ms,
                    run_id,
                    &node_id,
                    provider_metadata.clone(),
                    coverage,
                );
                events.push(NormalizedEvent::AgentNodeUpsert {
                    metadata: link_metadata,
                    node: AgentNodeObservation {
                        agent_node_id: node_id.clone(),
                        provider,
                        native_session_id: Some(agent_thread_id.clone()),
                        task_run_id: run_id,
                        parent_agent_node_id: Some(parent_agent_node_id),
                        state: None,
                        model_id: None,
                        session_file: None,
                    },
                });
            }

            let mut resolved_metadata = provider_metadata_for(
                session,
                provider,
                event_id,
                "session_resolved",
                observed_at_ms,
                run_id,
                &node_id,
                provider_metadata,
                coverage,
            );
            if !identity_disagreement {
                resolved_metadata.native_session_id = owner_session_id;
            }
            events.push(NormalizedEvent::AgentNodeUpsert {
                metadata: resolved_metadata,
                node: AgentNodeObservation {
                    agent_node_id: node_id,
                    provider,
                    native_session_id: Some(agent_thread_id),
                    task_run_id: run_id,
                    parent_agent_node_id: None,
                    state: None,
                    model_id,
                    session_file: Some(path_text),
                },
            });
            NormalizedProviderObservation {
                events,
                identity_disagreement,
            }
        }
        ProviderEvent::AgentUpsert {
            provider,
            agent_thread_id,
            owner_session_id,
            parent_thread_id,
            state,
            model_id,
            event_id,
            observed_at_ms,
            ..
        } => {
            let model = shared.borrow();
            let owner_run = owner_session_id.as_deref().and_then(|sid| {
                model
                    .task_run_by_key(&RunKey::Native {
                        provider,
                        sid: sid.to_owned(),
                    })
                    .map(|run| run.run_id)
            });
            let node_run = model
                .agent_nodes()
                .find(|node| {
                    node.provider == provider
                        && node.native_session_id.as_deref() == Some(agent_thread_id.as_str())
                })
                .map(|node| node.task_run_id);
            let Some(run_id) = owner_run.or(node_run) else {
                return NormalizedProviderObservation {
                    events: Vec::new(),
                    identity_disagreement: false,
                };
            };
            drop(model);
            let node_id = deterministic_agent_node_id(provider, &agent_thread_id);
            let parent_agent_node_id = parent_thread_id
                .as_deref()
                .map(|parent| deterministic_agent_node_id(provider, parent));
            let provider_metadata = MinimalProviderMetadata {
                agent_id: Some(agent_thread_id.clone()),
                parent_agent_id: parent_thread_id,
                model_id: model_id.clone(),
                event_kind: Some("agent_upsert".to_owned()),
                ..MinimalProviderMetadata::default()
            };
            let metadata = provider_metadata_for(
                session,
                provider,
                event_id,
                "agent_upsert",
                observed_at_ms,
                run_id,
                &node_id,
                provider_metadata,
                coverage,
            );
            NormalizedProviderObservation {
                events: vec![NormalizedEvent::AgentNodeUpsert {
                    metadata,
                    node: AgentNodeObservation {
                        agent_node_id: node_id,
                        provider,
                        native_session_id: Some(agent_thread_id),
                        task_run_id: run_id,
                        parent_agent_node_id,
                        state,
                        model_id,
                        session_file: None,
                    },
                }],
                identity_disagreement: false,
            }
        }
        ProviderEvent::Activity {
            provider,
            agent_thread_id,
            activity,
            event_id,
            observed_at_ms,
            ..
        } => {
            let model = shared.borrow();
            let lane_activity =
                activity.event_kind.as_deref() == Some(crate::provider::lane::LIVE_LINE_EVENT_KIND);
            let resolved = if lane_activity {
                let lane_key = match provider {
                    Provider::Claude => RunKey::Controller(agent_thread_id.clone()),
                    Provider::Codex => RunKey::Native {
                        provider,
                        sid: agent_thread_id.clone(),
                    },
                };
                model.task_run_by_key(&lane_key).map(|run| {
                    let node_id =
                        deterministic_agent_node_id(provider, &format!("lane:{agent_thread_id}"));
                    let create_node = model.agent_node(&node_id).is_none();
                    (run.run_id, node_id, create_node)
                })
            } else {
                model
                    .agent_nodes()
                    .filter(|node| {
                        node.provider == provider
                            && node.native_session_id.as_deref() == Some(agent_thread_id.as_str())
                    })
                    .min_by_key(|node| node.agent_node_id.as_str())
                    .cloned()
                    .map(|node| (node.task_run_id, node.agent_node_id, false))
            };
            let Some((run_id, node_id, create_node)) = resolved else {
                return NormalizedProviderObservation {
                    events: Vec::new(),
                    identity_disagreement: false,
                };
            };
            drop(model);
            let mut events = Vec::new();
            if create_node {
                let node_metadata = provider_metadata_for(
                    session,
                    provider,
                    format!("{event_id}:node"),
                    "agent_node",
                    observed_at_ms,
                    run_id,
                    &node_id,
                    activity.clone(),
                    coverage,
                );
                events.push(NormalizedEvent::AgentNodeUpsert {
                    metadata: node_metadata,
                    node: AgentNodeObservation {
                        agent_node_id: node_id.clone(),
                        provider,
                        native_session_id: None,
                        task_run_id: run_id,
                        parent_agent_node_id: None,
                        state: None,
                        model_id: None,
                        session_file: None,
                    },
                });
            }
            let metadata = provider_metadata_for(
                session,
                provider,
                event_id,
                "activity",
                observed_at_ms,
                run_id,
                &node_id,
                activity.clone(),
                coverage,
            );
            events.push(NormalizedEvent::AgentActivity {
                metadata,
                agent_node_id: node_id,
                activity,
            });
            NormalizedProviderObservation {
                events,
                identity_disagreement: false,
            }
        }
        ProviderEvent::Synthesized(_)
        | ProviderEvent::RunLiveness { .. }
        | ProviderEvent::LaneClose { .. }
        | ProviderEvent::Telemetry { .. }
        | ProviderEvent::SourceState { .. }
        | ProviderEvent::Malformed { .. } => NormalizedProviderObservation {
            events: Vec::new(),
            identity_disagreement: false,
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn provider_metadata_for(
    session: &str,
    provider: Provider,
    event_id: String,
    kind: &str,
    observed_at_ms: i64,
    run_id: RunId,
    agent_node_id: &str,
    provider_metadata: MinimalProviderMetadata,
    coverage: &SourceCoverageRegistry,
) -> EventMetadata {
    EventMetadata {
        event_id,
        timestamp_ms: observed_at_ms,
        receipt_time_ms: unix_now_ms(),
        source: "provider".to_owned(),
        source_event_type: kind.to_owned(),
        herdr_session: session.to_owned(),
        workspace_id: None,
        tab_id: None,
        pane_id: None,
        terminal_id: None,
        provider: Some(provider),
        native_session_id: None,
        task_run_id: Some(run_id),
        agent_node_id: Some(agent_node_id.to_owned()),
        task_state: None,
        execution_parent: None,
        dependency: None,
        source_coverage: coverage.provider_metadata(),
        provider_metadata: Some(provider_metadata),
        label: None,
        reason: None,
        progress: None,
        ingest_seq: None,
    }
}

fn normalized_metadata(event: &NormalizedEvent) -> &EventMetadata {
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

fn deterministic_agent_node_id(provider: Provider, thread_id: &str) -> String {
    format!("agent:{}:{thread_id}", provider_name(provider))
}

const fn provider_name(provider: Provider) -> &'static str {
    match provider {
        Provider::Claude => "claude",
        Provider::Codex => "codex",
    }
}

fn pane_entity(pane: &PaneInfo) -> Result<Pane, CollectorError> {
    Ok(Pane {
        pane_id: pane.pane_id.clone(),
        workspace_id: pane.workspace_id.clone(),
        tab_id: pane
            .tab_id
            .clone()
            .ok_or_else(|| CollectorError::MissingTabId {
                pane_id: pane.pane_id.clone(),
            })?,
        terminal_id: pane.terminal_id.clone(),
        display_name: pane_display_name(pane),
    })
}

fn tab_entity(tab: TabInfo) -> Tab {
    Tab {
        tab_id: tab.tab_id,
        workspace_id: tab.workspace_id,
        label: tab
            .label
            .as_deref()
            .filter(|label| !label.is_empty())
            .and_then(sanitized_name),
    }
}

fn pane_display_name(pane: &PaneInfo) -> Option<String> {
    pane.label
        .as_deref()
        .filter(|label| !label.is_empty())
        .or_else(|| {
            pane.terminal_title_stripped
                .as_deref()
                .filter(|title| !title.is_empty())
        })
        .and_then(sanitized_name)
}

fn sanitized_name(value: &str) -> Option<String> {
    let value = sanitize_controller_text(value);
    (!value.is_empty()).then_some(value)
}

fn agent_session_reference(reference: &super::types::AgentSessionInfo) -> AgentSessionReference {
    AgentSessionReference {
        source: reference.source.clone(),
        agent: reference.agent.clone(),
        kind: match reference.kind {
            AgentSessionKind::Id => AgentSessionReferenceKind::Id,
            AgentSessionKind::Path => AgentSessionReferenceKind::Path,
        },
        value: reference.value.clone(),
    }
}

fn pane_provider(pane: &PaneInfo) -> Option<Provider> {
    pane.agent_session
        .as_ref()
        .and_then(|reference| provider_from_name(&reference.agent))
        .or_else(|| pane.agent.as_deref().and_then(provider_from_name))
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

fn status_from_value(value: Option<&Value>) -> ExecState {
    status_from_str(value.and_then(Value::as_str))
}

fn status_from_str(value: Option<&str>) -> ExecState {
    match value {
        Some("idle" | "done") => ExecState::Idle,
        Some("working") => ExecState::Working,
        Some("blocked") => ExecState::Blocked,
        _ => ExecState::Unknown,
    }
}

fn snapshot_entity_keys(snapshot: &Snapshot) -> HashSet<EntityKey> {
    snapshot
        .workspaces
        .iter()
        .map(|workspace| EntityKey::Workspace(workspace.workspace_id.clone()))
        .chain(
            snapshot
                .tabs
                .iter()
                .map(|tab| EntityKey::Tab(tab.tab_id.clone())),
        )
        .chain(
            snapshot
                .panes
                .iter()
                .map(|pane| EntityKey::Pane(pane.pane_id.clone())),
        )
        .collect()
}

fn record_replay_facts(
    index: usize,
    received: &ReceivedEvent,
    created: &mut HashSet<EntityKey>,
    closures: &mut HashMap<EntityKey, Vec<usize>>,
    candidates: &mut Vec<(usize, EntityKey)>,
) {
    for entity in created_entities(received) {
        created.insert(entity);
    }
    for entity in closed_entities(received) {
        closures.entry(entity).or_default().push(index);
    }
    if let Some(entity) = updated_entity(received) {
        candidates.push((index, entity));
    }
}

fn created_entities(received: &ReceivedEvent) -> Vec<EntityKey> {
    let mut entities = Vec::new();
    match received.event.as_str() {
        "workspace_created" => {
            nested_string(&received.data, "workspace", "workspace_id")
                .map(EntityKey::Workspace)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        "tab_created" => {
            nested_string(&received.data, "tab", "tab_id")
                .map(EntityKey::Tab)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        "pane_created" => {
            nested_string(&received.data, "pane", "pane_id")
                .map(EntityKey::Pane)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        "pane_moved" => {
            nested_string(&received.data, "created_tab", "tab_id")
                .map(EntityKey::Tab)
                .into_iter()
                .for_each(|entity| entities.push(entity));
            nested_string(&received.data, "pane", "pane_id")
                .map(EntityKey::Pane)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        _ => {}
    }
    entities
}

fn closed_entities(received: &ReceivedEvent) -> Vec<EntityKey> {
    let mut entities = Vec::new();
    match received.event.as_str() {
        "workspace_closed" => string_field(&received.data, "workspace_id")
            .map(EntityKey::Workspace)
            .into_iter()
            .for_each(|entity| entities.push(entity)),
        "tab_closed" => string_field(&received.data, "tab_id")
            .map(EntityKey::Tab)
            .into_iter()
            .for_each(|entity| entities.push(entity)),
        "pane_closed" | "pane_exited" => string_field(&received.data, "pane_id")
            .map(EntityKey::Pane)
            .into_iter()
            .for_each(|entity| entities.push(entity)),
        "pane_moved" => {
            string_field(&received.data, "previous_pane_id")
                .map(EntityKey::Pane)
                .into_iter()
                .for_each(|entity| entities.push(entity));
            string_field(&received.data, "closed_tab_id")
                .map(EntityKey::Tab)
                .into_iter()
                .for_each(|entity| entities.push(entity));
            string_field(&received.data, "closed_workspace_id")
                .map(EntityKey::Workspace)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        _ => {}
    }
    entities
}

fn updated_entity(received: &ReceivedEvent) -> Option<EntityKey> {
    match received.event.as_str() {
        "workspace_renamed" => nested_string(&received.data, "workspace", "workspace_id")
            .or_else(|| string_field(&received.data, "workspace_id"))
            .map(EntityKey::Workspace),
        "workspace_focused" => {
            string_field(&received.data, "workspace_id").map(EntityKey::Workspace)
        }
        "tab_renamed" => string_field(&received.data, "tab_id").map(EntityKey::Tab),
        "tab_focused" => string_field(&received.data, "tab_id").map(EntityKey::Tab),
        "pane_updated" | "pane_agent_detected" => nested_string(&received.data, "pane", "pane_id")
            .or_else(|| string_field(&received.data, "pane_id"))
            .map(EntityKey::Pane),
        "pane_focused" | "pane_agent_status_changed" => string_field(&received.data, "pane_id")
            .or_else(|| nested_string(&received.data, "pane", "pane_id"))
            .map(EntityKey::Pane),
        _ => None,
    }
}

fn entity_exists(shared: &SharedModel, entity: &EntityKey) -> bool {
    let model = shared.borrow();
    match entity {
        EntityKey::Workspace(workspace_id) => model.workspace(workspace_id).is_some(),
        EntityKey::Tab(tab_id) => model.tab(tab_id).is_some(),
        EntityKey::Pane(pane_id) => model.pane(pane_id).is_some(),
    }
}

fn nested_string(value: &Value, object: &str, field: &str) -> Option<String> {
    value
        .get(object)
        .and_then(|nested| string_field(nested, field))
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn decode_wire<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, CollectorError> {
    serde_json::from_value(value.clone())
        .map_err(|error| CollectorError::Wire(WireError::MalformedFrame(error.to_string())))
}

struct OwnerTracker {
    terminal_id: Option<String>,
    pane_id: Option<String>,
    started_at_ms: i64,
}

impl OwnerTracker {
    fn from_environment() -> Self {
        Self {
            terminal_id: nonempty_env("HERDR_TERMINAL_ID"),
            pane_id: nonempty_env("HERDR_PANE_ID"),
            started_at_ms: unix_now_ms(),
        }
    }

    fn record(&self) -> OwnerRecord {
        OwnerRecord {
            pid: std::process::id(),
            started_at_ms: self.started_at_ms,
            terminal_id: self.terminal_id.clone(),
            pane_id: self.pane_id.clone(),
        }
    }

    async fn refresh_from_snapshot(
        &mut self,
        snapshot: &Snapshot,
        persistence: &mut RuntimePersistence,
    ) -> Result<(), WriterError> {
        let pane =
            self.terminal_id
                .as_deref()
                .and_then(|terminal_id| {
                    snapshot
                        .panes
                        .iter()
                        .find(|pane| pane.terminal_id == terminal_id)
                })
                .or_else(|| {
                    self.pane_id.as_deref().and_then(|pane_id| {
                        snapshot.panes.iter().find(|pane| pane.pane_id == pane_id)
                    })
                })
                .or_else(|| {
                    snapshot.focused_pane_id.as_deref().and_then(|pane_id| {
                        snapshot.panes.iter().find(|pane| pane.pane_id == pane_id)
                    })
                });
        if let Some(pane) = pane {
            self.update(&pane.terminal_id, &pane.pane_id, persistence)
                .await?;
        }
        Ok(())
    }

    async fn refresh_from_move(
        &mut self,
        data: &Value,
        persistence: &mut RuntimePersistence,
    ) -> Result<(), WriterError> {
        let Some(pane) = data.get("pane") else {
            return Ok(());
        };
        let Some(terminal_id) = string_field(pane, "terminal_id") else {
            return Ok(());
        };
        let Some(pane_id) = string_field(pane, "pane_id") else {
            return Ok(());
        };
        let previous_pane_id = string_field(data, "previous_pane_id");
        if self.terminal_id.as_deref() == Some(terminal_id.as_str())
            || self.pane_id.as_deref() == previous_pane_id.as_deref()
        {
            self.update(&terminal_id, &pane_id, persistence).await?;
        }
        Ok(())
    }

    async fn update(
        &mut self,
        terminal_id: &str,
        pane_id: &str,
        persistence: &mut RuntimePersistence,
    ) -> Result<(), WriterError> {
        let location_changed = self.terminal_id.as_deref() != Some(terminal_id)
            || self.pane_id.as_deref() != Some(pane_id);
        if location_changed
            && persistence
                .update_owner_location(terminal_id, pane_id)
                .await?
                == RuntimeWriteOutcome::Durable
        {
            self.terminal_id = Some(terminal_id.to_owned());
            self.pane_id = Some(pane_id.to_owned());
        }
        Ok(())
    }
}

fn nonempty_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

#[cfg(unix)]
fn socket_identity(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;

    std::fs::metadata(path)
        .ok()
        .map(|metadata| (metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn socket_identity(_path: &Path) -> Option<(u64, u64)> {
    None
}

async fn receive_controller(
    receiver: &mut Option<ControllerRequestReceiver>,
) -> ControllerRuntimeEvent {
    match receiver {
        Some(receiver) => receiver.recv_runtime_event().await,
        None => pending().await,
    }
}

async fn receive_operator_command(
    receiver: &mut Option<mpsc::Receiver<OperatorCommand>>,
) -> Option<OperatorCommand> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => pending().await,
    }
}

async fn service_operator_command(
    command: Option<OperatorCommand>,
    receiver: &mut Option<mpsc::Receiver<OperatorCommand>>,
    reducer: &mut Reducer,
    persistence: &mut RuntimePersistence,
    shared: &SharedModel,
    coverage: &SourceCoverageRegistry,
) -> Result<bool, CollectorError> {
    let Some(command) = command else {
        *receiver = None;
        return Ok(false);
    };
    let persist = reducer.apply_operator_command(command, unix_now_ms());
    let changed = !persist.is_empty();
    if changed {
        let _ = persist_submission(persistence, reducer, persist).await?;
    }
    persistence.refresh_snapshot(&shared.borrow(), coverage);
    Ok(changed)
}

async fn receive_provider(
    receiver: &mut Option<mpsc::Receiver<ProviderIngressEvent>>,
) -> Option<ProviderIngressEvent> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => pending().await,
    }
}

async fn drain_provider_events(
    provider: &mut ProviderIntegration,
    session: &str,
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
) -> Result<(), CollectorError> {
    while provider.events.is_some() {
        let event = receive_provider(&mut provider.events).await;
        service_provider_event(event, provider, session, reducer, shared, persistence).await?;
    }
    Ok(())
}

async fn service_provider_event(
    event: Option<ProviderIngressEvent>,
    provider: &mut ProviderIntegration,
    session: &str,
    reducer: &mut Reducer,
    shared: &SharedModel,
    persistence: &mut RuntimePersistence,
) -> Result<(), CollectorError> {
    let result = match event {
        Some(ProviderIngressEvent {
            event:
                ProviderEvent::SourceState {
                    provider: source,
                    state,
                },
            admission,
        }) => {
            assert!(
                admission.is_none(),
                "provider source-state controls must not carry performance admissions"
            );
            provider.update_source_state(source, state);
            Ok(())
        }
        Some(ProviderIngressEvent {
            event:
                ProviderEvent::Malformed {
                    provider: source,
                    path_display: _,
                    generation: _,
                    byte_offset,
                    error_code,
                },
            admission,
        }) => {
            assert!(
                admission.is_none(),
                "provider malformed controls must not carry performance admissions"
            );
            tracing::warn!(
                warning_code = "provider_record_malformed",
                provider = provider_name(source),
                byte_offset,
                error_code,
                "malformed provider record"
            );
            Ok(())
        }
        Some(ProviderIngressEvent { event, admission }) => {
            let admission = admission
                .expect("reducer-bound provider events must carry a performance admission");
            let coverage = provider.coverage.registry.clone();
            apply_provider_event_with_admission(
                event,
                Some(admission),
                session,
                reducer,
                shared,
                persistence,
                &coverage,
            )
            .await
        }
        None => {
            provider.events = None;
            provider.acknowledge_events_drained();
            provider.coverage.mark_egress_closed();
            Ok(())
        }
    };
    persistence.refresh_snapshot(&shared.borrow(), &provider.coverage.registry);
    result
}

async fn service_controller(
    event: ControllerRuntimeEvent,
    receiver: &mut Option<ControllerRequestReceiver>,
    session: &str,
    reducer: &mut Reducer,
    persistence: &mut RuntimePersistence,
    shared: &SharedModel,
    coverage: &SourceCoverageRegistry,
) {
    match event {
        ControllerRuntimeEvent::Request(Some(request)) => {
            controller::service_request(request, session, reducer, persistence).await;
            persistence.refresh_snapshot(&shared.borrow(), coverage);
        }
        ControllerRuntimeEvent::Request(None) => {
            *receiver = None;
            persistence.mark_acceptor_stopped();
        }
        ControllerRuntimeEvent::DiagnosticsChanged => persistence.publish(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_or_service_controller(
    cancellation: &CancellationToken,
    duration: Duration,
    receiver: &mut Option<ControllerRequestReceiver>,
    operator_commands: &mut Option<mpsc::Receiver<OperatorCommand>>,
    session: &str,
    reducer: &mut Reducer,
    persistence: &mut RuntimePersistence,
    shared: &SharedModel,
    provider: &mut ProviderIntegration,
) -> Result<bool, CollectorError> {
    let delay = tokio::time::sleep(duration);
    tokio::pin!(delay);
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return Ok(true),
            () = &mut delay => return Ok(false),
            request = receive_controller(receiver) => {
                service_controller(
                    request,
                    receiver,
                    session,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await;
                provider.publish_targets(shared);
            }
            command = receive_operator_command(operator_commands) => {
                if service_operator_command(
                    command,
                    operator_commands,
                    reducer,
                    persistence,
                    shared,
                    &provider.coverage.registry,
                ).await? {
                    provider.publish_targets(shared);
                }
            }
            event = receive_provider(&mut provider.events) => {
                service_provider_event(
                    event,
                    provider,
                    session,
                    reducer,
                    shared,
                    persistence,
                ).await?;
                provider.publish_targets(shared);
            }
        }
    }
}

fn unix_now_ms() -> i64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::io;
    #[cfg(feature = "workload-harness")]
    use std::sync::Barrier;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn watchdog_backoff_uses_exponential_delays_with_a_hard_cap() {
        let expected = [
            1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000, 60_000,
        ];
        for (consecutive_failures, expected_ms) in expected.into_iter().enumerate() {
            assert_eq!(
                backoff_delay_ms(u32::try_from(consecutive_failures).unwrap()),
                expected_ms
            );
        }
        assert_eq!(backoff_delay_ms(u32::MAX), 60_000);
    }

    #[test]
    fn watchdog_silence_deadline_uses_monotonic_time() {
        let policy = LivenessPolicy { timeout_ms: 30_000 };
        let last_event_at = Instant::now();

        assert_eq!(
            silence_deadline(last_event_at, &policy).duration_since(last_event_at),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn watchdog_backoff_resets_after_the_first_reconnected_event() {
        let mut backoff = ReconnectBackoff::default();

        assert_eq!(backoff.on_watchdog_silence(), 1_000);
        assert_eq!(backoff.consecutive_failures, 1);
        assert_eq!(backoff.on_watchdog_silence(), 2_000);
        assert_eq!(backoff.consecutive_failures, 2);

        backoff.on_event();
        assert_eq!(backoff.consecutive_failures, 0);
        assert_eq!(backoff.on_watchdog_silence(), 1_000);
        assert_eq!(backoff.consecutive_failures, 1);
    }

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    use tracing::instrument::WithSubscriber;

    use super::*;
    use crate::activity::OperatorSnapshot;
    use crate::diagnostics::{OccurrenceLogStatus, RuntimeWriteOutcome};
    use crate::model::{
        AgentNode, DependencyEdge, DisplayOrdinal, ExecutionEdge, OperatorCommand, TaskRun,
        TaskState, Workspace,
    };
    use crate::performance::{
        PerformanceDegradationReason, PerformanceSnapshot, TestPerformanceClock,
        performance_tracker,
    };
    use crate::store::writer::{
        DurabilityDisposition, PersistenceFailure, PersistenceFailureCode, PersistenceOperation,
        PersistencePhase,
    };
    use crate::store::{PersistExecution, PersistTaskRun, open_writer, spawn_writer};

    #[derive(Default)]
    struct RecordingOccurrenceSink {
        attempts: AtomicUsize,
        bytes: Mutex<Vec<u8>>,
        fail: bool,
    }

    impl PersistenceOccurrenceSink for RecordingOccurrenceSink {
        fn append(&self, record: &[u8]) -> io::Result<()> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            if self.fail {
                return Err(io::Error::other("injected append failure"));
            }
            self.bytes.lock().unwrap().extend_from_slice(record);
            Ok(())
        }
    }

    fn failure(
        operation: PersistenceOperation,
        phase: PersistencePhase,
        code: PersistenceFailureCode,
        durability: DurabilityDisposition,
    ) -> PersistenceFailure {
        PersistenceFailure {
            operation,
            phase,
            code,
            durability,
        }
    }

    fn runtime_with_sink(
        sink: Arc<RecordingOccurrenceSink>,
    ) -> (
        tempfile::TempDir,
        crate::store::WriterLifecycle,
        RuntimePersistence,
        watch::Receiver<crate::diagnostics::RuntimeDiagnosticsSnapshot>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (runtime, diagnostics) = RuntimePersistence::new_for_test(writer, sink);
        (directory, lifecycle, runtime, diagnostics)
    }

    async fn shutdown_writer(lifecycle: crate::store::WriterLifecycle) {
        tokio::time::timeout(STOP_TIMEOUT, lifecycle.shutdown())
            .await
            .expect("writer shutdown timed out")
            .unwrap();
    }

    async fn wait_for_attempts(attempts: &AtomicUsize, expected: usize, context: &str) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while attempts.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{context}: observed {} attempts, expected at least {expected}",
                attempts.load(Ordering::Acquire)
            )
        });
    }

    async fn wait_for_quality(
        quality: &mut watch::Receiver<ObservationQuality>,
        expected: ObservationQuality,
        context: &str,
    ) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if *quality.borrow_and_update() == expected {
                    return;
                }
                quality
                    .changed()
                    .await
                    .expect("collector quality publisher closed before the expected transition");
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{context}: quality did not become {expected:?}"));
    }

    async fn accept_wire_request(
        listener: &tokio::net::UnixListener,
    ) -> (tokio::io::BufReader<tokio::net::UnixStream>, Value) {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .expect("fake Herdr listener timed out waiting for a connection")
            .expect("fake Herdr listener failed to accept a connection");
        let mut reader = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line))
            .await
            .expect("fake Herdr listener timed out reading a request")
            .expect("fake Herdr listener failed to read a request");
        let request = serde_json::from_str(&line).expect("collector sent malformed request JSON");
        (reader, request)
    }

    async fn write_wire_frame(
        reader: &mut tokio::io::BufReader<tokio::net::UnixStream>,
        frame: &Value,
    ) {
        let mut bytes = serde_json::to_vec(frame).expect("fake Herdr response did not serialize");
        bytes.push(b'\n');
        tokio::time::timeout(Duration::from_secs(3), reader.get_mut().write_all(&bytes))
            .await
            .expect("fake Herdr listener timed out writing a response")
            .expect("fake Herdr listener failed to write a response");
    }

    async fn write_primary_overflow_burst(
        reader: &mut tokio::io::BufReader<tokio::net::UnixStream>,
    ) {
        let mut frame = serde_json::to_vec(&json!({
            "event": "pane_focused",
            "data": {"pane_id": "w1:p4"},
        }))
        .expect("overflow event did not serialize");
        frame.push(b'\n');
        let mut burst = Vec::with_capacity(frame.len() * EVENT_QUEUE_CAPACITY * 2);
        for _ in 0..EVENT_QUEUE_CAPACITY * 2 {
            burst.extend_from_slice(&frame);
        }
        tokio::time::timeout(Duration::from_secs(3), reader.get_mut().write_all(&burst))
            .await
            .expect("fake Herdr listener timed out writing an overflow burst")
            .expect("fake Herdr listener failed to write an overflow burst");
    }

    async fn wait_for_wire_peer_close(reader: &mut tokio::io::BufReader<tokio::net::UnixStream>) {
        let mut line = String::new();
        let bytes_read = tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line))
            .await
            .expect("fake Herdr listener timed out waiting for peer close")
            .expect("fake Herdr listener failed while waiting for peer close");
        assert_eq!(
            bytes_read, 0,
            "collector sent an unexpected second request frame"
        );
    }

    async fn write_malformed_wire_event(reader: &mut tokio::io::BufReader<tokio::net::UnixStream>) {
        tokio::time::timeout(
            Duration::from_secs(3),
            reader.get_mut().write_all(b"not-json\n"),
        )
        .await
        .expect("fake Herdr listener timed out writing a malformed event")
        .expect("fake Herdr listener failed to write a malformed event");
    }

    async fn wait_for_server_cancellation(cancellation: &CancellationToken) {
        tokio::time::timeout(Duration::from_secs(3), cancellation.cancelled())
            .await
            .expect("fake Herdr server did not receive cancellation");
    }

    async fn join_fake_server(server: JoinHandle<()>, context: &str) {
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap_or_else(|_| panic!("{context}: fake Herdr server did not stop"))
            .expect("fake Herdr server task panicked");
    }

    struct PrimaryCollectorHarness {
        cancellation: CancellationToken,
        task: JoinHandle<Result<(), CollectorError>>,
        model: SharedModel,
        primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle,
        enrichment_diagnostics: EnrichmentDiagnosticsHandle,
        provider_events: mpsc::Sender<ProviderIngressEvent>,
        ignored_provider_events: mpsc::Receiver<ProviderEvent>,
        provider_thread: ProviderThreadHandle,
        lifecycle: crate::store::WriterLifecycle,
        source_quality: watch::Receiver<ObservationQuality>,
        log_path: PathBuf,
    }

    impl PrimaryCollectorHarness {
        async fn stop(self) -> String {
            let Self {
                cancellation,
                task,
                model: _,
                primary_stream_diagnostics: _,
                enrichment_diagnostics: _,
                provider_events,
                ignored_provider_events,
                provider_thread,
                lifecycle,
                source_quality: _,
                log_path,
            } = self;
            cancellation.cancel();
            drop(provider_events);
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .expect("primary collector did not stop after cancellation")
                .expect("primary collector task panicked")
                .expect("primary collector returned an error");
            tokio::time::timeout(Duration::from_secs(3), provider_thread.stop())
                .await
                .expect("provider thread did not stop")
                .unwrap();
            drop(ignored_provider_events);
            shutdown_writer(lifecycle).await;
            std::fs::read_to_string(log_path).expect("failed to read primary collector log")
        }
    }

    fn spawn_primary_collector_harness(
        directory: &tempfile::TempDir,
        socket: PathBuf,
        log_name: &str,
    ) -> PrimaryCollectorHarness {
        spawn_primary_collector_harness_with_policy(
            directory,
            socket,
            log_name,
            LivenessPolicy::default(),
        )
    }

    fn spawn_primary_collector_harness_with_policy(
        directory: &tempfile::TempDir,
        socket: PathBuf,
        log_name: &str,
        liveness_policy: LivenessPolicy,
    ) -> PrimaryCollectorHarness {
        let log_path = directory.path().join(log_name);
        let log = std::fs::File::create(&log_path).unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::WARN)
            .with_writer(log)
            .finish();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let restored = store.load_restored_state().unwrap();
        let owner = OwnerTracker::from_environment();
        store.replace_owner(&owner.record()).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (reducer, shared) = Reducer::new(restored);
        let initial_coverage = SourceCoverageRegistry::new(SourceAvailability::NotApplicable);
        let (coverage_sender, _source_coverage) = watch::channel(initial_coverage.clone());
        let (source_quality_sender, source_quality) =
            watch::channel(ObservationQuality::Reconciling);
        let coverage = CoverageTracker::new(
            SourceAvailability::NotApplicable,
            coverage_sender,
            source_quality_sender,
        );
        let (persistence, _diagnostics) = RuntimePersistence::new(
            writer,
            &shared.borrow(),
            &initial_coverage,
            Arc::new(RecordingOccurrenceSink::default()),
        );
        let enrichment_diagnostics = persistence.enrichment_diagnostics();

        let provider_diagnostics = crate::provider::ProviderDiagnostics::default();
        let (ignored_events, ignored_provider_events) = mpsc::channel(1);
        let provider_thread = spawn_provider_thread_with_diagnostics(
            AdapterProviderWorker::new(Vec::new(), provider_diagnostics.clone()),
            ignored_events,
            None,
            provider_diagnostics,
        )
        .unwrap();
        let (provider_events, provider_receiver) = mpsc::channel(1);
        let provider = ProviderIntegration::new(
            provider_receiver,
            provider_thread.target_publisher(),
            TargetSet::default(),
            coverage,
        );
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let (performance, _sampler) =
            performance_tracker(Arc::new(TestPerformanceClock::new(Duration::ZERO)));
        let task_model = shared.clone();
        let primary_stream_diagnostics = PrimaryStreamDiagnosticsHandle::default();
        let task_primary_stream_diagnostics = primary_stream_diagnostics.clone();
        let task = tokio::spawn(
            run_collector(
                socket,
                "health-edge-session".to_owned(),
                persistence,
                reducer,
                task_model,
                performance,
                task_cancellation,
                owner,
                None,
                None,
                provider,
                liveness_policy,
                task_primary_stream_diagnostics,
            )
            .with_subscriber(subscriber),
        );

        PrimaryCollectorHarness {
            cancellation,
            task,
            model: shared,
            primary_stream_diagnostics,
            enrichment_diagnostics,
            provider_events,
            ignored_provider_events,
            provider_thread,
            lifecycle,
            source_quality,
            log_path,
        }
    }

    struct EnrichmentReaderHarness {
        cancellation: CancellationToken,
        task: JoinHandle<()>,
        targets: watch::Sender<BTreeSet<String>>,
        events: mpsc::Receiver<EnrichmentPayload>,
        prunes: mpsc::UnboundedReceiver<EnrichmentPrune>,
        log_path: PathBuf,
    }

    impl EnrichmentReaderHarness {
        async fn stop(self) -> String {
            let Self {
                cancellation,
                task,
                targets,
                events,
                prunes,
                log_path,
            } = self;
            cancellation.cancel();
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .expect("enrichment reader did not stop after cancellation")
                .expect("enrichment reader task panicked");
            drop(targets);
            drop(events);
            drop(prunes);
            std::fs::read_to_string(log_path).expect("failed to read enrichment reader log")
        }
    }

    fn spawn_enrichment_reader_harness(
        directory: &tempfile::TempDir,
        socket: PathBuf,
        log_name: &str,
    ) -> EnrichmentReaderHarness {
        let log_path = directory.path().join(log_name);
        let log = std::fs::File::create(&log_path).unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::WARN)
            .with_writer(log)
            .finish();
        let (targets, target_receiver) = watch::channel(BTreeSet::from(["pane-1".to_owned()]));
        let (sender, events) = mpsc::channel(ENRICHMENT_QUEUE_CAPACITY);
        let (prune_sender, prunes) = mpsc::unbounded_channel();
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(
            run_enrichment_reader(
                socket,
                target_receiver,
                sender,
                prune_sender,
                EnrichmentDiagnosticsHandle::default(),
                Arc::new(EnrichmentHealth::default()),
                task_cancellation,
            )
            .with_subscriber(subscriber),
        );
        EnrichmentReaderHarness {
            cancellation,
            task,
            targets,
            events,
            prunes,
            log_path,
        }
    }

    fn target_performance_inputs() -> (DomainModel, OperatorSnapshot, usize) {
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "workspace-0001".to_owned(),
        });
        model.insert_tab(Tab {
            tab_id: "tab-0001".to_owned(),
            workspace_id: "workspace-0001".to_owned(),
            label: None,
        });
        for index in 1..=50 {
            model.insert_pane(Pane {
                pane_id: format!("pane-{index:04}"),
                workspace_id: "workspace-0001".to_owned(),
                tab_id: "tab-0001".to_owned(),
                terminal_id: format!("terminal-{index:04}"),
                display_name: None,
            });
        }

        let run_ids = (1..=200)
            .map(|index| RunId::parse(&format!("{index:026}")).unwrap())
            .collect::<Vec<_>>();
        for (index, run_id) in run_ids.iter().copied().enumerate() {
            model.insert_task_run(TaskRun {
                run_id,
                key: RunKey::Controller(format!("run-{:04}", index + 1)),
                display_ordinal: DisplayOrdinal::new(index as i64 + 1),
                state: TaskState::Running,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            });
        }

        let expected_execution_edges = run_ids.windows(2).count();
        for pair in run_ids.windows(2) {
            assert!(model.insert_execution_edge(ExecutionEdge {
                parent_run_id: pair[0],
                child_run_id: pair[1],
            }));
        }
        let mut inserted_dependencies = 0;
        'pairs: for dependent in 1..run_ids.len() {
            for prerequisite in 0..dependent {
                assert!(model.insert_dependency_edge(DependencyEdge {
                    prerequisite_run_id: run_ids[prerequisite],
                    dependent_run_id: run_ids[dependent],
                }));
                inserted_dependencies += 1;
                if inserted_dependencies == 1_000 {
                    break 'pairs;
                }
            }
        }
        assert_eq!(inserted_dependencies, 1_000);
        assert_eq!(model.execution_edges().count(), expected_execution_edges);

        (
            model,
            OperatorSnapshot {
                activity: Arc::from(Vec::new()),
                terminal_times: Arc::new(HashMap::new()),
            },
            expected_execution_edges,
        )
    }

    #[cfg(feature = "workload-harness")]
    fn test_performance_publication(
        snapshot: PerformanceSnapshot,
        effective_quality: ObservationQuality,
        stamp: Option<(u64, u64)>,
    ) -> PerformancePublication {
        PerformancePublication {
            snapshot,
            effective_quality,
            workload_sample_stamp: stamp.map(|(sample_ordinal, sampled_at_ns)| {
                WorkloadSampleStamp {
                    sample_ordinal,
                    sampled_at_ns,
                }
            }),
        }
    }

    #[test]
    fn performance_quality_composition_preserves_stronger_source_states() {
        let degraded = PerformanceSnapshot {
            reasons: BTreeSet::from([PerformanceDegradationReason::EventsSixtySeconds]),
            ..PerformanceSnapshot::default()
        };
        assert_eq!(
            compose_quality(ObservationQuality::Disconnected, &degraded),
            ObservationQuality::Disconnected
        );
        assert_eq!(
            compose_quality(ObservationQuality::Reconciling, &degraded),
            ObservationQuality::Reconciling
        );
        assert_eq!(
            compose_quality(ObservationQuality::Live, &degraded),
            ObservationQuality::Degraded
        );
    }

    #[tokio::test]
    async fn twice_target_becomes_degraded_by_sixty_seconds_without_loss() {
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock.clone());
        let (model, operator, expected_execution_edges) = target_performance_inputs();
        for index in 0_u64..1_201 {
            clock.set(Duration::from_millis((index + 1) * 25));
            ingress.admit().complete();
        }
        clock.set(Duration::from_millis(30_025));
        let threshold_snapshot = sampler.sample(&model, &operator, 30_025);
        assert_eq!(
            (
                threshold_snapshot.live_panes,
                threshold_snapshot.default_visible_task_runs,
                threshold_snapshot.dependency_edges,
                threshold_snapshot.execution_edges
            ),
            (50, 200, 1_000, expected_execution_edges)
        );
        assert!(
            threshold_snapshot
                .reasons
                .contains(&PerformanceDegradationReason::EventsSixtySeconds)
        );
        for index in 1_201_u64..2_400 {
            clock.set(Duration::from_millis((index + 1) * 25));
            ingress.admit().complete();
        }
        clock.set(Duration::from_secs(60));
        let snapshot = sampler.sample(&model, &operator, 60_000);
        assert_eq!(snapshot.pending_events, 0);
        assert_eq!(snapshot.admission_high_water, 2_400);
        assert_eq!(snapshot.completion_high_water, 2_400);
        assert_eq!(
            (
                snapshot.live_panes,
                snapshot.default_visible_task_runs,
                snapshot.dependency_edges,
                snapshot.execution_edges
            ),
            (50, 200, 1_000, expected_execution_edges)
        );
        assert!(
            snapshot
                .reasons
                .contains(&PerformanceDegradationReason::EventsSixtySeconds)
        );
        assert_eq!(
            compose_quality(ObservationQuality::Live, &snapshot),
            ObservationQuality::Degraded
        );
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn composed_quality_recovers_only_after_all_windows_lag_and_source_clear() {
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock.clone());
        let (model, operator, _expected_execution_edges) = target_performance_inputs();
        let (performance_sender, performance) = watch::channel(initial_performance_publication());
        let (quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
        let publish = |snapshot: PerformanceSnapshot,
                       source_quality: ObservationQuality,
                       sample_ordinal: u64,
                       sampled_at: Duration| {
            let effective_quality = compose_quality(source_quality, &snapshot);
            publish_performance_generation(
                &performance_sender,
                &quality_sender,
                source_quality,
                test_performance_publication(
                    snapshot,
                    effective_quality,
                    Some((
                        sample_ordinal,
                        u64::try_from(sampled_at.as_nanos()).unwrap(),
                    )),
                ),
                None,
                None,
            );
        };

        let lagging_admission = ingress.admit();
        clock.set(Duration::from_millis(1_500));
        for _ in 0..1_201 {
            ingress.admit().complete();
        }

        clock.set(Duration::from_secs(2));
        let all_reasons = sampler.sample(&model, &operator, 2_000);
        assert_eq!(
            all_reasons.reasons,
            BTreeSet::from([
                PerformanceDegradationReason::EventsOneSecond,
                PerformanceDegradationReason::EventsTenSeconds,
                PerformanceDegradationReason::EventsSixtySeconds,
                PerformanceDegradationReason::EventLag,
            ])
        );
        publish(
            all_reasons,
            ObservationQuality::Live,
            0,
            Duration::from_secs(2),
        );
        assert_eq!(
            performance.borrow().effective_quality,
            ObservationQuality::Degraded
        );
        assert_eq!(*quality.borrow(), ObservationQuality::Degraded);

        clock.set(Duration::from_millis(2_501));
        let after_one_second = sampler.sample(&model, &operator, 2_501);
        assert_eq!(
            after_one_second.reasons,
            BTreeSet::from([
                PerformanceDegradationReason::EventsTenSeconds,
                PerformanceDegradationReason::EventsSixtySeconds,
                PerformanceDegradationReason::EventLag,
            ])
        );
        publish(
            after_one_second,
            ObservationQuality::Live,
            1,
            Duration::from_millis(2_501),
        );
        assert_eq!(
            performance.borrow().effective_quality,
            ObservationQuality::Degraded
        );

        clock.set(Duration::from_millis(11_501));
        let after_ten_seconds = sampler.sample(&model, &operator, 11_501);
        assert_eq!(
            after_ten_seconds.reasons,
            BTreeSet::from([
                PerformanceDegradationReason::EventsSixtySeconds,
                PerformanceDegradationReason::EventLag,
            ])
        );
        publish(
            after_ten_seconds,
            ObservationQuality::Live,
            2,
            Duration::from_millis(11_501),
        );
        assert_eq!(
            performance.borrow().effective_quality,
            ObservationQuality::Degraded
        );

        clock.set(Duration::from_millis(61_501));
        let after_sixty_seconds = sampler.sample(&model, &operator, 61_501);
        assert_eq!(
            after_sixty_seconds.reasons,
            BTreeSet::from([PerformanceDegradationReason::EventLag])
        );
        publish(
            after_sixty_seconds,
            ObservationQuality::Live,
            3,
            Duration::from_millis(61_501),
        );
        assert_eq!(
            performance.borrow().effective_quality,
            ObservationQuality::Degraded
        );

        lagging_admission.complete();
        let clean = sampler.sample(&model, &operator, 61_501);
        assert!(clean.reasons.is_empty());
        publish(
            clean.clone(),
            ObservationQuality::Degraded,
            4,
            Duration::from_millis(61_501),
        );
        assert!(performance.borrow().snapshot.reasons.is_empty());
        assert_eq!(
            performance.borrow().effective_quality,
            ObservationQuality::Degraded
        );
        assert_eq!(*quality.borrow(), ObservationQuality::Degraded);

        publish(
            clean,
            ObservationQuality::Live,
            5,
            Duration::from_millis(61_501),
        );
        assert!(performance.borrow().snapshot.reasons.is_empty());
        assert_eq!(
            performance.borrow().effective_quality,
            ObservationQuality::Live
        );
        assert_eq!(*quality.borrow(), ObservationQuality::Live);
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn performance_publication_remains_coherent_while_quality_projection_is_paused() {
        let initial = test_performance_publication(
            PerformanceSnapshot::default(),
            ObservationQuality::Live,
            None,
        );
        let (performance_sender, performance) = watch::channel(initial);
        let (quality_sender, quality) = watch::channel(ObservationQuality::Live);
        let observed = Arc::new(Mutex::new(Vec::<WorkloadPerformanceSample>::new()));
        let observer: WorkloadPerformanceObserver = {
            let observed = Arc::clone(&observed);
            Arc::new(move |sample| observed.lock().unwrap().push(sample.clone()))
        };
        let published = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let pause: Arc<dyn Fn() + Send + Sync> = {
            let published = Arc::clone(&published);
            let release = Arc::clone(&release);
            Arc::new(move || {
                published.wait();
                release.wait();
            })
        };
        let degraded = PerformanceSnapshot {
            reasons: BTreeSet::from([PerformanceDegradationReason::EventsSixtySeconds]),
            ..PerformanceSnapshot::default()
        };
        let publication = test_performance_publication(
            degraded.clone(),
            ObservationQuality::Degraded,
            Some((1, 30_025_000_000)),
        );

        let thread = std::thread::spawn(move || {
            publish_performance_generation(
                &performance_sender,
                &quality_sender,
                ObservationQuality::Live,
                publication,
                Some(&observer),
                Some(pause.as_ref()),
            );
        });
        published.wait();

        let current = performance.borrow();
        assert_eq!(current.snapshot, degraded);
        assert_eq!(current.effective_quality, ObservationQuality::Degraded);
        assert_eq!(*quality.borrow(), ObservationQuality::Live);
        drop(current);

        release.wait();
        thread.join().unwrap();
        assert_eq!(*quality.borrow(), ObservationQuality::Degraded);
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn performance_generation_observer_records_every_sample_before_watch_coalescing() {
        let initial = test_performance_publication(
            PerformanceSnapshot::default(),
            ObservationQuality::Live,
            None,
        );
        let (performance_sender, mut performance) = watch::channel(initial);
        let (quality_sender, _quality) = watch::channel(ObservationQuality::Live);
        let observed = Arc::new(Mutex::new(Vec::<WorkloadPerformanceSample>::new()));
        let observer: WorkloadPerformanceObserver = {
            let observed = Arc::clone(&observed);
            Arc::new(move |sample| observed.lock().unwrap().push(sample.clone()))
        };
        let first = test_performance_publication(
            PerformanceSnapshot::default(),
            ObservationQuality::Live,
            Some((10, 2_000_000_000)),
        );
        publish_performance_generation(
            &performance_sender,
            &quality_sender,
            ObservationQuality::Live,
            first.clone(),
            Some(&observer),
            None,
        );
        assert!(performance.has_changed().unwrap());
        assert_eq!(*performance.borrow_and_update(), first);

        let second = test_performance_publication(
            PerformanceSnapshot::default(),
            ObservationQuality::Live,
            Some((11, 2_050_000_000)),
        );
        publish_performance_generation(
            &performance_sender,
            &quality_sender,
            ObservationQuality::Live,
            second.clone(),
            Some(&observer),
            None,
        );
        assert!(!performance.has_changed().unwrap());
        assert_eq!(*performance.borrow(), first);

        let changed_snapshot = PerformanceSnapshot {
            admission_high_water: 1,
            completion_high_water: 1,
            events_one_second: 1,
            events_ten_seconds: 1,
            events_sixty_seconds: 1,
            ..PerformanceSnapshot::default()
        };
        let third = test_performance_publication(
            changed_snapshot,
            ObservationQuality::Live,
            Some((12, 2_100_000_000)),
        );
        publish_performance_generation(
            &performance_sender,
            &quality_sender,
            ObservationQuality::Live,
            third.clone(),
            Some(&observer),
            None,
        );
        assert!(performance.has_changed().unwrap());
        assert_eq!(*performance.borrow_and_update(), third);
        assert!(!performance.has_changed().unwrap());

        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 3);
        assert_eq!(
            observed
                .iter()
                .map(|sample| {
                    let stamp = sample.publication.workload_sample_stamp.unwrap();
                    (stamp.sample_ordinal, stamp.sampled_at_ns)
                })
                .collect::<Vec<_>>(),
            vec![
                (10, 2_000_000_000),
                (11, 2_050_000_000),
                (12, 2_100_000_000),
            ]
        );
        assert!(observed.windows(2).all(|pair| {
            pair[0]
                .publication
                .workload_sample_stamp
                .unwrap()
                .sampled_at_ns
                <= pair[1]
                    .publication
                    .workload_sample_stamp
                    .unwrap()
                    .sampled_at_ns
        }));
        assert!(
            observed
                .iter()
                .all(|sample| sample.source_quality == ObservationQuality::Live)
        );
        assert_eq!(observed[0].publication, first);
        assert_eq!(observed[1].publication, second);
        assert_eq!(observed[2].publication, third);
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn equal_performance_sample_does_not_silently_advance_watch_stamp() {
        let initial = test_performance_publication(
            PerformanceSnapshot::default(),
            ObservationQuality::Live,
            Some((7, 1_000)),
        );
        let (performance_sender, mut performance) = watch::channel(initial.clone());
        let (quality_sender, _quality) = watch::channel(ObservationQuality::Live);
        let observer: WorkloadPerformanceObserver = Arc::new(|_| {});
        let _ = performance.borrow_and_update();
        let equal = test_performance_publication(
            PerformanceSnapshot::default(),
            ObservationQuality::Live,
            Some((8, 2_000)),
        );

        publish_performance_generation(
            &performance_sender,
            &quality_sender,
            ObservationQuality::Live,
            equal,
            Some(&observer),
            None,
        );

        assert_eq!(*performance.borrow(), initial);
        assert!(!performance.has_changed().unwrap());
    }

    #[cfg(feature = "workload-harness")]
    #[test]
    fn changed_performance_sample_publishes_stamp_snapshot_and_quality_atomically() {
        let initial = test_performance_publication(
            PerformanceSnapshot::default(),
            ObservationQuality::Live,
            Some((7, 1_000)),
        );
        let (performance_sender, mut performance) = watch::channel(initial);
        let (quality_sender, _quality) = watch::channel(ObservationQuality::Live);
        let observer: WorkloadPerformanceObserver = Arc::new(|_| {});
        let _ = performance.borrow_and_update();
        let next = test_performance_publication(
            PerformanceSnapshot {
                pending_events: 1,
                admission_high_water: 1,
                reasons: BTreeSet::from([PerformanceDegradationReason::EventLag]),
                ..PerformanceSnapshot::default()
            },
            ObservationQuality::Degraded,
            Some((8, 2_000)),
        );

        publish_performance_generation(
            &performance_sender,
            &quality_sender,
            ObservationQuality::Live,
            next.clone(),
            Some(&observer),
            None,
        );

        assert!(performance.has_changed().unwrap());
        assert_eq!(*performance.borrow_and_update(), next);
        assert!(!performance.has_changed().unwrap());
    }

    #[tokio::test]
    async fn runtime_persistence_reserve_enqueue_preserves_late_health_observation() {
        let expected = failure(
            PersistenceOperation::Apply,
            PersistencePhase::QueueAdmission,
            PersistenceFailureCode::ChannelClosed,
            DurabilityDisposition::NotCommitted,
        );

        {
            let sink = Arc::new(RecordingOccurrenceSink::default());
            let directory = tempfile::tempdir().unwrap();
            let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
            let store = open_writer(&root).unwrap();
            let (lifecycle, mut writer) = spawn_writer(store).unwrap();
            writer.set_after_second_reserve_health_check_failure(expected);
            let (mut runtime, diagnostics) = RuntimePersistence::new_for_test(writer, sink.clone());

            assert!(runtime.reserve_enqueue().is_none());
            let snapshot = diagnostics.borrow();
            assert_eq!(
                snapshot.persistence,
                PersistenceStatus::Degraded { failure: expected }
            );
            assert_eq!(
                snapshot.controller_input,
                ControllerInputStatus::Unavailable {
                    reason: ControllerInputUnavailableReason::PersistenceUnavailable,
                }
            );
            assert_eq!(sink.attempts.load(Ordering::Relaxed), 1);
            drop(snapshot);

            shutdown_writer(lifecycle).await;
        }

        {
            let sink = Arc::new(RecordingOccurrenceSink::default());
            let directory = tempfile::tempdir().unwrap();
            let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
            let store = open_writer(&root).unwrap();
            let (lifecycle, writer) = spawn_writer(store).unwrap();
            let (mut runtime, diagnostics) = RuntimePersistence::new_for_test(writer, sink.clone());
            shutdown_writer(lifecycle).await;

            assert!(runtime.reserve_enqueue().is_none());
            assert_eq!(
                diagnostics.borrow().persistence,
                PersistenceStatus::Degraded { failure: expected }
            );
            assert_eq!(sink.attempts.load(Ordering::Relaxed), 1);
        }

        {
            let sink = Arc::new(RecordingOccurrenceSink::default());
            let directory = tempfile::tempdir().unwrap();
            let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
            let store = open_writer(&root).unwrap();
            let (lifecycle, writer) = spawn_writer(store).unwrap();
            let (mut runtime, diagnostics) = RuntimePersistence::new_for_test(writer, sink.clone());

            let pending = runtime
                .reserve_enqueue()
                .expect("healthy writer must return a usable permit")
                .enqueue(Vec::new());
            assert_eq!(
                runtime.finish_pending(pending).await.unwrap(),
                RuntimeWriteOutcome::Durable
            );
            assert_eq!(diagnostics.borrow().persistence, PersistenceStatus::Healthy);
            assert_eq!(sink.attempts.load(Ordering::Relaxed), 0);

            shutdown_writer(lifecycle).await;
        }

        {
            let sink = Arc::new(RecordingOccurrenceSink::default());
            let directory = tempfile::tempdir().unwrap();
            let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
            let store = open_writer(&root).unwrap();
            let (lifecycle, writer) = spawn_writer(store).unwrap();
            let capacity_guard = lifecycle.hold_queue_capacity_for_test().await;
            let (mut runtime, diagnostics) = RuntimePersistence::new_for_test(writer, sink.clone());

            assert!(runtime.reserve_enqueue().is_none());
            assert_eq!(diagnostics.borrow().persistence, PersistenceStatus::Healthy);
            assert_eq!(sink.attempts.load(Ordering::Relaxed), 0);

            drop(capacity_guard);
            shutdown_writer(lifecycle).await;
        }
    }

    #[tokio::test]
    async fn i4_operator_acceptor_only_change_wakes_diagnostics() {
        let sink = Arc::new(RecordingOccurrenceSink::default());
        let (_directory, lifecycle, mut runtime, mut diagnostics) = runtime_with_sink(sink);
        let (mut reducer, mut model) = Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let coverage = SourceCoverageRegistry::default();
        let (performance, _sampler) =
            performance_tracker(Arc::new(TestPerformanceClock::new(Duration::ZERO)));
        let (_sender, receiver) =
            controller::request_channel(1, runtime.acceptor_diagnostics.clone(), performance);
        let mut receiver = Some(receiver);
        let _ = diagnostics.borrow_and_update();
        let _ = model.borrow_and_update();

        runtime.acceptor_diagnostics.record_socket_saturation();
        service_controller(
            ControllerRuntimeEvent::DiagnosticsChanged,
            &mut receiver,
            "operator-test",
            &mut reducer,
            &mut runtime,
            &model,
            &coverage,
        )
        .await;

        tokio::time::timeout(Duration::from_secs(1), diagnostics.changed())
            .await
            .expect("acceptor-only counter change must wake runtime diagnostics")
            .unwrap();
        assert_eq!(
            diagnostics.borrow().controller_counters.socket_saturations,
            1
        );
        assert!(!model.has_changed().unwrap());

        drop(receiver);
        drop(runtime);
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn operator_command_receiver_publishes_dismissed_snapshot() {
        let sink = Arc::new(RecordingOccurrenceSink::default());
        let (_directory, lifecycle, mut runtime, _diagnostics) = runtime_with_sink(sink);
        let run_id = RunId::new();
        let mut domain = DomainModel::default();
        domain.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller("clearable".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Completed,
            has_controller_task_state_event: true,
            created_at_ms: Some(10),
            updated_at_ms: Some(20),
            finished_at_ms: Some(20),
            subject: None,
            dismissed_at_ms: None,
        });
        let (mut reducer, mut model) = Reducer::new(RestoredState {
            model: domain,
            next_ordinal: 2,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let coverage = SourceCoverageRegistry::default();
        let (sender, receiver) = mpsc::channel(1);
        let mut receiver = Some(receiver);
        let _ = model.borrow_and_update();
        sender
            .send(OperatorCommand::DismissClearable)
            .await
            .unwrap();

        let command = receive_operator_command(&mut receiver).await;
        assert!(
            service_operator_command(
                command,
                &mut receiver,
                &mut reducer,
                &mut runtime,
                &model,
                &coverage,
            )
            .await
            .unwrap()
        );

        tokio::time::timeout(Duration::from_secs(1), model.changed())
            .await
            .expect("operator command did not publish a model snapshot")
            .unwrap();
        assert!(
            model
                .borrow_and_update()
                .task_run(&run_id)
                .unwrap()
                .dismissed_at_ms
                .is_some()
        );

        drop(receiver);
        drop(runtime);
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn reconnect_wait_services_operator_command_before_delay_elapses() {
        let sink = Arc::new(RecordingOccurrenceSink::default());
        let (_directory, lifecycle, mut runtime, _diagnostics) = runtime_with_sink(sink);
        let run_id = RunId::new();
        let mut domain = DomainModel::default();
        domain.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller("clearable-while-down".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Completed,
            has_controller_task_state_event: true,
            created_at_ms: Some(10),
            updated_at_ms: Some(20),
            finished_at_ms: Some(20),
            subject: None,
            dismissed_at_ms: None,
        });
        let (mut reducer, model) = Reducer::new(RestoredState {
            model: domain,
            next_ordinal: 2,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let cancellation = CancellationToken::new();
        let mut controller_requests = None;
        let (command_sender, operator_commands) = mpsc::channel(1);
        let mut operator_commands = Some(operator_commands);
        let (provider_sender, mut provider, provider_thread) = inactive_provider_integration();
        let mut observed = model.clone();
        let _ = observed.borrow_and_update();
        command_sender
            .send(OperatorCommand::DismissClearable)
            .await
            .unwrap();

        {
            let wait = wait_or_service_controller(
                &cancellation,
                Duration::from_secs(1),
                &mut controller_requests,
                &mut operator_commands,
                "subscription-down",
                &mut reducer,
                &mut runtime,
                &model,
                &mut provider,
            );
            tokio::pin!(wait);
            tokio::select! {
                result = &mut wait => {
                    panic!("reconnect wait completed before publishing the dismissal: {result:?}");
                }
                result = observed.changed() => {
                    result.expect("operator command must publish before the reconnect delay");
                }
            }
        }

        assert!(
            observed
                .borrow_and_update()
                .task_run(&run_id)
                .unwrap()
                .dismissed_at_ms
                .is_some()
        );

        drop(provider_sender);
        provider_thread.stop().await.unwrap();
        drop(runtime);
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn i4_d3_owner_update_failure_marks_stale_and_skips_later_writes() {
        let sink = Arc::new(RecordingOccurrenceSink::default());
        let (_directory, lifecycle, mut runtime, diagnostics) = runtime_with_sink(sink);
        let owner_failure = failure(
            PersistenceOperation::UpdateOwnerLocation,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::OwnerAbsent,
            DurabilityDisposition::NotCommitted,
        );

        assert_eq!(
            runtime
                .update_owner_location("terminal", "pane")
                .await
                .unwrap(),
            RuntimeWriteOutcome::NotCommitted(owner_failure)
        );
        assert_eq!(
            runtime.apply(Vec::new()).await.unwrap(),
            RuntimeWriteOutcome::Skipped
        );
        assert_eq!(
            runtime
                .update_owner_location("terminal", "pane")
                .await
                .unwrap(),
            RuntimeWriteOutcome::Skipped
        );
        let snapshot = diagnostics.borrow();
        assert_eq!(snapshot.owner, crate::diagnostics::OwnerFreshness::Stale);
        assert_eq!(snapshot.persistence_counters.not_committed_batches, 0);
        assert_eq!(snapshot.persistence_counters.skipped_batches, 1);
        assert_eq!(snapshot.persistence_counters.skipped_owner_updates, 1);
        drop(snapshot);
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn i4_d3_accepted_not_committed_counts_without_rollback() {
        let sink = Arc::new(RecordingOccurrenceSink::default());
        let (_directory, lifecycle, mut runtime, diagnostics) = runtime_with_sink(sink);
        let expected = failure(
            PersistenceOperation::Apply,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::NotCommitted,
        );

        assert_eq!(
            runtime.record_failure(expected, RuntimeCommandClass::Batch),
            RuntimeWriteOutcome::NotCommitted(expected)
        );
        assert_eq!(
            diagnostics
                .borrow()
                .persistence_counters
                .not_committed_batches,
            1
        );
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn i4_d3_post_commit_cleanup_failure_counts_committed() {
        let sink = Arc::new(RecordingOccurrenceSink::default());
        let (_directory, lifecycle, mut runtime, diagnostics) = runtime_with_sink(sink);
        let expected = failure(
            PersistenceOperation::Cleanup,
            PersistencePhase::PostApplyCommit,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::Committed,
        );

        assert_eq!(
            runtime.record_failure(expected, RuntimeCommandClass::Batch),
            RuntimeWriteOutcome::CommittedButDegraded(expected)
        );
        assert_eq!(
            diagnostics
                .borrow()
                .persistence_counters
                .committed_but_degraded_batches,
            1
        );
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn i4_d3_ack_drop_counts_durability_unknown() {
        let sink = Arc::new(RecordingOccurrenceSink::default());
        let (_directory, lifecycle, mut runtime, diagnostics) = runtime_with_sink(sink);
        let expected = failure(
            PersistenceOperation::Apply,
            PersistencePhase::Acknowledgement,
            PersistenceFailureCode::AcknowledgementDropped,
            DurabilityDisposition::Unknown,
        );

        assert_eq!(
            runtime.record_failure(expected, RuntimeCommandClass::Batch),
            RuntimeWriteOutcome::DurabilityUnknown(expected)
        );
        assert_eq!(
            diagnostics
                .borrow()
                .persistence_counters
                .durability_unknown_batches,
            1
        );
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn i4_d3_first_failure_log_contains_codes_not_private_values() {
        let sink = Arc::new(RecordingOccurrenceSink::default());
        let (_directory, lifecycle, mut runtime, diagnostics) = runtime_with_sink(sink.clone());
        let expected = failure(
            PersistenceOperation::Apply,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Sqlite,
            DurabilityDisposition::NotCommitted,
        );

        runtime.record_failure(expected, RuntimeCommandClass::Batch);
        let bytes = sink.bytes.lock().unwrap().clone();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("HERDR_TOP_PERSISTENCE_V1 "));
        assert!(text.ends_with('\n'));
        assert!(text.contains("\"operation\":\"apply\""));
        assert!(text.contains("\"code\":\"sqlite\""));
        assert!(!text.contains("PRIVATE_PATH_OR_EVENT_ID"));
        assert_eq!(
            diagnostics.borrow().first_failure_log,
            OccurrenceLogStatus::Emitted
        );
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn i4_d3_occurrence_append_failure_is_reported_and_attempted_once() {
        let sink = Arc::new(RecordingOccurrenceSink {
            fail: true,
            ..RecordingOccurrenceSink::default()
        });
        let (_directory, lifecycle, mut runtime, diagnostics) = runtime_with_sink(sink.clone());
        let first = failure(
            PersistenceOperation::Cleanup,
            PersistencePhase::CommandExecution,
            PersistenceFailureCode::Io,
            DurabilityDisposition::NotCommitted,
        );
        let later = failure(
            PersistenceOperation::Apply,
            PersistencePhase::Acknowledgement,
            PersistenceFailureCode::AcknowledgementDropped,
            DurabilityDisposition::Unknown,
        );

        runtime.record_failure(first, RuntimeCommandClass::Batch);
        runtime.record_failure(later, RuntimeCommandClass::Batch);
        assert_eq!(sink.attempts.load(Ordering::Relaxed), 1);
        assert_eq!(
            diagnostics.borrow().first_failure_log,
            OccurrenceLogStatus::Failed
        );
        assert_eq!(
            diagnostics.borrow().persistence,
            crate::store::PersistenceStatus::Degraded { failure: first }
        );
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn i4_d3_accepted_failure_log_excludes_raw_store_text() {
        const RAW_STORE_TEXT: &str = "PRIVATE_SQLITE_ROW_AND_PATH_2A31";
        let sink = Arc::new(RecordingOccurrenceSink::default());
        let directory = tempfile::tempdir().unwrap();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        rusqlite::Connection::open(crate::store::database_path(&root))
            .unwrap()
            .execute_batch(&format!(
                "CREATE TRIGGER i4_private_store_text BEFORE INSERT ON events \
                 BEGIN SELECT RAISE(ABORT, '{RAW_STORE_TEXT}'); END;"
            ))
            .unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut runtime, _diagnostics) = RuntimePersistence::new_for_test(writer, sink.clone());

        let outcome = runtime
            .apply(vec![PersistOp::RecordCollectorGap(CollectorGap {
                event_id: "private-event-id".to_owned(),
                herdr_session: "private-session".to_owned(),
                seen_at_ms: 0,
                kind: GapKind::Startup,
            })])
            .await
            .unwrap();
        assert!(matches!(outcome, RuntimeWriteOutcome::NotCommitted(_)));

        let text = String::from_utf8(sink.bytes.lock().unwrap().clone()).unwrap();
        assert!(!text.contains(RAW_STORE_TEXT));
        assert!(!text.contains("private-event-id"));
        assert!(!text.contains("private-session"));
        assert!(!text.contains(directory.path().to_string_lossy().as_ref()));
        shutdown_writer(lifecycle).await;
    }

    #[tokio::test]
    async fn unscoped_subscriptions_omit_pane_scoped_agent_status_event() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            let response = json!({
                "id": request["id"],
                "result": {"type": "subscription_started"},
            });
            let mut bytes = serde_json::to_vec(&response).unwrap();
            bytes.push(b'\n');
            reader.get_mut().write_all(&bytes).await.unwrap();
            request
        });

        let stream = wire::subscribe(&socket, &subscriptions()).await.unwrap();
        drop(stream);
        let request = server.await.unwrap();
        let requested = request["params"]["subscriptions"].as_array().unwrap();

        assert!(
            requested.iter().all(|subscription| {
                subscription["type"].as_str() != Some("pane.agent_status_changed")
            }),
            "pane.agent_status_changed requires pane_id and must not be requested unscoped: {requested:?}"
        );
    }

    fn watchdog_probe_topology(
        agent_name: Option<&str>,
        session: Option<(AgentSessionReferenceKind, &str, &str)>,
    ) -> TopologySnapshot {
        TopologySnapshot {
            workspaces: vec![Workspace {
                workspace_id: "w1".to_owned(),
            }],
            tabs: vec![Tab {
                tab_id: "w1:t1".to_owned(),
                workspace_id: "w1".to_owned(),
                label: None,
            }],
            panes: vec![PaneSnapshot {
                pane_id: "w1:p4".to_owned(),
                workspace_id: "w1".to_owned(),
                tab_id: "w1:t1".to_owned(),
                terminal_id: "terminal-4".to_owned(),
                display_name: None,
                agent: agent_name.map(|name| SnapshotAgent {
                    agent_name: name.to_owned(),
                    state: ExecState::Working,
                }),
                agent_session: session.map(|(kind, source, value)| AgentSessionReference {
                    source: source.to_owned(),
                    agent: agent_name.unwrap_or("claude").to_owned(),
                    kind,
                    value: value.to_owned(),
                }),
            }],
        }
    }

    fn reconciled_probe_topologies(
        topology: TopologySnapshot,
    ) -> (TopologySnapshot, TopologySnapshot) {
        let (mut reducer, shared) = Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let _ = reducer
            .reconcile_gap(ReconcileBatch {
                topology: topology.clone(),
                gap_kind: GapKind::Startup,
            })
            .unwrap();
        let projected = current_model_topology(&shared, &PendingTopologyClosures::default())
            .expect("reconciled topology had an ambiguous execution");
        (projected, topology)
    }

    fn assert_probe_comparison_diverges(
        projected: &TopologySnapshot,
        probed: TopologySnapshot,
        scenario: &str,
    ) {
        assert!(
            !probe_topology_matches_model(probed, projected.clone()),
            "probe comparison missed {scenario}"
        );
    }

    #[test]
    fn watchdog_probe_round_trips_all_retained_snapshot_shapes() {
        let agent_names = [
            Some("claude"),
            Some("codex"),
            Some("claude-code"),
            Some("Claude"),
            Some("aider"),
            None,
        ];
        let sessions = [
            None,
            Some((AgentSessionReferenceKind::Id, "sid-1")),
            Some((AgentSessionReferenceKind::Path, "/tmp/session.jsonl")),
        ];
        let sources = ["herdr:claude", "herdr", "arbitrary-source"];

        for agent_name in agent_names {
            for session in sessions {
                for source in sources {
                    let topology = watchdog_probe_topology(
                        agent_name,
                        session.map(|(kind, value)| (kind, source, value)),
                    );
                    let (projected, probed) = reconciled_probe_topologies(topology);
                    assert!(
                        probe_topology_matches_model(probed, projected),
                        "probe round trip mismatched agent={agent_name:?}, session={session:?}, source={source:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn watchdog_probe_name_comparison_matches_retention_and_detects_non_null_changes() {
        let mut current = watchdog_probe_topology(None, None);
        current.tabs[0].label = Some("stored tab".to_owned());
        current.panes[0].display_name = Some("stored pane".to_owned());

        let mut nameless_probe = current.clone();
        nameless_probe.tabs[0].label = None;
        nameless_probe.panes[0].display_name = None;
        assert!(probe_topology_matches_model(
            nameless_probe.clone(),
            current.clone()
        ));

        let mut changed_tab = nameless_probe.clone();
        changed_tab.tabs[0].label = Some("renamed tab".to_owned());
        assert!(!probe_topology_matches_model(changed_tab, current.clone()));

        let mut changed_pane = nameless_probe;
        changed_pane.panes[0].display_name = Some("renamed pane".to_owned());
        assert!(!probe_topology_matches_model(changed_pane, current));
    }

    #[test]
    fn watchdog_probe_detects_topology_membership_changes() {
        let topology = watchdog_probe_topology(Some("claude"), None);
        let (projected, probed) = reconciled_probe_topologies(topology);

        let mut changed = probed.clone();
        changed.workspaces.push(Workspace {
            workspace_id: "w2".to_owned(),
        });
        assert_probe_comparison_diverges(&projected, changed, "added workspace");
        let mut changed = probed.clone();
        changed.workspaces.clear();
        assert_probe_comparison_diverges(&projected, changed, "removed workspace");

        let mut changed = probed.clone();
        changed.tabs.push(Tab {
            tab_id: "w1:t2".to_owned(),
            workspace_id: "w1".to_owned(),
            label: None,
        });
        assert_probe_comparison_diverges(&projected, changed, "added tab");
        let mut changed = probed.clone();
        changed.tabs.clear();
        assert_probe_comparison_diverges(&projected, changed, "removed tab");

        let mut changed = probed.clone();
        let mut pane = changed.panes[0].clone();
        pane.pane_id = "w1:p5".to_owned();
        pane.terminal_id = "terminal-5".to_owned();
        changed.panes.push(pane);
        assert_probe_comparison_diverges(&projected, changed, "added pane");
        let mut changed = probed;
        changed.panes.clear();
        assert_probe_comparison_diverges(&projected, changed, "removed pane");
    }

    #[test]
    fn watchdog_probe_detects_agent_presence_state_and_provider_changes() {
        let topology = watchdog_probe_topology(Some("claude"), None);
        let (projected, probed) = reconciled_probe_topologies(topology);

        let mut changed = probed.clone();
        changed.panes[0].agent = None;
        assert_probe_comparison_diverges(&projected, changed, "agent disappearance");

        let no_agent = watchdog_probe_topology(None, None);
        let (no_agent_projected, mut agent_appeared) = reconciled_probe_topologies(no_agent);
        agent_appeared.panes[0].agent = Some(SnapshotAgent {
            agent_name: "claude".to_owned(),
            state: ExecState::Working,
        });
        assert_probe_comparison_diverges(&no_agent_projected, agent_appeared, "agent appearance");

        let mut changed = probed.clone();
        changed.panes[0].agent.as_mut().unwrap().state = ExecState::Idle;
        assert_probe_comparison_diverges(&projected, changed, "execution state change");

        let mut changed = probed;
        changed.panes[0].agent.as_mut().unwrap().agent_name = "codex".to_owned();
        assert_probe_comparison_diverges(&projected, changed, "resolved provider change");
    }

    #[test]
    fn watchdog_probe_detects_session_kind_value_and_presence_changes() {
        let topology = watchdog_probe_topology(
            Some("claude"),
            Some((AgentSessionReferenceKind::Id, "herdr:claude", "sid-1")),
        );
        let (projected, probed) = reconciled_probe_topologies(topology);

        let mut changed = probed.clone();
        changed.panes[0].agent_session.as_mut().unwrap().kind = AgentSessionReferenceKind::Path;
        assert_probe_comparison_diverges(&projected, changed, "session kind change");

        let mut changed = probed.clone();
        changed.panes[0].agent_session.as_mut().unwrap().value = "sid-2".to_owned();
        assert_probe_comparison_diverges(&projected, changed, "session value change");

        let mut changed = probed.clone();
        changed.panes[0].agent_session = None;
        assert_probe_comparison_diverges(&projected, changed, "session disappearance");

        let topology = watchdog_probe_topology(Some("claude"), None);
        let (projected, mut changed) = reconciled_probe_topologies(topology);
        changed.panes[0].agent_session = Some(AgentSessionReference {
            source: "herdr".to_owned(),
            agent: "claude".to_owned(),
            kind: AgentSessionReferenceKind::Id,
            value: "sid-1".to_owned(),
        });
        assert_probe_comparison_diverges(&projected, changed, "session appearance");

        let topology = watchdog_probe_topology(
            Some("claude"),
            Some((AgentSessionReferenceKind::Id, "herdr", "sid-1")),
        );
        let (projected, mut changed) = reconciled_probe_topologies(topology);
        changed.panes[0].agent_session.as_mut().unwrap().agent = "codex".to_owned();
        assert_probe_comparison_diverges(
            &projected,
            changed,
            "session agent resolved-provider change",
        );
    }

    #[test]
    fn watchdog_probe_detects_pane_location_changes() {
        let topology = watchdog_probe_topology(Some("claude"), None);
        let (projected, probed) = reconciled_probe_topologies(topology);

        for (scenario, workspace_id, tab_id, terminal_id) in [
            ("pane workspace move", "w2", "w1:t1", "terminal-4"),
            ("pane tab move", "w1", "w1:t2", "terminal-4"),
            ("pane terminal move", "w1", "w1:t1", "terminal-5"),
        ] {
            let mut changed = probed.clone();
            changed.panes[0].workspace_id = workspace_id.to_owned();
            changed.panes[0].tab_id = tab_id.to_owned();
            changed.panes[0].terminal_id = terminal_id.to_owned();
            assert_probe_comparison_diverges(&projected, changed, scenario);
        }
    }

    #[test]
    fn watchdog_agent_node_fallback_prefers_root_session() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller("unbound-controller-run".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "gap-agent-root".to_owned(),
            provider: Provider::Claude,
            native_session_id: Some("R".to_owned()),
            task_run_id: run_id,
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
        model.insert_agent_node(AgentNode {
            agent_node_id: "agent:claude:C".to_owned(),
            provider: Provider::Claude,
            native_session_id: Some("C".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(3),
            parent_agent_node_id: Some("gap-agent-root".to_owned()),
            state: Some(ExecState::Working),
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        });
        let execution = Execution {
            execution_id: "live-execution".to_owned(),
            pane_id: "w1:p4".to_owned(),
            terminal_id: "terminal-4".to_owned(),
            task_run_id: run_id,
            state: ExecState::Working,
        };

        let (_, session) = current_execution_identity(&model, &execution);

        assert_eq!(
            session.as_ref().map(|session| session.value.as_str()),
            Some("R")
        );
    }

    #[tokio::test]
    async fn watchdog_controller_native_alias_projects_root_session_and_stays_healthy() {
        let run_id = RunId::new();
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "w1".to_owned(),
        });
        model.insert_tab(Tab {
            tab_id: "w1:t1".to_owned(),
            workspace_id: "w1".to_owned(),
            label: None,
        });
        model.insert_pane(Pane {
            pane_id: "w1:p4".to_owned(),
            workspace_id: "w1".to_owned(),
            tab_id: "w1:t1".to_owned(),
            terminal_id: "terminal-4".to_owned(),
            display_name: None,
        });
        model.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller("controller-run".to_owned()),
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
        model.insert_task_run_alias(
            RunKey::Native {
                provider: Provider::Claude,
                sid: "R".to_owned(),
            },
            run_id,
        );
        model.insert_execution(Execution {
            execution_id: "live-execution".to_owned(),
            pane_id: "w1:p4".to_owned(),
            terminal_id: "terminal-4".to_owned(),
            task_run_id: run_id,
            state: ExecState::Working,
        });
        model.insert_agent_node(AgentNode {
            agent_node_id: "gap-agent-root".to_owned(),
            provider: Provider::Claude,
            native_session_id: None,
            task_run_id: run_id,
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
        model.insert_agent_node(AgentNode {
            agent_node_id: "agent:claude:C".to_owned(),
            provider: Provider::Claude,
            native_session_id: Some("C".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(3),
            parent_agent_node_id: Some("gap-agent-root".to_owned()),
            state: Some(ExecState::Working),
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        });
        let shared = watch::channel(Arc::new(model)).1;

        let projected = current_model_topology(&shared, &PendingTopologyClosures::default())
            .expect("Controller alias topology should project unambiguously");
        let projected_session = projected.panes[0]
            .agent_session
            .as_ref()
            .map(|session| session.value.as_str());

        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("controller-alias-projection.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (mut reader, request) = accept_wire_request(&listener).await;
            let mut frame = watchdog_snapshot_frame(&request, false, "claude");
            frame["result"]["snapshot"]["panes"][0]["agent_session"]["value"] =
                Value::String("R".to_owned());
            write_wire_frame(&mut reader, &frame).await;
        });

        let outcome = probe_primary_topology(
            &socket,
            &shared,
            PendingTopologyClosures::default(),
            LivenessPolicy { timeout_ms: 500 },
        )
        .await;

        let healthy = matches!(outcome, WatchdogProbeOutcome::HealthyIdle);
        assert_eq!(
            projected_session,
            Some("R"),
            "watchdog healthy outcome: {healthy}"
        );
        assert!(healthy);
        join_fake_server(server, "Controller alias topology probe").await;
    }

    #[tokio::test]
    async fn watchdog_probe_treats_ambiguous_live_executions_as_inconclusive_until_reconciliation()
    {
        let topology = watchdog_probe_topology(
            Some("claude"),
            Some((
                AgentSessionReferenceKind::Id,
                "herdr:claude",
                "watchdog-session",
            )),
        );
        let mut model = DomainModel::default();
        model.insert_workspace(topology.workspaces[0].clone());
        model.insert_tab(topology.tabs[0].clone());
        model.insert_pane(Pane {
            pane_id: "w1:p4".to_owned(),
            workspace_id: "w1".to_owned(),
            tab_id: "w1:t1".to_owned(),
            terminal_id: "terminal-4".to_owned(),
            display_name: None,
        });
        for execution_id in ["live-execution-1", "live-execution-2"] {
            model.insert_execution(Execution {
                execution_id: execution_id.to_owned(),
                pane_id: "w1:p4".to_owned(),
                terminal_id: "terminal-4".to_owned(),
                task_run_id: RunId::new(),
                state: ExecState::Working,
            });
        }
        let (mut reducer, shared) = Reducer::new(RestoredState {
            model,
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });

        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("ambiguous-projection.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut reader, request) = accept_wire_request(&listener).await;
                write_wire_frame(
                    &mut reader,
                    &watchdog_snapshot_frame(&request, false, "claude"),
                )
                .await;
            }
        });

        let ambiguous = probe_primary_topology(
            &socket,
            &shared,
            PendingTopologyClosures::default(),
            LivenessPolicy { timeout_ms: 500 },
        )
        .await;
        assert!(matches!(ambiguous, WatchdogProbeOutcome::Inconclusive));

        let _ = reducer
            .reconcile_gap(ReconcileBatch {
                topology,
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();
        let reconciled = probe_primary_topology(
            &socket,
            &shared,
            PendingTopologyClosures::default(),
            LivenessPolicy { timeout_ms: 500 },
        )
        .await;
        assert!(matches!(reconciled, WatchdogProbeOutcome::HealthyIdle));
        join_fake_server(server, "ambiguous projection probes").await;
    }

    fn watchdog_snapshot_frame(
        request: &Value,
        include_second_pane: bool,
        agent_name: &str,
    ) -> Value {
        let mut panes = vec![json!({
            "pane_id": "w1:p4",
            "terminal_id": "terminal-4",
            "workspace_id": "w1",
            "tab_id": "w1:t1",
            "agent": agent_name,
            "agent_status": "working",
            "agent_session": {
                "source": "herdr:claude",
                "agent": "claude",
                "kind": "id",
                "value": "watchdog-session",
            },
        })];
        if include_second_pane {
            panes.push(json!({
                "pane_id": "w1:p5",
                "terminal_id": "terminal-5",
                "workspace_id": "w1",
                "tab_id": "w1:t1",
                "agent_status": "unknown",
            }));
        }
        json!({
            "id": request["id"],
            "result": {
                "type": "session_snapshot",
                "snapshot": {
                    "version": "0.8.0",
                    "protocol": 19,
                    "focused_workspace_id": "w1",
                    "focused_tab_id": "w1:t1",
                    "focused_pane_id": "w1:p4",
                    "workspaces": [{"workspace_id": "w1"}],
                    "tabs": [{
                        "tab_id": "w1:t1",
                        "workspace_id": "w1",
                    }],
                    "panes": panes,
                    "layouts": [],
                    "agents": [],
                },
            },
        })
    }

    fn watchdog_ambiguous_snapshot_frame(request: &Value) -> Value {
        let mut frame = watchdog_snapshot_frame(request, false, "claude");
        frame["result"]["snapshot"]["panes"]
            .as_array_mut()
            .expect("watchdog snapshot panes must be an array")
            .push(json!({
                "pane_id": "w1:p4",
                "terminal_id": "terminal-4",
                "workspace_id": "w1",
                "tab_id": "w1:t1",
                "agent": "codex",
                "agent_status": "working",
                "agent_session": {
                    "source": "herdr:codex",
                    "agent": "codex",
                    "kind": "id",
                    "value": "ambiguous-watchdog-session",
                },
            }));
        frame
    }

    fn primary_subscription_is_scoped(request: &Value) -> bool {
        request["params"]["subscriptions"]
            .as_array()
            .is_some_and(|subscriptions| {
                subscriptions
                    .iter()
                    .any(|subscription| subscription.get("pane_id").is_some())
            })
    }

    fn reconnect_gap_count(root: &crate::lockfile::StateRoot) -> i64 {
        rusqlite::Connection::open(crate::store::database_path(root))
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE gap_kind = 'reconnect'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[tokio::test]
    async fn watchdog_matching_probes_keep_idle_subscription_and_model_stable() {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let socket = directory.path().join("idle-herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (probes_sender, probes_receiver) = tokio::sync::oneshot::channel();
        let liveness_policy = LivenessPolicy { timeout_ms: 400 };
        let mut harness = spawn_primary_collector_harness_with_policy(
            &directory,
            socket,
            "idle-watchdog.log",
            liveness_policy,
        );
        let server_cancellation = harness.cancellation.clone();
        let primary_subscriptions = Arc::new(AtomicUsize::new(0));
        let observed_primary_subscriptions = Arc::clone(&primary_subscriptions);
        let server = tokio::spawn(async move {
            let mut snapshot_instants = Vec::new();
            let mut probes_sender = Some(probes_sender);
            let mut held_streams = Vec::new();
            loop {
                let (mut reader, request) = tokio::select! {
                    () = server_cancellation.cancelled() => break,
                    accepted = accept_wire_request(&listener) => accepted,
                };
                match request["method"].as_str() {
                    Some("events.subscribe") => {
                        write_wire_frame(
                            &mut reader,
                            &json!({
                                "id": request["id"],
                                "result": {"type": "subscription_started"},
                            }),
                        )
                        .await;
                        if !primary_subscription_is_scoped(&request) {
                            observed_primary_subscriptions.fetch_add(1, Ordering::Release);
                        }
                        held_streams.push(reader);
                    }
                    Some("session.snapshot") => {
                        snapshot_instants.push(Instant::now());
                        write_wire_frame(
                            &mut reader,
                            &watchdog_snapshot_frame(&request, false, "claude-code"),
                        )
                        .await;
                        if snapshot_instants.len() == 3
                            && let Some(sender) = probes_sender.take()
                        {
                            sender.send(snapshot_instants.clone()).unwrap();
                        }
                    }
                    method => panic!("unexpected idle watchdog request: {method:?}"),
                }
            }
            drop(held_streams);
        });

        wait_for_quality(
            &mut harness.source_quality,
            ObservationQuality::Live,
            "initial idle subscription",
        )
        .await;
        let before_model = harness.model.borrow().clone();
        let database = rusqlite::Connection::open(crate::store::database_path(&root)).unwrap();
        let before_data_version: i64 = database
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .unwrap();

        let snapshot_instants = tokio::time::timeout(Duration::from_secs(3), probes_receiver)
            .await
            .expect("matching idle snapshot probes did not run twice")
            .expect("idle snapshot probe observer was dropped");
        assert_eq!(snapshot_instants.len(), 3);
        assert!(
            snapshot_instants[2].duration_since(snapshot_instants[1])
                >= liveness_timeout(&liveness_policy),
            "consecutive probes were not separated by a freshly stamped deadline"
        );

        assert_eq!(primary_subscriptions.load(Ordering::Acquire), 1);
        assert_eq!(*harness.source_quality.borrow(), ObservationQuality::Live);
        assert!(
            tokio::time::timeout(Duration::from_millis(250), harness.source_quality.changed())
                .await
                .is_err(),
            "matching probes changed observation quality"
        );
        let after_model = harness.model.borrow().clone();
        assert_eq!(
            before_model.executions().collect::<Vec<_>>(),
            after_model.executions().collect::<Vec<_>>()
        );
        assert_eq!(
            database
                .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            before_data_version,
            "matching probes persisted an operation"
        );

        let contents = harness.stop().await;
        join_fake_server(server, "idle watchdog probes").await;
        assert_eq!(reconnect_gap_count(&root), 0, "collector log: {contents}");
        assert!(
            !contents.contains("warning_code=\"herdr_primary_stream_watchdog_silence\""),
            "healthy idle subscription emitted a watchdog warning: {contents}"
        );
    }

    #[tokio::test]
    async fn watchdog_matching_probe_recovers_live_quality_after_dirty_replays() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("dirty-replays-herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (matched_sender, matched_receiver) = tokio::sync::oneshot::channel();
        let (enrichment_sender, enrichment_receiver) = tokio::sync::oneshot::channel();
        let mut harness = spawn_primary_collector_harness_with_policy(
            &directory,
            socket,
            "dirty-replays-watchdog.log",
            LivenessPolicy { timeout_ms: 100 },
        );
        let server_cancellation = harness.cancellation.clone();
        let server = tokio::spawn(async move {
            let mut snapshots = 0;
            let mut matched_sender = Some(matched_sender);
            let mut enrichment_sender = Some(enrichment_sender);
            let mut primary_stream = None;
            let mut held_streams = Vec::new();
            loop {
                let (mut reader, request) = tokio::select! {
                    () = server_cancellation.cancelled() => break,
                    accepted = accept_wire_request(&listener) => accepted,
                };
                match request["method"].as_str() {
                    Some("events.subscribe") => {
                        write_wire_frame(
                            &mut reader,
                            &json!({
                                "id": request["id"],
                                "result": {"type": "subscription_started"},
                            }),
                        )
                        .await;
                        if primary_subscription_is_scoped(&request) {
                            if let Some(sender) = enrichment_sender.take() {
                                sender.send(()).unwrap();
                            }
                            held_streams.push(reader);
                        } else {
                            assert!(
                                primary_stream.replace(reader).is_none(),
                                "dirty replay test unexpectedly reconnected"
                            );
                        }
                    }
                    Some("session.snapshot") => {
                        snapshots += 1;
                        write_wire_frame(
                            &mut reader,
                            &watchdog_snapshot_frame(&request, false, "claude"),
                        )
                        .await;
                        if snapshots <= RESNAPSHOT_ATTEMPTS + 1 {
                            let primary_stream = primary_stream
                                .as_mut()
                                .expect("snapshot arrived before the primary subscription");
                            write_wire_frame(
                                primary_stream,
                                &json!({
                                    "event": "pane_focused",
                                    "data": {"pane_id": format!("missing-pane-{snapshots}")},
                                }),
                            )
                            .await;
                        } else if snapshots == RESNAPSHOT_ATTEMPTS + 2
                            && let Some(sender) = matched_sender.take()
                        {
                            sender.send(()).unwrap();
                        }
                    }
                    method => panic!("unexpected dirty replay watchdog request: {method:?}"),
                }
            }
            drop(primary_stream);
            drop(held_streams);
        });

        tokio::time::timeout(Duration::from_secs(3), matched_receiver)
            .await
            .expect("reconciling watchdog probe did not receive a matching snapshot")
            .expect("matching reconciling probe observer was dropped");
        wait_for_quality(
            &mut harness.source_quality,
            ObservationQuality::Live,
            "matching reconciling watchdog probe",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(3), enrichment_receiver)
            .await
            .expect("clean-generation recovery did not activate enrichment")
            .expect("clean-generation enrichment observer was dropped");

        let contents = harness.stop().await;
        join_fake_server(server, "dirty replay watchdog recovery").await;
        assert!(
            !contents.contains("warning_code=\"herdr_primary_stream_watchdog_silence\""),
            "matching reconciling probe emitted a watchdog warning: {contents}"
        );
    }

    #[tokio::test]
    async fn watchdog_matching_probe_after_overflow_stops_enrichment_episode_discards() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("overflow-recovery-herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (activated_sender, activated_receiver) = tokio::sync::oneshot::channel();
        let (matched_sender, matched_receiver) = tokio::sync::oneshot::channel();
        let mut harness = spawn_primary_collector_harness_with_policy(
            &directory,
            socket,
            "overflow-recovery-watchdog.log",
            LivenessPolicy { timeout_ms: 100 },
        );
        let enrichment_diagnostics = harness.enrichment_diagnostics.clone();
        let server_cancellation = harness.cancellation.clone();
        let enrichment_writes = Arc::new(AtomicUsize::new(0));
        let observed_enrichment_writes = Arc::clone(&enrichment_writes);
        let server = tokio::spawn(async move {
            let mut snapshots = 0;
            let mut activated_sender = Some(activated_sender);
            let mut matched_sender = Some(matched_sender);
            let mut primary_stream = None;
            let mut enrichment_pulse = None;
            loop {
                let (mut reader, request) = tokio::select! {
                    () = server_cancellation.cancelled() => break,
                    accepted = accept_wire_request(&listener) => accepted,
                };
                match request["method"].as_str() {
                    Some("events.subscribe") => {
                        write_wire_frame(
                            &mut reader,
                            &json!({
                                "id": request["id"],
                                "result": {"type": "subscription_started"},
                            }),
                        )
                        .await;
                        if primary_subscription_is_scoped(&request) {
                            assert!(
                                enrichment_pulse.is_none(),
                                "overflow recovery unexpectedly replaced enrichment"
                            );
                            if let Some(sender) = activated_sender.take() {
                                sender.send(()).unwrap();
                            }
                            let pulse_cancellation = server_cancellation.clone();
                            let pulse_writes = Arc::clone(&observed_enrichment_writes);
                            let mut stream = reader.into_inner();
                            enrichment_pulse = Some(tokio::spawn(async move {
                                let mut interval = tokio::time::interval(Duration::from_millis(20));
                                interval.set_missed_tick_behavior(
                                    tokio::time::MissedTickBehavior::Delay,
                                );
                                let mut frame = serde_json::to_vec(&json!({
                                    "event": "pane_agent_status_changed",
                                    "data": {
                                        "pane_id": "w1:p4",
                                        "terminal_id": "terminal-4",
                                        "agent_status": "working",
                                    },
                                }))
                                .expect("enrichment pulse did not serialize");
                                frame.push(b'\n');
                                loop {
                                    tokio::select! {
                                        () = pulse_cancellation.cancelled() => break,
                                        _ = interval.tick() => {
                                            if stream.write_all(&frame).await.is_err() {
                                                break;
                                            }
                                            pulse_writes.fetch_add(1, Ordering::Release);
                                        }
                                    }
                                }
                            }));
                            write_primary_overflow_burst(
                                primary_stream
                                    .as_mut()
                                    .expect("enrichment activated before the primary stream"),
                            )
                            .await;
                        } else {
                            assert!(
                                primary_stream.replace(reader).is_none(),
                                "overflow recovery unexpectedly reconnected"
                            );
                        }
                    }
                    Some("session.snapshot") => {
                        snapshots += 1;
                        write_wire_frame(
                            &mut reader,
                            &watchdog_snapshot_frame(&request, false, "claude"),
                        )
                        .await;
                        if (2..=RESNAPSHOT_ATTEMPTS + 2).contains(&snapshots) {
                            write_primary_overflow_burst(
                                primary_stream
                                    .as_mut()
                                    .expect("snapshot arrived before the primary subscription"),
                            )
                            .await;
                        } else if snapshots == RESNAPSHOT_ATTEMPTS + 3
                            && let Some(sender) = matched_sender.take()
                        {
                            sender.send(()).unwrap();
                        }
                    }
                    method => panic!("unexpected overflow recovery request: {method:?}"),
                }
            }
            drop(primary_stream);
            if let Some(pulse) = enrichment_pulse {
                pulse.await.expect("enrichment pulse task panicked");
            }
        });

        wait_for_quality(
            &mut harness.source_quality,
            ObservationQuality::Live,
            "initial clean generation",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(3), activated_receiver)
            .await
            .expect("initial clean generation did not activate enrichment")
            .expect("initial enrichment observer was dropped");
        wait_for_quality(
            &mut harness.source_quality,
            ObservationQuality::Reconciling,
            "overflow-driven dirty streak",
        )
        .await;
        tokio::time::timeout(Duration::from_secs(3), matched_receiver)
            .await
            .expect("reconciling watchdog did not receive a matching overflow probe")
            .expect("overflow probe observer was dropped");
        wait_for_quality(
            &mut harness.source_quality,
            ObservationQuality::Live,
            "clean recovery after overflow",
        )
        .await;

        let discards_before = enrichment_diagnostics.episode_discards();
        let writes_before = enrichment_writes.load(Ordering::Acquire);
        tokio::time::sleep(Duration::from_millis(350)).await;
        let writes_after = enrichment_writes.load(Ordering::Acquire);
        assert!(
            writes_after > writes_before,
            "enrichment stream was not active after overflow recovery"
        );
        assert_eq!(
            enrichment_diagnostics.episode_discards(),
            discards_before,
            "enrichment episode discards kept accumulating after Live recovery"
        );

        let contents = harness.stop().await;
        join_fake_server(server, "overflow watchdog recovery").await;
        assert!(
            !contents.contains("warning_code=\"herdr_primary_stream_watchdog_silence\""),
            "matching overflow probe emitted a watchdog warning: {contents}"
        );
    }

    #[tokio::test]
    async fn watchdog_repeated_ambiguous_probes_do_not_reconnect_or_record_gaps() {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let socket = directory.path().join("ambiguous-herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (probed_sender, probed_receiver) = tokio::sync::oneshot::channel();
        let harness = spawn_primary_collector_harness_with_policy(
            &directory,
            socket,
            "ambiguous-watchdog.log",
            LivenessPolicy { timeout_ms: 100 },
        );
        let server_cancellation = harness.cancellation.clone();
        let primary_subscriptions = Arc::new(AtomicUsize::new(0));
        let observed_primary_subscriptions = Arc::clone(&primary_subscriptions);
        let server = tokio::spawn(async move {
            let mut snapshots = 0;
            let mut probed_sender = Some(probed_sender);
            let mut held_streams = Vec::new();
            loop {
                let (mut reader, request) = tokio::select! {
                    () = server_cancellation.cancelled() => break,
                    accepted = accept_wire_request(&listener) => accepted,
                };
                match request["method"].as_str() {
                    Some("events.subscribe") => {
                        write_wire_frame(
                            &mut reader,
                            &json!({
                                "id": request["id"],
                                "result": {"type": "subscription_started"},
                            }),
                        )
                        .await;
                        if !primary_subscription_is_scoped(&request) {
                            observed_primary_subscriptions.fetch_add(1, Ordering::Release);
                        }
                        held_streams.push(reader);
                    }
                    Some("session.snapshot") => {
                        snapshots += 1;
                        write_wire_frame(&mut reader, &watchdog_ambiguous_snapshot_frame(&request))
                            .await;
                        if snapshots == 4
                            && let Some(sender) = probed_sender.take()
                        {
                            sender.send(()).unwrap();
                        }
                    }
                    method => panic!("unexpected ambiguous watchdog request: {method:?}"),
                }
            }
            drop(held_streams);
        });

        tokio::time::timeout(Duration::from_secs(3), probed_receiver)
            .await
            .expect("ambiguous topology was not probed repeatedly")
            .expect("ambiguous probe observer was dropped");
        tokio::time::timeout(Duration::from_secs(3), async {
            while harness
                .primary_stream_diagnostics
                .snapshot()
                .inconclusive_topology_probes
                < 3
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ambiguous topology probes did not increment their diagnostic counter");
        assert!(
            harness
                .primary_stream_diagnostics
                .snapshot()
                .inconclusive_topology_probes
                >= 3,
            "ambiguous topology probe counter did not reach the observed threshold"
        );
        assert_eq!(
            primary_subscriptions.load(Ordering::Acquire),
            1,
            "inconclusive probes must keep the primary subscription alive"
        );
        assert_eq!(reconnect_gap_count(&root), 0);

        let contents = harness.stop().await;
        join_fake_server(server, "ambiguous watchdog probes").await;
        assert_eq!(reconnect_gap_count(&root), 0, "collector log: {contents}");
        assert!(
            !contents.contains("warning_code=\"herdr_primary_stream_watchdog_silence\""),
            "inconclusive probes emitted a watchdog warning: {contents}"
        );
    }

    #[tokio::test]
    async fn watchdog_divergent_probe_reconnects_and_records_gap() {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let socket = directory.path().join("starved-herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (reconnected_sender, reconnected_receiver) = tokio::sync::oneshot::channel();
        let harness = spawn_primary_collector_harness_with_policy(
            &directory,
            socket,
            "starved-watchdog.log",
            LivenessPolicy { timeout_ms: 80 },
        );
        let server_cancellation = harness.cancellation.clone();
        let probe_preceded_reconnect = Arc::new(AtomicBool::new(false));
        let observed_probe_order = Arc::clone(&probe_preceded_reconnect);
        let primary_subscriptions = Arc::new(AtomicUsize::new(0));
        let observed_primary_subscriptions = Arc::clone(&primary_subscriptions);
        let server = tokio::spawn(async move {
            let mut primary_subscription_count = 0;
            let mut snapshots = 0;
            let mut reconnected_sender = Some(reconnected_sender);
            let mut held_streams = Vec::new();
            loop {
                let (mut reader, request) = tokio::select! {
                    () = server_cancellation.cancelled() => break,
                    accepted = accept_wire_request(&listener) => accepted,
                };
                match request["method"].as_str() {
                    Some("events.subscribe") => {
                        write_wire_frame(
                            &mut reader,
                            &json!({
                                "id": request["id"],
                                "result": {"type": "subscription_started"},
                            }),
                        )
                        .await;
                        if !primary_subscription_is_scoped(&request) {
                            primary_subscription_count += 1;
                            observed_primary_subscriptions
                                .store(primary_subscription_count, Ordering::Release);
                            if primary_subscription_count == 2
                                && let Some(sender) = reconnected_sender.take()
                            {
                                sender.send(()).unwrap();
                            }
                        }
                        held_streams.push(reader);
                    }
                    Some("session.snapshot") => {
                        snapshots += 1;
                        if snapshots == 2 && primary_subscription_count == 1 {
                            observed_probe_order.store(true, Ordering::Release);
                        }
                        write_wire_frame(
                            &mut reader,
                            &watchdog_snapshot_frame(&request, snapshots >= 2, "claude"),
                        )
                        .await;
                    }
                    method => panic!("unexpected starved watchdog request: {method:?}"),
                }
            }
            drop(held_streams);
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while !probe_preceded_reconnect.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("divergent topology was not observed by a snapshot probe");
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            primary_subscriptions.load(Ordering::Acquire),
            1,
            "divergent probe reconnect skipped the first backoff delay"
        );
        tokio::time::timeout(Duration::from_secs(3), reconnected_receiver)
            .await
            .expect("divergent snapshot probe did not trigger a reconnect")
            .expect("starved reconnect observer was dropped");
        assert!(
            probe_preceded_reconnect.load(Ordering::Acquire),
            "snapshot probe was not issued before reconnecting the silent subscription"
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while harness.model.borrow().pane("w1:p5").is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reconnect snapshot did not reconcile divergent topology");

        let contents = harness.stop().await;
        join_fake_server(server, "starved watchdog reconnect").await;
        assert_eq!(reconnect_gap_count(&root), 1, "collector log: {contents}");
        assert!(
            contents.contains("warning_code=\"herdr_primary_stream_watchdog_silence\""),
            "starved subscription did not emit the watchdog warning: {contents}"
        );
    }

    #[tokio::test]
    async fn watchdog_failed_probe_reconnects_and_records_gap() {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let socket = directory.path().join("dead-herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (reconnected_sender, reconnected_receiver) = tokio::sync::oneshot::channel();
        let harness = spawn_primary_collector_harness_with_policy(
            &directory,
            socket,
            "dead-watchdog.log",
            LivenessPolicy { timeout_ms: 80 },
        );
        let server_cancellation = harness.cancellation.clone();
        let failed_probe_preceded_reconnect = Arc::new(AtomicBool::new(false));
        let observed_failed_probe = Arc::clone(&failed_probe_preceded_reconnect);
        let server = tokio::spawn(async move {
            let mut primary_subscriptions = 0;
            let mut snapshots = 0;
            let mut reconnected_sender = Some(reconnected_sender);
            let mut held_streams = Vec::new();
            loop {
                let (mut reader, request) = tokio::select! {
                    () = server_cancellation.cancelled() => break,
                    accepted = accept_wire_request(&listener) => accepted,
                };
                match request["method"].as_str() {
                    Some("events.subscribe") => {
                        write_wire_frame(
                            &mut reader,
                            &json!({
                                "id": request["id"],
                                "result": {"type": "subscription_started"},
                            }),
                        )
                        .await;
                        if !primary_subscription_is_scoped(&request) {
                            primary_subscriptions += 1;
                            if primary_subscriptions == 2
                                && let Some(sender) = reconnected_sender.take()
                            {
                                sender.send(()).unwrap();
                            }
                        }
                        held_streams.push(reader);
                    }
                    Some("session.snapshot") => {
                        snapshots += 1;
                        if snapshots == 2 {
                            if primary_subscriptions == 1 {
                                observed_failed_probe.store(true, Ordering::Release);
                            }
                            write_wire_frame(
                                &mut reader,
                                &json!({
                                    "id": request["id"],
                                    "error": {
                                        "code": "unavailable",
                                        "message": "probe failed",
                                    },
                                }),
                            )
                            .await;
                        } else {
                            write_wire_frame(
                                &mut reader,
                                &watchdog_snapshot_frame(&request, false, "claude"),
                            )
                            .await;
                        }
                    }
                    method => panic!("unexpected dead watchdog request: {method:?}"),
                }
            }
            drop(held_streams);
        });

        tokio::time::timeout(Duration::from_secs(3), reconnected_receiver)
            .await
            .expect("failed snapshot probe did not trigger a reconnect")
            .expect("dead reconnect observer was dropped");
        assert!(
            failed_probe_preceded_reconnect.load(Ordering::Acquire),
            "failed snapshot request was not a pre-reconnect probe"
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            while reconnect_gap_count(&root) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed probe reconnect did not record a gap");

        let contents = harness.stop().await;
        join_fake_server(server, "dead watchdog reconnect").await;
        assert_eq!(reconnect_gap_count(&root), 1, "collector log: {contents}");
    }

    #[tokio::test]
    async fn watchdog_nonresponsive_probe_times_out_reconnects_and_records_one_gap() {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let socket = directory.path().join("nonresponsive-herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (reconnected_sender, reconnected_receiver) = tokio::sync::oneshot::channel();
        let harness = spawn_primary_collector_harness_with_policy(
            &directory,
            socket,
            "nonresponsive-watchdog.log",
            LivenessPolicy { timeout_ms: 120 },
        );
        let server_cancellation = harness.cancellation.clone();
        let pulse_cancellation = harness.cancellation.clone();
        let provider_events = harness.provider_events.clone();
        let pulse = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(25));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = pulse_cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        let event = ProviderIngressEvent {
                            event: ProviderEvent::SourceState {
                                provider: Provider::Claude,
                                state: ProviderSourceState::NotApplicable,
                            },
                            admission: None,
                        };
                        let sent = tokio::time::timeout(
                            Duration::from_secs(1),
                            provider_events.send(event),
                        )
                        .await;
                        if !matches!(sent, Ok(Ok(()))) {
                            break;
                        }
                    }
                }
            }
        });
        let server = tokio::spawn(async move {
            let mut primary_subscriptions = 0;
            let mut snapshots = 0;
            let mut reconnected_sender = Some(reconnected_sender);
            let mut held_streams = Vec::new();
            let mut nonresponsive_probes = Vec::new();
            loop {
                let (mut reader, request) = tokio::select! {
                    () = server_cancellation.cancelled() => break,
                    accepted = accept_wire_request(&listener) => accepted,
                };
                match request["method"].as_str() {
                    Some("events.subscribe") => {
                        write_wire_frame(
                            &mut reader,
                            &json!({
                                "id": request["id"],
                                "result": {"type": "subscription_started"},
                            }),
                        )
                        .await;
                        if !primary_subscription_is_scoped(&request) {
                            primary_subscriptions += 1;
                            if primary_subscriptions == 2
                                && let Some(sender) = reconnected_sender.take()
                            {
                                sender.send(()).unwrap();
                            }
                        }
                        held_streams.push(reader);
                    }
                    Some("session.snapshot") => {
                        snapshots += 1;
                        if snapshots == 2 && primary_subscriptions == 1 {
                            nonresponsive_probes.push(reader);
                        } else {
                            write_wire_frame(
                                &mut reader,
                                &watchdog_snapshot_frame(&request, false, "claude"),
                            )
                            .await;
                        }
                    }
                    method => panic!("unexpected nonresponsive watchdog request: {method:?}"),
                }
            }
            drop(nonresponsive_probes);
            drop(held_streams);
        });

        tokio::time::timeout(Duration::from_secs(3), reconnected_receiver)
            .await
            .expect("nonresponsive snapshot probe did not time out and reconnect")
            .expect("nonresponsive reconnect observer was dropped");
        tokio::time::timeout(Duration::from_secs(3), async {
            while reconnect_gap_count(&root) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("nonresponsive probe reconnect did not record a gap");

        let contents = harness.stop().await;
        tokio::time::timeout(Duration::from_secs(3), pulse)
            .await
            .expect("provider pulse task did not stop")
            .expect("provider pulse task panicked");
        join_fake_server(server, "nonresponsive watchdog reconnect").await;
        assert_eq!(reconnect_gap_count(&root), 1, "collector log: {contents}");
        assert!(
            contents.contains("warning_code=\"herdr_primary_stream_watchdog_silence\""),
            "nonresponsive subscription did not emit the watchdog warning: {contents}"
        );
        assert!(
            contents.contains("reason=\"snapshot_probe_failed\""),
            "nonresponsive warning did not report probe failure: {contents}"
        );
    }

    async fn capture_primary_subscribe_recovery(log_name: &str) -> String {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (allow_success, success_allowed) = tokio::sync::oneshot::channel();
        let mut harness = spawn_primary_collector_harness(&directory, socket, log_name);
        let server_cancellation = harness.cancellation.clone();
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut reader, request) = accept_wire_request(&listener).await;
                write_wire_frame(
                    &mut reader,
                    &json!({
                        "id": request["id"],
                        "error": {"code": "unavailable", "message": "temporarily offline"},
                    }),
                )
                .await;
            }
            let (mut reader, request) = accept_wire_request(&listener).await;
            tokio::time::timeout(Duration::from_secs(3), success_allowed)
                .await
                .expect("primary recovery success was not allowed")
                .expect("primary recovery success gate was dropped");
            write_wire_frame(
                &mut reader,
                &json!({
                    "id": request["id"],
                    "result": {"type": "subscription_started"},
                }),
            )
            .await;
            wait_for_server_cancellation(&server_cancellation).await;
        });

        wait_for_quality(
            &mut harness.source_quality,
            ObservationQuality::Disconnected,
            "primary failure edge",
        )
        .await;
        allow_success
            .send(())
            .expect("primary recovery server stopped before success was allowed");
        wait_for_quality(
            &mut harness.source_quality,
            ObservationQuality::Reconciling,
            "primary recovery publication barrier",
        )
        .await;
        let contents = harness.stop().await;
        join_fake_server(server, "primary recovery").await;
        contents
    }

    #[tokio::test]
    async fn a3_primary_subscribe_failures_warn_once_across_three_retries() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut reader, request) = accept_wire_request(&listener).await;
                assert_eq!(request["method"], "events.subscribe");
                write_wire_frame(
                    &mut reader,
                    &json!({
                        "id": request["id"],
                        "result": {"type": "not_subscription_started"},
                    }),
                )
                .await;
                wait_for_wire_peer_close(&mut reader).await;
                server_attempts.fetch_add(1, Ordering::Release);
            }
        });
        let harness =
            spawn_primary_collector_harness(&directory, socket, "primary-failures-once.log");

        wait_for_attempts(&attempts, 3, "primary subscribe failures").await;
        let contents = harness.stop().await;
        join_fake_server(server, "primary subscribe failures").await;

        assert_eq!(
            contents
                .matches("warning_code=\"herdr_subscription_failed\"")
                .count(),
            1,
            "primary subscribe failures must log once per failed edge: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_primary_decorated_server_rejection_warns_once_and_keeps_retrying() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut reader, request) = accept_wire_request(&listener).await;
                assert_eq!(request["method"], "events.subscribe");
                let request_id = request["id"]
                    .as_str()
                    .expect("subscribe request id was not a string");
                write_wire_frame(
                    &mut reader,
                    &json!({
                        "id": format!("{request_id}:pane-1"),
                        "error": {
                            "code": "pane_not_found",
                            "message": "pane pane-1 not found",
                        },
                    }),
                )
                .await;
                wait_for_wire_peer_close(&mut reader).await;
                server_attempts.fetch_add(1, Ordering::Release);
            }
        });
        let harness =
            spawn_primary_collector_harness(&directory, socket, "primary-server-rejection.log");

        wait_for_attempts(&attempts, 3, "decorated server rejection retries").await;
        let contents = harness.stop().await;
        join_fake_server(server, "decorated server rejection retries").await;

        assert!(
            attempts.load(Ordering::Acquire) >= 3,
            "collector did not continue retrying after WireError::Server"
        );
        assert!(
            contents.contains("warning_code=\"herdr_subscription_failed\""),
            "decorated server rejection did not log the warning code: {contents}"
        );
        assert!(
            contents.contains("herdr server error pane_not_found: pane pane-1 not found"),
            "decorated server rejection did not log WireError::Server Display: {contents}"
        );
        assert_eq!(
            contents
                .matches("warning_code=\"herdr_subscription_failed\"")
                .count(),
            1,
            "decorated server rejections must log once while retries continue: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_primary_subscribe_recovery_logs_one_notice() {
        let contents = capture_primary_subscribe_recovery("primary-recovery.log").await;

        assert_eq!(
            contents
                .matches("notice_code=\"herdr_subscription_recovered\"")
                .count(),
            1,
            "primary failed-to-healthy transition must log one recovery notice: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_recovery_notice_survives_production_warn_level_cap() {
        let contents = capture_primary_subscribe_recovery("primary-warn-level-recovery.log").await;

        assert_eq!(
            contents
                .matches("notice_code=\"herdr_subscription_recovered\"")
                .count(),
            1,
            "the recovery notice must survive the production WARN level cap: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_enrichment_unextractable_rejections_warn_once_across_three_retries() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut reader, request) = accept_wire_request(&listener).await;
                write_wire_frame(
                    &mut reader,
                    &json!({
                        "id": request["id"],
                        "error": {
                            "code": "pane_not_found",
                            "message": "missing pane without a parseable id",
                        },
                    }),
                )
                .await;
                wait_for_wire_peer_close(&mut reader).await;
                server_attempts.fetch_add(1, Ordering::Release);
            }
        });
        let harness = spawn_enrichment_reader_harness(
            &directory,
            socket,
            "enrichment-unextractable-rejection.log",
        );

        wait_for_attempts(&attempts, 3, "unextractable enrichment rejections").await;
        let contents = harness.stop().await;
        join_fake_server(server, "unextractable enrichment rejections").await;

        assert_eq!(
            contents
                .matches("warning_code=\"herdr_enrichment_subscription_failed\"")
                .count(),
            1,
            "unextractable enrichment rejections must log once per failed edge: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_enrichment_generic_subscribe_failures_warn_once_across_three_retries() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut reader, request) = accept_wire_request(&listener).await;
                write_wire_frame(
                    &mut reader,
                    &json!({
                        "id": request["id"],
                        "error": {"code": "busy", "message": "try again"},
                    }),
                )
                .await;
                wait_for_wire_peer_close(&mut reader).await;
                server_attempts.fetch_add(1, Ordering::Release);
            }
        });
        let harness =
            spawn_enrichment_reader_harness(&directory, socket, "enrichment-generic-failure.log");

        wait_for_attempts(&attempts, 3, "generic enrichment subscribe failures").await;
        let contents = harness.stop().await;
        join_fake_server(server, "generic enrichment subscribe failures").await;

        assert_eq!(
            contents
                .matches("warning_code=\"herdr_enrichment_subscription_failed\"")
                .count(),
            1,
            "generic enrichment failures must log once per failed edge: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_enrichment_subscribe_recovery_logs_one_notice() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let mut harness = spawn_enrichment_reader_harness(
            &directory,
            socket,
            "enrichment-subscribe-recovery.log",
        );
        let server_cancellation = harness.cancellation.clone();
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut reader, request) = accept_wire_request(&listener).await;
                write_wire_frame(
                    &mut reader,
                    &json!({
                        "id": request["id"],
                        "error": {"code": "busy", "message": "try again"},
                    }),
                )
                .await;
            }
            let (mut reader, request) = accept_wire_request(&listener).await;
            write_wire_frame(
                &mut reader,
                &json!({
                    "id": request["id"],
                    "result": {"type": "subscription_started"},
                }),
            )
            .await;
            write_wire_frame(
                &mut reader,
                &json!({
                    "event": "pane_agent_status_changed",
                    "data": {
                        "pane_id": "pane-1",
                        "terminal_id": "terminal-1",
                        "agent_status": "working",
                    },
                }),
            )
            .await;
            wait_for_server_cancellation(&server_cancellation).await;
        });

        let payload = tokio::time::timeout(Duration::from_secs(3), harness.events.recv())
            .await
            .expect("enrichment recovery event was not delivered")
            .expect("enrichment recovery event channel closed");
        assert_eq!(payload.pane_id, "pane-1");
        let contents = harness.stop().await;
        join_fake_server(server, "enrichment subscribe recovery").await;

        assert_eq!(
            contents
                .matches("notice_code=\"herdr_enrichment_subscription_recovered\"")
                .count(),
            1,
            "enrichment failed-to-healthy subscribe transition must log one notice: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_enrichment_subscription_health_persists_across_reader_generations() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = Arc::new(tokio::net::UnixListener::bind(&socket).unwrap());
        let log_path = directory.path().join("enrichment-cross-generation.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(log)
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let health = Arc::new(EnrichmentHealth::default());

        let first_attempts = Arc::new(AtomicUsize::new(0));
        let first_server_attempts = Arc::clone(&first_attempts);
        let first_listener = Arc::clone(&listener);
        let first_cancellation = CancellationToken::new();
        let first_server_cancellation = first_cancellation.clone();
        let first_server = tokio::spawn(async move {
            let (mut reader, request) = accept_wire_request(&first_listener).await;
            write_wire_frame(
                &mut reader,
                &json!({
                    "id": request["id"],
                    "error": {"code": "busy", "message": "generation one outage"},
                }),
            )
            .await;
            wait_for_wire_peer_close(&mut reader).await;
            first_server_attempts.fetch_add(1, Ordering::Release);
            first_server_cancellation.cancel();
        });
        let (first_targets, first_target_receiver) =
            watch::channel(BTreeSet::from(["pane-1".to_owned()]));
        let (first_sender, first_events) = mpsc::channel(ENRICHMENT_QUEUE_CAPACITY);
        let (first_prune_sender, first_prunes) = mpsc::unbounded_channel();
        let first_task_cancellation = first_cancellation.clone();
        let first_task = tokio::spawn(
            run_enrichment_reader(
                socket.clone(),
                first_target_receiver,
                first_sender,
                first_prune_sender,
                EnrichmentDiagnosticsHandle::default(),
                Arc::clone(&health),
                first_task_cancellation,
            )
            .with_subscriber(dispatch.clone()),
        );

        wait_for_attempts(&first_attempts, 1, "first enrichment reader generation").await;
        tokio::time::timeout(Duration::from_secs(3), first_task)
            .await
            .expect("first enrichment reader generation did not stop")
            .expect("first enrichment reader generation panicked");
        join_fake_server(first_server, "first enrichment reader generation").await;
        drop(first_targets);
        drop(first_events);
        drop(first_prunes);

        let second_attempts = Arc::new(AtomicUsize::new(0));
        let second_server_attempts = Arc::clone(&second_attempts);
        let second_listener = Arc::clone(&listener);
        let second_cancellation = CancellationToken::new();
        let second_server_cancellation = second_cancellation.clone();
        let second_server = tokio::spawn(async move {
            let (mut failed_reader, failed_request) = accept_wire_request(&second_listener).await;
            write_wire_frame(
                &mut failed_reader,
                &json!({
                    "id": failed_request["id"],
                    "error": {"code": "busy", "message": "generation two outage"},
                }),
            )
            .await;
            wait_for_wire_peer_close(&mut failed_reader).await;
            second_server_attempts.fetch_add(1, Ordering::Release);

            let (mut recovered_reader, recovered_request) =
                accept_wire_request(&second_listener).await;
            write_wire_frame(
                &mut recovered_reader,
                &json!({
                    "id": recovered_request["id"],
                    "result": {"type": "subscription_started"},
                }),
            )
            .await;
            write_wire_frame(
                &mut recovered_reader,
                &json!({
                    "event": "pane_agent_status_changed",
                    "data": {
                        "pane_id": "pane-1",
                        "terminal_id": "terminal-1",
                        "agent_status": "working",
                    },
                }),
            )
            .await;
            wait_for_server_cancellation(&second_server_cancellation).await;
        });
        let (second_targets, second_target_receiver) =
            watch::channel(BTreeSet::from(["pane-1".to_owned()]));
        let (second_sender, mut second_events) = mpsc::channel(ENRICHMENT_QUEUE_CAPACITY);
        let (second_prune_sender, second_prunes) = mpsc::unbounded_channel();
        let second_task_cancellation = second_cancellation.clone();
        let second_task = tokio::spawn(
            run_enrichment_reader(
                socket,
                second_target_receiver,
                second_sender,
                second_prune_sender,
                EnrichmentDiagnosticsHandle::default(),
                Arc::clone(&health),
                second_task_cancellation,
            )
            .with_subscriber(dispatch.clone()),
        );

        let payload = tokio::time::timeout(Duration::from_secs(3), second_events.recv())
            .await
            .expect("later-generation recovery event was not delivered")
            .expect("later-generation recovery event channel closed");
        assert_eq!(payload.pane_id, "pane-1");
        wait_for_attempts(&second_attempts, 1, "second enrichment reader generation").await;
        second_cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(3), second_task)
            .await
            .expect("second enrichment reader generation did not stop")
            .expect("second enrichment reader generation panicked");
        join_fake_server(second_server, "second enrichment reader generation").await;
        drop(second_targets);
        drop(second_events);
        drop(second_prunes);
        drop(dispatch);

        let contents = std::fs::read_to_string(log_path).unwrap();
        assert!(
            contents.contains("generation one outage"),
            "the first generation's uniquely marked log line was lost: {contents}"
        );
        assert_eq!(
            contents
                .matches("warning_code=\"herdr_enrichment_subscription_failed\"")
                .count(),
            1,
            "a persistent outage must warn once across reader generations: {contents}"
        );
        assert_eq!(
            contents
                .matches("notice_code=\"herdr_enrichment_subscription_recovered\"")
                .count(),
            1,
            "a later-generation recovery must log one notice: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_enrichment_stream_failures_warn_once_across_three_retries() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut reader, request) = accept_wire_request(&listener).await;
                write_wire_frame(
                    &mut reader,
                    &json!({
                        "id": request["id"],
                        "result": {"type": "subscription_started"},
                    }),
                )
                .await;
                write_malformed_wire_event(&mut reader).await;
                wait_for_wire_peer_close(&mut reader).await;
                server_attempts.fetch_add(1, Ordering::Release);
            }
        });
        let harness =
            spawn_enrichment_reader_harness(&directory, socket, "enrichment-stream-failures.log");

        wait_for_attempts(&attempts, 3, "enrichment stream failures").await;
        let contents = harness.stop().await;
        join_fake_server(server, "enrichment stream failures").await;

        assert_eq!(
            contents
                .matches("warning_code=\"herdr_enrichment_stream_failed\"")
                .count(),
            1,
            "enrichment stream failures must log once per failed edge: {contents}"
        );
        assert_eq!(
            contents
                .matches("notice_code=\"herdr_enrichment_subscription_recovered\"")
                .count(),
            0,
            "successful subscribes without a preceding failure must not log recovery: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_enrichment_clean_stream_eof_warns_once_then_recovers() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let mut harness =
            spawn_enrichment_reader_harness(&directory, socket, "enrichment-clean-stream-eof.log");
        let server_cancellation = harness.cancellation.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut reader, request) = accept_wire_request(&listener).await;
                write_wire_frame(
                    &mut reader,
                    &json!({
                        "id": request["id"],
                        "result": {"type": "subscription_started"},
                    }),
                )
                .await;
                reader
                    .get_mut()
                    .shutdown()
                    .await
                    .expect("fake Herdr listener failed to close the stream cleanly");
                wait_for_wire_peer_close(&mut reader).await;
                server_attempts.fetch_add(1, Ordering::Release);
            }

            let (mut recovered_reader, recovered_request) = accept_wire_request(&listener).await;
            write_wire_frame(
                &mut recovered_reader,
                &json!({
                    "id": recovered_request["id"],
                    "result": {"type": "subscription_started"},
                }),
            )
            .await;
            write_wire_frame(
                &mut recovered_reader,
                &json!({
                    "event": "pane_agent_status_changed",
                    "data": {
                        "pane_id": "pane-1",
                        "terminal_id": "terminal-1",
                        "agent_status": "working",
                    },
                }),
            )
            .await;
            wait_for_server_cancellation(&server_cancellation).await;
        });

        wait_for_attempts(&attempts, 3, "clean enrichment stream closes").await;
        let payload = tokio::time::timeout(Duration::from_secs(3), harness.events.recv())
            .await
            .expect("valid post-EOF enrichment event was not delivered")
            .expect("enrichment event channel closed before clean-EOF recovery");
        assert_eq!(payload.pane_id, "pane-1");
        let contents = harness.stop().await;
        join_fake_server(server, "clean enrichment stream EOF recovery").await;

        assert_eq!(
            contents
                .matches("warning_code=\"herdr_enrichment_stream_failed\"")
                .count(),
            1,
            "clean enrichment stream closes must warn once per failed edge: {contents}"
        );
        assert_eq!(
            contents
                .matches("notice_code=\"herdr_enrichment_stream_recovered\"")
                .count(),
            1,
            "the first valid event after clean stream EOF must log one recovery notice: {contents}"
        );
    }

    #[tokio::test]
    async fn a3_enrichment_stream_recovery_logs_after_valid_event() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let mut harness =
            spawn_enrichment_reader_harness(&directory, socket, "enrichment-stream-recovery.log");
        let server_cancellation = harness.cancellation.clone();
        let server = tokio::spawn(async move {
            let (mut failed_reader, failed_request) = accept_wire_request(&listener).await;
            write_wire_frame(
                &mut failed_reader,
                &json!({
                    "id": failed_request["id"],
                    "result": {"type": "subscription_started"},
                }),
            )
            .await;
            write_malformed_wire_event(&mut failed_reader).await;
            drop(failed_reader);

            let (mut recovered_reader, recovered_request) = accept_wire_request(&listener).await;
            write_wire_frame(
                &mut recovered_reader,
                &json!({
                    "id": recovered_request["id"],
                    "result": {"type": "subscription_started"},
                }),
            )
            .await;
            write_wire_frame(
                &mut recovered_reader,
                &json!({
                    "event": "pane_agent_status_changed",
                    "data": {
                        "pane_id": "pane-1",
                        "terminal_id": "terminal-1",
                        "agent_status": "working",
                    },
                }),
            )
            .await;
            wait_for_server_cancellation(&server_cancellation).await;
        });

        let payload = tokio::time::timeout(Duration::from_secs(3), harness.events.recv())
            .await
            .expect("valid post-failure enrichment event was not delivered")
            .expect("enrichment event channel closed before stream recovery");
        assert_eq!(payload.pane_id, "pane-1");
        let contents = harness.stop().await;
        join_fake_server(server, "enrichment stream recovery").await;

        assert_eq!(
            contents
                .matches("warning_code=\"herdr_enrichment_stream_failed\"")
                .count(),
            1,
            "the initial malformed stream must log one warning: {contents}"
        );
        assert_eq!(
            contents
                .matches("notice_code=\"herdr_enrichment_stream_recovered\"")
                .count(),
            1,
            "the first valid event after a stream failure must log one recovery notice: {contents}"
        );
    }

    #[test]
    fn health_edge_reports_only_failure_and_recovery_transitions() {
        let edge = HealthEdge::default();

        assert!(edge.record_failure());
        assert!(!edge.record_failure());
        assert!(edge.record_recovery());
        assert!(!edge.record_recovery());
    }

    #[test]
    fn enrichment_deferral_keeps_quiet_reconnect_delay() {
        let base = tokio::time::Instant::now();
        let mut deferral = EnrichmentDeferral { first: None };

        assert_eq!(deferral.on_change(base), base + Duration::from_millis(50));
    }

    #[test]
    fn enrichment_deferral_clamps_twenty_changes_to_first_change_budget() {
        let base = tokio::time::Instant::now();
        let mut deferral = EnrichmentDeferral { first: None };
        let expected_deadlines_ms = [
            50, 90, 130, 170, 210, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250, 250,
            250, 250,
        ];

        for (index, expected_deadline_ms) in expected_deadlines_ms.into_iter().enumerate() {
            let now = base + Duration::from_millis(index as u64 * 40);
            assert_eq!(
                deferral.on_change(now),
                base + Duration::from_millis(expected_deadline_ms)
            );
        }
    }

    #[test]
    fn enrichment_deferral_swap_resets_anchor_and_budget() {
        let base = tokio::time::Instant::now();
        let mut deferral = EnrichmentDeferral { first: None };
        assert_eq!(deferral.on_change(base), base + Duration::from_millis(50));
        assert_eq!(
            deferral.on_change(base + Duration::from_millis(400)),
            base + Duration::from_millis(250)
        );

        deferral.on_swap();
        let next = base + Duration::from_millis(800);
        assert_eq!(deferral.on_change(next), next + Duration::from_millis(50));
        assert_eq!(
            deferral.on_change(next + Duration::from_millis(240)),
            next + Duration::from_millis(250)
        );
    }

    #[test]
    fn enrichment_deferral_empty_target_resets_anchor_and_budget() {
        let base = tokio::time::Instant::now();
        let mut deferral = EnrichmentDeferral { first: None };
        assert_eq!(deferral.on_change(base), base + Duration::from_millis(50));
        assert_eq!(
            deferral.on_change(base + Duration::from_millis(400)),
            base + Duration::from_millis(250)
        );

        deferral.on_empty();
        let next = base + Duration::from_millis(800);
        assert_eq!(deferral.on_change(next), next + Duration::from_millis(50));
        assert_eq!(
            deferral.on_change(next + Duration::from_millis(240)),
            next + Duration::from_millis(250)
        );
    }

    #[test]
    fn enrichment_deferral_reanchors_after_resolved_stream_swap() {
        let base = tokio::time::Instant::now();
        let mut deferral = EnrichmentDeferral { first: None };
        assert_eq!(deferral.on_change(base), base + Duration::from_millis(50));
        assert_eq!(
            deferral.on_change(base + Duration::from_millis(600)),
            base + Duration::from_millis(250)
        );

        deferral.on_swap();
        let later_change = base + Duration::from_millis(1_000);
        assert_eq!(
            deferral.on_change(later_change),
            later_change + Duration::from_millis(50)
        );
        assert_eq!(
            deferral.on_change(later_change + Duration::from_millis(400)),
            later_change + Duration::from_millis(250)
        );
    }

    fn status_model(states: &[(&str, ExecState)]) -> SharedModel {
        let mut model = DomainModel::default();
        for (index, (execution_id, state)) in states.iter().enumerate() {
            let run_id = RunId::new();
            model.insert_task_run(TaskRun {
                run_id,
                key: RunKey::Controller(format!("status-run-{index}")),
                display_ordinal: DisplayOrdinal::new(i64::try_from(index + 1).unwrap()),
                state: TaskState::Running,
                has_controller_task_state_event: false,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            });
            model.insert_execution(Execution {
                execution_id: (*execution_id).to_owned(),
                pane_id: "w1:p1".to_owned(),
                terminal_id: "terminal-1".to_owned(),
                task_run_id: run_id,
                state: state.clone(),
            });
        }
        watch::channel(Arc::new(model)).1
    }

    fn known_pane_model() -> SharedModel {
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "w1".to_owned(),
        });
        model.insert_tab(Tab {
            tab_id: "w1:t1".to_owned(),
            workspace_id: "w1".to_owned(),
            label: None,
        });
        model.insert_pane(Pane {
            pane_id: "w1:p4".to_owned(),
            workspace_id: "w1".to_owned(),
            tab_id: "w1:t1".to_owned(),
            terminal_id: "terminal-4".to_owned(),
            display_name: None,
        });
        watch::channel(Arc::new(model)).1
    }

    fn pane_created_with_names(
        label: Option<&str>,
        terminal_title_stripped: Option<&str>,
    ) -> ReceivedEvent {
        ReceivedEvent {
            event: "pane_created".to_owned(),
            data: json!({
                "pane": {
                    "pane_id": "w1:p4",
                    "terminal_id": "terminal-4",
                    "workspace_id": "w1",
                    "tab_id": "w1:t1",
                    "terminal_title_stripped": terminal_title_stripped,
                    "label": label,
                },
            }),
            primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle::default(),
        }
    }

    fn normalized_pane_display_name(
        label: Option<&str>,
        terminal_title_stripped: Option<&str>,
    ) -> Option<String> {
        let shared = watch::channel(Arc::new(DomainModel::default())).1;
        let normalized = normalize_event(
            &shared,
            "name-session",
            &pane_created_with_names(label, terminal_title_stripped),
        )
        .expect("pane_created should normalize");
        let Some(NormalizedEvent::TopologyUpsert {
            entity: TopologyEntity::Pane(pane),
            ..
        }) = normalized.first()
        else {
            panic!("pane_created must emit a pane topology upsert first");
        };
        pane.display_name.clone()
    }

    #[test]
    fn live_pane_upserts_capture_sanitized_display_names() {
        assert_eq!(
            normalized_pane_display_name(Some("UI修正"), None).as_deref(),
            Some("UI修正")
        );
        assert_eq!(
            normalized_pane_display_name(None, Some("build")).as_deref(),
            Some("build")
        );
        assert_eq!(
            normalized_pane_display_name(Some("label wins"), Some("ignored title")).as_deref(),
            Some("label wins")
        );
        assert_eq!(
            normalized_pane_display_name(Some(""), Some("fallback title")).as_deref(),
            Some("fallback title")
        );
        assert_eq!(normalized_pane_display_name(None, None), None);

        let long_name = "界".repeat(100);
        let captured = normalized_pane_display_name(Some(&long_name), None)
            .expect("a non-empty pane label should be captured");
        assert!(captured.len() <= 256);
        assert!(std::str::from_utf8(captured.as_bytes()).is_ok());
        assert!(captured.chars().all(|character| character == '界'));
    }

    #[test]
    fn tab_created_captures_sanitized_label() {
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "w1".to_owned(),
        });
        let (mut reducer, shared) = Reducer::new(RestoredState {
            model,
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let received = ReceivedEvent {
            event: "tab_created".to_owned(),
            data: json!({
                "tab": {
                    "tab_id": "w1:t1",
                    "workspace_id": "w1",
                    "label": "レビュー",
                },
            }),
            primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle::default(),
        };

        let normalized = normalize_event(&shared, "name-session", &received).unwrap();
        let persist = apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("tab upsert should apply");

        assert_eq!(
            shared.borrow().tab("w1:t1").unwrap().label.as_deref(),
            Some("レビュー")
        );
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTab { tab, .. }
                if tab.tab_id == "w1:t1" && tab.label.as_deref() == Some("レビュー")
        )));
    }

    fn tab_renamed_event(tab_id: &str, label: Option<&str>) -> ReceivedEvent {
        let mut data = json!({
            "type": "tab_renamed",
            "tab_id": tab_id,
            "workspace_id": "untrusted-workspace",
        });
        if let Some(label) = label {
            data["label"] = Value::String(label.to_owned());
        }
        ReceivedEvent {
            event: "tab_renamed".to_owned(),
            data,
            primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle::default(),
        }
    }

    fn assert_authoritative_tab_clear_survives_restart(label: Option<&str>) {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![
                PersistOp::UpsertWorkspace {
                    workspace: Workspace {
                        workspace_id: "w1".to_owned(),
                    },
                    display_ordinal: DisplayOrdinal::new(1),
                },
                PersistOp::UpsertTab {
                    tab: Tab {
                        tab_id: "w1:t1".to_owned(),
                        workspace_id: "w1".to_owned(),
                        label: Some("persisted label".to_owned()),
                    },
                    display_ordinal: DisplayOrdinal::new(2),
                },
            ])
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (mut reducer, shared) = Reducer::new(restored);

        let normalized = normalize_event(
            &shared,
            "authoritative-clear-session",
            &tab_renamed_event("w1:t1", label),
        )
        .unwrap();
        let persist = apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("known tab rename should apply");
        assert_eq!(shared.borrow().tab("w1:t1").unwrap().label, None);

        store.apply_batch(persist).unwrap();
        let restored = store.load_restored_state().unwrap();
        assert_eq!(restored.model.tab("w1:t1").unwrap().label, None);
    }

    #[test]
    fn tab_rename_with_empty_label_clears_store_across_restart() {
        assert_authoritative_tab_clear_survives_restart(Some(""));
    }

    #[test]
    fn tab_rename_without_label_clears_store_across_restart() {
        assert_authoritative_tab_clear_survives_restart(None);
    }

    #[test]
    fn tab_renamed_updates_existing_label_clears_empty_and_ignores_unknown_tabs() {
        let mut model = DomainModel::default();
        model.insert_workspace(Workspace {
            workspace_id: "w1".to_owned(),
        });
        model.insert_tab(Tab {
            tab_id: "w1:t1".to_owned(),
            workspace_id: "w1".to_owned(),
            label: Some("old".to_owned()),
        });
        let (mut reducer, shared) = Reducer::new(RestoredState {
            model,
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });

        let normalized = normalize_event(
            &shared,
            "rename-session",
            &tab_renamed_event("w1:t1", Some("レビュー")),
        )
        .unwrap();
        let persist = apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("known tab rename should apply");
        assert_eq!(
            shared.borrow().tab("w1:t1").unwrap().label.as_deref(),
            Some("レビュー")
        );
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTab { tab, .. }
                if tab.tab_id == "w1:t1"
                    && tab.workspace_id == "w1"
                    && tab.label.as_deref() == Some("レビュー")
        )));

        let normalized = normalize_event(
            &shared,
            "rename-session",
            &tab_renamed_event("w1:t1", Some("")),
        )
        .unwrap();
        let persist = apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("empty known tab rename should apply");
        assert_eq!(shared.borrow().tab("w1:t1").unwrap().label, None);
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTab { tab, .. }
                if tab.tab_id == "w1:t1" && tab.workspace_id == "w1" && tab.label.is_none()
        )));

        let normalized = normalize_event(
            &shared,
            "rename-session",
            &tab_renamed_event("w1:t1", Some("before absent")),
        )
        .unwrap();
        apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("known tab rename should apply");
        assert_eq!(
            shared.borrow().tab("w1:t1").unwrap().label.as_deref(),
            Some("before absent")
        );
        let normalized =
            normalize_event(&shared, "rename-session", &tab_renamed_event("w1:t1", None)).unwrap();
        let persist = apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("absent-label known tab rename should apply");
        assert_eq!(shared.borrow().tab("w1:t1").unwrap().label, None);
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTab { tab, .. }
                if tab.tab_id == "w1:t1" && tab.workspace_id == "w1" && tab.label.is_none()
        )));

        let normalized = normalize_event(
            &shared,
            "rename-session",
            &tab_renamed_event("unknown-tab", Some("ignored")),
        )
        .unwrap();
        assert!(normalized.is_empty());
    }

    fn snapshot_with_names() -> Snapshot {
        Snapshot {
            version: "test".to_owned(),
            protocol: 1,
            focused_workspace_id: Some("w1".to_owned()),
            focused_tab_id: Some("w1:t1".to_owned()),
            focused_pane_id: Some("w1:p4".to_owned()),
            workspaces: vec![WorkspaceInfo {
                workspace_id: "w1".to_owned(),
                number: Some(1),
                label: None,
                focused: Some(true),
                pane_count: Some(1),
                tab_count: Some(1),
                active_tab_id: Some("w1:t1".to_owned()),
                agent_status: None,
            }],
            tabs: vec![TabInfo {
                tab_id: "w1:t1".to_owned(),
                workspace_id: "w1".to_owned(),
                number: Some(1),
                label: Some("レビュー".to_owned()),
                focused: Some(true),
                pane_count: Some(1),
                agent_status: None,
            }],
            panes: vec![PaneInfo {
                pane_id: "w1:p4".to_owned(),
                terminal_id: "terminal-4".to_owned(),
                workspace_id: "w1".to_owned(),
                tab_id: Some("w1:t1".to_owned()),
                focused: Some(true),
                cwd: None,
                foreground_cwd: None,
                terminal_title: None,
                terminal_title_stripped: Some("build".to_owned()),
                label: Some("UI修正".to_owned()),
                agent: None,
                agent_status: None,
                scroll: None,
                revision: None,
                agent_session: None,
            }],
            layouts: Vec::new(),
            agents: Vec::new(),
        }
    }

    fn empty_reducer() -> (Reducer, SharedModel) {
        Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        })
    }

    fn assert_named_topology(model: &DomainModel) {
        assert_eq!(
            model.tab("w1:t1").unwrap().label.as_deref(),
            Some("レビュー")
        );
        assert_eq!(
            model.pane("w1:p4").unwrap().display_name.as_deref(),
            Some("UI修正")
        );
    }

    #[test]
    fn snapshot_reconciliation_preserves_captured_tab_and_pane_names() {
        let topology = topology_from_snapshot(&snapshot_with_names()).unwrap();
        let (mut reducer, shared) = empty_reducer();

        let persist = reducer
            .reconcile_gap(ReconcileBatch {
                topology,
                gap_kind: GapKind::Reconnect,
            })
            .unwrap();

        assert_named_topology(&shared.borrow());
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertTab { tab, .. }
                if tab.label.as_deref() == Some("レビュー")
        )));
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertPane { pane, .. }
                if pane.display_name.as_deref() == Some("UI修正")
        )));
    }

    #[test]
    fn tab_rename_keeps_watchdog_topology_probe_in_sync() {
        let probed = topology_from_snapshot(&snapshot_with_names()).unwrap();
        let mut initial = probed.clone();
        initial.tabs[0].label = Some("old label".to_owned());
        let (mut reducer, shared) = empty_reducer();
        reducer
            .reconcile_gap(ReconcileBatch {
                topology: initial,
                gap_kind: GapKind::Startup,
            })
            .unwrap();

        let stale = current_model_topology(&shared, &PendingTopologyClosures::default()).unwrap();
        assert!(!probe_topology_matches_model(probed.clone(), stale));

        let normalized = normalize_event(
            &shared,
            "rename-session",
            &tab_renamed_event("w1:t1", Some("レビュー")),
        )
        .unwrap();
        apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("known tab rename should apply");

        let current = current_model_topology(&shared, &PendingTopologyClosures::default()).unwrap();
        assert!(probe_topology_matches_model(probed, current));

        let normalized = normalize_event(
            &shared,
            "rename-session",
            &tab_renamed_event("w1:t1", Some("")),
        )
        .unwrap();
        let persist = apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("empty-label known tab rename should apply");
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::ClearTabLabel { tab_id } if tab_id == "w1:t1"
        )));
        assert_eq!(shared.borrow().tab("w1:t1").unwrap().label, None);
        let mut cleared_probe = topology_from_snapshot(&snapshot_with_names()).unwrap();
        cleared_probe.tabs[0].label = None;
        let current = current_model_topology(&shared, &PendingTopologyClosures::default()).unwrap();
        assert!(probe_topology_matches_model(cleared_probe, current));
    }

    #[test]
    fn in_place_snapshot_preserves_captured_tab_and_pane_names() {
        let named_topology = topology_from_snapshot(&snapshot_with_names()).unwrap();
        let (mut reducer, shared) = empty_reducer();
        reducer
            .reconcile_gap(ReconcileBatch {
                topology: named_topology.clone(),
                gap_kind: GapKind::Startup,
            })
            .unwrap();
        let mut topology = named_topology;
        topology.tabs[0].label = None;
        topology.panes[0].display_name = None;
        let mut pending = PendingTopologyClosures::default();

        let persist = apply_snapshot_in_place(
            &mut reducer,
            &shared,
            topology,
            "snapshot-session",
            &mut pending,
        )
        .unwrap();

        assert_named_topology(&shared.borrow());
        assert!(persist.iter().any(|operation| matches!(
            operation,
            PersistOp::UpsertPane { pane, .. }
                if pane.display_name.as_deref() == Some("UI修正")
        )));
    }

    fn flat_pane_agent_detected(
        pane_id: &str,
        diagnostics: PrimaryStreamDiagnosticsHandle,
    ) -> ReceivedEvent {
        ReceivedEvent {
            event: "pane_agent_detected".to_owned(),
            data: json!({
                "agent": "claude",
                "pane_id": pane_id,
                "type": "pane_agent_detected",
                "workspace_id": "w1",
            }),
            primary_stream_diagnostics: diagnostics,
        }
    }

    fn nested_pane_agent_detected(diagnostics: PrimaryStreamDiagnosticsHandle) -> ReceivedEvent {
        ReceivedEvent {
            event: "pane_agent_detected".to_owned(),
            data: json!({
                "type": "pane_agent_detected",
                "pane": {
                    "agent": "claude",
                    "agent_status": "working",
                    "pane_id": "w1:p4",
                    "tab_id": "w1:t1",
                    "terminal_id": "terminal-4",
                    "workspace_id": "w1",
                },
            }),
            primary_stream_diagnostics: diagnostics,
        }
    }

    fn nameless_pane_updated() -> ReceivedEvent {
        ReceivedEvent {
            event: "pane_updated".to_owned(),
            data: json!({
                "type": "pane_updated",
                "pane": {
                    "pane_id": "w1:p4",
                    "tab_id": "w1:t1",
                    "terminal_id": "terminal-4",
                    "workspace_id": "w1",
                },
            }),
            primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle::default(),
        }
    }

    fn named_reducer() -> (Reducer, SharedModel) {
        let topology = topology_from_snapshot(&snapshot_with_names()).unwrap();
        let (mut reducer, shared) = empty_reducer();
        reducer
            .reconcile_gap(ReconcileBatch {
                topology,
                gap_kind: GapKind::Startup,
            })
            .unwrap();
        (reducer, shared)
    }

    #[test]
    fn nameless_nested_pane_agent_detected_retains_live_display_name() {
        let (mut reducer, shared) = named_reducer();
        let normalized = normalize_event(
            &shared,
            "nested-frame-session",
            &nested_pane_agent_detected(PrimaryStreamDiagnosticsHandle::default()),
        )
        .unwrap();

        apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("nested pane_agent_detected should apply");

        assert_eq!(
            shared
                .borrow()
                .pane("w1:p4")
                .unwrap()
                .display_name
                .as_deref(),
            Some("UI修正")
        );
    }

    #[test]
    fn nameless_pane_updated_retains_live_display_name() {
        let (mut reducer, shared) = named_reducer();
        let normalized =
            normalize_event(&shared, "pane-updated-session", &nameless_pane_updated()).unwrap();

        apply_collector_observation(&mut reducer, normalized)
            .unwrap()
            .expect("pane_updated should apply");

        assert_eq!(
            shared
                .borrow()
                .pane("w1:p4")
                .unwrap()
                .display_name
                .as_deref(),
            Some("UI修正")
        );
    }

    #[test]
    fn flat_pane_agent_detected_counts_without_topology_mutation() {
        let shared = known_pane_model();
        let flat_diagnostics = PrimaryStreamDiagnosticsHandle::default();
        let flat = flat_pane_agent_detected("w1:p4", flat_diagnostics.clone());

        let normalized = normalize_event(&shared, "flat-frame-session", &flat)
            .expect("flat pane_agent_detected should be tolerated");
        assert!(
            normalized.is_empty(),
            "flat pane_agent_detected must not produce topology or persistence operations"
        );
        assert_eq!(flat_diagnostics.snapshot().flat_pane_agent_detected, 1);

        let nested_diagnostics = PrimaryStreamDiagnosticsHandle::default();
        let nested = nested_pane_agent_detected(nested_diagnostics.clone());
        let normalized = normalize_event(&shared, "nested-frame-session", &nested)
            .expect("nested pane_agent_detected should still normalize");
        assert!(normalized.iter().any(|event| matches!(
            event,
            NormalizedEvent::TopologyUpsert {
                entity: TopologyEntity::Pane(pane),
                ..
            } if pane.pane_id == "w1:p4"
        )));
        assert_eq!(nested_diagnostics.snapshot().flat_pane_agent_detected, 0);
    }

    #[test]
    fn flat_pane_agent_detected_participates_in_resync_admission() {
        let diagnostics = PrimaryStreamDiagnosticsHandle::default();
        let flat_known = flat_pane_agent_detected("w1:p4", diagnostics.clone());
        let flat_unknown = flat_pane_agent_detected("w1:p9", diagnostics.clone());
        let nested = nested_pane_agent_detected(diagnostics);

        assert_eq!(
            updated_entity(&flat_known),
            Some(EntityKey::Pane("w1:p4".to_owned()))
        );
        assert_eq!(
            updated_entity(&nested),
            Some(EntityKey::Pane("w1:p4".to_owned()))
        );

        let shared = known_pane_model();
        assert!(
            !updated_entity(&flat_known).is_some_and(|entity| !entity_exists(&shared, &entity))
        );
        assert!(
            updated_entity(&flat_unknown).is_some_and(|entity| !entity_exists(&shared, &entity))
        );
    }

    fn status_received(status: &str) -> ReceivedEvent {
        ReceivedEvent {
            event: "pane_agent_status_changed".to_owned(),
            data: json!({
                "pane_id": "w1:p1",
                "terminal_id": "terminal-1",
                "agent_status": status,
            }),
            primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle::default(),
        }
    }

    fn inactive_provider_integration() -> (
        mpsc::Sender<ProviderIngressEvent>,
        ProviderIntegration,
        ProviderThreadHandle,
    ) {
        let provider_diagnostics = crate::provider::ProviderDiagnostics::default();
        let (ignored_events, _ignored_receiver) = mpsc::channel(1);
        let provider_thread = spawn_provider_thread_with_diagnostics(
            AdapterProviderWorker::new(Vec::new(), provider_diagnostics.clone()),
            ignored_events,
            None,
            provider_diagnostics,
        )
        .unwrap();
        let (provider_sender, provider_events) = mpsc::channel(1);
        let initial_coverage = SourceCoverageRegistry::default();
        let (coverage_sender, _source_coverage) = watch::channel(initial_coverage);
        let (source_quality_sender, _source_quality) =
            watch::channel(ObservationQuality::Reconciling);
        let coverage = CoverageTracker::new(
            SourceAvailability::NotApplicable,
            coverage_sender,
            source_quality_sender,
        );
        let provider = ProviderIntegration::new(
            provider_events,
            provider_thread.target_publisher(),
            TargetSet::default(),
            coverage,
        );
        (provider_sender, provider, provider_thread)
    }

    #[test]
    fn live_status_expands_per_differing_execution_with_distinct_receipt_identity() {
        let shared = status_model(&[
            ("live", ExecState::Idle),
            ("stale", ExecState::Stale { since_ms: 7 }),
        ]);
        let normalized = normalize_event(&shared, "status-session", &status_received("working"))
            .expect("status payload should normalize");
        assert_eq!(normalized.len(), 2);
        let mut identities = BTreeSet::new();
        let mut executions = BTreeSet::new();
        for event in normalized {
            let NormalizedEvent::AgentStatusChanged {
                metadata,
                execution_id,
                state,
            } = event
            else {
                panic!("status receipt emitted a non-status event");
            };
            assert_eq!(state, ExecState::Working);
            assert_eq!(metadata.source_event_type, "pane_agent_status_changed");
            assert_eq!(metadata.pane_id.as_deref(), Some("w1:p1"));
            assert_eq!(metadata.terminal_id.as_deref(), Some("terminal-1"));
            assert_eq!(metadata.timestamp_ms, metadata.receipt_time_ms);
            identities.insert(metadata.event_id);
            executions.insert(execution_id);
        }
        assert_eq!(identities.len(), 2);
        assert_eq!(
            executions,
            BTreeSet::from(["live".to_owned(), "stale".to_owned()])
        );
    }

    #[tokio::test]
    async fn enrichment_payload_persists_distinct_receipt_identity_activity_and_one_admission() {
        let directory = tempfile::tempdir().unwrap();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let run_id = RunId::new();
        let task_run = TaskRun {
            run_id,
            key: RunKey::Controller("persisted-status-run".to_owned()),
            display_ordinal: DisplayOrdinal::new(4),
            state: TaskState::Running,
            has_controller_task_state_event: false,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let executions = ["live", "stale"].map(|execution_id| Execution {
            execution_id: execution_id.to_owned(),
            pane_id: "w1:p1".to_owned(),
            terminal_id: "terminal-1".to_owned(),
            task_run_id: run_id,
            state: if execution_id == "live" {
                ExecState::Idle
            } else {
                ExecState::Stale { since_ms: 7 }
            },
        });
        store
            .apply_batch(
                [
                    PersistOp::UpsertWorkspace {
                        workspace: Workspace {
                            workspace_id: "w1".to_owned(),
                        },
                        display_ordinal: DisplayOrdinal::new(1),
                    },
                    PersistOp::UpsertTab {
                        tab: Tab {
                            tab_id: "w1:t1".to_owned(),
                            workspace_id: "w1".to_owned(),
                            label: None,
                        },
                        display_ordinal: DisplayOrdinal::new(2),
                    },
                    PersistOp::UpsertPane {
                        pane: Pane {
                            pane_id: "w1:p1".to_owned(),
                            workspace_id: "w1".to_owned(),
                            tab_id: "w1:t1".to_owned(),
                            terminal_id: "terminal-1".to_owned(),
                            display_name: None,
                        },
                        display_ordinal: DisplayOrdinal::new(3),
                    },
                    PersistOp::UpsertTaskRun(PersistTaskRun {
                        task_run,
                        native_session: None,
                        created_at_ms: 1,
                        updated_at_ms: 1,
                        finished_at_ms: None,
                    }),
                ]
                .into_iter()
                .chain(executions.into_iter().map(|execution| {
                    PersistOp::UpsertExecution(PersistExecution {
                        execution,
                        started_at_ms: 1,
                        updated_at_ms: 1,
                        ended_at_ms: None,
                    })
                }))
                .collect(),
            )
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared, operator) =
            Reducer::new_with_operator(restored, empty_operator_seed());
        let (mut persistence, diagnostics) =
            RuntimePersistence::new_for_test(writer, Arc::new(RecordingOccurrenceSink::default()));
        let (provider_sender, mut provider, provider_thread) = inactive_provider_integration();
        let (performance, mut sampler) =
            performance_tracker(Arc::new(TestPerformanceClock::new(Duration::ZERO)));
        let before = sampler
            .sample(&shared.borrow(), &operator.borrow(), 1_900_000_000_222)
            .admission_high_water;
        let mut pending = PendingTopologyClosures::default();

        apply_enrichment_payload(
            &mut reducer,
            &shared,
            &mut persistence,
            "status-session",
            EnrichmentPayload {
                pane_id: "w1:p1".to_owned(),
                terminal_id: Some("terminal-1".to_owned()),
                state: ExecState::Working,
                timestamp_ms: 1_900_000_000_111,
                receipt_time_ms: 1_900_000_000_222,
            },
            &BTreeSet::from(["w1:p1".to_owned()]),
            &performance,
            &mut pending,
            &mut provider,
        )
        .await
        .unwrap();
        assert_eq!(diagnostics.borrow().persistence, PersistenceStatus::Healthy);

        let after = sampler
            .sample(&shared.borrow(), &operator.borrow(), 1_900_000_000_222)
            .admission_high_water;
        assert_eq!(after, before + 1);
        let activity = operator.borrow();
        let status_activity = activity
            .activity
            .iter()
            .filter(|item| item.source_event_type == "pane_agent_status_changed")
            .collect::<Vec<_>>();
        assert_eq!(
            status_activity.len(),
            2,
            "unexpected operator activity: {:?}",
            activity.activity
        );
        assert!(status_activity.iter().all(|item| {
            item.source == "herdr"
                && item.normalized_kind == "agent_status_changed"
                && item.event_timestamp_ms == 1_900_000_000_111
                && item.seen_at_ms == 1_900_000_000_222
        }));
        assert_ne!(
            status_activity[0].identity.event_id,
            status_activity[1].identity.event_id
        );
        drop(activity);

        drop(provider_sender);
        provider_thread.stop().await.unwrap();
        drop(persistence);
        shutdown_writer(lifecycle).await;
        let connection = rusqlite::Connection::open(crate::store::database_path(&root)).unwrap();
        let rows = connection
            .prepare(
                "SELECT event_id, event_timestamp_ms, seen_at_ms, source_event_type FROM events \
                 ORDER BY event_row_id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2, "unexpected persisted rows: {rows:?}");
        assert_ne!(rows[0].0, rows[1].0);
        assert!(rows.iter().all(|row| {
            row.1 == 1_900_000_000_111
                && row.2 == 1_900_000_000_222
                && row.3 == "pane_agent_status_changed"
        }));
    }

    #[tokio::test]
    async fn enrichment_payload_cancels_pending_pane_closure_through_application_path() {
        let restored = RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        };
        let (mut reducer, shared) = Reducer::new(restored);
        let (_directory, lifecycle, mut persistence, _diagnostics) =
            runtime_with_sink(Arc::new(RecordingOccurrenceSink::default()));
        let (provider_sender, mut provider, provider_thread) = inactive_provider_integration();
        let (performance, _sampler) =
            performance_tracker(Arc::new(TestPerformanceClock::new(Duration::ZERO)));
        let mut pending = PendingTopologyClosures {
            panes: HashSet::from(["w1:p1".to_owned()]),
            ..PendingTopologyClosures::default()
        };

        apply_enrichment_payload(
            &mut reducer,
            &shared,
            &mut persistence,
            "status-session",
            EnrichmentPayload {
                pane_id: "w1:p1".to_owned(),
                terminal_id: Some("terminal-1".to_owned()),
                state: ExecState::Working,
                timestamp_ms: 111,
                receipt_time_ms: 222,
            },
            &BTreeSet::new(),
            &performance,
            &mut pending,
            &mut provider,
        )
        .await
        .unwrap();

        assert!(!pending.panes.contains("w1:p1"));
        drop(provider_sender);
        provider_thread.stop().await.unwrap();
        drop(persistence);
        shutdown_writer(lifecycle).await;
    }

    #[test]
    fn live_status_equal_to_every_execution_emits_nothing() {
        let shared = status_model(&[
            ("first", ExecState::Working),
            ("second", ExecState::Working),
        ]);
        let normalized = normalize_event(&shared, "status-session", &status_received("working"))
            .expect("status payload should normalize");
        assert!(normalized.is_empty());
    }

    #[test]
    fn live_status_filters_each_execution_independently_and_skips_terminal_sibling() {
        let shared = status_model(&[
            ("equal", ExecState::Working),
            ("different", ExecState::Idle),
            ("terminal", ExecState::Ended),
        ]);
        let normalized = normalize_event(&shared, "status-session", &status_received("working"))
            .expect("status payload should normalize");
        assert_eq!(normalized.len(), 1);
        assert!(matches!(
            &normalized[0],
            NormalizedEvent::AgentStatusChanged { execution_id, state, .. }
                if execution_id == "different" && *state == ExecState::Working
        ));
        assert_eq!(
            shared.borrow().execution("terminal").unwrap().state,
            ExecState::Ended
        );
    }

    #[tokio::test]
    async fn i4_d3_herdr_disconnect_immediately_refreshes_diagnostics_once() {
        let directory = tempfile::tempdir().unwrap();
        let log_path = directory.path().join("collector-subscribe.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(log)
            .finish();
        let root = crate::lockfile::StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (reducer, shared) = Reducer::new(restored);
        let (quality_sender, _quality) = watch::channel(ObservationQuality::Reconciling);
        let initial_coverage = SourceCoverageRegistry::new(SourceAvailability::NotApplicable);
        let (coverage_sender, _source_coverage) = watch::channel(initial_coverage.clone());
        let coverage = CoverageTracker::new(
            SourceAvailability::NotApplicable,
            coverage_sender,
            quality_sender,
        );
        let (persistence, mut diagnostics) = RuntimePersistence::new(
            writer,
            &shared.borrow(),
            &initial_coverage,
            Arc::new(RecordingOccurrenceSink::default()),
        );
        diagnostics.borrow_and_update();

        let provider_diagnostics = crate::provider::ProviderDiagnostics::default();
        let (ignored_events, _ignored_receiver) = mpsc::channel(1);
        let provider_thread = spawn_provider_thread_with_diagnostics(
            AdapterProviderWorker::new(Vec::new(), provider_diagnostics.clone()),
            ignored_events,
            None,
            provider_diagnostics,
        )
        .unwrap();
        let (idle_events, provider_events) = mpsc::channel(1);
        let provider = ProviderIntegration::new(
            provider_events,
            provider_thread.target_publisher(),
            TargetSet::default(),
            coverage,
        );
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let (performance, _sampler) =
            performance_tracker(Arc::new(TestPerformanceClock::new(Duration::ZERO)));
        let missing_socket = directory.path().join("missing-herdr.sock");
        let task = tokio::spawn(
            run_collector(
                missing_socket,
                "diagnostics-session".to_owned(),
                persistence,
                reducer,
                shared,
                performance,
                task_cancellation,
                OwnerTracker::from_environment(),
                None,
                None,
                provider,
                LivenessPolicy::default(),
                PrimaryStreamDiagnosticsHandle::default(),
            )
            .with_subscriber(subscriber),
        );

        tokio::time::timeout(Duration::from_secs(1), diagnostics.changed())
            .await
            .expect("Herdr disconnect did not refresh consolidated diagnostics")
            .unwrap();
        assert!(
            diagnostics
                .borrow_and_update()
                .source_coverage
                .iter()
                .any(|source| source.source == DiagnosticSource::Herdr
                    && source.availability == InputAvailability::Unavailable)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(180), diagnostics.changed())
                .await
                .is_err(),
            "identical disconnect transitions must not publish spurious diagnostics versions"
        );

        cancellation.cancel();
        drop(idle_events);
        task.await.unwrap().unwrap();
        provider_thread.stop().await.unwrap();
        shutdown_writer(lifecycle).await;

        let contents = std::fs::read_to_string(log_path).unwrap();
        assert!(
            contents.contains("warning_code=\"herdr_subscription_failed\""),
            "subscribe failure warning code was not logged: {contents}"
        );
        assert!(
            contents.contains("herdr wire I/O failed:"),
            "subscribe WireError display was not logged: {contents}"
        );
    }
}

#[cfg(test)]
mod provider_integration_tests {
    use std::collections::HashSet;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use tokio::sync::watch;

    use super::*;
    use crate::lockfile::StateRoot;
    use crate::model::{
        AgentNode, ControllerEventKind, DisplayOrdinal, DomainModel, ExecState, ExecutionEdge,
        MinimalProviderMetadata, RunId, RunKey, SharedModel, TaskRun, TaskState,
    };
    use crate::provider::claude_facts::{extract_claude_line, extract_meta_json};
    use crate::provider::facts::{ActivitySource, LogFact, SessionScope};
    use crate::provider::lane::{Admission, AdmissionIndex, Synthesis};
    use crate::provider::{ProviderCycle, ProviderEvent, ProviderWorker, SourcePosition};
    use crate::store::WriterLifecycle;
    use crate::store::{
        NativeSessionBinding, PersistOp, PersistTaskRun, open_reader, open_writer, spawn_writer,
    };
    use crate::tui::app::AppState;
    use crate::tui::view::build_rows;

    struct TestOccurrenceSink;

    impl PersistenceOccurrenceSink for TestOccurrenceSink {
        fn append(&self, _record: &[u8]) -> io::Result<()> {
            Ok(())
        }
    }

    fn test_runtime(writer: WriterClient) -> RuntimePersistence {
        RuntimePersistence::new_for_test(writer, Arc::new(TestOccurrenceSink)).0
    }

    struct LaneModelHarness {
        _directory: tempfile::TempDir,
        lifecycle: WriterLifecycle,
        persistence: RuntimePersistence,
        reducer: Reducer,
        shared: SharedModel,
    }

    impl LaneModelHarness {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let root = StateRoot(directory.path().to_path_buf());
            let store = open_writer(&root).unwrap();
            let (lifecycle, writer) = spawn_writer(store).unwrap();
            let persistence = test_runtime(writer);
            let (reducer, shared) = Reducer::new(RestoredState {
                model: DomainModel::default(),
                next_ordinal: 1,
                next_ingest_seq: Some(1),
                event_ledger: Vec::new(),
            });
            Self {
                _directory: directory,
                lifecycle,
                persistence,
                reducer,
                shared,
            }
        }

        async fn apply(&mut self, events: impl IntoIterator<Item = ProviderEvent>) {
            let coverage = SourceCoverageRegistry::new(SourceAvailability::Available);
            for event in events {
                apply_provider_event(
                    event,
                    "session",
                    &mut self.reducer,
                    &self.shared,
                    &mut self.persistence,
                    &coverage,
                )
                .await
                .unwrap();
            }
        }

        fn run_id(&self, key: &RunKey) -> RunId {
            self.shared
                .borrow()
                .task_run_by_key(key)
                .unwrap_or_else(|| panic!("missing run for {key:?}"))
                .run_id
        }

        fn row_label(&self, run_id: RunId) -> String {
            let model = self.shared.borrow();
            build_rows(&model, &AppState::default())
                .into_iter()
                .find(|row| row.key.run_id() == Some(run_id))
                .unwrap_or_else(|| panic!("missing row for {run_id}"))
                .label
        }

        fn row_labels(&self) -> Vec<String> {
            let model = self.shared.borrow();
            build_rows(&model, &AppState::default())
                .into_iter()
                .map(|row| row.label)
                .collect()
        }

        async fn shutdown(self) {
            drop(self.persistence);
            self.lifecycle.shutdown().await.unwrap();
        }
    }

    fn synthesize_claude_record(
        synthesis: &mut Synthesis,
        admission: &mut Admission,
        artifact: &Path,
        scope: &SessionScope,
        ordinal: u64,
        line: &str,
    ) -> Vec<ProviderEvent> {
        synthesis.synthesize_batch(
            artifact,
            extract_claude_line(scope, line)
                .into_iter()
                .map(|fact| (ordinal, fact)),
            admission,
            &AdmissionIndex::new(),
        )
    }

    #[tokio::test]
    async fn live_prefers_commentary_then_command() {
        const ROLLOUT: &str = "22222222-2222-4222-8222-222222222222";
        let scope = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let run_key = RunKey::Native {
            provider: Provider::Codex,
            sid: ROLLOUT.to_owned(),
        };
        let mut synthesis = Synthesis::default();
        let mut admission = Admission::new(0);
        let discovered = AdmissionIndex::new();
        let mut harness = LaneModelHarness::new();

        let events = synthesis.synthesize_batch(
            Path::new("rollout.jsonl"),
            [
                (
                    1,
                    LogFact::CodexTurnStarted {
                        rollout_id: ROLLOUT.to_owned(),
                        at_ms: 1,
                    },
                ),
                (
                    2,
                    LogFact::Activity {
                        scope: scope.clone(),
                        at_ms: 2,
                        source: ActivitySource::Command,
                        line: "cargo test".to_owned(),
                    },
                ),
                (
                    3,
                    LogFact::Activity {
                        scope: scope.clone(),
                        at_ms: 3,
                        source: ActivitySource::Commentary,
                        line: "checking invariants".to_owned(),
                    },
                ),
            ],
            &mut admission,
            &discovered,
        );
        harness.apply(events).await;
        let run_id = harness.run_id(&run_key);
        let commentary_row = harness.row_label(run_id);
        assert!(
            commentary_row.contains(" — checking invariants"),
            "lane commentary did not reach the row: {commentary_row}"
        );
        assert!(
            harness
                .row_labels()
                .iter()
                .all(|label| !label.contains("native agent: agent:codex:lane:")),
            "the live-line transport node must not create a synthetic agent row"
        );

        let events = synthesis.synthesize_batch(
            Path::new("rollout.jsonl"),
            [(
                4,
                LogFact::Activity {
                    scope: scope.clone(),
                    at_ms: 4,
                    source: ActivitySource::Command,
                    line: "later command".to_owned(),
                },
            )],
            &mut admission,
            &discovered,
        );
        harness.apply(events).await;
        let same_turn_row = harness.row_label(run_id);
        assert!(same_turn_row.contains(" — checking invariants"));
        assert!(!same_turn_row.contains("later command"));

        let events = synthesis.synthesize_batch(
            Path::new("rollout.jsonl"),
            [
                (
                    5,
                    LogFact::CodexTurnStarted {
                        rollout_id: ROLLOUT.to_owned(),
                        at_ms: 5,
                    },
                ),
                (
                    6,
                    LogFact::Activity {
                        scope,
                        at_ms: 6,
                        source: ActivitySource::Command,
                        line: "next turn command".to_owned(),
                    },
                ),
            ],
            &mut admission,
            &discovered,
        );
        harness.apply(events).await;
        let next_turn_row = harness.row_label(run_id);
        assert!(
            next_turn_row.contains(" — next turn command"),
            "next-turn command did not reach the row: {next_turn_row}"
        );
        assert!(!next_turn_row.contains("checking invariants"));

        const CLAUDE_SESSION: &str = "77777777-7777-4777-8777-777777777777";
        let claude_scope = SessionScope::ClaudeRoot(CLAUDE_SESSION.to_owned());
        let claude_events = synthesis.synthesize_batch(
            Path::new("claude-session.jsonl"),
            [
                (
                    7,
                    LogFact::Append {
                        scope: claude_scope.clone(),
                        at_ms: 7,
                    },
                ),
                (
                    8,
                    LogFact::AiTitle {
                        session_id: CLAUDE_SESSION.to_owned(),
                        title: "inspect project".to_owned(),
                    },
                ),
                (
                    9,
                    LogFact::Activity {
                        scope: claude_scope,
                        at_ms: 9,
                        source: ActivitySource::ToolUse,
                        line: "Read: Cargo.toml".to_owned(),
                    },
                ),
            ],
            &mut admission,
            &discovered,
        );
        harness.apply(claude_events).await;
        let claude_run_id = harness.run_id(&RunKey::Controller(format!(
            "hook:claude-code:{CLAUDE_SESSION}"
        )));
        let claude_row = harness.row_label(claude_run_id);
        assert!(
            claude_row.contains("claude-code inspect project — Read: Cargo.toml"),
            "Claude ToolUse selected line changed: {claude_row}"
        );

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn lane_and_adapter_activity_for_same_codex_rollout_both_reach_model() {
        const ROLLOUT: &str = "019f7504-83e2-75f0-870d-cc423f88a73b";
        let fixture = include_str!("../../tests/fixtures/provider/codex-depth2-child.jsonl");
        let root_meta = fixture
            .lines()
            .nth(1)
            .expect("fixture has copied root metadata");
        let adapter_activity = fixture
            .lines()
            .nth(15)
            .expect("fixture has root activity in the child rollout");
        let task_started = r#"{"timestamp":"2026-08-24T02:00:00.020Z","type":"event_msg","payload":{"type":"task_started"}}"#;
        let commentary = r#"{"timestamp":"2026-08-24T02:00:01.010Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"AgentMessage","content":[{"type":"Text","text":"adapter and lane both survive"}],"phase":"commentary"}}}"#;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("fallback/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new_with_log_lane_config(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
            LogLaneConfig {
                headless_inactivity_ms: i64::MAX / 2,
                ..LogLaneConfig::default()
            },
        );
        let mut pending = PendingEvents::new(diagnostics);
        let path = root.join(format!("rollout-2026-08-24T02-00-00-{ROLLOUT}.jsonl"));
        std::fs::write(&path, format!("{root_meta}\n")).unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        let mut harness = LaneModelHarness::new();
        let mut seed_synthesis = Synthesis::default();
        let mut seed_admission = Admission::new(0);
        let seed = seed_synthesis.synthesize_batch(
            Path::new("seed-rollout.jsonl"),
            [(
                1,
                LogFact::CodexTurnStarted {
                    rollout_id: ROLLOUT.to_owned(),
                    at_ms: 1,
                },
            )],
            &mut seed_admission,
            &AdmissionIndex::new(),
        );
        harness.apply(seed).await;

        process_adapter_worker(&mut worker, &targets, &mut pending);
        harness.apply(drain_pending(&mut pending)).await;
        let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        writeln!(file, "{task_started}").unwrap();
        writeln!(file, "{adapter_activity}").unwrap();
        writeln!(file, "{commentary}").unwrap();
        file.flush().unwrap();
        process_adapter_worker(&mut worker, &targets, &mut pending);
        harness.apply(drain_pending(&mut pending)).await;

        let run_id = harness.run_id(&RunKey::Native {
            provider: Provider::Codex,
            sid: ROLLOUT.to_owned(),
        });
        {
            let model = harness.shared.borrow();
            let adapter_node = model
                .agent_nodes()
                .find(|node| {
                    node.task_run_id == run_id && node.native_session_id.as_deref() == Some(ROLLOUT)
                })
                .expect("adapter root node reaches the model");
            assert_eq!(adapter_node.last_event_kind.as_deref(), Some("interacted"));
            assert_eq!(adapter_node.last_activity_at_ms, Some(1_784_391_169_725));
        }
        let row = harness.row_label(run_id);
        assert!(
            row.contains(" — adapter and lane both survive"),
            "lane commentary did not survive the mixed batch: {row}"
        );

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn terminal_rows_drop_live() {
        const ROLLOUT: &str = "33333333-3333-4333-8333-333333333333";
        let scope = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let run_key = RunKey::Native {
            provider: Provider::Codex,
            sid: ROLLOUT.to_owned(),
        };
        let mut synthesis = Synthesis::default();
        let mut admission = Admission::new(0);
        let discovered = AdmissionIndex::new();
        let mut harness = LaneModelHarness::new();

        let active = synthesis.synthesize_batch(
            Path::new("terminal-rollout.jsonl"),
            [
                (
                    1,
                    LogFact::CodexTurnStarted {
                        rollout_id: ROLLOUT.to_owned(),
                        at_ms: 1,
                    },
                ),
                (
                    2,
                    LogFact::Activity {
                        scope,
                        at_ms: 2,
                        source: ActivitySource::Command,
                        line: "must disappear".to_owned(),
                    },
                ),
            ],
            &mut admission,
            &discovered,
        );
        harness.apply(active).await;
        let run_id = harness.run_id(&run_key);
        let active_row = harness.row_label(run_id);
        assert!(
            active_row.contains(" — must disappear"),
            "test precondition missing real live line: {active_row}"
        );

        let terminal = synthesis.synthesize_batch(
            Path::new("terminal-rollout.jsonl"),
            [(
                3,
                LogFact::CodexTurnAborted {
                    rollout_id: ROLLOUT.to_owned(),
                    at_ms: 3,
                },
            )],
            &mut admission,
            &discovered,
        );
        harness.apply(terminal).await;
        let terminal_row = harness.row_label(run_id);
        assert!(
            terminal_row.starts_with('✗'),
            "unexpected terminal row: {terminal_row}"
        );
        assert!(!terminal_row.contains("must disappear"));
        assert!(!terminal_row.contains(" — "));

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn subject_chain_meta_title_cwd_id() {
        const TITLE_SESSION: &str = "44444444-4444-4444-8444-444444444444";
        const CWD_SESSION: &str = "55555555-5555-4555-8555-555555555555";
        const ID_SESSION: &str = "66666666-6666-4666-8666-666666666666";
        const META_PARENT: &str = "88888888-8888-4888-8888-888888888888";
        const META_AGENT: &str = "agent-meta-subject";
        let title_scope = SessionScope::ClaudeRoot(TITLE_SESSION.to_owned());
        let cwd_scope = SessionScope::ClaudeRoot(CWD_SESSION.to_owned());
        let id_scope = SessionScope::ClaudeRoot(ID_SESSION.to_owned());
        let meta_parent_scope = SessionScope::ClaudeRoot(META_PARENT.to_owned());
        let mut synthesis = Synthesis::default();
        let mut admission = Admission::new(0);
        let mut harness = LaneModelHarness::new();

        for (ordinal, line) in [
            (
                1,
                "{\"type\":\"assistant\",\"timestamp\":\"2026-08-24T00:00:01Z\",\"cwd\":\"/work/herdr-top\",\"message\":{\"content\":[]}}"
                    .to_owned(),
            ),
            (
                2,
                format!(
                    "{{\"type\":\"ai-title\",\"sessionId\":\"{TITLE_SESSION}\",\"aiTitle\":\"Initial title\"}}"
                ),
            ),
            (
                3,
                format!(
                    "{{\"type\":\"ai-title\",\"sessionId\":\"{TITLE_SESSION}\",\"aiTitle\":\"Latest title\"}}"
                ),
            ),
        ] {
            let events = synthesize_claude_record(
                &mut synthesis,
                &mut admission,
                Path::new("title-session.jsonl"),
                &title_scope,
                ordinal,
                &line,
            );
            harness.apply(events).await;
        }
        let cwd_events = synthesize_claude_record(
            &mut synthesis,
            &mut admission,
            Path::new("cwd-session.jsonl"),
            &cwd_scope,
            1,
            "{\"type\":\"assistant\",\"timestamp\":\"2026-08-24T00:00:02Z\",\"cwd\":\"/work/cwd-project\",\"message\":{\"content\":[]}}",
        );
        harness.apply(cwd_events).await;
        let id_events = synthesize_claude_record(
            &mut synthesis,
            &mut admission,
            Path::new("id-session.jsonl"),
            &id_scope,
            1,
            "{\"type\":\"user\",\"timestamp\":\"2026-08-24T00:00:03Z\"}",
        );
        harness.apply(id_events).await;
        let meta_parent_events = synthesize_claude_record(
            &mut synthesis,
            &mut admission,
            Path::new("meta-parent.jsonl"),
            &meta_parent_scope,
            1,
            "{\"type\":\"user\",\"timestamp\":\"2026-08-24T00:00:04Z\"}",
        );
        harness.apply(meta_parent_events).await;
        let meta_fact = extract_meta_json(
            META_PARENT,
            META_AGENT,
            br#"{"agentType":"reviewer","description":"Review the meta-derived subject","toolUseId":"tool-meta","spawnDepth":1}"#,
        )
        .expect("meta fixture yields an allowlisted appearance fact");
        let meta_events = synthesis.synthesize_batch(
            Path::new("agent-meta-subject.meta.json"),
            [(2, meta_fact)],
            &mut admission,
            &AdmissionIndex::new(),
        );
        harness.apply(meta_events).await;

        let title_id = harness.run_id(&RunKey::Controller(format!(
            "hook:claude-code:{TITLE_SESSION}"
        )));
        let cwd_id = harness.run_id(&RunKey::Controller(format!(
            "hook:claude-code:{CWD_SESSION}"
        )));
        let fallback_id = harness.run_id(&RunKey::Controller(format!(
            "hook:claude-code:{ID_SESSION}"
        )));
        let meta_id = harness.run_id(&RunKey::Controller(format!(
            "hook:claude-code:{META_PARENT}:agent:{META_AGENT}"
        )));
        assert!(
            harness
                .row_label(title_id)
                .contains("claude-code Latest title"),
            "latest ai-title did not win over cwd"
        );
        assert!(
            harness
                .row_label(cwd_id)
                .contains("claude-code cwd-project"),
            "cwd basename did not supply the subject"
        );
        assert!(
            harness
                .row_label(fallback_id)
                .contains(&format!("claude-code {ID_SESSION}")),
            "session id did not supply the final fallback"
        );
        assert!(
            harness
                .row_label(meta_id)
                .contains("claude-code Review the meta-derived subject"),
            "meta.json description did not supply the dispatched subject"
        );

        harness.shutdown().await;
    }

    fn test_diagnostics() -> watch::Receiver<RuntimeDiagnosticsSnapshot> {
        watch::channel(RuntimeDiagnosticsSnapshot {
            persistence: PersistenceStatus::Healthy,
            controller_input: ControllerInputStatus::Unavailable {
                reason: ControllerInputUnavailableReason::ListenerUnavailable,
            },
            owner: OwnerFreshness::Current,
            persistence_counters: PersistenceCounters::default(),
            controller_counters: crate::diagnostics::ControllerCounterSnapshot::default(),
            enrichment_counters: crate::diagnostics::EnrichmentCounterSnapshot::default(),
            source_coverage: Vec::new(),
            dangling_announcement_components: 0,
            first_failure_log: OccurrenceLogStatus::NotAttempted,
        })
        .1
    }

    async fn wait_for_provider_readiness(events: &mut mpsc::Receiver<ProviderEvent>) {
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut ready = HashSet::new();
            while ready.len() < 2 {
                if let Some(ProviderEvent::SourceState { provider, .. }) = events.recv().await {
                    ready.insert(provider);
                }
            }
        })
        .await
        .expect("provider readiness is bounded");
    }

    #[tokio::test]
    async fn graceful_provider_stop_emits_complete_held_in_grace() {
        let now_ms = unix_now_ms();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new_with_log_lane_config(
            Vec::new(),
            diagnostics,
            LogLaneConfig {
                complete_grace_ms: i64::MAX / 2,
                ..LogLaneConfig::default()
            },
        );
        assert!(worker.synthesis.advance_lifecycle(now_ms).is_empty());
        let held = worker.synthesis.synthesize_batch(
            Path::new("rollout.jsonl"),
            [(
                4,
                crate::provider::facts::LogFact::CodexTurnComplete {
                    rollout_id: "22222222-2222-4222-8222-222222222222".to_owned(),
                    at_ms: now_ms,
                },
            )],
            &mut worker.log_admission,
            &worker.admission_index,
        );
        assert!(held.iter().all(|event| !matches!(
            event,
            ProviderEvent::Synthesized(controller)
                if matches!(controller.event, ControllerEventKind::Complete)
        )));
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (persistence, diagnostics) =
            RuntimePersistence::new_for_test(writer, Arc::new(TestOccurrenceSink));
        let (reducer, shared, operator) = Reducer::new_with_operator(
            RestoredState {
                model: DomainModel::default(),
                next_ordinal: 1,
                next_ingest_seq: Some(1),
                event_ledger: Vec::new(),
            },
            empty_operator_seed(),
        );
        let (performance_ingress, _performance_sampler) =
            performance_tracker(Arc::new(SystemPerformanceClock::new()));
        let (provider_sender, provider_events) = mpsc::channel(8);
        let provider_thread = spawn_provider_thread_with_diagnostics_and_performance(
            worker,
            provider_sender,
            None,
            crate::provider::ProviderDiagnostics::default(),
            performance_ingress,
        )
        .unwrap();
        let (_performance_sender, performance) = watch::channel(initial_performance_publication());
        let (_quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
        let (coverage_sender, source_coverage) =
            watch::channel(SourceCoverageRegistry::new(SourceAvailability::Available));
        let (source_quality_sender, _source_quality) =
            watch::channel(ObservationQuality::Reconciling);
        let coverage = CoverageTracker::new(
            SourceAvailability::Available,
            coverage_sender,
            source_quality_sender,
        );
        let (events_drained_sender, events_drained) = oneshot::channel();
        let mut provider = ProviderIntegration::new_with_drain_acknowledgement(
            provider_events,
            provider_thread.target_publisher(),
            TargetSet::default(),
            coverage,
            events_drained_sender,
        );
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task_shared = shared.clone();
        let task = tokio::spawn(async move {
            let mut reducer = reducer;
            let mut persistence = persistence;
            task_cancellation.cancelled().await;
            drain_provider_events(
                &mut provider,
                "session",
                &mut reducer,
                &task_shared,
                &mut persistence,
            )
            .await
        });
        let monitor_cancellation = cancellation.clone();
        let handle = CollectorHandle {
            performance,
            quality,
            source_coverage,
            diagnostics,
            operator,
            model: shared,
            primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle::default(),
            cancellation,
            task,
            performance_monitor: tokio::spawn(async move {
                monitor_cancellation.cancelled().await;
            }),
            controller_acceptor: None,
            provider_thread: Some(provider_thread),
            provider_events_drained: Some(events_drained),
        };

        handle.stop().await.unwrap();
        lifecycle.shutdown().await.unwrap();

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(
            restored
                .model
                .task_run_by_key(&RunKey::Native {
                    provider: Provider::Codex,
                    sid: "22222222-2222-4222-8222-222222222222".to_owned(),
                })
                .unwrap()
                .state,
            TaskState::Completed
        );
    }

    fn process_adapter_worker(
        worker: &mut AdapterProviderWorker,
        targets: &TargetSet,
        pending: &mut PendingEvents,
    ) {
        try_process_adapter_worker(worker, targets, pending).unwrap();
    }

    fn try_process_adapter_worker(
        worker: &mut AdapterProviderWorker,
        targets: &TargetSet,
        pending: &mut PendingEvents,
    ) -> io::Result<()> {
        let mut watch_requests = Vec::new();
        let mut cycle = crate::provider::test_provider_cycle(targets, pending, &mut watch_requests);
        worker.process(&mut cycle)
    }

    fn try_process_adapter_worker_with_stop(
        worker: &mut AdapterProviderWorker,
        targets: &TargetSet,
        pending: &mut PendingEvents,
        stop_flag: &AtomicBool,
    ) -> io::Result<()> {
        let mut watch_requests = Vec::new();
        let mut cycle = crate::provider::test_provider_cycle_with_stop(
            targets,
            pending,
            stop_flag,
            &mut watch_requests,
        );
        worker.process(&mut cycle)
    }

    fn drain_pending(pending: &mut PendingEvents) -> Vec<ProviderEvent> {
        let (sender, mut receiver) = mpsc::channel(64);
        pending.flush_to(&sender);
        let mut events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }
        events
    }

    fn codex_artifact_name(label: &str) -> String {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in label.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let rollout_id = format!(
            "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
            (hash >> 32) as u32,
            (hash >> 16) as u16,
            hash & 0x0fff,
            (hash >> 12) & 0x0fff,
            hash & 0xffff_ffff_ffff
        );
        format!("rollout-{label}-{rollout_id}.jsonl")
    }

    fn codex_artifact(root: &Path, label: &str) -> PathBuf {
        root.join(codex_artifact_name(label))
    }

    #[test]
    fn worker_admission_gates_bootstrap_and_tail_before_open() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.claude/projects");
        let target = root.join(format!(
            "project/{0}.jsonl",
            "11111111-1111-4111-8111-111111111111"
        ));
        let stranger = root.join(format!(
            "project/{0}.jsonl",
            "33333333-3333-4333-8333-333333333333"
        ));
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        let record = |session: &str| {
            format!(
                "{{\"type\":\"assistant\",\"uuid\":\"record-{session}\",\"timestamp\":\"2026-08-24T00:00:00Z\",\"sessionId\":\"{session}\",\"isSidechain\":false}}\n"
            )
        };
        std::fs::write(&target, record("11111111-1111-4111-8111-111111111111")).unwrap();
        std::fs::write(&stranger, record("33333333-3333-4333-8333-333333333333")).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Claude,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Claude,
            path: target.clone(),
        }]);
        let mut pending = PendingEvents::new(diagnostics.clone());

        process_adapter_worker(&mut worker, &targets, &mut pending);

        let state = worker
            .roots
            .get(&(Provider::Claude, root))
            .expect("target root should be scanned");
        assert_eq!(state.discovery.files().len(), 1);
        assert_eq!(
            state.discovery.files()[0]
                .root
                .join(&state.discovery.files()[0].relative_path),
            target
        );
        assert_eq!(worker.tails.len(), 1);
        assert!(diagnostics.admission_open_attempts() > 0);
        assert_eq!(tail_count_for_absolute(&worker, &stranger), 0);
    }

    #[test]
    fn admitted_meta_sidecar_synthesizes_dispatch_and_started() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.claude/projects");
        let session = "11111111-1111-4111-8111-111111111111";
        let target = root.join(format!("project/{session}.jsonl"));
        let subagents = root.join(format!("project/{session}/subagents"));
        let transcript = subagents.join("agent-child.jsonl");
        let meta = subagents.join("agent-child.meta.json");
        std::fs::create_dir_all(&subagents).unwrap();
        std::fs::write(
            &target,
            format!(
                "{{\"type\":\"assistant\",\"timestamp\":\"2026-08-24T00:00:00Z\",\"sessionId\":\"{session}\"}}\n"
            ),
        )
        .unwrap();
        std::fs::write(&transcript, "").unwrap();
        std::fs::write(
            &meta,
            format!(
                "{{\"agentType\":\"reviewer\",\"description\":\"Review lane synthesis\",\"agentId\":\"child\",\"sessionId\":\"{session}\"}}"
            ),
        )
        .unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Claude,
                path: root,
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Claude,
            path: target,
        }]);
        let mut pending = PendingEvents::new(diagnostics);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let synthesized = drain_pending(&mut pending)
            .into_iter()
            .filter_map(|event| match event {
                ProviderEvent::Synthesized(event) => Some(event),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(synthesized.len(), 2);
        assert!(matches!(
            synthesized[0].event,
            crate::model::ControllerEventKind::Dispatch { .. }
        ));
        assert!(matches!(
            synthesized[1].event,
            crate::model::ControllerEventKind::TaskStarted
        ));
        assert_eq!(
            synthesized[0].metadata.event_id,
            "log:agent-child.meta.json:0:dispatch:child"
        );
        assert_eq!(
            synthesized[1].metadata.event_id,
            "log:agent-child.meta.json:0:task-started:child"
        );
    }

    #[test]
    fn pane_root_basename_twins_open_only_single_newest_artifact() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.claude/projects");
        let session = "11111111-1111-4111-8111-111111111111";
        let older = root.join(format!("project-a/{session}.jsonl"));
        let newer = root.join(format!("project-b/{session}.jsonl"));
        for path in [&older, &newer] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(
                path,
                format!(
                    "{{\"type\":\"assistant\",\"uuid\":\"record-{session}\",\"timestamp\":\"2026-08-24T00:00:00Z\",\"sessionId\":\"{session}\",\"isSidechain\":false}}\n"
                ),
            )
            .unwrap();
        }
        let old_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let new_time = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
        std::fs::File::options()
            .write(true)
            .open(&older)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old_time))
            .unwrap();
        std::fs::File::options()
            .write(true)
            .open(&newer)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(new_time))
            .unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Claude,
                path: root,
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Claude,
            path: older.clone(),
        }]);
        let mut pending = PendingEvents::new(diagnostics);

        process_adapter_worker(&mut worker, &targets, &mut pending);

        assert_eq!(tail_count_for_absolute(&worker, &older), 0);
        assert_eq!(tail_count_for_absolute(&worker, &newer), 1);
        assert_eq!(worker.tails.len(), 1);
    }

    #[test]
    fn wired_worker_record_ordinal_is_restart_stable_at_nonzero_offset() {
        use std::io::Write;

        let directory = tempfile::tempdir().unwrap();
        let rollout = "22222222-2222-4222-8222-222222222222";
        let root = directory.path().join("home/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join(format!("rollout-2026-08-24T00-00-00-{rollout}.jsonl"));
        std::fs::write(
            &path,
            format!(
                "{{\"timestamp\":\"2026-08-24T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"{rollout}\",\"session_id\":\"{rollout}\",\"cwd\":\"/repo\",\"originator\":\"codex\",\"cli_version\":\"0.149.0\"}}}}\n{{\"timestamp\":\"2026-08-24T00:00:01Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"task_started\"}}}}\n"
            ),
        )
        .unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root,
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);

        writeln!(
            std::fs::OpenOptions::new().append(true).open(&path).unwrap(),
            "{{\"timestamp\":\"2026-08-24T00:00:02Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"turn_aborted\"}}}}"
        )
        .unwrap();
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let events = drain_pending(&mut pending);

        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::Synthesized(controller)
                if controller.metadata.event_id == format!(
                    "log:{}:2:cancelled:{rollout}",
                    path.file_name().unwrap().to_string_lossy()
                )
        )));
    }

    #[tokio::test]
    async fn event_ids_deterministic_across_replay() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut persistence = test_runtime(writer);
        let (mut reducer, shared) = Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let mut synthesis = crate::provider::lane::Synthesis::default();
        let events = synthesis.synthesize_batch(
            Path::new("session.jsonl"),
            [(
                7,
                crate::provider::facts::LogFact::Append {
                    scope: crate::provider::facts::SessionScope::ClaudeRoot(
                        "11111111-1111-4111-8111-111111111111".to_owned(),
                    ),
                    at_ms: 100,
                },
            )],
            &mut crate::provider::lane::Admission::new(0),
            &crate::provider::lane::AdmissionIndex::new(),
        );
        let event = events
            .into_iter()
            .find(|event| matches!(event, ProviderEvent::Synthesized(_)))
            .expect("append should synthesize task_started");
        for replay in [event.clone(), event.clone()] {
            apply_provider_event(
                replay,
                "session",
                &mut reducer,
                &shared,
                &mut persistence,
                &SourceCoverageRegistry::new(SourceAvailability::Available),
            )
            .await
            .unwrap();
        }

        assert_eq!(shared.borrow().task_runs().count(), 1);
        lifecycle.shutdown().await.unwrap();
        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(restored.event_ledger.len(), 1);
        assert_eq!(
            restored.event_ledger[0].event_id,
            "log:session.jsonl:7:task-started:11111111-1111-4111-8111-111111111111"
        );
        let next_ingest_seq = restored.next_ingest_seq;

        drop(persistence);
        let reopened_store = open_writer(&root).unwrap();
        let (reopened_lifecycle, reopened_writer) = spawn_writer(reopened_store).unwrap();
        let mut reopened_persistence = test_runtime(reopened_writer);
        let (mut reopened_reducer, reopened_shared) = Reducer::new(restored);
        for replay in [event.clone(), event] {
            apply_provider_event(
                replay,
                "session",
                &mut reopened_reducer,
                &reopened_shared,
                &mut reopened_persistence,
                &SourceCoverageRegistry::new(SourceAvailability::Available),
            )
            .await
            .unwrap();
        }
        assert_eq!(reopened_shared.borrow().task_runs().count(), 1);
        reopened_lifecycle.shutdown().await.unwrap();
        drop(reopened_persistence);
        let replayed = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(
            replayed.next_ingest_seq, next_ingest_seq,
            "hydrated durable ledger must suppress every post-restart replay apply"
        );
    }

    fn codex_records(owner: &str, agent: &str, event: &str) -> String {
        format!(
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{owner}\",\"session_id\":\"{owner}\"}}}}\n{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"sub_agent_activity\",\"event_id\":\"{event}\",\"occurred_at_ms\":1,\"agent_thread_id\":\"{agent}\",\"agent_path\":\"/root/{agent}\",\"kind\":\"interacted\"}}}}\n"
        )
    }

    fn append_codex_activity(path: &Path, agent: &str, event: &str) {
        use std::io::Write;

        writeln!(
            std::fs::OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap(),
            "{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"sub_agent_activity\",\"event_id\":\"{event}\",\"occurred_at_ms\":2,\"agent_thread_id\":\"{agent}\",\"agent_path\":\"/root/{agent}\",\"kind\":\"interacted\"}}}}"
        )
        .unwrap();
    }

    fn tail_read_calls(
        worker: &AdapterProviderWorker,
        root: &Path,
        file_name: &str,
    ) -> Option<u64> {
        let state = worker.roots.get(&(Provider::Codex, root.to_path_buf()))?;
        let path_id = state
            .discovery
            .files()
            .into_iter()
            .find(|file| file.relative_path == Path::new(file_name))?
            .path_id;
        worker.tails.get(&path_id).map(TailFile::read_calls)
    }

    fn tail_count_for_absolute(worker: &AdapterProviderWorker, path: &Path) -> usize {
        worker
            .tails
            .values()
            .filter(|tail| tail.absolute_path() == path)
            .count()
    }

    fn activity_generation(events: &[ProviderEvent], event: &str) -> Option<u64> {
        let expected = format!("prov:codex:act:{event}");
        events.iter().find_map(|candidate| match candidate {
            ProviderEvent::Activity {
                event_id,
                position: SourcePosition { generation, .. },
                ..
            } if event_id == &expected => Some(*generation),
            _ => None,
        })
    }

    fn provider_source_state(
        events: &[ProviderEvent],
        provider: Provider,
    ) -> Option<ProviderSourceState> {
        events.iter().rev().find_map(|event| match event {
            ProviderEvent::SourceState {
                provider: event_provider,
                state,
            } if *event_provider == provider => Some(state.clone()),
            _ => None,
        })
    }

    fn apply_source_states(tracker: &mut CoverageTracker, events: &[ProviderEvent]) {
        for event in events {
            if let ProviderEvent::SourceState { provider, state } = event {
                tracker.update_provider_state(*provider, state.clone());
            }
        }
    }

    fn capacity_blocker(id: &str) -> ProviderEvent {
        ProviderEvent::Activity {
            provider: Provider::Codex,
            agent_thread_id: id.to_owned(),
            activity: MinimalProviderMetadata::default(),
            depth: None,
            event_id: format!("{id}-event"),
            observed_at_ms: 0,
            position: SourcePosition {
                path_id: u32::MAX,
                generation: 0,
                offset: 0,
            },
        }
    }

    #[test]
    fn targeted_root_loss_degrades_coverage_and_restoration_recovers() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let path = codex_artifact(&root, "session");
        std::fs::write(&path, b"").unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        let mut pending = PendingEvents::new(diagnostics);
        let (quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
        let (coverage_sender, coverage) =
            watch::channel(SourceCoverageRegistry::new(SourceAvailability::Available));
        let mut tracker = CoverageTracker::new(
            SourceAvailability::Available,
            coverage_sender,
            quality_sender,
        );
        tracker.set_herdr_quality(ObservationQuality::Live);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        apply_source_states(&mut tracker, &drain_pending(&mut pending));
        std::fs::remove_dir_all(&root).unwrap();

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let unavailable = drain_pending(&mut pending);
        apply_source_states(&mut tracker, &unavailable);

        assert_eq!(
            provider_source_state(&unavailable, Provider::Codex),
            Some(ProviderSourceState::Unavailable {
                detail: "root_not_found".to_owned(),
            })
        );
        assert!(
            coverage
                .borrow()
                .summary()
                .contains("codex=unavailable(root_not_found)")
        );
        assert_eq!(*quality.borrow(), ObservationQuality::Degraded);

        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(path, b"").unwrap();
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let recovered = drain_pending(&mut pending);
        apply_source_states(&mut tracker, &recovered);

        assert_eq!(
            provider_source_state(&recovered, Provider::Codex),
            Some(ProviderSourceState::Available)
        );
        assert_eq!(*quality.borrow(), ObservationQuality::Live);
    }

    #[test]
    fn one_failed_root_makes_the_provider_aggregate_unavailable() {
        let directory = tempfile::tempdir().unwrap();
        let good_root = directory.path().join("good/.codex/sessions");
        let failed_root = directory.path().join("failed/.codex/sessions");
        std::fs::create_dir_all(&good_root).unwrap();
        let good = codex_artifact(&good_root, "good");
        let failed = codex_artifact(&failed_root, "failed");
        std::fs::write(&good, b"").unwrap();
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: good,
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: failed,
            },
        ]);
        let mut worker = AdapterProviderWorker::default();
        let mut pending = PendingEvents::new(crate::provider::ProviderDiagnostics::default());

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let events = drain_pending(&mut pending);

        assert_eq!(
            provider_source_state(&events, Provider::Codex),
            Some(ProviderSourceState::Unavailable {
                detail: "root_not_found".to_owned(),
            })
        );
        assert!(tail_read_calls(&worker, &good_root, &codex_artifact_name("good")).is_some());
    }

    #[test]
    fn unreadable_new_file_is_file_io_error_and_does_not_starve_sibling_file() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);

        let unreadable = codex_artifact(&root, "1-unreadable");
        let good = codex_artifact(&root, "2-good");
        std::fs::write(
            &unreadable,
            codex_records("unreadable-owner", "unreadable-agent", "unreadable-event"),
        )
        .unwrap();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
        std::fs::write(
            &good,
            codex_records("good-owner", "good-agent", "good-event"),
        )
        .unwrap();
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: unreadable.clone(),
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: good,
            },
        ]);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o600)).unwrap();
        let events = drain_pending(&mut pending);

        assert_eq!(
            provider_source_state(&events, Provider::Codex),
            Some(ProviderSourceState::Unavailable {
                detail: "file_io_error".to_owned(),
            }),
            "per-file permission failure was mislabeled as a root failure"
        );
        assert!(
            tail_read_calls(&worker, &root, &codex_artifact_name("2-good"))
                .is_some_and(|calls| calls > 0),
            "unreadable file starved its readable sibling"
        );
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::Activity { event_id, .. }
                if event_id == "prov:codex:act:good-event"
        )));
    }

    #[test]
    fn unreadable_subdirectory_is_file_io_error_and_does_not_starve_sibling_file() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);

        let unreadable = root.join("1-unreadable");
        let good = codex_artifact(&root, "2-good");
        std::fs::create_dir(&unreadable).unwrap();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
        std::fs::write(
            &good,
            codex_records("good-owner", "good-agent", "good-event"),
        )
        .unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: good,
        }]);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let events = drain_pending(&mut pending);

        assert_eq!(
            provider_source_state(&events, Provider::Codex),
            Some(ProviderSourceState::Unavailable {
                detail: "file_io_error".to_owned(),
            }),
            "nested traversal failure was mislabeled as a root failure"
        );
        assert!(
            tail_read_calls(&worker, &root, &codex_artifact_name("2-good"))
                .is_some_and(|calls| calls > 0),
            "unreadable subdirectory starved its readable sibling"
        );
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::Activity { event_id, .. }
                if event_id == "prov:codex:act:good-event"
        )));
    }

    #[test]
    fn dirty_root_walk_preserves_tail_bootstrap_baseline_and_offset_until_clean_scan() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        let nested = root.join("nested");
        let relative = PathBuf::from("nested").join(codex_artifact_name("session"));
        let path = root.join(&relative);
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(
            &path,
            codex_records("retained-owner", "retained-agent", "historical-event"),
        )
        .unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        let mut pending = PendingEvents::new(diagnostics);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);
        append_codex_activity(&path, "retained-agent", "post-baseline-event");
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let appended = drain_pending(&mut pending);
        assert!(appended.iter().any(|event| matches!(
            event,
            ProviderEvent::Activity { event_id, .. }
                if event_id == "prov:codex:act:post-baseline-event"
        )));

        let state = worker.roots.get(&(Provider::Codex, root.clone())).unwrap();
        let path_id = state
            .discovery
            .files()
            .into_iter()
            .find(|file| file.relative_path == relative)
            .unwrap()
            .path_id;
        let tail_before = worker.tails.get(&path_id).unwrap();
        let state_before = (tail_before.generation(), tail_before.offset());
        assert!(state.discovery.baseline().contained(&root, &relative));
        assert!(worker.bootstrap_emitted.contains(&path_id));

        std::fs::set_permissions(&nested, std::fs::Permissions::from_mode(0o000)).unwrap();
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let dirty_events = drain_pending(&mut pending);
        let dirty_state = worker
            .tails
            .get(&path_id)
            .map(|tail| (tail.generation(), tail.offset()));
        let dirty_file_retained = worker
            .roots
            .get(&(Provider::Codex, root.clone()))
            .unwrap()
            .discovery
            .files()
            .into_iter()
            .any(|file| file.path_id == path_id);
        let dirty_bootstrap_retained = worker.bootstrap_emitted.contains(&path_id);
        let dirty_baseline_retained = worker
            .roots
            .get(&(Provider::Codex, root.clone()))
            .unwrap()
            .discovery
            .baseline()
            .contained(&root, &relative);
        std::fs::set_permissions(&nested, std::fs::Permissions::from_mode(0o700)).unwrap();

        assert_eq!(
            dirty_state,
            Some(state_before),
            "dirty walk destroyed tail state"
        );
        assert!(
            dirty_file_retained,
            "dirty walk removed the discovered file"
        );
        assert!(
            dirty_bootstrap_retained,
            "dirty walk removed the bootstrap marker"
        );
        assert!(dirty_baseline_retained, "dirty walk pruned the baseline");
        assert_eq!(
            provider_source_state(&dirty_events, Provider::Codex),
            Some(ProviderSourceState::Unavailable {
                detail: "file_io_error".to_owned(),
            })
        );

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let restored_events = drain_pending(&mut pending);
        assert_eq!(
            worker
                .tails
                .get(&path_id)
                .map(|tail| (tail.generation(), tail.offset())),
            Some(state_before)
        );
        assert!(
            !restored_events.iter().any(|event| matches!(
                event,
                ProviderEvent::SessionResolved { .. } | ProviderEvent::Activity { .. }
            )),
            "clean recovery re-emitted retained provider history: {restored_events:?}"
        );
    }

    #[test]
    fn per_entry_not_found_is_silent_churn_and_does_not_reject_baseline() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let path = codex_artifact(&root, "vanishing-race");
        std::fs::write(&path, codex_records("owner", "agent", "event")).unwrap();
        let hook_path = path.clone();
        crate::provider::set_discovery_file_type_hook(move |entry_path| {
            assert_eq!(entry_path, hook_path);
            std::fs::remove_file(entry_path).unwrap();
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "injected enumeration-to-stat deletion race",
            ))
        });
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path,
        }]);
        let mut pending = PendingEvents::new(diagnostics);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let events = drain_pending(&mut pending);

        assert!(
            worker.roots.contains_key(&(Provider::Codex, root)),
            "NotFound churn rejected the run baseline"
        );
        assert_eq!(
            provider_source_state(&events, Provider::Codex),
            Some(ProviderSourceState::Available),
            "NotFound churn poisoned the availability sweep: {events:?}"
        );
    }

    #[test]
    fn target_before_root_exists_recovers_when_the_root_appears() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("fresh/.codex/sessions");
        let path = codex_artifact(&root, "future");
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path,
        }]);
        let mut worker = AdapterProviderWorker::default();
        let mut pending = PendingEvents::new(crate::provider::ProviderDiagnostics::default());

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let unavailable = drain_pending(&mut pending);
        assert_eq!(
            provider_source_state(&unavailable, Provider::Codex),
            Some(ProviderSourceState::Unavailable {
                detail: "root_not_found".to_owned(),
            })
        );

        std::fs::create_dir_all(root).unwrap();
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let recovered = drain_pending(&mut pending);
        assert_eq!(
            provider_source_state(&recovered, Provider::Codex),
            Some(ProviderSourceState::Available)
        );
    }

    #[test]
    fn targeted_file_not_found_is_normal_churn() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let path = codex_artifact(&root, "vanishing");
        std::fs::write(&path, b"").unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        let mut worker = AdapterProviderWorker::default();
        let mut pending = PendingEvents::new(crate::provider::ProviderDiagnostics::default());

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);
        std::fs::remove_file(path).unwrap();
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let events = drain_pending(&mut pending);

        assert!(!events.iter().any(|event| matches!(
            event,
            ProviderEvent::SourceState {
                provider: Provider::Codex,
                state: ProviderSourceState::Unavailable { .. },
            }
        )));
        assert_eq!(
            provider_source_state(&events, Provider::Codex),
            Some(ProviderSourceState::Available)
        );
    }

    #[test]
    fn never_targeted_provider_emits_not_applicable_only_on_initial_transition() {
        let mut worker = AdapterProviderWorker::default();
        let mut pending = PendingEvents::new(crate::provider::ProviderDiagnostics::default());

        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let initial = drain_pending(&mut pending);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let repeated = drain_pending(&mut pending);

        assert_eq!(
            provider_source_state(&initial, Provider::Claude),
            Some(ProviderSourceState::NotApplicable)
        );
        assert_eq!(
            provider_source_state(&initial, Provider::Codex),
            Some(ProviderSourceState::NotApplicable)
        );
        assert!(
            repeated.is_empty(),
            "targetless steady state re-emitted availability"
        );
    }

    #[test]
    fn failed_provider_root_does_not_affect_sibling_provider() {
        let directory = tempfile::tempdir().unwrap();
        let claude_root = directory.path().join("claude/.claude/projects");
        let codex_root = directory.path().join("codex/.codex/sessions");
        std::fs::create_dir_all(&claude_root).unwrap();
        let claude = claude_root.join("d414d449-40ef-448f-9c0b-fb239dc81bd8.jsonl");
        std::fs::write(&claude, b"").unwrap();
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Claude,
                path: claude,
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: codex_artifact(&codex_root, "missing"),
            },
        ]);
        let mut worker = AdapterProviderWorker::default();
        let mut pending = PendingEvents::new(crate::provider::ProviderDiagnostics::default());

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let events = drain_pending(&mut pending);

        assert_eq!(
            provider_source_state(&events, Provider::Claude),
            Some(ProviderSourceState::Available)
        );
        assert_eq!(
            provider_source_state(&events, Provider::Codex),
            Some(ProviderSourceState::Unavailable {
                detail: "root_not_found".to_owned(),
            })
        );
    }

    #[test]
    fn relative_target_is_skipped_without_aborting_sibling_provider_work() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("codex/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics.clone());
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);

        let valid = codex_artifact(&root, "valid");
        std::fs::write(
            &valid,
            codex_records("valid-owner", "valid-agent", "valid-event"),
        )
        .unwrap();
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Claude,
                path: PathBuf::from("relative.jsonl"),
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: valid,
            },
        ]);

        let result = try_process_adapter_worker(&mut worker, &targets, &mut pending);

        assert!(
            result.is_ok(),
            "relative target aborted the complete provider cycle: {result:?}"
        );
        let events = drain_pending(&mut pending);
        assert_eq!(diagnostics.invalid_targets(), 1);
        assert_eq!(
            provider_source_state(&events, Provider::Codex),
            Some(ProviderSourceState::Available)
        );
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::Activity { event_id, .. }
                if event_id == "prov:codex:act:valid-event"
        )));
        assert!(
            tail_read_calls(&worker, &root, &codex_artifact_name("valid"))
                .is_some_and(|calls| calls > 0)
        );
    }

    #[test]
    fn mis_shaped_pane_target_is_rejected_and_diagnosed() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("codex/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let target = root.join("session.jsonl");
        std::fs::write(&target, b"{}\n").unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root,
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: target.clone(),
        }]);
        let mut pending = PendingEvents::new(diagnostics.clone());

        process_adapter_worker(&mut worker, &targets, &mut pending);

        assert_eq!(diagnostics.invalid_targets(), 1);
        assert!(!worker.log_admission.is_admitted_path(&target));
    }

    #[test]
    fn untargeting_transition_precedes_sustained_deferred_backlog_gate() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("codex/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let path = codex_artifact(&root, "target");
        std::fs::write(&path, b"").unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root,
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::with_capacity(1, diagnostics);
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path,
        }]);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);
        assert!(matches!(
            pending.merge(capacity_blocker("resident")),
            MergeOutcome::Accepted
        ));
        worker.deferred.push_back(capacity_blocker("deferred"));

        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let events = drain_pending(&mut pending);

        assert_eq!(
            provider_source_state(&events, Provider::Codex),
            Some(ProviderSourceState::NotApplicable),
            "untargeting transition was hidden behind the deferred-entity drain gate"
        );
        assert_eq!(worker.deferred.len(), 1);
    }

    #[test]
    fn logical_sweep_recovers_even_when_every_physical_cycle_saturates() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        let first = codex_artifact(&root, "first");
        let second = codex_artifact(&root, "second");
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: first.clone(),
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: second.clone(),
            },
        ]);
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::with_capacity(1, diagnostics);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);

        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(&first, codex_records("first-owner", "first-agent", "first")).unwrap();
        std::fs::write(
            &second,
            codex_records("second-owner", "second-agent", "second"),
        )
        .unwrap();
        let mut recovered = false;
        for _ in 0..12 {
            process_adapter_worker(&mut worker, &targets, &mut pending);
            assert!(
                !worker.deferred.is_empty(),
                "physical cycle did not saturate as arranged"
            );
            let all_visited = [codex_artifact_name("first"), codex_artifact_name("second")]
                .into_iter()
                .all(|name| tail_read_calls(&worker, &root, &name).is_some_and(|calls| calls > 0));
            let events = drain_pending(&mut pending);
            if provider_source_state(&events, Provider::Codex)
                == Some(ProviderSourceState::Available)
            {
                assert!(
                    all_visited,
                    "availability recovered before the logical sweep completed"
                );
                recovered = true;
                break;
            }
        }

        assert!(recovered, "logical sweep did not recover within 12 cycles");
    }

    #[test]
    fn mid_order_topology_addition_blocks_completion_until_visited() {
        let directory = tempfile::tempdir().unwrap();
        let late_root = directory.path().join("z/.codex/sessions");
        let added_root = directory.path().join("a/.codex/sessions");
        std::fs::create_dir_all(&late_root).unwrap();
        std::fs::create_dir_all(&added_root).unwrap();
        let late = codex_artifact(&late_root, "late");
        let added = codex_artifact(&added_root, "added");
        std::fs::write(&late, codex_records("late-owner", "late-agent", "late")).unwrap();
        let initial_targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: late.clone(),
        }]);
        let expanded_targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: late.clone(),
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: added.clone(),
            },
        ]);
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![
                DiscoveryRoot {
                    provider: Provider::Codex,
                    path: added_root.clone(),
                },
                DiscoveryRoot {
                    provider: Provider::Codex,
                    path: late_root.clone(),
                },
            ],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::with_capacity(1, diagnostics);
        pending.merge(capacity_blocker("initial-blocker"));
        process_adapter_worker(&mut worker, &initial_targets, &mut pending);
        let _ = drain_pending(&mut pending);
        append_codex_activity(&late, "late-appended-agent", "late-appended");
        std::fs::write(&added, codex_records("added-owner", "added-agent", "added")).unwrap();

        let mut observed_passed_addition = false;
        for _ in 0..4 {
            process_adapter_worker(&mut worker, &expanded_targets, &mut pending);
            let late_visited = tail_read_calls(&worker, &late_root, &codex_artifact_name("late"))
                .is_some_and(|calls| calls > 0);
            let added_visited =
                tail_read_calls(&worker, &added_root, &codex_artifact_name("added"))
                    .is_some_and(|calls| calls > 0);
            let events = drain_pending(&mut pending);
            if late_visited && !added_visited {
                assert_eq!(
                    provider_source_state(&events, Provider::Codex),
                    None,
                    "positional end emitted before the added universe member was visited"
                );
                observed_passed_addition = true;
                break;
            }
        }
        assert!(
            observed_passed_addition,
            "cursor did not reach the post-addition position within four cycles"
        );

        let mut completed = false;
        for _ in 0..6 {
            process_adapter_worker(&mut worker, &expanded_targets, &mut pending);
            let added_visited =
                tail_read_calls(&worker, &added_root, &codex_artifact_name("added"))
                    .is_some_and(|calls| calls > 0);
            let events = drain_pending(&mut pending);
            if provider_source_state(&events, Provider::Codex)
                == Some(ProviderSourceState::Available)
            {
                assert!(added_visited);
                completed = true;
                break;
            }
        }
        assert!(
            completed,
            "expanded sweep did not complete within six cycles"
        );
    }

    #[test]
    fn provisional_availability_precedes_sweep_and_unavailable_does_not_flicker() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let initial = codex_artifact(&root, "initial");
        std::fs::write(
            &initial,
            codex_records("initial-owner", "initial-agent", "initial"),
        )
        .unwrap();
        let initial_targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: initial,
        }]);
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::default();
        let mut pending = PendingEvents::with_capacity(1, diagnostics);
        pending.merge(capacity_blocker("startup-blocker"));
        let (quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
        let (coverage_sender, coverage) =
            watch::channel(SourceCoverageRegistry::new(SourceAvailability::Available));
        let mut tracker = CoverageTracker::new(
            SourceAvailability::Available,
            coverage_sender,
            quality_sender,
        );
        tracker.set_herdr_quality(ObservationQuality::Live);

        process_adapter_worker(&mut worker, &initial_targets, &mut pending);
        let provisional = drain_pending(&mut pending);
        apply_source_states(&mut tracker, &provisional);
        assert_eq!(
            provider_source_state(&provisional, Provider::Codex),
            Some(ProviderSourceState::Available)
        );
        assert_eq!(
            tail_read_calls(&worker, &root, &codex_artifact_name("initial")),
            Some(0)
        );

        std::fs::remove_dir_all(&root).unwrap();
        process_adapter_worker(&mut worker, &initial_targets, &mut pending);
        let unavailable = drain_pending(&mut pending);
        apply_source_states(&mut tracker, &unavailable);
        assert_eq!(
            provider_source_state(&unavailable, Provider::Codex),
            Some(ProviderSourceState::Unavailable {
                detail: "root_not_found".to_owned(),
            })
        );
        assert_eq!(*quality.borrow(), ObservationQuality::Degraded);

        std::fs::create_dir_all(&root).unwrap();
        let recovery_paths = (0..3)
            .map(|index| {
                let path = codex_artifact(&root, &format!("recovery-{index}"));
                std::fs::write(
                    &path,
                    codex_records(
                        &format!("recovery-owner-{index}"),
                        &format!("recovery-agent-{index}"),
                        &format!("recovery-{index}"),
                    ),
                )
                .unwrap();
                path
            })
            .collect::<Vec<_>>();
        let recovery_targets =
            TargetSet::new(recovery_paths.into_iter().map(|path| ProviderTarget {
                provider: Provider::Codex,
                path,
            }));

        for _ in 0..2 {
            process_adapter_worker(&mut worker, &recovery_targets, &mut pending);
            let events = drain_pending(&mut pending);
            apply_source_states(&mut tracker, &events);
            assert_eq!(
                provider_source_state(&events, Provider::Codex),
                None,
                "incomplete sweep flickered away from the unavailable aggregate"
            );
            assert!(
                coverage
                    .borrow()
                    .summary()
                    .contains("codex=unavailable(root_not_found)")
            );
            assert_eq!(*quality.borrow(), ObservationQuality::Degraded);
        }
    }

    #[tokio::test]
    async fn file_created_after_worker_readiness_before_first_target_starts_at_zero() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let (sender, mut events) = mpsc::channel(16);
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let thread = crate::provider::spawn_provider_thread_with_rescan_interval(
            AdapterProviderWorker::new(
                vec![DiscoveryRoot {
                    provider: Provider::Codex,
                    path: root.clone(),
                }],
                diagnostics,
            ),
            sender,
            None,
            Duration::from_secs(30),
        )
        .unwrap();
        wait_for_provider_readiness(&mut events).await;

        let path = codex_artifact(&root, "created-after-ready");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"ready-owner\",\"session_id\":\"ready-owner\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"sub_agent_activity\",\"event_id\":\"after-ready-event\",\"occurred_at_ms\":1,\"agent_thread_id\":\"ready-owner\",\"agent_path\":\"/root\",\"kind\":\"interacted\"}}\n"
            ),
        )
        .unwrap();
        thread.update_targets(TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path,
        }]));

        let activity = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(event @ ProviderEvent::Activity { .. }) = events.recv().await {
                    break event;
                }
            }
        })
        .await;
        thread.stop().await.unwrap();

        assert!(activity.is_ok(), "byte-zero activity was not emitted");
    }

    #[tokio::test]
    async fn file_existing_before_worker_readiness_starts_at_eof() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("home/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let path = codex_artifact(&root, "existing-before-ready");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"existing-owner\",\"session_id\":\"existing-owner\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"sub_agent_activity\",\"event_id\":\"existing-event\",\"occurred_at_ms\":1,\"agent_thread_id\":\"existing-owner\",\"agent_path\":\"/root\",\"kind\":\"interacted\"}}\n"
            ),
        )
        .unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let (sender, mut events) = mpsc::channel(16);
        let thread = crate::provider::spawn_provider_thread_with_rescan_interval(
            AdapterProviderWorker::new(
                vec![DiscoveryRoot {
                    provider: Provider::Codex,
                    path: root,
                }],
                diagnostics,
            ),
            sender,
            None,
            Duration::from_secs(30),
        )
        .unwrap();
        wait_for_provider_readiness(&mut events).await;
        thread.update_targets(TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path,
        }]));

        let activity = tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if let Some(event @ ProviderEvent::Activity { .. }) = events.recv().await {
                    break event;
                }
            }
        })
        .await;
        thread.stop().await.unwrap();

        assert!(
            activity.is_err(),
            "pre-existing activity replayed from byte zero"
        );
    }

    #[test]
    fn foreign_root_uses_first_scan_approximation_and_counts_it() {
        let directory = tempfile::tempdir().unwrap();
        let standard_root = directory.path().join("standard/.codex/sessions");
        let fallback_root = directory.path().join("foreign/.codex/sessions");
        std::fs::create_dir_all(&standard_root).unwrap();
        std::fs::create_dir_all(&fallback_root).unwrap();
        let path = codex_artifact(&fallback_root, "fallback");
        std::fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"fallback-owner\",\"session_id\":\"fallback-owner\"}}\n",
        )
        .unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: standard_root,
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path,
        }]);
        let mut pending = PendingEvents::new(diagnostics.clone());

        process_adapter_worker(&mut worker, &targets, &mut pending);

        assert_eq!(diagnostics.baseline_approximations(), 1);
    }

    #[test]
    fn standard_baseline_capture_failure_retries_and_counts_late_success() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let factory_attempts = Arc::clone(&attempts);
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new_with_root_state_factory(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
            move |provider, path| {
                if factory_attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "injected baseline capture failure",
                    ))
                } else {
                    AdapterRootState::new(provider, path)
                }
            },
        );
        let mut pending = PendingEvents::new(diagnostics.clone());

        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let first_cycle = drain_pending(&mut pending);
        assert_eq!(
            provider_source_state(&first_cycle, Provider::Codex),
            Some(ProviderSourceState::NotApplicable),
            "baseline capture failure blocked the readiness barrier"
        );
        assert!(!worker.roots.contains_key(&(Provider::Codex, root.clone())));

        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);

        assert_eq!(
            attempts.load(Ordering::Relaxed),
            2,
            "standard baseline capture was not retried on the next cycle"
        );
        assert!(worker.roots.contains_key(&(Provider::Codex, root)));
        assert_eq!(diagnostics.baseline_approximations(), 1);
    }

    #[test]
    fn standard_baseline_traversal_error_retries_before_accepting_baseline() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        let unreadable = root.join("unreadable");
        let existing = codex_artifact(&root, "existing");
        std::fs::create_dir_all(&unreadable).unwrap();
        std::fs::write(&existing, codex_records("owner", "agent", "event")).unwrap();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics.clone());

        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        assert!(
            !worker.roots.contains_key(&(Provider::Codex, root.clone())),
            "partial standard baseline was accepted"
        );

        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o700)).unwrap();
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);

        let state = worker
            .roots
            .get(&(Provider::Codex, root.clone()))
            .expect("standard baseline traversal was retried");
        assert!(
            state
                .discovery
                .baseline()
                .contained(&root, Path::new(&codex_artifact_name("existing"))),
            "successful retry did not capture the complete baseline"
        );
        assert_eq!(diagnostics.baseline_approximations(), 1);
    }

    #[test]
    fn delete_then_recreate_reopens_at_zero_with_a_new_generation() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("fallback-root");
        std::fs::create_dir_all(&root).unwrap();
        let path = codex_artifact(&root, "session");
        std::fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"recreate-owner\",\"session_id\":\"recreate-owner\"}}\n",
        )
        .unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        let mut worker = AdapterProviderWorker::default();
        let mut pending = PendingEvents::new(crate::provider::ProviderDiagnostics::default());

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let first_generation = worker.tails.values().next().unwrap().generation();
        let _ = drain_pending(&mut pending);

        std::fs::remove_file(&path).unwrap();
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"recreate-owner\",\"session_id\":\"recreate-owner\"}}\n",
                "{\"type\":\"event_msg\",\"payload\":{\"type\":\"sub_agent_activity\",\"event_id\":\"recreated-event\",\"occurred_at_ms\":2,\"agent_thread_id\":\"recreate-owner\",\"agent_path\":\"/root\",\"kind\":\"interacted\"}}\n"
            ),
        )
        .unwrap();

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let recreated_generation = worker.tails.values().next().unwrap().generation();
        let events = drain_pending(&mut pending);

        assert!(recreated_generation > first_generation);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ProviderEvent::Activity {
                    event_id,
                    position: SourcePosition { offset, .. },
                    ..
                } if event_id == "prov:codex:act:recreated-event" && *offset > 0
            )
        }));
    }

    #[test]
    fn overlapping_roots_share_one_path_id_and_generation_history() {
        let directory = tempfile::tempdir().unwrap();
        let outer = directory.path().join("outer");
        let inner = outer.join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        let absolute = codex_artifact(&inner, "shared");
        std::fs::write(&absolute, b"{}\n").unwrap();
        let mut outer_state = AdapterRootState::new(Provider::Codex, outer).unwrap();
        let mut inner_state = AdapterRootState::new(Provider::Codex, inner).unwrap();
        let mut worker = AdapterProviderWorker::default();
        let mut parser = AdapterBootstrapParser::default();
        assert!(
            worker
                .log_admission
                .admit_pane_artifact(Provider::Codex, &absolute)
        );

        outer_state
            .discovery
            .scan_admitted(
                &mut parser,
                &mut worker.interner,
                &worker.log_admission,
                &mut worker.admission_index,
                &worker.diagnostics,
            )
            .unwrap();
        inner_state
            .discovery
            .scan_admitted(
                &mut parser,
                &mut worker.interner,
                &worker.log_admission,
                &mut worker.admission_index,
                &worker.diagnostics,
            )
            .unwrap();
        let outer_id = outer_state.discovery.files()[0].path_id;
        let inner_id = inner_state.discovery.files()[0].path_id;
        let first = worker.next_open_generation(&absolute);
        worker.observe_generation(absolute.clone(), first + 4);
        let reopened = worker.next_open_generation(&absolute);

        assert_eq!(outer_id, inner_id);
        assert_eq!(first, 0);
        assert_eq!(reopened, 5);
        assert_eq!(worker.generations.len(), 1);
    }

    #[test]
    fn overlapping_fallback_root_ownership_flaps_keep_one_monotone_tail() {
        let directory = tempfile::tempdir().unwrap();
        let outer = directory.path().join("fallback");
        let inner = outer.join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        let outer_target = codex_artifact(&outer, "owner");
        let shared = codex_artifact(&inner, "shared");
        std::fs::write(
            &outer_target,
            codex_records("shared-owner", "outer-agent", "outer-initial"),
        )
        .unwrap();
        std::fs::write(
            &shared,
            codex_records("shared-owner", "shared-agent", "shared-initial"),
        )
        .unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(Vec::new(), diagnostics.clone());
        let evidence_parent = crate::provider::facts::SessionScope::Codex {
            rollout_id: "pane-owner".to_owned(),
        };
        worker
            .log_admission
            .admit_pane_session(Provider::Codex, "pane-owner");
        worker
            .admission_index
            .insert_codex_rollout("shared-owner", shared.clone());
        assert!(
            worker
                .log_admission
                .on_evidence(
                    &evidence_parent,
                    &crate::provider::facts::EvidenceId::Uuid("shared-owner".to_owned()),
                    &worker.admission_index,
                )
                .is_some()
        );
        let mut pending = PendingEvents::new(diagnostics.clone());
        let outer_only = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: outer_target.clone(),
        }]);
        let inner_only = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: shared.clone(),
        }]);
        let both = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: outer_target,
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: shared.clone(),
            },
        ]);

        process_adapter_worker(&mut worker, &outer_only, &mut pending);
        let _ = drain_pending(&mut pending);
        append_codex_activity(&shared, "flap-agent-a", "flap-a");
        process_adapter_worker(&mut worker, &outer_only, &mut pending);
        let generation_a = activity_generation(&drain_pending(&mut pending), "flap-a").unwrap();

        process_adapter_worker(&mut worker, &inner_only, &mut pending);
        let _ = drain_pending(&mut pending);
        append_codex_activity(&shared, "flap-agent-b", "flap-b");
        process_adapter_worker(&mut worker, &inner_only, &mut pending);
        let generation_b = activity_generation(&drain_pending(&mut pending), "flap-b").unwrap();

        append_codex_activity(&shared, "flap-agent-c", "flap-c");
        process_adapter_worker(&mut worker, &outer_only, &mut pending);
        let generation_c = activity_generation(&drain_pending(&mut pending), "flap-c").unwrap();

        assert_eq!(
            [generation_a, generation_b, generation_c],
            [generation_a; 3],
            "root ownership flap resurrected a stale tail generation"
        );
        assert_eq!(tail_count_for_absolute(&worker, &shared), 1);

        process_adapter_worker(&mut worker, &both, &mut pending);
        assert_eq!(diagnostics.duplicate_path_targets(), 1);
    }

    #[test]
    fn overlapping_root_aliases_collapse_to_one_file_sweep_member() {
        let directory = tempfile::tempdir().unwrap();
        let outer = directory.path().join("fallback");
        let inner = outer.join("inner");
        std::fs::create_dir_all(&outer).unwrap();
        let outer_target = codex_artifact(&outer, "owner");
        let shared = codex_artifact(&inner, "shared");
        std::fs::write(
            &outer_target,
            codex_records("shared-owner", "outer-agent", "outer-initial"),
        )
        .unwrap();
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: outer_target,
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: shared.clone(),
            },
        ]);
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(Vec::new(), diagnostics.clone());
        let mut pending = PendingEvents::new(diagnostics);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        assert_eq!(
            provider_source_state(&drain_pending(&mut pending), Provider::Codex),
            Some(ProviderSourceState::Unavailable {
                detail: "root_not_found".to_owned(),
            })
        );

        std::fs::create_dir_all(&inner).unwrap();
        std::fs::write(
            &shared,
            codex_records("shared-owner", "shared-agent", "shared-initial"),
        )
        .unwrap();
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let recovered = drain_pending(&mut pending);

        assert_eq!(
            provider_source_state(&recovered, Provider::Codex),
            Some(ProviderSourceState::Available),
            "unvisited root alias prevented the overlapping-root sweep from completing"
        );
    }

    #[test]
    fn green_guard_saturation_continues_after_current_file_without_starvation() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::with_capacity(1, diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);

        let first = codex_artifact(&root, "1-first");
        let second = codex_artifact(&root, "2-second");
        std::fs::write(
            &first,
            codex_records("first-owner", "first-agent-0", "first-0"),
        )
        .unwrap();
        std::fs::write(
            &second,
            codex_records("second-owner", "second-owner", "second-0"),
        )
        .unwrap();
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: first.clone(),
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: second,
            },
        ]);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        for cycle in 1..=4 {
            let _ = drain_pending(&mut pending);
            append_codex_activity(
                &first,
                &format!("first-agent-{cycle}"),
                &format!("first-{cycle}"),
            );
            process_adapter_worker(&mut worker, &targets, &mut pending);
            if tail_read_calls(&worker, &root, &codex_artifact_name("2-second"))
                .is_some_and(|calls| calls > 0)
            {
                break;
            }
        }

        assert!(
            tail_read_calls(&worker, &root, &codex_artifact_name("2-second"))
                .is_some_and(|calls| calls > 0),
            "second file was not polled within the bounded cycle budget"
        );
    }

    #[test]
    fn vanished_cursor_key_resumes_at_next_greater_file() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::with_capacity(1, diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);

        let first = codex_artifact(&root, "1-first");
        let vanished = codex_artifact(&root, "2-vanished");
        let third = codex_artifact(&root, "3-third");
        std::fs::write(
            &first,
            codex_records("first-owner", "first-agent-0", "first-0"),
        )
        .unwrap();
        std::fs::write(
            &vanished,
            codex_records("middle-owner", "middle-owner", "middle-0"),
        )
        .unwrap();
        std::fs::write(
            &third,
            codex_records("third-owner", "third-owner", "third-0"),
        )
        .unwrap();
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: first.clone(),
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: vanished.clone(),
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: third,
            },
        ]);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);
        std::fs::remove_file(&vanished).unwrap();
        append_codex_activity(&first, "first-agent-1", "first-1");
        process_adapter_worker(&mut worker, &targets, &mut pending);

        assert!(
            tail_read_calls(&worker, &root, &codex_artifact_name("3-third")).is_some(),
            "cursor disappearance restarted at the starving first file"
        );
    }

    #[test]
    fn bootstrap_saturation_resumes_same_file() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::with_capacity(1, diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);
        let path = codex_artifact(&root, "bootstrap");
        std::fs::write(
            &path,
            codex_records("bootstrap-owner", "bootstrap-owner", "bootstrap-activity"),
        )
        .unwrap();
        pending.merge(ProviderEvent::Activity {
            provider: Provider::Codex,
            agent_thread_id: "capacity-blocker".to_owned(),
            activity: MinimalProviderMetadata::default(),
            depth: None,
            event_id: "capacity-blocker-event".to_owned(),
            observed_at_ms: 0,
            position: SourcePosition {
                path_id: u32::MAX,
                generation: 0,
                offset: 0,
            },
        });
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path,
        }]);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        assert_eq!(
            tail_read_calls(&worker, &root, &codex_artifact_name("bootstrap")),
            Some(0)
        );
        assert!(worker.resume_cursor.is_some());

        let _ = drain_pending(&mut pending);
        process_adapter_worker(&mut worker, &targets, &mut pending);

        assert_eq!(
            tail_read_calls(&worker, &root, &codex_artifact_name("bootstrap")),
            Some(1)
        );
    }

    #[test]
    fn recreated_file_bootstrap_precedes_tail_after_stale_cursor() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::with_capacity(1, diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);

        let path = codex_artifact(&root, "recreated");
        std::fs::write(
            &path,
            codex_records("old-owner", "old-owner", "old-activity"),
        )
        .unwrap();
        assert!(matches!(
            pending.merge(capacity_blocker("bootstrap-blocker")),
            MergeOutcome::Accepted
        ));
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        assert!(worker.resume_cursor.is_some());

        std::fs::remove_file(&path).unwrap();
        let _ = drain_pending(&mut pending);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);
        std::fs::write(
            &path,
            codex_records("fresh-owner", "fresh-owner", "fresh-activity"),
        )
        .unwrap();

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let events = drain_pending(&mut pending);
        let bootstrap = events.iter().position(|event| {
            matches!(
                event,
                ProviderEvent::SessionResolved {
                    agent_thread_id,
                    ..
                } if agent_thread_id == "fresh-owner"
            )
        });
        let activity = events.iter().position(|event| {
            matches!(
                event,
                ProviderEvent::Activity { event_id, .. }
                    if event_id == "prov:codex:act:fresh-activity"
            )
        });

        assert!(
            bootstrap.is_some(),
            "recreated file skipped its fresh SessionResolved event at a stale TailPoll cursor"
        );
        assert!(
            bootstrap < activity,
            "recreated file activity preceded its fresh bootstrap: {events:?}"
        );
    }

    #[test]
    fn green_guard_in_place_rotation_does_not_reemit_bootstrap() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);
        let path = codex_artifact(&root, "rotated");
        std::fs::write(
            &path,
            codex_records("original-owner", "original-owner", "original-activity"),
        )
        .unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);

        std::fs::rename(&path, root.join("rotated.old")).unwrap();
        std::fs::write(
            &path,
            codex_records(
                "replacement-owner",
                "replacement-owner",
                "replacement-activity",
            ),
        )
        .unwrap();
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let events = drain_pending(&mut pending);

        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::Activity { event_id, .. }
                if event_id == "prov:codex:act:replacement-activity"
        )));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderEvent::SessionResolved { .. })),
            "continuous-path rotation re-emitted a stale discovery bootstrap"
        );
    }

    #[test]
    fn capacity_stops_tail_reads_and_deferred_memory_growth() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::with_capacity(1, diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);
        let path = codex_artifact(&root, "bounded");
        let mut contents = codex_records("bounded-owner", "bounded-agent-0", "bounded-0");
        for index in 1..8 {
            contents.push_str(&format!(
                "{{\"type\":\"event_msg\",\"payload\":{{\"type\":\"sub_agent_activity\",\"event_id\":\"bounded-{index}\",\"occurred_at_ms\":1,\"agent_thread_id\":\"bounded-agent-{index}\",\"agent_path\":\"/root/bounded-agent-{index}\",\"kind\":\"interacted\"}}}}\n"
            ));
        }
        std::fs::write(&path, contents).unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path,
        }]);

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let read_calls = tail_read_calls(&worker, &root, &codex_artifact_name("bounded")).unwrap();
        let deferred_len = worker.deferred.len();
        let cursor = worker.resume_cursor.clone();
        assert!(deferred_len > 0);

        for _ in 0..3 {
            process_adapter_worker(&mut worker, &targets, &mut pending);
        }

        assert_eq!(
            tail_read_calls(&worker, &root, &codex_artifact_name("bounded")),
            Some(read_calls)
        );
        assert_eq!(worker.deferred.len(), deferred_len);
        assert_eq!(worker.resume_cursor, cursor);
        assert_eq!(pending.entity_count(), 1);
    }

    #[test]
    fn dormant_one_mib_backlog_drains_to_cycle_goalpost() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);
        let path = codex_artifact(&root, "dormant");
        std::fs::write(&path, b"").unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);

        let mut backlog = vec![b'x'; 1024 * 1024];
        backlog.extend_from_slice(
            b"\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"sub_agent_activity\",\"event_id\":\"dormant-tail\",\"occurred_at_ms\":3,\"agent_thread_id\":\"dormant-agent\",\"agent_path\":\"/root\",\"kind\":\"interacted\"}}\n",
        );
        std::fs::write(&path, backlog).unwrap();
        let goalpost = std::fs::metadata(&path).unwrap().len();

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let path_id = worker
            .roots
            .get(&(Provider::Codex, root))
            .unwrap()
            .discovery
            .files()[0]
            .path_id;
        let offset = worker.tails.get(&path_id).unwrap().offset();

        assert_eq!(
            offset, goalpost,
            "dormant backlog remained queued for another polling interval"
        );
    }

    #[test]
    fn mid_drain_rotation_refreezes_replacement_goalpost_in_same_cycle() {
        use crate::provider::tail::MAX_TAIL_CHUNK_BYTES;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);
        let path = codex_artifact(&root, "rotating");
        std::fs::write(&path, b"").unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);

        std::fs::write(&path, vec![b'o'; MAX_TAIL_CHUNK_BYTES + 64 * 1024]).unwrap();
        let mut replacement = vec![b'r'; MAX_TAIL_CHUNK_BYTES * 2 + 64 * 1024];
        replacement.extend_from_slice(
            b"\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"sub_agent_activity\",\"event_id\":\"replacement-tail\",\"occurred_at_ms\":3,\"agent_thread_id\":\"replacement-agent\",\"agent_path\":\"/root\",\"kind\":\"interacted\"}}\n",
        );
        let replacement_goalpost = replacement.len() as u64;
        let rotated = Arc::new(AtomicBool::new(false));
        let hook_rotated = Arc::clone(&rotated);
        let rotated_path = root.join("rotating.old");
        worker.set_after_tail_chunk(move |path, _| {
            if !hook_rotated.swap(true, Ordering::AcqRel) {
                std::fs::rename(path, &rotated_path).unwrap();
                std::fs::write(path, &replacement).unwrap();
            }
        });

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let events = drain_pending(&mut pending);
        let path_id = worker
            .roots
            .get(&(Provider::Codex, root))
            .unwrap()
            .discovery
            .files()[0]
            .path_id;
        let tail = worker.tails.get(&path_id).unwrap();

        assert!(rotated.load(Ordering::Acquire));
        assert_eq!(
            tail.offset(),
            replacement_goalpost,
            "replacement file was not drained to its own goalpost in the rotation cycle"
        );
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::Activity { event_id, .. }
                if event_id == "prov:codex:act:replacement-tail"
        )));
    }

    #[test]
    fn second_generation_change_defers_remaining_drain_to_next_cycle() {
        use crate::provider::tail::MAX_TAIL_CHUNK_BYTES;

        fn rotating_contents(index: usize) -> Vec<u8> {
            let mut contents = codex_records(
                &format!("swap-owner-{index}"),
                &format!("swap-agent-{index}"),
                &format!("swap-event-{index}"),
            )
            .into_bytes();
            contents.resize(MAX_TAIL_CHUNK_BYTES * 2 + 64 * 1024, b'x');
            contents
        }

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let path = codex_artifact(&root, "1-rotating");
        let sibling = codex_artifact(&root, "2-sibling");
        std::fs::write(&path, b"").unwrap();
        std::fs::write(&sibling, b"").unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: path.clone(),
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: sibling.clone(),
            },
        ]);
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);

        std::fs::write(&path, rotating_contents(0)).unwrap();
        append_codex_activity(&sibling, "sibling-agent", "sibling-event");
        let swaps = Arc::new(AtomicUsize::new(0));
        let hook_swaps = Arc::clone(&swaps);
        let hook_path = path.clone();
        let hook_root = root.clone();
        worker.set_after_tail_chunk(move |read_path, _| {
            if read_path != hook_path || hook_swaps.load(Ordering::Acquire) >= 8 {
                return;
            }
            let index = hook_swaps.fetch_add(1, Ordering::AcqRel);
            std::fs::rename(read_path, hook_root.join(format!("rotating-{index}.old"))).unwrap();
            std::fs::write(read_path, rotating_contents(index + 1)).unwrap();
        });

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let first_cycle = drain_pending(&mut pending);

        assert_eq!(
            swaps.load(Ordering::Acquire),
            3,
            "drain refroze more than one replacement goalpost"
        );
        assert!(
            first_cycle.iter().any(|event| matches!(
                event,
                ProviderEvent::Activity { event_id, .. }
                    if event_id == "prov:codex:act:swap-event-2"
            )),
            "records consumed by the second-change poll were dropped: {first_cycle:?}"
        );
        assert!(
            first_cycle.iter().any(|event| matches!(
                event,
                ProviderEvent::Activity { event_id, .. }
                    if event_id == "prov:codex:act:sibling-event"
            )),
            "continuously replaced path pinned out its sibling"
        );

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let second_cycle = drain_pending(&mut pending);

        assert_eq!(
            swaps.load(Ordering::Acquire),
            6,
            "next cycle did not continue the deferred drain"
        );
        assert!(second_cycle.iter().any(|event| matches!(
            event,
            ProviderEvent::Activity { event_id, .. }
                if event_id == "prov:codex:act:swap-event-5"
        )));
    }

    #[test]
    fn active_appender_stops_at_first_poll_goalpost_and_later_file_is_polled() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);
        let active = codex_artifact(&root, "1-active");
        let later = codex_artifact(&root, "2-later");
        std::fs::write(&active, b"").unwrap();
        std::fs::write(&later, b"").unwrap();
        let targets = TargetSet::new([
            ProviderTarget {
                provider: Provider::Codex,
                path: active.clone(),
            },
            ProviderTarget {
                provider: Provider::Codex,
                path: later.clone(),
            },
        ]);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);

        std::fs::write(&active, vec![b'x'; 300 * 1024]).unwrap();
        append_codex_activity(&later, "later-agent", "later-event");
        let goalpost = std::fs::metadata(&active).unwrap().len();
        let appended = Arc::new(AtomicBool::new(false));
        let hook_appended = Arc::clone(&appended);
        let hook_active = active.clone();
        worker.set_after_tail_chunk(move |path, _| {
            if path == hook_active && !hook_appended.swap(true, Ordering::AcqRel) {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(path)
                    .unwrap()
                    .write_all(&vec![b'y'; 300 * 1024])
                    .unwrap();
            }
        });

        process_adapter_worker(&mut worker, &targets, &mut pending);
        let events = drain_pending(&mut pending);
        let active_id = worker
            .roots
            .get(&(Provider::Codex, root.clone()))
            .unwrap()
            .discovery
            .files()
            .into_iter()
            .find(|file| file.relative_path == Path::new(&codex_artifact_name("1-active")))
            .unwrap()
            .path_id;

        assert_eq!(
            worker.tails.get(&active_id).unwrap().offset(),
            goalpost,
            "tail chased bytes appended after the first-poll snapshot"
        );
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::Activity { event_id, .. }
                if event_id == "prov:codex:act:later-event"
        )));
        assert!(
            tail_read_calls(&worker, &root, &codex_artifact_name("2-later"))
                .is_some_and(|calls| calls > 0)
        );
    }

    #[test]
    fn stop_flag_mid_drain_exits_after_current_chunk() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);
        let path = codex_artifact(&root, "stopping");
        std::fs::write(&path, b"").unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path: path.clone(),
        }]);
        process_adapter_worker(&mut worker, &targets, &mut pending);
        let _ = drain_pending(&mut pending);
        let mut backlog = vec![b'x'; 1024 * 1024];
        backlog.push(b'\n');
        std::fs::write(&path, backlog).unwrap();
        let goalpost = std::fs::metadata(&path).unwrap().len();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let hook_stop = Arc::clone(&stop_flag);
        worker.set_after_tail_chunk(move |_, _| hook_stop.store(true, Ordering::Release));
        let reads_before =
            tail_read_calls(&worker, &root, &codex_artifact_name("stopping")).unwrap();

        try_process_adapter_worker_with_stop(&mut worker, &targets, &mut pending, &stop_flag)
            .unwrap();
        let path_id = worker
            .roots
            .get(&(Provider::Codex, root.clone()))
            .unwrap()
            .discovery
            .files()[0]
            .path_id;
        let tail = worker.tails.get(&path_id).unwrap();

        assert!(
            tail.offset() < goalpost,
            "stop flag was ignored until the complete backlog drained"
        );
        assert_eq!(tail.read_calls(), reads_before + 1);
    }

    #[test]
    fn oversized_record_emits_one_malformed_then_valid_activity_lands() {
        use crate::provider::tail::{MAX_TAIL_CHUNK_BYTES, MAX_TAIL_RECORD_BYTES};

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("standard/.codex/sessions");
        std::fs::create_dir_all(&root).unwrap();
        let diagnostics = crate::provider::ProviderDiagnostics::default();
        let mut worker = AdapterProviderWorker::new(
            vec![DiscoveryRoot {
                provider: Provider::Codex,
                path: root.clone(),
            }],
            diagnostics.clone(),
        );
        let mut pending = PendingEvents::new(diagnostics);
        process_adapter_worker(&mut worker, &TargetSet::default(), &mut pending);
        let _ = drain_pending(&mut pending);
        let path = codex_artifact(&root, "oversized");
        let mut contents = vec![b'x'; MAX_TAIL_RECORD_BYTES + MAX_TAIL_CHUNK_BYTES + 1];
        contents.extend_from_slice(
            b"\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"sub_agent_activity\",\"event_id\":\"after-oversized\",\"occurred_at_ms\":3,\"agent_thread_id\":\"after-oversized-agent\",\"agent_path\":\"/root\",\"kind\":\"interacted\"}}\n",
        );
        std::fs::write(&path, contents).unwrap();
        let targets = TargetSet::new([ProviderTarget {
            provider: Provider::Codex,
            path,
        }]);
        let mut events = Vec::new();

        for _ in 0..8 {
            process_adapter_worker(&mut worker, &targets, &mut pending);
            events.extend(drain_pending(&mut pending));
        }

        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    matches!(
                        event,
                        ProviderEvent::Malformed {
                            error_code: "record_too_long",
                            ..
                        }
                    )
                })
                .count(),
            1
        );
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ProviderEvent::Activity { event_id, .. }
                    if event_id == "prov:codex:act:after-oversized"
            )
        }));
    }

    #[test]
    fn not_applicable_provider_does_not_degrade_live_quality() {
        let (quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
        let (coverage_sender, _coverage) = watch::channel(SourceCoverageRegistry::new(
            SourceAvailability::NotApplicable,
        ));
        let mut coverage = CoverageTracker::new(
            SourceAvailability::NotApplicable,
            coverage_sender,
            quality_sender,
        );

        coverage.set_herdr_quality(ObservationQuality::Live);
        coverage.update_provider_state(Provider::Claude, ProviderSourceState::NotApplicable);

        assert_eq!(*quality.borrow(), ObservationQuality::Live);
    }

    #[test]
    fn unavailable_controller_remains_coverage_only() {
        let registry = SourceCoverageRegistry::new(SourceAvailability::Unavailable {
            detail: "bind_failure".to_owned(),
        });

        assert_eq!(
            registry.effective_quality(ObservationQuality::Live),
            ObservationQuality::Live
        );
        assert_eq!(
            registry.state(CoverageSource::Controller),
            &SourceAvailability::Unavailable {
                detail: "bind_failure".to_owned(),
            }
        );
    }

    #[test]
    fn unavailable_provider_degrades_and_available_clears_live_quality() {
        let (quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
        let (coverage_sender, coverage) =
            watch::channel(SourceCoverageRegistry::new(SourceAvailability::Available));
        let mut tracker = CoverageTracker::new(
            SourceAvailability::Available,
            coverage_sender,
            quality_sender,
        );

        tracker.set_herdr_quality(ObservationQuality::Live);
        tracker.update_provider_state(
            Provider::Codex,
            ProviderSourceState::Unavailable {
                detail: "read_failed".to_owned(),
            },
        );
        assert_eq!(*quality.borrow(), ObservationQuality::Degraded);
        assert_eq!(
            coverage.borrow().state(CoverageSource::Codex),
            &SourceAvailability::Unavailable {
                detail: "read_failed".to_owned(),
            }
        );

        tracker.update_provider_state(Provider::Codex, ProviderSourceState::Available);
        assert_eq!(*quality.borrow(), ObservationQuality::Live);
    }

    #[test]
    fn session_resolution_normalizes_stable_node_link_and_meta_effects() {
        let run_id = RunId::new();
        let path = "/tmp/provider/root.jsonl";
        let mut model = DomainModel::default();
        model.insert_task_run(TaskRun {
            run_id,
            key: RunKey::NativePath {
                provider: Provider::Codex,
                path: path.to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: false,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
        let (_sender, shared) = watch::channel(Arc::new(model));
        let event = ProviderEvent::SessionResolved {
            provider: Provider::Codex,
            agent_thread_id: "child".to_owned(),
            owner_session_id: Some("owner".to_owned()),
            parent_thread_id: Some("parent".to_owned()),
            path: PathBuf::from(path),
            model_id: Some("gpt-test".to_owned()),
            depth: Some(1),
            event_id: "prov:codex:meta:child".to_owned(),
            observed_at_ms: 123,
            position: SourcePosition {
                path_id: 1,
                generation: 0,
                offset: 2,
            },
        };

        let mut coverage = SourceCoverageRegistry::new(SourceAvailability::Available);
        coverage.set(CoverageSource::Codex, SourceAvailability::Available);
        let normalized = normalize_provider_event(&shared, "session", event, &coverage);
        let ids = normalized
            .events
            .iter()
            .map(|event| super::normalized_metadata(event).event_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            ids,
            [
                "prov:codex:node:child",
                "prov:codex:link:child:parent",
                "prov:codex:meta:child",
            ]
        );
        assert!(!normalized.identity_disagreement);
        let metadata = super::normalized_metadata(normalized.events.last().unwrap());
        assert_eq!(metadata.task_run_id, Some(run_id));
        assert_eq!(metadata.native_session_id.as_deref(), Some("owner"));
        assert_eq!(
            metadata
                .source_coverage
                .iter()
                .map(|source| source.source.as_str())
                .collect::<Vec<_>>(),
            ["herdr", "controller", "codex"]
        );
    }

    struct DropObservedWorker(Arc<AtomicBool>);

    impl ProviderWorker for DropObservedWorker {
        fn process(&mut self, _cycle: &mut ProviderCycle<'_>) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Drop for DropObservedWorker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    struct PanickingProviderWorker;

    impl ProviderWorker for PanickingProviderWorker {
        fn process(&mut self, _cycle: &mut ProviderCycle<'_>) -> std::io::Result<()> {
            panic!("synthetic provider worker panic");
        }
    }

    #[test]
    fn collector_precedence_logs_masked_provider_shutdown_error() {
        let directory = tempfile::tempdir().unwrap();
        let log_path = directory.path().join("collector-stop.log");
        let log = std::fs::File::create(&log_path).unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(log)
            .finish();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(async {
                let (provider_sender, _provider_events) = mpsc::channel(1);
                let provider_thread = crate::provider::spawn_provider_thread(
                    PanickingProviderWorker,
                    provider_sender,
                    None,
                )
                .unwrap();
                let (_quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
                let (_performance_sender, performance) =
                    watch::channel(initial_performance_publication());
                let (_coverage_sender, source_coverage) =
                    watch::channel(SourceCoverageRegistry::default());
                let (_reducer, model, operator) = Reducer::new_with_operator(
                    RestoredState {
                        model: DomainModel::default(),
                        next_ordinal: 1,
                        next_ingest_seq: Some(1),
                        event_ledger: Vec::new(),
                    },
                    empty_operator_seed(),
                );
                let handle = CollectorHandle {
                    performance,
                    quality,
                    source_coverage,
                    diagnostics: test_diagnostics(),
                    operator,
                    model,
                    primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle::default(),
                    cancellation: CancellationToken::new(),
                    task: tokio::spawn(async {
                        Err(CollectorError::Task(
                            "synthetic collector failure".to_owned(),
                        ))
                    }),
                    performance_monitor: tokio::spawn(async {}),
                    controller_acceptor: None,
                    provider_thread: Some(provider_thread),
                    provider_events_drained: None,
                };

                handle.stop_with_timeout(Duration::from_secs(1)).await
            })
        });

        assert!(matches!(
            result,
            Err(CollectorError::Task(detail)) if detail == "synthetic collector failure"
        ));
        let contents = std::fs::read_to_string(log_path).unwrap();
        assert!(
            contents.contains("provider_thread_panicked"),
            "masked provider shutdown error was not logged: {contents}"
        );
    }

    #[tokio::test]
    async fn collector_timeout_still_stops_and_joins_the_provider_thread() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (provider_sender, _provider_events) = mpsc::channel(1);
        let provider_thread = crate::provider::spawn_provider_thread(
            DropObservedWorker(Arc::clone(&dropped)),
            provider_sender,
            None,
        )
        .unwrap();
        let (_quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
        let (_performance_sender, performance) = watch::channel(initial_performance_publication());
        let (_coverage_sender, source_coverage) = watch::channel(SourceCoverageRegistry::default());
        let (_reducer, model, operator) = Reducer::new_with_operator(
            RestoredState {
                model: DomainModel::default(),
                next_ordinal: 1,
                next_ingest_seq: Some(1),
                event_ledger: Vec::new(),
            },
            empty_operator_seed(),
        );
        let handle = CollectorHandle {
            performance,
            quality,
            source_coverage,
            diagnostics: test_diagnostics(),
            operator,
            model,
            primary_stream_diagnostics: PrimaryStreamDiagnosticsHandle::default(),
            cancellation: CancellationToken::new(),
            task: tokio::spawn(async {
                std::future::pending::<()>().await;
                Ok(())
            }),
            performance_monitor: tokio::spawn(async {}),
            controller_acceptor: None,
            provider_thread: Some(provider_thread),
            provider_events_drained: None,
        };

        assert!(matches!(
            handle.stop_with_timeout(Duration::from_millis(10)).await,
            Err(CollectorError::StopTimeout { .. })
        ));
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn provider_fallback_promotes_path_and_duplicate_payload_is_first_wins() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let path = directory.path().join("provider/root.jsonl");
        let run_id = RunId::new();
        let base_ms = unix_now_ms();
        let task_run = TaskRun {
            run_id,
            key: RunKey::NativePath {
                provider: Provider::Codex,
                path: path.to_string_lossy().into_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::EndedUnknown,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![PersistOp::UpsertTaskRun(PersistTaskRun {
                task_run,
                native_session: None,
                created_at_ms: base_ms + 1,
                updated_at_ms: base_ms + 1,
                finished_at_ms: None,
            })])
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut persistence = test_runtime(writer);
        let (mut reducer, shared) = Reducer::new(restored);

        apply_provider_event(
            ProviderEvent::SessionResolved {
                provider: Provider::Codex,
                agent_thread_id: "owner".to_owned(),
                owner_session_id: Some("owner".to_owned()),
                parent_thread_id: None,
                path: path.clone(),
                model_id: Some("gpt-first".to_owned()),
                depth: Some(0),
                event_id: "prov:codex:meta:owner".to_owned(),
                observed_at_ms: base_ms + 100,
                position: SourcePosition {
                    path_id: 1,
                    generation: 0,
                    offset: 0,
                },
            },
            "session",
            &mut reducer,
            &shared,
            &mut persistence,
            &SourceCoverageRegistry::new(SourceAvailability::Available),
        )
        .await
        .unwrap();
        let activity = |kind: &str| ProviderEvent::Activity {
            provider: Provider::Codex,
            agent_thread_id: "owner".to_owned(),
            activity: MinimalProviderMetadata {
                agent_id: Some("owner".to_owned()),
                event_kind: Some(kind.to_owned()),
                ..MinimalProviderMetadata::default()
            },
            depth: Some(0),
            event_id: "prov:codex:act:same".to_owned(),
            observed_at_ms: base_ms + 200,
            position: SourcePosition {
                path_id: 1,
                generation: 0,
                offset: 10,
            },
        };
        apply_provider_event(
            activity("first"),
            "session",
            &mut reducer,
            &shared,
            &mut persistence,
            &SourceCoverageRegistry::new(SourceAvailability::Available),
        )
        .await
        .unwrap();
        apply_provider_event(
            activity("different-second"),
            "session",
            &mut reducer,
            &shared,
            &mut persistence,
            &SourceCoverageRegistry::new(SourceAvailability::Available),
        )
        .await
        .unwrap();

        {
            let model = shared.borrow();
            assert_eq!(
                model.task_run(&run_id).unwrap().key,
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "owner".to_owned(),
                }
            );
            assert_eq!(
                model.task_run(&run_id).unwrap().state,
                TaskState::EndedUnknown
            );
            assert_eq!(
                model
                    .agent_node("agent:codex:owner")
                    .unwrap()
                    .last_event_kind
                    .as_deref(),
                Some("first")
            );
            let node = model.agent_node("agent:codex:owner").unwrap();
            assert_eq!(node.model_id.as_deref(), Some("gpt-first"));
            assert_eq!(node.session_file.as_deref(), path.to_str());
        }
        lifecycle.shutdown().await.unwrap();

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(
            restored.model.task_run(&run_id).unwrap().key,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "owner".to_owned(),
            }
        );
        assert_eq!(
            restored
                .model
                .agent_node("agent:codex:owner")
                .unwrap()
                .last_event_kind
                .as_deref(),
            Some("first")
        );
    }

    #[tokio::test]
    async fn herdr_id_disagreement_is_counted_and_never_corroborates() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let path = directory.path().join("provider/root.jsonl");
        let run_id = RunId::new();
        let task_run = TaskRun {
            run_id,
            key: RunKey::Native {
                provider: Provider::Codex,
                sid: "herdr-sid".to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        };
        let agent_node = AgentNode {
            agent_node_id: "gap-agent-herdr".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("herdr-sid".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: Some(path.to_string_lossy().into_owned()),
        };
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![
                PersistOp::UpsertTaskRun(PersistTaskRun {
                    task_run,
                    native_session: Some(NativeSessionBinding {
                        provider: Provider::Codex,
                        native_session_id: "herdr-sid".to_owned(),
                    }),
                    created_at_ms: 1,
                    updated_at_ms: 1,
                    finished_at_ms: None,
                }),
                PersistOp::UpsertAgentNode(agent_node),
            ])
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut persistence = test_runtime(writer);
        let (mut reducer, shared) = Reducer::new(restored);

        apply_provider_event(
            ProviderEvent::SessionResolved {
                provider: Provider::Codex,
                agent_thread_id: "adapter-sid".to_owned(),
                owner_session_id: Some("adapter-sid".to_owned()),
                parent_thread_id: None,
                path,
                model_id: None,
                depth: Some(0),
                event_id: "prov:codex:meta:adapter-sid".to_owned(),
                observed_at_ms: 100,
                position: SourcePosition {
                    path_id: 1,
                    generation: 0,
                    offset: 0,
                },
            },
            "session",
            &mut reducer,
            &shared,
            &mut persistence,
            &SourceCoverageRegistry::new(SourceAvailability::Available),
        )
        .await
        .unwrap();

        let model = shared.borrow();
        assert_eq!(
            model.task_run(&run_id).unwrap().key,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "herdr-sid".to_owned(),
            }
        );
        assert_eq!(
            model
                .controller_diagnostics()
                .provider_identity_disagreements(),
            1
        );
        drop(model);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn owned_session_merge_conflict_is_diagnostic_and_non_fatal() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let path = directory.path().join("provider/root.jsonl");
        let owner = RunId::new();
        let path_run = RunId::new();
        let first_parent = RunId::new();
        let second_parent = RunId::new();
        let mut model = DomainModel::default();
        for task_run in [
            TaskRun {
                run_id: owner,
                key: RunKey::Native {
                    provider: Provider::Codex,
                    sid: "owner".to_owned(),
                },
                display_ordinal: DisplayOrdinal::new(1),
                state: TaskState::Running,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            },
            TaskRun {
                run_id: path_run,
                key: RunKey::NativePath {
                    provider: Provider::Codex,
                    path: path.to_string_lossy().into_owned(),
                },
                display_ordinal: DisplayOrdinal::new(2),
                state: TaskState::Running,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            },
            TaskRun {
                run_id: first_parent,
                key: RunKey::Controller("first-parent".to_owned()),
                display_ordinal: DisplayOrdinal::new(3),
                state: TaskState::Running,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            },
            TaskRun {
                run_id: second_parent,
                key: RunKey::Controller("second-parent".to_owned()),
                display_ordinal: DisplayOrdinal::new(4),
                state: TaskState::Running,
                has_controller_task_state_event: true,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            },
        ] {
            model.insert_task_run(task_run);
        }
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: first_parent,
            child_run_id: owner,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: second_parent,
            child_run_id: path_run,
        });
        let restored = RestoredState {
            model,
            next_ordinal: 5,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        };
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut persistence = test_runtime(writer);
        let (mut reducer, shared) = Reducer::new(restored);

        apply_provider_event(
            ProviderEvent::SessionResolved {
                provider: Provider::Codex,
                agent_thread_id: "owner".to_owned(),
                owner_session_id: Some("owner".to_owned()),
                parent_thread_id: None,
                path,
                model_id: None,
                depth: Some(0),
                event_id: "prov:codex:meta:owner".to_owned(),
                observed_at_ms: 100,
                position: SourcePosition {
                    path_id: 1,
                    generation: 0,
                    offset: 0,
                },
            },
            "session",
            &mut reducer,
            &shared,
            &mut persistence,
            &SourceCoverageRegistry::new(SourceAvailability::Available),
        )
        .await
        .unwrap();

        let model = shared.borrow();
        assert!(model.task_run(&owner).is_some());
        assert!(model.task_run(&path_run).is_some());
        assert_eq!(model.agent_nodes().count(), 0);
        assert_eq!(model.controller_diagnostics().binding_conflicts(), 1);
        drop(model);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn restored_targets_feed_provider_events_during_herdr_reconnect_wait() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let sessions = directory.path().join("home/.codex/sessions/2026/08/09");
        std::fs::create_dir_all(&sessions).unwrap();
        let session_file = codex_artifact(&sessions, "owner");
        std::fs::write(
            &session_file,
            br#"{"type":"session_meta","payload":{"id":"owner","session_id":"owner","model":"gpt-test"}}
"#,
        )
        .unwrap();
        let run_id = RunId::new();
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![PersistOp::UpsertTaskRun(PersistTaskRun {
                task_run: TaskRun {
                    run_id,
                    key: RunKey::NativePath {
                        provider: Provider::Codex,
                        path: session_file.to_string_lossy().into_owned(),
                    },
                    display_ordinal: DisplayOrdinal::new(1),
                    state: TaskState::EndedUnknown,
                    has_controller_task_state_event: true,
                    created_at_ms: None,
                    updated_at_ms: None,
                    finished_at_ms: None,
                    subject: None,
                    dismissed_at_ms: None,
                },
                native_session: None,
                created_at_ms: 1,
                updated_at_ms: 1,
                finished_at_ms: Some(1),
            })])
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut collector = spawn(
            directory.path().join("missing-herdr.sock"),
            "provider-reconnect".to_owned(),
            restored,
            writer,
        )
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if collector
                    .model
                    .borrow()
                    .agent_node("agent:codex:owner")
                    .is_some()
                {
                    break;
                }
                collector.model.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        {
            let model = collector.model.borrow();
            assert_eq!(
                model.task_run(&run_id).unwrap().key,
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "owner".to_owned(),
                }
            );
            assert_eq!(
                model.task_run(&run_id).unwrap().state,
                TaskState::EndedUnknown
            );
            assert_eq!(model.agent_nodes().count(), 1);
            assert_eq!(model.panes().count(), 0);
            assert_eq!(model.executions().count(), 0);
        }

        collector.stop().await.unwrap();
        lifecycle.shutdown().await.unwrap();
    }
}
