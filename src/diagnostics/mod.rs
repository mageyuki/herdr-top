//! Closed, privacy-safe diagnostic contracts shared by the runtime and doctor.

use std::fs::File;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tracing_subscriber::fmt::MakeWriter;

use crate::model::DomainModel;
use crate::store::writer::{PersistenceFailure, PersistenceStatus};

pub mod local;
pub mod remote;

const PERSISTENCE_OCCURRENCE_PREFIX: &[u8] = b"HERDR_TOP_PERSISTENCE_V1 ";

/// Whether the Controller input endpoint can admit event envelopes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControllerInputUnavailableReason {
    ListenerUnavailable,
    RuntimeUnsafe,
    PersistenceUnavailable,
    AcceptorStopped,
}

/// Current Controller input availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ControllerInputStatus {
    Available,
    Unavailable {
        reason: ControllerInputUnavailableReason,
    },
}

/// Freshness of the durable process-owner location.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerFreshness {
    Current,
    Stale,
}

/// One of the four fixed diagnostic sources.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSource {
    Herdr,
    Controller,
    Claude,
    Codex,
}

/// Closed input-coverage state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InputAvailability {
    Available,
    Partial,
    Unavailable,
}

/// Coverage of one fixed source.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct SourceCoverageSnapshot {
    pub source: DiagnosticSource,
    pub availability: InputAvailability,
}

/// Process-lifetime persistence outcome counters. Values count batches, not rows.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct PersistenceCounters {
    pub not_committed_batches: u64,
    pub durability_unknown_batches: u64,
    pub committed_but_degraded_batches: u64,
    pub skipped_batches: u64,
    pub skipped_owner_updates: u64,
}

/// Immutable copy of all Controller diagnostic counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct ControllerCounterSnapshot {
    pub binding_conflicts: u64,
    pub terminal_blocked_progress_noops: u64,
    pub terminal_forward_reference_creations: u64,
    pub dangling_announcement_components: u64,
    pub ingest_sequence_exhaustions: u64,
    pub provider_parent_conflicts: u64,
    pub provider_identity_disagreements: u64,
    pub socket_saturations: u64,
    pub accept_failures: u64,
}

/// Immutable copy of enrichment-stream loss counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct EnrichmentCounterSnapshot {
    pub channel_full_drops: u64,
    pub episode_discards: u64,
}

/// Immutable copy of primary-stream tolerance counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct PrimaryStreamCounterSnapshot {
    pub flat_pane_agent_detected: u64,
}

/// Reader-shareable counters for tolerated primary-stream wire shapes.
#[derive(Clone, Debug, Default)]
pub struct PrimaryStreamDiagnosticsHandle {
    flat_pane_agent_detected: Arc<AtomicU64>,
}

impl PrimaryStreamDiagnosticsHandle {
    /// Records one flat `pane_agent_detected` frame.
    pub fn record_flat_pane_agent_detected(&self) {
        let _ = self.flat_pane_agent_detected.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| Some(value.saturating_add(1)),
        );
    }

    /// Returns an immutable copy of the primary-stream counters.
    #[must_use]
    pub fn snapshot(&self) -> PrimaryStreamCounterSnapshot {
        PrimaryStreamCounterSnapshot {
            flat_pane_agent_detected: self.flat_pane_agent_detected.load(Ordering::Relaxed),
        }
    }
}

/// Result of the process's single persistence-occurrence append attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccurrenceLogStatus {
    NotAttempted,
    Emitted,
    Failed,
}

/// Complete immutable runtime diagnostics published through a watch receiver.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct RuntimeDiagnosticsSnapshot {
    pub persistence: PersistenceStatus,
    pub controller_input: ControllerInputStatus,
    pub owner: OwnerFreshness,
    pub persistence_counters: PersistenceCounters,
    pub controller_counters: ControllerCounterSnapshot,
    pub enrichment_counters: EnrichmentCounterSnapshot,
    pub source_coverage: Vec<SourceCoverageSnapshot>,
    pub dangling_announcement_components: u64,
    pub first_failure_log: OccurrenceLogStatus,
}

/// Truth known to the collector after one runtime persistence request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeWriteOutcome {
    Durable,
    CommittedButDegraded(PersistenceFailure),
    NotCommitted(PersistenceFailure),
    DurabilityUnknown(PersistenceFailure),
    Skipped,
}

/// Fallible process-owned destination for the single persistence occurrence.
pub trait PersistenceOccurrenceSink: Send + Sync {
    /// Appends one complete versioned record and flushes the shared handle.
    fn append(&self, record: &[u8]) -> io::Result<()>;
}

/// Occurrence sink sharing the same synchronized file handle as tracing.
#[derive(Clone)]
pub struct SharedFileOccurrenceSink {
    file: Arc<Mutex<File>>,
}

impl SharedFileOccurrenceSink {
    #[must_use]
    pub fn new(file: Arc<Mutex<File>>) -> Self {
        Self { file }
    }
}

impl PersistenceOccurrenceSink for SharedFileOccurrenceSink {
    fn append(&self, record: &[u8]) -> io::Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("shared persistence log lock is poisoned"))?;
        file.write_all(record)?;
        file.flush()
    }
}

/// Locked tracing writer borrowing the same process-owned file seam.
pub struct SharedFileGuard<'a>(MutexGuard<'a, File>);

impl Write for SharedFileGuard<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<'a> MakeWriter<'a> for SharedFileOccurrenceSink {
    type Writer = SharedFileGuard<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        SharedFileGuard(match self.file.lock() {
            Ok(file) => file,
            Err(poisoned) => poisoned.into_inner(),
        })
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum OccurrenceKind {
    PersistenceDegraded,
}

#[derive(serde::Serialize)]
struct PersistenceOccurrenceV1 {
    schema_version: u64,
    kind: OccurrenceKind,
    timestamp_ms: i64,
    failure: PersistenceFailure,
    counters: PersistenceCounters,
}

pub(crate) fn encode_persistence_occurrence(
    timestamp_ms: i64,
    failure: PersistenceFailure,
    counters: PersistenceCounters,
) -> Vec<u8> {
    let occurrence = PersistenceOccurrenceV1 {
        schema_version: 1,
        kind: OccurrenceKind::PersistenceDegraded,
        timestamp_ms,
        failure,
        counters,
    };
    let mut record = Vec::with_capacity(256);
    record.extend_from_slice(PERSISTENCE_OCCURRENCE_PREFIX);
    serde_json::to_writer(&mut record, &occurrence)
        .expect("serializing a closed persistence occurrence to Vec cannot fail");
    record.push(b'\n');
    record
}

pub(crate) fn controller_counter_snapshot(model: &DomainModel) -> ControllerCounterSnapshot {
    let diagnostics = model.controller_diagnostics();
    ControllerCounterSnapshot {
        binding_conflicts: diagnostics.binding_conflicts(),
        terminal_blocked_progress_noops: diagnostics.terminal_blocked_progress_noops(),
        terminal_forward_reference_creations: diagnostics.terminal_forward_reference_creations(),
        dangling_announcement_components: diagnostics.dangling_announcement_components(),
        ingest_sequence_exhaustions: diagnostics.ingest_sequence_exhaustions(),
        provider_parent_conflicts: diagnostics.provider_parent_conflicts(),
        provider_identity_disagreements: diagnostics.provider_identity_disagreements(),
        socket_saturations: diagnostics.socket_saturations(),
        accept_failures: diagnostics.accept_failures(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::store::writer::{
        DurabilityDisposition, PersistenceFailureCode, PersistenceOperation, PersistencePhase,
    };

    #[test]
    fn i4_runtime_diagnostics_closed_spellings_and_d4_duplicate_equality() {
        let failure = PersistenceFailure {
            operation: PersistenceOperation::Cleanup,
            phase: PersistencePhase::PostApplyCommit,
            code: PersistenceFailureCode::Sqlite,
            durability: DurabilityDisposition::Committed,
        };
        let snapshot = RuntimeDiagnosticsSnapshot {
            persistence: PersistenceStatus::Degraded { failure },
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
                dangling_announcement_components: 7,
                ..ControllerCounterSnapshot::default()
            },
            enrichment_counters: EnrichmentCounterSnapshot::default(),
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
            ],
            dangling_announcement_components: 7,
            first_failure_log: OccurrenceLogStatus::Emitted,
        };

        assert_eq!(
            serde_json::to_value(&snapshot).unwrap(),
            json!({
                "persistence": {
                    "status": "degraded",
                    "failure": {
                        "operation": "cleanup",
                        "phase": "post_apply_commit",
                        "code": "sqlite",
                        "durability": "committed"
                    }
                },
                "controller_input": {
                    "status": "unavailable",
                    "reason": "persistence_unavailable"
                },
                "owner": "stale",
                "persistence_counters": {
                    "not_committed_batches": 1,
                    "durability_unknown_batches": 2,
                    "committed_but_degraded_batches": 3,
                    "skipped_batches": 4,
                    "skipped_owner_updates": 5
                },
                "controller_counters": {
                    "binding_conflicts": 0,
                    "terminal_blocked_progress_noops": 0,
                    "terminal_forward_reference_creations": 0,
                    "dangling_announcement_components": 7,
                    "ingest_sequence_exhaustions": 0,
                    "provider_parent_conflicts": 0,
                    "provider_identity_disagreements": 0,
                    "socket_saturations": 0,
                    "accept_failures": 0
                },
                "enrichment_counters": {
                    "channel_full_drops": 0,
                    "episode_discards": 0
                },
                "source_coverage": [
                    {"source": "herdr", "availability": "available"},
                    {"source": "controller", "availability": "partial"},
                    {"source": "claude", "availability": "unavailable"}
                ],
                "dangling_announcement_components": 7,
                "first_failure_log": "emitted"
            })
        );
        assert_eq!(
            snapshot
                .controller_counters
                .dangling_announcement_components,
            snapshot.dangling_announcement_components
        );
        assert_eq!(
            encode_persistence_occurrence(123, failure, snapshot.persistence_counters),
            br#"HERDR_TOP_PERSISTENCE_V1 {"schema_version":1,"kind":"persistence_degraded","timestamp_ms":123,"failure":{"operation":"cleanup","phase":"post_apply_commit","code":"sqlite","durability":"committed"},"counters":{"not_committed_batches":1,"durability_unknown_batches":2,"committed_but_degraded_batches":3,"skipped_batches":4,"skipped_owner_updates":5}}
"#,
        );
    }

    #[test]
    fn i4_d3_poisoned_shared_log_recovers_tracing_writer_and_fails_occurrence() {
        let directory = tempfile::tempdir().unwrap();
        let file = Arc::new(Mutex::new(
            File::create(directory.path().join("shared.log")).unwrap(),
        ));
        let sink = SharedFileOccurrenceSink::new(Arc::clone(&file));
        let (poisoned, poisoned_rx) = mpsc::channel();
        let poison_file = Arc::clone(&file);
        let poisoner = std::thread::spawn(move || {
            let _guard = poison_file.lock().unwrap();
            poisoned.send(()).unwrap();
            panic!("poison shared tracing mutex");
        });
        poisoned_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("poisoner did not acquire the mutex");
        assert!(poisoner.join().is_err());

        assert!(sink.append(b"occurrence must fail\n").is_err());

        let (written, written_rx) = mpsc::channel();
        let tracing_sink = sink.clone();
        let tracing_writer = std::thread::spawn(move || {
            let result = tracing_sink.make_writer().write_all(b"tracing recovers\n");
            let _ = written.send(result);
        });
        let write_result = written_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("tracing writer did not report a bounded result");
        tracing_writer.join().unwrap();
        write_result.unwrap();
    }
}
