//! Session-scoped Controller event transport, admission, and emit client.

use std::future::Future;
use std::io;
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::diagnostics::{
    ControllerInputStatus, ControllerInputUnavailableReason, DiagnosticSource, InputAvailability,
    RuntimeDiagnosticsSnapshot, RuntimeWriteOutcome,
};
use crate::herdr::collector::RuntimePersistence;
use crate::model::{
    ControllerDiagnosticsHandle, ControllerEvent, ControllerEventKind, EventMetadata,
    MinimalProviderMetadata, Provider, SourceCoverage, quantize_progress,
};
#[cfg(feature = "workload-harness")]
use crate::performance::AdmissionStamp;
use crate::performance::{
    AdmissionObserver, Admitted, AdmittingSender, PerformanceIngress, admitted_channel_observed,
};
use crate::reducer::{CommitStagedError, Reducer, RejectReason};
// increment5-workload-harness: begin controller timing imports
#[cfg(feature = "workload-harness")]
use crate::reducer::{WorkloadTimingKind, WorkloadTimingObserver, workload_timing_scope};
// increment5-workload-harness: end controller timing imports

/// Maximum JSON payload size, excluding the newline delimiter.
pub const MAX_FRAME_BYTES: usize = 64 * 1024;
/// Bounds concurrent pre-frame connection tasks; the kernel listen backlog queues beyond it —
/// implements design §9.2 "connection queueing bounded by the acceptor."
pub const MAX_CONTROLLER_CONNECTIONS: usize = 64;
/// Per-operation transport timeout for Controller sockets.
pub const CONTROLLER_IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Production capacity of the bounded acceptor-to-reducer request queue.
pub const CONTROLLER_REQUEST_QUEUE_CAPACITY: usize = 64;

const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(250);
// increment5-workload-harness: begin controller workload observation ABI
/// Successful reservation of one synthetic workload request in the real bounded queue.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadAdmissionObservation {
    pub sequence: u64,
    pub scheduled_ns: u64,
    pub admitted_ns: u64,
}

/// Terminal reducer and model-publication timestamps for one admitted workload request.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadTerminalObservation {
    pub sequence: u64,
    pub terminal_ns: u64,
    pub published_ns: u64,
}

/// Durable acknowledgement for one admitted workload request.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadPersistenceObservation {
    pub sequence: u64,
    pub persisted_ns: u64,
}

/// Feature-only clocks and callbacks attached to the real bounded Controller channel.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
#[derive(Clone)]
pub struct WorkloadControllerHooks {
    pub clock: std::sync::Arc<dyn Fn() -> u64 + Send + Sync>,
    pub admission_observer: std::sync::Arc<dyn Fn(WorkloadAdmissionObservation) + Send + Sync>,
    pub terminal_observer: std::sync::Arc<dyn Fn(WorkloadTerminalObservation) + Send + Sync>,
    pub persistence_observer: std::sync::Arc<dyn Fn(WorkloadPersistenceObservation) + Send + Sync>,
    pub timing_observer: WorkloadTimingObserver,
}

#[cfg(feature = "workload-harness")]
#[derive(Clone)]
struct WorkloadRequestContext {
    sequence: u64,
    scheduled_ns: u64,
    hooks: WorkloadControllerHooks,
}
// increment5-workload-harness: end controller workload observation ABI

/// One Controller request as emitted by the standalone client.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ControllerEnvelope {
    pub schema_version: u64,
    pub event_id: String,
    pub emitted_at_ms: i64,
    pub source: String,
    pub event_type: String,
    pub task_run_id: String,
    pub parent_task_run_id: Option<String>,
    pub depends_on_id: Option<String>,
    pub label: Option<String>,
    pub reason: Option<String>,
    pub progress: Option<f64>,
    pub provider: Option<String>,
    pub native_session_id: Option<String>,
    pub terminal_id: Option<String>,
}

/// One closed-shape Controller response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ControllerResponse {
    Accepted,
    Duplicate,
    Rejected { reason: RejectResponseReason },
    Retryable { reason: RetryableReason },
}

impl<'de> Deserialize<'de> for ControllerResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("Controller response must be an object"))?;
        let status = object
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| serde::de::Error::custom("Controller response status is required"))?;
        match (status, object.len()) {
            ("accepted", 1) => Ok(Self::Accepted),
            ("duplicate", 1) => Ok(Self::Duplicate),
            ("rejected", 2) => {
                let reason = object
                    .get("reason")
                    .and_then(Value::as_str)
                    .and_then(reject_reason_from_wire)
                    .ok_or_else(|| {
                        serde::de::Error::custom("invalid Controller rejection reason")
                    })?;
                Ok(Self::Rejected { reason })
            }
            ("retryable", 2) => {
                let reason = object
                    .get("reason")
                    .and_then(Value::as_str)
                    .and_then(retryable_reason_from_wire)
                    .ok_or_else(|| {
                        serde::de::Error::custom("invalid Controller retryable reason")
                    })?;
                Ok(Self::Retryable { reason })
            }
            _ => Err(serde::de::Error::custom(
                "unknown or non-closed Controller response",
            )),
        }
    }
}

fn reject_reason_from_wire(value: &str) -> Option<RejectResponseReason> {
    match value {
        "invalid" => Some(RejectResponseReason::Invalid),
        "cycle" => Some(RejectResponseReason::Cycle),
        "conflict" => Some(RejectResponseReason::Conflict),
        "stale_event" => Some(RejectResponseReason::StaleEvent),
        "unsupported_version" => Some(RejectResponseReason::UnsupportedVersion),
        _ => None,
    }
}

fn retryable_reason_from_wire(value: &str) -> Option<RetryableReason> {
    match value {
        "busy" => Some(RetryableReason::Busy),
        "persistence_unavailable" => Some(RetryableReason::PersistenceUnavailable),
        _ => None,
    }
}

/// Stable rejection reasons exposed on the Controller wire.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectResponseReason {
    Invalid,
    Cycle,
    Conflict,
    StaleEvent,
    UnsupportedVersion,
}

/// Stable retryable reasons exposed on the Controller wire.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryableReason {
    Busy,
    PersistenceUnavailable,
}

/// Exact closed request for the additive read-only status surface.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerStatusRequest {
    pub request: StatusRequestKind,
    pub schema_version: u64,
}

/// Supported status request kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusRequestKind {
    Status,
}

/// Distinct closed response for Controller runtime status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ControllerStatusResponse {
    Ok {
        schema_version: u64,
        diagnostics: RuntimeDiagnosticsSnapshot,
    },
    Error {
        schema_version: u64,
        reason: ControllerStatusErrorReason,
    },
}

/// Stable status-request error taxonomy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControllerStatusErrorReason {
    InvalidRequest,
    UnsupportedVersion,
}

/// Outcome observed by the best-effort emit client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EmitOutcome {
    Response(ControllerResponse),
    Unresolved(String),
}

impl EmitOutcome {
    /// Returns whether strict mode considers the delivery successful.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(
            self,
            Self::Response(ControllerResponse::Accepted | ControllerResponse::Duplicate)
        )
    }
}

/// Fatal acceptor setup or listener failure.
#[derive(Debug, Error)]
pub enum ControllerServerError {
    #[error("Controller socket I/O failed: {0}")]
    Io(#[from] io::Error),
}

/// Cloneable producer for the bounded serialized-reducer request queue.
#[derive(Clone)]
pub struct ControllerRequestSender {
    sender: AdmittingSender<ControllerRequest>,
    diagnostics: ControllerDiagnosticsHandle,
    diagnostic_changes: watch::Sender<u64>,
    // increment5-workload-harness: begin controller sender workload hooks
    #[cfg(feature = "workload-harness")]
    workload_hooks: Option<WorkloadControllerHooks>,
    // increment5-workload-harness: end controller sender workload hooks
}

/// Unique consumer for the serialized-reducer request queue.
pub struct ControllerRequestReceiver {
    receiver: mpsc::Receiver<Admitted<ControllerRequest>>,
    diagnostic_changes: watch::Receiver<u64>,
}

pub(crate) enum ControllerRuntimeEvent {
    Request(Option<Admitted<ControllerRequest>>),
    DiagnosticsChanged,
}

pub(crate) struct ControllerRequest {
    frame: Vec<u8>,
    receipt_time_ms: i64,
    responder: oneshot::Sender<ControllerResponse>,
    // increment5-workload-harness: begin controller request workload context
    #[cfg(feature = "workload-harness")]
    workload: Option<WorkloadRequestContext>,
    // increment5-workload-harness: end controller request workload context
}

/// Builds a bounded Controller request channel.
#[must_use]
pub fn request_channel(
    capacity: usize,
    diagnostics: ControllerDiagnosticsHandle,
    performance: PerformanceIngress,
) -> (ControllerRequestSender, ControllerRequestReceiver) {
    request_channel_inner(capacity, diagnostics, performance, None)
}

fn request_channel_inner(
    capacity: usize,
    diagnostics: ControllerDiagnosticsHandle,
    performance: PerformanceIngress,
    observer: Option<AdmissionObserver<ControllerRequest>>,
) -> (ControllerRequestSender, ControllerRequestReceiver) {
    let (sender, receiver) = admitted_channel_observed(capacity, performance, observer);
    let (diagnostic_changes, diagnostic_change_receiver) = watch::channel(0);
    (
        ControllerRequestSender {
            sender,
            diagnostics,
            diagnostic_changes,
            // increment5-workload-harness: begin controller sender default hooks
            #[cfg(feature = "workload-harness")]
            workload_hooks: None,
            // increment5-workload-harness: end controller sender default hooks
        },
        ControllerRequestReceiver {
            receiver,
            diagnostic_changes: diagnostic_change_receiver,
        },
    )
}
// increment5-workload-harness: begin observed controller channel adapter
/// Builds the production bounded request channel with feature-only workload callbacks.
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
#[must_use]
pub fn request_channel_with_workload_harness(
    capacity: usize,
    diagnostics: ControllerDiagnosticsHandle,
    performance: PerformanceIngress,
    hooks: WorkloadControllerHooks,
) -> (ControllerRequestSender, ControllerRequestReceiver) {
    let observer: AdmissionObserver<ControllerRequest> =
        std::sync::Arc::new(|stamp: AdmissionStamp, request: &ControllerRequest| {
            let workload = request
                .workload
                .as_ref()
                .expect("observed workload request must carry its context");
            (workload.hooks.admission_observer)(WorkloadAdmissionObservation {
                sequence: workload.sequence,
                scheduled_ns: workload.scheduled_ns,
                admitted_ns: u64::try_from(stamp.admitted_at.as_nanos())
                    .expect("performance admission timestamp must fit u64 nanoseconds"),
            });
        });
    let (mut sender, receiver) =
        request_channel_inner(capacity, diagnostics, performance, Some(observer));
    sender.workload_hooks = Some(hooks);
    (sender, receiver)
}
// increment5-workload-harness: end observed controller channel adapter

impl ControllerRequestSender {
    // increment5-workload-harness: begin bounded workload admission adapter
    /// Reserves real bounded capacity, observes admission, then consumes that exact permit.
    #[cfg(feature = "workload-harness")]
    #[doc(hidden)]
    pub async fn submit_workload_frame(
        &self,
        frame: Vec<u8>,
        receipt_time_ms: i64,
        sequence: u64,
        scheduled_ns: u64,
    ) -> ControllerResponse {
        let Some(hooks) = self.workload_hooks.clone() else {
            return ControllerResponse::Retryable {
                reason: RetryableReason::PersistenceUnavailable,
            };
        };
        let (responder, response) = oneshot::channel();
        let request = ControllerRequest {
            frame,
            receipt_time_ms,
            responder,
            workload: Some(WorkloadRequestContext {
                sequence,
                scheduled_ns,
                hooks,
            }),
        };
        match self.sender.try_send(request) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.record_socket_saturation();
                return ControllerResponse::Retryable {
                    reason: RetryableReason::Busy,
                };
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return ControllerResponse::Retryable {
                    reason: RetryableReason::PersistenceUnavailable,
                };
            }
        }
        match tokio::time::timeout(CONTROLLER_IO_TIMEOUT, response).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) | Err(_) => ControllerResponse::Retryable {
                reason: RetryableReason::PersistenceUnavailable,
            },
        }
    }

    // increment5-workload-harness: end bounded workload admission adapter
    fn record_accept_failure(&self) {
        self.diagnostics.record_accept_failure();
        self.notify_diagnostic_change();
    }

    fn record_socket_saturation(&self) {
        self.diagnostics.record_socket_saturation();
        self.notify_diagnostic_change();
    }

    fn notify_diagnostic_change(&self) {
        self.diagnostic_changes.send_modify(|version| {
            *version = version.wrapping_add(1);
        });
    }
}

impl ControllerRequestReceiver {
    /// Returns the number of admitted requests awaiting the serialized reducer.
    #[must_use]
    pub fn queued_requests(&self) -> usize {
        self.receiver.len()
    }

    #[cfg(test)]
    async fn recv(&mut self) -> Option<Admitted<ControllerRequest>> {
        self.receiver.recv().await
    }

    pub(crate) async fn recv_runtime_event(&mut self) -> ControllerRuntimeEvent {
        tokio::select! {
            request = self.receiver.recv() => ControllerRuntimeEvent::Request(request),
            changed = self.diagnostic_changes.changed() => {
                match changed {
                    Ok(()) => ControllerRuntimeEvent::DiagnosticsChanged,
                    Err(_) => ControllerRuntimeEvent::Request(self.receiver.recv().await),
                }
            }
        }
    }
}

/// Starts the one-request-per-connection acceptor.
pub fn spawn_acceptor(
    listener: StdUnixListener,
    sender: ControllerRequestSender,
    cancellation: CancellationToken,
) -> Result<JoinHandle<Result<(), ControllerServerError>>, ControllerServerError> {
    spawn_acceptor_configured(listener, sender, cancellation, None)
}

/// Starts an acceptor that can answer status directly from a read-only watch.
pub fn spawn_acceptor_with_diagnostics(
    listener: StdUnixListener,
    sender: ControllerRequestSender,
    cancellation: CancellationToken,
    diagnostics: watch::Receiver<RuntimeDiagnosticsSnapshot>,
) -> Result<JoinHandle<Result<(), ControllerServerError>>, ControllerServerError> {
    spawn_acceptor_configured(listener, sender, cancellation, Some(diagnostics))
}

fn spawn_acceptor_configured(
    listener: StdUnixListener,
    sender: ControllerRequestSender,
    cancellation: CancellationToken,
    diagnostics: Option<watch::Receiver<RuntimeDiagnosticsSnapshot>>,
) -> Result<JoinHandle<Result<(), ControllerServerError>>, ControllerServerError> {
    listener.set_nonblocking(true)?;
    let mut listener = UnixListener::from_std(listener)?;
    Ok(tokio::spawn(async move {
        run_acceptor(&mut listener, sender, cancellation, diagnostics).await
    }))
}

trait AcceptSource {
    fn accept(&mut self) -> impl Future<Output = io::Result<UnixStream>> + Send;
}

impl AcceptSource for UnixListener {
    async fn accept(&mut self) -> io::Result<UnixStream> {
        UnixListener::accept(self)
            .await
            .map(|(stream, _address)| stream)
    }
}

async fn run_acceptor(
    source: &mut impl AcceptSource,
    sender: ControllerRequestSender,
    cancellation: CancellationToken,
    diagnostics: Option<watch::Receiver<RuntimeDiagnosticsSnapshot>>,
) -> Result<(), ControllerServerError> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            () = cancellation.cancelled() => break,
            accepted = source.accept(), if connections.len() < MAX_CONTROLLER_CONNECTIONS => {
                match accepted {
                    Ok(stream) => {
                        let request_sender = sender.clone();
                        let runtime_diagnostics = diagnostics.clone();
                        connections.spawn(async move {
                            if let Err(error) = handle_connection(
                                stream,
                                request_sender,
                                runtime_diagnostics,
                            ).await {
                                tracing::warn!(
                                    warning_code = "controller_connection_io",
                                    io_kind = ?error.kind(),
                                    "Controller connection ended without a response"
                                );
                            }
                        });
                    }
                    Err(error) => {
                        sender.record_accept_failure();
                        tracing::warn!(
                            warning_code = "controller_accept_failed",
                            io_kind = ?error.kind(),
                            "Controller accept failed; retrying"
                        );
                        tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                    }
                }
            }
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result {
                    tracing::warn!(
                        warning_code = "controller_connection_task_failed",
                        cancelled = error.is_cancelled(),
                        panicked = error.is_panic(),
                        "Controller connection task failed"
                    );
                }
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn handle_connection(
    mut stream: UnixStream,
    sender: ControllerRequestSender,
    diagnostics: Option<watch::Receiver<RuntimeDiagnosticsSnapshot>>,
) -> io::Result<()> {
    let mut should_drain = false;
    let response = match tokio::time::timeout(CONTROLLER_IO_TIMEOUT, read_frame(&mut stream)).await
    {
        Ok(Ok(frame)) => {
            if let Some(response) =
                status_response(&frame, diagnostics.as_ref(), &sender.diagnostics)
            {
                write_response(&mut stream, &response).await?;
                tokio::time::timeout(CONTROLLER_IO_TIMEOUT, stream.shutdown())
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "Controller shutdown timed out")
                    })??;
                return Ok(());
            }
            let (responder, response) = oneshot::channel();
            let request = ControllerRequest {
                frame,
                receipt_time_ms: unix_now_ms(),
                responder,
                // increment5-workload-harness: begin socket request default workload context
                #[cfg(feature = "workload-harness")]
                workload: None,
                // increment5-workload-harness: end socket request default workload context
            };
            match sender.sender.try_send(request) {
                Ok(()) => match tokio::time::timeout(CONTROLLER_IO_TIMEOUT, response).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(_)) => ControllerResponse::Retryable {
                        reason: RetryableReason::PersistenceUnavailable,
                    },
                    Err(_) => return Ok(()),
                },
                Err(mpsc::error::TrySendError::Full(_)) => {
                    sender.record_socket_saturation();
                    ControllerResponse::Retryable {
                        reason: RetryableReason::Busy,
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => ControllerResponse::Retryable {
                    reason: RetryableReason::PersistenceUnavailable,
                },
            }
        }
        Ok(Err(_)) => {
            should_drain = true;
            ControllerResponse::Rejected {
                reason: RejectResponseReason::Invalid,
            }
        }
        Err(_) => {
            should_drain = true;
            ControllerResponse::Rejected {
                reason: RejectResponseReason::Invalid,
            }
        }
    };
    write_response(&mut stream, &response).await?;
    if should_drain {
        tokio::time::timeout(CONTROLLER_IO_TIMEOUT, drain_inbound(&mut stream))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Controller drain timed out"))??;
    }
    tokio::time::timeout(CONTROLLER_IO_TIMEOUT, stream.shutdown())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Controller shutdown timed out"))??;
    Ok(())
}

async fn write_response(
    mut stream: impl AsyncWriteExt + Unpin,
    response: &impl Serialize,
) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(response).map_err(invalid_data)?;
    bytes.push(b'\n');
    tokio::time::timeout(CONTROLLER_IO_TIMEOUT, stream.write_all(&bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Controller response timed out"))??;
    Ok(())
}

fn status_response(
    frame: &[u8],
    diagnostics: Option<&watch::Receiver<RuntimeDiagnosticsSnapshot>>,
    acceptor_diagnostics: &ControllerDiagnosticsHandle,
) -> Option<ControllerStatusResponse> {
    let diagnostics = diagnostics?;
    let Value::Object(object) = serde_json::from_slice::<Value>(frame).ok()? else {
        return None;
    };
    if object.contains_key("event_id") || !object.contains_key("request") {
        return None;
    }
    let request = serde_json::from_value::<ControllerStatusRequest>(Value::Object(object));
    Some(match request {
        Ok(request) if request.schema_version == 1 => {
            let diagnostics_closed = diagnostics.has_changed().is_err();
            let mut snapshot = diagnostics.borrow().clone();
            if diagnostics_closed {
                snapshot.controller_input = ControllerInputStatus::Unavailable {
                    reason: ControllerInputUnavailableReason::RuntimeUnsafe,
                };
                if let Some(controller) = snapshot
                    .source_coverage
                    .iter_mut()
                    .find(|source| source.source == DiagnosticSource::Controller)
                {
                    controller.availability = InputAvailability::Unavailable;
                }
            }
            snapshot.controller_counters.socket_saturations =
                acceptor_diagnostics.socket_saturations();
            snapshot.controller_counters.accept_failures = acceptor_diagnostics.accept_failures();
            ControllerStatusResponse::Ok {
                schema_version: 1,
                diagnostics: snapshot,
            }
        }
        Ok(_) => ControllerStatusResponse::Error {
            schema_version: 1,
            reason: ControllerStatusErrorReason::UnsupportedVersion,
        },
        Err(_) => ControllerStatusResponse::Error {
            schema_version: 1,
            reason: ControllerStatusErrorReason::InvalidRequest,
        },
    })
}

async fn drain_inbound(stream: &mut UnixStream) -> io::Result<()> {
    let mut chunk = [0_u8; 8192];
    while stream.read(&mut chunk).await? != 0 {}
    Ok(())
}

async fn read_frame(stream: &mut UnixStream) -> io::Result<Vec<u8>> {
    let mut frame = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let remaining = MAX_FRAME_BYTES
            .saturating_add(1)
            .saturating_sub(frame.len());
        if remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Controller request exceeds 64 KiB",
            ));
        }
        let chunk_limit = remaining.min(chunk.len());
        let read = stream.read(&mut chunk[..chunk_limit]).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Controller request ended before newline",
            ));
        }
        if let Some(newline) = chunk[..read].iter().position(|byte| *byte == b'\n') {
            frame.extend_from_slice(&chunk[..newline]);
            if frame.len() > MAX_FRAME_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Controller request exceeds 64 KiB",
                ));
            }
            return Ok(frame);
        }
        frame.extend_from_slice(&chunk[..read]);
    }
}

/// Runs the fixed-precedence pipeline at the reducer's serialized boundary.
pub(crate) async fn service_request(
    request: Admitted<ControllerRequest>,
    session: &str,
    reducer: &mut Reducer,
    persistence: &mut RuntimePersistence,
) {
    let (request, admission) = request.into_parts();
    let decoded = decode_envelope(&request.frame);
    // Alias resolution for a merged-away key happens inside `validate_controller_event`
    // against current reducer state before any placeholder is staged. Event-id deduplication
    // is independent of alias keys, so there is no observable ordering interaction.
    if decoded
        .event_id
        .as_deref()
        .is_some_and(|event_id| persistence.is_duplicate(event_id))
    {
        let _ = request.responder.send(ControllerResponse::Duplicate);
        admission.complete();
        return;
    }
    let event = match decoded.decode_event(request.receipt_time_ms, session) {
        Ok(event) => event,
        Err(reason) => {
            let _ = request.responder.send(ControllerResponse::Rejected {
                reason: reason.into(),
            });
            admission.complete();
            return;
        }
    };
    // increment5-workload-harness: begin controller reducer timing scope
    #[cfg(feature = "workload-harness")]
    let workload_timing = request.workload.as_ref().map(|workload| {
        workload_timing_scope(
            WorkloadTimingKind::ControllerEvent,
            workload.sequence,
            std::sync::Arc::clone(&workload.hooks.timing_observer),
        )
    });
    // increment5-workload-harness: end controller reducer timing scope
    let delta = match reducer.validate_controller_event(&event) {
        Ok(delta) => delta,
        Err(reason) => {
            let _ = request.responder.send(ControllerResponse::Rejected {
                reason: reason.into(),
            });
            admission.complete();
            return;
        }
    };
    // increment5-workload-harness: begin controller terminal timestamp
    #[cfg(feature = "workload-harness")]
    let workload_terminal_ns = request
        .workload
        .as_ref()
        .map(|workload| (workload.hooks.clock)());
    // increment5-workload-harness: end controller terminal timestamp
    let Some(permit) = persistence.reserve_enqueue() else {
        let _ = request.responder.send(ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable,
        });
        admission.complete();
        return;
    };
    let pending = match reducer.commit_staged(delta, permit) {
        Ok(pending) => pending,
        Err(CommitStagedError::IngestSequenceExhausted) => {
            let _ = request.responder.send(ControllerResponse::Retryable {
                reason: RetryableReason::PersistenceUnavailable,
            });
            admission.complete();
            return;
        }
    };
    admission.complete();
    // increment5-workload-harness: begin controller publication observation
    #[cfg(feature = "workload-harness")]
    if let Some(workload_timing) = workload_timing {
        workload_timing.finish();
    }
    #[cfg(feature = "workload-harness")]
    if let Some(workload) = request.workload.as_ref() {
        (workload.hooks.terminal_observer)(WorkloadTerminalObservation {
            sequence: workload.sequence,
            terminal_ns: workload_terminal_ns
                .expect("workload terminal timestamp must accompany its context"),
            published_ns: (workload.hooks.clock)(),
        });
    }
    // increment5-workload-harness: end controller publication observation
    let _ = request.responder.send(ControllerResponse::Accepted);
    match persistence.finish_pending(pending).await {
        Ok(outcome) => {
            // increment5-workload-harness: begin controller persistence observation
            #[cfg(feature = "workload-harness")]
            if matches!(
                outcome,
                RuntimeWriteOutcome::Durable | RuntimeWriteOutcome::CommittedButDegraded(_)
            ) && let Some(workload) = request.workload.as_ref()
            {
                (workload.hooks.persistence_observer)(WorkloadPersistenceObservation {
                    sequence: workload.sequence,
                    persisted_ns: (workload.hooks.clock)(),
                });
            }
            // increment5-workload-harness: end controller persistence observation
            reducer.complete_operator_submission(outcome);
            if matches!(
                outcome,
                RuntimeWriteOutcome::CommittedButDegraded(_)
                    | RuntimeWriteOutcome::NotCommitted(_)
                    | RuntimeWriteOutcome::DurabilityUnknown(_)
            ) {
                tracing::warn!(
                    warning_code = "controller_persistence_degraded",
                    "accepted Controller event degraded persistence"
                );
            } else if outcome == RuntimeWriteOutcome::Skipped {
                tracing::warn!(
                    warning_code = "controller_persistence_skipped_after_admission",
                    "accepted Controller event persistence was skipped"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                warning_code = writer_error_code(&error),
                "accepted Controller event persistence failed unexpectedly"
            );
        }
    }
}

fn writer_error_code(error: &crate::store::writer::WriterError) -> &'static str {
    match error {
        crate::store::writer::WriterError::Store(_) => "controller_writer_startup_store_error",
        crate::store::writer::WriterError::Persistence(_) => "controller_writer_persistence_error",
        crate::store::writer::WriterError::Closed => "controller_writer_closed",
        crate::store::writer::WriterError::AcknowledgementDropped => {
            "controller_writer_acknowledgement_dropped"
        }
        crate::store::writer::WriterError::ThreadPanicked => "controller_writer_thread_panicked",
        crate::store::writer::WriterError::ThreadSpawn(_) => "controller_writer_thread_spawn",
    }
}

struct DecodedEnvelope {
    event_id: Option<String>,
    object: Result<Map<String, Value>, RejectReason>,
}

impl DecodedEnvelope {
    fn decode_event(
        &self,
        receipt_time_ms: i64,
        session: &str,
    ) -> Result<ControllerEvent, RejectReason> {
        decode_object(
            self.object.as_ref().map_err(|reason| *reason)?,
            receipt_time_ms,
            session,
        )
    }
}

fn decode_envelope(frame: &[u8]) -> DecodedEnvelope {
    let Ok(Value::Object(object)) = serde_json::from_slice::<Value>(frame) else {
        return DecodedEnvelope {
            event_id: None,
            object: Err(RejectReason::Invalid),
        };
    };
    let event_id = optional_valid_string(&object, "event_id");
    DecodedEnvelope {
        event_id,
        object: Ok(object),
    }
}

fn decode_object(
    object: &Map<String, Value>,
    receipt_time_ms: i64,
    session: &str,
) -> Result<ControllerEvent, RejectReason> {
    let schema_version = required_u64(object, "schema_version")?;
    if schema_version > 1 {
        return Err(RejectReason::UnsupportedVersion);
    }
    if schema_version == 0 {
        return Err(RejectReason::Invalid);
    }
    let event_id = required_string(object, "event_id")?;
    let emitted_at_ms = required_i64(object, "emitted_at_ms")?;
    let source = required_string(object, "source")?;
    let event_type = required_string(object, "event_type")?;
    let task_run_id = required_string(object, "task_run_id")?;
    let parent_task_run_id = optional_string(object, "parent_task_run_id")?;
    let depends_on_id = optional_string(object, "depends_on_id")?;
    let label = optional_string(object, "label")?;
    let reason = optional_string(object, "reason")?;
    let progress = optional_progress(object)?;
    let provider = optional_provider(object)?;
    let native_session_id = optional_string(object, "native_session_id")?;
    let terminal_id = optional_string(object, "terminal_id")?;
    if native_session_id.is_some() && provider.is_none() {
        return Err(RejectReason::Invalid);
    }
    let event = match event_type.as_str() {
        "dispatch" if depends_on_id.is_none() => ControllerEventKind::Dispatch {
            parent_task_run_id: parent_task_run_id.ok_or(RejectReason::Invalid)?,
        },
        "depends_on" if parent_task_run_id.is_none() => ControllerEventKind::DependsOn {
            depends_on_id: depends_on_id.ok_or(RejectReason::Invalid)?,
        },
        "task_started" if parent_task_run_id.is_none() && depends_on_id.is_none() => {
            ControllerEventKind::TaskStarted
        }
        "blocked" if parent_task_run_id.is_none() && depends_on_id.is_none() => {
            ControllerEventKind::Blocked
        }
        "progress" if parent_task_run_id.is_none() && depends_on_id.is_none() => {
            ControllerEventKind::Progress
        }
        "complete" if parent_task_run_id.is_none() && depends_on_id.is_none() => {
            ControllerEventKind::Complete
        }
        "failed" if parent_task_run_id.is_none() && depends_on_id.is_none() => {
            ControllerEventKind::Failed
        }
        "cancelled" if parent_task_run_id.is_none() && depends_on_id.is_none() => {
            ControllerEventKind::Cancelled
        }
        _ => return Err(RejectReason::Invalid),
    };
    Ok(ControllerEvent {
        schema_version,
        task_run_id,
        metadata: EventMetadata {
            event_id,
            timestamp_ms: emitted_at_ms,
            receipt_time_ms,
            source,
            source_event_type: event_type,
            herdr_session: session.to_owned(),
            workspace_id: None,
            tab_id: None,
            pane_id: None,
            terminal_id,
            provider,
            native_session_id,
            task_run_id: None,
            agent_node_id: None,
            task_state: None,
            execution_parent: None,
            dependency: None,
            source_coverage: Vec::<SourceCoverage>::new(),
            provider_metadata: None::<MinimalProviderMetadata>,
            label,
            reason,
            progress,
            ingest_seq: None,
        },
        event,
    })
}

fn required_string(object: &Map<String, Value>, key: &str) -> Result<String, RejectReason> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or(RejectReason::Invalid)
}

fn optional_valid_string(object: &Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn optional_string(object: &Map<String, Value>, key: &str) -> Result<Option<String>, RejectReason> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(RejectReason::Invalid),
    }
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Result<u64, RejectReason> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .ok_or(RejectReason::Invalid)
}

fn required_i64(object: &Map<String, Value>, key: &str) -> Result<i64, RejectReason> {
    object
        .get(key)
        .and_then(Value::as_i64)
        .ok_or(RejectReason::Invalid)
}

fn optional_progress(object: &Map<String, Value>) -> Result<Option<u16>, RejectReason> {
    match object.get("progress") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_f64()
            .ok_or(RejectReason::Invalid)
            .and_then(|value| quantize_progress(value).map_err(|_| RejectReason::Invalid))
            .map(Some),
        Some(_) => Err(RejectReason::Invalid),
    }
}

fn optional_provider(object: &Map<String, Value>) -> Result<Option<Provider>, RejectReason> {
    match object.get("provider") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value == "claude" => Ok(Some(Provider::Claude)),
        Some(Value::String(value)) if value == "codex" => Ok(Some(Provider::Codex)),
        Some(_) => Err(RejectReason::Invalid),
    }
}

impl From<RejectReason> for RejectResponseReason {
    fn from(reason: RejectReason) -> Self {
        match reason {
            RejectReason::Invalid => Self::Invalid,
            RejectReason::Cycle => Self::Cycle,
            RejectReason::Conflict => Self::Conflict,
            RejectReason::StaleEvent => Self::StaleEvent,
            RejectReason::UnsupportedVersion => Self::UnsupportedVersion,
        }
    }
}

/// Sends one envelope and validates the single closed-shape response.
pub async fn emit_to_endpoint(path: &Path, envelope: &ControllerEnvelope) -> EmitOutcome {
    let mut stream =
        match tokio::time::timeout(CONTROLLER_IO_TIMEOUT, UnixStream::connect(path)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => return EmitOutcome::Unresolved(error.to_string()),
            Err(_) => return EmitOutcome::Unresolved("connect timeout".to_owned()),
        };
    let mut request = match serde_json::to_vec(envelope) {
        Ok(request) => request,
        Err(error) => return EmitOutcome::Unresolved(error.to_string()),
    };
    request.push(b'\n');
    match tokio::time::timeout(CONTROLLER_IO_TIMEOUT, stream.write_all(&request)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return EmitOutcome::Unresolved(error.to_string()),
        Err(_) => return EmitOutcome::Unresolved("write timeout".to_owned()),
    }
    if let Err(error) = stream.shutdown().await {
        return EmitOutcome::Unresolved(error.to_string());
    }
    let mut response = Vec::new();
    let read = async {
        let mut limited = (&mut stream).take((MAX_FRAME_BYTES + 2) as u64);
        limited.read_to_end(&mut response).await
    };
    match tokio::time::timeout(CONTROLLER_IO_TIMEOUT, read).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => return EmitOutcome::Unresolved(error.to_string()),
        Err(_) => return EmitOutcome::Unresolved("read timeout".to_owned()),
    }
    if response.len() > MAX_FRAME_BYTES + 1 || response.last() != Some(&b'\n') {
        return EmitOutcome::Unresolved("invalid Controller response framing".to_owned());
    }
    response.pop();
    if response.contains(&b'\n') {
        return EmitOutcome::Unresolved("multiple Controller responses".to_owned());
    }
    match serde_json::from_slice::<ControllerResponse>(&response) {
        Ok(response) => EmitOutcome::Response(response),
        Err(error) => EmitOutcome::Unresolved(error.to_string()),
    }
}

/// Queries the additive read-only status surface without admitting a reducer request.
pub async fn query_status(path: &Path) -> io::Result<Value> {
    let mut stream = tokio::time::timeout(CONTROLLER_IO_TIMEOUT, UnixStream::connect(path))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Controller connect timed out"))??;
    tokio::time::timeout(
        CONTROLLER_IO_TIMEOUT,
        stream.write_all(b"{\"request\":\"status\",\"schema_version\":1}\n"),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Controller write timed out"))??;
    tokio::time::timeout(CONTROLLER_IO_TIMEOUT, stream.shutdown())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Controller shutdown timed out"))??;
    let mut response = Vec::new();
    let read = async {
        let mut limited = (&mut stream).take((MAX_FRAME_BYTES + 2) as u64);
        limited.read_to_end(&mut response).await
    };
    tokio::time::timeout(CONTROLLER_IO_TIMEOUT, read)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Controller read timed out"))??;
    if response.len() > MAX_FRAME_BYTES + 1 || response.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Controller status response framing",
        ));
    }
    response.pop();
    if response.contains(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "multiple Controller status responses",
        ));
    }
    serde_json::from_slice(&response).map_err(invalid_data)
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn unix_now_ms() -> i64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::future::pending;
    use std::sync::Arc;

    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, BufReader};

    use super::*;
    use crate::activity::OperatorSnapshot;
    use crate::lockfile::StateRoot;
    use crate::model::{DomainModel, NormalizedEvent};
    #[cfg(feature = "workload-harness")]
    use crate::performance::PerformanceClock;
    use crate::performance::{
        PerformanceIngress, TestPerformanceClock, admitted_channel, performance_tracker,
    };
    use crate::store::{PersistOp, RestoredState, open_writer, spawn_writer};

    struct TestOccurrenceSink;

    impl crate::diagnostics::PersistenceOccurrenceSink for TestOccurrenceSink {
        fn append(&self, _record: &[u8]) -> io::Result<()> {
            Ok(())
        }
    }

    fn test_runtime_diagnostics() -> (
        watch::Sender<RuntimeDiagnosticsSnapshot>,
        watch::Receiver<RuntimeDiagnosticsSnapshot>,
    ) {
        use crate::diagnostics::{
            ControllerCounterSnapshot, OccurrenceLogStatus, OwnerFreshness, PersistenceCounters,
            SourceCoverageSnapshot,
        };

        watch::channel(RuntimeDiagnosticsSnapshot {
            persistence: crate::store::writer::PersistenceStatus::Healthy,
            controller_input: ControllerInputStatus::Available,
            owner: OwnerFreshness::Current,
            persistence_counters: PersistenceCounters::default(),
            controller_counters: ControllerCounterSnapshot::default(),
            source_coverage: [
                DiagnosticSource::Herdr,
                DiagnosticSource::Controller,
                DiagnosticSource::Claude,
                DiagnosticSource::Codex,
            ]
            .into_iter()
            .map(|source| SourceCoverageSnapshot {
                source,
                availability: if source == DiagnosticSource::Controller {
                    InputAvailability::Available
                } else {
                    InputAvailability::Unavailable
                },
            })
            .collect(),
            dangling_announcement_components: 0,
            first_failure_log: OccurrenceLogStatus::NotAttempted,
        })
    }

    fn test_performance_ingress() -> PerformanceIngress {
        performance_tracker(Arc::new(TestPerformanceClock::new(Duration::ZERO))).0
    }

    async fn service_frame(
        frame: Value,
        reducer: &mut Reducer,
        persistence: &mut RuntimePersistence,
    ) -> ControllerResponse {
        let (sender, mut receiver) = admitted_channel(1, test_performance_ingress());
        let (responder, response) = oneshot::channel();
        assert!(
            sender
                .try_send(ControllerRequest {
                    frame: serde_json::to_vec(&frame).unwrap(),
                    receipt_time_ms: unix_now_ms(),
                    responder,
                    // increment5-workload-harness: begin unit request default workload context
                    #[cfg(feature = "workload-harness")]
                    workload: None,
                    // increment5-workload-harness: end unit request default workload context
                })
                .is_ok()
        );
        service_request(
            receiver.recv().await.unwrap(),
            "session",
            reducer,
            persistence,
        )
        .await;
        response.await.unwrap()
    }

    #[tokio::test]
    async fn invalid_but_admitted_frame_completes_after_typed_rejection() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut persistence, _diagnostics) =
            RuntimePersistence::new_for_test(writer, Arc::new(TestOccurrenceSink));
        let (mut reducer, shared) = Reducer::new(restored);
        let operator = OperatorSnapshot {
            activity: Arc::from(Vec::new()),
            terminal_times: Arc::new(HashMap::new()),
        };
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (ingress, mut sampler) = performance_tracker(clock);
        let (sender, mut receiver) = admitted_channel(1, ingress);
        let (responder, response) = oneshot::channel();
        assert!(
            sender
                .try_send(ControllerRequest {
                    frame: b"{".to_vec(),
                    receipt_time_ms: 1,
                    responder,
                    #[cfg(feature = "workload-harness")]
                    workload: None,
                })
                .is_ok()
        );
        let before = sampler.sample(&shared.borrow(), &operator, 0);
        let admitted = receiver.recv().await.unwrap();

        service_request(admitted, "session", &mut reducer, &mut persistence).await;
        let observed_response = response.await.unwrap();
        let after = sampler.sample(&shared.borrow(), &operator, 0);

        drop(persistence);
        lifecycle.shutdown().await.unwrap();
        assert_eq!(before.pending_events, 1);
        assert_eq!(
            observed_response,
            ControllerResponse::Rejected {
                reason: RejectResponseReason::Invalid,
            }
        );
        assert_eq!(after.pending_events, 0);
    }

    #[cfg(feature = "workload-harness")]
    #[tokio::test]
    async fn workload_observer_and_received_request_share_the_exact_admission_stamp() {
        let origin = Duration::from_secs(123_456) + Duration::from_nanos(789);
        let origin_ns = u64::try_from(origin.as_nanos()).unwrap();
        let clock = Arc::new(TestPerformanceClock::new(origin));
        let (performance, mut sampler) = performance_tracker(clock.clone());
        let admissions = Arc::new(std::sync::Mutex::new(Vec::new()));
        let terminals = Arc::new(std::sync::Mutex::new(Vec::new()));
        let persisted = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hooks = WorkloadControllerHooks {
            clock: {
                let clock = Arc::clone(&clock);
                Arc::new(move || u64::try_from(clock.monotonic_now().as_nanos()).unwrap())
            },
            admission_observer: {
                let admissions = Arc::clone(&admissions);
                Arc::new(move |sample| admissions.lock().unwrap().push(sample))
            },
            terminal_observer: {
                let terminals = Arc::clone(&terminals);
                Arc::new(move |sample| terminals.lock().unwrap().push(sample))
            },
            persistence_observer: {
                let persisted = Arc::clone(&persisted);
                Arc::new(move |sample| persisted.lock().unwrap().push(sample))
            },
            timing_observer: Arc::new(|_| {}),
        };
        let (sender, mut receiver) = request_channel_with_workload_harness(
            1,
            ControllerDiagnosticsHandle::default(),
            performance,
            hooks,
        );
        let submit = tokio::spawn(async move {
            sender
                .submit_workload_frame(
                    serde_json::to_vec(&json!({
                        "schema_version": 1,
                        "event_id": "absolute-admission",
                        "emitted_at_ms": 1,
                        "source": "workload-test",
                        "event_type": "task_started",
                        "task_run_id": "absolute-run",
                    }))
                    .unwrap(),
                    1,
                    77,
                    origin_ns,
                )
                .await
        });
        let admitted = receiver.recv().await.unwrap();
        let workload_stamp = admitted.workload_stamp();

        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut persistence, _diagnostics) =
            RuntimePersistence::new_for_test(writer, Arc::new(TestOccurrenceSink));
        let (mut reducer, shared) = Reducer::new(restored);
        service_request(admitted, "session", &mut reducer, &mut persistence).await;
        assert_eq!(submit.await.unwrap(), ControllerResponse::Accepted);

        let observed_admissions = admissions.lock().unwrap().clone();
        assert_eq!(observed_admissions.len(), 1);
        assert_eq!(observed_admissions[0].sequence, 77);
        assert_eq!(observed_admissions[0].scheduled_ns, origin_ns);
        assert_eq!(observed_admissions[0].admitted_ns, origin_ns);
        assert_eq!(workload_stamp, (1, origin));
        let observed_terminals = terminals.lock().unwrap().clone();
        assert_eq!(observed_terminals.len(), 1);
        assert_eq!(observed_terminals[0].sequence, 77);
        assert_eq!(observed_terminals[0].terminal_ns, origin_ns);
        assert_eq!(observed_terminals[0].published_ns, origin_ns);
        let observed_persistence = persisted.lock().unwrap().clone();
        assert_eq!(observed_persistence.len(), 1);
        assert_eq!(observed_persistence[0].sequence, 77);
        assert_eq!(observed_persistence[0].persisted_ns, origin_ns);
        let snapshot = sampler.sample(
            &shared.borrow(),
            &OperatorSnapshot {
                activity: Arc::from(Vec::new()),
                terminal_times: Arc::new(HashMap::new()),
            },
            0,
        );
        assert_eq!(
            (
                snapshot.pending_events,
                snapshot.admission_high_water,
                snapshot.completion_high_water,
            ),
            (0, 1, 1)
        );

        drop(persistence);
        lifecycle.shutdown().await.unwrap();
    }

    struct ScriptedAcceptSource {
        accepts: VecDeque<io::Result<UnixStream>>,
    }

    impl AcceptSource for ScriptedAcceptSource {
        async fn accept(&mut self) -> io::Result<UnixStream> {
            let accepted = self.accepts.pop_front();
            match accepted {
                Some(accepted) => accepted,
                None => pending().await,
            }
        }
    }

    #[tokio::test]
    async fn acceptor_counter_change_notifies_runtime_owner() {
        let acceptor_diagnostics = ControllerDiagnosticsHandle::default();
        let (sender, mut receiver) =
            request_channel(1, acceptor_diagnostics.clone(), test_performance_ingress());
        let keepalive = sender.clone();
        let (queued_responder, _queued_response) = oneshot::channel();
        sender
            .sender
            .try_send(ControllerRequest {
                frame: b"queued".to_vec(),
                receipt_time_ms: 1,
                responder: queued_responder,
                // increment5-workload-harness: begin queued unit request default workload context
                #[cfg(feature = "workload-harness")]
                workload: None,
                // increment5-workload-harness: end queued unit request default workload context
            })
            .unwrap();
        let (mut client, server) = UnixStream::pair().unwrap();
        let handler = tokio::spawn(handle_connection(server, sender, None));
        client
            .write_all(b"{\"schema_version\":1,\"event_id\":\"full\"}\n")
            .await
            .unwrap();
        client.shutdown().await.unwrap();
        let mut response = Vec::new();
        BufReader::new(&mut client)
            .read_until(b'\n', &mut response)
            .await
            .unwrap();

        drop(receiver.recv().await.unwrap());
        let wake = tokio::time::timeout(Duration::from_secs(1), receiver.recv_runtime_event())
            .await
            .expect("acceptor-only counter change must wake runtime diagnostics");
        assert!(matches!(wake, ControllerRuntimeEvent::DiagnosticsChanged));
        assert_eq!(acceptor_diagnostics.socket_saturations(), 1);
        assert_eq!(
            serde_json::from_slice::<ControllerResponse>(&response).unwrap(),
            ControllerResponse::Retryable {
                reason: RetryableReason::Busy,
            }
        );
        handler.await.unwrap().unwrap();
        drop(keepalive);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn acceptor_retries_after_accept_error() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let mut source = ScriptedAcceptSource {
            accepts: VecDeque::from([Err(io::Error::from_raw_os_error(libc::EMFILE)), Ok(server)]),
        };
        let diagnostics = ControllerDiagnosticsHandle::default();
        let (sender, mut receiver) =
            request_channel(1, diagnostics.clone(), test_performance_ingress());
        let responder = tokio::spawn(async move {
            let admitted = receiver.recv().await.unwrap();
            let (request, admission) = admitted.into_parts();
            request
                .responder
                .send(ControllerResponse::Accepted)
                .unwrap();
            admission.complete();
        });
        let cancellation = CancellationToken::new();
        let (_diagnostics_sender, runtime_diagnostics) = test_runtime_diagnostics();
        let cancel_after_response = cancellation.clone();
        let client_exchange = async move {
            let mut frame = serde_json::to_vec(&json!({
                "schema_version": 1,
                "event_id": "after-accept-error",
                "emitted_at_ms": 1,
                "source": "test",
                "event_type": "task_started",
                "task_run_id": "run"
            }))
            .unwrap();
            frame.push(b'\n');
            client.write_all(&frame).await.unwrap();
            client.shutdown().await.unwrap();

            let mut response = Vec::new();
            let mut reader = BufReader::new(client);
            tokio::time::timeout(
                Duration::from_secs(5),
                reader.read_until(b'\n', &mut response),
            )
            .await
            .expect("acceptor did not handle the scripted connection")
            .unwrap();
            cancel_after_response.cancel();
            serde_json::from_slice::<ControllerResponse>(&response).unwrap()
        };

        let (acceptor, response) = tokio::join!(
            run_acceptor(&mut source, sender, cancellation, Some(runtime_diagnostics),),
            client_exchange
        );

        assert_eq!(response, ControllerResponse::Accepted);
        assert_eq!(diagnostics.accept_failures(), 1);
        acceptor.unwrap();
        responder.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_timeout_drains_before_handler_completion() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let diagnostics = ControllerDiagnosticsHandle::default();
        let (sender, _receiver) = request_channel(1, diagnostics, test_performance_ingress());
        let (_diagnostics_sender, runtime_diagnostics) = test_runtime_diagnostics();
        let handle = tokio::spawn(handle_connection(server, sender, Some(runtime_diagnostics)));

        client.write_all(b"{").await.unwrap();
        let mut response = Vec::new();
        {
            let mut reader = BufReader::new(&mut client);
            tokio::time::timeout(
                Duration::from_secs(8),
                reader.read_until(b'\n', &mut response),
            )
            .await
            .expect("read-timeout response did not arrive")
            .unwrap();
        }
        assert_eq!(
            serde_json::from_slice::<ControllerResponse>(&response).unwrap(),
            ControllerResponse::Rejected {
                reason: RejectResponseReason::Invalid,
            }
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        client.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("handler did not finish after inbound EOF")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn pipeline_applies_deduplicates_and_rejects_without_transport() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let store = open_writer(&root).unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut persistence, _diagnostics) =
            RuntimePersistence::new_for_test(writer, Arc::new(TestOccurrenceSink));
        let (mut reducer, shared) = Reducer::new(restored);
        let started = json!({
            "schema_version": 1,
            "event_id": "accepted",
            "emitted_at_ms": -9,
            "source": "test",
            "event_type": "task_started",
            "task_run_id": "external-key",
            "unknown": true
        });

        assert_eq!(
            service_frame(started.clone(), &mut reducer, &mut persistence).await,
            ControllerResponse::Accepted
        );
        assert_eq!(
            service_frame(started, &mut reducer, &mut persistence).await,
            ControllerResponse::Duplicate
        );
        assert_eq!(
            service_frame(
                json!({
                    "schema_version": 1,
                    "event_id": "invalid-cycle",
                    "emitted_at_ms": 1,
                    "source": "test",
                    "event_type": "dispatch",
                    "task_run_id": "self",
                    "parent_task_run_id": "self"
                }),
                &mut reducer,
                &mut persistence,
            )
            .await,
            ControllerResponse::Rejected {
                reason: RejectResponseReason::Cycle
            }
        );
        assert!(
            shared
                .borrow()
                .task_run_by_key(&crate::model::RunKey::Controller("external-key".to_owned()))
                .is_some()
        );
        assert!(
            shared
                .borrow()
                .task_run_by_key(&crate::model::RunKey::Controller("self".to_owned()))
                .is_none()
        );

        lifecycle.shutdown().await.unwrap();
    }

    #[test]
    fn i4_status_request_is_closed_and_event_wire_is_compatible() {
        let (_diagnostics_sender, diagnostics) = test_runtime_diagnostics();
        let acceptor_diagnostics = ControllerDiagnosticsHandle::default();
        let valid = status_response(
            br#"{"request":"status","schema_version":1}"#,
            Some(&diagnostics),
            &acceptor_diagnostics,
        )
        .unwrap();
        assert!(matches!(
            &valid,
            ControllerStatusResponse::Ok {
                schema_version: 1,
                ..
            }
        ));
        assert_eq!(
            serde_json::to_vec(&valid).unwrap(),
            br#"{"status":"ok","schema_version":1,"diagnostics":{"persistence":{"status":"healthy"},"controller_input":{"status":"available"},"owner":"current","persistence_counters":{"not_committed_batches":0,"durability_unknown_batches":0,"committed_but_degraded_batches":0,"skipped_batches":0,"skipped_owner_updates":0},"controller_counters":{"binding_conflicts":0,"terminal_blocked_progress_noops":0,"terminal_forward_reference_creations":0,"dangling_announcement_components":0,"ingest_sequence_exhaustions":0,"provider_parent_conflicts":0,"provider_identity_disagreements":0,"socket_saturations":0,"accept_failures":0},"source_coverage":[{"source":"herdr","availability":"unavailable"},{"source":"controller","availability":"available"},{"source":"claude","availability":"unavailable"},{"source":"codex","availability":"unavailable"}],"dangling_announcement_components":0,"first_failure_log":"not_attempted"}}"#,
        );
        let extra = status_response(
            br#"{"request":"status","schema_version":1,"extra":true}"#,
            Some(&diagnostics),
            &acceptor_diagnostics,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_vec(&extra).unwrap(),
            br#"{"status":"error","schema_version":1,"reason":"invalid_request"}"#
        );
        let unsupported = status_response(
            br#"{"request":"status","schema_version":2}"#,
            Some(&diagnostics),
            &acceptor_diagnostics,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_vec(&unsupported).unwrap(),
            br#"{"status":"error","schema_version":1,"reason":"unsupported_version"}"#
        );
        assert!(
            status_response(
                br#"{"schema_version":1}"#,
                Some(&diagnostics),
                &acceptor_diagnostics,
            )
            .is_none()
        );
    }

    #[test]
    fn i4_status_request_key_on_existing_event_remains_an_ignored_extension() {
        let (_diagnostics_sender, diagnostics) = test_runtime_diagnostics();
        let acceptor_diagnostics = ControllerDiagnosticsHandle::default();
        assert!(
            status_response(
                br#"{"event_id":null,"request":"status","schema_version":1}"#,
                Some(&diagnostics),
                &acceptor_diagnostics,
            )
            .is_none()
        );
        assert!(
            status_response(
                br#"{"event_id":"event","request":"status","schema_version":1}"#,
                Some(&diagnostics),
                &acceptor_diagnostics,
            )
            .is_none()
        );
    }

    #[test]
    fn label_reason_utf8_boundary_truncation_and_control_escape() {
        let label = "é".repeat(130);
        let frame = serde_json::to_vec(&json!({
            "schema_version": 1,
            "event_id": "text",
            "emitted_at_ms": 1,
            "source": "test",
            "event_type": "task_started",
            "task_run_id": "run",
            "label": label,
            "reason": "line\n\tend",
            "progress": null,
            "provider": null,
            "native_session_id": null,
            "terminal_id": null
        }))
        .unwrap();
        let decoded = decode_envelope(&frame);
        let event = decoded.decode_event(2, "session").unwrap();
        let (reducer, _shared) = Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let delta = reducer.validate_controller_event(&event).unwrap();
        let metadata = delta
            .batch
            .iter()
            .find_map(|operation| match operation {
                PersistOp::RecordEvent { event, .. } => match event.as_ref() {
                    NormalizedEvent::ControllerEvent { metadata, .. } => Some(metadata),
                    _ => None,
                },
                _ => None,
            })
            .unwrap();

        assert_eq!(metadata.label.as_deref().unwrap().len(), 256);
        assert_eq!(metadata.label.as_deref().unwrap(), "é".repeat(128));
        assert_eq!(metadata.reason.as_deref(), Some("line\\n\\tend"));
        assert!(
            metadata
                .reason
                .as_deref()
                .is_some_and(|reason| !reason.chars().any(char::is_control))
        );
    }
}
