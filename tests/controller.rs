use std::fs::{self, Permissions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use herdr_top::diagnostics::{
    ControllerCounterSnapshot, ControllerInputStatus, DiagnosticSource, InputAvailability,
    OccurrenceLogStatus, OwnerFreshness, PersistenceCounters, RuntimeDiagnosticsSnapshot,
    SourceCoverageSnapshot,
};
use herdr_top::herdr::collector::{self, CollectorHandle, SourceAvailability};
use herdr_top::herdr::controller::{
    self, ControllerEnvelope, ControllerResponse, RejectResponseReason, RetryableReason,
};
use herdr_top::lockfile::{OwnerLock, StateRoot, state_root_in, try_acquire};
use herdr_top::model::{
    ControllerDiagnosticsHandle, ControllerEventKind, DisplayOrdinal, EventMetadata,
    MinimalProviderMetadata, NormalizedEvent, Provider, RunId, RunKey, SourceCoverage, TaskRun,
    TaskState,
};
use herdr_top::performance::{PerformanceIngress, SystemPerformanceClock, performance_tracker};
use herdr_top::rendezvous::{
    ControllerSocketStatus, ValidatedRuntimeDir, open_runtime_dir_at, prepare_controller_socket,
    shutdown_controller_socket,
};
use herdr_top::session_key;
use herdr_top::store::{self, PersistOp, PersistTaskRun, WriterLifecycle};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SESSION: &str = "controller-test-session";
const PERSISTENCE_FAILURE_EVENT_ID: &str = "test-persistence-failure";

struct RunningController {
    _state: TempDir,
    root: StateRoot,
    _socket_dir: TempDir,
    socket_path: PathBuf,
    collector: CollectorHandle,
    lifecycle: WriterLifecycle,
    persistence: watch::Receiver<store::PersistenceHealthSnapshot>,
}

struct RendezvousController {
    _state: TempDir,
    runtime_base: TempDir,
    runtime: ValidatedRuntimeDir,
    owner_lock: OwnerLock,
    socket_status: ControllerSocketStatus,
    collector: CollectorHandle,
    lifecycle: WriterLifecycle,
}

impl RendezvousController {
    async fn start() -> Self {
        let state = tempfile::tempdir().unwrap();
        let runtime_base = tempfile::tempdir().unwrap();
        fs::set_permissions(runtime_base.path(), Permissions::from_mode(0o700)).unwrap();
        let key = session_key::encode(SESSION).unwrap();
        let root = state_root_in(state.path(), &key).unwrap();
        let owner_lock = try_acquire(&root).unwrap().unwrap();
        let runtime = open_runtime_dir_at(runtime_base.path()).unwrap();
        let socket_status = prepare_controller_socket(&runtime, &key, &owner_lock).unwrap();
        let listener = socket_status.try_clone_listener().unwrap();
        assert!(listener.is_some());
        let store = store::open_writer(&root).unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = store::spawn_writer(store).unwrap();
        let collector = collector::spawn_with_controller(
            runtime_base.path().join("missing-herdr.sock"),
            SESSION.to_owned(),
            restored,
            writer,
            listener,
        )
        .await
        .unwrap();
        Self {
            _state: state,
            runtime_base,
            runtime,
            owner_lock,
            socket_status,
            collector,
            lifecycle,
        }
    }

    async fn stop(self) {
        self.collector.stop().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), self.lifecycle.shutdown())
            .await
            .expect("writer shutdown timed out")
            .unwrap();
        shutdown_controller_socket(self.socket_status, &self.owner_lock).unwrap();
        drop(self.runtime);
    }
}

impl RunningController {
    async fn start() -> Self {
        Self::start_with_store_setup(|_| {}).await
    }

    async fn start_seeded(seed: Vec<PersistOp>) -> Self {
        Self::start_with_store_seed_and_setup(seed, |_| {}, SourceAvailability::Available).await
    }

    async fn start_with_persistence_failure() -> Self {
        Self::start_with_store_setup(install_persistence_failure_trigger).await
    }

    async fn start_with_store_setup(setup: impl FnOnce(&StateRoot)) -> Self {
        Self::start_with_store_setup_and_coverage(setup, SourceAvailability::Available).await
    }

    async fn start_with_store_setup_and_coverage(
        setup: impl FnOnce(&StateRoot),
        controller_coverage: SourceAvailability,
    ) -> Self {
        Self::start_with_store_seed_and_setup(Vec::new(), setup, controller_coverage).await
    }

    async fn start_with_store_seed_and_setup(
        seed: Vec<PersistOp>,
        setup: impl FnOnce(&StateRoot),
        controller_coverage: SourceAvailability,
    ) -> Self {
        let state = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("controller.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let key = session_key::encode(SESSION).unwrap();
        let root = state_root_in(state.path(), &key).unwrap();
        let mut store = store::open_writer(&root).unwrap();
        if !seed.is_empty() {
            store.apply_batch(seed).unwrap();
        }
        setup(&root);
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = store::spawn_writer(store).unwrap();
        let persistence = writer.subscribe_persistence();
        let collector = collector::spawn_with_controller_coverage(
            socket_dir.path().join("missing-herdr.sock"),
            SESSION.to_owned(),
            restored,
            writer,
            Some(listener),
            controller_coverage,
        )
        .await
        .unwrap();
        Self {
            _state: state,
            root,
            _socket_dir: socket_dir,
            socket_path,
            collector,
            lifecycle,
            persistence,
        }
    }

    async fn send(&self, value: &Value) -> ControllerResponse {
        send_raw(&self.socket_path, value).await
    }

    async fn send_bounded(&self, value: &Value) -> ControllerResponse {
        tokio::time::timeout(Duration::from_secs(8), self.send(value))
            .await
            .expect("Controller wire exchange timed out")
    }

    async fn stop(self) {
        self.collector.stop().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), self.lifecycle.shutdown())
            .await
            .expect("writer shutdown timed out")
            .unwrap();
    }

    async fn stop_and_reopen(self) -> (TempDir, store::Store) {
        let Self {
            _state,
            root,
            collector,
            lifecycle,
            ..
        } = self;
        collector.stop().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), lifecycle.shutdown())
            .await
            .expect("writer shutdown timed out")
            .unwrap();
        let reopened = store::open_writer(&root).unwrap();
        (_state, reopened)
    }

    async fn wait_for_persistence_degradation(&self) -> store::PersistenceFailure {
        let mut persistence = self.persistence.clone();
        let mut diagnostics = self.collector.diagnostics.clone();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let writer_status = persistence.borrow().status;
                let diagnostic_status = diagnostics.borrow().persistence;
                if let (
                    store::PersistenceStatus::Degraded {
                        failure: writer_failure,
                    },
                    store::PersistenceStatus::Degraded {
                        failure: diagnostic_failure,
                    },
                ) = (writer_status, diagnostic_status)
                {
                    assert_eq!(writer_failure, diagnostic_failure);
                    break writer_failure;
                }
                tokio::select! {
                    result = persistence.changed() => {
                        result.expect("persistence publisher should remain available");
                    }
                    result = diagnostics.changed() => {
                        result.expect("diagnostics publisher should remain available");
                    }
                }
            }
        })
        .await
        .expect("writer health and runtime diagnostics must report degradation")
    }

    async fn induce_persistence_failure(&self) {
        assert_eq!(
            self.persistence.borrow().status,
            store::PersistenceStatus::Healthy
        );
        assert_eq!(
            self.collector.diagnostics.borrow().persistence,
            store::PersistenceStatus::Healthy
        );
        assert_eq!(
            self.send(&envelope(
                PERSISTENCE_FAILURE_EVENT_ID,
                "task_started",
                "persistence-failure-run",
            ))
            .await,
            ControllerResponse::Accepted
        );
        self.wait_for_persistence_degradation().await;
    }
}

fn envelope(event_id: &str, event_type: &str, task_run_id: &str) -> Value {
    json!({
        "schema_version": 1,
        "event_id": event_id,
        "emitted_at_ms": 123,
        "source": "controller-test",
        "event_type": event_type,
        "task_run_id": task_run_id,
        "parent_task_run_id": null,
        "depends_on_id": null,
        "label": null,
        "reason": null,
        "progress": null,
        "provider": null,
        "native_session_id": null,
        "terminal_id": null
    })
}

fn dispatch(event_id: &str, child: &str, parent: &str) -> Value {
    let mut value = envelope(event_id, "dispatch", child);
    value["parent_task_run_id"] = json!(parent);
    value
}

fn depends_on(event_id: &str, dependent: &str, prerequisite: &str) -> Value {
    let mut value = envelope(event_id, "depends_on", dependent);
    value["depends_on_id"] = json!(prerequisite);
    value
}

fn provider_record(event_id: &str, event_kind: &str) -> PersistOp {
    let activity = MinimalProviderMetadata {
        agent_id: Some("provider-agent".to_owned()),
        event_kind: Some(event_kind.to_owned()),
        ..MinimalProviderMetadata::default()
    };
    PersistOp::RecordEvent {
        event: Box::new(NormalizedEvent::AgentActivity {
            metadata: EventMetadata {
                event_id: event_id.to_owned(),
                timestamp_ms: 123,
                receipt_time_ms: 124,
                source: "provider".to_owned(),
                source_event_type: "activity".to_owned(),
                herdr_session: SESSION.to_owned(),
                workspace_id: None,
                tab_id: None,
                pane_id: None,
                terminal_id: None,
                provider: Some(Provider::Codex),
                native_session_id: Some("provider-agent".to_owned()),
                task_run_id: None,
                agent_node_id: Some("agent:codex:provider-agent".to_owned()),
                task_state: None,
                execution_parent: None,
                dependency: None,
                source_coverage: vec![SourceCoverage {
                    source: "codex".to_owned(),
                    available: true,
                    detail: None,
                }],
                provider_metadata: Some(activity.clone()),
                label: None,
                reason: None,
                progress: None,
                ingest_seq: None,
            },
            agent_node_id: "agent:codex:provider-agent".to_owned(),
            activity,
        }),
        seen_at_ms: unix_now_ms(),
    }
}

fn expired_controller_record(event_id: &str) -> PersistOp {
    PersistOp::RecordEvent {
        event: Box::new(NormalizedEvent::ControllerEvent {
            metadata: EventMetadata {
                event_id: event_id.to_owned(),
                timestamp_ms: 0,
                receipt_time_ms: 0,
                source: "seed".to_owned(),
                source_event_type: "task_started".to_owned(),
                herdr_session: SESSION.to_owned(),
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
            },
            event: ControllerEventKind::TaskStarted,
        }),
        seen_at_ms: 0,
    }
}

fn install_persistence_failure_trigger(root: &StateRoot) {
    rusqlite::Connection::open(store::database_path(root))
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER fail_test_persistence BEFORE INSERT ON events \
             WHEN NEW.event_id = '{PERSISTENCE_FAILURE_EVENT_ID}' \
             BEGIN SELECT RAISE(ABORT, 'test persistence failure'); END;"
        ))
        .unwrap();
}

async fn wait_for_durable_row_count(running: &RunningController, query: &str, expected: i64) {
    wait_for_durable_row_count_with_timeout(running, query, expected, Duration::from_secs(3)).await;
}

async fn wait_for_durable_row_count_with_timeout(
    running: &RunningController,
    query: &str,
    expected: i64,
    timeout: Duration,
) {
    // This durable-row poll is a liveness wait, not a barrier: rows commit before cleanup/failure publication, so the health guard cannot catch a post-commit failure.
    tokio::time::timeout(timeout, async {
        loop {
            assert_eq!(
                running.persistence.borrow().status,
                store::PersistenceStatus::Healthy,
                "persistence degraded while waiting for durable rows"
            );
            assert_eq!(
                running.collector.diagnostics.borrow().persistence,
                store::PersistenceStatus::Healthy,
                "runtime diagnostics degraded while waiting for durable rows"
            );
            let rows: i64 = rusqlite::Connection::open(store::database_path(&running.root))
                .unwrap()
                .query_row(query, [], |row| row.get(0))
                .unwrap();
            if rows == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("durable row count did not reach {expected}: {query}"));
}

async fn send_raw(path: &Path, value: &Value) -> ControllerResponse {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    send_bytes(path, &bytes).await
}

async fn send_bytes(path: &Path, bytes: &[u8]) -> ControllerResponse {
    serde_json::from_slice(&send_wire_bytes(path, bytes).await).unwrap()
}

async fn send_wire_value(path: &Path, value: &Value) -> Value {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    serde_json::from_slice(&send_wire_bytes_bounded(path, &bytes).await).unwrap()
}

async fn shutdown_client_write(stream: &mut UnixStream) {
    match stream.shutdown().await {
        Ok(()) => {}
        // Darwin may report ENOTCONN after the peer closes; Linux accepts the same shutdown(2).
        Err(error) if error.kind() == std::io::ErrorKind::NotConnected => {}
        Err(error) => panic!("client-side UnixStream shutdown failed: {error}"),
    }
}

async fn send_wire_bytes(path: &Path, bytes: &[u8]) -> Vec<u8> {
    let mut stream = UnixStream::connect(path).await.unwrap();
    stream.write_all(bytes).await.unwrap();
    shutdown_client_write(&mut stream).await;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    response
}

async fn send_wire_bytes_bounded(path: &Path, bytes: &[u8]) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(8), async {
        send_wire_bytes(path, bytes).await
    })
    .await
    .expect("Controller wire exchange timed out")
}

#[tokio::test]
async fn client_shutdown_after_peer_close_preserves_buffered_response() {
    let (mut client, mut server) = UnixStream::pair().unwrap();
    let server = tokio::spawn(async move {
        let mut request = [0; 7];
        server.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        server.write_all(b"response").await.unwrap();
    });

    client.write_all(b"request").await.unwrap();
    server.await.unwrap();
    shutdown_client_write(&mut client).await;

    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    assert_eq!(response, b"response");
}

fn controlled_diagnostics() -> (
    watch::Sender<RuntimeDiagnosticsSnapshot>,
    watch::Receiver<RuntimeDiagnosticsSnapshot>,
) {
    let controller_counters = ControllerCounterSnapshot::default();
    watch::channel(RuntimeDiagnosticsSnapshot {
        persistence: store::PersistenceStatus::Healthy,
        persistence_detail: None,
        controller_input: ControllerInputStatus::Available,
        owner: OwnerFreshness::Current,
        persistence_counters: PersistenceCounters::default(),
        controller_counters,
        enrichment_counters: herdr_top::diagnostics::EnrichmentCounterSnapshot::default(),
        provider_counters: herdr_top::diagnostics::ProviderCounterSnapshot::default(),
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
        dangling_announcement_components: controller_counters.dangling_announcement_components,
        first_failure_log: OccurrenceLogStatus::NotAttempted,
    })
}

fn spawn_invalid_snapshot_herdr(path: &Path) -> JoinHandle<()> {
    let listener = UnixListener::bind(path).unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                    return;
                }
                let request: Value = serde_json::from_str(&line).unwrap();
                let id = request["id"].clone();
                let response = match request["method"].as_str() {
                    Some("events.subscribe") => {
                        json!({"id": id, "result": {"type": "subscription_started"}})
                    }
                    Some("session.snapshot") => json!({
                        "id": id,
                        "result": {
                            "type": "session_snapshot",
                            "snapshot": {
                                "version": "0.8.0",
                                "protocol": 19,
                                "focused_workspace_id": null,
                                "focused_tab_id": null,
                                "focused_pane_id": null,
                                "workspaces": [],
                                "tabs": [],
                                "panes": [{
                                    "pane_id": "missing-tab-pane",
                                    "terminal_id": "missing-tab-terminal",
                                    "workspace_id": "missing-tab-workspace",
                                    "tab_id": null
                                }]
                            }
                        }
                    }),
                    _ => return,
                };
                let mut bytes = serde_json::to_vec(&response).unwrap();
                bytes.push(b'\n');
                if reader.get_mut().write_all(&bytes).await.is_err() {
                    return;
                }
                if request["method"] == "events.subscribe" {
                    let mut remaining = Vec::new();
                    let _ = reader.read_to_end(&mut remaining).await;
                }
            });
        }
    })
}

fn rejected(reason: RejectResponseReason) -> ControllerResponse {
    ControllerResponse::Rejected { reason }
}

fn emit_command(runtime_base: &Path, strict: bool, event_id: &str) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_herdr-top"));
    command
        .args([
            "--session",
            SESSION,
            "emit",
            "--event-id",
            event_id,
            "--emitted-at-ms",
            "123",
            "--source",
            "integration-test",
            "--event-type",
            "task_started",
            "--task-run-id",
            "run",
        ])
        .env("XDG_RUNTIME_DIR", runtime_base)
        .env_remove("TMPDIR")
        .env_remove("HERDR_SESSION")
        .env_remove("HERDR_ENV");
    if strict {
        command.arg("--strict");
    }
    command.output().unwrap()
}

fn hook_emit_command(
    runtime_base: &Path,
    strict: bool,
    provider: &str,
    payload: Vec<u8>,
) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_herdr-top"));
    command
        .args(["--session", SESSION, "emit", "--from-hook", provider])
        .env("XDG_RUNTIME_DIR", runtime_base)
        .env_remove("TMPDIR")
        .env_remove("HERDR_SESSION")
        .env_remove("HERDR_ENV")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if strict {
        command.arg("--strict");
    }

    let mut child = command.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let writer = std::thread::spawn(move || stdin.write_all(&payload));
    let output = child.wait_with_output().unwrap();
    match writer.join().unwrap() {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => {}
        Err(error) => panic!("hook stdin write failed: {error}"),
    }
    output
}

fn scripted_emit_listener() -> (
    TempDir,
    ValidatedRuntimeDir,
    std::os::unix::net::UnixListener,
) {
    let runtime_base = tempfile::tempdir().unwrap();
    fs::set_permissions(runtime_base.path(), Permissions::from_mode(0o700)).unwrap();
    let runtime = open_runtime_dir_at(runtime_base.path()).unwrap();
    let key = session_key::encode(SESSION).unwrap();
    let runtime_child = runtime_base.path().join("herdr-top");
    let sentinel = runtime_child.join(format!("{}.name", key.hash16()));
    fs::write(&sentinel, SESSION.as_bytes()).unwrap();
    fs::set_permissions(&sentinel, Permissions::from_mode(0o600)).unwrap();
    let socket = runtime_child.join(format!("{}.sock", key.hash16()));
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    (runtime_base, runtime, listener)
}

fn output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

async fn serve_responses(
    listener: std::os::unix::net::UnixListener,
    responses: Vec<ControllerResponse>,
) {
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::UnixListener::from_std(listener).unwrap();
    for response in responses {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            let read = stream.read(&mut byte).await.unwrap();
            if read == 0 || byte[0] == b'\n' {
                break;
            }
            request.push(byte[0]);
        }
        assert!(!request.is_empty());
        let mut bytes = serde_json::to_vec(&response).unwrap();
        bytes.push(b'\n');
        stream.write_all(&bytes).await.unwrap();
        stream.shutdown().await.unwrap();
    }
}

struct CapturedEnvelope {
    wire: Value,
    envelope: ControllerEnvelope,
}

async fn receive_captured_envelope(
    listener: &UnixListener,
    response: &ControllerResponse,
) -> CapturedEnvelope {
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(8), listener.accept())
        .await
        .expect("adapter connection timed out")
        .unwrap();
    let mut request = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        let read = stream.read(&mut byte).await.unwrap();
        if read == 0 || byte[0] == b'\n' {
            break;
        }
        request.push(byte[0]);
    }
    assert!(!request.is_empty());
    let wire: Value = serde_json::from_slice(&request).unwrap();
    let envelope = serde_json::from_value(wire.clone()).unwrap();
    let mut bytes = serde_json::to_vec(response).unwrap();
    bytes.push(b'\n');
    stream.write_all(&bytes).await.unwrap();
    shutdown_client_write(&mut stream).await;
    CapturedEnvelope { wire, envelope }
}

async fn serve_captured_responses(
    listener: std::os::unix::net::UnixListener,
    responses: Vec<ControllerResponse>,
) -> Vec<CapturedEnvelope> {
    let listener = UnixListener::from_std(listener).unwrap();
    let mut envelopes = Vec::with_capacity(responses.len());
    for response in responses {
        envelopes.push(receive_captured_envelope(&listener, &response).await);
    }
    envelopes
}

async fn serve_captured_responses_until_done(
    listener: std::os::unix::net::UnixListener,
    responses: Vec<ControllerResponse>,
    mut done: oneshot::Receiver<()>,
) -> Vec<CapturedEnvelope> {
    let listener = UnixListener::from_std(listener).unwrap();
    let mut envelopes = Vec::with_capacity(responses.len());
    for response in responses {
        envelopes.push(receive_captured_envelope(&listener, &response).await);
    }
    loop {
        tokio::select! {
            biased;
            connection = listener.accept() => {
                let (mut stream, _) = connection.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut byte = [0_u8; 1];
                    let read = stream.read(&mut byte).await.unwrap();
                    if read == 0 || byte[0] == b'\n' {
                        break;
                    }
                    request.push(byte[0]);
                }
                assert!(!request.is_empty());
                let wire: Value = serde_json::from_slice(&request).unwrap();
                let envelope = serde_json::from_value(wire.clone()).unwrap();
                envelopes.push(CapturedEnvelope { wire, envelope });
                let mut bytes = serde_json::to_vec(&rejected(RejectResponseReason::Invalid)).unwrap();
                bytes.push(b'\n');
                stream.write_all(&bytes).await.unwrap();
                shutdown_client_write(&mut stream).await;
            }
            _ = &mut done => break,
        }
    }
    envelopes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accept_and_apply_dispatch_edge() {
    let running = RunningController::start().await;
    assert_eq!(
        running.send(&dispatch("event-1", "child", "parent")).await,
        ControllerResponse::Accepted
    );
    let model = running.collector.model.borrow();
    let child = model
        .task_run_by_key(&RunKey::Controller("child".to_owned()))
        .unwrap();
    let parent = model
        .task_run_by_key(&RunKey::Controller("parent".to_owned()))
        .unwrap();
    assert!(
        model.execution_edges().any(|edge| {
            edge.child_run_id == child.run_id && edge.parent_run_id == parent.run_id
        })
    );
    drop(model);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn depends_on_creates_dependency_edge() {
    let running = RunningController::start().await;
    assert_eq!(
        running
            .send(&depends_on("event-1", "child", "prerequisite"))
            .await,
        ControllerResponse::Accepted
    );
    let model = running.collector.model.borrow();
    let child = model
        .task_run_by_key(&RunKey::Controller("child".to_owned()))
        .unwrap();
    let prerequisite = model
        .task_run_by_key(&RunKey::Controller("prerequisite".to_owned()))
        .unwrap();
    assert!(model.dependency_edges().any(|edge| {
        edge.dependent_run_id == child.run_id && edge.prerequisite_run_id == prerequisite.run_id
    }));
    drop(model);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_event_id_is_duplicate() {
    let running = RunningController::start().await;
    let event = envelope("same-event", "task_started", "run");
    assert_eq!(running.send(&event).await, ControllerResponse::Accepted);
    assert_eq!(running.send(&event).await, ControllerResponse::Duplicate);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_first_reserved_provider_id_is_invalid_and_never_reserves_ledger() {
    let running = RunningController::start().await;
    let event_id = "prov:codex:act:controller-first";
    assert_eq!(
        running
            .send(&envelope(event_id, "task_started", "run"))
            .await,
        rejected(RejectResponseReason::Invalid)
    );
    let (_state_guard, mut reopened) = running.stop_and_reopen().await;
    assert!(
        reopened
            .load_restored_state()
            .unwrap()
            .event_ledger
            .iter()
            .all(|entry| entry.event_id != event_id)
    );
    reopened
        .apply_batch(vec![provider_record(event_id, "provider-wins")])
        .unwrap();
    assert!(
        reopened
            .load_restored_state()
            .unwrap()
            .event_ledger
            .iter()
            .any(|entry| entry.event_id == event_id)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_first_reserved_provider_id_keeps_duplicate_precedence() {
    let event_id = "prov:codex:act:provider-first";
    let running = RunningController::start_seeded(vec![provider_record(event_id, "first")]).await;

    assert_eq!(
        running
            .send(&envelope(event_id, "task_started", "run"))
            .await,
        ControllerResponse::Duplicate
    );
    assert!(
        running
            .collector
            .model
            .borrow()
            .task_run_by_key(&RunKey::Controller("run".to_owned()))
            .is_none()
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_schema_version_rejected() {
    let running = RunningController::start().await;
    let mut event = envelope("event-1", "task_started", "run");
    event["schema_version"] = json!(2);
    assert_eq!(
        running.send(&event).await,
        ControllerResponse::Rejected {
            reason: RejectResponseReason::UnsupportedVersion
        }
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn version_zero_or_absent_invalid() {
    let running = RunningController::start().await;
    let mut zero = envelope("zero", "task_started", "run");
    zero["schema_version"] = json!(0);
    assert_eq!(
        running.send(&zero).await,
        ControllerResponse::Rejected {
            reason: RejectResponseReason::Invalid
        }
    );
    let mut absent = envelope("absent", "task_started", "run");
    absent.as_object_mut().unwrap().remove("schema_version");
    assert_eq!(
        running.send(&absent).await,
        ControllerResponse::Rejected {
            reason: RejectResponseReason::Invalid
        }
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_field_tolerated_missing_required_invalid() {
    let running = RunningController::start().await;
    let mut accepted = envelope("known", "task_started", "run");
    accepted["future_extension"] = json!({"anything": true});
    assert_eq!(running.send(&accepted).await, ControllerResponse::Accepted);
    let mut missing = envelope("missing", "task_started", "run-2");
    missing.as_object_mut().unwrap().remove("source");
    assert_eq!(
        running.send(&missing).await,
        ControllerResponse::Rejected {
            reason: RejectResponseReason::Invalid
        }
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forward_reference_creates_placeholder() {
    let running = RunningController::start().await;
    assert_eq!(
        running.send(&dispatch("event-1", "child", "parent")).await,
        ControllerResponse::Accepted
    );
    let model = running.collector.model.borrow();
    assert_eq!(
        model
            .task_run_by_key(&RunKey::Controller("child".to_owned()))
            .unwrap()
            .state,
        TaskState::Queued
    );
    assert_eq!(
        model
            .task_run_by_key(&RunKey::Controller("parent".to_owned()))
            .unwrap()
            .state,
        TaskState::Queued
    );
    drop(model);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_forward_reference_flagged() {
    let running = RunningController::start().await;
    assert_eq!(
        running
            .send(&envelope("event-1", "complete", "unknown"))
            .await,
        ControllerResponse::Accepted
    );
    let model = running.collector.model.borrow();
    assert_eq!(
        model
            .controller_diagnostics()
            .terminal_forward_reference_creations(),
        1
    );
    assert_eq!(
        model
            .task_run_by_key(&RunKey::Controller("unknown".to_owned()))
            .unwrap()
            .state,
        TaskState::Completed
    );
    drop(model);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_started_stale_on_terminal() {
    let running = RunningController::start().await;
    assert_eq!(
        running.send(&envelope("event-1", "complete", "run")).await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running
            .send(&envelope("event-2", "task_started", "run"))
            .await,
        rejected(RejectResponseReason::StaleEvent)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_progress_on_terminal_are_diagnostic_noops() {
    let running = RunningController::start().await;
    assert_eq!(
        running.send(&envelope("event-1", "complete", "run")).await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running.send(&envelope("event-2", "blocked", "run")).await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running.send(&envelope("event-3", "progress", "run")).await,
        ControllerResponse::Accepted
    );
    let model = running.collector.model.borrow();
    assert_eq!(
        model
            .controller_diagnostics()
            .terminal_blocked_progress_noops(),
        2
    );
    assert_eq!(
        model
            .task_run_by_key(&RunKey::Controller("run".to_owned()))
            .unwrap()
            .state,
        TaskState::Completed
    );
    drop(model);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_terminal_is_noop_differing_is_conflict() {
    let running = RunningController::start().await;
    assert_eq!(
        running.send(&envelope("event-1", "complete", "run")).await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running.send(&envelope("event-2", "complete", "run")).await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running.send(&envelope("event-3", "failed", "run")).await,
        rejected(RejectResponseReason::Conflict)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn depends_on_terminal_subject_and_resolved_restatement() {
    let running = RunningController::start().await;
    assert_eq!(
        running
            .send(&depends_on("event-1", "subject", "prerequisite"))
            .await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running
            .send(&envelope("event-2", "complete", "subject"))
            .await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running
            .send(&depends_on("event-3", "subject", "prerequisite"))
            .await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running
            .send(&depends_on("event-4", "subject", "other"))
            .await,
        rejected(RejectResponseReason::StaleEvent)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_self_cycle() {
    let running = RunningController::start().await;
    assert_eq!(
        running.send(&dispatch("event-1", "same", "same")).await,
        rejected(RejectResponseReason::Cycle)
    );
    assert!(
        running
            .collector
            .model
            .borrow()
            .task_runs()
            .next()
            .is_none()
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_multihop_cycle() {
    let running = RunningController::start().await;
    assert_eq!(
        running.send(&dispatch("event-1", "a", "b")).await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running.send(&dispatch("event-2", "b", "c")).await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running.send(&dispatch("event-3", "c", "a")).await,
        rejected(RejectResponseReason::Cycle)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dependency_cycle() {
    let running = RunningController::start().await;
    assert_eq!(
        running.send(&depends_on("event-1", "a", "b")).await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running.send(&depends_on("event-2", "b", "c")).await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running.send(&depends_on("event-3", "c", "a")).await,
        rejected(RejectResponseReason::Cycle)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differing_dispatch_parent_conflict() {
    let running = RunningController::start().await;
    assert_eq!(
        running
            .send(&dispatch("event-1", "child", "parent-a"))
            .await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        running
            .send(&dispatch("event-2", "child", "parent-b"))
            .await,
        rejected(RejectResponseReason::Conflict)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_event_type_rejected_invalid() {
    let running = RunningController::start().await;
    assert_eq!(
        running
            .send(&envelope("event-1", "future_event", "run"))
            .await,
        rejected(RejectResponseReason::Invalid)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn endpoint_required_forbidden_matrix_eight_events() {
    let running = RunningController::start().await;
    for (index, event_type) in [
        "task_started",
        "blocked",
        "progress",
        "complete",
        "failed",
        "cancelled",
    ]
    .into_iter()
    .enumerate()
    {
        let mut event = envelope(&format!("forbidden-{index}"), event_type, "run");
        event["parent_task_run_id"] = json!("forbidden");
        assert_eq!(
            running.send(&event).await,
            rejected(RejectResponseReason::Invalid)
        );
    }
    assert_eq!(
        running
            .send(&envelope("dispatch-missing", "dispatch", "run"))
            .await,
        rejected(RejectResponseReason::Invalid)
    );
    assert_eq!(
        running
            .send(&envelope("depends-missing", "depends_on", "run"))
            .await,
        rejected(RejectResponseReason::Invalid)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_session_without_provider_rejected_invalid() {
    let running = RunningController::start().await;
    let mut event = envelope("event-1", "task_started", "run");
    event["native_session_id"] = json!("native");
    assert_eq!(
        running.send(&event).await,
        rejected(RejectResponseReason::Invalid)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn progress_out_of_range_rejected() {
    let running = RunningController::start().await;
    for (index, progress) in [-0.0001, 1.0001].into_iter().enumerate() {
        let mut event = envelope(&format!("event-{index}"), "progress", "run");
        event["progress"] = json!(progress);
        assert_eq!(
            running.send(&event).await,
            rejected(RejectResponseReason::Invalid)
        );
    }
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retryable_on_unhealthy_writer() {
    let running = RunningController::start_with_persistence_failure().await;
    running.induce_persistence_failure().await;
    let before_retryable = running.collector.model.borrow().task_runs().count();
    assert_eq!(
        running
            .send(&envelope("event-1", "task_started", "run"))
            .await,
        ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable
        }
    );
    let model = running.collector.model.borrow();
    assert_eq!(model.task_runs().count(), before_retryable);
    assert!(
        model
            .task_run_by_key(&RunKey::Controller("run".to_owned()))
            .is_none()
    );
    drop(model);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflict_vs_unhealthy_precedence() {
    let running = RunningController::start_with_persistence_failure().await;
    running.induce_persistence_failure().await;
    assert_eq!(
        running.send(&dispatch("event-1", "same", "same")).await,
        rejected(RejectResponseReason::Cycle)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn response_precedence_full() {
    let running = RunningController::start_with_persistence_failure().await;
    let accepted = envelope("duplicate", "task_started", "run");
    assert_eq!(running.send(&accepted).await, ControllerResponse::Accepted);
    let mut invalid_duplicate = dispatch("duplicate", "same", "same");
    invalid_duplicate["schema_version"] = json!(99);
    assert_eq!(
        running.send(&invalid_duplicate).await,
        ControllerResponse::Duplicate
    );
    running.induce_persistence_failure().await;
    assert_eq!(
        running.send(&dispatch("cycle", "same", "same")).await,
        rejected(RejectResponseReason::Cycle)
    );
    assert_eq!(
        running
            .send(&envelope("retry", "task_started", "other"))
            .await,
        ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable
        }
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_roundtrip_accepted() {
    let running = RendezvousController::start().await;
    let runtime_path = running.runtime_base.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || emit_command(&runtime_path, false, "emit-1"))
        .await
        .unwrap();
    assert!(output.status.success(), "{}", output_text(&output));
    assert_eq!(
        serde_json::from_slice::<ControllerResponse>(&output.stdout).unwrap(),
        ControllerResponse::Accepted
    );
    assert!(
        running
            .collector
            .model
            .borrow()
            .task_run_by_key(&RunKey::Controller("run".to_owned()))
            .is_some()
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_from_hook_delivers_subagent_dispatch_then_started_without_stdout() {
    let (runtime_base, _runtime, listener) = scripted_emit_listener();
    let server = tokio::spawn(serve_captured_responses(
        listener,
        vec![ControllerResponse::Accepted, ControllerResponse::Accepted],
    ));
    let runtime_path = runtime_base.path().to_path_buf();
    let payload = serde_json::to_vec(&json!({
        "hook_event_name": "SubagentStart",
        "session_id": "session-123",
        "agent_id": "agent-7",
        "agent_type": "researcher"
    }))
    .unwrap();
    let output = tokio::task::spawn_blocking(move || {
        hook_emit_command(&runtime_path, false, "claude-code", payload)
    })
    .await
    .unwrap();

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(
        output.stdout.is_empty(),
        "adapter stdout must be zero bytes"
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stderr)
            .matches("\"status\":\"accepted\"")
            .count(),
        2
    );

    let envelopes = server.await.unwrap();
    assert_eq!(envelopes.len(), 2);
    let dispatch = &envelopes[0].envelope;
    let started = &envelopes[1].envelope;
    let dispatch_wire = &envelopes[0].wire;
    let started_wire = &envelopes[1].wire;
    let session_run_id = "hook:claude-code:session-123";
    let agent_run_id = "hook:claude-code:session-123:agent:agent-7";

    assert_eq!(dispatch.schema_version, 1);
    assert_eq!(dispatch.source, "hook:claude-code");
    assert_eq!(dispatch.event_type, "dispatch");
    assert_eq!(dispatch.task_run_id, agent_run_id);
    assert_eq!(dispatch.parent_task_run_id.as_deref(), Some(session_run_id));
    assert_eq!(dispatch.depends_on_id, None);
    assert_eq!(dispatch.label, None);
    assert_eq!(dispatch.reason, None);
    assert_eq!(dispatch.progress, None);
    assert_eq!(dispatch.provider.as_deref(), Some("claude"));
    assert_eq!(dispatch.native_session_id, None);
    assert_eq!(dispatch.terminal_id, None);
    assert_eq!(dispatch_wire.as_object().unwrap().len(), 14);
    assert_eq!(dispatch_wire["depends_on_id"], Value::Null);
    assert_eq!(dispatch_wire["label"], Value::Null);
    assert_eq!(dispatch_wire["reason"], Value::Null);
    assert_eq!(dispatch_wire["progress"], Value::Null);
    assert_eq!(dispatch_wire["native_session_id"], Value::Null);
    assert_eq!(dispatch_wire["terminal_id"], Value::Null);

    assert_eq!(started.schema_version, 1);
    assert_eq!(started.source, "hook:claude-code");
    assert_eq!(started.event_type, "task_started");
    assert_eq!(started.task_run_id, agent_run_id);
    assert_eq!(started.parent_task_run_id, None);
    assert_eq!(started.depends_on_id, None);
    assert_eq!(started.label.as_deref(), Some("researcher"));
    assert_eq!(started.reason, None);
    assert_eq!(started.progress, None);
    assert_eq!(started.provider.as_deref(), Some("claude"));
    assert_eq!(started.native_session_id, None);
    assert_eq!(started.terminal_id, None);
    assert_eq!(started_wire.as_object().unwrap().len(), 14);
    assert_eq!(started_wire["parent_task_run_id"], Value::Null);
    assert_eq!(started_wire["depends_on_id"], Value::Null);
    assert_eq!(started_wire["reason"], Value::Null);
    assert_eq!(started_wire["progress"], Value::Null);
    assert_eq!(started_wire["native_session_id"], Value::Null);
    assert_eq!(started_wire["terminal_id"], Value::Null);

    let dispatch_suffix = dispatch
        .event_id
        .strip_prefix("hook:claude-code:session-123:SubagentStart:agent-7:dispatch:")
        .unwrap();
    let started_suffix = started
        .event_id
        .strip_prefix("hook:claude-code:session-123:SubagentStart:agent-7:started:")
        .unwrap();
    assert_eq!(dispatch_suffix, started_suffix);
    let (millis, nonce) = dispatch_suffix.split_once(':').unwrap();
    assert_eq!(millis.parse::<i64>().unwrap(), dispatch.emitted_at_ms);
    assert_eq!(dispatch.emitted_at_ms, started.emitted_at_ms);
    assert_eq!(nonce.len(), 16);
    assert!(nonce.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_ne!(dispatch.event_id, started.event_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_from_hook_stops_after_invalid_rejection_and_strict_fails() {
    let payload = serde_json::to_vec(&json!({
        "hook_event_name": "SubagentStart",
        "session_id": "session-123",
        "agent_id": "agent-7",
        "agent_type": "researcher"
    }))
    .unwrap();

    for strict in [false, true] {
        let (runtime_base, _runtime, listener) = scripted_emit_listener();
        let (done_sender, done_receiver) = oneshot::channel();
        let server = tokio::spawn(serve_captured_responses_until_done(
            listener,
            vec![rejected(RejectResponseReason::Invalid)],
            done_receiver,
        ));
        let runtime_path = runtime_base.path().to_path_buf();
        let payload = payload.clone();
        let output = tokio::task::spawn_blocking(move || {
            hook_emit_command(&runtime_path, strict, "claude-code", payload)
        })
        .await
        .unwrap();
        done_sender.send(()).unwrap();

        assert_eq!(output.status.success(), !strict, "{}", output_text(&output));
        assert!(
            output.stdout.is_empty(),
            "adapter stdout must be zero bytes"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("\"status\":\"rejected\""), "{stderr}");
        assert!(stderr.contains("\"reason\":\"invalid\""), "{stderr}");
        assert!(stderr.contains("skipped 1"), "{stderr}");

        let envelopes = server.await.unwrap();
        assert_eq!(envelopes.len(), 1, "task_started must never be sent");
        assert_eq!(envelopes[0].envelope.event_type, "dispatch");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_from_hook_continues_after_stale_event_with_fresh_invocation_metadata() {
    let payload = serde_json::to_vec(&json!({
        "hook_event_name": "SubagentStart",
        "session_id": "stale-first-session",
        "agent_id": "agent-7",
        "agent_type": "researcher"
    }))
    .unwrap();
    let mut invocation_nonces = Vec::new();

    for _ in 0..2 {
        let (runtime_base, _runtime, listener) = scripted_emit_listener();
        let server = tokio::spawn(serve_captured_responses(
            listener,
            vec![
                rejected(RejectResponseReason::StaleEvent),
                ControllerResponse::Accepted,
            ],
        ));
        let runtime_path = runtime_base.path().to_path_buf();
        let payload = payload.clone();
        let before_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let output = tokio::task::spawn_blocking(move || {
            hook_emit_command(&runtime_path, true, "claude-code", payload)
        })
        .await
        .unwrap();
        let after_ms = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();

        assert!(output.status.success(), "{}", output_text(&output));
        assert!(
            output.stdout.is_empty(),
            "adapter stdout must be zero bytes"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        let benign_position = stderr
            .find("benign stale_event; continuing")
            .unwrap_or_else(|| panic!("missing benign stale_event diagnostic: {stderr}"));
        let accepted_position = stderr
            .find("\"status\":\"accepted\"")
            .unwrap_or_else(|| panic!("missing accepted diagnostic: {stderr}"));
        assert!(
            benign_position < accepted_position,
            "accepted diagnostic must follow the benign stale_event note: {stderr}"
        );
        assert!(!stderr.contains("skipped"), "{stderr}");

        let envelopes = server.await.unwrap();
        assert_eq!(envelopes.len(), 2);
        let dispatch = &envelopes[0].envelope;
        let started = &envelopes[1].envelope;
        let session_run_id = "hook:claude-code:stale-first-session";
        let agent_run_id = "hook:claude-code:stale-first-session:agent:agent-7";

        assert_eq!(dispatch.event_type, "dispatch");
        assert_eq!(dispatch.task_run_id, agent_run_id);
        assert_eq!(dispatch.parent_task_run_id.as_deref(), Some(session_run_id));
        assert_eq!(started.event_type, "task_started");
        assert_eq!(started.task_run_id, dispatch.task_run_id);
        assert_eq!(started.parent_task_run_id, None);

        for captured in &envelopes {
            assert!(
                (before_ms..=after_ms).contains(&captured.envelope.emitted_at_ms),
                "emitted_at_ms {} is outside invocation bounds {before_ms}..={after_ms}",
                captured.envelope.emitted_at_ms
            );
        }
        assert_eq!(dispatch.emitted_at_ms, started.emitted_at_ms);

        let dispatch_suffix = dispatch
            .event_id
            .strip_prefix("hook:claude-code:stale-first-session:SubagentStart:agent-7:dispatch:")
            .unwrap();
        let started_suffix = started
            .event_id
            .strip_prefix("hook:claude-code:stale-first-session:SubagentStart:agent-7:started:")
            .unwrap();
        assert_eq!(dispatch_suffix, started_suffix);
        let (emitted_at_ms, nonce) = dispatch_suffix.split_once(':').unwrap();
        assert_eq!(
            emitted_at_ms.parse::<i64>().unwrap(),
            dispatch.emitted_at_ms
        );
        assert_eq!(nonce.len(), 16);
        assert!(nonce.bytes().all(|byte| byte.is_ascii_hexdigit()));
        invocation_nonces.push(nonce.to_owned());
    }

    assert_ne!(invocation_nonces[0], invocation_nonces[1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_from_hook_oversized_identifier_is_ignored_without_delivery() {
    let (runtime_base, _runtime, listener) = scripted_emit_listener();
    let (done_sender, done_receiver) = oneshot::channel();
    let server = tokio::spawn(serve_captured_responses_until_done(
        listener,
        Vec::new(),
        done_receiver,
    ));
    let runtime_path = runtime_base.path().to_path_buf();
    let payload = serde_json::to_vec(&json!({
        "hook_event_name": "SessionStart",
        "session_id": "a".repeat(129)
    }))
    .unwrap();
    let output = tokio::task::spawn_blocking(move || {
        hook_emit_command(&runtime_path, false, "claude-code", payload)
    })
    .await
    .unwrap();
    done_sender.send(()).unwrap();

    assert!(output.status.success(), "{}", output_text(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("exceeding the 128-byte cap"), "{stderr}");

    let envelopes = server.await.unwrap();
    assert!(
        envelopes.is_empty(),
        "oversized identifier must not be sent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_from_hook_malformed_or_oversized_input_is_ignored_without_delivery() {
    let cases = [
        ("non-json", b"not-json".to_vec()),
        (
            "missing-session-id",
            serde_json::to_vec(&json!({"hook_event_name": "SessionStart"})).unwrap(),
        ),
        ("oversized", vec![b'x'; 1_048_577]),
    ];

    for (case, payload) in cases {
        let (runtime_base, _runtime, listener) = scripted_emit_listener();
        let runtime_path = runtime_base.path().to_path_buf();
        let output = tokio::task::spawn_blocking(move || {
            hook_emit_command(&runtime_path, true, "claude-code", payload)
        })
        .await
        .unwrap();

        assert!(output.status.success(), "{case}: {}", output_text(&output));
        assert!(
            output.stdout.is_empty(),
            "{case}: adapter stdout must be zero bytes"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("herdr-top emit:"), "{case}: {stderr}");
        assert!(stderr.contains("hook"), "{case}: {stderr}");
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "{case}: malformed input must not connect to the Controller"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_from_hook_treats_terminal_before_start_stale_event_as_benign() {
    let running = RendezvousController::start().await;
    let stop_payload = serde_json::to_vec(&json!({
        "hook_event_name": "SubagentStop",
        "session_id": "race-session",
        "agent_id": "agent-7"
    }))
    .unwrap();
    let runtime_path = running.runtime_base.path().to_path_buf();
    let stop_output = tokio::task::spawn_blocking(move || {
        hook_emit_command(&runtime_path, true, "claude-code", stop_payload)
    })
    .await
    .unwrap();
    assert!(
        stop_output.status.success(),
        "{}",
        output_text(&stop_output)
    );
    assert!(stop_output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&stop_output.stderr).contains("\"status\":\"accepted\""));

    let agent_run_id = "hook:claude-code:race-session:agent:agent-7";
    assert_eq!(
        running
            .collector
            .model
            .borrow()
            .task_run_by_key(&RunKey::Controller(agent_run_id.to_owned()))
            .unwrap()
            .state,
        TaskState::Completed
    );

    let start_payload = serde_json::to_vec(&json!({
        "hook_event_name": "SubagentStart",
        "session_id": "race-session",
        "agent_id": "agent-7",
        "agent_type": "researcher"
    }))
    .unwrap();
    for strict in [false, true] {
        let runtime_path = running.runtime_base.path().to_path_buf();
        let payload = start_payload.clone();
        let output = tokio::task::spawn_blocking(move || {
            hook_emit_command(&runtime_path, strict, "claude-code", payload)
        })
        .await
        .unwrap();

        assert!(output.status.success(), "{}", output_text(&output));
        assert!(
            output.stdout.is_empty(),
            "adapter stdout must be zero bytes"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(stderr.matches("\"status\":\"accepted\"").count(), 1);
        assert_eq!(
            stderr
                .matches("\"status\":\"rejected\",\"reason\":\"stale_event\"")
                .count(),
            1,
            "{stderr}"
        );
        assert!(!stderr.contains("skipped"), "{stderr}");
    }

    let model = running.collector.model.borrow();
    let agent = model
        .task_run_by_key(&RunKey::Controller(agent_run_id.to_owned()))
        .unwrap();
    let session = model
        .task_run_by_key(&RunKey::Controller(
            "hook:claude-code:race-session".to_owned(),
        ))
        .unwrap();
    assert_eq!(agent.state, TaskState::Completed);
    assert!(
        model.execution_edges().any(|edge| {
            edge.child_run_id == agent.run_id && edge.parent_run_id == session.run_id
        })
    );
    drop(model);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_sentinel_mismatch_refuses() {
    let runtime_base = tempfile::tempdir().unwrap();
    fs::set_permissions(runtime_base.path(), Permissions::from_mode(0o700)).unwrap();
    let runtime = open_runtime_dir_at(runtime_base.path()).unwrap();
    let key = session_key::encode(SESSION).unwrap();
    let runtime_child = runtime_base.path().join("herdr-top");
    let sentinel = runtime_child.join(format!("{}.name", key.hash16()));
    fs::write(&sentinel, b"different-session").unwrap();
    fs::set_permissions(&sentinel, Permissions::from_mode(0o600)).unwrap();
    let socket = runtime_child.join(format!("{}.sock", key.hash16()));
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();

    let runtime_path = runtime_base.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || emit_command(&runtime_path, false, "emit-1"))
        .await
        .unwrap();

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(output_text(&output).contains("SentinelMismatch"));
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    assert_eq!(fs::read(&sentinel).unwrap(), b"different-session");
    drop(runtime);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_unresolved_best_effort_exit0() {
    let runtime_base = tempfile::tempdir().unwrap();
    fs::set_permissions(runtime_base.path(), Permissions::from_mode(0o700)).unwrap();
    let _runtime = open_runtime_dir_at(runtime_base.path()).unwrap();
    let key = session_key::encode(SESSION).unwrap();
    let sentinel = runtime_base
        .path()
        .join("herdr-top")
        .join(format!("{}.name", key.hash16()));
    fs::write(&sentinel, SESSION.as_bytes()).unwrap();
    fs::set_permissions(&sentinel, Permissions::from_mode(0o600)).unwrap();

    let runtime_path = runtime_base.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || emit_command(&runtime_path, false, "emit-1"))
        .await
        .unwrap();

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(output_text(&output).contains("unresolved"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn emit_strict_nonzero_on_rejected() {
    let runtime_base = tempfile::tempdir().unwrap();
    fs::set_permissions(runtime_base.path(), Permissions::from_mode(0o700)).unwrap();
    let _runtime = open_runtime_dir_at(runtime_base.path()).unwrap();
    let key = session_key::encode(SESSION).unwrap();
    let runtime_child = runtime_base.path().join("herdr-top");
    let sentinel = runtime_child.join(format!("{}.name", key.hash16()));
    fs::write(&sentinel, SESSION.as_bytes()).unwrap();
    fs::set_permissions(&sentinel, Permissions::from_mode(0o600)).unwrap();
    let socket = runtime_child.join(format!("{}.sock", key.hash16()));
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(serve_responses(
        listener,
        vec![rejected(RejectResponseReason::Invalid)],
    ));

    let runtime_path = runtime_base.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || emit_command(&runtime_path, true, "emit-1"))
        .await
        .unwrap();

    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<ControllerResponse>(&output.stdout).unwrap(),
        rejected(RejectResponseReason::Invalid)
    );
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_and_strict_response_matrix() {
    let runtime_base = tempfile::tempdir().unwrap();
    fs::set_permissions(runtime_base.path(), Permissions::from_mode(0o700)).unwrap();
    let _runtime = open_runtime_dir_at(runtime_base.path()).unwrap();
    let key = session_key::encode(SESSION).unwrap();
    let runtime_child = runtime_base.path().join("herdr-top");
    let sentinel = runtime_child.join(format!("{}.name", key.hash16()));
    fs::write(&sentinel, SESSION.as_bytes()).unwrap();
    fs::set_permissions(&sentinel, Permissions::from_mode(0o600)).unwrap();
    let socket = runtime_child.join(format!("{}.sock", key.hash16()));
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let statuses = vec![
        ControllerResponse::Accepted,
        ControllerResponse::Duplicate,
        rejected(RejectResponseReason::Invalid),
        ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable,
        },
    ];
    let responses: Vec<_> = statuses
        .iter()
        .flat_map(|status| [status.clone(), status.clone()])
        .collect();
    let server = tokio::spawn(serve_responses(listener, responses));

    for (index, status) in statuses.into_iter().enumerate() {
        let strict_success = matches!(
            &status,
            ControllerResponse::Accepted | ControllerResponse::Duplicate
        );
        for strict in [false, true] {
            let runtime_path = runtime_base.path().to_path_buf();
            let event_id = format!("matrix-{index}-{strict}");
            let output =
                tokio::task::spawn_blocking(move || emit_command(&runtime_path, strict, &event_id))
                    .await
                    .unwrap();
            assert_eq!(
                output.status.success(),
                !strict || strict_success,
                "{status:?}: {}",
                output_text(&output)
            );
        }
    }
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_discard_for_every_reject_and_retryable() {
    let running = RunningController::start_with_persistence_failure().await;
    assert_eq!(
        running.send(&dispatch("cycle", "unknown", "unknown")).await,
        rejected(RejectResponseReason::Cycle)
    );
    assert!(
        running
            .collector
            .model
            .borrow()
            .task_runs()
            .next()
            .is_none()
    );
    running.induce_persistence_failure().await;
    let before_retryable = running.collector.model.borrow().task_runs().count();
    assert_eq!(
        running
            .send(&envelope("retry", "task_started", "still-unknown"))
            .await,
        ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable
        }
    );
    let model = running.collector.model.borrow();
    assert_eq!(model.task_runs().count(), before_retryable);
    assert!(
        model
            .task_run_by_key(&RunKey::Controller("still-unknown".to_owned()))
            .is_none()
    );
    drop(model);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alias_resolves_subject_parent_prerequisite_no_resurrection() {
    let state = tempfile::tempdir().unwrap();
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("controller.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let key = session_key::encode(SESSION).unwrap();
    let root = state_root_in(state.path(), &key).unwrap();
    let mut store = store::open_writer(&root).unwrap();
    let canonical = [
        (RunId::new(), "subject"),
        (RunId::new(), "parent"),
        (RunId::new(), "prerequisite"),
    ];
    let mut seed = Vec::new();
    for (index, (run_id, key)) in canonical.iter().enumerate() {
        seed.push(PersistOp::UpsertTaskRun(PersistTaskRun {
            task_run: TaskRun {
                run_id: *run_id,
                key: RunKey::Controller((*key).to_owned()),
                display_ordinal: DisplayOrdinal::new(index as i64 + 1),
                state: TaskState::Queued,
                has_controller_task_state_event: false,
                created_at_ms: None,
                updated_at_ms: None,
                finished_at_ms: None,
                subject: None,
                dismissed_at_ms: None,
            },
            native_session: None,
            created_at_ms: 1,
            updated_at_ms: 1,
            finished_at_ms: None,
        }));
    }
    store.apply_batch(seed).unwrap();
    let mut restored = store.load_restored_state().unwrap();
    for ((run_id, _), alias) in
        canonical
            .iter()
            .zip(["subject-alias", "parent-alias", "prerequisite-alias"])
    {
        restored
            .model
            .insert_task_run_alias(RunKey::Controller(alias.to_owned()), *run_id);
    }
    let (lifecycle, writer) = store::spawn_writer(store).unwrap();
    let collector = collector::spawn_with_controller(
        socket_dir.path().join("missing-herdr.sock"),
        SESSION.to_owned(),
        restored,
        writer,
        Some(listener),
    )
    .await
    .unwrap();

    assert_eq!(
        send_raw(
            &socket_path,
            &envelope("started", "task_started", "subject-alias")
        )
        .await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        send_raw(
            &socket_path,
            &dispatch("dispatch", "subject-alias", "parent-alias")
        )
        .await,
        ControllerResponse::Accepted
    );
    assert_eq!(
        send_raw(
            &socket_path,
            &depends_on("depends", "subject-alias", "prerequisite-alias")
        )
        .await,
        ControllerResponse::Accepted
    );
    let model = collector.model.borrow();
    assert_eq!(model.task_runs().count(), 3);
    assert_eq!(
        model
            .task_run_by_key(&RunKey::Controller("subject-alias".to_owned()))
            .unwrap()
            .run_id,
        canonical[0].0
    );
    assert!(model.execution_edges().any(|edge| {
        edge.child_run_id == canonical[0].0 && edge.parent_run_id == canonical[1].0
    }));
    assert!(model.dependency_edges().any(|edge| {
        edge.dependent_run_id == canonical[0].0 && edge.prerequisite_run_id == canonical[2].0
    }));
    drop(model);
    collector.stop().await.unwrap();
    lifecycle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binding_conflict_event_acks_rejected_conflict_collector_stays_live() {
    let running = RunningController::start().await;
    let mut first = envelope("first", "task_started", "run-a");
    first["provider"] = json!("codex");
    first["native_session_id"] = json!("shared-native");
    assert_eq!(running.send(&first).await, ControllerResponse::Accepted);
    let mut conflict = envelope("conflict", "task_started", "run-b");
    conflict["provider"] = json!("codex");
    conflict["native_session_id"] = json!("shared-native");
    assert_eq!(
        running.send(&conflict).await,
        rejected(RejectResponseReason::Conflict)
    );
    assert_eq!(
        running
            .send(&envelope("after", "task_started", "run-c"))
            .await,
        ControllerResponse::Accepted
    );
    assert!(
        running
            .collector
            .model
            .borrow()
            .task_run_by_key(&RunKey::Controller("run-c".to_owned()))
            .is_some()
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monotonic_ingest_under_concurrent_connections() {
    let running = RunningController::start().await;
    let mut requests = Vec::new();
    for index in 0..24 {
        let path = running.socket_path.clone();
        requests.push(tokio::spawn(async move {
            send_raw(
                &path,
                &envelope(
                    &format!("event-{index}"),
                    "task_started",
                    &format!("run-{index}"),
                ),
            )
            .await
        }));
    }
    for request in requests {
        assert_eq!(request.await.unwrap(), ControllerResponse::Accepted);
    }
    wait_for_durable_row_count(
        &running,
        "SELECT COUNT(*) FROM events WHERE normalized_kind = 'controller_event'",
        24,
    )
    .await;
    let connection = rusqlite::Connection::open(store::database_path(&running.root)).unwrap();
    let mut statement = connection
        .prepare("SELECT ingest_seq FROM events WHERE normalized_kind = 'controller_event' ORDER BY ingest_seq")
        .unwrap();
    let sequences: Vec<i64> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(sequences, (1_i64..=24).collect::<Vec<_>>());
    drop(statement);
    drop(connection);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn envelope_timestamp_is_not_ordering() {
    let running = RunningController::start().await;
    let mut future = envelope("future", "task_started", "run-future");
    future["emitted_at_ms"] = json!(i64::MAX);
    let mut past = envelope("past", "task_started", "run-past");
    past["emitted_at_ms"] = json!(i64::MIN);
    assert_eq!(running.send(&future).await, ControllerResponse::Accepted);
    assert_eq!(running.send(&past).await, ControllerResponse::Accepted);
    wait_for_durable_row_count(
        &running,
        "SELECT COUNT(*) FROM events WHERE event_id IN ('future', 'past')",
        2,
    )
    .await;
    let connection = rusqlite::Connection::open(store::database_path(&running.root)).unwrap();
    let rows: Vec<(String, i64, i64)> = connection
        .prepare("SELECT event_id, event_timestamp_ms, ingest_seq FROM events WHERE event_id IN ('future', 'past') ORDER BY ingest_seq")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("future".to_owned(), i64::MAX, 1),
            ("past".to_owned(), i64::MIN, 2)
        ]
    );
    drop(connection);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_frame_and_frame_limit_and_timeout() {
    let running = RunningController::start().await;
    assert_eq!(
        send_bytes(&running.socket_path, b"not-json\n").await,
        rejected(RejectResponseReason::Invalid)
    );
    let oversized = vec![b'x'; herdr_top::herdr::controller::MAX_FRAME_BYTES + 1];
    let mut oversized_frame = oversized;
    oversized_frame.push(b'\n');
    assert_eq!(
        send_bytes(&running.socket_path, &oversized_frame).await,
        rejected(RejectResponseReason::Invalid)
    );
    let mut timed_out = UnixStream::connect(&running.socket_path).await.unwrap();
    let mut response = Vec::new();
    {
        let mut reader = BufReader::new(&mut timed_out);
        tokio::time::timeout(
            Duration::from_secs(6),
            reader.read_until(b'\n', &mut response),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert_eq!(
        serde_json::from_slice::<ControllerResponse>(&response).unwrap(),
        rejected(RejectResponseReason::Invalid)
    );
    drop(timed_out);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn controller_connection_cap_defers_excess_connections() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let running = RunningController::start().await;
        let mut idle_connections = Vec::new();
        for _ in 0..herdr_top::herdr::controller::MAX_CONTROLLER_CONNECTIONS + 8 {
            idle_connections.push(UnixStream::connect(&running.socket_path).await.unwrap());
        }

        let started = tokio::time::Instant::now();
        let response = running
            .send(&envelope("after-cap", "task_started", "run"))
            .await;

        assert_eq!(response, ControllerResponse::Accepted);
        assert!(started.elapsed() >= Duration::from_secs(3));

        drop(idle_connections);
        running.stop().await;
    })
    .await
    .expect("connection-cap integration test timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_accepted_while_herdr_disconnected() {
    let running = RunningController::start().await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if *running.collector.quality.borrow() == collector::ObservationQuality::Disconnected {
                break;
            }
            let mut quality = running.collector.quality.clone();
            quality.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(
        running
            .send(&envelope("event-1", "task_started", "run"))
            .await,
        ControllerResponse::Accepted
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collector_handle_publishes_coherent_performance_generation() {
    let running = RunningController::start().await;
    let mut performance = running.collector.performance.clone();
    let _ = performance.borrow_and_update();

    assert_eq!(
        running
            .send(&envelope(
                "performance-generation",
                "task_started",
                "performance-run",
            ))
            .await,
        ControllerResponse::Accepted
    );

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let current = performance.borrow();
            if current.snapshot.admission_high_water >= 1
                && current.snapshot.completion_high_water >= 1
                && current.snapshot.pending_events == 0
            {
                break;
            }
            drop(current);
            performance
                .changed()
                .await
                .expect("performance publisher should remain available");
        }
    })
    .await
    .expect("Controller admission must publish a performance generation");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if *running.collector.quality.borrow() == collector::ObservationQuality::Disconnected {
                break;
            }
            let mut quality = running.collector.quality.clone();
            quality.changed().await.unwrap();
        }
    })
    .await
    .unwrap();

    let publication = performance.borrow().clone();
    assert_eq!(publication.snapshot.admission_high_water, 1);
    assert_eq!(publication.snapshot.completion_high_water, 1);
    assert_eq!(publication.snapshot.pending_events, 0);
    assert!(publication.snapshot.reasons.is_empty());
    assert_eq!(
        publication.effective_quality,
        collector::ObservationQuality::Disconnected
    );
    assert!(
        running
            .collector
            .model
            .borrow()
            .task_run_by_key(&RunKey::Controller("performance-run".to_owned()))
            .is_some()
    );

    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acceptor_survives_a_rejected_then_accepts() {
    let running = RunningController::start().await;
    assert_eq!(
        running.send(&dispatch("bad", "same", "same")).await,
        rejected(RejectResponseReason::Cycle)
    );
    assert_eq!(
        running.send(&envelope("good", "task_started", "run")).await,
        ControllerResponse::Accepted
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enqueue_closure_after_health_gate_is_impossible() {
    let running = RunningController::start().await;
    assert_eq!(
        running
            .send(&envelope("event-1", "task_started", "run"))
            .await,
        ControllerResponse::Accepted
    );
    wait_for_durable_row_count(
        &running,
        "SELECT COUNT(*) FROM event_ledger WHERE event_id = 'event-1'",
        1,
    )
    .await;
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receipt_time_persisted_for_task_run_and_both_edge_rows() {
    let running = RunningController::start().await;
    let before = unix_now_ms();
    let mut dispatched = dispatch("dispatch", "child", "parent");
    dispatched["emitted_at_ms"] = json!(-123_456);
    assert_eq!(
        running.send(&dispatched).await,
        ControllerResponse::Accepted
    );
    let mut dependency = depends_on("dependency", "child", "prerequisite");
    dependency["emitted_at_ms"] = json!(-654_321);
    assert_eq!(
        running.send(&dependency).await,
        ControllerResponse::Accepted
    );
    let after = unix_now_ms();
    wait_for_durable_row_count(
        &running,
        "SELECT COUNT(*) FROM events WHERE event_id IN ('dispatch', 'dependency')",
        2,
    )
    .await;
    let connection = rusqlite::Connection::open(store::database_path(&running.root)).unwrap();
    for query in [
        "SELECT created_at_ms FROM task_runs",
        "SELECT created_at_ms FROM execution_edges",
        "SELECT created_at_ms FROM dependency_edges",
        "SELECT seen_at_ms FROM events WHERE event_id IN ('dispatch', 'dependency')",
    ] {
        let mut statement = connection.prepare(query).unwrap();
        let values: Vec<i64> = statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(!values.is_empty());
        assert!(
            values
                .into_iter()
                .all(|value| (before..=after).contains(&value))
        );
    }
    drop(connection);
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reused_event_id_after_seven_idle_days_is_not_suppressed() {
    let running = RunningController::start_seeded(vec![expired_controller_record("reused")]).await;
    let event = envelope("reused", "task_started", "run");
    assert_eq!(running.send(&event).await, ControllerResponse::Duplicate);
    assert_eq!(
        running
            .send(&envelope(
                "cleanup-driver",
                "task_started",
                "cleanup-driver-run",
            ))
            .await,
        ControllerResponse::Accepted
    );
    wait_for_durable_row_count(
        &running,
        "SELECT COUNT(*) FROM event_ledger WHERE event_id = 'reused'",
        0,
    )
    .await;
    assert_eq!(running.send(&event).await, ControllerResponse::Accepted);
    running.stop().await;
}

#[test]
fn startup_failure_after_bind_cleans_up() {
    let state_base = tempfile::tempdir().unwrap();
    let runtime_base = tempfile::tempdir().unwrap();
    fs::set_permissions(runtime_base.path(), Permissions::from_mode(0o700)).unwrap();
    let key = session_key::encode(SESSION).unwrap();
    let root = state_root_in(state_base.path(), &key).unwrap();
    let mut store = store::open_writer(&root).unwrap();
    store.checkpoint().unwrap();
    drop(store);
    let connection = rusqlite::Connection::open(store::database_path(&root)).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_owner BEFORE INSERT ON owner \
             BEGIN SELECT RAISE(FAIL, 'fabricated owner failure'); END;",
        )
        .unwrap();
    drop(connection);

    let output = Command::new(env!("CARGO_BIN_EXE_herdr-top"))
        .args(["--session", SESSION, "--socket", "/missing/herdr.sock"])
        .env("XDG_STATE_HOME", state_base.path())
        .env("XDG_RUNTIME_DIR", runtime_base.path())
        .env_remove("HERDR_SESSION")
        .env_remove("HERDR_ENV")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let runtime_child = runtime_base.path().join("herdr-top");
    assert!(
        !runtime_child
            .join(format!("{}.sock", key.hash16()))
            .exists()
    );
    assert_eq!(
        fs::read(runtime_child.join(format!("{}.name", key.hash16()))).unwrap(),
        SESSION.as_bytes()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_cleanup_tick_evicts_conditionally() {
    let running =
        RunningController::start_seeded(vec![expired_controller_record("idle-reuse")]).await;
    let reused = envelope("idle-reuse", "task_started", "reused-run");
    assert_eq!(running.send(&reused).await, ControllerResponse::Duplicate);
    wait_for_durable_row_count_with_timeout(
        &running,
        "SELECT COUNT(*) FROM event_ledger WHERE event_id = 'idle-reuse'",
        0,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(running.send(&reused).await, ControllerResponse::Accepted);
    running.stop().await;
}

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn request_performance_ingress() -> PerformanceIngress {
    performance_tracker(Arc::new(SystemPerformanceClock::new())).0
}

#[tokio::test]
async fn busy_on_saturation() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("controller.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let diagnostics = ControllerDiagnosticsHandle::default();
    let (sender, receiver) =
        controller::request_channel(1, diagnostics.clone(), request_performance_ingress());
    let cancellation = tokio_util::sync::CancellationToken::new();
    let acceptor = controller::spawn_acceptor(listener, sender, cancellation.clone()).unwrap();

    let first_path = socket_path.clone();
    let first = tokio::spawn(async move {
        send_raw(&first_path, &envelope("first", "task_started", "run-1")).await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while receiver.queued_requests() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        send_raw(&socket_path, &envelope("busy", "task_started", "run-2")).await,
        ControllerResponse::Retryable {
            reason: RetryableReason::Busy,
        }
    );
    assert_eq!(diagnostics.socket_saturations(), 1);
    first.abort();
    cancellation.cancel();
    acceptor.await.unwrap().unwrap();
}

#[test]
fn response_objects_have_closed_shape() {
    assert_eq!(
        serde_json::to_value(ControllerResponse::Accepted).unwrap(),
        json!({"status": "accepted"})
    );
    assert_eq!(
        serde_json::to_value(ControllerResponse::Duplicate).unwrap(),
        json!({"status": "duplicate"})
    );
    assert_eq!(
        serde_json::to_value(ControllerResponse::Rejected {
            reason: RejectResponseReason::Cycle
        })
        .unwrap(),
        json!({"status": "rejected", "reason": "cycle"})
    );
    assert_eq!(
        serde_json::to_value(ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable
        })
        .unwrap(),
        json!({"status": "retryable", "reason": "persistence_unavailable"})
    );
    assert!(
        serde_json::from_value::<ControllerResponse>(
            json!({"status": "accepted", "event_id": "must-not-leak"})
        )
        .is_err()
    );
}

#[tokio::test]
async fn i4_d3_later_controller_event_is_retryable_without_change() {
    const PRIVATE_SQLITE_TEXT: &str = "PRIVATE_SQLITE_TRIGGER_TEXT_6D52";
    let running = RunningController::start_with_store_setup(|root| {
        let connection = rusqlite::Connection::open(store::database_path(root)).unwrap();
        connection
            .execute_batch(&format!(
                "CREATE TRIGGER i4_fail_controller_event BEFORE INSERT ON events \
                 WHEN NEW.event_id = 'i4-controller-fails' \
                 BEGIN SELECT RAISE(ABORT, '{PRIVATE_SQLITE_TEXT}'); END;"
            ))
            .unwrap();
    })
    .await;

    let first = envelope("i4-controller-fails", "task_started", "first-run");
    assert_eq!(
        running.send_bounded(&first).await,
        ControllerResponse::Accepted
    );
    running.wait_for_persistence_degradation().await;

    let diagnostics = send_wire_value(
        &running.socket_path,
        &json!({"request": "status", "schema_version": 1}),
    )
    .await;
    assert_eq!(diagnostics["status"], "ok");
    assert_eq!(
        diagnostics["diagnostics"]["persistence_counters"]["not_committed_batches"],
        1
    );
    assert_eq!(
        diagnostics["diagnostics"]["controller_input"],
        json!({"status": "unavailable", "reason": "persistence_unavailable"})
    );
    let detail = diagnostics["diagnostics"]["persistence_detail"]
        .as_str()
        .expect("store failures must retain bounded detail");
    assert!(detail.contains(PRIVATE_SQLITE_TEXT));
    assert!(detail.len() <= store::writer::PERSISTENCE_DETAIL_MAX_BYTES);

    assert_eq!(
        running.send_bounded(&first).await,
        ControllerResponse::Duplicate,
        "the accepted event remains deduplicated after confirmed non-commit"
    );

    let later = envelope("i4-controller-later", "task_started", "later-run");
    assert_eq!(
        running.send_bounded(&later).await,
        ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable,
        }
    );
    let model = running.collector.model.borrow();
    assert!(
        model
            .task_run_by_key(&RunKey::Controller("first-run".to_owned()))
            .is_some(),
        "the accepted in-memory event must not roll back"
    );
    assert!(
        model
            .task_run_by_key(&RunKey::Controller("later-run".to_owned()))
            .is_none(),
        "the later retryable event must not mutate"
    );
    drop(model);
    let database = store::database_path(&running.root);
    let RunningController {
        _state: state_guard,
        collector,
        lifecycle,
        ..
    } = running;
    collector
        .stop()
        .await
        .expect("collector should remain stoppable after persistence degradation");
    tokio::time::timeout(Duration::from_secs(3), lifecycle.shutdown())
        .await
        .expect("writer shutdown timed out")
        .expect("writer should checkpoint after confirmed non-commit");
    let connection = rusqlite::Connection::open(database).unwrap();
    let durable: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE event_id = 'i4-controller-fails'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(durable, 0, "the trigger proves confirmed non-commit");
    drop(connection);
    drop(state_guard);
}

#[tokio::test]
async fn i4_status_request_bypasses_reducer_and_writer() {
    let running = RunningController::start().await;
    let before_model = {
        let model = running.collector.model.borrow();
        (
            model.workspaces().count(),
            model.tabs().count(),
            model.panes().count(),
            model.task_runs().count(),
        )
    };
    let before_events: i64 = rusqlite::Connection::open(store::database_path(&running.root))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();

    let response = controller::query_status(&running.socket_path)
        .await
        .expect("the read-only status helper should receive one closed response");

    assert_eq!(response["status"], "ok");
    assert_eq!(response["schema_version"], 1);
    let after_model = {
        let model = running.collector.model.borrow();
        (
            model.workspaces().count(),
            model.tabs().count(),
            model.panes().count(),
            model.task_runs().count(),
        )
    };
    assert_eq!(after_model, before_model);
    let after_events: i64 = rusqlite::Connection::open(store::database_path(&running.root))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after_events, before_events);
    running.stop().await;
}

#[tokio::test]
async fn i4_status_request_bypasses_saturated_reducer_queue() {
    let directory = tempfile::tempdir().unwrap();
    let socket_path = directory.path().join("saturated.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let (sender, receiver) = controller::request_channel(
        1,
        ControllerDiagnosticsHandle::default(),
        request_performance_ingress(),
    );
    let cancellation = CancellationToken::new();
    let (_diagnostics_sender, diagnostics) = controlled_diagnostics();
    let acceptor = controller::spawn_acceptor_with_diagnostics(
        listener,
        sender,
        cancellation.clone(),
        diagnostics,
    )
    .unwrap();

    let mut blocker = UnixStream::connect(&socket_path).await.unwrap();
    let mut event = serde_json::to_vec(&envelope("queue-blocker", "task_started", "run")).unwrap();
    event.push(b'\n');
    blocker.write_all(&event).await.unwrap();
    blocker.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while receiver.queued_requests() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the reducer queue must become saturated");

    let status = send_wire_value(
        &socket_path,
        &json!({"request": "status", "schema_version": 1}),
    )
    .await;
    assert_eq!(status["status"], "ok");
    assert_eq!(receiver.queued_requests(), 1);

    cancellation.cancel();
    acceptor.await.unwrap().unwrap();
    drop(blocker);
}

#[tokio::test]
async fn i4_status_legacy_acceptor_keeps_status_shaped_input_on_event_path() {
    let directory = tempfile::tempdir().unwrap();
    let socket_path = directory.path().join("legacy.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let (sender, receiver) = controller::request_channel(
        1,
        ControllerDiagnosticsHandle::default(),
        request_performance_ingress(),
    );
    let cancellation = CancellationToken::new();
    let acceptor = controller::spawn_acceptor(listener, sender, cancellation.clone()).unwrap();

    let blocker_path = socket_path.clone();
    let blocker = tokio::spawn(async move {
        send_wire_bytes_bounded(&blocker_path, &{
            let mut bytes = serde_json::to_vec(&envelope(
                "legacy-queue-blocker",
                "task_started",
                "legacy-run",
            ))
            .unwrap();
            bytes.push(b'\n');
            bytes
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while receiver.queued_requests() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("legacy reducer queue must become saturated");

    let response = send_wire_bytes_bounded(
        &socket_path,
        b"{\"request\":\"status\",\"schema_version\":1}\n",
    )
    .await;
    assert_eq!(
        response, b"{\"status\":\"retryable\",\"reason\":\"busy\"}\n",
        "legacy acceptor must retain the pre-T2 event admission path"
    );

    blocker.abort();
    cancellation.cancel();
    acceptor.await.unwrap().unwrap();
}

#[tokio::test]
async fn i4_status_request_is_closed_and_event_wire_is_compatible() {
    let running = RunningController::start().await;
    let success = send_wire_bytes_bounded(
        &running.socket_path,
        b"{\"request\":\"status\",\"schema_version\":1}\n",
    )
    .await;
    assert_eq!(success.last(), Some(&b'\n'));
    assert_eq!(
        serde_json::from_slice::<Value>(&success).unwrap()["status"],
        "ok"
    );

    let extra = send_wire_bytes_bounded(
        &running.socket_path,
        b"{\"request\":\"status\",\"schema_version\":1,\"extra\":true}\n",
    )
    .await;
    assert_eq!(
        extra,
        b"{\"status\":\"error\",\"schema_version\":1,\"reason\":\"invalid_request\"}\n"
    );
    let unsupported = send_wire_bytes_bounded(
        &running.socket_path,
        b"{\"request\":\"status\",\"schema_version\":2}\n",
    )
    .await;
    assert_eq!(
        unsupported,
        b"{\"status\":\"error\",\"schema_version\":1,\"reason\":\"unsupported_version\"}\n"
    );
    let missing =
        send_wire_bytes_bounded(&running.socket_path, b"{\"request\":\"status\"}\n").await;
    assert_eq!(
        missing,
        b"{\"status\":\"error\",\"schema_version\":1,\"reason\":\"invalid_request\"}\n"
    );

    let accepted = send_wire_bytes_bounded(&running.socket_path, &{
        let mut bytes = serde_json::to_vec(&envelope(
            "wire-compatible",
            "task_started",
            "wire-compatible-run",
        ))
        .unwrap();
        bytes.push(b'\n');
        bytes
    })
    .await;
    assert_eq!(accepted, b"{\"status\":\"accepted\"}\n");
    running.stop().await;
}

#[tokio::test]
async fn i4_status_request_key_on_existing_event_remains_an_ignored_extension() {
    let running = RunningController::start().await;
    let mut event = envelope("status-extension", "task_started", "extension-run");
    event["request"] = json!("status");
    assert_eq!(
        running.send_bounded(&event).await,
        ControllerResponse::Accepted
    );

    let with_invalid_event_id = send_wire_value(
        &running.socket_path,
        &json!({
            "event_id": null,
            "request": "status",
            "schema_version": 1
        }),
    )
    .await;
    assert_eq!(
        with_invalid_event_id,
        serde_json::to_value(ControllerResponse::Rejected {
            reason: RejectResponseReason::Invalid,
        })
        .unwrap()
    );
    running.stop().await;
}

#[tokio::test]
async fn i4_status_unavailable_reason_excludes_raw_found_and_bind_text() {
    const PRIVATE_BIND_TEXT: &str = "PRIVATE_BIND_PATH_AND_ERROR_F2D9";
    let running = RunningController::start_with_store_setup_and_coverage(
        |_| {},
        SourceAvailability::Unavailable {
            detail: PRIVATE_BIND_TEXT.to_owned(),
        },
    )
    .await;

    let response = send_wire_value(
        &running.socket_path,
        &json!({"request": "status", "schema_version": 1}),
    )
    .await;
    assert_eq!(
        response["diagnostics"]["controller_input"],
        json!({"status": "unavailable", "reason": "runtime_unsafe"})
    );
    assert!(!response.to_string().contains(PRIVATE_BIND_TEXT));
    running.stop().await;
}

#[tokio::test]
async fn i4_status_live_diagnostics_track_herdr_d4_and_source_order() {
    let running = RunningController::start().await;
    let mut diagnostics = running.collector.diagnostics.clone();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let disconnected = diagnostics
                .borrow()
                .source_coverage
                .iter()
                .find(|source| source.source == DiagnosticSource::Herdr)
                .is_some_and(|source| source.availability == InputAvailability::Unavailable);
            if disconnected {
                break;
            }
            diagnostics.changed().await.unwrap();
        }
    })
    .await
    .expect("consolidated diagnostics did not observe disconnected Herdr");

    let status = send_wire_value(
        &running.socket_path,
        &json!({"request": "status", "schema_version": 1}),
    )
    .await;
    assert_eq!(
        status["diagnostics"]["source_coverage"][0],
        json!({"source": "herdr", "availability": "unavailable"})
    );

    assert_eq!(
        running
            .send_bounded(&dispatch("d4-dispatch", "d4-child", "d4-parent"))
            .await,
        ControllerResponse::Accepted
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if diagnostics
                .borrow()
                .controller_counters
                .dangling_announcement_components
                == 1
            {
                break;
            }
            diagnostics.changed().await.unwrap();
        }
    })
    .await
    .expect("live diagnostics did not observe the reducer D4 counter");
    let snapshot = diagnostics.borrow();
    assert_eq!(
        snapshot.dangling_announcement_components,
        snapshot
            .controller_counters
            .dangling_announcement_components
    );
    assert_eq!(snapshot.dangling_announcement_components, 1);
    assert_eq!(
        snapshot
            .source_coverage
            .iter()
            .map(|source| source.source)
            .collect::<Vec<_>>(),
        vec![
            DiagnosticSource::Herdr,
            DiagnosticSource::Controller,
            DiagnosticSource::Claude,
            DiagnosticSource::Codex,
        ]
    );
    drop(snapshot);
    running.stop().await;
}

#[tokio::test]
async fn i4_status_closed_real_diagnostics_downgrades_controller_only() {
    let state = tempfile::tempdir().unwrap();
    let socket_dir = tempfile::tempdir().unwrap();
    let controller_path = socket_dir.path().join("controller.sock");
    let controller_listener = std::os::unix::net::UnixListener::bind(&controller_path).unwrap();
    let herdr_dir = tempfile::tempdir().unwrap();
    let herdr_path = herdr_dir.path().join("herdr.sock");
    let herdr_task = spawn_invalid_snapshot_herdr(&herdr_path);
    let root = StateRoot(state.path().to_path_buf());
    let store = store::open_writer(&root).unwrap();
    let restored = store.load_restored_state().unwrap();
    let (lifecycle, writer) = store::spawn_writer(store).unwrap();
    let mut collector = collector::spawn_with_controller(
        herdr_path,
        SESSION.to_owned(),
        restored,
        writer,
        Some(controller_listener),
    )
    .await
    .unwrap();
    collector.diagnostics.borrow_and_update();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Ok(()) = collector.diagnostics.changed().await {
            collector.diagnostics.borrow_and_update();
        }
    })
    .await
    .expect("collector task did not close its diagnostics publisher");

    assert_eq!(
        serde_json::from_value::<ControllerResponse>(
            send_wire_value(
                &controller_path,
                &envelope("closed-diagnostics-event", "task_started", "closed-run"),
            )
            .await,
        )
        .unwrap(),
        ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable,
        }
    );
    let status = send_wire_value(
        &controller_path,
        &json!({"request": "status", "schema_version": 1}),
    )
    .await;
    assert_eq!(status["status"], "ok");
    assert_eq!(
        status["diagnostics"]["persistence"],
        json!({"status": "healthy"})
    );
    assert_eq!(status["diagnostics"]["owner"], "current");
    assert_eq!(
        status["diagnostics"]["controller_input"],
        json!({"status": "unavailable", "reason": "runtime_unsafe"})
    );
    assert_eq!(
        status["diagnostics"]["source_coverage"][1],
        json!({"source": "controller", "availability": "unavailable"})
    );

    assert!(collector.stop().await.is_err());
    tokio::time::timeout(Duration::from_secs(3), lifecycle.shutdown())
        .await
        .expect("writer shutdown timed out")
        .unwrap();
    herdr_task.abort();
}
