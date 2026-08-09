use std::fs::{self, Permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use herdr_top::herdr::collector::{self, CollectorHandle};
use herdr_top::herdr::controller::{
    self, ControllerResponse, RejectResponseReason, RetryableReason,
};
use herdr_top::lockfile::{OwnerLock, StateRoot, state_root_in, try_acquire};
use herdr_top::model::{
    ControllerDiagnosticsHandle, ControllerEventKind, DisplayOrdinal, EventMetadata,
    NormalizedEvent, RunId, RunKey, Tab, TaskRun, TaskState,
};
use herdr_top::rendezvous::{
    ControllerSocketStatus, ValidatedRuntimeDir, open_runtime_dir_at, prepare_controller_socket,
    shutdown_controller_socket,
};
use herdr_top::session_key;
use herdr_top::store::{self, PersistOp, PersistTaskRun, WriterClient, WriterLifecycle};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const SESSION: &str = "controller-test-session";

struct RunningController {
    _state: TempDir,
    root: StateRoot,
    _socket_dir: TempDir,
    socket_path: PathBuf,
    collector: CollectorHandle,
    lifecycle: WriterLifecycle,
    writer: WriterClient,
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
        self.lifecycle.shutdown().await.unwrap();
        shutdown_controller_socket(self.socket_status, &self.owner_lock).unwrap();
        drop(self.runtime);
    }
}

impl RunningController {
    async fn start() -> Self {
        let state = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("controller.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let key = session_key::encode(SESSION).unwrap();
        let root = state_root_in(state.path(), &key).unwrap();
        let store = store::open_writer(&root).unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = store::spawn_writer(store).unwrap();
        let collector = collector::spawn_with_controller(
            socket_dir.path().join("missing-herdr.sock"),
            SESSION.to_owned(),
            restored,
            writer.clone(),
            Some(listener),
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
            writer,
        }
    }

    async fn send(&self, value: &Value) -> ControllerResponse {
        send_raw(&self.socket_path, value).await
    }

    async fn stop(self) {
        self.collector.stop().await.unwrap();
        self.lifecycle.shutdown().await.unwrap();
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

async fn send_raw(path: &Path, value: &Value) -> ControllerResponse {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    send_bytes(path, &bytes).await
}

async fn send_bytes(path: &Path, bytes: &[u8]) -> ControllerResponse {
    let mut stream = UnixStream::connect(path).await.unwrap();
    stream.write_all(bytes).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    serde_json::from_slice(&response).unwrap()
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
    let running = RunningController::start().await;
    assert!(
        running
            .writer
            .apply(vec![herdr_top::store::PersistOp::UpsertTab(Tab {
                tab_id: "orphan".to_owned(),
                workspace_id: "missing".to_owned(),
            })])
            .await
            .is_err()
    );
    assert_eq!(
        running
            .send(&envelope("event-1", "task_started", "run"))
            .await,
        ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable
        }
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
async fn conflict_vs_unhealthy_precedence() {
    let running = RunningController::start().await;
    assert!(
        running
            .writer
            .apply(vec![herdr_top::store::PersistOp::UpsertTab(Tab {
                tab_id: "orphan".to_owned(),
                workspace_id: "missing".to_owned(),
            })])
            .await
            .is_err()
    );
    assert_eq!(
        running.send(&dispatch("event-1", "same", "same")).await,
        rejected(RejectResponseReason::Cycle)
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn response_precedence_full() {
    let running = RunningController::start().await;
    let accepted = envelope("duplicate", "task_started", "run");
    assert_eq!(running.send(&accepted).await, ControllerResponse::Accepted);
    let mut invalid_duplicate = dispatch("duplicate", "same", "same");
    invalid_duplicate["schema_version"] = json!(99);
    assert_eq!(
        running.send(&invalid_duplicate).await,
        ControllerResponse::Duplicate
    );
    assert!(
        running
            .writer
            .apply(vec![herdr_top::store::PersistOp::UpsertTab(Tab {
                tab_id: "orphan".to_owned(),
                workspace_id: "missing".to_owned(),
            })])
            .await
            .is_err()
    );
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
    let running = RunningController::start().await;
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
    assert!(
        running
            .writer
            .apply(vec![herdr_top::store::PersistOp::UpsertTab(Tab {
                tab_id: "orphan".to_owned(),
                workspace_id: "missing".to_owned(),
            })])
            .await
            .is_err()
    );
    assert_eq!(
        running
            .send(&envelope("retry", "task_started", "still-unknown"))
            .await,
        ControllerResponse::Retryable {
            reason: RetryableReason::PersistenceUnavailable
        }
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
    running.writer.barrier().await.unwrap();
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
    running.writer.barrier().await.unwrap();
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
    running.writer.barrier().await.unwrap();
    assert!(running.writer.is_duplicate("event-1"));
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
    running.writer.barrier().await.unwrap();
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
    const EIGHT_DAYS_MS: i64 = 8 * 24 * 60 * 60 * 1_000;
    let running = RunningController::start().await;
    let event = envelope("reused", "task_started", "run");
    assert_eq!(running.send(&event).await, ControllerResponse::Accepted);
    let cleanup = running
        .writer
        .cleanup(unix_now_ms() + EIGHT_DAYS_MS)
        .await
        .unwrap();
    assert_eq!(cleanup.ledger_pruned, 1);
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
    const EIGHT_DAYS_MS: i64 = 8 * 24 * 60 * 60 * 1_000;
    let state = tempfile::tempdir().unwrap();
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("controller.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let key = session_key::encode(SESSION).unwrap();
    let root = state_root_in(state.path(), &key).unwrap();
    let mut store = store::open_writer(&root).unwrap();
    let old = unix_now_ms() - EIGHT_DAYS_MS;
    store
        .apply_batch(vec![PersistOp::RecordEvent {
            event: Box::new(NormalizedEvent::ControllerEvent {
                metadata: EventMetadata {
                    event_id: "idle-reuse".to_owned(),
                    timestamp_ms: old,
                    receipt_time_ms: old,
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
            seen_at_ms: old,
        }])
        .unwrap();
    let restored = store.load_restored_state().unwrap();
    let (lifecycle, writer) = store::spawn_writer(store).unwrap();
    assert!(writer.is_duplicate("idle-reuse"));
    let collector = collector::spawn_with_controller(
        socket_dir.path().join("missing-herdr.sock"),
        SESSION.to_owned(),
        restored,
        writer.clone(),
        Some(listener),
    )
    .await
    .unwrap();

    tokio::time::timeout(Duration::from_secs(7), async {
        while writer.is_duplicate("idle-reuse") {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        send_raw(&socket_path, &envelope("idle-reuse", "task_started", "run")).await,
        ControllerResponse::Accepted
    );
    collector.stop().await.unwrap();
    lifecycle.shutdown().await.unwrap();
}

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[tokio::test]
async fn busy_on_saturation() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("controller.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let diagnostics = ControllerDiagnosticsHandle::default();
    let (sender, receiver) = controller::request_channel(1, diagnostics.clone());
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
