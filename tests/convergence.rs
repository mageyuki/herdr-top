#[allow(dead_code)]
mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::hardening_mock::{HardeningConfig, HardeningHerdr, SnapshotReply};
use common::live_mock::{LiveConfig, LiveHerdr};
use common::mock::{MockConfig, MockHerdr, fixture_payloads};
use common::scripted_mock::{ScriptedConfig, ScriptedHerdr};
use herdr_top::activity::ActivityDurability;
use herdr_top::activity::{
    DEFAULT_TERMINAL_VISIBILITY_MS, OperatorSnapshot, default_visible_task_run_count,
    is_default_visible_task_run, runs_with_executions,
};
use herdr_top::herdr::collector::{self, CollectorHandle, ObservationQuality};
use herdr_top::identity::MergeConflict;
use herdr_top::lockfile::{StateRoot, state_root_in};
use herdr_top::model::{
    AgentSessionReference, AgentSessionReferenceKind, ControllerEventKind, DependencyEdge,
    DisplayOrdinal, DomainModel, EventMetadata, ExecState, Execution, ExecutionEdge, GapKind,
    NativeLifecycleWatermark, NativeSessionEndStatus, NormalizedEvent, Pane, PaneAgentStatus,
    PaneSnapshot, Provider, ReconcileBatch, RunId, RunKey, SnapshotAgent, Tab, TaskRun,
    TaskRunV6State, TaskState, TopologyAuthority, TopologySnapshot, Workspace,
};
use herdr_top::reducer::{ApplyOutcome, Reducer};
use herdr_top::session_key;
use herdr_top::store::writer::{WriterClient, WriterLifecycle, spawn_writer};
use herdr_top::store::{
    CollectorGap, NativeSessionBinding, PersistExecution, PersistHistoryDrain,
    PersistHistoryDrainRun, PersistOp, PersistTaskRun, PersistTaskRunV6, PersistV6Batch,
    RestoredState, database_path, open_reader, open_writer,
};
use rusqlite::Connection;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(3);

#[test]
fn historical_enrichment_never_regresses_live_state_or_watermark() {
    let directory = tempfile::tempdir().unwrap();
    let root = StateRoot(directory.path().to_path_buf());
    let mut store = open_writer(&root).unwrap();
    let run_id = RunId::new();
    let drain_id = herdr_top::model::HistoryDrainId::new("codex:convergence-history").unwrap();
    let task_run = |state, created_at_ms, updated_at_ms, finished_at_ms, subject: &str| TaskRun {
        run_id,
        key: RunKey::Native {
            provider: Provider::Codex,
            sid: "convergence-history".to_owned(),
        },
        display_ordinal: DisplayOrdinal::new(1),
        state,
        has_controller_task_state_event: state == TaskState::Completed,
        created_at_ms: Some(created_at_ms),
        updated_at_ms: Some(updated_at_ms),
        finished_at_ms,
        subject: Some(subject.to_owned()),
        dismissed_at_ms: None,
    };
    let persisted = |task_run: TaskRun| PersistTaskRun {
        created_at_ms: task_run.created_at_ms.unwrap(),
        updated_at_ms: task_run.updated_at_ms.unwrap(),
        finished_at_ms: task_run.finished_at_ms,
        native_session: Some(NativeSessionBinding {
            provider: Provider::Codex,
            native_session_id: "convergence-history".to_owned(),
        }),
        task_run,
    };
    let live_watermark = NativeLifecycleWatermark {
        source_at_ms: 500,
        observed_at_ms: 600,
        source_order: "live:controller".to_owned(),
    };
    store
        .apply_v6_batch(PersistV6Batch {
            task_runs: vec![PersistTaskRunV6 {
                task_run: persisted(task_run(
                    TaskState::Completed,
                    200,
                    500,
                    Some(500),
                    "live subject",
                )),
                state: TaskRunV6State {
                    lifecycle_watermark: Some(live_watermark.clone()),
                    history_ready: true,
                    latest_provider_at_ms: Some(500),
                    ..TaskRunV6State::default()
                },
            }],
            ..PersistV6Batch::default()
        })
        .unwrap();

    store
        .apply_v6_batch(PersistV6Batch {
            task_runs: vec![PersistTaskRunV6 {
                task_run: persisted(task_run(
                    TaskState::Running,
                    100,
                    300,
                    None,
                    "historical subject",
                )),
                state: TaskRunV6State {
                    history_ready: false,
                    latest_provider_at_ms: Some(300),
                    ..TaskRunV6State::default()
                },
            }],
            history_drains: vec![PersistHistoryDrain {
                drain_id: drain_id.clone(),
                provider: Provider::Codex,
                created_at_ms: 700,
                artifacts: Vec::new(),
            }],
            history_associations: vec![PersistHistoryDrainRun { drain_id, run_id }],
            ..PersistV6Batch::default()
        })
        .unwrap();

    let restored = store.load_restored_state().unwrap();
    let run = restored.model.task_run(&run_id).unwrap();
    assert_eq!(run.state, TaskState::Completed);
    assert!(run.has_controller_task_state_event);
    assert_eq!(run.created_at_ms, Some(100));
    assert_eq!(run.updated_at_ms, Some(500));
    assert_eq!(run.finished_at_ms, Some(500));
    assert_eq!(run.subject.as_deref(), Some("live subject"));
    let state = restored.model.task_run_v6_state(&run_id).unwrap();
    assert!(state.history_ready);
    assert_eq!(state.latest_provider_at_ms, Some(500));
    assert_eq!(state.lifecycle_watermark, Some(live_watermark));
}

#[test]
fn historical_working_state_is_not_published_before_durable_finalization() {
    let directory = tempfile::tempdir().unwrap();
    let root = StateRoot(directory.path().to_path_buf());
    let mut store = open_writer(&root).unwrap();
    let run_id = RunId::new();
    let drain_id = herdr_top::model::HistoryDrainId::new("codex:no-working-flash").unwrap();
    store
        .apply_v6_batch(PersistV6Batch {
            task_runs: vec![PersistTaskRunV6 {
                task_run: PersistTaskRun {
                    task_run: TaskRun {
                        run_id,
                        key: RunKey::Native {
                            provider: Provider::Codex,
                            sid: "historical-working".to_owned(),
                        },
                        display_ordinal: DisplayOrdinal::new(1),
                        state: TaskState::Running,
                        has_controller_task_state_event: true,
                        created_at_ms: Some(1_000),
                        updated_at_ms: Some(2_000),
                        finished_at_ms: None,
                        subject: None,
                        dismissed_at_ms: None,
                    },
                    native_session: Some(NativeSessionBinding {
                        provider: Provider::Codex,
                        native_session_id: "historical-working".to_owned(),
                    }),
                    created_at_ms: 1_000,
                    updated_at_ms: 2_000,
                    finished_at_ms: None,
                },
                state: TaskRunV6State {
                    history_ready: false,
                    latest_provider_at_ms: Some(2_000),
                    ..TaskRunV6State::default()
                },
            }],
            history_drains: vec![PersistHistoryDrain {
                drain_id: drain_id.clone(),
                provider: Provider::Codex,
                created_at_ms: 2_500,
                artifacts: Vec::new(),
            }],
            history_associations: vec![PersistHistoryDrainRun {
                drain_id: drain_id.clone(),
                run_id,
            }],
            ..PersistV6Batch::default()
        })
        .unwrap();

    let staged = store.load_restored_state().unwrap();
    assert_eq!(
        staged.model.task_run(&run_id).unwrap().state,
        TaskState::Running
    );
    assert!(
        !staged
            .model
            .task_run_v6_state(&run_id)
            .unwrap()
            .history_ready
    );
    let (_before_commit, published_before_commit) = Reducer::new(staged);
    assert!(
        published_before_commit.borrow().task_run(&run_id).is_none(),
        "historical Working must not enter a published snapshot before finalization commits"
    );

    let finalization = store.finalize_history_drain(&drain_id, 3_000).unwrap();
    assert_eq!(finalization.finalized_at_ms, 3_000);
    assert_eq!(finalization.runs.len(), 1);
    assert_eq!(finalization.runs[0].run_id, run_id);
    assert!(finalization.runs[0].state.history_ready);
    assert_eq!(
        finalization.runs[0]
            .state
            .native_session_end
            .as_ref()
            .map(|end| end.status),
        Some(NativeSessionEndStatus::Unknown)
    );

    let finalized = store.load_restored_state().unwrap();
    let (_after_commit, published_after_commit) = Reducer::new(finalized);
    let published = published_after_commit.borrow();
    assert_eq!(
        published.task_run(&run_id).unwrap().state,
        TaskState::Running
    );
    assert!(published.task_run_v6_state(&run_id).unwrap().history_ready);
    assert_eq!(
        published
            .task_run_v6_state(&run_id)
            .and_then(|state| state.native_session_end.as_ref())
            .map(|end| end.status),
        Some(NativeSessionEndStatus::Unknown),
        "the first published historical state must be the finalized native outcome"
    );
}

#[test]
fn terminal_visibility_is_one_hour_at_every_depth_and_closes_over_ancestors() {
    let now_ms = 10_000_000;
    let boundary_at_ms = now_ms - DEFAULT_TERMINAL_VISIBILITY_MS;
    let root = RunId::new();
    let child = RunId::new();
    let grandchild = RunId::new();
    let mut model = DomainModel::default();
    for (run_id, sid, ordinal) in [
        (root, "root", 1),
        (child, "child", 2),
        (grandchild, "grandchild", 3),
    ] {
        model.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Native {
                provider: Provider::Codex,
                sid: sid.to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(ordinal),
            state: TaskState::Completed,
            has_controller_task_state_event: true,
            created_at_ms: Some(1),
            updated_at_ms: Some(boundary_at_ms),
            finished_at_ms: Some(boundary_at_ms),
            subject: None,
            dismissed_at_ms: None,
        });
    }
    model.insert_execution_edge(ExecutionEdge {
        parent_run_id: root,
        child_run_id: child,
    });
    model.insert_execution_edge(ExecutionEdge {
        parent_run_id: child,
        child_run_id: grandchild,
    });
    let execution_runs = runs_with_executions(&model);
    let operator = |times: HashMap<RunId, i64>| OperatorSnapshot {
        activity: Arc::from([]),
        terminal_times: Arc::new(times),
    };
    let all_at_boundary = operator(HashMap::from([
        (root, boundary_at_ms),
        (child, boundary_at_ms),
        (grandchild, boundary_at_ms),
    ]));

    assert_eq!(
        default_visible_task_run_count(&model, &all_at_boundary, now_ms - 1),
        3
    );
    for run_id in [root, child, grandchild] {
        assert!(!is_default_visible_task_run(
            model.task_run(&run_id).unwrap(),
            &all_at_boundary,
            &execution_runs,
            now_ms,
        ));
    }
    assert_eq!(
        default_visible_task_run_count(&model, &all_at_boundary, now_ms),
        0
    );

    let descendant_visible = operator(HashMap::from([
        (root, boundary_at_ms),
        (child, boundary_at_ms),
        (grandchild, boundary_at_ms + 1),
    ]));
    assert!(!is_default_visible_task_run(
        model.task_run(&root).unwrap(),
        &descendant_visible,
        &execution_runs,
        now_ms,
    ));
    assert!(!is_default_visible_task_run(
        model.task_run(&child).unwrap(),
        &descendant_visible,
        &execution_runs,
        now_ms,
    ));
    assert!(is_default_visible_task_run(
        model.task_run(&grandchild).unwrap(),
        &descendant_visible,
        &execution_runs,
        now_ms,
    ));
    assert_eq!(
        default_visible_task_run_count(&model, &descendant_visible, now_ms),
        3,
        "expired ancestors must remain as structural rows"
    );
}

#[derive(Clone, Debug)]
struct ScopedHerdrConfig {
    snapshots: Vec<Value>,
    reject_pane_once: Option<String>,
    reject_all_enrichment: bool,
    snapshot_delay: Duration,
    gate_snapshot_responses: bool,
}

impl ScopedHerdrConfig {
    fn snapshots(snapshots: Vec<Value>) -> Self {
        Self {
            snapshots,
            reject_pane_once: None,
            reject_all_enrichment: false,
            snapshot_delay: Duration::ZERO,
            gate_snapshot_responses: false,
        }
    }
}

struct ScopedSnapshotArrival {
    index: usize,
    release: oneshot::Sender<()>,
}

enum ScopedStreamCommand {
    Push(Value, oneshot::Sender<()>),
    PushMany(Vec<Value>, oneshot::Sender<()>),
    Close(oneshot::Sender<()>),
}

struct ScopedHerdr {
    _directory: TempDir,
    socket_path: std::path::PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
    ordering: Arc<Mutex<Vec<String>>>,
    primary: Arc<Mutex<Option<mpsc::UnboundedSender<ScopedStreamCommand>>>>,
    enrichment: Arc<Mutex<Option<mpsc::UnboundedSender<ScopedStreamCommand>>>>,
    enrichment_subscriptions: Arc<AtomicUsize>,
    enrichment_closures: Arc<AtomicUsize>,
    snapshot_requests: Arc<AtomicUsize>,
    snapshot_state: Arc<Mutex<Value>>,
    snapshot_arrivals: tokio::sync::Mutex<mpsc::UnboundedReceiver<ScopedSnapshotArrival>>,
    accept_task: JoinHandle<()>,
}

impl ScopedHerdr {
    async fn start(config: ScopedHerdrConfig) -> std::io::Result<Self> {
        let directory = tempfile::tempdir()?;
        let socket_path = directory.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket_path)?;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let ordering = Arc::new(Mutex::new(Vec::new()));
        let primary = Arc::new(Mutex::new(None));
        let enrichment = Arc::new(Mutex::new(None));
        let enrichment_subscriptions = Arc::new(AtomicUsize::new(0));
        let enrichment_closures = Arc::new(AtomicUsize::new(0));
        let snapshot_requests = Arc::new(AtomicUsize::new(0));
        let snapshot_state = Arc::new(Mutex::new(
            config
                .snapshots
                .first()
                .cloned()
                .unwrap_or_else(|| json!({})),
        ));
        let (snapshot_arrival_sender, snapshot_arrivals) = mpsc::unbounded_channel();
        let rejected = Arc::new(AtomicBool::new(false));
        let config = Arc::new(config);
        let accept_task = {
            let requests = Arc::clone(&requests);
            let ordering = Arc::clone(&ordering);
            let primary = Arc::clone(&primary);
            let enrichment = Arc::clone(&enrichment);
            let enrichment_subscriptions = Arc::clone(&enrichment_subscriptions);
            let enrichment_closures = Arc::clone(&enrichment_closures);
            let snapshot_requests = Arc::clone(&snapshot_requests);
            let snapshot_state = Arc::clone(&snapshot_state);
            let snapshot_arrival_sender = snapshot_arrival_sender.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let config = Arc::clone(&config);
                    let requests = Arc::clone(&requests);
                    let ordering = Arc::clone(&ordering);
                    let primary = Arc::clone(&primary);
                    let enrichment = Arc::clone(&enrichment);
                    let enrichment_subscriptions = Arc::clone(&enrichment_subscriptions);
                    let enrichment_closures = Arc::clone(&enrichment_closures);
                    let snapshot_requests = Arc::clone(&snapshot_requests);
                    let snapshot_state = Arc::clone(&snapshot_state);
                    let snapshot_arrival_sender = snapshot_arrival_sender.clone();
                    let rejected = Arc::clone(&rejected);
                    tokio::spawn(async move {
                        let _ = scoped_handle_connection(
                            stream,
                            &config,
                            &requests,
                            &ordering,
                            &primary,
                            &enrichment,
                            &enrichment_subscriptions,
                            &enrichment_closures,
                            &snapshot_requests,
                            &snapshot_state,
                            &snapshot_arrival_sender,
                            &rejected,
                        )
                        .await;
                    });
                }
            })
        };
        Ok(Self {
            _directory: directory,
            socket_path,
            requests,
            ordering,
            primary,
            enrichment,
            enrichment_subscriptions,
            enrichment_closures,
            snapshot_requests,
            snapshot_state,
            snapshot_arrivals: tokio::sync::Mutex::new(snapshot_arrivals),
            accept_task,
        })
    }

    fn socket_path(&self) -> &std::path::Path {
        &self.socket_path
    }

    fn requests(&self) -> Vec<Value> {
        self.requests.lock().unwrap().clone()
    }

    fn ordering(&self) -> Vec<String> {
        self.ordering.lock().unwrap().clone()
    }

    fn enrichment_subscriptions(&self) -> usize {
        self.enrichment_subscriptions.load(Ordering::SeqCst)
    }

    fn enrichment_closures(&self) -> usize {
        self.enrichment_closures.load(Ordering::SeqCst)
    }

    fn snapshot_requests(&self) -> usize {
        self.snapshot_requests.load(Ordering::SeqCst)
    }

    async fn next_snapshot_request(&self) -> ScopedSnapshotArrival {
        tokio::time::timeout(WAIT, self.snapshot_arrivals.lock().await.recv())
            .await
            .expect("snapshot request-arrival gate timed out")
            .expect("snapshot request-arrival gate closed")
    }

    async fn push_primary(&self, frame: Value) -> std::io::Result<()> {
        apply_primary_hint_to_snapshot(&mut self.snapshot_state.lock().unwrap(), &frame);
        scoped_push(&self.primary, frame).await
    }

    async fn push_enrichment(&self, frame: Value) -> std::io::Result<()> {
        scoped_push(&self.enrichment, frame).await
    }

    async fn push_enrichment_many(&self, frames: Vec<Value>) -> std::io::Result<()> {
        let sender = self
            .enrichment
            .lock()
            .map_err(|_| std::io::Error::other("enrichment fixture mutex poisoned"))?
            .clone()
            .ok_or_else(|| std::io::Error::other("enrichment stream is not connected"))?;
        let (acknowledgement, response) = oneshot::channel();
        sender
            .send(ScopedStreamCommand::PushMany(frames, acknowledgement))
            .map_err(|_| std::io::Error::other("enrichment stream is closed"))?;
        response
            .await
            .map_err(|_| std::io::Error::other("enrichment burst was not acknowledged"))
    }

    async fn close_enrichment(&self) -> std::io::Result<()> {
        let sender = self
            .enrichment
            .lock()
            .map_err(|_| std::io::Error::other("enrichment fixture mutex poisoned"))?
            .clone()
            .ok_or_else(|| std::io::Error::other("enrichment stream is not connected"))?;
        let (acknowledgement, response) = oneshot::channel();
        sender
            .send(ScopedStreamCommand::Close(acknowledgement))
            .map_err(|_| std::io::Error::other("enrichment stream is closed"))?;
        response
            .await
            .map_err(|_| std::io::Error::other("enrichment close was not acknowledged"))
    }
}

impl Drop for ScopedHerdr {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

#[allow(clippy::too_many_arguments)]
async fn scoped_handle_connection(
    stream: tokio::net::UnixStream,
    config: &ScopedHerdrConfig,
    requests: &Mutex<Vec<Value>>,
    ordering: &Mutex<Vec<String>>,
    primary: &Mutex<Option<mpsc::UnboundedSender<ScopedStreamCommand>>>,
    enrichment: &Mutex<Option<mpsc::UnboundedSender<ScopedStreamCommand>>>,
    enrichment_subscriptions: &AtomicUsize,
    enrichment_closures: &AtomicUsize,
    snapshot_requests: &AtomicUsize,
    snapshot_state: &Mutex<Value>,
    snapshot_arrival_sender: &mpsc::UnboundedSender<ScopedSnapshotArrival>,
    rejected: &AtomicBool,
) -> std::io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }
    let request: Value = serde_json::from_str(&line)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    requests.lock().unwrap().push(request.clone());
    let id = request["id"]
        .as_str()
        .ok_or_else(|| std::io::Error::other("request id missing"))?
        .to_owned();
    match request["method"].as_str() {
        Some("session.snapshot") => {
            let index = snapshot_requests.fetch_add(1, Ordering::SeqCst);
            if !config.snapshot_delay.is_zero() {
                tokio::time::sleep(config.snapshot_delay).await;
            }
            if config.gate_snapshot_responses {
                let (release, released) = oneshot::channel();
                snapshot_arrival_sender
                    .send(ScopedSnapshotArrival { index, release })
                    .map_err(|_| std::io::Error::other("snapshot arrival observer closed"))?;
                released
                    .await
                    .map_err(|_| std::io::Error::other("snapshot response release dropped"))?;
            }
            let snapshot = config.snapshots.get(index).cloned().unwrap_or_else(|| {
                snapshot_state
                    .lock()
                    .expect("snapshot fixture mutex poisoned")
                    .clone()
            });
            if index > 0 && index < config.snapshots.len() {
                *snapshot_state
                    .lock()
                    .expect("snapshot fixture mutex poisoned") = snapshot.clone();
            }
            scoped_write_frame(
                &mut write_half,
                &json!({"id": id, "result": {"type": "session_snapshot", "snapshot": snapshot}}),
            )
            .await
        }
        Some("events.subscribe") => {
            let scoped =
                request["params"]["subscriptions"]
                    .as_array()
                    .is_some_and(|subscriptions| {
                        subscriptions.iter().any(|subscription| {
                            subscription["type"].as_str() == Some("pane.agent_status_changed")
                        })
                    });
            if scoped {
                let index = enrichment_subscriptions.fetch_add(1, Ordering::SeqCst) + 1;
                ordering
                    .lock()
                    .unwrap()
                    .push(format!("enrichment_subscribe:{index}"));
                if config.reject_all_enrichment {
                    return scoped_write_frame(
                        &mut write_half,
                        &json!({
                            "id": format!("{id}:sub:0:probe"),
                            "error": {"code": "subscription_unavailable", "message": "enrichment unavailable"}
                        }),
                    )
                    .await;
                }
                if let Some(pane_id) = &config.reject_pane_once
                    && !rejected.swap(true, Ordering::SeqCst)
                    && request["params"]["subscriptions"]
                        .as_array()
                        .is_some_and(|subscriptions| {
                            subscriptions.iter().any(|subscription| {
                                subscription["pane_id"].as_str() == Some(pane_id)
                            })
                        })
                {
                    return scoped_write_frame(
                        &mut write_half,
                        &json!({
                            "id": format!("{id}:sub:1:probe"),
                            "error": {
                                "code": "pane_not_found",
                                "message": format!("pane {pane_id} not found")
                            }
                        }),
                    )
                    .await;
                }
            }
            scoped_write_frame(
                &mut write_half,
                &json!({"id": id, "result": {"type": "subscription_started"}}),
            )
            .await?;
            let (sender, mut commands) = mpsc::unbounded_channel();
            if scoped {
                *enrichment.lock().unwrap() = Some(sender);
            } else {
                *primary.lock().unwrap() = Some(sender);
            }
            line.clear();
            loop {
                tokio::select! {
                    command = commands.recv() => match command {
                        Some(ScopedStreamCommand::Push(frame, acknowledgement)) => {
                            scoped_write_frame(&mut write_half, &frame).await?;
                            let _ = acknowledgement.send(());
                        }
                        Some(ScopedStreamCommand::PushMany(frames, acknowledgement)) => {
                            for frame in frames {
                                scoped_write_frame(&mut write_half, &frame).await?;
                            }
                            let _ = acknowledgement.send(());
                        }
                        Some(ScopedStreamCommand::Close(acknowledgement)) => {
                            let _ = acknowledgement.send(());
                            break;
                        }
                        None => break,
                    },
                    result = reader.read_line(&mut line) => {
                        let _ = result?;
                        break;
                    }
                }
            }
            if scoped {
                let index = enrichment_closures.fetch_add(1, Ordering::SeqCst) + 1;
                ordering
                    .lock()
                    .unwrap()
                    .push(format!("enrichment_close:{index}"));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn scoped_write_frame(
    stream: &mut tokio::net::unix::OwnedWriteHalf,
    frame: &Value,
) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(frame)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;
    stream.flush().await
}

async fn scoped_push(
    stream: &Mutex<Option<mpsc::UnboundedSender<ScopedStreamCommand>>>,
    frame: Value,
) -> std::io::Result<()> {
    let sender = stream
        .lock()
        .map_err(|_| std::io::Error::other("scoped fixture mutex poisoned"))?
        .clone()
        .ok_or_else(|| std::io::Error::other("scoped stream is not connected"))?;
    let (acknowledgement, response) = oneshot::channel();
    sender
        .send(ScopedStreamCommand::Push(frame, acknowledgement))
        .map_err(|_| std::io::Error::other("scoped stream is closed"))?;
    response
        .await
        .map_err(|_| std::io::Error::other("scoped push was not acknowledged"))
}

fn apply_primary_hint_to_snapshot(snapshot: &mut Value, frame: &Value) {
    let Some(event) = frame["event"].as_str() else {
        return;
    };
    let event = event.replace('.', "_");
    let data = &frame["data"];

    match event.as_str() {
        "workspace_created"
        | "workspace_updated"
        | "workspace_metadata_updated"
        | "workspace_renamed"
        | "workspace_moved"
        | "workspace_reordered" => {
            if let Some(workspace) = data.get("workspace") {
                upsert_snapshot_entity(snapshot, "workspaces", "workspace_id", workspace);
            } else if let Some(workspace_id) = data["workspace_id"].as_str() {
                merge_snapshot_entity(snapshot, "workspaces", "workspace_id", workspace_id, data);
            }
        }
        "workspace_closed" => {
            if let Some(workspace_id) = data["workspace_id"].as_str() {
                remove_snapshot_entity(snapshot, "workspaces", "workspace_id", workspace_id);
                remove_snapshot_members(snapshot, "tabs", "workspace_id", workspace_id);
                remove_snapshot_members(snapshot, "panes", "workspace_id", workspace_id);
            }
        }
        "tab_created" | "tab_renamed" | "tab_moved" => {
            if let Some(tab) = data.get("tab") {
                upsert_snapshot_entity(snapshot, "tabs", "tab_id", tab);
            } else if let Some(tab_id) = data["tab_id"].as_str() {
                merge_snapshot_entity(snapshot, "tabs", "tab_id", tab_id, data);
            }
        }
        "tab_closed" => {
            if let Some(tab_id) = data["tab_id"].as_str() {
                remove_snapshot_entity(snapshot, "tabs", "tab_id", tab_id);
                remove_snapshot_members(snapshot, "panes", "tab_id", tab_id);
            }
        }
        "pane_created" | "pane_updated" | "pane_agent_detected" => {
            if let Some(pane) = data.get("pane") {
                upsert_snapshot_entity(snapshot, "panes", "pane_id", pane);
            }
        }
        "pane_moved" => {
            if let Some(previous_pane_id) = data["previous_pane_id"].as_str() {
                remove_snapshot_entity(snapshot, "panes", "pane_id", previous_pane_id);
            }
            if let Some(workspace_id) = data["closed_workspace_id"].as_str() {
                remove_snapshot_entity(snapshot, "workspaces", "workspace_id", workspace_id);
                remove_snapshot_members(snapshot, "tabs", "workspace_id", workspace_id);
                remove_snapshot_members(snapshot, "panes", "workspace_id", workspace_id);
            }
            if let Some(tab_id) = data["closed_tab_id"].as_str() {
                remove_snapshot_entity(snapshot, "tabs", "tab_id", tab_id);
                remove_snapshot_members(snapshot, "panes", "tab_id", tab_id);
            }
            if let Some(tab) = data.get("created_tab") {
                upsert_snapshot_entity(snapshot, "tabs", "tab_id", tab);
            }
            if let Some(pane) = data.get("pane") {
                upsert_snapshot_entity(snapshot, "panes", "pane_id", pane);
            }
        }
        "pane_closed" | "pane_exited" => {
            if let Some(pane_id) = data["pane_id"].as_str() {
                remove_snapshot_entity(snapshot, "panes", "pane_id", pane_id);
            }
        }
        _ => return,
    }

    refresh_snapshot_counts(snapshot);
}

fn snapshot_after_primary_hints(mut snapshot: Value, frames: &[Value]) -> Value {
    for frame in frames {
        apply_primary_hint_to_snapshot(&mut snapshot, frame);
    }
    snapshot
}

fn upsert_snapshot_entity(snapshot: &mut Value, collection: &str, id_field: &str, entity: &Value) {
    let Some(entity_id) = entity[id_field].as_str() else {
        return;
    };
    let Some(items) = snapshot[collection].as_array_mut() else {
        return;
    };
    if let Some(existing) = items
        .iter_mut()
        .find(|item| item[id_field].as_str() == Some(entity_id))
    {
        merge_json_object(existing, entity);
    } else {
        items.push(entity.clone());
    }
}

fn merge_snapshot_entity(
    snapshot: &mut Value,
    collection: &str,
    id_field: &str,
    entity_id: &str,
    update: &Value,
) {
    let Some(items) = snapshot[collection].as_array_mut() else {
        return;
    };
    if let Some(existing) = items
        .iter_mut()
        .find(|item| item[id_field].as_str() == Some(entity_id))
    {
        merge_json_object(existing, update);
    }
}

fn merge_json_object(target: &mut Value, update: &Value) {
    let (Some(target), Some(update)) = (target.as_object_mut(), update.as_object()) else {
        return;
    };
    for (key, value) in update {
        if key != "type" {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn remove_snapshot_entity(snapshot: &mut Value, collection: &str, id_field: &str, id: &str) {
    if let Some(items) = snapshot[collection].as_array_mut() {
        items.retain(|item| item[id_field].as_str() != Some(id));
    }
}

fn remove_snapshot_members(
    snapshot: &mut Value,
    collection: &str,
    parent_field: &str,
    parent_id: &str,
) {
    if let Some(items) = snapshot[collection].as_array_mut() {
        items.retain(|item| item[parent_field].as_str() != Some(parent_id));
    }
}

fn refresh_snapshot_counts(snapshot: &mut Value) {
    let pane_parents: Vec<_> = snapshot["panes"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|pane| {
            (
                pane["workspace_id"].as_str().map(str::to_owned),
                pane["tab_id"].as_str().map(str::to_owned),
            )
        })
        .collect();
    let tab_parents: Vec<_> = snapshot["tabs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tab| tab["workspace_id"].as_str().map(str::to_owned))
        .collect();

    if let Some(workspaces) = snapshot["workspaces"].as_array_mut() {
        for workspace in workspaces {
            let workspace_id = workspace["workspace_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            workspace["pane_count"] = json!(
                pane_parents
                    .iter()
                    .filter(|(parent, _)| parent.as_deref() == Some(workspace_id.as_str()))
                    .count()
            );
            workspace["tab_count"] = json!(
                tab_parents
                    .iter()
                    .filter(|parent| parent.as_str() == workspace_id.as_str())
                    .count()
            );
        }
    }
    if let Some(tabs) = snapshot["tabs"].as_array_mut() {
        for tab in tabs {
            let tab_id = tab["tab_id"].as_str().unwrap_or_default().to_owned();
            tab["pane_count"] = json!(
                pane_parents
                    .iter()
                    .filter(|(_, parent)| parent.as_deref() == Some(tab_id.as_str()))
                    .count()
            );
        }
    }
}

#[tokio::test]
async fn scoped_subscription_keeps_primary_unscoped_and_enriches_each_snapshot_pane() {
    let mut snapshot = p1_snapshot();
    snapshot["panes"] = json!([
        snapshot["panes"][0].clone(),
        pane_value("w1:p2", "terminal-2", "w1", "w1:t1")
    ]);
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![snapshot]))
        .await
        .unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;

    let subscriptions: Vec<_> = mock
        .requests()
        .into_iter()
        .filter(|request| request["method"] == "events.subscribe")
        .collect();
    assert_eq!(subscriptions.len(), 2);
    assert_eq!(
        subscriptions[0]["params"]["subscriptions"],
        json!([
            {"type":"workspace.created"},
            {"type":"workspace.updated"},
            {"type":"workspace.metadata_updated"},
            {"type":"workspace.renamed"},
            {"type":"workspace.moved"},
            {"type":"workspace.reordered"},
            {"type":"workspace.closed"},
            {"type":"workspace.focused"},
            {"type":"tab.created"},
            {"type":"tab.renamed"},
            {"type":"tab.moved"},
            {"type":"tab.closed"},
            {"type":"tab.focused"},
            {"type":"pane.created"},
            {"type":"pane.closed"},
            {"type":"pane.updated"},
            {"type":"pane.focused"},
            {"type":"pane.moved"},
            {"type":"pane.exited"},
            {"type":"pane.agent_detected"},
            {"type":"layout.updated"}
        ])
    );
    assert_eq!(
        subscriptions[1]["params"]["subscriptions"],
        json!([
            {"type":"pane.agent_status_changed", "pane_id":"w1:p1"},
            {"type":"pane.agent_status_changed", "pane_id":"w1:p2"}
        ])
    );

    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn collector_accepts_exact_dotted_and_underscore_pane_status_aliases() {
    let mut snapshot = agent_snapshot("alias-session", AgentSessionReferenceKind::Id, "idle");
    snapshot["panes"]
        .as_array_mut()
        .unwrap()
        .push(agent_pane_value(
            "w1:p2",
            "alias-sentinel-terminal",
            "w1",
            "w1:t1",
            "alias-sentinel-session",
        ));
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![snapshot]))
        .await
        .unwrap();
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;

    mock.push_enrichment(agent_status_alias_push(
        "pane.agent_status_changed",
        "w1:p1",
        "term_6583d08d791e41",
        "working",
    ))
    .await
    .unwrap();
    wait_execution_state(&handle, ExecState::Working).await;
    mock.push_enrichment(agent_status_alias_push(
        "pane_agent_status_changed",
        "w1:p1",
        "term_6583d08d791e41",
        "idle",
    ))
    .await
    .unwrap();
    wait_execution_state(&handle, ExecState::Idle).await;
    mock.push_enrichment(agent_status_alias_push(
        "pane.agent.status_changed",
        "w1:p1",
        "term_6583d08d791e41",
        "working",
    ))
    .await
    .unwrap();
    mock.push_enrichment(agent_status_alias_push(
        "pane_agent_status_changed",
        "w1:p2",
        "alias-sentinel-terminal",
        "idle",
    ))
    .await
    .unwrap();
    wait_until(|| pane_status_event_count(&root) >= 3).await;
    assert_eq!(
        handle
            .model
            .borrow()
            .executions()
            .find(|execution| execution.pane_id == "w1:p1" && !execution.state.is_terminal())
            .unwrap()
            .state,
        ExecState::Idle
    );

    shutdown(handle, lifecycle).await;
    let connection = Connection::open(database_path(&root)).unwrap();
    let rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE source_event_type = 'pane_agent_status_changed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 3);
}

#[tokio::test]
async fn pane_not_found_prunes_only_rejected_enrichment_target_and_retries() {
    let mut snapshot = p1_snapshot();
    snapshot["panes"] = json!([
        snapshot["panes"][0].clone(),
        pane_value("w9:p99", "terminal-99", "w1", "w1:t1")
    ]);
    let mut config = ScopedHerdrConfig::snapshots(vec![snapshot]);
    config.reject_pane_once = Some("w9:p99".to_owned());
    let mock = ScopedHerdr::start(config).await.unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() >= 2).await;

    let scoped: Vec<_> = mock
        .requests()
        .into_iter()
        .filter(|request| {
            request["method"] == "events.subscribe"
                && request["params"]["subscriptions"]
                    .as_array()
                    .is_some_and(|subscriptions| {
                        subscriptions
                            .iter()
                            .any(|subscription| subscription["type"] == "pane.agent_status_changed")
                    })
        })
        .collect();
    assert_eq!(scoped.len(), 2);
    assert!(scoped[0].to_string().contains("w9:p99"));
    assert_eq!(
        scoped[1]["params"]["subscriptions"],
        json!([{"type":"pane.agent_status_changed", "pane_id":"w1:p1"}])
    );

    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn pane_created_swaps_enrichment_break_before_make_without_collector_gap() {
    let snapshot = agent_snapshot("swap-created", AgentSessionReferenceKind::Id, "idle");
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![snapshot]))
        .await
        .unwrap();
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;
    let execution_id = handle
        .model
        .borrow()
        .executions()
        .find(|execution| !execution.state.is_terminal())
        .unwrap()
        .execution_id
        .clone();

    mock.push_primary(push(
        "pane_created",
        json!({"type":"pane_created", "pane": pane_value("w1:p2", "terminal-2", "w1", "w1:t1")}),
    ))
    .await
    .unwrap();
    wait_until(|| mock.enrichment_subscriptions() == 2).await;
    wait_until(|| {
        mock.ordering()
            .iter()
            .any(|entry| entry == "enrichment_close:1")
    })
    .await;

    let order = mock.ordering();
    let closed = order
        .iter()
        .position(|entry| entry == "enrichment_close:1")
        .unwrap();
    let opened = order
        .iter()
        .position(|entry| entry == "enrichment_subscribe:2")
        .unwrap();
    assert!(
        closed < opened,
        "old connection must close before replacement: {order:?}"
    );
    let second = mock
        .requests()
        .into_iter()
        .filter(|request| {
            request["method"] == "events.subscribe"
                && request["params"]["subscriptions"][0]["type"] == "pane.agent_status_changed"
        })
        .nth(1)
        .unwrap();
    assert_eq!(
        second["params"]["subscriptions"],
        json!([
            {"type":"pane.agent_status_changed", "pane_id":"w1:p1"},
            {"type":"pane.agent_status_changed", "pane_id":"w1:p2"}
        ])
    );
    assert!(
        handle
            .model
            .borrow()
            .execution(&execution_id)
            .is_some_and(|execution| !execution.state.is_terminal())
    );

    shutdown(handle, lifecycle).await;
    let connection = Connection::open(database_path(&root)).unwrap();
    let gaps: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE normalized_kind = 'collector_gap'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        gaps, 1,
        "enrichment replacement must not record a collector gap"
    );
}

#[tokio::test]
async fn pane_moved_replaces_old_public_pane_id_in_enrichment_target() {
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![p1_snapshot()]))
        .await
        .unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;
    let moved = fixture_payloads("p4-terminal-id-move.jsonl", "B2", "recv")
        .into_iter()
        .find(|payload| payload["event"] == "pane_moved")
        .unwrap();
    mock.push_primary(moved).await.unwrap();
    wait_until(|| mock.enrichment_subscriptions() == 2).await;
    wait_until(|| {
        mock.ordering()
            .iter()
            .any(|entry| entry == "enrichment_close:1")
    })
    .await;

    let second = mock
        .requests()
        .into_iter()
        .filter(|request| {
            request["method"] == "events.subscribe"
                && request["params"]["subscriptions"][0]["type"] == "pane.agent_status_changed"
        })
        .nth(1)
        .unwrap();
    assert_eq!(
        second["params"]["subscriptions"],
        json!([{"type":"pane.agent_status_changed", "pane_id":"w2:p2"}])
    );
    let order = mock.ordering();
    let closed = order
        .iter()
        .position(|entry| entry == "enrichment_close:1")
        .unwrap();
    let opened = order
        .iter()
        .position(|entry| entry == "enrichment_subscribe:2")
        .unwrap();
    assert!(
        closed < opened,
        "pane move replacement must be break-before-make: {order:?}"
    );

    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn swap_window_records_one_transition_on_each_side_without_overlap() {
    let snapshot = agent_snapshot("swap-window", AgentSessionReferenceKind::Id, "idle");
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![snapshot]))
        .await
        .unwrap();
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;

    mock.push_enrichment(agent_status_push("w1:p1", "term_6583d08d791e41", "working"))
        .await
        .unwrap();
    wait_execution_state(&handle, ExecState::Working).await;
    mock.push_primary(push(
        "pane_created",
        json!({"type":"pane_created", "pane": pane_value("w1:p2", "terminal-2", "w1", "w1:t1")}),
    ))
    .await
    .unwrap();
    wait_until(|| mock.enrichment_subscriptions() == 2).await;
    wait_until(|| {
        mock.ordering()
            .iter()
            .any(|entry| entry == "enrichment_close:1")
    })
    .await;
    mock.push_enrichment(agent_status_push("w1:p1", "term_6583d08d791e41", "blocked"))
        .await
        .unwrap();
    wait_execution_state(&handle, ExecState::Blocked).await;

    let order = mock.ordering();
    let closed = order
        .iter()
        .position(|entry| entry == "enrichment_close:1")
        .unwrap();
    let opened = order
        .iter()
        .position(|entry| entry == "enrichment_subscribe:2")
        .unwrap();
    assert!(
        closed < opened,
        "the old stream must close before the replacement subscribes: {order:?}"
    );
    shutdown(handle, lifecycle).await;
    let connection = Connection::open(database_path(&root)).unwrap();
    let rows: Vec<(String, String)> = connection
        .prepare(
            "SELECT event_id, source_event_type FROM events \
             WHERE source_event_type = 'pane_agent_status_changed' ORDER BY event_row_id",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0].0, rows[1].0);
}

#[tokio::test]
async fn enrichment_eof_retries_without_changing_primary_quality_or_resnapshotting() {
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![p1_snapshot()]))
        .await
        .unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;

    mock.close_enrichment().await.unwrap();
    wait_until(|| mock.enrichment_subscriptions() == 2).await;
    assert_eq!(*handle.quality.borrow(), ObservationQuality::Live);
    assert_eq!(mock.snapshot_requests(), 1);

    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn enrichment_flood_counts_plain_channel_drops_without_primary_resnapshot() {
    let snapshot = agent_snapshot("enrichment-flood", AgentSessionReferenceKind::Id, "idle");
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![snapshot]))
        .await
        .unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;
    let frames = (0..512)
        .map(|index| {
            agent_status_push(
                "w1:p1",
                "term_6583d08d791e41",
                if index % 2 == 0 { "working" } else { "blocked" },
            )
        })
        .collect();
    mock.push_enrichment_many(frames).await.unwrap();
    mock.push_primary(push(
        "workspace_focused",
        json!({"type":"workspace_focused", "workspace_id":"w1"}),
    ))
    .await
    .unwrap();
    wait_until(|| {
        serde_json::to_value(handle.diagnostics.borrow().clone()).unwrap()
            ["enrichment_counters"]["channel_full_drops"]
            .as_u64()
            .is_some_and(|drops| drops > 0)
    })
    .await;

    assert_eq!(*handle.quality.borrow(), ObservationQuality::Live);
    assert_eq!(mock.snapshot_requests(), 1);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn queued_enrichment_during_convergence_is_discarded_before_live() {
    let snapshot = agent_snapshot("episode-discard", AgentSessionReferenceKind::Id, "idle");
    let mut config = ScopedHerdrConfig::snapshots(vec![snapshot.clone(), snapshot]);
    config.snapshot_delay = Duration::from_millis(250);
    let mock = ScopedHerdr::start(config).await.unwrap();
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;

    mock.push_primary(resnapshot_anomaly()).await.unwrap();
    wait_until(|| mock.snapshot_requests() == 2).await;
    mock.push_enrichment_many(
        (0..10)
            .map(|_| agent_status_push("w1:p1", "term_6583d08d791e41", "working"))
            .collect(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    mock.push_primary(push(
        "workspace_focused",
        json!({"type":"workspace_focused", "workspace_id":"w1"}),
    ))
    .await
    .unwrap();
    wait_until(|| {
        serde_json::to_value(handle.diagnostics.borrow().clone()).unwrap()
            ["enrichment_counters"]["episode_discards"]
            .as_u64()
            .is_some_and(|discards| discards >= 10)
    })
    .await;
    assert!(
        handle.model.borrow().executions().any(|execution| {
            !execution.state.is_terminal() && execution.state == ExecState::Idle
        })
    );

    shutdown(handle, lifecycle).await;
    let connection = Connection::open(database_path(&root)).unwrap();
    let rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE source_event_type = 'pane_agent_status_changed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn pane_updated_fallback_converges_when_enrichment_is_unavailable() {
    let snapshot = agent_snapshot("fallback-only", AgentSessionReferenceKind::Id, "idle");
    let mut config = ScopedHerdrConfig::snapshots(vec![snapshot]);
    config.reject_all_enrichment = true;
    let mock = ScopedHerdr::start(config).await.unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() >= 2).await;

    mock.push_primary(push(
        "pane_updated",
        json!({
            "type":"pane_updated",
            "pane": agent_pane_value(
                "w1:p1",
                "term_6583d08d791e41",
                "w1",
                "w1:t1",
                "fallback-only"
            )
        }),
    ))
    .await
    .unwrap();
    wait_execution_state(&handle, ExecState::Working).await;
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    assert_eq!(*handle.quality.borrow(), ObservationQuality::Live);
    assert_eq!(mock.snapshot_requests(), 2);

    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn terminal_reconciling_keeps_discarding_enrichment_at_the_driven_rate() {
    let snapshot = agent_snapshot("terminal-discard", AgentSessionReferenceKind::Id, "idle");
    let mut config = ScopedHerdrConfig::snapshots(vec![snapshot; 5]);
    config.snapshot_delay = Duration::from_millis(100);
    let mock = ScopedHerdr::start(config).await.unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;

    for expected_snapshot in 2..=4 {
        mock.push_primary(resnapshot_anomaly()).await.unwrap();
        wait_until(|| mock.snapshot_requests() == expected_snapshot).await;
    }
    mock.push_primary(resnapshot_anomaly()).await.unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Reconciling).await;

    for _ in 0..3 {
        mock.push_enrichment(agent_status_push("w1:p1", "term_6583d08d791e41", "working"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        mock.push_primary(push(
            "workspace_focused",
            json!({"type":"workspace_focused", "workspace_id":"w1"}),
        ))
        .await
        .unwrap();
    }
    wait_until(|| {
        serde_json::to_value(handle.diagnostics.borrow().clone()).unwrap()
            ["enrichment_counters"]["episode_discards"]
            .as_u64()
            .is_some_and(|discards| discards >= 3)
    })
    .await;
    assert_eq!(*handle.quality.borrow(), ObservationQuality::Reconciling);

    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn agentless_snapshot_ends_execution_and_status_cannot_restore_provider_identity() {
    let present = agent_snapshot("stale-member", AgentSessionReferenceKind::Id, "idle");
    let mut agentless = present.clone();
    agentless["panes"][0]["agent"] = Value::Null;
    agentless["panes"][0]["agent_status"] = json!("unknown");
    agentless["panes"][0]["agent_session"] = Value::Null;
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![present, agentless]))
        .await
        .unwrap();
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;

    mock.push_primary(resnapshot_anomaly()).await.unwrap();
    wait_until(|| mock.snapshot_requests() == 2).await;
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| {
        handle
            .model
            .borrow()
            .executions()
            .all(|execution| execution.state.is_terminal())
    })
    .await;
    mock.push_enrichment(agent_status_push("w1:p1", "term_6583d08d791e41", "working"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(handle.model.borrow().pane("w1:p1").is_some());
    assert!(
        handle
            .model
            .borrow()
            .executions()
            .all(|execution| execution.state.is_terminal())
    );

    shutdown(handle, lifecycle).await;
    let connection = Connection::open(database_path(&root)).unwrap();
    let rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE source_event_type = 'pane_agent_status_changed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn removed_pane_status_never_applies_outside_snapshot_target_set() {
    let mut present = agent_snapshot("grace-gate", AgentSessionReferenceKind::Id, "working");
    let survivor = pane_value("w1:p2", "terminal-2", "w1", "w1:t1");
    present["panes"]
        .as_array_mut()
        .unwrap()
        .push(survivor.clone());
    let mut missing = present.clone();
    missing["panes"] = json!([survivor]);
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![present, missing]))
        .await
        .unwrap();
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;
    mock.push_primary(resnapshot_anomaly()).await.unwrap();
    wait_until(|| mock.snapshot_requests() == 2).await;
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 2).await;
    let before = handle.performance.borrow().snapshot.admission_high_water;

    mock.push_enrichment(agent_status_push("w1:p1", "term_6583d08d791e41", "idle"))
        .await
        .expect("grace-remnant payload must be written to the active enrichment stream");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(handle.model.borrow().pane("w1:p1").is_none());
    assert!(
        handle
            .model
            .borrow()
            .executions()
            .all(|execution| { execution.pane_id != "w1:p1" || execution.state.is_terminal() })
    );
    assert_eq!(
        handle.performance.borrow().snapshot.admission_high_water,
        before
    );

    shutdown(handle, lifecycle).await;
    let connection = Connection::open(database_path(&root)).unwrap();
    let rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE source_event_type = 'pane_agent_status_changed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn equal_live_status_has_no_row_and_no_performance_admission() {
    let snapshot = agent_snapshot("equal-status", AgentSessionReferenceKind::Id, "working");
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![snapshot]))
        .await
        .unwrap();
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;
    let before = handle.performance.borrow().snapshot.admission_high_water;

    mock.push_enrichment(agent_status_push("w1:p1", "term_6583d08d791e41", "working"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after = handle.performance.borrow().snapshot.admission_high_water;
    assert_eq!(after, before);

    shutdown(handle, lifecycle).await;
    let connection = Connection::open(database_path(&root)).unwrap();
    let rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE source_event_type = 'pane_agent_status_changed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn collector_shutdown_joins_enrichment_reader_and_stops_post_shutdown_sends() {
    let mock = ScopedHerdr::start(ScopedHerdrConfig::snapshots(vec![p1_snapshot()]))
        .await
        .unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;

    shutdown(handle, lifecycle).await;
    wait_until(|| mock.enrichment_closures() == 1).await;
    assert!(
        mock.push_enrichment(agent_status_push("w1:p1", "term_6583d08d791e41", "working"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sustained_enrichment_does_not_cancel_snapshot_or_replay_convergence_futures() {
    let snapshot = agent_snapshot("liveness", AgentSessionReferenceKind::Id, "idle");
    let mut config = ScopedHerdrConfig::snapshots(vec![snapshot.clone(), snapshot]);
    config.snapshot_delay = Duration::from_millis(150);
    let mock = ScopedHerdr::start(config).await.unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| mock.enrichment_subscriptions() == 1).await;

    mock.push_primary(resnapshot_anomaly()).await.unwrap();
    wait_until(|| mock.snapshot_requests() == 2).await;
    mock.push_enrichment_many(
        (0..40)
            .map(|index| {
                agent_status_push(
                    "w1:p1",
                    "term_6583d08d791e41",
                    if index % 2 == 0 { "working" } else { "blocked" },
                )
            })
            .collect(),
    )
    .await
    .unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(mock.snapshot_requests(), 2);

    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn i4_operator_coverage_only_change_wakes_diagnostics() {
    let herdr_directory = tempfile::tempdir().unwrap();
    let absent_socket = herdr_directory.path().join("absent.sock");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let handle = collector::spawn(absent_socket, test_session(), empty_restored(), writer)
        .await
        .unwrap();
    let mut model = handle.model.clone();
    let mut diagnostics = handle.diagnostics.clone();
    let _ = model.borrow_and_update();
    let _ = diagnostics.borrow_and_update();

    wait_diagnostic_source(
        &mut diagnostics,
        herdr_top::diagnostics::DiagnosticSource::Herdr,
        herdr_top::diagnostics::InputAvailability::Unavailable,
    )
    .await;
    assert!(
        !model.has_changed().unwrap(),
        "coverage-only publication must not require a model event"
    );

    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn subscribe_buffer_snapshot_replay_order() {
    let snapshot = p1_snapshot();
    let pane = pane_value("w1:p2", "terminal-2", "w1", "w1:t1");
    let pushes = vec![
        push(
            "pane_created",
            json!({"type": "pane_created", "pane": pane}),
        ),
        push(
            "pane_closed",
            json!({"type": "pane_closed", "pane_id": "w1:p2"}),
        ),
        push(
            "pane_created",
            json!({"type": "pane_created", "pane": pane_value("w1:p2", "terminal-2", "w1", "w1:t1")}),
        ),
    ];
    let closing_snapshot = snapshot_after_primary_hints(snapshot.clone(), &pushes);
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot, closing_snapshot])
            .generations(vec![pushes, vec![]]),
    )
    .await
    .expect("mock server should bind");
    let (directory, root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert!(handle.model.borrow().pane("w1:p2").is_some());
    let methods: Vec<_> = mock
        .requests()
        .into_iter()
        .filter_map(|request| request["method"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(
        methods.first().map(String::as_str),
        Some("events.subscribe")
    );
    assert_eq!(methods.get(1).map(String::as_str), Some("session.snapshot"));

    shutdown(handle, lifecycle).await;
    drop(directory);
    drop(root);
}

#[tokio::test]
async fn snapshot_refresh_replay_never_resurrects_absent_pane() {
    let authoritative = p1_snapshot();
    let mut config =
        ScopedHerdrConfig::snapshots(vec![authoritative.clone(), authoritative.clone()]);
    config.gate_snapshot_responses = true;
    let mock = ScopedHerdr::start(config).await.unwrap();
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .unwrap();

    let first = mock.next_snapshot_request().await;
    assert_eq!(first.index, 0);
    mock.push_primary(push(
        "pane_created",
        json!({
            "type": "pane_created",
            "pane": pane_value("w1:historical", "historical-terminal", "w1", "w1:t1"),
        }),
    ))
    .await
    .unwrap();
    first.release.send(()).unwrap();

    let closing = mock.next_snapshot_request().await;
    assert_eq!(closing.index, 1);
    closing.release.send(()).unwrap();
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert!(handle.model.borrow().pane("w1:historical").is_none());
    assert_eq!(mock.snapshot_requests(), 2);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn replay_drains_admitted_events_before_closed_end() {
    let final_sid = "replay-drain-final";
    let generation: Vec<_> = ["replay-drain-first", "replay-drain-second", final_sid]
        .into_iter()
        .map(|sid| {
            push(
                "pane_updated",
                json!({
                    "type": "pane_updated",
                    "pane": agent_pane_value(
                        "w1:p1",
                        "term_6583d08d791e41",
                        "w1",
                        "w1:t1",
                        sid,
                    ),
                }),
            )
        })
        .collect();
    let initial = p1_snapshot();
    let closing_snapshot = snapshot_after_primary_hints(initial.clone(), &generation);
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![initial, closing_snapshot])
            .generations(vec![generation])
            .close_after_snapshots(vec![1]),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, root, lifecycle, writer) = test_writer();
    let handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");

    let mut performance = handle.performance.clone();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = performance.borrow().snapshot.clone();
            if snapshot.admission_high_water == 3
                && snapshot.completion_high_water == 3
                && snapshot.pending_events == 0
            {
                break;
            }
            performance
                .changed()
                .await
                .expect("performance monitor should remain available");
        }
    })
    .await
    .expect("all admitted replay events should complete");
    let expected_key = RunKey::Native {
        provider: Provider::Codex,
        sid: final_sid.to_owned(),
    };
    tokio::time::timeout(WAIT, async {
        while mock.snapshot_requests() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "closing snapshot request did not arrive; observed {}",
            mock.snapshot_requests()
        )
    });
    assert_eq!(mock.snapshot_requests(), 2);
    tokio::time::timeout(WAIT, async {
        loop {
            if handle
                .model
                .borrow()
                .task_run_by_key(&expected_key)
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("closing snapshot identity did not reach the model");
    let in_memory = handle
        .model
        .borrow()
        .task_run_by_key(&expected_key)
        .is_some();

    shutdown(handle, lifecycle).await;
    let restored = open_reader(&root)
        .expect("reader should reopen after shutdown")
        .load_restored_state()
        .expect("persisted state should restore");
    let persisted = restored.model.task_run_by_key(&expected_key).is_some();

    assert!(
        in_memory,
        "the closing authoritative snapshot must reach the in-memory model"
    );
    assert!(
        persisted,
        "the closing authoritative snapshot must reach durable storage before EOF"
    );
}

#[tokio::test]
async fn anomaly_triggers_fresh_generation_resnapshot() {
    let snapshot = p1_snapshot();
    let anomaly = push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": pane_value("ghost:p1", "ghost-terminal", "ghost", "ghost:t1")}),
    );
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot.clone(), snapshot])
            .generations(vec![vec![anomaly], vec![]]),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert_eq!(mock.snapshot_requests(), 2);
    assert_eq!(
        mock.requests()
            .iter()
            .filter(|request| {
                request["method"] == "events.subscribe"
                    && request["params"]["subscriptions"]
                        .as_array()
                        .is_some_and(|subscriptions| {
                            subscriptions.iter().all(|subscription| {
                                subscription["type"] != "pane.agent_status_changed"
                            })
                        })
            })
            .count(),
        1,
        "an in-place resnapshot must not reconnect the primary subscription"
    );
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn later_closure_coalesces_to_one_refresh_without_raw_mutation() {
    let snapshot = p1_snapshot();
    let generation = vec![
        push(
            "pane_updated",
            json!({"type": "pane_updated", "pane": pane_value("ghost:p1", "ghost-terminal", "ghost", "ghost:t1")}),
        ),
        push(
            "pane_closed",
            json!({"type": "pane_closed", "pane_id": "ghost:p1"}),
        ),
    ];
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot.clone(), snapshot])
            .generations(vec![generation, vec![]]),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert_eq!(mock.snapshot_requests(), 2);
    assert!(handle.model.borrow().pane("ghost:p1").is_none());
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn three_attempts_then_stays_reconciling() {
    let snapshot = p1_snapshot();
    let anomaly = resnapshot_anomaly();
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot; 4])
            .generations(vec![vec![anomaly]; 4]),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_until(|| mock.snapshot_requests() == 4).await;
    tokio::time::sleep(Duration::from_millis(25)).await;

    assert_eq!(*handle.quality.borrow(), ObservationQuality::Reconciling);
    assert_eq!(mock.subscription_connections(), 1);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn overflow_recovery_shares_immediate_attempt_budget_without_retiring_execution() {
    let sid = "overflow-native-session";
    let snapshot = agent_snapshot(sid, AgentSessionReferenceKind::Id, "working");
    let flood: Vec<_> = (0..100)
        .map(|_| {
            push(
                "workspace_focused",
                json!({"type": "workspace_focused", "workspace_id": "w1"}),
            )
        })
        .collect();
    let anomaly = resnapshot_anomaly();
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot; 5])
            .generations(vec![
                flood,
                vec![anomaly.clone()],
                vec![anomaly.clone()],
                vec![anomaly],
            ]),
    )
    .await
    .expect("scripted mock should bind");
    let (restored, seed, run_id) = persisted_native_restored(sid);
    let (_directory, _root, lifecycle, writer) = test_writer_seeded(seed);

    let handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        restored,
        writer,
    )
    .await
    .expect("collector should start");
    wait_until(|| handle.primary_stream_counters().suppressed_topology_frames == 3).await;

    // One second spans 200 five-millisecond drain-quiet periods while remaining
    // thirty times shorter than this integration path's default watchdog. An
    // independent fourth immediate slot would therefore accept request five;
    // the quiescent monitor must not.
    let fifth_request = tokio::time::timeout(Duration::from_secs(1), async {
        while mock.snapshot_requests() < 5 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        fifth_request.is_err(),
        "overflow recovery exposed an independent fourth immediate request slot"
    );

    let model = handle.model.borrow();
    assert_eq!(mock.snapshot_requests(), 4);
    assert_eq!(*handle.quality.borrow(), ObservationQuality::Reconciling);
    assert_eq!(
        handle
            .primary_stream_counters()
            .event_triggered_topology_refreshes,
        2,
        "overflow recovery is not hint-caused, and the third anomaly must stop at quiescence"
    );
    assert_eq!(
        model
            .executions()
            .filter(|execution| execution.task_run_id == run_id && execution.state.is_terminal())
            .count(),
        1
    );
    assert_eq!(
        model
            .executions()
            .filter(|execution| execution.task_run_id == run_id && !execution.state.is_terminal())
            .count(),
        1,
        "active-subscription resnapshots must not retire the fresh execution"
    );
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn gap_retires_pre_gap_executions_all_three_kinds() {
    let sid = "all-gap-kinds";
    let snapshot = agent_snapshot(sid, AgentSessionReferenceKind::Id, "working");
    let mut mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot; 3])
            .generations(vec![vec![]; 3])
            .close_after_snapshots(vec![0]),
    )
    .await
    .expect("scripted mock should bind");
    let (restored, seed, run_id) = persisted_native_restored(sid);
    let (_directory, root, lifecycle, writer) = test_writer_seeded(seed);
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        restored,
        writer,
    )
    .await
    .expect("collector should start");

    wait_until(|| mock.snapshot_requests() >= 2).await;
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    // The startup row for the old pane stays ended, while an unchanged occupant reuses the same
    // live execution across reconnect and socket-replacement gaps.
    assert_execution_generations(&handle, run_id, 1, 1);

    mock.replace_socket()
        .await
        .expect("scripted socket should be replaced at the same path");
    wait_until(|| mock.snapshot_requests() >= 3).await;
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    assert_execution_generations(&handle, run_id, 1, 1);

    shutdown(handle, lifecycle).await;
    let connection = Connection::open(database_path(&root)).expect("database should open");
    for expected in ["startup", "reconnect", "socket_replacement"] {
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE gap_kind = ?1",
                [expected],
                |row| row.get(0),
            )
            .expect("gap marker count should query");
        assert_eq!(count, 1, "collector should attest {expected} exactly once");
    }
}

#[test]
fn attach_requires_equal_nonempty_identity() {
    let (restored, _seed, run_id) = persisted_native_restored("same-sid");
    let (mut reducer, shared) = Reducer::new(restored);
    reducer
        .reconcile_gap(ReconcileBatch {
            topology: topology_with_session("same-sid", AgentSessionReferenceKind::Id, "working"),
            gap_kind: GapKind::Startup,
        })
        .unwrap();
    assert!(shared.borrow().executions().any(|execution| {
        execution.execution_id != "pre-gap-execution"
            && execution.task_run_id == run_id
            && !execution.state.is_terminal()
    }));

    let (restored, _seed, run_id) = persisted_native_restored("nonempty-sid");
    let (mut reducer, shared) = Reducer::new(restored);
    reducer
        .reconcile_gap(ReconcileBatch {
            topology: topology_with_session("", AgentSessionReferenceKind::Id, "working"),
            gap_kind: GapKind::Startup,
        })
        .unwrap();
    assert!(!shared.borrow().executions().any(|execution| {
        execution.execution_id != "pre-gap-execution"
            && execution.task_run_id == run_id
            && !execution.state.is_terminal()
    }));
}

#[test]
fn path_kind_never_corroborates() {
    let (restored, _seed, run_id) = persisted_native_restored("same-text");
    let (mut reducer, shared) = Reducer::new(restored);
    reducer
        .reconcile_gap(ReconcileBatch {
            topology: topology_with_session(
                "same-text",
                AgentSessionReferenceKind::Path,
                "working",
            ),
            gap_kind: GapKind::Startup,
        })
        .unwrap();

    assert!(!shared.borrow().executions().any(|execution| {
        execution.execution_id != "pre-gap-execution"
            && execution.task_run_id == run_id
            && !execution.state.is_terminal()
    }));
}

#[tokio::test]
async fn restored_synthesized_idle_attaches_truthfully() {
    let sid = "probe-first-9101";
    let post = fixture_payloads("p6-cold-restart.jsonl", "POST", "recv")
        .pop()
        .expect("p6 fixture should have POST snapshot")["result"]["snapshot"]
        .clone();
    let mock =
        MockHerdr::start(MockConfig::default().respond("session.snapshot", snapshot_result(post)))
            .await
            .expect("mock server should bind");
    let (restored, seed, run_id) = persisted_native_restored(sid);
    let (_directory, _root, lifecycle, writer) = test_writer_seeded(seed);

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        restored,
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert!(handle.model.borrow().executions().any(|execution| {
        execution.task_run_id == run_id
            && execution.execution_id != "pre-gap-execution"
            && execution.state == ExecState::Idle
    }));
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn terminal_id_preserved_across_move_replay() {
    let snapshot = p1_snapshot();
    let pushes: Vec<_> = fixture_payloads("p4-terminal-id-move.jsonl", "B2", "recv")
        .into_iter()
        .filter(|payload| payload.get("event").is_some())
        .collect();
    let expected_terminal = snapshot["panes"][0]["terminal_id"]
        .as_str()
        .expect("fixture terminal id should be a string")
        .to_owned();
    let closing_snapshot = snapshot_after_primary_hints(snapshot.clone(), &pushes);
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot, closing_snapshot])
            .generations(vec![pushes, vec![]]),
    )
    .await
    .expect("mock server should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert_eq!(
        handle
            .model
            .borrow()
            .pane("w2:p2")
            .expect("moved pane should use its new public id")
            .terminal_id,
        expected_terminal
    );
    assert!(handle.model.borrow().pane("w1:p1").is_none());
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn live_only_after_clean_drain() {
    let snapshot = p1_snapshot();
    let anomaly = resnapshot_anomaly();
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot.clone(), snapshot])
            .generations(vec![vec![anomaly], vec![]])
            .snapshot_delay(Duration::from_millis(75)),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_until(|| mock.snapshot_requests() == 1).await;
    assert_eq!(*handle.quality.borrow(), ObservationQuality::Reconciling);
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    assert_eq!(mock.snapshot_requests(), 2);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn owner_location_refreshed_on_move() {
    let snapshot = p1_snapshot();
    let terminal_id = snapshot["panes"][0]["terminal_id"]
        .as_str()
        .expect("terminal id should exist")
        .to_owned();
    let pushes: Vec<_> = fixture_payloads("p4-terminal-id-move.jsonl", "B2", "recv")
        .into_iter()
        .filter(|payload| payload.get("event").is_some())
        .collect();
    let closing_snapshot = snapshot_after_primary_hints(snapshot.clone(), &pushes);
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot, closing_snapshot])
            .generations(vec![pushes, vec![]]),
    )
    .await
    .expect("mock server should bind");
    let (_directory, root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    shutdown(handle, lifecycle).await;

    let reader = open_reader(&root).expect("reader should reopen after shutdown");
    let owner = reader
        .read_owner()
        .expect("owner read should succeed")
        .expect("owner row should exist");
    assert_eq!(owner.terminal_id.as_deref(), Some(terminal_id.as_str()));
    assert_eq!(owner.pane_id.as_deref(), Some("w2:p2"));
}

#[tokio::test]
async fn owner_replace_committed_before_subscription() {
    let mock = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", snapshot_result(p1_snapshot())),
    )
    .await
    .expect("mock server should bind");
    let (_directory, root, lifecycle, writer) = test_writer();

    let handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    let reader = open_reader(&root).expect("reader should see committed owner");
    assert!(
        reader
            .read_owner()
            .expect("owner query should succeed")
            .is_some()
    );
    wait_until(|| mock.accepted_connections() > 0).await;
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn owner_replace_failure_is_startup_error() {
    let mock = MockHerdr::start(MockConfig::default())
        .await
        .expect("mock server should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    lifecycle
        .shutdown()
        .await
        .expect("writer should shut down cleanly");

    let result = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await;

    assert!(result.is_err());
    assert_eq!(mock.accepted_connections(), 0);
}

#[tokio::test]
async fn snapshot_maps_to_topology() {
    let mock = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", snapshot_result(p1_snapshot())),
    )
    .await
    .expect("mock server should bind");
    let (_directory, root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    assert!(handle.model.borrow().workspace("w1").is_some());
    assert!(handle.model.borrow().tab("w1:t1").is_some());
    assert_eq!(
        handle
            .model
            .borrow()
            .pane("w1:p1")
            .expect("mapped pane should exist")
            .terminal_id,
        "term_6583d08d791e41"
    );
    shutdown(handle, lifecycle).await;

    let connection = Connection::open(database_path(&root)).expect("database should open");
    let startup_gaps: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE gap_kind = 'startup'",
            [],
            |row| row.get(0),
        )
        .expect("gap count should query");
    assert_eq!(startup_gaps, 1, "startup gap must be collector-attested");
}

#[tokio::test]
async fn writer_barrier_orders_assertions() {
    let (_directory, root, lifecycle, mut writer) = test_writer();
    let writer = &mut writer;
    writer
        .apply(vec![PersistOp::UpsertWorkspace {
            workspace: herdr_top::model::Workspace {
                workspace_id: "barrier-workspace".to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
        }])
        .await
        .expect("write should commit");
    writer.barrier().await.expect("barrier should acknowledge");

    let reader = open_reader(&root).expect("reader should open while writer is alive");
    assert!(
        reader
            .load_restored_state()
            .expect("state should restore")
            .model
            .workspace("barrier-workspace")
            .is_some()
    );
    drop(reader);
    lifecycle.shutdown().await.expect("writer should shut down");
}

#[tokio::test]
async fn periodic_cleanup_after_ingestion() {
    let (_directory, root, lifecycle, mut writer) = test_writer();
    let writer = &mut writer;
    writer
        .apply(vec![PersistOp::RecordCollectorGap(CollectorGap {
            event_id: "ancient-gap".to_owned(),
            herdr_session: "cleanup-session".to_owned(),
            seen_at_ms: 0,
            kind: GapKind::Startup,
        })])
        .await
        .expect("ancient event should first commit");
    writer.barrier().await.expect("first barrier should commit");

    tokio::time::sleep(Duration::from_millis(300)).await;
    writer
        .apply(Vec::new())
        .await
        .expect("later ingestion should run periodic cleanup");
    writer
        .barrier()
        .await
        .expect("cleanup barrier should commit");
    assert_eq!(event_count(&root), 0);

    lifecycle.shutdown().await.expect("writer should shut down");
}

#[tokio::test]
async fn i4_d3_cleanup_failure_keeps_collector_alive() {
    let snapshot = p1_snapshot();
    let later_pane = pane_value("w1:p2", "terminal-2", "w1", "w1:t1");
    let pane_created = push(
        "pane_created",
        json!({"type": "pane_created", "pane": later_pane}),
    );
    let closing_snapshot =
        snapshot_after_primary_hints(snapshot.clone(), std::slice::from_ref(&pane_created));
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot, closing_snapshot])
            .generations(vec![vec![pane_created], vec![]]),
    )
    .await
    .expect("mock server should bind");
    let ancient = PersistOp::RecordCollectorGap(CollectorGap {
        event_id: "i4-ancient-gap".to_owned(),
        herdr_session: test_session(),
        seen_at_ms: 0,
        kind: GapKind::Startup,
    });
    let (_directory, root, lifecycle, writer) = test_writer_configured(vec![ancient], |root| {
        Connection::open(database_path(root))
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER i4_fail_cleanup BEFORE DELETE ON events \
                 BEGIN SELECT RAISE(ABORT, 'PRIVATE_CLEANUP_TEXT_701A'); END;",
            )
            .unwrap();
    });
    let mut persistence = writer.subscribe_persistence();

    let handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    let mut diagnostics = handle.diagnostics.clone();
    wait_model_pane(&handle.model, "w1:p2").await;
    wait_for_persistence_degradation(&mut persistence, &mut diagnostics).await;
    assert!(handle.model.borrow().pane("w1:p2").is_some());
    handle
        .stop()
        .await
        .expect("collector should remain stoppable");
    tokio::time::timeout(Duration::from_secs(3), lifecycle.shutdown())
        .await
        .expect("writer shutdown timed out")
        .expect("writer should checkpoint after cleanup degradation");
    let reopened = open_reader(&root).unwrap().load_restored_state().unwrap();
    assert!(
        reopened.model.pane("w1:p1").is_some(),
        "post-Apply cleanup failure must preserve the committed snapshot batch"
    );
    assert!(
        reopened.model.pane("w1:p2").is_none(),
        "the later in-memory event must be skipped by persistence"
    );
}

#[tokio::test]
async fn i4_d3_herdr_transitions_refresh_consolidated_diagnostics() {
    let herdr_dir = tempfile::tempdir().unwrap();
    let herdr_path = herdr_dir.path().join("herdr.sock");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let handle = collector::spawn(herdr_path.clone(), test_session(), empty_restored(), writer)
        .await
        .expect("collector should start before Herdr is available");
    let mut diagnostics = handle.diagnostics.clone();

    wait_diagnostic_source(
        &mut diagnostics,
        herdr_top::diagnostics::DiagnosticSource::Herdr,
        herdr_top::diagnostics::InputAvailability::Unavailable,
    )
    .await;

    let herdr_task = spawn_static_herdr(&herdr_path, p1_snapshot());
    wait_diagnostic_source(
        &mut diagnostics,
        herdr_top::diagnostics::DiagnosticSource::Herdr,
        herdr_top::diagnostics::InputAvailability::Available,
    )
    .await;

    shutdown(handle, lifecycle).await;
    herdr_task.abort();
}

#[tokio::test]
async fn i4_d3_apply_failure_keeps_in_memory_model_advancing() {
    let snapshot = p1_snapshot();
    let pushes = vec![
        push(
            "pane_created",
            json!({"type": "pane_created", "pane": pane_value("w1:p2", "terminal-2", "w1", "w1:t1")}),
        ),
        push(
            "pane_created",
            json!({"type": "pane_created", "pane": pane_value("w1:p3", "terminal-3", "w1", "w1:t1")}),
        ),
    ];
    let closing_snapshot = snapshot_after_primary_hints(snapshot.clone(), &pushes);
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot, closing_snapshot])
            .generations(vec![pushes, vec![]]),
    )
    .await
    .expect("mock server should bind");
    let (_directory, root, lifecycle, writer) = test_writer_configured(Vec::new(), |root| {
        Connection::open(database_path(root))
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER i4_fail_second_pane BEFORE INSERT ON panes \
                 WHEN NEW.pane_id = 'w1:p2' \
                 BEGIN SELECT RAISE(ABORT, 'PRIVATE_APPLY_TEXT_04E8'); END;",
            )
            .unwrap();
    });
    let mut persistence = writer.subscribe_persistence();

    let handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    let mut diagnostics = handle.diagnostics.clone();
    wait_model_pane(&handle.model, "w1:p3").await;
    wait_for_persistence_degradation(&mut persistence, &mut diagnostics).await;
    assert!(
        handle
            .operator
            .borrow()
            .activity
            .iter()
            .all(|item| item.durability != ActivityDurability::Durable),
        "the rejected authoritative snapshot batch must not be reported durable"
    );
    let model = handle.model.borrow();
    assert!(model.pane("w1:p2").is_some());
    assert!(model.pane("w1:p3").is_some());
    drop(model);
    handle
        .stop()
        .await
        .expect("collector should remain stoppable");
    tokio::time::timeout(Duration::from_secs(3), lifecycle.shutdown())
        .await
        .expect("writer shutdown timed out")
        .expect("writer should checkpoint after apply degradation");
    let reopened = open_reader(&root).unwrap().load_restored_state().unwrap();
    assert!(reopened.model.pane("w1:p2").is_none());
    assert!(reopened.model.pane("w1:p3").is_none());
}

#[tokio::test]
async fn fifty_pane_mock_smoke() {
    let mut snapshot = p1_snapshot();
    snapshot["panes"] = Value::Array(
        (1..=50)
            .map(|number| {
                pane_value(
                    &format!("w1:p{number}"),
                    &format!("terminal-{number}"),
                    "w1",
                    "w1:t1",
                )
            })
            .collect(),
    );
    snapshot["workspaces"][0]["pane_count"] = json!(50);
    snapshot["tabs"][0]["pane_count"] = json!(50);
    let mock = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", snapshot_result(snapshot)),
    )
    .await
    .expect("mock server should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert_eq!(handle.model.borrow().panes().count(), 50);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn authoritative_snapshot_new_sid_replaces_provisional_run() {
    let initial = agent_snapshot("", AgentSessionReferenceKind::Id, "working");
    let update = push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": agent_pane_value("w1:p1", "term_6583d08d791e41", "w1", "w1:t1", "promoted-sid")}),
    );
    let closing_snapshot =
        snapshot_after_primary_hints(initial.clone(), std::slice::from_ref(&update));
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![initial, closing_snapshot])
            .generations(vec![vec![update], vec![]]),
    )
    .await
    .expect("mock server should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let model = handle.model.borrow();
    let promoted = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: "promoted-sid".to_owned(),
        })
        .expect("authoritative snapshot must install the native run");
    assert_eq!(model.task_runs().count(), 2);
    assert!(
        model
            .task_runs()
            .filter(|run| run.run_id != promoted.run_id)
            .all(|run| run.state.is_terminal())
    );
    assert_eq!(
        model
            .executions()
            .filter(|execution| {
                execution.task_run_id == promoted.run_id && !execution.state.is_terminal()
            })
            .count(),
        1
    );
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn different_sid_same_terminal_starts_new_run() {
    let initial = agent_snapshot("old-sid", AgentSessionReferenceKind::Id, "working");
    let update = push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": agent_pane_value("w1:p1", "term_6583d08d791e41", "w1", "w1:t1", "new-sid")}),
    );
    let closing_snapshot =
        snapshot_after_primary_hints(initial.clone(), std::slice::from_ref(&update));
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![initial, closing_snapshot])
            .generations(vec![vec![update], vec![]]),
    )
    .await
    .expect("mock server should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let model = handle.model.borrow();
    let old_run = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: "old-sid".to_owned(),
        })
        .unwrap();
    let new_run = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: "new-sid".to_owned(),
        })
        .unwrap();
    assert_ne!(old_run.run_id, new_run.run_id);
    assert!(old_run.display_ordinal < new_run.display_ordinal);
    assert!(
        model
            .executions()
            .filter(|execution| execution.task_run_id == old_run.run_id)
            .all(|execution| execution.state.is_terminal())
    );
    assert_eq!(
        model
            .executions()
            .filter(|execution| {
                execution.terminal_id == "term_6583d08d791e41" && !execution.state.is_terminal()
            })
            .count(),
        1
    );
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn provider_transition_starts_new_run() {
    let initial = agent_snapshot("old-codex-sid", AgentSessionReferenceKind::Id, "working");
    let mut replacement = agent_pane_value(
        "w1:p1",
        "term_6583d08d791e41",
        "w1",
        "w1:t1",
        "new-claude-sid",
    );
    replacement["agent"] = json!("claude");
    replacement["agent_session"]["source"] = json!("herdr:claude");
    replacement["agent_session"]["agent"] = json!("claude");
    let update = push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": replacement}),
    );
    let closing_snapshot =
        snapshot_after_primary_hints(initial.clone(), std::slice::from_ref(&update));
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![initial, closing_snapshot])
            .generations(vec![vec![update], vec![]]),
    )
    .await
    .expect("mock server should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let model = handle.model.borrow();
    let old_run = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: "old-codex-sid".to_owned(),
        })
        .expect("the original Codex run should remain addressable")
        .run_id;
    let new_run = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Claude,
            sid: "new-claude-sid".to_owned(),
        })
        .expect("the Claude observation should create a new run")
        .run_id;
    assert_ne!(old_run, new_run);
    assert!(
        model
            .executions()
            .filter(|execution| execution.task_run_id == old_run)
            .all(|execution| execution.state.is_terminal())
    );
    assert_eq!(
        model
            .executions()
            .filter(|execution| execution.task_run_id == new_run && !execution.state.is_terminal())
            .count(),
        1
    );
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[test]
fn binding_conflict_returns_typed_dropped_result() {
    let owner_run_id = RunId::new();
    let claimant_run_id = RunId::new();
    let mut model = DomainModel::default();
    model.insert_task_run(TaskRun {
        run_id: owner_run_id,
        key: RunKey::Controller("owner-controller-run".to_owned()),
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
            provider: Provider::Codex,
            sid: "bound-codex-sid".to_owned(),
        },
        owner_run_id,
    );
    let (mut reducer, mut shared) = Reducer::new(RestoredState {
        model,
        next_ordinal: 2,
        next_ingest_seq: Some(1),
        event_ledger: Vec::new(),
    });
    let _ = shared.borrow_and_update();
    let mut event_metadata = identity_metadata("binding-conflict", "task_started");
    event_metadata.source = "controller".to_owned();
    event_metadata.task_run_id = Some(claimant_run_id);
    event_metadata.task_state = Some(TaskState::Running);
    event_metadata.provider = Some(Provider::Codex);
    event_metadata.native_session_id = Some("bound-codex-sid".to_owned());

    let outcome = reducer
        .apply(NormalizedEvent::TopologyUpsert {
            metadata: event_metadata,
            authority: TopologyAuthority::Partial,
            entity: herdr_top::model::TopologyEntity::Workspace(Workspace {
                workspace_id: "must-roll-back".to_owned(),
            }),
        })
        .expect("binding conflicts should be typed non-fatal outcomes");

    assert!(matches!(
        outcome,
        ApplyOutcome::DroppedBindingConflict(MergeConflict::NativeSessionAlreadyBound {
            owner,
            claimant,
            ..
        }) if owner == owner_run_id && claimant == claimant_run_id
    ));
    assert!(shared.borrow().task_run(&claimant_run_id).is_none());
    assert!(shared.borrow().workspace("must-roll-back").is_none());
    assert!(!shared.has_changed().unwrap());

    let next_run_id = RunId::new();
    let mut next_metadata = identity_metadata("after-binding-conflict", "task_started");
    next_metadata.source = "controller".to_owned();
    next_metadata.task_run_id = Some(next_run_id);
    next_metadata.task_state = Some(TaskState::Running);
    reducer
        .apply(NormalizedEvent::TopologyUpsert {
            metadata: next_metadata,
            authority: TopologyAuthority::Partial,
            entity: herdr_top::model::TopologyEntity::Workspace(Workspace {
                workspace_id: "after-rollback".to_owned(),
            }),
        })
        .expect("a non-conflicting event should apply after the dropped conflict");

    assert_eq!(
        shared
            .borrow()
            .task_run(&next_run_id)
            .expect("the next run should be created")
            .display_ordinal,
        DisplayOrdinal::new(2)
    );
}

#[test]
fn hook_metadata_and_later_log_lifecycle_converge_one_run() {
    const ROLLOUT: &str = "77777777-7777-4777-8777-777777777777";
    let parent_run_id = RunId::new();
    let prerequisite_run_id = RunId::new();
    let child_run_id = RunId::new();
    let mut model = DomainModel::default();
    for (run_id, key, ordinal) in [
        (parent_run_id, "mixed-parent", 1),
        (prerequisite_run_id, "mixed-prerequisite", 2),
    ] {
        model.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller(key.to_owned()),
            display_ordinal: DisplayOrdinal::new(ordinal),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: Some(1),
            updated_at_ms: Some(1),
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
    }
    let (mut reducer, shared) = Reducer::new(RestoredState {
        model,
        next_ordinal: 3,
        next_ingest_seq: Some(1),
        event_ledger: Vec::new(),
    });

    let mut hook_started = identity_metadata("mixed-hook-started", "task_started");
    hook_started.timestamp_ms = 10;
    hook_started.receipt_time_ms = 10;
    hook_started.source = "hook:codex".to_owned();
    hook_started.provider = Some(Provider::Codex);
    hook_started.native_session_id = Some(ROLLOUT.to_owned());
    hook_started.task_run_id = Some(child_run_id);
    hook_started.task_state = Some(TaskState::Running);
    hook_started.label = Some("preserved hook subject".to_owned());
    assert!(matches!(
        reducer
            .apply(NormalizedEvent::ControllerEvent {
                metadata: hook_started,
                event: ControllerEventKind::TaskStarted,
            })
            .unwrap(),
        ApplyOutcome::Applied(_)
    ));

    let execution_edge = ExecutionEdge {
        parent_run_id,
        child_run_id,
    };
    let mut dispatch = identity_metadata("mixed-controller-dispatch", "dispatch");
    dispatch.timestamp_ms = 11;
    dispatch.receipt_time_ms = 11;
    dispatch.source = "controller".to_owned();
    dispatch.task_run_id = Some(child_run_id);
    dispatch.execution_parent = Some(execution_edge.clone());
    assert!(matches!(
        reducer
            .apply(NormalizedEvent::ControllerEvent {
                metadata: dispatch,
                event: ControllerEventKind::Dispatch {
                    parent_task_run_id: "mixed-parent".to_owned(),
                },
            })
            .unwrap(),
        ApplyOutcome::Applied(_)
    ));

    let dependency_edge = DependencyEdge {
        prerequisite_run_id,
        dependent_run_id: child_run_id,
    };
    let mut depends = identity_metadata("mixed-controller-dependency", "depends_on");
    depends.timestamp_ms = 12;
    depends.receipt_time_ms = 12;
    depends.source = "controller".to_owned();
    depends.task_run_id = Some(child_run_id);
    depends.dependency = Some(dependency_edge.clone());
    assert!(matches!(
        reducer
            .apply(NormalizedEvent::ControllerEvent {
                metadata: depends,
                event: ControllerEventKind::DependsOn {
                    depends_on_id: "mixed-prerequisite".to_owned(),
                },
            })
            .unwrap(),
        ApplyOutcome::Applied(_)
    ));

    let mut execution_metadata = identity_metadata("mixed-execution", "pane_agent_detected");
    execution_metadata.timestamp_ms = 13;
    execution_metadata.receipt_time_ms = 13;
    execution_metadata.provider = Some(Provider::Codex);
    execution_metadata.native_session_id = Some(ROLLOUT.to_owned());
    execution_metadata.task_run_id = Some(child_run_id);
    let execution = Execution {
        execution_id: "mixed-preserved-execution".to_owned(),
        pane_id: "w1:p1".to_owned(),
        terminal_id: "terminal-1".to_owned(),
        task_run_id: child_run_id,
        state: ExecState::Working,
    };
    assert!(matches!(
        reducer
            .apply(NormalizedEvent::ExecutionBegin {
                metadata: execution_metadata,
                execution: execution.clone(),
            })
            .unwrap(),
        ApplyOutcome::Applied(_)
    ));

    let mut cancelled = identity_metadata("mixed-log-cancelled", "cancelled");
    cancelled.timestamp_ms = 30;
    cancelled.receipt_time_ms = 300;
    cancelled.source = herdr_top::provider::lane::SOURCE_LOG_LANE.to_owned();
    cancelled.provider = Some(Provider::Codex);
    cancelled.native_session_id = Some(ROLLOUT.to_owned());
    cancelled.task_run_id = Some(child_run_id);
    cancelled.task_state = Some(TaskState::Cancelled);
    assert!(matches!(
        reducer
            .apply(NormalizedEvent::ControllerEvent {
                metadata: cancelled,
                event: ControllerEventKind::Cancelled,
            })
            .unwrap(),
        ApplyOutcome::Applied(_)
    ));
    {
        let snapshot = shared.borrow();
        let run = snapshot.task_run(&child_run_id).unwrap();
        assert_eq!(run.state, TaskState::Running);
        assert_eq!(run.finished_at_ms, None);
        assert_eq!(
            snapshot
                .task_run_v6_state(&child_run_id)
                .and_then(|state| state.native_session_end.as_ref())
                .map(|end| (end.status, end.at_ms)),
            Some((NativeSessionEndStatus::Cancelled, 30))
        );
    }

    let before_valid_start = {
        let snapshot = shared.borrow();
        (
            snapshot.task_runs().count(),
            snapshot.executions().count(),
            snapshot.controller_diagnostics().binding_conflicts(),
            snapshot
                .controller_diagnostics()
                .terminal_blocked_progress_noops(),
            snapshot
                .controller_diagnostics()
                .terminal_forward_reference_creations(),
            snapshot
                .controller_diagnostics()
                .unknown_lane_terminal_drops(),
            snapshot.provider_diagnostics().duplicate_events(),
        )
    };
    let mut later_start = identity_metadata("mixed-log-later-start", "task_started");
    later_start.timestamp_ms = 40;
    later_start.receipt_time_ms = 400;
    later_start.source = herdr_top::provider::lane::SOURCE_LOG_LANE.to_owned();
    later_start.provider = Some(Provider::Codex);
    later_start.native_session_id = Some(ROLLOUT.to_owned());
    later_start.task_run_id = Some(child_run_id);
    later_start.task_state = Some(TaskState::Running);
    assert!(matches!(
        reducer
            .apply(NormalizedEvent::ControllerEvent {
                metadata: later_start,
                event: ControllerEventKind::TaskStarted,
            })
            .unwrap(),
        ApplyOutcome::Applied(_)
    ));

    let snapshot = shared.borrow();
    let run = snapshot.task_run(&child_run_id).unwrap();
    assert_eq!(run.state, TaskState::Running);
    assert_eq!(run.finished_at_ms, None);
    assert!(
        snapshot
            .task_run_v6_state(&child_run_id)
            .and_then(|state| state.native_session_end.as_ref())
            .is_none()
    );
    assert_eq!(run.subject.as_deref(), Some("preserved hook subject"));
    assert_eq!(
        snapshot
            .task_run_by_key(&RunKey::Native {
                provider: Provider::Codex,
                sid: ROLLOUT.to_owned(),
            })
            .unwrap()
            .run_id,
        child_run_id
    );
    assert_eq!(
        snapshot.execution("mixed-preserved-execution"),
        Some(&execution)
    );
    assert!(
        snapshot
            .execution_edges()
            .any(|edge| edge == &execution_edge)
    );
    assert!(
        snapshot
            .dependency_edges()
            .any(|edge| edge == &dependency_edge)
    );
    assert_eq!(
        (
            snapshot.task_runs().count(),
            snapshot.executions().count(),
            snapshot.controller_diagnostics().binding_conflicts(),
            snapshot
                .controller_diagnostics()
                .terminal_blocked_progress_noops(),
            snapshot
                .controller_diagnostics()
                .terminal_forward_reference_creations(),
            snapshot
                .controller_diagnostics()
                .unknown_lane_terminal_drops(),
            snapshot.provider_diagnostics().duplicate_events(),
        ),
        before_valid_start,
        "valid mixed-source reopen must not add a run, execution, or conflict/duplicate diagnostic"
    );
}

#[tokio::test]
async fn authoritative_provider_move_replaces_prior_identity_without_raw_conflict() {
    let initial = agent_snapshot("bound-codex-sid", AgentSessionReferenceKind::Id, "working");
    let mut moved = agent_pane_value(
        "w1:p2",
        "term_6583d08d791e41",
        "w1",
        "w1:t1",
        "different-claude-sid",
    );
    moved["agent"] = json!("claude");
    moved["agent_session"]["source"] = json!("herdr:claude");
    moved["agent_session"]["agent"] = json!("claude");
    let moved_frame = push(
        "pane_moved",
        json!({
            "type": "pane_moved",
            "previous_pane_id": "w1:p1",
            "pane": moved,
        }),
    );
    let closing_snapshot =
        snapshot_after_primary_hints(initial.clone(), std::slice::from_ref(&moved_frame));
    let mock = LiveHerdr::start(LiveConfig::default().snapshots(vec![initial, closing_snapshot]))
        .await
        .expect("live mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let (execution_id, old_run_id) = {
        let model = handle.model.borrow();
        let execution = model
            .executions()
            .next()
            .expect("snapshot should create one execution");
        (execution.execution_id.clone(), execution.task_run_id)
    };
    mock.push(moved_frame)
        .await
        .expect("conflicting pane move should be delivered");
    wait_until(|| mock.snapshot_requests() == 2).await;
    wait_model_pane(&handle.model, "w1:p2").await;
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    assert_eq!(*handle.quality.borrow(), ObservationQuality::Live);
    let model = handle.model.borrow();
    let execution = model
        .execution(&execution_id)
        .expect("the original execution should remain as history");
    assert_eq!(execution.task_run_id, old_run_id);
    assert_eq!(execution.pane_id, "w1:p1");
    assert!(execution.state.is_terminal());
    let replacement = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Claude,
            sid: "different-claude-sid".to_owned(),
        })
        .expect("the authoritative snapshot should install the replacement identity");
    assert!(model.executions().any(|execution| {
        execution.task_run_id == replacement.run_id
            && execution.pane_id == "w1:p2"
            && execution.state == ExecState::Working
    }));
    assert!(model.pane("w1:p1").is_none());
    assert!(model.pane("w1:p2").is_some());
    assert_eq!(model.controller_diagnostics().binding_conflicts(), 0);
    assert_eq!(model.task_runs().count(), 2);
    drop(model);

    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn different_sid_replacement_publishes_once() {
    let old_run_id = RunId::new();
    let new_run_id = RunId::new();
    let mut model = DomainModel::default();
    model.insert_task_run(TaskRun {
        run_id: old_run_id,
        key: RunKey::Native {
            provider: Provider::Codex,
            sid: "publication-old-sid".to_owned(),
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
    model.insert_execution(Execution {
        execution_id: "publication-old-execution".to_owned(),
        pane_id: "w1:p1".to_owned(),
        terminal_id: "terminal-1".to_owned(),
        task_run_id: old_run_id,
        state: ExecState::Working,
    });
    let (mut reducer, mut shared) = Reducer::new(RestoredState {
        model,
        next_ordinal: 2,
        next_ingest_seq: Some(1),
        event_ledger: Vec::new(),
    });
    let _ = shared.borrow_and_update();
    let mut begin_metadata = identity_metadata("replacement-begin", "pane_updated");
    begin_metadata.provider = Some(Provider::Codex);
    begin_metadata.native_session_id = Some("publication-new-sid".to_owned());

    reducer
        .apply_observation(vec![
            NormalizedEvent::ExecutionEnd {
                metadata: identity_metadata("replacement-end", "pane_updated"),
                execution_id: "publication-old-execution".to_owned(),
            },
            NormalizedEvent::ExecutionBegin {
                metadata: begin_metadata,
                execution: Execution {
                    execution_id: "publication-new-execution".to_owned(),
                    pane_id: "w1:p1".to_owned(),
                    terminal_id: "terminal-1".to_owned(),
                    task_run_id: new_run_id,
                    state: ExecState::Working,
                },
            },
        ])
        .expect("replacement observation should apply atomically");

    shared
        .changed()
        .await
        .expect("replacement should publish one complete observation");
    let published = shared.borrow_and_update();
    assert_eq!(
        published
            .execution("publication-old-execution")
            .expect("old execution should remain as history")
            .state,
        ExecState::Ended
    );
    assert_eq!(
        published
            .execution("publication-new-execution")
            .expect("new execution should be visible in the same publication")
            .state,
        ExecState::Working
    );
    drop(published);
    assert!(!shared.has_changed().unwrap());
}

#[tokio::test]
async fn authoritative_sid_snapshot_replaces_provisional_without_raw_agent_payload() {
    let initial = agent_snapshot("", AgentSessionReferenceKind::Id, "blocked");
    let mut sid_only = agent_pane_value(
        "w1:p1",
        "term_6583d08d791e41",
        "w1",
        "w1:t1",
        "promoted-without-agent",
    );
    sid_only
        .as_object_mut()
        .expect("pane fixture should be an object")
        .remove("agent");
    sid_only["agent_status"] = json!("blocked");
    let update = push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": sid_only}),
    );
    let closing_snapshot =
        snapshot_after_primary_hints(initial.clone(), std::slice::from_ref(&update));
    let mock = LiveHerdr::start(LiveConfig::default().snapshots(vec![initial, closing_snapshot]))
        .await
        .expect("live mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let (provisional_run_id, provisional_execution_id) = {
        let model = handle.model.borrow();
        let provisional = model
            .task_runs()
            .next()
            .expect("snapshot should create the provisional run");
        assert!(matches!(provisional.key, RunKey::Provisional { .. }));
        let execution = model
            .executions()
            .next()
            .expect("snapshot should create the execution");
        (provisional.run_id, execution.execution_id.clone())
    };

    mock.push(update)
        .await
        .expect("SID-only push should deliver");
    wait_until(|| {
        handle
            .model
            .borrow()
            .task_run_by_key(&RunKey::Native {
                provider: Provider::Codex,
                sid: "promoted-without-agent".to_owned(),
            })
            .is_some()
    })
    .await;

    let model = handle.model.borrow();
    let promoted = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: "promoted-without-agent".to_owned(),
        })
        .expect("SID-only push should promote the existing provisional run");
    assert_ne!(promoted.run_id, provisional_run_id);
    assert_eq!(model.task_runs().count(), 2);
    let executions: Vec<_> = model.executions().collect();
    assert_eq!(executions.len(), 2);
    assert!(executions.iter().any(|execution| {
        execution.execution_id == provisional_execution_id && execution.state.is_terminal()
    }));
    assert!(executions.iter().any(|execution| {
        execution.task_run_id == promoted.run_id && execution.state == ExecState::Blocked
    }));
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn successive_authoritative_sid_snapshots_retain_history_and_restore_established_run() {
    let established = "established-binding";
    let initial = agent_snapshot(established, AgentSessionReferenceKind::Id, "working");
    let mut stale = agent_pane_value(
        "w1:p1",
        "term_6583d08d791e41",
        "w1",
        "w1:t1",
        "stale-latched-sid",
    );
    stale
        .as_object_mut()
        .expect("pane fixture should be an object")
        .remove("agent");
    let stale_update = push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": stale}),
    );
    let mut follow_up =
        agent_pane_value("w1:p1", "term_6583d08d791e41", "w1", "w1:t1", established);
    follow_up
        .as_object_mut()
        .expect("pane fixture should be an object")
        .insert("agent_status".to_owned(), json!("idle"));
    let follow_up_update = push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": follow_up}),
    );
    let stale_snapshot =
        snapshot_after_primary_hints(initial.clone(), std::slice::from_ref(&stale_update));
    let final_snapshot = snapshot_after_primary_hints(
        stale_snapshot.clone(),
        std::slice::from_ref(&follow_up_update),
    );
    let mock = LiveHerdr::start(LiveConfig::default().snapshots(vec![
        initial,
        stale_snapshot,
        final_snapshot,
    ]))
    .await
    .expect("live mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    wait_until(|| {
        handle
            .model
            .borrow()
            .task_run_by_key(&RunKey::Native {
                provider: Provider::Codex,
                sid: established.to_owned(),
            })
            .is_some()
    })
    .await;

    mock.push(stale_update)
        .await
        .expect("conflicting SID-only push should deliver");

    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    mock.push(follow_up_update)
        .await
        .expect("follow-up push should deliver");
    wait_until(|| {
        handle
            .model
            .borrow()
            .executions()
            .any(|execution| execution.state == ExecState::Idle)
    })
    .await;

    let model = handle.model.borrow();
    assert!(
        model
            .task_run_by_key(&RunKey::Native {
                provider: Provider::Codex,
                sid: established.to_owned(),
            })
            .is_some(),
        "the established run must be active again after the closing snapshot"
    );
    let historical = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: "stale-latched-sid".to_owned(),
        })
        .expect("the intermediate authoritative identity remains historical");
    assert!(historical.state.is_terminal());
    assert!(model.executions().any(|execution| {
        execution.task_run_id == historical.run_id && execution.state.is_terminal()
    }));
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn latched_session_without_agent_yields_no_live_execution() {
    let sid = "latched-without-agent";
    let mut snapshot = agent_snapshot(sid, AgentSessionReferenceKind::Id, "idle");
    snapshot["panes"][0]
        .as_object_mut()
        .expect("pane fixture should be an object")
        .remove("agent");
    let mock = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", snapshot_result(snapshot)),
    )
    .await
    .expect("mock server should bind");
    let (restored, seed, run_id) = persisted_native_restored(sid);
    let (_directory, _root, lifecycle, writer) = test_writer_seeded(seed);
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        restored,
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let model = handle.model.borrow();
    assert!(model.pane("w1:p1").is_some());
    assert!(
        model
            .executions()
            .filter(|execution| execution.task_run_id == run_id)
            .all(|execution| execution.state.is_terminal()),
        "latched identity without a reported agent must not attach a live execution"
    );
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn resnapshot_discovers_new_agent_pane() {
    let initial = p1_snapshot();
    let mut discovered = initial.clone();
    discovered["panes"]
        .as_array_mut()
        .unwrap()
        .push(agent_pane_value(
            "w1:p2",
            "new-terminal",
            "w1",
            "w1:t1",
            "discovered-sid",
        ));
    discovered["workspaces"][0]["pane_count"] = json!(2);
    discovered["tabs"][0]["pane_count"] = json!(2);
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![initial, discovered])
            .generations(vec![vec![resnapshot_anomaly()], vec![]]),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let model = handle.model.borrow();
    let run = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: "discovered-sid".to_owned(),
        })
        .expect("resnapshot must begin an execution for the new agent pane");
    assert!(model.executions().any(|execution| {
        execution.task_run_id == run.run_id
            && execution.pane_id == "w1:p2"
            && !execution.state.is_terminal()
    }));
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn authoritative_resnapshot_removes_missing_pane_and_ends_execution() {
    let initial = agent_snapshot("stale-sid", AgentSessionReferenceKind::Id, "working");
    let missing = snapshot_without_panes(&initial);
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![initial, missing])
            .generations(vec![vec![resnapshot_anomaly()], vec![]]),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let model = handle.model.borrow();
    assert!(model.pane("w1:p1").is_none());
    assert!(model.tab("w1:t1").is_some());
    assert!(model.workspace("w1").is_some());
    assert!(
        model.executions().any(|execution| {
            execution.pane_id == "w1:p1" && execution.state == ExecState::Ended
        })
    );
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn explicit_pane_close_bypasses_stale_grace() {
    let initial = agent_snapshot("close-sid", AgentSessionReferenceKind::Id, "working");
    let close = push(
        "pane_closed",
        json!({"type": "pane_closed", "pane_id": "w1:p1"}),
    );
    let closing_snapshot =
        snapshot_after_primary_hints(initial.clone(), std::slice::from_ref(&close));
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![initial, closing_snapshot])
            .generations(vec![vec![close], vec![]]),
    )
    .await
    .expect("mock server should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let model = handle.model.borrow();
    assert!(model.pane("w1:p1").is_none());
    assert!(
        model
            .executions()
            .all(|execution| execution.state == ExecState::Ended)
    );
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn stale_same_sid_reappearance_preserves_execution() {
    let present = agent_snapshot("returning-sid", AgentSessionReferenceKind::Id, "working");
    let missing = snapshot_without_panes(&present);
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![present.clone(), missing, present])
            .generations(vec![
                vec![resnapshot_anomaly()],
                vec![resnapshot_anomaly()],
                vec![],
            ]),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let model = handle.model.borrow();
    assert_eq!(model.executions().count(), 1);
    assert_eq!(model.executions().next().unwrap().state, ExecState::Working);
    assert!(model.pane("w1:p1").is_some());
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn snapshot_reappearance_reuses_identity_until_later_authoritative_exit() {
    let sid = "live-reappearance";
    let present = agent_snapshot(sid, AgentSessionReferenceKind::Id, "working");
    let missing = snapshot_without_panes(&present);
    let mock = LiveHerdr::start(LiveConfig::default().snapshots(vec![
        present.clone(),
        missing.clone(),
        present,
        missing,
    ]))
    .await
    .expect("live mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    mock.push(resnapshot_anomaly())
        .await
        .expect("anomaly should trigger an in-place snapshot");
    wait_until(|| mock.snapshot_requests() == 2).await;
    wait_until(|| handle.model.borrow().pane("w1:p1").is_none()).await;
    assert!(
        handle
            .model
            .borrow()
            .executions()
            .all(|execution| execution.state.is_terminal())
    );
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    mock.push(push(
        "pane_updated",
        json!({
            "type": "pane_updated",
            "pane": agent_pane_value(
                "w1:p1",
                "term_6583d08d791e41",
                "w1",
                "w1:t1",
                sid,
            ),
        }),
    ))
    .await
    .expect("live pane reappearance should be delivered");
    wait_until(|| {
        handle
            .model
            .borrow()
            .executions()
            .any(|execution| execution.state == ExecState::Working)
    })
    .await;
    mock.push(push(
        "pane_exited",
        json!({"type": "pane_exited", "pane_id": "w1:p1"}),
    ))
    .await
    .expect("later execution exit should be delivered");
    wait_until(|| {
        handle
            .model
            .borrow()
            .executions()
            .all(|execution| execution.state.is_terminal())
    })
    .await;

    assert!(handle.model.borrow().pane("w1:p1").is_none());
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn topology_closure_cascades_and_persists() {
    let initial = agent_snapshot("cascade-sid", AgentSessionReferenceKind::Id, "working");
    let close = push(
        "workspace_closed",
        json!({"type": "workspace_closed", "workspace_id": "w1"}),
    );
    let closing_snapshot =
        snapshot_after_primary_hints(initial.clone(), std::slice::from_ref(&close));
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![initial, closing_snapshot])
            .generations(vec![vec![close], vec![]]),
    )
    .await
    .expect("mock server should bind");
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert_eq!(handle.model.borrow().workspaces().count(), 0);
    assert_eq!(handle.model.borrow().tabs().count(), 0);
    assert_eq!(handle.model.borrow().panes().count(), 0);
    assert!(
        handle
            .model
            .borrow()
            .executions()
            .all(|execution| execution.state == ExecState::Ended)
    );
    shutdown(handle, lifecycle).await;
    let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
    assert_eq!(restored.model.workspaces().count(), 0);
    assert_eq!(restored.model.tabs().count(), 0);
    assert_eq!(restored.model.panes().count(), 0);
}

#[tokio::test]
async fn gap_replacement_prunes_absent_topology_on_restore() {
    let seed = vec![
        PersistOp::UpsertWorkspace {
            workspace: Workspace {
                workspace_id: "old-workspace".to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
        },
        PersistOp::UpsertTab {
            tab: Tab {
                tab_id: "old-tab".to_owned(),
                workspace_id: "old-workspace".to_owned(),
                label: None,
            },
            display_ordinal: DisplayOrdinal::new(2),
        },
        PersistOp::UpsertPane {
            pane: Pane {
                pane_id: "old-pane".to_owned(),
                workspace_id: "old-workspace".to_owned(),
                tab_id: "old-tab".to_owned(),
                terminal_id: "old-terminal".to_owned(),
                display_name: None,
            },
            display_ordinal: DisplayOrdinal::new(3),
        },
    ];
    let mock = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", snapshot_result(p1_snapshot())),
    )
    .await
    .expect("mock server should bind");
    let (_directory, root, lifecycle, writer) = test_writer_seeded(seed);
    let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        restored,
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    shutdown(handle, lifecycle).await;

    let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
    assert!(restored.model.workspace("old-workspace").is_none());
    assert!(restored.model.tab("old-tab").is_none());
    assert!(restored.model.pane("old-pane").is_none());
    assert!(restored.model.workspace("w1").is_some());
    let connection = Connection::open(database_path(&root)).unwrap();
    let orphaned_old_ordinals: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM display_ordinals \
             WHERE entity_kind IN ('workspace', 'tab', 'pane') \
               AND entity_id IN ('old-workspace', 'old-tab', 'old-pane')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(orphaned_old_ordinals, 0);
}

#[tokio::test]
async fn subscribe_hang_cannot_block_stop() {
    let mock = HardeningHerdr::start(HardeningConfig::default().hang_subscription_ack())
        .await
        .expect("hardening mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_until(|| mock.subscriptions() == 1).await;

    tokio::time::timeout(Duration::from_secs(1), handle.stop())
        .await
        .expect("stop must not wait for the hanging subscription acknowledgement")
        .expect("cancellation should stop the collector cleanly");
    wait_until(|| mock.active_subscriptions() == 0).await;
    assert_eq!(mock.joined_subscriptions(), 1);
    lifecycle.shutdown().await.unwrap();
}

#[tokio::test]
async fn reducer_rejection_cancels_and_joins_reader_before_reconnect() {
    let snapshot = agent_snapshot("ordinal-failure", AgentSessionReferenceKind::Id, "working");
    let mock = HardeningHerdr::start(
        HardeningConfig::default().replies(vec![SnapshotReply::Snapshot(snapshot)]),
    )
    .await
    .expect("hardening mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        RestoredState {
            model: DomainModel::default(),
            next_ordinal: i64::MAX,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        },
        writer,
    )
    .await
    .expect("collector should start");
    wait_until(|| mock.snapshot_requests() == 1).await;
    wait_until(|| mock.joined_subscriptions() == 1).await;
    wait_quality(&mut handle.quality, ObservationQuality::Disconnected).await;

    assert_eq!(handle.model.borrow().task_runs().count(), 0);
    handle
        .stop()
        .await
        .expect("cancellation during reconnect backoff should remain clean");
    assert_eq!(mock.active_subscriptions(), 0);
    lifecycle.shutdown().await.unwrap();
}

#[tokio::test]
async fn collector_uses_resolved_session_name() {
    let mock = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", snapshot_result(p1_snapshot())),
    )
    .await
    .expect("mock server should bind");
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        "resolved-session-name".to_owned(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    shutdown(handle, lifecycle).await;

    let connection = Connection::open(database_path(&root)).unwrap();
    let names: Vec<String> = connection
        .prepare("SELECT DISTINCT herdr_session FROM events")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, vec!["resolved-session-name"]);
}

#[tokio::test]
async fn startup_gap_recorded_despite_first_snapshot_retry() {
    let mock = HardeningHerdr::start(HardeningConfig::default().replies(vec![
        SnapshotReply::Error,
        SnapshotReply::Snapshot(p1_snapshot()),
    ]))
    .await
    .expect("hardening mock should bind");
    let (_directory, root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    assert_eq!(mock.snapshot_requests(), 2);
    assert_eq!(mock.subscriptions(), 2);
    shutdown(handle, lifecycle).await;

    let connection = Connection::open(database_path(&root)).unwrap();
    let startup: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE gap_kind = 'startup'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let reconnect: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE gap_kind = 'reconnect'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(startup, 1);
    assert_eq!(reconnect, 0);
}

#[tokio::test]
async fn snapshot_maps_agent_and_agent_session_identity() {
    let snapshot = agent_snapshot("mapped-sid", AgentSessionReferenceKind::Id, "blocked");
    let mock = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", snapshot_result(snapshot)),
    )
    .await
    .expect("mock server should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    let model = handle.model.borrow();
    let run = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: "mapped-sid".to_owned(),
        })
        .unwrap();
    assert!(model.agent_nodes().any(|node| {
        node.task_run_id == run.run_id
            && node.provider == Provider::Codex
            && node.native_session_id.as_deref() == Some("mapped-sid")
    }));
    assert!(model.executions().any(|execution| {
        execution.task_run_id == run.run_id && execution.state == ExecState::Blocked
    }));
    drop(model);
    shutdown(handle, lifecycle).await;
}

fn test_writer() -> (TempDir, StateRoot, WriterLifecycle, WriterClient) {
    test_writer_seeded(Vec::new())
}

fn test_writer_seeded(seed: Vec<PersistOp>) -> (TempDir, StateRoot, WriterLifecycle, WriterClient) {
    test_writer_configured(seed, |_| {})
}

fn test_writer_configured(
    seed: Vec<PersistOp>,
    setup: impl FnOnce(&StateRoot),
) -> (TempDir, StateRoot, WriterLifecycle, WriterClient) {
    let directory = tempfile::tempdir().expect("temporary store directory should exist");
    let key = session_key::encode("convergence-test").expect("session key should encode");
    let root = state_root_in(directory.path(), &key).expect("state root should initialize");
    let mut store = open_writer(&root).expect("writer store should open");
    if !seed.is_empty() {
        store
            .apply_batch(seed)
            .expect("restored seed should persist");
    }
    setup(&root);
    let (lifecycle, writer) = spawn_writer(store).expect("writer thread should start");
    (directory, root, lifecycle, writer)
}

fn empty_restored() -> RestoredState {
    RestoredState {
        model: DomainModel::default(),
        next_ordinal: 1,
        next_ingest_seq: Some(1),
        event_ledger: Vec::new(),
    }
}

fn test_session() -> String {
    "convergence-test".to_owned()
}

fn identity_metadata(event_id: &str, event_type: &str) -> EventMetadata {
    EventMetadata {
        event_id: event_id.to_owned(),
        timestamp_ms: 1,
        receipt_time_ms: 1,
        source: "herdr".to_owned(),
        source_event_type: event_type.to_owned(),
        herdr_session: test_session(),
        workspace_id: Some("w1".to_owned()),
        tab_id: Some("w1:t1".to_owned()),
        pane_id: Some("w1:p1".to_owned()),
        terminal_id: Some("terminal-1".to_owned()),
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

fn persisted_native_restored(sid: &str) -> (RestoredState, Vec<PersistOp>, RunId) {
    let run_id = RunId::new();
    let task_run = TaskRun {
        run_id,
        key: RunKey::Native {
            provider: Provider::Codex,
            sid: sid.to_owned(),
        },
        display_ordinal: DisplayOrdinal::new(1),
        state: TaskState::Running,
        has_controller_task_state_event: false,
        created_at_ms: None,
        updated_at_ms: None,
        finished_at_ms: None,
        subject: None,
        dismissed_at_ms: None,
    };
    let execution = Execution {
        execution_id: "pre-gap-execution".to_owned(),
        pane_id: "old:p1".to_owned(),
        terminal_id: "old-terminal".to_owned(),
        task_run_id: run_id,
        state: ExecState::Working,
    };
    let mut model = DomainModel::default();
    model.insert_task_run(task_run.clone());
    model.insert_execution(execution.clone());
    let seed = vec![
        PersistOp::UpsertTaskRun(PersistTaskRun {
            task_run,
            native_session: Some(NativeSessionBinding {
                provider: Provider::Codex,
                native_session_id: sid.to_owned(),
            }),
            created_at_ms: 1,
            updated_at_ms: 1,
            finished_at_ms: None,
        }),
        PersistOp::UpsertExecution(PersistExecution {
            execution,
            started_at_ms: 1,
            updated_at_ms: 1,
            ended_at_ms: None,
        }),
    ];
    (
        RestoredState {
            model,
            next_ordinal: 2,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        },
        seed,
        run_id,
    )
}

fn topology_with_session(
    value: &str,
    kind: AgentSessionReferenceKind,
    status: &str,
) -> TopologySnapshot {
    TopologySnapshot {
        workspaces: vec![herdr_top::model::Workspace {
            workspace_id: "w1".to_owned(),
        }],
        tabs: vec![herdr_top::model::Tab {
            tab_id: "w1:t1".to_owned(),
            workspace_id: "w1".to_owned(),
            label: None,
        }],
        panes: vec![PaneSnapshot {
            pane_id: "w1:p1".to_owned(),
            workspace_id: "w1".to_owned(),
            tab_id: "w1:t1".to_owned(),
            terminal_id: "new-terminal".to_owned(),
            display_name: None,
            agent: Some(SnapshotAgent {
                agent_name: "codex".to_owned(),
                status: PaneAgentStatus::from_wire(Some(status)),
            }),
            agent_session: Some(AgentSessionReference {
                source: "herdr:codex".to_owned(),
                agent: "codex".to_owned(),
                kind,
                value: value.to_owned(),
            }),
        }],
    }
}

fn agent_snapshot(value: &str, kind: AgentSessionReferenceKind, status: &str) -> Value {
    let mut snapshot = p1_snapshot();
    snapshot["panes"][0]["agent"] = json!("codex");
    snapshot["panes"][0]["agent_status"] = json!(status);
    snapshot["panes"][0]["agent_session"] = json!({
        "source": "herdr:codex",
        "agent": "codex",
        "kind": match kind {
            AgentSessionReferenceKind::Id => "id",
            AgentSessionReferenceKind::Path => "path",
        },
        "value": value,
    });
    snapshot
}

fn p1_snapshot() -> Value {
    fixture_payloads("p1-snapshot.jsonl", "A2", "recv")
        .pop()
        .expect("p1 fixture should contain snapshot response")["result"]["snapshot"]
        .clone()
}

fn snapshot_result(snapshot: Value) -> Value {
    json!({"type": "session_snapshot", "snapshot": snapshot})
}

fn pane_value(pane_id: &str, terminal_id: &str, workspace_id: &str, tab_id: &str) -> Value {
    json!({
        "pane_id": pane_id,
        "terminal_id": terminal_id,
        "workspace_id": workspace_id,
        "tab_id": tab_id,
        "focused": false,
        "agent_status": "unknown",
        "revision": 1
    })
}

fn agent_pane_value(
    pane_id: &str,
    terminal_id: &str,
    workspace_id: &str,
    tab_id: &str,
    sid: &str,
) -> Value {
    json!({
        "pane_id": pane_id,
        "terminal_id": terminal_id,
        "workspace_id": workspace_id,
        "tab_id": tab_id,
        "focused": false,
        "agent": "codex",
        "agent_status": "working",
        "agent_session": {
            "source": "herdr:codex",
            "agent": "codex",
            "kind": "id",
            "value": sid,
        },
        "revision": 2
    })
}

fn snapshot_without_panes(snapshot: &Value) -> Value {
    let mut missing = snapshot.clone();
    missing["panes"] = json!([]);
    missing["workspaces"][0]["pane_count"] = json!(0);
    missing["tabs"][0]["pane_count"] = json!(0);
    missing["focused_pane_id"] = Value::Null;
    missing
}

fn resnapshot_anomaly() -> Value {
    push(
        "pane_updated",
        json!({
            "type": "pane_updated",
            "pane": pane_value("ghost:p1", "ghost-terminal", "ghost", "ghost:t1"),
        }),
    )
}

fn push(event: &str, data: Value) -> Value {
    json!({"event": event, "data": data})
}

fn agent_status_push(pane_id: &str, terminal_id: &str, status: &str) -> Value {
    agent_status_alias_push("pane_agent_status_changed", pane_id, terminal_id, status)
}

fn agent_status_alias_push(
    event_type: &str,
    pane_id: &str,
    terminal_id: &str,
    status: &str,
) -> Value {
    push(
        event_type,
        json!({
            "type": event_type,
            "pane_id": pane_id,
            "terminal_id": terminal_id,
            "agent_status": status,
        }),
    )
}

fn spawn_static_herdr(path: &std::path::Path, snapshot: Value) -> JoinHandle<()> {
    let listener = UnixListener::bind(path).unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let snapshot = snapshot.clone();
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
                        "result": {"type": "session_snapshot", "snapshot": snapshot}
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

async fn wait_for_persistence_degradation(
    persistence: &mut tokio::sync::watch::Receiver<herdr_top::store::PersistenceHealthSnapshot>,
    diagnostics: &mut tokio::sync::watch::Receiver<
        herdr_top::diagnostics::RuntimeDiagnosticsSnapshot,
    >,
) -> herdr_top::store::PersistenceFailure {
    tokio::time::timeout(WAIT, async {
        loop {
            let writer_status = persistence.borrow().status;
            let diagnostic_status = diagnostics.borrow().persistence;
            if let (
                herdr_top::store::PersistenceStatus::Degraded {
                    failure: writer_failure,
                },
                herdr_top::store::PersistenceStatus::Degraded {
                    failure: diagnostic_failure,
                },
            ) = (writer_status, diagnostic_status)
            {
                assert_eq!(writer_failure, diagnostic_failure);
                return writer_failure;
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

async fn wait_diagnostic_source(
    diagnostics: &mut tokio::sync::watch::Receiver<
        herdr_top::diagnostics::RuntimeDiagnosticsSnapshot,
    >,
    expected_source: herdr_top::diagnostics::DiagnosticSource,
    expected_availability: herdr_top::diagnostics::InputAvailability,
) {
    tokio::time::timeout(WAIT, async {
        loop {
            if diagnostics.borrow().source_coverage.iter().any(|source| {
                source.source == expected_source && source.availability == expected_availability
            }) {
                return;
            }
            diagnostics
                .changed()
                .await
                .expect("diagnostics publisher should remain available");
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("diagnostic source {expected_source:?} did not become {expected_availability:?}")
    });
}

async fn wait_quality(
    quality: &mut tokio::sync::watch::Receiver<ObservationQuality>,
    expected: ObservationQuality,
) {
    tokio::time::timeout(WAIT, async {
        loop {
            if *quality.borrow() == expected {
                return;
            }
            quality
                .changed()
                .await
                .expect("quality publisher should remain available");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("quality did not become {expected:?}"));
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition should become true before timeout");
}

async fn wait_model_pane(model: &herdr_top::model::SharedModel, pane_id: &str) {
    let mut updates = model.clone();
    tokio::time::timeout(WAIT, async {
        loop {
            if updates.borrow().pane(pane_id).is_some() {
                return;
            }
            updates
                .changed()
                .await
                .expect("model publisher should remain available");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("pane {pane_id:?} did not appear before timeout"));
}

async fn wait_execution_state(handle: &CollectorHandle, expected: ExecState) {
    let mut model = handle.model.clone();
    tokio::time::timeout(WAIT, async {
        loop {
            if model
                .borrow()
                .executions()
                .any(|execution| !execution.state.is_terminal() && execution.state == expected)
            {
                return;
            }
            model
                .changed()
                .await
                .expect("model publisher should remain available");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("execution did not become {expected:?}"));
}

async fn shutdown(handle: CollectorHandle, lifecycle: WriterLifecycle) {
    handle.stop().await.expect("collector should stop cleanly");
    lifecycle
        .shutdown()
        .await
        .expect("writer should drain and checkpoint");
}

fn assert_execution_generations(
    handle: &CollectorHandle,
    run_id: RunId,
    ended: usize,
    live: usize,
) {
    let model = handle.model.borrow();
    assert_eq!(
        model
            .executions()
            .filter(|execution| execution.task_run_id == run_id && execution.state.is_terminal())
            .count(),
        ended
    );
    assert_eq!(
        model
            .executions()
            .filter(|execution| execution.task_run_id == run_id && !execution.state.is_terminal())
            .count(),
        live
    );
}

fn event_count(root: &StateRoot) -> i64 {
    let connection = Connection::open(database_path(root)).expect("database should open");
    connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .expect("event count should query")
}

fn pane_status_event_count(root: &StateRoot) -> i64 {
    let connection = Connection::open(database_path(root)).expect("database should open");
    connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE source_event_type = 'pane_agent_status_changed'",
            [],
            |row| row.get(0),
        )
        .expect("pane status event count should query")
}
