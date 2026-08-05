#[allow(dead_code)]
mod common;

use std::time::Duration;

use common::mock::{MockConfig, MockHerdr, fixture_payloads};
use common::scripted_mock::{ScriptedConfig, ScriptedHerdr};
use herdr_top::herdr::collector::{self, CollectorHandle, ObservationQuality};
use herdr_top::lockfile::{StateRoot, state_root_in};
use herdr_top::model::{
    AgentSessionReference, AgentSessionReferenceKind, DisplayOrdinal, DomainModel, ExecState,
    Execution, GapKind, PaneSnapshot, Provider, ReconcileBatch, RunId, RunKey, SnapshotAgent,
    TaskRun, TaskState, TopologySnapshot,
};
use herdr_top::reducer::Reducer;
use herdr_top::session_key;
use herdr_top::store::writer::{WriterClient, WriterLifecycle, spawn_writer};
use herdr_top::store::{
    CollectorGap, NativeSessionBinding, PersistExecution, PersistOp, PersistTaskRun, RestoredState,
    database_path, open_reader, open_writer,
};
use rusqlite::Connection;
use serde_json::{Value, json};
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(3);

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
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(snapshot))
            .subscription_pushes(pushes),
    )
    .await
    .expect("mock server should bind");
    let (directory, root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
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

    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
        .await
        .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert_eq!(mock.snapshot_requests(), 2);
    assert_eq!(mock.subscription_connections(), 1);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn later_closure_exempts_anomaly() {
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
            .snapshots(vec![snapshot])
            .generations(vec![generation]),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
        .await
        .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert_eq!(mock.snapshot_requests(), 1);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn three_attempts_then_stays_reconciling() {
    let snapshot = p1_snapshot();
    let anomaly = push(
        "pane_focused",
        json!({"type": "pane_focused", "pane_id": "ghost:p1", "workspace_id": "ghost"}),
    );
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot; 4])
            .generations(vec![vec![anomaly]; 4]),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
        .await
        .expect("collector should start");
    wait_until(|| mock.snapshot_requests() == 4).await;
    tokio::time::sleep(Duration::from_millis(25)).await;

    assert_eq!(*handle.quality.borrow(), ObservationQuality::Reconciling);
    assert_eq!(mock.subscription_connections(), 1);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn overflow_consumes_shared_counter_no_retirement() {
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
    let anomaly = push(
        "pane_focused",
        json!({"type": "pane_focused", "pane_id": "ghost:p1", "workspace_id": "ghost"}),
    );
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot; 4])
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

    let handle = collector::spawn(mock.socket_path().to_path_buf(), restored, writer)
        .await
        .expect("collector should start");
    wait_until(|| mock.snapshot_requests() == 4).await;
    tokio::time::sleep(Duration::from_millis(25)).await;

    let model = handle.model.borrow();
    assert_eq!(*handle.quality.borrow(), ObservationQuality::Reconciling);
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
    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), restored, writer)
        .await
        .expect("collector should start");

    wait_until(|| mock.snapshot_requests() >= 2).await;
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    assert_execution_generations(&handle, run_id, 2, 1);

    mock.replace_socket()
        .await
        .expect("scripted socket should be replaced at the same path");
    wait_until(|| mock.snapshot_requests() >= 3).await;
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;
    assert_execution_generations(&handle, run_id, 3, 1);

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
    reducer.reconcile_gap(ReconcileBatch {
        topology: topology_with_session("same-sid", AgentSessionReferenceKind::Id, "working"),
        gap_kind: GapKind::Startup,
    });
    assert!(shared.borrow().executions().any(|execution| {
        execution.execution_id != "pre-gap-execution"
            && execution.task_run_id == run_id
            && !execution.state.is_terminal()
    }));

    let (restored, _seed, run_id) = persisted_native_restored("nonempty-sid");
    let (mut reducer, shared) = Reducer::new(restored);
    reducer.reconcile_gap(ReconcileBatch {
        topology: topology_with_session("", AgentSessionReferenceKind::Id, "working"),
        gap_kind: GapKind::Startup,
    });
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
    reducer.reconcile_gap(ReconcileBatch {
        topology: topology_with_session("same-text", AgentSessionReferenceKind::Path, "working"),
        gap_kind: GapKind::Startup,
    });

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

    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), restored, writer)
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
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(snapshot))
            .subscription_pushes(pushes),
    )
    .await
    .expect("mock server should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
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
    let anomaly = push(
        "pane_focused",
        json!({"type": "pane_focused", "pane_id": "ghost:p1", "workspace_id": "ghost"}),
    );
    let mock = ScriptedHerdr::start(
        ScriptedConfig::default()
            .snapshots(vec![snapshot.clone(), snapshot])
            .generations(vec![vec![anomaly], vec![]])
            .snapshot_delay(Duration::from_millis(75)),
    )
    .await
    .expect("scripted mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
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
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(snapshot))
            .subscription_pushes(pushes),
    )
    .await
    .expect("mock server should bind");
    let (_directory, root, lifecycle, writer) = test_writer();

    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
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

    let handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
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

    let result = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer).await;

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

    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
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
    let (_directory, root, lifecycle, writer) = test_writer();
    writer
        .apply(vec![PersistOp::UpsertWorkspace(
            herdr_top::model::Workspace {
                workspace_id: "barrier-workspace".to_owned(),
            },
        )])
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
    let (_directory, root, lifecycle, writer) = test_writer();
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

    let mut handle = collector::spawn(mock.socket_path().to_path_buf(), empty_restored(), writer)
        .await
        .expect("collector should start");
    wait_quality(&mut handle.quality, ObservationQuality::Live).await;

    assert_eq!(handle.model.borrow().panes().count(), 50);
    shutdown(handle, lifecycle).await;
}

fn test_writer() -> (TempDir, StateRoot, WriterLifecycle, WriterClient) {
    test_writer_seeded(Vec::new())
}

fn test_writer_seeded(seed: Vec<PersistOp>) -> (TempDir, StateRoot, WriterLifecycle, WriterClient) {
    let directory = tempfile::tempdir().expect("temporary store directory should exist");
    let key = session_key::encode("convergence-test").expect("session key should encode");
    let root = state_root_in(directory.path(), &key).expect("state root should initialize");
    let mut store = open_writer(&root).expect("writer store should open");
    if !seed.is_empty() {
        store
            .apply_batch(seed)
            .expect("restored seed should persist");
    }
    let (lifecycle, writer) = spawn_writer(store).expect("writer thread should start");
    (directory, root, lifecycle, writer)
}

fn empty_restored() -> RestoredState {
    RestoredState {
        model: DomainModel::default(),
        next_ordinal: 1,
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
        }],
        panes: vec![PaneSnapshot {
            pane_id: "w1:p1".to_owned(),
            workspace_id: "w1".to_owned(),
            tab_id: "w1:t1".to_owned(),
            terminal_id: "new-terminal".to_owned(),
            agent: Some(SnapshotAgent {
                agent_name: "codex".to_owned(),
                state: match status {
                    "idle" => ExecState::Idle,
                    _ => ExecState::Working,
                },
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

fn push(event: &str, data: Value) -> Value {
    json!({"event": event, "data": data})
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
