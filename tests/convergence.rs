#[allow(dead_code)]
mod common;

use std::time::Duration;

use common::hardening_mock::{HardeningConfig, HardeningHerdr, SnapshotReply};
use common::live_mock::{LiveConfig, LiveHerdr};
use common::mock::{MockConfig, MockHerdr, fixture_payloads};
use common::scripted_mock::{ScriptedConfig, ScriptedHerdr};
use herdr_top::herdr::collector::{self, CollectorHandle, ObservationQuality};
use herdr_top::lockfile::{StateRoot, state_root_in};
use herdr_top::model::{
    AgentSessionReference, AgentSessionReferenceKind, DisplayOrdinal, DomainModel, EventMetadata,
    ExecState, Execution, GapKind, NormalizedEvent, Pane, PaneSnapshot, Provider, ReconcileBatch,
    RunId, RunKey, SnapshotAgent, Tab, TaskRun, TaskState, TopologySnapshot, Workspace,
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

    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        empty_restored(),
        writer,
    )
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

    let handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        restored,
        writer,
    )
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
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(snapshot))
            .subscription_pushes(pushes),
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
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(snapshot))
            .subscription_pushes(pushes),
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
async fn pane_update_with_new_sid_promotes_provisional() {
    let initial = agent_snapshot("", AgentSessionReferenceKind::Id, "working");
    let update = push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": agent_pane_value("w1:p1", "term_6583d08d791e41", "w1", "w1:t1", "promoted-sid")}),
    );
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(initial))
            .subscription_pushes(vec![update]),
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
        .expect("pane update identity must promote the provisional run");
    assert_eq!(model.task_runs().count(), 1);
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
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(initial))
            .subscription_pushes(vec![update]),
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
        .unwrap()
        .run_id;
    let new_run = model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: "new-sid".to_owned(),
        })
        .unwrap()
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
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(initial))
            .subscription_pushes(vec![update]),
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
fn binding_conflict_surfaces_diagnostic() {
    let run_id = RunId::new();
    let execution_id = "binding-conflict-execution".to_owned();
    let mut model = DomainModel::default();
    model.insert_task_run(TaskRun {
        run_id,
        key: RunKey::Native {
            provider: Provider::Codex,
            sid: "bound-codex-sid".to_owned(),
        },
        display_ordinal: DisplayOrdinal::new(1),
        state: TaskState::Running,
        has_controller_task_state_event: false,
    });
    model.insert_execution(Execution {
        execution_id: execution_id.clone(),
        pane_id: "w1:p1".to_owned(),
        terminal_id: "terminal-1".to_owned(),
        task_run_id: run_id,
        state: ExecState::Working,
    });
    let (mut reducer, mut shared) = Reducer::new(RestoredState {
        model,
        next_ordinal: 2,
    });
    let _ = shared.borrow_and_update();
    let mut event_metadata = identity_metadata("binding-conflict", "pane_updated");
    event_metadata.provider = Some(Provider::Claude);
    event_metadata.native_session_id = Some("different-claude-sid".to_owned());

    let diagnostic = reducer
        .apply(NormalizedEvent::ExecutionBegin {
            metadata: event_metadata,
            execution: Execution {
                execution_id: execution_id.clone(),
                pane_id: "w1:p1".to_owned(),
                terminal_id: "terminal-1".to_owned(),
                task_run_id: run_id,
                state: ExecState::Blocked,
            },
        })
        .expect_err("conflicting native identity must surface from the reducer");

    assert!(diagnostic.to_string().contains("binding evidence"));
    assert_eq!(
        shared
            .borrow()
            .execution(&execution_id)
            .expect("the original execution should remain")
            .state,
        ExecState::Working
    );
    assert!(!shared.has_changed().unwrap());
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
async fn sid_only_push_promotes_without_agent() {
    let initial = agent_snapshot("", AgentSessionReferenceKind::Id, "blocked");
    let mock = LiveHerdr::start(LiveConfig::default().snapshots(vec![initial]))
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
    mock.push(push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": sid_only}),
    ))
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
    assert_eq!(
        promoted.run_id, provisional_run_id,
        "promotion must keep the provisional run, not end-and-recreate it"
    );
    assert_eq!(model.task_runs().count(), 1);
    let executions: Vec<_> = model.executions().collect();
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0].execution_id, provisional_execution_id);
    assert_eq!(executions[0].state, ExecState::Blocked);
    drop(model);
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn conflicting_sid_only_push_is_skipped_not_fatal() {
    let established = "established-binding";
    let initial = agent_snapshot(established, AgentSessionReferenceKind::Id, "working");
    let mock = LiveHerdr::start(LiveConfig::default().snapshots(vec![initial]))
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
    mock.push(push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": stale}),
    ))
    .await
    .expect("conflicting SID-only push should deliver");

    let mut follow_up =
        agent_pane_value("w1:p1", "term_6583d08d791e41", "w1", "w1:t1", established);
    follow_up
        .as_object_mut()
        .expect("pane fixture should be an object")
        .insert("agent_status".to_owned(), json!("idle"));
    mock.push(push(
        "pane_updated",
        json!({"type": "pane_updated", "pane": follow_up}),
    ))
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
        "the established binding must survive the conflicting SID-only push"
    );
    assert!(
        model
            .task_run_by_key(&RunKey::Native {
                provider: Provider::Codex,
                sid: "stale-latched-sid".to_owned(),
            })
            .is_none(),
        "conflicting evidence must be skipped, never bound"
    );
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
async fn resnapshot_missing_pane_goes_stale_not_ended() {
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
    assert!(
        model.pane("w1:p1").is_some(),
        "stale pane must remain renderable"
    );
    assert!(model.tab("w1:t1").is_some());
    assert!(model.workspace("w1").is_some());
    assert!(model.executions().any(|execution| {
        execution.pane_id == "w1:p1" && matches!(execution.state, ExecState::Stale { .. })
    }));
    assert!(
        !model.executions().any(|execution| {
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
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(initial))
            .subscription_pushes(vec![close]),
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
async fn push_reappearance_cancels_pending_retirement() {
    let sid = "live-reappearance";
    let present = agent_snapshot(sid, AgentSessionReferenceKind::Id, "working");
    let missing = snapshot_without_panes(&present);
    let mock = LiveHerdr::start(LiveConfig::default().snapshots(vec![present, missing]))
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
    wait_until(|| {
        handle
            .model
            .borrow()
            .executions()
            .any(|execution| matches!(execution.state, ExecState::Stale { .. }))
    })
    .await;
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

    tokio::time::sleep(Duration::from_millis(5_250)).await;
    assert!(
        handle.model.borrow().pane("w1:p1").is_some(),
        "the retired pre-reappearance closure must not close the re-observed pane"
    );
    shutdown(handle, lifecycle).await;
}

#[tokio::test]
async fn topology_closure_cascades_and_persists() {
    let initial = agent_snapshot("cascade-sid", AgentSessionReferenceKind::Id, "working");
    let close = push(
        "workspace_closed",
        json!({"type": "workspace_closed", "workspace_id": "w1"}),
    );
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond("session.snapshot", snapshot_result(initial))
            .subscription_pushes(vec![close]),
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
    let mut old_model = DomainModel::default();
    old_model.insert_workspace(Workspace {
        workspace_id: "old-workspace".to_owned(),
    });
    old_model.insert_tab(Tab {
        tab_id: "old-tab".to_owned(),
        workspace_id: "old-workspace".to_owned(),
    });
    old_model.insert_pane(Pane {
        pane_id: "old-pane".to_owned(),
        workspace_id: "old-workspace".to_owned(),
        tab_id: "old-tab".to_owned(),
        terminal_id: "old-terminal".to_owned(),
    });
    let seed = vec![
        PersistOp::UpsertWorkspace(Workspace {
            workspace_id: "old-workspace".to_owned(),
        }),
        PersistOp::UpsertTab(Tab {
            tab_id: "old-tab".to_owned(),
            workspace_id: "old-workspace".to_owned(),
        }),
        PersistOp::UpsertPane(Pane {
            pane_id: "old-pane".to_owned(),
            workspace_id: "old-workspace".to_owned(),
            tab_id: "old-tab".to_owned(),
            terminal_id: "old-terminal".to_owned(),
        }),
    ];
    let mock = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", snapshot_result(p1_snapshot())),
    )
    .await
    .expect("mock server should bind");
    let (_directory, root, lifecycle, writer) = test_writer_seeded(seed);
    let mut handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        RestoredState {
            model: old_model,
            next_ordinal: 1,
        },
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
async fn converge_error_cancels_and_joins_reader() {
    let snapshot = agent_snapshot("ordinal-failure", AgentSessionReferenceKind::Id, "working");
    let mock = HardeningHerdr::start(
        HardeningConfig::default().replies(vec![SnapshotReply::Snapshot(snapshot)]),
    )
    .await
    .expect("hardening mock should bind");
    let (_directory, _root, lifecycle, writer) = test_writer();
    let handle = collector::spawn(
        mock.socket_path().to_path_buf(),
        test_session(),
        RestoredState {
            model: DomainModel::default(),
            next_ordinal: i64::MAX,
        },
        writer,
    )
    .await
    .expect("collector should start");
    wait_until(|| mock.snapshot_requests() == 1).await;
    wait_until(|| mock.joined_subscriptions() == 1).await;

    let error = handle
        .stop()
        .await
        .expect_err("ordinal exhaustion must surface from convergence");
    assert!(
        error
            .to_string()
            .contains("display ordinal allocator is exhausted")
    );
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

fn test_session() -> String {
    "convergence-test".to_owned()
}

fn identity_metadata(event_id: &str, event_type: &str) -> EventMetadata {
    EventMetadata {
        event_id: event_id.to_owned(),
        timestamp_ms: 1,
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
        "pane_focused",
        json!({"type": "pane_focused", "pane_id": "ghost:p1", "workspace_id": "ghost"}),
    )
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
