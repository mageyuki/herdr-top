#[allow(dead_code)]
mod common;

use std::collections::{HashMap, HashSet};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use common::mock::{MockConfig, MockHerdr};
use herdr_top::herdr::collector::{self, CollectorHandle, ObservationQuality};
use herdr_top::lockfile::{OwnerRecord, StateRoot, state_root_in, try_acquire};
use herdr_top::model::{DisplayOrdinal, ExecState, Provider, RunId, RunKey, TaskRun, TaskState};
use herdr_top::rendezvous::{
    ControllerSocketStatus, open_runtime_dir_at, prepare_controller_socket,
};
use herdr_top::session_key;
use herdr_top::store::{
    PersistOp, PersistTaskRun, SchemaVerdict, database_path, open_reader, open_writer,
    preflight_schema, spawn_writer,
};
use rusqlite::Connection;
use serde_json::{Value, json};
use tempfile::TempDir;

const SESSION_NAME: &str = "Task 11 restore session";
const WAIT: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_launch_reads_owner_resolves_and_focuses() {
    let state_directory = tempfile::tempdir().expect("temporary state directory should exist");
    let (root, _owner_lock) = held_root(&state_directory);
    let owner = OwnerRecord {
        pid: 41_001,
        started_at_ms: 1_725_000_000_001,
        terminal_id: Some("owner-terminal".to_owned()),
        pane_id: Some("old:pane".to_owned()),
    };
    seed_owner(&root, &owner);
    let backups_before = backup_count(&root);
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond(
                "session.snapshot",
                snapshot_result(snapshot(vec![pane(
                    "current:pane",
                    "owner-terminal",
                    Some(("codex", "owner-session")),
                )])),
            )
            .respond(
                "pane.focus",
                json!({"type": "pane_focused", "pane_id": "current:pane"}),
            ),
    )
    .await
    .expect("mock server should bind");

    let output = run_binary(
        state_directory.path(),
        Some(mock.socket_path()),
        SESSION_NAME,
    );

    assert!(
        output.status.success(),
        "second launch failed: {}",
        output_text(&output)
    );
    assert!(output_text(&output).contains("focused owner pane current:pane"));
    let requests = mock.requests();
    let methods: Vec<_> = requests
        .iter()
        .filter_map(|request| request["method"].as_str())
        .collect();
    assert_eq!(methods, ["session.snapshot", "pane.focus"]);
    assert_eq!(requests[1]["params"], json!({"pane_id": "current:pane"}));
    assert_eq!(backup_count(&root), backups_before);
    assert_eq!(
        open_reader(&root)
            .expect("reader should open")
            .read_owner()
            .expect("owner should read"),
        Some(owner),
        "held branch must not replace the owner row"
    );
}

#[test]
fn simultaneous_launch_second_reports_owner_starting() {
    let state_directory = tempfile::tempdir().expect("temporary state directory should exist");
    let (root, _owner_lock) = held_root(&state_directory);
    let started = Instant::now();

    let output = run_binary(state_directory.path(), None, SESSION_NAME);

    assert!(
        output.status.success(),
        "owner-starting launch failed: {}",
        output_text(&output)
    );
    let diagnostic = output_text(&output);
    assert!(diagnostic.contains("OwnerStarting"));
    assert!(!diagnostic.contains("pid="));
    assert!(!diagnostic.contains("terminal_id="));
    assert!(!diagnostic.contains("pane_id="));
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "owner-starting branch should perform its bounded retry"
    );
    assert!(
        !database_path(&root).exists(),
        "held branch must not create a writer database"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_branch_focus_failure_reports_owner_info() {
    let state_directory = tempfile::tempdir().expect("temporary state directory should exist");
    let (root, _owner_lock) = held_root(&state_directory);
    let owner = OwnerRecord {
        pid: 41_002,
        started_at_ms: 1_725_000_000_002,
        terminal_id: Some("owner-terminal".to_owned()),
        pane_id: Some("last-known:pane".to_owned()),
    };
    seed_owner(&root, &owner);
    let backups_before = backup_count(&root);
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond(
                "session.snapshot",
                snapshot_result(snapshot(vec![pane(
                    "current:pane",
                    "owner-terminal",
                    Some(("codex", "owner-session")),
                )])),
            )
            .error("pane.focus", "FOCUS_FAILED", "fabricated focus failure"),
    )
    .await
    .expect("mock server should bind");

    let output = run_binary(
        state_directory.path(),
        Some(mock.socket_path()),
        SESSION_NAME,
    );

    assert!(
        output.status.success(),
        "focus-failure branch failed: {}",
        output_text(&output)
    );
    let diagnostic = output_text(&output);
    assert!(diagnostic.contains("could not focus existing owner"));
    assert!(diagnostic.contains("pid=41002"));
    assert!(diagnostic.contains("terminal_id=owner-terminal"));
    assert!(diagnostic.contains("pane_id=last-known:pane"));
    assert_eq!(
        mock.requests()
            .iter()
            .filter(|request| request["method"] == "events.subscribe")
            .count(),
        0,
        "held branch must not launch a second collector"
    );
    assert_eq!(backup_count(&root), backups_before);
    assert_eq!(
        open_reader(&root)
            .expect("reader should open")
            .read_owner()
            .expect("owner should read"),
        Some(owner)
    );
}

#[test]
fn i4_restore_terminal_time_survives_cold_restart() {
    let directory = tempfile::tempdir().expect("temporary state directory should exist");
    let root = StateRoot(directory.path().to_path_buf());
    let run_id = RunId::new();
    let first_terminal_ms = 1_900_000_000_100;
    {
        let mut store = open_writer(&root).expect("writer should open");
        store
            .apply_batch(vec![i4_run_op(
                run_id,
                1,
                TaskState::Completed,
                1_900_000_000_200,
                Some(first_terminal_ms),
            )])
            .expect("terminal run should persist");
        store.checkpoint().expect("terminal run should checkpoint");
    }

    let restored = open_reader(&root)
        .expect("reader should cold reopen")
        .load_restored_operator_state()
        .expect("operator state should restore");

    assert_eq!(
        restored.terminal_times.get(&run_id),
        Some(&first_terminal_ms)
    );
}

#[test]
fn i4_restore_terminal_restatement_preserves_first_time() {
    let directory = tempfile::tempdir().expect("temporary state directory should exist");
    let root = StateRoot(directory.path().to_path_buf());
    let run_id = RunId::new();
    let first_terminal_ms = 1_900_000_000_100;
    {
        let mut store = open_writer(&root).expect("writer should open");
        store
            .apply_batch(vec![i4_run_op(
                run_id,
                1,
                TaskState::Completed,
                first_terminal_ms,
                Some(first_terminal_ms),
            )])
            .expect("first terminal transition should persist");
        store
            .apply_batch(vec![i4_run_op(
                run_id,
                1,
                TaskState::Failed,
                1_900_000_000_200,
                Some(1_900_000_000_200),
            )])
            .expect("terminal restatement should persist");
        store
            .checkpoint()
            .expect("terminal restatement should checkpoint");
    }

    let restored = open_reader(&root)
        .expect("reader should cold reopen")
        .load_restored_operator_state()
        .expect("operator state should restore");

    assert_eq!(
        restored.terminal_times.get(&run_id),
        Some(&first_terminal_ms)
    );
}

#[test]
fn i4_restore_reopen_clears_and_reterminal_sets_new_time() {
    let directory = tempfile::tempdir().expect("temporary state directory should exist");
    let root = StateRoot(directory.path().to_path_buf());
    let run_id = RunId::new();
    {
        let mut store = open_writer(&root).expect("writer should open");
        store
            .apply_batch(vec![i4_run_op(
                run_id,
                1,
                TaskState::Completed,
                1_900_000_000_100,
                Some(1_900_000_000_100),
            )])
            .expect("first terminal transition should persist");
        store
            .apply_batch(vec![i4_run_op(
                run_id,
                1,
                TaskState::Running,
                1_900_000_000_200,
                None,
            )])
            .expect("reopen should persist");
        store.checkpoint().expect("reopen should checkpoint");
    }
    let reopened = open_reader(&root)
        .expect("reader should cold reopen")
        .load_restored_operator_state()
        .expect("operator state should restore after reopen");
    assert!(!reopened.terminal_times.contains_key(&run_id));

    let second_terminal_ms = 1_900_000_000_300;
    {
        let mut store = open_writer(&root).expect("writer should reopen");
        store
            .apply_batch(vec![i4_run_op(
                run_id,
                1,
                TaskState::Cancelled,
                second_terminal_ms,
                None,
            )])
            .expect("second terminal transition should persist");
        store
            .checkpoint()
            .expect("second terminal transition should checkpoint");
    }
    let reterminated = open_reader(&root)
        .expect("reader should cold reopen again")
        .load_restored_operator_state()
        .expect("operator state should restore after second terminal transition");
    assert_eq!(
        reterminated.terminal_times.get(&run_id),
        Some(&second_terminal_ms)
    );
}

#[test]
fn i4_restore_merge_uses_earliest_terminal_time_and_excludes_alias() {
    let directory = tempfile::tempdir().expect("temporary state directory should exist");
    let root = StateRoot(directory.path().to_path_buf());
    let terminal_survivor = RunId::new();
    let terminal_absorbed = RunId::new();
    let running_survivor = RunId::new();
    let running_absorbed = RunId::new();
    let fallback_survivor = RunId::new();
    let fallback_absorbed = RunId::new();
    let base = 1_900_000_000_000;
    {
        let mut store = open_writer(&root).expect("writer should open");
        store
            .apply_batch(vec![
                i4_run_op(
                    terminal_survivor,
                    1,
                    TaskState::Completed,
                    base + 200,
                    Some(base + 200),
                ),
                i4_run_op(
                    terminal_absorbed,
                    2,
                    TaskState::Failed,
                    base + 100,
                    Some(base + 100),
                ),
                PersistOp::MergeTaskRuns {
                    survivor: terminal_survivor,
                    absorbed: terminal_absorbed,
                },
                i4_run_op(running_survivor, 3, TaskState::Running, base + 500, None),
                i4_run_op(
                    running_absorbed,
                    4,
                    TaskState::Completed,
                    base + 400,
                    Some(base + 400),
                ),
                PersistOp::MergeTaskRuns {
                    survivor: running_survivor,
                    absorbed: running_absorbed,
                },
                i4_run_op(
                    fallback_survivor,
                    5,
                    TaskState::Completed,
                    base + 600,
                    Some(base + 600),
                ),
                i4_run_op(
                    fallback_absorbed,
                    6,
                    TaskState::Failed,
                    base + 700,
                    Some(base + 700),
                ),
            ])
            .expect("merge fixtures should persist");
        store
            .checkpoint()
            .expect("merge fixtures should checkpoint");
    }
    {
        let connection = Connection::open(database_path(&root)).expect("database should open");
        connection
            .execute(
                "UPDATE task_runs SET finished_at_ms = NULL WHERE run_id IN (?1, ?2)",
                [fallback_survivor.to_string(), fallback_absorbed.to_string()],
            )
            .expect("fallback fixture should remove both terminal times");
    }
    {
        let mut store = open_writer(&root).expect("writer should reopen");
        store
            .apply_batch(vec![PersistOp::MergeTaskRuns {
                survivor: fallback_survivor,
                absorbed: fallback_absorbed,
            }])
            .expect("fallback merge should persist");
        store.checkpoint().expect("merges should checkpoint");
    }

    let restored = open_reader(&root)
        .expect("reader should cold reopen")
        .load_restored_operator_state()
        .expect("operator state should restore");

    assert_eq!(
        restored.terminal_times.get(&terminal_survivor),
        Some(&(base + 100))
    );
    assert!(!restored.terminal_times.contains_key(&terminal_absorbed));
    assert!(!restored.terminal_times.contains_key(&running_survivor));
    assert!(!restored.terminal_times.contains_key(&running_absorbed));
    assert_eq!(
        restored.terminal_times.get(&fallback_survivor),
        Some(&(base + 600))
    );
    assert!(!restored.terminal_times.contains_key(&fallback_absorbed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_across_cold_restart() {
    let state_directory = tempfile::tempdir().expect("temporary state directory should exist");
    let runtime_directory = tempfile::tempdir().expect("temporary runtime directory should exist");
    std::fs::set_permissions(runtime_directory.path(), Permissions::from_mode(0o700))
        .expect("runtime test base should be private");
    let key = session_key::encode(SESSION_NAME).expect("session key should encode");
    let root = state_root_in(state_directory.path(), &key).expect("state root should initialize");
    let first_lock = try_acquire(&root)
        .expect("first lock attempt should work")
        .expect("first launch should own the session");
    let runtime =
        open_runtime_dir_at(runtime_directory.path()).expect("runtime directory should validate");
    assert!(matches!(
        prepare_controller_socket(&runtime, &key, &first_lock)
            .expect("controller pre-bind should succeed"),
        ControllerSocketStatus::Bound(_)
    ));
    assert_eq!(
        preflight_schema(&root).expect("first schema preflight should work"),
        SchemaVerdict::Absent
    );
    let mock_a = MockHerdr::start(MockConfig::default().respond(
        "session.snapshot",
        snapshot_result(snapshot(vec![
            pane("a:p1", "a-terminal-1", Some(("codex", "latched-sid"))),
            pane("a:p2", "a-terminal-2", Some(("claude", "retired-sid"))),
        ])),
    ))
    .await
    .expect("mock A should bind");
    let store_a = open_writer(&root).expect("first writer should open");
    let restored_a = store_a
        .load_restored_state()
        .expect("empty first state should load");
    let (lifecycle_a, writer_a) = spawn_writer(store_a).expect("first writer should spawn");
    let mut collector_a = collector::spawn(
        mock_a.socket_path().to_path_buf(),
        SESSION_NAME.to_owned(),
        restored_a,
        writer_a,
    )
    .await
    .expect("collector A should start");
    wait_live(&mut collector_a).await;

    let before = collector_a.model.borrow();
    let old_ordinals: HashMap<RunId, i64> = before
        .task_runs()
        .map(|run| (run.run_id, run.display_ordinal.get()))
        .collect();
    let old_execution_ids: HashSet<String> = before
        .executions()
        .map(|execution| execution.execution_id.clone())
        .collect();
    let old_workspace_ordinal = before
        .workspace_ordinal("w1")
        .expect("workspace should have a display ordinal");
    let old_tab_ordinal = before
        .tab_ordinal("w1:t1")
        .expect("tab should have a display ordinal");
    let old_pane_ordinals: HashMap<String, i64> = before
        .panes()
        .map(|pane| {
            (
                pane.pane_id.clone(),
                before
                    .pane_ordinal(&pane.pane_id)
                    .expect("pane should have a display ordinal")
                    .get(),
            )
        })
        .collect();
    let latched_run = run_for_sid(&before, Provider::Codex, "latched-sid");
    let retired_run = run_for_sid(&before, Provider::Claude, "retired-sid");
    assert_eq!(old_ordinals.len(), 2);
    assert_eq!(old_execution_ids.len(), 2);
    drop(before);

    collector_a
        .stop()
        .await
        .expect("collector A should stop before writer A");
    lifecycle_a
        .shutdown()
        .await
        .expect("writer A should drain, checkpoint, and join");
    let reader_a = open_reader(&root).expect("reader should reopen after first shutdown");
    let restored_after_a = reader_a
        .load_restored_state()
        .expect("first cold state should restore");
    assert_eq!(
        restored_after_a.model.task_runs().count(),
        old_ordinals.len()
    );
    assert_eq!(
        restored_after_a.model.workspace_ordinal("w1"),
        Some(old_workspace_ordinal)
    );
    assert_eq!(
        restored_after_a.model.tab_ordinal("w1:t1"),
        Some(old_tab_ordinal)
    );
    for (pane_id, ordinal) in &old_pane_ordinals {
        assert_eq!(
            restored_after_a
                .model
                .pane_ordinal(pane_id)
                .map(herdr_top::model::DisplayOrdinal::get),
            Some(*ordinal),
            "pane {pane_id} should keep its ordinal across restart"
        );
    }
    let owner_a = reader_a
        .read_owner()
        .expect("first owner should read")
        .expect("first owner should exist");
    assert_eq!(owner_a.terminal_id.as_deref(), Some("a-terminal-1"));
    assert_eq!(owner_a.pane_id.as_deref(), Some("a:p1"));
    drop(reader_a);
    drop(mock_a);
    drop(first_lock);

    tokio::time::sleep(Duration::from_millis(5)).await;
    let second_lock = try_acquire(&root)
        .expect("second lock attempt should work")
        .expect("cold restart should reacquire the session");
    assert!(matches!(
        prepare_controller_socket(&runtime, &key, &second_lock)
            .expect("second controller pre-bind should succeed"),
        ControllerSocketStatus::Bound(_)
    ));
    assert_eq!(
        preflight_schema(&root).expect("second schema preflight should work"),
        SchemaVerdict::Current
    );
    let mock_b = MockHerdr::start(MockConfig::default().respond(
        "session.snapshot",
        snapshot_result(snapshot(vec![
            pane("b:p1", "b-terminal-1", Some(("codex", "latched-sid"))),
            pane("b:p2", "b-terminal-2", None),
        ])),
    ))
    .await
    .expect("mock B should bind");
    let store_b = open_writer(&root).expect("second writer should backup and open");
    let restored_b = store_b
        .load_restored_state()
        .expect("second launch should load persisted state");
    assert!(
        old_ordinals
            .keys()
            .all(|run_id| restored_b.model.task_run(run_id).is_some()),
        "every Task Run must be restored before reconciliation"
    );
    let (lifecycle_b, writer_b) = spawn_writer(store_b).expect("second writer should spawn");
    let mut collector_b = collector::spawn(
        mock_b.socket_path().to_path_buf(),
        SESSION_NAME.to_owned(),
        restored_b,
        writer_b,
    )
    .await
    .expect("collector B should start");
    wait_live(&mut collector_b).await;

    let after = collector_b.model.borrow();
    assert!(
        old_ordinals
            .keys()
            .all(|run_id| after.task_run(run_id).is_some()),
        "restored Task Runs must survive startup reconciliation"
    );
    for (run_id, ordinal) in &old_ordinals {
        assert_eq!(
            after
                .task_run(run_id)
                .expect("restored run should remain")
                .display_ordinal
                .get(),
            *ordinal,
            "restored display ordinals must be preserved"
        );
    }
    assert!(old_execution_ids.iter().all(|execution_id| {
        after
            .execution(execution_id)
            .is_some_and(|execution| execution.state == ExecState::Ended)
    }));
    let latched_live: Vec<_> = after
        .executions()
        .filter(|execution| execution.task_run_id == latched_run && !execution.state.is_terminal())
        .collect();
    assert_eq!(latched_live.len(), 1);
    assert_eq!(latched_live[0].pane_id, "b:p1");
    assert_eq!(latched_live[0].terminal_id, "b-terminal-1");
    assert!(
        after
            .executions()
            .filter(|execution| execution.task_run_id == retired_run)
            .all(|execution| execution.state.is_terminal()),
        "only the pane with corroborated native identity may re-attach"
    );
    assert!(after.executions().all(|execution| {
        execution.state.is_terminal()
            || matches!(
                execution.terminal_id.as_str(),
                "b-terminal-1" | "b-terminal-2"
            )
    }));
    drop(after);

    collector_b
        .stop()
        .await
        .expect("collector B should stop before writer B");
    lifecycle_b
        .shutdown()
        .await
        .expect("writer B should drain, checkpoint, and join");
    let reader_b = open_reader(&root).expect("reader should reopen after second shutdown");
    let owner_b = reader_b
        .read_owner()
        .expect("second owner should read")
        .expect("second owner should exist");
    assert_eq!(owner_b.terminal_id.as_deref(), Some("b-terminal-1"));
    assert_eq!(owner_b.pane_id.as_deref(), Some("b:p1"));
    assert_ne!(owner_b.started_at_ms, owner_a.started_at_ms);
    assert_ne!(owner_b, owner_a, "the stale owner row must be replaced");
    drop(reader_b);

    let connection = Connection::open(database_path(&root)).expect("database should open");
    let startup_gaps: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE gap_kind = 'startup'",
            [],
            |row| row.get(0),
        )
        .expect("startup gap count should query");
    assert_eq!(startup_gaps, 2, "each cold owner launch records Startup");
}

fn held_root(directory: &TempDir) -> (StateRoot, herdr_top::lockfile::OwnerLock) {
    let key = session_key::encode(SESSION_NAME).expect("session key should encode");
    let root = state_root_in(directory.path(), &key).expect("state root should initialize");
    let owner_lock = try_acquire(&root)
        .expect("lock attempt should work")
        .expect("test should acquire owner lock");
    (root, owner_lock)
}

fn seed_owner(root: &StateRoot, owner: &OwnerRecord) {
    let mut store = open_writer(root).expect("writer store should open");
    store.replace_owner(owner).expect("owner should persist");
    store.checkpoint().expect("seed WAL should checkpoint");
}

fn i4_run_op(
    run_id: RunId,
    ordinal: i64,
    state: TaskState,
    updated_at_ms: i64,
    finished_at_ms: Option<i64>,
) -> PersistOp {
    PersistOp::UpsertTaskRun(PersistTaskRun {
        task_run: TaskRun {
            run_id,
            key: RunKey::Controller(format!("controller-{run_id}")),
            display_ordinal: DisplayOrdinal::new(ordinal),
            state,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        },
        native_session: None,
        created_at_ms: updated_at_ms,
        updated_at_ms,
        finished_at_ms,
    })
}

fn run_binary(state_base: &Path, socket: Option<&Path>, session: &str) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_herdr-top"));
    command
        .args(["--session", session])
        .env("XDG_STATE_HOME", state_base)
        .env_remove("HERDR_SESSION")
        .env_remove("HERDR_ENV")
        .env_remove("HERDR_SOCKET_PATH");
    if let Some(socket) = socket {
        command.arg("--socket").arg(socket);
    }
    command.output().expect("herdr-top process should start")
}

fn output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn backup_count(root: &StateRoot) -> usize {
    std::fs::read_dir(&root.0)
        .expect("state root should list")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("herdr-top.sqlite3.backup-")
        })
        .count()
}

fn snapshot(panes: Vec<Value>) -> Value {
    json!({
        "version": "0.8.0",
        "protocol": 19,
        "focused_workspace_id": "w1",
        "focused_tab_id": "w1:t1",
        "focused_pane_id": panes.first().and_then(|pane| pane["pane_id"].as_str()),
        "workspaces": [{
            "workspace_id": "w1",
            "number": 1,
            "label": "restore",
            "focused": true,
            "pane_count": panes.len(),
            "tab_count": 1,
            "active_tab_id": "w1:t1",
            "agent_status": "working"
        }],
        "tabs": [{
            "tab_id": "w1:t1",
            "workspace_id": "w1",
            "number": 1,
            "label": "1",
            "focused": true,
            "pane_count": panes.len(),
            "agent_status": "working"
        }],
        "panes": panes,
        "layouts": [],
        "agents": []
    })
}

fn pane(pane_id: &str, terminal_id: &str, session: Option<(&str, &str)>) -> Value {
    let (agent, agent_session) = match session {
        Some((agent, sid)) => (
            Some(agent),
            Some(json!({
                "source": format!("herdr:{agent}"),
                "agent": agent,
                "kind": "id",
                "value": sid
            })),
        ),
        None => (Some("codex"), None),
    };
    json!({
        "pane_id": pane_id,
        "terminal_id": terminal_id,
        "workspace_id": "w1",
        "tab_id": "w1:t1",
        "focused": pane_id.ends_with("p1"),
        "agent": agent,
        "agent_status": "working",
        "agent_session": agent_session,
        "revision": 0
    })
}

fn snapshot_result(snapshot: Value) -> Value {
    json!({"type": "session_snapshot", "snapshot": snapshot})
}

fn run_for_sid(model: &herdr_top::model::DomainModel, provider: Provider, sid: &str) -> RunId {
    model
        .task_run_by_key(&RunKey::Native {
            provider,
            sid: sid.to_owned(),
        })
        .unwrap_or_else(|| panic!("run for {provider:?}/{sid} should exist"))
        .run_id
}

async fn wait_live(handle: &mut CollectorHandle) {
    tokio::time::timeout(WAIT, async {
        loop {
            if *handle.quality.borrow() == ObservationQuality::Live {
                return;
            }
            handle
                .quality
                .changed()
                .await
                .expect("quality publisher should remain available");
        }
    })
    .await
    .expect("collector should become live");
}
