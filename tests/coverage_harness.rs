#[allow(dead_code)]
mod common;

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions, Permissions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use common::mock::{MockConfig, MockHerdr, fixture_payloads};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use herdr_top::activity::OperatorSnapshot;
use herdr_top::diagnostics::remote::{VersionCommand, VersionCommandRunner, VersionProbeResult};
use herdr_top::herdr::collector::{
    self, CoverageSource, ObservationQuality, PerformancePublication, SourceAvailability,
    SourceCoverageRegistry,
};
use herdr_top::lockfile::{StateRoot, state_root_in};
use herdr_top::model::{
    DisplayOrdinal, DomainModel, MinimalProviderMetadata, Provider, RunId, RunKey, TaskRun,
    TaskState,
};
use herdr_top::performance::{PerformanceDegradationReason, PerformanceSnapshot};
use herdr_top::provider::{
    FirstSeenBaseline, FsReadBoundary, MergeOutcome, ProviderCycle, ProviderEvent, ProviderTarget,
    ProviderWorker, RecommendedNotifyFactory, SourcePosition, TailFile, TargetSet,
    spawn_provider_thread_with_rescan_interval,
};
use herdr_top::session_key;
use herdr_top::store::{database_path, open_writer, spawn_writer};
use herdr_top::tui::app::{App, HeaderInputs, SystemClock, TuiSetup};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use rusqlite::{Connection, MAIN_DB};
use serde_json::{Value, json};

// Bounds event-driven waits only; generous because these assertions prove
// non-vacuity, not speed, and the suite runs under full-host load. (A 5s
// bound flaked under load twice across eras at ~35x its isolated runtime.)
const WAIT: Duration = Duration::from_secs(30);
const ROOT_THREAD: &str = "ALLOWLIST_NEGATIVE_CONTROL_THREAD_I2E_91B7";
const CHILD_THREAD: &str = "ALLOWLIST_CHILD_THREAD_I2E_3F62";
const PROMPT_SENTINEL: &str = "PROMPT_FORBIDDEN_SENTINEL_I2E_4C21";
const RESPONSE_SENTINEL: &str = "RESPONSE_FORBIDDEN_SENTINEL_I2E_5D32";
const TOOL_ARGUMENT_SENTINEL: &str = "TOOL_ARGUMENT_FORBIDDEN_SENTINEL_I2E_6E43";
const MALFORMED_RAW_SENTINEL: &str = "MALFORMED_RAW_FORBIDDEN_SENTINEL_I2E_7F54";
const DOCTOR_PRIVATE_SENTINEL: &str = "DOCTOR_PRIVATE_SENTINEL_I4_T7_8A21";
const ACTIVITY_IDENTITY_SENTINEL: &str = "ACTIVITY_IDENTITY_FORBIDDEN_I4_A1_HARNESS";
const FILTER_INDICATOR_QUERY: &str = "FILTER_INDICATOR_I9_T10";
const TUI_ROW_ONLY_MARKER: &str = "TUI_ROW_ONLY_MARKER_I4_C3";

#[test]
fn workload_header_projection_seam_is_default_build_source_clean() {
    const BEGIN: &str = "// increment5-workload-header-projection-begin";
    const END: &str = "// increment5-workload-header-projection-end";
    const TOKENS: [&str; 4] = [
        "workload_header_projection",
        "WorkloadHeaderProjection",
        "render_with_workload_projection",
        "omit_performance_label",
    ];
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tui/view.rs");
    let source = fs::read_to_string(path).expect("view source should be readable");
    let begins = source.match_indices(BEGIN).collect::<Vec<_>>();
    let ends = source.match_indices(END).collect::<Vec<_>>();

    let stripped = match (begins.as_slice(), ends.as_slice()) {
        ([], []) => source.clone(),
        ([(begin, _)], [(end, _)]) => {
            assert!(begin < end, "projection markers must be ordered");
            let region_end = end + END.len();
            let region = &source[*begin..region_end];
            assert_eq!(region.matches(BEGIN).count(), 1);
            assert_eq!(region.matches(END).count(), 1);
            assert_eq!(
                region
                    .matches("#[cfg(feature = \"workload-harness\")]")
                    .count(),
                1,
                "the seam module must have exactly one feature gate"
            );
            assert!(
                region.contains("///")
                    && region.contains("mod workload_header_projection")
                    && region.contains("WorkloadHeaderProjection")
                    && region.contains("render_with_workload_projection")
                    && region.contains("omit_performance_label"),
                "the gated seam must remain documented and contain its closed API"
            );
            let mut without = String::with_capacity(source.len() - region.len());
            without.push_str(&source[..*begin]);
            without.push_str(&source[region_end..]);
            without
        }
        _ => panic!("projection markers must be absent or form exactly one ordered pair"),
    };

    for token in TOKENS {
        assert_eq!(
            stripped.matches(token).count(),
            0,
            "{token} must not escape the feature-gated marker region"
        );
    }
    assert_eq!(stripped.matches("pub(super) fn render(").count(), 1);
    let render_start = stripped
        .find("pub(super) fn render(")
        .expect("ordinary render declaration should exist");
    let render_signature = stripped[render_start..]
        .split_once('{')
        .map(|(signature, _)| signature)
        .expect("ordinary render signature should have an opening brace");
    assert!(!render_signature.contains("workload"));
    assert!(!render_signature.contains("projection"));
    assert!(!render_signature.contains("omit_performance_label"));
}

#[test]
fn coherent_performance_publication_changes_rendered_header() {
    let (_model_sender, model_receiver) =
        tokio::sync::watch::channel(std::sync::Arc::new(DomainModel::default()));
    let (_coverage_sender, source_coverage) =
        tokio::sync::watch::channel(SourceCoverageRegistry::default());
    let (performance_sender, performance) = tokio::sync::watch::channel(PerformancePublication {
        snapshot: PerformanceSnapshot::default(),
        effective_quality: ObservationQuality::Live,
        #[cfg(feature = "workload-harness")]
        workload_sample_stamp: None,
    });
    let mut app = App::new(
        model_receiver,
        HeaderInputs {
            host: "coverage-host".to_owned(),
            session: "coverage-session".to_owned(),
            source_coverage,
            performance,
        },
    );
    let before = render_app(&app);
    performance_sender
        .send(PerformancePublication {
            snapshot: PerformanceSnapshot {
                event_lag: Duration::from_millis(1_234),
                reasons: [PerformanceDegradationReason::DependencyEdges]
                    .into_iter()
                    .collect(),
                ..PerformanceSnapshot::default()
            },
            effective_quality: ObservationQuality::Degraded,
            #[cfg(feature = "workload-harness")]
            workload_sample_stamp: None,
        })
        .unwrap();
    app.refresh();
    let after = render_app(&app);

    assert_ne!(before, after);
    assert!(after.contains("lag:1234ms"));
    assert!(after.contains("perf:dependency_edges"));
    scan_artifacts(
        &[
            Artifact::bytes("performance-before", before.into_bytes()),
            Artifact::bytes("performance-after", after.into_bytes()),
        ],
        &[
            PROMPT_SENTINEL,
            RESPONSE_SENTINEL,
            TOOL_ARGUMENT_SENTINEL,
            MALFORMED_RAW_SENTINEL,
        ],
    )
    .expect("coherent header refresh must retain the TUI allowlist boundary");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i4_doctor_output_excludes_private_values() {
    let directory = tempfile::tempdir().expect("doctor privacy root should exist");
    let private_home = directory.path().join(DOCTOR_PRIVATE_SENTINEL);
    let mock = MockHerdr::start(
        MockConfig::default()
            .error("ping", "PRIVATE_CODE", DOCTOR_PRIVATE_SENTINEL)
            .respond(
                "server.agent_manifests",
                json!({
                    "type": "agent_manifest_status",
                    "manifests": [{
                        "agent": "claude",
                        "source": DOCTOR_PRIVATE_SENTINEL,
                        "source_kind": DOCTOR_PRIVATE_SENTINEL,
                        "local_override_shadowing_remote": false,
                        "active_version": null,
                        "remote_update_error": DOCTOR_PRIVATE_SENTINEL
                    }]
                }),
            )
            .respond(
                "session.snapshot",
                json!({
                    "type": "session_snapshot",
                    "snapshot": {
                        "version": "1",
                        "protocol": 19,
                        "focused_workspace_id": null,
                        "focused_tab_id": null,
                        "focused_pane_id": null,
                        "workspaces": [], "tabs": [], "panes": [],
                        "layouts": [], "agents": []
                    }
                }),
            ),
    )
    .await
    .expect("mock Herdr should bind");
    let versions = format!(
        r#"{{"standalone":{{"version":"{0}","stderr_present":true}},"claude":{{"version":"{0}","stderr_present":true}},"codex":{{"version":"{0}","stderr_present":true}}}}"#,
        DOCTOR_PRIVATE_SENTINEL
    );

    for json_mode in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_herdr-top"));
        command
            .args(["--session", "privacy", "--socket"])
            .arg(mock.socket_path())
            .arg("doctor")
            .env_clear()
            .env("HOME", &private_home)
            .env("XDG_STATE_HOME", private_home.join("state"))
            .env("XDG_RUNTIME_DIR", private_home.join("runtime"))
            .env("HERDR_PLUGIN_STATE_DIR", private_home.join("plugin"))
            .env("HERDR_TOP_TEST_DOCTOR_VERSION_FIXTURES", &versions);
        if json_mode {
            command.arg("--json");
        }
        let output = command.output().expect("doctor privacy process should run");
        assert_eq!(output.status.code(), Some(1));
        if json_mode {
            let parsed: Value = serde_json::from_slice(&output.stdout)
                .expect("exit-one Doctor stdout must remain complete JSON");
            assert_eq!(parsed["schema_version"], 1);
        } else {
            assert!(String::from_utf8_lossy(&output.stdout).contains("overall_status=error"));
        }
        for stream in [&output.stdout, &output.stderr] {
            assert!(!String::from_utf8_lossy(stream).contains(DOCTOR_PRIVATE_SENTINEL));
            assert!(!String::from_utf8_lossy(stream).contains("PRIVATE_CODE"));
            assert!(!String::from_utf8_lossy(stream).contains("MainError"));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allowlist_scan_is_non_vacuous_across_every_artifact() {
    let state_directory = tempfile::tempdir().expect("temporary state base should exist");
    let key = session_key::encode("allowlist-harness").expect("session key should encode");
    let root = state_root_in(state_directory.path(), &key).expect("state root should initialize");
    let log_path = root.0.join("herdr-top.log");
    install_test_log_sink(&log_path);

    let provider_directory = tempfile::tempdir().expect("provider base should exist");
    let observed_root = provider_directory.path().join(".codex/sessions");
    fs::create_dir_all(&observed_root).expect("observed root should exist before observation");
    let observed_file = observed_root
        .join("rollout-2026-08-24T00-00-00-c0d15934-5f67-465f-8157-708524997bb0.jsonl");
    assert!(!observed_file.exists());

    let snapshot = snapshot_with_provider_path(&observed_file);
    let mock = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", snapshot_result(snapshot)),
    )
    .await
    .expect("mock Herdr should bind");
    let store = open_writer(&root).expect("writer store should open");
    let restored = store
        .load_restored_state()
        .expect("empty state should load");
    let (lifecycle, writer) = spawn_writer(store).expect("writer thread should start");
    let mut collector = collector::spawn(
        mock.socket_path().to_path_buf(),
        "allowlist-harness".to_owned(),
        restored,
        writer,
    )
    .await
    .expect("collector should start");
    wait_quality(&mut collector.quality, ObservationQuality::Live).await;
    wait_coverage(&mut collector.source_coverage, CoverageSource::Codex).await;

    // Created only after provider observation and its directory watch are active.
    let overlay = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/provider/codex-allowlist-overlay-synthetic.jsonl");
    fs::copy(&overlay, &observed_file).expect("synthetic overlay should be copied after start");

    // Positive structural assertions intentionally precede every negative scan.
    wait_for_model(&collector.model, model_has_threads).await;
    wait_for_database(&root, |connection| {
        persisted_thread_count(connection, &[ROOT_THREAD, CHILD_THREAD]) == 2
    })
    .await;
    let rendered = render_collector(&collector);
    for marker in [ROOT_THREAD, CHILD_THREAD] {
        assert!(
            rendered.contains(marker),
            "rendered buffer omitted structural marker {marker}"
        );
    }
    let tui_surfaces = render_new_tui_surfaces(&collector);
    assert_eq!(
        tui_surfaces.len(),
        6,
        "filter/help/detail/notice/runtime/identity-filter surfaces are all scanned"
    );
    for (surface, bytes) in &tui_surfaces {
        assert!(
            String::from_utf8_lossy(bytes).contains(ROOT_THREAD),
            "TUI surface {surface} omitted its allowlisted negative control"
        );
    }

    // Force a checkpointed DB image, then a fresh uncheckpointed WAL write.
    let checkpoint = Connection::open(database_path(&root)).expect("checkpoint connection opens");
    let (busy, _frames, _checkpointed): (i64, i64, i64) = checkpoint
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("passive checkpoint should run");
    assert_eq!(busy, 0);
    drop(checkpoint);

    append(
        &observed_file,
        br#"{"type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"allowlist-wal-update-i2e","occurred_at_ms":1786200001000,"agent_thread_id":"ALLOWLIST_NEGATIVE_CONTROL_THREAD_I2E_91B7","agent_path":"/root","kind":"interacted"}}
"#,
    );
    wait_for_database(&root, |connection| {
        connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE event_id = 'prov:codex:act:allowlist-wal-update-i2e'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            == 1
    })
    .await;

    let database = database_path(&root);
    let wal = sidecar_path(&database, "-wal");
    wait_for_nonempty_file(&wal).await;
    let backup = root.0.join("herdr-top.sqlite3.backup-allowlist-harness");
    Connection::open(&database)
        .expect("backup source should open")
        .backup(MAIN_DB, &backup, None)
        .expect("online backup should complete");
    fs::set_permissions(&backup, Permissions::from_mode(0o600))
        .expect("backup artifact should be private");
    assert!(fs::metadata(&backup).expect("backup metadata").len() > 0);
    wait_for_file_text(&log_path, "malformed provider record").await;
    assert!(fs::metadata(&log_path).expect("log metadata").len() > 0);

    // Harness-mode negative control plants an allowlisted provider ID into the log too.
    tracing::warn!(
        provider = "codex",
        agent_thread_id = ROOT_THREAD,
        "allowlist harness negative control"
    );
    wait_for_file_text(&log_path, ROOT_THREAD).await;

    let mut artifacts = vec![
        Artifact::file("database", &database),
        Artifact::file("wal", &wal),
        Artifact::file("backup", &backup),
        Artifact::file("log", &log_path),
        Artifact::bytes("rendered", rendered.into_bytes()),
    ];
    artifacts.extend(
        tui_surfaces
            .into_iter()
            .map(|(name, bytes)| Artifact::bytes(name, bytes)),
    );
    scan_artifacts(
        &artifacts,
        &[
            PROMPT_SENTINEL,
            RESPONSE_SENTINEL,
            TOOL_ARGUMENT_SENTINEL,
            MALFORMED_RAW_SENTINEL,
        ],
    )
    .expect("forbidden raw content must be absent from every artifact");

    let hits = scan_artifacts(&artifacts, &[ROOT_THREAD])
        .expect_err("allowlisted-field negative control must make the scanner fail");
    assert!(hits.iter().all(|hit| hit.sentinel == ROOT_THREAD));
    assert_eq!(
        hits.into_iter()
            .map(|hit| hit.artifact)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "backup",
            "database",
            "log",
            "rendered",
            "tui-detail",
            "tui-filter",
            "tui-help",
            "tui-identity-filter",
            "tui-notice",
            "tui-runtime",
            "wal",
        ]),
        "the negative control must prove every claimed artifact is scanned"
    );

    collector.stop().await.expect("collector should stop");
    lifecycle.shutdown().await.expect("writer should stop");
}

struct RealWatcherWorker {
    root: PathBuf,
    relative: PathBuf,
    baseline: FirstSeenBaseline,
    tail: Option<TailFile>,
}

impl ProviderWorker for RealWatcherWorker {
    fn process(&mut self, cycle: &mut ProviderCycle<'_>) -> std::io::Result<()> {
        cycle.request_watch(self.root.clone());
        let _ = cycle.pending.merge(ProviderEvent::SourceState {
            provider: Provider::Codex,
            state: herdr_top::provider::ProviderSourceState::Available,
        });
        if self.tail.is_none() {
            let mut boundary = FsReadBoundary;
            self.tail = Some(TailFile::open(
                &self.root,
                &self.relative,
                &self.baseline,
                0,
                &mut boundary,
            )?);
        }
        if cycle.hint.is_some() {
            let mut boundary = FsReadBoundary;
            for record in self
                .tail
                .as_mut()
                .expect("tail initialized")
                .poll(&mut boundary)?
            {
                let event = ProviderEvent::Activity {
                    provider: Provider::Codex,
                    agent_thread_id: "real-watcher-thread".to_owned(),
                    activity: MinimalProviderMetadata {
                        agent_id: Some("real-watcher-thread".to_owned()),
                        event_kind: Some(String::from_utf8_lossy(&record.bytes).into_owned()),
                        ..MinimalProviderMetadata::default()
                    },
                    depth: None,
                    event_id: "real-watcher-append".to_owned(),
                    observed_at_ms: 1,
                    position: SourcePosition {
                        path_id: 1,
                        generation: self.tail.as_ref().expect("tail exists").generation(),
                        offset: record.offset,
                    },
                };
                assert!(!matches!(
                    cycle.pending.merge(event),
                    MergeOutcome::AtCapacity(_)
                ));
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn real_notify_watcher_detects_append_without_polling_fallback() {
    let directory = tempfile::tempdir().expect("watch directory should exist");
    let relative = PathBuf::from("session.jsonl");
    let path = directory.path().join(&relative);
    fs::write(&path, b"").expect("empty watched file should exist");
    let worker = RealWatcherWorker {
        root: directory.path().to_path_buf(),
        relative,
        baseline: FirstSeenBaseline::from_existing([path.clone()]),
        tail: None,
    };
    let (sender, mut events) = tokio::sync::mpsc::channel(8);
    let handle = spawn_provider_thread_with_rescan_interval(
        worker,
        sender,
        Some(Box::new(RecommendedNotifyFactory)),
        Duration::from_secs(60),
    )
    .expect("real notify watcher should start");
    handle.update_targets(TargetSet::new([ProviderTarget {
        provider: Provider::Codex,
        path: path.clone(),
    }]));
    wait_for_source_state(&mut events).await;

    append(&path, b"notify-append\n");
    let event = tokio::time::timeout(WAIT, async {
        loop {
            if let Some(event @ ProviderEvent::Activity { .. }) = events.recv().await {
                return event;
            }
        }
    })
    .await
    .expect("real notification should deliver the append before the 60s rescan");
    assert!(matches!(
        event,
        ProviderEvent::Activity { activity, .. }
            if activity.event_kind.as_deref() == Some("notify-append")
    ));
    handle.stop().await.expect("provider thread should stop");
}

fn install_test_log_sink(path: &Path) {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .expect("test log should open");
    file.set_permissions(Permissions::from_mode(0o600))
        .expect("test log should be private");
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(file)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("coverage harness owns this integration-test subscriber");
}

fn snapshot_with_provider_path(path: &Path) -> Value {
    let mut snapshot = fixture_payloads("p1-snapshot.jsonl", "A2", "recv")
        .pop()
        .expect("snapshot fixture should exist")["result"]["snapshot"]
        .clone();
    snapshot["panes"][0]["agent"] = json!("codex");
    snapshot["panes"][0]["agent_status"] = json!("working");
    snapshot["panes"][0]["agent_session"] = json!({
        "source": "herdr:codex",
        "agent": "codex",
        "kind": "path",
        "value": path.to_string_lossy(),
    });
    snapshot
}

fn snapshot_result(snapshot: Value) -> Value {
    json!({"type": "session_snapshot", "snapshot": snapshot})
}

fn model_has_threads(model: &DomainModel) -> bool {
    [ROOT_THREAD, CHILD_THREAD].into_iter().all(|thread| {
        model
            .agent_nodes()
            .any(|node| node.native_session_id.as_deref() == Some(thread))
    })
}

fn persisted_thread_count(connection: &Connection, threads: &[&str]) -> i64 {
    threads
        .iter()
        .map(|thread| {
            connection
                .query_row(
                    "SELECT COUNT(*) FROM agent_nodes WHERE native_session_id = ?1",
                    [thread],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap_or(0)
        })
        .sum()
}

fn render_collector(collector: &collector::CollectorHandle) -> String {
    let app = App::new(
        collector.model.clone(),
        HeaderInputs {
            host: "test-host".to_owned(),
            session: "allowlist-harness".to_owned(),
            source_coverage: collector.source_coverage.clone(),
            performance: collector.performance.clone(),
        },
    );
    render_app(&app)
}

struct MismatchedStandalone;

impl VersionCommandRunner for MismatchedStandalone {
    fn run(&self, command: VersionCommand) -> VersionProbeResult {
        assert_eq!(command, VersionCommand::Standalone);
        VersionProbeResult::Available {
            version: "999.0.0".to_owned(),
            stderr_present: false,
        }
    }
}

fn render_new_tui_surfaces(collector: &collector::CollectorHandle) -> Vec<(&'static str, Vec<u8>)> {
    let state_base = tempfile::tempdir().expect("notice state base should exist");
    let setup = TuiSetup::for_owner(state_base.path().to_path_buf(), None, &MismatchedStandalone);
    let operator = collector.operator.borrow();
    let mut activity = operator.activity.to_vec();
    assert!(
        !activity.is_empty(),
        "activity identity privacy surface needs a real matching activity item"
    );
    for (index, item) in activity.iter_mut().enumerate() {
        item.identity.event_id = format!("{ACTIVITY_IDENTITY_SENTINEL}-{index}");
    }
    let (_operator_sender, operator_receiver) = tokio::sync::watch::channel(OperatorSnapshot {
        activity: std::sync::Arc::from(activity),
        terminal_times: std::sync::Arc::clone(&operator.terminal_times),
    });
    drop(operator);
    let mut tui_model = collector.model.borrow().as_ref().clone();
    tui_model.insert_task_run(TaskRun {
        run_id: RunId::parse("01ARZ3NDEKTSV4RRFFQ69G5FB9")
            .expect("row-control Run ID should parse"),
        key: RunKey::Controller(TUI_ROW_ONLY_MARKER.to_owned()),
        display_ordinal: DisplayOrdinal::new(i64::MAX),
        state: TaskState::Running,
        has_controller_task_state_event: true,
        created_at_ms: None,
        updated_at_ms: None,
        finished_at_ms: None,
        subject: None,
        dismissed_at_ms: None,
    });
    let (_model_sender, model_receiver) =
        tokio::sync::watch::channel(std::sync::Arc::new(tui_model));
    let mut app = App::with_inputs(
        model_receiver,
        HeaderInputs {
            host: "test-host".to_owned(),
            session: ROOT_THREAD.to_owned(),
            source_coverage: collector.source_coverage.clone(),
            performance: collector.performance.clone(),
        },
        collector.diagnostics.clone(),
        operator_receiver,
        setup,
        std::sync::Arc::new(SystemClock),
    );
    let mut surfaces = vec![("tui-notice", render_app(&app).into_bytes())];
    app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let runtime = render_app(&app);
    assert!(
        runtime.contains(TUI_ROW_ONLY_MARKER),
        "the unfiltered TUI surface needs a distinctive semantic-row control"
    );
    surfaces.push(("tui-runtime", runtime.into_bytes()));
    app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
    for character in ROOT_THREAD.chars() {
        app.handle_key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE));
    }
    surfaces.push(("tui-filter", render_app(&app).into_bytes()));
    app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
    app.handle_key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
    surfaces.push(("tui-help", render_app(&app).into_bytes()));
    app.handle_key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));
    app.handle_key(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE));
    surfaces.push(("tui-detail", render_app(&app).into_bytes()));
    app.handle_key(KeyEvent::new(KeyCode::Char('i'), KeyModifiers::NONE));
    app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
    for character in FILTER_INDICATOR_QUERY.chars() {
        app.handle_key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE));
    }
    app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let identity_filter = render_app(&app);
    assert!(identity_filter.contains("Selected: none"));
    assert!(!identity_filter.contains(TUI_ROW_ONLY_MARKER));
    assert!(
        identity_filter.contains("filter:FILTER_INDICATOR_I9_T10"),
        "committed filter indicator must preserve the short operator query verbatim"
    );
    assert!(!identity_filter.contains(ACTIVITY_IDENTITY_SENTINEL));
    surfaces.push(("tui-identity-filter", identity_filter.into_bytes()));
    for (surface, bytes) in &surfaces {
        assert!(
            !String::from_utf8_lossy(bytes).contains(ACTIVITY_IDENTITY_SENTINEL),
            "activity identity leaked through TUI surface {surface}"
        );
    }
    surfaces
}

fn render_app(app: &App) -> String {
    let backend = TestBackend::new(220, 24);
    let mut terminal = Terminal::new(backend).expect("test terminal should construct");
    terminal
        .draw(|frame| app.render(frame))
        .expect("render should work");
    terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}

struct Artifact {
    name: &'static str,
    bytes: Vec<u8>,
}

impl Artifact {
    fn file(name: &'static str, path: &Path) -> Self {
        Self::bytes(
            name,
            fs::read(path).unwrap_or_else(|error| {
                panic!(
                    "failed to read {} artifact at {}: {error}",
                    name,
                    path.display()
                )
            }),
        )
    }

    fn bytes(name: &'static str, bytes: Vec<u8>) -> Self {
        Self { name, bytes }
    }
}

#[derive(Debug)]
struct ScanHit {
    artifact: &'static str,
    sentinel: &'static str,
}

fn scan_artifacts(artifacts: &[Artifact], sentinels: &[&'static str]) -> Result<(), Vec<ScanHit>> {
    let hits = artifacts
        .iter()
        .flat_map(|artifact| {
            sentinels.iter().filter_map(move |sentinel| {
                artifact
                    .bytes
                    .windows(sentinel.len())
                    .any(|window| window == sentinel.as_bytes())
                    .then_some(ScanHit {
                        artifact: artifact.name,
                        sentinel,
                    })
            })
        })
        .collect::<Vec<_>>();
    if hits.is_empty() { Ok(()) } else { Err(hits) }
}

fn append(path: &Path, bytes: &[u8]) {
    OpenOptions::new()
        .append(true)
        .open(path)
        .expect("append target should open")
        .write_all(bytes)
        .expect("append should complete");
}

fn sidecar_path(database: &Path, suffix: &str) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

async fn wait_quality(
    receiver: &mut tokio::sync::watch::Receiver<ObservationQuality>,
    expected: ObservationQuality,
) {
    tokio::time::timeout(WAIT, async {
        loop {
            if *receiver.borrow() == expected {
                return;
            }
            receiver.changed().await.expect("quality watch stays open");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("quality did not become {expected:?}"));
}

async fn wait_coverage(
    receiver: &mut tokio::sync::watch::Receiver<collector::SourceCoverageRegistry>,
    source: CoverageSource,
) {
    tokio::time::timeout(WAIT, async {
        loop {
            if receiver.borrow().state(source) == &SourceAvailability::Available {
                return;
            }
            receiver.changed().await.expect("coverage watch stays open");
        }
    })
    .await
    .expect("provider coverage should become available");
}

async fn wait_for_model(
    receiver: &tokio::sync::watch::Receiver<std::sync::Arc<DomainModel>>,
    predicate: impl Fn(&DomainModel) -> bool,
) {
    tokio::time::timeout(WAIT, async {
        loop {
            if predicate(receiver.borrow().as_ref()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("model condition should become true");
}

async fn wait_for_database(root: &StateRoot, predicate: impl Fn(&Connection) -> bool) {
    tokio::time::timeout(WAIT, async {
        loop {
            if Connection::open(database_path(root))
                .ok()
                .as_ref()
                .is_some_and(&predicate)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("database condition should become true");
}

async fn wait_for_nonempty_file(path: &Path) {
    tokio::time::timeout(WAIT, async {
        loop {
            if fs::metadata(path).is_ok_and(|metadata| metadata.len() > 0) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{} did not become non-empty", path.display()));
}

async fn wait_for_file_text(path: &Path, needle: &str) {
    tokio::time::timeout(WAIT, async {
        loop {
            if fs::read(path).is_ok_and(|bytes| {
                bytes
                    .windows(needle.len())
                    .any(|window| window == needle.as_bytes())
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{} did not contain {needle:?}", path.display()));
}

async fn wait_for_source_state(receiver: &mut tokio::sync::mpsc::Receiver<ProviderEvent>) {
    tokio::time::timeout(WAIT, async {
        loop {
            if matches!(
                receiver.recv().await,
                Some(ProviderEvent::SourceState {
                    provider: Provider::Codex,
                    state: herdr_top::provider::ProviderSourceState::Available,
                })
            ) {
                return;
            }
        }
    })
    .await
    .expect("initial source-state event should arrive after watch installation");
}
