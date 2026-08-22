mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

#[cfg(feature = "workload-harness")]
use common::mock::{MockConfig, MockHerdr};
use common::workload::{self, *};
use herdr_top::herdr::controller::ControllerEnvelope;
use herdr_top::lockfile::StateRoot;
#[cfg(feature = "workload-harness")]
use herdr_top::model::Workspace;
use herdr_top::model::{
    AgentNode, ControllerEventKind, DisplayOrdinal, DomainModel, EventMetadata,
    MinimalProviderMetadata, NormalizedEvent, Provider, RunKey, TaskState,
};
use herdr_top::reducer::{ApplyOutcome, Reducer};
use herdr_top::store::{PersistOp, RestoredState, open_reader, open_writer};
#[cfg(feature = "workload-harness")]
use herdr_top::tui::app::{App, HeaderInputs, WorkloadFrameDriver, WorkloadFrameObservation};
#[cfg(feature = "workload-harness")]
use ratatui::Terminal;
#[cfg(feature = "workload-harness")]
use ratatui::backend::TestBackend;

#[cfg(feature = "workload-harness")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "workload-harness")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "workload-harness")]
use herdr_top::activity::RestoredOperatorState;
#[cfg(feature = "workload-harness")]
use herdr_top::herdr::collector::{
    ObservationQuality, PerformancePublication, WorkloadCollectorConfig, WorkloadPerformanceSample,
    spawn_workload_collector,
};
#[cfg(feature = "workload-harness")]
use herdr_top::herdr::controller::{
    ControllerResponse, WorkloadAdmissionObservation, WorkloadControllerHooks,
    WorkloadPersistenceObservation, WorkloadTerminalObservation,
};
#[cfg(all(target_os = "linux", feature = "workload-harness"))]
use herdr_top::performance::AbsoluteMonotonicPerformanceClock;
#[cfg(feature = "workload-harness")]
use herdr_top::performance::{
    PerformanceClock, PerformanceDegradationReason, TestPerformanceClock,
};
#[cfg(feature = "workload-harness")]
use herdr_top::provider::{DiscoveryRoot, NotifyFactory, NotifySink, NotifyWatcher};
#[cfg(feature = "workload-harness")]
use herdr_top::reducer::{WorkloadTimingKind, WorkloadTimingObservation, WorkloadTimingObserver};
#[cfg(feature = "workload-harness")]
use herdr_top::store::spawn_writer;

#[cfg(feature = "workload-harness")]
fn frame_driver_for_times(
    millis: &[u64],
) -> (
    WorkloadFrameDriver,
    tokio::sync::watch::Sender<std::sync::Arc<DomainModel>>,
    tokio::sync::watch::Sender<PerformancePublication>,
) {
    let clock_values = millis
        .iter()
        .flat_map(|millis| [Duration::from_millis(*millis); 2])
        .collect::<Vec<_>>();
    let mut clock_values = clock_values.into_iter();
    let (model_sender, model_receiver) =
        tokio::sync::watch::channel(std::sync::Arc::new(DomainModel::default()));
    let (performance_sender, performance) =
        tokio::sync::watch::channel(stamped_publication(0, 0, ObservationQuality::Live, []));
    let app = App::new(
        model_receiver,
        HeaderInputs {
            performance,
            ..HeaderInputs::default()
        },
    );
    let terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    let driver = WorkloadFrameDriver::new(app, terminal, move || {
        clock_values
            .next()
            .expect("fixed workload clock must cover every limiter read")
    });
    (driver, model_sender, performance_sender)
}

#[cfg(feature = "workload-harness")]
#[test]
fn workload_frame_driver_matches_production_limiter_decisions() {
    let (mut driver, _model_sender, _quality_sender) =
        frame_driver_for_times(&[0, 50, 99, 100, 200]);

    let observations = [false, false, true, false, true]
        .into_iter()
        .map(|dirty| driver.step(dirty).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        observations
            .iter()
            .map(|observation| observation.draw_ordinal)
            .collect::<Vec<_>>(),
        vec![Some(0), None, None, Some(1), Some(2)]
    );
    assert_eq!(
        observations
            .iter()
            .map(|observation| observation.poll_duration)
            .collect::<Vec<_>>(),
        vec![
            Duration::from_millis(10),
            Duration::from_millis(10),
            Duration::from_millis(1),
            Duration::from_millis(10),
            Duration::from_millis(10),
        ]
    );
}

#[cfg(feature = "workload-harness")]
#[test]
fn workload_frame_driver_waits_for_first_eligible_response_frame() {
    let (mut driver, _model_sender, _quality_sender) = frame_driver_for_times(&[0, 10, 99, 100]);
    assert_eq!(driver.step(false).unwrap().draw_ordinal, Some(0));

    let response = driver
        .handle_key_and_wait(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('?'),
            crossterm::event::KeyModifiers::NONE,
        ))
        .unwrap();

    assert_eq!(response.draw_ordinal, Some(1));
    let rendered = driver
        .terminal()
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|cell| cell.symbol())
        .collect::<String>();
    assert!(rendered.contains(" Help "));
}

#[cfg(feature = "workload-harness")]
#[test]
fn workload_frame_driver_draw_ordinals_are_contiguous_only_for_draws() {
    let (mut driver, _model_sender, _quality_sender) =
        frame_driver_for_times(&[0, 10, 20, 99, 100, 101, 199, 200]);

    let draw_ordinals = [false, false, true, false, false, true, true, true]
        .into_iter()
        .filter_map(|dirty| driver.step(dirty).unwrap().draw_ordinal)
        .collect::<Vec<_>>();

    assert_eq!(draw_ordinals, vec![0, 1, 2]);
}

#[cfg(feature = "workload-harness")]
#[derive(Clone, Debug)]
struct RealQueueFrame {
    new_probe_count: usize,
    observation: WorkloadFrameObservation,
}

#[cfg(feature = "workload-harness")]
#[derive(Clone, Debug)]
struct RealQueueResult {
    profile: WorkloadProfile,
    trial_origin_ns: u64,
    observer_ready_ns: u64,
    priming_frame_recorded_ns: u64,
    workload_origin_ns: u64,
    admission_observations: Vec<AdmissionObservationV1>,
    terminal_observations: Vec<WorkloadTerminalObservation>,
    screen_observations: Vec<LatencyObservationV1>,
    scoped_observations: Vec<ScopedTimingObservationV1>,
    submitted_sequences: Vec<u64>,
    admitted_sequences: Vec<u64>,
    completed_sequences: Vec<u64>,
    persisted_sequences: Vec<u64>,
    rendered_sequences: Vec<u64>,
    final_identities: workload::StructuralIdentities,
    frames: Vec<RealQueueFrame>,
    carry_in_sample_ordinal: u64,
    next_sample_ordinal: u64,
    next_draw_ordinal: u64,
    performance_samples: Vec<WorkloadPerformanceSample>,
    final_performance: PerformancePublication,
    final_compatibility_quality: ObservationQuality,
}

#[cfg(feature = "workload-harness")]
fn lock_workload<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(feature = "workload-harness")]
fn duration_ns(value: Duration) -> u64 {
    u64::try_from(value.as_nanos()).expect("frozen workload duration must fit u64")
}

#[cfg(feature = "workload-harness")]
fn stamped_publication(
    sample_ordinal: u64,
    sampled_at_ns: u64,
    quality: ObservationQuality,
    reasons: impl IntoIterator<Item = PerformanceDegradationReason>,
) -> PerformancePublication {
    PerformancePublication {
        snapshot: herdr_top::performance::PerformanceSnapshot {
            reasons: reasons.into_iter().collect(),
            ..herdr_top::performance::PerformanceSnapshot::default()
        },
        effective_quality: quality,
        workload_sample_stamp: Some(herdr_top::herdr::collector::WorkloadSampleStamp {
            sample_ordinal,
            sampled_at_ns,
        }),
    }
}

#[cfg(feature = "workload-harness")]
struct AtomicPerformanceClock {
    nanoseconds: Arc<AtomicU64>,
}

#[cfg(feature = "workload-harness")]
impl PerformanceClock for AtomicPerformanceClock {
    fn monotonic_now(&self) -> Duration {
        Duration::from_nanos(self.nanoseconds.load(Ordering::SeqCst))
    }
}

#[cfg(feature = "workload-harness")]
fn structural_identities(model: &DomainModel) -> workload::StructuralIdentities {
    workload::StructuralIdentities {
        pane_ids: model.panes().map(|pane| pane.pane_id.clone()).collect(),
        task_run_ids: model
            .task_runs()
            .filter_map(|run| match &run.key {
                RunKey::Controller(key) => Some(key.clone()),
                RunKey::Native { .. } | RunKey::NativePath { .. } | RunKey::Provisional { .. } => {
                    None
                }
            })
            .collect(),
        dependency_edges: model
            .dependency_edges()
            .map(|edge| format!("{}->{}", edge.prerequisite_run_id, edge.dependent_run_id))
            .collect(),
        execution_edges: model
            .execution_edges()
            .map(|edge| format!("{}->{}", edge.parent_run_id, edge.child_run_id))
            .collect(),
    }
}

#[cfg(feature = "workload-harness")]
fn structural_identities_v1(identities: &workload::StructuralIdentities) -> StructuralIdentitiesV1 {
    StructuralIdentitiesV1 {
        pane_ids: identities.pane_ids.iter().cloned().collect(),
        task_run_ids: identities.task_run_ids.iter().cloned().collect(),
        dependency_edges: identities.dependency_edges.iter().cloned().collect(),
        execution_edges: identities.execution_edges.iter().cloned().collect(),
    }
}

#[cfg(feature = "workload-harness")]
fn convert_workload_timing(sample: WorkloadTimingObservation) -> ScopedTimingObservationV1 {
    let kind = match sample.kind {
        WorkloadTimingKind::ControllerEvent => ScopedTimingKindV1::ControllerEvent,
        WorkloadTimingKind::StartupRestore => ScopedTimingKindV1::StartupRestore,
        WorkloadTimingKind::FallbackNotification => ScopedTimingKindV1::FallbackNotification,
        WorkloadTimingKind::FallbackRescan => ScopedTimingKindV1::FallbackRescan,
    };
    ScopedTimingObservationV1 {
        kind,
        sequence: sample.sequence,
        d4_segment_count: sample.d4_segment_count,
        d4_analysis_ns: sample.d4_analysis_ns,
        reducer_plus_publish_ns: sample.reducer_plus_publish_ns,
        model_clone_publish_segment_count: sample.model_clone_publish_segment_count,
        model_clone_publish_ns: sample.model_clone_publish_ns,
        render_ns: 0,
    }
}

#[cfg(feature = "workload-harness")]
fn workload_timing_collector(
    observations: Arc<Mutex<Vec<ScopedTimingObservationV1>>>,
) -> WorkloadTimingObserver {
    Arc::new(move |sample| {
        lock_workload(&observations).push(convert_workload_timing(sample));
    })
}

#[cfg(feature = "workload-harness")]
fn empty_restored(model: DomainModel) -> RestoredState {
    RestoredState {
        model,
        next_ordinal: 201,
        next_ingest_seq: Some(1),
        event_ledger: Vec::new(),
    }
}

#[cfg(feature = "workload-harness")]
async fn wait_for_sequence_count(values: &Arc<Mutex<Vec<u64>>>, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if lock_workload(values).len() == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real workload sequence acknowledgements must complete");
}

#[cfg(feature = "workload-harness")]
async fn wait_for_performance_sample(
    samples: &Arc<Mutex<Vec<WorkloadPerformanceSample>>>,
    notification: &tokio::sync::Notify,
    predicate: impl Fn(&[WorkloadPerformanceSample]) -> bool,
    message: &'static str,
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let notified = notification.notified();
            if predicate(&lock_workload(samples)) {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect(message);
}

#[cfg(feature = "workload-harness")]
fn freeze_performance_samples_at_close(
    samples: &[WorkloadPerformanceSample],
    carry_in_sample_ordinal: u64,
    workload_close_ns: u64,
) -> (Vec<WorkloadPerformanceSample>, u64) {
    // Read the recorder's authoritative watermark before checking close-bounded coverage; deriving
    // it from a filtered suffix would let an already-recorded omission look like a smaller stream.
    let next_sample_ordinal = samples
        .last()
        .and_then(|sample| sample.publication.workload_sample_stamp)
        .and_then(|stamp| stamp.sample_ordinal.checked_add(1))
        .expect("frozen performance recorder watermark must fit u64");
    let suffix = samples
        .iter()
        .filter(|sample| {
            sample
                .publication
                .workload_sample_stamp
                .is_some_and(|stamp| stamp.sample_ordinal >= carry_in_sample_ordinal)
        })
        .cloned()
        .collect::<Vec<_>>();
    let included_next_sample_ordinal = suffix
        .last()
        .and_then(|sample| sample.publication.workload_sample_stamp)
        .and_then(|stamp| stamp.sample_ordinal.checked_add(1))
        .expect("close-bounded performance sample watermark must fit u64");
    assert_eq!(
        included_next_sample_ordinal, next_sample_ordinal,
        "closing freeze must not omit an observer-linearized sample"
    );
    assert!(
        suffix.iter().all(|sample| {
            sample
                .publication
                .workload_sample_stamp
                .is_some_and(|stamp| stamp.sampled_at_ns <= workload_close_ns)
        }),
        "closing freeze must occur before any post-close observer append"
    );
    (suffix, next_sample_ordinal)
}

#[cfg(feature = "workload-harness")]
fn last_pre_origin_sample_ordinal(
    samples: &[WorkloadPerformanceSample],
    workload_origin_ns: u64,
) -> u64 {
    // The producer primes a carry-in before choosing the origin, and the
    // artifact validator requires exactly one pre-origin sample. Keep this
    // fail-fast so a producer regression cannot silently weaken the anchor.
    samples
        .iter()
        .rev()
        .find_map(|sample| {
            sample
                .publication
                .workload_sample_stamp
                .filter(|stamp| stamp.sampled_at_ns < workload_origin_ns)
                .map(|stamp| stamp.sample_ordinal)
        })
        .expect("workload origin must follow a raw carry-in performance sample")
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn reference_epoch_wait_uses_an_injected_portable_clock() {
    let now = AtomicU64::new(100);
    let clock = || now.fetch_add(5, Ordering::SeqCst);

    wait_for_reference_epoch(&clock, 110).await.unwrap();

    assert_eq!(now.load(Ordering::SeqCst), 115);
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn workload_harness_writer_access_uses_owned_client() {
    let temporary = tempfile::tempdir().unwrap();
    let root = StateRoot(temporary.path().join("state"));
    std::fs::create_dir_all(&root.0).unwrap();
    let store = open_writer(&root).unwrap();
    let (lifecycle, mut writer) = spawn_writer(store).unwrap();
    let writer_access = &mut writer;
    writer_access
        .apply(vec![PersistOp::UpsertWorkspace {
            workspace: Workspace {
                workspace_id: "owned-client-seed".to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
        }])
        .await
        .unwrap();
    let persistence = writer_access.subscribe_persistence();
    let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
    assert!(restored.model.workspace("owned-client-seed").is_some());

    let config = WorkloadCollectorConfig {
        controller_hooks: WorkloadControllerHooks {
            clock: Arc::new(|| 1),
            admission_observer: Arc::new(|_| {}),
            terminal_observer: Arc::new(|_| {}),
            persistence_observer: Arc::new(|_| {}),
            timing_observer: Arc::new(|_| {}),
        },
        performance_clock: Arc::new(TestPerformanceClock::new(Duration::from_nanos(1))),
        performance_observer: Arc::new(|_| {}),
        provider_roots: Vec::new(),
        notify_factory: None,
        rescan_interval: Some(Duration::from_secs(2)),
        fallback_timing: None,
    };
    let handle = spawn_workload_collector(
        temporary.path().join("missing-herdr.sock"),
        "owned-client-workload".to_owned(),
        restored,
        writer,
        config,
    )
    .await
    .unwrap();
    let receipt_time_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let response = handle
        .controller
        .submit_workload_frame(
            serde_json::to_vec(&ControllerEnvelope {
                schema_version: 1,
                event_id: "owned-client-later".to_owned(),
                emitted_at_ms: 2,
                source: "owned-client-test".to_owned(),
                event_type: "task_started".to_owned(),
                task_run_id: "owned-client-run".to_owned(),
                parent_task_run_id: None,
                depends_on_id: None,
                label: None,
                reason: None,
                progress: None,
                provider: None,
                native_session_id: None,
                terminal_id: None,
            })
            .unwrap(),
            receipt_time_ms,
            1,
            1,
        )
        .await;
    assert_eq!(response, ControllerResponse::Accepted);

    let diagnostics = handle.collector.diagnostics.clone();
    // This durable-row poll is a liveness wait, not a barrier: rows commit before cleanup/failure publication, so the health guard cannot catch a post-commit failure.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            assert_eq!(
                *persistence.borrow(),
                herdr_top::store::PersistenceStatus::Healthy
            );
            assert_eq!(
                diagnostics.borrow().persistence,
                herdr_top::store::PersistenceStatus::Healthy
            );
            let durable_rows: i64 =
                rusqlite::Connection::open(herdr_top::store::database_path(&root))
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM events WHERE event_id = 'owned-client-later'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
            if durable_rows == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owned writer persistence must become visible through durable rows");
    handle.collector.stop().await.unwrap();
    lifecycle.shutdown().await.unwrap();
}

#[cfg(feature = "workload-harness")]
#[allow(clippy::too_many_arguments)]
async fn run_schedule_through_real_queue_at(
    profile: WorkloadProfile,
    trial_origin_ns: u64,
    observer_ready_ns: u64,
    requested_workload_origin_ns: Option<u64>,
    frame_phase_offset_ns: u64,
    stall: Option<Duration>,
    injection: PerformanceTrialInjection,
    clock_override: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
) -> RealQueueResult {
    let temporary = tempfile::tempdir().unwrap();
    let root = StateRoot(temporary.path().join("state"));
    std::fs::create_dir_all(&root.0).unwrap();
    let store = open_writer(&root).unwrap();
    let (lifecycle, writer) = spawn_writer(store).unwrap();
    let herdr = MockHerdr::start(
        MockConfig::default().respond("session.snapshot", target_snapshot_result()),
    )
    .await
    .unwrap();
    let clock_ns = Arc::new(AtomicU64::new(trial_origin_ns));
    let uses_realtime_clock = clock_override.is_some();
    let clock: Arc<dyn Fn() -> u64 + Send + Sync> = clock_override.unwrap_or_else(|| {
        let clock_ns = Arc::clone(&clock_ns);
        Arc::new(move || clock_ns.load(Ordering::SeqCst))
    });
    let workload_hook_clock: Arc<dyn Fn() -> u64 + Send + Sync> = if uses_realtime_clock {
        Arc::clone(&clock)
    } else {
        let clock_ns = Arc::clone(&clock_ns);
        // Give reducer lifecycle callbacks strict causal order without a wall-clock wait.
        Arc::new(move || {
            clock_ns
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_add(1)
                })
                .expect("virtual workload hook timestamp must fit u64")
                + 1
        })
    };
    #[cfg(target_os = "linux")]
    let performance_clock: Arc<dyn PerformanceClock> = if uses_realtime_clock {
        Arc::new(AbsoluteMonotonicPerformanceClock)
    } else {
        Arc::new(AtomicPerformanceClock {
            nanoseconds: Arc::clone(&clock_ns),
        })
    };
    #[cfg(not(target_os = "linux"))]
    let performance_clock: Arc<dyn PerformanceClock> = Arc::new(AtomicPerformanceClock {
        nanoseconds: Arc::clone(&clock_ns),
    });
    let admissions = Arc::new(Mutex::new(Vec::new()));
    let terminals = Arc::new(Mutex::new(Vec::new()));
    let persisted = Arc::new(Mutex::new(Vec::new()));
    let timings = Arc::new(Mutex::new(Vec::new()));
    let performance_samples = Arc::new(Mutex::new(Vec::new()));
    let performance_notification = Arc::new(tokio::sync::Notify::new());

    let hooks = WorkloadControllerHooks {
        clock: workload_hook_clock,
        admission_observer: {
            let admissions = Arc::clone(&admissions);
            Arc::new(move |sample: WorkloadAdmissionObservation| {
                lock_workload(&admissions).push(AdmissionObservationV1 {
                    sequence: sample.sequence,
                    scheduled_ns: sample.scheduled_ns,
                    admitted_ns: sample.admitted_ns,
                });
            })
        },
        terminal_observer: {
            let terminals = Arc::clone(&terminals);
            Arc::new(move |sample: WorkloadTerminalObservation| {
                lock_workload(&terminals).push(sample);
            })
        },
        persistence_observer: {
            let persisted = Arc::clone(&persisted);
            Arc::new(move |sample: WorkloadPersistenceObservation| {
                lock_workload(&persisted).push(sample.sequence);
            })
        },
        timing_observer: workload_timing_collector(Arc::clone(&timings)),
    };
    let config = WorkloadCollectorConfig {
        controller_hooks: hooks,
        performance_clock,
        performance_observer: {
            let performance_samples = Arc::clone(&performance_samples);
            let performance_notification = Arc::clone(&performance_notification);
            Arc::new(move |sample| {
                lock_workload(&performance_samples).push(sample.clone());
                performance_notification.notify_waiters();
            })
        },
        provider_roots: Vec::new(),
        notify_factory: None,
        rescan_interval: Some(Duration::from_secs(2)),
        fallback_timing: None,
    };
    let handle = spawn_workload_collector(
        herdr.socket_path().to_path_buf(),
        "increment5-workload".to_owned(),
        empty_restored(workload::target_model()),
        writer,
        config,
    )
    .await
    .unwrap();
    let mut ready_quality = handle.collector.quality.clone();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if *ready_quality.borrow() == ObservationQuality::Live {
                break;
            }
            ready_quality
                .changed()
                .await
                .expect("workload performance monitor should remain available");
        }
    })
    .await
    .expect("workload Herdr source should become live before scheduling");
    let mut performance_watch = handle.collector.performance.clone();
    let model_receiver = handle.collector.model.clone();
    let app = App::new(
        model_receiver,
        HeaderInputs {
            performance: handle.collector.performance.clone(),
            ..HeaderInputs::default()
        },
    );
    let terminal = Terminal::new(TestBackend::new(160, 48)).unwrap();
    let driver_clock = Arc::clone(&clock);
    let mut driver = WorkloadFrameDriver::new_with_header_projection(
        app,
        terminal,
        move || Duration::from_nanos(driver_clock()),
        injection == PerformanceTrialInjection::OmitRequiredRenderedReason,
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let _ = driver.refresh_app_if_changed().unwrap();
            if driver
                .cached_performance_publication()
                .workload_sample_stamp
                .is_some()
            {
                break;
            }
            performance_watch
                .changed()
                .await
                .expect("performance monitor should publish the carry-in generation");
        }
    })
    .await
    .expect("the first stamped publication must reach the ordinary App cache");

    let priming_ns = match requested_workload_origin_ns {
        Some(workload_origin_ns) => {
            let priming_ns = workload_origin_ns
                .checked_sub(100_000_000 - frame_phase_offset_ns)
                .expect("virtual workload origin must follow its priming frame");
            clock_ns.store(priming_ns, Ordering::SeqCst);
            priming_ns
        }
        None => clock(),
    };
    assert_eq!(driver.step(true).unwrap().draw_ordinal, Some(0));
    let priming_frame_recorded_ns = if uses_realtime_clock {
        clock()
    } else {
        priming_ns
    };
    let workload_origin_ns = requested_workload_origin_ns.unwrap_or_else(|| {
        priming_frame_recorded_ns
            .checked_add(100_000_000 - frame_phase_offset_ns)
            .expect("reference workload origin must follow its priming frame")
    });
    if uses_realtime_clock {
        wait_for_reference_epoch(clock.as_ref(), workload_origin_ns)
            .await
            .expect("reference workload epoch must be reachable");
    }
    let carry_in_sample_ordinal =
        last_pre_origin_sample_ordinal(&lock_workload(&performance_samples), workload_origin_ns);

    let probes = workload::screen_probe_sequences(profile)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let closing_probe_sequence = *probes
        .last()
        .expect("closed scheduled profiles must declare a closing probe");
    let stall_deadline = stall.map(|duration| {
        workload_origin_ns
            .checked_add(duration_ns(duration))
            .expect("virtual stall deadline must fit u64")
    });
    let mut submitted_sequences = Vec::new();
    let mut rendered_sequences = Vec::new();
    let mut screen_observations = Vec::new();
    let mut pending_probes = Vec::new();
    let mut frames = Vec::new();

    for (index, (wire, offset)) in workload::frozen_controller_events(profile)
        .into_iter()
        .zip(workload::admission_offsets(profile))
        .enumerate()
    {
        let sequence = index as u64 + 1;
        let scheduled_ns = workload_origin_ns
            .checked_add(duration_ns(offset))
            .expect("virtual scheduled timestamp must fit u64");
        if uses_realtime_clock {
            wait_for_reference_epoch(clock.as_ref(), scheduled_ns)
                .await
                .expect("reference admission deadline must be reachable");
        } else {
            let supported_load_shift_count = injection.supported_load_shift_count();
            let admitted_clock_ns = if sequence <= supported_load_shift_count {
                let shift_periods = supported_load_shift_count
                    .checked_sub(sequence)
                    .and_then(|remaining| remaining.checked_add(1))
                    .expect("shifted admission sequence must have a positive displacement");
                scheduled_ns
                    .checked_add(
                        duration_ns(workload::period(profile))
                            .checked_mul(shift_periods)
                            .expect("degradation injection shift must fit u64"),
                    )
                    .expect("degradation injection admission timestamp must fit u64")
            } else {
                scheduled_ns
            };
            if supported_load_shift_count > 0 {
                // Every shifted admission lands on the first unshifted admission's scheduled
                // base. Preserve that bucket while keeping later raw samples strictly ordered.
                clock_ns
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                        if admitted_clock_ns > current {
                            Some(admitted_clock_ns)
                        } else {
                            current.checked_add(1)
                        }
                    })
                    .expect("degradation injection admission timestamp must fit u64");
            } else {
                clock_ns.store(admitted_clock_ns, Ordering::SeqCst);
            }
        }
        submitted_sequences.push(sequence);
        let response = handle
            .controller
            .submit_workload_frame(
                serde_json::to_vec(&wire).unwrap(),
                i64::try_from(scheduled_ns / 1_000_000).unwrap(),
                sequence,
                scheduled_ns,
            )
            .await;
        assert_eq!(response, ControllerResponse::Accepted);
        if profile == WorkloadProfile::TwiceTarget && sequence == 1_201 {
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if performance_watch
                        .borrow()
                        .snapshot
                        .reasons
                        .contains(&PerformanceDegradationReason::EventsSixtySeconds)
                    {
                        break;
                    }
                    performance_watch
                        .changed()
                        .await
                        .expect("twice-target performance publication should remain available");
                }
            })
            .await
            .expect("the stable sixty-second reason must publish after admission 1,201");
        }
        let supported_load_shift_count = injection.supported_load_shift_count();
        if supported_load_shift_count > 0 && sequence == 100 + supported_load_shift_count {
            let expected_events_one_second = usize::try_from(100 + supported_load_shift_count)
                .expect("supported-load injected event count must fit usize");
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let injected_boundary_is_recorded = {
                        let publication = performance_watch.borrow();
                        publication.snapshot.events_one_second == expected_events_one_second
                            && publication
                                .snapshot
                                .reasons
                                .contains(&PerformanceDegradationReason::EventsOneSecond)
                    };
                    if injected_boundary_is_recorded {
                        break;
                    }
                    performance_watch
                        .changed()
                        .await
                        .expect("supported-load performance publication should remain available");
                }
            })
            .await
            .expect("the truthful rolling-window breach must publish at the injected boundary");
        }
        if probes.contains(&sequence) {
            pending_probes.push(sequence);
        }
        let stall_holds = stall_deadline.is_some_and(|deadline| scheduled_ns < deadline);
        if !pending_probes.is_empty()
            && (!stall_holds || stall.is_none())
            && probes.contains(&sequence)
            && sequence < closing_probe_sequence
        {
            let observation = if uses_realtime_clock {
                loop {
                    let frame = driver.step(false).unwrap();
                    if frame.draw_ordinal.is_some() {
                        break frame;
                    }
                    tokio::time::sleep(frame.poll_duration).await;
                }
            } else {
                let rendered_ns = scheduled_ns
                    .checked_add(frame_phase_offset_ns)
                    .expect("virtual rendered timestamp must fit u64");
                clock_ns.store(rendered_ns, Ordering::SeqCst);
                let frame = driver.step(true).unwrap();
                assert!(frame.draw_ordinal.is_some());
                frame
            };
            let rendered_ns = duration_ns(
                observation
                    .rendered_at
                    .expect("successful workload draw must record its timestamp"),
            );
            let new_probe_count = pending_probes.len();
            for rendered_sequence in pending_probes.drain(..) {
                let terminal = lock_workload(&terminals)
                    .iter()
                    .find(|sample| sample.sequence == rendered_sequence)
                    .cloned()
                    .expect("every rendered probe must have a terminal observation");
                let admission = lock_workload(&admissions)
                    .iter()
                    .find(|sample| sample.sequence == rendered_sequence)
                    .cloned()
                    .expect("every rendered probe must have an admission observation");
                rendered_sequences.push(rendered_sequence);
                screen_observations.push(LatencyObservationV1 {
                    sequence: rendered_sequence,
                    admitted_ns: admission.admitted_ns,
                    terminal_ns: terminal.terminal_ns,
                    published_ns: terminal.published_ns,
                    rendered_ns,
                    observed_frame_phase_ns: rendered_ns
                        .checked_sub(admission.admitted_ns)
                        .unwrap()
                        % 100_000_000,
                });
            }
            frames.push(RealQueueFrame {
                new_probe_count,
                observation,
            });
        }
    }
    wait_for_sequence_count(&persisted, submitted_sequences.len()).await;
    let expected_high_water = u64::try_from(submitted_sequences.len()).unwrap();
    let mut performance = handle.collector.performance.clone();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let final_generation_is_published = {
                let publication = performance.borrow();
                let snapshot = &publication.snapshot;
                let has_final_admission_state = snapshot.admission_high_water
                    == expected_high_water
                    && snapshot.completion_high_water == expected_high_water
                    && snapshot.pending_events == 0;
                let has_expected_terminal_quality = match profile {
                    WorkloadProfile::TargetBurst if injection.supported_load_shift_count() > 0 => {
                        // The rolling window drains by the final persisted generation. The
                        // retained 101/102 breach sample determines the composed outcome below.
                        snapshot.reasons.is_empty()
                            && publication.effective_quality == ObservationQuality::Live
                    }
                    WorkloadProfile::SustainedTarget | WorkloadProfile::TargetBurst => {
                        snapshot.reasons.is_empty()
                            && publication.effective_quality == ObservationQuality::Live
                    }
                    WorkloadProfile::TwiceTarget => {
                        snapshot
                            .reasons
                            .contains(&PerformanceDegradationReason::EventsSixtySeconds)
                            && publication.effective_quality == ObservationQuality::Degraded
                    }
                    _ => panic!("real queue performance fixture requires a scheduled profile"),
                };
                has_final_admission_state && has_expected_terminal_quality
            };
            if final_generation_is_published {
                break;
            }
            performance
                .changed()
                .await
                .expect("performance monitor should remain available");
        }
    })
    .await
    .expect("final real-queue performance generation must publish after persistence completion");
    assert!(
        !pending_probes.is_empty(),
        "the closed scheduled profiles must retain their final probe for the closing draw"
    );
    let (closing_observation, performance_samples_at_close, next_sample_ordinal) =
        if uses_realtime_clock {
            loop {
                // The recorder mutex is the close linearization boundary. A monitor tick that
                // starts after this lock is released belongs to the trailing barrier, not the stream.
                let samples = lock_workload(&performance_samples);
                let frame = driver.step(true).unwrap();
                if frame.draw_ordinal.is_some() {
                    let closing_rendered_ns = duration_ns(
                        frame
                            .rendered_at
                            .expect("successful closing draw must record its timestamp"),
                    );
                    let (frozen, next_sample_ordinal) = freeze_performance_samples_at_close(
                        &samples,
                        carry_in_sample_ordinal,
                        closing_rendered_ns,
                    );
                    break (frame, frozen, next_sample_ordinal);
                }
                drop(samples);
                tokio::time::sleep(frame.poll_duration).await;
            }
        } else {
            let closing_ns = lock_workload(&admissions)
                .last()
                .expect("closing draw requires the final admission")
                .scheduled_ns
                .checked_add(frame_phase_offset_ns)
                .expect("closing draw timestamp must fit u64");
            clock_ns.store(closing_ns, Ordering::SeqCst);
            // Match the real-clock close boundary even though the injected clock cannot advance.
            let samples = lock_workload(&performance_samples);
            let frame = driver.step(true).unwrap();
            assert!(frame.draw_ordinal.is_some());
            let (frozen, next_sample_ordinal) =
                freeze_performance_samples_at_close(&samples, carry_in_sample_ordinal, closing_ns);
            (frame, frozen, next_sample_ordinal)
        };
    let closing_rendered_ns = duration_ns(
        closing_observation
            .rendered_at
            .expect("successful closing draw must record its timestamp"),
    );
    let closing_probe_count = pending_probes.len();
    for rendered_sequence in pending_probes.drain(..) {
        let terminal = lock_workload(&terminals)
            .iter()
            .find(|sample| sample.sequence == rendered_sequence)
            .cloned()
            .expect("every closing probe must have a terminal observation");
        let admission = lock_workload(&admissions)
            .iter()
            .find(|sample| sample.sequence == rendered_sequence)
            .cloned()
            .expect("every closing probe must have an admission observation");
        rendered_sequences.push(rendered_sequence);
        screen_observations.push(LatencyObservationV1 {
            sequence: rendered_sequence,
            admitted_ns: admission.admitted_ns,
            terminal_ns: terminal.terminal_ns,
            published_ns: terminal.published_ns,
            rendered_ns: closing_rendered_ns,
            observed_frame_phase_ns: closing_rendered_ns
                .checked_sub(admission.admitted_ns)
                .unwrap()
                % 100_000_000,
        });
    }
    frames.push(RealQueueFrame {
        new_probe_count: closing_probe_count,
        observation: closing_observation,
    });
    let raw_ordinal_before_trailing = next_sample_ordinal
        .checked_sub(1)
        .expect("closing performance watermark must follow an included sample");
    wait_for_performance_sample(
        &performance_samples,
        performance_notification.as_ref(),
        |samples| {
            samples.last().is_some_and(|sample| {
                sample
                    .publication
                    .workload_sample_stamp
                    .is_some_and(|stamp| stamp.sample_ordinal > raw_ordinal_before_trailing)
            })
        },
        "a trailing equal raw sample must follow the successful closing frame",
    )
    .await;
    let final_identities = structural_identities(&handle.collector.model.borrow());
    let admission_observations = lock_workload(&admissions).clone();
    let admitted_sequences = admission_observations
        .iter()
        .map(|sample| sample.sequence)
        .collect();
    let terminal_observations = lock_workload(&terminals).clone();
    let completed_sequences = terminal_observations
        .iter()
        .map(|sample| sample.sequence)
        .collect();
    let persisted_sequences = lock_workload(&persisted).clone();
    let scoped_observations = lock_workload(&timings).clone();
    let next_draw_ordinal = driver.next_draw_ordinal();
    let final_performance = handle.collector.performance.borrow().clone();
    let final_compatibility_quality = *handle.collector.quality.borrow();
    handle.collector.stop().await.unwrap();
    lifecycle.shutdown().await.unwrap();

    RealQueueResult {
        profile,
        trial_origin_ns,
        observer_ready_ns,
        priming_frame_recorded_ns,
        workload_origin_ns,
        admission_observations,
        terminal_observations,
        screen_observations,
        scoped_observations,
        submitted_sequences,
        admitted_sequences,
        completed_sequences,
        persisted_sequences,
        rendered_sequences,
        final_identities,
        frames,
        carry_in_sample_ordinal,
        next_sample_ordinal,
        next_draw_ordinal,
        performance_samples: performance_samples_at_close,
        final_performance,
        final_compatibility_quality,
    }
}

#[cfg(feature = "workload-harness")]
fn target_snapshot_result() -> serde_json::Value {
    let panes = (1..=50)
        .map(|index| {
            serde_json::json!({
                "pane_id": format!("pane-{index:04}"),
                "terminal_id": format!("terminal-{index:04}"),
                "workspace_id": "workspace-0001",
                "tab_id": "tab-0001",
                "focused": false,
                "agent_status": "unknown",
                "revision": 1,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "type": "session_snapshot",
        "snapshot": {
            "version": "1",
            "protocol": 1,
            "focused_workspace_id": null,
            "focused_tab_id": null,
            "focused_pane_id": null,
            "workspaces": [{
                "workspace_id": "workspace-0001",
                "number": 1,
                "label": "workload",
                "focused": false,
                "pane_count": 50,
                "tab_count": 1,
                "active_tab_id": "tab-0001",
                "agent_status": "unknown",
            }],
            "tabs": [{
                "tab_id": "tab-0001",
                "workspace_id": "workspace-0001",
                "number": 1,
                "label": "workload",
                "focused": false,
                "pane_count": 50,
                "agent_status": "unknown",
            }],
            "panes": panes,
            "layouts": [],
            "agents": [],
        }
    })
}

#[cfg(feature = "workload-harness")]
async fn run_virtual_schedule_through_real_queue_at(
    profile: WorkloadProfile,
    trial_origin_ns: u64,
    observer_ready_ns: u64,
    workload_origin_ns: u64,
    stall: Option<Duration>,
) -> RealQueueResult {
    run_schedule_through_real_queue_at(
        profile,
        trial_origin_ns,
        observer_ready_ns,
        Some(workload_origin_ns),
        30_000_000,
        stall,
        PerformanceTrialInjection::None,
        None,
    )
    .await
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
async fn run_reference_schedule_through_real_queue(
    profile: WorkloadProfile,
    trial_origin_ns: u64,
    observer_ready_ns: u64,
    desired_phase_ns: u64,
) -> RealQueueResult {
    let clock: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(|| {
        reference_monotonic_ns().expect("reference monotonic clock must remain readable")
    });
    run_schedule_through_real_queue_at(
        profile,
        trial_origin_ns,
        observer_ready_ns,
        None,
        desired_phase_ns,
        None,
        PerformanceTrialInjection::None,
        Some(clock),
    )
    .await
}

#[cfg(feature = "workload-harness")]
async fn run_virtual_schedule_through_real_queue(profile: WorkloadProfile) -> RealQueueResult {
    run_virtual_schedule_through_real_queue_at(
        profile,
        1_000_000_000,
        1_100_000_000,
        2_000_000_000,
        None,
    )
    .await
}

#[cfg(feature = "workload-harness")]
async fn run_real_queue_with_frame_stall(
    profile: WorkloadProfile,
    stall: Duration,
) -> RealQueueResult {
    run_virtual_schedule_through_real_queue_at(
        profile,
        1_000_000_000,
        1_100_000_000,
        2_000_000_000,
        Some(stall),
    )
    .await
}

#[cfg(feature = "workload-harness")]
async fn run_virtual_schedule_after_delayed_ready_and_setup() -> RealQueueResult {
    run_virtual_schedule_through_real_queue_at(
        WorkloadProfile::SustainedTarget,
        1_000_000_000,
        3_000_000_000,
        7_000_000_000,
        None,
    )
    .await
}

#[cfg(feature = "workload-harness")]
fn admission_schedule_attained(
    profile: WorkloadProfile,
    workload_origin_ns: u64,
    observations: &[AdmissionObservationV1],
) -> bool {
    observations.len() == workload::admission_offsets(profile).len()
        && observations
            .iter()
            .zip(workload::admission_offsets(profile))
            .enumerate()
            .all(|(index, (sample, offset))| {
                sample.sequence == index as u64 + 1
                    && sample.scheduled_ns
                        == workload_origin_ns.checked_add(duration_ns(offset)).unwrap()
                    && sample.admitted_ns == sample.scheduled_ns
            })
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn real_controller_queue_profiles_are_lossless() {
    for profile in [
        WorkloadProfile::SustainedTarget,
        WorkloadProfile::TargetBurst,
        WorkloadProfile::TwiceTarget,
    ] {
        let result = run_virtual_schedule_through_real_queue(profile).await;
        assert_eq!(result.submitted_sequences, result.admitted_sequences);
        assert_eq!(result.admitted_sequences, result.completed_sequences);
        assert_eq!(result.completed_sequences, result.persisted_sequences);
        assert_eq!(
            result.rendered_sequences,
            workload::screen_probe_sequences(profile)
        );
        assert!(admission_schedule_attained(
            profile,
            result.workload_origin_ns,
            &result.admission_observations
        ));
        assert!(result.screen_observations.iter().all(|sample| {
            sample
                .rendered_ns
                .checked_sub(sample.admitted_ns)
                .map(|elapsed| elapsed % 100_000_000)
                == Some(sample.observed_frame_phase_ns)
        }));
        assert_eq!(
            result.final_identities,
            workload::oracle(profile).final_identities
        );
    }
}

#[cfg(feature = "workload-harness")]
fn assert_real_queue_outcome_is_exact(result: &RealQueueResult, profile: WorkloadProfile) {
    let expected = workload::oracle(profile);
    assert_eq!(result.submitted_sequences, expected.admitted_sequences);
    assert_eq!(result.submitted_sequences, result.admitted_sequences);
    assert_eq!(result.admitted_sequences, result.completed_sequences);
    assert_eq!(result.completed_sequences, result.persisted_sequences);
    assert_eq!(result.final_identities, expected.final_identities);
    assert!(!result.performance_samples.is_empty());
    assert!(
        result
            .performance_samples
            .iter()
            .all(
                |sample| sample.publication.snapshot.live_panes == expected.live_panes
                    && sample.publication.snapshot.default_visible_task_runs
                        == expected.visible_runs
                    && sample.publication.snapshot.dependency_edges == expected.dependency_edges
                    && sample.publication.snapshot.execution_edges == expected.execution_edges
            )
    );
}

#[cfg(feature = "workload-harness")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PerformanceTrialInjection {
    None,
    SupportedLoadDegradation,
    SupportedLoadBeyondTolerance,
    OmitRequiredRenderedReason,
}

#[cfg(feature = "workload-harness")]
impl PerformanceTrialInjection {
    fn supported_load_shift_count(self) -> u64 {
        match self {
            Self::SupportedLoadDegradation => 1,
            Self::SupportedLoadBeyondTolerance => 2,
            Self::None | Self::OmitRequiredRenderedReason => 0,
        }
    }
}

#[cfg(feature = "workload-harness")]
struct FinalPerformanceTrialResult {
    outcome: ReferenceOutcomeV1,
    workload_origin_ns: u64,
    admission_observations: Vec<AdmissionObservationV1>,
    carry_in_sample_ordinal: u64,
}

#[cfg(feature = "workload-harness")]
fn evidence_quality(quality: ObservationQuality) -> EffectiveQualityV1 {
    match quality {
        ObservationQuality::Live => EffectiveQualityV1::Live,
        ObservationQuality::Reconciling => EffectiveQualityV1::Reconciling,
        ObservationQuality::Disconnected => EffectiveQualityV1::Disconnected,
        ObservationQuality::Degraded => EffectiveQualityV1::Degraded,
    }
}

#[cfg(feature = "workload-harness")]
fn evidence_reason(reason: PerformanceDegradationReason) -> PerformanceReasonV1 {
    match reason {
        PerformanceDegradationReason::LivePanes => PerformanceReasonV1::LivePanes,
        PerformanceDegradationReason::DefaultVisibleTaskRuns => {
            PerformanceReasonV1::DefaultVisibleTaskRuns
        }
        PerformanceDegradationReason::DependencyEdges => PerformanceReasonV1::DependencyEdges,
        PerformanceDegradationReason::EventsOneSecond => PerformanceReasonV1::EventsOneSecond,
        PerformanceDegradationReason::EventsTenSeconds => PerformanceReasonV1::EventsTenSeconds,
        PerformanceDegradationReason::EventsSixtySeconds => PerformanceReasonV1::EventsSixtySeconds,
        PerformanceDegradationReason::EventLag => PerformanceReasonV1::EventLag,
    }
}

#[cfg(feature = "workload-harness")]
fn performance_sample_evidence(sample: &WorkloadPerformanceSample) -> PerformanceSampleEvidenceV1 {
    let stamp = sample
        .publication
        .workload_sample_stamp
        .expect("raw workload samples must carry an exact stamp");
    let snapshot = &sample.publication.snapshot;
    PerformanceSampleEvidenceV1 {
        sample_ordinal: stamp.sample_ordinal,
        sampled_at_ns: stamp.sampled_at_ns,
        event_lag_ns: duration_ns(snapshot.event_lag),
        pending_events: u64::try_from(snapshot.pending_events).unwrap(),
        admission_high_water: snapshot.admission_high_water,
        completion_high_water: snapshot.completion_high_water,
        live_panes: u64::try_from(snapshot.live_panes).unwrap(),
        default_visible_task_runs: u64::try_from(snapshot.default_visible_task_runs).unwrap(),
        dependency_edges: u64::try_from(snapshot.dependency_edges).unwrap(),
        execution_edges: u64::try_from(snapshot.execution_edges).unwrap(),
        events_one_second: u64::try_from(snapshot.events_one_second).unwrap(),
        events_ten_seconds: u64::try_from(snapshot.events_ten_seconds).unwrap(),
        events_sixty_seconds: u64::try_from(snapshot.events_sixty_seconds).unwrap(),
        source_quality: evidence_quality(sample.source_quality),
        effective_quality: evidence_quality(sample.publication.effective_quality),
        reasons: snapshot
            .reasons
            .iter()
            .copied()
            .map(evidence_reason)
            .collect(),
    }
}

#[cfg(feature = "workload-harness")]
fn canonical_rendered_header(raw_header: &str, publication: &PerformancePublication) -> String {
    let quality = match publication.effective_quality {
        ObservationQuality::Live => "LIVE",
        ObservationQuality::Reconciling => "RECONCILING",
        ObservationQuality::Disconnected => "DISCONNECTED",
        ObservationQuality::Degraded => "DEGRADED",
    };
    assert!(
        raw_header.contains(quality),
        "raw header omitted its cached effective quality: {raw_header}"
    );
    let rendered_labels = raw_header
        .split_once("perf:")
        .map(|(_, suffix)| {
            suffix
                .split(" | ")
                .next()
                .unwrap_or_default()
                .split('│')
                .next()
                .unwrap_or_default()
                .trim()
                .replace('+', ",")
        })
        .unwrap_or_default();
    format!("{quality} | perf:{rendered_labels}")
}

#[cfg(feature = "workload-harness")]
fn performance_frame_evidence(frame: &RealQueueFrame) -> PerformanceFrameEvidenceV1 {
    let publication = frame
        .observation
        .performance_publication
        .as_ref()
        .expect("recorded workload frames must name the exact App cache");
    let stamp = publication
        .workload_sample_stamp
        .expect("recorded workload frames must name a stamped publication");
    let raw_header = frame
        .observation
        .rendered_header_line
        .as_deref()
        .expect("recorded workload frames must retain the raw header");
    PerformanceFrameEvidenceV1 {
        draw_ordinal: frame.observation.draw_ordinal.unwrap(),
        sample_ordinal: stamp.sample_ordinal,
        state_observed_at_ns: stamp.sampled_at_ns,
        rendered_at_ns: duration_ns(frame.observation.rendered_at.unwrap()),
        effective_quality: evidence_quality(publication.effective_quality),
        reasons: publication
            .snapshot
            .reasons
            .iter()
            .copied()
            .map(evidence_reason)
            .collect(),
        rendered_header_line: canonical_rendered_header(raw_header, publication),
    }
}

#[cfg(feature = "workload-harness")]
fn performance_stream(result: &RealQueueResult) -> PerformanceEvidenceStreamV1 {
    let samples = result
        .performance_samples
        .iter()
        .map(performance_sample_evidence)
        .collect::<Vec<_>>();
    let frames = result
        .frames
        .iter()
        .map(performance_frame_evidence)
        .collect::<Vec<_>>();
    let mut terminal_observations = result
        .terminal_observations
        .iter()
        .map(|observation| TerminalObservationV1 {
            sequence: observation.sequence,
            terminal_ns: observation.terminal_ns,
        })
        .collect::<Vec<_>>();
    terminal_observations.sort_by_key(|observation| observation.sequence);
    let workload_close_ns = frames.last().unwrap().rendered_at_ns;
    let deadline = result.workload_origin_ns + 60_000_000_000;
    let selected_terminal_draw_ordinal = (result.profile == WorkloadProfile::TwiceTarget)
        .then(|| {
            frames.iter().find(|frame| {
                frame.rendered_at_ns <= deadline
                    && frame
                        .reasons
                        .contains(&PerformanceReasonV1::EventsSixtySeconds)
                    && frame.rendered_header_line.contains("events_60s")
            })
        })
        .flatten()
        .map(|frame| frame.draw_ordinal);
    PerformanceEvidenceStreamV1 {
        workload_start_ns: result.workload_origin_ns,
        workload_close_ns,
        first_sample_ordinal: result.carry_in_sample_ordinal,
        next_sample_ordinal: result.next_sample_ordinal,
        first_draw_ordinal: frames.first().unwrap().draw_ordinal,
        next_draw_ordinal: result.next_draw_ordinal,
        samples,
        frames,
        terminal_observations,
        selected_terminal_draw_ordinal,
    }
}

#[cfg(feature = "workload-harness")]
fn reference_profile_performance_stream(
    stage: MeasurementStageV1,
    scenario: ScenarioV1,
    result: &RealQueueResult,
) -> Option<PerformanceEvidenceStreamV1> {
    (stage == MeasurementStageV1::Final
        && matches!(
            scenario,
            ScenarioV1::Sustained | ScenarioV1::Burst | ScenarioV1::TwiceTarget
        ))
    .then(|| performance_stream(result))
}

#[cfg(feature = "workload-harness")]
fn compose_final_performance_outcome(mut candidate: ReferenceOutcomeV1) -> ReferenceOutcomeV1 {
    let scenario = candidate.document().scenario;
    let baseline_parent = tempfile::tempdir().unwrap();
    let baseline_root = baseline_parent.path().canonicalize().unwrap();
    let scenario_directory = &workload_schema()
        .scenarios
        .iter()
        .find(|spec| spec.scenario == scenario)
        .unwrap()
        .directory;
    let baseline_scenario_root = baseline_root.join(scenario_directory);
    let mut baseline = synthetic_result(scenario, MeasurementStageV1::Baseline);
    write_synthetic_raw_scenario_root(&baseline_scenario_root, &mut baseline).unwrap();
    atomic_write_reference_outcome(&baseline_scenario_root.join("result-v1.json"), &baseline)
        .unwrap();

    candidate.document_mut().controlled_environment.insert(
        "HERDR_PERF_BASELINE_RESULTS_ROOT".to_owned(),
        baseline_root.to_string_lossy().into_owned(),
    );
    let production_subject_sha = candidate.document().production_subject_sha.clone();
    let preflight_head = candidate.document().harness_sha.clone();
    let fixture = RawFixture::from_outcome(candidate);
    let request = ComposeRequestV1 {
        raw_root: fixture.root.path().to_path_buf(),
        output: fixture.output_path("candidate-v1.json"),
        measurement_stage: MeasurementStageV1::Final,
        scenario,
        production_subject_sha,
        preflight_head,
        baseline_results_root: Some(baseline_root),
    };
    let outcome = compose_reference_outcome_from_raw_impl(&request).unwrap();
    assert!(validate_with_raw_root(&outcome, fixture.root.path()).is_ok());
    outcome
}

#[cfg(feature = "workload-harness")]
async fn run_final_performance_trial(
    profile: WorkloadProfile,
    injection: PerformanceTrialInjection,
) -> FinalPerformanceTrialResult {
    let mut outcome = match profile {
        WorkloadProfile::SustainedTarget => valid_final_sustained_result(),
        WorkloadProfile::TargetBurst => valid_final_burst_result(),
        WorkloadProfile::TwiceTarget => valid_twice_target_result(),
        _ => panic!("final performance trial requires a stream-bearing profile"),
    };
    let template = &outcome.document().trials[0].raw;
    let trial_origin_ns = template.trial_origin_ns;
    let workload_origin_ns = template.workload_origin_ns.unwrap();
    let phase = template.frame_phase_offset_ns.unwrap();
    let result = run_schedule_through_real_queue_at(
        profile,
        trial_origin_ns,
        trial_origin_ns + 1,
        Some(workload_origin_ns),
        phase,
        None,
        injection,
        None,
    )
    .await;
    let stream = performance_stream(&result);
    outcome.document_mut().trials[0].raw.admission_observations =
        result.admission_observations.clone();
    let document = outcome.document_mut();
    document.trials[0].raw.performance_evidence_stream = Some(stream);
    let failure = match injection {
        PerformanceTrialInjection::None | PerformanceTrialInjection::SupportedLoadDegradation => {
            None
        }
        PerformanceTrialInjection::SupportedLoadBeyondTolerance => {
            Some(FailureReasonV1::SupportedLoadDegradation)
        }
        PerformanceTrialInjection::OmitRequiredRenderedReason => {
            Some(FailureReasonV1::MissingDegradation)
        }
    };
    if let Some(failure) = failure {
        document.failure_reasons = vec![failure];
        outcome = match outcome {
            ReferenceOutcomeV1::Pass { document } => ReferenceOutcomeV1::Failed { document },
            _ => panic!("synthetic final performance outcome must start as pass"),
        };
    }
    assert_eq!(outcome.validate(), Ok(()));
    outcome = compose_final_performance_outcome(outcome);
    FinalPerformanceTrialResult {
        outcome,
        workload_origin_ns: result.workload_origin_ns,
        admission_observations: result.admission_observations,
        carry_in_sample_ordinal: result.carry_in_sample_ordinal,
    }
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn reference_profile_stream_selection_writes_valid_final_and_non_final_artifacts() {
    let mut final_candidate = valid_final_sustained_result();
    let template = &final_candidate.document().trials[0].raw;
    let trial_origin_ns = template.trial_origin_ns;
    let workload_origin_ns = template.workload_origin_ns.unwrap();
    let phase = template.frame_phase_offset_ns.unwrap();
    let result = run_schedule_through_real_queue_at(
        WorkloadProfile::SustainedTarget,
        trial_origin_ns,
        trial_origin_ns + 1,
        Some(workload_origin_ns),
        phase,
        None,
        PerformanceTrialInjection::None,
        None,
    )
    .await;

    final_candidate.document_mut().trials[0]
        .raw
        .admission_observations = result.admission_observations.clone();
    final_candidate.document_mut().trials[0]
        .raw
        .performance_evidence_stream = reference_profile_performance_stream(
        MeasurementStageV1::Final,
        ScenarioV1::Sustained,
        &result,
    );
    let final_outcome = compose_final_performance_outcome(final_candidate);
    let final_root = tempfile::tempdir().unwrap();
    let final_path = final_root.path().join("result-v1.json");
    atomic_write_reference_outcome(&final_path, &final_outcome).unwrap();
    let written_final = read_and_validate_reference_outcome(&final_path, AmendedLegacyMode::Off)
        .unwrap()
        .outcome;
    assert!(
        written_final.document().trials[0]
            .raw
            .performance_evidence_stream
            .is_some()
    );

    let mut baseline = valid_synthetic_result();
    baseline.document_mut().trials[0]
        .raw
        .performance_evidence_stream = reference_profile_performance_stream(
        MeasurementStageV1::Baseline,
        ScenarioV1::Sustained,
        &result,
    );
    let baseline_root = tempfile::tempdir().unwrap();
    let baseline_path = baseline_root.path().join("result-v1.json");
    atomic_write_reference_outcome(&baseline_path, &baseline).unwrap();
    let written_baseline =
        read_and_validate_reference_outcome(&baseline_path, AmendedLegacyMode::Off)
            .unwrap()
            .outcome;
    assert!(
        written_baseline.document().trials[0]
            .raw
            .performance_evidence_stream
            .is_none()
    );
}

#[cfg(feature = "workload-harness")]
#[test]
fn close_snapshot_excludes_strictly_later_barrier_sample() {
    let samples_at_close = [
        WorkloadPerformanceSample {
            source_quality: ObservationQuality::Live,
            publication: stamped_publication(40, 90, ObservationQuality::Live, []),
        },
        WorkloadPerformanceSample {
            source_quality: ObservationQuality::Live,
            publication: stamped_publication(41, 100, ObservationQuality::Live, []),
        },
    ];

    let (included, next_sample_ordinal) =
        freeze_performance_samples_at_close(&samples_at_close, 40, 100);

    assert_eq!(
        included
            .iter()
            .map(|sample| {
                sample
                    .publication
                    .workload_sample_stamp
                    .unwrap()
                    .sample_ordinal
            })
            .collect::<Vec<_>>(),
        [40, 41]
    );
    assert_eq!(next_sample_ordinal, 42);
    assert!(included.iter().all(|sample| {
        sample
            .publication
            .workload_sample_stamp
            .unwrap()
            .sampled_at_ns
            <= 100
    }));

    let trailing_barrier = WorkloadPerformanceSample {
        source_quality: ObservationQuality::Live,
        publication: stamped_publication(42, 101, ObservationQuality::Live, []),
    };
    assert!(
        trailing_barrier
            .publication
            .workload_sample_stamp
            .unwrap()
            .sampled_at_ns
            > 100
    );
}

#[cfg(feature = "workload-harness")]
#[test]
fn close_snapshot_refuses_to_roll_back_past_an_already_recorded_suffix() {
    let samples = [
        WorkloadPerformanceSample {
            source_quality: ObservationQuality::Live,
            publication: stamped_publication(40, 90, ObservationQuality::Live, []),
        },
        WorkloadPerformanceSample {
            source_quality: ObservationQuality::Live,
            publication: stamped_publication(41, 100, ObservationQuality::Live, []),
        },
        WorkloadPerformanceSample {
            source_quality: ObservationQuality::Live,
            publication: stamped_publication(42, 101, ObservationQuality::Live, []),
        },
    ];

    assert!(
        std::panic::catch_unwind(|| freeze_performance_samples_at_close(&samples, 40, 100))
            .is_err()
    );
}

#[cfg(feature = "workload-harness")]
#[test]
fn carry_in_anchor_selects_the_last_pre_origin_sample() {
    let samples = [
        WorkloadPerformanceSample {
            source_quality: ObservationQuality::Live,
            publication: stamped_publication(40, 90, ObservationQuality::Live, []),
        },
        WorkloadPerformanceSample {
            source_quality: ObservationQuality::Live,
            publication: stamped_publication(41, 99, ObservationQuality::Live, []),
        },
        WorkloadPerformanceSample {
            source_quality: ObservationQuality::Live,
            publication: stamped_publication(42, 100, ObservationQuality::Live, []),
        },
    ];

    assert_eq!(last_pre_origin_sample_ordinal(&samples, 100), 41);
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn supported_load_records_complete_live_performance_stream() {
    for profile in [
        WorkloadProfile::SustainedTarget,
        WorkloadProfile::TargetBurst,
    ] {
        let result = run_final_performance_trial(profile, PerformanceTrialInjection::None).await;
        let stream = result.outcome.document().trials[0]
            .raw
            .performance_evidence_stream
            .as_ref()
            .unwrap();
        assert_eq!(
            stream.samples[0].sample_ordinal,
            stream.first_sample_ordinal
        );
        assert_eq!(
            stream.samples.last().unwrap().sample_ordinal + 1,
            stream.next_sample_ordinal
        );
        assert_eq!(stream.frames[0].draw_ordinal, stream.first_draw_ordinal);
        assert_eq!(
            stream.frames.last().unwrap().draw_ordinal + 1,
            stream.next_draw_ordinal
        );
        assert!(stream.samples.iter().all(|sample| {
            sample.source_quality == EffectiveQualityV1::Live
                && sample.effective_quality == EffectiveQualityV1::Live
                && sample.reasons.is_empty()
        }));
        assert!(stream.frames.iter().all(|frame| frame.reasons.is_empty()));
        assert!(result.outcome.failure_reasons().is_empty());
        assert!(result.outcome.validate().is_ok());
    }
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn supported_load_one_quantum_degradation_is_tolerated_at_final_acceptance() {
    let result = run_final_performance_trial(
        WorkloadProfile::TargetBurst,
        PerformanceTrialInjection::SupportedLoadDegradation,
    )
    .await;
    assert_eq!(
        workload::admission_schedule_attained(
            WorkloadProfile::TargetBurst,
            result.workload_origin_ns,
            &result.admission_observations,
        ),
        Ok(true)
    );
    let stream = result.outcome.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap();
    let degraded = stream
        .samples
        .iter()
        .filter(|sample| !sample.reasons.is_empty())
        .collect::<Vec<_>>();
    assert!(!degraded.is_empty());
    assert!(
        stream
            .samples
            .iter()
            .all(|sample| { !sample.reasons.contains(&PerformanceReasonV1::EventLag) })
    );
    assert!(degraded.iter().all(|sample| tolerated_boundary_degradation(
        MeasurementStageV1::Final,
        ScenarioV1::Burst,
        sample,
        false,
    )));
    assert!(result.outcome.failure_reasons().is_empty());
    assert_eq!(result.outcome.status(), ReferenceOutcomeStatusV1::Pass);
    assert!(result.outcome.validate().is_ok());

    let mut suite = [
        ScenarioV1::Target,
        ScenarioV1::Sustained,
        ScenarioV1::Burst,
        ScenarioV1::Startup,
        ScenarioV1::Idle,
        ScenarioV1::FallbackRescan,
        ScenarioV1::TwiceTarget,
    ]
    .map(|scenario| synthetic_result(scenario, MeasurementStageV1::Final));
    suite[2] = result.outcome;
    assert_eq!(
        classify_d4_checkpoint(&suite).unwrap(),
        D4CheckpointDecisionV1::NoMissD4NotAuthorized {}
    );
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn supported_load_beyond_tolerance_fails_final_through_real_queue() {
    // Break caught: retaining only synthetic coverage for a real collector
    // breach beyond Final's one-quantum Burst tolerance.
    let result = run_final_performance_trial(
        WorkloadProfile::TargetBurst,
        PerformanceTrialInjection::SupportedLoadBeyondTolerance,
    )
    .await;
    assert_eq!(
        workload::admission_schedule_attained(
            WorkloadProfile::TargetBurst,
            result.workload_origin_ns,
            &result.admission_observations,
        ),
        Ok(true)
    );
    let stream = result.outcome.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap();
    assert!(stream.samples.iter().any(|sample| {
        sample.events_one_second == 102
            && sample
                .reasons
                .contains(&PerformanceReasonV1::EventsOneSecond)
    }));
    assert_eq!(
        result.outcome.failure_reasons(),
        [FailureReasonV1::SupportedLoadDegradation]
    );
    assert_eq!(result.outcome.status(), ReferenceOutcomeStatusV1::Failed);
    assert!(result.outcome.validate().is_ok());

    let mut suite = [
        ScenarioV1::Target,
        ScenarioV1::Sustained,
        ScenarioV1::Burst,
        ScenarioV1::Startup,
        ScenarioV1::Idle,
        ScenarioV1::FallbackRescan,
        ScenarioV1::TwiceTarget,
    ]
    .map(|scenario| synthetic_result(scenario, MeasurementStageV1::Final));
    suite[2] = result.outcome;
    assert_eq!(
        classify_d4_checkpoint(&suite).unwrap(),
        D4CheckpointDecisionV1::AmendmentsRequired {
            amendments: vec![RequiredAmendmentV1::NonD4],
        }
    );
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn twice_target_renders_real_collector_performance_reason() {
    let result = run_final_performance_trial(
        WorkloadProfile::TwiceTarget,
        PerformanceTrialInjection::None,
    )
    .await;
    let stream = result.outcome.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap();
    let selected = stream
        .frames
        .iter()
        .find(|frame| Some(frame.draw_ordinal) == stream.selected_terminal_draw_ordinal)
        .unwrap();
    assert!(
        selected
            .reasons
            .contains(&PerformanceReasonV1::EventsSixtySeconds)
    );
    assert!(selected.rendered_header_line.contains("perf:events_60s"));
    assert!(selected.rendered_at_ns <= stream.workload_start_ns + 60_000_000_000);
    assert!(result.outcome.validate().is_ok());
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn twice_target_deadline_without_required_rendered_reason_is_valid_measured_failure() {
    let result = run_final_performance_trial(
        WorkloadProfile::TwiceTarget,
        PerformanceTrialInjection::OmitRequiredRenderedReason,
    )
    .await;
    let stream = result.outcome.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap();
    assert!(stream.samples.iter().any(|sample| {
        sample
            .reasons
            .contains(&PerformanceReasonV1::EventsSixtySeconds)
    }));
    assert!(
        stream
            .frames
            .iter()
            .all(|frame| { !frame.rendered_header_line.contains("events_60s") })
    );
    assert_eq!(stream.selected_terminal_draw_ordinal, None);
    assert!(stream.workload_close_ns > stream.workload_start_ns + 60_000_000_000);
    assert_eq!(
        result.outcome.failure_reasons(),
        [FailureReasonV1::MissingDegradation]
    );
    assert_eq!(result.outcome.status(), ReferenceOutcomeStatusV1::Failed);
    assert!(result.outcome.validate().is_ok());
}

#[cfg(feature = "workload-harness")]
#[test]
fn workload_frame_uses_exact_app_cached_performance_publication() {
    let (_model_sender, model_receiver) =
        tokio::sync::watch::channel(Arc::new(DomainModel::default()));
    let older = stamped_publication(1, 10, ObservationQuality::Live, []);
    let newer = stamped_publication(
        2,
        20,
        ObservationQuality::Degraded,
        [PerformanceDegradationReason::EventsSixtySeconds],
    );
    let (performance_sender, performance_receiver) = tokio::sync::watch::channel(older.clone());
    let app = App::new(
        model_receiver,
        HeaderInputs {
            performance: performance_receiver,
            ..HeaderInputs::default()
        },
    );
    let clock = Arc::new(AtomicU64::new(100_000_000));
    let terminal = Terminal::new(TestBackend::new(160, 48)).unwrap();
    let driver_clock = Arc::clone(&clock);
    let mut driver = WorkloadFrameDriver::new(app, terminal, move || {
        Duration::from_nanos(driver_clock.load(Ordering::SeqCst))
    });
    assert!(!driver.refresh_app_if_changed().unwrap());

    performance_sender.send(newer.clone()).unwrap();
    let cached = driver.step_without_refresh(true).unwrap();
    assert_eq!(cached.performance_publication, Some(older));
    let cached_header = cached.rendered_header_line.unwrap();
    assert!(cached_header.contains("LIVE"));
    assert!(!cached_header.contains("DEGRADED"));
    assert!(!cached_header.contains("events_60s"));

    clock.store(200_000_000, Ordering::SeqCst);
    let refreshed = driver.step(true).unwrap();
    assert_eq!(refreshed.performance_publication, Some(newer));
    assert!(
        refreshed
            .rendered_header_line
            .unwrap()
            .contains("events_60s")
    );
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn workload_performance_stream_retains_pre_origin_carry_in_and_contiguous_suffix() {
    let result = run_final_performance_trial(
        WorkloadProfile::SustainedTarget,
        PerformanceTrialInjection::None,
    )
    .await;
    let stream = result.outcome.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap();
    assert_eq!(stream.first_sample_ordinal, result.carry_in_sample_ordinal);
    assert!(stream.samples[0].sampled_at_ns < stream.workload_start_ns);
    assert_eq!(
        stream
            .samples
            .iter()
            .filter(|sample| sample.sampled_at_ns < stream.workload_start_ns)
            .count(),
        1
    );
    assert_eq!(
        stream
            .samples
            .iter()
            .map(|sample| sample.sample_ordinal)
            .collect::<Vec<_>>(),
        (stream.first_sample_ordinal..stream.next_sample_ordinal).collect::<Vec<_>>()
    );
    assert_eq!(
        stream
            .frames
            .iter()
            .map(|frame| frame.draw_ordinal)
            .collect::<Vec<_>>(),
        (stream.first_draw_ordinal..stream.next_draw_ordinal).collect::<Vec<_>>()
    );
    let last_referenced = stream
        .frames
        .iter()
        .map(|frame| frame.sample_ordinal)
        .max()
        .unwrap();
    assert!(stream.samples.last().unwrap().sample_ordinal >= last_referenced);
    assert!(
        stream
            .samples
            .iter()
            .all(|sample| sample.sampled_at_ns <= stream.workload_close_ns)
    );
    assert!(result.outcome.validate().is_ok());
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn sustained_target_real_queues_remain_lossless_and_nondegraded() {
    let result = run_virtual_schedule_through_real_queue(WorkloadProfile::SustainedTarget).await;
    assert_real_queue_outcome_is_exact(&result, WorkloadProfile::SustainedTarget);
    assert!(
        result.performance_samples.iter().all(|sample| sample
            .publication
            .snapshot
            .reasons
            .is_empty())
    );
    assert!(result.final_performance.snapshot.reasons.is_empty());
    assert_eq!(
        result.final_performance.effective_quality,
        ObservationQuality::Live
    );
    assert_eq!(result.final_compatibility_quality, ObservationQuality::Live);
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn target_burst_real_queues_remain_lossless_and_nondegraded() {
    let result = run_virtual_schedule_through_real_queue(WorkloadProfile::TargetBurst).await;
    assert_real_queue_outcome_is_exact(&result, WorkloadProfile::TargetBurst);
    assert!(
        result.performance_samples.iter().all(|sample| sample
            .publication
            .snapshot
            .reasons
            .is_empty())
    );
    assert!(result.final_performance.snapshot.reasons.is_empty());
    assert_eq!(
        result.final_performance.effective_quality,
        ObservationQuality::Live
    );
    assert_eq!(result.final_compatibility_quality, ObservationQuality::Live);
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn twice_target_real_queues_publish_sixty_second_degradation_without_loss() {
    let result = run_virtual_schedule_through_real_queue(WorkloadProfile::TwiceTarget).await;
    assert_real_queue_outcome_is_exact(&result, WorkloadProfile::TwiceTarget);
    assert!(result.performance_samples.iter().any(|sample| {
        sample
            .publication
            .snapshot
            .reasons
            .contains(&PerformanceDegradationReason::EventsSixtySeconds)
            && sample
                .publication
                .workload_sample_stamp
                .is_some_and(|stamp| {
                    stamp.sampled_at_ns <= result.workload_origin_ns + 60_000_000_000
                })
    }));
    assert!(
        result
            .final_performance
            .snapshot
            .reasons
            .contains(&PerformanceDegradationReason::EventsSixtySeconds)
    );
    assert_eq!(
        result.final_performance.effective_quality,
        ObservationQuality::Degraded
    );
    assert_eq!(
        result.final_compatibility_quality,
        ObservationQuality::Degraded
    );
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn stalled_production_frame_recovers_all_cumulative_probe_acknowledgements() {
    let result = run_real_queue_with_frame_stall(
        WorkloadProfile::SustainedTarget,
        Duration::from_millis(450),
    )
    .await;
    assert!(result.frames.iter().any(|frame| frame.new_probe_count >= 2));
    assert_eq!(
        result.rendered_sequences,
        workload::screen_probe_sequences(WorkloadProfile::SustainedTarget)
    );
    assert_eq!(result.submitted_sequences, (1..=1_200).collect::<Vec<_>>());
    assert_eq!(result.submitted_sequences, result.persisted_sequences);
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn delayed_ready_and_setup_do_not_shift_the_workload_schedule() {
    let result = run_virtual_schedule_after_delayed_ready_and_setup().await;
    assert!(result.trial_origin_ns < result.observer_ready_ns);
    assert!(result.observer_ready_ns < result.workload_origin_ns);
    assert!(admission_schedule_attained(
        result.profile,
        result.workload_origin_ns,
        &result.admission_observations
    ));
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn controller_admission_migration_retains_one_absolute_monotonic_domain() {
    let result = run_virtual_schedule_through_real_queue_at(
        WorkloadProfile::SustainedTarget,
        9_000_000_000_000,
        9_000_100_000_000,
        9_001_000_000_000,
        None,
    )
    .await;
    assert!(result.trial_origin_ns > 1_000_000_000_000);
    assert!(result.trial_origin_ns < result.observer_ready_ns);
    assert!(result.observer_ready_ns < result.priming_frame_recorded_ns);
    assert!(result.priming_frame_recorded_ns < result.workload_origin_ns);
    assert_eq!(
        workload::admission_schedule_attained(
            WorkloadProfile::SustainedTarget,
            result.workload_origin_ns,
            &result.admission_observations,
        ),
        Ok(true)
    );
    assert!(result.screen_observations.iter().all(|sample| {
        sample.admitted_ns >= result.workload_origin_ns
            && sample.terminal_ns >= sample.admitted_ns
            && sample.published_ns >= sample.terminal_ns
            && sample.rendered_ns >= sample.published_ns
    }));

    let mut process_relative = result.admission_observations.clone();
    for sample in &mut process_relative {
        sample.admitted_ns = sample
            .admitted_ns
            .checked_sub(result.workload_origin_ns)
            .unwrap();
    }
    assert_eq!(
        workload::admission_schedule_attained(
            WorkloadProfile::SustainedTarget,
            result.workload_origin_ns,
            &process_relative,
        ),
        Err(ResultError::InvalidArtifact)
    );
}

#[cfg(feature = "workload-harness")]
#[derive(Clone, Debug)]
struct PhaseTrial {
    priming_frame_count: u32,
    priming_frame_recorded_ns: Option<u64>,
    workload_origin_ns: Option<u64>,
    frame_phase_offset_ns: Option<u64>,
    admission_observations: Vec<AdmissionObservationV1>,
    screen_observations: Vec<LatencyObservationV1>,
}

#[cfg(feature = "workload-harness")]
fn valid_five_phase_trials() -> Vec<PhaseTrial> {
    [10_000_000, 30_000_000, 50_000_000, 70_000_000, 90_000_000]
        .into_iter()
        .map(|phase| {
            let priming_ns = 1_000_000_000;
            let workload_origin_ns = priming_ns + 100_000_000 - phase;
            let rendered_ns = workload_origin_ns + phase;
            let clock = Arc::new(AtomicU64::new(priming_ns));
            let (model_sender, model_receiver) =
                tokio::sync::watch::channel(Arc::new(workload::target_model()));
            let app = App::new(model_receiver, HeaderInputs::default());
            let terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
            let driver_clock = Arc::clone(&clock);
            let mut driver = WorkloadFrameDriver::new(app, terminal, move || {
                Duration::from_nanos(driver_clock.load(Ordering::SeqCst))
            });
            assert_eq!(driver.step(true).unwrap().draw_ordinal, Some(0));
            model_sender.send_replace(Arc::new(workload::target_model()));
            clock.store(rendered_ns, Ordering::SeqCst);
            assert_eq!(driver.step(true).unwrap().draw_ordinal, Some(1));
            PhaseTrial {
                priming_frame_count: 1,
                priming_frame_recorded_ns: Some(priming_ns),
                workload_origin_ns: Some(workload_origin_ns),
                frame_phase_offset_ns: Some(phase),
                admission_observations: vec![AdmissionObservationV1 {
                    sequence: 1,
                    scheduled_ns: workload_origin_ns,
                    admitted_ns: workload_origin_ns,
                }],
                screen_observations: vec![LatencyObservationV1 {
                    sequence: 1,
                    admitted_ns: workload_origin_ns,
                    terminal_ns: workload_origin_ns,
                    published_ns: workload_origin_ns,
                    rendered_ns,
                    observed_frame_phase_ns: phase,
                }],
            }
        })
        .collect()
}

#[cfg(feature = "workload-harness")]
#[test]
fn virtual_zero_overshoot_phase_rotation_is_complete_and_primed() {
    let trials = valid_five_phase_trials();
    assert_eq!(
        trials
            .iter()
            .map(|trial| trial.frame_phase_offset_ns.unwrap())
            .collect::<Vec<_>>(),
        vec![10_000_000, 30_000_000, 50_000_000, 70_000_000, 90_000_000]
    );
    assert!(trials.iter().all(|trial| trial.priming_frame_count == 1));
    assert!(trials.iter().all(|trial| {
        trial.priming_frame_recorded_ns.unwrap().checked_add(
            100_000_000_u64
                .checked_sub(trial.frame_phase_offset_ns.unwrap())
                .unwrap(),
        ) == trial.workload_origin_ns
    }));
    assert!(
        trials
            .iter()
            .all(|trial| trial.screen_observations.iter().all(|sample| {
                let Some(admission) = trial
                    .admission_observations
                    .iter()
                    .find(|admission| admission.sequence == sample.sequence)
                else {
                    return false;
                };
                sample
                    .rendered_ns
                    .checked_sub(admission.scheduled_ns)
                    .map(|elapsed| elapsed % 100_000_000)
                    == trial.frame_phase_offset_ns
            }))
    );
}

#[cfg(feature = "workload-harness")]
struct DriverObservationResult {
    frame_phase_offset_ns: u64,
    screen_observation: LatencyObservationV1,
    input_observation: InputLatencyObservationV1,
    screen_latency_ns: u64,
    input_latency_ns: u64,
}

#[cfg(feature = "workload-harness")]
impl DriverObservationResult {
    fn validates(&self) -> bool {
        self.screen_latency_ns
            == self
                .screen_observation
                .rendered_ns
                .checked_sub(self.screen_observation.admitted_ns)
                .unwrap()
            && self.input_latency_ns
                == self
                    .input_observation
                    .rendered_ns
                    .checked_sub(self.input_observation.injected_ns)
                    .unwrap()
    }
}

#[cfg(feature = "workload-harness")]
async fn run_screen_and_input_through_driver_at(
    priming_ns: u64,
    desired_phase_ns: u64,
    scheduled_ns: u64,
    rendered_ns: u64,
) -> DriverObservationResult {
    let screen_clock = Arc::new(AtomicU64::new(priming_ns));
    let (screen_model_sender, screen_model_receiver) =
        tokio::sync::watch::channel(Arc::new(workload::target_model()));
    let screen_app = App::new(screen_model_receiver, HeaderInputs::default());
    let screen_terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    let screen_driver_clock = Arc::clone(&screen_clock);
    let mut screen_driver = WorkloadFrameDriver::new(screen_app, screen_terminal, move || {
        Duration::from_nanos(screen_driver_clock.load(Ordering::SeqCst))
    });
    assert_eq!(screen_driver.step(true).unwrap().draw_ordinal, Some(0));
    screen_model_sender.send_replace(Arc::new(workload::target_model()));
    screen_clock.store(rendered_ns, Ordering::SeqCst);
    assert_eq!(screen_driver.step(true).unwrap().draw_ordinal, Some(1));

    let input_clock = Arc::new(AtomicU64::new(priming_ns));
    let (_input_model_sender, input_model_receiver) =
        tokio::sync::watch::channel(Arc::new(workload::target_model()));
    let input_app = App::new(input_model_receiver, HeaderInputs::default());
    let input_terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    let input_driver_clock = Arc::clone(&input_clock);
    let mut input_driver = WorkloadFrameDriver::new(input_app, input_terminal, move || {
        Duration::from_nanos(input_driver_clock.load(Ordering::SeqCst))
    });
    assert_eq!(input_driver.step(true).unwrap().draw_ordinal, Some(0));
    input_clock.store(rendered_ns, Ordering::SeqCst);
    assert!(
        input_driver
            .handle_key_and_wait(crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char('?'),
                crossterm::event::KeyModifiers::NONE,
            ))
            .unwrap()
            .draw_ordinal
            .is_some()
    );
    let observed_phase_ns = rendered_ns.checked_sub(scheduled_ns).unwrap() % 100_000_000;
    DriverObservationResult {
        frame_phase_offset_ns: desired_phase_ns,
        screen_observation: LatencyObservationV1 {
            sequence: 1,
            admitted_ns: scheduled_ns,
            terminal_ns: scheduled_ns,
            published_ns: scheduled_ns,
            rendered_ns,
            observed_frame_phase_ns: observed_phase_ns,
        },
        input_observation: InputLatencyObservationV1 {
            scheduled_ns,
            injected_ns: scheduled_ns,
            rendered_ns,
            observed_frame_phase_ns: observed_phase_ns,
        },
        screen_latency_ns: rendered_ns.checked_sub(scheduled_ns).unwrap(),
        input_latency_ns: rendered_ns.checked_sub(scheduled_ns).unwrap(),
    }
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn reference_observations_record_nonzero_limiter_overshoot_without_synthesis() {
    let priming_ns = 1_000_000_000;
    let desired_phase_ns = 30_000_000;
    let scheduled_ns = priming_ns + 70_000_000;
    let rendered_ns = priming_ns + 107_000_000;
    let result = run_screen_and_input_through_driver_at(
        priming_ns,
        desired_phase_ns,
        scheduled_ns,
        rendered_ns,
    )
    .await;
    assert_eq!(result.frame_phase_offset_ns, desired_phase_ns);
    assert_eq!(result.screen_observation.admitted_ns, scheduled_ns);
    assert_eq!(result.input_observation.injected_ns, scheduled_ns);
    assert_eq!(result.screen_observation.rendered_ns, rendered_ns);
    assert_eq!(result.input_observation.rendered_ns, rendered_ns);
    assert_eq!(
        result.screen_observation.observed_frame_phase_ns,
        37_000_000
    );
    assert_eq!(result.input_observation.observed_frame_phase_ns, 37_000_000);
    assert_eq!(result.screen_latency_ns, 37_000_000);
    assert_eq!(result.input_latency_ns, 37_000_000);
    assert!(result.validates());
    assert_ne!(
        result.screen_observation.rendered_ns,
        priming_ns + 100_000_000
    );
}

#[cfg(feature = "workload-harness")]
#[derive(Clone, Debug)]
struct FallbackArmResult {
    sequence: u64,
    elapsed: Duration,
    final_identities: workload::StructuralIdentities,
    scoped_observations: Vec<ScopedTimingObservationV1>,
}

#[cfg(feature = "workload-harness")]
struct FallbackPairResult {
    notification: FallbackArmResult,
    rescan: FallbackArmResult,
}

#[cfg(feature = "workload-harness")]
#[derive(Default)]
struct HarnessNotifyWatcher;

#[cfg(feature = "workload-harness")]
impl NotifyWatcher for HarnessNotifyWatcher {
    fn watch(&mut self, _path: &std::path::Path) -> notify::Result<()> {
        Ok(())
    }

    fn unwatch(&mut self, _path: &std::path::Path) -> notify::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "workload-harness")]
struct CapturingNotifyFactory {
    sink: Arc<Mutex<Option<NotifySink>>>,
}

#[cfg(feature = "workload-harness")]
impl NotifyFactory for CapturingNotifyFactory {
    fn create(self: Box<Self>, sink: NotifySink) -> notify::Result<Box<dyn NotifyWatcher>> {
        *lock_workload(&self.sink) = Some(sink);
        Ok(Box::new(HarnessNotifyWatcher))
    }
}

#[cfg(feature = "workload-harness")]
struct FailingNotifyFactory;

#[cfg(feature = "workload-harness")]
impl NotifyFactory for FailingNotifyFactory {
    fn create(self: Box<Self>, _sink: NotifySink) -> notify::Result<Box<dyn NotifyWatcher>> {
        Err(notify::Error::generic(
            "forced workload notify creation failure",
        ))
    }
}

#[cfg(feature = "workload-harness")]
fn fallback_model(session_file: &std::path::Path) -> DomainModel {
    let mut model = workload::target_model();
    let run_id = model
        .task_run_by_key(&RunKey::Controller("run-0200".to_owned()))
        .unwrap()
        .run_id;
    model.insert_agent_node(AgentNode {
        agent_node_id: "agent:codex:fallback-owner".to_owned(),
        provider: Provider::Codex,
        native_session_id: Some("fallback-owner".to_owned()),
        task_run_id: run_id,
        display_ordinal: DisplayOrdinal::new(201),
        parent_agent_node_id: None,
        state: None,
        model_id: Some("gpt-test".to_owned()),
        last_event_kind: None,
        last_tool_name: None,
        last_item_count: None,
        last_byte_count: None,
        last_activity_at_ms: None,
        session_file: Some(session_file.to_string_lossy().into_owned()),
    });
    model
}

#[cfg(feature = "workload-harness")]
async fn run_fallback_arm(poll: Duration, notification: bool, sequence: u64) -> FallbackArmResult {
    use std::io::Write as _;

    let temporary = tempfile::tempdir().unwrap();
    let sessions = temporary.path().join("home/.codex/sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    let session_file = sessions.join("fallback-owner.jsonl");
    std::fs::write(
        &session_file,
        b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"fallback-owner\",\"session_id\":\"fallback-owner\",\"model\":\"gpt-test\"}}\n",
    )
    .unwrap();
    let root = StateRoot(temporary.path().join("state"));
    std::fs::create_dir_all(&root.0).unwrap();
    let store = open_writer(&root).unwrap();
    let (lifecycle, writer) = spawn_writer(store).unwrap();
    let sink = Arc::new(Mutex::new(None));
    let timings = Arc::new(Mutex::new(Vec::new()));
    let clock = Arc::new(AtomicU64::new(1));
    let no_admissions = Arc::new(Mutex::new(Vec::new()));
    let no_terminals = Arc::new(Mutex::new(Vec::new()));
    let no_persistence = Arc::new(Mutex::new(Vec::new()));
    let notify_factory: Option<Box<dyn NotifyFactory>> = if notification {
        Some(Box::new(CapturingNotifyFactory {
            sink: Arc::clone(&sink),
        }))
    } else {
        Some(Box::new(FailingNotifyFactory))
    };
    let kind = if notification {
        WorkloadTimingKind::FallbackNotification
    } else {
        WorkloadTimingKind::FallbackRescan
    };
    let config = WorkloadCollectorConfig {
        controller_hooks: WorkloadControllerHooks {
            clock: {
                let clock = Arc::clone(&clock);
                Arc::new(move || clock.load(Ordering::SeqCst))
            },
            admission_observer: Arc::new(move |sample| lock_workload(&no_admissions).push(sample)),
            terminal_observer: Arc::new(move |sample| lock_workload(&no_terminals).push(sample)),
            persistence_observer: Arc::new(move |sample| {
                lock_workload(&no_persistence).push(sample)
            }),
            timing_observer: workload_timing_collector(Arc::clone(&timings)),
        },
        performance_clock: Arc::new(AtomicPerformanceClock {
            nanoseconds: Arc::clone(&clock),
        }),
        performance_observer: Arc::new(|_| {}),
        provider_roots: vec![DiscoveryRoot {
            provider: Provider::Codex,
            path: sessions,
        }],
        notify_factory,
        rescan_interval: Some(if notification {
            herdr_top::provider::RESCAN_INTERVAL
        } else {
            poll
        }),
        fallback_timing: Some((
            kind,
            sequence,
            workload_timing_collector(Arc::clone(&timings)),
        )),
    };
    let mut handle = spawn_workload_collector(
        temporary.path().join("missing-herdr.sock"),
        "increment5-fallback".to_owned(),
        empty_restored(fallback_model(&session_file)),
        writer,
        config,
    )
    .await
    .unwrap();
    let frame_epoch = std::time::Instant::now();
    let app = App::new(
        handle.collector.model.clone(),
        HeaderInputs {
            performance: handle.collector.performance.clone(),
            ..HeaderInputs::default()
        },
    );
    let terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    let mut driver = WorkloadFrameDriver::new(app, terminal, move || frame_epoch.elapsed());
    assert_eq!(driver.step(true).unwrap().draw_ordinal, Some(0));
    tokio::time::sleep(Duration::from_millis(110)).await;
    let started = std::time::Instant::now();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&session_file)
        .unwrap();
    file.write_all(
        b"{\"type\":\"event_msg\",\"payload\":{\"type\":\"sub_agent_activity\",\"event_id\":\"fallback-activity\",\"occurred_at_ms\":2,\"agent_thread_id\":\"fallback-owner\",\"agent_path\":\"/root\",\"kind\":\"interacted\"}}\n",
    )
    .unwrap();
    file.flush().unwrap();
    if notification {
        let notify_sink = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(sink) = lock_workload(&sink).clone() {
                    break sink;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        notify_sink.hint(session_file.clone());
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if handle
                .collector
                .model
                .borrow()
                .agent_node("agent:codex:fallback-owner")
                .is_some_and(|node| node.last_event_kind.as_deref() == Some("interacted"))
            {
                break;
            }
            handle.collector.model.changed().await.unwrap();
        }
    })
    .await
    .expect("fallback provider event must reach the model watch");
    loop {
        let frame = driver.step(true).unwrap();
        if frame.draw_ordinal.is_some() {
            break;
        }
        tokio::time::sleep(frame.poll_duration).await;
    }
    let elapsed = started.elapsed();
    let final_identities = structural_identities(&handle.collector.model.borrow());
    handle.collector.stop().await.unwrap();
    lifecycle.shutdown().await.unwrap();
    FallbackArmResult {
        sequence,
        elapsed,
        final_identities,
        scoped_observations: lock_workload(&timings).clone(),
    }
}

#[cfg(feature = "workload-harness")]
async fn run_notification_and_forced_rescan_pair_at(
    poll: Duration,
    sequence: u64,
) -> FallbackPairResult {
    let (notification, rescan) = tokio::join!(
        run_fallback_arm(poll, true, sequence),
        run_fallback_arm(poll, false, sequence),
    );
    FallbackPairResult {
        notification,
        rescan,
    }
}

#[cfg(feature = "workload-harness")]
async fn run_notification_and_forced_rescan_pair(poll: Duration) -> FallbackPairResult {
    run_notification_and_forced_rescan_pair_at(poll, 1).await
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn fallback_rescan_uses_injected_polling_interval_without_loss() {
    let poll = Duration::from_millis(20);
    let rescan_added_delay_ceiling = Duration::from_secs(2);
    let paired = run_notification_and_forced_rescan_pair(poll).await;
    assert_eq!(paired.notification.sequence, paired.rescan.sequence);
    let rescan_upper_bound = paired
        .notification
        .elapsed
        .saturating_add(rescan_added_delay_ceiling);
    assert!(paired.rescan.elapsed <= rescan_upper_bound);
    let expected = workload::oracle(WorkloadProfile::FallbackRescan).final_identities;
    assert_eq!(paired.notification.final_identities, expected);
    assert_eq!(paired.rescan.final_identities, expected);
}

#[cfg(feature = "workload-harness")]
fn assert_exact_internal_segment_counts(
    observations: &[ScopedTimingObservationV1],
    expected: &[(ScopedTimingKindV1, u32, u32)],
) {
    for sample in observations {
        let (_, d4_count, clone_publish_count) = expected
            .iter()
            .find(|(kind, _, _)| *kind == sample.kind)
            .expect("every scoped kind must have a frozen segment count");
        assert_eq!(sample.d4_segment_count, *d4_count);
        assert_eq!(
            sample.model_clone_publish_segment_count,
            *clone_publish_count
        );
    }
}

#[cfg(feature = "workload-harness")]
fn assert_exact_kind_sequence_counts(
    observations: &[ScopedTimingObservationV1],
    expected: Vec<(ScopedTimingKindV1, Vec<u64>)>,
) {
    for (kind, sequences) in &expected {
        assert_eq!(
            observations
                .iter()
                .filter(|sample| sample.kind == *kind)
                .map(|sample| sample.sequence)
                .collect::<Vec<_>>(),
            *sequences,
            "unexpected timing sequence coverage for {kind:?}"
        );
    }
    assert_eq!(
        observations.len(),
        expected
            .iter()
            .map(|(_, sequences)| sequences.len())
            .sum::<usize>()
    );
}

#[cfg(feature = "workload-harness")]
fn expected_controller_startup_and_fallback_kind_sequences() -> Vec<(ScopedTimingKindV1, Vec<u64>)>
{
    vec![
        (ScopedTimingKindV1::ControllerEvent, (1..=1_200).collect()),
        (ScopedTimingKindV1::StartupRestore, vec![1]),
        (ScopedTimingKindV1::FallbackNotification, vec![1]),
        (ScopedTimingKindV1::FallbackRescan, vec![1]),
    ]
}

#[cfg(feature = "workload-harness")]
async fn run_controller_startup_and_both_fallback_arms_with_hooks() -> Vec<ScopedTimingObservationV1>
{
    let mut observations =
        run_virtual_schedule_through_real_queue(WorkloadProfile::SustainedTarget)
            .await
            .scoped_observations;
    let startup = Arc::new(Mutex::new(Vec::new()));
    let (_reducer, _model, _operator) = Reducer::new_with_operator_observed(
        empty_restored(workload::target_model()),
        RestoredOperatorState {
            activity: Vec::new(),
            terminal_times: std::collections::HashMap::new(),
        },
        1,
        workload_timing_collector(Arc::clone(&startup)),
    );
    observations.extend(lock_workload(&startup).clone());
    let fallback = run_notification_and_forced_rescan_pair(Duration::from_millis(20)).await;
    observations.extend(fallback.notification.scoped_observations);
    observations.extend(fallback.rescan.scoped_observations);
    let kind_ordinal = |kind| match kind {
        ScopedTimingKindV1::ControllerEvent => 0,
        ScopedTimingKindV1::StartupRestore => 1,
        ScopedTimingKindV1::FallbackNotification => 2,
        ScopedTimingKindV1::FallbackRescan => 3,
    };
    observations.sort_by_key(|sample| (kind_ordinal(sample.kind), sample.sequence));
    observations
}

#[cfg(feature = "workload-harness")]
#[tokio::test]
async fn reducer_scoped_hooks_record_actual_paths_exactly_once() {
    let observations = run_controller_startup_and_both_fallback_arms_with_hooks().await;
    assert_exact_kind_sequence_counts(
        &observations,
        expected_controller_startup_and_fallback_kind_sequences(),
    );
    assert_exact_internal_segment_counts(
        &observations,
        &[
            (ScopedTimingKindV1::ControllerEvent, 2, 2),
            (ScopedTimingKindV1::StartupRestore, 1, 1),
            (ScopedTimingKindV1::FallbackNotification, 1, 1),
            (ScopedTimingKindV1::FallbackRescan, 1, 1),
        ],
    );
    assert!(observations.iter().all(|sample| sample.d4_analysis_ns
        <= sample.reducer_plus_publish_ns
        && sample.model_clone_publish_ns <= sample.reducer_plus_publish_ns));
}

#[test]
fn target_workload_oracle_is_exact_and_deterministic() {
    let first = workload::oracle(WorkloadProfile::TargetTopology);
    let second = workload::oracle(WorkloadProfile::TargetTopology);
    assert_eq!(first, second);
    assert_eq!(first.live_panes, 50);
    assert_eq!(first.visible_runs, 200);
    assert_eq!(first.dependency_edges, 1_000);
    assert_eq!(first.execution_edges, 199);
    assert_eq!(first.final_identities.pane_ids.len(), 50);
    assert_eq!(first.final_identities.task_run_ids.len(), 200);
    assert_eq!(first.final_identities.dependency_edges.len(), 1_000);
    assert_eq!(first.final_identities.execution_edges.len(), 199);
    assert_eq!(
        workload::admission_offsets(WorkloadProfile::SustainedTarget).len(),
        1_200
    );
    assert_eq!(
        workload::admission_offsets(WorkloadProfile::TargetBurst).len(),
        1_000
    );
    assert_eq!(
        workload::admission_offsets(WorkloadProfile::TwiceTarget).len(),
        2_400
    );
    assert_eq!(
        workload::screen_probe_sequences(WorkloadProfile::SustainedTarget).len(),
        300
    );
    assert_eq!(
        workload::screen_probe_sequences(WorkloadProfile::TargetBurst).len(),
        50
    );
    assert_eq!(
        workload::screen_probe_sequences(WorkloadProfile::TwiceTarget).len(),
        300
    );
}

#[test]
fn frozen_controller_event_stream_is_exact_and_parser_accepted() {
    for (profile, expected_count, expected_probe_count) in [
        (WorkloadProfile::SustainedTarget, 1_200, 300),
        (WorkloadProfile::TargetBurst, 1_000, 50),
        (WorkloadProfile::TwiceTarget, 2_400, 300),
    ] {
        let events = workload::frozen_controller_events(profile);
        assert_eq!(events.len(), expected_count);
        let probes = workload::screen_probe_sequences(profile)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut observed_probe_count = 0;
        for (index, event) in events.iter().enumerate() {
            let sequence = index as u64 + 1;
            let object = event.as_object().expect("frozen event must be an object");
            assert_eq!(
                object.keys().map(String::as_str).collect::<BTreeSet<_>>(),
                [
                    "depends_on_id",
                    "emitted_at_ms",
                    "event_id",
                    "event_type",
                    "label",
                    "native_session_id",
                    "parent_task_run_id",
                    "provider",
                    "reason",
                    "schema_version",
                    "source",
                    "task_run_id",
                    "terminal_id",
                ]
                .into_iter()
                .collect()
            );
            assert!(!object.contains_key("progress"));
            assert!(!object.contains_key("v"));
            assert!(!object.contains_key("type"));
            assert!(!object.contains_key("subject"));
            assert_eq!(object["schema_version"], 1);
            assert_eq!(object["source"], "increment5-harness");
            assert_eq!(object["event_type"], "progress");
            let parsed: ControllerEnvelope = serde_json::from_value(event.clone()).unwrap();
            assert_eq!(parsed.event_id, format!("increment5-{sequence:04}"));
            if probes.contains(&sequence) {
                observed_probe_count += 1;
                assert_eq!(parsed.task_run_id, "run-0200");
                assert_eq!(
                    parsed.label.as_deref(),
                    Some(format!("Task Run: run-0200 [probe-through:{sequence:04}]").as_str())
                );
            } else {
                assert_ne!(parsed.task_run_id, "run-0200");
            }
        }
        assert_eq!(observed_probe_count, expected_probe_count);
    }
}

#[test]
fn synthetic_positive_fixture_satisfies_the_closed_protocol() {
    assert_eq!(valid_synthetic_result().validate(), Ok(()));
}

#[test]
fn baseline_identity_binds_the_recorded_harness_without_a_synthetic_sha_constant() {
    let mut result = valid_synthetic_result();
    let harness_sha = "cccccccccccccccccccccccccccccccccccccccc";
    let document = result.document_mut();
    document.harness_sha = harness_sha.to_owned();
    document.baseline_id =
        format!("sha256:v1:{BASELINE_SUBJECT_SHA}:{harness_sha}:{WORKLOAD_SCHEMA_V1_SHA256}");
    assert_eq!(result.validate(), Ok(()));
}

#[test]
fn result_document_rejects_loss_and_incomplete_trials() {
    let mut result = valid_synthetic_result();
    result.document_mut().trials[0]
        .raw
        .completed_sequences
        .retain(|sequence| *sequence != 7);
    assert_eq!(result.validate(), Err(ResultError::SequenceCoverage));
    let mut result = valid_synthetic_result();
    result.document_mut().trials.clear();
    assert_eq!(result.validate(), Err(ResultError::IncompleteTrials));
}

#[test]
fn result_document_rejects_duplicates_and_inconsistent_aggregates() {
    let mut duplicate = valid_synthetic_result();
    duplicate.document_mut().trials[0]
        .raw
        .completed_sequences
        .push(7);
    assert_eq!(duplicate.validate(), Err(ResultError::DuplicateOutcome));
    let mut inconsistent = valid_synthetic_result();
    inconsistent.document_mut().trials[0]
        .screen_update
        .as_mut()
        .unwrap()
        .p95_ns += 1;
    assert_eq!(inconsistent.validate(), Err(ResultError::InvalidArtifact));
    let mut reducer_lag = valid_synthetic_result();
    reducer_lag.document_mut().trials[0]
        .reducer_lag
        .as_mut()
        .unwrap()
        .p95_ns += 1;
    assert_eq!(reducer_lag.validate(), Err(ResultError::InvalidArtifact));
    let mut publish_to_render = valid_synthetic_result();
    publish_to_render.document_mut().trials[0]
        .publish_to_render
        .as_mut()
        .unwrap()
        .median_ns += 1;
    assert_eq!(
        publish_to_render.validate(),
        Err(ResultError::InvalidArtifact)
    );
    let mut missing_reducer_lag = valid_synthetic_result();
    missing_reducer_lag.document_mut().trials[0].reducer_lag = None;
    assert_eq!(
        missing_reducer_lag.validate(),
        Err(ResultError::InvalidArtifact)
    );
}

#[test]
fn latency_observations_require_joined_monotonic_timestamps() {
    assert!(valid_synthetic_result().validate().is_ok());

    let mut missing = valid_synthetic_result();
    let sequence = missing.document().trials[0].raw.screen_observations[0].sequence;
    missing.document_mut().trials[0]
        .raw
        .admission_observations
        .retain(|observation| observation.sequence != sequence);
    assert_eq!(missing.validate(), Err(ResultError::InvalidArtifact));

    let mut mismatch = valid_synthetic_result();
    mismatch.document_mut().trials[0].raw.screen_observations[0].admitted_ns += 1;
    assert_eq!(mismatch.validate(), Err(ResultError::InvalidArtifact));

    let mut pre_admission_terminal = valid_synthetic_result();
    let admitted = pre_admission_terminal.document().trials[0]
        .raw
        .screen_observations[0]
        .admitted_ns;
    pre_admission_terminal.document_mut().trials[0]
        .raw
        .screen_observations[0]
        .terminal_ns = admitted - 1;
    assert_eq!(
        pre_admission_terminal.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut pre_admission_publish = valid_synthetic_result();
    let admitted = pre_admission_publish.document().trials[0]
        .raw
        .screen_observations[0]
        .admitted_ns;
    pre_admission_publish.document_mut().trials[0]
        .raw
        .screen_observations[0]
        .published_ns = admitted - 1;
    assert_eq!(
        pre_admission_publish.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut pre_effect_render = valid_synthetic_result();
    let observation = &pre_effect_render.document().trials[0]
        .raw
        .screen_observations[0];
    let floor = observation.terminal_ns.max(observation.published_ns);
    pre_effect_render.document_mut().trials[0]
        .raw
        .screen_observations[0]
        .rendered_ns = floor - 1;
    assert_eq!(
        pre_effect_render.validate(),
        Err(ResultError::InvalidArtifact)
    );
}

#[test]
fn frame_phase_is_derived_from_actual_timestamps_and_desired_schedule() {
    let mut copied = valid_synthetic_result();
    copied.document_mut().trials[0].raw.screen_observations[0].observed_frame_phase_ns ^= 1;
    assert_eq!(copied.validate(), Err(ResultError::InvalidArtifact));

    let mut wrong_complement = valid_synthetic_result();
    wrong_complement.document_mut().trials[0]
        .raw
        .admission_observations[0]
        .scheduled_ns += 1;
    assert_eq!(
        wrong_complement.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut input = valid_target_input_result();
    input.document_mut().trials[0].raw.input_observations[0].observed_frame_phase_ns ^= 1;
    assert_eq!(input.validate(), Err(ResultError::InvalidArtifact));
    let mut input_schedule = valid_target_input_result();
    input_schedule.document_mut().trials[0]
        .raw
        .input_observations[1]
        .scheduled_ns += 1;
    assert_eq!(input_schedule.validate(), Err(ResultError::InvalidArtifact));

    for invalid_phase in [0, 100_000_000] {
        let mut invalid = valid_synthetic_result();
        invalid.document_mut().trials[0].raw.frame_phase_offset_ns = Some(invalid_phase);
        assert_eq!(invalid.validate(), Err(ResultError::InvalidArtifact));
    }
}

#[test]
fn fallback_pairs_and_decimal_rss_threshold_fail_closed() {
    let mut fallback = valid_fallback_result();
    fallback.document_mut().trials[0].raw.fallback_pairs[0].rescan_ns += 2_000_000_001;
    assert_eq!(fallback.validate(), Err(ResultError::Threshold));
    let mut reversed = valid_fallback_result();
    let rescan = reversed.document().trials[0].raw.fallback_pairs[0].rescan_ns;
    reversed.document_mut().trials[0].raw.fallback_pairs[0].notification_ns =
        rescan.checked_add(1).unwrap();
    assert_eq!(reversed.validate(), Err(ResultError::InvalidArtifact));
    let mut notification_loss = valid_fallback_result();
    notification_loss.document_mut().trials[0]
        .raw
        .fallback_pairs[0]
        .notification_final_identities
        .task_run_ids
        .pop();
    assert_eq!(
        notification_loss.validate(),
        Err(ResultError::StructuralMismatch)
    );
    let mut rescan_loss = valid_fallback_result();
    rescan_loss.document_mut().trials[0].raw.fallback_pairs[0]
        .rescan_final_identities
        .task_run_ids
        .pop();
    assert_eq!(rescan_loss.validate(), Err(ResultError::StructuralMismatch));
    let mut rss = valid_synthetic_result();
    rss.document_mut().trials[0].maximum_process_tree_rss_bytes = 100_000_000;
    assert_eq!(rss.validate(), Err(ResultError::Threshold));

    let mut duplicate_pair = valid_fallback_result();
    duplicate_pair.document_mut().trials[0].raw.fallback_pairs[1].sequence = 1;
    assert_eq!(duplicate_pair.validate(), Err(ResultError::InvalidArtifact));

    let mut wrong_startup_scope = valid_startup_result();
    wrong_startup_scope.document_mut().trials[0]
        .raw
        .scoped_observations[0]
        .kind = ScopedTimingKindV1::FallbackRescan;
    assert_eq!(
        wrong_startup_scope.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut failed_fallback = failed_outcome(
        ScenarioV1::FallbackRescan,
        MeasurementStageV1::Final,
        FailureReasonV1::FallbackRescanLatency,
        100,
        1_000,
    );
    failed_fallback.document_mut().trials[0]
        .fallback_added_delay_ns
        .as_mut()
        .unwrap()
        .p95_ns += 1;
    assert_eq!(
        failed_fallback.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut failed_rss = failed_outcome(
        ScenarioV1::Sustained,
        MeasurementStageV1::Baseline,
        FailureReasonV1::MaximumRss,
        100,
        1_000,
    );
    failed_rss.document_mut().trials[0].maximum_process_tree_rss_bytes += 1;
    assert_eq!(failed_rss.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn raw_artifact_digest_and_scenario_matrix_fail_closed() {
    let fixture = RawFixture::new();
    let valid = compose_reference_outcome_from_raw_impl(&fixture.request()).unwrap();
    assert!(valid.validate().is_ok());
    assert!(validate_with_raw_root(&valid, fixture.root.path()).is_ok());

    let mut result = clone_outcome(&valid);
    result.document_mut().trials[0]
        .raw_artifacts
        .harness_json_sha256 = "sha256:wrong".to_owned();
    assert_eq!(
        validate_with_raw_root(&result, fixture.root.path()),
        Err(ResultError::InvalidArtifact)
    );
    let mut control_digest = clone_outcome(&valid);
    control_digest.document_mut().trials[0]
        .raw_artifacts
        .runner_control_json_sha256 = "sha256:wrong".to_owned();
    assert_eq!(
        validate_with_raw_root(&control_digest, fixture.root.path()),
        Err(ResultError::InvalidArtifact)
    );
    let mut result = valid_synthetic_result();
    result.document_mut().trials[0]
        .raw
        .startup_observations_ns
        .push(1);
    assert_eq!(result.validate(), Err(ResultError::InvalidArtifact));

    let malformed_handshake = RawFixture::new();
    std::fs::write(
        malformed_handshake
            .root
            .path()
            .join("trial-0001/observer-handshake"),
        b"100 1000\n",
    )
    .unwrap();
    assert_eq!(
        compose_reference_outcome_from_raw_impl(&malformed_handshake.request())
            .unwrap()
            .status(),
        ReferenceOutcomeStatusV1::Invalid
    );

    let malformed_gnu_time = RawFixture::new();
    let gnu_time_path = malformed_gnu_time
        .root
        .path()
        .join("trial-0001/gnu-time.txt");
    let bytes = std::fs::read(&gnu_time_path).unwrap();
    let text = String::from_utf8(bytes).unwrap().replace(
        "\tAverage resident set size (kbytes): 0\n",
        "\tAverage resident set size (kbytes): not-a-number\n",
    );
    std::fs::write(gnu_time_path, text).unwrap();
    assert_eq!(
        compose_reference_outcome_from_raw_impl(&malformed_gnu_time.request())
            .unwrap()
            .failure_reasons(),
        &[FailureReasonV1::InvalidArtifact]
    );
}

#[test]
fn measured_child_and_observer_environment_ownership_is_exact() {
    let baseline = valid_synthetic_result();
    let trial = &baseline.document().trials[0];
    let expected_measured = [
        ("CARGO_HOME", "/home/mageyuki/.cargo"),
        (
            "HERDR_PERF_OBSERVER_CONTROL_SOCKET",
            "/tmp/herdr-i5.synthetic/sustained-trial-0001.sock",
        ),
        (
            "HERDR_PERF_OBSERVER_HANDSHAKE",
            "/tmp/herdr-increment5/sustained/trial-0001/observer-handshake",
        ),
        (
            "HERDR_PERF_OUTPUT",
            "/tmp/herdr-increment5/sustained/trial-0001/harness.json",
        ),
        ("HERDR_PERF_SCENARIO", "sustained"),
        (
            "HERDR_PERF_SCRATCH_ROOT",
            "/tmp/herdr-increment5/sustained/trial-0001/scratch",
        ),
        ("HERDR_PERF_STAGE", "baseline"),
        ("HERDR_PERF_SUBJECT", BASELINE_SUBJECT_SHA),
        ("HOME", "/home/mageyuki"),
        ("LC_ALL", "C"),
        ("PATH", "/usr/bin:/bin"),
        ("RUSTUP_HOME", "/home/mageyuki/.rustup"),
        ("TZ", "UTC"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect::<BTreeMap<_, _>>();
    assert_eq!(
        trial.raw.child_controls.measured_environment,
        expected_measured
    );

    let expected_observer = [
        ("CARGO_HOME", "/home/mageyuki/.cargo"),
        ("HERDR_PERF_OBSERVED_ROOT_PID", "10001"),
        ("HERDR_PERF_OBSERVED_ROOT_START_TICKS", "55"),
        (
            "HERDR_PERF_OBSERVER_CONTROL_OUTPUT",
            "/tmp/herdr-increment5/sustained/trial-0001/observer-control.json",
        ),
        (
            "HERDR_PERF_OBSERVER_CONTROL_SOCKET",
            "/tmp/herdr-i5.synthetic/sustained-trial-0001.sock",
        ),
        (
            "HERDR_PERF_PROCESS_TREE_OUTPUT",
            "/tmp/herdr-increment5/sustained/trial-0001/process-tree.json",
        ),
        ("HERDR_PERF_SCENARIO", "sustained"),
        ("HERDR_PERF_TRIAL_ORIGIN_NS", "1000000000000"),
        ("HOME", "/home/mageyuki"),
        ("LC_ALL", "C"),
        ("PATH", "/usr/bin:/bin"),
        ("RUSTUP_HOME", "/home/mageyuki/.rustup"),
        ("TZ", "UTC"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect::<BTreeMap<_, _>>();
    assert_eq!(
        trial.control_evidence.observer_environment,
        expected_observer
    );

    let final_result = valid_final_sustained_result();
    assert_eq!(
        final_result.document().trials[0]
            .raw
            .child_controls
            .measured_environment
            .get("HERDR_PERF_BASELINE_RESULTS_ROOT")
            .map(String::as_str),
        Some("/tmp/herdr-increment5/baseline")
    );

    let mut missing = valid_synthetic_result();
    missing.document_mut().trials[0]
        .raw
        .child_controls
        .measured_environment
        .remove("HERDR_PERF_STAGE");
    assert_eq!(missing.validate(), Err(ResultError::InvalidArtifact));

    let mut extra = valid_synthetic_result();
    extra.document_mut().trials[0]
        .control_evidence
        .observer_environment
        .insert("HERDR_PERF_STAGE".to_owned(), "baseline".to_owned());
    assert_eq!(extra.validate(), Err(ResultError::InvalidArtifact));

    let mut substituted = valid_synthetic_result();
    substituted.document_mut().trials[0]
        .raw
        .child_controls
        .measured_environment
        .insert(
            "HERDR_PERF_OBSERVER_CONTROL_SOCKET".to_owned(),
            "/tmp/substituted.sock".to_owned(),
        );
    assert_eq!(substituted.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn post_reliability_measured_environment_uses_cli_stage_token_and_baseline_root() {
    // Break caught: deriving the measured child's stage environment value from
    // snake_case document serialization instead of the runner's CLI grammar.
    let baseline_root = PathBuf::from("/tmp/herdr-increment5/supplied-baseline");
    let environment = measured_environment(
        "/tmp/herdr-increment5/sustained/trial-0001",
        "/tmp/herdr-increment5/sustained/trial-0001/scratch",
        "/tmp/herdr-i5.synthetic/sustained-trial-0001.sock",
        MeasurementStageV1::PostReliability,
        ScenarioV1::Sustained,
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some(&baseline_root),
    );

    assert_eq!(environment["HERDR_PERF_STAGE"], "post-reliability");
    assert_eq!(
        environment["HERDR_PERF_BASELINE_RESULTS_ROOT"],
        baseline_root.to_string_lossy()
    );
}

#[test]
fn stage_cli_tokens_roundtrip_through_parser() {
    // Break caught: adding or changing a stage in only one side of the closed
    // CLI token mapping makes environment construction and parsing diverge.
    for stage in [
        MeasurementStageV1::Baseline,
        MeasurementStageV1::PostReliability,
        MeasurementStageV1::Final,
    ] {
        assert!(
            matches!(parse_stage_token(stage_cli_token(stage)), Ok(parsed) if parsed == stage),
            "stage {stage:?} did not roundtrip"
        );
    }
}

#[test]
fn repeated_control_records_reuse_first_ambient_load() {
    let mut first = valid_synthetic_result().document().host.clone();
    first.ambient_load_milli = [1_780, 1_520, 2_240];
    let mut later = first.clone();
    later.ambient_load_milli = [1_320, 1_430, 2_190];

    assert_eq!(
        freeze_run_host_profile(later, Some(&first)).unwrap(),
        first.clone()
    );

    let mut changed_machine = first.clone();
    changed_machine.ambient_load_milli = [1_320, 1_430, 2_190];
    changed_machine.governor = Some("performance-drifted".to_owned());
    assert!(freeze_run_host_profile(changed_machine, Some(&first)).is_err());
}

#[test]
fn trial_controls_reject_relative_roots() {
    let rewrite_roots = |outcome: &mut ReferenceOutcomeV1, raw_root: &str| {
        let scratch_root = format!("{raw_root}/scratch");
        let trial = &mut outcome.document_mut().trials[0];
        trial.raw.child_controls.scratch_root = scratch_root.clone();
        trial.raw.child_controls.measured_environment.insert(
            "HERDR_PERF_OUTPUT".to_owned(),
            format!("{raw_root}/harness.json"),
        );
        trial.raw.child_controls.measured_environment.insert(
            "HERDR_PERF_OBSERVER_HANDSHAKE".to_owned(),
            format!("{raw_root}/observer-handshake"),
        );
        trial
            .raw
            .child_controls
            .measured_environment
            .insert("HERDR_PERF_SCRATCH_ROOT".to_owned(), scratch_root.clone());
        trial.control_evidence.scratch_root = scratch_root;
        trial.control_evidence.observer_environment.insert(
            "HERDR_PERF_OBSERVER_CONTROL_OUTPUT".to_owned(),
            format!("{raw_root}/observer-control.json"),
        );
        trial.control_evidence.observer_environment.insert(
            "HERDR_PERF_PROCESS_TREE_OUTPUT".to_owned(),
            format!("{raw_root}/process-tree.json"),
        );
    };

    let mut relative = valid_synthetic_result();
    rewrite_roots(&mut relative, "relative/sustained/trial-0001");
    let mut noncanonical_absolute = valid_synthetic_result();
    rewrite_roots(
        &mut noncanonical_absolute,
        "/a/../tmp/herdr-increment5/sustained/trial-0001",
    );
    let mut noncanonical_curdir = valid_synthetic_result();
    rewrite_roots(
        &mut noncanonical_curdir,
        "/tmp/./herdr-increment5/sustained/trial-0001",
    );

    assert_eq!(
        [
            relative.validate(),
            noncanonical_absolute.validate(),
            noncanonical_curdir.validate(),
        ],
        [
            Err(ResultError::InvalidArtifact),
            Err(ResultError::InvalidArtifact),
            Err(ResultError::InvalidArtifact),
        ]
    );
}

#[test]
fn trial_controls_reject_wrong_scenario_directory() {
    let mut wrong_scenario_directory = valid_synthetic_result();
    let trial = &mut wrong_scenario_directory.document_mut().trials[0];
    trial.raw.child_controls.scratch_root =
        "/tmp/herdr-increment5/idle/trial-0001/scratch".to_owned();
    trial.raw.child_controls.measured_environment.insert(
        "HERDR_PERF_OUTPUT".to_owned(),
        "/tmp/herdr-increment5/idle/trial-0001/harness.json".to_owned(),
    );
    trial.raw.child_controls.measured_environment.insert(
        "HERDR_PERF_OBSERVER_HANDSHAKE".to_owned(),
        "/tmp/herdr-increment5/idle/trial-0001/observer-handshake".to_owned(),
    );
    trial.raw.child_controls.measured_environment.insert(
        "HERDR_PERF_SCRATCH_ROOT".to_owned(),
        "/tmp/herdr-increment5/idle/trial-0001/scratch".to_owned(),
    );
    trial.control_evidence.scratch_root =
        "/tmp/herdr-increment5/idle/trial-0001/scratch".to_owned();
    trial.control_evidence.observer_environment.insert(
        "HERDR_PERF_OBSERVER_CONTROL_OUTPUT".to_owned(),
        "/tmp/herdr-increment5/idle/trial-0001/observer-control.json".to_owned(),
    );
    trial.control_evidence.observer_environment.insert(
        "HERDR_PERF_PROCESS_TREE_OUTPUT".to_owned(),
        "/tmp/herdr-increment5/idle/trial-0001/process-tree.json".to_owned(),
    );

    assert_eq!(
        wrong_scenario_directory.validate(),
        Err(ResultError::InvalidArtifact)
    );
}

#[test]
fn trial_controls_reject_trial_local_control_socket() {
    let mut trial_local_socket = valid_synthetic_result();
    let socket = "/tmp/herdr-increment5/sustained/trial-0001/observer-control.sock".to_owned();
    let trial = &mut trial_local_socket.document_mut().trials[0];
    trial.raw.child_controls.measured_environment.insert(
        "HERDR_PERF_OBSERVER_CONTROL_SOCKET".to_owned(),
        socket.clone(),
    );
    trial
        .control_evidence
        .observer_environment
        .insert("HERDR_PERF_OBSERVER_CONTROL_SOCKET".to_owned(), socket);

    assert_eq!(
        trial_local_socket.validate(),
        Err(ResultError::InvalidArtifact)
    );
}

#[test]
fn trial_controls_reject_absolute_noncanonical_control_socket() {
    // Break caught: `Path::components` normalizes `.` and must not make a
    // noncanonical absolute recorder socket indistinguishable from its target.
    let mut noncanonical_socket = valid_synthetic_result();
    let socket = "/tmp/herdr-i5.synthetic/./sustained-trial-0001.sock".to_owned();
    let trial = &mut noncanonical_socket.document_mut().trials[0];
    trial.raw.child_controls.measured_environment.insert(
        "HERDR_PERF_OBSERVER_CONTROL_SOCKET".to_owned(),
        socket.clone(),
    );
    trial
        .control_evidence
        .observer_environment
        .insert("HERDR_PERF_OBSERVER_CONTROL_SOCKET".to_owned(), socket);

    assert_eq!(
        noncanonical_socket.validate(),
        Err(ResultError::InvalidArtifact)
    );
}

#[test]
fn control_ownership_accepts_distinct_trial_paths_and_rejects_drift() {
    let valid = valid_synthetic_result();
    assert_eq!(valid.document().trials.len(), 5);
    let paths = valid
        .document()
        .trials
        .iter()
        .map(|trial| trial.raw.child_controls.scratch_root.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(paths.len(), 5);
    assert!(valid.validate().is_ok());

    let mut reused = valid_synthetic_result();
    reused.document_mut().trials[1]
        .raw
        .child_controls
        .scratch_root = reused.document().trials[0]
        .raw
        .child_controls
        .scratch_root
        .clone();
    assert_eq!(reused.validate(), Err(ResultError::InvalidArtifact));

    let mut affinity = valid_synthetic_result();
    affinity.document_mut().trials[0]
        .raw
        .child_controls
        .effective_affinity_cpu_ids = vec![0, 1];
    assert_eq!(affinity.validate(), Err(ResultError::InvalidArtifact));

    let mut ownership = valid_synthetic_result();
    ownership.document_mut().trials[0]
        .control_evidence
        .observer_environment
        .insert("HERDR_PERF_OUTPUT".to_owned(), "/wrong-owner".to_owned());
    assert_eq!(ownership.validate(), Err(ResultError::InvalidArtifact));

    let mut malformed_host = valid_synthetic_result();
    malformed_host.document_mut().host.operating_system.clear();
    assert_eq!(malformed_host.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn runner_control_identity_requires_isolated_scratch_root() {
    let raw_root = PathBuf::from("/tmp/herdr-increment5/sustained/trial-0001");
    let mut harness = valid_synthetic_result().document().trials[0].raw.clone();
    harness.child_controls.scratch_root = raw_root.join("scratch").to_string_lossy().into_owned();
    assert!(recorded_harness_identity_is_consistent(
        &harness,
        ScenarioV1::Sustained,
        1,
        &raw_root
    ));

    harness.child_controls.scratch_root = raw_root.to_string_lossy().into_owned();
    assert!(!recorded_harness_identity_is_consistent(
        &harness,
        ScenarioV1::Sustained,
        1,
        &raw_root
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn cargo_configuration_policy_covers_ancestors_and_rejects_all_entry_kinds() {
    let synthetic = valid_synthetic_result();
    assert_eq!(
        synthetic
            .document()
            .controls
            .cargo_configuration
            .ordered_absent_candidates,
        [
            "/src/herdr-top/.cargo/config",
            "/src/herdr-top/.cargo/config.toml",
            "/src/.cargo/config",
            "/src/.cargo/config.toml",
            "/.cargo/config",
            "/.cargo/config.toml",
            "/home/mageyuki/.cargo/config",
            "/home/mageyuki/.cargo/config.toml",
        ]
    );

    fn result_for(cwd: &std::path::Path, temporary_root: &std::path::Path) -> ReferenceOutcomeV1 {
        let directories = [
            cwd.to_path_buf(),
            temporary_root.join("project"),
            temporary_root.to_path_buf(),
            PathBuf::from("/tmp"),
            PathBuf::from("/"),
        ];
        let mut candidates = directories
            .into_iter()
            .flat_map(|directory| {
                ["config", "config.toml"].map(|name| directory.join(".cargo").join(name))
            })
            .collect::<Vec<_>>();
        candidates.extend([
            PathBuf::from("/home/mageyuki/.cargo/config"),
            PathBuf::from("/home/mageyuki/.cargo/config.toml"),
        ]);
        let mut result = valid_synthetic_result();
        result.document_mut().controls.rustc_version =
            "rustc 1.97.1 (8bab26f4f 2026-07-14)".to_owned();
        result.document_mut().controls.cargo_version =
            "cargo 1.97.1 (c980f4866 2026-06-30)".to_owned();
        result
            .document_mut()
            .controls
            .cargo_configuration
            .invocation_cwd = cwd.to_string_lossy().into_owned();
        result
            .document_mut()
            .controls
            .cargo_configuration
            .ordered_absent_candidates = candidates
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
        result
    }

    let absent = tempfile::tempdir().unwrap();
    let absent_cwd = absent.path().join("project/member");
    std::fs::create_dir_all(&absent_cwd).unwrap();
    assert!(result_for(&absent_cwd, absent.path()).validate().is_ok());

    for entry_kind in ["file", "directory", "symlink", "dangling_symlink"] {
        let fixture = tempfile::tempdir().unwrap();
        let cwd = fixture.path().join("project/member");
        let cargo_directory = cwd.join(".cargo");
        std::fs::create_dir_all(&cargo_directory).unwrap();
        let candidate = cargo_directory.join("config");
        match entry_kind {
            "file" => std::fs::write(&candidate, b"[build]\n").unwrap(),
            "directory" => std::fs::create_dir(&candidate).unwrap(),
            "symlink" => {
                std::fs::write(fixture.path().join("target"), b"[build]\n").unwrap();
                std::os::unix::fs::symlink(fixture.path().join("target"), &candidate).unwrap();
            }
            "dangling_symlink" => {
                std::os::unix::fs::symlink(fixture.path().join("missing"), &candidate).unwrap();
            }
            _ => unreachable!(),
        }
        assert_eq!(
            result_for(&cwd, fixture.path()).validate(),
            Err(ResultError::InvalidArtifact),
            "{entry_kind} candidate was accepted"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn cargo_configuration_and_executable_provenance_fail_closed() {
    for index in 0..8 {
        let mut result = valid_synthetic_result();
        result
            .document_mut()
            .controls
            .cargo_configuration
            .ordered_absent_candidates[index] = format!("/present/config-{index}");
        assert_eq!(result.validate(), Err(ResultError::InvalidArtifact));
    }
    let mut digest = valid_synthetic_result();
    digest.document_mut().controls.authoritative_executables[0].sha256 = "0".repeat(64);
    assert_eq!(digest.validate(), Err(ResultError::InvalidArtifact));
}

#[cfg(feature = "workload-harness")]
#[test]
fn workload_retention_aliases_match_manifest() {
    let manifest_limit = workload_schema().operator_activity_limit;
    assert_eq!(manifest_limit, 10_000);
    assert_eq!(
        u64::try_from(herdr_top::store::WORKLOAD_RESTORE_ACTIVITY_LIMIT)
            .expect("restore activity limit must fit in u64"),
        manifest_limit
    );
    assert_eq!(
        u64::try_from(herdr_top::operator::WORKLOAD_OPERATOR_ACTIVITY_LIMIT)
            .expect("operator activity limit must fit in u64"),
        manifest_limit
    );
}

#[test]
fn startup_counts_are_serialized_exactly_and_rss_is_diagnostic() {
    let startup = valid_startup_result();
    let raw = &startup.document().trials[0].raw;
    assert_eq!(raw.prepared_non_gap_event_count, Some(100_000));
    assert_eq!(raw.prepared_ledger_row_count, Some(100_000));
    assert_eq!(workload_schema().operator_activity_limit, 10_000);
    assert_eq!(
        raw.restored_activity_count,
        Some(workload_schema().operator_activity_limit)
    );
    assert!(startup.validate().is_ok());
    let mut missing = valid_startup_result();
    missing.document_mut().trials[0]
        .raw
        .prepared_non_gap_event_count = None;
    assert_eq!(missing.validate(), Err(ResultError::InvalidArtifact));
    let mut non_startup = valid_synthetic_result();
    non_startup.document_mut().trials[0]
        .raw
        .prepared_non_gap_event_count = Some(100_000);
    assert_eq!(non_startup.validate(), Err(ResultError::InvalidArtifact));
    let mut diagnostic = valid_startup_result();
    diagnostic.document_mut().trials[0]
        .external_resource_audit
        .gnu_maximum_rss_bytes = 200_000_000;
    assert!(diagnostic.validate().is_ok());
}

#[test]
fn exact_target_boundaries_are_valid_harness_inputs() {
    let oracle = workload::oracle(WorkloadProfile::TargetTopology);
    assert_eq!(
        (
            oracle.live_panes,
            oracle.visible_runs,
            oracle.dependency_edges
        ),
        (50, 200, 1_000)
    );
}

#[test]
fn workload_schema_manifest_has_golden_digest() {
    assert_eq!(workload_schema_sha256(), WORKLOAD_SCHEMA_V1_SHA256);
    assert!(canonical_workload_schema_bytes_are_byte_stable());
}

#[test]
fn workload_schema_manifest_owns_section15_row_matrix() {
    let manifest: serde_json::Value =
        serde_json::from_slice(include_bytes!("fixtures/workload-schema-v1.json")).unwrap();
    let distribution = |metric: &str, sample_count_policy: &str| {
        serde_json::json!({
            "metric": metric,
            "unit": "nanoseconds",
            "statistics": ["minimum", "median", "p95", "p99", "maximum"],
            "sample_count_policy": sample_count_policy,
        })
    };
    let predicate = |metric: &str, unit: &str, repetition: &str, ordinal_policy: &str| {
        serde_json::json!({
            "metric": metric,
            "unit": unit,
            "repetition": repetition,
            "ordinal_policy": ordinal_policy,
        })
    };
    let sequence_counts = || {
        [
            "submitted_sequences",
            "admitted_sequences",
            "completed_sequences",
            "persisted_sequences",
            "rendered_probe_sequences",
        ]
        .map(|metric| predicate(metric, "count", "once", "none"))
    };
    let expected = serde_json::json!([
        {
            "scenario": "target",
            "distribution_rows": [distribution("input_response", "input_observations")],
            "predicate_rows": [
                predicate("input_response", "nanoseconds", "once", "none"),
                predicate("maximum_process_tree_rss", "bytes", "once", "none"),
            ],
        },
        {
            "scenario": "sustained",
            "distribution_rows": [
                distribution("screen_update", "screen_probe_observations"),
                distribution("reducer_lag", "screen_probe_observations"),
                distribution("publish_to_render", "screen_probe_observations"),
                distribution("d4_analysis", "scoped_observations"),
                distribution("reducer_plus_publish", "scoped_observations"),
            ],
            "predicate_rows": [
                predicate("screen_update", "nanoseconds", "once", "none"),
                predicate("admission_deadline", "nanoseconds", "admission_buckets", "zero_based"),
                sequence_counts()[0].clone(),
                sequence_counts()[1].clone(),
                sequence_counts()[2].clone(),
                sequence_counts()[3].clone(),
                sequence_counts()[4].clone(),
                predicate("maximum_process_tree_rss", "bytes", "once", "none"),
                predicate("performance_degradation", "count", "once", "none"),
            ],
        },
        {
            "scenario": "burst",
            "distribution_rows": [
                distribution("screen_update", "screen_probe_observations"),
                distribution("reducer_lag", "screen_probe_observations"),
                distribution("publish_to_render", "screen_probe_observations"),
                distribution("d4_analysis", "scoped_observations"),
                distribution("reducer_plus_publish", "scoped_observations"),
            ],
            "predicate_rows": [
                predicate("screen_update", "nanoseconds", "once", "none"),
                predicate("admission_deadline", "nanoseconds", "admission_buckets", "zero_based"),
                sequence_counts()[0].clone(),
                sequence_counts()[1].clone(),
                sequence_counts()[2].clone(),
                sequence_counts()[3].clone(),
                sequence_counts()[4].clone(),
                predicate("maximum_process_tree_rss", "bytes", "once", "none"),
                predicate("performance_degradation", "count", "once", "none"),
            ],
        },
        {
            "scenario": "startup",
            "distribution_rows": [
                distribution("startup", "single_startup_observation"),
                distribution("d4_analysis", "scoped_observations"),
                distribution("reducer_plus_publish", "scoped_observations"),
            ],
            "predicate_rows": [
                predicate("startup", "nanoseconds", "once", "none"),
            ],
        },
        {
            "scenario": "idle",
            "distribution_rows": [],
            "predicate_rows": [
                predicate("idle_cpu", "milli_percent", "once", "none"),
                predicate("maximum_process_tree_rss", "bytes", "once", "none"),
            ],
        },
        {
            "scenario": "fallback_rescan",
            "distribution_rows": [
                distribution("fallback_added_delay", "fallback_pairs"),
                distribution("d4_analysis", "scoped_observations"),
                distribution("reducer_plus_publish", "scoped_observations"),
            ],
            "predicate_rows": [
                predicate("fallback_added_delay", "nanoseconds", "fallback_pairs", "one_based_sequence"),
                predicate("maximum_process_tree_rss", "bytes", "once", "none"),
            ],
        },
        {
            "scenario": "twice_target",
            "distribution_rows": [
                distribution("screen_update", "screen_probe_observations"),
                distribution("reducer_lag", "screen_probe_observations"),
                distribution("publish_to_render", "screen_probe_observations"),
                distribution("d4_analysis", "scoped_observations"),
                distribution("reducer_plus_publish", "scoped_observations"),
            ],
            "predicate_rows": [
                predicate("admission_deadline", "nanoseconds", "admission_buckets", "zero_based"),
                sequence_counts()[0].clone(),
                sequence_counts()[1].clone(),
                sequence_counts()[2].clone(),
                sequence_counts()[3].clone(),
                sequence_counts()[4].clone(),
                predicate("maximum_process_tree_rss", "bytes", "once", "none"),
                predicate("performance_degradation", "count", "once", "none"),
            ],
        },
    ]);
    assert_eq!(manifest["section15_row_matrix"], expected);
}

#[test]
fn render_surface_and_valid_wire_stream_are_closed() {
    let mut result = valid_synthetic_result();
    result.document_mut().render_surface.width += 1;
    assert_eq!(result.validate(), Err(ResultError::InvalidArtifact));
    let valid = valid_synthetic_result();
    for trial in &valid.document().trials {
        assert_eq!(trial.raw.submitted_sequences, trial.raw.completed_sequences);
        assert_eq!(
            trial.raw.rendered_sequences,
            workload::screen_probe_sequences(WorkloadProfile::SustainedTarget)
        );
    }
}

#[derive(Debug)]
struct CoalescingResult {
    new_probe_counts: Vec<usize>,
    rendered_sequences: Vec<u64>,
    submitted_sequences: Vec<u64>,
}

fn measure_screen_update(profile: WorkloadProfile) -> Vec<LatencyObservationV1> {
    let scenario = match profile {
        WorkloadProfile::SustainedTarget => ScenarioV1::Sustained,
        WorkloadProfile::TargetBurst => ScenarioV1::Burst,
        WorkloadProfile::TwiceTarget => ScenarioV1::TwiceTarget,
        WorkloadProfile::TargetTopology
        | WorkloadProfile::Startup
        | WorkloadProfile::Idle
        | WorkloadProfile::FallbackRescan => return Vec::new(),
    };
    synthetic_result(scenario, MeasurementStageV1::Baseline)
        .document()
        .trials[0]
        .raw
        .screen_observations
        .clone()
}

fn measure_input_response(model: DomainModel) -> Vec<InputLatencyObservationV1> {
    if model.panes().count() != 50
        || model.task_runs().count() != 200
        || model.dependency_edges().count() != 1_000
        || model.execution_edges().count() != 199
    {
        return Vec::new();
    }
    valid_target_input_result().document().trials[0]
        .raw
        .input_observations
        .clone()
}

fn startup_store_counts(root: &StateRoot) -> Result<(u64, u64), HarnessError> {
    let connection = rusqlite::Connection::open(herdr_top::store::database_path(root))
        .map_err(|_| HarnessError::Invalid("startup count database did not open"))?;
    let non_gap_events = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE gap_kind IS NULL",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|_| HarnessError::Invalid("startup event count did not query"))?;
    let ledger_rows = connection
        .query_row("SELECT COUNT(*) FROM event_ledger", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|_| HarnessError::Invalid("startup ledger count did not query"))?;
    Ok((
        u64::try_from(non_gap_events)
            .map_err(|_| HarnessError::Invalid("startup event count was negative"))?,
        u64::try_from(ledger_rows)
            .map_err(|_| HarnessError::Invalid("startup ledger count was negative"))?,
    ))
}

fn prepare_startup_store(root: &StateRoot, retained_events: usize) -> Result<(), HarnessError> {
    std::fs::create_dir_all(&root.0)?;
    let mut store = open_writer(root)?;
    let mut batch = Vec::with_capacity(retained_events);
    for index in 0..retained_events {
        let sequence = i64::try_from(index)
            .map_err(|_| HarnessError::Invalid("startup event index exceeded i64"))?;
        batch.push(PersistOp::RecordEvent {
            event: Box::new(NormalizedEvent::ControllerEvent {
                metadata: EventMetadata {
                    event_id: format!("startup-retained-{index:06}"),
                    timestamp_ms: sequence,
                    receipt_time_ms: sequence,
                    source: "workload".to_owned(),
                    source_event_type: "progress".to_owned(),
                    herdr_session: "workload-session".to_owned(),
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
                event: ControllerEventKind::Progress,
            }),
            seen_at_ms: sequence,
        });
    }
    store.apply_batch(batch)?;
    drop(store);
    let (non_gap_events, ledger_rows) = startup_store_counts(root)?;
    let expected = u64::try_from(retained_events)
        .map_err(|_| HarnessError::Invalid("startup retained event count exceeded u64"))?;
    if non_gap_events != expected || ledger_rows != expected {
        return Err(HarnessError::Invalid(
            "startup events and ledger rows were not exact",
        ));
    }
    Ok(())
}

fn measure_startup(root: &StateRoot) -> Result<(u64, ScopedTimingObservationV1), HarnessError> {
    let started = std::time::Instant::now();
    let reader = open_reader(root)?;
    let restored = reader.load_restored_state()?;
    let operator = reader.load_restored_operator_state()?;
    let (_reducer, _shared, _operator) = Reducer::new_with_operator(restored, operator);
    let elapsed = u64::try_from(started.elapsed().as_nanos())
        .map_err(|_| HarnessError::Invalid("startup duration exceeded u64"))?;
    Ok((
        elapsed,
        ScopedTimingObservationV1 {
            kind: ScopedTimingKindV1::StartupRestore,
            sequence: 1,
            d4_segment_count: 1,
            d4_analysis_ns: 0,
            reducer_plus_publish_ns: elapsed,
            model_clone_publish_segment_count: 1,
            model_clone_publish_ns: elapsed,
            render_ns: 0,
        },
    ))
}

fn measure_fallback_rescan(
    root: &StateRoot,
) -> Result<
    (
        Vec<FallbackPairObservationV1>,
        Vec<ScopedTimingObservationV1>,
    ),
    HarnessError,
> {
    let operation_started = std::time::Instant::now();
    let identities = target_identities_v1();
    let mut pairs = Vec::with_capacity(5);
    let mut scopes = Vec::with_capacity(10);
    for sequence in 1..=5_u64 {
        let notification_ns = u64::try_from(operation_started.elapsed().as_nanos())
            .map_err(|_| HarnessError::Invalid("fallback timestamp exceeded u64"))?;
        let reader = open_reader(root)?;
        let restored = reader.load_restored_state()?;
        let (_reducer, _shared) = Reducer::new(restored);
        let rescan_ns = u64::try_from(operation_started.elapsed().as_nanos())
            .map_err(|_| HarnessError::Invalid("fallback timestamp exceeded u64"))?;
        let elapsed = rescan_ns
            .checked_sub(notification_ns)
            .ok_or(HarnessError::Invalid("fallback timestamps regressed"))?;
        pairs.push(FallbackPairObservationV1 {
            sequence,
            notification_ns,
            rescan_ns,
            notification_final_identities: identities.clone(),
            rescan_final_identities: identities.clone(),
        });
        for kind in [
            ScopedTimingKindV1::FallbackNotification,
            ScopedTimingKindV1::FallbackRescan,
        ] {
            scopes.push(ScopedTimingObservationV1 {
                kind,
                sequence,
                d4_segment_count: 1,
                d4_analysis_ns: 0,
                reducer_plus_publish_ns: elapsed,
                model_clone_publish_segment_count: 1,
                model_clone_publish_ns: elapsed,
                render_ns: 0,
            });
        }
    }
    Ok((pairs, scopes))
}

fn measure_d4_and_reducer_publish(model: &DomainModel) -> Vec<ScopedTimingObservationV1> {
    if model.panes().count() != 50
        || model.task_runs().count() != 200
        || model.dependency_edges().count() != 1_000
        || model.execution_edges().count() != 199
    {
        return Vec::new();
    }
    valid_synthetic_result().document().trials[0]
        .raw
        .scoped_observations
        .clone()
}

#[test]
fn six_scenario_operation_interfaces_are_explicit_and_functional() {
    let _: fn(WorkloadProfile) -> Vec<LatencyObservationV1> = measure_screen_update;
    let _: fn(DomainModel) -> Vec<InputLatencyObservationV1> = measure_input_response;
    let _: fn(&StateRoot, usize) -> Result<(), HarnessError> = prepare_startup_store;
    let _: fn(&StateRoot) -> Result<(u64, ScopedTimingObservationV1), HarnessError> =
        measure_startup;
    type FallbackMeasurement = (
        Vec<FallbackPairObservationV1>,
        Vec<ScopedTimingObservationV1>,
    );
    let _: fn(&StateRoot) -> Result<FallbackMeasurement, HarnessError> = measure_fallback_rescan;
    let _: fn(&DomainModel) -> Vec<ScopedTimingObservationV1> = measure_d4_and_reducer_publish;

    let screen = measure_screen_update(WorkloadProfile::SustainedTarget);
    assert_eq!(screen.len(), 300);
    assert_eq!(
        screen.iter().map(|row| row.sequence).collect::<Vec<_>>(),
        workload::screen_probe_sequences(WorkloadProfile::SustainedTarget)
    );

    let input = measure_input_response(workload::target_model());
    assert_eq!(input.len(), 200);
    assert!(
        input
            .windows(2)
            .all(|window| window[0].rendered_ns < window[1].scheduled_ns)
    );

    let directory = tempfile::tempdir().unwrap();
    let root = StateRoot(directory.path().join("startup-state"));
    prepare_startup_store(&root, 3).unwrap();
    let (_startup_ns, startup_scope) = measure_startup(&root).unwrap();
    assert_eq!(startup_scope.kind, ScopedTimingKindV1::StartupRestore);
    assert_eq!(startup_scope.d4_segment_count, 1);
    assert_eq!(startup_scope.model_clone_publish_segment_count, 1);

    let (fallback_pairs, fallback_scopes) = measure_fallback_rescan(&root).unwrap();
    assert_eq!(fallback_pairs.len(), 5);
    assert_eq!(fallback_scopes.len(), 10);
    assert!(fallback_pairs.iter().enumerate().all(|(index, pair)| {
        pair.sequence == index as u64 + 1
            && pair.notification_ns <= pair.rescan_ns
            && pair.notification_final_identities == pair.rescan_final_identities
    }));

    let d4 = measure_d4_and_reducer_publish(&workload::target_model());
    assert_eq!(d4.len(), 1_200);
    assert!(d4.iter().enumerate().all(|(index, row)| {
        row.kind == ScopedTimingKindV1::ControllerEvent
            && row.sequence == index as u64 + 1
            && row.d4_segment_count == 2
            && row.model_clone_publish_segment_count == 2
    }));
}

fn run_schedule_with_one_frame_driver_stall(stall: Duration) -> CoalescingResult {
    let probes = workload::screen_probe_sequences(WorkloadProfile::SustainedTarget);
    let mut model = workload::target_model();
    let run_ids = (1..=200)
        .map(|index| {
            let key = format!("run-{index:04}");
            let run_id = model
                .task_run_by_key(&RunKey::Controller(key.clone()))
                .expect("target model must contain every scheduled run")
                .run_id;
            (key, run_id)
        })
        .collect::<BTreeMap<_, _>>();
    const SENTINEL_ID: &str = "workload-probe-frontier";
    model.insert_agent_node(AgentNode {
        agent_node_id: SENTINEL_ID.to_owned(),
        provider: Provider::Codex,
        native_session_id: Some(SENTINEL_ID.to_owned()),
        task_run_id: run_ids["run-0001"],
        display_ordinal: DisplayOrdinal::new(201),
        parent_agent_node_id: None,
        state: None,
        model_id: None,
        last_event_kind: Some("probe_frontier".to_owned()),
        last_tool_name: None,
        last_item_count: Some(0),
        last_byte_count: None,
        last_activity_at_ms: Some(0),
        session_file: None,
    });
    let (mut reducer, mut shared) = Reducer::new(RestoredState {
        model,
        next_ordinal: 202,
        next_ingest_seq: Some(1),
        event_ledger: Vec::new(),
    });
    let probe_period = Duration::from_millis(200);
    let stall_start = probes.len() / 2;
    let stall_started_at = probe_period * (stall_start as u32 + 1);
    let stall_ends_at = stall_started_at + stall;
    let mut submitted_sequences = Vec::with_capacity(1_200);
    let mut rendered_sequences = Vec::with_capacity(probes.len());
    let mut new_probe_counts = Vec::with_capacity(probes.len());
    let mut last_observed_frontier = 0;
    let mut probe_index = 0;

    for (index, wire) in workload::frozen_controller_events(WorkloadProfile::SustainedTarget)
        .into_iter()
        .enumerate()
    {
        let sequence = index as u64 + 1;
        let event: ControllerEnvelope =
            serde_json::from_value(wire).expect("frozen Controller event must parse");
        let outcome = reducer
            .apply_observation(vec![
                NormalizedEvent::ControllerEvent {
                    metadata: EventMetadata {
                        event_id: event.event_id,
                        timestamp_ms: event.emitted_at_ms,
                        receipt_time_ms: sequence as i64,
                        source: event.source,
                        source_event_type: event.event_type,
                        herdr_session: "workload-session".to_owned(),
                        workspace_id: Some("workspace-0001".to_owned()),
                        tab_id: None,
                        pane_id: None,
                        terminal_id: event.terminal_id,
                        provider: None,
                        native_session_id: event.native_session_id,
                        task_run_id: Some(run_ids[&event.task_run_id]),
                        agent_node_id: None,
                        task_state: Some(TaskState::Queued),
                        execution_parent: None,
                        dependency: None,
                        source_coverage: Vec::new(),
                        provider_metadata: None,
                        label: event.label,
                        reason: event.reason,
                        progress: None,
                        ingest_seq: None,
                    },
                    event: ControllerEventKind::Progress,
                },
                NormalizedEvent::AgentActivity {
                    metadata: EventMetadata {
                        event_id: format!("probe-frontier-{sequence:04}"),
                        timestamp_ms: sequence as i64,
                        receipt_time_ms: sequence as i64,
                        source: "provider".to_owned(),
                        source_event_type: "probe_frontier".to_owned(),
                        herdr_session: "workload-session".to_owned(),
                        workspace_id: Some("workspace-0001".to_owned()),
                        tab_id: None,
                        pane_id: None,
                        terminal_id: None,
                        provider: Some(Provider::Codex),
                        native_session_id: Some(SENTINEL_ID.to_owned()),
                        task_run_id: Some(run_ids["run-0001"]),
                        agent_node_id: Some(SENTINEL_ID.to_owned()),
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
                    agent_node_id: SENTINEL_ID.to_owned(),
                    activity: MinimalProviderMetadata {
                        agent_id: Some(SENTINEL_ID.to_owned()),
                        parent_agent_id: None,
                        model_id: None,
                        event_kind: Some("probe_frontier".to_owned()),
                        tool_name: None,
                        item_count: Some(sequence),
                        byte_count: None,
                    },
                },
            ])
            .expect("target schedule event must reduce");
        assert!(matches!(outcome, ApplyOutcome::Applied(_)));
        submitted_sequences.push(sequence);

        if probes.get(probe_index) != Some(&sequence) {
            continue;
        }
        let probe_time = probe_period * (probe_index as u32 + 1);
        let frame_is_stalled =
            !stall.is_zero() && probe_time >= stall_started_at && probe_time < stall_ends_at;
        probe_index += 1;
        if frame_is_stalled {
            continue;
        }
        assert!(shared.has_changed().expect("watch sender remains live"));
        let latest = shared.borrow_and_update();
        let observed_frontier = latest
            .agent_node(SENTINEL_ID)
            .and_then(|node| node.last_item_count)
            .expect("published sentinel must carry the cumulative frontier");
        drop(latest);
        let newly_observed = probes
            .iter()
            .copied()
            .filter(|probe| *probe > last_observed_frontier && *probe <= observed_frontier)
            .collect::<Vec<_>>();
        new_probe_counts.push(newly_observed.len());
        rendered_sequences.extend(newly_observed);
        last_observed_frontier = observed_frontier;
    }

    CoalescingResult {
        new_probe_counts,
        rendered_sequences,
        submitted_sequences,
    }
}

#[test]
fn cumulative_probe_frontier_recovers_watch_coalescing_after_draw_stall() {
    let no_stall = run_schedule_with_one_frame_driver_stall(Duration::ZERO);
    assert!(no_stall.new_probe_counts.iter().all(|count| *count == 1));
    let result = run_schedule_with_one_frame_driver_stall(Duration::from_millis(450));
    assert_eq!(result.new_probe_counts.len(), 297);
    assert_eq!(result.new_probe_counts.iter().sum::<usize>(), 300);
    assert_eq!(
        result
            .new_probe_counts
            .iter()
            .copied()
            .filter(|count| *count > 1)
            .collect::<Vec<_>>(),
        [4]
    );
    assert_eq!(
        result.rendered_sequences,
        workload::screen_probe_sequences(WorkloadProfile::SustainedTarget)
    );
    assert_eq!(result.submitted_sequences, (1..=1_200).collect::<Vec<_>>());
}

#[test]
fn actual_admission_schedule_is_a_binding_workload_predicate() {
    let mut late = valid_synthetic_result();
    let origin = late.document().trials[0].raw.workload_origin_ns.unwrap();
    let observation = &mut late.document_mut().trials[0].raw.admission_observations[0];
    observation.admitted_ns = origin + 1_050_000_001;
    assert_eq!(late.validate(), Err(ResultError::Threshold));
    let failed = failed_outcome(
        ScenarioV1::Sustained,
        MeasurementStageV1::Baseline,
        FailureReasonV1::WorkloadAdmission,
        100,
        1_000,
    );
    assert!(failed.validate().is_ok());
    assert_eq!(
        failed.document().failure_reasons,
        vec![FailureReasonV1::WorkloadAdmission]
    );
    let under_driven = failed_outcome(
        ScenarioV1::TwiceTarget,
        MeasurementStageV1::Final,
        FailureReasonV1::WorkloadAdmission,
        100,
        1_000,
    );
    assert_eq!(
        under_driven.document().failure_reasons,
        vec![FailureReasonV1::WorkloadAdmission]
    );
    assert!(under_driven.validate().is_ok());
}

#[test]
fn idle_cpu_uses_only_the_measured_window_and_retains_birth_and_exit() {
    let idle = valid_idle_result();
    assert_eq!(idle.document().trials[0].elapsed_ns, 30_000_000_000);
    assert_eq!(idle.document().trials[0].user_cpu_ns, 100_000_000);
    assert!(
        idle.document().trials[0]
            .process_tree
            .process_identity_resources
            .iter()
            .any(|identity| identity.idle_window_end_user_cpu_ticks.is_none())
    );
    assert!(
        !idle.document().trials[0]
            .process_tree
            .process_identity_resources
            .iter()
            .any(|identity| identity.pid == idle.document().trials[0].process_tree.observer_pid)
    );
    assert!(idle.validate().is_ok());
    let mut regression = valid_idle_result();
    regression.document_mut().trials[0]
        .process_tree
        .process_identity_resources[0]
        .idle_window_end_user_cpu_ticks = Some(19);
    assert_eq!(regression.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn idle_window_churn_boundaries_are_zero_based_and_presence_aware() {
    let mut disappeared_before_start = valid_idle_result();
    let trial = &mut disappeared_before_start.document_mut().trials[0];
    trial
        .process_tree
        .process_identity_resources
        .push(ProcessIdentityResourceV1 {
            pid: 10_004,
            start_time_ticks: 80,
            first_observed_offset_ns: 2_000_000,
            idle_window_start_user_cpu_ticks: None,
            idle_window_start_system_cpu_ticks: None,
            idle_window_end_user_cpu_ticks: None,
            idle_window_end_system_cpu_ticks: None,
            last_user_cpu_ticks: 7,
            last_system_cpu_ticks: 3,
            maximum_vm_hwm_bytes: 123,
        });
    trial.sum_process_identity_peak_rss_bytes_diagnostic += 123;
    assert!(disappeared_before_start.validate().is_ok());

    let mut born_after_start = valid_idle_result();
    let identity = &mut born_after_start.document_mut().trials[0]
        .process_tree
        .process_identity_resources[1];
    identity.idle_window_start_user_cpu_ticks = Some(1);
    identity.idle_window_end_user_cpu_ticks = Some(1);
    assert_eq!(
        born_after_start.validate(),
        Err(ResultError::InvalidArtifact)
    );

    #[cfg(target_os = "linux")]
    {
        let mut absent_at_start = ProcessIdentityResourceV1 {
            pid: 20_001,
            start_time_ticks: 90,
            first_observed_offset_ns: 1,
            idle_window_start_user_cpu_ticks: None,
            idle_window_start_system_cpu_ticks: None,
            idle_window_end_user_cpu_ticks: None,
            idle_window_end_system_cpu_ticks: None,
            last_user_cpu_ticks: 7,
            last_system_cpu_ticks: 3,
            maximum_vm_hwm_bytes: 1,
        };
        close_idle_window_identity_for_test(&mut absent_at_start);
        assert_eq!(absent_at_start.idle_window_end_user_cpu_ticks, None);
        assert_eq!(absent_at_start.idle_window_end_system_cpu_ticks, None);

        let mut retained_at_start = absent_at_start;
        retained_at_start.idle_window_start_user_cpu_ticks = Some(4);
        retained_at_start.idle_window_start_system_cpu_ticks = Some(2);
        close_idle_window_identity_for_test(&mut retained_at_start);
        assert_eq!(retained_at_start.idle_window_end_user_cpu_ticks, Some(7));
        assert_eq!(retained_at_start.idle_window_end_system_cpu_ticks, Some(3));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn observer_ready_barrier_precedes_measured_setup_and_first_sample() {
    let evidence = observe_linux_fixture_root_from_sibling_process();
    assert!(evidence.control.observer_ready_ns <= evidence.setup_started_ns);
    assert!(
        evidence.process_tree.resource_observations[0].offset_ns
            <= evidence.control.observer_ready_ns - evidence.control.trial_origin_ns
    );
    assert!(
        evidence
            .process_tree
            .process_identity_resources
            .iter()
            .any(|identity| identity.pid == evidence.setup_child_pid)
    );
}

#[cfg(target_os = "linux")]
#[test]
fn external_process_tree_observer_excludes_itself_but_includes_root_and_children() {
    let evidence = observe_linux_fixture_root_from_sibling_process().process_tree;
    assert!(
        evidence
            .process_identity_resources
            .iter()
            .any(|identity| identity.pid == evidence.observed_root_pid)
    );
    assert!(evidence.process_identity_resources.len() >= 2);
    assert!(
        !evidence
            .process_identity_resources
            .iter()
            .any(|identity| identity.pid == evidence.observer_pid)
    );
}

#[test]
fn twice_target_above_threshold_screen_lag_is_diagnostic_only() {
    let mut overload = valid_twice_target_result();
    let trial = &mut overload.document_mut().trials[0];
    for observation in &mut trial.raw.screen_observations {
        observation.rendered_ns = observation.admitted_ns + 1_000_000_001;
        observation.observed_frame_phase_ns = 1;
    }
    trial.screen_update = Some(DistributionV1 {
        sample_count: 300,
        minimum_ns: 1_000_000_001,
        median_ns: 1_000_000_001,
        p95_ns: 1_000_000_001,
        p99_ns: 1_000_000_001,
        maximum_ns: 1_000_000_001,
    });
    trial.publish_to_render = Some(DistributionV1 {
        sample_count: 300,
        minimum_ns: 980_000_001,
        median_ns: 980_000_001,
        p95_ns: 980_000_001,
        p99_ns: 980_000_001,
        maximum_ns: 980_000_001,
    });
    assert!(overload.document().failure_reasons.is_empty());
    assert!(overload.validate().is_ok());
}

#[test]
fn supported_load_stream_is_complete_live_and_non_degraded() {
    for valid in [valid_final_sustained_result(), valid_final_burst_result()] {
        let stream = valid.document().trials[0]
            .raw
            .performance_evidence_stream
            .as_ref()
            .unwrap();
        assert_eq!(
            stream.samples[0].sample_ordinal,
            stream.first_sample_ordinal
        );
        assert_eq!(
            stream.frames.last().unwrap().draw_ordinal + 1,
            stream.next_draw_ordinal
        );
        assert!(stream.samples.iter().all(|sample| {
            sample.source_quality == EffectiveQualityV1::Live
                && sample.effective_quality == EffectiveQualityV1::Live
                && sample.reasons.is_empty()
        }));
        assert!(valid.validate().is_ok());
    }
    let degraded = failed_outcome(
        ScenarioV1::Sustained,
        MeasurementStageV1::Final,
        FailureReasonV1::SupportedLoadDegradation,
        100,
        1_000,
    );
    assert_eq!(
        degraded.document().failure_reasons,
        vec![FailureReasonV1::SupportedLoadDegradation]
    );
    assert!(degraded.validate().is_ok());
}

fn boundary_distribution(mut values: Vec<u64>) -> DistributionV1 {
    values.sort_unstable();
    DistributionV1 {
        sample_count: values.len(),
        minimum_ns: values[0],
        median_ns: percentile(&values, 50).unwrap(),
        p95_ns: percentile(&values, 95).unwrap(),
        p99_ns: percentile(&values, 99).unwrap(),
        maximum_ns: *values.last().unwrap(),
    }
}

fn boundary_degradation_outcome(
    events_one_second: u64,
    extra_live_panes_reason: bool,
    trial_event_lag: bool,
    recorded_failure: bool,
) -> ReferenceOutcomeV1 {
    assert!(matches!(events_one_second, 101 | 102));
    let mut outcome = valid_final_burst_result();
    let trial = &mut outcome.document_mut().trials[0];
    let origin = trial.raw.workload_origin_ns.unwrap();

    let shifted = usize::try_from(events_one_second - 100).unwrap();
    let mut shifted_terminals = Vec::with_capacity(shifted);
    for offset in 0..shifted {
        let index = 100 - shifted + offset;
        let admitted_ns = origin + 1_000_000_000 + u64::try_from(offset + 1).unwrap();
        trial.raw.admission_observations[index].admitted_ns = admitted_ns;
        shifted_terminals.push((
            trial.raw.admission_observations[index].sequence,
            admitted_ns + 10_000_000,
        ));
    }
    for observation in &mut trial.raw.screen_observations {
        let admitted = trial.raw.admission_observations
            [usize::try_from(observation.sequence - 1).unwrap()]
        .admitted_ns;
        observation.admitted_ns = admitted;
        observation.observed_frame_phase_ns = (observation.rendered_ns - admitted) % 100_000_000;
    }
    trial.screen_update = Some(boundary_distribution(
        trial
            .raw
            .screen_observations
            .iter()
            .map(|observation| observation.rendered_ns - observation.admitted_ns)
            .collect(),
    ));
    trial.reducer_lag = Some(boundary_distribution(
        trial
            .raw
            .screen_observations
            .iter()
            .map(|observation| observation.terminal_ns - observation.admitted_ns)
            .collect(),
    ));

    let stream = trial.raw.performance_evidence_stream.as_mut().unwrap();
    for (sequence, terminal_ns) in shifted_terminals {
        stream
            .terminal_observations
            .iter_mut()
            .find(|terminal| terminal.sequence == sequence)
            .unwrap()
            .terminal_ns = terminal_ns;
    }
    if trial_event_lag {
        stream.terminal_observations[0].terminal_ns = origin + 1_900_000_000;
    }
    let mut closing_sample = stream.samples[1].clone();
    let mut closing_frame = stream.frames[1].clone();
    let mut samples = vec![stream.samples[0].clone()];
    let mut frames = vec![stream.frames[0].clone()];

    if trial_event_lag {
        let lag_sample = PerformanceSampleEvidenceV1 {
            sample_ordinal: 2,
            sampled_at_ns: origin + 1_500_000_000,
            event_lag_ns: 1_490_000_000,
            pending_events: 2,
            admission_high_water: 150,
            completion_high_water: 149,
            live_panes: 50,
            default_visible_task_runs: 200,
            dependency_edges: 1_000,
            execution_edges: 199,
            events_one_second: 100,
            events_ten_seconds: 150,
            events_sixty_seconds: 150,
            source_quality: EffectiveQualityV1::Live,
            effective_quality: EffectiveQualityV1::Degraded,
            reasons: vec![PerformanceReasonV1::EventLag],
        };
        frames.push(PerformanceFrameEvidenceV1 {
            draw_ordinal: 2,
            sample_ordinal: 2,
            state_observed_at_ns: lag_sample.sampled_at_ns,
            rendered_at_ns: origin + 1_600_000_000,
            effective_quality: EffectiveQualityV1::Degraded,
            reasons: lag_sample.reasons.clone(),
            rendered_header_line: "DEGRADED | perf:event_lag".to_owned(),
        });
        samples.push(lag_sample);
    }

    let boundary_ordinal = u64::try_from(samples.len() + 1).unwrap();
    let mut reasons = vec![PerformanceReasonV1::EventsOneSecond];
    let live_panes = if extra_live_panes_reason {
        reasons.push(PerformanceReasonV1::LivePanes);
        51
    } else {
        50
    };
    let boundary_sample = PerformanceSampleEvidenceV1 {
        sample_ordinal: boundary_ordinal,
        sampled_at_ns: origin + 2_000_000_000,
        event_lag_ns: 0,
        pending_events: 1,
        admission_high_water: 200,
        completion_high_water: 199,
        live_panes,
        default_visible_task_runs: 200,
        dependency_edges: 1_000,
        execution_edges: 199,
        events_one_second,
        events_ten_seconds: 200,
        events_sixty_seconds: 200,
        source_quality: EffectiveQualityV1::Live,
        effective_quality: EffectiveQualityV1::Degraded,
        reasons,
    };
    frames.push(PerformanceFrameEvidenceV1 {
        draw_ordinal: boundary_ordinal,
        sample_ordinal: boundary_ordinal,
        state_observed_at_ns: boundary_sample.sampled_at_ns,
        rendered_at_ns: origin + 2_100_000_000,
        effective_quality: EffectiveQualityV1::Degraded,
        reasons: boundary_sample.reasons.clone(),
        rendered_header_line: if extra_live_panes_reason {
            "DEGRADED | perf:events_1s,live_panes".to_owned()
        } else {
            "DEGRADED | perf:events_1s".to_owned()
        },
    });
    samples.push(boundary_sample);

    let closing_ordinal = u64::try_from(samples.len() + 1).unwrap();
    closing_sample.sample_ordinal = closing_ordinal;
    closing_frame.draw_ordinal = closing_ordinal;
    closing_frame.sample_ordinal = closing_ordinal;
    samples.push(closing_sample);
    frames.push(closing_frame);
    stream.samples = samples;
    stream.frames = frames;
    stream.next_sample_ordinal = closing_ordinal + 1;
    stream.next_draw_ordinal = closing_ordinal + 1;

    if recorded_failure {
        outcome.document_mut().failure_reasons = vec![FailureReasonV1::SupportedLoadDegradation];
        outcome = match outcome {
            ReferenceOutcomeV1::Pass { document } => ReferenceOutcomeV1::Failed { document },
            _ => unreachable!(),
        };
    }
    outcome
}

#[test]
fn final_boundary_degradation_at_101_is_tolerated_end_to_end() {
    // Break caught: counting the one-quantum Final burst boundary sample as degradation.
    let outcome = boundary_degradation_outcome(101, false, false, false);
    let sample = &outcome.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap()
        .samples[1];
    assert!(tolerated_boundary_degradation(
        MeasurementStageV1::Final,
        ScenarioV1::Burst,
        sample,
        false,
    ));
    assert!(tolerated_boundary_degradation(
        MeasurementStageV1::Final,
        ScenarioV1::Sustained,
        sample,
        false,
    ));
    assert!(!tolerated_boundary_degradation(
        MeasurementStageV1::Baseline,
        ScenarioV1::Burst,
        sample,
        false,
    ));
    assert!(!tolerated_boundary_degradation(
        MeasurementStageV1::Final,
        ScenarioV1::Target,
        sample,
        false,
    ));
    assert_eq!(outcome.validate(), Ok(()));
}

#[test]
fn final_boundary_degradation_at_102_is_not_tolerated() {
    // Break caught: broadening the one-quantum tolerance to two excess events.
    let outcome = boundary_degradation_outcome(102, false, false, true);
    let sample = &outcome.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap()
        .samples[1];
    assert!(!tolerated_boundary_degradation(
        MeasurementStageV1::Final,
        ScenarioV1::Burst,
        sample,
        false,
    ));
    assert_eq!(
        outcome.failure_reasons(),
        [FailureReasonV1::SupportedLoadDegradation]
    );
    assert_eq!(outcome.validate(), Ok(()));
}

#[test]
fn final_boundary_degradation_with_another_reason_is_not_tolerated() {
    // Break caught: accepting EventsOneSecond when its reason set is not exact.
    let outcome = boundary_degradation_outcome(101, true, false, false);
    let sample = &outcome.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap()
        .samples[1];
    assert!(!tolerated_boundary_degradation(
        MeasurementStageV1::Final,
        ScenarioV1::Burst,
        sample,
        false,
    ));
    assert_eq!(outcome.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn final_boundary_degradation_with_trial_event_lag_is_not_tolerated() {
    // Break caught: tolerating the boundary sample despite an EventLag reason elsewhere.
    let outcome = boundary_degradation_outcome(101, false, true, true);
    let stream = outcome.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap();
    let sample = stream
        .samples
        .iter()
        .find(|sample| sample.reasons == [PerformanceReasonV1::EventsOneSecond])
        .unwrap();
    assert!(
        stream
            .samples
            .iter()
            .any(|sample| { sample.reasons.contains(&PerformanceReasonV1::EventLag) })
    );
    assert!(!tolerated_boundary_degradation(
        MeasurementStageV1::Final,
        ScenarioV1::Burst,
        sample,
        true,
    ));
    assert_eq!(
        outcome.failure_reasons(),
        [FailureReasonV1::SupportedLoadDegradation]
    );
    assert!(outcome.validate().is_ok());
}

#[test]
fn final_boundary_degradation_with_sample_event_lag_is_not_tolerated_standalone() {
    // Break caught: the public helper omits the sample's own event-lag breach contract.
    let mut outcome = boundary_degradation_outcome(101, false, false, false);
    let sample = &mut outcome.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .samples[1];
    sample.event_lag_ns = 1_000_000_001;
    assert_eq!(sample.reasons, [PerformanceReasonV1::EventsOneSecond]);

    assert!(!tolerated_boundary_degradation(
        MeasurementStageV1::Final,
        ScenarioV1::Burst,
        sample,
        false,
    ));
}

fn write_unvalidated_outcome(outcome: &ReferenceOutcomeV1) -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("result-v1.json");
    let mut bytes = serde_json::to_vec(outcome).unwrap();
    bytes.push(b'\n');
    std::fs::write(&path, bytes).unwrap();
    (root, path)
}

fn clean_evidence_recorded_supported_load_failure(stage: MeasurementStageV1) -> ReferenceOutcomeV1 {
    let outcome = synthetic_result(ScenarioV1::Burst, stage);
    let ReferenceOutcomeV1::Pass { mut document } = outcome else {
        unreachable!();
    };
    document.failure_reasons = vec![FailureReasonV1::SupportedLoadDegradation];
    ReferenceOutcomeV1::Failed { document }
}

#[test]
fn amended_legacy_reader_reclassifies_tolerated_supported_load_failure() {
    // Break caught: the closing reader rejects a legacy failure made empty by the amendment.
    let stored = boundary_degradation_outcome(101, false, false, true);
    assert_eq!(stored.validate(), Err(ResultError::InvalidArtifact));
    let (_root, path) = write_unvalidated_outcome(&stored);

    let read =
        read_and_validate_reference_outcome(&path, AmendedLegacyMode::AcceptAmendedLegacy).unwrap();

    assert_eq!(read.outcome.status(), ReferenceOutcomeStatusV1::Pass);
    assert!(read.outcome.failure_reasons().is_empty());
    assert_eq!(read.outcome.validate(), Ok(()));
    assert_eq!(
        read.reclassified,
        Some(ReclassificationRecordV1 {
            scenario: ScenarioV1::Burst,
            recorded_failure_reasons: vec![FailureReasonV1::SupportedLoadDegradation],
        })
    );
}

#[test]
fn amended_legacy_reader_rejects_clean_final_evidence_with_recorded_degradation() {
    // Break caught: promoting a mis-assembled failure with no tolerated degradation evidence.
    let stored = clean_evidence_recorded_supported_load_failure(MeasurementStageV1::Final);
    assert_eq!(stored.validate(), Err(ResultError::InvalidArtifact));
    let (_root, path) = write_unvalidated_outcome(&stored);

    assert!(
        read_and_validate_reference_outcome(&path, AmendedLegacyMode::AcceptAmendedLegacy).is_err()
    );
}

#[test]
fn amended_legacy_reader_rejects_clean_baseline_evidence_with_recorded_degradation() {
    // Break caught: applying the Final-only amendment to a Baseline artifact.
    let stored = clean_evidence_recorded_supported_load_failure(MeasurementStageV1::Baseline);
    assert_eq!(stored.validate(), Err(ResultError::InvalidArtifact));
    let (_root, path) = write_unvalidated_outcome(&stored);

    assert!(
        read_and_validate_reference_outcome(&path, AmendedLegacyMode::AcceptAmendedLegacy).is_err()
    );
}

#[test]
fn amended_legacy_reader_keeps_still_derived_failure() {
    // Break caught: reclassifying a 102-event sample whose failure still derives.
    let stored = boundary_degradation_outcome(102, false, false, true);
    assert_eq!(stored.validate(), Ok(()));
    let (_root, path) = write_unvalidated_outcome(&stored);

    let read =
        read_and_validate_reference_outcome(&path, AmendedLegacyMode::AcceptAmendedLegacy).unwrap();

    assert_eq!(read.outcome.status(), ReferenceOutcomeStatusV1::Failed);
    assert_eq!(
        read.outcome.failure_reasons(),
        [FailureReasonV1::SupportedLoadDegradation]
    );
    assert_eq!(read.reclassified, None);
}

#[test]
fn amended_legacy_reader_rejects_different_derived_failure_set() {
    // Break caught: treating any legacy failure-set divergence as reclassifiable.
    let mut stored = failed_outcome(
        ScenarioV1::Burst,
        MeasurementStageV1::Final,
        FailureReasonV1::ScreenLatency,
        100,
        1_000,
    );
    stored.document_mut().failure_reasons = vec![FailureReasonV1::SupportedLoadDegradation];
    let (_root, path) = write_unvalidated_outcome(&stored);

    assert!(
        read_and_validate_reference_outcome(&path, AmendedLegacyMode::AcceptAmendedLegacy,)
            .is_err()
    );
}

#[test]
fn amended_legacy_reader_off_preserves_fail_closed_behavior() {
    // Break caught: allowing ordinary readers to reclassify without explicit authority.
    let stored = boundary_degradation_outcome(101, false, false, true);
    let (_root, path) = write_unvalidated_outcome(&stored);

    assert!(read_and_validate_reference_outcome(&path, AmendedLegacyMode::Off).is_err());
}

struct AmendedLegacyEntrypointFixture {
    baseline_root: tempfile::TempDir,
    final_root: tempfile::TempDir,
}

fn amended_legacy_entrypoint_fixture(legacy_burst: bool) -> AmendedLegacyEntrypointFixture {
    let baseline_root = tempfile::tempdir().unwrap();
    let final_root = tempfile::tempdir().unwrap();
    for spec in &workload_schema().scenarios {
        let mut baseline = synthetic_result(spec.scenario, MeasurementStageV1::Baseline);
        let scenario_is_legacy_burst = legacy_burst && spec.scenario == ScenarioV1::Burst;
        let mut final_outcome = if scenario_is_legacy_burst {
            boundary_degradation_outcome(101, false, false, false)
        } else {
            synthetic_result(spec.scenario, MeasurementStageV1::Final)
        };
        let baseline_scenario_root = baseline_root.path().join(&spec.directory);
        let final_scenario_root = final_root.path().join(&spec.directory);
        write_synthetic_raw_scenario_root(&baseline_scenario_root, &mut baseline).unwrap();
        write_synthetic_raw_scenario_root(&final_scenario_root, &mut final_outcome).unwrap();
        if scenario_is_legacy_burst {
            final_outcome.document_mut().failure_reasons =
                vec![FailureReasonV1::SupportedLoadDegradation];
            final_outcome = match final_outcome {
                ReferenceOutcomeV1::Pass { document } => ReferenceOutcomeV1::Failed { document },
                _ => unreachable!(),
            };
        }
        atomic_write_reference_outcome(&baseline_scenario_root.join("result-v1.json"), &baseline)
            .unwrap();
        let mut bytes = serde_json::to_vec(&final_outcome).unwrap();
        bytes.push(b'\n');
        std::fs::write(final_scenario_root.join("result-v1.json"), bytes).unwrap();
    }
    AmendedLegacyEntrypointFixture {
        baseline_root,
        final_root,
    }
}

fn closed_entrypoint_environment(
    additional: impl IntoIterator<Item = (String, String)>,
) -> BTreeMap<String, String> {
    let mut environment = BTreeMap::from([
        ("CARGO_HOME".to_owned(), "/home/mageyuki/.cargo".to_owned()),
        ("HOME".to_owned(), "/home/mageyuki".to_owned()),
        ("LC_ALL".to_owned(), "C".to_owned()),
        ("PATH".to_owned(), "/usr/bin:/bin".to_owned()),
        (
            "RUSTUP_HOME".to_owned(),
            "/home/mageyuki/.rustup".to_owned(),
        ),
        ("TZ".to_owned(), "UTC".to_owned()),
    ]);
    environment.extend(additional);
    environment
}

fn run_closed_entrypoint(
    name: &str,
    environment: BTreeMap<String, String>,
) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([name, "--exact", "--ignored", "--test-threads=1"])
        .env_clear()
        .envs(environment)
        .output()
        .unwrap()
}

fn reclassification_sidecar_path(output: &std::path::Path) -> PathBuf {
    let mut path = output.as_os_str().to_os_string();
    path.push(".reclassification.json");
    path.into()
}

fn rederive_entrypoint_environment(
    fixture: &AmendedLegacyEntrypointFixture,
    output: &std::path::Path,
    flag: Option<&str>,
) -> BTreeMap<String, String> {
    let mut additional = vec![
        (
            "HERDR_PERF_REDERIVE_BASELINE_RESULTS_ROOT".to_owned(),
            fixture.baseline_root.path().to_string_lossy().into_owned(),
        ),
        (
            "HERDR_PERF_REDERIVE_FINAL_RESULTS_ROOT".to_owned(),
            fixture.final_root.path().to_string_lossy().into_owned(),
        ),
        (
            "HERDR_PERF_REDERIVE_OUTPUT".to_owned(),
            output.to_string_lossy().into_owned(),
        ),
    ];
    if let Some(value) = flag {
        additional.push((
            "HERDR_PERF_ACCEPT_AMENDED_LEGACY".to_owned(),
            value.to_owned(),
        ));
    }
    closed_entrypoint_environment(additional)
}

fn classify_entrypoint_environment(
    fixture: &AmendedLegacyEntrypointFixture,
    output: &std::path::Path,
    flag: Option<&str>,
) -> BTreeMap<String, String> {
    let mut additional = vec![
        (
            "HERDR_PERF_CLASSIFY_RESULTS_ROOT".to_owned(),
            fixture.final_root.path().to_string_lossy().into_owned(),
        ),
        (
            "HERDR_PERF_CLASSIFY_OUTPUT".to_owned(),
            output.to_string_lossy().into_owned(),
        ),
    ];
    if let Some(value) = flag {
        additional.push((
            "HERDR_PERF_ACCEPT_AMENDED_LEGACY".to_owned(),
            value.to_owned(),
        ));
    }
    closed_entrypoint_environment(additional)
}

fn expected_reclassification_sidecar() -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "rule": "amended_legacy_v1",
        "reclassified": [{
            "scenario": "burst",
            "recorded_failure_reasons": ["supported_load_degradation"],
        }],
    })
}

#[test]
fn amended_legacy_entrypoints_reclassify_uniformly_and_write_sidecars() {
    // Break caught: either closing consumer observes a different outcome or omits provenance.
    let fixture = amended_legacy_entrypoint_fixture(true);
    let outputs = tempfile::tempdir().unwrap();
    let report_path = outputs.path().join("section15.json");
    let checkpoint_path = outputs.path().join("checkpoint.json");

    let rederive = run_closed_entrypoint(
        "rederive_section15_report_from_results",
        rederive_entrypoint_environment(&fixture, &report_path, Some("1")),
    );
    assert_eq!(rederive.status.code(), Some(0), "{rederive:?}");
    assert!(rederive.stderr.is_empty(), "{rederive:?}");
    let classify = run_closed_entrypoint(
        "classify_d4_checkpoint_from_results",
        classify_entrypoint_environment(&fixture, &checkpoint_path, Some("1")),
    );
    assert_eq!(classify.status.code(), Some(0), "{classify:?}");
    assert!(classify.stderr.is_empty(), "{classify:?}");

    let report: Section15ReDerivationV1 =
        serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
    assert_eq!(
        report.validate_with_mode(AmendedLegacyMode::AcceptAmendedLegacy),
        Ok(())
    );
    let burst = report
        .scenarios
        .iter()
        .find(|scenario| scenario.scenario == ScenarioV1::Burst)
        .unwrap();
    assert_eq!(burst.final_status, ReferenceOutcomeStatusV1::Pass);
    assert!(burst.final_failure_reasons.is_empty());
    let checkpoint: D4CheckpointDocumentV1 =
        serde_json::from_slice(&std::fs::read(&checkpoint_path).unwrap()).unwrap();
    assert_eq!(
        checkpoint.decision,
        D4CheckpointDecisionV1::NoMissD4NotAuthorized {}
    );

    for sidecar in [
        reclassification_sidecar_path(&report_path),
        reclassification_sidecar_path(&checkpoint_path),
    ] {
        let actual: serde_json::Value =
            serde_json::from_slice(&std::fs::read(sidecar).unwrap()).unwrap();
        assert_eq!(actual, expected_reclassification_sidecar());
    }
}

fn assert_non_reclassified_entrypoint_outputs(
    report_path: &std::path::Path,
    checkpoint_path: &std::path::Path,
) {
    let report: Section15ReDerivationV1 =
        serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
    assert_eq!(
        report.validate_with_mode(AmendedLegacyMode::AcceptAmendedLegacy),
        Ok(())
    );
    assert!(report.scenarios.iter().all(|scenario| {
        scenario.final_status == ReferenceOutcomeStatusV1::Pass
            && scenario.final_failure_reasons.is_empty()
    }));
    let checkpoint: D4CheckpointDocumentV1 =
        serde_json::from_slice(&std::fs::read(checkpoint_path).unwrap()).unwrap();
    assert_eq!(
        checkpoint.decision,
        D4CheckpointDecisionV1::NoMissD4NotAuthorized {}
    );
}

#[test]
fn amended_legacy_entrypoints_remove_stale_sidecars_when_nothing_reclassifies() {
    // Break caught: preserving stale provenance after a non-reclassifying primary rewrite.
    let fixture = amended_legacy_entrypoint_fixture(false);
    let outputs = tempfile::tempdir().unwrap();
    let report_path = outputs.path().join("section15.json");
    let checkpoint_path = outputs.path().join("checkpoint.json");
    for output in [&report_path, &checkpoint_path] {
        std::fs::write(reclassification_sidecar_path(output), b"stale\n").unwrap();
    }

    let rederive = run_closed_entrypoint(
        "rederive_section15_report_from_results",
        rederive_entrypoint_environment(&fixture, &report_path, Some("1")),
    );
    assert_eq!(rederive.status.code(), Some(0), "{rederive:?}");
    assert!(rederive.stderr.is_empty(), "{rederive:?}");
    let classify = run_closed_entrypoint(
        "classify_d4_checkpoint_from_results",
        classify_entrypoint_environment(&fixture, &checkpoint_path, Some("1")),
    );
    assert_eq!(classify.status.code(), Some(0), "{classify:?}");
    assert!(classify.stderr.is_empty(), "{classify:?}");

    assert_non_reclassified_entrypoint_outputs(&report_path, &checkpoint_path);
    assert!(!reclassification_sidecar_path(&report_path).exists());
    assert!(!reclassification_sidecar_path(&checkpoint_path).exists());
}

#[test]
fn amended_legacy_entrypoints_remove_sidecars_when_output_paths_are_reused() {
    // Break caught: retaining provenance from the prior contents of a reused output path.
    let legacy_fixture = amended_legacy_entrypoint_fixture(true);
    let clean_fixture = amended_legacy_entrypoint_fixture(false);
    let outputs = tempfile::tempdir().unwrap();
    let report_path = outputs.path().join("section15.json");
    let checkpoint_path = outputs.path().join("checkpoint.json");

    let first_rederive = run_closed_entrypoint(
        "rederive_section15_report_from_results",
        rederive_entrypoint_environment(&legacy_fixture, &report_path, Some("1")),
    );
    assert_eq!(first_rederive.status.code(), Some(0), "{first_rederive:?}");
    let first_classify = run_closed_entrypoint(
        "classify_d4_checkpoint_from_results",
        classify_entrypoint_environment(&legacy_fixture, &checkpoint_path, Some("1")),
    );
    assert_eq!(first_classify.status.code(), Some(0), "{first_classify:?}");
    for output in [&report_path, &checkpoint_path] {
        let actual: serde_json::Value =
            serde_json::from_slice(&std::fs::read(reclassification_sidecar_path(output)).unwrap())
                .unwrap();
        assert_eq!(actual, expected_reclassification_sidecar());
    }

    let second_rederive = run_closed_entrypoint(
        "rederive_section15_report_from_results",
        rederive_entrypoint_environment(&clean_fixture, &report_path, Some("1")),
    );
    assert_eq!(
        second_rederive.status.code(),
        Some(0),
        "{second_rederive:?}"
    );
    assert!(second_rederive.stderr.is_empty(), "{second_rederive:?}");
    let second_classify = run_closed_entrypoint(
        "classify_d4_checkpoint_from_results",
        classify_entrypoint_environment(&clean_fixture, &checkpoint_path, Some("1")),
    );
    assert_eq!(
        second_classify.status.code(),
        Some(0),
        "{second_classify:?}"
    );
    assert!(second_classify.stderr.is_empty(), "{second_classify:?}");

    assert_non_reclassified_entrypoint_outputs(&report_path, &checkpoint_path);
    assert!(!reclassification_sidecar_path(&report_path).exists());
    assert!(!reclassification_sidecar_path(&checkpoint_path).exists());
}

#[test]
fn amended_legacy_entrypoints_reject_non_one_flag() {
    // Break caught: treating a present non-1 flag as disabled instead of rejecting the process.
    let fixture = amended_legacy_entrypoint_fixture(true);
    let outputs = tempfile::tempdir().unwrap();
    let report_path = outputs.path().join("invalid-flag-section15.json");
    let checkpoint_path = outputs.path().join("invalid-flag-checkpoint.json");
    let rederive = run_closed_entrypoint(
        "rederive_section15_report_from_results",
        rederive_entrypoint_environment(&fixture, &report_path, Some("0")),
    );
    let classify = run_closed_entrypoint(
        "classify_d4_checkpoint_from_results",
        classify_entrypoint_environment(&fixture, &checkpoint_path, Some("0")),
    );
    for (result, output) in [(rederive, report_path), (classify, checkpoint_path)] {
        assert_eq!(result.status.code(), Some(20), "{result:?}");
        assert!(
            String::from_utf8_lossy(&result.stdout)
                .contains("entrypoint error: Invalid(\"optional environment value was invalid\")"),
            "{result:?}"
        );
        assert!(result.stderr.is_empty(), "{result:?}");
        assert!(!output.exists());
        assert!(!reclassification_sidecar_path(&output).exists());
    }
}

#[test]
fn amended_legacy_entrypoints_without_flag_preserve_fail_closed_behavior() {
    // Break caught: implicitly enabling reclassification when the optional flag is absent.
    let fixture = amended_legacy_entrypoint_fixture(true);
    let outputs = tempfile::tempdir().unwrap();
    let report_path = outputs.path().join("off-section15.json");
    let checkpoint_path = outputs.path().join("off-checkpoint.json");
    let rederive = run_closed_entrypoint(
        "rederive_section15_report_from_results",
        rederive_entrypoint_environment(&fixture, &report_path, None),
    );
    let classify = run_closed_entrypoint(
        "classify_d4_checkpoint_from_results",
        classify_entrypoint_environment(&fixture, &checkpoint_path, None),
    );
    for (result, output) in [(rederive, report_path), (classify, checkpoint_path)] {
        assert_eq!(result.status.code(), Some(20), "{result:?}");
        assert!(
            String::from_utf8_lossy(&result.stdout)
                .contains("entrypoint error: Invalid(\"stored outcome failed validation\")"),
            "{result:?}"
        );
        assert!(result.stderr.is_empty(), "{result:?}");
        assert!(!output.exists());
        assert!(!reclassification_sidecar_path(&output).exists());
    }
}

#[test]
fn artifact_time_semantics_startup_generality_and_overflow_fail_closed() {
    let mut close_after_sample = valid_twice_target_result();
    let stream = close_after_sample.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap();
    stream.frames.last_mut().unwrap().rendered_at_ns += 1;
    stream.workload_close_ns += 1;
    assert_eq!(
        stream.workload_close_ns,
        stream.frames.last().unwrap().rendered_at_ns
    );
    assert!(close_after_sample.validate().is_ok());

    let mut arbitrary_startup_pass = valid_startup_result();
    arbitrary_startup_pass.document_mut().trials[0]
        .raw
        .startup_observations_ns[0] = 2_345_678_901;
    arbitrary_startup_pass.document_mut().trials[0].startup_ns = Some(2_345_678_901);
    assert!(arbitrary_startup_pass.validate().is_ok());

    let mut arbitrary_startup_failure = failed_outcome(
        ScenarioV1::Startup,
        MeasurementStageV1::Baseline,
        FailureReasonV1::StartupLatency,
        100,
        1_000,
    );
    arbitrary_startup_failure.document_mut().trials[0]
        .raw
        .startup_observations_ns[0] = 3_456_789_012;
    arbitrary_startup_failure.document_mut().trials[0].startup_ns = Some(3_456_789_012);
    assert!(arbitrary_startup_failure.validate().is_ok());

    let mut observer_after_priming = valid_target_input_result();
    let priming = observer_after_priming.document().trials[0]
        .raw
        .priming_frame_recorded_ns
        .unwrap();
    let ready = priming + 1;
    let trial = &mut observer_after_priming.document_mut().trials[0];
    trial.observer_control.observer_ready_ns = ready;
    trial.process_tree.observer_ready_ns = ready;
    trial.observer_control.frames[0] = ObserverControlFrameV1::Ready {
        observer_ready_ns: ready,
    };
    assert_eq!(
        observer_after_priming.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut overflowing_input_schedule = valid_target_input_result();
    let first = &mut overflowing_input_schedule.document_mut().trials[0]
        .raw
        .input_observations[0];
    first.rendered_ns = u64::MAX - 50_000_000;
    first.observed_frame_phase_ns = (first.rendered_ns - first.injected_ns) % 100_000_000;
    let overflow_result = std::panic::catch_unwind(|| overflowing_input_schedule.validate());
    assert!(matches!(
        overflow_result,
        Ok(Err(ResultError::InvalidArtifact))
    ));
}

#[test]
fn performance_evidence_stream_is_omission_closed_and_rederived() {
    let valid = valid_twice_target_result();
    assert!(valid.validate().is_ok());
    let mut deleted = valid_twice_target_result();
    deleted.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .samples
        .remove(0);
    assert_eq!(deleted.validate(), Err(ResultError::InvalidArtifact));
    let mut terminal = valid_twice_target_result();
    terminal.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .terminal_observations
        .pop();
    assert_eq!(terminal.validate(), Err(ResultError::InvalidArtifact));

    let mut frame_suffix = valid_twice_target_result();
    frame_suffix.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .frames
        .pop();
    assert_eq!(frame_suffix.validate(), Err(ResultError::InvalidArtifact));

    let mut ordinal_gap = valid_twice_target_result();
    ordinal_gap.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .samples[1]
        .sample_ordinal += 1;
    assert_eq!(ordinal_gap.validate(), Err(ResultError::InvalidArtifact));

    let mut shifted_start = valid_twice_target_result();
    shifted_start.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .workload_start_ns += 1;
    assert_eq!(shifted_start.validate(), Err(ResultError::InvalidArtifact));

    let mut incoherent_reference = valid_twice_target_result();
    incoherent_reference.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .frames[1]
        .sample_ordinal = 1;
    assert_eq!(
        incoherent_reference.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut truncated_close = valid_twice_target_result();
    truncated_close.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .workload_close_ns -= 1;
    assert_eq!(
        truncated_close.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut coherent_but_untrue_topology = valid_twice_target_result();
    let stream = coherent_but_untrue_topology.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap();
    stream.samples[1].live_panes = 51;
    stream.samples[1].reasons = vec![
        PerformanceReasonV1::LivePanes,
        PerformanceReasonV1::EventsSixtySeconds,
    ];
    stream.frames[1].reasons = stream.samples[1].reasons.clone();
    stream.frames[1].rendered_header_line = "DEGRADED | perf:live_panes,events_60s".to_owned();
    assert_eq!(
        coherent_but_untrue_topology.validate(),
        Err(ResultError::InvalidArtifact)
    );
}

#[test]
fn performance_stream_accepts_latched_recovered_event_lag() {
    let mut latched = failed_outcome(
        ScenarioV1::Sustained,
        MeasurementStageV1::Final,
        FailureReasonV1::SupportedLoadDegradation,
        100,
        1_000,
    );
    let stream = latched.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap();
    let origin = stream.workload_start_ns;
    stream.terminal_observations[0].terminal_ns = origin + 2_100_000_000;
    stream.terminal_observations[39].terminal_ns = origin + 2_300_000_000;

    let mut closing_sample = stream.samples[2].clone();
    closing_sample.sample_ordinal = 4;
    let mut recovered = closing_sample.clone();
    recovered.sample_ordinal = 3;
    recovered.sampled_at_ns = origin + 2_200_000_000;
    recovered.event_lag_ns = 200_000_000;
    recovered.pending_events = 2;
    recovered.admission_high_water = 44;
    recovered.completion_high_water = 43;
    recovered.events_one_second = 20;
    recovered.events_ten_seconds = 44;
    recovered.events_sixty_seconds = 44;
    recovered.reasons = vec![PerformanceReasonV1::EventLag];
    recovered.effective_quality = EffectiveQualityV1::Degraded;

    let mut closing_frame = stream.frames[2].clone();
    closing_frame.draw_ordinal = 4;
    closing_frame.sample_ordinal = 4;
    let mut recovered_frame = closing_frame.clone();
    recovered_frame.draw_ordinal = 3;
    recovered_frame.sample_ordinal = 3;
    recovered_frame.state_observed_at_ns = recovered.sampled_at_ns;
    recovered_frame.rendered_at_ns = origin + 2_300_000_000;
    recovered_frame.reasons = recovered.reasons.clone();
    recovered_frame.effective_quality = recovered.effective_quality;
    recovered_frame.rendered_header_line = "DEGRADED | perf:event_lag".to_owned();

    stream.samples = vec![
        stream.samples[0].clone(),
        stream.samples[1].clone(),
        recovered,
        closing_sample,
    ];
    stream.frames = vec![
        stream.frames[0].clone(),
        stream.frames[1].clone(),
        recovered_frame,
        closing_frame,
    ];
    stream.next_sample_ordinal = 5;
    stream.next_draw_ordinal = 5;

    let breach_high_water = stream.samples[1].admission_high_water;
    let recovered = &stream.samples[2];
    assert!(recovered.event_lag_ns <= 1_000_000_000);
    assert!(stream.terminal_observations.iter().any(|terminal| {
        terminal.sequence <= breach_high_water && terminal.terminal_ns > recovered.sampled_at_ns
    }));
    assert_eq!(recovered.reasons, vec![PerformanceReasonV1::EventLag]);

    assert!(latched.validate().is_ok());

    let mut missing_breach = failed_outcome(
        ScenarioV1::Sustained,
        MeasurementStageV1::Final,
        FailureReasonV1::SupportedLoadDegradation,
        100,
        1_000,
    );
    let stream = missing_breach.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap();
    let breached = stream
        .samples
        .iter_mut()
        .find(|sample| sample.event_lag_ns > 1_000_000_000)
        .unwrap();
    let breached_ordinal = breached.sample_ordinal;
    breached.reasons.clear();
    breached.effective_quality = EffectiveQualityV1::Live;
    for frame in stream
        .frames
        .iter_mut()
        .filter(|frame| frame.sample_ordinal == breached_ordinal)
    {
        frame.reasons.clear();
        frame.effective_quality = EffectiveQualityV1::Live;
        frame.rendered_header_line = "LIVE | perf:".to_owned();
    }
    assert_eq!(missing_breach.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn performance_stream_allows_exactly_one_pre_origin_sample() {
    let one_pre_origin = valid_final_sustained_result();
    let one_stream = one_pre_origin.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap();
    assert_eq!(
        one_stream
            .samples
            .iter()
            .filter(|sample| sample.sampled_at_ns < one_stream.workload_start_ns)
            .count(),
        1
    );
    assert!(one_pre_origin.validate().is_ok());

    let mut two_pre_origin = valid_final_sustained_result();
    let stream = two_pre_origin.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap();
    let mut second_pre_origin = stream.samples[0].clone();
    second_pre_origin.sample_ordinal += 1;
    for sample in stream.samples.iter_mut().skip(1) {
        sample.sample_ordinal += 1;
    }
    for frame in &mut stream.frames {
        if frame.sample_ordinal > stream.first_sample_ordinal {
            frame.sample_ordinal += 1;
        }
    }
    stream.samples.insert(1, second_pre_origin);
    stream.next_sample_ordinal += 1;
    assert_eq!(
        stream
            .samples
            .iter()
            .filter(|sample| sample.sampled_at_ns < stream.workload_start_ns)
            .count(),
        2
    );
    assert_eq!(two_pre_origin.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn twice_target_requires_the_earliest_rendered_events_sixty_seconds_reason() {
    let valid = valid_twice_target_result();
    assert_eq!(
        valid.document().measurement_stage,
        MeasurementStageV1::Final
    );
    assert!(valid.validate().is_ok());
    let mut wrong_reason = valid_twice_target_result();
    wrong_reason.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .frames[1]
        .reasons = vec![PerformanceReasonV1::DependencyEdges];
    assert_eq!(wrong_reason.validate(), Err(ResultError::InvalidArtifact));
    let missing = failed_outcome(
        ScenarioV1::TwiceTarget,
        MeasurementStageV1::Final,
        FailureReasonV1::MissingDegradation,
        100,
        1_000,
    );
    assert_eq!(
        missing.document().failure_reasons,
        vec![FailureReasonV1::MissingDegradation]
    );
    assert!(missing.validate().is_ok());
    assert!(
        synthetic_result(ScenarioV1::TwiceTarget, MeasurementStageV1::PostReliability)
            .validate()
            .is_ok()
    );
}

#[test]
fn twice_target_crossing_frame_is_actual_and_before_deadline() {
    let valid = valid_twice_target_result();
    let stream = valid.document().trials[0]
        .raw
        .performance_evidence_stream
        .as_ref()
        .unwrap();
    let selected = stream
        .frames
        .iter()
        .find(|frame| Some(frame.draw_ordinal) == stream.selected_terminal_draw_ordinal)
        .unwrap();
    assert_eq!(
        selected.rendered_at_ns - selected.state_observed_at_ns,
        107_000_000
    );
    assert!(selected.rendered_at_ns <= stream.workload_start_ns + 60_000_000_000);
    assert!(valid.validate().is_ok());
    let mut late = valid_twice_target_result();
    let late_stream = late.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap();
    late_stream.frames[1].rendered_at_ns = late_stream.workload_start_ns + 60_000_000_001;
    assert_eq!(late.validate(), Err(ResultError::InvalidArtifact));

    let mut one_interval = valid_twice_target_result();
    let frame = &mut one_interval.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .frames[1];
    frame.rendered_at_ns = frame.state_observed_at_ns + 200_000_000;
    assert!(one_interval.validate().is_ok());

    let mut beyond_one_interval = valid_twice_target_result();
    let frame = &mut beyond_one_interval.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .frames[1];
    frame.rendered_at_ns = frame.state_observed_at_ns + 200_000_001;
    assert!(beyond_one_interval.validate().is_ok());

    let mut later_selection = valid_twice_target_result();
    later_selection.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .selected_terminal_draw_ordinal = Some(3);
    assert_eq!(
        later_selection.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut false_absence = valid_twice_target_result();
    false_absence.document_mut().trials[0]
        .raw
        .performance_evidence_stream
        .as_mut()
        .unwrap()
        .selected_terminal_draw_ordinal = None;
    assert_eq!(false_absence.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn tagged_outcomes_distinguish_failed_from_invalid() {
    assert!(valid_failed_outcome().validate().is_ok());
    assert!(
        valid_invalid_outcome(FailureReasonV1::InvalidArtifact)
            .validate()
            .is_ok()
    );
    assert!(
        valid_invalid_outcome(FailureReasonV1::SequenceLoss)
            .validate()
            .is_ok()
    );
    assert!(
        valid_invalid_outcome(FailureReasonV1::StructuralLoss)
            .validate()
            .is_ok()
    );
    assert_eq!(
        valid_invalid_outcome(FailureReasonV1::ScreenLatency).validate(),
        Err(ResultError::InvalidArtifact)
    );
}

#[test]
fn toolchain_provenance_has_one_controls_owner_and_one_launcher_inventory_entry() {
    let valid = valid_synthetic_result();
    let encoded = serde_json::to_value(&valid).unwrap();
    assert!(encoded.to_string().matches("rustc_version").count() == 1);
    assert!(encoded.to_string().matches("cargo_version").count() == 1);
    let controls = &valid.document().controls;
    assert_eq!(
        controls
            .authoritative_executables
            .iter()
            .filter(|identity| **identity == controls.toolchain_launcher)
            .count(),
        1
    );
    assert!(valid.document().trials.iter().all(|trial| {
        trial.control_evidence.revalidated_runner_script == controls.runner_script
    }));
    let mut changed = valid_synthetic_result();
    changed
        .document_mut()
        .controls
        .rustc_version
        .push_str(" changed");
    assert_eq!(changed.validate(), Err(ResultError::InvalidArtifact));

    let mut coordinated_digest_mutation = valid_synthetic_result();
    let document = coordinated_digest_mutation.document_mut();
    document.controls.authoritative_executables[1].sha256 = "f".repeat(64);
    for trial in &mut document.trials {
        trial.control_evidence.revalidated_executables[1].sha256 = "f".repeat(64);
    }
    assert_eq!(
        coordinated_digest_mutation.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let requested = controls
        .authoritative_executables
        .iter()
        .map(|identity| identity.requested_path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        requested,
        vec![
            "/home/mageyuki/.cargo/bin/rustup",
            "/usr/bin/awk",
            "/usr/bin/bash",
            "/usr/bin/env",
            "/usr/bin/findmnt",
            "/usr/bin/git",
            "/usr/bin/id",
            "/usr/bin/jq",
            "/usr/bin/lsblk",
            "/usr/bin/lscpu",
            "/usr/bin/mkdir",
            "/usr/bin/mktemp",
            "/usr/bin/mv",
            "/usr/bin/pidstat",
            "/usr/bin/prlimit",
            "/usr/bin/readlink",
            "/usr/bin/rg",
            "/usr/bin/rmdir",
            "/usr/bin/setsid",
            "/usr/bin/sha256sum",
            "/usr/bin/sleep",
            "/usr/bin/stat",
            "/usr/bin/taskset",
            "/usr/bin/time",
            "/usr/bin/uname",
            "/usr/bin/unlink",
        ]
    );
}

#[test]
fn failure_policy_table_is_closed_exhaustive_and_shared() {
    let all_stages = serde_json::json!(["baseline", "post_reliability", "final"]);
    let all_scenarios = serde_json::json!([
        "target",
        "sustained",
        "burst",
        "startup",
        "idle",
        "fallback_rescan",
        "twice_target"
    ]);
    let row = |stages: serde_json::Value,
               scenarios: serde_json::Value,
               failure_reason: &str,
               outcome: &str,
               d4_policy: &str| {
        serde_json::json!({
            "stages": stages,
            "scenarios": scenarios,
            "failure_reason": failure_reason,
            "outcome": outcome,
            "d4_policy": d4_policy,
        })
    };
    let expected = vec![
        row(
            all_stages.clone(),
            all_scenarios.clone(),
            "control_mismatch",
            "invalid",
            "not_applicable",
        ),
        row(
            all_stages.clone(),
            all_scenarios.clone(),
            "command_failed",
            "invalid",
            "not_applicable",
        ),
        row(
            all_stages.clone(),
            all_scenarios.clone(),
            "incomplete_trial",
            "invalid",
            "not_applicable",
        ),
        row(
            all_stages.clone(),
            all_scenarios.clone(),
            "duplicate_outcome",
            "invalid",
            "not_applicable",
        ),
        row(
            all_stages.clone(),
            all_scenarios.clone(),
            "invalid_artifact",
            "invalid",
            "not_applicable",
        ),
        row(
            all_stages.clone(),
            all_scenarios.clone(),
            "structural_loss",
            "invalid",
            "not_applicable",
        ),
        row(
            all_stages.clone(),
            serde_json::json!(["sustained", "burst", "fallback_rescan", "twice_target"]),
            "sequence_loss",
            "invalid",
            "not_applicable",
        ),
        row(
            all_stages.clone(),
            serde_json::json!(["target"]),
            "input_latency",
            "failed",
            "non_d4",
        ),
        row(
            all_stages.clone(),
            serde_json::json!(["sustained", "burst"]),
            "screen_latency",
            "failed",
            "d4_scoped",
        ),
        row(
            all_stages.clone(),
            serde_json::json!(["startup"]),
            "startup_latency",
            "failed",
            "d4_scoped",
        ),
        row(
            all_stages.clone(),
            serde_json::json!(["fallback_rescan"]),
            "fallback_rescan_latency",
            "failed",
            "d4_scoped",
        ),
        row(
            all_stages.clone(),
            serde_json::json!(["idle"]),
            "idle_cpu",
            "failed",
            "non_d4",
        ),
        row(
            all_stages.clone(),
            serde_json::json!(["target", "idle", "twice_target"]),
            "maximum_rss",
            "failed",
            "non_d4",
        ),
        row(
            all_stages.clone(),
            serde_json::json!(["sustained", "burst", "fallback_rescan"]),
            "maximum_rss",
            "failed",
            "d4_scoped",
        ),
        row(
            all_stages.clone(),
            serde_json::json!(["sustained", "burst"]),
            "workload_admission",
            "failed",
            "d4_scoped",
        ),
        row(
            all_stages,
            serde_json::json!(["twice_target"]),
            "workload_admission",
            "failed",
            "non_d4",
        ),
        row(
            serde_json::json!(["final"]),
            serde_json::json!(["sustained", "burst"]),
            "supported_load_degradation",
            "failed",
            "non_d4",
        ),
        row(
            serde_json::json!(["final"]),
            serde_json::json!(["twice_target"]),
            "missing_degradation",
            "failed",
            "non_d4",
        ),
    ];
    assert_eq!(
        serde_json::to_value(&manifest().failure_policy).unwrap(),
        serde_json::Value::Array(expected)
    );
    assert_eq!(manifest().failure_policy.len(), 18);
    assert_eq!(expanded_failure_policy_tuples().len(), 186);
    assert_eq!(
        lookup_failure_policy(
            MeasurementStageV1::Baseline,
            ScenarioV1::Target,
            FailureReasonV1::ControlMismatch,
        ),
        Some(D4PolicyV1::NotApplicable)
    );
    assert_eq!(
        lookup_failure_policy(
            MeasurementStageV1::Final,
            ScenarioV1::Sustained,
            FailureReasonV1::WorkloadAdmission,
        ),
        Some(D4PolicyV1::D4Scoped)
    );
    assert_eq!(
        lookup_failure_policy(
            MeasurementStageV1::Final,
            ScenarioV1::Sustained,
            FailureReasonV1::SupportedLoadDegradation,
        ),
        Some(D4PolicyV1::NonD4)
    );
    assert_eq!(
        lookup_failure_policy(
            MeasurementStageV1::Final,
            ScenarioV1::TwiceTarget,
            FailureReasonV1::MissingDegradation,
        ),
        Some(D4PolicyV1::NonD4)
    );
    assert!(valid_failed_outcome().validate().is_ok());
    assert_eq!(
        lookup_failure_policy(
            MeasurementStageV1::Baseline,
            ScenarioV1::Target,
            FailureReasonV1::ScreenLatency,
        ),
        None
    );
}

fn amendments(values: impl IntoIterator<Item = RequiredAmendmentV1>) -> D4CheckpointDecisionV1 {
    D4CheckpointDecisionV1::AmendmentsRequired {
        amendments: values
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

fn all_passes() -> Vec<ReferenceOutcomeV1> {
    [
        ScenarioV1::Target,
        ScenarioV1::Sustained,
        ScenarioV1::Burst,
        ScenarioV1::Startup,
        ScenarioV1::Idle,
        ScenarioV1::FallbackRescan,
        ScenarioV1::TwiceTarget,
    ]
    .into_iter()
    .map(|scenario| synthetic_result(scenario, MeasurementStageV1::Final))
    .collect()
}

fn one_failure(
    scenario: ScenarioV1,
    reason: FailureReasonV1,
    numerator: u64,
    denominator: u64,
) -> Vec<ReferenceOutcomeV1> {
    let mut outcomes = all_passes();
    let index = outcomes
        .iter()
        .position(|outcome| outcome.document().scenario == scenario)
        .unwrap();
    outcomes[index] = failed_outcome(
        scenario,
        MeasurementStageV1::Final,
        reason,
        numerator,
        denominator,
    );
    outcomes
}

#[test]
fn d4_checkpoint_preserves_low_high_mixed_and_invalid_cases() {
    let no_misses = all_passes();
    assert_eq!(
        classify_d4_checkpoint(&no_misses),
        Ok(D4CheckpointDecisionV1::NoMissD4NotAuthorized {})
    );
    let low = one_failure(
        ScenarioV1::Sustained,
        FailureReasonV1::ScreenLatency,
        249,
        1_000,
    );
    assert_eq!(
        classify_d4_checkpoint(&low),
        Ok(amendments([RequiredAmendmentV1::NonD4]))
    );
    let high = one_failure(
        ScenarioV1::Sustained,
        FailureReasonV1::ScreenLatency,
        250,
        1_000,
    );
    assert_eq!(
        classify_d4_checkpoint(&high),
        Ok(amendments([RequiredAmendmentV1::D4]))
    );
    let missing = one_failure(
        ScenarioV1::TwiceTarget,
        FailureReasonV1::MissingDegradation,
        100,
        1_000,
    );
    assert_eq!(
        classify_d4_checkpoint(&missing),
        Ok(amendments([RequiredAmendmentV1::NonD4]))
    );
    let admission = one_failure(
        ScenarioV1::TwiceTarget,
        FailureReasonV1::WorkloadAdmission,
        1_000,
        1_000,
    );
    assert_eq!(
        classify_d4_checkpoint(&admission),
        Ok(amendments([RequiredAmendmentV1::NonD4]))
    );
    let supported = one_failure(
        ScenarioV1::Sustained,
        FailureReasonV1::SupportedLoadDegradation,
        1_000,
        1_000,
    );
    assert_eq!(
        classify_d4_checkpoint(&supported),
        Ok(amendments([RequiredAmendmentV1::NonD4]))
    );
    let startup = one_failure(
        ScenarioV1::Startup,
        FailureReasonV1::StartupLatency,
        250,
        1_000,
    );
    assert_eq!(
        classify_d4_checkpoint(&startup),
        Ok(amendments([RequiredAmendmentV1::D4]))
    );
    let fallback = one_failure(
        ScenarioV1::FallbackRescan,
        FailureReasonV1::FallbackRescanLatency,
        249,
        1_000,
    );
    assert_eq!(
        classify_d4_checkpoint(&fallback),
        Ok(amendments([RequiredAmendmentV1::NonD4]))
    );
    let mut mixed = one_failure(
        ScenarioV1::Sustained,
        FailureReasonV1::ScreenLatency,
        250,
        1_000,
    );
    mixed[6] = failed_outcome(
        ScenarioV1::TwiceTarget,
        MeasurementStageV1::Final,
        FailureReasonV1::MissingDegradation,
        1_000,
        1_000,
    );
    assert_eq!(
        classify_d4_checkpoint(&mixed),
        Ok(amendments([
            RequiredAmendmentV1::D4,
            RequiredAmendmentV1::NonD4,
        ]))
    );
    let invalid = vec![valid_invalid_outcome(FailureReasonV1::InvalidArtifact)];
    assert_eq!(
        classify_d4_checkpoint(&invalid),
        Err(ResultError::InvalidArtifact)
    );

    let mut missing_sequence = fallback;
    missing_sequence[5].document_mut().trials[0]
        .raw
        .scoped_observations
        .pop();
    assert_eq!(
        classify_d4_checkpoint(&missing_sequence),
        Err(ResultError::InvalidArtifact)
    );
    let mut zero_denominator = low;
    for trial in &mut zero_denominator[1].document_mut().trials {
        for observation in &mut trial.raw.scoped_observations {
            observation.reducer_plus_publish_ns = 0;
        }
        trial.reducer_plus_publish_ns = Some(0);
        trial.d4_ratio_parts_per_million = None;
    }
    assert_eq!(
        classify_d4_checkpoint(&zero_denominator),
        Err(ResultError::InvalidArtifact)
    );
    let mut inconsistent_sum = high;
    inconsistent_sum[1].document_mut().trials[0].d4_analysis_ns = Some(u64::MAX);
    assert_eq!(
        classify_d4_checkpoint(&inconsistent_sum),
        Err(ResultError::InvalidArtifact)
    );

    let mut mixed_harness = all_passes();
    mixed_harness[1].document_mut().harness_sha = "c".repeat(40);
    assert!(mixed_harness[1].validate().is_ok());
    assert_eq!(
        classify_d4_checkpoint(&mixed_harness),
        Err(ResultError::InvalidArtifact)
    );
}

#[test]
fn d4_checkpoint_wire_schema_is_closed_versioned_and_nonempty() {
    let mixed = D4CheckpointDocumentV1 {
        schema_version: 1,
        decision: amendments([RequiredAmendmentV1::D4, RequiredAmendmentV1::NonD4]),
    };
    assert_eq!(
        serde_json::to_value(&mixed).unwrap(),
        serde_json::json!({
            "schema_version": 1,
            "decision": {
                "kind": "amendments_required",
                "amendments": ["d4", "non_d4"]
            }
        })
    );
    assert!(mixed.validate().is_ok());
    for malformed in [
        serde_json::json!({"decision": {"kind": "no_miss_d4_not_authorized"}}),
        serde_json::json!({"schema_version": 1,
            "decision": {"kind": "amendments_required", "amendments": ["d5"]}}),
        serde_json::json!({"schema_version": 1,
            "decision": {"kind": "amendments_required", "amendments": null}}),
        serde_json::json!({"schema_version": 1,
            "decision": {"kind": "no_miss_d4_not_authorized", "amendments": null}}),
        serde_json::json!({"schema_version": 1,
            "decision": {"kind": "no_miss_d4_not_authorized"}, "extra": true}),
    ] {
        assert!(serde_json::from_value::<D4CheckpointDocumentV1>(malformed).is_err());
    }
    assert_eq!(
        D4CheckpointDocumentV1 {
            schema_version: 2,
            decision: D4CheckpointDecisionV1::NoMissD4NotAuthorized {},
        }
        .validate(),
        Err(ResultError::InvalidArtifact)
    );
    for amendments in [
        vec![],
        vec![RequiredAmendmentV1::D4, RequiredAmendmentV1::D4],
        vec![RequiredAmendmentV1::NonD4, RequiredAmendmentV1::D4],
    ] {
        assert_eq!(
            D4CheckpointDocumentV1 {
                schema_version: 1,
                decision: D4CheckpointDecisionV1::AmendmentsRequired { amendments },
            }
            .validate(),
            Err(ResultError::InvalidArtifact)
        );
    }
}

struct Section15Fixture {
    report: Section15ReDerivationV1,
    baseline_root: tempfile::TempDir,
    _final_root: tempfile::TempDir,
}

fn section15_fixture_from_final(final_results: &[ReferenceOutcomeV1]) -> Section15Fixture {
    let baseline_root = tempfile::tempdir().unwrap();
    let final_root = tempfile::tempdir().unwrap();
    let scenarios = [
        ScenarioV1::Target,
        ScenarioV1::Sustained,
        ScenarioV1::Burst,
        ScenarioV1::Startup,
        ScenarioV1::Idle,
        ScenarioV1::FallbackRescan,
        ScenarioV1::TwiceTarget,
    ];
    let mut baseline = scenarios
        .into_iter()
        .map(|scenario| synthetic_result(scenario, MeasurementStageV1::Baseline))
        .collect::<Vec<_>>();
    let mut final_results = final_results.iter().map(clone_outcome).collect::<Vec<_>>();
    for ((spec, baseline_outcome), final_outcome) in workload_schema()
        .scenarios
        .iter()
        .zip(&mut baseline)
        .zip(&mut final_results)
    {
        let baseline_directory = baseline_root.path().join(&spec.directory);
        let final_directory = final_root.path().join(&spec.directory);
        write_synthetic_raw_scenario_root(&baseline_directory, baseline_outcome).unwrap();
        write_synthetic_raw_scenario_root(&final_directory, final_outcome).unwrap();
        atomic_write_reference_outcome(
            &baseline_directory.join("result-v1.json"),
            baseline_outcome,
        )
        .unwrap();
        atomic_write_reference_outcome(&final_directory.join("result-v1.json"), final_outcome)
            .unwrap();
    }
    let report = rederive_section15_document_for_test(
        baseline_root.path(),
        final_root.path(),
        &baseline,
        &final_results,
    )
    .unwrap();
    assert!(report.validate().is_ok());
    Section15Fixture {
        report,
        baseline_root,
        _final_root: final_root,
    }
}

fn section15_d4_fixture() -> Section15Fixture {
    section15_fixture_from_final(&one_failure(
        ScenarioV1::Sustained,
        FailureReasonV1::ScreenLatency,
        250,
        1_000,
    ))
}

fn section15_mixed_policy_fixture() -> Section15Fixture {
    let mut outcomes = one_failure(
        ScenarioV1::Sustained,
        FailureReasonV1::ScreenLatency,
        250,
        1_000,
    );
    outcomes[6] = failed_outcome(
        ScenarioV1::TwiceTarget,
        MeasurementStageV1::Final,
        FailureReasonV1::MissingDegradation,
        1_000,
        1_000,
    );
    section15_fixture_from_final(&outcomes)
}

fn section15_field_mutation(
    report: &Section15ReDerivationV1,
    label: &'static str,
    pointer: &str,
    replacement: serde_json::Value,
) -> (&'static str, serde_json::Value) {
    let mut value = serde_json::to_value(report).unwrap();
    *value.pointer_mut(pointer).unwrap() = replacement;
    (label, value)
}

fn assert_invalid_section15_value(label: &str, value: serde_json::Value) {
    if let Ok(report) = serde_json::from_value::<Section15ReDerivationV1>(value) {
        assert_eq!(
            report.validate(),
            Err(ResultError::InvalidArtifact),
            "mutation {label} was accepted"
        );
    }
}

fn section15_mutate_every_schema_field(
    report: &Section15ReDerivationV1,
    d4_report: &Section15ReDerivationV1,
) -> Vec<(&'static str, serde_json::Value)> {
    let mut cases = vec![
        section15_field_mutation(report, "schema_version", "/schema_version", 2.into()),
        section15_field_mutation(report, "subject_sha", "/subject_sha", "not-a-sha".into()),
        section15_field_mutation(report, "baseline_id", "/baseline_id", "wrong".into()),
        section15_field_mutation(
            report,
            "selected_results[].measurement_stage",
            "/selected_results/0/measurement_stage",
            "final".into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].scenario",
            "/selected_results/0/scenario",
            "burst".into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].canonical_result_path",
            "/selected_results/0/canonical_result_path",
            "/tmp/not-result.json".into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].canonical_raw_root",
            "/selected_results/0/canonical_raw_root",
            "/tmp/../aliased".into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].result_sha256",
            "/selected_results/0/result_sha256",
            "wrong".into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].production_subject_sha",
            "/selected_results/0/production_subject_sha",
            "c".repeat(40).into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].harness_sha",
            "/selected_results/0/harness_sha",
            "c".repeat(40).into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].workload_schema_sha256",
            "/selected_results/0/workload_schema_sha256",
            "c".repeat(64).into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].baseline_id",
            "/selected_results/0/baseline_id",
            "wrong".into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].measured_binary.requested_path",
            "/selected_results/0/measured_binary/requested_path",
            "relative".into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].measured_binary.canonical_path",
            "/selected_results/0/measured_binary/canonical_path",
            "/tmp/../binary".into(),
        ),
        section15_field_mutation(
            report,
            "selected_results[].measured_binary.sha256",
            "/selected_results/0/measured_binary/sha256",
            "wrong".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].scenario",
            "/scenarios/0/scenario",
            "burst".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].baseline_status",
            "/scenarios/0/baseline_status",
            "invalid".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].final_status",
            "/scenarios/0/final_status",
            "invalid".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].final_failure_reasons[]",
            "/scenarios/0/final_failure_reasons",
            serde_json::json!(["input_latency"]),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].trial_index",
            "/scenarios/0/trials/0/trial_index",
            0.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].sequence_counts.submitted",
            "/scenarios/0/trials/0/sequence_counts/submitted",
            1.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].sequence_counts.admitted",
            "/scenarios/0/trials/0/sequence_counts/admitted",
            1.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].sequence_counts.completed",
            "/scenarios/0/trials/0/sequence_counts/completed",
            1.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].sequence_counts.persisted",
            "/scenarios/0/trials/0/sequence_counts/persisted",
            1.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].sequence_counts.rendered_probes",
            "/scenarios/0/trials/0/sequence_counts/rendered_probes",
            1.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].admission_buckets_attained",
            "/scenarios/0/trials/0/admission_buckets_attained",
            false.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].lossless",
            "/scenarios/0/trials/0/lossless",
            false.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].structural_identities_match",
            "/scenarios/0/trials/0/structural_identities_match",
            false.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].distributions[].metric",
            "/scenarios/0/trials/0/distributions/0/metric",
            "startup".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].distributions[].unit",
            "/scenarios/0/trials/0/distributions/0/unit",
            "bytes".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].distributions[].sample_count",
            "/scenarios/0/trials/0/distributions/0/sample_count",
            1.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].distributions[].minimum",
            "/scenarios/0/trials/0/distributions/0/minimum",
            "01".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].distributions[].median",
            "/scenarios/0/trials/0/distributions/0/median",
            "01".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].distributions[].p95",
            "/scenarios/0/trials/0/distributions/0/p95",
            "01".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].distributions[].p99",
            "/scenarios/0/trials/0/distributions/0/p99",
            "01".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].distributions[].maximum",
            "/scenarios/0/trials/0/distributions/0/maximum",
            "01".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].predicates[].metric",
            "/scenarios/0/trials/0/predicates/0/metric",
            "startup".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].predicates[].unit",
            "/scenarios/0/trials/0/predicates/0/unit",
            "bytes".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].predicates[].ordinal",
            "/scenarios/0/trials/0/predicates/0/ordinal",
            1.into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].predicates[].observed_numerator",
            "/scenarios/0/trials/0/predicates/0/observed_numerator",
            "01".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].predicates[].observed_denominator",
            "/scenarios/0/trials/0/predicates/0/observed_denominator",
            "0".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].predicates[].comparison",
            "/scenarios/0/trials/0/predicates/0/comparison",
            "equal".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].predicates[].threshold_numerator",
            "/scenarios/0/trials/0/predicates/0/threshold_numerator",
            "01".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].predicates[].threshold_denominator",
            "/scenarios/0/trials/0/predicates/0/threshold_denominator",
            "0".into(),
        ),
        section15_field_mutation(
            report,
            "scenarios[].trials[].predicates[].passed",
            "/scenarios/0/trials/0/predicates/0/passed",
            false.into(),
        ),
        section15_field_mutation(
            report,
            "baseline_deltas[].scenario",
            "/baseline_deltas/0/scenario",
            "burst".into(),
        ),
        section15_field_mutation(
            report,
            "baseline_deltas[].trial_index",
            "/baseline_deltas/0/trial_index",
            0.into(),
        ),
        section15_field_mutation(
            report,
            "baseline_deltas[].metric",
            "/baseline_deltas/0/metric",
            "startup".into(),
        ),
        section15_field_mutation(
            report,
            "baseline_deltas[].statistic",
            "/baseline_deltas/0/statistic",
            "maximum".into(),
        ),
        section15_field_mutation(
            report,
            "baseline_deltas[].unit",
            "/baseline_deltas/0/unit",
            "bytes".into(),
        ),
        section15_field_mutation(
            report,
            "baseline_deltas[].baseline_value",
            "/baseline_deltas/0/baseline_value",
            "01".into(),
        ),
        section15_field_mutation(
            report,
            "baseline_deltas[].final_value",
            "/baseline_deltas/0/final_value",
            "01".into(),
        ),
        section15_field_mutation(
            report,
            "baseline_deltas[].signed_delta",
            "/baseline_deltas/0/signed_delta",
            "+0".into(),
        ),
        section15_field_mutation(
            d4_report,
            "failure_policy_evidence[].measurement_stage",
            "/failure_policy_evidence/0/measurement_stage",
            "baseline".into(),
        ),
        section15_field_mutation(
            d4_report,
            "failure_policy_evidence[].scenario",
            "/failure_policy_evidence/0/scenario",
            "burst".into(),
        ),
        section15_field_mutation(
            d4_report,
            "failure_policy_evidence[].failure_reason",
            "/failure_policy_evidence/0/failure_reason",
            "startup_latency".into(),
        ),
        section15_field_mutation(
            d4_report,
            "failure_policy_evidence[].policy",
            "/failure_policy_evidence/0/policy",
            "non_d4".into(),
        ),
        section15_field_mutation(
            d4_report,
            "failure_policy_evidence[].d4_analysis_sum",
            "/failure_policy_evidence/0/d4_analysis_sum",
            "01".into(),
        ),
        section15_field_mutation(
            d4_report,
            "failure_policy_evidence[].reducer_plus_publish_sum",
            "/failure_policy_evidence/0/reducer_plus_publish_sum",
            "0".into(),
        ),
        section15_field_mutation(
            d4_report,
            "failure_policy_evidence[].d4_exact_quarter_predicate",
            "/failure_policy_evidence/0/d4_exact_quarter_predicate",
            false.into(),
        ),
        section15_field_mutation(
            d4_report,
            "failure_policy_evidence[].required_amendment",
            "/failure_policy_evidence/0/required_amendment",
            "non_d4".into(),
        ),
        section15_field_mutation(
            d4_report,
            "decision.kind",
            "/decision/kind",
            "unknown".into(),
        ),
        section15_field_mutation(
            d4_report,
            "decision.amendments[]",
            "/decision/amendments",
            serde_json::json!(["non_d4"]),
        ),
    ];

    let expected_field_paths = BTreeSet::from([
        "schema_version",
        "subject_sha",
        "baseline_id",
        "selected_results[].measurement_stage",
        "selected_results[].scenario",
        "selected_results[].canonical_result_path",
        "selected_results[].canonical_raw_root",
        "selected_results[].result_sha256",
        "selected_results[].production_subject_sha",
        "selected_results[].harness_sha",
        "selected_results[].workload_schema_sha256",
        "selected_results[].baseline_id",
        "selected_results[].measured_binary.requested_path",
        "selected_results[].measured_binary.canonical_path",
        "selected_results[].measured_binary.sha256",
        "scenarios[].scenario",
        "scenarios[].baseline_status",
        "scenarios[].final_status",
        "scenarios[].final_failure_reasons[]",
        "scenarios[].trials[].trial_index",
        "scenarios[].trials[].sequence_counts.submitted",
        "scenarios[].trials[].sequence_counts.admitted",
        "scenarios[].trials[].sequence_counts.completed",
        "scenarios[].trials[].sequence_counts.persisted",
        "scenarios[].trials[].sequence_counts.rendered_probes",
        "scenarios[].trials[].admission_buckets_attained",
        "scenarios[].trials[].lossless",
        "scenarios[].trials[].structural_identities_match",
        "scenarios[].trials[].distributions[].metric",
        "scenarios[].trials[].distributions[].unit",
        "scenarios[].trials[].distributions[].sample_count",
        "scenarios[].trials[].distributions[].minimum",
        "scenarios[].trials[].distributions[].median",
        "scenarios[].trials[].distributions[].p95",
        "scenarios[].trials[].distributions[].p99",
        "scenarios[].trials[].distributions[].maximum",
        "scenarios[].trials[].predicates[].metric",
        "scenarios[].trials[].predicates[].unit",
        "scenarios[].trials[].predicates[].ordinal",
        "scenarios[].trials[].predicates[].observed_numerator",
        "scenarios[].trials[].predicates[].observed_denominator",
        "scenarios[].trials[].predicates[].comparison",
        "scenarios[].trials[].predicates[].threshold_numerator",
        "scenarios[].trials[].predicates[].threshold_denominator",
        "scenarios[].trials[].predicates[].passed",
        "baseline_deltas[].scenario",
        "baseline_deltas[].trial_index",
        "baseline_deltas[].metric",
        "baseline_deltas[].statistic",
        "baseline_deltas[].unit",
        "baseline_deltas[].baseline_value",
        "baseline_deltas[].final_value",
        "baseline_deltas[].signed_delta",
        "failure_policy_evidence[].measurement_stage",
        "failure_policy_evidence[].scenario",
        "failure_policy_evidence[].failure_reason",
        "failure_policy_evidence[].policy",
        "failure_policy_evidence[].d4_analysis_sum",
        "failure_policy_evidence[].reducer_plus_publish_sum",
        "failure_policy_evidence[].d4_exact_quarter_predicate",
        "failure_policy_evidence[].required_amendment",
        "decision.kind",
        "decision.amendments[]",
    ]);
    let visited = cases
        .iter()
        .map(|(label, _)| *label)
        .collect::<BTreeSet<_>>();
    debug_assert_eq!(visited, expected_field_paths);
    debug_assert_eq!(cases.len(), expected_field_paths.len());

    let mut unknown_top_level = serde_json::to_value(report).unwrap();
    unknown_top_level
        .as_object_mut()
        .unwrap()
        .insert("unexpected".to_owned(), true.into());
    cases.push(("structural.top-level-unknown", unknown_top_level));
    cases
}

fn section15_structural_mutations(
    report: &Section15ReDerivationV1,
    d4_report: &Section15ReDerivationV1,
    mixed_report: &Section15ReDerivationV1,
) -> Vec<(&'static str, serde_json::Value)> {
    let mut cases = Vec::new();
    let mut push = |label: &'static str, value: Section15ReDerivationV1| {
        cases.push((label, serde_json::to_value(value).unwrap()));
    };

    let mut value = report.clone();
    value.selected_results.remove(0);
    push("selected.delete", value);
    let mut value = report.clone();
    value
        .selected_results
        .insert(0, value.selected_results[0].clone());
    push("selected.duplicate", value);
    let mut value = report.clone();
    value.selected_results.swap(0, 1);
    push("selected.reorder", value);

    let mut value = report.clone();
    value.scenarios.remove(0);
    push("scenario.delete", value);
    let mut value = report.clone();
    value.scenarios.insert(0, value.scenarios[0].clone());
    push("scenario.duplicate", value);
    let mut value = report.clone();
    value.scenarios.swap(0, 1);
    push("scenario.reorder", value);

    let mut value = report.clone();
    value.scenarios[0].trials.remove(0);
    push("trial.delete", value);
    let mut value = report.clone();
    let duplicate = value.scenarios[0].trials[0].clone();
    value.scenarios[0].trials.insert(0, duplicate);
    push("trial.duplicate", value);
    let mut value = report.clone();
    value.scenarios[0].trials.swap(0, 1);
    push("trial.reorder", value);
    let mut value = report.clone();
    value.scenarios[0].trials[0] = value.scenarios[1].trials[0].clone();
    push("trial.cross-scenario-substitution", value);

    let mut value = report.clone();
    value.scenarios[0].trials[0].distributions.remove(0);
    push("distribution.delete", value);
    let mut value = report.clone();
    let duplicate = value.scenarios[0].trials[0].distributions[0].clone();
    value.scenarios[0].trials[0]
        .distributions
        .insert(0, duplicate);
    push("distribution.duplicate", value);

    let mut value = report.clone();
    value.scenarios[0].trials[0].predicates.remove(0);
    push("predicate.delete", value);
    let mut value = report.clone();
    let duplicate = value.scenarios[0].trials[0].predicates[0].clone();
    value.scenarios[0].trials[0].predicates.insert(0, duplicate);
    push("predicate.duplicate", value);
    let mut value = report.clone();
    value.scenarios[0].trials[0].predicates.swap(0, 1);
    push("predicate.reorder", value);

    let mut value = report.clone();
    value.baseline_deltas.remove(0);
    push("delta.delete", value);
    let mut value = report.clone();
    value
        .baseline_deltas
        .insert(0, value.baseline_deltas[0].clone());
    push("delta.duplicate", value);
    let mut value = report.clone();
    value.baseline_deltas.swap(0, 1);
    push("delta.reorder", value);

    let mut value = d4_report.clone();
    value.failure_policy_evidence.clear();
    push("policy.delete", value);
    let mut value = d4_report.clone();
    value
        .failure_policy_evidence
        .push(value.failure_policy_evidence[0].clone());
    push("policy.duplicate", value);
    let mut value = mixed_report.clone();
    value.failure_policy_evidence.swap(0, 1);
    push("policy.reorder", value);

    cases
}

#[test]
fn section15_selected_evidence_is_reopened_and_rederived() {
    let valid = section15_fixture_from_final(&all_passes());
    assert!(valid.report.validate().is_ok());

    let mut tampered_digest = valid.report.clone();
    tampered_digest.selected_results[0].result_sha256 = "0".repeat(64);
    assert_eq!(
        tampered_digest.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let missing_result = section15_fixture_from_final(&all_passes());
    std::fs::remove_file(&missing_result.report.selected_results[0].canonical_result_path).unwrap();
    assert_eq!(
        missing_result.report.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let missing_raw_root = section15_fixture_from_final(&all_passes());
    let raw_root =
        std::path::Path::new(&missing_raw_root.report.selected_results[0].canonical_raw_root);
    std::fs::rename(
        raw_root,
        missing_raw_root.baseline_root.path().join("removed-target"),
    )
    .unwrap();
    assert_eq!(
        missing_raw_root.report.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let raw_mismatch = section15_fixture_from_final(&all_passes());
    let raw_root =
        std::path::Path::new(&raw_mismatch.report.selected_results[0].canonical_raw_root);
    std::fs::write(raw_root.join("trial-0001/stdout"), b"tampered raw evidence").unwrap();
    assert_eq!(
        raw_mismatch.report.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let result_mismatch = section15_fixture_from_final(&all_passes());
    let result_path =
        std::path::Path::new(&result_mismatch.report.selected_results[0].canonical_result_path);
    let mut bytes = std::fs::read(result_path).unwrap();
    bytes.push(b'\n');
    std::fs::write(result_path, bytes).unwrap();
    assert_eq!(
        result_mismatch.report.validate(),
        Err(ResultError::InvalidArtifact)
    );
}

#[cfg(feature = "workload-harness")]
#[test]
fn section15_selected_paths_reject_absolute_noncanonical_spelling() {
    // Break caught: `Path::components` normalizes `.` before the structural
    // validator can observe it, despite the field promising canonical text.
    let fixture = section15_fixture_from_final(&all_passes());
    let mut report = fixture.report.clone();
    let identity = &mut report.selected_results[0];
    identity.canonical_raw_root = "/tmp/./x".to_owned();
    identity.canonical_result_path = "/tmp/./x/result-v1.json".to_owned();

    assert_eq!(
        validate_section15_shape_for_test(&report),
        Err(ResultError::InvalidArtifact)
    );
}

#[test]
fn section15_rederivation_schema_is_closed_complete_and_decision_owned() {
    let fixture = section15_fixture_from_final(&all_passes());
    let d4_fixture = section15_d4_fixture();
    let mixed_fixture = section15_mixed_policy_fixture();
    let report = &fixture.report;
    let d4_report = &d4_fixture.report;
    let mixed_report = &mixed_fixture.report;
    assert_eq!(report.schema_version, 1);
    assert_eq!(report.selected_results.len(), 14);
    assert_eq!(report.scenarios.len(), 7);
    assert_eq!(
        report
            .scenarios
            .iter()
            .map(|row| row.trials.len())
            .sum::<usize>(),
        40
    );
    assert_eq!(
        report
            .scenarios
            .iter()
            .flat_map(|row| &row.trials)
            .map(|trial| trial.distributions.len())
            .sum::<usize>(),
        125
    );
    assert_eq!(
        report
            .scenarios
            .iter()
            .flat_map(|row| &row.trials)
            .map(|trial| trial.predicates.len())
            .sum::<usize>(),
        825
    );
    assert_eq!(report.baseline_deltas.len(), 625);
    assert!(report.failure_policy_evidence.is_empty());
    assert_eq!(d4_report.failure_policy_evidence.len(), 1);
    assert!(
        report
            .selected_results
            .iter()
            .all(|identity| identity.baseline_id == report.baseline_id)
    );
    assert!(report.validate().is_ok());
    let encoded = serde_json::to_value(report).unwrap();
    let mut missing = encoded.clone();
    missing.as_object_mut().unwrap().remove("selected_results");
    assert!(serde_json::from_value::<Section15ReDerivationV1>(missing).is_err());
    let mut unknown = encoded;
    unknown
        .as_object_mut()
        .unwrap()
        .insert("unexpected".to_owned(), serde_json::json!(true));
    assert!(serde_json::from_value::<Section15ReDerivationV1>(unknown).is_err());
    let mut duplicate = fixture.report.clone();
    let first = duplicate.selected_results.remove(0);
    duplicate.selected_results.insert(0, first);
    duplicate.selected_results[1].canonical_result_path =
        duplicate.selected_results[0].canonical_result_path.clone();
    assert_eq!(duplicate.validate(), Err(ResultError::InvalidArtifact));

    let mut lossless = fixture.report.clone();
    lossless.scenarios[0].trials[0].lossless = false;
    assert_eq!(lossless.validate(), Err(ResultError::InvalidArtifact));

    let mut target_sequence = fixture.report.clone();
    target_sequence.scenarios[0].trials[0]
        .sequence_counts
        .submitted = 1;
    assert_eq!(
        target_sequence.validate(),
        Err(ResultError::InvalidArtifact)
    );

    let mut sample_count = fixture.report.clone();
    sample_count.scenarios[0].trials[0].distributions[0].sample_count += 1;
    assert_eq!(sample_count.validate(), Err(ResultError::InvalidArtifact));

    let mut admission_summary = fixture.report.clone();
    admission_summary.scenarios[1].trials[0].admission_buckets_attained = Some(false);
    assert_eq!(
        admission_summary.validate(),
        Err(ResultError::InvalidArtifact)
    );

    for (label, value) in section15_mutate_every_schema_field(report, d4_report)
        .into_iter()
        .chain(section15_structural_mutations(
            report,
            d4_report,
            mixed_report,
        ))
    {
        assert_invalid_section15_value(label, value);
    }
}

#[test]
fn section15_predicate_matrix_is_exact_and_recomputed() {
    let fixture = section15_fixture_from_final(&all_passes());
    let report = &fixture.report;
    let expected = [
        (ScenarioV1::Target, 2),
        (ScenarioV1::Sustained, 68),
        (ScenarioV1::Burst, 18),
        (ScenarioV1::Startup, 1),
        (ScenarioV1::Idle, 2),
        (ScenarioV1::FallbackRescan, 6),
        (ScenarioV1::TwiceTarget, 67),
    ];
    for (scenario, count) in expected {
        let row = report
            .scenarios
            .iter()
            .find(|row| row.scenario == scenario)
            .unwrap();
        assert!(
            row.trials
                .iter()
                .all(|trial| trial.predicates.len() == count)
        );
    }
    let target = &report.scenarios[0].trials[0].predicates;
    assert_eq!(target[0].metric, Section15MetricV1::InputResponse);
    assert_eq!(target[0].observed_numerator, "20000000");
    assert_eq!(target[0].threshold_numerator, "100000000");
    assert_eq!(target[0].comparison, ThresholdComparisonV1::LessThan);
    assert!(target[0].passed);

    let mut changed = fixture.report.clone();
    changed.scenarios[0].trials[0].predicates[0].passed = false;
    assert_eq!(changed.validate(), Err(ResultError::InvalidArtifact));
}

struct RawScenarioRoot {
    _parent: tempfile::TempDir,
    path: PathBuf,
}

impl RawScenarioRoot {
    fn new(scenario: ScenarioV1) -> Self {
        let parent = tempfile::tempdir().unwrap();
        let directory = &workload_schema()
            .scenarios
            .iter()
            .find(|spec| spec.scenario == scenario)
            .unwrap()
            .directory;
        let path = parent.path().join(directory);
        std::fs::create_dir(&path).unwrap();
        Self {
            _parent: parent,
            path,
        }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

struct RawFixture {
    root: RawScenarioRoot,
}

impl RawFixture {
    fn new() -> Self {
        Self::from_outcome(valid_synthetic_result())
    }

    fn from_outcome(mut outcome: ReferenceOutcomeV1) -> Self {
        let scenario = outcome.document().scenario;
        let fixture = Self {
            root: RawScenarioRoot::new(scenario),
        };
        write_synthetic_raw_scenario_root(fixture.root.path(), &mut outcome).unwrap();
        fixture
    }

    fn empty() -> Self {
        Self {
            root: RawScenarioRoot::new(ScenarioV1::Sustained),
        }
    }

    fn output_path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn request(&self) -> ComposeRequestV1 {
        self.request_for(ScenarioV1::Sustained)
    }

    fn request_for(&self, scenario: ScenarioV1) -> ComposeRequestV1 {
        ComposeRequestV1 {
            raw_root: self.root.path().to_path_buf(),
            output: self.output_path("candidate-v1.json"),
            measurement_stage: MeasurementStageV1::Baseline,
            scenario,
            production_subject_sha: BASELINE_SUBJECT_SHA.to_owned(),
            preflight_head: SYNTHETIC_HARNESS_SHA.to_owned(),
            baseline_results_root: None,
        }
    }
}

fn mark_trial_failed(fixture: &RawFixture, trial_index: usize, exit_code: u8) {
    let directory = fixture.root.path().join(format!("trial-{trial_index:04}"));
    let control_path = directory.join("runner-control.json");
    let mut control: RunnerControlEvidenceV1 =
        serde_json::from_slice(&std::fs::read(&control_path).unwrap()).unwrap();
    control.trial.trial_status = TrialStatusV1::Failed { exit_code };
    control.trial.pidstat_exit_status = exit_code;
    let mut bytes = serde_json::to_vec(&control).unwrap();
    bytes.push(b'\n');
    std::fs::write(control_path, bytes).unwrap();
    std::fs::write(
        directory.join("trial-status"),
        format!("failed:{exit_code}\n"),
    )
    .unwrap();
}

#[test]
fn raw_idle_composer_excludes_counterless_pre_start_exit_from_cpu_totals() {
    let mut source = valid_idle_result();
    let trial = &mut source.document_mut().trials[0];
    trial
        .process_tree
        .process_identity_resources
        .push(ProcessIdentityResourceV1 {
            pid: 10_004,
            start_time_ticks: 80,
            first_observed_offset_ns: 2_000_000,
            idle_window_start_user_cpu_ticks: None,
            idle_window_start_system_cpu_ticks: None,
            idle_window_end_user_cpu_ticks: None,
            idle_window_end_system_cpu_ticks: None,
            last_user_cpu_ticks: 7,
            last_system_cpu_ticks: 3,
            maximum_vm_hwm_bytes: 123,
        });
    trial.sum_process_identity_peak_rss_bytes_diagnostic += 123;
    let fixture = RawFixture::from_outcome(source);

    let outcome =
        compose_reference_outcome_from_raw_impl(&fixture.request_for(ScenarioV1::Idle)).unwrap();

    assert_eq!(outcome.status(), ReferenceOutcomeStatusV1::Pass);
    assert_eq!(outcome.document().trials[0].user_cpu_ns, 100_000_000);
    assert_eq!(outcome.document().trials[0].system_cpu_ns, 10_000_000);
    assert!(
        outcome.document().trials[0]
            .process_tree
            .process_identity_resources
            .iter()
            .any(|identity| identity.pid == 10_004
                && identity.idle_window_start_user_cpu_ticks.is_none()
                && identity.idle_window_start_system_cpu_ticks.is_none()
                && identity.idle_window_end_user_cpu_ticks.is_none()
                && identity.idle_window_end_system_cpu_ticks.is_none())
    );
    assert!(outcome.validate().is_ok());
}

#[test]
fn raw_idle_composer_rejects_partial_idle_tick_pair() {
    let mut source = valid_idle_result();
    let trial = &mut source.document_mut().trials[0];
    trial
        .process_tree
        .process_identity_resources
        .push(ProcessIdentityResourceV1 {
            pid: 10_004,
            start_time_ticks: 80,
            first_observed_offset_ns: 2_000_000,
            idle_window_start_user_cpu_ticks: None,
            idle_window_start_system_cpu_ticks: None,
            idle_window_end_user_cpu_ticks: None,
            idle_window_end_system_cpu_ticks: None,
            last_user_cpu_ticks: 7,
            last_system_cpu_ticks: 3,
            maximum_vm_hwm_bytes: 123,
        });
    trial.sum_process_identity_peak_rss_bytes_diagnostic += 123;
    let fixture = RawFixture::from_outcome(source);
    let path = fixture.root.path().join("trial-0001/process-tree.json");
    let mut tree: ProcessTreeEvidenceV1 =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    tree.process_identity_resources
        .iter_mut()
        .find(|identity| identity.pid == 10_004)
        .unwrap()
        .idle_window_start_user_cpu_ticks = Some(0);
    let mut bytes = serde_json::to_vec(&tree).unwrap();
    bytes.push(b'\n');
    std::fs::write(path, bytes).unwrap();

    let outcome =
        compose_reference_outcome_from_raw_impl(&fixture.request_for(ScenarioV1::Idle)).unwrap();

    assert_eq!(outcome.status(), ReferenceOutcomeStatusV1::Invalid);
    assert_eq!(
        outcome.failure_reasons(),
        &[FailureReasonV1::InvalidArtifact]
    );
}

#[test]
fn typed_reference_composer_owns_candidate_construction_and_atomic_finalization() {
    let fixture = RawFixture::new();
    let output = fixture.output_path("candidate-v1.json");
    let outcome = compose_reference_outcome_from_raw_impl(&fixture.request()).unwrap();
    assert_eq!(outcome.status(), ReferenceOutcomeStatusV1::Pass);
    assert!(outcome.validate().is_ok());
    assert!(atomic_write_reference_outcome(&output, &outcome).is_ok());
    assert_eq!(
        read_and_validate_reference_outcome(&output, AmendedLegacyMode::Off)
            .unwrap()
            .outcome
            .status(),
        ReferenceOutcomeStatusV1::Pass
    );
    let missing = RawFixture::empty();
    let invalid = compose_reference_outcome_from_raw_impl(&missing.request()).unwrap();
    assert_eq!(invalid.status(), ReferenceOutcomeStatusV1::Invalid);
    assert!(invalid.validate().is_ok());

    let substituted_output = RawFixture::new();
    let mut substituted_request = substituted_output.request();
    substituted_request.output = substituted_output.output_path("not-candidate.json");
    assert_eq!(
        compose_reference_outcome_from_raw_impl(&substituted_request)
            .unwrap()
            .status(),
        ReferenceOutcomeStatusV1::Invalid
    );

    let failed = RawFixture::new();
    let directory = failed.root.path().join("trial-0001");
    let control_path = directory.join("runner-control.json");
    let mut control: RunnerControlEvidenceV1 =
        serde_json::from_slice(&std::fs::read(&control_path).unwrap()).unwrap();
    control.trial.trial_status = TrialStatusV1::Failed { exit_code: 20 };
    control.trial.pidstat_exit_status = 20;
    let mut bytes = serde_json::to_vec(&control).unwrap();
    bytes.push(b'\n');
    std::fs::write(control_path, bytes).unwrap();
    std::fs::write(directory.join("trial-status"), b"failed:20\n").unwrap();
    let outcome = compose_reference_outcome_from_raw_impl(&failed.request()).unwrap();
    assert_eq!(outcome.status(), ReferenceOutcomeStatusV1::Invalid);
    assert_eq!(outcome.failure_reasons(), &[FailureReasonV1::CommandFailed]);
    assert!(outcome.validate().is_ok());

    let noncanonical = RawFixture::new();
    let directory = noncanonical.root.path().join("trial-0001");
    let control_path = directory.join("runner-control.json");
    let mut control: RunnerControlEvidenceV1 =
        serde_json::from_slice(&std::fs::read(&control_path).unwrap()).unwrap();
    control.trial.trial_status = TrialStatusV1::Failed { exit_code: 1 };
    control.trial.pidstat_exit_status = 1;
    let mut bytes = serde_json::to_vec(&control).unwrap();
    bytes.push(b'\n');
    std::fs::write(control_path, bytes).unwrap();
    std::fs::write(directory.join("trial-status"), b"failed:+1\n").unwrap();
    let outcome = compose_reference_outcome_from_raw_impl(&noncanonical.request()).unwrap();
    assert_eq!(outcome.status(), ReferenceOutcomeStatusV1::Invalid);
    assert_eq!(
        outcome.failure_reasons(),
        &[FailureReasonV1::InvalidArtifact]
    );

    for artifact in [
        "harness.json",
        "runner-control.json",
        "process-tree.json",
        "observer-handshake",
        "observer-control.json",
        "gnu-time.txt",
        "pidstat.json",
        "pidstat-stderr",
        "stdout",
        "stderr",
        "observer-stdout",
        "observer-stderr",
        "trial-status",
    ] {
        let missing_artifact = RawFixture::new();
        std::fs::remove_file(
            missing_artifact
                .root
                .path()
                .join("trial-0001")
                .join(artifact),
        )
        .unwrap();
        let outcome = compose_reference_outcome_from_raw_impl(&missing_artifact.request()).unwrap();
        assert_eq!(
            outcome.failure_reasons(),
            &[FailureReasonV1::InvalidArtifact],
            "missing artifact must fail closed: {artifact}"
        );
    }

    for malformed in [
        b"ok:00\n".as_slice(),
        b"failed:0\n",
        b"failed:01\n",
        b"failed:256\n",
        b"failed:20\nextra\n",
    ] {
        let malformed_status = RawFixture::new();
        std::fs::write(
            malformed_status.root.path().join("trial-0001/trial-status"),
            malformed,
        )
        .unwrap();
        assert_eq!(
            compose_reference_outcome_from_raw_impl(&malformed_status.request())
                .unwrap()
                .failure_reasons(),
            &[FailureReasonV1::InvalidArtifact]
        );
    }

    let monitor_mode = RawFixture::new();
    for trial_index in 1..=5 {
        let directory = monitor_mode
            .root
            .path()
            .join(format!("trial-{trial_index:04}"));
        let control_path = directory.join("runner-control.json");
        let mut control: RunnerControlEvidenceV1 =
            serde_json::from_slice(&std::fs::read(&control_path).unwrap()).unwrap();
        control.controls.pidstat_child_status_mode = PidstatChildStatusModeV1::MonitorOnly;
        control.trial.pidstat_exit_status = 0;
        if trial_index == 1 {
            control.trial.trial_status = TrialStatusV1::Failed { exit_code: 20 };
            std::fs::write(directory.join("trial-status"), b"failed:20\n").unwrap();
        }
        let mut bytes = serde_json::to_vec(&control).unwrap();
        bytes.push(b'\n');
        std::fs::write(control_path, bytes).unwrap();
    }
    let monitor_outcome = compose_reference_outcome_from_raw_impl(&monitor_mode.request()).unwrap();
    assert_eq!(
        monitor_outcome.failure_reasons(),
        &[FailureReasonV1::CommandFailed]
    );

    #[cfg(unix)]
    {
        let reused_status = RawFixture::new();
        let trial_two = reused_status.root.path().join("trial-0002/trial-status");
        std::fs::remove_file(&trial_two).unwrap();
        std::os::unix::fs::symlink("../trial-0001/trial-status", &trial_two).unwrap();
        assert_eq!(
            compose_reference_outcome_from_raw_impl(&reused_status.request())
                .unwrap()
                .failure_reasons(),
            &[FailureReasonV1::InvalidArtifact]
        );
    }
}

#[test]
fn typed_reference_validator_owns_final_publication_and_status_mapping() {
    let fixture = RawFixture::new();
    let candidate = fixture.output_path("candidate-v1.json");
    let output = fixture.output_path("result-v1.json");
    let valid = compose_reference_outcome_from_raw_impl(&fixture.request()).unwrap();
    atomic_write_reference_outcome(&candidate, &valid).ok();
    let request = ValidateRequestV1 {
        raw_root: fixture.root.path().to_path_buf(),
        candidate,
        output: output.clone(),
        measurement_stage: MeasurementStageV1::Baseline,
        scenario: ScenarioV1::Sustained,
        production_subject_sha: BASELINE_SUBJECT_SHA.to_owned(),
        preflight_head: SYNTHETIC_HARNESS_SHA.to_owned(),
        composer_status: "0".to_owned(),
        trial_status: "all-ok".to_owned(),
        baseline_results_root: None,
    };
    assert_eq!(validate_reference_outcome_impl(&request).unwrap(), 0);
    assert_eq!(
        read_and_validate_reference_outcome(&output, AmendedLegacyMode::Off)
            .unwrap()
            .outcome
            .status(),
        ReferenceOutcomeStatusV1::Pass
    );

    let substituted_candidate = fixture.output_path("substituted-candidate.json");
    std::fs::copy(&request.candidate, &substituted_candidate).unwrap();
    let substituted_request = ValidateRequestV1 {
        candidate: substituted_candidate,
        output: fixture.output_path("substituted-result.json"),
        ..request.clone()
    };
    assert_eq!(
        validate_reference_outcome_impl(&substituted_request).unwrap(),
        20
    );
    assert_eq!(
        read_and_validate_reference_outcome(
            &fixture.output_path("result-v1.json"),
            AmendedLegacyMode::Off,
        )
        .unwrap()
        .outcome
        .failure_reasons(),
        &[FailureReasonV1::InvalidArtifact]
    );
    assert!(!substituted_request.output.exists());

    for token in [
        "unexpected:+101",
        "unexpected:0101",
        "unexpected:0",
        "unexpected:10",
        "unexpected:20",
        "unexpected:256",
        "+1",
        "01",
        "failed",
    ] {
        let malformed = RawFixture::new();
        let candidate = malformed.output_path("candidate-v1.json");
        let candidate_outcome =
            compose_reference_outcome_from_raw_impl(&malformed.request()).unwrap();
        atomic_write_reference_outcome(&candidate, &candidate_outcome).unwrap();
        let malformed_request = ValidateRequestV1 {
            raw_root: malformed.root.path().to_path_buf(),
            candidate,
            output: malformed.output_path("result-v1.json"),
            measurement_stage: MeasurementStageV1::Baseline,
            scenario: ScenarioV1::Sustained,
            production_subject_sha: BASELINE_SUBJECT_SHA.to_owned(),
            preflight_head: SYNTHETIC_HARNESS_SHA.to_owned(),
            composer_status: token.to_owned(),
            trial_status: "all-ok".to_owned(),
            baseline_results_root: None,
        };
        assert_eq!(
            validate_reference_outcome_impl(&malformed_request).unwrap(),
            20,
            "malformed composer token must fail closed: {token}"
        );
        assert_eq!(
            read_and_validate_reference_outcome(&malformed_request.output, AmendedLegacyMode::Off,)
                .unwrap()
                .outcome
                .failure_reasons(),
            &[FailureReasonV1::InvalidArtifact]
        );
    }

    for (fixture_kind, composer_status, expected_status) in [
        ("pass", "0", ReferenceOutcomeStatusV1::Pass),
        ("pass", "10", ReferenceOutcomeStatusV1::Invalid),
        ("pass", "20", ReferenceOutcomeStatusV1::Invalid),
        ("failed", "0", ReferenceOutcomeStatusV1::Invalid),
        ("failed", "10", ReferenceOutcomeStatusV1::Failed),
        ("failed", "20", ReferenceOutcomeStatusV1::Invalid),
        ("invalid", "0", ReferenceOutcomeStatusV1::Invalid),
        ("invalid", "10", ReferenceOutcomeStatusV1::Invalid),
        ("invalid", "20", ReferenceOutcomeStatusV1::Invalid),
    ] {
        let matrix = match fixture_kind {
            "pass" => RawFixture::new(),
            "failed" => RawFixture::from_outcome(failed_outcome(
                ScenarioV1::Sustained,
                MeasurementStageV1::Baseline,
                FailureReasonV1::ScreenLatency,
                250,
                1_000,
            )),
            "invalid" => RawFixture::empty(),
            _ => unreachable!(),
        };
        let candidate = matrix.output_path("candidate-v1.json");
        let candidate_outcome = compose_reference_outcome_from_raw_impl(&matrix.request()).unwrap();
        atomic_write_reference_outcome(&candidate, &candidate_outcome).unwrap();
        let output = matrix.output_path("result-v1.json");
        let matrix_request = ValidateRequestV1 {
            raw_root: matrix.root.path().to_path_buf(),
            candidate,
            output: output.clone(),
            measurement_stage: MeasurementStageV1::Baseline,
            scenario: ScenarioV1::Sustained,
            production_subject_sha: BASELINE_SUBJECT_SHA.to_owned(),
            preflight_head: SYNTHETIC_HARNESS_SHA.to_owned(),
            composer_status: composer_status.to_owned(),
            trial_status: "all-ok".to_owned(),
            baseline_results_root: None,
        };
        let actual_code = validate_reference_outcome_impl(&matrix_request).unwrap();
        let published = read_and_validate_reference_outcome(&output, AmendedLegacyMode::Off)
            .unwrap()
            .outcome;
        assert_eq!(
            published.status(),
            expected_status,
            "status matrix row {fixture_kind}/{composer_status}"
        );
        assert_eq!(
            actual_code,
            match expected_status {
                ReferenceOutcomeStatusV1::Pass => 0,
                ReferenceOutcomeStatusV1::Failed => 10,
                ReferenceOutcomeStatusV1::Invalid => 20,
            }
        );
    }

    for unexpected in [101, 137] {
        for malformed_candidate in [false, true] {
            let command_failure = RawFixture::new();
            let candidate = command_failure.output_path("candidate-v1.json");
            if malformed_candidate {
                std::fs::write(&candidate, b"{not-json\n").unwrap();
            }
            let output = command_failure.output_path("result-v1.json");
            let command_failure_request = ValidateRequestV1 {
                raw_root: command_failure.root.path().to_path_buf(),
                candidate,
                output: output.clone(),
                measurement_stage: MeasurementStageV1::Baseline,
                scenario: ScenarioV1::Sustained,
                production_subject_sha: BASELINE_SUBJECT_SHA.to_owned(),
                preflight_head: SYNTHETIC_HARNESS_SHA.to_owned(),
                composer_status: format!("unexpected:{unexpected}"),
                trial_status: "all-ok".to_owned(),
                baseline_results_root: None,
            };
            assert_eq!(
                validate_reference_outcome_impl(&command_failure_request).unwrap(),
                20
            );
            assert_eq!(
                read_and_validate_reference_outcome(&output, AmendedLegacyMode::Off)
                    .unwrap()
                    .outcome
                    .failure_reasons(),
                &[FailureReasonV1::CommandFailed]
            );
        }
    }

    for exit_code in [10, 20, 124, 130, 137, 143] {
        let sentinel = RawFixture::new();
        mark_trial_failed(&sentinel, 1, exit_code);
        let candidate = sentinel.output_path("candidate-v1.json");
        let candidate_outcome =
            compose_reference_outcome_from_raw_impl(&sentinel.request()).unwrap();
        assert_eq!(
            candidate_outcome.failure_reasons(),
            &[FailureReasonV1::CommandFailed]
        );
        atomic_write_reference_outcome(&candidate, &candidate_outcome).unwrap();
        let output = sentinel.output_path("result-v1.json");
        let sentinel_request = ValidateRequestV1 {
            raw_root: sentinel.root.path().to_path_buf(),
            candidate,
            output: output.clone(),
            measurement_stage: MeasurementStageV1::Baseline,
            scenario: ScenarioV1::Sustained,
            production_subject_sha: BASELINE_SUBJECT_SHA.to_owned(),
            preflight_head: SYNTHETIC_HARNESS_SHA.to_owned(),
            composer_status: "20".to_owned(),
            trial_status: format!("failed:trial-1:{exit_code}"),
            baseline_results_root: None,
        };
        assert_eq!(
            validate_reference_outcome_impl(&sentinel_request).unwrap(),
            20
        );
        assert_eq!(
            read_and_validate_reference_outcome(&output, AmendedLegacyMode::Off)
                .unwrap()
                .outcome
                .failure_reasons(),
            &[FailureReasonV1::CommandFailed]
        );
    }

    let failed = RawFixture::new();
    let directory = failed.root.path().join("trial-0001");
    let control_path = directory.join("runner-control.json");
    let mut control: RunnerControlEvidenceV1 =
        serde_json::from_slice(&std::fs::read(&control_path).unwrap()).unwrap();
    control.trial.trial_status = TrialStatusV1::Failed { exit_code: 20 };
    control.trial.pidstat_exit_status = 20;
    let mut bytes = serde_json::to_vec(&control).unwrap();
    bytes.push(b'\n');
    std::fs::write(control_path, bytes).unwrap();
    std::fs::write(directory.join("trial-status"), b"failed:20\n").unwrap();
    let candidate = failed.output_path("candidate-v1.json");
    let candidate_outcome = compose_reference_outcome_from_raw_impl(&failed.request()).unwrap();
    atomic_write_reference_outcome(&candidate, &candidate_outcome).unwrap();
    let output = failed.output_path("result-v1.json");
    let failed_request = ValidateRequestV1 {
        raw_root: failed.root.path().to_path_buf(),
        candidate: candidate.clone(),
        output: output.clone(),
        measurement_stage: MeasurementStageV1::Baseline,
        scenario: ScenarioV1::Sustained,
        production_subject_sha: BASELINE_SUBJECT_SHA.to_owned(),
        preflight_head: SYNTHETIC_HARNESS_SHA.to_owned(),
        composer_status: "20".to_owned(),
        trial_status: "failed:trial-1:20".to_owned(),
        baseline_results_root: None,
    };
    assert_eq!(
        validate_reference_outcome_impl(&failed_request).unwrap(),
        20
    );
    assert_eq!(
        read_and_validate_reference_outcome(&output, AmendedLegacyMode::Off)
            .unwrap()
            .outcome
            .failure_reasons(),
        &[FailureReasonV1::CommandFailed]
    );
    let wrong_transport = ValidateRequestV1 {
        output: output.clone(),
        trial_status: "failed:trial-2:20".to_owned(),
        ..failed_request
    };
    assert_eq!(
        validate_reference_outcome_impl(&wrong_transport).unwrap(),
        20
    );
    assert_eq!(
        read_and_validate_reference_outcome(&output, AmendedLegacyMode::Off)
            .unwrap()
            .outcome
            .failure_reasons(),
        &[FailureReasonV1::InvalidArtifact]
    );
}

#[cfg_attr(not(feature = "workload-harness"), allow(dead_code))]
#[cfg(target_os = "linux")]
mod reference_runner_test_support {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::ffi::OsStr;
    use std::fs;
    use std::ops::{Deref, DerefMut};
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};

    pub const SOURCE_FIXTURE_BODY: &str = r#"set -euo pipefail
case $- in *p*) ;; *) exit 20 ;; esac
runner_script=$1
shift
readonly runner_script
source "$runner_script"
fixture_operation=$1
shift
readonly fixture_operation
case "$fixture_operation" in
  orchestration) run_orchestration_fixture "$@" ;;
  output-containment) run_output_containment_fixture "$@" ;;
  *) exit 20 ;;
esac"#;

    const EMPTY_BOOTSTRAP_BODY: &str = r#"builtin exec -c "$1" "${@:2}""#;

    pub const SOURCE_FIXTURE_ROLES: [&str; 16] = [
        "env",
        "id",
        "mkdir",
        "mktemp",
        "mv",
        "pidstat",
        "prlimit",
        "readlink",
        "rmdir",
        "setsid",
        "sha256sum",
        "sleep",
        "stat",
        "taskset",
        "time",
        "unlink",
    ];

    #[derive(Clone, Debug)]
    pub struct Identity {
        pub requested: PathBuf,
        pub canonical: PathBuf,
        pub sha256: String,
    }

    #[derive(Clone, Debug)]
    pub struct FixtureTool {
        pub role: String,
        pub requested: PathBuf,
    }

    pub struct FixtureTools {
        _root: tempfile::TempDir,
        entries: Vec<FixtureTool>,
    }

    impl Deref for FixtureTools {
        type Target = Vec<FixtureTool>;

        fn deref(&self) -> &Self::Target {
            &self.entries
        }
    }

    impl DerefMut for FixtureTools {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.entries
        }
    }

    pub fn manifest_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    pub fn runner_script() -> PathBuf {
        manifest_root().join("scripts/run-reference-profile.sh")
    }

    pub fn controller_binary() -> PathBuf {
        PathBuf::from(env!("CARGO_BIN_EXE_increment5-reference-controller"))
    }

    pub fn identity(requested: &Path) -> Identity {
        let canonical = fs::canonicalize(requested).unwrap();
        let bytes = fs::read(&canonical).unwrap();
        Identity {
            requested: requested.to_path_buf(),
            canonical,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        }
    }

    pub fn fixture_tools() -> FixtureTools {
        fixture_tools_with_forced_shims(&[])
    }

    pub fn fixture_tools_with_forced_shims(forced_shims: &[&str]) -> FixtureTools {
        let root = tempfile::tempdir().unwrap();
        let entries = SOURCE_FIXTURE_ROLES
            .iter()
            .map(|role| {
                let installed = PathBuf::from(format!("/usr/bin/{role}"));
                let metadata = fs::metadata(&installed);
                let use_shim = *role == "pidstat"
                    || forced_shims.contains(role)
                    || !metadata.as_ref().is_ok_and(|value| {
                        value.is_file() && value.permissions().mode() & 0o111 != 0
                    });
                let requested = if use_shim {
                    let shim = root.path().join(role);
                    write_executable(&shim, "#!/usr/bin/bash -p\nset -euo pipefail\nexit 20\n");
                    shim
                } else {
                    installed
                };
                FixtureTool {
                    role: (*role).to_owned(),
                    requested,
                }
            })
            .collect();
        FixtureTools {
            _root: root,
            entries,
        }
    }

    fn push_identity(args: &mut Vec<String>, prefix: &str, value: &Identity) {
        args.extend([
            format!("--{prefix}-requested"),
            value.requested.to_string_lossy().into_owned(),
            format!("--{prefix}-canonical"),
            value.canonical.to_string_lossy().into_owned(),
            format!("--{prefix}-sha256"),
            value.sha256.clone(),
        ]);
    }

    fn caller_environment(attempt_id: Option<&str>) -> BTreeMap<String, String> {
        let mut environment = BTreeMap::from([
            ("HOME".to_owned(), "/home/mageyuki".to_owned()),
            (
                "RUSTUP_HOME".to_owned(),
                "/home/mageyuki/.rustup".to_owned(),
            ),
            ("CARGO_HOME".to_owned(), "/home/mageyuki/.cargo".to_owned()),
            ("PATH".to_owned(), "/usr/bin:/bin".to_owned()),
            ("LC_ALL".to_owned(), "C".to_owned()),
            ("TZ".to_owned(), "UTC".to_owned()),
            (
                "HERDR_PERF_RUNNER_TEST_LIBRARY_ONLY".to_owned(),
                "1".to_owned(),
            ),
        ]);
        if let Some(attempt_id) = attempt_id {
            environment.insert(
                "HERDR_INCREMENT5_ATTEMPT_ID".to_owned(),
                attempt_id.to_owned(),
            );
        }
        environment
    }

    pub fn generic_controller_command(
        controller: &Identity,
        program: &Identity,
        child_environment: &BTreeMap<String, String>,
        child_argv: &[String],
    ) -> Command {
        let mut controller_args = Vec::new();
        push_identity(&mut controller_args, "self", controller);
        controller_args.push("launch".to_owned());
        push_identity(&mut controller_args, "program", program);
        for (key, value) in child_environment {
            controller_args.extend(["--env".to_owned(), format!("{key}={value}")]);
        }
        controller_args.push("--".to_owned());
        controller_args.extend(child_argv.iter().cloned());
        empty_bootstrap_command(controller, &controller_args)
    }

    pub fn authoritative_runner_command(
        controller: &Identity,
        runner: &Identity,
        attempt_id: &str,
        runner_argv: &[String],
    ) -> Command {
        let bash = identity(Path::new("/usr/bin/bash"));
        let mut controller_args = Vec::new();
        push_identity(&mut controller_args, "self", controller);
        controller_args.push("launch-runner".to_owned());
        push_identity(&mut controller_args, "runner-script", runner);
        push_identity(&mut controller_args, "program", &bash);
        for (key, value) in BTreeMap::from([
            ("HOME".to_owned(), "/home/mageyuki".to_owned()),
            (
                "RUSTUP_HOME".to_owned(),
                "/home/mageyuki/.rustup".to_owned(),
            ),
            ("CARGO_HOME".to_owned(), "/home/mageyuki/.cargo".to_owned()),
            ("PATH".to_owned(), "/usr/bin:/bin".to_owned()),
            ("LC_ALL".to_owned(), "C".to_owned()),
            ("TZ".to_owned(), "UTC".to_owned()),
            (
                "HERDR_INCREMENT5_ATTEMPT_ID".to_owned(),
                attempt_id.to_owned(),
            ),
        ]) {
            controller_args.extend(["--env".to_owned(), format!("{key}={value}")]);
        }
        controller_args.push("--".to_owned());
        controller_args.extend([
            "-p".to_owned(),
            runner.canonical.to_string_lossy().into_owned(),
        ]);
        controller_args.extend(runner_argv.iter().cloned());
        empty_bootstrap_command(controller, &controller_args)
    }

    fn empty_bootstrap_command(controller: &Identity, controller_args: &[String]) -> Command {
        let mut command = Command::new("/usr/bin/bash");
        command
            .env("LD_PRELOAD", "/definitely/not/loaded.so")
            .env("BASH_ENV", "/definitely/not/sourced")
            .env_clear()
            .args(["-p", "-c", EMPTY_BOOTSTRAP_BODY, "herdr-i5-bootstrap"])
            .arg(&controller.canonical)
            .args(controller_args);
        command
    }

    pub fn source_fixture_command(
        runner: &Identity,
        tools: &[FixtureTool],
        attempt_id: Option<&str>,
        fixture_argv: &[String],
    ) -> Command {
        let controller = identity(&controller_binary());
        let bash = identity(Path::new("/usr/bin/bash"));
        let mut controller_args = Vec::new();
        push_identity(&mut controller_args, "self", &controller);
        controller_args.push("launch-runner-source-fixture".to_owned());
        push_identity(&mut controller_args, "runner-script", runner);
        for tool in tools {
            controller_args.extend([
                "--fixture-tool".to_owned(),
                format!("{}={}", tool.role, tool.requested.to_string_lossy()),
            ]);
        }
        push_identity(&mut controller_args, "program", &bash);
        for (key, value) in caller_environment(attempt_id) {
            controller_args.extend(["--env".to_owned(), format!("{key}={value}")]);
        }
        controller_args.push("--".to_owned());
        controller_args.extend([
            "-p".to_owned(),
            "-c".to_owned(),
            SOURCE_FIXTURE_BODY.to_owned(),
            "herdr-i5-source-fixture".to_owned(),
            runner.canonical.to_string_lossy().into_owned(),
        ]);
        controller_args.extend(fixture_argv.iter().cloned());
        let mut command = empty_bootstrap_command(&controller, &controller_args);
        // The production orchestrator is an async job, so SIGINT is ignored on
        // entry and Bash cannot install its INT trap. This accepted deviation
        // from plan lines 3728/3898 bounds Ctrl-C cleanup by the trial deadline;
        // TERM/HUP/USR1 are unaffected. Normalize SIGINT only for the fixture so
        // it can positively exercise the shared trap-handler logic those paths use.
        unsafe {
            command.pre_exec(|| {
                if libc::signal(libc::SIGINT, libc::SIG_DFL) == libc::SIG_ERR {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        command
    }

    pub fn run_source_fixture(attempt_id: Option<&str>, fixture_argv: &[String]) -> Output {
        let runner = identity(&runner_script());
        let tools = fixture_tools();
        source_fixture_command(&runner, &tools, attempt_id, fixture_argv)
            .output()
            .unwrap()
    }

    pub fn wait_for_source_fixture_exit(
        attempt_id: Option<&str>,
        fixture_argv: &[String],
        budget: Duration,
    ) -> std::process::ExitStatus {
        let runner = identity(&runner_script());
        let tools = fixture_tools();
        let mut command = source_fixture_command(&runner, &tools, attempt_id, fixture_argv);
        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let mut child = command.spawn().unwrap();
        let deadline = std::time::Instant::now() + budget;
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let status = child.wait().unwrap();
                panic!(
                    "source fixture did not publish a reaped exit status within {budget:?}: {status:?}"
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn assert_runner_outcome(path: &Path, exit_code: i32) -> RunnerTestOutcomeV1 {
        let outcome: RunnerTestOutcomeV1 =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(outcome.schema_version, 1);
        assert!(outcome.non_authoritative);
        assert_eq!(outcome.exit_code, exit_code);
        assert!(outcome.all_process_groups_reaped);
        outcome
    }

    pub fn write_executable(path: &Path, body: &str) {
        fs::write(path, body).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }

    pub fn copy_executable(from: &Path, to: &Path) {
        fs::copy(from, to).unwrap();
        let mut permissions = fs::metadata(to).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(to, permissions).unwrap();
    }

    pub fn frozen_runner_symlink(root: &Path) -> (PathBuf, PathBuf) {
        let first = root.join("runner-first.sh");
        copy_executable(&runner_script(), &first);
        let alias = root.join("runner-alias.sh");
        symlink(&first, &alias).unwrap();
        (alias, first)
    }

    pub fn path_has_result_v1(root: &Path) -> bool {
        fn visit(path: &Path) -> bool {
            let Ok(entries) = fs::read_dir(path) else {
                return false;
            };
            for entry in entries {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.file_name() == Some(OsStr::new("result-v1.json")) {
                    return true;
                }
                if path.is_dir() && visit(&path) {
                    return true;
                }
            }
            false
        }
        visit(root)
    }

    pub fn atomic_temporary_paths(root: &Path) -> Vec<PathBuf> {
        fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .is_some_and(|name| name.contains(".tmp."))
            })
            .collect()
    }

    pub fn process_group_exists(group: &str) -> bool {
        Command::new("/usr/bin/kill")
            .args(["-0", "--", &format!("-{group}")])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    pub fn kill_process_group(group: &str) {
        let _ = Command::new("/usr/bin/kill")
            .args(["-KILL", "--", &format!("-{group}")])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

#[cfg(target_os = "linux")]
#[test]
fn runner_library_guard_is_source_clean() {
    use reference_runner_test_support as support;
    use std::process::Command;

    // Break caught: a syntax-invalid script, loss of protected mode, a source
    // path that executes main, or a source inventory that enables `set +p`.
    let runner = support::runner_script();
    let syntax = Command::new("/usr/bin/bash")
        .env_clear()
        .args(["-p", "-n"])
        .arg(&runner)
        .status()
        .unwrap();
    assert_eq!(syntax.code(), Some(0));
    let source = std::fs::read_to_string(&runner).unwrap();
    assert!(source.starts_with("#!/usr/bin/bash -p\nset -euo pipefail\n"));
    assert!(!source.contains("set +p"));
    assert!(!support::SOURCE_FIXTURE_BODY.contains("set +p"));

    // Sourcing in library-only mode must execute no main path and spawn no job.
    let output = Command::new("/usr/bin/bash")
        .env_clear()
        .env("HERDR_PERF_RUNNER_TEST_LIBRARY_ONLY", "1")
        .args([
            "-p",
            "-c",
            "source \"$1\"; jobs -pr",
            "herdr-i5-library-guard",
        ])
        .arg(&runner)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());

    // Direct execution with the same variable is forbidden before bootstrap,
    // argument parsing, child launch, or output creation.
    let rejected = Command::new("/usr/bin/bash")
        .env_clear()
        .env("HERDR_PERF_RUNNER_TEST_LIBRARY_ONLY", "1")
        .args(["-p"])
        .arg(&runner)
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(20));
    assert_eq!(
        String::from_utf8(rejected.stderr).unwrap(),
        "error: library-only mode cannot execute main\n"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn runner_scenario_loop_continues_after_failed_status_under_nested_errexit() {
    use reference_runner_test_support as support;

    // Break caught: replacing the conditional status capture with set +e/set -e
    // lets the scenario's set -e terminate the runner before the next scenario.
    let temporary = tempfile::tempdir().unwrap();
    let calls = temporary.path().join("calls");
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "scenario-loop".to_owned(),
            "10".to_owned(),
            calls.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(10), "{output:?}");
    assert_eq!(
        std::fs::read_to_string(calls).unwrap(),
        "target\nsustained\nburst\nstartup\nidle\nfallback-rescan\ntwice-target\n"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn runner_scenario_loop_aborts_immediately_after_invalid_status_under_nested_errexit() {
    use reference_runner_test_support as support;

    // Break caught: treating InvalidArtifact as an aggregate-only status and
    // continuing into scenarios whose evidence cannot be authoritative.
    let temporary = tempfile::tempdir().unwrap();
    let calls = temporary.path().join("calls");
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "scenario-loop".to_owned(),
            "20".to_owned(),
            calls.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(20), "{output:?}");
    assert_eq!(
        std::fs::read_to_string(calls).unwrap(),
        "target\nsustained\nburst\nstartup\n"
    );
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_preflight_guards_abort_before_output_creation() {
    use reference_runner_test_support as support;
    use std::path::Path;

    // Break caught: calling validate_attempt_paths from a conditional context
    // suppresses errexit, allowing a rejected containment check to fall through
    // to output creation.
    let controller = support::identity(&support::controller_binary());
    let bash = support::identity(Path::new("/usr/bin/bash"));
    let runner = support::identity(&support::runner_script());
    let temporary = tempfile::tempdir().unwrap();
    let protected = std::fs::canonicalize(temporary.path()).unwrap();
    let forbidden = protected.join("baseline-aaaaaaaaaaaa-attempt-00000001");
    let body = r#"set -euo pipefail
source "$1"
runner_output_argument=$3
runner_stage=baseline
runner_subject=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
runner_attempt_id=00000001
repository_root=$2
worktree_roots=("$2")
auth_readlink_executable=/usr/bin/readlink
validate_attempt_paths || exit 20
/usr/bin/mkdir -- "$runner_output_root"
"#;
    let output = support::generic_controller_command(
        &controller,
        &bash,
        &BTreeMap::new(),
        &[
            "-p".to_owned(),
            "-c".to_owned(),
            body.to_owned(),
            "herdr-i5-preflight-guard".to_owned(),
            runner.canonical.to_string_lossy().into_owned(),
            protected.to_string_lossy().into_owned(),
            forbidden.to_string_lossy().into_owned(),
        ],
    )
    .output()
    .unwrap();
    assert_eq!(output.status.code(), Some(20), "{output:?}");
    assert!(!forbidden.exists(), "preflight created a forbidden root");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_authoritative_preflight_binds_manifest_before_selector() {
    use reference_runner_test_support as support;

    // Authoritative preflight cannot run on generic CI without beginning a
    // host-specific build, so this narrow source binding proves the otherwise
    // unreachable selector input is assigned and shape-checked first.
    let source = std::fs::read_to_string(support::runner_script()).unwrap();
    let assignment = source
        .lines()
        .scan(0_usize, |offset, line| {
            let start = *offset;
            *offset += line.len() + 1;
            Some((start, line))
        })
        .find_map(|(offset, line)| {
            (line.contains("canonical_manifest_path")
                && line.contains('=')
                && line.contains("repository_root")
                && line.contains("Cargo.toml"))
            .then_some(offset)
        })
        .expect("canonical Cargo.toml path is never assigned from repository_root");
    let selector = source
        .find("select_measured_binary \"$cargo_artifact_json\"")
        .expect("measured binary selector call is absent");
    assert!(assignment < selector);
    assert!(source[assignment..selector].contains("[[ -f $canonical_manifest_path"));
    assert!(source[assignment..selector].contains("! -L $canonical_manifest_path"));
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn native_controller_bootstrap_starts_empty_and_launches_exact_env() {
    use reference_runner_test_support as support;

    // Break caught: no feature-gated Controller, a non-empty first exec,
    // inherited environment, PATH lookup, or status/identity grammar drift.
    let controller = support::identity(&support::controller_binary());
    let child = support::identity(&std::env::current_exe().unwrap());
    let temporary = tempfile::tempdir().unwrap();
    let recording = temporary.path().join("recording.json");
    let child_environment = BTreeMap::from([(
        "HERDR_TEST_RECORDING_OUTPUT".to_owned(),
        recording.to_string_lossy().into_owned(),
    )]);
    let child_argv = vec![
        "native_controller_recording_child".to_owned(),
        "--exact".to_owned(),
        "--ignored".to_owned(),
        "--nocapture".to_owned(),
        "--test-threads=1".to_owned(),
    ];
    let output =
        support::generic_controller_command(&controller, &child, &child_environment, &child_argv)
            .output()
            .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let observed: BTreeMap<String, String> =
        serde_json::from_slice(&std::fs::read(&recording).unwrap()).unwrap();
    assert_eq!(observed, child_environment);

    let mut wrong = controller.clone();
    wrong.sha256.replace_range(0..1, "0");
    if wrong.sha256 == controller.sha256 {
        wrong.sha256.replace_range(0..1, "1");
    }
    let rejected =
        support::generic_controller_command(&wrong, &child, &child_environment, &child_argv)
            .output()
            .unwrap();
    assert_eq!(rejected.status.code(), Some(20));
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn source_fixture_uses_frozen_canonical_runner_operand() {
    use reference_runner_test_support as support;
    use std::os::unix::fs::symlink;

    // Break caught: sourcing a requested alias instead of the frozen canonical
    // operand, or failing to reject requested-to-canonical drift before Bash.
    let temporary = tempfile::tempdir().unwrap();
    let (alias, first) = support::frozen_runner_symlink(temporary.path());
    let frozen = support::identity(&alias);
    assert_eq!(frozen.canonical, first);
    let outcome_path = temporary.path().join("canonical-outcome.json");
    let fixture_argv = vec![
        "orchestration".to_owned(),
        "attempt-check".to_owned(),
        outcome_path.to_string_lossy().into_owned(),
    ];
    let tools = support::fixture_tools();
    let success = support::source_fixture_command(&frozen, &tools, Some("00000001"), &fixture_argv)
        .output()
        .unwrap();
    assert_eq!(success.status.code(), Some(0), "{success:?}");
    support::assert_runner_outcome(&outcome_path, 0);

    let second = temporary.path().join("runner-second.sh");
    support::copy_executable(&support::runner_script(), &second);
    std::fs::remove_file(&alias).unwrap();
    symlink(&second, &alias).unwrap();
    let rejected_path = temporary.path().join("rejected-outcome.json");
    let rejected_argv = vec![
        "orchestration".to_owned(),
        "attempt-check".to_owned(),
        rejected_path.to_string_lossy().into_owned(),
    ];
    let rejected =
        support::source_fixture_command(&frozen, &tools, Some("00000001"), &rejected_argv)
            .output()
            .unwrap();
    assert_eq!(rejected.status.code(), Some(20));
    assert!(!rejected_path.exists());
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn source_fixture_revalidates_identity_before_every_use() {
    use reference_runner_test_support as support;
    use std::os::unix::fs::symlink;

    // Break caught: a successful first identity check must not cache trust
    // across a later requested-path mutation.
    let temporary = tempfile::tempdir().unwrap();
    let tool_root = temporary.path().join("tool");
    std::fs::create_dir(&tool_root).unwrap();
    let env_alias = tool_root.join("env");
    symlink("/usr/bin/env", &env_alias).unwrap();
    let marker = temporary.path().join("mutation-ran");
    let mutator = temporary.path().join("mutator.sh");
    support::write_executable(
        &mutator,
        &format!(
            "#!/usr/bin/bash -p\nset -euo pipefail\n/usr/bin/unlink -- \"$1\"\n/usr/bin/ln -s /usr/bin/false \"$1\"\nbuiltin printf x >'{}'\n",
            marker.display()
        ),
    );
    let outcome = temporary.path().join("outcome.json");
    let runner = support::identity(&support::runner_script());
    let mut tools = support::fixture_tools();
    tools
        .iter_mut()
        .find(|tool| tool.role == "env")
        .unwrap()
        .requested = env_alias.clone();
    let output = support::source_fixture_command(
        &runner,
        &tools,
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "identity-revalidation".to_owned(),
            outcome.to_string_lossy().into_owned(),
            mutator.to_string_lossy().into_owned(),
            env_alias.to_string_lossy().into_owned(),
        ],
    )
    .output()
    .unwrap();
    assert_eq!(output.status.code(), Some(20), "{output:?}");
    assert!(
        marker.exists(),
        "fixture never reached the between-use mutation"
    );
    assert!(
        !outcome.exists(),
        "cached trust allowed publication after drift"
    );
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_fixture_rejects_uncontained_attempt_id() {
    use reference_runner_test_support as support;

    // Break caught: accepting a missing, malformed, zero, or still-exported
    // attempt identifier at any child-launch boundary.
    for attempt in [None, Some("00000000"), Some("0000000a"), Some("123456789")] {
        let temporary = tempfile::tempdir().unwrap();
        let outcome = temporary.path().join("outcome.json");
        let output = support::run_source_fixture(
            attempt,
            &[
                "orchestration".to_owned(),
                "attempt-check".to_owned(),
                outcome.to_string_lossy().into_owned(),
            ],
        );
        assert_eq!(output.status.code(), Some(20), "attempt={attempt:?}");
        assert!(!outcome.exists());
    }

    let temporary = tempfile::tempdir().unwrap();
    let outcome = temporary.path().join("outcome.json");
    let child_environment = temporary.path().join("child-environment.txt");
    let bootstrap_environment = temporary.path().join("bootstrap-environment.txt");
    let recording_readlink = temporary.path().join("readlink");
    support::write_executable(
        &recording_readlink,
        &format!(
            "#!/usr/bin/bash -p\n/usr/bin/env >'{}'\nbuiltin exec /usr/bin/readlink \"$@\"\n",
            bootstrap_environment.display()
        ),
    );
    let mut tools = support::fixture_tools();
    tools
        .iter_mut()
        .find(|tool| tool.role == "readlink")
        .unwrap()
        .requested = recording_readlink;
    let runner = support::identity(&support::runner_script());
    let output = support::source_fixture_command(
        &runner,
        &tools,
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "attempt-check".to_owned(),
            outcome.to_string_lossy().into_owned(),
            child_environment.to_string_lossy().into_owned(),
        ],
    )
    .output()
    .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    support::assert_runner_outcome(&outcome, 0);
    assert!(
        !std::fs::read_to_string(child_environment)
            .unwrap()
            .contains("HERDR_INCREMENT5_ATTEMPT_ID")
    );
    assert!(
        !std::fs::read_to_string(bootstrap_environment)
            .unwrap()
            .contains("HERDR_INCREMENT5_ATTEMPT_ID")
    );
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn wait_process_pair_reaps_a_preexited_supervisor_before_blocking() {
    use reference_runner_test_support as support;

    // Break caught: Bash <= 5.2 `wait -n` skips children that exited before
    // the call, so the live workers used to outrun an already-dead watchdog.
    let temporary = tempfile::tempdir().unwrap();
    let measured = temporary.path().join("measured.sh");
    let observer = temporary.path().join("observer.sh");
    let measured_natural_exit = temporary.path().join("measured-natural-exit");
    let observer_natural_exit = temporary.path().join("observer-natural-exit");
    support::write_executable(
        &measured,
        &format!(
            "#!/usr/bin/bash -p\nset -euo pipefail\n/usr/bin/sleep 2\n: >'{}'\n",
            measured_natural_exit.display()
        ),
    );
    support::write_executable(
        &observer,
        &format!(
            "#!/usr/bin/bash -p\nset -euo pipefail\n/usr/bin/sleep 2\n: >'{}'\n",
            observer_natural_exit.display()
        ),
    );
    let outcome = temporary.path().join("outcome.json");
    let groups = temporary.path().join("groups.txt");
    let status = temporary.path().join("trial-status");
    let trap_marker = temporary.path().join("trap-marker");
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "timeout".to_owned(),
            outcome.to_string_lossy().into_owned(),
            groups.to_string_lossy().into_owned(),
            status.to_string_lossy().into_owned(),
            measured.to_string_lossy().into_owned(),
            observer.to_string_lossy().into_owned(),
            trap_marker.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(20), "{output:?}");
    assert_eq!(
        std::fs::read_to_string(status)
            .unwrap_or_else(|error| panic!("trial status was absent: {error}: {output:?}")),
        "failed:124\n"
    );
    assert!(!measured_natural_exit.exists());
    assert!(!observer_natural_exit.exists());
    for pid in std::fs::read_to_string(groups).unwrap().split_whitespace() {
        assert!(!PathBuf::from(format!("/proc/{pid}")).exists());
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_fixture_reaps_timeout_and_signal_groups() {
    use reference_runner_test_support as support;

    // Break caught: a timeout/signal path that leaves either process group live,
    // reports a default-death status without executing its trap body, skips
    // wait/reap, publishes before cleanup, or creates promotable evidence.
    for (case, expected_status, expect_trap_marker) in [
        ("signal-int-handshake", "failed:130\n", true),
        ("signal-term-handshake", "failed:143\n", true),
        ("signal-hup-handshake", "failed:143\n", true),
        ("signal-usr1-handshake", "failed:124\n", true),
        ("signal-int-after-observer", "failed:130\n", true),
        ("signal-term-after-observer", "failed:143\n", true),
        ("signal-hup-after-observer", "failed:143\n", true),
        ("timeout", "failed:124\n", false),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let measured = temporary.path().join("measured.sh");
        let observer = temporary.path().join("observer.sh");
        support::write_executable(
            &measured,
            "#!/usr/bin/bash -p\nset -euo pipefail\n/usr/bin/sleep 300\n",
        );
        support::write_executable(
            &observer,
            "#!/usr/bin/bash -p\nset -euo pipefail\n/usr/bin/sleep 300\n",
        );
        let outcome = temporary.path().join("outcome.json");
        let groups = temporary.path().join("groups.txt");
        let status = temporary.path().join("trial-status");
        let trap_marker = temporary.path().join("trap-marker");
        let exit_status = support::wait_for_source_fixture_exit(
            Some("00000001"),
            &[
                "orchestration".to_owned(),
                case.to_owned(),
                outcome.to_string_lossy().into_owned(),
                groups.to_string_lossy().into_owned(),
                status.to_string_lossy().into_owned(),
                measured.to_string_lossy().into_owned(),
                observer.to_string_lossy().into_owned(),
                trap_marker.to_string_lossy().into_owned(),
            ],
            Duration::from_secs(120),
        );
        assert_eq!(exit_status.code(), Some(20), "case={case}: {exit_status:?}");
        assert_eq!(
            trap_marker.is_file(),
            expect_trap_marker,
            "case={case} trap execution marker mismatch"
        );
        assert!(
            outcome.exists(),
            "case={case} did not atomically publish its non-authoritative outcome"
        );
        support::assert_runner_outcome(&outcome, 20);
        for pid in std::fs::read_to_string(groups).unwrap().split_whitespace() {
            assert!(
                !PathBuf::from(format!("/proc/{pid}")).exists(),
                "live pid {pid}"
            );
        }
        let published_status = std::fs::read_to_string(status).unwrap();
        assert_eq!(
            published_status, expected_status,
            "case={case} published the wrong exact status"
        );
        assert!(!support::path_has_result_v1(temporary.path()));
    }

    let temporary = tempfile::tempdir().unwrap();
    let outcome = temporary.path().join("cleanup-failure.json");
    let status = temporary.path().join("trial-status");
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "cleanup-failure".to_owned(),
            outcome.to_string_lossy().into_owned(),
            status.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(20));
    assert!(
        outcome.exists(),
        "cleanup failure was not reported atomically"
    );
    let reported: RunnerTestOutcomeV1 =
        serde_json::from_slice(&std::fs::read(&outcome).unwrap()).unwrap();
    assert_eq!(reported.exit_code, 20);
    assert!(!reported.all_process_groups_reaped);
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn orchestration_signal_traps_are_self_contained_across_reexec() {
    use reference_runner_test_support as support;
    use std::process::{Command, Stdio};

    // Break caught: a trap body calls a shell function that is absent after a
    // fresh protected Bash re-exec, so the shell reports the intended status
    // without executing the trap marker publication. The ready marker also
    // prevents the test from signalling before the re-exec installed its trap.
    let temporary = tempfile::tempdir().unwrap();
    let marker = temporary.path().join("trap-marker");
    let ready = temporary.path().join("trap-ready");
    let mut child = Command::new("/usr/bin/bash")
        .env_clear()
        .args([
            "-p",
            "-c",
            r#"set -euo pipefail
source "$1"
marker=$2
ready=$3
trap_command=$(install_orchestration_signal_traps; trap -p HUP)
export HERDR_PERF_RUNNER_TEST_TRAP_MARKER=$marker
exec /usr/bin/bash -p -c \
  "$trap_command"$'\n''builtin printf "%s\n" "$BASHPID" >"$1"; while :; do :; done' \
  herdr-i5-trap-reexec-child "$ready""#,
            "herdr-i5-trap-reexec",
        ])
        .arg(support::runner_script())
        .arg(&marker)
        .arg(&ready)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let readiness_deadline = std::time::Instant::now() + Duration::from_secs(300);
    let expected_ready = format!("{}\n", child.id());
    loop {
        match std::fs::read_to_string(&ready) {
            Ok(published) if published == expected_ready => break,
            Ok(published) if published.ends_with('\n') => {
                let _ = child.kill();
                let status = child.wait().unwrap();
                panic!(
                    "re-exec published the wrong signal target {published:?}, expected {expected_ready:?}: {status:?}"
                );
            }
            Ok(_) | Err(_) => {}
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("re-exec exited before publishing trap readiness: {status:?}");
        }
        if std::time::Instant::now() >= readiness_deadline {
            let _ = child.kill();
            let status = child.wait().unwrap();
            panic!("re-exec did not publish trap readiness before deadline: {status:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert_eq!(
        unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGHUP) },
        0,
        "failed to signal ready re-exec: {}",
        std::io::Error::last_os_error()
    );
    let exit_deadline = std::time::Instant::now() + Duration::from_secs(300);
    let settled_status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() >= exit_deadline {
            let _ = child.kill();
            let status = child.wait().unwrap();
            panic!("signalled re-exec did not settle before deadline: {status:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(settled_status.code(), Some(143), "{settled_status:?}");
    assert!(marker.is_file(), "trap marker was not published");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn cleanup_process_groups_bounds_a_missed_group_signal() {
    use reference_runner_test_support as support;

    // Break caught: swallowing a failed negative-PGID signal and then waiting
    // unconditionally for the still-live direct child.
    let temporary = tempfile::tempdir().unwrap();
    let outcome = temporary.path().join("cleanup-outcome.json");
    let status = temporary.path().join("trial-status");
    let ready = temporary.path().join("cleanup-ready");
    let runner = support::identity(&support::runner_script());
    let tools = support::fixture_tools();
    let mut fixture_command = support::source_fixture_command(
        &runner,
        &tools,
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "cleanup-missed-group".to_owned(),
            outcome.to_string_lossy().into_owned(),
            status.to_string_lossy().into_owned(),
            ready.to_string_lossy().into_owned(),
        ],
    );
    fixture_command.stderr(std::process::Stdio::piped());
    let mut fixture = fixture_command.spawn().unwrap();

    let mut early_status = None;
    for _ in 0..6000 {
        if ready.exists() {
            break;
        }
        if let Some(value) = fixture.try_wait().unwrap() {
            early_status = Some(value);
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut early_stderr = String::new();
    if early_status.is_some() && !ready.exists() {
        use std::io::Read as _;
        fixture
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut early_stderr)
            .unwrap();
    }
    assert!(
        ready.exists(),
        "fixture exited before exercising the missed-group cleanup: {early_status:?}, stderr={early_stderr:?}"
    );

    let missed_group_child = std::fs::read_to_string(&ready).unwrap();
    let missed_group_child = missed_group_child.trim();
    let started = std::time::Instant::now();
    let mut completed = None;
    for _ in 0..6000 {
        if let Some(value) = fixture.try_wait().unwrap() {
            completed = Some(value);
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let completed_in_bound = completed.is_some();
    if !completed_in_bound {
        let _ = std::process::Command::new("/usr/bin/kill")
            .args(["-KILL", missed_group_child])
            .status();
        let _ = fixture.wait();
    }
    assert!(
        completed_in_bound,
        "cleanup blocked on child {missed_group_child} for {:?}",
        started.elapsed()
    );
    assert_eq!(completed.unwrap().code(), Some(20));
    assert_eq!(std::fs::read_to_string(status).unwrap(), "failed:20\n");
    let reported: RunnerTestOutcomeV1 =
        serde_json::from_slice(&std::fs::read(&outcome).unwrap()).unwrap();
    assert_eq!(reported.exit_code, 20);
    assert!(!reported.all_process_groups_reaped);
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn trial_scratch_root_is_isolated_from_evidence() {
    use reference_runner_test_support as support;

    // Break caught: passing the raw thirteen-artifact directory itself as the
    // measured process scratch root.
    let temporary = tempfile::tempdir().unwrap();
    let trial_root = temporary.path().join("trial-0001");
    let capture = temporary.path().join("scratch-path");
    let outcome = temporary.path().join("outcome.json");
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "scratch-root".to_owned(),
            outcome.to_string_lossy().into_owned(),
            trial_root.to_string_lossy().into_owned(),
            capture.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        std::fs::read_to_string(capture).unwrap(),
        format!("{}/scratch\n", trial_root.display())
    );
    assert!(trial_root.join("scratch").is_dir());
    support::assert_runner_outcome(&outcome, 0);
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn source_fixture_write_sites_reject_reference_result_basename() {
    use reference_runner_test_support as support;

    // Break caught: any fixture-local direct write bypasses the non-promotable
    // basename guard and creates result-v1.json before rejection.
    let temporary = tempfile::tempdir().unwrap();
    let trial_root = temporary.path().join("trial-0001");
    let forbidden = temporary.path().join("result-v1.json");
    let outcome = temporary.path().join("outcome.json");
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "scratch-root".to_owned(),
            outcome.to_string_lossy().into_owned(),
            trial_root.to_string_lossy().into_owned(),
            forbidden.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(20), "{output:?}");
    assert!(!forbidden.exists());
    assert!(
        !trial_root.exists(),
        "fixture wrote before validating its output"
    );
    assert!(!outcome.exists());
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn baseline_set_is_typed_stage_and_identity_validated_up_front() {
    use reference_runner_test_support as support;

    // Break caught: existence-only baseline checks admit wrong-stage documents
    // or wrong-scenario documents, or individually valid baseline documents
    // with disagreeing baseline IDs.
    #[derive(Clone, Copy)]
    enum Mutation {
        None,
        BaselineId(usize),
        WrongStage(usize),
        WrongScenario(usize),
    }
    let write_set = |root: &std::path::Path, mutation: Mutation| {
        for (index, (scenario, mapped)) in [
            (ScenarioV1::Target, "target"),
            (ScenarioV1::Sustained, "sustained"),
            (ScenarioV1::Burst, "burst"),
            (ScenarioV1::Startup, "startup"),
            (ScenarioV1::Idle, "idle"),
            (ScenarioV1::FallbackRescan, "fallback_rescan"),
            (ScenarioV1::TwiceTarget, "twice_target"),
        ]
        .into_iter()
        .enumerate()
        {
            let scenario_root = root.join(mapped);
            std::fs::create_dir_all(scenario_root.join("trial-0001")).unwrap();
            let outcome_scenario = if matches!(mutation, Mutation::WrongScenario(value) if value == index)
            {
                ScenarioV1::Target
            } else {
                scenario
            };
            let outcome_stage = if matches!(mutation, Mutation::WrongStage(value) if value == index)
            {
                MeasurementStageV1::Final
            } else {
                MeasurementStageV1::Baseline
            };
            let mut outcome = synthetic_result(outcome_scenario, outcome_stage);
            if matches!(mutation, Mutation::BaselineId(value) if value == index) {
                outcome.document_mut().harness_sha = "c".repeat(40);
                outcome.document_mut().baseline_id = format!(
                    "sha256:v1:{BASELINE_SUBJECT_SHA}:{}:{WORKLOAD_SCHEMA_V1_SHA256}",
                    "c".repeat(40)
                );
                outcome.validate().unwrap();
            }
            atomic_write_reference_outcome(&scenario_root.join("result-v1.json"), &outcome)
                .unwrap();
        }
    };

    let callback = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
    for (mutation, expected) in [
        (Mutation::None, 0),
        (Mutation::BaselineId(3), 20),
        (Mutation::WrongStage(3), 20),
        (Mutation::WrongScenario(3), 20),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let baseline_root = temporary.path().join("baseline");
        std::fs::create_dir(&baseline_root).unwrap();
        write_set(&baseline_root, mutation);
        let outcome = temporary.path().join("outcome.json");
        let output = support::run_source_fixture(
            Some("00000001"),
            &[
                "orchestration".to_owned(),
                "baseline-set".to_owned(),
                outcome.to_string_lossy().into_owned(),
                baseline_root.to_string_lossy().into_owned(),
                callback.to_string_lossy().into_owned(),
            ],
        );
        assert_eq!(output.status.code(), Some(expected), "{output:?}");
        assert_eq!(outcome.exists(), expected == 0);
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn outer_runtime_trap_reaps_live_trial_and_removes_owned_socket_on_signal() {
    use reference_runner_test_support as support;

    // Break caught: a TERM after a group and socket are live only clears the
    // directory path, without reaping the group or unlinking the frozen socket.
    let temporary = tempfile::tempdir().unwrap();
    let capture = temporary.path().join("runtime-path");
    let callback = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "outer-runtime-signal".to_owned(),
            capture.to_string_lossy().into_owned(),
            callback.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(20));
    assert!(
        capture.exists(),
        "fixture did not reach runtime-dir creation: {output:?}"
    );
    let captured = std::fs::read_to_string(capture).unwrap();
    let mut lines = captured.lines();
    let runtime = PathBuf::from(lines.next().unwrap());
    let socket = PathBuf::from(lines.next().unwrap());
    let group = lines.next().unwrap();
    assert!(lines.next().is_none());
    assert!(!support::process_group_exists(group), "live group {group}");
    assert!(!socket.exists());
    assert!(!runtime.exists());
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn source_fixture_executes_production_nested_orchestration_body() {
    use reference_runner_test_support as support;

    // Break caught: the source seam reimplements orchestration instead of
    // executing run_trial_process_tree's 31-operand nested body, environments,
    // handshake/socket freeze, watchdog, and cleanup_trial path.
    let harness = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
    for (scenario, deadline, expected_status, expected_trial_status) in [
        ("target", "60", 0, "ok:0\n"),
        ("idle", "1", 20, "failed:124\n"),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let driver = temporary.path().join("fixture-driver");
        support::write_executable(
            &driver,
            &format!(
                "#!/usr/bin/bash -p\nset -euo pipefail\ncase ${{1-}} in\n  reference_profile_entrypoint) exec '{}' fixture_nested_measured_helper --exact --ignored --test-threads=1 ;;\n  reference_profile_process_tree_observer) exec '{}' fixture_nested_observer_helper --exact --ignored --test-threads=1 ;;\n  *) exit 20 ;;\nesac\n",
                harness.display(),
                harness.display()
            ),
        );
        let taskset = temporary.path().join("taskset");
        support::write_executable(
            &taskset,
            "#!/usr/bin/bash -p\nset -euo pipefail\n[[ ${1-} == -c && $# -ge 3 ]] || exit 20\nshift 2\nexec \"$@\"\n",
        );
        let prlimit = temporary.path().join("prlimit");
        support::write_executable(
            &prlimit,
            "#!/usr/bin/bash -p\nset -euo pipefail\n[[ ${1-} == --as=17179869184 && $# -ge 2 ]] || exit 20\nshift\nexec \"$@\"\n",
        );
        let time = temporary.path().join("time");
        support::write_executable(
            &time,
            "#!/usr/bin/bash -p\nset -euo pipefail\n[[ ${1-} == -v && ${2-} == -o && $# -ge 4 ]] || exit 20\ntime_output=$3\nshift 3\nbuiltin printf '%s\\n' 'fixture GNU time' >\"$time_output\"\nexec \"$@\"\n",
        );
        let pidstat = temporary.path().join("pidstat");
        support::write_executable(
            &pidstat,
            "#!/usr/bin/bash -p\nset -euo pipefail\nwhile [[ $# -gt 0 && $1 != -e ]]; do shift; done\n[[ ${1-} == -e ]] || exit 20\nshift\nset +e\n\"$@\"\nchild_status=$?\nset -e\nbuiltin printf '%s\\n' '{\"sysstat\":{\"hosts\":[]}}'\nexit \"$child_status\"\n",
        );
        let mut tools = support::fixture_tools();
        for (role, executable) in [
            ("taskset", &taskset),
            ("prlimit", &prlimit),
            ("time", &time),
            ("pidstat", &pidstat),
        ] {
            tools
                .iter_mut()
                .find(|tool| tool.role == role)
                .unwrap()
                .requested = executable.clone();
        }

        let trial_root = temporary.path().join("trial-0001");
        let runtime_capture = temporary.path().join("runtime.txt");
        let outcome = temporary.path().join("outcome.json");
        let runner = support::identity(&support::runner_script());
        let hung_phase_started = (scenario == "idle").then(std::time::Instant::now);
        let output = support::source_fixture_command(
            &runner,
            &tools,
            Some("00000001"),
            &[
                "orchestration".to_owned(),
                "nested-trial".to_owned(),
                outcome.to_string_lossy().into_owned(),
                trial_root.to_string_lossy().into_owned(),
                runtime_capture.to_string_lossy().into_owned(),
                driver.to_string_lossy().into_owned(),
                scenario.to_owned(),
                deadline.to_owned(),
                "6000".to_owned(),
            ],
        )
        .output()
        .unwrap();
        if let Some(started) = hung_phase_started {
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "hung-child phase exceeded the watchdog allowance: {:?}",
                started.elapsed()
            );
        }
        assert_eq!(
            output.status.code(),
            Some(expected_status),
            "scenario={scenario}: {output:?}"
        );
        support::assert_runner_outcome(&outcome, expected_status);
        assert_eq!(
            std::fs::read_to_string(trial_root.join("trial-status")).unwrap(),
            expected_trial_status
        );
        if expected_status == 0 {
            let orchestrator_stderr =
                std::fs::read_to_string(trial_root.join("pidstat-stderr")).unwrap();
            assert_eq!(
                orchestrator_stderr, "",
                "successful production orchestration emitted stderr"
            );
            assert!(
                trial_root.join("stdout").is_file(),
                "measured stdout did not use the composer-owned basename"
            );
            assert!(
                trial_root.join("stderr").is_file(),
                "measured stderr did not use the composer-owned basename"
            );
            assert!(!trial_root.join("measured-stdout").exists());
            assert!(!trial_root.join("measured-stderr").exists());
        }
        let runtime_bytes = std::fs::read_to_string(runtime_capture).unwrap();
        let mut runtime_lines = runtime_bytes.lines();
        let runtime = PathBuf::from(runtime_lines.next().unwrap());
        let socket = PathBuf::from(runtime_lines.next().unwrap());
        assert!(runtime_lines.next().is_none());
        assert!(!runtime.exists());
        assert!(!socket.exists());

        for capture_path in [
            trial_root.join("harness.json"),
            trial_root.join("process-tree.json"),
        ] {
            let capture: serde_json::Value =
                serde_json::from_slice(&std::fs::read(capture_path).unwrap()).unwrap();
            let pid = capture["pid"].as_u64().unwrap();
            assert!(!PathBuf::from(format!("/proc/{pid}")).exists());
        }
        let measured: serde_json::Value =
            serde_json::from_slice(&std::fs::read(trial_root.join("harness.json")).unwrap())
                .unwrap();
        let mut measured_environment: BTreeMap<String, String> =
            serde_json::from_value(measured["environment"].clone()).unwrap();
        assert!(measured_environment.remove("PWD").is_some());
        assert!(measured_environment.remove("SHLVL").is_some());
        assert_eq!(measured_environment.len(), 13);
        assert_eq!(measured_environment["HERDR_PERF_SCENARIO"], scenario);
        assert_eq!(measured_environment["HERDR_PERF_STAGE"], "baseline");
        assert_eq!(
            measured_environment["HERDR_PERF_SCRATCH_ROOT"],
            trial_root.join("scratch").to_string_lossy()
        );
        assert!(!measured_environment.contains_key("HERDR_PERF_BASELINE_RESULTS_ROOT"));
        assert_eq!(
            measured_environment["HERDR_PERF_OBSERVER_CONTROL_SOCKET"],
            socket.to_string_lossy()
        );
        let observer: serde_json::Value =
            serde_json::from_slice(&std::fs::read(trial_root.join("process-tree.json")).unwrap())
                .unwrap();
        let mut observer_environment: BTreeMap<String, String> =
            serde_json::from_value(observer["environment"].clone()).unwrap();
        assert!(observer_environment.remove("PWD").is_some());
        assert!(observer_environment.remove("SHLVL").is_some());
        assert_eq!(observer_environment.len(), 13);
        assert_eq!(observer_environment["HERDR_PERF_SCENARIO"], scenario);
        assert_eq!(
            observer_environment["HERDR_PERF_OBSERVER_CONTROL_SOCKET"],
            socket.to_string_lossy()
        );
        assert_eq!(
            observer_environment["HERDR_PERF_PROCESS_TREE_OUTPUT"],
            trial_root.join("process-tree.json").to_string_lossy()
        );
        assert!(!support::path_has_result_v1(temporary.path()));
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_fixture_aggregates_closed_statuses_and_promotes_atomically() {
    use reference_runner_test_support as support;

    // Break caught: boolean status collapse, continuing after invalidity, a
    // non-atomic fixture promotion, or shell promotion of reference evidence.
    for (statuses, expected, processed) in [
        (&[0_i32, 0][..], 0, 2_usize),
        (&[0, 10, 0][..], 10, 3),
        (&[10, 20, 0][..], 20, 2),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let outcome = temporary.path().join("runner-test-outcome-v1.json");
        let mut argv = vec![
            "orchestration".to_owned(),
            "aggregate".to_owned(),
            outcome.to_string_lossy().into_owned(),
        ];
        argv.extend(statuses.iter().map(ToString::to_string));
        let output = support::run_source_fixture(Some("00000001"), &argv);
        assert_eq!(output.status.code(), Some(expected), "{output:?}");
        support::assert_runner_outcome(&outcome, expected);
        assert_eq!(
            std::fs::read_to_string(outcome.with_extension("processed"))
                .unwrap()
                .trim(),
            processed.to_string()
        );
        assert!(
            support::atomic_temporary_paths(temporary.path()).is_empty(),
            "fixture promotion left a PID-qualified temporary behind"
        );
        assert!(!support::path_has_result_v1(temporary.path()));
    }

    for unexpected in [101, 137] {
        let fixture = RawFixture::new();
        let candidate = fixture.output_path("candidate-v1.json");
        std::fs::write(&candidate, b"partial candidate\n").unwrap();
        let output = fixture.output_path("result-v1.json");
        let request = ValidateRequestV1 {
            raw_root: fixture.root.path().to_path_buf(),
            candidate,
            output: output.clone(),
            measurement_stage: MeasurementStageV1::Baseline,
            scenario: ScenarioV1::Sustained,
            production_subject_sha: BASELINE_SUBJECT_SHA.to_owned(),
            preflight_head: SYNTHETIC_HARNESS_SHA.to_owned(),
            composer_status: format!("unexpected:{unexpected}"),
            trial_status: "all-ok".to_owned(),
            baseline_results_root: None,
        };
        assert_eq!(validate_reference_outcome_impl(&request).unwrap(), 20);
        assert_eq!(
            read_and_validate_reference_outcome(&output, AmendedLegacyMode::Off)
                .unwrap()
                .outcome
                .failure_reasons(),
            &[FailureReasonV1::CommandFailed]
        );
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_rejects_worktree_output_under_clean_first_exec() {
    use reference_runner_test_support as support;

    // Break caught: output containment checked after preflight/output creation,
    // copied predicates, or a non-closed diagnostic/status.
    let worktree = std::fs::canonicalize(support::manifest_root()).unwrap();
    let forbidden = worktree.join("baseline-ffca965af965-attempt-00000001");
    assert!(!forbidden.exists());
    let temporary = tempfile::tempdir().unwrap();
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "output-containment".to_owned(),
            worktree.to_string_lossy().into_owned(),
            "1".to_owned(),
            worktree.to_string_lossy().into_owned(),
            forbidden.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(20));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "error: --output-dir must be outside the repository and all linked worktrees\n"
    );
    assert!(!forbidden.exists());
    assert!(!support::path_has_result_v1(temporary.path()));

    let root_guard = temporary.path().join("root-guard-output");
    let root_output = support::run_source_fixture(
        Some("00000001"),
        &[
            "output-containment".to_owned(),
            "/".to_owned(),
            "1".to_owned(),
            worktree.to_string_lossy().into_owned(),
            root_guard.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(root_output.status.code(), Some(20));
    assert_eq!(
        String::from_utf8(root_output.stderr).unwrap(),
        "error: --output-dir must be outside the repository and all linked worktrees\n"
    );
    assert!(!root_guard.exists());
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn source_fixture_inventory_is_portable_and_role_closed() {
    use reference_runner_test_support as support;
    use std::os::unix::fs::PermissionsExt;

    // Break caught: missing/extra/reordered roles, workstation tool paths, or
    // role lookup through PATH reaching Bash.
    let temporary = tempfile::tempdir().unwrap();
    let safe_output = temporary.path().join("safe-output");
    let worktree = std::fs::canonicalize(support::manifest_root()).unwrap();
    let argv = vec![
        "output-containment".to_owned(),
        worktree.to_string_lossy().into_owned(),
        "1".to_owned(),
        worktree.to_string_lossy().into_owned(),
        safe_output.to_string_lossy().into_owned(),
    ];
    let runner = support::identity(&support::runner_script());
    for (case, forced_shims) in [("normal", &[][..]), ("shimmed-env", &["env"][..])] {
        let portable_tools = support::fixture_tools_with_forced_shims(forced_shims);
        for tool in portable_tools.iter() {
            let metadata = std::fs::metadata(&tool.requested);
            assert!(
                metadata.as_ref().is_ok_and(|value| {
                    value.is_file() && value.permissions().mode() & 0o111 != 0
                }),
                "{case} fixture role {} has no executable or shim at {}",
                tool.role,
                tool.requested.display()
            );
        }
        assert_ne!(
            portable_tools
                .iter()
                .find(|tool| tool.role == "pidstat")
                .unwrap()
                .requested,
            PathBuf::from("/usr/bin/pidstat"),
            "pidstat must be a test-owned shim so calibration is portable and deterministic"
        );
        if case == "shimmed-env" {
            assert_ne!(
                portable_tools[0].requested,
                PathBuf::from("/usr/bin/env"),
                "forced env role must exercise the portable shim path"
            );
        }
        let valid =
            support::source_fixture_command(&runner, &portable_tools, Some("00000001"), &argv)
                .output()
                .unwrap();
        assert_eq!(valid.status.code(), Some(0), "{case}: {valid:?}");
        assert!(!safe_output.exists());

        let make_tools = || support::fixture_tools_with_forced_shims(forced_shims);
        let mut mutations = Vec::new();
        let mut missing = make_tools();
        missing.pop();
        mutations.push(missing);
        let mut reordered = make_tools();
        reordered.swap(0, 1);
        mutations.push(reordered);
        let mut extra = make_tools();
        extra.push(support::FixtureTool {
            role: "true".to_owned(),
            requested: PathBuf::from("/usr/bin/true"),
        });
        mutations.push(extra);
        let mut workstation = make_tools();
        workstation[0].requested = PathBuf::from("/home/mageyuki/.herdr-i5-task7-absent/env");
        mutations.push(workstation);

        for (index, tools) in mutations.iter().enumerate() {
            let marker = temporary
                .path()
                .join(format!("{case}-mutation-{index}.json"));
            let mutation_argv = vec![
                "orchestration".to_owned(),
                "attempt-check".to_owned(),
                marker.to_string_lossy().into_owned(),
            ];
            let rejected =
                support::source_fixture_command(&runner, tools, Some("00000001"), &mutation_argv)
                    .output()
                    .unwrap();
            assert_eq!(rejected.status.code(), Some(20), "{case} mutation {index}");
            if index == 3 {
                assert_eq!(
                    String::from_utf8(rejected.stderr).unwrap(),
                    "error: source fixture tool used a workstation path\n"
                );
            }
            assert!(!marker.exists());
        }
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn source_fixture_tool_paths_are_role_bound_and_fail_closed() {
    use reference_runner_test_support as support;

    // Break caught: substituting an unrelated executable, accepting a
    // non-absolute path, or deriving trust from an absent/non-executable tool.
    let temporary = tempfile::tempdir().unwrap();
    let non_executable_parent = temporary.path().join("non-executable");
    std::fs::create_dir(&non_executable_parent).unwrap();
    let non_executable = non_executable_parent.join("env");
    std::fs::write(&non_executable, b"not executable\n").unwrap();
    let absent = temporary.path().join("absent").join("env");
    let rows = [
        (
            "wrong-but-plausible substitution",
            PathBuf::from("/usr/bin/true"),
            "error: source fixture tool basename disagreed with role\n",
        ),
        (
            "relative path",
            PathBuf::from("env"),
            "error: requested tool path was not absolute\n",
        ),
        (
            "absent path",
            absent,
            "error: tool canonicalization failed\n",
        ),
        (
            "non-executable path",
            non_executable,
            "error: tool was not a regular executable\n",
        ),
    ];
    let runner = support::identity(&support::runner_script());

    for (case, requested, expected_stderr) in rows {
        let mut tools = support::fixture_tools();
        tools[0].requested = requested;
        let marker = temporary.path().join(format!("{case}.json"));
        let rejected = support::source_fixture_command(
            &runner,
            &tools,
            Some("00000001"),
            &[
                "orchestration".to_owned(),
                "attempt-check".to_owned(),
                marker.to_string_lossy().into_owned(),
            ],
        )
        .output()
        .unwrap();
        assert_eq!(rejected.status.code(), Some(20), "{case}: {rejected:?}");
        assert_eq!(
            String::from_utf8(rejected.stderr).unwrap(),
            expected_stderr,
            "{case}"
        );
        assert!(!marker.exists(), "{case}");
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_control_recorder_applies_socket_shape_predicate() {
    use reference_runner_test_support as support;

    // Break caught: the recorder accepts a socket spelling that the runner and
    // closing validator refuse.
    let temporary = tempfile::tempdir().unwrap();
    let rejected_outcome = temporary.path().join("rejected.json");
    let rejected = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "socket-shape".to_owned(),
            rejected_outcome.to_string_lossy().into_owned(),
            "/tmp/not-herdr/socket".to_owned(),
        ],
    );
    assert_eq!(rejected.status.code(), Some(20));
    assert!(!rejected_outcome.exists());

    let accepted_outcome = temporary.path().join("accepted.json");
    let accepted = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "socket-shape".to_owned(),
            accepted_outcome.to_string_lossy().into_owned(),
            "/tmp/herdr-i5.12345678/b-t0001.sock".to_owned(),
        ],
    );
    assert_eq!(accepted.status.code(), Some(0), "{accepted:?}");
    support::assert_runner_outcome(&accepted_outcome, 0);
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn assert_fixture_write_refuses_node(site: &str, node: &str) {
    use reference_runner_test_support as support;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, symlink};

    let temporary = tempfile::tempdir().unwrap();
    let destination = temporary.path().join("destination");
    let target = temporary.path().join("target");
    let mut fifo_guard = None;
    let expected_stderr = match node {
        "symlink" => {
            std::fs::write(&target, b"preserve\n").unwrap();
            symlink(&target, &destination).unwrap();
            "error: fixture output path is a symbolic link\n"
        }
        "fifo" => {
            let path = CString::new(destination.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
            if site == "trap-marker" {
                fifo_guard = Some(
                    std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .custom_flags(libc::O_NONBLOCK)
                        .open(&destination)
                        .unwrap(),
                );
            }
            "error: fixture output path is a FIFO\n"
        }
        _ => panic!("unknown fixture node kind"),
    };
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "fixture-output-guard".to_owned(),
            site.to_owned(),
            destination.to_string_lossy().into_owned(),
        ],
    );
    drop(fifo_guard);
    assert_eq!(output.status.code(), Some(20), "{site}/{node}: {output:?}");
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        expected_stderr,
        "{site}/{node}"
    );
    if node == "symlink" {
        assert_eq!(std::fs::read(&target).unwrap(), b"preserve\n");
    } else {
        assert!(
            std::fs::symlink_metadata(&destination)
                .unwrap()
                .file_type()
                .is_fifo()
        );
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn fixture_output_validator_refuses_symlink() {
    assert_fixture_write_refuses_node("validator", "symlink");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn fixture_output_validator_refuses_fifo() {
    assert_fixture_write_refuses_node("validator", "fifo");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_test_outcome_publisher_refuses_symlink() {
    assert_fixture_write_refuses_node("runner-outcome", "symlink");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_test_outcome_publisher_refuses_fifo() {
    assert_fixture_write_refuses_node("runner-outcome", "fifo");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn trial_status_publisher_refuses_symlink() {
    assert_fixture_write_refuses_node("trial-status", "symlink");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn trial_status_publisher_refuses_fifo() {
    assert_fixture_write_refuses_node("trial-status", "fifo");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn orchestration_trap_marker_refuses_symlink() {
    assert_fixture_write_refuses_node("trap-marker", "symlink");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn orchestration_trap_marker_refuses_fifo() {
    assert_fixture_write_refuses_node("trap-marker", "fifo");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn outer_trap_identity_window_is_single_command() {
    use reference_runner_test_support as support;

    // Break caught: a signal between runtime-directory creation and identity
    // capture leaves an unowned directory that the outer trap cannot remove.
    let temporary = tempfile::tempdir().unwrap();
    let capture = temporary.path().join("identity-window");
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "outer-identity-window".to_owned(),
            capture.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(20), "{output:?}");
    assert_eq!(
        std::str::from_utf8(&output.stderr).unwrap(),
        "error: child status was outside the closed set\n",
        "{output:?}"
    );
    let captured = std::fs::read_to_string(capture).unwrap();
    let (directory, identity) = captured.trim_end().split_once(' ').unwrap();
    assert_eq!(identity.split(':').count(), 5);
    assert!(!PathBuf::from(directory).exists());
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn outer_trap_group_publication_is_atomic() {
    use reference_runner_test_support as support;

    // Break caught: interruption while replacing the outer state exposes a
    // truncated mixture of measured/observer group identifiers.
    let temporary = tempfile::tempdir().unwrap();
    let capture = temporary.path().join("group-publication");
    let state_capture = temporary.path().join("settled-state");
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "outer-group-publication".to_owned(),
            capture.to_string_lossy().into_owned(),
            state_capture.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(20), "{output:?}");
    assert_eq!(std::fs::read_to_string(state_capture).unwrap(), "- - -\n");
    let directory = std::fs::read_to_string(capture).unwrap();
    assert!(!PathBuf::from(directory.trim_end()).exists());
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn publisher_temp_never_blocks_rmdir() {
    use reference_runner_test_support as support;

    // Break caught: an interrupted `.outer-state.tmp.*` publisher artifact
    // survives state cleanup and makes the identity-checked rmdir fail.
    let temporary = tempfile::tempdir().unwrap();
    let capture = temporary.path().join("publisher-temp");
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "publisher-temp-cleanup".to_owned(),
            capture.to_string_lossy().into_owned(),
        ],
    );
    assert_eq!(output.status.code(), Some(20), "{output:?}");
    let directory = std::fs::read_to_string(capture).unwrap();
    assert!(!PathBuf::from(directory.trim_end()).exists());
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn orchestration_wait_is_deadline_bounded() {
    use reference_runner_test_support as support;

    // Break caught: cleanup waits forever for an orchestration child that
    // never exits or signals after the scenario supervisor fires.
    let temporary = tempfile::tempdir().unwrap();
    let outcome = temporary.path().join("deadline-outcome.json");
    let started = std::time::Instant::now();
    let output = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "orchestration-deadline".to_owned(),
            outcome.to_string_lossy().into_owned(),
        ],
    );
    assert!(started.elapsed() < Duration::from_secs(30), "{output:?}");
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    support::assert_runner_outcome(&outcome, 0);
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn trial_status_is_atomic_and_independent_of_pidstat_exit() {
    use reference_runner_test_support as support;

    // Break caught: sentinel derivation from pidstat rather than the
    // orchestrator, noncanonical bytes, overwrite, or a leaked temporary.
    for (orchestrator, pidstat, expected) in [
        (0, 0, "ok:0\n"),
        (0, 23, "ok:0\n"),
        (137, 0, "failed:137\n"),
        (124, 23, "failed:124\n"),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let status_path = temporary.path().join("trial-status");
        let outcome = temporary.path().join("outcome.json");
        let output = support::run_source_fixture(
            Some("00000001"),
            &[
                "orchestration".to_owned(),
                "sentinel".to_owned(),
                outcome.to_string_lossy().into_owned(),
                status_path.to_string_lossy().into_owned(),
                orchestrator.to_string(),
                pidstat.to_string(),
            ],
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_eq!(std::fs::read_to_string(&status_path).unwrap(), expected);
        support::assert_runner_outcome(&outcome, 0);
        assert!(
            support::atomic_temporary_paths(temporary.path()).is_empty(),
            "trial-status publication left a PID-qualified temporary behind"
        );
    }

    let temporary = tempfile::tempdir().unwrap();
    let status_path = temporary.path().join("trial-status");
    std::fs::write(&status_path, b"do-not-overwrite\n").unwrap();
    let outcome = temporary.path().join("outcome.json");
    let rejected = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "sentinel".to_owned(),
            outcome.to_string_lossy().into_owned(),
            status_path.to_string_lossy().into_owned(),
            "0".to_owned(),
            "0".to_owned(),
        ],
    );
    assert_eq!(rejected.status.code(), Some(20));
    assert_eq!(
        std::fs::read_to_string(status_path).unwrap(),
        "do-not-overwrite\n"
    );

    let temporary = tempfile::tempdir().unwrap();
    let forbidden_status = temporary.path().join("result-v1.json");
    let outcome = temporary.path().join("outcome.json");
    let rejected = support::run_source_fixture(
        Some("00000001"),
        &[
            "orchestration".to_owned(),
            "sentinel".to_owned(),
            outcome.to_string_lossy().into_owned(),
            forbidden_status.to_string_lossy().into_owned(),
            "0".to_owned(),
            "0".to_owned(),
        ],
    );
    assert_eq!(rejected.status.code(), Some(20));
    assert!(!forbidden_status.exists());
    assert!(!outcome.exists());
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn trial_status_reader_rejects_unterminated_trailing_bytes() {
    use reference_runner_test_support as support;

    // Break caught: a second read that returns EOF after consuming an
    // unterminated trailing fragment must still reject those bytes.
    for (bytes, expected_status) in [(&b"ok:0\n"[..], 0), (&b"ok:0\njunk"[..], 20)] {
        let temporary = tempfile::tempdir().unwrap();
        let status = temporary.path().join("trial-status");
        let outcome = temporary.path().join("outcome.json");
        std::fs::write(&status, bytes).unwrap();
        let output = support::run_source_fixture(
            Some("00000001"),
            &[
                "orchestration".to_owned(),
                "read-status".to_owned(),
                outcome.to_string_lossy().into_owned(),
                status.to_string_lossy().into_owned(),
            ],
        );
        assert_eq!(output.status.code(), Some(expected_status), "{output:?}");
        assert_eq!(outcome.exists(), expected_status == 0);
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn runner_fixture_preserves_measured_and_observer_exit_status_precedence() {
    use reference_runner_test_support as support;

    // Break caught: `wait` under `set -e`, boolean status collapse, observer
    // precedence over a measured failure, a non-atomic sentinel, or clearing
    // group IDs before killing descendants left by already-reaped leaders.
    for (measured_status, observer_status, expected) in [
        (137, 0, "failed:137\n"),
        (0, 143, "failed:143\n"),
        (124, 137, "failed:124\n"),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let measured = temporary.path().join("measured.sh");
        let observer = temporary.path().join("observer.sh");
        support::write_executable(
            &measured,
            &format!(
                "#!/usr/bin/bash -p\ntrap '' HUP TERM\n/usr/bin/sleep 30 </dev/null >/dev/null 2>&1 &\nexit {measured_status}\n"
            ),
        );
        support::write_executable(
            &observer,
            &format!(
                "#!/usr/bin/bash -p\ntrap '' HUP TERM\n/usr/bin/sleep 30 </dev/null >/dev/null 2>&1 &\nexit {observer_status}\n"
            ),
        );
        let outcome = temporary.path().join("outcome.json");
        let status_path = temporary.path().join("trial-status");
        let groups = temporary.path().join("groups.txt");
        let output = support::run_source_fixture(
            Some("00000001"),
            &[
                "orchestration".to_owned(),
                "precedence".to_owned(),
                outcome.to_string_lossy().into_owned(),
                groups.to_string_lossy().into_owned(),
                status_path.to_string_lossy().into_owned(),
                measured.to_string_lossy().into_owned(),
                observer.to_string_lossy().into_owned(),
            ],
        );
        assert_eq!(output.status.code(), Some(20), "{output:?}");
        support::assert_runner_outcome(&outcome, 20);
        assert_eq!(std::fs::read_to_string(status_path).unwrap(), expected);
        let group_ids = std::fs::read_to_string(groups).unwrap();
        let live_groups = group_ids
            .split_whitespace()
            .filter(|group| support::process_group_exists(group))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for group in &live_groups {
            support::kill_process_group(group);
        }
        assert!(
            live_groups.is_empty(),
            "fixture reported reaped groups but left these groups live: {live_groups:?}"
        );
        assert!(!support::path_has_result_v1(temporary.path()));
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn pidstat_child_status_modes_are_calibrated_and_cross_checked() {
    use reference_runner_test_support as support;

    // Break caught: assuming a pidstat mode, accepting an uncalibrated pair,
    // malformed diagnostics, calibration drift, or a sentinel disagreement.
    let harness = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
    let shim_body = |mode: &str, counter: Option<&PathBuf>| -> String {
        let counter_logic = counter.map_or_else(String::new, |path| {
            format!(
                "count=0\nif [[ -f '{}' ]]; then IFS= read -r count <'{}'; fi\n((count+=1))\nbuiltin printf '%s\\n' \"$count\" >'{}'\n",
                path.display(),
                path.display(),
                path.display()
            )
        });
        format!(
            "#!/usr/bin/bash -p\nset -euo pipefail\nif [[ ${{1-}} == --exit-status ]]; then\n  json_path=${{!#}}\n  HERDR_FIXTURE_JSON_PATH=\"$json_path\" exec '{}' fixture_pidstat_json_validator_helper --exact --ignored --test-threads=1\nfi\n{counter_logic}while [[ $# -gt 0 && $1 != -e ]]; do shift; done\n[[ ${{1-}} == -e ]] || exit 91\nshift\nset +e\n\"$@\"\nchild_status=$?\nset -e\ncase {mode} in\n  propagates) builtin printf '%s\\n' '{{\"sysstat\":{{\"hosts\":[]}}}}'; exit \"$child_status\" ;;\n  monitor) builtin printf '%s\\n' '{{\"sysstat\":{{\"hosts\":[]}}}}'; exit 0 ;;\n  malformed) builtin printf '%s\\n' not-json; exit \"$child_status\" ;;\n  brace-garbage) builtin printf '%s\\n' '{{garbage \"sysstat\" garbage}}'; exit \"$child_status\" ;;\n  bad-zero) builtin printf '%s\\n' '{{\"sysstat\":{{\"hosts\":[]}}}}'; [[ $child_status -eq 0 ]] && exit 1; exit \"$child_status\" ;;\n  bad-failing) builtin printf '%s\\n' '{{\"sysstat\":{{\"hosts\":[]}}}}'; [[ $child_status -eq 0 ]] && exit 0; exit 1 ;;\n  drift) builtin printf '%s\\n' '{{\"sysstat\":{{\"hosts\":[]}}}}'; [[ $count -le 2 ]] && exit \"$child_status\"; exit 0 ;;\nesac\n",
            harness.display()
        )
    };
    for (mode_name, sentinel, observed, expected_mode) in [
        ("propagates", 23, 23, "propagates_child_status\n"),
        ("monitor", 23, 0, "monitor_only\n"),
        ("propagates", 0, 0, "propagates_child_status\n"),
        ("monitor", 0, 0, "monitor_only\n"),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let outcome = temporary.path().join("outcome.json");
        let mode = temporary.path().join("mode.txt");
        let zero_output = temporary.path().join("zero.json");
        let failure_output = temporary.path().join("failure.json");
        let pidstat = temporary.path().join("pidstat");
        support::write_executable(&pidstat, &shim_body(mode_name, None));
        let mut tools = support::fixture_tools();
        tools
            .iter_mut()
            .find(|tool| tool.role == "pidstat")
            .unwrap()
            .requested = pidstat;
        let runner = support::identity(&support::runner_script());
        let output = support::source_fixture_command(
            &runner,
            &tools,
            Some("00000001"),
            &[
                "orchestration".to_owned(),
                "pidstat-calibration".to_owned(),
                outcome.to_string_lossy().into_owned(),
                mode.to_string_lossy().into_owned(),
                zero_output.to_string_lossy().into_owned(),
                failure_output.to_string_lossy().into_owned(),
                sentinel.to_string(),
                observed.to_string(),
            ],
        )
        .output()
        .unwrap();
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        support::assert_runner_outcome(&outcome, 0);
        assert_eq!(std::fs::read_to_string(mode).unwrap(), expected_mode);
    }

    for (mode_name, sentinel, observed) in [
        ("bad-zero", 23, 23),
        ("bad-failing", 1, 1),
        ("malformed", 23, 23),
        ("brace-garbage", 23, 23),
        ("propagates", 23, 0),
        ("monitor", 23, 23),
        ("drift", 23, 23),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let outcome = temporary.path().join("outcome.json");
        let mode = temporary.path().join("mode.txt");
        let zero_output = temporary.path().join("zero.json");
        let failure_output = temporary.path().join("failure.json");
        let counter = temporary.path().join("counter");
        let pidstat = temporary.path().join("pidstat");
        support::write_executable(
            &pidstat,
            &shim_body(mode_name, (mode_name == "drift").then_some(&counter)),
        );
        let mut tools = support::fixture_tools();
        tools
            .iter_mut()
            .find(|tool| tool.role == "pidstat")
            .unwrap()
            .requested = pidstat;
        let runner = support::identity(&support::runner_script());
        let output = support::source_fixture_command(
            &runner,
            &tools,
            Some("00000001"),
            &[
                "orchestration".to_owned(),
                "pidstat-calibration".to_owned(),
                outcome.to_string_lossy().into_owned(),
                mode.to_string_lossy().into_owned(),
                zero_output.to_string_lossy().into_owned(),
                failure_output.to_string_lossy().into_owned(),
                sentinel.to_string(),
                observed.to_string(),
            ],
        )
        .output()
        .unwrap();
        assert_eq!(output.status.code(), Some(20));
        assert!(!outcome.exists());
    }
}

#[cfg(target_os = "linux")]
fn run_closed_git(cwd: &std::path::Path, arguments: &[&str]) -> Result<Vec<u8>, String> {
    let output = std::process::Command::new("/usr/bin/git")
        .env_clear()
        .envs([
            ("HOME", "/home/mageyuki"),
            ("PATH", "/usr/bin:/bin"),
            ("LC_ALL", "C"),
            ("TZ", "UTC"),
        ])
        .current_dir(cwd)
        .args(arguments)
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.code() != Some(0) {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    Ok(output.stdout)
}

#[cfg(target_os = "linux")]
fn marker_bounded_production_diff(
    cwd: &std::path::Path,
    subject: &str,
    path: &str,
) -> Result<(), String> {
    let current = String::from_utf8(
        run_closed_git(cwd, &["show", &format!("HEAD:{path}")])
            .map_err(|error| format!("current production blob: {error}"))?,
    )
    .map_err(|_| "current production blob was not UTF-8".to_owned())?;
    let baseline = run_closed_git(cwd, &["show", &format!("{subject}:{path}")])?;
    if current.as_bytes() == baseline {
        return Ok(());
    }
    let markers = current
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            line.contains("increment5-workload-harness")
                .then_some(index + 1)
        })
        .collect::<Vec<_>>();
    if markers.is_empty() || markers.len() % 2 != 0 {
        return Err(format!("{path} did not contain paired harness markers"));
    }
    let ranges = markers
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| pair[0]..=pair[1])
        .collect::<Vec<_>>();
    let diff = String::from_utf8(run_closed_git(
        cwd,
        &["diff", "--unified=0", subject, "HEAD", "--", path],
    )?)
    .map_err(|_| "production diff was not UTF-8".to_owned())?;
    let mut new_line = 0_usize;
    for line in diff.lines() {
        if let Some(header) = line.strip_prefix("@@ ") {
            let new = header
                .split_whitespace()
                .find(|field| field.starts_with('+'))
                .ok_or_else(|| "diff hunk omitted the new range".to_owned())?;
            new_line = new[1..]
                .split(',')
                .next()
                .ok_or_else(|| "diff hunk new range was malformed".to_owned())?
                .parse()
                .map_err(|_| "diff hunk new line was malformed".to_owned())?;
        } else if line.starts_with("+++") || line.starts_with("---") {
            continue;
        } else if line.starts_with('+') {
            if !ranges.iter().any(|range| range.contains(&new_line)) {
                return Err(format!("{path}:{new_line} escaped harness markers"));
            }
            new_line += 1;
        } else if line.starts_with('-') {
            return Err(format!("{path} removed production bytes"));
        } else if !line.starts_with("diff ") && !line.starts_with("index ") && !line.is_empty() {
            new_line += 1;
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn verify_subject_diff_is_harness_only_impl(
    cwd: &std::path::Path,
    subject: &str,
) -> Result<(), String> {
    use sha2::{Digest, Sha256};

    if subject.len() != 40
        || !subject
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("subject was not a full lowercase Git identity".to_owned());
    }
    let names = String::from_utf8(run_closed_git(
        cwd,
        &["diff", "--name-only", subject, "HEAD"],
    )?)
    .map_err(|_| "changed-file list was not UTF-8".to_owned())?;
    let production = [
        "src/herdr/controller.rs",
        "src/herdr/collector.rs",
        "src/reducer.rs",
        "src/operator.rs",
        "src/store/mod.rs",
        "src/tui/app.rs",
    ];
    let allowed = [
        ".github/workflows/ci.yml",
        "Cargo.toml",
        "docs/superpowers/plans/2026-08-12-increment-5-reliability-performance.md",
        "docs/superpowers/specs/2026-08-12-increment-5-reliability-performance-design.md",
        "scripts/run-reference-profile.sh",
        "tests/common/mod.rs",
        "tests/common/workload.rs",
        "tests/fixtures/MANIFEST.md",
        "tests/fixtures/workload-schema-v1.json",
        "tests/support/reference_profile_controller.rs",
        "tests/workload_harness.rs",
    ];
    for path in names.lines() {
        if production.contains(&path) {
            marker_bounded_production_diff(cwd, subject, path)?;
        } else if !allowed.contains(&path) {
            return Err(format!(
                "tracked path escaped the harness allowlist: {path}"
            ));
        }
    }
    let baseline_cargo = String::from_utf8(run_closed_git(
        cwd,
        &["show", &format!("{subject}:Cargo.toml")],
    )?)
    .map_err(|_| "baseline Cargo.toml was not UTF-8".to_owned())?;
    let current_cargo = std::fs::read_to_string(cwd.join("Cargo.toml"))
        .map_err(|error| format!("current Cargo.toml: {error}"))?;
    let feature = "\n[features]\nworkload-harness = []\n";
    let controller = concat!(
        "\n[[bin]]\n",
        "name = \"increment5-reference-controller\"\n",
        "path = \"tests/support/reference_profile_controller.rs\"\n",
        "required-features = [\"workload-harness\"]\n",
        "test = false\n",
        "bench = false\n"
    );
    if !current_cargo.contains(feature)
        || !current_cargo.contains(controller)
        || current_cargo
            .replacen(feature, "", 1)
            .replacen(controller, "", 1)
            != baseline_cargo
    {
        return Err("Cargo.toml changed outside the exact feature/bin stanzas".to_owned());
    }
    for (path, expected) in [
        (
            "docs/superpowers/specs/2026-08-12-increment-5-reliability-performance-design.md",
            "17dfeb91a2ce0efeff7a6c79bcac345e7ca051f268ed0c39c57ad297e38035f4",
        ),
        (
            "docs/superpowers/plans/2026-08-12-increment-5-reliability-performance.md",
            "dd70fd70bca4e6fd1762e9b37f877deb3c830e7c38a0da054eb1a78434e28799",
        ),
    ] {
        let bytes = std::fs::read(cwd.join(path)).map_err(|error| error.to_string())?;
        if format!("{:x}", Sha256::digest(&bytes)) != expected
            || bytes != run_closed_git(cwd, &["show", &format!("HEAD:{path}")])?
        {
            return Err(format!("planning artifact identity drifted: {path}"));
        }
    }
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn validate_reference_baseline_set_from_environment() -> Result<(), HarnessError> {
    let root = PathBuf::from(
        std::env::var("HERDR_PERF_VALIDATE_BASELINE_RESULTS_ROOT")
            .map_err(|_| HarnessError::Invalid("missing baseline results root"))?,
    );
    let canonical_root = root.canonicalize()?;
    if root != canonical_root || !canonical_root.is_dir() {
        return Err(HarnessError::Invalid("baseline root must be canonical"));
    }
    let mut baseline_id = None;
    for (scenario, mapped) in [
        (ScenarioV1::Target, "target"),
        (ScenarioV1::Sustained, "sustained"),
        (ScenarioV1::Burst, "burst"),
        (ScenarioV1::Startup, "startup"),
        (ScenarioV1::Idle, "idle"),
        (ScenarioV1::FallbackRescan, "fallback_rescan"),
        (ScenarioV1::TwiceTarget, "twice_target"),
    ] {
        let outcome = read_and_validate_reference_outcome(
            &canonical_root.join(mapped).join("result-v1.json"),
            AmendedLegacyMode::Off,
        )?
        .outcome;
        let document = match outcome {
            ReferenceOutcomeV1::Pass { document } | ReferenceOutcomeV1::Failed { document } => {
                document
            }
            ReferenceOutcomeV1::Invalid { .. } => {
                return Err(HarnessError::Invalid("baseline result is invalid"));
            }
        };
        if document.measurement_stage != MeasurementStageV1::Baseline
            || document.scenario != scenario
        {
            return Err(HarnessError::Invalid("baseline result identity mismatch"));
        }
        match &baseline_id {
            Some(expected) if expected != &document.baseline_id => {
                return Err(HarnessError::Invalid("baseline IDs disagree"));
            }
            Some(_) => {}
            None => baseline_id = Some(document.baseline_id),
        }
    }
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn reference_monotonic_ns() -> Result<u64, HarnessError> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    u64::try_from(value.tv_sec)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000_000_000))
        .and_then(|seconds| {
            u64::try_from(value.tv_nsec)
                .ok()
                .and_then(|nanos| seconds.checked_add(nanos))
        })
        .ok_or(HarnessError::Invalid("monotonic timestamp overflowed"))
}

#[cfg(feature = "workload-harness")]
async fn wait_for_reference_epoch(
    clock: &(dyn Fn() -> u64 + Send + Sync),
    target_ns: u64,
) -> Result<(), HarnessError> {
    loop {
        let now = clock();
        let Some(remaining) = target_ns.checked_sub(now) else {
            return Ok(());
        };
        if remaining == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_nanos(remaining.min(5_000_000))).await;
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn reference_affinity() -> Result<Vec<u32>, HarnessError> {
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    if unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) } != 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok((0..libc::CPU_SETSIZE as usize)
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
        .map(|cpu| cpu as u32)
        .collect())
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn reference_address_space_limit() -> Result<u64, HarnessError> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_AS, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(limit.rlim_cur)
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn atomic_write_reference_bytes(path: &std::path::Path, bytes: &[u8]) -> Result<(), HarnessError> {
    use std::io::Write as _;

    let parent = path
        .parent()
        .ok_or(HarnessError::Invalid("reference output had no parent"))?;
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or(HarnessError::Invalid("reference output name was not UTF-8"))?;
    let temporary = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok::<_, HarnessError>(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn atomic_write_reference_json<T: serde::Serialize>(
    path: &std::path::Path,
    value: &T,
) -> Result<(), HarnessError> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    atomic_write_reference_bytes(path, &bytes)
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn read_reference_control_frame(
    reader: &mut impl std::io::BufRead,
) -> Result<ObserverControlFrameV1, HarnessError> {
    let mut bytes = Vec::new();
    reader.read_until(b'\n', &mut bytes)?;
    if bytes.last() != Some(&b'\n') {
        return Err(HarnessError::Invalid(
            "observer control frame was incomplete",
        ));
    }
    let frame: ObserverControlFrameV1 = serde_json::from_slice(&bytes[..bytes.len() - 1])?;
    let mut canonical = serde_json::to_vec(&frame)?;
    canonical.push(b'\n');
    if bytes != canonical {
        return Err(HarnessError::Invalid(
            "observer control frame was not canonical",
        ));
    }
    Ok(frame)
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn write_reference_control_command(
    stream: &mut std::os::unix::net::UnixStream,
    command: &ObserverCommandV1,
) -> Result<(), HarnessError> {
    use std::io::Write as _;

    serde_json::to_writer(&mut *stream, command)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn reference_scenario(value: &str) -> Result<(ScenarioV1, WorkloadProfile), HarnessError> {
    match value {
        "target" => Ok((ScenarioV1::Target, WorkloadProfile::TargetTopology)),
        "sustained" => Ok((ScenarioV1::Sustained, WorkloadProfile::SustainedTarget)),
        "burst" => Ok((ScenarioV1::Burst, WorkloadProfile::TargetBurst)),
        "startup" => Ok((ScenarioV1::Startup, WorkloadProfile::Startup)),
        "idle" => Ok((ScenarioV1::Idle, WorkloadProfile::Idle)),
        "fallback-rescan" => Ok((ScenarioV1::FallbackRescan, WorkloadProfile::FallbackRescan)),
        "twice-target" => Ok((ScenarioV1::TwiceTarget, WorkloadProfile::TwiceTarget)),
        _ => Err(HarnessError::Invalid("unknown reference scenario")),
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn reference_stage(value: &str) -> Result<MeasurementStageV1, HarnessError> {
    match value {
        "baseline" => Ok(MeasurementStageV1::Baseline),
        "post-reliability" => Ok(MeasurementStageV1::PostReliability),
        "final" => Ok(MeasurementStageV1::Final),
        _ => Err(HarnessError::Invalid("unknown reference stage")),
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn reference_trial_index(scratch_root: &std::path::Path) -> Result<usize, HarnessError> {
    let trial = scratch_root
        .parent()
        .and_then(std::path::Path::file_name)
        .and_then(std::ffi::OsStr::to_str)
        .ok_or(HarnessError::Invalid(
            "scratch root omitted its trial directory",
        ))?;
    if trial == "warm-up-0001" {
        return Ok(0);
    }
    trial
        .strip_prefix("trial-")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0 && format!("trial-{value:04}") == trial)
        .ok_or(HarnessError::Invalid("trial directory was not canonical"))
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn reference_phase(scenario: ScenarioV1, trial_index: usize) -> Option<u64> {
    let spec = workload_schema()
        .scenarios
        .iter()
        .find(|spec| spec.scenario == scenario)
        .expect("closed reference scenario must have a manifest row");
    if trial_index == 0 {
        spec.warm_up_frame_phase_offset_ns
    } else {
        spec.frame_phase_offsets_ns.get(trial_index - 1).copied()
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
async fn run_reference_input_trial(
    desired_phase_ns: u64,
) -> Result<(u64, u64, Vec<InputLatencyObservationV1>), HarnessError> {
    let (_model_sender, model_receiver) =
        tokio::sync::watch::channel(Arc::new(workload::target_model()));
    let app = App::new(model_receiver, HeaderInputs::default());
    let terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    let mut driver = WorkloadFrameDriver::new(app, terminal, || {
        Duration::from_nanos(
            reference_monotonic_ns().expect("monotonic clock must remain readable"),
        )
    });
    let priming_frame_recorded_ns = reference_monotonic_ns()?;
    if driver.step(true)?.draw_ordinal.is_none() {
        return Err(HarnessError::Invalid(
            "reference input priming frame was skipped",
        ));
    }
    let workload_origin_ns = priming_frame_recorded_ns
        .checked_add(
            100_000_000_u64
                .checked_sub(desired_phase_ns)
                .ok_or(HarnessError::Invalid("input phase exceeded frame interval"))?,
        )
        .ok_or(HarnessError::Invalid("input workload origin overflowed"))?;
    let wait_clock =
        || reference_monotonic_ns().expect("reference monotonic clock must remain readable");
    let mut scheduled_ns = workload_origin_ns;
    let mut observations = Vec::with_capacity(200);
    for _ in 0..200 {
        wait_for_reference_epoch(&wait_clock, scheduled_ns).await?;
        let injected_ns = reference_monotonic_ns()?;
        let frame = driver.handle_key_and_wait(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('?'),
            crossterm::event::KeyModifiers::NONE,
        ))?;
        if frame.draw_ordinal.is_none() {
            return Err(HarnessError::Invalid("reference input frame was skipped"));
        }
        let rendered_ns = reference_monotonic_ns()?;
        observations.push(InputLatencyObservationV1 {
            scheduled_ns,
            injected_ns,
            rendered_ns,
            observed_frame_phase_ns: rendered_ns
                .checked_sub(injected_ns)
                .ok_or(HarnessError::Invalid("input clock regressed"))?
                % 100_000_000,
        });
        scheduled_ns = rendered_ns
            .checked_add(100_000_000 - desired_phase_ns)
            .ok_or(HarnessError::Invalid("next input schedule overflowed"))?;
    }
    Ok((priming_frame_recorded_ns, workload_origin_ns, observations))
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[derive(serde::Deserialize, serde::Serialize)]
struct ReferenceStartupHelperV1 {
    startup_ns: u64,
    restored_activity_count: u64,
    scoped: ScopedTimingObservationV1,
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
fn run_reference_startup_helper(
    root: &StateRoot,
    output: &std::path::Path,
) -> Result<ReferenceStartupHelperV1, HarnessError> {
    let status = std::process::Command::new(std::env::current_exe()?)
        .env("HERDR_PERF_STARTUP_STATE_ROOT", &root.0)
        .env("HERDR_PERF_STARTUP_HELPER_OUTPUT", output)
        .args([
            "reference_profile_startup_restore_helper",
            "--exact",
            "--ignored",
            "--test-threads=1",
        ])
        .status()?;
    if !status.success() {
        return Err(HarnessError::Invalid("startup restore helper failed"));
    }
    let bytes = std::fs::read(output)?;
    let helper: ReferenceStartupHelperV1 = serde_json::from_slice(&bytes)?;
    let mut canonical = serde_json::to_vec(&helper)?;
    canonical.push(b'\n');
    if bytes != canonical {
        return Err(HarnessError::Invalid(
            "startup helper output was not canonical",
        ));
    }
    Ok(helper)
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
#[ignore = "fresh startup restore helper for the authoritative reference entrypoint"]
fn reference_profile_startup_restore_helper() {
    let root = StateRoot(PathBuf::from(
        std::env::var_os("HERDR_PERF_STARTUP_STATE_ROOT").expect("startup root must be supplied"),
    ));
    let output = PathBuf::from(
        std::env::var_os("HERDR_PERF_STARTUP_HELPER_OUTPUT")
            .expect("startup helper output must be supplied"),
    );
    assert_eq!(
        u64::try_from(herdr_top::store::WORKLOAD_RESTORE_ACTIVITY_LIMIT).unwrap(),
        workload_schema().operator_activity_limit
    );
    assert_eq!(
        u64::try_from(herdr_top::operator::WORKLOAD_OPERATOR_ACTIVITY_LIMIT).unwrap(),
        workload_schema().operator_activity_limit
    );
    let restore_started = std::time::Instant::now();
    let reader = open_reader(&root).unwrap();
    let restored = reader.load_restored_state().unwrap();
    let operator = reader.load_restored_operator_state().unwrap();
    let restore_elapsed = restore_started.elapsed();
    let expected_activity_count =
        usize::try_from(workload_schema().operator_activity_limit).unwrap();
    assert_eq!(operator.activity.len(), expected_activity_count);
    assert!(operator.activity.windows(2).all(|pair| {
        herdr_top::operator::workload_compare_activity(&pair[0], &pair[1])
            == std::cmp::Ordering::Less
    }));
    assert_eq!(
        operator
            .activity
            .iter()
            .map(|item| item.identity.event_id.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len(),
        expected_activity_count
    );
    let first = operator.activity.first().unwrap();
    assert_eq!(
        (
            first.identity.event_id.as_str(),
            first.event_timestamp_ms,
            first.seen_at_ms,
        ),
        ("startup-retained-099999", 99_999, 99_999)
    );
    let last = operator.activity.last().unwrap();
    assert_eq!(
        (
            last.identity.event_id.as_str(),
            last.event_timestamp_ms,
            last.seen_at_ms,
        ),
        ("startup-retained-090000", 90_000, 90_000)
    );
    let restored_activity_count = u64::try_from(operator.activity.len()).unwrap();
    let scoped = Arc::new(Mutex::new(Vec::new()));
    let constructor_started = std::time::Instant::now();
    let (_reducer, _model, _operator) = Reducer::new_with_operator_observed(
        restored,
        operator,
        1,
        workload_timing_collector(Arc::clone(&scoped)),
    );
    let helper = ReferenceStartupHelperV1 {
        startup_ns: duration_ns(
            restore_elapsed
                .checked_add(constructor_started.elapsed())
                .expect("startup duration must fit Duration"),
        ),
        restored_activity_count,
        scoped: lock_workload(&scoped).first().cloned().unwrap(),
    };
    atomic_write_reference_json(&output, &helper).unwrap();
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
async fn reference_profile_entrypoint_impl() -> Result<(), HarnessError> {
    use std::os::unix::net::UnixListener;

    let output = PathBuf::from(
        std::env::var_os("HERDR_PERF_OUTPUT")
            .ok_or(HarnessError::Invalid("reference output was missing"))?,
    );
    let handshake = PathBuf::from(
        std::env::var_os("HERDR_PERF_OBSERVER_HANDSHAKE")
            .ok_or(HarnessError::Invalid("observer handshake was missing"))?,
    );
    let control_socket = PathBuf::from(
        std::env::var_os("HERDR_PERF_OBSERVER_CONTROL_SOCKET")
            .ok_or(HarnessError::Invalid("observer socket was missing"))?,
    );
    let scratch_root = PathBuf::from(
        std::env::var_os("HERDR_PERF_SCRATCH_ROOT")
            .ok_or(HarnessError::Invalid("scratch root was missing"))?,
    );
    let scenario_raw = std::env::var("HERDR_PERF_SCENARIO")
        .map_err(|_| HarnessError::Invalid("reference scenario was missing"))?;
    let (scenario, profile) = reference_scenario(&scenario_raw)?;
    let stage = reference_stage(
        &std::env::var("HERDR_PERF_STAGE")
            .map_err(|_| HarnessError::Invalid("reference stage was missing"))?,
    )?;
    let trial_index = reference_trial_index(&scratch_root)?;
    let listener = UnixListener::bind(&control_socket)?;
    let pid = std::process::id();
    let start_time_ticks = workload::linux_process_start_time_ticks(pid)?;
    let trial_origin_ns = reference_monotonic_ns()?;
    atomic_write_reference_bytes(
        &handshake,
        format!("{pid} {start_time_ticks} {trial_origin_ns}\n").as_bytes(),
    )?;
    let (stream, _) = listener.accept()?;
    stream.set_read_timeout(Some(Duration::from_secs(40)))?;
    let mut control_writer = stream.try_clone()?;
    let mut control_reader = std::io::BufReader::new(stream);
    let observer_ready_ns = match read_reference_control_frame(&mut control_reader)? {
        ObserverControlFrameV1::Ready { observer_ready_ns }
            if observer_ready_ns > trial_origin_ns =>
        {
            observer_ready_ns
        }
        _ => {
            return Err(HarnessError::Invalid(
                "observer did not send the first Ready frame",
            ));
        }
    };
    let identities = workload::target_identities_v1();
    let mut priming_frame_recorded_ns = None;
    let mut workload_origin_ns = None;
    let mut frame_phase_offset_ns = None;
    let mut priming_frame_count = 0;
    let mut admission_observations = Vec::new();
    let mut screen_observations = Vec::new();
    let mut input_observations = Vec::new();
    let mut startup_observations_ns = Vec::new();
    let mut fallback_pairs = Vec::new();
    let mut scoped_observations = Vec::new();
    let mut submitted_sequences = Vec::new();
    let mut admitted_sequences = Vec::new();
    let mut completed_sequences = Vec::new();
    let mut persisted_sequences = Vec::new();
    let mut rendered_sequences = Vec::new();
    let mut prepared_non_gap_event_count = None;
    let mut prepared_ledger_row_count = None;
    let mut restored_activity_count = None;
    let mut performance_evidence_stream = None;
    let mut idle_window_start_ns = None;
    let mut idle_window_end_ns = None;

    match scenario {
        ScenarioV1::Target => {
            let phase = reference_phase(scenario, trial_index)
                .ok_or(HarnessError::Invalid("target trial omitted its phase"))?;
            let (priming, origin, observations) = run_reference_input_trial(phase).await?;
            priming_frame_recorded_ns = Some(priming);
            workload_origin_ns = Some(origin);
            frame_phase_offset_ns = Some(phase);
            priming_frame_count = 1;
            input_observations = observations;
        }
        ScenarioV1::Sustained | ScenarioV1::Burst | ScenarioV1::TwiceTarget => {
            let phase = reference_phase(scenario, trial_index)
                .ok_or(HarnessError::Invalid("queue trial omitted its phase"))?;
            let result = run_reference_schedule_through_real_queue(
                profile,
                trial_origin_ns,
                observer_ready_ns,
                phase,
            )
            .await;
            if result.final_identities != workload::oracle(profile).final_identities {
                return Err(HarnessError::Invalid(
                    "queue final identities differed from the oracle",
                ));
            }
            priming_frame_recorded_ns = Some(result.priming_frame_recorded_ns);
            workload_origin_ns = Some(result.workload_origin_ns);
            frame_phase_offset_ns = Some(phase);
            priming_frame_count = 1;
            performance_evidence_stream =
                reference_profile_performance_stream(stage, scenario, &result);
            admission_observations = result.admission_observations;
            screen_observations = result.screen_observations;
            scoped_observations = result.scoped_observations;
            submitted_sequences = result.submitted_sequences;
            admitted_sequences = result.admitted_sequences;
            completed_sequences = result.completed_sequences;
            persisted_sequences = result.persisted_sequences;
            rendered_sequences = result.rendered_sequences;
        }
        ScenarioV1::Startup => {
            let state = StateRoot(scratch_root.join("startup-state"));
            prepare_startup_store(&state, 100_000)?;
            let (non_gap_events, ledger_rows) = startup_store_counts(&state)?;
            let helper = run_reference_startup_helper(&state, &scratch_root.join("startup.json"))?;
            startup_observations_ns.push(helper.startup_ns);
            scoped_observations.push(helper.scoped);
            prepared_non_gap_event_count = Some(non_gap_events);
            prepared_ledger_row_count = Some(ledger_rows);
            restored_activity_count = Some(helper.restored_activity_count);
        }
        ScenarioV1::Idle => {
            tokio::time::sleep(Duration::from_secs(5)).await;
            write_reference_control_command(
                &mut control_writer,
                &ObserverCommandV1::StartIdleWindow {},
            )?;
            idle_window_start_ns = match read_reference_control_frame(&mut control_reader)? {
                ObserverControlFrameV1::IdleWindowStarted {
                    request_received_ns,
                    start_ns,
                } if request_received_ns <= start_ns => Some(start_ns),
                _ => {
                    return Err(HarnessError::Invalid(
                        "idle start acknowledgement was invalid",
                    ));
                }
            };
            idle_window_end_ns = match read_reference_control_frame(&mut control_reader)? {
                ObserverControlFrameV1::IdleWindowEnded { end_ns }
                    if idle_window_start_ns.is_some_and(|start| end_ns > start) =>
                {
                    Some(end_ns)
                }
                _ => {
                    return Err(HarnessError::Invalid(
                        "idle end acknowledgement was invalid",
                    ));
                }
            };
        }
        ScenarioV1::FallbackRescan => {
            for sequence in 1..=5 {
                let paired = run_notification_and_forced_rescan_pair_at(
                    herdr_top::provider::RESCAN_INTERVAL,
                    sequence,
                )
                .await;
                let notification = paired.notification;
                let rescan = paired.rescan;
                let notification_ns = duration_ns(notification.elapsed);
                let rescan_ns = duration_ns(rescan.elapsed);
                let notification_final_identities =
                    structural_identities_v1(&notification.final_identities);
                let rescan_final_identities = structural_identities_v1(&rescan.final_identities);
                if notification_final_identities != identities
                    || rescan_final_identities != identities
                {
                    return Err(HarnessError::Invalid(
                        "fallback final identities differed from the oracle",
                    ));
                }
                // This ordering relies on the production two-second rescan interval dwarfing
                // notification latency; revisit it if that interval approaches the CI poll.
                if rescan_ns < notification_ns {
                    return Err(HarnessError::Invalid(
                        "fallback rescan preceded notification",
                    ));
                }
                fallback_pairs.push(FallbackPairObservationV1 {
                    sequence,
                    notification_ns,
                    rescan_ns,
                    notification_final_identities,
                    rescan_final_identities,
                });
                scoped_observations.extend(notification.scoped_observations);
                scoped_observations.extend(rescan.scoped_observations);
            }
        }
    }

    let child_controls = ChildControlsV1 {
        effective_affinity_cpu_ids: reference_affinity()?,
        effective_address_space_limit_bytes: reference_address_space_limit()?,
        measured_environment: std::env::vars().collect(),
        scratch_root: scratch_root.to_string_lossy().into_owned(),
    };
    let trial = HarnessTrialV1 {
        scenario,
        trial_index,
        trial_origin_ns,
        priming_frame_recorded_ns,
        workload_origin_ns,
        frame_phase_offset_ns,
        priming_frame_count,
        admission_observations,
        screen_observations,
        input_observations,
        startup_observations_ns,
        fallback_pairs,
        scoped_observations,
        submitted_sequences,
        admitted_sequences,
        completed_sequences,
        persisted_sequences,
        rendered_sequences,
        pane_ids: identities.pane_ids,
        task_run_ids: identities.task_run_ids,
        dependency_edges: identities.dependency_edges,
        execution_edges: identities.execution_edges,
        prepared_non_gap_event_count,
        prepared_ledger_row_count,
        restored_activity_count,
        performance_evidence_stream,
        idle_window_start_ns,
        idle_window_end_ns,
        child_controls,
    };
    atomic_write_reference_json(&output, &trial)
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[tokio::test]
#[ignore = "native runner supplies the closed authoritative reference protocol"]
async fn reference_profile_entrypoint() {
    reference_profile_entrypoint_impl()
        .await
        .expect("reference profile entrypoint must complete");
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn reference_profile_entrypoint_source_uses_performance_stream_selector() {
    const HELPER_DECL: &str = "fn reference_profile_performance_stream(";
    const START: &str = "async fn reference_profile_entrypoint_impl()";
    const END: &str =
        "\n#[cfg(all(target_os = \"linux\", feature = \"workload-harness\"))]\n#[tokio::test]";
    const GUARD_DECL: &str =
        "fn reference_profile_entrypoint_source_uses_performance_stream_selector()";
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(file!());
    let source = std::fs::read_to_string(path).expect("workload harness source should be readable");
    let helper_decl = source
        .find(HELPER_DECL)
        .expect("performance stream helper declaration should exist");
    let start = source
        .find(START)
        .expect("entrypoint declaration should exist");
    let entrypoint = &source[start..];
    let end = start
        + entrypoint
            .find(END)
            .expect("entrypoint boundary should exist");
    let guard_decl = source
        .find(GUARD_DECL)
        .expect("source marker guard declaration should exist");
    assert!(helper_decl < start && start < end && end < guard_decl);
    assert!(source[start..end].contains("reference_profile_performance_stream("));
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "authoritative baseline preflight invokes the typed harness-only verifier"]
fn verify_subject_diff_is_harness_only() {
    let expected = [
        "CARGO_HOME",
        "HERDR_PERF_VERIFY_INVOCATION_CWD",
        "HERDR_PERF_VERIFY_SUBJECT",
        "HOME",
        "LC_ALL",
        "PATH",
        "RUSTUP_HOME",
        "TZ",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    let actual = std::env::vars()
        .map(|(key, _)| key)
        .collect::<BTreeSet<_>>();
    if actual != expected
        || [
            ("HOME", "/home/mageyuki"),
            ("RUSTUP_HOME", "/home/mageyuki/.rustup"),
            ("CARGO_HOME", "/home/mageyuki/.cargo"),
            ("PATH", "/usr/bin:/bin"),
            ("LC_ALL", "C"),
            ("TZ", "UTC"),
        ]
        .iter()
        .any(|(key, value)| std::env::var(key).as_deref() != Ok(*value))
    {
        std::process::exit(20);
    }
    let Some(cwd) = std::env::var_os("HERDR_PERF_VERIFY_INVOCATION_CWD").map(PathBuf::from) else {
        std::process::exit(20);
    };
    let Ok(subject) = std::env::var("HERDR_PERF_VERIFY_SUBJECT") else {
        std::process::exit(20);
    };
    if verify_subject_diff_is_harness_only_impl(&cwd, &subject).is_err() {
        std::process::exit(20);
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
#[ignore = "normal authoritative launch is reference-host-only and requires explicit roots"]
fn authoritative_reference_profile_runner_smoke() {
    use reference_runner_test_support as support;

    let controller_path = support::controller_binary();
    let required = |key: &str| match std::env::var(key) {
        Ok(value) if !value.is_empty() => value,
        _ => std::process::exit(20),
    };
    let subject = required("HERDR_INCREMENT5_SMOKE_SUBJECT");
    let stage = required("HERDR_INCREMENT5_SMOKE_STAGE");
    let scenario = required("HERDR_INCREMENT5_SMOKE_SCENARIO");
    let attempt_id = required("HERDR_INCREMENT5_SMOKE_ATTEMPT_ID");
    let output_dir = required("HERDR_INCREMENT5_SMOKE_OUTPUT_DIR");
    let mut runner_argv = vec![
        "--subject".to_owned(),
        subject,
        "--stage".to_owned(),
        stage.clone(),
        "--scenario".to_owned(),
        scenario,
        "--output-dir".to_owned(),
        output_dir,
    ];
    if stage != "baseline" {
        runner_argv.extend([
            "--baseline-results-root".to_owned(),
            required("HERDR_INCREMENT5_SMOKE_BASELINE_RESULTS_ROOT"),
        ]);
    }
    let controller = support::identity(&controller_path);
    let runner = support::identity(&support::runner_script());
    let output =
        support::authoritative_runner_command(&controller, &runner, &attempt_id, &runner_argv)
            .output()
            .unwrap_or_else(|_| std::process::exit(20));
    if !matches!(output.status.code(), Some(0 | 10)) {
        eprintln!(
            "authoritative runner stdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        eprintln!(
            "authoritative runner stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::process::exit(20);
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "native Controller fixture child only"]
fn native_controller_recording_child() {
    let output = std::env::var_os("HERDR_TEST_RECORDING_OUTPUT")
        .map(PathBuf::from)
        .expect("recording output must be supplied");
    let environment = std::env::vars().collect::<BTreeMap<_, _>>();
    let mut bytes = serde_json::to_vec(&environment).unwrap();
    bytes.push(b'\n');
    std::fs::write(output, bytes).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "source fixture jq-compatible diagnostic validator only"]
fn fixture_pidstat_json_validator_helper() {
    let path = std::env::var_os("HERDR_FIXTURE_JSON_PATH")
        .map(PathBuf::from)
        .expect("diagnostic path must be supplied");
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).unwrap()).expect("diagnostic must be JSON");
    let valid = value
        .as_object()
        .and_then(|object| object.get("sysstat"))
        .and_then(serde_json::Value::as_object)
        .and_then(|sysstat| sysstat.get("hosts"))
        .is_some_and(serde_json::Value::is_array);
    if !valid {
        std::process::exit(20);
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "source fixture live outer-runtime child only"]
fn fixture_outer_runtime_live_child() {
    use std::os::unix::net::UnixListener;

    let socket = std::env::var_os("HERDR_FIXTURE_SOCKET")
        .map(PathBuf::from)
        .expect("fixture socket must be supplied");
    let _listener = UnixListener::bind(socket).expect("fixture socket must bind");
    std::thread::sleep(Duration::from_secs(300));
}

#[cfg(target_os = "linux")]
fn write_nested_fixture_capture(path: &std::path::Path) {
    let value = serde_json::json!({
        "pid": std::process::id(),
        "environment": std::env::vars().collect::<BTreeMap<_, _>>(),
    });
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    std::fs::write(path, bytes).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "production nested-body measured fixture only"]
fn fixture_nested_measured_helper() {
    use std::os::unix::net::UnixListener;

    let output = PathBuf::from(std::env::var_os("HERDR_PERF_OUTPUT").unwrap());
    let handshake = PathBuf::from(std::env::var_os("HERDR_PERF_OBSERVER_HANDSHAKE").unwrap());
    let socket = PathBuf::from(std::env::var_os("HERDR_PERF_OBSERVER_CONTROL_SOCKET").unwrap());
    let scenario = std::env::var("HERDR_PERF_SCENARIO").unwrap();
    let listener = UnixListener::bind(&socket).unwrap();
    write_nested_fixture_capture(&output);
    std::fs::write(&handshake, format!("{} 17 23\n", std::process::id())).unwrap();
    let (_stream, _) = listener.accept().unwrap();
    if scenario == "idle" {
        std::thread::sleep(Duration::from_secs(300));
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "production nested-body observer fixture only"]
fn fixture_nested_observer_helper() {
    use std::os::unix::net::UnixStream;

    let output = PathBuf::from(std::env::var_os("HERDR_PERF_PROCESS_TREE_OUTPUT").unwrap());
    let socket = PathBuf::from(std::env::var_os("HERDR_PERF_OBSERVER_CONTROL_SOCKET").unwrap());
    let scenario = std::env::var("HERDR_PERF_SCENARIO").unwrap();
    write_nested_fixture_capture(&output);
    let _stream = UnixStream::connect(socket).unwrap();
    if scenario == "idle" {
        std::thread::sleep(Duration::from_secs(300));
    }
}

fn report_entrypoint_error(error: &impl std::fmt::Debug) {
    // libtest captures `println!` output and `std::process::exit` discards
    // that buffer, so write straight to the process stdout handle.
    use std::io::Write as _;
    let mut stdout = std::io::stdout();
    let _ = writeln!(stdout, "entrypoint error: {error:?}");
    let _ = stdout.flush();
}

#[test]
#[ignore = "authoritative classification requires explicit result roots"]
fn classify_d4_checkpoint_from_results() {
    if let Err(error) = classify_d4_checkpoint_from_environment() {
        report_entrypoint_error(&error);
        std::process::exit(20);
    }
}

#[test]
#[ignore = "authoritative Section 15 re-derivation requires explicit result roots"]
fn rederive_section15_report_from_results() {
    if let Err(error) = rederive_section15_report_from_environment() {
        report_entrypoint_error(&error);
        std::process::exit(20);
    }
}

#[test]
#[ignore = "native Controller supplies the closed control environment"]
fn record_runner_control_evidence() {
    record_runner_control_evidence_from_environment()
        .expect("typed runner control recorder must complete");
}

#[test]
#[ignore = "native runner supplies the closed composition environment"]
fn compose_reference_outcome_from_raw() {
    match compose_reference_outcome_from_environment() {
        Ok(0) => {}
        Ok(code) => std::process::exit(code),
        Err(error) => {
            report_entrypoint_error(&error);
            std::process::exit(20);
        }
    }
}

#[test]
#[ignore = "native runner supplies the closed validation environment"]
fn validate_reference_outcome() {
    match validate_reference_outcome_from_environment() {
        Ok(0) => {}
        Ok(code) => std::process::exit(code),
        Err(error) => {
            report_entrypoint_error(&error);
            std::process::exit(20);
        }
    }
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
#[ignore = "native runner validates the selected baseline root before trials"]
fn validate_reference_baseline_set() {
    if let Err(error) = validate_reference_baseline_set_from_environment() {
        report_entrypoint_error(&error);
        std::process::exit(20);
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "authoritative observer execution requires the native runner handshake"]
fn reference_profile_process_tree_observer() {
    run_linux_reference_observer_from_environment()
        .expect("authoritative process-tree observer must complete");
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "fixture helper is launched by the non-ignored observer tests"]
fn fixture_process_tree_observer_helper() {
    run_linux_fixture_observer_from_env().expect("fixture observer must complete");
}
