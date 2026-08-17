mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use common::workload::{self, *};
use herdr_top::herdr::controller::ControllerEnvelope;
use herdr_top::lockfile::StateRoot;
use herdr_top::model::{
    AgentNode, ControllerEventKind, DisplayOrdinal, DomainModel, EventMetadata,
    MinimalProviderMetadata, NormalizedEvent, Provider, RunKey, TaskState,
};
use herdr_top::reducer::{ApplyOutcome, Reducer};
use herdr_top::store::{PersistOp, RestoredState, open_reader, open_writer};

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
            "/tmp/herdr-increment5/sustained/trial-0001/observer-control.sock",
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
            "/tmp/herdr-increment5/sustained/trial-0001",
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
            "/tmp/herdr-increment5/sustained/trial-0001/observer-control.sock",
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

struct RawFixture {
    root: tempfile::TempDir,
}

impl RawFixture {
    fn new() -> Self {
        Self::from_outcome(valid_synthetic_result())
    }

    fn from_outcome(mut outcome: ReferenceOutcomeV1) -> Self {
        let fixture = Self {
            root: tempfile::tempdir().unwrap(),
        };
        write_synthetic_raw_scenario_root(fixture.root.path(), &mut outcome).unwrap();
        fixture
    }

    fn empty() -> Self {
        Self {
            root: tempfile::tempdir().unwrap(),
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
        read_and_validate_reference_outcome(&output)
            .unwrap()
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
        read_and_validate_reference_outcome(&output)
            .unwrap()
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
        read_and_validate_reference_outcome(&fixture.output_path("result-v1.json"))
            .unwrap()
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
            read_and_validate_reference_outcome(&malformed_request.output)
                .unwrap()
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
        let published = read_and_validate_reference_outcome(&output).unwrap();
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
                read_and_validate_reference_outcome(&output)
                    .unwrap()
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
            read_and_validate_reference_outcome(&output)
                .unwrap()
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
        read_and_validate_reference_outcome(&output)
            .unwrap()
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
        read_and_validate_reference_outcome(&output)
            .unwrap()
            .failure_reasons(),
        &[FailureReasonV1::InvalidArtifact]
    );
}

#[test]
#[ignore = "authoritative classification requires explicit result roots"]
fn classify_d4_checkpoint_from_results() {
    if classify_d4_checkpoint_from_environment().is_err() {
        std::process::exit(20);
    }
}

#[test]
#[ignore = "authoritative Section 15 re-derivation requires explicit result roots"]
fn rederive_section15_report_from_results() {
    if rederive_section15_report_from_environment().is_err() {
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
        Err(_) => std::process::exit(20),
    }
}

#[test]
#[ignore = "native runner supplies the closed validation environment"]
fn validate_reference_outcome() {
    match validate_reference_outcome_from_environment() {
        Ok(0) => {}
        Ok(code) => std::process::exit(code),
        Err(_) => std::process::exit(20),
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
