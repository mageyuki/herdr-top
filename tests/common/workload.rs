use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use herdr_top::model::{
    DependencyEdge, DisplayOrdinal, DomainModel, ExecutionEdge, Pane, RunId, RunKey, Tab, TaskRun,
    TaskState, Workspace,
};

fn developer_home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .expect("HOME must be set for the reference workload")
}

fn developer_home_path(relative: &str) -> String {
    developer_home()
        .join(relative)
        .into_os_string()
        .into_string()
        .expect("HOME must be valid UTF-8 for the reference workload")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadProfile {
    TargetTopology,
    SustainedTarget,
    TargetBurst,
    Startup,
    Idle,
    FallbackRescan,
    TwiceTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructuralIdentities {
    pub pane_ids: BTreeSet<String>,
    pub task_run_ids: BTreeSet<String>,
    pub dependency_edges: BTreeSet<String>,
    pub execution_edges: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadOracle {
    pub live_panes: usize,
    pub visible_runs: usize,
    pub dependency_edges: usize,
    pub execution_edges: usize,
    pub admitted_sequences: Vec<u64>,
    pub screen_probe_sequences: Vec<u64>,
    pub final_identities: StructuralIdentities,
}

pub fn oracle(profile: WorkloadProfile) -> WorkloadOracle {
    let model = target_model();
    let final_identities = structural_identities(&model);
    WorkloadOracle {
        live_panes: model.panes().count(),
        visible_runs: model.task_runs().count(),
        dependency_edges: model.dependency_edges().count(),
        execution_edges: model.execution_edges().count(),
        admitted_sequences: admission_offsets(profile)
            .iter()
            .enumerate()
            .map(|(index, _)| index as u64 + 1)
            .collect(),
        screen_probe_sequences: screen_probe_sequences(profile),
        final_identities,
    }
}

pub fn period(profile: WorkloadProfile) -> Duration {
    match profile {
        WorkloadProfile::SustainedTarget => Duration::from_millis(50),
        WorkloadProfile::TargetBurst => Duration::from_millis(10),
        WorkloadProfile::TwiceTarget => Duration::from_millis(25),
        WorkloadProfile::TargetTopology
        | WorkloadProfile::Startup
        | WorkloadProfile::Idle
        | WorkloadProfile::FallbackRescan => Duration::ZERO,
    }
}

pub fn admission_offsets(profile: WorkloadProfile) -> Vec<Duration> {
    let count = match profile {
        WorkloadProfile::SustainedTarget => 1_200,
        WorkloadProfile::TargetBurst => 1_000,
        WorkloadProfile::TwiceTarget => 2_400,
        WorkloadProfile::TargetTopology
        | WorkloadProfile::Startup
        | WorkloadProfile::Idle
        | WorkloadProfile::FallbackRescan => 0,
    };
    let cadence = period(profile);
    (1..=count).map(|sequence| cadence * sequence).collect()
}

pub fn screen_probe_sequences(profile: WorkloadProfile) -> Vec<u64> {
    let (count, stride) = match profile {
        WorkloadProfile::SustainedTarget => (1_200, 4),
        WorkloadProfile::TargetBurst => (1_000, 20),
        WorkloadProfile::TwiceTarget => (2_400, 8),
        WorkloadProfile::TargetTopology
        | WorkloadProfile::Startup
        | WorkloadProfile::Idle
        | WorkloadProfile::FallbackRescan => return Vec::new(),
    };
    (stride..=count).step_by(stride as usize).collect()
}

pub fn frozen_controller_events(profile: WorkloadProfile) -> Vec<serde_json::Value> {
    let count = match profile {
        WorkloadProfile::SustainedTarget => 1_200,
        WorkloadProfile::TargetBurst => 1_000,
        WorkloadProfile::TwiceTarget => 2_400,
        WorkloadProfile::TargetTopology
        | WorkloadProfile::Startup
        | WorkloadProfile::Idle
        | WorkloadProfile::FallbackRescan => return Vec::new(),
    };
    let probes = screen_probe_sequences(profile)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let period_ms = u64::try_from(period(profile).as_millis())
        .expect("frozen workload period must fit milliseconds");
    (1..=count)
        .map(|sequence| {
            let probe = probes.contains(&sequence);
            let run = if probe { 200 } else { (sequence - 1) % 199 + 1 };
            let task_run_id = format!("run-{run:04}");
            let label = if probe {
                format!("Task Run: run-0200 [probe-through:{sequence:04}]")
            } else {
                format!("Task Run: {task_run_id} [sequence:{sequence:04}]")
            };
            serde_json::json!({
                "schema_version": 1,
                "event_id": format!("increment5-{sequence:04}"),
                "emitted_at_ms": sequence * period_ms,
                "source": "increment5-harness",
                "event_type": "progress",
                "task_run_id": task_run_id,
                "parent_task_run_id": null,
                "depends_on_id": null,
                "label": label,
                "reason": null,
                "provider": null,
                "native_session_id": null,
                "terminal_id": null
            })
        })
        .collect()
}

pub fn target_model() -> DomainModel {
    let mut model = DomainModel::default();
    model.insert_workspace(Workspace {
        workspace_id: "workspace-0001".to_owned(),
    });
    model.insert_tab(Tab {
        tab_id: "tab-0001".to_owned(),
        workspace_id: "workspace-0001".to_owned(),
        label: None,
    });
    for index in 1..=50 {
        model.insert_pane(Pane {
            pane_id: format!("pane-{index:04}"),
            workspace_id: "workspace-0001".to_owned(),
            tab_id: "tab-0001".to_owned(),
            terminal_id: format!("terminal-{index:04}"),
            display_name: None,
        });
    }

    let run_ids = (1..=200).map(stable_run_id).collect::<Vec<_>>();
    for (index, run_id) in run_ids.iter().copied().enumerate() {
        let ordinal = index + 1;
        model.insert_task_run(TaskRun {
            run_id,
            key: RunKey::Controller(format!("run-{ordinal:04}")),
            display_ordinal: DisplayOrdinal::new(ordinal as i64),
            state: TaskState::Running,
            has_controller_task_state_event: true,
            created_at_ms: None,
            updated_at_ms: None,
            finished_at_ms: None,
            subject: None,
            dismissed_at_ms: None,
        });
    }
    for pair in run_ids.windows(2) {
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: pair[0],
            child_run_id: pair[1],
        });
    }
    let mut inserted = 0;
    'pairs: for dependent in 1..run_ids.len() {
        for prerequisite in 0..dependent {
            model.insert_dependency_edge(DependencyEdge {
                prerequisite_run_id: run_ids[prerequisite],
                dependent_run_id: run_ids[dependent],
            });
            inserted += 1;
            if inserted == 1_000 {
                break 'pairs;
            }
        }
    }
    model
}

fn stable_run_id(index: usize) -> RunId {
    RunId::parse(&format!("{index:026}"))
        .unwrap_or_else(|error| panic!("stable workload Run ID must parse: {error}"))
}

fn structural_identities(model: &DomainModel) -> StructuralIdentities {
    StructuralIdentities {
        pane_ids: model.panes().map(|pane| pane.pane_id.clone()).collect(),
        task_run_ids: model
            .task_runs()
            .map(|run| match &run.key {
                RunKey::Controller(key) => key.clone(),
                _ => unreachable!("target model contains only Controller task runs"),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioV1 {
    Target,
    Sustained,
    Burst,
    Startup,
    Idle,
    FallbackRescan,
    TwiceTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementStageV1 {
    Baseline,
    PostReliability,
    Final,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PidstatChildStatusModeV1 {
    PropagatesChildStatus,
    MonitorOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum TrialStatusV1 {
    Ok,
    Failed { exit_code: u8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadRenderViewV1 {
    ExecutionTree,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadOverlayV1 {
    None,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RenderSurfaceV1 {
    pub width: u16,
    pub height: u16,
    pub view: WorkloadRenderViewV1,
    pub follow: bool,
    pub filter_query: String,
    pub initial_selected_task_run_key: String,
    pub collapsed_task_run_keys: Vec<String>,
    pub overlay: WorkloadOverlayV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralIdentitiesV1 {
    pub pane_ids: Vec<String>,
    pub task_run_ids: Vec<String>,
    pub dependency_edges: Vec<String>,
    pub execution_edges: Vec<String>,
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FailureReasonV1 {
    ControlMismatch,
    CommandFailed,
    IncompleteTrial,
    SequenceLoss,
    StructuralLoss,
    ScreenLatency,
    InputLatency,
    StartupLatency,
    FallbackRescanLatency,
    IdleCpu,
    MaximumRss,
    WorkloadAdmission,
    SupportedLoadDegradation,
    MissingDegradation,
    DuplicateOutcome,
    InvalidArtifact,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionObservationV1 {
    pub sequence: u64,
    pub scheduled_ns: u64,
    pub admitted_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyObservationV1 {
    pub sequence: u64,
    pub admitted_ns: u64,
    pub terminal_ns: u64,
    pub published_ns: u64,
    pub rendered_ns: u64,
    pub observed_frame_phase_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputLatencyObservationV1 {
    pub scheduled_ns: u64,
    pub injected_ns: u64,
    pub rendered_ns: u64,
    pub observed_frame_phase_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopedTimingKindV1 {
    ControllerEvent,
    StartupRestore,
    FallbackNotification,
    FallbackRescan,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedTimingObservationV1 {
    pub kind: ScopedTimingKindV1,
    pub sequence: u64,
    pub d4_segment_count: u32,
    pub d4_analysis_ns: u64,
    pub reducer_plus_publish_ns: u64,
    pub model_clone_publish_segment_count: u32,
    pub model_clone_publish_ns: u64,
    pub render_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackPairObservationV1 {
    pub sequence: u64,
    pub notification_ns: u64,
    pub rescan_ns: u64,
    pub notification_final_identities: StructuralIdentitiesV1,
    pub rescan_final_identities: StructuralIdentitiesV1,
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceReasonV1 {
    LivePanes,
    DefaultVisibleTaskRuns,
    DependencyEdges,
    EventsOneSecond,
    EventsTenSeconds,
    EventsSixtySeconds,
    EventLag,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectiveQualityV1 {
    Live,
    Reconciling,
    Disconnected,
    Degraded,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalObservationV1 {
    pub sequence: u64,
    pub terminal_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceSampleEvidenceV1 {
    pub sample_ordinal: u64,
    pub sampled_at_ns: u64,
    pub event_lag_ns: u64,
    pub pending_events: u64,
    pub admission_high_water: u64,
    pub completion_high_water: u64,
    pub live_panes: u64,
    pub default_visible_task_runs: u64,
    pub dependency_edges: u64,
    pub execution_edges: u64,
    pub events_one_second: u64,
    pub events_ten_seconds: u64,
    pub events_sixty_seconds: u64,
    pub source_quality: EffectiveQualityV1,
    pub effective_quality: EffectiveQualityV1,
    pub reasons: Vec<PerformanceReasonV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceFrameEvidenceV1 {
    pub draw_ordinal: u64,
    pub sample_ordinal: u64,
    pub state_observed_at_ns: u64,
    pub rendered_at_ns: u64,
    pub effective_quality: EffectiveQualityV1,
    pub reasons: Vec<PerformanceReasonV1>,
    pub rendered_header_line: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceEvidenceStreamV1 {
    pub workload_start_ns: u64,
    pub workload_close_ns: u64,
    pub first_sample_ordinal: u64,
    pub next_sample_ordinal: u64,
    pub first_draw_ordinal: u64,
    pub next_draw_ordinal: u64,
    pub samples: Vec<PerformanceSampleEvidenceV1>,
    pub frames: Vec<PerformanceFrameEvidenceV1>,
    pub terminal_observations: Vec<TerminalObservationV1>,
    pub selected_terminal_draw_ordinal: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceObservationV1 {
    pub offset_ns: u64,
    pub process_tree_user_cpu_ns: u64,
    pub process_tree_system_cpu_ns: u64,
    pub process_tree_rss_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentityResourceV1 {
    pub pid: u32,
    pub start_time_ticks: u64,
    pub first_observed_offset_ns: u64,
    pub idle_window_start_user_cpu_ticks: Option<u64>,
    pub idle_window_start_system_cpu_ticks: Option<u64>,
    pub idle_window_end_user_cpu_ticks: Option<u64>,
    pub idle_window_end_system_cpu_ticks: Option<u64>,
    pub last_user_cpu_ticks: u64,
    pub last_system_cpu_ticks: u64,
    pub maximum_vm_hwm_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObserverCommandV1 {
    StartIdleWindow {},
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObserverControlFrameV1 {
    Ready {
        observer_ready_ns: u64,
    },
    IdleWindowStarted {
        request_received_ns: u64,
        start_ns: u64,
    },
    IdleWindowEnded {
        end_ns: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverControlEvidenceV1 {
    pub protocol_version: u32,
    pub scenario: ScenarioV1,
    pub observed_root_pid: u32,
    pub observed_root_start_time_ticks: u64,
    pub trial_origin_ns: u64,
    pub observer_ready_ns: u64,
    pub idle_window_start_ns: Option<u64>,
    pub idle_window_end_ns: Option<u64>,
    pub commands: Vec<ObserverCommandV1>,
    pub frames: Vec<ObserverControlFrameV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessTreeEvidenceV1 {
    pub observer_pid: u32,
    pub observer_affinity_cpu_ids: Vec<u32>,
    pub observed_root_pid: u32,
    pub observed_root_start_time_ticks: u64,
    pub clock_ticks_per_second: u64,
    pub trial_origin_ns: u64,
    pub observer_ready_ns: u64,
    pub idle_window_start_ns: Option<u64>,
    pub idle_window_end_ns: Option<u64>,
    pub resource_observations: Vec<ResourceObservationV1>,
    pub process_identity_resources: Vec<ProcessIdentityResourceV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawArtifactDigestsV1 {
    pub harness_json_sha256: String,
    pub runner_control_json_sha256: String,
    pub process_tree_json_sha256: String,
    pub observer_handshake_sha256: String,
    pub observer_control_json_sha256: String,
    pub gnu_time_sha256: String,
    pub pidstat_json_sha256: String,
    pub pidstat_stderr_sha256: String,
    pub child_stdout_sha256: String,
    pub child_stderr_sha256: String,
    pub observer_stdout_sha256: String,
    pub observer_stderr_sha256: String,
    pub trial_status_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResourceAuditV1 {
    pub gnu_elapsed_ns: u64,
    pub gnu_user_cpu_ns: u64,
    pub gnu_system_cpu_ns: u64,
    pub gnu_maximum_rss_bytes: u64,
    pub gnu_exit_status: i32,
    pub pidstat_child_user_cpu_ns: Option<u64>,
    pub pidstat_child_system_cpu_ns: Option<u64>,
    pub pidstat_wrapper_maximum_rss_bytes: Option<u64>,
    pub pidstat_sample_count: usize,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct DistributionV1 {
    pub sample_count: usize,
    pub minimum_ns: u64,
    pub median_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub maximum_ns: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct HostProfileV1 {
    pub operating_system: String,
    pub kernel: String,
    pub architecture: String,
    pub cpu_model: String,
    pub physical_core_ids: Vec<String>,
    pub memory_total_bytes: u64,
    pub storage_kind: String,
    pub storage_device: String,
    pub governor: Option<String>,
    pub boost: Option<String>,
    pub ambient_load_milli: [u64; 3],
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutableIdentityV1 {
    pub requested_path: String,
    pub canonical_path: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CargoConfigurationPolicyV1 {
    pub policy_version: u32,
    pub invocation_cwd: String,
    pub ordered_absent_candidates: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlsV1 {
    pub affinity_cpu_ids: Vec<u32>,
    pub address_space_limit_bytes: u64,
    pub true_cgroup_memory_limit: bool,
    pub toolchain_launcher: ExecutableIdentityV1,
    pub toolchain_name: String,
    pub rustc_version: String,
    pub cargo_version: String,
    pub build_environment: std::collections::BTreeMap<String, String>,
    pub cargo_configuration: CargoConfigurationPolicyV1,
    pub measured_binary: ExecutableIdentityV1,
    pub runner_script: ExecutableIdentityV1,
    pub authoritative_executables: Vec<ExecutableIdentityV1>,
    pub pidstat_child_status_mode: PidstatChildStatusModeV1,
    pub limitation: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildControlsV1 {
    pub effective_affinity_cpu_ids: Vec<u32>,
    pub effective_address_space_limit_bytes: u64,
    pub measured_environment: std::collections::BTreeMap<String, String>,
    pub scratch_root: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrialControlEvidenceV1 {
    pub scratch_root: String,
    pub scratch_storage_kind: String,
    pub scratch_storage_devices: Vec<String>,
    pub orchestrator_environment: std::collections::BTreeMap<String, String>,
    pub observer_environment: std::collections::BTreeMap<String, String>,
    pub validator_environment_template: std::collections::BTreeMap<String, String>,
    pub revalidated_executables: Vec<ExecutableIdentityV1>,
    pub revalidated_runner_script: ExecutableIdentityV1,
    pub revalidated_measured_binary: ExecutableIdentityV1,
    pub trial_status: TrialStatusV1,
    pub pidstat_exit_status: u8,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerControlEvidenceV1 {
    pub schema_version: u32,
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub trial_index: usize,
    pub canonical_raw_root: String,
    pub production_subject_sha: String,
    pub preflight_head: String,
    pub harness_sha: String,
    pub workload_schema_sha256: String,
    pub tracked_clean_before_composition: bool,
    pub build_profile: String,
    pub command: Vec<String>,
    pub controlled_environment: std::collections::BTreeMap<String, String>,
    pub render_surface: RenderSurfaceV1,
    pub host: HostProfileV1,
    pub controls: RunControlsV1,
    pub trial: TrialControlEvidenceV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdsV1 {
    pub screen_update_p95_ns_exclusive: u64,
    pub input_response_p95_ns_exclusive: u64,
    pub startup_ns_exclusive: u64,
    pub fallback_added_delay_ns_inclusive: u64,
    pub idle_cpu_milli_percent_exclusive: u64,
    pub process_tree_rss_bytes_exclusive: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessTrialV1 {
    pub scenario: ScenarioV1,
    pub trial_index: usize,
    pub trial_origin_ns: u64,
    pub priming_frame_recorded_ns: Option<u64>,
    pub workload_origin_ns: Option<u64>,
    pub frame_phase_offset_ns: Option<u64>,
    pub priming_frame_count: u32,
    pub admission_observations: Vec<AdmissionObservationV1>,
    pub screen_observations: Vec<LatencyObservationV1>,
    pub input_observations: Vec<InputLatencyObservationV1>,
    pub startup_observations_ns: Vec<u64>,
    pub fallback_pairs: Vec<FallbackPairObservationV1>,
    pub scoped_observations: Vec<ScopedTimingObservationV1>,
    pub submitted_sequences: Vec<u64>,
    pub admitted_sequences: Vec<u64>,
    pub completed_sequences: Vec<u64>,
    pub persisted_sequences: Vec<u64>,
    pub rendered_sequences: Vec<u64>,
    pub pane_ids: Vec<String>,
    pub task_run_ids: Vec<String>,
    pub dependency_edges: Vec<String>,
    pub execution_edges: Vec<String>,
    pub prepared_non_gap_event_count: Option<u64>,
    pub prepared_ledger_row_count: Option<u64>,
    pub restored_activity_count: Option<u64>,
    pub performance_evidence_stream: Option<PerformanceEvidenceStreamV1>,
    pub idle_window_start_ns: Option<u64>,
    pub idle_window_end_ns: Option<u64>,
    pub child_controls: ChildControlsV1,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrialResultV1 {
    pub trial_index: usize,
    pub raw: HarnessTrialV1,
    pub observer_control: ObserverControlEvidenceV1,
    pub process_tree: ProcessTreeEvidenceV1,
    pub raw_artifacts: RawArtifactDigestsV1,
    pub control_evidence: TrialControlEvidenceV1,
    pub screen_update: Option<DistributionV1>,
    pub reducer_lag: Option<DistributionV1>,
    pub publish_to_render: Option<DistributionV1>,
    pub input_response: Option<DistributionV1>,
    pub startup_ns: Option<u64>,
    pub elapsed_ns: u64,
    pub user_cpu_ns: u64,
    pub system_cpu_ns: u64,
    pub maximum_process_tree_rss_bytes: u64,
    pub sum_process_identity_peak_rss_bytes_diagnostic: u64,
    pub fallback_added_delay_ns: Option<DistributionV1>,
    pub d4_analysis_ns: Option<u64>,
    pub reducer_plus_publish_ns: Option<u64>,
    pub d4_ratio_parts_per_million: Option<u64>,
    pub external_resource_audit: ExternalResourceAuditV1,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceRunV1 {
    pub schema_version: u32,
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub production_subject_sha: String,
    pub harness_sha: String,
    pub workload_schema_sha256: String,
    pub baseline_id: String,
    pub tracked_clean: bool,
    pub build_profile: String,
    pub command: Vec<String>,
    pub controlled_environment: std::collections::BTreeMap<String, String>,
    pub render_surface: RenderSurfaceV1,
    pub host: HostProfileV1,
    pub controls: RunControlsV1,
    pub thresholds: ThresholdsV1,
    pub warm_up_trials: usize,
    pub recorded_trials: usize,
    pub trials: Vec<TrialResultV1>,
    pub failure_reasons: Vec<FailureReasonV1>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvalidRunV1 {
    pub schema_version: u32,
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub production_subject_sha: String,
    pub harness_sha: String,
    pub workload_schema_sha256: String,
    pub baseline_id: Option<String>,
    pub command: Vec<String>,
    pub controlled_environment: std::collections::BTreeMap<String, String>,
    pub failure_reasons: Vec<FailureReasonV1>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReferenceOutcomeV1 {
    Pass { document: ReferenceRunV1 },
    Failed { document: ReferenceRunV1 },
    Invalid { document: InvalidRunV1 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AmendedLegacyMode {
    Off,
    AcceptAmendedLegacy,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReclassificationRecordV1 {
    pub scenario: ScenarioV1,
    pub recorded_failure_reasons: Vec<FailureReasonV1>,
}

#[derive(Debug)]
pub struct ReferenceOutcomeRead {
    pub outcome: ReferenceOutcomeV1,
    pub reclassified: Option<ReclassificationRecordV1>,
}

#[derive(serde::Serialize)]
struct ReclassificationSidecarV1<'a> {
    schema_version: u32,
    rule: &'static str,
    reclassified: &'a [ReclassificationRecordV1],
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerTestOutcomeV1 {
    pub schema_version: u32,
    pub non_authoritative: bool,
    pub exit_code: i32,
    pub all_process_groups_reaped: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceOutcomeStatusV1 {
    Pass,
    Failed,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum D4PolicyV1 {
    NotApplicable,
    NonD4,
    D4Scoped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Section15MetricV1 {
    ScreenUpdate,
    InputResponse,
    Startup,
    IdleCpu,
    MaximumProcessTreeRss,
    FallbackAddedDelay,
    AdmissionDeadline,
    SubmittedSequences,
    AdmittedSequences,
    CompletedSequences,
    PersistedSequences,
    RenderedProbeSequences,
    ReducerLag,
    PublishToRender,
    PerformanceDegradation,
    D4Analysis,
    ReducerPlusPublish,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Section15UnitV1 {
    Nanoseconds,
    Bytes,
    MilliPercent,
    Count,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ThresholdComparisonV1 {
    LessThan,
    LessThanOrEqual,
    Equal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributionStatisticV1 {
    Minimum,
    Median,
    P95,
    P99,
    Maximum,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectedResultIdentityV1 {
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub canonical_result_path: String,
    pub canonical_raw_root: String,
    pub result_sha256: String,
    pub production_subject_sha: String,
    pub harness_sha: String,
    pub workload_schema_sha256: String,
    pub baseline_id: String,
    pub measured_binary: ExecutableIdentityV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15DistributionV1 {
    pub metric: Section15MetricV1,
    pub unit: Section15UnitV1,
    pub sample_count: u64,
    pub minimum: String,
    pub median: String,
    pub p95: String,
    pub p99: String,
    pub maximum: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15PredicateV1 {
    pub metric: Section15MetricV1,
    pub unit: Section15UnitV1,
    pub ordinal: Option<u64>,
    pub observed_numerator: String,
    pub observed_denominator: Option<String>,
    pub comparison: ThresholdComparisonV1,
    pub threshold_numerator: String,
    pub threshold_denominator: Option<String>,
    pub passed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15SequenceCountsV1 {
    pub submitted: u64,
    pub admitted: u64,
    pub completed: u64,
    pub persisted: u64,
    pub rendered_probes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15TrialReDerivationV1 {
    pub trial_index: u64,
    pub sequence_counts: Section15SequenceCountsV1,
    pub admission_buckets_attained: Option<bool>,
    pub lossless: bool,
    pub structural_identities_match: bool,
    pub distributions: Vec<Section15DistributionV1>,
    pub predicates: Vec<Section15PredicateV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15ScenarioReDerivationV1 {
    pub scenario: ScenarioV1,
    pub baseline_status: ReferenceOutcomeStatusV1,
    pub final_status: ReferenceOutcomeStatusV1,
    pub final_failure_reasons: Vec<FailureReasonV1>,
    pub trials: Vec<Section15TrialReDerivationV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15BaselineDeltaV1 {
    pub scenario: ScenarioV1,
    pub trial_index: u64,
    pub metric: Section15MetricV1,
    pub statistic: DistributionStatisticV1,
    pub unit: Section15UnitV1,
    pub baseline_value: String,
    pub final_value: String,
    pub signed_delta: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15FailurePolicyEvidenceV1 {
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub failure_reason: FailureReasonV1,
    pub policy: D4PolicyV1,
    pub d4_analysis_sum: Option<String>,
    pub reducer_plus_publish_sum: Option<String>,
    pub d4_exact_quarter_predicate: Option<bool>,
    pub required_amendment: Option<RequiredAmendmentV1>,
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RequiredAmendmentV1 {
    D4,
    NonD4,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum D4CheckpointDecisionV1 {
    NoMissD4NotAuthorized {},
    AmendmentsRequired {
        amendments: Vec<RequiredAmendmentV1>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct D4CheckpointDocumentV1 {
    pub schema_version: u32,
    pub decision: D4CheckpointDecisionV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15ReDerivationV1 {
    pub schema_version: u32,
    pub subject_sha: String,
    pub baseline_id: String,
    pub selected_results: Vec<SelectedResultIdentityV1>,
    pub scenarios: Vec<Section15ScenarioReDerivationV1>,
    pub baseline_deltas: Vec<Section15BaselineDeltaV1>,
    pub failure_policy_evidence: Vec<Section15FailurePolicyEvidenceV1>,
    pub decision: D4CheckpointDecisionV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResultError {
    #[error("recorded trials are incomplete")]
    IncompleteTrials,
    #[error("admitted sequence coverage is not lossless")]
    SequenceCoverage,
    #[error("a sequence outcome is duplicated")]
    DuplicateOutcome,
    #[error("final structural identities differ from the oracle")]
    StructuralMismatch,
    #[error("a measured threshold failed")]
    Threshold,
    #[error("required reference controls were not proven")]
    InvalidControl,
    #[error("raw tool or harness evidence is missing or inconsistent")]
    InvalidArtifact,
}

#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("harness I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("store operation failed: {0}")]
    Store(#[from] herdr_top::store::StoreError),
    #[error("result encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("harness invariant failed: {0}")]
    Invalid(&'static str),
}

#[allow(clippy::manual_div_ceil)] // Frozen v1 formula from the approved protocol.
pub fn percentile(sorted: &[u64], percentile: u32) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((sorted.len() as u128 * percentile as u128) + 99) / 100;
    sorted.get(rank.saturating_sub(1) as usize).copied()
}

impl ReferenceOutcomeV1 {
    pub fn status(&self) -> ReferenceOutcomeStatusV1 {
        match self {
            Self::Pass { .. } => ReferenceOutcomeStatusV1::Pass,
            Self::Failed { .. } => ReferenceOutcomeStatusV1::Failed,
            Self::Invalid { .. } => ReferenceOutcomeStatusV1::Invalid,
        }
    }

    pub fn document(&self) -> &ReferenceRunV1 {
        match self {
            Self::Pass { document } | Self::Failed { document } => document,
            Self::Invalid { .. } => panic!("invalid outcomes do not contain trial aggregates"),
        }
    }

    pub fn document_mut(&mut self) -> &mut ReferenceRunV1 {
        match self {
            Self::Pass { document } | Self::Failed { document } => document,
            Self::Invalid { .. } => panic!("invalid outcomes do not contain trial aggregates"),
        }
    }

    pub fn failure_reasons(&self) -> &[FailureReasonV1] {
        match self {
            Self::Pass { document } | Self::Failed { document } => &document.failure_reasons,
            Self::Invalid { document } => &document.failure_reasons,
        }
    }

    pub fn validate(&self) -> Result<(), ResultError> {
        match self {
            Self::Pass { document } => {
                let derived = validate_reference_run(document, false)?;
                if !derived.is_empty() {
                    return Err(ResultError::Threshold);
                }
                if !document.failure_reasons.is_empty() {
                    return Err(ResultError::InvalidArtifact);
                }
                Ok(())
            }
            Self::Failed { document } => {
                let derived = validate_reference_run(document, false)?;
                if derived.is_empty()
                    || document.failure_reasons != derived.into_iter().collect::<Vec<_>>()
                {
                    return Err(ResultError::InvalidArtifact);
                }
                for reason in &document.failure_reasons {
                    let Some(row) =
                        failure_policy_row(document.measurement_stage, document.scenario, *reason)
                    else {
                        return Err(ResultError::InvalidArtifact);
                    };
                    if row.outcome != ReferenceOutcomeStatusV1::Failed {
                        return Err(ResultError::InvalidArtifact);
                    }
                }
                Ok(())
            }
            Self::Invalid { document } => validate_invalid_run(document),
        }
    }
}

impl D4CheckpointDocumentV1 {
    pub fn validate(&self) -> Result<BTreeSet<RequiredAmendmentV1>, ResultError> {
        if self.schema_version != 1 {
            return Err(ResultError::InvalidArtifact);
        }
        match &self.decision {
            D4CheckpointDecisionV1::NoMissD4NotAuthorized {} => Ok(BTreeSet::new()),
            D4CheckpointDecisionV1::AmendmentsRequired { amendments }
                if !amendments.is_empty()
                    && amendments.windows(2).all(|window| window[0] < window[1]) =>
            {
                Ok(amendments.iter().copied().collect())
            }
            D4CheckpointDecisionV1::AmendmentsRequired { .. } => Err(ResultError::InvalidArtifact),
        }
    }
}

impl Section15ReDerivationV1 {
    pub fn validate(&self) -> Result<(), ResultError> {
        self.validate_with_mode(AmendedLegacyMode::Off)
    }

    pub fn validate_with_mode(&self, legacy: AmendedLegacyMode) -> Result<(), ResultError> {
        validate_section15_selected_evidence(self, legacy)
    }
}

fn validate_reference_run(
    document: &ReferenceRunV1,
    allow_command_failure: bool,
) -> Result<BTreeSet<FailureReasonV1>, ResultError> {
    if document.schema_version != 1
        || !is_lower_hex(&document.production_subject_sha, 40)
        || !is_lower_hex(&document.harness_sha, 40)
        || document.workload_schema_sha256 != WORKLOAD_SCHEMA_V1_SHA256
        || !document.tracked_clean
        || document.build_profile != "release"
        || document.render_surface != workload_schema().render_surface
        || document.thresholds != workload_schema().thresholds
        || document.warm_up_trials != 1
    {
        return Err(ResultError::InvalidArtifact);
    }
    if document.measurement_stage == MeasurementStageV1::Baseline
        && document.production_subject_sha != BASELINE_SUBJECT_SHA
    {
        return Err(ResultError::InvalidControl);
    }
    if !baseline_id_is_valid(
        &document.baseline_id,
        document.measurement_stage,
        &document.harness_sha,
    ) {
        return Err(ResultError::InvalidControl);
    }
    let spec = scenario_spec(document.scenario);
    if document.recorded_trials != spec.recorded_trials
        || document.command != ["workload_harness", spec.cli_token.as_str()]
        || !run_environment_is_valid(
            &document.controlled_environment,
            document.measurement_stage,
            document.scenario,
            &document.production_subject_sha,
        )
    {
        return Err(ResultError::InvalidArtifact);
    }
    validate_run_controls(&document.controls)?;
    validate_host_profile(&document.host)?;
    if document.trials.len() != document.recorded_trials {
        return Err(ResultError::IncompleteTrials);
    }
    if !allow_command_failure
        && document.trials.iter().any(|trial| {
            matches!(
                trial.control_evidence.trial_status,
                TrialStatusV1::Failed { .. }
            )
        })
    {
        return Err(ResultError::InvalidArtifact);
    }
    if !strictly_sorted_unique(&document.failure_reasons) {
        return Err(ResultError::InvalidArtifact);
    }
    let mut scratch_roots = BTreeSet::new();
    let mut failures = BTreeSet::new();
    for (index, trial) in document.trials.iter().enumerate() {
        let trial_index = index + 1;
        if trial.trial_index != trial_index || trial.raw.trial_index != trial_index {
            return Err(ResultError::InvalidArtifact);
        }
        if !scratch_roots.insert(trial.raw.child_controls.scratch_root.clone()) {
            return Err(ResultError::InvalidArtifact);
        }
        failures.extend(validate_trial(document, trial)?);
    }
    Ok(failures)
}

fn validate_host_profile(host: &HostProfileV1) -> Result<(), ResultError> {
    if host.operating_system != "linux"
        || host.kernel.is_empty()
        || host.architecture.is_empty()
        || host.cpu_model.is_empty()
        || host.physical_core_ids.is_empty()
        || !strictly_sorted_unique(&host.physical_core_ids)
        || host.memory_total_bytes == 0
        || host.storage_kind.is_empty()
        || host.storage_device.is_empty()
        || host.governor.as_deref().is_some_and(str::is_empty)
        || host.boost.as_deref().is_some_and(str::is_empty)
    {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

// The run envelope's ambient_load_milli is anchored to the FIRST recorded
// trial by design (design lines 202/426-427: ambient is recorded context,
// never a gate); every non-ambient host field is freshly resampled per
// trial and must stay byte-identical or the run fails closed.
pub fn freeze_run_host_profile(
    mut current: HostProfileV1,
    first: Option<&HostProfileV1>,
) -> Result<HostProfileV1, HarnessError> {
    let Some(first) = first else {
        return Ok(current);
    };
    current.ambient_load_milli = first.ambient_load_milli;
    if current != *first {
        return Err(HarnessError::Invalid(
            "run host profile changed after the first recorded trial",
        ));
    }
    Ok(first.clone())
}

fn validate_invalid_run(document: &InvalidRunV1) -> Result<(), ResultError> {
    if document.schema_version != 1
        || !is_lower_hex(&document.production_subject_sha, 40)
        || !is_lower_hex(&document.harness_sha, 40)
        || document.workload_schema_sha256 != WORKLOAD_SCHEMA_V1_SHA256
        || document.measurement_stage == MeasurementStageV1::Baseline
            && document.production_subject_sha != BASELINE_SUBJECT_SHA
        || document.baseline_id.as_deref().is_some_and(|baseline_id| {
            !baseline_id_is_valid(
                baseline_id,
                document.measurement_stage,
                &document.harness_sha,
            )
        })
        || document.command
            != [
                "workload_harness",
                scenario_spec(document.scenario).cli_token.as_str(),
            ]
        || document.failure_reasons.is_empty()
        || !strictly_sorted_unique(&document.failure_reasons)
        || !run_environment_is_valid(
            &document.controlled_environment,
            document.measurement_stage,
            document.scenario,
            &document.production_subject_sha,
        )
    {
        return Err(ResultError::InvalidArtifact);
    }
    for reason in &document.failure_reasons {
        let Some(row) = failure_policy_row(document.measurement_stage, document.scenario, *reason)
        else {
            return Err(ResultError::InvalidArtifact);
        };
        if row.outcome != ReferenceOutcomeStatusV1::Invalid
            || row.d4_policy != D4PolicyV1::NotApplicable
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    Ok(())
}

fn baseline_id_is_valid(
    baseline_id: &str,
    measurement_stage: MeasurementStageV1,
    harness_sha: &str,
) -> bool {
    let parts = baseline_id.split(':').collect::<Vec<_>>();
    parts.len() == 5
        && parts[0] == "sha256"
        && parts[1] == "v1"
        && parts[2] == BASELINE_SUBJECT_SHA
        && is_lower_hex(parts[3], 40)
        && parts[4] == WORKLOAD_SCHEMA_V1_SHA256
        && (measurement_stage != MeasurementStageV1::Baseline || parts[3] == harness_sha)
}

fn run_environment_is_valid(
    actual: &std::collections::BTreeMap<String, String>,
    measurement_stage: MeasurementStageV1,
    scenario: ScenarioV1,
    production_subject_sha: &str,
) -> bool {
    let mut expected = invariant_environment();
    expected.insert(
        "HERDR_PERF_SCENARIO".to_owned(),
        scenario_spec(scenario).cli_token.clone(),
    );
    expected.insert(
        "HERDR_PERF_STAGE".to_owned(),
        stage_cli_token(measurement_stage).to_owned(),
    );
    expected.insert(
        "HERDR_PERF_SUBJECT".to_owned(),
        production_subject_sha.to_owned(),
    );
    if measurement_stage == MeasurementStageV1::Baseline {
        actual == &expected
    } else {
        let Some(root) = actual.get("HERDR_PERF_BASELINE_RESULTS_ROOT") else {
            return false;
        };
        if !std::path::Path::new(root).is_absolute() {
            return false;
        }
        expected.insert("HERDR_PERF_BASELINE_RESULTS_ROOT".to_owned(), root.clone());
        actual == &expected
    }
}

fn validate_run_controls(controls: &RunControlsV1) -> Result<(), ResultError> {
    let synthetic_rustc = controls.rustc_version == "rustc 1.97.1 (synthetic)";
    let synthetic_cargo = controls.cargo_version == "cargo 1.97.1 (synthetic)";
    let mut expected_synthetic = synthetic_run_controls();
    expected_synthetic.pidstat_child_status_mode = controls.pidstat_child_status_mode;
    if synthetic_rustc != synthetic_cargo || synthetic_rustc && controls != &expected_synthetic {
        return Err(ResultError::InvalidArtifact);
    }
    if controls.affinity_cpu_ids != [0, 1, 2, 3]
        || controls.address_space_limit_bytes != 16 * 1024 * 1024 * 1024
        || controls.true_cgroup_memory_limit
        || controls.toolchain_name != "1.97.1"
        || !matches!(
            controls.rustc_version.as_str(),
            "rustc 1.97.1 (synthetic)" | "rustc 1.97.1 (8bab26f4f 2026-07-14)"
        )
        || !matches!(
            controls.cargo_version.as_str(),
            "cargo 1.97.1 (synthetic)" | "cargo 1.97.1 (c980f4866 2026-06-30)"
        )
        || controls.build_environment != invariant_environment()
        || controls.cargo_configuration.policy_version != 1
        || controls.limitation != "address-space cap is not a true cgroup memory limit"
    {
        return Err(ResultError::InvalidArtifact);
    }
    let cwd = std::path::Path::new(&controls.cargo_configuration.invocation_cwd);
    let expected_absent = cargo_configuration_candidates(cwd)?;
    if controls.cargo_configuration.ordered_absent_candidates
        != expected_absent
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
        || cargo_configuration_candidates_are_absent(&expected_absent).is_err()
    {
        return Err(ResultError::InvalidArtifact);
    }
    let expected_requested = authoritative_executables()
        .into_iter()
        .map(|identity| identity.requested_path)
        .collect::<Vec<_>>();
    let rustup_requested = developer_home_path(".cargo/bin/rustup");
    if controls
        .authoritative_executables
        .iter()
        .map(|identity| &identity.requested_path)
        .ne(expected_requested.iter())
        || controls.authoritative_executables.iter().any(|identity| {
            !executable_identity_is_well_formed(identity)
                || identity.requested_path == rustup_requested
                    && std::path::Path::new(&identity.canonical_path)
                        .components()
                        .any(|component| component.as_os_str() == "mise")
        })
        || !executable_identity_is_well_formed(&controls.measured_binary)
        || !executable_identity_is_well_formed(&controls.runner_script)
        || controls.authoritative_executables.first() != Some(&controls.toolchain_launcher)
        || controls
            .authoritative_executables
            .iter()
            .filter(|identity| **identity == controls.toolchain_launcher)
            .count()
            != 1
    {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn cargo_configuration_candidates(
    invocation_cwd: &std::path::Path,
) -> Result<Vec<std::path::PathBuf>, ResultError> {
    if !invocation_cwd.is_absolute()
        || invocation_cwd.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(ResultError::InvalidArtifact);
    }
    let mut seen = BTreeSet::new();
    let mut candidates = Vec::new();
    for directory in invocation_cwd.ancestors() {
        for name in ["config", "config.toml"] {
            let candidate = directory.join(".cargo").join(name);
            if seen.insert(candidate.clone()) {
                candidates.push(candidate);
            }
        }
    }
    for candidate in [
        developer_home().join(".cargo/config"),
        developer_home().join(".cargo/config.toml"),
    ] {
        if seen.insert(candidate.clone()) {
            candidates.push(candidate);
        }
    }
    Ok(candidates)
}

fn cargo_configuration_candidates_are_absent(
    candidates: &[std::path::PathBuf],
) -> Result<(), ResultError> {
    for candidate in candidates {
        match std::fs::symlink_metadata(candidate) {
            Ok(_) => return Err(ResultError::InvalidArtifact),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(ResultError::InvalidArtifact),
        }
    }
    Ok(())
}

fn executable_identity_is_well_formed(identity: &ExecutableIdentityV1) -> bool {
    std::path::Path::new(&identity.requested_path).is_absolute()
        && std::path::Path::new(&identity.canonical_path).is_absolute()
        && !identity.requested_path.contains(['\t', '\n', '\r', '\0'])
        && !identity.canonical_path.contains(['\t', '\n', '\r', '\0'])
        && is_lower_hex(&identity.sha256, 64)
}

fn validate_trial(
    document: &ReferenceRunV1,
    trial: &TrialResultV1,
) -> Result<BTreeSet<FailureReasonV1>, ResultError> {
    let raw = &trial.raw;
    let spec = scenario_spec(document.scenario);
    if raw.scenario != document.scenario
        || trial.observer_control.scenario != document.scenario
        || !raw_artifact_digests_are_well_formed(&trial.raw_artifacts)
    {
        return Err(ResultError::InvalidArtifact);
    }
    validate_trial_controls(document, trial)?;
    validate_external_resource_audit(document.scenario, trial)?;
    validate_observer_evidence(
        document.scenario,
        raw,
        &trial.observer_control,
        &trial.process_tree,
    )?;
    validate_structural_identities(raw)?;
    let mut failures = BTreeSet::new();
    validate_phase_and_schedule(raw, spec, trial.observer_control.observer_ready_ns)?;
    validate_sequences(raw, spec)?;
    if spec.admission_count > 0 {
        let attained = admission_schedule_attained(
            scenario_profile(document.scenario),
            raw.workload_origin_ns.ok_or(ResultError::InvalidArtifact)?,
            &raw.admission_observations,
        )?;
        if !attained {
            failures.insert(FailureReasonV1::WorkloadAdmission);
        }
    }
    validate_latency_and_aggregates(document.scenario, trial, &mut failures)?;
    validate_scenario_matrix(document, trial, &mut failures)?;
    validate_scoped_observations(document.scenario, trial)?;
    validate_resource_aggregates(
        document.scenario,
        trial,
        &document.failure_reasons,
        &mut failures,
    )?;
    validate_performance_stream(document, trial, &mut failures)?;
    Ok(failures)
}

fn validate_external_resource_audit(
    scenario: ScenarioV1,
    trial: &TrialResultV1,
) -> Result<(), ResultError> {
    let audit = &trial.external_resource_audit;
    if matches!(trial.control_evidence.trial_status, TrialStatusV1::Ok)
        && audit.gnu_exit_status != 0
    {
        return Err(ResultError::InvalidArtifact);
    }
    let pidstat_values = [
        audit.pidstat_child_user_cpu_ns,
        audit.pidstat_child_system_cpu_ns,
        audit.pidstat_wrapper_maximum_rss_bytes,
    ];
    if audit.pidstat_sample_count == 0 {
        if scenario == ScenarioV1::Idle || pidstat_values.iter().any(Option::is_some) {
            return Err(ResultError::InvalidArtifact);
        }
    } else if pidstat_values.iter().any(Option::is_none) {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn validate_trial_controls(
    document: &ReferenceRunV1,
    trial: &TrialResultV1,
) -> Result<(), ResultError> {
    let scratch_root = &trial.raw.child_controls.scratch_root;
    let raw_root = scratch_root
        .strip_suffix("/scratch")
        .ok_or(ResultError::InvalidArtifact)?;
    let scratch_root_path = std::path::Path::new(scratch_root.as_str());
    let raw_root_path = std::path::Path::new(raw_root);
    let control_socket = trial
        .raw
        .child_controls
        .measured_environment
        .get("HERDR_PERF_OBSERVER_CONTROL_SOCKET")
        .ok_or(ResultError::InvalidArtifact)?;
    let control_socket_path = std::path::Path::new(control_socket);
    let baseline_root = document
        .controlled_environment
        .get("HERDR_PERF_BASELINE_RESULTS_ROOT")
        .map(std::path::Path::new);
    let expected_suffix = format!(
        "/{}/trial-{:04}/scratch",
        scenario_spec(document.scenario).directory,
        trial.trial_index
    );
    if !absolute_path_text_is_canonical(scratch_root_path)
        || !absolute_path_text_is_canonical(raw_root_path)
        || !scratch_root.ends_with(&expected_suffix)
        || !absolute_path_text_is_canonical(control_socket_path)
        || !control_socket.starts_with("/tmp/herdr-i5.")
        || control_socket_path.starts_with(raw_root)
        || trial.raw.child_controls.effective_affinity_cpu_ids != document.controls.affinity_cpu_ids
        || trial.raw.child_controls.effective_address_space_limit_bytes
            != document.controls.address_space_limit_bytes
        || trial.raw.child_controls.measured_environment
            != measured_environment(
                raw_root,
                scratch_root,
                control_socket,
                document.measurement_stage,
                document.scenario,
                &document.production_subject_sha,
                baseline_root,
            )
        || trial.control_evidence.scratch_root != *scratch_root
        || trial.control_evidence.scratch_storage_kind != document.host.storage_kind
        || trial.control_evidence.scratch_storage_devices != [document.host.storage_device.clone()]
        || trial.control_evidence.orchestrator_environment != invariant_environment()
        || trial.control_evidence.observer_environment
            != observer_environment(
                raw_root,
                control_socket,
                document.scenario,
                &trial.process_tree,
            )
        || trial.control_evidence.validator_environment_template != invariant_environment()
        || trial.control_evidence.revalidated_executables
            != document.controls.authoritative_executables
        || trial.control_evidence.revalidated_runner_script != document.controls.runner_script
        || trial.control_evidence.revalidated_measured_binary != document.controls.measured_binary
        || !pidstat_status_is_consistent(
            document.controls.pidstat_child_status_mode,
            trial.control_evidence.trial_status,
            trial.control_evidence.pidstat_exit_status,
        )
    {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn observer_environment(
    raw_root: &str,
    control_socket: &str,
    scenario: ScenarioV1,
    process_tree: &ProcessTreeEvidenceV1,
) -> std::collections::BTreeMap<String, String> {
    let mut values = invariant_environment();
    values.insert(
        "HERDR_PERF_SCENARIO".to_owned(),
        scenario_spec(scenario).cli_token.clone(),
    );
    values.insert(
        "HERDR_PERF_OBSERVED_ROOT_PID".to_owned(),
        process_tree.observed_root_pid.to_string(),
    );
    values.insert(
        "HERDR_PERF_OBSERVED_ROOT_START_TICKS".to_owned(),
        process_tree.observed_root_start_time_ticks.to_string(),
    );
    values.insert(
        "HERDR_PERF_TRIAL_ORIGIN_NS".to_owned(),
        process_tree.trial_origin_ns.to_string(),
    );
    values.insert(
        "HERDR_PERF_OBSERVER_CONTROL_SOCKET".to_owned(),
        control_socket.to_owned(),
    );
    values.insert(
        "HERDR_PERF_OBSERVER_CONTROL_OUTPUT".to_owned(),
        format!("{raw_root}/observer-control.json"),
    );
    values.insert(
        "HERDR_PERF_PROCESS_TREE_OUTPUT".to_owned(),
        format!("{raw_root}/process-tree.json"),
    );
    values
}

fn pidstat_status_is_consistent(
    mode: PidstatChildStatusModeV1,
    trial_status: TrialStatusV1,
    pidstat_exit_status: u8,
) -> bool {
    match (mode, trial_status) {
        (PidstatChildStatusModeV1::PropagatesChildStatus, TrialStatusV1::Ok) => {
            pidstat_exit_status == 0
        }
        (PidstatChildStatusModeV1::PropagatesChildStatus, TrialStatusV1::Failed { exit_code }) => {
            pidstat_exit_status == exit_code
        }
        (PidstatChildStatusModeV1::MonitorOnly, _) => pidstat_exit_status == 0,
    }
}

fn validate_observer_evidence(
    scenario: ScenarioV1,
    raw: &HarnessTrialV1,
    control: &ObserverControlEvidenceV1,
    tree: &ProcessTreeEvidenceV1,
) -> Result<(), ResultError> {
    let identity_keys = tree
        .process_identity_resources
        .iter()
        .map(|identity| (identity.pid, identity.start_time_ticks))
        .collect::<BTreeSet<_>>();
    if control.protocol_version != 1
        || control.observed_root_pid != tree.observed_root_pid
        || control.observed_root_start_time_ticks != tree.observed_root_start_time_ticks
        || raw.trial_origin_ns != control.trial_origin_ns
        || control.trial_origin_ns != tree.trial_origin_ns
        || control.observer_ready_ns != tree.observer_ready_ns
        || control.observer_ready_ns < control.trial_origin_ns
        || control.observer_ready_ns - control.trial_origin_ns > 5_000_000_000
        || tree.clock_ticks_per_second == 0
        || tree.observer_affinity_cpu_ids != [4, 5, 6, 7, 12, 13, 14, 15]
        || tree.resource_observations.is_empty()
        || tree
            .resource_observations
            .windows(2)
            .any(|window| window[0].offset_ns >= window[1].offset_ns)
        || identity_keys.len() != tree.process_identity_resources.len()
        || tree.observer_pid == tree.observed_root_pid
        || tree.resource_observations[0].offset_ns
            > control.observer_ready_ns - control.trial_origin_ns
        || tree
            .process_identity_resources
            .iter()
            .any(|identity| identity.pid == tree.observer_pid)
        || !tree.process_identity_resources.iter().any(|identity| {
            identity.pid == tree.observed_root_pid
                && identity.start_time_ticks == tree.observed_root_start_time_ticks
                && identity.first_observed_offset_ns
                    <= control.observer_ready_ns - control.trial_origin_ns
        })
    {
        return Err(ResultError::InvalidArtifact);
    }
    if scenario == ScenarioV1::Idle {
        let (Some(start), Some(end)) = (control.idle_window_start_ns, control.idle_window_end_ns)
        else {
            return Err(ResultError::InvalidArtifact);
        };
        if raw.idle_window_start_ns != Some(start)
            || raw.idle_window_end_ns != Some(end)
            || tree.idle_window_start_ns != Some(start)
            || tree.idle_window_end_ns != Some(end)
            || end.checked_sub(start).ok_or(ResultError::InvalidArtifact)? < 30_000_000_000
            || control.commands != [ObserverCommandV1::StartIdleWindow {}]
            || control.frames.len() != 3
        {
            return Err(ResultError::InvalidArtifact);
        }
        match &control.frames[..] {
            [
                ObserverControlFrameV1::Ready { observer_ready_ns },
                ObserverControlFrameV1::IdleWindowStarted {
                    request_received_ns,
                    start_ns,
                },
                ObserverControlFrameV1::IdleWindowEnded { end_ns },
            ] if *observer_ready_ns == control.observer_ready_ns
                && *observer_ready_ns <= *request_received_ns
                && *request_received_ns <= *start_ns
                && *start_ns == start
                && *start_ns < *end_ns
                && *end_ns == end => {}
            _ => return Err(ResultError::InvalidArtifact),
        }
    } else if raw.idle_window_start_ns.is_some()
        || raw.idle_window_end_ns.is_some()
        || control.idle_window_start_ns.is_some()
        || control.idle_window_end_ns.is_some()
        || tree.idle_window_start_ns.is_some()
        || tree.idle_window_end_ns.is_some()
        || !control.commands.is_empty()
        || tree.process_identity_resources.iter().any(|identity| {
            identity.idle_window_start_user_cpu_ticks.is_some()
                || identity.idle_window_start_system_cpu_ticks.is_some()
                || identity.idle_window_end_user_cpu_ticks.is_some()
                || identity.idle_window_end_system_cpu_ticks.is_some()
        })
        || control.frames
            != [ObserverControlFrameV1::Ready {
                observer_ready_ns: control.observer_ready_ns,
            }]
    {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn validate_structural_identities(raw: &HarnessTrialV1) -> Result<(), ResultError> {
    for values in [
        &raw.pane_ids,
        &raw.task_run_ids,
        &raw.dependency_edges,
        &raw.execution_edges,
    ] {
        if values.iter().collect::<BTreeSet<_>>().len() != values.len() {
            return Err(ResultError::InvalidArtifact);
        }
    }
    let expected = target_identities_v1();
    if raw.pane_ids != expected.pane_ids
        || raw.task_run_ids != expected.task_run_ids
        || raw.dependency_edges != expected.dependency_edges
        || raw.execution_edges != expected.execution_edges
    {
        return Err(ResultError::StructuralMismatch);
    }
    Ok(())
}

fn validate_phase_and_schedule(
    raw: &HarnessTrialV1,
    spec: &ScenarioManifestV1,
    observer_ready_ns: u64,
) -> Result<(), ResultError> {
    let expected_phase = spec
        .frame_phase_offsets_ns
        .get(raw.trial_index.saturating_sub(1))
        .copied();
    match expected_phase {
        Some(phase) => {
            let priming = raw
                .priming_frame_recorded_ns
                .ok_or(ResultError::InvalidArtifact)?;
            let origin = raw.workload_origin_ns.ok_or(ResultError::InvalidArtifact)?;
            if raw.frame_phase_offset_ns != Some(phase)
                || phase == 0
                || phase >= 100_000_000
                || raw.priming_frame_count != 1
                || raw.trial_origin_ns > observer_ready_ns
                || observer_ready_ns > priming
                || priming >= origin
                || origin
                    != priming
                        .checked_add(100_000_000 - phase)
                        .ok_or(ResultError::InvalidArtifact)?
            {
                return Err(ResultError::InvalidArtifact);
            }
        }
        None => {
            if raw.priming_frame_recorded_ns.is_some()
                || raw.workload_origin_ns.is_some()
                || raw.frame_phase_offset_ns.is_some()
                || raw.priming_frame_count != 0
            {
                return Err(ResultError::InvalidArtifact);
            }
        }
    }
    Ok(())
}

fn validate_sequences(raw: &HarnessTrialV1, spec: &ScenarioManifestV1) -> Result<(), ResultError> {
    for values in [
        &raw.submitted_sequences,
        &raw.admitted_sequences,
        &raw.completed_sequences,
        &raw.persisted_sequences,
        &raw.rendered_sequences,
    ] {
        if values.iter().copied().collect::<BTreeSet<_>>().len() != values.len() {
            return Err(ResultError::DuplicateOutcome);
        }
    }
    let expected = (1..=spec.admission_count).collect::<Vec<_>>();
    if raw.submitted_sequences != expected
        || raw.admitted_sequences != expected
        || raw.completed_sequences != expected
        || raw.persisted_sequences != expected
        || raw.rendered_sequences != screen_probe_sequences(scenario_profile(raw.scenario))
    {
        return Err(ResultError::SequenceCoverage);
    }
    Ok(())
}

fn validate_latency_and_aggregates(
    scenario: ScenarioV1,
    trial: &TrialResultV1,
    failures: &mut BTreeSet<FailureReasonV1>,
) -> Result<(), ResultError> {
    let raw = &trial.raw;
    let admissions = raw
        .admission_observations
        .iter()
        .map(|observation| (observation.sequence, observation))
        .collect::<std::collections::BTreeMap<_, _>>();
    if admissions.len() != raw.admission_observations.len() {
        return Err(ResultError::DuplicateOutcome);
    }
    let mut screen = Vec::new();
    let mut reducer = Vec::new();
    let mut publish = Vec::new();
    for observation in &raw.screen_observations {
        let admission = admissions
            .get(&observation.sequence)
            .ok_or(ResultError::InvalidArtifact)?;
        if admission.admitted_ns != observation.admitted_ns
            || observation.terminal_ns < observation.admitted_ns
            || observation.published_ns < observation.admitted_ns
            || observation.rendered_ns < observation.terminal_ns
            || observation.rendered_ns < observation.published_ns
            || observation.observed_frame_phase_ns
                != observation
                    .rendered_ns
                    .checked_sub(observation.admitted_ns)
                    .ok_or(ResultError::InvalidArtifact)?
                    % 100_000_000
        {
            return Err(ResultError::InvalidArtifact);
        }
        screen.push(observation.rendered_ns - observation.admitted_ns);
        reducer.push(observation.terminal_ns - observation.admitted_ns);
        publish.push(observation.rendered_ns - observation.published_ns);
    }
    let screen_distribution = distribution_from(screen);
    let reducer_distribution = distribution_from(reducer);
    let publish_distribution = distribution_from(publish);
    if trial.screen_update != screen_distribution
        || trial.reducer_lag != reducer_distribution
        || trial.publish_to_render != publish_distribution
    {
        return Err(ResultError::InvalidArtifact);
    }
    if matches!(scenario, ScenarioV1::Sustained | ScenarioV1::Burst)
        && trial.screen_update.as_ref().is_some_and(|value| {
            value.p95_ns >= workload_schema().thresholds.screen_update_p95_ns_exclusive
        })
    {
        failures.insert(FailureReasonV1::ScreenLatency);
    }
    let mut input = Vec::new();
    let mut prior_rendered: Option<u64> = None;
    for observation in &raw.input_observations {
        if observation.injected_ns < observation.scheduled_ns
            || observation.rendered_ns < observation.injected_ns
            || observation.observed_frame_phase_ns
                != (observation.rendered_ns - observation.injected_ns) % 100_000_000
        {
            return Err(ResultError::InvalidArtifact);
        }
        let expected_scheduled = if let Some(rendered) = prior_rendered {
            rendered
                .checked_add(
                    100_000_000
                        - raw
                            .frame_phase_offset_ns
                            .ok_or(ResultError::InvalidArtifact)?,
                )
                .ok_or(ResultError::InvalidArtifact)?
        } else {
            raw.workload_origin_ns.ok_or(ResultError::InvalidArtifact)?
        };
        if observation.scheduled_ns != expected_scheduled {
            return Err(ResultError::InvalidArtifact);
        }
        prior_rendered = Some(observation.rendered_ns);
        input.push(observation.rendered_ns - observation.injected_ns);
    }
    let input_distribution = distribution_from(input);
    if trial.input_response != input_distribution {
        return Err(ResultError::InvalidArtifact);
    }
    if trial.input_response.as_ref().is_some_and(|value| {
        value.p95_ns >= workload_schema().thresholds.input_response_p95_ns_exclusive
    }) {
        failures.insert(FailureReasonV1::InputLatency);
    }
    if !matches!(
        scenario,
        ScenarioV1::Sustained | ScenarioV1::Burst | ScenarioV1::TwiceTarget
    ) && (trial.reducer_lag.is_some() || trial.publish_to_render.is_some())
    {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn validate_scenario_matrix(
    document: &ReferenceRunV1,
    trial: &TrialResultV1,
    failures: &mut BTreeSet<FailureReasonV1>,
) -> Result<(), ResultError> {
    let raw = &trial.raw;
    match document.scenario {
        ScenarioV1::Target => {
            if raw.input_observations.len() != 200
                || !raw.admission_observations.is_empty()
                || !raw.screen_observations.is_empty()
                || !raw.startup_observations_ns.is_empty()
                || !raw.fallback_pairs.is_empty()
                || !raw.scoped_observations.is_empty()
                || raw.performance_evidence_stream.is_some()
            {
                return Err(ResultError::InvalidArtifact);
            }
        }
        ScenarioV1::Sustained | ScenarioV1::Burst | ScenarioV1::TwiceTarget => {
            let spec = scenario_spec(document.scenario);
            if raw.admission_observations.len() != spec.admission_count as usize
                || raw.screen_observations.len() != spec.screen_probe_count as usize
                || raw.scoped_observations.len() != spec.admission_count as usize
                || !raw.input_observations.is_empty()
                || !raw.startup_observations_ns.is_empty()
                || !raw.fallback_pairs.is_empty()
            {
                return Err(ResultError::InvalidArtifact);
            }
        }
        ScenarioV1::Startup => {
            if raw.startup_observations_ns.len() != 1 {
                return Err(ResultError::InvalidArtifact);
            }
            let startup = raw.startup_observations_ns[0];
            if trial.startup_ns != Some(startup)
                || raw.prepared_non_gap_event_count != Some(100_000)
                || raw.prepared_ledger_row_count != Some(100_000)
                || raw.restored_activity_count != Some(workload_schema().operator_activity_limit)
                || raw.scoped_observations.len() != 1
            {
                return Err(ResultError::InvalidArtifact);
            }
            if startup >= workload_schema().thresholds.startup_ns_exclusive {
                failures.insert(FailureReasonV1::StartupLatency);
            }
        }
        ScenarioV1::Idle => {
            if !raw.admission_observations.is_empty()
                || !raw.screen_observations.is_empty()
                || !raw.input_observations.is_empty()
                || !raw.startup_observations_ns.is_empty()
                || !raw.fallback_pairs.is_empty()
                || !raw.scoped_observations.is_empty()
                || trial.external_resource_audit.pidstat_sample_count == 0
            {
                return Err(ResultError::InvalidArtifact);
            }
        }
        ScenarioV1::FallbackRescan => {
            if raw.fallback_pairs.len() != 5 || raw.scoped_observations.len() != 10 {
                return Err(ResultError::InvalidArtifact);
            }
            let expected = target_identities_v1();
            let mut delays = Vec::new();
            for (index, pair) in raw.fallback_pairs.iter().enumerate() {
                if pair.sequence != index as u64 + 1 {
                    return Err(ResultError::InvalidArtifact);
                }
                let Some(delay) = pair.rescan_ns.checked_sub(pair.notification_ns) else {
                    return Err(ResultError::InvalidArtifact);
                };
                if pair.notification_final_identities != expected
                    || pair.rescan_final_identities != expected
                {
                    return Err(ResultError::StructuralMismatch);
                }
                if delay
                    > workload_schema()
                        .thresholds
                        .fallback_added_delay_ns_inclusive
                {
                    failures.insert(FailureReasonV1::FallbackRescanLatency);
                }
                delays.push(delay);
            }
            if trial.fallback_added_delay_ns != distribution_from(delays)
                && (!failures.contains(&FailureReasonV1::FallbackRescanLatency)
                    || document
                        .failure_reasons
                        .contains(&FailureReasonV1::FallbackRescanLatency))
            {
                return Err(ResultError::InvalidArtifact);
            }
        }
    }
    if document.scenario != ScenarioV1::Startup
        && (raw.prepared_non_gap_event_count.is_some()
            || raw.prepared_ledger_row_count.is_some()
            || raw.restored_activity_count.is_some()
            || trial.startup_ns.is_some())
    {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn validate_scoped_observations(
    scenario: ScenarioV1,
    trial: &TrialResultV1,
) -> Result<(), ResultError> {
    let mut keys = BTreeSet::new();
    for observation in &trial.raw.scoped_observations {
        if !keys.insert((observation.kind as u8, observation.sequence)) {
            return Err(ResultError::InvalidArtifact);
        }
        let expected_segments = match observation.kind {
            ScopedTimingKindV1::ControllerEvent => 2,
            ScopedTimingKindV1::StartupRestore
            | ScopedTimingKindV1::FallbackNotification
            | ScopedTimingKindV1::FallbackRescan => 1,
        };
        if observation.d4_segment_count != expected_segments
            || observation.model_clone_publish_segment_count != expected_segments
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    let actual = trial
        .raw
        .scoped_observations
        .iter()
        .map(|observation| (observation.kind, observation.sequence))
        .collect::<Vec<_>>();
    let expected = match scenario {
        ScenarioV1::Sustained | ScenarioV1::Burst | ScenarioV1::TwiceTarget => {
            (1..=scenario_spec(scenario).admission_count)
                .map(|sequence| (ScopedTimingKindV1::ControllerEvent, sequence))
                .collect()
        }
        ScenarioV1::Startup => vec![(ScopedTimingKindV1::StartupRestore, 1)],
        ScenarioV1::FallbackRescan => (1..=scenario_spec(scenario).fallback_pair_count)
            .flat_map(|sequence| {
                [
                    ScopedTimingKindV1::FallbackNotification,
                    ScopedTimingKindV1::FallbackRescan,
                ]
                .map(|kind| (kind, sequence))
            })
            .collect(),
        ScenarioV1::Target | ScenarioV1::Idle => Vec::new(),
    };
    if actual != expected {
        return Err(ResultError::InvalidArtifact);
    }
    let d4_sum = checked_optional_u64_sum(
        trial
            .raw
            .scoped_observations
            .iter()
            .map(|value| value.d4_analysis_ns),
    )?;
    let reducer_sum = checked_optional_u64_sum(
        trial
            .raw
            .scoped_observations
            .iter()
            .map(|value| value.reducer_plus_publish_ns),
    )?;
    if trial.d4_analysis_ns != d4_sum || trial.reducer_plus_publish_ns != reducer_sum {
        return Err(ResultError::InvalidArtifact);
    }
    let ratio = d4_sum
        .zip(reducer_sum)
        .map(|(numerator, denominator)| {
            numerator
                .checked_mul(1_000_000)
                .and_then(|value| value.checked_div(denominator))
                .ok_or(ResultError::InvalidArtifact)
        })
        .transpose()?;
    if trial.d4_ratio_parts_per_million != ratio {
        return Err(ResultError::InvalidArtifact);
    }
    if matches!(scenario, ScenarioV1::Target | ScenarioV1::Idle)
        && (!trial.raw.scoped_observations.is_empty()
            || trial.d4_analysis_ns.is_some()
            || trial.reducer_plus_publish_ns.is_some())
    {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn idle_identity_tick_totals(
    tree: &ProcessTreeEvidenceV1,
    start: u64,
    end: u64,
) -> Result<(u64, u64), ResultError> {
    let mut user_ticks = 0_u64;
    let mut system_ticks = 0_u64;
    for identity in &tree.process_identity_resources {
        let first_ns = tree
            .trial_origin_ns
            .checked_add(identity.first_observed_offset_ns)
            .ok_or(ResultError::InvalidArtifact)?;
        let start_ticks = match (
            identity.idle_window_start_user_cpu_ticks,
            identity.idle_window_start_system_cpu_ticks,
        ) {
            (Some(user), Some(system)) => Some((user, system)),
            (None, None) => None,
            _ => return Err(ResultError::InvalidArtifact),
        };
        let end_ticks = match (
            identity.idle_window_end_user_cpu_ticks,
            identity.idle_window_end_system_cpu_ticks,
        ) {
            (Some(user), Some(system)) => Some((user, system)),
            (None, None) => None,
            _ => return Err(ResultError::InvalidArtifact),
        };
        if first_ns > end || first_ns < start {
            if start_ticks.is_none() && end_ticks.is_none() {
                continue;
            }
            if first_ns > end {
                return Err(ResultError::InvalidArtifact);
            }
        }
        let (start_user, start_system) = start_ticks.ok_or(ResultError::InvalidArtifact)?;
        let (end_user, end_system) = end_ticks.ok_or(ResultError::InvalidArtifact)?;
        if first_ns > start && (start_user != 0 || start_system != 0) {
            return Err(ResultError::InvalidArtifact);
        }
        user_ticks = user_ticks
            .checked_add(
                end_user
                    .checked_sub(start_user)
                    .ok_or(ResultError::InvalidArtifact)?,
            )
            .ok_or(ResultError::InvalidArtifact)?;
        system_ticks = system_ticks
            .checked_add(
                end_system
                    .checked_sub(start_system)
                    .ok_or(ResultError::InvalidArtifact)?,
            )
            .ok_or(ResultError::InvalidArtifact)?;
    }
    Ok((user_ticks, system_ticks))
}

fn validate_resource_aggregates(
    scenario: ScenarioV1,
    trial: &TrialResultV1,
    declared_failures: &[FailureReasonV1],
    failures: &mut BTreeSet<FailureReasonV1>,
) -> Result<(), ResultError> {
    let raw_max = trial
        .process_tree
        .resource_observations
        .iter()
        .map(|observation| observation.process_tree_rss_bytes)
        .max()
        .ok_or(ResultError::InvalidArtifact)?;
    let threshold_missed = trial.maximum_process_tree_rss_bytes
        >= workload_schema()
            .thresholds
            .process_tree_rss_bytes_exclusive
        && scenario != ScenarioV1::Startup;
    if threshold_missed {
        failures.insert(FailureReasonV1::MaximumRss);
    }
    if trial.maximum_process_tree_rss_bytes != raw_max
        && (!threshold_missed || declared_failures.contains(&FailureReasonV1::MaximumRss))
    {
        return Err(ResultError::InvalidArtifact);
    }
    let diagnostic_sum = trial
        .process_tree
        .process_identity_resources
        .iter()
        .map(|identity| identity.maximum_vm_hwm_bytes)
        .try_fold(0_u64, u64::checked_add)
        .ok_or(ResultError::InvalidArtifact)?;
    if trial.sum_process_identity_peak_rss_bytes_diagnostic != diagnostic_sum {
        return Err(ResultError::InvalidArtifact);
    }
    if scenario == ScenarioV1::Idle {
        let start = trial
            .process_tree
            .idle_window_start_ns
            .ok_or(ResultError::InvalidArtifact)?;
        let end = trial
            .process_tree
            .idle_window_end_ns
            .ok_or(ResultError::InvalidArtifact)?;
        let elapsed = end.checked_sub(start).ok_or(ResultError::InvalidArtifact)?;
        let ticks_per_second = trial.process_tree.clock_ticks_per_second;
        let (user_ticks, system_ticks) =
            idle_identity_tick_totals(&trial.process_tree, start, end)?;
        let user_ns = (user_ticks as u128)
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_div(ticks_per_second as u128))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(ResultError::InvalidArtifact)?;
        let system_ns = (system_ticks as u128)
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_div(ticks_per_second as u128))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(ResultError::InvalidArtifact)?;
        if trial.elapsed_ns != elapsed
            || trial.user_cpu_ns != user_ns
            || trial.system_cpu_ns != system_ns
        {
            return Err(ResultError::InvalidArtifact);
        }
        let milli_percent = (user_ns as u128 + system_ns as u128)
            .checked_mul(100_000)
            .and_then(|value| value.checked_div(elapsed as u128))
            .ok_or(ResultError::InvalidArtifact)?;
        if milli_percent
            >= workload_schema()
                .thresholds
                .idle_cpu_milli_percent_exclusive as u128
        {
            failures.insert(FailureReasonV1::IdleCpu);
        }
    }
    Ok(())
}

fn checked_optional_u64_sum(values: impl Iterator<Item = u64>) -> Result<Option<u64>, ResultError> {
    let mut count = 0_usize;
    let sum = values
        .inspect(|_| count += 1)
        .try_fold(0_u64, u64::checked_add)
        .ok_or(ResultError::InvalidArtifact)?;
    Ok((count > 0).then_some(sum))
}

fn validate_performance_stream(
    document: &ReferenceRunV1,
    trial: &TrialResultV1,
    failures: &mut BTreeSet<FailureReasonV1>,
) -> Result<(), ResultError> {
    let required = document.measurement_stage == MeasurementStageV1::Final
        && matches!(
            document.scenario,
            ScenarioV1::Sustained | ScenarioV1::Burst | ScenarioV1::TwiceTarget
        );
    let Some(stream) = &trial.raw.performance_evidence_stream else {
        return if required {
            Err(ResultError::InvalidArtifact)
        } else {
            Ok(())
        };
    };
    if !required || stream.workload_start_ns != trial.raw.workload_origin_ns.unwrap_or(0) {
        return Err(ResultError::InvalidArtifact);
    }
    let last_frame = stream.frames.last().ok_or(ResultError::InvalidArtifact)?;
    if stream.samples.is_empty()
        || stream.workload_close_ns <= stream.workload_start_ns
        || last_frame.rendered_at_ns != stream.workload_close_ns
        || stream
            .workload_start_ns
            .checked_add(
                scenario_spec(document.scenario)
                    .admission_count
                    .checked_mul(scenario_spec(document.scenario).period_ns)
                    .ok_or(ResultError::InvalidArtifact)?,
            )
            .is_none_or(|schedule_end| stream.workload_close_ns <= schedule_end)
        || stream
            .samples
            .windows(2)
            .any(|window| window[0].sampled_at_ns > window[1].sampled_at_ns)
        || stream
            .frames
            .windows(2)
            .any(|window| window[0].rendered_at_ns > window[1].rendered_at_ns)
    {
        return Err(ResultError::InvalidArtifact);
    }
    validate_contiguous_ordinals(
        stream.first_sample_ordinal,
        stream.next_sample_ordinal,
        stream.samples.iter().map(|sample| sample.sample_ordinal),
    )?;
    validate_contiguous_ordinals(
        stream.first_draw_ordinal,
        stream.next_draw_ordinal,
        stream.frames.iter().map(|frame| frame.draw_ordinal),
    )?;
    let terminals = stream
        .terminal_observations
        .iter()
        .map(|terminal| (terminal.sequence, terminal.terminal_ns))
        .collect::<std::collections::BTreeMap<_, _>>();
    if terminals.len() != stream.terminal_observations.len()
        || terminals.len() != trial.raw.admission_observations.len()
        || stream
            .terminal_observations
            .iter()
            .enumerate()
            .any(|(index, terminal)| terminal.sequence != index as u64 + 1)
    {
        return Err(ResultError::InvalidArtifact);
    }
    for admission in &trial.raw.admission_observations {
        if terminals
            .get(&admission.sequence)
            .is_none_or(|terminal| *terminal < admission.admitted_ns)
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    if terminals
        .values()
        .any(|terminal| *terminal > stream.workload_close_ns)
    {
        return Err(ResultError::InvalidArtifact);
    }
    let sample_map = stream
        .samples
        .iter()
        .map(|sample| (sample.sample_ordinal, sample))
        .collect::<std::collections::BTreeMap<_, _>>();
    // The one-quantum boundary exception is trial-wide: any EventLag sample
    // closes the exception, even when the 101-event sample has no EventLag reason.
    let trial_has_event_lag_reason = stream
        .samples
        .iter()
        .any(|sample| sample.reasons.contains(&PerformanceReasonV1::EventLag));
    let mut degraded_samples = 0;
    let mut publication_ordinals = BTreeSet::new();
    let mut prior_sample = None;
    for (index, sample) in stream.samples.iter().enumerate() {
        if sample.sampled_at_ns > stream.workload_close_ns
            || index > 0 && sample.sampled_at_ns < stream.workload_start_ns
            || sample.source_quality != EffectiveQualityV1::Live
        {
            return Err(ResultError::InvalidArtifact);
        }
        let derived = derive_performance_state(
            sample.sampled_at_ns,
            &trial.raw.admission_observations,
            &terminals,
        )?;
        if sample.event_lag_ns != derived.event_lag_ns
            || sample.pending_events != derived.pending_events
            || sample.admission_high_water != derived.admission_high_water
            || sample.completion_high_water != derived.completion_high_water
            || sample.live_panes != 50
            || sample.default_visible_task_runs != 200
            || sample.dependency_edges != 1_000
            || sample.execution_edges != 199
            || sample.events_one_second != derived.events_one_second
            || sample.events_ten_seconds != derived.events_ten_seconds
            || sample.events_sixty_seconds != derived.events_sixty_seconds
        {
            return Err(ResultError::InvalidArtifact);
        }
        let instantaneous_reasons = expected_performance_reasons(sample);
        // EventLag is generation-latched until the breached admission generation drains. The
        // remaining reasons are instantaneous and must still match exactly in canonical order.
        let reasons_are_valid = sample.reasons == instantaneous_reasons
            || sample.event_lag_ns <= 1_000_000_000
                && sample.reasons.last() == Some(&PerformanceReasonV1::EventLag)
                && sample.reasons[..sample.reasons.len() - 1] == instantaneous_reasons;
        let effective = if sample.reasons.is_empty() {
            sample.source_quality
        } else {
            EffectiveQualityV1::Degraded
        };
        if !reasons_are_valid || sample.effective_quality != effective {
            return Err(ResultError::InvalidArtifact);
        }
        if prior_sample.is_none_or(|prior| performance_payload_changed(prior, sample)) {
            publication_ordinals.insert(sample.sample_ordinal);
        }
        prior_sample = Some(sample);
        degraded_samples += usize::from(
            !sample.reasons.is_empty()
                && !tolerated_boundary_degradation(
                    document.measurement_stage,
                    document.scenario,
                    sample,
                    trial_has_event_lag_reason,
                ),
        );
    }
    for frame in &stream.frames {
        let sample = sample_map
            .get(&frame.sample_ordinal)
            .ok_or(ResultError::InvalidArtifact)?;
        if frame.state_observed_at_ns != sample.sampled_at_ns
            || frame.rendered_at_ns < sample.sampled_at_ns
            || frame.effective_quality != sample.effective_quality
            || frame.reasons != sample.reasons
            || !publication_ordinals.contains(&frame.sample_ordinal)
        {
            return Err(ResultError::InvalidArtifact);
        }
        let expected = expected_header(frame.effective_quality, &frame.reasons);
        let header_is_valid = if document.scenario == ScenarioV1::TwiceTarget
            && frame
                .reasons
                .contains(&PerformanceReasonV1::EventsSixtySeconds)
        {
            frame.rendered_header_line == expected
                || frame.rendered_header_line
                    == expected_header_without_events_sixty(frame.effective_quality, &frame.reasons)
        } else {
            frame.rendered_header_line == expected
        };
        if !header_is_valid {
            return Err(ResultError::InvalidArtifact);
        }
    }
    match document.scenario {
        ScenarioV1::Sustained | ScenarioV1::Burst => {
            if degraded_samples > 0 {
                failures.insert(FailureReasonV1::SupportedLoadDegradation);
            }
        }
        ScenarioV1::TwiceTarget => {
            let attained = admission_schedule_attained(
                WorkloadProfile::TwiceTarget,
                stream.workload_start_ns,
                &trial.raw.admission_observations,
            )?;
            let deadline = stream
                .workload_start_ns
                .checked_add(60_000_000_000)
                .ok_or(ResultError::InvalidArtifact)?;
            let first_match = stream.frames.iter().find(|frame| {
                frame.rendered_at_ns <= deadline
                    && frame
                        .reasons
                        .contains(&PerformanceReasonV1::EventsSixtySeconds)
                    && frame.rendered_header_line
                        == expected_header(frame.effective_quality, &frame.reasons)
            });
            match (stream.selected_terminal_draw_ordinal, first_match) {
                (Some(selected), Some(frame))
                    if selected == frame.draw_ordinal && frame.rendered_at_ns <= deadline => {}
                (None, None) if !attained => {}
                (None, None) if stream.workload_close_ns > deadline => {
                    failures.insert(FailureReasonV1::MissingDegradation);
                }
                _ => return Err(ResultError::InvalidArtifact),
            }
        }
        _ => return Err(ResultError::InvalidArtifact),
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DerivedPerformanceState {
    event_lag_ns: u64,
    pending_events: u64,
    admission_high_water: u64,
    completion_high_water: u64,
    events_one_second: u64,
    events_ten_seconds: u64,
    events_sixty_seconds: u64,
}

fn derive_performance_state(
    sampled_at_ns: u64,
    admissions: &[AdmissionObservationV1],
    terminals: &std::collections::BTreeMap<u64, u64>,
) -> Result<DerivedPerformanceState, ResultError> {
    let admitted = admissions
        .iter()
        .filter(|observation| observation.admitted_ns <= sampled_at_ns)
        .collect::<Vec<_>>();
    let completed = admitted
        .iter()
        .filter(|observation| {
            terminals
                .get(&observation.sequence)
                .is_some_and(|terminal| *terminal <= sampled_at_ns)
        })
        .collect::<Vec<_>>();
    let pending = admitted
        .iter()
        .filter(|observation| {
            terminals
                .get(&observation.sequence)
                .is_none_or(|terminal| *terminal > sampled_at_ns)
        })
        .collect::<Vec<_>>();
    let window_count = |width: u64| -> Result<u64, ResultError> {
        let lower = sampled_at_ns.saturating_sub(width);
        u64::try_from(
            admitted
                .iter()
                .filter(|observation| observation.admitted_ns > lower)
                .count(),
        )
        .map_err(|_| ResultError::InvalidArtifact)
    };
    Ok(DerivedPerformanceState {
        event_lag_ns: pending
            .iter()
            .map(|observation| sampled_at_ns - observation.admitted_ns)
            .max()
            .unwrap_or(0),
        pending_events: u64::try_from(pending.len()).map_err(|_| ResultError::InvalidArtifact)?,
        admission_high_water: admitted
            .iter()
            .map(|observation| observation.sequence)
            .max()
            .unwrap_or(0),
        completion_high_water: completed
            .iter()
            .map(|observation| observation.sequence)
            .max()
            .unwrap_or(0),
        events_one_second: window_count(1_000_000_000)?,
        events_ten_seconds: window_count(10_000_000_000)?,
        events_sixty_seconds: window_count(60_000_000_000)?,
    })
}

fn performance_payload_changed(
    prior: &PerformanceSampleEvidenceV1,
    current: &PerformanceSampleEvidenceV1,
) -> bool {
    prior.event_lag_ns != current.event_lag_ns
        || prior.pending_events != current.pending_events
        || prior.admission_high_water != current.admission_high_water
        || prior.completion_high_water != current.completion_high_water
        || prior.live_panes != current.live_panes
        || prior.default_visible_task_runs != current.default_visible_task_runs
        || prior.dependency_edges != current.dependency_edges
        || prior.execution_edges != current.execution_edges
        || prior.events_one_second != current.events_one_second
        || prior.events_ten_seconds != current.events_ten_seconds
        || prior.events_sixty_seconds != current.events_sixty_seconds
        || prior.source_quality != current.source_quality
        || prior.effective_quality != current.effective_quality
        || prior.reasons != current.reasons
}

fn validate_contiguous_ordinals(
    first: u64,
    next: u64,
    values: impl Iterator<Item = u64>,
) -> Result<(), ResultError> {
    let actual = values.collect::<Vec<_>>();
    let expected = (first..next).collect::<Vec<_>>();
    if actual.is_empty() || actual != expected {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn expected_performance_reasons(sample: &PerformanceSampleEvidenceV1) -> Vec<PerformanceReasonV1> {
    let mut reasons = Vec::new();
    if sample.live_panes > 50 {
        reasons.push(PerformanceReasonV1::LivePanes);
    }
    if sample.default_visible_task_runs > 200 {
        reasons.push(PerformanceReasonV1::DefaultVisibleTaskRuns);
    }
    if sample.dependency_edges > 1_000 {
        reasons.push(PerformanceReasonV1::DependencyEdges);
    }
    if sample.events_one_second > 100 {
        reasons.push(PerformanceReasonV1::EventsOneSecond);
    }
    if sample.events_ten_seconds > 1_000 {
        reasons.push(PerformanceReasonV1::EventsTenSeconds);
    }
    if sample.events_sixty_seconds > 1_200 {
        reasons.push(PerformanceReasonV1::EventsSixtySeconds);
    }
    if sample.event_lag_ns > 1_000_000_000 {
        reasons.push(PerformanceReasonV1::EventLag);
    }
    reasons
}

pub fn tolerated_boundary_degradation(
    stage: MeasurementStageV1,
    scenario: ScenarioV1,
    sample: &PerformanceSampleEvidenceV1,
    trial_has_event_lag_reason: bool,
) -> bool {
    stage == MeasurementStageV1::Final
        && matches!(scenario, ScenarioV1::Sustained | ScenarioV1::Burst)
        && sample.reasons == [PerformanceReasonV1::EventsOneSecond]
        && sample.events_one_second == 101
        && sample.event_lag_ns <= 1_000_000_000
        && !trial_has_event_lag_reason
}

fn expected_header(quality: EffectiveQualityV1, reasons: &[PerformanceReasonV1]) -> String {
    let quality = match quality {
        EffectiveQualityV1::Live => "LIVE",
        EffectiveQualityV1::Reconciling => "RECONCILING",
        EffectiveQualityV1::Disconnected => "DISCONNECTED",
        EffectiveQualityV1::Degraded => "DEGRADED",
    };
    let labels = reasons
        .iter()
        .filter_map(|reason| {
            workload_schema()
                .performance_reason_labels
                .iter()
                .find(|row| row.reason == *reason)
                .map(|row| row.label.as_str())
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{quality} | perf:{labels}")
}

fn expected_header_without_events_sixty(
    quality: EffectiveQualityV1,
    reasons: &[PerformanceReasonV1],
) -> String {
    expected_header(
        quality,
        &reasons
            .iter()
            .copied()
            .filter(|reason| *reason != PerformanceReasonV1::EventsSixtySeconds)
            .collect::<Vec<_>>(),
    )
}

fn raw_artifact_digests_are_well_formed(value: &RawArtifactDigestsV1) -> bool {
    [
        &value.harness_json_sha256,
        &value.runner_control_json_sha256,
        &value.process_tree_json_sha256,
        &value.observer_handshake_sha256,
        &value.observer_control_json_sha256,
        &value.gnu_time_sha256,
        &value.pidstat_json_sha256,
        &value.pidstat_stderr_sha256,
        &value.child_stdout_sha256,
        &value.child_stderr_sha256,
        &value.observer_stdout_sha256,
        &value.observer_stderr_sha256,
        &value.trial_status_sha256,
    ]
    .into_iter()
    .all(|digest| is_lower_hex(digest, 64))
}

fn failure_policy_row(
    stage: MeasurementStageV1,
    scenario: ScenarioV1,
    reason: FailureReasonV1,
) -> Option<&'static FailurePolicyRowV1> {
    workload_schema().failure_policy.iter().find(|row| {
        row.stages.contains(&stage)
            && row.scenarios.contains(&scenario)
            && row.failure_reason == reason
    })
}

fn strictly_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|window| window[0] < window[1])
}

fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn absolute_path_text_is_canonical(path: &std::path::Path) -> bool {
    let normalized = path.components().collect::<std::path::PathBuf>();
    path.is_absolute()
        && !path
            .components()
            .any(|component| component == std::path::Component::ParentDir)
        && path.as_os_str() == normalized.as_os_str()
}

fn validate_section15_internal(report: &Section15ReDerivationV1) -> Result<(), ResultError> {
    let scenarios = [
        ScenarioV1::Target,
        ScenarioV1::Sustained,
        ScenarioV1::Burst,
        ScenarioV1::Startup,
        ScenarioV1::Idle,
        ScenarioV1::FallbackRescan,
        ScenarioV1::TwiceTarget,
    ];
    if report.schema_version != 1
        || !is_lower_hex(&report.subject_sha, 40)
        || report.selected_results.len() != 14
        || report.scenarios.len() != 7
        || report
            .scenarios
            .iter()
            .map(|row| row.scenario)
            .ne(scenarios)
    {
        return Err(ResultError::InvalidArtifact);
    }
    let mut result_paths = BTreeSet::new();
    let mut raw_roots = BTreeSet::new();
    let mut stage_harnesses = BTreeMap::new();
    let mut stage_binaries = BTreeMap::new();
    for (index, identity) in report.selected_results.iter().enumerate() {
        let expected_scenario = scenarios[index / 2];
        let expected_stage = if index % 2 == 0 {
            MeasurementStageV1::Baseline
        } else {
            MeasurementStageV1::Final
        };
        let result_path = std::path::Path::new(&identity.canonical_result_path);
        let raw_root = std::path::Path::new(&identity.canonical_raw_root);
        if identity.scenario != expected_scenario
            || identity.measurement_stage != expected_stage
            || identity.baseline_id != report.baseline_id
            || identity.workload_schema_sha256 != WORKLOAD_SCHEMA_V1_SHA256
            || !is_lower_hex(&identity.result_sha256, 64)
            || !is_lower_hex(&identity.production_subject_sha, 40)
            || !is_lower_hex(&identity.harness_sha, 40)
            || !absolute_path_text_is_canonical(result_path)
            || !absolute_path_text_is_canonical(raw_root)
            || result_path.parent() != Some(raw_root)
            || result_path.file_name() != Some(std::ffi::OsStr::new("result-v1.json"))
            || !executable_identity_is_well_formed(&identity.measured_binary)
            || expected_stage == MeasurementStageV1::Baseline
                && (identity.production_subject_sha != BASELINE_SUBJECT_SHA
                    || !baseline_id_is_valid(
                        &identity.baseline_id,
                        MeasurementStageV1::Baseline,
                        &identity.harness_sha,
                    ))
            || expected_stage == MeasurementStageV1::Final
                && identity.production_subject_sha != report.subject_sha
            || !result_paths.insert(&identity.canonical_result_path)
            || !raw_roots.insert(&identity.canonical_raw_root)
        {
            return Err(ResultError::InvalidArtifact);
        }
        let stage_key = expected_stage as u8;
        if stage_harnesses
            .get(&stage_key)
            .is_some_and(|value| value != &identity.harness_sha)
            || stage_binaries
                .get(&stage_key)
                .is_some_and(|value| value != &identity.measured_binary)
        {
            return Err(ResultError::InvalidArtifact);
        }
        stage_harnesses
            .entry(stage_key)
            .or_insert_with(|| identity.harness_sha.clone());
        stage_binaries
            .entry(stage_key)
            .or_insert_with(|| identity.measured_binary.clone());
    }
    for scenario in &report.scenarios {
        let spec = scenario_spec(scenario.scenario);
        let mut derived_failures = BTreeSet::new();
        if scenario.baseline_status == ReferenceOutcomeStatusV1::Invalid
            || scenario.final_status == ReferenceOutcomeStatusV1::Invalid
            || !strictly_sorted_unique(&scenario.final_failure_reasons)
            || (scenario.final_failure_reasons.is_empty()
                != (scenario.final_status == ReferenceOutcomeStatusV1::Pass))
        {
            return Err(ResultError::InvalidArtifact);
        }
        if scenario.trials.len() != scenario_spec(scenario.scenario).recorded_trials {
            return Err(ResultError::InvalidArtifact);
        }
        let mut any_admission_bucket_missed = false;
        for (index, trial) in scenario.trials.iter().enumerate() {
            if trial.trial_index != index as u64 + 1
                || !trial.lossless
                || !trial.structural_identities_match
                || trial.sequence_counts
                    != (Section15SequenceCountsV1 {
                        submitted: spec.admission_count,
                        admitted: spec.admission_count,
                        completed: spec.admission_count,
                        persisted: spec.admission_count,
                        rendered_probes: spec.screen_probe_count,
                    })
                || if spec.admission_count == 0 {
                    trial.admission_buckets_attained.is_some()
                } else {
                    trial.admission_buckets_attained.is_none()
                }
                || trial
                    .distributions
                    .iter()
                    .map(|row| row.metric)
                    .ne(expected_section15_distribution_metrics(scenario.scenario))
            {
                return Err(ResultError::InvalidArtifact);
            }
            any_admission_bucket_missed |= trial.admission_buckets_attained == Some(false);
            if !canonical_decimal(&trial.sequence_counts.submitted.to_string()) {
                return Err(ResultError::InvalidArtifact);
            }
            for distribution in &trial.distributions {
                let manifest_row = section15_manifest_rows(scenario.scenario)?
                    .distribution_rows
                    .iter()
                    .find(|row| row.metric == distribution.metric)
                    .ok_or(ResultError::InvalidArtifact)?;
                let values = [
                    &distribution.minimum,
                    &distribution.median,
                    &distribution.p95,
                    &distribution.p99,
                    &distribution.maximum,
                ];
                let parsed = values
                    .iter()
                    .map(|value| value.parse::<u128>())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| ResultError::InvalidArtifact)?;
                if distribution.unit != manifest_row.unit
                    || distribution.sample_count
                        != expected_section15_distribution_sample_count(
                            scenario.scenario,
                            distribution.metric,
                        )?
                    || parsed.windows(2).any(|window| window[0] > window[1])
                {
                    return Err(ResultError::InvalidArtifact);
                }
                for value in [
                    &distribution.minimum,
                    &distribution.median,
                    &distribution.p95,
                    &distribution.p99,
                    &distribution.maximum,
                ] {
                    if !canonical_decimal(value) {
                        return Err(ResultError::InvalidArtifact);
                    }
                }
            }
            for predicate in &trial.predicates {
                if !canonical_decimal(&predicate.observed_numerator)
                    || !canonical_decimal(&predicate.threshold_numerator)
                    || predicate
                        .observed_denominator
                        .as_deref()
                        .is_some_and(|value| !canonical_decimal(value))
                    || predicate
                        .threshold_denominator
                        .as_deref()
                        .is_some_and(|value| !canonical_decimal(value))
                {
                    return Err(ResultError::InvalidArtifact);
                }
            }
            validate_section15_predicate_matrix(scenario.scenario, trial)?;
            for predicate in trial
                .predicates
                .iter()
                .filter(|predicate| !predicate.passed)
            {
                let reason = match predicate.metric {
                    Section15MetricV1::InputResponse => FailureReasonV1::InputLatency,
                    Section15MetricV1::ScreenUpdate => FailureReasonV1::ScreenLatency,
                    Section15MetricV1::Startup => FailureReasonV1::StartupLatency,
                    Section15MetricV1::IdleCpu => FailureReasonV1::IdleCpu,
                    Section15MetricV1::MaximumProcessTreeRss => FailureReasonV1::MaximumRss,
                    Section15MetricV1::FallbackAddedDelay => FailureReasonV1::FallbackRescanLatency,
                    Section15MetricV1::AdmissionDeadline => FailureReasonV1::WorkloadAdmission,
                    Section15MetricV1::PerformanceDegradation
                        if scenario.scenario == ScenarioV1::TwiceTarget =>
                    {
                        if any_admission_bucket_missed {
                            continue;
                        }
                        FailureReasonV1::MissingDegradation
                    }
                    Section15MetricV1::PerformanceDegradation => {
                        FailureReasonV1::SupportedLoadDegradation
                    }
                    Section15MetricV1::SubmittedSequences
                    | Section15MetricV1::AdmittedSequences
                    | Section15MetricV1::CompletedSequences
                    | Section15MetricV1::PersistedSequences
                    | Section15MetricV1::RenderedProbeSequences
                    | Section15MetricV1::ReducerLag
                    | Section15MetricV1::PublishToRender
                    | Section15MetricV1::D4Analysis
                    | Section15MetricV1::ReducerPlusPublish => {
                        return Err(ResultError::InvalidArtifact);
                    }
                };
                derived_failures.insert(reason);
            }
        }
        if any_admission_bucket_missed
            != scenario
                .final_failure_reasons
                .contains(&FailureReasonV1::WorkloadAdmission)
        {
            return Err(ResultError::InvalidArtifact);
        }
        if scenario.final_failure_reasons
            != derived_failures
                .into_iter()
                .collect::<Vec<FailureReasonV1>>()
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    let mut expected_deltas = Vec::new();
    for scenario in &report.scenarios {
        for trial in &scenario.trials {
            for distribution in &trial.distributions {
                let manifest_row = section15_manifest_rows(scenario.scenario)?
                    .distribution_rows
                    .iter()
                    .find(|row| row.metric == distribution.metric)
                    .ok_or(ResultError::InvalidArtifact)?;
                for statistic in manifest_row.statistics.iter().copied() {
                    expected_deltas.push((
                        scenario.scenario,
                        trial.trial_index,
                        distribution.metric,
                        statistic,
                        distribution.unit,
                    ));
                }
            }
        }
    }
    for (delta, expected) in report.baseline_deltas.iter().zip(&expected_deltas) {
        if !canonical_decimal(&delta.baseline_value)
            || !canonical_decimal(&delta.final_value)
            || !canonical_signed_decimal(&delta.signed_delta)
            || (
                delta.scenario,
                delta.trial_index,
                delta.metric,
                delta.statistic,
                delta.unit,
            ) != *expected
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    if report.baseline_deltas.len() != expected_deltas.len() {
        return Err(ResultError::InvalidArtifact);
    }
    for delta in &report.baseline_deltas {
        let distribution = report
            .scenarios
            .iter()
            .find(|scenario| scenario.scenario == delta.scenario)
            .and_then(|scenario| {
                scenario
                    .trials
                    .iter()
                    .find(|trial| trial.trial_index == delta.trial_index)
            })
            .and_then(|trial| {
                trial
                    .distributions
                    .iter()
                    .find(|row| row.metric == delta.metric && row.unit == delta.unit)
            })
            .ok_or(ResultError::InvalidArtifact)?;
        let final_value = section15_distribution_statistic(distribution, delta.statistic);
        if &delta.final_value != final_value
            || delta.signed_delta
                != signed_decimal_delta(&delta.baseline_value, &delta.final_value)?
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    let expected_failures = report
        .scenarios
        .iter()
        .flat_map(|scenario| {
            scenario
                .final_failure_reasons
                .iter()
                .map(move |reason| (scenario.scenario, *reason))
        })
        .collect::<Vec<_>>();
    if report.failure_policy_evidence.len() != expected_failures.len() {
        return Err(ResultError::InvalidArtifact);
    }
    for (row, (scenario, reason)) in report.failure_policy_evidence.iter().zip(expected_failures) {
        if row.measurement_stage != MeasurementStageV1::Final
            || row.scenario != scenario
            || row.failure_reason != reason
            || lookup_failure_policy(MeasurementStageV1::Final, scenario, reason)
                != Some(row.policy)
            || row
                .d4_analysis_sum
                .as_deref()
                .is_some_and(|value| !canonical_decimal(value))
            || row
                .reducer_plus_publish_sum
                .as_deref()
                .is_some_and(|value| !canonical_decimal(value))
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    let decision = classify_failure_policy_evidence(&report.failure_policy_evidence)?;
    if report.decision != decision {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn validate_section15_selected_evidence(
    report: &Section15ReDerivationV1,
    legacy: AmendedLegacyMode,
) -> Result<(), ResultError> {
    validate_section15_internal(report)?;

    let baseline_root = std::path::Path::new(&report.selected_results[0].canonical_raw_root)
        .parent()
        .ok_or(ResultError::InvalidArtifact)?
        .to_path_buf();
    let final_root = std::path::Path::new(&report.selected_results[1].canonical_raw_root)
        .parent()
        .ok_or(ResultError::InvalidArtifact)?
        .to_path_buf();
    let canonical_baseline_root = baseline_root
        .canonicalize()
        .map_err(|_| ResultError::InvalidArtifact)?;
    let canonical_final_root = final_root
        .canonicalize()
        .map_err(|_| ResultError::InvalidArtifact)?;
    if baseline_root != canonical_baseline_root
        || final_root != canonical_final_root
        || canonical_baseline_root == canonical_final_root
        || canonical_baseline_root.starts_with(&canonical_final_root)
        || canonical_final_root.starts_with(&canonical_baseline_root)
    {
        return Err(ResultError::InvalidArtifact);
    }

    let mut baseline = Vec::with_capacity(workload_schema().scenarios.len());
    let mut final_results = Vec::with_capacity(workload_schema().scenarios.len());
    for (index, identity) in report.selected_results.iter().enumerate() {
        let spec = &workload_schema().scenarios[index / 2];
        let selected_root = if identity.measurement_stage == MeasurementStageV1::Baseline {
            &canonical_baseline_root
        } else {
            &canonical_final_root
        };
        let expected_raw_root = selected_root.join(&spec.directory);
        let raw_root = std::path::Path::new(&identity.canonical_raw_root);
        let result_path = std::path::Path::new(&identity.canonical_result_path);
        let canonical_raw_root = raw_root
            .canonicalize()
            .map_err(|_| ResultError::InvalidArtifact)?;
        let canonical_result_path = result_path
            .canonicalize()
            .map_err(|_| ResultError::InvalidArtifact)?;
        if raw_root != canonical_raw_root
            || result_path != canonical_result_path
            || canonical_raw_root != expected_raw_root
            || canonical_result_path != canonical_raw_root.join("result-v1.json")
            || sha256_path(result_path).map_err(|_| ResultError::InvalidArtifact)?
                != identity.result_sha256
        {
            return Err(ResultError::InvalidArtifact);
        }

        let outcome = read_and_validate_reference_outcome(result_path, legacy)
            .map_err(|_| ResultError::InvalidArtifact)?
            .outcome;
        if outcome.status() == ReferenceOutcomeStatusV1::Invalid
            || validate_with_raw_root(&outcome, raw_root).is_err()
        {
            return Err(ResultError::InvalidArtifact);
        }
        let document = outcome.document();
        if document.measurement_stage != identity.measurement_stage
            || document.scenario != identity.scenario
            || document.production_subject_sha != identity.production_subject_sha
            || document.harness_sha != identity.harness_sha
            || document.workload_schema_sha256 != identity.workload_schema_sha256
            || document.baseline_id != identity.baseline_id
            || document.controls.measured_binary != identity.measured_binary
        {
            return Err(ResultError::InvalidArtifact);
        }
        if identity.measurement_stage == MeasurementStageV1::Baseline {
            baseline.push(outcome);
        } else {
            final_results.push(outcome);
        }
    }

    let fresh = rederive_section15_document(
        &canonical_baseline_root,
        &canonical_final_root,
        &baseline,
        &final_results,
    )
    .map_err(|_| ResultError::InvalidArtifact)?;
    validate_section15_internal(&fresh)?;
    if fresh != *report {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(())
}

fn section15_manifest_rows(
    scenario: ScenarioV1,
) -> Result<&'static Section15ScenarioRowManifestV1, ResultError> {
    workload_schema()
        .section15_row_matrix
        .iter()
        .find(|row| row.scenario == scenario)
        .ok_or(ResultError::InvalidArtifact)
}

fn expected_section15_distribution_sample_count(
    scenario: ScenarioV1,
    metric: Section15MetricV1,
) -> Result<u64, ResultError> {
    let spec = scenario_spec(scenario);
    let policy = section15_manifest_rows(scenario)?
        .distribution_rows
        .iter()
        .find(|row| row.metric == metric)
        .ok_or(ResultError::InvalidArtifact)?
        .sample_count_policy;
    match policy {
        Section15DistributionSampleCountPolicyV1::InputObservations => Ok(spec.input_count),
        Section15DistributionSampleCountPolicyV1::ScreenProbeObservations => {
            Ok(spec.screen_probe_count)
        }
        Section15DistributionSampleCountPolicyV1::SingleStartupObservation => Ok(1),
        Section15DistributionSampleCountPolicyV1::FallbackPairs => Ok(spec.fallback_pair_count),
        Section15DistributionSampleCountPolicyV1::ScopedObservations => match scenario {
            ScenarioV1::Sustained | ScenarioV1::Burst | ScenarioV1::TwiceTarget => {
                Ok(spec.admission_count)
            }
            ScenarioV1::Startup => Ok(1),
            ScenarioV1::FallbackRescan => Ok(spec.fallback_pair_count * 2),
            ScenarioV1::Target | ScenarioV1::Idle => Err(ResultError::InvalidArtifact),
        },
    }
}

fn expanded_section15_predicate_manifest_rows(
    scenario: ScenarioV1,
) -> Result<Vec<(&'static Section15PredicateRowManifestV1, Option<u64>)>, ResultError> {
    let scenario_spec = scenario_spec(scenario);
    let bucket_count = scenario_spec
        .admission_count
        .checked_mul(scenario_spec.period_ns)
        .and_then(|duration| duration.checked_div(1_000_000_000))
        .ok_or(ResultError::InvalidArtifact)?;
    let mut expanded = Vec::new();
    for row in &section15_manifest_rows(scenario)?.predicate_rows {
        match (row.repetition, row.ordinal_policy) {
            (Section15PredicateRepetitionV1::Once, Section15OrdinalPolicyV1::None) => {
                expanded.push((row, None));
            }
            (
                Section15PredicateRepetitionV1::AdmissionBuckets,
                Section15OrdinalPolicyV1::ZeroBased,
            ) => {
                expanded.extend((0..bucket_count).map(|ordinal| (row, Some(ordinal))));
            }
            (
                Section15PredicateRepetitionV1::FallbackPairs,
                Section15OrdinalPolicyV1::OneBasedSequence,
            ) => {
                expanded.extend(
                    (1..=scenario_spec.fallback_pair_count).map(|ordinal| (row, Some(ordinal))),
                );
            }
            _ => return Err(ResultError::InvalidArtifact),
        }
    }
    Ok(expanded)
}

fn validate_section15_predicate_matrix(
    scenario: ScenarioV1,
    trial: &Section15TrialReDerivationV1,
) -> Result<(), ResultError> {
    let spec = scenario_spec(scenario);
    let expanded = expanded_section15_predicate_manifest_rows(scenario)?;
    if trial.predicates.len() != expanded.len() {
        return Err(ResultError::InvalidArtifact);
    }
    let thresholds = &workload_schema().thresholds;
    let distribution = |metric| {
        trial
            .distributions
            .iter()
            .find(|distribution| distribution.metric == metric)
            .ok_or(ResultError::InvalidArtifact)
    };
    let mut previous_admission_threshold = None;
    for (predicate, (manifest_row, ordinal)) in trial.predicates.iter().zip(expanded) {
        let observed = predicate
            .observed_numerator
            .parse::<u128>()
            .map_err(|_| ResultError::InvalidArtifact)?;
        let observed_denominator = predicate
            .observed_denominator
            .as_deref()
            .unwrap_or("1")
            .parse::<u128>()
            .map_err(|_| ResultError::InvalidArtifact)?;
        let threshold = predicate
            .threshold_numerator
            .parse::<u128>()
            .map_err(|_| ResultError::InvalidArtifact)?;
        let threshold_denominator = predicate
            .threshold_denominator
            .as_deref()
            .unwrap_or("1")
            .parse::<u128>()
            .map_err(|_| ResultError::InvalidArtifact)?;
        if observed_denominator == 0 || threshold_denominator == 0 {
            return Err(ResultError::InvalidArtifact);
        }
        let left = observed
            .checked_mul(threshold_denominator)
            .ok_or(ResultError::InvalidArtifact)?;
        let right = threshold
            .checked_mul(observed_denominator)
            .ok_or(ResultError::InvalidArtifact)?;
        let passed = match predicate.comparison {
            ThresholdComparisonV1::LessThan => left < right,
            ThresholdComparisonV1::LessThanOrEqual => left <= right,
            ThresholdComparisonV1::Equal => left == right,
        };
        if predicate.passed != passed {
            return Err(ResultError::InvalidArtifact);
        }
        if predicate.metric != manifest_row.metric
            || predicate.unit != manifest_row.unit
            || predicate.ordinal != ordinal
            || predicate.threshold_denominator.is_some()
        {
            return Err(ResultError::InvalidArtifact);
        }
        let (comparison, threshold_value, expected_observed, denominator_required) =
            match predicate.metric {
                Section15MetricV1::InputResponse => (
                    ThresholdComparisonV1::LessThan,
                    thresholds.input_response_p95_ns_exclusive as u128,
                    Some(distribution(Section15MetricV1::InputResponse)?.p95.clone()),
                    false,
                ),
                Section15MetricV1::ScreenUpdate => (
                    ThresholdComparisonV1::LessThan,
                    thresholds.screen_update_p95_ns_exclusive as u128,
                    Some(distribution(Section15MetricV1::ScreenUpdate)?.p95.clone()),
                    false,
                ),
                Section15MetricV1::Startup => (
                    ThresholdComparisonV1::LessThan,
                    thresholds.startup_ns_exclusive as u128,
                    Some(distribution(Section15MetricV1::Startup)?.maximum.clone()),
                    false,
                ),
                Section15MetricV1::IdleCpu => (
                    ThresholdComparisonV1::LessThan,
                    thresholds.idle_cpu_milli_percent_exclusive as u128,
                    None,
                    true,
                ),
                Section15MetricV1::FallbackAddedDelay => (
                    ThresholdComparisonV1::LessThanOrEqual,
                    thresholds.fallback_added_delay_ns_inclusive as u128,
                    None,
                    false,
                ),
                Section15MetricV1::AdmissionDeadline => {
                    if let Some(previous) = previous_admission_threshold
                        && threshold.checked_sub(previous) != Some(1_000_000_000)
                    {
                        return Err(ResultError::InvalidArtifact);
                    }
                    previous_admission_threshold = Some(threshold);
                    (
                        ThresholdComparisonV1::LessThanOrEqual,
                        threshold,
                        None,
                        false,
                    )
                }
                Section15MetricV1::SubmittedSequences => (
                    ThresholdComparisonV1::Equal,
                    spec.admission_count as u128,
                    Some(trial.sequence_counts.submitted.to_string()),
                    false,
                ),
                Section15MetricV1::AdmittedSequences => (
                    ThresholdComparisonV1::Equal,
                    spec.admission_count as u128,
                    Some(trial.sequence_counts.admitted.to_string()),
                    false,
                ),
                Section15MetricV1::CompletedSequences => (
                    ThresholdComparisonV1::Equal,
                    spec.admission_count as u128,
                    Some(trial.sequence_counts.completed.to_string()),
                    false,
                ),
                Section15MetricV1::PersistedSequences => (
                    ThresholdComparisonV1::Equal,
                    spec.admission_count as u128,
                    Some(trial.sequence_counts.persisted.to_string()),
                    false,
                ),
                Section15MetricV1::RenderedProbeSequences => (
                    ThresholdComparisonV1::Equal,
                    spec.screen_probe_count as u128,
                    Some(trial.sequence_counts.rendered_probes.to_string()),
                    false,
                ),
                Section15MetricV1::MaximumProcessTreeRss => (
                    ThresholdComparisonV1::LessThan,
                    thresholds.process_tree_rss_bytes_exclusive as u128,
                    None,
                    false,
                ),
                Section15MetricV1::PerformanceDegradation => (
                    ThresholdComparisonV1::Equal,
                    u128::from(scenario == ScenarioV1::TwiceTarget),
                    None,
                    false,
                ),
                Section15MetricV1::ReducerLag
                | Section15MetricV1::PublishToRender
                | Section15MetricV1::D4Analysis
                | Section15MetricV1::ReducerPlusPublish => {
                    return Err(ResultError::InvalidArtifact);
                }
            };
        if predicate.comparison != comparison
            || predicate.threshold_numerator != threshold_value.to_string()
            || predicate.observed_denominator.is_some() != denominator_required
            || expected_observed
                .as_ref()
                .is_some_and(|expected| &predicate.observed_numerator != expected)
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    Ok(())
}

fn expected_section15_distribution_metrics(
    scenario: ScenarioV1,
) -> impl Iterator<Item = Section15MetricV1> {
    section15_manifest_rows(scenario)
        .expect("all closed scenarios have Section 15 manifest rows")
        .distribution_rows
        .iter()
        .map(|row| row.metric)
}

fn signed_decimal_delta(baseline: &str, final_value: &str) -> Result<String, ResultError> {
    let baseline = baseline
        .parse::<u128>()
        .map_err(|_| ResultError::InvalidArtifact)?;
    let final_value = final_value
        .parse::<u128>()
        .map_err(|_| ResultError::InvalidArtifact)?;
    Ok(match final_value.cmp(&baseline) {
        std::cmp::Ordering::Less => format!("-{}", baseline - final_value),
        std::cmp::Ordering::Equal => "0".to_owned(),
        std::cmp::Ordering::Greater => (final_value - baseline).to_string(),
    })
}

fn canonical_decimal(value: &str) -> bool {
    value.parse::<u128>().is_ok()
        && (value == "0"
            || (!value.starts_with('0') && value.bytes().all(|byte| byte.is_ascii_digit())))
}

fn canonical_signed_decimal(value: &str) -> bool {
    if value == "0" {
        return true;
    }
    value.strip_prefix('-').map_or_else(
        || canonical_decimal(value),
        |magnitude| canonical_decimal(magnitude) && magnitude != "0",
    )
}

fn classify_failure_policy_evidence(
    rows: &[Section15FailurePolicyEvidenceV1],
) -> Result<D4CheckpointDecisionV1, ResultError> {
    let mut amendments = BTreeSet::new();
    for row in rows {
        match row.policy {
            D4PolicyV1::D4Scoped => {
                let numerator = row
                    .d4_analysis_sum
                    .as_deref()
                    .ok_or(ResultError::InvalidArtifact)?
                    .parse::<u128>()
                    .map_err(|_| ResultError::InvalidArtifact)?;
                let denominator = row
                    .reducer_plus_publish_sum
                    .as_deref()
                    .ok_or(ResultError::InvalidArtifact)?
                    .parse::<u128>()
                    .map_err(|_| ResultError::InvalidArtifact)?;
                if denominator == 0 {
                    return Err(ResultError::InvalidArtifact);
                }
                let high = numerator
                    .checked_mul(4)
                    .ok_or(ResultError::InvalidArtifact)?
                    >= denominator;
                if row.d4_exact_quarter_predicate != Some(high) {
                    return Err(ResultError::InvalidArtifact);
                }
                let amendment = if high {
                    RequiredAmendmentV1::D4
                } else {
                    RequiredAmendmentV1::NonD4
                };
                if row.required_amendment != Some(amendment) {
                    return Err(ResultError::InvalidArtifact);
                }
                amendments.insert(amendment);
            }
            D4PolicyV1::NonD4 => {
                if row.d4_analysis_sum.is_some()
                    || row.reducer_plus_publish_sum.is_some()
                    || row.d4_exact_quarter_predicate.is_some()
                    || row.required_amendment != Some(RequiredAmendmentV1::NonD4)
                {
                    return Err(ResultError::InvalidArtifact);
                }
                amendments.insert(RequiredAmendmentV1::NonD4);
            }
            D4PolicyV1::NotApplicable => return Err(ResultError::InvalidArtifact),
        }
    }
    Ok(if amendments.is_empty() {
        D4CheckpointDecisionV1::NoMissD4NotAuthorized {}
    } else {
        D4CheckpointDecisionV1::AmendmentsRequired {
            amendments: amendments.into_iter().collect(),
        }
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PercentileRuleV1 {
    NearestRank,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioFieldV1 {
    Admission,
    Screen,
    Input,
    Startup,
    Fallback,
    Scoped,
    Sequences,
    PerformanceStreamFinalOnly,
    IdleWindow,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioManifestV1 {
    pub scenario: ScenarioV1,
    pub cli_token: String,
    pub directory: String,
    pub warm_up_trials: usize,
    pub recorded_trials: usize,
    pub admission_count: u64,
    pub period_ns: u64,
    pub screen_probe_stride: u64,
    pub screen_probe_count: u64,
    pub input_count: u64,
    pub startup_retained_events: u64,
    pub fallback_pair_count: u64,
    pub frame_phase_offsets_ns: Vec<u64>,
    pub warm_up_frame_phase_offset_ns: Option<u64>,
    pub applicable_fields: Vec<ScenarioFieldV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceReasonLabelV1 {
    pub reason: PerformanceReasonV1,
    pub label: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FailurePolicyRowV1 {
    pub stages: Vec<MeasurementStageV1>,
    pub scenarios: Vec<ScenarioV1>,
    pub failure_reason: FailureReasonV1,
    pub outcome: ReferenceOutcomeStatusV1,
    pub d4_policy: D4PolicyV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Section15DistributionSampleCountPolicyV1 {
    InputObservations,
    ScreenProbeObservations,
    SingleStartupObservation,
    FallbackPairs,
    ScopedObservations,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Section15PredicateRepetitionV1 {
    Once,
    AdmissionBuckets,
    FallbackPairs,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Section15OrdinalPolicyV1 {
    None,
    ZeroBased,
    OneBasedSequence,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15DistributionRowManifestV1 {
    pub metric: Section15MetricV1,
    pub unit: Section15UnitV1,
    pub statistics: Vec<DistributionStatisticV1>,
    pub sample_count_policy: Section15DistributionSampleCountPolicyV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15PredicateRowManifestV1 {
    pub metric: Section15MetricV1,
    pub unit: Section15UnitV1,
    pub repetition: Section15PredicateRepetitionV1,
    pub ordinal_policy: Section15OrdinalPolicyV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15ScenarioRowManifestV1 {
    pub scenario: ScenarioV1,
    pub distribution_rows: Vec<Section15DistributionRowManifestV1>,
    pub predicate_rows: Vec<Section15PredicateRowManifestV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadManifestV1 {
    pub schema_version: u32,
    pub scenarios: Vec<ScenarioManifestV1>,
    pub thresholds: ThresholdsV1,
    pub percentile_rule: PercentileRuleV1,
    pub operator_activity_limit: u64,
    pub admission_deadline_grace_periods: u64,
    pub screen_probe_interval_ns: u64,
    pub reducer_lag_derivation: String,
    pub publish_to_render_derivation: String,
    pub fallback_arm_policy: String,
    pub baseline_transfer_policy: String,
    pub idle_resource_reduction_policy: String,
    pub measurement_stage_applicability: String,
    pub performance_evidence_stream_policy: String,
    pub performance_reason_labels: Vec<PerformanceReasonLabelV1>,
    pub section15_row_matrix: Vec<Section15ScenarioRowManifestV1>,
    pub render_surface: RenderSurfaceV1,
    pub failure_policy: Vec<FailurePolicyRowV1>,
}

pub const WORKLOAD_SCHEMA_V1_SHA256: &str =
    "1adbe866face1b46908c143882fcc3a032f85709f8fa1eefac45d9f7a8a96dc3";

pub fn workload_schema() -> &'static WorkloadManifestV1 {
    static MANIFEST: std::sync::OnceLock<WorkloadManifestV1> = std::sync::OnceLock::new();
    MANIFEST.get_or_init(|| {
        serde_json::from_slice(include_bytes!("../fixtures/workload-schema-v1.json"))
            .expect("the checked-in workload manifest must match its closed schema")
    })
}

pub fn manifest() -> &'static WorkloadManifestV1 {
    workload_schema()
}

pub fn workload_schema_sha256() -> String {
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(include_bytes!("../fixtures/workload-schema-v1.json"));
    format!("{digest:x}")
}

pub fn canonical_workload_schema_bytes_are_byte_stable() -> bool {
    let source = include_bytes!("../fixtures/workload-schema-v1.json");
    if source.is_empty()
        || source.last() != Some(&b'\n')
        || source[..source.len() - 1].contains(&b'\n')
    {
        return false;
    }
    serde_json::to_vec(workload_schema())
        .map(|mut encoded| {
            encoded.push(b'\n');
            encoded == source
        })
        .unwrap_or(false)
}

pub fn lookup_failure_policy(
    stage: MeasurementStageV1,
    scenario: ScenarioV1,
    reason: FailureReasonV1,
) -> Option<D4PolicyV1> {
    workload_schema()
        .failure_policy
        .iter()
        .find(|row| {
            row.stages.contains(&stage)
                && row.scenarios.contains(&scenario)
                && row.failure_reason == reason
        })
        .map(|row| row.d4_policy)
}

pub fn expanded_failure_policy_tuples() -> Vec<(MeasurementStageV1, ScenarioV1, FailureReasonV1)> {
    workload_schema()
        .failure_policy
        .iter()
        .flat_map(|row| {
            row.stages.iter().copied().flat_map(move |stage| {
                row.scenarios
                    .iter()
                    .copied()
                    .map(move |scenario| (stage, scenario, row.failure_reason))
            })
        })
        .collect()
}

pub fn admission_schedule_attained(
    profile: WorkloadProfile,
    workload_origin_ns: u64,
    observations: &[AdmissionObservationV1],
) -> Result<bool, ResultError> {
    let cadence_ns =
        u64::try_from(period(profile).as_nanos()).map_err(|_| ResultError::InvalidArtifact)?;
    if cadence_ns == 0 || observations.len() != admission_offsets(profile).len() {
        return Err(ResultError::InvalidArtifact);
    }
    for (index, observation) in observations.iter().enumerate() {
        let sequence = u64::try_from(index + 1).map_err(|_| ResultError::InvalidArtifact)?;
        let scheduled_ns = workload_origin_ns
            .checked_add(
                sequence
                    .checked_mul(cadence_ns)
                    .ok_or(ResultError::InvalidArtifact)?,
            )
            .ok_or(ResultError::InvalidArtifact)?;
        if observation.sequence != sequence
            || observation.scheduled_ns != scheduled_ns
            || observation.admitted_ns < scheduled_ns
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    let per_second = 1_000_000_000 / cadence_ns;
    for (bucket, chunk) in observations.chunks(per_second as usize).enumerate() {
        let bucket_end = workload_origin_ns
            .checked_add(
                (u64::try_from(bucket).map_err(|_| ResultError::InvalidArtifact)? + 1)
                    .checked_mul(1_000_000_000)
                    .ok_or(ResultError::InvalidArtifact)?,
            )
            .ok_or(ResultError::InvalidArtifact)?;
        let deadline = bucket_end
            .checked_add(cadence_ns)
            .ok_or(ResultError::InvalidArtifact)?;
        if chunk
            .iter()
            .any(|observation| observation.admitted_ns > deadline)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub const BASELINE_SUBJECT_SHA: &str = "9cd98131038a53b6dd36ff53e9b89825acba70ae";
pub const SYNTHETIC_HARNESS_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

pub fn valid_synthetic_result() -> ReferenceOutcomeV1 {
    synthetic_result(ScenarioV1::Sustained, MeasurementStageV1::Baseline)
}

pub fn valid_target_input_result() -> ReferenceOutcomeV1 {
    synthetic_result(ScenarioV1::Target, MeasurementStageV1::Baseline)
}

pub fn valid_startup_result() -> ReferenceOutcomeV1 {
    synthetic_result(ScenarioV1::Startup, MeasurementStageV1::Baseline)
}

pub fn valid_idle_result() -> ReferenceOutcomeV1 {
    synthetic_result(ScenarioV1::Idle, MeasurementStageV1::Baseline)
}

pub fn valid_fallback_result() -> ReferenceOutcomeV1 {
    synthetic_result(ScenarioV1::FallbackRescan, MeasurementStageV1::Baseline)
}

pub fn valid_final_sustained_result() -> ReferenceOutcomeV1 {
    synthetic_result(ScenarioV1::Sustained, MeasurementStageV1::Final)
}

pub fn valid_final_burst_result() -> ReferenceOutcomeV1 {
    synthetic_result(ScenarioV1::Burst, MeasurementStageV1::Final)
}

pub fn valid_twice_target_result() -> ReferenceOutcomeV1 {
    synthetic_result(ScenarioV1::TwiceTarget, MeasurementStageV1::Final)
}

pub fn synthetic_result(
    scenario: ScenarioV1,
    measurement_stage: MeasurementStageV1,
) -> ReferenceOutcomeV1 {
    let spec = scenario_spec(scenario);
    let production_subject_sha = if measurement_stage == MeasurementStageV1::Baseline {
        BASELINE_SUBJECT_SHA
    } else {
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    };
    let trials = (1..=spec.recorded_trials)
        .map(|trial_index| synthetic_trial(scenario, measurement_stage, trial_index))
        .collect();
    ReferenceOutcomeV1::Pass {
        document: ReferenceRunV1 {
            schema_version: 1,
            measurement_stage,
            scenario,
            production_subject_sha: production_subject_sha.to_owned(),
            harness_sha: SYNTHETIC_HARNESS_SHA.to_owned(),
            workload_schema_sha256: WORKLOAD_SCHEMA_V1_SHA256.to_owned(),
            baseline_id: format!(
                "sha256:v1:{BASELINE_SUBJECT_SHA}:{SYNTHETIC_HARNESS_SHA}:{WORKLOAD_SCHEMA_V1_SHA256}"
            ),
            tracked_clean: true,
            build_profile: "release".to_owned(),
            command: vec!["workload_harness".to_owned(), spec.cli_token.clone()],
            controlled_environment: run_environment(
                measurement_stage,
                scenario,
                production_subject_sha,
            ),
            render_surface: workload_schema().render_surface.clone(),
            host: synthetic_host(),
            controls: synthetic_run_controls(),
            thresholds: workload_schema().thresholds.clone(),
            warm_up_trials: spec.warm_up_trials,
            recorded_trials: spec.recorded_trials,
            trials,
            failure_reasons: Vec::new(),
        },
    }
}

fn scenario_spec(scenario: ScenarioV1) -> &'static ScenarioManifestV1 {
    workload_schema()
        .scenarios
        .iter()
        .find(|spec| spec.scenario == scenario)
        .expect("every closed scenario must have one manifest row")
}

fn synthetic_trial(
    scenario: ScenarioV1,
    measurement_stage: MeasurementStageV1,
    trial_index: usize,
) -> TrialResultV1 {
    let spec = scenario_spec(scenario);
    let production_subject_sha = if measurement_stage == MeasurementStageV1::Baseline {
        BASELINE_SUBJECT_SHA
    } else {
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    };
    let phase = spec
        .frame_phase_offsets_ns
        .get(trial_index.saturating_sub(1))
        .copied();
    let trial_origin_ns = 1_000_000_000_000_u64
        .checked_mul(trial_index as u64)
        .expect("synthetic trial origin must fit");
    let observer_ready_ns = trial_origin_ns + 1_000_000;
    let priming_frame_recorded_ns = phase.map(|_| observer_ready_ns + 1_000_000);
    let workload_origin_ns = priming_frame_recorded_ns
        .zip(phase)
        .map(|(priming, offset)| priming + (100_000_000 - offset));
    let identities = target_identities_v1();
    let profile = scenario_profile(scenario);
    let mut admission_observations = Vec::new();
    if spec.admission_count > 0 {
        let origin = workload_origin_ns.expect("scheduled profile must have an origin");
        admission_observations = (1..=spec.admission_count)
            .map(|sequence| {
                let scheduled_ns = origin + sequence * spec.period_ns;
                AdmissionObservationV1 {
                    sequence,
                    scheduled_ns,
                    admitted_ns: scheduled_ns,
                }
            })
            .collect();
    }
    let probe_sequences = screen_probe_sequences(profile);
    let screen_observations = probe_sequences
        .iter()
        .map(|sequence| {
            let admitted_ns = admission_observations[*sequence as usize - 1].admitted_ns;
            LatencyObservationV1 {
                sequence: *sequence,
                admitted_ns,
                terminal_ns: admitted_ns + 10_000_000,
                published_ns: admitted_ns + 20_000_000,
                rendered_ns: admitted_ns + 30_000_000,
                observed_frame_phase_ns: 30_000_000,
            }
        })
        .collect::<Vec<_>>();
    let input_observations: Vec<InputLatencyObservationV1> = workload_origin_ns
        .filter(|_| scenario == ScenarioV1::Target)
        .map(|origin| {
            let complement = 100_000_000 - phase.expect("target phase must exist");
            (0..spec.input_count)
                .scan(origin, |scheduled_ns, _| {
                    let injected_ns = *scheduled_ns;
                    let rendered_ns = injected_ns + 20_000_000;
                    *scheduled_ns = rendered_ns + complement;
                    Some(InputLatencyObservationV1 {
                        scheduled_ns: injected_ns,
                        injected_ns,
                        rendered_ns,
                        observed_frame_phase_ns: 20_000_000,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let startup_observations_ns = (scenario == ScenarioV1::Startup)
        .then_some(vec![1_000_000_000])
        .unwrap_or_default();
    let fallback_pairs = if scenario == ScenarioV1::FallbackRescan {
        (1..=spec.fallback_pair_count)
            .map(|sequence| FallbackPairObservationV1 {
                sequence,
                notification_ns: trial_origin_ns + sequence * 10_000_000,
                rescan_ns: trial_origin_ns + sequence * 10_000_000 + 100_000_000,
                notification_final_identities: identities.clone(),
                rescan_final_identities: identities.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };
    let scoped_observations = match scenario {
        ScenarioV1::Sustained | ScenarioV1::Burst | ScenarioV1::TwiceTarget => (1..=spec
            .admission_count)
            .map(|sequence| ScopedTimingObservationV1 {
                kind: ScopedTimingKindV1::ControllerEvent,
                sequence,
                d4_segment_count: 2,
                d4_analysis_ns: 100,
                reducer_plus_publish_ns: 1_000,
                model_clone_publish_segment_count: 2,
                model_clone_publish_ns: 100,
                render_ns: 100,
            })
            .collect(),
        ScenarioV1::Startup => vec![ScopedTimingObservationV1 {
            kind: ScopedTimingKindV1::StartupRestore,
            sequence: 1,
            d4_segment_count: 1,
            d4_analysis_ns: 100,
            reducer_plus_publish_ns: 1_000,
            model_clone_publish_segment_count: 1,
            model_clone_publish_ns: 100,
            render_ns: 100,
        }],
        ScenarioV1::FallbackRescan => (1..=spec.fallback_pair_count)
            .flat_map(|sequence| {
                [
                    ScopedTimingKindV1::FallbackNotification,
                    ScopedTimingKindV1::FallbackRescan,
                ]
                .map(|kind| ScopedTimingObservationV1 {
                    kind,
                    sequence,
                    d4_segment_count: 1,
                    d4_analysis_ns: 100,
                    reducer_plus_publish_ns: 1_000,
                    model_clone_publish_segment_count: 1,
                    model_clone_publish_ns: 100,
                    render_ns: 100,
                })
            })
            .collect(),
        ScenarioV1::Target | ScenarioV1::Idle => Vec::new(),
    };
    let sequence_vector = (1..=spec.admission_count).collect::<Vec<_>>();
    let raw_root = format!(
        "/tmp/herdr-increment5/{}/trial-{trial_index:04}",
        spec.directory
    );
    let scratch_root = format!("{raw_root}/scratch");
    let control_socket = format!(
        "/tmp/herdr-i5.synthetic/{}-trial-{trial_index:04}.sock",
        spec.directory
    );
    let idle_start = (scenario == ScenarioV1::Idle).then_some(observer_ready_ns + 5_000_000_000);
    let idle_end = idle_start.map(|start| start + 30_000_000_000);
    let observer_control = synthetic_observer_control(
        scenario,
        trial_origin_ns,
        observer_ready_ns,
        idle_start,
        idle_end,
    );
    let process_tree = synthetic_process_tree(
        scenario,
        trial_origin_ns,
        observer_ready_ns,
        idle_start,
        idle_end,
    );
    let sum_process_identity_peak_rss_bytes_diagnostic = process_tree
        .process_identity_resources
        .iter()
        .map(|identity| identity.maximum_vm_hwm_bytes)
        .sum();
    let screen_update = distribution_from(
        screen_observations
            .iter()
            .map(|observation| observation.rendered_ns - observation.admitted_ns)
            .collect(),
    );
    let reducer_lag = distribution_from(
        screen_observations
            .iter()
            .map(|observation| observation.terminal_ns - observation.admitted_ns)
            .collect(),
    );
    let publish_to_render = distribution_from(
        screen_observations
            .iter()
            .map(|observation| observation.rendered_ns - observation.published_ns)
            .collect(),
    );
    let input_response = distribution_from(
        input_observations
            .iter()
            .map(|observation| observation.rendered_ns - observation.injected_ns)
            .collect(),
    );
    let fallback_added_delay_ns = distribution_from(
        fallback_pairs
            .iter()
            .map(|pair| pair.rescan_ns - pair.notification_ns)
            .collect(),
    );
    let d4_analysis_ns = (!scoped_observations.is_empty()).then(|| {
        scoped_observations
            .iter()
            .map(|value| value.d4_analysis_ns)
            .sum()
    });
    let reducer_plus_publish_ns = (!scoped_observations.is_empty()).then(|| {
        scoped_observations
            .iter()
            .map(|value| value.reducer_plus_publish_ns)
            .sum()
    });
    let performance_evidence_stream = matches!(
        (measurement_stage, scenario),
        (
            MeasurementStageV1::Final,
            ScenarioV1::Sustained | ScenarioV1::Burst | ScenarioV1::TwiceTarget
        )
    )
    .then(|| {
        synthetic_performance_stream(
            scenario,
            workload_origin_ns.expect("final stream scenario must have origin"),
            &admission_observations,
        )
    });
    let raw = HarnessTrialV1 {
        scenario,
        trial_index,
        trial_origin_ns,
        priming_frame_recorded_ns,
        workload_origin_ns,
        frame_phase_offset_ns: phase,
        priming_frame_count: u32::from(phase.is_some()),
        admission_observations,
        screen_observations,
        input_observations,
        startup_observations_ns,
        fallback_pairs,
        scoped_observations,
        submitted_sequences: sequence_vector.clone(),
        admitted_sequences: sequence_vector.clone(),
        completed_sequences: sequence_vector.clone(),
        persisted_sequences: sequence_vector,
        rendered_sequences: probe_sequences,
        pane_ids: identities.pane_ids.clone(),
        task_run_ids: identities.task_run_ids.clone(),
        dependency_edges: identities.dependency_edges.clone(),
        execution_edges: identities.execution_edges.clone(),
        prepared_non_gap_event_count: (scenario == ScenarioV1::Startup).then_some(100_000),
        prepared_ledger_row_count: (scenario == ScenarioV1::Startup).then_some(100_000),
        restored_activity_count: (scenario == ScenarioV1::Startup).then_some(10_000),
        performance_evidence_stream,
        idle_window_start_ns: idle_start,
        idle_window_end_ns: idle_end,
        child_controls: ChildControlsV1 {
            effective_affinity_cpu_ids: vec![0, 1, 2, 3],
            effective_address_space_limit_bytes: 16 * 1024 * 1024 * 1024,
            measured_environment: measured_environment(
                &raw_root,
                &scratch_root,
                &control_socket,
                measurement_stage,
                scenario,
                production_subject_sha,
                (measurement_stage != MeasurementStageV1::Baseline)
                    .then(|| std::path::Path::new("/tmp/herdr-increment5/baseline")),
            ),
            scratch_root: scratch_root.clone(),
        },
    };
    let control_evidence = synthetic_trial_control(
        &raw_root,
        &scratch_root,
        &control_socket,
        scenario,
        &process_tree,
    );
    TrialResultV1 {
        trial_index,
        raw,
        observer_control,
        process_tree,
        raw_artifacts: synthetic_digests(),
        control_evidence,
        screen_update,
        reducer_lag,
        publish_to_render,
        input_response,
        startup_ns: (scenario == ScenarioV1::Startup).then_some(1_000_000_000),
        elapsed_ns: if scenario == ScenarioV1::Idle {
            30_000_000_000
        } else {
            1_000_000_000
        },
        user_cpu_ns: if scenario == ScenarioV1::Idle {
            100_000_000
        } else {
            10_000_000
        },
        system_cpu_ns: 10_000_000,
        maximum_process_tree_rss_bytes: 10_000_000,
        sum_process_identity_peak_rss_bytes_diagnostic,
        fallback_added_delay_ns,
        d4_analysis_ns,
        reducer_plus_publish_ns,
        d4_ratio_parts_per_million: d4_analysis_ns
            .zip(reducer_plus_publish_ns)
            .map(|(numerator, denominator)| numerator.saturating_mul(1_000_000) / denominator),
        external_resource_audit: ExternalResourceAuditV1 {
            gnu_elapsed_ns: 1_000_000_000,
            gnu_user_cpu_ns: 10_000_000,
            gnu_system_cpu_ns: 10_000_000,
            gnu_maximum_rss_bytes: 9_999_360,
            gnu_exit_status: 0,
            pidstat_child_user_cpu_ns: (scenario == ScenarioV1::Idle).then_some(100_000_000),
            pidstat_child_system_cpu_ns: (scenario == ScenarioV1::Idle).then_some(10_000_000),
            pidstat_wrapper_maximum_rss_bytes: (scenario == ScenarioV1::Idle).then_some(9_999_360),
            pidstat_sample_count: usize::from(scenario == ScenarioV1::Idle),
        },
    }
}

fn scenario_profile(scenario: ScenarioV1) -> WorkloadProfile {
    match scenario {
        ScenarioV1::Target => WorkloadProfile::TargetTopology,
        ScenarioV1::Sustained => WorkloadProfile::SustainedTarget,
        ScenarioV1::Burst => WorkloadProfile::TargetBurst,
        ScenarioV1::Startup => WorkloadProfile::Startup,
        ScenarioV1::Idle => WorkloadProfile::Idle,
        ScenarioV1::FallbackRescan => WorkloadProfile::FallbackRescan,
        ScenarioV1::TwiceTarget => WorkloadProfile::TwiceTarget,
    }
}

pub fn target_identities_v1() -> StructuralIdentitiesV1 {
    let identities = oracle(WorkloadProfile::TargetTopology).final_identities;
    StructuralIdentitiesV1 {
        pane_ids: identities.pane_ids.into_iter().collect(),
        task_run_ids: identities.task_run_ids.into_iter().collect(),
        dependency_edges: identities.dependency_edges.into_iter().collect(),
        execution_edges: identities.execution_edges.into_iter().collect(),
    }
}

fn distribution_from(mut values: Vec<u64>) -> Option<DistributionV1> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    Some(DistributionV1 {
        sample_count: values.len(),
        minimum_ns: values[0],
        median_ns: percentile(&values, 50).expect("nonempty distribution"),
        p95_ns: percentile(&values, 95).expect("nonempty distribution"),
        p99_ns: percentile(&values, 99).expect("nonempty distribution"),
        maximum_ns: *values.last().expect("nonempty distribution"),
    })
}

fn synthetic_host() -> HostProfileV1 {
    HostProfileV1 {
        operating_system: "linux".to_owned(),
        kernel: "synthetic".to_owned(),
        architecture: "x86_64".to_owned(),
        cpu_model: "synthetic".to_owned(),
        physical_core_ids: vec![
            "0".to_owned(),
            "1".to_owned(),
            "2".to_owned(),
            "3".to_owned(),
        ],
        memory_total_bytes: 32 * 1024 * 1024 * 1024,
        storage_kind: "nvme".to_owned(),
        storage_device: "nvme0n1".to_owned(),
        governor: Some("performance".to_owned()),
        boost: Some("enabled".to_owned()),
        ambient_load_milli: [0, 0, 0],
    }
}

fn synthetic_executable(path: &str, byte: char) -> ExecutableIdentityV1 {
    ExecutableIdentityV1 {
        requested_path: path.to_owned(),
        canonical_path: path.to_owned(),
        sha256: byte.to_string().repeat(64),
    }
}

fn authoritative_executables() -> Vec<ExecutableIdentityV1> {
    let mut executables = vec![(developer_home_path(".cargo/bin/rustup"), '1')];
    executables.extend(
        [
            ("/usr/bin/awk", '2'),
            ("/usr/bin/bash", '2'),
            ("/usr/bin/env", '3'),
            ("/usr/bin/findmnt", '3'),
            ("/usr/bin/git", '4'),
            ("/usr/bin/id", '4'),
            ("/usr/bin/jq", '5'),
            ("/usr/bin/lsblk", '5'),
            ("/usr/bin/lscpu", '5'),
            ("/usr/bin/mkdir", '5'),
            ("/usr/bin/mktemp", '5'),
            ("/usr/bin/mv", '5'),
            ("/usr/bin/pidstat", '5'),
            ("/usr/bin/prlimit", '5'),
            ("/usr/bin/readlink", '6'),
            ("/usr/bin/rg", '6'),
            ("/usr/bin/rmdir", '6'),
            ("/usr/bin/setsid", '6'),
            ("/usr/bin/sha256sum", '6'),
            ("/usr/bin/sleep", '6'),
            ("/usr/bin/stat", '6'),
            ("/usr/bin/taskset", '6'),
            ("/usr/bin/time", '7'),
            ("/usr/bin/uname", '8'),
            ("/usr/bin/unlink", '8'),
        ]
        .into_iter()
        .map(|(path, byte)| (path.to_owned(), byte)),
    );
    executables
        .into_iter()
        .map(|(path, byte)| synthetic_executable(&path, byte))
        .collect()
}

fn synthetic_run_controls() -> RunControlsV1 {
    let authoritative_executables = authoritative_executables();
    RunControlsV1 {
        affinity_cpu_ids: vec![0, 1, 2, 3],
        address_space_limit_bytes: 16 * 1024 * 1024 * 1024,
        true_cgroup_memory_limit: false,
        toolchain_launcher: authoritative_executables[0].clone(),
        toolchain_name: "1.97.1".to_owned(),
        rustc_version: "rustc 1.97.1 (synthetic)".to_owned(),
        cargo_version: "cargo 1.97.1 (synthetic)".to_owned(),
        build_environment: invariant_environment(),
        cargo_configuration: CargoConfigurationPolicyV1 {
            policy_version: 1,
            invocation_cwd: "/src/herdr-top".to_owned(),
            ordered_absent_candidates: vec![
                "/src/herdr-top/.cargo/config".to_owned(),
                "/src/herdr-top/.cargo/config.toml".to_owned(),
                "/src/.cargo/config".to_owned(),
                "/src/.cargo/config.toml".to_owned(),
                "/.cargo/config".to_owned(),
                "/.cargo/config.toml".to_owned(),
                developer_home_path(".cargo/config"),
                developer_home_path(".cargo/config.toml"),
            ],
        },
        measured_binary: synthetic_executable(
            "/src/herdr-top/target/release/workload_harness",
            '9',
        ),
        runner_script: synthetic_executable("/src/herdr-top/tests/workload-runner-v1.sh", 'a'),
        authoritative_executables,
        pidstat_child_status_mode: PidstatChildStatusModeV1::PropagatesChildStatus,
        limitation: "address-space cap is not a true cgroup memory limit".to_owned(),
    }
}

fn invariant_environment() -> std::collections::BTreeMap<String, String> {
    BTreeMap::from([
        ("CARGO_HOME".to_owned(), developer_home_path(".cargo")),
        (
            "HOME".to_owned(),
            developer_home()
                .into_os_string()
                .into_string()
                .expect("HOME must be valid UTF-8 for the reference workload"),
        ),
        ("LC_ALL".to_owned(), "C".to_owned()),
        ("PATH".to_owned(), "/usr/bin:/bin".to_owned()),
        ("RUSTUP_HOME".to_owned(), developer_home_path(".rustup")),
        ("TZ".to_owned(), "UTC".to_owned()),
    ])
}

fn run_environment(
    measurement_stage: MeasurementStageV1,
    scenario: ScenarioV1,
    production_subject_sha: &str,
) -> std::collections::BTreeMap<String, String> {
    let mut values = invariant_environment();
    values.insert(
        "HERDR_PERF_SCENARIO".to_owned(),
        scenario_spec(scenario).cli_token.clone(),
    );
    values.insert(
        "HERDR_PERF_STAGE".to_owned(),
        stage_cli_token(measurement_stage).to_owned(),
    );
    values.insert(
        "HERDR_PERF_SUBJECT".to_owned(),
        production_subject_sha.to_owned(),
    );
    if measurement_stage != MeasurementStageV1::Baseline {
        values.insert(
            "HERDR_PERF_BASELINE_RESULTS_ROOT".to_owned(),
            "/tmp/herdr-increment5/baseline".to_owned(),
        );
    }
    values
}

pub(crate) fn measured_environment(
    raw_root: &str,
    scratch_root: &str,
    control_socket: &str,
    measurement_stage: MeasurementStageV1,
    scenario: ScenarioV1,
    production_subject_sha: &str,
    baseline_root: Option<&std::path::Path>,
) -> std::collections::BTreeMap<String, String> {
    let mut values = invariant_environment();
    values.insert(
        "HERDR_PERF_SCENARIO".to_owned(),
        scenario_spec(scenario).cli_token.clone(),
    );
    values.insert(
        "HERDR_PERF_STAGE".to_owned(),
        stage_cli_token(measurement_stage).to_owned(),
    );
    values.insert(
        "HERDR_PERF_SUBJECT".to_owned(),
        production_subject_sha.to_owned(),
    );
    values.insert(
        "HERDR_PERF_OUTPUT".to_owned(),
        format!("{raw_root}/harness.json"),
    );
    values.insert(
        "HERDR_PERF_OBSERVER_HANDSHAKE".to_owned(),
        format!("{raw_root}/observer-handshake"),
    );
    values.insert(
        "HERDR_PERF_OBSERVER_CONTROL_SOCKET".to_owned(),
        control_socket.to_owned(),
    );
    values.insert(
        "HERDR_PERF_SCRATCH_ROOT".to_owned(),
        scratch_root.to_owned(),
    );
    if let Some(root) = baseline_root {
        values.insert(
            "HERDR_PERF_BASELINE_RESULTS_ROOT".to_owned(),
            root.to_string_lossy().into_owned(),
        );
    }
    values
}

fn synthetic_trial_control(
    raw_root: &str,
    scratch_root: &str,
    control_socket: &str,
    scenario: ScenarioV1,
    process_tree: &ProcessTreeEvidenceV1,
) -> TrialControlEvidenceV1 {
    let controls = synthetic_run_controls();
    TrialControlEvidenceV1 {
        scratch_root: scratch_root.to_owned(),
        scratch_storage_kind: "nvme".to_owned(),
        scratch_storage_devices: vec!["nvme0n1".to_owned()],
        orchestrator_environment: invariant_environment(),
        observer_environment: observer_environment(
            raw_root,
            control_socket,
            scenario,
            process_tree,
        ),
        validator_environment_template: invariant_environment(),
        revalidated_executables: controls.authoritative_executables.clone(),
        revalidated_runner_script: controls.runner_script.clone(),
        revalidated_measured_binary: controls.measured_binary.clone(),
        trial_status: TrialStatusV1::Ok,
        pidstat_exit_status: 0,
    }
}

fn synthetic_observer_control(
    scenario: ScenarioV1,
    trial_origin_ns: u64,
    observer_ready_ns: u64,
    idle_start: Option<u64>,
    idle_end: Option<u64>,
) -> ObserverControlEvidenceV1 {
    let (commands, frames) = if let (Some(start_ns), Some(end_ns)) = (idle_start, idle_end) {
        (
            vec![ObserverCommandV1::StartIdleWindow {}],
            vec![
                ObserverControlFrameV1::Ready { observer_ready_ns },
                ObserverControlFrameV1::IdleWindowStarted {
                    request_received_ns: start_ns,
                    start_ns,
                },
                ObserverControlFrameV1::IdleWindowEnded { end_ns },
            ],
        )
    } else {
        (
            Vec::new(),
            vec![ObserverControlFrameV1::Ready { observer_ready_ns }],
        )
    };
    ObserverControlEvidenceV1 {
        protocol_version: 1,
        scenario,
        observed_root_pid: 10_001,
        observed_root_start_time_ticks: 55,
        trial_origin_ns,
        observer_ready_ns,
        idle_window_start_ns: idle_start,
        idle_window_end_ns: idle_end,
        commands,
        frames,
    }
}

fn synthetic_process_tree(
    scenario: ScenarioV1,
    trial_origin_ns: u64,
    observer_ready_ns: u64,
    idle_start: Option<u64>,
    idle_end: Option<u64>,
) -> ProcessTreeEvidenceV1 {
    let mut identities = vec![ProcessIdentityResourceV1 {
        pid: 10_001,
        start_time_ticks: 55,
        first_observed_offset_ns: 100,
        idle_window_start_user_cpu_ticks: idle_start.map(|_| 20),
        idle_window_start_system_cpu_ticks: idle_start.map(|_| 10),
        idle_window_end_user_cpu_ticks: idle_end.map(|_| 30),
        idle_window_end_system_cpu_ticks: idle_end.map(|_| 11),
        last_user_cpu_ticks: 30,
        last_system_cpu_ticks: 11,
        maximum_vm_hwm_bytes: 10_000_000,
    }];
    if scenario == ScenarioV1::Idle {
        identities.push(ProcessIdentityResourceV1 {
            pid: 10_002,
            start_time_ticks: 60,
            first_observed_offset_ns: 6_000_000_000,
            idle_window_start_user_cpu_ticks: Some(0),
            idle_window_start_system_cpu_ticks: Some(0),
            idle_window_end_user_cpu_ticks: Some(0),
            idle_window_end_system_cpu_ticks: Some(0),
            last_user_cpu_ticks: 0,
            last_system_cpu_ticks: 0,
            maximum_vm_hwm_bytes: 1_000_000,
        });
        identities.push(ProcessIdentityResourceV1 {
            pid: 10_003,
            start_time_ticks: 70,
            first_observed_offset_ns: 36_000_000_001,
            idle_window_start_user_cpu_ticks: None,
            idle_window_start_system_cpu_ticks: None,
            idle_window_end_user_cpu_ticks: None,
            idle_window_end_system_cpu_ticks: None,
            last_user_cpu_ticks: 0,
            last_system_cpu_ticks: 0,
            maximum_vm_hwm_bytes: 500_000,
        });
    }
    ProcessTreeEvidenceV1 {
        observer_pid: 20_001,
        observer_affinity_cpu_ids: vec![4, 5, 6, 7, 12, 13, 14, 15],
        observed_root_pid: 10_001,
        observed_root_start_time_ticks: 55,
        clock_ticks_per_second: 100,
        trial_origin_ns,
        observer_ready_ns,
        idle_window_start_ns: idle_start,
        idle_window_end_ns: idle_end,
        resource_observations: vec![ResourceObservationV1 {
            offset_ns: observer_ready_ns - trial_origin_ns,
            process_tree_user_cpu_ns: 10_000_000,
            process_tree_system_cpu_ns: 10_000_000,
            process_tree_rss_bytes: 10_000_000,
        }],
        process_identity_resources: identities,
    }
}

fn synthetic_digests() -> RawArtifactDigestsV1 {
    let digest = "0".repeat(64);
    RawArtifactDigestsV1 {
        harness_json_sha256: digest.clone(),
        runner_control_json_sha256: digest.clone(),
        process_tree_json_sha256: digest.clone(),
        observer_handshake_sha256: digest.clone(),
        observer_control_json_sha256: digest.clone(),
        gnu_time_sha256: digest.clone(),
        pidstat_json_sha256: digest.clone(),
        pidstat_stderr_sha256: digest.clone(),
        child_stdout_sha256: digest.clone(),
        child_stderr_sha256: digest.clone(),
        observer_stdout_sha256: digest.clone(),
        observer_stderr_sha256: digest.clone(),
        trial_status_sha256: digest,
    }
}

fn synthetic_performance_stream(
    scenario: ScenarioV1,
    origin: u64,
    admissions: &[AdmissionObservationV1],
) -> PerformanceEvidenceStreamV1 {
    let close = admissions
        .last()
        .map(|value| value.admitted_ns + 200_000_000)
        .unwrap_or(origin + 200_000_000);
    let crossing = scenario == ScenarioV1::TwiceTarget;
    let crossing_ns = origin + 30_025_000_000;
    let mut samples = vec![
        PerformanceSampleEvidenceV1 {
            sample_ordinal: 1,
            sampled_at_ns: origin.saturating_sub(1),
            event_lag_ns: 0,
            pending_events: 0,
            admission_high_water: 0,
            completion_high_water: 0,
            live_panes: 50,
            default_visible_task_runs: 200,
            dependency_edges: 1_000,
            execution_edges: 199,
            events_one_second: 0,
            events_ten_seconds: 0,
            events_sixty_seconds: 0,
            source_quality: EffectiveQualityV1::Live,
            effective_quality: EffectiveQualityV1::Live,
            reasons: Vec::new(),
        },
        PerformanceSampleEvidenceV1 {
            sample_ordinal: 2,
            sampled_at_ns: if crossing { crossing_ns } else { close },
            event_lag_ns: 0,
            pending_events: 0,
            admission_high_water: if crossing {
                1_201
            } else {
                admissions.len() as u64
            },
            completion_high_water: if crossing {
                1_201
            } else {
                admissions.len() as u64
            },
            live_panes: 50,
            default_visible_task_runs: 200,
            dependency_edges: 1_000,
            execution_edges: 199,
            events_one_second: if crossing { 40 } else { 0 },
            events_ten_seconds: if crossing { 400 } else { 0 },
            events_sixty_seconds: if crossing {
                1_201
            } else {
                admissions.len() as u64
            },
            source_quality: EffectiveQualityV1::Live,
            effective_quality: if crossing {
                EffectiveQualityV1::Degraded
            } else {
                EffectiveQualityV1::Live
            },
            reasons: if crossing {
                vec![PerformanceReasonV1::EventsSixtySeconds]
            } else {
                Vec::new()
            },
        },
    ];
    let mut frames = vec![
        PerformanceFrameEvidenceV1 {
            draw_ordinal: 1,
            sample_ordinal: 1,
            state_observed_at_ns: origin.saturating_sub(1),
            rendered_at_ns: origin,
            effective_quality: EffectiveQualityV1::Live,
            reasons: Vec::new(),
            rendered_header_line: "LIVE | perf:".to_owned(),
        },
        PerformanceFrameEvidenceV1 {
            draw_ordinal: 2,
            sample_ordinal: 2,
            state_observed_at_ns: if crossing { crossing_ns } else { close },
            rendered_at_ns: if crossing {
                crossing_ns + 107_000_000
            } else {
                close
            },
            effective_quality: if crossing {
                EffectiveQualityV1::Degraded
            } else {
                EffectiveQualityV1::Live
            },
            reasons: if crossing {
                vec![PerformanceReasonV1::EventsSixtySeconds]
            } else {
                Vec::new()
            },
            rendered_header_line: if crossing {
                "DEGRADED | perf:events_60s".to_owned()
            } else {
                "LIVE | perf:".to_owned()
            },
        },
    ];
    if crossing {
        samples.push(PerformanceSampleEvidenceV1 {
            sample_ordinal: 3,
            sampled_at_ns: close,
            event_lag_ns: 0,
            pending_events: 0,
            admission_high_water: admissions.len() as u64,
            completion_high_water: admissions.len() as u64,
            live_panes: 50,
            default_visible_task_runs: 200,
            dependency_edges: 1_000,
            execution_edges: 199,
            events_one_second: 40,
            events_ten_seconds: 400,
            events_sixty_seconds: admissions.len() as u64,
            source_quality: EffectiveQualityV1::Live,
            effective_quality: EffectiveQualityV1::Degraded,
            reasons: vec![PerformanceReasonV1::EventsSixtySeconds],
        });
        frames.push(PerformanceFrameEvidenceV1 {
            draw_ordinal: 3,
            sample_ordinal: 3,
            state_observed_at_ns: close,
            rendered_at_ns: close,
            effective_quality: EffectiveQualityV1::Degraded,
            reasons: vec![PerformanceReasonV1::EventsSixtySeconds],
            rendered_header_line: "DEGRADED | perf:events_60s".to_owned(),
        });
    }
    let terminal_observations = admissions
        .iter()
        .map(|observation| TerminalObservationV1 {
            sequence: observation.sequence,
            terminal_ns: observation.admitted_ns + 10_000_000,
        })
        .collect::<Vec<_>>();
    let terminals = terminal_observations
        .iter()
        .map(|terminal| (terminal.sequence, terminal.terminal_ns))
        .collect::<std::collections::BTreeMap<_, _>>();
    for sample in &mut samples {
        let derived = derive_performance_state(sample.sampled_at_ns, admissions, &terminals)
            .expect("synthetic performance state must derive");
        sample.event_lag_ns = derived.event_lag_ns;
        sample.pending_events = derived.pending_events;
        sample.admission_high_water = derived.admission_high_water;
        sample.completion_high_water = derived.completion_high_water;
        sample.events_one_second = derived.events_one_second;
        sample.events_ten_seconds = derived.events_ten_seconds;
        sample.events_sixty_seconds = derived.events_sixty_seconds;
        sample.reasons = expected_performance_reasons(sample);
        sample.effective_quality = if sample.reasons.is_empty() {
            sample.source_quality
        } else {
            EffectiveQualityV1::Degraded
        };
    }
    for frame in &mut frames {
        let sample = samples
            .iter()
            .find(|sample| sample.sample_ordinal == frame.sample_ordinal)
            .expect("synthetic frame must reference a sample");
        frame.state_observed_at_ns = sample.sampled_at_ns;
        frame.effective_quality = sample.effective_quality;
        frame.reasons = sample.reasons.clone();
        frame.rendered_header_line = expected_header(frame.effective_quality, &frame.reasons);
    }
    PerformanceEvidenceStreamV1 {
        workload_start_ns: origin,
        workload_close_ns: frames.last().expect("two frames").rendered_at_ns,
        first_sample_ordinal: 1,
        next_sample_ordinal: samples.len() as u64 + 1,
        first_draw_ordinal: 1,
        next_draw_ordinal: frames.len() as u64 + 1,
        samples,
        frames,
        terminal_observations,
        selected_terminal_draw_ordinal: crossing.then_some(2),
    }
}

fn refresh_performance_evidence(raw: &mut HarnessTrialV1) {
    let admissions = raw.admission_observations.clone();
    let stream = raw
        .performance_evidence_stream
        .as_mut()
        .expect("performance fixture must contain a stream");
    let terminals = stream
        .terminal_observations
        .iter()
        .map(|terminal| (terminal.sequence, terminal.terminal_ns))
        .collect::<std::collections::BTreeMap<_, _>>();
    for sample in &mut stream.samples {
        let derived = derive_performance_state(sample.sampled_at_ns, &admissions, &terminals)
            .expect("performance fixture state must derive");
        sample.event_lag_ns = derived.event_lag_ns;
        sample.pending_events = derived.pending_events;
        sample.admission_high_water = derived.admission_high_water;
        sample.completion_high_water = derived.completion_high_water;
        sample.events_one_second = derived.events_one_second;
        sample.events_ten_seconds = derived.events_ten_seconds;
        sample.events_sixty_seconds = derived.events_sixty_seconds;
        sample.reasons = expected_performance_reasons(sample);
        sample.effective_quality = if sample.reasons.is_empty() {
            sample.source_quality
        } else {
            EffectiveQualityV1::Degraded
        };
    }
    for frame in &mut stream.frames {
        let sample = stream
            .samples
            .iter()
            .find(|sample| sample.sample_ordinal == frame.sample_ordinal)
            .expect("performance fixture frame must reference a sample");
        frame.state_observed_at_ns = sample.sampled_at_ns;
        frame.effective_quality = sample.effective_quality;
        frame.reasons = sample.reasons.clone();
        frame.rendered_header_line = expected_header(frame.effective_quality, &frame.reasons);
    }
}

pub fn clone_outcome(value: &ReferenceOutcomeV1) -> ReferenceOutcomeV1 {
    serde_json::from_slice(
        &serde_json::to_vec(value).expect("synthetic outcome must serialize for mutation"),
    )
    .expect("synthetic outcome must deserialize for mutation")
}

pub fn valid_invalid_outcome(reason: FailureReasonV1) -> ReferenceOutcomeV1 {
    ReferenceOutcomeV1::Invalid {
        document: InvalidRunV1 {
            schema_version: 1,
            measurement_stage: MeasurementStageV1::Baseline,
            scenario: ScenarioV1::Sustained,
            production_subject_sha: BASELINE_SUBJECT_SHA.to_owned(),
            harness_sha: SYNTHETIC_HARNESS_SHA.to_owned(),
            workload_schema_sha256: WORKLOAD_SCHEMA_V1_SHA256.to_owned(),
            baseline_id: Some(format!(
                "sha256:v1:{BASELINE_SUBJECT_SHA}:{SYNTHETIC_HARNESS_SHA}:{WORKLOAD_SCHEMA_V1_SHA256}"
            )),
            command: vec!["workload_harness".to_owned(), "sustained".to_owned()],
            controlled_environment: run_environment(
                MeasurementStageV1::Baseline,
                ScenarioV1::Sustained,
                BASELINE_SUBJECT_SHA,
            ),
            failure_reasons: vec![reason],
        },
    }
}

pub fn valid_failed_outcome() -> ReferenceOutcomeV1 {
    failed_outcome(
        ScenarioV1::Target,
        MeasurementStageV1::Baseline,
        FailureReasonV1::InputLatency,
        100,
        1_000,
    )
}

pub fn failed_outcome(
    scenario: ScenarioV1,
    stage: MeasurementStageV1,
    reason: FailureReasonV1,
    d4_numerator: u64,
    d4_denominator: u64,
) -> ReferenceOutcomeV1 {
    let mut outcome = synthetic_result(scenario, stage);
    let document = outcome.document_mut();
    document.failure_reasons = vec![reason];
    for trial in &mut document.trials {
        for scoped in &mut trial.raw.scoped_observations {
            scoped.d4_analysis_ns = d4_numerator;
            scoped.reducer_plus_publish_ns = d4_denominator;
        }
        if !trial.raw.scoped_observations.is_empty() {
            trial.d4_analysis_ns = Some(
                trial
                    .raw
                    .scoped_observations
                    .iter()
                    .map(|value| value.d4_analysis_ns)
                    .sum(),
            );
            trial.reducer_plus_publish_ns = Some(
                trial
                    .raw
                    .scoped_observations
                    .iter()
                    .map(|value| value.reducer_plus_publish_ns)
                    .sum(),
            );
            trial.d4_ratio_parts_per_million = trial
                .d4_analysis_ns
                .zip(trial.reducer_plus_publish_ns)
                .map(|(numerator, denominator)| numerator * 1_000_000 / denominator);
        }
    }
    match reason {
        FailureReasonV1::InputLatency => {
            for trial in &mut document.trials {
                let complement = 100_000_000
                    - trial
                        .raw
                        .frame_phase_offset_ns
                        .expect("target phase must exist");
                let mut scheduled = trial
                    .raw
                    .workload_origin_ns
                    .expect("target origin must exist");
                for observation in &mut trial.raw.input_observations {
                    observation.scheduled_ns = scheduled;
                    observation.injected_ns = scheduled;
                    observation.rendered_ns = observation.injected_ns + 100_000_000;
                    observation.observed_frame_phase_ns = 0;
                    scheduled = observation.rendered_ns + complement;
                }
                trial.input_response = distribution_from(
                    trial
                        .raw
                        .input_observations
                        .iter()
                        .map(|value| value.rendered_ns - value.injected_ns)
                        .collect(),
                );
            }
        }
        FailureReasonV1::ScreenLatency => {
            for trial in &mut document.trials {
                for observation in &mut trial.raw.screen_observations {
                    observation.rendered_ns = observation.admitted_ns + 1_000_000_000;
                    observation.observed_frame_phase_ns = 0;
                }
                trial.screen_update = distribution_from(
                    trial
                        .raw
                        .screen_observations
                        .iter()
                        .map(|value| value.rendered_ns - value.admitted_ns)
                        .collect(),
                );
                trial.publish_to_render = distribution_from(
                    trial
                        .raw
                        .screen_observations
                        .iter()
                        .map(|value| value.rendered_ns - value.published_ns)
                        .collect(),
                );
            }
        }
        FailureReasonV1::StartupLatency => {
            for trial in &mut document.trials {
                trial.raw.startup_observations_ns[0] = 3_000_000_000;
                trial.startup_ns = Some(3_000_000_000);
            }
        }
        FailureReasonV1::FallbackRescanLatency => {
            for trial in &mut document.trials {
                trial.raw.fallback_pairs[0].rescan_ns =
                    trial.raw.fallback_pairs[0].notification_ns + 2_000_000_001;
                trial.fallback_added_delay_ns = distribution_from(
                    trial
                        .raw
                        .fallback_pairs
                        .iter()
                        .map(|pair| pair.rescan_ns - pair.notification_ns)
                        .collect(),
                );
            }
        }
        FailureReasonV1::IdleCpu => {
            for trial in &mut document.trials {
                trial.user_cpu_ns = 60_000_000_000;
                trial.process_tree.process_identity_resources[0].idle_window_end_user_cpu_ticks =
                    Some(6_020);
            }
        }
        FailureReasonV1::MaximumRss => {
            for trial in &mut document.trials {
                trial.maximum_process_tree_rss_bytes = 100_000_000;
                trial.process_tree.resource_observations[0].process_tree_rss_bytes = 100_000_000;
            }
        }
        FailureReasonV1::WorkloadAdmission => {
            for trial in &mut document.trials {
                let origin = trial.raw.workload_origin_ns.expect("scheduled trial");
                let late = origin + 1_000_000_000 + scenario_spec(scenario).period_ns + 1;
                trial.raw.admission_observations[0].admitted_ns = late;
                if let Some(stream) = &mut trial.raw.performance_evidence_stream
                    && let Some(terminal) = stream
                        .terminal_observations
                        .iter_mut()
                        .find(|terminal| terminal.sequence == 1)
                {
                    terminal.terminal_ns = late + 10_000_000;
                }
                if trial.raw.performance_evidence_stream.is_some() {
                    refresh_performance_evidence(&mut trial.raw);
                }
            }
        }
        FailureReasonV1::SupportedLoadDegradation => {
            for trial in &mut document.trials {
                let origin = trial
                    .raw
                    .workload_origin_ns
                    .expect("supported-load fixture has an origin");
                let stream = trial
                    .raw
                    .performance_evidence_stream
                    .as_mut()
                    .expect("supported-load final fixture has a stream");
                let mut closing_sample = stream.samples[1].clone();
                closing_sample.sample_ordinal = 3;
                let mut closing_frame = stream.frames[1].clone();
                closing_frame.draw_ordinal = 3;
                closing_frame.sample_ordinal = 3;
                let mut delayed_sample = closing_sample.clone();
                delayed_sample.sample_ordinal = 2;
                delayed_sample.sampled_at_ns = origin + 2_000_000_000;
                let mut delayed_frame = closing_frame.clone();
                delayed_frame.draw_ordinal = 2;
                delayed_frame.sample_ordinal = 2;
                delayed_frame.state_observed_at_ns = delayed_sample.sampled_at_ns;
                delayed_frame.rendered_at_ns = delayed_sample.sampled_at_ns + 100_000_000;
                stream.samples = vec![stream.samples[0].clone(), delayed_sample, closing_sample];
                stream.frames = vec![stream.frames[0].clone(), delayed_frame, closing_frame];
                stream.next_sample_ordinal = 4;
                stream.next_draw_ordinal = 4;
                stream.terminal_observations[0].terminal_ns = origin + 3_000_000_000;
                refresh_performance_evidence(&mut trial.raw);
            }
        }
        FailureReasonV1::MissingDegradation => {
            for trial in &mut document.trials {
                let stream = trial
                    .raw
                    .performance_evidence_stream
                    .as_mut()
                    .expect("twice-target final fixture has a stream");
                stream.selected_terminal_draw_ordinal = None;
                for frame in &mut stream.frames {
                    if frame
                        .reasons
                        .contains(&PerformanceReasonV1::EventsSixtySeconds)
                    {
                        frame.rendered_header_line = expected_header_without_events_sixty(
                            frame.effective_quality,
                            &frame.reasons,
                        );
                    }
                }
            }
        }
        FailureReasonV1::ControlMismatch
        | FailureReasonV1::CommandFailed
        | FailureReasonV1::IncompleteTrial
        | FailureReasonV1::SequenceLoss
        | FailureReasonV1::StructuralLoss
        | FailureReasonV1::DuplicateOutcome
        | FailureReasonV1::InvalidArtifact => {}
    }
    match outcome {
        ReferenceOutcomeV1::Pass { document } => ReferenceOutcomeV1::Failed { document },
        _ => unreachable!("synthetic result starts as Pass"),
    }
}

pub fn classify_d4_checkpoint(
    outcomes: &[ReferenceOutcomeV1],
) -> Result<D4CheckpointDecisionV1, ResultError> {
    let scenarios = [
        ScenarioV1::Target,
        ScenarioV1::Sustained,
        ScenarioV1::Burst,
        ScenarioV1::Startup,
        ScenarioV1::Idle,
        ScenarioV1::FallbackRescan,
        ScenarioV1::TwiceTarget,
    ];
    if outcomes.len() != scenarios.len() {
        return Err(ResultError::InvalidArtifact);
    }
    let first = match outcomes.first() {
        Some(ReferenceOutcomeV1::Pass { document })
        | Some(ReferenceOutcomeV1::Failed { document }) => document,
        _ => return Err(ResultError::InvalidArtifact),
    };
    let mut amendments = BTreeSet::new();
    for (outcome, expected_scenario) in outcomes.iter().zip(scenarios) {
        outcome.validate()?;
        let document = match outcome {
            ReferenceOutcomeV1::Pass { document } | ReferenceOutcomeV1::Failed { document } => {
                document
            }
            ReferenceOutcomeV1::Invalid { .. } => return Err(ResultError::InvalidArtifact),
        };
        if document.scenario != expected_scenario
            || document.measurement_stage != MeasurementStageV1::Final
            || document.production_subject_sha != first.production_subject_sha
            || document.harness_sha != first.harness_sha
            || document.baseline_id != first.baseline_id
            || document.workload_schema_sha256 != first.workload_schema_sha256
            || document.controls != first.controls
        {
            return Err(ResultError::InvalidArtifact);
        }
        for reason in &document.failure_reasons {
            let policy =
                lookup_failure_policy(document.measurement_stage, document.scenario, *reason)
                    .ok_or(ResultError::InvalidArtifact)?;
            match policy {
                D4PolicyV1::NotApplicable => return Err(ResultError::InvalidArtifact),
                D4PolicyV1::NonD4 => {
                    amendments.insert(RequiredAmendmentV1::NonD4);
                }
                D4PolicyV1::D4Scoped => {
                    let (d4, denominator) = document
                        .trials
                        .iter()
                        .try_fold((0_u128, 0_u128), |(d4, denominator), trial| {
                            let trial_d4 = trial.raw.scoped_observations.iter().try_fold(
                                0_u128,
                                |sum, observation| {
                                    sum.checked_add(observation.d4_analysis_ns as u128)
                                },
                            )?;
                            let trial_denominator = trial.raw.scoped_observations.iter().try_fold(
                                0_u128,
                                |sum, observation| {
                                    sum.checked_add(observation.reducer_plus_publish_ns as u128)
                                },
                            )?;
                            Some((
                                d4.checked_add(trial_d4)?,
                                denominator.checked_add(trial_denominator)?,
                            ))
                        })
                        .ok_or(ResultError::InvalidArtifact)?;
                    if denominator == 0 {
                        return Err(ResultError::InvalidArtifact);
                    }
                    amendments.insert(
                        if d4.checked_mul(4).ok_or(ResultError::InvalidArtifact)? >= denominator {
                            RequiredAmendmentV1::D4
                        } else {
                            RequiredAmendmentV1::NonD4
                        },
                    );
                }
            }
        }
    }
    Ok(if amendments.is_empty() {
        D4CheckpointDecisionV1::NoMissD4NotAuthorized {}
    } else {
        D4CheckpointDecisionV1::AmendmentsRequired {
            amendments: amendments.into_iter().collect(),
        }
    })
}

pub fn classify_d4_checkpoint_from_environment() -> Result<(), HarnessError> {
    let optional = require_closed_environment_with_optional(
        &[
            "HERDR_PERF_CLASSIFY_OUTPUT",
            "HERDR_PERF_CLASSIFY_RESULTS_ROOT",
        ],
        &["HERDR_PERF_ACCEPT_AMENDED_LEGACY"],
    )?;
    let legacy = amended_legacy_mode(&optional);
    let root = required_environment_path("HERDR_PERF_CLASSIFY_RESULTS_ROOT")?;
    let output = required_environment_path("HERDR_PERF_CLASSIFY_OUTPUT")?;
    let loaded = load_all_scenario_outcomes(&root, legacy)?;
    let decision = classify_d4_checkpoint(&loaded.outcomes)
        .map_err(|_| HarnessError::Invalid("D4 classification input was invalid"))?;
    let document = D4CheckpointDocumentV1 {
        schema_version: 1,
        decision,
    };
    document
        .validate()
        .map_err(|_| HarnessError::Invalid("D4 checkpoint document was invalid"))?;
    validate_reclassification_records(&loaded.reclassified)?;
    atomic_write_json(&output, &document)?;
    write_reclassification_sidecar(&output, &loaded.reclassified)
}

pub fn rederive_section15_report_from_environment() -> Result<(), HarnessError> {
    let optional = require_closed_environment_with_optional(
        &[
            "HERDR_PERF_REDERIVE_BASELINE_RESULTS_ROOT",
            "HERDR_PERF_REDERIVE_FINAL_RESULTS_ROOT",
            "HERDR_PERF_REDERIVE_OUTPUT",
        ],
        &["HERDR_PERF_ACCEPT_AMENDED_LEGACY"],
    )?;
    let legacy = amended_legacy_mode(&optional);
    let baseline_root = required_environment_path("HERDR_PERF_REDERIVE_BASELINE_RESULTS_ROOT")?;
    let final_root = required_environment_path("HERDR_PERF_REDERIVE_FINAL_RESULTS_ROOT")?;
    let output = required_environment_path("HERDR_PERF_REDERIVE_OUTPUT")?;
    let baseline_root = baseline_root.canonicalize()?;
    let final_root = final_root.canonicalize()?;
    if baseline_root == final_root
        || baseline_root.starts_with(&final_root)
        || final_root.starts_with(&baseline_root)
    {
        return Err(HarnessError::Invalid(
            "Section 15 roots must be distinct and non-nested",
        ));
    }
    let baseline = load_all_scenario_outcomes(&baseline_root, legacy)?;
    let final_results = load_all_scenario_outcomes(&final_root, legacy)?;
    if baseline
        .outcomes
        .iter()
        .any(|outcome| outcome.document().measurement_stage != MeasurementStageV1::Baseline)
        || final_results
            .outcomes
            .iter()
            .any(|outcome| outcome.document().measurement_stage != MeasurementStageV1::Final)
    {
        return Err(HarnessError::Invalid("Section 15 stage identity mismatch"));
    }
    let report = rederive_section15_document(
        &baseline_root,
        &final_root,
        &baseline.outcomes,
        &final_results.outcomes,
    )?;
    report
        .validate_with_mode(legacy)
        .map_err(|_| HarnessError::Invalid("Section 15 document was invalid"))?;
    let mut reclassified = baseline.reclassified;
    reclassified.extend(final_results.reclassified);
    validate_reclassification_records(&reclassified)?;
    atomic_write_json(&output, &report)?;
    write_reclassification_sidecar(&output, &reclassified)
}

pub fn compose_reference_outcome_from_environment() -> Result<i32, HarnessError> {
    let stage = parse_stage_token(&required_environment_string("HERDR_PERF_COMPOSE_STAGE")?)?;
    let mut keys = vec![
        "HERDR_PERF_COMPOSE_OUTPUT",
        "HERDR_PERF_COMPOSE_PREFLIGHT_HEAD",
        "HERDR_PERF_COMPOSE_RAW_ROOT",
        "HERDR_PERF_COMPOSE_SCENARIO",
        "HERDR_PERF_COMPOSE_STAGE",
        "HERDR_PERF_COMPOSE_SUBJECT",
    ];
    if stage != MeasurementStageV1::Baseline {
        keys.push("HERDR_PERF_COMPOSE_BASELINE_RESULTS_ROOT");
    }
    require_closed_environment(&keys)?;
    let scenario =
        parse_mapped_scenario_token(&required_environment_string("HERDR_PERF_COMPOSE_SCENARIO")?)?;
    let request = ComposeRequestV1 {
        raw_root: required_environment_path("HERDR_PERF_COMPOSE_RAW_ROOT")?,
        output: required_environment_path("HERDR_PERF_COMPOSE_OUTPUT")?,
        measurement_stage: stage,
        scenario,
        production_subject_sha: required_environment_string("HERDR_PERF_COMPOSE_SUBJECT")?,
        preflight_head: required_environment_string("HERDR_PERF_COMPOSE_PREFLIGHT_HEAD")?,
        baseline_results_root: if stage == MeasurementStageV1::Baseline {
            None
        } else {
            Some(required_environment_path(
                "HERDR_PERF_COMPOSE_BASELINE_RESULTS_ROOT",
            )?)
        },
    };
    let outcome = compose_reference_outcome_from_raw_impl(&request)?;
    let code = status_code(&outcome);
    atomic_write_reference_outcome(&request.output, &outcome)?;
    Ok(code)
}

pub fn validate_reference_outcome_from_environment() -> Result<i32, HarnessError> {
    let stage = parse_stage_token(&required_environment_string("HERDR_PERF_VALIDATE_STAGE")?)?;
    let mut keys = vec![
        "HERDR_PERF_VALIDATE_CANDIDATE",
        "HERDR_PERF_VALIDATE_COMPOSER_STATUS",
        "HERDR_PERF_VALIDATE_OUTPUT",
        "HERDR_PERF_VALIDATE_PREFLIGHT_HEAD",
        "HERDR_PERF_VALIDATE_RAW_ROOT",
        "HERDR_PERF_VALIDATE_SCENARIO",
        "HERDR_PERF_VALIDATE_STAGE",
        "HERDR_PERF_VALIDATE_SUBJECT",
        "HERDR_PERF_VALIDATE_TRIAL_STATUS",
    ];
    if stage != MeasurementStageV1::Baseline {
        keys.push("HERDR_PERF_VALIDATE_BASELINE_RESULTS_ROOT");
    }
    require_closed_environment(&keys)?;
    let scenario = parse_mapped_scenario_token(&required_environment_string(
        "HERDR_PERF_VALIDATE_SCENARIO",
    )?)?;
    validate_reference_outcome_impl(&ValidateRequestV1 {
        raw_root: required_environment_path("HERDR_PERF_VALIDATE_RAW_ROOT")?,
        candidate: required_environment_path("HERDR_PERF_VALIDATE_CANDIDATE")?,
        output: required_environment_path("HERDR_PERF_VALIDATE_OUTPUT")?,
        measurement_stage: stage,
        scenario,
        production_subject_sha: required_environment_string("HERDR_PERF_VALIDATE_SUBJECT")?,
        preflight_head: required_environment_string("HERDR_PERF_VALIDATE_PREFLIGHT_HEAD")?,
        composer_status: required_environment_string("HERDR_PERF_VALIDATE_COMPOSER_STATUS")?,
        trial_status: required_environment_string("HERDR_PERF_VALIDATE_TRIAL_STATUS")?,
        baseline_results_root: if stage == MeasurementStageV1::Baseline {
            None
        } else {
            Some(required_environment_path(
                "HERDR_PERF_VALIDATE_BASELINE_RESULTS_ROOT",
            )?)
        },
    })
}

pub fn recorded_harness_identity_is_consistent(
    harness: &HarnessTrialV1,
    scenario: ScenarioV1,
    trial_index: usize,
    raw_root: &std::path::Path,
) -> bool {
    harness.scenario == scenario
        && harness.trial_index == trial_index
        && harness.child_controls.scratch_root == raw_root.join("scratch").to_string_lossy()
}

pub fn record_runner_control_evidence_from_environment() -> Result<(), HarnessError> {
    require_exact_environment(&[
        "CARGO_HOME",
        "HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1",
        "HERDR_INCREMENT5_CONTROLLER_CANONICAL",
        "HERDR_INCREMENT5_CONTROLLER_REQUESTED",
        "HERDR_INCREMENT5_CONTROLLER_SHA256",
        "HERDR_INCREMENT5_RUNNER_CANONICAL",
        "HERDR_INCREMENT5_RUNNER_REQUESTED",
        "HERDR_INCREMENT5_RUNNER_SHA256",
        "HERDR_PERF_CONTROL_INVOCATION_CWD",
        "HERDR_PERF_CONTROL_MEASURED_CANONICAL",
        "HERDR_PERF_CONTROL_MEASURED_REQUESTED",
        "HERDR_PERF_CONTROL_MEASURED_SHA256",
        "HERDR_PERF_CONTROL_OUTPUT",
        "HERDR_PERF_CONTROL_PIDSTAT_CHILD_STATUS_MODE",
        "HERDR_PERF_CONTROL_PIDSTAT_EXIT_STATUS",
        "HERDR_PERF_CONTROL_PREFLIGHT_HEAD",
        "HERDR_PERF_CONTROL_RAW_ROOT",
        "HERDR_PERF_CONTROL_SCENARIO",
        "HERDR_PERF_CONTROL_STAGE",
        "HERDR_PERF_CONTROL_SUBJECT",
        "HERDR_PERF_CONTROL_TRIAL_INDEX",
        "HERDR_PERF_CONTROL_TRIAL_STATUS_PATH",
        "HOME",
        "LC_ALL",
        "PATH",
        "RUSTUP_HOME",
        "TZ",
    ])
    .or_else(|error| {
        if std::env::var_os("HERDR_PERF_CONTROL_BASELINE_RESULTS_ROOT").is_some() {
            require_exact_environment(&[
                "CARGO_HOME",
                "HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1",
                "HERDR_INCREMENT5_CONTROLLER_CANONICAL",
                "HERDR_INCREMENT5_CONTROLLER_REQUESTED",
                "HERDR_INCREMENT5_CONTROLLER_SHA256",
                "HERDR_INCREMENT5_RUNNER_CANONICAL",
                "HERDR_INCREMENT5_RUNNER_REQUESTED",
                "HERDR_INCREMENT5_RUNNER_SHA256",
                "HERDR_PERF_CONTROL_BASELINE_RESULTS_ROOT",
                "HERDR_PERF_CONTROL_INVOCATION_CWD",
                "HERDR_PERF_CONTROL_MEASURED_CANONICAL",
                "HERDR_PERF_CONTROL_MEASURED_REQUESTED",
                "HERDR_PERF_CONTROL_MEASURED_SHA256",
                "HERDR_PERF_CONTROL_OUTPUT",
                "HERDR_PERF_CONTROL_PIDSTAT_CHILD_STATUS_MODE",
                "HERDR_PERF_CONTROL_PIDSTAT_EXIT_STATUS",
                "HERDR_PERF_CONTROL_PREFLIGHT_HEAD",
                "HERDR_PERF_CONTROL_RAW_ROOT",
                "HERDR_PERF_CONTROL_SCENARIO",
                "HERDR_PERF_CONTROL_STAGE",
                "HERDR_PERF_CONTROL_SUBJECT",
                "HERDR_PERF_CONTROL_TRIAL_INDEX",
                "HERDR_PERF_CONTROL_TRIAL_STATUS_PATH",
                "HOME",
                "LC_ALL",
                "PATH",
                "RUSTUP_HOME",
                "TZ",
            ])
        } else {
            Err(error)
        }
    })?;
    if invariant_environment()
        .iter()
        .any(|(key, value)| std::env::var(key).ok().as_ref() != Some(value))
    {
        return Err(HarnessError::Invalid(
            "control invariant environment mismatch",
        ));
    }
    let stage = parse_stage_token(&required_environment_string("HERDR_PERF_CONTROL_STAGE")?)?;
    let scenario =
        parse_mapped_scenario_token(&required_environment_string("HERDR_PERF_CONTROL_SCENARIO")?)?;
    let subject = required_environment_string("HERDR_PERF_CONTROL_SUBJECT")?;
    let preflight_head = required_environment_string("HERDR_PERF_CONTROL_PREFLIGHT_HEAD")?;
    if !is_lower_hex(&subject, 40) || !is_lower_hex(&preflight_head, 40) {
        return Err(HarnessError::Invalid("control Git identity was malformed"));
    }
    let raw_root = required_environment_path("HERDR_PERF_CONTROL_RAW_ROOT")?.canonicalize()?;
    let output = required_environment_path("HERDR_PERF_CONTROL_OUTPUT")?;
    if output != raw_root.join("runner-control.json") {
        return Err(HarnessError::Invalid("control output path was substituted"));
    }
    let trial_index = required_environment_string("HERDR_PERF_CONTROL_TRIAL_INDEX")?
        .parse::<usize>()
        .ok()
        .filter(|index| {
            *index > 0
                && index.to_string()
                    == std::env::var("HERDR_PERF_CONTROL_TRIAL_INDEX").unwrap_or_default()
        })
        .ok_or(HarnessError::Invalid(
            "control trial index was not canonical",
        ))?;
    if trial_index > scenario_spec(scenario).recorded_trials {
        return Err(HarnessError::Invalid(
            "control trial index was out of range",
        ));
    }
    let trial_status_path = required_environment_path("HERDR_PERF_CONTROL_TRIAL_STATUS_PATH")?;
    if trial_status_path != raw_root.join("trial-status") {
        return Err(HarnessError::Invalid("trial-status path was substituted"));
    }
    let trial_status = parse_trial_status(&std::fs::read(&trial_status_path)?)
        .ok_or(HarnessError::Invalid("trial-status was malformed"))?;
    let pidstat_exit_status =
        required_environment_string("HERDR_PERF_CONTROL_PIDSTAT_EXIT_STATUS")?
            .parse::<u8>()
            .ok()
            .filter(|status| {
                status.to_string()
                    == std::env::var("HERDR_PERF_CONTROL_PIDSTAT_EXIT_STATUS").unwrap_or_default()
            })
            .ok_or(HarnessError::Invalid("pidstat status was not canonical"))?;
    let pidstat_child_status_mode =
        match required_environment_string("HERDR_PERF_CONTROL_PIDSTAT_CHILD_STATUS_MODE")?.as_str()
        {
            "propagates_child_status" => PidstatChildStatusModeV1::PropagatesChildStatus,
            "monitor_only" => PidstatChildStatusModeV1::MonitorOnly,
            _ => return Err(HarnessError::Invalid("pidstat status mode was invalid")),
        };
    if !pidstat_status_is_consistent(pidstat_child_status_mode, trial_status, pidstat_exit_status) {
        return Err(HarnessError::Invalid(
            "pidstat status disagreed with sentinel",
        ));
    }
    let authoritative = parse_bootstrap_tool_manifest(&required_environment_string(
        "HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1",
    )?)?;
    for identity in &authoritative {
        revalidate_executable_identity(identity)?;
    }
    let controller = executable_identity_from_environment(
        "HERDR_INCREMENT5_CONTROLLER_REQUESTED",
        "HERDR_INCREMENT5_CONTROLLER_CANONICAL",
        "HERDR_INCREMENT5_CONTROLLER_SHA256",
    )?;
    revalidate_executable_identity(&controller)?;
    let runner_script = executable_identity_from_environment(
        "HERDR_INCREMENT5_RUNNER_REQUESTED",
        "HERDR_INCREMENT5_RUNNER_CANONICAL",
        "HERDR_INCREMENT5_RUNNER_SHA256",
    )?;
    revalidate_executable_identity(&runner_script)?;
    let measured_binary = executable_identity_from_environment(
        "HERDR_PERF_CONTROL_MEASURED_REQUESTED",
        "HERDR_PERF_CONTROL_MEASURED_CANONICAL",
        "HERDR_PERF_CONTROL_MEASURED_SHA256",
    )?;
    revalidate_executable_identity(&measured_binary)?;
    let invocation_cwd = required_environment_path("HERDR_PERF_CONTROL_INVOCATION_CWD")?;
    let canonical_cwd = invocation_cwd.canonicalize()?;
    if invocation_cwd != canonical_cwd {
        return Err(HarnessError::Invalid("invocation cwd was not canonical"));
    }
    let git = authoritative
        .iter()
        .find(|identity| identity.requested_path == "/usr/bin/git")
        .ok_or(HarnessError::Invalid("Git identity was absent"))?;
    let actual_head =
        run_closed_command(&git.canonical_path, &["rev-parse", "HEAD"], &canonical_cwd)?;
    if actual_head.trim_end() != preflight_head
        || !run_closed_status(&git.canonical_path, &["diff", "--quiet"], &canonical_cwd)?
        || !run_closed_status(
            &git.canonical_path,
            &["diff", "--cached", "--quiet"],
            &canonical_cwd,
        )?
    {
        return Err(HarnessError::Invalid(
            "pre-composition Git state was not clean",
        ));
    }
    let absent_candidates = cargo_configuration_candidates(&canonical_cwd)
        .map_err(|_| HarnessError::Invalid("Cargo configuration candidates were invalid"))?;
    cargo_configuration_candidates_are_absent(&absent_candidates)
        .map_err(|_| HarnessError::Invalid("Cargo configuration was present"))?;
    let rustup_requested = developer_home_path(".cargo/bin/rustup");
    let rustup = authoritative
        .first()
        .filter(|identity| identity.requested_path == rustup_requested)
        .ok_or(HarnessError::Invalid("rustup identity was absent"))?;
    let rustc_version = run_closed_command(
        &rustup.canonical_path,
        &["run", "1.97.1", "rustc", "--version"],
        &canonical_cwd,
    )?
    .trim_end()
    .to_owned();
    let cargo_version = run_closed_command(
        &rustup.canonical_path,
        &["run", "1.97.1", "cargo", "--version"],
        &canonical_cwd,
    )?
    .trim_end()
    .to_owned();
    let harness: HarnessTrialV1 = read_closed_json(&raw_root.join("harness.json"))
        .map_err(|_| HarnessError::Invalid("recorded harness was invalid"))?;
    let process_tree: ProcessTreeEvidenceV1 = read_closed_json(&raw_root.join("process-tree.json"))
        .map_err(|_| HarnessError::Invalid("recorded process tree was invalid"))?;
    if !recorded_harness_identity_is_consistent(&harness, scenario, trial_index, &raw_root) {
        return Err(HarnessError::Invalid(
            "recorded harness identity mismatched",
        ));
    }
    let findmnt = authoritative
        .iter()
        .find(|identity| identity.requested_path == "/usr/bin/findmnt")
        .ok_or(HarnessError::Invalid("findmnt identity was absent"))?;
    let scratch_root_path = raw_root.join("scratch");
    let (storage_kind, storage_device) =
        linux_storage_profile(&findmnt.canonical_path, &scratch_root_path)?;
    let current_host = linux_host_profile(storage_kind.clone(), storage_device.clone())?;
    let first_control = if trial_index == 1 {
        None
    } else {
        let scenario_root = raw_root.parent().ok_or(HarnessError::Invalid(
            "recorded trial root had no scenario parent",
        ))?;
        let first_root = scenario_root.join("trial-0001").canonicalize()?;
        let first: RunnerControlEvidenceV1 =
            read_closed_json(&first_root.join("runner-control.json"))
                .map_err(|_| HarnessError::Invalid("first recorded runner control was invalid"))?;
        if first.schema_version != 1
            || first.measurement_stage != stage
            || first.scenario != scenario
            || first.trial_index != 1
            || first.canonical_raw_root != first_root.to_string_lossy()
            || first.production_subject_sha != subject
            || first.preflight_head != preflight_head
            || first.harness_sha != preflight_head
            || first.workload_schema_sha256 != WORKLOAD_SCHEMA_V1_SHA256
        {
            return Err(HarnessError::Invalid(
                "first recorded runner control identity mismatched",
            ));
        }
        Some(first)
    };
    let host = freeze_run_host_profile(
        current_host,
        first_control.as_ref().map(|control| &control.host),
    )?;
    let baseline_root = match (
        stage,
        std::env::var_os("HERDR_PERF_CONTROL_BASELINE_RESULTS_ROOT"),
    ) {
        (MeasurementStageV1::Baseline, None) => None,
        (MeasurementStageV1::PostReliability | MeasurementStageV1::Final, Some(root)) => {
            Some(std::path::PathBuf::from(root).canonicalize()?)
        }
        _ => {
            return Err(HarnessError::Invalid(
                "baseline root applicability mismatched",
            ));
        }
    };
    let controlled_environment =
        run_environment_with_baseline(stage, scenario, &subject, baseline_root.as_deref());
    let controls = RunControlsV1 {
        affinity_cpu_ids: vec![0, 1, 2, 3],
        address_space_limit_bytes: 17_179_869_184,
        true_cgroup_memory_limit: false,
        toolchain_launcher: rustup.clone(),
        toolchain_name: "1.97.1".to_owned(),
        rustc_version,
        cargo_version,
        build_environment: invariant_environment(),
        cargo_configuration: CargoConfigurationPolicyV1 {
            policy_version: 1,
            invocation_cwd: canonical_cwd.to_string_lossy().into_owned(),
            ordered_absent_candidates: absent_candidates
                .into_iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect(),
        },
        measured_binary: measured_binary.clone(),
        runner_script: runner_script.clone(),
        authoritative_executables: authoritative.clone(),
        pidstat_child_status_mode,
        limitation: "address-space cap is not a true cgroup memory limit".to_owned(),
    };
    validate_run_controls(&controls)
        .map_err(|_| HarnessError::Invalid("recorded run controls were invalid"))?;
    let canonical_raw_root = raw_root.to_string_lossy().into_owned();
    let scratch_root = scratch_root_path.to_string_lossy().into_owned();
    let control_socket = harness
        .child_controls
        .measured_environment
        .get("HERDR_PERF_OBSERVER_CONTROL_SOCKET")
        .ok_or(HarnessError::Invalid(
            "measured child control socket was absent",
        ))?;
    if harness.child_controls.effective_affinity_cpu_ids != controls.affinity_cpu_ids
        || harness.child_controls.effective_address_space_limit_bytes
            != controls.address_space_limit_bytes
        || harness.child_controls.measured_environment
            != measured_environment(
                &canonical_raw_root,
                &scratch_root,
                control_socket,
                stage,
                scenario,
                &subject,
                baseline_root.as_deref(),
            )
    {
        return Err(HarnessError::Invalid("measured child controls mismatched"));
    }
    let evidence = RunnerControlEvidenceV1 {
        schema_version: 1,
        measurement_stage: stage,
        scenario,
        trial_index,
        canonical_raw_root: canonical_raw_root.clone(),
        production_subject_sha: subject,
        preflight_head: preflight_head.clone(),
        harness_sha: preflight_head,
        workload_schema_sha256: WORKLOAD_SCHEMA_V1_SHA256.to_owned(),
        tracked_clean_before_composition: true,
        build_profile: "release".to_owned(),
        command: vec![
            "workload_harness".to_owned(),
            scenario_spec(scenario).cli_token.clone(),
        ],
        controlled_environment,
        render_surface: workload_schema().render_surface.clone(),
        host,
        controls,
        trial: TrialControlEvidenceV1 {
            scratch_root: scratch_root.clone(),
            scratch_storage_kind: storage_kind,
            scratch_storage_devices: vec![storage_device],
            orchestrator_environment: invariant_environment(),
            observer_environment: observer_environment(
                &canonical_raw_root,
                control_socket,
                scenario,
                &process_tree,
            ),
            validator_environment_template: invariant_environment(),
            revalidated_executables: authoritative,
            revalidated_runner_script: runner_script,
            revalidated_measured_binary: measured_binary,
            trial_status,
            pidstat_exit_status,
        },
    };
    atomic_write_json(&output, &evidence)
}

fn parse_mapped_scenario_token(value: &str) -> Result<ScenarioV1, HarnessError> {
    workload_schema()
        .scenarios
        .iter()
        .find(|row| row.directory == value)
        .map(|row| row.scenario)
        .ok_or(HarnessError::Invalid("unknown mapped workload scenario"))
}

fn run_environment_with_baseline(
    measurement_stage: MeasurementStageV1,
    scenario: ScenarioV1,
    production_subject_sha: &str,
    baseline_root: Option<&std::path::Path>,
) -> std::collections::BTreeMap<String, String> {
    let mut values = invariant_environment();
    values.insert(
        "HERDR_PERF_SCENARIO".to_owned(),
        scenario_spec(scenario).cli_token.clone(),
    );
    values.insert(
        "HERDR_PERF_STAGE".to_owned(),
        stage_cli_token(measurement_stage).to_owned(),
    );
    values.insert(
        "HERDR_PERF_SUBJECT".to_owned(),
        production_subject_sha.to_owned(),
    );
    if let Some(root) = baseline_root {
        values.insert(
            "HERDR_PERF_BASELINE_RESULTS_ROOT".to_owned(),
            root.to_string_lossy().into_owned(),
        );
    }
    values
}

fn executable_identity_from_environment(
    requested: &'static str,
    canonical: &'static str,
    sha256: &'static str,
) -> Result<ExecutableIdentityV1, HarnessError> {
    Ok(ExecutableIdentityV1 {
        requested_path: required_environment_string(requested)?,
        canonical_path: required_environment_string(canonical)?,
        sha256: required_environment_string(sha256)?,
    })
}

fn parse_bootstrap_tool_manifest(value: &str) -> Result<Vec<ExecutableIdentityV1>, HarnessError> {
    if !value.ends_with('\n') || value.contains('\r') {
        return Err(HarnessError::Invalid(
            "bootstrap tool manifest was malformed",
        ));
    }
    let expected = authoritative_executables()
        .into_iter()
        .map(|identity| identity.requested_path)
        .collect::<Vec<_>>();
    let mut identities = Vec::new();
    for (line, expected_requested) in value.lines().zip(&expected) {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 3 || fields[0] != expected_requested {
            return Err(HarnessError::Invalid(
                "bootstrap tool manifest was reordered",
            ));
        }
        identities.push(ExecutableIdentityV1 {
            requested_path: fields[0].to_owned(),
            canonical_path: fields[1].to_owned(),
            sha256: fields[2].to_owned(),
        });
    }
    if identities.len() != expected.len() || value.lines().count() != expected.len() {
        return Err(HarnessError::Invalid(
            "bootstrap tool manifest was incomplete",
        ));
    }
    Ok(identities)
}

fn revalidate_executable_identity(identity: &ExecutableIdentityV1) -> Result<(), HarnessError> {
    use std::os::unix::fs::PermissionsExt as _;
    if !executable_identity_is_well_formed(identity) {
        return Err(HarnessError::Invalid("executable identity was malformed"));
    }
    let canonical = std::fs::canonicalize(&identity.requested_path)?;
    if canonical != std::path::Path::new(&identity.canonical_path)
        || std::fs::canonicalize(&identity.canonical_path)? != canonical
    {
        return Err(HarnessError::Invalid("executable canonical path drifted"));
    }
    let metadata = std::fs::metadata(&canonical)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(HarnessError::Invalid(
            "executable target was not executable",
        ));
    }
    if sha256_path(&canonical)? != identity.sha256 {
        return Err(HarnessError::Invalid("executable digest drifted"));
    }
    Ok(())
}

fn run_closed_command(
    program: &str,
    args: &[&str],
    cwd: &std::path::Path,
) -> Result<String, HarnessError> {
    let output = std::process::Command::new(program)
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .envs(invariant_environment())
        .output()?;
    if !output.status.success() || !output.stderr.is_empty() {
        return Err(HarnessError::Invalid("closed command failed"));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| HarnessError::Invalid("closed command output was not UTF-8"))
}

fn run_closed_status(
    program: &str,
    args: &[&str],
    cwd: &std::path::Path,
) -> Result<bool, HarnessError> {
    let output = std::process::Command::new(program)
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .envs(invariant_environment())
        .output()?;
    Ok(output.status.success() && output.stdout.is_empty() && output.stderr.is_empty())
}

#[cfg(target_os = "linux")]
fn linux_storage_profile(
    findmnt: &str,
    path: &std::path::Path,
) -> Result<(String, String), HarnessError> {
    let output = std::process::Command::new(findmnt)
        .args(["-n", "-o", "SOURCE,FSTYPE", "--target"])
        .arg(path)
        .env_clear()
        .envs(invariant_environment())
        .output()?;
    if !output.status.success() || !output.stderr.is_empty() {
        return Err(HarnessError::Invalid("storage lookup failed"));
    }
    let text = String::from_utf8(output.stdout)
        .map_err(|_| HarnessError::Invalid("storage lookup was not UTF-8"))?;
    let fields = text.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 2 {
        return Err(HarnessError::Invalid("storage lookup was malformed"));
    }
    let device = std::path::Path::new(fields[0])
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or(fields[0])
        .to_owned();
    let kind = if device.starts_with("nvme") {
        "nvme".to_owned()
    } else {
        fields[1].to_owned()
    };
    Ok((kind, device))
}

#[cfg(not(target_os = "linux"))]
fn linux_storage_profile(
    _findmnt: &str,
    _path: &std::path::Path,
) -> Result<(String, String), HarnessError> {
    Err(HarnessError::Invalid("reference recorder requires Linux"))
}

#[cfg(target_os = "linux")]
fn linux_host_profile(
    storage_kind: String,
    storage_device: String,
) -> Result<HostProfileV1, HarnessError> {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo")?;
    let cpu_model = cpuinfo
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.trim() == "model name")
        })
        .map(|(_, value)| value.trim().to_owned())
        .ok_or(HarnessError::Invalid("CPU model was absent"))?;
    let mut physical_core_ids = BTreeSet::new();
    let mut physical = None;
    let mut core = None;
    for line in cpuinfo.lines().chain(std::iter::once("")) {
        if let Some((key, value)) = line.split_once(':') {
            match key.trim() {
                "physical id" => physical = Some(value.trim().to_owned()),
                "core id" => core = Some(value.trim().to_owned()),
                _ => {}
            }
        } else if line.is_empty()
            && let (Some(physical), Some(core)) = (physical.take(), core.take())
        {
            physical_core_ids.insert(format!("{physical}:{core}"));
        }
    }
    if physical_core_ids.is_empty() {
        return Err(HarnessError::Invalid(
            "physical core identities were absent",
        ));
    }
    let memory_total_bytes = std::fs::read_to_string("/proc/meminfo")?
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|kib| kib.checked_mul(1024))
        .ok_or(HarnessError::Invalid("memory total was malformed"))?;
    let load = std::fs::read_to_string("/proc/loadavg")?;
    let loads = load
        .split_whitespace()
        .take(3)
        .map(parse_decimal_milli)
        .collect::<Result<Vec<_>, _>>()?;
    let ambient_load_milli: [u64; 3] = loads
        .try_into()
        .map_err(|_| HarnessError::Invalid("load average was malformed"))?;
    let governor = std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
        .ok()
        .map(|value| value.trim().to_owned());
    let boost = std::fs::read_to_string("/sys/devices/system/cpu/cpufreq/boost")
        .ok()
        .map(|value| value.trim().to_owned())
        .or_else(|| {
            std::fs::read_to_string("/sys/devices/system/cpu/intel_pstate/no_turbo")
                .ok()
                .map(|value| value.trim().to_owned())
        });
    Ok(HostProfileV1 {
        operating_system: "linux".to_owned(),
        kernel: std::fs::read_to_string("/proc/sys/kernel/osrelease")?
            .trim()
            .to_owned(),
        architecture: std::env::consts::ARCH.to_owned(),
        cpu_model,
        physical_core_ids: physical_core_ids.into_iter().collect(),
        memory_total_bytes,
        storage_kind,
        storage_device,
        governor,
        boost,
        ambient_load_milli,
    })
}

#[cfg(not(target_os = "linux"))]
fn linux_host_profile(
    _storage_kind: String,
    _storage_device: String,
) -> Result<HostProfileV1, HarnessError> {
    Err(HarnessError::Invalid("reference recorder requires Linux"))
}

fn parse_decimal_milli(value: &str) -> Result<u64, HarnessError> {
    let (whole, fraction) = value
        .split_once('.')
        .ok_or(HarnessError::Invalid("decimal load value was malformed"))?;
    if fraction.len() > 3
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(HarnessError::Invalid("decimal load value was malformed"));
    }
    let whole = whole
        .parse::<u64>()
        .map_err(|_| HarnessError::Invalid("decimal load value overflowed"))?;
    let fraction = format!("{fraction:0<3}")
        .parse::<u64>()
        .map_err(|_| HarnessError::Invalid("decimal load value overflowed"))?;
    whole
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(fraction))
        .ok_or(HarnessError::Invalid("decimal load value overflowed"))
}

#[derive(Clone, Debug)]
pub struct ComposeRequestV1 {
    pub raw_root: std::path::PathBuf,
    pub output: std::path::PathBuf,
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub production_subject_sha: String,
    pub preflight_head: String,
    pub baseline_results_root: Option<std::path::PathBuf>,
}

#[derive(Clone, Debug)]
pub struct ValidateRequestV1 {
    pub raw_root: std::path::PathBuf,
    pub candidate: std::path::PathBuf,
    pub output: std::path::PathBuf,
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub production_subject_sha: String,
    pub preflight_head: String,
    pub composer_status: String,
    pub trial_status: String,
    pub baseline_results_root: Option<std::path::PathBuf>,
}

pub fn validate_with_raw_root(
    outcome: &ReferenceOutcomeV1,
    raw_root: &std::path::Path,
) -> Result<(), ResultError> {
    outcome.validate()?;
    let document = match outcome {
        ReferenceOutcomeV1::Pass { document } | ReferenceOutcomeV1::Failed { document } => document,
        ReferenceOutcomeV1::Invalid { .. } => return Ok(()),
    };
    let canonical_root = raw_root
        .canonicalize()
        .map_err(|_| ResultError::InvalidArtifact)?;
    let mut canonical_paths = BTreeSet::new();
    for trial in &document.trials {
        let directory = canonical_root.join(format!("trial-{:04}", trial.trial_index));
        let harness: HarnessTrialV1 = read_closed_json(&directory.join("harness.json"))?;
        let runner: RunnerControlEvidenceV1 =
            read_closed_json(&directory.join("runner-control.json"))?;
        let tree: ProcessTreeEvidenceV1 = read_closed_json(&directory.join("process-tree.json"))?;
        let control: ObserverControlEvidenceV1 =
            read_closed_json(&directory.join("observer-control.json"))?;
        let handshake = parse_observer_handshake(
            &std::fs::read(directory.join("observer-handshake"))
                .map_err(|_| ResultError::InvalidArtifact)?,
        )
        .ok_or(ResultError::InvalidArtifact)?;
        if harness != trial.raw
            || tree != trial.process_tree
            || control != trial.observer_control
            || runner != runner_control_for(document, trial, &directory)
            || !recorded_harness_identity_is_consistent(
                &harness,
                document.scenario,
                trial.trial_index,
                &directory,
            )
            || runner.trial.scratch_root != directory.join("scratch").to_string_lossy()
            || handshake
                != (
                    tree.observed_root_pid,
                    tree.observed_root_start_time_ticks,
                    tree.trial_origin_ns,
                )
        {
            return Err(ResultError::InvalidArtifact);
        }
        for (name, digest) in artifact_digest_pairs(&trial.raw_artifacts) {
            let path = directory.join(name);
            let canonical = path
                .canonicalize()
                .map_err(|_| ResultError::InvalidArtifact)?;
            if !canonical.starts_with(&canonical_root)
                || !canonical_paths.insert(canonical)
                || sha256_path(&path).map_err(|_| ResultError::InvalidArtifact)? != digest
            {
                return Err(ResultError::InvalidArtifact);
            }
        }
    }
    Ok(())
}

pub fn compose_reference_outcome_from_raw_impl(
    request: &ComposeRequestV1,
) -> Result<ReferenceOutcomeV1, HarnessError> {
    match compose_reference_run(request) {
        Ok(mut document) => {
            let failures = validate_reference_run(&document, true)
                .map_err(|_| HarnessError::Invalid("composed result did not validate"))?;
            if document.trials.iter().any(|trial| {
                matches!(
                    trial.control_evidence.trial_status,
                    TrialStatusV1::Failed { .. }
                )
            }) {
                return Ok(invalid_for_request(request, FailureReasonV1::CommandFailed));
            }
            document.failure_reasons = failures.into_iter().collect();
            let outcome = if document.failure_reasons.is_empty() {
                ReferenceOutcomeV1::Pass { document }
            } else {
                ReferenceOutcomeV1::Failed { document }
            };
            outcome
                .validate()
                .map_err(|_| HarnessError::Invalid("composed outcome did not validate"))?;
            Ok(outcome)
        }
        Err(_) => Ok(invalid_for_request(
            request,
            FailureReasonV1::InvalidArtifact,
        )),
    }
}

pub fn atomic_write_reference_outcome(
    path: &std::path::Path,
    outcome: &ReferenceOutcomeV1,
) -> Result<(), HarnessError> {
    outcome
        .validate()
        .map_err(|_| HarnessError::Invalid("refused to write an invalid outcome"))?;
    atomic_write_json(path, outcome)
}

fn reclassification_sidecar_path(output: &std::path::Path) -> std::path::PathBuf {
    let mut path = output.as_os_str().to_os_string();
    path.push(".reclassification.json");
    path.into()
}

fn validate_reclassification_records(
    records: &[ReclassificationRecordV1],
) -> Result<(), HarnessError> {
    if records.iter().enumerate().any(|(index, record)| {
        records[index + 1..]
            .iter()
            .any(|later| later.scenario == record.scenario)
    }) {
        return Err(HarnessError::Invalid(
            "legacy reclassification contained duplicate scenarios",
        ));
    }
    Ok(())
}

fn write_reclassification_sidecar(
    output: &std::path::Path,
    records: &[ReclassificationRecordV1],
) -> Result<(), HarnessError> {
    if records.is_empty() {
        return match std::fs::remove_file(reclassification_sidecar_path(output)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        };
    }
    atomic_write_json(
        &reclassification_sidecar_path(output),
        &ReclassificationSidecarV1 {
            schema_version: 1,
            rule: "amended_legacy_v1",
            reclassified: records,
        },
    )
}

fn document_has_tolerated_boundary_sample(document: &ReferenceRunV1) -> bool {
    document.trials.iter().any(|trial| {
        trial
            .raw
            .performance_evidence_stream
            .as_ref()
            .is_some_and(|stream| {
                let trial_has_event_lag_reason = stream
                    .samples
                    .iter()
                    .any(|sample| sample.reasons.contains(&PerformanceReasonV1::EventLag));
                stream.samples.iter().any(|sample| {
                    tolerated_boundary_degradation(
                        document.measurement_stage,
                        document.scenario,
                        sample,
                        trial_has_event_lag_reason,
                    )
                })
            })
    })
}

pub fn read_and_validate_reference_outcome(
    path: &std::path::Path,
    legacy: AmendedLegacyMode,
) -> Result<ReferenceOutcomeRead, HarnessError> {
    let mut outcome: ReferenceOutcomeV1 = read_closed_json(path)
        .map_err(|_| HarnessError::Invalid("stored outcome was not canonical closed JSON"))?;
    if outcome.validate().is_ok() {
        return Ok(ReferenceOutcomeRead {
            outcome,
            reclassified: None,
        });
    }
    let reclassified = match (&outcome, legacy) {
        (ReferenceOutcomeV1::Failed { document }, AmendedLegacyMode::AcceptAmendedLegacy)
            if document.failure_reasons == [FailureReasonV1::SupportedLoadDegradation]
                && document.measurement_stage == MeasurementStageV1::Final
                && document_has_tolerated_boundary_sample(document)
                && validate_reference_run(document, false)
                    .is_ok_and(|derived| derived.is_empty()) =>
        {
            Some(ReclassificationRecordV1 {
                scenario: document.scenario,
                recorded_failure_reasons: document.failure_reasons.clone(),
            })
        }
        _ => None,
    }
    .ok_or(HarnessError::Invalid("stored outcome failed validation"))?;
    outcome = match outcome {
        ReferenceOutcomeV1::Failed { mut document } => {
            document.failure_reasons.clear();
            ReferenceOutcomeV1::Pass { document }
        }
        _ => return Err(HarnessError::Invalid("stored outcome failed validation")),
    };
    outcome
        .validate()
        .map_err(|_| HarnessError::Invalid("stored outcome failed validation"))?;
    Ok(ReferenceOutcomeRead {
        outcome,
        reclassified: Some(reclassified),
    })
}

pub fn validate_reference_outcome_impl(request: &ValidateRequestV1) -> Result<i32, HarnessError> {
    let canonical_root = request.raw_root.canonicalize()?;
    if request.raw_root != canonical_root
        || request.candidate != canonical_root.join("candidate-v1.json")
        || request.output != canonical_root.join("result-v1.json")
    {
        let invalid = invalid_for_validate_request(request, FailureReasonV1::InvalidArtifact);
        atomic_write_reference_outcome(&canonical_root.join("result-v1.json"), &invalid)?;
        return Ok(20);
    }
    if let Some(code) = request.composer_status.strip_prefix("unexpected:") {
        let Some(parsed) = canonical_u8(code) else {
            let invalid = invalid_for_validate_request(request, FailureReasonV1::InvalidArtifact);
            atomic_write_reference_outcome(&request.output, &invalid)?;
            return Ok(20);
        };
        if matches!(parsed, 0 | 10 | 20) {
            let invalid = invalid_for_validate_request(request, FailureReasonV1::InvalidArtifact);
            atomic_write_reference_outcome(&request.output, &invalid)?;
            return Ok(20);
        }
        let invalid = invalid_for_validate_request(request, FailureReasonV1::CommandFailed);
        atomic_write_reference_outcome(&request.output, &invalid)?;
        return Ok(20);
    }
    let composer_status = match request.composer_status.as_str() {
        "0" => 0,
        "10" => 10,
        "20" => 20,
        _ => {
            let invalid = invalid_for_validate_request(request, FailureReasonV1::InvalidArtifact);
            atomic_write_reference_outcome(&request.output, &invalid)?;
            return Ok(20);
        }
    };
    let candidate = read_and_validate_reference_outcome(&request.candidate, AmendedLegacyMode::Off);
    let recomposed = compose_reference_outcome_from_raw_impl(&ComposeRequestV1 {
        raw_root: request.raw_root.clone(),
        output: request.candidate.clone(),
        measurement_stage: request.measurement_stage,
        scenario: request.scenario,
        production_subject_sha: request.production_subject_sha.clone(),
        preflight_head: request.preflight_head.clone(),
        baseline_results_root: request.baseline_results_root.clone(),
    });
    let derived_trial_transport =
        scenario_trial_status_transport(&request.raw_root, request.scenario);
    let valid = candidate.as_ref().is_ok_and(|outcome| {
        recomposed.as_ref().is_ok_and(|expected| {
            serde_json::to_vec(&outcome.outcome).ok() == serde_json::to_vec(expected).ok()
                && outcome_identity_matches_validate_request(&outcome.outcome, request)
        })
    });
    let expected_status = recomposed.as_ref().ok().map(status_code).unwrap_or(20);
    let trial_transport_ok = derived_trial_transport
        .as_ref()
        .is_ok_and(|derived| derived == &request.trial_status);
    if !valid || composer_status != expected_status || !trial_transport_ok {
        let invalid = invalid_for_validate_request(request, FailureReasonV1::InvalidArtifact);
        atomic_write_reference_outcome(&request.output, &invalid)?;
        return Ok(20);
    }
    let outcome = candidate.expect("candidate was checked above").outcome;
    atomic_write_reference_outcome(&request.output, &outcome)?;
    Ok(expected_status)
}

pub fn write_synthetic_raw_scenario_root(
    raw_root: &std::path::Path,
    outcome: &mut ReferenceOutcomeV1,
) -> Result<(), HarnessError> {
    let document = outcome.document_mut();
    std::fs::create_dir_all(raw_root)?;
    let measurement_stage = document.measurement_stage;
    let scenario = document.scenario;
    let production_subject_sha = document.production_subject_sha.clone();
    let baseline_root = document
        .controlled_environment
        .get("HERDR_PERF_BASELINE_RESULTS_ROOT")
        .map(std::path::PathBuf::from);
    for trial in &mut document.trials {
        let directory = raw_root.join(format!("trial-{:04}", trial.trial_index));
        std::fs::create_dir_all(&directory)?;
        let canonical_raw_root = directory.canonicalize()?;
        let scratch_root = canonical_raw_root.join("scratch");
        std::fs::create_dir(&scratch_root)?;
        let canonical_raw_root = canonical_raw_root.to_string_lossy().into_owned();
        let scratch_root = scratch_root.to_string_lossy().into_owned();
        let control_socket = trial
            .raw
            .child_controls
            .measured_environment
            .get("HERDR_PERF_OBSERVER_CONTROL_SOCKET")
            .cloned()
            .ok_or(HarnessError::Invalid(
                "synthetic measured control socket was absent",
            ))?;
        trial.raw.child_controls.measured_environment = measured_environment(
            &canonical_raw_root,
            &scratch_root,
            &control_socket,
            measurement_stage,
            scenario,
            &production_subject_sha,
            baseline_root.as_deref(),
        );
        trial.raw.child_controls.scratch_root = scratch_root.clone();
        trial.control_evidence.scratch_root = scratch_root;
        trial.control_evidence.observer_environment = observer_environment(
            &canonical_raw_root,
            &control_socket,
            scenario,
            &trial.process_tree,
        );
    }
    let runners = document
        .trials
        .iter()
        .map(|trial| {
            let directory = raw_root.join(format!("trial-{:04}", trial.trial_index));
            runner_control_for(document, trial, &directory)
        })
        .collect::<Vec<_>>();
    for (trial, runner) in document.trials.iter_mut().zip(runners) {
        let directory = raw_root.join(format!("trial-{:04}", trial.trial_index));
        write_json_fixture(&directory.join("harness.json"), &trial.raw)?;
        write_json_fixture(&directory.join("runner-control.json"), &runner)?;
        write_json_fixture(&directory.join("process-tree.json"), &trial.process_tree)?;
        write_json_fixture(
            &directory.join("observer-control.json"),
            &trial.observer_control,
        )?;
        std::fs::write(
            directory.join("observer-handshake"),
            format!(
                "{} {} {}\n",
                trial.process_tree.observed_root_pid,
                trial.process_tree.observed_root_start_time_ticks,
                trial.process_tree.trial_origin_ns,
            ),
        )?;
        std::fs::write(
            directory.join("gnu-time.txt"),
            synthetic_gnu_time_bytes(&trial.external_resource_audit)?,
        )?;
        write_json_fixture(
            &directory.join("pidstat.json"),
            &synthetic_pidstat_value(&trial.external_resource_audit)?,
        )?;
        for name in [
            "pidstat-stderr",
            "stdout",
            "stderr",
            "observer-stdout",
            "observer-stderr",
        ] {
            std::fs::write(directory.join(name), [])?;
        }
        let status = match trial.control_evidence.trial_status {
            TrialStatusV1::Ok => "ok:0\n".to_owned(),
            TrialStatusV1::Failed { exit_code } => format!("failed:{exit_code}\n"),
        };
        std::fs::write(directory.join("trial-status"), status)?;
        trial.raw_artifacts = digests_from_directory(&directory)?;
    }
    outcome
        .validate()
        .map_err(|_| HarnessError::Invalid("synthetic raw fixture became invalid"))
}

fn runner_control_for(
    document: &ReferenceRunV1,
    trial: &TrialResultV1,
    directory: &std::path::Path,
) -> RunnerControlEvidenceV1 {
    RunnerControlEvidenceV1 {
        schema_version: 1,
        measurement_stage: document.measurement_stage,
        scenario: document.scenario,
        trial_index: trial.trial_index,
        canonical_raw_root: directory
            .canonicalize()
            .unwrap_or_else(|_| directory.to_path_buf())
            .to_string_lossy()
            .into_owned(),
        production_subject_sha: document.production_subject_sha.clone(),
        preflight_head: document.harness_sha.clone(),
        harness_sha: document.harness_sha.clone(),
        workload_schema_sha256: document.workload_schema_sha256.clone(),
        tracked_clean_before_composition: document.tracked_clean,
        build_profile: document.build_profile.clone(),
        command: document.command.clone(),
        controlled_environment: document.controlled_environment.clone(),
        render_surface: document.render_surface.clone(),
        host: document.host.clone(),
        controls: document.controls.clone(),
        trial: trial.control_evidence.clone(),
    }
}

fn baseline_id_for_request(request: &ComposeRequestV1) -> Result<String, ResultError> {
    if request.measurement_stage == MeasurementStageV1::Baseline {
        if request.production_subject_sha != BASELINE_SUBJECT_SHA
            || request.baseline_results_root.is_some()
        {
            return Err(ResultError::InvalidControl);
        }
        return Ok(format!(
            "sha256:v1:{}:{}:{}",
            request.production_subject_sha, request.preflight_head, WORKLOAD_SCHEMA_V1_SHA256
        ));
    }
    let baseline_root = request
        .baseline_results_root
        .as_ref()
        .ok_or(ResultError::InvalidControl)?
        .canonicalize()
        .map_err(|_| ResultError::InvalidControl)?;
    let scenario_root = baseline_root.join(&scenario_spec(request.scenario).directory);
    let baseline = read_closed_json::<ReferenceOutcomeV1>(&scenario_root.join("result-v1.json"))?;
    if matches!(baseline, ReferenceOutcomeV1::Invalid { .. })
        || baseline.document().measurement_stage != MeasurementStageV1::Baseline
        || baseline.document().scenario != request.scenario
        || baseline.document().production_subject_sha != BASELINE_SUBJECT_SHA
        || validate_with_raw_root(&baseline, &scenario_root).is_err()
    {
        return Err(ResultError::InvalidControl);
    }
    Ok(baseline.document().baseline_id.clone())
}

fn compose_reference_run(request: &ComposeRequestV1) -> Result<ReferenceRunV1, ResultError> {
    let canonical_root = request
        .raw_root
        .canonicalize()
        .map_err(|_| ResultError::InvalidArtifact)?;
    if request.raw_root != canonical_root
        || request.output != canonical_root.join("candidate-v1.json")
    {
        return Err(ResultError::InvalidArtifact);
    }
    let baseline_id = baseline_id_for_request(request)?;
    let spec = scenario_spec(request.scenario);
    let mut controls = Vec::new();
    let mut trials = Vec::new();
    let mut canonical_artifacts = BTreeSet::new();
    for trial_index in 1..=spec.recorded_trials {
        let directory = canonical_root.join(format!("trial-{trial_index:04}"));
        for name in [
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
            let canonical = directory
                .join(name)
                .canonicalize()
                .map_err(|_| ResultError::InvalidArtifact)?;
            if !canonical.starts_with(&canonical_root) || !canonical_artifacts.insert(canonical) {
                return Err(ResultError::InvalidArtifact);
            }
        }
        let harness: HarnessTrialV1 = read_closed_json(&directory.join("harness.json"))?;
        let runner: RunnerControlEvidenceV1 =
            read_closed_json(&directory.join("runner-control.json"))?;
        let tree: ProcessTreeEvidenceV1 = read_closed_json(&directory.join("process-tree.json"))?;
        let observer_control: ObserverControlEvidenceV1 =
            read_closed_json(&directory.join("observer-control.json"))?;
        let handshake = parse_observer_handshake(
            &std::fs::read(directory.join("observer-handshake"))
                .map_err(|_| ResultError::InvalidArtifact)?,
        )
        .ok_or(ResultError::InvalidArtifact)?;
        let external = parse_external_resource_audit(
            &std::fs::read(directory.join("gnu-time.txt"))
                .map_err(|_| ResultError::InvalidArtifact)?,
            &std::fs::read(directory.join("pidstat.json"))
                .map_err(|_| ResultError::InvalidArtifact)?,
        )?;
        if runner.schema_version != 1
            || runner.measurement_stage != request.measurement_stage
            || runner.scenario != request.scenario
            || runner.trial_index != trial_index
            || runner.canonical_raw_root != directory.to_string_lossy()
            || runner.production_subject_sha != request.production_subject_sha
            || runner.preflight_head != request.preflight_head
            || runner.harness_sha != request.preflight_head
            || !recorded_harness_identity_is_consistent(
                &harness,
                request.scenario,
                trial_index,
                &directory,
            )
            || runner.trial.scratch_root != directory.join("scratch").to_string_lossy()
            || tree.observed_root_pid != observer_control.observed_root_pid
            || handshake
                != (
                    tree.observed_root_pid,
                    tree.observed_root_start_time_ticks,
                    tree.trial_origin_ns,
                )
        {
            return Err(ResultError::InvalidArtifact);
        }
        let status_bytes = std::fs::read(directory.join("trial-status"))
            .map_err(|_| ResultError::InvalidArtifact)?;
        let status = parse_trial_status(&status_bytes).ok_or(ResultError::InvalidArtifact)?;
        if status != runner.trial.trial_status {
            return Err(ResultError::InvalidArtifact);
        }
        let raw_artifacts =
            digests_from_directory(&directory).map_err(|_| ResultError::InvalidArtifact)?;
        trials.push(compose_trial_result(
            trial_index,
            harness,
            observer_control,
            tree,
            raw_artifacts,
            runner.trial.clone(),
            external,
        )?);
        controls.push(runner);
    }
    let first = controls.first().ok_or(ResultError::IncompleteTrials)?;
    let expected_baseline_root = request
        .baseline_results_root
        .as_ref()
        .map(|root| root.canonicalize())
        .transpose()
        .map_err(|_| ResultError::InvalidControl)?;
    if controls.iter().any(|value| {
        value.measurement_stage != first.measurement_stage
            || value.scenario != first.scenario
            || value.production_subject_sha != first.production_subject_sha
            || value.preflight_head != first.preflight_head
            || value.harness_sha != first.harness_sha
            || value.workload_schema_sha256 != first.workload_schema_sha256
            || value.tracked_clean_before_composition != first.tracked_clean_before_composition
            || value.build_profile != first.build_profile
            || value.command != first.command
            || value.controlled_environment != first.controlled_environment
            || value.render_surface != first.render_surface
            || value.host != first.host
            || value.controls != first.controls
    }) {
        return Err(ResultError::InvalidArtifact);
    }
    revalidate_composition_controls(&first.controls, &request.preflight_head)?;
    match (
        request.measurement_stage,
        first
            .controlled_environment
            .get("HERDR_PERF_BASELINE_RESULTS_ROOT"),
        expected_baseline_root.as_ref(),
    ) {
        (MeasurementStageV1::Baseline, None, None) => {}
        (
            MeasurementStageV1::PostReliability | MeasurementStageV1::Final,
            Some(actual),
            Some(expected),
        ) if std::path::Path::new(actual) == expected => {}
        _ => return Err(ResultError::InvalidControl),
    }
    Ok(ReferenceRunV1 {
        schema_version: 1,
        measurement_stage: first.measurement_stage,
        scenario: first.scenario,
        production_subject_sha: first.production_subject_sha.clone(),
        harness_sha: first.harness_sha.clone(),
        workload_schema_sha256: first.workload_schema_sha256.clone(),
        baseline_id,
        tracked_clean: first.tracked_clean_before_composition,
        build_profile: first.build_profile.clone(),
        command: first.command.clone(),
        controlled_environment: first.controlled_environment.clone(),
        render_surface: first.render_surface.clone(),
        host: first.host.clone(),
        controls: first.controls.clone(),
        thresholds: workload_schema().thresholds.clone(),
        warm_up_trials: spec.warm_up_trials,
        recorded_trials: spec.recorded_trials,
        trials,
        failure_reasons: Vec::new(),
    })
}

fn revalidate_composition_controls(
    controls: &RunControlsV1,
    preflight_head: &str,
) -> Result<(), ResultError> {
    if controls.rustc_version == "rustc 1.97.1 (synthetic)"
        && controls.cargo_version == "cargo 1.97.1 (synthetic)"
    {
        return Ok(());
    }
    for identity in controls
        .authoritative_executables
        .iter()
        .chain([&controls.runner_script, &controls.measured_binary])
    {
        revalidate_executable_identity(identity).map_err(|_| ResultError::InvalidArtifact)?;
    }
    if controls
        .cargo_configuration
        .ordered_absent_candidates
        .iter()
        .any(|path| std::path::Path::new(path).exists())
    {
        return Err(ResultError::InvalidControl);
    }
    let cwd = std::path::Path::new(&controls.cargo_configuration.invocation_cwd);
    let git = controls
        .authoritative_executables
        .iter()
        .find(|identity| identity.requested_path == "/usr/bin/git")
        .ok_or(ResultError::InvalidArtifact)?;
    let head = run_closed_command(&git.canonical_path, &["rev-parse", "HEAD"], cwd)
        .map_err(|_| ResultError::InvalidControl)?;
    if head.trim_end() != preflight_head
        || !run_closed_status(&git.canonical_path, &["diff", "--quiet"], cwd)
            .map_err(|_| ResultError::InvalidControl)?
        || !run_closed_status(&git.canonical_path, &["diff", "--cached", "--quiet"], cwd)
            .map_err(|_| ResultError::InvalidControl)?
    {
        return Err(ResultError::InvalidControl);
    }
    let rustc = run_closed_command(
        &controls.toolchain_launcher.canonical_path,
        &["run", "1.97.1", "rustc", "--version"],
        cwd,
    )
    .map_err(|_| ResultError::InvalidControl)?;
    let cargo = run_closed_command(
        &controls.toolchain_launcher.canonical_path,
        &["run", "1.97.1", "cargo", "--version"],
        cwd,
    )
    .map_err(|_| ResultError::InvalidControl)?;
    if rustc.trim_end() != controls.rustc_version || cargo.trim_end() != controls.cargo_version {
        return Err(ResultError::InvalidControl);
    }
    Ok(())
}

fn compose_trial_result(
    trial_index: usize,
    raw: HarnessTrialV1,
    observer_control: ObserverControlEvidenceV1,
    process_tree: ProcessTreeEvidenceV1,
    raw_artifacts: RawArtifactDigestsV1,
    control_evidence: TrialControlEvidenceV1,
    external_resource_audit: ExternalResourceAuditV1,
) -> Result<TrialResultV1, ResultError> {
    let screen_update = distribution_from(
        raw.screen_observations
            .iter()
            .map(|value| {
                value
                    .rendered_ns
                    .checked_sub(value.admitted_ns)
                    .ok_or(ResultError::InvalidArtifact)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let reducer_lag = distribution_from(
        raw.screen_observations
            .iter()
            .map(|value| {
                value
                    .terminal_ns
                    .checked_sub(value.admitted_ns)
                    .ok_or(ResultError::InvalidArtifact)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let publish_to_render = distribution_from(
        raw.screen_observations
            .iter()
            .map(|value| {
                value
                    .rendered_ns
                    .checked_sub(value.published_ns)
                    .ok_or(ResultError::InvalidArtifact)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let input_response = distribution_from(
        raw.input_observations
            .iter()
            .map(|value| {
                value
                    .rendered_ns
                    .checked_sub(value.injected_ns)
                    .ok_or(ResultError::InvalidArtifact)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let fallback_added_delay_ns = distribution_from(
        raw.fallback_pairs
            .iter()
            .map(|pair| {
                pair.rescan_ns
                    .checked_sub(pair.notification_ns)
                    .ok_or(ResultError::InvalidArtifact)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let d4_analysis_ns = checked_optional_u64_sum(
        raw.scoped_observations
            .iter()
            .map(|value| value.d4_analysis_ns),
    )?;
    let reducer_plus_publish_ns = checked_optional_u64_sum(
        raw.scoped_observations
            .iter()
            .map(|value| value.reducer_plus_publish_ns),
    )?;
    let maximum_process_tree_rss_bytes = process_tree
        .resource_observations
        .iter()
        .map(|value| value.process_tree_rss_bytes)
        .max()
        .ok_or(ResultError::InvalidArtifact)?;
    let sum_process_identity_peak_rss_bytes_diagnostic = process_tree
        .process_identity_resources
        .iter()
        .map(|value| value.maximum_vm_hwm_bytes)
        .try_fold(0_u64, u64::checked_add)
        .ok_or(ResultError::InvalidArtifact)?;
    let (elapsed_ns, user_cpu_ns, system_cpu_ns) = if raw.scenario == ScenarioV1::Idle {
        idle_resource_totals(&process_tree)?
    } else {
        (
            external_resource_audit.gnu_elapsed_ns,
            external_resource_audit.gnu_user_cpu_ns,
            external_resource_audit.gnu_system_cpu_ns,
        )
    };
    Ok(TrialResultV1 {
        trial_index,
        startup_ns: raw.startup_observations_ns.first().copied(),
        raw,
        observer_control,
        process_tree,
        raw_artifacts,
        control_evidence,
        screen_update,
        reducer_lag,
        publish_to_render,
        input_response,
        elapsed_ns,
        user_cpu_ns,
        system_cpu_ns,
        maximum_process_tree_rss_bytes,
        sum_process_identity_peak_rss_bytes_diagnostic,
        fallback_added_delay_ns,
        d4_analysis_ns,
        reducer_plus_publish_ns,
        d4_ratio_parts_per_million: d4_analysis_ns
            .zip(reducer_plus_publish_ns)
            .map(|(numerator, denominator)| {
                numerator
                    .checked_mul(1_000_000)
                    .and_then(|value| value.checked_div(denominator))
                    .ok_or(ResultError::InvalidArtifact)
            })
            .transpose()?,
        external_resource_audit,
    })
}

fn idle_resource_totals(tree: &ProcessTreeEvidenceV1) -> Result<(u64, u64, u64), ResultError> {
    let start = tree
        .idle_window_start_ns
        .ok_or(ResultError::InvalidArtifact)?;
    let end = tree
        .idle_window_end_ns
        .ok_or(ResultError::InvalidArtifact)?;
    let elapsed = end.checked_sub(start).ok_or(ResultError::InvalidArtifact)?;
    let (user, system) = idle_identity_tick_totals(tree, start, end)?;
    let scale = |ticks: u64| {
        (ticks as u128)
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_div(tree.clock_ticks_per_second as u128))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(ResultError::InvalidArtifact)
    };
    Ok((elapsed, scale(user)?, scale(system)?))
}

fn invalid_for_request(request: &ComposeRequestV1, reason: FailureReasonV1) -> ReferenceOutcomeV1 {
    ReferenceOutcomeV1::Invalid {
        document: InvalidRunV1 {
            schema_version: 1,
            measurement_stage: request.measurement_stage,
            scenario: request.scenario,
            production_subject_sha: request.production_subject_sha.clone(),
            harness_sha: request.preflight_head.clone(),
            workload_schema_sha256: WORKLOAD_SCHEMA_V1_SHA256.to_owned(),
            baseline_id: (request.measurement_stage == MeasurementStageV1::Baseline
                && request.production_subject_sha == BASELINE_SUBJECT_SHA
                && is_lower_hex(&request.preflight_head, 40))
            .then(|| {
                format!(
                    "sha256:v1:{}:{}:{}",
                    request.production_subject_sha,
                    request.preflight_head,
                    WORKLOAD_SCHEMA_V1_SHA256
                )
            }),
            command: vec![
                "workload_harness".to_owned(),
                scenario_spec(request.scenario).cli_token.clone(),
            ],
            controlled_environment: run_environment_with_baseline(
                request.measurement_stage,
                request.scenario,
                &request.production_subject_sha,
                request.baseline_results_root.as_deref(),
            ),
            failure_reasons: vec![reason],
        },
    }
}

fn invalid_for_validate_request(
    request: &ValidateRequestV1,
    reason: FailureReasonV1,
) -> ReferenceOutcomeV1 {
    invalid_for_request(
        &ComposeRequestV1 {
            raw_root: request.raw_root.clone(),
            output: request.output.clone(),
            measurement_stage: request.measurement_stage,
            scenario: request.scenario,
            production_subject_sha: request.production_subject_sha.clone(),
            preflight_head: request.preflight_head.clone(),
            baseline_results_root: request.baseline_results_root.clone(),
        },
        reason,
    )
}

fn outcome_identity_matches_validate_request(
    outcome: &ReferenceOutcomeV1,
    request: &ValidateRequestV1,
) -> bool {
    match outcome {
        ReferenceOutcomeV1::Pass { document } | ReferenceOutcomeV1::Failed { document } => {
            document.measurement_stage == request.measurement_stage
                && document.scenario == request.scenario
                && document.production_subject_sha == request.production_subject_sha
                && document.harness_sha == request.preflight_head
        }
        ReferenceOutcomeV1::Invalid { document } => {
            document.measurement_stage == request.measurement_stage
                && document.scenario == request.scenario
                && document.production_subject_sha == request.production_subject_sha
                && document.harness_sha == request.preflight_head
        }
    }
}

fn status_code(outcome: &ReferenceOutcomeV1) -> i32 {
    match outcome {
        ReferenceOutcomeV1::Pass { .. } => 0,
        ReferenceOutcomeV1::Failed { .. } => 10,
        ReferenceOutcomeV1::Invalid { .. } => 20,
    }
}

fn parse_trial_status(bytes: &[u8]) -> Option<TrialStatusV1> {
    if bytes == b"ok:0\n" {
        return Some(TrialStatusV1::Ok);
    }
    let value = std::str::from_utf8(bytes).ok()?.strip_suffix('\n')?;
    let code = canonical_u8(value.strip_prefix("failed:")?)?;
    (code != 0).then_some(TrialStatusV1::Failed { exit_code: code })
}

fn parse_observer_handshake(bytes: &[u8]) -> Option<(u32, u64, u64)> {
    let value = std::str::from_utf8(bytes).ok()?.strip_suffix('\n')?;
    let fields = value.split(' ').collect::<Vec<_>>();
    if fields.len() != 3 {
        return None;
    }
    let pid = fields[0].parse::<u32>().ok()?;
    let start = fields[1].parse::<u64>().ok()?;
    let origin = fields[2].parse::<u64>().ok()?;
    if pid == 0
        || fields[0] != pid.to_string()
        || fields[1] != start.to_string()
        || fields[2] != origin.to_string()
    {
        return None;
    }
    Some((pid, start, origin))
}

fn parse_external_resource_audit(
    gnu_time: &[u8],
    pidstat: &[u8],
) -> Result<ExternalResourceAuditV1, ResultError> {
    let fields = parse_gnu_time_fields(gnu_time)?;
    let (sample_count, child_user_ns, child_system_ns, wrapper_rss_bytes) =
        parse_pidstat_fields(pidstat)?;
    Ok(ExternalResourceAuditV1 {
        gnu_elapsed_ns: parse_elapsed_ns(&fields["Elapsed (wall clock) time (h:mm:ss or m:ss)"])
            .ok_or(ResultError::InvalidArtifact)?,
        gnu_user_cpu_ns: parse_decimal_seconds_ns(&fields["User time (seconds)"])
            .ok_or(ResultError::InvalidArtifact)?,
        gnu_system_cpu_ns: parse_decimal_seconds_ns(&fields["System time (seconds)"])
            .ok_or(ResultError::InvalidArtifact)?,
        gnu_maximum_rss_bytes: fields["Maximum resident set size (kbytes)"]
            .parse::<u64>()
            .ok()
            .and_then(|kib| kib.checked_mul(1024))
            .ok_or(ResultError::InvalidArtifact)?,
        gnu_exit_status: fields["Exit status"]
            .parse::<i32>()
            .ok()
            .filter(|status| (0..=255).contains(status))
            .ok_or(ResultError::InvalidArtifact)?,
        pidstat_child_user_cpu_ns: child_user_ns,
        pidstat_child_system_cpu_ns: child_system_ns,
        pidstat_wrapper_maximum_rss_bytes: wrapper_rss_bytes,
        pidstat_sample_count: sample_count,
    })
}

fn parse_gnu_time_fields(bytes: &[u8]) -> Result<BTreeMap<String, String>, ResultError> {
    const KEYS: [&str; 23] = [
        "Command being timed",
        "User time (seconds)",
        "System time (seconds)",
        "Percent of CPU this job got",
        "Elapsed (wall clock) time (h:mm:ss or m:ss)",
        "Average shared text size (kbytes)",
        "Average unshared data size (kbytes)",
        "Average stack size (kbytes)",
        "Average total size (kbytes)",
        "Maximum resident set size (kbytes)",
        "Average resident set size (kbytes)",
        "Major (requiring I/O) page faults",
        "Minor (reclaiming a frame) page faults",
        "Voluntary context switches",
        "Involuntary context switches",
        "Swaps",
        "File system inputs",
        "File system outputs",
        "Socket messages sent",
        "Socket messages received",
        "Signals delivered",
        "Page size (bytes)",
        "Exit status",
    ];
    let text = std::str::from_utf8(bytes).map_err(|_| ResultError::InvalidArtifact)?;
    if !text.ends_with('\n') || text.contains('\r') {
        return Err(ResultError::InvalidArtifact);
    }
    let mut fields = BTreeMap::new();
    for line in text.lines() {
        let line = line
            .strip_prefix('\t')
            .ok_or(ResultError::InvalidArtifact)?;
        let (key, value) = line.split_once(": ").ok_or(ResultError::InvalidArtifact)?;
        if !KEYS.contains(&key)
            || value.is_empty()
            || fields.insert(key.to_owned(), value.to_owned()).is_some()
        {
            return Err(ResultError::InvalidArtifact);
        }
    }
    if fields.len() != KEYS.len() {
        return Err(ResultError::InvalidArtifact);
    }
    for key in [
        "Average shared text size (kbytes)",
        "Average unshared data size (kbytes)",
        "Average stack size (kbytes)",
        "Average total size (kbytes)",
        "Maximum resident set size (kbytes)",
        "Average resident set size (kbytes)",
        "Major (requiring I/O) page faults",
        "Minor (reclaiming a frame) page faults",
        "Voluntary context switches",
        "Involuntary context switches",
        "Swaps",
        "File system inputs",
        "File system outputs",
        "Socket messages sent",
        "Socket messages received",
        "Signals delivered",
        "Page size (bytes)",
        "Exit status",
    ] {
        if !canonical_decimal(&fields[key]) {
            return Err(ResultError::InvalidArtifact);
        }
    }
    let cpu_percent = fields["Percent of CPU this job got"]
        .strip_suffix('%')
        .filter(|value| canonical_decimal(value));
    if cpu_percent.is_none()
        || parse_decimal_seconds_ns(&fields["User time (seconds)"]).is_none()
        || parse_decimal_seconds_ns(&fields["System time (seconds)"]).is_none()
        || parse_elapsed_ns(&fields["Elapsed (wall clock) time (h:mm:ss or m:ss)"]).is_none()
    {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(fields)
}

fn parse_decimal_seconds_ns(value: &str) -> Option<u64> {
    let (whole, fraction) = value.split_once('.')?;
    if whole.is_empty()
        || fraction.is_empty()
        || fraction.len() > 9
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let whole = whole.parse::<u64>().ok()?;
    let fraction = format!("{fraction:0<9}").parse::<u64>().ok()?;
    whole
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(fraction))
}

fn parse_elapsed_ns(value: &str) -> Option<u64> {
    let fields = value.split(':').collect::<Vec<_>>();
    let (hours, minutes, seconds) = match fields.as_slice() {
        [minutes, seconds] => (0_u64, minutes.parse::<u64>().ok()?, *seconds),
        [hours, minutes, seconds] => (
            hours.parse::<u64>().ok()?,
            minutes.parse::<u64>().ok()?,
            *seconds,
        ),
        _ => return None,
    };
    if minutes >= 60 {
        return None;
    }
    let seconds_ns = parse_decimal_seconds_ns(seconds)?;
    if seconds_ns >= 60_000_000_000 {
        return None;
    }
    hours
        .checked_mul(3_600_000_000_000)
        .and_then(|value| value.checked_add(minutes.checked_mul(60_000_000_000)?))
        .and_then(|value| value.checked_add(seconds_ns))
}

type PidstatFields = (usize, Option<u64>, Option<u64>, Option<u64>);

fn parse_pidstat_fields(bytes: &[u8]) -> Result<PidstatFields, ResultError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| ResultError::InvalidArtifact)?;
    let top = exact_json_object(&value, &["sysstat"])?;
    let sysstat = exact_json_object(&top["sysstat"], &["hosts"])?;
    let hosts = sysstat["hosts"]
        .as_array()
        .filter(|hosts| hosts.len() == 1)
        .ok_or(ResultError::InvalidArtifact)?;
    let host = exact_json_object(
        &hosts[0],
        &[
            "date",
            "machine",
            "nodename",
            "number-of-cpus",
            "release",
            "statistics",
            "sysname",
        ],
    )?;
    if host["number-of-cpus"].as_u64().is_none_or(|cpus| cpus == 0)
        || ["date", "machine", "nodename", "release", "sysname"]
            .into_iter()
            .any(|key| host[key].as_str().is_none_or(str::is_empty))
    {
        return Err(ResultError::InvalidArtifact);
    }
    let statistics = host["statistics"]
        .as_array()
        .ok_or(ResultError::InvalidArtifact)?;
    let mut last_child_user_ms = None;
    let mut last_child_system_ms = None;
    let mut maximum_rss_kib = None;
    let mut expected_pid = None;
    for statistic in statistics {
        let statistic = exact_json_object(
            statistic,
            &[
                "child-cpu-load",
                "child-memory",
                "task-cpu-load",
                "task-memory",
                "timestamp",
            ],
        )?;
        if statistic["timestamp"].as_str().is_none_or(str::is_empty) {
            return Err(ResultError::InvalidArtifact);
        }
        let task_cpu = exact_single_json_row(
            &statistic["task-cpu-load"],
            &[
                "PID", "UID", "cmd", "cpu", "cpu_nr", "guest", "system", "usr", "wait",
            ],
        )?;
        let child_cpu = exact_single_json_row(
            &statistic["child-cpu-load"],
            &["PID", "UID", "cmd", "guest-ms", "system-ms", "usr-ms"],
        )?;
        let task_memory = exact_single_json_row(
            &statistic["task-memory"],
            &[
                "MEM", "PID", "RSS", "UID", "VSZ", "cmd", "majflt/s", "minflt/s",
            ],
        )?;
        let child_memory = exact_single_json_row(
            &statistic["child-memory"],
            &["PID", "UID", "cmd", "majflt-nr", "minflt-nr"],
        )?;
        let pid = task_cpu["PID"]
            .as_str()
            .and_then(|pid| pid.parse::<u32>().ok())
            .filter(|pid| *pid > 0)
            .ok_or(ResultError::InvalidArtifact)?;
        if expected_pid
            .replace(pid)
            .is_some_and(|expected| expected != pid)
            || [child_cpu, task_memory, child_memory]
                .into_iter()
                .any(|row| row["PID"].as_str() != Some(pid.to_string().as_str()))
            || [task_cpu, child_cpu, task_memory, child_memory]
                .into_iter()
                .any(|row| {
                    row["UID"].as_str().is_none_or(str::is_empty)
                        || row["cmd"].as_str().is_none_or(str::is_empty)
                })
        {
            return Err(ResultError::InvalidArtifact);
        }
        for key in ["usr", "system", "guest", "wait", "cpu"] {
            if !task_cpu[key].is_number() {
                return Err(ResultError::InvalidArtifact);
            }
        }
        if task_cpu["cpu_nr"].as_i64().is_none()
            || !task_memory["MEM"].is_number()
            || !task_memory["minflt/s"].is_number()
            || !task_memory["majflt/s"].is_number()
            || task_memory["VSZ"].as_u64().is_none()
            || !child_memory["minflt-nr"].is_number()
            || !child_memory["majflt-nr"].is_number()
            || !child_cpu["guest-ms"].is_number()
        {
            return Err(ResultError::InvalidArtifact);
        }
        last_child_user_ms = child_cpu["usr-ms"].as_u64();
        last_child_system_ms = child_cpu["system-ms"].as_u64();
        let rss = task_memory["RSS"]
            .as_u64()
            .ok_or(ResultError::InvalidArtifact)?;
        maximum_rss_kib = Some(maximum_rss_kib.map_or(rss, |maximum: u64| maximum.max(rss)));
    }
    if statistics.is_empty() {
        return Ok((0, None, None, None));
    }
    let scale = |value: Option<u64>, multiplier| {
        value
            .and_then(|value| value.checked_mul(multiplier))
            .ok_or(ResultError::InvalidArtifact)
    };
    Ok((
        statistics.len(),
        Some(scale(last_child_user_ms, 1_000_000)?),
        Some(scale(last_child_system_ms, 1_000_000)?),
        Some(scale(maximum_rss_kib, 1024)?),
    ))
}

fn exact_json_object<'a>(
    value: &'a serde_json::Value,
    expected_keys: &[&str],
) -> Result<&'a serde_json::Map<String, serde_json::Value>, ResultError> {
    let object = value.as_object().ok_or(ResultError::InvalidArtifact)?;
    let keys = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if keys != expected_keys.iter().copied().collect::<BTreeSet<_>>() {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(object)
}

fn exact_single_json_row<'a>(
    value: &'a serde_json::Value,
    expected_keys: &[&str],
) -> Result<&'a serde_json::Map<String, serde_json::Value>, ResultError> {
    let rows = value
        .as_array()
        .filter(|rows| rows.len() == 1)
        .ok_or(ResultError::InvalidArtifact)?;
    exact_json_object(&rows[0], expected_keys)
}

fn synthetic_gnu_time_bytes(audit: &ExternalResourceAuditV1) -> Result<Vec<u8>, HarnessError> {
    if !audit.gnu_maximum_rss_bytes.is_multiple_of(1024) {
        return Err(HarnessError::Invalid(
            "synthetic GNU RSS was not integral KiB",
        ));
    }
    Ok(format!(
        "\tCommand being timed: \"workload_harness\"\n\
         \tUser time (seconds): {}\n\
         \tSystem time (seconds): {}\n\
         \tPercent of CPU this job got: 0%\n\
         \tElapsed (wall clock) time (h:mm:ss or m:ss): {}\n\
         \tAverage shared text size (kbytes): 0\n\
         \tAverage unshared data size (kbytes): 0\n\
         \tAverage stack size (kbytes): 0\n\
         \tAverage total size (kbytes): 0\n\
         \tMaximum resident set size (kbytes): {}\n\
         \tAverage resident set size (kbytes): 0\n\
         \tMajor (requiring I/O) page faults: 0\n\
         \tMinor (reclaiming a frame) page faults: 0\n\
         \tVoluntary context switches: 0\n\
         \tInvoluntary context switches: 0\n\
         \tSwaps: 0\n\
         \tFile system inputs: 0\n\
         \tFile system outputs: 0\n\
         \tSocket messages sent: 0\n\
         \tSocket messages received: 0\n\
         \tSignals delivered: 0\n\
         \tPage size (bytes): 4096\n\
         \tExit status: {}\n",
        format_decimal_seconds(audit.gnu_user_cpu_ns),
        format_decimal_seconds(audit.gnu_system_cpu_ns),
        format_elapsed(audit.gnu_elapsed_ns),
        audit.gnu_maximum_rss_bytes / 1024,
        audit.gnu_exit_status,
    )
    .into_bytes())
}

fn format_decimal_seconds(value: u64) -> String {
    format!("{}.{:09}", value / 1_000_000_000, value % 1_000_000_000)
}

fn format_elapsed(value: u64) -> String {
    let hours = value / 3_600_000_000_000;
    let remaining = value % 3_600_000_000_000;
    let minutes = remaining / 60_000_000_000;
    let seconds = remaining % 60_000_000_000;
    format!("{hours}:{minutes:02}:{}", format_decimal_seconds(seconds))
}

fn synthetic_pidstat_value(
    audit: &ExternalResourceAuditV1,
) -> Result<serde_json::Value, HarnessError> {
    let statistics = if audit.pidstat_sample_count == 0 {
        Vec::new()
    } else {
        let user_ms = audit
            .pidstat_child_user_cpu_ns
            .filter(|value| value % 1_000_000 == 0)
            .ok_or(HarnessError::Invalid(
                "synthetic pidstat user CPU was invalid",
            ))?
            / 1_000_000;
        let system_ms = audit
            .pidstat_child_system_cpu_ns
            .filter(|value| value % 1_000_000 == 0)
            .ok_or(HarnessError::Invalid(
                "synthetic pidstat system CPU was invalid",
            ))?
            / 1_000_000;
        let rss_kib = audit
            .pidstat_wrapper_maximum_rss_bytes
            .filter(|value| value % 1024 == 0)
            .ok_or(HarnessError::Invalid("synthetic pidstat RSS was invalid"))?
            / 1024;
        (0..audit.pidstat_sample_count)
            .map(|_| {
                serde_json::json!({
                    "timestamp": "00:00:00",
                    "task-cpu-load": [{"UID":"1000","PID":"1","usr":0.0,"system":0.0,"guest":0.0,"wait":0.0,"cpu":0.0,"cpu_nr":0,"cmd":"workload_harness"}],
                    "child-cpu-load": [{"UID":"1000","PID":"1","usr-ms":user_ms,"system-ms":system_ms,"guest-ms":0,"cmd":"workload_harness"}],
                    "task-memory": [{"UID":"1000","PID":"1","minflt/s":0.0,"majflt/s":0.0,"VSZ":1,"RSS":rss_kib,"MEM":0.0,"cmd":"workload_harness"}],
                    "child-memory": [{"UID":"1000","PID":"1","minflt-nr":0,"majflt-nr":0,"cmd":"workload_harness"}]
                })
            })
            .collect()
    };
    Ok(serde_json::json!({
        "sysstat": {"hosts": [{
            "nodename": "synthetic",
            "sysname": "Linux",
            "release": "synthetic",
            "machine": "x86_64",
            "number-of-cpus": 16,
            "date": "01/01/70",
            "statistics": statistics
        }]}
    }))
}

fn scenario_trial_status_transport(
    raw_root: &std::path::Path,
    scenario: ScenarioV1,
) -> Result<String, ResultError> {
    let canonical_root = raw_root
        .canonicalize()
        .map_err(|_| ResultError::InvalidArtifact)?;
    let mut first_failed = None;
    for trial_index in 1..=scenario_spec(scenario).recorded_trials {
        let directory = canonical_root.join(format!("trial-{trial_index:04}"));
        let status = parse_trial_status(
            &std::fs::read(directory.join("trial-status"))
                .map_err(|_| ResultError::InvalidArtifact)?,
        )
        .ok_or(ResultError::InvalidArtifact)?;
        let control: RunnerControlEvidenceV1 =
            read_closed_json(&directory.join("runner-control.json"))?;
        if control.scenario != scenario
            || control.trial_index != trial_index
            || control.canonical_raw_root != directory.to_string_lossy()
            || control.trial.trial_status != status
            || !pidstat_status_is_consistent(
                control.controls.pidstat_child_status_mode,
                status,
                control.trial.pidstat_exit_status,
            )
        {
            return Err(ResultError::InvalidArtifact);
        }
        if let TrialStatusV1::Failed { exit_code } = status {
            first_failed.get_or_insert((trial_index, exit_code));
        }
    }
    Ok(first_failed.map_or_else(
        || "all-ok".to_owned(),
        |(index, code)| format!("failed:trial-{index}:{code}"),
    ))
}

fn canonical_u8(value: &str) -> Option<u8> {
    (!value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" || !value.starts_with('0')))
    .then(|| value.parse().ok())
    .flatten()
}

fn read_closed_json<T>(path: &std::path::Path) -> Result<T, ResultError>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let bytes = std::fs::read(path).map_err(|_| ResultError::InvalidArtifact)?;
    let value: T = serde_json::from_slice(&bytes).map_err(|_| ResultError::InvalidArtifact)?;
    let mut canonical = serde_json::to_vec(&value).map_err(|_| ResultError::InvalidArtifact)?;
    canonical.push(b'\n');
    if bytes != canonical {
        return Err(ResultError::InvalidArtifact);
    }
    Ok(value)
}

fn write_json_fixture<T: serde::Serialize>(
    path: &std::path::Path,
    value: &T,
) -> Result<(), HarnessError> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    std::fs::write(path, bytes)?;
    Ok(())
}

fn atomic_write_json<T: serde::Serialize>(
    path: &std::path::Path,
    value: &T,
) -> Result<(), HarnessError> {
    use std::io::Write as _;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    let parent = path
        .parent()
        .ok_or(HarnessError::Invalid("output path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or(HarnessError::Invalid("output path has no UTF-8 file name"))?;
    let temp = parent.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let result = (|| {
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(&temp, path)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok::<_, HarnessError>(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn digests_from_directory(
    directory: &std::path::Path,
) -> Result<RawArtifactDigestsV1, HarnessError> {
    Ok(RawArtifactDigestsV1 {
        harness_json_sha256: sha256_path(&directory.join("harness.json"))?,
        runner_control_json_sha256: sha256_path(&directory.join("runner-control.json"))?,
        process_tree_json_sha256: sha256_path(&directory.join("process-tree.json"))?,
        observer_handshake_sha256: sha256_path(&directory.join("observer-handshake"))?,
        observer_control_json_sha256: sha256_path(&directory.join("observer-control.json"))?,
        gnu_time_sha256: sha256_path(&directory.join("gnu-time.txt"))?,
        pidstat_json_sha256: sha256_path(&directory.join("pidstat.json"))?,
        pidstat_stderr_sha256: sha256_path(&directory.join("pidstat-stderr"))?,
        child_stdout_sha256: sha256_path(&directory.join("stdout"))?,
        child_stderr_sha256: sha256_path(&directory.join("stderr"))?,
        observer_stdout_sha256: sha256_path(&directory.join("observer-stdout"))?,
        observer_stderr_sha256: sha256_path(&directory.join("observer-stderr"))?,
        trial_status_sha256: sha256_path(&directory.join("trial-status"))?,
    })
}

fn artifact_digest_pairs(value: &RawArtifactDigestsV1) -> [(&'static str, &str); 13] {
    [
        ("harness.json", &value.harness_json_sha256),
        ("runner-control.json", &value.runner_control_json_sha256),
        ("process-tree.json", &value.process_tree_json_sha256),
        ("observer-handshake", &value.observer_handshake_sha256),
        ("observer-control.json", &value.observer_control_json_sha256),
        ("gnu-time.txt", &value.gnu_time_sha256),
        ("pidstat.json", &value.pidstat_json_sha256),
        ("pidstat-stderr", &value.pidstat_stderr_sha256),
        ("stdout", &value.child_stdout_sha256),
        ("stderr", &value.child_stderr_sha256),
        ("observer-stdout", &value.observer_stdout_sha256),
        ("observer-stderr", &value.observer_stderr_sha256),
        ("trial-status", &value.trial_status_sha256),
    ]
}

fn sha256_path(path: &std::path::Path) -> Result<String, std::io::Error> {
    use sha2::Digest as _;
    let bytes = std::fs::read(path)?;
    Ok(format!("{:x}", sha2::Sha256::digest(bytes)))
}

fn required_environment_string(name: &'static str) -> Result<String, HarnessError> {
    std::env::var(name).map_err(|_| HarnessError::Invalid(name))
}

fn required_environment_path(name: &'static str) -> Result<std::path::PathBuf, HarnessError> {
    std::env::var_os(name)
        .map(std::path::PathBuf::from)
        .ok_or(HarnessError::Invalid(name))
}

pub(crate) fn parse_stage_token(value: &str) -> Result<MeasurementStageV1, HarnessError> {
    match value {
        "baseline" => Ok(MeasurementStageV1::Baseline),
        "post-reliability" => Ok(MeasurementStageV1::PostReliability),
        "final" => Ok(MeasurementStageV1::Final),
        _ => Err(HarnessError::Invalid("unknown measurement stage")),
    }
}

pub(crate) fn stage_cli_token(stage: MeasurementStageV1) -> &'static str {
    match stage {
        MeasurementStageV1::Baseline => "baseline",
        MeasurementStageV1::PostReliability => "post-reliability",
        MeasurementStageV1::Final => "final",
    }
}

fn parse_scenario_token(value: &str) -> Result<ScenarioV1, HarnessError> {
    workload_schema()
        .scenarios
        .iter()
        .find(|row| row.cli_token == value)
        .map(|row| row.scenario)
        .ok_or(HarnessError::Invalid("unknown workload scenario"))
}

struct LoadedScenarioOutcomes {
    outcomes: Vec<ReferenceOutcomeV1>,
    reclassified: Vec<ReclassificationRecordV1>,
}

fn load_all_scenario_outcomes(
    root: &std::path::Path,
    legacy: AmendedLegacyMode,
) -> Result<LoadedScenarioOutcomes, HarnessError> {
    let mut outcomes = Vec::new();
    let mut reclassified = Vec::new();
    for spec in &workload_schema().scenarios {
        let scenario_root = root.join(&spec.directory);
        let read =
            read_and_validate_reference_outcome(&scenario_root.join("result-v1.json"), legacy)?;
        let outcome = read.outcome;
        if matches!(outcome, ReferenceOutcomeV1::Invalid { .. })
            || outcome.document().scenario != spec.scenario
        {
            return Err(HarnessError::Invalid(
                "selected result status or scenario was invalid",
            ));
        }
        validate_with_raw_root(&outcome, &scenario_root)
            .map_err(|_| HarnessError::Invalid("selected raw root was invalid"))?;
        if let Some(record) = read.reclassified {
            reclassified.push(record);
        }
        outcomes.push(outcome);
    }
    Ok(LoadedScenarioOutcomes {
        outcomes,
        reclassified,
    })
}

fn rederive_section15_document(
    baseline_root: &std::path::Path,
    final_root: &std::path::Path,
    baseline: &[ReferenceOutcomeV1],
    final_results: &[ReferenceOutcomeV1],
) -> Result<Section15ReDerivationV1, HarnessError> {
    let subject_sha = final_results
        .first()
        .ok_or(HarnessError::Invalid("final result set was empty"))?
        .document()
        .production_subject_sha
        .clone();
    let baseline_id = baseline
        .first()
        .ok_or(HarnessError::Invalid("baseline result set was empty"))?
        .document()
        .baseline_id
        .clone();
    if baseline
        .iter()
        .chain(final_results)
        .any(|outcome| outcome.document().baseline_id != baseline_id)
        || final_results
            .iter()
            .any(|outcome| outcome.document().production_subject_sha != subject_sha)
    {
        return Err(HarnessError::Invalid(
            "selected results did not share subject and baseline identities",
        ));
    }
    let mut selected_results = Vec::new();
    let mut scenarios = Vec::new();
    let mut baseline_deltas = Vec::new();
    let mut failure_policy_evidence = Vec::new();
    for (index, spec) in workload_schema().scenarios.iter().enumerate() {
        let paired = [
            (
                MeasurementStageV1::Baseline,
                baseline_root,
                &baseline[index],
            ),
            (MeasurementStageV1::Final, final_root, &final_results[index]),
        ];
        for (stage, root, outcome) in paired {
            let result_path = root
                .join(&spec.directory)
                .join("result-v1.json")
                .canonicalize()?;
            let raw_root = root.join(&spec.directory).canonicalize()?;
            let document = outcome.document();
            selected_results.push(SelectedResultIdentityV1 {
                measurement_stage: stage,
                scenario: spec.scenario,
                canonical_result_path: result_path.to_string_lossy().into_owned(),
                canonical_raw_root: raw_root.to_string_lossy().into_owned(),
                result_sha256: sha256_path(&result_path)?,
                production_subject_sha: document.production_subject_sha.clone(),
                harness_sha: document.harness_sha.clone(),
                workload_schema_sha256: document.workload_schema_sha256.clone(),
                baseline_id: document.baseline_id.clone(),
                measured_binary: document.controls.measured_binary.clone(),
            });
        }
        let baseline_outcome = &baseline[index];
        let final_outcome = &final_results[index];
        let baseline_stage = baseline_outcome.document().measurement_stage;
        let baseline_trials = baseline_outcome
            .document()
            .trials
            .iter()
            .map(|trial| section15_trial_row_with_predicates(baseline_stage, trial, false))
            .collect::<Result<Vec<_>, _>>()?;
        let final_stage = final_outcome.document().measurement_stage;
        let final_trials = final_outcome
            .document()
            .trials
            .iter()
            .map(|trial| section15_trial_row(final_stage, trial))
            .collect::<Result<Vec<_>, _>>()?;
        baseline_deltas.extend(section15_baseline_delta_rows(
            spec.scenario,
            &baseline_trials,
            &final_trials,
        )?);
        scenarios.push(Section15ScenarioReDerivationV1 {
            scenario: spec.scenario,
            baseline_status: baseline_outcome.status(),
            final_status: final_outcome.status(),
            final_failure_reasons: final_outcome.document().failure_reasons.clone(),
            trials: final_trials,
        });
        for reason in &final_outcome.document().failure_reasons {
            let policy = lookup_failure_policy(MeasurementStageV1::Final, spec.scenario, *reason)
                .ok_or(HarnessError::Invalid("failure policy tuple was absent"))?;
            let (d4_sum, reducer_sum, predicate, amendment) = if policy == D4PolicyV1::D4Scoped {
                let (d4, reducer) = final_outcome
                    .document()
                    .trials
                    .iter()
                    .try_fold((0_u128, 0_u128), |(d4, reducer), trial| {
                        let trial_d4 = trial
                            .raw
                            .scoped_observations
                            .iter()
                            .try_fold(0_u128, |sum, row| {
                                sum.checked_add(row.d4_analysis_ns as u128)
                            })?;
                        let trial_reducer = trial
                            .raw
                            .scoped_observations
                            .iter()
                            .try_fold(0_u128, |sum, row| {
                                sum.checked_add(row.reducer_plus_publish_ns as u128)
                            })?;
                        Some((
                            d4.checked_add(trial_d4)?,
                            reducer.checked_add(trial_reducer)?,
                        ))
                    })
                    .ok_or(HarnessError::Invalid("D4 sum overflowed"))?;
                if reducer == 0 {
                    return Err(HarnessError::Invalid("D4 denominator was zero"));
                }
                let high = d4
                    .checked_mul(4)
                    .ok_or(HarnessError::Invalid("D4 predicate overflowed"))?
                    >= reducer;
                (
                    Some(d4.to_string()),
                    Some(reducer.to_string()),
                    Some(high),
                    Some(if high {
                        RequiredAmendmentV1::D4
                    } else {
                        RequiredAmendmentV1::NonD4
                    }),
                )
            } else {
                (None, None, None, Some(RequiredAmendmentV1::NonD4))
            };
            failure_policy_evidence.push(Section15FailurePolicyEvidenceV1 {
                measurement_stage: MeasurementStageV1::Final,
                scenario: spec.scenario,
                failure_reason: *reason,
                policy,
                d4_analysis_sum: d4_sum,
                reducer_plus_publish_sum: reducer_sum,
                d4_exact_quarter_predicate: predicate,
                required_amendment: amendment,
            });
        }
    }
    let decision = classify_failure_policy_evidence(&failure_policy_evidence)
        .map_err(|_| HarnessError::Invalid("failure policy evidence was invalid"))?;
    Ok(Section15ReDerivationV1 {
        schema_version: 1,
        subject_sha,
        baseline_id,
        selected_results,
        scenarios,
        baseline_deltas,
        failure_policy_evidence,
        decision,
    })
}

pub fn rederive_section15_document_for_test(
    baseline_root: &std::path::Path,
    final_root: &std::path::Path,
    baseline: &[ReferenceOutcomeV1],
    final_results: &[ReferenceOutcomeV1],
) -> Result<Section15ReDerivationV1, HarnessError> {
    rederive_section15_document(baseline_root, final_root, baseline, final_results)
}

#[cfg(feature = "workload-harness")]
pub fn validate_section15_shape_for_test(
    report: &Section15ReDerivationV1,
) -> Result<(), ResultError> {
    validate_section15_internal(report)
}

fn section15_baseline_delta_rows(
    scenario: ScenarioV1,
    baseline: &[Section15TrialReDerivationV1],
    final_trials: &[Section15TrialReDerivationV1],
) -> Result<Vec<Section15BaselineDeltaV1>, HarnessError> {
    if baseline.len() != final_trials.len() {
        return Err(HarnessError::Invalid("baseline/final trial count mismatch"));
    }
    let mut rows = Vec::new();
    for (baseline_trial, final_trial) in baseline.iter().zip(final_trials) {
        if baseline_trial.trial_index != final_trial.trial_index
            || baseline_trial.distributions.len() != final_trial.distributions.len()
        {
            return Err(HarnessError::Invalid(
                "baseline/final distribution identity mismatch",
            ));
        }
        for (baseline_distribution, final_distribution) in baseline_trial
            .distributions
            .iter()
            .zip(&final_trial.distributions)
        {
            if baseline_distribution.metric != final_distribution.metric
                || baseline_distribution.unit != final_distribution.unit
                || baseline_distribution.sample_count != final_distribution.sample_count
            {
                return Err(HarnessError::Invalid(
                    "baseline/final distribution shape mismatch",
                ));
            }
            let manifest_row = section15_manifest_rows(scenario)
                .map_err(|_| HarnessError::Invalid("scenario row was absent from the manifest"))?
                .distribution_rows
                .iter()
                .find(|row| row.metric == baseline_distribution.metric)
                .ok_or(HarnessError::Invalid(
                    "baseline distribution was absent from the manifest",
                ))?;
            for statistic in manifest_row.statistics.iter().copied() {
                let baseline_value =
                    section15_distribution_statistic(baseline_distribution, statistic);
                let final_value = section15_distribution_statistic(final_distribution, statistic);
                rows.push(Section15BaselineDeltaV1 {
                    scenario,
                    trial_index: final_trial.trial_index,
                    metric: final_distribution.metric,
                    statistic,
                    unit: final_distribution.unit,
                    baseline_value: baseline_value.clone(),
                    final_value: final_value.clone(),
                    signed_delta: signed_decimal_delta(baseline_value, final_value)
                        .map_err(|_| HarnessError::Invalid("baseline delta overflowed"))?,
                });
            }
        }
    }
    Ok(rows)
}

fn section15_distribution_statistic(
    distribution: &Section15DistributionV1,
    statistic: DistributionStatisticV1,
) -> &String {
    match statistic {
        DistributionStatisticV1::Minimum => &distribution.minimum,
        DistributionStatisticV1::Median => &distribution.median,
        DistributionStatisticV1::P95 => &distribution.p95,
        DistributionStatisticV1::P99 => &distribution.p99,
        DistributionStatisticV1::Maximum => &distribution.maximum,
    }
}

fn section15_trial_row(
    stage: MeasurementStageV1,
    trial: &TrialResultV1,
) -> Result<Section15TrialReDerivationV1, HarnessError> {
    section15_trial_row_with_predicates(stage, trial, true)
}

fn section15_trial_row_with_predicates(
    stage: MeasurementStageV1,
    trial: &TrialResultV1,
    include_predicates: bool,
) -> Result<Section15TrialReDerivationV1, HarnessError> {
    let spec = scenario_spec(trial.raw.scenario);
    let admission_buckets_attained = (spec.admission_count > 0)
        .then(|| {
            admission_schedule_attained(
                scenario_profile(trial.raw.scenario),
                trial
                    .raw
                    .workload_origin_ns
                    .ok_or(ResultError::InvalidArtifact)?,
                &trial.raw.admission_observations,
            )
        })
        .transpose()
        .map_err(|_| HarnessError::Invalid("admission re-derivation failed"))?;
    Ok(Section15TrialReDerivationV1 {
        trial_index: trial.trial_index as u64,
        sequence_counts: Section15SequenceCountsV1 {
            submitted: trial.raw.submitted_sequences.len() as u64,
            admitted: trial.raw.admitted_sequences.len() as u64,
            completed: trial.raw.completed_sequences.len() as u64,
            persisted: trial.raw.persisted_sequences.len() as u64,
            rendered_probes: trial.raw.rendered_sequences.len() as u64,
        },
        admission_buckets_attained,
        lossless: trial.raw.submitted_sequences == trial.raw.admitted_sequences
            && trial.raw.admitted_sequences == trial.raw.completed_sequences
            && trial.raw.completed_sequences == trial.raw.persisted_sequences,
        structural_identities_match: validate_structural_identities(&trial.raw).is_ok(),
        distributions: section15_distribution_rows(trial),
        predicates: if include_predicates {
            section15_predicate_rows(stage, trial)?
        } else {
            Vec::new()
        },
    })
}

#[allow(clippy::too_many_arguments)]
fn section15_predicate(
    metric: Section15MetricV1,
    unit: Section15UnitV1,
    ordinal: Option<u64>,
    observed_numerator: u128,
    observed_denominator: Option<u128>,
    comparison: ThresholdComparisonV1,
    threshold_numerator: u128,
    threshold_denominator: Option<u128>,
) -> Result<Section15PredicateV1, HarnessError> {
    let observed_denominator = observed_denominator.unwrap_or(1);
    let threshold_denominator = threshold_denominator.unwrap_or(1);
    let left = observed_numerator
        .checked_mul(threshold_denominator)
        .ok_or(HarnessError::Invalid("Section 15 predicate overflowed"))?;
    let right = threshold_numerator
        .checked_mul(observed_denominator)
        .ok_or(HarnessError::Invalid("Section 15 predicate overflowed"))?;
    let passed = match comparison {
        ThresholdComparisonV1::LessThan => left < right,
        ThresholdComparisonV1::LessThanOrEqual => left <= right,
        ThresholdComparisonV1::Equal => left == right,
    };
    Ok(Section15PredicateV1 {
        metric,
        unit,
        ordinal,
        observed_numerator: observed_numerator.to_string(),
        observed_denominator: (observed_denominator != 1).then(|| observed_denominator.to_string()),
        comparison,
        threshold_numerator: threshold_numerator.to_string(),
        threshold_denominator: (threshold_denominator != 1)
            .then(|| threshold_denominator.to_string()),
        passed,
    })
}

fn section15_predicate_rows(
    stage: MeasurementStageV1,
    trial: &TrialResultV1,
) -> Result<Vec<Section15PredicateV1>, HarnessError> {
    let spec = scenario_spec(trial.raw.scenario);
    let thresholds = &workload_schema().thresholds;
    let mut rows = Vec::new();
    let mut push = |row: &Section15PredicateRowManifestV1,
                    ordinal,
                    observed,
                    observed_denominator,
                    comparison,
                    threshold,
                    threshold_denominator| {
        rows.push(section15_predicate(
            row.metric,
            row.unit,
            ordinal,
            observed,
            observed_denominator,
            comparison,
            threshold,
            threshold_denominator,
        )?);
        Ok::<(), HarnessError>(())
    };
    let expanded = expanded_section15_predicate_manifest_rows(trial.raw.scenario)
        .map_err(|_| HarnessError::Invalid("predicate manifest expansion failed"))?;
    for (row, ordinal) in expanded {
        match row.metric {
            Section15MetricV1::InputResponse => push(
                row,
                ordinal,
                trial
                    .input_response
                    .as_ref()
                    .ok_or(HarnessError::Invalid("input distribution is missing"))?
                    .p95_ns as u128,
                None,
                ThresholdComparisonV1::LessThan,
                thresholds.input_response_p95_ns_exclusive as u128,
                None,
            )?,
            Section15MetricV1::ScreenUpdate => push(
                row,
                ordinal,
                trial
                    .screen_update
                    .as_ref()
                    .ok_or(HarnessError::Invalid("screen distribution is missing"))?
                    .p95_ns as u128,
                None,
                ThresholdComparisonV1::LessThan,
                thresholds.screen_update_p95_ns_exclusive as u128,
                None,
            )?,
            Section15MetricV1::Startup => push(
                row,
                ordinal,
                trial
                    .startup_ns
                    .ok_or(HarnessError::Invalid("startup observation is missing"))?
                    as u128,
                None,
                ThresholdComparisonV1::LessThan,
                thresholds.startup_ns_exclusive as u128,
                None,
            )?,
            Section15MetricV1::IdleCpu => {
                let numerator = (trial.user_cpu_ns as u128)
                    .checked_add(trial.system_cpu_ns as u128)
                    .and_then(|value| value.checked_mul(100_000))
                    .ok_or(HarnessError::Invalid("idle CPU predicate overflowed"))?;
                push(
                    row,
                    ordinal,
                    numerator,
                    Some(trial.elapsed_ns as u128),
                    ThresholdComparisonV1::LessThan,
                    thresholds.idle_cpu_milli_percent_exclusive as u128,
                    None,
                )?;
            }
            Section15MetricV1::FallbackAddedDelay => {
                let pair = trial
                    .raw
                    .fallback_pairs
                    .iter()
                    .find(|pair| Some(pair.sequence) == ordinal)
                    .ok_or(HarnessError::Invalid("fallback pair is missing"))?;
                let delay = pair
                    .rescan_ns
                    .checked_sub(pair.notification_ns)
                    .ok_or(HarnessError::Invalid("fallback timestamps regressed"))?;
                push(
                    row,
                    ordinal,
                    delay as u128,
                    None,
                    ThresholdComparisonV1::LessThanOrEqual,
                    thresholds.fallback_added_delay_ns_inclusive as u128,
                    None,
                )?;
            }
            Section15MetricV1::AdmissionDeadline => {
                let bucket = ordinal.ok_or(HarnessError::Invalid(
                    "admission predicate ordinal is missing",
                ))?;
                let origin = trial
                    .raw
                    .workload_origin_ns
                    .ok_or(HarnessError::Invalid("admission origin is missing"))?;
                let bucket_end = origin
                    .checked_add(
                        bucket
                            .checked_add(1)
                            .and_then(|value| value.checked_mul(1_000_000_000))
                            .ok_or(HarnessError::Invalid("admission bucket overflowed"))?,
                    )
                    .ok_or(HarnessError::Invalid("admission bucket overflowed"))?;
                let observed = trial
                    .raw
                    .admission_observations
                    .iter()
                    .filter(|observation| observation.scheduled_ns <= bucket_end)
                    .map(|observation| observation.admitted_ns)
                    .max()
                    .ok_or(HarnessError::Invalid("admission bucket is empty"))?;
                let threshold = bucket_end
                    .checked_add(spec.period_ns)
                    .ok_or(HarnessError::Invalid("admission deadline overflowed"))?;
                push(
                    row,
                    ordinal,
                    observed as u128,
                    None,
                    ThresholdComparisonV1::LessThanOrEqual,
                    threshold as u128,
                    None,
                )?;
            }
            Section15MetricV1::SubmittedSequences
            | Section15MetricV1::AdmittedSequences
            | Section15MetricV1::CompletedSequences
            | Section15MetricV1::PersistedSequences
            | Section15MetricV1::RenderedProbeSequences => {
                let (observed, expected) = match row.metric {
                    Section15MetricV1::SubmittedSequences => {
                        (trial.raw.submitted_sequences.len(), spec.admission_count)
                    }
                    Section15MetricV1::AdmittedSequences => {
                        (trial.raw.admitted_sequences.len(), spec.admission_count)
                    }
                    Section15MetricV1::CompletedSequences => {
                        (trial.raw.completed_sequences.len(), spec.admission_count)
                    }
                    Section15MetricV1::PersistedSequences => {
                        (trial.raw.persisted_sequences.len(), spec.admission_count)
                    }
                    Section15MetricV1::RenderedProbeSequences => {
                        (trial.raw.rendered_sequences.len(), spec.screen_probe_count)
                    }
                    _ => unreachable!(),
                };
                push(
                    row,
                    ordinal,
                    observed as u128,
                    None,
                    ThresholdComparisonV1::Equal,
                    expected as u128,
                    None,
                )?;
            }
            Section15MetricV1::MaximumProcessTreeRss => push(
                row,
                ordinal,
                trial.maximum_process_tree_rss_bytes as u128,
                None,
                ThresholdComparisonV1::LessThan,
                thresholds.process_tree_rss_bytes_exclusive as u128,
                None,
            )?,
            Section15MetricV1::PerformanceDegradation => {
                let stream = trial
                    .raw
                    .performance_evidence_stream
                    .as_ref()
                    .ok_or(HarnessError::Invalid("performance stream is missing"))?;
                let observed = if trial.raw.scenario == ScenarioV1::TwiceTarget {
                    u128::from(stream.selected_terminal_draw_ordinal.is_some())
                } else {
                    let trial_has_event_lag_reason = stream
                        .samples
                        .iter()
                        .any(|sample| sample.reasons.contains(&PerformanceReasonV1::EventLag));
                    stream
                        .samples
                        .iter()
                        .filter(|sample| {
                            !sample.reasons.is_empty()
                                && !tolerated_boundary_degradation(
                                    stage,
                                    trial.raw.scenario,
                                    sample,
                                    trial_has_event_lag_reason,
                                )
                        })
                        .count() as u128
                };
                push(
                    row,
                    ordinal,
                    observed,
                    None,
                    ThresholdComparisonV1::Equal,
                    u128::from(trial.raw.scenario == ScenarioV1::TwiceTarget),
                    None,
                )?;
            }
            Section15MetricV1::ReducerLag
            | Section15MetricV1::PublishToRender
            | Section15MetricV1::D4Analysis
            | Section15MetricV1::ReducerPlusPublish => {
                return Err(HarnessError::Invalid(
                    "distribution-only metric appeared in predicate manifest",
                ));
            }
        }
    }
    Ok(rows)
}

fn section15_distribution_rows(trial: &TrialResultV1) -> Vec<Section15DistributionV1> {
    let d4 = distribution_from(
        trial
            .raw
            .scoped_observations
            .iter()
            .map(|row| row.d4_analysis_ns)
            .collect(),
    );
    let reducer_plus_publish = distribution_from(
        trial
            .raw
            .scoped_observations
            .iter()
            .map(|row| row.reducer_plus_publish_ns)
            .collect(),
    );
    let startup = trial.startup_ns.map(|value| DistributionV1 {
        sample_count: 1,
        minimum_ns: value,
        median_ns: value,
        p95_ns: value,
        p99_ns: value,
        maximum_ns: value,
    });
    section15_manifest_rows(trial.raw.scenario)
        .expect("all closed scenarios have Section 15 manifest rows")
        .distribution_rows
        .iter()
        .map(|row| {
            let value = match row.metric {
                Section15MetricV1::InputResponse => trial.input_response.as_ref(),
                Section15MetricV1::ScreenUpdate => trial.screen_update.as_ref(),
                Section15MetricV1::ReducerLag => trial.reducer_lag.as_ref(),
                Section15MetricV1::PublishToRender => trial.publish_to_render.as_ref(),
                Section15MetricV1::Startup => startup.as_ref(),
                Section15MetricV1::FallbackAddedDelay => trial.fallback_added_delay_ns.as_ref(),
                Section15MetricV1::D4Analysis => d4.as_ref(),
                Section15MetricV1::ReducerPlusPublish => reducer_plus_publish.as_ref(),
                Section15MetricV1::IdleCpu
                | Section15MetricV1::MaximumProcessTreeRss
                | Section15MetricV1::AdmissionDeadline
                | Section15MetricV1::SubmittedSequences
                | Section15MetricV1::AdmittedSequences
                | Section15MetricV1::CompletedSequences
                | Section15MetricV1::PersistedSequences
                | Section15MetricV1::RenderedProbeSequences
                | Section15MetricV1::PerformanceDegradation => None,
            }
            .expect("manifest distribution rows must name applicable measured distributions");
            Section15DistributionV1 {
                metric: row.metric,
                unit: row.unit,
                sample_count: value.sample_count as u64,
                minimum: value.minimum_ns.to_string(),
                median: value.median_ns.to_string(),
                p95: value.p95_ns.to_string(),
                p99: value.p99_ns.to_string(),
                maximum: value.maximum_ns.to_string(),
            }
        })
        .collect()
}

pub fn synthetic_section15_rederivation() -> Section15ReDerivationV1 {
    let scenarios = [
        ScenarioV1::Target,
        ScenarioV1::Sustained,
        ScenarioV1::Burst,
        ScenarioV1::Startup,
        ScenarioV1::Idle,
        ScenarioV1::FallbackRescan,
        ScenarioV1::TwiceTarget,
    ];
    let selected_results = scenarios
        .iter()
        .flat_map(|scenario| {
            [MeasurementStageV1::Baseline, MeasurementStageV1::Final].map(|stage| {
                SelectedResultIdentityV1 {
                    measurement_stage: stage,
                    scenario: *scenario,
                    canonical_result_path: format!("/tmp/results/{stage:?}/{scenario:?}/result-v1.json"),
                    canonical_raw_root: format!("/tmp/results/{stage:?}/{scenario:?}"),
                    result_sha256: "1".repeat(64),
                    production_subject_sha: if stage == MeasurementStageV1::Baseline {
                        BASELINE_SUBJECT_SHA.to_owned()
                    } else {
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()
                    },
                    harness_sha: SYNTHETIC_HARNESS_SHA.to_owned(),
                    workload_schema_sha256: WORKLOAD_SCHEMA_V1_SHA256.to_owned(),
                    baseline_id: format!(
                        "sha256:v1:{BASELINE_SUBJECT_SHA}:{SYNTHETIC_HARNESS_SHA}:{WORKLOAD_SCHEMA_V1_SHA256}"
                    ),
                    measured_binary: synthetic_run_controls().measured_binary,
                }
            })
        })
        .collect();
    let scenario_rows = scenarios
        .iter()
        .map(|scenario| {
            let final_outcome = synthetic_result(*scenario, MeasurementStageV1::Final);
            Section15ScenarioReDerivationV1 {
                scenario: *scenario,
                baseline_status: ReferenceOutcomeStatusV1::Pass,
                final_status: ReferenceOutcomeStatusV1::Pass,
                final_failure_reasons: Vec::new(),
                trials: final_outcome
                    .document()
                    .trials
                    .iter()
                    .map(|trial| section15_trial_row(MeasurementStageV1::Final, trial))
                    .collect::<Result<Vec<_>, _>>()
                    .expect("synthetic Section 15 trials must re-derive"),
            }
        })
        .collect::<Vec<_>>();
    let baseline_deltas = scenario_rows
        .iter()
        .flat_map(|scenario| {
            scenario.trials.iter().flat_map(move |trial| {
                trial.distributions.iter().flat_map(move |distribution| {
                    [
                        (DistributionStatisticV1::Minimum, &distribution.minimum),
                        (DistributionStatisticV1::Median, &distribution.median),
                        (DistributionStatisticV1::P95, &distribution.p95),
                        (DistributionStatisticV1::P99, &distribution.p99),
                        (DistributionStatisticV1::Maximum, &distribution.maximum),
                    ]
                    .map(|(statistic, value)| Section15BaselineDeltaV1 {
                        scenario: scenario.scenario,
                        trial_index: trial.trial_index,
                        metric: distribution.metric,
                        statistic,
                        unit: distribution.unit,
                        baseline_value: value.clone(),
                        final_value: value.clone(),
                        signed_delta: "0".to_owned(),
                    })
                })
            })
        })
        .collect();
    Section15ReDerivationV1 {
        schema_version: 1,
        subject_sha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        baseline_id: format!(
            "sha256:v1:{BASELINE_SUBJECT_SHA}:{SYNTHETIC_HARNESS_SHA}:{WORKLOAD_SCHEMA_V1_SHA256}"
        ),
        selected_results,
        scenarios: scenario_rows,
        baseline_deltas,
        failure_policy_evidence: Vec::new(),
        decision: D4CheckpointDecisionV1::NoMissD4NotAuthorized {},
    }
}

#[derive(Clone, Debug)]
pub struct ObserverFixtureEvidenceV1 {
    pub control: ObserverControlEvidenceV1,
    pub process_tree: ProcessTreeEvidenceV1,
    pub setup_started_ns: u64,
    pub setup_child_pid: u32,
}

pub fn observer_ready_scaffold() -> ObserverFixtureEvidenceV1 {
    ObserverFixtureEvidenceV1 {
        control: synthetic_observer_control(ScenarioV1::Sustained, 1, 3, None, None),
        process_tree: ProcessTreeEvidenceV1 {
            observer_pid: 2,
            observer_affinity_cpu_ids: vec![4, 5, 6, 7, 12, 13, 14, 15],
            observed_root_pid: 1,
            observed_root_start_time_ticks: 1,
            clock_ticks_per_second: 100,
            trial_origin_ns: 1,
            observer_ready_ns: 3,
            idle_window_start_ns: None,
            idle_window_end_ns: None,
            resource_observations: vec![ResourceObservationV1 {
                offset_ns: 2,
                process_tree_user_cpu_ns: 0,
                process_tree_system_cpu_ns: 0,
                process_tree_rss_bytes: 1,
            }],
            process_identity_resources: Vec::new(),
        },
        setup_started_ns: 2,
        setup_child_pid: 4,
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ObserverFixtureWireV1 {
    control: ObserverControlEvidenceV1,
    process_tree: ProcessTreeEvidenceV1,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct LinuxProcessStat {
    pid: u32,
    parent_pid: u32,
    user_ticks: u64,
    system_ticks: u64,
    start_time_ticks: u64,
    rss_pages: u64,
    vm_hwm_bytes: u64,
}

#[cfg(target_os = "linux")]
pub fn observe_linux_fixture_root_from_sibling_process() -> ObserverFixtureEvidenceV1 {
    use std::io::Write as _;
    use std::process::{Command, Stdio};
    let fixture = tempfile::tempdir().expect("observer fixture root must exist");
    let ready = fixture.path().join("ready");
    let child_pid_path = fixture.path().join("child-pid");
    let output = fixture.path().join("observer-result.json");
    let trial_origin_ns = monotonic_ns().expect("CLOCK_MONOTONIC must be available");
    let mut root = Command::new("/usr/bin/bash")
        .arg("-p")
        .arg("-c")
        .arg("read -r _; /usr/bin/sleep 5 & child=$!; /usr/bin/printf '%s\\n' \"$child\" > \"$FIXTURE_CHILD_PID\"; wait \"$child\"")
        .env("FIXTURE_CHILD_PID", &child_pid_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("fixture measured root must start");
    let mut root_stdin = root.stdin.take().expect("fixture root stdin must be piped");
    let root_pid = root.id();
    let root_start = linux_process_start_time_ticks(root_pid)
        .expect("fixture root immutable identity must be readable");
    let mut observer = Command::new(
        std::env::current_exe().expect("integration-test executable must be available"),
    )
    .args([
        "fixture_process_tree_observer_helper",
        "--exact",
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ])
    .env("HERDR_FIXTURE_ROOT_PID", root_pid.to_string())
    .env("HERDR_FIXTURE_ROOT_START_TICKS", root_start.to_string())
    .env("HERDR_FIXTURE_TRIAL_ORIGIN_NS", trial_origin_ns.to_string())
    .env("HERDR_FIXTURE_READY_PATH", &ready)
    .env("HERDR_FIXTURE_OBSERVER_OUTPUT", &output)
    .spawn()
    .expect("fixture sibling observer must start");
    wait_for_path(&ready, Duration::from_secs(5)).expect("observer Ready barrier must arrive");
    let setup_started_ns = monotonic_ns().expect("CLOCK_MONOTONIC must be available");
    root_stdin
        .write_all(b"start\n")
        .expect("Ready barrier must release measured setup");
    drop(root_stdin);
    wait_for_path(&child_pid_path, Duration::from_secs(5))
        .expect("measured root setup child must start after Ready");
    let setup_child_pid = std::fs::read_to_string(&child_pid_path)
        .expect("setup child PID must be readable")
        .trim()
        .parse()
        .expect("setup child PID must be canonical");
    let status = observer.wait().expect("fixture observer must be waitable");
    assert!(
        status.success(),
        "fixture sibling observer failed: {status}"
    );
    let wire: ObserverFixtureWireV1 =
        read_closed_json(&output).expect("fixture observer must atomically write closed evidence");
    let _ = root.kill();
    let _ = root.wait();
    ObserverFixtureEvidenceV1 {
        control: wire.control,
        process_tree: wire.process_tree,
        setup_started_ns,
        setup_child_pid,
    }
}

#[cfg(target_os = "linux")]
pub fn run_linux_fixture_observer_from_env() -> Result<(), HarnessError> {
    let root_pid = required_env_parse::<u32>("HERDR_FIXTURE_ROOT_PID")?;
    let root_start = required_env_parse::<u64>("HERDR_FIXTURE_ROOT_START_TICKS")?;
    let trial_origin_ns = required_env_parse::<u64>("HERDR_FIXTURE_TRIAL_ORIGIN_NS")?;
    let ready_path = required_env_path("HERDR_FIXTURE_READY_PATH")?;
    let output_path = required_env_path("HERDR_FIXTURE_OBSERVER_OUTPUT")?;
    let observer_pid = std::process::id();
    let observer_ready_ns = monotonic_ns()?;
    let mut process_tree = sample_linux_process_tree(
        root_pid,
        root_start,
        observer_pid,
        trial_origin_ns,
        observer_ready_ns,
    )?;
    process_tree.observer_ready_ns = observer_ready_ns;
    let control = ObserverControlEvidenceV1 {
        protocol_version: 1,
        scenario: ScenarioV1::Sustained,
        observed_root_pid: root_pid,
        observed_root_start_time_ticks: root_start,
        trial_origin_ns,
        observer_ready_ns,
        idle_window_start_ns: None,
        idle_window_end_ns: None,
        commands: Vec::new(),
        frames: vec![ObserverControlFrameV1::Ready { observer_ready_ns }],
    };
    atomic_write_bytes(&ready_path, b"ready\n")?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let sampled_at_ns = monotonic_ns()?;
        let latest = sample_linux_process_tree(
            root_pid,
            root_start,
            observer_pid,
            trial_origin_ns,
            sampled_at_ns,
        )?;
        if latest.process_identity_resources.len() >= 2 {
            process_tree
                .resource_observations
                .extend(latest.resource_observations);
            process_tree.process_identity_resources = latest.process_identity_resources;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if process_tree.process_identity_resources.len() < 2 {
        return Err(HarnessError::Invalid(
            "fixture root did not create a descendant after Ready",
        ));
    }
    atomic_write_json(
        &output_path,
        &ObserverFixtureWireV1 {
            control,
            process_tree,
        },
    )
}

#[cfg(target_os = "linux")]
pub fn run_linux_reference_observer_from_environment() -> Result<(), HarnessError> {
    use std::io::BufRead as _;
    use std::os::unix::net::UnixStream;

    require_exact_environment(&[
        "CARGO_HOME",
        "HERDR_PERF_OBSERVED_ROOT_PID",
        "HERDR_PERF_OBSERVED_ROOT_START_TICKS",
        "HERDR_PERF_OBSERVER_CONTROL_OUTPUT",
        "HERDR_PERF_OBSERVER_CONTROL_SOCKET",
        "HERDR_PERF_PROCESS_TREE_OUTPUT",
        "HERDR_PERF_SCENARIO",
        "HERDR_PERF_TRIAL_ORIGIN_NS",
        "HOME",
        "LC_ALL",
        "PATH",
        "RUSTUP_HOME",
        "TZ",
    ])?;
    if invariant_environment()
        .iter()
        .any(|(key, value)| std::env::var(key).ok().as_ref() != Some(value))
    {
        return Err(HarnessError::Invalid(
            "observer invariant environment mismatch",
        ));
    }
    let scenario = parse_scenario_token(&required_environment_string("HERDR_PERF_SCENARIO")?)?;
    let root_pid = required_env_parse::<u32>("HERDR_PERF_OBSERVED_ROOT_PID")?;
    let root_start = required_env_parse::<u64>("HERDR_PERF_OBSERVED_ROOT_START_TICKS")?;
    let trial_origin_ns = required_env_parse::<u64>("HERDR_PERF_TRIAL_ORIGIN_NS")?;
    let socket_path = required_env_path("HERDR_PERF_OBSERVER_CONTROL_SOCKET")?;
    let control_output = required_env_path("HERDR_PERF_OBSERVER_CONTROL_OUTPUT")?;
    let process_output = required_env_path("HERDR_PERF_PROCESS_TREE_OUTPUT")?;
    if control_output == process_output
        || !socket_path.is_absolute()
        || !control_output.is_absolute()
        || !process_output.is_absolute()
        || socket_path == control_output
        || socket_path == process_output
    {
        return Err(HarnessError::Invalid("observer paths were invalid"));
    }
    let observer_pid = std::process::id();
    let ticks_per_second = clock_ticks_per_second()?;
    let affinity = linux_current_affinity()?;
    if affinity != [4, 5, 6, 7, 12, 13, 14, 15] {
        return Err(HarnessError::Invalid("observer affinity was not isolated"));
    }
    let stream = UnixStream::connect(&socket_path)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut writer = stream.try_clone()?;
    let mut reader = std::io::BufReader::new(stream);
    let first_sample_ns = monotonic_ns()?;
    let first_stats = linux_process_tree_stats(root_pid, root_start, observer_pid)?;
    let observer_ready_ns = monotonic_ns()?;
    if observer_ready_ns
        .checked_sub(trial_origin_ns)
        .is_none_or(|elapsed| elapsed > 5_000_000_000)
    {
        return Err(HarnessError::Invalid("observer Ready deadline was missed"));
    }
    let ready = ObserverControlFrameV1::Ready { observer_ready_ns };
    write_control_frame(&mut writer, &ready)?;
    let mut frames = vec![ready];
    let mut commands = Vec::new();
    let mut observations = Vec::new();
    let mut identities = std::collections::BTreeMap::new();
    record_linux_sample(
        &first_stats,
        first_sample_ns,
        trial_origin_ns,
        ticks_per_second,
        &mut observations,
        &mut identities,
    )?;
    let mut idle_window_start_ns = None;
    let mut idle_window_end_ns = None;
    if scenario == ScenarioV1::Idle {
        let command = read_control_command(&mut reader)?;
        if command != (ObserverCommandV1::StartIdleWindow {}) {
            return Err(HarnessError::Invalid("unexpected observer command"));
        }
        commands.push(command);
        let request_received_ns = monotonic_ns()?;
        let start_stats = linux_process_tree_stats(root_pid, root_start, observer_pid)?;
        let start_ns = monotonic_ns()?;
        for process in &start_stats {
            let first_observed_offset_ns =
                start_ns
                    .checked_sub(trial_origin_ns)
                    .ok_or(HarnessError::Invalid(
                        "observer sample preceded trial origin",
                    ))?;
            let identity = identities
                .entry((process.pid, process.start_time_ticks))
                .or_insert_with(|| process_identity_from_stat(process, first_observed_offset_ns));
            identity.idle_window_start_user_cpu_ticks = Some(process.user_ticks);
            identity.idle_window_start_system_cpu_ticks = Some(process.system_ticks);
        }
        idle_window_start_ns = Some(start_ns);
        let started = ObserverControlFrameV1::IdleWindowStarted {
            request_received_ns,
            start_ns,
        };
        write_control_frame(&mut writer, &started)?;
        frames.push(started);
        loop {
            std::thread::sleep(Duration::from_millis(10));
            let sampled_ns = monotonic_ns()?;
            let stats = linux_process_tree_stats(root_pid, root_start, observer_pid)?;
            record_linux_sample(
                &stats,
                sampled_ns,
                trial_origin_ns,
                ticks_per_second,
                &mut observations,
                &mut identities,
            )?;
            for process in &stats {
                let identity = identities
                    .get_mut(&(process.pid, process.start_time_ticks))
                    .ok_or(HarnessError::Invalid("sampled identity was not retained"))?;
                if identity.idle_window_start_user_cpu_ticks.is_none() {
                    identity.idle_window_start_user_cpu_ticks = Some(0);
                    identity.idle_window_start_system_cpu_ticks = Some(0);
                }
            }
            if sampled_ns
                .checked_sub(start_ns)
                .is_some_and(|elapsed| elapsed >= 30_000_000_000)
            {
                close_idle_window_identities(identities.values_mut());
                idle_window_end_ns = Some(sampled_ns);
                let ended = ObserverControlFrameV1::IdleWindowEnded { end_ns: sampled_ns };
                write_control_frame(&mut writer, &ended)?;
                frames.push(ended);
                break;
            }
        }
    } else {
        reader
            .get_ref()
            .set_read_timeout(Some(Duration::from_millis(10)))?;
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => return Err(HarnessError::Invalid("unexpected observer command")),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(error) => return Err(error.into()),
            }
            let sampled_ns = monotonic_ns()?;
            let stats = match linux_process_tree_stats(root_pid, root_start, observer_pid) {
                Ok(stats) => stats,
                Err(_) => break,
            };
            record_linux_sample(
                &stats,
                sampled_ns,
                trial_origin_ns,
                ticks_per_second,
                &mut observations,
                &mut identities,
            )?;
        }
    }
    let control = ObserverControlEvidenceV1 {
        protocol_version: 1,
        scenario,
        observed_root_pid: root_pid,
        observed_root_start_time_ticks: root_start,
        trial_origin_ns,
        observer_ready_ns,
        idle_window_start_ns,
        idle_window_end_ns,
        commands,
        frames,
    };
    let process_tree = ProcessTreeEvidenceV1 {
        observer_pid,
        observer_affinity_cpu_ids: affinity,
        observed_root_pid: root_pid,
        observed_root_start_time_ticks: root_start,
        clock_ticks_per_second: ticks_per_second,
        trial_origin_ns,
        observer_ready_ns,
        idle_window_start_ns,
        idle_window_end_ns,
        resource_observations: observations,
        process_identity_resources: identities.into_values().collect(),
    };
    atomic_write_json(&control_output, &control)?;
    atomic_write_json(&process_output, &process_tree)
}

#[cfg(target_os = "linux")]
fn close_idle_window_identities<'a>(
    identities: impl Iterator<Item = &'a mut ProcessIdentityResourceV1>,
) {
    for identity in identities {
        if identity.idle_window_start_user_cpu_ticks.is_some()
            && identity.idle_window_start_system_cpu_ticks.is_some()
        {
            identity.idle_window_end_user_cpu_ticks = Some(identity.last_user_cpu_ticks);
            identity.idle_window_end_system_cpu_ticks = Some(identity.last_system_cpu_ticks);
        }
    }
}

#[cfg(target_os = "linux")]
pub fn close_idle_window_identity_for_test(identity: &mut ProcessIdentityResourceV1) {
    close_idle_window_identities(std::iter::once(identity));
}

#[cfg(target_os = "linux")]
fn write_control_frame(
    writer: &mut std::os::unix::net::UnixStream,
    frame: &ObserverControlFrameV1,
) -> Result<(), HarnessError> {
    use std::io::Write as _;
    serde_json::to_writer(&mut *writer, frame)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn read_control_command(
    reader: &mut impl std::io::BufRead,
) -> Result<ObserverCommandV1, HarnessError> {
    let mut bytes = Vec::new();
    reader.read_until(b'\n', &mut bytes)?;
    if bytes.last() != Some(&b'\n') {
        return Err(HarnessError::Invalid("observer command was incomplete"));
    }
    let command: ObserverCommandV1 = serde_json::from_slice(&bytes[..bytes.len() - 1])?;
    let mut canonical = serde_json::to_vec(&command)?;
    canonical.push(b'\n');
    if bytes != canonical {
        return Err(HarnessError::Invalid("observer command was not canonical"));
    }
    Ok(command)
}

#[cfg(target_os = "linux")]
fn linux_process_tree_stats(
    root_pid: u32,
    root_start_time_ticks: u64,
    observer_pid: u32,
) -> Result<Vec<LinuxProcessStat>, HarnessError> {
    let stats = linux_process_stats()?;
    if stats
        .get(&root_pid)
        .is_none_or(|root| root.start_time_ticks != root_start_time_ticks)
    {
        return Err(HarnessError::Invalid("observed root identity changed"));
    }
    let mut members = BTreeSet::from([root_pid]);
    loop {
        let before = members.len();
        for process in stats.values() {
            if members.contains(&process.parent_pid) && process.pid != observer_pid {
                members.insert(process.pid);
            }
        }
        if members.len() == before {
            break;
        }
    }
    members.remove(&observer_pid);
    members
        .into_iter()
        .map(|pid| {
            stats.get(&pid).cloned().ok_or(HarnessError::Invalid(
                "descendant disappeared during snapshot",
            ))
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn process_identity_from_stat(
    process: &LinuxProcessStat,
    first_observed_offset_ns: u64,
) -> ProcessIdentityResourceV1 {
    ProcessIdentityResourceV1 {
        pid: process.pid,
        start_time_ticks: process.start_time_ticks,
        first_observed_offset_ns,
        idle_window_start_user_cpu_ticks: None,
        idle_window_start_system_cpu_ticks: None,
        idle_window_end_user_cpu_ticks: None,
        idle_window_end_system_cpu_ticks: None,
        last_user_cpu_ticks: process.user_ticks,
        last_system_cpu_ticks: process.system_ticks,
        maximum_vm_hwm_bytes: process.vm_hwm_bytes,
    }
}

#[cfg(target_os = "linux")]
fn record_linux_sample(
    stats: &[LinuxProcessStat],
    sampled_ns: u64,
    trial_origin_ns: u64,
    ticks_per_second: u64,
    observations: &mut Vec<ResourceObservationV1>,
    identities: &mut std::collections::BTreeMap<(u32, u64), ProcessIdentityResourceV1>,
) -> Result<(), HarnessError> {
    let page_size = page_size_bytes()?;
    let offset_ns = sampled_ns
        .checked_sub(trial_origin_ns)
        .ok_or(HarnessError::Invalid(
            "observer sample preceded trial origin",
        ))?;
    let mut user_ticks = 0_u64;
    let mut system_ticks = 0_u64;
    let mut rss_bytes = 0_u64;
    for process in stats {
        user_ticks = user_ticks
            .checked_add(process.user_ticks)
            .ok_or(HarnessError::Invalid("user CPU ticks overflowed"))?;
        system_ticks = system_ticks
            .checked_add(process.system_ticks)
            .ok_or(HarnessError::Invalid("system CPU ticks overflowed"))?;
        rss_bytes = rss_bytes
            .checked_add(
                process
                    .rss_pages
                    .checked_mul(page_size)
                    .ok_or(HarnessError::Invalid("RSS bytes overflowed"))?,
            )
            .ok_or(HarnessError::Invalid("process-tree RSS overflowed"))?;
        let identity = identities
            .entry((process.pid, process.start_time_ticks))
            .or_insert_with(|| process_identity_from_stat(process, offset_ns));
        identity.last_user_cpu_ticks = process.user_ticks;
        identity.last_system_cpu_ticks = process.system_ticks;
        identity.maximum_vm_hwm_bytes = identity.maximum_vm_hwm_bytes.max(process.vm_hwm_bytes);
    }
    let ticks_to_ns = |ticks: u64| {
        (ticks as u128)
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_div(ticks_per_second as u128))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(HarnessError::Invalid("CPU nanoseconds overflowed"))
    };
    observations.push(ResourceObservationV1 {
        offset_ns,
        process_tree_user_cpu_ns: ticks_to_ns(user_ticks)?,
        process_tree_system_cpu_ns: ticks_to_ns(system_ticks)?,
        process_tree_rss_bytes: rss_bytes,
    });
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_current_affinity() -> Result<Vec<u32>, HarnessError> {
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    // SAFETY: `set` is a valid writable cpu_set_t for the current process.
    if unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) } != 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok((0..libc::CPU_SETSIZE as usize)
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
        .map(|cpu| cpu as u32)
        .collect())
}

/// CoreFoundation, linked through `notify` -> `fsevent-sys`, sets this key
/// inside the child process after exec on macOS, so a parent's
/// `env_clear()` cannot exclude it. Tolerate exactly this key: it is never
/// required, and its value is never pinned or read.
#[cfg(target_os = "macos")]
const HOST_INJECTED_ENVIRONMENT_KEYS: &[&str] = &["__CF_USER_TEXT_ENCODING"];
#[cfg(not(target_os = "macos"))]
const HOST_INJECTED_ENVIRONMENT_KEYS: &[&str] = &[];

fn require_exact_environment(expected: &[&str]) -> Result<(), HarnessError> {
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    let actual = std::env::vars_os()
        .map(|(key, _)| {
            key.into_string()
                .map_err(|_| HarnessError::Invalid("environment key was not UTF-8"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut actual_keys = actual.iter().map(String::as_str).collect::<BTreeSet<_>>();
    for key in HOST_INJECTED_ENVIRONMENT_KEYS {
        actual_keys.remove(*key);
    }
    if actual_keys != expected {
        return Err(HarnessError::Invalid("environment key set was not closed"));
    }
    Ok(())
}

fn require_closed_environment(additional: &[&str]) -> Result<(), HarnessError> {
    require_closed_environment_with_optional(additional, &[])?;
    Ok(())
}

fn require_closed_environment_with_optional(
    additional: &[&str],
    optional: &[&str],
) -> Result<BTreeSet<String>, HarnessError> {
    let actual = std::env::vars_os()
        .map(|(key, value)| {
            key.into_string()
                .map(|key| (key, value))
                .map_err(|_| HarnessError::Invalid("environment key was not UTF-8"))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
    let mut required = ["CARGO_HOME", "HOME", "LC_ALL", "PATH", "RUSTUP_HOME", "TZ"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    required.extend(additional.iter().copied());
    let mut allowed = required.clone();
    allowed.extend(optional.iter().copied());
    allowed.extend(HOST_INJECTED_ENVIRONMENT_KEYS.iter().copied());
    let actual_keys = actual.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if !required.is_subset(&actual_keys) || !actual_keys.is_subset(&allowed) {
        return Err(HarnessError::Invalid("environment key set was not closed"));
    }
    if invariant_environment().iter().any(|(key, value)| {
        actual
            .get(key)
            .and_then(|actual| actual.to_str())
            .is_none_or(|actual| actual != value)
    }) {
        return Err(HarnessError::Invalid("invariant environment mismatch"));
    }
    let mut present = BTreeSet::new();
    for key in optional {
        if let Some(value) = actual.get(*key) {
            if value != std::ffi::OsStr::new("1") {
                return Err(HarnessError::Invalid(
                    "optional environment value was invalid",
                ));
            }
            present.insert((*key).to_owned());
        }
    }
    Ok(present)
}

fn amended_legacy_mode(optional: &BTreeSet<String>) -> AmendedLegacyMode {
    if optional.contains("HERDR_PERF_ACCEPT_AMENDED_LEGACY") {
        AmendedLegacyMode::AcceptAmendedLegacy
    } else {
        AmendedLegacyMode::Off
    }
}

#[cfg(target_os = "linux")]
fn sample_linux_process_tree(
    root_pid: u32,
    root_start_time_ticks: u64,
    observer_pid: u32,
    trial_origin_ns: u64,
    sampled_at_ns: u64,
) -> Result<ProcessTreeEvidenceV1, HarnessError> {
    let stats = linux_process_stats()?;
    if stats
        .get(&root_pid)
        .is_none_or(|root| root.start_time_ticks != root_start_time_ticks)
    {
        return Err(HarnessError::Invalid("observed root identity changed"));
    }
    let mut members = BTreeSet::from([root_pid]);
    loop {
        let before = members.len();
        for process in stats.values() {
            if members.contains(&process.parent_pid) && process.pid != observer_pid {
                members.insert(process.pid);
            }
        }
        if members.len() == before {
            break;
        }
    }
    members.remove(&observer_pid);
    let ticks_per_second = clock_ticks_per_second()?;
    let page_size = page_size_bytes()?;
    let mut user_ticks = 0_u64;
    let mut system_ticks = 0_u64;
    let mut rss_bytes = 0_u64;
    let offset_ns = sampled_at_ns
        .checked_sub(trial_origin_ns)
        .ok_or(HarnessError::Invalid(
            "observer sample preceded trial origin",
        ))?;
    let mut identities = Vec::new();
    for pid in members {
        let process = stats.get(&pid).ok_or(HarnessError::Invalid(
            "descendant disappeared during snapshot",
        ))?;
        user_ticks = user_ticks
            .checked_add(process.user_ticks)
            .ok_or(HarnessError::Invalid("user CPU ticks overflowed"))?;
        system_ticks = system_ticks
            .checked_add(process.system_ticks)
            .ok_or(HarnessError::Invalid("system CPU ticks overflowed"))?;
        rss_bytes = rss_bytes
            .checked_add(
                process
                    .rss_pages
                    .checked_mul(page_size)
                    .ok_or(HarnessError::Invalid("RSS bytes overflowed"))?,
            )
            .ok_or(HarnessError::Invalid("process-tree RSS overflowed"))?;
        identities.push(ProcessIdentityResourceV1 {
            pid,
            start_time_ticks: process.start_time_ticks,
            first_observed_offset_ns: offset_ns,
            idle_window_start_user_cpu_ticks: None,
            idle_window_start_system_cpu_ticks: None,
            idle_window_end_user_cpu_ticks: None,
            idle_window_end_system_cpu_ticks: None,
            last_user_cpu_ticks: process.user_ticks,
            last_system_cpu_ticks: process.system_ticks,
            maximum_vm_hwm_bytes: process.vm_hwm_bytes,
        });
    }
    let ticks_to_ns = |ticks: u64| {
        (ticks as u128)
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_div(ticks_per_second as u128))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(HarnessError::Invalid("CPU nanoseconds overflowed"))
    };
    Ok(ProcessTreeEvidenceV1 {
        observer_pid,
        observer_affinity_cpu_ids: vec![4, 5, 6, 7, 12, 13, 14, 15],
        observed_root_pid: root_pid,
        observed_root_start_time_ticks: root_start_time_ticks,
        clock_ticks_per_second: ticks_per_second,
        trial_origin_ns,
        observer_ready_ns: sampled_at_ns,
        idle_window_start_ns: None,
        idle_window_end_ns: None,
        resource_observations: vec![ResourceObservationV1 {
            offset_ns,
            process_tree_user_cpu_ns: ticks_to_ns(user_ticks)?,
            process_tree_system_cpu_ns: ticks_to_ns(system_ticks)?,
            process_tree_rss_bytes: rss_bytes,
        }],
        process_identity_resources: identities,
    })
}

#[cfg(target_os = "linux")]
fn linux_process_stats() -> Result<std::collections::BTreeMap<u32, LinuxProcessStat>, HarnessError>
{
    let mut values = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
        else {
            continue;
        };
        if let Ok(stat) = read_linux_process_stat(pid) {
            values.insert(pid, stat);
        }
    }
    Ok(values)
}

#[cfg(target_os = "linux")]
fn read_linux_process_stat(pid: u32) -> Result<LinuxProcessStat, HarnessError> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let close = stat
        .rfind(')')
        .ok_or(HarnessError::Invalid("malformed /proc stat comm field"))?;
    let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
    let field = |index: usize| {
        fields
            .get(index)
            .ok_or(HarnessError::Invalid("truncated /proc stat"))
    };
    let parse = |index: usize| {
        field(index)?
            .parse::<u64>()
            .map_err(|_| HarnessError::Invalid("non-numeric /proc stat field"))
    };
    let parent_pid = u32::try_from(parse(1)?)
        .map_err(|_| HarnessError::Invalid("parent PID did not fit u32"))?;
    let rss_pages = parse(21)?;
    let vm_hwm_bytes = match read_vm_hwm_bytes(pid) {
        Ok(value) => value,
        Err(_) => rss_pages
            .checked_mul(page_size_bytes()?)
            .ok_or(HarnessError::Invalid("VmHWM fallback overflowed"))?,
    };
    Ok(LinuxProcessStat {
        pid,
        parent_pid,
        user_ticks: parse(11)?,
        system_ticks: parse(12)?,
        start_time_ticks: parse(19)?,
        rss_pages,
        vm_hwm_bytes,
    })
}

#[cfg(target_os = "linux")]
fn read_vm_hwm_bytes(pid: u32) -> Result<u64, HarnessError> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    let kib = status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .ok_or(HarnessError::Invalid("VmHWM is missing"))?;
    kib.checked_mul(1024)
        .ok_or(HarnessError::Invalid("VmHWM bytes overflowed"))
}

#[cfg(target_os = "linux")]
pub fn linux_process_start_time_ticks(pid: u32) -> Result<u64, HarnessError> {
    Ok(read_linux_process_stat(pid)?.start_time_ticks)
}

#[cfg(target_os = "linux")]
fn clock_ticks_per_second() -> Result<u64, HarnessError> {
    // SAFETY: `sysconf` has no pointer arguments and `_SC_CLK_TCK` is a valid selector.
    let value = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(HarnessError::Invalid("_SC_CLK_TCK is unavailable"))
}

#[cfg(target_os = "linux")]
fn page_size_bytes() -> Result<u64, HarnessError> {
    // SAFETY: `sysconf` has no pointer arguments and `_SC_PAGESIZE` is a valid selector.
    let value = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(HarnessError::Invalid("_SC_PAGESIZE is unavailable"))
}

#[cfg(target_os = "linux")]
fn monotonic_ns() -> Result<u64, HarnessError> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `value` is a valid writable `timespec`, and `CLOCK_MONOTONIC` is valid on Linux.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let seconds = u64::try_from(value.tv_sec)
        .map_err(|_| HarnessError::Invalid("negative monotonic seconds"))?;
    let nanos = u64::try_from(value.tv_nsec)
        .map_err(|_| HarnessError::Invalid("negative monotonic nanoseconds"))?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanos))
        .ok_or(HarnessError::Invalid("monotonic nanoseconds overflowed"))
}

#[cfg(target_os = "linux")]
fn wait_for_path(path: &std::path::Path, timeout: Duration) -> Result<(), HarnessError> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if path
            .metadata()
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Err(HarnessError::Invalid("timed out waiting for fixture path"))
}

#[cfg(target_os = "linux")]
fn required_env_parse<T>(name: &'static str) -> Result<T, HarnessError>
where
    T: std::str::FromStr + ToString,
{
    let raw = std::env::var(name).map_err(|_| HarnessError::Invalid(name))?;
    let value = raw.parse::<T>().map_err(|_| HarnessError::Invalid(name))?;
    if value.to_string() != raw {
        return Err(HarnessError::Invalid(name));
    }
    Ok(value)
}

#[cfg(target_os = "linux")]
fn required_env_path(name: &'static str) -> Result<std::path::PathBuf, HarnessError> {
    std::env::var_os(name)
        .map(std::path::PathBuf::from)
        .ok_or(HarnessError::Invalid(name))
}

#[cfg(target_os = "linux")]
fn atomic_write_bytes(path: &std::path::Path, bytes: &[u8]) -> Result<(), HarnessError> {
    use std::io::Write as _;
    let parent = path
        .parent()
        .ok_or(HarnessError::Invalid("atomic byte path has no parent"))?;
    let temp = parent.join(format!(".observer-ready-{}.tmp", std::process::id()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(temp, path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}
