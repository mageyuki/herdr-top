//! Complete, deterministic, non-mutating Doctor report and renderers.

use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::diagnostics::local::{
    self, BreadcrumbVerdict, NameRecordVerdict, OwnerLockVerdict, PersistenceLogVerdict,
    SchemaProbeVerdict,
};
use crate::diagnostics::remote::{
    self, HerdrCompatibility, HerdrCompatibilityIssue, OfficialIntegrationAssessment,
    OfficialIntegrationStatus, OfficialIntegrationUnavailableReason, ProviderCliVersionStatus,
    StandaloneVersionStatus, VersionCommand, VersionCommandRunner, VersionProbeFailure,
    VersionProbeResult,
};
use crate::diagnostics::{
    ControllerCounterSnapshot, ControllerInputStatus, ControllerInputUnavailableReason,
    DiagnosticSource, EnrichmentCounterSnapshot, InputAvailability, OccurrenceLogStatus,
    OwnerFreshness, PersistenceCounters, RuntimeDiagnosticsSnapshot, SourceCoverageSnapshot,
};
use crate::herdr::controller;
use crate::herdr::types::{AgentSessionKind, Pong, Snapshot};
use crate::herdr::{types::AgentManifestStatus, wire};
use crate::lockfile::{self, StateRoot};
use crate::model::Provider;
use crate::provider;
use crate::rendezvous::{self, ControllerRuntimeProbe, SocketPathLength};
use crate::session_key::{self, ResolvedSession, ResolverSource};
use crate::store::{
    DurabilityDisposition, PersistenceFailure, PersistenceFailureCode, PersistenceOperation,
    PersistencePhase, PersistenceStatus,
};

const SCHEMA_VERSION: u64 = 1;
const LOG_FILE: &str = "herdr-top.log";
const VERSION_FIXTURE_ENV: &str = "HERDR_TOP_TEST_DOCTOR_VERSION_FIXTURES";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Ok,
    Warning,
    Error,
    NotApplicable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Check<T> {
    pub status: CheckStatus,
    pub code: &'static str,
    pub message: &'static str,
    pub observed: Option<T>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OverallStatus {
    Ok,
    Warning,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DoctorReportV1 {
    pub schema_version: u64,
    pub overall_status: OverallStatus,
    pub session: SessionSection,
    pub paths: PathsSection,
    pub herdr: HerdrSection,
    pub controller: ControllerSection,
    pub lock: LockSection,
    pub database: DatabaseSection,
    pub providers: ProvidersSection,
    pub compatibility: CompatibilitySection,
    pub coverage: CoverageSection,
    pub logs: LogsSection,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionSection {
    pub resolution: Check<SessionResolutionObservation>,
    pub name_record: Check<NameRecordObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PathsSection {
    pub state_base: Check<String>,
    pub state_root: Check<String>,
    pub breadcrumb: Check<BreadcrumbObservation>,
    pub controller_socket: Check<String>,
    pub socket_length: Check<PathLengthObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HerdrSection {
    pub endpoint: Check<EndpointObservation>,
    pub pong: Check<PongObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ControllerSection {
    pub runtime: Check<RuntimeDiagnosticsSnapshot>,
    pub rendezvous: Check<ControllerRendezvousObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LockSection {
    pub ownership: Check<LockObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DatabaseSection {
    pub schema: Check<DatabaseSchemaObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProvidersSection {
    pub discovery: Check<Vec<ProviderRootObservation>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompatibilitySection {
    pub herdr: Check<HerdrCompatibilityObservation>,
    pub integrations: Check<Vec<IntegrationObservation>>,
    pub standalone: Check<ExecutableVersionObservation>,
    pub provider_clis: Check<Vec<ExecutableVersionObservation>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CoverageSection {
    pub native_sessions: Check<CoverageObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LogsSection {
    pub location: Check<String>,
    pub persistence_occurrence: Check<PersistenceOccurrenceObservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionResolutionSource {
    Flag,
    Environment,
    ManagedDefault,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SessionResolutionObservation {
    pub source: SessionResolutionSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NameRecordObservation {
    Absent,
    Matches,
    Mismatch,
    Unsafe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BreadcrumbObservation {
    Absent,
    Matches,
    OtherRoot,
    Malformed,
    Unreadable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PathLengthObservation {
    pub bytes: u64,
    pub limit: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EndpointObservation {
    pub reachable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PongObservation {
    pub version: String,
    pub protocol_version: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControllerRendezvousObservation {
    Absent,
    Live,
    Stale,
    OrphanSocket,
    MalformedSentinel,
    Collision,
    PathTooLong,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LockObservation {
    Free,
    HeldWithOwner,
    HeldWithoutOwner,
    Unsafe,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DatabaseSchemaObservation {
    pub found: Option<u32>,
    pub supported: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProviderRootObservation {
    pub provider: DoctorProvider,
    pub path: String,
    pub exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HerdrCompatibilityObservation {
    pub found_version: String,
    pub minimum_version: String,
    pub found_protocol: u32,
    pub supported_protocols: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IntegrationObservation {
    pub provider: DoctorProvider,
    pub active_version: Option<String>,
    pub minimum_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExecutableVersionObservation {
    pub component: String,
    pub found_version: Option<String>,
    pub expected_version: Option<String>,
    pub stderr_present: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProviderCoverageCounts {
    pub provider: DoctorProvider,
    pub covered: u64,
    pub partial: u64,
    pub uncovered: u64,
    pub total: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CoverageObservation {
    pub covered: u64,
    pub partial: u64,
    pub uncovered: u64,
    pub total: u64,
    pub unknown_provider: u64,
    pub by_provider: Vec<ProviderCoverageCounts>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PersistenceOccurrenceObservation {
    pub timestamp_ms: i64,
    pub failure: PersistenceFailure,
    pub counters: PersistenceCounters,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorProvider {
    Claude,
    Codex,
}

impl From<Provider> for DoctorProvider {
    fn from(provider: Provider) -> Self {
        match provider {
            Provider::Claude => Self::Claude,
            Provider::Codex => Self::Codex,
        }
    }
}

#[derive(Clone)]
struct CheckView {
    status: CheckStatus,
    code: &'static str,
    message: &'static str,
    observed: String,
}

impl DoctorReportV1 {
    pub fn recompute_overall_status(&mut self) {
        self.overall_status = overall_status(self.checks().map(|(_, check)| check.status));
    }

    #[must_use]
    pub fn exit_code(&self) -> u8 {
        u8::from(
            self.checks()
                .any(|(_, check)| check.status == CheckStatus::Error),
        )
    }

    fn checks(&self) -> std::vec::IntoIter<(&'static str, CheckView)> {
        vec![
            view("session.resolution", &self.session.resolution),
            view("session.name_record", &self.session.name_record),
            view("paths.state_base", &self.paths.state_base),
            view("paths.state_root", &self.paths.state_root),
            view("paths.breadcrumb", &self.paths.breadcrumb),
            view("paths.controller_socket", &self.paths.controller_socket),
            view("paths.socket_length", &self.paths.socket_length),
            view("herdr.endpoint", &self.herdr.endpoint),
            view("herdr.pong", &self.herdr.pong),
            view("controller.runtime", &self.controller.runtime),
            view("controller.rendezvous", &self.controller.rendezvous),
            view("lock.ownership", &self.lock.ownership),
            view("database.schema", &self.database.schema),
            view("providers.discovery", &self.providers.discovery),
            view("compatibility.herdr", &self.compatibility.herdr),
            view(
                "compatibility.integrations",
                &self.compatibility.integrations,
            ),
            view("compatibility.standalone", &self.compatibility.standalone),
            view(
                "compatibility.provider_clis",
                &self.compatibility.provider_clis,
            ),
            view("coverage.native_sessions", &self.coverage.native_sessions),
            view("logs.location", &self.logs.location),
            view(
                "logs.persistence_occurrence",
                &self.logs.persistence_occurrence,
            ),
        ]
        .into_iter()
    }
}

fn view<T: Serialize>(name: &'static str, check: &Check<T>) -> (&'static str, CheckView) {
    let observed = serde_json::to_string(&check.observed)
        .expect("closed Doctor observations must serialize to JSON");
    (
        name,
        CheckView {
            status: check.status,
            code: check.code,
            message: check.message,
            observed,
        },
    )
}

fn overall_status(statuses: impl Iterator<Item = CheckStatus>) -> OverallStatus {
    let mut overall = OverallStatus::Ok;
    for status in statuses {
        match status {
            CheckStatus::Error => return OverallStatus::Error,
            CheckStatus::Warning => overall = OverallStatus::Warning,
            CheckStatus::Ok | CheckStatus::NotApplicable => {}
        }
    }
    overall
}

#[must_use]
pub fn render_json(report: &DoctorReportV1) -> String {
    serde_json::to_string(report).expect("closed Doctor report must serialize to JSON")
}

#[must_use]
pub fn render_human(report: &DoctorReportV1) -> String {
    let mut rendered = format!(
        "herdr-top doctor schema_version={} overall_status={}\n",
        report.schema_version,
        overall_name(report.overall_status)
    );
    for (name, check) in report.checks() {
        rendered.push_str(&format!(
            "{name}: {} [{}] {} observed={}\n",
            status_name(check.status),
            check.code,
            check.message,
            check.observed
        ));
    }
    rendered
}

const fn status_name(status: CheckStatus) -> &'static str {
    match status {
        CheckStatus::Ok => "ok",
        CheckStatus::Warning => "warning",
        CheckStatus::Error => "error",
        CheckStatus::NotApplicable => "not_applicable",
    }
}

const fn overall_name(status: OverallStatus) -> &'static str {
    match status {
        OverallStatus::Ok => "ok",
        OverallStatus::Warning => "warning",
        OverallStatus::Error => "error",
    }
}

fn check<T>(status: CheckStatus, code: &'static str, observed: Option<T>) -> Check<T> {
    Check {
        status,
        code,
        message: message(code),
        observed,
    }
}

fn message(code: &str) -> &'static str {
    match code {
        "session_resolved" => "session resolved",
        "session_unresolved" => "session could not be resolved",
        "session_name_matches" => "session name record matches",
        "session_name_absent" => "session name record is absent",
        "session_name_mismatch" => "session name record does not match",
        "session_name_unsafe" => "session name record is unsafe",
        "state_base_resolved" => "state base resolved",
        "state_base_unresolved" => "state base could not be resolved",
        "state_root_resolved" => "state root resolved",
        "state_root_unresolved" => "state root could not be resolved",
        "breadcrumb_matches" => "plugin breadcrumb matches",
        "breadcrumb_absent" => "plugin breadcrumb is absent",
        "breadcrumb_other_root" => "plugin breadcrumb points to another root",
        "breadcrumb_malformed" => "plugin breadcrumb is malformed",
        "breadcrumb_unreadable" => "plugin breadcrumb is unreadable",
        "controller_path_resolved" => "Controller path resolved",
        "controller_path_unresolved" => "Controller path could not be resolved",
        "controller_path_fits" => "Controller path fits platform limit",
        "controller_path_too_long" => "Controller path exceeds platform limit",
        "herdr_reachable" => "Herdr endpoint is reachable",
        "herdr_unavailable" => "Herdr endpoint is unavailable",
        "herdr_pong_valid" => "Herdr Pong is valid",
        "herdr_pong_malformed" => "Herdr Pong is malformed",
        "controller_live_healthy" => "Controller is live and healthy",
        "controller_live_degraded" => "Controller is live with degraded persistence",
        "controller_unavailable" => "Controller status is unavailable",
        "controller_live" => "Controller rendezvous is live",
        "controller_absent" => "Controller rendezvous is absent",
        "controller_stale" => "Controller rendezvous is stale",
        "controller_no_live_endpoint" => "Controller rendezvous has no live endpoint",
        "controller_orphan_socket" => "Controller socket has no trusted sentinel",
        "controller_malformed_sentinel" => "Controller sentinel is malformed or unreadable",
        "controller_collision" => "Controller sentinel names another session",
        "lock_free" => "owner lock is free",
        "lock_held_with_owner" => "owner lock has an owner record",
        "lock_held_without_owner" => "owner lock has no owner record",
        "lock_unsafe" => "owner lock is unsafe",
        "database_current" => "database schema is current",
        "database_absent" => "database is absent",
        "database_migratable" => "database schema is migratable",
        "database_newer" => "database schema is newer",
        "database_corrupt" => "database is corrupt",
        "database_unreadable" => "database is unreadable",
        "provider_roots_found" => "provider roots found",
        "provider_roots_missing" => "provider roots are missing",
        "provider_roots_partial" => "provider roots are partial",
        "herdr_compatible" => "Herdr is compatible",
        "herdr_below_floor" => "Herdr is below the minimum version",
        "herdr_protocol_mismatch" => "Herdr protocol differs",
        "herdr_version_unparseable" => "Herdr version is unavailable",
        "integrations_current" => "provider integrations are current",
        "integration_missing" => "a provider integration is missing",
        "integration_version_unavailable" => "a provider integration version is unavailable",
        "integration_below_floor" => "a provider integration is below its minimum",
        "standalone_exact" => "standalone version matches",
        "standalone_missing" => "standalone executable is missing",
        "standalone_mismatch" => "standalone version differs",
        "standalone_unparseable" => "standalone version is unavailable",
        "standalone_timeout" => "standalone version query timed out",
        "standalone_stderr_present" => "standalone version query used stderr",
        "provider_cli_version_available" => "provider CLI versions are available",
        "provider_cli_missing" => "a provider CLI is missing",
        "provider_cli_unparseable" => "a provider CLI version is unavailable",
        "provider_cli_timeout" => "a provider CLI version query timed out",
        "provider_cli_stderr_present" => "a provider CLI version query used stderr",
        "coverage_complete" => "native-session coverage is complete",
        "coverage_zero_relevant" => "no relevant native sessions were found",
        "coverage_partial" => "native-session coverage is partial",
        "log_path_resolved" => "log path resolved",
        "log_absent" => "log is absent",
        "log_unreadable" => "log is unreadable",
        "no_persistence_occurrence" => "no persistence occurrence found",
        "historical_persistence_occurrence" => "historical persistence occurrence found",
        "persistence_log_malformed_or_irrelevant" => "persistence log has no valid occurrence",
        _ => panic!("unknown Doctor code: {code}"),
    }
}

fn herdr_compatibility_check(pong: Option<&Pong>) -> Check<HerdrCompatibilityObservation> {
    let Some(pong) = pong else {
        return check(CheckStatus::Error, "herdr_version_unparseable", None);
    };
    let assessment = remote::assess_herdr_compatibility(pong);
    let normalized = match &assessment {
        HerdrCompatibility::Compatible { version } => Some(version.clone()),
        HerdrCompatibility::Unavailable { version, .. } => version.clone(),
    };
    let observed = normalized.map(|found_version| HerdrCompatibilityObservation {
        found_version,
        minimum_version: remote::MINIMUM_HERDR_VERSION.to_owned(),
        found_protocol: pong.protocol,
        supported_protocols: remote::SUPPORTED_HERDR_PROTOCOLS.to_vec(),
    });
    match assessment {
        HerdrCompatibility::Compatible { .. } => {
            check(CheckStatus::Ok, "herdr_compatible", observed)
        }
        HerdrCompatibility::Unavailable { reason, .. } => match reason {
            HerdrCompatibilityIssue::InvalidVersion => {
                check(CheckStatus::Error, "herdr_version_unparseable", None)
            }
            HerdrCompatibilityIssue::VersionTooOld => {
                check(CheckStatus::Error, "herdr_below_floor", observed)
            }
            HerdrCompatibilityIssue::ProtocolMismatch => {
                check(CheckStatus::Error, "herdr_protocol_mismatch", observed)
            }
        },
    }
}

fn integration_check(
    assessments: &[OfficialIntegrationAssessment],
) -> Check<Vec<IntegrationObservation>> {
    let mut rows = assessments.to_vec();
    rows.sort_by_key(|assessment| DoctorProvider::from(assessment.provider));
    let observed = rows
        .iter()
        .map(|assessment| IntegrationObservation {
            provider: assessment.provider.into(),
            active_version: assessment.active_version.clone(),
            minimum_version: assessment.minimum_version.clone(),
        })
        .collect::<Vec<_>>();
    let reasons = rows
        .iter()
        .filter_map(|assessment| match assessment.status {
            OfficialIntegrationStatus::Compatible => None,
            OfficialIntegrationStatus::Unavailable { reason } => Some(reason),
        })
        .collect::<Vec<_>>();
    let code = if reasons.contains(&OfficialIntegrationUnavailableReason::MissingManifest) {
        "integration_missing"
    } else if reasons.iter().any(|reason| {
        matches!(
            reason,
            OfficialIntegrationUnavailableReason::MissingActiveVersion
                | OfficialIntegrationUnavailableReason::InvalidActiveVersion
                | OfficialIntegrationUnavailableReason::DuplicateManifest
        )
    }) {
        "integration_version_unavailable"
    } else if reasons.contains(&OfficialIntegrationUnavailableReason::BelowMinimum) {
        "integration_below_floor"
    } else {
        "integrations_current"
    };
    let status = if reasons.is_empty() {
        CheckStatus::Ok
    } else {
        CheckStatus::Warning
    };
    check(status, code, Some(observed))
}

fn standalone_check(status: StandaloneVersionStatus) -> Check<ExecutableVersionObservation> {
    let expected = Some(env!("CARGO_PKG_VERSION").to_owned());
    match status {
        StandaloneVersionStatus::Compatible {
            version,
            stderr_present,
        } => check(
            if stderr_present {
                CheckStatus::Warning
            } else {
                CheckStatus::Ok
            },
            if stderr_present {
                "standalone_stderr_present"
            } else {
                "standalone_exact"
            },
            Some(ExecutableVersionObservation {
                component: "herdr-top".to_owned(),
                found_version: Some(version),
                expected_version: expected,
                stderr_present,
            }),
        ),
        StandaloneVersionStatus::Mismatch {
            version,
            stderr_present,
        } => check(
            CheckStatus::Warning,
            "standalone_mismatch",
            Some(ExecutableVersionObservation {
                component: "herdr-top".to_owned(),
                found_version: Some(version),
                expected_version: expected,
                stderr_present,
            }),
        ),
        StandaloneVersionStatus::Unavailable { reason } => check(
            CheckStatus::Warning,
            standalone_failure_code(reason),
            Some(ExecutableVersionObservation {
                component: "herdr-top".to_owned(),
                found_version: None,
                expected_version: expected,
                stderr_present: false,
            }),
        ),
    }
}

fn standalone_failure_code(reason: VersionProbeFailure) -> &'static str {
    match reason {
        VersionProbeFailure::NotFound | VersionProbeFailure::SpawnFailed => "standalone_missing",
        VersionProbeFailure::TimedOut => "standalone_timeout",
        VersionProbeFailure::ExecutionFailed
        | VersionProbeFailure::UnsuccessfulExit
        | VersionProbeFailure::OutputReadFailed
        | VersionProbeFailure::OutputTooLarge
        | VersionProbeFailure::InvalidOutput => "standalone_unparseable",
    }
}

fn provider_cli_check(
    statuses: &[(Provider, ProviderCliVersionStatus)],
) -> Check<Vec<ExecutableVersionObservation>> {
    let mut rows = statuses
        .iter()
        .map(|(provider, status)| {
            let component = provider_name(*provider).to_owned();
            match status {
                ProviderCliVersionStatus::Informational {
                    version,
                    stderr_present,
                } => (
                    DoctorProvider::from(*provider),
                    if *stderr_present {
                        Some("provider_cli_stderr_present")
                    } else {
                        None
                    },
                    ExecutableVersionObservation {
                        component,
                        found_version: Some(version.clone()),
                        expected_version: None,
                        stderr_present: *stderr_present,
                    },
                ),
                ProviderCliVersionStatus::Unavailable { reason } => (
                    DoctorProvider::from(*provider),
                    Some(provider_cli_failure_code(*reason)),
                    ExecutableVersionObservation {
                        component,
                        found_version: None,
                        expected_version: None,
                        stderr_present: false,
                    },
                ),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(provider, _, _)| *provider);
    let code = [
        "provider_cli_missing",
        "provider_cli_unparseable",
        "provider_cli_timeout",
        "provider_cli_stderr_present",
    ]
    .into_iter()
    .find(|candidate| {
        rows.iter()
            .any(|(_, code, _)| code.as_deref() == Some(*candidate))
    })
    .unwrap_or("provider_cli_version_available");
    let status = if code == "provider_cli_version_available" {
        CheckStatus::Ok
    } else {
        CheckStatus::Warning
    };
    check(
        status,
        code,
        Some(rows.into_iter().map(|(_, _, row)| row).collect()),
    )
}

fn provider_cli_failure_code(reason: VersionProbeFailure) -> &'static str {
    match reason {
        VersionProbeFailure::NotFound | VersionProbeFailure::SpawnFailed => "provider_cli_missing",
        VersionProbeFailure::TimedOut => "provider_cli_timeout",
        VersionProbeFailure::ExecutionFailed
        | VersionProbeFailure::UnsuccessfulExit
        | VersionProbeFailure::OutputReadFailed
        | VersionProbeFailure::OutputTooLarge
        | VersionProbeFailure::InvalidOutput => "provider_cli_unparseable",
    }
}

fn provider_name(provider: Provider) -> &'static str {
    match provider {
        Provider::Claude => "claude",
        Provider::Codex => "codex",
    }
}

fn provider_from_name(name: &str) -> Option<Provider> {
    let normalized = name.to_ascii_lowercase();
    if normalized.contains("codex") {
        Some(Provider::Codex)
    } else if normalized.contains("claude") {
        Some(Provider::Claude)
    } else {
        None
    }
}

fn coverage_check(snapshot: Option<&Snapshot>) -> Check<CoverageObservation> {
    let mut rows = [
        ProviderCoverageCounts {
            provider: DoctorProvider::Claude,
            covered: 0,
            partial: 0,
            uncovered: 0,
            total: 0,
        },
        ProviderCoverageCounts {
            provider: DoctorProvider::Codex,
            covered: 0,
            partial: 0,
            uncovered: 0,
            total: 0,
        },
    ];
    let mut unknown_provider = 0_u64;
    if let Some(snapshot) = snapshot {
        for pane in &snapshot.panes {
            let Some(agent) = pane.agent.as_deref() else {
                continue;
            };
            let Some(provider) = provider_from_name(agent) else {
                unknown_provider = unknown_provider.saturating_add(1);
                continue;
            };
            let index = match provider {
                Provider::Claude => 0,
                Provider::Codex => 1,
            };
            let row = &mut rows[index];
            row.total = row.total.saturating_add(1);
            match pane.agent_session.as_ref() {
                Some(reference) if provider_from_name(&reference.agent) == Some(provider) => {
                    match reference.kind {
                        AgentSessionKind::Id => row.covered = row.covered.saturating_add(1),
                        AgentSessionKind::Path => row.partial = row.partial.saturating_add(1),
                    }
                }
                _ => row.uncovered = row.uncovered.saturating_add(1),
            }
        }
    }
    let observation = CoverageObservation {
        covered: rows.iter().map(|row| row.covered).sum(),
        partial: rows.iter().map(|row| row.partial).sum(),
        uncovered: rows.iter().map(|row| row.uncovered).sum(),
        total: rows.iter().map(|row| row.total).sum(),
        unknown_provider,
        by_provider: rows.into_iter().collect(),
    };
    if observation.total == 0 {
        check(
            CheckStatus::NotApplicable,
            "coverage_zero_relevant",
            Some(observation),
        )
    } else if observation.partial > 0 || observation.uncovered > 0 {
        check(CheckStatus::Warning, "coverage_partial", Some(observation))
    } else {
        check(CheckStatus::Ok, "coverage_complete", Some(observation))
    }
}

fn controller_runtime_check(
    verdict: ControllerRuntimeProbe,
    snapshot: Option<RuntimeDiagnosticsSnapshot>,
) -> Check<RuntimeDiagnosticsSnapshot> {
    if verdict != ControllerRuntimeProbe::Live {
        return check(CheckStatus::Warning, "controller_unavailable", None);
    }
    match snapshot {
        Some(snapshot) => {
            let (status, code) = match (&snapshot.persistence, &snapshot.controller_input) {
                (PersistenceStatus::Degraded { .. }, _) => {
                    (CheckStatus::Warning, "controller_live_degraded")
                }
                (PersistenceStatus::Healthy, ControllerInputStatus::Available) => {
                    (CheckStatus::Ok, "controller_live_healthy")
                }
                (PersistenceStatus::Healthy, ControllerInputStatus::Unavailable { .. }) => {
                    (CheckStatus::Warning, "controller_unavailable")
                }
            };
            check(status, code, Some(snapshot))
        }
        None => check(CheckStatus::Warning, "controller_unavailable", None),
    }
}

fn persistence_occurrence_check(
    verdict: PersistenceLogVerdict,
) -> Check<PersistenceOccurrenceObservation> {
    match verdict {
        PersistenceLogVerdict::Absent => check(CheckStatus::Ok, "no_persistence_occurrence", None),
        PersistenceLogVerdict::Unreadable | PersistenceLogVerdict::NoValidRecord => check(
            CheckStatus::Warning,
            "persistence_log_malformed_or_irrelevant",
            None,
        ),
        PersistenceLogVerdict::Found(observed) => check(
            CheckStatus::Warning,
            "historical_persistence_occurrence",
            Some(PersistenceOccurrenceObservation {
                timestamp_ms: observed.timestamp_ms,
                failure: observed.failure,
                counters: observed.counters,
            }),
        ),
    }
}

fn rendezvous_check(verdict: ControllerRuntimeProbe) -> Check<ControllerRendezvousObservation> {
    match verdict {
        ControllerRuntimeProbe::Absent => check(
            CheckStatus::NotApplicable,
            "controller_absent",
            Some(ControllerRendezvousObservation::Absent),
        ),
        ControllerRuntimeProbe::Live => check(
            CheckStatus::Ok,
            "controller_live",
            Some(ControllerRendezvousObservation::Live),
        ),
        ControllerRuntimeProbe::Stale => check(
            CheckStatus::Warning,
            "controller_stale",
            Some(ControllerRendezvousObservation::Stale),
        ),
        ControllerRuntimeProbe::NoLiveEndpoint => check(
            CheckStatus::Warning,
            "controller_no_live_endpoint",
            Some(ControllerRendezvousObservation::Stale),
        ),
        ControllerRuntimeProbe::OrphanSocket => check(
            CheckStatus::Error,
            "controller_orphan_socket",
            Some(ControllerRendezvousObservation::OrphanSocket),
        ),
        ControllerRuntimeProbe::Collision => check(
            CheckStatus::Error,
            "controller_collision",
            Some(ControllerRendezvousObservation::Collision),
        ),
        ControllerRuntimeProbe::PathTooLong { .. } => check(
            CheckStatus::Error,
            "controller_path_too_long",
            Some(ControllerRendezvousObservation::PathTooLong),
        ),
        ControllerRuntimeProbe::MalformedSentinel
        | ControllerRuntimeProbe::MalformedSocket
        | ControllerRuntimeProbe::Unreadable => check(
            CheckStatus::Error,
            "controller_malformed_sentinel",
            Some(ControllerRendezvousObservation::MalformedSentinel),
        ),
    }
}

fn owner_lock_check(verdict: OwnerLockVerdict) -> Check<LockObservation> {
    match verdict {
        OwnerLockVerdict::Missing | OwnerLockVerdict::Available => {
            check(CheckStatus::Ok, "lock_free", Some(LockObservation::Free))
        }
        OwnerLockVerdict::HeldWithOwner => check(
            CheckStatus::Ok,
            "lock_held_with_owner",
            Some(LockObservation::HeldWithOwner),
        ),
        OwnerLockVerdict::HeldWithoutOwner => check(
            CheckStatus::Warning,
            "lock_held_without_owner",
            Some(LockObservation::HeldWithoutOwner),
        ),
        OwnerLockVerdict::HeldOwnerUnreadable
        | OwnerLockVerdict::MalformedOrUnsafe
        | OwnerLockVerdict::Unreadable => check(
            CheckStatus::Error,
            "lock_unsafe",
            Some(LockObservation::Unsafe),
        ),
    }
}

fn schema_check(verdict: SchemaProbeVerdict) -> Check<DatabaseSchemaObservation> {
    let observation = |found: Option<i64>, supported: i64| DatabaseSchemaObservation {
        found: found.and_then(|value| u32::try_from(value).ok()),
        supported: u32::try_from(supported).unwrap_or(u32::MAX),
    };
    match verdict {
        SchemaProbeVerdict::Absent { supported } => check(
            CheckStatus::NotApplicable,
            "database_absent",
            Some(observation(None, supported)),
        ),
        SchemaProbeVerdict::Current { found, supported } => check(
            CheckStatus::Ok,
            "database_current",
            Some(observation(Some(found), supported)),
        ),
        SchemaProbeVerdict::Migratable { found, supported } => check(
            CheckStatus::Warning,
            "database_migratable",
            Some(observation(Some(found), supported)),
        ),
        SchemaProbeVerdict::Newer { found, supported } => check(
            CheckStatus::Error,
            "database_newer",
            Some(observation(Some(found), supported)),
        ),
        SchemaProbeVerdict::Corrupt { supported } => check(
            CheckStatus::Error,
            "database_corrupt",
            Some(observation(None, supported)),
        ),
        SchemaProbeVerdict::Unreadable { supported } => check(
            CheckStatus::Error,
            "database_unreadable",
            Some(observation(None, supported)),
        ),
    }
}

fn provider_roots_check(home: Option<&OsStr>) -> Check<Vec<ProviderRootObservation>> {
    let mut roots = provider::standard_discovery_roots(home)
        .into_iter()
        .map(|root| ProviderRootObservation {
            provider: root.provider.into(),
            path: local::format_operational_path(&root.path, home),
            exists: std::fs::symlink_metadata(&root.path)
                .is_ok_and(|metadata| metadata.file_type().is_dir()),
        })
        .collect::<Vec<_>>();
    roots.sort_by_key(|root| root.provider);
    let found = roots.iter().filter(|root| root.exists).count();
    let (status, code) = if found == 2 && roots.len() == 2 {
        (CheckStatus::Ok, "provider_roots_found")
    } else if found == 0 {
        (CheckStatus::Warning, "provider_roots_missing")
    } else {
        (CheckStatus::Warning, "provider_roots_partial")
    };
    check(status, code, Some(roots))
}

fn name_record_check(
    root: Option<(&StateRoot, &session_key::SessionKey)>,
) -> Check<NameRecordObservation> {
    let Some((root, key)) = root else {
        return check(CheckStatus::NotApplicable, "session_name_absent", None);
    };
    match local::probe_name_record(root, key) {
        NameRecordVerdict::Absent => check(
            CheckStatus::NotApplicable,
            "session_name_absent",
            Some(NameRecordObservation::Absent),
        ),
        NameRecordVerdict::Matches => check(
            CheckStatus::Ok,
            "session_name_matches",
            Some(NameRecordObservation::Matches),
        ),
        NameRecordVerdict::Mismatch => check(
            CheckStatus::Error,
            "session_name_mismatch",
            Some(NameRecordObservation::Mismatch),
        ),
        NameRecordVerdict::Unsafe | NameRecordVerdict::Unreadable => check(
            CheckStatus::Error,
            "session_name_unsafe",
            Some(NameRecordObservation::Unsafe),
        ),
    }
}

fn breadcrumb_check(
    root: Option<&StateRoot>,
    plugin_state_dir: Option<&OsStr>,
) -> Check<BreadcrumbObservation> {
    let Some(root) = root else {
        return check(CheckStatus::NotApplicable, "breadcrumb_absent", None);
    };
    match local::probe_plugin_breadcrumb(plugin_state_dir, root) {
        BreadcrumbVerdict::Absent => check(
            CheckStatus::NotApplicable,
            "breadcrumb_absent",
            Some(BreadcrumbObservation::Absent),
        ),
        BreadcrumbVerdict::Matches => check(
            CheckStatus::Ok,
            "breadcrumb_matches",
            Some(BreadcrumbObservation::Matches),
        ),
        BreadcrumbVerdict::OtherRoot => check(
            CheckStatus::Warning,
            "breadcrumb_other_root",
            Some(BreadcrumbObservation::OtherRoot),
        ),
        BreadcrumbVerdict::Malformed => check(
            CheckStatus::Warning,
            "breadcrumb_malformed",
            Some(BreadcrumbObservation::Malformed),
        ),
        BreadcrumbVerdict::Unreadable => check(
            CheckStatus::Warning,
            "breadcrumb_unreadable",
            Some(BreadcrumbObservation::Unreadable),
        ),
    }
}

#[derive(Clone, Debug, Deserialize)]
struct VersionFixture {
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    stderr_present: bool,
    #[serde(default)]
    failure: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct VersionFixtures {
    standalone: VersionFixture,
    claude: VersionFixture,
    codex: VersionFixture,
}

#[derive(Clone, Debug)]
enum VersionRunnerMode {
    Production(remote::BoundedVersionCommandRunner),
    Fixtures(VersionFixtures),
    InvalidFixtures,
}

#[derive(Clone, Debug)]
pub struct DoctorVersionRunner {
    mode: VersionRunnerMode,
}

impl DoctorVersionRunner {
    #[must_use]
    pub fn from_environment() -> Self {
        if let Some(fixtures) = env::var_os(VERSION_FIXTURE_ENV) {
            let mode = fixtures
                .to_str()
                .and_then(|fixtures| serde_json::from_str(fixtures).ok())
                .map_or(
                    VersionRunnerMode::InvalidFixtures,
                    VersionRunnerMode::Fixtures,
                );
            return Self { mode };
        }
        Self {
            mode: VersionRunnerMode::Production(remote::BoundedVersionCommandRunner::default()),
        }
    }
}

impl VersionCommandRunner for DoctorVersionRunner {
    fn run(&self, command: VersionCommand) -> VersionProbeResult {
        match &self.mode {
            VersionRunnerMode::Production(runner) => runner.run(command),
            VersionRunnerMode::InvalidFixtures => VersionProbeResult::Unavailable {
                reason: VersionProbeFailure::InvalidOutput,
            },
            VersionRunnerMode::Fixtures(fixtures) => {
                let fixture = match command {
                    VersionCommand::Standalone => &fixtures.standalone,
                    VersionCommand::Claude => &fixtures.claude,
                    VersionCommand::Codex => &fixtures.codex,
                };
                if let Some(version) = &fixture.version {
                    VersionProbeResult::Available {
                        version: version.clone(),
                        stderr_present: fixture.stderr_present,
                    }
                } else {
                    VersionProbeResult::Unavailable {
                        reason: fixture
                            .failure
                            .as_deref()
                            .and_then(version_failure)
                            .unwrap_or(VersionProbeFailure::InvalidOutput),
                    }
                }
            }
        }
    }
}

fn version_failure(value: &str) -> Option<VersionProbeFailure> {
    match value {
        "not_found" => Some(VersionProbeFailure::NotFound),
        "spawn_failed" => Some(VersionProbeFailure::SpawnFailed),
        "timed_out" => Some(VersionProbeFailure::TimedOut),
        "execution_failed" => Some(VersionProbeFailure::ExecutionFailed),
        "unsuccessful_exit" => Some(VersionProbeFailure::UnsuccessfulExit),
        "output_read_failed" => Some(VersionProbeFailure::OutputReadFailed),
        "output_too_large" => Some(VersionProbeFailure::OutputTooLarge),
        "invalid_output" => Some(VersionProbeFailure::InvalidOutput),
        _ => None,
    }
}

pub async fn collect_report(
    session_flag: Option<&str>,
    socket_override: Option<&Path>,
    runner: &impl VersionCommandRunner,
) -> DoctorReportV1 {
    let home = env::var_os("HOME");
    let xdg_state_home = env::var_os("XDG_STATE_HOME");
    let plugin_state_dir = env::var_os("HERDR_PLUGIN_STATE_DIR");
    let environment_session = env::var("HERDR_SESSION").ok();
    let managed_pane = env::var("HERDR_ENV").is_ok_and(|value| value == "1");
    let resolved = session_key::resolve(session_flag, environment_session.as_deref(), managed_pane);
    let state_base = lockfile::resolve_state_base(xdg_state_home.as_deref(), home.as_deref());
    let root = resolved.as_ref().ok().and_then(|resolved| {
        state_base
            .as_ref()
            .ok()
            .map(|base| lockfile::derive_state_root(base, resolved.session_key()))
    });

    let session_resolution = match &resolved {
        Ok(resolved) => check(
            CheckStatus::Ok,
            "session_resolved",
            Some(SessionResolutionObservation {
                source: resolution_source(resolved),
            }),
        ),
        Err(_) => check(CheckStatus::Error, "session_unresolved", None),
    };
    let state_base_check = match &state_base {
        Ok(base) => check(
            CheckStatus::Ok,
            "state_base_resolved",
            Some(local::format_operational_path(base, home.as_deref())),
        ),
        Err(_) => check(CheckStatus::Error, "state_base_unresolved", None),
    };
    let state_root_check = match &root {
        Some(root) => check(
            CheckStatus::Ok,
            "state_root_resolved",
            Some(local::format_operational_path(&root.0, home.as_deref())),
        ),
        None => check(CheckStatus::Error, "state_root_unresolved", None),
    };
    let name_record = name_record_check(
        root.as_ref()
            .zip(resolved.as_ref().ok().map(ResolvedSession::session_key)),
    );
    let breadcrumb = breadcrumb_check(root.as_ref(), plugin_state_dir.as_deref());

    let runtime_base = rendezvous::runtime_base(
        env::var_os("XDG_RUNTIME_DIR").as_deref(),
        env::var_os("TMPDIR").as_deref(),
        rendezvous::effective_uid(),
    );
    let controller_paths = resolved.as_ref().ok().map(|resolved| {
        rendezvous::controller_runtime_paths(&runtime_base, resolved.session_key())
    });
    let controller_socket = controller_paths.as_ref().map_or_else(
        || check(CheckStatus::Error, "controller_path_unresolved", None),
        |paths| {
            check(
                CheckStatus::Ok,
                "controller_path_resolved",
                Some(local::format_operational_path(
                    &paths.socket,
                    home.as_deref(),
                )),
            )
        },
    );
    let socket_length = controller_paths.as_ref().map_or_else(
        || check(CheckStatus::Error, "controller_path_too_long", None),
        |paths| match rendezvous::socket_path_length(&paths.socket) {
            SocketPathLength::Fits { bytes, limit } => check(
                CheckStatus::Ok,
                "controller_path_fits",
                Some(PathLengthObservation {
                    bytes: bytes as u64,
                    limit: limit as u64,
                }),
            ),
            SocketPathLength::TooLong { bytes, limit } => check(
                CheckStatus::Error,
                "controller_path_too_long",
                Some(PathLengthObservation {
                    bytes: bytes as u64,
                    limit: limit as u64,
                }),
            ),
        },
    );
    let runtime_verdict = resolved
        .as_ref()
        .ok()
        .map_or(ControllerRuntimeProbe::Absent, |resolved| {
            local::probe_runtime(&runtime_base, resolved.session_key())
        });

    let herdr_socket = socket_override.map(Path::to_path_buf).or_else(|| {
        env::var_os("HERDR_SOCKET_PATH")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    });
    let (pong_result, manifests_result, snapshot_result) = if let Some(socket) = &herdr_socket {
        let (pong, manifests, snapshot) =
            tokio::join!(wire::ping(socket), wire::agent_manifests(socket), async {
                wire::request(socket, "session.snapshot", serde_json::json!({}))
                    .await
                    .and_then(wire::WireResult::into_snapshot)
            });
        (Some(pong), Some(manifests), Some(snapshot))
    } else {
        (None, None, None)
    };
    let pong = pong_result.as_ref().and_then(|result| result.as_ref().ok());
    let endpoint_reachable = pong_result.as_ref().is_some_and(|result| match result {
        Ok(_) => true,
        Err(
            wire::WireError::MalformedFrame(_)
            | wire::WireError::Server { .. }
            | wire::WireError::UnexpectedResponse(_),
        ) => true,
        Err(wire::WireError::Io(_) | wire::WireError::Timeout { .. }) => false,
    });
    let endpoint = check(
        if endpoint_reachable {
            CheckStatus::Ok
        } else {
            CheckStatus::Error
        },
        if endpoint_reachable {
            "herdr_reachable"
        } else {
            "herdr_unavailable"
        },
        Some(EndpointObservation {
            reachable: endpoint_reachable,
        }),
    );
    let herdr_compatibility = herdr_compatibility_check(pong);
    let pong_check = pong.map_or_else(
        || check(CheckStatus::Error, "herdr_pong_malformed", None),
        |pong| {
            let observed =
                herdr_compatibility
                    .observed
                    .as_ref()
                    .map(|compatibility| PongObservation {
                        version: compatibility.found_version.clone(),
                        protocol_version: pong.protocol,
                    });
            check(CheckStatus::Ok, "herdr_pong_valid", observed)
        },
    );

    let integration_assessments = integration_assessments(
        manifests_result
            .as_ref()
            .and_then(|result| result.as_ref().ok()),
    );
    let integrations = integration_check(&integration_assessments);
    let coverage = coverage_check(
        snapshot_result
            .as_ref()
            .and_then(|result| result.as_ref().ok()),
    );

    let controller_snapshot = if runtime_verdict == ControllerRuntimeProbe::Live {
        match controller_paths.as_ref() {
            Some(paths) => controller::query_status(&paths.socket)
                .await
                .ok()
                .and_then(|value| parse_runtime_status(&value)),
            None => None,
        }
    } else {
        None
    };
    let controller_runtime = controller_runtime_check(runtime_verdict, controller_snapshot);
    let rendezvous = rendezvous_check(runtime_verdict);

    let (lock, database, log_location, persistence_occurrence) = match &root {
        Some(root) => {
            let log_verdict = local::probe_persistence_log(root);
            let log_path = local::format_operational_path(&root.0.join(LOG_FILE), home.as_deref());
            let location = match log_verdict {
                PersistenceLogVerdict::Absent => {
                    check(CheckStatus::NotApplicable, "log_absent", Some(log_path))
                }
                PersistenceLogVerdict::Unreadable => {
                    check(CheckStatus::Warning, "log_unreadable", Some(log_path))
                }
                PersistenceLogVerdict::NoValidRecord | PersistenceLogVerdict::Found(_) => {
                    check(CheckStatus::Ok, "log_path_resolved", Some(log_path))
                }
            };
            (
                owner_lock_check(local::probe_owner_lock(root)),
                schema_check(local::probe_schema(root)),
                location,
                persistence_occurrence_check(log_verdict),
            )
        }
        None => {
            let supported = crate::store::schema::supported_schema_version();
            (
                check(CheckStatus::NotApplicable, "lock_free", None),
                schema_check(SchemaProbeVerdict::Absent { supported }),
                check(CheckStatus::NotApplicable, "log_absent", None),
                check(
                    CheckStatus::NotApplicable,
                    "no_persistence_occurrence",
                    None,
                ),
            )
        }
    };

    let standalone = standalone_check(remote::standalone_version_status(runner));
    let provider_clis = provider_cli_check(&[
        (
            Provider::Claude,
            remote::provider_cli_version_status(runner, Provider::Claude),
        ),
        (
            Provider::Codex,
            remote::provider_cli_version_status(runner, Provider::Codex),
        ),
    ]);

    let mut report = DoctorReportV1 {
        schema_version: SCHEMA_VERSION,
        overall_status: OverallStatus::Ok,
        session: SessionSection {
            resolution: session_resolution,
            name_record,
        },
        paths: PathsSection {
            state_base: state_base_check,
            state_root: state_root_check,
            breadcrumb,
            controller_socket,
            socket_length,
        },
        herdr: HerdrSection {
            endpoint,
            pong: pong_check,
        },
        controller: ControllerSection {
            runtime: controller_runtime,
            rendezvous,
        },
        lock: LockSection { ownership: lock },
        database: DatabaseSection { schema: database },
        providers: ProvidersSection {
            discovery: provider_roots_check(home.as_deref()),
        },
        compatibility: CompatibilitySection {
            herdr: herdr_compatibility,
            integrations,
            standalone,
            provider_clis,
        },
        coverage: CoverageSection {
            native_sessions: coverage,
        },
        logs: LogsSection {
            location: log_location,
            persistence_occurrence,
        },
    };
    report.recompute_overall_status();
    report
}

fn resolution_source(resolved: &ResolvedSession) -> SessionResolutionSource {
    match resolved.source() {
        ResolverSource::ExplicitFlag => SessionResolutionSource::Flag,
        ResolverSource::Environment => SessionResolutionSource::Environment,
        ResolverSource::ManagedPaneDefault => SessionResolutionSource::ManagedDefault,
    }
}

fn integration_assessments(
    manifests: Option<&AgentManifestStatus>,
) -> Vec<OfficialIntegrationAssessment> {
    let empty_manifests = AgentManifestStatus {
        result_type: "agent_manifest_status".to_owned(),
        manifests: Vec::new(),
        last_check_unix: None,
        last_result: None,
    };
    let manifests = manifests.unwrap_or(&empty_manifests);
    [Provider::Claude, Provider::Codex]
        .into_iter()
        .map(|provider| remote::assess_official_integration(manifests, provider))
        .collect()
}

fn parse_runtime_status(response: &Value) -> Option<RuntimeDiagnosticsSnapshot> {
    let response = response.as_object()?;
    if response.len() != 3
        || response.get("status")?.as_str()? != "ok"
        || response.get("schema_version")?.as_u64()? != 1
    {
        return None;
    }
    parse_runtime_snapshot(response.get("diagnostics")?)
}

fn parse_runtime_snapshot(value: &Value) -> Option<RuntimeDiagnosticsSnapshot> {
    let object = value.as_object()?;
    if object.len() != 9 {
        return None;
    }
    let persistence = parse_persistence_status(object.get("persistence")?)?;
    let controller_input = parse_controller_input(object.get("controller_input")?)?;
    let owner = match object.get("owner")?.as_str()? {
        "current" => OwnerFreshness::Current,
        "stale" => OwnerFreshness::Stale,
        _ => return None,
    };
    let persistence_counters = parse_persistence_counters(object.get("persistence_counters")?)?;
    let controller_counters = parse_controller_counters(object.get("controller_counters")?)?;
    let enrichment_counters = parse_enrichment_counters(object.get("enrichment_counters")?)?;
    let mut source_coverage = object
        .get("source_coverage")?
        .as_array()?
        .iter()
        .map(parse_source_coverage)
        .collect::<Option<Vec<_>>>()?;
    source_coverage.sort_by_key(|coverage| match coverage.source {
        DiagnosticSource::Herdr => 0,
        DiagnosticSource::Controller => 1,
        DiagnosticSource::Claude => 2,
        DiagnosticSource::Codex => 3,
    });
    let dangling_announcement_components =
        object.get("dangling_announcement_components")?.as_u64()?;
    let first_failure_log = match object.get("first_failure_log")?.as_str()? {
        "not_attempted" => OccurrenceLogStatus::NotAttempted,
        "emitted" => OccurrenceLogStatus::Emitted,
        "failed" => OccurrenceLogStatus::Failed,
        _ => return None,
    };
    Some(RuntimeDiagnosticsSnapshot {
        persistence,
        controller_input,
        owner,
        persistence_counters,
        controller_counters,
        enrichment_counters,
        source_coverage,
        dangling_announcement_components,
        first_failure_log,
    })
}

fn parse_enrichment_counters(value: &Value) -> Option<EnrichmentCounterSnapshot> {
    let object = value.as_object()?;
    if object.len() != 2 {
        return None;
    }
    Some(EnrichmentCounterSnapshot {
        channel_full_drops: object.get("channel_full_drops")?.as_u64()?,
        episode_discards: object.get("episode_discards")?.as_u64()?,
    })
}

fn parse_persistence_status(value: &Value) -> Option<PersistenceStatus> {
    let object = value.as_object()?;
    match object.get("status")?.as_str()? {
        "healthy" if object.len() == 1 => Some(PersistenceStatus::Healthy),
        "degraded" if object.len() == 2 => Some(PersistenceStatus::Degraded {
            failure: parse_persistence_failure(object.get("failure")?)?,
        }),
        _ => None,
    }
}

fn parse_persistence_failure(value: &Value) -> Option<PersistenceFailure> {
    let object = value.as_object()?;
    if object.len() != 4 {
        return None;
    }
    let operation = match object.get("operation")?.as_str()? {
        "apply" => PersistenceOperation::Apply,
        "cleanup" => PersistenceOperation::Cleanup,
        "update_owner_location" => PersistenceOperation::UpdateOwnerLocation,
        "replace_owner" => PersistenceOperation::ReplaceOwner,
        "barrier" => PersistenceOperation::Barrier,
        "checkpoint" => PersistenceOperation::Checkpoint,
        _ => return None,
    };
    let phase = match object.get("phase")?.as_str()? {
        "queue_admission" => PersistencePhase::QueueAdmission,
        "command_execution" => PersistencePhase::CommandExecution,
        "post_apply_commit" => PersistencePhase::PostApplyCommit,
        "acknowledgement" => PersistencePhase::Acknowledgement,
        _ => return None,
    };
    let code = match object.get("code")?.as_str()? {
        "sqlite" => PersistenceFailureCode::Sqlite,
        "io" => PersistenceFailureCode::Io,
        "invalid_data" => PersistenceFailureCode::InvalidData,
        "clock" => PersistenceFailureCode::Clock,
        "owner_absent" => PersistenceFailureCode::OwnerAbsent,
        "checkpoint_busy" => PersistenceFailureCode::CheckpointBusy,
        "channel_closed" => PersistenceFailureCode::ChannelClosed,
        "acknowledgement_dropped" => PersistenceFailureCode::AcknowledgementDropped,
        _ => return None,
    };
    let durability = match object.get("durability")?.as_str()? {
        "not_applicable" => DurabilityDisposition::NotApplicable,
        "not_committed" => DurabilityDisposition::NotCommitted,
        "committed" => DurabilityDisposition::Committed,
        "unknown" => DurabilityDisposition::Unknown,
        _ => return None,
    };
    Some(PersistenceFailure {
        operation,
        phase,
        code,
        durability,
    })
}

fn parse_controller_input(value: &Value) -> Option<ControllerInputStatus> {
    let object = value.as_object()?;
    match object.get("status")?.as_str()? {
        "available" if object.len() == 1 => Some(ControllerInputStatus::Available),
        "unavailable" if object.len() == 2 => {
            let reason = match object.get("reason")?.as_str()? {
                "listener_unavailable" => ControllerInputUnavailableReason::ListenerUnavailable,
                "runtime_unsafe" => ControllerInputUnavailableReason::RuntimeUnsafe,
                "persistence_unavailable" => {
                    ControllerInputUnavailableReason::PersistenceUnavailable
                }
                "acceptor_stopped" => ControllerInputUnavailableReason::AcceptorStopped,
                _ => return None,
            };
            Some(ControllerInputStatus::Unavailable { reason })
        }
        _ => None,
    }
}

fn parse_persistence_counters(value: &Value) -> Option<PersistenceCounters> {
    let object = value.as_object()?;
    if object.len() != 5 {
        return None;
    }
    Some(PersistenceCounters {
        not_committed_batches: object.get("not_committed_batches")?.as_u64()?,
        durability_unknown_batches: object.get("durability_unknown_batches")?.as_u64()?,
        committed_but_degraded_batches: object.get("committed_but_degraded_batches")?.as_u64()?,
        skipped_batches: object.get("skipped_batches")?.as_u64()?,
        skipped_owner_updates: object.get("skipped_owner_updates")?.as_u64()?,
    })
}

fn parse_controller_counters(value: &Value) -> Option<ControllerCounterSnapshot> {
    let object = value.as_object()?;
    if object.len() != 9 {
        return None;
    }
    Some(ControllerCounterSnapshot {
        binding_conflicts: object.get("binding_conflicts")?.as_u64()?,
        terminal_blocked_progress_noops: object.get("terminal_blocked_progress_noops")?.as_u64()?,
        terminal_forward_reference_creations: object
            .get("terminal_forward_reference_creations")?
            .as_u64()?,
        dangling_announcement_components: object
            .get("dangling_announcement_components")?
            .as_u64()?,
        ingest_sequence_exhaustions: object.get("ingest_sequence_exhaustions")?.as_u64()?,
        provider_parent_conflicts: object.get("provider_parent_conflicts")?.as_u64()?,
        provider_identity_disagreements: object.get("provider_identity_disagreements")?.as_u64()?,
        socket_saturations: object.get("socket_saturations")?.as_u64()?,
        accept_failures: object.get("accept_failures")?.as_u64()?,
    })
}

fn parse_source_coverage(value: &Value) -> Option<SourceCoverageSnapshot> {
    let object = value.as_object()?;
    if object.len() != 2 {
        return None;
    }
    let source = match object.get("source")?.as_str()? {
        "herdr" => DiagnosticSource::Herdr,
        "controller" => DiagnosticSource::Controller,
        "claude" => DiagnosticSource::Claude,
        "codex" => DiagnosticSource::Codex,
        _ => return None,
    };
    let availability = match object.get("availability")?.as_str()? {
        "available" => InputAvailability::Available,
        "partial" => InputAvailability::Partial,
        "unavailable" => InputAvailability::Unavailable,
        _ => return None,
    };
    Some(SourceCoverageSnapshot {
        source,
        availability,
    })
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum RequiredErrorField {
    Session,
    HerdrUnavailable,
    PongMalformed,
    HerdrBelowFloor,
    ProtocolMismatch,
    UnsafeRuntime,
    DatabaseCorrupt,
    DatabaseUnreadable,
    DatabaseNewer,
}

#[cfg(test)]
impl RequiredErrorField {
    const ALL: [Self; 9] = [
        Self::Session,
        Self::HerdrUnavailable,
        Self::PongMalformed,
        Self::HerdrBelowFloor,
        Self::ProtocolMismatch,
        Self::UnsafeRuntime,
        Self::DatabaseCorrupt,
        Self::DatabaseUnreadable,
        Self::DatabaseNewer,
    ];
}

#[cfg(test)]
fn report_with_required_error(field: RequiredErrorField) -> DoctorReportV1 {
    let mut report = canonical_healthy_report_for_test();
    match field {
        RequiredErrorField::Session => {
            report.session.resolution = check(CheckStatus::Error, "session_unresolved", None)
        }
        RequiredErrorField::HerdrUnavailable => {
            report.herdr.endpoint.status = CheckStatus::Error;
            report.herdr.endpoint.code = "herdr_unavailable";
        }
        RequiredErrorField::PongMalformed => {
            report.herdr.pong.status = CheckStatus::Error;
            report.herdr.pong.code = "herdr_pong_malformed";
        }
        RequiredErrorField::HerdrBelowFloor => {
            report.compatibility.herdr.status = CheckStatus::Error;
            report.compatibility.herdr.code = "herdr_below_floor";
        }
        RequiredErrorField::ProtocolMismatch => {
            report.compatibility.herdr.status = CheckStatus::Error;
            report.compatibility.herdr.code = "herdr_protocol_mismatch";
        }
        RequiredErrorField::UnsafeRuntime => {
            report.controller.rendezvous.status = CheckStatus::Error;
            report.controller.rendezvous.code = "controller_malformed_sentinel";
        }
        RequiredErrorField::DatabaseCorrupt => {
            report.database.schema.status = CheckStatus::Error;
            report.database.schema.code = "database_corrupt";
        }
        RequiredErrorField::DatabaseUnreadable => {
            report.database.schema.status = CheckStatus::Error;
            report.database.schema.code = "database_unreadable";
        }
        RequiredErrorField::DatabaseNewer => {
            report.database.schema.status = CheckStatus::Error;
            report.database.schema.code = "database_newer";
        }
    }
    report.recompute_overall_status();
    report
}

#[cfg(test)]
fn canonical_healthy_report_for_test() -> DoctorReportV1 {
    let mut report = DoctorReportV1 {
        schema_version: SCHEMA_VERSION,
        overall_status: OverallStatus::Ok,
        session: SessionSection {
            resolution: check(
                CheckStatus::Ok,
                "session_resolved",
                Some(SessionResolutionObservation {
                    source: SessionResolutionSource::Flag,
                }),
            ),
            name_record: check(CheckStatus::Ok, "session_name_matches", None),
        },
        paths: PathsSection {
            state_base: check(CheckStatus::Ok, "state_base_resolved", None),
            state_root: check(CheckStatus::Ok, "state_root_resolved", None),
            breadcrumb: check(CheckStatus::Ok, "breadcrumb_matches", None),
            controller_socket: check(CheckStatus::Ok, "controller_path_resolved", None),
            socket_length: check(CheckStatus::Ok, "controller_path_fits", None),
        },
        herdr: HerdrSection {
            endpoint: check(CheckStatus::Ok, "herdr_reachable", None),
            pong: check(CheckStatus::Ok, "herdr_pong_valid", None),
        },
        controller: ControllerSection {
            runtime: check(CheckStatus::Ok, "controller_live_healthy", None),
            rendezvous: check(CheckStatus::Ok, "controller_live", None),
        },
        lock: LockSection {
            ownership: check(CheckStatus::Ok, "lock_free", None),
        },
        database: DatabaseSection {
            schema: check(CheckStatus::Ok, "database_current", None),
        },
        providers: ProvidersSection {
            discovery: check(
                CheckStatus::Ok,
                "provider_roots_found",
                Some(vec![
                    ProviderRootObservation {
                        provider: DoctorProvider::Claude,
                        path: "~/.claude/projects".to_owned(),
                        exists: true,
                    },
                    ProviderRootObservation {
                        provider: DoctorProvider::Codex,
                        path: "~/.codex/sessions".to_owned(),
                        exists: true,
                    },
                ]),
            ),
        },
        compatibility: CompatibilitySection {
            herdr: check(CheckStatus::Ok, "herdr_compatible", None),
            integrations: check(CheckStatus::Ok, "integrations_current", None),
            standalone: check(CheckStatus::Ok, "standalone_exact", None),
            provider_clis: check(CheckStatus::Ok, "provider_cli_version_available", None),
        },
        coverage: CoverageSection {
            native_sessions: check(CheckStatus::Ok, "coverage_complete", None),
        },
        logs: LogsSection {
            location: check(CheckStatus::Ok, "log_path_resolved", None),
            persistence_occurrence: check(CheckStatus::Ok, "no_persistence_occurrence", None),
        },
    };
    report.recompute_overall_status();
    report
}

#[cfg(test)]
fn test_persistence_failure() -> PersistenceFailure {
    PersistenceFailure {
        operation: PersistenceOperation::Apply,
        phase: PersistencePhase::CommandExecution,
        code: PersistenceFailureCode::Sqlite,
        durability: DurabilityDisposition::NotCommitted,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::diagnostics::local::{
        PersistenceLogVerdict, PersistenceOccurrenceObservation as LocalOccurrence,
    };
    use crate::diagnostics::remote::{
        OfficialIntegrationAssessment, OfficialIntegrationStatus,
        OfficialIntegrationUnavailableReason, ProviderCliVersionStatus, StandaloneVersionStatus,
        VersionProbeFailure,
    };
    use crate::diagnostics::{
        ControllerCounterSnapshot, ControllerInputStatus, OccurrenceLogStatus, OwnerFreshness,
        PersistenceCounters, RuntimeDiagnosticsSnapshot,
    };
    use crate::herdr::types::{
        AgentManifestInfo, AgentManifestStatus, AgentSessionInfo, AgentSessionKind, PaneInfo, Pong,
        Snapshot,
    };
    use crate::model::Provider;
    use crate::rendezvous::ControllerRuntimeProbe;
    use crate::store::PersistenceStatus;

    fn runtime(persistence: PersistenceStatus) -> RuntimeDiagnosticsSnapshot {
        RuntimeDiagnosticsSnapshot {
            persistence,
            controller_input: ControllerInputStatus::Available,
            owner: OwnerFreshness::Current,
            persistence_counters: PersistenceCounters::default(),
            controller_counters: ControllerCounterSnapshot::default(),
            enrichment_counters: EnrichmentCounterSnapshot::default(),
            source_coverage: Vec::new(),
            dangling_announcement_components: 0,
            first_failure_log: OccurrenceLogStatus::NotAttempted,
        }
    }

    fn pane(agent: Option<&str>, reference: Option<(&str, AgentSessionKind)>) -> PaneInfo {
        PaneInfo {
            pane_id: "pane".to_owned(),
            terminal_id: "terminal".to_owned(),
            workspace_id: "workspace".to_owned(),
            tab_id: Some("tab".to_owned()),
            focused: None,
            cwd: None,
            foreground_cwd: None,
            terminal_title: None,
            terminal_title_stripped: None,
            label: None,
            agent: agent.map(str::to_owned),
            agent_status: None,
            scroll: None,
            revision: None,
            agent_session: reference.map(|(provider, kind)| AgentSessionInfo {
                source: "herdr".to_owned(),
                agent: provider.to_owned(),
                kind,
                value: "safe-id".to_owned(),
            }),
        }
    }

    fn snapshot(panes: Vec<PaneInfo>) -> Snapshot {
        Snapshot {
            version: "1".to_owned(),
            protocol: 19,
            focused_workspace_id: None,
            focused_tab_id: None,
            focused_pane_id: None,
            workspaces: Vec::new(),
            tabs: Vec::new(),
            panes,
            layouts: Vec::new(),
            agents: Vec::new(),
        }
    }

    fn integration_report(active_version: &str) -> DoctorReportV1 {
        let manifests = AgentManifestStatus {
            result_type: "agent_manifest_status".to_owned(),
            manifests: vec![
                integration_manifest("claude", active_version),
                integration_manifest("codex", "5"),
            ],
            last_check_unix: None,
            last_result: None,
        };
        let assessments = [Provider::Claude, Provider::Codex]
            .into_iter()
            .map(|provider| remote::assess_official_integration(&manifests, provider))
            .collect::<Vec<_>>();
        let mut report = canonical_healthy_report_for_test();
        report.compatibility.integrations = integration_check(&assessments);
        report.recompute_overall_status();
        report
    }

    fn integration_manifest(agent: &str, active_version: &str) -> AgentManifestInfo {
        AgentManifestInfo {
            agent: agent.to_owned(),
            source: "official".to_owned(),
            source_kind: "remote".to_owned(),
            local_override_shadowing_remote: false,
            active_version: Some(active_version.to_owned()),
            cached_remote_version: None,
            remote_last_checked_unix: None,
            remote_update_error: None,
            remote_update_result: None,
            warning: None,
        }
    }

    #[test]
    fn i4_doctor_aggregate_code_precedence_is_deterministic_for_ties() {
        let integrations = integration_check(&[
            OfficialIntegrationAssessment {
                provider: Provider::Codex,
                active_version: Some("4".to_owned()),
                minimum_version: "5".to_owned(),
                status: OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::BelowMinimum,
                },
            },
            OfficialIntegrationAssessment {
                provider: Provider::Claude,
                active_version: None,
                minimum_version: "6".to_owned(),
                status: OfficialIntegrationStatus::Unavailable {
                    reason: OfficialIntegrationUnavailableReason::MissingManifest,
                },
            },
        ]);
        assert_eq!(integrations.code, "integration_missing");
        assert_eq!(
            integrations.observed.as_ref().unwrap()[0].provider,
            DoctorProvider::Claude
        );

        let standalone = standalone_check(StandaloneVersionStatus::Mismatch {
            version: "0.0.9".to_owned(),
            stderr_present: true,
        });
        assert_eq!(standalone.code, "standalone_mismatch");

        let provider_clis = provider_cli_check(&[
            (
                Provider::Codex,
                ProviderCliVersionStatus::Unavailable {
                    reason: VersionProbeFailure::TimedOut,
                },
            ),
            (
                Provider::Claude,
                ProviderCliVersionStatus::Unavailable {
                    reason: VersionProbeFailure::NotFound,
                },
            ),
        ]);
        assert_eq!(provider_clis.code, "provider_cli_missing");
        assert_eq!(
            provider_clis.observed.as_ref().unwrap()[0].component,
            "claude"
        );
    }

    #[test]
    fn i4_doctor_human_and_json_share_typed_report() {
        let report = canonical_healthy_report_for_test();
        let human = render_human(&report);
        let json = render_json(&report);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["overall_status"], "ok");
        let expected_codes = report
            .checks()
            .map(|(_, check)| check.code)
            .collect::<Vec<_>>();
        let human_codes = human
            .lines()
            .skip(1)
            .map(|line| line.split(['[', ']']).nth(1).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(human_codes, expected_codes);
        assert!(human.find("session.resolution").unwrap() < human.find("logs.location").unwrap());
    }

    #[test]
    fn doctor_renderers_preserve_legacy_integer_integration_version() {
        let report = integration_report("7");
        let human = render_human(&report);
        let integration_line = human
            .lines()
            .find(|line| line.starts_with("compatibility.integrations:"))
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&render_json(&report)).unwrap();

        assert!(integration_line.contains("[integrations_current]"));
        assert!(integration_line.contains(r#""active_version":"7""#));
        assert_eq!(
            parsed["compatibility"]["integrations"]["code"],
            "integrations_current"
        );
        assert_eq!(
            parsed["compatibility"]["integrations"]["observed"][0]["active_version"],
            "7"
        );
    }

    #[test]
    fn doctor_renderers_preserve_date_form_integration_version() {
        let report = integration_report("2026.08.12.1");
        let human = render_human(&report);
        let integration_line = human
            .lines()
            .find(|line| line.starts_with("compatibility.integrations:"))
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&render_json(&report)).unwrap();

        assert!(integration_line.contains("[integrations_current]"));
        assert!(integration_line.contains(r#""active_version":"2026.08.12.1""#));
        assert_eq!(
            parsed["compatibility"]["integrations"]["code"],
            "integrations_current"
        );
        assert_eq!(
            parsed["compatibility"]["integrations"]["observed"][0]["active_version"],
            "2026.08.12.1"
        );
    }

    #[test]
    fn doctor_renderers_report_malformed_integration_version_unavailable() {
        let report = integration_report("07");
        let human = render_human(&report);
        let integration_line = human
            .lines()
            .find(|line| line.starts_with("compatibility.integrations:"))
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&render_json(&report)).unwrap();

        assert!(integration_line.contains("[integration_version_unavailable]"));
        assert!(integration_line.contains(r#""active_version":null"#));
        assert_eq!(
            parsed["compatibility"]["integrations"]["code"],
            "integration_version_unavailable"
        );
        assert_eq!(
            parsed["compatibility"]["integrations"]["observed"][0]["active_version"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn i4_doctor_required_optional_not_applicable_exit_matrix() {
        let mut report = canonical_healthy_report_for_test();
        assert_eq!(report.exit_code(), 0);

        report.database.schema.status = CheckStatus::NotApplicable;
        report.database.schema.code = "database_absent";
        report.controller.runtime.status = CheckStatus::Warning;
        report.controller.runtime.code = "controller_unavailable";
        report.coverage.native_sessions.status = CheckStatus::Warning;
        report.coverage.native_sessions.code = "coverage_partial";
        report.recompute_overall_status();
        assert_eq!(report.exit_code(), 0);
        assert_eq!(report.overall_status, OverallStatus::Warning);

        for field in RequiredErrorField::ALL {
            let report = report_with_required_error(field);
            assert_eq!(report.exit_code(), 1, "{field:?} must be required");
            assert_eq!(report.overall_status, OverallStatus::Error);
        }
    }

    #[test]
    fn i4_doctor_live_current_differs_from_historical_log() {
        let healthy = controller_runtime_check(
            ControllerRuntimeProbe::Live,
            Some(runtime(PersistenceStatus::Healthy)),
        );
        let parsed_live = parse_runtime_status(&json!({
            "status": "ok",
            "schema_version": 1,
            "diagnostics": runtime(PersistenceStatus::Healthy),
        }));
        let historical =
            persistence_occurrence_check(PersistenceLogVerdict::Found(LocalOccurrence {
                timestamp_ms: 7,
                failure: test_persistence_failure(),
                counters: PersistenceCounters::default(),
            }));
        assert_eq!(healthy.code, "controller_live_healthy");
        assert_eq!(parsed_live.unwrap().persistence, PersistenceStatus::Healthy);
        assert_eq!(historical.code, "historical_persistence_occurrence");

        let unavailable = controller_runtime_check(ControllerRuntimeProbe::Live, None);
        assert_eq!(unavailable.code, "controller_unavailable");
        assert_ne!(unavailable.code, "controller_live_degraded");
    }

    #[test]
    fn runtime_parser_preserves_enrichment_counters() {
        let mut diagnostics = serde_json::to_value(runtime(PersistenceStatus::Healthy)).unwrap();
        diagnostics.as_object_mut().unwrap().insert(
            "enrichment_counters".to_owned(),
            json!({"channel_full_drops": 7, "episode_discards": 11}),
        );

        let parsed = parse_runtime_status(&json!({
            "status": "ok",
            "schema_version": 1,
            "diagnostics": diagnostics,
        }))
        .expect("the closed runtime parser should accept the enrichment family");
        let rendered = serde_json::to_value(parsed).unwrap();
        assert_eq!(
            rendered["enrichment_counters"],
            json!({"channel_full_drops": 7, "episode_discards": 11})
        );
    }

    #[test]
    fn i4_doctor_persistence_occurrence_verdict_matrix() {
        let absent = persistence_occurrence_check(PersistenceLogVerdict::Absent);
        assert_eq!(absent.status, CheckStatus::Ok);
        assert_eq!(absent.code, "no_persistence_occurrence");
        assert!(absent.observed.is_none());

        for verdict in [
            PersistenceLogVerdict::Unreadable,
            PersistenceLogVerdict::NoValidRecord,
        ] {
            let malformed = persistence_occurrence_check(verdict);
            assert_eq!(malformed.status, CheckStatus::Warning);
            assert_eq!(malformed.code, "persistence_log_malformed_or_irrelevant");
            assert!(malformed.observed.is_none());
        }

        let found = persistence_occurrence_check(PersistenceLogVerdict::Found(LocalOccurrence {
            timestamp_ms: 11,
            failure: test_persistence_failure(),
            counters: PersistenceCounters::default(),
        }));
        assert_eq!(found.status, CheckStatus::Warning);
        assert_eq!(found.code, "historical_persistence_occurrence");
        let observed = found.observed.expect("found verdict has typed observation");
        assert_eq!(observed.timestamp_ms, 11);
        assert_eq!(observed.failure, test_persistence_failure());
        assert_eq!(observed.counters, PersistenceCounters::default());
    }

    #[test]
    fn i4_doctor_compatibility_matrix() {
        let current = herdr_compatibility_check(Some(&Pong {
            result_type: "pong".to_owned(),
            version: "0.8.0".to_owned(),
            protocol: 19,
            capabilities: None,
        }));
        assert_eq!(current.code, "herdr_compatible");
        let current_observed = current
            .observed
            .expect("compatible Herdr has a typed observation");
        assert_eq!(current_observed.found_protocol, 19_u32);
        assert_eq!(current_observed.supported_protocols, vec![19, 20]);

        let old = herdr_compatibility_check(Some(&Pong {
            result_type: "pong".to_owned(),
            version: "0.7.9".to_owned(),
            protocol: 19,
            capabilities: None,
        }));
        assert_eq!(old.code, "herdr_below_floor");

        let protocol = herdr_compatibility_check(Some(&Pong {
            result_type: "pong".to_owned(),
            version: "0.9.0".to_owned(),
            protocol: 20,
            capabilities: None,
        }));
        assert_eq!(protocol.code, "herdr_compatible");
        assert_eq!(protocol.observed.unwrap().found_protocol, 20_u32);

        let protocol_mismatch = herdr_compatibility_check(Some(&Pong {
            result_type: "pong".to_owned(),
            version: "0.9.0".to_owned(),
            protocol: 21,
            capabilities: None,
        }));
        assert_eq!(protocol_mismatch.code, "herdr_protocol_mismatch");
        assert_eq!(protocol_mismatch.observed.unwrap().found_protocol, 21_u32);

        let invalid = herdr_compatibility_check(Some(&Pong {
            result_type: "pong".to_owned(),
            version: "PRIVATE_VERSION".to_owned(),
            protocol: 19,
            capabilities: None,
        }));
        assert_eq!(invalid.code, "herdr_version_unparseable");
        assert!(invalid.observed.is_none());

        let unavailable = herdr_compatibility_check(None);
        assert_eq!(unavailable.code, "herdr_version_unparseable");
        assert!(unavailable.observed.is_none());

        let empty_manifests = AgentManifestStatus {
            result_type: "agent_manifest_status".to_owned(),
            manifests: Vec::new(),
            last_check_unix: None,
            last_result: None,
        };
        let direct = [Provider::Claude, Provider::Codex]
            .into_iter()
            .map(|provider| remote::assess_official_integration(&empty_manifests, provider))
            .collect::<Vec<_>>();
        assert_eq!(integration_assessments(None), direct);
    }

    #[test]
    fn i4_doctor_coverage_id_path_missing_unknown_zero_matrix() {
        let observed = coverage_check(Some(&snapshot(vec![
            pane(Some("Claude"), Some(("claude-code", AgentSessionKind::Id))),
            pane(
                Some("claude-code"),
                Some(("CLAUDE", AgentSessionKind::Path)),
            ),
            pane(Some("codex-cli"), Some(("CoDeX", AgentSessionKind::Id))),
            pane(
                Some("Codex CLAUDE"),
                Some(("claude-code", AgentSessionKind::Id)),
            ),
            pane(Some("codex"), Some(("unknown", AgentSessionKind::Path))),
            pane(Some("claude"), None),
            pane(Some("unknown"), Some(("claude", AgentSessionKind::Id))),
            pane(None, Some(("codex", AgentSessionKind::Id))),
        ])));
        assert_eq!(observed.status, CheckStatus::Warning);
        assert_eq!(observed.code, "coverage_partial");
        assert_eq!(
            serde_json::to_value(observed.observed.unwrap()).unwrap(),
            json!({
                "covered": 2,
                "partial": 1,
                "uncovered": 3,
                "total": 6,
                "unknown_provider": 1,
                "by_provider": [
                    {"provider": "claude", "covered": 1, "partial": 1, "uncovered": 1, "total": 3},
                    {"provider": "codex", "covered": 1, "partial": 0, "uncovered": 2, "total": 3}
                ]
            })
        );

        let zero = coverage_check(Some(&snapshot(Vec::new())));
        assert_eq!(zero.status, CheckStatus::NotApplicable);
        assert_eq!(zero.code, "coverage_zero_relevant");
        assert_eq!(zero.observed.unwrap().total, 0);

        let unavailable = coverage_check(None);
        assert_eq!(unavailable.status, CheckStatus::NotApplicable);
        assert_eq!(unavailable.code, "coverage_zero_relevant");
    }
}
