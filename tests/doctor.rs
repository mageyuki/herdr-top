mod common;

use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use common::mock::{MockConfig, MockHerdr};
use herdr_top::diagnostics::{
    ControllerCounterSnapshot, ControllerInputStatus, OccurrenceLogStatus, OwnerFreshness,
    PersistenceCounters, RuntimeDiagnosticsSnapshot,
};
use herdr_top::doctor::*;
use herdr_top::store::{
    DurabilityDisposition, PersistenceFailure, PersistenceFailureCode, PersistenceOperation,
    PersistencePhase, PersistenceStatus,
};
use serde_json::{Value, json};

const VERSION_FIXTURES: &str = r#"{
  "standalone":{"version":"0.1.0","stderr_present":false},
  "claude":{"version":"1.2.3","stderr_present":false},
  "codex":{"version":"2.3.4","stderr_present":false}
}"#;

const HEALTHY_JSON_V1: &str = r#"{"schema_version":1,"overall_status":"ok","session":{"resolution":{"status":"ok","code":"session_resolved","message":"session resolved","observed":{"source":"flag"}},"name_record":{"status":"ok","code":"session_name_matches","message":"session name record matches","observed":"matches"}},"paths":{"state_base":{"status":"ok","code":"state_base_resolved","message":"state base resolved","observed":"~/state"},"state_root":{"status":"ok","code":"state_root_resolved","message":"state root resolved","observed":"~/state/herdr-top/sessions/fixture"},"breadcrumb":{"status":"ok","code":"breadcrumb_matches","message":"plugin breadcrumb matches","observed":"matches"},"controller_socket":{"status":"ok","code":"controller_path_resolved","message":"Controller path resolved","observed":"~/runtime/herdr-top/fixture.sock"},"socket_length":{"status":"ok","code":"controller_path_fits","message":"Controller path fits platform limit","observed":{"bytes":42,"limit":108}}},"herdr":{"endpoint":{"status":"ok","code":"herdr_reachable","message":"Herdr endpoint is reachable","observed":{"reachable":true}},"pong":{"status":"ok","code":"herdr_pong_valid","message":"Herdr Pong is valid","observed":{"version":"0.8.0","protocol_version":19}}},"controller":{"runtime":{"status":"ok","code":"controller_live_healthy","message":"Controller is live and healthy","observed":{"persistence":{"status":"healthy"},"controller_input":{"status":"available"},"owner":"current","persistence_counters":{"not_committed_batches":0,"durability_unknown_batches":0,"committed_but_degraded_batches":0,"skipped_batches":0,"skipped_owner_updates":0},"controller_counters":{"binding_conflicts":0,"terminal_blocked_progress_noops":0,"terminal_forward_reference_creations":0,"dangling_announcement_components":0,"ingest_sequence_exhaustions":0,"provider_parent_conflicts":0,"provider_identity_disagreements":0,"socket_saturations":0,"accept_failures":0},"enrichment_counters":{"channel_full_drops":0,"episode_discards":0},"source_coverage":[],"dangling_announcement_components":0,"first_failure_log":"not_attempted"}},"rendezvous":{"status":"ok","code":"controller_live","message":"Controller rendezvous is live","observed":"live"}},"lock":{"ownership":{"status":"ok","code":"lock_free","message":"owner lock is free","observed":"free"}},"database":{"schema":{"status":"ok","code":"database_current","message":"database schema is current","observed":{"found":4,"supported":4}}},"providers":{"discovery":{"status":"ok","code":"provider_roots_found","message":"provider roots found","observed":[{"provider":"claude","path":"~/.claude/projects","exists":true},{"provider":"codex","path":"~/.codex/sessions","exists":true}]}},"compatibility":{"herdr":{"status":"ok","code":"herdr_compatible","message":"Herdr is compatible","observed":{"found_version":"0.8.0","minimum_version":"0.8.0","found_protocol":19,"supported_protocols":[19,20]}},"integrations":{"status":"ok","code":"integrations_current","message":"provider integrations are current","observed":[{"provider":"claude","active_version":"6","minimum_version":"6"},{"provider":"codex","active_version":"5","minimum_version":"5"}]},"standalone":{"status":"ok","code":"standalone_exact","message":"standalone version matches","observed":{"component":"herdr-top","found_version":"0.1.0","expected_version":"0.1.0","stderr_present":false}},"provider_clis":{"status":"ok","code":"provider_cli_version_available","message":"provider CLI versions are available","observed":[{"component":"claude","found_version":"1.2.3","expected_version":null,"stderr_present":false},{"component":"codex","found_version":"2.3.4","expected_version":null,"stderr_present":false}]}},"coverage":{"native_sessions":{"status":"ok","code":"coverage_complete","message":"native-session coverage is complete","observed":{"covered":2,"partial":0,"uncovered":0,"total":2,"unknown_provider":0,"by_provider":[{"provider":"claude","covered":1,"partial":0,"uncovered":0,"total":1},{"provider":"codex","covered":1,"partial":0,"uncovered":0,"total":1}]}}},"logs":{"location":{"status":"ok","code":"log_path_resolved","message":"log path resolved","observed":"~/state/herdr-top/sessions/fixture/herdr-top.log"},"persistence_occurrence":{"status":"ok","code":"no_persistence_occurrence","message":"no persistence occurrence found","observed":null}}}"#;

const WARNING_NOT_APPLICABLE_JSON_V1: &str = r#"{"schema_version":1,"overall_status":"warning","session":{"resolution":{"status":"ok","code":"session_resolved","message":"session resolved","observed":{"source":"environment"}},"name_record":{"status":"not_applicable","code":"session_name_absent","message":"session name record is absent","observed":"absent"}},"paths":{"state_base":{"status":"ok","code":"state_base_resolved","message":"state base resolved","observed":"~/state"},"state_root":{"status":"ok","code":"state_root_resolved","message":"state root resolved","observed":"~/state/herdr-top/sessions/fixture"},"breadcrumb":{"status":"not_applicable","code":"breadcrumb_absent","message":"plugin breadcrumb is absent","observed":"absent"},"controller_socket":{"status":"ok","code":"controller_path_resolved","message":"Controller path resolved","observed":"~/runtime/herdr-top/fixture.sock"},"socket_length":{"status":"ok","code":"controller_path_fits","message":"Controller path fits platform limit","observed":{"bytes":42,"limit":108}}},"herdr":{"endpoint":{"status":"ok","code":"herdr_reachable","message":"Herdr endpoint is reachable","observed":{"reachable":true}},"pong":{"status":"ok","code":"herdr_pong_valid","message":"Herdr Pong is valid","observed":{"version":"0.8.0","protocol_version":19}}},"controller":{"runtime":{"status":"warning","code":"controller_unavailable","message":"Controller status is unavailable","observed":null},"rendezvous":{"status":"not_applicable","code":"controller_absent","message":"Controller rendezvous is absent","observed":"absent"}},"lock":{"ownership":{"status":"warning","code":"lock_held_without_owner","message":"owner lock has no owner record","observed":"held_without_owner"}},"database":{"schema":{"status":"not_applicable","code":"database_absent","message":"database is absent","observed":{"found":null,"supported":4}}},"providers":{"discovery":{"status":"warning","code":"provider_roots_partial","message":"provider roots are partial","observed":[{"provider":"claude","path":"~/.claude/projects","exists":true},{"provider":"codex","path":"~/.codex/sessions","exists":false}]}},"compatibility":{"herdr":{"status":"ok","code":"herdr_compatible","message":"Herdr is compatible","observed":{"found_version":"0.8.0","minimum_version":"0.8.0","found_protocol":19,"supported_protocols":[19,20]}},"integrations":{"status":"warning","code":"integration_missing","message":"a provider integration is missing","observed":[{"provider":"claude","active_version":null,"minimum_version":"6"},{"provider":"codex","active_version":"4","minimum_version":"5"}]},"standalone":{"status":"warning","code":"standalone_mismatch","message":"standalone version differs","observed":{"component":"herdr-top","found_version":"0.0.9","expected_version":"0.1.0","stderr_present":true}},"provider_clis":{"status":"warning","code":"provider_cli_missing","message":"a provider CLI is missing","observed":[{"component":"claude","found_version":null,"expected_version":null,"stderr_present":false},{"component":"codex","found_version":null,"expected_version":null,"stderr_present":false}]}},"coverage":{"native_sessions":{"status":"warning","code":"coverage_partial","message":"native-session coverage is partial","observed":{"covered":0,"partial":1,"uncovered":1,"total":2,"unknown_provider":1,"by_provider":[{"provider":"claude","covered":0,"partial":1,"uncovered":0,"total":1},{"provider":"codex","covered":0,"partial":0,"uncovered":1,"total":1}]}}},"logs":{"location":{"status":"not_applicable","code":"log_absent","message":"log is absent","observed":"~/state/herdr-top/sessions/fixture/herdr-top.log"},"persistence_occurrence":{"status":"warning","code":"persistence_log_malformed_or_irrelevant","message":"persistence log has no valid occurrence","observed":null}}}"#;

const ERROR_COMPLETE_JSON_V1: &str = r#"{"schema_version":1,"overall_status":"error","session":{"resolution":{"status":"error","code":"session_unresolved","message":"session could not be resolved","observed":null},"name_record":{"status":"error","code":"session_name_unsafe","message":"session name record is unsafe","observed":"unsafe"}},"paths":{"state_base":{"status":"ok","code":"state_base_resolved","message":"state base resolved","observed":"~/state"},"state_root":{"status":"ok","code":"state_root_resolved","message":"state root resolved","observed":"~/state/herdr-top/sessions/fixture"},"breadcrumb":{"status":"warning","code":"breadcrumb_other_root","message":"plugin breadcrumb points to another root","observed":"other_root"},"controller_socket":{"status":"ok","code":"controller_path_resolved","message":"Controller path resolved","observed":"~/runtime/herdr-top/fixture.sock"},"socket_length":{"status":"ok","code":"controller_path_fits","message":"Controller path fits platform limit","observed":{"bytes":42,"limit":108}}},"herdr":{"endpoint":{"status":"error","code":"herdr_unavailable","message":"Herdr endpoint is unavailable","observed":{"reachable":false}},"pong":{"status":"error","code":"herdr_pong_malformed","message":"Herdr Pong is malformed","observed":null}},"controller":{"runtime":{"status":"warning","code":"controller_unavailable","message":"Controller status is unavailable","observed":null},"rendezvous":{"status":"error","code":"controller_orphan_socket","message":"Controller socket has no trusted sentinel","observed":"orphan_socket"}},"lock":{"ownership":{"status":"error","code":"lock_unsafe","message":"owner lock is unsafe","observed":"unsafe"}},"database":{"schema":{"status":"error","code":"database_newer","message":"database schema is newer","observed":{"found":5,"supported":4}}},"providers":{"discovery":{"status":"ok","code":"provider_roots_found","message":"provider roots found","observed":[{"provider":"claude","path":"~/.claude/projects","exists":true},{"provider":"codex","path":"~/.codex/sessions","exists":true}]}},"compatibility":{"herdr":{"status":"error","code":"herdr_protocol_mismatch","message":"Herdr protocol differs","observed":{"found_version":"0.9.0","minimum_version":"0.8.0","found_protocol":21,"supported_protocols":[19,20]}},"integrations":{"status":"ok","code":"integrations_current","message":"provider integrations are current","observed":[{"provider":"claude","active_version":"6","minimum_version":"6"},{"provider":"codex","active_version":"5","minimum_version":"5"}]},"standalone":{"status":"ok","code":"standalone_exact","message":"standalone version matches","observed":{"component":"herdr-top","found_version":"0.1.0","expected_version":"0.1.0","stderr_present":false}},"provider_clis":{"status":"ok","code":"provider_cli_version_available","message":"provider CLI versions are available","observed":[{"component":"claude","found_version":"1.2.3","expected_version":null,"stderr_present":false},{"component":"codex","found_version":"2.3.4","expected_version":null,"stderr_present":false}]}},"coverage":{"native_sessions":{"status":"ok","code":"coverage_complete","message":"native-session coverage is complete","observed":{"covered":2,"partial":0,"uncovered":0,"total":2,"unknown_provider":0,"by_provider":[{"provider":"claude","covered":1,"partial":0,"uncovered":0,"total":1},{"provider":"codex","covered":1,"partial":0,"uncovered":0,"total":1}]}}},"logs":{"location":{"status":"ok","code":"log_path_resolved","message":"log path resolved","observed":"~/state/herdr-top/sessions/fixture/herdr-top.log"},"persistence_occurrence":{"status":"warning","code":"historical_persistence_occurrence","message":"historical persistence occurrence found","observed":{"timestamp_ms":7,"failure":{"operation":"apply","phase":"command_execution","code":"sqlite","durability":"not_committed"},"counters":{"not_committed_batches":1,"durability_unknown_batches":0,"committed_but_degraded_batches":0,"skipped_batches":0,"skipped_owner_updates":0}}}}}"#;

fn message(code: &str) -> &'static str {
    match code {
        "session_resolved" => "session resolved",
        "session_unresolved" => "session could not be resolved",
        "session_name_matches" => "session name record matches",
        "session_name_absent" => "session name record is absent",
        "session_name_unsafe" => "session name record is unsafe",
        "state_base_resolved" => "state base resolved",
        "state_root_resolved" => "state root resolved",
        "breadcrumb_matches" => "plugin breadcrumb matches",
        "breadcrumb_absent" => "plugin breadcrumb is absent",
        "breadcrumb_other_root" => "plugin breadcrumb points to another root",
        "controller_path_resolved" => "Controller path resolved",
        "controller_path_fits" => "Controller path fits platform limit",
        "herdr_reachable" => "Herdr endpoint is reachable",
        "herdr_unavailable" => "Herdr endpoint is unavailable",
        "herdr_pong_valid" => "Herdr Pong is valid",
        "herdr_pong_malformed" => "Herdr Pong is malformed",
        "controller_live_healthy" => "Controller is live and healthy",
        "controller_unavailable" => "Controller status is unavailable",
        "controller_live" => "Controller rendezvous is live",
        "controller_absent" => "Controller rendezvous is absent",
        "controller_orphan_socket" => "Controller socket has no trusted sentinel",
        "lock_free" => "owner lock is free",
        "lock_held_without_owner" => "owner lock has no owner record",
        "lock_unsafe" => "owner lock is unsafe",
        "database_current" => "database schema is current",
        "database_absent" => "database is absent",
        "database_newer" => "database schema is newer",
        "provider_roots_found" => "provider roots found",
        "provider_roots_partial" => "provider roots are partial",
        "herdr_compatible" => "Herdr is compatible",
        "herdr_protocol_mismatch" => "Herdr protocol differs",
        "integrations_current" => "provider integrations are current",
        "integration_missing" => "a provider integration is missing",
        "standalone_exact" => "standalone version matches",
        "standalone_mismatch" => "standalone version differs",
        "provider_cli_version_available" => "provider CLI versions are available",
        "provider_cli_missing" => "a provider CLI is missing",
        "coverage_complete" => "native-session coverage is complete",
        "coverage_partial" => "native-session coverage is partial",
        "log_path_resolved" => "log path resolved",
        "log_absent" => "log is absent",
        "no_persistence_occurrence" => "no persistence occurrence found",
        "historical_persistence_occurrence" => "historical persistence occurrence found",
        "persistence_log_malformed_or_irrelevant" => "persistence log has no valid occurrence",
        _ => panic!("fixture omitted message for {code}"),
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

fn healthy_runtime() -> RuntimeDiagnosticsSnapshot {
    RuntimeDiagnosticsSnapshot {
        persistence: PersistenceStatus::Healthy,
        controller_input: ControllerInputStatus::Available,
        owner: OwnerFreshness::Current,
        persistence_counters: PersistenceCounters::default(),
        controller_counters: ControllerCounterSnapshot::default(),
        enrichment_counters: herdr_top::diagnostics::EnrichmentCounterSnapshot::default(),
        source_coverage: Vec::new(),
        dangling_announcement_components: 0,
        first_failure_log: OccurrenceLogStatus::NotAttempted,
    }
}

fn roots(all_exist: bool) -> Vec<ProviderRootObservation> {
    vec![
        ProviderRootObservation {
            provider: DoctorProvider::Claude,
            path: "~/.claude/projects".to_owned(),
            exists: true,
        },
        ProviderRootObservation {
            provider: DoctorProvider::Codex,
            path: "~/.codex/sessions".to_owned(),
            exists: all_exist,
        },
    ]
}

fn integrations(current: bool) -> Vec<IntegrationObservation> {
    vec![
        IntegrationObservation {
            provider: DoctorProvider::Claude,
            active_version: current.then(|| "6".to_owned()),
            minimum_version: "6".to_owned(),
        },
        IntegrationObservation {
            provider: DoctorProvider::Codex,
            active_version: Some(if current { "5" } else { "4" }.to_owned()),
            minimum_version: "5".to_owned(),
        },
    ]
}

fn cli_versions(available: bool) -> Vec<ExecutableVersionObservation> {
    vec![
        ExecutableVersionObservation {
            component: "claude".to_owned(),
            found_version: available.then(|| "1.2.3".to_owned()),
            expected_version: None,
            stderr_present: false,
        },
        ExecutableVersionObservation {
            component: "codex".to_owned(),
            found_version: available.then(|| "2.3.4".to_owned()),
            expected_version: None,
            stderr_present: false,
        },
    ]
}

fn coverage(complete: bool) -> CoverageObservation {
    CoverageObservation {
        covered: if complete { 2 } else { 0 },
        partial: if complete { 0 } else { 1 },
        uncovered: if complete { 0 } else { 1 },
        total: 2,
        unknown_provider: if complete { 0 } else { 1 },
        by_provider: vec![
            ProviderCoverageCounts {
                provider: DoctorProvider::Claude,
                covered: u64::from(complete),
                partial: u64::from(!complete),
                uncovered: 0,
                total: 1,
            },
            ProviderCoverageCounts {
                provider: DoctorProvider::Codex,
                covered: u64::from(complete),
                partial: 0,
                uncovered: u64::from(!complete),
                total: 1,
            },
        ],
    }
}

fn healthy_report() -> DoctorReportV1 {
    DoctorReportV1 {
        schema_version: 1,
        overall_status: OverallStatus::Ok,
        session: SessionSection {
            resolution: check(
                CheckStatus::Ok,
                "session_resolved",
                Some(SessionResolutionObservation {
                    source: SessionResolutionSource::Flag,
                }),
            ),
            name_record: check(
                CheckStatus::Ok,
                "session_name_matches",
                Some(NameRecordObservation::Matches),
            ),
        },
        paths: PathsSection {
            state_base: check(
                CheckStatus::Ok,
                "state_base_resolved",
                Some("~/state".to_owned()),
            ),
            state_root: check(
                CheckStatus::Ok,
                "state_root_resolved",
                Some("~/state/herdr-top/sessions/fixture".to_owned()),
            ),
            breadcrumb: check(
                CheckStatus::Ok,
                "breadcrumb_matches",
                Some(BreadcrumbObservation::Matches),
            ),
            controller_socket: check(
                CheckStatus::Ok,
                "controller_path_resolved",
                Some("~/runtime/herdr-top/fixture.sock".to_owned()),
            ),
            socket_length: check(
                CheckStatus::Ok,
                "controller_path_fits",
                Some(PathLengthObservation {
                    bytes: 42,
                    limit: 108,
                }),
            ),
        },
        herdr: HerdrSection {
            endpoint: check(
                CheckStatus::Ok,
                "herdr_reachable",
                Some(EndpointObservation { reachable: true }),
            ),
            pong: check(
                CheckStatus::Ok,
                "herdr_pong_valid",
                Some(PongObservation {
                    version: "0.8.0".to_owned(),
                    protocol_version: 19,
                }),
            ),
        },
        controller: ControllerSection {
            runtime: check(
                CheckStatus::Ok,
                "controller_live_healthy",
                Some(healthy_runtime()),
            ),
            rendezvous: check(
                CheckStatus::Ok,
                "controller_live",
                Some(ControllerRendezvousObservation::Live),
            ),
        },
        lock: LockSection {
            ownership: check(CheckStatus::Ok, "lock_free", Some(LockObservation::Free)),
        },
        database: DatabaseSection {
            schema: check(
                CheckStatus::Ok,
                "database_current",
                Some(DatabaseSchemaObservation {
                    found: Some(4),
                    supported: 4,
                }),
            ),
        },
        providers: ProvidersSection {
            discovery: check(CheckStatus::Ok, "provider_roots_found", Some(roots(true))),
        },
        compatibility: CompatibilitySection {
            herdr: check(
                CheckStatus::Ok,
                "herdr_compatible",
                Some(HerdrCompatibilityObservation {
                    found_version: "0.8.0".to_owned(),
                    minimum_version: "0.8.0".to_owned(),
                    found_protocol: 19,
                    supported_protocols: vec![19, 20],
                }),
            ),
            integrations: check(
                CheckStatus::Ok,
                "integrations_current",
                Some(integrations(true)),
            ),
            standalone: check(
                CheckStatus::Ok,
                "standalone_exact",
                Some(ExecutableVersionObservation {
                    component: "herdr-top".to_owned(),
                    found_version: Some("0.1.0".to_owned()),
                    expected_version: Some("0.1.0".to_owned()),
                    stderr_present: false,
                }),
            ),
            provider_clis: check(
                CheckStatus::Ok,
                "provider_cli_version_available",
                Some(cli_versions(true)),
            ),
        },
        coverage: CoverageSection {
            native_sessions: check(CheckStatus::Ok, "coverage_complete", Some(coverage(true))),
        },
        logs: LogsSection {
            location: check(
                CheckStatus::Ok,
                "log_path_resolved",
                Some("~/state/herdr-top/sessions/fixture/herdr-top.log".to_owned()),
            ),
            persistence_occurrence: check(CheckStatus::Ok, "no_persistence_occurrence", None),
        },
    }
}

fn warning_report() -> DoctorReportV1 {
    let mut report = healthy_report();
    report.overall_status = OverallStatus::Warning;
    report.session.resolution.observed.as_mut().unwrap().source =
        SessionResolutionSource::Environment;
    report.session.name_record = check(
        CheckStatus::NotApplicable,
        "session_name_absent",
        Some(NameRecordObservation::Absent),
    );
    report.paths.breadcrumb = check(
        CheckStatus::NotApplicable,
        "breadcrumb_absent",
        Some(BreadcrumbObservation::Absent),
    );
    report.controller.runtime = check(CheckStatus::Warning, "controller_unavailable", None);
    report.controller.rendezvous = check(
        CheckStatus::NotApplicable,
        "controller_absent",
        Some(ControllerRendezvousObservation::Absent),
    );
    report.lock.ownership = check(
        CheckStatus::Warning,
        "lock_held_without_owner",
        Some(LockObservation::HeldWithoutOwner),
    );
    report.database.schema = check(
        CheckStatus::NotApplicable,
        "database_absent",
        Some(DatabaseSchemaObservation {
            found: None,
            supported: 4,
        }),
    );
    report.providers.discovery = check(
        CheckStatus::Warning,
        "provider_roots_partial",
        Some(roots(false)),
    );
    report.compatibility.integrations = check(
        CheckStatus::Warning,
        "integration_missing",
        Some(integrations(false)),
    );
    report.compatibility.standalone = check(
        CheckStatus::Warning,
        "standalone_mismatch",
        Some(ExecutableVersionObservation {
            component: "herdr-top".to_owned(),
            found_version: Some("0.0.9".to_owned()),
            expected_version: Some("0.1.0".to_owned()),
            stderr_present: true,
        }),
    );
    report.compatibility.provider_clis = check(
        CheckStatus::Warning,
        "provider_cli_missing",
        Some(cli_versions(false)),
    );
    report.coverage.native_sessions = check(
        CheckStatus::Warning,
        "coverage_partial",
        Some(coverage(false)),
    );
    report.logs.location = check(
        CheckStatus::NotApplicable,
        "log_absent",
        Some("~/state/herdr-top/sessions/fixture/herdr-top.log".to_owned()),
    );
    report.logs.persistence_occurrence = check(
        CheckStatus::Warning,
        "persistence_log_malformed_or_irrelevant",
        None,
    );
    report
}

fn test_failure() -> PersistenceFailure {
    PersistenceFailure {
        operation: PersistenceOperation::Apply,
        phase: PersistencePhase::CommandExecution,
        code: PersistenceFailureCode::Sqlite,
        durability: DurabilityDisposition::NotCommitted,
    }
}

fn error_report() -> DoctorReportV1 {
    let mut report = healthy_report();
    report.overall_status = OverallStatus::Error;
    report.session.resolution = check(CheckStatus::Error, "session_unresolved", None);
    report.session.name_record = check(
        CheckStatus::Error,
        "session_name_unsafe",
        Some(NameRecordObservation::Unsafe),
    );
    report.paths.breadcrumb = check(
        CheckStatus::Warning,
        "breadcrumb_other_root",
        Some(BreadcrumbObservation::OtherRoot),
    );
    report.herdr.endpoint = check(
        CheckStatus::Error,
        "herdr_unavailable",
        Some(EndpointObservation { reachable: false }),
    );
    report.herdr.pong = check(CheckStatus::Error, "herdr_pong_malformed", None);
    report.controller.runtime = check(CheckStatus::Warning, "controller_unavailable", None);
    report.controller.rendezvous = check(
        CheckStatus::Error,
        "controller_orphan_socket",
        Some(ControllerRendezvousObservation::OrphanSocket),
    );
    report.lock.ownership = check(
        CheckStatus::Error,
        "lock_unsafe",
        Some(LockObservation::Unsafe),
    );
    report.database.schema = check(
        CheckStatus::Error,
        "database_newer",
        Some(DatabaseSchemaObservation {
            found: Some(5),
            supported: 4,
        }),
    );
    report.compatibility.herdr = check(
        CheckStatus::Error,
        "herdr_protocol_mismatch",
        Some(HerdrCompatibilityObservation {
            found_version: "0.9.0".to_owned(),
            minimum_version: "0.8.0".to_owned(),
            found_protocol: 21,
            supported_protocols: vec![19, 20],
        }),
    );
    report.logs.persistence_occurrence = check(
        CheckStatus::Warning,
        "historical_persistence_occurrence",
        Some(PersistenceOccurrenceObservation {
            timestamp_ms: 7,
            failure: test_failure(),
            counters: PersistenceCounters {
                not_committed_batches: 1,
                ..PersistenceCounters::default()
            },
        }),
    );
    report
}

fn assert_human_order(report: &DoctorReportV1) {
    let human = render_human(report);
    let mut previous = 0;
    for name in [
        "session.resolution",
        "session.name_record",
        "paths.state_base",
        "paths.state_root",
        "paths.breadcrumb",
        "paths.controller_socket",
        "paths.socket_length",
        "herdr.endpoint",
        "herdr.pong",
        "controller.runtime",
        "controller.rendezvous",
        "lock.ownership",
        "database.schema",
        "providers.discovery",
        "compatibility.herdr",
        "compatibility.integrations",
        "compatibility.standalone",
        "compatibility.provider_clis",
        "coverage.native_sessions",
        "logs.location",
        "logs.persistence_occurrence",
    ] {
        let found = human
            .find(name)
            .unwrap_or_else(|| panic!("missing human row {name}"));
        assert!(found >= previous, "human row {name} is out of order");
        previous = found;
    }
    assert!(human.find("\"claude\"").unwrap() < human.find("\"codex\"").unwrap());
}

#[test]
fn i4_doctor_json_has_fixed_sections_nulls_and_order() {
    let rendered = render_json(&healthy_report());
    assert!(rendered.contains("\"observed\":null"));
    let mut previous = 0;
    for key in [
        "\"schema_version\"",
        "\"overall_status\"",
        "\"session\"",
        "\"paths\"",
        "\"herdr\"",
        "\"controller\"",
        "\"lock\"",
        "\"database\"",
        "\"providers\"",
        "\"compatibility\"",
        "\"coverage\"",
        "\"logs\"",
    ] {
        let found = rendered[previous..].find(key).expect("fixed key missing") + previous;
        assert!(found >= previous);
        previous = found + key.len();
    }
}

#[test]
fn i4_doctor_healthy_json_v1_matches_canonical_fixture() {
    let report = healthy_report();
    assert_eq!(render_json(&report), HEALTHY_JSON_V1);
    assert_human_order(&report);
}

#[test]
fn i4_doctor_warning_not_applicable_json_v1_matches_canonical_fixture() {
    let report = warning_report();
    assert_eq!(render_json(&report), WARNING_NOT_APPLICABLE_JSON_V1);
    assert_eq!(
        report.compatibility.integrations.code,
        "integration_missing"
    );
    assert_eq!(report.compatibility.standalone.code, "standalone_mismatch");
    assert_eq!(
        report.compatibility.provider_clis.code,
        "provider_cli_missing"
    );
    assert_human_order(&report);
}

#[test]
fn i4_doctor_error_complete_json_v1_matches_canonical_fixture() {
    let report = error_report();
    assert_eq!(render_json(&report), ERROR_COMPLETE_JSON_V1);
    assert_eq!(report.exit_code(), 1);
    assert_human_order(&report);
}

fn snapshot_result() -> Value {
    json!({
        "type": "session_snapshot",
        "snapshot": {
            "version": "1",
            "protocol": 19,
            "focused_workspace_id": null,
            "focused_tab_id": null,
            "focused_pane_id": null,
            "workspaces": [],
            "tabs": [],
            "panes": [],
            "layouts": [],
            "agents": []
        }
    })
}

fn manifests_result() -> Value {
    json!({
        "type": "agent_manifest_status",
        "manifests": [
            {
                "agent": "claude", "source": "official", "source_kind": "bundled",
                "local_override_shadowing_remote": false, "active_version": "6"
            },
            {
                "agent": "codex", "source": "official", "source_kind": "bundled",
                "local_override_shadowing_remote": false, "active_version": "5"
            }
        ]
    })
}

fn command_output(mock: &MockHerdr, root: &Path, extra_args: &[&OsStr]) -> Output {
    let state = root.join("state-must-not-be-created");
    let runtime = root.join("runtime-must-not-be-created");
    let plugin = root.join("plugin-must-not-be-created");
    let mut command = Command::new(env!("CARGO_BIN_EXE_herdr-top"));
    command
        .arg("--session")
        .arg("fixture")
        .arg("--socket")
        .arg(mock.socket_path())
        .arg("doctor")
        .args(extra_args)
        .env_clear()
        .env("HOME", root)
        .env("XDG_STATE_HOME", &state)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("HERDR_PLUGIN_STATE_DIR", &plugin)
        .env("HERDR_TOP_TEST_DOCTOR_VERSION_FIXTURES", VERSION_FIXTURES);
    command.output().expect("doctor binary should start")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i4_doctor_cli_exposes_package_version_and_json() {
    let version = Command::new(env!("CARGO_BIN_EXE_herdr-top"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8(version.stdout).unwrap().trim(),
        format!("herdr-top {}", env!("CARGO_PKG_VERSION"))
    );

    let directory = tempfile::tempdir().unwrap();
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond(
                "ping",
                json!({"type":"pong","version":"0.8.0","protocol":19}),
            )
            .respond("server.agent_manifests", manifests_result())
            .respond("session.snapshot", snapshot_result()),
    )
    .await
    .unwrap();
    let output = command_output(&mock, directory.path(), &[OsStr::new("--json")]);
    let parsed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(parsed["schema_version"], 1);
    assert!(output.stderr.is_empty());

    let usage = Command::new(env!("CARGO_BIN_EXE_herdr-top"))
        .args(["doctor", "--unknown"])
        .output()
        .unwrap();
    assert_eq!(usage.status.code(), Some(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i4_doctor_complete_json_precedes_exit_one() {
    let directory = tempfile::tempdir().unwrap();
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond(
                "ping",
                json!({"type":"pong","version":"HOSTILE_PRIVATE","protocol":19}),
            )
            .respond("server.agent_manifests", manifests_result())
            .respond("session.snapshot", snapshot_result()),
    )
    .await
    .unwrap();
    let output = command_output(&mock, directory.path(), &[OsStr::new("--json")]);
    assert_eq!(output.status.code(), Some(1));
    let parsed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(parsed["overall_status"], "error");
    assert!(parsed["logs"]["persistence_occurrence"].is_object());
    assert!(output.stderr.is_empty());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("HOSTILE_PRIVATE"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i4_doctor_creates_no_filesystem_entries() {
    let directory = tempfile::tempdir().unwrap();
    let mock = MockHerdr::start(
        MockConfig::default()
            .respond(
                "ping",
                json!({"type":"pong","version":"0.8.0","protocol":19}),
            )
            .respond("server.agent_manifests", manifests_result())
            .respond("session.snapshot", snapshot_result()),
    )
    .await
    .unwrap();
    let before = fs::read_dir(directory.path()).unwrap().count();
    let output = command_output(&mock, directory.path(), &[OsStr::new("--json")]);
    assert!(serde_json::from_slice::<Value>(&output.stdout).is_ok());
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), before);
    for child in [
        "state-must-not-be-created",
        "runtime-must-not-be-created",
        "plugin-must-not-be-created",
        ".claude",
        ".codex",
    ] {
        assert!(
            !directory.path().join(child).exists(),
            "Doctor created {child}"
        );
    }
}
