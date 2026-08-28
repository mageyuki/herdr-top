mod common;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use herdr_top::model::{
    ControllerEventKind, DomainModel, ExecState, NativeSessionEndStatus, Provider, RunKey,
    TaskState,
};
use herdr_top::provider::codex::{CodexAdapter, CodexBootstrapParser};
use herdr_top::provider::codex_facts::extract_codex_line;
use herdr_top::provider::facts::{EvidenceId, LogFact, SessionScope};
use herdr_top::provider::lane::{Admission, AdmissionIndex, Synthesis};
use herdr_top::provider::{
    BootstrapParser, DiscoveryIndex, DiscoveryRoot, MergeOutcome, PathInterner, PendingEvents,
    ProviderDiagnostics, ProviderEvent, SourcePosition, TailRecord,
};
use herdr_top::reducer::Reducer;
use herdr_top::store::RestoredState;

use common::flat_jsonl_fixture;

const ROOT_ID: &str = "019f7504-83e2-75f0-870d-cc423f88a73b";
const CHILD_ID: &str = "019f75fb-aa34-78a0-b97c-89a188fed43a";
const GRANDCHILD_ID: &str = "019f76d1-1ac4-7083-b4b8-434c8dba857b";
const SIBLING_ID: &str = "019f75fb-47a6-7d62-9806-81327ba4ff61";
const SHARED_CALL_ID: &str = "call_wMoPb3JNAmYC7ejgB8QfWQFy";
const SENTINEL: &str = "PROMPT_SENTINEL_DO_NOT_EMIT_7A3B9C";

struct FixtureIndex {
    _directory: tempfile::TempDir,
    index: DiscoveryIndex,
    relative_paths: HashMap<String, String>,
}

impl FixtureIndex {
    fn new(file_names: &[&str]) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut admission = Admission::new(0);
        let mut relative_paths = HashMap::new();
        for file_name in file_names {
            let mut bytes = Vec::new();
            for (_, record) in flat_jsonl_fixture(file_name) {
                bytes.extend(record);
                bytes.push(b'\n');
            }
            let relative_path = fixture_artifact_name(file_name);
            let path = directory.path().join(&relative_path);
            fs::write(&path, bytes).unwrap();
            assert!(admission.admit_pane_artifact(Provider::Codex, &path));
            relative_paths.insert((*file_name).to_owned(), relative_path);
        }
        let mut index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Codex,
            path: directory.path().to_path_buf(),
        }])
        .unwrap();
        index
            .scan_admitted(
                &mut CodexBootstrapParser,
                &mut PathInterner::default(),
                &admission,
                &mut AdmissionIndex::new(),
                &ProviderDiagnostics::default(),
            )
            .unwrap();
        Self {
            _directory: directory,
            index,
            relative_paths,
        }
    }

    fn file(&self, file_name: &str) -> &herdr_top::provider::DiscoveredFile {
        let relative_path = self
            .relative_paths
            .get(file_name)
            .unwrap_or_else(|| panic!("fixture {file_name} has no artifact path"));
        self.index
            .files()
            .into_iter()
            .find(|file| file.relative_path == Path::new(relative_path))
            .unwrap_or_else(|| panic!("fixture {file_name} was not discovered"))
    }

    fn relative_path(&self, file_name: &str) -> &str {
        self.relative_paths
            .get(file_name)
            .map(String::as_str)
            .unwrap_or_else(|| panic!("fixture {file_name} has no artifact path"))
    }

    fn events(&self, file_name: &str, generation: u64) -> Vec<ProviderEvent> {
        let adapter = CodexAdapter;
        let file = self.file(file_name);
        let mut events = adapter
            .bootstrap_event(file, generation, 1_800_000_000_000)
            .into_iter()
            .collect::<Vec<_>>();
        for (offset, bytes) in flat_jsonl_fixture(file_name) {
            events.extend(adapter.parse_record(
                &self.index,
                file,
                &TailRecord::data(offset, generation, bytes),
            ));
        }
        events
    }
}

fn fixture_artifact_name(label: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in label.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let rollout_id = format!(
        "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
        (hash >> 32) as u32,
        (hash >> 16) as u16,
        hash & 0x0fff,
        (hash >> 12) & 0x0fff,
        hash & 0xffff_ffff_ffff
    );
    format!("rollout-fixture-{rollout_id}.jsonl")
}

fn session<'a>(events: &'a [ProviderEvent], thread_id: &str) -> &'a ProviderEvent {
    events
        .iter()
        .find(|event| {
            matches!(
                event,
                ProviderEvent::SessionResolved {
                    agent_thread_id,
                    ..
                } if agent_thread_id == thread_id
            )
        })
        .unwrap_or_else(|| panic!("missing session event for {thread_id}"))
}

fn event_id(event: &ProviderEvent) -> Option<&str> {
    match event {
        ProviderEvent::SessionResolved { event_id, .. }
        | ProviderEvent::AgentUpsert { event_id, .. }
        | ProviderEvent::Activity { event_id, .. } => Some(event_id),
        ProviderEvent::Synthesized(_)
        | ProviderEvent::RunLiveness { .. }
        | ProviderEvent::LaneClose { .. }
        | ProviderEvent::Telemetry { .. }
        | ProviderEvent::SourceState { .. }
        | ProviderEvent::Malformed { .. } => None,
    }
}

#[test]
fn structural_child_reference_yields_exact_rollout_admission() {
    const STRUCTURAL_CHILD: &str = "d9999999-9999-4999-8999-999999999999";
    let child_path = PathBuf::from(format!(
        "/sessions/rollout-2026-08-24T08-00-00-{STRUCTURAL_CHILD}.jsonl"
    ));
    let line = format!(
        r#"{{"timestamp":"2026-08-24T08:00:00.000Z","type":"event_msg","payload":{{"type":"sub_agent_activity","event_id":"call_structural_child","occurred_at_ms":1787558400000,"agent_thread_id":"{STRUCTURAL_CHILD}","agent_path":"/root/child","kind":"started"}}}}"#
    );
    let facts = extract_codex_line(ROOT_ID, 0, &line);
    assert!(facts.contains(&LogFact::EvidenceId {
        parent: SessionScope::Codex {
            rollout_id: ROOT_ID.to_owned(),
        },
        id: EvidenceId::Uuid(STRUCTURAL_CHILD.to_owned()),
        at_ms: 1_787_558_400_000,
    }));

    let mut admission = Admission::new(0);
    admission.admit_pane_session(Provider::Codex, ROOT_ID);
    let mut discovered = AdmissionIndex::new();
    discovered.insert_codex_rollout(STRUCTURAL_CHILD, child_path.clone(), 0);
    let facts = facts
        .into_iter()
        .enumerate()
        .map(|(ordinal, fact)| (ordinal as u64, fact));
    let _events = Synthesis::default().synthesize_batch(
        Path::new("parent-rollout.jsonl"),
        facts,
        &mut admission,
        &discovered,
    );

    assert!(admission.is_admitted_path(&child_path));
}

#[test]
fn codex_free_text_uuid_is_not_lineage_evidence() {
    let line = r#"{"timestamp":"2026-08-24T08:01:00.000Z","type":"response_item","payload":{"type":"message","content":"pasted d9999999-9999-4999-8999-999999999999"}}"#;

    assert!(
        extract_codex_line(ROOT_ID, 0, line)
            .into_iter()
            .all(|fact| !matches!(fact, LogFact::EvidenceId { .. }))
    );
}

fn parse_inline(
    index: &DiscoveryIndex,
    file: &herdr_top::provider::DiscoveredFile,
    generation: u64,
    records: &[(u64, &[u8])],
) -> Vec<ProviderEvent> {
    let adapter = CodexAdapter;
    records
        .iter()
        .flat_map(|(offset, bytes)| {
            adapter.parse_record(
                index,
                file,
                &TailRecord::data(*offset, generation, bytes.to_vec()),
            )
        })
        .collect()
}

fn apply_synthesized_events(
    mut restored: RestoredState,
    events: impl IntoIterator<Item = ProviderEvent>,
) -> RestoredState {
    for event in events {
        let ProviderEvent::Synthesized(controller) = event else {
            continue;
        };
        let (reducer, _) = Reducer::new(restored);
        let delta = reducer
            .validate_controller_event(&controller)
            .expect("provider controller event should validate");
        restored = RestoredState {
            model: delta.post_model,
            next_ordinal: delta.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        };
    }
    restored
}

fn empty_restored() -> RestoredState {
    RestoredState {
        model: DomainModel::default(),
        next_ordinal: 1,
        next_ingest_seq: Some(1),
        event_ledger: Vec::new(),
    }
}

#[test]
fn native_root_runtime_lifecycle_and_ordinals_are_resumable_and_append_only() {
    const NEW_ROOT: &str = "019f7504-83e2-75f0-870d-cc423f88a74c";
    const FAILED_ROOT: &str = "019f7504-83e2-75f0-870d-cc423f88a75d";
    const UNKNOWN_ROOT: &str = "019f7504-83e2-75f0-870d-cc423f88a76e";
    let mut synthesis = Synthesis::with_lifecycle_timing_at(30, 50, 1);
    let mut admission = Admission::new(0);
    let discovered = AdmissionIndex::new();

    let started = synthesis.synthesize_batch(
        Path::new("old-root.jsonl"),
        [(
            1,
            LogFact::CodexTurnStarted {
                rollout_id: ROOT_ID.to_owned(),
                at_ms: 100,
            },
        )],
        &mut admission,
        &discovered,
    );
    let mut restored = apply_synthesized_events(empty_restored(), started);
    let old_key = RunKey::Native {
        provider: Provider::Codex,
        sid: ROOT_ID.to_owned(),
    };
    let old_run = restored.model.task_run_by_key(&old_key).unwrap().clone();

    let completed_turn = synthesis.synthesize_batch(
        Path::new("old-root.jsonl"),
        [(
            2,
            LogFact::CodexTurnComplete {
                rollout_id: ROOT_ID.to_owned(),
                at_ms: 120,
            },
        )],
        &mut admission,
        &discovered,
    );
    assert!(completed_turn.iter().any(|event| matches!(
        event,
        ProviderEvent::AgentUpsert {
            agent_thread_id,
            state: Some(ExecState::Idle),
            ..
        } if agent_thread_id == ROOT_ID
    )));
    assert!(completed_turn.iter().all(|event| !matches!(
        event,
        ProviderEvent::Synthesized(controller)
            if matches!(
                controller.event,
                ControllerEventKind::Complete
                    | ControllerEventKind::Failed
                    | ControllerEventKind::Cancelled
            )
    )));

    let aborted = synthesis.synthesize_batch(
        Path::new("old-root.jsonl"),
        [(
            3,
            LogFact::CodexTurnAborted {
                rollout_id: ROOT_ID.to_owned(),
                at_ms: 130,
            },
        )],
        &mut admission,
        &discovered,
    );
    restored = apply_synthesized_events(restored, aborted);
    let state = restored.model.task_run_v6_state(&old_run.run_id).unwrap();
    assert_eq!(
        restored.model.task_run(&old_run.run_id).unwrap().state,
        TaskState::Running
    );
    assert_eq!(
        state.native_session_end.as_ref().map(|end| end.status),
        Some(NativeSessionEndStatus::Cancelled)
    );

    let resumed = synthesis.synthesize_batch(
        Path::new("old-root.jsonl"),
        [(
            4,
            LogFact::CodexTurnStarted {
                rollout_id: ROOT_ID.to_owned(),
                at_ms: 140,
            },
        )],
        &mut admission,
        &discovered,
    );
    restored = apply_synthesized_events(restored, resumed);
    let resumed_run = restored.model.task_run_by_key(&old_key).unwrap();
    assert_eq!(resumed_run.run_id, old_run.run_id);
    assert_eq!(resumed_run.display_ordinal, old_run.display_ordinal);
    assert!(
        restored
            .model
            .task_run_v6_state(&old_run.run_id)
            .unwrap()
            .native_session_end
            .is_none()
    );

    let appended = synthesis.synthesize_batch(
        Path::new("new-root.jsonl"),
        [(
            1,
            LogFact::CodexTurnStarted {
                rollout_id: NEW_ROOT.to_owned(),
                at_ms: 150,
            },
        )],
        &mut admission,
        &discovered,
    );
    restored = apply_synthesized_events(restored, appended);
    let new_run = restored
        .model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: NEW_ROOT.to_owned(),
        })
        .unwrap();
    assert!(old_run.display_ordinal < new_run.display_ordinal);

    let branch = |model: &DomainModel, next_ordinal| RestoredState {
        model: model.clone(),
        next_ordinal,
        next_ingest_seq: Some(1),
        event_ledger: Vec::new(),
    };
    let failed_started = synthesis.synthesize_batch(
        Path::new("failed-root.jsonl"),
        [(
            1,
            LogFact::CodexTurnStarted {
                rollout_id: FAILED_ROOT.to_owned(),
                at_ms: 200,
            },
        )],
        &mut admission,
        &discovered,
    );
    let failed_state = apply_synthesized_events(
        branch(&restored.model, restored.next_ordinal),
        failed_started,
    );
    let failed = synthesis
        .synthesize_batch(
            Path::new("failed-root.jsonl"),
            [(
                2,
                LogFact::CodexTurnAborted {
                    rollout_id: FAILED_ROOT.to_owned(),
                    at_ms: 210,
                },
            )],
            &mut admission,
            &discovered,
        )
        .into_iter()
        .map(|event| match event {
            ProviderEvent::Synthesized(mut controller) => {
                controller.event = ControllerEventKind::Failed;
                controller.metadata.event_id =
                    format!("log:failed-root.jsonl:2:failed:{FAILED_ROOT}");
                controller.metadata.source_event_type = "failed".to_owned();
                ProviderEvent::Synthesized(controller)
            }
            event => event,
        });
    let failed_state = apply_synthesized_events(failed_state, failed);
    let failed_run = failed_state
        .model
        .task_run_by_key(&RunKey::Native {
            provider: Provider::Codex,
            sid: FAILED_ROOT.to_owned(),
        })
        .unwrap();
    assert_eq!(failed_run.state, TaskState::Running);
    assert_eq!(
        failed_state
            .model
            .task_run_v6_state(&failed_run.run_id)
            .and_then(|state| state.native_session_end.as_ref())
            .map(|end| end.status),
        Some(NativeSessionEndStatus::Error)
    );

    let unknown_started = synthesis.synthesize_batch(
        Path::new("unknown-root.jsonl"),
        [(
            1,
            LogFact::CodexTurnStarted {
                rollout_id: UNKNOWN_ROOT.to_owned(),
                at_ms: 300,
            },
        )],
        &mut admission,
        &discovered,
    );
    let unknown_state = apply_synthesized_events(
        branch(&restored.model, restored.next_ordinal),
        unknown_started,
    );
    let unknown_key = RunKey::Native {
        provider: Provider::Codex,
        sid: UNKNOWN_ROOT.to_owned(),
    };
    let unknown_run_id = unknown_state
        .model
        .task_run_by_key(&unknown_key)
        .unwrap()
        .run_id;
    let (mut reducer, shared) = Reducer::new(unknown_state);
    assert!(!reducer.apply_lane_close(&unknown_key, 310).is_empty());
    assert_eq!(
        shared.borrow().task_run(&unknown_run_id).unwrap().state,
        TaskState::Running
    );
    assert_eq!(
        shared
            .borrow()
            .task_run_v6_state(&unknown_run_id)
            .and_then(|state| state.native_session_end.as_ref())
            .map(|end| end.status),
        Some(NativeSessionEndStatus::Unknown)
    );
}

#[test]
fn real_depth_two_chain_resolves_identity_parent_and_depth() {
    let fixtures = FixtureIndex::new(&[
        "codex-depth2-root.jsonl",
        "codex-depth2-child.jsonl",
        "codex-depth2-grandchild.jsonl",
    ]);

    let cases = [
        ("codex-depth2-root.jsonl", ROOT_ID, None, 0),
        ("codex-depth2-child.jsonl", CHILD_ID, Some(ROOT_ID), 1),
        (
            "codex-depth2-grandchild.jsonl",
            GRANDCHILD_ID,
            Some(CHILD_ID),
            2,
        ),
    ];
    for (file_name, thread_id, expected_parent, expected_depth) in cases {
        let events = fixtures.events(file_name, 0);
        match session(&events, thread_id) {
            ProviderEvent::SessionResolved {
                provider,
                owner_session_id,
                parent_thread_id,
                model_id,
                depth,
                event_id,
                position,
                ..
            } => {
                assert_eq!(*provider, Provider::Codex);
                assert_eq!(owner_session_id.as_deref(), Some(ROOT_ID));
                assert_eq!(parent_thread_id.as_deref(), expected_parent);
                assert_eq!(model_id, &None);
                assert_eq!(*depth, Some(expected_depth));
                assert_eq!(event_id, &format!("prov:codex:meta:{thread_id}"));
                assert_eq!(position.generation, 0);
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn child_rollout_root_interactions_do_not_reparent_or_upsert_root() {
    let fixtures = FixtureIndex::new(&[
        "codex-depth2-root.jsonl",
        "codex-depth2-child.jsonl",
        "codex-depth2-grandchild.jsonl",
    ]);
    let events = fixtures.events("codex-depth2-child.jsonl", 0);

    let root_activities = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                ProviderEvent::Activity {
                    agent_thread_id,
                    activity,
                    ..
                } if agent_thread_id == ROOT_ID
                    && activity.event_kind.as_deref() == Some("interacted")
                    && activity.parent_agent_id.is_none()
            )
        })
        .count();
    assert_eq!(root_activities, 13);
    assert!(!events.iter().any(|event| {
        matches!(
            event,
            ProviderEvent::AgentUpsert {
                agent_thread_id,
                ..
            } if agent_thread_id == ROOT_ID
        )
    }));
}

#[test]
fn copied_sibling_start_resolves_root_parent_and_dedups_by_event_id() {
    let fixtures = FixtureIndex::new(&[
        "codex-depth2-root.jsonl",
        "codex-depth2-child.jsonl",
        "codex-depth2-grandchild.jsonl",
    ]);
    let root_events = fixtures.events("codex-depth2-root.jsonl", 0);
    let child_events = fixtures.events("codex-depth2-child.jsonl", 0);
    let select_shared_upsert = |events: &[ProviderEvent]| {
        events
            .iter()
            .find(|event| {
                matches!(
                    event,
                    ProviderEvent::AgentUpsert {
                        agent_thread_id,
                        parent_thread_id,
                        state: Some(ExecState::Working),
                        event_id,
                        ..
                    } if agent_thread_id == SIBLING_ID
                        && parent_thread_id.as_deref() == Some(ROOT_ID)
                        && event_id == &format!("prov:codex:up:{SHARED_CALL_ID}")
                )
            })
            .cloned()
            .expect("shared sibling upsert should exist")
    };
    let root_copy = select_shared_upsert(&root_events);
    let child_copy = select_shared_upsert(&child_events);
    let diagnostics = ProviderDiagnostics::default();
    let mut pending = PendingEvents::new(diagnostics.clone());

    assert_eq!(pending.merge(root_copy), MergeOutcome::Accepted);
    assert_eq!(pending.merge(child_copy), MergeOutcome::Duplicate);
    assert_eq!(diagnostics.duplicate_events(), 1);
}

#[test]
fn unknown_record_stubs_do_not_stop_known_records_on_both_sides() {
    let fixtures = FixtureIndex::new(&["codex-depth2-root.jsonl"]);
    let file = fixtures.file("codex-depth2-root.jsonl");
    let records = flat_jsonl_fixture("codex-depth2-root.jsonl");
    let selected = records[14..22]
        .iter()
        .map(|(offset, bytes)| (*offset, bytes.as_slice()))
        .collect::<Vec<_>>();

    let events = parse_inline(&fixtures.index, file, 0, &selected);

    assert!(
        events.iter().any(|event| {
            event_id(event) == Some("prov:codex:act:call_lPAVTyUj0U8KAboGHL7e2Vfz")
        })
    );
    assert!(
        events.iter().any(|event| {
            event_id(event) == Some("prov:codex:act:call_be93rTKXsW4pbhOxfnWhX1JC")
        })
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ProviderEvent::Malformed { .. }))
    );
}

#[test]
fn unknown_fields_are_ignored_and_unknown_kind_never_changes_state() {
    let fixtures = FixtureIndex::new(&["codex-depth2-root.jsonl"]);
    let file = fixtures.file("codex-depth2-root.jsonl");
    let record = br#"{"type":"event_msg","extra":"ignored","payload":{"type":"sub_agent_activity","event_id":"call_unknown_kind","occurred_at_ms":42,"agent_thread_id":"unknown-kind-thread","agent_path":"/root/unknown_kind","kind":"future_kind","another":{"nested":"ignored"}}}"#;

    let events = parse_inline(&fixtures.index, file, 0, &[(7, record)]);

    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        ProviderEvent::Activity {
            agent_thread_id,
            activity,
            event_id,
            observed_at_ms: 42,
            ..
        } if agent_thread_id == "unknown-kind-thread"
            && activity.event_kind.as_deref() == Some("future_kind")
            && event_id == "prov:codex:act:call_unknown_kind"
    ));
}

#[test]
fn non_root_agent_path_segment_is_accepted_with_correct_depth() {
    let fixtures = FixtureIndex::new(&["codex-depth2-root.jsonl"]);
    let file = fixtures.file("codex-depth2-root.jsonl");
    let activity_record = br#"{"type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"call_non_root_path","occurred_at_ms":43,"agent_thread_id":"non-root-worker","agent_path":"/main/worker","kind":"interacted"}}"#;
    let session_record = br#"{"type":"session_meta","payload":{"id":"non-root-thread","session_id":"non-root-owner","agent_path":"/main/worker"}}"#;

    let events = parse_inline(&fixtures.index, file, 0, &[(8, activity_record)]);
    let identity = CodexBootstrapParser
        .parse_structural(Provider::Codex, Path::new("inline.jsonl"), session_record)
        .expect("non-root-named agent path should parse structurally");

    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        ProviderEvent::Activity {
            agent_thread_id,
            activity,
            event_id,
            observed_at_ms: 43,
            ..
        } if agent_thread_id == "non-root-worker"
            && activity.event_kind.as_deref() == Some("interacted")
            && event_id == "prov:codex:act:call_non_root_path"
    ));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ProviderEvent::Malformed { .. }))
    );
    assert_eq!(identity.thread_id, "non-root-thread");
    assert_eq!(identity.depth, Some(1));
}

#[test]
fn invalid_agent_paths_are_malformed_and_parsing_continues() {
    let fixtures = FixtureIndex::new(&["codex-depth2-root.jsonl"]);
    let file = fixtures.file("codex-depth2-root.jsonl");
    let cases: [(&str, &[u8]); 2] = [
        (
            "missing leading slash",
            br#"{"type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"call_missing_slash","occurred_at_ms":1,"agent_thread_id":"missing-slash","agent_path":"root/worker","kind":"interacted"}}"#,
        ),
        (
            "grammar-violating segment",
            br#"{"type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"call_bad_path_segment","occurred_at_ms":1,"agent_thread_id":"bad-path-segment","agent_path":"/root/bad:segment","kind":"interacted"}}"#,
        ),
    ];
    let good = br#"{"type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"call_after_bad_path","occurred_at_ms":2,"agent_thread_id":"after-bad-path","agent_path":"/root/good","kind":"interacted"}}"#;

    for (case, bad) in cases {
        let events = parse_inline(&fixtures.index, file, 3, &[(100, bad), (300, good)]);

        assert!(
            matches!(
                events.first(),
                Some(ProviderEvent::Malformed {
                    generation: 3,
                    byte_offset: 100,
                    error_code: "codex_agent_path",
                    ..
                })
            ),
            "{case}"
        );
        assert!(
            events
                .iter()
                .any(|event| event_id(event) == Some("prov:codex:act:call_after_bad_path")),
            "{case}"
        );
    }
}

#[test]
fn later_session_meta_never_overrides_bootstrap_identity() {
    let fixtures = FixtureIndex::new(&["codex-depth2-root.jsonl", "codex-depth2-child.jsonl"]);
    let events = fixtures.events("codex-depth2-child.jsonl", 0);
    let sessions = events
        .iter()
        .filter(|event| matches!(event, ProviderEvent::SessionResolved { .. }))
        .collect::<Vec<_>>();

    assert_eq!(sessions.len(), 1);
    assert!(matches!(
        sessions[0],
        ProviderEvent::SessionResolved {
            agent_thread_id,
            parent_thread_id,
            depth: Some(1),
            ..
        } if agent_thread_id == CHILD_ID
            && parent_thread_id.as_deref() == Some(ROOT_ID)
    ));
}

#[test]
fn forked_from_without_parent_thread_id_creates_no_edge() {
    let fixtures = FixtureIndex::new(&["codex-fork-only-synthetic.jsonl"]);
    let events = fixtures.events("codex-fork-only-synthetic.jsonl", 0);

    assert!(matches!(
        session(&events, "codex-fork-only-thread"),
        ProviderEvent::SessionResolved {
            owner_session_id,
            parent_thread_id: None,
            depth: Some(1),
            ..
        } if owner_session_id.as_deref() == Some("codex-owner-thread")
    ));
}

#[test]
fn malformed_line_reports_context_and_next_record_still_lands() {
    let fixtures = FixtureIndex::new(&["codex-depth2-root.jsonl"]);
    let file = fixtures.file("codex-depth2-root.jsonl");
    let expected_path = fixtures.relative_path("codex-depth2-root.jsonl");
    let good = br#"{"type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"call_after_malformed","occurred_at_ms":84,"agent_thread_id":"after-malformed-thread","agent_path":"/root/after_malformed","kind":"interacted"}}"#;

    let events = parse_inline(&fixtures.index, file, 9, &[(41, b"{malformed"), (52, good)]);

    assert!(matches!(
        &events[0],
        ProviderEvent::Malformed {
            provider: Provider::Codex,
            path_display,
            generation: 9,
            byte_offset: 41,
            error_code: "codex_json",
        } if path_display == expected_path
    ));
    assert!(
        events
            .iter()
            .any(|event| { event_id(event) == Some("prov:codex:act:call_after_malformed") })
    );
}

#[test]
fn grammar_failing_id_is_malformed_and_parsing_continues() {
    let fixtures = FixtureIndex::new(&["codex-depth2-root.jsonl"]);
    let file = fixtures.file("codex-depth2-root.jsonl");
    let bad = br#"{"type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"call_bad_id","occurred_at_ms":1,"agent_thread_id":"bad:id","agent_path":"/root/bad","kind":"started"}}"#;
    let good = br#"{"type":"event_msg","payload":{"type":"sub_agent_activity","event_id":"call_after_bad_id","occurred_at_ms":2,"agent_thread_id":"good-id","agent_path":"/root/good","kind":"interacted"}}"#;

    let events = parse_inline(&fixtures.index, file, 3, &[(100, bad), (300, good)]);

    assert!(matches!(
        events.first(),
        Some(ProviderEvent::Malformed {
            generation: 3,
            byte_offset: 100,
            error_code: "codex_native_id",
            ..
        })
    ));
    assert!(
        events
            .iter()
            .any(|event| event_id(event) == Some("prov:codex:act:call_after_bad_id"))
    );
}

#[test]
fn non_allowlisted_prompt_sentinel_never_enters_an_event() {
    let fixtures = FixtureIndex::new(&[
        "codex-depth2-root.jsonl",
        "codex-depth2-child.jsonl",
        "codex-depth2-grandchild.jsonl",
        "codex-depth3-synthetic.jsonl",
    ]);
    let events = fixtures.events("codex-depth3-synthetic.jsonl", 0);

    assert!(!events.is_empty());
    for event in events {
        assert!(!format!("{event:?}").contains(SENTINEL));
    }
}

#[test]
fn synthetic_depth_three_identity_and_deeper_started_parent_resolve() {
    let fixtures = FixtureIndex::new(&[
        "codex-depth2-root.jsonl",
        "codex-depth2-child.jsonl",
        "codex-depth2-grandchild.jsonl",
        "codex-depth3-synthetic.jsonl",
    ]);
    let events = fixtures.events("codex-depth3-synthetic.jsonl", 4);

    assert!(matches!(
        session(&events, "codex-depth3-thread"),
        ProviderEvent::SessionResolved {
            parent_thread_id,
            model_id,
            depth: Some(3),
            position: SourcePosition { generation: 4, .. },
            ..
        } if parent_thread_id.as_deref() == Some(GRANDCHILD_ID)
            && model_id.as_deref() == Some("gpt-5.6-sol")
    ));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            ProviderEvent::AgentUpsert {
                agent_thread_id,
                parent_thread_id,
                state: Some(ExecState::Working),
                depth: Some(4),
                event_id,
                ..
            } if agent_thread_id == "codex-depth4-worker"
                && parent_thread_id.as_deref() == Some("codex-depth3-thread")
                && event_id == "prov:codex:up:call_depth4_worker_started"
        )
    }));
}

#[test]
fn activity_only_entity_flushes_at_validated_agent_path_depth() {
    let fixtures = FixtureIndex::new(&[
        "codex-depth2-root.jsonl",
        "codex-depth2-child.jsonl",
        "codex-depth2-grandchild.jsonl",
        "codex-depth3-synthetic.jsonl",
    ]);
    let deep_activity = fixtures
        .events("codex-depth3-synthetic.jsonl", 0)
        .into_iter()
        .find(|event| {
            matches!(
                event,
                ProviderEvent::Activity {
                    agent_thread_id,
                    ..
                } if agent_thread_id == "codex-depth4-worker"
            )
        })
        .expect("synthetic activity exists");
    let unknown_depth = ProviderEvent::Activity {
        provider: Provider::Codex,
        agent_thread_id: "000-unknown-depth".to_owned(),
        activity: Default::default(),
        depth: None,
        event_id: "unknown-depth-event".to_owned(),
        observed_at_ms: 1,
        position: SourcePosition {
            path_id: 99,
            generation: 0,
            offset: 0,
        },
    };
    let mut pending = PendingEvents::new(ProviderDiagnostics::default());
    pending.merge(unknown_depth);
    pending.merge(deep_activity);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);

    pending.flush_to(&sender);

    assert!(matches!(
        receiver.try_recv().unwrap(),
        ProviderEvent::Activity {
            agent_thread_id,
            ..
        } if agent_thread_id == "codex-depth4-worker"
    ));
}
