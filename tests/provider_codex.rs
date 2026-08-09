mod common;

use std::fs;
use std::path::Path;

use herdr_top::model::{ExecState, Provider};
use herdr_top::provider::codex::{CodexAdapter, CodexBootstrapParser};
use herdr_top::provider::{
    BootstrapParser, DiscoveryIndex, DiscoveryRoot, MergeOutcome, PathInterner, PendingEvents,
    ProviderDiagnostics, ProviderEvent, SourcePosition, TailRecord,
};

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
}

impl FixtureIndex {
    fn new(file_names: &[&str]) -> Self {
        let directory = tempfile::tempdir().unwrap();
        for file_name in file_names {
            let mut bytes = Vec::new();
            for (_, record) in flat_jsonl_fixture(file_name) {
                bytes.extend(record);
                bytes.push(b'\n');
            }
            fs::write(directory.path().join(file_name), bytes).unwrap();
        }
        let mut index = DiscoveryIndex::new(vec![DiscoveryRoot {
            provider: Provider::Codex,
            path: directory.path().to_path_buf(),
        }])
        .unwrap();
        index
            .scan(&mut CodexBootstrapParser, &mut PathInterner::default())
            .unwrap();
        Self {
            _directory: directory,
            index,
        }
    }

    fn file(&self, file_name: &str) -> &herdr_top::provider::DiscoveredFile {
        self.index
            .files()
            .into_iter()
            .find(|file| file.relative_path == Path::new(file_name))
            .unwrap_or_else(|| panic!("fixture {file_name} was not discovered"))
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
        ProviderEvent::SourceState { .. } | ProviderEvent::Malformed { .. } => None,
    }
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
        } if path_display == "codex-depth2-root.jsonl"
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
