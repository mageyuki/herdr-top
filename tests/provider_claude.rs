mod common;

use std::fs;
use std::path::{Path, PathBuf};

use herdr_top::model::Provider;
use herdr_top::provider::claude::{ClaudeAdapter, ClaudeBootstrapParser};
use herdr_top::provider::claude_facts::extract_claude_line;
use herdr_top::provider::facts::{EvidenceId, SessionScope};
use herdr_top::provider::lane::{Admission, AdmissionIndex, Synthesis};
use herdr_top::provider::{
    DiscoveryIndex, DiscoveryRoot, PathInterner, ProviderDiagnostics, ProviderEvent,
    SourcePosition, TailRecord,
};

use common::flat_jsonl_fixture;

const ROOT_ID: &str = "87099666-7f29-425c-859c-6b95a6dd650a";
const CHILD_ID: &str = "ae01a56c5071a72c5";
const PROJECT: &str = "project-safe";
const MAIN_PATH: &str = "project-safe/87099666-7f29-425c-859c-6b95a6dd650a.jsonl";
const CHILD_PATH: &str =
    "project-safe/87099666-7f29-425c-859c-6b95a6dd650a/subagents/agent-ae01a56c5071a72c5.jsonl";
const SENTINEL: &str = "PROMPT_SENTINEL_DO_NOT_EMIT_7A3B9C";

struct FixtureIndex {
    _directory: tempfile::TempDir,
    index: DiscoveryIndex,
}

impl FixtureIndex {
    fn new(entries: &[(&str, &str)]) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut admitted_paths = Vec::new();
        for (relative_path, fixture_name) in entries {
            let records = flat_jsonl_fixture(fixture_name);
            let path = directory.path().join(relative_path);
            write_records(path.clone(), &records);
            admitted_paths.push(path);
        }
        let index = scan(directory.path(), admitted_paths);
        Self {
            _directory: directory,
            index,
        }
    }

    fn file(&self, relative_path: &str) -> &herdr_top::provider::DiscoveredFile {
        self.index
            .files()
            .into_iter()
            .find(|file| file.relative_path == Path::new(relative_path))
            .unwrap_or_else(|| panic!("fixture {relative_path} was not discovered"))
    }

    fn events(
        &self,
        relative_path: &str,
        fixture_name: &str,
        generation: u64,
    ) -> Vec<ProviderEvent> {
        let adapter = ClaudeAdapter;
        let file = self.file(relative_path);
        let mut events = adapter
            .bootstrap_event(file, generation, 1_800_000_000_000)
            .into_iter()
            .collect::<Vec<_>>();
        for (offset, bytes) in flat_jsonl_fixture(fixture_name) {
            events.extend(adapter.parse_record(
                &self.index,
                file,
                &TailRecord::data(offset, generation, bytes),
            ));
        }
        events
    }
}

struct SyntheticIndex {
    _directory: tempfile::TempDir,
    index: DiscoveryIndex,
    relative_path: PathBuf,
}

impl SyntheticIndex {
    fn new(relative_path: &str, records: &[&[u8]]) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let records = records_with_offsets(records);
        let path = directory.path().join(relative_path);
        write_records(path.clone(), &records);
        let index = scan(directory.path(), [path]);
        Self {
            _directory: directory,
            index,
            relative_path: PathBuf::from(relative_path),
        }
    }

    fn file(&self) -> &herdr_top::provider::DiscoveredFile {
        self.index
            .files()
            .into_iter()
            .find(|file| file.relative_path == self.relative_path)
            .expect("synthetic file should be discovered")
    }

    fn all_events(&self, records: &[&[u8]], generation: u64) -> Vec<ProviderEvent> {
        let adapter = ClaudeAdapter;
        let file = self.file();
        let mut events = adapter
            .bootstrap_event(file, generation, 1_800_000_000_000)
            .into_iter()
            .collect::<Vec<_>>();
        for (offset, bytes) in records_with_offsets(records) {
            events.extend(adapter.parse_record(
                &self.index,
                file,
                &TailRecord::data(offset, generation, bytes),
            ));
        }
        events
    }
}

fn scan(root: &Path, admitted_paths: impl IntoIterator<Item = PathBuf>) -> DiscoveryIndex {
    let mut index = DiscoveryIndex::new(vec![DiscoveryRoot {
        provider: Provider::Claude,
        path: root.to_path_buf(),
    }])
    .unwrap();
    let admitted_paths = admitted_paths.into_iter().collect::<Vec<_>>();
    let pane_session = if admitted_paths
        .iter()
        .any(|path| path.file_name() == Some(std::ffi::OsStr::new(&format!("{ROOT_ID}.jsonl"))))
    {
        ROOT_ID
    } else {
        "fixture-pane-root"
    };
    let mut admission = Admission::new(0);
    admission.admit_pane_session(Provider::Claude, pane_session);
    let mut evidence = AdmissionIndex::new();
    let parent = SessionScope::ClaudeRoot(pane_session.to_owned());
    for path in admitted_paths {
        let Some(agent_id) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("agent-"))
            .and_then(|name| name.strip_suffix(".jsonl"))
        else {
            continue;
        };
        evidence.insert_claude_subagent(ROOT_ID, agent_id, path.clone(), 0);
        assert!(
            admission
                .on_evidence(&parent, &EvidenceId::Uuid(agent_id.to_owned()), &evidence,)
                .is_some()
        );
    }
    index
        .scan_admitted(
            &mut ClaudeBootstrapParser,
            &mut PathInterner::default(),
            &admission,
            &mut evidence,
            &ProviderDiagnostics::default(),
        )
        .unwrap();
    index
}

fn write_records(path: PathBuf, records: &[(u64, Vec<u8>)]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut bytes = Vec::new();
    for (_, record) in records {
        bytes.extend(record);
        bytes.push(b'\n');
    }
    fs::write(path, bytes).unwrap();
}

fn records_with_offsets(records: &[&[u8]]) -> Vec<(u64, Vec<u8>)> {
    let mut offset = 0_u64;
    records
        .iter()
        .map(|record| {
            let record_offset = offset;
            offset += record.len() as u64 + 1;
            (record_offset, record.to_vec())
        })
        .collect()
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

fn parse_inline(
    index: &DiscoveryIndex,
    file: &herdr_top::provider::DiscoveredFile,
    generation: u64,
    records: &[(u64, &[u8])],
) -> Vec<ProviderEvent> {
    records
        .iter()
        .flat_map(|(offset, bytes)| {
            ClaudeAdapter.parse_record(
                index,
                file,
                &TailRecord::data(*offset, generation, bytes.to_vec()),
            )
        })
        .collect()
}

#[test]
fn resume_command_admits_child_from_only_the_pane_root() {
    const RESUMED: &str = "d7777777-7777-4777-8777-777777777777";
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join(PROJECT);
    fs::create_dir_all(&project).unwrap();
    let parent_path = project.join(format!("{ROOT_ID}.jsonl"));
    let child_path = project.join(format!("{RESUMED}.jsonl"));
    let parent_line = format!(
        r#"{{"type":"assistant","uuid":"77777777-7777-4777-8777-777777777777","timestamp":"2026-08-24T07:30:00.000Z","sessionId":"{ROOT_ID}","isSidechain":false,"message":{{"content":[{{"type":"tool_use","name":"Bash","input":{{"command":"claude --resume {RESUMED}"}}}}]}}}}"#
    );
    let child_line = format!(
        r#"{{"type":"user","uuid":"88888888-8888-4888-8888-888888888888","timestamp":"2026-08-24T07:31:00.000Z","sessionId":"{RESUMED}","isSidechain":false}}"#
    );
    fs::write(&parent_path, format!("{parent_line}\n")).unwrap();
    fs::write(&child_path, format!("{child_line}\n")).unwrap();

    let mut index = DiscoveryIndex::new(vec![DiscoveryRoot {
        provider: Provider::Claude,
        path: directory.path().to_path_buf(),
    }])
    .unwrap();
    let mut admission = Admission::new(0);
    admission.admit_pane_session(Provider::Claude, ROOT_ID);
    let mut evidence = AdmissionIndex::new();
    let mut parser = ClaudeBootstrapParser;
    let mut interner = PathInterner::default();
    index
        .scan_admitted(
            &mut parser,
            &mut interner,
            &admission,
            &mut evidence,
            &ProviderDiagnostics::default(),
        )
        .unwrap();
    assert_eq!(index.files().len(), 1);
    assert!(!admission.is_admitted_path(&child_path));

    let facts = extract_claude_line(&SessionScope::ClaudeRoot(ROOT_ID.to_owned()), &parent_line)
        .into_iter()
        .enumerate()
        .map(|(ordinal, fact)| (ordinal as u64, fact));
    let _events =
        Synthesis::default().synthesize_batch(&parent_path, facts, &mut admission, &evidence);
    assert!(admission.is_admitted_path(&child_path));

    index
        .scan_admitted(
            &mut parser,
            &mut interner,
            &admission,
            &mut evidence,
            &ProviderDiagnostics::default(),
        )
        .unwrap();
    assert!(index.files().iter().any(|file| {
        file.root.join(&file.relative_path) == child_path
            && file
                .bootstrap
                .as_ref()
                .is_some_and(|identity| identity.thread_id == RESUMED)
    }));
}

#[test]
fn real_parent_and_subagent_resolve_lineage_depth_and_activity() {
    let fixtures = FixtureIndex::new(&[
        (MAIN_PATH, "claude-depth1-parent.jsonl"),
        (CHILD_PATH, "claude-depth1-subagent.jsonl"),
    ]);
    let parent_events = fixtures.events(MAIN_PATH, "claude-depth1-parent.jsonl", 4);
    let child_events = fixtures.events(CHILD_PATH, "claude-depth1-subagent.jsonl", 5);
    let first_parent_structural_offset = flat_jsonl_fixture("claude-depth1-parent.jsonl")[3].0;

    assert!(matches!(
        session(&parent_events, ROOT_ID),
        ProviderEvent::SessionResolved {
            provider: Provider::Claude,
            owner_session_id,
            parent_thread_id: None,
            depth: Some(0),
            event_id,
            position: SourcePosition {
                generation: 4,
                offset,
                ..
            },
            ..
        } if owner_session_id.as_deref() == Some(ROOT_ID)
            && event_id == &format!("prov:claude:meta:{ROOT_ID}")
            && *offset == first_parent_structural_offset
    ));
    assert!(matches!(
        session(&child_events, CHILD_ID),
        ProviderEvent::SessionResolved {
            provider: Provider::Claude,
            owner_session_id,
            parent_thread_id,
            depth: Some(1),
            event_id,
            position: SourcePosition { generation: 5, .. },
            ..
        } if owner_session_id.as_deref() == Some(ROOT_ID)
            && parent_thread_id.as_deref() == Some(ROOT_ID)
            && event_id == &format!("prov:claude:meta:{CHILD_ID}")
    ));
    assert!(parent_events.iter().any(|event| {
        matches!(
            event,
            ProviderEvent::Activity {
                agent_thread_id,
                activity,
                depth: Some(0),
                event_id,
                ..
            } if agent_thread_id == ROOT_ID
                && activity.agent_id.as_deref() == Some(ROOT_ID)
                && activity.parent_agent_id.is_none()
                && activity.event_kind.as_deref() == Some("attachment")
                && event_id == "prov:claude:rec:13b76c1d-9412-4c20-a4b3-ab03a99e811b"
        )
    }));
    assert!(child_events.iter().any(|event| {
        matches!(
            event,
            ProviderEvent::Activity {
                agent_thread_id,
                activity,
                depth: Some(1),
                event_id,
                ..
            } if agent_thread_id == CHILD_ID
                && activity.agent_id.as_deref() == Some(CHILD_ID)
                && activity.parent_agent_id.as_deref() == Some(ROOT_ID)
                && activity.event_kind.as_deref() == Some("user")
                && event_id == "prov:claude:rec:1d707aeb-b27b-48b6-b1b0-5d5bff57c689"
        )
    }));
    assert!(
        !parent_events
            .iter()
            .chain(&child_events)
            .any(|event| matches!(event, ProviderEvent::AgentUpsert { .. }))
    );
}

#[test]
fn meta_json_sibling_is_filtered_at_discovery() {
    let directory = tempfile::tempdir().unwrap();
    let child_path = directory.path().join(CHILD_PATH);
    write_records(
        child_path.clone(),
        &flat_jsonl_fixture("claude-depth1-subagent.jsonl"),
    );
    fs::write(
        child_path.with_file_name("agent-ae01a56c5071a72c5.meta.json"),
        b"{}",
    )
    .unwrap();

    let index = scan(directory.path(), [child_path]);
    let files = index.files();

    assert_eq!(files.len(), 1);
    assert_eq!(files[0].relative_path, Path::new(CHILD_PATH));
}

#[test]
fn named_spawn_filename_uses_in_record_agent_id() {
    let named_path = format!("{PROJECT}/{ROOT_ID}/subagents/agent-investigate-deadbeef.jsonl");
    let fixtures = FixtureIndex::new(&[(&named_path, "claude-depth1-subagent.jsonl")]);
    let events = fixtures.events(&named_path, "claude-depth1-subagent.jsonl", 0);

    assert!(matches!(
        session(&events, CHILD_ID),
        ProviderEvent::SessionResolved {
            agent_thread_id,
            parent_thread_id,
            depth: Some(1),
            ..
        } if agent_thread_id == CHILD_ID
            && parent_thread_id.as_deref() == Some(ROOT_ID)
    ));
    assert!(!events.iter().any(|event| {
        matches!(
            event,
            ProviderEvent::SessionResolved {
                agent_thread_id,
                ..
            } if agent_thread_id == "agent-investigate-deadbeef"
        )
    }));
}

#[test]
fn unknown_record_types_and_fields_preserve_known_records_on_both_sides() {
    let before = br#"{"type":"user","uuid":"11111111-1111-4111-8111-111111111111","timestamp":"2026-08-09T01:02:03.456Z","sessionId":"87099666-7f29-425c-859c-6b95a6dd650a","isSidechain":false}"#;
    let unknown = br#"{"type":"future-record","uuid":"not:an:id","message":{"content":"ignored"}}"#;
    let after = br#"{"type":"assistant","uuid":"22222222-2222-4222-8222-222222222222","timestamp":"2026-08-09T01:02:04.456Z","sessionId":"87099666-7f29-425c-859c-6b95a6dd650a","isSidechain":false,"futureField":{"nested":"ignored"}}"#;
    let records: [&[u8]; 3] = [before, unknown, after];
    let fixtures = SyntheticIndex::new(MAIN_PATH, &records);
    let events = fixtures.all_events(&records, 0);

    assert!(events.iter().any(
        |event| event_id(event) == Some("prov:claude:rec:11111111-1111-4111-8111-111111111111")
    ));
    assert!(events.iter().any(
        |event| event_id(event) == Some("prov:claude:rec:22222222-2222-4222-8222-222222222222")
    ));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ProviderEvent::Malformed { .. }))
    );
}

#[test]
fn malformed_line_reports_escaped_context_and_next_record_still_lands() {
    let bootstrap = br#"{"type":"user","uuid":"33333333-3333-4333-8333-333333333333","timestamp":"2026-08-09T01:02:03Z","sessionId":"87099666-7f29-425c-859c-6b95a6dd650a","isSidechain":false}"#;
    let relative_path = format!("project\nslug/{ROOT_ID}.jsonl");
    let fixtures = SyntheticIndex::new(&relative_path, &[bootstrap]);
    let good = br#"{"type":"assistant","uuid":"44444444-4444-4444-8444-444444444444","timestamp":"2026-08-09T01:02:04Z","sessionId":"87099666-7f29-425c-859c-6b95a6dd650a","isSidechain":false}"#;

    let events = parse_inline(
        &fixtures.index,
        fixtures.file(),
        9,
        &[(41, b"{malformed"), (52, good)],
    );

    assert!(matches!(
        &events[0],
        ProviderEvent::Malformed {
            provider: Provider::Claude,
            path_display,
            generation: 9,
            byte_offset: 41,
            error_code: "claude_json",
        } if path_display == "project\\nslug/87099666-7f29-425c-859c-6b95a6dd650a.jsonl"
    ));
    assert!(events.iter().any(|event| {
        event_id(event) == Some("prov:claude:rec:44444444-4444-4444-8444-444444444444")
    }));
}

#[test]
fn grammar_failing_record_id_is_malformed_and_parsing_continues() {
    let bootstrap = br#"{"type":"user","uuid":"55555555-5555-4555-8555-555555555555","timestamp":"2026-08-09T01:02:03Z","sessionId":"87099666-7f29-425c-859c-6b95a6dd650a","isSidechain":false}"#;
    let fixtures = SyntheticIndex::new(MAIN_PATH, &[bootstrap]);
    let bad = br#"{"type":"user","uuid":"bad:uuid","timestamp":"2026-08-09T01:02:04Z","sessionId":"87099666-7f29-425c-859c-6b95a6dd650a","isSidechain":false}"#;
    let good = br#"{"type":"assistant","uuid":"66666666-6666-4666-8666-666666666666","timestamp":"2026-08-09T01:02:05Z","sessionId":"87099666-7f29-425c-859c-6b95a6dd650a","isSidechain":false}"#;

    let events = parse_inline(
        &fixtures.index,
        fixtures.file(),
        3,
        &[(100, bad), (300, good)],
    );

    assert!(matches!(
        events.first(),
        Some(ProviderEvent::Malformed {
            generation: 3,
            byte_offset: 100,
            error_code: "claude_native_id",
            ..
        })
    ));
    assert!(events.iter().any(|event| {
        event_id(event) == Some("prov:claude:rec:66666666-6666-4666-8666-666666666666")
    }));
}

#[test]
fn non_allowlisted_prompt_sentinel_never_enters_events_or_diagnostics() {
    let record = format!(
        r#"{{"type":"user","uuid":"77777777-7777-4777-8777-777777777777","timestamp":"2026-08-09T01:02:03Z","sessionId":"sentinel-session","isSidechain":false,"message":{{"role":"user","content":"{SENTINEL}"}},"toolUseResult":{{"arguments":"{SENTINEL}"}},"future":"{SENTINEL}"}}"#
    );
    let records: [&[u8]; 1] = [record.as_bytes()];
    let fixtures = SyntheticIndex::new(MAIN_PATH, &records);
    let events = fixtures.all_events(&records, 0);

    assert!(!events.is_empty());
    assert!(!format!("{events:?}").contains(SENTINEL));
}
