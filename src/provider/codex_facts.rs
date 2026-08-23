//! Typed allowlist extraction for Codex rollout records.
//!
//! Extraction is stateless and emits one [`LogFact::CodexMeta`] for every
//! `session_meta` record. A file-order consumer preserves the first emitted
//! metadata fact as rollout identity and ignores later copies.

use std::borrow::Cow;
use std::fmt;

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::model::sanitize_controller_text;

use super::claude_facts::{parse_decimal, parse_timestamp_ms};
use super::facts::{
    CodexInternal, LogFact, SessionScope, repo_relative, sanitize_command_script, scan_raw_ids,
    truncate_60,
};

#[derive(Debug, Deserialize)]
struct RecordEnvelope {
    timestamp: Option<String>,
    ordinal: Option<u64>,
    #[serde(rename = "type")]
    record_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SessionMetaEnvelope {
    payload: Option<SessionMetaPayload>,
}

#[derive(Debug, Deserialize)]
struct SessionMetaPayload {
    id: Option<String>,
    cwd: Option<String>,
    originator: Option<String>,
    cli_version: Option<String>,
    source: Option<SessionSource>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SessionSource {
    Object(SessionSourceObject),
    Ignored(IgnoredAny),
}

#[derive(Debug, Deserialize)]
struct SessionSourceObject {
    subagent: Option<SubagentSource>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SubagentSource {
    Named(NamedSubagent),
    ThreadSpawn(ThreadSpawnSubagent),
    Ignored(IgnoredAny),
}

#[derive(Debug, Deserialize)]
struct NamedSubagent {
    other: String,
}

#[derive(Debug, Deserialize)]
struct ThreadSpawnSubagent {
    thread_spawn: ThreadSpawnSource,
}

#[derive(Debug, Deserialize)]
struct ThreadSpawnSource {
    parent_thread_id: String,
    agent_nickname: Option<String>,
    agent_role: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TurnContextEnvelope {
    payload: Option<TurnContextPayload>,
}

#[derive(Debug, Deserialize)]
struct TurnContextPayload {
    turn_id: Option<String>,
    model: Option<String>,
    effort: Option<String>,
    sandbox_policy: Option<SandboxPolicy>,
}

#[derive(Debug, Deserialize)]
struct SandboxPolicy {
    mode: Option<String>,
    #[serde(rename = "type")]
    policy_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventTypeEnvelope {
    payload: Option<EventTypePayload>,
}

#[derive(Debug, Deserialize)]
struct EventTypePayload {
    #[serde(rename = "type")]
    event_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ItemTypeEnvelope {
    payload: Option<ItemTypePayload>,
}

#[derive(Debug, Deserialize)]
struct ItemTypePayload {
    item: Option<ItemType>,
}

#[derive(Debug, Deserialize)]
struct ItemType {
    #[serde(rename = "type")]
    item_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AgentMessageEnvelope {
    payload: Option<AgentMessagePayload>,
}

#[derive(Debug, Deserialize)]
struct AgentMessagePayload {
    item: Option<AgentMessageItem>,
}

#[derive(Debug, Deserialize)]
struct AgentMessageItem {
    phase: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AgentCommentaryEnvelope {
    payload: Option<AgentCommentaryPayload>,
}

#[derive(Debug, Deserialize)]
struct AgentCommentaryPayload {
    item: Option<AgentCommentaryItem>,
}

#[derive(Debug, Deserialize)]
struct AgentCommentaryItem {
    content: Option<FirstAgentContent>,
}

#[derive(Debug)]
struct FirstAgentContent(Option<AgentContent>);

#[derive(Debug, Deserialize)]
struct AgentContent {
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CommandExecutionEnvelope {
    payload: Option<CommandExecutionPayload>,
}

#[derive(Debug, Deserialize)]
struct CommandExecutionPayload {
    item: Option<CommandExecutionItem>,
}

#[derive(Debug, Deserialize)]
struct CommandExecutionItem {
    process_id: Option<ProcessId>,
    command: Option<Vec<String>>,
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ProcessId {
    String(String),
    Ignored(IgnoredAny),
}

#[derive(Debug, Deserialize)]
struct TokenCountEnvelope {
    payload: Option<TokenCountPayload>,
}

#[derive(Debug, Deserialize)]
struct TokenCountPayload {
    info: Option<TokenCountInfo>,
}

#[derive(Debug, Deserialize)]
struct TokenCountInfo {
    last_token_usage: Option<LastTokenUsage>,
}

#[derive(Debug, Deserialize)]
struct LastTokenUsage {
    output_tokens: Option<u64>,
    reasoning_output_tokens: Option<u64>,
}

impl SessionSource {
    fn into_internal(self) -> Option<CodexInternal> {
        match self {
            Self::Object(source) => source.subagent.and_then(SubagentSource::into_internal),
            Self::Ignored(_) => None,
        }
    }
}

impl SubagentSource {
    fn into_internal(self) -> Option<CodexInternal> {
        match self {
            Self::Named(source) => Some(CodexInternal::Named { name: source.other }),
            Self::ThreadSpawn(source) => {
                let source = source.thread_spawn;
                Some(CodexInternal::ThreadSpawn {
                    parent_thread_id: source.parent_thread_id,
                    nickname: source.agent_nickname,
                    role: source.agent_role,
                })
            }
            Self::Ignored(_) => None,
        }
    }
}

impl ProcessId {
    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            Self::Ignored(_) => None,
        }
    }
}

impl<'de> Deserialize<'de> for FirstAgentContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(FirstAgentContentVisitor)
    }
}

#[derive(Debug)]
struct FirstAgentContentVisitor;

impl<'de> Visitor<'de> for FirstAgentContentVisitor {
    type Value = FirstAgentContent;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an AgentMessage content array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let first = sequence.next_element::<AgentContent>()?;
        while sequence.next_element::<IgnoredAny>()?.is_some() {}
        Ok(FirstAgentContent(first))
    }
}

/// Extracts allowlisted facts from one Codex rollout JSONL line.
///
/// This function is stateless. It emits metadata for each `session_meta`
/// record so a file-order consumer can retain the first record as identity.
#[must_use]
pub fn extract_codex_line(rollout_id: &str, line: &str) -> Vec<LogFact> {
    let scope = SessionScope::Codex {
        rollout_id: rollout_id.to_owned(),
    };
    let mut facts = scan_raw_ids(line)
        .into_iter()
        .map(|id| LogFact::EvidenceId {
            parent: scope.clone(),
            id,
        })
        .collect::<Vec<_>>();

    let Ok(record) = serde_json::from_str::<RecordEnvelope>(line) else {
        return facts;
    };
    let at_ms = record.timestamp.as_deref().and_then(parse_timestamp_ms);
    if let Some(at_ms) = at_ms {
        facts.push(LogFact::Append {
            scope: scope.clone(),
            at_ms,
        });
    }

    match record.record_type.as_deref() {
        Some("session_meta") => extract_session_meta(rollout_id, line, &mut facts),
        Some("turn_context") => extract_turn_context(rollout_id, line, &mut facts),
        Some("event_msg") => {
            extract_event(rollout_id, &scope, line, at_ms, record.ordinal, &mut facts)
        }
        _ => {}
    }
    facts
}

fn extract_session_meta(rollout_id: &str, line: &str, facts: &mut Vec<LogFact>) {
    let Ok(envelope) = serde_json::from_str::<SessionMetaEnvelope>(line) else {
        return;
    };
    let Some(payload) = envelope.payload else {
        return;
    };
    let (Some(cwd), Some(originator), Some(cli_version)) =
        (payload.cwd, payload.originator, payload.cli_version)
    else {
        return;
    };
    let internal = payload.source.and_then(SessionSource::into_internal);

    facts.push(LogFact::CodexMeta {
        rollout_id: payload.id.unwrap_or_else(|| rollout_id.to_owned()),
        cwd,
        originator,
        internal,
        cli_version,
    });
}

fn extract_turn_context(rollout_id: &str, line: &str, facts: &mut Vec<LogFact>) {
    let Ok(envelope) = serde_json::from_str::<TurnContextEnvelope>(line) else {
        return;
    };
    let Some(payload) = envelope.payload else {
        return;
    };
    let (Some(turn_id), Some(model)) = (payload.turn_id, payload.model) else {
        return;
    };
    let sandbox = payload
        .sandbox_policy
        .and_then(|policy| policy.mode.or(policy.policy_type));

    facts.push(LogFact::CodexTurn {
        rollout_id: rollout_id.to_owned(),
        turn_id,
        model,
        effort: payload.effort,
        sandbox,
    });
}

fn extract_event(
    rollout_id: &str,
    scope: &SessionScope,
    line: &str,
    at_ms: Option<i64>,
    ordinal: Option<u64>,
    facts: &mut Vec<LogFact>,
) {
    let Ok(envelope) = serde_json::from_str::<EventTypeEnvelope>(line) else {
        return;
    };
    let Some(event_type) = envelope.payload.and_then(|payload| payload.event_type) else {
        return;
    };

    match event_type.as_str() {
        "task_started" => push_lifecycle(rollout_id, at_ms, facts, Lifecycle::Started),
        "task_complete" => push_lifecycle(rollout_id, at_ms, facts, Lifecycle::Complete),
        "turn_aborted" => push_lifecycle(rollout_id, at_ms, facts, Lifecycle::Aborted),
        "token_count" => extract_token_count(scope, line, at_ms, ordinal, facts),
        "item_completed" => extract_item_completed(rollout_id, scope, line, at_ms, facts),
        _ => {}
    }
}

#[derive(Debug)]
enum Lifecycle {
    Started,
    Complete,
    Aborted,
}

fn push_lifecycle(
    rollout_id: &str,
    at_ms: Option<i64>,
    facts: &mut Vec<LogFact>,
    lifecycle: Lifecycle,
) {
    let Some(at_ms) = at_ms else {
        return;
    };
    let fact = match lifecycle {
        Lifecycle::Started => LogFact::CodexTurnStarted {
            rollout_id: rollout_id.to_owned(),
            at_ms,
        },
        Lifecycle::Complete => LogFact::CodexTurnComplete {
            rollout_id: rollout_id.to_owned(),
            at_ms,
        },
        Lifecycle::Aborted => LogFact::CodexTurnAborted {
            rollout_id: rollout_id.to_owned(),
            at_ms,
        },
    };
    facts.push(fact);
}

fn extract_token_count(
    scope: &SessionScope,
    line: &str,
    at_ms: Option<i64>,
    ordinal: Option<u64>,
    facts: &mut Vec<LogFact>,
) {
    let (Some(at_ms), Some(ordinal)) = (at_ms, ordinal) else {
        return;
    };
    let Ok(envelope) = serde_json::from_str::<TokenCountEnvelope>(line) else {
        return;
    };
    let Some(usage) = envelope
        .payload
        .and_then(|payload| payload.info)
        .and_then(|info| info.last_token_usage)
    else {
        return;
    };
    let output_tokens = usage
        .output_tokens
        .unwrap_or_default()
        .saturating_add(usage.reasoning_output_tokens.unwrap_or_default());

    // Codex token_count records carry no turn_id. Ordinal is unique within the
    // rollout and the SessionScope supplies the remaining sample identity.
    facts.push(LogFact::Usage {
        scope: scope.clone(),
        at_ms,
        sample_id: ordinal.to_string(),
        output_tokens,
        model: None,
        effort: None,
    });
}

fn extract_item_completed(
    rollout_id: &str,
    scope: &SessionScope,
    line: &str,
    at_ms: Option<i64>,
    facts: &mut Vec<LogFact>,
) {
    let Ok(envelope) = serde_json::from_str::<ItemTypeEnvelope>(line) else {
        return;
    };
    let Some(item_type) = envelope
        .payload
        .and_then(|payload| payload.item)
        .and_then(|item| item.item_type)
    else {
        return;
    };

    match item_type.as_str() {
        "AgentMessage" => extract_agent_message(scope, line, at_ms, facts),
        "CommandExecution" => extract_command_execution(rollout_id, scope, line, at_ms, facts),
        _ => {}
    }
}

fn extract_agent_message(
    scope: &SessionScope,
    line: &str,
    at_ms: Option<i64>,
    facts: &mut Vec<LogFact>,
) {
    let Some(at_ms) = at_ms else {
        return;
    };
    let Ok(envelope) = serde_json::from_str::<AgentMessageEnvelope>(line) else {
        return;
    };
    let phase = envelope
        .payload
        .and_then(|payload| payload.item)
        .and_then(|item| item.phase);
    if phase.as_deref() != Some("commentary") {
        return;
    }

    let Ok(envelope) = serde_json::from_str::<AgentCommentaryEnvelope>(line) else {
        return;
    };
    let Some(text) = envelope
        .payload
        .and_then(|payload| payload.item)
        .and_then(|item| item.content)
        .and_then(|content| content.0)
        .and_then(|content| content.text)
    else {
        return;
    };
    if text.contains(['\n', '\r']) || text.chars().count() > 60 {
        return;
    }

    facts.push(LogFact::Activity {
        scope: scope.clone(),
        at_ms,
        line: truncate_60(&sanitize_controller_text(&text)),
    });
}

fn extract_command_execution(
    rollout_id: &str,
    scope: &SessionScope,
    line: &str,
    at_ms: Option<i64>,
    facts: &mut Vec<LogFact>,
) {
    let Ok(envelope) = serde_json::from_str::<CommandExecutionEnvelope>(line) else {
        return;
    };
    let Some(item) = envelope.payload.and_then(|payload| payload.item) else {
        return;
    };

    if let Some(pid) = item
        .process_id
        .as_ref()
        .and_then(ProcessId::as_str)
        .and_then(parse_decimal)
    {
        facts.push(LogFact::CodexPid {
            rollout_id: rollout_id.to_owned(),
            pid,
        });
    }

    let (Some(at_ms), Some(command)) = (at_ms, item.command.as_deref()) else {
        return;
    };
    let cwd = item.cwd.as_deref().unwrap_or_default();
    if let Some(line) = command_activity(command, cwd) {
        facts.push(LogFact::Activity {
            scope: scope.clone(),
            at_ms,
            line,
        });
    }
}

fn command_activity(command: &[String], cwd: &str) -> Option<String> {
    if command.is_empty() {
        return None;
    }
    let mut script = None;
    for (index, argument) in command.iter().enumerate() {
        if matches!(argument.as_str(), "-c" | "-lc" | "-ic" | "-lic") {
            script = Some(Cow::Borrowed(command.get(index + 1)?.as_str()));
            break;
        }
    }
    let script = script.unwrap_or_else(|| Cow::Owned(command.join(" ")));
    let sanitized = sanitize_controller_text(&script);
    let relative = sanitized
        .split_whitespace()
        .map(|token| {
            if token.starts_with('/') {
                repo_relative(token, cwd)
            } else {
                token.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    let line = sanitize_command_script(&relative);
    (!line.is_empty()).then_some(line)
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::fs;
    use std::path::Path;

    use serde::de::DeserializeOwned;

    use super::*;
    use crate::provider::facts::{CodexInternal, LogFact, SessionScope};

    const ROLLOUT: &str = "6f9bdfa0-1502-4a37-97aa-c45591141130";

    fn fixture(name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/provider-logs")
            .join(name);
        fs::read_to_string(path).expect("fixture must be readable")
    }

    fn fixture_facts(name: &str, rollout_id: &str) -> Vec<LogFact> {
        fixture(name)
            .lines()
            .flat_map(|line| extract_codex_line(rollout_id, line))
            .collect()
    }

    fn assert_bounded_debug<T>(source: &str, body_markers: &[&str])
    where
        T: Debug + DeserializeOwned,
    {
        assert!(source.len() > 500, "source was only {} bytes", source.len());
        for marker in body_markers {
            assert!(source.contains(marker), "source lacks marker {marker:?}");
        }

        let envelope: T =
            serde_json::from_str(source).expect("allowlisted envelope must deserialize");
        let debug = format!("{envelope:?}");

        assert!(
            debug.len() < 512,
            "debug representation was {} bytes",
            debug.len()
        );
        for marker in body_markers {
            assert!(!debug.contains(marker), "debug retained marker {marker:?}");
        }
    }

    #[test]
    fn first_session_meta_wins_second_ignored() {
        let metas = fixture_facts("codex-internal-subagents.jsonl", "discovered-rollout")
            .into_iter()
            .filter(|fact| matches!(fact, LogFact::CodexMeta { .. }))
            .collect::<Vec<_>>();

        assert_eq!(metas.len(), 2);
        assert_eq!(
            metas.first(),
            Some(&LogFact::CodexMeta {
                rollout_id: "273e0c2b-4af4-4014-b24c-8b0d03ba8905".to_owned(),
                cwd: "/home/user/git/example/herdr-top".to_owned(),
                originator: "codex".to_owned(),
                internal: Some(CodexInternal::ThreadSpawn {
                    parent_thread_id: "69c67f5c-9d6d-4976-8465-5e6a31df2c0b".to_owned(),
                    nickname: Some("Ada".to_owned()),
                    role: Some("reviewer".to_owned()),
                }),
                cli_version: "0.149.0".to_owned(),
            })
        );
        assert!(matches!(
            metas.get(1),
            Some(LogFact::CodexMeta { originator, .. }) if originator == "codex_cli"
        ));
    }

    #[test]
    fn turn_context_per_turn_model_and_effort() {
        let turns = fixture_facts("codex-exec-resume-appended.jsonl", ROLLOUT)
            .into_iter()
            .filter(|fact| matches!(fact, LogFact::CodexTurn { .. }))
            .collect::<Vec<_>>();

        assert_eq!(
            turns,
            vec![
                LogFact::CodexTurn {
                    rollout_id: ROLLOUT.to_owned(),
                    turn_id: "1fd2a4db-5b53-454f-a4b3-d830ef95e20a".to_owned(),
                    model: "gpt-5.6-terra".to_owned(),
                    effort: Some("low".to_owned()),
                    sandbox: Some("read-only".to_owned()),
                },
                LogFact::CodexTurn {
                    rollout_id: ROLLOUT.to_owned(),
                    turn_id: "af7273d3-3951-468f-af26-ab16494c09d4".to_owned(),
                    model: "gpt-5.6-sol".to_owned(),
                    effort: Some("xhigh".to_owned()),
                    sandbox: Some("workspace-write".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn commentary_preferred_then_sanitized_argv_script() {
        let activities = fixture_facts("codex-exec.jsonl", ROLLOUT)
            .into_iter()
            .filter_map(|fact| match fact {
                LogFact::Activity { line, .. } => Some(line),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            activities,
            vec![
                "Cache report boundaries are under review.",
                "cargo test --locked --test fictional_cache_report",
            ]
        );
    }

    #[test]
    fn pid_parses_from_json_string() {
        let pids = fixture_facts("codex-exec.jsonl", ROLLOUT)
            .into_iter()
            .filter(|fact| matches!(fact, LogFact::CodexPid { .. }))
            .collect::<Vec<_>>();

        assert_eq!(
            pids,
            vec![LogFact::CodexPid {
                rollout_id: ROLLOUT.to_owned(),
                pid: 42_420,
            }]
        );
    }

    #[test]
    fn usage_is_turn_delta_not_total_sum() {
        let usage = fixture_facts("codex-exec-resume-appended.jsonl", ROLLOUT)
            .into_iter()
            .filter_map(|fact| match fact {
                LogFact::Usage {
                    sample_id,
                    output_tokens,
                    model,
                    effort,
                    ..
                } => Some((sample_id, output_tokens, model, effort)),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            usage,
            vec![
                ("3".to_owned(), 150, None, None),
                ("8".to_owned(), 350, None, None),
            ]
        );
        let total = usage.iter().map(|(_, tokens, _, _)| tokens).sum::<u64>();
        let max_sample = usage
            .iter()
            .map(|(_, tokens, _, _)| *tokens)
            .max()
            .expect("fixture has usage samples");
        let last_sample = usage
            .last()
            .map(|(_, tokens, _, _)| *tokens)
            .expect("fixture has a last usage sample");
        assert_eq!(total, 500);
        assert_ne!(total, max_sample);
        assert_ne!(total, last_sample);
    }

    #[test]
    fn internal_subagent_both_shapes() {
        let internals = [
            ("codex-exec.jsonl", ROLLOUT),
            ("codex-internal-subagents.jsonl", "discovered-rollout"),
        ]
        .into_iter()
        .flat_map(|(name, rollout)| fixture_facts(name, rollout))
        .filter_map(|fact| match fact {
            LogFact::CodexMeta {
                internal: Some(internal),
                ..
            } => Some(internal),
            _ => None,
        })
        .collect::<Vec<_>>();

        assert_eq!(
            internals,
            vec![
                CodexInternal::Named {
                    name: "guardian".to_owned(),
                },
                CodexInternal::ThreadSpawn {
                    parent_thread_id: "69c67f5c-9d6d-4976-8465-5e6a31df2c0b".to_owned(),
                    nickname: Some("Ada".to_owned()),
                    role: Some("reviewer".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn turn_aborted_maps_to_cancelled_fact() {
        let line = r#"{"timestamp":"2026-08-24T06:00:00.123Z","ordinal":14,"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"49986dbe-bd02-4b81-b8c5-91d5d0b2830c"}}"#;

        assert!(
            extract_codex_line(ROLLOUT, line).contains(&LogFact::CodexTurnAborted {
                rollout_id: ROLLOUT.to_owned(),
                at_ms: 1_787_551_200_123,
            })
        );
    }

    #[test]
    fn file_uri_cwd_relativizes() {
        let line = r#"{"timestamp":"2026-08-24T02:00:02.000Z","ordinal":9,"type":"event_msg","payload":{"type":"item_completed","item":{"type":"CommandExecution","process_id":"7","command":["/bin/bash","-lc","cargo test /home/user/git/example/herdr-top/src/provider/facts.rs"],"cwd":"file:///home/user/git/example/herdr-top"}}}"#;

        assert!(
            extract_codex_line(ROLLOUT, line).contains(&LogFact::Activity {
                scope: SessionScope::Codex {
                    rollout_id: ROLLOUT.to_owned(),
                },
                at_ms: 1_787_536_802_000,
                line: "cargo test src/provider/facts.rs".to_owned(),
            })
        );
    }

    #[test]
    fn bodies_never_enter_facts() {
        let facts = [
            "codex-exec.jsonl",
            "codex-exec-resume-appended.jsonl",
            "codex-internal-subagents.jsonl",
        ]
        .into_iter()
        .flat_map(|name| fixture_facts(name, ROLLOUT))
        .collect::<Vec<_>>();
        let debug = format!("{facts:?}");
        let forbidden = [
            "cache_report_orders_normalized_names",
            "Synthetic warning stream",
            "summary: fresh=1 approaching=1 expired=1",
            "Synthetic cache-report tests passed",
            "Q2FjaGVSZXBvcnRTdGF0",
            "Evaluate invented metadata for deterministic ordering",
            "Synthetic fixture skill text",
            "Review the fictional cache report using only invented metadata",
            "Synthetic cache probe output: atlas fresh",
        ];

        for marker in forbidden {
            assert!(
                !debug.contains(marker),
                "fact retained body marker {marker:?}"
            );
        }
    }

    #[test]
    fn lifecycle_uses_envelope_timestamp() {
        let lifecycle = fixture_facts("codex-exec.jsonl", ROLLOUT)
            .into_iter()
            .filter(|fact| {
                matches!(
                    fact,
                    LogFact::CodexTurnStarted { .. } | LogFact::CodexTurnComplete { .. }
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            lifecycle,
            vec![
                LogFact::CodexTurnStarted {
                    rollout_id: ROLLOUT.to_owned(),
                    at_ms: 1_787_536_800_020,
                },
                LogFact::CodexTurnComplete {
                    rollout_id: ROLLOUT.to_owned(),
                    at_ms: 1_787_536_802_020,
                },
            ]
        );
    }

    #[test]
    fn commentary_rejects_nonqualifying_text() {
        let wrong_phase = r#"{"timestamp":"2026-08-24T02:00:01.010Z","ordinal":8,"type":"event_msg","payload":{"type":"item_completed","item":{"type":"AgentMessage","content":[{"text":"short private final"}],"phase":"final"}}}"#;
        let multiline = r#"{"timestamp":"2026-08-24T02:00:01.010Z","ordinal":8,"type":"event_msg","payload":{"type":"item_completed","item":{"type":"AgentMessage","content":[{"text":"first\nsecond"}],"phase":"commentary"}}}"#;
        let overlong = format!(
            r#"{{"timestamp":"2026-08-24T02:00:01.010Z","ordinal":8,"type":"event_msg","payload":{{"type":"item_completed","item":{{"type":"AgentMessage","content":[{{"text":"{}"}}],"phase":"commentary"}}}}}}"#,
            "x".repeat(61)
        );

        for line in [wrong_phase, multiline, overlong.as_str()] {
            assert!(
                extract_codex_line(ROLLOUT, line)
                    .into_iter()
                    .all(|fact| !matches!(fact, LogFact::Activity { .. }))
            );
        }
    }

    #[test]
    fn unknown_and_malformed_records_skip_silently() {
        let malformed = extract_codex_line(ROLLOUT, "not json");
        assert!(malformed.is_empty());

        let unknown = r#"{"timestamp":"2026-08-24T02:00:00.005Z","ordinal":1,"type":"future_record","payload":{"private":"body"}}"#;
        assert_eq!(
            extract_codex_line(ROLLOUT, unknown),
            vec![LogFact::Append {
                scope: SessionScope::Codex {
                    rollout_id: ROLLOUT.to_owned(),
                },
                at_ms: 1_787_536_800_005,
            }]
        );
    }

    #[test]
    fn record_envelope_debug_excludes_response_item_bodies() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(10).expect("fixture has tool output");
        assert_bounded_debug::<RecordEnvelope>(
            line,
            &["Synthetic cache probe output: atlas fresh"],
        );
    }

    #[test]
    fn session_meta_envelope_debug_excludes_base_instructions() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().next().expect("fixture has session metadata");
        assert_bounded_debug::<SessionMetaEnvelope>(
            line,
            &["Evaluate invented metadata for deterministic ordering"],
        );
    }

    #[test]
    fn turn_context_envelope_debug_excludes_nonallowlisted_context() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(2).expect("fixture has turn context");
        assert_bounded_debug::<TurnContextEnvelope>(line, &["exclude_slash_tmp"]);
    }

    #[test]
    fn event_type_envelope_debug_excludes_item_body() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(9).expect("fixture has command output");
        assert_bounded_debug::<EventTypeEnvelope>(line, &["Synthetic diagnostic table"]);
    }

    #[test]
    fn item_type_envelope_debug_excludes_command_output() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(9).expect("fixture has command output");
        assert_bounded_debug::<ItemTypeEnvelope>(line, &["Synthetic warning stream"]);
    }

    #[test]
    fn agent_message_envelope_debug_excludes_non_agent_item_body() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(9).expect("fixture has command output");
        assert_bounded_debug::<AgentMessageEnvelope>(line, &["Synthetic diagnostic table"]);
    }

    #[test]
    fn agent_commentary_envelope_debug_excludes_non_agent_item_body() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(9).expect("fixture has command output");
        assert_bounded_debug::<AgentCommentaryEnvelope>(line, &["Synthetic diagnostic table"]);
    }

    #[test]
    fn command_execution_envelope_debug_excludes_command_output() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(9).expect("fixture has command output");
        assert_bounded_debug::<CommandExecutionEnvelope>(
            line,
            &[
                "cache_report_orders_normalized_names",
                "Synthetic warning stream",
                "Synthetic cache-report tests passed",
            ],
        );
    }

    #[test]
    fn token_count_envelope_debug_excludes_rate_limits() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(11).expect("fixture has token count");
        assert_bounded_debug::<TokenCountEnvelope>(line, &["Synthetic cache review", "73.00"]);
    }
}
