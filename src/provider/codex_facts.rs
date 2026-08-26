//! Typed allowlist extraction for Codex rollout records.
//!
//! Extraction is stateless and emits one [`LogFact::CodexMeta`] for every
//! `session_meta` record. A file-order consumer preserves the first emitted
//! metadata fact as rollout identity and ignores later copies.

use std::borrow::Cow;
use std::fmt;

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::model::{TokenBreakdown, sanitize_controller_text};

use super::claude_facts::{parse_decimal, parse_timestamp_ms};
use super::facts::{
    ActivitySource, CodexInternal, EvidenceId, LogFact, SessionScope, is_uuid_token, repo_relative,
    sanitize_command_script, truncate_60,
};

#[derive(Debug, Deserialize)]
struct RecordEnvelope {
    timestamp: Option<String>,
    #[serde(rename = "type")]
    record_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SessionMetaEnvelope {
    payload: Option<SessionMetaPayload>,
}

#[derive(Debug, Deserialize)]
struct SessionMetaPayload {
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
    agent_thread_id: Option<String>,
    occurred_at_ms: Option<i64>,
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
    model_context_window: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct LastTokenUsage {
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    cache_write_input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    reasoning_output_tokens: Option<u64>,
    total_tokens: Option<u64>,
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
/// record so the Task 5 file-order consumer can retain the first record as
/// identity. `record_ordinal` is the caller-maintained zero-based record index
/// within the artifact.
#[must_use]
pub fn extract_codex_line(rollout_id: &str, record_ordinal: u64, line: &str) -> Vec<LogFact> {
    let scope = SessionScope::Codex {
        rollout_id: rollout_id.to_owned(),
    };
    let mut facts = Vec::new();

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
            extract_event(rollout_id, &scope, line, at_ms, record_ordinal, &mut facts)
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
        rollout_id: rollout_id.to_owned(),
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
    record_ordinal: u64,
    facts: &mut Vec<LogFact>,
) {
    let Ok(envelope) = serde_json::from_str::<EventTypeEnvelope>(line) else {
        return;
    };
    let Some(payload) = envelope.payload else {
        return;
    };
    let Some(event_type) = payload.event_type else {
        return;
    };

    match event_type.as_str() {
        "task_started" => push_lifecycle(rollout_id, at_ms, facts, Lifecycle::Started),
        "task_complete" => push_lifecycle(rollout_id, at_ms, facts, Lifecycle::Complete),
        "turn_aborted" => push_lifecycle(rollout_id, at_ms, facts, Lifecycle::Aborted),
        "token_count" => extract_token_count(scope, line, at_ms, record_ordinal, facts),
        "item_completed" => extract_item_completed(rollout_id, scope, line, at_ms, facts),
        "sub_agent_activity" => {
            if let (Some(agent_thread_id), Some(at_ms)) = (
                payload.agent_thread_id.filter(|id| is_uuid_token(id)),
                payload.occurred_at_ms.filter(|at_ms| *at_ms > 0),
            ) {
                facts.push(LogFact::EvidenceId {
                    parent: scope.clone(),
                    id: EvidenceId::Uuid(agent_thread_id),
                    at_ms,
                });
            }
        }
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
    record_ordinal: u64,
    facts: &mut Vec<LogFact>,
) {
    let Some(at_ms) = at_ms else {
        return;
    };
    let Ok(envelope) = serde_json::from_str::<TokenCountEnvelope>(line) else {
        return;
    };
    let Some(info) = envelope.payload.and_then(|payload| payload.info) else {
        return;
    };
    let Some(usage) = info.last_token_usage else {
        return;
    };
    let output_tokens = usage.output_tokens.unwrap_or_default();

    // The caller-maintained record ordinal is unique only within this artifact;
    // SessionScope supplies the remaining sample identity.
    facts.push(LogFact::Usage {
        scope: scope.clone(),
        at_ms,
        sample_id: record_ordinal.to_string(),
        output_tokens,
        token_breakdown: TokenBreakdown {
            input_tokens: usage.input_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            cache_write_input_tokens: usage.cache_write_input_tokens,
            reasoning_output_tokens: usage.reasoning_output_tokens,
            total_tokens: usage.total_tokens,
            context_window: info.model_context_window,
        },
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
        source: ActivitySource::Commentary,
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
            source: ActivitySource::Command,
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
    let relative = script
        .split_whitespace()
        .map(sanitize_controller_text)
        .map(|token| relativize_command_token(&token, cwd))
        .collect::<Vec<_>>()
        .join(" ");
    let line = sanitize_command_script(&relative);
    (!line.is_empty()).then_some(line)
}

fn relativize_command_token(token: &str, cwd: &str) -> String {
    if let Some(relative) = relativize_absolute_fragment(token, cwd) {
        return relative;
    }
    if let Some((prefix, path)) = token.rsplit_once('=')
        && let Some(relative) = relativize_absolute_fragment(path, cwd)
    {
        return format!("{prefix}={relative}");
    }
    token.to_owned()
}

fn relativize_absolute_fragment(fragment: &str, cwd: &str) -> Option<String> {
    if fragment.starts_with('/') {
        return Some(repo_relative(fragment, cwd));
    }
    let quote @ ('\'' | '"') = fragment.chars().next()? else {
        return None;
    };
    let path = fragment.strip_prefix(quote)?.strip_suffix(quote)?;
    path.starts_with('/')
        .then(|| format!("{quote}{}{quote}", repo_relative(path, cwd)))
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::fs;
    use std::path::Path;

    use serde::de::DeserializeOwned;

    use super::*;
    use crate::provider::facts::{ActivitySource, CodexInternal, LogFact, SessionScope};

    const ROLLOUT: &str = "6f9bdfa0-1502-4a37-97aa-c45591141130";
    const COMMENTARY_LINE: &str = "Cache report boundaries are under review.";
    const NON_COMMENTARY_BODY_MARKER: &str = "Synthetic non-commentary cache report body marker";

    fn fixture(name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/provider-logs")
            .join(name);
        fs::read_to_string(path).expect("fixture must be readable")
    }

    fn fixture_facts(name: &str, rollout_id: &str) -> Vec<LogFact> {
        fixture(name)
            .lines()
            .enumerate()
            .flat_map(|(index, line)| extract_codex_line(rollout_id, index as u64, line))
            .collect()
    }

    fn assert_bounded_debug<T>(source: &str, body_markers: &[&str])
    where
        T: Debug + DeserializeOwned,
    {
        assert_bounded_debug_with_min::<T>(source, 500, body_markers);
    }

    fn assert_bounded_debug_with_min<T>(
        source: &str,
        minimum_source_len: usize,
        body_markers: &[&str],
    ) where
        T: Debug + DeserializeOwned,
    {
        for marker in body_markers {
            assert!(source.contains(marker), "source lacks marker {marker:?}");
        }

        let debug = debug_envelope_with_min::<T>(source, minimum_source_len);
        for marker in body_markers {
            assert!(!debug.contains(marker), "debug retained marker {marker:?}");
        }
    }

    fn debug_envelope_with_min<T>(source: &str, minimum_source_len: usize) -> String
    where
        T: Debug + DeserializeOwned,
    {
        assert!(
            source.len() > minimum_source_len,
            "source was only {} bytes, expected more than {minimum_source_len}",
            source.len()
        );
        let envelope: T =
            serde_json::from_str(source).expect("allowlisted envelope must deserialize");
        let debug = format!("{envelope:?}");

        assert!(
            debug.len() < 512,
            "debug representation was {} bytes",
            debug.len()
        );
        debug
    }

    #[test]
    fn session_meta_records_surface_in_file_order() {
        let metas = fixture_facts("codex-internal-subagents.jsonl", "discovered-rollout")
            .into_iter()
            .filter(|fact| matches!(fact, LogFact::CodexMeta { .. }))
            .collect::<Vec<_>>();

        assert_eq!(metas.len(), 2);
        assert_eq!(
            metas.first(),
            Some(&LogFact::CodexMeta {
                rollout_id: "discovered-rollout".to_owned(),
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
        // The stateless extractor surfaces both; Task 5 enforces first-wins.
        assert!(matches!(
            metas.get(1),
            Some(LogFact::CodexMeta { rollout_id, originator, .. })
                if rollout_id == "discovered-rollout" && originator == "codex_cli"
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
                LogFact::Activity { source, line, .. } => Some((source, line)),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            activities,
            vec![
                (
                    ActivitySource::Commentary,
                    "Cache report boundaries are under review.".to_owned(),
                ),
                (
                    ActivitySource::Command,
                    "cargo test --locked --test fictional_cache_report".to_owned(),
                ),
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
                ("3".to_owned(), 120, None, None),
                ("4".to_owned(), 80, None, None),
                ("9".to_owned(), 250, None, None),
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
        assert_eq!(total, 450);
        assert_ne!(total, 570);
        assert_ne!(total, max_sample);
        assert_ne!(total, last_sample);
    }

    #[test]
    fn token_count_without_provider_ordinal_uses_record_ordinal() {
        let line = r#"{"timestamp":"2026-08-24T03:00:00.100Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"output_tokens":120,"reasoning_output_tokens":30}}}}"#;
        let usage = extract_codex_line(ROLLOUT, 41, line)
            .into_iter()
            .find(|fact| matches!(fact, LogFact::Usage { .. }));

        assert_eq!(
            usage,
            Some(LogFact::Usage {
                scope: SessionScope::Codex {
                    rollout_id: ROLLOUT.to_owned(),
                },
                at_ms: 1_787_540_400_100,
                sample_id: "41".to_owned(),
                output_tokens: 120,
                token_breakdown: TokenBreakdown {
                    reasoning_output_tokens: Some(30),
                    ..TokenBreakdown::default()
                },
                model: None,
                effort: None,
            })
        );
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
            extract_codex_line(ROLLOUT, 14, line).contains(&LogFact::CodexTurnAborted {
                rollout_id: ROLLOUT.to_owned(),
                at_ms: 1_787_551_200_123,
            })
        );
    }

    #[test]
    fn file_uri_cwd_relativizes() {
        let line = r#"{"timestamp":"2026-08-24T02:00:02.000Z","ordinal":9,"type":"event_msg","payload":{"type":"item_completed","item":{"type":"CommandExecution","process_id":"7","command":["/bin/bash","-lc","cargo test /home/user/git/example/herdr-top/src/provider/facts.rs"],"cwd":"file:///home/user/git/example/herdr-top"}}}"#;

        assert!(
            extract_codex_line(ROLLOUT, 9, line).contains(&LogFact::Activity {
                scope: SessionScope::Codex {
                    rollout_id: ROLLOUT.to_owned(),
                },
                at_ms: 1_787_536_802_000,
                source: ActivitySource::Command,
                line: "cargo test src/provider/facts.rs".to_owned(),
            })
        );
    }

    #[test]
    fn command_activity_strips_env_wrapped_secret_assignments() {
        let secret = "sk-live-x";
        let command = [
            "/bin/bash".to_owned(),
            "-lc".to_owned(),
            format!("env API_TOKEN={secret} curl https://example.test"),
        ];

        let head = command_activity(&command, "/repo").expect("command head");

        assert!(!head.contains(secret), "secret leaked in {head:?}");
        assert_eq!(head, "curl https://example.test");
    }

    #[test]
    fn command_activity_raw_tabs_do_not_leak_env_assignment() {
        let command = [
            "/bin/bash".to_owned(),
            "-lc".to_owned(),
            "env\tAPI_TOKEN=sk-x\tcurl".to_owned(),
        ];

        let head = command_activity(&command, "/repo").expect("command head");

        assert!(!head.contains("sk-x"), "secret leaked in {head:?}");
    }

    #[test]
    fn command_activity_raw_leading_newline_does_not_leak_env_assignment() {
        let command = [
            "/bin/bash".to_owned(),
            "-lc".to_owned(),
            "\nenv API_TOKEN=sk-x curl".to_owned(),
        ];

        let head = command_activity(&command, "/repo").expect("command head");

        assert!(!head.contains("sk-x"), "secret leaked in {head:?}");
    }

    #[test]
    fn command_activity_quoted_assignment_does_not_leak_secret() {
        let command = [
            "/bin/bash".to_owned(),
            "-lc".to_owned(),
            "env 'API_TOKEN=sk-x' curl".to_owned(),
        ];

        let head = command_activity(&command, "/repo").expect("command head");

        assert!(!head.contains("sk-x"), "secret leaked in {head:?}");
    }

    #[test]
    fn command_activity_raw_tab_relativizes_absolute_path() {
        let absolute = "/home/alice/private/key";
        let command = [
            "/bin/bash".to_owned(),
            "-lc".to_owned(),
            format!("cat\t{absolute}"),
        ];

        let head = command_activity(&command, "/repo").expect("command head");

        assert_eq!(head, "cat key");
        assert!(!head.contains(absolute), "absolute path leaked in {head:?}");
    }

    #[test]
    fn command_activity_never_retains_raw_control_characters() {
        let command = [
            "/bin/bash".to_owned(),
            "-lc".to_owned(),
            "printf foo\u{7}bar\tbaz\nqux".to_owned(),
        ];

        let head = command_activity(&command, "/repo").expect("command head");

        assert!(
            !head.chars().any(char::is_control),
            "raw control character leaked in {head:?}"
        );
    }

    #[test]
    fn command_activity_relativizes_equals_embedded_absolute_path() {
        let absolute = "/repo/src/private.rs";
        let command = [
            "/bin/bash".to_owned(),
            "-lc".to_owned(),
            format!("cargo test --file={absolute}"),
        ];

        let head = command_activity(&command, "/repo").expect("command head");

        assert!(head.contains("--file=src/private.rs"), "head was {head:?}");
        assert!(!head.contains(absolute), "absolute path leaked in {head:?}");
    }

    #[test]
    fn command_activity_relativizes_quoted_absolute_path() {
        let absolute = "/repo/src/private.rs";
        let command = [
            "/bin/bash".to_owned(),
            "-lc".to_owned(),
            format!("cargo test '{absolute}'"),
        ];

        let head = command_activity(&command, "/repo").expect("command head");

        assert!(head.contains("'src/private.rs'"), "head was {head:?}");
        assert!(!head.contains(absolute), "absolute path leaked in {head:?}");
    }

    #[test]
    fn command_activity_preserves_quotes_around_equals_embedded_paths() {
        for quote in ['\'', '"'] {
            let absolute = "/repo/src/private.rs";
            let command = [
                "/bin/bash".to_owned(),
                "-lc".to_owned(),
                format!("cargo test --file={quote}{absolute}{quote}"),
            ];

            let head = command_activity(&command, "/repo").expect("command head");

            assert!(
                head.contains(&format!("--file={quote}src/private.rs{quote}")),
                "head was {head:?}"
            );
            assert!(!head.contains(absolute), "absolute path leaked in {head:?}");
        }
    }

    #[test]
    fn bodies_never_enter_facts() {
        let inputs = [
            "codex-exec.jsonl",
            "codex-exec-resume-appended.jsonl",
            "codex-internal-subagents.jsonl",
        ]
        .map(fixture);
        let facts = inputs
            .iter()
            .flat_map(|input| {
                input
                    .lines()
                    .enumerate()
                    .flat_map(|(index, line)| extract_codex_line(ROLLOUT, index as u64, line))
            })
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
            NON_COMMENTARY_BODY_MARKER,
        ];

        for marker in forbidden {
            assert!(
                inputs.iter().any(|input| input.contains(marker)),
                "raw fixture input lacks body marker {marker:?}"
            );
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
                extract_codex_line(ROLLOUT, 8, line)
                    .into_iter()
                    .all(|fact| !matches!(fact, LogFact::Activity { .. }))
            );
        }
    }

    #[test]
    fn unknown_and_malformed_records_skip_silently() {
        let malformed = extract_codex_line(ROLLOUT, 0, "not json");
        assert!(malformed.is_empty());

        let unknown = r#"{"timestamp":"2026-08-24T02:00:00.005Z","ordinal":1,"type":"future_record","payload":{"private":"body"}}"#;
        assert_eq!(
            extract_codex_line(ROLLOUT, 1, unknown),
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
        let line = input.lines().nth(11).expect("fixture has tool output");
        assert_bounded_debug::<RecordEnvelope>(
            line,
            &["Synthetic cache probe output: atlas fresh"],
        );
    }

    #[test]
    fn world_state_envelope_debug_stays_type_tag_only() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(1).expect("fixture has world state");
        assert_bounded_debug::<RecordEnvelope>(line, &["Synthetic fixture skill text"]);
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
        let line = input.lines().nth(10).expect("fixture has command output");
        assert_bounded_debug::<EventTypeEnvelope>(line, &["Synthetic diagnostic table"]);
    }

    #[test]
    fn item_type_envelope_debug_excludes_command_output() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(10).expect("fixture has command output");
        assert_bounded_debug::<ItemTypeEnvelope>(line, &["Synthetic warning stream"]);
    }

    #[test]
    fn agent_message_envelope_debug_excludes_non_agent_item_body() {
        let input = fixture("codex-exec.jsonl");
        let line = input
            .lines()
            .nth(9)
            .expect("fixture has non-commentary agent message");
        assert_bounded_debug::<AgentMessageEnvelope>(line, &[NON_COMMENTARY_BODY_MARKER]);
    }

    #[test]
    fn agent_commentary_envelope_debug_excludes_non_agent_item_body() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(8).expect("fixture has commentary");
        let debug = debug_envelope_with_min::<AgentCommentaryEnvelope>(line, 400);
        assert!(
            debug.contains(COMMENTARY_LINE),
            "debug omitted authorized commentary {COMMENTARY_LINE:?}"
        );
        for marker in [
            "cache_report_orders_normalized_names",
            "Synthetic warning stream",
            "summary: fresh=1 approaching=1 expired=1",
            "Synthetic cache-report tests passed",
            "Q2FjaGVSZXBvcnRTdGF0",
            "Evaluate invented metadata for deterministic ordering",
            "Synthetic fixture skill text",
            "Review the fictional cache report using only invented metadata",
            "Synthetic cache probe output: atlas fresh",
            NON_COMMENTARY_BODY_MARKER,
        ] {
            assert!(!debug.contains(marker), "debug retained marker {marker:?}");
        }
    }

    #[test]
    fn non_commentary_agent_message_body_never_becomes_activity() {
        let input = fixture("codex-exec.jsonl");
        let line = input
            .lines()
            .nth(9)
            .expect("fixture has non-commentary agent message");
        assert!(line.contains(NON_COMMENTARY_BODY_MARKER));

        let facts = extract_codex_line(ROLLOUT, 9, line);
        assert!(
            facts
                .iter()
                .all(|fact| !matches!(fact, LogFact::Activity { .. }))
        );
        assert!(!format!("{facts:?}").contains(NON_COMMENTARY_BODY_MARKER));
    }

    #[test]
    fn command_execution_envelope_debug_excludes_command_output() {
        let input = fixture("codex-exec.jsonl");
        let line = input.lines().nth(10).expect("fixture has command output");
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
        let line = input.lines().nth(12).expect("fixture has token count");
        assert_bounded_debug::<TokenCountEnvelope>(line, &["Synthetic cache review", "73.00"]);
    }
}
