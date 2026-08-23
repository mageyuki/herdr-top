//! Typed allowlist extraction for Claude transcript records.

use serde::Deserialize;

use crate::model::sanitize_controller_text;

use super::facts::{LogFact, SessionScope, repo_relative, scan_raw_ids, truncate_60};

#[derive(Deserialize)]
struct RecordType {
    #[serde(rename = "type")]
    record_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AiTitleRecord {
    session_id: Option<String>,
    ai_title: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssistantRecord {
    timestamp: Option<String>,
    cwd: Option<String>,
    effort: Option<String>,
    message: Option<AssistantMessage>,
}

#[derive(Debug, Deserialize)]
struct AssistantMessage {
    id: Option<String>,
    model: Option<String>,
    usage: Option<AssistantUsage>,
    content: Option<Vec<ContentBlock>>,
}

#[derive(Debug, Deserialize)]
struct AssistantUsage {
    output_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    block_type: Option<String>,
    name: Option<String>,
    input: Option<ToolInput>,
}

#[derive(Debug, Deserialize)]
struct ToolInput {
    description: Option<String>,
    file_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserRecord {
    timestamp: Option<String>,
    tool_use_result: Option<ToolUseResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolUseResult {
    agent_id: Option<String>,
}

// Deliberate carve-in-2 exception: content is scanned transiently only for
// `<task-notification>` tags; nothing beyond the extracted task ID and status is retained.
#[derive(Deserialize)]
struct QueueOperationRecord {
    timestamp: Option<String>,
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetaRecord {
    agent_type: Option<String>,
    description: Option<String>,
    #[serde(rename = "toolUseId")]
    _tool_use_id: Option<String>,
    #[serde(rename = "spawnDepth")]
    _spawn_depth: Option<u32>,
}

/// Extracts allowlisted facts from one Claude JSONL transcript line.
#[must_use]
pub fn extract_claude_line(scope: &SessionScope, line: &str) -> Vec<LogFact> {
    let mut facts = scan_raw_ids(line)
        .into_iter()
        .map(|id| LogFact::EvidenceId {
            parent: scope.clone(),
            id,
        })
        .collect::<Vec<_>>();

    let Ok(record_type) = serde_json::from_str::<RecordType>(line) else {
        return facts;
    };
    match record_type.record_type.as_deref() {
        Some("ai-title") => extract_ai_title(line, &mut facts),
        Some("assistant") => extract_assistant(scope, line, &mut facts),
        Some("user") => extract_user(scope, line, &mut facts),
        Some("queue-operation") => extract_queue_operation(scope, line, &mut facts),
        _ => {}
    }
    facts
}

/// Extracts an allowlisted subagent-appearance fact from Claude `meta.json` bytes.
#[must_use]
pub fn extract_meta_json(parent: &str, agent_id: &str, bytes: &[u8]) -> Option<LogFact> {
    let record = serde_json::from_slice::<MetaRecord>(bytes).ok()?;
    Some(LogFact::SubagentAppeared {
        parent: parent.to_owned(),
        agent_id: agent_id.to_owned(),
        agent_type: record.agent_type?,
        description: record.description?,
    })
}

fn extract_ai_title(line: &str, facts: &mut Vec<LogFact>) {
    let Ok(record) = serde_json::from_str::<AiTitleRecord>(line) else {
        return;
    };
    if let (Some(session_id), Some(title)) = (record.session_id, record.ai_title) {
        facts.push(LogFact::AiTitle { session_id, title });
    }
}

fn extract_assistant(scope: &SessionScope, line: &str, facts: &mut Vec<LogFact>) {
    let Ok(record) = serde_json::from_str::<AssistantRecord>(line) else {
        return;
    };
    let Some(at_ms) = record.timestamp.as_deref().and_then(parse_timestamp_ms) else {
        return;
    };
    facts.push(LogFact::Append {
        scope: scope.clone(),
        at_ms,
    });

    let Some(message) = record.message else {
        return;
    };
    if let (Some(sample_id), Some(output_tokens)) = (
        message.id,
        message.usage.and_then(|usage| usage.output_tokens),
    ) {
        facts.push(LogFact::Usage {
            scope: scope.clone(),
            at_ms,
            sample_id,
            output_tokens,
            model: message.model,
            effort: record.effort,
        });
    }

    for block in message.content.unwrap_or_default() {
        if let Some(line) = activity_line(block, record.cwd.as_deref().unwrap_or_default()) {
            facts.push(LogFact::Activity {
                scope: scope.clone(),
                at_ms,
                line,
            });
        }
    }
}

fn extract_user(scope: &SessionScope, line: &str, facts: &mut Vec<LogFact>) {
    let Ok(record) = serde_json::from_str::<UserRecord>(line) else {
        return;
    };
    if let Some(at_ms) = record.timestamp.as_deref().and_then(parse_timestamp_ms) {
        facts.push(LogFact::Append {
            scope: scope.clone(),
            at_ms,
        });
    }
    let Some(result) = record.tool_use_result else {
        return;
    };
    if let Some(agent_id) = result.agent_id {
        facts.push(LogFact::SubagentEnded {
            parent: parent_id(scope).to_owned(),
            agent_id,
            failed: false,
        });
    }
}

fn extract_queue_operation(scope: &SessionScope, line: &str, facts: &mut Vec<LogFact>) {
    let Ok(record) = serde_json::from_str::<QueueOperationRecord>(line) else {
        return;
    };
    if let Some(at_ms) = record.timestamp.as_deref().and_then(parse_timestamp_ms) {
        facts.push(LogFact::Append {
            scope: scope.clone(),
            at_ms,
        });
    }
    let Some(content) = record.content else {
        return;
    };
    facts.extend(
        task_notifications(&content).map(|(agent_id, failed)| LogFact::SubagentEnded {
            parent: parent_id(scope).to_owned(),
            agent_id,
            failed,
        }),
    );
}

fn activity_line(block: ContentBlock, cwd: &str) -> Option<String> {
    if block.block_type.as_deref() != Some("tool_use") {
        return None;
    }
    let name = block.name?;
    let input = block.input.as_ref();
    let line = if name == "Bash" {
        if let Some(description) = input.and_then(|input| input.description.as_deref()) {
            description.to_owned()
        } else {
            name
        }
    } else if let Some(file_path) = input.and_then(|input| input.file_path.as_deref()) {
        format!("{name} {}", repo_relative(file_path, cwd))
    } else if matches!(name.as_str(), "Agent" | "Task") {
        input
            .and_then(|input| input.description.clone())
            .unwrap_or(name)
    } else {
        name
    };
    Some(truncate_60(&sanitize_controller_text(&line)))
}

fn task_notifications(content: &str) -> impl Iterator<Item = (String, bool)> + '_ {
    const OPEN: &str = "<task-notification>";
    const CLOSE: &str = "</task-notification>";

    let mut remainder = content;
    std::iter::from_fn(move || {
        loop {
            let open = remainder.find(OPEN)?;
            let after_open = &remainder[open + OPEN.len()..];
            let Some(close) = after_open.find(CLOSE) else {
                remainder = "";
                return None;
            };
            let block = &after_open[..close];
            remainder = &after_open[close + CLOSE.len()..];
            let Some(agent_id) = tag_value(block, "task-id") else {
                continue;
            };
            if agent_id.len() < 8
                || !agent_id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                continue;
            }
            let Some(status) = tag_value(block, "status") else {
                continue;
            };
            let failed = match status {
                "completed" => false,
                "failed" => true,
                _ => continue,
            };
            return Some((agent_id.to_owned(), failed));
        }
    })
}

fn tag_value<'a>(block: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = block.find(&open)? + open.len();
    let end = block[start..].find(&close)? + start;
    let value = block[start..end].trim();
    (!value.is_empty()).then_some(value)
}

fn parent_id(scope: &SessionScope) -> &str {
    match scope {
        SessionScope::ClaudeRoot(session_id) => session_id,
        SessionScope::ClaudeSubagent { parent, .. } => parent,
        SessionScope::Codex { rollout_id } => rollout_id,
    }
}

pub(crate) fn parse_timestamp_ms(timestamp: &str) -> Option<i64> {
    let (date_time, offset_seconds) = if let Some(value) = timestamp.strip_suffix('Z') {
        (value, 0_i64)
    } else {
        let separator = timestamp
            .char_indices()
            .rev()
            .find(|(index, value)| *index > 10 && matches!(value, '+' | '-'))
            .map(|(index, _)| index)?;
        let (value, offset) = timestamp.split_at(separator);
        let sign = if offset.starts_with('+') {
            1_i64
        } else {
            -1_i64
        };
        let (hours, minutes) = offset[1..].split_once(':')?;
        let hours = parse_decimal(hours)?;
        let minutes = parse_decimal(minutes)?;
        if hours > 23 || minutes > 59 {
            return None;
        }
        (value, sign * i64::from(hours * 3_600 + minutes * 60))
    };

    let (date, time) = date_time.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = parse_decimal(date_parts.next()?)?;
    let month = parse_decimal(date_parts.next()?)?;
    let day = parse_decimal(date_parts.next()?)?;
    if date_parts.next().is_some()
        || year > 9_999
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
    {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour = parse_decimal(time_parts.next()?)?;
    let minute = parse_decimal(time_parts.next()?)?;
    let seconds = time_parts.next()?;
    if time_parts.next().is_some() || hour > 23 || minute > 59 {
        return None;
    }
    let (second, millis) = match seconds.split_once('.') {
        Some((second, fraction)) => (parse_decimal(second)?, fraction_millis(fraction)?),
        None => (parse_decimal(seconds)?, 0),
    };
    if second > 59 {
        return None;
    }

    let days = days_from_civil(i64::from(year), month, day);
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour * 3_600 + minute * 60 + second))?
        .checked_sub(offset_seconds)?;
    seconds.checked_mul(1_000)?.checked_add(i64::from(millis))
}

pub(crate) fn parse_decimal(value: &str) -> Option<u32> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

pub(crate) fn fraction_millis(fraction: &str) -> Option<u32> {
    if fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let mut millis = 0_u32;
    for (index, byte) in fraction.bytes().take(3).enumerate() {
        millis += u32::from(byte - b'0') * 10_u32.pow(2 - index as u32);
    }
    Some(millis)
}

pub(crate) const fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(400) || (year.is_multiple_of(4) && !year.is_multiple_of(100)) => {
            29
        }
        2 => 28,
        _ => 0,
    }
}

pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::provider::facts::{EvidenceId, LogFact, SessionScope, repo_relative};

    const PARENT: &str = "13f03635-c1f6-46e2-8e52-83d217b6f01c";

    fn fixture(name: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/provider-logs")
            .join(name);
        fs::read_to_string(path).expect("fixture must be readable")
    }

    fn root_scope() -> SessionScope {
        SessionScope::ClaudeRoot(PARENT.to_owned())
    }

    #[test]
    fn meta_json_yields_role_and_subject() {
        let bytes = fixture("claude-subagent-meta.json");

        assert_eq!(
            extract_meta_json(PARENT, "a7189abbf3c5741ac", bytes.as_bytes()),
            Some(LogFact::SubagentAppeared {
                parent: PARENT.to_owned(),
                agent_id: "a7189abbf3c5741ac".to_owned(),
                agent_type: "reviewer".to_owned(),
                description: "Check deterministic cache report boundaries".to_owned(),
            })
        );
        assert_eq!(
            extract_meta_json(PARENT, "child", br#"{"agentType":"reviewer"}"#),
            None
        );
        assert_eq!(extract_meta_json(PARENT, "child", b"not json"), None);
    }

    #[test]
    fn ai_title_latest_is_file_order() {
        let input = fixture("claude-session.jsonl");
        let later = r#"{"type":"ai-title","sessionId":"session-later","aiTitle":"Later title"}"#;
        let titles = input
            .lines()
            .chain([later])
            .flat_map(|line| extract_claude_line(&root_scope(), line))
            .filter_map(|fact| match fact {
                LogFact::AiTitle { session_id, title } => Some((session_id, title)),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            titles,
            vec![
                (
                    PARENT.to_owned(),
                    "Add deterministic cache expiry reporting".to_owned()
                ),
                ("session-later".to_owned(), "Later title".to_owned()),
            ]
        );
        assert_eq!(
            titles.last().map(|(_, title)| title.as_str()),
            Some("Later title")
        );
    }

    #[test]
    fn bash_description_becomes_activity_line() {
        let activities = fixture("claude-session.jsonl")
            .lines()
            .flat_map(|line| extract_claude_line(&root_scope(), line))
            .filter_map(|fact| match fact {
                LogFact::Activity { line, .. } => Some(line),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert!(
            activities
                .contains(&"Ask an isolated reviewer to inspect cache-report ordering".to_owned())
        );
        assert!(
            !activities
                .iter()
                .any(|line| line.contains("CLAUDE_CONFIG_DIR"))
        );
        assert!(activities.iter().all(|line| line.chars().count() <= 60));
    }

    #[test]
    fn bash_without_description_uses_bare_tool_name() {
        let line = r#"{"type":"assistant","timestamp":"2026-08-24T01:00:03.000Z","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"echo private-command-body"}}]}}"#;
        assert!(line.contains("private-command-body"));
        let envelope: AssistantRecord =
            serde_json::from_str(line).expect("allowlisted envelope parses");
        assert!(!format!("{envelope:?}").contains("private-command-body"));

        let activities = extract_claude_line(&root_scope(), line)
            .into_iter()
            .filter_map(|fact| match fact {
                LogFact::Activity { line, .. } => Some(line),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(activities, vec!["Bash"]);
    }

    #[test]
    fn activity_lines_escape_controls_before_truncation() {
        let description = format!("Inspect\n\u{1b}[31m{}", "\n".repeat(40));
        let encoded = serde_json::to_string(&description).expect("description serializes");
        let line = format!(
            r#"{{"type":"assistant","timestamp":"2026-08-24T01:00:03.000Z","message":{{"content":[{{"type":"tool_use","name":"Agent","input":{{"description":{encoded}}}}}]}}}}"#
        );
        let activity = extract_claude_line(&root_scope(), &line)
            .into_iter()
            .find_map(|fact| match fact {
                LogFact::Activity { line, .. } => Some(line),
                _ => None,
            })
            .expect("tool use emits activity");

        assert!(!activity.chars().any(char::is_control));
        assert!(activity.contains(r"\n"));
        assert!(activity.contains(r"\u{1b}[31m"));
        assert!(activity.chars().count() <= 60);
    }

    #[test]
    fn edit_paths_render_repo_relative() {
        let line = r#"{"type":"assistant","timestamp":"2026-08-24T01:00:03.000Z","cwd":"/home/user/git/example/herdr-top","message":{"content":[{"type":"tool_use","name":"Edit","input":{"file_path":"/home/user/git/example/herdr-top/src/provider/facts.rs"}}]}}"#;
        let facts = extract_claude_line(&root_scope(), line);

        assert!(facts.contains(&LogFact::Activity {
            scope: root_scope(),
            at_ms: 1_787_533_203_000,
            line: "Edit src/provider/facts.rs".to_owned(),
        }));
        assert_eq!(
            repo_relative(
                "file:///home/user/git/example/herdr-top/src/provider/facts.rs",
                "file:///home/user/git/example/herdr-top"
            ),
            "src/provider/facts.rs"
        );
        assert_eq!(
            repo_relative(
                "/home/user/elsewhere/data.txt",
                "/home/user/git/example/herdr-top"
            ),
            "data.txt"
        );
    }

    #[test]
    fn repo_relative_handles_equal_and_outside_paths_without_absolute_display() {
        assert_eq!(
            repo_relative(
                "file:///home/user/git/example/herdr-top",
                "file:///home/user/git/example/herdr-top"
            ),
            "."
        );
        assert_eq!(
            repo_relative(
                "file:///home/user/elsewhere/data.txt",
                "/home/user/git/example/herdr-top"
            ),
            "data.txt"
        );
        assert_eq!(
            repo_relative("file:///", "/home/user/git/example/herdr-top"),
            "."
        );
    }

    #[test]
    fn usage_dedupes_by_message_id_and_sums_output_only() {
        let input = fixture("claude-session.jsonl");
        let repeated = r#"{"type":"assistant","timestamp":"2026-08-24T01:00:13.000Z","message":{"id":"msg_02SyntheticStreamChunk","usage":{"output_tokens":726,"cache_creation_input_tokens":1024,"cache_read_input_tokens":934}}}"#;
        let mut usage = input
            .lines()
            .chain([repeated, repeated])
            .flat_map(|line| extract_claude_line(&root_scope(), line))
            .filter_map(|fact| match fact {
                LogFact::Usage {
                    sample_id,
                    output_tokens,
                    ..
                } => Some((sample_id, output_tokens)),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(usage.len(), 6);
        assert_eq!(
            usage.iter().map(|(_, tokens)| *tokens).collect::<Vec<_>>(),
            vec![726, 726, 91, 84, 726, 726]
        );
        let mut seen = HashSet::new();
        usage.retain(|(sample_id, _)| seen.insert(sample_id.clone()));
        assert_eq!(usage.iter().map(|(_, tokens)| tokens).sum::<u64>(), 901);
        assert!(
            usage
                .iter()
                .all(|(_, tokens)| ![240, 128, 512, 640, 812, 934, 1024].contains(tokens))
        );
    }

    #[test]
    fn effort_extracted_from_assistant_records() {
        let usage = fixture("claude-session.jsonl")
            .lines()
            .flat_map(|line| extract_claude_line(&root_scope(), line))
            .filter_map(|fact| match fact {
                LogFact::Usage { model, effort, .. } => Some((model, effort)),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(usage.len(), 4);
        assert!(usage.iter().all(|(model, effort)| {
            model.as_deref() == Some("claude-fable-5") && effort.as_deref() == Some("high")
        }));
    }

    #[test]
    fn task_notification_failed_maps_to_failed() {
        let ended = fixture("claude-queue-notifications.jsonl")
            .lines()
            .flat_map(|line| extract_claude_line(&root_scope(), line))
            .filter(|fact| matches!(fact, LogFact::SubagentEnded { .. }))
            .collect::<Vec<_>>();

        assert_eq!(
            ended,
            vec![
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "a7189abbf3c5741ac".to_owned(),
                    failed: false,
                },
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "aa78091e473624d60".to_owned(),
                    failed: true,
                },
            ]
        );
    }

    #[test]
    fn task_notification_running_is_not_ended_evidence() {
        let line = r#"{"type":"queue-operation","content":"<task-notification><task-id>abcdef12</task-id><status>running</status></task-notification>"}"#;

        assert!(
            extract_claude_line(&root_scope(), line)
                .into_iter()
                .all(|fact| !matches!(fact, LogFact::SubagentEnded { .. }))
        );
    }

    #[test]
    fn typed_envelopes_cannot_hold_bodies() {
        let line = fixture("claude-session.jsonl")
            .lines()
            .nth(2)
            .expect("fixture has first assistant record")
            .to_owned();
        assert!(line.len() > 3_000);
        assert!(line.contains("immutable snapshot"));

        let envelope: AssistantRecord =
            serde_json::from_str(&line).expect("allowlisted envelope parses");
        let debug = format!("{envelope:?}");

        assert!(
            debug.len() < 512,
            "debug representation was {} bytes",
            debug.len()
        );
        assert!(!debug.contains("immutable snapshot"));
        assert!(!debug.contains("I will treat the cache report"));
    }

    #[test]
    fn user_record_debug_excludes_message_and_tool_result_bodies() {
        let line = fixture("claude-session.jsonl")
            .lines()
            .nth(6)
            .expect("fixture has user tool-result record")
            .to_owned();
        let prompt =
            "Inspect only the fictional cache diagnostic behavior and report boundary-case gaps.";
        let result_text = "Synthetic reviewer result: equality at the warning threshold";
        let body_markers = [prompt, result_text];
        for marker in body_markers {
            assert!(line.contains(marker));
        }

        let envelope: UserRecord =
            serde_json::from_str(&line).expect("allowlisted envelope parses");
        let debug = format!("{envelope:?}");

        assert!(
            debug.len() < 512,
            "debug representation was {} bytes",
            debug.len()
        );
        for marker in body_markers {
            assert!(!debug.contains(marker));
        }
    }

    #[test]
    fn tool_use_result_debug_excludes_non_allowlisted_body_fields() {
        let line = fixture("claude-session.jsonl")
            .lines()
            .nth(6)
            .expect("fixture has user tool-result record")
            .to_owned();
        let prompt =
            "Inspect only the fictional cache diagnostic behavior and report boundary-case gaps.";
        let body_markers = [
            prompt,
            "Check deterministic cache report boundaries",
            "claude-sonnet-5",
            "/tmp/claude-4242/",
            "completed",
        ];
        for marker in body_markers {
            assert!(line.contains(marker));
        }

        let envelope: UserRecord =
            serde_json::from_str(&line).expect("allowlisted envelope parses");
        let result = envelope
            .tool_use_result
            .expect("fixture has a tool-use result");
        let debug = format!("{result:?}");

        assert!(
            debug.len() < 512,
            "debug representation was {} bytes",
            debug.len()
        );
        for marker in body_markers {
            assert!(!debug.contains(marker));
        }
    }

    #[test]
    fn meta_record_debug_excludes_non_allowlisted_fields() {
        let source = fixture("claude-subagent-meta.json");
        assert!(source.contains("claude-opus-5"));
        assert!(source.contains("cache-report-worker"));

        let envelope: MetaRecord =
            serde_json::from_str(&source).expect("allowlisted envelope parses");
        let debug = format!("{envelope:?}");

        assert!(
            debug.len() < 512,
            "debug representation was {} bytes",
            debug.len()
        );
        assert!(!debug.contains("claude-opus-5"));
        assert!(!debug.contains("cache-report-worker"));
    }

    #[test]
    fn ai_title_record_debug_excludes_unknown_body() {
        let body = "ai-title-private-body ".repeat(64);
        let source = format!(
            r#"{{"type":"ai-title","sessionId":"session-title","aiTitle":"Safe title","largeBody":{}}}"#,
            serde_json::to_string(&body).expect("body serializes")
        );
        assert!(source.contains("ai-title-private-body"));

        let envelope: AiTitleRecord =
            serde_json::from_str(&source).expect("allowlisted envelope parses");
        let debug = format!("{envelope:?}");

        assert!(
            debug.len() < 512,
            "debug representation was {} bytes",
            debug.len()
        );
        assert!(!debug.contains("ai-title-private-body"));
    }

    #[test]
    fn unknown_record_types_skip_silently() {
        assert!(
            extract_claude_line(&root_scope(), r#"{"type":"future-record","body":"secret"}"#)
                .is_empty()
        );
        assert!(extract_claude_line(&root_scope(), "not json at all").is_empty());
        assert_eq!(
            extract_claude_line(
                &root_scope(),
                "garbage 6f9bdfa0-1502-4a37-97aa-c45591141130"
            ),
            vec![LogFact::EvidenceId {
                parent: root_scope(),
                id: EvidenceId::Uuid("6f9bdfa0-1502-4a37-97aa-c45591141130".to_owned()),
            }]
        );
    }

    #[test]
    fn tool_use_result_agent_id_marks_subagent_ended() {
        let ended = fixture("claude-session.jsonl")
            .lines()
            .flat_map(|line| extract_claude_line(&root_scope(), line))
            .filter(|fact| matches!(fact, LogFact::SubagentEnded { .. }))
            .collect::<Vec<_>>();

        assert_eq!(
            ended,
            vec![LogFact::SubagentEnded {
                parent: PARENT.to_owned(),
                agent_id: "a7189abbf3c5741ac".to_owned(),
                failed: false,
            }]
        );
    }

    #[test]
    fn tool_use_result_agent_id_without_status_marks_subagent_ended() {
        let line = r#"{"type":"user","timestamp":"2026-08-24T01:00:12.000Z","toolUseResult":{"agentId":"a7189abbf3c5741ac"}}"#;

        assert!(
            extract_claude_line(&root_scope(), line).contains(&LogFact::SubagentEnded {
                parent: PARENT.to_owned(),
                agent_id: "a7189abbf3c5741ac".to_owned(),
                failed: false,
            })
        );
    }

    #[test]
    fn multiple_task_notifications_are_extracted_in_order() {
        let line = r#"{"type":"queue-operation","content":"<task-notification><task-id>abcdef12</task-id><status>completed</status></task-notification><task-notification><task-id>0123456789ab</task-id><status>failed</status></task-notification>"}"#;
        let ended = extract_claude_line(&root_scope(), line)
            .into_iter()
            .filter(|fact| matches!(fact, LogFact::SubagentEnded { .. }))
            .collect::<Vec<_>>();

        assert_eq!(
            ended,
            vec![
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "abcdef12".to_owned(),
                    failed: false,
                },
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "0123456789ab".to_owned(),
                    failed: true,
                },
            ]
        );
    }

    #[test]
    fn unparseable_timestamp_suppresses_timestamped_facts() {
        let line = r#"{"type":"assistant","timestamp":"not-a-time","message":{"id":"msg_bad_time","usage":{"output_tokens":42},"content":[{"type":"tool_use","name":"Bash","input":{"description":"must not appear"}}]}}"#;

        assert!(extract_claude_line(&root_scope(), line).is_empty());
    }

    #[test]
    fn subagent_scope_uses_own_activity_scope_and_parent_for_ending() {
        let scope = SessionScope::ClaudeSubagent {
            parent: PARENT.to_owned(),
            agent_id: "abcdef12".to_owned(),
        };
        let line = r#"{"type":"user","timestamp":"2026-08-24T01:00:03.000Z","toolUseResult":{"agentId":"fedcba98","status":"failed"}}"#;
        let facts = extract_claude_line(&scope, line);

        assert!(facts.contains(&LogFact::Append {
            scope,
            at_ms: 1_787_533_203_000,
        }));
        assert!(facts.contains(&LogFact::SubagentEnded {
            parent: PARENT.to_owned(),
            agent_id: "fedcba98".to_owned(),
            failed: false,
        }));
    }

    #[test]
    fn assistant_envelope_skips_unauthorized_subagent_type() {
        let line = fixture("claude-session.jsonl")
            .lines()
            .nth(5)
            .expect("fixture has Agent tool-use record")
            .to_owned();
        assert!(line.contains(r#""subagent_type":"reviewer""#));

        let envelope: AssistantRecord =
            serde_json::from_str(&line).expect("allowlisted envelope parses");
        let debug = format!("{envelope:?}");

        assert!(!debug.contains("reviewer"));
    }

    #[test]
    fn config_dir_evidence_is_preserved_from_bash_raw_line() {
        let facts = fixture("claude-session.jsonl")
            .lines()
            .flat_map(|line| extract_claude_line(&root_scope(), line))
            .collect::<Vec<_>>();

        assert!(facts.contains(&LogFact::EvidenceId {
            parent: root_scope(),
            id: EvidenceId::ConfigDir(PathBuf::from("/home/user/.claude-secondary")),
        }));
    }
}
