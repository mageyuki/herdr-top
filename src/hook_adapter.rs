use crate::herdr::controller::ControllerEnvelope;

pub enum HookProvider {
    ClaudeCode,
    Codex,
}

#[derive(serde::Deserialize)]
pub struct HookPayload {
    pub hook_event_name: String,
    pub session_id: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub agent_type: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub task_subject: Option<String>,
}

pub fn map_hook_payload(
    provider: HookProvider,
    payload: &HookPayload,
    emitted_at_ms: i64,
    invocation_nonce: u64,
) -> Vec<ControllerEnvelope> {
    let (provider_selector, wire_provider, supports_task_events) = match provider {
        HookProvider::ClaudeCode => ("claude-code", "claude", true),
        HookProvider::Codex => ("codex", "codex", false),
    };
    let source = format!("hook:{provider_selector}");
    let session_run_id = format!("{source}:{}", payload.session_id);
    let make_envelope = |event_type: &str,
                         task_run_id: String,
                         parent_task_run_id: Option<String>,
                         label: Option<String>,
                         native_session_id: Option<String>,
                         entity: &str,
                         transition: &str| ControllerEnvelope {
        schema_version: 1,
        event_id: format!(
            "{source}:{}:{}:{entity}:{transition}:{emitted_at_ms}:{invocation_nonce:016x}",
            payload.session_id, payload.hook_event_name
        ),
        emitted_at_ms,
        source: source.clone(),
        event_type: event_type.to_owned(),
        task_run_id,
        parent_task_run_id,
        depends_on_id: None,
        label,
        reason: None,
        progress: None,
        provider: Some(wire_provider.to_owned()),
        native_session_id,
        terminal_id: None,
    };

    match payload.hook_event_name.as_str() {
        "SessionStart" => vec![make_envelope(
            "task_started",
            session_run_id,
            None,
            None,
            Some(payload.session_id.clone()),
            "session",
            "started",
        )],
        "SubagentStart" => {
            let Some(agent_id) = payload.agent_id.as_deref() else {
                return Vec::new();
            };
            let agent_run_id = format!("{session_run_id}:agent:{agent_id}");
            vec![
                make_envelope(
                    "dispatch",
                    agent_run_id.clone(),
                    Some(session_run_id),
                    None,
                    None,
                    agent_id,
                    "dispatch",
                ),
                make_envelope(
                    "task_started",
                    agent_run_id,
                    None,
                    payload.agent_type.clone(),
                    None,
                    agent_id,
                    "started",
                ),
            ]
        }
        "SubagentStop" => {
            let Some(agent_id) = payload.agent_id.as_deref() else {
                return Vec::new();
            };
            vec![make_envelope(
                "complete",
                format!("{session_run_id}:agent:{agent_id}"),
                None,
                None,
                None,
                agent_id,
                "complete",
            )]
        }
        "TaskCreated" if supports_task_events => {
            let Some(task_id) = payload.task_id.as_deref() else {
                return Vec::new();
            };
            let task_run_id = format!("{session_run_id}:task:{task_id}");
            vec![
                make_envelope(
                    "dispatch",
                    task_run_id.clone(),
                    Some(session_run_id),
                    None,
                    None,
                    task_id,
                    "dispatch",
                ),
                make_envelope(
                    "progress",
                    task_run_id,
                    None,
                    payload.task_subject.clone(),
                    None,
                    task_id,
                    "created",
                ),
            ]
        }
        "TaskCompleted" if supports_task_events => {
            let Some(task_id) = payload.task_id.as_deref() else {
                return Vec::new();
            };
            vec![make_envelope(
                "complete",
                format!("{session_run_id}:task:{task_id}"),
                None,
                None,
                None,
                task_id,
                "complete",
            )]
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{HookPayload, HookProvider, map_hook_payload};
    use crate::herdr::controller::ControllerEnvelope;
    use serde_json::json;

    const EMITTED_AT_MS: i64 = 1_723_456_789_012;
    const NONCE: u64 = 0x0123_4567_89ab_cdef;

    fn payload(event_name: &str) -> HookPayload {
        serde_json::from_value(json!({
            "hook_event_name": event_name,
            "session_id": "session-123"
        }))
        .expect("test hook payload should deserialize")
    }

    fn envelope(
        event_id: &str,
        event_type: &str,
        task_run_id: &str,
        parent_task_run_id: Option<&str>,
        label: Option<&str>,
        provider: &str,
        native_session_id: Option<&str>,
    ) -> ControllerEnvelope {
        let source = match provider {
            "claude" => "hook:claude-code",
            "codex" => "hook:codex",
            _ => panic!("unexpected test provider: {provider}"),
        };
        ControllerEnvelope {
            schema_version: 1,
            event_id: event_id.to_owned(),
            emitted_at_ms: EMITTED_AT_MS,
            source: source.to_owned(),
            event_type: event_type.to_owned(),
            task_run_id: task_run_id.to_owned(),
            parent_task_run_id: parent_task_run_id.map(str::to_owned),
            depends_on_id: None,
            label: label.map(str::to_owned),
            reason: None,
            progress: None,
            provider: Some(provider.to_owned()),
            native_session_id: native_session_id.map(str::to_owned),
            terminal_id: None,
        }
    }

    #[test]
    fn session_start_maps_to_exact_claude_envelope() {
        let actual = map_hook_payload(
            HookProvider::ClaudeCode,
            &payload("SessionStart"),
            EMITTED_AT_MS,
            NONCE,
        );

        assert_eq!(
            actual,
            vec![envelope(
                "hook:claude-code:session-123:SessionStart:session:started:1723456789012:0123456789abcdef",
                "task_started",
                "hook:claude-code:session-123",
                None,
                None,
                "claude",
                Some("session-123"),
            )]
        );
    }

    #[test]
    fn subagent_start_maps_dispatch_then_started_with_distinct_ids() {
        let payload = serde_json::from_value(json!({
            "hook_event_name": "SubagentStart",
            "session_id": "session-123",
            "agent_id": "agent-7",
            "agent_type": "researcher"
        }))
        .expect("test hook payload should deserialize");

        let actual = map_hook_payload(HookProvider::Codex, &payload, EMITTED_AT_MS, NONCE);

        assert_eq!(
            actual,
            vec![
                envelope(
                    "hook:codex:session-123:SubagentStart:agent-7:dispatch:1723456789012:0123456789abcdef",
                    "dispatch",
                    "hook:codex:session-123:agent:agent-7",
                    Some("hook:codex:session-123"),
                    None,
                    "codex",
                    None,
                ),
                envelope(
                    "hook:codex:session-123:SubagentStart:agent-7:started:1723456789012:0123456789abcdef",
                    "task_started",
                    "hook:codex:session-123:agent:agent-7",
                    None,
                    Some("researcher"),
                    "codex",
                    None,
                ),
            ]
        );
        assert_ne!(actual[0].event_id, actual[1].event_id);
    }

    #[test]
    fn subagent_stop_maps_to_exact_complete_envelope() {
        let payload = serde_json::from_value(json!({
            "hook_event_name": "SubagentStop",
            "session_id": "session-123",
            "agent_id": "agent-7"
        }))
        .expect("test hook payload should deserialize");

        let actual = map_hook_payload(HookProvider::ClaudeCode, &payload, EMITTED_AT_MS, NONCE);

        assert_eq!(
            actual,
            vec![envelope(
                "hook:claude-code:session-123:SubagentStop:agent-7:complete:1723456789012:0123456789abcdef",
                "complete",
                "hook:claude-code:session-123:agent:agent-7",
                None,
                None,
                "claude",
                None,
            )]
        );
    }

    #[test]
    fn task_created_maps_dispatch_then_progress_with_distinct_ids() {
        let payload = serde_json::from_value(json!({
            "hook_event_name": "TaskCreated",
            "session_id": "session-123",
            "task_id": "task-9",
            "task_subject": "Map hook payloads"
        }))
        .expect("test hook payload should deserialize");

        let actual = map_hook_payload(HookProvider::ClaudeCode, &payload, EMITTED_AT_MS, NONCE);

        assert_eq!(
            actual,
            vec![
                envelope(
                    "hook:claude-code:session-123:TaskCreated:task-9:dispatch:1723456789012:0123456789abcdef",
                    "dispatch",
                    "hook:claude-code:session-123:task:task-9",
                    Some("hook:claude-code:session-123"),
                    None,
                    "claude",
                    None,
                ),
                envelope(
                    "hook:claude-code:session-123:TaskCreated:task-9:created:1723456789012:0123456789abcdef",
                    "progress",
                    "hook:claude-code:session-123:task:task-9",
                    None,
                    Some("Map hook payloads"),
                    "claude",
                    None,
                ),
            ]
        );
        assert_ne!(actual[0].event_id, actual[1].event_id);
    }

    #[test]
    fn task_completed_maps_to_exact_complete_envelope() {
        let payload = serde_json::from_value(json!({
            "hook_event_name": "TaskCompleted",
            "session_id": "session-123",
            "task_id": "task-9"
        }))
        .expect("test hook payload should deserialize");

        let actual = map_hook_payload(HookProvider::ClaudeCode, &payload, EMITTED_AT_MS, NONCE);

        assert_eq!(
            actual,
            vec![envelope(
                "hook:claude-code:session-123:TaskCompleted:task-9:complete:1723456789012:0123456789abcdef",
                "complete",
                "hook:claude-code:session-123:task:task-9",
                None,
                None,
                "claude",
                None,
            )]
        );
    }

    #[test]
    fn session_end_maps_to_empty() {
        assert!(
            map_hook_payload(
                HookProvider::ClaudeCode,
                &payload("SessionEnd"),
                EMITTED_AT_MS,
                NONCE,
            )
            .is_empty()
        );
    }

    #[test]
    fn unknown_event_maps_to_empty() {
        assert!(
            map_hook_payload(
                HookProvider::Codex,
                &payload("FutureHookEvent"),
                EMITTED_AT_MS,
                NONCE,
            )
            .is_empty()
        );
    }

    #[test]
    fn missing_agent_id_maps_subagent_events_to_empty() {
        for event_name in ["SubagentStart", "SubagentStop"] {
            assert!(
                map_hook_payload(
                    HookProvider::Codex,
                    &payload(event_name),
                    EMITTED_AT_MS,
                    NONCE,
                )
                .is_empty()
            );
        }
    }

    #[test]
    fn missing_task_id_maps_task_events_to_empty() {
        for event_name in ["TaskCreated", "TaskCompleted"] {
            assert!(
                map_hook_payload(
                    HookProvider::ClaudeCode,
                    &payload(event_name),
                    EMITTED_AT_MS,
                    NONCE,
                )
                .is_empty()
            );
        }
    }

    #[test]
    fn codex_task_events_map_to_empty() {
        for event_name in ["TaskCreated", "TaskCompleted"] {
            let payload = serde_json::from_value(json!({
                "hook_event_name": event_name,
                "session_id": "session-123",
                "task_id": "task-9",
                "task_subject": "Map hook payloads"
            }))
            .expect("test hook payload should deserialize");

            assert!(
                map_hook_payload(HookProvider::Codex, &payload, EMITTED_AT_MS, NONCE,).is_empty()
            );
        }
    }

    #[test]
    fn event_ids_include_timestamp_and_nonce_without_provider_prefix() {
        let payload = serde_json::from_value(json!({
            "hook_event_name": "SubagentStart",
            "session_id": "session-123",
            "agent_id": "agent-7"
        }))
        .expect("test hook payload should deserialize");

        let actual = map_hook_payload(HookProvider::ClaudeCode, &payload, EMITTED_AT_MS, NONCE);

        assert_eq!(actual.len(), 2);
        for envelope in actual {
            assert!(
                envelope
                    .event_id
                    .ends_with(":1723456789012:0123456789abcdef")
            );
            assert!(!envelope.event_id.starts_with("prov:"));
        }
    }

    #[test]
    fn different_nonces_distinguish_same_timestamp_invocations() {
        let payload = payload("SessionStart");

        let first = map_hook_payload(
            HookProvider::Codex,
            &payload,
            EMITTED_AT_MS,
            0x0000_0000_0000_0001,
        );
        let second = map_hook_payload(
            HookProvider::Codex,
            &payload,
            EMITTED_AT_MS,
            0x0000_0000_0000_0002,
        );

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_ne!(first[0].event_id, second[0].event_id);
        assert!(first[0].event_id.ends_with(":0000000000000001"));
        assert!(second[0].event_id.ends_with(":0000000000000002"));
    }

    #[test]
    fn privacy_sentinel_excludes_content_fields_but_forwards_task_subject() {
        let payload: HookPayload = serde_json::from_value(json!({
            "hook_event_name": "TaskCreated",
            "session_id": "session-123",
            "task_id": "task-9",
            "task_subject": "allowed-subject-sentinel",
            "prompt": "private-prompt-sentinel",
            "description": "private-description-sentinel",
            "task_description": "private-task-description-sentinel",
            "teammate_name": "private-teammate-name-sentinel",
            "team_name": "private-team-name-sentinel",
            "last_assistant_message": "private-assistant-message-sentinel"
        }))
        .expect("unknown content fields should be ignored");

        let envelopes = map_hook_payload(HookProvider::ClaudeCode, &payload, EMITTED_AT_MS, NONCE);
        let serialized = serde_json::to_string(&envelopes).expect("envelopes should serialize");

        assert!(serialized.contains("allowed-subject-sentinel"));
        for excluded in [
            "private-prompt-sentinel",
            "private-description-sentinel",
            "private-task-description-sentinel",
            "private-teammate-name-sentinel",
            "private-team-name-sentinel",
            "private-assistant-message-sentinel",
        ] {
            assert!(!serialized.contains(excluded), "leaked value: {excluded}");
        }
    }
}
