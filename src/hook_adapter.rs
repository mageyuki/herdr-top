use crate::herdr::controller::ControllerEnvelope;
use crate::model::sanitize_controller_text;

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

/// Longest accepted hook-provided identifier, in bytes. Observed provider
/// identifiers (UUIDs, prefixed hex ids) stay far below this; the cap bounds
/// run-id, event-id, and log growth from a misbehaving hook caller.
pub const HOOK_IDENTIFIER_MAX_BYTES: usize = 128;

/// Rejects hook payloads whose identifiers exceed the byte cap. The error
/// names the field and length but never echoes identifier content.
#[allow(clippy::collapsible_if)]
pub fn validate_hook_identifiers(payload: &HookPayload) -> Result<(), String> {
    let fields = [
        ("session_id", Some(payload.session_id.as_str())),
        ("agent_id", payload.agent_id.as_deref()),
        ("task_id", payload.task_id.as_deref()),
    ];
    for (name, value) in fields {
        if let Some(value) = value {
            if value.len() > HOOK_IDENTIFIER_MAX_BYTES {
                return Err(format!(
                    "hook {name} is {} bytes, exceeding the {HOOK_IDENTIFIER_MAX_BYTES}-byte cap",
                    value.len()
                ));
            }
        }
    }
    Ok(())
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
        label: label.as_deref().map(sanitize_controller_text),
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
    use super::{
        HOOK_IDENTIFIER_MAX_BYTES, HookPayload, HookProvider, map_hook_payload,
        validate_hook_identifiers,
    };
    use crate::herdr::controller::ControllerEnvelope;
    use crate::model::sanitize_controller_text;
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

    fn payload_with(session: &str, agent: Option<&str>, task: Option<&str>) -> HookPayload {
        HookPayload {
            hook_event_name: "SessionStart".to_owned(),
            session_id: session.to_owned(),
            source: None,
            agent_id: agent.map(str::to_owned),
            agent_type: None,
            task_id: task.map(str::to_owned),
            task_subject: None,
        }
    }

    #[test]
    fn i7_identifiers_at_the_cap_are_accepted() {
        let max = "a".repeat(HOOK_IDENTIFIER_MAX_BYTES);
        let payload = payload_with(&max, Some(&max), Some(&max));
        assert_eq!(validate_hook_identifiers(&payload), Ok(()));
    }

    #[test]
    fn i7_oversized_identifiers_are_rejected_per_field() {
        let over = "a".repeat(HOOK_IDENTIFIER_MAX_BYTES + 1);
        let session = payload_with(&over, None, None);
        let error = validate_hook_identifiers(&session).unwrap_err();
        assert!(error.contains("session_id"), "{error}");
        assert!(error.contains("129"), "{error}");
        assert!(!error.contains(&over), "must not echo the identifier");

        let agent = payload_with("s", Some(&over), None);
        assert!(
            validate_hook_identifiers(&agent)
                .unwrap_err()
                .contains("agent_id")
        );

        let task = payload_with("s", None, Some(&over));
        assert!(
            validate_hook_identifiers(&task)
                .unwrap_err()
                .contains("task_id")
        );
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
    fn subagent_start_bounds_oversized_agent_type_below_frame_limit() {
        let huge_agent_type = "a".repeat(100_000);
        let mut payload = payload("SubagentStart");
        payload.agent_id = Some("agent-7".to_owned());
        payload.agent_type = Some(huge_agent_type.clone());

        let actual = map_hook_payload(HookProvider::ClaudeCode, &payload, EMITTED_AT_MS, NONCE);
        let started = actual
            .iter()
            .find(|envelope| envelope.event_type == "task_started")
            .expect("SubagentStart should produce a task_started envelope");
        let label = started
            .label
            .as_deref()
            .expect("task_started should carry the agent type label");

        assert!(
            label.len() <= 256,
            "sanitized agent type was {} bytes",
            label.len()
        );
        assert_eq!(label, sanitize_controller_text(&huge_agent_type));
        assert!(
            serde_json::to_vec(started).unwrap().len() < crate::herdr::controller::MAX_FRAME_BYTES
        );
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
    fn task_created_bounds_oversized_subject_below_frame_limit() {
        let huge_subject = "s".repeat(100_000);
        let mut payload = payload("TaskCreated");
        payload.task_id = Some("task-9".to_owned());
        payload.task_subject = Some(huge_subject.clone());

        let actual = map_hook_payload(HookProvider::ClaudeCode, &payload, EMITTED_AT_MS, NONCE);
        let progress = actual
            .iter()
            .find(|envelope| envelope.event_type == "progress")
            .expect("TaskCreated should produce a progress envelope");
        let label = progress
            .label
            .as_deref()
            .expect("progress should carry the task subject label");

        assert!(
            label.len() <= 256,
            "sanitized task subject was {} bytes",
            label.len()
        );
        assert_eq!(label, sanitize_controller_text(&huge_subject));
        assert!(
            serde_json::to_vec(progress).unwrap().len() < crate::herdr::controller::MAX_FRAME_BYTES
        );
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
