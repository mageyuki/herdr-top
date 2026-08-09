//! Allowlist-only adapter for Codex rollout JSONL records.

use std::path::Path;

use serde::Deserialize;

use crate::model::{ExecState, MinimalProviderMetadata, Provider};

use super::{
    BootstrapIdentity, BootstrapParser, DiscoveredFile, DiscoveryIndex, ProviderEvent,
    SourcePosition, TailRecord, valid_native_id,
};

const JSON_ERROR: &str = "codex_json";
const SESSION_SHAPE_ERROR: &str = "codex_session_meta";
const ACTIVITY_SHAPE_ERROR: &str = "codex_activity_shape";
const NATIVE_ID_ERROR: &str = "codex_native_id";
const AGENT_PATH_ERROR: &str = "codex_agent_path";

#[derive(Deserialize)]
struct RecordType {
    #[serde(rename = "type")]
    record_type: Option<String>,
}

#[derive(Deserialize)]
struct PayloadTypeEnvelope {
    payload: Option<PayloadType>,
}

#[derive(Deserialize)]
struct PayloadType {
    #[serde(rename = "type")]
    payload_type: Option<String>,
}

#[derive(Deserialize)]
struct SessionEnvelope {
    payload: Option<SessionPayload>,
}

#[derive(Deserialize)]
struct SessionPayload {
    id: Option<String>,
    session_id: Option<String>,
    parent_thread_id: Option<String>,
    agent_path: Option<String>,
    model: Option<String>,
    model_id: Option<String>,
}

#[derive(Deserialize)]
struct ActivityEnvelope {
    payload: Option<ActivityPayload>,
}

#[derive(Deserialize)]
struct ActivityPayload {
    event_id: Option<String>,
    occurred_at_ms: Option<i64>,
    agent_thread_id: Option<String>,
    agent_path: Option<String>,
    kind: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StructuralError {
    Shape,
    NativeId,
    AgentPath,
}

impl StructuralError {
    const fn session_code(self) -> &'static str {
        match self {
            Self::Shape => SESSION_SHAPE_ERROR,
            Self::NativeId => NATIVE_ID_ERROR,
            Self::AgentPath => AGENT_PATH_ERROR,
        }
    }

    const fn activity_code(self) -> &'static str {
        match self {
            Self::Shape => ACTIVITY_SHAPE_ERROR,
            Self::NativeId => NATIVE_ID_ERROR,
            Self::AgentPath => AGENT_PATH_ERROR,
        }
    }
}

/// Bounded-prefix structural parser for Codex rollout metadata.
#[derive(Debug, Default)]
pub struct CodexBootstrapParser;

impl BootstrapParser for CodexBootstrapParser {
    fn parse_structural(
        &mut self,
        provider: Provider,
        _relative_path: &Path,
        record: &[u8],
    ) -> Option<BootstrapIdentity> {
        if provider != Provider::Codex
            || record_type(record).ok()?.as_deref() != Some("session_meta")
        {
            return None;
        }
        parse_session(record).ok()
    }
}

/// Converts allowlisted Codex rollout records into provider events.
#[derive(Clone, Copy, Debug, Default)]
pub struct CodexAdapter;

impl CodexAdapter {
    /// Emits the discovery-time identity without replay-position-dependent IDs.
    #[must_use]
    pub fn bootstrap_event(
        self,
        file: &DiscoveredFile,
        generation: u64,
        observed_at_ms: i64,
    ) -> Option<ProviderEvent> {
        if file.provider != Provider::Codex {
            return None;
        }
        let identity = file.bootstrap.as_ref()?;
        Some(ProviderEvent::SessionResolved {
            provider: Provider::Codex,
            agent_thread_id: identity.thread_id.clone(),
            owner_session_id: identity.owner_session_id.clone(),
            parent_thread_id: identity.parent_thread_id.clone(),
            path: file.root.join(&file.relative_path),
            model_id: identity.model_id.clone(),
            depth: identity.depth,
            event_id: format!("prov:codex:meta:{}", identity.thread_id),
            observed_at_ms,
            position: SourcePosition {
                path_id: file.path_id,
                generation,
                offset: identity.byte_offset,
            },
        })
    }

    /// Parses one complete tail record. Unknown shapes are ignored; malformed known records
    /// become content-free diagnostics so callers can continue with the next line.
    #[must_use]
    pub fn parse_record(
        self,
        discovery: &DiscoveryIndex,
        file: &DiscoveredFile,
        record: &TailRecord,
    ) -> Vec<ProviderEvent> {
        if let Some(error_code) = record.error_code {
            return vec![malformed(
                file,
                record.generation,
                record.offset,
                error_code,
            )];
        }
        let record_type = match record_type(&record.bytes) {
            Ok(record_type) => record_type,
            Err(()) => {
                return vec![malformed(
                    file,
                    record.generation,
                    record.offset,
                    JSON_ERROR,
                )];
            }
        };
        match record_type.as_deref() {
            Some("session_meta") => match parse_session(&record.bytes) {
                Ok(_) => Vec::new(),
                Err(error) => vec![malformed(
                    file,
                    record.generation,
                    record.offset,
                    error.session_code(),
                )],
            },
            Some("event_msg") => {
                let payload_type = serde_json::from_slice::<PayloadTypeEnvelope>(&record.bytes)
                    .ok()
                    .and_then(|envelope| envelope.payload)
                    .and_then(|payload| payload.payload_type);
                if payload_type.as_deref() != Some("sub_agent_activity") {
                    return Vec::new();
                }
                match parse_activity(&record.bytes) {
                    Ok(activity) => activity_events(discovery, file, record, activity),
                    Err(error) => vec![malformed(
                        file,
                        record.generation,
                        record.offset,
                        error.activity_code(),
                    )],
                }
            }
            Some(_) | None => Vec::new(),
        }
    }
}

fn record_type(record: &[u8]) -> Result<Option<String>, ()> {
    serde_json::from_slice::<RecordType>(record)
        .map(|record| record.record_type)
        .map_err(|_| ())
}

fn parse_session(record: &[u8]) -> Result<BootstrapIdentity, StructuralError> {
    let payload = serde_json::from_slice::<SessionEnvelope>(record)
        .map_err(|_| StructuralError::Shape)?
        .payload
        .ok_or(StructuralError::Shape)?;
    let thread_id = payload.id.ok_or(StructuralError::Shape)?;
    let owner_session_id = payload.session_id.ok_or(StructuralError::Shape)?;
    if !valid_native_id(&thread_id)
        || !valid_native_id(&owner_session_id)
        || payload
            .parent_thread_id
            .as_deref()
            .is_some_and(|parent| !valid_native_id(parent))
    {
        return Err(StructuralError::NativeId);
    }
    let depth = match payload.agent_path.as_deref() {
        Some(agent_path) => Some(agent_path_depth(agent_path)?),
        None => Some(0),
    };
    Ok(BootstrapIdentity {
        thread_id,
        owner_session_id: Some(owner_session_id),
        parent_thread_id: payload.parent_thread_id,
        model_id: payload.model_id.or(payload.model),
        depth,
        agent_path: payload.agent_path,
        byte_offset: 0,
    })
}

fn parse_activity(record: &[u8]) -> Result<ActivityPayload, StructuralError> {
    let payload = serde_json::from_slice::<ActivityEnvelope>(record)
        .map_err(|_| StructuralError::Shape)?
        .payload
        .ok_or(StructuralError::Shape)?;
    if payload.event_id.is_none()
        || payload.occurred_at_ms.is_none()
        || payload.agent_thread_id.is_none()
        || payload.agent_path.is_none()
        || payload.kind.is_none()
    {
        return Err(StructuralError::Shape);
    }
    if !valid_native_id(payload.event_id.as_deref().expect("checked"))
        || !valid_native_id(payload.agent_thread_id.as_deref().expect("checked"))
    {
        return Err(StructuralError::NativeId);
    }
    agent_path_depth(payload.agent_path.as_deref().expect("checked"))?;
    Ok(payload)
}

fn agent_path_depth(agent_path: &str) -> Result<u32, StructuralError> {
    let segments = agent_path
        .strip_prefix('/')
        .ok_or(StructuralError::AgentPath)?
        .split('/')
        .collect::<Vec<_>>();
    if segments.iter().any(|segment| !valid_native_id(segment)) {
        return Err(StructuralError::AgentPath);
    }
    u32::try_from(segments.len().saturating_sub(1)).map_err(|_| StructuralError::AgentPath)
}

fn parent_agent_path(agent_path: &str) -> Option<&str> {
    let (parent, _) = agent_path.rsplit_once('/')?;
    (!parent.is_empty()).then_some(parent)
}

fn activity_events(
    discovery: &DiscoveryIndex,
    file: &DiscoveredFile,
    record: &TailRecord,
    activity: ActivityPayload,
) -> Vec<ProviderEvent> {
    let event_id = activity.event_id.expect("validated activity event id");
    let observed_at_ms = activity
        .occurred_at_ms
        .expect("validated activity timestamp");
    let agent_thread_id = activity
        .agent_thread_id
        .expect("validated activity thread id");
    let agent_path = activity.agent_path.expect("validated activity path");
    let depth = Some(agent_path_depth(&agent_path).expect("validated activity path"));
    let kind = activity.kind.expect("validated activity kind");
    let owner_session_id = file
        .bootstrap
        .as_ref()
        .and_then(|identity| identity.owner_session_id.as_deref());
    let parent_thread_id = parent_agent_path(&agent_path)
        .and_then(|parent| discovery.resolve_agent_path(Provider::Codex, owner_session_id, parent))
        .map(str::to_owned);
    let position = SourcePosition {
        path_id: file.path_id,
        generation: record.generation,
        offset: record.offset,
    };
    let mut events = vec![ProviderEvent::Activity {
        provider: Provider::Codex,
        agent_thread_id: agent_thread_id.clone(),
        activity: MinimalProviderMetadata {
            agent_id: Some(agent_thread_id.clone()),
            parent_agent_id: parent_thread_id.clone(),
            event_kind: Some(kind.clone()),
            ..MinimalProviderMetadata::default()
        },
        depth,
        event_id: format!("prov:codex:act:{event_id}"),
        observed_at_ms,
        position,
    }];
    if kind == "started" {
        events.push(ProviderEvent::AgentUpsert {
            provider: Provider::Codex,
            agent_thread_id,
            owner_session_id: owner_session_id.map(str::to_owned),
            parent_thread_id,
            state: Some(ExecState::Working),
            model_id: None,
            depth,
            event_id: format!("prov:codex:up:{event_id}"),
            observed_at_ms,
            position,
        });
    }
    events
}

fn malformed(
    file: &DiscoveredFile,
    generation: u64,
    byte_offset: u64,
    error_code: &'static str,
) -> ProviderEvent {
    ProviderEvent::Malformed {
        provider: Provider::Codex,
        path_display: file
            .relative_path
            .to_string_lossy()
            .chars()
            .flat_map(char::escape_default)
            .collect(),
        generation,
        byte_offset,
        error_code,
    }
}
