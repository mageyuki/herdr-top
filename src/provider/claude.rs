//! Allowlist-only adapter for Claude session JSONL records.

use std::ffi::OsStr;
use std::path::{Component, Path};

use serde::Deserialize;

use crate::model::{MinimalProviderMetadata, Provider};

use super::{
    BootstrapIdentity, BootstrapParser, DiscoveredFile, DiscoveryIndex, ProviderEvent,
    SourcePosition, TailRecord, valid_native_id,
};

const JSON_ERROR: &str = "claude_json";
const ACTIVITY_SHAPE_ERROR: &str = "claude_activity_shape";
const NATIVE_ID_ERROR: &str = "claude_native_id";
const TOPOLOGY_ERROR: &str = "claude_topology";
const TIMESTAMP_ERROR: &str = "claude_timestamp";

#[derive(Deserialize)]
struct RecordType {
    #[serde(rename = "type")]
    record_type: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActivityRecord {
    #[serde(rename = "type")]
    record_type: Option<String>,
    uuid: Option<String>,
    timestamp: Option<String>,
    session_id: Option<String>,
    agent_id: Option<String>,
    is_sidechain: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedActivity {
    record_type: String,
    uuid: String,
    observed_at_ms: i64,
    thread_id: String,
    owner_session_id: String,
    parent_thread_id: Option<String>,
    depth: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaudePathTopology {
    Main {
        thread_id: String,
    },
    Subagent {
        parent_session: String,
        agent_id: String,
    },
    SubagentsDir {
        parent_session: String,
    },
    SubagentMeta {
        parent_session: String,
        agent_id: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StructuralError {
    Shape,
    NativeId,
    Topology,
    Timestamp,
}

impl StructuralError {
    const fn code(self) -> &'static str {
        match self {
            Self::Shape => ACTIVITY_SHAPE_ERROR,
            Self::NativeId => NATIVE_ID_ERROR,
            Self::Topology => TOPOLOGY_ERROR,
            Self::Timestamp => TIMESTAMP_ERROR,
        }
    }
}

/// Bounded-prefix structural parser for Claude session identity.
#[derive(Debug, Default)]
pub struct ClaudeBootstrapParser;

impl BootstrapParser for ClaudeBootstrapParser {
    fn parse_structural(
        &mut self,
        provider: Provider,
        relative_path: &Path,
        record: &[u8],
    ) -> Option<BootstrapIdentity> {
        if provider != Provider::Claude {
            return None;
        }
        let record_type = record_type(record).ok()??;
        if !is_activity_type(&record_type) {
            return None;
        }
        let activity = parse_activity(relative_path, record).ok()?;
        Some(BootstrapIdentity {
            thread_id: activity.thread_id,
            owner_session_id: Some(activity.owner_session_id),
            parent_thread_id: activity.parent_thread_id,
            model_id: None,
            depth: Some(activity.depth),
            agent_path: None,
            byte_offset: 0,
        })
    }
}

/// Converts allowlisted Claude session records into provider events.
#[derive(Clone, Copy, Debug, Default)]
pub struct ClaudeAdapter;

impl ClaudeAdapter {
    /// Emits the discovery-time identity without replay-position-dependent IDs.
    #[must_use]
    pub fn bootstrap_event(
        self,
        file: &DiscoveredFile,
        generation: u64,
        observed_at_ms: i64,
    ) -> Option<ProviderEvent> {
        if file.provider != Provider::Claude {
            return None;
        }
        let identity = file.bootstrap.as_ref()?;
        Some(ProviderEvent::SessionResolved {
            provider: Provider::Claude,
            agent_thread_id: identity.thread_id.clone(),
            owner_session_id: identity.owner_session_id.clone(),
            parent_thread_id: identity.parent_thread_id.clone(),
            path: file.root.join(&file.relative_path),
            model_id: None,
            depth: identity.depth,
            event_id: format!("prov:claude:meta:{}", identity.thread_id),
            observed_at_ms,
            position: SourcePosition {
                path_id: file.path_id,
                generation,
                offset: identity.byte_offset,
            },
        })
    }

    /// Parses one complete tail record. Unknown types are ignored; malformed known records
    /// become content-free diagnostics so callers can continue with the next line.
    #[must_use]
    pub fn parse_record(
        self,
        _discovery: &DiscoveryIndex,
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
        if !record_type.as_deref().is_some_and(is_activity_type) {
            return Vec::new();
        }
        match parse_activity(&file.relative_path, &record.bytes) {
            Ok(activity) => vec![activity_event(file, record, activity)],
            Err(error) => vec![malformed(
                file,
                record.generation,
                record.offset,
                error.code(),
            )],
        }
    }
}

fn record_type(record: &[u8]) -> Result<Option<String>, ()> {
    serde_json::from_slice::<RecordType>(record)
        .map(|record| record.record_type)
        .map_err(|_| ())
}

fn is_activity_type(record_type: &str) -> bool {
    matches!(record_type, "user" | "assistant" | "attachment" | "system")
}

fn parse_activity(relative_path: &Path, record: &[u8]) -> Result<ParsedActivity, StructuralError> {
    let record =
        serde_json::from_slice::<ActivityRecord>(record).map_err(|_| StructuralError::Shape)?;
    let record_type = record.record_type.ok_or(StructuralError::Shape)?;
    if !is_activity_type(&record_type) {
        return Err(StructuralError::Shape);
    }
    let uuid = record.uuid.ok_or(StructuralError::Shape)?;
    let timestamp = record.timestamp.ok_or(StructuralError::Shape)?;
    let session_id = record.session_id.ok_or(StructuralError::Shape)?;
    let is_sidechain = record.is_sidechain.ok_or(StructuralError::Shape)?;
    if !valid_native_id(&uuid) || !valid_native_id(&session_id) {
        return Err(StructuralError::NativeId);
    }

    let topology = path_topology(relative_path).ok_or(StructuralError::Topology)?;
    let (thread_id, parent_thread_id, depth) = match topology {
        ClaudePathTopology::Main { thread_id } => {
            if is_sidechain || thread_id != session_id {
                return Err(StructuralError::Topology);
            }
            (thread_id, None, 0)
        }
        ClaudePathTopology::Subagent { .. } => {
            if !is_sidechain {
                return Err(StructuralError::Topology);
            }
            let agent_id = record.agent_id.ok_or(StructuralError::Shape)?;
            if !valid_native_id(&agent_id) {
                return Err(StructuralError::NativeId);
            }
            (agent_id, Some(session_id.clone()), 1)
        }
        ClaudePathTopology::SubagentsDir { .. } | ClaudePathTopology::SubagentMeta { .. } => {
            return Err(StructuralError::Topology);
        }
    };

    Ok(ParsedActivity {
        record_type,
        uuid,
        observed_at_ms: parse_timestamp_ms(&timestamp)?,
        thread_id,
        owner_session_id: session_id,
        parent_thread_id,
        depth,
    })
}

pub fn path_topology(relative_path: &Path) -> Option<ClaudePathTopology> {
    let components = relative_path
        .components()
        .map(|component| match component {
            Component::Normal(value) => Some(value),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;

    match components.as_slice() {
        [_project, file_name] => {
            let path = Path::new(file_name);
            if path.extension() != Some(OsStr::new("jsonl")) {
                return None;
            }
            let thread_id = path.file_stem()?.to_str()?.to_owned();
            valid_native_id(&thread_id).then_some(ClaudePathTopology::Main { thread_id })
        }
        [_project, parent_session, subagents]
            if *subagents == OsStr::new("subagents")
                && valid_native_id(parent_session.to_str()?) =>
        {
            Some(ClaudePathTopology::SubagentsDir {
                parent_session: parent_session.to_string_lossy().into_owned(),
            })
        }
        [_project, parent_session, subagents, file_name]
            if *subagents == OsStr::new("subagents")
                && valid_native_id(parent_session.to_str()?) =>
        {
            let file_name = file_name.to_str()?;
            if let Some(stem) = file_name.strip_suffix(".jsonl") {
                let agent_id = stem
                    .strip_prefix("agent-")
                    .filter(|agent_id| !agent_id.is_empty())?;
                return Some(ClaudePathTopology::Subagent {
                    parent_session: parent_session.to_string_lossy().into_owned(),
                    agent_id: agent_id.to_owned(),
                });
            }
            file_name.strip_suffix(".meta.json").and_then(|stem| {
                let agent_id = stem
                    .strip_prefix("agent-")
                    .filter(|agent_id| !agent_id.is_empty())?;
                Some(ClaudePathTopology::SubagentMeta {
                    parent_session: parent_session.to_string_lossy().into_owned(),
                    agent_id: agent_id.to_owned(),
                })
            })
        }
        _ => None,
    }
}

fn activity_event(
    file: &DiscoveredFile,
    record: &TailRecord,
    activity: ParsedActivity,
) -> ProviderEvent {
    ProviderEvent::Activity {
        provider: Provider::Claude,
        agent_thread_id: activity.thread_id.clone(),
        activity: MinimalProviderMetadata {
            agent_id: Some(activity.thread_id.clone()),
            parent_agent_id: activity.parent_thread_id,
            event_kind: Some(activity.record_type),
            ..MinimalProviderMetadata::default()
        },
        depth: Some(activity.depth),
        event_id: format!("prov:claude:rec:{}", activity.uuid),
        observed_at_ms: activity.observed_at_ms,
        position: SourcePosition {
            path_id: file.path_id,
            generation: record.generation,
            offset: record.offset,
        },
    }
}

fn parse_timestamp_ms(timestamp: &str) -> Result<i64, StructuralError> {
    let (date_time, offset_seconds) = if let Some(value) = timestamp.strip_suffix('Z') {
        (value, 0_i64)
    } else {
        let separator = timestamp
            .char_indices()
            .rev()
            .find(|(index, value)| *index > 10 && matches!(value, '+' | '-'))
            .map(|(index, _)| index)
            .ok_or(StructuralError::Timestamp)?;
        let (value, offset) = timestamp.split_at(separator);
        let sign = if offset.starts_with('+') {
            1_i64
        } else {
            -1_i64
        };
        let (hours, minutes) = offset[1..]
            .split_once(':')
            .ok_or(StructuralError::Timestamp)?;
        let hours = parse_decimal(hours)?;
        let minutes = parse_decimal(minutes)?;
        if hours > 23 || minutes > 59 {
            return Err(StructuralError::Timestamp);
        }
        (value, sign * i64::from(hours * 3_600 + minutes * 60))
    };

    let (date, time) = date_time
        .split_once('T')
        .ok_or(StructuralError::Timestamp)?;
    let mut date_parts = date.split('-');
    let year = parse_decimal(date_parts.next().ok_or(StructuralError::Timestamp)?)?;
    let month = parse_decimal(date_parts.next().ok_or(StructuralError::Timestamp)?)?;
    let day = parse_decimal(date_parts.next().ok_or(StructuralError::Timestamp)?)?;
    if date_parts.next().is_some()
        || year > 9_999
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
    {
        return Err(StructuralError::Timestamp);
    }

    let mut time_parts = time.split(':');
    let hour = parse_decimal(time_parts.next().ok_or(StructuralError::Timestamp)?)?;
    let minute = parse_decimal(time_parts.next().ok_or(StructuralError::Timestamp)?)?;
    let seconds = time_parts.next().ok_or(StructuralError::Timestamp)?;
    if time_parts.next().is_some() || hour > 23 || minute > 59 {
        return Err(StructuralError::Timestamp);
    }
    let (second, millis) = match seconds.split_once('.') {
        Some((second, fraction)) => (parse_decimal(second)?, fraction_millis(fraction)?),
        None => (parse_decimal(seconds)?, 0),
    };
    if second > 59 {
        return Err(StructuralError::Timestamp);
    }

    let days = days_from_civil(i64::from(year), month, day);
    let seconds = days
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(i64::from(hour * 3_600 + minute * 60 + second)))
        .and_then(|value| value.checked_sub(offset_seconds))
        .ok_or(StructuralError::Timestamp)?;
    seconds
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(i64::from(millis)))
        .ok_or(StructuralError::Timestamp)
}

fn parse_decimal(value: &str) -> Result<u32, StructuralError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(StructuralError::Timestamp);
    }
    value.parse().map_err(|_| StructuralError::Timestamp)
}

fn fraction_millis(fraction: &str) -> Result<u32, StructuralError> {
    if fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(StructuralError::Timestamp);
    }
    let mut millis = 0_u32;
    for (index, byte) in fraction.bytes().take(3).enumerate() {
        millis += u32::from(byte - b'0') * 10_u32.pow(2 - index as u32);
    }
    Ok(millis)
}

const fn days_in_month(year: u32, month: u32) -> u32 {
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

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn malformed(
    file: &DiscoveredFile,
    generation: u64,
    byte_offset: u64,
    error_code: &'static str,
) -> ProviderEvent {
    ProviderEvent::Malformed {
        provider: Provider::Claude,
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
