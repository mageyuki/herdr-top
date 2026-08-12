//! Immutable operator activity and terminal-visibility read models.

use std::collections::HashMap;
use std::sync::Arc;

use crate::model::{Provider, RunId, TaskState};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ActivityIdentity {
    pub event_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivityDurability {
    Durable,
    CurrentOnly,
    DurabilityUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityItem {
    pub identity: ActivityIdentity,
    pub event_timestamp_ms: i64,
    pub seen_at_ms: i64,
    pub ingest_seq: Option<u64>,
    pub source: String,
    pub normalized_kind: String,
    pub source_event_type: String,
    pub workspace_id: Option<String>,
    pub tab_id: Option<String>,
    pub pane_id: Option<String>,
    pub terminal_id: Option<String>,
    pub provider: Option<Provider>,
    pub native_session_id: Option<String>,
    pub task_run_id: Option<RunId>,
    pub agent_node_id: Option<String>,
    pub task_state: Option<TaskState>,
    pub model_id: Option<String>,
    pub provider_event_kind: Option<String>,
    pub tool_name: Option<String>,
    pub item_count: Option<u64>,
    pub byte_count: Option<u64>,
    pub provider_agent_id: Option<String>,
    pub provider_parent_agent_id: Option<String>,
    pub controller_label: Option<String>,
    pub controller_reason: Option<String>,
    pub durability: ActivityDurability,
}

pub struct RestoredOperatorState {
    pub activity: Vec<ActivityItem>,
    pub terminal_times: HashMap<RunId, i64>,
}

pub struct OperatorSnapshot {
    pub activity: Arc<[ActivityItem]>,
    pub terminal_times: Arc<HashMap<RunId, i64>>,
}
