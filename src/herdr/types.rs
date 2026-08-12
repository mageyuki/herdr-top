//! Tolerant data types carried by the herdr wire protocol.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Successful response returned by the schema-declared `ping` request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Pong {
    #[serde(rename = "type")]
    pub result_type: String,
    pub version: String,
    pub protocol: u32,
    #[serde(default)]
    pub capabilities: Option<PongCapabilities>,
}

/// Optional server capabilities embedded in a [`Pong`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PongCapabilities {
    pub live_handoff: bool,
    #[serde(default)]
    pub detached_server_daemon: bool,
}

/// Successful response returned by `server.agent_manifests`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentManifestStatus {
    #[serde(rename = "type")]
    pub result_type: String,
    pub manifests: Vec<AgentManifestInfo>,
    #[serde(default)]
    pub last_check_unix: Option<u64>,
    #[serde(default)]
    pub last_result: Option<String>,
}

/// One provider's installed Herdr integration manifest status.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentManifestInfo {
    pub agent: String,
    pub source: String,
    pub source_kind: String,
    pub local_override_shadowing_remote: bool,
    #[serde(default)]
    pub active_version: Option<String>,
    #[serde(default)]
    pub cached_remote_version: Option<String>,
    #[serde(default)]
    pub remote_last_checked_unix: Option<u64>,
    #[serde(default)]
    pub remote_update_error: Option<String>,
    #[serde(default)]
    pub remote_update_result: Option<String>,
    #[serde(default)]
    pub warning: Option<String>,
}

/// One event subscription sent to `events.subscribe`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Subscription {
    /// Dot-form event name, such as `pane.updated`.
    #[serde(rename = "type")]
    pub event_type: String,
    /// Optional pane scope used by pane-specific subscriptions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pane_id: Option<String>,
}

impl Subscription {
    /// Creates an unscoped subscription.
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            pane_id: None,
        }
    }

    /// Creates a subscription scoped to one pane.
    pub fn for_pane(event_type: impl Into<String>, pane_id: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            pane_id: Some(pane_id.into()),
        }
    }
}

/// A complete topology snapshot returned by `session.snapshot`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Snapshot {
    pub version: String,
    pub protocol: u64,
    pub focused_workspace_id: Option<String>,
    pub focused_tab_id: Option<String>,
    pub focused_pane_id: Option<String>,
    pub workspaces: Vec<WorkspaceInfo>,
    pub tabs: Vec<TabInfo>,
    pub panes: Vec<PaneInfo>,
    #[serde(default)]
    pub layouts: Vec<Value>,
    #[serde(default)]
    pub agents: Vec<Value>,
}

/// Workspace information embedded in snapshots and event data.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkspaceInfo {
    pub workspace_id: String,
    pub number: Option<u64>,
    pub label: Option<String>,
    pub focused: Option<bool>,
    pub pane_count: Option<u64>,
    pub tab_count: Option<u64>,
    pub active_tab_id: Option<String>,
    pub agent_status: Option<String>,
}

/// Tab information embedded in snapshots and event data.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TabInfo {
    pub tab_id: String,
    pub workspace_id: String,
    pub number: Option<u64>,
    pub label: Option<String>,
    pub focused: Option<bool>,
    pub pane_count: Option<u64>,
    pub agent_status: Option<String>,
}

/// Pane information embedded in snapshots and event data.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PaneInfo {
    pub pane_id: String,
    pub terminal_id: String,
    pub workspace_id: String,
    pub tab_id: Option<String>,
    pub focused: Option<bool>,
    pub cwd: Option<String>,
    pub foreground_cwd: Option<String>,
    pub terminal_title: Option<String>,
    pub terminal_title_stripped: Option<String>,
    pub label: Option<String>,
    pub agent: Option<String>,
    pub agent_status: Option<String>,
    pub scroll: Option<ScrollInfo>,
    pub revision: Option<u64>,
    pub agent_session: Option<AgentSessionInfo>,
}

/// Scroll state reported for a pane while its terminal is running.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ScrollInfo {
    pub offset_from_bottom: Option<u64>,
    pub max_offset_from_bottom: Option<u64>,
    pub viewport_rows: Option<u64>,
}

/// Persistent agent-session identity associated with a pane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentSessionInfo {
    pub source: String,
    pub agent: String,
    pub kind: AgentSessionKind,
    pub value: String,
}

/// How an agent-session value identifies its backing session.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentSessionKind {
    Id,
    Path,
}

/// Body of a schema-declared server error envelope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}
