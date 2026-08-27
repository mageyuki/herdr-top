//! Evidence-gated provider log admission.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, DirEntry, FileType};
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

use crate::activity::{DEFAULT_GHOST_VISIBILITY_MS, DEFAULT_STALL_WARN_MS};
use crate::hook_adapter::{HookPayload, HookProvider, map_hook_payload};
use crate::model::{
    ControllerEvent, ControllerEventKind, EventMetadata, MinimalProviderMetadata, Provider, RunKey,
    sanitize_controller_text,
};

use super::facts::{ActivitySource, CodexInternal, EvidenceId, LogFact, SessionScope};
use super::{DiscoveryRoot, ProviderDiagnostics, ProviderEvent, SourcePosition};

#[cfg(test)]
use std::cell::RefCell;

/// Default bounded provider-log backfill window: one day.
pub const DEFAULT_BACKFILL_WINDOW_MS: i64 = 86_400_000;
/// Default delay before a provider-log Complete becomes durable.
pub const DEFAULT_COMPLETE_GRACE_MS: i64 = 30_000;
/// Default inactivity interval before a provider-log run closes without a terminal fact.
pub const DEFAULT_HEADLESS_INACTIVITY_MS: i64 = 600_000;
/// Event-source marker reserved for facts synthesized from provider log artifacts.
pub const SOURCE_LOG_LANE: &str = "provider-log";
/// Provider metadata marker for a lane-selected row live line.
pub const LIVE_LINE_EVENT_KIND: &str = "lane_live_line";

/// Effective provider-log lane timing configuration resolved at process startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogLaneConfig {
    /// Hard historical scan bound.
    pub backfill_window_ms: i64,
    /// Delay that allows a later failure or resume to supersede a Complete.
    pub complete_grace_ms: i64,
    /// Inactivity interval before an active lane run becomes `ended_unknown`.
    pub headless_inactivity_ms: i64,
}

impl Default for LogLaneConfig {
    fn default() -> Self {
        Self {
            backfill_window_ms: DEFAULT_BACKFILL_WINDOW_MS,
            complete_grace_ms: DEFAULT_COMPLETE_GRACE_MS,
            headless_inactivity_ms: DEFAULT_HEADLESS_INACTIVITY_MS,
        }
    }
}

/// First allowlisted Codex session metadata retained for one artifact identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexSessionMetadata {
    /// Raw Codex working directory.
    pub cwd: String,
    /// Codex process that originated the rollout.
    pub originator: String,
    /// Typed internal-agent source, when recognized.
    pub internal: Option<CodexInternal>,
    /// Codex CLI version from the first metadata record.
    pub cli_version: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ActivitySelection {
    tool_use: Option<String>,
    commentary: Option<String>,
    command: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FactPosition {
    ordinal: u64,
    sequence: usize,
}

impl ActivitySelection {
    fn observe(&mut self, source: &ActivitySource, line: &str) {
        match source {
            ActivitySource::ToolUse => self.tool_use = Some(line.to_owned()),
            ActivitySource::Commentary => self.commentary = Some(line.to_owned()),
            ActivitySource::Command => self.command = Some(line.to_owned()),
        }
    }

    fn selected(&self) -> Option<String> {
        self.commentary
            .clone()
            .or_else(|| self.command.clone())
            .or_else(|| self.tool_use.clone())
    }
}

#[derive(Clone, Copy, Debug)]
struct SubagentEnd {
    failed: bool,
    ordinal: u64,
    at_ms: i64,
}

/// Stateful fact consumer that emits deterministic provider-lane events.
#[derive(Clone, Debug)]
pub struct Synthesis {
    complete_grace_ms: i64,
    headless_inactivity_ms: i64,
    session_meta: HashMap<PathBuf, CodexSessionMetadata>,
    turn_context: HashMap<ScopeKey, (String, Option<String>, Option<String>)>,
    ai_titles: HashMap<String, String>,
    claude_cwds: HashMap<String, String>,
    published_subjects: HashMap<String, String>,
    started: HashMap<ScopeKey, i64>,
    usage_samples: HashSet<(ScopeKey, String)>,
    subagent_ends: HashMap<(String, String), SubagentEnd>,
    lineage: HashSet<(ScopeKey, ScopeKey)>,
    last_append_ms: HashMap<ScopeKey, i64>,
    /// Grace-held outcomes flush on graceful shutdown. After a crash, `EndedUnknown` is the
    /// designed honest recovery for an outcome nobody witnessed.
    pending_completes: HashMap<ScopeKey, ControllerEvent>,
    latest_lifecycle_ms: i64,
    completed: HashSet<ScopeKey>,
    inactivity_closed: HashSet<ScopeKey>,
    live_lines: HashMap<ScopeKey, ActivitySelection>,
}

impl Default for Synthesis {
    fn default() -> Self {
        Self::with_lifecycle_timing(DEFAULT_COMPLETE_GRACE_MS, DEFAULT_HEADLESS_INACTIVITY_MS)
    }
}

impl Synthesis {
    /// Creates synthesis state with explicit, deterministic lifecycle intervals.
    #[must_use]
    pub fn with_lifecycle_timing(complete_grace_ms: i64, headless_inactivity_ms: i64) -> Self {
        Self::with_lifecycle_timing_at(complete_grace_ms, headless_inactivity_ms, 1)
    }

    /// Creates synthesis state with explicit intervals and an injected lifecycle clock anchor.
    #[must_use]
    pub fn with_lifecycle_timing_at(
        complete_grace_ms: i64,
        headless_inactivity_ms: i64,
        now_ms: i64,
    ) -> Self {
        Self {
            complete_grace_ms,
            headless_inactivity_ms,
            session_meta: HashMap::new(),
            turn_context: HashMap::new(),
            ai_titles: HashMap::new(),
            claude_cwds: HashMap::new(),
            published_subjects: HashMap::new(),
            started: HashMap::new(),
            usage_samples: HashSet::new(),
            subagent_ends: HashMap::new(),
            lineage: HashSet::new(),
            last_append_ms: HashMap::new(),
            pending_completes: HashMap::new(),
            latest_lifecycle_ms: now_ms.max(1),
            completed: HashSet::new(),
            inactivity_closed: HashSet::new(),
            live_lines: HashMap::new(),
        }
    }

    /// Advances grace and inactivity against an injected Unix-epoch millisecond clock.
    pub fn advance_lifecycle(&mut self, now_ms: i64) -> Vec<ProviderEvent> {
        self.latest_lifecycle_ms = self.latest_lifecycle_ms.max(now_ms);
        let mut events = Vec::new();
        self.flush_due_completes(now_ms, &mut events);

        let mut inactive = self
            .started
            .iter()
            .filter_map(|(scope, started_at_ms)| {
                let anchor_ms = self
                    .last_append_ms
                    .get(scope)
                    .copied()
                    .unwrap_or(*started_at_ms);
                (!self.inactivity_closed.contains(scope)
                    && !self.pending_completes.contains_key(scope)
                    && !self.completed.contains(scope)
                    && now_ms.saturating_sub(anchor_ms) >= self.headless_inactivity_ms)
                    .then(|| (scope.clone(), anchor_ms))
            })
            .collect::<Vec<_>>();
        inactive.sort();
        for (scope, anchor_ms) in inactive {
            self.inactivity_closed.insert(scope.clone());
            self.started.remove(&scope);
            events.push(ProviderEvent::LaneClose {
                key: run_key_for_scope_key(&scope),
                at_ms: anchor_ms,
            });
        }
        events
    }

    fn flush_due_completes(&mut self, now_ms: i64, events: &mut Vec<ProviderEvent>) {
        let mut due = self
            .pending_completes
            .iter()
            .filter(|(_, event)| {
                now_ms
                    >= event
                        .metadata
                        .timestamp_ms
                        .saturating_add(self.complete_grace_ms)
            })
            .map(|(scope, event)| (scope.clone(), event.metadata.event_id.clone()))
            .collect::<Vec<_>>();
        due.sort_by(|left, right| left.1.cmp(&right.1));
        for (scope, _) in due {
            if let Some(event) = self.pending_completes.remove(&scope) {
                self.started.remove(&scope);
                self.completed.insert(scope);
                events.push(ProviderEvent::Synthesized(event));
            }
        }
    }

    /// Forfeits completion grace and emits every held outcome for graceful shutdown.
    pub fn flush_pending_completes(&mut self) -> Vec<ProviderEvent> {
        let mut pending = self.pending_completes.drain().collect::<Vec<_>>();
        pending.sort_by(|left, right| left.1.metadata.event_id.cmp(&right.1.metadata.event_id));
        pending
            .into_iter()
            .map(|(scope, event)| {
                self.started.remove(&scope);
                self.completed.insert(scope);
                ProviderEvent::Synthesized(event)
            })
            .collect()
    }

    fn prepare_resume(
        &mut self,
        scope: &ScopeKey,
        at_ms: i64,
        events: &mut Vec<ProviderEvent>,
    ) -> bool {
        self.flush_due_completes(at_ms, events);
        self.pending_completes.remove(scope);
        let was_completed = self.completed.remove(scope);
        let was_inactive = self.inactivity_closed.remove(scope);
        if let ScopeKey::ClaudeSubagent { parent, agent_id } = scope {
            self.subagent_ends
                .remove(&(parent.clone(), agent_id.clone()));
        }
        was_completed || was_inactive
    }

    fn hold_complete(&mut self, scope: ScopeKey, event: ControllerEvent) {
        self.pending_completes.entry(scope).or_insert(event);
    }

    fn start_scope(&mut self, scope: ScopeKey, source_at_ms: i64) -> bool {
        let started_at_ms = if source_at_ms > 0 {
            source_at_ms
        } else {
            self.latest_lifecycle_ms.max(1)
        };
        match self.started.entry(scope) {
            Entry::Vacant(entry) => {
                entry.insert(started_at_ms);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Consumes a file-order batch of facts from one artifact.
    ///
    /// Duplicate subagent terminal facts are collapsed before mapping so `failed: true`
    /// dominates independently of their order within the observed batch.
    pub fn synthesize_batch(
        &mut self,
        artifact: &Path,
        facts: impl IntoIterator<Item = (u64, LogFact)>,
        admission: &mut Admission,
        discovered: &AdmissionIndex,
    ) -> Vec<ProviderEvent> {
        let mut ordinary = Vec::new();
        let mut ended: HashMap<(String, String), (u64, bool, i64)> = HashMap::new();
        let mut ordinal_sequences = HashMap::<u64, usize>::new();
        for (order, (ordinal, fact)) in facts.into_iter().enumerate() {
            let sequence = ordinal_sequences.entry(ordinal).or_default();
            let record_sequence = *sequence;
            *sequence = sequence.saturating_add(1);
            if let LogFact::SubagentEnded {
                parent,
                agent_id,
                failed,
                at_ms,
            } = fact
            {
                let terminal = ended
                    .entry((parent, agent_id))
                    .or_insert((ordinal, false, at_ms));
                terminal.0 = terminal.0.min(ordinal);
                terminal.1 |= failed;
                terminal.2 = terminal.2.min(at_ms);
            } else {
                ordinary.push((ordinal, order, record_sequence, fact));
            }
        }
        ordinary.extend(
            ended
                .into_iter()
                .map(|((parent, agent_id), (ordinal, failed, at_ms))| {
                    (
                        ordinal,
                        usize::MAX,
                        0,
                        LogFact::SubagentEnded {
                            parent,
                            agent_id,
                            failed,
                            at_ms,
                        },
                    )
                }),
        );
        ordinary.sort_by_key(|(ordinal, order, _, fact)| {
            let append_first = !matches!(fact, LogFact::Append { .. });
            (*ordinal, append_first, *order)
        });

        let mut events = Vec::new();
        for (ordinal, _, sequence, fact) in ordinary {
            self.synthesize_fact(
                artifact,
                FactPosition { ordinal, sequence },
                fact,
                admission,
                discovered,
                &mut events,
            );
        }
        events
    }

    /// Returns the selected current-turn activity for a session scope.
    ///
    /// Claude has no turn boundary, so its newest `ToolUse` persists as required by spec row 7.
    /// Task 9 consumes this selected line.
    #[must_use]
    pub fn live_line(&self, scope: &SessionScope) -> Option<String> {
        self.live_lines
            .get(&ScopeKey::from(scope))
            .and_then(ActivitySelection::selected)
    }

    fn synthesize_fact(
        &mut self,
        artifact: &Path,
        position: FactPosition,
        fact: LogFact,
        admission: &mut Admission,
        discovered: &AdmissionIndex,
        events: &mut Vec<ProviderEvent>,
    ) {
        let FactPosition { ordinal, sequence } = position;
        if let Some(at_ms) = fact_lifecycle_time(&fact) {
            self.latest_lifecycle_ms = self.latest_lifecycle_ms.max(at_ms);
        }
        match fact {
            LogFact::Append { scope, at_ms } => {
                let key = ScopeKey::from(&scope);
                let reopen = self.prepare_resume(&key, at_ms, events);
                self.last_append_ms.insert(key.clone(), at_ms);
                events.push(ProviderEvent::RunLiveness {
                    key: run_key_for_scope(&scope),
                    at_ms,
                });
                if (matches!(scope, SessionScope::ClaudeRoot(_)) || reopen)
                    && self.start_scope(key, at_ms)
                {
                    events.push(ProviderEvent::Synthesized(controller_event(
                        artifact,
                        ordinal,
                        &scope,
                        ControllerEventKind::TaskStarted,
                        at_ms,
                        None,
                        None,
                    )));
                }
            }
            LogFact::AiTitle { session_id, title } => {
                self.ai_titles.insert(session_id.clone(), title);
                self.publish_claude_subject(artifact, ordinal, &session_id, events);
            }
            LogFact::ClaudeCwd { session_id, cwd } => {
                if let Some(basename) = Path::new(&cwd)
                    .file_name()
                    .and_then(OsStr::to_str)
                    .filter(|basename| !basename.is_empty())
                {
                    self.claude_cwds
                        .insert(session_id.clone(), basename.to_owned());
                    self.publish_claude_subject(artifact, ordinal, &session_id, events);
                }
            }
            LogFact::CodexMeta {
                rollout_id,
                cwd,
                originator,
                internal,
                cli_version,
            } => {
                if self.session_meta.contains_key(artifact) {
                    return;
                }
                self.session_meta.insert(
                    artifact.to_path_buf(),
                    CodexSessionMetadata {
                        cwd,
                        originator: originator.clone(),
                        internal,
                        cli_version,
                    },
                );
                let scope = SessionScope::Codex {
                    rollout_id: rollout_id.clone(),
                };
                let scope_key = ScopeKey::from(&scope);
                let at_ms = self
                    .last_append_ms
                    .get(&scope_key)
                    .copied()
                    .unwrap_or(self.latest_lifecycle_ms);
                let _ = self.prepare_resume(&scope_key, at_ms, events);
                self.start_scope(scope_key, at_ms);
                events.push(ProviderEvent::Synthesized(controller_event(
                    artifact,
                    ordinal,
                    &scope,
                    ControllerEventKind::TaskStarted,
                    at_ms,
                    None,
                    Some(MinimalProviderMetadata {
                        event_kind: Some(originator),
                        ..MinimalProviderMetadata::default()
                    }),
                )));
            }
            LogFact::CodexTurn {
                rollout_id,
                turn_id: _,
                model,
                effort,
                sandbox,
            } => {
                self.turn_context
                    .insert(ScopeKey::Codex(rollout_id), (model, effort, sandbox));
            }
            LogFact::CodexPid { .. } => {}
            LogFact::CodexTurnStarted { rollout_id, at_ms } => {
                let scope = SessionScope::Codex { rollout_id };
                let scope_key = ScopeKey::from(&scope);
                let _ = self.prepare_resume(&scope_key, at_ms, events);
                if self
                    .live_lines
                    .remove(&scope_key)
                    .and_then(|selection| selection.selected())
                    .is_some()
                {
                    events.push(live_line_event(
                        artifact, ordinal, sequence, &scope, at_ms, None,
                    ));
                }
                if self.start_scope(scope_key, at_ms) {
                    events.push(ProviderEvent::Synthesized(controller_event(
                        artifact,
                        ordinal,
                        &scope,
                        ControllerEventKind::TaskStarted,
                        at_ms,
                        None,
                        None,
                    )));
                }
            }
            LogFact::CodexTurnComplete { rollout_id, at_ms } => {
                let scope = SessionScope::Codex { rollout_id };
                self.flush_due_completes(at_ms, events);
                let scope_key = ScopeKey::from(&scope);
                let event = controller_event(
                    artifact,
                    ordinal,
                    &scope,
                    ControllerEventKind::Complete,
                    at_ms,
                    None,
                    None,
                );
                self.hold_complete(scope_key, event);
            }
            LogFact::CodexTurnAborted { rollout_id, at_ms } => {
                let scope = SessionScope::Codex { rollout_id };
                let scope_key = ScopeKey::from(&scope);
                self.flush_due_completes(at_ms, events);
                self.pending_completes.remove(&scope_key);
                self.completed.remove(&scope_key);
                self.started.remove(&scope_key);
                events.push(ProviderEvent::Synthesized(controller_event(
                    artifact,
                    ordinal,
                    &scope,
                    ControllerEventKind::Cancelled,
                    at_ms,
                    None,
                    None,
                )));
            }
            LogFact::SubagentAppeared {
                parent,
                agent_id,
                agent_type,
                description,
                at_ms,
            } => {
                let parent_scope = SessionScope::ClaudeRoot(parent.clone());
                let child_scope = SessionScope::ClaudeSubagent { parent, agent_id };
                let child_key = ScopeKey::from(&child_scope);
                let _ = self.prepare_resume(&child_key, at_ms, events);
                let provider_metadata = MinimalProviderMetadata {
                    event_kind: Some(agent_type),
                    ..MinimalProviderMetadata::default()
                };
                events.push(ProviderEvent::Synthesized(controller_event(
                    artifact,
                    ordinal,
                    &child_scope,
                    ControllerEventKind::Dispatch {
                        parent_task_run_id: controller_key_for_scope(&parent_scope),
                    },
                    at_ms,
                    Some(description.clone()),
                    Some(provider_metadata.clone()),
                )));
                events.push(ProviderEvent::Synthesized(controller_event(
                    artifact,
                    ordinal,
                    &child_scope,
                    ControllerEventKind::TaskStarted,
                    at_ms,
                    Some(description),
                    Some(provider_metadata),
                )));
                self.start_scope(child_key, at_ms);
            }
            LogFact::SubagentEnded {
                parent,
                agent_id,
                failed,
                at_ms,
            } => {
                let terminal_key = (parent.clone(), agent_id.clone());
                let previous = self.subagent_ends.get(&terminal_key).copied();
                let terminal = *self
                    .subagent_ends
                    .entry(terminal_key)
                    .and_modify(|current| {
                        current.failed |= failed;
                        current.ordinal = current.ordinal.min(ordinal);
                        current.at_ms = current.at_ms.min(at_ms);
                    })
                    .or_insert(SubagentEnd {
                        failed,
                        ordinal,
                        at_ms,
                    });
                let scope = SessionScope::ClaudeSubagent { parent, agent_id };
                let scope_key = ScopeKey::from(&scope);
                // Durable terminals cannot be rewritten; only a grace-held Complete is mutable.
                if previous.is_some_and(|previous| previous.failed)
                    || self.completed.contains(&scope_key)
                {
                    return;
                }
                let SubagentEnd {
                    failed,
                    ordinal,
                    at_ms,
                } = terminal;
                self.flush_due_completes(at_ms, events);
                let event = controller_event(
                    artifact,
                    ordinal,
                    &scope,
                    if failed {
                        ControllerEventKind::Failed
                    } else {
                        ControllerEventKind::Complete
                    },
                    at_ms,
                    None,
                    None,
                );
                if failed {
                    self.pending_completes.remove(&scope_key);
                    self.completed.remove(&scope_key);
                    self.started.remove(&scope_key);
                    events.push(ProviderEvent::Synthesized(event));
                } else {
                    // Refresh the held event from the accumulated ordinal and timestamp.
                    self.pending_completes.insert(scope_key, event);
                }
            }
            LogFact::Activity {
                scope,
                at_ms,
                source,
                line,
            } => {
                let selection = self.live_lines.entry(ScopeKey::from(&scope)).or_default();
                let previous = selection.selected();
                selection.observe(&source, &line);
                let selected = selection.selected();
                if selected != previous {
                    events.push(live_line_event(
                        artifact, ordinal, sequence, &scope, at_ms, selected,
                    ));
                }
            }
            LogFact::Usage {
                scope,
                at_ms,
                sample_id,
                output_tokens,
                token_breakdown,
                model,
                effort,
            } => {
                let scope_key = ScopeKey::from(&scope);
                if self.usage_samples.insert((scope_key.clone(), sample_id)) {
                    let context = self.turn_context.get(&scope_key);
                    events.push(ProviderEvent::Telemetry {
                        key: run_key_for_scope(&scope),
                        at_ms,
                        output_tokens,
                        token_breakdown,
                        model: model.or_else(|| context.map(|(model, _, _)| model.clone())),
                        effort: effort
                            .or_else(|| context.and_then(|(_, effort, _)| effort.clone())),
                        sandbox: context.and_then(|(_, _, sandbox)| sandbox.clone()),
                    });
                }
            }
            LogFact::EvidenceId { parent, id, at_ms } => {
                let Some(child) = admission.on_evidence(&parent, &id, discovered) else {
                    return;
                };
                if child == parent {
                    return;
                }
                let edge = (ScopeKey::from(&parent), ScopeKey::from(&child));
                if !self.lineage.insert(edge) {
                    return;
                }
                events.push(ProviderEvent::Synthesized(controller_event(
                    artifact,
                    ordinal,
                    &child,
                    ControllerEventKind::Dispatch {
                        parent_task_run_id: controller_key_for_scope(&parent),
                    },
                    at_ms,
                    None,
                    None,
                )));
            }
        }
    }

    fn publish_claude_subject(
        &mut self,
        artifact: &Path,
        ordinal: u64,
        session_id: &str,
        events: &mut Vec<ProviderEvent>,
    ) {
        let subject = self
            .ai_titles
            .get(session_id)
            .filter(|title| !title.is_empty())
            .or_else(|| self.claude_cwds.get(session_id))
            .cloned();
        let Some(subject) = subject else {
            return;
        };
        if self.published_subjects.get(session_id) == Some(&subject) {
            return;
        }
        self.published_subjects
            .insert(session_id.to_owned(), subject.clone());
        let scope = SessionScope::ClaudeRoot(session_id.to_owned());
        let scope_key = ScopeKey::from(&scope);
        let at_ms = self
            .last_append_ms
            .get(&scope_key)
            .copied()
            .unwrap_or(self.latest_lifecycle_ms);
        let mut event = controller_event(
            artifact,
            ordinal,
            &scope,
            ControllerEventKind::Progress,
            at_ms,
            Some(subject),
            None,
        );
        event.metadata.event_id = deterministic_subject_event_id(artifact, ordinal, &scope);
        events.push(ProviderEvent::Synthesized(event));
    }
}

/// Returns the identity form consumed by run-scoped liveness and telemetry routes.
#[must_use]
pub fn run_key_for_scope(scope: &SessionScope) -> RunKey {
    match scope {
        SessionScope::ClaudeRoot(_) | SessionScope::ClaudeSubagent { .. } => {
            RunKey::Controller(controller_key_for_scope(scope))
        }
        SessionScope::Codex { rollout_id } => RunKey::Native {
            provider: Provider::Codex,
            sid: rollout_id.clone(),
        },
    }
}

fn run_key_for_scope_key(scope: &ScopeKey) -> RunKey {
    match scope {
        ScopeKey::ClaudeRoot(session_id) => {
            RunKey::Controller(hook_key(HookProvider::ClaudeCode, session_id, None))
        }
        ScopeKey::ClaudeSubagent { parent, agent_id } => {
            RunKey::Controller(hook_key(HookProvider::ClaudeCode, parent, Some(agent_id)))
        }
        ScopeKey::Codex(rollout_id) => RunKey::Native {
            provider: Provider::Codex,
            sid: rollout_id.clone(),
        },
    }
}

fn controller_key_for_scope(scope: &SessionScope) -> String {
    match scope {
        SessionScope::ClaudeRoot(session_id) => {
            hook_key(HookProvider::ClaudeCode, session_id, None)
        }
        SessionScope::ClaudeSubagent { parent, agent_id } => {
            hook_key(HookProvider::ClaudeCode, parent, Some(agent_id))
        }
        SessionScope::Codex { rollout_id } => hook_key(HookProvider::Codex, rollout_id, None),
    }
}

fn hook_key(provider: HookProvider, session_id: &str, agent_id: Option<&str>) -> String {
    let payload = HookPayload {
        hook_event_name: if agent_id.is_some() {
            "SubagentStart".to_owned()
        } else {
            "SessionStart".to_owned()
        },
        session_id: session_id.to_owned(),
        source: None,
        agent_id: agent_id.map(str::to_owned),
        agent_type: None,
        task_id: None,
        task_subject: None,
    };
    map_hook_payload(provider, &payload, 0, 0)
        .into_iter()
        .find(|event| {
            if agent_id.is_some() {
                event.event_type == "dispatch"
            } else {
                event.event_type == "task_started"
            }
        })
        .expect("supported hook identity must map")
        .task_run_id
}

fn fact_lifecycle_time(fact: &LogFact) -> Option<i64> {
    match fact {
        LogFact::Append { at_ms, .. }
        | LogFact::CodexTurnStarted { at_ms, .. }
        | LogFact::CodexTurnComplete { at_ms, .. }
        | LogFact::CodexTurnAborted { at_ms, .. }
        | LogFact::Activity { at_ms, .. }
        | LogFact::Usage { at_ms, .. }
        | LogFact::SubagentAppeared { at_ms, .. }
        | LogFact::SubagentEnded { at_ms, .. }
        | LogFact::EvidenceId { at_ms, .. } => Some(*at_ms),
        LogFact::AiTitle { .. }
        | LogFact::ClaudeCwd { .. }
        | LogFact::CodexMeta { .. }
        | LogFact::CodexTurn { .. }
        | LogFact::CodexPid { .. } => None,
    }
}

fn live_line_event(
    artifact: &Path,
    ordinal: u64,
    sequence: usize,
    scope: &SessionScope,
    at_ms: i64,
    line: Option<String>,
) -> ProviderEvent {
    let (provider, agent_thread_id) = match run_key_for_scope(scope) {
        RunKey::Controller(key) => (Provider::Claude, key),
        RunKey::Native { provider, sid } => (provider, sid),
        RunKey::NativePath { .. } | RunKey::Provisional { .. } => {
            unreachable!("lane scopes have only Controller or Native keys")
        }
    };
    ProviderEvent::Activity {
        provider,
        agent_thread_id: agent_thread_id.clone(),
        activity: MinimalProviderMetadata {
            agent_id: Some(agent_thread_id),
            event_kind: Some(LIVE_LINE_EVENT_KIND.to_owned()),
            tool_name: Some(line.unwrap_or_default()),
            ..MinimalProviderMetadata::default()
        },
        depth: None,
        event_id: deterministic_live_line_event_id(artifact, ordinal, sequence, scope),
        observed_at_ms: at_ms,
        position: SourcePosition {
            path_id: u32::MAX,
            generation: 0,
            offset: ordinal,
        },
    }
}

fn controller_event(
    artifact: &Path,
    ordinal: u64,
    scope: &SessionScope,
    event: ControllerEventKind,
    at_ms: i64,
    label: Option<String>,
    provider_metadata: Option<MinimalProviderMetadata>,
) -> ControllerEvent {
    let source_event_type = match &event {
        ControllerEventKind::Dispatch { .. } => "dispatch",
        ControllerEventKind::TaskStarted => "task_started",
        ControllerEventKind::DependsOn { .. } => "depends_on",
        ControllerEventKind::Blocked => "blocked",
        ControllerEventKind::Progress => "progress",
        ControllerEventKind::Complete => "complete",
        ControllerEventKind::Failed => "failed",
        ControllerEventKind::Cancelled => "cancelled",
        ControllerEventKind::Dismiss => "dismiss",
    };
    let (provider, native_session_id) = match scope {
        SessionScope::ClaudeRoot(session_id) => (Some(Provider::Claude), Some(session_id.clone())),
        SessionScope::ClaudeSubagent { .. } => (Some(Provider::Claude), None),
        SessionScope::Codex { rollout_id } => (Some(Provider::Codex), Some(rollout_id.clone())),
    };
    ControllerEvent {
        schema_version: 1,
        task_run_id: controller_key_for_scope(scope),
        metadata: EventMetadata {
            event_id: deterministic_event_id(artifact, ordinal, &event, scope),
            timestamp_ms: at_ms,
            receipt_time_ms: at_ms,
            source: SOURCE_LOG_LANE.to_owned(),
            source_event_type: source_event_type.to_owned(),
            herdr_session: String::new(),
            workspace_id: None,
            tab_id: None,
            pane_id: None,
            terminal_id: None,
            provider,
            native_session_id,
            task_run_id: None,
            agent_node_id: None,
            task_state: None,
            execution_parent: None,
            dependency: None,
            source_coverage: Vec::new(),
            provider_metadata,
            label: label.as_deref().map(sanitize_controller_text),
            reason: None,
            progress: None,
            ingest_seq: None,
        },
        event,
    }
}

fn controller_event_kind_slug(event: &ControllerEventKind) -> &'static str {
    match event {
        ControllerEventKind::Dispatch { .. } => "dispatch",
        ControllerEventKind::TaskStarted => "task-started",
        ControllerEventKind::DependsOn { .. } => "depends-on",
        ControllerEventKind::Blocked => "blocked",
        ControllerEventKind::Progress => "progress",
        ControllerEventKind::Complete => "complete",
        ControllerEventKind::Failed => "failed",
        ControllerEventKind::Cancelled => "cancelled",
        ControllerEventKind::Dismiss => "dismiss",
    }
}

fn deterministic_event_id(
    artifact: &Path,
    ordinal: u64,
    event: &ControllerEventKind,
    scope: &SessionScope,
) -> String {
    let basename = artifact
        .file_name()
        .unwrap_or_else(|| OsStr::new("artifact"))
        .to_string_lossy();
    let kind = controller_event_kind_slug(event);
    let target_id = match scope {
        SessionScope::ClaudeRoot(session_id) => session_id,
        SessionScope::ClaudeSubagent { agent_id, .. } => agent_id,
        SessionScope::Codex { rollout_id } => rollout_id,
    };
    format!("log:{basename}:{ordinal}:{kind}:{target_id}")
}

fn deterministic_subject_event_id(artifact: &Path, ordinal: u64, scope: &SessionScope) -> String {
    deterministic_lane_event_id(artifact, ordinal, "subject", None, scope)
}

fn deterministic_live_line_event_id(
    artifact: &Path,
    ordinal: u64,
    sequence: usize,
    scope: &SessionScope,
) -> String {
    deterministic_lane_event_id(artifact, ordinal, "activity", Some(sequence), scope)
}

fn deterministic_lane_event_id(
    artifact: &Path,
    ordinal: u64,
    kind: &str,
    sequence: Option<usize>,
    scope: &SessionScope,
) -> String {
    let basename = artifact
        .file_name()
        .unwrap_or_else(|| OsStr::new("artifact"))
        .to_string_lossy();
    let target_id = match scope {
        SessionScope::ClaudeRoot(session_id) => session_id,
        SessionScope::ClaudeSubagent { agent_id, .. } => agent_id,
        SessionScope::Codex { rollout_id } => rollout_id,
    };
    sequence.map_or_else(
        || format!("log:{basename}:{ordinal}:{kind}:{target_id}"),
        |sequence| format!("log:{basename}:{ordinal}:{kind}:{target_id}:{sequence}"),
    )
}

/// One provider artifact kind retained by the evidence index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiscoveredArtifactKind {
    /// Claude root transcript.
    ClaudeSession {
        /// Provider-native Claude session ID.
        session_id: String,
    },
    /// Claude subagent transcript or metadata sidecar.
    ClaudeSubagent {
        /// Owning Claude root session ID.
        parent: String,
        /// Provider-native Claude subagent ID.
        agent_id: String,
    },
    /// Codex rollout transcript.
    CodexRollout {
        /// Provider-native Codex rollout ID.
        rollout_id: String,
    },
}

/// One artifact observed by bounded provider discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredArtifact {
    /// Provider that owns the artifact.
    pub provider: Provider,
    /// Absolute path observed during discovery.
    pub path: PathBuf,
    /// Filesystem modification time observed during discovery, in Unix milliseconds.
    pub modified_ms: i64,
    /// Filename-derived creation time, when the provider artifact names it unambiguously.
    pub creation_ms: Option<i64>,
    /// Provider-specific artifact identity and kind.
    pub kind: DiscoveredArtifactKind,
}

/// Identity-keyed artifact inventory used only for evidence matching.
#[derive(Clone, Debug, Default)]
pub struct AdmissionIndex {
    by_identity: HashMap<String, Vec<DiscoveredArtifact>>,
    had_errors: bool,
}

impl AdmissionIndex {
    /// Creates an empty evidence-matching index.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one discovered Claude root transcript.
    pub fn insert_claude_session(&mut self, session_id: &str, path: PathBuf, modified_ms: i64) {
        self.insert(
            session_id,
            DiscoveredArtifact {
                provider: Provider::Claude,
                path,
                modified_ms,
                creation_ms: None,
                kind: DiscoveredArtifactKind::ClaudeSession {
                    session_id: session_id.to_owned(),
                },
            },
        );
    }

    /// Records one discovered Claude subagent transcript or metadata sidecar.
    pub fn insert_claude_subagent(
        &mut self,
        parent: &str,
        agent_id: &str,
        path: PathBuf,
        modified_ms: i64,
    ) {
        self.insert(
            agent_id,
            DiscoveredArtifact {
                provider: Provider::Claude,
                path,
                modified_ms,
                creation_ms: None,
                kind: DiscoveredArtifactKind::ClaudeSubagent {
                    parent: parent.to_owned(),
                    agent_id: agent_id.to_owned(),
                },
            },
        );
    }

    /// Records one discovered Codex rollout transcript.
    pub fn insert_codex_rollout(&mut self, rollout_id: &str, path: PathBuf, modified_ms: i64) {
        let creation_ms = path
            .file_name()
            .and_then(OsStr::to_str)
            .and_then(rollout_filename_timestamp_ms);
        self.insert(
            rollout_id,
            DiscoveredArtifact {
                provider: Provider::Codex,
                path,
                modified_ms,
                creation_ms,
                kind: DiscoveredArtifactKind::CodexRollout {
                    rollout_id: rollout_id.to_owned(),
                },
            },
        );
    }

    fn insert(&mut self, identity: &str, artifact: DiscoveredArtifact) {
        if identity.is_empty() {
            return;
        }
        let artifacts = self.by_identity.entry(identity.to_owned()).or_default();
        if let Some(existing) = artifacts.iter_mut().find(|existing| {
            existing.provider == artifact.provider
                && existing.path == artifact.path
                && existing.kind == artifact.kind
        }) {
            existing.modified_ms = artifact.modified_ms;
        } else {
            artifacts.push(artifact);
        }
    }

    /// Indexes rollout filenames only in local-time date shards on or after `anchor_ms`.
    ///
    /// Within the anchor day, parseable filename timestamps before the anchor are skipped.
    /// Individual nested-entry errors are recorded and do not discard healthy siblings.
    pub fn discover_codex_date_shards(root: &Path, anchor_ms: i64) -> io::Result<Self> {
        let result = Self::discover_codex_date_shards_inner(root, anchor_ms);
        #[cfg(test)]
        CODEX_SHARD_SCAN_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        #[cfg(test)]
        CODEX_DISCOVERY_FILE_TYPE_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        result
    }

    fn discover_codex_date_shards_inner(root: &Path, anchor_ms: i64) -> io::Result<Self> {
        let mut index = Self::new();
        for year in sorted_directory_entries(root, true, &mut index.had_errors)? {
            let Some(year_value) = fixed_decimal(&year.file_name(), 4) else {
                continue;
            };
            let year_path = year.path();
            let year_kind = match codex_discovery_file_type(&year, &year_path) {
                Ok(kind) => kind,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => {
                    index.had_errors = true;
                    continue;
                }
            };
            if !year_kind.is_dir() {
                continue;
            }
            for month in sorted_directory_entries(&year_path, false, &mut index.had_errors)? {
                let Some(month_value) = fixed_decimal(&month.file_name(), 2) else {
                    continue;
                };
                let month_path = month.path();
                let month_kind = match codex_discovery_file_type(&month, &month_path) {
                    Ok(kind) => kind,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(_) => {
                        index.had_errors = true;
                        continue;
                    }
                };
                if !month_kind.is_dir() {
                    continue;
                }
                for day in sorted_directory_entries(&month_path, false, &mut index.had_errors)? {
                    let Some(day_value) = fixed_decimal(&day.file_name(), 2) else {
                        continue;
                    };
                    let day_path = day.path();
                    let day_kind = match codex_discovery_file_type(&day, &day_path) {
                        Ok(kind) => kind,
                        Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                        Err(_) => {
                            index.had_errors = true;
                            continue;
                        }
                    };
                    if !day_kind.is_dir() {
                        continue;
                    }
                    let Some(shard_start_ms) =
                        local_datetime_epoch_ms(year_value, month_value, day_value, 0, 0, 0)
                    else {
                        continue;
                    };
                    let (next_year, next_month, next_day) =
                        if day_value < days_in_month(year_value, month_value) {
                            (year_value, month_value, day_value + 1)
                        } else if month_value < 12 {
                            (year_value, month_value + 1, 1)
                        } else {
                            (year_value.saturating_add(1), 1, 1)
                        };
                    let shard_end_ms = if next_year > 9_999 {
                        i64::MAX
                    } else if let Some(shard_end_ms) =
                        local_datetime_epoch_ms(next_year, next_month, next_day, 0, 0, 0)
                    {
                        shard_end_ms
                    } else {
                        continue;
                    };
                    if shard_end_ms <= anchor_ms {
                        continue;
                    }
                    let anchor_shard = shard_start_ms <= anchor_ms;
                    record_codex_shard_scan(&day_path);
                    for artifact in
                        sorted_directory_entries(&day_path, false, &mut index.had_errors)?
                    {
                        let artifact_path = artifact.path();
                        let artifact_kind =
                            match codex_discovery_file_type(&artifact, &artifact_path) {
                                Ok(kind) => kind,
                                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                                Err(_) => {
                                    index.had_errors = true;
                                    continue;
                                }
                            };
                        if !artifact_kind.is_file() {
                            continue;
                        }
                        let file_name = artifact.file_name();
                        let Some(file_name) = file_name.to_str() else {
                            continue;
                        };
                        if !file_name.starts_with("rollout-") || !file_name.ends_with(".jsonl") {
                            continue;
                        }
                        if anchor_shard
                            && rollout_filename_timestamp_ms(file_name)
                                .is_some_and(|timestamp_ms| timestamp_ms < anchor_ms)
                        {
                            continue;
                        }
                        let metadata = match artifact.metadata() {
                            Ok(metadata) => metadata,
                            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                            Err(_) => {
                                index.had_errors = true;
                                continue;
                            }
                        };
                        let modified_ms = match metadata.modified() {
                            Ok(modified) => super::system_time_ms(modified),
                            Err(_) => {
                                index.had_errors = true;
                                continue;
                            }
                        };
                        let rollout_id = super::facts::scan_raw_ids(file_name)
                            .into_iter()
                            .filter_map(|id| match id {
                                EvidenceId::Uuid(uuid) => Some(uuid),
                                EvidenceId::ConfigDir(_) => None,
                            })
                            .next_back();
                        if let Some(rollout_id) = rollout_id {
                            index.insert_codex_rollout(&rollout_id, artifact_path, modified_ms);
                        }
                    }
                }
            }
        }
        Ok(index)
    }

    /// Reports whether an exact discovered artifact is keyed by `uuid`.
    #[must_use]
    pub fn contains_uuid(&self, uuid: &str) -> bool {
        self.by_identity.contains_key(uuid)
    }

    /// Reports whether any nested directory entry could not be inspected.
    #[must_use]
    pub const fn had_errors(&self) -> bool {
        self.had_errors
    }

    /// Returns every discovered artifact exactly keyed by `uuid`.
    #[must_use]
    pub fn artifacts_for_uuid(&self, uuid: &str) -> &[DiscoveredArtifact] {
        self.by_identity.get(uuid).map_or(&[], Vec::as_slice)
    }

    /// Returns every filename-timestamped Codex rollout in stable identity/path order.
    pub fn codex_rollouts(&self) -> Vec<(&str, &DiscoveredArtifact)> {
        let mut rollouts = self
            .by_identity
            .iter()
            .flat_map(|(identity, artifacts)| {
                artifacts.iter().filter_map(move |artifact| {
                    matches!(artifact.kind, DiscoveredArtifactKind::CodexRollout { .. })
                        .then_some((identity.as_str(), artifact))
                })
            })
            .collect::<Vec<_>>();
        rollouts.sort_by(|left, right| (left.0, &left.1.path).cmp(&(right.0, &right.1.path)));
        rollouts
    }

    /// Indexes an artifact by provider-native identity using path topology and discovery mtime.
    /// This operation never opens or parses the artifact.
    pub(crate) fn observe_discovered_path(
        &mut self,
        provider: Provider,
        path: &Path,
        modified_ms: i64,
    ) {
        match provider {
            Provider::Claude => {
                let components = normal_components(path).unwrap_or_default();
                if let Some(index) = components
                    .iter()
                    .position(|component| *component == OsStr::new("subagents"))
                    && let (Some(parent), Some(file_name)) = (
                        index.checked_sub(1).and_then(|i| components.get(i)),
                        components.get(index + 1),
                    )
                    && let (Some(parent), Some(agent_id)) =
                        (parent.to_str(), subagent_artifact_id(file_name))
                {
                    self.insert_claude_subagent(parent, agent_id, path.to_path_buf(), modified_ms);
                    return;
                }
                if path.extension() == Some(OsStr::new("jsonl"))
                    && let Some(session_id) = path.file_stem().and_then(OsStr::to_str)
                    && super::valid_native_id(session_id)
                {
                    self.insert_claude_session(session_id, path.to_path_buf(), modified_ms);
                }
            }
            Provider::Codex => {
                let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
                    return;
                };
                if let Some(rollout_id) = super::facts::scan_raw_ids(file_name)
                    .into_iter()
                    .filter_map(|id| match id {
                        EvidenceId::Uuid(uuid) => Some(uuid),
                        EvidenceId::ConfigDir(_) => None,
                    })
                    .next_back()
                {
                    self.insert_codex_rollout(&rollout_id, path.to_path_buf(), modified_ms);
                }
            }
        }
    }
}

/// A discovery root whose membership stays scoped to its evidence parent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedRoot {
    /// Already-admitted scope whose evidence produced this root.
    pub scope: SessionScope,
    /// Provider and scoped configuration path to discover.
    pub root: DiscoveryRoot,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ScopeKey {
    ClaudeRoot(String),
    ClaudeSubagent { parent: String, agent_id: String },
    Codex(String),
}

impl From<&SessionScope> for ScopeKey {
    fn from(scope: &SessionScope) -> Self {
        match scope {
            SessionScope::ClaudeRoot(session_id) => Self::ClaudeRoot(session_id.clone()),
            SessionScope::ClaudeSubagent { parent, agent_id } => Self::ClaudeSubagent {
                parent: parent.clone(),
                agent_id: agent_id.clone(),
            },
            SessionScope::Codex { rollout_id } => Self::Codex(rollout_id.clone()),
        }
    }
}

/// Evidence-gated provider artifact admission state.
#[derive(Clone, Debug)]
pub struct Admission {
    anchor_ms: i64,
    claude_sessions: HashSet<String>,
    codex_rollouts: HashSet<String>,
    admitted_scopes: HashSet<ScopeKey>,
    admitted_paths: HashSet<PathBuf>,
    pane_paths: HashSet<PathBuf>,
    derived_roots: Vec<DerivedRoot>,
}

impl Admission {
    /// Creates an empty admission graph with a hard file-mtime anchor.
    #[must_use]
    pub fn new(anchor_ms: i64) -> Self {
        Self {
            anchor_ms,
            claude_sessions: HashSet::new(),
            codex_rollouts: HashSet::new(),
            admitted_scopes: HashSet::new(),
            admitted_paths: HashSet::new(),
            pane_paths: HashSet::new(),
            derived_roots: Vec::new(),
        }
    }

    /// Admits the provider-native session identity observed in a herdr pane.
    pub fn admit_pane_session(&mut self, provider: Provider, session_id: &str) {
        if session_id.is_empty() {
            return;
        }
        match provider {
            Provider::Claude => {
                self.claude_sessions.insert(session_id.to_owned());
                self.admitted_scopes
                    .insert(ScopeKey::ClaudeRoot(session_id.to_owned()));
            }
            Provider::Codex => {
                self.codex_rollouts.insert(session_id.to_owned());
                self.admitted_scopes
                    .insert(ScopeKey::Codex(session_id.to_owned()));
            }
        }
    }

    /// Starts a target refresh while retaining exact evidence admissions.
    pub(crate) fn begin_pane_cycle(&mut self) {
        self.claude_sessions.clear();
        self.codex_rollouts.clear();
        self.pane_paths.clear();
    }

    /// Admits an exact provider transcript path and its pane-root identity.
    ///
    /// Claude accepts only `<uuid>.jsonl`; Codex accepts only `rollout-*.jsonl` containing
    /// a rollout UUID. Returns `true` when the path is admitted and `false` when the path does
    /// not match the given provider.
    #[must_use]
    pub fn admit_pane_artifact(&mut self, provider: Provider, path: &Path) -> bool {
        let Some(session_id) = pane_artifact_session_id(provider, path) else {
            return false;
        };
        self.pane_paths.insert(path.to_path_buf());
        self.admit_pane_session(provider, &session_id);
        true
    }

    /// Applies one allowlisted evidence ID emitted by an already-admitted parent scope.
    ///
    /// UUID evidence admits only exact, in-window paths present in `discovered`.
    /// When one UUID matches multiple artifacts, all artifacts must identify the same scope and
    /// at least one must be in-window; this lets a live transcript admit its scope when a sidecar
    /// lags, while stale paths remain unattached. Configuration-directory evidence derives a
    /// provider root without enumerating or opening it.
    pub fn on_evidence(
        &mut self,
        parent: &SessionScope,
        id: &EvidenceId,
        discovered: &AdmissionIndex,
    ) -> Option<SessionScope> {
        if !self.admitted_scopes.contains(&ScopeKey::from(parent)) {
            return None;
        }
        match id {
            EvidenceId::ConfigDir(config_dir) => {
                if !config_dir.is_absolute() {
                    return None;
                }
                let derived = DerivedRoot {
                    scope: parent.clone(),
                    root: DiscoveryRoot {
                        provider: Provider::Claude,
                        path: config_dir.join("projects"),
                    },
                };
                if !self.derived_roots.contains(&derived) {
                    self.derived_roots.push(derived);
                }
                Some(parent.clone())
            }
            EvidenceId::Uuid(uuid) => {
                let artifacts = discovered.artifacts_for_uuid(uuid);
                let scope = artifacts.first()?.scope();
                if artifacts.iter().any(|artifact| artifact.scope() != scope) {
                    return None;
                }
                if !artifacts
                    .iter()
                    .any(|artifact| artifact.modified_ms >= self.anchor_ms)
                {
                    return None;
                }
                for artifact in artifacts
                    .iter()
                    .filter(|artifact| artifact.modified_ms >= self.anchor_ms)
                {
                    self.admitted_paths.insert(artifact.path.clone());
                }
                self.admitted_scopes.insert(ScopeKey::from(&scope));
                Some(scope)
            }
        }
    }

    /// Reports whether a path is admitted by pane identity or exact lineage evidence.
    ///
    /// Any path containing a `tool-results` component is categorically rejected.
    #[must_use]
    pub fn is_admitted_path(&self, path: &Path) -> bool {
        if has_component(path, OsStr::new("tool-results")) {
            return false;
        }
        if self.admitted_paths.contains(path) || self.pane_paths.contains(path) {
            return true;
        }
        if self
            .codex_rollouts
            .iter()
            .any(|rollout_id| codex_path_matches(path, rollout_id))
        {
            return true;
        }
        self.claude_sessions
            .iter()
            .any(|session_id| claude_session_path_matches(path, session_id))
    }

    /// Applies the hard backfill anchor to every admitted regular file except a
    /// pane-root or exact pane-path identity.
    #[must_use]
    pub fn is_admitted_file(&self, path: &Path, modified_ms: i64) -> bool {
        self.is_admitted_path(path)
            && (modified_ms >= self.anchor_ms
                || self.pane_paths.contains(path)
                || self.is_pane_root_path(path))
    }

    /// Returns provider roots derived from allowlisted configuration-directory evidence.
    #[must_use]
    pub fn derived_roots(&self) -> &[DerivedRoot] {
        &self.derived_roots
    }

    /// Returns the immutable hard backfill anchor for this admission graph.
    #[must_use]
    pub const fn anchor_ms(&self) -> i64 {
        self.anchor_ms
    }

    /// Returns a pane-root identity for basename-twin resolution.
    pub(crate) fn pane_root_identity(&self, provider: Provider, path: &Path) -> Option<String> {
        match provider {
            Provider::Claude => self.claude_sessions.iter().find_map(|session_id| {
                claude_root_path_matches(path, session_id).then(|| session_id.clone())
            }),
            Provider::Codex => self.codex_rollouts.iter().find_map(|rollout_id| {
                codex_path_matches(path, rollout_id).then(|| rollout_id.clone())
            }),
        }
    }

    fn is_pane_root_path(&self, path: &Path) -> bool {
        self.codex_rollouts
            .iter()
            .any(|rollout_id| codex_path_matches(path, rollout_id))
            || self
                .claude_sessions
                .iter()
                .any(|session_id| claude_root_path_matches(path, session_id))
    }
}

fn pane_artifact_session_id(provider: Provider, path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    match provider {
        Provider::Claude => {
            let session_id = file_name.strip_suffix(".jsonl")?;
            match super::facts::scan_raw_ids(session_id).as_slice() {
                [EvidenceId::Uuid(uuid)] if uuid == session_id => Some(uuid.clone()),
                _ => None,
            }
        }
        Provider::Codex => {
            let body = file_name.strip_prefix("rollout-")?.strip_suffix(".jsonl")?;
            super::facts::scan_raw_ids(body)
                .into_iter()
                .filter_map(|id| match id {
                    EvidenceId::Uuid(uuid) => Some(uuid),
                    EvidenceId::ConfigDir(_) => None,
                })
                .next_back()
        }
    }
}

/// Computes the hard backfill anchor, allowing the database only to narrow the window.
#[must_use]
pub fn backfill_anchor_ms(earliest_db_event: Option<i64>, now_ms: i64, window_ms: i64) -> i64 {
    let window_anchor = now_ms.saturating_sub(window_ms);
    earliest_db_event.map_or(window_anchor, |earliest| earliest.max(window_anchor))
}

fn parse_positive_duration_ms(value: Option<&OsStr>, default_ms: i64) -> i64 {
    value
        .and_then(OsStr::to_str)
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_ms)
}

/// Parses a positive UTF-8 decimal millisecond window or returns the one-day default.
#[must_use]
pub fn parse_backfill_window_ms(value: Option<&OsStr>) -> i64 {
    parse_positive_duration_ms(value, DEFAULT_BACKFILL_WINDOW_MS)
}

/// Parses a positive UTF-8 decimal completion grace or returns the 30-second default.
#[must_use]
pub fn parse_complete_grace_ms(value: Option<&OsStr>) -> i64 {
    parse_positive_duration_ms(value, DEFAULT_COMPLETE_GRACE_MS)
}

/// Parses a positive UTF-8 decimal inactivity interval or returns the ten-minute default.
#[must_use]
pub fn parse_headless_inactivity_ms(value: Option<&OsStr>) -> i64 {
    parse_positive_duration_ms(value, DEFAULT_HEADLESS_INACTIVITY_MS)
}

/// Parses a positive UTF-8 decimal stall warning interval or returns the five-minute default.
#[must_use]
pub fn parse_stall_warn_ms(value: Option<&OsStr>) -> i64 {
    parse_positive_duration_ms(value, DEFAULT_STALL_WARN_MS)
}

/// Parses a positive UTF-8 decimal ghost visibility interval or returns the five-minute default.
#[must_use]
pub fn parse_ghost_visibility_ms(value: Option<&OsStr>) -> i64 {
    parse_positive_duration_ms(value, DEFAULT_GHOST_VISIBILITY_MS)
}

/// Recomputes the zero-based record ordinal by counting newlines before `byte_offset`.
///
/// The read is routed through the admission-gated open seam and is used when reopening an
/// artifact at a nonzero byte offset without a persisted ordinal.
pub fn record_ordinal_at_offset(
    root: &Path,
    relative: &Path,
    byte_offset: u64,
    admission: &Admission,
    diagnostics: &ProviderDiagnostics,
) -> io::Result<u64> {
    let Some(mut file) = super::open_admitted_regular_file(root, relative, admission, diagnostics)?
    else {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "provider artifact is not admitted",
        ));
    };
    let mut remaining = byte_offset;
    let mut ordinal = 0_u64;
    let mut buffer = [0_u8; 8 * 1024];
    while remaining > 0 {
        let limit = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let read = file.read(&mut buffer[..limit])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "provider offset exceeds artifact length",
            ));
        }
        ordinal = ordinal.saturating_add(
            u64::try_from(buffer[..read].iter().filter(|byte| **byte == b'\n').count())
                .unwrap_or(u64::MAX),
        );
        remaining -= read as u64;
    }
    Ok(ordinal)
}

#[cfg(test)]
type CodexShardScanHook = Box<dyn FnMut(&Path)>;

#[cfg(test)]
type CodexDiscoveryFileTypeHook = Box<dyn FnMut(&Path) -> io::Result<()>>;

#[cfg(test)]
thread_local! {
    static CODEX_SHARD_SCAN_HOOK: RefCell<Option<CodexShardScanHook>> =
        const { RefCell::new(None) };
    static CODEX_DISCOVERY_FILE_TYPE_HOOK: RefCell<Option<CodexDiscoveryFileTypeHook>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
fn set_codex_shard_scan_hook(hook: impl FnMut(&Path) + 'static) {
    CODEX_SHARD_SCAN_HOOK.with(|slot| {
        assert!(
            slot.borrow_mut().replace(Box::new(hook)).is_none(),
            "codex shard scan hook was already installed"
        );
    });
}

#[cfg(test)]
fn set_codex_discovery_file_type_hook(hook: impl FnMut(&Path) -> io::Result<()> + 'static) {
    CODEX_DISCOVERY_FILE_TYPE_HOOK.with(|slot| {
        assert!(
            slot.borrow_mut().replace(Box::new(hook)).is_none(),
            "codex discovery file-type hook was already installed"
        );
    });
}

fn record_codex_shard_scan(path: &Path) {
    #[cfg(test)]
    CODEX_SHARD_SCAN_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(path);
        }
    });
    #[cfg(not(test))]
    let _ = path;
}

impl DiscoveredArtifact {
    fn scope(&self) -> SessionScope {
        match &self.kind {
            DiscoveredArtifactKind::ClaudeSession { session_id } => {
                SessionScope::ClaudeRoot(session_id.clone())
            }
            DiscoveredArtifactKind::ClaudeSubagent { parent, agent_id } => {
                SessionScope::ClaudeSubagent {
                    parent: parent.clone(),
                    agent_id: agent_id.clone(),
                }
            }
            DiscoveredArtifactKind::CodexRollout { rollout_id } => SessionScope::Codex {
                rollout_id: rollout_id.clone(),
            },
        }
    }
}

fn sorted_directory_entries(
    path: &Path,
    is_root: bool,
    had_errors: &mut bool,
) -> io::Result<Vec<DirEntry>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) if is_root => return Err(error),
        Err(_) => {
            *had_errors = true;
            return Ok(Vec::new());
        }
    };
    let mut found = Vec::new();
    for entry in entries {
        match entry {
            Ok(entry) => found.push(entry),
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => *had_errors = true,
        }
    }
    let mut entries = found;
    entries.sort_by_key(DirEntry::file_name);
    Ok(entries)
}

fn codex_discovery_file_type(entry: &DirEntry, path: &Path) -> io::Result<FileType> {
    #[cfg(test)]
    CODEX_DISCOVERY_FILE_TYPE_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(path)?;
        }
        Ok::<(), io::Error>(())
    })?;
    #[cfg(not(test))]
    let _ = path;
    entry.file_type()
}

fn fixed_decimal(value: &OsString, width: usize) -> Option<u32> {
    let value = value.to_str()?;
    (value.len() == width && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse().ok())?
}

fn civil_day(year: u32, month: u32, day: u32) -> Option<i64> {
    if year > 9_999
        || !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
    {
        return None;
    }
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

fn local_datetime_epoch_ms(
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Option<i64> {
    civil_day(year, month, day)?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    // Rollout names carry no UTC offset, so a local wall-clock time repeated during a
    // DST fall-back cannot be mapped to a unique epoch.
    let requested_year = i32::try_from(year).ok()?.checked_sub(1900)?;
    let requested_month = i32::try_from(month).ok()?.checked_sub(1)?;
    let requested_day = i32::try_from(day).ok()?;
    let requested_hour = i32::try_from(hour).ok()?;
    let requested_minute = i32::try_from(minute).ok()?;
    let requested_second = i32::try_from(second).ok()?;
    let matches_requested = |local: &libc::tm| {
        local.tm_year == requested_year
            && local.tm_mon == requested_month
            && local.tm_mday == requested_day
            && local.tm_hour == requested_hour
            && local.tm_min == requested_minute
            && local.tm_sec == requested_second
    };
    // SAFETY: all-zero is a valid baseline for C's integer/pointer `tm` fields;
    // the fields consumed by `mktime` are initialized below before the call.
    let mut local = unsafe { std::mem::zeroed::<libc::tm>() };
    local.tm_year = requested_year;
    local.tm_mon = requested_month;
    local.tm_mday = requested_day;
    local.tm_hour = requested_hour;
    local.tm_min = requested_minute;
    local.tm_sec = requested_second;
    local.tm_isdst = -1;
    local.tm_wday = -1;
    // SAFETY: `local` is a live, writable `tm`; `mktime` retains no pointer and
    // resolves the local timezone and DST status because `tm_isdst` is `-1`.
    let epoch_seconds = unsafe { libc::mktime(&raw mut local) };
    if local.tm_wday == -1 || local.tm_isdst < 0 || !matches_requested(&local) {
        return None;
    }

    // Retry the untouched civil fields with the opposite resolved DST state, as in the
    // installed mktime(3) ambiguity wrapper. `tm_wday` distinguishes failure from the
    // valid epoch value `-1` because successful `mktime` calls replace the sentinel.
    // SAFETY: the same all-zero baseline and field initialization rules apply here.
    let mut alternative = unsafe { std::mem::zeroed::<libc::tm>() };
    alternative.tm_year = requested_year;
    alternative.tm_mon = requested_month;
    alternative.tm_mday = requested_day;
    alternative.tm_hour = requested_hour;
    alternative.tm_min = requested_minute;
    alternative.tm_sec = requested_second;
    alternative.tm_isdst = i32::from(local.tm_isdst == 0);
    alternative.tm_wday = -1;
    // SAFETY: `alternative` is live and writable, and `mktime` retains no pointer.
    let alternative_epoch_seconds = unsafe { libc::mktime(&raw mut alternative) };
    if alternative.tm_wday != -1
        && alternative.tm_isdst >= 0
        && matches_requested(&alternative)
        && (alternative.tm_isdst > 0) != (local.tm_isdst > 0)
        && alternative_epoch_seconds != epoch_seconds
    {
        return None;
    }
    epoch_seconds.checked_mul(1_000)
}

pub(crate) fn rollout_filename_timestamp_ms(file_name: &str) -> Option<i64> {
    let value = file_name.strip_prefix("rollout-")?;
    let timestamp = value.get(..19)?;
    if value.as_bytes().get(19) != Some(&b'-')
        || timestamp.as_bytes().get(4) != Some(&b'-')
        || timestamp.as_bytes().get(7) != Some(&b'-')
        || timestamp.as_bytes().get(10) != Some(&b'T')
        || timestamp.as_bytes().get(13) != Some(&b'-')
        || timestamp.as_bytes().get(16) != Some(&b'-')
    {
        return None;
    }
    let year = timestamp.get(0..4)?.parse::<u32>().ok()?;
    let month = timestamp.get(5..7)?.parse::<u32>().ok()?;
    let day = timestamp.get(8..10)?.parse::<u32>().ok()?;
    let hour = timestamp.get(11..13)?.parse::<u32>().ok()?;
    let minute = timestamp.get(14..16)?.parse::<u32>().ok()?;
    let second = timestamp.get(17..19)?.parse::<u32>().ok()?;
    local_datetime_epoch_ms(year, month, day, hour, minute, second)
}

#[cfg(test)]
pub(crate) fn local_rollout_fixture_parts(epoch_ms: i64) -> (String, String) {
    let epoch_seconds = libc::time_t::try_from(epoch_ms.div_euclid(1_000)).unwrap();
    let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
    // SAFETY: `local` points to writable storage for one `tm`, and `epoch_seconds`
    // remains live for the duration of this non-retaining libc call.
    let result = unsafe { libc::localtime_r(&epoch_seconds, local.as_mut_ptr()) };
    assert!(
        !result.is_null(),
        "localtime_r rejected fixture epoch {epoch_ms}"
    );
    // SAFETY: non-null `localtime_r` initialized the output `tm`.
    let local = unsafe { local.assume_init() };
    (
        format!(
            "{:04}/{:02}/{:02}",
            local.tm_year + 1900,
            local.tm_mon + 1,
            local.tm_mday,
        ),
        format!(
            "{:04}-{:02}-{:02}T{:02}-{:02}-{:02}",
            local.tm_year + 1900,
            local.tm_mon + 1,
            local.tm_mday,
            local.tm_hour,
            local.tm_min,
            local.tm_sec,
        ),
    )
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

fn has_component(path: &Path, expected: &OsStr) -> bool {
    path.components()
        .any(|component| matches!(component, Component::Normal(value) if value == expected))
}

fn normal_components(path: &Path) -> Option<Vec<&OsStr>> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(Some(value)),
            Component::CurDir | Component::RootDir => None,
            Component::ParentDir | Component::Prefix(_) => Some(None),
        })
        .collect()
}

fn codex_path_matches(path: &Path, rollout_id: &str) -> bool {
    let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
        return false;
    };
    let Some(stem) = file_name.strip_suffix(".jsonl") else {
        return false;
    };
    stem == rollout_id
        || stem
            .strip_suffix(rollout_id)
            .is_some_and(|prefix| prefix.ends_with('-'))
}

fn claude_session_path_matches(path: &Path, session_id: &str) -> bool {
    claude_root_path_matches(path, session_id)
        || claude_subagent_path_matches(path, session_id, None)
}

fn claude_root_path_matches(path: &Path, session_id: &str) -> bool {
    path.extension() == Some(OsStr::new("jsonl"))
        && path.file_stem().and_then(OsStr::to_str) == Some(session_id)
}

fn claude_subagent_path_matches(
    path: &Path,
    parent: &str,
    expected_agent_id: Option<&str>,
) -> bool {
    let Some(components) = normal_components(path) else {
        return false;
    };
    components.windows(2).enumerate().any(|(index, pair)| {
        if pair[0] != OsStr::new(parent) || pair[1] != OsStr::new("subagents") {
            return false;
        }
        match components.get(index + 2) {
            None => true,
            Some(file_name) if components.len() == index + 3 => subagent_artifact_id(file_name)
                .is_some_and(|agent_id| {
                    expected_agent_id.is_none_or(|expected| agent_id == expected)
                }),
            Some(_) => false,
        }
    })
}

pub(crate) fn claude_subagent_artifact_path_matches(path: &Path) -> bool {
    if path.extension() != Some(OsStr::new("jsonl")) {
        return false;
    }
    let Some(components) = normal_components(path) else {
        return false;
    };
    let Some(parent) = components
        .len()
        .checked_sub(3)
        .and_then(|index| components.get(index))
        .and_then(|parent| parent.to_str())
    else {
        return false;
    };
    super::valid_native_id(parent) && claude_subagent_path_matches(path, parent, None)
}

fn subagent_artifact_id(file_name: &OsStr) -> Option<&str> {
    let file_name = file_name.to_str()?;
    let stem = file_name
        .strip_suffix(".jsonl")
        .or_else(|| file_name.strip_suffix(".meta.json"))?;
    stem.strip_prefix("agent-").filter(|id| !id.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::env;
    use std::ffi::OsStr;
    use std::fs::{self, FileTimes, OpenOptions};
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, UNIX_EPOCH};

    use crate::hook_adapter::{HookPayload, HookProvider, map_hook_payload};
    use crate::model::{
        AgentNodeObservation, ControllerEvent, ControllerEventKind, DomainModel, EventMetadata,
        MinimalProviderMetadata, NormalizedEvent, Provider, RunKey, SourceCoverage, TaskState,
    };
    use crate::provider::claude::{ClaudePathTopology, path_topology};
    use crate::provider::claude_facts::{extract_claude_line, extract_claude_line_at};
    use crate::provider::facts::{ActivitySource, EvidenceId, LogFact, SessionScope};
    use crate::provider::tail::{MAX_TAIL_RECORD_BYTES, RECORD_TOO_LONG_ERROR};
    use crate::provider::{
        FirstSeenBaseline, FsReadBoundary, ProviderDiagnostics, ProviderEvent, TailFile,
        open_admitted_regular_file,
    };
    use crate::reducer::Reducer;
    use crate::store::RestoredState;
    use crate::tui::view::task_run_label;

    use super::*;

    const PARENT: &str = "11111111-1111-4111-8111-111111111111";
    const ROLLOUT: &str = "22222222-2222-4222-8222-222222222222";
    const STRANGER: &str = "33333333-3333-4333-8333-333333333333";

    fn discover_codex_rollout_with_mtime(
        root: &Path,
        rollout_id: &str,
        modified_ms: i64,
        anchor_ms: i64,
    ) -> (AdmissionIndex, PathBuf) {
        let (shard, timestamp) = local_rollout_fixture_parts(anchor_ms.saturating_add(3_600_000));
        let shard = root.join(shard);
        fs::create_dir_all(&shard).unwrap();
        let path = shard.join(format!("rollout-{timestamp}-{rollout_id}.jsonl"));
        fs::write(&path, b"{}\n").unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(
                FileTimes::new()
                    .set_modified(UNIX_EPOCH + Duration::from_millis(modified_ms as u64)),
            )
            .unwrap();
        (
            AdmissionIndex::discover_codex_date_shards(root, anchor_ms).unwrap(),
            path,
        )
    }

    fn synthesize(
        synthesis: &mut Synthesis,
        artifact: &str,
        facts: impl IntoIterator<Item = (u64, LogFact)>,
    ) -> Vec<ProviderEvent> {
        synthesis.synthesize_batch(
            Path::new(artifact),
            facts,
            &mut Admission::new(0),
            &AdmissionIndex::new(),
        )
    }

    fn synthesize_subagent_start(
        synthesis: &mut Synthesis,
        agent_id: &str,
        source_at_ms: Option<i64>,
    ) -> (SessionScope, Vec<ProviderEvent>) {
        let artifact_at_ms = source_at_ms.unwrap_or(synthesis.latest_lifecycle_ms.max(1));
        if let Some(source_at_ms) = source_at_ms {
            synthesis
                .last_append_ms
                .insert(ScopeKey::ClaudeRoot(PARENT.to_owned()), source_at_ms);
        }
        let scope = SessionScope::ClaudeSubagent {
            parent: PARENT.to_owned(),
            agent_id: agent_id.to_owned(),
        };
        let events = synthesize(
            synthesis,
            "agent-child.meta.json",
            [(
                1,
                LogFact::SubagentAppeared {
                    parent: PARENT.to_owned(),
                    agent_id: agent_id.to_owned(),
                    agent_type: "reviewer".to_owned(),
                    description: format!("Review {agent_id} lifecycle"),
                    at_ms: artifact_at_ms,
                },
            )],
        );
        (scope, events)
    }

    #[test]
    fn subagent_metadata_before_parent_append_uses_artifact_time() {
        let events = synthesize(
            &mut Synthesis::default(),
            "agent-before-append.meta.json",
            [(
                0,
                LogFact::SubagentAppeared {
                    parent: PARENT.to_owned(),
                    agent_id: "before-append".to_owned(),
                    agent_type: "reviewer".to_owned(),
                    description: "Review metadata ordering".to_owned(),
                    at_ms: 7_654,
                },
            )],
        );

        assert!(
            synthesized_events(&events)
                .iter()
                .all(|event| event.metadata.timestamp_ms == 7_654)
        );
    }

    #[test]
    fn subagent_end_coalescing_keeps_earliest_ordinal_and_timestamp_across_batches() {
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 600);
        let _ = synthesize_subagent_start(&mut synthesis, "coalesced", Some(50));

        let mut events = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                9,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "coalesced".to_owned(),
                    failed: false,
                    at_ms: 200,
                },
            )],
        );
        events.extend(synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                3,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "coalesced".to_owned(),
                    failed: false,
                    at_ms: 100,
                },
            )],
        ));
        events.extend(synthesis.advance_lifecycle(129));
        events.extend(synthesis.advance_lifecycle(130));
        events.extend(synthesis.advance_lifecycle(300));

        assert!(matches!(
            synthesized_events(&events).as_slice(),
            [event]
                if event.metadata.event_id == "log:queue.jsonl:3:complete:coalesced"
                    && event.metadata.timestamp_ms == 100
                    && matches!(event.event, ControllerEventKind::Complete)
        ));
    }

    #[test]
    fn earlier_failed_subagent_end_supersedes_cross_batch_complete() {
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 600);
        let _ = synthesize_subagent_start(&mut synthesis, "failed-coalesced", Some(50));

        let mut events = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                3,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "failed-coalesced".to_owned(),
                    failed: false,
                    at_ms: 200,
                },
            )],
        );
        events.extend(synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                9,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "failed-coalesced".to_owned(),
                    failed: true,
                    at_ms: 100,
                },
            )],
        ));
        events.extend(synthesis.advance_lifecycle(300));

        assert!(matches!(
            synthesized_events(&events).as_slice(),
            [event]
                if event.metadata.event_id == "log:queue.jsonl:3:failed:failed-coalesced"
                    && event.metadata.timestamp_ms == 100
                    && matches!(event.event, ControllerEventKind::Failed)
        ));
    }

    #[test]
    fn published_subagent_complete_ignores_retroactive_duplicate() {
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 600);
        let _ = synthesize_subagent_start(&mut synthesis, "published", Some(50));

        let mut events = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                9,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "published".to_owned(),
                    failed: false,
                    at_ms: 200,
                },
            )],
        );
        events.extend(synthesis.advance_lifecycle(230));
        events.extend(synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                3,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "published".to_owned(),
                    failed: false,
                    at_ms: 100,
                },
            )],
        ));
        events.extend(synthesis.advance_lifecycle(300));

        assert!(matches!(
            synthesized_events(&events).as_slice(),
            [event]
                if event.metadata.event_id == "log:queue.jsonl:9:complete:published"
                    && event.metadata.timestamp_ms == 200
                    && matches!(event.event, ControllerEventKind::Complete)
        ));
    }

    #[test]
    fn lineage_fact_times_participate_in_lifecycle_time() {
        let facts = [
            LogFact::SubagentAppeared {
                parent: PARENT.to_owned(),
                agent_id: "appeared".to_owned(),
                agent_type: "reviewer".to_owned(),
                description: "Review appearance".to_owned(),
                at_ms: 10,
            },
            LogFact::SubagentEnded {
                parent: PARENT.to_owned(),
                agent_id: "ended".to_owned(),
                failed: false,
                at_ms: 20,
            },
            LogFact::EvidenceId {
                parent: SessionScope::ClaudeRoot(PARENT.to_owned()),
                id: EvidenceId::Uuid("77777777-7777-4777-8777-777777777777".to_owned()),
                at_ms: 30,
            },
        ];

        assert_eq!(
            facts.iter().map(fact_lifecycle_time).collect::<Vec<_>>(),
            [Some(10), Some(20), Some(30),]
        );
    }

    fn synthesized_events(events: &[ProviderEvent]) -> Vec<&ControllerEvent> {
        events
            .iter()
            .filter_map(|event| match event {
                ProviderEvent::Synthesized(event) => Some(event),
                _ => None,
            })
            .collect()
    }

    fn hook_payload(event_name: &str, session_id: &str, agent_id: Option<&str>) -> HookPayload {
        HookPayload {
            hook_event_name: event_name.to_owned(),
            session_id: session_id.to_owned(),
            source: None,
            agent_id: agent_id.map(str::to_owned),
            agent_type: Some("reviewer".to_owned()),
            task_id: None,
            task_subject: None,
        }
    }

    fn controller_event_from_hook(
        envelope: &crate::herdr::controller::ControllerEnvelope,
    ) -> ControllerEvent {
        let event = match envelope.event_type.as_str() {
            "dispatch" => ControllerEventKind::Dispatch {
                parent_task_run_id: envelope.parent_task_run_id.clone().unwrap(),
            },
            "task_started" => ControllerEventKind::TaskStarted,
            "complete" => ControllerEventKind::Complete,
            other => panic!("unsupported test hook event {other}"),
        };
        ControllerEvent {
            schema_version: envelope.schema_version,
            task_run_id: envelope.task_run_id.clone(),
            metadata: EventMetadata {
                event_id: envelope.event_id.clone(),
                timestamp_ms: envelope.emitted_at_ms,
                receipt_time_ms: envelope.emitted_at_ms,
                source: envelope.source.clone(),
                source_event_type: envelope.event_type.clone(),
                herdr_session: "test".to_owned(),
                workspace_id: None,
                tab_id: None,
                pane_id: None,
                terminal_id: envelope.terminal_id.clone(),
                provider: match envelope.provider.as_deref() {
                    Some("claude") => Some(Provider::Claude),
                    Some("codex") => Some(Provider::Codex),
                    _ => None,
                },
                native_session_id: envelope.native_session_id.clone(),
                task_run_id: None,
                agent_node_id: None,
                task_state: None,
                execution_parent: None,
                dependency: None,
                source_coverage: Vec::<SourceCoverage>::new(),
                provider_metadata: None::<MinimalProviderMetadata>,
                label: envelope.label.clone(),
                reason: envelope.reason.clone(),
                progress: None,
                ingest_seq: None,
            },
            event,
        }
    }

    fn advance_reducer(reducer: Reducer, event: &ControllerEvent) -> Reducer {
        let delta = reducer
            .validate_controller_event(event)
            .expect("test event should validate");
        Reducer::new(RestoredState {
            model: delta.post_model,
            next_ordinal: delta.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        })
        .0
    }

    fn apply_once_per_event_id(events: &[ProviderEvent]) -> DomainModel {
        let mut state = RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        };
        let mut ledger = HashSet::new();
        for event in synthesized_events(events) {
            if ledger.insert(event.metadata.event_id.clone()) {
                let reducer = Reducer::new(state).0;
                let delta = reducer
                    .validate_controller_event(event)
                    .expect("test event should validate");
                state = RestoredState {
                    model: delta.post_model,
                    next_ordinal: delta.post_next_ordinal,
                    next_ingest_seq: Some(1),
                    event_ledger: Vec::new(),
                };
            }
        }
        state.model
    }

    #[test]
    fn run_rows_render_stable_lane_kind() {
        const AGENT: &str = "child-kind";
        let claude_scope = SessionScope::ClaudeSubagent {
            parent: PARENT.to_owned(),
            agent_id: AGENT.to_owned(),
        };
        let codex_scope = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let mut synthesis = Synthesis::default();
        let mut events = synthesize(
            &mut synthesis,
            "agent-child-kind.meta.json",
            [(
                0,
                LogFact::SubagentAppeared {
                    parent: PARENT.to_owned(),
                    agent_id: AGENT.to_owned(),
                    agent_type: "reviewer".to_owned(),
                    description: "Review the projected rows".to_owned(),
                    at_ms: 1,
                },
            )],
        );
        events.extend(synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [
                (
                    0,
                    LogFact::Append {
                        scope: codex_scope.clone(),
                        at_ms: 1,
                    },
                ),
                (
                    0,
                    LogFact::CodexMeta {
                        rollout_id: ROLLOUT.to_owned(),
                        cwd: "/tmp/project".to_owned(),
                        originator: "codex_cli_rs".to_owned(),
                        internal: None,
                        cli_version: "0.1.0".to_owned(),
                    },
                ),
            ],
        ));
        let model = apply_once_per_event_id(&events);
        let claude_run = model
            .task_run_by_key(&run_key_for_scope(&claude_scope))
            .expect("the lane must create the Claude subagent run");
        let codex_run = model
            .task_run_by_key(&run_key_for_scope(&codex_scope))
            .expect("the lane must create the Codex session run");
        let claude_run_id = claude_run.run_id;
        let claude_label = task_run_label(&model, claude_run, false, 0, false, true);
        let codex_label = task_run_label(&model, codex_run, false, 0, false, true);
        assert!(
            claude_label.starts_with("● reviewer "),
            "Claude row must render agentType: {claude_label}"
        );
        assert!(
            codex_label.starts_with("● codex_cli_rs"),
            "Codex row must render rollout originator: {codex_label}"
        );

        let agent_node_id = "volatile-agent-node";
        let lane_started = synthesized_events(&events)
            .into_iter()
            .find(|event| {
                event.task_run_id == controller_key_for_scope(&claude_scope)
                    && matches!(event.event, ControllerEventKind::TaskStarted)
            })
            .expect("the lane must synthesize the Claude start")
            .clone();
        let mut later_started = lane_started.clone();
        later_started.metadata.event_id = "later-task-started".to_owned();
        later_started.metadata.timestamp_ms = 1;
        later_started.metadata.receipt_time_ms = 1;
        later_started.metadata.provider_metadata = Some(MinimalProviderMetadata {
            event_kind: Some("must-not-overwrite-reviewer".to_owned()),
            ..MinimalProviderMetadata::default()
        });
        let (reducer, _shared) = Reducer::new(RestoredState {
            model,
            next_ordinal: 10,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let later_started = reducer
            .validate_controller_event(&later_started)
            .expect("a repeated lane start must remain valid");
        let mut metadata = lane_started.metadata;
        metadata.event_id = "later-agent-node".to_owned();
        metadata.timestamp_ms = 1;
        metadata.receipt_time_ms = 1;
        metadata.source_event_type = "agent_node".to_owned();
        metadata.task_run_id = Some(claude_run_id);
        metadata.task_state = None;
        metadata.execution_parent = None;
        metadata.agent_node_id = Some(agent_node_id.to_owned());
        metadata.provider_metadata = None;
        let (mut reducer, shared) = Reducer::new(RestoredState {
            model: later_started.post_model,
            next_ordinal: later_started.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        reducer
            .apply(NormalizedEvent::AgentNodeUpsert {
                metadata: metadata.clone(),
                node: AgentNodeObservation {
                    agent_node_id: agent_node_id.to_owned(),
                    provider: Provider::Claude,
                    native_session_id: None,
                    task_run_id: claude_run_id,
                    parent_agent_node_id: None,
                    state: None,
                    model_id: None,
                    session_file: None,
                },
            })
            .expect("agent node must apply");
        let activity = MinimalProviderMetadata {
            agent_id: Some("volatile-agent".to_owned()),
            event_kind: Some("unrelated_activity".to_owned()),
            ..MinimalProviderMetadata::default()
        };
        metadata.event_id = "later-agent-activity".to_owned();
        metadata.source_event_type = "activity".to_owned();
        metadata.provider_metadata = Some(activity.clone());
        reducer
            .apply(NormalizedEvent::AgentActivity {
                metadata,
                agent_node_id: agent_node_id.to_owned(),
                activity,
            })
            .expect("later activity must apply");

        let snapshot = shared.borrow();
        assert_eq!(
            snapshot
                .agent_node(agent_node_id)
                .and_then(|node| node.last_event_kind.as_deref()),
            Some("unrelated_activity"),
            "the volatile activity path must actually update"
        );
        let stable_label = task_run_label(
            &snapshot,
            snapshot.task_run(&claude_run_id).unwrap(),
            false,
            1,
            false,
            true,
        );
        assert!(
            stable_label.starts_with("● reviewer "),
            "later activity must not overwrite the stable run kind: {stable_label}"
        );
    }

    #[test]
    fn synthesized_claude_keys_byte_match_hook_adapter() {
        let hook = map_hook_payload(
            HookProvider::ClaudeCode,
            &hook_payload("SubagentStart", PARENT, Some("child-7")),
            100,
            7,
        );
        let hook_key = hook
            .iter()
            .find(|event| event.event_type == "dispatch")
            .unwrap()
            .task_run_id
            .clone();

        assert_eq!(
            run_key_for_scope(&SessionScope::ClaudeSubagent {
                parent: PARENT.to_owned(),
                agent_id: "child-7".to_owned(),
            }),
            RunKey::Controller(hook_key)
        );
    }

    #[test]
    fn event_ids_deterministic_across_replay() {
        let fact = LogFact::CodexTurnAborted {
            rollout_id: ROLLOUT.to_owned(),
            at_ms: 123,
        };
        let first = synthesize(
            &mut Synthesis::default(),
            "rollout.jsonl",
            [(9, fact.clone())],
        );
        let replay = synthesize(&mut Synthesis::default(), "rollout.jsonl", [(9, fact)]);
        let first_ids = synthesized_events(&first)
            .iter()
            .map(|event| event.metadata.event_id.as_str())
            .collect::<Vec<_>>();
        let replay_ids = synthesized_events(&replay)
            .iter()
            .map(|event| event.metadata.event_id.as_str())
            .collect::<Vec<_>>();
        let ledger = first_ids
            .iter()
            .chain(replay_ids.iter())
            .copied()
            .collect::<HashSet<_>>();

        assert_eq!(
            first_ids,
            ["log:rollout.jsonl:9:cancelled:22222222-2222-4222-8222-222222222222"]
        );
        assert_eq!(first_ids, replay_ids);
        assert_eq!(ledger.len(), 1, "replay must have one durable ledger key");
    }

    #[test]
    fn controller_event_kind_slugs_cover_the_stable_wire_vocabulary() {
        let cases = [
            (
                ControllerEventKind::Dispatch {
                    parent_task_run_id: "parent".to_owned(),
                },
                "dispatch",
            ),
            (ControllerEventKind::TaskStarted, "task-started"),
            (
                ControllerEventKind::DependsOn {
                    depends_on_id: "dependency".to_owned(),
                },
                "depends-on",
            ),
            (ControllerEventKind::Blocked, "blocked"),
            (ControllerEventKind::Progress, "progress"),
            (ControllerEventKind::Complete, "complete"),
            (ControllerEventKind::Failed, "failed"),
            (ControllerEventKind::Cancelled, "cancelled"),
            (ControllerEventKind::Dismiss, "dismiss"),
        ];

        assert_eq!(
            cases
                .iter()
                .map(|(kind, _)| controller_event_kind_slug(kind))
                .collect::<Vec<_>>(),
            cases.iter().map(|(_, slug)| *slug).collect::<Vec<_>>()
        );
    }

    #[test]
    fn lane_terminal_task_notifications_for_unannounced_children_do_not_create_runs() {
        let record = r#"{"type":"queue-operation","content":"<task-notification><task-id>1111111111111111</task-id><status>completed</status></task-notification><task-notification><task-id>2222222222222222</task-id><status>failed</status></task-notification>"}"#;
        let facts = extract_claude_line_at(&SessionScope::ClaudeRoot(PARENT.to_owned()), record, 1)
            .into_iter()
            .map(|fact| (7, fact));
        let mut synthesis = Synthesis::default();
        let mut events = synthesize(&mut synthesis, "queue.jsonl", facts);
        events.extend(synthesis.advance_lifecycle(1 + DEFAULT_COMPLETE_GRACE_MS));
        let synthesized = synthesized_events(&events);
        let event_ids = synthesized
            .iter()
            .map(|event| event.metadata.event_id.as_str())
            .collect::<HashSet<_>>();
        let task_run_ids = synthesized
            .iter()
            .map(|event| event.task_run_id.as_str())
            .collect::<HashSet<_>>();

        assert_eq!(synthesized.len(), 2);
        assert_eq!(task_run_ids.len(), 2);
        assert_eq!(
            event_ids,
            HashSet::from([
                "log:queue.jsonl:7:complete:1111111111111111",
                "log:queue.jsonl:7:failed:2222222222222222",
            ])
        );
        let model = apply_once_per_event_id(&events);
        assert_eq!(
            model.task_runs().count(),
            0,
            "unannounced task notifications must not mint Task Runs"
        );
        for agent_id in ["1111111111111111", "2222222222222222"] {
            let expected_key = format!("hook:claude-code:{PARENT}:agent:{agent_id}");
            assert!(
                model
                    .task_run_by_key(&RunKey::Controller(expected_key.clone()))
                    .is_none(),
                "notification-only child key {expected_key} must remain absent"
            );
        }
    }

    #[test]
    fn append_start_applies_while_unannounced_subagent_end_is_dropped() {
        let mut synthesis = Synthesis::default();
        let mut events = synthesize(
            &mut synthesis,
            "session.jsonl",
            [
                (
                    11,
                    LogFact::Append {
                        scope: SessionScope::ClaudeRoot(PARENT.to_owned()),
                        at_ms: 100,
                    },
                ),
                (
                    11,
                    LogFact::SubagentEnded {
                        parent: PARENT.to_owned(),
                        agent_id: "child".to_owned(),
                        failed: false,
                        at_ms: 100,
                    },
                ),
            ],
        );
        events.extend(synthesis.advance_lifecycle(100 + DEFAULT_COMPLETE_GRACE_MS));
        let synthesized = synthesized_events(&events);
        let event_ids = synthesized
            .iter()
            .map(|event| event.metadata.event_id.as_str())
            .collect::<HashSet<_>>();

        assert_eq!(synthesized.len(), 2);
        assert_eq!(
            event_ids,
            HashSet::from([
                "log:session.jsonl:11:task-started:11111111-1111-4111-8111-111111111111",
                "log:session.jsonl:11:complete:child",
            ])
        );
        let model = apply_once_per_event_id(&events);
        assert_eq!(
            model.task_runs().count(),
            1,
            "the root append may create only the root run"
        );
        assert!(
            model
                .task_run_by_key(&RunKey::Controller(format!("hook:claude-code:{PARENT}")))
                .is_some(),
            "the append-derived root TaskStarted must still create its run"
        );
        let child_key = format!("hook:claude-code:{PARENT}:agent:child");
        assert!(
            model
                .task_run_by_key(&RunKey::Controller(child_key.clone()))
                .is_none(),
            "the same-record terminal-only child key {child_key} must remain absent"
        );
    }

    #[test]
    fn announced_subagent_notification_completes_run_created_by_lane_dispatch() {
        const AGENT: &str = "a1b2c3d4e5f60718";
        let child_key = format!("hook:claude-code:{PARENT}:agent:{AGENT}");
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 600);
        let creators = synthesize(
            &mut synthesis,
            "agent-announced-child.meta.json",
            [(
                0,
                LogFact::SubagentAppeared {
                    parent: PARENT.to_owned(),
                    agent_id: AGENT.to_owned(),
                    agent_type: "reviewer".to_owned(),
                    description: "Review the reducer gate".to_owned(),
                    at_ms: 1,
                },
            )],
        );
        let creator_events = synthesized_events(&creators);
        let dispatch = creator_events
            .iter()
            .find(|event| matches!(event.event, ControllerEventKind::Dispatch { .. }))
            .expect("subagent metadata must synthesize Dispatch");
        let started = creator_events
            .iter()
            .find(|event| matches!(event.event, ControllerEventKind::TaskStarted))
            .expect("subagent metadata must synthesize TaskStarted");
        let (reducer, _) = Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });

        let dispatched = reducer
            .validate_controller_event(dispatch)
            .expect("lane Dispatch must create the announced child");
        let child = dispatched
            .post_model
            .task_run_by_key(&RunKey::Controller(child_key.clone()))
            .expect("Dispatch must create the exact child Controller key");
        assert_eq!(child.state, TaskState::Queued);
        let (reducer, _) = Reducer::new(RestoredState {
            model: dispatched.post_model,
            next_ordinal: dispatched.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let running = reducer
            .validate_controller_event(started)
            .expect("lane TaskStarted must start the announced child");
        assert_eq!(
            running
                .post_model
                .task_run_by_key(&RunKey::Controller(child_key.clone()))
                .expect("TaskStarted must preserve the child run")
                .state,
            TaskState::Running
        );

        let notification = format!(
            r#"{{"type":"queue-operation","content":"<task-notification><task-id>{AGENT}</task-id><status>completed</status></task-notification>"}}"#
        );
        let held = synthesize(
            &mut synthesis,
            "parent.jsonl",
            extract_claude_line_at(
                &SessionScope::ClaudeRoot(PARENT.to_owned()),
                &notification,
                1,
            )
            .into_iter()
            .map(|fact| (7, fact)),
        );
        assert!(
            synthesized_events(&held).is_empty(),
            "the completion must remain grace-held before the flush"
        );
        let flushed = synthesis.advance_lifecycle(31);
        let complete = synthesized_events(&flushed)
            .into_iter()
            .find(|event| matches!(event.event, ControllerEventKind::Complete))
            .expect("the due-grace flush must release the completion");
        let (reducer, _) = Reducer::new(RestoredState {
            model: running.post_model,
            next_ordinal: running.post_next_ordinal,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let completed = reducer
            .validate_controller_event(complete)
            .expect("the terminal must apply to the run created by Dispatch");

        assert_eq!(
            completed
                .post_model
                .task_run_by_key(&RunKey::Controller(child_key))
                .expect("the completed child must retain its Controller key")
                .state,
            TaskState::Completed
        );
    }

    #[test]
    fn semantic_event_ids_do_not_depend_on_fact_order() {
        let facts = [
            LogFact::SubagentEnded {
                parent: PARENT.to_owned(),
                agent_id: "first".to_owned(),
                failed: false,
                at_ms: 1,
            },
            LogFact::SubagentEnded {
                parent: PARENT.to_owned(),
                agent_id: "second".to_owned(),
                failed: true,
                at_ms: 1,
            },
        ];
        let ids = |facts: Vec<LogFact>| {
            let mut synthesis = Synthesis::default();
            let mut events = synthesize(
                &mut synthesis,
                "queue.jsonl",
                facts.into_iter().map(|fact| (3, fact)),
            );
            events.extend(synthesis.advance_lifecycle(1 + DEFAULT_COMPLETE_GRACE_MS));
            synthesized_events(&events)
                .into_iter()
                .map(|event| (event.task_run_id.clone(), event.metadata.event_id.clone()))
                .collect::<HashMap<_, _>>()
        };

        assert_eq!(ids(facts.to_vec()), ids(facts.into_iter().rev().collect()));
    }

    #[test]
    fn repeated_target_and_kind_in_one_record_is_one_semantic_event() {
        let duplicate = LogFact::SubagentEnded {
            parent: PARENT.to_owned(),
            agent_id: "child".to_owned(),
            failed: false,
            at_ms: 1,
        };
        let mut synthesis = Synthesis::default();
        let mut events = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(4, duplicate.clone()), (4, duplicate)],
        );
        events.extend(synthesis.advance_lifecycle(1 + DEFAULT_COMPLETE_GRACE_MS));

        assert!(matches!(
            synthesized_events(&events).as_slice(),
            [event]
                if event.metadata.event_id == "log:queue.jsonl:4:complete:child"
                    && matches!(event.event, ControllerEventKind::Complete)
        ));
    }

    #[test]
    fn event_ids_never_prov_prefixed() {
        let events = synthesize(
            &mut Synthesis::default(),
            "agent-child.meta.json",
            [(
                0,
                LogFact::SubagentAppeared {
                    parent: PARENT.to_owned(),
                    agent_id: "child".to_owned(),
                    agent_type: "reviewer".to_owned(),
                    description: "Review the lane".to_owned(),
                    at_ms: 1,
                },
            )],
        );

        assert!(
            synthesized_events(&events)
                .iter()
                .all(|event| !event.metadata.event_id.starts_with("prov:"))
        );
    }

    #[test]
    fn dispatch_and_started_carry_subject_and_kind() {
        let events = synthesize(
            &mut Synthesis::default(),
            "agent-child.meta.json",
            [(
                0,
                LogFact::SubagentAppeared {
                    parent: PARENT.to_owned(),
                    agent_id: "child".to_owned(),
                    agent_type: "reviewer".to_owned(),
                    description: "Review deterministic synthesis".to_owned(),
                    at_ms: 1,
                },
            )],
        );
        let synthesized = synthesized_events(&events);

        assert_eq!(synthesized.len(), 2);
        assert!(matches!(
            synthesized[0].event,
            ControllerEventKind::Dispatch { .. }
        ));
        assert!(matches!(
            synthesized[1].event,
            ControllerEventKind::TaskStarted
        ));
        assert_eq!(
            synthesized[0].metadata.label.as_deref(),
            Some("Review deterministic synthesis")
        );
        assert_eq!(
            synthesized[1].metadata.label.as_deref(),
            Some("Review deterministic synthesis")
        );
        assert_eq!(
            synthesized[0]
                .metadata
                .provider_metadata
                .as_ref()
                .and_then(|metadata| metadata.event_kind.as_deref()),
            Some("reviewer")
        );
        assert_eq!(
            synthesized[1]
                .metadata
                .provider_metadata
                .as_ref()
                .and_then(|metadata| metadata.event_kind.as_deref()),
            Some("reviewer")
        );
    }

    #[test]
    fn codex_scope_claims_native_run_key() {
        assert_eq!(
            run_key_for_scope(&SessionScope::Codex {
                rollout_id: ROLLOUT.to_owned(),
            }),
            RunKey::Native {
                provider: Provider::Codex,
                sid: ROLLOUT.to_owned(),
            }
        );
    }

    #[test]
    fn codex_meta_without_append_uses_non_epoch_lifecycle_time() {
        let mut synthesis = Synthesis::with_lifecycle_timing_at(30, 600, 4_321);
        let events = synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [(
                0,
                LogFact::CodexMeta {
                    rollout_id: ROLLOUT.to_owned(),
                    cwd: "/workspace".to_owned(),
                    originator: "codex".to_owned(),
                    internal: None,
                    cli_version: "0.149.0".to_owned(),
                },
            )],
        );

        assert!(matches!(
            synthesized_events(&events).as_slice(),
            [event]
                if matches!(event.event, ControllerEventKind::TaskStarted)
                    && event.metadata.timestamp_ms == 4_321
        ));
        let model = apply_once_per_event_id(&events);
        let run = model
            .task_run_by_key(&RunKey::Native {
                provider: Provider::Codex,
                sid: ROLLOUT.to_owned(),
            })
            .expect("timestamp-less session_meta must still mint the Codex root run");
        assert_eq!(run.created_at_ms, Some(4_321));
        assert_eq!(run.updated_at_ms, Some(4_321));
    }

    #[test]
    fn failed_notification_yields_failed_state() {
        let events = synthesize(
            &mut Synthesis::default(),
            "queue.jsonl",
            [(
                4,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "child".to_owned(),
                    failed: true,
                    at_ms: 1,
                },
            )],
        );

        assert!(matches!(
            synthesized_events(&events).as_slice(),
            [event] if matches!(event.event, ControllerEventKind::Failed)
        ));
    }

    #[test]
    fn hook_and_lane_same_fixture_yield_single_run() {
        let hook = map_hook_payload(
            HookProvider::ClaudeCode,
            &hook_payload("SessionStart", PARENT, None),
            100,
            9,
        );
        let lane = synthesize(
            &mut Synthesis::default(),
            &format!("{PARENT}.jsonl"),
            [(
                0,
                LogFact::Append {
                    scope: SessionScope::ClaudeRoot(PARENT.to_owned()),
                    at_ms: 100,
                },
            )],
        );
        let reducer = Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        })
        .0;
        let reducer = advance_reducer(reducer, &controller_event_from_hook(&hook[0]));
        let reducer = advance_reducer(reducer, synthesized_events(&lane)[0]);
        let lane_key = run_key_for_scope(&SessionScope::ClaudeRoot(PARENT.to_owned()));

        assert!(
            reducer
                .resolve_controller_run(match &lane_key {
                    RunKey::Controller(key) => key,
                    _ => panic!("Claude key must be Controller identity"),
                })
                .is_some()
        );
        let delta = reducer
            .validate_controller_event(synthesized_events(&lane)[0])
            .expect("same lane identity remains a no-conflict no-op");
        assert_eq!(delta.post_model.task_runs().count(), 1);
    }

    #[test]
    fn codex_hook_and_lane_starts_converge_in_both_arrival_orders() {
        let hook = map_hook_payload(
            HookProvider::Codex,
            &hook_payload("SessionStart", ROLLOUT, None),
            100,
            9,
        );
        let hook_started = controller_event_from_hook(&hook[0]);
        let lane = synthesize(
            &mut Synthesis::default(),
            "rollout.jsonl",
            [(
                0,
                LogFact::CodexMeta {
                    rollout_id: ROLLOUT.to_owned(),
                    cwd: "/workspace".to_owned(),
                    originator: "codex".to_owned(),
                    internal: None,
                    cli_version: "0.149.0".to_owned(),
                },
            )],
        );
        let lane_started = synthesized_events(&lane)
            .into_iter()
            .find(|event| matches!(event.event, ControllerEventKind::TaskStarted))
            .expect("Codex metadata must synthesize TaskStarted")
            .clone();

        for (first, second) in [
            (&hook_started, &lane_started),
            (&lane_started, &hook_started),
        ] {
            let reducer = Reducer::new(RestoredState {
                model: DomainModel::default(),
                next_ordinal: 1,
                next_ingest_seq: Some(1),
                event_ledger: Vec::new(),
            })
            .0;
            let reducer = advance_reducer(reducer, first);
            let reducer = advance_reducer(reducer, second);
            let model = reducer
                .validate_controller_event(second)
                .expect("replayed Codex start must not conflict or be rejected")
                .post_model;
            let controller_run = model
                .task_run_by_key(&RunKey::Controller(hook_started.task_run_id.clone()))
                .expect("the canonical Codex Controller key must resolve");
            let native_run = model
                .task_run_by_key(&RunKey::Native {
                    provider: Provider::Codex,
                    sid: ROLLOUT.to_owned(),
                })
                .expect("the native Codex SID must resolve to the converged run");

            assert_eq!(model.task_runs().count(), 1);
            assert_eq!(native_run.run_id, controller_run.run_id);
            assert_eq!(model.controller_diagnostics().binding_conflicts(), 0);
            assert_eq!(
                model
                    .controller_diagnostics()
                    .provider_identity_disagreements(),
                0
            );
        }

        assert_eq!(hook_started.task_run_id, lane_started.task_run_id);
    }

    #[test]
    fn hook_and_lane_subagent_pair_yields_one_run_and_one_agent_node() {
        const AGENT: &str = "child-7";
        let hook = map_hook_payload(
            HookProvider::ClaudeCode,
            &hook_payload("SubagentStart", PARENT, Some(AGENT)),
            100,
            9,
        );
        let lane = synthesize(
            &mut Synthesis::default(),
            "agent-child-7.meta.json",
            [(
                0,
                LogFact::SubagentAppeared {
                    parent: PARENT.to_owned(),
                    agent_id: AGENT.to_owned(),
                    agent_type: "reviewer".to_owned(),
                    description: "Review identity convergence".to_owned(),
                    at_ms: 100,
                },
            )],
        );
        let mut reducer = Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        })
        .0;
        let hook_started = hook
            .iter()
            .find(|event| event.event_type == "task_started")
            .map(controller_event_from_hook)
            .expect("hook must emit a subagent start");
        let lane_started = synthesized_events(&lane)
            .into_iter()
            .find(|event| matches!(event.event, ControllerEventKind::TaskStarted))
            .expect("lane must synthesize a subagent start");
        reducer = advance_reducer(reducer, &hook_started);
        reducer = advance_reducer(reducer, lane_started);

        let agent_key = match run_key_for_scope(&SessionScope::ClaudeSubagent {
            parent: PARENT.to_owned(),
            agent_id: AGENT.to_owned(),
        }) {
            RunKey::Controller(key) => key,
            _ => panic!("Claude subagent key must be Controller identity"),
        };
        let agent_run_id = reducer
            .resolve_controller_run(&agent_key)
            .expect("hook and lane must converge on one subagent run");
        let post_model = reducer
            .validate_controller_event(lane_started)
            .expect("replayed lane start remains a no-conflict no-op")
            .post_model;
        assert_eq!(
            post_model.task_runs().count(),
            1,
            "the session and agent pair must identify exactly one run"
        );
        assert_eq!(
            post_model
                .task_runs()
                .filter(|run| run.key == RunKey::Controller(agent_key.clone()))
                .count(),
            1
        );

        let (mut reducer, shared) = Reducer::new(RestoredState {
            model: post_model,
            next_ordinal: 2,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let mut metadata = lane_started.metadata.clone();
        metadata.event_id = "prov:claude:node:child-7".to_owned();
        metadata.source = "provider".to_owned();
        metadata.source_event_type = "agent_node".to_owned();
        metadata.task_run_id = Some(agent_run_id);
        reducer
            .apply(NormalizedEvent::AgentNodeUpsert {
                metadata,
                node: AgentNodeObservation {
                    agent_node_id: "agent:claude:child-7".to_owned(),
                    provider: Provider::Claude,
                    native_session_id: Some(AGENT.to_owned()),
                    task_run_id: agent_run_id,
                    parent_agent_node_id: None,
                    state: None,
                    model_id: None,
                    session_file: Some("agent-child-7.jsonl".to_owned()),
                },
            })
            .expect("subagent node should apply through the reducer");

        let model = shared.borrow();
        assert_eq!(model.agent_nodes().count(), 1);
        assert_eq!(
            model.agent_nodes().next().unwrap().task_run_id,
            agent_run_id
        );
        assert_eq!(model.controller_diagnostics().binding_conflicts(), 0);
        assert_eq!(
            model
                .controller_diagnostics()
                .provider_identity_disagreements(),
            0
        );
    }

    #[test]
    fn first_session_meta_wins_in_file_order() {
        let mut synthesis = Synthesis::default();
        let facts = [
            (
                0,
                LogFact::CodexMeta {
                    rollout_id: ROLLOUT.to_owned(),
                    cwd: "/first".to_owned(),
                    originator: "first".to_owned(),
                    internal: None,
                    cli_version: "0.149.0".to_owned(),
                },
            ),
            (
                1,
                LogFact::CodexMeta {
                    rollout_id: ROLLOUT.to_owned(),
                    cwd: "/later".to_owned(),
                    originator: "later".to_owned(),
                    internal: None,
                    cli_version: "9.9.9".to_owned(),
                },
            ),
        ];

        synthesize(&mut synthesis, "rollout.jsonl", facts);

        let meta = synthesis
            .session_meta
            .get(Path::new("rollout.jsonl"))
            .unwrap();
        assert_eq!(meta.cwd, "/first");
        assert_eq!(meta.originator, "first");
        assert_eq!(meta.cli_version, "0.149.0");

        synthesize(
            &mut synthesis,
            "copied-rollout.jsonl",
            [(
                0,
                LogFact::CodexMeta {
                    rollout_id: ROLLOUT.to_owned(),
                    cwd: "/other-artifact".to_owned(),
                    originator: "other".to_owned(),
                    internal: None,
                    cli_version: "0.150.0".to_owned(),
                },
            )],
        );
        assert_eq!(
            synthesis
                .session_meta
                .get(Path::new("copied-rollout.jsonl"))
                .unwrap()
                .cwd,
            "/other-artifact"
        );
    }

    #[test]
    fn usage_dedup_is_scoped_and_claude_fixture_uses_production_path() {
        let fixture = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/provider-logs/claude-session.jsonl"),
        )
        .unwrap();
        assert!(fixture.matches("msg_02SyntheticStreamChunk").count() > 1);
        let root = SessionScope::ClaudeRoot(PARENT.to_owned());
        let mut facts = fixture
            .lines()
            .enumerate()
            .flat_map(|(ordinal, line)| {
                extract_claude_line(&root, line)
                    .into_iter()
                    .map(move |fact| (ordinal as u64, fact))
            })
            .collect::<Vec<_>>();
        facts.push((
            99,
            LogFact::Usage {
                scope: SessionScope::ClaudeRoot(STRANGER.to_owned()),
                at_ms: 200,
                sample_id: "msg_02SyntheticStreamChunk".to_owned(),
                output_tokens: 11,
                token_breakdown: crate::model::TokenBreakdown::default(),
                model: None,
                effort: None,
            },
        ));

        let events = synthesize(&mut Synthesis::default(), "session.jsonl", facts);
        let samples = events
            .iter()
            .filter_map(|event| match event {
                ProviderEvent::Telemetry {
                    key, output_tokens, ..
                } => Some((key, output_tokens)),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            samples
                .iter()
                .filter(|(key, _)| **key == run_key_for_scope(&root))
                .count(),
            3,
            "the repeated Claude message.id must be first-wins in synthesis"
        );
        assert!(samples.iter().any(|(key, tokens)| {
            **key == run_key_for_scope(&SessionScope::ClaudeRoot(STRANGER.to_owned()))
                && **tokens == 11
        }));
    }

    #[test]
    fn codex_turn_context_fills_telemetry_model_and_effort() {
        let events = synthesize(
            &mut Synthesis::default(),
            "rollout.jsonl",
            [
                (
                    1,
                    LogFact::CodexTurn {
                        rollout_id: ROLLOUT.to_owned(),
                        turn_id: "turn-1".to_owned(),
                        model: "gpt-5.6-sol".to_owned(),
                        effort: Some("xhigh".to_owned()),
                        sandbox: Some("workspace-write".to_owned()),
                    },
                ),
                (
                    2,
                    LogFact::Usage {
                        scope: SessionScope::Codex {
                            rollout_id: ROLLOUT.to_owned(),
                        },
                        at_ms: 20,
                        sample_id: "turn-1:2".to_owned(),
                        output_tokens: 42,
                        token_breakdown: crate::model::TokenBreakdown::default(),
                        model: None,
                        effort: None,
                    },
                ),
            ],
        );

        assert!(matches!(
            events.as_slice(),
            [ProviderEvent::Telemetry {
                model: Some(model),
                effort: Some(effort),
                sandbox: Some(sandbox),
                ..
            }] if model == "gpt-5.6-sol" && effort == "xhigh" && sandbox == "workspace-write"
        ));
    }

    #[test]
    fn duplicate_subagent_ended_merges_failure_within_one_synthesis_batch() {
        // Task 6 owns persisted-state supersession by grace-deferring lane Completes.
        for failed_first in [false, true] {
            let statuses = if failed_first {
                [true, false]
            } else {
                [false, true]
            };
            let events = synthesize(
                &mut Synthesis::default(),
                "queue.jsonl",
                statuses.into_iter().enumerate().map(|(ordinal, failed)| {
                    (
                        ordinal as u64,
                        LogFact::SubagentEnded {
                            parent: PARENT.to_owned(),
                            agent_id: "child".to_owned(),
                            failed,
                            at_ms: ordinal as i64 + 1,
                        },
                    )
                }),
            );
            let terminal = synthesized_events(&events);

            assert!(matches!(
                terminal.as_slice(),
                [event] if matches!(event.event, ControllerEventKind::Failed)
            ));
        }
    }

    #[test]
    fn failed_subagent_end_suppresses_ok_downgrade_in_later_batch() {
        let mut synthesis = Synthesis::default();
        let failed = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                4,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "child".to_owned(),
                    failed: true,
                    at_ms: 100,
                },
            )],
        );
        let downgrade = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                5,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "child".to_owned(),
                    failed: false,
                    at_ms: 110,
                },
            )],
        );

        assert!(matches!(
            synthesized_events(&failed).as_slice(),
            [event] if matches!(event.event, ControllerEventKind::Failed)
        ));
        assert_eq!(synthesized_events(&downgrade).len(), 0);
    }

    #[test]
    fn complete_lands_after_grace_only() {
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 600);
        let held = synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [(
                4,
                LogFact::CodexTurnComplete {
                    rollout_id: ROLLOUT.to_owned(),
                    at_ms: 100,
                },
            )],
        );

        assert!(synthesized_events(&held).is_empty());
        assert!(synthesis.advance_lifecycle(129).is_empty());
        assert!(matches!(
            synthesized_events(&synthesis.advance_lifecycle(130)).as_slice(),
            [event] if matches!(event.event, ControllerEventKind::Complete)
        ));
        let reopened = synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [(
                5,
                LogFact::Append {
                    scope: SessionScope::Codex {
                        rollout_id: ROLLOUT.to_owned(),
                    },
                    at_ms: 140,
                },
            )],
        );
        assert!(
            synthesized_events(&reopened)
                .iter()
                .any(|event| matches!(event.event, ControllerEventKind::TaskStarted))
        );
    }

    #[test]
    fn lane_terminal_grace_flush_for_unannounced_child_does_not_create_run() {
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 600);
        assert!(synthesis.advance_lifecycle(100).is_empty());

        let held = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                4,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "child".to_owned(),
                    failed: false,
                    at_ms: 100,
                },
            )],
        );

        assert!(synthesized_events(&held).is_empty());
        assert!(synthesis.advance_lifecycle(129).is_empty());
        let flushed = synthesis.advance_lifecycle(130);
        assert!(matches!(
            synthesized_events(&flushed).as_slice(),
            [event]
                if matches!(event.event, ControllerEventKind::Complete)
                    && event.metadata.timestamp_ms == 100
        ));
        let model = apply_once_per_event_id(&flushed);
        let child_key = format!("hook:claude-code:{PARENT}:agent:child");
        assert_eq!(
            model.task_runs().count(),
            0,
            "a due-grace completion for an unknown child must not recreate a run"
        );
        assert!(
            model
                .task_run_by_key(&RunKey::Controller(child_key.clone()))
                .is_none(),
            "the due-grace child key {child_key} must remain absent"
        );
    }

    #[test]
    fn lane_terminal_shutdown_flush_for_unannounced_child_does_not_create_run() {
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 600);
        let held = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                4,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "shutdown-child".to_owned(),
                    failed: false,
                    at_ms: 100,
                },
            )],
        );
        assert!(
            synthesized_events(&held).is_empty(),
            "the completion must be held before shutdown flush"
        );

        let flushed = synthesis.flush_pending_completes();
        assert!(matches!(
            synthesized_events(&flushed).as_slice(),
            [event] if matches!(event.event, ControllerEventKind::Complete)
        ));
        let model = apply_once_per_event_id(&flushed);
        let child_key = format!("hook:claude-code:{PARENT}:agent:shutdown-child");
        assert_eq!(
            model.task_runs().count(),
            0,
            "a shutdown-flushed completion for an unknown child must not create a run"
        );
        assert!(
            model
                .task_run_by_key(&RunKey::Controller(child_key.clone()))
                .is_none(),
            "the shutdown-flushed child key {child_key} must remain absent"
        );
    }

    #[test]
    fn resume_within_grace_never_flaps() {
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 600);
        let mut events = synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [(
                1,
                LogFact::CodexMeta {
                    rollout_id: ROLLOUT.to_owned(),
                    cwd: "/workspace".to_owned(),
                    originator: "codex".to_owned(),
                    internal: None,
                    cli_version: "0.149.0".to_owned(),
                },
            )],
        );
        events.extend(synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [(
                4,
                LogFact::CodexTurnComplete {
                    rollout_id: ROLLOUT.to_owned(),
                    at_ms: 100,
                },
            )],
        ));
        events.extend(synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [(
                5,
                LogFact::CodexTurnStarted {
                    rollout_id: ROLLOUT.to_owned(),
                    at_ms: 120,
                },
            )],
        ));
        events.extend(synthesis.advance_lifecycle(200));

        assert!(
            synthesized_events(&events)
                .iter()
                .all(|event| !matches!(event.event, ControllerEventKind::Complete))
        );
        assert_eq!(
            apply_once_per_event_id(&events)
                .task_run_by_key(&RunKey::Native {
                    provider: Provider::Codex,
                    sid: ROLLOUT.to_owned(),
                })
                .unwrap()
                .state,
            crate::model::TaskState::Running
        );
    }

    #[test]
    fn failed_subagent_end_in_later_batch_supersedes_grace_held_complete() {
        let child_scope = SessionScope::ClaudeSubagent {
            parent: PARENT.to_owned(),
            agent_id: "child".to_owned(),
        };
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 600);
        let mut events = synthesize(
            &mut synthesis,
            "agent-child.meta.json",
            [(
                0,
                LogFact::SubagentAppeared {
                    parent: PARENT.to_owned(),
                    agent_id: "child".to_owned(),
                    agent_type: "reviewer".to_owned(),
                    description: "Review lifecycle".to_owned(),
                    at_ms: 100,
                },
            )],
        );
        events.extend(synthesize(
            &mut synthesis,
            "queue.jsonl",
            [
                (
                    3,
                    LogFact::Append {
                        scope: SessionScope::ClaudeRoot(PARENT.to_owned()),
                        at_ms: 100,
                    },
                ),
                (
                    3,
                    LogFact::SubagentEnded {
                        parent: PARENT.to_owned(),
                        agent_id: "child".to_owned(),
                        failed: false,
                        at_ms: 100,
                    },
                ),
            ],
        ));
        assert!(
            synthesized_events(&events)
                .iter()
                .all(|event| !matches!(event.event, ControllerEventKind::Complete))
        );
        let failed = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [
                (
                    4,
                    LogFact::Append {
                        scope: SessionScope::ClaudeRoot(PARENT.to_owned()),
                        at_ms: 120,
                    },
                ),
                (
                    4,
                    LogFact::SubagentEnded {
                        parent: PARENT.to_owned(),
                        agent_id: "child".to_owned(),
                        failed: true,
                        at_ms: 120,
                    },
                ),
            ],
        );
        assert!(matches!(
            synthesized_events(&failed)
                .into_iter()
                .find(|event| event.task_run_id == controller_key_for_scope(&child_scope))
                .map(|event| &event.event),
            Some(ControllerEventKind::Failed)
        ));
        events.extend(failed);

        assert_eq!(
            apply_once_per_event_id(&events)
                .task_run_by_key(&run_key_for_scope(&child_scope))
                .unwrap()
                .state,
            crate::model::TaskState::Failed
        );
    }

    #[test]
    fn inactivity_timer_closes_once_at_the_boundary() {
        let scope = SessionScope::ClaudeRoot(PARENT.to_owned());
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 50);
        let events = synthesize(
            &mut synthesis,
            "session.jsonl",
            [(
                1,
                LogFact::Append {
                    scope: scope.clone(),
                    at_ms: 100,
                },
            )],
        );
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::Synthesized(event)
                if matches!(event.event, ControllerEventKind::TaskStarted)
        )));

        assert!(synthesis.advance_lifecycle(149).is_empty());
        assert!(matches!(
            synthesis.advance_lifecycle(150).as_slice(),
            [ProviderEvent::LaneClose { key, at_ms }]
                if key == &run_key_for_scope(&scope) && *at_ms == 100
        ));
        assert!(synthesis.advance_lifecycle(200).is_empty());
    }

    #[test]
    fn appendless_started_scope_closes_once_at_start_time_boundary() {
        let mut synthesis = Synthesis::with_lifecycle_timing_at(30, 50, 1);
        let (scope, started) =
            synthesize_subagent_start(&mut synthesis, "appendless-child", Some(100));
        let child_events = synthesized_events(&started)
            .into_iter()
            .filter(|event| event.task_run_id == controller_key_for_scope(&scope))
            .collect::<Vec<_>>();
        assert!(matches!(
            child_events.as_slice(),
            [dispatch, task_started]
                if matches!(dispatch.event, ControllerEventKind::Dispatch { .. })
                    && matches!(task_started.event, ControllerEventKind::TaskStarted)
                    && dispatch.metadata.timestamp_ms == 100
                    && task_started.metadata.timestamp_ms == 100
        ));

        assert!(
            synthesis.advance_lifecycle(149).is_empty(),
            "an append-less scope must remain active before its start-time threshold"
        );
        assert_eq!(
            synthesis.advance_lifecycle(150),
            vec![ProviderEvent::LaneClose {
                key: run_key_for_scope(&scope),
                at_ms: 100,
            }],
            "an append-less started scope must close at its start-time threshold"
        );
        assert!(
            synthesis.advance_lifecycle(200).is_empty(),
            "an inactivity-closed scope must not close again"
        );
    }

    #[test]
    fn appendless_start_without_source_time_uses_injected_lifecycle_clock() {
        let mut synthesis = Synthesis::with_lifecycle_timing_at(30, 50, 100);
        let (scope, _) = synthesize_subagent_start(&mut synthesis, "clock-anchored-child", None);

        assert!(
            synthesis.advance_lifecycle(100).is_empty(),
            "a fresh start without a source timestamp must not close immediately"
        );
        assert!(synthesis.advance_lifecycle(149).is_empty());
        assert_eq!(
            synthesis.advance_lifecycle(150),
            vec![ProviderEvent::LaneClose {
                key: run_key_for_scope(&scope),
                at_ms: 100,
            }]
        );
    }

    #[test]
    fn append_bearing_scope_closes_relative_to_last_append() {
        let mut synthesis = Synthesis::with_lifecycle_timing_at(30, 50, 1);
        let (scope, _) = synthesize_subagent_start(&mut synthesis, "active-child", Some(100));
        let _ = synthesize(
            &mut synthesis,
            "agent-active-child.jsonl",
            [(
                2,
                LogFact::Append {
                    scope: scope.clone(),
                    at_ms: 140,
                },
            )],
        );

        assert!(
            synthesis.advance_lifecycle(150).is_empty(),
            "the start-time threshold must not close a scope with a newer append"
        );
        assert_eq!(
            synthesis.advance_lifecycle(190),
            vec![ProviderEvent::LaneClose {
                key: run_key_for_scope(&scope),
                at_ms: 140,
            }]
        );
    }

    #[test]
    fn appendless_pending_complete_is_never_inactivity_closed() {
        let mut synthesis = Synthesis::with_lifecycle_timing_at(100, 50, 1);
        let (scope, _) = synthesize_subagent_start(&mut synthesis, "completing-child", Some(100));
        let held = synthesize(
            &mut synthesis,
            "queue.jsonl",
            [(
                2,
                LogFact::SubagentEnded {
                    parent: PARENT.to_owned(),
                    agent_id: "completing-child".to_owned(),
                    failed: false,
                    at_ms: 100,
                },
            )],
        );
        assert!(synthesized_events(&held).is_empty());

        assert!(
            synthesis.advance_lifecycle(150).is_empty(),
            "a grace-held completion must suppress inactivity close"
        );
        assert!(matches!(
            synthesis.advance_lifecycle(200).as_slice(),
            [ProviderEvent::Synthesized(event)]
                if event.task_run_id == controller_key_for_scope(&scope)
                    && matches!(event.event, ControllerEventKind::Complete)
        ));
        assert!(
            synthesis.advance_lifecycle(250).is_empty(),
            "a completed scope must not be inactivity-closed later"
        );
    }

    #[test]
    fn append_after_inactivity_close_reopens_and_clears_close_state() {
        let scope = SessionScope::ClaudeRoot(PARENT.to_owned());
        let mut synthesis = Synthesis::with_lifecycle_timing(30, 50);
        let _ = synthesize(
            &mut synthesis,
            "session.jsonl",
            [(
                1,
                LogFact::Append {
                    scope: scope.clone(),
                    at_ms: 100,
                },
            )],
        );
        assert!(matches!(
            synthesis.advance_lifecycle(150).as_slice(),
            [ProviderEvent::LaneClose { .. }]
        ));
        let reopened = synthesize(
            &mut synthesis,
            "session.jsonl",
            [(
                2,
                LogFact::Append {
                    scope: scope.clone(),
                    at_ms: 160,
                },
            )],
        );
        assert!(
            synthesized_events(&reopened)
                .iter()
                .any(|event| matches!(event.event, ControllerEventKind::TaskStarted))
        );
        assert!(
            !synthesis
                .inactivity_closed
                .contains(&ScopeKey::from(&scope))
        );
    }

    #[test]
    fn bare_subagent_append_emits_liveness_only() {
        let scope = SessionScope::ClaudeSubagent {
            parent: PARENT.to_owned(),
            agent_id: "unannounced".to_owned(),
        };

        let events = synthesize(
            &mut Synthesis::default(),
            "agent-unannounced.jsonl",
            [(
                1,
                LogFact::Append {
                    scope: scope.clone(),
                    at_ms: 100,
                },
            )],
        );

        assert_eq!(
            events,
            vec![ProviderEvent::RunLiveness {
                key: run_key_for_scope(&scope),
                at_ms: 100,
            }]
        );
    }

    #[test]
    fn one_record_with_two_codex_evidence_children_applies_both_dispatches() {
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);
        let mut discovered = AdmissionIndex::new();
        discovered.insert_codex_rollout(
            ROLLOUT,
            PathBuf::from(format!("/sessions/rollout-{ROLLOUT}.jsonl")),
            0,
        );
        discovered.insert_codex_rollout(
            STRANGER,
            PathBuf::from(format!("/sessions/rollout-{STRANGER}.jsonl")),
            0,
        );
        let events = Synthesis::default().synthesize_batch(
            Path::new("parent.jsonl"),
            [ROLLOUT, STRANGER].into_iter().map(|id| {
                (
                    7,
                    LogFact::EvidenceId {
                        parent: SessionScope::ClaudeRoot(PARENT.to_owned()),
                        id: EvidenceId::Uuid(id.to_owned()),
                        at_ms: 7,
                    },
                )
            }),
            &mut admission,
            &discovered,
        );
        let synthesized = synthesized_events(&events);

        assert_eq!(synthesized.len(), 2);
        assert_eq!(
            synthesized
                .iter()
                .map(|event| event.metadata.event_id.as_str())
                .collect::<HashSet<_>>()
                .len(),
            2
        );
        let model = apply_once_per_event_id(&events);
        for rollout_id in [ROLLOUT, STRANGER] {
            assert!(
                model
                    .task_run_by_key(&RunKey::Native {
                        provider: Provider::Codex,
                        sid: rollout_id.to_owned(),
                    })
                    .is_some(),
                "missing reduced Codex child {rollout_id}"
            );
        }
    }

    #[test]
    fn one_record_with_two_claude_root_evidence_children_applies_both_dispatches() {
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);
        let mut discovered = AdmissionIndex::new();
        discovered.insert_claude_session(
            ROLLOUT,
            PathBuf::from(format!("/projects/workspace/{ROLLOUT}.jsonl")),
            0,
        );
        discovered.insert_claude_session(
            STRANGER,
            PathBuf::from(format!("/projects/workspace/{STRANGER}.jsonl")),
            0,
        );
        let events = Synthesis::default().synthesize_batch(
            Path::new("parent.jsonl"),
            [ROLLOUT, STRANGER].into_iter().map(|id| {
                (
                    7,
                    LogFact::EvidenceId {
                        parent: SessionScope::ClaudeRoot(PARENT.to_owned()),
                        id: EvidenceId::Uuid(id.to_owned()),
                        at_ms: 7,
                    },
                )
            }),
            &mut admission,
            &discovered,
        );
        let synthesized = synthesized_events(&events);

        assert_eq!(synthesized.len(), 2);
        assert_eq!(
            synthesized
                .iter()
                .map(|event| event.metadata.event_id.as_str())
                .collect::<HashSet<_>>()
                .len(),
            2
        );
        let model = apply_once_per_event_id(&events);
        for session_id in [ROLLOUT, STRANGER] {
            let scope = SessionScope::ClaudeRoot(session_id.to_owned());
            assert!(
                model
                    .task_run_by_key(&RunKey::Controller(controller_key_for_scope(&scope)))
                    .is_some(),
                "missing reduced Claude root child {session_id}"
            );
        }
    }

    #[test]
    fn unmatched_evidence_uuid_is_discarded_before_dispatch() {
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);
        let mut discovered = AdmissionIndex::new();
        discovered.insert_codex_rollout(
            ROLLOUT,
            PathBuf::from(format!("/sessions/rollout-{ROLLOUT}.jsonl")),
            0,
        );
        let mut synthesis = Synthesis::default();
        let unmatched = synthesis.synthesize_batch(
            Path::new("parent.jsonl"),
            [(
                1,
                LogFact::EvidenceId {
                    parent: SessionScope::ClaudeRoot(PARENT.to_owned()),
                    id: EvidenceId::Uuid(STRANGER.to_owned()),
                    at_ms: 1,
                },
            )],
            &mut admission,
            &discovered,
        );
        let matched = synthesis.synthesize_batch(
            Path::new("parent.jsonl"),
            [(
                2,
                LogFact::EvidenceId {
                    parent: SessionScope::ClaudeRoot(PARENT.to_owned()),
                    id: EvidenceId::Uuid(ROLLOUT.to_owned()),
                    at_ms: 2,
                },
            )],
            &mut admission,
            &discovered,
        );

        assert!(synthesized_events(&unmatched).is_empty());
        assert!(matches!(
            synthesized_events(&matched).as_slice(),
            [event] if matches!(event.event, ControllerEventKind::Dispatch { .. })
        ));
    }

    #[test]
    fn live_line_policy_is_turn_scoped_and_commentary_first() {
        let codex_scope = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let mut synthesis = Synthesis::default();

        synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [
                (
                    1,
                    LogFact::Activity {
                        scope: codex_scope.clone(),
                        at_ms: 1,
                        source: ActivitySource::Command,
                        line: "first command".to_owned(),
                    },
                ),
                (
                    2,
                    LogFact::Activity {
                        scope: codex_scope.clone(),
                        at_ms: 2,
                        source: ActivitySource::Commentary,
                        line: "first commentary".to_owned(),
                    },
                ),
                (
                    3,
                    LogFact::Activity {
                        scope: codex_scope.clone(),
                        at_ms: 3,
                        source: ActivitySource::Command,
                        line: "later command".to_owned(),
                    },
                ),
                (
                    4,
                    LogFact::Activity {
                        scope: codex_scope.clone(),
                        at_ms: 4,
                        source: ActivitySource::Commentary,
                        line: "latest commentary".to_owned(),
                    },
                ),
            ],
        );
        assert_eq!(
            synthesis.live_line(&codex_scope),
            Some("latest commentary".to_owned())
        );

        synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [(
                5,
                LogFact::CodexTurnStarted {
                    rollout_id: ROLLOUT.to_owned(),
                    at_ms: 5,
                },
            )],
        );
        assert_eq!(synthesis.live_line(&codex_scope), None);

        synthesize(
            &mut synthesis,
            "rollout.jsonl",
            [(
                6,
                LogFact::Activity {
                    scope: codex_scope.clone(),
                    at_ms: 6,
                    source: ActivitySource::Command,
                    line: "new turn command".to_owned(),
                },
            )],
        );
        assert_eq!(
            synthesis.live_line(&codex_scope),
            Some("new turn command".to_owned())
        );

        let claude_scope = SessionScope::ClaudeRoot(PARENT.to_owned());
        synthesize(
            &mut synthesis,
            "session.jsonl",
            [(
                7,
                LogFact::Activity {
                    scope: claude_scope.clone(),
                    at_ms: 7,
                    source: ActivitySource::ToolUse,
                    line: "first tool".to_owned(),
                },
            )],
        );
        synthesize(
            &mut synthesis,
            "session.jsonl",
            [(
                8,
                LogFact::Activity {
                    scope: claude_scope.clone(),
                    at_ms: 8,
                    source: ActivitySource::ToolUse,
                    line: "latest tool".to_owned(),
                },
            )],
        );
        assert_eq!(
            synthesis.live_line(&claude_scope),
            Some("latest tool".to_owned())
        );
    }

    #[test]
    fn live_line_event_ids_do_not_depend_on_batching() {
        let scope = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let facts = [
            (
                1,
                LogFact::Activity {
                    scope: scope.clone(),
                    at_ms: 1,
                    source: ActivitySource::Command,
                    line: "command".to_owned(),
                },
            ),
            (
                2,
                LogFact::Activity {
                    scope,
                    at_ms: 2,
                    source: ActivitySource::Commentary,
                    line: "commentary".to_owned(),
                },
            ),
        ];
        let batched = synthesize(&mut Synthesis::default(), "rollout.jsonl", facts.clone())
            .into_iter()
            .filter_map(|event| match event {
                ProviderEvent::Activity { event_id, .. } => Some(event_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut synthesis = Synthesis::default();
        let separate = facts
            .into_iter()
            .flat_map(|fact| synthesize(&mut synthesis, "rollout.jsonl", [fact]))
            .filter_map(|event| match event {
                ProviderEvent::Activity { event_id, .. } => Some(event_id),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(batched.len(), 2);
        assert_eq!(batched, separate);
    }

    #[test]
    fn live_line_event_ids_distinguish_activity_facts_at_the_same_ordinal() {
        let scope = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let event_ids = synthesize(
            &mut Synthesis::default(),
            "rollout.jsonl",
            [
                (
                    7,
                    LogFact::Activity {
                        scope: scope.clone(),
                        at_ms: 1,
                        source: ActivitySource::Command,
                        line: "command".to_owned(),
                    },
                ),
                (
                    7,
                    LogFact::Activity {
                        scope,
                        at_ms: 2,
                        source: ActivitySource::Commentary,
                        line: "commentary".to_owned(),
                    },
                ),
            ],
        )
        .into_iter()
        .filter_map(|event| match event {
            ProviderEvent::Activity { event_id, .. } => Some(event_id),
            _ => None,
        })
        .collect::<Vec<_>>();

        assert_ne!(event_ids[0], event_ids[1]);
        assert_eq!(
            event_ids,
            [
                format!("log:rollout.jsonl:7:activity:{ROLLOUT}:0"),
                format!("log:rollout.jsonl:7:activity:{ROLLOUT}:1"),
            ]
        );
    }

    #[test]
    fn live_line_events_use_reserved_source_positions() {
        let scope = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let events = synthesize(
            &mut Synthesis::default(),
            "rollout.jsonl",
            [
                (
                    7,
                    LogFact::Activity {
                        scope: scope.clone(),
                        at_ms: 1,
                        source: ActivitySource::Command,
                        line: "command".to_owned(),
                    },
                ),
                (
                    9,
                    LogFact::Activity {
                        scope,
                        at_ms: 2,
                        source: ActivitySource::Commentary,
                        line: "commentary".to_owned(),
                    },
                ),
            ],
        );
        let positions = events
            .into_iter()
            .filter_map(|event| match event {
                ProviderEvent::Activity { position, .. } => Some(position),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            positions,
            [
                SourcePosition {
                    path_id: u32::MAX,
                    generation: 0,
                    offset: 7,
                },
                SourcePosition {
                    path_id: u32::MAX,
                    generation: 0,
                    offset: 9,
                },
            ]
        );
    }

    #[test]
    fn open_admitted_regular_file_gates_strangers_and_tool_results() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("projects");
        let admitted = PathBuf::from(format!("workspace/{PARENT}.jsonl"));
        let stranger = PathBuf::from(format!("workspace/{STRANGER}.jsonl"));
        let tool_results = [
            PathBuf::from(format!("workspace/tool-results/{PARENT}.jsonl")),
            PathBuf::from(format!(
                "workspace/tool-results/{PARENT}/subagents/agent-child.jsonl"
            )),
        ];
        for relative in [&admitted, &stranger]
            .into_iter()
            .chain(tool_results.iter())
        {
            let path = root.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"{}\n").unwrap();
        }

        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);

        let admitted_diagnostics = ProviderDiagnostics::default();
        assert!(
            open_admitted_regular_file(&root, &admitted, &admission, &admitted_diagnostics)
                .unwrap()
                .is_some()
        );
        assert_eq!(admitted_diagnostics.admission_open_attempts(), 1);

        let stranger_diagnostics = ProviderDiagnostics::default();
        assert!(
            open_admitted_regular_file(&root, &stranger, &admission, &stranger_diagnostics)
                .unwrap()
                .is_none()
        );
        assert_eq!(stranger_diagnostics.admission_open_attempts(), 0);

        for tool_result in tool_results {
            let tool_diagnostics = ProviderDiagnostics::default();
            assert!(
                open_admitted_regular_file(&root, &tool_result, &admission, &tool_diagnostics)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(tool_diagnostics.admission_open_attempts(), 0);
        }
    }

    #[test]
    fn tool_results_contents_are_never_enumerated() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("projects");
        fs::create_dir_all(root.join("workspace/tool-results")).unwrap();
        fs::write(root.join("workspace/allowed.jsonl"), b"{}\n").unwrap();
        fs::write(
            root.join("workspace/tool-results/private-output.jsonl"),
            b"{}\n",
        )
        .unwrap();

        let discovery = crate::provider::discover_artifacts(&root, false).unwrap();

        assert_eq!(
            discovery
                .artifacts
                .into_iter()
                .map(|artifact| artifact.relative_path)
                .collect::<Vec<_>>(),
            [PathBuf::from("workspace/allowed.jsonl")]
        );
    }

    #[test]
    fn pane_artifact_admission_rejects_arbitrary_path() {
        let invalid = Path::new("/etc/shadow");
        let mut admission = Admission::new(0);

        assert!(!admission.admit_pane_artifact(Provider::Codex, invalid));

        assert!(!admission.is_admitted_path(invalid));
    }

    #[test]
    fn pane_artifact_admission_accepts_codex_rollout() {
        let valid = PathBuf::from(format!(
            "/home/user/.codex/sessions/2026/08/24/rollout-2026-08-24T00-00-00-{ROLLOUT}.jsonl"
        ));
        let mut admission = Admission::new(0);

        assert!(admission.admit_pane_artifact(Provider::Codex, &valid));

        assert!(admission.is_admitted_path(&valid));
    }

    #[test]
    fn pane_artifact_admission_accepts_claude_uuid_transcript() {
        let valid = PathBuf::from(format!(
            "/home/user/.claude/projects/workspace/{PARENT}.jsonl"
        ));
        let mut admission = Admission::new(0);

        assert!(admission.admit_pane_artifact(Provider::Claude, &valid));

        assert!(admission.is_admitted_path(&valid));
    }

    #[test]
    fn pane_artifact_admission_rejects_provider_mismatch() {
        let codex = PathBuf::from(format!("/sessions/rollout-example-{ROLLOUT}.jsonl"));
        let claude = PathBuf::from(format!("/projects/workspace/{PARENT}.jsonl"));
        let mut admission = Admission::new(0);

        assert!(!admission.admit_pane_artifact(Provider::Claude, &codex));
        assert!(!admission.admit_pane_artifact(Provider::Codex, &claude));

        assert!(!admission.is_admitted_path(&codex));
        assert!(!admission.is_admitted_path(&claude));
    }

    #[test]
    fn subagents_dir_admitted_via_parent() {
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);
        let directory = PathBuf::from(format!("projects/workspace/{PARENT}/subagents"));

        assert!(admission.is_admitted_path(&directory));
        assert!(admission.is_admitted_path(&directory.join("agent-child.jsonl")));
        assert!(admission.is_admitted_path(&directory.join("agent-child.meta.json")));
    }

    #[test]
    fn evidence_uuid_admits_matching_rollout_only() {
        let rollout_path = PathBuf::from(format!(
            "/home/user/.codex/sessions/2026/08/24/rollout-2026-08-24T00-00-00-{ROLLOUT}.jsonl"
        ));
        let mut discovered = AdmissionIndex::new();
        discovered.insert_codex_rollout(ROLLOUT, rollout_path.clone(), 0);
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert_eq!(
            admission.on_evidence(&parent, &EvidenceId::Uuid(STRANGER.to_owned()), &discovered),
            None
        );
        assert!(!admission.is_admitted_path(Path::new(&format!("/tmp/rollout-{STRANGER}.jsonl"))));
        assert_eq!(
            admission.on_evidence(&parent, &EvidenceId::Uuid(ROLLOUT.to_owned()), &discovered),
            Some(SessionScope::Codex {
                rollout_id: ROLLOUT.to_owned(),
            })
        );
        assert!(admission.is_admitted_path(&rollout_path));
    }

    #[test]
    fn evidence_uuid_older_than_anchor_is_not_admitted() {
        const ANCHOR_MS: i64 = 1_787_486_400_000;
        let directory = tempfile::tempdir().unwrap();
        let (discovered, rollout_path) =
            discover_codex_rollout_with_mtime(directory.path(), ROLLOUT, ANCHOR_MS - 1, ANCHOR_MS);
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let rollout_scope = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let mut admission = Admission::new(ANCHOR_MS);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert_eq!(
            admission.on_evidence(&parent, &EvidenceId::Uuid(ROLLOUT.to_owned()), &discovered),
            None,
            "out-of-window UUID evidence must not attach its scope"
        );
        assert!(
            !admission.is_admitted_path(&rollout_path),
            "out-of-window UUID evidence must not admit its artifact path"
        );
        assert_eq!(
            admission.on_evidence(
                &rollout_scope,
                &EvidenceId::ConfigDir(PathBuf::from("/tmp/stale-child")),
                &discovered,
            ),
            None,
            "an out-of-window evidence scope must not become an admitted parent"
        );
    }

    #[test]
    fn evidence_uuid_newer_than_anchor_is_admitted() {
        const ANCHOR_MS: i64 = 1_787_486_400_000;
        let directory = tempfile::tempdir().unwrap();
        let (discovered, rollout_path) =
            discover_codex_rollout_with_mtime(directory.path(), ROLLOUT, ANCHOR_MS + 1, ANCHOR_MS);
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let expected = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let mut admission = Admission::new(ANCHOR_MS);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert_eq!(
            admission.on_evidence(&parent, &EvidenceId::Uuid(ROLLOUT.to_owned()), &discovered),
            Some(expected)
        );
        assert!(admission.is_admitted_path(&rollout_path));
    }

    #[test]
    fn evidence_uuid_at_anchor_is_admitted() {
        const ANCHOR_MS: i64 = 1_787_486_400_000;
        let directory = tempfile::tempdir().unwrap();
        let (discovered, rollout_path) =
            discover_codex_rollout_with_mtime(directory.path(), ROLLOUT, ANCHOR_MS, ANCHOR_MS);
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let expected = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let mut admission = Admission::new(ANCHOR_MS);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert_eq!(
            admission.on_evidence(&parent, &EvidenceId::Uuid(ROLLOUT.to_owned()), &discovered),
            Some(expected)
        );
        assert!(admission.is_admitted_path(&rollout_path));
    }

    #[test]
    fn evidence_uuid_with_multiple_artifacts_admits_only_in_window_paths() {
        const ANCHOR_MS: i64 = 1_787_486_400_000;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let stale_path = root.join(format!(
            "2026/08/23/rollout-2026-08-23T13-00-00-{ROLLOUT}.jsonl"
        ));
        let fresh_path = root.join(format!(
            "2026/08/24/rollout-2026-08-24T13-00-00-{ROLLOUT}.jsonl"
        ));
        for (path, modified_ms) in [(&stale_path, ANCHOR_MS - 1), (&fresh_path, ANCHOR_MS)] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"{}\n").unwrap();
            OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_times(
                    FileTimes::new()
                        .set_modified(UNIX_EPOCH + Duration::from_millis(modified_ms as u64)),
                )
                .unwrap();
        }
        let discovered = AdmissionIndex::discover_codex_date_shards(root, ANCHOR_MS).unwrap();
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let expected = SessionScope::Codex {
            rollout_id: ROLLOUT.to_owned(),
        };
        let mut admission = Admission::new(ANCHOR_MS);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert_eq!(
            admission.on_evidence(&parent, &EvidenceId::Uuid(ROLLOUT.to_owned()), &discovered),
            Some(expected)
        );
        assert!(admission.is_admitted_path(&fresh_path));
        assert!(
            !admission.is_admitted_path(&stale_path),
            "a fresh sidecar must not cause a stale artifact path to attach"
        );
    }

    #[test]
    fn evidence_admission_is_path_exact_across_shards() {
        let file_name = format!("rollout-2026-08-24T00-00-00-{ROLLOUT}.jsonl");
        let admitted_path = PathBuf::from("/home/user/.codex/sessions/2026/08/24").join(&file_name);
        let anchored_out_copy =
            PathBuf::from("/home/user/.codex/sessions/2026/08/20").join(&file_name);
        let mut discovered = AdmissionIndex::new();
        discovered.insert_codex_rollout(ROLLOUT, admitted_path.clone(), 0);
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert!(
            admission
                .on_evidence(&parent, &EvidenceId::Uuid(ROLLOUT.to_owned()), &discovered,)
                .is_some()
        );
        assert!(admission.is_admitted_path(&admitted_path));
        assert!(!admission.is_admitted_path(&anchored_out_copy));
    }

    #[test]
    fn evidence_requires_an_already_admitted_parent() {
        let rollout_path = PathBuf::from(format!(
            "/home/user/.codex/sessions/2026/08/24/rollout-2026-08-24T00-00-00-{ROLLOUT}.jsonl"
        ));
        let mut discovered = AdmissionIndex::new();
        discovered.insert_codex_rollout(ROLLOUT, rollout_path.clone(), 0);
        let mut admission = Admission::new(0);

        assert_eq!(
            admission.on_evidence(
                &SessionScope::ClaudeRoot(PARENT.to_owned()),
                &EvidenceId::Uuid(ROLLOUT.to_owned()),
                &discovered,
            ),
            None
        );
        assert!(!admission.is_admitted_path(&rollout_path));
    }

    #[test]
    fn config_dir_evidence_creates_scoped_root() {
        let parent = SessionScope::ClaudeRoot(PARENT.to_owned());
        let config_dir = PathBuf::from("/opt/claude-secondary");
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Claude, PARENT);

        assert_eq!(
            admission.on_evidence(
                &parent,
                &EvidenceId::ConfigDir(config_dir.clone()),
                &AdmissionIndex::new(),
            ),
            Some(parent.clone())
        );
        assert_eq!(
            admission.derived_roots(),
            &[DerivedRoot {
                scope: parent,
                root: crate::provider::DiscoveryRoot {
                    provider: Provider::Claude,
                    path: config_dir.join("projects"),
                },
            }]
        );
    }

    #[test]
    fn anchor_is_max_of_db_event_and_window() {
        assert_eq!(backfill_anchor_ms(None, 1_000, 400), 600);
        assert_eq!(backfill_anchor_ms(Some(550), 1_000, 400), 600);
        assert_eq!(backfill_anchor_ms(Some(850), 1_000, 400), 850);
    }

    #[test]
    fn rollout_filename_timestamp_round_trips_local_epoch_seconds() {
        for epoch_ms in [
            -1_000,
            0,
            946_684_800_000,
            1_609_459_200_000,
            1_787_745_600_000,
            2_145_916_799_000,
        ] {
            let (_, timestamp) = local_rollout_fixture_parts(epoch_ms);
            let file_name =
                format!("rollout-{timestamp}-22222222-2222-4222-8222-222222222222.jsonl",);

            assert_eq!(
                rollout_filename_timestamp_ms(&file_name),
                Some(epoch_ms),
                "local rollout time did not round-trip for {file_name}"
            );
        }
    }

    #[test]
    fn rollout_filename_rejects_ambiguous_and_nonexistent_local_times() {
        const CHILD_SENTINEL: &str = "HERDR_TOP_TEST_ROLLOUT_DST_CHILD";
        if env::var_os(CHILD_SENTINEL).is_none() {
            let status = Command::new(env::current_exe().unwrap())
                .args([
                    "--exact",
                    "provider::lane::tests::rollout_filename_rejects_ambiguous_and_nonexistent_local_times",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD_SENTINEL, "1")
                .env("TZ", "EST5EDT,M3.2.0/2,M11.1.0/2")
                .status()
                .unwrap();
            assert!(status.success(), "isolated DST regression failed: {status}");
            return;
        }

        let file_name = |timestamp: &str| {
            format!("rollout-{timestamp}-22222222-2222-4222-8222-222222222222.jsonl")
        };
        assert_eq!(
            rollout_filename_timestamp_ms(&file_name("2026-11-01T00-30-00")),
            Some(1_793_507_400_000)
        );
        assert_eq!(
            rollout_filename_timestamp_ms(&file_name("2026-11-01T02-30-00")),
            Some(1_793_518_200_000)
        );

        let ambiguous = file_name("2026-11-01T01-30-00");
        let nonexistent = file_name("2026-03-08T02-30-00");
        let mut index = AdmissionIndex::new();
        index.insert_codex_rollout(ROLLOUT, PathBuf::from(&ambiguous), 0);
        assert_eq!(index.artifacts_for_uuid(ROLLOUT).len(), 1);
        assert_eq!(
            (
                rollout_filename_timestamp_ms(&ambiguous),
                rollout_filename_timestamp_ms(&nonexistent),
                index.artifacts_for_uuid(ROLLOUT)[0].creation_ms,
            ),
            (None, None, None)
        );
    }

    #[test]
    fn per_file_anchor_only_exempts_pane_roots() {
        let mut admission = Admission::new(1_000);
        admission.admit_pane_session(Provider::Claude, PARENT);
        let pane_root = PathBuf::from(format!("/logs/workspace/{PARENT}.jsonl"));
        let subagent = PathBuf::from(format!(
            "/logs/workspace/{PARENT}/subagents/agent-child.jsonl"
        ));
        let evidence_path = PathBuf::from(format!(
            "/logs/sessions/2026/08/24/rollout-2026-08-24T12-00-00-{ROLLOUT}.jsonl"
        ));
        let mut index = AdmissionIndex::new();
        index.insert_codex_rollout(ROLLOUT, evidence_path.clone(), 1_000);
        assert!(
            admission
                .on_evidence(
                    &SessionScope::ClaudeRoot(PARENT.to_owned()),
                    &EvidenceId::Uuid(ROLLOUT.to_owned()),
                    &index,
                )
                .is_some()
        );

        assert!(
            admission.is_admitted_path(&evidence_path),
            "in-window lineage evidence must admit the artifact identity"
        );
        assert!(
            admission.is_admitted_file(&pane_root, 100),
            "pane roots must remain readable before the anchor"
        );
        assert!(!admission.is_admitted_file(&subagent, 999));
        assert!(
            !admission.is_admitted_file(&evidence_path, 100),
            "old evidence-admitted artifacts must honor the anchor"
        );
        assert!(
            admission.is_admitted_file(&evidence_path, 1_000),
            "fresh evidence-admitted artifacts must remain readable"
        );
        assert!(admission.is_admitted_file(&subagent, 1_000));
    }

    #[test]
    fn date_shard_scan_bounded_by_anchor() {
        const ANCHOR_MS: i64 = 1_787_486_400_000;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("sessions");
        let (previous_shard, _) = local_rollout_fixture_parts(ANCHOR_MS - 86_400_000);
        let (anchor_shard, _) = local_rollout_fixture_parts(ANCHOR_MS);
        let (next_shard, _) = local_rollout_fixture_parts(ANCHOR_MS + 86_400_000);
        let shards = [
            (&previous_shard, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            (&anchor_shard, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            (&next_shard, "cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        ];
        for (shard, id) in shards {
            let path = root.join(shard);
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join(format!("rollout-example-{id}.jsonl")), b"{}\n").unwrap();
        }
        let scanned = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&scanned);
        set_codex_shard_scan_hook(move |path| observed.lock().unwrap().push(path.to_path_buf()));

        let index = AdmissionIndex::discover_codex_date_shards(&root, ANCHOR_MS).unwrap();

        assert!(!index.contains_uuid("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"));
        assert!(index.contains_uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"));
        assert!(index.contains_uuid("cccccccc-cccc-4ccc-8ccc-cccccccccccc"));
        assert_eq!(
            *scanned.lock().unwrap(),
            [root.join(anchor_shard), root.join(next_shard)]
        );
    }

    #[test]
    fn anchor_day_rollouts_are_bounded_by_filename_timestamp() {
        const ANCHOR_MS: i64 = 1_787_486_400_000;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("sessions");
        let (before_shard, before_timestamp) = local_rollout_fixture_parts(ANCHOR_MS - 1_000);
        let (anchor_shard, equal_timestamp) = local_rollout_fixture_parts(ANCHOR_MS);
        let (_, after_timestamp) = local_rollout_fixture_parts(ANCHOR_MS + 1_000);
        let (later_shard, later_timestamp) = local_rollout_fixture_parts(ANCHOR_MS + 86_400_000);
        let before_day = root.join(before_shard);
        let anchor_day = root.join(anchor_shard);
        let later_day = root.join(later_shard);
        fs::create_dir_all(&before_day).unwrap();
        fs::create_dir_all(&anchor_day).unwrap();
        fs::create_dir_all(&later_day).unwrap();
        let before = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
        let equal = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
        let after = "ffffffff-ffff-4fff-8fff-ffffffffffff";
        let malformed = "99999999-9999-4999-8999-999999999999";
        let later = "77777777-7777-4777-8777-777777777777";
        fs::write(
            before_day.join(format!("rollout-{before_timestamp}-{before}.jsonl")),
            b"{}\n",
        )
        .unwrap();
        for (name, id) in [
            (format!("rollout-{equal_timestamp}"), equal),
            (format!("rollout-{after_timestamp}"), after),
            ("rollout-not-a-time".to_owned(), malformed),
        ] {
            fs::write(anchor_day.join(format!("{name}-{id}.jsonl")), b"{}\n").unwrap();
        }
        fs::write(
            later_day.join(format!("rollout-{later_timestamp}-{later}.jsonl")),
            b"{}\n",
        )
        .unwrap();

        let index = AdmissionIndex::discover_codex_date_shards(&root, ANCHOR_MS).unwrap();

        assert!(!index.contains_uuid(before));
        assert!(index.contains_uuid(equal));
        assert!(index.contains_uuid(after));
        assert!(index.contains_uuid(malformed));
        assert!(index.contains_uuid(later));
    }

    #[test]
    fn codex_shard_entry_error_retains_sibling_rollouts() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("sessions");
        let shard = root.join("2026/08/24");
        fs::create_dir_all(&shard).unwrap();
        let unreadable_id = "11111111-aaaa-4111-8111-111111111111";
        let sibling_id = "22222222-bbbb-4222-8222-222222222222";
        let unreadable = shard.join(format!("rollout-2026-08-24T00-00-00-{unreadable_id}.jsonl"));
        let sibling = shard.join(format!("rollout-2026-08-24T00-00-01-{sibling_id}.jsonl"));
        fs::write(&unreadable, b"{}\n").unwrap();
        fs::write(&sibling, b"{}\n").unwrap();
        let injected_path = unreadable.clone();
        set_codex_discovery_file_type_hook(move |path| {
            if path == injected_path {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected unreadable rollout",
                ))
            } else {
                Ok(())
            }
        });

        let index = AdmissionIndex::discover_codex_date_shards(&root, 0).unwrap();

        assert!(index.had_errors());
        assert!(!index.contains_uuid(unreadable_id));
        assert!(index.contains_uuid(sibling_id));
    }

    #[test]
    fn offsets_advance_incrementally_via_existing_tail() {
        let directory = tempfile::tempdir().unwrap();
        let relative = Path::new("rollout.jsonl");
        let path = directory.path().join(relative);
        fs::write(&path, b"").unwrap();
        let mut boundary = FsReadBoundary;
        let mut tail = TailFile::open(
            directory.path(),
            relative,
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();

        let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
        writer.write_all(b"one\n").unwrap();
        writer.flush().unwrap();
        assert_eq!(tail.poll(&mut boundary).unwrap().len(), 1);
        assert_eq!(tail.offset(), 4);

        writer.write_all(b"two\n").unwrap();
        writer.flush().unwrap();
        assert_eq!(tail.poll(&mut boundary).unwrap().len(), 1);
        assert_eq!(tail.offset(), 8);
    }

    #[test]
    fn record_ordinal_is_stable_after_reopen_at_nonzero_offset() {
        let directory = tempfile::tempdir().unwrap();
        let relative = PathBuf::from(format!("rollout-example-{ROLLOUT}.jsonl"));
        let mut fixture = b"first\r\n".to_vec();
        fixture.extend(vec![b'x'; MAX_TAIL_RECORD_BYTES + 1]);
        fixture.push(b'\n');
        fixture.extend_from_slice(b"trailing-partial");
        fs::write(directory.path().join(&relative), &fixture).unwrap();
        let mut admission = Admission::new(0);
        admission.admit_pane_session(Provider::Codex, ROLLOUT);
        let mut boundary = FsReadBoundary;
        let mut tail = TailFile::open(
            directory.path(),
            &relative,
            &FirstSeenBaseline::default(),
            0,
            &mut boundary,
        )
        .unwrap();
        let mut records = Vec::new();
        while tail.offset() < fixture.len() as u64 {
            records.extend(tail.poll(&mut boundary).unwrap());
        }

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].bytes, b"first");
        assert_eq!(records[0].error_code, None);
        assert!(records[1].bytes.is_empty());
        assert_eq!(records[1].error_code, Some(RECORD_TOO_LONG_ERROR));
        assert!(tail.poll(&mut boundary).unwrap().is_empty());

        for (record_index, record) in records.iter().enumerate() {
            assert_eq!(
                record_ordinal_at_offset(
                    directory.path(),
                    &relative,
                    record.offset,
                    &admission,
                    &ProviderDiagnostics::default(),
                )
                .unwrap(),
                record_index as u64
            );
        }

        let reopened_ordinal = record_ordinal_at_offset(
            directory.path(),
            &relative,
            records[1].offset,
            &admission,
            &ProviderDiagnostics::default(),
        )
        .unwrap();
        assert_eq!(reopened_ordinal, 1);
    }

    #[test]
    fn backfill_window_parser_uses_positive_utf8_i64_or_default() {
        assert_eq!(parse_backfill_window_ms(None), DEFAULT_BACKFILL_WINDOW_MS);
        assert_eq!(parse_backfill_window_ms(Some(OsStr::new("42"))), 42);
        assert_eq!(parse_complete_grace_ms(None), DEFAULT_COMPLETE_GRACE_MS);
        assert_eq!(parse_complete_grace_ms(Some(OsStr::new("42"))), 42);
        assert_eq!(
            parse_headless_inactivity_ms(None),
            DEFAULT_HEADLESS_INACTIVITY_MS
        );
        assert_eq!(parse_headless_inactivity_ms(Some(OsStr::new("42"))), 42);
        for value in ["", "0", "-1", "1.5", " 42", "9223372036854775808"] {
            assert_eq!(
                parse_backfill_window_ms(Some(OsStr::new(value))),
                DEFAULT_BACKFILL_WINDOW_MS
            );
            assert_eq!(
                parse_complete_grace_ms(Some(OsStr::new(value))),
                DEFAULT_COMPLETE_GRACE_MS
            );
            assert_eq!(
                parse_headless_inactivity_ms(Some(OsStr::new(value))),
                DEFAULT_HEADLESS_INACTIVITY_MS
            );
        }
    }

    #[test]
    fn display_duration_parsers_use_positive_utf8_i64_or_default() {
        assert_eq!(parse_stall_warn_ms(None), DEFAULT_STALL_WARN_MS);
        assert_eq!(parse_ghost_visibility_ms(None), DEFAULT_GHOST_VISIBILITY_MS);
        assert_eq!(parse_stall_warn_ms(Some(OsStr::new("42"))), 42);
        assert_eq!(parse_ghost_visibility_ms(Some(OsStr::new("42"))), 42);
        for value in ["", "0", "-1", "1.5", " 42", "9223372036854775808"] {
            assert_eq!(
                parse_stall_warn_ms(Some(OsStr::new(value))),
                DEFAULT_STALL_WARN_MS
            );
            assert_eq!(
                parse_ghost_visibility_ms(Some(OsStr::new(value))),
                DEFAULT_GHOST_VISIBILITY_MS
            );
        }
    }

    #[test]
    fn claude_topology_represents_subagent_directory_and_sidecars() {
        assert_eq!(
            path_topology(Path::new(&format!("workspace/{PARENT}/subagents"))),
            Some(ClaudePathTopology::SubagentsDir {
                parent_session: PARENT.to_owned(),
            })
        );
        assert_eq!(
            path_topology(Path::new(&format!(
                "workspace/{PARENT}/subagents/agent-child.jsonl"
            ))),
            Some(ClaudePathTopology::Subagent {
                parent_session: PARENT.to_owned(),
                agent_id: "child".to_owned(),
            })
        );
        assert_eq!(
            path_topology(Path::new(&format!(
                "workspace/{PARENT}/subagents/agent-child.meta.json"
            ))),
            Some(ClaudePathTopology::SubagentMeta {
                parent_session: PARENT.to_owned(),
                agent_id: "child".to_owned(),
            })
        );
        assert_eq!(
            path_topology(Path::new(&format!(
                "workspace/{PARENT}/subagents/agent-.jsonl"
            ))),
            None
        );
        assert_eq!(
            path_topology(Path::new(&format!(
                "workspace/{PARENT}/subagents/agent-.meta.json"
            ))),
            None
        );
    }
}
