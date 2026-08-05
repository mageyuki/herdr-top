//! T9 subscribe/buffer/snapshot/replay collector, convergence, and gap reconciliation.

use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::lockfile::OwnerRecord;
use crate::model::{
    AgentSessionReference, AgentSessionReferenceKind, EventMetadata, ExecState, Execution, GapKind,
    NormalizedEvent, Pane, PaneSnapshot, Provider, ReconcileBatch, RunId, SharedModel,
    SnapshotAgent, Tab, TopologyEntity, TopologyEntityId, TopologySnapshot, Workspace,
};
use crate::reducer::Reducer;
use crate::store::writer::{WriterClient, WriterError};
use crate::store::{CollectorGap, PersistBatch, PersistOp, RestoredState};

use super::types::{AgentSessionKind, PaneInfo, Snapshot, Subscription, TabInfo, WorkspaceInfo};
use super::wire::{self, EventStream, WireError};

const EVENT_QUEUE_CAPACITY: usize = 64;
const RESNAPSHOT_ATTEMPTS: usize = 3;
const RECONNECT_DELAY: Duration = Duration::from_millis(50);
const DRAIN_QUIET_PERIOD: Duration = Duration::from_millis(5);

/// Current quality of the Herdr physical-state observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationQuality {
    /// Subscription is active and the latest buffer generation drained cleanly.
    Live,
    /// The collector is restoring, reconnecting, or resnapshotting.
    Reconciling,
    /// The Herdr socket or subscription is unavailable.
    Disconnected,
    /// Physical state is available but another source or target is unavailable.
    Degraded,
}

/// Errors surfaced by collector startup or orderly shutdown.
#[derive(Debug, Error)]
pub enum CollectorError {
    /// The SQLite writer rejected an operation.
    #[error(transparent)]
    Writer(#[from] WriterError),
    /// Herdr returned an invalid or failed wire exchange.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// A snapshot pane did not carry the tab identity required by the domain model.
    #[error("snapshot pane {pane_id:?} has no tab_id")]
    MissingTabId { pane_id: String },
    /// The collector task panicked or was cancelled externally.
    #[error("collector task failed: {0}")]
    Task(String),
}

/// Handle to the collector's coherent model and observation-quality streams.
pub struct CollectorHandle {
    /// Independently published Herdr observation quality.
    pub quality: watch::Receiver<ObservationQuality>,
    /// Coherent reducer-owned domain snapshots.
    pub model: SharedModel,
    cancellation: CancellationToken,
    task: JoinHandle<Result<(), CollectorError>>,
}

impl CollectorHandle {
    /// Cancels the collector and waits for its subscription task to exit.
    pub async fn stop(self) -> Result<(), CollectorError> {
        self.cancellation.cancel();
        self.task
            .await
            .map_err(|error| CollectorError::Task(error.to_string()))?
    }
}

/// Commits the new owner record, then launches subscribe-first convergence.
pub async fn spawn(
    sock: PathBuf,
    restored: RestoredState,
    writer: WriterClient,
) -> Result<CollectorHandle, CollectorError> {
    let owner = OwnerTracker::from_environment();
    writer.replace_owner(owner.record()).await?;

    let (reducer, model) = Reducer::new(restored);
    let (quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task_model = model.clone();
    let task = tokio::spawn(async move {
        run_collector(
            sock,
            writer,
            reducer,
            task_model,
            quality_sender,
            task_cancellation,
            owner,
        )
        .await
    });

    Ok(CollectorHandle {
        quality,
        model,
        cancellation,
        task,
    })
}

async fn run_collector(
    sock: PathBuf,
    writer: WriterClient,
    mut reducer: Reducer,
    shared: SharedModel,
    quality: watch::Sender<ObservationQuality>,
    cancellation: CancellationToken,
    mut owner: OwnerTracker,
) -> Result<(), CollectorError> {
    let session = env::var("HERDR_SESSION")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "default".to_owned());
    let mut first_subscription = true;
    let mut previous_socket = None;

    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }

        let socket_identity = socket_identity(&sock);
        let stream = match wire::subscribe(&sock, &subscriptions()).await {
            Ok(stream) => stream,
            Err(_) => {
                quality.send_replace(ObservationQuality::Disconnected);
                if wait_or_cancel(&cancellation, RECONNECT_DELAY).await {
                    return Ok(());
                }
                continue;
            }
        };
        let gap_kind = if first_subscription {
            GapKind::Startup
        } else if previous_socket.is_some() && previous_socket != socket_identity {
            GapKind::SocketReplacement
        } else {
            GapKind::Reconnect
        };
        first_subscription = false;
        previous_socket = socket_identity;
        quality.send_replace(ObservationQuality::Reconciling);

        let reader_cancellation = cancellation.child_token();
        let (events, overflowed, reader) = spawn_event_reader(stream, reader_cancellation.clone());
        let outcome = converge(
            &sock,
            &writer,
            &mut reducer,
            &shared,
            &quality,
            &cancellation,
            &mut owner,
            &session,
            gap_kind,
            events,
            Arc::clone(&overflowed),
        )
        .await?;

        reader_cancellation.cancel();
        let _ = reader.await;
        match outcome {
            SubscriptionOutcome::Cancelled => return Ok(()),
            SubscriptionOutcome::Ended => {
                quality.send_replace(ObservationQuality::Disconnected);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn converge(
    sock: &Path,
    writer: &WriterClient,
    reducer: &mut Reducer,
    shared: &SharedModel,
    quality: &watch::Sender<ObservationQuality>,
    cancellation: &CancellationToken,
    owner: &mut OwnerTracker,
    session: &str,
    gap_kind: GapKind,
    mut events: mpsc::Receiver<ReceivedEvent>,
    overflowed: Arc<AtomicBool>,
) -> Result<SubscriptionOutcome, CollectorError> {
    let mut first_generation = true;
    let mut resnapshot_attempts = 0;

    loop {
        overflowed.store(false, Ordering::Release);
        let snapshot = tokio::select! {
            () = cancellation.cancelled() => return Ok(SubscriptionOutcome::Cancelled),
            result = wire::request(sock, "session.snapshot", json!({})) => {
                match result.and_then(|value| value.into_snapshot()) {
                    Ok(snapshot) => snapshot,
                    Err(_) => return Ok(SubscriptionOutcome::Ended),
                }
            }
        };
        let topology = topology_from_snapshot(&snapshot)?;
        let mut batch = if first_generation {
            let mut batch = reducer.reconcile_gap(ReconcileBatch { topology, gap_kind });
            batch.push(PersistOp::RecordCollectorGap(CollectorGap {
                event_id: format!("collector-gap-{}", ulid::Ulid::new()),
                herdr_session: session.to_owned(),
                seen_at_ms: unix_now_ms(),
                kind: gap_kind,
            }));
            first_generation = false;
            batch
        } else {
            apply_snapshot_in_place(reducer, shared, topology, session)
        };
        writer.apply(std::mem::take(&mut batch)).await?;
        owner.refresh_from_snapshot(&snapshot, writer).await?;

        let replay = replay_generation(
            reducer,
            shared,
            writer,
            owner,
            session,
            &snapshot,
            &mut events,
            &overflowed,
            cancellation,
        )
        .await?;
        match replay {
            ReplayOutcome::Cancelled => return Ok(SubscriptionOutcome::Cancelled),
            ReplayOutcome::Ended => return Ok(SubscriptionOutcome::Ended),
            ReplayOutcome::Clean => {
                quality.send_replace(ObservationQuality::Live);
                match monitor_live(
                    reducer,
                    shared,
                    writer,
                    owner,
                    session,
                    &mut events,
                    &overflowed,
                    cancellation,
                )
                .await?
                {
                    ReplayOutcome::Dirty => {
                        quality.send_replace(ObservationQuality::Reconciling);
                        resnapshot_attempts = 0;
                    }
                    ReplayOutcome::Ended => return Ok(SubscriptionOutcome::Ended),
                    ReplayOutcome::Cancelled => return Ok(SubscriptionOutcome::Cancelled),
                    ReplayOutcome::Clean => {}
                }
            }
            ReplayOutcome::Dirty => {
                quality.send_replace(ObservationQuality::Reconciling);
                if resnapshot_attempts == RESNAPSHOT_ATTEMPTS {
                    return monitor_reconciling(
                        reducer,
                        shared,
                        writer,
                        owner,
                        session,
                        events,
                        cancellation,
                    )
                    .await;
                }
                resnapshot_attempts += 1;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn replay_generation(
    reducer: &mut Reducer,
    shared: &SharedModel,
    writer: &WriterClient,
    owner: &mut OwnerTracker,
    session: &str,
    snapshot: &Snapshot,
    events: &mut mpsc::Receiver<ReceivedEvent>,
    overflowed: &AtomicBool,
    cancellation: &CancellationToken,
) -> Result<ReplayOutcome, CollectorError> {
    let snapshot_entities = snapshot_entity_keys(snapshot);
    let mut buffered = Vec::new();
    let mut created = HashSet::new();
    let mut closures: HashMap<EntityKey, Vec<usize>> = HashMap::new();
    let mut candidates = Vec::new();
    let mut next = 0;
    if drain_events(events, &mut buffered).is_err() {
        return Ok(ReplayOutcome::Ended);
    }

    loop {
        while next < buffered.len() {
            let received = buffered[next].clone();
            record_replay_facts(
                next,
                &received,
                &mut created,
                &mut closures,
                &mut candidates,
            );
            apply_received_event(reducer, shared, writer, owner, session, received).await?;
            next += 1;
            if drain_events(events, &mut buffered).is_err() {
                return Ok(ReplayOutcome::Ended);
            }
        }

        let next_received = tokio::select! {
            () = cancellation.cancelled() => return Ok(ReplayOutcome::Cancelled),
            result = tokio::time::timeout(DRAIN_QUIET_PERIOD, events.recv()) => result,
        };
        match next_received {
            Ok(Some(received)) => buffered.push(received),
            Ok(None) => return Ok(ReplayOutcome::Ended),
            Err(_) => break,
        }
    }

    let anomalous = candidates.into_iter().any(|(index, entity)| {
        !snapshot_entities.contains(&entity)
            && !created.contains(&entity)
            && !closures
                .get(&entity)
                .is_some_and(|positions| positions.iter().any(|position| *position > index))
    });
    if anomalous || overflowed.load(Ordering::Acquire) {
        Ok(ReplayOutcome::Dirty)
    } else if cancellation.is_cancelled() {
        Ok(ReplayOutcome::Cancelled)
    } else {
        Ok(ReplayOutcome::Clean)
    }
}

#[allow(clippy::too_many_arguments)]
async fn monitor_live(
    reducer: &mut Reducer,
    shared: &SharedModel,
    writer: &WriterClient,
    owner: &mut OwnerTracker,
    session: &str,
    events: &mut mpsc::Receiver<ReceivedEvent>,
    overflowed: &AtomicBool,
    cancellation: &CancellationToken,
) -> Result<ReplayOutcome, CollectorError> {
    loop {
        let received = tokio::select! {
            () = cancellation.cancelled() => return Ok(ReplayOutcome::Cancelled),
            received = events.recv() => match received {
                Some(received) => received,
                None => return Ok(ReplayOutcome::Ended),
            },
        };
        let anomalous =
            updated_entity(&received).is_some_and(|entity| !entity_exists(shared, &entity));
        apply_received_event(reducer, shared, writer, owner, session, received).await?;
        if anomalous || overflowed.swap(false, Ordering::AcqRel) {
            return Ok(ReplayOutcome::Dirty);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn monitor_reconciling(
    reducer: &mut Reducer,
    shared: &SharedModel,
    writer: &WriterClient,
    owner: &mut OwnerTracker,
    session: &str,
    mut events: mpsc::Receiver<ReceivedEvent>,
    cancellation: &CancellationToken,
) -> Result<SubscriptionOutcome, CollectorError> {
    loop {
        let received = tokio::select! {
            () = cancellation.cancelled() => return Ok(SubscriptionOutcome::Cancelled),
            received = events.recv() => match received {
                Some(received) => received,
                None => return Ok(SubscriptionOutcome::Ended),
            },
        };
        apply_received_event(reducer, shared, writer, owner, session, received).await?;
    }
}

fn spawn_event_reader(
    mut stream: EventStream,
    cancellation: CancellationToken,
) -> (
    mpsc::Receiver<ReceivedEvent>,
    Arc<AtomicBool>,
    JoinHandle<Result<(), WireError>>,
) {
    let (sender, receiver) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let overflowed = Arc::new(AtomicBool::new(false));
    let reader_overflowed = Arc::clone(&overflowed);
    let task = tokio::spawn(async move {
        loop {
            let received = tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                received = stream.next_event() => received?,
            };
            let Some((event, data)) = received else {
                return Ok(());
            };
            let received = ReceivedEvent { event, data };
            match sender.try_send(received) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(received)) => {
                    reader_overflowed.store(true, Ordering::Release);
                    tokio::select! {
                        () = cancellation.cancelled() => return Ok(()),
                        result = sender.send(received) => {
                            if result.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
            }
        }
    });
    (receiver, overflowed, task)
}

#[derive(Clone)]
struct ReceivedEvent {
    event: String,
    data: Value,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum EntityKey {
    Workspace(String),
    Tab(String),
    Pane(String),
}

#[derive(Clone, Copy)]
enum ReplayOutcome {
    Clean,
    Dirty,
    Ended,
    Cancelled,
}

#[derive(Clone, Copy)]
enum SubscriptionOutcome {
    Ended,
    Cancelled,
}

enum DrainError {
    Closed,
}

fn drain_events(
    events: &mut mpsc::Receiver<ReceivedEvent>,
    buffered: &mut Vec<ReceivedEvent>,
) -> Result<(), DrainError> {
    loop {
        match events.try_recv() {
            Ok(received) => buffered.push(received),
            Err(mpsc::error::TryRecvError::Empty) => return Ok(()),
            Err(mpsc::error::TryRecvError::Disconnected) => return Err(DrainError::Closed),
        }
    }
}

async fn apply_received_event(
    reducer: &mut Reducer,
    shared: &SharedModel,
    writer: &WriterClient,
    owner: &mut OwnerTracker,
    session: &str,
    received: ReceivedEvent,
) -> Result<(), CollectorError> {
    if received.event == "pane_moved" {
        owner.refresh_from_move(&received.data, writer).await?;
    }
    let normalized = normalize_event(shared, session, &received)?;
    let mut persist = Vec::new();
    for event in normalized {
        persist.extend(reducer.apply(event));
    }
    if !persist.is_empty() {
        writer.apply(persist).await?;
    }
    Ok(())
}

fn normalize_event(
    shared: &SharedModel,
    session: &str,
    received: &ReceivedEvent,
) -> Result<Vec<NormalizedEvent>, CollectorError> {
    let mut events = Vec::new();
    match received.event.as_str() {
        "workspace_created" => {
            if let Some(value) = received.data.get("workspace") {
                let workspace: WorkspaceInfo = decode_wire(value)?;
                events.push(topology_upsert(
                    session,
                    &received.event,
                    TopologyEntity::Workspace(Workspace {
                        workspace_id: workspace.workspace_id,
                    }),
                ));
            }
        }
        "workspace_renamed" => {
            if let Some(value) = received.data.get("workspace") {
                let workspace: WorkspaceInfo = decode_wire(value)?;
                if shared.borrow().workspace(&workspace.workspace_id).is_some() {
                    events.push(topology_upsert(
                        session,
                        &received.event,
                        TopologyEntity::Workspace(Workspace {
                            workspace_id: workspace.workspace_id,
                        }),
                    ));
                }
            }
        }
        "workspace_closed" => {
            if let Some(workspace_id) = string_field(&received.data, "workspace_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Workspace { workspace_id },
                ));
            }
        }
        "tab_created" => {
            if let Some(value) = received.data.get("tab") {
                let tab: TabInfo = decode_wire(value)?;
                events.push(topology_upsert(
                    session,
                    &received.event,
                    TopologyEntity::Tab(Tab {
                        tab_id: tab.tab_id,
                        workspace_id: tab.workspace_id,
                    }),
                ));
            }
        }
        "tab_closed" => {
            if let Some(tab_id) = string_field(&received.data, "tab_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Tab { tab_id },
                ));
            }
        }
        "pane_created" => {
            if let Some(value) = received.data.get("pane") {
                let pane: PaneInfo = decode_wire(value)?;
                append_pane_upsert(shared, session, &received.event, pane, &mut events)?;
            }
        }
        "pane_updated" | "pane_agent_detected" => {
            if let Some(value) = received.data.get("pane") {
                let pane: PaneInfo = decode_wire(value)?;
                if shared.borrow().pane(&pane.pane_id).is_some() {
                    append_pane_upsert(shared, session, &received.event, pane, &mut events)?;
                }
            }
        }
        "pane_closed" => {
            if let Some(pane_id) = string_field(&received.data, "pane_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Pane { pane_id },
                ));
            }
        }
        "pane_exited" => {
            let pane_id = string_field(&received.data, "pane_id");
            append_execution_ends(
                shared,
                session,
                &received.event,
                pane_id.as_deref(),
                &mut events,
            );
        }
        "pane_moved" => {
            if let Some(value) = received.data.get("created_tab") {
                let tab: TabInfo = decode_wire(value)?;
                events.push(topology_upsert(
                    session,
                    &received.event,
                    TopologyEntity::Tab(Tab {
                        tab_id: tab.tab_id,
                        workspace_id: tab.workspace_id,
                    }),
                ));
            }
            if let Some(value) = received.data.get("pane") {
                let pane: PaneInfo = decode_wire(value)?;
                append_pane_move(shared, session, &received.event, pane, &mut events)?;
            }
            if let Some(tab_id) = string_field(&received.data, "closed_tab_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Tab { tab_id },
                ));
            }
            if let Some(workspace_id) = string_field(&received.data, "closed_workspace_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Workspace { workspace_id },
                ));
            }
            if let Some(previous_pane_id) = string_field(&received.data, "previous_pane_id") {
                events.push(topology_closure(
                    session,
                    &received.event,
                    TopologyEntityId::Pane {
                        pane_id: previous_pane_id,
                    },
                ));
            }
        }
        "pane_agent_status_changed" => {
            let state = status_from_value(
                received
                    .data
                    .get("agent_status")
                    .or_else(|| received.data.get("status"))
                    .or_else(|| received.data.get("new_status")),
            );
            let pane_id = string_field(&received.data, "pane_id")
                .or_else(|| nested_string(&received.data, "pane", "pane_id"));
            let terminal_id = string_field(&received.data, "terminal_id")
                .or_else(|| nested_string(&received.data, "pane", "terminal_id"));
            for execution in shared.borrow().executions().filter(|execution| {
                !execution.state.is_terminal()
                    && (pane_id.as_deref() == Some(execution.pane_id.as_str())
                        || terminal_id.as_deref() == Some(execution.terminal_id.as_str()))
            }) {
                events.push(NormalizedEvent::AgentStatusChanged {
                    metadata: metadata(session, &received.event),
                    execution_id: execution.execution_id.clone(),
                    state: state.clone(),
                });
            }
        }
        _ => {}
    }
    Ok(events)
}

fn append_pane_move(
    shared: &SharedModel,
    session: &str,
    event_kind: &str,
    pane: PaneInfo,
    events: &mut Vec<NormalizedEvent>,
) -> Result<(), CollectorError> {
    let pane_entity = pane_entity(&pane)?;
    events.push(topology_upsert(
        session,
        event_kind,
        TopologyEntity::Pane(pane_entity.clone()),
    ));
    for execution in shared.borrow().executions().filter(|execution| {
        execution.terminal_id == pane.terminal_id && !execution.state.is_terminal()
    }) {
        let mut moved = execution.clone();
        moved.pane_id.clone_from(&pane_entity.pane_id);
        events.push(NormalizedEvent::ExecutionBegin {
            metadata: pane_metadata(session, event_kind, &pane),
            execution: moved,
        });
    }
    Ok(())
}

fn append_pane_upsert(
    shared: &SharedModel,
    session: &str,
    event_kind: &str,
    pane: PaneInfo,
    events: &mut Vec<NormalizedEvent>,
) -> Result<(), CollectorError> {
    events.push(topology_upsert(
        session,
        event_kind,
        TopologyEntity::Pane(pane_entity(&pane)?),
    ));
    let has_live_execution = shared.borrow().executions().any(|execution| {
        execution.terminal_id == pane.terminal_id && !execution.state.is_terminal()
    });
    if pane.agent.is_some() && !has_live_execution {
        events.push(execution_begin(session, event_kind, &pane));
    }
    Ok(())
}

fn append_execution_ends(
    shared: &SharedModel,
    session: &str,
    event_kind: &str,
    pane_id: Option<&str>,
    events: &mut Vec<NormalizedEvent>,
) {
    for execution in shared.borrow().executions().filter(|execution| {
        !execution.state.is_terminal() && pane_id == Some(execution.pane_id.as_str())
    }) {
        events.push(NormalizedEvent::ExecutionEnd {
            metadata: metadata(session, event_kind),
            execution_id: execution.execution_id.clone(),
        });
    }
}

fn apply_snapshot_in_place(
    reducer: &mut Reducer,
    shared: &SharedModel,
    topology: TopologySnapshot,
    session: &str,
) -> PersistBatch {
    let mut persist = Vec::new();
    let snapshot_workspace_ids: HashSet<_> = topology
        .workspaces
        .iter()
        .map(|workspace| workspace.workspace_id.clone())
        .collect();
    let snapshot_tab_ids: HashSet<_> = topology.tabs.iter().map(|tab| tab.tab_id.clone()).collect();
    let snapshot_pane_ids: HashSet<_> = topology
        .panes
        .iter()
        .map(|pane| pane.pane_id.clone())
        .collect();

    for workspace in &topology.workspaces {
        persist.extend(reducer.apply(topology_upsert(
            session,
            "snapshot_workspace",
            TopologyEntity::Workspace(workspace.clone()),
        )));
    }
    for tab in &topology.tabs {
        persist.extend(reducer.apply(topology_upsert(
            session,
            "snapshot_tab",
            TopologyEntity::Tab(tab.clone()),
        )));
    }
    for pane in &topology.panes {
        persist.extend(reducer.apply(topology_upsert(
            session,
            "snapshot_pane",
            TopologyEntity::Pane(Pane {
                pane_id: pane.pane_id.clone(),
                workspace_id: pane.workspace_id.clone(),
                tab_id: pane.tab_id.clone(),
                terminal_id: pane.terminal_id.clone(),
            }),
        )));
        let current: Vec<_> = shared
            .borrow()
            .executions()
            .filter(|execution| {
                execution.terminal_id == pane.terminal_id && !execution.state.is_terminal()
            })
            .cloned()
            .collect();
        for mut execution in current {
            execution.pane_id.clone_from(&pane.pane_id);
            if let Some(agent) = &pane.agent {
                execution.state = agent.state.clone();
            }
            persist.extend(reducer.apply(NormalizedEvent::ExecutionBegin {
                metadata: metadata(session, "snapshot_execution"),
                execution,
            }));
        }
    }

    let old_panes: Vec<_> = shared
        .borrow()
        .panes()
        .filter(|pane| !snapshot_pane_ids.contains(&pane.pane_id))
        .map(|pane| pane.pane_id.clone())
        .collect();
    let old_tabs: Vec<_> = shared
        .borrow()
        .tabs()
        .filter(|tab| !snapshot_tab_ids.contains(&tab.tab_id))
        .map(|tab| tab.tab_id.clone())
        .collect();
    let old_workspaces: Vec<_> = shared
        .borrow()
        .workspaces()
        .filter(|workspace| !snapshot_workspace_ids.contains(&workspace.workspace_id))
        .map(|workspace| workspace.workspace_id.clone())
        .collect();
    for pane_id in old_panes {
        persist.extend(reducer.apply(topology_closure(
            session,
            "snapshot_pane_missing",
            TopologyEntityId::Pane { pane_id },
        )));
    }
    for tab_id in old_tabs {
        persist.extend(reducer.apply(topology_closure(
            session,
            "snapshot_tab_missing",
            TopologyEntityId::Tab { tab_id },
        )));
    }
    for workspace_id in old_workspaces {
        persist.extend(reducer.apply(topology_closure(
            session,
            "snapshot_workspace_missing",
            TopologyEntityId::Workspace { workspace_id },
        )));
    }
    persist
}

fn topology_from_snapshot(snapshot: &Snapshot) -> Result<TopologySnapshot, CollectorError> {
    let workspaces = snapshot
        .workspaces
        .iter()
        .map(|workspace| Workspace {
            workspace_id: workspace.workspace_id.clone(),
        })
        .collect();
    let tabs = snapshot
        .tabs
        .iter()
        .map(|tab| Tab {
            tab_id: tab.tab_id.clone(),
            workspace_id: tab.workspace_id.clone(),
        })
        .collect();
    let panes = snapshot
        .panes
        .iter()
        .map(|pane| {
            let tab_id = pane
                .tab_id
                .clone()
                .ok_or_else(|| CollectorError::MissingTabId {
                    pane_id: pane.pane_id.clone(),
                })?;
            Ok(PaneSnapshot {
                pane_id: pane.pane_id.clone(),
                workspace_id: pane.workspace_id.clone(),
                tab_id,
                terminal_id: pane.terminal_id.clone(),
                agent: pane.agent.as_ref().map(|agent| SnapshotAgent {
                    agent_name: agent.clone(),
                    state: status_from_str(pane.agent_status.as_deref()),
                }),
                agent_session: pane.agent_session.as_ref().map(agent_session_reference),
            })
        })
        .collect::<Result<Vec<_>, CollectorError>>()?;
    Ok(TopologySnapshot {
        workspaces,
        tabs,
        panes,
    })
}

fn subscriptions() -> Vec<Subscription> {
    [
        "workspace.created",
        "workspace.renamed",
        "workspace.closed",
        "workspace.focused",
        "tab.created",
        "tab.closed",
        "tab.focused",
        "pane.created",
        "pane.closed",
        "pane.updated",
        "pane.focused",
        "pane.moved",
        "pane.exited",
        "pane.agent_detected",
        "pane.agent_status_changed",
        "layout.updated",
    ]
    .into_iter()
    .map(Subscription::new)
    .collect()
}

fn topology_upsert(session: &str, kind: &str, entity: TopologyEntity) -> NormalizedEvent {
    NormalizedEvent::TopologyUpsert {
        metadata: metadata(session, kind),
        entity,
    }
}

fn topology_closure(session: &str, kind: &str, entity: TopologyEntityId) -> NormalizedEvent {
    NormalizedEvent::TopologyClosure {
        metadata: metadata(session, kind),
        entity,
    }
}

fn execution_begin(session: &str, kind: &str, pane: &PaneInfo) -> NormalizedEvent {
    NormalizedEvent::ExecutionBegin {
        metadata: pane_metadata(session, kind, pane),
        execution: Execution {
            execution_id: format!("herdr-execution-{}", ulid::Ulid::new()),
            pane_id: pane.pane_id.clone(),
            terminal_id: pane.terminal_id.clone(),
            task_run_id: RunId::new(),
            state: status_from_str(pane.agent_status.as_deref()),
        },
    }
}

fn pane_metadata(session: &str, kind: &str, pane: &PaneInfo) -> EventMetadata {
    let mut metadata = metadata(session, kind);
    metadata.workspace_id = Some(pane.workspace_id.clone());
    metadata.tab_id.clone_from(&pane.tab_id);
    metadata.pane_id = Some(pane.pane_id.clone());
    metadata.terminal_id = Some(pane.terminal_id.clone());
    metadata.provider = pane_provider(pane);
    metadata.native_session_id = pane.agent_session.as_ref().and_then(|reference| {
        (reference.kind == AgentSessionKind::Id && !reference.value.is_empty())
            .then(|| reference.value.clone())
    });
    metadata
}

fn metadata(session: &str, kind: &str) -> EventMetadata {
    EventMetadata {
        event_id: format!("herdr-event-{}", ulid::Ulid::new()),
        timestamp_ms: unix_now_ms(),
        source: "herdr".to_owned(),
        source_event_type: kind.to_owned(),
        herdr_session: session.to_owned(),
        workspace_id: None,
        tab_id: None,
        pane_id: None,
        terminal_id: None,
        provider: None,
        native_session_id: None,
        task_run_id: None,
        agent_node_id: None,
        task_state: None,
        execution_parent: None,
        dependency: None,
        source_coverage: Vec::new(),
        provider_metadata: None,
    }
}

fn pane_entity(pane: &PaneInfo) -> Result<Pane, CollectorError> {
    Ok(Pane {
        pane_id: pane.pane_id.clone(),
        workspace_id: pane.workspace_id.clone(),
        tab_id: pane
            .tab_id
            .clone()
            .ok_or_else(|| CollectorError::MissingTabId {
                pane_id: pane.pane_id.clone(),
            })?,
        terminal_id: pane.terminal_id.clone(),
    })
}

fn agent_session_reference(reference: &super::types::AgentSessionInfo) -> AgentSessionReference {
    AgentSessionReference {
        source: reference.source.clone(),
        agent: reference.agent.clone(),
        kind: match reference.kind {
            AgentSessionKind::Id => AgentSessionReferenceKind::Id,
            AgentSessionKind::Path => AgentSessionReferenceKind::Path,
        },
        value: reference.value.clone(),
    }
}

fn pane_provider(pane: &PaneInfo) -> Option<Provider> {
    pane.agent_session
        .as_ref()
        .and_then(|reference| provider_from_name(&reference.agent))
        .or_else(|| pane.agent.as_deref().and_then(provider_from_name))
}

fn provider_from_name(name: &str) -> Option<Provider> {
    let normalized = name.to_ascii_lowercase();
    if normalized.contains("codex") {
        Some(Provider::Codex)
    } else if normalized.contains("claude") {
        Some(Provider::Claude)
    } else {
        None
    }
}

fn status_from_value(value: Option<&Value>) -> ExecState {
    status_from_str(value.and_then(Value::as_str))
}

fn status_from_str(value: Option<&str>) -> ExecState {
    match value {
        Some("idle" | "done") => ExecState::Idle,
        Some("working") => ExecState::Working,
        Some("blocked") => ExecState::Blocked,
        _ => ExecState::Unknown,
    }
}

fn snapshot_entity_keys(snapshot: &Snapshot) -> HashSet<EntityKey> {
    snapshot
        .workspaces
        .iter()
        .map(|workspace| EntityKey::Workspace(workspace.workspace_id.clone()))
        .chain(
            snapshot
                .tabs
                .iter()
                .map(|tab| EntityKey::Tab(tab.tab_id.clone())),
        )
        .chain(
            snapshot
                .panes
                .iter()
                .map(|pane| EntityKey::Pane(pane.pane_id.clone())),
        )
        .collect()
}

fn record_replay_facts(
    index: usize,
    received: &ReceivedEvent,
    created: &mut HashSet<EntityKey>,
    closures: &mut HashMap<EntityKey, Vec<usize>>,
    candidates: &mut Vec<(usize, EntityKey)>,
) {
    for entity in created_entities(received) {
        created.insert(entity);
    }
    for entity in closed_entities(received) {
        closures.entry(entity).or_default().push(index);
    }
    if let Some(entity) = updated_entity(received) {
        candidates.push((index, entity));
    }
}

fn created_entities(received: &ReceivedEvent) -> Vec<EntityKey> {
    let mut entities = Vec::new();
    match received.event.as_str() {
        "workspace_created" => {
            nested_string(&received.data, "workspace", "workspace_id")
                .map(EntityKey::Workspace)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        "tab_created" => {
            nested_string(&received.data, "tab", "tab_id")
                .map(EntityKey::Tab)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        "pane_created" => {
            nested_string(&received.data, "pane", "pane_id")
                .map(EntityKey::Pane)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        "pane_moved" => {
            nested_string(&received.data, "created_tab", "tab_id")
                .map(EntityKey::Tab)
                .into_iter()
                .for_each(|entity| entities.push(entity));
            nested_string(&received.data, "pane", "pane_id")
                .map(EntityKey::Pane)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        _ => {}
    }
    entities
}

fn closed_entities(received: &ReceivedEvent) -> Vec<EntityKey> {
    let mut entities = Vec::new();
    match received.event.as_str() {
        "workspace_closed" => string_field(&received.data, "workspace_id")
            .map(EntityKey::Workspace)
            .into_iter()
            .for_each(|entity| entities.push(entity)),
        "tab_closed" => string_field(&received.data, "tab_id")
            .map(EntityKey::Tab)
            .into_iter()
            .for_each(|entity| entities.push(entity)),
        "pane_closed" | "pane_exited" => string_field(&received.data, "pane_id")
            .map(EntityKey::Pane)
            .into_iter()
            .for_each(|entity| entities.push(entity)),
        "pane_moved" => {
            string_field(&received.data, "previous_pane_id")
                .map(EntityKey::Pane)
                .into_iter()
                .for_each(|entity| entities.push(entity));
            string_field(&received.data, "closed_tab_id")
                .map(EntityKey::Tab)
                .into_iter()
                .for_each(|entity| entities.push(entity));
            string_field(&received.data, "closed_workspace_id")
                .map(EntityKey::Workspace)
                .into_iter()
                .for_each(|entity| entities.push(entity));
        }
        _ => {}
    }
    entities
}

fn updated_entity(received: &ReceivedEvent) -> Option<EntityKey> {
    match received.event.as_str() {
        "workspace_renamed" => nested_string(&received.data, "workspace", "workspace_id")
            .or_else(|| string_field(&received.data, "workspace_id"))
            .map(EntityKey::Workspace),
        "workspace_focused" => {
            string_field(&received.data, "workspace_id").map(EntityKey::Workspace)
        }
        "tab_focused" => string_field(&received.data, "tab_id").map(EntityKey::Tab),
        "pane_updated" | "pane_agent_detected" => {
            nested_string(&received.data, "pane", "pane_id").map(EntityKey::Pane)
        }
        "pane_focused" | "pane_agent_status_changed" => string_field(&received.data, "pane_id")
            .or_else(|| nested_string(&received.data, "pane", "pane_id"))
            .map(EntityKey::Pane),
        _ => None,
    }
}

fn entity_exists(shared: &SharedModel, entity: &EntityKey) -> bool {
    let model = shared.borrow();
    match entity {
        EntityKey::Workspace(workspace_id) => model.workspace(workspace_id).is_some(),
        EntityKey::Tab(tab_id) => model.tab(tab_id).is_some(),
        EntityKey::Pane(pane_id) => model.pane(pane_id).is_some(),
    }
}

fn nested_string(value: &Value, object: &str, field: &str) -> Option<String> {
    value
        .get(object)
        .and_then(|nested| string_field(nested, field))
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn decode_wire<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, CollectorError> {
    serde_json::from_value(value.clone())
        .map_err(|error| CollectorError::Wire(WireError::MalformedFrame(error.to_string())))
}

struct OwnerTracker {
    terminal_id: Option<String>,
    pane_id: Option<String>,
    started_at_ms: i64,
}

impl OwnerTracker {
    fn from_environment() -> Self {
        Self {
            terminal_id: nonempty_env("HERDR_TERMINAL_ID"),
            pane_id: nonempty_env("HERDR_PANE_ID"),
            started_at_ms: unix_now_ms(),
        }
    }

    fn record(&self) -> OwnerRecord {
        OwnerRecord {
            pid: std::process::id(),
            started_at_ms: self.started_at_ms,
            terminal_id: self.terminal_id.clone(),
            pane_id: self.pane_id.clone(),
        }
    }

    async fn refresh_from_snapshot(
        &mut self,
        snapshot: &Snapshot,
        writer: &WriterClient,
    ) -> Result<(), WriterError> {
        let pane =
            self.terminal_id
                .as_deref()
                .and_then(|terminal_id| {
                    snapshot
                        .panes
                        .iter()
                        .find(|pane| pane.terminal_id == terminal_id)
                })
                .or_else(|| {
                    self.pane_id.as_deref().and_then(|pane_id| {
                        snapshot.panes.iter().find(|pane| pane.pane_id == pane_id)
                    })
                })
                .or_else(|| {
                    snapshot.focused_pane_id.as_deref().and_then(|pane_id| {
                        snapshot.panes.iter().find(|pane| pane.pane_id == pane_id)
                    })
                });
        if let Some(pane) = pane {
            self.update(&pane.terminal_id, &pane.pane_id, writer)
                .await?;
        }
        Ok(())
    }

    async fn refresh_from_move(
        &mut self,
        data: &Value,
        writer: &WriterClient,
    ) -> Result<(), WriterError> {
        let Some(pane) = data.get("pane") else {
            return Ok(());
        };
        let Some(terminal_id) = string_field(pane, "terminal_id") else {
            return Ok(());
        };
        let Some(pane_id) = string_field(pane, "pane_id") else {
            return Ok(());
        };
        let previous_pane_id = string_field(data, "previous_pane_id");
        if self.terminal_id.as_deref() == Some(terminal_id.as_str())
            || self.pane_id.as_deref() == previous_pane_id.as_deref()
        {
            self.update(&terminal_id, &pane_id, writer).await?;
        }
        Ok(())
    }

    async fn update(
        &mut self,
        terminal_id: &str,
        pane_id: &str,
        writer: &WriterClient,
    ) -> Result<(), WriterError> {
        if self.terminal_id.as_deref() != Some(terminal_id)
            || self.pane_id.as_deref() != Some(pane_id)
        {
            writer.update_owner_location(terminal_id, pane_id).await?;
            self.terminal_id = Some(terminal_id.to_owned());
            self.pane_id = Some(pane_id.to_owned());
        }
        Ok(())
    }
}

fn nonempty_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

#[cfg(unix)]
fn socket_identity(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;

    std::fs::metadata(path)
        .ok()
        .map(|metadata| (metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn socket_identity(_path: &Path) -> Option<(u64, u64)> {
    None
}

async fn wait_or_cancel(cancellation: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        () = cancellation.cancelled() => true,
        () = tokio::time::sleep(duration) => false,
    }
}

fn unix_now_ms() -> i64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis().min(i64::MAX as u128) as i64
}
