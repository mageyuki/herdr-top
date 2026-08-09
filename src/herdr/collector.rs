//! T9 subscribe/buffer/snapshot/replay collector, convergence, and gap reconciliation.

use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::ffi::OsStr;
use std::future::pending;
use std::io;
use std::os::unix::net::UnixListener as StdUnixListener;
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
    AgentNodeObservation, AgentSessionReference, AgentSessionReferenceKind, DomainModel,
    EventMetadata, ExecState, Execution, GapKind, MinimalProviderMetadata, NormalizedEvent, Pane,
    PaneSnapshot, Provider, ReconcileBatch, RunId, RunKey, SharedModel, SnapshotAgent,
    SourceCoverage, Tab, TopologyEntity, TopologyEntityId, TopologySnapshot, Workspace,
};
use crate::provider::{
    BootstrapIdentity, BootstrapParser, DiscoveryIndex, DiscoveryRoot, FsReadBoundary,
    MergeOutcome, PendingEvents, ProviderCycle, ProviderEvent, ProviderSourceState,
    ProviderSpawnError, ProviderTarget, ProviderTargetPublisher, ProviderThreadError,
    ProviderThreadHandle, ProviderWorker, RecommendedNotifyFactory, TailFile, TargetSet,
    spawn_provider_thread,
};
use crate::reducer::{ApplyOutcome, Reducer, ReducerError};
use crate::store::writer::{WriterClient, WriterError};
use crate::store::{CollectorGap, PersistBatch, PersistOp, RestoredState};

use super::controller::{
    self, ControllerRequest, ControllerRequestReceiver, ControllerServerError,
};
use super::types::{AgentSessionKind, PaneInfo, Snapshot, Subscription, TabInfo, WorkspaceInfo};
use super::wire::{self, EventStream, WireError};

const EVENT_QUEUE_CAPACITY: usize = 64;
const RESNAPSHOT_ATTEMPTS: usize = 3;
const RECONNECT_DELAY: Duration = Duration::from_millis(50);
const DRAIN_QUIET_PERIOD: Duration = Duration::from_millis(5);
const STALE_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// A reducer rejected the transition before publication.
    #[error(transparent)]
    Reducer(#[from] ReducerError),
    /// The collector task panicked or was cancelled externally.
    #[error("collector task failed: {0}")]
    Task(String),
    /// The collector ignored cancellation until it had to be aborted and joined.
    #[error("collector did not stop within {seconds} seconds and was aborted")]
    StopTimeout { seconds: u64 },
    /// The Controller socket acceptor could not be started or stopped cleanly.
    #[error(transparent)]
    Controller(#[from] ControllerServerError),
    /// The Controller acceptor task panicked or was cancelled externally.
    #[error("Controller acceptor task failed: {0}")]
    ControllerTask(String),
    /// The provider I/O thread or notify backend could not be started.
    #[error(transparent)]
    ProviderSpawn(#[from] ProviderSpawnError),
    /// The provider I/O thread did not stop cleanly.
    #[error(transparent)]
    ProviderThread(#[from] ProviderThreadError),
}

/// Handle to the collector's coherent model and observation-quality streams.
pub struct CollectorHandle {
    /// Independently published Herdr observation quality.
    pub quality: watch::Receiver<ObservationQuality>,
    /// Coherent reducer-owned domain snapshots.
    pub model: SharedModel,
    cancellation: CancellationToken,
    task: JoinHandle<Result<(), CollectorError>>,
    controller_acceptor: Option<JoinHandle<Result<(), ControllerServerError>>>,
    provider_thread: Option<ProviderThreadHandle>,
}

impl CollectorHandle {
    /// Cancels the collector and waits for its subscription task to exit.
    pub async fn stop(self) -> Result<(), CollectorError> {
        self.stop_with_timeout(STOP_TIMEOUT).await
    }

    async fn stop_with_timeout(self, timeout: Duration) -> Result<(), CollectorError> {
        self.cancellation.cancel();
        let mut task = self.task;
        let collector_result = match tokio::time::timeout(timeout, &mut task).await {
            Ok(result) => result
                .map_err(|error| CollectorError::Task(error.to_string()))
                .and_then(std::convert::identity),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(CollectorError::StopTimeout {
                    seconds: timeout.as_secs(),
                })
            }
        };
        let controller_result = if let Some(mut acceptor) = self.controller_acceptor {
            match tokio::time::timeout(timeout, &mut acceptor).await {
                Ok(result) => result
                    .map_err(|error| CollectorError::ControllerTask(error.to_string()))
                    .and_then(|result| result.map_err(CollectorError::from)),
                Err(_) => {
                    acceptor.abort();
                    let _ = acceptor.await;
                    Err(CollectorError::StopTimeout {
                        seconds: timeout.as_secs(),
                    })
                }
            }
        } else {
            Ok(())
        };
        let provider_result = match self.provider_thread {
            Some(provider) => provider.stop().await.map_err(CollectorError::from),
            None => Ok(()),
        };

        collector_result?;
        controller_result?;
        provider_result
    }
}

/// Commits the new owner record, then launches subscribe-first convergence.
pub async fn spawn(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
) -> Result<CollectorHandle, CollectorError> {
    spawn_with_controller(sock, session, restored, writer, None).await
}

/// Commits the owner record and launches convergence plus an optional Controller acceptor.
pub async fn spawn_with_controller(
    sock: PathBuf,
    session: String,
    restored: RestoredState,
    writer: WriterClient,
    controller_listener: Option<StdUnixListener>,
) -> Result<CollectorHandle, CollectorError> {
    let owner = OwnerTracker::from_environment();
    writer.replace_owner(owner.record()).await?;

    let (reducer, model) = Reducer::new(restored);
    let (quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
    let cancellation = CancellationToken::new();
    let (controller_sender, controller_requests) =
        controller_listener.as_ref().map_or((None, None), |_| {
            let (sender, receiver) = controller::request_channel(
                controller::CONTROLLER_REQUEST_QUEUE_CAPACITY,
                reducer.controller_diagnostics_handle(),
            );
            (Some(sender), Some(receiver))
        });
    let mut controller_acceptor = match (controller_listener, controller_sender) {
        (Some(listener), Some(sender)) => Some(controller::spawn_acceptor(
            listener,
            sender,
            cancellation.clone(),
        )?),
        _ => None,
    };
    let (provider_sender, provider_events) = mpsc::channel(EVENT_QUEUE_CAPACITY);
    let provider_thread = match spawn_provider_thread(
        AdapterProviderWorker::default(),
        provider_sender,
        Some(Box::new(RecommendedNotifyFactory)),
    ) {
        Ok(handle) => handle,
        Err(error) => {
            cancellation.cancel();
            if let Some(mut acceptor) = controller_acceptor.take()
                && tokio::time::timeout(STOP_TIMEOUT, &mut acceptor)
                    .await
                    .is_err()
            {
                acceptor.abort();
                let _ = acceptor.await;
            }
            return Err(error.into());
        }
    };
    let provider_publisher = provider_thread.target_publisher();
    let restored_targets = derive_provider_targets(&model.borrow());
    provider_publisher.update_targets(restored_targets.clone());
    let provider_integration =
        ProviderIntegration::new(provider_events, provider_publisher, restored_targets);
    let task_cancellation = cancellation.clone();
    let task_model = model.clone();
    let task = tokio::spawn(async move {
        run_collector(
            sock,
            session,
            writer,
            reducer,
            task_model,
            quality_sender,
            task_cancellation,
            owner,
            controller_requests,
            provider_integration,
        )
        .await
    });

    Ok(CollectorHandle {
        quality,
        model,
        cancellation,
        task,
        controller_acceptor,
        provider_thread: Some(provider_thread),
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_collector(
    sock: PathBuf,
    session: String,
    writer: WriterClient,
    mut reducer: Reducer,
    shared: SharedModel,
    quality: watch::Sender<ObservationQuality>,
    cancellation: CancellationToken,
    mut owner: OwnerTracker,
    mut controller_requests: Option<ControllerRequestReceiver>,
    mut provider: ProviderIntegration,
) -> Result<(), CollectorError> {
    let mut first_subscription = true;
    let mut previous_socket = None;
    let mut retention_cleanup = tokio::time::interval(STALE_SWEEP_INTERVAL);
    retention_cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    retention_cleanup.tick().await;

    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }

        let socket_identity = socket_identity(&sock);
        let subscriptions = subscriptions();
        let stream = match tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            _ = retention_cleanup.tick() => {
                let _ = writer.cleanup(unix_now_ms()).await?;
                continue;
            }
            request = receive_controller(&mut controller_requests) => {
                service_controller(request, &mut controller_requests, &session, &mut reducer, &writer).await;
                provider.publish_targets(&shared);
                continue;
            }
            result = wire::subscribe(&sock, &subscriptions) => result,
        } {
            Ok(stream) => stream,
            Err(_) => {
                quality.send_replace(ObservationQuality::Disconnected);
                if wait_or_service_controller(
                    &cancellation,
                    RECONNECT_DELAY,
                    &mut controller_requests,
                    &session,
                    &mut reducer,
                    &writer,
                    &shared,
                    &mut provider,
                )
                .await?
                {
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
            &mut controller_requests,
            &mut provider,
        )
        .await;

        reader_cancellation.cancel();
        let reader_result = reader
            .await
            .map_err(|error| CollectorError::Task(error.to_string()))?;
        let outcome = outcome?;
        if outcome.gap_committed {
            first_subscription = false;
            previous_socket = socket_identity;
        }
        if let Err(error) = reader_result {
            quality.send_replace(ObservationQuality::Disconnected);
            if matches!(outcome.outcome, SubscriptionOutcome::Cancelled) {
                return Ok(());
            }
            let _ = error;
        }
        match outcome.outcome {
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
    controller_requests: &mut Option<ControllerRequestReceiver>,
    provider: &mut ProviderIntegration,
) -> Result<ConvergeOutcome, CollectorError> {
    let mut first_generation = true;
    let mut resnapshot_attempts = 0;
    let mut pending_closures = PendingTopologyClosures::default();

    loop {
        overflowed.store(false, Ordering::Release);
        let snapshot = tokio::select! {
            () = cancellation.cancelled() => return Ok(ConvergeOutcome::new(SubscriptionOutcome::Cancelled, !first_generation)),
            request = receive_controller(controller_requests) => {
                service_controller(request, controller_requests, session, reducer, writer).await;
                provider.publish_targets(shared);
                continue;
            }
            result = wire::request(sock, "session.snapshot", json!({})) => {
                match result.and_then(|value| value.into_snapshot()) {
                    Ok(snapshot) => snapshot,
                    Err(_) => return Ok(ConvergeOutcome::new(SubscriptionOutcome::Ended, !first_generation)),
                }
            }
        };
        let topology = topology_from_snapshot(&snapshot)?;
        let mut batch = if first_generation {
            pending_closures = PendingTopologyClosures::default();
            let mut batch = reducer.reconcile_gap(ReconcileBatch { topology, gap_kind })?;
            batch.push(PersistOp::RecordCollectorGap(CollectorGap {
                event_id: format!("collector-gap-{}", ulid::Ulid::new()),
                herdr_session: session.to_owned(),
                seen_at_ms: unix_now_ms(),
                kind: gap_kind,
            }));
            first_generation = false;
            batch
        } else {
            apply_snapshot_in_place(reducer, shared, topology, session, &mut pending_closures)?
        };
        writer.apply(std::mem::take(&mut batch)).await?;
        provider.publish_targets(shared);
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
            &mut pending_closures,
            controller_requests,
            provider,
        )
        .await?;
        match replay {
            ReplayOutcome::Cancelled => {
                return Ok(ConvergeOutcome::new(
                    SubscriptionOutcome::Cancelled,
                    !first_generation,
                ));
            }
            ReplayOutcome::Ended => {
                return Ok(ConvergeOutcome::new(
                    SubscriptionOutcome::Ended,
                    !first_generation,
                ));
            }
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
                    &mut pending_closures,
                    controller_requests,
                    provider,
                )
                .await?
                {
                    ReplayOutcome::Dirty => {
                        quality.send_replace(ObservationQuality::Reconciling);
                        resnapshot_attempts = 0;
                    }
                    ReplayOutcome::Ended => {
                        return Ok(ConvergeOutcome::new(
                            SubscriptionOutcome::Ended,
                            !first_generation,
                        ));
                    }
                    ReplayOutcome::Cancelled => {
                        return Ok(ConvergeOutcome::new(
                            SubscriptionOutcome::Cancelled,
                            !first_generation,
                        ));
                    }
                    ReplayOutcome::Clean => {}
                }
            }
            ReplayOutcome::Dirty => {
                quality.send_replace(ObservationQuality::Reconciling);
                if resnapshot_attempts == RESNAPSHOT_ATTEMPTS {
                    let outcome = monitor_reconciling(
                        reducer,
                        shared,
                        writer,
                        owner,
                        session,
                        events,
                        cancellation,
                        &mut pending_closures,
                        controller_requests,
                        provider,
                    )
                    .await?;
                    return Ok(ConvergeOutcome::new(outcome, !first_generation));
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
    pending_closures: &mut PendingTopologyClosures,
    controller_requests: &mut Option<ControllerRequestReceiver>,
    provider: &mut ProviderIntegration,
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
            apply_received_event(
                reducer,
                shared,
                writer,
                owner,
                session,
                received,
                pending_closures,
                provider,
            )
            .await?;
            next += 1;
            if drain_events(events, &mut buffered).is_err() {
                return Ok(ReplayOutcome::Ended);
            }
        }

        let next_received = tokio::select! {
            () = cancellation.cancelled() => return Ok(ReplayOutcome::Cancelled),
            request = receive_controller(controller_requests) => {
                service_controller(request, controller_requests, session, reducer, writer).await;
                provider.publish_targets(shared);
                continue;
            }
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
    pending_closures: &mut PendingTopologyClosures,
    controller_requests: &mut Option<ControllerRequestReceiver>,
    provider: &mut ProviderIntegration,
) -> Result<ReplayOutcome, CollectorError> {
    let mut stale_sweep = tokio::time::interval(STALE_SWEEP_INTERVAL);
    stale_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    stale_sweep.tick().await;
    loop {
        let received = tokio::select! {
            () = cancellation.cancelled() => return Ok(ReplayOutcome::Cancelled),
            request = receive_controller(controller_requests) => {
                service_controller(request, controller_requests, session, reducer, writer).await;
                provider.publish_targets(shared);
                continue;
            }
            event = receive_provider(&mut provider.events) => {
                service_provider_event(event, &mut provider.events, session, reducer, shared, writer).await?;
                provider.publish_targets(shared);
                continue;
            }
            received = events.recv() => match received {
                Some(received) => Some(received),
                None => return Ok(ReplayOutcome::Ended),
            },
            _ = stale_sweep.tick() => None,
        };
        let Some(received) = received else {
            let mut persist = reducer.sweep_stale(unix_now_ms());
            persist.extend(apply_pending_topology_closures(
                reducer,
                shared,
                session,
                pending_closures,
            )?);
            if !persist.is_empty() {
                writer.apply(persist).await?;
                provider.publish_targets(shared);
            }
            let _ = writer.cleanup(unix_now_ms()).await?;
            continue;
        };
        let anomalous =
            updated_entity(&received).is_some_and(|entity| !entity_exists(shared, &entity));
        apply_received_event(
            reducer,
            shared,
            writer,
            owner,
            session,
            received,
            pending_closures,
            provider,
        )
        .await?;
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
    pending_closures: &mut PendingTopologyClosures,
    controller_requests: &mut Option<ControllerRequestReceiver>,
    provider: &mut ProviderIntegration,
) -> Result<SubscriptionOutcome, CollectorError> {
    let mut stale_sweep = tokio::time::interval(STALE_SWEEP_INTERVAL);
    stale_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    stale_sweep.tick().await;
    loop {
        let received = tokio::select! {
            () = cancellation.cancelled() => return Ok(SubscriptionOutcome::Cancelled),
            request = receive_controller(controller_requests) => {
                service_controller(request, controller_requests, session, reducer, writer).await;
                provider.publish_targets(shared);
                continue;
            }
            event = receive_provider(&mut provider.events) => {
                service_provider_event(event, &mut provider.events, session, reducer, shared, writer).await?;
                provider.publish_targets(shared);
                continue;
            }
            received = events.recv() => match received {
                Some(received) => Some(received),
                None => return Ok(SubscriptionOutcome::Ended),
            },
            _ = stale_sweep.tick() => None,
        };
        let Some(received) = received else {
            let mut persist = reducer.sweep_stale(unix_now_ms());
            persist.extend(apply_pending_topology_closures(
                reducer,
                shared,
                session,
                pending_closures,
            )?);
            if !persist.is_empty() {
                writer.apply(persist).await?;
                provider.publish_targets(shared);
            }
            let _ = writer.cleanup(unix_now_ms()).await?;
            continue;
        };
        apply_received_event(
            reducer,
            shared,
            writer,
            owner,
            session,
            received,
            pending_closures,
            provider,
        )
        .await?;
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

#[derive(Debug)]
struct AdapterRootState {
    discovery: DiscoveryIndex,
    tails: HashMap<u32, TailFile>,
    bootstrap_emitted: HashSet<u32>,
}

impl AdapterRootState {
    fn new(provider: Provider, root: PathBuf) -> io::Result<Self> {
        Ok(Self {
            discovery: DiscoveryIndex::new(vec![DiscoveryRoot {
                provider,
                path: root,
            }])?,
            tails: HashMap::new(),
            bootstrap_emitted: HashSet::new(),
        })
    }
}

#[derive(Debug, Default)]
struct AdapterBootstrapParser {
    codex: crate::provider::codex::CodexBootstrapParser,
    claude: crate::provider::claude::ClaudeBootstrapParser,
}

impl BootstrapParser for AdapterBootstrapParser {
    fn parse_structural(
        &mut self,
        provider: Provider,
        relative_path: &Path,
        record: &[u8],
    ) -> Option<BootstrapIdentity> {
        match provider {
            Provider::Codex => self.codex.parse_structural(provider, relative_path, record),
            Provider::Claude => self
                .claude
                .parse_structural(provider, relative_path, record),
        }
    }
}

#[derive(Debug, Default)]
struct AdapterProviderWorker {
    roots: HashMap<(Provider, PathBuf), AdapterRootState>,
    deferred: VecDeque<ProviderEvent>,
}

impl ProviderWorker for AdapterProviderWorker {
    fn process(&mut self, cycle: &mut ProviderCycle<'_>) -> io::Result<()> {
        if !drain_deferred_provider_events(&mut self.deferred, cycle.pending) {
            return Ok(());
        }

        let mut targets_by_root: HashMap<(Provider, PathBuf), HashSet<PathBuf>> = HashMap::new();
        for target in cycle.targets.iter() {
            let root = provider_root_for_target(target.provider, &target.path)?;
            targets_by_root
                .entry((target.provider, root))
                .or_default()
                .insert(target.path.clone());
        }
        for provider in [Provider::Claude, Provider::Codex] {
            let state = if targets_by_root
                .keys()
                .any(|(current, _)| *current == provider)
            {
                ProviderSourceState::Available
            } else {
                ProviderSourceState::NotApplicable
            };
            let _ = cycle
                .pending
                .merge(ProviderEvent::SourceState { provider, state });
        }

        let mut roots = targets_by_root.into_iter().collect::<Vec<_>>();
        roots.sort_by(|left, right| {
            integration_provider_rank((left.0).0)
                .cmp(&integration_provider_rank((right.0).0))
                .then_with(|| (left.0).1.cmp(&(right.0).1))
        });
        for ((provider, root), targets) in roots {
            let state = match self.roots.entry((provider, root.clone())) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(AdapterRootState::new(provider, root.clone())?)
                }
            };
            if root.is_dir() {
                cycle.request_watch(root.clone());
            }
            let mut parser = AdapterBootstrapParser::default();
            state.discovery.scan(&mut parser)?;
            let files = state
                .discovery
                .files()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let owner_sessions = target_owner_sessions(&files, &targets);
            let mut relevant = files
                .into_iter()
                .filter(|file| {
                    let absolute = file.root.join(&file.relative_path);
                    targets.contains(&absolute)
                        || file.bootstrap.as_ref().is_some_and(|identity| {
                            identity.owner_session_id.as_ref().map_or_else(
                                || owner_sessions.contains(&identity.thread_id),
                                |id| owner_sessions.contains(id),
                            )
                        })
                })
                .collect::<Vec<_>>();
            relevant.sort_by_key(|file| file.path_id);
            let relevant_ids = relevant
                .iter()
                .map(|file| file.path_id)
                .collect::<HashSet<_>>();
            state
                .tails
                .retain(|path_id, _| relevant_ids.contains(path_id));
            state
                .bootstrap_emitted
                .retain(|path_id| relevant_ids.contains(path_id));

            for file in relevant {
                if let Some(parent) = file.root.join(&file.relative_path).parent()
                    && parent.is_dir()
                {
                    cycle.request_watch(parent.to_path_buf());
                }
                if !state.tails.contains_key(&file.path_id) {
                    let mut boundary = FsReadBoundary;
                    let tail = TailFile::open(
                        &file.root,
                        &file.relative_path,
                        state.discovery.baseline(),
                        &mut boundary,
                    )?;
                    state.tails.insert(file.path_id, tail);
                }
                let tail = state
                    .tails
                    .get_mut(&file.path_id)
                    .expect("tail inserted for relevant file");
                if state.bootstrap_emitted.insert(file.path_id) {
                    let event =
                        match provider {
                            Provider::Codex => crate::provider::codex::CodexAdapter
                                .bootstrap_event(&file, tail.generation(), unix_now_ms()),
                            Provider::Claude => crate::provider::claude::ClaudeAdapter
                                .bootstrap_event(&file, tail.generation(), unix_now_ms()),
                        };
                    if let Some(event) = event
                        && !merge_adapter_events(
                            std::iter::once(event),
                            cycle.pending,
                            &mut self.deferred,
                        )
                    {
                        return Ok(());
                    }
                }
                let mut boundary = FsReadBoundary;
                let generation = tail.generation();
                let mut records = tail.poll(&mut boundary)?.into_iter();
                while let Some(record) = records.next() {
                    let events = parse_adapter_record(
                        provider,
                        &state.discovery,
                        &file,
                        generation,
                        &record,
                    );
                    if !merge_adapter_events(events, cycle.pending, &mut self.deferred) {
                        self.deferred.extend(records.flat_map(|record| {
                            parse_adapter_record(
                                provider,
                                &state.discovery,
                                &file,
                                generation,
                                &record,
                            )
                        }));
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }
}

fn parse_adapter_record(
    provider: Provider,
    discovery: &DiscoveryIndex,
    file: &crate::provider::DiscoveredFile,
    generation: u64,
    record: &crate::provider::TailRecord,
) -> Vec<ProviderEvent> {
    match provider {
        Provider::Codex => {
            crate::provider::codex::CodexAdapter.parse_record(discovery, file, generation, record)
        }
        Provider::Claude => {
            crate::provider::claude::ClaudeAdapter.parse_record(discovery, file, generation, record)
        }
    }
}

fn target_owner_sessions(
    files: &[crate::provider::DiscoveredFile],
    targets: &HashSet<PathBuf>,
) -> HashSet<String> {
    files
        .iter()
        .filter(|file| targets.contains(&file.root.join(&file.relative_path)))
        .filter_map(|file| file.bootstrap.as_ref())
        .map(|identity| {
            identity
                .owner_session_id
                .clone()
                .unwrap_or_else(|| identity.thread_id.clone())
        })
        .collect()
}

fn drain_deferred_provider_events(
    deferred: &mut VecDeque<ProviderEvent>,
    pending: &mut PendingEvents,
) -> bool {
    while let Some(event) = deferred.pop_front() {
        match pending.merge(event) {
            MergeOutcome::AtCapacity(event) => {
                deferred.push_front(*event);
                return false;
            }
            MergeOutcome::Accepted | MergeOutcome::Coalesced | MergeOutcome::Duplicate => {}
        }
    }
    true
}

fn merge_adapter_events(
    events: impl IntoIterator<Item = ProviderEvent>,
    pending: &mut PendingEvents,
    deferred: &mut VecDeque<ProviderEvent>,
) -> bool {
    let mut events = events.into_iter();
    while let Some(event) = events.next() {
        match pending.merge(event) {
            MergeOutcome::AtCapacity(event) => {
                deferred.push_back(*event);
                deferred.extend(events);
                return false;
            }
            MergeOutcome::Accepted | MergeOutcome::Coalesced | MergeOutcome::Duplicate => {}
        }
    }
    true
}

fn provider_root_for_target(provider: Provider, target: &Path) -> io::Result<PathBuf> {
    if !target.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "provider target must be an absolute path",
        ));
    }
    let anchor = match provider {
        Provider::Codex => "sessions",
        Provider::Claude => "projects",
    };
    if let Some(root) = target.ancestors().find(|ancestor| {
        ancestor.file_name() == Some(OsStr::new(anchor))
            && ancestor.parent().is_some_and(|parent| {
                parent.file_name()
                    == Some(OsStr::new(match provider {
                        Provider::Codex => ".codex",
                        Provider::Claude => ".claude",
                    }))
            })
    }) {
        return Ok(root.to_path_buf());
    }
    target
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "provider target has no parent"))
}

const fn integration_provider_rank(provider: Provider) -> u8 {
    match provider {
        Provider::Claude => 0,
        Provider::Codex => 1,
    }
}

struct ProviderIntegration {
    events: Option<mpsc::Receiver<ProviderEvent>>,
    target_publisher: ProviderTargetPublisher,
    published_targets: TargetSet,
}

impl ProviderIntegration {
    fn new(
        events: mpsc::Receiver<ProviderEvent>,
        target_publisher: ProviderTargetPublisher,
        published_targets: TargetSet,
    ) -> Self {
        Self {
            events: Some(events),
            target_publisher,
            published_targets,
        }
    }

    fn publish_targets(&mut self, shared: &SharedModel) {
        let targets = derive_provider_targets(&shared.borrow());
        if targets != self.published_targets {
            self.target_publisher.update_targets(targets.clone());
            self.published_targets = targets;
        }
    }
}

fn derive_provider_targets(model: &DomainModel) -> TargetSet {
    let run_targets = model.task_runs().filter_map(|run| match &run.key {
        RunKey::NativePath { provider, path } if !path.is_empty() => Some(ProviderTarget {
            provider: *provider,
            path: PathBuf::from(path),
        }),
        RunKey::Controller(_)
        | RunKey::Native { .. }
        | RunKey::NativePath { .. }
        | RunKey::Provisional { .. } => None,
    });
    let node_targets = model.agent_nodes().filter_map(|node| {
        node.session_file
            .as_deref()
            .filter(|path| !path.is_empty())
            .map(|path| ProviderTarget {
                provider: node.provider,
                path: PathBuf::from(path),
            })
    });
    TargetSet::new(run_targets.chain(node_targets))
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

struct ConvergeOutcome {
    outcome: SubscriptionOutcome,
    gap_committed: bool,
}

#[derive(Default)]
struct PendingTopologyClosures {
    workspaces: HashSet<String>,
    tabs: HashSet<String>,
    panes: HashSet<String>,
}

fn cancel_pending_topology_closures(
    received: &ReceivedEvent,
    pending: &mut PendingTopologyClosures,
) {
    let mut reobserved = created_entities(received);
    reobserved.extend(updated_entity(received));
    for entity in reobserved {
        match entity {
            EntityKey::Workspace(workspace_id) => {
                pending.workspaces.remove(&workspace_id);
            }
            EntityKey::Tab(tab_id) => {
                pending.tabs.remove(&tab_id);
            }
            EntityKey::Pane(pane_id) => {
                pending.panes.remove(&pane_id);
            }
        }
    }
}

impl ConvergeOutcome {
    const fn new(outcome: SubscriptionOutcome, gap_committed: bool) -> Self {
        Self {
            outcome,
            gap_committed,
        }
    }
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

#[allow(clippy::too_many_arguments)]
async fn apply_received_event(
    reducer: &mut Reducer,
    shared: &SharedModel,
    writer: &WriterClient,
    owner: &mut OwnerTracker,
    session: &str,
    received: ReceivedEvent,
    pending_closures: &mut PendingTopologyClosures,
    provider: &mut ProviderIntegration,
) -> Result<(), CollectorError> {
    if received.event == "pane_moved" {
        owner.refresh_from_move(&received.data, writer).await?;
    }
    let normalized = normalize_event(shared, session, &received)?;
    let Some(persist) = apply_collector_observation(reducer, normalized)? else {
        return Ok(());
    };
    if !persist.is_empty() {
        writer.apply(persist).await?;
    }
    provider.publish_targets(shared);
    cancel_pending_topology_closures(&received, pending_closures);
    Ok(())
}

fn apply_collector_event(
    reducer: &mut Reducer,
    event: NormalizedEvent,
) -> Result<Option<PersistBatch>, ReducerError> {
    let outcome = reducer.apply(event)?;
    Ok(collector_apply_outcome(reducer, outcome))
}

fn apply_collector_observation(
    reducer: &mut Reducer,
    events: Vec<NormalizedEvent>,
) -> Result<Option<PersistBatch>, ReducerError> {
    let outcome = reducer.apply_observation(events)?;
    Ok(collector_apply_outcome(reducer, outcome))
}

async fn apply_provider_event(
    event: ProviderEvent,
    session: &str,
    reducer: &mut Reducer,
    shared: &SharedModel,
    writer: &WriterClient,
) -> Result<(), CollectorError> {
    let normalized = normalize_provider_event(shared, session, event);
    let identity_disagreement = normalized.identity_disagreement;
    let events = normalized
        .events
        .into_iter()
        .filter(|event| !writer.is_duplicate(&normalized_metadata(event).event_id))
        .collect::<Vec<_>>();
    if events.is_empty() {
        return Ok(());
    }
    let disagreement_is_new = identity_disagreement
        && events
            .iter()
            .any(|event| normalized_metadata(event).source_event_type == "session_resolved");
    if let Some(persist) = apply_collector_observation(reducer, events)?
        && !persist.is_empty()
    {
        writer.apply(persist).await?;
    }
    if disagreement_is_new {
        reducer.record_provider_identity_disagreement();
    }
    Ok(())
}

fn collector_apply_outcome(reducer: &mut Reducer, outcome: ApplyOutcome) -> Option<PersistBatch> {
    match outcome {
        ApplyOutcome::Applied(persist) => Some(persist),
        ApplyOutcome::DroppedBindingConflict(_conflict) => {
            reducer.record_binding_conflict();
            None
        }
    }
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
    let current: Vec<_> = shared
        .borrow()
        .executions()
        .filter(|execution| {
            execution.terminal_id == pane.terminal_id && !execution.state.is_terminal()
        })
        .cloned()
        .collect();
    let provider = pane_provider(&pane);
    let incoming_sid = pane.agent_session.as_ref().and_then(|reference| {
        (reference.kind == AgentSessionKind::Id && !reference.value.is_empty())
            .then_some(reference.value.as_str())
    });
    if pane.agent.is_none() {
        if incoming_sid.is_some() {
            for execution in current {
                let has_native = run_has_native_binding(shared, execution.task_run_id);
                let same_native = provider.zip(incoming_sid).is_some_and(|(provider, sid)| {
                    run_owns_native(shared, execution.task_run_id, provider, sid)
                });
                if has_native && !same_native {
                    // An agent-less pane carries no liveness that could justify a
                    // replacement run, so conflicting latched evidence is dropped.
                    tracing::warn!(
                        terminal_id = pane.terminal_id.as_str(),
                        "skipped conflicting agent-session evidence from an agent-less pane update"
                    );
                    continue;
                }
                events.push(NormalizedEvent::ExecutionBegin {
                    metadata: pane_metadata(session, event_kind, &pane),
                    execution,
                });
            }
        }
        return Ok(());
    }
    let mut reused = false;
    for execution in current {
        let has_native = run_has_native_binding(shared, execution.task_run_id);
        let same_native = provider.zip(incoming_sid).is_some_and(|(provider, sid)| {
            run_owns_native(shared, execution.task_run_id, provider, sid)
        });
        let should_reuse = incoming_sid.is_none() || same_native || !has_native;
        if should_reuse && !reused {
            let mut updated = execution;
            updated.pane_id.clone_from(&pane.pane_id);
            updated.state = status_from_str(pane.agent_status.as_deref());
            events.push(NormalizedEvent::ExecutionBegin {
                metadata: pane_metadata(session, event_kind, &pane),
                execution: updated,
            });
            reused = true;
        } else {
            events.push(NormalizedEvent::ExecutionEnd {
                metadata: metadata(session, event_kind),
                execution_id: execution.execution_id,
            });
        }
    }
    if !reused {
        events.push(execution_begin(session, event_kind, &pane));
    }
    Ok(())
}

fn run_has_native_binding(shared: &SharedModel, run_id: RunId) -> bool {
    let model = shared.borrow();
    model
        .task_run_bindings()
        .any(|(key, owner)| *owner == run_id && matches!(key, crate::model::RunKey::Native { .. }))
        || model.agent_nodes().any(|node| {
            node.task_run_id == run_id
                && node
                    .native_session_id
                    .as_ref()
                    .is_some_and(|sid| !sid.is_empty())
        })
}

fn run_owns_native(shared: &SharedModel, run_id: RunId, provider: Provider, sid: &str) -> bool {
    let model = shared.borrow();
    model
        .task_run_by_key(&crate::model::RunKey::Native {
            provider,
            sid: sid.to_owned(),
        })
        .is_some_and(|run| run.run_id == run_id)
        || model.agent_nodes().any(|node| {
            node.task_run_id == run_id
                && node.provider == provider
                && node.native_session_id.as_deref() == Some(sid)
        })
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
    pending_closures: &mut PendingTopologyClosures,
) -> Result<PersistBatch, ReducerError> {
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
        if let Some(batch) = apply_collector_event(
            reducer,
            topology_upsert(
                session,
                "snapshot_workspace",
                TopologyEntity::Workspace(workspace.clone()),
            ),
        )? {
            persist.extend(batch);
        }
    }
    for tab in &topology.tabs {
        if let Some(batch) = apply_collector_event(
            reducer,
            topology_upsert(session, "snapshot_tab", TopologyEntity::Tab(tab.clone())),
        )? {
            persist.extend(batch);
        }
    }
    for pane in &topology.panes {
        let mut observation = vec![topology_upsert(
            session,
            "snapshot_pane",
            TopologyEntity::Pane(Pane {
                pane_id: pane.pane_id.clone(),
                workspace_id: pane.workspace_id.clone(),
                tab_id: pane.tab_id.clone(),
                terminal_id: pane.terminal_id.clone(),
            }),
        )];
        let provider = pane.agent.as_ref().and_then(|agent| {
            snapshot_provider_name(
                &agent.agent_name,
                pane.agent_session
                    .as_ref()
                    .map(|session| session.agent.as_str()),
            )
        });
        let incoming_sid = pane.agent_session.as_ref().and_then(|reference| {
            (reference.kind == AgentSessionReferenceKind::Id && !reference.value.is_empty())
                .then_some(reference.value.as_str())
        });
        let current: Vec<_> = shared
            .borrow()
            .executions()
            .filter(|execution| {
                execution.terminal_id == pane.terminal_id && !execution.state.is_terminal()
            })
            .cloned()
            .collect();
        let mut reused = false;
        if let Some(agent) = &pane.agent {
            for mut execution in current {
                let has_native = run_has_native_binding(shared, execution.task_run_id);
                let same_native = provider.zip(incoming_sid).is_some_and(|(provider, sid)| {
                    run_owns_native(shared, execution.task_run_id, provider, sid)
                });
                let should_reuse = incoming_sid.is_none() || same_native || !has_native;
                if should_reuse && !reused {
                    execution.pane_id.clone_from(&pane.pane_id);
                    execution.state = agent.state.clone();
                    observation.push(NormalizedEvent::ExecutionBegin {
                        metadata: snapshot_pane_metadata(session, "snapshot_execution", pane),
                        execution,
                    });
                    reused = true;
                } else {
                    observation.push(NormalizedEvent::ExecutionEnd {
                        metadata: metadata(session, "snapshot_different_session"),
                        execution_id: execution.execution_id,
                    });
                }
            }
            if !reused {
                observation.push(snapshot_execution_begin(session, pane, agent));
            }
        }
        if let Some(batch) = apply_collector_observation(reducer, observation)? {
            persist.extend(batch);
        }
    }

    let stale_ids: Vec<_> = shared
        .borrow()
        .executions()
        .filter(|execution| {
            !execution.state.is_terminal()
                && !topology
                    .panes
                    .iter()
                    .any(|pane| pane.terminal_id == execution.terminal_id && pane.agent.is_some())
        })
        .map(|execution| execution.execution_id.clone())
        .collect();
    for execution_id in stale_ids {
        if let Some(batch) = apply_collector_event(
            reducer,
            NormalizedEvent::AgentStatusChanged {
                metadata: metadata(session, "snapshot_execution_missing"),
                execution_id,
                state: ExecState::Stale { since_ms: 0 },
            },
        )? {
            persist.extend(batch);
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
    pending_closures.panes = old_panes.iter().cloned().collect();
    pending_closures.tabs = old_tabs.iter().cloned().collect();
    pending_closures.workspaces = old_workspaces.iter().cloned().collect();
    for pane_id in old_panes {
        let in_grace = shared.borrow().executions().any(|execution| {
            execution.pane_id == pane_id && matches!(execution.state, ExecState::Stale { .. })
        });
        if !in_grace {
            if let Some(batch) = apply_collector_event(
                reducer,
                topology_closure(
                    session,
                    "snapshot_pane_missing",
                    TopologyEntityId::Pane {
                        pane_id: pane_id.clone(),
                    },
                ),
            )? {
                persist.extend(batch);
            }
            pending_closures.panes.remove(&pane_id);
        }
    }
    for tab_id in old_tabs {
        if !shared.borrow().panes().any(|pane| pane.tab_id == tab_id) {
            if let Some(batch) = apply_collector_event(
                reducer,
                topology_closure(
                    session,
                    "snapshot_tab_missing",
                    TopologyEntityId::Tab {
                        tab_id: tab_id.clone(),
                    },
                ),
            )? {
                persist.extend(batch);
            }
            pending_closures.tabs.remove(&tab_id);
        }
    }
    for workspace_id in old_workspaces {
        if !shared
            .borrow()
            .tabs()
            .any(|tab| tab.workspace_id == workspace_id)
        {
            if let Some(batch) = apply_collector_event(
                reducer,
                topology_closure(
                    session,
                    "snapshot_workspace_missing",
                    TopologyEntityId::Workspace {
                        workspace_id: workspace_id.clone(),
                    },
                ),
            )? {
                persist.extend(batch);
            }
            pending_closures.workspaces.remove(&workspace_id);
        }
    }
    Ok(persist)
}

fn apply_pending_topology_closures(
    reducer: &mut Reducer,
    shared: &SharedModel,
    session: &str,
    pending: &mut PendingTopologyClosures,
) -> Result<PersistBatch, ReducerError> {
    let mut persist = Vec::new();
    let pane_ids: Vec<_> = pending.panes.iter().cloned().collect();
    for pane_id in pane_ids {
        let has_live_execution = shared
            .borrow()
            .executions()
            .any(|execution| execution.pane_id == pane_id && !execution.state.is_terminal());
        if !has_live_execution {
            if shared.borrow().pane(&pane_id).is_some()
                && let Some(batch) = apply_collector_event(
                    reducer,
                    topology_closure(
                        session,
                        "stale_grace_expired_pane",
                        TopologyEntityId::Pane {
                            pane_id: pane_id.clone(),
                        },
                    ),
                )?
            {
                persist.extend(batch);
            }
            pending.panes.remove(&pane_id);
        }
    }

    let tab_ids: Vec<_> = pending.tabs.iter().cloned().collect();
    for tab_id in tab_ids {
        if !shared.borrow().panes().any(|pane| pane.tab_id == tab_id) {
            if shared.borrow().tab(&tab_id).is_some()
                && let Some(batch) = apply_collector_event(
                    reducer,
                    topology_closure(
                        session,
                        "stale_grace_expired_tab",
                        TopologyEntityId::Tab {
                            tab_id: tab_id.clone(),
                        },
                    ),
                )?
            {
                persist.extend(batch);
            }
            pending.tabs.remove(&tab_id);
        }
    }

    let workspace_ids: Vec<_> = pending.workspaces.iter().cloned().collect();
    for workspace_id in workspace_ids {
        if !shared
            .borrow()
            .tabs()
            .any(|tab| tab.workspace_id == workspace_id)
        {
            if shared.borrow().workspace(&workspace_id).is_some()
                && let Some(batch) = apply_collector_event(
                    reducer,
                    topology_closure(
                        session,
                        "stale_grace_expired_workspace",
                        TopologyEntityId::Workspace {
                            workspace_id: workspace_id.clone(),
                        },
                    ),
                )?
            {
                persist.extend(batch);
            }
            pending.workspaces.remove(&workspace_id);
        }
    }
    Ok(persist)
}

fn snapshot_provider_name(agent_name: &str, session_agent: Option<&str>) -> Option<Provider> {
    session_agent
        .and_then(provider_from_name)
        .or_else(|| provider_from_name(agent_name))
}

fn snapshot_pane_metadata(session: &str, kind: &str, pane: &PaneSnapshot) -> EventMetadata {
    let mut value = metadata(session, kind);
    value.workspace_id = Some(pane.workspace_id.clone());
    value.tab_id = Some(pane.tab_id.clone());
    value.pane_id = Some(pane.pane_id.clone());
    value.terminal_id = Some(pane.terminal_id.clone());
    value.provider = pane.agent.as_ref().and_then(|agent| {
        snapshot_provider_name(
            &agent.agent_name,
            pane.agent_session
                .as_ref()
                .map(|session| session.agent.as_str()),
        )
    });
    value.native_session_id = pane.agent_session.as_ref().and_then(|reference| {
        (reference.kind == AgentSessionReferenceKind::Id && !reference.value.is_empty())
            .then(|| reference.value.clone())
    });
    value
}

fn snapshot_execution_begin(
    session: &str,
    pane: &PaneSnapshot,
    agent: &SnapshotAgent,
) -> NormalizedEvent {
    NormalizedEvent::ExecutionBegin {
        metadata: snapshot_pane_metadata(session, "snapshot_execution_discovered", pane),
        execution: Execution {
            execution_id: format!("herdr-execution-{}", ulid::Ulid::new()),
            pane_id: pane.pane_id.clone(),
            terminal_id: pane.terminal_id.clone(),
            task_run_id: RunId::new(),
            state: agent.state.clone(),
        },
    }
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
    let receipt_time_ms = unix_now_ms();
    EventMetadata {
        event_id: format!("herdr-event-{}", ulid::Ulid::new()),
        timestamp_ms: receipt_time_ms,
        receipt_time_ms,
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
        label: None,
        reason: None,
        progress: None,
        ingest_seq: None,
    }
}

struct NormalizedProviderObservation {
    events: Vec<NormalizedEvent>,
    identity_disagreement: bool,
}

fn normalize_provider_event(
    shared: &SharedModel,
    session: &str,
    event: ProviderEvent,
) -> NormalizedProviderObservation {
    match event {
        ProviderEvent::SessionResolved {
            provider,
            agent_thread_id,
            owner_session_id,
            parent_thread_id,
            path,
            model_id,
            event_id,
            observed_at_ms,
            ..
        } => {
            let path_text = path.to_string_lossy().into_owned();
            let model = shared.borrow();
            let path_key = RunKey::NativePath {
                provider,
                path: path_text.clone(),
            };
            let path_run = model.task_run_by_key(&path_key).map(|run| run.run_id);
            let file_run = model
                .agent_nodes()
                .filter(|node| {
                    node.provider == provider
                        && node.session_file.as_deref() == Some(path_text.as_str())
                })
                .map(|node| node.task_run_id)
                .min();
            let owner_run = owner_session_id.as_deref().and_then(|sid| {
                model
                    .task_run_by_key(&RunKey::Native {
                        provider,
                        sid: sid.to_owned(),
                    })
                    .map(|run| run.run_id)
            });
            let Some(run_id) = path_run.or(file_run).or(owner_run) else {
                return NormalizedProviderObservation {
                    events: Vec::new(),
                    identity_disagreement: false,
                };
            };
            let identity_disagreement = owner_session_id.as_deref().is_some_and(|resolved_sid| {
                matches!(
                    model.task_run(&run_id).map(|run| &run.key),
                    Some(RunKey::Native { provider: current_provider, sid })
                        if *current_provider == provider && sid != resolved_sid
                )
            });
            drop(model);

            let node_id = deterministic_agent_node_id(provider, &agent_thread_id);
            let parent_node_id = parent_thread_id
                .as_deref()
                .map(|parent| deterministic_agent_node_id(provider, parent));
            let provider_metadata = MinimalProviderMetadata {
                agent_id: Some(agent_thread_id.clone()),
                parent_agent_id: parent_thread_id.clone(),
                model_id: model_id.clone(),
                event_kind: Some("session_resolved".to_owned()),
                ..MinimalProviderMetadata::default()
            };
            let mut events = Vec::new();

            let mut node_metadata = provider_metadata_for(
                session,
                provider,
                format!("prov:{}:node:{agent_thread_id}", provider_name(provider)),
                "agent_node",
                observed_at_ms,
                run_id,
                &node_id,
                provider_metadata.clone(),
            );
            node_metadata.native_session_id = None;
            events.push(NormalizedEvent::AgentNodeUpsert {
                metadata: node_metadata,
                node: AgentNodeObservation {
                    agent_node_id: node_id.clone(),
                    provider,
                    native_session_id: Some(agent_thread_id.clone()),
                    task_run_id: run_id,
                    parent_agent_node_id: None,
                    state: None,
                    model_id: None,
                    session_file: None,
                },
            });

            if let Some(parent_agent_node_id) = parent_node_id {
                let link_metadata = provider_metadata_for(
                    session,
                    provider,
                    format!(
                        "prov:{}:link:{agent_thread_id}:{}",
                        provider_name(provider),
                        parent_thread_id.as_deref().expect("parent ID exists")
                    ),
                    "agent_parent_link",
                    observed_at_ms,
                    run_id,
                    &node_id,
                    provider_metadata.clone(),
                );
                events.push(NormalizedEvent::AgentNodeUpsert {
                    metadata: link_metadata,
                    node: AgentNodeObservation {
                        agent_node_id: node_id.clone(),
                        provider,
                        native_session_id: Some(agent_thread_id.clone()),
                        task_run_id: run_id,
                        parent_agent_node_id: Some(parent_agent_node_id),
                        state: None,
                        model_id: None,
                        session_file: None,
                    },
                });
            }

            let mut resolved_metadata = provider_metadata_for(
                session,
                provider,
                event_id,
                "session_resolved",
                observed_at_ms,
                run_id,
                &node_id,
                provider_metadata,
            );
            if !identity_disagreement {
                resolved_metadata.native_session_id = owner_session_id;
            }
            events.push(NormalizedEvent::AgentNodeUpsert {
                metadata: resolved_metadata,
                node: AgentNodeObservation {
                    agent_node_id: node_id,
                    provider,
                    native_session_id: Some(agent_thread_id),
                    task_run_id: run_id,
                    parent_agent_node_id: None,
                    state: None,
                    model_id,
                    session_file: Some(path_text),
                },
            });
            NormalizedProviderObservation {
                events,
                identity_disagreement,
            }
        }
        ProviderEvent::AgentUpsert {
            provider,
            agent_thread_id,
            owner_session_id,
            parent_thread_id,
            state,
            model_id,
            event_id,
            observed_at_ms,
            ..
        } => {
            let model = shared.borrow();
            let owner_run = owner_session_id.as_deref().and_then(|sid| {
                model
                    .task_run_by_key(&RunKey::Native {
                        provider,
                        sid: sid.to_owned(),
                    })
                    .map(|run| run.run_id)
            });
            let node_run = model
                .agent_nodes()
                .find(|node| {
                    node.provider == provider
                        && node.native_session_id.as_deref() == Some(agent_thread_id.as_str())
                })
                .map(|node| node.task_run_id);
            let Some(run_id) = owner_run.or(node_run) else {
                return NormalizedProviderObservation {
                    events: Vec::new(),
                    identity_disagreement: false,
                };
            };
            drop(model);
            let node_id = deterministic_agent_node_id(provider, &agent_thread_id);
            let parent_agent_node_id = parent_thread_id
                .as_deref()
                .map(|parent| deterministic_agent_node_id(provider, parent));
            let provider_metadata = MinimalProviderMetadata {
                agent_id: Some(agent_thread_id.clone()),
                parent_agent_id: parent_thread_id,
                model_id: model_id.clone(),
                event_kind: Some("agent_upsert".to_owned()),
                ..MinimalProviderMetadata::default()
            };
            let metadata = provider_metadata_for(
                session,
                provider,
                event_id,
                "agent_upsert",
                observed_at_ms,
                run_id,
                &node_id,
                provider_metadata,
            );
            NormalizedProviderObservation {
                events: vec![NormalizedEvent::AgentNodeUpsert {
                    metadata,
                    node: AgentNodeObservation {
                        agent_node_id: node_id,
                        provider,
                        native_session_id: Some(agent_thread_id),
                        task_run_id: run_id,
                        parent_agent_node_id,
                        state,
                        model_id,
                        session_file: None,
                    },
                }],
                identity_disagreement: false,
            }
        }
        ProviderEvent::Activity {
            provider,
            agent_thread_id,
            activity,
            event_id,
            observed_at_ms,
            ..
        } => {
            let model = shared.borrow();
            let node = model
                .agent_nodes()
                .filter(|node| {
                    node.provider == provider
                        && node.native_session_id.as_deref() == Some(agent_thread_id.as_str())
                })
                .min_by_key(|node| node.agent_node_id.as_str())
                .cloned();
            let Some(node) = node else {
                return NormalizedProviderObservation {
                    events: Vec::new(),
                    identity_disagreement: false,
                };
            };
            drop(model);
            let metadata = provider_metadata_for(
                session,
                provider,
                event_id,
                "activity",
                observed_at_ms,
                node.task_run_id,
                &node.agent_node_id,
                activity.clone(),
            );
            NormalizedProviderObservation {
                events: vec![NormalizedEvent::AgentActivity {
                    metadata,
                    agent_node_id: node.agent_node_id,
                    activity,
                }],
                identity_disagreement: false,
            }
        }
        ProviderEvent::SourceState { .. } | ProviderEvent::Malformed { .. } => {
            NormalizedProviderObservation {
                events: Vec::new(),
                identity_disagreement: false,
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn provider_metadata_for(
    session: &str,
    provider: Provider,
    event_id: String,
    kind: &str,
    observed_at_ms: i64,
    run_id: RunId,
    agent_node_id: &str,
    provider_metadata: MinimalProviderMetadata,
) -> EventMetadata {
    EventMetadata {
        event_id,
        timestamp_ms: observed_at_ms,
        receipt_time_ms: unix_now_ms(),
        source: "provider".to_owned(),
        source_event_type: kind.to_owned(),
        herdr_session: session.to_owned(),
        workspace_id: None,
        tab_id: None,
        pane_id: None,
        terminal_id: None,
        provider: Some(provider),
        native_session_id: None,
        task_run_id: Some(run_id),
        agent_node_id: Some(agent_node_id.to_owned()),
        task_state: None,
        execution_parent: None,
        dependency: None,
        source_coverage: vec![SourceCoverage {
            source: provider_name(provider).to_owned(),
            available: true,
            detail: None,
        }],
        provider_metadata: Some(provider_metadata),
        label: None,
        reason: None,
        progress: None,
        ingest_seq: None,
    }
}

fn normalized_metadata(event: &NormalizedEvent) -> &EventMetadata {
    match event {
        NormalizedEvent::ControllerEvent { metadata, .. }
        | NormalizedEvent::TopologyUpsert { metadata, .. }
        | NormalizedEvent::TopologyClosure { metadata, .. }
        | NormalizedEvent::AgentStatusChanged { metadata, .. }
        | NormalizedEvent::AgentNodeUpsert { metadata, .. }
        | NormalizedEvent::AgentActivity { metadata, .. }
        | NormalizedEvent::ExecutionBegin { metadata, .. }
        | NormalizedEvent::ExecutionEnd { metadata, .. } => metadata,
    }
}

fn deterministic_agent_node_id(provider: Provider, thread_id: &str) -> String {
    format!("agent:{}:{thread_id}", provider_name(provider))
}

const fn provider_name(provider: Provider) -> &'static str {
    match provider {
        Provider::Claude => "claude",
        Provider::Codex => "codex",
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

async fn receive_controller(
    receiver: &mut Option<ControllerRequestReceiver>,
) -> Option<ControllerRequest> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => pending().await,
    }
}

async fn receive_provider(
    receiver: &mut Option<mpsc::Receiver<ProviderEvent>>,
) -> Option<ProviderEvent> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => pending().await,
    }
}

async fn service_provider_event(
    event: Option<ProviderEvent>,
    receiver: &mut Option<mpsc::Receiver<ProviderEvent>>,
    session: &str,
    reducer: &mut Reducer,
    shared: &SharedModel,
    writer: &WriterClient,
) -> Result<(), CollectorError> {
    match event {
        Some(event) => apply_provider_event(event, session, reducer, shared, writer).await,
        None => {
            *receiver = None;
            Ok(())
        }
    }
}

async fn service_controller(
    request: Option<ControllerRequest>,
    receiver: &mut Option<ControllerRequestReceiver>,
    session: &str,
    reducer: &mut Reducer,
    writer: &WriterClient,
) {
    match request {
        Some(request) => controller::service_request(request, session, reducer, writer).await,
        None => *receiver = None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_or_service_controller(
    cancellation: &CancellationToken,
    duration: Duration,
    receiver: &mut Option<ControllerRequestReceiver>,
    session: &str,
    reducer: &mut Reducer,
    writer: &WriterClient,
    shared: &SharedModel,
    provider: &mut ProviderIntegration,
) -> Result<bool, CollectorError> {
    let delay = tokio::time::sleep(duration);
    tokio::pin!(delay);
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return Ok(true),
            () = &mut delay => return Ok(false),
            request = receive_controller(receiver) => {
                service_controller(request, receiver, session, reducer, writer).await;
                provider.publish_targets(shared);
            }
            event = receive_provider(&mut provider.events) => {
                service_provider_event(
                    event,
                    &mut provider.events,
                    session,
                    reducer,
                    shared,
                    writer,
                ).await?;
                provider.publish_targets(shared);
            }
        }
    }
}

fn unix_now_ms() -> i64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_millis().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod provider_integration_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::sync::watch;

    use super::*;
    use crate::lockfile::StateRoot;
    use crate::model::{
        AgentNode, DisplayOrdinal, DomainModel, ExecState, ExecutionEdge, MinimalProviderMetadata,
        RunKey, TaskRun, TaskState,
    };
    use crate::provider::{ProviderCycle, ProviderEvent, ProviderWorker, SourcePosition};
    use crate::store::{
        NativeSessionBinding, PersistOp, PersistTaskRun, open_reader, open_writer, spawn_writer,
    };

    #[test]
    fn session_resolution_normalizes_stable_node_link_and_meta_effects() {
        let run_id = RunId::new();
        let path = "/tmp/provider/root.jsonl";
        let mut model = DomainModel::default();
        model.insert_task_run(TaskRun {
            run_id,
            key: RunKey::NativePath {
                provider: Provider::Codex,
                path: path.to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: false,
        });
        let (_sender, shared) = watch::channel(Arc::new(model));
        let event = ProviderEvent::SessionResolved {
            provider: Provider::Codex,
            agent_thread_id: "child".to_owned(),
            owner_session_id: Some("owner".to_owned()),
            parent_thread_id: Some("parent".to_owned()),
            path: PathBuf::from(path),
            model_id: Some("gpt-test".to_owned()),
            depth: Some(1),
            event_id: "prov:codex:meta:child".to_owned(),
            observed_at_ms: 123,
            position: SourcePosition {
                path_id: 1,
                generation: 0,
                offset: 2,
            },
        };

        let normalized = normalize_provider_event(&shared, "session", event);
        let ids = normalized
            .events
            .iter()
            .map(|event| super::normalized_metadata(event).event_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            ids,
            [
                "prov:codex:node:child",
                "prov:codex:link:child:parent",
                "prov:codex:meta:child",
            ]
        );
        assert!(!normalized.identity_disagreement);
        let metadata = super::normalized_metadata(normalized.events.last().unwrap());
        assert_eq!(metadata.task_run_id, Some(run_id));
        assert_eq!(metadata.native_session_id.as_deref(), Some("owner"));
        assert_eq!(metadata.source_coverage.len(), 1);
        assert_eq!(metadata.source_coverage[0].source, "codex");
    }

    struct DropObservedWorker(Arc<AtomicBool>);

    impl ProviderWorker for DropObservedWorker {
        fn process(&mut self, _cycle: &mut ProviderCycle<'_>) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Drop for DropObservedWorker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn collector_timeout_still_stops_and_joins_the_provider_thread() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (provider_sender, _provider_events) = mpsc::channel(1);
        let provider_thread = spawn_provider_thread(
            DropObservedWorker(Arc::clone(&dropped)),
            provider_sender,
            None,
        )
        .unwrap();
        let (_quality_sender, quality) = watch::channel(ObservationQuality::Reconciling);
        let (_reducer, model) = Reducer::new(RestoredState {
            model: DomainModel::default(),
            next_ordinal: 1,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        });
        let handle = CollectorHandle {
            quality,
            model,
            cancellation: CancellationToken::new(),
            task: tokio::spawn(async {
                std::future::pending::<()>().await;
                Ok(())
            }),
            controller_acceptor: None,
            provider_thread: Some(provider_thread),
        };

        assert!(matches!(
            handle.stop_with_timeout(Duration::from_millis(10)).await,
            Err(CollectorError::StopTimeout { .. })
        ));
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn provider_fallback_promotes_path_and_duplicate_payload_is_first_wins() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let path = directory.path().join("provider/root.jsonl");
        let run_id = RunId::new();
        let task_run = TaskRun {
            run_id,
            key: RunKey::NativePath {
                provider: Provider::Codex,
                path: path.to_string_lossy().into_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::EndedUnknown,
            has_controller_task_state_event: false,
        };
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![PersistOp::UpsertTaskRun(PersistTaskRun {
                task_run,
                native_session: None,
                created_at_ms: 1,
                updated_at_ms: 1,
                finished_at_ms: None,
            })])
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored);

        apply_provider_event(
            ProviderEvent::SessionResolved {
                provider: Provider::Codex,
                agent_thread_id: "owner".to_owned(),
                owner_session_id: Some("owner".to_owned()),
                parent_thread_id: None,
                path: path.clone(),
                model_id: Some("gpt-first".to_owned()),
                depth: Some(0),
                event_id: "prov:codex:meta:owner".to_owned(),
                observed_at_ms: 100,
                position: SourcePosition {
                    path_id: 1,
                    generation: 0,
                    offset: 0,
                },
            },
            "session",
            &mut reducer,
            &shared,
            &writer,
        )
        .await
        .unwrap();
        let activity = |kind: &str| ProviderEvent::Activity {
            provider: Provider::Codex,
            agent_thread_id: "owner".to_owned(),
            activity: MinimalProviderMetadata {
                agent_id: Some("owner".to_owned()),
                event_kind: Some(kind.to_owned()),
                ..MinimalProviderMetadata::default()
            },
            event_id: "prov:codex:act:same".to_owned(),
            observed_at_ms: 200,
            position: SourcePosition {
                path_id: 1,
                generation: 0,
                offset: 10,
            },
        };
        apply_provider_event(activity("first"), "session", &mut reducer, &shared, &writer)
            .await
            .unwrap();
        apply_provider_event(
            activity("different-second"),
            "session",
            &mut reducer,
            &shared,
            &writer,
        )
        .await
        .unwrap();

        {
            let model = shared.borrow();
            assert_eq!(
                model.task_run(&run_id).unwrap().key,
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "owner".to_owned(),
                }
            );
            assert_eq!(
                model.task_run(&run_id).unwrap().state,
                TaskState::EndedUnknown
            );
            assert_eq!(
                model
                    .agent_node("agent:codex:owner")
                    .unwrap()
                    .last_event_kind
                    .as_deref(),
                Some("first")
            );
            let node = model.agent_node("agent:codex:owner").unwrap();
            assert_eq!(node.model_id.as_deref(), Some("gpt-first"));
            assert_eq!(node.session_file.as_deref(), path.to_str());
        }
        lifecycle.shutdown().await.unwrap();

        let restored = open_reader(&root).unwrap().load_restored_state().unwrap();
        assert_eq!(
            restored.model.task_run(&run_id).unwrap().key,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "owner".to_owned(),
            }
        );
        assert_eq!(
            restored
                .model
                .agent_node("agent:codex:owner")
                .unwrap()
                .last_event_kind
                .as_deref(),
            Some("first")
        );
    }

    #[tokio::test]
    async fn herdr_id_disagreement_is_counted_and_never_corroborates() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let path = directory.path().join("provider/root.jsonl");
        let run_id = RunId::new();
        let task_run = TaskRun {
            run_id,
            key: RunKey::Native {
                provider: Provider::Codex,
                sid: "herdr-sid".to_owned(),
            },
            display_ordinal: DisplayOrdinal::new(1),
            state: TaskState::Running,
            has_controller_task_state_event: false,
        };
        let agent_node = AgentNode {
            agent_node_id: "gap-agent-herdr".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some("herdr-sid".to_owned()),
            task_run_id: run_id,
            display_ordinal: DisplayOrdinal::new(2),
            parent_agent_node_id: None,
            state: Some(ExecState::Working),
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: Some(path.to_string_lossy().into_owned()),
        };
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![
                PersistOp::UpsertTaskRun(PersistTaskRun {
                    task_run,
                    native_session: Some(NativeSessionBinding {
                        provider: Provider::Codex,
                        native_session_id: "herdr-sid".to_owned(),
                    }),
                    created_at_ms: 1,
                    updated_at_ms: 1,
                    finished_at_ms: None,
                }),
                PersistOp::UpsertAgentNode(agent_node),
            ])
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored);

        apply_provider_event(
            ProviderEvent::SessionResolved {
                provider: Provider::Codex,
                agent_thread_id: "adapter-sid".to_owned(),
                owner_session_id: Some("adapter-sid".to_owned()),
                parent_thread_id: None,
                path,
                model_id: None,
                depth: Some(0),
                event_id: "prov:codex:meta:adapter-sid".to_owned(),
                observed_at_ms: 100,
                position: SourcePosition {
                    path_id: 1,
                    generation: 0,
                    offset: 0,
                },
            },
            "session",
            &mut reducer,
            &shared,
            &writer,
        )
        .await
        .unwrap();

        let model = shared.borrow();
        assert_eq!(
            model.task_run(&run_id).unwrap().key,
            RunKey::Native {
                provider: Provider::Codex,
                sid: "herdr-sid".to_owned(),
            }
        );
        assert_eq!(
            model
                .controller_diagnostics()
                .provider_identity_disagreements(),
            1
        );
        drop(model);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn owned_session_merge_conflict_is_diagnostic_and_non_fatal() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let path = directory.path().join("provider/root.jsonl");
        let owner = RunId::new();
        let path_run = RunId::new();
        let first_parent = RunId::new();
        let second_parent = RunId::new();
        let mut model = DomainModel::default();
        for task_run in [
            TaskRun {
                run_id: owner,
                key: RunKey::Native {
                    provider: Provider::Codex,
                    sid: "owner".to_owned(),
                },
                display_ordinal: DisplayOrdinal::new(1),
                state: TaskState::Running,
                has_controller_task_state_event: false,
            },
            TaskRun {
                run_id: path_run,
                key: RunKey::NativePath {
                    provider: Provider::Codex,
                    path: path.to_string_lossy().into_owned(),
                },
                display_ordinal: DisplayOrdinal::new(2),
                state: TaskState::Running,
                has_controller_task_state_event: false,
            },
            TaskRun {
                run_id: first_parent,
                key: RunKey::Controller("first-parent".to_owned()),
                display_ordinal: DisplayOrdinal::new(3),
                state: TaskState::Running,
                has_controller_task_state_event: true,
            },
            TaskRun {
                run_id: second_parent,
                key: RunKey::Controller("second-parent".to_owned()),
                display_ordinal: DisplayOrdinal::new(4),
                state: TaskState::Running,
                has_controller_task_state_event: true,
            },
        ] {
            model.insert_task_run(task_run);
        }
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: first_parent,
            child_run_id: owner,
        });
        model.insert_execution_edge(ExecutionEdge {
            parent_run_id: second_parent,
            child_run_id: path_run,
        });
        let restored = RestoredState {
            model,
            next_ordinal: 5,
            next_ingest_seq: Some(1),
            event_ledger: Vec::new(),
        };
        let store = open_writer(&root).unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut reducer, shared) = Reducer::new(restored);

        apply_provider_event(
            ProviderEvent::SessionResolved {
                provider: Provider::Codex,
                agent_thread_id: "owner".to_owned(),
                owner_session_id: Some("owner".to_owned()),
                parent_thread_id: None,
                path,
                model_id: None,
                depth: Some(0),
                event_id: "prov:codex:meta:owner".to_owned(),
                observed_at_ms: 100,
                position: SourcePosition {
                    path_id: 1,
                    generation: 0,
                    offset: 0,
                },
            },
            "session",
            &mut reducer,
            &shared,
            &writer,
        )
        .await
        .unwrap();

        let model = shared.borrow();
        assert!(model.task_run(&owner).is_some());
        assert!(model.task_run(&path_run).is_some());
        assert_eq!(model.agent_nodes().count(), 0);
        assert_eq!(model.controller_diagnostics().binding_conflicts(), 1);
        drop(model);
        lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn restored_targets_feed_provider_events_during_herdr_reconnect_wait() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let sessions = directory.path().join("home/.codex/sessions/2026/08/09");
        std::fs::create_dir_all(&sessions).unwrap();
        let session_file = sessions.join("rollout-owner.jsonl");
        std::fs::write(
            &session_file,
            br#"{"type":"session_meta","payload":{"id":"owner","session_id":"owner","model":"gpt-test"}}
"#,
        )
        .unwrap();
        let run_id = RunId::new();
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![PersistOp::UpsertTaskRun(PersistTaskRun {
                task_run: TaskRun {
                    run_id,
                    key: RunKey::NativePath {
                        provider: Provider::Codex,
                        path: session_file.to_string_lossy().into_owned(),
                    },
                    display_ordinal: DisplayOrdinal::new(1),
                    state: TaskState::EndedUnknown,
                    has_controller_task_state_event: false,
                },
                native_session: None,
                created_at_ms: 1,
                updated_at_ms: 1,
                finished_at_ms: Some(1),
            })])
            .unwrap();
        let restored = store.load_restored_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let mut collector = spawn(
            directory.path().join("missing-herdr.sock"),
            "provider-reconnect".to_owned(),
            restored,
            writer,
        )
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if collector
                    .model
                    .borrow()
                    .agent_node("agent:codex:owner")
                    .is_some()
                {
                    break;
                }
                collector.model.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        {
            let model = collector.model.borrow();
            assert_eq!(
                model.task_run(&run_id).unwrap().key,
                RunKey::Native {
                    provider: Provider::Codex,
                    sid: "owner".to_owned(),
                }
            );
            assert_eq!(
                model.task_run(&run_id).unwrap().state,
                TaskState::EndedUnknown
            );
            assert_eq!(model.agent_nodes().count(), 1);
            assert_eq!(model.panes().count(), 0);
            assert_eq!(model.executions().count(), 0);
        }

        collector.stop().await.unwrap();
        lifecycle.shutdown().await.unwrap();
    }
}
