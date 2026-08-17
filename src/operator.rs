//! Closed live operator projection.

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::watch;

use crate::activity::{
    ActivityDurability, ActivityIdentity, ActivityItem, OperatorSnapshot, RestoredOperatorState,
};
use crate::diagnostics::RuntimeWriteOutcome;
use crate::model::{EventMetadata, NormalizedEvent, RunId, sanitize_controller_text};
use crate::store::{PersistOp, PersistTaskRun};

const ACTIVITY_LIMIT: usize = 10_000;

// increment5-workload-harness: begin operator activity limit alias
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
pub const WORKLOAD_OPERATOR_ACTIVITY_LIMIT: usize = ACTIVITY_LIMIT;
// increment5-workload-harness: end operator activity limit alias
/// Reducer-owned, closed projection of live operator-safe state.
pub(crate) struct OperatorProjection {
    activity: Vec<ActivityItem>,
    terminal_times: HashMap<RunId, i64>,
    pending_identities: HashSet<ActivityIdentity>,
    publisher: watch::Sender<OperatorSnapshot>,
}

impl OperatorProjection {
    /// Restores the durable seed and returns its immutable watch receiver.
    pub(crate) fn new(
        restored: RestoredOperatorState,
    ) -> (Self, watch::Receiver<OperatorSnapshot>) {
        let RestoredOperatorState {
            mut activity,
            terminal_times,
        } = restored;
        normalize_activity(&mut activity);
        let initial = snapshot(&activity, &terminal_times);
        let (publisher, receiver) = watch::channel(initial);
        (
            Self {
                activity,
                terminal_times,
                pending_identities: HashSet::new(),
                publisher,
            },
            receiver,
        )
    }

    /// Optimistically applies one complete reducer submission as durable.
    pub(crate) fn apply_submission(&mut self, batch: &[PersistOp]) {
        let mut changed = false;
        // Project the complete submission before applying its merge metadata so
        // even events emitted after a merge op in the durable batch participate
        // in every lineage rewrite from that same atomic submission.
        for operation in batch {
            if let PersistOp::RecordEvent { event, seen_at_ms } = operation {
                let projected = project_event(event, *seen_at_ms);
                if let Some(item) = projected {
                    self.pending_identities.insert(item.identity.clone());
                    self.activity
                        .retain(|existing| existing.identity != item.identity);
                    self.activity.push(item);
                    changed = true;
                }
            }
        }
        for operation in batch {
            match operation {
                PersistOp::RecordEvent { .. } => {}
                PersistOp::MergeTaskRuns { survivor, absorbed } => {
                    for item in &mut self.activity {
                        if item.task_run_id == Some(*absorbed) {
                            item.task_run_id = Some(*survivor);
                            self.pending_identities.insert(item.identity.clone());
                            changed = true;
                        }
                    }
                    changed |= self.merge_terminal_times(*survivor, *absorbed);
                }
                PersistOp::UpsertTaskRun(task_run) => {
                    changed |= self.apply_terminal_time(task_run);
                }
                PersistOp::PromoteTaskRunKey { promoted, .. } => {
                    changed |= self.apply_terminal_time(promoted);
                }
                PersistOp::UpsertWorkspace { .. }
                | PersistOp::UpsertTab { .. }
                | PersistOp::UpsertPane { .. }
                | PersistOp::DeleteWorkspace { .. }
                | PersistOp::DeleteTab { .. }
                | PersistOp::DeletePane { .. }
                | PersistOp::UpsertExecution(_)
                | PersistOp::UpsertAgentNode(_)
                | PersistOp::UpsertExecutionEdge { .. }
                | PersistOp::UpsertDependencyEdge { .. }
                | PersistOp::RecordCollectorGap(_)
                | PersistOp::AdvanceIngestSequence { .. } => {}
            }
        }
        if changed {
            normalize_activity(&mut self.activity);
            self.publish();
        }
    }

    /// Reclassifies exactly the identities affected by the preceding submission.
    pub(crate) fn complete_submission(&mut self, outcome: RuntimeWriteOutcome) {
        let durability = match outcome {
            RuntimeWriteOutcome::Durable | RuntimeWriteOutcome::CommittedButDegraded(_) => {
                ActivityDurability::Durable
            }
            RuntimeWriteOutcome::NotCommitted(_) | RuntimeWriteOutcome::Skipped => {
                ActivityDurability::CurrentOnly
            }
            RuntimeWriteOutcome::DurabilityUnknown(_) => ActivityDurability::DurabilityUnknown,
        };
        let mut changed = false;
        for item in &mut self.activity {
            if self.pending_identities.contains(&item.identity) && item.durability != durability {
                item.durability = durability;
                changed = true;
            }
        }
        self.pending_identities.clear();
        if changed {
            self.publish();
        }
    }

    fn apply_terminal_time(&mut self, persisted: &PersistTaskRun) -> bool {
        let run_id = persisted.task_run.run_id;
        if persisted.task_run.state.is_terminal() {
            match self.terminal_times.entry(run_id) {
                Entry::Occupied(_) => false,
                Entry::Vacant(entry) => {
                    entry.insert(persisted.finished_at_ms.unwrap_or(persisted.updated_at_ms));
                    true
                }
            }
        } else {
            self.terminal_times.remove(&run_id).is_some()
        }
    }

    fn merge_terminal_times(&mut self, survivor: RunId, absorbed: RunId) -> bool {
        let survivor_time = self.terminal_times.remove(&survivor);
        let absorbed_time = self.terminal_times.remove(&absorbed);
        let merged_time = match (survivor_time, absorbed_time) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(time), None) | (None, Some(time)) => Some(time),
            (None, None) => None,
        };
        if let Some(time) = merged_time {
            self.terminal_times.insert(survivor, time);
        }
        absorbed_time.is_some() || survivor_time != merged_time
    }

    fn publish(&self) {
        let next = snapshot(&self.activity, &self.terminal_times);
        self.publisher.send_if_modified(|current| {
            if current.activity.as_ref() == next.activity.as_ref()
                && current.terminal_times.as_ref() == next.terminal_times.as_ref()
            {
                false
            } else {
                *current = next;
                true
            }
        });
    }
}

fn project_event(event: &NormalizedEvent, seen_at_ms: i64) -> Option<ActivityItem> {
    let (metadata, normalized_kind, controller_text) = match event {
        NormalizedEvent::ControllerEvent { metadata, .. } => (metadata, "controller_event", true),
        NormalizedEvent::TopologyUpsert { metadata, .. } => (metadata, "topology_upsert", false),
        NormalizedEvent::TopologyClosure { metadata, .. } => (metadata, "topology_closure", false),
        NormalizedEvent::AgentStatusChanged { metadata, .. } => {
            (metadata, "agent_status_changed", false)
        }
        NormalizedEvent::AgentNodeUpsert { metadata, .. } => (metadata, "agent_node_upsert", false),
        NormalizedEvent::AgentActivity { metadata, .. } => (metadata, "agent_activity", false),
        NormalizedEvent::ExecutionBegin { metadata, .. } => (metadata, "execution_begin", false),
        NormalizedEvent::ExecutionEnd { metadata, .. } => (metadata, "execution_end", false),
    };
    project_metadata(metadata, normalized_kind, seen_at_ms, controller_text)
}

fn project_metadata(
    metadata: &EventMetadata,
    normalized_kind: &str,
    seen_at_ms: i64,
    controller_text: bool,
) -> Option<ActivityItem> {
    if metadata.event_id.is_empty()
        || metadata.source.is_empty()
        || metadata.source_event_type.is_empty()
    {
        return None;
    }
    let provider = metadata.provider_metadata.as_ref();
    Some(ActivityItem {
        identity: ActivityIdentity {
            event_id: metadata.event_id.clone(),
        },
        event_timestamp_ms: metadata.timestamp_ms,
        seen_at_ms,
        ingest_seq: metadata.ingest_seq,
        source: metadata.source.clone(),
        normalized_kind: normalized_kind.to_owned(),
        source_event_type: metadata.source_event_type.clone(),
        workspace_id: metadata.workspace_id.clone(),
        tab_id: metadata.tab_id.clone(),
        pane_id: metadata.pane_id.clone(),
        terminal_id: metadata.terminal_id.clone(),
        provider: metadata.provider,
        native_session_id: metadata.native_session_id.clone(),
        task_run_id: metadata.task_run_id,
        agent_node_id: metadata.agent_node_id.clone(),
        task_state: metadata.task_state,
        model_id: provider.and_then(|value| value.model_id.clone()),
        provider_event_kind: provider.and_then(|value| value.event_kind.clone()),
        tool_name: provider.and_then(|value| value.tool_name.clone()),
        item_count: provider.and_then(|value| value.item_count),
        byte_count: provider.and_then(|value| value.byte_count),
        provider_agent_id: provider.and_then(|value| value.agent_id.clone()),
        provider_parent_agent_id: provider.and_then(|value| value.parent_agent_id.clone()),
        controller_label: controller_text
            .then(|| metadata.label.as_deref().map(sanitize_controller_text))
            .flatten(),
        controller_reason: controller_text
            .then(|| metadata.reason.as_deref().map(sanitize_controller_text))
            .flatten(),
        durability: ActivityDurability::Durable,
    })
}

fn normalize_activity(activity: &mut Vec<ActivityItem>) {
    activity.sort_by(compare_activity);
    let mut identities = HashSet::with_capacity(activity.len());
    activity.retain(|item| identities.insert(item.identity.clone()));
    activity.truncate(ACTIVITY_LIMIT);
}

fn compare_activity(left: &ActivityItem, right: &ActivityItem) -> Ordering {
    right
        .event_timestamp_ms
        .cmp(&left.event_timestamp_ms)
        .then_with(|| right.seen_at_ms.cmp(&left.seen_at_ms))
        .then_with(|| right.ingest_seq.is_some().cmp(&left.ingest_seq.is_some()))
        .then_with(|| right.ingest_seq.cmp(&left.ingest_seq))
        .then_with(|| right.identity.event_id.cmp(&left.identity.event_id))
}

fn snapshot(activity: &[ActivityItem], terminal_times: &HashMap<RunId, i64>) -> OperatorSnapshot {
    OperatorSnapshot {
        activity: Arc::from(activity.to_vec()),
        terminal_times: Arc::new(terminal_times.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use rusqlite::Connection;

    use super::OperatorProjection;
    use crate::activity::{
        ActivityDurability, ActivityIdentity, ActivityItem, RestoredOperatorState,
    };
    use crate::diagnostics::RuntimeWriteOutcome;
    use crate::lockfile::StateRoot;
    use crate::model::{
        DisplayOrdinal, EventMetadata, NormalizedEvent, RunId, RunKey, TaskRun, TaskState,
        TopologyEntity, Workspace,
    };
    use crate::store::writer::{
        DurabilityDisposition, PersistenceFailure, PersistenceFailureCode, PersistenceOperation,
        PersistencePhase, PersistenceStatus, WriterError, spawn_writer,
    };
    use crate::store::{
        CollectorGap, PersistOp, PersistTaskRun, database_path, open_reader, open_writer,
    };

    const ACTIVITY_LIMIT: usize = 10_000;

    fn activity(
        event_id: impl Into<String>,
        timestamp_ms: i64,
        seen_at_ms: i64,
        ingest_seq: Option<u64>,
        task_run_id: Option<RunId>,
        durability: ActivityDurability,
    ) -> ActivityItem {
        ActivityItem {
            identity: ActivityIdentity {
                event_id: event_id.into(),
            },
            event_timestamp_ms: timestamp_ms,
            seen_at_ms,
            ingest_seq,
            source: "seed".to_owned(),
            normalized_kind: "topology_upsert".to_owned(),
            source_event_type: "workspace_created".to_owned(),
            workspace_id: Some("workspace".to_owned()),
            tab_id: None,
            pane_id: None,
            terminal_id: None,
            provider: None,
            native_session_id: None,
            task_run_id,
            agent_node_id: None,
            task_state: None,
            model_id: None,
            provider_event_kind: None,
            tool_name: None,
            item_count: None,
            byte_count: None,
            provider_agent_id: None,
            provider_parent_agent_id: None,
            controller_label: None,
            controller_reason: None,
            durability,
        }
    }

    fn event(
        event_id: &str,
        timestamp_ms: i64,
        seen_at_ms: i64,
        ingest_seq: Option<u64>,
        task_run_id: Option<RunId>,
        source: &str,
    ) -> PersistOp {
        PersistOp::RecordEvent {
            event: Box::new(NormalizedEvent::TopologyUpsert {
                metadata: EventMetadata {
                    event_id: event_id.to_owned(),
                    timestamp_ms,
                    receipt_time_ms: seen_at_ms,
                    source: source.to_owned(),
                    source_event_type: "workspace_created".to_owned(),
                    herdr_session: "operator-test".to_owned(),
                    workspace_id: Some("workspace".to_owned()),
                    tab_id: None,
                    pane_id: None,
                    terminal_id: None,
                    provider: None,
                    native_session_id: None,
                    task_run_id,
                    agent_node_id: None,
                    task_state: None,
                    execution_parent: None,
                    dependency: None,
                    source_coverage: Vec::new(),
                    provider_metadata: None,
                    label: None,
                    reason: None,
                    progress: None,
                    ingest_seq,
                },
                entity: TopologyEntity::Workspace(Workspace {
                    workspace_id: "workspace".to_owned(),
                }),
            }),
            seen_at_ms,
        }
    }

    fn run_op(
        run_id: RunId,
        ordinal: i64,
        state: TaskState,
        updated_at_ms: i64,
        finished_at_ms: Option<i64>,
    ) -> PersistOp {
        PersistOp::UpsertTaskRun(PersistTaskRun {
            task_run: TaskRun {
                run_id,
                key: RunKey::Controller(format!("controller-{run_id}")),
                display_ordinal: DisplayOrdinal::new(ordinal),
                state,
                has_controller_task_state_event: true,
            },
            native_session: None,
            created_at_ms: updated_at_ms,
            updated_at_ms,
            finished_at_ms,
        })
    }

    fn merge_batch(
        survivor: RunId,
        absorbed: RunId,
        survivor_state: TaskState,
        timestamp_ms: i64,
    ) -> Vec<PersistOp> {
        vec![
            PersistOp::MergeTaskRuns { survivor, absorbed },
            run_op(
                survivor,
                1,
                survivor_state,
                timestamp_ms,
                survivor_state.is_terminal().then_some(timestamp_ms),
            ),
        ]
    }

    fn restored(activity: Vec<ActivityItem>) -> RestoredOperatorState {
        RestoredOperatorState {
            activity,
            terminal_times: std::collections::HashMap::new(),
        }
    }

    fn failure(
        operation: PersistenceOperation,
        durability: DurabilityDisposition,
    ) -> PersistenceFailure {
        PersistenceFailure {
            operation,
            phase: match durability {
                DurabilityDisposition::Committed => PersistencePhase::PostApplyCommit,
                DurabilityDisposition::Unknown => PersistencePhase::Acknowledgement,
                DurabilityDisposition::NotApplicable | DurabilityDisposition::NotCommitted => {
                    PersistencePhase::CommandExecution
                }
            },
            code: PersistenceFailureCode::Sqlite,
            durability,
        }
    }

    fn unix_now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }

    #[test]
    fn i4_operator_restored_and_live_activity_share_bound() {
        let seed = (0..ACTIVITY_LIMIT)
            .map(|index| {
                activity(
                    format!("seed-{index:05}"),
                    index as i64,
                    index as i64,
                    None,
                    None,
                    ActivityDurability::Durable,
                )
            })
            .collect();
        let (mut projection, operator) = OperatorProjection::new(restored(seed));

        projection.apply_submission(&[event(
            "live-newest",
            ACTIVITY_LIMIT as i64,
            ACTIVITY_LIMIT as i64,
            None,
            None,
            "herdr",
        )]);
        projection.complete_submission(RuntimeWriteOutcome::Durable);

        let snapshot = operator.borrow();
        assert_eq!(snapshot.activity.len(), ACTIVITY_LIMIT);
        assert_eq!(snapshot.activity[0].identity.event_id, "live-newest");
        assert!(
            snapshot
                .activity
                .iter()
                .all(|item| item.identity.event_id != "seed-00000")
        );
    }

    #[test]
    fn i4_operator_duplicate_identity_replaces_without_growth() {
        let (mut projection, operator) = OperatorProjection::new(restored(vec![activity(
            "same",
            1,
            1,
            None,
            None,
            ActivityDurability::Durable,
        )]));

        projection.apply_submission(&[event("same", 2, 3, Some(4), None, "controller")]);
        projection.complete_submission(RuntimeWriteOutcome::Durable);

        let snapshot = operator.borrow();
        assert_eq!(snapshot.activity.len(), 1);
        assert_eq!(snapshot.activity[0].event_timestamp_ms, 2);
        assert_eq!(snapshot.activity[0].seen_at_ms, 3);
        assert_eq!(snapshot.activity[0].ingest_seq, Some(4));
        assert_eq!(snapshot.activity[0].source, "controller");

        let PersistOp::RecordEvent { event, seen_at_ms } =
            event("unsafe-controller", 4, 4, None, None, "controller")
        else {
            unreachable!();
        };
        let NormalizedEvent::TopologyUpsert { mut metadata, .. } = *event else {
            unreachable!();
        };
        metadata.label = Some("unsafe\nlabel".to_owned());
        metadata.reason = Some("unsafe\rreason".to_owned());
        let projected = super::project_metadata(&metadata, "controller_event", seen_at_ms, true)
            .expect("complete Controller metadata must project");
        assert_eq!(
            projected.controller_label.as_deref(),
            Some("unsafe\\nlabel")
        );
        assert_eq!(
            projected.controller_reason.as_deref(),
            Some("unsafe\\rreason")
        );
    }

    #[test]
    fn i4_operator_post_degradation_activity_is_current_only() {
        let (mut projection, operator) = OperatorProjection::new(restored(Vec::new()));
        projection.apply_submission(&[event("failed", 1, 1, None, None, "herdr")]);
        projection.complete_submission(RuntimeWriteOutcome::NotCommitted(failure(
            PersistenceOperation::Apply,
            DurabilityDisposition::NotCommitted,
        )));
        projection.apply_submission(&[event("later", 2, 2, None, None, "herdr")]);
        projection.complete_submission(RuntimeWriteOutcome::Skipped);

        let snapshot = operator.borrow();
        assert_eq!(snapshot.activity.len(), 2);
        assert!(
            snapshot
                .activity
                .iter()
                .all(|item| { matches!(item.durability, ActivityDurability::CurrentOnly) })
        );
    }

    #[test]
    fn i4_operator_failed_multi_event_batch_reclassifies_every_identity() {
        let (mut projection, operator) = OperatorProjection::new(restored(Vec::new()));
        projection.apply_submission(&[
            event("first", 1, 1, None, None, "provider"),
            event("second", 2, 2, None, None, "provider"),
        ]);
        projection.complete_submission(RuntimeWriteOutcome::NotCommitted(failure(
            PersistenceOperation::Apply,
            DurabilityDisposition::NotCommitted,
        )));

        let snapshot = operator.borrow();
        assert_eq!(snapshot.activity.len(), 2);
        assert!(
            snapshot
                .activity
                .iter()
                .all(|item| { matches!(item.durability, ActivityDurability::CurrentOnly) })
        );
    }

    #[test]
    fn i4_operator_unknown_multi_event_batch_marks_every_identity_unknown() {
        let (mut projection, operator) = OperatorProjection::new(restored(Vec::new()));
        projection.apply_submission(&[
            event("first", 1, 1, None, None, "provider"),
            event("second", 2, 2, None, None, "provider"),
        ]);
        projection.complete_submission(RuntimeWriteOutcome::DurabilityUnknown(failure(
            PersistenceOperation::Apply,
            DurabilityDisposition::Unknown,
        )));

        let snapshot = operator.borrow();
        assert_eq!(snapshot.activity.len(), 2);
        assert!(
            snapshot
                .activity
                .iter()
                .all(|item| { matches!(item.durability, ActivityDurability::DurabilityUnknown) })
        );
    }

    #[tokio::test]
    async fn i4_operator_committed_cleanup_failure_stays_durable_across_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        store
            .apply_batch(vec![PersistOp::RecordCollectorGap(CollectorGap {
                event_id: "ancient-gap".to_owned(),
                herdr_session: "operator-test".to_owned(),
                seen_at_ms: 0,
                kind: crate::model::GapKind::Startup,
            })])
            .unwrap();
        Connection::open(database_path(&root))
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER i4_operator_fail_cleanup BEFORE DELETE ON events \
                 BEGIN SELECT RAISE(ABORT, 'private cleanup failure'); END;",
            )
            .unwrap();
        let seed = store.load_restored_operator_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut projection, operator) = OperatorProjection::new(seed);
        let now = unix_now_ms();
        let batch = vec![event("committed", now, now, None, None, "provider")];
        projection.apply_submission(&batch);
        let error = writer.apply(batch).await.unwrap_err();
        let WriterError::Persistence(failure) = error else {
            panic!("cleanup failure must be a typed runtime persistence failure");
        };
        assert_eq!(failure.durability, DurabilityDisposition::Committed);
        projection.complete_submission(RuntimeWriteOutcome::CommittedButDegraded(failure));
        assert_eq!(
            operator.borrow().activity[0].durability,
            ActivityDurability::Durable
        );
        lifecycle.shutdown().await.unwrap();

        let reopened = open_reader(&root)
            .unwrap()
            .load_restored_operator_state()
            .unwrap();
        let committed = reopened
            .activity
            .iter()
            .find(|item| item.identity.event_id == "committed")
            .unwrap();
        assert_eq!(committed.durability, ActivityDurability::Durable);
    }

    #[test]
    fn i4_operator_merge_reparents_activity_to_survivor_without_duplication() {
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let final_survivor = RunId::new();
        let (mut projection, operator) = OperatorProjection::new(restored(vec![
            activity(
                "absorbed-a",
                2,
                2,
                None,
                Some(absorbed),
                ActivityDurability::Durable,
            ),
            activity(
                "absorbed-b",
                1,
                1,
                None,
                Some(absorbed),
                ActivityDurability::Durable,
            ),
            activity("unrelated", 0, 0, None, None, ActivityDurability::Durable),
        ]));

        let mut batch = merge_batch(survivor, absorbed, TaskState::Running, 3);
        batch.push(event(
            "same-batch-after-merge",
            3,
            3,
            None,
            Some(absorbed),
            "provider",
        ));
        batch.extend(merge_batch(final_survivor, survivor, TaskState::Running, 4));
        projection.apply_submission(&batch);
        projection.complete_submission(RuntimeWriteOutcome::Durable);

        let snapshot = operator.borrow();
        assert_eq!(snapshot.activity.len(), 4);
        assert_eq!(
            snapshot
                .activity
                .iter()
                .filter(|item| item.task_run_id == Some(final_survivor))
                .count(),
            3
        );
        assert!(
            snapshot.activity.iter().all(
                |item| item.task_run_id != Some(absorbed) && item.task_run_id != Some(survivor)
            )
        );
    }

    #[test]
    fn i4_operator_live_and_cold_restore_match_after_merge() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let now = unix_now_ms();
        store
            .apply_batch(vec![
                run_op(survivor, 1, TaskState::Running, now, None),
                run_op(absorbed, 2, TaskState::Running, now, None),
                event("old-lineage", now, now, None, Some(absorbed), "provider"),
            ])
            .unwrap();
        let seed = store.load_restored_operator_state().unwrap();
        let (mut projection, operator) = OperatorProjection::new(seed);
        let batch = merge_batch(survivor, absorbed, TaskState::Running, now + 1);

        projection.apply_submission(&batch);
        store.apply_batch(batch).unwrap();
        projection.complete_submission(RuntimeWriteOutcome::Durable);
        let reopened = store.load_restored_operator_state().unwrap();

        let snapshot = operator.borrow();
        assert_eq!(snapshot.activity.as_ref(), reopened.activity.as_slice());
        assert_eq!(snapshot.terminal_times.as_ref(), &reopened.terminal_times);
    }

    #[test]
    fn i4_operator_failed_merge_batch_marks_reparented_activity_current_only_and_reopens_old_lineage()
     {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        store
            .apply_batch(vec![
                run_op(survivor, 1, TaskState::Running, 1, None),
                run_op(absorbed, 2, TaskState::Running, 1, None),
                event("old-lineage", 2, 2, None, Some(absorbed), "provider"),
            ])
            .unwrap();
        let seed = store.load_restored_operator_state().unwrap();
        let (mut projection, operator) = OperatorProjection::new(seed);

        projection.apply_submission(&merge_batch(survivor, absorbed, TaskState::Running, 3));
        projection.complete_submission(RuntimeWriteOutcome::NotCommitted(failure(
            PersistenceOperation::Apply,
            DurabilityDisposition::NotCommitted,
        )));

        let live = operator.borrow();
        assert_eq!(live.activity[0].task_run_id, Some(survivor));
        assert_eq!(live.activity[0].durability, ActivityDurability::CurrentOnly);
        drop(live);
        let reopened = store.load_restored_operator_state().unwrap();
        assert_eq!(reopened.activity[0].task_run_id, Some(absorbed));
        assert_eq!(reopened.activity[0].durability, ActivityDurability::Durable);
    }

    #[tokio::test]
    async fn i4_operator_unknown_merge_batch_marks_reparented_activity_unknown_until_cold_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let root = StateRoot(directory.path().to_path_buf());
        let mut store = open_writer(&root).unwrap();
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let now = unix_now_ms();
        store
            .apply_batch(vec![
                run_op(survivor, 1, TaskState::Running, now, None),
                run_op(absorbed, 2, TaskState::Running, now, None),
                event("old-lineage", now, now, None, Some(absorbed), "provider"),
            ])
            .unwrap();
        let seed = store.load_restored_operator_state().unwrap();
        let (lifecycle, writer) = spawn_writer(store).unwrap();
        let (mut projection, operator) = OperatorProjection::new(seed);
        let batch = merge_batch(survivor, absorbed, TaskState::Running, now + 1);

        projection.apply_submission(&batch);
        let pending = writer.reserve_enqueue().unwrap().enqueue(batch);
        writer.barrier().await.unwrap();
        drop(pending);
        let PersistenceStatus::Degraded { failure } = writer.persistence_status() else {
            panic!("dropping an unread committed receipt must make durability unknown");
        };
        assert_eq!(failure.durability, DurabilityDisposition::Unknown);
        projection.complete_submission(RuntimeWriteOutcome::DurabilityUnknown(failure));
        assert_eq!(operator.borrow().activity[0].task_run_id, Some(survivor));
        assert_eq!(
            operator.borrow().activity[0].durability,
            ActivityDurability::DurabilityUnknown
        );
        lifecycle.shutdown().await.unwrap();

        let reopened = open_reader(&root)
            .unwrap()
            .load_restored_operator_state()
            .unwrap();
        assert_eq!(reopened.activity[0].task_run_id, Some(survivor));
        assert_eq!(reopened.activity[0].durability, ActivityDurability::Durable);
    }

    #[test]
    fn i4_operator_terminal_times_follow_first_and_reopen_rules() {
        let survivor = RunId::new();
        let absorbed = RunId::new();
        let (mut projection, operator) = OperatorProjection::new(restored(Vec::new()));

        projection.apply_submission(&[run_op(survivor, 1, TaskState::Completed, 100, Some(100))]);
        projection.complete_submission(RuntimeWriteOutcome::Durable);
        projection.apply_submission(&[run_op(survivor, 1, TaskState::Completed, 200, Some(200))]);
        projection.complete_submission(RuntimeWriteOutcome::Durable);
        assert_eq!(operator.borrow().terminal_times.get(&survivor), Some(&100));

        projection.apply_submission(&[run_op(survivor, 1, TaskState::Running, 300, None)]);
        projection.complete_submission(RuntimeWriteOutcome::Durable);
        assert!(!operator.borrow().terminal_times.contains_key(&survivor));

        projection.apply_submission(&[
            run_op(absorbed, 2, TaskState::Completed, 350, Some(350)),
            run_op(survivor, 1, TaskState::Completed, 400, None),
        ]);
        projection.complete_submission(RuntimeWriteOutcome::Durable);
        projection.apply_submission(&merge_batch(survivor, absorbed, TaskState::Completed, 450));
        projection.complete_submission(RuntimeWriteOutcome::Durable);

        let snapshot = operator.borrow();
        assert_eq!(snapshot.terminal_times.get(&survivor), Some(&350));
        assert!(!snapshot.terminal_times.contains_key(&absorbed));
    }
}
