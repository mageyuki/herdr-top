# Cold-Safe History Drain Finalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every frozen provider-history drain durably finalizable even when it emits no novel event, close the remaining provider-lane clone finding, and prove all four Codex/Claude controller-child routes through real-pane cold restart.

**Architecture:** `HistoryDrainBarrier` becomes the sole owner of the frozen `Arc<PersistHistoryDrain>` and exposes the drain ID only through that manifest. The reducer stages the same `Arc`, the writer transports it unchanged, and the Store validates or inserts the immutable manifest before finalizing it in the same transaction. The provider-lane cleanup is a separate one-file characterization-first task. The two disjoint implementation tasks may run concurrently in dedicated linked worktrees, but Codex integrates and verifies them serially.

**Tech Stack:** Rust 2024, Tokio MPSC/oneshot, rusqlite transactions, SHA-256 history identities, Cargo test, GNU Make, Herdr pane CLI, Herdr Top doctor JSON, and Python's standard `sqlite3` module for read-only acceptance queries because the system `sqlite3` CLI is unavailable.

**Spec:** `docs/internal/superpowers/2026-08-29-history-drain-barrier-manifest/spec.md`

## Global Constraints

- Starting branch: `agent/stable-task-history-rates`; the planning baseline contains spec commit `7f7aea7` plus this reviewed plan.
- No SQLite schema change, migration, dependency change, Task Run lifecycle change, display-precedence change, retention change, or public CLI change.
- `history_drain_finalized` must continue returning an error for an absent drain; `history_drain_finalization` must continue returning `Ok(None)` for an absent or incomplete drain.
- A conflicting provider or frozen artifact set for an existing drain ID remains a hard `StoreError::InvalidData` and leaves durable state unchanged.
- The barrier has no independent drain ID. `HistoryDrainBarrier::new` accepts the frozen manifest and observation time; every ID read uses `barrier.manifest.drain_id` or `barrier.drain_id()`.
- Manifest upsert and drain finalization occur inside one Store transaction. Event-bearing V6 batches may still upsert the same manifest idempotently.
- Provider ingress remains closed until finalization is durably known and the exact page has been read back.
- Grok implementation, Grok routing, Grok review authority, and PR merge are outside this increment.
- Every implementation task uses TDD, changes only its declared file set, performs no nested AI delegation, and returns an uncommitted result. Claude CLI and its wrapper must not stage, commit, push, merge, or rebase.
- Codex independently verifies every returned diff and test result, commits task branches only after verification, and integrates one task at a time.
- Commit messages in this user repository include `Co-Authored-By: OpenAI Codex <codex@openai.com>`.
- Temporary Pane, hook, doctor, and SQLite evidence stays under owner-only `/tmp` directories and is never committed.

## File and Responsibility Map

### Task 1: History-drain correctness

- `src/provider/mod.rs`: barrier payload and pending-queue identity/order behavior.
- `src/herdr/collector.rs`: frozen-manifest barrier creation, held-barrier retry, RuntimePersistence writer call, and zero-event/all-duplicate integration regressions.
- `src/reducer.rs`: immutable staged finalization payload.
- `src/store/writer.rs`: full-manifest writer command and acknowledgement-loss path.
- `src/store/mod.rs`: one-transaction manifest validation/upsert and finalization plus rollback/conflict tests.
- `src/identity.rs`: migrate the existing binding-merge finalization fixture to pass its frozen manifest.
- `tests/restore.rs`: migrate the direct completed-drain restore fixture to pass its frozen manifest.
- `tests/convergence.rs`: migrate the historical-publication fixture to pass its frozen manifest.

### Task 2: Provider-lane cleanup

- `src/provider/lane.rs`: Working/Idle event characterization and removal of the avoidable `ExecState` clone.

The declared file sets are disjoint and neither task consumes the other's output. Dispatch both after the reviewed planning baseline is committed. Task 1 is integrated first because it removes the persistence blocker; Task 2 is integrated second as the independently testable Copilot cleanup.

After the reviewed planning baseline commit, use `superpowers:using-git-worktrees` and create both task worktrees from that exact commit:

```bash
git worktree add /home/mageyuki/git/mageyuki/herdr-top/.worktrees/pr21-history-drain-finalization \
  -b agent/pr21-history-drain-finalization "$(git rev-parse HEAD)"
git worktree add /home/mageyuki/git/mageyuki/herdr-top/.worktrees/pr21-lane-clone-cleanup \
  -b agent/pr21-lane-clone-cleanup "$(git rev-parse HEAD)"
```

Run `cargo build --locked` in each new worktree before dispatch. A baseline failure stops that task before implementation.

---

### Task 1: Carry and Atomically Finalize the Frozen History Manifest

**Files:**
- Modify: `src/provider/mod.rs:153-174, 1060-1200, 1390-1475, 3220-3305`
- Modify: `src/herdr/collector.rs:4748-4765, 878-925, 7152-7210, 8595-8730, 9580-9900, 22080-22820`
- Modify: `src/reducer.rs:318-323, 870-885, 5140-5300`
- Modify: `src/store/writer.rs:1-20, 950-995, 1275-1310, 1540-1585`
- Modify: `src/store/mod.rs:1080-1320, 3815-3895, 5315-5715`
- Modify tests only: `src/identity.rs:2010-2140`, `tests/restore.rs:40-120`, and `tests/convergence.rs:150-225`.
- Test in these eight existing Rust files; create no new file.

**Interfaces:**
- Consumes: the frozen `Arc<PersistHistoryDrain>` already stored in `AdapterProviderWorker::history_manifests` and the existing `upsert_history_drain(&Transaction, &PersistHistoryDrain)` immutability checks.
- Produces: `HistoryDrainBarrier::new(manifest: Arc<PersistHistoryDrain>, observed_at_ms: i64) -> Self`.
- Produces: `HistoryDrainBarrier::drain_id(&self) -> &HistoryDrainId`, derived from `self.manifest.drain_id`.
- Produces: `StagedHistoryFinalization { manifest: Arc<PersistHistoryDrain>, observed_at_ms: i64 }`.
- Produces: `WriterClient::finalize_history_drain(&mut self, manifest: Arc<PersistHistoryDrain>, observed_at_ms: i64) -> Result<HistoryDrainFinalization, WriterError>`.
- Produces: `Store::finalize_history_drain(&mut self, manifest: &PersistHistoryDrain, observed_at_ms: i64) -> Result<HistoryDrainFinalization, StoreError>`.
- Preserves: `WriterClient::history_drain_finalized(&HistoryDrainId)` and `WriterClient::history_drain_finalization(&HistoryDrainId)` signatures and absent-row behavior.

- [ ] **Step 1: Add collector regressions against the current API**

Add `zero_event_history_barrier_persists_and_finalizes_manifest` in `src/herdr/collector.rs`. It creates a temporary Store/writer, `RuntimePersistence`, reducer, and inactive provider integration, but sends no provider event before this control. At this RED stage, deliberately use the current ID-taking barrier constructor:

```rust
let manifest = Arc::new(PersistHistoryDrain {
    drain_id: HistoryDrainId::new("codex:zero-event-barrier").unwrap(),
    provider: Provider::Codex,
    created_at_ms: 1_000,
    artifacts: Vec::new(),
});
let barrier = HistoryDrainBarrier::new(manifest.drain_id.clone(), 2_000);
let acknowledgement = barrier.acknowledgement();
service_provider_event(
    Some(ProviderIngressEvent {
        event: ProviderEvent::SourceState {
            provider: Provider::Codex,
            state: ProviderSourceState::HistoryDrainBarrier(barrier),
        },
        admission: None,
        origin: ObservationOrigin::Live,
        history_manifest: None,
    }),
    &mut provider,
    "zero-event-barrier",
    &mut reducer,
    &shared,
    &mut persistence,
).await.unwrap();
assert!(acknowledgement.is_committed());
assert!(provider.held_history_barrier.is_none());
```

Read the exact page through the existing writer readback or `open_reader(&root)`; do not expose a production-private field solely for the test. Require the page's drain ID and finalized time and require no runs.

Add `duplicate_only_history_barrier_persists_and_finalizes_manifest`. It must:

1. send one existing `ProviderEvent::Synthesized` fixture as `ObservationOrigin::Live` and wait for durability;
2. send the byte-equivalent event with the same event ID as historical and `Some(Arc::clone(&manifest))`, forcing `apply_normalized_provider_event` through `events.is_empty()`;
3. prove `history_drain_finalization(&manifest.drain_id) == None` before the barrier;
4. send the current-API barrier with `manifest.drain_id.clone()`; and
5. require committed acknowledgement, an exact finalization page, healthy persistence, and one durable event row rather than a duplicate.

- [ ] **Step 2: Run the collector regressions and record runtime RED**

```bash
cargo test --locked --lib herdr::collector::tests::zero_event_history_barrier_persists_and_finalizes_manifest -- --exact --nocapture
cargo test --locked --lib herdr::collector::tests::duplicate_only_history_barrier_persists_and_finalizes_manifest -- --exact --nocapture
```

Expected: both tests compile and reach the real Store/writer path, then fail with `history drain does not exist`. Record the exact runtime diagnostic before adding any signature-dependent test.

- [ ] **Step 3: Add Store RED tests for missing, conflicting, and rolled-back manifests**

Add three tests beside the existing history-drain Store tests. The first calls finalization with a previously absent empty manifest and asserts the row and empty finalization page commit together:

```rust
#[test]
fn finalize_history_drain_upserts_missing_manifest_atomically() {
    let (_directory, root) = test_root();
    let mut store = open_writer(&root).unwrap();
    let manifest = PersistHistoryDrain {
        drain_id: HistoryDrainId::new("codex:barrier-owned-empty").unwrap(),
        provider: Provider::Codex,
        created_at_ms: 1_000,
        artifacts: Vec::new(),
    };
    assert_eq!(store.history_drain_finalization(&manifest.drain_id).unwrap(), None);

    let page = store.finalize_history_drain(&manifest, 2_000).unwrap();

    assert_eq!(page.drain_id, manifest.drain_id);
    assert_eq!(page.finalized_at_ms, 2_000);
    assert!(page.runs.is_empty());
    assert!(store.history_drain_finalized(&page.drain_id).unwrap());
}
```

The conflict test finalizes a manifest with `a.jsonl/dev:1/100`, then retries the same drain ID with `b.jsonl/dev:2/200`. It must return `StoreError::InvalidData`, preserve the original `history_drain_manifest`, and return the exact first finalization page.

The rollback test installs this temporary trigger before finalizing a missing manifest:

```rust
store.connection.execute_batch(
    "CREATE TEMP TRIGGER force_finalize_rollback \
     BEFORE UPDATE OF finalized_at_ms ON history_drains \
     WHEN NEW.drain_id = 'codex:forced-finalize-rollback' \
     BEGIN SELECT RAISE(ABORT, 'forced finalization failure'); END;",
).unwrap();
```

Call `finalize_history_drain(&manifest, 2_000)`, require an error, then require `history_drain_finalization == None` and `history_drain_finalized` to retain its absent-row `InvalidData`. This proves insertion is rolled back when later finalization SQL fails.

- [ ] **Step 4: Run the Store tests and record RED**

```bash
cargo test --locked --lib store::tests::finalize_history_drain_upserts_missing_manifest_atomically -- --exact --nocapture
cargo test --locked --lib store::tests::finalize_history_drain_conflict_preserves_committed_manifest -- --exact --nocapture
cargo test --locked --lib store::tests::finalize_history_drain_rolls_back_new_manifest_when_finalize_fails -- --exact --nocapture
```

Expected before production edits: compilation fails because `Store::finalize_history_drain` expects `&HistoryDrainId`, not `&PersistHistoryDrain`. Record the exact compiler diagnostic.

- [ ] **Step 5: Add writer RED coverage for a missing manifest with lost acknowledgement**

Use `spawn_writer_with_dropped_apply_ack` without pre-inserting the manifest:

```rust
#[tokio::test]
async fn writer_finalization_of_missing_manifest_survives_lost_ack() {
    let (_directory, root) = test_root();
    let store = open_writer(&root).unwrap();
    let manifest = Arc::new(PersistHistoryDrain {
        drain_id: HistoryDrainId::new("codex:barrier-lost-ack").unwrap(),
        provider: Provider::Codex,
        created_at_ms: 1_000,
        artifacts: Vec::new(),
    });
    let (lifecycle, mut writer) = writer::spawn_writer_with_dropped_apply_ack(store).unwrap();
    let failure = writer
        .finalize_history_drain(Arc::clone(&manifest), 2_000)
        .await
        .unwrap_err();
    assert!(matches!(failure, WriterError::Persistence(PersistenceFailure {
        operation: PersistenceOperation::Apply,
        phase: PersistencePhase::Acknowledgement,
        durability: DurabilityDisposition::Unknown,
        ..
    })));
    let first = writer.history_drain_finalization(&manifest.drain_id)
        .await.unwrap().unwrap();
    lifecycle.shutdown().await.unwrap();
    let mut store = open_writer(&root).unwrap();
    let replay = store.finalize_history_drain(manifest.as_ref(), 9_000).unwrap();
    assert_eq!(replay, first);
}
```

- [ ] **Step 6: Run the writer test and record RED**

```bash
cargo test --locked --lib store::tests::writer_finalization_of_missing_manifest_survives_lost_ack -- --exact --nocapture
```

Expected: compilation fails because the writer accepts a drain ID rather than the frozen manifest. Do not edit production code yet; all vertical-slice tests are written before the coordinated signature change.

- [ ] **Step 7: Add barrier and reducer RED tests**

In `src/provider/mod.rs`, add:

```rust
#[test]
fn history_barrier_owns_frozen_manifest_identity() {
    let manifest = Arc::new(PersistHistoryDrain {
        drain_id: HistoryDrainId::new("codex:owned-barrier").unwrap(),
        provider: Provider::Codex,
        created_at_ms: 1_000,
        artifacts: Vec::new(),
    });
    let barrier = HistoryDrainBarrier::new(Arc::clone(&manifest), 2_000);
    assert!(Arc::ptr_eq(&barrier.manifest, &manifest));
    assert_eq!(barrier.drain_id(), &manifest.drain_id);
    assert_eq!(barrier.observed_at_ms, 2_000);
}
```

Extend `pending_history_barrier_never_overtakes_ordinary_slots` to inspect `originated_events_for_test()` before flush and assert that the queued `HistoryDrainBarrier` contains the same `Arc`. Replaying the cloned barrier remains `MergeOutcome::Duplicate`, and ordinary slots still flush first.

In `src/reducer.rs`, construct a reducer with the existing test helper and add:

```rust
#[test]
fn staged_history_finalization_retains_barrier_manifest() {
    let manifest = Arc::new(PersistHistoryDrain {
        drain_id: HistoryDrainId::new("codex:staged-manifest").unwrap(),
        provider: Provider::Codex,
        created_at_ms: 1_000,
        artifacts: Vec::new(),
    });
    let barrier = HistoryDrainBarrier::new(Arc::clone(&manifest), 2_000);
    let staged = reducer.stage_history_finalization(&barrier);
    assert!(Arc::ptr_eq(&staged.manifest, &manifest));
    assert_eq!(staged.observed_at_ms, 2_000);
}
```

- [ ] **Step 8: Run barrier/staging tests and record RED**

```bash
cargo test --locked --lib provider::tests::history_barrier_owns_frozen_manifest_identity -- --exact --nocapture
cargo test --locked --lib reducer::tests::staged_history_finalization_retains_barrier_manifest -- --exact --nocapture
```

Expected: compilation fails because the barrier accepts a drain ID and staged finalization has no manifest.

- [ ] **Step 9: Implement the coordinated manifest-carrying vertical slice**

Make the signature changes together, after all RED tests above are recorded, so the worktree is not expected to compile between individual production edits.

In `src/provider/mod.rs`, replace the barrier's independent drain ID with the frozen manifest:

```rust
pub struct HistoryDrainBarrier {
    pub manifest: Arc<PersistHistoryDrain>,
    pub observed_at_ms: i64,
    acknowledgement: HistoryDrainAcknowledgement,
}

impl HistoryDrainBarrier {
    pub fn new(manifest: Arc<PersistHistoryDrain>, observed_at_ms: i64) -> Self {
        // Preserve the existing acknowledgement initialization.
    }

    pub fn drain_id(&self) -> &HistoryDrainId {
        &self.manifest.drain_id
    }
}
```

Update equality, merge, queue, debug, and test-only inspection code to derive identity from `drain_id()`. Do not introduce another copied ID or rebuild a manifest from an ID.

In `src/reducer.rs`, stage the same allocation:

```rust
pub struct StagedHistoryFinalization {
    pub manifest: Arc<PersistHistoryDrain>,
    pub observed_at_ms: i64,
}
```

`stage_history_finalization` must use `Arc::clone(&barrier.manifest)`. All finalization identity checks read `staged.manifest.drain_id`.

In `src/store/mod.rs`, change `Store::finalize_history_drain` to accept `&PersistHistoryDrain`. Start its existing transaction, call `upsert_history_drain(&transaction, manifest)?` before any finalization query or update, bind `&manifest.drain_id` everywhere the old parameter was used, and then continue with the exact existing finalization body at `src/store/mod.rs:1110-1313`. Commit once at the existing commit point. Do not weaken the provider/artifact conflict checks in `upsert_history_drain` or change finalization page construction.

In `src/store/writer.rs`, carry `Arc<PersistHistoryDrain>` in the finalization command and client method. The writer loop calls `store.finalize_history_drain(manifest.as_ref(), observed_at_ms)`. Preserve response delivery, durability classification, shutdown, and readback behavior exactly.

Change `AdapterProviderWorker::enqueue_history_barrier` to clone the frozen manifest rather than only its ID:

```rust
let Some(manifest) = self.history_manifests.get(&provider).cloned() else {
    return;
};
let barrier = HistoryDrainBarrier::new(manifest, unix_now_ms());
```

Change the two collector regressions recorded RED in Steps 1-2 from the temporary current-API `manifest.drain_id.clone()` constructor argument to `Arc::clone(&manifest)`. Keep the existing accepted/coalesced/duplicate/at-capacity behavior. In `RuntimePersistence::finalize_history_drain`, pass `Arc::clone(&staged.manifest)` to the writer, and use `&staged.manifest.drain_id` for both response-only readbacks. `retry_held_history_barrier` stages and retries the same manifest without rebuilding it.

Leave the `events.is_empty()` early return unchanged. It no longer owns manifest correctness. Update every remaining `HistoryDrainBarrier::new` and direct Store/writer finalization call in the eight declared files to reuse its exact manifest. In `src/identity.rs`, `tests/restore.rs`, and `tests/convergence.rs`, bind the already constructed `PersistHistoryDrain` fixture to a local variable, clone it into the existing V6 batch, and pass a reference to that same value into Store finalization.

- [ ] **Step 10: Run the focused GREEN matrix and retain ordinary behavior**

```bash
cargo test --locked --lib store::tests::finalize_history_drain_upserts_missing_manifest_atomically -- --exact --nocapture
cargo test --locked --lib store::tests::finalize_history_drain_conflict_preserves_committed_manifest -- --exact --nocapture
cargo test --locked --lib store::tests::finalize_history_drain_rolls_back_new_manifest_when_finalize_fails -- --exact --nocapture
cargo test --locked --lib store::tests::writer_finalization_of_missing_manifest_survives_lost_ack -- --exact --nocapture
cargo test --locked --lib provider::tests::history_barrier_owns_frozen_manifest_identity -- --exact --nocapture
cargo test --locked --lib provider::tests::pending_history_barrier_never_overtakes_ordinary_slots -- --exact --nocapture
cargo test --locked --lib reducer::tests::staged_history_finalization_retains_barrier_manifest -- --exact --nocapture
cargo test --locked --lib herdr::collector::tests::zero_event_history_barrier_persists_and_finalizes_manifest -- --exact --nocapture
cargo test --locked --lib herdr::collector::tests::duplicate_only_history_barrier_persists_and_finalizes_manifest -- --exact --nocapture
cargo test --locked --lib herdr::collector::tests::history_barrier_writer_outcome_matrix_retries_until_commit_known -- --exact --nocapture
cargo test --locked --lib reducer::tests::historical_activity_publishes_model_and_operator_only_at_durable_finalization -- --exact --nocapture
cargo test --locked --lib store::tests::ready_run_history_resume_without_terminal_fact_finalizes_unknown_once -- --exact --nocapture
```

- [ ] **Step 11: Run Task 1 scope gates**

```bash
cargo fmt --all -- --check
cargo test --locked --lib provider::tests -- --nocapture
cargo test --locked --lib reducer::tests -- --nocapture
cargo test --locked --lib store::tests -- --nocapture
cargo test --locked --lib herdr::collector::tests -- --nocapture
cargo test --locked --lib identity::tests::binding_merge_folds_all_v6_run_state_and_restores -- --exact --nocapture
cargo test --locked --test restore incomplete_history_stays_suppressed_and_completed_drain_restores -- --exact --nocapture
cargo test --locked --test convergence historical_working_state_is_not_published_before_durable_finalization -- --exact --nocapture
git diff --check
git status --short
```

Expected: all commands exit zero. The final status lists only the eight declared Task 1 files.

- [ ] **Step 12: Return the uncommitted Task 1 result**

Report the exact changed-file set, RED and GREEN evidence, final signatures, acknowledgement-loss result, rollback result, and any coverage gap. Do not stage, commit, push, merge, rebase, or delegate.

---

### Task 2: Characterize Runtime Event IDs and Remove the Clone

**Files:**
- Modify and test: `src/provider/lane.rs:960-995` and its same-file test module.

**Interfaces:**
- Consumes: `root_runtime_state_event(..., state: ExecState) -> ProviderEvent`.
- Produces: identical `ProviderEvent::AgentUpsert` state and exact event IDs for `ExecState::Working` and `ExecState::Idle`, without cloning the owned state.
- Produces no public type, function, configuration, persistence, or lifecycle change.

- [ ] **Step 1: Add the two-state characterization test**

```rust
#[test]
fn root_runtime_state_event_preserves_state_and_suffix() {
    let artifact = Path::new("rollout-runtime.jsonl");
    let scope = SessionScope::Codex {
        rollout_id: "runtime-root".to_owned(),
    };
    for (state, suffix) in [
        (ExecState::Working, "working"),
        (ExecState::Idle, "idle"),
    ] {
        let event = root_runtime_state_event(artifact, 7, 3, &scope, 11_000, state.clone());
        match event {
            ProviderEvent::AgentUpsert { state: observed, event_id, .. } => {
                assert_eq!(observed, Some(state));
                assert_eq!(
                    event_id,
                    format!("prov:codex:runtime:rollout-runtime.jsonl:7:3:{suffix}")
                );
            }
            event => panic!("unexpected runtime event: {event:?}"),
        }
    }
}
```

- [ ] **Step 2: Run the characterization baseline**

```bash
cargo test --locked --lib provider::lane::tests::root_runtime_state_event_preserves_state_and_suffix -- --exact --nocapture
```

Expected: PASS on the starting implementation. This is intentionally a characterization GREEN, not a RED; it freezes both exact suffixes and states before the mechanical change.

- [ ] **Step 3: Remove the clone without changing the event**

```rust
let suffix = match &state {
    ExecState::Working => "working",
    ExecState::Idle => "idle",
    _ => unreachable!("only live Codex runtime states are emitted"),
};

// In the existing ProviderEvent::AgentUpsert literal, replace only these fields:
state: Some(state),
event_id: format!("prov:codex:runtime:{basename}:{ordinal}:{sequence}:{suffix}"),
```

Do not add `Copy` to `ExecState`, change accepted variants, alter event ordering, or edit another function.

- [ ] **Step 4: Run Task 2 gates**

```bash
cargo fmt --all -- --check
cargo test --locked --lib provider::lane::tests::root_runtime_state_event_preserves_state_and_suffix -- --exact --nocapture
cargo test --locked --lib provider::lane::tests -- --nocapture
git diff --check
git status --short
```

Expected: all commands exit zero and the only changed file is `src/provider/lane.rs`.

- [ ] **Step 5: Return the uncommitted Task 2 result**

Report the characterization command before and after cleanup, exact diff, actual changed-file set, and any coverage gap. Do not stage, commit, push, merge, rebase, or delegate.

---

## Approved Bounded Cell 3 Hook Correction

This acceptance-discovered correction is a bounded post-plan task, not a new
subsystem or lifecycle design. The approved behavior is to ignore only a
Claude `SubagentStop` whose `agent_type` field is explicitly present and empty.
Absent and non-empty types retain the current complete-envelope mapping and
lost-start recovery; Codex hooks and the reducer remain unchanged.

One fresh `claude-implementer` works on branch
`agent/pr21-claude-empty-stop-filter` in the dedicated linked worktree
`/home/mageyuki/git/mageyuki/herdr-top/.worktrees/pr21-claude-empty-stop-filter`
at base `2e27e1cbbb50399fe06ecafde390fb5e7d2a4ca0` and may change only:

```bash
git worktree add -b agent/pr21-claude-empty-stop-filter \
  /home/mageyuki/git/mageyuki/herdr-top/.worktrees/pr21-claude-empty-stop-filter \
  2e27e1cbbb50399fe06ecafde390fb5e7d2a4ca0
```

The Controller has already executed this preparation command; it is recorded
for reproducibility and must not be executed a second time.

```text
src/hook_adapter.rs
tests/controller.rs
docs/guides/controller-emit-setup.md
docs/design/herdr-top-mvp.md
```

The implementation uses TDD in this order:

1. Add unit test
   `claude_explicit_empty_subagent_stop_maps_to_empty` and integration test
   `emit_from_hook_ignores_redacted_claude_explicit_empty_stop_without_delivery`.
   The integration fixture preserves every captured structural key but replaces
   every identifier, path, CWD, prompt ID, message, and nested background-task
   value with synthetic redacted sentinels. Assert those sentinels never reach
   the socket. Require RED because the current adapter emits `complete` and
   creates a terminal forward reference.
2. Add or retain exact controls
   `subagent_stop_maps_to_exact_complete_envelope` for absent type,
   `claude_null_subagent_type_stop_retains_complete_envelope`,
   `claude_nonempty_subagent_type_stop_retains_complete_envelope`, and
   `codex_explicit_empty_subagent_stop_retains_complete_envelope`. Add a
   non-empty type to the stop payload in
   `emit_from_hook_treats_terminal_before_start_stale_event_as_benign` so it
   pins the typed Claude stop-before-start and forward-reference path.
3. Filter only `HookProvider::ClaudeCode` with
   `payload.agent_type.as_deref() == Some("")` before constructing the terminal
   envelope. Do not inspect transcript paths, change
   reducer semantics, or add a schema or counter.
4. Update the two declared normative documents to explain the discriminator
   and retained lost-start behavior.
5. Run the new RED test names exactly before production, then run these exact
   GREEN and compatibility gates:

```bash
cargo test --locked --lib hook_adapter::tests::claude_explicit_empty_subagent_stop_maps_to_empty -- --exact --nocapture
cargo test --locked --test controller emit_from_hook_ignores_redacted_claude_explicit_empty_stop_without_delivery -- --exact --nocapture
cargo test --locked --lib hook_adapter::tests::subagent_stop_maps_to_exact_complete_envelope -- --exact --nocapture
cargo test --locked --lib hook_adapter::tests::claude_null_subagent_type_stop_retains_complete_envelope -- --exact --nocapture
cargo test --locked --lib hook_adapter::tests::claude_nonempty_subagent_type_stop_retains_complete_envelope -- --exact --nocapture
cargo test --locked --lib hook_adapter::tests::codex_explicit_empty_subagent_stop_retains_complete_envelope -- --exact --nocapture
cargo test --locked --test controller emit_from_hook_treats_terminal_before_start_stale_event_as_benign -- --exact --nocapture
cargo test --locked --test controller terminal_forward_reference_flagged -- --exact --nocapture
cargo test --locked --test controller task_started_stale_on_terminal -- --exact --nocapture
cargo test --locked --lib hook_adapter::tests -- --nocapture
cargo test --locked --test controller -- --nocapture
cargo fmt --all -- --check
git diff --check
git status --short
```

The actual changed-file set must be a subset of the four declared paths. The
worker returns an uncommitted diff and exact RED/GREEN evidence without staging,
committing, pushing, merging, rebasing, or delegating. Codex performs task
review and reruns every command. After successful review, Codex executes:

```bash
git add src/hook_adapter.rs tests/controller.rs docs/guides/controller-emit-setup.md docs/design/herdr-top-mvp.md
git commit -m "fix: ignore internal empty-type Claude stops" \
  -m "Co-Authored-By: OpenAI Codex <codex@openai.com>"
```

Codex then cherry-picks that exact commit serially onto
`agent/stable-task-history-rates` and reruns:

```bash
HOOK_FILTER_COMMIT=$(git -C /home/mageyuki/git/mageyuki/herdr-top/.worktrees/pr21-claude-empty-stop-filter rev-parse HEAD)
git -C /home/mageyuki/git/mageyuki/herdr-top/.worktrees/stable-task-history-rates cherry-pick "$HOOK_FILTER_COMMIT"
cargo test --locked --lib hook_adapter::tests -- --nocapture
cargo test --locked --test controller -- --nocapture
cargo fmt --all -- --check
make test
make lint
make build
git diff --check origin/main...HEAD
git status --short
```

Only after these post-integration gates pass may Cell 3 repeat.

---

## Controller Task Review and Serial Integration

- [ ] **Step 1: Independently review Task 1**

The actual changed files must be a subset of:

```text
src/provider/mod.rs
src/herdr/collector.rs
src/reducer.rs
src/store/writer.rs
src/store/mod.rs
src/identity.rs
tests/restore.rs
tests/convergence.rs
```

Inspect the complete diff, verify no second barrier drain ID remains, verify `upsert_history_drain` precedes the finalization query in the same transaction, and rerun every Task 1 RED/GREEN command. Reject integration if later finalization SQL changed without a test-backed need.

- [ ] **Step 2: Commit and integrate Task 1**

After successful Codex task review, commit on branch `agent/pr21-history-drain-finalization`:

```bash
git add src/provider/mod.rs src/herdr/collector.rs src/reducer.rs src/store/writer.rs src/store/mod.rs src/identity.rs tests/restore.rs tests/convergence.rs
git commit -m "fix: finalize barrier-owned history manifests" \
  -m "Co-Authored-By: OpenAI Codex <codex@openai.com>"
```

Cherry-pick that exact verified commit onto `agent/stable-task-history-rates`. Rerun both new collector tests and the lost-ack writer test on the integrated branch.

- [ ] **Step 3: Independently review Task 2**

The actual changed-file set must be exactly `src/provider/lane.rs`. Inspect the characterization and production hunk, rerun both Task 2 commands, and confirm no `Copy` derive or adjacent behavior change.

- [ ] **Step 4: Commit and integrate Task 2**

After successful Codex task review, commit on branch `agent/pr21-lane-clone-cleanup`:

```bash
git add src/provider/lane.rs
git commit -m "refactor: move provider runtime state into event" \
  -m "Co-Authored-By: OpenAI Codex <codex@openai.com>"
```

Cherry-pick that exact verified commit onto `agent/stable-task-history-rates`. Rerun the characterization test on the integrated branch.

- [ ] **Step 5: Run complete integrated repository gates**

```bash
cargo fmt --all -- --check
make test
make lint
make build
git diff --check origin/main...HEAD
git status --short
```

Expected: all commands exit zero and the worktree is clean. Record test counts and elapsed time. Do not start live Pane acceptance until this gate passes.

---

## Four-Cell Real-Pane Cold Acceptance

The Codex Controller performs this section after integrated repository gates. Implementation workers do not. Recheck `HERDR_ENV=1` immediately before the first `herdr` command.

### Common cell setup

For each cell, create distinct owner-only roots and a unique marker:

```bash
INTEGRATION_ROOT=/home/mageyuki/git/mageyuki/herdr-top/.worktrees/stable-task-history-rates
CELL_ROOT=$(mktemp -d /tmp/herdr-top-pr21-cold.XXXXXXXX)
chmod 700 "$CELL_ROOT"
mkdir -m 700 "$CELL_ROOT/state" "$CELL_ROOT/runtime" "$CELL_ROOT/plugin" "$CELL_ROOT/evidence"
CELL_MARKER="pr21-$(date -u +%Y%m%dT%H%M%SZ)-$(basename "$CELL_ROOT")"
printf '%s\n' "$CELL_MARKER" > "$CELL_ROOT/evidence/marker.txt"
date -u +%s%3N > "$CELL_ROOT/evidence/start-ms.txt"
```

Set `CELL_LABEL` to one of `pr21-codex-codex`, `pr21-codex-claude`, `pr21-claude-codex`, or `pr21-claude-claude`, then create the TUI pane and parse its current ID rather than guessing it:

```bash
TAB_JSON=$(herdr tab create --workspace w1 --cwd "$INTEGRATION_ROOT" --label "$CELL_LABEL" --no-focus)
TUI_PANE=$(printf '%s' "$TAB_JSON" | jq -r '.result.root_pane.pane_id')
test -n "$TUI_PANE"
```

Launch the exact integrated release binary in that pane. The wrapper records the foreground PID before `exec` so cold stop can target only this scratch process:

```bash
TUI_COMMAND="sh -c 'echo \$\$ > \"$CELL_ROOT/tui.pid\"; exec env XDG_STATE_HOME=\"$CELL_ROOT/state\" XDG_RUNTIME_DIR=\"$CELL_ROOT/runtime\" HERDR_PLUGIN_STATE_DIR=\"$CELL_ROOT/plugin\" HERDR_SESSION=herdr-top HERDR_TOP_HEADLESS_INACTIVITY_MS=30000 HERDR_TOP_COMPLETE_GRACE_MS=1000 HERDR_TOP_BACKFILL_WINDOW_MS=600000 \"$INTEGRATION_ROOT/target/release/herdr-top\" --session herdr-top'"
herdr pane run "$TUI_PANE" "$TUI_COMMAND"
herdr pane wait-output "$TUI_PANE" --match LIVE --timeout 120000
herdr pane read "$TUI_PANE" --source recent-unwrapped --lines 160 > "$CELL_ROOT/evidence/tui-start.txt"
STATE_ROOT=$(cat "$CELL_ROOT/plugin/state-root.txt")
DATABASE="$STATE_ROOT/herdr-top.sqlite3"
test -f "$DATABASE"
```

Every Python SQLite probe must open the live WAL database read-only, without `immutable=1`:

```python
connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
```

Before child dispatch, save the current `task_runs.run_id` set through that connection. Determine the controller's exact native identity: use `provider=codex` and `$CODEX_SESSION_ID` for Cells 1-2; for Cells 3-4, wait for the raw `SessionStart` hook and extract its top-level `session_id`, then use `provider=claude`. Poll at one-second intervals for at most 120 seconds. The provider-specific predicate must return exactly one row.

For a Codex controller, require its Working Agent upsert:

```sql
SELECT run.run_id
FROM task_runs AS run
JOIN agent_nodes AS node ON node.task_run_id = run.run_id
WHERE run.native_provider = ?1
  AND run.native_session_id = ?2
  AND run.merged_into IS NULL
  AND node.provider = ?1
  AND node.native_session_id = ?2
  AND node.state = 'working';
```

For a Claude controller, whose activity events intentionally leave Agent state null, require a running exact-native Task Run plus positive Agent activity:

```sql
SELECT run.run_id
FROM task_runs AS run
JOIN agent_nodes AS node ON node.task_run_id = run.run_id
WHERE run.native_provider = 'claude'
  AND run.native_session_id = ?1
  AND run.merged_into IS NULL
  AND run.task_state = 'running'
  AND node.provider = 'claude'
  AND node.native_session_id = ?1
  AND node.state IS NULL
  AND node.last_activity_at_ms IS NOT NULL;
```

Each unsuccessful iteration closes the read-only connection before sleeping. Save the unique run ID to `evidence/controller-run-id.txt`; zero rows at the deadline or more than one row at any time fails the cell. Record elapsed milliseconds from `start-ms.txt`; process existence alone is not controller readiness.

### Cell 1: Codex Controller to Codex child

From this Codex Controller, dispatch one fresh read-only `worker` subagent with this exact brief:

```text
Acceptance probe only. Calculate 17 * 19. Return the decimal result 323 and the supplied cell marker. Do not edit files, run another AI agent, or delegate.
```

Record the child provider-native session ID from the subagent result. Capture the TUI while it is working and after it is done beneath the current Codex controller.

### Cell 2: Codex Controller to Claude child

From this Codex Controller, dispatch one fresh read-only `claude-reviewer`
custom agent. Freeze the cell marker in its inline dispatch and ask the one
direct Claude CLI run to verify byte equality and return it. The brief prohibits
edits and re-delegation and records this as an acceptance probe rather than a
mandatory review gate. Derive the outer wrapper rollout ID from the linked
`task_runs.native_session_id`, and verify the same ID in its rollout filename.
Take only the inner Claude session ID, model, and exit statuses from the
validated wrapper report. Capture the managed outer wrapper row and the
validated inner result as separate evidence.

Production `claude-reviewer` runs its inner CLI with both `--safe-mode` and
`--no-session-persistence`. The inner process therefore emits neither hooks nor
a Claude transcript and is not itself a Herdr task. Do not remove either flag,
change live agent configuration, or infer lifecycle from the wrapper's
free-form command bytes. For this cell the managed target is the outer Codex
wrapper: capture its linked working-to-done transition, exact native binding,
Task Run, Agent Node, edge, stale-boundary result, and cold-restore result.
Independently retain and validate the wrapper's structured report, including
inner Claude exit `0`, validator exit `0`, exact marker equality, model, and
inner Claude session ID. Save and require a single zero row from this
provider-agnostic reference query; any partial or unattached inner reference
fails the cell:

```sql
SELECT
  (SELECT COUNT(*) FROM task_runs
   WHERE native_session_id = ?1 OR key_native_sid = ?1) AS task_run_refs,
  (SELECT COUNT(*) FROM agent_nodes
   WHERE native_session_id = ?1) AS agent_node_refs,
  (SELECT COUNT(*) FROM native_agent_sessions
   WHERE native_session_id = ?1) AS native_session_refs;
```

Required result: `0|0|0` for the validated inner Claude SID.

### Claude hook capture for Cells 3 and 4

For each Claude-controller cell, create a temporary additional settings file. Its command appends the exact raw stdin bytes before forwarding the same bytes to the integrated emitter:

```bash
HOOK_COMMAND="umask 077; tee -a '$CELL_ROOT/evidence/claude-hooks.raw.jsonl' | env XDG_STATE_HOME='$CELL_ROOT/state' XDG_RUNTIME_DIR='$CELL_ROOT/runtime' HERDR_SESSION=herdr-top '$INTEGRATION_ROOT/target/release/herdr-top' emit --from-hook claude-code"
jq -n --arg command "$HOOK_COMMAND" '
  {hooks: (["SessionStart","SessionEnd","SubagentStart","SubagentStop","TaskCreated","TaskCompleted"]
    | map({key: ., value: [{hooks: [{type: "command", command: $command}]}]})
    | from_entries)}' > "$CELL_ROOT/claude-hooks.json"
chmod 600 "$CELL_ROOT/claude-hooks.json"
```

Create a second pane in the cell tab, parse its returned ID, and run:

```bash
CLAUDE_PANE_JSON=$(herdr pane split "$TUI_PANE" --direction right --cwd "$INTEGRATION_ROOT" --no-focus)
CLAUDE_PANE=$(printf '%s' "$CLAUDE_PANE_JSON" | jq -r '.result.pane.pane_id')
test -n "$CLAUDE_PANE"
```

Then launch the controller in that pane:

```bash
claude --settings "$CELL_ROOT/claude-hooks.json" --name "$CELL_LABEL-controller"
```

Use `herdr pane wait-output "$CLAUDE_PANE" --match '>' --timeout 120000` for the interactive prompt. Wait for the raw `SessionStart`, derive its `session_id`, and apply the exact controller-link SQL predicate above before sending its child task.

### Cell 3: Claude Controller to Codex child

Send the Claude Controller this exact request:

```text
Acceptance probe only. As Controller, dispatch exactly one installed codex-reviewer agent to compare the supplied marker with itself and return it. Wait for that child, print the marker, edit nothing, and perform no other delegation. The codex-reviewer must execute its received role directly and must not delegate.
```

The managed and checked target is the Claude `codex-reviewer` wrapper. Capture
its exact Claude Controller-to-wrapper edge and working-to-done transition, and
retain every raw hook payload. Production `codex-reviewer` starts a new bare
`codex exec`: the Claude hooks do not carry its Codex session ID, its Codex
session metadata has no parent session, and the safe lineage grammar only
admits a known ID from `codex exec resume`. Therefore do not fabricate a
wrapper-to-inner edge or infer lifecycle from free-form Bash command bytes.
Instead, validate the wrapper's structured report and matching Codex rollout,
including inner exit `0`, exact marker equality, model, reasoning effort, and
inner Codex session ID. Apply the provider-agnostic reference query from Cell 2
to that inner SID and require `0|0|0`.

Immediately after the controller is linkable but before dispatch, save Doctor
JSON as `doctor-before-child.json` and require
`terminal_forward_reference_creations == 0`. After the wrapper completes but
before stopping the scratch TUI, save `doctor-after-child.json` and require the
same zero value. Then extract every raw stop whose `agent_type` is the exact
JSON string `""`. The count must be greater than zero; zero means the
discriminator was not live-exercised and the cell is inconclusive rather than
passed. Preserve the private raw bytes only in the owner-only evidence root;
do not copy them into the repository.

For every observed `(session_id, agent_id)` pair, construct the exact Task Run
key and hook event-ID prefix, bind them as `?1` through `?4`, and require one
row containing `0|0|0|0`:

```sql
SELECT
  (SELECT COUNT(*) FROM task_runs
   WHERE key_controller_id = ?1) AS task_run_refs,
  (SELECT COUNT(*) FROM events
   WHERE substr(event_id, 1, length(?2)) = ?2) AS event_refs,
  (SELECT COUNT(*) FROM agent_nodes
   WHERE provider = 'claude' AND native_session_id = ?3) AS agent_refs,
  (SELECT COUNT(*) FROM native_agent_sessions
   WHERE provider = 'claude' AND native_session_id = ?4) AS native_refs;
```

Here `?1` is
`hook:claude-code:<session_id>:agent:<agent_id>`, `?2` is
`hook:claude-code:<session_id>:SubagentStop:<agent_id>:`, and `?3` and `?4`
are the exact agent ID. Separately require that every non-empty or absent-type
stop in the cell either has its matching start and normal managed identity or
remains a failed-cell lost-start forward reference; the explicit-empty
exception must not waive any other stop-only row.

### Cell 4: Claude Controller to Claude child

Send the Claude Controller this exact request:

```text
Acceptance probe only. As Controller, dispatch exactly one direct general-purpose Claude subagent to calculate 17 * 19 and return 323 plus the supplied marker. Wait for that child, print the marker, edit nothing, and prohibit nested delegation.
```

Capture working then done under the Claude controller and retain every raw hook payload.

### Per-cell durable checks

For the Codex-managed targets in Cells 1 and 2, run and save these read-only
SQL results for the exact provider/native session ID:

```sql
PRAGMA quick_check;
PRAGMA foreign_key_check;

SELECT run_id, key_kind, key_controller_id, key_provider, key_native_sid,
       native_provider, native_session_id, task_state, finished_at_ms,
       native_session_end_status, history_ready
FROM task_runs
WHERE native_provider = ?1 AND native_session_id = ?2;

SELECT agent_node_id, provider, native_session_id, task_run_id,
       parent_agent_node_id, state, last_activity_at_ms
FROM agent_nodes
WHERE provider = ?1 AND native_session_id = ?2;

SELECT edge.parent_run_id, edge.child_run_id
FROM execution_edges AS edge
JOIN task_runs AS child ON child.run_id = edge.child_run_id
WHERE child.native_provider = ?1 AND child.native_session_id = ?2;

SELECT native_provider, native_session_id, COUNT(*)
FROM task_runs
WHERE native_session_id IS NOT NULL
GROUP BY native_provider, native_session_id
HAVING COUNT(*) > 1;
```

For the Claude-managed targets in Cells 3 and 4, use the exact controller SID
and hook agent ID from the raw matching `SubagentStart`/`SubagentStop` pair.
The production hook shape deliberately keys the child Task Run by the compound
controller identity while its Agent Node and `native_agent_sessions` entry use
the agent ID. Run and save:

```sql
SELECT run_id, key_kind, key_controller_id, task_state, finished_at_ms,
       history_ready
FROM task_runs
WHERE key_kind = 'controller'
  AND key_controller_id =
      'hook:claude-code:' || ?1 || ':agent:' || ?2;

SELECT child.agent_node_id, child.provider, child.native_session_id,
       child.task_run_id, child.parent_agent_node_id, child.state,
       child.last_activity_at_ms, parent.native_session_id AS parent_native_sid
FROM agent_nodes AS child
JOIN agent_nodes AS parent
  ON parent.agent_node_id = child.parent_agent_node_id
WHERE child.provider = 'claude'
  AND child.native_session_id = ?2
  AND parent.provider = 'claude'
  AND parent.native_session_id = ?1;

SELECT provider, native_session_id
FROM native_agent_sessions
WHERE provider = 'claude' AND native_session_id = ?2;

SELECT edge.parent_run_id, edge.child_run_id
FROM execution_edges AS edge
JOIN task_runs AS parent ON parent.run_id = edge.parent_run_id
JOIN task_runs AS child ON child.run_id = edge.child_run_id
WHERE parent.native_provider = 'claude'
  AND parent.native_session_id = ?1
  AND child.key_controller_id =
      'hook:claude-code:' || ?1 || ':agent:' || ?2;
```

Required for every cell: quick check is `ok`; foreign-key and duplicate queries
return zero rows; exactly one managed-target Task Run, exact child Agent Node,
and expected parent edge exist; and the Task Run renders done after the stale
boundary and cold restart. For Cells 1 and 2, additionally require the exact
native-bound Task Run, Agent state `ended`, and approved exact-native fallback.
For Cells 3 and 4, require the exact compound hook key, child Agent parent link,
one native-agent-session entry, Task state `completed`, and non-null
`finished_at_ms`; the Claude hook lifecycle does not require the Agent Node's
optional state column to be `ended`. In Cells 2 and 3, follow the outer-wrapper
checks with the separate `0|0|0` reference check for the validated inner SID.

Wait beyond the configured positive 30,000 ms staleness threshold, capture the TUI again, and require the child Task Run to remain done when the ended Agent row is no longer a visible tree row. Stop only the scratch TUI:

```bash
TUI_PID=$(cat "$CELL_ROOT/tui.pid")
test "$TUI_PID" -gt 1
kill -TERM "$TUI_PID"
for _attempt in $(seq 1 30); do
  if ! kill -0 "$TUI_PID" 2>/dev/null; then break; fi
  sleep 1
done
! kill -0 "$TUI_PID" 2>/dev/null
herdr pane run "$TUI_PANE" "stty sane; printf 'PR21_TERMINAL_RESTORED\\n'"
herdr pane wait-output "$TUI_PANE" --match PR21_TERMINAL_RESTORED --timeout 30000
```

`SIGTERM` is used only for the exact scratch PID; the explicit `stty sane` round trip proves the resumed parent shell can accept input and restores terminal modes before relaunch. Restart the same command in the same pane and same roots, use `herdr pane wait-output "$TUI_PANE" --match LIVE --timeout 120000`, and require the same linked done row without duplication.

Run doctor against the restarted scratch owner:

```bash
env XDG_STATE_HOME="$CELL_ROOT/state" \
  XDG_RUNTIME_DIR="$CELL_ROOT/runtime" \
  HERDR_PLUGIN_STATE_DIR="$CELL_ROOT/plugin" \
  HERDR_SESSION=herdr-top \
  HERDR_TOP_HEADLESS_INACTIVITY_MS=30000 \
  HERDR_TOP_COMPLETE_GRACE_MS=1000 \
  HERDR_TOP_BACKFILL_WINDOW_MS=600000 \
  "$INTEGRATION_ROOT/target/release/herdr-top" doctor --session herdr-top --json \
  > "$CELL_ROOT/evidence/doctor-after-restart.json"
```

Apply this exact predicate to the doctor report:

```bash
jq -e '
  .controller.runtime.observed.persistence.status == "healthy" and
  .controller.runtime.observed.persistence_counters.not_committed_batches == 0 and
  .controller.runtime.observed.persistence_counters.durability_unknown_batches == 0 and
  .controller.runtime.observed.persistence_counters.committed_but_degraded_batches == 0 and
  .controller.runtime.observed.persistence_counters.skipped_batches == 0 and
  .controller.runtime.observed.persistence_counters.skipped_owner_updates == 0 and
  .controller.runtime.observed.persistence_counters.skipped_enqueues == 0 and
  .controller.runtime.observed.controller_counters.binding_conflicts == 0 and
  .controller.runtime.observed.controller_counters.terminal_forward_reference_creations == 0 and
  .controller.runtime.observed.controller_counters.provider_identity_disagreements == 0
' "$CELL_ROOT/evidence/doctor-after-restart.json"
```

Search the scratch log for `history drain does not exist`, native-session `UNIQUE constraint failed`, `persistence_degraded`, and skipped-update occurrences; any match fails the cell.

Diff pre-dispatch and post-restart Task Run IDs. Every new row must be the controller, managed target, planned direct child, or explicitly expected wrapper. A new parentless or subjectless row fails until event rows, provider artifact, and, for Claude, raw hook bytes identify its source. A stop-only Claude identity without a matching start remains a blocker unless it has the exact explicit-empty Cell 3 shape and all four zero-reference checks above; in that case no Task Run exists to waive. The validated but intentionally unpersisted inner SID in Cell 2 or Cell 3 is not an expected row.

Record a four-row ledger with controller, requested child engine, managed
target, marker, managed native session ID, validated inner session ID when
applicable, exact controller-to-managed-target edge, time to controller link,
time to managed target done,
stale-display result, restart result, inner-result validation, doctor result,
integrity result, and unattached-row result. All four rows must pass.

---

## Shared Full-History Scratch Acceptance

- [ ] Create a fifth owner-only scratch root and real Herdr pane, but use `HERDR_TOP_BACKFILL_WINDOW_MS=86400000` with the real provider roots from the current user environment. Do not change the live Herdr Top state root.
- [ ] Use `herdr pane wait-output "$TUI_PANE" --match LIVE --timeout 120000`, then sample doctor twice at least 35 seconds apart. Apply the exact per-cell health/counter predicate to both reports. Require the second `.controller.runtime.observed.provider_counters.provider_cycles` to exceed the first, `egress_closed == 0` in both, and the second `egress_saturations` to equal the first after the bounded replay drains.
- [ ] Require at least one `history_drains` row, zero unexplained rows with `finalized_at_ms IS NULL`, zero native-binding duplicates, `PRAGMA quick_check = ok`, and zero `PRAGMA foreign_key_check` rows. All SQLite reads use `sqlite3.connect(f"file:{database}?mode=ro", uri=True)`.
- [ ] For each incomplete drain, construct its exact same-provider window from its `created_at_ms` through scratch shutdown. It is explainable only if: (1) exactly one pre-restart scratch-log `provider_record_malformed` occurrence has the same provider and a unique `(byte_offset, error_code)` inside that window; (2) no second incomplete same-provider drain overlaps that window; and (3) the second doctor sample has a higher `provider_cycles`, `malformed_records >= 1`, `egress_closed == 0`, stable `egress_saturations`, and all persistence/skipped counters zero. A malformed provider is marked failed for the rest of that process, so no in-process successor drain is required. After restart, require one of two explicit dispositions: either the row is finalized or has `completed_by_drain_id` naming a durably finalized drain, or the scratch log contains exactly one new `provider_record_malformed` occurrence after restart with the same provider, `byte_offset`, and `error_code`, while the same row remains the sole incomplete drain for that provider and doctor again proves advancing cycles, open egress, and zero persistence/skipped counters. Because the runtime has no direct held-barrier diagnostic, any missing, multiple, or temporally ambiguous match fails acceptance rather than being inferred as abandonment. These combined predicates are the operational evidence that no held barrier, pending mutation, or closed ingress remains.
- [ ] Search the scratch log for former drain ID `v1:455628923d6bf7bc7d24fdb949eb2576220fd610a47691c0b1134c0a8f484c4c`, `history drain does not exist`, persistence degradation, skipped updates, and native-session uniqueness failures. The former ID may appear only as a successfully finalized identifier, never in an error.
- [ ] Stop by exact PID, restore the pane with the same `stty sane` marker round trip, cold restart against the same scratch roots, use `herdr pane wait-output "$TUI_PANE" --match LIVE --timeout 120000`, and repeat doctor and SQLite checks.
- [ ] Account for every new Unattached Task Run from event and provider evidence. Any unexplained row blocks completion.

---

## Final Review and Publication Gates

- [ ] Update the research workspace with implementation commits, all automated commands, the four-cell ledger, full-history evidence, unavailable Grok sources, and residual uncertainty; synchronize mirror and canonical workspace byte-for-byte.
- [ ] Run exactly one mandatory `claude-reviewer` final whole-change review over `origin/main...HEAD`. Identify `e357aa6...bbd8b42` as already approved and focus the new review on the planning delta, both implementation commits, and integration seams. Adjudicate every finding against source, tests, docs, and live evidence.
- [ ] Verify every origin fetch and push URL again. All must parse as the same non-`aces-inc` GitHub owner `mageyuki`; otherwise stop.
- [ ] Push `agent/stable-task-history-rates` and update PR #21. Do not merge.
- [ ] Wait for a non-empty applicable CI set where every check is conclusive and success or applicable skipped, with zero failure, cancelled, timed-out, or action-required results.
- [ ] Request GitHub Copilot review on latest HEAD. For each finding, form a source-backed provisional judgment and obtain `claude-reviewer` judgment before editing, replying, or resolving.
- [ ] Resolve the existing provider-lane thread only after its characterization test and cleanup are on reviewed latest HEAD. If any fix is pushed, repeat CI and latest-HEAD Copilot review.
- [ ] Completion requires latest-HEAD green CI, no actionable unresolved Copilot finding, mandatory Claude final review ready, four-of-four cold acceptance, and shared-history cold acceptance. PR merge remains unauthorized.

## Rollback

The implementation adds no schema or migration. Before publication, Task 2 can be reverted independently if the mechanical cleanup is disputed. Revert Task 1 together with this planning delta if barrier-owned finalization must be withdrawn. Never validate rollback by starting the known missing-manifest build against live state. Scratch roots and evidence remain until PR #21 completes and are removed only after explicit destructive-action authorization.
