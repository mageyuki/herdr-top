# Native Binding Merge Recovery Implementation Plan

> **For the Controller and assigned implementation worker:** Execute the single
> coherent task directly. The worker must not start sub-agents, custom agents,
> helper agents, or additional reviewers. Steps use checkbox (`- [ ]`) syntax
> for tracking.

**Goal:** Prevent PR #21 provider-history replay from creating duplicate native-session ownership, then prove corrected replay reopens ingress and restores real child-task visibility.

**Architecture:** Keep `PersistV6Batch.operations` as event-time identity transitions and keep `PersistV6Batch.task_runs` as the final post-operation V6 projection. The reducer no longer back-propagates a final binding into earlier core upserts; the Store preseeds missing merge survivors without a binding, replays core operations in order, and applies the final V6 projection afterward.

**Tech Stack:** Rust 2024, Tokio, rusqlite/SQLite, cargo test, Make, Herdr socket CLI.

**Spec:** `docs/internal/superpowers/2026-08-29-native-binding-merge-recovery/spec.md`

## Global Constraints

- Preserve the UNIQUE invariant on `(native_provider, native_session_id)`; never disable or weaken it.
- Preserve `PersistV6Batch.operations` order and each operation's event-time identity/binding payload.
- Keep preseed writes binding-neutral only for final merge survivors; non-merge preseed behavior and promotion old-key behavior remain unchanged.
- Keep final `batch.task_runs` authoritative after core operations.
- Do not delete, reset, or directly modify the live SQLite database.
- TDD is mandatory: observe each new regression fail for the expected reason before changing production code.
- Implementation workers are not Controllers and must not delegate to any AI agent or reviewer.
- Implementation workers do not stage, commit, push, merge, or rebase.
- Controller integrates serially, runs the adjacent-pane acceptance, and owns every git/GitHub operation.

## File Map

- `src/reducer.rs` — preserve event-time core upserts while deriving the final canonical V6 task-run projection; host the batch-shape regression.
- `src/store/mod.rs` — make missing final merge-survivor preseeds binding-neutral; host real-SQLite direct/chained/replay regressions.
- `docs/internal/superpowers/2026-08-29-native-binding-merge-recovery/spec.md` — approved behavior and recovery contract; no implementation edit expected.
- `docs/internal/superpowers/plans/2026-08-29-native-binding-merge-recovery.md` — this reviewed execution plan; no implementation edit expected.

The implementation task's declared writable file set is exactly
`src/reducer.rs` and `src/store/mod.rs`. If implementation requires any other
file, stop and return to the Controller for scope review before editing it.

---

### Task 1: Preserve native-binding ownership through reducer and Store merge order

**Files:**

- Modify: `src/reducer.rs`
- Test: `src/reducer.rs` test module
- Modify: `src/store/mod.rs`
- Test: `src/store/mod.rs` test module

**Interfaces:**

- Consumes: existing `Reducer::finish_provider_observation`, `PersistV6Batch`, `PersistOp::MergeTaskRuns`, `Store::apply_v6_batch`, `canonical_run_after_operations`, reducer test helper `run`, and Store test helpers `run_op_with_key`, `binding`, and `merged_into`.
- Produces: unchanged public and crate-visible signatures; ordered core operations retain their original binding payloads, while missing final merge survivors are preseeded without `native_session` ownership.

- [ ] **Step 1: Add the failing reducer batch-shape regression**

Add `provider_observation_keeps_pre_merge_survivor_upsert_unbound` beside the
existing provider-observation merge tests in `src/reducer.rs`.

Define this test-local upsert closure with literal persistence timestamps:

```rust
let upsert = |run_id,
              key,
              ordinal,
              at_ms,
              controller_evidence,
              native_session| {
    let mut task_run = run(run_id, key, ordinal, TaskState::Running);
    task_run.has_controller_task_state_event = controller_evidence;
    task_run.created_at_ms = Some(at_ms);
    task_run.updated_at_ms = Some(at_ms);
    PersistOp::UpsertTaskRun(PersistTaskRun {
        task_run,
        native_session,
        created_at_ms: at_ms,
        updated_at_ms: at_ms,
        finished_at_ms: None,
    })
};
```

Construct four core moments for one survivor ID:

```rust
let operations = vec![
    upsert(
        survivor,
        RunKey::Controller("binding-order-controller".to_owned()),
        1,
        1_000,
        true,
        None,
    ),
    upsert(
        absorbed,
        RunKey::Native {
            provider: Provider::Codex,
            sid: sid.to_owned(),
        },
        2,
        1_100,
        false,
        Some(NativeSessionBinding {
            provider: Provider::Codex,
            native_session_id: sid.to_owned(),
        }),
    ),
    PersistOp::MergeTaskRuns { survivor, absorbed },
    upsert(
        survivor,
        RunKey::Controller("binding-order-controller".to_owned()),
        1,
        1_200,
        true,
        Some(NativeSessionBinding {
            provider: Provider::Codex,
            native_session_id: sid.to_owned(),
        }),
    ),
];
```

Initialize the reducer model with only the final canonical survivor under the
same Controller key and final Codex binding, call `begin_provider_observation`
with a historical origin, and pass these operations to
`finish_provider_observation`. The absorbed native key remains an alias of the
Controller-keyed survivor, matching `plan_controller_native` and
`merge_in_memory`. Assert all of the following with literal values:

```rust
let survivor_upserts = batch
    .operations
    .iter()
    .filter_map(|operation| match operation {
        PersistOp::UpsertTaskRun(value) if value.task_run.run_id == survivor => Some(value),
        _ => None,
    })
    .collect::<Vec<_>>();
assert_eq!(survivor_upserts.len(), 2);
assert!(matches!(
    survivor_upserts[0].task_run.key,
    RunKey::Controller(ref controller) if controller == "binding-order-controller"
));
assert_eq!(survivor_upserts[0].native_session, None);
assert!(matches!(
    survivor_upserts[1].task_run.key,
    RunKey::Controller(ref controller) if controller == "binding-order-controller"
));
assert_eq!(
    survivor_upserts[1]
        .native_session
        .as_ref()
        .map(|binding| binding.native_session_id.as_str()),
    Some(sid)
);
let final_run = batch
    .task_runs
    .iter()
    .find(|value| value.task_run.task_run.run_id == survivor)
    .unwrap();
assert_eq!(
    final_run
        .task_run
        .native_session
        .as_ref()
        .map(|binding| binding.native_session_id.as_str()),
    Some(sid)
);
assert!(matches!(
    final_run.task_run.task_run.key,
    RunKey::Controller(ref controller) if controller == "binding-order-controller"
));
assert!(batch.operations.iter().any(|operation| matches!(
    operation,
    PersistOp::MergeTaskRuns {
        survivor: actual_survivor,
        absorbed: actual_absorbed,
    } if *actual_survivor == survivor && *actual_absorbed == absorbed
)));
```

The production change that must make this test fail is the current loop that
replaces every matching earlier upsert with the last final-bound payload.

- [ ] **Step 2: Run the reducer regression and verify RED**

Run:

```bash
cargo test --lib reducer::tests::provider_observation_keeps_pre_merge_survivor_upsert_unbound -- --exact
```

Expected: FAIL because `survivor_upserts[0].native_session` is the final Codex
binding instead of `None`. A compile error or unrelated assertion is not an
acceptable RED result.

- [ ] **Step 3: Stop reducer back-propagation of the final projection**

In `Reducer::finish_provider_observation`:

1. Remove `mut` from the `operations: PersistBatch` parameter.
2. Keep the current selection and construction of `persisted`; it remains the
   final `PersistTaskRunV6.task_run` payload.
3. Delete only the loop that assigns `persisted.clone()` back into every
   matching `PersistOp::UpsertTaskRun` and `PersistOp::PromoteTaskRunKey`.
4. Leave touched-run canonicalization, readiness, watermarks, associations,
   receipts, and final `task_runs` sorting/deduplication unchanged.

The intended production delta is structurally:

```rust
// Keep final persisted construction above unchanged.
task_runs.push(PersistTaskRunV6 {
    task_run: persisted,
    state: persisted_state,
});
```

There must be no replacement pass over `operations` between final-persisted
construction and `task_runs.push`.

- [ ] **Step 4: Run the reducer regression and focused reducer neighbors**

Run:

```bash
cargo test --lib reducer::tests::provider_observation_keeps_pre_merge_survivor_upsert_unbound -- --exact
cargo test --lib reducer::tests::live_identity_merge_releases_ready_run_before_image_without_lifecycle_regression -- --exact
cargo test --lib reducer::tests::historical_agent_mutation_of_ready_run_stays_at_published_before_image -- --exact
```

Expected: all PASS.

- [ ] **Step 5: Add the failing real-SQLite merge-survivor preseed regression**

Add `v6_merge_survivor_preseed_is_binding_neutral` in the `src/store/mod.rs`
test module near existing merge tests.

Use a temporary real Store. First persist only `absorbed` as a native-keyed row
owning `sid` with display ordinal `2` through a V6 batch with literal core times
`1_900`,
`history_ready: false`, and `latest_provider_at_ms: Some(1_900)`. In that seed
transaction also create a non-finalized drain and associate it with `absorbed`.
This makes the later merge exercise the production drain-association repoint.

Construct `final_task_run` from the existing Store test helper. The survivor's
key stays Controller-shaped throughout; the merge transfers only the separate
`native_session` binding. Normalize its core creation time to the earlier
unbound upsert so exact replay can compare literal durable semantics:

```rust
let mut final_task_run = match run_op_with_key(
    survivor,
    RunKey::Controller("merge-preseed-controller".to_owned()),
    1,
    TaskState::Running,
    2_100,
    true,
    Some(NativeSessionBinding {
        provider: Provider::Codex,
        native_session_id: sid.to_owned(),
    }),
) {
    PersistOp::UpsertTaskRun(task_run) => PersistTaskRunV6 {
        task_run,
        state: TaskRunV6State {
            history_ready: false,
            latest_provider_at_ms: Some(2_100),
            ..TaskRunV6State::default()
        },
    },
    _ => unreachable!("run_op_with_key returns an upsert"),
};
final_task_run.task_run.created_at_ms = 2_000;
```

Then apply this production-shaped non-ingest V6 batch. Insert an Agent Node and
event referencing `absorbed` before the merge; use the seeded drain as
`history_event_drain`. Use `final_task_run.task_run.clone()` as the trailing
bound upsert so the first unbound upsert and final bound upsert have matching
Controller keys and literal `2_000`/`2_100` core timestamps:

```rust
let batch = PersistV6Batch {
    operations: vec![
        run_op_with_key(
            survivor,
            RunKey::Controller("merge-preseed-controller".to_owned()),
            1,
            TaskState::Running,
            2_000,
            true,
            None,
        ),
        PersistOp::UpsertAgentNode(AgentNode {
            agent_node_id: "merge-preseed-agent".to_owned(),
            provider: Provider::Codex,
            native_session_id: Some(sid.to_owned()),
            task_run_id: absorbed,
            display_ordinal: DisplayOrdinal::new(3),
            parent_agent_node_id: None,
            state: None,
            model_id: None,
            last_event_kind: None,
            last_tool_name: None,
            last_item_count: None,
            last_byte_count: None,
            last_activity_at_ms: None,
            session_file: None,
        }),
        PersistOp::RecordEvent {
            event: Box::new(run_event("merge-preseed-event", absorbed, 2_050)),
            seen_at_ms: 2_050,
        },
        PersistOp::MergeTaskRuns { survivor, absorbed },
        PersistOp::UpsertTaskRun(final_task_run.task_run.clone()),
    ],
    task_runs: vec![final_task_run],
    history_event_drain: Some(drain_id.clone()),
    ..PersistV6Batch::default()
};
```

Do not introduce a second production fixture builder. After apply, assert:

```rust
assert_eq!(binding(&store.connection, survivor), codex_binding(sid));
assert_eq!(binding(&store.connection, absorbed), (None, None));
assert_eq!(merged_into(&store.connection, absorbed), Some(survivor));
```

Also assert all of the following:

- the survivor key is still the literal Controller key and the absorbed row
  retains the native key as an alias;
- `COUNT(*)` for the SID binding is the literal value `1`;
- `referenced_run` reports `survivor` for both `merge-preseed-agent` and
  `merge-preseed-event`;
- `history_drain_run_ids(&drain_id)` is exactly `vec![survivor]`;
- `load_restored_state()` exposes one canonical survivor with
  `history_ready == false`, the final binding, and the absorbed native alias.

Capture sorted, complete SQL row projections for `task_runs`, `agent_nodes`,
`events`, `history_drain_runs`, and `history_event_before_images`; apply
`batch.clone()` a second time through the exact non-ingest identity replay path
and assert those full projections are identical before and after replay.

Finally, create a second temporary Store seeded with the same absorbed row and
drain association. Clone the production-shaped batch, append a deterministic
failing `MergeTaskRuns { survivor, absorbed: survivor }`, and apply it. Assert
the transaction returns `StoreError::InvalidData`, the survivor row, new Agent
Node, new event, and before-image rows are absent, the absorbed row still owns
the SID with `merged_into == None`, and the drain association still names only
`absorbed`. This is the atomic-rollback control required by the spec.

The production change that must make this test fail is clearing the merge
survivor's preseed binding. Before that change, `apply_v6_batch` must return the
observed UNIQUE failure; do not weaken the test to accept either outcome.

- [ ] **Step 6: Run the Store regression and verify RED**

Run:

```bash
cargo test --lib store::tests::v6_merge_survivor_preseed_is_binding_neutral -- --exact
```

Expected: FAIL at `apply_v6_batch(...).unwrap()` with
`UNIQUE constraint failed: task_runs.native_provider, task_runs.native_session_id`.

- [ ] **Step 7: Preserve the missing-row promotion preseed path**

Before changing production code, add
`v6_missing_promotion_preseed_uses_old_key_before_promotion` as a focused
passing characterization test. Start with an empty Store and a final
`PersistTaskRunV6` whose canonical key is native and owns its matching Codex
binding. Its only operation is `PromoteTaskRunKey` from a literal
`RunKey::NativePath` old key to that final payload, with a distinct
`alias_run_id`. Applying the batch must succeed and assert:

- the canonical row owns the promoted native key and exactly one SID binding;
- `alias_run_id` retains the old native-path key and points to the canonical
  row;
- no row remains both native-path keyed and canonical.

This test must pass before and after the merge change. It specifically enters
the `if !exists` preseed branch and protects the existing old-key rewrite while
merge-survivor binding neutralization is added beside it.

Run:

```bash
cargo test --lib store::tests::v6_missing_promotion_preseed_uses_old_key_before_promotion -- --exact
```

Expected before production edit: PASS.

- [ ] **Step 8: Make final merge-survivor preseeds binding-neutral**

In `Store::apply_v6_batch`, before the missing-row preseed loop, derive the set
of final survivors represented by any merge in the complete operation chain:

```rust
let final_merge_survivors = batch
    .operations
    .iter()
    .filter_map(|operation| match operation {
        PersistOp::MergeTaskRuns { survivor, .. } => {
            Some(canonical_run_after_operations(*survivor, &batch.operations))
        }
        _ => None,
    })
    .collect::<HashSet<_>>();
```

Inside the existing `if !exists` block, preserve promotion handling, then clear
only the preseed binding for a final merge survivor:

```rust
if final_merge_survivors.contains(&task_run.task_run.task_run.run_id) {
    seed.task_run.native_session = None;
}
```

Do not change the final `for task_run in batch.task_runs` upsert after core
operations. Do not change `merge_task_runs`, the UNIQUE indexes, transaction
scope, exact replay detection, or non-merge preseed behavior.

- [ ] **Step 9: Run the direct Store regression and promotion neighbor**

Run:

```bash
cargo test --lib store::tests::v6_merge_survivor_preseed_is_binding_neutral -- --exact
cargo test --lib store::tests::v6_missing_promotion_preseed_uses_old_key_before_promotion -- --exact
cargo test --lib store::tests::schema_v7_exact_non_ingest_identity_replay_requires_matching_durable_state -- --exact
```

Expected: all PASS.

- [ ] **Step 10: Add chained-merge and ingest exact-replay coverage**

Add `v6_chained_merge_preseed_transfers_binding_once_and_replays` in
`src/store/mod.rs`.

Start with only `first` durably owning `sid`. `first` keeps its native key;
`intermediate` and `final_survivor` use distinct Controller keys. Construct
ordered operations:

```rust
vec![
    PersistOp::AdvanceIngestSequence { ingest_seq: 41 },
    PersistOp::RecordEvent { /* ingest_seq 41, references first */ },
    controller_unbound_upsert(intermediate, 2_000),
    PersistOp::MergeTaskRuns {
        survivor: intermediate,
        absorbed: first,
    },
    controller_unbound_upsert(final_survivor, 2_100),
    PersistOp::MergeTaskRuns {
        survivor: final_survivor,
        absorbed: intermediate,
    },
    controller_bound_upsert(final_survivor, sid, 2_200),
]
```

Use existing `run_op_with_key` calls directly for the three named upserts; the
labels above describe their required literal payloads and are not new helper
functions. Build the event from `run_event`, set its metadata `ingest_seq` to
`Some(41)`, and put only the final bound `PersistTaskRunV6` in
`batch.task_runs`. The final payload must retain the final Controller key, use
`created_at_ms: 2_100`, `updated_at_ms: 2_200`,
`history_ready: true`, and `latest_provider_at_ms: Some(2_200)`. Give `first`,
`intermediate`, and `final_survivor` the distinct display ordinals `1`, `2`,
and `3`; seed `first` with `latest_provider_at_ms: Some(1_900)`. These values
make the MIN/OR/MAX semantics of core upsert and merge explicit.

Apply `batch.clone()` once, assert `first -> intermediate -> final_survivor`
canonicalization ends with exactly one SID owner at `final_survivor`, and assert
both `first` and `intermediate` have been flattened to point directly to
`final_survivor`; assert the event was repointed through both merges to
`final_survivor`. Then apply the identical batch again and assert the same
bindings and direct merge targets. The second apply must use the production
ingest replay path and return `Ok(())`.
Capture sorted complete `task_runs`, `events`, and ledger projections after the
first apply and assert byte-for-byte logical equality after replay. Also run the
existing non-ingest complete-merge-chain replay test so both replay routes are
covered.

- [ ] **Step 11: Run chained, retry, and ingress regressions**

Run:

```bash
cargo test --lib store::tests::v6_chained_merge_preseed_transfers_binding_once_and_replays -- --exact
cargo test --lib store::tests::schema_v7_exact_non_ingest_identity_replay_accepts_complete_merge_chain_only -- --exact
cargo test --lib herdr::collector::tests::committed_non_ingest_identity_replays_clear_pending_gate_and_finalize -- --exact
cargo test --lib herdr::collector::tests::failed_historical_mutation_retries_exactly_once_before_barrier_and_live_ingress -- --exact
cargo test --lib herdr::collector::tests::successful_probe_retries_retained_history_before_later_ingest_sequence -- --exact
```

Expected: all PASS. These existing collector tests are the regression gates for
pending mutation retention, exact committed replay, ingress reopening, and
later-event ordering; do not duplicate them with a mock-only test.

- [ ] **Step 12: Run task-level verification**

Run:

```bash
cargo fmt --all -- --check
cargo test --lib reducer::tests:: -- --test-threads=1
cargo test --lib store::tests:: -- --test-threads=1
cargo test --lib herdr::collector::tests::failed_historical_mutation_retries_exactly_once_before_barrier_and_live_ingress -- --exact
git diff --check
```

Expected: all commands exit 0. Report elapsed time and exact test counts where
the runner prints them.

- [ ] **Step 13: Return the uncommitted implementation for Controller review**

Report:

- actual changed files, which must be a subset of `src/reducer.rs` and
  `src/store/mod.rs`;
- RED command and expected failure evidence for each new regression;
- GREEN and task-level command results;
- any deviation from this plan;
- no staging, commit, push, merge, or rebase.

The Controller independently inspects the diff, reruns the exact commands, and
commits only after the task review passes.

## Controller Integration and Verification

- [ ] Commit Task 1 on its dedicated task branch only after independent Codex
  diff inspection and test reruns.
- [ ] Integrate Task 1 serially into `agent/stable-task-history-rates`; verify
  the actual changed file set remains within the declaration.
- [ ] Run:

```bash
make test
make lint
make build
cargo fmt --all -- --check
git diff --check origin/main...HEAD
```

- [ ] Copy the affected state root to a private temporary directory outside
  the repository, start the integrated binary against the copy, and verify
  restart/replay reaches healthy persistence without SQL repair. Never mutate
  the live root during this proof.
- [ ] Use `HERDR_ENV=1` and the `herdr` CLI to run the newest binary in an
  adjacent pane with a separate temporary `HERDR_PLUGIN_STATE_DIR`.
- [ ] Once the TUI is LIVE, dispatch one fresh read-only Codex child task from
  the Controller, then read the adjacent pane and verify parentage, active
  status, terminal status, retention, and absence of duplicate rows.
- [ ] Run the matching integrated binary's `doctor --json`; verify no
  native-session UNIQUE occurrence, persistence degradation, pending/skipped
  growth, provider egress saturation, or stale provider roots.
- [ ] Stop and restart the adjacent-pane binary against the same temporary
  state root and verify duplicate-free cold restore to LIVE.
- [ ] Run exactly one final whole-change `claude-reviewer` over the PR #21
  merge-base-to-HEAD diff, identifying the newly added persistence delta and
  integration seam while listing the already-reviewed earlier PR range.
- [ ] Push `agent/stable-task-history-rates`, wait for the non-empty applicable
  CI set to become conclusive and successful, then request Copilot review on
  the latest HEAD.
- [ ] For any Copilot finding, Codex forms a provisional evidence-backed
  judgment and obtains `claude-reviewer` judgment review before implementing,
  replying, or resolving. If a fix is pushed, repeat CI and latest-HEAD Copilot
  review.

## Publication Preflight

- Remote fetch and push URLs both resolve to GitHub owner `mageyuki`; the
  repository owner is not `aces-inc`, so Codex Controller performs the decided
  git/GitHub operations through the OpenAI-side route.
- `gh repo view` reports `ADMIN` permission.
- PR #21 already targets `main` from `agent/stable-task-history-rates` in the
  same repository; no fork or first-contributor approval gate applies.
- `.github/workflows/ci.yml` triggers on pull requests to `main` and provides a
  non-empty lint, four-platform test, and MSRV check set.
- No PR template exists. Update the existing PR body with the persistence
  correction and verification evidence; do not open a second PR.
