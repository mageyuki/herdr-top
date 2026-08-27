# Stable Task History and Active-Time Rates Implementation Plan

> **Execution contract:** Implement task-by-task in dedicated linked
> worktrees. Each implementation process is fresh Codex; each critical task
> and whole-change review is fresh Grok. Workers and reviewers may not delegate
> to another AI and may not commit, push, merge, or rebase.

**Goal:** Keep Task history stable and append-only in the TUI, converge Herdr
pane status on the installed event spelling, and calculate tok/s from only
reliably observed working time.

**Architecture:** Schema v6 persists native lifecycle ordering, historical
drain state, and closed active-time totals. Historical association spills to
SQLite; one ordered, acknowledged drain barrier prevents visibility before a
known commit. A shared status projection feeds TUI display and the rate ledger.
Rate cursors carry a live measurement epoch so history and reconnect gaps only
rebaseline.

**Tech stack:** Rust 1.97.1, Tokio, SQLite/rusqlite, ratatui, Herdr 0.8.2 wire
protocol, provider JSONL adapters.

**Spec:** `docs/internal/superpowers/2026-08-27-stable-task-history-rates/spec.md`

## Global constraints

- Base is `e357aa634fff32d95cb68c979aea2a4b04d05e75` from `origin/main`, containing
  merged PR #20.
- Do not use Claude. Implementation uses fresh Codex `gpt-5.6-sol` with
  `xhigh` reasoning. Critical review uses fresh Grok `grok-4.6`, high reasoning,
  read-only sandbox, web disabled, and subagents disabled. Grok is the
  authoritative reviewer; Codex verifies commands and adjudicates evidence.
- Each task has a dedicated project-local linked worktree and branch. The
  Controller checks that actual changed paths are a subset of the declared
  file set before integration.
- Tasks integrate serially because their files and output interfaces overlap.
- No new dependency and no terminal-output scraping.
- Preserve semantic Task state, native-session lifecycle, runtime execution,
  status source, and graph relationships as separate axes.
- Siblings at every hierarchy depth retain immutable ascending
  `display_ordinal`; a status change never reorders a row.
- Every hierarchy depth uses `DEFAULT_TERMINAL_VISIBILITY_MS`; Summary retains
  hidden history while ordinary DB retention retains the Task Run.
- The rate denominator includes only reliably observed run-level `working`
  time. History, gaps, idle, blocked, queued, unknown, and terminal time are
  excluded.
- Existing databases migrate through the online-backup path. Existing Task
  Runs start history-ready and with no synthesized rate row.
- Use TDD. Every test added in a task must fail first for the named missing or
  incorrect production behavior, then pass after the minimal implementation.
- Every Rust command uses this SIGHUP-safe prefix:

  ```sh
  setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 --
  ```

- Never waive `runner_fixture_reaps_timeout_and_signal_groups` or
  `orchestration_signal_traps_are_self_contained_across_reexec`.

## Publication preflight

- Fetch and push URLs both resolve to `https://github.com/mageyuki/herdr-top.git`.
- The authenticated account has `ADMIN`; a dry-run branch push succeeds.
- The repository is not a fork, `main` is default, and no PR template exists.
- `.github/workflows/ci.yml` runs for PRs to `main`. Success requires a
  non-empty applicable check set, every check conclusive, only `success` or
  inapplicable `skipped`, and zero failure/cancel/timeout/action-required.

---

### Task 1: Add schema-v6 state, drain storage, and production merge rules

**Files:**

- Modify: `src/model/state.rs`
- Modify: `src/model/entities.rs`
- Modify: `src/model/mod.rs`
- Modify: `src/identity.rs`
- Modify: `src/diagnostics/mod.rs`
- Modify: `src/store/schema.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/store/writer.rs`

**Produced interfaces:**

```rust
pub enum NativeSessionEndStatus { Done, Error, Cancelled, Unknown }
pub struct NativeSessionEnd { pub status: NativeSessionEndStatus, pub at_ms: i64 }
pub struct NativeLifecycleWatermark {
    pub source_at_ms: i64,
    pub observed_at_ms: i64,
    pub source_order: String,
}
pub struct RunRateTotals { pub output_tokens: u64, pub working_ms: i64 }
pub struct HistoryDrainId(/* stable bounded identity */);
```

`TaskRun` gains the lifecycle end/watermark, `history_ready`, and
`latest_provider_at_ms`. `DomainModel` owns rate totals. Schema v6 adds the
TaskRun columns, `run_rate_totals`, `history_drains`,
`history_drain_artifacts`, and `history_drain_runs`. Store/writer commands can
stage historical association, atomically finalize a drain, and query whether
an uncertain finalization committed.

The production restore implementation is `Store::load_restored_state` and its
helpers in `src/store/mod.rs`; there is no `src/store/restore.rs`. Task 1 must
hydrate every new v6 field through that path. `tests/restore.rs` is only the
cross-process integration-test target used by later tasks.

The identity merge matrix is part of this task, through the real
`apply_binding_plan_at` path:

- rate totals add once and the absorbed row is removed;
- latest provider timestamp takes the maximum;
- newest lifecycle watermark wins deterministically, with its matching end;
- live evidence or completed drain coverage preserves readiness;
- outstanding drain associations rekey to the survivor;
- cursors are never merged.

- [ ] **Step 1: Add RED schema/domain/identity tests**

  Add exact tests for v5-to-v6 migration, lifecycle/watermark and totals
  round-trip, drain association uniqueness, uncertain-finalization readback,
  cascade retention, and the complete merge matrix through
  `apply_binding_plan_at` -> `MergeTaskRuns` -> SQLite -> restore.

- [ ] **Step 2: Verify RED**

  Run the new exact `store::tests`, `identity::tests`, and model tests. They
  must fail because v6 objects/interfaces do not exist, not because fixtures
  are malformed.

- [ ] **Step 3: Implement checked domain and merge behavior**

  Add the minimal types and actual identity fold. Clamp token and millisecond
  totals before enqueue to `i64::MAX`, record one bounded diagnostic on
  saturation, and reject negative stored values. Never pass an out-of-domain
  `u64` to rusqlite.

- [ ] **Step 4: Implement schema and store commands**

  Add exact v6 schema validation and migration. Existing rows become ready,
  carry no lifecycle evidence, and gain no rate row. Drain operations are
  idempotent and foreign-key safe. Finalization is one transaction and its
  commit identity is queryable after `DurabilityUnknown`.

- [ ] **Step 5: Verify GREEN**

  ```sh
  cargo test --locked store::tests
  cargo test --locked identity::tests
  cargo test --locked model::entities::tests
  cargo fmt --all -- --check
  cargo clippy --locked --all-targets --all-features -- -D warnings
  git diff --check
  ```

- [ ] **Step 6: Handoff uncommitted changes**

  Report RED/GREEN evidence and confirm the changed set is a subset of the
  eight declared files.

### Task 2: Implement the bounded, ordered, durable history-drain protocol

**Files:**

- Modify: `src/model/state.rs`
- Modify: `src/model/entities.rs`
- Modify: `src/model/mod.rs`
- Modify: `src/provider/mod.rs`
- Modify: `src/provider/facts.rs`
- Modify: `src/provider/lane.rs`
- Modify: `src/herdr/collector.rs`
- Modify: `src/reducer.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/store/writer.rs`
- Test: `tests/convergence.rs`
- Test: `tests/restore.rs`

**Produced interfaces:**

- `ObservationOrigin::{Live, Historical { drain_id, artifact_id }}` is carried
  through provider reduction.
- Parser progress advances only for an event accepted by `PendingEvents`.
- A stable scan drain is the streaming digest of the sorted frozen artifact
  manifest `(provider, artifact identity, generation, goalpost)`.
- `HistoryDrainBarrier` is queued after all ordinary pending classes, contains
  a bounded acknowledgement handle, and blocks further provider work until
  finalization is known committed.
- The reducer exposes a two-phase barrier path: stage DB finalization without
  publication, then apply the durable result to the model and publish once.

- [ ] **Step 1: Add RED pending-order/backpressure tests**

  Cover mixed lane/entity output, a full pending buffer, parser cursor
  non-advancement, and a barrier that cannot overtake any coalesced slot. Assert
  that `run_provider_cycle` skips `worker.process` until all pending output and
  the held barrier are acknowledged.

- [ ] **Step 2: Add RED spill/restore tests**

  Stream more than 4,096 distinct run associations across multiple cycles and
  assert bounded provider memory, idempotent SQLite associations, and no
  default-ready row before finalization. Add crash/restart, changed manifest,
  proven-superset supersession, and historical enrichment of an already-live
  run without status/lifecycle regression.

- [ ] **Step 3: Verify RED**

  Run the exact provider, collector, reducer, store, convergence, and restore
  tests. The current implementation must fail on ordering, capacity, or
  visibility—not test infrastructure.

- [ ] **Step 4: Implement origin and resumable provider admission**

  Freeze the manifest, compute its stable drain ID, persist associations with
  historical mutations, and make provider parsing resumable at the first
  unaccepted event. Do not retain a drain-wide key set in `Synthesis`.

- [ ] **Step 5: Implement ordered acknowledged finalization**

  Emit exactly one barrier only after every ordinary pending slot is empty.
  Hold it and pause parsing. Stage the finalization transaction without
  publishing. Publish ready/unknown results only after `Durable` or known
  `CommittedButDegraded`. Resolve `DurabilityUnknown` by completion readback;
  retain and retry on all uncommitted outcomes. On shutdown, either finish the
  acknowledged barrier or leave rows durably suppressed. A synthesized
  `Unknown` uses `(latest_provider_at_ms, barrier_observed_at_ms,
  stable_drain_run_order)` and is conditionally applied against the persisted
  current watermark in that same finalization transaction.

- [ ] **Step 6: Add writer-outcome acceptance tests**

  Exercise `Durable`, `CommittedButDegraded`, `NotCommitted`, `Skipped`, and
  `DurabilityUnknown` both with and without a committed completion row. Also
  cover parse failure and shutdown during the drain. Pin lifecycle ordering for
  a drain `Unknown` versus live starts with older, newer, and equal source
  timestamps, including a restore between the two facts.

- [ ] **Step 7: Verify GREEN**

  ```sh
  cargo test --locked provider::tests
  cargo test --locked herdr::collector::tests
  cargo test --locked herdr::collector::provider_integration_tests
  cargo test --locked reducer::tests
  cargo test --locked store::tests
  cargo test --locked --test convergence
  cargo test --locked --test restore
  cargo fmt --all -- --check
  cargo clippy --locked --all-targets --all-features -- -D warnings
  git diff --check
  ```

- [ ] **Step 8: Handoff uncommitted changes**

  Report the boundedness/order/durability evidence and confirm the actual set
  is a subset of the twelve declared paths.

### Task 3: Normalize lifecycle/status and stabilize hierarchy visibility

**Files:**

- Create: `src/status.rs`
- Modify: `src/lib.rs`
- Modify: `src/model/entities.rs`
- Modify: `src/model/state.rs`
- Modify: `src/identity.rs`
- Modify: `src/herdr/controller.rs`
- Modify: `src/herdr/collector.rs`
- Modify: `src/hook_adapter.rs`
- Modify: `src/provider/mod.rs`
- Modify: `src/provider/facts.rs`
- Modify: `src/provider/lane.rs`
- Modify: `src/provider/codex_facts.rs`
- Modify: `src/provider/claude_facts.rs`
- Modify: `src/reducer.rs`
- Modify: `src/activity.rs`
- Modify: `src/operator.rs`
- Modify: `src/tui/projection.rs`
- Modify: `src/tui/view.rs`
- Modify: `src/tui/dag.rs`
- Modify: `src/tui/app.rs`
- Test: `tests/controller.rs`
- Test: `tests/convergence.rs`
- Test: `tests/provider_codex.rs`
- Test: `tests/provider_claude.rs`
- Test: `tests/restore.rs`

**Produced interfaces:**

- One predicate accepts only `pane.agent_status_changed` and
  `pane_agent_status_changed`, and is reused by observation, classification,
  dispatch, and owner lookup.
- Controller wire kind `session_ended` targets only a known bound run; unknown
  or unbound targets return accepted diagnostic no-op, never a forward ref.
- Lifecycle order is `(source_at_ms, observed_at_ms, stable source_order)` and
  is persisted for both end and reopen.
- `src/status.rs` exposes occurrence-specific display status plus
  `RunRateActivity::{Working, Paused}`. A compatibility re-export prevents DAG
  or tests from depending on TUI-only status ownership.
- One ancestor-closed visible-run set is computed before placement and reused
  by `place_runs`, projection, counts, tree, and DAG.

- [ ] **Step 1: Add RED pane-event alias tests**

  Prove dotted and underscore observations are identical, the installed dotted
  spelling changes a full collector snapshot from idle to working, and other
  dotted names remain ignored.

- [ ] **Step 2: Add RED lifecycle-order/mapping tests**

  Cover hook `SessionEnd` -> resumable `Done`, normal start/reopen, delayed old
  end after newer start, equal source timestamps, restart between facts,
  unknown/unbound diagnostic no-op, and unchanged manual dismiss. Pin the
  provider mapping table: Codex turn complete is idle; explicit abort is
  resumable Cancelled; explicit root failure is Error; root lane close is
  Unknown; subagent completion remains semantic.
  The restart fixture must use the production `Store::load_restored_state`
  path already extended in Task 1, including drain-Unknown versus hook-start
  watermark ordering.

- [ ] **Step 3: Add RED identity/order/visibility tests**

  Assert literal rows for two same-Pane native roots and a
  root/child/grandchild tree. Same session reuses ordinal; different session
  appends below. Status changes do not reorder. All levels expire at exactly
  one hour. An expired root and child remain as structure for a visible
  grandchild. Follow selects the last row; manual movement disables follow.

- [ ] **Step 4: Verify RED**

  Run every new exact hook/controller/provider/reducer/TUI test and record the
  current incorrect behavior.

- [ ] **Step 5: Implement lifecycle and shared status**

  Add closed decoding/mapping, watermark comparison, root lifecycle mapping,
  and status precedence. Semantic terminal state remains authoritative. A
  matching newer liveness/start/execution clears only native lifecycle end.

- [ ] **Step 6: Implement one visibility closure**

  Compute individually visible runs, close over dispatch ancestors, then pass
  the same set into placement and all projections. Preserve existing ascending
  ordinal order in tree and DAG.

- [ ] **Step 7: Verify GREEN**

  ```sh
  cargo test --locked hook_adapter::tests
  cargo test --locked herdr::controller::tests
  cargo test --locked reducer::tests
  cargo test --locked herdr::collector::tests
  cargo test --locked tui::projection::tests
  cargo test --locked tui::view::tests
  cargo test --locked tui::dag::tests
  cargo test --locked tui::app::tests
  cargo test --locked --test controller
  cargo test --locked --test convergence
  cargo test --locked --test provider_codex
  cargo test --locked --test provider_claude
  cargo test --locked --test restore
  cargo fmt --all -- --check
  cargo clippy --locked --all-targets --all-features -- -D warnings
  git diff --check
  ```

- [ ] **Step 8: Handoff uncommitted changes**

  Confirm the actual set is a subset of the twenty-five declared paths and
  report RED/GREEN evidence.

### Task 4: Accumulate and render live-epoch active-time rates

**Files:**

- Modify: `src/model/entities.rs`
- Modify: `src/model/state.rs`
- Modify: `src/identity.rs`
- Modify: `src/reducer.rs`
- Modify: `src/herdr/collector.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/status.rs`
- Modify: `src/tui/projection.rs`
- Modify: `src/tui/view.rs`
- Modify: `src/tui/dag.rs`
- Test: `tests/restore.rs`
- Test: `tests/convergence.rs`

**Produced interfaces:**

```rust
pub(crate) enum RateObservationOrigin { Historical, Live { epoch: u64 } }
pub(crate) fn observe_run_rates(&mut self, observation: RateObservation) -> PersistBatch;
pub(crate) fn begin_rate_epoch(&mut self) -> u64;
```

Activity, cumulative tokens, identity, origin, epoch, and timestamp are read
from one post-transition reducer snapshot. History only rebaselines. Entering
Disconnected/Reconciling or overflow recovery clears cursors before any sweep;
the reconciled snapshot begins a fresh live epoch. Persisted positive totals
render after restore even without a cursor.

- [ ] **Step 1: Add RED deterministic ledger tests**

  Cover working->idle freeze, idle->working resume, paused states, delayed idle
  tokens, shared occurrence OR, historical multi-sample replay, same-transition
  status/token updates, counter regression, and clock reversal. Use the literal
  baseline 100@1000, 140@3000 working, delayed 150@13000 idle, 170@14000
  working: expected 70 tokens / 3,000 ms = 23.333... tok/s.

- [ ] **Step 2: Add RED epoch/persistence tests**

  Cover disconnect before the next five-second sweep, reconciliation,
  queue-overflow recovery, cold restart, graceful final checkpoint, merge
  rebaseline, saturation at `i64::MAX`, and terminal hidden-run restore with
  positive totals but no cursor.

- [ ] **Step 3: Verify RED**

  Run the exact reducer/collector/store/restore/convergence tests. Confirm the
  current implementation includes idle/history/offline time or loses restored
  measured totals.

- [ ] **Step 4: Implement cursors and epoch transitions**

  Accrue the prior interval only when its run-level activity was Working. Add
  every positive post-baseline token delta once, including delayed idle
  reports. Historical or changed epoch observations only rebaseline. Reset
  before reconciling sweeps; rebaseline after authoritative reconciliation.

- [ ] **Step 5: Implement checkpoints and rendering**

  Persist changed totals on the five-second sweep and graceful shutdown, never
  at the 50 ms paint cadence. Replace wall-time tok/s everywhere with measured
  numerator/denominator. Summary uses sums, not mean of rates. Positive stored
  totals are valid without a cursor; zero duration/missing totals are `—`.

- [ ] **Step 6: Verify GREEN**

  ```sh
  cargo test --locked reducer::tests
  cargo test --locked herdr::collector::tests
  cargo test --locked store::tests
  cargo test --locked tui::projection::tests
  cargo test --locked tui::view::tests
  cargo test --locked tui::dag::tests
  cargo test --locked --test restore
  cargo test --locked --test convergence
  cargo fmt --all -- --check
  cargo clippy --locked --all-targets --all-features -- -D warnings
  git diff --check
  ```

- [ ] **Step 7: Handoff uncommitted changes**

  Confirm the actual set is a subset of the twelve declared paths and report
  exact RED/GREEN evidence.

### Task 5: Document and verify the complete operator contract

**Files:**

- Modify: `README.md`
- Modify: `docs/tui.md`
- Modify: `docs/guides/controller-emit-setup.md`
- Modify: `docs/design/herdr-top-mvp.md`
- Modify: `docs/adr/2026-08-22-session-end-auto-dismiss.md`
- Modify: `src/tui/view.rs` (Help text/tests only)
- Test: `tests/provider_codex.rs`
- Test: `tests/provider_claude.rs`
- Test: `tests/convergence.rs`
- Test: `tests/restore.rs`

- [ ] **Step 1: Add RED Help/cross-provider acceptance tests**

  Pin active-time tok/s, oldest-first append ordering, same-session resume,
  all-depth one-hour visibility, durable historical suppression, and hidden
  Summary history for both Codex and Claude roots.

- [ ] **Step 2: Update self-contained documentation**

  Supersede the old ADR without deleting its historical rationale. Explain
  lifecycle evidence/watermarks, provider mappings, drain finalization,
  active-time epochs, restored totals, Summary, ordering, and visibility.

- [ ] **Step 3: Verify GREEN**

  ```sh
  cargo test --locked tui::view::tests
  cargo test --locked --test provider_codex
  cargo test --locked --test provider_claude
  cargo test --locked --test convergence
  cargo test --locked --test restore
  cargo test --locked --doc
  cargo fmt --all -- --check
  cargo clippy --locked --all-targets --all-features -- -D warnings
  git diff --check
  ```

- [ ] **Step 4: Handoff uncommitted changes**

  Confirm the actual set is a subset of the ten declared paths and report the
  exact outcomes.

## Serial implementation, review, and integration

For each task:

1. Create its worktree from the integration branch's current HEAD.
2. Generate a self-contained brief with exact files, acceptance criteria,
   commands, TDD requirements, and the prior task's produced interfaces.
3. Run one fresh Codex `gpt-5.6-sol` / `xhigh` implementation process with
   workspace-write approval. It must use no subagents and leave changes
   uncommitted.
4. Verify the actual changed paths are a subset of the declared set. Inspect
   the diff and independently run focused tests, rustfmt, Clippy, and
   `git diff --check`.
5. Commit on the task branch with
   `Co-Authored-By: OpenAI Codex <noreply@openai.com>`.
6. Run one fresh Grok `grok-4.6` critical review of the exact committed range
   with read-only sandbox, web disabled, and subagents disabled. Require cited
   source/test evidence, exact reproduction commands, acceptance matrix,
   changed-file audit, and a clear APPROVE/REVISE verdict.
7. Resolve every blocking/important finding through a fresh Codex TDD fix
   process. Re-review only the delta with fresh Grok unless the original Grok
   session can be safely resumed without broadening the approved range.
8. Cherry-pick reviewed commits into the integration branch and rerun focused
   tests there before starting the next task.

No two integrations overlap.

## Final verification and publication

After Task 5 integration:

1. Run fresh full verification on integration HEAD:

   ```sh
   cargo test --locked --all-targets --all-features
   cargo test --locked --doc
   cargo clippy --locked --all-targets --all-features -- -D warnings
   cargo fmt --all -- --check
   cargo build --locked --release
   git diff --check origin/main...HEAD
   ```

2. Confirm both mandatory signal regressions passed in the full suite.
3. Run exactly one fresh Grok `grok-4.6` critical whole-change review of
   `origin/main..HEAD`, emphasizing unreviewed seams, migration, drain
   ordering/durability, lifecycle ordering, measurement epochs, merge behavior,
   and Task 1-5 integration.
4. Fix blocking findings with fresh Codex TDD work, Grok-review only the delta,
   and rerun the affected and complete final gates.
5. Push `agent/stable-task-history-rates`; create a self-contained Draft PR to
   `main`; monitor CI until the exact non-empty success predicate holds.
6. Mark Ready, request GitHub Copilot review on latest HEAD, and wait.
7. Verify each Copilot finding against code/tests/docs/spec. Use fresh Grok
   critical judgment for actionable or disputed findings. Do not implement,
   reply, or resolve until the evidence-based judgment is settled.
8. Implement accepted findings by the same Codex TDD route, push, await CI,
   request Copilot again on latest HEAD, reply, and resolve addressed threads.
9. Completion is a non-Draft PR at latest HEAD with successful applicable CI
   and no actionable unresolved Copilot thread. Do not merge.
10. Build the final release binary, copy it to
    `/home/mageyuki/.local/bin/herdr-top`, and verify byte identity.
