# PR #21 Cold Acceptance Stability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Every production change follows strict RED-GREEN-REFACTOR TDD. Implementers do not commit.

**Goal:** Make PR #21 pass the real four-pattern cold acceptance test by repairing live lifecycle convergence, draining provider pending output promptly, and rendering Codex roles without provider UUIDs.

**Architecture:** Preserve existing ownership boundaries. The Store excludes open executions from history-only unknown synthesis, while the reducer repairs already persisted stale lifecycle evidence through the monotonic native lifecycle API. The provider thread adds a short pending-only retry deadline without changing scans or event semantics. Codex lane metadata publishes the best available role into the existing transient run-kind projection, and the TUI resolves exact native aliases even after Controller/native key convergence.

**Tech Stack:** Rust 2024, Tokio MPSC, std MPSC provider control thread, rusqlite, Ratatui, Cargo test, GNU Make, Herdr CLI, and read-only SQLite acceptance probes.

**Spec:** `docs/internal/superpowers/2026-08-30-pr21-cold-acceptance-stability/spec.md`

## Global Constraints

- Starting branch: `agent/stable-task-history-rates`; planning baseline begins at `3fbd60cdca61cc75a5f0df2469658e85a31f6ada` plus the reviewed spec and plan commit.
- No schema, migration, dependency, retention, visibility, public CLI, or persisted run-kind change.
- Semantic terminal Task Run states remain authoritative and are never reopened by a snapshot.
- Native lifecycle repair must use `apply_native_lifecycle` and its existing total-order watermark checks; never overwrite V6 lifecycle fields directly.
- History finalization must still mark eligible runs history-ready and preserve the current exact finalization page and watermark semantics.
- Pending retry is 20 ms, flush-only while output remains pending, stop-aware, non-spinning, and independent of the two-second periodic full-rescan clock.
- No provider event is dropped or newly coalesced. Provider workers do not advance while a prior pending event remains blocked.
- Codex run-kind priority is normalized run kind, raw `thread_spawn.agent_role`, meaningful `subagent.other`, originator, then provider fallback. Agent nickname is not a role fallback.
- Primary provider-backed Task and Agent rows never expose provider session IDs or path-derived run UUIDs. Detail remains the complete identity surface.
- Every implementation task changes only its declared file set, performs no nested AI delegation, and returns an uncommitted result. Claude CLI and its wrapper must not stage, commit, push, merge, or rebase.
- Codex independently verifies each diff and exact test command, performs the task review, commits only after verification, and integrates one task at a time.
- Commit messages in this user repository include `Co-Authored-By: OpenAI Codex <codex@openai.com>`.
- Temporary pane, state, doctor, and SQLite evidence stays in owner-only directories outside the repository and is never committed.
- Every focused `cargo test ... -- --exact` RED and GREEN invocation must report
  `1 test`; an exit-zero command reporting `0 tests` is a failed verification.
- Push is prohibited until every automated test item and the adjacent-pane
  four-pattern cold acceptance in Task 5 are complete. The terminal goal is an
  unmerged PR whose latest HEAD has passing CI, passing Copilot review, no
  actionable unresolved finding, and is Ready to merge.

## File and Responsibility Map

### Task 1: Lifecycle convergence

- `src/store/mod.rs`: exclude open executions from history-derived unknown synthesis and add real SQLite regression coverage.
- `src/reducer.rs`: clear stale native lifecycle evidence from an exact-bound live snapshot and add reducer/persistence regressions.

### Task 2: Pending egress retry

- `src/provider/mod.rs`: schedule 20 ms pending-only retries and add the provider-thread timing/cursor regression.

### Task 3: Codex role and UUID-free labels

- `src/provider/lane.rs`: select role/other/originator for the existing run-kind metadata and add source-priority tests.
- `src/tui/projection.rs`: classify provider-backed Controller keys and exact aliases for UUID-free kind fallback.
- `src/tui/view.rs`: resolve provider-backed runs through exact aliases, suppress identity fallback, and render Agent roles.
- `docs/tui.md`: document run-kind priority and UUID-free primary labels.
- `docs/design/herdr-top-mvp.md`: update the normative execution-tree grammar and identity surface.

The three file sets are disjoint and no implementation task consumes another
task's output. After the plan review and planning-baseline commit, create three
dedicated project-local linked worktrees from that exact commit and dispatch the
tasks concurrently. Codex integrates verified task branches serially in task
number order.

Before dispatch, run `cargo build --locked` in each new worktree. A baseline
failure stops that task before implementation.

---

### Task 1: Prevent and Repair Stale Native Unknown

**Files:**

- Modify: `src/store/mod.rs`
- Modify: `src/reducer.rs`
- Create no file.

**Interfaces and decisions:**

- The Store finalization predicate gains `NOT EXISTS` over an execution for the
  same run whose `ended_at_ms IS NULL`. Apply the same condition to every CASE
  arm that writes native end status/time or lifecycle watermark fields.
- Snapshot repair is limited to a nonterminal snapshot execution whose
  provider/native-session identity resolves to the owning run through the
  existing `run_for_native_session` path. A preexisting Native alias is not
  required.
- Call existing activation first. Only if the resulting Task Run is semantically
  nonterminal and currently has a native lifecycle end, call
  `apply_native_lifecycle(run_id, None, watermark, persist)`.
- The snapshot watermark uses `source_at_ms = now_ms`,
  `observed_at_ms = now_ms`, and
  `source_order = "snapshot-live-execution:<execution_id>"`.
- The ordinary monotonic comparison may reject older snapshot evidence; this is
  correct. No direct V6 mutation or display workaround is allowed.
- After snapshot execution replacement, a pre-gap run receives the deferred
  history unknown only when it is history-ready, semantically nonterminal, has
  no native end, has `latest_provider_at_ms`, and has no live execution. Apply
  the existing `close_run_without_live_execution` decision first, then this
  deferred rule through `apply_native_lifecycle`, using
  `latest_provider_at_ms` as source time and snapshot time as observed time.
  This makes finalization-before-snapshot and snapshot-before-finalization
  converge without duplicate semantic/native writes.

- [ ] **Step 1: Add Store RED coverage**

Add `history_finalization_preserves_open_execution_native_lifecycle` beside the
existing history-watermark tests. Use a real Store. Persist two equivalent
running, history-associated native runs: one with an execution whose
`ended_at_ms` is null, and one with an ended execution. Finalize the drain and
assert the open-execution run is history-ready with no native end, while the
ended-execution control receives the existing native `Unknown` result.

- [ ] **Step 2: Run and record Store RED**

```bash
cargo test --locked --lib store::tests::history_finalization_preserves_open_execution_native_lifecycle -- --exact --nocapture
```

Expected RED: the open-execution run incorrectly receives native `Unknown`.

- [ ] **Step 3: Implement the Store guard**

Add one logically identical open-execution exclusion to every repeated
finalization CASE predicate. Keep all existing total-order and history-ready
updates unchanged.

- [ ] **Step 4: Run Store GREEN**

```bash
cargo test --locked --lib store::tests::history_finalization_preserves_open_execution_native_lifecycle -- --exact --nocapture
cargo test --locked --lib store::tests::history_unknown_watermark_total_order_survives_restore -- --exact --nocapture
```

- [ ] **Step 5: Add reducer RED coverage**

Add `live_snapshot_clears_restored_native_unknown` using the real snapshot
reconciliation path. Seed a running Controller-keyed Task Run whose native
identity is available only through unanimous Agent Node evidence, plus a
persisted native `Unknown` watermark older than the snapshot. Reconcile a live
execution and assert:

- the Task Run remains running;
- native end becomes `None`;
- the newer snapshot watermark is stored;
- the returned persistence batch contains the repaired V6 task-run state; and
- replay/restoration of that batch retains the repair.

Add a semantic-terminal control proving completed/failed/cancelled state is not
reopened or lifecycle-cleared by the snapshot repair. Add total-order controls
proving an older snapshot cannot erase a newer terminal watermark and a run
with no native end creates no lifecycle write.

Add `history_finalization_before_snapshot_defers_only_stale_execution_close`.
Apply a finalization page that marks two runs history-ready while both still
have restored open executions and no native end. Reconcile a snapshot that
contains only one run. Require the current run to remain live with no native
end and the absent run to receive exactly one native `Unknown` based on its
latest provider timestamp. Replay the snapshot and require no newer duplicate
lifecycle write.

- [ ] **Step 6: Run and record reducer RED**

```bash
cargo test --locked --lib reducer::tests::live_snapshot_clears_restored_native_unknown -- --exact --nocapture
cargo test --locked --lib reducer::tests::history_finalization_before_snapshot_defers_only_stale_execution_close -- --exact --nocapture
```

Expected RED: `activate_for_live_execution` returns without clearing the stale
native end when the Task Run is already running.

- [ ] **Step 7: Implement snapshot self-repair**

After inserting the nonterminal snapshot execution and applying ordinary Task
Run activation, use the already resolved execution owner. When the
post-activation Task Run is semantically nonterminal and has a native end,
apply the snapshot liveness watermark through `apply_native_lifecycle`. After
all current executions are installed, process each pre-gap run by calling
`close_run_without_live_execution` first and the deferred history-close rule
second. The deferred condition exactly requires history-ready, semantically
nonterminal, no native end, `latest_provider_at_ms`, and no live execution.
Avoid a new persistence operation when the condition fails or the total order
rejects the observation. Assert the expected persistence-operation count in the
order regression.

- [ ] **Step 8: Run Task 1 verification**

```bash
cargo test --locked --lib store::tests::history_finalization_preserves_open_execution_native_lifecycle -- --exact --nocapture
cargo test --locked --lib store::tests::history_unknown_watermark_total_order_survives_restore -- --exact --nocapture
cargo test --locked --lib reducer::tests::live_snapshot_clears_restored_native_unknown -- --exact --nocapture
cargo test --locked --lib reducer::tests::history_finalization_before_snapshot_defers_only_stale_execution_close -- --exact --nocapture
cargo test --locked --lib reducer::tests -- --nocapture
cargo fmt --check
git diff --check
```

Return changed files, RED and GREEN evidence, test output, and any deviation.
Do not commit.

---

### Task 2: Retry Pending Provider Output Without Waiting for Rescan

**Files:**

- Modify: `src/provider/mod.rs`
- Create no file.

**Interfaces and decisions:**

- Add private `PENDING_RETRY_INTERVAL: Duration = Duration::from_millis(20)`.
- At the top-level provider wait, choose the earlier of the periodic deadline
  and the pending retry deadline when `pending.next_event().is_some()`.
- A pending timeout invokes a dedicated flush-only helper that calls
  `pending.flush_to_sender` and records diagnostics but never calls
  `run_provider_cycle` or `worker.process`; it does not clear `force_rescan` or
  update `last_full_rescan`.
- A periodic timeout retains the current full-rescan behavior.
- Control and stop handling retain priority. Do not add a blocking send or a
  zero-duration retry loop.

- [ ] **Step 1: Add provider-thread RED coverage**

Add `pending_egress_retries_before_periodic_rescan_without_advancing_worker`.
Use `run_bounded`, `CountingWorker`, a capacity-one egress prefilled before
spawn, and a one-second rescan interval. Wait until the initial cycle has run
and egress saturation is recorded, drain the prefilled event, then require the
pending worker event within 250 ms. Assert the worker call count stays exactly
one through pending delivery.

- [ ] **Step 2: Run and record RED**

```bash
cargo test --locked --lib provider::tests::pending_egress_retries_before_periodic_rescan_without_advancing_worker -- --exact --nocapture
```

Expected RED: no pending event arrives inside 250 ms because the thread waits
for the one-second periodic timeout.

- [ ] **Step 3: Implement pending-only scheduling**

Refactor only the timeout selection and timeout branch needed to distinguish a
pending retry from a periodic rescan. Add a small flush-only helper; do not call
`run_provider_cycle` from the pending timeout. Do not change `PendingEvents`,
worker parsing, event ordering, or coalescing.

- [ ] **Step 4: Run Task 2 verification**

```bash
cargo test --locked --lib provider::tests::pending_egress_retries_before_periodic_rescan_without_advancing_worker -- --exact --nocapture
cargo test --locked --lib provider::tests::saturated_egress_pauses_worker_without_blocking_provider_thread -- --exact --nocapture
cargo test --locked --lib provider::tests::sustained_hint_overflow_respects_full_rescan_cooldown -- --exact --nocapture
cargo test --locked --lib provider::tests -- --nocapture
cargo fmt --check
git diff --check
```

Return changed files, RED and GREEN timing evidence, worker-call evidence, test
output, and any deviation. Do not commit.

---

### Task 3: Publish Codex Roles and Remove Provider IDs from Primary Rows

**Files:**

- Modify: `src/provider/lane.rs`
- Modify: `src/tui/projection.rs`
- Modify: `src/tui/view.rs`
- Modify: `docs/tui.md`
- Modify: `docs/design/herdr-top-mvp.md`
- Create no file.

**Interfaces and decisions:**

- Add a small lane helper that selects a nonempty Codex kind from
  `CodexInternal::ThreadSpawn.role`, then `CodexInternal::Named.name`, then the
  nonempty originator. Existing reducer `run_kind` first-nonempty behavior is
  the normalized authority.
- Do not use `ThreadSpawn.nickname` as a role or subject.
- Add a projection helper that identifies provider-backed Task Runs through the
  primary key, recognized hook selector, and exact entries in
  `DomainModel::task_run_bindings()`.
- Generalize child detection so a Controller-primary Codex run with a native
  alias and an incoming execution edge is treated as a Codex child.
- Provider-backed rows with no captured subject render kind alone instead of a
  native SID, hook session ID, or path-derived run UUID. Non-child roots retain
  a captured subject.
- `[dispatched by: ...]` uses the parent's UUID-free projected kind or captured
  human subject, not `short_run_name` identity fallback.
- Pass `&DomainModel` into Agent row rendering. Resolve an Agent's exact native
  alias first, then its owning Task Run, and use the selected run's nonempty
  projected kind after sanitization. Exact alias precedence preserves Codex
  child roles; ownership fallback preserves Controller-keyed Claude roles.
  Otherwise render no identity suffix. Keep model and activity annotations.
- Detail identity fields and selection keys remain unchanged.

- [ ] **Step 1: Add lane RED coverage**

Add the exact focused test
`codex_run_kind_prefers_role_and_meaningful_internal_name` for these literal
fixtures:

- `ThreadSpawn { role: Some("worker"), nickname: Some("volatile") }` publishes
  `worker`;
- `ThreadSpawn { role: None, ... }` falls back to `codex-tui` originator;
- `Named { name: "reviewer" }` publishes `reviewer`; and
- empty role/name values do not create an empty kind.

- [ ] **Step 2: Run and record lane RED**

```bash
cargo test --locked --lib provider::lane::tests::codex_run_kind_prefers_role_and_meaningful_internal_name -- --exact --nocapture
```

Expected RED: the projected kind remains the generic originator when a role is
present.

- [ ] **Step 3: Add TUI RED coverage**

Add focused view tests covering:

1. a Controller-primary Codex child plus exact native alias, `run_kind=worker`,
   and identity-shaped subject/fallback renders exactly `● working worker`;
2. a provider-backed Codex root without a subject renders exactly
   `● working codex-tui`, while a captured human subject is retained;
3. an exact-bound Codex Agent Node renders `Codex native agent: worker` and
   contains neither native SID nor Agent Node ID;
4. an unmatched Agent Node renders `Codex native agent` with no colon or ID;
5. a Controller-keyed Claude Agent Node with no Native alias renders
   `Claude native agent: reviewer` from its owning run kind;
6. a pane-placed child annotation renders `[dispatched by: <UUID-free kind>]`
   before and after Controller/native alias convergence; and
7. existing detail output still contains the full Task/Agent identities.

Use structural assertions and literal expected labels. Do not add a source-text
grep test.

- [ ] **Step 4: Run and record TUI RED**

```bash
cargo test --locked --lib tui::view::tests::controller_primary_codex_rows_render_roles_without_provider_ids -- --exact --nocapture
```

Expected RED: Task and Agent rows contain the native SID or Agent Node ID and
the Controller-primary child is not recognized as a Codex child.

- [ ] **Step 5: Implement lane and view projection**

Make the minimal role-selection and exact-binding changes described above.
Build lookup data once per tree construction or reuse an existing per-frame
projection; do not introduce a model-wide binding scan for every row when a
single indexed map can serve all Agent rows.

- [ ] **Step 6: Update normative documentation**

Update the Task and Agent row grammar in both documents. State the source
priority, exact-alias behavior after identity convergence, UUID-free primary
surface, and Detail-only identity rule. Do not claim `agent_role` is a stable
public Codex API.

- [ ] **Step 7: Run Task 3 verification**

```bash
cargo test --locked --lib provider::lane::tests::codex_run_kind_prefers_role_and_meaningful_internal_name -- --exact --nocapture
cargo test --locked --lib tui::view::tests::controller_primary_codex_rows_render_roles_without_provider_ids -- --exact --nocapture
cargo test --locked --lib provider::lane::tests -- --nocapture
cargo test --locked --lib tui::view::tests -- --nocapture
cargo fmt --check
git diff --check
```

Return changed files, RED and GREEN evidence, exact rendered labels, test output,
and any deviation. Do not commit.

---

### Task 4: Serial Integration and Automated Gates

**Files:**

- Modify only the integration branch by applying the three verified task
  commits; author no new production change in this task.

- [ ] Confirm each actual changed file set is a subset of its declared set.
- [ ] Independently inspect each diff against the spec and rerun its exact test
  commands before committing the task branch.
- [ ] Integrate Task 1, run its focused tests, and commit.
- [ ] Integrate Task 2, run its focused tests plus Task 1 focused tests, and
  commit.
- [ ] Integrate Task 3, run all focused tests, and commit.
- [ ] Run the complete gates:

```bash
cargo fmt --check
git diff --check
make test
make lint
make build
```

- [ ] Run the existing restore/convergence suites that cover PR #21 cold state:

```bash
cargo test --locked --test restore -- --nocapture
cargo test --locked --test convergence -- --nocapture
```

- [ ] Dispatch exactly one `claude-reviewer` final whole-change review over the
  complete merge-base-to-HEAD diff, with special attention to previously
  unreviewed seams between lifecycle, provider scheduling, and labels.
- [ ] Resolve every actionable finding through the normal implementation and
  scoped re-review route, then rerun affected tests and complete gates.

---

### Task 5: Install and Real-Pane Four-Pattern Cold Acceptance

**Files:**

- Do not modify repository files. Store evidence outside the repository.

- [ ] Record the verified branch SHA and build the exact binary:

```bash
make build
```

- [ ] Install that binary at `~/.local/bin/herdr-top` and verify its SHA-256 is
  byte-identical to `target/release/herdr-top`.
- [ ] Create owner-only temporary plugin/state roots outside the repository and
  start the exact installed binary in the adjacent Herdr Top pane.
- [ ] Execute these cold patterns using bounded, non-mutating child tasks:

```text
Codex controller -> Codex child
Codex controller -> Claude child
Claude controller -> Codex child
Claude controller -> Claude child
```

- [ ] For each pattern record timestamps for first root visibility, first child
  visibility, terminal stabilization, and history readiness. Require prompt
  stabilization rather than the previous multi-minute backlog tail.
- [ ] Verify Task/Agent labels show role or provider fallback without rollout,
  native-session, Agent Node, or run UUID text.
- [ ] Verify the Claude controller root does not remain `unknown` while its
  execution is live.
- [ ] Stop only the test monitor, restart the same installed binary against the
  same private state, and repeat status/label/persistence checks.
- [ ] Capture `herdr-top doctor --json`, bounded pane output, and read-only
  SQLite evidence. Require healthy persistence, completed history drains,
  unique native bindings, no retained pending mutation, and no unlinked
  duplicate Task Run for any tested native session.

If any acceptance condition fails, return to a new diagnosis task with the
failing evidence. Do not repair the private database manually and do not test an
experimental binary against the live production state root.

---

### Task 6: Publish PR #21 Update

**Files:**

- Update the existing PR description only after the final verified commit; no
  repository file change is required unless review finds a documentation gap.

**Entry gate:** Every checkbox in Tasks 1 through 5 is complete, including the
adjacent-pane test. Do not push a partial implementation or use post-push pane
testing as a substitute.

- [ ] Verify every origin fetch and push URL resolves to the same non-`aces-inc`
  owner before GitHub operations.
- [ ] Push the integration branch and update PR #21 with the three fixes, exact
  automated gates, installed-binary identity, and real-pane cold evidence.
- [ ] Monitor the latest HEAD until the applicable nonempty CI check set is
  conclusive and successful.
- [ ] Ensure the PR is Ready for review, request GitHub Copilot review on the
  latest HEAD, and wait for its result.
- [ ] For every Copilot finding, form a code/test/docs-backed provisional
  judgment and dispatch `claude-reviewer` for finding judgment before replying,
  resolving, or implementing.
- [ ] If a fix is pushed, rerun CI and request Copilot review again on the new
  HEAD.
- [ ] Completion is latest-HEAD CI success with no actionable unresolved
  Copilot finding. Do not merge the PR.
