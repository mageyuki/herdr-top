# Live truth corrections implementation plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the Herdr snapshot the sole authority for physical topology and names, keep a continuing Codex session visible after an interrupted turn, and enable the optional Controller `emit` precision layer only after the zero-configuration path passes live acceptance.

**Architecture:** Primary topology events are admitted and completed as coalesced invalidation hints; they are never reducer mutations. A new reducer snapshot-reconciliation API replaces complete topology and current snapshot executions in one rollback-safe model transaction and publishes exactly once. Complete snapshots carry explicit authoritative name semantics, while partial event observations preserve omitted names. The existing log-lane reopening exception expands narrowly from `Completed` to `Completed | Cancelled`. Operational hook registration is additive and follows successful zero-hook live acceptance.

**Tech Stack:** Rust 1.97.1, Tokio, SQLite/rusqlite, Herdr JSON-RPC, Ratatui, shell/JQ, Make, GitHub Actions.

**Specification:** `docs/internal/superpowers/2026-08-27-live-truth-corrections/spec.md`

## Execution rules

- Execute Tasks 1-4 serially because their expected file sets overlap.
- Each implementation task runs in a fresh linked worktree and fresh `codex exec` session using `gpt-5.6-sol` with `model_reasoning_effort="xhigh"`.
- The implementation process writes tests and code but does not commit, push, merge, rebase, or delegate to another AI.
- The Controller verifies the actual file set, red/green evidence, diff, and exact tests. A separate fresh read-only Codex session reviews each task at `model_reasoning_effort="max"` before the Controller commits and integrates serially.
- Claude/Opus review is unavailable because its weekly limit is exhausted. The user explicitly approved fresh Codex review as the substitute for this increment. Reports and the PR state that the review is independent-session but same-model, not cross-model.
- Full test and lint gates run with default SIGHUP handling through `setsid --wait`. The two workload-harness process/signal tests are mandatory and are never waived.
- No task adds a dependency or database migration. Discovering either need pauses implementation and returns to the approved design.
- A review finding never authorizes an undeclared edit. The Controller first creates an explicitly scoped delta task with files, acceptance criteria, red/green tests, and review command.

## Task 0: freeze the reviewed plan

**Files:**

- Modify: `docs/internal/superpowers/2026-08-27-live-truth-corrections/spec.md`
- Create: `docs/internal/superpowers/2026-08-27-live-truth-corrections/plan.md`

### Step 1: record publication preflight

Planning-time verification on 2026-08-27 established:

- fetch URL: `https://github.com/mageyuki/herdr-top.git`;
- push URL: `https://github.com/mageyuki/herdr-top.git`;
- all URLs resolve to the single owner `mageyuki`;
- authenticated viewer `mageyuki` has `ADMIN` permission and HTTPS push credentials;
- the repository is public and `main` is the default branch;
- `.github/workflows/ci.yml` triggers on pull requests to `main`, with `lint`, four-platform `test`, and `msrv (1.97.1)` jobs;
- GitHub Actions is enabled for all actions;
- `main` has no branch-protection rule, and a same-owner branch does not enter fork/first-contributor approval gates;
- no pull-request template exists; and
- publication uses one direct branch, `agent/live-truth-corrections`, and one cumulative PR to `main`.

Revalidate all remote facts immediately before publication.

### Step 2: mechanically self-review the plan

Run:

```sh
PLANNING_PLACEHOLDERS='TO''DO|TB''D|Similar ''to|as ''needed|appropriate ''test|appropriate ''file|<new-test-''name>|<full-test-''path>|relevant new ''tests|un''less the current helper|if ''present|otherwise ''use|none expected un''less|required by its Make''file'
rg -n "$PLANNING_PLACEHOLDERS" \
  docs/internal/superpowers/2026-08-27-live-truth-corrections/{spec,plan}.md
```

Expected: no matches.

### Step 3: run an independent plan review

Run one fresh read-only Codex review at maximum reasoning effort against the exact spec and plan bytes plus the current source tree. Require independent derivation from installed code, tests, docs, CLI help, CI, and remote preflight; review the topology state machine, atomic reducer boundary, partial/authoritative names, SQL clear/reload behavior, cancelled-turn reopening, deterministic seams, exact file sets, build/install identity, and live-config rollback. The report contains target identity, content digests, blockers, important/minor findings, verbatim excerpts, re-derivation commands, and coverage gaps.

Expected: no unresolved blocker or important finding before Task 1.

### Step 4: commit the approved planning artifacts

Commit message:

```text
docs: finalize live truth correction plan

Co-Authored-By: OpenAI Codex <noreply@openai.com>
```

## Task 1: represent and persist authoritative name absence

**Files:**

- Modify: `src/model/entities.rs`
- Modify: `src/reducer.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/operator.rs`
- Modify: `src/herdr/collector.rs`
- Modify: `src/store/writer.rs`
- Modify: `tests/convergence.rs`

### Step 1: write exact failing tests

Add these tests:

- `model::entities::tests::topology_authority_defaults_to_partial_when_missing_from_json`
- `reducer::tests::partial_topology_upsert_preserves_names`
- `reducer::tests::authoritative_topology_upsert_sets_and_clears_names`
- `reducer::tests::authoritative_snapshot_clear_orders_upsert_before_clear`
- `reducer::tests::authoritative_orphans_emit_no_upsert_or_clear`
- `store::tests::authoritative_name_clear_persists_across_restore`
- `reducer::tests::reconcile_gap_clears_names_durably`

Run:

```sh
mise exec rust@1.97.1 -- cargo test model::entities::tests::topology_authority_defaults_to_partial_when_missing_from_json -- --exact
mise exec rust@1.97.1 -- cargo test reducer::tests::partial_topology_upsert_preserves_names -- --exact
mise exec rust@1.97.1 -- cargo test reducer::tests::authoritative_topology_upsert_sets_and_clears_names -- --exact
mise exec rust@1.97.1 -- cargo test reducer::tests::authoritative_snapshot_clear_orders_upsert_before_clear -- --exact
mise exec rust@1.97.1 -- cargo test reducer::tests::authoritative_orphans_emit_no_upsert_or_clear -- --exact
mise exec rust@1.97.1 -- cargo test store::tests::authoritative_name_clear_persists_across_restore -- --exact
mise exec rust@1.97.1 -- cargo test reducer::tests::reconcile_gap_clears_names_durably -- --exact
```

Record that every behavior test fails for the intended pre-fix reason; the backward-compatibility test may become green as part of the same first compile once the serde-defaulted field exists.

### Step 2: add explicit authority and clear operations

In `src/model/entities.rs`, add serde-defaulted `TopologyAuthority::{Partial, Authoritative}`, with `Partial` as `Default`, to `NormalizedEvent::TopologyUpsert`.

In `src/reducer.rs`:

- `Partial + None` preserves the current tab/pane name;
- `Authoritative + Some(non-empty)` sets the sanitized name;
- `Authoritative + None/empty-after-sanitization` clears the name;
- the authoritative upsert is emitted before `PersistOp::ClearTabLabel` or new `PersistOp::ClearPaneDisplayName`;
- complete `replace_topology` never retains an absent old name and appends the corresponding clear after its upsert; and
- orphan tab/pane observations emit neither an upsert nor a clear.

In `src/store/mod.rs`, add `ClearPaneDisplayName` and explicit `UPDATE ... SET display_name = NULL`; retain `COALESCE` in partial upserts. In `src/operator.rs`, classify the new operation with topology persistence operations.

Update every constructor and exhaustive match in all seven declared files. Every existing constructor is `Partial` in Task 1; Task 2 changes complete-snapshot constructors to `Authoritative`.

### Step 3: make focused tests pass

Run:

```sh
mise exec rust@1.97.1 -- cargo test model::entities::tests
mise exec rust@1.97.1 -- cargo test reducer::tests
mise exec rust@1.97.1 -- cargo test store::tests
mise exec rust@1.97.1 -- cargo test operator::tests
mise exec rust@1.97.1 -- cargo test --test convergence
```

Expected: pass, including SQLite write/reload and upsert-before-clear ordering.

### Step 4: independently verify and review Task 1

The Controller requires the actual file set to be a subset of the seven declared files, inspects the SQL and all constructor changes, and reruns the tests. A fresh read-only Codex review at maximum effort checks serde compatibility, reducer/store convergence, clear ordering, orphan behavior, restore coverage, and unchanged partial semantics.

Expected: no unresolved blocker or important finding.

Commit message:

```text
fix(topology): persist authoritative name clears

Co-Authored-By: OpenAI Codex <noreply@openai.com>
```

## Task 2: project label-only names and compare watchdog names exactly

**Files:**

- Modify: `src/herdr/collector.rs`
- Modify: `src/tui/view.rs`
- Modify: `docs/tui.md`
- Modify: `docs/design/herdr-top-mvp.md`

### Step 1: write exact failing tests

Add these tests:

- `herdr::collector::tests::pane_display_name_uses_label_without_terminal_title_fallback`
- `herdr::collector::tests::empty_pane_label_does_not_fall_back_to_terminal_title`
- `herdr::collector::tests::snapshot_null_name_clears_model_and_persistence`
- `herdr::collector::tests::watchdog_probe_compares_authoritative_null_names_exactly`
- `tui::view::tests::topology_rows_omit_absent_names`
- `tui::view::tests::topology_rows_omit_empty_names`

Run these literal commands and require each output to report exactly one selected test (`1 passed`, not a zero-test success):

```sh
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::pane_display_name_uses_label_without_terminal_title_fallback -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::empty_pane_label_does_not_fall_back_to_terminal_title -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::snapshot_null_name_clears_model_and_persistence -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::watchdog_probe_compares_authoritative_null_names_exactly -- --exact
mise exec rust@1.97.1 -- cargo test tui::view::tests::topology_rows_omit_absent_names -- --exact
mise exec rust@1.97.1 -- cargo test tui::view::tests::topology_rows_omit_empty_names -- --exact
```

Record the intended pre-fix failure for each behavior.

### Step 2: remove fallback and null rewriting

In `src/herdr/collector.rs`, derive a pane name only from sanitized `pane.label`; never use `terminal_title_stripped`. Mark all complete-snapshot topology constructors `Authoritative`; leave raw partial observations `Partial`. Compare watchdog snapshots exactly without copying a current name into a probed null.

In `src/tui/view.rs`, make `topology_row_label` treat both `None` and an empty sanitized string as absent, for both tabs and panes. In `docs/tui.md` and `docs/design/herdr-top-mvp.md`, replace the terminal-title fallback contract with label-only authoritative naming and the no-parentheses absence grammar.

### Step 3: make focused tests pass

Run:

```sh
mise exec rust@1.97.1 -- cargo test herdr::collector::tests
mise exec rust@1.97.1 -- cargo test tui::view::tests
```

Expected: pass.

### Step 4: independently verify and review Task 2

The Controller requires a subset of the four declared files and reruns the tests. A fresh read-only Codex review checks the single authoritative name source, sanitization, authoritative/partial boundaries, watchdog canonicalization, persistence clear coverage, UI grammar, and corrected docs.

Expected: no unresolved blocker or important finding.

Commit message:

```text
fix(tui): display authoritative Herdr labels

Co-Authored-By: OpenAI Codex <noreply@openai.com>
```

## Task 3: make primary topology events refresh snapshot authority

**Files:**

- Modify: `src/herdr/collector.rs`
- Modify: `src/reducer.rs`
- Modify: `src/diagnostics/mod.rs`
- Modify: `tests/convergence.rs`
- Modify: `docs/design/herdr-top-mvp.md`

### Step 1: lock the exact state machine and event table

Use the existing subscribe-before-snapshot reader and buffered event channel. Preserve `RESNAPSHOT_ATTEMPTS = 3` as the finite immediate hint-caused request budget for one refresh episode. The generation state is:

1. `SnapshotInFlight`: request and decode one complete snapshot while primary frames buffer.
2. `ReplayDrain { origin }`: atomically install that snapshot, then drain immediately available buffered frames. Every topology hint is admitted, calls `admission.complete()` exactly once before ownership is discarded, has its raw payload discarded, and sets `refresh_required = true`. Early return or channel-drop paths rely only on `Admission`'s existing `Drop` safeguard. `origin` is `CatchUp` until the first successful entry to `Live` in a subscription generation and `LiveRefresh` for an episode started after `Live`; it is retained across coalesced follow-ups. Other frames retain existing behavior.
3. `Live`: entered only when `ReplayDrain` reaches a quiet boundary with `refresh_required == false`.
4. A replay with `refresh_required == true` returns `TopologyRefreshRequired(origin)`. If fewer than three immediate hint-caused requests have been issued in this episode, the outer converge loop increments `event_triggered_topology_refreshes`, consumes one budget unit immediately before issuing exactly one snapshot request, installs it, and drains again. Hints arriving during that request buffer and coalesce into the next single follow-up.
5. If a hint accompanies all three permitted requests, the next dirty quiet boundary enters `monitor_reconciling(origin)` without a fourth request, carrying the episode's `CatchUp | LiveRefresh` origin through that state and its recovery boundary. While there, every topology hint is admitted/completed/dropped, resets the liveness deadline, cannot mutate topology or start a request, and increments `suppressed_topology_frames` exactly once only when `origin == CatchUp`. A full quiet watchdog interval followed by a healthy topology probe resets the budget to zero and returns `RestartGeneration(origin)` for one recovery snapshot, retaining the origin until the subsequent replay either reaches `Live` or reconnects. That probe/recovery request is watchdog/quiescence-caused, not counted as event-triggered. Probe failure/divergence uses bounded reconnect. Cancellation or EOF exits with its existing outcome.
6. In `Live`, the first topology hint is admitted/completed/dropped and returns `TopologyRefreshRequired(LiveRefresh)` immediately. The budget resets to zero at this episode boundary, so the triggering request consumes unit one. Additional hints received while its snapshot is in flight buffer and coalesce at the next replay drain. A quiet replay enters `Live` and resets the budget.

The centralized classifier is:

| Primary frame | Classification |
|---|---|
| `workspace_created`, `workspace_updated`, `workspace_metadata_updated`, `workspace_renamed`, `workspace_moved`, `workspace_reordered`, `workspace_closed` | subscribed topology hint |
| `tab_created`, `tab_renamed`, `tab_moved`, `tab_closed` | subscribed topology hint |
| `pane_created`, `pane_closed`, `pane_updated`, `pane_moved`, `pane_exited`, `pane_agent_detected` | topology hint |
| `workspace_focused`, `tab_focused`, `pane_focused`, `layout_updated` | existing non-topology/no-op path |
| `pane_agent_status_changed` | enrichment gauge for an already snapshot-admitted pane; never topology/provider identity |
| `pane_output_changed` | explicit non-topology/no-op classification; do not subscribe because terminal output volume cannot change modeled topology |
| `worktree_created`, `worktree_opened`, `worktree_removed` | explicit non-topology/no-op classification; do not subscribe because this model has no worktree entity |

Add the five missing workspace update/move subscriptions and `tab.moved` to `subscriptions()`. Keep `pane.output_changed` and all worktree events intentionally unsubscribed, but make the classifier total for them so direct fixtures or a permissive server still take the no-op path. A table-driven test enumerates every schema event above and proves all topology rows request a snapshot while focus/layout/output/worktree rows do not.

`suppressed_topology_frames` increments once per admitted-and-dropped topology hint when the carried origin is `CatchUp`, whether the drop occurs in `ReplayDrain` or `monitor_reconciling`; it never counts a live hint, a hint buffered during a live refresh, or a hint dropped while a `LiveRefresh` episode waits in `monitor_reconciling`. `event_triggered_topology_refreshes` increments immediately before each hint-caused request, including a failed request and a coalesced follow-up, excluding startup/reconnect/watchdog/quiescence snapshots. Both saturate at `u64::MAX`.

### Step 2: define one atomic reducer install

Add `Reducer::reconcile_snapshot(TopologySnapshot) -> Result<PersistBatch, ReducerError>`. Refactor `reconcile_gap` to delegate its topology replacement to the same implementation while retaining its gap-specific caller behavior.

The new reducer boundary:

- clones the model and allocation ordinal before any mutation;
- retires/reconstructs snapshot executions using the current `reconcile_gap_inner` identity-reuse rules;
- replaces all workspace/tab/pane membership immediately, bypassing `PendingTopologyClosures` and stale grace;
- constructs one normalized persistence batch, including authoritative clears;
- on any fallible reducer step, restores the cloned model and ordinal and publishes nothing;
- after all fallible steps succeed, normalizes lineage, updates operator submission state, and calls `publish()` exactly once; and
- returns the one persistence batch. No entity-by-entity `apply_snapshot_in_place` path remains for complete refreshes.

After successful model installation, the collector submits the returned persistence batch, refreshes owner location, replaces enrichment targets, updates sessionless-Codex observations and provider targets, then refreshes the persistence snapshot. No raw hint updates these derivatives.

Failure semantics are explicit. Replace `SubscriptionOutcome::WatchdogReconnect` with `SubscriptionOutcome::Reconnect(ReconnectReason)` and use a closed `ReconnectReason::{ProbeFailed, TopologyDiverged, SnapshotRequestFailed, SnapshotTimedOut, SnapshotDecodeFailed, TopologyConversionFailed, ReducerRejected, PersistenceUnexpected, OwnerUnexpected}`. Every `Reconnect` branch sets Herdr quality to `Disconnected`, logs its closed reason, applies `ReconnectBackoff::on_watchdog_silence()` (1 second doubling to 60 seconds and reset by a received primary event), services Controller/operator requests during the delay, and opens a fresh subscription afterward.

- Snapshot request transport/wire error maps to `SnapshotRequestFailed`; the request is wrapped in `timeout(liveness_timeout(&liveness_policy), ...)`, whose expiry maps to `SnapshotTimedOut`. Snapshot decoding and `topology_from_snapshot` map to their named reasons. None calls the reducer; the preceding model remains.
- Reducer validation/allocation error maps to `ReducerRejected`; rollback leaves the preceding model and publication count unchanged.
- After atomic model installation, every classified `RuntimeWriteOutcome::{Durable, CommittedButDegraded, NotCommitted, DurabilityUnknown, Skipped}` from persistence or owner update continues with the latest in-memory snapshot and existing durability/owner-staleness diagnostics. Only an unclassified `WriterError` maps to `PersistenceUnexpected` from batch submission or `OwnerUnexpected` from owner refresh; the latest model remains and the collector reconnects.
- `gap_committed` is false until the first atomic snapshot model install succeeds and true thereafter, including classified durability degradation and a later owner/unexpected-writer reconnect. Request/decode/conversion/reducer failure before the first install leaves it false. This preserves startup versus reconnect gap bookkeeping.
- Cancellation while waiting for a request returns `Cancelled`; EOF returns `Ended`. Both retain the last complete model, use existing termination semantics, do not increment a failure counter, and do not enter reconnect backoff in `converge`.

### Step 3: write exact failing tests through the deterministic seam

Use two separate deterministic seams:

- collector unit tests use the in-module `PrimaryCollectorHarness` plus its fake Unix server helpers. Add request-arrival and response-release `mpsc`/`oneshot` gates to the fake server so a test can observe each snapshot request, inject primary frames while it is in flight, then release exactly one configured response. Use the injected short `LivenessPolicy` and explicit request/probe acknowledgements for watchdog and budget-boundary assertions;
- the integration regression uses the test-crate-local `ScopedHerdr`. Extend `ScopedHerdrConfig` and `ScopedHerdr` with the same request-arrival/response-release gate. The new regression must not use its existing wall-clock `snapshot_delay`; existing unrelated tests may retain that compatibility field.

Map each interleaving test to one of those gates, use bounded `tokio::time::timeout` only as a deadlock guard, and do not add sleeps, paused time, Tokio `test-util`, or a dependency.

Add these tests:

- `reducer::tests::authoritative_snapshot_publishes_once_and_rolls_back_on_late_error`
- `reducer::tests::authoritative_snapshot_removes_absent_entities_immediately`
- `herdr::collector::tests::catch_up_history_cannot_overwrite_closing_snapshot`
- `herdr::collector::tests::snapshot_absent_pane_cannot_resurrect_across_generations`
- `herdr::collector::tests::catch_up_without_topology_hint_skips_closing_snapshot`
- `herdr::collector::tests::catch_up_topology_burst_coalesces_one_refresh`
- `herdr::collector::tests::primary_event_classifier_is_total_and_refreshes_only_topology`
- `herdr::collector::tests::live_topology_hint_applies_only_snapshot_payload`
- `herdr::collector::tests::topology_hints_during_refresh_coalesce_one_follow_up`
- `herdr::collector::tests::continuous_topology_hints_stop_after_three_requests_until_quiescent`
- `herdr::collector::tests::failed_topology_refresh_preserves_last_good_model`
- `herdr::collector::tests::topology_refresh_failure_reasons_preserve_model_and_route_exactly`
- `herdr::collector::tests::snapshot_refresh_updates_owner_and_enrichment_targets_after_install`
- `herdr::collector::tests::classified_persistence_and_owner_degradation_keep_installed_snapshot`
- `herdr::collector::tests::suppressed_replay_stays_live_across_two_watchdog_boundaries`
- `diagnostics::tests::primary_stream_topology_counters_have_exact_saturating_semantics`
- integration regression `snapshot_refresh_replay_never_resurrects_absent_pane` in `tests/convergence.rs`.

Run these literal commands and require each output to report exactly one selected test:

```sh
mise exec rust@1.97.1 -- cargo test reducer::tests::authoritative_snapshot_publishes_once_and_rolls_back_on_late_error -- --exact
mise exec rust@1.97.1 -- cargo test reducer::tests::authoritative_snapshot_removes_absent_entities_immediately -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::catch_up_history_cannot_overwrite_closing_snapshot -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::snapshot_absent_pane_cannot_resurrect_across_generations -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::catch_up_without_topology_hint_skips_closing_snapshot -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::catch_up_topology_burst_coalesces_one_refresh -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::primary_event_classifier_is_total_and_refreshes_only_topology -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::live_topology_hint_applies_only_snapshot_payload -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::topology_hints_during_refresh_coalesce_one_follow_up -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::continuous_topology_hints_stop_after_three_requests_until_quiescent -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::failed_topology_refresh_preserves_last_good_model -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::topology_refresh_failure_reasons_preserve_model_and_route_exactly -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::snapshot_refresh_updates_owner_and_enrichment_targets_after_install -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::classified_persistence_and_owner_degradation_keep_installed_snapshot -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::suppressed_replay_stays_live_across_two_watchdog_boundaries -- --exact
mise exec rust@1.97.1 -- cargo test diagnostics::tests::primary_stream_topology_counters_have_exact_saturating_semantics -- --exact
mise exec rust@1.97.1 -- cargo test --test convergence snapshot_refresh_replay_never_resurrects_absent_pane -- --exact
```

The continuous-hint test gates every request, injects one topology hint during each response, asserts exactly three hint-caused requests, proves no fourth request before the quiet liveness boundary, observes the healthy probe/recovery request, reaches `Live`, and proves cancellation completes every admission and ends the task. It runs both a catch-up-origin episode and a live-origin episode, injects a known number of hints in each `monitor_reconciling` state, and asserts that only the catch-up monitor hints increment `suppressed_topology_frames`. The failure-routing test table covers request/wire error, timeout, decode, topology conversion, reducer rollback, unexpected persistence error, unexpected owner error, cancellation, and EOF; it asserts the exact reason, quality, `gap_committed`, last-good model, publication count, backoff/no-backoff choice, and request counter. The classified-degradation test separately exercises `NotCommitted`, `DurabilityUnknown`, `CommittedButDegraded`, and `Skipped` batch/owner outcomes and proves they retain the installed snapshot without reconnect. The diagnostics test asserts exact values for initial catch-up, its closing refresh, catch-up monitor drops, one live hint, multiple live-origin buffered hints, live-origin monitor drops, one coalesced follow-up, budget exhaustion, and a failed request. Record intended red evidence. Revise existing tests that expected topology hints to mutate the reducer directly.

### Step 4: implement and make focused tests pass

Implement the state machine, classifier, reducer API, derived-state ordering, and diagnostic counters exactly as above. Do not change the 30-second production watchdog interval or reconnect backoff.

Replace the affected `docs/design/herdr-top-mvp.md` contracts explicitly: topology frames are invalidation-only; convergence is subscribe -> snapshot -> drain -> conditional bounded refresh -> `Live`; three immediate hint-caused requests are followed by quiescent `Reconciling`; all complete snapshots use atomic reconciliation and one publication; the two diagnostic counters have the exact origin-sensitive meanings above; and watchdog/reconnect preserves the last good snapshot. Remove claims that creation/closure frames directly upsert/remove entities, buffered topology replay is authoritative, or complete live resnapshots use the old in-place stale-grace path. After editing, run:

```sh
rg -n 'creation events upsert|closure events remove|Replay the buffered.*topology|in-place resnapshot|normal upsert behavior' docs/design/herdr-top-mvp.md
```

Expected: no obsolete-contract match.

Run:

```sh
mise exec rust@1.97.1 -- cargo test reducer::tests
mise exec rust@1.97.1 -- cargo test herdr::collector::tests
mise exec rust@1.97.1 -- cargo test diagnostics::tests
mise exec rust@1.97.1 -- cargo test --test convergence
```

Expected: pass.

### Step 5: independently verify and review Task 3

The Controller requires a subset of the five declared files, traces every admission completion and state transition, checks one-publication and rollback evidence, and reruns all tests. A fresh read-only Codex review at maximum effort checks gap freedom, coalescing bounds, immediate absence, failure semantics, owner/enrichment ordering, counter scope, liveness, and the legacy replay scenarios.

Expected: no unresolved blocker or important finding.

Commit message:

```text
fix(collector): refresh topology from snapshots

Co-Authored-By: OpenAI Codex <noreply@openai.com>
```

## Task 4: reopen a log-lane cancelled Codex run without duplication

**Files:**

- Modify: `src/reducer.rs`
- Modify: `src/herdr/collector.rs`
- Modify: `tests/convergence.rs`
- Modify: `docs/guides/controller-emit-setup.md`
- Modify: `docs/design/herdr-top-mvp.md`
- Modify: `docs/tui.md`

### Step 1: write exact failing tests

Add these tests:

- `reducer::tests::cancelled_lane_run_reopens_only_for_strictly_newer_lane_start`
- `reducer::tests::failed_and_non_lane_terminal_runs_do_not_reopen`
- `reducer::tests::cancelled_lane_reopen_preserves_identity_and_clears_terminal_state`
- `herdr::collector::tests::codex_live_abort_then_later_start_reuses_run`
- `herdr::collector::tests::restart_backfill_reopens_cancelled_run_without_duplication`
- `herdr::collector::tests::restart_backfill_rejects_start_not_newer_than_cancel`
- integration regression `hook_metadata_and_later_log_start_converge_one_run` in `tests/convergence.rs`.

The collector restart fixture persists after `task_started -> turn_aborted`, restores the store and terminal-source bookkeeping, then replays an equal/older start and a strictly later start before completion. It asserts one run ID, one execution lineage, no duplicate event, strict rejection, later acceptance, and terminal-source cleanup.

The integration regression supplies both a hook-authored `task_started` with a non-empty subject and Controller metadata/dependency edges before provider-log cancellation and the later valid log start. It asserts one run and execution, preserved subject and edges, the expected final lifecycle, and no duplicate, stale, binding-conflict, or lifecycle-conflict diagnostic increase attributable to the valid later start.

Run these literal commands and require each output to report exactly one selected test:

```sh
mise exec rust@1.97.1 -- cargo test reducer::tests::cancelled_lane_run_reopens_only_for_strictly_newer_lane_start -- --exact
mise exec rust@1.97.1 -- cargo test reducer::tests::failed_and_non_lane_terminal_runs_do_not_reopen -- --exact
mise exec rust@1.97.1 -- cargo test reducer::tests::cancelled_lane_reopen_preserves_identity_and_clears_terminal_state -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::codex_live_abort_then_later_start_reuses_run -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::restart_backfill_reopens_cancelled_run_without_duplication -- --exact
mise exec rust@1.97.1 -- cargo test herdr::collector::tests::restart_backfill_rejects_start_not_newer_than_cancel -- --exact
mise exec rust@1.97.1 -- cargo test --test convergence hook_metadata_and_later_log_start_converge_one_run -- --exact
```

Record the intended pre-fix failure.

### Step 2: expand only the log-lane exception

In `src/reducer.rs`, change both eligibility and transition so the existing exception accepts `Completed | Cancelled`. Retain exact log source, prior log terminal source, same resolved native run, strict source timestamp, and ordinary identity/ledger/binding guards. `Failed`, equal/older, Controller, and manual starts remain excluded. A successful reopen preserves run/native/execution/ordinal/subject/telemetry identity and clears `finished_at_ms`, `dismissed_at_ms`, and terminal-source bookkeeping.

Extend the actual collector adapter restart/backfill harness; do not substitute reducer-only synthetic coverage.

Update all three docs to remove the old schema-migration/resume limitation and describe the guarded `Completed | Cancelled` log-lane rule, strict source time, restart/backfill behavior, no migration, truthful temporary cancellation, and optional non-authoritative hooks.

### Step 3: make focused tests pass

Run:

```sh
mise exec rust@1.97.1 -- cargo test reducer::tests
mise exec rust@1.97.1 -- cargo test herdr::collector::tests
mise exec rust@1.97.1 -- cargo test --test convergence
```

Expected: pass.

### Step 4: independently verify and review Task 4

The Controller requires a subset of the six unique declared files and checks the durable restore boundary. A fresh read-only Codex review at maximum effort checks temporal strictness, terminal-source cleanup, failed/manual exclusion, identity preservation, actual adapter backfill, mixed-source convergence, and documentation truth.

Expected: no unresolved blocker or important finding.

Commit message:

```text
fix(provider): resume cancelled Codex turns

Co-Authored-By: OpenAI Codex <noreply@openai.com>
```

## Task 5: run whole-change gates and final review

**Files:** None. A finding creates a new explicitly scoped delta task before editing.

### Step 1: format and run repository gates

Run:

```sh
mise exec rust@1.97.1 -- cargo fmt --all -- --check
setsid --wait perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- \
  mise exec rust@1.97.1 -- make test
setsid --wait perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- \
  mise exec rust@1.97.1 -- make lint
```

Expected: all pass, including the complete workload harness with no waiver.

### Step 2: run exactly one final whole-change review

Run one fresh read-only Codex session at maximum effort against `b065ad7..HEAD`. Identify every per-task reviewed range, emphasize integration seams and uncovered regions, and require evidence excerpts, re-derivation commands, target SHA, findings, and coverage gaps.

A valid finding receives a fresh TDD delta task and fresh delta-only review; rerun affected tests and all repository gates. Do not re-review an approved range.

Expected: no unresolved blocker or important finding.

## Task 6: build, install, and prove the zero-configuration live path

**Files:**

- Runtime install target: `~/.local/bin/herdr-top`

### Step 1: build and atomically install the reviewed HEAD

Require a clean tracked and untracked implementation worktree, record `git rev-parse HEAD`, and require it to equal the SHA approved by Task 5. Build only that tree.

Do not use `install.sh` or remote mode of `scripts/fetch-release.sh`; both can select release bytes. Record the artifact hash, create a unique non-overwriting backup of an existing `~/.local/bin/herdr-top`, install with mode `0755` to a temporary sibling in `~/.local/bin`, atomically rename it over the destination, then compare SHA-256 and bytes with `cmp`. Verify `herdr-top --version` and `herdr-top --help` from the installed path.

Execute the following complete block as one non-interactive Bash transaction from the reviewed implementation worktree. The absolute paths deliberately avoid an unresolved home-directory target:

```bash
set -euo pipefail

HERDR_TOP_DEST=/home/mageyuki/.local/bin/herdr-top
HERDR_TOP_ARTIFACT="$PWD/target/release/herdr-top"
HERDR_TOP_REVIEWED_SHA=$(git rev-parse HEAD)
: "${HERDR_TOP_APPROVED_SHA:?export the literal Task 5 approved full SHA}"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
test "$HERDR_TOP_REVIEWED_SHA" = "$HERDR_TOP_APPROVED_SHA"

mise exec rust@1.97.1 -- cargo build --release --locked
test -f "$HERDR_TOP_ARTIFACT"
test ! -L "$HERDR_TOP_ARTIFACT"
HERDR_TOP_ARTIFACT_HASH=$(sha256sum "$HERDR_TOP_ARTIFACT" | awk '{print $1}')
printf '%s  %s\n' "$HERDR_TOP_ARTIFACT_HASH" "$HERDR_TOP_ARTIFACT"

if [ -e "$HERDR_TOP_DEST" ] || [ -L "$HERDR_TOP_DEST" ]; then
  test -f "$HERDR_TOP_DEST"
  test ! -L "$HERDR_TOP_DEST"
  HERDR_TOP_BACKUP_DIR=$(mktemp -d /home/mageyuki/.local/bin/.herdr-top-backup.XXXXXXXX)
  cp --update=none-fail --preserve=all --no-target-directory "$HERDR_TOP_DEST" "$HERDR_TOP_BACKUP_DIR/herdr-top"
  cmp -s "$HERDR_TOP_DEST" "$HERDR_TOP_BACKUP_DIR/herdr-top"
fi

HERDR_TOP_STAGE_DIR=$(mktemp -d /home/mageyuki/.local/bin/.herdr-top-install.XXXXXXXX)
install --mode=0755 --no-target-directory "$HERDR_TOP_ARTIFACT" "$HERDR_TOP_STAGE_DIR/herdr-top"
test "$(stat -c '%a' "$HERDR_TOP_STAGE_DIR/herdr-top")" = 755
cmp -s "$HERDR_TOP_ARTIFACT" "$HERDR_TOP_STAGE_DIR/herdr-top"
mv --no-copy --force --no-target-directory "$HERDR_TOP_STAGE_DIR/herdr-top" "$HERDR_TOP_DEST"
rmdir "$HERDR_TOP_STAGE_DIR"
test ! -L "$HERDR_TOP_DEST"
test "$(stat -c '%a' "$HERDR_TOP_DEST")" = 755
test "$(sha256sum "$HERDR_TOP_DEST" | awk '{print $1}')" = "$HERDR_TOP_ARTIFACT_HASH"
cmp -s "$HERDR_TOP_ARTIFACT" "$HERDR_TOP_DEST"
/home/mageyuki/.local/bin/herdr-top --version
/home/mageyuki/.local/bin/herdr-top --help >/dev/null
```

At execution time, export `HERDR_TOP_APPROVED_SHA` as the literal Task 5 approved full SHA before running the block. `set -euo pipefail` makes any nonzero build, identity, type, backup, byte-comparison, staging, or mode check abort the transaction; in particular, every pre-install failure aborts before `mv`. `mktemp -d` makes both destinations non-existing within unique same-filesystem directories; `cp --update=none-fail` cannot overwrite a prior backup, and `mv --no-copy` fails instead of falling back to a copy if the atomic rename cannot be performed. Retain the backup path in the execution ledger.

Expected: installed bytes equal the final-reviewed release artifact.

### Step 2: verify live behavior with hooks unchanged

Capture live hook-file digests before testing and prove they remain unchanged. Through Herdr snapshot/output inspection, verify:

- API pane label `Agent` appears as `(Agent)` and remains stable longer than two watchdog intervals;
- no roughly 33-second reconnect loop occurs;
- a continuing Codex run is visible and interrupted-then-restarted turns reuse one row;
- no snapshot-absent pane or new provisional unlinked run appears;
- pane and tab renames converge through event-triggered snapshots; and
- clearing a pane label removes parentheses, after which the temporary label is restored.

If any baseline criterion fails, do not enable hooks. Record the precise failed criterion and create a scoped TDD delta with exact files, acceptance criteria, red/green commands, and no unrelated edits. Run a fresh implementation session, Controller verification, and an independent maximum-effort delta-only review; commit only the approved delta, rerun its affected tests plus the complete Task 5 SIGHUP-default gates, record the new approved HEAD, rebuild and repeat the atomic installation from that HEAD, then restart all zero-hook acceptance checks from the first bullet. Task 7 cannot begin until one reviewed SHA closes this loop with every baseline criterion passing.

## Task 7: append and validate optional live `emit` registrations

**Files:**

- Live-authoritative modify: `~/.claude/settings.json`
- Live-authoritative modify: `~/.codex/hooks.json`
- Snapshot via `.ai`: `/home/mageyuki/.ai/.claude/settings.json`
- Snapshot via `.ai`: `/home/mageyuki/.ai/codex-hooks.json`

### Step 1: establish a clean serial integration window

In `/home/mageyuki/.ai`, run `make diff-live` and `git status --short`. Require live files to match the committed snapshot and repository status to be empty. Stop on any unrelated change.

### Step 2: back up and append handlers

Create unique non-overwriting backups with the repository's `make backup` workflow. Append exactly one documented `herdr-top emit --from-hook claude-code` entry to each missing Claude array: `SessionStart`, `SessionEnd`, `SubagentStart`, `SubagentStop`, `TaskCreated`, `TaskCompleted`. Append exactly one `herdr-top emit --from-hook codex` entry to each missing Codex array: `SessionStart`, `SessionEnd`, `SubagentStart`, `SubagentStop`.

Preserve all existing objects and order; do not duplicate an identical command.

### Step 3: validate append-only semantics and behavior

Run:

```sh
jq empty ~/.claude/settings.json
jq empty ~/.codex/hooks.json
make test
```

Run the guide's prefix/append-only comparison against the unique backups and enumerate command strings and before/after counts. Complete Codex hook trust through the supported UI/control surface; never edit trust state directly. Fresh provider sessions must improve lifecycle/subject/dispatch evidence without duplicate runs. Hook failure cannot waive zero-configuration acceptance.

### Step 4: snapshot and commit live-authoritative configuration

Immediately before saving, rerun `make diff-live` and `git status --short`; require only the two intended live hook differences and an otherwise clean repo. `make save` itself runs `make validate`. Run:

```sh
make save MSG='feat(hooks): enable herdr-top emit precision'
```

Verify live/snapshot byte identity and a clean `.ai` worktree. Do not push `.ai`; report its local commit SHA.

## Task 8: publish and leave the PR ready to merge

**Files:** None. A CI or review finding creates a new explicitly scoped delta task before editing.

### Step 1: revalidate publication preflight

Resolve every fetch/push URL again; require one parseable owner `mageyuki`. Reconfirm permission, `main` PR trigger and jobs, branch/approval gates, Actions availability, and absent PR template.

### Step 2: push and create a Draft PR

Push `agent/live-truth-corrections` and create one Draft PR to `main`. Its self-contained body describes the problem, snapshot-authority state machine, guarded cancelled-turn continuation, exact tests, zero-hook live evidence, optional hook result, and review limitation: fresh independent Codex sessions substituted for unavailable Claude/Opus weekly capacity.

### Step 3: monitor conclusive latest-HEAD CI

An empty check list is pending. Success requires a non-empty applicable set, every check conclusive, only `success` or applicability-only `skipped`, and zero `failure`, `cancelled`, `timed_out`, or `action_required` conclusions.

For an in-scope failure, diagnose, create a fresh TDD delta task, independently review only the delta, commit/push, and wait for the replacement HEAD. Leave Draft and report only when safe repair requires scope expansion or new authority.

### Step 4: request and clear Copilot review

After latest-HEAD CI succeeds, mark Ready and request GitHub Copilot review when available. Verify every finding against code, tests, docs, and external facts. Valid findings use the fresh implementation/review/delta-gate flow; reply and resolve only after the decision and successful replacement CI. Re-request Copilot after every pushed fix.

Expected final state: latest-HEAD applicable CI is conclusively successful, no actionable Copilot thread remains, the PR is Ready for review, and it is not merged.
