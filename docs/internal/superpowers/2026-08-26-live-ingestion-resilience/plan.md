# Implementation plan: live-ingestion resilience (revised, round 3, final)

Spec: `docs/internal/superpowers/2026-08-26-live-ingestion-resilience/spec.md`
Branch: `agent/live-ingestion-resilience` (worktree, base `main` = c5c28ac)

Three tasks, executed serially in this order, all on one branch/PR. One
conventional commit per task (plus one docs commit for spec+plan), tests
first. Serialization is deliberate; no disjointness claim is made.

## Task 1 — F-D persistence observability and recovery (writer-first)

Expected files (complete set):
`src/store/writer.rs`, `src/herdr/collector.rs`, `src/diagnostics/mod.rs`,
`src/diagnostics/local.rs`, `src/doctor.rs`, `src/model/entities.rs` (GapKind
variant), `src/store/mod.rs` (gap_kind text mapping), `src/operator.rs` (only
if the health-payload change surfaces there; expected untouched given the
placement below), and the mechanical field-init sites for the new
`RuntimeDiagnosticsSnapshot` field: `src/herdr/controller.rs`,
`src/tui/app.rs`, `src/tui/projection.rs`, `src/tui/view.rs`,
`tests/controller.rs`, `tests/workload_harness.rs` (consumes
`subscribe_persistence()`, whose payload type changes). Test files:
`tests/doctor.rs` (goldens) and `tests/convergence.rs` (both for the
payload-type change — it types a `watch::Receiver<…PersistenceStatus>`
parameter near line 3562 — and for failure-trigger seam reuse: verify the
conditional trigger near line 1973 before writing a new seam). TUI edits are
compilation-mechanical only (new field init); no TUI behaviour change.

Order of work inside the task (writer-first; each numbered step TDD):

1. **Schema/versioning decision — ALL closed-reader changes in one step
   (D2 + the D4 counter).**
   - Detail placement: writer health watch payload becomes a small non-`Copy`
     struct (status + `Option<BoundedDetail>`, 240-byte truncation);
     `RuntimeDiagnosticsSnapshot` gains `persistence_detail`.
     `PersistenceStatus`/`PersistenceFailure` unchanged (stay `Copy`;
     `const fn` constructors unaffected; `src/operator.rs`'s exhaustive
     `Degraded { failure }` pattern untouched).
   - `Copy`-loss fallout to fix (all in the declared set): the four
     dereference sites `src/store/writer.rs:136` (`*self.sender.borrow()`),
     `src/store/writer.rs:606` (`let status = *self.health.borrow();`),
     `src/herdr/collector.rs:600` and `:700` (deref-copy of the borrow).
   - `skipped_enqueues` counter added to the counters struct NOW (schema-wise;
     the increment site lands in step 3): update
     `src/diagnostics/local.rs` `RawCounters` (deny_unknown_fields),
     `src/doctor.rs:1788` (`len != 5` counters arity), and the golden/fixture
     occurrences across `src/diagnostics/mod.rs`, `src/diagnostics/local.rs`,
     `src/doctor.rs`, `tests/doctor.rs`.
   - Failure-object readers relaxed for the optional detail: the exact-arity
     sites to audit are `src/doctor.rs:1621` (snapshot `!= 10`), `:1712`
     (`"degraded"` with `len == 2`), `:1721` (failure `!= 4`), `:1788`
     (counters `!= 5`) — NOT `:1699`, which is the unrelated
     enrichment-counters parser — plus `RawOccurrence`/`RawFailure` in
     `src/diagnostics/local.rs`
     (keep `deny_unknown_fields`, add `Option` fields). Readers accept BOTH
     old and new shapes; cross-version tests prove an old 4-field/5-counter
     record still parses.
   - `store_failure(...)` captures the `StoreError` `Display` text;
     synthesized failures carry typed absence.
2. **Writer-side recovery (D3).** In `src/store/writer.rs`: add a probe
   command (a minimal real write — pick the cheapest statement the writer
   already executes that exercises the failing surface; verify before
   coding). Its waiter bypasses the degraded-health short-circuit
   (`wait`'s health fast-path) and returns the actual result. On success the
   writer publishes `Degraded -> Healthy` via a new `publish_recovery`.
   Update `i4_writer_first_failure_is_sticky_and_watch_wakes_exactly_once` to
   pin the new contract (no spontaneous recovery; exactly one transition per
   successful probe) and add probe-success/probe-failure tests using the
   writer failure-injection seams (`install_temp_failure_trigger` scoping, or
   the `tests/convergence.rs` conditional-trigger pattern if it supports
   one-shot failure; else add a one-shot mode to the writer's test-control
   injector).
3. **Facade recovery + probe cadence (D3/D4).** In `src/herdr/collector.rs`:
   - `PERSISTENCE_RETRY_INTERVAL` (30 s prod) injectable through
     `RuntimePersistence` construction (tests pass a short interval; no tokio
     `test-util`, so cadence tests use the injected interval).
   - Amend the `ingest_writer_status` guard (the early return at
     `collector.rs:738-741` that today prevents observing a `Healthy` watch
     value while the facade snapshot is `Degraded`) so recovery can be seen.
   - Probe driving: the async gated paths (`apply`, `cleanup`) drive a probe
     when the cooldown elapsed. The synchronous `reserve_enqueue` only ARMS a
     "probe due" flag; the async staging caller consumes it (same
     flag-consumed-by-async-caller shape as the gap marker). `reserve_enqueue`
     increments `skipped_enqueues` on refusal.
   - On observed recovery: snapshot `Healthy`; `controller_input` restored to
     `Available` iff currently `Unavailable { PersistenceUnavailable }` AND no
     acceptor stop was recorded during the outage — `mark_acceptor_stopped`
     while degraded records a pending flag instead of being dropped, and the
     pending flag converts to `Unavailable { AcceptorStopped }` at recovery;
     recovery warn (warning_code `persistence_recovered`); raise the
     "recovered, marker pending" flag which the collector loop consumes to
     enqueue `RecordCollectorGap { kind: PersistenceOutage }` via
     `persist_submission` (Reducer in scope there).
   - Owner freshness is not restored on recovery; that pre-existing gap is
     outside this increment and remains recorded in the backlog.
4. **Transition warn (D1).** Warn on `Healthy -> Degraded` in the facade
   (`record_facade_failure`) — once per transition, not per skip.

Out of scope for this task: buffering dropped batches; TUI *behaviour* (the
degraded state already renders; the TUI edits above are field-init only).

## Task 2 — F-C flood tolerance

Expected files: `src/herdr/collector.rs` and `tests/convergence.rs` (fixture
anomaly-driver substitution only).

1. Anomaly exclusion (C1): filter `*_focused` event names at the two anomaly
   sites — candidate recording in `record_replay_facts` and the dirty check in
   `monitor_live` — NOT in `updated_entity` (its third consumer
   `cancel_pending_topology_closures` must keep seeing focus frames).
   `pane_agent_status_changed` keeps its current behaviour (it shares the
   `pane_focused` match arm; split the arm carefully).
2. Watchdog starvation (C2): derive a `produced_observations: bool` from the
   receipt-application path (the normalized `Vec<NormalizedEvent>` is owned in
   `apply_received_event` before it is consumed; thread the emptiness out via
   return value or a thin wrapper). In `monitor_live` and
   `monitor_reconciling`, execute `watchdog_probe = None; watchdog_deadline =
   silence_deadline(...)` ONLY when `produced_observations` is true; zero-op
   receipts leave both untouched so an in-flight probe survives the flood and
   completes.
3. Tests (red first), using the existing harness
   (`spawn_primary_collector_harness_with_policy` + `join_fake_server`, short
   `LivenessPolicy`), streams paced below the 64-slot overflow (C3), bounded
   waits rather than exact-iteration assertions (unbiased selects):
   - focus frames for an unknown pane id during replay/live do not produce
     `Dirty`;
   - in reconciling, a continuous zero-normalizing stream does not prevent the
     probe from being created, POLLED TO COMPLETION, and yielding
     `HealthyIdle -> RestartGeneration` (assert the exit within a bounded
     wait);
   - a receipt that does produce observations still resets the deadline and
     cancels an in-flight probe;
   - overflow guard: a burst exceeding the queue still yields `Dirty`.

## Task 3 — F-Z dispatch-only zero-fact closure

Expected files: `src/reducer.rs`, `src/activity.rs` (threshold accessor reuse
and/or a shared predicate helper), and their in-file test modules. The sweep
entry point is the existing `reducer.sweep_stale(now_ms)` call sites; the
signature must not change and `src/herdr/collector.rs` must not be touched.
If the implementation cannot avoid another file, stop and report.

1. Restructure `sweep_stale` so the new closure runs on EVERY sweep tick: the
   current early return (`if execution_ids.is_empty() { return ... }`) fires
   in exactly the steady state where the zombie population exists, so the new
   closure must be evaluated before/independently of the stale-execution
   pass — a sibling function called unconditionally from within `sweep_stale`,
   with both contributions merged into one `PersistBatch`.
2. Closure predicate (spec Z1, all conditions): `RunKey::Controller`-keyed;
   no execution attached (reuse the `runs_with_executions` construction from
   the dismissal path); state `== Queued` exactly (Running/Blocked stay open —
   liveness deferral for hook-managed roots is pinned by
   `root_liveness_defers_hook_only_expiry` and must keep passing); not
   dismissed; inactivity anchor `updated_at_ms.or(created_at_ms)` — when BOTH
   are `None` the run is NOT closed (no anchor means no evidence of age; a
   test pins this so an unanchored restored run cannot be closed on the first
   sweep tick) — and `now_ms - anchor >= activity::headless_inactivity_ms()`.
   Close to `EndedUnknown` with the same
   persist-op shape as `apply_lane_close` (state transition + finished_at).
   Do NOT copy `apply_lane_close`'s `non_lane_task_state_runs` exclusion —
   controller task-state evidence is expected here.
3. Tests (red first): closes after threshold (updated_at anchored, and
   created_at fallback when updated_at is None); does not close before the
   threshold; does not close `Running`/`Blocked` runs; does not close a run
   with a live execution; does not close dismissed/terminal runs; closure
   fires when NO stale execution exists (the restructuring guard); reopen via
   a controller Started transition and via a new execution binding; the closed
   run is dismissible by `OperatorCommand::DismissClearable` immediately;
   `root_liveness_defers_hook_only_expiry` still passes unmodified.

## Verification and process

- TDD per step; every behavioral test proven red by surgical reversion of the
  corresponding production hunk (sha-verified restore).
- `mise exec rust@1.97.1 -- make test` and `-- make lint` green after each
  task and at the end (worktrees lack the primary checkout's untracked
  `mise.toml`, hence the prefix).
- Load robustness (only for new timing-sensitive tests): run each affected
  `--exact` test 10 consecutive times while 16 `while :; do :; done` busy-loop
  subshells run; require 10/10; kill spinners and verify none survive.
- Spec + plan are committed on the branch as a docs commit before the task
  commits.
- Per-task review checkpoints at the wrapper's discretion; one final
  whole-diff review before push covers base..HEAD with a per-commit map.
- After merge + reinstall: live verification per the spec's acceptance
  evidence (expecting the closed zombies to appear briefly as terminal rows
  before `c`/aging), then re-check defect B (provider ingestion), expected to
  be resolved by F-D + F-C together.

## Risks

- D2/D4 touch closed privacy-safe contracts: bounded optional detail +
  one new counter, one versioning decision, readers accept old and new
  shapes, goldens updated with cross-version coverage. An old doctor binary
  reading a new log rejects new records — accepted (single-machine tooling
  upgraded together).
- D3 changes the writer health latch contract (sticky -> sticky-except-probe);
  the named sticky test is deliberately updated, not deleted — the new pin is
  "no spontaneous recovery, exactly one transition per successful probe".
- Probe outcomes on a genuinely broken store: probes fail, cooldown re-arms,
  detail refreshes — steady state is one cheap failed write per 30 s.
- C2's bias: an extra probe is NOT free (a probe can return `Reconnect` and
  force resubscription + gap recording); the design still biases toward
  probing because a missed probe means permanent livelock, while a spurious
  reconnect is bounded and self-healing. The reconciling test asserts the
  full RestartGeneration path to keep this honest.
- F-Z: restricting to `Queued` leaves Controller-keyed `Running`/`Blocked`
  rows to the existing 24-hour hook-only-stale fallback by design; the
  observed zombie population is entirely `queued`.
