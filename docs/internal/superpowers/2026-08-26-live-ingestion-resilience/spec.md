# Live-ingestion resilience: persistence recovery, flood tolerance, dispatch-only run closure

Status: revised after pre-implementation review (round 3, final)
Branch: `agent/live-ingestion-resilience`

## Background

A live investigation of a monitor instance that silently stopped recording events
identified three independent defects, confirmed against the running process and
the code with line-level evidence.

1. **Terminal persistence degradation (D).** A single failed SQLite command
   permanently silences all persistence. The writer health latch
   (`src/store/writer.rs`, `publish_failure`) is one-way `Healthy -> Degraded`
   (pinned by `i4_writer_first_failure_is_sticky_and_watch_wakes_exactly_once`);
   the collector facade re-derives `Degraded` from the writer watch on every
   command; `skip_if_degraded` then drops `apply`/`cleanup`/owner updates with
   only a counter, and `reserve_enqueue` refuses the live-ingestion staging path
   entirely — silently and without any counter. The retained failure detail is
   only a coarse `code` (e.g. `"sqlite"`); the underlying error text is
   discarded, making the trigger undiagnosable after the fact.
2. **Reconciling livelock under an event flood (C).** The herdr server can emit
   a continuous stream of `*_focused` events (observed: a ~10 Hz level-triggered
   rebroadcast of focus state, including a stale pane id absent from every
   session snapshot). Focus events for an entity missing from the topology make
   the anomaly check report `Dirty`; after `RESNAPSHOT_ATTEMPTS` the collector
   enters `monitor_reconciling`. Both `monitor_live` and `monitor_reconciling`
   unconditionally execute `watchdog_probe = None; watchdog_deadline = ...` on
   **every** primary receipt, so a steady flood both postpones probe creation
   and destroys any in-flight probe before it completes. The collector parks
   forever: no snapshot requests, no probes, no resubscription (confirmed by
   70 s of 13 µs file-descriptor sampling showing zero new connections).
3. **Dispatch-only zero-fact zombie runs (Z).** Task runs created solely from
   replayed controller dispatch events (`RunKey::Controller`, state `queued`,
   no execution ever attached) are never closed: the append-less inactivity
   closure lives in the provider lane (`Synthesis` in `src/provider/lane.rs`
   driving `Reducer::apply_lane_close`) and deliberately excludes runs with
   controller task-state evidence. Such runs stay `queued` indefinitely. They
   only become dismissible via the hook-only-stale carve-out in
   `OperatorCommand::DismissClearable` (`activity::is_hook_only_stale_task_run`)
   after a 24-hour visibility deadline — observed live as "`c` does nothing"
   for rows ~11 hours old. They should instead close like other abandoned runs.

## Requirements

### F-D: persistence degradation must be observable and recoverable

- D1. On the `Healthy -> Degraded` transition, emit one `tracing::warn!`
  (warning_code `persistence_degraded`); the existing structured
  `HERDR_TOP_PERSISTENCE_V1` record is kept.
- D2. Retain bounded error detail. `PersistenceFailure` itself stays `Copy` and
  privacy-closed as today. A new optional, bounded detail string (the store
  error's `Display` output, truncated to 240 bytes) is captured where a real
  `StoreError` is available (`store_failure`); synthesized failures (queue
  admission, acknowledgement loss) have typed absence. Placement decision:
  the detail travels NEXT TO the status, not inside it — the writer health
  watch payload becomes a small non-`Copy` struct (status + detail), and
  `RuntimeDiagnosticsSnapshot` gains a `persistence_detail: Option<...>`
  field. `PersistenceStatus` and `PersistenceFailure` are unchanged, so
  `src/operator.rs`'s exhaustive `Degraded { failure }` pattern and all
  `const fn` constructors are untouched. Every closed reader/golden that the
  two schema additions touch (the failure/occurrence readers AND the counters
  reader — see D4) is updated in one versioning decision: readers accept both
  the old and new shapes; cross-version tests prove old records still parse.
  An old doctor binary reading a new log will reject new records — accepted
  (single-machine tooling upgraded together). Privacy stance: the detail is a
  bounded store-error string in a local, non-shared log and diagnostics
  channel.
- D3. Bounded recovery, writer-authoritative. A recovery probe is a real
  writer command whose acknowledgement is awaited WITHOUT the degraded-health
  short-circuit (the waiter's health fast-path is bypassed for probes). On
  probe success the WRITER publishes `Degraded -> Healthy` (the one-way latch
  gains an explicit recovery publication; the sticky test is updated to pin
  the new contract: no spontaneous recovery, exactly one transition per
  successful probe). The collector facade observes the recovery through the
  existing health watch — which requires amending the facade guard that today
  returns early unless its own snapshot is `Healthy`
  (`ingest_writer_status`) so a `Healthy` watch value can be observed while
  the facade snapshot is `Degraded`. On observed recovery the facade restores
  its snapshot to `Healthy`, restores `controller_input` to `Available` iff
  it is `Unavailable { PersistenceUnavailable }` AND no acceptor stop was
  recorded during the outage (an acceptor stop observed while degraded is
  remembered and takes precedence: `controller_input` then becomes
  `Unavailable { AcceptorStopped }`), emits a recovery `tracing::warn!`
  (warning_code `persistence_recovered`), and raises a "recovered, marker
  pending" flag; the collector loop consumes the flag and enqueues a
  `RecordCollectorGap { kind: PersistenceOutage }` through the normal
  `persist_submission` path (where a `Reducer` is in scope). Owner freshness
  is not restored on recovery; that pre-existing gap is outside this increment
  and remains recorded in the backlog.
- D4. Probe cadence and gating: at most one probe per
  `PERSISTENCE_RETRY_INTERVAL` (default 30 s, injectable for tests). The
  async gated entry points (`apply`, `cleanup`) may drive a probe directly;
  the synchronous `reserve_enqueue` CANNOT await — it only ARMS a "probe due"
  flag, which the next async caller (the staging path caller is async)
  drives. A failed probe re-arms the cooldown and refreshes the retained
  detail. Batches and staged events dropped while degraded stay dropped (no
  buffering). The currently uncounted `reserve_enqueue` refusal gains a
  `skipped_enqueues` counter; because the counters object is itself a closed
  schema (deny-unknown-fields raw reader, exact-arity doctor check, and
  multiple goldens), this counter addition is part of the same single
  versioning decision as D2.

### F-C: a flood of no-op events must not disable convergence or the watchdog

- C1. `workspace_focused` / `tab_focused` / `pane_focused` receipts are no
  longer anomaly candidates. The exclusion is applied at the two anomaly
  sites (candidate recording in replay, and the live dirty check) — NOT in the
  shared `updated_entity` helper, whose third consumer
  (`cancel_pending_topology_closures`) legitimately uses focus frames as
  re-observation evidence and must keep doing so.
- C2. In both `monitor_live` and `monitor_reconciling`, a primary receipt that
  normalizes to zero observations leaves BOTH `watchdog_deadline` AND any
  in-flight `watchdog_probe` untouched. Only receipts producing a non-empty
  normalized observation reset the deadline/probe state. The acceptance test
  asserts probe COMPLETION and the `HealthyIdle -> RestartGeneration` exit
  from reconciling under a continuous zero-normalizing flood — with a bounded
  wait, not an exact-iteration assertion (the selects are unbiased, so branch
  choice among ready branches is pseudo-random).
- C3. No change to the probe itself, `RESNAPSHOT_ATTEMPTS`, or the
  `EVENT_QUEUE_CAPACITY`-overflow `Dirty` path: a flood fast enough to
  overflow the 64-slot reader queue still forces `Dirty` by design, so F-C
  tests pace their streams below overflow, and a guard test pins the
  overflow behaviour.

### F-Z: dispatch-only zero-fact runs must close and become clearable

- Z1. The reducer's sweep gains a closure for runs that are
  (a) `RunKey::Controller`-keyed, (b) have no execution attached (the
  `runs_with_executions` notion already used by
  `activity::is_hook_only_stale_task_run`), (c) are in state `Queued`
  exactly — `Running` and `Blocked` are deliberately excluded because they
  describe live work (hook-managed roots rely on liveness deferral, pinned by
  `root_liveness_defers_hook_only_expiry`), while the observed zombie
  population is entirely `queued` — (d) are not dismissed, and (e) have been
  inactive (anchored at `updated_at_ms.or(created_at_ms)`; when both are
  absent the run is NOT closed — no anchor means no evidence of age) for
  at least `activity::headless_inactivity_ms()` (default 10 minutes). They
  close to `ended_unknown` at sweep time. This closure applies to runs WITH
  controller task-state evidence (unlike the lane closure): a queued dispatch
  that never attached an execution and has been silent past the threshold is
  abandoned. The sweep must run on every sweep tick regardless of whether any
  stale execution exists (the current `sweep_stale` early-returns when no
  execution is stale; the new closure must not sit behind that guard).
- Z2. Reopen semantics (verified paths only): a later controller
  Started/Blocked/Progress transition for the same run, or a new live
  execution binding to it, reopens the run; `touch_run_liveness` alone does
  not reopen terminal runs. Tests exercise the real reopen paths.
- Z3. Once `ended_unknown` (terminal), the TUI `c` key dismisses them through
  the existing `DismissClearable` path immediately (no 24-hour wait). Closed
  rows remain visible as terminal rows for the terminal-visibility window
  before aging out — the live acceptance check expects that brief
  reappearance. No TUI change expected.

## Non-goals

- Fixing the herdr server's focus-event rebroadcast or its stale focus
  reference (tracked separately in the herdr repository).
- Changing the slow-consumer eviction behaviour of the herdr server.
- Buffering or replaying batches dropped during a persistence outage.
- Changing the 24-hour hook-only-stale dismissal carve-out (it remains as a
  fallback for non-closed Controller-keyed runs, including `Running`/`Blocked`
  ones this increment deliberately leaves open).

## Acceptance evidence

- `make test` and `make lint` green at MSRV 1.97.1 (in worktrees:
  `mise exec rust@1.97.1 -- make ...`; the bare-make failure is caused by the
  primary checkout's untracked `mise.toml` not existing in worktrees).
- TDD: each requirement lands with a test written first and proven red against
  the pre-change behaviour by surgical production-hunk reversion
  (sha-verified restore).
- Load robustness for new timing-sensitive tests: 10 consecutive green runs of
  the affected `--exact` test binaries while 16 `while :; do :; done` busy-loop
  shells run, spinners killed and verified gone afterwards.
- Live verification after merge: on a monitor attached to the flooding herdr
  session — (1) doctor's live `controller.runtime.observed.persistence.status`
  is `healthy` (the historical-occurrence warning may persist by design);
  (2) events continue to persist during the focus flood; (3) the legacy zombie
  rows close within one inactivity threshold, appear briefly as terminal rows,
  and `c` clears them.
