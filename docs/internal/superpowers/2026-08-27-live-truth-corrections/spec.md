# Live truth corrections: snapshot-authoritative topology and resumable turns

Status: awaiting user approval
Branch: `agent/live-truth-corrections`

## Background

Live verification after the backfill-fidelity work exposed two independent
correctness defects. Both make the TUI disagree with the current Herdr and
provider state rather than merely lagging it.

1. **Historical Herdr replay overwrites the current topology.** A primary
   Herdr subscription replays historical topology frames without exposing a
   global sequence or cursor. On one long-lived pane, the current snapshot
   identified a Codex session and the current pane label, while the newly
   opened subscription replayed older Claude pane revisions and an older
   label. On another session, the snapshot no longer contained a previously
   closed pane, but replay delivered `pane_closed` before older
   `pane_created`/`pane_updated` frames for that pane. herdr-top applied those
   frames as current state. The 30-second watchdog then detected divergence,
   reconnected, and repeated the same replay. The result was a deterministic
   roughly 33-second reconnect loop, a stale pane label, and a newly minted
   provisional unlinked Task Run on every cycle.
2. **A cancelled Codex turn permanently hides a continuing session.** Codex
   provider logs represent an interrupted turn as `turn_aborted`, which the
   lane maps to `Cancelled`. The next turn in the same native Codex session
   emits a later `task_started`, but the reducer permits a log-lane reopen only
   from `Completed`. The valid later start is rejected as stale, later terminal
   facts conflict, and the unchanged native run remains cancelled even while
   the provider session continues to produce activity.

A related display defect made the replay overwrite persistent. Snapshot
reconciliation currently treats a missing tab label or pane display name as
"preserve the old value." Pane display names also fall back to the terminal
title. Consequently, an authoritative `label: null` cannot clear a historical
label, and a terminal title can be presented as though it were a pane name.

These are monitor-side defects. The fix must not depend on changing Herdr's
server replay behavior, adding an unavailable event cursor, enabling Controller
`emit`, or deleting historical rows from the local store.

## Decisions

### T1: snapshots are the sole authority for physical topology

The complete `session.snapshot` result is authoritative for:

- workspace, tab, and pane membership;
- parentage and pane location;
- tab and pane labels, including their absence;
- pane terminal identity;
- the pane's current agent provider and native-session identity; and
- the agent state captured by that snapshot.

The primary Herdr event subscription remains necessary for low-latency change
notification and disconnect detection, but its topology-shaping frames are
invalidation hints, not independently authoritative mutations. This applies to
workspace/tab/pane create, rename, close, update, move, exit, and agent-detected
frames. Focus and layout frames retain their existing non-topology behavior.
Pane-scoped `pane_agent_status_changed` enrichment remains a live gauge for an
already snapshot-admitted pane and execution; it cannot create topology or
change provider/native-session identity.

The collector uses a subscribe/snapshot/drain/snapshot handshake:

1. Establish the primary subscription before the first snapshot, preserving
   the existing no-gap ordering.
2. Install the first complete snapshot as the current authority.
3. Drain the subscription's catch-up burst without applying its
   topology-shaping frames. Coalesce them into at most one refresh request.
4. Take a closing snapshot after the catch-up quiet boundary and atomically
   reconcile that snapshot before entering live mode. A real mutation between
   subscription establishment and the closing snapshot is therefore retained;
   historical replay cannot override the snapshot.
5. In live mode, a topology-shaping frame triggers an immediate, coalesced
   snapshot refresh. Events arriving while a refresh is in flight request at
   most one subsequent refresh. The refresh result, not the triggering frame,
   is applied.

If a refresh fails, the triggering frame is never applied speculatively. The
last complete snapshot remains the materialized truth, observation quality
leaves `Live`, and the existing bounded reconnect/retry machinery obtains a new
complete snapshot. This is a drop/preserve-last-good fail-safe.

No decision relies on `PaneInfo.revision`: observed historical pane revisions
were monotone within the replay, but a real pane-label change did not advance
the revision. No decision relies on event receipt order, emitted timestamp, or
a server cursor that Herdr does not provide.

The following invariants must hold:

- a pane absent from the closing snapshot cannot be created by replay;
- a historical pane agent/session cannot displace the snapshot's current
  agent/session or end its execution;
- a replayed close cannot remove an entity present in the closing snapshot;
- owner location and enrichment target sets are derived from the applied
  snapshot, not a raw move/create/close hint;
- a burst of topology hints is coalesced and cannot create an unbounded
  snapshot-request loop; and
- after a successful refresh, the watchdog compares against exactly the state
  installed from that refresh, so stale replay cannot cause periodic
  `topology_diverged` reconnects.

The existing 30-second monotonic watchdog deadline and reconnect backoff remain
unchanged. If an event is missed entirely, the watchdog snapshot still bounds
eventual convergence. A normally delivered rename or topology change converges
through its immediate refresh rather than waiting for the watchdog.

Primary-stream diagnostics gain bounded, in-memory counters for suppressed
catch-up topology frames and event-triggered topology refreshes. They are test
and diagnosis surfaces only in this increment; persisted records and doctor
JSON do not change schema. Counter arithmetic saturates like the existing
primary-stream counters.

### T2: absence is authoritative for display names

Name handling must distinguish three meanings at the collector/reducer
boundary:

- `Preserve`: a genuinely partial, non-authoritative observation omitted the
  field;
- `Set(value)`: an authoritative observation supplied a non-empty, sanitized
  label; and
- `Clear`: an authoritative observation supplied null, an empty value that
  sanitizes to empty, or otherwise established absence.

The exact internal type name is not prescribed, but source-event string checks
and implicit `Option` retention are not sufficient. Complete snapshots use
`Set` or `Clear`, never `Preserve`. An explicit rename-clear event, where the
wire supports one, also means `Clear`. The closing snapshot is decisive if a
rename event and snapshot disagree.

Pane display names come only from `pane.label`. `terminal_title_stripped`, cwd,
shell title, agent provider, and task subject are not pane names and must not
occupy the pane-name slot. They may remain available in their existing detail
or task surfaces.

The tree grammar is:

- pane label present: `Pane: <pane-id> (<label>)`;
- pane label absent: `Pane: <pane-id>`;
- tab label present: `Tab: <tab-id> (<label>)`;
- tab label absent: `Tab: <tab-id>`.

No empty parentheses are rendered. The same null/clear rule applies
defensively to tabs even though the current `herdr tab rename` CLI does not
offer a clear flag. Workspace and session identifiers are unchanged.

The watchdog's topology comparison uses exact authoritative name semantics.
A probed null must not be rewritten from the current model before comparison.
This lets the watchdog repair a missed clear and prevents a stale persisted
name from being considered equal to a nameless snapshot.

### T3: a newer Codex turn may reopen a lane-cancelled run

`turn_aborted` remains a truthful terminal observation for the interrupted
turn: the Task Run enters `Cancelled`, receives its terminal timestamp, and is
temporarily rendered as cancelled. It does not mean that the native Codex
session can never run another turn.

Extend the existing log-lane reopen exception so `task_started` may reopen a
run from either `Completed` or `Cancelled`, subject to every existing guard:

- the start source is exactly the provider log lane;
- the prior terminal source is exactly the provider log lane;
- the incoming fact belongs to the same resolved run/native session;
- its source timestamp is strictly greater than the stored terminal timestamp;
  and
- ordinary identity, ledger, and binding-conflict validation still succeeds.

`Failed` remains terminal and cannot reopen through this exception. Equal or
older starts remain stale. Controller/manual starts do not acquire a new broad
terminal-reopen power.

A successful reopen preserves the Task Run ID, native key, execution lineage,
display ordinal, subject, and accumulated telemetry identity. It sets the run
to `Running`, clears the obsolete terminal timestamp and terminal-source
bookkeeping, clears a prior dismissal as the existing start path does, and
allows a later terminal fact to be accepted normally. It must not mint a second
run or a second live pane execution for the same native session.

The rule must work during live tailing and after restart/backfill. Historical
starts at or before the cancelled terminal remain rejected after restart; a
previously rejected, genuinely later start is accepted under the corrected
rule.

### T4: Controller `emit` remains an optional precision layer

Zero-configuration provider-log monitoring is the correctness baseline. This
increment must be correct with no hook registration and no Controller events.
`emit` remains additive:

- provider hooks can sharpen explicit lifecycle transitions;
- Controller-authored subjects can improve labels;
- dispatch events can add explicit parentage without session-ID evidence; and
- manual events can add semantic dependency edges.

Controller events do not authorize physical topology and cannot bypass the
snapshot boundary. When hook events and provider-log facts describe the same
native run, existing identity resolution must converge them into one Task Run.
The cancelled-turn reopen stays narrowly log-lane-authored; `emit` does not
become a generic terminal-state override.

Acceptance includes mixed-source coverage proving that hook `task_started` or
metadata followed by provider-log activity, cancellation, and a later valid
log start neither duplicates the run nor regresses its state.

Enabling and validating the optional hook registrations is the final
operational task in this increment. It runs only after the fixed binary passes
the zero-configuration live acceptance, so hook evidence cannot mask or waive
a failure in the baseline path.

### T5: persistence converges forward without destructive cleanup

No store migration or history purge is required. Applying an authoritative
snapshot overwrites stale tab/pane names in the normal persistence batch, and
normal topology/execution closure retires entities absent from current
snapshots. Previously created historical Task Runs remain auditable and follow
the existing terminal visibility and dismissal rules. The fix guarantees that
reconnects create no additional phantom runs.

## Expected implementation boundaries

The implementation plan must declare exact file sets per task. The expected
subsystems are:

- `src/herdr/collector.rs`: catch-up suppression, coalesced snapshot refresh,
  exact snapshot comparison, label-only pane projection, owner/target refresh,
  and primary-stream counters;
- `src/diagnostics/mod.rs`: bounded in-memory primary-stream counter fields;
- `src/reducer.rs` and, only if an explicit mutation type is shared,
  `src/model/entities.rs`: authoritative name clear semantics and the guarded
  cancelled-run reopen;
- `src/store/mod.rs` and `src/operator.rs` if the existing coalescing name
  upserts require an explicit pane-name clear persistence operation;
- TUI projection/view tests only if the existing row helper does not already
  satisfy the null grammar;
- `tests/convergence.rs` plus focused collector/reducer/controller tests and
  wire fixtures needed for end-to-end replay sequences; and
- `docs/design/herdr-top-mvp.md`, `docs/tui.md`, and
  `docs/guides/controller-emit-setup.md` for the authority, display, and
  optional-precision contracts.

No dependency addition and no database migration are expected. Discovering a
need for either upgrades the plan for explicit review before implementation.

## Acceptance evidence

### Deterministic regression tests

1. **Old pane history cannot overwrite a current snapshot.** Start from a
   snapshot containing one pane with a current Codex native session and label
   `Agent`. Feed a catch-up burst containing older Claude pane revisions,
   including an equal-revision stale label. After the closing snapshot, assert
   one current Codex execution, the `Agent` label, no historical Claude
   execution, and no extra Task Run.
2. **A snapshot-absent pane cannot resurrect.** Start from a snapshot without a
   historical pane. Replay close-before-create/update/move frames for it, then
   repeat the reconnect generation. Assert no pane, execution, provisional
   run, enrichment target, or count growth.
3. **No watchdog loop.** With an injectable short liveness interval, prove that
   a successful closing snapshot followed by suppressed historical replay
   reaches `Live` and remains there across at least two watchdog boundaries.
   Snapshot request and subscription counts are bounded; no periodic
   topology-divergence reconnect occurs.
4. **Live changes refresh promptly and coalesce.** A live pane/tab rename and a
   create/move/close burst trigger bounded immediate snapshots, apply only the
   snapshot result, and do not wait for the watchdog. A failed refresh leaves
   the last good model intact and exits `Live`.
5. **Name truth.** Snapshot label set then snapshot label null renders
   `Pane: <id> (Agent)` then `Pane: <id>`. A terminal title with no label still
   renders `Pane: <id>`. Tabs obey the same present/absent grammar. Persisted
   stale names are cleared after reconciliation and remain cleared after
   restore.
6. **Codex continuation.** Feed `task_started`, `turn_aborted`, a strictly
   later `task_started`, and `complete` for one native Codex session. Assert the
   same run transitions `Running -> Cancelled -> Running -> Completed`, its
   terminal timestamp is cleared on reopen and reset on completion, and no
   duplicate run/execution appears. Equal/older starts and starts after
   `Failed` are rejected. Repeat across restore/backfill.
7. **Mixed-source precision.** Add valid hook/Controller metadata and starts to
   the preceding provider-log sequence. Assert one run, preserved optional
   subject/edges, and the same final lifecycle without stale/conflict
   pollution from the valid later log start.

TDD red-first evidence is required for each behavioral group. Timing-sensitive
tests use injected deadlines and bounded waits rather than wall-clock sleeps.

### Repository gates

Run the complete gates under a process that explicitly restores default
SIGHUP handling:

```sh
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- \
  mise exec rust@1.97.1 -- make test
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- \
  mise exec rust@1.97.1 -- make lint
```

The full `workload_harness` suite is mandatory. Its two process/signal tests
must not be waived as environmental flakes: inheriting `SIGHUP=SIG_IGN` from a
`nohup` parent deterministically produces the approximately 301-second hang,
while the default disposition completes successfully.

### Live acceptance after build and install

1. Rebuild the current branch and install that exact binary into the normal
   user binary location.
2. In a session whose API snapshot has pane label `Agent`, verify the TUI
   changes to `(Agent)` after restart and remains stable for longer than two
   watchdog intervals, with no roughly 33-second reconnect cycle.
3. In a session containing only a continuing Codex pane, verify the live Codex
   Task Run is visible, an interrupted turn may show cancelled temporarily,
   and the next turn returns the same row to running.
4. Verify no new snapshot-absent pane or provisional unlinked Task Run appears
   across reconnects.
5. Rename a pane and tab and observe event-triggered convergence; clear the
   pane label and verify parentheses disappear. Restore any temporary test
   label before completion.
6. After steps 2-5 pass without hooks, back up and append the documented
   `emit` commands to the existing Claude Code and Codex hook arrays, preserving
   every pre-existing handler and accepting Codex hook trust. Verify the files
   still parse and satisfy the guide's append-only checks.
7. Start fresh provider sessions and verify the hooks improve
   subject/lifecycle/dispatch detail without creating duplicate rows. A hook
   delivery failure does not retroactively waive steps 2-5.

## Non-goals

- Changing Herdr's server-side replay or retention behavior.
- Inventing a global event cursor or relying solely on pane revision.
- Polling continuously; snapshots remain startup, event-triggered, recovery,
  and watchdog operations.
- Using Controller `emit` as a topology source or a prerequisite for correct
  monitoring.
- Automatically deleting previously persisted phantom Task Runs.
- Implementing the deferred wrapper-child linking design for headless
  `codex exec` or `claude -p` children.
- Changing dependency semantics, provider-log privacy allowlists, or telemetry
  persistence.

## Follow-up sequence

1. Complete and live-verify the P0 truth/convergence implementation without
   hooks, then enable/verify the documented Claude Code and Codex `emit` hook
   registrations as the increment's final operational task. Measure the
   additional lifecycle, subject, and dispatch-edge precision against the
   zero-configuration baseline.
2. Enter release preparation. Run the required repository-wide security audit
   with an intentionally sufficient budget, triage its completed findings,
   then perform the first-release flow. The security audit is a release
   trigger, not a per-change gate for step 1.
3. Design and implement the already-deferred wrapper-child linking increment
   from the committed inner-worker investigation evidence; do not reintroduce
   free-text UUID inference.
