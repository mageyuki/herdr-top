# Increment 6: Controller Emit Integration and Hardening

**Status:** approved design for implementation planning.
**Baseline:** `main = 5a0eab7` (Increment 5 merged as PR #4; real-herdr
subscription hotfix merged as PR #5).
**Design references:** `docs/design/herdr-top-mvp.md` sections 5.4, 7.3, 12.3,
14, and 20; `docs/superpowers/plans/2026-08-12-increment-5-reliability-performance.md`
(the frozen Increment 5 plan, called "the Increment 5 plan" below).

## Summary

Increment 6 completes the last unimplemented element of the MVP design's
Controller story — the calling side of `herdr-top emit` — and discharges every
obligation carried out of Increment 5. It has three phases, integrated
serially: Phase A hardens product code (doctor version parsing, per-pane
agent-status subscriptions, warning throttling, two recorded fragility fixes);
Phase B closes the open NonD4 amendment with an evidence-backed acceptance
change, hardens the measurement harness, and runs one confirmation
measurement; Phase C ships a Rust-native hook adapter (`herdr-top emit
--from-hook`) plus reference hook registrations and setup documentation for
both Claude Code and Codex, lighting up deep monitoring content (execution
tree and task lifecycle) without violating the no-heuristics binding rule.

Increment 7 (packaging and release, MVP design section 12) follows this
increment and is out of scope here. The release gate depends on the NonD4
amendment closed by Phase B.

## Goals

1. Close the D4-checkpoint output `AmendmentsRequired[non_d4]` for the burst
   `SupportedLoadDegradation` miss with a justified acceptance amendment,
   deterministic re-derivation over the preserved Increment 5 final
   measurement, and one confirmation measurement on the hardened head.
2. Fix `doctor`'s integration-version check so live herdr version responses
   (date-form) no longer report `InvalidActiveVersion` unconditionally.
3. Restore the pane-scoped `pane.agent_status_changed` subscription in its
   correct per-pane form, recovering transition-timestamp fidelity, ledger
   rows and activity items, rate accounting, and the fourth topology-closure
   rescue trigger lost in the PR #5 hotfix.
4. Throttle the subscription-failure warning and cover the server-rejection
   error variant with a regression test.
5. Apply the two recorded reliability prescriptions: the performance-watch
   BrokenPipe fix (`HeaderInputs::default()` first) and the OwnerLock
   explicit-unlock-on-drop plus fork-before-exec regression.
6. Land the six measurement-harness backlog items, re-adjudicate the two
   documented permissivenesses, and apply the guard-ordering nicety, before
   the confirmation measurement uses the harness.
7. Ship the emit calling side for Claude Code and Codex: a tested Rust hook
   adapter, reference hook registrations, and setup documentation, so that
   Controller sessions produce Task Runs, dispatch edges, and task lifecycle
   in the TUI through explicit events only.

## Non-goals

- Packaging, release artifacts, and Marketplace work (Increment 7).
- D4 incrementalization. The Increment 5 D4 checkpoint did not authorize it,
  and nothing in this increment may implement it.
- Server-side (herdr) changes. herdr is an external product; only client-side
  behavior changes. Upstream reports remain the only server-side channel.
- Inference or heuristics for session-to-pane binding. Deep content appears
  only through explicit Controller events, per the design's non-goal.
- Automatic dependency-edge derivation from hooks. Hook surfaces expose
  dispatch structure and task lifecycle, not inter-task data dependencies;
  `depends_on` remains an explicit Controller action documented in setup
  guidance.
- Correction or supersede events, multiple TUI clients, and the other MVP
  section 18 deferrals.

## Context and carried obligations

Increment 5 closed with all tasks executed and its final measurement
(attempt 20260826, subject `e86e0ef`) valid: six scenarios passed and burst
failed with the single reason `supported_load_degradation`, reproduced across
two attempts. The Section 15 re-derivation (report SHA-256 `48d18f12…`) and
the D4 checkpoint classifier (document SHA-256 `bb6cc481…`) both returned
`amendments_required["non_d4"]`. That decision obligates a non-D4 design
amendment before the acceptance can pass and, through the Increment 7 release
gate, before any release.

The consolidated carry ledger from Increment 5 and the post-close live-herdr
findings contribute the remaining obligations: the doctor version-check
defect, the per-pane subscription follow-up with its two hotfix companions,
the F3 BrokenPipe and OwnerLock prescriptions, six harness backlog items
(controller-binary validation hardening, the CurDir/canonicality joint
validator, the recorder socket-shape predicate, the outer-trap hardening set,
the fixture TOCTOU class, and comparator exposure), two documented
permissivenesses (the bounded trailing-EventLag admission and the
`last_pre_origin` expect), the guard-ordering nicety, and four catalogued
fragility sub-classes to be carried as review checklist items.

## Considered approaches

Three shaping decisions were made before this design and are recorded with
their alternatives.

1. Increment structure. Alternatives: one combined increment including
   packaging; two increments with packaging design written now; two
   increments with packaging planned after this one lands. The last was
   chosen: the NonD4 closure is a release-gate dependency, so packaging
   planning would rest on unfinished ground, and Increment 5 demonstrated the
   review cost of oversized plans.
2. Execution order. Hardening before emit was chosen over emit-first: the
   hardening tasks are small and prescribed, the per-pane subscription
   changes the collector path that the confirmation measurement must cover,
   and the emit work is the design-heavy tail that benefits from an already
   quiet tree.
3. Hook mapping placement. A Rust-native adapter mode on the existing `emit`
   subcommand was chosen over a shell reference-script family. The mapping
   logic becomes unit-testable in the product suite, needs no runtime
   dependency (no `python3` or `jq` on the hook path), and avoids embedding
   untested logic in shell — the defect class this project has paid for
   repeatedly. The alternative would have mirrored herdr's own integration
   style but placed the most semantically loaded logic outside the test
   suite.

## Phase A: product hardening

### A1: doctor integration-version parsing

`doctor` currently expects integer integration versions (Claude Code 6 or
newer, Codex 5 or newer, MVP design section 12.3) while live herdr answers
the version query in a date form such as `2026.08.12.1`, so the check always
reports `InvalidActiveVersion`. The parser accepts both forms: an integer
compares under the existing rule, and a date-form version is treated as
belonging to a newer versioning era than any legacy integer requirement and
therefore satisfies the minimum. The raw reported string is preserved in
`doctor` output either way; nothing is inferred from paths. Malformed values
keep reporting a structured invalid-version diagnostic. The implementation
plan must capture the exact live response shape from a real herdr 0.8.0
server before freezing fixtures, and unit tests must cover integer, date
form, and malformed inputs for both human and JSON renderers.

### A2: per-pane agent-status subscription

The PR #5 hotfix dropped the unscoped `pane.agent_status_changed`
subscription because live herdr requires a concrete `pane_id`. This task
subscribes in the correct per-pane form: the collector derives the live pane
set from snapshots and `pane.updated` events, adds a scoped subscription when
a pane appears, and releases or abandons it when the pane closes. Whether
herdr supports explicit unsubscription — and whether an orphaned scoped
subscription is benign — must be verified against the live server during
planning; the design permits either explicit removal or documented benign
abandonment, but not silent accumulation without evidence.

The scoped subscriptions ride a dedicated secondary connection, separate
from the primary subscription connection, because live herdr accepts
exactly one `events.subscribe` per connection. The primary connection and
its convergence lifecycle are untouched: replacing the secondary
enrichment connection on a pane-set change is NOT an observation gap in
the design's sense — the primary stream and the `pane.updated` fallback
remain continuous throughout, so no execution is retired and no collector
gap is recorded. Live herdr also decorates the response id on the
subscribe error path (observed: `<id>:sub:<index>:<token>`), so the wire
client must recognize a decorated error response as belonging to its
request before the collector can read `pane_not_found`.

Two consequences are binding. First, pane-set changes swap the enrichment
connection BREAK-BEFORE-MAKE: the old connection closes before the new
one subscribes, so no transition is ever delivered twice — herdr-sourced
events carry no deduplication key, and an overlap window would
double-record ledger rows, activity items, and rate observations; the
close-to-resubscribe gap is bounded per attempt but open-ended under
sustained enrichment failure — surfaced by the per-stream health below —
and is covered throughout by the fallback path. Second, enrichment events are isolated from the primary pipeline:
they never enter the primary connection's replay and anomaly
classification, they never touch the primary's overflow state, and their
subscription health is tracked separately so an enrichment failure can
neither degrade the primary's observation quality nor be masked by
primary health. The enrichment reader only reads and parses; each
event becomes a pane-level payload (pane and terminal identity, the
parsed status, and the receipt instants — a normalized status event
needs an execution identity that only application-time matching
determines, and event identities are unique in the store) forwarded
over a dedicated bounded channel to the converge task. Losses are
observable through a new dedicated diagnostics counter family
(channel-full drops — failed enqueues in any phase — and episode
discards — enqueued payloads consumed without applying in a non-Live
loop; which counter a lost transition lands in is determined by where
it was lost, not by phase duration), produced by a cloneable handle in
the codebase's existing cross-task counter pattern, published beside
the existing counter families, and rendered by doctor without touching
the closed controller-counter shape. The converge task is the single
consumer and already owns every piece of state involved — the model,
the pending topology closures, and its own phase — so no cross-task
EVENT-ORDERING apparatus (lock, ordering counter, watermark) exists;
the diagnostics handle orders nothing and is orthogonal. It APPLIES
payloads only while Live; in every non-Live LOOP, including the
terminal Reconciling state, it consumes the channel through a bounded
synchronous drain at each loop iteration — never a `select!` arm,
which would cancel the convergence loops' inline primary futures and
let sustained enrichment traffic starve convergence — discard-counting
without applying, and on entering Live it drains and discards what was
enqueued before the drain completes (the reader is phase-unaware, so
that is the exact discarded set). This is
an owner-decided simplification, accepted on the accurate premise:
ordinary convergence episodes (startup, reconnect, resnapshot) are
seconds-scale, while the terminal Reconciling state is open-ended —
but there the entire herdr source is already degraded and surfaced as
Reconciling observation quality, so the scoped stream's suspension is
subsumed by that larger surfaced condition. Transitions in any
non-Live phase surface only through the fallback family's final
state — an explicit bounded-per-event fidelity loss accepted in
exchange for eliminating the buffering apparatus. While Live, each payload has two decoupled
effects. Closure cancellation always fires — the existing
re-observation cancellation, which is how the fourth topology-closure
rescue trigger (reachable only for snapshot-absent panes) survives;
its honest bound is the fallback family's today: a cancellation on
in-flight stale evidence defers that closure until the next
observation gap repopulates the pending set — an existing exposure
class, not a new one. Status application is gated by the enrichment
target set — the most recent snapshot's panes plus panes created or
moved in since (`pane_moved` is a creation path for a new public pane
id, not only `pane_created`), excluding a grace-retained remnant — so
a stale in-flight event can never reset a removed pane's stale
execution or write a row for it; for a member pane (pane records carry
no status; status lives on executions, and one pane can host several),
the payload expands into one event per matching non-terminal execution
whose state differs from the event's status, each with a fresh
identity and the stored receipt instants — original timestamps
survive, and the differs-filter, not any dedup, prevents duplicate
rows. A stale execution on a member pane stays eligible: staleness
also arises from a snapshot momentarily reporting no agent, and
restoring exactly that transition is this stream's purpose — only the
target-set gate guards resurrection. The per-transition restoration
below holds exactly while the primary is Live AND the enrichment
subscription is healthy; during a convergence episode it is suspended
with only the fallback family covering, and likewise during an
enrichment outage — which can outlast any single retry attempt —
until the per-stream health recovers.

The existing derivation of agent status from `pane.updated` payloads remains
in place as the fallback path; the scoped subscription restores the richer
event stream: transition-timestamp fidelity, the ledger row and activity
item per transition, rate accounting, and the fourth topology-closure rescue
trigger. Subscription management is covered by wire-level tests (real
listener, outbound payload assertions) in the pattern the hotfix
established, plus convergence coverage proving the fallback path is
unchanged when scoped subscriptions are unavailable.

### A3: warning throttle and rejection-variant regression

The `herdr_subscription_failed` warning currently repeats on every retry
(roughly twenty lines per second during an outage). It becomes
once-per-transition: one warning when subscription health degrades, one
notice on recovery, with the retry loop otherwise silent. A regression test
covers the server-rejection error variant (the existing test exercises only
the I/O variant), asserting both the warning code and the fallback behavior.

### A4: recorded reliability prescriptions

Two prescriptions recorded in the Increment 5 ledger are applied verbatim
unless the code has drifted, in which case the task returns to design:

1. F3: the performance-watch BrokenPipe path constructs
   `HeaderInputs::default()` first, so a broken pipe during header
   composition cannot poison later output.
2. OwnerLock: the lock guard gains an explicit unlock on drop
   (`flock_unlock`) and a regression test pinning the fork-before-exec
   ordering — the fourth catalogued fragility sub-class — so a forked child
   can never inherit a held lock across exec.

## Phase B: NonD4 amendment, harness hardening, and measurement

### B1: the amendment

Evidence, from the preserved attempt-20260826 burst results: the failing
predicate is `PerformanceDegradation count == 0`; observed counts were 6,
10, 0, 5, and 14 across the five trials; every flagged sample carries the
single reason `EventsOneSecond` with an observed one-second count of exactly
101 against the envelope of 100; flagged samples show event lag between
2.4 and 5.3 microseconds; all other burst predicates pass.

Static analysis of both implicated components shows neither is defective.
The runtime tracker counts admissions in a half-open window
`(now − width, now]` over actual monotonic admission instants — no boundary
double-count, no falsification. The burst workload schedules 1,000 events at
a uniform 10 ms cadence — exactly the 100-events-per-second envelope — and a
half-open one-second window over an ideal 10 ms grid contains exactly 100
points at every alignment. The observed 101 therefore comes from real
scheduling jitter: one admission landing late and a later one landing early
places 101 actual instants inside one sliding second, and the
strictly-greater-than classifier truthfully reports it. This explains every
observation: the flagged value is always exactly 101, lag stays in
microseconds, one trial shows zero flags, and only burst is affected
(sustained runs at 20 events per second and twice-target at 40 — neither
approaches the one-second envelope).

The root cause is an acceptance-predicate mis-specification. The Increment 5
plan's admission predicate explicitly tolerates exactly one scheduling
quantum at a bucket boundary; the degradation-count acceptance for Final
sustained and burst carries no matching tolerance, so an at-envelope
workload fails on the same jitter the admission side deliberately absorbs.

The amendment changes acceptance only; product code, the classifier, and the
TUI health surface stay truthful and unchanged. At the Final stage, for
sustained and burst, a degradation sample is tolerated if and only if all of
the following hold:

1. its reason set is exactly `{EventsOneSecond}`;
2. its observed one-second count is exactly 101 (envelope plus one,
   mirroring the admission side's one-quantum tolerance);
3. it carries no event-lag breach, and the trial has no `EventLag` reason.

The acceptance predicate becomes: the count of non-tolerated degradation
samples equals zero. A sample with a count of 102 or more, any other reason,
or a lag breach is non-tolerated and remains a failure. The
`MissingDegradation` requirement for twice-target is unchanged. The tolerance definition lives in the shared typed validator used
by both CI tests and the final validator, with boundary tests for all four
edges (101 tolerated; 102 rejected; other reason rejected; lag breach
rejected).

### Closing path

The Section 15 re-derivation and the D4 checkpoint classifier are
deterministic functions of stored results. Closing therefore does not burn a
measurement attempt:

1. Amend the validator and the acceptance text (this document plus a ledger
   amendment referencing the frozen Increment 5 plan; the frozen plan file
   itself is not rewritten).
2. Re-run the re-derivation and the D4 checkpoint over the preserved
   attempt-20260826 results (subject `e86e0ef`), binding BOTH required
   result roots (the preserved baseline attempt 20260822 and the preserved
   final attempt 20260826). The stored-outcome reader validates a recorded
   `Failed` document by re-deriving its failure set, so under the amended
   validator the preserved burst document (recorded `failed`, every flagged
   sample the tolerated shape) would re-derive empty and be rejected as an
   invalid artifact. The SHARED stored-outcome reader therefore gains an
   explicit legacy-reclassification mode, enabled by an environment flag
   set only by this closing path and honored uniformly by every consumer
   that reads stored outcomes in the closing process. The closing
   entrypoints' closed-environment gate enforces exact key-set equality,
   so it gains an OPTIONAL-KEY contract: the flag key may be absent
   (today's behavior, byte-identical) or present with exactly the value
   `1`; any other value, and any other unexpected key, is still
   rejected. The flag is read once at each entrypoint and passed down as
   a parameter — the reader itself never consults the environment.
   A reclassified outcome is presented as a pass with its recorded
   failure reasons cleared on the in-memory representation (the stored
   bytes are never touched); without that clearing the pass-side
   validator would reject the non-empty recorded reasons. The mode's
   consumers are the Section 15 re-derivation, the report's own
   self-validation re-read (which re-derives a fresh document and
   requires equality with the stored report), and the D4 checkpoint
   classifier — all of them read through the one shared reader. The
   rule: a stored `Failed` document is reclassified at read time as an
   amended pass if and only if its recorded `failure_reasons` equal
   exactly `["supported_load_degradation"]` AND the amended predicate
   derives no failure from its evidence; any other divergence between
   recorded and derived outcomes remains fail-closed. Because reclassification happens
   at read time in one shared place, the regenerated report contains no
   burst failure and the self-validation equality gate holds without
   special-casing. The reclassification itself is recorded in a SIDECAR
   document beside the regenerated report and in the increment ledger —
   never inside the report, which would break its deterministic
   re-derivation equality. The sidecar has a concrete contract: when the
   flag is set and at least one reclassification occurred, each
   entrypoint additionally writes `<output>.reclassification.json`
   beside its declared output — schema version 1, listing per scenario
   the recorded failure reasons and the rule identifier
   `amended_legacy_v1` — and writes no sidecar otherwise; the closing
   step consolidates both sidecars into the increment record. Expected
   decision: `no_miss_d4_not_authorized`. Both regenerated documents and
   the sidecars are preserved with their hashes recorded. This closes
   `AmendmentsRequired[non_d4]`.
3. Independently, run one full confirmation measurement (next fresh attempt
   identifier 20260827) at the post-Phase-A/B head under the same
   fail-closed protocol as Increment 5, comparing against the Increment 5
   baseline (attempt 20260822). This is Increment 6's own regression
   evidence — required because Phase A changes the collector runtime path —
   and is expected to pass all seven scenarios under the amended predicate.
   Attempt identifiers burn on use whether or not the run completes.

### B2: harness hardening batch

The six backlog items are implemented from their verbatim ledger
prescriptions, extracted into the implementation plan at planning time:
controller-binary validation hardening (role-name and path-order checks with
their covering rows), the CurDir/canonicality joint validator change, the
recorder socket-shape predicate, the outer-trap hardening set, the fixture
TOCTOU class, and comparator exposure. The two documented permissivenesses
(the bounded trailing-EventLag admission and the `last_pre_origin` expect)
are re-adjudicated: each is either closed with a cheap hardening or
re-documented with its reasoning carried into this increment's record. The
guard-ordering nicety is applied. All of B2 lands before the confirmation
measurement so the measuring harness is the hardened one.

## Phase C: the emit calling side

### C1: `herdr-top emit --from-hook`

The `emit` subcommand gains an adapter mode: `herdr-top emit --from-hook
<provider>` with `<provider>` one of `claude-code` or `codex`. It reads one
hook payload (JSON) from standard input, maps it to at most a few section
7.3 envelope events, and delivers them through the existing emit pipeline —
session resolution, runtime-sentinel validation, wire protocol, and
best-effort failure policy all unchanged. The adapter adds no protocol
surface and no new event kinds.

Both CLIs present the same hook payload shape (`hook_event_name`,
`session_id`, `transcript_path`, event-specific fields) and the same
registration schema, so one adapter serves both providers with a small
per-provider field table.

Behavioral rules:

1. The adapter never blocks or fails an agent session. Malformed input, an
   unmapped event, an unresolvable session, and every delivery failure exit
   with status 0 after at most a warning. `--strict` remains available but
   reference registrations never use it. In adapter mode nothing is written
   to standard output — both CLIs parse hook stdout as structured hook
   output, and Codex validates it against a closed schema and marks the
   hook invalid on any unrecognized JSON — so all adapter diagnostics go to
   standard error.
2. Event identifiers are collision-resistant per invocation with a
   deterministic prefix:
   `hook:<provider>:<native-session-id>:<hook-event>:<entity>:<transition>:<emitted-at-ms>:<nonce>`.
   The transition segment is mandatory on every event (two events mapped
   from one hook invocation must never share an identifier), and the nonce
   is a per-invocation random component, because millisecond timestamps
   alone can collide across two invocations in the same millisecond. The
   random nonce makes accidental collision negligible rather than
   impossible; the correctness backstop is the semantic-no-op idempotency
   below, not identifier uniqueness.
   A fully deterministic identifier would be wrong: a session resume fires
   `SessionStart` again, and its `task_started` must reactivate the run,
   but an identifier already present in the deduplication ledger returns
   `duplicate` and applies nothing. Idempotency against hook re-fires comes
   from section 7.3's semantic no-ops instead — `task_started` on a running
   run, a re-stated `dispatch` parent, and a repeated terminal of the same
   kind are all accepted no-ops. The reserved `prov:` prefix is never
   produced.
3. Task Run identifiers are deterministic: the session run is
   `hook:<provider>:<native-session-id>`; a subagent run appends
   `:agent:<agent-id>`; a task run appends `:task:<task-id>`.
4. Session-run events carry the provider and native session identifier as
   binding identity, producing the durable K2-to-K1 merge of design section
   5.4 with the run herdr already observes. Subagent and task runs carry no
   binding identity; they are semantic children connected by dispatch
   edges, exactly the design's model for Controller-declared sub-runs.
   Terminal identity (pane addresses) is never used for binding.
5. The session run never receives a terminal event. Sessions resume
   routinely, and section 7.3 rejects `task_started` on a terminal run as
   `stale_event`; liveness for the session run flows from observed
   executions (retirement to `ended_unknown`, reactivation on resume).
   `SessionEnd` maps to nothing.
6. Labels the adapter forwards are exactly two: `agent_type` (a
   structural type name) and `task_subject`. Design amendment, decided by
   the owner with the display consequences reviewed: `task_subject` is
   agent-authored task text supplied at task creation, which the base
   design's section 7.2 allowlist sentence would exclude; this increment
   amends the design with a carve-out EXACTLY as wide as the decision —
   Controller-supplied `label` values may carry the agent-authored task
   SUBJECT (the task's one-line name) and nothing else agent-generated,
   passing the existing label sanitization (256-byte cap,
   control-character escaping, UTF-8-safe truncation) — via an ADR under
   `docs/adr/` plus matching amendments to BOTH normative sentences: the
   section 7.2 allowlist sentence and the section 14 bullet ("never
   prompts, responses, tool arguments or results", design line 600),
   landed and reviewed together with the adapter task so the design
   never contradicts itself. Everything else stays
   excluded: the adapter's payload struct has no `prompt`,
   `description`, `task_description`, `teammate_name`, `team_name`,
   `last_assistant_message`, or tool-input fields, and a sentinel test
   pins that a payload carrying all of those alongside the structural
   fields maps to envelopes whose serialized JSON contains none of their
   values.
7. Within one hook invocation the mapped envelopes are delivered strictly
   in order and delivery stops at the first failure: delivering a child's
   `task_started` after its `dispatch` failed would create a permanently
   unlinked run. A terminal event arriving from a later hook while an
   earlier one was lost still lands as a forward-referenced terminal run,
   flagged in diagnostics — a documented degraded outcome, not corruption.
   Because hooks run in parallel, a terminal hook can occasionally deliver
   before its start hook; the start's `task_started` is then rejected as
   `stale_event`. The adapter therefore treats a `rejected` response whose
   reason is `stale_event` as a benign outcome: logged to standard error,
   not a delivery failure, not a stop condition, and not a `--strict`
   failure. Every other rejection and every unresolved delivery remains a
   failure under the stop-on-first-failure and strict rules.

Event mapping:

Both CLIs use identical PascalCase hook event names (verified against the
installed Codex binary's embedded hook schemas, whose `hook_event_name`
constants are `SessionStart`, `SessionEnd`, `SubagentStart`,
`SubagentStop`, and whose `subagent-start.command.input` schema REQUIRES
`agent_id` and `agent_type` — so the Codex subagent rows are
unconditional, not subject to a runtime probe):

| Hook event (both CLIs) | Emitted events | Subject |
| --- | --- | --- |
| `SessionStart` | `task_started` with binding identity | session run |
| `SubagentStart` | `dispatch` (parent: session run) then `task_started`, label = agent type | subagent run |
| `SubagentStop` | `complete` | subagent run |
| `TaskCreated` (Claude Code only) | `dispatch` (parent: session run) then `progress` (creates the run queued via the forward-reference rule), label = task subject | task run |
| `TaskCompleted` (Claude Code only) | `complete` | task run |
| `SessionEnd`, all others | nothing | — |

### C2: setup documentation

A repository document (location fixed in the implementation plan, referenced
from the in-product `?` help and first-launch notice text if those name a
destination) covers: installing the standalone CLI (design section 12.3);
registration snippets for `~/.claude/settings.json` and `~/.codex/hooks.json`
running `herdr-top emit --from-hook <provider>` on the mapped events; the
Codex hook-trust acceptance step; the fact that hooks run in parallel and
coexist with herdr's own integration hooks; behavior outside managed panes
(no resolvable session means a warning and a clean exit); and explicit
`herdr-top emit depends_on` guidance for dependency edges, which no hook
surface can derive.

### C3: live acceptance

On the real environment: rebuild the release binary at the integrated head
and reinstall it to the standalone CLI location, verifying the installed
binary's version and digest before any registration (hooks registered
against a stale installed binary would silently exercise the old CLI);
then register the hooks for both CLIs, run a session
with at least one subagent dispatch under each provider, and verify in the
TUI that the session runs bind to their observed executions (no `unbound`
diagnostic for them), subagent runs appear as children with correct
lifecycle, and `doctor` reports a healthy integration surface. This
acceptance is observational; it complements, never replaces, the unit and
wire-level suites.

## Error handling and invalid evidence

Phase B keeps Increment 5's fail-closed measurement discipline: an
incomplete confirmation run is reported as incomplete, never as a pass, and
burns its attempt identifier. Re-derivation over preserved results must
reproduce the recorded input hashes before the amended validator runs;
any mismatch stops the closing path. The adapter treats every failure as
non-fatal for the hosting agent session while preserving emit's existing
diagnostics; nothing in the adapter can make a hook exit non-zero in
reference registrations.

## Verification strategy

1. Test-driven development for every behavior change, red first.
2. Phase A: doctor version-parser unit tests over integer, date-form, and
   malformed inputs in both renderers, with fixtures mirroring the live
   response shape; wire-level subscription tests plus pane-set-following
   tests for A2 with unchanged-fallback convergence coverage; throttle and
   rejection-variant regressions for A3; prescription regressions for A4.
3. Phase B: four-edge boundary tests for the amended tolerance; harness
   tests for each B2 item from its ledger prescription; deterministic
   re-derivation with input-hash verification; the confirmation measurement
   under the full fail-closed protocol.
4. Phase C: adapter unit tests covering the whole mapping table for both
   providers, deterministic identifier derivation, the session-terminal
   prohibition, malformed-payload no-op behavior, and label privacy;
   wire-level tests proving delivered envelopes; the live acceptance of C3.
5. Review checklist for every task review in this increment, carried from
   the Increment 5 ledger: (1) no positive assertions over race-dependent
   transient state — wait on the asserted channel; (2) no positive age or
   staleness thresholds over independently scheduled samplers; (3) never
   join two independently published watches — wait on the channel the
   assertion reads; (4) resource guards must be exception- and fork-safe
   (the OwnerLock class).
6. The final whole-change review runs exactly once over the full integrated
   diff and explicitly verifies that Phase C touched no runtime hot path
   (collector, reducer, writer, TUI render), so the Phase B confirmation
   measurement remains representative of the released head.

## Completion criteria

1. All Phase A behaviors landed with their tests; `doctor` reports a valid
   integration version against live herdr.
2. The amended validator is in place with its boundary tests; the
   re-derivation and D4 checkpoint over preserved attempt-20260826 results
   return `no_miss_d4_not_authorized`; the amendment is recorded closed.
3. The confirmation measurement (attempt 20260827) at the post-Phase-A/B
   head completes all seven scenarios and passes under the amended
   predicate, with the comparison against the Increment 5 baseline recorded.
4. B2's six backlog items, the permissiveness re-adjudications, and the
   guard-ordering change are landed before the confirmation measurement.
5. The adapter, registrations, and setup document are landed with the full
   mapping-table test suite; C3 live acceptance passes for both providers.
6. The final whole-change review is complete, including the hot-path
   neutrality verification for Phase C.

## Implementation boundary and process constraints

- Serial integration, one task at a time, on a dedicated increment branch;
  implementation work runs in linked worktrees, never the primary checkout.
- Planning-time reality checks (external facts the implementation plan must
  verify against the live environment before freezing task briefs):
  (1) the exact live herdr version-query response form;
  (2) herdr's subscription-removal semantics for scoped subscriptions;
  (3) the Codex `SubagentStart`/`SubagentStop` payload identity fields
  (RESOLVED at planning time: the installed binary's embedded schemas
  require `agent_id` and `agent_type`);
  (4) how herdr wires its Claude Code integration hook, to prove
  coexistence; (5) whether the in-product help text names a setup-document
  destination that C2 must satisfy.
- Measurement outputs remain outside the repository and every linked
  worktree; preserved measurement roots are never cleaned; attempt
  identifiers burn on use, next fresh 20260827.
- The frozen Increment 5 plan file is never edited; amendments are recorded
  in this increment's documents and ledger.
- No push, publication, or release action without an explicit user request.
