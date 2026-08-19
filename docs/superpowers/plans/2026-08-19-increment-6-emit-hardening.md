# Increment 6: Emit Integration and Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land every Increment 5 carry obligation (doctor version parsing,
per-pane subscriptions, warning throttle, F3, OwnerLock, six harness backlog
items), close the NonD4 amendment with an evidence-backed acceptance change
plus one confirmation measurement, and ship the Rust-native hook adapter and
setup documentation that light up deep monitoring for Claude Code and Codex.

**Architecture:** Three serially integrated phases on branch
`agent/increment6-emit-hardening`. Phase A hardens product code, Phase B
amends and re-closes the performance acceptance and hardens the measurement
harness before one confirmation measurement, Phase C adds a pure mapping
module plus a CLI adapter mode on the existing `emit` pipeline and the
operator documentation. No protocol change, no server-side change, no D4
incrementalization.

**Tech Stack:** Rust (existing crate layout), tokio, clap, serde; bash only
inside the existing reference-profile runner.

**Spec:** `docs/superpowers/specs/2026-08-19-increment-6-emit-hardening-design.md`
(commit `464464c` includes the event-identifier amendment). The frozen
Increment 5 plan `docs/superpowers/plans/2026-08-12-increment-5-reliability-performance.md`
is referenced read-only and is never edited.

## Global Constraints

- Serial integration: exactly one task integrates at a time into
  `agent/increment6-emit-hardening`; implementation work runs in dedicated
  linked worktrees under `.worktrees/`, never in the primary checkout.
- The user-owned untracked `mise.toml` in the primary checkout is never
  read, edited, or committed.
- No push, PR, release, or other publication action without an explicit
  user request. No merge of anything by workers; workers never commit.
- D4 stays the full recomputation; nothing here implements D4
  incrementalization (checkpoint decision: not authorized).
- herdr is an external product: client-side changes only.
- Measurement outputs stay outside the repository and every linked worktree.
  Preserved roots under the Increment 5 research workspace are never
  cleaned. Attempt identifiers burn on use; next fresh is 20260827.
- Full verification for product tasks: `cargo fmt --check`,
  `cargo clippy --all-targets`, `cargo test --all-targets`, plus the
  feature-gated harness suite where a task touches it
  (`cargo test --features workload-harness --test workload_harness`; the
  feature name `workload-harness` is verified against `Cargo.toml`).
- Review checklist for every task review (from the Increment 5 ledger):
  (1) never assert positively over race-dependent transient state — wait on
  the asserted channel; (2) never place positive age or staleness thresholds
  over independently scheduled samplers; (3) never join two independently
  published watches — wait on the channel the assertion reads; (4) resource
  guards must be exception- and fork-safe.

## Facts verified against the live environment during planning

Implementers rely on these without re-deriving them; the plan reviewer
re-derives them with the listed commands.

1. Live herdr 0.8.0 `server.agent_manifests` returns date-form
   `active_version` values: claude `2026.08.12.1`, codex `2026.08.09.1`
   (agent-detection manifests). Re-derive: connect to `$HERDR_SOCKET_PATH`
   and send `{"id":"x","method":"server.agent_manifests","params":{}}` as
   one line over the unix socket.
2. Scoped subscriptions: one `events.subscribe` call per connection (a
   second call on the same connection resets the connection); one call may
   carry multiple `pane.agent_status_changed` entries with distinct
   `pane_id` values plus unscoped types and answers
   `subscription_started`; including a nonexistent pane answers error code
   `pane_not_found` with the pane id in the message and rejects the whole
   request; the herdr binary exposes no `events.unsubscribe` method
   (`strings ~/.local/bin/herdr | grep -o 'events\.[a-z_]*'`). On the
   subscribe ERROR path the server decorates the response id — observed
   `<request-id>:sub:<index>:<token>` — while the success path echoes the
   id verbatim; the wire client's strict id-equality check therefore turns
   every subscribe error into `WireError::UnexpectedResponse`
   (`src/herdr/wire.rs:236-246`), and Task 2 must teach it to recognize
   decorated error ids before `pane_not_found` is reachable.
3. Claude Code hooks (official docs, code.claude.com/docs/en/hooks.md):
   events include `SessionStart`, `SessionEnd`, `SubagentStart`
   (`agent_id`, `agent_type`), `SubagentStop`, `TaskCreated`/`TaskCompleted`
   (`task_id`, `task_subject`); `session_id` and `hook_event_name` are
   always present; hooks inherit the process environment; headless `-p`
   sessions fire the same events; exit 0 never blocks.
4. Codex CLI (self-updating; 0.148.0 at plan revision — re-derive the
   version at execution time with `codex --version`): `hooks` feature
   stable and enabled; registration in `~/.codex/hooks.json` with the same
   schema as Claude Code; hook event names are PascalCase and IDENTICAL to
   Claude Code's — the binary's embedded hook schemas carry
   `hook_event_name` constants `SessionStart`, `SessionEnd`,
   `SubagentStart`, `SubagentStop`, `Stop` and NO snake_case variants
   (re-derive: `strings -a ~/.local/bin/codex | grep -A1 '"hook_event_name"' | grep '"const"'`).
   The `subagent-start.command.input` schema REQUIRES `agent_id`,
   `agent_type`, `session_id`, `transcript_path` (among others) and
   `subagent-stop.command.input` requires `agent_id`,
   `agent_transcript_path`, `agent_type` — so the Codex subagent mapping
   rows are unconditional. Codex parses hook stdout as JSON against a
   CLOSED output schema (`session-start.command.output` etc.,
   `additionalProperties: false`) and marks the hook invalid on any
   unrecognized JSON ("hook returned invalid session start JSON output"),
   so the adapter must write nothing to stdout. Hook trust
   (`trusted_hash`) applies.
5. The in-product notice and help text (`src/tui/view.rs`, `notice_lines`
   and `help_lines`) name no setup-document destination, so Task 13 may
   choose `docs/guides/controller-emit-setup.md` freely and no TUI text
   changes are needed in Phase C.
6. herdr manages its Claude Code hook registration itself (the herdr binary
   contains an `integration::claude_settings` module with removal
   commands). On this host the managed script exists at
   `~/.claude/hooks/herdr-agent-state.sh` (v7) but `~/.claude/settings.json`
   currently contains no reference to it; Task 14 reports this observation
   to the user — it is a user-environment concern, not a product change.

---

## Phase A: product hardening

### Task 1: doctor accepts date-form integration versions

**Files:**
- Modify: `src/diagnostics/remote.rs` (function `assess_official_integration`
  near line 114, helper `normalize_integer` near line 616, module tests)
- Test: same file, `mod tests`; plus `src/doctor.rs` `mod tests` renderer
  coverage (the spec requires integer, date-form, and malformed inputs
  exercised through BOTH the human and JSON renderers — assert the
  `compatibility.integrations` check row renders the verbatim date-form
  `active_version` and the correct code in each renderer for all three
  input classes)

**Interfaces:**
- Consumes: `AgentManifestStatus` (unchanged), `OfficialIntegrationStatus`,
  `OfficialIntegrationUnavailableReason` (unchanged variants).
- Produces: unchanged public shapes. `OfficialIntegrationAssessment.active_version`
  now carries the verbatim date-form string when one is reported.

Behavior (spec section "A1"): an all-digit token without leading zero is a
legacy integer and compares against the minimum exactly as today; a
dot-separated sequence of two or more nonempty all-digit components (leading
zeros allowed inside components, e.g. `2026.08.12.1`) is a date-era version,
belongs to a newer versioning era than any legacy integer minimum, and is
`Compatible` with the verbatim string preserved; anything else remains
`InvalidActiveVersion`. `src/doctor.rs` needs no change: `active_version`
is already an `Option<String>` pass-through in both renderers.

- [ ] **Step 1: Write failing tests** in `src/diagnostics/remote.rs` `mod tests`:

```rust
#[test]
fn date_form_active_version_is_compatible_verbatim() {
    let status = manifest_status(vec![manifest("claude", Some("2026.08.12.1"))]);
    let a = assess_official_integration(&status, Provider::Claude);
    assert_eq!(a.status, OfficialIntegrationStatus::Compatible);
    assert_eq!(a.active_version.as_deref(), Some("2026.08.12.1"));
}

#[test]
fn date_form_with_two_components_is_compatible() {
    let status = manifest_status(vec![manifest("codex", Some("2026.8"))]);
    let a = assess_official_integration(&status, Provider::Codex);
    assert_eq!(a.status, OfficialIntegrationStatus::Compatible);
}

#[test]
fn legacy_integer_still_compares_against_minimum() {
    let below = manifest_status(vec![manifest("claude", Some("5"))]);
    assert!(matches!(
        assess_official_integration(&below, Provider::Claude).status,
        OfficialIntegrationStatus::Unavailable {
            reason: OfficialIntegrationUnavailableReason::BelowMinimum
        }
    ));
    let ok = manifest_status(vec![manifest("claude", Some("7"))]);
    assert_eq!(
        assess_official_integration(&ok, Provider::Claude).status,
        OfficialIntegrationStatus::Compatible
    );
}

#[test]
fn malformed_versions_stay_invalid() {
    for bad in ["", "v7", "7a", "07", "2026..1", ".1", "1.", "2026.08.x"] {
        let status = manifest_status(vec![manifest("claude", Some(bad))]);
        assert!(matches!(
            assess_official_integration(&status, Provider::Claude).status,
            OfficialIntegrationStatus::Unavailable {
                reason: OfficialIntegrationUnavailableReason::InvalidActiveVersion
            }
        ), "{bad:?} must stay invalid");
    }
}
```

Reuse or add local helpers `manifest_status`/`manifest` following the
existing test fixtures in that module.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p herdr-top date_form -- --nocapture` (and the other new
names). Expected: FAIL — date-form currently returns `InvalidActiveVersion`.

- [ ] **Step 3: Implement**

```rust
enum ActiveVersionForm {
    LegacyInteger(String),
    DateEra(String),
}

fn classify_active_version(value: &str) -> Option<ActiveVersionForm> {
    if let Some(integer) = normalize_integer(value) {
        return Some(ActiveVersionForm::LegacyInteger(integer));
    }
    let components: Vec<&str> = value.split('.').collect();
    let date_era = components.len() >= 2
        && components
            .iter()
            .all(|c| !c.is_empty() && c.bytes().all(|b| b.is_ascii_digit()));
    date_era.then(|| ActiveVersionForm::DateEra(value.to_owned()))
}
```

In `assess_official_integration`, replace the `normalize_integer` gate with
`classify_active_version`; `DateEra(v)` returns
`assessment(Some(v), OfficialIntegrationStatus::Compatible)`;
`LegacyInteger(v)` keeps the existing `compare_numeric_component` path.
Keep `normalize_integer` itself unchanged (other callers unaffected).

- [ ] **Step 4: Run tests to verify pass** — the new tests plus the whole
`remote.rs` module suite: `cargo test -p herdr-top diagnostics::remote`.
Expected: PASS, no existing test regresses.

- [ ] **Step 5: Full verification** — fmt, clippy, `cargo test --all-targets`.

- [ ] **Step 6: Commit** `fix(doctor): accept date-era integration versions`

### Task 2: per-pane agent-status subscriptions

**Files:**
- Modify: `src/herdr/collector.rs` (the primary subscribe site near line
  1462 stays untouched in behavior; new enrichment-connection management;
  pane-set tracking; module tests including
  `unscoped_subscriptions_omit_pane_scoped_agent_status_event` near line
  5827)
- Modify: `src/herdr/wire.rs` (response-id matching near lines 236-246:
  recognize decorated error ids)
- Modify (only if the `Subscription` type lacks an optional `pane_id`
  field): `src/herdr/types.rs`
- Test: `src/herdr/collector.rs` module tests; `src/herdr/wire.rs` module
  tests; `tests/convergence.rs`.

**Interfaces:**
- Produces: `fn enrichment_subscriptions(pane_ids: &BTreeSet<String>) -> Vec<Subscription>`
  building one `pane.agent_status_changed` entry per pane id (the primary
  connection keeps today's argumentless `subscriptions()` and its 15
  unscoped types, byte-identical); a wire-client change so an error
  response whose id equals the expected id OR starts with the expected id
  followed by `:` is accepted as this request's error and surfaces as
  `WireError::Server { code, message }` (Task 3 consumes this for its
  server-rejection regression).
- Consumes: reality fact 2 (one subscribe per connection; whole-request
  rejection on `pane_not_found`; decorated error ids).

Behavior — the scoped subscriptions ride a DEDICATED secondary
"enrichment" connection (spec section "A2"); the primary subscription
connection, its startup order, and its gap/convergence semantics are
untouched. Design section 9.2 forbids treating pane churn as an
observation gap: a new subscription connection's first snapshot is a gap
reconciliation that retires every pre-gap execution
(`src/herdr/collector.rs:1614-1620`), so the pane-scoped stream must
never ride the primary connection's lifecycle.

1. After the primary connection converges, the collector derives the
   target pane set from the converged model and keeps it current from
   `pane_created` / pane-removal handling (note: creation flows through
   `pane_created`; `pane_updated` silently ignores unknown panes —
   `src/herdr/collector.rs:3287-3300`).
2. On a pane-set change, the collector opens a NEW enrichment connection
   subscribed to `enrichment_subscriptions(target_set)` and closes the old
   one after the new subscription is acknowledged. Changes arriving while
   a swap is in flight coalesce into the next single swap (no per-event
   storm). Enrichment-connection replacement records no collector gap,
   retires no execution, and performs no resnapshot: the primary stream
   and the `pane.updated` fallback stay continuous throughout, and a
   transition missed during a swap is bounded fidelity loss the fallback
   family already covers.
3. If the enrichment subscribe fails with `WireError::Server` code
   `pane_not_found`, the collector parses the pane id from the message,
   prunes exactly that id from the target set, and retries; other errors
   warn (through Task 3's edge-triggered path) and retry with the existing
   backoff. Enrichment failures never degrade the primary connection's
   observation quality.
4. Events arriving on the enrichment connection feed the existing
   `pane_agent_status_changed` handler (near line 3360; its `pane_id`
   read is at lines 3368-3369), restoring timestamp fidelity, ledger rows
   and activity items, rate accounting, and the fourth topology-closure
   rescue trigger. The `pane.updated`-derived fallback stays byte-level
   untouched.
5. The stale reality comment at lines 3935-3937 is replaced by the new
   contract description.

- [ ] **Step 1: Write failing wire-id test** in `src/herdr/wire.rs`
`mod tests`: an error envelope with id `req:sub:1:probe` for expected id
`req` decodes to `WireError::Server { code: "pane_not_found", .. }`; an
error with an unrelated id (`other:sub:1:probe`) still fails as
`UnexpectedResponse`; a success envelope with a decorated id remains
`UnexpectedResponse` (success echoes verbatim — reality fact 2).

- [ ] **Step 2: Write failing enrichment-subscribe test**: real
`UnixListener` plays the enrichment endpoint; after primary convergence
with two panes the collector opens the enrichment connection and the test
asserts its subscribe payload is exactly one `pane.agent_status_changed`
entry per pane with matching `pane_id` and nothing else; the primary
connection's subscribe payload stays the unchanged unscoped list (replace
the omission test at line 5827 with this pair of assertions).

- [ ] **Step 3: Write failing prune-retry test**: the enrichment listener
answers the first subscribe with the DECORATED error frame
`{"id":"<request-id>:sub:1:probe","error":{"code":"pane_not_found","message":"pane w9:p99 not found"}}`
(the mandatory `id` mirrors live herdr; an id-less error does not even
deserialize as `ResponseEnvelope`), and the second subscribe succeeds;
assert the second request omits exactly `w9:p99`.

- [ ] **Step 4: Write failing pane-created swap test**: after enrichment
convergence with one pane, deliver a `pane_created` event for a new pane
on the primary stream; assert the enrichment listener observes a second
subscribe containing both panes, and assert NO collector gap is recorded
and no execution retires (query the persisted ops / reducer state the
existing gap tests use).

- [ ] **Step 5: Write failing fallback-unavailable convergence test**: with
the enrichment endpoint permanently refusing connections, drive the
existing convergence scenario and assert agent-status still converges via
the `pane.updated` fallback with observation quality unchanged (this is
the spec's "fallback path unchanged when scoped subscriptions are
unavailable" proof).

- [ ] **Step 6: Run the five tests** — expected FAIL.

- [ ] **Step 7: Implement** the wire-id recognition,
`enrichment_subscriptions`, the enrichment-connection manager with
coalesced swaps, and prune-on-`pane_not_found`. Do not touch the reducer,
writer, or the primary connection's startup order.

- [ ] **Step 8: Run collector suite + wire suite + `tests/convergence.rs`**
— all green, fallback tests unchanged.

- [ ] **Step 9: Full verification, then commit**
`feat(collector): subscribe pane agent status per pane`

### Task 3: subscription-warning throttle and rejection-variant regression

**Files:**
- Modify: `src/herdr/collector.rs` (warn site near line 1487)
- Test: `src/herdr/collector.rs` module tests

Behavior: the `herdr_subscription_failed` warning becomes edge-triggered —
one warning when subscription health transitions healthy-to-failed
(carrying the error), one recovery notice on failed-to-healthy; retries in
a steady failed state stay silent. A regression covers the server-rejection
variant (listener answers the subscribe request with a wire error result,
as distinct from an I/O failure): assert the warning code appears exactly
once across three consecutive failed retries and that the retry flow
continues.

- [ ] **Step 1: Write failing once-per-transition test** (three failed
retries, count warning emissions via the existing test logging capture used
by the PR #5 disconnect test; assert exactly one).
- [ ] **Step 2: Write failing server-rejection regression** (wire error
answer with the decorated id shape from Task 2 Step 3, surfacing as
`WireError::Server`; assert `herdr_subscription_failed` with the error
`Display` and continued retry).
- [ ] **Step 2b: Write failing recovery-notice test** (failed retries, then
a successful subscribe; assert exactly one recovery notice is logged on
the failed-to-healthy transition — the spec requires the notice, not only
the warning).
- [ ] **Step 3: Verify all three fail.**
- [ ] **Step 4: Implement the health-edge state in the collector's
subscribe/retry loop.**
- [ ] **Step 5: Suite green; full verification; commit**
`fix(collector): warn once per subscription health transition`

### Task 4: F3 — performance watch BrokenPipe

**Files:**
- Modify: `src/tui/app.rs` (`HeaderInputs::default` at line 170; the
  performance watch read at line 615, mirroring the model watch's
  BrokenPipe arm at lines 609-613)
- Test: `src/tui/app.rs` module tests

Verbatim carried prescription (Increment 5 ledger): "fix Default to retain
a live sender → mirror the model watch's BrokenPipe at :615 → regression
test (model alive, performance dropped → BrokenPipe)". Today
`HeaderInputs::default()` constructs and drops both senders, so a bare
error mapping would fail every default-backed `App` on first refresh —
the Default change must land together with the watch change.

Constraint on the "retain a live sender" mechanism: `HeaderInputs` is a
public four-field struct constructed with exhaustive literals in 21 places
across five files (`git grep -n 'HeaderInputs {' -- src tests`: main.rs 1,
app.rs 10, view.rs 3, coverage_harness.rs 3, workload_harness.rs 4), so
adding a field — public or private — breaks either those literals or
external construction. Instead, `Default` keeps its senders alive without
changing the struct shape: `std::mem::forget(coverage_sender)` and
`std::mem::forget(performance_sender)` inside `default()` leak the two
senders deliberately, so their channels never close. The leak is one
channel pair per `Default` call, and `Default` is a fixture/test
constructor (production `main.rs:288` builds the literal directly); a
comment in `default()` states the contract.

- [ ] **Step 1: Write the failing regression**: construct an `App` whose
model watch is alive while the performance watch's sender is dropped;
drive one refresh; assert the refresh returns the same BrokenPipe-classified
error the model watch path produces (not a panic, not a silent stale
header).
- [ ] **Step 2: Verify it fails** (current code ignores the closed
performance channel or panics — record which).
- [ ] **Step 3: Implement**: `HeaderInputs::default()` retains its live
senders via the documented `mem::forget` leak (struct shape unchanged);
the performance-watch read at line 615 mirrors the model watch's
BrokenPipe arm at lines 609-613.
- [ ] **Step 4: App suite green; full verification; commit**
`fix(tui): surface performance watch closure as broken pipe`

### Task 5: OwnerLock unlock-on-drop and exec-safety regression

**Files:**
- Modify: `src/lockfile.rs` (`OwnerLock` near line 27; `flock_unlock` near
  line 343 already exists)
- Test: `src/lockfile.rs` module tests

Verbatim carried prescription: "OwnerLock explicit-Drop flock_unlock +
fork-before-exec regression (4th fragility sub-class)".

RED premise, corrected: `OwnerLock` is `{ _file: File }`
(`src/lockfile.rs:25-29`), and dropping it closes the descriptor, which
already releases `flock` when no other process shares the open file
description. The same-process drop/re-acquire case therefore ALREADY
passes and is pinned, not RED. The genuine defect explicit unlock fixes is
the inherited-descriptor case: `flock` belongs to the open file
description, so a child that inherited the descriptor keeps the lock alive
after the parent's close — while `LOCK_UN` releases the lock on the
description even though the child still holds a descriptor to it.

- [ ] **Step 1: Write the pinned drop test** (expected to pass already;
records the baseline): acquire the lock, drop the `OwnerLock`, re-acquire
on the same path within the same process; assert success.
- [ ] **Step 2: Write the FAILING inherited-descriptor regression**:
acquire the lock, then spawn a long-lived child (`/bin/sh -c 'sleep 5'`)
with the lock descriptor deliberately inherited into it (clear
`FD_CLOEXEC` on a dup of the lock fd passed to the child, simulating the
fork-before-exec window), drop the parent's guard, and assert re-acquire
SUCCEEDS while the child still runs. Without explicit `LOCK_UN` this
fails — the child's descriptor keeps the lock — which is the RED.
- [ ] **Step 2b: Write the exec-safety pin**: spawn a child through plain
`std::process::Command` while holding the lock, drop the guard, assert
re-acquire succeeds — pinning that std's default close-on-exec keeps
ordinary children from inheriting the lock descriptor.
- [ ] **Step 3: Verify Step 2 is RED and Steps 1/2b are recorded** (pass
expected; any surprise stops the task for re-diagnosis).
- [ ] **Step 4: Implement** `impl Drop for OwnerLock` calling
`flock_unlock` (best-effort; ignore the error, the descriptor close remains
the backstop).
- [ ] **Step 5: Suite green; full verification; commit**
`fix(lockfile): unlock owner lock on drop and pin exec safety`

## Phase B: NonD4 amendment, harness batch, measurements

### Task 6: amended degradation tolerance in the shared validator

**Files:**
- Modify: `tests/common/workload.rs` (the degradation COUNT site at line
  2419 — `degraded_samples += usize::from(!sample.reasons.is_empty());` —
  whose failure insertion is at line 2452; the Section 15 observed-count
  consumer at line 7867 inside `section15_predicate_rows` (declared at
  lines 7673-7675), whose signature gains the stage; and the
  stored-outcome reader's legacy-reclassification mode at lines
  1074-1081)
- Test: `tests/workload_harness.rs`

**Interfaces:**
- Produces: `pub fn tolerated_boundary_degradation(stage: MeasurementStageV1, scenario: ScenarioV1, sample: &PerformanceSampleEvidenceV1, trial_has_event_lag_reason: bool) -> bool`
  in `tests/common/workload.rs` (`PerformanceSampleEvidenceV1` is defined
  there at line 400 with `reasons: Vec<PerformanceReasonV1>`), used by
  every degradation-count site; and
  `section15_predicate_rows(stage: MeasurementStageV1, trial: &TrialResultV1)`
  — the stage parameter is a declared signature change, plumbed from each
  call site's scenario document (`document.measurement_stage`), because
  `HarnessTrialV1` carries `scenario` but no stage (lines 676-706).
- Produces (Task 8 consumes): a legacy-reclassification mode on the
  stored-outcome reader, enabled only by the re-derivation entrypoint: a
  stored `Failed` document whose recorded `failure_reasons` equal exactly
  `["supported_load_degradation"]` and whose amended re-derivation yields
  an EMPTY failure set is reclassified as an amended pass, with the
  reclassification recorded in the regenerated report; every other
  recorded/derived divergence stays `InvalidArtifact` (fail-closed), and
  the mode is off everywhere else.

Amended predicate (spec section "B1: the amendment"): the tolerance
applies only when `stage == Final` and `scenario` is `Sustained` or
`Burst` — the function returns false for every other stage/scenario, so
Baseline-stage observed counts are unchanged; a sample is tolerated iff
its reason set is exactly `{EventsOneSecond}`, its `events_one_second`
equals 101 (envelope plus one), and the trial carries no `EventLag`
reason anywhere. The count threshold stays `== 0`. `MissingDegradation`
for TwiceTarget is unchanged. Both derivation sites route through the
single shared function — no reimplementation, mirroring the plan rule
that the runner never reimplements harness predicates.

- [ ] **Step 1: Write the four failing boundary tests** in
`tests/workload_harness.rs` against the shared function and one end-to-end
validator case per edge:
  1. one flagged sample, reasons exactly `[EventsOneSecond]`, count 101, no
     trial EventLag → validator passes (degradation predicate observed 0);
  2. same but count 102 → fails with `SupportedLoadDegradation`;
  3. same but reasons `[EventsOneSecond, LivePanes]` → fails;
  4. same but another sample in the trial carries `[EventLag]` → fails.
- [ ] **Step 2: Verify the RED gate precisely**: edge 1 must be RED today
(the validator still fails the tolerated shape); edges 2-4 assert against
the shared function and the non-tolerated count directly, so they are RED
today because neither exists yet. Record the four observed failures.
- [ ] **Step 3: Implement the shared function, the stage plumbing, and
route both sites.**
- [ ] **Step 4: Write the three failing reclassification tests**: (a)
stored `Failed` burst document with recorded
`["supported_load_degradation"]` whose every flagged sample is the
tolerated shape → reclassified amended pass, recorded in the output; (b)
same recorded reasons but one flagged sample at 102 → recorded and
derived failure sets agree, so the document stays a valid `Failed` and is
never reclassified; (c) recorded reasons
`["supported_load_degradation"]` with a derived nonempty DIFFERENT set →
`InvalidArtifact` (fail-closed). Then implement the mode and verify green.
- [ ] **Step 5: Harness suite green** (feature-gated suite plus doctests).
- [ ] **Step 6: Full verification; commit**
`fix(perf): tolerate one-quantum boundary degradation at final acceptance`

### Task 7: measurement-harness hardening batch

One batch task of enumerated, independently testable sub-changes; the
review covers every listed file and sub-change. Verbatim prescriptions are
quoted from the Increment 5 ledger; where the ledger names line anchors,
re-locate by symbol at execution time.

**Files:**
- Modify: `tests/support/reference_profile_controller.rs` (sub-change 1)
- Modify: `tests/common/workload.rs` (sub-change 2)
- Modify: `scripts/run-reference-profile.sh` (sub-changes 3, 4, 5)
- Modify: `src/operator.rs` (sub-change 6: the private `compare_activity`
  at line 252 gains a feature-gated export; the sibling copy at
  `src/tui/projection.rs:502` is out of scope and untouched)
- Test: `tests/workload_harness.rs` (all sub-changes; sub-change 6 deletes
  the byte-identical mirror `compare_reference_activity` at line 9152 and
  keeps its ordering assertion pointed at the exported production
  comparator)

Sub-changes:

1. Controller-binary hardening — "one gated change to
   reference_profile_controller.rs bundling prefix-before-canonicalize
   reorder + basename==role + four covering rows": reorder the workstation
   prefix guard before canonicalization so the rejection reason is
   assertable; bind executable basename to the role name; add four rows —
   wrong-but-plausible substitution (`/usr/bin/true`), relative path,
   absent path, non-executable path.
2. CurDir/canonicality joint validator — "the CurDir arm is unreachable on
   absolute paths (Rust Components normalizes '.') so /tmp/./… absolute
   non-canonical passes; a joint change is the correct future form": change
   BOTH path validators (the trial-control-root validator added at commit
   `45e5086` and its production mirror near `workload.rs:2723-2731`) to
   reject absolute-but-non-canonical inputs, with a `/tmp/./x` covering row
   on each.
3. Recorder socket-shape predicate — "the recorder does not apply the
   socket shape check (~3-line shared-predicate follow-up)": the
   runner-control recorder applies the same shared socket-shape predicate
   the validators use.
4. Outer-trap hardening set — the four round-2 Minors fixed TOGETHER
   (items 2 and 4 interact): one-command identity window;
   group-publication windows; publisher temp blocking rmdir; unsignalled
   orchestration wait bounded by the scenario deadline.
   Test contract (one named harness test per race, driven through the
   source-mode runner fixtures the errexit fix established):
   `outer_trap_identity_window_is_single_command` (interrupt inside the
   former multi-command identity window; assert the trap records a
   coherent identity), `outer_trap_group_publication_is_atomic`
   (interrupt during group publication; assert no partial group is
   observable), `publisher_temp_never_blocks_rmdir` (plant a leftover
   publisher temp; assert cleanup succeeds), and
   `orchestration_wait_is_deadline_bounded` (never-signalling child;
   assert the wait returns at the scenario deadline instead of hanging).
5. Fixture-write TOCTOU class — harden `publish_runner_test_outcome`,
   `publish_trial_status`, `validate_fixture_output_path`, and the trap
   marker with the offered two-token guards (`! -L` and `! -p`). Test
   contract: for each of the four sites, one named test plants a symlink
   and one plants a FIFO at the destination and asserts the write is
   refused with the guard's diagnostic (RED first: today's behavior
   follows or overwrites, failing the new expectation).
6. Comparator exposure — "exposing the production comparator under the
   feature would remove the duplication": give `src/operator.rs`'s private
   `compare_activity` (line 252) a feature-gated
   (`workload-harness`) public export, delete the byte-identical mirror
   `compare_reference_activity` (`tests/workload_harness.rs:9152`), and
   keep the ordering assertion that made mirror drift fail closed, now
   exercising the production comparator directly.

Permissiveness re-adjudication (recorded decisions, not silent carries):
the bounded trailing-EventLag admission and the `last_pre_origin` expect
are each re-examined against the post-Task-6 validator; each is either
closed by a cheap hardening within this task or re-documented with its
reasoning in this increment's ledger entry. The guard-ordering nicety
(assert `helper_decl < START < END < guard_decl` in the marker guard) is
applied.

- [ ] **Step 1** For each sub-change: write its failing test (or RED
  demonstration for shell changes via the existing runner test fixtures),
  verify RED, implement, verify GREEN. Sub-changes commit together as one
  reviewed batch after all six are green.
- [ ] **Step 2** Record the two permissiveness adjudications in the task
  report for the increment ledger.
- [ ] **Step 3** Full verification including `bash -n
  scripts/run-reference-profile.sh` and the feature-gated harness suite.
- [ ] **Step 4: Commit** `test(perf): harden reference harness backlog set`

### Task 8: close the NonD4 amendment by re-derivation (operational, Controller-executed)

No repository files change in this task. Inputs: the preserved Increment 5
final measurement root `final-e86e0efdd463-attempt-20260826` in the
Increment 5 research workspace (~370 MB, preserved; report SHA-256
`48d18f12…`, checkpoint SHA-256 `bb6cc481…`).

- [ ] **Step 1** Verify preserved-input identity: recompute SHA-256 of the
  existing `section15-rederivation-v1.json` and `d4-checkpoint-v1.json`
  and match the recorded hashes; verify the seven per-scenario
  `result-v1.json` sha16 values against the Increment 5 ledger entry. Any
  mismatch stops this task.
- [ ] **Step 2** Build the harness at the post-Task-7 head and run the
  Section 15 re-derivation entrypoint and the D4 checkpoint classifier
  (the same native Controller-launch bootstrap and
  `classify_d4_checkpoint_from_results` entrypoints the Increment 5 plan
  defined, with their HEAD/clean predicates), binding BOTH required roots —
  `HERDR_PERF_REDERIVE_BASELINE_RESULTS_ROOT` to the preserved baseline
  attempt 20260822 and `HERDR_PERF_REDERIVE_FINAL_RESULTS_ROOT` to the
  preserved final attempt 20260826 (`tests/common/workload.rs:5111-5115`) —
  with Task 6's legacy-reclassification mode enabled for the stored-outcome
  read (without it the preserved burst document, recorded `failed` with
  every flagged sample the tolerated shape, re-derives an empty failure
  set and is rejected as `InvalidArtifact` by the stored-outcome validator
  at `tests/common/workload.rs:1074-1081`). Regenerated documents are
  written into the Increment 6 research workspace — never into the
  repository or the preserved roots. This refines the spec's "preserved
  alongside the originals": the preserved roots stay immutable, and the
  workspace records the original and regenerated document hashes side by
  side, including the reclassification record.
- [ ] **Step 3** Expected: decision `no_miss_d4_not_authorized` from both.
  Record regenerated document hashes and the closure in the Increment 6
  ledger. Any other decision stops the increment for user consultation.

### Task 9: confirmation measurement, attempt 20260827 (operational, Controller-executed)

Subject: the post-Task-8 integrated head (Phases A and B landed; Phase C
not yet started). Protocol: identical to the Increment 5 final-stage
measurement (Step-1 suite gate, frozen identities, seven-scenario drive
under the selected reference profile, typed validation, Section 15
re-derivation, D4 checkpoint), with the amended validator from Task 6.

- [ ] **Step 1** Step-1 suite gate at the subject head (fmt, clippy,
  all-targets, doc, no-run build) — all zero.
- [ ] **Step 2** Rebuild and record identities (controller binary, runner
  script, bash, drive script substitution counts) exactly as the
  Increment 5 protocol prescribes.
- [ ] **Step 3** Drive all seven scenarios; output root
  `final-<subject12>-attempt-20260827` outside every worktree, beside the
  preserved Increment 5 roots. The run is fail-closed: an incomplete drive
  is reported incomplete, never as a pass, and 20260827 burns either way.
- [ ] **Step 4** Expected: seven schema-valid documents, all PASS under the
  amended predicate; Section 15 re-derivation and D4 checkpoint agree on
  `no_miss_d4_not_authorized`; baseline deltas against attempt 20260822
  recorded in the ledger. A burst failure that is NOT the tolerated
  boundary shape stops the increment for user consultation.

## Phase C: emit calling side

### Task 10: RESOLVED AT PLANNING TIME — no work

The question this task existed to answer (does the Codex
`SubagentStart`/`SubagentStop` payload carry a usable subagent identity?)
is answered by the installed binary's embedded hook schemas:
`subagent-start.command.input` REQUIRES `agent_id` and `agent_type`, and
`subagent-stop.command.input` requires `agent_id`,
`agent_transcript_path`, `agent_type` (planning fact 4; re-derive with
`strings -a ~/.local/bin/codex | grep -B45 '"title": "subagent-start.command.input"' | grep -A12 '"required"'`).
A runtime probe would also have been unexecutable cleanly: `codex exec
--ignore-user-config` still uses `CODEX_HOME` auth and hook trust is
path-bound, so "no subagent observed" could not be distinguished from an
auth or trust failure. The Codex mapping rows in Tasks 11-13 are
therefore UNCONDITIONAL. Task numbering is retained to keep references
stable; no steps execute.

### Task 11: hook mapping module

**Files:**
- Create: `src/hook_adapter.rs`
- Modify: `src/lib.rs` (module registration)
- Test: `src/hook_adapter.rs` module tests

**Interfaces:**
- Consumes: `ControllerEnvelope`, defined at
  `src/herdr/controller.rs:102` (the wire struct `run_emit` builds today in
  `src/main.rs:169-184`); import as
  `crate::herdr::controller::ControllerEnvelope`.
- Produces:

```rust
pub enum HookProvider { ClaudeCode, Codex }

#[derive(serde::Deserialize)]
pub struct HookPayload {
    pub hook_event_name: String,
    pub session_id: String,
    #[serde(default)] pub source: Option<String>,
    #[serde(default)] pub agent_id: Option<String>,
    #[serde(default)] pub agent_type: Option<String>,
    #[serde(default)] pub task_id: Option<String>,
    #[serde(default)] pub task_subject: Option<String>,
}
// Unknown fields are ignored by default — hooks evolve; never deny_unknown_fields.

pub fn map_hook_payload(
    provider: HookProvider,
    payload: &HookPayload,
    emitted_at_ms: i64,
    invocation_nonce: u64,
) -> Vec<ControllerEnvelope>
```

The nonce is one random `u64` generated once per CLI invocation by the
caller (Task 12) and rendered as fixed-width lowercase hex in every event
id; passing it as a parameter keeps the mapping function deterministic and
unit-testable.

Mapping (spec table; both CLIs use the SAME PascalCase event names —
planning fact 4). Every envelope: `schema_version` 1, `source`
`hook:claude-code`|`hook:codex`, **wire `provider` `claude`|`codex`** —
the envelope decoder `optional_provider`
(`src/herdr/controller.rs:1055-1061`) accepts ONLY `claude`/`codex` and
rejects anything else as `invalid`, so `claude-code` is the CLI selector
and `source` token, never the wire provider value — `emitted_at_ms` as
passed, and `event_id`
`hook:<provider-selector>:<session_id>:<hook_event_name>:<entity>:<transition>:<emitted_at_ms>:<nonce-hex>`
(the transition segment is MANDATORY on every event so two events from
one invocation can never collide, and the nonce guards same-millisecond
invocations):

1. `SessionStart` → one `task_started`; `task_run_id`
   `hook:<provider-selector>:<session_id>`; `native_session_id` =
   session_id; entity `session`, transition `started`.
2. `SubagentStart` (with `agent_id`) → `dispatch`
   (subject `…:agent:<agent_id>`, `parent_task_run_id` = session run,
   entity `<agent_id>`, transition `dispatch`) then `task_started` (same
   subject, `label` = `agent_type` when present, transition `started`).
   Missing `agent_id` → empty vec (cannot occur per the Codex schema and
   the Claude docs; guarded anyway).
3. `SubagentStop` (with `agent_id`) → one `complete`
   (transition `complete`).
4. `TaskCreated` (Claude Code only; with `task_id`) → `dispatch` (subject
   `…:task:<task_id>`, transition `dispatch`) then `progress` with
   `label` = `task_subject`, transition `created`.
5. `TaskCompleted` (with `task_id`) → one `complete` (transition
   `complete`).
6. `SessionEnd` and every other event name → empty vec.
7. No field from the payload other than the listed structural fields is
   ever read; `native_session_id` appears only on the SessionStart
   envelope. Prompt-, response-, or tool-derived fields (`prompt`,
   `description`, `last_assistant_message`, tool inputs) are never
   deserialized: `HookPayload` simply has no such fields, and the privacy
   sentinel test pins that the struct never gains them.

- [ ] **Step 1: Write failing unit tests**: one per table row and per
guard —
session-start shape (all envelope fields asserted exactly, including wire
`provider` `"claude"` for the `claude-code` selector);
subagent start pair (dispatch parent, ids, label, distinct event_ids);
subagent stop; task created pair (distinct `dispatch`/`created`
transitions in the ids); task completed; SessionEnd → empty;
unknown event → empty; missing agent_id/task_id → empty; event ids carry
the `emitted_at_ms` and nonce suffixes and never start with `prov:`; two
invocations with the SAME timestamp but different nonces produce
different event ids for the same hook; the privacy sentinel — a payload
containing `prompt`, `description`, and `last_assistant_message` fields
maps to envelopes whose serialized JSON contains none of those values.
- [ ] **Step 2: Verify RED** (module absent).
- [ ] **Step 3: Implement the module exactly per the table.**
- [ ] **Step 4: GREEN; full verification; commit**
`feat(emit): map controller hook payloads to envelope events`

### Task 12: `emit --from-hook` CLI integration

**Files:**
- Modify: `src/main.rs` (`EmitArgs` near line 60, `run_emit` near line 153)
- Test: `tests/controller.rs` (wire-level delivery test using the existing
  real-socket fixtures and the PR #5 `shutdown_client_write` helper)

**Interfaces:**
- Consumes: `hook_adapter::{HookProvider, HookPayload, map_hook_payload}`;
  `controller::emit_to_endpoint`; existing session resolution and
  `rendezvous::resolve_controller_socket`.
- Produces: CLI surface `herdr-top emit --from-hook <claude-code|codex>`.

clap shape: add `#[arg(long, value_enum)] from_hook: Option<HookProviderArg>`
to `EmitArgs`, where `HookProviderArg` is a two-variant clap `ValueEnum`
(`claude-code`, `codex`) with a `From<HookProviderArg> for
hook_adapter::HookProvider` conversion; every currently required manual argument (`event_id`,
`emitted_at_ms`, `source`, `event_type`, `task_run_id`) becomes
`required_unless_present = "from_hook"` and `conflicts_with = "from_hook"`.
The optional manual arguments (`parent_task_run_id`, `depends_on_id`,
`label`, `reason`, `progress`, `provider`, `native_session_id`,
`terminal_id`) gain `conflicts_with = "from_hook"`; `strict` and
`schema_version` do NOT — `--strict` must remain usable in adapter mode
(the strict exit rule below depends on it), and `schema_version` keeps its
default. The manual path is byte-for-byte unchanged when `--from-hook` is
absent.

Adapter path behavior: read standard input to end with a 1 MiB cap
(oversize → warn on stderr, exit 0); parse `HookPayload` (failure → warn
on stderr, exit 0); resolve session and endpoint once through the existing
`run_emit` calls (unavailable → existing `emit_unavailable`, which exits 0
without `--strict`); `emitted_at_ms` from the system wall clock and one
random `u64` invocation nonce; deliver the mapped envelopes strictly in
order via `emit_to_endpoint`, and STOP at the first delivery failure —
delivering a child's `task_started` after its `dispatch` failed would
create a permanently unlinked run (spec C1 rule 7). In adapter mode
NOTHING is written to stdout — Codex parses hook stdout against a closed
output schema and marks the hook invalid on unrecognized JSON (planning
fact 4), so every diagnostic and every per-envelope outcome line goes to
stderr. Exit 0 unless `--strict` and any envelope failed or was skipped.

- [ ] **Step 1: Write the failing wire test** in `tests/controller.rs`: a
real listener plays collector, the test invokes the adapter code path (the
extracted `run_emit_from_hook` function called with a `SubagentStart`
payload string), and asserts two envelopes arrive in order (`dispatch`
then `task_started`) with the exact ids and fields from the Task 11 table
(wire `provider` `"claude"`), each acknowledged `accepted` — and that the
function produced ZERO bytes on stdout.
- [ ] **Step 1b: Write the failing stop-on-failure test**: the listener
rejects the first envelope (`rejected`/`invalid`); assert the second
envelope is never sent and, under `--strict` semantics, the outcome is
failure while the non-strict outcome is success with a stderr diagnostic.
- [ ] **Step 1c: Write the failing malformed-input tests**: non-JSON
stdin, JSON missing `session_id`, and stdin exceeding the 1 MiB cap each
produce exit-0 semantics, zero deliveries, zero stdout bytes, and a
stderr warning.
- [ ] **Step 2: Write the failing CLI-surface tests** (unit, in
`src/main.rs` tests): `--from-hook claude-code` parses with no manual
arguments; `--from-hook claude-code --strict` parses; manual invocation
without `--event-id` still errors; combining `--from-hook` with
`--event-id` errors.
- [ ] **Step 3: RED, implement, GREEN.**
- [ ] **Step 4** Confirm the breadcrumb non-publication test
(`i4_local_doctor_emit_and_nonplugin_launch_never_publish_breadcrumb`,
`src/main.rs:531`) covers the new path or extend it to.
- [ ] **Step 5: Full verification; commit**
`feat(emit): add hook adapter mode to the emit CLI`

### Task 13: setup documentation

**Files:**
- Create: `docs/guides/controller-emit-setup.md`

Content contract (self-contained for a third-party operator): what the
emit integration provides (execution tree and task lifecycle; explicitly
NOT dependency edges); installing the standalone CLI per design section
12.3; registration presented as an APPEND/MERGE procedure, never a
replacement — the document states in prose that each entry below is
APPENDED to the corresponding event's existing array in the operator's
live file (both files routinely already contain handlers: herdr's own
integration, permission guards), shows this valid-JSON fragment of the
entries to add (the target-path comment lives in prose above the fence,
not inside the JSON):

```json
{
  "hooks": {
    "SessionStart": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}],
    "SubagentStart": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}],
    "SubagentStop": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}],
    "TaskCreated": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}],
    "TaskCompleted": [{"hooks": [{"type": "command",
      "command": "herdr-top emit --from-hook claude-code"}]}]
  }
}
```

and prescribes the merge procedure: back up the file, append the new
entries to each event's array (creating the event key only when absent),
then verify with `jq` that every pre-existing handler is still present
and the file parses. The `~/.codex/hooks.json` fragment is the same shape
for `SessionStart`, `SubagentStart`, `SubagentStop` (PascalCase — the
Codex event names are identical to Claude Code's) with
`--from-hook codex`; the Codex hook-trust acceptance step follows the
registration. The document also covers: hooks run in parallel and coexist
with herdr's own integration hooks; the delivery semantics under partial
failure and hook-parallelism races (spec C1 rule 7 — stop-on-first-
failure, forward-referenced terminals flagged in diagnostics, stale
`task_started` after a terminal is harmless); behavior outside managed
panes (no resolvable session → warn, exit 0, deliver nothing);
dependency-edge guidance with one complete manual
`herdr-top emit --event-type depends_on` example including every required
flag; troubleshooting via `herdr-top doctor`.

- [ ] **Step 1** Write the document; verify every snippet against the
implemented CLI by executing the commands with a throwaway session name.
- [ ] **Step 2: Commit** `docs: add controller emit setup guide`

### Task 14: live acceptance (operational, Controller-executed with the user)

- [ ] **Step 0** Rebuild the release binary at the integrated head and
reinstall it to `~/.local/bin/herdr-top` (the repository's established
procedure), then verify `herdr-top --version` and a digest match against
the fresh build, and prove the adapter surface exists:
`printf '{"hook_event_name":"SessionStart","session_id":"probe"}' | herdr-top emit --from-hook claude-code`
must exit 0 (the currently installed 0.1.0 binary exits 2 on this —
registering hooks against a stale binary would silently exercise the old
CLI).
- [ ] **Step 1** With user confirmation, register the Task 13 hooks in the
live `~/.claude/settings.json` and `~/.codex/hooks.json` using Task 13's
merge procedure: back up both files, APPEND the new entries to each
event's existing array (never replace an array — both files already carry
herdr-integration and guard hooks), verify with `jq` that every
pre-existing handler survived and both files parse, and report the exact
diff (these are live-authoritative user files: Controller edits them
directly, per the established rule for worker-uneditable live paths).
- [ ] **Step 2** Run one Claude Code session and one Codex session inside
managed panes, each dispatching at least one subagent; observe the TUI:
session runs bound (no `unbound` diagnostic), subagent children with
correct lifecycle, `doctor` healthy including the Task 1 version check.
- [ ] **Step 3** Report the herdr Claude-integration observation (planning
fact 6: managed script present, `settings.json` entry absent) to the user
with the suggestion to re-run `herdr integration install claude` if herdr's
own reporting is wanted; this plan takes no action on it.
- [ ] **Step 4** Record acceptance evidence (pane reads, doctor output) in
the increment ledger. If Codex shipped session-level only (Task 10), the
recorded gap is part of the acceptance statement, not a failure.

## Integration, final review, and completion

- Every task integrates serially: worker implements in its worktree,
  Controller verifies the diff and test evidence, per-task review runs per
  the Controller's routing rules, Controller commits to the increment
  branch, and only then does the next task dispatch.
- After Task 14: one final whole-change review over
  `main..agent/increment6-emit-hardening`, exactly once, with the
  already-reviewed per-task ranges listed in the dispatch prompt. The
  review explicitly verifies: (a) Phase C touched no runtime hot path
  (collector, reducer, store/writer, TUI render — `src/hook_adapter.rs`,
  `src/main.rs` CLI surface, and docs only), so the Task 9 measurement
  remains representative; (b) the four checklist items from Global
  Constraints across the whole diff; (c) the frozen Increment 5 plan is
  untouched.
- Publication (push, PR) happens only on explicit user request, following
  the established publication workflow.
- Completion = the spec's six completion criteria, all evidenced in the
  increment ledger.
