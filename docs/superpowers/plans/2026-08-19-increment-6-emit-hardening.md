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
   (`strings ~/.local/bin/herdr | grep -o 'events\.[a-z_]*'`).
3. Claude Code hooks (official docs, code.claude.com/docs/en/hooks.md):
   events include `SessionStart`, `SessionEnd`, `SubagentStart`
   (`agent_id`, `agent_type`), `SubagentStop`, `TaskCreated`/`TaskCompleted`
   (`task_id`, `task_subject`); `session_id` and `hook_event_name` are
   always present; hooks inherit the process environment; headless `-p`
   sessions fire the same events; exit 0 never blocks.
4. Codex CLI 0.147.0: `hooks` feature stable and enabled; registration in
   `~/.codex/hooks.json` with the same schema as Claude Code; event set
   includes `session_start`, `session_end`, `subagent_start`,
   `subagent_stop`; hook stdin carries `hook_event_name`, `session_id`,
   `transcript_path` (proven by herdr's own installed integration script,
   which parses exactly those fields); hook trust (`trusted_hash`) applies.
   The `subagent_start`/`subagent_stop` payload identity fields are NOT yet
   verified — Task 10 resolves this empirically before the adapter tasks.
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
- Test: same file, `mod tests`

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
- Modify: `src/herdr/collector.rs` (`subscriptions()` near line 3919, the
  subscribe site near line 1462, pane-set tracking in the event loop,
  module tests including
  `unscoped_subscriptions_omit_pane_scoped_agent_status_event` near line
  5827)
- Modify (only if the `Subscription` type lacks an optional `pane_id`
  field): `src/herdr/types.rs`
- Test: `src/herdr/collector.rs` module tests; `tests/convergence.rs`
  stays green unchanged.

**Interfaces:**
- Produces: `fn subscriptions_for_panes(pane_ids: &BTreeSet<String>) -> Vec<Subscription>`
  replacing the argumentless `subscriptions()`; base list identical to
  today's 15 unscoped types plus one
  `pane.agent_status_changed` entry per pane id.
- Consumes: reality facts 2 (one subscribe per connection; whole-request
  rejection on `pane_not_found`).

Behavior:

1. The collector derives the target pane set from the converged snapshot
   and keeps it current from `pane.updated` and pane-removal events.
2. When the target set differs from the subscribed set, the collector
   triggers its existing reconnect-and-resubscribe convergence cycle (the
   same machinery as overflow recovery: fresh subscription connection with
   the full new list, then the bounded in-place resnapshot). No attempt is
   made to add subscriptions on the live connection (reality fact 2). Pane
   churn is human-scale, so a resubscribe per change is acceptable;
   coalescing multiple changes arriving during one cycle into the next
   single resubscribe is required (no per-event storm).
3. If subscribe fails with `pane_not_found`, the collector prunes exactly
   the named pane id from the target set and retries within the existing
   retry flow; other errors keep the existing warn-and-retry behavior.
4. The `pane.updated`-derived agent-status fallback path stays byte-level
   untouched; scoped events restore timestamp fidelity, ledger rows and
   activity items, rate accounting, and the fourth topology-closure rescue
   trigger through the existing `pane_agent_status_changed` handler
   (near line 3360), which already consumes `pane_id` (line 4544).
5. The stale reality comment at lines 3935-3937 is replaced by the new
   contract description.

- [ ] **Step 1: Write failing wire-level test**: real `UnixListener` accepts
the subscribe request and the test asserts the outbound payload contains
one `pane.agent_status_changed` entry per snapshot pane with matching
`pane_id`, alongside the unchanged unscoped list. Model on the existing
wire-level pattern of `unscoped_subscriptions_omit_pane_scoped_agent_status_event`,
which this test replaces (delete the omission assertion; it is now false by
design).

- [ ] **Step 2: Write failing prune-retry test**: listener answers the first
subscribe with `{"error":{"code":"pane_not_found","message":"pane w9:p99 not found"}}`
and the second subscribe succeeds; assert the second request omits exactly
`w9:p99` and the collector reaches its subscribed state.

- [ ] **Step 3: Write failing pane-set-change test**: after convergence with
one pane, deliver a `pane.updated` for a new pane; assert the collector
opens a new subscription connection whose list includes both panes (listener
observes a second subscribe request).

- [ ] **Step 4: Run the three tests** — expected FAIL (current code never
scopes).

- [ ] **Step 5: Implement** `subscriptions_for_panes`, target-set tracking,
resubscribe trigger, and prune-on-`pane_not_found`, reusing the existing
reconnect machinery. Keep the change inside the collector; do not touch the
reducer or writer.

- [ ] **Step 6: Run collector suite + `tests/convergence.rs`** — all green,
fallback tests unchanged.

- [ ] **Step 7: Full verification, then commit**
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
answer; assert `herdr_subscription_failed` with the error `Display` and
continued retry).
- [ ] **Step 3: Verify both fail.**
- [ ] **Step 4: Implement the health-edge state in the collector's
subscribe/retry loop.**
- [ ] **Step 5: Suite green; full verification; commit**
`fix(collector): warn once per subscription health transition`

### Task 4: F3 — performance watch BrokenPipe

**Files:**
- Modify: `src/tui/app.rs` (`HeaderInputs::default` near line 174; the
  performance watch refresh; mirror the model watch's BrokenPipe handling
  near line 615)
- Test: `src/tui/app.rs` module tests

Verbatim carried prescription (Increment 5 ledger): "fix Default to retain
a live sender → mirror the model watch's BrokenPipe at :615 → regression
test (model alive, performance dropped → BrokenPipe)". Today
`HeaderInputs::default()` constructs and drops its performance sender, so a
bare error mapping would fail every default-backed `App` on first refresh —
the Default change must land together with the watch change.

- [ ] **Step 1: Write the failing regression**: construct an `App` whose
model watch is alive while the performance watch's sender is dropped;
drive one refresh; assert the refresh returns the same BrokenPipe-classified
error the model watch path produces (not a panic, not a silent stale
header).
- [ ] **Step 2: Verify it fails** (current code ignores the closed
performance channel or panics — record which).
- [ ] **Step 3: Implement**: `HeaderInputs::default()` retains a live
sender (store the sender beside the receiver in the struct); the
performance-watch read mirrors the model watch's BrokenPipe arm.
- [ ] **Step 4: App suite green; full verification; commit**
`fix(tui): surface performance watch closure as broken pipe`

### Task 5: OwnerLock unlock-on-drop and exec-safety regression

**Files:**
- Modify: `src/lockfile.rs` (`OwnerLock` near line 27; `flock_unlock` near
  line 343 already exists)
- Test: `src/lockfile.rs` module tests

Verbatim carried prescription: "OwnerLock explicit-Drop flock_unlock +
fork-before-exec regression (4th fragility sub-class)".

- [ ] **Step 1: Write failing drop test**: acquire the lock, drop the
`OwnerLock`, then re-acquire on the same path within the same process using
a second `File` handle; assert immediate success (today release relies on
process exit closing the descriptor; within one process, re-acquisition
after drop must succeed only with an explicit unlock).
- [ ] **Step 2: Write the exec-safety regression**: while holding the lock,
spawn a real child via `std::process::Command` (a `sleep`-equivalent
helper binary or `/bin/sh -c 'exec sleep 5'`), then drop the parent's
guard and assert re-acquisition succeeds while the child still runs —
pinning that the lock descriptor is not inherited across exec
(close-on-exec) and that unlock-on-drop is not defeated by a forked child.
- [ ] **Step 3: Verify both fail or record which already passes** (the
CLOEXEC half may already hold via std; a currently passing half stays as a
pinned regression).
- [ ] **Step 4: Implement** `impl Drop for OwnerLock` calling
`flock_unlock` (best-effort; ignore the error, the descriptor close remains
the backstop).
- [ ] **Step 5: Suite green; full verification; commit**
`fix(lockfile): unlock owner lock on drop and pin exec safety`

## Phase B: NonD4 amendment, harness batch, measurements

### Task 6: amended degradation tolerance in the shared validator

**Files:**
- Modify: `tests/common/workload.rs` (both degradation-count derivation
  sites — the scenario-result derivation near the
  `FailureReasonV1::SupportedLoadDegradation` insertions at lines 2452 and
  2906, and the Section 15 observed-count consumer near line 7867 noted in
  the Increment 5 ledger; plus the reason mapping near line 4928)
- Test: `tests/workload_harness.rs`

**Interfaces:**
- Produces: `pub fn tolerated_boundary_degradation(sample: &PerformanceSampleEvidenceV1, trial_has_event_lag_reason: bool) -> bool`
  in `tests/common/workload.rs` (`PerformanceSampleEvidenceV1` is defined
  there at line 400 with `reasons: Vec<PerformanceReasonV1>`), used by
  every degradation-count site.

Amended predicate (spec section "B1: the amendment"): at Final stage for
Sustained and Burst, the acceptance counts only non-tolerated degradation
samples; a sample is tolerated iff its reason set is exactly
`{EventsOneSecond}`, its `events_one_second` equals 101 (envelope plus
one), and the trial carries no `EventLag` reason anywhere. The count
threshold stays `== 0`. `MissingDegradation` for TwiceTarget is unchanged.
Both derivation sites and the Section 15 consumer route through the single
shared function — no reimplementation, mirroring the plan rule that the
runner never reimplements harness predicates.

- [ ] **Step 1: Write the four failing boundary tests** in
`tests/workload_harness.rs` against the shared function and one end-to-end
validator case per edge:
  1. one flagged sample, reasons exactly `[EventsOneSecond]`, count 101, no
     trial EventLag → validator passes (degradation predicate observed 0);
  2. same but count 102 → fails with `SupportedLoadDegradation`;
  3. same but reasons `[EventsOneSecond, LivePanes]` → fails;
  4. same but another sample in the trial carries `[EventLag]` → fails.
- [ ] **Step 2: Verify all four fail** (edge 1 fails today; 2-4 pass today
for the wrong reason — assert on the tolerated-count field so all four are
discriminating; RED must show edge 1 red).
- [ ] **Step 3: Implement the shared function and route all sites.**
- [ ] **Step 4: Harness suite green** (feature-gated suite plus doctests).
- [ ] **Step 5: Full verification; commit**
`fix(perf): tolerate one-quantum boundary degradation at final acceptance`

### Task 7: measurement-harness hardening batch

One batch task of enumerated, independently testable sub-changes; the
review covers every listed file and sub-change. Verbatim prescriptions are
quoted from the Increment 5 ledger; where the ledger names line anchors,
re-locate by symbol at execution time.

**Files:**
- Modify: `tests/support/reference_profile_controller.rs` (sub-change 1)
- Modify: `tests/common/workload.rs` (sub-changes 2, 6)
- Modify: `scripts/run-reference-profile.sh` (sub-changes 3, 4, 5)
- Test: `tests/workload_harness.rs` (all sub-changes)

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
5. Fixture-write TOCTOU class — harden `publish_runner_test_outcome`,
   `publish_trial_status`, `validate_fixture_output_path`, and the trap
   marker with the offered two-token guards (`! -L` and `! -p`).
6. Comparator exposure — "exposing the production comparator under the
   feature would remove the duplication": export `compare_activity` under
   the harness feature and delete the byte-identical startup mirror,
   keeping the ordering assertion that made mirror drift fail closed.

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
  defined, with their HEAD/clean predicates) against the preserved root,
  writing the regenerated documents into the Increment 6 research
  workspace — never into the repository or the preserved root. This
  refines the spec's "preserved alongside the originals": the preserved
  root stays immutable, and the workspace records the original and
  regenerated document hashes side by side.
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

### Task 10: Codex subagent payload probe (operational, throwaway)

Decision input for Tasks 11-13 (spec: the Codex subagent mapping rows are
conditional on a usable identity in the payload).

- [ ] **Step 1** Create an isolated `CODEX_HOME` under the session
  scratchpad with a minimal config and a `hooks.json` registering
  `SessionStart`, `SubagentStart`, and `SubagentStop` command hooks that
  append their stdin to a capture file. Never touch `~/.codex`.
- [ ] **Step 2** Run one bounded `codex exec` (pinned model `gpt-5.6-sol`)
  with a prompt that instructs delegating a trivial subtask to a subagent.
  If the run spawns no subagent after two bounded attempts, the probe
  concludes "not provocable" — that outcome selects the session-level
  branch, it does not fail the task.
- [ ] **Step 3** Record the captured payloads (redacting nothing beyond
  content fields, which the capture must not persist) in the Increment 6
  research workspace, and decide: identity fields present → Tasks 11-12
  implement the full Codex mapping rows; absent or not provocable →
  session-level rows only, gap recorded in the ledger and in Task 13's
  document.

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
) -> Vec<ControllerEnvelope>
```

Mapping (spec table; every envelope: `schema_version` 1, `source`
`hook:claude-code`|`hook:codex`, `provider` `claude-code`|`codex`,
`emitted_at_ms` as passed, `event_id`
`hook:<provider>:<session_id>:<hook_event_name>:<entity>[:<transition>]:<emitted_at_ms>`):

1. `SessionStart`/`session_start` → one `task_started`; `task_run_id`
   `hook:<provider>:<session_id>`; `native_session_id` = session_id;
   entity `session`.
2. `SubagentStart`/`subagent_start` (with `agent_id`) → `dispatch`
   (subject `…:agent:<agent_id>`, `parent_task_run_id` = session run,
   entity `<agent_id>`, transition `dispatch`) then `task_started` (same
   subject, `label` = `agent_type` when present, transition `started`).
   Codex rows only per Task 10's decision; missing `agent_id` → empty vec.
3. `SubagentStop`/`subagent_stop` (with `agent_id`) → one `complete`
   (transition `complete`).
4. `TaskCreated` (Claude Code; with `task_id`) → `dispatch` (subject
   `…:task:<task_id>`) then `progress` with `label` = `task_subject`.
5. `TaskCompleted` (with `task_id`) → one `complete`.
6. `SessionEnd`, `session_end`, and every other event name → empty vec.
7. No field from the payload other than the listed structural fields is
   ever read; `native_session_id` appears only on the SessionStart
   envelope.

- [ ] **Step 1: Write failing unit tests**: one per table row and per
guard —
session-start shape (all envelope fields asserted exactly);
subagent start pair (dispatch parent, ids, label, distinct event_ids);
subagent stop; task created pair; task completed; session_end → empty;
unknown event → empty; missing agent_id/task_id → empty; event ids carry
the `emitted_at_ms` suffix and never start with `prov:`; two invocations
with different timestamps produce different event ids for the same hook.
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
`required_unless_present = "from_hook"` and `conflicts_with = "from_hook"`
(optional manual arguments only gain `conflicts_with`). The manual path is
byte-for-byte unchanged when `--from-hook` is absent.

Adapter path behavior: read standard input to end with a 1 MiB cap
(oversize → warn, exit 0); parse `HookPayload` (failure → warn, exit 0);
resolve session and endpoint once through the existing `run_emit` calls
(unavailable → existing `emit_unavailable`, which exits 0 without
`--strict`); `emitted_at_ms` from the system wall clock; deliver every
mapped envelope sequentially via `emit_to_endpoint`, printing each response
line exactly as the manual path does; delivery failure warns and continues
to the next envelope; exit 0 unless `--strict` and any envelope failed.

- [ ] **Step 1: Write the failing wire test** in `tests/controller.rs`: a
real listener plays collector, the test invokes the adapter code path (the
extracted `run_emit_from_hook` function called with a `SubagentStart`
payload string), and asserts two envelopes arrive in order (`dispatch`
then `task_started`) with the exact ids and fields from the Task 11 table,
each acknowledged `accepted`.
- [ ] **Step 2: Write the failing CLI-surface tests** (unit, in
`src/main.rs` tests): `--from-hook claude-code` parses with no manual
arguments; manual invocation without `--event-id` still errors; combining
`--from-hook` with `--event-id` errors.
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
12.3; exact registration snippets —

```json
// ~/.claude/settings.json (merge into "hooks")
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

and the `~/.codex/hooks.json` equivalent for the events Task 10 selected;
the Codex hook-trust acceptance step; that hooks run in parallel and
coexist with herdr's own integration hooks; behavior outside managed panes
(no resolvable session → warn, exit 0, deliver nothing); dependency-edge
guidance with one complete manual `herdr-top emit --event-type depends_on`
example including every required flag; troubleshooting via
`herdr-top doctor`.

- [ ] **Step 1** Write the document; verify every snippet against the
implemented CLI by executing the commands with a throwaway session name.
- [ ] **Step 2: Commit** `docs: add controller emit setup guide`

### Task 14: live acceptance (operational, Controller-executed with the user)

- [ ] **Step 1** With user confirmation, register the Task 13 hooks in the
live `~/.claude/settings.json` and `~/.codex/hooks.json` (these are
live-authoritative user files: Controller edits them directly, per the
established rule for worker-uneditable live paths, and reports the exact
diff).
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
