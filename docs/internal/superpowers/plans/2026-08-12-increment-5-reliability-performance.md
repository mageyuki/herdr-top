# Increment 5 Reliability and Measured Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking. Under the active no-Fable
> route, each implementation task is executed by one fresh `codex exec` worker
> in its own linked worktree; workers do not commit or re-delegate.

**Goal:** Establish an unchanged performance baseline, structurally close D1 and
D2, publish measured event lag and load-driven degradation, and reach an
evidence-backed D4 decision without speculative optimization.

**Architecture:** A deterministic integration harness records production subject
`9cd9813` before production code changes. Persistence hardening and a reusable
runtime performance tracker then land behind explicit ownership and admission
interfaces. Provider, Controller, collector, and TUI wiring are integrated in
dependency order, followed by authoritative current-host measurements and a
stop/go D4 checkpoint.

**Tech Stack:** Rust 1.97.1, Tokio watch/mpsc/oneshot, SQLite through rusqlite,
Ratatui `TestBackend`, Serde JSON, Bash, util-linux `taskset`/`prlimit`, GNU time,
sysstat `pidstat`, and jq 1.8.1.

## Global Constraints

- The approved design is
  `docs/internal/superpowers/specs/2026-08-12-increment-5-reliability-performance-design.md`
  at commit `fb4c651ca60471de5e399e8e1e161d0f0507ea5e`.
- Production subject for the untouched baseline is
  `9cd98131038a53b6dd36ff53e9b89825acba70ae`.
- The binding target is 50 live panes, 200 live/default-visible Task Runs,
  1,000 dependency edges, 20 admitted reducer events/s sustained, and a
  100 events/s burst for ten seconds without loss.
- Screen-update p95 is below one second; input-response p95 is below 100 ms;
  startup is below three seconds after restoring 100,000 non-gap retained
  activity `events` with matching deduplication-ledger rows; idle CPU is below
  2%; maximum process-tree RSS is below decimal 100 MB (100,000,000 bytes) on
  the selected reference profile. Fallback scanning adds no more than its single
  two-second polling interval and loses no event.
- Exact envelope values pass. Only values strictly greater than 50 panes,
  200 visible runs, 1,000 dependency edges, 100 events/1s, 1,000 events/10s,
  1,200 events/60s, or one second of event lag activate their reason.
- Runtime performance state is process-local and is never persisted or added to
  a migration.
- No Task Run, execution edge, dependency edge, or admitted event outcome may be
  dropped to satisfy a performance target.
- D4 remains the full recomputation unless a valid authoritative run both misses
  a section 15 target and measures D4 at 25% or more of reducer-plus-publish time
  at the failing workload. Even then, stop for a design amendment; do not
  implement D4 from this plan.
- The selected reference profile is CPUs 0-3 under `taskset -c 0-3`, inherited
  `prlimit --as=17179869184`, local NVMe, GNU time, and `pidstat`. Reports must
  say that this is not a physical 16-GB host and not cgroup memory isolation.
- Shared CI verifies deterministic logic, no-loss invariants, schema validation,
  and ordinary fmt/clippy/tests. Shared CI never gates absolute timing, CPU, or
  RSS from an arbitrary runner.
- The root untracked `mise.toml` is user-owned. No task may read, enumerate,
  modify, stage, commit, ignore, or delete it. Tracked cleanliness checks use
  `git diff` and `git diff --cached`, never `git status --untracked-files=all`.
- Local commands must not resolve `cargo`, `rustc`, or `rustup` through `PATH`.
  On the selected host they invoke the requested launcher
  `/home/mageyuki/.cargo/bin/rustup` directly as
  `/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo` or
  `/home/mageyuki/.cargo/bin/rustup run 1.97.1 rustc`; bare tool names,
  mise-backed launchers, and `mise`
  itself are forbidden. The runner canonicalizes that exact requested path
  without searching ancestor directories. The requested path may be a symlink,
  but its canonical target must be a regular executable and no canonical path
  component may be `mise`. The runner records requested path, canonical path,
  SHA-256, selected
  toolchain, and exact cargo/rustc version output in every trial control and run
  envelope. The `cargo +stable` spellings added inside GitHub Actions are CI-only
  commands on an isolated runner and do not relax this local contract.
- Every focused Cargo test command using libtest `--exact`, and every filtered
  rustdoc command that reaches its test harness, captures the harness summary,
  rejects zero selected tests even when Cargo exits zero, and requires its
  task-declared selected/pass count (one unless the plan explicitly names a
  multi-test target). This applies to behavioral-red runs and every green run.
  `store::writer::WriterClient` is the explicitly declared two-doctest target in
  Task 2B.2a.
- A task-declared compile-boundary red is the only selected-count exception.
  Before invoking Cargo, the frozen canonical `rg` must find exactly one exact
  declaration of every named test in its declared source path. Cargo must then
  exit nonzero before the harness starts, and the captured compiler output must
  name the exact task-declared missing file, type, function, or field. Cargo
  success, zero-selection success, an absent/duplicate declaration, or an
  unrelated compiler error is not the red state. As soon as compilation
  succeeds, the selected/pass-count rule above applies without exception.
- Fable dispatch and Fable health checks remain prohibited. Mandatory Claude
  review uses actual `claude-opus-5`; every review supplement uses a fresh
  `gpt-5.6-sol` process with `model_reasoning_effort="max"`.
- Every behavior-changing implementation task uses TDD and one dedicated branch
  plus project-local linked worktree under `.worktrees/`. The worker uses fresh
  `gpt-5.6-sol` at effort `xhigh`, does not commit, and does not re-delegate.
- After each task, Opus 5 is the authoritative per-task reviewer and a different
  fresh max-effort Codex review is supplemental. The Controller verifies the
  actual diff and tests, then alone stages and commits the approved file set.
- Integration is serial. Push, PR, merge, release, squash, reset, rewrite, and
  rebase are outside this plan.
- A rejected task worktree and its unmerged branch remain isolated and are never
  integrated; the Controller reports them and removes neither without explicit
  user authorization. A defect discovered after integration is corrected
  forward in a new task branch/worktree and commit, never by rewriting history.
- Every authoritative profile run is exclusive: pause implementation workers,
  builds, tests, and other elective repository workloads before warm-up, and
  record ambient load. Parallel task dispatch resumes only after measurement.
- Every authoritative first exec uses the Task 1A.2a versioned native Controller
  launcher. The already-trusted parent Bash replaces itself with that launcher
  through its `exec -c` special builtin, so the launcher's native loader receives
  an empty `envp`; the launcher then uses Rust `Command::env_clear()` followed by
  the exact child allowlist. An external `env -i`, a shebang, or a newly loaded
  intermediate shell never establishes this boundary. If the parent cannot use
  that exact bootstrap or the launcher identity does not revalidate, the run
  fails closed before Bash, the measured binary, or any result writer starts.
- Before Task 1A.1 is dispatched, the Controller commits this plan only after its
  mandatory actual-Opus and fresh-Codex reviews both approve the same bytes,
  records that planning commit in the research ledger, and creates every task
  branch/worktree from the latest integrated HEAD that contains it. An untracked
  or merely staged plan is never an implementation base.

## File and dependency map

| Task | Declared repository file set | Depends on | Parallel eligibility |
| --- | --- | --- | --- |
| 1A.1 | `Cargo.toml`, `tests/common/mod.rs`, new `tests/common/workload.rs`, `tests/fixtures/MANIFEST.md`, new `tests/fixtures/workload-schema-v1.json`, new `tests/workload_harness.rs` | reviewed plan commit | first; harness/protocol plus Linux observer |
| 1A.2a | `Cargo.toml`, `tests/workload_harness.rs`, new `tests/support/reference_profile_controller.rs`, new `scripts/run-reference-profile.sh` | Task 1A.1 | serial; harness/protocol plus native first-exec Controller and runner |
| 1A.2b | `.github/workflows/ci.yml` | Task 1A.2a | serial; CI-only doctest integration |
| 1B.1 | `src/operator.rs`, `src/store/mod.rs`, `tests/workload_harness.rs` | Task 1A.2b | serial; retention aliases and red-first drift test |
| 1B.2 | `src/tui/app.rs`, `tests/workload_harness.rs` | Task 1B.1 | serial; production FrameLimiter driver |
| 1B.3 | `src/herdr/controller.rs`, `src/herdr/collector.rs`, `src/reducer.rs`, `tests/workload_harness.rs` | Tasks 1B.1 and 1B.2 | serial; composes real paths and records baseline |
| 2A | `src/reducer.rs`, `src/operator.rs`, `tests/controller.rs`, `tests/convergence.rs`, `tests/workload_harness.rs` | Task 1B.3 baseline | serial integration after baseline |
| 2B.1 | `src/store/writer.rs`, `src/herdr/collector.rs` | Task 2A | serial compatibility and borrow-order bridge |
| 2B.2a | `src/store/writer.rs`, `src/store/mod.rs`, `src/reducer.rs` | Task 2B.1 | serial D1/D2 core across store and reducer only |
| 2B.2b | `src/herdr/collector.rs` | Task 2B.2a | serial lifetime-closure cleanup; followed by post-reliability barrier |
| 3 | `src/activity.rs`, `src/tui/projection.rs`, `src/tui/app.rs` | valid post-reliability measurement at exact Task 2B.2b HEAD | no integration before the measurement barrier |
| 4 | new `src/performance.rs`, `src/lib.rs` | Task 3 | serial after Task 3 |
| 5 | `src/provider/mod.rs` | Task 4 | serial after Task 4 |
| 6 | `src/provider/mod.rs`, `src/herdr/controller.rs`, `src/herdr/collector.rs`, `tests/controller.rs`, `tests/convergence.rs`, `tests/workload_harness.rs` | Tasks 2B.2b, 4, 5 | serial after all dependencies |
| 7 | `src/main.rs`, `src/tui/app.rs`, `src/tui/view.rs`, `tests/coverage_harness.rs`, `tests/workload_harness.rs` | Task 6 | serial |
| 8 | no repository mutation; external research result and ledger files only | Tasks 1A-7 integrated and the exact final-review HEAD approved | final measurement checkpoint |

Production subsystem boundaries for the degraded-mode split rule are
harness/protocol, runner, CI, herdr, reducer, operator, store, TUI, activity
policy, performance core, provider, and binary composition. A shared integration
test is validation surface for the production subsystem(s) under test and is not
an additional production subsystem. Every dispatch above therefore touches at
most two production subsystems.

Tasks 1A.1, 1A.2a, 1B.1, 1B.2, 1B.3, 2A, 6, and 7 deliberately touch
`tests/workload_harness.rs` in that serial dependency order. Tasks 1B.2, 3, and 7
touch `src/tui/app.rs`; Tasks 1B.3, 2A, and 2B.2a touch `src/reducer.rs`; Tasks
1B.3, 2B.1, 2B.2b, and 6 touch `src/herdr/collector.rs`; Tasks 1B.1 and 2A touch
`src/operator.rs`; Tasks 1A.1 and 1A.2a touch `Cargo.toml`; Tasks 1B.1 and 2B.2a
touch `src/store/mod.rs`; Tasks 1B.3 and 6 touch `src/herdr/controller.rs`; Tasks
5 and 6 touch `src/provider/mod.rs`; Tasks 2A and 6 touch `tests/controller.rs`
and `tests/convergence.rs`; Tasks 2B.1 and 2B.2a touch `src/store/writer.rs`.
Those overlaps are serial. Task 6's ancestry must include the integrated Task 2B.2b barrier plus
the reviewed Task 4 and Task 5 commits. At acceptance,
the Controller confirms each actual changed-file set is a subset of the declared
set before integration.

## Publication preflight (read-only; no publication authorized)

- Fetch and push URLs both resolve to `mageyuki/herdr-top`; the owner is not
  `aces-inc`, so any later explicitly authorized Git/GitHub operation follows the
  OpenAI-side operator route.
- The connected viewer permission is `ADMIN`; the repository is not a fork and
  its default branch is `main`.
- `.github/workflows/ci.yml` triggers on pull requests to `main` and pushes to
  `main`; the applicable nonempty checks are lint, Ubuntu/macOS tests, and MSRV
  compile.
- GitHub's fork-contributor approval policy is `first_time_contributors`, but the
  planned same-repository branch route does not use a fork.
- No PR template exists in the standard root, `docs`, or `.github` locations.
- If publication is later requested, use one cumulative Draft PR from the
  integrated Increment 5 branch to `main`; do not create stacked PRs for the
  task worktrees. This is strategy only, not authorization to push or create it.


---

### Task 1A.1: Deterministic workload protocol and Linux observer

**Files:**

- Modify: `Cargo.toml`
- Modify: `tests/common/mod.rs`
- Create: `tests/common/workload.rs`
- Modify: `tests/fixtures/MANIFEST.md`
- Create: `tests/fixtures/workload-schema-v1.json`
- Create: `tests/workload_harness.rs`

**Interfaces:**

- Consumes: the Controller-recorded reviewed-plan commit, public `DomainModel`,
  `Reducer`, store restore APIs, Ratatui `TestBackend`, and subject revision
  `9cd9813`.
- Produces the test-only Cargo feature `workload-harness`,
  `WorkloadProfile::{TargetTopology,SustainedTarget,TargetBurst,Startup,Idle,FallbackRescan,TwiceTarget}`,
  `WorkloadOracle`, `HarnessTrialV1`, `TrialResultV1`, `ReferenceOutcomeV1`, and
  `Section15ReDerivationV1`, plus
  the closed executable-runner contract consumed by Task 1A.2a: the empty-envp
  native Controller injects its self-derived controller, runner, and closed tool
  identities, then launches frozen canonical Bash with `-p`, the revalidated absolute
  `$HERDR_INCREMENT5_FROZEN_RUNNER_SCRIPT`, and `--subject SHA --stage
  baseline|post-reliability|final --scenario NAME --output-dir DIR
  [--baseline-results-root DIR]`. The baseline run against `9cd9813` omits the
  optional input; every later candidate run requires it.
- Also produces the Linux-only ignored
  `reference_profile_process_tree_observer`, which validates an immutable root
  `(pid,start_time_ticks)` received from the handshake, samples only that root
  and its descendants, and atomically writes `ProcessTreeEvidenceV1`.
- Task 1A.1 defines and validates the portable artifact protocol and Linux
  process-tree observer. Task 1A.2a implements the native Controller/Bash
  transport and Task 1A.2b owns CI only. Task 1B.3 adds the ignored
  exact `reference_profile_entrypoint` using test-feature admission adapters and
  records the untouched baseline.

Add this opt-in feature without default membership or dependencies:

```toml
[features]
workload-harness = []
```

- [ ] **Step 1: Add failing deterministic topology and schedule tests**

Add `#[allow(dead_code)] pub mod workload;` to `tests/common/mod.rs` so every
integration-test crate that includes `common` remains clean under `-D warnings`.
In
`tests/common/workload.rs`, define the closed profiles and oracle:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadProfile {
    TargetTopology,
    SustainedTarget,
    TargetBurst,
    Startup,
    Idle,
    FallbackRescan,
    TwiceTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructuralIdentities {
    pub pane_ids: std::collections::BTreeSet<String>,
    pub task_run_ids: std::collections::BTreeSet<String>,
    pub dependency_edges: std::collections::BTreeSet<String>,
    pub execution_edges: std::collections::BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadOracle {
    pub live_panes: usize,
    pub visible_runs: usize,
    pub dependency_edges: usize,
    pub execution_edges: usize,
    pub admitted_sequences: Vec<u64>,
    pub screen_probe_sequences: Vec<u64>,
    pub final_identities: StructuralIdentities,
}

pub fn oracle(profile: WorkloadProfile) -> WorkloadOracle;
pub fn period(profile: WorkloadProfile) -> std::time::Duration;
pub fn admission_offsets(profile: WorkloadProfile) -> Vec<std::time::Duration>;
pub fn screen_probe_sequences(profile: WorkloadProfile) -> Vec<u64>;
pub fn target_model() -> herdr_top::model::DomainModel;
```

Generate sequence `n` (zero-based) at `(n + 1) * period`: 50 ms for sustained
target, 10 ms for burst, and 25 ms for twice target. With half-open windows
`(now - width, now]`, this yields exactly 20, 100, and 40 admissions in each
aligned one-second interval and avoids an extra boundary event at time zero.

In `tests/workload_harness.rs`, declare the shared module before using it, then
write exact assertions:

```rust
mod common;

use common::workload::{self, WorkloadProfile};

#[test]
fn target_workload_oracle_is_exact_and_deterministic() {
    let first = workload::oracle(WorkloadProfile::TargetTopology);
    let second = workload::oracle(WorkloadProfile::TargetTopology);
    assert_eq!(first, second);
    assert_eq!(first.live_panes, 50);
    assert_eq!(first.visible_runs, 200);
    assert_eq!(first.dependency_edges, 1_000);
    assert_eq!(workload::admission_offsets(WorkloadProfile::SustainedTarget).len(), 1_200);
    assert_eq!(workload::admission_offsets(WorkloadProfile::TargetBurst).len(), 1_000);
    assert_eq!(workload::admission_offsets(WorkloadProfile::TwiceTarget).len(), 2_400);
    assert_eq!(workload::screen_probe_sequences(WorkloadProfile::SustainedTarget).len(), 300);
    assert_eq!(workload::screen_probe_sequences(WorkloadProfile::TargetBurst).len(), 50);
    assert_eq!(workload::screen_probe_sequences(WorkloadProfile::TwiceTarget).len(), 300);
}
```

- [ ] **Step 2: Run the new test and verify the red state**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test workload_harness target_workload_oracle_is_exact_and_deterministic -- --exact --nocapture
```

Expected: compilation fails because `tests/common/workload.rs` and its builders
are not implemented yet. A pre-existing test failure is not an acceptable red
state.

- [ ] **Step 3: Freeze deterministic model, event, render, and result interfaces**

The following shapes and invariants are the implementation target, not authority
to implement validator/composer behavior yet. Add the minimum topology/oracle
builders needed to make Step 1 green. After freezing the shapes below and before
the Step 4 run, add every test-only fixture constructor, raw-root helper,
test-facing accessor, and callable validator/composer/classifier declaration
referenced by the Step 4 tests. Those declarations must compile, must not panic,
and must return deliberately conservative/incomplete values such as
`Err(ResultError::InvalidArtifact)`; the synthetic builders need only be
structurally complete enough for the shown mutations. They may not implement a
successful validation, composition, observation, or D4 decision path. Write the
complete validation tests in Step 4 and observe assertion-level behavioral
failures before implementing the real builders, composer, validator, observer,
or classifier in Step 5.

Build stable zero-padded IDs and insert exactly 50 panes, 200 Task Runs, and
1,000 dependency edges. Ensure the dependency graph is acyclic by enumerating
ordered run-index pairs `(prerequisite, dependent)` where `prerequisite <
dependent`, taking the first 1,000 pairs, and making the dependent run depend on
the prerequisite. Record the exact generated execution-edge count in the oracle
rather than adding an execution-edge limit.

Define versioned result/outcome types with `#[serde(deny_unknown_fields)]` on
every object and closed snake-case enums. These are the complete final-result
schema fields; helper-only builder state and the separately frozen manifest type
are not serialized into a result:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioV1 {
    Target,
    Sustained,
    Burst,
    Startup,
    Idle,
    FallbackRescan,
    TwiceTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementStageV1 {
    Baseline,
    PostReliability,
    Final,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PidstatChildStatusModeV1 {
    PropagatesChildStatus,
    MonitorOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum TrialStatusV1 {
    Ok,
    Failed { exit_code: u8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadRenderViewV1 {
    ExecutionTree,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadOverlayV1 {
    None,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RenderSurfaceV1 {
    pub width: u16,
    pub height: u16,
    pub view: WorkloadRenderViewV1,
    pub follow: bool,
    pub filter_query: String,
    pub initial_selected_task_run_key: String,
    pub collapsed_task_run_keys: Vec<String>,
    pub overlay: WorkloadOverlayV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralIdentitiesV1 {
    pub pane_ids: Vec<String>,
    pub task_run_ids: Vec<String>,
    pub dependency_edges: Vec<String>,
    pub execution_edges: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureReasonV1 {
    ControlMismatch,
    CommandFailed,
    IncompleteTrial,
    SequenceLoss,
    StructuralLoss,
    ScreenLatency,
    InputLatency,
    StartupLatency,
    FallbackRescanLatency,
    IdleCpu,
    MaximumRss,
    WorkloadAdmission,
    SupportedLoadDegradation,
    MissingDegradation,
    DuplicateOutcome,
    InvalidArtifact,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionObservationV1 {
    pub sequence: u64,
    pub scheduled_ns: u64,
    pub admitted_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyObservationV1 {
    pub sequence: u64,
    pub admitted_ns: u64,
    pub terminal_ns: u64,
    pub published_ns: u64,
    pub rendered_ns: u64,
    pub observed_frame_phase_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputLatencyObservationV1 {
    pub scheduled_ns: u64,
    pub injected_ns: u64,
    pub rendered_ns: u64,
    pub observed_frame_phase_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopedTimingKindV1 {
    ControllerEvent,
    StartupRestore,
    FallbackNotification,
    FallbackRescan,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedTimingObservationV1 {
    pub kind: ScopedTimingKindV1,
    pub sequence: u64,
    pub d4_segment_count: u32,
    pub d4_analysis_ns: u64,
    pub reducer_plus_publish_ns: u64,
    pub model_clone_publish_segment_count: u32,
    pub model_clone_publish_ns: u64,
    pub render_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackPairObservationV1 {
    pub sequence: u64,
    pub notification_ns: u64,
    pub rescan_ns: u64,
    pub notification_final_identities: StructuralIdentitiesV1,
    pub rescan_final_identities: StructuralIdentitiesV1,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd,
    serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceReasonV1 {
    LivePanes,
    DefaultVisibleTaskRuns,
    DependencyEdges,
    EventsOneSecond,
    EventsTenSeconds,
    EventsSixtySeconds,
    EventLag,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectiveQualityV1 {
    Live,
    Reconciling,
    Disconnected,
    Degraded,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalObservationV1 {
    pub sequence: u64,
    pub terminal_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceSampleEvidenceV1 {
    pub sample_ordinal: u64,
    pub sampled_at_ns: u64,
    pub event_lag_ns: u64,
    pub pending_events: u64,
    pub admission_high_water: u64,
    pub completion_high_water: u64,
    pub live_panes: u64,
    pub default_visible_task_runs: u64,
    pub dependency_edges: u64,
    pub execution_edges: u64,
    pub events_one_second: u64,
    pub events_ten_seconds: u64,
    pub events_sixty_seconds: u64,
    pub source_quality: EffectiveQualityV1,
    pub effective_quality: EffectiveQualityV1,
    pub reasons: Vec<PerformanceReasonV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceFrameEvidenceV1 {
    pub draw_ordinal: u64,
    pub sample_ordinal: u64,
    pub state_observed_at_ns: u64,
    pub rendered_at_ns: u64,
    pub effective_quality: EffectiveQualityV1,
    pub reasons: Vec<PerformanceReasonV1>,
    pub rendered_header_line: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceEvidenceStreamV1 {
    pub workload_start_ns: u64,
    pub workload_close_ns: u64,
    pub first_sample_ordinal: u64,
    pub next_sample_ordinal: u64,
    pub first_draw_ordinal: u64,
    pub next_draw_ordinal: u64,
    pub samples: Vec<PerformanceSampleEvidenceV1>,
    pub frames: Vec<PerformanceFrameEvidenceV1>,
    pub terminal_observations: Vec<TerminalObservationV1>,
    pub selected_terminal_draw_ordinal: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceObservationV1 {
    pub offset_ns: u64,
    pub process_tree_user_cpu_ns: u64,
    pub process_tree_system_cpu_ns: u64,
    pub process_tree_rss_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentityResourceV1 {
    pub pid: u32,
    pub start_time_ticks: u64,
    pub first_observed_offset_ns: u64,
    pub idle_window_start_user_cpu_ticks: Option<u64>,
    pub idle_window_start_system_cpu_ticks: Option<u64>,
    pub idle_window_end_user_cpu_ticks: Option<u64>,
    pub idle_window_end_system_cpu_ticks: Option<u64>,
    pub last_user_cpu_ticks: u64,
    pub last_system_cpu_ticks: u64,
    pub maximum_vm_hwm_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObserverCommandV1 {
    StartIdleWindow {},
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObserverControlFrameV1 {
    Ready { observer_ready_ns: u64 },
    IdleWindowStarted { request_received_ns: u64, start_ns: u64 },
    IdleWindowEnded { end_ns: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverControlEvidenceV1 {
    pub protocol_version: u32,
    pub scenario: ScenarioV1,
    pub observed_root_pid: u32,
    pub observed_root_start_time_ticks: u64,
    pub trial_origin_ns: u64,
    pub observer_ready_ns: u64,
    pub idle_window_start_ns: Option<u64>,
    pub idle_window_end_ns: Option<u64>,
    pub commands: Vec<ObserverCommandV1>,
    pub frames: Vec<ObserverControlFrameV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessTreeEvidenceV1 {
    pub observer_pid: u32,
    pub observer_affinity_cpu_ids: Vec<u32>,
    pub observed_root_pid: u32,
    pub observed_root_start_time_ticks: u64,
    pub clock_ticks_per_second: u64,
    pub trial_origin_ns: u64,
    pub observer_ready_ns: u64,
    pub idle_window_start_ns: Option<u64>,
    pub idle_window_end_ns: Option<u64>,
    pub resource_observations: Vec<ResourceObservationV1>,
    pub process_identity_resources: Vec<ProcessIdentityResourceV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawArtifactDigestsV1 {
    pub harness_json_sha256: String,
    pub runner_control_json_sha256: String,
    pub process_tree_json_sha256: String,
    pub observer_handshake_sha256: String,
    pub observer_control_json_sha256: String,
    pub gnu_time_sha256: String,
    pub pidstat_json_sha256: String,
    pub pidstat_stderr_sha256: String,
    pub child_stdout_sha256: String,
    pub child_stderr_sha256: String,
    pub observer_stdout_sha256: String,
    pub observer_stderr_sha256: String,
    pub trial_status_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalResourceAuditV1 {
    pub gnu_elapsed_ns: u64,
    pub gnu_user_cpu_ns: u64,
    pub gnu_system_cpu_ns: u64,
    pub gnu_maximum_rss_bytes: u64,
    pub gnu_exit_status: i32,
    pub pidstat_child_user_cpu_ns: Option<u64>,
    pub pidstat_child_system_cpu_ns: Option<u64>,
    pub pidstat_wrapper_maximum_rss_bytes: Option<u64>,
    pub pidstat_sample_count: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct DistributionV1 {
    pub sample_count: usize,
    pub minimum_ns: u64,
    pub median_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub maximum_ns: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct HostProfileV1 {
    pub operating_system: String,
    pub kernel: String,
    pub architecture: String,
    pub cpu_model: String,
    pub physical_core_ids: Vec<String>,
    pub memory_total_bytes: u64,
    pub storage_kind: String,
    pub storage_device: String,
    pub governor: Option<String>,
    pub boost: Option<String>,
    pub ambient_load_milli: [u64; 3],
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutableIdentityV1 {
    pub requested_path: String,
    pub canonical_path: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct CargoConfigurationPolicyV1 {
    pub policy_version: u32,
    pub invocation_cwd: String,
    pub ordered_absent_candidates: Vec<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlsV1 {
    pub affinity_cpu_ids: Vec<u32>,
    pub address_space_limit_bytes: u64,
    pub true_cgroup_memory_limit: bool,
    pub toolchain_launcher: ExecutableIdentityV1,
    pub toolchain_name: String,
    pub rustc_version: String,
    pub cargo_version: String,
    pub build_environment: std::collections::BTreeMap<String, String>,
    pub cargo_configuration: CargoConfigurationPolicyV1,
    pub measured_binary: ExecutableIdentityV1,
    pub runner_script: ExecutableIdentityV1,
    pub authoritative_executables: Vec<ExecutableIdentityV1>,
    pub pidstat_child_status_mode: PidstatChildStatusModeV1,
    pub limitation: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildControlsV1 {
    pub effective_affinity_cpu_ids: Vec<u32>,
    pub effective_address_space_limit_bytes: u64,
    pub measured_environment: std::collections::BTreeMap<String, String>,
    pub scratch_root: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrialControlEvidenceV1 {
    pub scratch_root: String,
    pub scratch_storage_kind: String,
    pub scratch_storage_devices: Vec<String>,
    pub orchestrator_environment: std::collections::BTreeMap<String, String>,
    pub observer_environment: std::collections::BTreeMap<String, String>,
    pub validator_environment_template: std::collections::BTreeMap<String, String>,
    pub revalidated_executables: Vec<ExecutableIdentityV1>,
    pub revalidated_runner_script: ExecutableIdentityV1,
    pub revalidated_measured_binary: ExecutableIdentityV1,
    pub trial_status: TrialStatusV1,
    pub pidstat_exit_status: u8,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerControlEvidenceV1 {
    pub schema_version: u32,
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub trial_index: usize,
    pub canonical_raw_root: String,
    pub production_subject_sha: String,
    pub preflight_head: String,
    pub harness_sha: String,
    pub workload_schema_sha256: String,
    pub tracked_clean_before_composition: bool,
    pub build_profile: String,
    pub command: Vec<String>,
    pub controlled_environment: std::collections::BTreeMap<String, String>,
    pub render_surface: RenderSurfaceV1,
    pub host: HostProfileV1,
    pub controls: RunControlsV1,
    pub trial: TrialControlEvidenceV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdsV1 {
    pub screen_update_p95_ns_exclusive: u64,
    pub input_response_p95_ns_exclusive: u64,
    pub startup_ns_exclusive: u64,
    pub fallback_added_delay_ns_inclusive: u64,
    pub idle_cpu_milli_percent_exclusive: u64,
    pub process_tree_rss_bytes_exclusive: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessTrialV1 {
    pub scenario: ScenarioV1,
    pub trial_index: usize,
    pub trial_origin_ns: u64,
    pub priming_frame_recorded_ns: Option<u64>,
    pub workload_origin_ns: Option<u64>,
    pub frame_phase_offset_ns: Option<u64>,
    pub priming_frame_count: u32,
    pub admission_observations: Vec<AdmissionObservationV1>,
    pub screen_observations: Vec<LatencyObservationV1>,
    pub input_observations: Vec<InputLatencyObservationV1>,
    pub startup_observations_ns: Vec<u64>,
    pub fallback_pairs: Vec<FallbackPairObservationV1>,
    pub scoped_observations: Vec<ScopedTimingObservationV1>,
    pub submitted_sequences: Vec<u64>,
    pub admitted_sequences: Vec<u64>,
    pub completed_sequences: Vec<u64>,
    pub persisted_sequences: Vec<u64>,
    pub rendered_sequences: Vec<u64>,
    pub pane_ids: Vec<String>,
    pub task_run_ids: Vec<String>,
    pub dependency_edges: Vec<String>,
    pub execution_edges: Vec<String>,
    pub prepared_non_gap_event_count: Option<u64>,
    pub prepared_ledger_row_count: Option<u64>,
    pub restored_activity_count: Option<u64>,
    pub performance_evidence_stream: Option<PerformanceEvidenceStreamV1>,
    pub idle_window_start_ns: Option<u64>,
    pub idle_window_end_ns: Option<u64>,
    pub child_controls: ChildControlsV1,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrialResultV1 {
    pub trial_index: usize,
    pub raw: HarnessTrialV1,
    pub observer_control: ObserverControlEvidenceV1,
    pub process_tree: ProcessTreeEvidenceV1,
    pub raw_artifacts: RawArtifactDigestsV1,
    pub control_evidence: TrialControlEvidenceV1,
    pub screen_update: Option<DistributionV1>,
    pub reducer_lag: Option<DistributionV1>,
    pub publish_to_render: Option<DistributionV1>,
    pub input_response: Option<DistributionV1>,
    pub startup_ns: Option<u64>,
    pub elapsed_ns: u64,
    pub user_cpu_ns: u64,
    pub system_cpu_ns: u64,
    pub maximum_process_tree_rss_bytes: u64,
    pub sum_process_identity_peak_rss_bytes_diagnostic: u64,
    pub fallback_added_delay_ns: Option<DistributionV1>,
    pub d4_analysis_ns: Option<u64>,
    pub reducer_plus_publish_ns: Option<u64>,
    pub d4_ratio_parts_per_million: Option<u64>,
    pub external_resource_audit: ExternalResourceAuditV1,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceRunV1 {
    pub schema_version: u32,
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub production_subject_sha: String,
    pub harness_sha: String,
    pub workload_schema_sha256: String,
    pub baseline_id: String,
    pub tracked_clean: bool,
    pub build_profile: String,
    pub command: Vec<String>,
    pub controlled_environment: std::collections::BTreeMap<String, String>,
    pub render_surface: RenderSurfaceV1,
    pub host: HostProfileV1,
    pub controls: RunControlsV1,
    pub thresholds: ThresholdsV1,
    pub warm_up_trials: usize,
    pub recorded_trials: usize,
    pub trials: Vec<TrialResultV1>,
    pub failure_reasons: Vec<FailureReasonV1>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvalidRunV1 {
    pub schema_version: u32,
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub production_subject_sha: String,
    pub harness_sha: String,
    pub workload_schema_sha256: String,
    pub baseline_id: Option<String>,
    pub command: Vec<String>,
    pub controlled_environment: std::collections::BTreeMap<String, String>,
    pub failure_reasons: Vec<FailureReasonV1>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReferenceOutcomeV1 {
    Pass { document: ReferenceRunV1 },
    Failed { document: ReferenceRunV1 },
    Invalid { document: InvalidRunV1 },
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerTestOutcomeV1 {
    pub schema_version: u32,
    pub non_authoritative: bool,
    pub exit_code: i32,
    pub all_process_groups_reaped: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceOutcomeStatusV1 {
    Pass,
    Failed,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum D4PolicyV1 {
    NotApplicable,
    NonD4,
    D4Scoped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Section15MetricV1 {
    ScreenUpdate,
    InputResponse,
    Startup,
    IdleCpu,
    MaximumProcessTreeRss,
    FallbackAddedDelay,
    AdmissionDeadline,
    SubmittedSequences,
    AdmittedSequences,
    CompletedSequences,
    PersistedSequences,
    RenderedProbeSequences,
    ReducerLag,
    PublishToRender,
    PerformanceDegradation,
    D4Analysis,
    ReducerPlusPublish,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Section15UnitV1 {
    Nanoseconds,
    Bytes,
    MilliPercent,
    Count,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ThresholdComparisonV1 {
    LessThan,
    LessThanOrEqual,
    Equal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributionStatisticV1 {
    Minimum,
    Median,
    P95,
    P99,
    Maximum,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelectedResultIdentityV1 {
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub canonical_result_path: String,
    pub canonical_raw_root: String,
    pub result_sha256: String,
    pub production_subject_sha: String,
    pub harness_sha: String,
    pub workload_schema_sha256: String,
    pub baseline_id: String,
    pub measured_binary: ExecutableIdentityV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15DistributionV1 {
    pub metric: Section15MetricV1,
    pub unit: Section15UnitV1,
    pub sample_count: u64,
    pub minimum: String,
    pub median: String,
    pub p95: String,
    pub p99: String,
    pub maximum: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15PredicateV1 {
    pub metric: Section15MetricV1,
    pub unit: Section15UnitV1,
    pub ordinal: Option<u64>,
    pub observed_numerator: String,
    pub observed_denominator: Option<String>,
    pub comparison: ThresholdComparisonV1,
    pub threshold_numerator: String,
    pub threshold_denominator: Option<String>,
    pub passed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15SequenceCountsV1 {
    pub submitted: u64,
    pub admitted: u64,
    pub completed: u64,
    pub persisted: u64,
    pub rendered_probes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15TrialReDerivationV1 {
    pub trial_index: u64,
    pub sequence_counts: Section15SequenceCountsV1,
    pub admission_buckets_attained: Option<bool>,
    pub lossless: bool,
    pub structural_identities_match: bool,
    pub distributions: Vec<Section15DistributionV1>,
    pub predicates: Vec<Section15PredicateV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15ScenarioReDerivationV1 {
    pub scenario: ScenarioV1,
    pub baseline_status: ReferenceOutcomeStatusV1,
    pub final_status: ReferenceOutcomeStatusV1,
    pub final_failure_reasons: Vec<FailureReasonV1>,
    pub trials: Vec<Section15TrialReDerivationV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15BaselineDeltaV1 {
    pub scenario: ScenarioV1,
    pub trial_index: u64,
    pub metric: Section15MetricV1,
    pub statistic: DistributionStatisticV1,
    pub unit: Section15UnitV1,
    pub baseline_value: String,
    pub final_value: String,
    pub signed_delta: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15FailurePolicyEvidenceV1 {
    pub measurement_stage: MeasurementStageV1,
    pub scenario: ScenarioV1,
    pub failure_reason: FailureReasonV1,
    pub policy: D4PolicyV1,
    pub d4_analysis_sum: Option<String>,
    pub reducer_plus_publish_sum: Option<String>,
    pub d4_exact_quarter_predicate: Option<bool>,
    pub required_amendment: Option<RequiredAmendmentV1>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd,
    serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredAmendmentV1 {
    D4,
    NonD4,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum D4CheckpointDecisionV1 {
    NoMissD4NotAuthorized {},
    AmendmentsRequired { amendments: Vec<RequiredAmendmentV1> },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct D4CheckpointDocumentV1 {
    pub schema_version: u32,
    pub decision: D4CheckpointDecisionV1,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Section15ReDerivationV1 {
    pub schema_version: u32,
    pub subject_sha: String,
    pub baseline_id: String,
    pub selected_results: Vec<SelectedResultIdentityV1>,
    pub scenarios: Vec<Section15ScenarioReDerivationV1>,
    pub baseline_deltas: Vec<Section15BaselineDeltaV1>,
    pub failure_policy_evidence: Vec<Section15FailurePolicyEvidenceV1>,
    pub decision: D4CheckpointDecisionV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResultError {
    #[error("recorded trials are incomplete")]
    IncompleteTrials,
    #[error("admitted sequence coverage is not lossless")]
    SequenceCoverage,
    #[error("a sequence outcome is duplicated")]
    DuplicateOutcome,
    #[error("final structural identities differ from the oracle")]
    StructuralMismatch,
    #[error("a measured threshold failed")]
    Threshold,
    #[error("required reference controls were not proven")]
    InvalidControl,
    #[error("raw tool or harness evidence is missing or inconsistent")]
    InvalidArtifact,
}

#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("harness I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("store operation failed: {0}")]
    Store(#[from] herdr_top::store::StoreError),
    #[error("result encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("harness invariant failed: {0}")]
    Invalid(&'static str),
}

```

`D4CheckpointDocumentV1::validate` accepts only `schema_version == 1` and rejects
an empty, duplicate, or non-strictly-sorted `AmendmentsRequired` vector before
converting it to a semantic `BTreeSet`. The exact JSON is a wrapper such as
`{"schema_version":1,"decision":{"kind":"no_miss_d4_not_authorized"}}` or
`{"schema_version":1,"decision":{"kind":"amendments_required","amendments":["d4","non_d4"]}}`.
The empty struct variant makes content on `no_miss_d4_not_authorized` an unknown
field rather than adjacent-tagged unit content. Serde's closed enum values,
struct variants, `Vec` wire representation, and `deny_unknown_fields` reject
unknown decision kinds, amendment names, null/unit content, and extra fields;
validation rejects duplicates and unsorted input before any set can normalize it.

`Section15ReDerivationV1::validate` accepts only schema version 1. It requires
exactly fourteen strictly ordered selected identities—Baseline then Final for
each of the seven manifest scenarios—whose canonical paths are non-reused, whose
digests and embedded identities revalidate byte-for-byte, whose Final subjects
all equal `subject_sha`, and whose baseline IDs all equal `baseline_id`. It
requires exactly one ordered scenario row per manifest scenario, the exact
recorded-trial count, the scenario-applicable sequence counts, distributions,
predicates, and baseline-delta statistics, plus an exact row for every and only
the validated Final failure reason. `Invalid` is rejected from both selected
stages. Every unsigned numeric string is canonical base-10 `u128` with no sign or
leading zero except literal `0`; every signed delta is canonical `0` or an
optional `-` followed by a nonzero canonical magnitude. Validation re-derives
all values from selected raw evidence with checked arithmetic, enforces the
metric/unit/applicability combinations, recomputes each `passed` bit and signed
delta, and rejects missing, duplicate, extra, unsorted, overflowed, or mismatched
rows.

The workload manifest owns this exact ordered section-15 row matrix. A repeated
row is ordered by the raw manifest order named in the final column; there is no
map iteration or implementation-selected ordering. `dist` means one
`Section15DistributionV1` with all five statistics and `pred` means one
`Section15PredicateV1`:

| Scenario | Ordered distribution rows per paired trial | Ordered predicate rows per Final trial | Repetition/order |
| --- | --- | --- | --- |
| `Target` | `InputResponse/Nanoseconds` | `InputResponse/Nanoseconds`, `MaximumProcessTreeRss/Bytes` | one distribution over the 200 input observations; predicates use its p95 then the simultaneous-RSS maximum |
| `Sustained` | `ScreenUpdate/Nanoseconds`, `ReducerLag/Nanoseconds`, `PublishToRender/Nanoseconds`, `D4Analysis/Nanoseconds`, `ReducerPlusPublish/Nanoseconds` | `ScreenUpdate/Nanoseconds`, then `AdmissionDeadline/Nanoseconds` repeated 60 times, `SubmittedSequences/Count`, `AdmittedSequences/Count`, `CompletedSequences/Count`, `PersistedSequences/Count`, `RenderedProbeSequences/Count`, `MaximumProcessTreeRss/Bytes`, `PerformanceDegradation/Count` | admission predicates follow bucket index `0..59`; count predicates follow the literal order shown; the final predicate requires zero truthfully re-derived degraded performance samples |
| `Burst` | `ScreenUpdate/Nanoseconds`, `ReducerLag/Nanoseconds`, `PublishToRender/Nanoseconds`, `D4Analysis/Nanoseconds`, `ReducerPlusPublish/Nanoseconds` | `ScreenUpdate/Nanoseconds`, then `AdmissionDeadline/Nanoseconds` repeated 10 times, the same five sequence-count predicates, `MaximumProcessTreeRss/Bytes`, `PerformanceDegradation/Count` | admission predicates follow bucket index `0..9`; the final predicate requires zero truthfully re-derived degraded performance samples |
| `Startup` | `Startup/Nanoseconds`, `D4Analysis/Nanoseconds`, `ReducerPlusPublish/Nanoseconds` | `Startup/Nanoseconds` | all three distributions have one raw sample; whole-wrapper RSS is deliberately absent |
| `Idle` | none | `IdleCpu/MilliPercent`, `MaximumProcessTreeRss/Bytes` | scalar resource predicates only |
| `FallbackRescan` | `FallbackAddedDelay/Nanoseconds`, `D4Analysis/Nanoseconds`, `ReducerPlusPublish/Nanoseconds` | `FallbackAddedDelay/Nanoseconds` repeated five times, then `MaximumProcessTreeRss/Bytes` | delay predicates follow ascending fallback-pair sequence; both D4 distributions contain both arms in `(sequence, notification, rescan)` order |
| `TwiceTarget` | `ScreenUpdate/Nanoseconds`, `ReducerLag/Nanoseconds`, `PublishToRender/Nanoseconds`, `D4Analysis/Nanoseconds`, `ReducerPlusPublish/Nanoseconds` | `AdmissionDeadline/Nanoseconds` repeated 60 times, the same five sequence-count predicates, `MaximumProcessTreeRss/Bytes`, `PerformanceDegradation/Count` | admission predicates follow bucket index `0..59`; the final predicate is `1 == 1` only when `selected_terminal_draw_ordinal` names the earliest matching by-deadline frame and `0 == 1` for valid `MissingDegradation` |

The Final predicate thresholds/comparisons are fixed. `ordinal` is `Some(0..N)`
only for admission bucket rows and is `Some(sequence)` only for fallback-pair
rows; it is `None` everywhere else. Input p95 is `<
100_000_000 ns`; screen p95 is `< 1_000_000_000 ns`; Startup is `<
3_000_000_000 ns`; each admission row is `admitted_at_ns <= bucket_end_ns +
cadence_ns`; each sequence-count row is `observed == manifest expected`; each
fallback row is `rescan_ns - notification_ns <= 2_000_000_000 ns`; idle CPU is
the checked milli-percent ratio `< 2_000`; every binding RSS row is `<
100_000_000 bytes`; supported-load performance degradation is `observed == 0`,
where the numerator is the number of stream samples whose truthfully re-derived
performance-reason vector is nonempty, while twice-target performance degradation
is `observed == 1`. Predicate
re-derivation first requires `rescan_ns >= notification_ns` and uses
`checked_sub`; a one-nanosecond reversal invalidates the selected raw result and
the entire Section 15 report rather than producing a zero delay.
Numerator and denominator presence is fixed by those threshold formulas: only
idle CPU carries a denominator; every other row has `None`. `passed` is always
freshly re-derived.

`baseline_deltas` contains exactly five rows—`Minimum`, `Median`, `P95`, `P99`,
then `Maximum`—for every distribution row in the table, ordered by scenario,
then trial index, then distribution row, then statistic. It pairs only the
Baseline and Final result with the same scenario and one-based recorded trial
index and requires the same metric, unit, raw sample cardinality, manifest, and
baseline identity on both sides. Scalar predicates, sequence counts, statuses,
failure reasons, resource values, and performance-degradation counts are
intentionally excluded from baseline deltas; an attempted delta for any of
those is an extra row. D4 aggregation is limited to the raw scoped observations
of that same scenario/trial: Sustained and Burst use all admitted sequences,
Startup uses its single restore scope, FallbackRescan uses both arms of all five
pairs, and TwiceTarget uses all admitted sequences. No Target or Idle D4 row is
permitted.

`failure_policy_evidence` is ordered by scenario and then the manifest's closed
failure-reason order and contains every and only Final failure. `D4Scoped` alone
carries both checked sums and `d4_exact_quarter_predicate`; its
`required_amendment` is `Some(D4)` when that predicate is true and
`Some(NonD4)` when false. `NonD4` carries none of the three D4 fields and always
carries `Some(NonD4)`. `NotApplicable` cannot appear for a valid failure, and an
empty failure set requires an empty evidence vector and
`NoMissD4NotAuthorized`. The document's decision must equal a fresh
classification over exactly those rows. Thus Step 3 owns both re-derivation and
the decision; Task 8 Step 4 independently validates and manually reproduces it
rather than adding a decision later.

`HarnessTrialV1` is the measured-process intermediate artifact. The separate
`ProcessTreeEvidenceV1` is written by an external observer process that is a
sibling of the measured root and is never part of that root's descendant tree.
The separate `RunnerControlEvidenceV1` is written by the typed
`record_runner_control_evidence` entrypoint after each recorded trial and before
composition; shell never constructs or edits it. `TrialResultV1` embeds the
exactly deserialized harness, observer-control, and process-tree values, copies
only `RunnerControlEvidenceV1.trial` as its control evidence, and adds only
aggregates re-derived from their raw arrays plus the thirteen exact recorded-trial
artifact digests. The validator receives the
scenario raw root, rehashes the fixed files
`trial-<index>/{harness.json,runner-control.json,process-tree.json,observer-handshake,observer-control.json,gnu-time.txt,pidstat.json,pidstat-stderr,stdout,stderr,observer-stdout,observer-stderr,trial-status}`, and rejects
a missing file, digest mismatch, reused canonical path, or trial-index mismatch.
Every recorded control artifact must carry the requested subject, the exact
clean `preflight_head`, and `harness_sha == preflight_head`, where both harness
fields are the same lowercase 40-hex Git object name. Its scenario/trial/raw-root
identity must match the other three typed artifacts. All run-level members must
be byte-identical across a scenario's recorded control artifacts and every
per-trial member must match that trial's deterministic paths and fresh
revalidations. The composer obtains no run or trial control identity from hidden
discovery or an ambient environment; it constructs the run envelope solely from
these validated control artifacts and the built-in v1 manifest.
Sequence outcomes remain `Vec<u64>` until validation so duplicate outcomes
cannot be erased by set construction. Validation rejects a duplicate, then
compares the corresponding sets and exact structural identities. Fallback pairs
carry one shared sequence plus separate notification-arm and rescan-arm identity
sets instead of relying on parallel-array position or one ambiguous combined
result. Each arm is independently compared with the frozen oracle. Process
identity evidence is keyed by `(pid,start_time_ticks)` and remains raw so the
resource reductions are reproducible. The validator requires the harness,
observer-control, and process-tree scenario/root identity/protocol-origin values
to match,
requires `protocol_version == 1`, requires the exact frame vector `[Ready]` for
non-Idle with an empty command vector, or the exact child-to-observer command
vector `[StartIdleWindow {}]` and observer-authored frame vector
`[Ready, IdleWindowStarted, IdleWindowEnded]` for Idle. The observer stamps
`request_received_ns`; the measured child never supplies that value. Validation
requires `observer_ready_ns <= request_received_ns <= idle_window_start_ns <
idle_window_end_ns` with matching fields, and requires `observer_pid` to be absent
from the observed root's transitive descendant identities, requires observer
affinity exactly `[4,5,6,7,12,13,14,15]`, and requires the
observer's first root identity to match `(observed_root_pid,
observed_root_start_time_ticks)` at or before `observer_ready_ns`. The first
resource sample must contain that root and have an offset no later than the ready
acknowledgement, and `observer_ready_ns - trial_origin_ns` must not exceed five
seconds; measured setup is forbidden before that acknowledgement.

For `Idle`, the external observer owns both binding boundaries. At the start
request it records cumulative ticks for every then-live identity and replies with
the exact monotonic start; after at least 30 seconds it records end ticks for
every live identity, retains the last pre-exit ticks for an identity that exited
inside the window, and replies with the exact monotonic end. An identity first
observed after start but no later than end uses zero start ticks; an identity
first observed after end has `None` end ticks and is excluded. The validator
subtracts only each identity's explicit end/start fields, never `last_*`, which
remain diagnostic. Harness, control, and process-tree boundary values must match
exactly. Validation rejects a missing start/end value under those rules,
end-before-start, a window shorter than 30 seconds, identity ambiguity, counter
regression, post-end inclusion, or a non-Idle trial carrying idle-window fields.
All time is integer nanoseconds and RSS is integer bytes. The fixed
threshold values are
`1_000_000_000`, `100_000_000`, `3_000_000_000`, `2_000_000_000`, `2_000`
milli-percent, and `100_000_000` bytes respectively.

`trial_origin_ns` is the protocol origin captured before the observer handshake;
it is never the workload schedule origin. Every scheduled reducer profile waits
for validated observer `Ready`, completes topology/store setup, primes the
production `FrameLimiter` with one recorded draw and records its absolute
`priming_frame_recorded_ns`. The manifest `frame_phase_offset_ns`, written `φ`,
is a desired scheduling seed: the offset from a scheduled mutation or input to
the earliest eligible production draw when the limiter has zero overshoot. It is
strictly between zero and the 100-ms frame interval and is not a promise about
an actual real-clock draw timestamp.
The harness checked-subtracts `φ` from 100 ms, checked-adds that complement to
the priming draw, and records the result as `workload_origin_ns`. It waits until
that schedule epoch before producing events. Validation requires the exact
checked arithmetic and
`trial_origin_ns <= observer_ready_ns <= priming_frame_recorded_ns <
workload_origin_ns`. All protocol,
admission, and frame timestamps use the same system-wide `CLOCK_MONOTONIC`
nanosecond domain; no process-relative `Instant::elapsed()` value is serialized
or joined across processes. Task 4's feature-only absolute clock is the sole
source for serialized in-process values after migration; ordinary production's
relative `Instant` clock remains runtime-only. For one-based sequence `n`, `scheduled_ns` must equal
checked `workload_origin_ns + n * period_ns`, where the period is exactly 50 ms, 10 ms, or
25 ms for sustained, burst, or twice-target, and `admitted_ns` must not precede
that value. For every aligned one-second schedule bucket, all 20, 100, or 40
events scheduled in that bucket must be successfully admitted no later than the
bucket end plus one cadence period. This proves the actual queue sustains the
requested rate while allowing exactly one scheduling quantum at a bucket
boundary; it does not require every individual reservation to complete within
one period. A missed bucket is a measured `WorkloadAdmission` failure, not
lossless success. Submitted/admitted/completed/persisted/rendered sequence
coverage remains independently mandatory. A target sustained/burst admission
miss is a section 15 reducer-workload miss and keeps its D4-scoped samples; a
twice-target admission miss is an acceptance failure but not a section 15 target
miss and therefore requires a non-D4 amendment rather than authorizing D4.
The portable harness helper
`admission_schedule_attained(profile, workload_origin_ns, observations)` implements
this checked-integer bucket predicate and is used by both CI tests and the final
typed validator; the runner does not reimplement it in shell.

The validator joins each `LatencyObservationV1` to exactly one
`AdmissionObservationV1` by sequence and requires byte-for-byte numeric equality
of their `admitted_ns` values. It rejects a missing or duplicate sequence,
`terminal_ns < admitted_ns`, `published_ns < admitted_ns`, or `rendered_ns`
earlier than either terminal or publication. It derives every latency with
`checked_sub`; subtraction failure, rather than saturation or wrapping, produces
`InvalidArtifact`. No ordering between terminal and publication is invented.
For `Sustained`, `Burst`, and `TwiceTarget`, the composer also derives one
`reducer_lag` sample as `terminal_ns.checked_sub(admitted_ns)` and one
`publish_to_render` sample as `rendered_ns.checked_sub(published_ns)` from every
raw screen observation. Both distributions therefore have exactly the raw
screen-observation count (`300`, `50`, or `300`) and use the same nearest-rank
summary rule as `screen_update`. They are diagnostic distributions with no
threshold failure reason. Every other scenario requires both fields to be
`None`; a missing applicable value, a non-`None` inapplicable value, a sample
count mismatch, or any aggregate not exactly re-derived from raw timestamps is
`InvalidArtifact`.
For every screen sample, the validator derives
`observed_frame_phase_ns = (rendered_ns - admitted_ns) % 100_000_000` with
checked subtraction; the serialized observation must equal that derived value.
The exact scheduled timestamp remains control evidence and must satisfy the
manifest's origin/cadence arithmetic, but an authoritative real-clock sample
does not equate its scheduled-to-render modulo with desired `φ`: scheduler and
limiter overshoot remains in the actual timestamps and latency. Every
`InputLatencyObservationV1` requires `injected_ns >= scheduled_ns` and
`rendered_ns >= injected_ns`, derives its duration with `checked_sub`, and carries
`observed_frame_phase_ns = (rendered_ns - injected_ns) % 100_000_000`, which the
validator re-derives exactly. Its first `scheduled_ns` equals
`workload_origin_ns`; each later
`scheduled_ns` is the prior response frame's `rendered_ns` plus checked
`(100_000_000 - frame_phase_offset_ns)`. A phase-bearing scenario requires
`priming_frame_count == 1` and the complete five-trial phase multiset from the
manifest; a non-phase scenario requires `priming_frame_count == 0` and rejects
the priming timestamp, workload origin, and phase. This
prevents one unrecorded limiter phase from determining p95.
Observed sequence or structural loss is never a valid measurement: it produces
`Invalid(SequenceLoss)` or `Invalid(StructuralLoss)`. Malformed, duplicate, or
internally inconsistent evidence likewise produces `Invalid`. No loss reason is
permitted in `ReferenceRunV1.failure_reasons`.

`tests/fixtures/workload-schema-v1.json` is the canonical workload manifest. Its
exact UTF-8 bytes are compact JSON with no BOM and exactly one trailing LF. It
contains the schema version, all scenario names and schedules, recorded/warm-up
counts, applicability matrix, fixed threshold inclusivity, nearest-rank
percentile rule, the frozen `operator_activity_limit: 10_000`, admission-deadline
and screen-probe policy, reducer-lag and publish-to-render derivation/applicability,
fallback arm policy,
baseline-transfer policy, external-observer idle-window resource-reduction
policy, measurement-stage applicability,
performance-evidence-stream policy, the complete ordered reason-to-label table
(`live_panes`→`panes`, `default_visible_task_runs`→`visible_runs`,
`dependency_edges`→`dependency_edges`, `events_one_second`→`events_1s`,
`events_ten_seconds`→`events_10s`, `events_sixty_seconds`→`events_60s`, and
`event_lag`→`event_lag`), the fixed render surface, and the recorded-trial
`frame_phase_offsets_ns` vector `[10_000_000, 30_000_000, 50_000_000,
70_000_000, 90_000_000]`. The five recorded Target/Sustained/Burst/TwiceTarget
trials consume those offsets in order; the single warm-up uses 50 ms and is not
aggregated. Each value is the desired scheduled-mutation/input-to-next-draw
delay `φ`, never elapsed time since the preceding draw; zero, 100 ms, or a value
outside that open interval is invalid. Startup/Idle/FallbackRescan carry no frame
phase. Its closed Rust
representation uses only structs, enums, and
vectors in declared order—no unordered map participates in canonicalization.
It also freezes the only CLI-token-to-directory mapping:
`target`→`target`, `sustained`→`sustained`, `burst`→`burst`,
`startup`→`startup`, `idle`→`idle`,
`fallback-rescan`→`fallback_rescan`, and
`twice-target`→`twice_target`. `all` is an aggregate command token and never a
directory. Directory names are therefore exactly the snake-case `ScenarioV1`
serde values; the runner, validator, baseline transfer, and D4 classifier all
consume this manifest mapping and never translate or discover names independently.
`tests/common/workload.rs` loads it with `include_bytes!`,
deserializes it into a closed manifest type, and rejects any byte sequence whose
compact serialization plus one LF is not byte-identical. The lowercase
64-hex-character SHA-256 of those exact bytes is `workload_schema_sha256`.
`workload_schema_manifest_has_golden_digest()` pins the digest literal; every
candidate and later run must equal it. Any schedule, threshold, applicability,
render, or reduction change therefore requires a deliberate v2 manifest rather
than silently reusing a baseline identity.
Task 1A.1's validator derives `perf:` only through this manifest table. Task 7 adds
an exhaustive test that enumerates every `PerformanceDegradationReason` and
requires the production `performance_reason_label` result to equal the same
checked-in fixture table, so validator and renderer cannot drift independently.

Update `tests/fixtures/MANIFEST.md` in the same task with a new **Workload
family** section. It identifies `workload-schema-v1.json` as a wholly synthetic,
versioned protocol fixture authored from the approved Increment 5 design (not a
captured transcript), lists the exact structural/schedule/threshold/render/
failure-policy evidence it freezes, states that it contains no provider payload
or private raw diagnostic, and points to the golden SHA-256 test. This file is
part of Task 1A.1's declared and committed file set; the fixture may not land
without its provenance entry.

The manifest also owns the one closed failure-policy table below. The JSON stores
the stage and scenario arrays exactly as shown, in declaration order; `*` below
is presentation shorthand for the explicit ordered array
`[baseline, post_reliability, final]`, and “all seven scenarios” is shorthand for
`[target, sustained, burst, startup, idle, fallback_rescan, twice_target]`;
neither is a runtime wildcard. Validation and
`classify_d4_checkpoint` both consume these same typed rows. A
`(measurement_stage, scenario, failure_reason)` tuple absent from the table is
`InvalidArtifact`; neither component may invent a default route.

| Stages | Scenarios | Failure reason | Outcome | D4 route |
| --- | --- | --- | --- | --- |
| `*` | all seven scenarios | `ControlMismatch` | `Invalid` | `NotApplicable` |
| `*` | all seven scenarios | `CommandFailed` | `Invalid` | `NotApplicable` |
| `*` | all seven scenarios | `IncompleteTrial` | `Invalid` | `NotApplicable` |
| `*` | all seven scenarios | `DuplicateOutcome` | `Invalid` | `NotApplicable` |
| `*` | all seven scenarios | `InvalidArtifact` | `Invalid` | `NotApplicable` |
| `*` | all seven scenarios | `StructuralLoss` | `Invalid` | `NotApplicable` |
| `*` | `Sustained`, `Burst`, `FallbackRescan`, `TwiceTarget` | `SequenceLoss` | `Invalid` | `NotApplicable` |
| `*` | `Target` | `InputLatency` | `Failed` | `NonD4` |
| `*` | `Sustained`, `Burst` | `ScreenLatency` | `Failed` | `D4Scoped` |
| `*` | `Startup` | `StartupLatency` | `Failed` | `D4Scoped` |
| `*` | `FallbackRescan` | `FallbackRescanLatency` | `Failed` | `D4Scoped` |
| `*` | `Idle` | `IdleCpu` | `Failed` | `NonD4` |
| `*` | `Target`, `Idle`, `TwiceTarget` | `MaximumRss` | `Failed` | `NonD4` |
| `*` | `Sustained`, `Burst`, `FallbackRescan` | `MaximumRss` | `Failed` | `D4Scoped` |
| `*` | `Sustained`, `Burst` | `WorkloadAdmission` | `Failed` | `D4Scoped` |
| `*` | `TwiceTarget` | `WorkloadAdmission` | `Failed` | `NonD4` |
| `Final` | `Sustained`, `Burst` | `SupportedLoadDegradation` | `Failed` | `NonD4` |
| `Final` | `TwiceTarget` | `MissingDegradation` | `Failed` | `NonD4` |

`Startup` maximum RSS remains recorded as diagnostic evidence but is deliberately
absent from this binding table; only Startup latency is a failure there. The
table explicitly prevents `Final/{Sustained,Burst}/SupportedLoadDegradation`,
`Final/TwiceTarget/MissingDegradation`, or `TwiceTarget/WorkloadAdmission` from
authorizing D4. If the same supported-load trial also misses screen latency,
admission, or RSS, those distinct existing `D4Scoped` rows are evaluated
independently; publication-state degradation alone is not a section 15 target
miss.

The machine-comparable baseline ID is exactly
`sha256:v1:<production_subject_sha>:<harness_sha>:<workload_schema_sha256>`.
`harness_sha` is not a source-file digest or a caller-selected label: it is
exactly the lowercase 40-hex clean `preflight_head` recorded and revalidated by
every `RunnerControlEvidenceV1` for that run. The baseline may therefore retain
the pinned production subject while binding the separately reviewed harness
revision that produced its evidence.
Later stages retain the v1 manifest hash and the untouched baseline ID as the
comparison identity while recording their own production subject and harness
SHA fields. A `Baseline` run is valid only for the pinned production subject and
without a baseline root. `PostReliability` and `Final` runs receive an explicit
baseline results root, load
the matching `<root>/<mapped-snake-case-scenario>/result-v1.json` plus its raw
trial root through the same typed validator, and require: valid `Pass` or
`Failed` status, baseline
production subject exactly `9cd98131038a53b6dd36ff53e9b89825acba70ae`, the
v1 manifest digest, a baseline harness SHA equal to every selected baseline
control artifact's clean `preflight_head`, and the baseline ID
re-derived from those fields. All seven documents in an `all` run must carry one
identical baseline ID. Candidate composition copies only that validated ID; it
never derives a replacement from the candidate harness. `controlled_environment`
accepts only the sorted fixed keys `HOME=/home/mageyuki`,
`RUSTUP_HOME=/home/mageyuki/.rustup`, `CARGO_HOME=/home/mageyuki/.cargo`,
`PATH=/usr/bin:/bin`, `LC_ALL=C`, and `TZ=UTC`, plus
`HERDR_PERF_BASELINE_RESULTS_ROOT`, `HERDR_PERF_SCENARIO`, `HERDR_PERF_STAGE`, and
`HERDR_PERF_SUBJECT`; the baseline key is absent only for the untouched baseline
run. Unknown or inherited environment keys invalidate composition.
`RunControlsV1.build_environment` records the exact build allowlist. Each raw
`HarnessTrialV1.child_controls.measured_environment` records the exact
measured-child allowlist including its deterministic per-trial plumbing values;
the child also records its observed effective affinity/address-space limit and
the scratch-root string it received. The composer preserves those raw bytes and
copies only the validated `RunnerControlEvidenceV1.trial` field as
`TrialControlEvidenceV1`: canonical scratch/storage verification, the
separate closed orchestrator and observer environment assignments, the closed
validator-environment template,
and the executable identities revalidated for that trial. The validator requires
the build, measured-child, orchestrator, observer, and validator-template maps to equal
the exact allowlists supplied after direct-process environment clearing, byte
for byte; optional downstream frozen canonical `env -i` invocations must receive
the same maps. No authoritative evidence producer or validator may inherit an
unrecorded variable.
The validator template deliberately excludes its candidate path, final path,
and composer-transport status, which do not exist when the pre-composition
control artifact is written. The validator entrypoint validates those three
dynamic values separately through its exact callable contract; they are never
retroactively attributed to a trial artifact.
It requires every
child effective value to equal the run request, every raw scratch root to equal
the deterministic trial path, and every outer scratch/executable observation to
match independently derived evidence. Run-invariant controls never contain a
trial output, socket, handshake, or scratch path; per-trial paths are required to
differ where the layout says they differ and are never compared as run-global
values. `authoritative_executables` is unique and sorted by requested path and
must contain the exact closed v1 inventory from Step 5. Every recorded trial's
`revalidated_executables` must be byte-identical to that run vector; warm-up
rehashes are retained as raw orchestration evidence but are not composed into a
recorded trial. Each recorded trial's `revalidated_runner_script` must be
byte-identical to the run's runner-script identity, and its
`revalidated_measured_binary` must be byte-identical to the run's measured-binary
identity.
`RunControlsV1` is the sole owner of `toolchain_launcher`, `toolchain_name`,
`rustc_version`, `cargo_version`, `build_environment`, `cargo_configuration`,
`measured_binary`, `runner_script`, and `authoritative_executables`;
`ReferenceRunV1` contains no
duplicate toolchain/version fields. Baseline transfer, later-stage composition,
the Task 8 classifier, and every equality check consume those exact values only
through `document.controls`. `toolchain_launcher` must equal exactly one entry in
`authoritative_executables` by all three `ExecutableIdentityV1` fields, and no
second rustup requested or canonical identity may occur. Mutation tests change
each sole-owner field in turn, reject a missing/duplicate/mismatched launcher
inventory entry, and assert the serialized run contains only the one
`controls.rustc_version` and one `controls.cargo_version` owner.
`HERDR_PERF_OUTPUT` is deliberately not a run-level control because every trial
has a different path. The runner derives it exactly
as `<attempt-root>/<mapped-scenario>/warm-up-<four-digit-index>/harness.json` or
`<attempt-root>/<mapped-scenario>/trial-<four-digit-index>/harness.json` (the
single warm-up is `warm-up-0000`, recorded trials begin at `trial-0001`); the
composer validates that canonical path against the trial kind/index and the
corresponding raw-artifact digest. The measured-child map includes
`HERDR_PERF_OBSERVER_HANDSHAKE`, `HERDR_PERF_OBSERVER_CONTROL_SOCKET`, and the
canonical `HERDR_PERF_SCRATCH_ROOT`.
`HERDR_PERF_OBSERVER_CONTROL_OUTPUT` and `HERDR_PERF_PROCESS_TREE_OUTPUT` belong
only to the observer map; they are not silently attributed to the measured
child. All of these are per-trial raw-path plumbing, are covered by the
corresponding handshake/control/output artifact hashes, and are absent from the
run-level controlled-environment map.

`ReferenceOutcomeV1` is the only final-file top level. `Pass` and `Failed` wrap a
fully validated `ReferenceRunV1`; `Invalid` wraps the smaller, independently
validated `InvalidRunV1`, whose failure reasons must be a nonempty, sorted,
duplicate-free subset of `ControlMismatch`, `CommandFailed`, `IncompleteTrial`,
`SequenceLoss`, `StructuralLoss`, `DuplicateOutcome`, or `InvalidArtifact` that
is applicable in the manifest table. The invalid envelope contains no
guessed trial aggregates. Its `baseline_id` is `Some` only after that identity
has passed typed baseline validation; failures before that point carry `None`
rather than copying an untrusted or invented value.
The validator rejects a `Pass` with misses, a `Failed` without the exact miss
set, an invalid-only reason in a valid run, or a threshold reason in an invalid
envelope. Thus a final file always distinguishes pass, measured failure, and
invalid evidence without pretending that malformed evidence is a measurement.
The same portable module implements `classify_d4_checkpoint` over validated
outcomes. It rejects any `Invalid` outcome before classification and returns no
authorization only when all seven `Pass`/`Failed` outcomes have zero failures.
Any remaining threshold or acceptance failure follows the exact manifest row and
returns a nonempty set of required amendments while preserving simultaneous D4
and non-D4 follow-up. Its decision inputs are re-derived checked `u128` sums,
never the report-only parts-per-million field.

Use the nearest-rank percentile definition:

```rust
pub fn percentile(sorted: &[u64], percentile: u32) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((sorted.len() as u128 * percentile as u128) + 99) / 100;
    sorted.get(rank.saturating_sub(1) as usize).copied()
}
```

The validator first rejects duplicates in every sequence/identity vector, then
compares exact sets and exact final identities. It creates only synthetic labels
and IDs. It must never load a
provider transcript, prompt, token, credential, home-directory session file, or
root configuration file.

Implement these scenario operations explicitly in `tests/workload_harness.rs`:

```rust
fn measure_screen_update(profile: WorkloadProfile) -> Vec<LatencyObservationV1>;
fn measure_input_response(model: DomainModel) -> Vec<InputLatencyObservationV1>;
fn prepare_startup_store(root: &StateRoot, retained_events: usize) -> Result<(), HarnessError>;
fn measure_startup(root: &StateRoot)
    -> Result<(u64, ScopedTimingObservationV1), HarnessError>;
fn measure_fallback_rescan(root: &StateRoot)
    -> Result<(Vec<FallbackPairObservationV1>, Vec<ScopedTimingObservationV1>), HarnessError>;
fn measure_d4_and_reducer_publish(model: &DomainModel) -> Vec<ScopedTimingObservationV1>;
```

`tests/common/workload.rs` owns only reusable manifest/schema/oracle types and
deterministic fixture builders. All six `measure_*`/`prepare_*` operations above,
including their Task 1B.3 production-admission and Task 1B.2
`WorkloadFrameDriver` adapters, remain serially owned by
`tests/workload_harness.rs`; those tasks do not need to
edit `tests/common/workload.rs`.
The startup, fallback, and D4/publish operations consume only Task 1B.3's actual
`src/reducer.rs` hook observations; they may not time a second D4 call or external
model clone as a surrogate.
Each observation contains checked sums plus exact segment counts. A
`ControllerEvent` covers the real `validate_controller_event` through successful
`commit_staged` publication and requires two D4 segments (scratch construction and
post-transition diagnostics) plus two clone/publication segments (scratch
`watch::channel` initialization and committed publish). `StartupRestore` covers
`new_with_operator`, requiring one D4 segment and the initial
`watch::channel(Arc::new(model.clone()))` segment. Each fallback arm covers its
real `apply_observation` call and requires one D4 plus one private-publish segment.
Missing, extra, nested, rejected-without-publication, or cross-sequence segments
invalidate the observation; scalar durations alone cannot hide a call-count gap.

Every run records and validates this exact render surface: `160x48`, execution
tree, follow enabled, empty filter, initial selection `run-0200`, no collapsed
runs, and no overlay. Baseline/candidate composition rejects any surface
mismatch. The target builder makes `run-0200` the deterministic last row, so the
follow viewport keeps the measured row visible; input trials begin from the same
surface and record the deterministic selection reached by each navigation key.

Task 1B.3 records screen timing at the successful bounded-queue admission returned
by its test-feature adapter, not immediately before reducer application. The
producer submits independently at the frozen offsets while the consumer services
requests in queue order. A feature-only `WorkloadFrameDriver` in `src/tui/app.rs`
polls the same model/quality/diagnostic watches through `App::refresh_if_changed`,
uses the existing `FrameLimiter::{ready,record,poll_duration}` at the production
100-ms interval, and calls the real `App::render` into `TestBackend`; it never
draws directly after a reducer outcome or serializes queue service behind a draw.
At the start of every phase-bearing trial it produces and records one
production-scheduled priming frame, checked-subtracts the exact manifest phase
offset from the 100-ms interval, checked-adds that complement to the priming
timestamp to derive `workload_origin_ns`, and waits until that schedule epoch
before beginning the reducer schedule or injecting the first key. Because screen
probes are 200 ms apart, they retain that phase within a trial; the five recorded
trials cover the complete 10/30/50/70/90-ms phase rotation. For input, after each
observed response frame the driver checked-adds the same checked complement to
obtain the next `scheduled_ns`, waits until at least that timestamp, and only then
records `injected_ns` and injects the next key, so all 200 samples record a controlled phase rather than an
uninitialized/immediate first draw. The priming frame is not a latency sample.

Only frozen screen-probe sequences advance the selected sentinel row: every 4th
sequence for Sustained, every 20th for Burst, and every 8th for TwiceTarget,
which spaces probes exactly 200 ms apart and yields 300, 50, and 300 observations.
Other sequences mutate deterministic non-sentinel runs. A probe applies a valid
Controller event whose sentinel label contains a monotonic cumulative frontier,
for example `Task Run: run-0200 [probe-through:0008]`. The latest model therefore
acknowledges every probe sequence at or below that frontier even when a Tokio
watch coalesces intermediate values. The render recorder expands each newly
observed frontier into all previously unseen frozen probe sequences, rejects
regression, a frontier absent from the frozen probe vector, a frontier beyond the
terminally processed sequence, or a duplicate acknowledgement, and never
requires the producer to wait for a draw.
Every sequence expanded from one frame receives that frame's one real
`rendered_ns`; no synthetic draw timestamp is invented.
`rendered_sequences` must equal the exact probe vector;
submitted/admitted/completed/persisted vectors must each equal the complete
`1..=N` vector. A deliberate frame-driver stall longer than 200 ms must still
recover every coalesced probe from the latest cumulative frontier, so watch
coalescing cannot be misclassified as event loss and cannot silently discard a
screen sample.

The frozen JSON object for every event explicitly contains `schema_version: 1`,
unique `event_id`, integer `emitted_at_ms`, `source:
"increment5-harness"`, `event_type`, `task_run_id`, and the optional keys
`parent_task_run_id`, `depends_on_id`, `label`, `reason`, `provider`,
`native_session_id`, and `terminal_id` with JSON null. It contains no `v`,
`type`, `subject`, or `progress`. The parser-acceptance test checks this exact
field set before measurement. After the required priming/phase wait, input timing starts immediately before
`App::handle_key` and ends only when the same `WorkloadFrameDriver` produces the
first production-scheduled frame reflecting the new selection/view state; direct
post-key rendering is prohibited. Startup preparation writes exactly 100,000
unique retained activity rows into `events` with `gap_kind IS NULL`, with the
matching 100,000 deduplication rows in `event_ledger`, all for one retained
synthetic session plus target topology through store APIs. It closes the writer,
queries and asserts both prepared counts, serializes them as
`prepared_non_gap_event_count == Some(100_000)` and
`prepared_ledger_row_count == Some(100_000)`, and measures a fresh child process from
open/restore to its first usable frame. Within that measured constructor/restore
path, the child proves the restored operator exposes exactly
the manifest's frozen `operator_activity_limit` of `10_000` ordered activity
entries, serializes that value as `restored_activity_count`, and proves the
frozen operator semantics;
ledger-only or collector-gap rows cannot satisfy the setup. All three count
fields are `None` outside `Startup`.
Fallback trials pair the same synthetic provider append once through normal
notification and once with notification creation forced unavailable; each raw
pair carries the shared sequence, requires `rescan_ns >= notification_ns`, and
derives `rescan_ns.checked_sub(notification_ns)`; underflow is
`InvalidArtifact`, while a valid checked difference must be at most two seconds.
Scoped D4 timing records per-event D4 analysis,
reducer-through-model-publication, clone/publish, and render samples on matching
sequence IDs. Startup records sequence 1 around
`Reducer::new_with_operator`, with isolated
`dangling_announcement_components` time and the complete restore-through-initial
publication denominator. Each fallback arm records its own matching scoped
sample around the provider observation's `apply_observation` path. No aggregate
substitutes for raw observations or end-to-end latency.

Freeze the reducer streams as follows: `SustainedTarget`, `TargetBurst`, and
`TwiceTarget` use zero-padded event IDs, `schema_version: 1`, `source:
"increment5-harness"`, null relationship/provider/native/terminal fields, and no
progress value. Exactly the frozen probe sequences—300, 50, and 300
respectively—send the cumulative-frontier Controller events above to `run-0200`.
Every non-probe sequence deterministically targets a
non-sentinel run selected by its sequence modulo `run-0001..run-0199`; it may
never target `run-0200`. The three profiles use respectively 1,200 events at
50 ms, 1,000 at 10 ms, and 2,400 at 25 ms. The test enumerates every target,
asserts the exact sentinel counts `300/50/300`, asserts zero non-probe sentinel
targets, checks that the parser accepts the literal frozen JSON, requires every
admitted sequence to reach a terminal reducer outcome, and requires every frozen
probe to appear in the cumulative frontier of a production-scheduled frame.
`TargetTopology` renders the exact restored target.
`Startup` uses one fresh process and store per observation. `Idle` settles for
five seconds and then records 30 seconds without new input. `FallbackRescan`
uses five notification/rescan pairs per recorded trial. Input response uses 200
alternating deterministic navigation keys per trial.

The validator implements this closed applicability matrix; “exact identities”
means the four final identity vectors match the frozen oracle. Every valid
`Pass`/`Failed` row requires at least one external process-tree resource sample,
one complete thirteen-entry per-trial artifact-digest set,
`TrialStatusV1::Ok`, and a `pidstat_exit_status` consistent with the run's
calibrated `PidstatChildStatusModeV1`. Target, Sustained, Burst, and TwiceTarget are the
phase-bearing rows and require exactly one priming frame; all other rows require
zero:

| Scenario | Recorded trials | Required observations/outcomes | Empty or `None` fields | Binding checks |
| --- | ---: | --- | --- | --- |
| `Target` | 5 | 200 input observations, one workload origin, and the manifest trial phase; exact identities | admission, screen, reducer-lag, publish-to-render, startup, fallback, scoped, all sequence vectors, performance evidence stream, idle window | input p95, complete five-trial phase rotation, simultaneous RSS |
| `Sustained` | 5 | 1,200 admission observations, 300 screen observations on exact probe sequences, 300-sample reducer-lag and publish-to-render distributions, 1,200 scoped observations, one workload origin, and the manifest trial phase; submitted/admitted/completed/persisted are exactly `1..=1_200`, rendered is exactly the probe vector; exact identities. `Final` additionally requires the complete anchored sample/frame/terminal stream | input, startup, fallback, idle window; performance evidence stream is absent before `Final` | admission deadlines, screen p95, checked diagnostic latency derivations, complete five-trial phase rotation, losslessness, simultaneous RSS, and final supported-load degradation count `== 0`; threshold misses are D4-scoped, but `SupportedLoadDegradation` is NonD4 |
| `Burst` | 5 | 1,000 admission observations, 50 screen observations on exact probe sequences, 50-sample reducer-lag and publish-to-render distributions, 1,000 scoped observations, one workload origin, and the manifest trial phase; submitted/admitted/completed/persisted are exactly `1..=1_000`, rendered is exactly the probe vector; exact identities. `Final` additionally requires the complete anchored sample/frame/terminal stream | input, startup, fallback, idle window; performance evidence stream is absent before `Final` | admission deadlines, screen p95, checked diagnostic latency derivations, complete five-trial phase rotation, losslessness, simultaneous RSS, and final supported-load degradation count `== 0`; threshold misses are D4-scoped, but `SupportedLoadDegradation` is NonD4 |
| `Startup` | 10 | exactly one startup observation and one sequence-1 scoped observation from a fresh process/store; `prepared_non_gap_event_count == Some(100_000)`, `prepared_ledger_row_count == Some(100_000)`, and `restored_activity_count == Some(10_000)` from the manifest; exact identities | workload origin, frame phase, admission, screen, input, fallback, reducer-lag, publish-to-render, all sequence vectors, performance evidence stream, idle window | startup latency; whole-wrapper RSS is diagnostic only; latency is D4-scoped |
| `Idle` | 5 | resource samples spanning one exact window of at least 30 seconds after the five-second settle; start/end boundaries and per-identity start/final ticks; exact identities | workload origin, frame phase, admission and all latency/scoped and sequence fields, including reducer-lag and publish-to-render; performance evidence stream | idle-window CPU delta and simultaneous RSS |
| `FallbackRescan` | 5 | exactly five unique-sequence fallback pairs and ten scoped samples keyed by arm plus sequence; each pair has separate notification/rescan identities and both equal the oracle | workload origin, frame phase, admission, screen, reducer-lag, publish-to-render, input, startup, all general sequence vectors, performance evidence stream, idle window | each rescan-minus-notify pair `<= 2 s`, both-arm losslessness, simultaneous RSS; D4-scoped |
| `TwiceTarget` | 5 | 2,400 admission observations, 300 screen observations on exact probe sequences, 300-sample reducer-lag and publish-to-render distributions, 2,400 scoped observations, one workload origin, and the manifest trial phase; submitted/admitted/completed/persisted are exactly `1..=2_400`, rendered is exactly the probe vector; exact identities. `Final` additionally requires the complete anchored sample/frame/terminal stream and an optional selected terminal draw ordinal. An attained workload with no qualifying draw by the closed deadline is valid `Failed(MissingDegradation)` | input, startup, fallback, idle window; performance evidence stream is absent before `Final` | admission deadlines, checked diagnostic latency derivations, complete five-trial phase rotation, losslessness and simultaneous RSS in every stage; final-only visible performance degradation by 60 seconds; threshold misses are D4-scoped, but above-target lag and `MissingDegradation` are not target misses |

Every scenario has exactly one unrecorded warm-up. `ExternalResourceAuditV1`
is required in every recorded trial. `pidstat_sample_count == 0` is valid for a
sub-one-second non-idle trial only when all three optional pidstat aggregates are
`None`; Idle requires at least one pidstat sample. Inapplicable aggregates are
`None`, inapplicable raw vectors are empty, and any value outside this matrix is
`InvalidArtifact`, never silently ignored.
The three prepared/restored count fields are required with the exact `Startup`
values above and are `None` in all other scenario rows. The Startup helper itself
queries and asserts the restored count in-process before writing its raw result;
the composer independently validates all three serialized values.
`performance_evidence_stream` is required only for `Final` `Sustained`, `Burst`,
and `TwiceTarget`; it is `None` for every Baseline/PostReliability result and every
other scenario. `workload_start_ns == workload_origin_ns`, but the sample vector
begins with the raw carry-in publication already cached by `App` before the
priming frame establishes that origin. Its `sampled_at_ns` may precede the
origin. Install the feature-only raw observer before starting the monitor, wait
for the first stamped publication to be refreshed into `App`, perform the one
real priming frame, and only then establish `workload_origin_ns` and the first
ordinary draw bound. `first_sample_ordinal` is that cached carry-in ordinal.

The raw sample vector contains every observer-linearized monitor sample from the
carry-in through an independently frozen closing watermark, including samples
whose render payload is equal and therefore never advances the watch version.
The frame vector contains every ordinary production-scheduled
`WorkloadFrameDriver` draw at or after the origin through the first draw after
both the manifest schedule interval and all admitted events' terminal outcomes.
That closing draw is caused by the ordinary monitor publication after the last
terminal outcome and still passes through the real limiter; it is not a direct
or synthetic renderer call. `workload_close_ns` equals that draw's actual
`rendered_at_ns`, is strictly after the applicable schedule deadline, and is no
earlier than every terminal timestamp.

The monitor assigns each `sample_ordinal` before invoking the feature-only
synchronous observer and before conditionally publishing the same stamped
composite watch value. The first real feature-build sample is force-published
because the initial stamp is `None`; a later equal render payload is raw-only and
must leave the watch payload, stamp, version, and `has_changed()` state untouched.
The frame driver assigns `draw_ordinal` once before each production-scheduled
draw. After the closing render succeeds, the scheduler freezes
`next_sample_ordinal` under the same recorder lock that linearizes observer
appends. The bound is the recorder's next contiguous ordinal after its last
completed append, never a separately read producer counter; a producer that has
assigned an ordinal but has not entered the observer is therefore outside the
frozen interval. The scheduler then freezes `next_draw_ordinal`; observer calls
after that lock boundary are outside the closed stream rather than silently
omitted from its interval.
Serialization never derives or renumbers the four half-open bounds
`first_sample_ordinal..next_sample_ordinal` and
`first_draw_ordinal..next_draw_ordinal`.

Validation requires nonempty exact contiguous and strictly increasing ordinal
coverage of both intervals, nondecreasing producer-authored timestamps, the
first entries at the named anchors, and the final entries at
`next_*_ordinal - 1`. Equal clock readings remain unchanged; ordinals, never a
synthetic `+1 ns`, establish strict order. Deleting an interior or suffix entry,
shifting an anchor, or truncating/changing a closure watermark is therefore
invalid rather than a smaller plausible stream.

Immediately before the real `App::render`, the frame driver clones the complete
`PerformancePublication` currently cached by `App`; that same clone supplies the
frame evidence after the render succeeds. Direct watch borrowing at draw time is
forbidden. A frame may repeat a prior published sample ordinal and may skip raw
ordinals that were coalesced, but its `sample_ordinal` must name an exact raw
record in this stream whose `sampled_at_ns`, effective quality, and ordered
reasons match the cached publication. It repeats that timestamp as
`state_observed_at_ns` and records the raw complete fixed-surface header without
normalizing it. `rendered_at_ns` is nondecreasing and no earlier than the named
sample. Every admitted sequence has exactly one terminal observation, joined
to the admission by sequence with `terminal_ns >= admitted_ns`; the complete
terminal vector, admissions, and frozen limits re-derive each sample's pending
set, high-water marks, rolling one/ten/sixty-second counts, event lag, structural
counts, ordered performance reasons, and effective quality using checked
arithmetic. Any gap, duplicate, impossible reference, false reason, missing
reason, or incoherent source/effective quality is `InvalidArtifact`. Header
interpretation is scenario-specific below: a claimed match must be exact, while
truthfully recorded absence can be a measured failure rather than corrupt
evidence.

For `Final` `Sustained` and `Burst`, the authoritative source quality must remain
`Live`. An empty re-derived performance-reason vector and `Live` effective
quality at every sample/frame, paired with the exactly derived `LIVE` header and
empty `perf:` field at every draw, passes the supported-load publication invariant.
Because these runs require a healthy source, a header that disagrees with its
coherent sample is invalid evidence rather than a second failure taxonomy.
One or more truthfully re-derived performance reasons produce the valid measured
failure `SupportedLoadDegradation`; the matching
`PerformanceDegradation/Count` numerator is the number of such samples and must
equal zero to pass. This failure is `NonD4`: it enforces the approved exact-boundary
non-degradation completion criterion but is not itself a section 15 latency,
admission, or RSS miss. A non-`Live` source state, or a non-`Live` effective state
with no performance reason, is invalid control evidence rather than this measured
failure.

For `Final` `TwiceTarget`, exact count 1,200 is within the envelope and admission
1,201 is the first possible `EventsSixtySeconds` transition. The optional
`selected_terminal_draw_ordinal` must name the lowest draw ordinal after attained
admission 1,201 whose joined sample has effective `Degraded`, contains
`EventsSixtySeconds`, and whose complete header contains the exactly derived
`events_60s` label. That frame must not precede its joined sample and must be no
later than checked `workload_origin_ns + 60_000_000_000`. The actual
sample-to-render duration remains a checked diagnostic; the production limiter
defines a minimum frame interval, not a 100-ms completion deadline. A valid
`MissingDegradation` failure leaves the ordinal `None`, closes the contiguous
stream strictly beyond the deadline, and proves no qualifying draw through the
deadline. Samples and frames remain coherent even when the raw rendered header
does not contain the required complete label; that observed display absence is
the measured failure. A later draw cannot repair the miss. Exact earliest-match and
true-absence are derived over the complete ordinal stream rather than a retained
diagnostic subset.
For `Final` `TwiceTarget`, evaluate admission attainment first. If any bucket
misses, preserve truthful frame evidence but derive
`WorkloadAdmission`, not `MissingDegradation`; an under-driven workload cannot
prove or disprove the required overload transition. Only an attained workload
may derive `MissingDegradation` from absence through the deadline.

- [ ] **Step 4: Add result validation and losslessness tests**

Write tests named:

```rust
#[test]
fn result_document_rejects_loss_and_incomplete_trials() {
    let mut result = valid_synthetic_result();
    result.document_mut().trials[0].raw.completed_sequences
        .retain(|sequence| *sequence != 7);
    assert_eq!(result.validate(), Err(ResultError::SequenceCoverage));
    let mut result = valid_synthetic_result();
    result.document_mut().trials.clear();
    assert_eq!(result.validate(), Err(ResultError::IncompleteTrials));
}

#[test]
fn result_document_rejects_duplicates_and_inconsistent_aggregates() {
    let mut duplicate = valid_synthetic_result();
    duplicate.document_mut().trials[0].raw.completed_sequences.push(7);
    assert_eq!(duplicate.validate(), Err(ResultError::DuplicateOutcome));
    let mut inconsistent = valid_synthetic_result();
    inconsistent.document_mut().trials[0]
        .screen_update.as_mut().unwrap().p95_ns += 1;
    assert_eq!(inconsistent.validate(), Err(ResultError::InvalidArtifact));
    let mut reducer_lag = valid_synthetic_result();
    reducer_lag.document_mut().trials[0]
        .reducer_lag.as_mut().unwrap().p95_ns += 1;
    assert_eq!(reducer_lag.validate(), Err(ResultError::InvalidArtifact));
    let mut publish_to_render = valid_synthetic_result();
    publish_to_render.document_mut().trials[0]
        .publish_to_render.as_mut().unwrap().median_ns += 1;
    assert_eq!(publish_to_render.validate(), Err(ResultError::InvalidArtifact));
    let mut missing_reducer_lag = valid_synthetic_result();
    missing_reducer_lag.document_mut().trials[0].reducer_lag = None;
    assert_eq!(missing_reducer_lag.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn latency_observations_require_joined_monotonic_timestamps() {
    assert!(valid_synthetic_result().validate().is_ok());

    let mut missing = valid_synthetic_result();
    let sequence = missing.document().trials[0].raw.screen_observations[0].sequence;
    missing.document_mut().trials[0].raw.admission_observations
        .retain(|observation| observation.sequence != sequence);
    assert_eq!(missing.validate(), Err(ResultError::InvalidArtifact));

    let mut mismatch = valid_synthetic_result();
    mismatch.document_mut().trials[0].raw.screen_observations[0].admitted_ns += 1;
    assert_eq!(mismatch.validate(), Err(ResultError::InvalidArtifact));

    let mut pre_admission_terminal = valid_synthetic_result();
    let admitted = pre_admission_terminal.document().trials[0]
        .raw.screen_observations[0].admitted_ns;
    pre_admission_terminal.document_mut().trials[0]
        .raw.screen_observations[0].terminal_ns = admitted - 1;
    assert_eq!(pre_admission_terminal.validate(), Err(ResultError::InvalidArtifact));

    let mut pre_admission_publish = valid_synthetic_result();
    let admitted = pre_admission_publish.document().trials[0]
        .raw.screen_observations[0].admitted_ns;
    pre_admission_publish.document_mut().trials[0]
        .raw.screen_observations[0].published_ns = admitted - 1;
    assert_eq!(pre_admission_publish.validate(), Err(ResultError::InvalidArtifact));

    let mut pre_effect_render = valid_synthetic_result();
    let floor = pre_effect_render.document().trials[0].raw.screen_observations[0]
        .terminal_ns.max(pre_effect_render.document().trials[0]
            .raw.screen_observations[0].published_ns);
    pre_effect_render.document_mut().trials[0]
        .raw.screen_observations[0].rendered_ns = floor - 1;
    assert_eq!(pre_effect_render.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn frame_phase_is_derived_from_actual_timestamps_and_desired_schedule() {
    let mut copied = valid_synthetic_result();
    copied.document_mut().trials[0].raw.screen_observations[0]
        .observed_frame_phase_ns ^= 1;
    assert_eq!(copied.validate(), Err(ResultError::InvalidArtifact));

    let mut wrong_complement = valid_synthetic_result();
    wrong_complement.document_mut().trials[0].raw.admission_observations[0]
        .scheduled_ns += 1;
    assert_eq!(wrong_complement.validate(), Err(ResultError::InvalidArtifact));

    let mut input = valid_target_input_result();
    input.document_mut().trials[0].raw.input_observations[0]
        .observed_frame_phase_ns ^= 1;
    assert_eq!(input.validate(), Err(ResultError::InvalidArtifact));
    let mut input_schedule = valid_target_input_result();
    input_schedule.document_mut().trials[0].raw.input_observations[1]
        .scheduled_ns += 1;
    assert_eq!(input_schedule.validate(), Err(ResultError::InvalidArtifact));

    for invalid_phase in [0, 100_000_000] {
        let mut invalid = valid_synthetic_result();
        invalid.document_mut().trials[0].raw.frame_phase_offset_ns =
            Some(invalid_phase);
        assert_eq!(invalid.validate(), Err(ResultError::InvalidArtifact));
    }
}

#[test]
fn fallback_pairs_and_decimal_rss_threshold_fail_closed() {
    let mut fallback = valid_fallback_result();
    fallback.document_mut().trials[0].raw.fallback_pairs[0].rescan_ns += 2_000_000_001;
    assert_eq!(fallback.validate(), Err(ResultError::Threshold));
    let mut reversed = valid_fallback_result();
    let rescan = reversed.document().trials[0].raw.fallback_pairs[0].rescan_ns;
    reversed.document_mut().trials[0].raw.fallback_pairs[0].notification_ns =
        rescan.checked_add(1).unwrap();
    assert_eq!(reversed.validate(), Err(ResultError::InvalidArtifact));
    let mut notification_loss = valid_fallback_result();
    notification_loss.document_mut().trials[0].raw.fallback_pairs[0]
        .notification_final_identities.task_run_ids.pop();
    assert_eq!(notification_loss.validate(), Err(ResultError::StructuralMismatch));
    let mut rescan_loss = valid_fallback_result();
    rescan_loss.document_mut().trials[0].raw.fallback_pairs[0]
        .rescan_final_identities.task_run_ids.pop();
    assert_eq!(rescan_loss.validate(), Err(ResultError::StructuralMismatch));
    let mut rss = valid_synthetic_result();
    rss.document_mut().trials[0].maximum_process_tree_rss_bytes = 100_000_000;
    assert_eq!(rss.validate(), Err(ResultError::Threshold));
}

#[test]
fn raw_artifact_digest_and_scenario_matrix_fail_closed() {
    let valid = valid_synthetic_result();
    assert!(valid.validate().is_ok());
    assert!(validate_with_raw_root(&valid, fixture_raw_root()).is_ok());

    let mut result = valid_synthetic_result();
    result.document_mut().trials[0].raw_artifacts.harness_json_sha256 =
        "sha256:wrong".to_owned();
    assert_eq!(validate_with_raw_root(&result, fixture_raw_root()),
        Err(ResultError::InvalidArtifact));
    let mut control_digest = valid_synthetic_result();
    control_digest.document_mut().trials[0]
        .raw_artifacts.runner_control_json_sha256 = "sha256:wrong".to_owned();
    assert_eq!(validate_with_raw_root(&control_digest, fixture_raw_root()),
        Err(ResultError::InvalidArtifact));
    let mut result = valid_synthetic_result();
    result.document_mut().trials[0].raw.startup_observations_ns.push(1);
    assert_eq!(result.validate(), Err(ResultError::InvalidArtifact));
}

#[test]
fn control_ownership_accepts_distinct_trial_paths_and_rejects_drift() {
    let valid = valid_synthetic_result_with_distinct_trial_paths();
    assert_eq!(valid.document().trials.len(), 5);
    assert_all_trial_output_socket_and_scratch_paths_are_distinct(&valid);
    assert!(valid.validate().is_ok());
    assert_eq!(result_with_reused_trial_path().validate(),
        Err(ResultError::InvalidArtifact));
    assert_eq!(result_with_child_affinity_drift().validate(),
        Err(ResultError::InvalidArtifact));
    assert_eq!(result_with_measured_key_in_observer_environment().validate(),
        Err(ResultError::InvalidArtifact));
}

#[cfg(target_os = "linux")]
#[test]
fn cargo_configuration_and_executable_provenance_fail_closed() {
    assert!(reject_all_cargo_config_fixture(CargoConfigFixture::LegacyName));
    assert!(reject_all_cargo_config_fixture(CargoConfigFixture::BothNames));
    assert!(reject_all_cargo_config_fixture(CargoConfigFixture::CargoHome));
    assert!(reject_all_cargo_config_fixture(CargoConfigFixture::ExternalAncestor));
    assert!(hostile_path_fixture_executes_no_sentinel());
    assert_eq!(result_with_changed_executable_digest().validate(),
        Err(ResultError::InvalidArtifact));
}

#[test]
fn startup_counts_are_serialized_exactly_and_rss_is_diagnostic() {
    let startup = valid_startup_result();
    let raw = &startup.document().trials[0].raw;
    assert_eq!(raw.prepared_non_gap_event_count, Some(100_000));
    assert_eq!(raw.prepared_ledger_row_count, Some(100_000));
    assert_eq!(workload_schema().operator_activity_limit, 10_000);
    assert_eq!(raw.restored_activity_count,
        Some(workload_schema().operator_activity_limit));
    assert!(startup.validate().is_ok());
    assert_eq!(startup_with_missing_or_wrong_count().validate(),
        Err(ResultError::InvalidArtifact));
    assert_eq!(non_startup_with_preparation_count().validate(),
        Err(ResultError::InvalidArtifact));
    assert!(valid_startup_with_high_diagnostic_wrapper_rss()
        .validate().is_ok());
}

#[test]
fn exact_target_boundaries_are_valid_harness_inputs() {
    let oracle = workload::oracle(WorkloadProfile::TargetTopology);
    assert_eq!((oracle.live_panes, oracle.visible_runs, oracle.dependency_edges), (50, 200, 1_000));
}

#[test]
fn workload_schema_manifest_has_golden_digest() {
    assert_eq!(workload_schema_sha256(), WORKLOAD_SCHEMA_V1_SHA256);
    assert!(canonical_workload_schema_bytes_are_byte_stable());
}

#[test]
fn render_surface_and_valid_wire_stream_are_closed() {
    let mut result = valid_synthetic_result();
    result.document_mut().render_surface.width += 1;
    assert_eq!(result.validate(), Err(ResultError::InvalidArtifact));
    assert_all_admissions_reach_terminal_outcomes();
    assert_exact_screen_probe_sequences_reach_production_scheduled_frames();
}

#[test]
fn cumulative_probe_frontier_recovers_watch_coalescing_after_draw_stall() {
    let result = run_schedule_with_one_frame_driver_stall(Duration::from_millis(450));
    assert!(result.frames.iter().any(|frame| frame.new_probe_count >= 2));
    assert_eq!(result.rendered_sequences,
        workload::screen_probe_sequences(WorkloadProfile::SustainedTarget));
    assert_eq!(result.submitted_sequences, (1..=1_200).collect::<Vec<_>>());
}

#[test]
fn actual_admission_schedule_is_a_binding_workload_predicate() {
    let mut late = valid_synthetic_result();
    let origin = late.document().trials[0].raw.workload_origin_ns.unwrap();
    let observation = &mut late.document_mut().trials[0].raw.admission_observations[0];
    observation.admitted_ns = origin + 1_050_000_001;
    assert_eq!(late.validate(), Err(ResultError::Threshold));
    let failed = valid_workload_admission_failure();
    assert!(failed.validate().is_ok());
    assert_eq!(failed.document().failure_reasons,
        vec![FailureReasonV1::WorkloadAdmission]);
    let under_driven = valid_twice_target_workload_admission_failure();
    assert_eq!(under_driven.document().failure_reasons,
        vec![FailureReasonV1::WorkloadAdmission]);
    assert!(under_driven.validate().is_ok());
}

#[test]
fn idle_cpu_uses_only_the_measured_window_and_retains_birth_and_exit() {
    let idle = valid_idle_result_with_setup_cpu_mid_window_exit_and_post_window_birth();
    assert_eq!(idle.document().trials[0].elapsed_ns, 30_000_000_000);
    assert_eq!(idle.document().trials[0].user_cpu_ns, expected_idle_delta_ns());
    assert!(idle.document().trials[0].process_tree.process_identity_resources
        .iter().any(|identity| identity.idle_window_end_user_cpu_ticks.is_none()));
    assert!(!idle.document().trials[0].process_tree.process_identity_resources
        .iter().any(|identity| identity.pid ==
            idle.document().trials[0].process_tree.observer_pid));
    assert!(idle.validate().is_ok());
    for invalid in [idle_counter_regression(), idle_missing_start_baseline(),
        idle_missing_end_boundary(), idle_control_transcript_mismatch(),
        idle_reused_pid_with_different_start_time()] {
        assert_eq!(invalid.validate(), Err(ResultError::InvalidArtifact));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn observer_ready_barrier_precedes_measured_setup_and_first_sample() {
    let evidence = observe_fixture_root_with_setup_child_after_ready();
    assert!(evidence.control.observer_ready_ns <= evidence.setup_started_ns);
    assert!(evidence.process_tree.resource_observations[0].offset_ns
        <= evidence.control.observer_ready_ns - evidence.control.trial_origin_ns);
    assert!(evidence.process_tree.process_identity_resources.iter()
        .any(|identity| identity.pid == evidence.setup_child_pid));
}

#[cfg(target_os = "linux")]
#[test]
fn external_process_tree_observer_excludes_itself_but_includes_root_and_children() {
    let evidence = observe_fixture_root_from_sibling_process();
    assert!(evidence.process_identity_resources.iter()
        .any(|identity| identity.pid == evidence.observed_root_pid));
    assert!(evidence.process_identity_resources.iter()
        .any(|identity| identity.pid == fixture_child_pid()));
    assert!(!evidence.process_identity_resources.iter()
        .any(|identity| identity.pid == evidence.observer_pid));
}

#[test]
fn supported_load_stream_is_complete_live_and_non_degraded() {
    for valid in [valid_final_sustained_result(), valid_final_burst_result()] {
        let stream = valid.document().trials[0].raw
            .performance_evidence_stream.as_ref().unwrap();
        assert_exact_half_open_sample_and_draw_ordinals(stream);
        assert_every_admission_has_one_terminal_observation(stream);
        assert!(stream.samples.iter().all(|sample|
            sample.source_quality == EffectiveQualityV1::Live &&
            sample.effective_quality == EffectiveQualityV1::Live &&
            sample.reasons.is_empty()));
        assert!(valid.validate().is_ok());
    }
    let degraded = valid_supported_load_degradation_failure();
    assert_eq!(degraded.document().failure_reasons,
        vec![FailureReasonV1::SupportedLoadDegradation]);
    assert!(degraded.validate().is_ok());
}

#[test]
fn performance_evidence_stream_is_omission_closed_and_rederived() {
    let valid = valid_twice_target_result();
    assert!(valid.validate().is_ok());
    for invalid in [
        performance_stream_with_deleted_interior_sample(),
        performance_stream_with_deleted_frame_suffix(),
        performance_stream_with_sample_or_draw_ordinal_gap(),
        performance_stream_with_shifted_start_anchor(),
        performance_stream_with_truncated_closure_watermark(),
        performance_stream_with_missing_terminal_observation(),
        performance_stream_with_false_reason_or_quality(),
        performance_stream_with_incoherent_frame_sample_reference(),
    ] {
        assert_eq!(invalid.validate(), Err(ResultError::InvalidArtifact));
    }
}

#[test]
fn twice_target_requires_the_earliest_rendered_events_sixty_seconds_reason() {
    let valid = valid_twice_target_result();
    assert_eq!(valid.document().measurement_stage, MeasurementStageV1::Final);
    assert!(valid.validate().is_ok());
    assert!(valid_twice_target_result_with_simultaneous_event_lag()
        .validate().is_ok());
    let early_lag = valid_twice_target_with_early_event_lag_then_events_sixty();
    assert!(early_lag.document().trials[0].raw.performance_evidence_stream
        .as_ref().unwrap().frames.len() > 1);
    assert!(early_lag.validate().is_ok());
    let mut wrong_reason = valid_twice_target_result();
    selected_performance_frame_mut(&mut wrong_reason).reasons =
        vec![PerformanceReasonV1::DependencyEdges];
    assert_eq!(wrong_reason.validate(), Err(ResultError::InvalidArtifact));
    let mut watch_only = valid_twice_target_result();
    selected_performance_frame_mut(&mut watch_only).rendered_header_line.clear();
    assert_eq!(watch_only.validate(), Err(ResultError::InvalidArtifact));
    let missing = valid_twice_target_missing_degradation_failure();
    assert_eq!(missing.document().failure_reasons,
        vec![FailureReasonV1::MissingDegradation]);
    assert!(missing.validate().is_ok());
    let mut fabricated_early = valid_twice_target_result();
    fabricated_early.document_mut().measurement_stage =
        MeasurementStageV1::PostReliability;
    assert_eq!(fabricated_early.validate(), Err(ResultError::InvalidArtifact));
    assert!(valid_post_reliability_twice_target_without_performance_render()
        .validate().is_ok());
}

#[test]
fn twice_target_crossing_frame_is_actual_and_before_deadline() {
    let origin = 9_000_000_000_000;
    let crossing_sampled_at_ns = origin.checked_add(30_025_000_000).unwrap();
    let phased = valid_twice_target_with_crossing_state_and_first_frame(
        origin, crossing_sampled_at_ns, 107_000_000,
    );
    assert!(phased.validate().is_ok());
    let selected = selected_performance_frame(&phased);
    assert_eq!(selected.state_observed_at_ns, crossing_sampled_at_ns);
    assert_eq!(selected.rendered_at_ns,
        crossing_sampled_at_ns.checked_add(107_000_000).unwrap());
    assert_eq!(selected.rendered_at_ns
        .checked_sub(selected.state_observed_at_ns).unwrap(), 107_000_000);
    assert_eq!(twice_target_observation_before_origin(origin).validate(),
        Err(ResultError::InvalidArtifact));
    assert_eq!(twice_target_state_before_threshold_crossing_admission(origin).validate(),
        Err(ResultError::InvalidArtifact));
    assert_eq!(twice_target_first_matching_render_after_deadline(origin).validate(),
        Err(ResultError::InvalidArtifact));
    assert_eq!(twice_target_skips_earlier_complete_matching_frame(origin).validate(),
        Err(ResultError::InvalidArtifact));
    assert_eq!(twice_target_missing_claim_has_matching_diagnostic(origin).validate(),
        Err(ResultError::InvalidArtifact));
    assert_eq!(twice_target_selected_draw_is_not_lowest_matching_ordinal(origin).validate(),
        Err(ResultError::InvalidArtifact));
    assert_eq!(twice_target_deadline_addition_overflow().validate(),
        Err(ResultError::InvalidArtifact));
}

#[test]
fn tagged_outcomes_distinguish_failed_from_invalid() {
    assert!(valid_failed_outcome().validate().is_ok());
    assert!(valid_invalid_outcome(FailureReasonV1::InvalidArtifact).validate().is_ok());
    assert!(valid_invalid_outcome(FailureReasonV1::SequenceLoss).validate().is_ok());
    assert!(valid_invalid_outcome(FailureReasonV1::StructuralLoss).validate().is_ok());
    assert_eq!(invalid_outcome_with_threshold_reason().validate(),
        Err(ResultError::InvalidArtifact));
}

#[test]
fn toolchain_provenance_has_one_controls_owner_and_one_launcher_inventory_entry() {
    let valid = valid_synthetic_result();
    assert_controls_are_the_only_serialized_toolchain_version_owner(&valid);
    assert_eq!(launcher_inventory_matches(&valid), 1);
    assert!(runner_script_identity_matches_every_trial(&valid));
    for mutated in toolchain_control_single_field_mutations(&valid) {
        assert_eq!(mutated.validate(), Err(ResultError::InvalidArtifact));
    }
    for mutated in launcher_missing_duplicate_and_mismatch_mutations(&valid) {
        assert_eq!(mutated.validate(), Err(ResultError::InvalidArtifact));
    }
    for mutated in runner_script_path_mode_digest_and_trial_mutations(&valid) {
        assert_eq!(mutated.validate(), Err(ResultError::InvalidArtifact));
    }
}

#[test]
fn failure_policy_table_is_closed_exhaustive_and_shared() {
    let expected = exact_failure_policy_rows();
    assert_eq!(expected.len(), 18); // exact declared rows in the table above
    assert_eq!(manifest().failure_policy, expected);
    assert_eq!(expanded_failure_policy_tuples().len(), 186);
    assert_eq!(lookup_failure_policy(
        MeasurementStageV1::Baseline,
        ScenarioV1::Target,
        FailureReasonV1::ControlMismatch,
    ), Some(D4PolicyV1::NotApplicable));
    assert_eq!(lookup_failure_policy(
        MeasurementStageV1::Final,
        ScenarioV1::Sustained,
        FailureReasonV1::WorkloadAdmission,
    ), Some(D4PolicyV1::D4Scoped));
    assert_eq!(lookup_failure_policy(
        MeasurementStageV1::Final,
        ScenarioV1::Sustained,
        FailureReasonV1::SupportedLoadDegradation,
    ), Some(D4PolicyV1::NonD4));
    assert_eq!(lookup_failure_policy(
        MeasurementStageV1::Final,
        ScenarioV1::TwiceTarget,
        FailureReasonV1::MissingDegradation,
    ), Some(D4PolicyV1::NonD4));
    assert!(valid_failed_outcome().validate().is_ok());
    assert!(classify_d4_checkpoint(high_d4_miss()).is_ok());
    let absent = failure_policy_absent_tuple();
    assert_eq!(lookup_failure_policy(absent.stage, absent.scenario, absent.reason), None);
    assert_eq!(outcome_with_failure_policy_tuple(absent).validate(),
        Err(ResultError::InvalidArtifact));
}

fn amendments(
    values: impl IntoIterator<Item = RequiredAmendmentV1>,
) -> D4CheckpointDecisionV1 {
    let amendments = values.into_iter().collect::<BTreeSet<_>>()
        .into_iter().collect();
    D4CheckpointDecisionV1::AmendmentsRequired { amendments }
}

#[test]
fn d4_checkpoint_preserves_low_high_mixed_and_invalid_cases() {
    assert_eq!(classify_d4_checkpoint(no_misses()),
        Ok(D4CheckpointDecisionV1::NoMissD4NotAuthorized {}));
    assert_eq!(classify_d4_checkpoint(low_d4_miss()),
        Ok(amendments([RequiredAmendmentV1::NonD4])));
    assert_eq!(classify_d4_checkpoint(high_d4_miss()),
        Ok(amendments([RequiredAmendmentV1::D4])));
    assert_eq!(classify_d4_checkpoint(mixed_misses()),
        Ok(amendments([RequiredAmendmentV1::D4, RequiredAmendmentV1::NonD4])));
    assert_eq!(classify_d4_checkpoint(missing_degradation_only()),
        Ok(amendments([RequiredAmendmentV1::NonD4])));
    assert_eq!(classify_d4_checkpoint(twice_target_admission_only()),
        Ok(amendments([RequiredAmendmentV1::NonD4])));
    assert_eq!(classify_d4_checkpoint(supported_load_degradation_only()),
        Ok(amendments([RequiredAmendmentV1::NonD4])));
    assert_eq!(classify_d4_checkpoint(high_d4_startup_miss()),
        Ok(amendments([RequiredAmendmentV1::D4])));
    assert_eq!(classify_d4_checkpoint(low_d4_fallback_miss()),
        Ok(amendments([RequiredAmendmentV1::NonD4])));
    for invalid in [zero_denominator(), missing_sequence(), overflowing_sums()] {
        assert_eq!(classify_d4_checkpoint(invalid), Err(ResultError::InvalidArtifact));
    }
}

#[test]
fn d4_checkpoint_wire_schema_is_closed_versioned_and_nonempty() {
    let mixed = D4CheckpointDocumentV1 {
        schema_version: 1,
        decision: amendments([
            RequiredAmendmentV1::D4,
            RequiredAmendmentV1::NonD4,
        ]),
    };
    assert_eq!(serde_json::to_value(&mixed).unwrap(), serde_json::json!({
        "schema_version": 1,
        "decision": {
            "kind": "amendments_required",
            "amendments": ["d4", "non_d4"]
        }
    }));
    assert!(mixed.validate().is_ok());
    for malformed in [
        serde_json::json!({"decision": {"kind": "no_miss_d4_not_authorized"}}),
        serde_json::json!({"schema_version": 1,
            "decision": {"kind": "amendments_required", "amendments": ["d5"]}}),
        serde_json::json!({"schema_version": 1,
            "decision": {"kind": "amendments_required", "amendments": null}}),
        serde_json::json!({"schema_version": 1,
            "decision": {"kind": "no_miss_d4_not_authorized", "amendments": null}}),
        serde_json::json!({"schema_version": 1,
            "decision": {"kind": "no_miss_d4_not_authorized"}, "extra": true}),
    ] {
        assert!(serde_json::from_value::<D4CheckpointDocumentV1>(malformed).is_err());
    }
    assert_eq!(D4CheckpointDocumentV1 {
        schema_version: 2,
        decision: D4CheckpointDecisionV1::NoMissD4NotAuthorized {},
    }.validate(), Err(ResultError::InvalidArtifact));
    for amendments in [
        vec![],
        vec![RequiredAmendmentV1::D4, RequiredAmendmentV1::D4],
        vec![RequiredAmendmentV1::NonD4, RequiredAmendmentV1::D4],
    ] {
        assert_eq!(D4CheckpointDocumentV1 {
            schema_version: 1,
            decision: D4CheckpointDecisionV1::AmendmentsRequired { amendments },
        }.validate(), Err(ResultError::InvalidArtifact));
    }
}

#[test]
fn section15_rederivation_schema_is_closed_complete_and_decision_owned() {
    let report = valid_section15_rederivation();
    assert_eq!(report.schema_version, 1);
    assert_eq!(report.selected_results.len(), 14);
    assert_eq!(report.scenarios.len(), 7);
    assert!(report.selected_results.iter().all(|identity|
        identity.baseline_id == report.baseline_id));
    assert!(report.validate().is_ok());
    assert_eq!(report.decision, classify_report_rows(&report).unwrap());

    let encoded = serde_json::to_value(&report).unwrap();
    for malformed in [
        json_without_field(&encoded, "selected_results"),
        json_with_unknown_field(&encoded, "unexpected", serde_json::json!(true)),
    ] {
        assert!(serde_json::from_value::<Section15ReDerivationV1>(malformed).is_err());
    }
    for mutated in [
        section15_with_duplicate_selected_identity(&report),
        section15_with_result_digest_mismatch(&report),
        section15_with_missing_metric(&report),
        section15_with_duplicate_metric(&report),
        section15_with_extra_metric(&report),
        section15_with_reordered_metric(&report),
        section15_with_unknown_metric_unit_pair(&report),
        section15_with_noncanonical_or_overflowing_decimal(&report),
        section15_with_wrong_baseline_delta(&report),
        section15_with_substituted_baseline_trial_pair(&report),
        section15_with_wrong_failure_policy_sum(&report),
        section15_with_mutated_decision(&report),
    ] {
        assert_eq!(mutated.validate(), Err(ResultError::InvalidArtifact));
    }
    for mutated in section15_mutate_every_schema_field(&report) {
        assert_eq!(mutated.validate(), Err(ResultError::InvalidArtifact));
    }
    assert_rederiver_rejects_selected_fallback_with_reversed_chronology(&report);
}

#[test]
fn typed_reference_composer_owns_candidate_construction_and_atomic_finalization() {
    let fixture = valid_raw_scenario_root();
    let output = fixture.output_path("candidate-v1.json");
    let outcome = compose_reference_outcome_from_raw(&fixture.request()).unwrap();
    assert_eq!(outcome.status(), ReferenceOutcomeStatusV1::Pass);
    assert!(outcome.validate().is_ok());
    assert!(atomic_write_reference_outcome(&output, &outcome).is_ok());
    assert_eq!(read_and_validate_reference_outcome(&output).unwrap().status(),
        ReferenceOutcomeStatusV1::Pass);

    for mutated in [
        raw_root_with_missing_artifact(&fixture),
        raw_root_with_missing_runner_control(&fixture),
        raw_root_with_substituted_runner_control(&fixture),
        raw_root_with_digest_mismatch(&fixture),
        raw_root_with_duplicate_trial_path(&fixture),
        raw_root_with_control_mismatch(&fixture),
        raw_root_with_threshold_status_mismatch(&fixture),
    ] {
        let invalid = compose_reference_outcome_from_raw(&mutated.request()).unwrap();
        assert_eq!(invalid.status(), ReferenceOutcomeStatusV1::Invalid);
        assert!(invalid.validate().is_ok());
    }
}

#[test]
fn typed_reference_validator_owns_final_publication_and_status_mapping() {
    let fixture = composed_candidate_fixture(ReferenceOutcomeStatusV1::Pass);
    let request = fixture.validator_request(0);
    assert_eq!(validate_reference_outcome(&request).unwrap(), 0);
    assert_eq!(read_and_validate_reference_outcome(&fixture.final_output())
        .unwrap().status(), ReferenceOutcomeStatusV1::Pass);

    for (candidate_status, composer_status, expected) in closed_status_cross_product() {
        let mutated = composed_candidate_fixture(candidate_status);
        let actual = validate_reference_outcome(
            &mutated.validator_request(composer_status)).unwrap();
        assert_eq!(actual, expected);
        assert!(read_and_validate_reference_outcome(&mutated.final_output())
            .unwrap().validate().is_ok());
    }
    for transport in ["unexpected:101", "unexpected:137"] {
        for candidate in [Some(valid_candidate()), None, Some(malformed_candidate())] {
            let fixture = candidate_fixture(candidate);
            assert_eq!(validate_reference_outcome(
                &fixture.validator_request_token(transport)).unwrap(), 20);
            let final_outcome = read_and_validate_reference_outcome(
                &fixture.final_output()).unwrap();
            assert_eq!(final_outcome.status(), ReferenceOutcomeStatusV1::Invalid);
            assert_eq!(final_outcome.failure_reasons(), &[FailureReasonV1::CommandFailed]);
        }
    }
    assert_validator_transport_environment_and_interruption_matrix();
}
```

`section15_mutate_every_schema_field` is a table-driven mutation inventory, not
a sampling helper. It mutates every top-level field; every field of every
`SelectedResultIdentityV1` including every nested `ExecutableIdentityV1` field;
every scenario/status/failure/trial field; every sequence-count member; and
every member of distribution, predicate, baseline-delta, failure-policy, and
decision rows. Separate structural cases delete, duplicate, insert, and reorder
each row kind and substitute a Baseline trial/result/raw root from another
scenario or trial. The positive fixture asserts the exact row counts produced by
the manifest matrix before those mutations. Any schema field added later must be
added to this inventory in the same commit; a debug assertion compares the
visited field-path set to the fixture's canonical field-path enumeration.

Run the new validation suite now, after adding the compile-complete test
scaffolding above but before implementing successful fixture/validator behavior:

The conservative composer and validator scaffolds return `Ok` with one valid
`Invalid(InvalidArtifact)` candidate/final envelope and status `20`; the
section-15 scaffold returns a fully constructible document whose `validate()`
returns `Err(InvalidArtifact)`. Therefore each positive test reaches and fails
its first explicit behavioral assertion before any `.unwrap()` that depends on
the not-yet-implemented classifier or successful status. The scaffold itself
must not return `Err`, omit a required file, or panic.

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test workload_harness -- --nocapture
```

Expected: the tests compile against the frozen public shapes and fail at their
behavioral assertions because the result composer, validator, observer evidence,
and D4 classifier still return the deliberately incomplete scaffold behavior. A
missing-symbol, type, panic, or fixture-construction failure is not the accepted
red state.

- [ ] **Step 5: Implement the protocol, validator, observer, and classifier**

Replace the conservative scaffolds with the real implementation.
`valid_synthetic_result()` now constructs one `MeasurementStageV1::Baseline`
Sustained `ReferenceOutcomeV1::Pass`
whose `ReferenceRunV1` has five
complete trials whose submitted/admitted/completed/persisted vectors are exactly
`1..=1_200` and whose rendered vector is the exact 300-sequence screen-probe
vector, whose
four structural identity sets match `target_model()`, whose controls name CPUs
0-3 and the 16-GiB address-space cap with `true_cgroup_memory_limit == false`,
whose run controls pin the non-mise rustup identity, every authoritative
executable identity, toolchain `1.97.1`, exact cargo/rustc version strings,
closed build environment, reject-all Cargo-configuration policy, and measured
binary identity. Each raw child independently records the exact per-trial
measured environment, effective limits, and scratch root; each outer trial
records the separate observer environment and revalidated executable/scratch
evidence. The canonical scratch roots match the deterministic raw layout and
record the same verified NVMe leaf devices as the host envelope, and
whose process-tree evidence records a positive `clock_ticks_per_second`,
and whose raw observations/artifact fixtures derive exactly to screen-update,
reducer-lag, and publish-to-render distributions plus resources below every
binding threshold. It calls `validate()` successfully
before each test mutates one invariant. The test-only `document()` and
`document_mut()` helpers return the inner run for `Pass`/`Failed` and panic for
`Invalid`; invalid tests
use the invalid-envelope builders directly. `valid_failed_outcome()` contains a real
threshold miss and its exact closed reason set; `valid_invalid_outcome()` contains
no trial aggregates. Additional composition tests feed a missing/reused raw path, a digest
mismatch, missing or self-contaminating process-tree evidence, a missing or
mismatched observer-control transcript, missing GNU-time, pidstat stderr, or
observer stdout/stderr evidence, malformed pidstat JSON, a zero-sample
short-trial pidstat document, a zero clock-tick rate, mismatched scratch root or
storage device, an unexpected run-level `HERDR_PERF_OUTPUT` key, a per-trial
output path that disagrees with its exact trial directory, reused paths across a
valid five-trial fixture, an effective child control that differs from the run
request, a measured/observer environment ownership mismatch, a changed
executable identity, missing/substituted/mutated `runner-control.json`, a
control artifact whose `preflight_head` differs from `harness_sha` or the
current composition request, a missing/malformed/multi-line/leading-zero/
out-of-range/substituted/reused/digest-mismatched `trial-status`, trial transport
or runner-control disagreement, both calibrated pidstat modes, every illegal
sentinel/pidstat-status pair, a cleanup override to `failed:20`, timeout/signal
codes `124/130/137/143`, a nonzero command result, and malformed intermediate
harness JSON to the typed composer. Missing/malformed/mismatched evidence produces an
atomic `ReferenceOutcomeV1::Invalid` with `InvalidArtifact` or `CommandFailed`;
a structurally valid zero-sample pidstat document is allowed only by the
applicability matrix. A positive five-trial fixture deliberately uses different
output/socket/scratch paths in every trial and must validate, proving that only
run invariants—not per-trial plumbing—are shared.

`valid_workload_admission_failure()` is a valid `Failed` Sustained document whose
first one-second bucket has one admission one nanosecond beyond its bucket-end
plus 50-ms cadence allowance and whose exact reason set is
`[WorkloadAdmission]`. `valid_twice_target_workload_admission_failure()` applies
the analogous 25-ms rule, carries a truthful complete performance evidence stream
through the deadline, and also has only
`[WorkloadAdmission]`; it proves the validator does not misderive
`MissingDegradation` from an under-driven overload. The
`twice_target_admission_only()` classifier fixture wraps that same validated
failure and therefore requires `NonD4`. The sibling-observer fixtures start a
small measured root that binds its control socket, atomically publishes its
handshake, and blocks. The test parent launches the observer; the observer
validates the immutable root, captures its first sample, sends `Ready`, and only
then may the root fork its setup child. They assert both immutable identities are
present and the observer PID is absent. They never sample the test runner as the
measured root.

`high_d4_startup_miss()` is a valid failed Startup outcome with one
`StartupRestore` scope per trial and exact checked ratio 0.25; it authorizes D4.
`low_d4_fallback_miss()` is a valid failed FallbackRescan outcome with matched
`FallbackNotification`/`FallbackRescan` scopes for every pair whose checked sums
are 249 D4 nanoseconds over 1,000 denominator nanoseconds; it
requires a non-D4 amendment. Missing either kind or
duplicating a `(kind, sequence)` is `InvalidArtifact`.

The idle fixtures set nonzero setup/settle ticks before the window, include one
identity born inside the window, retain a child that exits before the end
boundary, add a post-end birth, and prove only checked explicit end-minus-start
deltas enter the numerator. The matching control transcript supplies the same
ready/start/end monotonic values as harness and process-tree evidence. Task
1A.1's performance-stream validator fixtures are explicitly synthetic and
manifest-derived: they use the frozen reason-to-label table, limits, surface,
nonzero frame phase, admissions, terminal outcomes, sample/draw ordinal bounds,
and complete synthetic header lines without constructing
`CollectorHandle.performance`, `HeaderInputs`, or a `TestBackend`. They therefore
exercise the closed schema and mutations before Tasks 6 and 7 exist. The validator
re-derives every raw sample from admissions, terminals, topology, and the closed
limits; derives the first-or-payload-changed publication ordinals; requires every
frame to name an exact existing raw record from one of those published ordinals
with byte-coherent state; and re-derives the complete stable `perf:` label. It
does not invent a fresh watch read or require the frame to use a later publication
that `App` had not refreshed before the draw. Tests delete interior and suffix
entries, create ordinal gaps,
shift the start anchor, truncate the independently supplied closure watermark,
delete a terminal observation, alter a reason/quality/header, and point a draw at
an incoherent sample. Every mutation is invalid.

The supported-load fixtures provide both all-`Live` passing streams and a
truthfully derived nonempty reason stream whose only measured failure is
`SupportedLoadDegradation`; `supported_load_degradation_only()` proves that this
failure requires `NonD4`. The twice-target fixture uses admission 1,201 and the
first qualifying draw ordinal. Tests freeze its qualifying sample at origin plus
30.025 seconds and derive its frame by checked addition at origin plus 30.132
seconds, proving an exact 107-ms sample-to-render diagnostic. They also cover
before-origin and before-threshold-crossing state, nonzero real-clock limiter
overshoot without timestamp synthesis, delay beyond one production interval,
after-deadline first match, selection of a later matching ordinal, false absence,
and deadline-addition overflow. An earlier draw containing only `EventLag` does
not end recording; the selected draw is the lowest ordinal containing
`EventsSixtySeconds`, while the complete workload and stream continue to their
independently closed bounds. Extra simultaneous reasons are valid only when the
sample, frame, and rendered label contain them in the same stable order. A
complete stream with no qualifying draw through the deadline derives
`MissingDegradation`; an omission or internally inconsistent sample/frame pair
derives `InvalidArtifact`.
Task 7 separately owns the integration proof that the real
`CollectorHandle.performance` receiver and actual frozen `TestBackend` header
produce the same accepted evidence; no Task 1A.1 commit depends on that future
state.

Also add ignored `classify_d4_checkpoint_from_results`. It reads only the
explicit `HERDR_PERF_CLASSIFY_RESULTS_ROOT`, independently validates the seven
scenario outcomes and their raw roots, wraps the decision in a validated
`D4CheckpointDocumentV1`, writes that exact closed JSON to the explicit
`HERDR_PERF_CLASSIFY_OUTPUT` via atomic rename, and returns nonzero for
invalid/missing evidence or an invalid output document. It never discovers paths
from the home directory.

Also add ignored `rederive_section15_report_from_results`. It reads only the two
explicit `HERDR_PERF_REDERIVE_BASELINE_RESULTS_ROOT` and
`HERDR_PERF_REDERIVE_FINAL_RESULTS_ROOT` values, rejects equal, nested, aliased,
or non-canonical roots, validates the seven selected Baseline and seven selected
Final result documents and all fourteen fixed raw roots, and re-derives every section 15 count,
distribution, threshold comparison, baseline delta, failure-policy lookup, and
D4 numerator/denominator from raw samples in typed Rust. Counts and nanoseconds
use checked `u64`; cross-sample sums and the exact
`4 * d4_sum >= reducer_plus_publish_sum` predicate use checked `u128`. It writes
one closed, validated `Section15ReDerivationV1` to the explicit
`HERDR_PERF_REDERIVE_OUTPUT` by atomic rename and returns nonzero on missing,
duplicate, overflowed, structurally inconsistent, or control-mismatched evidence.
It performs no path discovery. jq is not permitted to calculate or decide any
value in that document. Tests cover a missing root, stage-mismatched result,
equal/ancestor/descendant roots, a selected Baseline result substituted from a
different scenario or trial, and any selected-result/raw-root identity mismatch.

Also add ignored `record_runner_control_evidence`. Its callable contract is
program = the frozen measured `workload_harness` test binary; argv =
`record_runner_control_evidence --exact --ignored --nocapture --test-threads=1`;
and, after `env_clear()`, environment = the six invariant keys, the seven
native-Controller-derived `HERDR_INCREMENT5_{CONTROLLER,RUNNER,BOOTSTRAP}_*`
keys, and exactly these control keys:

```text
HERDR_PERF_CONTROL_RAW_ROOT
HERDR_PERF_CONTROL_OUTPUT
HERDR_PERF_CONTROL_STAGE
HERDR_PERF_CONTROL_SCENARIO
HERDR_PERF_CONTROL_SUBJECT
HERDR_PERF_CONTROL_PREFLIGHT_HEAD
HERDR_PERF_CONTROL_TRIAL_INDEX
HERDR_PERF_CONTROL_INVOCATION_CWD
HERDR_PERF_CONTROL_MEASURED_REQUESTED
HERDR_PERF_CONTROL_MEASURED_CANONICAL
HERDR_PERF_CONTROL_MEASURED_SHA256
HERDR_PERF_CONTROL_TRIAL_STATUS_PATH
HERDR_PERF_CONTROL_PIDSTAT_EXIT_STATUS
HERDR_PERF_CONTROL_PIDSTAT_CHILD_STATUS_MODE
```

Outside Baseline the map additionally contains exactly
`HERDR_PERF_CONTROL_BASELINE_RESULTS_ROOT`. The output must be the current
recorded trial's canonical `runner-control.json`; the raw root, one-based trial
index, mapped scenario, stage, subject, invocation cwd, and optional baseline
root must match the runner's already-frozen values. The entrypoint uses native
Rust path canonicalization and SHA-256 plus only identities in the injected
closed tool manifest. The trial-status path must be exactly
`<raw-root>/trial-status`; the entrypoint reads its exact bytes, rejects anything
other than `ok:0\n` or `failed:<canonical-1..255>\n`, checks the captured
canonical `pidstat` exit in `0..=255` against the recorded preflight mode, and
requires `HERDR_PERF_CONTROL_PIDSTAT_CHILD_STATUS_MODE` to be exactly
`propagates_child_status` or `monitor_only`; it serializes the mode, sentinel,
and exit as typed evidence. It revalidates the native Controller, runner script,
measured binary, toolchain launcher, every authoritative executable, Cargo
configuration absence, exact rustc/cargo versions, current Git HEAD, both
tracked-clean predicates, host/storage profile, all trial paths, and the exact
child/orchestrator/observer/validator-template maps. It requires the supplied
lowercase 40-hex `preflight_head` to equal current HEAD and sets
`harness_sha` to those exact same bytes. It derives command, build profile,
controlled environment, render surface, thresholds, and the v1 workload digest
from the closed manifest rather than accepting caller-authored replacements.
It then validates and atomically renames one closed
`RunnerControlEvidenceV1`; shell cannot write, edit, or normalize the JSON.
Missing, substituted, reordered, duplicated, or mutated controller/runner/tool,
measured, harness-revision, environment, path, host, or trial evidence fails
before `runner-control.json` is published. Table-driven tests cover every exact
environment key, a forbidden extra key, a missing/substituted control artifact,
every sole-owner run-control field, `preflight_head != harness_sha`, subject or
trial mismatch, every sentinel/status-mode pair, tool-manifest reordering, and
mutation after a successful hash.

Also add ignored `compose_reference_outcome_from_raw`. Its callable contract is
program = the frozen measured `workload_harness` test binary; argv =
`compose_reference_outcome_from_raw --exact --ignored --nocapture
--test-threads=1`; and, after `env_clear()`, environment = the six invariant
keys plus exactly `HERDR_PERF_COMPOSE_RAW_ROOT`, `HERDR_PERF_COMPOSE_OUTPUT`,
`HERDR_PERF_COMPOSE_STAGE`, `HERDR_PERF_COMPOSE_SCENARIO`,
`HERDR_PERF_COMPOSE_SUBJECT`, `HERDR_PERF_COMPOSE_PREFLIGHT_HEAD`, and, only outside Baseline,
`HERDR_PERF_COMPOSE_BASELINE_RESULTS_ROOT`. The entrypoint alone deserializes the
closed raw artifacts including every `runner-control.json` and canonical
plain-text `trial-status`, rehashes them,
requires every artifact's `preflight_head == harness_sha` to match the exact
request, calculates aggregates, constructs the
tagged `ReferenceOutcomeV1`, calls its validator, and atomically renames the
complete JSON to the explicit candidate output. It exits `0` for `Pass`, `10` for valid
`Failed`, and `20` for an atomically written, independently valid `Invalid`;
interruption before atomic finalization may leave no final file. Shell code may
not construct, edit, or normalize candidate JSON. The positive fixture above and
each missing/digest/duplicate/control/status mutation run this same composer
function before the ignored transport is installed, so both composition and its
status mapping are TDD-reachable.

Also add ignored `validate_reference_outcome`. Its callable contract is program
= the same frozen measured `workload_harness` test binary; argv =
`validate_reference_outcome --exact --ignored --nocapture --test-threads=1`;
and, after `env_clear()`, environment = the six invariant keys plus exactly
`HERDR_PERF_VALIDATE_RAW_ROOT`, `HERDR_PERF_VALIDATE_CANDIDATE`,
`HERDR_PERF_VALIDATE_OUTPUT`, `HERDR_PERF_VALIDATE_STAGE`,
`HERDR_PERF_VALIDATE_SCENARIO`, `HERDR_PERF_VALIDATE_SUBJECT`,
`HERDR_PERF_VALIDATE_PREFLIGHT_HEAD`,
`HERDR_PERF_VALIDATE_COMPOSER_STATUS`,
`HERDR_PERF_VALIDATE_TRIAL_STATUS`, and, only outside Baseline,
`HERDR_PERF_VALIDATE_BASELINE_RESULTS_ROOT`. The candidate path is the composer's
private `candidate-v1.json`; the output is the only public `result-v1.json`.
`HERDR_PERF_VALIDATE_COMPOSER_STATUS` accepts only canonical ASCII `0`, `10`,
`20`, or `unexpected:<code>`, where `<code>` is a decimal shell status in
`0..=255` with no leading zero unless it is exactly zero; the `unexpected` form
is valid only when the code is not `0`, `10`, or `20`. A signal-observed status
such as `137` uses `unexpected:137`; no signal name or private stderr is copied
into the artifact.
`HERDR_PERF_VALIDATE_TRIAL_STATUS` is a distinct scenario-level transport and
accepts only canonical ASCII `all-ok` or
`failed:trial-<index>:<code>`. `<index>` is a no-leading-zero one-based recorded
trial index in the manifest's exact range and `<code>` is a no-leading-zero
decimal value in `1..=255`. `all-ok` requires every recorded sentinel to be
`ok:0\n`; the failed form names the first well-formed failed sentinel and its
exact code. Thus cleanup status `20` is `failed:trial-N:20`, never
`unexpected:20`; valid threshold failure is `all-ok` plus composer status `10`.
The validator alone re-opens and validates the candidate and all raw evidence,
including every `runner-control.json` and `trial-status`, requires its exact request subject and
preflight HEAD to agree with the validated control artifacts, requires candidate
tag, freshly derived status, the supplied trial-status transport, and a normal
supplied composer status to agree, and
atomically writes the final output. Normal agreement returns that same
`0`/`10`/`20`; a missing/malformed candidate, normal transport/control mismatch,
or normal-status disagreement atomically replaces any prior final output with an
independently constructed `Invalid(InvalidArtifact)` and returns `20`. An
`unexpected:<code>` composer token instead ignores any candidate tag, independently
constructs `Invalid(CommandFailed)` from the exact request plus validated control
evidence when available, atomically replaces the final output, and returns `20`;
missing or malformed control evidence cannot become measured data and still uses
the exact request subject, `harness_sha == preflight_head`, and built-in v1
schema identity for the smaller valid invalid envelope. Shell may delete neither
candidate nor final output and has no rename authority. Interruption before the
validator's atomic-finalization point may leave only the candidate and no final
output; after that point the final file must be complete and validated.

The typed composer derives `TrialStatusV1` from the sentinels. All `ok:0`
sentinels permit ordinary composition. A well-formed failed sentinel produces a
typed `Invalid(CommandFailed)` candidate; missing, malformed, multi-line,
substituted, reused, digest-mismatched, or transport-disagreeing sentinel is
`Invalid(InvalidArtifact)`. The validator re-derives this independently;
sentinel integrity takes precedence over the claimed command-failure token.
Neither shell nor `pidstat` may synthesize, repair, or overwrite a sentinel.

Table-driven tests invoke the real validator function and ignored transport with
an exact recording child. They cover every argv and environment key, a forbidden
extra/missing/duplicate key, candidate/raw/output path substitution, all nine
normal composer-status/tag/derived-status combinations, `unexpected:101`,
`unexpected:137`, absent and malformed candidates on those two paths,
`all-ok`, failed trial codes `10`, `20`, `124`, `130`, `137`, and `143`, every
composer/trial cross-product, missing/malformed/multi-line/leading-zero/
out-of-range/substituted/reused/digest-mismatched sentinels, runner-control and
trial-transport disagreement, noncanonical status text, atomic replacement of a stale final, and interruption
immediately before and after final rename. A validator panic, signal, or
unexpected process status is deliberately outside its own atomicity boundary:
the runner must return `20` and report the incomplete attempt; the Controller
records that state in the research ledger and never selects a final file even if
the failure occurred after rename. A panic, missing symbol,
or shell-generated JSON is not an accepted red state.

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test workload_harness -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

Expected: formatting, all-feature/all-target lint and tests, and all non-ignored
harness tests pass. The reference entrypoint remains ignored under the ordinary
default-suite invocation.

After task review and Controller verification, commit Task 1A.1 alone:

```bash
git add Cargo.toml tests/common/mod.rs tests/common/workload.rs \
  tests/fixtures/MANIFEST.md tests/fixtures/workload-schema-v1.json \
  tests/workload_harness.rs
git commit -m "test(perf): define deterministic workload protocol" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```


---

### Task 1A.2a: Fail-closed native Controller and Bash runner

**Files:**

- Modify: `Cargo.toml`
- Modify: `tests/workload_harness.rs`
- Create: `tests/support/reference_profile_controller.rs`
- Create: `scripts/run-reference-profile.sh`

**Interfaces:**

- Consumes: Task 1A.1's closed schema, typed control writer,
  composer/validator, process-tree observer, re-deriver, and classifier
  entrypoints.
- Produces: a feature-gated native `increment5-reference-controller` first-exec
  launcher, the executable runner contract, and ordinary Linux source-only
  fixtures without creating promotable measurement evidence.

- [ ] **Step 1: Add a source-clean skeleton and failing runner fixtures**

Create only a sourceable skeleton beginning with exactly `#!/usr/bin/bash -p`
and `set -euo pipefail`, whose source inventory rejects `set +p` and whose library-only guard
prevents `main` execution. Add the Linux source-only fixtures for argument
closure, attempt containment, hostile environment/PATH, executable mutation,
process-group cleanup, signals, timeout, `all` aggregation, typed invalid
finalization, and atomic promotion before implementing their target functions.
Name the binding tests
`runner_library_guard_is_source_clean`,
`native_controller_bootstrap_starts_empty_and_launches_exact_env`,
`source_fixture_uses_frozen_canonical_runner_operand`,
`runner_fixture_rejects_uncontained_attempt_id`,
`runner_fixture_reaps_timeout_and_signal_groups`, and
`runner_fixture_aggregates_closed_statuses_and_promotes_atomically`, plus
`runner_rejects_worktree_output_under_clean_first_exec`,
`source_fixture_inventory_is_portable_and_role_closed`,
`trial_status_is_atomic_and_independent_of_pidstat_exit`, and
`runner_fixture_preserves_measured_and_observer_exit_status_precedence`, and
`pidstat_child_status_modes_are_calibrated_and_cross_checked`. Run all eleven
individually before Step 2:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test workload_harness runner_library_guard_is_source_clean -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness native_controller_bootstrap_starts_empty_and_launches_exact_env -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness source_fixture_uses_frozen_canonical_runner_operand -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness runner_fixture_rejects_uncontained_attempt_id -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness runner_fixture_reaps_timeout_and_signal_groups -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness runner_fixture_aggregates_closed_statuses_and_promotes_atomically -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness runner_rejects_worktree_output_under_clean_first_exec -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness source_fixture_inventory_is_portable_and_role_closed -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness trial_status_is_atomic_and_independent_of_pidstat_exit -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness runner_fixture_preserves_measured_and_observer_exit_status_precedence -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness pidstat_child_status_modes_are_calibrated_and_cross_checked -- --exact --nocapture
```

The skeleton and `runner_library_guard_is_source_clean` must pass `bash -n` and
leave no child or output root. The native-controller and canonical-runner-operand
tests must compile and fail because the declared feature-gated binary and closed
launch grammar do not yet exist. The other eight named fixtures must compile and fail behavioral
assertions for the missing orchestration/preflight behavior;
syntax errors, missing script, and missing Rust symbols are not accepted red
states. The aggregation fixture's red matrix includes composer exits `101` and
signal-observed `137`, plus validator exit `101` both before and after a fixture
atomic rename; trial sentinels `ok:0`, `failed:10`, `failed:20`, timeout/signal
codes `124/130/137/143`, malformed/missing status, both calibrated pidstat modes,
and inconsistent pidstat/sentinel pairs; and authoritative/source-fixture
inventory substitution plus missing/extra/reordered portable roles. Its green assertions require the two composer cases to produce a
validated `Invalid(CommandFailed)` and require both validator cases to leave the
attempt incomplete, abort `all`, and make every file under that attempt
ineligible for selection.

- [ ] **Step 2: Implement the fail-closed Linux reference runner**

Add this non-default binary target; it is runner infrastructure, not production
`herdr-top` behavior:

```toml
[[bin]]
name = "increment5-reference-controller"
path = "tests/support/reference_profile_controller.rs"
required-features = ["workload-harness"]
test = false
bench = false
```

The native Controller accepts only this closed grammar:

```text
--self-requested ABS --self-canonical ABS --self-sha256 HEX64
(launch | launch-runner | launch-runner-source-fixture
  --runner-script-requested ABS --runner-script-canonical ABS
  --runner-script-sha256 HEX64
  [--fixture-tool ROLE=ABS]...)
--program-requested ABS --program-canonical ABS --program-sha256 HEX64
[--env KEY=VALUE]... -- [ARGV]...
```

It first requires its own process environment to be exactly empty, then
canonicalizes and hashes its requested path, requires it to match both
`current_exe()` and the supplied regular-executable canonical identity, rejects
duplicate/malformed environment keys and duplicate control flags, revalidates
the child executable identity, and calls
`Command::new(program_canonical).env_clear().envs(exact_map).args(argv)`.
It installs no default environment, does not invoke a shell, does not search
`PATH`, and mirrors only child statuses `0`, `10`, or `20`; any launch, identity,
grammar, signal, or unexpected-status failure exits `20`.

`launch-runner` additionally requires the program identity to equal the native
Controller's freshly derived `/usr/bin/bash` entry and the child argv prefix to
be exactly `-p <runner-script-canonical>`. The non-authoritative
`launch-runner-source-fixture` requires exactly the source-fixture caller
allowlist, and child argv shape `-p -c BODY herdr-i5-source-fixture
<runner-script-canonical> [FIXTURE_ARGV]`; the sourced runner refuses to promote
reference evidence. `FIXTURE_ARGV` begins with exactly one closed operation:
`orchestration` for the existing callback-driven fixture, or
`output-containment REPOSITORY_ROOT ROOT_COUNT WORKTREE_ROOT... OUTPUT_DIR` for
the portable containment fixture. The Rust parent supplies canonical absolute
roots, `ROOT_COUNT` must match the exact following worktree-root cardinality, and
no extra operand is accepted. `--fixture-tool` is forbidden for `launch` and
`launch-runner`. For `launch-runner-source-fixture` it occurs exactly once for
each role in this compile-time ordered `source-fixture-v1` set and no other role:

```text
env,id,mkdir,mktemp,mv,pidstat,prlimit,readlink,rmdir,setsid,
sha256sum,sleep,stat,taskset,time,unlink
```

The trusted Rust test parent supplies an absolute CI-installed tool or a
test-owned executable shim for each role. The Controller, never `PATH` or Bash,
canonicalizes and SHA-256 hashes each requested path, requires a regular
executable target, and serializes the ordered role/requested/canonical/digest
tuples. Bash remains the separately attested child program; Controller/self and
runner-script identities remain separately attested. The source-fixture mode
rejects any `/home/mageyuki` requested/canonical tool path, missing/extra/
reordered role, cross-mode manifest, or identity mutation before Bash starts.
Normal `launch-runner` rejects the portable manifest and continues to derive the
full fixed `authoritative-v1` host-tool inventory below.

Before spawning Bash, the Controller also canonicalizes and hashes the runner
script, rejects a non-regular/non-executable target or drift from the supplied
runner identity, and constructs the six common reserved identity values plus
exactly one mode-specific tool-manifest value:

```text
HERDR_INCREMENT5_CONTROLLER_REQUESTED
HERDR_INCREMENT5_CONTROLLER_CANONICAL
HERDR_INCREMENT5_CONTROLLER_SHA256
HERDR_INCREMENT5_RUNNER_REQUESTED
HERDR_INCREMENT5_RUNNER_CANONICAL
HERDR_INCREMENT5_RUNNER_SHA256
HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1                  # authoritative-v1 only
HERDR_INCREMENT5_BOOTSTRAP_TOOLS_SOURCE_FIXTURE_V1   # source-fixture-v1 only
```

Exactly seven reserved values are present in either mode. The source-fixture
manifest cannot deserialize as `RunControlsV1.authoritative_executables`, its
subcommand cannot call authoritative preflight or composition, and neither the
fixture Controller nor sourced runner can create `result-v1.json`.

The tool value is one UTF-8 line per inventory entry in the declared inventory
order, each exactly `requested<TAB>canonical<TAB>lowercase-hex64<LF>`; all paths
must be absolute and contain no tab, LF, CR, or NUL. The Controller rejects any
caller `--env` using a reserved key or any `HERDR_INCREMENT5_CONTROLLER_`,
`HERDR_INCREMENT5_RUNNER_`, or `HERDR_INCREMENT5_BOOTSTRAP_` prefix, then injects
the seven derived values after parsing caller input. `launch` injects none of
them and still rejects those reserved caller keys. Thus only the process that
validated `current_exe()` can attest the launcher identity seen by the runner;
opaque child argv cannot claim a different launcher.

Unit tests cover empty and hostile bootstrap maps, self/program/runner digest
mutation, duplicate keys, relative paths, signals, exact argv/environment
recording, a caller-supplied reserved key, missing/duplicate/reordered tool rows,
and a requested child launcher tuple that disagrees with outer `--self-*`. Every
such mutation fails before Bash/recording-child start and before output creation.

Before an authoritative run, the trusted Controller builds this one target from
the exact clean subject with the frozen rustup/toolchain/build environment,
selects exactly one absolute launcher artifact from Cargo JSON, and freezes its
requested/canonical/SHA-256 identity. This pre-boundary build is not a measured
trial and cannot cleanse a Controller compromised before startup:

```bash
"$env_executable" -i HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup \
  CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
  "$rustup_executable" run 1.97.1 cargo build --release --locked \
    --features workload-harness --bin increment5-reference-controller \
    --message-format=json >"$controller_artifact_json"
```

The frozen jq selector uses the same fail-closed status discipline as the later
measured-binary selector and requires exactly one compiler artifact with the
canonical manifest path, target name `increment5-reference-controller`, target
kind exactly `["bin"]`, `profile.test == false`, and one absolute string
`executable`; zero, multiple, null, relative, wrong-manifest, wrong-kind, or
wrong-profile candidates fail before bootstrap. The Controller rechecks exact
HEAD/tracked/index cleanliness after this build and before freezing the artifact.

The bounded launch command then ends with this Bash special builtin and nothing
after it:

```bash
controller_argv=(
  --self-requested "$controller_requested"
  --self-canonical "$controller_canonical"
  --self-sha256 "$controller_sha256"
  launch-runner
  --runner-script-requested "$runner_requested"
  --runner-script-canonical "$runner_canonical"
  --runner-script-sha256 "$runner_sha256"
  --program-requested "$program_requested"
  --program-canonical "$program_canonical"
  --program-sha256 "$program_sha256"
)
for entry in "${child_environment[@]}"; do
  controller_argv+=(--env "$entry")
done
controller_argv+=(-- "${child_argv[@]}")
builtin exec -c "$controller_canonical" "${controller_argv[@]}"
```

Installed `/usr/bin/bash` documents `exec -c` as executing the command with an
empty environment. Task review re-runs `/usr/bin/bash -p -c 'help exec'`, requires
that exact statement, and verifies `exec` remains a Bash builtin. No subshell,
pipeline, external `env`, newly loaded intermediary, or command follows the
special builtin. The launched Controller asserts the empty map before touching
arguments, so its native loader is the first executable after the clean
boundary. The positive bootstrap test launches a clean trusted Bash, uses this
exact `exec -c` body, and observes the recording child; mutation fixtures prove
the launcher fails before the child starts.

The initial trust boundary is an already-trusted Controller or Rust test parent;
it cannot retroactively cleanse compromise predating that parent's startup. Every
authoritative runner launch uses the native Controller above; only a unit test of
the generic `launch` child helper may exercise its non-runner child contract
directly with `Command::env_clear()`.
Reversing the order is forbidden because `env_clear()` also removes entries
already added to the command. A normal launch passes frozen canonical Bash as
the child program, `-p`, and then the frozen absolute runner-script path and its
arguments. A source
fixture passes `-p -c`, one fixed source-fixture body, a fixed argv0, and the
frozen absolute runner-script path as a positional operand; the body sources only
that operand. Both paths freeze and record the runner script's requested absolute
path, canonical path, executable metadata, and SHA-256 as
`RunControlsV1.runner_script` before first use and revalidate it before every
later authoritative launch.

The caller-supplied normal first-exec allowlist is exactly `HOME=/home/mageyuki`,
`RUSTUP_HOME=/home/mageyuki/.rustup`, `CARGO_HOME=/home/mageyuki/.cargo`,
`PATH=/usr/bin:/bin`, `LC_ALL=C`, `TZ=UTC`, and the one explicit
`HERDR_INCREMENT5_ATTEMPT_ID=<eight-digit nonzero ID>`. The source-fixture
caller allowlist is exactly the same map plus
`HERDR_PERF_RUNNER_TEST_LIBRARY_ONLY=1`. Script path, subject, stage, scenario,
baseline root, output root, fixture callbacks, and hostile test data travel only
as direct argv operands or inert Rust values, never as additional environment
keys. For either runner subcommand the observable map is exactly the applicable caller
allowlist plus the seven native-derived reserved values above. The recording
probe compares the complete observed map and proves that removing or replacing
any reserved value is impossible through the caller grammar.

A frozen canonical `env` may optionally run only after this clean first-exec
boundary. Neither a newly executed inherited-environment shell nor an external
`env -i` started with a potentially contaminated environment may establish it:
`/usr/bin/env` (uutils
coreutils 0.8.0 on the selected host) and `/usr/bin/bash` are dynamically linked,
so their loaders can consume `LD_PRELOAD` before either program clears or ignores
it. The already-running trusted parent Bash's `exec -c` special builtin is the
only bootstrap exception: it performs the empty-`envp` native launcher exec
without loading another intermediary. Authoritative launch never invokes the script through its shebang; this pins
the recorded interpreter identity, although a direct shebang `execve` with an
already-clean `envp` would itself preserve that environment. If the Controller
cannot establish and verify that exact empty-native-launcher first exec,
measurement fails closed and is not run.

The runner begins with exactly `#!/usr/bin/bash -p` and `set -euo pipefail`, and
both the script and sourced fixture reject `set +p`. Bash `-p` is defense in
depth for `ENV`, `BASH_ENV`, imported functions, and shell-option variables; it
does not sanitize loader-visible state. Hostile tests start in a trusted Rust
test process. They keep `LD_PRELOAD`, `LD_LIBRARY_PATH`, `BASH_ENV`, `ENV`, and
`BASH_FUNC_*` spellings as inert data, may stage them only on a child `Command`
builder, then apply the same `env_clear()`-before-allowlist helper. No hostile
value is installed in the parent environment or reaches Bash, the recording
child, the script/fixture, or any intermediate executable. The observed child
environment must equal the allowlist exactly; loader/runtime influence keys,
including `LD_PRELOAD`, `LD_LIBRARY_PATH`, `LOCPATH`, and `GCONV_PATH`, are
excluded.

The Bash runner starts with `set -euo pipefail`. Before any external command it
uses only Bash builtins (`[[`, `read`, `case`, `declare`, and `printf`) to parse
the seven native-derived values. It requires the controller and runner triples
to be absolute/canonical/lowercase-digest shaped and the runner triple to equal
the script operand. It parses `HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1` with tab as
the sole field separator, requires the exact closed requested-path sequence,
and binds the native-attested canonical `readlink` and `sha256sum` entries. Their
first invocation is authorized by the immediately preceding native Rust
canonicalization/hash root; Bash does not attempt to discover or hash either
helper before that first invocation. Using only those two already-bound paths,
it then revalidates every tool row, the controller executable, and the runner
script before any other external use. Missing, extra, duplicate, reordered,
malformed, or drifted evidence exits `20` before an output root is created.
Subsequent authoritative uses recheck against the same native-bound manifest.
This native root and explicit first-use rule are the only bootstrap exception;
there is no bare `readlink`/`sha256sum` discovery cycle.

After bootstrap the runner parses only the five documented flags (`--subject`,
`--stage`, `--scenario`, `--output-dir`, and `--baseline-results-root`, which is
forbidden for the pinned `Baseline` run and required for both later stages),
accepts scenario names `target`, `sustained`, `burst`, `startup`, `idle`,
`fallback-rescan`, `twice-target`, or `all`, and defaults no output path. Require the caller to pass
an output directory outside the repository and all linked worktrees.
Resolve each CLI token through the manifest's single closed mapping and use only
the mapped snake-case value as the directory component. In particular,
`fallback-rescan` writes `fallback_rescan/`, `twice-target` writes
`twice_target/`, and `all` expands to the seven mapped directories without ever
creating `all/`.
It includes the injected and revalidated Controller identity exactly once in
`RunControlsV1.authoritative_executables`; no CLI launcher identity is accepted.
For `all`, it explicitly captures each scenario verdict using exit `0` for
`Pass`, `10` for valid `Failed`, and `20` for `Invalid`: a valid `Failed`
document is preserved and later scenarios still run, while `Invalid` aborts
immediately after atomically finalizing its envelope. The aggregate command exits
`10` if any scenario is `Failed`, `20` if any is `Invalid`, and zero only when
every scenario passes. An unexpected composer code is passed to the typed
validator and finalized as `Invalid(CommandFailed)`. An unexpected validator or
scenario-runner code is not reclassified by shell: it aborts `all`, returns `20`,
and leaves the whole attempt incomplete and ineligible for selection.

Factor the status aggregation, process-group cleanup, and atomic-output machinery
into sourceable shell functions, but keep authoritative preflight and reference
composition outside the substitution boundary. Also factor the output-root
containment predicate into one sourceable `validate_output_containment` helper.
Normal `main` passes its frozen canonical repository root plus the complete
frozen linked-worktree root vector; the source-only `output-containment`
operation passes the Rust-parent-supplied canonical roots. The helper rejects an
output equal to or nested below any protected root with the exact diagnostic
`error: --output-dir must be outside the repository and all linked worktrees`.
No call site copies the predicate. When executed normally, the
script always invokes `authoritative_preflight`; no documented or hidden flag or
environment variable can replace it. For ordinary Linux CI only,
`tests/workload_harness.rs` invokes the frozen native Controller's
`launch-runner-source-fixture` grammar with canonical Bash as program, the exact source-fixture
caller allowlist including `HERDR_PERF_RUNNER_TEST_LIBRARY_ONLY=1`, and child
argv `["-p", "-c", SOURCE_FIXTURE_BODY, "herdr-i5-source-fixture",
frozen_runner_script.canonical_path]`. The Controller establishes the empty
environment and injects the six identity values plus only the attested
`HERDR_INCREMENT5_BOOTSTRAP_TOOLS_SOURCE_FIXTURE_V1` role manifest. Tests use
temporary shims for portable roles unavailable on the generic runner and prove
that the fixture has no workstation-path dependency.
`SOURCE_FIXTURE_BODY` is fixed source that accepts only that absolute positional
canonical operand plus the closed operation operands, sources only that
canonical path without running `main`,
and dispatches `orchestration` to the source-only `run_orchestration_fixture`
with explicit fixture callbacks or `output-containment` to
`run_output_containment_fixture`. The latter calls the same
`validate_output_containment` helper before any authoritative-preflight,
composition, child-launch, or output creation. Direct script execution with the
library-only variable set is rejected. The
fixture path writes only a closed `RunnerTestOutcomeV1` carrying
`non_authoritative: true` under the test's temporary directory, never calls the
reference composer, never creates `result-v1.json`, and cannot deserialize as
`ReferenceOutcomeV1`. Its validator requires `schema_version == 1`, literal
`non_authoritative == true`, an exit code in `{0,10,20}`, and
`all_process_groups_reaped == true` whenever a process group was started. Tests
assert those boundaries. Thus generic CI can exercise
the real aggregation, timeout, signal, kill, wait, and atomic-rename code while
only a normal invocation that passes the real host/tool/storage preflight can
produce selectable measurement evidence.

`source_fixture_uses_frozen_canonical_runner_operand` freezes a stable requested
symlink, its canonical runner target, and their identity evidence. Its success
row proves the child argv and `BASH_SOURCE` use the frozen canonical target while
the requested alias remains separately recorded. It then repoints the requested
symlink after freezing and requires the native Controller to exit `20` before
Bash execution or output creation. Passing the requested symlink itself as the
source operand is forbidden even when it currently resolves to the same bytes.

The fixed body is exactly:

```bash
set -euo pipefail
case $- in *p*) ;; *) exit 20 ;; esac
runner_script=$1
shift
readonly runner_script
source "$runner_script"
fixture_operation=$1
shift
readonly fixture_operation
case "$fixture_operation" in
  orchestration) run_orchestration_fixture "$@" ;;
  output-containment) run_output_containment_fixture "$@" ;;
  *) exit 20 ;;
esac
```

The source-level inventory rejects `set +p` in both this body and the runner.

After attempt-ID containment and executable freezing, resolve `--subject`
exactly with the frozen Git executable's `rev-parse --verify
"$subject^{commit}"` and replace the user spelling with that full commit ID.
Resolve `HEAD` once in the same preflight. Require `--stage
baseline` if and only if the resolved subject is the pinned baseline SHA; that
mode rejects `--baseline-results-root`. Both `--stage post-reliability` and
`--stage final` require the resolved subject to equal the preflight `HEAD` and
require `--baseline-results-root`. Resolve it before any trial,
require it outside the
repository and linked worktrees, and validate the exact
`<root>/<mapped-snake-case-scenario>/result-v1.json` plus raw root before
accepting the candidate.
Every invocation uses an immutable attempt-qualified output basename
`<stage-label>-<subject12>-attempt-<eight-digit-positive-decimal>`, matching
`^(baseline|post-reliability|final)-[0-9a-f]{12}-attempt-[0-9]{8}$` with
`00000000` rejected. The stage and subject components must equal the parsed
arguments rather than merely matching the shape. Before the
command, the Controller chooses the next explicit attempt ID, records the ID and
prospective root in the research manifest/change log, and exports it as
`HERDR_INCREMENT5_ATTEMPT_ID`; that Controller-only variable is never forwarded
to the measured process or serialized as a run control. An `Invalid` result or
interruption permanently closes that attempt without deletion or reuse. A retry
uses a new ID and a new root. After typed validation, the research manifest may
select exactly one `Pass` or `Failed` attempt for each stage/subject pair;
`Invalid` and incomplete attempts are never selectable. Every later baseline,
comparison, or classifier command consumes the selected root explicitly from the
manifest through `HERDR_INCREMENT5_SELECTED_*_ROOT`; no command discovers an
attempt by listing or globbing measurement directories.

Immediately after shell-only argument and attempt-ID parsing, before the first
external command, the runner calls one sourceable containment function. Both
normal `main`, `run_orchestration_fixture`, and
`run_output_containment_fixture` call this same function before any child launch
or output creation; no copy of the logic exists at any call site:

```bash
contain_attempt_id() {
  runner_attempt_id="${HERDR_INCREMENT5_ATTEMPT_ID:?missing attempt ID}"
  export -n runner_attempt_id
  unset HERDR_INCREMENT5_ATTEMPT_ID
  readonly runner_attempt_id
}
```

All grammar and basename checks use `runner_attempt_id`. Every measured,
observer, validator, and orchestration-fixture child is therefore launched only
after the inherited name is absent. The source-only Linux fixture explicitly
exports a valid nonzero eight-digit `HERDR_INCREMENT5_ATTEMPT_ID`, calls
`contain_attempt_id`, asserts the retained non-exported `runner_attempt_id`, and
then either runs the portable output-containment operation without a child or
runs the frozen canonical `env` executable through the real build,
orchestrator, measured-child, observer, control-writer, composer, and validator
callbacks.
It asserts that none of those child environments contains
`HERDR_INCREMENT5_ATTEMPT_ID`; this is a binding containment test, not only a
serialized-control check and not a fixture that bypasses the production seam.
The same source-only fixture keeps sentinel values for `RUSTFLAGS`,
`RUSTC_WRAPPER`, `LD_PRELOAD`, `LD_LIBRARY_PATH`, `LOCPATH`, `GCONV_PATH`,
`BASH_ENV`, `ENV`, hostile `SHELLOPTS`/`BASHOPTS`, `GIT_DIR`, `GIT_WORK_TREE`,
`GIT_CONFIG_GLOBAL`, `LANG`, `LC_CTYPE`, `CARGO_PROFILE_RELEASE_LTO`,
`CARGO_PROFILE_RELEASE_CODEGEN_UNITS`, and a `BASH_FUNC_*%%` definition as inert
Rust strings. It stages them on each child `Command` builder before invoking the
real closure helper; that helper calls `env_clear()` and then adds the exact
allowlist. It also rejects any nested-body helper whose name collides with an
invoked builtin. The test never changes its trusted Rust parent's environment.
It then exercises the real build-command, orchestrator, measured-child, observer,
control-writer, composer, and validator environment constructors. A recording executable asserts each
child receives exactly its declared allowlist keys and values, none of the
sentinels or exported functions, and no additional inherited key. It also mutates
the built executable fixture after its first recorded hash and requires the next
trial to fail with `ControlMismatch`. The observer and validator remain outside
the measured process tree, but their authoritative evidence still comes from
closed, byte-recorded environments.

The attempt-qualified invocation output root must not yet exist; require its parent to exist,
canonicalize that parent, validate the basename grammar above, and use that
prospective canonical path before creating anything. In `PostReliability` and
`Final`, canonicalize the existing baseline root and reject equality,
baseline-as-ancestor-of-output, and output-as-ancestor-of-baseline on
path-component boundaries. After creating the
output root, canonicalize it again and require exact equality with the
prospective path, which rejects symlink aliases. Unit tests cover equal roots,
both nesting directions, a symlink alias, a malformed/nonpositive attempt ID,
reuse of an existing attempt root, and successful retry only through a distinct
recorded attempt root before any trial command can run.
For `--scenario all`, validate all seven baseline documents up front and require
their baseline IDs to agree and their stages to be `Baseline`. The pinned
baseline run rejects rather than ignores
an accidental `--baseline-results-root`, so baseline and candidate modes cannot
be confused. `Final` applies the final-only render-evidence rules;
`PostReliability` rejects that evidence because the runtime performance surface
has not landed yet.

For each non-startup scenario, run one unrecorded warm-up followed by five
recorded trials. Run startup as one unrecorded warm-up plus ten recorded fresh
processes. Retain every event latency sample. Every recorded trial must satisfy
losslessness and its own thresholds; do not average away a failing trial, select
the best trial, or discard an outlier.

Before building, execute exactly these tracked-only checks:

```bash
preflight_head="$("$git_executable" rev-parse --verify 'HEAD^{commit}')"
[[ "$preflight_head" =~ ^[0-9a-f]{40}$ ]]
harness_sha="$preflight_head"
readonly preflight_head harness_sha
"$git_executable" diff --quiet --exit-code
"$git_executable" diff --cached --quiet --exit-code
test "$("$git_executable" rev-parse HEAD)" = "$preflight_head"
test "$subject" = "$preflight_head" # post-reliability/final only
"$git_executable" diff --quiet "$subject" -- Cargo.lock 'src/**' \
  ':(exclude)src/herdr/controller.rs' ':(exclude)src/herdr/collector.rs' \
  ':(exclude)src/reducer.rs' ':(exclude)src/operator.rs' \
  ':(exclude)src/store/mod.rs' ':(exclude)src/tui/app.rs'
```

Do not call Git `status`, Git `clean`, `find` at repository root, or any command
that enumerates untracked paths. Verify the output directory is not below any
path printed by the frozen Git executable's `worktree list --porcelain` before
creating result files.
The baseline keeps the marker-bounded harness proof below; the two later stages
do not use that exclusion as a substitute for subject identity. Recheck both
tracked-clean predicates and exact `HEAD == preflight_head` immediately before
final result promotion. `PostReliability` and `Final` additionally require
`preflight_head == subject`; Baseline instead re-runs its pinned-subject and
marker-bounded harness proof, because its production subject intentionally
precedes the harness revision. A source-only fixture supplies a
clean commit that differs from HEAD only in a marker-owned excluded path and
requires post-reliability/final rejection before build or trial execution.

Resolve the requested launcher `/home/mageyuki/.cargo/bin/rustup` under the
unified executable-identity policy below; its canonical path must contain no
`mise` component. Obtain versions only with the closed build environment below.
Build and version commands inherit only the direct-launch allowlist established
above; the already-clean runner may additionally invoke frozen canonical
`env -i` with that exact allowlist
`HOME=/home/mageyuki`, `RUSTUP_HOME=/home/mageyuki/.rustup`,
`CARGO_HOME=/home/mageyuki/.cargo`, `PATH=/usr/bin:/bin`, `LC_ALL=C`, and
`TZ=UTC`; ambient `RUSTFLAGS`, `RUSTC_WRAPPER`, `LD_PRELOAD`, and
`CARGO_PROFILE_*` values can never reach them. Invoke the frozen canonical
rustup target as `"$rustup_executable" run 1.97.1 rustc --version` and
`"$rustup_executable" run 1.97.1 cargo --version` inside that same envelope.
Before any Cargo invocation, freeze its canonical working directory.
Without listing a directory, construct the ordered, duplicate-free candidate
vector for both `.cargo/config` and `.cargo/config.toml` at that directory and
every parent through `/`, followed by the same two names under the fixed
`CARGO_HOME` when not already present. Test each exact path with no-follow
existence semantics and reject a regular entry, symlink, or dangling symlink.
This v1 reject-all policy, the invocation cwd, and the complete ordered
absent-candidate vector are the `CargoConfigurationPolicyV1` evidence; tracked
cleanliness is not a substitute. Fixtures cover the legacy name, both names,
fixed `CARGO_HOME`, and a config in an external ancestor. Use the frozen
canonical launcher and environment for every authoritative local build/test
invocation and record its identity, toolchain name, exact versions, build
environment, Cargo policy, measured-binary identity, and authoritative
executable identities only in `RunControlsV1`. Never invoke bare `cargo`,
`rustc`, `rustup`, or `mise`.

The single measured executable is built with this exact command shape:

```bash
"$env_executable" -i HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup \
  CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC \
  "$rustup_executable" run 1.97.1 cargo test --release --locked \
    --features workload-harness --test workload_harness --no-run \
    --message-format=json >"$cargo_artifact_json"
```

Select exactly one absolute requested test-binary path with the frozen canonical
jq executable and this exact query:

```bash
set +e
measured_binary_requested="$(
  "$jq_executable" --exit-status --slurp --raw-output \
    --arg manifest "$canonical_manifest_path" '
      [ .[]
        | select(
            .reason == "compiler-artifact"
            and .manifest_path == $manifest
            and .target.name == "workload_harness"
            and .target.kind == ["test"]
            and .profile.test == true
            and (.executable | type == "string")
            and (.executable | startswith("/"))
          )
        | .executable
      ]
      | if length == 1 then .[0]
        else error("expected exactly one absolute workload_harness test executable")
        end
    ' "$cargo_artifact_json"
)"
jq_status=$?
set -e
[ "$jq_status" -eq 0 ] || exit "$jq_status"
```

Fixtures must reject zero or multiple candidates, a wrong manifest, target name,
target kind, or test profile, a null/non-string executable, and a relative path.
Treat any jq status other than zero as an operational/selection failure; no
fallback search or last-candidate selection is allowed. Apply the unified
executable-identity policy. The requested path may be a symlink; its canonical
target must be a regular executable. Compute the requested path, canonical path,
and SHA-256 as its `ExecutableIdentityV1`, and record it only in `RunControlsV1`.
Revalidate the requested-to-canonical mapping and rehash the canonical target
before every warm-up and recorded trial and record
that matching observation in `TrialControlEvidenceV1`;
any path, metadata, or digest change is `ControlMismatch`. The measured child is
launched directly by that absolute path via the frozen canonical `env -i` with the exact
fixed keys above plus the declared per-trial `HERDR_PERF_*` keys—never through
Cargo and never with an inherited environment.
After building the test executable, run its ignored
`verify_subject_diff_is_harness_only` test. It accepts only the exact
`workload-harness` Cargo feature and native-Controller `[[bin]]` stanza, the
declared Task 1A.1/1A.2 common-test, fixture/MANIFEST, workload test,
`tests/support/reference_profile_controller.rs`, script, and CI paths, the
two exact non-production planning paths
`docs/internal/superpowers/specs/2026-08-12-increment-5-reliability-performance-design.md`
and
`docs/internal/superpowers/plans/2026-08-12-increment-5-reliability-performance.md`, and
feature-only hunks wholly bounded by named `increment5-workload-harness` markers
in controller/collector/reducer/operator/store-mod/app. All added state/calls are compiled only
under that feature, and the no-feature path retains the same call order and
results. It rejects a
dependency/version change, Cargo lock change, non-marker production hunk, or any
other tracked path. The design path must hash to
`17dfeb91a2ce0efeff7a6c79bcac345e7ca051f268ed0c39c57ad297e38035f4`;
both planning paths must be tracked, clean, and byte-identical to their HEAD
blobs. This avoids a self-referential plan-digest literal while preventing
uncommitted artifact substitution. This is how the baseline proves that default
production behavior still matches `9cd9813` while permitting test-only admission
timestamps and non-production planning artifacts.
This harness-only production-diff proof is intentionally a baseline-only gate.
Post-reliability and final invocations pass the clean current HEAD as `--subject`,
so their subject diff is empty by construction; those later stages rely instead
on both tracked-clean checks, the recorded subject SHA, the already-validated
baseline artifact, and the integrated task test/review history. They must not be
reported as re-proving an untouched production diff.

The runner tests the preliminary pathspec and typed marker verifier together by
constructing one fixture diff for each of the six marker-owned production files.
Each exact in-marker hunk is accepted, including `src/reducer.rs`,
`src/operator.rs`, and `src/store/mod.rs`; an out-of-marker hunk in each file,
any seventh production file, and any Cargo lock/dependency change
are rejected. This prevents the broad preliminary exclusion from weakening the
marker boundary while proving that every declared Task 1B.1/1B.2/1B.3 hook file
can reach the
typed verifier.

Apply one executable-identity policy to every authoritative external command.
The closed `authoritative-v1` requested-path inventory is
`/home/mageyuki/.cargo/bin/rustup` and `/usr/bin/{awk,bash,env,findmnt,git,id,jq,
lsblk,lscpu,mkdir,mktemp,mv,pidstat,prlimit,readlink,rg,rmdir,setsid,sha256sum,
sleep,stat,taskset,time,uname,unlink}`. A requested path may be a symlink, but its
canonical target must be a regular executable; record requested path, canonical
path, and lowercase SHA-256, then invoke only the frozen canonical path. Reject
resolution/digest drift before every authoritative use and reject a rustup
canonical path containing a `mise` component. In addition, freeze the absolute
runner script as `RunControlsV1.runner_script`; it is outside the fixed external
command inventory but receives the same requested/canonical path, mode, regular
target, and SHA-256 checks. The runner shebang is exactly
`#!/usr/bin/bash -p`, and the script plus source-fixture body reject `set +p`.
Preflight additionally requires that requested interpreter path equal its
canonical path on the selected host. Enabled Bash builtins are
exempt because they resolve before PATH executables. Every authoritative shell
boundary is environment-closed before it can import a same-named function, and
the source-only fixture rejects nested-body helper definitions that collide with
an invoked builtin; an explicit `builtin` prefix is optional defense, not an
external-executable identity requirement.
The native Controller, using Rust `std::fs::canonicalize`, file metadata, and the
already-declared `sha2` dependency, derives the complete inventory and passes it
through `HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1`; Bash never bootstraps this list by
calling an unverified host helper. Native-attested canonical `readlink` and
`sha256sum` are the two initial verification roots described above. All other
entries are revalidated before first use, and every later authoritative use,
against the same immutable manifest. A source inventory test proves that no
external call can occur before the builtin parser and these two bound roots.
Table-driven Controller tests reject `authoritative-v1`/`source-fixture-v1`
substitution, both tool-manifest variables together, a missing/extra/reordered
portable role, path/digest mutation, a workstation path in portable mode, and a
portable manifest presented to normal `launch-runner`.

Authoritative preflight calibrates, rather than assumes, frozen `pidstat -e`
child-status behavior before any warm-up. Through the same native Controller and
closed environment, invoke frozen `pidstat` with frozen Bash children that exit
`0` and `23`, capture both outer statuses, and accept only `(0,23)` as
`PidstatChildStatusModeV1::PropagatesChildStatus` or `(0,0)` as
`PidstatChildStatusModeV1::MonitorOnly`. Any other pair or malformed diagnostic
output fails preflight. Record the mode in `RunControlsV1` and re-run the probe,
requiring the same pair, before every authoritative attempt. Source-fixture
tests exercise both modes with attested shims and every inconsistent pair
without claiming reference-host behavior.

The feature-gated native Controller is a built subject artifact rather than a
fixed host command, so it is frozen through the exact Cargo selector and
self-identity contract above and recorded separately in
`authoritative_executables`; it is not discovered through this host-path list.
If implementation needs another external command, add it to this closed list,
schema evidence, and hostile-path test before use. A source-level inventory test
rejects every undeclared or bare external-command call, including the shebang,
Git, canonicalization/hash helpers, `mktemp`, and `sleep`; a hostile `PATH`
fixture stages same-named sentinels only on child command builders and proves
none executes. Every decisive search invokes the frozen canonical `/usr/bin/rg`
identity with `--no-config` and handles status `0` as match, `1` as no match, and
every other status as an operational failure; a pipeline or `wc` may not mask the
rg status. Redundant nested
closure is optional only when the caller is already proven to run under the
same recorded closed map; no authoritative observer or validator may inherit
from an unclosed caller. Their executable selection and identity remain binding.

Fail closed unless the frozen `uname` identifies Linux x86_64, frozen `lscpu` identifies an AMD
Ryzen 7 5700X, CPUs 0-3 map to four unique package/core pairs, and the actual
per-trial state/scratch storage resolves to NVMe. Create each canonical scratch
root as `<attempt-root>/<mapped-scenario>/<trial-or-warm-up>/scratch`, pass that
exact path as `HERDR_PERF_SCRATCH_ROOT`, and require every harness database,
provider scratch directory, and temporary state root for that trial to be a
descendant of it rather than using `tempfile::tempdir()`'s default location.
Obtain the scratch root mount's `MAJ:MIN` with
`findmnt -T "$trial_scratch_root" -rno MAJ:MIN`; reject whitespace or a value other than
one `<major>:<minor>` token, then resolve `/sys/dev/block/<major>:<minor>`;
recursively follow every entry under each device's `slaves/`; normalize a leaf
partition to its parent whole-disk sysfs node; and require every leaf disk's
basename to match `^nvme[0-9]+n[0-9]+$` and its `queue/rotational` value to be
exactly `0`. Missing/ambiguous sysfs nodes, an empty slave set for a virtual
device, mixed backing types, or any non-NVMe/rotational leaf is
`ControlMismatch`. Frozen `lsblk -a` is recorded only as diagnostic metadata; the
major/minor-to-sysfs traversal is authoritative because it remains unambiguous
across device-name and namespace presentation differences. Repeat this check for
every warm-up and recorded scratch root; `TrialControlEvidenceV1` records its
canonical scratch root, `"nvme"`, and the sorted, duplicate-free leaf-device vector.
`HostProfileV1.storage_kind` is `"nvme"` and `storage_device` is the comma-joined
sorted union of those same leaves; composition rejects disagreement between any
trial and the run envelope. Record, but do not modify, governor and boost metadata.
Set `LC_ALL=C`. Before each trial, create a short private runtime directory with
`mktemp -d /tmp/herdr-i5.XXXXXXXX`, require `lstat` to report a real directory
rather than a symlink, require owner UID equal to the effective UID and mode
exactly `0700`, canonicalize it, and choose the socket basename from a closed
short scenario code plus the warm-up/recorded index. The exact scenario mapping
is `target=t`, `sustained=s`, `burst=b`, `startup=u`, `idle=i`,
`fallback-rescan=f`, and `twice-target=x`; use `w` for warm-up or `t` for a
recorded trial and four decimal digits, for example `s-t0001.sock`.
Require the prospective absolute pathname to be at most 107 bytes, contain no
NUL, and not exist before either endpoint starts. Pass that absolute path as
`HERDR_PERF_OBSERVER_CONTROL_SOCKET`; do not place the Unix socket below the
attempt tree. The short runtime path is rendezvous plumbing, not a recorded run
control or selectable artifact.

The outer runner owns an idempotent trap from immediately after `mktemp`; the
inner orchestration also receives the exact runtime directory as positional
argument 18. Cleanup terminates and reaps both process groups first, unlinks only
the exact socket pathname with `unlink --`, and then removes only the known-empty
owned runtime directory with `rmdir --`. It never recursively deletes or scans a
directory. Both layers tolerate an already-unlinked socket, but reject a changed
owner, mode, symlink, unexpected directory entry, or nonempty directory instead
of deleting it. Ordinary Linux tests cover the maximum accepted 107-byte path,
reject 108 bytes before bind, verify ownership/mode/symlink checks, and prove
normal, timeout, `INT`, `TERM`, and `HUP` paths leave neither the exact socket nor
the runtime directory behind. Adversarial tuple-change and unexpected-entry
fixtures instead require `Invalid(CommandFailed)` and prove the mismatched path
is left untouched for diagnosis.

Before launching either group, freeze the runtime directory's no-follow
`(device,inode,uid,mode,type)` tuple. After the measured root has bound the
control socket and atomically published its handshake, but before the observer
sends `Ready`, freeze the socket's no-follow
`(device,inode,uid,mode,type)` tuple and require type `socket` plus the effective
UID. The shell body defines `safe_cleanup_runtime_socket` and
`safe_cleanup_runtime_dir`: each treats an absent path as already cleaned, but a
present path must still match its frozen tuple exactly before the helper invokes
`unlink --` or `rmdir --`. The directory helper never enumerates entries;
`rmdir --` itself rejects an unexpected entry or nonempty directory. Any tuple
mismatch or removal failure changes the preserved trial status to `20`, so the
orchestrator publishes `failed:20` and the typed validator writes
`Invalid(CommandFailed)` rather than suppressing cleanup failure.

Execute each recorded trial with this process topology (the
runner implements the body as a Bash function so all `wait` statuses are
retained):

```bash
local pidstat_status
orchestrator_environment=(
  HOME=/home/mageyuki
  RUSTUP_HOME=/home/mageyuki/.rustup
  CARGO_HOME=/home/mageyuki/.cargo
  PATH=/usr/bin:/bin
  LC_ALL=C
  TZ=UTC
)
case "$scenario" in
  target|sustained|twice-target) trial_deadline_seconds=180 ;;
  burst|fallback-rescan) trial_deadline_seconds=120 ;;
  startup) trial_deadline_seconds=300 ;;
  idle) trial_deadline_seconds=90 ;;
  *) exit 20 ;;
esac
trial_status_output="$trial_raw_root/trial-status"
test ! -e "$trial_status_output" && test ! -L "$trial_status_output"
set +e
"$env_executable" -i "${orchestrator_environment[@]}" \
  "$taskset_executable" -c 4-7,12-15 \
  "$pidstat_executable" -u -r -T ALL -o JSON 1 -e "$bash_executable" -p -c '
  set -euo pipefail
  time_output=$1
  test_binary=$2
  child_stdout=$3
  child_stderr=$4
  scenario=$5
  subject=$6
  harness_output=$7
  stage=$8
  baseline_results_root_arg=$9
  observer_handshake=${10}
  observer_control_socket=${11}
  observer_control_output=${12}
  process_tree_output=${13}
  observer_stdout=${14}
  observer_stderr=${15}
  trial_deadline_seconds=${16}
  trial_scratch_root=${17}
  trial_runtime_dir=${18}
  trial_status_output=${19}
  stat_executable=${20}
  unlink_executable=${21}
  rmdir_executable=${22}
  sleep_executable=${23}
  setsid_executable=${24}
  env_executable=${25}
  taskset_executable=${26}
  prlimit_executable=${27}
  time_executable=${28}
  id_executable=${29}
  mv_executable=${30}
  readonly time_output test_binary child_stdout child_stderr scenario subject
  readonly harness_output stage baseline_results_root_arg observer_handshake
  readonly observer_control_socket observer_control_output process_tree_output
  readonly observer_stdout observer_stderr trial_deadline_seconds trial_scratch_root
  readonly trial_runtime_dir trial_status_output stat_executable unlink_executable rmdir_executable
  readonly sleep_executable setsid_executable env_executable taskset_executable
  readonly prlimit_executable time_executable id_executable mv_executable
  safe_cleanup_runtime_socket() {
    local current
    if [ ! -e "$observer_control_socket" ] && [ ! -L "$observer_control_socket" ]; then return 0; fi
    [ -n "$frozen_socket_identity" ] || return 1
    current="$("$stat_executable" --format="%d:%i:%u:%f:%F" -- "$observer_control_socket")" || return 1
    [ "$current" = "$frozen_socket_identity" ] || return 1
    "$unlink_executable" -- "$observer_control_socket"
  }
  safe_cleanup_runtime_dir() {
    local current
    if [ ! -e "$trial_runtime_dir" ] && [ ! -L "$trial_runtime_dir" ]; then return 0; fi
    [ -n "$frozen_runtime_dir_identity" ] || return 1
    current="$("$stat_executable" --format="%d:%i:%u:%f:%F" -- "$trial_runtime_dir")" || return 1
    [ "$current" = "$frozen_runtime_dir_identity" ] || return 1
    "$rmdir_executable" -- "$trial_runtime_dir"
  }
  publish_trial_status() {
    local status=$1 token temporary
    case "$status" in
      0) token=ok:0 ;;
      ''|*[!0-9]*) return 1 ;;
      *) [ "$status" -ge 1 ] && [ "$status" -le 255 ] || return 1
         token="failed:$status" ;;
    esac
    temporary="${trial_status_output}.tmp.${BASHPID}"
    [ ! -e "$trial_status_output" ] && [ ! -L "$trial_status_output" ] || return 1
    [ ! -e "$temporary" ] && [ ! -L "$temporary" ] || return 1
    builtin printf "%s\n" "$token" >"$temporary" || return 1
    "$mv_executable" -T -- "$temporary" "$trial_status_output"
  }
  measured_wrapper_pid=
  observer_pid=
  watchdog_pid=
  frozen_runtime_dir_identity="$("$stat_executable" --format="%d:%i:%u:%f:%F" -- "$trial_runtime_dir")"
  frozen_socket_identity=
  cleanup_trial() {
    trial_status=$?
    trap - EXIT INT TERM HUP USR1
    if [ -n "${watchdog_pid:-}" ]; then
      kill "$watchdog_pid" 2>/dev/null || true
      wait "$watchdog_pid" 2>/dev/null || true
    fi
    for group in "${measured_wrapper_pid:-}" "${observer_pid:-}"; do
      [ -n "$group" ] || continue
      kill -TERM -- "-$group" 2>/dev/null || true
    done
    for ((attempt=0; attempt<100; attempt++)); do
      any_live=false
      for group in "${measured_wrapper_pid:-}" "${observer_pid:-}"; do
        [ -n "$group" ] || continue
        if kill -0 -- "-$group" 2>/dev/null; then any_live=true; fi
      done
      [ "$any_live" = false ] && break
      "$sleep_executable" 0.01
    done
    for group in "${measured_wrapper_pid:-}" "${observer_pid:-}"; do
      [ -n "$group" ] || continue
      kill -KILL -- "-$group" 2>/dev/null || true
    done
    for child in "${measured_wrapper_pid:-}" "${observer_pid:-}"; do
      [ -n "$child" ] || continue
      wait "$child" 2>/dev/null || true
    done
    safe_cleanup_runtime_socket || trial_status=20
    safe_cleanup_runtime_dir || trial_status=20
    publish_trial_status "$trial_status" || trial_status=20
    exit "$trial_status"
  }
  trap cleanup_trial EXIT
  trap "exit 130" INT
  trap "exit 143" TERM HUP
  trap "exit 124" USR1
  orchestrator_pid=$BASHPID
  (
    trap - EXIT INT TERM HUP USR1
    "$sleep_executable" "$trial_deadline_seconds"
    kill -USR1 "$orchestrator_pid" 2>/dev/null || true
  ) &
  watchdog_pid=$!
  measured_environment=(
    HOME=/home/mageyuki
    RUSTUP_HOME=/home/mageyuki/.rustup
    CARGO_HOME=/home/mageyuki/.cargo
    PATH=/usr/bin:/bin
    LC_ALL=C
    TZ=UTC
    HERDR_PERF_SCENARIO="$scenario"
    HERDR_PERF_SUBJECT="$subject"
    HERDR_PERF_OUTPUT="$harness_output"
    HERDR_PERF_STAGE="$stage"
    HERDR_PERF_OBSERVER_HANDSHAKE="$observer_handshake"
    HERDR_PERF_OBSERVER_CONTROL_SOCKET="$observer_control_socket"
    HERDR_PERF_SCRATCH_ROOT="$trial_scratch_root"
  )
  if [ "$baseline_results_root_arg" != - ]; then
    measured_environment+=(HERDR_PERF_BASELINE_RESULTS_ROOT="$baseline_results_root_arg")
  fi
  "$setsid_executable" "$env_executable" -i "${measured_environment[@]}" \
    "$taskset_executable" -c 0-3 "$prlimit_executable" --as=17179869184 \
    "$time_executable" -v -o "$time_output" \
    "$test_binary" reference_profile_entrypoint --exact --ignored --test-threads=1 \
    >"$child_stdout" 2>"$child_stderr" &
  measured_wrapper_pid=$!
  for ((attempt=0; attempt<500; attempt++)); do
    [ -s "$observer_handshake" ] && break
    kill -0 "$measured_wrapper_pid" 2>/dev/null || break
    "$sleep_executable" 0.01
  done
  [ -s "$observer_handshake" ] || exit 20
  [ -S "$observer_control_socket" ] && [ ! -L "$observer_control_socket" ] || exit 20
  [ "$("$stat_executable" --format=%u -- "$observer_control_socket")" = "$("$id_executable" -u)" ] || exit 20
  frozen_socket_identity="$("$stat_executable" --format="%d:%i:%u:%f:%F" -- "$observer_control_socket")" || exit 20
  IFS=" " read -r observed_root_pid observed_start_ticks trial_origin_ns <"$observer_handshake"
  observer_environment=(
    HOME=/home/mageyuki
    RUSTUP_HOME=/home/mageyuki/.rustup
    CARGO_HOME=/home/mageyuki/.cargo
    PATH=/usr/bin:/bin
    LC_ALL=C
    TZ=UTC
    HERDR_PERF_SCENARIO="$scenario"
    HERDR_PERF_OBSERVED_ROOT_PID="$observed_root_pid"
    HERDR_PERF_OBSERVED_ROOT_START_TICKS="$observed_start_ticks"
    HERDR_PERF_TRIAL_ORIGIN_NS="$trial_origin_ns"
    HERDR_PERF_OBSERVER_CONTROL_SOCKET="$observer_control_socket"
    HERDR_PERF_OBSERVER_CONTROL_OUTPUT="$observer_control_output"
    HERDR_PERF_PROCESS_TREE_OUTPUT="$process_tree_output"
  )
  "$setsid_executable" "$env_executable" -i "${observer_environment[@]}" \
    "$test_binary" reference_profile_process_tree_observer --exact --ignored \
      --test-threads=1 >"$observer_stdout" 2>"$observer_stderr" &
  observer_pid=$!
  set +e
  wait "$measured_wrapper_pid"; measured_status=$?
  wait "$observer_pid"; observer_status=$?
  set -e
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  watchdog_pid=
  if [ "$measured_status" -ne 0 ]; then
    exit "$measured_status"
  fi
  if [ "$observer_status" -ne 0 ]; then
    exit "$observer_status"
  fi
  exit 0
' herdr-i5-orchestrator "$time_output" "$test_binary" "$child_stdout" "$child_stderr" \
  "$scenario" "$subject" "$harness_output" "$stage" \
  "${baseline_results_root:--}" "$observer_handshake" \
  "$observer_control_socket" "$observer_control_output" \
  "$process_tree_output" "$observer_stdout" "$observer_stderr" \
  "$trial_deadline_seconds" "$trial_scratch_root" "$trial_runtime_dir" \
  "$trial_status_output" \
  "$stat_executable" "$unlink_executable" "$rmdir_executable" \
  "$sleep_executable" "$setsid_executable" "$env_executable" \
  "$taskset_executable" "$prlimit_executable" "$time_executable" \
  "$id_executable" "$mv_executable" \
  >"$pidstat_output" 2>"$pidstat_stderr"
pidstat_status=$?
set -e
declare -r pidstat_status
```

Immediately after `pidstat` returns, the outer runner opens the exact
`trial_status_output` with Bash's own redirection and `read -r` on one dedicated
file descriptor: the first read must end at a newline and a second read must hit
EOF. It accepts only literal `ok:0` or `failed:<canonical-1..255>`, rejects a
symlink/non-regular file, additional bytes or lines, leading zeros, whitespace,
and reuse from another trial, and closes the descriptor. It uses no `cat`,
regular expression process, JSON, or path discovery. Typed Rust independently
performs the same byte-level parse in both control writer and validator.
The `local pidstat_status` declaration, sentinel parse, control-evidence writer,
and thirteen-artifact hashing all remain inside the same per-trial runner
function. Consequently every invocation receives a fresh function-local readonly
status while `pidstat_status`, `pidstat_child_status_mode`, and
`trial_status_output` remain in scope through control composition; no recorded
trial can inherit a prior trial's readonly variable.

Serialize `orchestrator_environment`, `observer_environment`, and the
pre-composition `validator_environment_template` exactly into the typed
`runner-control.json`; the first two sorted maps must equal the arrays passed to
the frozen canonical `env -i` calls, while the template excludes only the later
candidate/output/composer-status/trial-status transport keys named above. It
also records the typed sentinel value and captured `pidstat_status`. Per-trial
paths and scenario values remain in their owning observer/validator maps or in
the nested body's positional arguments, never in an ambient parent environment.

The two cleanup helpers above are definitions inside the exact nested
`bash -p -c` body, before their first use; the runner does not rely on an
unexported outer-shell function and does not use `export -f`. The first statements
capture all 30 positional arguments exactly once into readonly descriptive
variables. After that binding block, helpers, traps, and orchestration refer only
to those names; no numeric positional reference remains. The source-only fixture
extracts the nested body, permits numeric positional syntax only in the contiguous
binding block, rejects `\$[0-9]` or `\$\{[0-9]+\}` anywhere after its final
`readonly`, and then exercises timeout/signal cleanup so function-local positional
shadowing cannot recur. Argument 19 is the unique `trial-status` path and
arguments 20-30 are the already frozen canonical executable paths; the nested
body has no ambient-path command lookup. Requested `/usr/bin/stat`,
`/usr/bin/id`, `/usr/bin/unlink`, `/usr/bin/rmdir`, and `/usr/bin/mv` may be symlinks only under
the unified identity policy. The source-only
`run_orchestration_fixture` executes this same body and inventories these exact
definitions: an absent path succeeds, an exact frozen socket/directory tuple is
removed, a changed tuple is preserved and fails, and a nonempty directory is
preserved when `rmdir` fails. It also requires `publish_trial_status` to emit the
exact one-line token only after all reaping/cleanup, never overwrite an existing
path, and leave a failed temporary rename diagnosable. A fixture may not
substitute duplicate helper implementations.

At entry, the measured Rust process binds the unique per-trial Unix-domain
control socket, writes exactly one space-delimited line
`<pid> <proc-start-time-ticks> <CLOCK_MONOTONIC-ns>` to a temporary file, and
atomically renames it to `observer-handshake`. It then accepts exactly one
observer connection and blocks before setup. The observer validates that
immutable root identity, captures a first root-containing process-tree/RSS
sample, and only then sends the closed `Ready { observer_ready_ns }` frame. A
five-second handshake/Ready deadline, unexpected frame/order, peer closure, or
identity mismatch invalidates
the trial. The observer is a sibling created by the orchestration shell, never a
descendant of the measured root.
The outer `taskset` confines the runner, observer, and diagnostic `pidstat` to
CPUs 4-7 and 12-15; the inner measured command alone overrides affinity to CPUs
0-3. Preflight requires both CPU sets to exist and be disjoint, and the observer
records its effective affinity so evidence with observer execution on a measured
CPU is invalid.
The shell redirects the measured child and observer/libtest to their own unique
stdout/stderr artifacts, so `pidstat` alone owns the JSON stream on the outer
stdout descriptor; the outer stderr descriptor is the distinct preserved
`pidstat-stderr` artifact. All thirteen raw files, including the typed
`runner-control.json` and the canonical plain-text `trial-status`, have unique
trial paths. Each recorded trial hashes all thirteen before composition and
its `TrialResultV1` records those hashes; there are no run-level hashes standing
in for multiple trials. Warm-up files live under distinct `warm-up-*` paths,
remain external evidence, and are explicitly excluded from recorded aggregates.

The scenario table above supplies one hard per-trial deadline, including setup:
180 seconds for Target/Sustained/TwiceTarget, 120 for Burst/FallbackRescan, 300 for
Startup, and 90 for Idle. These are orchestration bounds, never performance
thresholds. `setsid` places the measured wrapper tree and observer in distinct
process groups. Handshake failure, deadline (`USR1`/124), `INT`, `TERM`, `HUP`, or
any ordinary shell exit runs the same idempotent trap: terminate both groups,
wait at most one second, kill survivors, and reap both group leaders plus the
watchdog. Only after cleanup does the orchestrator atomically publish its exact
plain-text status sentinel. The outer runner strictly parses, but never writes
or repairs, each sentinel and constructs `all-ok` or the first
`failed:trial-N:<code>` transport. Composer exit status remains a separate
transport. The validator, not shell, atomically writes
`Invalid(CommandFailed)`; a partial candidate is never promoted. If the
validator itself does not return `0`, `10`, or `20`, the
attempt is incomplete and unselectable even if a file exists. Preflight pins the
installed util-linux `setsid` behavior and
rejects a missing executable.

`runner_fixture_preserves_measured_and_observer_exit_status_precedence` executes
the real source-fixture orchestration body rather than a preconstructed sentinel.
It requires measured `137` plus observer `0` to produce `failed:137`, measured
`0` plus observer `143` to produce `failed:143`, and simultaneous measured `124`
plus observer `137` to preserve the deterministic measured-first
`failed:124`. The trap, atomic sentinel publication, outer trial transport, and
typed invalid result must all retain that exact status without collapsing it to
boolean status `1`.

The runner uses the closed frozen-`env -i` build command specified above and the
frozen jq executable to select the `workload_harness` executable, then freezes its canonical path
and SHA-256. Every warm-up and recorded trial revalidates that identity and receives unique
raw paths; no file is reused. The external
`reference_profile_process_tree_observer` samples `/proc` every 10 ms, follows
the immutable measured-root identity and its descendants, sums current RSS over
distinct live PIDs in that tree, and converts
each identity's cumulative user/system ticks with `_SC_CLK_TCK`. The observer
calls `sysconf(_SC_CLK_TCK)` once before `Ready`, rejects a nonpositive result,
and records the positive value as `clock_ticks_per_second` in
`ProcessTreeEvidenceV1`. Every conversion uses checked `u128` arithmetic as
`ticks * 1_000_000_000 / clock_ticks_per_second` and rejects multiplication,
division, or checked narrowing failure instead of assuming the host rate. It retains the
last sampled CPU totals and maximum `VmHWM` for every `(pid,start_time)` even
after exit. PID reuse is rejected by start time. The binding CPU values use the
explicit observer-owned boundary fields only; retained `last_*` totals remain
diagnostic.

For `Idle`, after five seconds of input-free settle the measured child sends the
closed child-to-observer command `StartIdleWindow {}` with no timestamp. On
receipt the observer alone records `request_received_ns`, immediately snapshots
every live identity, records zero starts for later identities born no later than
the end, and replies with the observer-authored
`IdleWindowStarted { request_received_ns, start_ns }`. The child remains idle. When the observer's
same monotonic clock reaches at least 30 seconds, it captures live end ticks,
uses retained pre-exit ticks for mid-window exits, freezes the end membership,
and replies `IdleWindowEnded { end_ns }`; later births are excluded. Only after
that acknowledgement may the child serialize/exit. The observer atomically
writes `ObserverControlEvidenceV1` containing the decoded command plus
observer-authored Ready/start/end frames and process-tree
evidence containing the identical boundaries. The harness copies the
acknowledged idle values into `HarnessTrialV1`; validator equality prevents either
side from inventing a window.

Binding CPU is the sum of each included identity's checked explicit
`idle_window_end - idle_window_start` user/system tick delta. Reject regression,
a missing required boundary, post-end inclusion, a short window, or identity
ambiguity, a zero clock-tick rate, or any checked tick-to-nanosecond conversion
failure. Set `elapsed_ns` to the observer's actual idle-window end minus start and compare
`100_000 * (user_cpu_ns + system_cpu_ns) < 2_000 * elapsed_ns` using checked
`u128` arithmetic, excluding all setup/settle CPU and all observer/orchestration
CPU. The binding memory value is the maximum
sampled simultaneous process-tree RSS. The sum of per-identity lifetime
`VmHWM` values is separately named
`sum_process_identity_peak_rss_bytes_diagnostic` and never gates the 100-MB
threshold. GNU-time user/system totals are an external whole-wrapper cross-check
and are never added to the binding process-tree totals, so descendants cannot be
double counted.

Keep the portable manifest, schema, oracle, composer, validator, and deterministic
queue/render tests available on Linux and macOS. Put `/proc`,
`sched_getaffinity`, Linux process-tree sampling, and the ignored measured
entrypoints behind `#[cfg(target_os = "linux")]`; no Linux libc name may appear in
a macOS-compiled item. The shell runner rejects non-Linux before building or
running a measured entrypoint. The macOS all-feature CI still compiles and runs
the portable validation tests, while only an authoritative Linux invocation can
produce a reference profile.

GNU time's verbose totals are an external audit; parse `Maximum resident set
size (kbytes)` with checked `u64 * 1024` before storing bytes. `pidstat` monitors
the wrapper topology and is diagnostic rather than binding. Its one-second
sampling can legitimately produce no row for a short Startup or FallbackRescan
trial; the matrix admits that closed zero-sample representation but Idle requires
samples. The sentinel is authoritative for the orchestrator/child result. Under
`PropagatesChildStatus`, captured `pidstat_status` must equal the sentinel code;
under `MonitorOnly`, it must be zero. Any inconsistent pair or independent
pidstat operational failure invalidates the artifact. Missing or malformed
pidstat JSON still invalidates, and no pidstat wrapper/task RSS is reported as
process-tree maximum.

The measured child records `sched_getaffinity` and `getrlimit(RLIMIT_AS)` in
`HarnessTrialV1`; composition requires exact CPUs `[0,1,2,3]` and
`17_179_869_184` bytes. After each recorded process exits and cleanup completes,
the shell invokes the typed control-evidence writer defined in Task 1A.1. The
following array is the exact direct-child environment; the loop variables name
the current recorded trial and no warm-up is composed:

```bash
control_environment=(
  HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup
  CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC
  HERDR_INCREMENT5_CONTROLLER_REQUESTED="$HERDR_INCREMENT5_CONTROLLER_REQUESTED"
  HERDR_INCREMENT5_CONTROLLER_CANONICAL="$HERDR_INCREMENT5_CONTROLLER_CANONICAL"
  HERDR_INCREMENT5_CONTROLLER_SHA256="$HERDR_INCREMENT5_CONTROLLER_SHA256"
  HERDR_INCREMENT5_RUNNER_REQUESTED="$HERDR_INCREMENT5_RUNNER_REQUESTED"
  HERDR_INCREMENT5_RUNNER_CANONICAL="$HERDR_INCREMENT5_RUNNER_CANONICAL"
  HERDR_INCREMENT5_RUNNER_SHA256="$HERDR_INCREMENT5_RUNNER_SHA256"
  HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1="$HERDR_INCREMENT5_BOOTSTRAP_TOOLS_V1"
  HERDR_PERF_CONTROL_RAW_ROOT="$trial_raw_root"
  HERDR_PERF_CONTROL_OUTPUT="$trial_raw_root/runner-control.json"
  HERDR_PERF_CONTROL_STAGE="$stage"
  HERDR_PERF_CONTROL_SCENARIO="$mapped_scenario"
  HERDR_PERF_CONTROL_SUBJECT="$subject"
  HERDR_PERF_CONTROL_PREFLIGHT_HEAD="$preflight_head"
  HERDR_PERF_CONTROL_TRIAL_INDEX="$trial_index"
  HERDR_PERF_CONTROL_INVOCATION_CWD="$invocation_cwd"
  HERDR_PERF_CONTROL_MEASURED_REQUESTED="$measured_binary_requested"
  HERDR_PERF_CONTROL_MEASURED_CANONICAL="$test_binary"
  HERDR_PERF_CONTROL_MEASURED_SHA256="$measured_binary_sha256"
  HERDR_PERF_CONTROL_TRIAL_STATUS_PATH="$trial_status_output"
  HERDR_PERF_CONTROL_PIDSTAT_EXIT_STATUS="$pidstat_status"
  HERDR_PERF_CONTROL_PIDSTAT_CHILD_STATUS_MODE="$pidstat_child_status_mode"
)
if [ "$stage" != baseline ]; then
  control_environment+=(
    HERDR_PERF_CONTROL_BASELINE_RESULTS_ROOT="$baseline_results_root"
  )
fi
"$env_executable" -i "${control_environment[@]}" \
  "$test_binary" record_runner_control_evidence --exact --ignored \
    --nocapture --test-threads=1
```

For a successful trial, the runner requires control-writer status zero and a
complete validated control artifact before hashing all thirteen raw files. A
failed trial still invokes the writer so a complete failed control record is
available when possible; missing or invalid ancillary evidence is left untouched
for typed invalid composition, never repaired by shell. The runner stops further
recorded trials after the first failed sentinel, preserves that one-based index
and code, and proceeds to the composer/validator so the scenario receives one
atomic typed `Invalid` result. If every manifest-required recorded sentinel is
`ok:0`, the scenario token is `all-ok`; otherwise it is exactly the first
`failed:trial-N:<code>`. Missing or malformed sentinel bytes use that trial's
synthetic transport code `20` only to reach the validator; because no matching
valid sentinel exists, the validator classifies the evidence as
`Invalid(InvalidArtifact)`, never as a synthesized command failure.

After the recorded trials complete or fail,
the shell invokes the typed composer defined in Task 1A.1; it never parses or
constructs candidate or final JSON:

```bash
composer_environment=(
  HOME=/home/mageyuki
  RUSTUP_HOME=/home/mageyuki/.rustup
  CARGO_HOME=/home/mageyuki/.cargo
  PATH=/usr/bin:/bin
  LC_ALL=C
  TZ=UTC
  HERDR_PERF_COMPOSE_RAW_ROOT="$scenario_raw_root"
  HERDR_PERF_COMPOSE_OUTPUT="$scenario_output/candidate-v1.json"
  HERDR_PERF_COMPOSE_STAGE="$stage"
  HERDR_PERF_COMPOSE_SCENARIO="$mapped_scenario"
  HERDR_PERF_COMPOSE_SUBJECT="$subject"
  HERDR_PERF_COMPOSE_PREFLIGHT_HEAD="$preflight_head"
)
if [ "$stage" != baseline ]; then
  composer_environment+=(
    HERDR_PERF_COMPOSE_BASELINE_RESULTS_ROOT="$baseline_results_root"
  )
fi
set +e
"$env_executable" -i "${composer_environment[@]}" \
  "$test_binary" compose_reference_outcome_from_raw --exact --ignored \
    --nocapture --test-threads=1
composer_status=$?
set -e
case "$composer_status" in
  0|10|20) composer_status_token="$composer_status" ;;
  *) composer_status_token="unexpected:$composer_status" ;;
esac

validator_environment=(
  HOME=/home/mageyuki
  RUSTUP_HOME=/home/mageyuki/.rustup
  CARGO_HOME=/home/mageyuki/.cargo
  PATH=/usr/bin:/bin
  LC_ALL=C
  TZ=UTC
  HERDR_PERF_VALIDATE_RAW_ROOT="$scenario_raw_root"
  HERDR_PERF_VALIDATE_CANDIDATE="$scenario_output/candidate-v1.json"
  HERDR_PERF_VALIDATE_OUTPUT="$scenario_output/result-v1.json"
  HERDR_PERF_VALIDATE_STAGE="$stage"
  HERDR_PERF_VALIDATE_SCENARIO="$mapped_scenario"
  HERDR_PERF_VALIDATE_SUBJECT="$subject"
  HERDR_PERF_VALIDATE_PREFLIGHT_HEAD="$preflight_head"
  HERDR_PERF_VALIDATE_COMPOSER_STATUS="$composer_status_token"
  HERDR_PERF_VALIDATE_TRIAL_STATUS="$trial_status_token"
)
if [ "$stage" != baseline ]; then
  validator_environment+=(
    HERDR_PERF_VALIDATE_BASELINE_RESULTS_ROOT="$baseline_results_root"
  )
fi
set +e
"$env_executable" -i "${validator_environment[@]}" \
  "$test_binary" validate_reference_outcome --exact --ignored \
    --nocapture --test-threads=1
validator_status=$?
set -e
case "$validator_status" in 0|10|20) ;; *) exit 20 ;; esac
case "$trial_status_token" in
  all-ok) ;;
  failed:trial-*:*) [ "$validator_status" -eq 20 ] || exit 20 ;;
  *) exit 20 ;;
esac
case "$composer_status_token" in
  unexpected:*) [ "$validator_status" -eq 20 ] || exit 20 ;;
  *) [ "$validator_status" -eq "$composer_status" ] || exit 20 ;;
esac
```

The composer deserializes the fixed intermediate harness JSON, typed runner
control JSON, external process-tree JSON, handshake/control evidence, GNU-time
text, pidstat JSON, measured/observer stdout and stderr, canonical plain-text
trial status, and their SHA-256
digests. It constructs the only candidate in typed Rust. The runner then invokes the separate
`validate_reference_outcome` ignored entrypoint under the exact contract above.
The composer owns only `candidate-v1.json`; the validator owns the only atomic
write or replacement of `result-v1.json`. That validator
deserializes the closed tagged outcome, rehashes every valid-run trial's thirteen
fixed raw files, and re-derives every
distribution, duplicate/loss check, threshold, resource reduction, structural
oracle, and D4 ratio from raw fields. A valid threshold miss must have status
`Failed` plus exactly matching closed failure reasons; that document validates
and is atomically renamed. A `Pass` with any miss, or `Failed` without the exact
miss set, is inconsistent evidence and becomes `Invalid(InvalidArtifact)`.
Known schema/control/composition failures create the independently
validated `InvalidRunV1` without copying malformed fields or private tool text.
Normal composer and validator statuses and the final tag must agree exactly; the
validator atomically replaces a mismatch with `Invalid(InvalidArtifact)` and
returns `20`. An unexpected composer exit or signal token instead finalizes
`Invalid(CommandFailed)` regardless of a partial candidate. A valid failed-trial
transport plus matching sentinel/control evidence does the same; malformed or
mismatched trial evidence finalizes `Invalid(InvalidArtifact)` instead. The final shell
case is a redundant transport guard and never constructs or edits output. Every
validator-completed outcome therefore returns only `0`, `10`, or `20` as defined
above. An unexpected validator exit or signal is not self-finalizable: the
runner returns `20` and reports an incomplete attempt. The Controller records
that state in the research ledger and forbids its root from selection even if an
atomic final rename preceded the failure.
Raw and rejected typed diagnostics remain beside an invalid final envelope for
diagnosis; missing final output
is permitted only for interruption or failure before the validator's
atomic-finalization stage and is never treated as a classified run.

The closed output layout is
`<output-dir>/<mapped-snake-case-scenario>/result-v1.json`, with that scenario's
`trial-*` and `warm-up-*` roots beside it. This same mapped layout is the only
accepted `--baseline-results-root` input; no recursive discovery or filename
guessing is allowed. The research manifest may select an attempt root only when
the top-level runner completed with `0` or valid aggregate `10`, every scenario
validator returned a normal `0`/`10`, and every selected final document validates.
Any validator/scenario-runner unexpected status, aggregate `20`, missing typed
final, or Controller-recorded incomplete-attempt state excludes the entire
attempt root regardless of files left behind.

The composed `RunControlsV1` always records
`"true_cgroup_memory_limit": false` and a human-readable limitation stating that
the address-space cap is neither physical 16-GB RAM nor cgroup isolation.

- [ ] **Step 3: Verify ordinary test behavior and runner fail-closed behavior**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test workload_harness -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --doc
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness native_controller_bootstrap_starts_empty_and_launches_exact_env -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness runner_rejects_worktree_output_under_clean_first_exec -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness source_fixture_uses_frozen_canonical_runner_operand -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness source_fixture_inventory_is_portable_and_role_closed -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness trial_status_is_atomic_and_independent_of_pidstat_exit -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness runner_fixture_preserves_measured_and_observer_exit_status_precedence -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness pidstat_child_status_modes_are_calibrated_and_cross_checked -- --exact --nocapture
```

Then run the runner with an explicit grammar-valid attempt ID and an output
directory deliberately equal to the worktree's direct child. Assert the path is
absent before and after, capture the exact closed diagnostic, and require exit
`20` so the test cannot pass merely because the basename grammar was invalid.
`runner_rejects_worktree_output_under_clean_first_exec` performs those exact
assertions from a trusted Rust test parent through the shared native-Controller
`launch-runner-source-fixture` helper: it freezes the absolute runner path,
supplies the complete portable role manifest and exact source-fixture caller
allowlist including attempt ID `00000001`, and invokes the closed
`output-containment` operation with the canonical repository root, exact
linked-worktree-root vector, and forbidden absolute output path. That operation
calls the same factored helper used by normal `main` and must reject before
authoritative inventory derivation. It requires the exact diagnostic above,
status `20`, absence before and after, and no `result-v1.json` anywhere under the
test temporary root. The distinct normal `launch-runner` authoritative smoke
test remains Linux-only and ignored.

Expected: the exact checks pass before measurement and no result directory is
created. Make the script Git mode `100755`.
Ordinary tests also exercise missing/malformed tool artifacts, a nonzero measured
command, duplicate outcomes, typed-composer invalid output, exact `0`/`10`/`20` exits,
`all` continuation after valid failure and abort after invalidity, and refusal to
create the final filename before typed validation. Also cover candidate mode
without `--baseline-results-root`, a missing scenario result, mismatched baseline
IDs across an `all` root, wrong baseline subject/schema/harness digest, mutated
raw evidence, illegal subject/stage combinations, fabricated final render
evidence in `PostReliability`, missing render evidence in `Final`, and a
candidate that tries to derive its own replacement ID. Each
classified invalid case must leave a validated atomic invalid envelope.
Every test that sources the orchestration fixture in
`scripts/run-reference-profile.sh`, including exit-code and `all` tests, is
`#[cfg(target_os = "linux")]` and is ordinary CI-eligible; it uses only the
non-promotable source-only seam above. Portable schema/composer/validator fixtures
remain enabled in macOS all-feature CI. Linux fixture tests substitute (a) a
measured child that never completes and (b) an observer that never completes,
use a one-second test-only deadline, and assert exit `20`, a validated atomic
`RunnerTestOutcomeV1`, absence of `result-v1.json`, no live process in either
recorded process group, and successful `wait`/reap. Separate signal fixtures cover
`INT`, `TERM`, and `HUP` during handshake and after observer spawn. A distinct
`#[cfg(target_os = "linux")] #[ignore]` smoke test executes normal authoritative
preflight and is reference-host-only; generic CI never asserts that it succeeds.
The ordinary Linux suite also requires
`source_fixture_uses_frozen_canonical_runner_operand`,
`source_fixture_inventory_is_portable_and_role_closed`,
`trial_status_is_atomic_and_independent_of_pidstat_exit`, and
`runner_fixture_preserves_measured_and_observer_exit_status_precedence`, and
`pidstat_child_status_modes_are_calibrated_and_cross_checked` to exercise the
real Controller/source seam and every mutation named above.

- [ ] **Step 4: Controller review and commit runner infrastructure**

The worker stops without staging or committing. The Controller confirms only the
four declared paths changed, runs the commands in Step 3, obtains the required
Opus/Codex task reviews, and commits:

```bash
git add Cargo.toml tests/workload_harness.rs \
  tests/support/reference_profile_controller.rs \
  scripts/run-reference-profile.sh
git commit -m "test(perf): add fail-closed reference runner" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```

Do not record the baseline yet. Task 1B.3 supplies the real queue-admission
adapter and complete reference entrypoint first.


---

### Task 1A.2b: CI-only workload doctest integration

**Files:**

- Modify: `.github/workflows/ci.yml`

**Interfaces:**

- Consumes: Task 1A.2a's ordinary portable/Linux test surface and the existing
  stable/MSRV CI jobs.
- Produces: explicit stable and MSRV doctest gates. It changes no harness,
  runner, or production subsystem and creates no measurement evidence.

- [ ] **Step 1: Prove the CI gates are absent**

Before editing, bind the frozen canonical `rg` and require both exact source
checks below to exit `1`; exit `0` means the red precondition is absent and any
other status is operational failure:

```bash
rg_executable="${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}"
"$rg_executable" --no-config -nF 'cargo +stable test --locked --doc' .github/workflows/ci.yml
"$rg_executable" --no-config -nF 'cargo +1.97.1 test --locked --doc' .github/workflows/ci.yml
```

- [ ] **Step 2: Add only the two doctest commands**

Add `cargo +stable test --locked --doc` to the stable test job and
`cargo +1.97.1 test --locked --doc` to the MSRV job so the later `WriterClient`
compile-fail proof remains a permanent gate; `--all-targets` alone does not run
doctests. Preserve every existing trigger, permission, matrix, and command. The
already-reviewed `verify_subject_diff_is_harness_only` allowlist continues to
include `.github/workflows/ci.yml`; this CI-only commit must not narrow or
duplicate that shared predicate.

- [ ] **Step 3: Verify, review, and commit CI alone**

Require both exact `rg` checks above to exit `0` with exactly one match each, then
run the repository-equivalent commands locally:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --doc
```

After Opus/Codex task reviews and Controller verification, require the actual
changed-file set to be exactly `.github/workflows/ci.yml`, then commit:

```bash
git add .github/workflows/ci.yml
git commit -m "ci: run workload doctests" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```


---

### Task 1B.1: Bind production retention limits to the workload manifest

**Files:**

- Modify: `src/operator.rs`
- Modify: `src/store/mod.rs`
- Modify: `tests/workload_harness.rs`

**Interfaces:**

- Consumes: Task 1A.1's manifest field `operator_activity_limit` after the Task
  1A.2b CI barrier.
- Produces: feature-only hidden aliases of the existing production restore and
  operator truncation limits, with no default-feature behavior change.

- [ ] **Step 1: Add the failing drift test**

Add `workload_retention_aliases_match_manifest`. It references the initially
missing `WORKLOAD_RESTORE_ACTIVITY_LIMIT` and
`WORKLOAD_OPERATOR_ACTIVITY_LIMIT`, converts both with checked integer
conversion, and requires each to equal the manifest value `10_000`. Run the
exact test and require missing aliases as its red state; do not add the aliases
before observing that failure.

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness \
  workload_retention_aliases_match_manifest -- --exact --nocapture
```

- [ ] **Step 2: Add only the two feature-gated aliases**

Inside named `increment5-workload-harness` markers, expose
`WORKLOAD_RESTORE_ACTIVITY_LIMIT: i64 = OPERATOR_ACTIVITY_LIMIT` and
`WORKLOAD_OPERATOR_ACTIVITY_LIMIT: usize = ACTIVITY_LIMIT`, each gated by
`workload-harness` and `#[doc(hidden)]`. Do not duplicate either literal in
production code.

- [ ] **Step 3: Verify, review, and commit**

Run the exact test above, formatting, all-feature clippy, and
all-target/all-feature tests:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness \
  workload_retention_aliases_match_manifest -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

After Opus/Codex task reviews and Controller verification, commit only the three
declared paths as `test(perf): bind workload retention limits`.


---

### Task 1B.2: Production FrameLimiter workload driver

**Files:**

- Modify: `src/tui/app.rs`
- Modify: `tests/workload_harness.rs`

**Interfaces:**

- Consumes: the production `App`, private `FrameLimiter`, and Ratatui
  `TestBackend`.
- Produces: feature-only documented `WorkloadFrameDriver`, one monotonically
  increasing `draw_ordinal` assigned before every production-scheduled draw, and
  focused decision/ordinal-equivalence tests; the default `App::run` path is
  unchanged.

- [ ] **Step 1: Add failing driver-equivalence tests**

Add `workload_frame_driver_matches_production_limiter_decisions`,
`workload_frame_driver_waits_for_first_eligible_response_frame`, and
`workload_frame_driver_draw_ordinals_are_contiguous_only_for_draws`. Feed fixed
dirty/time/key sequences through the requested driver API and require the exact
production limiter draw/poll decisions plus ordinals `0,1,2...` across actual
draws while skipped/not-ready iterations consume none. Run all three exact tests
before implementing the driver; missing driver symbols are the expected API red for this new
feature-only interface.

```bash
rg_executable="${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}"
for test_name in \
  workload_frame_driver_matches_production_limiter_decisions \
  workload_frame_driver_waits_for_first_eligible_response_frame \
  workload_frame_driver_draw_ordinals_are_contiguous_only_for_draws
do
  test "$("$rg_executable" --no-config -cF "fn $test_name()" tests/workload_harness.rs)" = 1
done
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness workload_frame_driver_matches_production_limiter_decisions -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness workload_frame_driver_waits_for_first_eligible_response_frame -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness workload_frame_driver_draw_ordinals_are_contiguous_only_for_draws -- --exact --nocapture
```

Each Cargo command must exit nonzero before the harness starts with a compiler
diagnostic naming the missing `WorkloadFrameDriver`; the exact declaration
inventory above proves that the intended test exists. The global compile-red
exception applies here, so no harness selected-count summary is expected until
Step 3. Cargo success, an unrelated diagnostic, or a declaration count other
than one is rejected.

- [ ] **Step 2: Implement the feature-only driver**

Inside one named marker in `src/tui/app.rs`, implement documented
`#[doc(hidden)] pub WorkloadFrameDriver` over `App`, `Terminal<TestBackend>`, the
existing private limiter, and a supplied monotonic clock. `step` performs the
same refresh/ready/draw/record/poll sequence as production; `handle_key_and_wait`
uses `App::handle_key` and returns only after the first eligible reflecting
frame. A single private counter assigns the ordinal immediately before each
successful ordinary draw and exposes it only in the feature-gated observation;
skipped/not-ready iterations consume no ordinal. It does not duplicate limiter
decisions or change production constants.

- [ ] **Step 3: Verify, review, and commit**

Run these exact feature-gated tests first:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness workload_frame_driver_matches_production_limiter_decisions -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness workload_frame_driver_waits_for_first_eligible_response_frame -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness workload_frame_driver_draw_ordinals_are_contiguous_only_for_draws -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

Each of the first three exact test commands must report exactly one selected and
passed test; zero selected tests is a failure even if Cargo exits zero.
After reviews and Controller verification, commit only the two declared paths as
`test(perf): expose production frame driver`.


---

### Task 1B.3: Real-path workload execution and untouched baseline

**Files:**

- Modify: `src/herdr/controller.rs`
- Modify: `src/herdr/collector.rs`
- Modify: `src/reducer.rs`
- Modify: `tests/workload_harness.rs`

**Interfaces:**

- Consumes: Task 1A.2a's closed runner protocol, Task 1B.1's retention aliases,
  Task 1B.2's `WorkloadFrameDriver`, and the `workload-harness` feature,
  the real bounded Controller queue, `RuntimePersistence`, reducer,
  model/operator watches, provider fallback rescan, the production
  `FrameLimiter`, and Ratatui `TestBackend`.
- Produces: feature-only admission timestamp adapters, the ignored
  `reference_profile_entrypoint`, fast
  nonignored real-queue losslessness/schedule tests, actual-call-site
  reducer/D4/clone-publish timing hooks, and a hidden feature-gated
  library-native `WorkloadTimingKind`/`WorkloadTimingObservation` callback ABI
  owned by `src/reducer.rs`,
  and the valid baseline identified by production subject `9cd9813` plus the
  Task 1B.3 harness commit.

- [ ] **Step 1: Add failing real-path and fallback tests**

Add tests with these exact names:

```rust
#[tokio::test]
async fn real_controller_queue_profiles_are_lossless() {
    for profile in [
        WorkloadProfile::SustainedTarget,
        WorkloadProfile::TargetBurst,
        WorkloadProfile::TwiceTarget,
    ] {
        let result = run_virtual_schedule_through_real_queue(profile).await;
        assert_eq!(result.submitted_sequences, result.admitted_sequences);
        assert_eq!(result.admitted_sequences, result.completed_sequences);
        assert_eq!(result.completed_sequences, result.persisted_sequences);
        assert_eq!(result.rendered_sequences,
            workload::screen_probe_sequences(profile));
        assert!(admission_schedule_attained(
            profile, result.workload_origin_ns, &result.admission_observations));
        assert!(result.screen_observations.iter().all(|sample| {
            sample.rendered_ns.checked_sub(sample.admitted_ns)
                .map(|elapsed| elapsed % 100_000_000)
                == Some(sample.observed_frame_phase_ns)
        }));
        assert_eq!(result.final_identities, workload::oracle(profile).final_identities);
    }
}

#[tokio::test]
async fn stalled_production_frame_recovers_all_cumulative_probe_acknowledgements() {
    let result = run_real_queue_with_frame_stall(
        WorkloadProfile::SustainedTarget,
        Duration::from_millis(450),
    ).await;
    assert!(result.frames.iter().any(|frame| frame.new_probe_count >= 2));
    assert_eq!(result.rendered_sequences,
        workload::screen_probe_sequences(WorkloadProfile::SustainedTarget));
    assert_eq!(result.submitted_sequences, (1..=1_200).collect::<Vec<_>>());
    assert_eq!(result.submitted_sequences, result.persisted_sequences);
}

#[tokio::test]
async fn delayed_ready_and_setup_do_not_shift_the_workload_schedule() {
    let result = run_virtual_schedule_after_delayed_ready_and_setup().await;
    assert!(result.trial_origin_ns < result.observer_ready_ns);
    assert!(result.observer_ready_ns < result.workload_origin_ns);
    assert!(admission_schedule_attained(
        result.profile, result.workload_origin_ns, &result.admission_observations));
}

#[test]
fn virtual_zero_overshoot_phase_rotation_is_complete_and_primed() {
    let trials = valid_five_phase_trials();
    assert_eq!(trials.iter().map(|trial| trial.frame_phase_offset_ns.unwrap())
        .collect::<Vec<_>>(), vec![10_000_000, 30_000_000, 50_000_000,
            70_000_000, 90_000_000]);
    assert!(trials.iter().all(|trial| trial.priming_frame_count == 1));
    assert!(trials.iter().all(|trial|
        trial.priming_frame_recorded_ns.unwrap()
            .checked_add(100_000_000_u64
                .checked_sub(trial.frame_phase_offset_ns.unwrap()).unwrap())
            == trial.workload_origin_ns));
    assert!(trials.iter().all(|trial| trial.screen_observations.iter().all(|sample| {
        let Some(admission) = trial.admission_observations.iter()
            .find(|admission| admission.sequence == sample.sequence) else {
                return false;
            };
        sample.rendered_ns.checked_sub(admission.scheduled_ns)
            .map(|elapsed| elapsed % 100_000_000)
            == trial.frame_phase_offset_ns
    })));
}

#[tokio::test]
async fn reference_observations_record_nonzero_limiter_overshoot_without_synthesis() {
    let priming_ns = 1_000_000_000;
    let desired_phase_ns = 30_000_000;
    let scheduled_ns = priming_ns + 70_000_000;
    let rendered_ns = priming_ns + 107_000_000;
    let result = run_screen_and_input_through_driver_at(
        priming_ns,
        desired_phase_ns,
        scheduled_ns,
        rendered_ns,
    ).await;
    assert_eq!(result.frame_phase_offset_ns, desired_phase_ns);
    assert_eq!(result.screen_observation.admitted_ns, scheduled_ns);
    assert_eq!(result.input_observation.injected_ns, scheduled_ns);
    assert_eq!(result.screen_observation.rendered_ns, rendered_ns);
    assert_eq!(result.input_observation.rendered_ns, rendered_ns);
    assert_eq!(result.screen_observation.observed_frame_phase_ns, 37_000_000);
    assert_eq!(result.input_observation.observed_frame_phase_ns, 37_000_000);
    assert_eq!(result.screen_latency_ns, 37_000_000);
    assert_eq!(result.input_latency_ns, 37_000_000);
    assert!(result.validates());
    assert_ne!(result.screen_observation.rendered_ns, priming_ns + 100_000_000);
}

#[tokio::test]
async fn fallback_rescan_uses_injected_polling_interval_without_loss() {
    let poll = Duration::from_millis(20);
    let scheduler_slack = Duration::from_millis(250);
    let paired = run_notification_and_forced_rescan_pair(poll).await;
    assert_eq!(paired.notification.sequence, paired.rescan.sequence);
    let added_delay = paired.rescan.elapsed
        .checked_sub(paired.notification.elapsed)
        .expect("rescan must not precede notification");
    assert!(added_delay <= poll + scheduler_slack);
    let expected = workload::oracle(WorkloadProfile::FallbackRescan).final_identities;
    assert_eq!(paired.notification.final_identities, expected);
    assert_eq!(paired.rescan.final_identities, expected);
}

fn assert_exact_internal_segment_counts(
    observations: &[ScopedTimingObservationV1],
    expected: &[(ScopedTimingKindV1, u32, u32)],
) {
    for sample in observations {
        let (_, d4_count, clone_publish_count) = expected.iter()
            .find(|(kind, _, _)| *kind == sample.kind)
            .expect("every scoped kind must have a frozen segment count");
        assert_eq!(sample.d4_segment_count, *d4_count);
        assert_eq!(sample.model_clone_publish_segment_count, *clone_publish_count);
    }
}

#[tokio::test]
async fn reducer_scoped_hooks_record_actual_paths_exactly_once() {
    let observations = run_controller_startup_and_both_fallback_arms_with_hooks().await;
    assert_exact_kind_sequence_counts(
        &observations,
        expected_controller_startup_and_fallback_kind_sequences(),
    );
    assert_exact_internal_segment_counts(&observations, &[
        (ScopedTimingKindV1::ControllerEvent, 2, 2),
        (ScopedTimingKindV1::StartupRestore, 1, 1),
        (ScopedTimingKindV1::FallbackNotification, 1, 1),
        (ScopedTimingKindV1::FallbackRescan, 1, 1),
    ]);
    assert!(observations.iter().all(|sample|
        sample.d4_analysis_ns <= sample.reducer_plus_publish_ns
            && sample.model_clone_publish_ns <= sample.reducer_plus_publish_ns));
}
```

The fast queue test uses the frozen offsets with an injected harness clock but
does not sleep; it sends every closed Controller payload through the actual
bounded acceptor-to-reducer sender, services actual terminal outcomes, and drives
the same frame-limiter state machine with virtual monotonic time. The
zero-overshoot phase-rotation test alone requires exact desired-phase equality.
The nonzero-overshoot test advances the same driver seven milliseconds past the
earliest eligible frame, requires the actual draw instant and 37-ms observed
latency/phase, and fails if the driver substitutes the idealized 100-ms draw;
authoritative validation adds no tolerance, clamping, or synthetic correction.
The
fallback test uses a temporary synthetic provider file, one normal notify
factory, one factory that deterministically fails creation, and a feature-only
20-ms injected rescan interval. The explicit 250-ms scheduler allowance makes
this a deterministic semantic/no-loss CI test rather than an absolute timing
gate. It drives model watches and an actual `App` render through
`WorkloadFrameDriver`, and compares each arm's identities rather than only
counts. Only the ignored reference-profile entrypoint
uses the production two-second interval and applies the section 15 two-second
paired-delay threshold.

- [ ] **Step 2: Run focused tests and verify the red state**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness \
  real_controller_queue_profiles_are_lossless -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness \
  fallback_rescan_uses_injected_polling_interval_without_loss -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness \
  reducer_scoped_hooks_record_actual_paths_exactly_once -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness \
  delayed_ready_and_setup_do_not_shift_the_workload_schedule -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness \
  virtual_zero_overshoot_phase_rotation_is_complete_and_primed -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness \
  reference_observations_record_nonzero_limiter_overshoot_without_synthesis -- --exact --nocapture
```

Expected: compilation fails because the feature-only admission adapter and
reference orchestration do not exist. A pre-existing failure is not the red
state.

- [ ] **Step 3: Add feature-only admission observation without changing default production**

Inside the named harness markers, add an optional admission observer to the
Controller request channel only when `workload-harness` is enabled. It records
the synthetic sequence, scheduled time, and harness monotonic timestamp
immediately after bounded capacity reservation succeeds and immediately before
the permit is consumed. Rejected/full/closed
attempts record no admission. The admitted request carries its synthetic
sequence only to the observer; the wire schema, normal sender/receiver types,
capacity, reducer ordering, and default-feature binary are unchanged.

In collector, add a feature-only spawn configuration accepting that observer,
an injected harness clock, synthetic provider root, notify factory, and optional
rescan interval, but delegate to the same production collector loop. The fast CI
test supplies 20 ms; the ignored reference entrypoint supplies the production
two-second constant. No harness capability is exported without the feature, and
no default runtime branch checks for it.

Inside named `increment5-workload-harness` markers in `src/reducer.rs`, define a
documented hidden, feature-gated library-native `WorkloadTimingKind` enum and
`WorkloadTimingObservation` callback payload, then add a feature-only scoped
observer and RAII `(WorkloadTimingKind, sequence)` context. Production library
code must never name the integration-test-only `ScopedTimingKindV1` type.
`tests/workload_harness.rs` converts every library observation exhaustively to
`ScopedTimingObservationV1`; the match names every variant and has no wildcard
arm, so a future library variant is a compile-time protocol change.
Production constructors and apply methods plus feature-only observed wrappers
must delegate to one implementation; the observed wrappers must not invoke D4,
clone the model, or publish a second time. Place timestamps at the actual
`dangling_announcement_components` call sites, around the complete reducer path,
inside the actual private `publish` clone/`send_replace` span, and around startup's
initial `watch::channel(Arc::new(model.clone()))` construction. Startup passes its
observer into the same `new_with_operator` implementation before D4 runs. Both
fallback arms set one scoped context around their real `apply_observation` path.
The Controller path instead starts one context immediately before the production
`validate_controller_event` call in `service_request`, keeps it through scratch
construction and the second D4 analysis, and closes it only after the matching
successful `commit_staged` publication; it must not claim an `apply_observation`
call that Controller does not make. Rejection clears the context without emitting
a successful sample. Clear every context on success/rejection/unwind. The hook
emits exactly one aggregate sample for every expected `(kind, sequence)`, sums all
actual D4 and clone/publication segments inside that scope with checked arithmetic,
records their exact counts, and rejects a duplicate/missing/unexpected segment. It
is absent without
`workload-harness`; no global recorder or duplicate surrogate call is allowed.

Consume, but do not redefine or edit, the two Task 1B.1 aliases:

```rust
// src/store/mod.rs
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
pub const WORKLOAD_RESTORE_ACTIVITY_LIMIT: i64 = OPERATOR_ACTIVITY_LIMIT;

// src/operator.rs
#[cfg(feature = "workload-harness")]
#[doc(hidden)]
pub const WORKLOAD_OPERATOR_ACTIVITY_LIMIT: usize = ACTIVITY_LIMIT;
```

Before any baseline trial, the harness requires both aliases to equal the
manifest's frozen `operator_activity_limit == 10_000` after checked conversion
to `u64`. Startup raw validation continues to consume only that manifest field;
Task 1A.1 never imports or guesses a private production identifier. This seam
detects drift independently at both the store query bound and the operator
truncation bound without changing an ordinary build's API or behavior.

Consume, but do not edit, Task 1B.2's documented
`#[doc(hidden)] pub WorkloadFrameDriver` over `App`, `Terminal<TestBackend>`, the existing private
`FrameLimiter`, and a supplied monotonic clock. Its `step(now)` performs
`refresh_if_changed`, checks `FrameLimiter::ready`, renders only when ready,
records the draw, and returns the exact `poll_duration`; `handle_key_and_wait`
routes the key through `App::handle_key` and repeatedly uses `step` until the
first eligible frame. The default `App::run` code path and constants are not
modified. Task 1B.2 already owns the focused comparison test; this task owns the
composed real-queue and ignored-entrypoint tests that use it.

- [ ] **Step 4: Implement the complete reference entrypoint**

For each scenario, build the exact Task 1A.1 oracle and run through real bounded
paths. Before setup, bind `HERDR_PERF_OBSERVER_CONTROL_SOCKET`, read the explicit
`HERDR_PERF_OBSERVER_HANDSHAKE`, capture the process's own PID,
`/proc/self/stat` start-time ticks, and absolute `CLOCK_MONOTONIC` nanoseconds,
then atomically write the three-field handshake; the same monotonic value becomes
`HarnessTrialV1.trial_origin_ns`. Accept exactly one observer connection and
block until its validated `Ready` frame before any setup. After topology/store
setup, produce exactly one production-scheduled priming frame, save its absolute
`CLOCK_MONOTONIC` timestamp, checked-subtract the manifest-selected desired
phase from the 100-ms frame interval, and checked-add that complement to derive
`HarnessTrialV1.workload_origin_ns`. Wait until that schedule epoch
before entering the reducer schedule. Target input trials use the same epoch as
the first key's `scheduled_ns`. Startup, Idle, and FallbackRescan
carry no workload origin or frame phase. Successful
Controller queue admission starts the screen/reducer latency clock, while every
admission deadline remains anchored to `workload_origin_ns`; delayed
handshake/setup time cannot consume schedule budget.
Terminal reducer outcome and model publication close the queue path.
For Sustained, Burst, and TwiceTarget, add a feature-only callback at that
terminal point so Task 7 can record exactly one `TerminalObservationV1` for every
admitted sequence, not only for the frozen screen probes. Task 1B.3's Baseline
and the later PostReliability result still carry no final-only performance stream;
the callback is inert until Task 7 installs the Final recorder.
Concurrently, the frame driver records the first production-scheduled
fixed-surface frame for each frozen sentinel probe; it never blocks the consumer
and never requires a frame for a non-probe sequence. Preserve semantic no-ops and typed
rejections as completed admissions, but require persistence/render only when the
scenario applicability table says the event mutates model/durable state.

For fallback, time from synthetic file append through real provider rescan,
reducer, watches, and frame, retaining the paired notify sample and both arms'
actual reducer-hook D4-scoped observations. For startup, prepare exactly 100,000
unique non-gap retained activity rows in `events` plus the matching 100,000
`event_ledger` deduplication rows for one session, assert both database counts,
close the writer, and then spawn a fresh helper process. The helper validates
that both feature-only production-limit aliases equal the manifest's frozen
`10_000`, then proves restore exposes exactly that many ordered activities with
the frozen operator semantics before accepting the measured
constructor D4 scope; ledger-only or collector-gap setup is invalid. For idle,
settle five seconds while the external sibling observer
samples the complete measured process tree, send `StartIdleWindow {}` without a
timestamp, remain idle
until the observer returns both acknowledged boundaries, and copy those exact
values into the raw harness document. The
entrypoint writes only `HarnessTrialV1` to its unique raw path. It never
constructs a final `ReferenceOutcomeV1` and never reads unrelated configuration or
real provider/session data. Immediately before serialization it obtains its own
effective CPU affinity and address-space limit, captures the exact allowlisted
environment and received scratch-root string, and writes those values only to
`ChildControlsV1`; it cannot see or manufacture run-global executable, Cargo, or
storage evidence.

- [ ] **Step 5: Run deterministic, feature-isolation, and full verification**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --doc
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo build --release --locked --no-default-features --bin herdr-top
```

Expected: fast target/burst/overload streams through real queues are exactly
lossless; the fast fallback semantic test obeys its injected interval plus
explicit slack, while only the ignored reference profile enforces the production
two-second added-delay bound; artifact failure tests fail closed; the default
production build contains no harness symbols.

- [ ] **Step 6: Controller review, commit, and untouched baseline**

The Controller confirms only the four declared paths changed, obtains required
actual-Opus/fresh-Codex task reviews, independently reruns Step 5, and commits:

```bash
git add src/herdr/controller.rs src/herdr/collector.rs src/reducer.rs \
  tests/workload_harness.rs
git commit -m "test(perf): drive reference workloads through runtime queues" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```

Pause all elective workloads and run all scenarios from that clean commit:

```bash
attempt_id="${HERDR_INCREMENT5_ATTEMPT_ID:?record a fresh eight-digit attempt ID in the research ledger first}"
export HERDR_INCREMENT5_ATTEMPT_ID="$attempt_id"
baseline_results_root="${RESEARCH_HOME:-$HOME/.research}/mageyuki--herdr-top/increment-5-reliability-performance/measurements/baseline-9cd98131038a-attempt-$attempt_id"
runner_script="${HERDR_INCREMENT5_FROZEN_RUNNER_SCRIPT:?set to the revalidated frozen absolute runner script}"
controller_requested="${HERDR_INCREMENT5_CONTROLLER_LAUNCHER_REQUESTED:?set to the frozen native Controller requested path}"
controller_canonical="${HERDR_INCREMENT5_CONTROLLER_LAUNCHER_CANONICAL:?set to its revalidated canonical path}"
controller_sha256="${HERDR_INCREMENT5_CONTROLLER_LAUNCHER_SHA256:?set to its revalidated digest}"
runner_argv=( -p "$runner_script" \
  --subject 9cd98131038a53b6dd36ff53e9b89825acba70ae \
  --stage baseline \
  --scenario all \
  --output-dir "$baseline_results_root" )
```

The trusted parent supplies frozen canonical Bash plus `runner_argv` to the
Task 1A.2a native Controller through the exact `builtin exec -c` bootstrap; the
native Controller then applies `env_clear()` and the exact runner allowlist.
The array is never eval'd or executed by a newly loaded inherited-environment
shell. The Controller independently invokes the
typed validator for every result and
records harness commit, schema/baseline IDs, raw/result SHA-256 digests, exact
host/control identity, and commands in the research manifest/change log. A valid
performance miss does not block reliability hardening; an invalid baseline
blocks Tasks 2A-7 until corrected. Every document must record
`measurement_stage: baseline` and omit the final-only performance evidence stream.


---

### Task 2A: Prepare non-cloneable writer call sites

**Files:**

- Modify: `src/reducer.rs`
- Modify: `src/operator.rs`
- Modify: `tests/controller.rs`
- Modify: `tests/convergence.rs`
- Modify: `tests/workload_harness.rs`

**Interfaces:**

- Consumes: Task 1B.3's valid baseline and current cloneable `WriterClient`.
- Produces: behavior-preserving call sites that move the operational writer into
  `RuntimePersistence`, observe health through `subscribe_persistence`, query
  cleanup semantically through Controller responses, and already bind writer
  locals/references mutably for Task 2B.

- [ ] **Step 1: Add/strengthen characterization assertions before refactoring**

Replace all four retained-clone capabilities before moving the unique writer:

- subscribe to persistence health and diagnostics before collector spawn, then
  assert the same typed degradation through those read-only receivers;
- seed provider-ledger rows through the store/writer before collector spawn, or
  stop and reopen the store after a controller-first rejection, rather than
  calling `apply` through a retained post-spawn client;
- induce out-of-band persistence failure with a preinstalled SQLite trigger and
  a real Controller-admitted batch, rather than a retained `apply` call;
- replace post-response `barrier` calls with bounded test-only polling of the
  expected durable rows plus the persistence-health watch; and
- seed the reused event ID with `seen_at_ms = 0`, prove its first wire request is
  `Duplicate`, admit a distinct real Controller event whose post-apply cleanup
  prunes that expired row, then prove the reused ID becomes `Accepted`. This
  exercises production cleanup without an eight-day sleep or retained client.

Apply the same replacements in `tests/workload_harness.rs`. In reducer/operator
tests, pass `&mut WriterClient` and keep all existing direct-writer durability,
dedup, and late-receipt assertions. No new product-only command surface is added.
Add the exact characterization test
`workload_harness_writer_access_uses_owned_client`; it seeds through the writer
before collector spawn, moves the sole client into the collector, and observes
later persistence state only through the previously subscribed read-only health
receiver and durable rows.

- [ ] **Step 2: Run characterization tests before changing ownership use**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test controller idle_cleanup -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test convergence cleanup -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness \
  workload_harness_writer_access_uses_owned_client -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked reducer::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked operator::tests -- --nocapture
```

Expected: current behavior passes, establishing the observable replacement for
each retained writer clone.

- [ ] **Step 3: Remove call-site clone dependence and make ownership affine-ready**

Move `WriterClient` into every collector spawn instead of cloning it. Store only
read-only watch receivers in test harnesses. Declare writer bindings mutable and
change helper parameters to `&mut WriterClient`; current `&self` APIs accept those
calls without behavior change. In the two direct `src/operator.rs` writer tests
currently containing `writer.apply(...)` and
`writer.reserve_enqueue(...)/writer.barrier(...)`, bind the spawned client as
`mut writer`, then immediately rebind `let writer = &mut writer;` before those
calls. This makes the Task 2A mutable use explicit under `-D warnings` and remains
source-compatible when Task 2B.2a changes the relevant receivers to `&mut self`.
Do not change `WriterClient` itself in this task.

- [ ] **Step 4: Verify and commit the mechanical preparation**

Run:

```bash
rg_executable="${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}"
set +e
"$rg_executable" --no-config -n '\bwriter\.clone\(\)' src/reducer.rs src/operator.rs \
  tests/controller.rs tests/convergence.rs tests/workload_harness.rs
external_status=$?
set -e
case "$external_status" in
  0) echo 'unexpected external WriterClient clone remains' >&2; exit 1 ;;
  1) ;;
  *) echo "rg operational failure: $external_status" >&2; exit "$external_status" ;;
esac
set +e
clone_inventory="$("$rg_executable" --no-config -n '\bwriter\.clone\(\)' src tests)"
inventory_status=$?
set -e
[ "$inventory_status" -eq 0 ] || {
  echo "rg clone inventory failure: $inventory_status" >&2
  exit "$inventory_status"
}
clone_count=0
while IFS= read -r _; do clone_count=$((clone_count + 1)); done <<<"$clone_inventory"
[ "$clone_count" -eq 1 ]
printf '%s\n' "$clone_inventory"
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

Expected: the guarded five-path search finds no external clone, so its failure
branch is skipped. The exhaustive search exits zero and reports exactly the single
`src/store/writer.rs` test-local clone inside the existing owner-panic fixture;
Task 2B.2a owns and removes that last site. Any other match fails verification. All
tests pass. After reviews and
independent verification, commit only the five paths:

```bash
git add src/reducer.rs src/operator.rs tests/controller.rs tests/convergence.rs \
  tests/workload_harness.rs
git commit -m "refactor(store): prepare affine writer ownership" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```


---

### Task 2B.1: Mutable writer facade and collector borrow-order bridge

**Files:**

- Modify: `src/store/writer.rs`
- Modify: `src/herdr/collector.rs`

**Interfaces:**

- Consumes: Task 2A's affine-ready external call sites and the current cloneable
  writer implementation.
- Produces: the final mutable `finish_pending`/`replace_owner` receiver shapes
  plus a stored read-only writer-health receiver and collector field-split
  ordering needed by Tasks 2B.2a/2B.2b, without changing the D1/D2 implementation
  or facade observation timing yet.

- [ ] **Step 1: Characterize the current pending result path green**

Add the exact test
`writer_client_finish_pending_preserves_pending_wait_behavior`. First run it
through the existing `PendingEnqueue::wait` path and require it to pass. Also run
the existing replace-owner failure test. Add the exact green-before/green-after
characterization
`runtime_persistence_reserve_enqueue_preserves_late_health_observation`. A
test-only WriterClient hook pauses immediately after its second healthy check and
before it returns the reserved permit, then publishes the exact
`Apply/QueueAdmission/ChannelClosed/NotCommitted` failure. The test requires the
facade reservation to return `None`, its diagnostics watch to contain that exact
failure before return, Controller input to be persistence-unavailable, and the
first-failure occurrence to be attempted exactly once. Additional rows require
writer-returns-`None` with degraded health to publish the same exact facade
failure, a healthy permit to remain usable, and healthy capacity exhaustion to
return `None` without false degradation. Run every row against the current code
before refactoring and again afterward. This is characterization-first discipline
for an intentionally non-behavior-changing API move; do not pretend a missing
symbol is a behavioral red.

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked \
  store::writer::tests::writer_client_finish_pending_preserves_pending_wait_behavior -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked \
  store::writer::tests::i4_writer_replace_owner_failure_names_replace_operation -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked \
  herdr::collector::tests::runtime_persistence_reserve_enqueue_preserves_late_health_observation -- --exact --nocapture
```

- [ ] **Step 2: Land the compile-green mutable bridge**

Add `WriterClient::finish_pending(&mut self, PendingEnqueue)` directly in its
final receiver form, initially delegating to `pending.wait().await`, and reroute
the same green characterization plus `RuntimePersistence::finish_pending`
through it. Change `WriterClient::replace_owner` to `&mut self`; make the affected
writer test binding and `spawn_configured`'s writer parameter mutable. The
startup call now genuinely consumes the mutable binding before the writer moves
into `RuntimePersistence`, so `unused_mut` cannot be hidden by an allowance.

Before moving `writer` into `RuntimePersistence`, subscribe once and store
`writer_health: watch::Receiver<PersistenceStatus>`. Make
`observe_writer_health` sample that stored receiver, copy the latest sticky
status in a short guard scope, release the guard, and pass the copy to a private
status-ingestion helper. The helper factors the existing facade
`record_failure`/`publish` body and accepts only the facade fields plus copied
status; it must not accept or reborrow `writer` or `&mut RuntimePersistence`.

In `RuntimePersistence::reserve_enqueue`, preserve the existing facade health
observation before reservation. Then explicitly destructure `self` into disjoint
borrows of `writer`, `writer_health`, and the facade fields before calling
`writer.reserve_enqueue()`. Copy the receiver's latest sticky status after that
call and release the receiver guard. If it is degraded, publish that exact
failure through the facade-only helper **while any returned permit is still
held**, then drop the permit and return `None`; this preserves the current
publish-before-capacity-release order. If it is healthy, return the reservation
outcome unchanged, including `None` for healthy capacity exhaustion. Do not use a
pre-reservation-only probe, a status embedded only in WriterClient's return, or a
whole-`self` method while the later borrowing permit is live. For every async
writer call, bind the awaited result to a local before passing it to
`classify_result`.

On that one `RuntimePersistence::reserve_enqueue` method only, add
`#[allow(mismatched_lifetime_syntaxes, reason = "removed by Task 2B.2b after EnqueuePermit becomes borrowing")]`.
At Task 2B.1 this known Rust 1.97.1 lint is not yet triggered because
`EnqueuePermit` has no lifetime; the narrow bridge exists solely so Task 2B.2a
can change store plus reducer and still pass all-feature `-D warnings` without
also editing the third production subsystem. Do not add a module/crate allowance,
and do not allow any other lint. Task 2B.2b must replace the bare return spelling
with `Option<crate::store::EnqueuePermit<'_>>` and remove this attribute before
the post-reliability barrier.

- [ ] **Step 3: Verify, review, and commit the bridge**

Run the complete self-contained gate:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked \
  store::writer::tests::writer_client_finish_pending_preserves_pending_wait_behavior -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked \
  store::writer::tests::i4_writer_replace_owner_failure_names_replace_operation -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked \
  herdr::collector::tests::runtime_persistence_reserve_enqueue_preserves_late_health_observation -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked herdr::collector::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

The three focused commands each select and pass exactly one test. After reviews
and Controller verification, commit only the two declared paths as
`refactor(store): prepare mutable writer facade`.


---

### Task 2B.2a: D1 single ledger authority and D2 panic-safe acknowledgement core

**Files:**

- Modify: `src/store/writer.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/reducer.rs`

**Interfaces:**

- Consumes: Task 2B.1's compile-green mutable facade/borrow-order/lint bridge and
  Task 2A's affine-ready call sites.
- Produces: non-`Clone` `WriterClient`; affine `EnqueuePermit<'_>` borrowing a
  Tokio `mpsc::Permit` and the collector-owned ledger until queue send; `PendingEnqueue` containing a read-only
  typed common acknowledgement waiter, and one private affine unread-receipt
  failure publisher; every direct and queued acknowledgement subscribes to
  health before send; writer acknowledgements carrying
  `WriterDelta { cleanup: CleanupStats }`; `WriterClient::finish_pending(&mut
  self, PendingEnqueue)`; and operation-scoped panic health publication.
- `WriterLifecycle` remains the unique shutdown/join owner and exposes no ordinary
  command method.
- This dispatch owns only the store and reducer production subsystems. It leaves
  the one pre-landed collector lint bridge byte-for-byte unchanged for Task
  2B.2b, so every integration remains compile/clippy green.

- [ ] **Step 1: Replace the healthy-after-panic expectation with failing D1/D2 tests**

In `src/store/writer.rs`, replace
`i4_writer_panicking_owner_of_armed_apply_receipt_keeps_health_healthy` with
these deterministic Tokio test bodies:

- `unread_pending_receipt_dropped_during_owner_unwind_degrades_once`
- `writer_panic_after_issued_permit_degrades_and_unblocks_waiter`
- `writer_thread_never_mutates_the_collector_ledger_mirror`
- `acknowledgement_waiter_covers_all_six_operations`
- `sender_drop_publishes_acknowledgement_failure_for_all_six_operations`
- `queued_sibling_waiter_observes_writer_panic_without_acknowledgement`
- `queued_sibling_health_and_closed_interleavings_return_first_published_failure`
- `precise_acknowledgement_wins_when_response_and_health_are_ready`
- `normal_failure_published_before_ack_returns_precise_durability`

Task 2B.1 already supplies the mutable `finish_pending` symbol and collector
borrow order, so no test in this task may fail merely because that seam is
missing.

`sender_drop_publishes_acknowledgement_failure_for_all_six_operations`,
`precise_acknowledgement_wins_when_response_and_health_are_ready`, and
`normal_failure_published_before_ack_returns_precise_durability` are the three
green characterization rows in this step: add and run them against the current
closure branches before adding the red D1/D2 assertions, and require all three
to pass. They freeze the already-correct closed-response publication,
response-first selection, and normal precise-failure publication behavior that
the common waiter refactor must preserve. The other six tests are the intended
red suite.

First add a `#[cfg(test)] AcknowledgementTestControl` to the existing spawn
fixture and keep the old suite green. For each operation it can (a) block the
writer after command receipt but before acknowledgement, (b) expose a test-only
clone of `PersistenceHealth` that publishes one supplied failure, and (c) record
`WaiterConstructed(operation)`, `CommandAdmitted(operation)`, and
`WaiterResolved(operation, failure)` callback variants. The current-code fixture
emits only the first two; Step 4 adds the resolution hook. It has no non-test
field or branch. Then implement
`acknowledgement_waiter_covers_all_six_operations` as six explicit cases—Apply,
Cleanup, UpdateOwnerLocation, ReplaceOwner, Barrier, and lifecycle-shutdown
Checkpoint—each with a fresh store/writer. Move the unique client or lifecycle
into the request future, wait for command receipt, and publish degraded health
while the acknowledgement sender remains live and blocked. For Apply, Cleanup,
UpdateOwnerLocation, ReplaceOwner, and Barrier, require the request future to
return the exact supplied first-published typed failure under a one-second test timeout
before releasing the writer. `WriterLifecycle::shutdown` must still join the OS
thread, so the Checkpoint fixture instead records a test-only
`WaiterResolved(Checkpoint, exact_failure)` callback immediately after the
common waiter resolves; require that callback before release, then release the
writer and separately require the full shutdown future to return the same typed
failure with a successful join. Run the potentially blocking join on a
multi-thread Tokio test runtime so the timeout remains pollable. Assert the first
two recorded callbacks are exactly
`[WaiterConstructed(operation), CommandAdmitted(operation)]`; do not use an
unimplemented `assert_waiter_is_installed_before_send_for` stand-in.

The owner-unwind test moves one armed `PendingEnqueue` into a Tokio task that
panics, awaits the join error, and asserts the health watch changes exactly once
to `Apply/Acknowledgement/AcknowledgementDropped/Unknown`; a second observation
must report no change. The D1 ledger test records the direct cache before writer
execution, after the returned delta but before `finish_pending`, and after
`finish_pending`, proving only the final state applies exact cleanup. The queued
sibling, normal-publication-before-ack, and simultaneous-ready tests assert their
named outcomes rather than only completing. Every test other than the three
explicitly green sender-drop, response-first, and precise-publication
characterization has an observable behavioral red state.

Keep `acknowledgement_waiter_covers_all_six_operations` as the health-race table
described above. Separately implement
`sender_drop_publishes_acknowledgement_failure_for_all_six_operations` as six
fresh-store rows that each start with `PersistenceStatus::Healthy`, admit exactly
one Apply, Cleanup, UpdateOwnerLocation, ReplaceOwner, Barrier, or Checkpoint
command, then deliberately drop that command's response sender without any prior
health publication. Each row must assert the operation-specific returned
acknowledgement error and the identical newly published `Degraded` watch value.
In this Step-1 green characterization, the Checkpoint row does **not** require a
`WaiterResolved` callback because the common waiter does not exist yet; it only
asserts the typed error, identical Degraded value, and successful release/join.
Step 4 adds the `WaiterResolved` assertion after installing the common waiter.
This table is the closure-branch proof; the original live-sender table remains
the simultaneous-health proof.

`normal_failure_published_before_ack_returns_precise_durability` uses a separate
test-only pause after `publish_result_failure` and before
`acknowledgement.send(result)`. Inject one ordinary SQLite Apply failure with a
known `NotCommitted` durability disposition, wait until the precise failure is
visible in health while the acknowledgement remains live and unsent, and require
the waiter to return that exact published tuple. Release the send afterward and
prove it cannot replace the first failure. This is a normal-result race, not a
panic or sender-drop surrogate.

Do not add or invoke `static_assertions`; the repository has no such dependency.
Make the compile-time ownership test a normal passing path-resolution example
plus a `compile_fail` example on `WriterClient`:

```rust
/// ```
/// # fn consumes(_: herdr_top::store::WriterClient) {}
/// ```
/// ```compile_fail
/// # fn duplicate(client: herdr_top::store::WriterClient) {
/// let second = client.clone();
/// # drop(second);
/// # }
/// ```
pub struct WriterClient {
    // fields are private
}
```

This is the plan's one explicit two-doctest target. The filtered rustdoc command
must report exactly two selected doctests. Before clone removal its red summary
is exactly one ordinary pass and one failed `compile_fail` block because cloning
still compiles; after clone removal its green summary is exactly two selected
and two passed. Zero, one, or more than two selected doctests fails closed.

The panic test wraps `writer.finish_pending(pending)` in a one-second **test-only**
`tokio::time::timeout`, asserts the returned typed failure is
`Apply/Acknowledgement/AcknowledgementDropped/Unknown`, asserts the health watch
contains that same value, asserts later reservation is denied, and asserts
shutdown/join returns `WriterError::ThreadPanicked` rather than hanging.

Construct the D2 interleaving in this exact order: subscribe to health; acquire
one `EnqueuePermit`, which now affinely borrows the ledger; use a private
`#[cfg(test)]` raw command injector returned by the spawn helper to send the
separate Apply whose injected writer clock panics, retaining its oneshot receiver
without polling or dropping it; require `health.changed()` before touching the
receiver, proving the writer unwind guard published the failure; then consume
await the raw injector sender's Tokio `Sender::closed()` under the same test-only
timeout and assert `is_closed()` before consuming the already-issued permit.
This proves `writer_main` has dropped its mpsc receiver rather than merely begun
unwinding. Then await that pending receipt and finally classify the retained
panic response. The raw injector is test-only
command authority and never exists in production, so it cannot violate the
production uniqueness invariant. This reproduces the debt ledger's
held-permit-after-receiver-drop route while respecting the new affine API.

- [ ] **Step 2: Run the focused tests and verify the intended failures**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests::unread_pending_receipt_dropped_during_owner_unwind_degrades_once -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests::writer_panic_after_issued_permit_degrades_and_unblocks_waiter -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests::writer_thread_never_mutates_the_collector_ledger_mirror -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests::acknowledgement_waiter_covers_all_six_operations -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests::sender_drop_publishes_acknowledgement_failure_for_all_six_operations -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests::queued_sibling_waiter_observes_writer_panic_without_acknowledgement -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests::queued_sibling_health_and_closed_interleavings_return_first_published_failure -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests::precise_acknowledgement_wins_when_response_and_health_are_ready -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests::normal_failure_published_before_ack_returns_precise_durability -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --doc store::writer::WriterClient -- --nocapture
```

Expected: the owner-unwind test observes the current suppressed healthy state;
the six-operation race times out while a live acknowledgement is blocked because
the current direct waiter ignores health; the ledger test lacks collector-side
ownership; the panic waiter fails its typed-health ordering; the sender-drop,
normal-publication-before-ack, and simultaneous response-first tests pass as green
characterizations; and the filtered rustdoc reports exactly one passed and one
failed doctest because `WriterClient` still implements `Clone`. Test-support compilation
or a missing-symbol error is not an accepted red state.

- [ ] **Step 3: Make ledger ownership collector-side and affine**

Remove `#[derive(Clone)]` from `WriterClient`. Store `EventLedgerCache` directly
in it, not in the writer thread. `EnqueuePermit<'a>` contains a borrowed Tokio
`mpsc::Permit<'a, WriterCommand>` plus an affine `&'a mut EventLedgerCache` borrowed from that
unique client; it never uses `Arc`, `Mutex`, a raw pointer, or a second owner.
Its only public operation consumes itself to enqueue, so ledger mutation cannot
escape the collector's serialized authority.

Use these concrete acknowledgement shapes:

```rust
#[derive(Debug)]
struct WriterDelta {
    cleanup: CleanupStats,
}

pub struct PendingEnqueue {
    waiter: AcknowledgementWaiter<WriterDelta>,
}

struct AcknowledgementWaiter<T> {
    response: oneshot::Receiver<Result<T, PersistenceFailure>>,
    health_publisher: PersistenceHealth,
    health: watch::Receiver<PersistenceStatus>,
    operation: PersistenceOperation,
    acknowledgement_observation: Option<AcknowledgementObservationGuard>,
}

impl WriterClient {
    pub fn reserve_enqueue(&mut self) -> Option<EnqueuePermit<'_>>;
    pub async fn finish_pending(
        &mut self,
        pending: PendingEnqueue,
    ) -> Result<CleanupStats, WriterError>;
}
```

`reserve_enqueue` checks health before and after acquiring the borrowed channel
permit. `EnqueuePermit::enqueue` derives exact inserted entries, reserves them in
its borrowed direct cache, creates the health subscription/waiter, and only then
calls `Permit::send`. This avoids cloning even the private operational sender and
preserves within-process dedup even if the
pending receipt is immediately dropped. `finish_pending` waits and applies only
exact `(event_id, seen_at_ms)` cleanup entries from the returned delta. Known
not-committed and unknown outcomes retain the process-lifetime reservation.

The ordinary direct `WriterClient::apply(&mut self, batch)` path derives and
reserves the batch's ledger entries in the same direct cache **before** its
fallible channel send; it retains those process-lifetime reservations on closed,
not-committed, and unknown outcomes. `cleanup(&mut self, now_ms)` applies only the
exact returned cleanup delta. This replaces the writer-thread reservation and
preserves the direct-writer/controller namespace tests after `writer_ledger` is
removed.

Change `Reducer::commit_staged` to accept `EnqueuePermit<'_>` explicitly; its
existing ordering still swaps/publishes the staged state and then consumes the
permit, whose `enqueue` now reserves before send. No second enqueue API may
bypass that ordering.

Change every ordinary writer request that may mutate the ledger to take
`&mut self`. Remove `writer_ledger` from `spawn_writer_inner` and from
`writer_main`; the writer owns only SQLite and returns exact deltas.

Existing writer tests that lock the shared ledger to stall the Apply arm must not
keep that accidental synchronization. Replace their mutex gate with one explicit
`#[cfg(test)]` Apply-before-store rendezvous injected by the spawn helper. The
test waits for “Apply entered,” releases it deterministically, and keeps the same
late-receipt assertions. Remove the superseded
`PANIC_RECEIPT_CLOCK_RENDEZVOUS`, `panic_receipt_gated_clock`, and any old ledger
gate so `-D warnings` leaves no orphaned test support.

- [ ] **Step 4: Publish typed failure from the writer unwind boundary**

Replace the panic-suppressing condition with an operation guard owned by
`writer_main` while each command executes:

```rust
struct WriterOperationGuard {
    health: PersistenceHealth,
    operation: PersistenceOperation,
    armed: bool,
}

impl Drop for WriterOperationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.health.publish_failure(acknowledgement_failure(self.operation));
        }
    }
}
```

Arm immediately before the store call and disarm only after the typed result has
been sent or a normal send attempt has completed. On panic the guard publishes
unknown acknowledgement failure during unwind and the OS thread terminates.
Do not use `catch_unwind` to continue the writer.

The one private `AcknowledgementWaiter<T>::wait` implementation used by
`WriterClient::finish_pending` and every direct request polls the acknowledgement
before every health check, then
uses a response-first biased select. This makes an already-ready precise typed
acknowledgement win even when health is also already degraded:

```rust
loop {
    match response.try_recv() {
        Ok(result) => {
            if let Some(guard) = acknowledgement_observation.as_mut() {
                guard.disarm();
            }
            return classify_typed_result(result);
        }
        Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
            if let Some(guard) = acknowledgement_observation.as_mut() {
                guard.disarm();
            }
            return classify_closed_response_or_publish_first_failure(
                operation,
                &health_publisher,
                &health,
            );
        }
        Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
    }
    if let PersistenceStatus::Degraded { failure } = *health.borrow() {
        if let Some(guard) = acknowledgement_observation.as_mut() {
            guard.disarm();
        }
        // Global health is the approved first-published typed failure payload.
        return Err(WriterError::Persistence(failure));
    }
    tokio::select! {
        biased;
        response = &mut response => {
            if let Some(guard) = acknowledgement_observation.as_mut() {
                guard.disarm();
            }
            return classify_response_or_publish_closed(
                operation,
                response,
                &health_publisher,
                &health,
            );
        }
        changed = health.changed() => {
            if changed.is_err() {
                if let Some(guard) = acknowledgement_observation.as_mut() {
                    guard.disarm();
                }
                return classify_closed_response_or_publish_first_failure(
                    operation,
                    &health_publisher,
                    &health,
                );
            }
            // Loop: poll the response again before classifying changed health.
        }
    }
}
```

`PersistenceHealth` is cloneable; every waiter owns one publisher clone as well
as its receiver. Degraded health is the sticky global wake-up **and** the exact
approved first-published typed failure payload. `Empty + Degraded(f)` and
`Closed + Degraded(f)` both return `f`, including to a sibling waiter. For
`Closed + Healthy`, `classify_closed_response_or_publish_first_failure`
constructs the operation-specific `acknowledgement_failure(operation)`, calls
the first-wins publisher to preserve the current `runtime_error` side effect,
rereads sticky health, and returns the actual first published failure; a
concurrent publisher therefore cannot create a returned/watch mismatch. The
helper applies to all six operations, including Barrier and Checkpoint, which
have no unread-receipt guard. Thus a panicking Apply leaves global health at
`Apply/Acknowledgement/AcknowledgementDropped/Unknown`, and a queued Barrier
awakened by it returns that same published Apply failure. Operation-scoped
unknown failure remains valid only for response closure with no prior typed
transition. `classify_response_or_publish_closed` delegates its closed-result arm
to this same helper, and every returned branch disarms its unread-receipt guard
before classification.
The separate sender-drop table starts Healthy and asserts both the exact returned
error and the resulting Degraded watch value for every closed-response case.
At this point, extend its Checkpoint row to require the test-only
`WaiterResolved(Checkpoint, exact_failure)` callback before release/join; this is
the hook assertion deliberately deferred from the Step-1 green characterization.

Add a deterministic test that makes both acknowledgement and degraded health
ready before the waiter is first polled and asserts that the precise
acknowledgement wins. Add two controlled sibling interleavings: publish Apply
health while the queued Barrier response remains `Empty`, and close that
response before its first poll. Both return the exact first-published Apply tuple;
the closed response may publish a local Barrier candidate only while health is
still Healthy, and the required reread still returns whichever tuple won.
Extend the same table to all queued sibling operation kinds. Also retain the
separate normal-failure pause between precise publication and acknowledgement
send, so sibling-panic cases cannot stand in for the ordinary same-command race.

Create every waiter and its health subscription before the corresponding command
send, arm its unread-receipt guard only after successful admission, and route all
six `PersistenceOperation` paths through the same response-first implementation:
permit/direct Apply, Cleanup, UpdateOwnerLocation, ReplaceOwner, Barrier, and the
`WriterLifecycle::shutdown` Checkpoint. Queue-send failure leaves the waiter
unarmed and returns the operation-specific queue failure. Add both table-driven
six-operation health-race and sender-drop coverage tests, a direct-operation precise acknowledgement case, the
normal publish-before-send race, a queued sibling stranded behind a panicking command, both sibling interleavings, simultaneous ready
acknowledgement/health, and shutdown/join coverage. No direct `response.await`
may remain outside the common waiter.

`AcknowledgementWaiter::new` constructs `Some(AcknowledgementObservationGuard)`
only for Apply, Cleanup, UpdateOwnerLocation, and ReplaceOwner; Barrier and
Checkpoint carry `None`, preserving their `NotApplicable` durability and the
guard's mutating-only assertion. Every waiter, independently, receives the
publisher clone needed by the common closed-response path. Every response branch disarms with
`if let Some(guard)`, while health observation remains common to all six. The
unread-receipt drop behavior below therefore applies only to the four mutating
operations; writer-operation panic publication still wakes Barrier/Checkpoint.

`WriterLifecycle::shutdown` always joins the OS thread. If the join reports a
panic, return `WriterError::ThreadPanicked` before an earlier queue/acknowledgement
error; otherwise return the operation result. This makes the D2 test's shutdown
assertion precise while preserving queue errors for non-panicking shutdowns.

There is no production timeout. Preserve the unread-pending-receipt behavior:
dropping an armed receipt still publishes exactly one unknown acknowledgement
failure, including when the receipt owner itself is unwinding. Remove the current
`!std::thread::panicking()` suppression; the first-failure health channel remains
idempotent, so the writer-operation guard and unread-receipt guard cannot replace
one another's typed failure.

- [ ] **Step 5: Preserve the pre-landed collector bridge and prove core ownership**

Task 2B.1 already keeps `WriterClient` only inside `RuntimePersistence`, routes
pending completion through the mutable writer, preserves the late
post-reservation facade observation through a stored receiver and disjoint-field
helper, and separates awaited results from facade classification. Preserve
that file byte-for-byte, including its one narrow, reasoned lifetime-lint bridge.
Task 2B.2a may not edit `src/herdr/collector.rs`; Task 2B.2b owns the explicit
lifetime spelling and removes the bridge immediately after this core commit.

Search and prove uniqueness:

```bash
rg_executable="${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}"
awk_executable="${HERDR_INCREMENT5_FROZEN_AWK_EXECUTABLE:?set to the revalidated canonical /usr/bin/awk identity}"
assert_rg_no_match() {
  set +e
  "$rg_executable" --no-config -n "$@"
  rg_status=$?
  set -e
  case "$rg_status" in
    0) echo 'forbidden match remains' >&2; exit 1 ;;
    1) ;;
    *) echo "rg operational failure: $rg_status" >&2; exit "$rg_status" ;;
  esac
}
rg_exact_line_count() {
  expected_count=$1
  shift
  set +e
  rg_matches="$("$rg_executable" --no-config -n "$@")"
  rg_status=$?
  set -e
  [ "$rg_status" -eq 0 ] || {
    echo "rg positive inventory failure: $rg_status" >&2
    exit "$rg_status"
  }
  actual_count=0
  while IFS= read -r _; do actual_count=$((actual_count + 1)); done <<<"$rg_matches"
  [ "$actual_count" -eq "$expected_count" ] || exit 1
  printf '%s\n' "$rg_matches"
}

assert_rg_no_match 'writer\.clone\(\)|self\.sender\.clone\(\)' \
  src/store src/herdr src/reducer.rs src/operator.rs
assert_rg_no_match 'Arc<Mutex<EventLedgerCache>>|writer_ledger' src

# Positive ownership inventory: exactly one type definition, one store re-export,
# and one production state field. Function signatures and impl headers are
# non-owning type references and remain visible in the complete inventory below.
rg_exact_line_count 1 '^pub struct WriterClient[[:space:]]*\{' src
rg_exact_line_count 1 '^[[:space:]]*WriterClient,' src/store/mod.rs
runtime_writer_fields="$(
  "$awk_executable" '
    /^pub\(crate\) struct RuntimePersistence \{$/ { in_runtime = 1; next }
    in_runtime && /^}$/ { in_runtime = 0 }
    in_runtime && /^[[:space:]]+writer: WriterClient,$/ { count++ }
    END { print count + 0 }
  ' src/herdr/collector.rs
)"
test "$runtime_writer_fields" -eq 1
set +e
writer_inventory="$("$rg_executable" --no-config -n 'WriterClient' \
  src/store src/herdr src/reducer.rs src/operator.rs)"
rg_status=$?
set -e
[ "$rg_status" -eq 0 ] || {
  echo "rg complete inventory failure: $rg_status" >&2
  exit "$rg_status"
}
printf '%s\n' "$writer_inventory"
```

Expected: no `WriterClient` clone, no writer-thread ledger mirror, and no
production `Arc<Mutex<EventLedgerCache>>` remain. The three count/structural gates are the
mechanical whitelist for ownership-bearing declarations; any extra definition,
re-export, or field fails. The final positive inventory is retained for review
and does not pretend that legitimate impl/function type references are forbidden.

- [ ] **Step 6: Run writer, collector, and full regression suites**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked store::writer::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --doc store::writer::WriterClient -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --doc
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test controller -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test convergence -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

Expected: all pass; the focused WriterClient command first reports exactly two
selected and two passed doctests, followed by the complete doctest suite. This
also includes all-feature clippy through the one pre-landed item-level bridge,
the new panic waiter, and existing durability,
cleanup, dedup, late-receipt, Controller precedence, and shutdown tests.

- [ ] **Step 7: Controller review and commit**

After the worker stops, verify the actual file set is a subset of the three
declared paths, obtain the required Opus/Codex reviews, rerun Step 6, then commit:

```bash
git add src/store/writer.rs src/store/mod.rs src/reducer.rs
git commit -m "fix(store): harden writer ownership and panic recovery" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```


---

### Task 2B.2b: Close the collector enqueue-permit lifetime bridge

**Files:**

- Modify: `src/herdr/collector.rs`

**Interfaces:**

- Consumes: Task 2B.2a's borrowing `EnqueuePermit<'_>` and the exact item-level
  temporary bridge from Task 2B.1.
- Produces: the same collector facade behavior with the return type spelled
  exactly `Option<crate::store::EnqueuePermit<'_>>` and no lifetime-lint
  allowance.
- This mechanical dispatch owns only the herdr production subsystem and consumes
  the already integrated store API; it does not edit store or reducer files.

- [ ] **Step 1: Re-derive the installed lint and characterize behavior**

With the frozen Rust 1.97.1 compiler, compile a temporary source containing a
lifetime-bearing type returned with its lifetime hidden under
`-W mismatched-lifetime-syntaxes`; require the named warning, then compile the
explicit `<'_>` spelling and require no warning. Run the existing collector
reservation/commit tests before editing and require them green. This task changes
no runtime behavior, so the installed lint and green collector behavior are its
pre-change characterization rather than a fabricated behavioral red.

Use this exact installed-lint probe; both compilations must exit zero, the hidden
spelling must emit the named warning exactly once, and the explicit spelling must
emit no output:

```bash
probe_dir="$(/usr/bin/mktemp -d /tmp/herdr-i5-lint-probe.XXXXXX)"
trap '/usr/bin/rm -rf -- "$probe_dir"' EXIT
rg_executable="${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}"
set +e
hidden_output="$(/home/mageyuki/.cargo/bin/rustup run 1.97.1 rustc \
  --edition 2024 --crate-type lib --emit metadata -A dead_code \
  -W mismatched-lifetime-syntaxes -o "$probe_dir/hidden.rmeta" - 2>&1 <<'RS'
struct Permit<'a>(&'a ());
fn reserve(value: &()) -> Permit { Permit(value) }
RS
)"
hidden_status=$?
explicit_output="$(/home/mageyuki/.cargo/bin/rustup run 1.97.1 rustc \
  --edition 2024 --crate-type lib --emit metadata -A dead_code \
  -W mismatched-lifetime-syntaxes -o "$probe_dir/explicit.rmeta" - 2>&1 <<'RS'
struct Permit<'a>(&'a ());
fn reserve(value: &()) -> Permit<'_> { Permit(value) }
RS
)"
explicit_status=$?
set -e
test "$hidden_status" = 0
test "$explicit_status" = 0
test "$(printf '%s\n' "$hidden_output" | "$rg_executable" --no-config -cF \
  "warning: hiding a lifetime that's elided elsewhere is confusing")" = 1
test -z "$explicit_output"
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked herdr::collector::tests -- --nocapture
```

- [ ] **Step 2: Remove the bridge and spell the lifetime**

Delete only the temporary item-level `allow` from
`RuntimePersistence::reserve_enqueue` and change its return type to exactly:

```rust
pub(crate) fn reserve_enqueue(&mut self) -> Option<crate::store::EnqueuePermit<'_>> {
```

Do not add any replacement allowance, type alias, second reservation method, or
ownership change.

- [ ] **Step 3: Verify, review, and commit the lifetime closure**

Rerun the complete Step 1 probe, then run collector tests, formatting,
all-feature clippy with `-D warnings`, all-target/all-feature tests, and the exact
source gates below. The rustfmt-canonical one-line signature must occur once. The lint
name must be absent from all `src/`; `rg` status `1` means the required absence,
status `0` is failure, and any other status is operational failure. After the
worker stops, verify the changed-file set is exactly the declared collector path,
obtain the required Opus/Codex reviews, rerun every command, and commit:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked herdr::collector::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
rg_executable="${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}"
test "$("$rg_executable" --no-config -cF \
  "pub(crate) fn reserve_enqueue(&mut self) -> Option<crate::store::EnqueuePermit<'_>> {" \
  src/herdr/collector.rs)" = 1
set +e
"$rg_executable" --no-config -nF 'mismatched_lifetime_syntaxes' src/
lint_inventory_status=$?
set -e
test "$lint_inventory_status" = 1
```

```bash
git add src/herdr/collector.rs
git commit -m "refactor(herdr): close enqueue permit lifetime" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```

Establish a hard post-reliability integration barrier at the clean Task 2B.2b
commit. That exact HEAD must contain, in order, the reviewed integrations for
Tasks 1A.1, 1A.2a, 1A.2b, 1B.1, 1B.2, 1B.3, 2A, 2B.1, 2B.2a, and 2B.2b, and must contain none of
Tasks 3, 4, or 5. Before measurement, the Controller records the exact HEAD,
each component commit ID, and `git diff --name-status` evidence for the complete
baseline-to-barrier range in the research workspace using the revalidated
canonical Git executable, then proves tracked and
index cleanliness without enumerating untracked files. Rerun the complete
reference profile at that frozen HEAD before any runtime performance-health task
is integrated:

```bash
baseline_results_root="${HERDR_INCREMENT5_SELECTED_BASELINE_ROOT:?set to the exactly selected valid baseline root from the research manifest}"
attempt_id="${HERDR_INCREMENT5_ATTEMPT_ID:?record a fresh eight-digit attempt ID in the research ledger first}"
export HERDR_INCREMENT5_ATTEMPT_ID="$attempt_id"
git_executable="${HERDR_INCREMENT5_FROZEN_GIT_EXECUTABLE:?set to the revalidated canonical Git executable}"
post_reliability_results_root="${RESEARCH_HOME:-$HOME/.research}/mageyuki--herdr-top/increment-5-reliability-performance/measurements/post-reliability-$("$git_executable" rev-parse --short=12 HEAD)-attempt-$attempt_id"
runner_script="${HERDR_INCREMENT5_FROZEN_RUNNER_SCRIPT:?set to the revalidated frozen absolute runner script}"
controller_requested="${HERDR_INCREMENT5_CONTROLLER_LAUNCHER_REQUESTED:?set to the frozen native Controller requested path}"
controller_canonical="${HERDR_INCREMENT5_CONTROLLER_LAUNCHER_CANONICAL:?set to its revalidated canonical path}"
controller_sha256="${HERDR_INCREMENT5_CONTROLLER_LAUNCHER_SHA256:?set to its revalidated digest}"
runner_argv=( -p "$runner_script" \
  --subject "$("$git_executable" rev-parse HEAD)" \
  --stage post-reliability \
  --scenario all \
  --baseline-results-root "$baseline_results_root" \
  --output-dir "$post_reliability_results_root" )
```

The trusted parent passes frozen canonical Bash plus this argv through the same
Task 1A.2a `builtin exec -c` native-Controller bootstrap; that Controller applies
`env_clear()` before the allowlist. No newly loaded inherited-environment shell
establishes the boundary.
Validate and record these intermediate results in the research workspace. Task 3
must not be dispatched or integrated until all seven post-reliability documents
are valid `Pass` or `Failed` results selected for this exact barrier HEAD. A valid
performance miss informs comparison but does not retroactively weaken D1 or D2
acceptance; invalid measurement is corrected at the same barrier HEAD before any
of Tasks 3-5 are integrated.
Every document must record `measurement_stage: post_reliability` and must not
claim the not-yet-integrated final performance evidence stream.


---

### Task 3: Share the default-visible Task Run policy

**Files:**

- Modify: `src/activity.rs`
- Modify: `src/tui/projection.rs`
- Modify: `src/tui/app.rs`

**Interfaces:**

- Consumes: existing `OperatorSnapshot.terminal_times` and TUI one-hour terminal
  visibility semantics.
- Produces: `DEFAULT_TERMINAL_VISIBILITY_MS` and
  `default_visible_task_run_count(&DomainModel, &OperatorSnapshot, i64) -> usize`
  in `activity`, reused by TUI and Task 4.

- [ ] **Step 1: Add failing shared-policy tests**

In `src/activity.rs`, add tests covering live, fresh terminal, old terminal, and
terminal-without-timestamp runs:

```rust
#[test]
fn default_visible_count_matches_live_and_one_hour_terminal_policy() {
    let now_ms = 7_200_000;
    let (model, operator) = visibility_fixture(now_ms);
    assert_eq!(default_visible_task_run_count(&model, &operator, now_ms), 3);
}
```

`visibility_fixture(now_ms)` creates four deterministic runs: one running, one
terminal at `now_ms - 3_599_999`, one terminal at `now_ms - 3_600_000`, and one
terminal absent from `terminal_times`. The returned `OperatorSnapshot` contains
only the two explicit terminal timestamps, so the exact visible count is three.

In `src/tui/projection.rs`, retain the existing boundary assertion that
`now_ms == first_terminal_ms + 3_600_000` hides the terminal run.

- [ ] **Step 2: Verify the test fails because the shared API is absent**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked activity::tests::default_visible_count_matches_live_and_one_hour_terminal_policy -- --exact --nocapture
```

Expected: compilation fails on the missing shared function.

- [ ] **Step 3: Implement one shared visibility predicate and count**

Add to `src/activity.rs`:

```rust
pub const DEFAULT_TERMINAL_VISIBILITY_MS: i64 = 60 * 60 * 1_000;

#[must_use]
pub fn is_default_visible_task_run(
    run: &crate::model::TaskRun,
    operator: &OperatorSnapshot,
    now_ms: i64,
) -> bool {
    !run.state.is_terminal()
        || operator.terminal_times.get(&run.run_id).is_none_or(|first_terminal_ms| {
            now_ms < first_terminal_ms.saturating_add(DEFAULT_TERMINAL_VISIBILITY_MS)
        })
}

#[must_use]
pub fn default_visible_task_run_count(
    model: &crate::model::DomainModel,
    operator: &OperatorSnapshot,
    now_ms: i64,
) -> usize {
    model.task_runs().filter(|run| is_default_visible_task_run(run, operator, now_ms)).count()
}
```

Replace `tui::projection`'s private terminal-duration constant and inline
predicate with the shared constant/predicate. Replace `tui::app`'s production
`TERMINAL_VISIBILITY_MS` use and all four test references reported by the exact
frozen-canonical-rg search
`"$rg_executable" --no-config -n 'TERMINAL_VISIBILITY_MS' src/tui/app.rs`, using
`activity::DEFAULT_TERMINAL_VISIBILITY_MS`; do not retain a private alias.
`rg_executable` is first bound explicitly from
`${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}`.
Treat rg status `0` as the expected inventory, `1` as a missing required inventory,
and every other status as an operational failure.
The expected migration is all five current sites. Preserve execution-tree subtree hiding and dependency-DAG
behavior; only the visibility decision moves.

- [ ] **Step 4: Run focused and full TUI tests**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked activity::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked tui::projection::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked tui::app::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

Expected: all pass with no UI behavior change.

- [ ] **Step 5: Controller review and commit**

After reviews and independent verification, commit only the declared paths:

```bash
git add src/activity.rs src/tui/projection.rs src/tui/app.rs
git commit -m "refactor(tui): share default-visible run policy" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```


---

### Task 4: Runtime performance tracker and admitting channels

**Files:**

- Create: `src/performance.rs`
- Modify: `src/lib.rs`

**Interfaces:**

- Consumes: Task 3's `default_visible_task_run_count`, `DomainModel`, and
  `OperatorSnapshot`.
- Produces:
  `PerformanceClock`, `SystemPerformanceClock`, `PerformanceIngress`,
  `Admission`, crate-private `AdmissionStamp`/`AdmissionObserver`, `Admitted<T>`,
  `AdmittingSender<T>`, `admitted_channel`, `admitted_channel_observed`,
  `PerformanceSampler`, `PerformanceSnapshot`, and
  `PerformanceDegradationReason`. Under `#[cfg(any(test, feature =
  "workload-harness"))]`, it also produces a documented, `#[doc(hidden)]` public
  `TestPerformanceClock` with `new(Duration)`, `set(Duration)`, and
  `advance(Duration)`. Under Linux plus `workload-harness`, it produces the
  documented hidden `AbsoluteMonotonicPerformanceClock`, backed by
  `clock_gettime(CLOCK_MONOTONIC)`, for every serialized authoritative stamp.
  `PerformanceSampler` retains the exact clock value used by its most recent
  `sample` call and, only under `cfg(any(test, feature = "workload-harness"))`, exposes documented hidden
  `workload_sampled_at() -> Duration`; this is observation of the already-used
  sample instant, never a second clock read.
  This visibility lets an external integration-test crate
  use the opt-in feature without exposing the clock in an ordinary build. Task 6,
  which owns `collector.rs`, adds the feature-only collector clock-injection
  constructor; Task 4 does not claim or edit that surface.
  Under `workload-harness`, `Admitted<T>` also exposes only the documented hidden
  primitive tuple accessor `workload_stamp`; the crate-private typed accessor is
  unit-test-only.

- [ ] **Step 1: Write failing virtual-clock envelope and recovery tests**

Create `src/performance.rs` with unit tests named:

```rust
#[test]
fn exact_rate_boundaries_pass_and_strictly_greater_values_degrade() {
    for (width, limit, reason) in [
        (Duration::from_secs(1), 100, PerformanceDegradationReason::EventsOneSecond),
        (Duration::from_secs(10), 1_000, PerformanceDegradationReason::EventsTenSeconds),
        (Duration::from_secs(60), 1_200, PerformanceDegradationReason::EventsSixtySeconds),
    ] {
        let clock = Arc::new(TestPerformanceClock::new(width - Duration::from_nanos(1)));
        let (ingress, mut sampler) = performance_tracker(clock);
        let admitted = (0..limit).map(|_| ingress.admit()).collect::<Vec<_>>();
        let (model, operator) = empty_inputs();
        assert!(!sampler.sample(&model, &operator, 0).reasons.contains(&reason));
        let over_limit = ingress.admit();
        assert!(sampler.sample(&model, &operator, 0).reasons.contains(&reason));
        drop(over_limit);
        drop(admitted);
    }
    let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
    let (ingress, mut sampler) = performance_tracker(clock.clone());
    ingress.admit().complete();
    clock.set(Duration::from_secs(1));
    let (model, operator) = empty_inputs();
    assert_eq!(sampler.sample(&model, &operator, 0).events_one_second, 0);
}

#[test]
fn lag_reason_latches_until_the_breach_generation_drains() {
    let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
    let (ingress, mut sampler) = performance_tracker(clock.clone());
    let first = ingress.admit();
    clock.advance(Duration::from_millis(500));
    let second = ingress.admit();
    clock.advance(Duration::from_millis(501));
    let (model, operator) = empty_inputs();
    assert!(sampler.sample(&model, &operator, 0).reasons.contains(
        &PerformanceDegradationReason::EventLag
    ));
    first.complete();
    assert!(sampler.sample(&model, &operator, 0).reasons.contains(
        &PerformanceDegradationReason::EventLag
    ));
    second.complete();
    assert!(!sampler.sample(&model, &operator, 0).reasons.contains(
        &PerformanceDegradationReason::EventLag
    ));
}

#[test]
fn rolling_windows_include_origin_without_duration_underflow() {
    let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
    let (ingress, mut sampler) = performance_tracker(clock);
    let admission = ingress.admit();
    let (model, operator) = empty_inputs();
    let snapshot = sampler.sample(&model, &operator, 0);
    assert_eq!((snapshot.events_one_second, snapshot.events_ten_seconds,
        snapshot.events_sixty_seconds), (1, 1, 1));
    admission.complete();
}

#[test]
fn sampler_reports_the_exact_clock_value_used_for_the_snapshot() {
    let clock = Arc::new(TestPerformanceClock::new(Duration::from_nanos(123)));
    let (_ingress, mut sampler) = performance_tracker(clock.clone());
    let (model, operator) = empty_inputs();
    sampler.sample(&model, &operator, 0);
    clock.set(Duration::from_nanos(456));
    assert_eq!(sampler.workload_sampled_at(), Duration::from_nanos(123));
}

#[cfg(all(target_os = "linux", feature = "workload-harness"))]
#[test]
fn absolute_performance_clock_matches_the_kernel_monotonic_domain() {
    let before = direct_clock_gettime_monotonic();
    let observed = AbsoluteMonotonicPerformanceClock.monotonic_now();
    let after = direct_clock_gettime_monotonic();
    assert!(before <= observed && observed <= after);
    assert!(observed > Duration::from_secs(1));
}

#[test]
fn model_envelope_boundaries_and_execution_cardinality_are_exact() {
    for (panes, visible_runs, dependency_edges, execution_edges, expected) in [
        (50, 200, 1_000, 1_000, BTreeSet::new()),
        (51, 200, 1_000, 1_000,
            BTreeSet::from([PerformanceDegradationReason::LivePanes])),
        (50, 201, 1_000, 1_000,
            BTreeSet::from([PerformanceDegradationReason::DefaultVisibleTaskRuns])),
        (50, 200, 1_001, 1_000,
            BTreeSet::from([PerformanceDegradationReason::DependencyEdges])),
        (50, 200, 1_000, 5_000, BTreeSet::new()),
    ] {
        let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
        let (_ingress, mut sampler) = performance_tracker(clock);
        let (model, operator) =
            load_inputs(panes, visible_runs, dependency_edges, execution_edges);
        let snapshot = sampler.sample(&model, &operator, 0);
        assert_eq!((snapshot.live_panes, snapshot.default_visible_task_runs,
            snapshot.dependency_edges, snapshot.execution_edges),
            (panes, visible_runs, dependency_edges, execution_edges));
        assert_eq!(snapshot.reasons, expected);
    }
}

#[tokio::test]
async fn admitted_channel_completes_dropped_and_explicit_admissions_once() {
    struct NonClone(u8);
    let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
    let (ingress, mut sampler) = performance_tracker(clock);
    let (sender, mut receiver) = admitted_channel(2, ingress.clone());
    let cloned = sender.clone();
    sender.try_send(NonClone(7)).unwrap();
    cloned.try_send(NonClone(8)).unwrap();
    assert!(matches!(sender.try_send(NonClone(9)),
        Err(TrySendError::Full(NonClone(9)))));
    let (model, operator) = empty_inputs();
    let before = sampler.sample(&model, &operator, 0);
    assert_eq!((before.pending_events, before.admission_high_water,
        before.events_one_second), (2, 2, 2));
    drop(receiver.recv().await.unwrap());
    assert_eq!(sampler.sample(&model, &operator, 0).pending_events, 1);
    let (value, admission) = receiver.recv().await.unwrap().into_parts();
    assert_eq!(value.0, 8);
    admission.complete();
    assert_eq!(sampler.sample(&model, &operator, 0).pending_events, 0);

    let (closed_sender, closed_receiver) = admitted_channel(1, ingress);
    drop(closed_receiver);
    assert!(matches!(closed_sender.try_send(NonClone(10)),
        Err(TrySendError::Closed(NonClone(10)))));
    let after = sampler.sample(&model, &operator, 0);
    assert_eq!((after.pending_events, after.admission_high_water,
        after.events_one_second), (0, 2, 2));
    assert_eq!(after.event_lag, Duration::ZERO);
}

#[tokio::test]
async fn observed_admitting_channel_preserves_post_reservation_timestamp() {
    struct NonClone(u8);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observer = admission_observer_collecting(observed.clone());
    let clock = Arc::new(TestPerformanceClock::new(Duration::from_millis(25)));
    let (ingress, _sampler) = performance_tracker(clock);
    let (sender, mut receiver) = admitted_channel_observed(1, ingress, Some(observer));
    sender.send(NonClone(7)).await.unwrap();
    let admitted = receiver.recv().await.unwrap();
    assert_eq!(observed.lock().unwrap().as_slice(), &[admitted.stamp()]);
}
```

Define `empty_inputs()` as `DomainModel::default()` plus an empty
`OperatorSnapshot`. Define `load_inputs(panes, visible_runs, dependency_edges,
execution_edges)` with deterministic IDs and unique acyclic edges, using the
same visibility rules as Task 1A.1; these helpers are test-only and return exactly
the requested counts. `direct_clock_gettime_monotonic()` is a test-only direct
`libc::clock_gettime` probe independent of the production clock implementation.

- [ ] **Step 2: Run the new module test and verify the red state**

Add `pub mod performance;` to `src/lib.rs`, then run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked performance::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness performance::tests::absolute_performance_clock_matches_the_kernel_monotonic_domain -- --exact --nocapture
```

Expected: compilation fails until the module interfaces and algorithms exist.

- [ ] **Step 3: Implement the monotonic tracker and closed snapshot**

The first line of the new `src/performance.rs` is exactly
`#![allow(unsafe_code)]`. This module-local allowance is required because the
crate root has `#![deny(unsafe_code)]`; it permits only two documented Linux
`libc::clock_gettime(CLOCK_MONOTONIC)` calls and does not relax any other module
or the crate root. Keep one unsafe block immediately around the production clock
FFI call, with its checked error/range conversion, and exactly one separately
gated unsafe block inside the test-only `direct_clock_gettime_monotonic` probe so
that the probe remains independent. A source assertion rejects another
`allow(unsafe_code)`, a crate-root allowance, any third unsafe block/call, or
either accepted block outside its exact production/test function.

Use these public shapes:

```rust
pub trait PerformanceClock: Send + Sync {
    fn monotonic_now(&self) -> Duration;
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PerformanceDegradationReason {
    LivePanes,
    DefaultVisibleTaskRuns,
    DependencyEdges,
    EventsOneSecond,
    EventsTenSeconds,
    EventsSixtySeconds,
    EventLag,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PerformanceSnapshot {
    pub event_lag: Duration,
    pub pending_events: usize,
    pub admission_high_water: u64,
    pub completion_high_water: u64,
    pub events_one_second: usize,
    pub events_ten_seconds: usize,
    pub events_sixty_seconds: usize,
    pub live_panes: usize,
    pub default_visible_task_runs: usize,
    pub dependency_edges: usize,
    pub execution_edges: usize,
    pub reasons: std::collections::BTreeSet<PerformanceDegradationReason>,
}

pub fn performance_tracker(
    clock: std::sync::Arc<dyn PerformanceClock>,
) -> (PerformanceIngress, PerformanceSampler);

impl PerformanceIngress {
    #[must_use]
    pub fn admit(&self) -> Admission;
}

impl Admission {
    pub fn complete(self);
}

impl PerformanceSampler {
    pub fn sample(
        &mut self,
        model: &crate::model::DomainModel,
        operator: &crate::activity::OperatorSnapshot,
        now_ms: i64,
    ) -> PerformanceSnapshot;

    #[cfg(any(test, feature = "workload-harness"))]
    #[doc(hidden)]
    pub fn workload_sampled_at(&self) -> Duration;
}
```

The shared state contains monotonically allocated sequences, admission times in a
`BTreeMap<u64, Duration>`, rolling admission timestamps in a `VecDeque`, maximum
completed sequence, and optional lag-breach high water. `Admission::complete`
removes once; `Drop` calls the same idempotent completion path.

At sample time, compute each boundary with `now.checked_sub(width)`. When it is
`Some(boundary)`, retain timestamps strictly greater than the boundary; when it
is `None`, the window reaches before the monotonic origin, so retain every
recorded timestamp through `now`. This avoids `Duration` underflow without
incorrectly dropping an admission at origin. Then compute
model counts, and add reasons only for strict `>` comparisons. Lag is zero only
when no admission remains. When lag first exceeds one second, latch current
admission high water; clear only after no pending sequence at or below that value
remains, then immediately relatch if the new oldest item is still over one
second.
The sampler stores that same `now` as `last_sampled_at` before returning the
snapshot. Calling `workload_sampled_at` before the first sample is a fail-closed
error in harness code and cannot synthesize zero.

`SystemPerformanceClock` stores one `Instant` origin and returns only elapsed
duration; ordinary production uses it, and its values are never serialized or
joined to an external clock. Linux `workload-harness` additionally implements
`AbsoluteMonotonicPerformanceClock` with `libc::clock_gettime(CLOCK_MONOTONIC)`,
checked conversion to `Duration`, and fail-closed handling of syscall or range
failure. The authoritative harness entrypoint injects this same clock into the
performance tracker, admission observer, and feature-only frame driver, while
the sibling observer reads the same OS clock domain. No serialized admission,
priming, workload, terminal, publication, or render timestamp may use the
origin-relative production clock. `TestPerformanceClock` stores nanoseconds in an `AtomicU64`; advancing
or setting it never sleeps and panics on nanosecond overflow in tests. Gate it
with `#[cfg(any(test, feature = "workload-harness"))]`, make the type and its three
methods `pub`, add API docs plus `#[doc(hidden)]`, and use it from integration
tests only when `workload-harness` is enabled. This avoids both external
visibility failure and unused-private-code warnings under all-feature clippy.

- [ ] **Step 4: Implement admission-aware Tokio channel wrappers**

Expose exact value-preserving APIs:

```rust
pub fn admitted_channel<T>(
    capacity: usize,
    ingress: PerformanceIngress,
) -> (AdmittingSender<T>, tokio::sync::mpsc::Receiver<Admitted<T>>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionStamp {
    pub sequence: u64,
    pub admitted_at: Duration,
}

pub(crate) type AdmissionObserver<T> =
    std::sync::Arc<dyn Fn(AdmissionStamp, &T) + Send + Sync>;

pub(crate) fn admitted_channel_observed<T>(
    capacity: usize,
    ingress: PerformanceIngress,
    observer: Option<AdmissionObserver<T>>,
) -> (AdmittingSender<T>, tokio::sync::mpsc::Receiver<Admitted<T>>);

impl<T> Clone for AdmittingSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            ingress: self.ingress.clone(),
            observer: self.observer.clone(),
        }
    }
}

impl<T> AdmittingSender<T> {
    pub fn try_send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::TrySendError<T>>;
    pub async fn send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>>;
}

impl<T> Admitted<T> {
    #[cfg(test)]
    pub(crate) fn stamp(&self) -> AdmissionStamp;
    #[cfg(feature = "workload-harness")]
    #[doc(hidden)]
    pub fn workload_stamp(&self) -> (u64, Duration);
    pub fn into_parts(self) -> (T, Admission);
}
```

`admitted_channel` delegates to `admitted_channel_observed(..., None)`. Call the
underlying sender's `try_reserve()` or `reserve().await` first. Create the
`Admission` only after capacity reservation succeeds, synchronously invoke the
optional observer with the same immutable stamp plus a shared borrow of the
reserved value, then send
`Admitted { value, admission }` through the reserved permit. Map reservation
failure from `TrySendError<()>`/`SendError<()>` to Tokio's public `Full(value)` or
`Closed(value)` with the original `T`. Thus full/closed attempts allocate no
sequence, add no rolling-rate timestamp, and create no lag. Never require
`T: Clone`. The optional observer is cloned as an `Arc`; it cannot allocate a
second admission or change the stamp. Implement `Clone` manually by cloning only
the underlying Tokio sender, `PerformanceIngress`, and optional observer; do not
derive it, because the derived generic bound would make
`AdmittingSender<ControllerRequest>` non-cloneable. The unit test sends a
non-Clone payload through both original and cloned wrappers.
In the unit-test module define the previously used helper concretely:

```rust
fn admission_observer_collecting<T: 'static>(
    observed: Arc<Mutex<Vec<AdmissionStamp>>>,
) -> AdmissionObserver<T> {
    Arc::new(move |stamp, _value| observed.lock().unwrap().push(stamp))
}
```

The crate-private `stamp` accessor and collecting helper are unit-test-only. The
feature-gated hidden public `workload_stamp` returns the same `(sequence,
admitted_at)` primitives and is consumed by the external workload integration
test, so an all-features non-test library has no dead crate-private accessor under
`-D warnings`. It is absent from an ordinary build and does not expose the
crate-private `AdmissionStamp` type. `AdmissionStamp` remains used by the sender's
internal observer callback and therefore is not dead production code.

- [ ] **Step 5: Run focused, lint, and full tests**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked performance::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

Expected: all pass, including exact half-open window boundaries, lag generation
recovery, stable reason ordering, visible-run policy, and channel value recovery.

- [ ] **Step 6: Controller review and commit**

After reviews and verification, commit only the two declared paths:

```bash
git add src/performance.rs src/lib.rs
git commit -m "feat(perf): add runtime load and lag tracker" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```


---

### Task 5: Provider queue admission tracking

**Files:**

- Modify: `src/provider/mod.rs`

**Interfaces:**

- Consumes: Task 4's `PerformanceIngress` and Tokio bounded reservation API.
- Produces:
  the private tracked `ProviderEventSender` path plus crate-private
  `ProviderIngressEvent { event, admission: Option<Admission> }`, while
  preserving existing public raw-sender constructors for their current callers
  and tests. Task 6 creates the crate-private production constructor in the same
  task that adds its collector caller, avoiding an unused library item between
  task commits.

- [ ] **Step 1: Add failing provider admission tests**

Add tests in `src/provider/mod.rs`:

```rust
#[test]
fn provider_flush_admits_only_events_that_enter_the_bounded_queue() {
    let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
    let (ingress, mut sampler) = performance_tracker(clock);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let mut pending = PendingEvents::new(ProviderDiagnostics::default());
    assert_eq!(pending.merge(source_state(Provider::Claude)), MergeOutcome::Accepted);
    assert_eq!(pending.merge(agent_upsert(Provider::Codex)), MergeOutcome::Accepted);
    assert_eq!(pending.merge(agent_activity(Provider::Codex)), MergeOutcome::Accepted);
    pending.flush_to_sender(&ProviderEventSender::Tracked { sender, ingress });
    let (model, operator) = empty_performance_inputs();
    let full = sampler.sample(&model, &operator, 0);
    assert_eq!((full.pending_events, full.admission_high_water,
        full.events_one_second), (1, 1, 1));
    let control = receiver.blocking_recv().unwrap();
    assert!(matches!(control.event, ProviderEvent::SourceState { .. }));
    assert!(control.admission.is_none());
    let reducer_bound = receiver.blocking_recv().unwrap();
    assert!(matches!(reducer_bound.event, ProviderEvent::AgentUpsert { .. }));
    reducer_bound.admission.unwrap().complete();
    assert_eq!(sampler.sample(&model, &operator, 0).pending_events, 0);
    assert!(matches!(pending.next_event(), Some((PendingToken::Activity(_), _))));
}

#[test]
fn provider_closed_queue_returns_the_original_pending_event_without_lag_leak() {
    let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
    let (ingress, mut sampler) = performance_tracker(clock);
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(receiver);
    let mut pending = PendingEvents::new(ProviderDiagnostics::default());
    assert_eq!(pending.merge(agent_upsert(Provider::Claude)), MergeOutcome::Accepted);
    pending.flush_to_sender(&ProviderEventSender::Tracked { sender, ingress });
    let (model, operator) = empty_performance_inputs();
    let closed = sampler.sample(&model, &operator, 0);
    assert_eq!((closed.pending_events, closed.admission_high_water,
        closed.events_one_second, closed.events_ten_seconds,
        closed.events_sixty_seconds), (0, 0, 0, 0, 0));
    assert_eq!(closed.event_lag, Duration::ZERO);
    assert!(matches!(pending.next_event(), Some((PendingToken::Upsert(_), _))));
}
```

Use Task 4's manual clock test support inside this crate module; do not use wall
sleeps. Define `source_state`
as a `ProviderEvent::SourceState` with `Available`, and define
`agent_upsert` as a valid reducer-bound synthetic `ProviderEvent::AgentUpsert`.
Define `agent_activity` for the same deterministic Codex entity so slot order is
upsert then activity. The third event
in the capacity-two test must remain pending after `Full`, proving that a failed
reservation changes neither rates nor high water; the closed test uses a
reducer-bound event rather than a control event and proves all three rate windows
and high water remain zero. Also add a malformed-event assertion proving it
carries `None` and changes no rate, lag, high-water, or pending counter. Define
`empty_performance_inputs` as an empty model and empty `OperatorSnapshot`.

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked provider::tests::provider_flush_admits_only_events_that_enter_the_bounded_queue -- --exact --nocapture
```

Expected: compilation fails because the tracked sender and flush path do not
exist.

- [ ] **Step 3: Add a private sender abstraction without changing public raw APIs**

Use a private enum to avoid duplicating the provider worker loop:

```rust
enum ProviderEventSender {
    Raw(tokio::sync::mpsc::Sender<ProviderEvent>),
    Tracked {
        sender: tokio::sync::mpsc::Sender<ProviderIngressEvent>,
        ingress: crate::performance::PerformanceIngress,
    },
}

pub(crate) struct ProviderIngressEvent {
    pub event: ProviderEvent,
    pub admission: Option<crate::performance::Admission>,
}

impl ProviderEventSender {
    fn try_send(
        &self,
        event: ProviderEvent,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<ProviderEvent>>;
}

```

Keep public `PendingEvents::flush_to(&mpsc::Sender<ProviderEvent>)` as a raw
compatibility wrapper. Add private
`flush_to_sender(&ProviderEventSender)`, and change `provider_thread_main` plus
`run_provider_cycle` to use the enum. Existing public `spawn_provider_thread`
and `spawn_provider_thread_with_rescan_interval` wrap their sender as `Raw`.
Do not add the crate-private performance constructor yet; Task 6 owns it and its
first production caller together. The tracked sender's `try_send` first reserves
queue capacity. Only after successful reservation does
it create `Some(ingress.admit())` for `SessionResolved`, `AgentUpsert`, or
`Activity`; `SourceState` and `Malformed` carry `None` because they terminate in
coverage/log handling and never enter the reducer. Full/closed attempts create
no admission and return the original event to `PendingEvents`. Both paths retain
the same diagnostics, ordering, and retry behavior.

Until Task 6 adds the production caller, place a narrowly scoped
`#[allow(dead_code)] // removed by Task 6 when the tracked production caller lands`
only on the tracked enum variant and `ProviderIngressEvent` item needed by the
Task 5 tests. Do not allow dead code at module or crate scope. Task 6 must remove
both allowances in the same diff that constructs and consumes these items; this
keeps each intermediate all-feature `-D warnings` gate honest without retaining
a permanent suppression.

- [ ] **Step 4: Run provider and full regression suites**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked provider::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test provider_claude -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test provider_codex -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

Expected: raw semantics are unchanged; successful reducer-bound entries produce
one admission; source/malformed controls produce none; full/closed attempts leak
none; all provider ordering/saturation tests pass.

- [ ] **Step 5: Controller review and commit**

After reviews and verification, commit:

```bash
git add src/provider/mod.rs
git commit -m "feat(perf): track provider queue admissions" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```


---

### Task 6: Controller and collector performance integration

**Files:**

- Modify: `src/provider/mod.rs`
- Modify: `src/herdr/controller.rs`
- Modify: `src/herdr/collector.rs`
- Modify: `tests/controller.rs`
- Modify: `tests/convergence.rs`
- Modify: `tests/workload_harness.rs`

**Interfaces:**

- Consumes: Task 2B.2a's non-cloneable writer, Task 4's tracker/admitting channel
  and observed-channel seam, Task 5's tracked provider sender path, the model
  receiver, and the operator receiver.
- Produces a public coherent publication:

  ```rust
  #[cfg(feature = "workload-harness")]
  #[derive(Clone, Copy, Debug, Eq, PartialEq)]
  #[doc(hidden)]
  pub struct WorkloadSampleStamp {
      pub sample_ordinal: u64,
      pub sampled_at_ns: u64,
  }

  #[derive(Clone, Debug, Eq, PartialEq)]
  pub struct PerformancePublication {
      pub snapshot: PerformanceSnapshot,
      pub effective_quality: ObservationQuality,
      #[cfg(feature = "workload-harness")]
      #[doc(hidden)]
      pub workload_sample_stamp: Option<WorkloadSampleStamp>,
  }

  #[cfg(feature = "workload-harness")]
  #[derive(Clone, Debug)]
  #[doc(hidden)]
  pub struct WorkloadPerformanceSample {
      pub source_quality: ObservationQuality,
      pub publication: PerformancePublication,
  }

  #[cfg(feature = "workload-harness")]
  #[doc(hidden)]
  pub type WorkloadPerformanceObserver =
      Arc<dyn Fn(&WorkloadPerformanceSample) + Send + Sync + 'static>;
  ```

  `CollectorHandle.performance` is a
  `watch::Receiver<PerformancePublication>` used by every authoritative renderer
  and deadline recorder. `CollectorHandle.quality` remains a compatibility
  projection of `PerformancePublication::effective_quality` for non-authoritative
  consumers. Herdr and Controller reducer queues carry an `Admission`; provider
  queue envelopes carry `Some(Admission)` only for reducer-bound variants. This
  task also adds the crate-private
  `spawn_provider_thread_with_diagnostics_and_performance` constructor and its
  first collector caller together, so all-feature `-D warnings` never sees an
  unused constructor. Under `workload-harness` it also produces a synchronous
  generation observer invoked for every performance-monitor sample before watch
  publication. It receives one `WorkloadPerformanceSample`; its nested
  publication always has `workload_sample_stamp == Some(...)` and carries the
  same snapshot/effective-quality pair considered for watch publication. It
  cannot suppress or coalesce production watch publication and is absent from
  default builds. `same_render_payload(left, right)` compares only `snapshot`
  and `effective_quality`, never the feature-only stamp.
  Under `workload-harness`, this task also owns a documented
  `#[doc(hidden)]` public
  `spawn_with_controller_and_performance_clock(sock, session, restored, writer,
  controller_listener, Arc<dyn PerformanceClock>,
  WorkloadPerformanceObserver)` wrapper with the same result as
  `spawn_with_controller`; it delegates to the common private constructor.
  The default constructor has no observer parameter, stamp field, first-sample
  publication branch, or workload evidence side channel.

- [ ] **Step 1: Write failing admission, lag, overload, and precedence tests**

In `src/herdr/controller.rs`, add a virtual-clock test proving an invalid but
queue-admitted frame completes its admission after the typed rejection. In
`src/herdr/collector.rs`, add focused tests:

```rust
#[test]
fn performance_quality_composition_preserves_stronger_source_states() {
    let degraded = PerformanceSnapshot {
        reasons: BTreeSet::from([PerformanceDegradationReason::EventsSixtySeconds]),
        ..PerformanceSnapshot::default()
    };
    assert_eq!(compose_quality(ObservationQuality::Disconnected, &degraded), ObservationQuality::Disconnected);
    assert_eq!(compose_quality(ObservationQuality::Reconciling, &degraded), ObservationQuality::Reconciling);
    assert_eq!(compose_quality(ObservationQuality::Live, &degraded), ObservationQuality::Degraded);
}

#[tokio::test]
async fn twice_target_becomes_degraded_by_sixty_seconds_without_loss() {
    let clock = Arc::new(TestPerformanceClock::new(Duration::ZERO));
    let (ingress, mut sampler) = performance_tracker(clock.clone());
    let (model, operator, expected_execution_edges) = target_performance_inputs();
    for index in 0_u64..1_201 {
        clock.set(Duration::from_millis((index + 1) * 25));
        ingress.admit().complete();
    }
    clock.set(Duration::from_millis(30_025));
    let threshold_snapshot = sampler.sample(&model, &operator, 30_025);
    assert_eq!((threshold_snapshot.live_panes,
        threshold_snapshot.default_visible_task_runs,
        threshold_snapshot.dependency_edges,
        threshold_snapshot.execution_edges),
        (50, 200, 1_000, expected_execution_edges));
    assert!(threshold_snapshot.reasons
        .contains(&PerformanceDegradationReason::EventsSixtySeconds));
    for index in 1_201_u64..2_400 {
        clock.set(Duration::from_millis((index + 1) * 25));
        ingress.admit().complete();
    }
    clock.set(Duration::from_secs(60));
    let snapshot = sampler.sample(&model, &operator, 60_000);
    assert_eq!(snapshot.pending_events, 0);
    assert_eq!(snapshot.admission_high_water, 2_400);
    assert_eq!(snapshot.completion_high_water, 2_400);
    assert_eq!((snapshot.live_panes, snapshot.default_visible_task_runs,
        snapshot.dependency_edges, snapshot.execution_edges),
        (50, 200, 1_000, expected_execution_edges));
    assert!(snapshot.reasons.contains(&PerformanceDegradationReason::EventsSixtySeconds));
    assert_eq!(compose_quality(ObservationQuality::Live, &snapshot), ObservationQuality::Degraded);
}
```

Also add the deterministic unit test
`performance_publication_remains_coherent_while_quality_projection_is_paused`.
A test-only hook pauses the monitor after it publishes one composite generation
but before it updates the compatibility `quality` projection. The test borrows
`CollectorHandle.performance` during that interleaving and requires its snapshot
reasons and `effective_quality` to describe the same new generation; no
authoritative consumer may read the separate projection to complete this test.
Add and keep
`performance_generation_observer_records_every_sample_before_watch_coalescing`.
Drive three monitor ticks whose first two render payloads are equal and whose
third changes. Require the raw observer to receive all three contiguous ordinals,
nondecreasing exact sampler timestamps, source quality, and complete stamped
publications. The first feature sample must force one notification from the
initial `None` stamp; the second equal sample must leave the watched value,
stamp, version, and receiver `has_changed()` state unchanged; and the third must
atomically publish its exact stamp/snapshot/quality tuple once. Also add the
focused regressions
`equal_performance_sample_does_not_silently_advance_watch_stamp` and
`changed_performance_sample_publishes_stamp_snapshot_and_quality_atomically`.
Together these are the red/green proof that the final evidence stream cannot be
silently reduced to watch notifications and that an equal sample cannot create
a hidden watch/App-cache divergence.

`target_performance_inputs()` constructs the exact 50-pane,
200-visible-run, 1,000-dependency-edge fixture inside the collector test module
using the same deterministic pair enumeration frozen in Task 1A and returns its
independently enumerated exact execution-edge count as the third tuple member.
The four snapshot assertions at both 30.025 and 60 seconds are the local drift
guard for all frozen model cardinalities. Task 6's workload-harness integration
also compares the final execution-edge identity set and its length with the Task
1A oracle, so the returned count cannot drift with both sides unnoticed.
The Controller request test samples the tracker before and after
`service_request`, asserting pending count changes from one to zero and that the
response is the existing typed `Rejected::Invalid` value.

Add integration assertions to `tests/controller.rs` and `tests/convergence.rs`
that `CollectorHandle.performance` is present and every submitted identity
remains in the final model. Exact rate-window boundaries remain virtual-clock
unit tests in `performance`/`collector`; integration tests do not sleep for a
minute or compress sustained traffic into a false one-second burst.
Name the Controller integration test
`collector_handle_publishes_coherent_performance_generation`; it subscribes to
the real handle, admits one request, and asserts the watch publishes the
corresponding high-water/pending transition paired with its effective quality,
without relying on a name filter.

In `tests/convergence.rs`, add
`replay_drains_admitted_events_before_closed_end`. Do not construct the private
`src/herdr/collector.rs::ReceivedEvent` type from the integration test. Instead,
configure the existing `tests/common/mod.rs::scripted_mock::ScriptedConfig` with
one snapshot generation containing at least three valid event frames and
`close_after_snapshots(vec![0])`. The normal collector subscription path must
create and admit the private `ReceivedEvent` values, observe closure after those
frames, enter the real replay path, and process the entire buffer before returning
`ReplayOutcome::Ended`. The first version of this test is deliberately limited
to APIs that exist before Task 6: observe the exact final run identity through
the existing model receiver, then
use the existing convergence-test ownership path: stop `CollectorHandle`, call
the separately retained `WriterLifecycle::shutdown` to drain/checkpoint/join, and
reopen with `open_reader` to assert the persisted events. No command-capable
writer or pending receiver is recovered from the handle, and this initial red
does not mention `CollectorHandle.performance`, `PerformanceSnapshot`, or any
other new Task 6 symbol. The pre-Task-6 behavior compiles cleanly and is
behaviorally red because `drain_events` appends the
values and then returns `Disconnected`, causing the caller to exit early.
Only after Step 3 has established the admission API does Step 4 extend this same
test with exact completed high-water and zero-pending assertions through the new
performance receiver.

Extend `tests/workload_harness.rs` with ordinary, nonignored virtual-clock tests
that drive the exact 1,200 sustained, 1,000 burst, and 2,400 twice-target
schedules through the real bounded Controller/provider/Herdr reducer queues.
They compare every sequence outcome and final run/edge identity, require exact
target and burst to remain nondegraded, and require twice-target to contain the
60-second reason by its deadline. They gate deterministic semantics only, not
host wall-clock timing.

- [ ] **Step 2: Run focused tests and verify the red state**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test convergence replay_drains_admitted_events_before_closed_end -- --exact --nocapture
rg_executable="${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}"
test "$("$rg_executable" --no-config -cF 'fn performance_quality_composition_preserves_stronger_source_states()' src/herdr/collector.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn performance_generation_observer_records_every_sample_before_watch_coalescing()' src/herdr/collector.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn equal_performance_sample_does_not_silently_advance_watch_stamp()' src/herdr/collector.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn changed_performance_sample_publishes_stamp_snapshot_and_quality_atomically()' src/herdr/collector.rs)" = 1
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked herdr::collector::tests::performance_quality_composition_preserves_stronger_source_states -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness herdr::collector::tests::performance_generation_observer_records_every_sample_before_watch_coalescing -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness herdr::collector::tests::equal_performance_sample_does_not_silently_advance_watch_stamp -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness herdr::collector::tests::changed_performance_sample_publishes_stamp_snapshot_and_quality_atomically -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test controller collector_handle_publishes_coherent_performance_generation -- --exact --nocapture
```

Expected: run the convergence command first in the actual TDD sequence, before
adding any new performance assertion. It must compile and fail only because the
buffered events are dropped on closed-channel replay. A compile error or
unrelated pre-existing failure is not this behavioral red; preserve the exact
failure output before implementation, and require its harness summary to report
exactly one selected failing test. The four focused performance Cargo commands
may then be deliberate missing-symbol reds because they specify genuinely new
Task 6 APIs: use the global compile-boundary exception, first prove one exact
declaration for every named test with frozen `rg`, then require each nonzero
Cargo exit's compiler diagnostic to name its exact missing Task 6 item. They do
not have a harness summary until their green runs; zero-selection success is
always rejected.

- [ ] **Step 3: Track Controller requests from queue entry to reducer outcome**

Change `ControllerRequestSender.sender` to
`AdmittingSender<ControllerRequest>` and `ControllerRequestReceiver.receiver` to
`mpsc::Receiver<Admitted<ControllerRequest>>`. Keep the public
`request_channel` surface limited to `PerformanceIngress`:

```rust
pub fn request_channel(
    capacity: usize,
    diagnostics: ControllerDiagnosticsHandle,
    performance: PerformanceIngress,
) -> (ControllerRequestSender, ControllerRequestReceiver);
```

The public function delegates to a private `request_channel_inner(...,
Option<AdmissionObserver<ControllerRequest>>)` with `None`. The existing Task 1B.3
feature-only collector configuration passes `Some` only through that private
inner function and constructs the sender with `admitted_channel_observed`. The
observer receives `AdmissionStamp` plus `&ControllerRequest`, recovers the
feature-only synthetic sequence/scheduled time from that request, and records the
identical post-reservation/pre-send instant; do not move it before
`try_send`/`send` or after the receiver dequeues. Add a migration test comparing
the observer stamp fields with the received feature-only
`Admitted<ControllerRequest>::workload_stamp()` tuple and
the Task 1B.3 raw-baseline semantics. That end-to-end fixture starts its virtual
clock at a large nonzero absolute origin and proves admission, priming, workload,
terminal/publication, and render timestamps retain that origin and validate in
one domain; substituting a process-relative zero-origin admission stamp must be
`InvalidArtifact`. The real workload entrypoint selects
`AbsoluteMonotonicPerformanceClock`; the normal production constructor remains
`SystemPerformanceClock`. No crate-private observer type appears in a
public signature.

Update `ControllerRuntimeEvent::Request` to carry an admitted request. At the
start of `service_request`, split `(request, admission)`. Call
`admission.complete()` immediately after every terminal reducer decision:
duplicate, decode rejection, reducer validation rejection, persistence permit
retry, commit-staged retry, or successful `commit_staged`. On the accepted path,
complete before awaiting durable acknowledgement because the model has already
been applied. RAII drop remains the panic/cancellation safety net.

- [ ] **Step 4: Track Herdr and provider queues and publish snapshots**

At the start of this step, after Step 3's admission surface exists, extend
`replay_drains_admitted_events_before_closed_end` to observe the matching
completed high-water and zero pending count through
`CollectorHandle.performance`. This later red must not replace or weaken the
compile-clean Step 2 replay proof.

At collector startup, create one system-clock performance ingress/sampler and a
watch channel initialized from one default snapshot. The default build literal
contains only `snapshot` and `effective_quality`; the feature build adds
`workload_sample_stamp: None`. That `None` is a sentinel requiring the first real
feature sample to publish even when its render payload equals the default. Use an
admitting channel in `spawn_event_reader`; `ReceivedEvent` is received as `Admitted`, and
its admission completes after normalization reaches a terminal reducer outcome.
Remove `ReceivedEvent`'s current `Clone` derive rather than making `Admission`
cloneable. In the actual `replay_generation` function, change `buffered` and the
`drain_events` helper parameter from `Vec<ReceivedEvent>` to
`VecDeque<Admitted<ReceivedEvent>>`. Pop one admission-bearing entry, split it
into `(received, admission)`, borrow `received` for `record_replay_facts`, then
keep the `Admission` alive until the moved `ReceivedEvent` reaches a terminal normalization/reducer outcome;
explicitly complete there, with RAII drop covering panic/cancellation. Maintain
a separate monotonically increasing logical index so closure/candidate positions
keep that index after the event moves into `apply_received_event`. The
receive-after-quiet path uses `push_back`. No admission or event payload is
duplicated or detached from terminal processing.

Replace the current disconnect-as-error early return with a closed-state drain.
`drain_events` pushes every available value and returns an explicit
`EventChannelState::{Open, Closed}`; `Closed` is recorded only after all values
that preceded the close have been appended. `replay_generation` continues
processing the `VecDeque` while nonempty and returns `ReplayOutcome::Ended` only
when the channel state is `Closed` and the buffer is empty. A `recv()` result of
`None` after a quiet open state performs the same closed-and-drain transition.
Normal close must never rely on `Admission::drop`: every buffered admission is
explicitly completed after its terminal reducer outcome, while RAII remains only
the panic/cancellation safety net.

In `src/provider/mod.rs`, create
`spawn_provider_thread_with_diagnostics_and_performance` by wrapping Task 5's
tracked sender path, and create its first caller when the collector creates the
Tokio provider queue. Remove Task 5's two temporary `dead_code` allowances in
this same change. `ProviderIntegration` receives `ProviderIngressEvent`. It completes
`Some(admission)` after the reducer-bound event reaches a terminal normalization
and reducer outcome. `SourceState` and `Malformed` must carry `None`; assert and
handle them only as coverage/log controls so they cannot inflate rate or lag.

Keep CoverageTracker's quality output as a private `source_quality` watch. Spawn
one performance monitor task at 50-ms cadence with clones of the model, operator,
source-quality, and cancellation receivers. In the feature build, install the
raw observer before starting this task. Each feature-build tick calls:

```rust
let snapshot = sampler.sample(&model.borrow(), &operator.borrow(), unix_now_ms());
let sampled_at_ns = match u64::try_from(sampler.workload_sampled_at().as_nanos()) {
    Ok(value) => value,
    Err(_) => { record_monitor_control_failure(); break; }
};
let source_quality = *source_quality.borrow();
let next_quality = compose_quality(source_quality, &snapshot);
let workload_sample_stamp = WorkloadSampleStamp {
    sample_ordinal: next_sample_ordinal(),
    sampled_at_ns,
};
let publication = PerformancePublication {
    snapshot,
    effective_quality: next_quality,
    workload_sample_stamp: Some(workload_sample_stamp),
};
(performance_observer)(&WorkloadPerformanceSample {
    source_quality,
    publication: publication.clone(),
});
performance_sender.send_if_modified(|current| {
    if current.workload_sample_stamp.is_none()
        || !same_render_payload(current, &publication)
    {
        *current = publication.clone();
        true
    } else {
        false
    }
});
quality_sender.send_if_modified(|current| {
    if *current == next_quality { false } else { *current = next_quality; true }
});
```

The observer call and the single `performance_sender` update share one immutable
publication value; the producer assigns the sample ordinal before both. The
observer records every sample even when `send_if_modified` later coalesces an
equal render payload. On that false branch the closure performs no assignment or
field mutation at all: Tokio does not roll back a mutation when the closure
returns `false`, so silently changing only the stamp would make the watch value
diverge from its version and from `App`'s cache. A changed payload atomically
replaces the entire stamped publication and notifies once. The single
`performance_sender` update is the authoritative generation boundary; the
subsequent `quality_sender` update is only a compatibility projection and must
never be paired independently with performance evidence. The default-build tick
uses the same payload-only coalescing as today and has no stamp, observer, or
force-first branch. Also react immediately to source-quality watch changes.
`compose_quality`
preserves `Disconnected`, then `Reconciling`, then existing `Degraded`; it turns
otherwise `Live` into `Degraded` only when performance reasons are nonempty.
Join the monitor during `CollectorHandle::stop`; do not leave a detached task.

- [ ] **Step 5: Prove exact boundaries, recovery, and no loss**

Unit tests use `TestPerformanceClock` as the sole virtual performance-time
authority while ordinary Tokio time drives the monitor cadence. Do not use
`tokio::time::pause`, `tokio::time::advance`, `start_paused`, or add Tokio's
`test-util` feature; Task 6 does not edit `Cargo.toml`.
Implement the exact feature-only public collector wrapper declared by this task
and pass Task 4's feature-visible public `TestPerformanceClock` into it. The
integration workload uses that wrapper because integration tests do not compile
the library with bare `cfg(test)`; it does not sleep for 60 real seconds. Assert:

```rust
assert!(at_target.performance.borrow().snapshot.reasons.is_empty());
assert_eq!(at_target.performance.borrow().effective_quality, ObservationQuality::Live);
assert!(overload.performance.borrow().snapshot.reasons.contains(
    &PerformanceDegradationReason::EventsSixtySeconds
));
assert_eq!(overload.performance.borrow().effective_quality, ObservationQuality::Degraded);
assert_eq!(*overload.quality.borrow(), ObservationQuality::Degraded); // compatibility only
assert_eq!(expected_run_ids, actual_run_ids);
assert_eq!(expected_dependency_edges, actual_dependency_edges);
```

Advance past each rolling window and complete the latched lag generation; assert
quality returns to `Live` only when no source degradation and no other
performance reason remain.

- [ ] **Step 6: Run focused and complete verification**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test convergence -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked herdr::controller::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked herdr::collector::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test controller -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
rg_executable="${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}"
set +e
"$rg_executable" --no-config -n \
  '#\[allow\(dead_code\)\]|removed by Task 6 when the tracked production caller lands' \
  src/provider/mod.rs
rg_status=$?
set -e
case "$rg_status" in
  0) echo 'temporary Task 5 dead-code allowance remains' >&2; exit 1 ;;
  1) ;;
  *) echo "rg operational failure: $rg_status" >&2; exit "$rg_status" ;;
esac
```

Expected: all pass; no production timeout or persisted telemetry is introduced.
Also require that neither an actual `#[allow(dead_code)]` attribute nor the Task
5 removal marker remains in `src/provider/mod.rs`, proving both temporary
allowances were removed even if their comments were edited.

- [ ] **Step 7: Controller review and commit**

After both reviews and independent verification, confirm the actual file set is
within the six declared paths and commit:

```bash
git add src/provider/mod.rs src/herdr/controller.rs src/herdr/collector.rs tests/controller.rs \
  tests/convergence.rs tests/workload_harness.rs
git commit -m "feat(perf): publish collector lag and load health" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```


---

### Task 7: TUI lag and performance-reason wiring

**Files:**

- Modify: `src/main.rs`
- Modify: `src/tui/app.rs`
- Modify: `src/tui/view.rs`
- Modify: `tests/coverage_harness.rs`
- Modify: `tests/workload_harness.rs`

**Interfaces:**

- Consumes: Task 6's coherent `CollectorHandle.performance` publication.
- Produces: live event-lag header input and a stable performance-reason summary;
  no static `Duration::ZERO` production wiring and no renderer-side join of
  independent performance/quality generations remains. Under
  `workload-harness`, the same real render path assembles the final-only anchored
  `PerformanceEvidenceStreamV1` from Task 6 samples, Task 1B.2 draw ordinals, and
  Task 1B.3 terminal callbacks.

- [ ] **Step 1: Write failing dynamic lag and reason-render tests**

In `src/tui/app.rs`, add a test that changes only the performance watch and
asserts `refresh_if_changed()` returns true without rebuilding the row
projection. In `src/tui/view.rs`, add:

```rust
#[test]
fn wide_header_renders_live_lag_and_stable_performance_reasons() {
    let snapshot = performance_snapshot(
        Duration::from_millis(1_234),
        [PerformanceDegradationReason::EventsSixtySeconds,
         PerformanceDegradationReason::DependencyEdges],
    );
    let line = rendered_header(snapshot, 140);
    assert!(line.contains("lag:1234ms"));
    assert!(line.contains("perf:dependency_edges+events_60s"));
}
```

Define `performance_snapshot(lag, reasons)` by collecting the supplied closed
enum values into `PerformanceSnapshot::reasons` and setting `event_lag`. Define
`rendered_header(snapshot, width)` by creating default model/source/diagnostic
watches and one performance watch containing `PerformancePublication { snapshot,
effective_quality: ObservationQuality::Degraded,
#[cfg(feature = "workload-harness")] workload_sample_stamp: None }`, rendering one frame to a
`TestBackend(width, 18)`, collecting buffer
rows with the existing width-aware helper, and returning the unique row that
contains `session:` as a `String`. This follows the existing
`render`/`header` helpers in `src/tui/view.rs`; it neither triggers the
minimum-height fallback nor mistakes the bordered title row for the header.
These are test-only helpers; they do not bypass `App` or the real header renderer.

Update `tests/coverage_harness.rs` to send a changed coherent publication and
require the rendered text to change without losing its allowlist guarantees.
Every Task 7 `PerformancePublication` literal in `src/main.rs`,
`src/tui/app.rs`, `src/tui/view.rs`, and `tests/coverage_harness.rs` likewise includes
`#[cfg(feature = "workload-harness")] workload_sample_stamp: None`, so the same
source compiles under the all-features gate without changing default-build
behavior.
Also add
`workload_header_projection_seam_is_default_build_source_clean`. It reads
`src/tui/view.rs` from `env!("CARGO_MANIFEST_DIR")` and accepts either no
projection marker pair before the implementation lands or exactly one ordered
pair after it lands:

```text
// increment5-workload-header-projection-begin
// increment5-workload-header-projection-end
```

When present, the region must contain exactly one
`#[cfg(feature = "workload-harness")]` gating a documented
`workload_header_projection` module. That module contains the
`WorkloadHeaderProjection` type, the `render_with_workload_projection` helper,
and the explicit `omit_performance_label` input; none is re-exported outside the
gated module. Strip that one region and
require zero remaining occurrences of those three identifiers. In the stripped
default-render source, require exactly one `pub(super) fn render(` declaration;
read through its opening `{` and reject any workload projection or omission
argument. A missing marker, duplicate/reversed/nested marker, forbidden token
outside the region, feature helper without the exact cfg, or injected ordinary
render parameter fails. This source guard is intentionally green before and
after the TDD change; the feature integration tests are the behavioral red.

In `tests/workload_harness.rs`, add feature-enabled integration tests
`supported_load_records_complete_live_performance_stream`,
`supported_load_truthful_degradation_is_a_valid_measured_failure`,
`twice_target_renders_real_collector_performance_reason`, and
`twice_target_deadline_without_required_rendered_reason_is_valid_measured_failure`,
plus the focused C1 regressions
`workload_frame_uses_exact_app_cached_performance_publication` and
`workload_performance_stream_retains_pre_origin_carry_in_and_contiguous_suffix`.
Each drives the real collector and bounded queues with `TestPerformanceClock`,
uses Task 6's pre-publication generation observer, Task 1B.2's draw ordinal, and
Task 1B.3's all-sequence terminal timestamps, passes the same coherent
`CollectorHandle.performance` receiver to `HeaderInputs`, and renders the exact
`160x48` surface. The stream begins at workload origin and closes only at the
first ordinary production draw after the schedule interval and all admitted
terminal outcomes. The independently recorded sample/draw bounds and complete
vectors must validate without gaps.

The supported-load positive test covers both Sustained and Burst and requires
every re-derived sample/frame to remain `Live` with no performance reason. The
failure-injection sibling uses a truthful admission pattern that passes the
aligned admission-bucket predicate but causes a rolling-window reason; the real
composer/validator must return valid `Failed(SupportedLoadDegradation)` and the
classifier must route it `NonD4`. It may not fabricate a reason, omit the sample,
or replace a concurrently observed section 15 miss.

The twice-target positive test selects the lowest qualifying draw ordinal after
admission 1,201 that does not precede its joined sample and is no later than the
60-second deadline. It includes a valid production-limiter overshoot case whose
sample-to-render duration exceeds 100 ms, proving that duration is diagnostic
rather than an invented completion bound; `EventLag` may coexist only when the
sample, frame, and full header label agree. The missing-rendered-reason
failure-injection keeps the real
collector publication and all coherent stream samples, but uses a feature-only
header-projection seam immediately inside the real render path to omit only the
required performance label. The seam neither changes the publication received by
`App` nor the frame/sample evidence. The raw header must therefore prove absence
while the reason-bearing samples remain independently rederivable. The trial must
still record a complete contiguous stream through a closing draw strictly beyond
the deadline, leave `selected_terminal_draw_ordinal` absent, and produce valid
`Failed(MissingDegradation)`. Deleting any interior/suffix
entry, shifting an anchor/watermark, introducing an ordinal gap, fabricating a
terminal draw, selecting a later match, or stopping at/before the deadline is
`InvalidArtifact`. The production collector has no suppression branch.
The projection seam is `#[cfg(feature = "workload-harness")]`, selected only by
an explicit test-only argument, covered by a default-build source guard, and has
no branch in the ordinary `view::render` call; it exists solely to make the valid
measured-failure path TDD-reachable.

The cached-publication regression first performs the normal App refresh, then
publishes a newer changed watch generation before draw without refreshing App.
The next real frame and its raw header must use the older complete cached
publication, and its evidence must name that older stamp. One subsequent normal
`refresh_if_changed`/`refresh_cached` cycle must adopt the newer complete
publication for the next frame. A direct receiver borrow in the draw path would
fail this test. The carry-in regression installs the raw observer before monitor
startup, waits until the first `Some(WorkloadSampleStamp)` publication is cached
by App, primes one real frame, and only then records `workload_origin_ns`.
It requires `first_sample_ordinal` to equal that carry-in stamp, permits its
producer timestamp to precede the origin, and requires every raw sample through
the recorder-lock closing watermark with exact contiguous ordinals. It also
creates trailing equal samples after the final frame; they remain in the closed
raw suffix even though no frame references them.

- [ ] **Step 2: Run focused tests and verify the red state**

Run:

```bash
rg_executable="${HERDR_INCREMENT5_FROZEN_RG_EXECUTABLE:?set to the revalidated canonical /usr/bin/rg identity}"
test "$("$rg_executable" --no-config -cF 'fn workload_header_projection_seam_is_default_build_source_clean()' tests/coverage_harness.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn wide_header_renders_live_lag_and_stable_performance_reasons()' src/tui/view.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn workload_frame_uses_exact_app_cached_performance_publication()' tests/workload_harness.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn workload_performance_stream_retains_pre_origin_carry_in_and_contiguous_suffix()' tests/workload_harness.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn supported_load_records_complete_live_performance_stream()' tests/workload_harness.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn supported_load_truthful_degradation_is_a_valid_measured_failure()' tests/workload_harness.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn twice_target_renders_real_collector_performance_reason()' tests/workload_harness.rs)" = 1
test "$("$rg_executable" --no-config -cF 'fn twice_target_deadline_without_required_rendered_reason_is_valid_measured_failure()' tests/workload_harness.rs)" = 1
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test coverage_harness workload_header_projection_seam_is_default_build_source_clean -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked tui::view::tests::wide_header_renders_live_lag_and_stable_performance_reasons -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test coverage_harness -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness workload_frame_uses_exact_app_cached_performance_publication -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness workload_performance_stream_retains_pre_origin_carry_in_and_contiguous_suffix -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness supported_load_records_complete_live_performance_stream -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness supported_load_truthful_degradation_is_a_valid_measured_failure -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness twice_target_renders_real_collector_performance_reason -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness twice_target_deadline_without_required_rendered_reason_is_valid_measured_failure -- --exact --nocapture
```

Expected: the first source-guard command passes with exactly one selected test
and zero markers before implementation. The remaining focused commands fail by
the task-declared compile boundary or assertion because `HeaderInputs` still
owns a static duration, the stamped publication/cache contract does not exist,
and the app does not observe performance changes. Apply the global declaration
count and exact missing-symbol diagnostic requirements to every compile-red
command; do not treat the intentionally green source guard as a missing red.

- [ ] **Step 3: Replace static header lag with a watch receiver**

Change `HeaderInputs` to:

```rust
pub struct HeaderInputs {
    pub host: String,
    pub session: String,
    pub source_coverage: watch::Receiver<SourceCoverageRegistry>,
    pub performance: watch::Receiver<PerformancePublication>,
}
```

Remove the independent quality receiver/cache from `App::new`,
`App::with_inputs`, and the authoritative render path. Cache one
`PerformancePublication` in `App`. In `refresh_if_changed`, include
`performance.has_changed()`; in `refresh_cached`, perform exactly one
`performance.borrow_and_update()`, clone the complete publication, and replace
the cache atomically. Update neither member from an independent watch read, and
do not rebuild row projection when performance is the only changed input. Pass
the cached snapshot and its paired `effective_quality` to the ordinary
`view::render`.
Add `app_never_joins_performance_and_quality_from_different_generations`, which
publishes alternating `Live`/empty and `Degraded`/reason-bearing values and
asserts every rendered frame is one of those two complete pairs.

Extend `WorkloadFrameDriver` without changing its production limiter schedule.
After the limiter admits an ordinary draw, clone `App`'s cached complete
publication immediately before invoking the real `App::render`; after a
successful draw, record frame evidence from that same clone and the actual
render timestamp. The driver must not borrow the performance receiver directly.
Before the priming frame, the Final workload waits for the first stamped
publication to pass through the ordinary App refresh and asserts the cached
stamp is `Some`; that exact ordinal becomes the raw carry-in anchor. The priming
draw remains outside the recorded draw interval. Closing the stream takes the
raw-recorder lock after the successful closing frame, freezes its next-sample
watermark and complete raw suffix, then freezes the next-draw watermark.

Inside the exact marker region declared in Step 1, add one feature-gated,
documented `workload_header_projection` module containing
`WorkloadHeaderProjection` and the `render_with_workload_projection` wrapper.
The wrapper may clone the cached publication and omit only its rendered
performance label before delegating to the ordinary `view::render`; it may not
change App's cache, the observer stream, frame evidence, or the ordinary render
signature.
No `cfg`, omission flag, workload projection argument, or selection branch is
added to `view::render` itself.

In `main.rs`, pass `collector.performance.clone()` into `HeaderInputs` and remove
only the production `event_lag: Duration::ZERO` field construction. Retain the
`Duration` import because `OWNER_STARTING_DELAY` still uses it. Test defaults
create a watch channel containing a `PerformancePublication` whose snapshot is
default and whose effective quality is the desired test state.

- [ ] **Step 4: Render stable, closed performance labels**

Map every reason exhaustively:

```rust
fn performance_reason_label(reason: PerformanceDegradationReason) -> &'static str {
    match reason {
        PerformanceDegradationReason::LivePanes => "panes",
        PerformanceDegradationReason::DefaultVisibleTaskRuns => "visible_runs",
        PerformanceDegradationReason::DependencyEdges => "dependency_edges",
        PerformanceDegradationReason::EventsOneSecond => "events_1s",
        PerformanceDegradationReason::EventsTenSeconds => "events_10s",
        PerformanceDegradationReason::EventsSixtySeconds => "events_60s",
        PerformanceDegradationReason::EventLag => "event_lag",
    }
}
```

Add `performance_reason_labels_match_workload_schema_v1` inside
`src/tui/view.rs`'s existing unit-test module so it can call the private
`performance_reason_label`. It locates Task 1A.1's closed fixture from
`env!("CARGO_MANIFEST_DIR")`, enumerates all seven enum variants exactly once,
and asserts this function's pair list is byte-for-byte equal to the fixture's
ordered reason-to-label table. Missing/extra/reordered mappings fail the test;
neither the renderer nor validator owns a second unfrozen table.

At width 88 or greater, render `lag:<ms>`. When reasons are nonempty, render a
`perf:` field joined in enum/BTreeSet order. In the shrink-priority array, shrink
`sources:` first and `perf:` last; the quality indicator remains non-shrinkable.
Thus a wide or moderately constrained terminal keeps the active degradation
reason. Preserve the quality indicator at every width. Never include raw event
values or dynamic diagnostic strings.

- [ ] **Step 5: Run TUI, privacy, and full verification**

Run:

```bash
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test coverage_harness workload_header_projection_seam_is_default_build_source_clean -- --exact --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked tui::app::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked tui::view::tests -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --test coverage_harness -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --features workload-harness --test workload_harness -- --nocapture
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
```

Expected: dynamic lag/reasons render, performance-only refresh avoids projection
rebuild, narrow/wide Unicode tests pass, sentinel/private content remains absent,
and all existing behavior is preserved.

- [ ] **Step 6: Controller review and commit**

After reviews and verification, commit only the five declared paths:

```bash
git add src/main.rs src/tui/app.rs src/tui/view.rs tests/coverage_harness.rs \
  tests/workload_harness.rs
git commit -m "feat(tui): display measured performance health" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
```


---

### Task 8: Authoritative post-change measurement and D4 checkpoint

**Files:**

- Repository mutation: none
- External artifacts only:
  `${RESEARCH_HOME:-$HOME/.research}/mageyuki--herdr-top/increment-5-reliability-performance/measurements/final-<subject12>-attempt-<eight-digit-ID>/`
- Update external research ledgers:
  `manifest.json`, `state.md`, `findings.md`, and `changes.md` in the Increment 5
  research workspace.

**Interfaces:**

- Consumes: clean integrated HEAD, Task 1B.3 baseline JSON, and the unchanged
  versioned harness schema.
- Produces: validated post-D1/D2 and final reference results, baseline deltas,
  section 15 verdicts, and either `NoMissD4NotAuthorized` or a nonempty
  `AmendmentsRequired` set that independently preserves `D4` and `NonD4` needs.

- [ ] **Step 1: Verify clean identity and full deterministic suite**

From a dedicated measurement linked worktree at the integrated HEAD, first bind
`HERDR_INCREMENT5_FROZEN_GIT_EXECUTABLE` to the revalidated canonical Git entry
from the selected baseline controls and revalidate its requested/canonical
mapping, executable metadata, and digest. Then run:

```bash
git_executable="${HERDR_INCREMENT5_FROZEN_GIT_EXECUTABLE:?set to the revalidated canonical Git executable}"
"$git_executable" diff --quiet --exit-code
"$git_executable" diff --cached --quiet --exit-code
test "$("$git_executable" rev-parse HEAD)" = "${HERDR_INCREMENT5_FINAL_REVIEWED_HEAD:?set to the exact approved final-review HEAD}"
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo fmt --all -- --check
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo clippy --locked --all-targets --all-features -- -D warnings
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --doc
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo test --locked --all-targets --all-features --no-run
/home/mageyuki/.cargo/bin/rustup run 1.97.1 cargo build --locked --all-targets --all-features
```

Expected: all pass. Do not enumerate or act on untracked files.

- [ ] **Step 2: Run the complete authoritative profile**

Run:

```bash
baseline_results_root="${HERDR_INCREMENT5_SELECTED_BASELINE_ROOT:?set to the exactly selected valid baseline root from the research manifest}"
attempt_id="${HERDR_INCREMENT5_ATTEMPT_ID:?record a fresh eight-digit attempt ID in the research ledger first}"
export HERDR_INCREMENT5_ATTEMPT_ID="$attempt_id"
git_executable="${HERDR_INCREMENT5_FROZEN_GIT_EXECUTABLE:?set to the revalidated canonical Git executable}"
final_results_root="${RESEARCH_HOME:-$HOME/.research}/mageyuki--herdr-top/increment-5-reliability-performance/measurements/final-$("$git_executable" rev-parse --short=12 HEAD)-attempt-$attempt_id"
runner_script="${HERDR_INCREMENT5_FROZEN_RUNNER_SCRIPT:?set to the revalidated frozen absolute runner script}"
controller_requested="${HERDR_INCREMENT5_CONTROLLER_LAUNCHER_REQUESTED:?set to the frozen native Controller requested path}"
controller_canonical="${HERDR_INCREMENT5_CONTROLLER_LAUNCHER_CANONICAL:?set to its revalidated canonical path}"
controller_sha256="${HERDR_INCREMENT5_CONTROLLER_LAUNCHER_SHA256:?set to its revalidated digest}"
runner_argv=( -p "$runner_script" \
  --subject "$("$git_executable" rev-parse HEAD)" \
  --stage final \
  --scenario all \
  --baseline-results-root "$baseline_results_root" \
  --output-dir "$final_results_root" )
```

The trusted parent passes frozen canonical Bash plus this argv through the same
Task 1A.2a `builtin exec -c` native-Controller bootstrap; that Controller applies
`env_clear()` before the allowlist. No newly loaded inherited-environment shell
establishes the boundary.
Expected: the runner emits a schema-valid `pass`/`failed` result for each scenario
until completion, or atomically emits an `invalid` envelope and exits `20` at the
first invalid scenario. `invalid` is never interpreted as a pass and blocks the
D4 decision until measurement is corrected. Every result must record
`measurement_stage: final`; the twice-target result cannot validate without the
complete real performance evidence stream and its earliest-match/closed-absence
decision.

- [ ] **Step 3: Re-derive all section 15 predicates and baseline deltas**

Bind `jq_executable` from the revalidated canonical jq entry carried by every
selected result and use only that path—never a bare `jq` lookup—against validated
documents to report, per trial and scenario:

```text
screen p95 < 1_000_000_000 ns
input p95 < 100_000_000 ns
startup < 3_000_000_000 ns
idle CPU (sum of checked per-identity idle-window tick deltas / measured idle-window elapsed * 100) < 2.0
idle maximum simultaneous process-tree RSS < 100_000_000 bytes
startup whole-wrapper RSS recorded as diagnostic, never a Startup failure reason
paired fallback rescan delay - notify delay <= 2_000_000_000 ns
every aligned one-second admission bucket reaches its exact 20/100/40 count by
bucket-end plus one cadence period, with no admission before its scheduled_ns
submitted == admitted == completed == persisted == 1..=N where applicable
rendered == the exact frozen screen-probe vector where applicable
for sustained, burst, and twice-target, reducer lag
  (terminal_ns - admitted_ns) and publish-to-render
  (rendered_ns - published_ns) distributions exactly re-derive from every raw
  screen observation with checked subtraction and counts 300/50/300
final run and edge identity sets == oracle
for final sustained and burst, the anchored performance evidence stream has exact
contiguous sample/draw ordinal coverage from workload origin through the first
ordinary closing draw after schedule completion and all terminal outcomes;
every admission has one terminal timestamp; every sample and frame re-derives
coherently; and PerformanceDegradation/Count == 0, otherwise the valid failure is
SupportedLoadDegradation (NonD4)
twice-target retains the earliest actual production frame after attained
admission 1,201 whose complete sampled state is `Degraded`, whose ordered reason
vector contains `EventsSixtySeconds`, and whose exactly matching rendered `perf:`
field contains `events_60s`; the frame does not precede its actual state sample
and is no later than the checked 60-second deadline, while its checked
sample-to-render duration remains diagnostic and the complete
sample/frame stream closes strictly and all 2,400 events complete losslessly
```

Perform that re-derivation with the already-frozen measured binary, never with
shell/jq arithmetic or Cargo. The trusted parent uses the Task 1A.2a
`builtin exec -c` native-Controller bootstrap to execute the following exact
child contract: program = the revalidated absolute
`HERDR_INCREMENT5_FROZEN_MEASURED_BINARY`; `env_clear()` first; then add exactly
`HOME=/home/mageyuki`, `RUSTUP_HOME=/home/mageyuki/.rustup`,
`CARGO_HOME=/home/mageyuki/.cargo`, `PATH=/usr/bin:/bin`, `LC_ALL=C`, `TZ=UTC`,
`HERDR_PERF_REDERIVE_BASELINE_RESULTS_ROOT=<the explicit selected baseline
root>`, `HERDR_PERF_REDERIVE_FINAL_RESULTS_ROOT=<the explicit selected final
root>`, and
`HERDR_PERF_REDERIVE_OUTPUT=<the explicit selected final
root>/section15-rederivation-v1.json`; argv =
`rederive_section15_report_from_results --exact --ignored --nocapture
--test-threads=1`. Require exit zero, an atomically written valid
`Section15ReDerivationV1`, and byte-equal selected-result identities before using
the report. The native Controller's self/program identities are revalidated
immediately before the bootstrap; no external `env`, shell, or Cargo process is
between the empty native-loader boundary and this child.

The frozen canonical jq executable is limited to structural projection after the
typed report validates. Its exact reporting command is:

```bash
"$jq_executable" --exit-status --raw-output \
  '[.schema_version, .subject_sha, .baseline_id, .decision.kind] | @tsv' \
  "$final_results_root/section15-rederivation-v1.json"
```

Any jq nonzero status is an operational reporting failure. jq never sums,
subtracts, calculates percentiles, compares thresholds, or selects D4.

Record raw values, every binding threshold comparison, both diagnostic latency
distributions and their baseline deltas, host/control identity, measured binary
path/SHA-256, result SHA-256, and command in the research ledger. Reducer lag and
publish-to-render have no pass/fail threshold in v1 and may not create a failure
reason. Do not copy private raw
diagnostics to GitHub or repository docs.

- [ ] **Step 4: Partition every validated failure, then apply the independent D4 gates**

First require all seven independently validated outcomes to be `Pass` or
`Failed`; any `Invalid`, including sequence or structural loss, blocks the
checkpoint and never enters D4 classification. Collect the complete
failure-reason set from the valid runs. Only an empty set may close the checkpoint
as `NoMissD4NotAuthorized`. For every nonempty reason, look up the exact
`(measurement_stage, scenario, failure_reason)` row in the manifest's closed
failure-policy table. An absent row is invalid evidence. A `NonD4` row adds
`NonD4` without a denominator; a `D4Scoped` row requires its exact scoped
samples. The prose examples remain: Idle CPU/RSS and input are `NonD4`; Startup
latency and fallback latency are D4-scoped; Sustained/Burst
`WorkloadAdmission` is D4-scoped; TwiceTarget `WorkloadAdmission` and
`MissingDegradation`, and Sustained/Burst `SupportedLoadDegradation`, are
`NonD4`. Neither validator nor classifier reimplements
those routes outside the shared table. For each failing D4-scoped reducer
workload, match raw scoped
observations by sequence and compute:

```text
D4 ratio = sum(d4_analysis_nanoseconds) /
           sum(reducer_plus_publish_nanoseconds)
```

Re-derive both sums from raw per-event samples using `u128` accumulators; reject
a missing/duplicate sequence, unmatched trial, aggregate mismatch, zero
denominator, or checked-arithmetic overflow as invalid evidence. Compare the
exact integer predicate using checked `u128` multiplication:
`4 * sum(d4_analysis_nanoseconds) >= sum(reducer_plus_publish_nanoseconds)`;
the parts-per-million field is report-only and cannot decide the gate.
For each D4-scoped miss, a ratio below 0.25 adds `NonD4` because D4 cannot explain
enough of that missed target; a ratio at or above 0.25 adds `D4`. Process every
miss, so a run can require both amendments. The decision is:

```text
if zero validated failure reasons across all seven outcomes:
    NoMissD4NotAuthorized
else:
    required = {}
    for each non-D4-scoped miss:
        required += NonD4
    for each D4-scoped miss:
        required += D4 if ratio >= 0.25 else NonD4
    AmendmentsRequired { amendments: strictly_sorted(required) }
    // wire Vec is nonempty and duplicate-free before semantic set conversion
```

Expected lag only in the twice-target overload is not a target miss when the run
is visibly degraded and lossless.

- [ ] **Step 5: Stop or close the checkpoint**

First require every independently validated final document to carry byte-equal
`controls.measured_binary`, `controls.toolchain_launcher`,
`controls.toolchain_name`, `controls.rustc_version`, `controls.cargo_version`,
`controls.build_environment`, `controls.cargo_configuration`,
`controls.runner_script`, and
`controls.authoritative_executables`, plus the same authoritative controlled
environment template: every invariant key/value is equal, the sole
per-scenario `HERDR_PERF_SCENARIO` value matches that document's closed scenario,
and no other difference is accepted. Revalidate the requested-to-canonical mapping, regular executable
target, executable bit, and SHA-256 for the selected measured binary and every
authoritative executable. The measured binary requested path may be a symlink
only under the unified policy; equality includes both requested and canonical
paths. Require `controls.toolchain_launcher` to equal exactly one rustup inventory
entry, and require the separate native Controller identity to occur exactly once
in `controls.authoritative_executables`.

Freeze the exact final-review `HEAD` and the two tracked/index-clean predicates
immediately before classification. Invoke the Task 1A.1 pure classifier as the
direct child of the revalidated Task 1A.2a native Controller, using the same
recorded measured test binary—never through Cargo, rustup, a rebuild, `PATH`, a
new shell, or a newly inferred environment. Writing remains confined to the
research workspace:

```bash
final_results_root="${HERDR_INCREMENT5_SELECTED_FINAL_ROOT:?set to the exactly selected valid final root from the research manifest}"
measured_binary="${HERDR_INCREMENT5_FROZEN_MEASURED_BINARY:?set to the revalidated canonical binary recorded by every final document}"
git_executable="${HERDR_INCREMENT5_FROZEN_GIT_EXECUTABLE:?set to the revalidated canonical Git recorded by every final document}"
test "$("$git_executable" rev-parse HEAD)" = "${HERDR_INCREMENT5_FINAL_REVIEWED_HEAD:?exact reviewed HEAD required}"
"$git_executable" diff --quiet --exit-code
"$git_executable" diff --cached --quiet --exit-code
classifier_environment=(
  HOME=/home/mageyuki RUSTUP_HOME=/home/mageyuki/.rustup
  CARGO_HOME=/home/mageyuki/.cargo PATH=/usr/bin:/bin LC_ALL=C TZ=UTC
  HERDR_PERF_CLASSIFY_RESULTS_ROOT="$final_results_root" \
  HERDR_PERF_CLASSIFY_OUTPUT="$final_results_root/d4-checkpoint-v1.json"
)
classifier_argv=(classify_d4_checkpoint_from_results --exact --ignored \
  --nocapture --test-threads=1)
test "$("$git_executable" rev-parse HEAD)" = "$HERDR_INCREMENT5_FINAL_REVIEWED_HEAD"
"$git_executable" diff --quiet --exit-code
"$git_executable" diff --cached --quiet --exit-code
```

The trusted parent uses the exact `builtin exec -c` bootstrap with the frozen
native Controller self identity, `measured_binary` as the verified child program,
`classifier_environment` as its complete `--env` map, and `classifier_argv` after
`--`. The native Controller calls `env_clear()` before adding that map; neither a
new inherited-environment shell nor `env -i` establishes the boundary. Re-run
the three HEAD/clean predicates in a new trusted Controller invocation
immediately after that child exits.

`final_results_root` is the explicit Task 8 output directory resolved before the
command; it is never inferred by scanning. Deserialize and validate the emitted
`D4CheckpointDocumentV1`, require `schema_version == 1`, revalidate the same
HEAD/cleanliness/binary/environment/control identities once more before atomic
selection of the checkpoint, and require its decision to match the Controller's
manual raw re-derivation. The classifier is a subordinate consistency check; the
manual raw decision is authoritative, and any command, control, identity, or
decision mismatch blocks promotion rather than choosing one result. If the
result is `AmendmentsRequired`, stop without source changes, write every decisive
workload and ratio to the research workspace, and request user approval for all
named focused amendments; a mixed result requests both D4 and non-D4 amendments.
Do not add either implementation opportunistically to this plan. If the result is
`NoMissD4NotAuthorized`, record that D4 remains full recomputation and that the
measurement checkpoint is complete. A missed target can never close the
checkpoint as “D4 not authorized” without a non-D4 amendment.


---

## Final integration and review gate

This gate executes **after Tasks 1A-7 are serially integrated and before Task 8
starts**, even though it is summarized after the task list. The Controller runs
the full deterministic commands from Task 8 Step 1 at the integrated HEAD, then
dispatches exactly one final whole-change review over the complete
merge-base-to-HEAD range: actual `claude-opus-5` is authoritative and one fresh
`gpt-5.6-sol` max-effort review is supplemental within the same final review
stage. The prompt identifies all per-task reviewed commits and emphasizes
unreviewed seams and integration boundaries. Review fixes are integrated before
measurement and receive bounded delta review in the same final-review stage; the
approved exact HEAD is recorded as `HERDR_INCREMENT5_FINAL_REVIEWED_HEAD`.

Task 8 may start only at that exact reviewed HEAD. Any tracked repository-byte
change after an authoritative attempt starts permanently invalidates that
attempt, requires the relevant deterministic checks and bounded final-review
delta to approve the new HEAD, and requires a fresh attempt ID, complete
seven-scenario profile, classifier, and baseline deltas. A result measured before
the last relevant change is never selected or reused.

The Controller verifies every final finding against the cited code, tests, docs,
and result artifacts. Findings are resolved before completion is claimed. No push,
PR, merge, or release follows unless the user explicitly requests publication.

## Spec coverage self-review

| Approved design requirement | Plan coverage |
| --- | --- |
| Untouched `9cd9813` baseline before production changes | Task 1A.1 Step 5, Task 1A.2a Steps 2-4, Task 1A.2b, and Task 1B.3 Step 6 |
| Exact target, burst, startup, idle, fallback-rescan, twice-target, no-loss workload | Task 1A.1 Steps 1-5, Task 1A.2a Steps 1-3, Task 1B.3 Steps 1-5, and Task 8 Steps 2-3 |
| Actual admitted schedule is attained rather than delayed by backpressure | Task 1A.1 Steps 3-5, Task 1B.3 Steps 1 and 3-4, and Task 8 Step 3 |
| Screen/input latency includes production 100-ms frame scheduling | Task 1A.1 Steps 3-5, Task 1B.2, and Task 1B.3 Steps 1 and 4 |
| Versioned fail-closed JSON and raw external evidence | Task 1A.1 Steps 3-5, Task 1A.2a Steps 1-4, and Task 1B.3 Steps 4-6 |
| Honest four-core/address-space profile limitation | Global constraints and Task 1A.2a Step 2 |
| Binding CPU observer is outside the measured tree | Task 1A.1 Steps 3-5 and Task 1A.2a Step 2 |
| D1 one non-cloneable collector-owned ledger mutation path | Task 2A, Task 2B.2a Steps 1-5, and Task 2B.2b |
| D2 writer panic closes an issued-permit wait without product timeout | Task 2B.2a Steps 1, 2, 4, and 6 |
| Runtime-only monotonic lag, rolling windows, stable reasons, recovery | Task 4 Steps 1-5 |
| Correct live/default-visible count | Task 3 |
| Admission begins at successful Herdr/provider/Controller queue entry | Tasks 4-6 |
| Source-quality precedence plus performance `DEGRADED` | Task 6 Steps 1 and 4-5 |
| Dynamic TUI lag and complete stable simultaneous-reason rendering | Task 7 |
| Exact supported load remains non-degraded in authoritative real-clock evidence | Task 1A.1 Steps 3-5, Tasks 1B.2-1B.3, Task 6, Task 7 Steps 1-4, and Task 8 Step 3 |
| Reference results after baseline, D1/D2, and final health stage | Task 1B.3 Step 6, Task 2B.2b Step 3 barrier, and Task 8 Step 2 |
| Conditional D4 only at target miss plus ratio at least 25%, including startup/fallback D4 execution | Task 1A.1 Steps 3-5, Task 1B.3 Step 4, and Task 8 Steps 3-5 |
| Dedicated worktrees, TDD, cross-model gates, serial integration | Global constraints, file map, every Controller commit step, final gate |
| No user-owned `mise.toml` access and no publication | Global constraints, Task 1A.2a tracked-only checks, publication preflight |

No approved spec requirement is intentionally deferred. D4 implementation is
not a gap: the approved design explicitly requires a new design decision if its
measurement gate fires.
