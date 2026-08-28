# Codex Child Visibility and Historical Performance Accounting Plan

> **Execution contract:** The existing linked PR #21 worktree is the integration
> target only. Before dispatch, the Controller commits this plan, its
> specification, and the already-reviewed canonical-design amendment as the
> planning baseline. Tasks 1 and 2 then run concurrently in separate linked
> worktrees and branches because their declared file sets are disjoint and
> neither consumes the other's output. Each fresh Codex implementation process
> receives one self-contained task, uses TDD, and may not commit, push, merge,
> rebase, or delegate. The Controller independently verifies each result and
> integrates and commits one task at a time.

**Goal:** Restore current Codex child lineage in the execution tree and prevent
historical startup replay from masquerading as live overload.

**Architecture:** Normalize the current nested `SubAgentActivity` record into
the existing typed activity/evidence paths. Split performance admission into
rated and unrated modes that share sequence, pending, completion, and lag state;
select unrated mode only for historical provider origins.

**Spec:**
`docs/internal/superpowers/2026-08-28-codex-child-history-performance/spec.md`

## Global constraints

- Base implementation is PR #21 commit `bc6fd9e` on
  `agent/stable-task-history-rates`.
- Claude roles are unavailable for this task. The user explicitly authorized
  Codex-only execution on 2026-08-28. Use fresh `gpt-5.6-sol` processes at
  `xhigh` for implementation and separate fresh processes at `max` for task and
  final reviews. This same-model route is a task-local degraded exception, not
  a durable replacement for cross-model review.
- Create Task 1 and Task 2 branches from the committed planning baseline in
  project-local `.worktrees/` paths. Dispatch them concurrently. Integrate
  their verified results serially into `agent/stable-task-history-rates`.
- The planning baseline owns the four-line live-versus-historical amendment in
  `docs/internal/superpowers/specs/2026-08-12-increment-5-reliability-performance-design.md`.
  Task 2 reads that contract but does not modify the document.
- No dependency, schema, public CLI, ordering, or retention change.
- Keep provider parsing allowlist-only and never retain prompt, response,
  command output, or other activity body content.
- Keep live admission accounting before reduction; do not filter rate samples
  by reducer outcome.
- Every missing-behavior test must fail for the named behavior before the
  production edit and pass afterward. Characterization tests for already-safe
  negative/privacy/reservation behavior must pass on the planning baseline and
  again after the production edit; report them separately from RED/GREEN proof.
- Each task receives a separate fresh Codex task review after implementation.
  The Controller verifies the report, changed-file subset, and test evidence
  before integration. After both tasks integrate, run exactly one fresh Codex
  whole-change review of the complete base-to-HEAD diff.
- Use this command prefix for Rust verification:

  ```sh
  setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 --
  ```

## Task 1: Parse current Codex child activity records

**Expected files:**

- Modify: `src/provider/codex.rs`
- Modify: `src/provider/codex_facts.rs`
- Modify: `tests/provider_codex.rs`

**Acceptance criteria:**

- Both the legacy top-level activity shape and current nested item shape emit
  the existing bounded activity semantics.
- `started` and `spawned` both cause a working child upsert.
- The nested child UUID emits `LogFact::EvidenceId` with the record timestamp.
- Unknown item types are ignored; malformed known activities emit the existing
  bounded diagnostic and parsing continues.
- Missing or invalid child UUIDs and unrelated item types emit no lineage
  evidence.
- The new typed nested envelope has its own bounded-debug sentinel proof that
  nonallowlisted item bodies are not materialized.
- Provider events use the nested item ID as their event identity and
  `completed_at_ms` as their event time; lineage evidence uses the outer record
  timestamp.

**Steps:**

1. Add a facts test named
   `nested_subagent_activity_emits_typed_child_evidence`. Use the existing
   internal-subagent fixture for representative current-shape coverage and an
   inline record whose outer timestamp, `started_at_ms`, and `completed_at_ms`
   are deliberately different. Assert that evidence uses only the outer record
   timestamp. Add facts tests named
   `nested_subagent_activity_rejects_missing_invalid_and_unrelated_evidence`
   and
   `nested_subagent_activity_envelope_debug_excludes_unallowlisted_body`.
   Add provider integration tests named
   `nested_subagent_activity_normalizes_started_and_spawned` and
   `malformed_nested_subagent_activity_does_not_stop_later_records`.
   The normalization test must use deliberately distinct time values and assert
   the exact item-ID-derived event identity, `completed_at_ms` event time, and
   both start-kind spellings. It must also assert that an unrelated
   `item_completed` item produces zero provider events.
2. Before any production edit, run the positive evidence, normalization, and
   malformed-known-activity tests and record RED caused by the missing nested
   dispatch. Run the negative evidence and bounded-debug sentinel tests and
   record baseline GREEN characterization evidence.
3. Add minimal typed envelopes for the nested item, normalize it into the
   existing activity validator/emitter, and recognize `started`/`spawned` as
   start kinds. Extend `extract_item_completed` only for the exact
   `SubAgentActivity` item.
4. Run:

   ```sh
   cargo test --locked nested_subagent_activity_emits_typed_child_evidence
   cargo test --locked nested_subagent_activity_rejects_missing_invalid_and_unrelated_evidence
   cargo test --locked nested_subagent_activity_envelope_debug_excludes_unallowlisted_body
   cargo test --locked --test provider_codex nested_subagent_activity_normalizes_started_and_spawned
   cargo test --locked --test provider_codex malformed_nested_subagent_activity_does_not_stop_later_records
   cargo test --locked --test provider_codex
   cargo fmt --all -- --check
   git diff --check
   ```

5. Report RED/GREEN evidence and confirm the actual changed files are a subset
   of the declared set.

## Task 2: Keep historical replay out of live rate windows

**Expected files:**

- Modify: `src/performance.rs`
- Modify: `src/provider/mod.rs`

**Acceptance criteria:**

- An unrated admission allocates a sequence and pending timestamp, contributes
  to event lag, completes explicitly or through RAII drop, and never enters
  rate windows. Both completion paths advance completion high-water state once.
- The tracked provider sender uses unrated admission only for historical
  origins and rated admission for live origins.
- A full or closed provider queue allocates no admission for either live or
  historical origin.
- The canonical performance design distinguishes live rated throughput from
  all-origin pending/lag work.
- Existing sustained-target and twice-target workload tests remain unchanged
  and pass.

**Steps:**

1. Add unit tests named
   `unrated_admission_tracks_lag_without_entering_rate_windows` and
   `historical_provider_delivery_is_unrated_but_live_delivery_is_rated`, plus
   `historical_provider_reservation_failure_allocates_no_admission`. The first
   must cover both explicit completion and RAII drop and assert pending,
   admission high-water, and completion high-water state. The reservation test
   must be parameterized over both historical full and closed queues and assert
   zero admission, lag, and rate-window change.
2. Before any production edit, run
   `unrated_admission_tracks_lag_without_entering_rate_windows` and
   `historical_provider_delivery_is_unrated_but_live_delivery_is_rated`; record
   RED because no unrated admission exists and historical origins use the rated
   path. Run `historical_provider_reservation_failure_allocates_no_admission`
   and record baseline GREEN characterization evidence for both full and closed
   cases.
3. Parameterize the internal admission state with whether it records a rate
   timestamp. Keep `admit()` rated and add a crate-visible unrated entry point.
   Select it in `ProviderEventSender::try_send` from `ObservationOrigin` after
   successful reservation.
4. Verify the planning-baseline canonical performance design still states that
   all-origin pending/lag work is tracked, live throughput alone is rated, and
   every live admitted event counts regardless of reducer outcome. Do not edit
   the document in this task.
5. Run:

   ```sh
   cargo test --locked unrated_admission_tracks_lag_without_entering_rate_windows
   cargo test --locked historical_provider_delivery_is_unrated_but_live_delivery_is_rated
   cargo test --locked historical_provider_reservation_failure_allocates_no_admission
   cargo test --locked performance::tests
   cargo test --locked provider::tests
   cargo test --locked --features workload-harness --test workload_harness sustained_target
   cargo test --locked --features workload-harness --test workload_harness twice_target
   cargo fmt --all -- --check
   git diff --check
   ```

6. Report RED/GREEN evidence and confirm the actual changed files are a subset
   of the declared set.

## Integration and publication

Planning-time publication preflight on 2026-08-28 established:

- fetch and push owner: `mageyuki`;
- authenticated repository permission: `ADMIN`;
- PR #21: open, ready, base `main`, head `agent/stable-task-history-rates`;
- PR CI: nonempty six-check workflow on `main`; latest reviewed head
  `bc6fd9e` was green;
- fork approval policy: `first_time_contributors` (not applicable to this
  same-repository branch);
- PR template: none.

1. Run these exact commands with the global Rust prefix on the integrated
   branch, then run the repository-only diff check:

   ```sh
   cargo test --locked --all-targets --all-features
   cargo test --locked --doc
   cargo fmt --all -- --check
   cargo clippy --locked --all-targets --all-features -- -D warnings
   cargo check --locked --all-targets
   cargo build --release --locked
   git diff --check
   ```

2. Replace `~/.local/bin/herdr-top` with the exact verified
   `target/release/herdr-top` binary.
3. Restart Herdr Top in the adjacent Pane with that exact binary. While a real
   child task is running, verify that it appears under the current root. After
   completion, verify that the same row remains terminal. Restart during an
   unfinished historical drain and verify that history alone does not produce
   a rate-window degradation reason.
4. Freeze one base-to-HEAD diff and obtain one critical whole-change review from
   a fresh Codex process at `max`. Adjudicate every finding against code, tests,
   and this specification. Record that this is same-model degraded assurance.
5. Push the latest branch, wait for a nonempty conclusive CI check set, request
   Copilot review when available, resolve agreed findings, and repeat CI/review
   only for a changed HEAD. Stop with PR #21 ready to merge; do not merge it.
