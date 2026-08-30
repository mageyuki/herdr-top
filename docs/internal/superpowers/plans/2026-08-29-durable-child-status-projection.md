# Durable Child Status Projection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to execute this plan. The Codex
> Controller dispatches the single implementation task to one fresh
> `claude-implementer` in a dedicated linked worktree. The implementer performs
> the task directly with TDD and does not commit, push, merge, rebase, or
> delegate. The Controller verifies and integrates the result serially.

**Goal:** Keep a persisted ended child displayed as `done` after its Agent Node
row becomes display-stale, without changing Task Run lifecycle, visibility,
retention, or rate accounting.

**Architecture:** Resolve each Agent Node's exact `(provider,
native_session_id)` through the Task Run native-alias index, because a real
child Agent Node is parented and remains owned by the controller/root run. Add a
presentation-only newest-exact-binding evidence map to `StatusReadModel` and
use it only to refine semantic or native unknown outcomes when the selected
state is exactly ended. Preserve the existing ownership-based fresh-root map
for ordinary runtime status and `RunRateActivity`, and preserve every tree-row
staleness filter.

**Tech Stack:** Rust 1.97.1, Cargo, Ratatui, and the existing in-memory status
and execution-tree projections.

**Spec:**
`docs/internal/superpowers/2026-08-29-durable-child-status-projection/spec.md`

## Global constraints

- Starting branch and HEAD: `agent/stable-task-history-rates` at
  `0e2c394e50669868120859802dd0e0451b562ba1`.
- This is one coherent implementation task. Its declared implementation file
  set is exactly `src/status.rs`, `src/tui/view.rs`, `docs/tui.md`, and
  `docs/design/herdr-top-mvp.md`.
- Do not modify Task Run state, native lifecycle data, Agent Node data,
  persistence schema, migrations, provider events, controller events,
  visibility duration, retention, duration endpoints, or dependencies.
- Do not alter `agent_node_is_display_stale` or any call site that filters Agent
  rows, live-line fallback, newest visible agents, or visibility deadlines.
- Match durable evidence only through an exact `RunKey::Native { provider,
  sid }` alias. Agent ownership, the Task Run's primary key, and
  `parent_agent_node_id` are not matching criteria.
- Select the durable candidate during the existing Agent Node scan. A single
  preliminary scan of `task_run_bindings()` is allowed; a model-wide scan per
  rendered Task Run is not.
- Select the newest exact-binding candidate by
  `(last_activity_at_ms, agent_node_id)`, then require exact state
  `ExecState::Ended`. Exclude synthetic live-line candidates.
- Keep the existing `root_agents` selection byte-for-byte equivalent in
  behavior. The filtered rate-only projection must neither build nor consult
  the new presentation map.
- Every missing-behavior test must demonstrate RED on the starting HEAD before
  the production edit and GREEN afterward. Existing semantic precedence,
  Agent-row staleness, and rate tests are characterization-GREEN checks, not RED
  evidence.
- Run Rust commands through:

  ```sh
  setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 --
  ```

- The user-authorized Grok plan-review request returned an invalid response and
  provided no review result. Do not retry or attribute findings to Grok. The
  mandatory Claude plan review and final whole-change review remain required.
- Do not merge PR 21.

## File responsibilities

- `src/status.rs`: build the exact native-alias evidence projection, implement
  the two unknown-outcome refinements, and host matching, precedence, and
  rate-isolation tests.
- `src/tui/view.rs`: reproduce the persisted topology and prove that the exact-
  bound child Task Run remains `done` while its parented, other-owned stale
  Agent row is hidden.
- `docs/tui.md`: document user-visible precedence and exact-binding evidence.
- `docs/design/herdr-top-mvp.md`: update the normative precedence contract
  without changing lifecycle semantics.

## Task 1: Refine unknown display outcomes with exact native completion evidence

**Files:**

- Modify: `src/status.rs`
- Modify: `src/tui/view.rs`
- Modify: `docs/tui.md`
- Modify: `docs/design/herdr-top-mvp.md`

**Interfaces:**

- Consumes: `DomainModel::task_run_bindings`, `RunKey::Native`, `TaskState`,
  `NativeSessionEndStatus`, `AgentNode`, `ExecState`, `DisplayStatus`,
  `StatusSource`, and the existing deterministic Agent Node ordering.
- Produces: one private `StatusReadModel` map named `durable_native_agents` and
  one private method named `durable_ended_status`; no public or persisted
  interface.

**Acceptance criteria:**

- Semantic completed, failed, and cancelled statuses remain authoritative.
- Semantic ended-unknown is refined to Agent-Node-sourced done only when the
  newest Agent Node for the Task Run's exact native alias is ended.
- For nonterminal tasks, native done, error, and cancelled remain authoritative;
  native unknown receives the same narrow ended-evidence refinement.
- A foreign provider, different SID, synthetic live-line node, or older ended
  node followed by a newer non-ended exact-binding node cannot refine unknown.
- A parented Agent Node owned by another Task Run can refine the exact-bound
  child Task Run, matching the persisted production topology.
- Agent row staleness, live-line fallback, `RunRateActivity`, visibility,
  retention, and persistence remain unchanged.

- [ ] **Step 1: Add every behavior and tree test before production code**

In `src/status.rs`, add module-local helpers that create:

1. a target Task Run with primary key
   `RunKey::Controller("hook:codex:child-sid")`;
2. its explicit alias `RunKey::Native { provider: Provider::Codex, sid:
   "child-sid" }` via `insert_task_run_alias`; and
3. Agent Nodes whose `task_run_id` is a different controller/root run,
   `parent_agent_node_id` is present, and whose provider and
   `native_session_id` are independently configurable.

Add these missing-behavior tests:

- `ended_unknown_uses_exact_native_ended_agent_across_staleness`: project an
  ended-unknown target with an exact-bound ended Agent Node immediately before
  and exactly at `headless_inactivity_ms()`. At both times require
  `DisplayStatus::new(TaskDisplayStatus::Done,
  StatusSource::AgentNodeState)`.
- `unknown_refinement_requires_exact_native_binding`: use four independent
  models. First add an exact Codex-SID, non-live-line ended positive control and
  require Agent-Node-sourced `done`. Then prove that a Codex node with a
  different SID, a Claude node with the same SID, and an exact-binding node
  whose `last_event_kind` is `LIVE_LINE_EVENT_KIND` each leave the target
  `unknown` with `StatusSource::TaskState`. The positive control makes the test
  RED on the starting HEAD; the three negative controls must already satisfy
  their unknown assertions.
- `newest_exact_native_agent_must_be_ended`: use two custom Agent Node IDs with
  the same exact binding. An older ended node followed by a newer working node
  must leave semantic ended-unknown as Task-State-sourced `unknown`; reversing
  the timestamps must produce Agent-Node-sourced `done`.
- `native_unknown_uses_exact_native_ended_agent_but_definitive_outcomes_win`:
  for a running target and stale exact-bound ended node, require native Unknown
  to display Agent-Node-sourced done, while native Done, Error, and Cancelled
  keep their existing status and `StatusSource::NativeSessionLifecycle`.

Add these characterization-GREEN tests in the same setup:

- `semantic_definitive_outcomes_override_exact_native_ended_agent`, covering
  Completed, Failed, and Cancelled with their existing Task-State sources.
- `durable_native_projection_does_not_change_run_rate_activity`, requiring the
  same `RunRateActivity` as the starting HEAD and checking the filtered
  rate-only projection's existing visit/candidate behavior.

In `src/tui/view.rs`, add
`display_stale_parented_ended_agent_refines_exact_bound_child_run`. Construct a
controller/root Task Run and a separately visible child Task Run connected by
the existing execution-edge placement. Give the child a controller primary key
and exact Codex native alias. Insert an ended Agent Node with the child's SID
but with the root run as `task_run_id`, a present `parent_agent_node_id`, and
`last_activity_at_ms` exactly at the negative default inactivity boundary.
Build the tree at `AppState::default()` and require:

```rust
assert_eq!(
    only_run_row(&rows, child_run_id).display_status,
    Some(projection::DisplayStatus::new(
        projection::TaskDisplayStatus::Done,
        projection::StatusSource::AgentNodeState,
    )),
);
assert!(only_run_row(&rows, child_run_id).label.starts_with("✓ done "));
assert!(!has_agent_row(&rows, "durable-ended-child"));
```

The child run must be tested in both real closure shapes: once as
`TaskState::EndedUnknown` without a native end and once as nonterminal with
`NativeSessionEndStatus::Unknown`.

- [ ] **Step 2: Run and record RED before any production edit**

Run the four missing status tests and the tree test separately:

```sh
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked ended_unknown_uses_exact_native_ended_agent_across_staleness
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked unknown_refinement_requires_exact_native_binding
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked newest_exact_native_agent_must_be_ended
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked native_unknown_uses_exact_native_ended_agent_but_definitive_outcomes_win
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked display_stale_parented_ended_agent_refines_exact_bound_child_run
```

Expected: each positive refinement assertion fails because the starting
projection has no exact-native evidence map and returns unknown before such
evidence could be considered. In
`unknown_refinement_requires_exact_native_binding`, the exact-match positive
control fails while all three negative controls already produce their required
Task-State-sourced unknown. An unrelated compile failure is not acceptable RED.

Run the two characterization tests separately:

```sh
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked semantic_definitive_outcomes_override_exact_native_ended_agent
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked durable_native_projection_does_not_change_run_rate_activity
```

Expected: both pass on the starting HEAD. Record them as baseline GREEN, not
RED evidence.

- [ ] **Step 3: Build presentation-only exact-native evidence**

Add `durable_native_agents: HashMap<RunId, AgentStatusEvidence>` to
`StatusReadModel`.

When `from_model_filtered` is creating the full display projection
(`run_id.is_none()`), build one local lookup from every `RunKey::Native {
provider, sid }` returned by `model.task_run_bindings()` to its canonical
`RunId`. Do not build this lookup in the filtered rate-only path.

During the existing single Agent Node loop, before the current ownership/root
filters:

1. reject a presentation candidate if its event kind is
   `LIVE_LINE_EVENT_KIND` or it lacks `native_session_id`;
2. look up its exact `(provider, native_session_id)` in the local alias map;
3. insert its evidence under the resolved target `RunId`, regardless of the
   Agent Node's `task_run_id`, `parent_agent_node_id`, or display staleness; and
4. use the existing ordering `(last_activity_at_ms, agent_node_id)` when
   replacing a candidate.

After that presentation insertion, execute the existing owner, root,
live-line, provider, and staleness filters for `root_agents` without changing
their behavior. Do not increment the test-only Agent Node visit counter twice.

Factor the deterministic replacement into a private helper shared by the two
maps:

```rust
fn insert_newest_agent_evidence(
    selected: &mut HashMap<RunId, AgentStatusEvidence>,
    run_id: RunId,
    candidate: AgentStatusEvidence,
) {
    selected
        .entry(run_id)
        .and_modify(|current| {
            if (current.last_activity_at_ms, current.agent_node_id.as_str())
                < (candidate.last_activity_at_ms, candidate.agent_node_id.as_str())
            {
                current.clone_from(&candidate);
            }
        })
        .or_insert(candidate);
}
```

- [ ] **Step 4: Refine only otherwise-unknown lifecycle outcomes**

Add this private method:

```rust
fn durable_ended_status(&self, run_id: RunId, inactive: bool) -> Option<DisplayStatus> {
    self.durable_native_agents
        .get(&run_id)
        .and_then(|evidence| {
            matches!(evidence.state.as_ref(), Some(ExecState::Ended)).then_some(
                DisplayStatus::new(TaskDisplayStatus::Done, StatusSource::AgentNodeState)
                    .with_stalled(inactive),
            )
        })
}
```

Restructure the semantic-terminal block so Completed, Failed, and Cancelled
return their existing Task-State status, while EndedUnknown first calls
`durable_ended_status` and otherwise returns the existing Task-State-sourced
unknown.

For a nonterminal Task Run, keep native Done, Error, and Cancelled unchanged.
Only native Unknown calls `durable_ended_status` and otherwise returns the
existing Native-Session-Lifecycle-sourced unknown. Leave queued/blocked, pane,
execution, fresh root, running fallback, and `run_rate_activity` unchanged.

- [ ] **Step 5: Run focused tests GREEN**

Rerun every command from Step 2, then run:

```sh
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked status::tests
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked display_stale_ended_agent_is_absent_from_tree_rows
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked only_display_stale_agent_supplies_no_run_live_line_fallback
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- mise exec rust@1.97.1 -- cargo test --locked tui::view::tests
```

Expected: all pass. Confirm the existing exact-run versus full-projection
activity test still passes, the rate-only result is unchanged, and the evidence
visit counter remains one visit per Agent Node.

- [ ] **Step 6: Update normative documentation**

In `docs/tui.md`, document that only an exact provider-and-native-session match
can refine semantic `ended_unknown` or nonterminal native lifecycle `Unknown`
to Agent-Node-sourced `done`. State that Agent ownership and parentage do not
identify the target Task Run, the Agent Node row and live-line still disappear
at the existing staleness deadline, and no semantic or native lifecycle record
is changed.

In `docs/design/herdr-top-mvp.md`, update the effective precedence paragraph
with the same two narrow unknown refinements. Preserve definitive semantic and
native outcomes and explicitly exclude running fallback and non-exact evidence.

- [ ] **Step 7: Run complete local gates**

Run:

```sh
make test
make lint
make build
git diff --check
```

Expected: all commands exit zero. Report the actual changed files and confirm
they are a subset of the four declared implementation files.

- [ ] **Step 8: Return the uncommitted implementation result**

The implementer returns the diff, every RED and GREEN command with outcome,
complete gate results, actual changed-file set, and any coverage gap. It does
not commit, push, merge, rebase, or delegate.

## Controller integration and live acceptance

1. Verify the implementation diff and reported tests independently.
2. Confirm the actual changed files are a subset of the declared four files.
3. Integrate and commit the verified task serially on
   `agent/stable-task-history-rates`.
4. Build the release binary and run it in an adjacent pane against fresh private
   state and runtime roots outside the repository:

   ```sh
   HERDR_SESSION=herdr-top \
   XDG_STATE_HOME=/tmp/herdr-top-durable-child-state \
   XDG_RUNTIME_DIR=/tmp/herdr-top-durable-child-runtime \
   HERDR_TOP_HEADLESS_INACTIVITY_MS=30000 \
   target/release/herdr-top
   ```

   Create the two `/tmp` directories with owner-only permissions before launch.
   Do not point the experimental binary at the operator's normal state root.
5. Spawn one real Codex child under the observed session. Before the 30-second
   inactivity closure, record the still-running child Task Run and the ended
   Agent row produced by exact completion. At or after closure, confirm that the
   Agent row is hidden and the child Task Run is `done`. These checkpoints
   intentionally use the same threshold for native lane closure and ended-Agent
   row staleness; they do not require an impossible pre-closure `done` Task Run.
6. Restart the same verified binary with the same private roots and confirm the
   child Task Run remains `done`. Inspect the private SQLite state to confirm
   the Agent Node is parented and root-owned, its provider and SID exactly match
   the child's unique native binding, and its state remains ended.
7. Run `target/release/herdr-top doctor` with the same environment and require
   healthy persistence, zero new not-committed or skipped persistence batches,
   no new native-session UNIQUE occurrence, and no duplicate native binding.
8. Run the mandatory final whole-change `claude-reviewer` over the complete new
   extension and adjudicate every finding. Grok supplied no valid plan result;
   any separate Grok final review would require fresh explicit authorization
   and a new frozen dispatch.
9. Push the latest branch, wait for a nonempty conclusive CI success set,
   request latest-HEAD Copilot review, and adjudicate all findings. Do not merge.

## Rollback

Revert the implementation commit and its planning-baseline commit together if
the behavior must be withdrawn. No database or schema rollback is required.
