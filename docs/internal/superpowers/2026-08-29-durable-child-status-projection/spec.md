# Durable Child Status Projection

## Status

Approved for planning on 2026-08-29. This is a corrective extension to
`../2026-08-28-codex-child-history-performance/spec.md`. It narrows the gap
between the existing durable Agent Node completion observation and the
corresponding child Task Run's presentation. It does not change Task Run
lifecycle state.

## Context

PR 21 persists an exact Codex `SubAgentActivity` completion as
`AgentNode.state = ended`. The corresponding child Task Run can independently
close as `TaskState::EndedUnknown`, or a native-bound nonterminal Task Run can
receive `NativeSessionEndStatus::Unknown`, because Agent Node observations are
not semantic Task Run completion events.

The persisted topology does not make that Agent Node a root owned by the child
Task Run. A real Codex child remains a parented Agent Node owned by its
controller/root Task Run, while the child Task Run is independently addressable
through an exact `RunKey::Native { provider, sid }` alias. Therefore ownership
and `parent_agent_node_id` cannot identify the child Task Run whose status the
ended observation should refine.

Before inactivity closure, the execution tree can show the completed Agent Node
as `done` while the child Task Run is still running. An Agent Node whose state
is ended becomes display-stale after
`HERDR_TOP_HEADLESS_INACTIVITY_MS`, whose default is 600,000 ms, and its row is
then hidden. `StatusReadModel` currently applies the same staleness filter to
Task Run status evidence. At the same inactivity boundary the Task Run can close
to an unknown lifecycle outcome, which returns `unknown` before Agent Node
evidence is considered. The ended Agent row therefore disappears just as the
visible child Task Run becomes `unknown`, even though the exact ended
observation remains persisted.

The persistence collision correction is independent and remains unchanged.
This specification completes the presentation behavior promised by durable
child completion.

## Goals

1. Keep a completed child Task Run displayed as `done` after its exact
   native-session Agent Node crosses the Agent Node row staleness deadline.
2. Preserve the stored Task Run state, lifecycle ownership, ordering, visibility
   duration, and retention.
3. Preserve stronger semantic and native lifecycle outcomes.
4. Keep Agent Node row visibility and Task Run live-line fallback unchanged.
5. Preserve rate-accounting behavior and asymptotic projection cost.
6. Prove the behavior before and after restart with automated tests and a real
   child in the adjacent Herdr Top pane.

## Non-goals

- Do not write `TaskState::Completed` from Agent Node evidence.
- Do not change controller events, provider commands, native-session lifecycle,
  persistence schema, migrations, or retention.
- Do not infer success from an absent, idle, working, blocked, stale, or unknown
  Agent Node.
- Do not let an older ended Agent Node override a newer Agent Node for the same
  exact native-session binding.
- Do not keep a display-stale Agent Node row or its live-line text visible.
- Do not change measured Working-time or performance-rate accounting.

## Durable completion evidence

`StatusReadModel` maintains a presentation-only view of the newest Agent Node
for each Task Run's exact native-session alias. Projection first builds a lookup
from every `RunKey::Native { provider, sid }` in
`DomainModel::task_run_bindings()` to its canonical Task Run. During the
existing Agent Node scan, an Agent Node is a candidate only when:

- both its provider and nonempty `native_session_id` exactly match one native
  alias;
- its `last_event_kind` is not the synthetic live-line event kind; and
- it is the newest exact-binding candidate by the existing deterministic order
  `(last_activity_at_ms, agent_node_id)`.

Agent Node ownership and `parent_agent_node_id` are intentionally irrelevant:
a real child completion is parented and remains owned by the controller/root
run. Provider equality and session-ID equality are both required, so a foreign
provider or a different session cannot refine the child.

The selected candidate is durable completion evidence only when its exact
state is `ExecState::Ended`. Selecting the newest exact-binding candidate before
testing the state prevents an older ended observation from overriding a newer
non-ended observation. This presentation-only selection is separate from the
existing fresh root evidence used by ordinary runtime status and
`RunRateActivity`, so rate accounting does not change.

No Agent Node row is made visible by this selection. The existing
`agent_node_is_display_stale` rule continues to control tree rows, detail
selection fallback, visibility deadlines, newest visible Agent Nodes, and
Task Run live-line fallback.

## Display-status precedence

The existing precedence is preserved with two narrow refinements of an unknown
outcome:

1. `TaskState::Completed`, `TaskState::Failed`, and `TaskState::Cancelled`
   remain authoritative and map to `done`, `error`, and `cancelled`.
2. `TaskState::EndedUnknown` maps to `done` with
   `StatusSource::AgentNodeState` only when durable completion evidence exists;
   otherwise it remains `unknown` with `StatusSource::TaskState`. Native
   lifecycle evidence does not bypass semantic terminal precedence.
3. For a nonterminal Task Run, native lifecycle `Done`, `Error`, and `Cancelled`
   remain authoritative.
4. For a nonterminal Task Run, native lifecycle `Unknown` maps to `done` with
   `StatusSource::AgentNodeState` only when durable completion evidence exists;
   otherwise it remains `unknown` with
   `StatusSource::NativeSessionLifecycle`.
5. Semantic queued or blocked state, exact-pane status, matching execution,
   fresh root Agent Node state, and running fallback retain their existing
   order and behavior. Durable exact-binding evidence does not turn a still
   running Task Run into `done` before an otherwise-unknown lifecycle outcome
   exists.

The refinement changes display status only. `effective_lifecycle_end_ms`, Task
Run visibility, duration endpoints, persistence rows, and controller transition
rules continue to derive from their existing lifecycle sources.

## Performance and complexity

The native-alias lookup is built once from `task_run_bindings`, and the durable
presentation candidate is selected during the existing single Agent Node scan
in `StatusReadModel::from_model_filtered`. It adds one lookup entry per native
alias and at most one evidence entry per resolved Task Run, and does not add a
model-wide scan to each row. The filtered rate-only projection does not build or
consult the presentation map; `run_rate_activity` continues to use only its
existing fresh root evidence.

## Documentation

The TUI reference and MVP design must describe the exact unknown-outcome
refinement, its evidence source, and the separation between hidden Agent Node
rows and durable Task Run display evidence. They must not claim that the Task
Run becomes semantically completed.

## Acceptance criteria

1. An `ended_unknown` Task Run whose native alias exactly matches the newest
   ended Agent Node displays `done` with `StatusSource::AgentNodeState` both
   immediately before and at or after the Agent Node row staleness boundary,
   even when that Agent Node is parented and owned by another Task Run.
2. The same Task Run without durable ended evidence displays `unknown` with
   `StatusSource::TaskState`.
3. A nonterminal Task Run with native lifecycle `Unknown` and durable ended
   evidence displays `done`; without that evidence it remains native
   `unknown`.
4. Semantic completed, failed, and cancelled outcomes and definitive native
   done, error, and cancelled outcomes retain their existing status and source.
5. A foreign provider, a nonmatching session ID, or a synthetic live-line node
   does not refine an unknown outcome.
6. An older ended exact-binding Agent Node does not override a newer non-ended
   exact-binding Agent Node.
7. A display-stale ended Agent Node remains absent from execution-tree Agent
   rows and live-line fallback, while the exact-bound child Task Run row
   displays `done`.
8. `RunRateActivity` results are unchanged by presentation-only durable evidence.
9. No Task Run, native lifecycle, Agent Node, persistence, visibility, duration,
   or retention field is mutated by status projection.
10. Focused status and tree tests, existing visibility tests, complete repository
   test, lint, formatting, and build gates pass.
11. A private-root adjacent-pane test observes a real child before the shortened
    positive inactivity deadline, then at or after lifecycle closure with its
    Agent row hidden and child Task Run displayed as `done`; restarting the same
    verified binary with the same private roots preserves `done`, healthy
    persistence, and a unique native binding.

## Rollback

Revert the status projection, tests, and documentation together. No data or
schema rollback is required because this change writes no new persisted state.
