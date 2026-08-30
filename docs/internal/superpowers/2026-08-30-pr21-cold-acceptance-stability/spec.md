# PR #21 Cold Acceptance Stability

## Status

Approved in intent on 2026-08-30 as the final corrective increment for PR #21.
The user authorized implementation after confirming three observed defects in a
real adjacent Herdr pane: a restored Claude controller remained `unknown`, a
large cold provider-history drain took about seven minutes, and Codex rows
displayed rollout identifiers instead of available agent roles.

## Context

The current PR already persists native Agent Node completion and survives the
same-batch native-binding collision that previously wedged provider ingress.
The four controller/child routes can be exercised, but the cold acceptance run
still exposes three independent defects:

1. history finalization may synthesize native `Unknown` for a Task Run even
   while that run has an open execution, and a later live Herdr snapshot does
   not clear that stale native lifecycle value when the Task Run is already
   `running`;
2. a full provider egress queue is retried only at the ordinary two-second
   rescan cadence, limiting a large durable backlog to roughly 32 events per
   second; and
3. Codex rollout metadata retains `source.subagent.thread_spawn.agent_role`,
   but lane synthesis publishes the generic originator as the run kind. After
   hook/native identity convergence, the Controller key also prevents the TUI
   from recognizing the row as a native Codex child, so rollout IDs leak into
   Task and Agent labels.

The three causes are independent. They share one user-visible acceptance gate:
after a cold start, all four Codex/Claude controller-child combinations must
stabilize promptly and render stable statuses and meaningful labels.

## Goals

1. Never synthesize a history-only native `Unknown` while the Task Run owns an
   execution whose durable `ended_at_ms` is null.
2. Make a current nonterminal Herdr snapshot repair a previously persisted
   stale native lifecycle end through the existing monotonic lifecycle path.
3. Retry a blocked provider pending queue on a short bounded cadence without
   advancing the provider parser, dropping events, changing coalescing, or
   resetting the periodic rescan clock.
4. Project a Codex role into the run kind when available, including raw
   `source.subagent.thread_spawn.agent_role`; otherwise use a meaningful
   `source.subagent.other`, then the originator/provider fallback.
5. Keep provider session identifiers out of primary Task Run and Agent Node
   rows. Full identity remains available in Detail.
6. Prove the final behavior with focused regressions, repository-wide gates,
   a rebuilt `~/.local/bin/herdr-top`, and a real adjacent-pane cold test of all
   four controller-child combinations.

## Non-goals

- Do not change SQLite schema, migrations, retention, or task visibility.
- Do not infer semantic `completed`, `failed`, or `cancelled` from a live Herdr
  snapshot.
- Do not weaken lifecycle watermark ordering or let older liveness overwrite a
  newer terminal observation.
- Do not drop, batch-away, or coalesce durable history events to improve drain
  speed.
- Do not make pending retries perform additional provider scans or advance a
  parser cursor while prior output remains blocked.
- Do not treat Codex rollout `agent_role` as a stable public API guarantee.
  Missing, null, empty, unknown, and future source shapes must remain safe.
- Do not expose a new public CLI option or configuration surface.
- Grok controller/worker runtime support remains a later increment.

## 1. Live execution and native lifecycle convergence

### 1.1 Store finalization guard

The history-drain finalization transaction may synthesize native `Unknown` only
for a historical run that has no open durable execution. Every repeated CASE
predicate in the single `task_runs` update must therefore include an equivalent
`NOT EXISTS` condition over `executions` for the same `run_id` with
`ended_at_ms IS NULL`.

This is a finalization eligibility rule, not a display-precedence rule. A run
with only ended executions remains eligible for the existing history-derived
unknown outcome. A run with any open execution is not.

### 1.2 Snapshot self-repair

The Store guard prevents new false terminal evidence, but existing databases
may already contain it. When `reconcile_snapshot_inner` observes a
nonterminal execution and resolves its provider/native-session identity to the
execution's owning Task Run, the reducer must:

1. retain the existing Task Run activation behavior;
2. after activation, require the Task Run to be semantically nonterminal;
3. when that run currently has a native lifecycle end, apply
   `native_session_end = None` through `apply_native_lifecycle` with a fresh
   snapshot-derived `NativeLifecycleWatermark`; and
4. persist the resulting V6 state in the same snapshot batch.

Resolution may come from a primary Native key, a Native alias, or unanimous
Agent Node identity evidence through the existing `run_for_native_session`
path. Requiring an already-installed Native alias would miss the affected cold
Claude root.

The watermark uses the snapshot observation time for both source and observed
time and a deterministic source order derived from the live execution ID. The
ordinary total ordering decides whether the liveness observation is newer: an
older snapshot cannot erase a newer terminal observation, while a newer
snapshot clears an older terminal observation. No direct field overwrite or
precedence bypass is allowed. Semantic terminal Task Runs remain terminal and
are not reopened by this repair. Skipping the lifecycle call when no native end
is present prevents every ordinary snapshot from producing a new V6 write.

This gives a fixed binary an automatic recovery path for the already affected
database: the next authoritative live snapshot clears the stale native unknown
without manual SQL.

### 1.3 Finalization-before-snapshot convergence

Provider history may be serviced while the Herdr subscription is unavailable,
so finalization can precede the first authoritative snapshot. In that order, an
old durable execution with `ended_at_ms IS NULL` suppresses history-derived
unknown even when the execution is no longer present. Because the run is then
history-ready, finalization will not be repeated.

Snapshot reconciliation must complete this deferred decision after it first
closes all pre-gap executions and reinstalls current live executions:

- a pre-gap run receives native `Unknown` through `apply_native_lifecycle` only
  when it is history-ready, semantically nonterminal, has no native lifecycle
  end, has `latest_provider_at_ms`, and has no live execution;
- the watermark source time is the run's existing `latest_provider_at_ms`, the
  observed time is the snapshot time, and the deterministic source order names
  the snapshot history-close decision;
- a currently live run is excluded and follows the liveness repair in section
  1.2; and
- a definitive or newer native terminal outcome is never overwritten.

Within the pre-gap pass, the existing semantic
`close_run_without_live_execution` decision runs first. The deferred native
rule then evaluates the resulting Task Run and V6 state, avoiding two competing
writes for a run that the semantic close already ended.

This is the order-independent complement to the Store guard. If the snapshot
runs first, it durably closes stale executions and Store finalization makes the
existing decision. If finalization runs first, the snapshot makes only the
deferred decisions that the open-execution guard intentionally skipped.

## 2. Pending egress retry

`provider_thread_main` must distinguish a pending-queue retry timeout from a
periodic full-rescan timeout.

When the pending queue is nonempty, the next wait deadline is the earlier of:

- the unchanged periodic full-rescan deadline; and
- a 20 ms pending retry deadline.

On a pending retry timeout the thread calls a flush-only path that invokes
`pending.flush_to_sender`, records the same bounded diagnostics, and never
calls `worker.process`. Reusing `run_provider_cycle` is incorrect because it
calls the worker immediately when the retry empties the queue. The retry is not
a full rescan, does not clear a deferred force-rescan request, does not update
`last_full_rescan`, and never installs watcher requests. Stop and control
messages retain priority through the existing receiver path. A closed egress
remains bounded by the 20 ms retry cadence rather than a busy loop.

No pending event semantics change. Deterministic order, capacity, history
barriers, diagnostics, and coalescing remain exactly as implemented.

## 3. Codex role and UUID-free primary labels

### 3.1 Run-kind source priority

The projected `DomainModel::run_kind` remains the normalized display source.
Codex lane synthesis initializes it from the first nonempty value in this
priority order:

1. a normalized role already supplied by an event source;
2. raw `source.subagent.thread_spawn.agent_role` retained as
   `CodexInternal::ThreadSpawn.role`;
3. meaningful `source.subagent.other` retained as
   `CodexInternal::Named.name`;
4. the existing nonempty originator, such as `codex-tui`; and
5. the provider fallback `Codex` in projection.

The current rollout parser supplies item 2 directly, so no second role field or
schema column is introduced. Null, empty, or control-containing values use the
existing sanitization and fallback behavior. Agent nicknames are not used as a
primary role because they are provider-assigned identity-like labels rather
than stable worker kinds.

### 3.2 Identity-converged Task Run rows

Native-provider membership is determined from exact task-run bindings, not
only the canonical `TaskRun.key`. A Controller-primary run with an exact
`RunKey::Native { provider: Codex, sid }` alias is therefore recognized as a
Codex run after identity convergence.

For provider-backed Task Run rows:

- a captured human subject is shown when the run is not an execution-edge
  child;
- an execution-edge child renders its run kind alone;
- when no captured subject exists, key-derived provider session IDs and
  path-derived run UUIDs are suppressed rather than used as fallback text;
- `[dispatched by: ...]` derives from the parent's UUID-free projected kind or
  captured human subject, never from `short_run_name` when that would expose a
  Controller key, native SID, or run UUID;
- pre-convergence `hook:codex:` and `hook:claude-code:` keys are recognized as
  provider-backed even before a Native alias exists; and
- generic non-provider Controller and provisional rows retain their existing
  meaningful fallback behavior when it is not identity-only.

### 3.3 Agent Node rows

For an Agent Node with a nonempty native session ID, the renderer first resolves
the exact `(provider, sid)` alias to its canonical Task Run and uses that run's
nonempty projected kind as the Agent label suffix. If no exact alias exists, it
uses a nonempty run kind from the Agent Node's owning Task Run. Exact alias wins
over ownership because a parented Codex child Agent Node may be owned by the
controller/root run while its SID names a separate child Task Run. The ownership
fallback preserves Controller-keyed Claude sub-agent roles that have no Native
alias. The row grammar becomes:

```text
<glyph> <status> <provider> native agent[: <role>]
```

If no exact run or role is available, the row ends at `native agent`; it never
falls back to `native_session_id` or `agent_node_id`. Model and last-activity
annotations remain unchanged. Full Agent and Task identities remain visible in
the Detail overlay and continue to participate in selection keys and filters.

## 4. Tests

### 4.1 Lifecycle tests

- A real Store finalization fixture with an open execution leaves native end
  unset and still marks history ready.
- The same historical run becomes eligible for native unknown after its
  execution has ended.
- A restored running run with stale native unknown is reconciled with a live
  snapshot through Native-key, Native-alias, and Agent-Node-only resolution,
  clears the native end through a newer watermark, persists the repair, and
  remains running.
- A semantic terminal run is not reopened by the snapshot repair.
- When history finalization precedes the first snapshot, the snapshot leaves a
  current live run open and gives an absent stale run the deferred native
  unknown outcome exactly once.
- An older snapshot cannot clear a newer terminal watermark, and an ordinary
  live snapshot with no current native end produces no lifecycle write.

### 4.2 Provider scheduling tests

- With a capacity-one egress prefilled before the initial provider cycle, the
  worker emits one pending event and pauses.
- After the receiver frees one slot, the pending event is delivered well before
  a one-second periodic rescan deadline.
- The worker call count does not advance while the pending event is being
  retried, and ordinary periodic rescan behavior remains covered by existing
  tests.

### 4.3 Label tests

- Codex metadata with `thread_spawn.agent_role = worker` publishes `worker` as
  the run kind.
- A null/empty role falls back to meaningful `subagent.other`, then originator.
- A Controller-primary Codex child with an exact native alias renders the role
  alone and contains neither the native SID nor run UUID.
- Pre-convergence Controller-backed Task rows and `[dispatched by: ...]`
  annotations contain no hook session ID.
- A provider-backed root with no subject renders its kind without the session
  ID; a root with a captured subject keeps the subject.
- An exact-bound Agent Node renders the role rather than its SID; an unmatched
  Agent Node renders no identity suffix.
- A Controller-keyed Claude Agent Node without a Native alias renders its
  owning Task Run's role rather than its Agent ID.
- Detail continues to expose full identities.

## 5. Acceptance

After automated verification and final cross-model review:

1. Run `make build` and install the byte-identical optimized
   `target/release/herdr-top` at `~/.local/bin/herdr-top`.
2. Use the adjacent Herdr pane and private owner-only state directories.
3. Cold-start a Codex controller with Codex and Claude child tasks, then a
   Claude controller with Codex and Claude child tasks.
4. For each pattern, observe root and child appearance while live, terminal
   stabilization after completion, persistence health, history readiness, and
   absence of unlinked duplicates.
5. Restart the monitor against the same private state and verify the stable
   result is restored promptly.
6. Require the Claude root to stop displaying stale `unknown`, require no
   seven-minute egress drain tail, and require Codex Task/Agent rows to show
   roles or provider fallbacks without rollout/session UUIDs.

The acceptance artifacts record timestamps, pane output, doctor JSON, and
read-only SQLite queries outside the repository. The test does not mutate the
live production state root.

## Rollback

Revert the three code slices and their documentation together. No schema or
data rollback is required. Existing stale lifecycle evidence will remain until
a pre-change binary's ordinary newer provider liveness clears it or the fixed
binary is restored.
