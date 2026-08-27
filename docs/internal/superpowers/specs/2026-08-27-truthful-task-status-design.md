# Truthful Task and Agent Status Display

**Status:** approved design for implementation planning.
**Baseline:** `main = 60e3df4` (PR #19 merged).
**Design references:** `docs/design/herdr-top-mvp.md` sections 2, 5.4, 8.3,
and 13; `docs/tui.md` sections "Execution tree" and "Task Run rows".

## Summary

Herdr Top will replace its current Task Run glyph-only lifecycle display with
an explicit, source-aware status vocabulary. The same visible words are used
throughout the execution tree and dependency DAG, but their evidence source is
selected independently for a pane-backed Task Run, a headless Task Run, and a
native Agent Node. This preserves the distinction between Herdr's pane agent
status, the Controller's semantic Task Run lifecycle, and provider-native
agent state instead of forcing all three into one enum.

The visible vocabulary is:

```text
queued / working / idle / blocked / done / error / cancelled / unknown
```

`stalled` remains an orthogonal warning, not a replacement lifecycle state.
The `[unlinked]` relationship annotation is removed from Task Run row labels;
relationship facts remain available in the dependency view and the Detail
overlay. A root Agent Node that merely duplicates its owning Task Run is no
longer rendered as a child, while Agent Nodes with explicit provider parentage
remain visible as native children.

The status contract is documented in the Help overlay, summarized in the
README, and specified completely in `docs/tui.md`.

## Problem

The current execution tree conflates three separate axes:

1. A Task Run glyph represents `TaskState`, collapsing `running` with
   `blocked`, `failed` with `cancelled`, and `queued` with `ended_unknown`.
2. An `[unlinked]` suffix represents the absence of execution or dependency
   edges, but its placement makes it appear to be a runtime status. It is not a
   Herdr API state and does not mean that the Task Run is detached from its
   pane.
3. Root Agent Nodes are rendered under their owning Task Run even when they
   carry no state, model, or activity. The indentation makes these duplicate
   session-owner records appear to be child tasks with unknown status.

The loss is observable for a pane waiting on user approval: Herdr reports
`agent_status=blocked`, but the Task Run row uses the same glyph as ordinary
running work. Herdr's `done` and `idle` are also currently normalized to the
same execution state at collection time, so the TUI cannot reproduce the
source status. Headless descendants have a different evidence surface and
must not be assigned a pane status they never had.

## Goals

1. Make the current operational status of every visible Task Run explicit in
   text as well as by glyph and color.
2. Preserve Herdr's five public pane agent statuses (`idle`, `working`,
   `blocked`, `done`, and `unknown`) without changing their source semantics.
3. Derive headless Task Run and native Agent Node statuses from their own
   evidence, with deterministic precedence and no guessed success or failure.
4. Keep semantic Task lifecycle, physical execution status, and relationship
   state as separate axes.
5. Show approval waits as `blocked`, using the same red attention treatment as
   Herdr, without claiming that every block is an approval prompt.
6. Keep `cancelled` distinct from `error`, because an intentional stop is not a
   failure.
7. Preserve `unknown` whenever evidence cannot distinguish the other states.
8. Document the status vocabulary and derivation rules in-product and in the
   repository's user documentation.

## Non-goals

- Inferring why Herdr reports `blocked`. The API supplies a state, not an
  approval reason.
- Inferring Task Run relationships from pane proximity, timestamps, shared
  paths, labels, or status.
- Converting missing state into `idle`, `done`, or `error`.
- Aggregating a child's activity into its parent Task Run. Each row reports its
  own entity or occurrence.
- Changing Controller emit commands or their semantic `TaskState` transition
  contract.
- Changing the SQLite schema solely to retain Herdr's transient pane status.
- Treating provider process exit as successful Task completion without a
  corresponding semantic terminal fact.

## Considered approaches

### 1. View-only relabeling

The renderer could map the existing `TaskState` and `ExecState` values to new
words. This is small, but it cannot distinguish Herdr `done` from `idle`
because the collector deliberately collapses both before they reach the
model. It also leaves pane-backed and headless evidence selection implicit.
This approach is rejected because it would advertise precision the model does
not retain.

### 2. Add `Done` to `ExecState`

This preserves the wire value through the existing execution pipeline, but it
mixes Herdr's transient pane-attention state with execution lifecycle.
Persisting a new `exec_state` value would also require a schema-version change
to keep older binaries from opening a database value they cannot parse. This
approach is rejected because the new state belongs to the pane observation
axis, not the durable execution axis.

### 3. Preserve pane agent status separately and project a display status

The chosen approach adds a transient, typed pane agent-status read model while
leaving `TaskState` and `ExecState` semantics intact. A pure TUI projection
then selects the authoritative evidence for each row occurrence and maps it to
the shared display vocabulary. This is more work than relabeling, but it keeps
the boundaries truthful and avoids a database migration.

## State axes

### Semantic Task Run lifecycle

`TaskState` remains unchanged:

```text
queued / running / blocked / completed / failed / cancelled / ended_unknown
```

It records Controller or reducer knowledge about the semantic Task Run. It is
durable and remains visible in the Detail overlay even when another source
drives the row's operational display status.

### Durable execution lifecycle

`ExecState` remains unchanged:

```text
unknown / idle / working / blocked / stale / ended
```

It continues to drive execution liveness, stale handling, placement, and
persistence. Herdr `done` continues to map to non-terminal `ExecState::Idle`
for these execution-lifecycle purposes; the separate pane observation retains
the exact source value for display.

### Transient Herdr pane agent status

A typed value retains Herdr's public API vocabulary exactly:

```text
idle / working / blocked / done / unknown
```

The current value is keyed by public pane ID in a transient `DomainModel` read
model. Snapshot reconciliation replaces the value for every observed pane,
pane-scoped agent-status events update it even when no execution state changes,
and pane removal deletes it. It is not written to SQLite. On cold restore,
before a fresh Herdr snapshot arrives, its absence is rendered honestly by the
fallback rules below. This avoids persisting an attention state that Herdr can
re-supply authoritatively.

The collector must preserve one raw pane observation independently from the
existing per-execution state updates. This transient observation produces no
event-ledger row or durable projection of its own. An `idle` to `done`
transition therefore updates the transient pane status even though both values
still map to `ExecState::Idle`. Existing per-execution ledger and
rate-accounting behavior continues unchanged.

### Display status

`TaskDisplayStatus` is a TUI read-model value, not a persisted domain state:

```text
Queued / Working / Idle / Blocked / Done / Error / Cancelled / Unknown
```

The projection also returns a source discriminator for Detail and tests:

```text
TaskState / PaneAgentStatus / ExecutionState / AgentNodeState / Fallback
```

Stall is returned as a separate boolean warning. Neither filtering nor
relationship computation may derive from `TaskDisplayStatus`.

## Task Run status derivation

The projection receives a Task Run and the optional pane ID of the rendered
occurrence. It applies the following precedence.

### 1. Semantic terminal facts

Terminal `TaskState` always wins over live or stale runtime observations:

| TaskState | Display status |
| --- | --- |
| `completed` | `done` |
| `failed` | `error` |
| `cancelled` | `cancelled` |
| `ended_unknown` | `unknown` |

This prevents a late or stale pane observation from repainting a completed or
failed task as active.

### 2. Semantic pre-start and blocked facts

| TaskState | Display status |
| --- | --- |
| `queued` | `queued` |
| `blocked` | `blocked` |

An explicit semantic block wins over a contradictory active-looking runtime
observation. The Detail overlay exposes the source so an operator can tell a
Controller block from a pane block.

### 3. Pane-backed occurrence

For a row placed under a pane, the exact transient status for that pane drives
the display when available:

| Herdr pane status | Display status |
| --- | --- |
| `working` | `working` |
| `idle` | `idle` |
| `blocked` | `blocked` |
| `done` | `done` |
| `unknown` | `unknown` |

The mapping is occurrence-specific. When one Task Run is shared across panes,
each repeated row uses its own pane's status rather than a global newest or
highest-severity status.

If no transient pane status exists, such as during cold restoration before
the first snapshot, the projection consults the matching execution for that
pane:

| ExecState | Display status |
| --- | --- |
| `working` | `working` |
| `idle` | `idle` |
| `blocked` | `blocked` |
| `unknown` | `unknown` |
| `stale` | `unknown` plus stalled warning |
| `ended` | `unknown` unless a terminal TaskState already won |

An explicit Herdr `unknown` is not treated as absence and does not fall
through to older execution evidence.

### 4. Headless Task Run

A Task Run with no rendered pane occurrence consults only its own root Agent
Node evidence; it does not aggregate descendant Agent Nodes. The projection
selects the newest non-display-stale root node that matches the run's provider
identity, using the existing deterministic activity and node-ID tie break.

| Agent node state | Display status |
| --- | --- |
| `working` | `working` |
| `idle` | `idle` |
| `blocked` | `blocked` |
| absent or `unknown` | fall through |
| `stale` | `unknown` plus stalled warning |
| `ended` | `unknown` unless a terminal TaskState already won |

Provider exit does not imply semantic success. A headless run whose only fact
is `TaskState::Running` falls back to `working`, with `TaskState` recorded as
the source. If no applicable fact exists, it renders `unknown`.

## Native Agent Node status derivation

Native Agent Node rows use the same visible vocabulary but their own evidence:

| Agent node state | Display status |
| --- | --- |
| `working` | `working` |
| `idle` | `idle` |
| `blocked` | `blocked` |
| `ended` | `done` |
| `stale` | `unknown` plus stalled warning |
| absent or `unknown` | `unknown` |

Agent Nodes have no authoritative queued, failed, or cancelled channel, so the
renderer never invents those values. Their model and last-activity fields
remain independent of status.

The execution tree renders an Agent Node only when provider evidence gives it
an explicit `parent_agent_node_id`. A root Agent Node is the provider-native
representation of the owning Task Run and is therefore redundant below that
same Task Run. Hiding it changes no model or SQLite data and does not affect
the Detail overlay. If a visible descendant's root parent is hidden, the
existing filtered-hierarchy rule attaches the descendant directly beneath the
Task Run.

## Stall warning

Stall remains an orthogonal alert derived from the existing inactivity
threshold. A non-terminal row keeps its base status word while its normal
glyph and color are replaced by the warning treatment. For example:

```text
⚠ working Codex Run unit tests
```

Terminal `done`, `error`, and `cancelled` rows never receive the stall
override. `ExecState::Stale` produces `unknown` plus the warning rather than a
fabricated active state.

## Row grammar and presentation

Task Run rows use this grammar:

```text
<glyph> <status> <worker-kind>[ <subject>][ — <live line>][ · <duration>][ relationship annotations]
```

The status token stays at the left edge so narrow-pane truncation drops
subject and live-line detail before the operational state. Status is always
written as text; color and glyph are redundant cues, not the only meaning.

| Status | Glyph | Color intent |
| --- | --- | --- |
| `queued` | `◌` | dim/default |
| `working` | `●` | green |
| `idle` | `○` | dim/default |
| `blocked` | `●` | red |
| `done` | `✓` | green |
| `error` | `✗` | red |
| `cancelled` | `⊘` | yellow |
| `unknown` | `?` | dim/default |
| stalled override | `⚠` | yellow |

Selection reversal must retain readable status text. ASCII tree mode affects
only tree connectors and does not replace status glyphs, matching the existing
scope of `HERDR_TOP_ASCII_TREE`.

The dependency DAG uses the same status prefix. The execution tree and DAG
remove `[unlinked]` from row labels. `[shared]` and `[dispatched by: ...]`
remain relationship annotations because they describe non-obvious placement.
The DAG's prerequisite and dependent columns continue to expose linked
relationships directly.

## Relationship detail

The absence of a semantic edge remains a real model fact, but it is no longer
presented as runtime status. Task Run Detail adds explicit relationship lines:

```text
dispatch_parent: <run name or none>
prerequisites: <count>
dependents: <count>
task_relationships: none | present
```

Filtering may continue to accept the legacy term `unlinked` as a searchable
relationship synonym, because removing a row annotation need not remove a
useful query. Documentation calls the condition "no recorded task
relationships" rather than a state.

## Help and documentation

The Help overlay gains a compact status section before runtime diagnostics:

```text
Task status: queued=announced, working=active, idle=waiting, blocked=needs attention
Task status: done=finished, error=failed, cancelled=stopped, unknown=insufficient evidence
Warning: ⚠ means stalled; it does not replace the status word
Status source: pane-backed rows use Herdr; headless rows use task/agent evidence
```

The exact prose may be line-wrapped for the overlay, but all statuses and the
stalled rule must remain discoverable by searching the rendered Help text.

The README gains the vocabulary table and a corrected TUI example. Its
unattached example no longer uses `[unlinked]`. `docs/tui.md` is the canonical
user contract and contains the full precedence tables, row grammar, native
Agent Node rule, relationship separation, and narrow-pane behavior.

The following existing documents must be updated wherever they promise the
old row annotation or glyph mapping:

- `docs/design/herdr-top-mvp.md`
- `docs/guides/controller-emit-setup.md`

Historical internal plans and completed design records remain unchanged; they
describe the contract at the time they were approved.

## Data flow and ownership

1. The Herdr collector parses the raw public `agent_status` string into a
   closed typed value. Missing or unrecognized strings fail safe to `unknown`
   without panicking or inventing another state.
2. Snapshot reconciliation replaces transient pane statuses while continuing
   to project execution liveness through `ExecState`.
3. Pane-scoped status events produce one raw pane-status observation even when
   their mapped `ExecState` is unchanged, then retain the existing matching
   per-execution updates and ledger behavior.
4. The reducer is the only writer of both the transient pane-status map and
   durable execution state. Pane removal clears transient status.
5. The TUI builds a frame-local status projection once and reuses it for tree,
   DAG, Detail, filter fields, and row styling. Rendering performs no model
   mutation and no additional full-model scans per row.

The transient map is intentionally absent from SQLite restore. Snapshot
reconciliation is its authority; an unavailable Herdr source leaves the
status absent and triggers the documented fallback rather than replaying stale
attention state.

## Failure and ambiguity handling

- An unrecognized Herdr status becomes `unknown`; it never becomes `idle`.
- A missing transient status may fall back to matching execution evidence, but
  an explicit Herdr `unknown` may not.
- An ended execution or Agent Node does not make a Task Run `done` without a
  success fact.
- A failed Task Run remains `error` even if its pane reports `idle`, `working`,
  or `done` later.
- A cancelled Task Run remains `cancelled`, never `error`.
- A pane's `blocked` status is rendered as `blocked`; no approval-specific text
  appears without a future reason-bearing API.
- Conflicting shared-pane statuses remain visible on their respective
  occurrences instead of being collapsed.
- Observation degradation and persistence diagnostics remain separate from
  Task display status.

## Verification strategy

Implementation follows TDD. Required regression coverage includes:

1. A complete table test for terminal and non-terminal TaskState precedence.
2. Exact parsing and retention of all five Herdr pane statuses, including an
   `idle` to `done` transition that does not change `ExecState`.
3. Snapshot reconciliation, pane event update, pane removal, and cold-restore
   absence for the transient status map.
4. Per-pane status selection for a shared Task Run with conflicting statuses.
5. Parent, child, grandchild, pane-backed, and headless Task Run projection.
6. Controller-blocked and Herdr-blocked rows, including the red blocked style.
7. Completed, failed, cancelled, and ended-unknown terminal precedence.
8. Headless Agent Node mapping, missing-state `unknown`, and no descendant
   aggregation into a parent Task Run.
9. Stall warning overlay without loss of the base status word.
10. Root Agent Node suppression with explicitly parented native descendants
    retained and correctly re-parented for display.
11. Removal of `[unlinked]` from tree and DAG row labels while Detail and
    filtering retain relationship information.
12. Help overlay status definitions, scrolling, and narrow-height rendering.
13. README and `docs/tui.md` examples matching the implemented grammar.
14. Existing metric-column, tree-indent, selection, filter, summary, restore,
    convergence, and workload-harness regressions.

The implementation plan must name exact targeted tests and retain the known
SIGHUP-safe gate wrapper for workload tests:

```sh
setsid perl -e '$SIG{HUP}="DEFAULT"; exec @ARGV' -- <test command>
```

At minimum, the final integrated verification runs formatting, linting, the
full Rust test suite, and the workload-harness tests through this environment.

## Implementation boundaries

The work is split into serial integration tasks because the state ingestion,
TUI projection, and documentation surfaces depend on one another:

1. Preserve transient Herdr pane status through collector, model, reducer, and
   convergence tests without changing visible output.
2. Add the source-aware Task and Agent display projection, row grammar,
   styling, Detail output, relationship-label removal, and TUI tests.
3. Update Help, README, `docs/tui.md`, the MVP design, and emit setup guidance,
   then run the whole-change verification and review gates.

Each implementation task must declare an exact file set in the implementation
plan. Integration remains serial even if read-only reviews run concurrently.

## Acceptance criteria

1. A pane awaiting approval renders `blocked` and is visually distinct from
   `working`.
2. Herdr `idle`, `working`, `blocked`, `done`, and `unknown` remain
   distinguishable for pane-backed rows.
3. Headless child and grandchild rows derive status only from their own Task
   and Agent evidence.
4. `queued`, `error`, `cancelled`, and `unknown` remain distinct and truthful.
5. A stalled row retains its underlying status word and shows a warning.
6. Shared Task Run occurrences can display different statuses per pane.
7. `[unlinked]` no longer appears as a Task Run row status or annotation.
8. Session-owner root Agent Nodes no longer appear as false child tasks;
   explicitly parented native descendants remain visible.
9. Help, README, and `docs/tui.md` explain the shipped status contract.
10. No SQLite schema migration is introduced for transient Herdr status, and
    cold restore never presents a stale persisted pane-attention state.
11. Relevant targeted tests, formatting, linting, full tests, and
    workload-harness tests pass under the SIGHUP-default gate.
