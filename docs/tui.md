# TUI guide

herdr-top is a fixed-screen terminal monitor for one resolved Herdr session. It
combines Herdr's physical workspace, tab, and pane topology with Task Runs and
native Claude Code or Codex agent nodes synthesized directly from provider
session logs. No hook registration or `emit` wiring is required. Run
`herdr-top` in a managed Herdr pane, or pass an explicit session and socket as
described in the [CLI reference](cli.md). Optional hooks and manual Controller
events can add subjects and semantic edges, but they never authorize physical
workspace, tab, pane, or execution topology.

## Screen layout

The normal screen has four regions:

- **Header.** Shows the host, named session, elapsed session time, workspace
  count, observation quality, event lag, and source coverage when width permits.
  A `perf:` field appears between `lag:` and `sources:` only when the
  performance snapshot contains at least one reason. Quality is one of `LIVE`,
  `RECONCILING`, `DISCONNECTED`, or `DEGRADED`.
- **Execution tree or dependency DAG.** The execution tree places Task Runs
  beneath their current workspace, tab, and pane. The DAG view is a stable list
  with Task Run, prerequisite, and dependent columns. `Tab` switches views.
- **Activity for selected item.** Shows persistence and Controller status, the
  selected row, selection recovery status, and the newest normalized activity
  item in the selected scope.
  Its first line uses `p:` for persistence, `ctl:` for Controller input, and
  `D4:` for the count of dangling relationship-only components (a single
  component may span several runs).
- **Footer.** Shows controls that fit the current terminal width. Its wording
  distinguishes stopping herdr-top with `q` from detaching the Herdr client.

The minimum rendered size is 48 columns by 14 rows. A smaller terminal displays
only a size warning.

## Overlays and filter editor

Four overlays can replace the center of the screen:

- **Setup notice.** On an owner TUI launch, a standalone binary version mismatch
  or unavailable standalone probe can produce a dismissible notice. Basic
  monitoring continues. `Enter` or `Esc` dismisses the notice and records its
  marker on a best-effort basis.
- **Selected detail (`i`).** Shows the selected entity's identifiers and state,
  plus up to 100 recent activity items in its scope. Task Run detail includes
  `native_lifecycle_end`, `lifecycle_watermark`, `history_ready`,
  `rate.measured_output_tokens`, `rate.measured_working_ms`, and
  `rate.cursor_initialized`, as well as
  `tokens.output`, `tokens.input`, `tokens.cached_input`,
  `tokens.cache_write_input`, `tokens.reasoning_output`, `tokens.total`, and
  `tokens.context_window`, relationship lines named `dispatch_parent`,
  `prerequisites`, `dependents`, and `task_relationships`, followed by
  `scope: semantic run and agent descendants`. Unreported fields use a
  not-reported-style placeholder rather than zero. Resumed Codex rollouts also
  show per-turn model, effort, and sandbox history.
- **Summary (`s`).** Groups published, history-ready Task Runs by worker kind
  and model. It reports
  run and live counts, valid terminal-run total and mean durations, accumulated
  output-token totals, and a weighted output-token rate: total measured output
  tokens divided by total measured Working seconds. Summary includes every
  published, history-ready run still retained by the store, including terminal
  history hidden from the default tree. A token field uses its placeholder only
  when the required telemetry is unavailable.
- **Help (`?`).** Shows the key map and current runtime diagnostics, including
  persistence, Controller input, source coverage, and the standalone probe.

The `/` key opens a footer editor rather than an overlay. Filtering is a
case-insensitive Unicode-lowercase substring match over safe identity and state
fields. It deliberately excludes paths, activity, Controller free text,
content, and raw events. A tree match retains its ancestors; a DAG match retains
its prerequisite paths. Filtering operates only within the default-visible row
set and does not restore expired or dismissed retained history.

## Keybindings

### Normal view

| Key | Action |
| --- | --- |
| `q` | Stop herdr-top and return to the shell. Monitored agents continue. |
| `Up` / `Down` | Move the selection one visible row and leave follow mode. |
| `f` / `End` | Resume follow mode, selecting and pinning the viewport to the newest visible row. |
| `Tab` | Switch between the execution tree and dependency DAG while preserving the closest semantic selection. |
| `/` | Open the filter editor with the current query. |
| `Left` | In an unfiltered execution tree, collapse an expanded branch; otherwise move to its parent. |
| `Right` | In an unfiltered execution tree, expand a collapsed branch; otherwise move to its first child. |
| `Enter` | Toggle the selected branch in an unfiltered execution tree. |
| `i` | Open the Selected detail overlay. |
| `s` | Open the Summary overlay when pressed without modifiers. |
| `?` | Open the Help overlay. |
| `c` | Without modifiers, persistently dismiss every terminal run and every hook-only run that has reached its 24-hour visibility boundary. |
| `Esc` | No action in the normal view; it cancels or closes the active interaction described below. |

Collapse input is ignored while filtering and in the dependency DAG. Collapsed
state is retained when switching views. Manual selection, collapse, and a
committed filter leave follow mode; `f` or `End` returns to it.

### Filter editor

| Key | Action |
| --- | --- |
| Character keys | Append characters to the draft query. |
| `Backspace` | Remove the last character. |
| `Enter` | Trim and commit the query. An empty query clears filtering. |
| `Esc` | Cancel the draft and retain the previous query. |

### Overlays

| Overlay | Close keys | Other keys |
| --- | --- | --- |
| Setup notice | `Enter`, `Esc` | Other keys are ignored. |
| Help | `?`, `Esc` | `Up` and `Down` scroll. |
| Selected detail | `i`, `Esc` | `Up` and `Down` scroll. |
| Summary | `s`, `Esc` | `Up` and `Down` scroll. |

While a filter draft or overlay is active, its key handling takes precedence
over normal-view bindings. In particular, a character typed in the filter is
text, and `q` does not stop the monitor from an overlay.

Herdr detach is not a herdr-top keybinding. Detaching leaves herdr-top running
with the session; reattaching returns to it. Pressing `q` stops only herdr-top,
never the Claude Code or Codex processes it observes.

## Reading the execution tree

### Identity, history, and ordering

Herdr Top keeps exactly one Task Run for each `(provider, native session ID)`.
Resuming the same native session preserves its run ID, row, and immutable
display ordinal. A different native session observed in the same pane creates a
new root below the old one instead of replacing it. Task Run and Agent Node
siblings at every depth sort by ascending display ordinal, so status changes do
not reorder history.

Follow mode selects and pins the viewport to the last visible row. Manual
selection, collapse, or a committed filter leaves follow mode; `f` or `End`
resumes it.

The physical levels use these row prefixes:

```text
Session: <session>
Workspace: <workspace-id>
Tab: <tab-id> (<Herdr label>)
Tab: <tab-id>
Pane: <pane-id> (<Herdr label>)
Pane: <pane-id>
```

Tab and pane names come only from their sanitized Herdr labels. Terminal titles
never occupy the pane-name slot. A complete snapshot is authoritative: a null,
empty, or sanitized-empty label clears the current and persisted name. A partial
raw event with an omitted, empty, or sanitized-empty label preserves the current
value until a complete snapshot replaces it. Parentheses appear only when the
resulting label is present and non-empty; an absent label renders no `()` at all.
Unicode connectors show sibling structure:

```text
├── non-final child
└── final child
│   continuing ancestor
```

Set `HERDR_TOP_ASCII_TREE=1` before launch to replace only those connectors with
the following ASCII forms; every other value leaves Unicode connectors enabled.

```text
|-- non-final child
`-- final child
|   continuing ancestor
```

A Task Run follows this grammar:

```text
<glyph> <status> <worker-kind>[ <subject>][ — <live line>][ · <duration when TIME is hidden>][ relationship annotations]
```

- **Glyph and status** are redundant operational cues. The written status stays
  at the left edge so narrow-width truncation drops subject and live detail
  before it drops the operational state.
- **Worker kind** comes from the projected run kind, falling back to the native
  provider name, the selector from a hook-backed Controller key, another
  Controller key, or `provisional`. The run-kind source priority is normative
  and described under [Run kind and primary-row identity](#run-kind-and-primary-row-identity).
- **Subject** is the captured task subject. A provider-backed run with no
  captured subject renders the kind alone; it never falls back to a native
  session ID, hook session ID, or path-derived run UUID. A Codex child — a
  Codex-backed run below an execution edge — renders the kind alone even when a
  subject was captured. Only a run with no provider backing (a plain Controller
  key or a provisional key) still uses its key-derived fallback rather than
  leaving the segment empty.
- **Live line** appears only on a non-terminal run. It comes from the log-lane
  live-line read model, or, for a Claude-flavoured run, from the newest Agent
  Node's last event kind with a `: <tool>` suffix when a tool name is present.
- **Duration** appears in the label when the start and live or finished endpoint
  form a non-negative interval and the row has no visible `TIME` metric column.
  DAG rows have no metric band, so they retain the label suffix. Live durations
  use the current time; terminal runs use their recorded finish time.

### Task Run status source precedence

Terminal `TaskState` facts win over every runtime observation:

| Terminal `TaskState` | Status |
| --- | --- |
| `completed` | `done` |
| `failed` | `error` |
| `cancelled` | `cancelled` |
| `ended_unknown` | `done` from durable exact-native ended evidence, otherwise `unknown` |

Persisted native-session lifecycle evidence is next when semantic state is not
terminal:

| Native lifecycle end | Status |
| --- | --- |
| `Done` | `done` |
| `Error` | `error` |
| `Cancelled` | `cancelled` |
| `Unknown` | `done` from durable exact-native ended evidence, otherwise `unknown` |

Durable exact-native ended evidence is presentation-only. Only an Agent Node
whose provider and nonempty native session ID both exactly match one of the
Task Run's `RunKey::Native` aliases can supply it, and only when it is the
newest such node by `(last_activity_at_ms, agent_node_id)` and its state is
exactly `ended`. Agent ownership and `parent_agent_node_id` do not identify
the target Task Run: a real Codex child completion remains a parented Agent
Node owned by its controller/root run. A synthetic live-line node, a foreign
provider, a different session ID, or an older ended node followed by a newer
non-ended exact node cannot refine `unknown`. The refinement applies only to
semantic `ended_unknown` and nonterminal native lifecycle `Unknown`; it
reports `done` with source `agent_node_state`. Definitive semantic
`completed`, `failed`, and `cancelled` and native `Done`, `Error`, and
`Cancelled` keep their existing status and source, and a still-running Task
Run never becomes `done` from this evidence. No semantic or native lifecycle
record is changed, and the Agent Node row and live-line fallback still
disappear at the existing staleness deadline.

Semantic pre-start and block facts follow:

| Semantic `TaskState` | Status |
| --- | --- |
| `queued` | `queued` |
| `blocked` | `blocked` |

For a pane-backed occurrence, the current status for that exact pane is the
next source. Shared Task Runs can therefore display a different status in each
pane occurrence. Herdr maps one-to-one:

| Herdr pane status | Status |
| --- | --- |
| `working` | `working` |
| `idle` | `idle` |
| `blocked` | `blocked` |
| `done` | `done` |
| `unknown` | `unknown` |

The collector accepts exactly `pane.agent_status_changed` and the legacy
`pane_agent_status_changed` spelling for pane-status events. They update the
same pane-status map; other spellings are ignored.

An explicit Herdr `unknown` is evidence, not absence. Only a missing pane
status falls back to the execution matching that pane:

| Matching execution state | Status |
| --- | --- |
| `working` | `working` |
| `idle` | `idle` |
| `blocked` | `blocked` |
| `unknown` | `unknown` |
| `stale` | `unknown` plus stalled |
| `ended` | `unknown`, unless terminal `TaskState` already won |

A headless Task Run consults only its newest non-display-stale,
provider-matching root Agent Node, never descendants:

| Root Agent Node state | Status |
| --- | --- |
| `working` | `working` |
| `idle` | `idle` |
| `blocked` | `blocked` |
| `stale` | `unknown` plus stalled |
| `ended` | `unknown` |
| absent or `unknown` | fall through |

After that fallthrough, `TaskState::Running` may finally supply `working`.
Without applicable evidence, the status is `unknown`.

Stalled is orthogonal to status. A stalled non-terminal row keeps its base
status word and uses `⚠` in place of the normal glyph; terminal rows are never
stalled.

| Status | Glyph | Meaning |
| --- | --- | --- |
| `queued` | `◌` | announced |
| `working` | `●` | active |
| `idle` | `○` | waiting |
| `blocked` | `●` | needs attention |
| `done` | `✓` | finished |
| `error` | `✗` | failed |
| `cancelled` | `⊘` | stopped |
| `unknown` | `?` | insufficient evidence |
| stalled override | `⚠` | orthogonal warning; written base status is retained |

Relationship annotations are appended in this order when applicable:

- `[shared]` means the same Task Run has live executions in more than one pane,
  so the run appears under each hosting pane. Its descendants expand only on
  the first occurrence.
- `[dispatched by: ...]` appears only on a pane-placed run and names its dispatch
  parent by the parent's stable run kind, then its captured subject, then its
  key-derived worker kind; it never uses the parent's native session, hook
  session, or run identity, and it reads the same before and after
  Controller/native alias convergence. A run with no execution history instead
  nests physically beneath its default-visible dispatch parent and carries no
  textual parent hint. A hidden or expired parent never hides a child: the
  child falls back to `Unattached` for that frame. A malformed parent cycle
  also falls back to `Unattached`.

Rows do not append a visible annotation for the absence of relationships.
Selected Task Run Detail reports `dispatch_parent`, `prerequisites`,
`dependents`, and `task_relationships: none | present`. The filter accepts
`unlinked` as a legacy synonym for "no recorded task relationships." Herdr Top
does not infer a relationship from neighboring panes or shared paths.

Placement follows live execution panes first, then the pane of the latest ended
execution, then a default-visible dispatch parent for a run with no execution
history, and finally `Unattached`. A shared run repeats beneath every live
hosting pane, while nested descendants appear only beneath its first occurrence.

### Metric columns and narrow panes

Task Run metrics are right-aligned in fixed-width columns at the end of each
tree row. These names describe the columns; the TUI does not render a header
row.

| Documentation name | Width | Value |
| --- | ---: | --- |
| `MODEL` | 11 | Current model, or an em-dash placeholder when unavailable. |
| `EFF` | 5 | Current effort, or an em-dash placeholder when unavailable. |
| `TOK` | 5 | Accumulated output tokens only, or an em-dash placeholder. |
| `TOK-S` | 5 | Measured output tokens divided by measured Working seconds, or an em-dash placeholder. |
| `TIME` | 6 | Run duration. |

When a selected execution-tree band includes `TIME`, the row suppresses the
label's `· <duration>` suffix so the duration is painted exactly once. Below
inner width 62 the metric band has no `TIME`, so the suffix remains; dependency
DAG rows also retain it because that view does not paint metric columns.

For `TOK-S`, the numerator starts after a trustworthy live baseline and counts
only later positive increases in the cumulative output-token counter. The
denominator is the union of reliably observed run-level Working intervals:
multiple pane occurrences are ORed, so concurrent Working observations do not
double-count time. Idle, blocked, queued, unknown, terminal, historical replay,
reconnect, reconciliation, and offline intervals add no time. A delayed token
increase observed after a transition to Idle still enters the numerator once,
without adding Idle time.

Cold restore, observation gaps, reconciliation, queue-overflow recovery,
identity-basis changes, historical input, and counter regression discard the
process-local cursor and establish a new baseline before measurement resumes.
Missing totals or zero measured Working time render an em dash. Positive
persisted totals remain renderable after restore even though the rate cursor is
not restored. `TOK` remains the lifetime total and is separate from this
measured numerator. Summary divides aggregate measured tokens by aggregate
measured Working time; it never averages per-run rates.

Column bands are selected from the `Execution tree` pane's inner width, after
subtracting its two border columns, rather than from raw terminal width:

| Tree inner width | Visible columns |
| ---: | --- |
| 120 or wider | `MODEL EFF TOK TOK-S TIME` |
| 104-119 | `EFF TOK TOK-S TIME` |
| 90-103 | `TOK TOK-S TIME` |
| 76-89 | `TOK TIME` |
| 62-75 | `TIME` |
| Below 62 | None |

The resulting drop order is `MODEL`, `EFF`, `TOK-S`, `TOK`, then `TIME` as the
pane narrows. Label text is truncated and deep indentation is compressed before
the active band's columns disappear at the next threshold. Columns are joined
with one space; the all-five band reserves 36 columns for metrics plus one
separator from the padded label. Selection reversal preserves readable status
text and its status color.

Native agent rows use this form:

```text
<glyph> <status> <Claude|Codex> native agent[: <role>] [model:<model>] [last:<timestamp>ms]
```

`<role>` is the sanitized stable run kind of the Task Run the Agent Node
represents. That run is resolved by exact native alias first: an Agent Node
whose provider and nonempty native session ID exactly match a `RunKey::Native`
binding renders that run's kind, so a Codex child Agent Node owned by its root
run reports the child's role (`Codex native agent: worker`). Otherwise the
owning Task Run applies only when it has no native binding of its own — a
Controller-keyed Claude subagent run renders `Claude native agent: reviewer`
from its owning run kind, while an unmatched Agent Node under a natively bound
owner renders `Codex native agent` with no colon and no identity. The native
session ID and Agent Node ID never appear in the row; Detail carries them.
Model and activity annotations are unchanged.

Visible native child Agent rows use their own evidence: `working`, `idle`, and
`blocked` map one-to-one; `ended` maps to `done`; `stale` maps to `unknown` plus
stalled; and absent or `unknown` maps to `unknown`. They never invent `queued`,
`error`, or `cancelled`.

### Run kind and primary-row identity

The primary surface — every Task Run row, Agent row, and `[dispatched by: ...]`
annotation in the execution tree and DAG — contains no provider session ID,
hook session ID, run UUID, or Agent Node ID. Identity lives in the Selected
detail overlay, which keeps the full key, `run_id`, bound `native_session_id`,
`agent_node_id`, `parent_agent_node_id`, and `dispatch_parent` unchanged. The
DAG prerequisite and dependent columns and the filter's searchable identity
fields are unchanged by this rule.

A run is **provider-backed** when its primary key is `RunKey::Native` or
`RunKey::NativePath`, when its Controller key carries a recognized hook selector
(`hook:claude-code:` or `hook:codex:`), or when an exact native or native-path
binding in the task-run bindings resolves to it. The last rule keeps a
Controller-primary run provider-backed after Controller/native alias
convergence, so a Controller-keyed Codex run with a native alias below an
execution edge is still a Codex child and renders its role alone.

The published run kind is the first nonempty value observed for a run; a later
value never overwrites it, and run kinds are never persisted. For a Codex
rollout the log lane selects, in order:

1. the `ThreadSpawn` agent role from the rollout's session metadata;
2. a provider-defined internal-agent name;
3. the rollout originator (for example `codex-tui` or `codex_cli_rs`).

Blank values are skipped so an empty role or name never publishes an empty
kind. The `ThreadSpawn` nickname is never a kind or a subject. The Codex
rollout `agent_role` field is read opportunistically from an unstable external
format and is not a stable public Codex API. A Claude subagent publishes its
`agentType`, and hook-backed runs without a lane kind fall back to the hook
selector.

The provider-native root Agent that duplicates its owning Task Run is hidden
from the tree. A visible descendant whose root is hidden attaches directly
beneath the Task Run. Explicit provider parentage otherwise nests visible Agent
Nodes recursively. Model and Detail data remain retained. An Agent Node whose
state is absent, unknown, or ended becomes display-stale after its recorded last
activity has been silent for `HERDR_TOP_HEADLESS_INACTIVITY_MS`; a node without
an activity timestamp remains visible, as do known states such as `stale` and
`working`. A visible child of any display-stale parent is re-parented beneath
the owning Task Run. These display rules remove no model or SQLite data, and
the Task Run live-line fallback also ignores display-stale Agent Nodes. A
display-stale ended Agent Node nevertheless remains durable exact-native
evidence for the Task Run bound to its exact provider and native session, so
that Task Run's otherwise-unknown row can stay `done` after the Agent row and
its live line are hidden.

The dependency view remains separate: it lists each Task Run with its explicit
prerequisites and dependents, or displays `no dependency edges recorded` when
there are none. Tree and DAG rows share the same glyph-and-status prefix. DAG
rows omit the live line. Across the two views, `[shared]` and
`[dispatched by: ...]` remain wherever the row placement implements them.

## Visibility and dismissal

The default view hides a Controller-keyed run with no execution after 24 hours
without an update. Native-keyed runs and Controller runs that have an execution
do not use this hook-only expiry. Terminal Task Runs at root, child, and
grandchild depth each remain in the default tree for
exactly `DEFAULT_TERMINAL_VISIBILITY_MS`, currently one hour, after their own
terminal observation. They then become default-hidden while remaining retained
in SQLite. Row projection chooses the default-visible IDs before applying a
filter, so filtering does not restore an expired row and Detail cannot directly
select a row absent from that projection. Once the row is published and
history-ready, Summary continues to include it until ordinary retention removes
it. An expired ancestor remains as a structural row whenever an individually
visible descendant needs it to preserve the tree path.

Pressing `c` persistently dismisses all currently terminal runs and hook-only
runs that have already reached the 24-hour boundary. It does not delete them,
and the dismissal survives a restart. A later non-terminal mutation clears the
dismissal, while a terminal touch retains it. A dismissed row likewise remains
in SQLite and, when published and history-ready, Summary, but filtering does not
restore it for direct selection or Detail.

Provider-native lifecycle is separate from semantic Task state. A normal
`SessionEnd` records lifecycle `Done`; an explicit provider abort records
`Cancelled`, an explicit failure records `Error`, and disappearance without a
stronger fact records `Unknown`. These facts do not write semantic completion,
failure, cancellation, or dismissal. In particular, Codex turn completion is
runtime Idle only. A lifecycle end uses the same one-hour visibility rule as a
semantic terminal row.

A later matching `task_started`, live execution, or provider liveness fact can
clear native lifecycle evidence without reopening a semantic terminal Task.
Lifecycle ordering compares trustworthy source time, then collector observation
time, then a stable source or event identity; an older delayed fact cannot
re-close a newer resume. Repeating the same watermark and status is idempotent.
A `SessionEnd` for an unknown or unbound native session is a diagnostic no-op.
Operator dismissal with `c` remains an independent visibility action.

## Restart and backfill

At startup, each provider freezes a sorted artifact manifest with stable
artifact identities, generations, and byte goalposts. Historical events carry
that drain identity, and run associations spill to SQLite instead of remaining
in an unbounded in-memory set. A run created solely by history is persisted with
`history_ready = false`, so intermediate Working states never enter the default
tree.

After every artifact reaches its frozen goalpost, the provider enqueues one
ordered barrier behind all pending output and pauses. One SQLite transaction
marks the drain complete, makes its historical runs ready, and gives otherwise
non-live, nonterminal history an `Unknown` native lifecycle end. The reducer
publishes that coalesced page only after the commit is known durable or known
committed despite a classified degradation. Incomplete, failed, interrupted, or
durability-unconfirmed drains remain suppressed. Retrying a barrier or replaying
a completed manifest is idempotent. Old completed history can therefore appear
in Summary without flashing a stale Working row in the default tree.

Schema v8 is restored after the mandatory pre-migration SQLite online backup.
Schema v6 persists native lifecycle ends and watermarks, history readiness and
drain associations, and per-run measured token and Working-time totals. Schema
v7 adds durable publication state, event before-images, and drain provenance.
Schema v8 rebuilds `agent_nodes` so an agent node's `ended` state is persisted
and restored alongside `working` and `NULL`, preserving existing rows and
fields, both foreign keys, and the parent index. Existing runs migrate
history-ready with no synthetic lifecycle or rate row. Pane status and rate
cursors remain process-local; a cold restore rebaselines before new accrual,
while already-persisted positive measured totals remain usable immediately.

## Liveness watchdog

If the Herdr event subscription is silent for 30 seconds, herdr-top leaves that
subscription open and requests a fresh session snapshot through the same socket
endpoint. A matching snapshot is healthy idle and simply rearms the deadline;
an ambiguous snapshot is recorded as inconclusive and also rearms it. A request,
timeout, decode, conversion, or divergent-topology result triggers a reconnect,
records an observation gap, and runs the normal subscribe-buffer-snapshot
reconciliation sequence. Reconnect delay backs off from 1 second to at most 60
seconds and resets after a subscription event, allowing a silently dead stream
to recover without treating every quiet session as disconnected. Snapshot names
are compared exactly: a probed null differs from a current non-null name and is
not rewritten from the current model before comparison.
