# TUI guide

herdr-top is a fixed-screen terminal monitor for one resolved Herdr session. It
combines Herdr's physical workspace, tab, and pane topology with Task Runs and
native Claude Code or Codex agent nodes synthesized directly from provider
session logs. No hook registration or `emit` wiring is required. Run
`herdr-top` in a managed Herdr pane, or pass an explicit session and socket as
described in the [CLI reference](cli.md).

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
  `tokens.output`, `tokens.input`, `tokens.cached_input`,
  `tokens.cache_write_input`, `tokens.reasoning_output`, `tokens.total`, and
  `tokens.context_window`, followed by
  `scope: semantic run and agent descendants`. Unreported fields use a
  not-reported-style placeholder rather than zero. Resumed Codex rollouts also
  show per-turn model, effort, and sandbox history.
- **Summary (`s`).** Groups all Task Runs by worker kind and model. It reports
  run and live counts, valid terminal-run total and mean durations, accumulated
  output-token totals, and a weighted output-token rate: total rated output
  tokens divided by total rated elapsed seconds. A token field uses its
  placeholder only when the required telemetry is unavailable.
- **Help (`?`).** Shows the key map and current runtime diagnostics, including
  persistence, Controller input, source coverage, and the standalone probe.

The `/` key opens a footer editor rather than an overlay. Filtering is a
case-insensitive Unicode-lowercase substring match over safe identity and state
fields. It deliberately excludes paths, activity, Controller free text,
content, and raw events. A tree match retains its ancestors; a DAG match retains
its prerequisite paths. Filtering also reveals retained rows that the default
visibility rules hide.

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
<glyph> <worker-kind>[ <subject>][ — <live line>][ · <duration when TIME is hidden>][ annotations]
```

- **Glyph** carries the lifecycle state, with the stall override described
  below.
- **Worker kind** comes from the projected run kind, falling back to the native
  provider name, the selector from a hook-backed Controller key, another
  Controller key, or `provisional`.
- **Subject** is the captured task subject. If none exists, the renderer uses a
  key-derived identity rather than leaving the segment empty. A native or
  native-path Codex run that is the child of an execution edge renders the kind
  alone and suppresses the subject.
- **Live line** appears only on a non-terminal run. It comes from the log-lane
  live-line read model, or, for a Claude-flavoured run, from the newest Agent
  Node's last event kind with a `: <tool>` suffix when a tool name is present.
- **Duration** appears in the label when the start and live or finished endpoint
  form a non-negative interval and the row has no visible `TIME` metric column.
  DAG rows have no metric band, so they retain the label suffix. Live durations
  use the current time; terminal runs use their recorded finish time.

The glyph vocabulary is:

| Glyph | Meaning |
| --- | --- |
| `⚠` | A non-terminal run whose activity silence has crossed the stall threshold. The override never replaces a terminal glyph. |
| `●` | `running` or `blocked`. |
| `✓` | `completed`. |
| `✗` | `failed` or `cancelled`. |
| `◌` | `queued` or `ended_unknown`. |

Relationship annotations are appended in this order when applicable:

- `[shared]` means the same Task Run has live executions in more than one pane,
  so the run appears under each hosting pane. Its descendants expand only on
  the first occurrence.
- `[dispatched by: ...]` appears only on a pane-placed run and names its dispatch
  parent. A run with no execution history instead nests physically beneath its
  default-visible dispatch parent and carries no textual parent hint. A hidden
  or expired parent never hides a child: the child falls back to `Unattached`
  for that frame. A malformed parent cycle also falls back to `Unattached`.
- `[unlinked]` means no execution or dependency edge links the Task Run. Herdr
  Top does not infer a relationship from neighboring panes or shared paths.

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
| `TOK-S` | 5 | Output tokens divided by elapsed seconds, or an em-dash placeholder. |
| `TIME` | 6 | Run duration. |

When a selected execution-tree band includes `TIME`, the row suppresses the
label's `· <duration>` suffix so the duration is painted exactly once. Below
inner width 62 the metric band has no `TIME`, so the suffix remains; dependency
DAG rows also retain it because that view does not paint metric columns.

For `TOK-S`, elapsed time starts at the run's log-time anchor. It ends at the
current time for a live run and at `finished_at_ms` for a terminal run, so the
rate freezes at completion. Tokens, an anchor, and a positive elapsed interval
are all required.

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
separator from the padded label.

Native agent rows use this form:

```text
<Claude|Codex> native agent: <native-session-or-node-id> [state:<state>] [model:<model>] [last:<timestamp>ms|unknown]
```

Agent nodes nest recursively only where provider metadata establishes a parent.
An Agent Node whose state is absent, unknown, or ended is hidden after its
recorded last activity has been silent for `HERDR_TOP_HEADLESS_INACTIVITY_MS`.
A node without an activity timestamp remains visible, as do known states such
as `stale` and `working`. When a hidden parent has a visible child, the child is
re-parented directly beneath the owning Task Run for display. This rule removes
no model or SQLite data, and the Selected detail projection continues to use
the complete, unfiltered Agent Node model. The Task Run live-line fallback
likewise ignores display-stale Agent Nodes.
The dependency view remains separate: it lists each Task Run with its explicit
prerequisites and dependents, or displays `no dependency edges recorded` when
there are none. DAG rows share the status-glyph and label grammar, but omit the
live line.

## Visibility and dismissal

The default view hides a Controller-keyed run with no execution after 24 hours
without an update. Native-keyed runs and Controller runs that have an execution
do not use this hook-only expiry. Terminal runs remain in the default view for
one hour after their first terminal observation, then remain retained and
available to filtering.

Pressing `c` persistently dismisses all currently terminal runs and hook-only
runs that have already reached the 24-hour boundary. It does not delete them,
and the dismissal survives a restart. A later non-terminal mutation clears the
dismissal, while a terminal touch retains it.

A provider `SessionEnd` hook dismisses its known session run immediately without
changing the Task Run state or advancing its activity time. The dismissal is
persisted. If that native session resumes, its `SessionStart` becomes
`task_started`; ordinary non-terminal bookkeeping clears the dismissal and the
run returns to the default view. A `SessionEnd` for an unknown run creates
nothing.

## Restart and backfill

At startup, herdr-top re-reads every admitted provider artifact selected by the
backfill window from byte zero; it does not restore a per-file byte offset. The
selection anchor is the later of the earliest database event and
`now - HERDR_TOP_BACKFILL_WINDOW_MS`. The window selects files, not records, so
every selected artifact is read in full and its run totals are complete.
Pane-root artifacts are exempt from the anchor. Lineage evidence admits only
artifacts whose mtime satisfies the anchor; an older identity echo is ignored
entirely.

Replay is idempotent through the durable event ledger. Token telemetry,
subjects, run kind, and per-turn context are transient and are recomputed from
the artifacts rather than restored from SQLite; token totals therefore return
after startup backfill instead of being persisted.

One fail-safe limitation remains. If a session completed and then resumed, and
both halves arrive in one backfill pass, the row remains `completed` until
genuinely new activity appears. The reopen gate requires the resume's
source-clock timestamp to be strictly newer than `finished_at_ms`, but replay
records the historical completion using the current receipt clock. The
historical resume is therefore older and is denied. Denial avoids a false
reopen on every restart; a durable correction requires a source-clock
completion timestamp and a schema change.

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
