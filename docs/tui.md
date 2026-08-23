# TUI guide

herdr-top is a fixed-screen terminal monitor for one resolved Herdr session. It
combines Herdr's physical workspace, tab, and pane topology with semantic Task
Runs and native Claude Code or Codex agent nodes. Run `herdr-top` in a managed
Herdr pane, or pass an explicit session and socket as described in the
[CLI reference](cli.md).

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
  `D4:` for the dangling announcement count.
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
  plus up to 100 recent activity items in its scope.
- **Summary (`s`).** Groups all Task Runs by worker kind and model. It reports
  run and live counts, valid terminal-run total and mean durations, and `-` for
  token fields that are not yet populated.
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
Tab: <tab-id> (<captured label>)
Pane: <pane-id> (<captured display name>)
```

The names in parentheses appear only when a captured name is available. Unicode
connectors show sibling structure:

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
<worker-kind> <subject> — <event-kind>: <tool> [model:<model>] [<status>] · <duration> <annotations>
```

- **Worker kind** comes from the run key: a native provider name, the selector
  from a hook-backed Controller key, another Controller key, or `provisional`.
- **Subject** is the captured task subject. If none exists, the renderer uses a
  key-derived identity rather than leaving the segment empty.
- **Activity** appears only for a non-terminal run whose newest Agent Node has
  a last event kind. The `: <tool>` suffix appears only when that node also has
  a tool name.
- **`[model:...]`** appears when that same newest Agent Node has a model ID.
- **Status** always appears and can be `queued`, `running`, `blocked`,
  `completed`, `failed`, `cancelled`, or `ended_unknown`.
- **Duration** appears when the start and live or finished endpoint form a
  non-negative interval. Live durations use the current time; terminal runs use
  their recorded finish time.

Relationship annotations are appended in this order when applicable:

- `[shared]` means the same Task Run has live executions in more than one pane,
  so the run appears under each hosting pane.
- `[dispatched by: ...]` names the explicit Controller dispatch parent. It does
  not re-parent the physical tree row.
- `[unlinked]` means no execution or dependency edge links the Task Run. Herdr
  Top does not infer a relationship from neighboring panes or shared paths.

Native agent rows use this form:

```text
<Claude|Codex> native agent: <native-session-or-node-id> [state:<state>] [model:<model>] [last:<timestamp>ms|unknown]
```

Agent nodes nest recursively only where provider metadata establishes a parent.
The dependency view remains separate: it lists each Task Run with its explicit
prerequisites and dependents, or displays `no dependency edges recorded` when
there are none.

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

## Liveness watchdog

If the Herdr event subscription is silent for 30 seconds, herdr-top leaves that
subscription open and requests a fresh session snapshot through the same socket
endpoint. A matching snapshot is healthy idle and simply rearms the deadline;
an ambiguous snapshot is recorded as inconclusive and also rearms it. A request,
timeout, decode, conversion, or divergent-topology result triggers a reconnect,
records an observation gap, and runs the normal subscribe-buffer-snapshot
reconciliation sequence. Reconnect delay backs off from 1 second to at most 60
seconds and resets after a subscription event, allowing a silently dead stream
to recover without treating every quiet session as disconnected.
