# Increment 8: TUI correctness and readability — design

Status: user-approved design (2026-08-22). Implementation starts only after
Increment 7 (packaging) is fully integrated, because two fixes touch files
Increment 7 tasks also modify (`src/herdr/wire.rs`, `src/hook_adapter.rs`).

## Problem statement

A live-instance diagnosis (report:
`~/.research/mageyuki--herdr-top/herdr-top-ui-fixes/diagnosis-attach-bugs.md`)
confirmed four defects and a set of readability gaps:

1. The collector has no liveness mechanism. A herdr event stream that stays
   connected but silent freezes the whole topology model permanently. In the
   diagnosed instance the model had not advanced for 26 hours while the
   socket stayed ESTAB and a fresh subscriber on the same socket received
   events normally. Task runs created by hooks therefore pile up under
   "Unattached Task Runs" (they carry no pane binding of their own and can
   only be attached by herdr pane observations), and a stale execution kept
   rendering a previous session's task run under a pane whose agent session
   had long since changed.
2. The `pane_agent_detected` collector arm only accepts the legacy nested
   `pane` object shape. Live herdr 0.8.2 (protocol 20) emits a flat frame
   (`{"agent": ..., "pane_id": ..., "type": "pane_agent_detected",
   "workspace_id": ...}`), which the arm silently drops. Confirmed real but
   not the operative cause of defect 1.
3. `run_for_native_session` (`src/reducer.rs`) resolves runs by
   `RunKey::Native` and by agent nodes but never consults the
   `task_runs.native_session_id` binding column. A snapshot-driven rebind can
   therefore mint a second run claiming a native session id that the unique
   index `task_runs_native_session_binding_unique` already covers.
4. Hook-originated task runs never terminate: the `SessionEnd` hook is
   deliberately mapped to zero events, runs that carry a controller task
   state event refuse observation-based closure, and non-terminal runs are
   always visible. The combination accumulates dead runs forever.

Readability gaps (user-reported): task run rows are UUID noise; the physical
tree shows containment only with 2-space indentation; tab/pane names are not
shown; the dependency DAG view renders indistinguishably from an empty list
when no edges exist; the footer truncates blindly on narrow panes; there are
no timing metrics anywhere; there is no way to clear finished or dead runs
from the display.

## Goals

1. The model self-heals from a silent event-stream freeze without restart.
2. Protocol-20 flat `pane_agent_detected` frames are ingested.
3. Snapshot-driven rebinding cannot violate the native-session unique index.
4. Hook-originated runs reach a terminal state on `SessionEnd`, and
   crash-orphaned runs leave the default view after a bounded time.
5. Task run rows read as work items, not UUIDs.
6. The physical tree, tab/pane names, DAG view, and footer are legible.
7. Session and per-run timing is visible; a summary overlay aggregates it.
8. A clear key dismisses finished and expired runs persistently.

## Non-goals

1. Token counts, throughput (tok/s), and reasoning effort capture. These
   require extending the provider-log privacy allowlist and an ADR change;
   they are deferred to Increment 9. This increment only leaves UI room for
   them.
2. Representing dependencies by nesting in the execution tree. The design
   doc's rule stands: the tree is the physical tree; dependencies belong to
   the DAG view and to annotations.
3. Prompt or response excerpts in any UI surface (unchanged privacy rule).
4. Deleting history. Every new "hide" mechanism retains rows in SQLite.

## Design

### 1. Collector liveness watchdog

The collector records the receive time of the last herdr-sourced event.
When no herdr event has arrived for `HERDR_LIVENESS_TIMEOUT` (default 30
seconds; injectable for tests and configuration), the collector:

1. records a diagnostics observation naming the silent interval,
2. drops and re-establishes the subscription socket,
3. requests a full snapshot,
4. reconciles through the existing observation-gap machinery (the same path
   startup uses), retiring executions the snapshot no longer supports.

Reconnect failures back off (bounded exponential, capped at 60 seconds) and
each attempt is recorded. The watchdog must be driven by an injected clock
so tests are deterministic. Periodic herdr traffic (for example pong replies)
counts as liveness only if it is delivered through the same event stream
being watched; the timer measures the stream, not the socket.

### 2. `pane_agent_detected` tolerance

Every collector arm that handles `pane_agent_detected` (there are two —
the main dispatch arm and the resync-admission path) accepts both shapes:
the legacy nested `pane` object keeps its current full-upsert behavior,
and the protocol-20 flat frame (top-level `pane_id` / `workspace_id` /
`agent`, no `pane` object, hence no `agent_session`) is accepted without
error: it counts as stream liveness and is recorded via the collector's
counter mechanism, but derives no topology mutation of its own — the
paired full `pane_updated` frame and the watchdog's snapshot path carry
the actual binding. Both shapes are covered by tests using frames captured
from real traffic. Whether herdr always pairs the flat frame with a full
`pane_updated` is not assumed: if it does not, the watchdog's periodic
re-snapshot still converges the model.

### 3. Native-session binding lookup

`run_for_native_session` consults, in order: `RunKey::Native`, the
`task_runs.native_session_id` binding column, then agent nodes. A run bound
to the session id by column is returned instead of minting a new run. A
regression test drives the previously fatal path (controller-keyed run with
a column binding, snapshot re-observation of the same sid).

### 4. Hook run lifecycle

1. `SessionEnd` auto-dismisses the session's run: it maps to a new
   controller `dismiss` event that sets `dismissed_at_ms` on the run
   identified by the hook's session identity. It deliberately does NOT
   produce a terminal task state: the reducer rejects `task_started` on
   terminal runs as stale (its replay protection), so a terminal mapping
   would make resumed sessions permanently invisible. Because any new
   activity clears a dismissal, a resumed session (`SessionStart` on the
   same id) reappears naturally. The decision and the rejected
   alternatives (terminal mapping with a reopen rule; expiry-only) are
   recorded in a new ADR (`docs/adr/`), and the mapping table in
   `docs/guides/controller-emit-setup.md` is updated.
2. Default-visibility expiry: a hook-only run (zero executions) that is
   non-terminal counts as expired when `now - last_activity >= 24h`; expired
   runs leave the default view but remain in the store and remain reachable
   via filtering. Any new activity resets the timer and restores visibility.
3. The existing rules for runs with executions are unchanged.

### 5. Task run row readability

A task run row renders as:

    <worker-kind> <subject> — <activity> [model] [status] · <duration>

1. `worker-kind` derives from the run key and provider (for example
   `claude`, `codex`, controller-source names from the hook adapter).
2. `subject` is the sanitized controller label (`task_subject` from the
   `TaskCreated` hook, existing 256-byte cap) — a static one-line anchor
   set once at dispatch. The label is copied onto the `TaskRun` (new
   field) when an event carries one; absent a label the row falls back to
   the current key-derived name. Free-text activity excerpts remain
   prohibited in this increment (unchanged privacy rule). The agreed end
   state is Claude-Code-style live activity text: Increment 9's allowlist
   revision (the same one that adds token metrics) is expected to design a
   bounded activity-excerpt channel, and this row layout keeps the activity
   slot so excerpts can replace the structured suffix without reshaping the
   row.
3. `activity` is a live, structured suffix rebuilt from the run's newest
   agent-node observation: `last_event_kind` and, when present,
   `last_tool_name` (for example `tool_use: Bash`). It updates on each
   event; terminal runs drop it.
4. `model` comes from the run's newest agent node with a `model_id`.
5. `status` is the existing task state, untruncated.
6. `duration` is `finished_at - created_at` for terminal runs and
   `now - created_at` for live ones (see section 8 for the data source),
   updating as time passes for live runs.
7. Full identifiers (UUIDs, run keys) move to the Detail overlay (`i`),
   which gains the run's identity block.

### 6. Physical tree glyphs

The execution tree renders `├──` / `└──` / `│` connectors reflecting the
existing containment hierarchy (Session → Workspace → Tab → Pane → Task Run
→ agent nodes). Nesting semantics are unchanged; dispatch remains an
annotation; the unlinked rule is untouched. UTF-8 connectors are the
default; setting `HERDR_TOP_ASCII_TREE=1` selects ASCII connectors (`|--`,
`` `-- ``). No automatic capability detection is attempted in this
increment.

### 7. Tab and pane names

The collector persists tab labels and pane titles from herdr observations
(new columns on the corresponding entities). Rows render `Pane: w1:p4
(UI修正)` — id first, name in parentheses when known and non-empty. Name
changes update the stored value; names are subject to the same sanitation
rules as controller labels.

### 8. Timing data and metrics surfaces

1. `restore_task_runs` reads the already-persisted `created_at_ms`,
   `updated_at_ms`, `finished_at_ms` back into the in-memory `TaskRun`
   (new fields); the reducer maintains them for live runs.
2. The header shows session-wide wall-clock elapsed time (now minus the
   session's first observation) at all times. Summed worker time (total of
   run durations) appears only in the Summary overlay, so the two figures
   are never conflated. The header layout reserves space for a future tok/s
   figure but renders nothing for it in this increment.
3. A new Summary overlay opens on `s` (and closes on `s` / `Esc`), built on
   the existing overlay mechanism. It aggregates, per worker-kind × model:
   run count, total and mean duration for terminal runs, and count of live
   runs. Columns for token totals and tok/s are laid out but render `-`
   until Increment 9 supplies data.

### 9. View legibility

1. The main panel header always names the active view (`Execution tree` /
   `Dependency DAG`), so the Tab toggle is visible.
2. When the DAG view has zero dependency edges it renders a one-line
   placeholder ("no dependency edges recorded") instead of empty columns.
3. The footer defines explicit truncation tiers by available width, dropping
   whole hints from the right rather than cutting mid-hint; the full hint
   list remains available in the Help overlay (`?`).

### 10. Clear key

`c` dismisses from the default view: every terminal run, and every expired
hook-only run (section 4.2). Dismissal is recorded in SQLite (new
`dismissed_at_ms` on task runs) and survives restart. Dismissed runs stay
filterable, and any new activity on a dismissed run clears the dismissal.
`c` never deletes rows.

## Error handling and edge cases

1. Watchdog reconnect storms are bounded by backoff; a reconnect that keeps
   failing surfaces as a diagnostics warning, not a crash.
2. Snapshot reconciliation after a freeze uses the section-3 lookup so
   rebinding cannot trip the unique index.
3. Expiry (4.2) and dismissal (10) are pure visibility states; retention
   pruning rules are unchanged.
4. A `dismiss` event (from `SessionEnd`) for an unknown run is a true
   no-op: no placeholder is minted. Dismissal has no ordering semantics
   against run creation — if a late `SessionStart` arrives afterwards, it
   creates a normally visible run, which the 24-hour expiry then covers.
5. Rows with no label, no model, or no timestamps render their fallbacks
   (key-derived name, omitted brackets) rather than empty brackets.

## Testing

1. Watchdog: injected-clock tests for freeze detection, reconnect, snapshot
   request, reconciliation, and backoff; a regression test that a silent
   stream with an open socket recovers.
2. Flat/nested `pane_agent_detected` fixtures, both asserted to produce the
   same normalized observation.
3. Binding-column lookup regression test (previously fatal path).
4. Reducer tests: `SessionEnd` terminality, 24h expiry with activity reset,
   dismissal persistence and un-dismissal on activity.
5. View tests: row format with and without label/model/duration, glyph and
   ASCII tree rendering, DAG placeholder, header view name, footer tiers,
   Summary overlay aggregation (fixed clock).
6. Store round-trip tests for the new columns (timestamps into the model,
   labels, dismissal).

## Documentation impact

1. `docs/design/herdr-top-mvp.md`: §6 (tree rendering, row format, names,
   footer), §7 (watchdog and reconnect), §10 (restored fields, dismissal),
   plus the keybinding list (`s`, `c`).
2. New ADR: `SessionEnd` terminal mapping (reversal of the no-op decision).
3. `docs/guides/controller-emit-setup.md`: event mapping table.

## Sequencing and integration

1. Single implementation branch cut from `main` after the Increment 7 PR
   merges (its Task 2 rewrote `src/herdr/wire.rs` envelope handling and its
   Task 4 touched `src/hook_adapter.rs`; this increment builds on both).
2. Expected implementation surface: `src/herdr/collector.rs`,
   `src/herdr/wire.rs` (reconnect only), `src/hook_adapter.rs`
   (SessionEnd), `src/reducer.rs`, `src/model/entities.rs`,
   `src/model/ids.rs` (if row identity needs it), `src/store/*`
   (columns + restore), `src/tui/*`, `src/activity.rs`, fixtures and tests,
   the documents above.
3. Token metrics (Increment 9) will build on the Summary overlay and the
   reserved header slot without reshaping this increment's UI.
