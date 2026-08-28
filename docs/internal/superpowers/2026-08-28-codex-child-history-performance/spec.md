# Codex Child Visibility and Historical Performance Accounting

## Status

Approved for implementation on 2026-08-28. This is a corrective delta to
`../2026-08-27-stable-task-history-rates/spec.md`.

## Context

Two defects were found while exercising the locally built PR #21 binary.

1. Current Codex rollouts report a spawned child inside
   `event_msg.payload.type = item_completed` with
   `payload.item.type = SubAgentActivity`. Herdr Top's structural adapter and
   typed fact extractor recognize only the older
   `payload.type = sub_agent_activity` shape. The child artifact is discovered,
   but evidence-gated admission never sees its UUID, so the TUI cannot display
   the child.
2. Historical provider replay uses the same performance-rate admission path as
   live observations. A large unfinished startup drain therefore keeps
   `events_sixty_seconds` above its live-load threshold even when the live event
   rate is low.

## Goals

1. Admit Codex children reported by both legacy and current activity shapes.
2. Preserve the typed allowlist and evidence-gated lineage boundary.
3. Make rolling event-rate windows describe live ingress only.
4. Keep historical reducer work visible to pending-event and event-lag
   accounting.
5. Verify both behaviors with automated tests and a real child in the adjacent
   Herdr Top Pane before publishing the change.

## Non-goals

- Do not infer lineage from free text, tool output, or terminal contents.
- Do not change Task Run ordering, terminal visibility, or retention.
- Do not stop counting a live admitted event because reduction later rejects
  it or finds it semantically idempotent.
- Do not optimize or discard historical replay records in this delta.

## Codex activity compatibility

The adapter accepts two closed structural shapes:

1. Legacy: `event_msg.payload.type = sub_agent_activity`, with `event_id`,
   `occurred_at_ms`, `agent_thread_id`, `agent_path`, and `kind` in `payload`.
2. Current: `event_msg.payload.type = item_completed`, with
   `completed_at_ms` in `payload` and `id`, `agent_thread_id`, `agent_path`, and
   `kind` in an `item` whose exact type is `SubAgentActivity`.

The current shape is normalized into the existing `Activity` event and, for a
start kind, the existing `AgentUpsert` event. `started` and `spawned` are the
two accepted start-kind spellings. IDs, paths, timestamps, event identities,
and malformed-known-record handling retain the existing validation rules.
Other `item_completed` item types remain ignored.

The typed fact extractor also recognizes only the exact nested
`SubAgentActivity` item. A valid UUID in `item.agent_thread_id` emits the
existing `LogFact::EvidenceId` under the current rollout scope using the parsed
record timestamp. It retains neither activity text nor unallowlisted item
fields. The older `sub_agent_activity` fact path remains unchanged.

The parent evidence admits the already-discovered child rollout; the child's
own `session_meta`, lifecycle, model, effort, and token facts remain the source
of its durable identity and state. The same rule applies recursively to
grandchildren.

## Performance accounting by origin

`PerformanceIngress` exposes two admission modes with one lifecycle:

- rated admission records a sequence, pending timestamp, and rate-window
  timestamp;
- unrated admission records the same sequence and pending timestamp but no
  rate-window timestamp.

Both modes complete and drop identically, advance admission/completion
high-water marks, and participate in oldest-pending `event_lag` and its latched
degradation reason.

`ProviderEventSender` chooses the mode only after a bounded queue reservation
succeeds:

- `ObservationOrigin::Live` uses rated admission;
- `ObservationOrigin::Historical` uses unrated admission.

Controller, Herdr, workload-harness, and other live ingress remain rated.
Events that do not require reducer admission remain untracked as before. A
stalled historical event may therefore truthfully produce `event_lag`, but
historical throughput alone cannot produce `events_one_second`,
`events_ten_seconds`, or `events_sixty_seconds`.

## Acceptance criteria

1. A nested `SubAgentActivity` with `kind = started` or `spawned` produces the
   same activity and working-upsert semantics as the legacy start form.
2. Its valid child UUID produces typed lineage evidence; missing, invalid, or
   unrelated item fields do not.
3. Unknown nested item types remain ignored and known malformed activity
   records remain bounded diagnostics without stopping later records.
4. A historical provider event advances pending and high-water state, can
   breach event lag, and contributes zero events to every rate window.
5. A corresponding live provider event continues to increment rate windows.
6. Existing target and twice-target workload boundaries remain unchanged.
7. The complete repository test, lint, and formatting gates pass.
8. The installed local binary shows a real child while active and retains it
   after completion; startup history replay does not cause a rate-only
   `DEGRADED` header.
