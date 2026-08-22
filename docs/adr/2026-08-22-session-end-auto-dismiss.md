# Session end auto-dismiss

## Status

Accepted

## Date

2026-08-22

## Context

A provider `SessionEnd` hook signals that an agent session ended. The hook adapter previously mapped this event to nothing, so ended session runs remained in the default view until some other observation changed their visibility.

The earlier no-op behavior avoided making the run terminal. A session can later resume and emit `SessionStart`, which maps to `task_started`. The reducer's stale-event protection rejects `task_started` for a run already in a terminal state, so terminalizing `SessionEnd` would prevent a resumed session from becoming visible again.

## Decision

Introduce a dedicated, non-terminal Controller `dismiss` event and map provider `SessionEnd` hooks to it. The event sets the run's persisted `dismissed_at_ms` without changing task state or advancing its activity timestamp. Dismissed runs are hidden from the default view, persist across restart, and remain available through filtering.

Any later non-terminal mutation clears `dismissed_at_ms`, so a resumed session reappears when its `SessionStart` produces `task_started`; no resume-specific reducer path is required. A `dismiss` for an unknown run is a true no-op and does not create a forward-reference placeholder.

## Alternatives considered

### Map session end to a terminal task state

Mapping `SessionEnd` to `complete` or `cancelled` would immediately remove the run from the default live view. It was rejected because the reducer's stale-event protection rejects a later `task_started` on a terminal run. A resumed session would therefore remain permanently invisible. This was the original reason for making `SessionEnd` a no-op, and the dedicated non-terminal event supersedes that rationale.

### Rely only on hook-only inactivity expiry

Controller-keyed runs with no execution attachment expire from the default view after 24 hours of inactivity. Relying on that rule alone was rejected because an ended session would continue cluttering the default view for a full day.

## Consequences

Session end now cleans up the default view immediately while preserving natural resume behavior. The Controller wire protocol gains one event type, `dismiss`. The 24-hour hook-only inactivity expiry remains as a backstop for sessions that end without ever emitting `SessionEnd`.
