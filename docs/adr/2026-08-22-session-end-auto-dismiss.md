# Session end auto-dismiss

## Status

Superseded on 2026-08-27 by resumable native-session lifecycle evidence.

## Date

2026-08-22; superseded 2026-08-27.

## Context

A provider `SessionEnd` hook signals that an agent session ended. The hook
adapter originally mapped this event to nothing, so ended session runs remained
in the default view until some other observation changed their visibility.

The original no-op avoided making the semantic Task Run terminal. A native
session can later resume, while semantic terminal state is intentionally not
reopened by runtime liveness. At the time, the model had no separate persisted
axis for resumable native-session lifecycle.

## Previous decision

The 2026-08-22 decision introduced a non-terminal Controller `dismiss` event
and mapped provider `SessionEnd` hooks to it. The event set persisted
`dismissed_at_ms` without changing task state or advancing activity time. A
later non-terminal mutation cleared dismissal, so a resumed `SessionStart`
could make the same run visible again. A dismiss for an unknown run was a true
no-op and created no forward-reference placeholder.

This preserved resume behavior and immediately removed ended sessions from the
default view. It also conflated provider lifecycle evidence with an operator
visibility action, hid ordinary session history immediately, and could not show
why a run appeared terminal-like.

## Superseding decision

`SessionEnd` now emits `session_ended` and records native lifecycle `Done` for a
known run with a matching, non-empty provider/native-session binding. It does
not write semantic Task completion and does not write dismissal. An unknown or
unbound session end is a diagnostic no-op.

Provider-native lifecycle is a persisted axis separate from semantic Task
state, execution state, pane status, graph relationships, and operator
visibility. Explicit provider abort, failure, and disappearance facts record
`Cancelled`, `Error`, and `Unknown` respectively. Codex turn completion is
runtime Idle only.

Each lifecycle fact advances a watermark ordered by trustworthy source time,
collector observation time, and stable source or event identity. Repeating the
same watermark and status is idempotent. A later matching `task_started`, live
execution, or provider liveness fact clears native lifecycle evidence; an older
delayed fact cannot re-close it. Clearing lifecycle never reopens semantic
terminal Task state.

A lifecycle-ended row follows the standard terminal visibility window: it
remains in the default tree for one hour, then becomes default-hidden while
remaining available through retained history and Summary. The `c` key and the
Controller `dismiss` event remain explicit visibility mechanisms independent of
provider lifecycle.

## Migration and compatibility

Schema v6 adds native lifecycle and watermark persistence. Existing Task Runs
migrate without synthesized lifecycle evidence, so upgrade does not reinterpret
old sessions. Existing persisted dismissals retain their visibility meaning;
new `SessionEnd` observations no longer create them. The mandatory SQLite online
backup still precedes migration.

## Alternatives considered

### Map session end to a terminal task state

Mapping `SessionEnd` to `complete` or semantic `cancelled` would make a provider
runtime fact claim a task outcome and would prevent ordinary liveness from
resuming that semantic state. This remains rejected.

### Keep session end as dismissal

The previous decision preserved resume behavior, but immediate hiding discarded
useful lifecycle history and mixed evidence with presentation control. The
separate native lifecycle axis makes that workaround unnecessary.

### Rely only on hook-only inactivity expiry

Expiry remains a fallback for missing evidence, but it is slower and less
truthful than recording a received `SessionEnd`. It cannot replace explicit
lifecycle state.

## Consequences

Normal session end is visible, resumable, ordered across delayed delivery and
restart, and distinguishable from semantic completion. The default tree gains
one hour of terminal session history, while explicit dismissal remains
available. Consumers must treat semantic Task state and native lifecycle as
separate fields and apply semantic terminal precedence first.
