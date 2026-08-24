# CLI reference

`herdr-top` monitors one Herdr named session and also exposes Controller-event
and diagnostic commands.

```text
Usage: herdr-top [OPTIONS] [COMMAND]
```

With no subcommand, the program launches the TUI. The session name resolves in
this order: `--session`, a non-empty `HERDR_SESSION`, then the reserved name
`default` when `HERDR_ENV=1`. Unless `--socket` is supplied, the Herdr socket
comes from a non-empty `HERDR_SOCKET_PATH`.

## Global options

These two options are global and may be written before or after a subcommand.

| Option | Value | Default and relationship | Meaning |
| --- | --- | --- | --- |
| `--session` | `<SESSION>` | Optional | Monitor this exact Herdr named session. |
| `--socket` | `<PATH>` | Optional; requires `--session` | Override the Herdr connection socket. |

At the top level, `-h` or `--help` prints help and `-V` or `--version` prints
the herdr-top version. Each named subcommand also provides `-h` and `--help`.

## Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `HERDR_SESSION` | Unset | Non-empty named-session fallback when `--session` is absent. |
| `HERDR_SOCKET_PATH` | Unset | Non-empty Herdr socket fallback when `--socket` is absent. |
| `HERDR_ENV` | Unset | The exact value `1` marks a managed pane and permits the reserved `default` session fallback. |
| `XDG_STATE_HOME` | `$HOME/.local/state` | Non-empty state base. When empty or absent, herdr-top uses `$HOME/.local/state`. |
| `HERDR_TOP_ASCII_TREE` | Unicode connectors | The exact value `1` selects ASCII tree connectors; every other value leaves Unicode enabled. |
| `HERDR_TOP_STALL_WARN_MS` | `300000` (5 minutes) | Activity silence after which a live row uses the stall glyph. |
| `HERDR_TOP_HEADLESS_INACTIVITY_MS` | `600000` (10 minutes) | Append silence after which a log-lane run closes as `ended_unknown`. |
| `HERDR_TOP_COMPLETE_GRACE_MS` | `30000` (30 seconds) | Delay before a provider-log completion becomes durable. |
| `HERDR_TOP_GHOST_VISIBILITY_MS` | `300000` (5 minutes) | Time a ghost row remains visible. |
| `HERDR_TOP_BACKFILL_WINDOW_MS` | `86400000` (24 hours) | Age window for admitting pre-existing provider artifacts at startup. |

The five `HERDR_TOP_*_MS` lane values use identical parsing. A value must be
valid UTF-8 and parse as a decimal `i64` millisecond count greater than zero.
An absent, non-UTF-8, malformed, zero, negative, or overflowing value silently
falls back to that variable's default. `doctor` reports the effective values.

## Commands

| Command | Purpose |
| --- | --- |
| `herdr-top` | Launch the fixed-screen monitor. |
| `herdr-top emit` | Send one manual Controller event, or adapt a provider hook payload from standard input. |
| `herdr-top doctor` | Produce a deterministic, non-mutating diagnostic report. |
| `herdr-top help [COMMAND]` | Print top-level help or help for a named command. |

## `emit`

```text
Usage: herdr-top emit [OPTIONS]
```

The command has two mutually exclusive input modes.

1. **Manual mode** builds an envelope from flags. `--event-id`,
   `--emitted-at-ms`, `--source`, `--event-type`, and `--task-run-id` are
   required.
2. **Hook adapter mode** uses `--from-hook` and reads one JSON hook payload from
   standard input. Every manual envelope flag conflicts with `--from-hook`.

### Common options

| Option | Value | Default and relationship | Meaning |
| --- | --- | --- | --- |
| `--strict` | none | Off | Return failure unless delivery is accepted or already present. In hook mode, a `stale_event` response is treated as benign; other delivery failures still fail under strict mode. |
| `--schema-version` | `<SCHEMA_VERSION>` | `1` | Set the Controller wire schema version. |
| `--from-hook` | `<FROM_HOOK>` | Optional; possible values `claude-code`, `codex` | Adapt the provider hook payload read from standard input. |
| `-h`, `--help` | none | - | Print `emit` help. |

The global `--session <SESSION>` and `--socket <PATH>` options are also accepted;
`--socket` still requires `--session`.

### Manual envelope options

Every option in this table conflicts with `--from-hook`.

| Option | Value | Required in manual mode | Envelope field |
| --- | --- | --- | --- |
| `--event-id` | `<EVENT_ID>` | Yes | Unique event ID used for deduplication. |
| `--emitted-at-ms` | `<EMITTED_AT_MS>` | Yes | Emission timestamp as a signed integer in milliseconds. |
| `--source` | `<SOURCE>` | Yes | Event source or Controller name. |
| `--event-type` | `<EVENT_TYPE>` | Yes | Controller event type. |
| `--task-run-id` | `<TASK_RUN_ID>` | Yes | Subject Task Run. |
| `--parent-task-run-id` | `<PARENT_TASK_RUN_ID>` | No | Dispatch parent. |
| `--depends-on-id` | `<DEPENDS_ON_ID>` | No | Prerequisite Task Run. |
| `--label` | `<LABEL>` | No | Task subject label. |
| `--reason` | `<REASON>` | No | Event reason. |
| `--progress` | `<PROGRESS>` | No | Finite floating-point progress in the inclusive range 0.0 to 1.0. |
| `--provider` | `<PROVIDER>` | No | Provider associated with a native session; protocol values are `claude` and `codex`. |
| `--native-session-id` | `<NATIVE_SESSION_ID>` | No | Provider-native session identity. Requires a valid provider at protocol validation time. |
| `--terminal-id` | `<TERMINAL_ID>` | No | Herdr terminal identity used for binding. |

Accepted event types are `dispatch`, `task_started`, `depends_on`, `blocked`,
`progress`, `complete`, `failed`, `cancelled`, and `dismiss`. Direction is
explicit: `task_run_id` is the subject, `parent_task_run_id` is the dispatch
parent, and `depends_on_id` is the prerequisite. A `dispatch` requires its
parent endpoint and a `depends_on` event requires its prerequisite endpoint;
other event types reject either relationship endpoint.

Example manual event:

```sh
herdr-top emit --strict \
  --event-id controller-42-started \
  --emitted-at-ms 1723456789012 \
  --source release-controller \
  --event-type task_started \
  --task-run-id release-42 \
  --label "Prepare release"
```

### Emit delivery contract

The collector answers `accepted`, `duplicate`, `rejected`, or `retryable`.
Events are deduplicated by `event_id`; retrying the same ID is safe within the
ledger retention window. By default, an unavailable collector, unresolved
exchange, rejection, or retryable response is reported without making emit stop
the calling orchestration. `--strict` makes delivery enforceable as described
above.

Hook adapter mode accepts `claude-code` and `codex`, derives versioned envelopes
from the JSON payload, and may emit zero, one, or two events depending on the
hook event. See the
[optional Controller event precision layer](guides/controller-emit-setup.md)
for the complete provider mapping and registration instructions.

## `doctor`

```text
Usage: herdr-top doctor [OPTIONS]
```

| Option | Value | Default and relationship | Meaning |
| --- | --- | --- | --- |
| `--json` | none | Off | Render the fixed Doctor JSON schema v1 instead of the human-readable report. |
| `-h`, `--help` | none | - | Print `doctor` help. |

The global `--session <SESSION>` and `--socket <PATH>` options are also accepted;
`--socket` still requires `--session`.

Doctor checks session and state paths, Herdr reachability, Controller
rendezvous, ownership, database schema, provider discovery, compatibility,
native-session coverage, provider-log lane health, and logs. It exits with
status 1 when any check has `error`; warnings alone do not produce a failing
exit status.

Individual checks use the lowercase statuses `ok`, `warning`, `error`, and
`not_applicable`. The last status is commonly emitted when a check does not
apply, such as the breadcrumb path check for a non-plugin install.
`overall_status` appears in the human-readable header line and as a JSON field.
It is the worst-of severity across the checks and has only `ok`, `warning`, or
`error`; there is no `not_applicable` overall status.

### Provider-log lane health

Three checks describe the zero-configuration provider-log lane:

| Check | Observation | Status and code |
| --- | --- | --- |
| `log_lane.readable` | A provider root exists but cannot be read | `warning` / `log_lane_roots_unreadable` |
| `log_lane.readable` | At least one provider root exists and is readable | `ok` / `log_lane_roots_readable` |
| `log_lane.readable` | No provider root exists | `not_applicable` / `log_lane_roots_absent` |
| `log_lane.coverage` | No pane session has artifacts and targets were rejected | `warning` / `log_lane_targets_rejected` |
| `log_lane.coverage` | No pane sessions are present | `not_applicable` / `log_lane_coverage_empty` |
| `log_lane.coverage` | At least one pane session has no artifact | `warning` / `log_lane_coverage_partial` |
| `log_lane.coverage` | Every pane session has an artifact | `ok` / `log_lane_coverage_complete` |
| `log_lane.freshness` | Latest watcher observation is at most 120000 ms old | `ok` / `log_lane_fresh` |
| `log_lane.freshness` | Latest watcher observation is older than 120000 ms | `warning` / `log_lane_stale` |
| `log_lane.freshness` | The watcher has never observed | `warning` / `log_lane_unobserved` |

The coverage and freshness checks both return `warning` /
`log_lane_runtime_unavailable` when runtime diagnostics are unavailable.
Coverage reports `pane_sessions_total`, `pane_sessions_with_artifacts`,
`pane_sessions_without_artifacts`, and `rejected_targets`. Freshness uses the
watcher's own observation timestamp, never file modification times, so a dead
watcher cannot report itself fresh. Its 120000 ms stale threshold is fixed and
not configurable.

### Herdr protocol compatibility

Compatibility uses Herdr version 0.8.0 and protocol 19 as floors, with protocols
19 and 20 as the reviewed set:

| Observation | Doctor status | Check identifier |
| --- | --- | --- |
| Herdr version is at least 0.8.0 and protocol is 19 or 20 | `ok` | `herdr_compatible` |
| Protocol is newer than 20 | `warning` | `herdr_protocol_newer_unreviewed` |
| Herdr version is below 0.8.0 | `error` | `herdr_below_floor` |
| Protocol is below 19, or falls into an unreviewed gap below the newest reviewed protocol | `error` | `herdr_protocol_mismatch` |
| Herdr version cannot be parsed or obtained | `error` | `herdr_version_unparseable` |

The version floor is checked before the protocol tier. A newer-than-reviewed
protocol is tolerated with a warning; a below-floor or unreviewed older
protocol is not considered compatible.

Examples:

```sh
herdr-top doctor
herdr-top doctor --json
herdr-top --session my-session --socket /path/to/herdr.sock doctor
```
